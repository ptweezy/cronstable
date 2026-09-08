import copy
import sys
import time

import pytest

from cronstable import dag, jobstate, recovery
from tests.test_state_dag_run import _drive


CONFIG = '''
dags:
  - name: flow
    tasks:
      - id: extract
        command: ignored
      - id: upload
        command: ignored
        dependsOn:
          - extract
      - id: notify
        command: ignored
        dependsOn:
          - upload
'''


async def failed_flow(factory, tmp_path):
    cron = await factory(CONFIG)
    templates = cron.cron_dags["flow"].task_templates
    marker = tmp_path / "upload-ready"
    for task in templates.values():
        task.command = [sys.executable, "-c", "pass"]
    templates["upload"].command = [
        sys.executable, "-c", f"from pathlib import Path; assert Path({str(marker)!r}).exists()",
    ]
    key = await cron._dag.trigger_run("flow")
    body = await _drive(cron, "flow", key)
    assert body["state"] == "failed"
    return cron, key, body, marker


async def test_recovery_preserves_success_and_copies_only_its_artifacts(dag_cron, tmp_path):
    cron, key, source, marker = await failed_flow(dag_cron, tmp_path)
    scope = dag.xcom_scope("flow", source["runId"])
    await jobstate.artifact_put(cron.state_backend, scope, "extract/data", b"important")
    await jobstate.artifact_put(cron.state_backend, scope, "upload/stale", b"stale")
    preview = await cron._dag.recover("flow", key)
    assert preview["tasks"] == ["notify", "upload"]
    assert preview["preserved"] == ["extract"]
    assert [a["name"] for a in preview["artifacts"]] == ["extract/data"]
    marker.touch()
    result = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    finished = await _drive(cron, "flow", result["runKey"])
    assert finished["state"] == "success"
    assert finished["tasks"]["extract"]["reusedFrom"] == key
    assert await cron._dag.get_run("flow", key) == source
    copied_scope = dag.xcom_scope("flow", finished["runId"])
    got = await jobstate.artifact_get(cron.state_backend, copied_scope, "extract/data")
    assert got[1] == b"important"
    assert await jobstate.artifact_get(cron.state_backend, copied_scope, "upload/stale") is None
    repeated = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    assert not repeated["created"]
    assert repeated["runKey"] == result["runKey"]


async def test_stale_preview_and_changed_configuration_require_review(dag_cron, tmp_path):
    cron, key, _, _ = await failed_flow(dag_cron, tmp_path)
    preview = await cron._dag.recover("flow", key)
    cron.cron_dags["flow"].task_templates["upload"].command = [sys.executable, "-c", "pass"]
    with pytest.raises(recovery.RecoveryError, match="stale"):
        await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    fresh = await cron._dag.recover("flow", key)
    assert fresh["configurationChanged"]
    with pytest.raises(recovery.RecoveryError, match="configuration differs"):
        await cron._dag.recover("flow", key, plan_token=fresh["planToken"])
    result = await cron._dag.recover(
        "flow", key, plan_token=fresh["planToken"], allow_config_change=True,
    )
    assert (await _drive(cron, "flow", result["runKey"]))["state"] == "success"


async def test_rerun_from_task_includes_downstream(dag_cron, tmp_path):
    cron, key, _, marker = await failed_flow(dag_cron, tmp_path)
    marker.touch()
    preview = await cron._dag.recover("flow", key, mode="from", tasks=["extract"])
    assert preview["tasks"] == ["extract", "notify", "upload"]
    assert preview["preserved"] == []
    result = await cron._dag.recover(
        "flow", key, mode="from", tasks=["extract"], plan_token=preview["planToken"],
    )
    assert (await _drive(cron, "flow", result["runKey"]))["state"] == "success"


def test_recovery_rejects_active_run():
    with pytest.raises(recovery.RecoveryError, match="finished"):
        recovery.plan(None, {"state": "running"})


async def test_mapped_recovery_preserves_successful_instances(dag_cron, tmp_path):
    from tests.test_state_dag_run import _FANOUT, _set_cmd

    cron = await dag_cron(_FANOUT)
    ready = tmp_path / "ready"
    items = tmp_path / "items.json"
    items.write_text('["alpha", "beta"]')
    for task in cron.cron_dags["fan"].tasks:
        _set_cmd(cron, "fan", task.spec.id, [sys.executable, "-c", "pass"])
    _set_cmd(cron, "fan", "gen", [sys.executable, "-m", "cronstable", "xcom", "push", "--key", "items", str(items)])
    _set_cmd(cron, "fan", "work", [sys.executable, "-c",
        f"import os; from pathlib import Path; assert os.environ['CRONSTABLE_DAG_MAP_INDEX'] == '0' or Path({str(ready)!r}).exists()"])
    key = await cron._dag.trigger_run("fan")
    source = await _drive(cron, "fan", key)
    assert source["tasks"]["work#0"]["state"] == "success"
    assert source["tasks"]["work#1"]["state"] == "failed"
    preview = await cron._dag.recover("fan", key)
    assert preview["tasks"] == ["collect", "work#1"]
    assert "work#0" in preview["preserved"]
    ready.touch()
    result = await cron._dag.recover("fan", key, plan_token=preview["planToken"])
    finished = await _drive(cron, "fan", result["runKey"])
    assert finished["state"] == "success"
    assert finished["tasks"]["work#0"]["reusedFrom"] == key
    assert finished["tasks"]["work#1"]["mapItem"] == "beta"
    upstream = await cron._dag.recover("fan", key, mode="from", tasks=["gen"])
    assert upstream["resetMappings"] == ["work"]
    fresh = recovery.new_run(cron.cron_dags["fan"], source, upstream, time.time())
    assert "work#0" not in fresh["tasks"]
    assert fresh["mapped"] == {}


async def test_failed_dates_resume_batch_without_repeating_success(dag_cron, tmp_path, monkeypatch):
    cron, _, source, marker = await failed_flow(dag_cron, tmp_path)
    for day, state in [(1, "failed"), (2, "success"), (3, "failed")]:
        body = copy.deepcopy(source)
        body.update(runKey=f"date-{day}", runId=f"date-{day}", logicalDate=f"2026-09-0{day}T00:00:00+00:00", state=state)
        await cron.state_backend.mutate_document(cron._dag._ns("flow"), body["runKey"], lambda _, b=body: (b, None))
    preview = await cron._dag.recover_range("flow", "2026-09-01", "2026-09-03")
    assert [p["sourceRunKey"] for p in preview["plans"]] == ["date-1", "date-3"]
    marker.touch()
    recover = cron._dag.recover

    async def interrupted(name, key, **kwargs):
        if key == "date-3":
            raise recovery.RecoveryError("temporary interruption")
        return await recover(name, key, **kwargs)

    monkeypatch.setattr(cron._dag, "recover", interrupted)
    with pytest.raises(recovery.RecoveryError, match="interruption"):
        await cron._dag.recover_range("flow", "2026-09-01", "2026-09-03", plan_token=preview["planToken"])
    monkeypatch.setattr(cron._dag, "recover", recover)
    result = await cron._dag.recover_range("flow", "2026-09-01", "2026-09-03", plan_token=preview["planToken"])
    assert len(result["runs"]) == 2
    for key in result["runs"]:
        assert (await _drive(cron, "flow", key))["state"] == "success"
    again = await cron._dag.recover_range("flow", "2026-09-01", "2026-09-03", plan_token=preview["planToken"])
    assert again == result
    assert (await cron._dag.recover_range("flow", "2026-09-01", "2026-09-03"))["dates"] == 0


async def test_missing_artifact_blocks_execution(dag_cron, tmp_path, monkeypatch):
    cron, key, source, _ = await failed_flow(dag_cron, tmp_path)
    await jobstate.artifact_put(cron.state_backend, dag.xcom_scope("flow", source["runId"]), "extract/data", b"x")
    preview = await cron._dag.recover("flow", key)
    async def missing(*args):
        return False
    monkeypatch.setattr(cron.state_backend, "blob_exists", missing)
    with pytest.raises(recovery.RecoveryError, match="artifact is missing"):
        await cron._dag.recover("flow", key, plan_token=preview["planToken"])


async def test_retention_preserves_source_during_recovery_preparation(dag_cron, tmp_path, monkeypatch):
    cron, key, source, _ = await failed_flow(dag_cron, tmp_path)
    async def defer(*args):
        pass
    monkeypatch.setattr(cron._dag, "_try_own", defer)
    preview = await cron._dag.recover("flow", key)
    result = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    await cron._dag._delete_run(cron.state_backend, "flow", key, source["runId"])
    assert await cron._dag._read("flow", key) is not None
    await cron._dag._prepare_recovery(("flow", result["runKey"]))
    await cron._dag._delete_run(cron.state_backend, "flow", key, source["runId"])
    assert await cron._dag._read("flow", key) is None
    repeated = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    assert repeated["runKey"] == result["runKey"]
    assert not repeated["created"]
    with pytest.raises(recovery.RecoveryError, match="selection differs"):
        await cron._dag.recover("flow", key, mode="from", tasks=["extract"], plan_token=preview["planToken"])
