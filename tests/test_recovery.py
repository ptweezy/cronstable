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


async def test_recovery_artifact_limit_counts_names(
    dag_cron, tmp_path, monkeypatch
):
    import hashlib

    from cronstable import dagrun

    cron, key, source, _ = await failed_flow(dag_cron, tmp_path)
    backend = cron.state_backend
    scope = dag.xcom_scope("flow", source["runId"])
    names = ["extract/a", "extract/b", "extract/c"]
    monkeypatch.setattr(dagrun, "RECOVERY_MAX_ARTIFACTS", len(names))
    # Each name is published twice. The scope holds the superseded records
    # until its next prune, as it holds the versions that another node
    # published, and they do not count toward the limit.
    monkeypatch.setattr(backend, "_unlink_superseded", lambda *args: None)
    for data in (b"old", b"new"):
        for name in names:
            await jobstate.artifact_put(backend, scope, name, data)
    stream = jobstate.ARTIFACT_STREAM_PREFIX + scope
    assert len(await backend.list_records(stream)) == 2 * len(names)
    preview = await cron._dag.recover("flow", key)
    assert [a["name"] for a in preview["artifacts"]] == names
    newest = hashlib.sha256(b"new").hexdigest()
    assert {a["sha256"] for a in preview["artifacts"]} == {newest}
    await jobstate.artifact_put(backend, scope, "extract/d", b"new")
    with pytest.raises(recovery.RecoveryError, match="at most 3 artifacts"):
        await cron._dag.recover("flow", key)


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
    run = [(key, source["runId"])]
    await cron._dag._delete_run_batch(cron.state_backend, "flow", run)
    assert await cron._dag._read("flow", key) is not None
    await cron._dag._prepare_recovery(("flow", result["runKey"]))
    await cron._dag._delete_run_batch(cron.state_backend, "flow", run)
    assert await cron._dag._read("flow", key) is None
    repeated = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    assert repeated["runKey"] == result["runKey"]
    assert not repeated["created"]
    with pytest.raises(recovery.RecoveryError, match="selection differs"):
        await cron._dag.recover("flow", key, mode="from", tasks=["extract"], plan_token=preview["planToken"])


def test_configuration_revision_hashes_environment_names_only(tmp_path, monkeypatch):
    from cronstable.config import parse_config_string

    env_file = tmp_path / "task.env"

    def revision(secret, extra=""):
        monkeypatch.setenv("DB_PASSWORD", secret)
        env_file.write_text(f"TOKEN={secret}\n")
        text = CONFIG.replace(
            "      - id: extract\n        command: ignored\n",
            "      - id: extract\n        command: ignored\n"
            f"        env_file: {env_file.as_posix()}\n"
            "        environment:\n"
            "          - key: DB_PASSWORD\n"
            "            value: ${DB_PASSWORD}\n" + extra,
        )
        config = parse_config_string(text, "")
        env = config.dags[0].task_templates["extract"].environment
        values = {e["key"]: e["value"] for e in env}
        assert values["DB_PASSWORD"] == values["TOKEN"] == secret
        return recovery.configuration_revision(config.dags[0])

    assert revision("orange77") == revision("correct-horse")
    added = "          - key: MODE\n            value: full\n"
    assert revision("orange77", added) != revision("orange77")


def _chain_config(count, extra=""):
    from cronstable.config import parse_config_string

    lines = ["dags:", "  - name: chain", "    tasks:"]
    for i in range(count):
        lines += ["      - id: t%d" % i, "        command: ignored"]
        if i:
            lines += ["        dependsOn:", "          - t%d" % (i - 1)]
    text = "\n".join(lines) + "\n" + extra
    return parse_config_string(text, "").dags[0]


def _finished_source(config):
    body = dag.new_run_body(
        dag=config.name,
        run_key="source",
        run_id="source-id",
        logical_date=None,
        kind="manual",
        now=1.0,
        spec=config.spec,
    )
    for entry in body["tasks"].values():
        entry["state"] = dag.SUCCESS
    body["state"] = dag.SUCCESS
    return body


def test_rerun_from_task_selects_everything_downstream_and_nothing_else():
    config = _chain_config(
        6,
        "      - id: side\n        command: ignored\n"
        "      - id: join\n        command: ignored\n"
        "        dependsOn:\n          - t5\n          - side\n",
    )
    source = _finished_source(config)
    plan = recovery.plan(config, source, mode="from", tasks=("t3",))
    assert plan["tasks"] == ["join", "t3", "t4", "t5"]
    assert plan["preserved"] == ["side", "t0", "t1", "t2"]
    plan = recovery.plan(config, source, mode="from", tasks=("side", "t5"))
    assert plan["tasks"] == ["join", "side", "t5"]


def test_rerun_from_task_walks_the_graph_a_fixed_number_of_times():
    # Planning runs on the event loop.  The downstream closure follows each
    # edge once, so a deep chain costs as many walks of the task list as a
    # shallow one.
    import types

    class _CountedTasks(tuple):
        def __iter__(self):
            self.walks += 1
            return super().__iter__()

    def walks(depth):
        config = _chain_config(depth)
        counted = _CountedTasks(config.spec.tasks)
        counted.walks = 0
        spec = types.SimpleNamespace(
            by_id=config.spec.by_id,
            mapped_tasks=config.spec.mapped_tasks,
            tasks=counted,
        )
        stand_in = types.SimpleNamespace(
            name=config.name, tasks=config.tasks, spec=spec
        )
        plan = recovery.plan(
            stand_in, _finished_source(config), mode="from", tasks=("t0",)
        )
        assert len(plan["tasks"]) == depth
        return counted.walks

    assert walks(6) == walks(40)
