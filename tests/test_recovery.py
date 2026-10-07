import copy
import dataclasses
import sys
import time
from types import SimpleNamespace

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
    key = (await cron._dag.trigger("flow"))["runKey"]
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
    key = (await cron._dag.trigger("fan"))["runKey"]
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


def _launch(**over):
    """A launch template with every digested field spelled out, so the
    pinned revision below does not depend on the host's defaults."""
    fields = dict(
        command="./run.sh",
        shell="/bin/sh",
        workingDirectory=None,
        user=None,
        group=None,
        executionTimeout=None,
        killTimeout=30,
        failsWhen={
            "producesStdout": False,
            "producesStderr": True,
            "nonzeroReturn": True,
            "always": False,
        },
        verify=None,
        pool=None,
        poolSlots=1,
        queueTimeout=None,
        queuePriority=0,
        environment=[],
    )
    fields.update(over)
    return SimpleNamespace(**fields)


def _pinned_config(spec_type=dag.TaskSpec, spec_params=(), **spec_extra):
    """One task of every kind: plain, sensor, mapped, approval, all_done."""
    specs = [
        spec_type(id="extract", **spec_extra),
        spec_type(
            id="wait",
            type=dag.SENSOR,
            depends_on=("extract",),
            poke_interval=5.0,
            poke_timeout=60.0,
            poke_jitter=1.5,
            **spec_extra,
        ),
        spec_type(
            id="work",
            depends_on=("extract", "wait"),
            max_attempts=3,
            retry_delay=2.5,
            expand=dag.ExpandSpec(from_task="extract", key="items"),
            **spec_extra,
        ),
        spec_type(
            id="gate",
            type=dag.APPROVAL,
            depends_on=("work",),
            on_reject=dag.SKIPPED,
            **spec_extra,
        ),
        spec_type(
            id="cleanup",
            depends_on=("gate",),
            trigger_rule=dag.ALL_DONE,
            **spec_extra,
        ),
    ]
    launches = {
        "extract": _launch(
            environment=[{"key": "B", "value": "2"}, {"key": "A", "value": "1"}]
        ),
        "wait": _launch(command=["test", "-e", "/tmp/flag"], shell=""),
        "work": _launch(pool="db", poolSlots=2, queuePriority=5),
        "gate": _launch(command=None),
        "cleanup": _launch(executionTimeout=90.0, workingDirectory="/srv"),
    }
    return SimpleNamespace(
        tasks=[
            SimpleNamespace(spec=spec, job_template=launches[spec.id])
            for spec in specs
        ],
        spec=SimpleNamespace(params=spec_params),
    )


def test_configuration_revision_is_pinned():
    # Every run document stores this digest, and recovery compares it with
    # the live configuration. A change to the digest input makes every
    # finished run read as "configuration changed", so a new value here needs
    # a recorded reason.
    assert recovery.configuration_revision(_pinned_config()) == (
        "b35b543ab1f3ea49572aed23088e42499b0705eed1526cbba3a15bc021afd185"
    )


def test_configuration_revision_reads_only_its_listed_spec_fields():
    @dataclasses.dataclass(frozen=True, slots=True)
    class WiderSpec(dag.TaskSpec):
        added_later: tuple = ()

    # the two lists cover every TaskSpec field; a new field fails here until
    # someone decides how it enters the digest.
    assert set(recovery._SPEC_KEYS) | set(recovery._OPTIONAL_SPEC_KEYS) == {
        f.name for f in dataclasses.fields(dag.TaskSpec)
    }
    assert not set(recovery._SPEC_KEYS) & set(recovery._OPTIONAL_SPEC_KEYS)
    for name, default in recovery._OPTIONAL_SPEC_KEYS.items():
        assert getattr(dag.TaskSpec(id="t"), name) == default
    wider = _pinned_config(WiderSpec, added_later=(99,))
    assert "added_later" in dataclasses.asdict(wider.tasks[0].spec)
    assert recovery.configuration_revision(
        wider
    ) == recovery.configuration_revision(_pinned_config())


def test_configuration_revision_reads_skip_exit_codes_only_when_set():
    # the pinned digest above is the "unset" half: a DAG with no
    # skipExitCodes keeps the revision it had before the key existed.
    plain = recovery.configuration_revision(_pinned_config())
    assert recovery.configuration_revision(
        _pinned_config(skip_exit_codes=())
    ) == plain
    with_codes = recovery.configuration_revision(
        _pinned_config(skip_exit_codes=(99,))
    )
    assert with_codes != plain
    assert with_codes != recovery.configuration_revision(
        _pinned_config(skip_exit_codes=(98,))
    )


def test_recovery_refuses_a_run_above_the_engine_level():
    source = {"state": "failed", "engine": dag.ENGINE_LEVEL + 1}
    with pytest.raises(recovery.RecoveryError, match="engine level"):
        recovery.plan(None, source)


async def _failed_branch(dag_cron, tmp_path):
    """The branching diamond with a full load that fails until `ready`.

    The incremental guard skips itself, so the source run ends failed with
    one skipped branch, an `upstream_failed` join, and a handler that ran.
    """
    from tests.test_state_dag_run import _BRANCH, _exit, _set_cmd

    cron = await dag_cron(_BRANCH)
    ready = tmp_path / "ready"
    for task in cron.cron_dags["load"].tasks:
        _set_cmd(cron, "load", task.spec.id, [sys.executable, "-c", "pass"])
    _set_cmd(cron, "load", "full", [
        sys.executable, "-c",
        f"from pathlib import Path; assert Path({str(ready)!r}).exists()",
    ])
    _set_cmd(cron, "load", "incremental", _exit(99))
    key = (await cron._dag.trigger("load"))["runKey"]
    source = await _drive(cron, "load", key)
    assert source["state"] == "failed"
    assert source["tasks"]["incremental"]["state"] == "skipped"
    assert source["tasks"]["publish"]["state"] == "upstream_failed"
    assert source["tasks"]["alert"]["state"] == "success"
    return cron, key, source, ready


async def test_recovery_keeps_a_skipped_branch_and_resets_the_join_and_handler(dag_cron, tmp_path):
    cron, key, source, ready = await _failed_branch(dag_cron, tmp_path)
    preview = await cron._dag.recover("load", key)
    # the join and the handler sit below the failure; the skipped branch
    # does not
    assert preview["tasks"] == ["alert", "full", "publish"]
    assert preview["preserved"] == ["extract", "incremental"]
    assert preview["preservedSkipped"] == {
        "incremental": {"kind": "exit_code", "detail": "exit code 99"},
    }
    ready.touch()
    result = await cron._dag.recover("load", key, plan_token=preview["planToken"])
    finished = await _drive(cron, "load", result["runKey"])
    assert finished["state"] == "success"
    assert finished["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    states = {k: v["state"] for k, v in finished["tasks"].items()}
    assert states == {
        "extract": "success",
        "full": "success",
        "incremental": "skipped",
        # evaluated again under its rule, against the preserved skip and
        # the rerun branch
        "publish": "success",
        # reset with the failure it handled, and nothing failed this time
        "alert": "skipped",
    }
    kept = finished["tasks"]["incremental"]
    assert kept["reusedFrom"] == key
    assert kept["skipReason"] == source["tasks"]["incremental"]["skipReason"]
    assert finished["tasks"]["alert"]["skipReason"]["kind"] == "trigger_rule"
    assert await cron._dag.get_run("load", key) == source


async def test_recovery_from_a_guard_decides_its_branch_again(dag_cron, tmp_path):
    from tests.test_state_dag_run import _exit, _set_cmd

    cron, key, _, ready = await _failed_branch(dag_cron, tmp_path)
    ready.touch()
    # this time the incremental guard lets its branch run and the full one
    # does not: recovering from both guards decides both again
    _set_cmd(cron, "load", "incremental", [sys.executable, "-c", "pass"])
    _set_cmd(cron, "load", "full", _exit(99))
    preview = await cron._dag.recover(
        "load", key, mode="from", tasks=["full", "incremental"],
    )
    assert preview["tasks"] == ["alert", "full", "incremental", "publish"]
    assert preview["preserved"] == ["extract"]
    assert "preservedSkipped" not in preview
    assert preview["configurationChanged"]  # the commands changed
    result = await cron._dag.recover(
        "load", key, mode="from", tasks=["full", "incremental"],
        plan_token=preview["planToken"], allow_config_change=True,
    )
    finished = await _drive(cron, "load", result["runKey"])
    assert finished["state"] == "success"
    states = {k: v["state"] for k, v in finished["tasks"].items()}
    assert states["full"] == "skipped"
    assert states["incremental"] == "success"
    assert states["publish"] == "success"
    assert "reusedFrom" not in finished["tasks"]["full"]


def test_recovery_preview_names_reused_skips_recorded_without_a_reason():
    # a run from a build that recorded no skipReason still lists the task
    spec = dag.DagSpec.build("d", [
        dag.TaskSpec("gate", type=dag.APPROVAL, on_reject=dag.SKIPPED),
        dag.TaskSpec("after", depends_on=("gate",)),
        dag.TaskSpec("other"),
    ])
    config = SimpleNamespace(
        name="d",
        spec=spec,
        tasks=[SimpleNamespace(spec=t, job_template=_launch()) for t in spec.tasks],
    )
    source = dag.new_run_body(
        dag="d", run_key="k", run_id="r", logical_date=None, kind="manual",
        now=1.0, spec=spec,
    )
    source["state"] = "failed"
    source["tasks"]["gate"]["state"] = "skipped"
    source["tasks"]["after"]["state"] = "skipped"
    source["tasks"]["after"]["skipReason"] = {"kind": "upstream", "detail": "upstream skipped: gate"}
    source["tasks"]["other"]["state"] = "failed"
    plan = recovery.plan(config, source)
    assert plan["tasks"] == ["other"]
    assert plan["preservedSkipped"] == {
        "after": {"kind": "upstream", "detail": "upstream skipped: gate"},
        "gate": None,
    }


def test_configuration_revision_reads_when_only_when_set():
    # the pinned digest is the "unset" half: a DAG with no `when:` keeps the
    # revision it had before the key existed.
    plain = recovery.configuration_revision(_pinned_config())
    assert recovery.configuration_revision(_pinned_config(when=())) == plain

    def revision(*conditions):
        return recovery.configuration_revision(_pinned_config(when=conditions))

    on_param = dag.Condition(
        source=dag.WHEN_PARAM, name="mode", op="equals", values=("full",)
    )
    on_xcom = dag.Condition(
        source=dag.WHEN_XCOM, name="extract", key="rows", op="notIn",
        values=("0", ""),
    )
    assert revision(on_param) != plain
    # each part of a comparison is in the digest
    seen = {plain, revision(on_param), revision(on_xcom), revision(on_param, on_xcom)}
    for changed in (
        dataclasses.replace(on_param, name="other"),
        dataclasses.replace(on_param, op="notEquals"),
        dataclasses.replace(on_param, values=("inc",)),
        dataclasses.replace(on_param, values=(True,)),
        dataclasses.replace(on_xcom, key="count"),
        dataclasses.replace(on_xcom, values=("0",)),
    ):
        digest = revision(changed)
        assert digest not in seen
        seen.add(digest)


async def _failed_condition(dag_cron, tmp_path, mode, published=b"12\n"):
    """The `cond` workflow of test_state_dag_run with the branch that `mode`
    selects failing until `ready` exists, so the source run ends failed with
    the other branch skipped by its condition."""
    from tests.test_state_dag_run import _when_cron, _set_cmd

    cron = await _when_cron(dag_cron, tmp_path, published)
    ready = tmp_path / "ready"
    _set_cmd(cron, "cond", "full" if mode == "full" else "incremental", [
        sys.executable, "-c",
        f"from pathlib import Path; assert Path({str(ready)!r}).exists()",
    ])
    started = await cron._dag.trigger("cond", params={"mode": mode})
    source = await _drive(cron, "cond", started["runKey"])
    assert source["state"] == "failed"
    return cron, started["runKey"], source, ready


async def test_recovery_keeps_a_branch_a_condition_skipped(dag_cron, tmp_path):
    cron, key, source, ready = await _failed_condition(dag_cron, tmp_path, "full")
    reason = {
        "kind": "condition",
        "detail": "param mode notEquals full: the value is full",
    }
    assert source["tasks"]["incremental"]["skipReason"] == reason
    preview = await cron._dag.recover("cond", key)
    assert preview["tasks"] == ["full", "publish"]
    assert preview["preserved"] == ["extract", "incremental"]
    assert preview["preservedSkipped"] == {"incremental": reason}
    assert preview["params"] == {"mode": "full"}
    ready.touch()
    result = await cron._dag.recover("cond", key, plan_token=preview["planToken"])
    finished = await _drive(cron, "cond", result["runKey"])
    assert finished["state"] == "success"
    assert finished["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    assert {k: v["state"] for k, v in finished["tasks"].items()} == {
        "extract": "success",
        "full": "success",
        "incremental": "skipped",
        "publish": "success",
    }
    kept = finished["tasks"]["incremental"]
    assert kept["reusedFrom"] == key and kept["skipReason"] == reason
    # the reset task got a fresh entry, so its condition was read again
    assert "reusedFrom" not in finished["tasks"]["full"]
    assert finished["tasks"]["full"]["whenMet"] is True


async def test_a_reset_task_reads_the_value_a_preserved_task_published(dag_cron, tmp_path):
    # `incremental` compares what `extract` published. `extract` is reused,
    # so the comparison reads the copy of its value in the recovery run.
    cron, key, source, ready = await _failed_condition(dag_cron, tmp_path, "incremental")
    assert source["tasks"]["incremental"]["whenMet"] is True
    preview = await cron._dag.recover("cond", key)
    assert preview["tasks"] == ["incremental", "publish"]
    assert preview["preserved"] == ["extract", "full"]
    assert [a["name"] for a in preview["artifacts"]] == ["extract/row_count"]
    ready.touch()
    result = await cron._dag.recover("cond", key, plan_token=preview["planToken"])
    finished = await _drive(cron, "cond", result["runKey"])
    assert finished["state"] == "success"
    assert finished["tasks"]["incremental"]["state"] == "success"
    assert finished["tasks"]["incremental"]["whenMet"] is True
    assert finished["tasks"]["extract"]["reusedFrom"] == key


async def test_recovery_from_the_publisher_reads_the_condition_again(dag_cron, tmp_path):
    from tests.test_state_dag_run import _when_cron

    # the source run published 0 rows, so the incremental load was skipped
    cron = await _when_cron(dag_cron, tmp_path, b"0\n")
    started = await cron._dag.trigger("cond", params={"mode": "incremental"})
    key = started["runKey"]
    source = await _drive(cron, "cond", key)
    assert source["state"] == "success"
    assert source["tasks"]["incremental"]["skipReason"]["detail"] == (
        "xcom extract/row_count notIn 0: the value is 0"
    )
    assert source["tasks"]["publish"]["state"] == "skipped"
    # the same command now publishes 12 rows
    (tmp_path / "row_count").write_bytes(b"12\n")
    preview = await cron._dag.recover("cond", key, mode="from", tasks=["extract"])
    # the publisher is upstream of the task that compares its value, so the
    # conditional task is reset with it
    assert preview["tasks"] == ["extract", "full", "incremental", "publish"]
    assert preview["preserved"] == [] and "preservedSkipped" not in preview
    assert not preview["configurationChanged"]
    result = await cron._dag.recover(
        "cond", key, mode="from", tasks=["extract"], plan_token=preview["planToken"],
    )
    finished = await _drive(cron, "cond", result["runKey"])
    assert finished["state"] == "success"
    assert {k: v["state"] for k, v in finished["tasks"].items()} == {
        "extract": "success",
        "full": "skipped",
        "incremental": "success",
        "publish": "success",
    }
    assert await cron._dag.get_run("cond", key) == source
