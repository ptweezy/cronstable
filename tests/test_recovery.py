import asyncio
import copy
import sys
import time

import pytest

from cronstable import dag, dagrun, jobstate, recovery
from tests.test_state_dag_run import _drive, _run_leases_free


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


_RANGE = ("flow", "2026-09-01", "2026-09-03")


async def _failed_dates(factory, days=(1, 3)):
    """A cron whose flow failed on each of ``days`` in September 2026, and
    the keys of those runs.  The recovery runs it creates stay unowned."""
    cron = await factory(CONFIG)
    config = cron.cron_dags["flow"]
    keys = []
    for day in days:
        key = "date-%d" % day
        body = dag.new_run_body(
            dag="flow",
            run_key=key,
            run_id=key,
            logical_date="2026-09-0%dT00:00:00+00:00" % day,
            kind="scheduled",
            now=1000.0 + day,
            spec=config.spec,
        )
        body["state"] = dag.FAILED
        body["configurationRevision"] = recovery.configuration_revision(config)
        for entry in body["tasks"].values():
            entry["state"] = dag.FAILED
        await cron.state_backend.mutate_document(
            cron._dag._ns("flow"), key, lambda _, b=body: (b, None)
        )
        keys.append(key)

    async def defer(*args):
        return False

    cron._dag._try_own = defer
    return cron, keys


def _listed(keys):
    """The runs of :func:`_failed_dates` under ``keys``, as a retention
    pass lists them."""
    return [(key, key, 1000.0 + int(key.rpartition("-")[2])) for key in keys]


@pytest.mark.parametrize("days", [(1, 3), (1, 2, 3)])
async def test_failed_dates_write_the_batch_under_every_source_lease(
    days, dag_cron, monkeypatch
):
    # The request holds the lease of each source when the batch document
    # lands, confirms the sources there with one listing however many
    # they are, and gives the leases back before recover() takes them.
    cron, keys = await _failed_dates(dag_cron, days)
    backend = cron.state_backend
    # a document that names no run key is no source
    await backend.mutate_document(
        "dagrun/flow", "stray", lambda _: ({"runKey": 7}, None)
    )
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    names = [cron._dag._lease_name(("flow", key)) for key in keys]
    real_mutate = backend.mutate_document
    real_list = backend.list_documents
    holders = []
    listings = []

    async def mutate(namespace, key, transform):
        if namespace == "recoverybatch/flow" and not holders:
            for name in names:
                lease = await backend.read_lease(name)
                holders.append(lease and lease.holder)
        return await real_mutate(namespace, key, transform)

    async def listing(namespace, **kwargs):
        listings.append(namespace)
        return await real_list(namespace, **kwargs)

    monkeypatch.setattr(backend, "mutate_document", mutate)
    monkeypatch.setattr(backend, "list_documents", listing)
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    monkeypatch.undo()
    assert holders[0] is not None and ":recovery:" in holders[0]
    assert holders == [holders[0]] * len(keys)
    # the listing that plans the dates, then the one under the leases
    assert listings == ["dagrun/flow"] * 2
    assert result["complete"] and len(result["runs"]) == len(keys)
    assert await _run_leases_free(cron, "flow", keys) == [True] * len(keys)


async def test_failed_dates_wait_for_a_source_that_retention_holds(dag_cron):
    # Retention holds the lease of the second source.  The request writes
    # no batch and gives back the lease that it took on the first.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    held = await backend.acquire_lease(
        cron._dag._lease_name(("flow", keys[1])), "retention", 60
    )
    assert held is not None
    with pytest.raises(recovery.RecoveryError, match="source run is busy"):
        await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert await backend.read_document("recoverybatch/flow", token) is None
    assert await _run_leases_free(cron, "flow", keys) == [True, False]
    await backend.release_lease(held)
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert result["complete"] and len(result["runs"]) == 2


async def test_retention_and_a_new_recovery_batch_take_turns_on_a_source(
    dag_cron, monkeypatch
):
    # A retention batch has read the recovery batches and is about to
    # delete a source.  A request that names the source writes no batch
    # behind that read, so no open batch outlives its source.
    cron, keys = await _failed_dates(dag_cron, days=(1,))
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    real = backend.mutate_document
    entered, resume = asyncio.Event(), asyncio.Event()

    async def held_delete(namespace, key, transform):
        if key == keys[0]:
            entered.set()
            await resume.wait()
        return await real(namespace, key, transform)

    monkeypatch.setattr(backend, "mutate_document", held_delete)
    retention = asyncio.ensure_future(
        cron._dag._delete_run_batch(backend, "flow", _listed(keys))
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        with pytest.raises(recovery.RecoveryError, match="run is busy"):
            await cron._dag.recover_range(*_RANGE, plan_token=token)
        batch = await backend.read_document("recoverybatch/flow", token)
    finally:
        resume.set()
        await asyncio.wait_for(retention, 5)
    assert batch is None
    assert await cron._dag._read("flow", keys[0]) is None
    with pytest.raises(recovery.RecoveryError, match="preview is stale"):
        await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert await backend.read_document("recoverybatch/flow", token) is None


@pytest.mark.parametrize("fate", ["deleted", "replaced"])
async def test_failed_dates_confirm_each_source_under_its_lease(
    fate, dag_cron, monkeypatch
):
    # The second source goes after the request listed it and before the
    # request holds its lease, as a retention delete takes it.  A backfill
    # of the date may then create the key again.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    real = backend.acquire_lease
    taken = []

    def backfilled(body):
        body["runId"] = "backfilled"
        return body, None

    async def acquire(name, holder, ttl):
        if not taken:
            if fate == "deleted":
                await backend.delete_document("dagrun/flow", keys[1])
            else:
                await backend.mutate_document(
                    "dagrun/flow", keys[1], backfilled
                )
        taken.append(name)
        return await real(name, holder, ttl)

    monkeypatch.setattr(backend, "acquire_lease", acquire)
    with pytest.raises(recovery.RecoveryError, match="preview is stale"):
        await cron._dag.recover_range(*_RANGE, plan_token=token)
    monkeypatch.undo()
    assert len(taken) == 2
    assert await backend.read_document("recoverybatch/flow", token) is None
    assert await _run_leases_free(cron, "flow", keys) == [True, True]


async def test_failed_dates_write_no_batch_once_half_a_lease_has_passed(
    dag_cron, monkeypatch
):
    # Leasing the sources takes half the lease TTL, so the batch write
    # could land after the first lease has lapsed.
    from types import SimpleNamespace

    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    clock = [1000.0]
    real = backend.acquire_lease

    async def slow_acquire(name, holder, ttl):
        clock[0] += dagrun.RECOVERY_SOURCE_LEASE_TTL / 4
        return await real(name, holder, ttl)

    with monkeypatch.context() as patch:
        # the module's own clock: the event loop keeps the real one
        patch.setattr(
            dagrun,
            "time",
            SimpleNamespace(time=dagrun.time.time, monotonic=lambda: clock[0]),
        )
        patch.setattr(backend, "acquire_lease", slow_acquire)
        with pytest.raises(
            recovery.RecoveryError,
            match="took too long; retry or select fewer dates",
        ):
            await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert await backend.read_document("recoverybatch/flow", token) is None
    assert await _run_leases_free(cron, "flow", keys) == [True, True]
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert result["complete"] and len(result["runs"]) == 2


@pytest.mark.parametrize("ending", ["timeout", "builtin", "cancelled"])
async def test_failed_dates_keep_their_leases_behind_an_abandoned_batch_write(
    ending, dag_cron, monkeypatch
):
    # A batch write that ends in a timeout or a cancellation can still
    # land.  The source leases stay, so retention leaves the sources.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    real = backend.mutate_document
    entered = asyncio.Event()
    abandoned = []

    async def write(namespace, key, transform):
        if namespace != "recoverybatch/flow":
            return await real(namespace, key, transform)
        abandoned.append(transform)
        if ending == "timeout":
            raise asyncio.TimeoutError
        if ending == "builtin":
            raise TimeoutError
        entered.set()
        await asyncio.Event().wait()

    with monkeypatch.context() as patch:
        patch.setattr(backend, "mutate_document", write)
        request = asyncio.ensure_future(
            cron._dag.recover_range(*_RANGE, plan_token=token)
        )
        if ending == "cancelled":
            await asyncio.wait_for(entered.wait(), 5)
            request.cancel()
        raised = {
            "timeout": asyncio.TimeoutError,
            "builtin": TimeoutError,
            "cancelled": asyncio.CancelledError,
        }[ending]
        with pytest.raises(raised):
            await request
    assert await _run_leases_free(cron, "flow", keys) == [False, False]
    await cron._dag._delete_run_batch(backend, "flow", _listed(keys))
    assert await backend.list_document_keys("dagrun/flow") == keys
    # the write lands and the leases lapse: the same token completes
    await real("recoverybatch/flow", token, abandoned[0])
    for key in keys:
        name = cron._dag._lease_name(("flow", key))
        await backend.release_lease(await backend.read_lease(name))
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert result["complete"] and len(result["runs"]) == 2


async def test_failed_dates_wait_behind_an_abandoned_retention_delete(
    dag_cron, monkeypatch
):
    # A retention delete that timed out can still land, so the run's lease
    # stays and a request that names the run records no batch.  Once the
    # delete has landed and the lease has lapsed, the preview is stale.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    real = backend.mutate_document
    abandoned = []

    async def delete(namespace, key, transform):
        if key != keys[1]:
            return await real(namespace, key, transform)
        abandoned.append(transform)
        raise asyncio.TimeoutError

    with monkeypatch.context() as patch:
        patch.setattr(backend, "mutate_document", delete)
        with pytest.raises(asyncio.TimeoutError):
            await cron._dag._delete_run_batch(
                backend, "flow", _listed(keys[1:])
            )
    assert await _run_leases_free(cron, "flow", keys) == [True, False]
    with pytest.raises(recovery.RecoveryError, match="source run is busy"):
        await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert await backend.read_document("recoverybatch/flow", token) is None
    await real("dagrun/flow", keys[1], abandoned[0])
    name = cron._dag._lease_name(("flow", keys[1]))
    await backend.release_lease(await backend.read_lease(name))
    with pytest.raises(recovery.RecoveryError, match="preview is stale"):
        await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert await backend.read_document("recoverybatch/flow", token) is None
    assert await backend.list_document_keys("dagrun/flow") == keys[:1]


@pytest.mark.parametrize("ending", ["timeout", "builtin", "cancelled"])
async def test_recovery_keeps_the_source_lease_behind_an_abandoned_create(
    ending, dag_cron, monkeypatch
):
    # A create of the recovery run that ends in a timeout or a cancellation
    # can still land.  The source lease stays, so retention leaves the
    # source.
    cron, keys = await _failed_dates(dag_cron, days=(1,))
    backend = cron.state_backend
    token = (await cron._dag.recover("flow", keys[0]))["planToken"]
    real = backend.mutate_document
    entered = asyncio.Event()
    abandoned = []

    async def write(namespace, key, transform):
        if key != "recovery-" + token:
            return await real(namespace, key, transform)
        abandoned.append(transform)
        if ending == "timeout":
            raise asyncio.TimeoutError
        if ending == "builtin":
            raise TimeoutError
        entered.set()
        await asyncio.Event().wait()

    raised = {
        "timeout": asyncio.TimeoutError,
        "builtin": TimeoutError,
        "cancelled": asyncio.CancelledError,
    }[ending]
    with monkeypatch.context() as patch:
        patch.setattr(backend, "mutate_document", write)
        request = asyncio.ensure_future(
            cron._dag.recover("flow", keys[0], plan_token=token)
        )
        if ending == "cancelled":
            await asyncio.wait_for(entered.wait(), 5)
            request.cancel()
        with pytest.raises(raised):
            await request
    assert await _run_leases_free(cron, "flow", keys) == [False]
    await cron._dag._delete_run_batch(backend, "flow", _listed(keys))
    assert await backend.list_document_keys("dagrun/flow") == keys
    # the create lands and the lease lapses: the same token answers with
    # the run, which holds the source while it prepares
    await real("dagrun/flow", "recovery-" + token, abandoned[0])
    name = cron._dag._lease_name(("flow", keys[0]))
    await backend.release_lease(await backend.read_lease(name))
    result = await cron._dag.recover("flow", keys[0], plan_token=token)
    assert not result["created"]
    assert result["runKey"] == "recovery-" + token
    await cron._dag._delete_run_batch(backend, "flow", _listed(keys))
    assert await cron._dag._read("flow", keys[0]) is not None


@pytest.mark.parametrize("failure", ["listing", "write"])
async def test_failed_dates_free_their_leases_when_no_write_is_in_flight(
    failure, dag_cron, monkeypatch
):
    # The listing under the leases times out, or the batch write fails.
    # Neither leaves a write that can still land.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    real_list = backend.list_documents
    real_mutate = backend.mutate_document
    listed = []

    async def listing(namespace, **kwargs):
        listed.append(namespace)
        if failure == "listing" and len(listed) == 2:
            raise asyncio.TimeoutError
        return await real_list(namespace, **kwargs)

    async def mutate(namespace, key, transform):
        if failure == "write" and namespace == "recoverybatch/flow":
            raise OSError("store went away")
        return await real_mutate(namespace, key, transform)

    with monkeypatch.context() as patch:
        patch.setattr(backend, "list_documents", listing)
        patch.setattr(backend, "mutate_document", mutate)
        with pytest.raises(
            asyncio.TimeoutError if failure == "listing" else OSError
        ):
            await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert listed == ["dagrun/flow"] * 2
    assert await backend.read_document("recoverybatch/flow", token) is None
    assert await _run_leases_free(cron, "flow", keys) == [True, True]


async def test_failed_dates_accept_the_batch_when_a_release_fails(
    dag_cron, monkeypatch
):
    # The lease of the first source cannot be released after the batch
    # write.  The batch stands, and that lease lapses with its TTL.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    stuck = cron._dag._lease_name(("flow", keys[0]))
    real = backend.release_lease
    kept = []

    async def release(lease):
        if lease.name == stuck:
            kept.append(lease)
            raise OSError("store went away")
        return await real(lease)

    with monkeypatch.context() as patch:
        patch.setattr(backend, "release_lease", release)
        with pytest.raises(recovery.RecoveryError, match="run is busy"):
            await cron._dag.recover_range(*_RANGE, plan_token=token)
    batch = await backend.read_document("recoverybatch/flow", token)
    assert batch["results"] == {} and not batch["complete"]
    assert await _run_leases_free(cron, "flow", keys) == [False, True]
    (lease,) = kept
    await backend.release_lease(lease)
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert result["complete"] and len(result["runs"]) == 2


async def test_failed_dates_repeat_a_batch_without_leasing_its_sources(
    dag_cron, monkeypatch
):
    # A batch that exists holds its sources by itself, and a complete one
    # answers after retention has deleted them.
    cron, keys = await _failed_dates(dag_cron)
    backend = cron.state_backend
    token = (await cron._dag.recover_range(*_RANGE))["planToken"]
    result = await cron._dag.recover_range(*_RANGE, plan_token=token)
    for key in keys:
        await backend.delete_document("dagrun/flow", key)
    leased = []
    real = backend.acquire_lease

    async def acquire(name, holder, ttl):
        leased.append(name)
        return await real(name, holder, ttl)

    monkeypatch.setattr(backend, "acquire_lease", acquire)
    again = await cron._dag.recover_range(*_RANGE, plan_token=token)
    assert again == result
    assert leased == []


async def test_missing_artifact_blocks_execution(dag_cron, tmp_path, monkeypatch):
    cron, key, source, _ = await failed_flow(dag_cron, tmp_path)
    await jobstate.artifact_put(cron.state_backend, dag.xcom_scope("flow", source["runId"]), "extract/data", b"x")
    preview = await cron._dag.recover("flow", key)
    async def missing(*args):
        return False
    monkeypatch.setattr(cron.state_backend, "blob_exists", missing)
    with pytest.raises(recovery.RecoveryError, match="artifact is missing"):
        await cron._dag.recover("flow", key, plan_token=preview["planToken"])


async def test_recovery_without_a_state_backend_is_unavailable(
    dag_cron, monkeypatch
):
    # Without a state backend no lookup can tell whether the workflow run
    # exists, so every entry point answers that the store is missing.
    cron, keys = await _failed_dates(dag_cron, days=(1,))
    scheduler = cron._dag
    token = (await scheduler.recover("flow", keys[0]))["planToken"]
    batch = (await scheduler.recover_range(*_RANGE))["planToken"]
    requests = {
        "plan": lambda: scheduler.recovery_plan("flow", keys[0]),
        "preview": lambda: scheduler.recover("flow", keys[0]),
        "execute": lambda: scheduler.recover(
            "flow", keys[0], plan_token=token
        ),
        "range preview": lambda: scheduler.recover_range(*_RANGE),
        "range execute": lambda: scheduler.recover_range(
            *_RANGE, plan_token=batch
        ),
        "absent workflow": lambda: scheduler.recover("absent", keys[0]),
        "absent workflow range": lambda: scheduler.recover_range(
            "absent", *_RANGE[1:]
        ),
    }
    unavailable = "recovery state is unavailable"
    with monkeypatch.context() as patch:
        patch.setattr(cron, "state_backend", None)
        for name, request in requests.items():
            with pytest.raises(recovery.RecoveryUnavailable) as raised:
                await request()
            assert raised.value.message == unavailable, name
    # with the store back, an absent workflow or run is a plain conflict
    requests["absent run"] = lambda: scheduler.recover("flow", "absent")
    conflicts = {
        "absent workflow": "workflow run not found",
        "absent run": "workflow run not found",
        "absent workflow range": "workflow not found",
    }
    for name, message in conflicts.items():
        with pytest.raises(recovery.RecoveryError) as raised:
            await requests[name]()
        assert type(raised.value) is recovery.RecoveryError, name
        assert raised.value.message == message, name


async def test_recovery_without_a_state_section_finds_no_workflow():
    # A workflow needs a ``state`` section, so a daemon without one has no
    # workflow for a request to name, and no store whose start would
    # change the answer.
    from cronstable.cron import Cron

    cron = Cron(
        None,
        config_yaml="jobs:\n  - name: plain\n    command: ignored\n"
        '    schedule: "@reboot"\n',
    )
    scheduler = cron._dag
    assert cron.state_backend is None and not cron._state_configured
    token = "0" * 64
    requests = {
        "plan": lambda: scheduler.recovery_plan("flow", "run"),
        "preview": lambda: scheduler.recover("flow", "run"),
        "execute": lambda: scheduler.recover("flow", "run", plan_token=token),
        "range preview": lambda: scheduler.recover_range(*_RANGE),
        "range execute": lambda: scheduler.recover_range(
            *_RANGE, plan_token=token
        ),
    }
    for name, request in requests.items():
        with pytest.raises(recovery.RecoveryError) as raised:
            await request()
        assert type(raised.value) is recovery.RecoveryError, name
        assert raised.value.message == (
            "workflow not found"
            if name.startswith("range")
            else "workflow run not found"
        ), name


async def test_recovery_of_a_loaded_workflow_waits_for_its_store(tmp_path):
    # The configuration is applied before its store starts.  The workflow
    # is known by then, so its recovery is unavailable until the store is.
    from cronstable.cron import Cron

    cron = Cron(
        None, config_yaml="state:\n  path: {}\n".format(tmp_path) + CONFIG
    )
    scheduler = cron._dag
    assert cron.state_backend is None and not cron._state_configured
    with pytest.raises(recovery.RecoveryUnavailable):
        await scheduler.recover("flow", "run")
    with pytest.raises(recovery.RecoveryUnavailable):
        await scheduler.recover("flow", "run", plan_token="0" * 64)
    with pytest.raises(recovery.RecoveryUnavailable):
        await scheduler.recover_range(*_RANGE)


async def test_retention_preserves_source_during_recovery_preparation(dag_cron, tmp_path, monkeypatch):
    cron, key, source, _ = await failed_flow(dag_cron, tmp_path)
    async def defer(*args):
        pass
    monkeypatch.setattr(cron._dag, "_try_own", defer)
    preview = await cron._dag.recover("flow", key)
    result = await cron._dag.recover("flow", key, plan_token=preview["planToken"])
    run = [(key, source["runId"], source["createdAt"])]
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


async def test_retention_keeps_a_recovery_run_that_is_accepted_again(
    dag_cron, monkeypatch
):
    # A recovery run takes its key and its run ID from the plan token.  A
    # peer's pass deletes the finished recovery run after this pass listed
    # it, and an operator executes the same preview again.  This pass keeps
    # the new run and its artifacts.
    cron, (source,) = await _failed_dates(dag_cron, days=(1,))
    backend = cron.state_backend
    config = cron.cron_dags["flow"]
    clock = [5000.0]
    monkeypatch.setattr(dagrun, "_now", lambda: clock[0])
    monkeypatch.setattr(config, "retain_runs", 1)
    token = (await cron._dag.recover("flow", source))["planToken"]
    scope = dag.xcom_scope("flow", token)

    def finished(body):
        body["state"] = dag.SUCCESS
        body["recovery"]["status"] = "ready"
        return body, None

    async def accept():
        result = await cron._dag.recover("flow", source, plan_token=token)
        assert result["created"]
        key = result["runKey"]
        await backend.mutate_document("dagrun/flow", key, finished)
        return key

    key = await accept()
    # a newer finished run, so the recovery run is over the retention bound
    newer = dag.new_run_body(
        dag="flow",
        run_key="newer",
        run_id="newer",
        logical_date=None,
        kind="manual",
        now=9000.0,
        spec=config.spec,
    )
    newer["state"] = dag.SUCCESS
    await backend.mutate_document(
        "dagrun/flow", "newer", lambda _: (newer, None)
    )
    real = backend.list_documents
    listed = []

    async def then_accept_again(namespace, **kwargs):
        docs = await real(namespace, **kwargs)
        listed.append(namespace)
        if listed == ["dagrun/flow"]:
            await backend.delete_document("dagrun/flow", key)
            clock[0] += 60.0
            assert await accept() == key
            await jobstate.artifact_put(backend, scope, "extract/data", b"x")
        return docs

    monkeypatch.setattr(backend, "list_documents", then_accept_again)
    await cron._dag._gc_one_dag(backend, "flow", config)
    monkeypatch.undo()
    assert await backend.list_document_keys("dagrun/flow") == ["newer", key]
    body = await cron._dag._read("flow", key)
    assert (body["runId"], body["createdAt"]) == (token, 5060.0)
    assert await jobstate.artifact_get(backend, scope, "extract/data")


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
