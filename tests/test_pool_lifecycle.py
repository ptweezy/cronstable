"""Cross-scheduler regressions for durable pool admission and completion."""

import asyncio
import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from cronstable.job import JobRetryState
from cronstable.pools import NAMESPACE, PoolError
from tests._helpers import _drain_pending, _reap_running
from tests.test_pools import CONFIG, make


SENSOR = (
    CONFIG
    + """
dags:
  - name: flow
    tasks:
      - id: task
        type: sensor
        pokeTimeoutSeconds: 10
        command: ignored
        pool: database
        poolSlots: 2
"""
)


async def wait_queued(cron):
    async def wait():
        while (await cron._pools.snapshot())[0]["queued"] == 0:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), 5)


async def test_full_queue_does_not_interrupt_due_slots(dag_cron, monkeypatch):
    cron = await make(
        dag_cron, monkeypatch, CONFIG.replace('"@reboot"', '"* * * * *"')
    )
    for _ in range(4):
        await cron.maybe_launch_job(cron.cron_jobs["one"])
    now = datetime.datetime.now(datetime.timezone.utc).replace(
        second=0, microsecond=0
    )
    for name in cron.cron_jobs:
        cron._set_next_fire(name, now)
    await cron._service_slots(False)
    assert all(cron._next_fire[name] > now for name in cron.cron_jobs)
    assert (await cron._pools.snapshot())[0]["queued"] == 4


@pytest.mark.parametrize(
    "error",
    [PoolError("draining"), OSError("offline"), asyncio.TimeoutError()],
)
async def test_scheduled_admission_failure_is_contained(
    dag_cron, monkeypatch, error
):
    cron = await make(dag_cron, monkeypatch)
    monkeypatch.setattr(
        cron._pools, "enqueue_job", AsyncMock(side_effect=error)
    )
    await cron.launch_scheduled_job(cron.cron_jobs["one"])
    assert not cron.running_jobs


async def test_catchup_waits_for_each_receipt_before_closing(
    dag_cron, monkeypatch
):
    cron = await make(
        dag_cron, monkeypatch, CONFIG.replace('"@reboot"', '"* * * * *"')
    )
    job = cron.cron_jobs["one"]
    job.onMissed = "run-all"
    job.concurrencyPolicy = "Allow"
    now = datetime.datetime.now(datetime.timezone.utc)
    watermark = (now - datetime.timedelta(minutes=3)).isoformat()
    monkeypatch.setattr(
        cron,
        "_missed_occurrences",
        AsyncMock(return_value=(3, watermark, False)),
    )
    checkpoint = AsyncMock()
    monkeypatch.setattr(cron, "_checkpoint_catchup", checkpoint)
    task = asyncio.create_task(cron._run_catch_up(job, 3, 0, now))
    try:
        for _ in range(3):
            await wait_queued(cron)
            checkpoint.assert_not_awaited()
            assert (await cron._pools.snapshot())[0]["queued"] == 1
            await cron._pools.tick()
            assert len(cron.running_jobs["one"]) == 1
            assert cron.running_jobs["one"][0].retry_state is None
            await _reap_running(cron)
        await asyncio.wait_for(task, 5)
        checkpoint.assert_awaited_once_with(job.name, "close", watermark)
        assert not cron.running_jobs
        assert (await cron._pools.snapshot())[0]["queued"] == 0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_catchup_shutdown_preserves_one_receipt(dag_cron, monkeypatch):
    cron = await make(
        dag_cron, monkeypatch, CONFIG.replace('"@reboot"', '"* * * * *"')
    )
    job = cron.cron_jobs["one"]
    job.onMissed = "run-all"
    now = datetime.datetime.now(datetime.timezone.utc)
    watermark = (now - datetime.timedelta(minutes=3)).isoformat()
    monkeypatch.setattr(
        cron,
        "_missed_occurrences",
        AsyncMock(return_value=(3, watermark, True)),
    )
    checkpoint = AsyncMock()
    monkeypatch.setattr(cron, "_checkpoint_catchup", checkpoint)
    task = asyncio.create_task(cron._run_catch_up(job, 3, 0, now))
    try:
        await wait_queued(cron)
        cron._stop_event.set()
        await asyncio.wait_for(task, 5)
        checkpoint.assert_not_awaited()
        other = await make(
            dag_cron, monkeypatch, CONFIG.replace('"@reboot"', '"* * * * *"')
        )
        other_job = other.cron_jobs["one"]
        other_job.onMissed = "run-all"
        key = other._pools.catchup_key(other_job, watermark, 0)
        await other._pools.enqueue_job(other_job, with_retries=False, key=key)
        assert (await other._pools.snapshot())[0]["queued"] == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("expire_entry", [False, True])
async def test_timed_out_sensor_releases_queue_and_backlog(
    dag_cron, monkeypatch, expire_entry
):
    cron = await make(dag_cron, monkeypatch, SENSOR)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    held = await cron._pools.acquire("database", entry["id"])
    key = await cron._dag.trigger_run("flow")
    await _drain_pending(cron)
    body = await cron._dag.get_run("flow", key)
    assert body["tasks"]["task"]["queued"]

    def expire_sensor(body):
        body["tasks"]["task"]["firstPokeAt"] = 1
        return body, None

    await cron._dag._mutate("flow", key, expire_sensor)
    await cron._dag.advance_one(("flow", key))
    await _drain_pending(cron)
    assert (await cron._dag.get_run("flow", key))["state"] == "failed"
    await cron._pools.finish(held)
    if expire_entry:

        def expire(body):
            for queued in body["entries"].values():
                if queued["payload"].get("kind") == "task":
                    queued["expiresAt"] = 0
            return body, None

        await cron.state_backend.mutate_document(NAMESPACE, "database", expire)
    await cron.maybe_launch_job(cron.cron_jobs["two"])
    await cron._pools.tick()
    pool = (await cron._pools.snapshot())[0]
    assert pool["used"] == 1 and pool["queued"] == 0
    body = await cron._pools._change("database", lambda body, now: body)
    retired = next(
        e
        for e in body["entries"].values()
        if e["payload"].get("kind") == "task"
    )
    assert retired["observed"]
    assert retired["state"] == ("expired" if expire_entry else "cancelled")
    await _reap_running(cron)


async def test_capacity_change_keeps_existing_task_admission(
    dag_cron, monkeypatch
):
    from tests.test_state_dag_run import _drive

    config = SENSOR.replace(
        "        type: sensor\n        pokeTimeoutSeconds: 10\n", ""
    )
    cron = await make(dag_cron, monkeypatch, config)
    cron.cron_dags["flow"].task_templates["task"].command = cron.cron_jobs[
        "one"
    ].command
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    held = await cron._pools.acquire("database", entry["id"])
    key = await cron._dag.trigger_run("flow")
    await _drain_pending(cron)
    cron.pool_config["database"]["slots"] = 3
    await cron._dag.advance_one(("flow", key))
    await _drain_pending(cron)
    body = await cron._dag.get_run("flow", key)
    assert body["tasks"]["task"]["state"] == "pending"
    assert body["tasks"]["task"]["attempt"] == 0
    await cron._pools.finish(held)
    assert (await _drive(cron, "flow", key))["state"] == "success"
    await cron._pools.enqueue(cron.cron_jobs["two"])
    assert (await cron._pools.snapshot())[0]["slots"] == 3


@pytest.mark.parametrize("manual", [False, True])
async def test_node_bound_entries_only_run_on_origin(
    dag_cron, monkeypatch, manual
):
    config = (
        CONFIG
        if manual
        else CONFIG.replace(
            "    pool: database",
            "    pool: database\n    clusterPolicy: EveryNode",
        )
    )
    a = await make(dag_cron, monkeypatch, config)
    b = await make(dag_cron, monkeypatch, config)
    a._state_host, b._state_host = "node-a", "node-b"
    first = await a._pools.enqueue_job(a.cron_jobs["one"], manual=manual)
    second = await b._pools.enqueue_job(b.cron_jobs["one"], manual=manual)
    assert await a._pools.acquire("database", second["id"]) is None
    assert await b._pools.acquire("database", first["id"]) is None
    await a._pools.tick()
    assert len(a.running_jobs["one"]) == 1
    assert not b.running_jobs
    await b._pools.tick()
    assert len(b.running_jobs["one"]) == 1
    await _reap_running(a)
    await _reap_running(b)


@pytest.mark.parametrize("reason", ["superseded", "succeeded", "cancelled"])
async def test_settled_retry_cannot_reappear_after_restart(
    dag_cron, monkeypatch, reason
):
    a = await make(dag_cron, monkeypatch)
    job = a.cron_jobs["one"]
    state = JobRetryState(60, 2, 120)
    state.next_delay()
    a.retry_state["one"] = state
    await a.maybe_launch_job(job)
    # Cancellation must also find the queue when memory was lost at restart.
    b = await make(dag_cron, monkeypatch)
    await b.cancel_job_retries("one", settle=reason)
    await b._pools.tick()
    assert not b.running_jobs
    assert (await b._pools.snapshot())[0]["queued"] == 0
    assert not await b._pools.retry_current(state.pool_retry)


async def test_new_schedule_runs_without_reviving_older_retry(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.onFailure["retry"]["maximumRetries"] = 3
    old = JobRetryState(60, 2, 120)
    old.next_delay()
    cron.retry_state["one"] = old
    await cron.maybe_launch_job(job)
    await cron.launch_scheduled_job(job)
    assert old.cancelled
    await cron._pools.tick()
    assert len(cron.running_jobs["one"]) == 1
    assert cron.running_jobs["one"][0].retry_state.count == 0
    await _reap_running(cron)


async def test_full_pool_defers_retry_without_consuming_it(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    for _ in range(4):
        await cron._pools.enqueue(cron.cron_jobs["two"])
    state = JobRetryState(60, 2, 120)
    state.next_delay()
    cron.retry_state["one"] = state
    assert not await cron._retry_consume_ok("one", 1, quiet=False)
    assert state.count == 1 and not state.cancelled
    entries = (await cron._pools.snapshot())[0]["entries"]
    await cron._pools.cancel("database", entries[0]["id"])
    assert await cron._retry_consume_ok("one", 1, quiet=False)
    assert state.count == 1
    assert (await cron._pools.snapshot())[0]["queued"] == 4


async def test_superseded_initial_run_keeps_its_work_but_not_its_retry(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.onFailure["retry"]["maximumRetries"] = 3
    await cron.launch_scheduled_job(job)
    await cron.launch_scheduled_job(job)
    await cron._pools.tick()
    assert len(cron.running_jobs["one"]) == 2
    assert [r.retry_state is None for r in cron.running_jobs["one"]] == [
        True,
        False,
    ]
    await _reap_running(cron)


async def test_cancellation_rechecks_a_retry_already_claimed_by_a_peer(
    dag_cron, monkeypatch
):
    a = await make(dag_cron, monkeypatch)
    b = await make(dag_cron, monkeypatch)
    state = JobRetryState(1, 2, 60)
    state.next_delay()
    a.retry_state["one"] = state
    entry = await a._pools.enqueue_job(a.cron_jobs["one"])
    ticket = await b._pools.acquire("database", entry["id"])
    assert ticket is not None
    await a.cancel_job_retries("one")
    assert not await b.maybe_launch_job(b.cron_jobs["one"], pool_ticket=ticket)
    assert not b.running_jobs
    assert (await b._pools.snapshot())[0]["used"] == 0


async def test_every_node_settlement_preserves_the_other_nodes_retry(
    dag_cron, monkeypatch
):
    config = CONFIG.replace(
        "    pool: database",
        "    pool: database\n    clusterPolicy: EveryNode",
    )
    a = await make(dag_cron, monkeypatch, config)
    b = await make(dag_cron, monkeypatch, config)
    a._state_host, b._state_host = "node-a", "node-b"
    for cron in (a, b):
        state = JobRetryState(1, 2, 60)
        state.next_delay()
        cron.retry_state["one"] = state
        await cron._pools.enqueue_job(cron.cron_jobs["one"])
    await a.cancel_job_retries("one")
    await b._pools.tick()
    assert len(b.running_jobs["one"]) == 1
    assert b.running_jobs["one"][0].retry_state.count == 1
    await _reap_running(b)


async def test_retry_admission_survives_restart_without_duplicate_attempt(
    dag_cron, monkeypatch
):
    config = CONFIG.replace('"@reboot"', '"* * * * *"')
    a = await make(dag_cron, monkeypatch, config)
    b = await make(dag_cron, monkeypatch, config)
    a.cron_jobs["one"].onFailure["retry"]["maximumRetries"] = 3
    state = JobRetryState(1, 2, 60)
    a.retry_state["one"] = state
    initial = await a._pools.enqueue_job(a.cron_jobs["one"])
    await a._pools.cancel("database", initial["id"])
    state.next_delay()
    await a.schedule_retry_job("one", 0, 1)
    await _drain_pending(a)
    assert (await a._pools.snapshot())[0]["queued"] == 1
    job = b.cron_jobs["one"]
    job.onFailure["retry"]["maximumRetries"] = 3
    await b._rearm_pending_retry("one", job)
    restored = b.retry_state["one"]
    assert restored.pool_retry == state.pool_retry
    await asyncio.wait_for(restored.task, 5)
    assert (await b._pools.snapshot())[0]["queued"] == 1
    launched = Mock()
    monkeypatch.setattr(b.metrics, "job_retry_launched", launched)
    await b._pools.tick()
    launched.assert_called_once_with("one")
    assert len(b.running_jobs["one"]) == 1
    await _reap_running(b)


async def test_deferred_settlement_retries_before_dispatch(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    state = JobRetryState(1, 2, 60)
    state.next_delay()
    cron.retry_state["one"] = state
    await cron._pools.enqueue_job(cron.cron_jobs["one"])
    with monkeypatch.context() as patch:
        patch.setattr(
            cron._pools, "_change", AsyncMock(side_effect=OSError("offline"))
        )
        await cron.cancel_job_retries("one")
    assert cron._pools._retry_settlements
    await cron._pools.tick()
    assert not cron._pools._retry_settlements
    assert not cron.running_jobs
    assert (await cron._pools.snapshot())[0]["queued"] == 0


async def test_late_settlement_does_not_cancel_a_new_generation(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.onFailure["retry"]["maximumRetries"] = 3
    original = cron._pools._change
    callbacks = []

    async def timeout_after_write(pool, action, **kwargs):
        await original(pool, action, **kwargs)
        callbacks.append((pool, action))
        raise asyncio.TimeoutError()

    with monkeypatch.context() as patch:
        patch.setattr(cron._pools, "_change", timeout_after_write)
        await cron.cancel_job_retries("one")
    await cron.launch_scheduled_job(job)
    current = cron.retry_state["one"]
    for pool, action in callbacks:
        await original(pool, action)
    assert await cron._pools.retry_current(current.pool_retry)
    await cron._pools.tick()
    assert cron.running_jobs["one"][0].retry_state is not None
    await _reap_running(cron)


async def test_unavailable_task_document_does_not_retire_live_work(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch, SENSOR)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    held = await cron._pools.acquire("database", entry["id"])
    await cron._dag.trigger_run("flow")
    await _drain_pending(cron)
    with monkeypatch.context() as patch:
        patch.setattr(
            cron._dag, "_read", AsyncMock(side_effect=OSError("offline"))
        )
        await cron._pools.tick()
    assert (await cron._pools.snapshot())[0]["queued"] == 1
    await cron._pools.finish(held)


@pytest.mark.parametrize("state", ["cancelled", "expired", "finished"])
async def test_catchup_resume_reuses_finished_work_only(
    dag_cron, monkeypatch, state
):
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    key = cron._pools.catchup_key(job, "2026-01-01T00:00:00+00:00", 0)
    await cron._pools.enqueue_job(job, with_retries=False, key=key)
    ticket = await cron._pools.acquire("database", key)
    await cron._pools.finish(ticket, state)
    entry = await cron._pools.enqueue_job(
        job, with_retries=False, key=key, resume=True
    )
    assert entry["state"] == ("finished" if state == "finished" else "queued")


@pytest.mark.parametrize(
    "error", [OSError("offline"), asyncio.CancelledError()]
)
async def test_interrupted_completion_releases_capacity_without_finishing_receipt(
    dag_cron, monkeypatch, error
):
    cron = await make(dag_cron, monkeypatch)
    entry = await cron._pools.enqueue_job(cron.cron_jobs["one"])
    ticket = await cron._pools.acquire("database", entry["id"])
    monkeypatch.setattr(
        cron, "_record_finished_job", AsyncMock(side_effect=error)
    )
    with pytest.raises(type(error)):
        await cron._handle_finished_job(Mock(pool_ticket=ticket))
    assert (await cron._pools.snapshot())[0]["used"] == 0
    assert not await cron._pools.wait_finished("database", entry["id"])
