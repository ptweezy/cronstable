import asyncio
import sys

import pytest

from cronstable.config import ConfigError, _validate_cross_sections, parse_config_string
from cronstable.pools import NAMESPACE, PoolError
from cronstable.job import JobRetryState
from tests._helpers import _drain_pending, _reap_running


CONFIG = '''
pools:
  database:
    slots: 2
    maxQueued: 4
jobs:
  - name: one
    command: ignored
    schedule: "@reboot"
    pool: database
  - name: two
    command: ignored
    schedule: "@reboot"
    pool: database
'''


async def make(factory, monkeypatch, config=CONFIG):
    cron = await factory(config)
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    for job in cron.cron_jobs.values():
        job.command = [sys.executable, "-c", "pass"]
    return cron


async def test_weighted_pool_limits_different_jobs(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    one.poolSlots = 2
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    assert not cron.running_jobs
    await cron._pools.tick()
    assert set(cron.running_jobs) == {"one"}
    state = (await cron._pools.snapshot())[0]
    assert state["used"] == 2 and state["queued"] == 1
    await _reap_running(cron)
    await cron._pools.tick()
    assert set(cron.running_jobs) == {"two"}
    await _reap_running(cron)


async def test_two_daemons_cannot_claim_same_ticket(dag_cron, monkeypatch):
    a = await make(dag_cron, monkeypatch)
    b = await make(dag_cron, monkeypatch)
    entry = await a._pools.enqueue(a.cron_jobs["one"])
    tickets = await asyncio.gather(
        a._pools.acquire("database", entry["id"]),
        b._pools.acquire("database", entry["id"]),
    )
    assert sum(t is not None for t in tickets) == 1
    for cron, ticket in zip((a, b), tickets):
        if ticket:
            await cron._pools.finish(ticket)


async def test_queue_survives_new_daemon_and_honors_priority(dag_cron, monkeypatch):
    first = await make(dag_cron, monkeypatch)
    one, two = first.cron_jobs.values()
    two.queuePriority = 10
    await first.maybe_launch_job(one)
    await first.maybe_launch_job(two)
    second = await make(dag_cron, monkeypatch)
    second.cron_jobs["two"].queuePriority = 10
    entries = (await second._pools.snapshot())[0]["entries"]
    high = next(e for e in entries if e["job"] == "two")
    low = next(e for e in entries if e["job"] == "one")
    assert await second._pools.acquire("database", low["id"]) is None
    ticket = await second._pools.acquire("database", high["id"])
    assert ticket is not None
    await second._pools.finish(ticket)


async def test_queue_expiration_and_cancel_are_durable(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    one = await cron._pools.enqueue(cron.cron_jobs["one"])
    two = await cron._pools.enqueue(cron.cron_jobs["two"])
    await cron._pools.cancel("database", one["id"])

    def expire(body):
        body["entries"][two["id"]]["expiresAt"] = 0
        return body, None

    await cron.state_backend.mutate_document(NAMESPACE, "database", expire)
    entries = (await cron._pools.snapshot())[0]["entries"]
    assert {e["state"] for e in entries} == {"cancelled", "expired"}
    assert await cron._pools.acquire("database", one["id"]) is None


async def test_admission_breaks_priority_and_time_ties_by_id(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    for key in ("z", "b", "a", "low"):
        await cron._pools.enqueue(cron.cron_jobs["one"], key=key)

    def arrange(body):
        for key, entry in body["entries"].items():
            entry["priority"] = 0 if key == "low" else 10
            entry["queuedAt"] = 1 if key in ("z", "low") else 2
        return body, None

    await cron.state_backend.mutate_document(NAMESPACE, "database", arrange)
    for key in ("z", "a", "b", "low"):
        if key != "low":
            assert await cron._pools.acquire("database", "low") is None
        ticket = await cron._pools.acquire("database", key)
        assert ticket is not None
        await cron._pools.finish(ticket)


@pytest.mark.parametrize("limit", [0, 1, 32, 200, None])
def test_waiting_selection_preserves_queue_order(limit):
    import random

    from cronstable.pools import _waiting

    rng = random.Random(42)
    entries = [
        {
            "id": str(i),
            "priority": rng.randrange(-3, 4),
            "queuedAt": rng.randrange(5),
            "state": rng.choice(["queued", "running", "finished"]),
        }
        for i in range(100)
    ]
    expected = sorted(
        (e for e in entries if e["state"] == "queued"),
        key=lambda e: (-e["priority"], e["queuedAt"], e["id"]),
    )[:limit]
    assert (
        _waiting({"entries": {e["id"]: e for e in entries}}, limit) == expected
    )
    assert _waiting({"entries": {}}, limit) == []


async def test_pool_queue_bound(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    for _ in range(4):
        await cron._pools.enqueue(cron.cron_jobs["one"])
    with pytest.raises(PoolError, match="full"):
        await cron._pools.enqueue(cron.cron_jobs["one"])


async def test_dag_tasks_wait_without_consuming_attempts(dag_cron, monkeypatch):
    from tests.test_state_dag_run import _drive

    cron = await make(dag_cron, monkeypatch, CONFIG + '''
dags:
  - name: flow
    tasks:
      - id: task
        command: ignored
        pool: database
        poolSlots: 2
''')
    template = cron.cron_dags["flow"].task_templates["task"]
    template.command = [sys.executable, "-c", "pass"]
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    held = await cron._pools.acquire("database", entry["id"])
    key = await cron._dag.trigger_run("flow")
    await _drain_pending(cron)
    body = await cron._dag.get_run("flow", key)
    assert body["tasks"]["task"]["state"] == "pending"
    assert body["tasks"]["task"]["attempt"] == 0
    assert body["tasks"]["task"]["queued"]["pool"] == "database"
    await cron._pools.finish(held)
    body = await _drive(cron, "flow", key)
    assert body["state"] == "success"
    assert body["tasks"]["task"]["attempt"] == 0


@pytest.mark.parametrize("yaml", [
    CONFIG,
    'state:\n  path: ./state\n' + CONFIG.replace("pool: database", "pool: missing"),
    'state:\n  path: ./state\n' + CONFIG.replace("slots: 2", "slots: 0"),
    'state:\n  path: ./state\n' + CONFIG.replace("pool: database", "pool: database\n    poolSlots: 3"),
])
def test_invalid_pool_configuration(yaml):
    with pytest.raises(ConfigError):
        _validate_cross_sections(parse_config_string(yaml, ""))


async def test_forbid_keeps_waiting_work(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.concurrencyPolicy = "Forbid"
    await cron.maybe_launch_job(job)
    await cron.maybe_launch_job(job)
    await cron._pools.tick()
    assert len(cron.running_jobs["one"]) == 1
    assert (await cron._pools.snapshot())[0]["queued"] == 1
    await _reap_running(cron)
    await cron._pools.tick()
    assert len(cron.running_jobs["one"]) == 1
    await _reap_running(cron)


async def test_lease_loss_prevents_launch_and_cancels_running_work(dag_cron, monkeypatch):
    from unittest.mock import AsyncMock
    cron = await make(dag_cron, monkeypatch)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    ticket = await cron._pools.acquire("database", entry["id"])
    running = AsyncMock()
    running.stopped = False
    ticket.running = running
    async def unavailable(*args, **kwargs):
        raise OSError("offline")
    monkeypatch.setattr(cron._pools, "_change", unavailable)
    await cron._pools._renew(ticket)
    running.cancel.assert_awaited_once()
    with pytest.raises(PoolError, match="lost"):
        cron._pools.check_ticket(ticket)


async def test_renewal_racing_a_completion_leaves_the_run_alone(
    dag_cron, monkeypatch
):
    from unittest.mock import AsyncMock

    cron = await make(dag_cron, monkeypatch)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    ticket = await cron._pools.acquire("database", entry["id"])
    running = AsyncMock()
    running.stopped = False
    ticket.running = running
    change = cron._pools._change
    completed = asyncio.Event()

    async def renew_after_completion(pool, action, **kwargs):
        # hold the renewal's read until the completion has been written
        if action.__name__ == "renew":
            await completed.wait()
        return await change(pool, action, **kwargs)

    monkeypatch.setattr(cron._pools, "_change", renew_after_completion)
    renewal = asyncio.create_task(cron._pools._renew(ticket))
    await asyncio.sleep(0)
    await cron._pools.finish(ticket)
    completed.set()
    await renewal
    running.cancel.assert_not_awaited()
    entries = (await cron._pools.snapshot())[0]["entries"]
    assert [e["state"] for e in entries] == ["finished"]


async def test_lost_lease_leaves_an_ended_run_alone(dag_cron, monkeypatch):
    from unittest.mock import AsyncMock

    cron = await make(dag_cron, monkeypatch)
    await cron.maybe_launch_job(cron.cron_jobs["one"])
    await cron._pools.tick()
    (running,) = cron.running_jobs["one"]
    ticket = running.pool_ticket
    assert ticket.running is running and not running.stopped
    await running.wait()
    assert running.stopped

    async def unavailable(*args, **kwargs):
        raise OSError("offline")

    cancel = AsyncMock()
    with monkeypatch.context() as patch:
        patch.setattr(cron._pools, "_change", unavailable)
        patch.setattr(running, "cancel", cancel)
        await cron._pools._renew(ticket)
    cancel.assert_not_awaited()
    assert not ticket.valid
    await cron._handle_finished_job(running)
    assert not cron._pools.held


async def test_queued_retry_retains_position_after_restart(dag_cron, monkeypatch):
    a = await make(dag_cron, monkeypatch)
    state = JobRetryState(5, 2, 60)
    state.next_delay()
    state.next_delay()
    a.retry_state["one"] = state
    await a.maybe_launch_job(a.cron_jobs["one"])
    b = await make(dag_cron, monkeypatch)
    await b._pools.tick()
    restored = b.running_jobs["one"][0].retry_state
    assert restored.count == 2 and restored.delay == 20
    await _reap_running(b)


async def test_capacity_change_drains_existing_queue(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    cron.pool_config["database"]["slots"] = 1
    with pytest.raises(PoolError, match="draining"):
        await cron._pools.enqueue(cron.cron_jobs["two"])
    ticket = await cron._pools.acquire("database", entry["id"])
    assert ticket is not None
    await cron._pools.finish(ticket)
    await cron._pools.enqueue(cron.cron_jobs["two"])
    assert (await cron._pools.snapshot())[0]["slots"] == 1


async def _fill_pool(cron):
    """Hold both slots of the two-slot pool; return the tickets."""
    tickets = []
    for _ in range(2):
        entry = await cron._pools.enqueue(cron.cron_jobs["one"])
        tickets.append(await cron._pools.acquire("database", entry["id"]))
    assert all(tickets)
    return tickets


def _count_admission_attempts(cron, monkeypatch):
    attempts = []
    acquire = cron._pools.acquire

    async def counting(pool, key):
        attempts.append(key)
        return await acquire(pool, key)

    monkeypatch.setattr(cron._pools, "acquire", counting)
    return attempts


async def test_full_pool_tick_makes_no_admission_attempt(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    tickets = await _fill_pool(cron)
    for job in (one, two, two):
        await cron.maybe_launch_job(job)
    attempts = _count_admission_attempts(cron, monkeypatch)
    await cron._pools._tick_pool("database")
    assert attempts == []
    assert not cron.running_jobs
    assert (await cron._pools.snapshot())[0]["queued"] == 3
    for ticket in tickets:
        await cron._pools.finish(ticket)


async def test_tick_stops_trying_when_the_last_slot_is_taken(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    for job in (one, two, one):
        await cron.maybe_launch_job(job)
    waiting = (await cron._pools.snapshot())[0]["entries"]
    attempts = _count_admission_attempts(cron, monkeypatch)
    await cron._pools._tick_pool("database")
    assert attempts == [e["id"] for e in waiting[:2]]
    assert {k: len(v) for k, v in cron.running_jobs.items()} == {
        "one": 1,
        "two": 1,
    }
    await _reap_running(cron)
    # the entry left waiting is admitted once capacity frees
    await cron._pools._tick_pool("database")
    assert attempts[2:] == [waiting[2]["id"]]
    assert len(cron.running_jobs["one"]) == 1
    await _reap_running(cron)


async def test_tick_still_tries_an_entry_that_fits_behind_one_that_does_not(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    held = await cron._pools.acquire(
        "database", (await cron._pools.enqueue(one))["id"]
    )
    two.poolSlots = 2
    await cron.maybe_launch_job(two)
    await cron.maybe_launch_job(one)
    wide, narrow = (await cron._pools.snapshot())[0]["entries"][1:]
    assert (wide["slots"], narrow["slots"]) == (2, 1)
    attempts = _count_admission_attempts(cron, monkeypatch)
    await cron._pools._tick_pool("database")
    # one slot is free: the two-slot entry cannot fit, the one-slot entry
    # can, and admission order still belongs to acquire()
    assert attempts == [narrow["id"]]
    assert not cron.running_jobs
    await cron._pools.finish(held)
    await cron._pools._tick_pool("database")
    assert set(cron.running_jobs) == {"two"}
    await _reap_running(cron)
    await cron._pools._tick_pool("database")
    await _reap_running(cron)


async def test_declined_launch_returns_its_slots_to_the_tick(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    held = await cron._pools.acquire(
        "database", (await cron._pools.enqueue(one))["id"]
    )
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    first, second = (await cron._pools.snapshot())[0]["entries"][1:]
    attempts = _count_admission_attempts(cron, monkeypatch)

    async def decline(job, **kwargs):
        return False

    with monkeypatch.context() as patch:
        patch.setattr(cron, "maybe_launch_job", decline)
        await cron._pools._tick_pool("database")
    assert attempts == [first["id"], second["id"]]
    assert (await cron._pools.snapshot())[0]["queued"] == 2
    await cron._pools.finish(held)
    for key in (first["id"], second["id"]):
        await cron._pools.cancel("database", key)


async def test_full_pool_tick_still_retires_dead_entries(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    tickets = await _fill_pool(cron)
    state = JobRetryState(5, 2, 60)
    state.next_delay()
    state.pool_retry = {
        "pool": "database",
        "scope": cron._pools._retry_scope("one", None),
        "generation": "an older ladder",
    }
    cron.retry_state["one"] = state
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    two.enabled = False
    await cron._pools._tick_pool("database")
    entries = (await cron._pools.snapshot())[0]["entries"]
    reasons = {e["job"]: e.get("reason") for e in entries[2:]}
    assert reasons == {
        "one": "retry superseded",
        "two": "job removed, disabled, or configuration changed",
    }
    assert {e["state"] for e in entries[2:]} == {"cancelled"}
    for ticket in tickets:
        await cron._pools.finish(ticket)
