import asyncio
import errno
import sys

import pytest

from cronstable import pools as pools_mod
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


# every way that the state of a pool can fail to answer
POOL_OUTAGES = (
    "store-error",
    "timeout",
    "unreadable-document",
    "misshapen-document",
    "no-backend",
)


async def break_pool_state(cron, patch, outage):
    """Make each read and write of the ``database`` pool fail as ``outage``,
    for as long as ``patch`` lasts.  A damaged document stays damaged after
    it."""
    backend = cron.state_backend
    if outage == "no-backend":
        patch.setattr(cron, "state_backend", None)
    elif outage == "unreadable-document":
        # a first read writes the document, so that there is one to damage
        await cron._pools.snapshot()
        _lock, path = backend._doc_paths(NAMESPACE, "database")
        with open(path, "wb") as document:
            document.write(b"{not json")
    elif outage == "misshapen-document":
        # a readable document whose body is not a pool's

        def other_body(current):
            return {"slots": 1}, None

        await backend.mutate_document(NAMESPACE, "database", other_body)
    elif outage == "timeout":

        async def hang(*args, **kwargs):
            await asyncio.Event().wait()

        patch.setattr(backend, "mutate_document", hang)
        patch.setattr(pools_mod, "OP_TIMEOUT", 0.01)
    else:
        assert outage == "store-error"

        async def offline(*args, **kwargs):
            raise OSError(errno.EIO, "Input/output error")

        patch.setattr(backend, "mutate_document", offline)


async def test_daemon_without_a_state_section_has_no_pool_to_change():
    # A pool needs a ``state`` section, so a daemon without one has no
    # pool for a request to name, and no store whose start would change
    # the answer.
    from cronstable.cron import Cron

    cron = Cron(
        None,
        config_yaml="jobs:\n  - name: plain\n    command: ignored\n"
        '    schedule: "@reboot"\n',
    )
    assert cron.state_backend is None and not cron._state_configured
    with pytest.raises(PoolError) as raised:
        await cron._pools.cancel("database", "absent")
    assert type(raised.value) is PoolError
    assert raised.value.message == "unknown pool 'database'"


async def test_loaded_pool_is_unavailable_until_its_store_starts(tmp_path):
    # The configuration is applied before its store starts, and the pool
    # is known by then.
    from cronstable.cron import Cron

    cron = Cron(
        None, config_yaml="state:\n  path: {}\n".format(tmp_path) + CONFIG
    )
    assert cron.state_backend is None and not cron._state_configured
    with pytest.raises(pools_mod.PoolUnavailable):
        await cron._pools.cancel("database", "absent")


async def test_absent_pool_is_unavailable_while_its_store_is_down(
    dag_cron, monkeypatch
):
    # A pool that left the configuration keeps its queue in the store, so
    # an absent name is unknown only where no store is configured.
    cron = await make(dag_cron, monkeypatch)
    assert cron._state_configured and "absent" not in cron.pool_config
    with monkeypatch.context() as patch:
        patch.setattr(cron, "state_backend", None)
        with pytest.raises(pools_mod.PoolUnavailable):
            await cron._pools.cancel("absent", "absent")


async def test_weighted_pool_limits_different_jobs(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    one.poolSlots = 2
    # a higher priority puts the two-slot job at the head for any enqueue
    # timestamps
    one.queuePriority = 1
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


async def test_lease_loss_prevents_launch_and_cancels_running_work(
    dag_cron, monkeypatch, caplog
):
    from unittest.mock import AsyncMock
    cron = await make(dag_cron, monkeypatch)
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    ticket = await cron._pools.acquire("database", entry["id"])
    running = AsyncMock()
    running.stopped = False
    running.ended = False
    ticket.running = running
    async def unavailable(*args, **kwargs):
        raise OSError("offline")
    monkeypatch.setattr(cron._pools, "_change", unavailable)
    with caplog.at_level("ERROR", logger="cronstable.pools"):
        await cron._pools._renew(ticket)
    running.cancel.assert_awaited_once()
    assert "pool lease lost for " + ticket.key in caplog.text
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
    running.ended = False
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


async def test_lost_lease_leaves_a_run_that_never_started_alone(
    dag_cron, monkeypatch, caplog
):
    from unittest.mock import AsyncMock

    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.command = ["cronstable-no-such-binary-xyz"]
    await cron.maybe_launch_job(job)
    await cron._pools.tick()
    (running,) = cron.running_jobs["one"]
    ticket = running.pool_ticket
    assert ticket.running is running
    assert running.start_failed and not running.stopped

    async def unavailable(*args, **kwargs):
        raise OSError("offline")

    cancel = AsyncMock()
    with monkeypatch.context() as patch:
        patch.setattr(cron._pools, "_change", unavailable)
        patch.setattr(running, "cancel", cancel)
        with caplog.at_level("ERROR", logger="cronstable.pools"):
            await cron._pools._renew(ticket)
    cancel.assert_not_awaited()
    assert "pool lease lost" not in caplog.text
    assert not ticket.valid
    await _reap_running(cron)
    assert not cron._pools.held


async def test_late_completion_retry_leaves_the_reclaimed_ticket_held(
    dag_cron, monkeypatch
):
    # A tick and a heartbeat both retry a pending completion.  The retry
    # that lands last finds the key claimed again.
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    await cron.maybe_launch_job(cron.cron_jobs["one"])
    (entry,) = (await pools.snapshot())[0]["entries"]
    key = ("database", entry["id"])
    stale = await pools.acquire(*key)
    change = pools._change

    async def unavailable(*args, **kwargs):
        raise OSError("offline")

    with monkeypatch.context() as patch:
        patch.setattr(pools, "_change", unavailable)
        await pools.finish(stale, "queued", "launch interrupted")
    assert stale.completion is not None and pools.held[key] is stale
    launched = []

    async def launch(job, **kwargs):
        launched.append(kwargs["pool_ticket"])
        return True

    ticked = asyncio.Event()

    async def heartbeat_lands_last(pool, action, **kwargs):
        # hold the heartbeat's retry until the tick has claimed the key
        if asyncio.current_task() is heartbeat:
            await ticked.wait()
        return await change(pool, action, **kwargs)

    monkeypatch.setattr(cron, "maybe_launch_job", launch)
    monkeypatch.setattr(pools, "_change", heartbeat_lands_last)
    heartbeat = asyncio.create_task(pools._renew(stale))
    await asyncio.sleep(0)
    await pools.tick()
    ticked.set()
    await heartbeat
    (ticket,) = launched
    assert ticket is not stale
    assert pools.held.get(key) is ticket
    await pools.finish(ticket)


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


def test_queue_rule_admits_the_head_into_free_slots():
    # The one statement of the queue rule.  A claim asks it of the stored
    # pool, and a tick asks it of the copy that it read.
    from cronstable.pools import _Queue

    def entry(key, state, slots=1, priority=0, at=0.0):
        return {
            "id": key,
            "state": state,
            "slots": slots,
            "priority": priority,
            "queuedAt": at,
        }

    entries = {
        e["id"]: e
        for e in (
            entry("run", "running"),
            entry("wide", "queued", slots=3, at=1.0),
            entry("narrow", "queued", at=2.0),
            entry("urgent", "queued", priority=5, at=3.0),
        )
    }
    queue = _Queue({"slots": 3, "entries": entries})
    # the entry of highest priority is the head, and nothing behind it
    # starts
    assert queue.admits(entries["urgent"])
    assert not queue.admits(entries["narrow"])
    queue.left(entries["urgent"], running=True)
    # the next head needs three slots and one is free: it holds back the
    # entry behind it, which would fit
    assert queue.free == 1
    assert not queue.admits(entries["wide"])
    assert not queue.admits(entries["narrow"])
    queue.left(entries["wide"])
    assert queue.admits(entries["narrow"])
    queue.left(entries["narrow"], running=True)
    assert queue.head is None and queue.free == 0


async def test_forbid_admits_past_an_instance_that_has_ended(
    dag_cron, monkeypatch
):
    # An instance that has ended stays in running_jobs until the reaper
    # records it.  It holds no Forbid job back from the pool.
    cron = await make(dag_cron, monkeypatch)
    job = cron.cron_jobs["one"]
    job.concurrencyPolicy = "Forbid"
    await cron.maybe_launch_job(job)
    await cron._pools.tick()
    (first,) = cron.running_jobs["one"]
    await first.wait()
    assert first.ended
    await cron.maybe_launch_job(job)
    await cron._pools.tick()
    assert len(cron.running_jobs["one"]) == 2
    await _reap_running(cron)


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
    claim = cron._pools._claim

    async def counting(pool, key):
        attempts.append(key)
        return await claim(pool, key)

    monkeypatch.setattr(cron._pools, "_claim", counting)
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
    # the queue orders entries of one enqueue timestamp by their random
    # IDs, so each entry's job comes from the snapshot
    waiting = (await cron._pools.snapshot())[0]["entries"]
    attempts = _count_admission_attempts(cron, monkeypatch)

    def running():
        return sorted(
            run.config.name
            for runs in cron.running_jobs.values()
            for run in runs
        )

    await cron._pools._tick_pool("database")
    assert attempts == [e["id"] for e in waiting[:2]]
    assert running() == sorted(e["job"] for e in waiting[:2])
    await _reap_running(cron)
    # the entry left waiting is admitted once capacity frees
    await cron._pools._tick_pool("database")
    assert attempts[2:] == [waiting[2]["id"]]
    assert running() == [waiting[2]["job"]]
    await _reap_running(cron)


async def test_tick_tries_nothing_behind_a_head_that_does_not_fit(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    held = await cron._pools.acquire(
        "database", (await cron._pools.enqueue(one))["id"]
    )
    two.poolSlots = 2
    # a higher priority puts the two-slot entry at the head for any enqueue
    # timestamps
    two.queuePriority = 1
    await cron.maybe_launch_job(two)
    await cron.maybe_launch_job(one)
    wide, narrow = (await cron._pools.snapshot())[0]["entries"][1:]
    assert (wide["slots"], narrow["slots"]) == (2, 1)
    attempts = _count_admission_attempts(cron, monkeypatch)
    await cron._pools._tick_pool("database")
    # one slot is free and the one-slot entry would fit it, but acquire()
    # admits the queue head alone, and the two-slot head cannot fit
    assert attempts == []
    assert not cron.running_jobs
    await cron._pools.finish(held)
    await cron._pools._tick_pool("database")
    assert attempts == [wide["id"]]
    assert set(cron.running_jobs) == {"two"}
    await _reap_running(cron)
    await cron._pools._tick_pool("database")
    assert attempts == [wide["id"], narrow["id"]]
    await _reap_running(cron)


async def test_tick_still_retires_a_dead_entry_behind_a_waiting_head(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    one, two = cron.cron_jobs.values()
    held = await cron._pools.acquire(
        "database", (await cron._pools.enqueue(one))["id"]
    )
    two.poolSlots = 2
    # a higher priority puts the two-slot entry at the head for any enqueue
    # timestamps
    two.queuePriority = 1
    await cron.maybe_launch_job(two)
    state = JobRetryState(5, 2, 60)
    state.next_delay()
    state.pool_retry = {
        "pool": "database",
        "scope": cron._pools._retry_scope("one", None),
        "generation": "an older ladder",
    }
    cron.retry_state["one"] = state
    await cron.maybe_launch_job(one)
    wide, retry = (await cron._pools.snapshot())[0]["entries"][1:]
    assert (wide["job"], retry["job"]) == ("two", "one")
    attempts = _count_admission_attempts(cron, monkeypatch)
    await cron._pools._tick_pool("database")
    # the head waits for a second slot; the superseded retry behind it
    # still goes to acquire(), which cancels it
    assert attempts == [retry["id"]]
    assert not cron.running_jobs
    entries = (await cron._pools.snapshot())[0]["entries"]
    assert {e["id"]: (e["state"], e.get("reason")) for e in entries[1:]} == {
        wide["id"]: ("queued", None),
        retry["id"]: ("cancelled", "retry superseded"),
    }
    await cron._pools.finish(held)
    await cron._pools.cancel("database", wide["id"])


async def test_declined_launch_closes_the_tick_behind_it(
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
    # the declined entry is queued again at the head, so the entry behind
    # it has nothing to claim
    assert attempts == [first["id"]]
    assert (await cron._pools.snapshot())[0]["queued"] == 2
    await cron._pools.finish(held)
    for key in (first["id"], second["id"]):
        await cron._pools.cancel("database", key)


async def test_declined_launch_leaves_the_service_asleep(
    dag_cron, monkeypatch
):
    # A declined entry goes back to the head of the queue.  A wake would
    # run the next tick at once, and that tick would decline it again.
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    one, _two = cron.cron_jobs.values()
    await cron.maybe_launch_job(one)
    (entry,) = (await pools.snapshot())[0]["entries"]

    async def decline(job, **kwargs):
        return False

    async def interrupted(job, **kwargs):
        raise OSError("spawn failed")

    pools._wake.clear()
    with monkeypatch.context() as patch:
        patch.setattr(cron, "maybe_launch_job", decline)
        await pools._tick_pool("database")
        assert not pools._wake.is_set()
        patch.setattr(cron, "maybe_launch_job", interrupted)
        with pytest.raises(OSError):
            await pools._tick_pool("database")
        assert not pools._wake.is_set()
    assert not pools.held
    assert (await pools.snapshot())[0]["queued"] == 1
    # a completion frees slots for the queue, so it wakes the service
    ticket = await pools.acquire("database", entry["id"])
    await pools.finish(ticket)
    assert pools._wake.is_set()


async def test_declined_launch_is_requeued_when_its_requeue_is_interrupted(
    dag_cron, monkeypatch
):
    # A tick that is interrupted while it requeues a declined entry hands
    # the claim back, and the entry is queued again at once.
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    await cron.maybe_launch_job(cron.cron_jobs["one"])

    async def decline(job, **kwargs):
        return False

    reasons = []
    finish = pools.finish

    async def interrupted_once(ticket, state, reason, **kwargs):
        reasons.append(reason)
        if len(reasons) == 1:
            raise asyncio.CancelledError
        return await finish(ticket, state, reason, **kwargs)

    monkeypatch.setattr(cron, "maybe_launch_job", decline)
    monkeypatch.setattr(pools, "finish", interrupted_once)
    with pytest.raises(asyncio.CancelledError):
        await pools._tick_pool("database")
    assert reasons == [
        "waiting for concurrency admission",
        "launch interrupted",
    ]
    assert not pools.held
    assert (await pools.snapshot())[0]["queued"] == 1


async def test_service_loop_waits_out_its_interval_on_a_declined_head(
    dag_cron, monkeypatch
):
    # While the head is declined, the service loop ticks once a second.
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    await cron.maybe_launch_job(cron.cron_jobs["one"])

    async def decline(job, **kwargs):
        return False

    monkeypatch.setattr(cron, "maybe_launch_job", decline)
    ticks = []
    tick = pools.tick

    async def counted():
        ticks.append(1)
        await tick()

    monkeypatch.setattr(pools, "tick", counted)
    parked = asyncio.Event()
    lapsed = asyncio.Event()
    wait = pools._wake.wait

    def park():
        # the loop asks for a wake only between two ticks
        if not parked.is_set():
            parked.set()
            # armed ahead of the loop's own timer, so it fires first
            asyncio.get_running_loop().call_later(0.3, lapsed.set)
        return wait()

    monkeypatch.setattr(pools._wake, "wait", park)
    pools._task = asyncio.create_task(pools._run())
    try:
        await asyncio.wait_for(parked.wait(), 30)
        # no wake is pending, so only the interval ends the wait
        assert not pools._wake.is_set()
        await lapsed.wait()
    finally:
        await pools.close()
    assert len(ticks) == 1
    assert (await pools.snapshot())[0]["queued"] == 1


async def test_tick_goes_on_past_an_entry_that_its_launch_settled(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    one, two = cron.cron_jobs.values()
    # a higher priority puts the settled entry at the head for any enqueue
    # timestamps
    one.queuePriority = 1
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    first, second = (await pools.snapshot())[0]["entries"]
    assert (first["job"], second["job"]) == ("one", "two")
    attempts = _count_admission_attempts(cron, monkeypatch)
    launch = cron.maybe_launch_job

    async def settle_first(job, **kwargs):
        if job is not one:
            return await launch(job, **kwargs)
        # what the launch does with a retry that fresher state supersedes
        await pools.finish(
            kwargs["pool_ticket"], "cancelled", "retry superseded"
        )
        return False

    with monkeypatch.context() as patch:
        patch.setattr(cron, "maybe_launch_job", settle_first)
        await pools._tick_pool("database")
    # the head is gone, so the entry behind it is the head and is admitted
    assert attempts == [first["id"], second["id"]]
    assert set(cron.running_jobs) == {"two"}
    entries = (await pools.snapshot())[0]["entries"]
    assert {e["id"]: (e["state"], e.get("reason")) for e in entries} == {
        first["id"]: ("cancelled", "retry superseded"),
        second["id"]: ("running", None),
    }
    await _reap_running(cron)


async def test_entry_that_its_launch_settled_leaves_its_slot_to_the_next(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    one, two = cron.cron_jobs.values()
    held = await pools.acquire("database", (await pools.enqueue(one))["id"])
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    first, second = (await pools.snapshot())[0]["entries"][1:]
    launch = cron.maybe_launch_job

    async def settle_first(job, **kwargs):
        if job.name != first["job"]:
            return await launch(job, **kwargs)
        # what the launch does with a retry that fresher state supersedes
        await pools.finish(
            kwargs["pool_ticket"], "cancelled", "retry superseded"
        )
        return False

    with monkeypatch.context() as patch:
        patch.setattr(cron, "maybe_launch_job", settle_first)
        await pools._tick_pool("database")
    # the settled entry holds no slot, so the last free one goes to the
    # entry behind it
    assert set(cron.running_jobs) == {second["job"]}
    entries = (await pools.snapshot())[0]["entries"]
    assert {e["id"]: (e["state"], e.get("reason")) for e in entries} == {
        held.key: ("running", None),
        first["id"]: ("cancelled", "retry superseded"),
        second["id"]: ("running", None),
    }
    await _reap_running(cron)
    await pools.finish(held)


async def test_tick_goes_on_past_a_retry_that_the_claim_cancelled(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    one, two = cron.cron_jobs.values()
    # a higher priority puts the retry at the head for any enqueue timestamps
    one.queuePriority = 1
    state = JobRetryState(5, 2, 60)
    state.next_delay()
    state.pool_retry = {
        "pool": "database",
        "scope": pools._retry_scope("one", None),
        "generation": "an older ladder",
    }
    cron.retry_state["one"] = state
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    retry, behind = (await pools.snapshot())[0]["entries"]
    attempts = _count_admission_attempts(cron, monkeypatch)
    launches = []
    launch = cron.maybe_launch_job

    async def recording(job, **kwargs):
        launches.append(job.name)
        return await launch(job, **kwargs)

    monkeypatch.setattr(cron, "maybe_launch_job", recording)
    await pools._tick_pool("database")
    # the claim cancels the superseded retry before any launch, which
    # leaves the entry behind it at the head
    assert attempts == [retry["id"], behind["id"]]
    assert launches == ["two"]
    assert set(cron.running_jobs) == {"two"}
    entries = (await pools.snapshot())[0]["entries"]
    assert {e["id"]: (e["state"], e.get("reason")) for e in entries} == {
        retry["id"]: ("cancelled", "retry superseded"),
        behind["id"]: ("running", None),
    }
    await _reap_running(cron)


async def test_tick_goes_on_past_a_head_whose_job_is_disabled(
    dag_cron, monkeypatch
):
    cron = await make(dag_cron, monkeypatch)
    pools = cron._pools
    one, two = cron.cron_jobs.values()
    held = await pools.acquire("database", (await pools.enqueue(one))["id"])
    await cron.maybe_launch_job(one)
    await cron.maybe_launch_job(two)
    head, behind = (await pools.snapshot())[0]["entries"][1:]
    cron.cron_jobs[head["job"]].enabled = False
    attempts = _count_admission_attempts(cron, monkeypatch)
    await pools._tick_pool("database")
    # the tick cancels the head itself, and the last free slot goes to the
    # entry behind it
    assert attempts == [behind["id"]]
    assert set(cron.running_jobs) == {behind["job"]}
    entries = (await pools.snapshot())[0]["entries"]
    assert {e["id"]: e["state"] for e in entries} == {
        held.key: "running",
        head["id"]: "cancelled",
        behind["id"]: "running",
    }
    await _reap_running(cron)
    await pools.finish(held)


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


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_retry_settlement_waits_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, caplog, outage
):
    cron = await make(dag_cron, monkeypatch)
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        with caplog.at_level("WARNING", logger="cronstable.pools"):
            await cron.cancel_job_retries("one")
    assert "pool database: retry settlement deferred" in caplog.text
    assert cron._pools._retry_settlements


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_scheduled_fire_is_dropped_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, caplog, outage
):
    # Nothing above launch_scheduled_job catches an error: one that left it
    # would end Cron.run().
    cron = await make(dag_cron, monkeypatch)
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        with caplog.at_level("WARNING"):
            await cron.launch_scheduled_job(cron.cron_jobs["one"])
    assert "Job one could not enter pool database" in caplog.text
    assert not cron.running_jobs


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_failed_run_is_handled_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, caplog, outage
):
    # The check of the run's retry guard waits for the pool, and the rest
    # of the failure handling goes on.
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    cron = await make(dag_cron, monkeypatch)
    state = JobRetryState(5, 2, 60)
    state.pool_retry = {
        "pool": "database",
        "scope": cron._pools._retry_scope("one", None),
        "generation": "",
    }
    cron.retry_state["one"] = state
    run = SimpleNamespace(
        config=cron.cron_jobs["one"],
        retry_state=state,
        stdout=None,
        stderr=None,
        report_failure=AsyncMock(),
        report_permanent_failure=AsyncMock(),
    )
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        with caplog.at_level("WARNING"):
            await cron.handle_job_failure(run)
    assert "Job one: deferring its retry guard check" in caplog.text
    # the job allows no retry, so this failure is its permanent one
    run.report_permanent_failure.assert_awaited_once()
    assert "one" not in cron.retry_state


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_due_retry_waits_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, caplog, outage
):
    cron = await make(dag_cron, monkeypatch)
    state = JobRetryState(60, 2, 120)
    state.next_delay()
    cron.retry_state["one"] = state
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        with caplog.at_level("WARNING"):
            assert not await cron._retry_consume_ok("one", 1, quiet=False)
    assert "Job one retry #1 waiting for pool admission" in caplog.text
    assert state.count == 1 and not state.cancelled
