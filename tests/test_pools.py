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
    ticket.running = running
    async def unavailable(*args, **kwargs):
        raise OSError("offline")
    monkeypatch.setattr(cron._pools, "_change", unavailable)
    await cron._pools._renew(ticket)
    running.cancel.assert_awaited_once()
    with pytest.raises(PoolError, match="lost"):
        cron._pools.check_ticket(ticket)


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
