import asyncio
import json
import sys

from cronstable.pools import PoolError
from tests.conftest import Req
from tests.test_pools import make
from tests.test_state_dag_run import _drive


async def test_job_queue_receipt_and_admission_order(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    low = await cron._pools.enqueue(cron.cron_jobs["one"])
    cron.cron_jobs["two"].queuePriority = 10
    high = await cron._pools.enqueue(cron.cron_jobs["two"])
    pool = (await cron._pools.snapshot())[0]
    assert [e["id"] for e in pool["entries"]] == [high["id"], low["id"]]
    assert [e["position"] for e in pool["entries"]] == [1, 2]
    assert "higher-priority" in pool["entries"][1]["waitingReason"]
    response = await cron._web_get_job(Req(match={"name": "one"}))
    job = json.loads(response.body)
    assert job["pool"]["queued"][0]["id"] == low["id"]
    assert job["pool"]["queued"][0]["position"] == 2
    await cron._pools.cancel("database", low["id"])
    payload = cron.jobs_payload()
    await cron._attach_job_queues(payload)
    assert payload[0]["pool"]["queued"] == []


async def test_queue_failure_keeps_job_state_available(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}

    async def unavailable():
        raise PoolError("unavailable")

    monkeypatch.setattr(cron._pools, "snapshot", unavailable)
    response = await cron._web_get_job(Req(match={"name": "one"}))
    job = json.loads(response.body)
    assert job["name"] == "one"
    assert job["pool"]["queueUnavailable"] is True
    assert "queued" not in job["pool"]


async def test_live_verification_is_published_before_completion(
    dag_cron, monkeypatch
):
    cron = await dag_cron("""
dags:
  - name: flow
    tasks:
      - id: export
        command: ignored
        verify:
          command: ignored
""")
    template = cron.cron_dags["flow"].task_templates["export"]
    template.command = [sys.executable, "-c", "pass"]
    template.verify["command"] = [sys.executable, "-c", "pass"]
    published = asyncio.Event()
    original = cron._dag.on_task_verifying

    async def observe(running):
        await original(running)
        ref = running.dag_ref
        doc = await cron._dag.get_run(ref.dag_name, ref.run_key)
        assert doc["tasks"]["export"]["state"] == "running"
        assert doc["tasks"]["export"]["verification"] == {"outcome": "running"}
        published.set()

    monkeypatch.setattr(cron._dag, "on_task_verifying", observe)
    key = await cron._dag.trigger_run("flow")
    result = await _drive(cron, "flow", key)
    assert published.is_set()
    assert result["tasks"]["export"]["verification"]["outcome"] == "success"
