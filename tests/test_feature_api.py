import json

import pytest
from aiohttp import web

from tests.conftest import Req
from tests.test_pools import make
from tests.test_recovery import failed_flow
from tests.test_web_scopes import _ScopedReq, _bearer, _run, _table
from cronstable.cron import Cron
from cronstable.config import _build_mcp_config
from cronstable.mcp import MCPHandler
from tests.test_mcp_tools import _req


async def test_manual_queue_api_acknowledges_and_cancels(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    response = await cron._web_start_job(Req(match={"name": "one"}))
    assert response.status == 202
    queued = json.loads(response.body)
    assert queued["pool"] == "database"
    pools = json.loads((await cron._web_pools(Req())).body)
    assert pools[0]["queued"] == 1
    cancel = Req(match={"name": "database", "key": queued["queueId"]})
    assert json.loads((await cron._web_pool_cancel(cancel)).body)["state"] == "cancelled"
    assert (await cron._web_pool_cancel(cancel)).status == 200
    await cron._pools.tick()
    assert not cron.running_jobs


@pytest.mark.parametrize("payload", [{"dryRun": False}, {"tasks": "x"}, {"dryRun": "false"}, {"allowConfigChange": 1}])
async def test_recovery_rejects_malformed_request(dag_cron, payload):
    cron = await dag_cron("", web=True)
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_dag_recover(Req(match={"name": "flow", "run_key": "x"}, body=payload))


async def test_recovery_preview_has_no_execution_side_effect(dag_cron, tmp_path):
    cron, key, _, _ = await failed_flow(dag_cron, tmp_path)
    cron.web_config = {}
    before = await cron._dag.list_runs("flow")
    response = await cron._web_dag_recover(Req(match={"name": "flow", "run_key": key}, body={}))
    assert json.loads(response.body)["dryRun"] is True
    assert await cron._dag.list_runs("flow") == before


@pytest.mark.parametrize("path", ["/pools/{name}/queue/{key}/cancel", "/dags/{name}/runs/{run_key}/recover", "/dags/{name}/recover"])
async def test_queue_and_recovery_controls_reject_view_token(path):
    mw = Cron._make_auth_middleware(_table(("view", ["view"], "viewer")))
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, _ScopedReq(path, method="POST", canonical=path, headers=_bearer("view")))


async def test_mcp_queue_uses_structured_content_and_requires_confirmation(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    handler = MCPHandler(cron, _build_mcp_config({"enabled": True, "readOnly": False, "toolsets": ["observe", "act", "dags"]}))
    entry = await cron._pools.enqueue(cron.cron_jobs["one"])
    listed = await _req(handler, "tools/call", {"name": "cron_list_pools", "arguments": {}})
    assert listed["result"]["structuredContent"]["pools"][0]["queued"] == 1
    args = {"pool": "database", "id": entry["id"]}
    denied = await _req(handler, "tools/call", {"name": "cron_cancel_queued", "arguments": args})
    assert denied["result"]["isError"]
    assert (await cron._pools.snapshot())[0]["queued"] == 1
    accepted = await _req(handler, "tools/call", {"name": "cron_cancel_queued", "arguments": {**args, "confirm": True}})
    assert accepted["result"]["structuredContent"]["state"] == "cancelled"


async def test_mcp_recovery_uses_reviewed_plan(dag_cron, tmp_path):
    cron, key, _, marker = await failed_flow(dag_cron, tmp_path)
    handler = MCPHandler(cron, _build_mcp_config({"enabled": True, "readOnly": False, "toolsets": ["dags"]}))
    args = {"dag": "flow", "run_key": key}
    response = await _req(handler, "tools/call", {"name": "cron_preview_recovery", "arguments": args})
    plan = response["result"]["structuredContent"]
    assert plan["tasks"] == ["notify", "upload"]
    marker.touch()
    response = await _req(handler, "tools/call", {"name": "cron_recover_dag", "arguments": {**args, "plan_token": plan["planToken"], "confirm": True}})
    assert response["result"]["structuredContent"]["runKey"].startswith("recovery-")
