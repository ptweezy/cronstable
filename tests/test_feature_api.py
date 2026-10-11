import asyncio
import json

import pytest
from aiohttp import web

from cronstable import recovery
from tests.conftest import Req
from tests.test_pools import POOL_OUTAGES, break_pool_state, make
from tests.test_recovery import _RANGE, _failed_dates, failed_flow
from tests.test_web_scopes import _ScopedReq, _bearer, _run, _table
from cronstable.cron import Cron
from cronstable.config import _build_mcp_config
from cronstable.mcp import MCPHandler
from tests.test_mcp_tools import _call, _req


async def _store_offline(*args, **kwargs):
    raise OSError("store offline")


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


async def test_manual_queue_answers_503_while_pool_state_is_unavailable(dag_cron, monkeypatch):
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    monkeypatch.setattr(cron._pools, "enqueue_job", _store_offline)
    with pytest.raises(web.HTTPServiceUnavailable) as ei:
        await cron._web_start_job(Req(match={"name": "one"}))
    assert json.loads(ei.value.text) == {"error": "pool state is unavailable"}


_MCP_TOOLSETS = {
    "enabled": True,
    "readOnly": False,
    "toolsets": ["observe", "act", "dags"],
}


def _pool_requests(cron):
    """Each HTTP request that reads or writes the state of a pool."""
    queue = {"name": "database", "key": "absent"}
    return {
        "GET /pools": lambda: cron._web_pools(Req()),
        "queue cancel": lambda: cron._web_pool_cancel(Req(match=queue)),
        "pooled start": lambda: cron._web_start_job(
            Req(match={"name": "one"})
        ),
    }


_POOL_TOOLS = {
    "cron_list_pools": {},
    "cron_cancel_queued": {
        "pool": "database",
        "id": "absent",
        "confirm": True,
    },
    "cron_run_job": {"name": "one", "confirm": True},
}


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_pool_requests_answer_503_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, outage
):
    # Pool state that cannot be read or written answers 503 on every
    # route, whichever way the store fails.
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        for name, request in _pool_requests(cron).items():
            with pytest.raises(web.HTTPServiceUnavailable) as raised:
                await request()
            assert json.loads(raised.value.text) == {
                "error": "pool state is unavailable"
            }, name


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_mcp_pool_tools_report_unavailable_pool_state(
    dag_cron, monkeypatch, outage
):
    cron = await make(dag_cron, monkeypatch)
    handler = MCPHandler(cron, _build_mcp_config(_MCP_TOOLSETS))
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        for tool, arguments in _POOL_TOOLS.items():
            result = await _call(handler, tool, arguments)
            assert result["isError"], tool
            text = result["content"][0]["text"]
            assert text == "pool state is unavailable", tool


@pytest.mark.parametrize("outage", POOL_OUTAGES)
async def test_job_listing_answers_while_pool_state_is_unavailable(
    dag_cron, monkeypatch, outage
):
    # The jobs themselves are known, so both job routes answer 200 and
    # mark the queue of each pooled job as unavailable.
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    with monkeypatch.context() as patch:
        await break_pool_state(cron, patch, outage)
        listing = await cron._web_list_jobs(Req())
        detail = await cron._web_get_job(Req(match={"name": "one"}))
    assert listing.status == detail.status == 200
    jobs = json.loads(listing.text) + [json.loads(detail.body)]
    assert [job["name"] for job in jobs] == ["one", "two", "one"]
    for job in jobs:
        assert job["pool"]["queueUnavailable"] is True
        assert "queued" not in job["pool"]


async def test_pool_refusals_answer_409(dag_cron, monkeypatch):
    # A refusal by a pool that answers is a conflict, on the routes that
    # answer 503 for an outage.
    cron = await make(dag_cron, monkeypatch)
    cron.web_config = {}
    pools = cron._pools
    handler = MCPHandler(cron, _build_mcp_config(_MCP_TOOLSETS))

    async def refusal(request):
        with pytest.raises(web.HTTPConflict) as raised:
            await request
        return json.loads(raised.value.text)["error"]

    async def tool_refusal(tool):
        result = await _call(handler, tool, _POOL_TOOLS[tool])
        assert result["isError"]
        return result["content"][0]["text"]

    def cancel(key):
        return cron._web_pool_cancel(
            Req(match={"name": "database", "key": key})
        )

    def start():
        return cron._web_start_job(Req(match={"name": "one"}))

    assert await refusal(cancel("absent")) == "queue entry not found"
    assert await tool_refusal("cron_cancel_queued") == "queue entry not found"
    entry = await pools.enqueue(cron.cron_jobs["one"])
    ticket = await pools.acquire("database", entry["id"])
    assert (
        await refusal(cancel(entry["id"]))
        == "only waiting entries can be cancelled"
    )
    await pools.finish(ticket)
    for _ in range(4):
        await pools.enqueue(cron.cron_jobs["two"])
    assert await refusal(start()) == "pool queue is full"
    assert await tool_refusal("cron_run_job") == "pool queue is full"
    with monkeypatch.context() as patch:
        patch.setitem(cron.pool_config["database"], "slots", 1)
        assert (
            await refusal(start())
            == "pool is draining before a capacity change"
        )
    with monkeypatch.context() as patch:
        patch.setattr(cron, "_slot_fidelity", "locks do not exclude")
        assert (
            await refusal(start())
            == "pool state requires reliable exclusive locks"
        )


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


@pytest.mark.parametrize(
    "tool, arguments, target, message",
    [
        ("cron_list_pools", {}, "snapshot", "pool state is unavailable"),
        ("cron_cancel_queued", {"pool": "database", "id": "x", "confirm": True}, "cancel", "pool state is unavailable"),
        ("cron_preview_recovery", {"dag": "flow", "run_key": "r"}, "recover", "recovery state is unavailable"),
    ],
)
async def test_mcp_store_failures_are_tool_errors(dag_cron, monkeypatch, tool, arguments, target, message):
    cron = await make(dag_cron, monkeypatch)
    owner = cron._dag if target == "recover" else cron._pools
    monkeypatch.setattr(owner, target, _store_offline)
    handler = MCPHandler(cron, _build_mcp_config({"enabled": True, "readOnly": False, "toolsets": ["observe", "act", "dags"]}))
    result = await _call(handler, tool, arguments)
    assert result["isError"]
    assert result["content"][0]["text"] == message


async def _records_unreadable(*args, **kwargs):
    from cronstable.state import _DocumentUnreadable

    raise _DocumentUnreadable("stream 's': records kept leaving during the read")


async def test_recovery_answers_unavailable_while_its_records_cannot_be_read(dag_cron, tmp_path, monkeypatch):
    # A strict read that cannot settle raises _DocumentUnreadable, which is
    # not an OSError.  Every recovery surface answers it as a store outage.
    cron, key, _, _ = await failed_flow(dag_cron, tmp_path)
    cron.web_config = {}
    monkeypatch.setattr(cron.state_backend, "newest_records_by", _records_unreadable)
    with pytest.raises(web.HTTPServiceUnavailable) as ei:
        await cron._web_dag_recover(Req(match={"name": "flow", "run_key": key}, body={}))
    assert json.loads(ei.value.text) == {"error": "recovery state is unavailable"}
    handler = MCPHandler(cron, _build_mcp_config({"enabled": True, "readOnly": False, "toolsets": ["dags"]}))
    result = await _call(handler, "cron_preview_recovery", {"dag": "flow", "run_key": key})
    assert result["isError"]
    assert result["content"][0]["text"] == "recovery state is unavailable"
    monkeypatch.setattr(cron._dag, "recover_range", _records_unreadable)
    with pytest.raises(web.HTTPServiceUnavailable) as ei:
        await cron._web_dag_recover_range(Req(match={"name": "flow"}, body={"from": "2026-09-01", "to": "2026-09-03"}))
    assert json.loads(ei.value.text) == {"error": "recovery state is unavailable"}


async def _assert_recovery_unavailable(cron, key):
    """Preview and execution, of one run and of a date range, answer that
    recovery state is unavailable, over HTTP and over MCP."""
    token = "0" * 64
    run = {"name": "flow", "run_key": key}
    workflow = {"name": "flow"}
    dates = {"from": _RANGE[1], "to": _RANGE[2]}
    execute = {"dryRun": False, "planToken": token}
    requests = {
        "preview": lambda: cron._web_dag_recover(Req(match=run, body={})),
        "execute": lambda: cron._web_dag_recover(
            Req(match=run, body=execute)
        ),
        "range preview": lambda: cron._web_dag_recover_range(
            Req(match=workflow, body=dates)
        ),
        "range execute": lambda: cron._web_dag_recover_range(
            Req(match=workflow, body={**dates, **execute})
        ),
    }
    accept = {"plan_token": token, "confirm": True}
    tools = [
        ("cron_preview_recovery", {"dag": "flow", "run_key": key}),
        ("cron_preview_recovery", {"dag": "flow", **dates}),
        ("cron_recover_dag", {"dag": "flow", "run_key": key, **accept}),
        ("cron_recover_dag", {"dag": "flow", **dates, **accept}),
    ]
    unavailable = "recovery state is unavailable"
    for name, request in requests.items():
        with pytest.raises(web.HTTPServiceUnavailable) as raised:
            await request()
        assert json.loads(raised.value.text) == {"error": unavailable}, name
    handler = MCPHandler(cron, _build_mcp_config(_MCP_TOOLSETS))
    for tool, arguments in tools:
        result = await _call(handler, tool, arguments)
        assert result["isError"], (tool, arguments)
        assert result["content"][0]["text"] == unavailable


def _unreadable():
    from cronstable.state import _DocumentUnreadable

    return _DocumentUnreadable("document 'd': not a document")


@pytest.mark.parametrize(
    "error",
    [
        lambda: OSError("store offline"),
        asyncio.TimeoutError,
        _unreadable,
        recovery.RecoveryUnavailable,
    ],
    ids=["store-error", "timeout", "unreadable-document", "no-backend"],
)
async def test_recovery_surfaces_answer_unavailable(
    dag_cron, monkeypatch, error
):
    # Each recovery surface reports the same outage, whichever way the
    # store fails to answer.
    cron = await dag_cron("", web=True)

    async def unavailable(*args, **kwargs):
        raise error()

    monkeypatch.setattr(cron._dag, "recover", unavailable)
    monkeypatch.setattr(cron._dag, "recover_range", unavailable)
    await _assert_recovery_unavailable(cron, "run")


async def test_recovery_answers_unavailable_without_a_state_backend(
    dag_cron, monkeypatch
):
    # Without a state backend no lookup can tell whether the run exists,
    # so the scheduler's own answer is an outage on every surface.
    cron, (key,) = await _failed_dates(dag_cron, days=(1,))
    cron.web_config = {}
    handler = MCPHandler(cron, _build_mcp_config(_MCP_TOOLSETS))
    with monkeypatch.context() as patch:
        patch.setattr(cron, "state_backend", None)
        await _assert_recovery_unavailable(cron, key)
    # with the store back, a run that does not exist is a conflict
    absent = {"name": "flow", "run_key": "absent"}
    with pytest.raises(web.HTTPConflict) as raised:
        await cron._web_dag_recover(Req(match=absent, body={}))
    assert json.loads(raised.value.text) == {
        "error": "workflow run not found"
    }
    result = await _call(
        handler, "cron_preview_recovery", {"dag": "flow", "run_key": "absent"}
    )
    assert result["isError"]
    assert result["content"][0]["text"] == "workflow run not found"


_STATELESS = """
jobs:
  - name: plain
    command: ignored
    schedule: "@reboot"
"""


async def test_stateless_daemon_answers_recovery_and_queue_requests_409():
    # A workflow or a pool needs a ``state`` section, so a daemon without
    # one has neither.  Each request names something absent, which no
    # retry changes.
    cron = Cron(None, config_yaml=_STATELESS)
    cron.web_config = {}
    assert cron.state_backend is None and not cron._state_configured
    token = "0" * 64
    run = {"name": "flow", "run_key": "run"}
    workflow = {"name": "flow"}
    dates = {"from": _RANGE[1], "to": _RANGE[2]}
    execute = {"dryRun": False, "planToken": token}
    queue = {"name": "database", "key": "absent"}
    absent_run = "workflow run not found"
    absent_workflow = "workflow not found"
    absent_pool = "unknown pool 'database'"
    requests = [
        (lambda: cron._web_dag_recover(Req(match=run, body={})), absent_run),
        (
            lambda: cron._web_dag_recover(Req(match=run, body=execute)),
            absent_run,
        ),
        (
            lambda: cron._web_dag_recover_range(
                Req(match=workflow, body=dates)
            ),
            absent_workflow,
        ),
        (
            lambda: cron._web_dag_recover_range(
                Req(match=workflow, body={**dates, **execute})
            ),
            absent_workflow,
        ),
        (lambda: cron._web_pool_cancel(Req(match=queue)), absent_pool),
    ]
    for request, message in requests:
        with pytest.raises(web.HTTPConflict) as raised:
            await request()
        assert json.loads(raised.value.text) == {"error": message}
    accept = {"plan_token": token, "confirm": True}
    cancel = {"pool": "database", "id": "absent", "confirm": True}
    one_run = {"dag": "flow", "run_key": "run"}
    in_range = {"dag": "flow", **dates}
    tools = [
        ("cron_preview_recovery", one_run, absent_run),
        ("cron_preview_recovery", in_range, absent_workflow),
        ("cron_recover_dag", {**one_run, **accept}, absent_run),
        ("cron_recover_dag", {**in_range, **accept}, absent_workflow),
        ("cron_cancel_queued", cancel, absent_pool),
    ]
    handler = MCPHandler(cron, _build_mcp_config(_MCP_TOOLSETS))
    for tool, arguments, message in tools:
        result = await _call(handler, tool, arguments)
        assert result["isError"], (tool, arguments)
        assert result["content"][0]["text"] == message
    # a daemon without pools still lists none
    assert json.loads((await cron._web_pools(Req())).body) == []


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
