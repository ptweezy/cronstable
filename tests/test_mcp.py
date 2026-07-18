"""Tests for the hand-rolled MCP server (:mod:`cronstable.mcp`) and its
config plumbing / stdio bridge.

The JSON-RPC dispatch is driven directly through ``MCPHandler.handle_message``
(the same direct-handler style as ``test_ui_endpoints.py``); the HTTP transport
is exercised with a minimal fake request; the fail-closed config check and the
bridge's import isolation are checked at the module level.
"""

import json
import subprocess
import sys

import pytest

from cronstable.config import (
    ConfigError,
    _build_mcp_config,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.cron import Cron
from cronstable.mcp import MCPHandler

_YAML = """
jobs:
  - name: hello
    command: echo hi
    schedule: "* * * * *"
  - name: nightly
    command: backup
    schedule: "0 3 * * *"
    enabled: false
"""


def _handler(mcp=None, yaml=_YAML):
    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    cfg = _build_mcp_config({"enabled": True, **(mcp or {})})
    return MCPHandler(cron, cfg)


def _req(handler, method, params=None, mid=1, notif=False):
    msg = {"jsonrpc": "2.0", "method": method}
    if not notif:
        msg["id"] = mid
    if params is not None:
        msg["params"] = params
    return handler.handle_message(msg)


async def _tool_names(handler):
    resp = await _req(handler, "tools/list")
    return [t["name"] for t in resp["result"]["tools"]]


class FakeReq:
    """Minimal aiohttp-request stand-in for the /mcp HTTP handlers."""

    def __init__(self, method="POST", headers=None, body=b""):
        self.method = method
        self.headers = headers or {}
        self._body = body
        self.content_length = len(body) if body else None

    async def read(self):
        return self._body


def _post_req(obj, headers=None, body=None):
    hdrs = {"Accept": "application/json", "Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    raw = body if body is not None else json.dumps(obj).encode()
    return FakeReq("POST", hdrs, raw)


# ---------------------------------------------------------------------------
# config: defaults + fail-closed cross validation
# ---------------------------------------------------------------------------


def test_build_mcp_defaults():
    cfg = _build_mcp_config(None)
    assert cfg["enabled"] is False
    assert cfg["readOnly"] is True
    assert cfg["toolsets"] == ["observe"]
    assert cfg["maxRows"] == 200


def test_build_mcp_dedupes_toolsets():
    cfg = _build_mcp_config({"toolsets": ["observe", "observe", "act"]})
    assert cfg["toolsets"] == ["observe", "act"]


@pytest.mark.parametrize("bad", [{"maxRows": 0}, {"maxBodyBytes": 0}])
def test_build_mcp_rejects_nonpositive_limits(bad):
    with pytest.raises(ConfigError):
        _build_mcp_config(bad)


def _parse(yaml):
    cfg = parse_config_string(yaml, "t.yaml")
    _validate_cross_sections(cfg)
    return cfg


def test_fail_closed_routable_no_token():
    yaml = (
        "web:\n  listen:\n    - http://0.0.0.0:8080\nmcp:\n  enabled: true\n"
    )
    with pytest.raises(ConfigError, match="without authentication"):
        _parse(yaml)


def test_loopback_no_token_allowed():
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "mcp:\n  enabled: true\n"
    )
    assert _parse(yaml).mcp_config["enabled"] is True


def test_routable_with_token_allowed():
    yaml = (
        "web:\n  listen:\n    - http://0.0.0.0:8080\n"
        "  authToken:\n    value: sekret\nmcp:\n  enabled: true\n"
    )
    assert _parse(yaml).mcp_config["enabled"] is True


def test_routable_allow_unauthenticated_escape_hatch():
    yaml = (
        "web:\n  listen:\n    - http://0.0.0.0:8080\n"
        "mcp:\n  enabled: true\n  allowUnauthenticated: true\n"
    )
    assert _parse(yaml).mcp_config["allowUnauthenticated"] is True


def test_enabled_without_web_rejected():
    with pytest.raises(ConfigError, match="requires a `web`"):
        _parse("mcp:\n  enabled: true\n")


# ---------------------------------------------------------------------------
# initialize / capabilities
# ---------------------------------------------------------------------------


async def test_initialize_negotiates_and_advertises_capabilities():
    h = _handler()
    resp = await _req(h, "initialize", {"protocolVersion": "2025-11-25"})
    result = resp["result"]
    assert result["protocolVersion"] == "2025-11-25"
    # tools always; resources+prompts because they are enabled by default.
    caps = result["capabilities"]
    assert caps["tools"] == {"listChanged": False}
    assert "resources" in caps
    assert "prompts" in caps
    assert result["serverInfo"]["name"] == "cronstable"
    assert "instructions" in result


async def test_capabilities_gated_when_resources_prompts_off():
    h = _handler({"resources": False, "prompts": False})
    resp = await _req(h, "initialize", {"protocolVersion": "2025-11-25"})
    # a server MUST NOT advertise what it does not implement.
    assert resp["result"]["capabilities"] == {"tools": {"listChanged": False}}
    # ...and the methods are then unknown.
    assert (await _req(h, "resources/list"))["error"]["code"] == -32601
    assert (await _req(h, "prompts/list"))["error"]["code"] == -32601


async def test_initialize_offers_own_version_for_unknown_client_version():
    h = _handler()
    resp = await _req(h, "initialize", {"protocolVersion": "1999-01-01"})
    assert resp["result"]["protocolVersion"] == "2025-11-25"


async def test_ping():
    h = _handler()
    resp = await _req(h, "ping")
    assert resp["result"] == {}


# ---------------------------------------------------------------------------
# tools/list: readOnly + toolset gating, annotations
# ---------------------------------------------------------------------------


async def test_default_lists_observe_readonly_only():
    h = _handler()  # readOnly True, toolsets [observe]
    resp = await _req(h, "tools/list")
    names = [t["name"] for t in resp["result"]["tools"]]
    assert "cron_list_jobs" in names
    assert "cron_get_status" in names
    # no dag/state/mutating tools in the default profile
    assert not any(n.startswith("cron_list_dags") for n in names)
    assert "cron_run_job" not in names
    assert "cron_inspect_state" not in names


async def test_mutating_tools_absent_under_readonly():
    h = _handler({"readOnly": True, "toolsets": ["observe", "act", "dags"]})
    names = await _tool_names(h)
    assert "cron_run_job" not in names
    assert "cron_cancel_job" not in names
    assert "cron_trigger_dag" not in names
    # read DAG tools still present (reads aren't gated by readOnly)
    assert "cron_list_dags" in names


async def test_mutating_tools_present_when_writes_enabled():
    h = _handler(
        {"readOnly": False, "toolsets": ["observe", "act", "dags", "state"]}
    )
    names = await _tool_names(h)
    for expected in (
        "cron_run_job",
        "cron_cancel_job",
        "cron_trigger_dag",
        "cron_backfill_dag",
        "cron_decide_gate",
        "cron_inspect_state",
    ):
        assert expected in names


async def test_read_tools_annotations():
    h = _handler()
    tools = (await _req(h, "tools/list"))["result"]["tools"]
    for t in tools:
        assert t["annotations"]["readOnlyHint"] is True
        # closed domain (cronstable's own state), never an open external set.
        assert t["annotations"]["openWorldHint"] is False


async def test_input_schemas_are_object_2020_12_shaped():
    h = _handler({"readOnly": False, "toolsets": ["observe", "act", "dags"]})
    for t in (await _req(h, "tools/list"))["result"]["tools"]:
        schema = t["inputSchema"]
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False


# ---------------------------------------------------------------------------
# tools/call
# ---------------------------------------------------------------------------


async def test_call_read_tool_returns_structured_and_text():
    h = _handler()
    resp = await _req(
        h, "tools/call", {"name": "cron_get_status", "arguments": {}}
    )
    result = resp["result"]
    assert result.get("isError") is None
    assert result["content"][0]["type"] == "text"
    assert len(result["structuredContent"]["status"]) == 2


async def test_call_unknown_tool_is_invalid_params():
    h = _handler()
    resp = await _req(
        h, "tools/call", {"name": "cron_nope", "arguments": {}}
    )
    assert resp["error"]["code"] == -32602


async def test_call_hidden_mutating_tool_is_unknown():
    # a suppressed (readOnly) tool must not be callable, even by exact name.
    h = _handler({"readOnly": True, "toolsets": ["observe", "act"]})
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_run_job", "arguments": {"name": "hello"}},
    )
    assert resp["error"]["code"] == -32602


async def test_call_missing_required_arg_is_tool_error():
    h = _handler()
    resp = await _req(h, "tools/call", {"name": "cron_get_job"})
    assert resp["result"]["isError"] is True


async def test_get_job_not_found_is_tool_error():
    h = _handler()
    resp = await _req(
        h, "tools/call", {"name": "cron_get_job", "arguments": {"name": "x"}}
    )
    assert resp["result"]["isError"] is True
    assert "not found" in resp["result"]["content"][0]["text"]


async def test_list_jobs_state_filter_and_pagination():
    h = _handler()
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_list_jobs", "arguments": {"state": "disabled"}},
    )
    jobs = resp["result"]["structuredContent"]["jobs"]
    assert [j["name"] for j in jobs] == ["nightly"]


async def test_maxrows_clamps_limit():
    h = _handler({"maxRows": 1})
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_list_jobs", "arguments": {"limit": 100}},
    )
    page = resp["result"]["structuredContent"]["page"]
    assert page["limit"] == 1
    assert page["returned"] == 1
    assert page["nextOffset"] == 1


# ---------------------------------------------------------------------------
# mutating tools: confirm gate + dry-run
# ---------------------------------------------------------------------------


async def test_run_job_requires_confirm():
    h = _handler({"readOnly": False, "toolsets": ["act"]})
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_run_job", "arguments": {"name": "hello"}},
    )
    assert resp["result"]["isError"] is True
    assert "confirm=true" in resp["result"]["content"][0]["text"]


async def test_cancel_job_not_running_is_tool_error():
    h = _handler({"readOnly": False, "toolsets": ["act"]})
    resp = await _req(
        h,
        "tools/call",
        {
            "name": "cron_cancel_job",
            "arguments": {"name": "hello", "confirm": True},
        },
    )
    assert resp["result"]["isError"] is True
    assert "not running" in resp["result"]["content"][0]["text"]


async def test_backfill_dry_run_is_default_preview():
    h = _handler({"readOnly": False, "toolsets": ["dags"]}, yaml=_YAML)
    # unknown dag -> tool error even in dry-run (validated first)
    resp = await _req(
        h,
        "tools/call",
        {
            "name": "cron_backfill_dag",
            "arguments": {"dag": "ghost", "from": "2026-01-01",
                          "to": "2026-01-02"},
        },
    )
    assert resp["result"]["isError"] is True


# ---------------------------------------------------------------------------
# JSON-RPC framing
# ---------------------------------------------------------------------------


async def test_unknown_method_is_method_not_found():
    h = _handler()
    resp = await _req(h, "frobnicate")
    assert resp["error"]["code"] == -32601


async def test_notification_returns_no_response():
    h = _handler()
    assert await _req(h, "notifications/initialized", notif=True) is None
    # an unknown notification is silently ignored, too
    assert await _req(h, "notifications/bogus", notif=True) is None


async def test_bad_jsonrpc_version_is_invalid_request():
    h = _handler()
    resp = await h.handle_message({"id": 1, "method": "ping"})
    assert resp["error"]["code"] == -32600


# ---------------------------------------------------------------------------
# HTTP transport (stateless Streamable HTTP)
# ---------------------------------------------------------------------------


async def test_http_post_ping_ok():
    h = _handler()
    resp = await h.handle_http(
        _post_req({"jsonrpc": "2.0", "id": 9, "method": "ping"})
    )
    assert resp.status == 200
    assert resp.content_type == "application/json"
    assert resp.headers["MCP-Protocol-Version"] == "2025-11-25"
    assert json.loads(resp.body)["result"] == {}


async def test_http_notification_is_202():
    h = _handler()
    resp = await h.handle_http(
        _post_req({"jsonrpc": "2.0", "method": "notifications/initialized"})
    )
    assert resp.status == 202


async def test_http_get_is_405():
    h = _handler()
    resp = await h.handle_http_get(FakeReq("GET"))
    assert resp.status == 405
    assert "POST" in resp.headers["Allow"]


async def test_http_origin_refused_when_not_allowlisted():
    h = _handler()  # allowedOrigins empty -> any Origin refused
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            headers={"Origin": "http://evil.example"},
        )
    )
    assert resp.status == 403


async def test_http_origin_allowlisted_passes_with_cors():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            headers={"Origin": "http://ok.example"},
        )
    )
    assert resp.status == 200
    assert (
        resp.headers["Access-Control-Allow-Origin"] == "http://ok.example"
    )


async def test_http_preflight_options():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_options(
        FakeReq("OPTIONS", {"Origin": "http://ok.example"})
    )
    assert resp.status == 204
    assert "POST" in resp.headers["Access-Control-Allow-Methods"]


async def test_http_bad_accept_is_406():
    h = _handler()
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            headers={"Accept": "text/html"},
        )
    )
    assert resp.status == 406


async def test_http_unsupported_protocol_version_is_400():
    h = _handler()
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            headers={"MCP-Protocol-Version": "1999-01-01"},
        )
    )
    assert resp.status == 400


async def test_http_oversized_body_is_413():
    h = _handler({"maxBodyBytes": 100})
    resp = await h.handle_http(FakeReq("POST", {"Accept": "*/*"}, b"x" * 200))
    assert resp.status == 413


async def test_http_batching_is_rejected():
    h = _handler()
    resp = await h.handle_http(
        _post_req([{"jsonrpc": "2.0", "id": 1, "method": "ping"}])
    )
    assert resp.status == 400


async def test_http_malformed_json_is_400():
    h = _handler()
    resp = await h.handle_http(_post_req(None, body=b"not json"))
    assert resp.status == 400


# ---------------------------------------------------------------------------
# resources + prompts
# ---------------------------------------------------------------------------


async def test_resources_list_observe_scope():
    h = _handler()
    uris = [
        r["uri"]
        for r in (await _req(h, "resources/list"))["result"]["resources"]
    ]
    assert "cronstable://status" in uris
    assert "cronstable://version" in uris


async def test_resource_read_fixed_and_template():
    h = _handler()
    ver = await _req(
        h, "resources/read", {"uri": "cronstable://version"}
    )
    contents = ver["result"]["contents"][0]
    assert contents["mimeType"] == "application/json"
    assert json.loads(contents["text"])["jobs"] == 2
    job = await _req(
        h, "resources/read", {"uri": "cronstable://jobs/hello"}
    )
    assert json.loads(job["result"]["contents"][0]["text"])["name"] == "hello"


async def test_resource_read_unknown_is_32002():
    h = _handler()
    resp = await _req(
        h, "resources/read", {"uri": "cronstable://jobs/ghost"}
    )
    assert resp["error"]["code"] == -32002


async def test_resource_templates_gated_by_toolset():
    # dag/state templates are hidden under the default observe-only profile
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://dags/x"})
    assert resp["error"]["code"] == -32002
    # ...and visible under the dags toolset
    h2 = _handler({"toolsets": ["observe", "dags"]})
    templates = [
        t["uriTemplate"]
        for t in (await _req(h2, "resources/templates/list"))["result"][
            "resourceTemplates"
        ]
    ]
    assert "cronstable://dags/{name}" in templates


async def test_prompts_list_and_get():
    h = _handler()
    names = [
        p["name"] for p in (await _req(h, "prompts/list"))["result"]["prompts"]
    ]
    assert "triage_job_failure" in names
    # dag prompts are gated by the dags toolset
    assert "why_did_dag_run_fail" not in names
    got = await _req(
        h,
        "prompts/get",
        {"name": "triage_job_failure", "arguments": {"job": "hello"}},
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "hello" in text
    assert got["result"]["messages"][0]["role"] == "user"


async def test_prompts_dag_scope_and_unknown():
    h = _handler({"toolsets": ["observe", "dags"]})
    names = [
        p["name"] for p in (await _req(h, "prompts/list"))["result"]["prompts"]
    ]
    assert "why_did_dag_run_fail" in names
    resp = await _req(h, "prompts/get", {"name": "nope"})
    assert resp["error"]["code"] == -32602


# ---------------------------------------------------------------------------
# stdio bridge: import isolation (must stay featherweight, no daemon graph)
# ---------------------------------------------------------------------------


def test_mcpcli_import_is_featherweight():
    # importing the bridge must NOT drag in aiohttp / strictyaml / the Cron
    # graph, so `cronstable mcp` starts instantly like the other job-facing
    # subcommands. Checked in a fresh interpreter (this test process has them
    # imported already).
    code = (
        "import cronstable.mcpcli, sys;"
        "heavy=[m for m in "
        "('aiohttp','strictyaml','cronstable.cron','cronstable.mcp') "
        "if m in sys.modules];"
        "print(','.join(heavy))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == ""


async def test_schedule_analysis_tools_registered_and_callable():
    h = _handler()
    names = await _tool_names(h)
    for expected in (
        "cron_schedule_pressure",
        "cron_schedule_duplicates",
        "cron_suggest_slot",
    ):
        assert expected in names
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_schedule_pressure", "arguments": {"hours": 24}},
    )
    data = resp["result"]["structuredContent"]
    assert data["hours"] == 24
    assert len(data["grid"]) == 24
    assert "busiest minute" in resp["result"]["content"][0]["text"]
    resp = await _req(
        h, "tools/call", {"name": "cron_schedule_duplicates", "arguments": {}}
    )
    assert "groups" in resp["result"]["structuredContent"]
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_suggest_slot", "arguments": {"period": "daily"}},
    )
    assert resp["result"]["structuredContent"]["period"] == "daily"
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_schedule_pressure", "arguments": {"tz": "Nope/Zone"}},
    )
    assert resp["result"]["isError"] is True
