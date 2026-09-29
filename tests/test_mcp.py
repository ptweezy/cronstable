"""Tests for the hand-rolled MCP server (:mod:`cronstable.mcp`) and its
config plumbing / stdio bridge.

The JSON-RPC dispatch is driven directly through ``MCPHandler.handle_message``
(the same direct-handler style as ``test_ui_endpoints.py``); the HTTP transport
is exercised with a minimal fake request; the fail-closed config check and the
bridge's import isolation are checked at the module level.
"""

import base64
import itertools
import json
import pathlib
import re
import subprocess
import sys
import urllib.parse

import pytest
from multidict import CIMultiDict

from cronstable import mcp as mcp_mod
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
        # case-insensitive, like aiohttp's request headers
        self.headers = CIMultiDict(headers or {})
        self._body = body
        self.content_length = len(body) if body else None
        # the mapping surface the auth middleware files the matched token
        # into (handle_http reads the caller's scopes from it)
        self.store = {}

    def __setitem__(self, key, value):
        self.store[key] = value

    def get(self, key, default=None):
        return self.store.get(key, default)

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


@pytest.mark.parametrize(
    ("yaml", "key"),
    [
        pytest.param(
            "web:\n  listen:\n    - http://127.0.0.1:8080\n"
            "mcp:\n  enabled: true\n",
            "enabled",
            id="loopback-no-token",
        ),
        # a routable listener with mcp.enabled and ONLY scoped web.authTokens
        # (no scalar authToken) still authenticates /mcp, so the gate must
        # pass.
        pytest.param(
            "web:\n  listen:\n    - http://0.0.0.0:8080\n"
            "  authTokens:\n"
            "    - label: agent\n"
            "      scopes:\n        - control\n"
            "      value: s3cret\n"
            "mcp:\n  enabled: true\n",
            "enabled",
            id="scoped-tokens-satisfy-the-gate",
        ),
        pytest.param(
            "web:\n  listen:\n    - http://0.0.0.0:8080\n"
            "  authToken:\n    value: sekret\nmcp:\n  enabled: true\n",
            "enabled",
            id="routable-with-token",
        ),
        pytest.param(
            "web:\n  listen:\n    - http://0.0.0.0:8080\n"
            "mcp:\n  enabled: true\n  allowUnauthenticated: true\n",
            "allowUnauthenticated",
            id="routable-allow-unauthenticated-escape-hatch",
        ),
    ],
)
def test_fail_closed_gate_passes(yaml, key):
    assert _parse(yaml).mcp_config[key] is True


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
    assert "cron_pause_job" not in names
    assert "cron_resume_job" not in names
    assert "cron_inspect_state" not in names


async def test_mutating_tools_absent_under_readonly():
    h = _handler({"readOnly": True, "toolsets": ["observe", "act", "dags"]})
    names = await _tool_names(h)
    assert "cron_run_job" not in names
    assert "cron_cancel_job" not in names
    assert "cron_pause_job" not in names
    assert "cron_resume_job" not in names
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
        "cron_pause_job",
        "cron_resume_job",
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


async def test_mutating_tool_annotations_are_declared_correctly():
    # _tool() derives the hints from its keywords (idempotent defaults to
    # `not mutating`), so a tool declared with the wrong keyword would
    # advertise the wrong safety hint to a client that uses these to decide
    # what it may retry or must confirm with the user. Pin all four hints
    # per tool.
    h = _handler({"readOnly": False, "toolsets": ["observe", "act", "dags"]})
    tools = {
        t["name"]: t["annotations"]
        for t in (await _req(h, "tools/list"))["result"]["tools"]
    }
    expected = {
        # (readOnlyHint, destructiveHint, idempotentHint, openWorldHint)
        # launching runs whatever command the job or task configures, so
        # its effects are open-ended, and two calls are two runs
        "cron_run_job": (False, True, False, True),
        "cron_trigger_dag": (False, True, False, True),
        "cron_backfill_dag": (False, True, False, True),
        # a recovery plan token executes once; a repeat changes nothing
        "cron_recover_dag": (False, True, True, True),
        # approving releases the downstream tasks
        "cron_decide_gate": (False, True, False, True),
        # stopping work is destructive but closed: nothing new runs, and a
        # repeat changes nothing
        "cron_cancel_job": (False, True, True, False),
        "cron_cancel_queued": (False, True, True, False),
        "cron_pause_job": (False, False, True, False),
        "cron_resume_job": (False, False, True, False),
    }
    for name, hints in expected.items():
        assert name in tools, "{} is gone; update this table".format(name)
        ann = tools[name]
        got = (
            ann["readOnlyHint"],
            ann["destructiveHint"],
            ann["idempotentHint"],
            ann["openWorldHint"],
        )
        assert got == hints, (name, got, hints)
    # every OTHER tool in this fully-enabled handler is read-only, so the
    # table above is the complete mutating set and a new mutating tool
    # cannot slip in unannotated
    for name, ann in tools.items():
        if name not in expected:
            assert ann["readOnlyHint"] is True, name


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
    resp = await _req(h, "tools/call", {"name": "cron_nope", "arguments": {}})
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


async def test_backfill_dry_run_is_default_preview():
    h = _handler({"readOnly": False, "toolsets": ["dags"]}, yaml=_YAML)
    # unknown dag -> tool error even in dry-run (validated first)
    resp = await _req(
        h,
        "tools/call",
        {
            "name": "cron_backfill_dag",
            "arguments": {
                "dag": "ghost",
                "from": "2026-01-01",
                "to": "2026-01-02",
            },
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


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        # allowedOrigins empty -> any Origin refused
        pytest.param(
            {"Origin": "http://evil.example"},
            403,
            id="origin-not-allowlisted",
        ),
        pytest.param({"Accept": "text/html"}, 406, id="bad-accept"),
        pytest.param(
            {"MCP-Protocol-Version": "1999-01-01"},
            400,
            id="unsupported-protocol-version",
        ),
    ],
)
async def test_http_post_header_gates(headers, status):
    h = _handler()
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers=headers
        )
    )
    assert resp.status == status


async def test_http_origin_allowlisted_passes_with_cors():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            headers={"Origin": "http://ok.example"},
        )
    )
    assert resp.status == 200
    assert resp.headers["Access-Control-Allow-Origin"] == "http://ok.example"


async def test_http_preflight_options():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_options(
        FakeReq("OPTIONS", {"Origin": "http://ok.example"})
    )
    assert resp.status == 204
    assert "POST" in resp.headers["Access-Control-Allow-Methods"]


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
    ver = await _req(h, "resources/read", {"uri": "cronstable://version"})
    contents = ver["result"]["contents"][0]
    assert contents["mimeType"] == "application/json"
    assert json.loads(contents["text"])["jobs"] == 2
    job = await _req(h, "resources/read", {"uri": "cronstable://jobs/hello"})
    assert json.loads(job["result"]["contents"][0]["text"])["name"] == "hello"


async def test_resource_read_unknown_is_32002():
    h = _handler()
    resp = await _req(h, "resources/read", {"uri": "cronstable://jobs/ghost"})
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


async def test_schedule_pressure_engine_clamps_hours():
    # the tool no longer clamps hours itself; the engine's authoritative
    # [1, 168] clamp must still reach the payload through the offload path
    h = _handler()
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_schedule_pressure", "arguments": {"hours": 9999}},
    )
    assert resp["result"]["structuredContent"]["hours"] == 168
    resp = await _req(
        h,
        "tools/call",
        {"name": "cron_schedule_pressure", "arguments": {"hours": -3}},
    )
    assert resp["result"]["structuredContent"]["hours"] == 1


# ---------------------------------------------------------------------------
# schedule authoring/debugging tools: validate, explain, why-no-run
# ---------------------------------------------------------------------------

_SCHED_YAML = """
jobs:
  - name: weekday-report
    command: echo hi
    schedule: "0 9 * * mon,fri"
    utc: true
  - name: friday13
    command: echo spooky
    schedule: "0 0 13 * 5"
    utc: true
  - name: nightly
    command: backup
    schedule: "0 3 * * *"
    enabled: false
    utc: true
  - name: boot
    command: echo boot
    schedule: "@reboot"
  - name: ny-early
    command: echo dst
    schedule: "30 2 * * *"
    timezone: America/New_York
"""


async def _call(handler, name, arguments):
    resp = await _req(
        handler, "tools/call", {"name": name, "arguments": arguments}
    )
    return resp["result"]


async def test_schedule_authoring_tools_registered():
    names = await _tool_names(_handler())
    for expected in (
        "cron_validate_schedule",
        "cron_explain_schedule",
        "cron_why_no_run",
    ):
        assert expected in names


async def test_validate_schedule_accepts_and_lints():
    h = _handler()
    result = await _call(
        h, "cron_validate_schedule", {"expression": "*/7 * * * *"}
    )
    data = result["structuredContent"]
    assert data["valid"] is True
    assert data["description"].startswith("At minutes")
    assert [f["code"] for f in data["lint"]] == ["uneven-step"]
    # the gate returns exactly one confirmation fire
    assert len(data["fires"]) == 1
    assert "valid" in result["content"][0]["text"]


async def test_validate_schedule_rejects_with_engine_error():
    h = _handler()
    result = await _call(
        h, "cron_validate_schedule", {"expression": "0 9 * * mon-fry"}
    )
    data = result["structuredContent"]
    assert data["valid"] is False
    assert "day-of-week" in data["error"]
    assert result["content"][0]["text"].startswith("INVALID")
    # a 7-field Quartz expression with '?' and an nth weekday now parses
    # verbatim (the '#' family is dialect); its description proves the
    # meaning came through
    result = await _call(
        h, "cron_validate_schedule", {"expression": "0 0 12 ? * MON#2 *"}
    )
    data = result["structuredContent"]
    assert data["valid"] is True
    assert "2nd Monday" in data["description"]
    # a Quartz form the dialect still spells differently keeps its hint
    result = await _call(
        h, "cron_validate_schedule", {"expression": "0 0 * * 5L"}
    )
    data = result["structuredContent"]
    assert data["valid"] is False
    assert "Quartz" in data["error"] and "L5" in data["error"]


async def test_validate_schedule_seed_resolves_hash_slots():
    h = _handler()
    # H without a seed is invalid, with the engine's own explanation
    result = await _call(
        h, "cron_validate_schedule", {"expression": "H 3 * * *"}
    )
    assert result["structuredContent"]["valid"] is False
    assert "hash key" in result["structuredContent"]["error"]
    result = await _call(
        h,
        "cron_validate_schedule",
        {"expression": "H 3 * * *", "seed": "newjob"},
    )
    data = result["structuredContent"]
    assert data["valid"] is True
    assert data["resolved"].endswith("3 * * *")
    assert [f["code"] for f in data["lint"]] == ["hashed-slot"]


async def test_validate_schedule_never_fires_is_loud():
    h = _handler()
    result = await _call(
        h, "cron_validate_schedule", {"expression": "0 0 30 2 *"}
    )
    assert result["structuredContent"]["never_fires"] is True
    assert "no future runs" in result["content"][0]["text"]


async def test_explain_schedule_counts_and_frames_fires():
    h = _handler()
    result = await _call(
        h,
        "cron_explain_schedule",
        {"expression": "0 9 * * mon,fri", "count": 3, "tz": "Europe/Berlin"},
    )
    data = result["structuredContent"]
    assert data["valid"] is True
    assert len(data["fires"]) == 3
    assert all(f.endswith(("+02:00", "+01:00")) for f in data["fires"])
    assert data["description"] == "At 09:00, on Monday and Friday"
    # default count is 5; the clamp caps at 60
    result = await _call(
        h, "cron_explain_schedule", {"expression": "* * * * *"}
    )
    assert len(result["structuredContent"]["fires"]) == 5
    result = await _call(
        h, "cron_explain_schedule", {"expression": "* * * * *", "count": 999}
    )
    assert len(result["structuredContent"]["fires"]) == 60


async def test_explain_and_validate_bad_inputs():
    h = _handler()
    for name in ("cron_explain_schedule", "cron_validate_schedule"):
        result = await _call(h, name, {})
        assert result["isError"] is True
        result = await _call(
            h, name, {"expression": "* * * * *", "tz": "Nope/Zone"}
        )
        assert result["isError"] is True
        assert "unknown timezone" in result["content"][0]["text"]


async def test_why_no_run_names_the_failing_field():
    h = _handler(yaml=_SCHED_YAML)
    # Tuesday 2026-07-14 against a Monday/Friday schedule
    result = await _call(
        h,
        "cron_why_no_run",
        {"name": "weekday-report", "at": "2026-07-14T09:00:00"},
    )
    data = result["structuredContent"]
    assert data["matches"] is False
    assert data["failed"] == ["day-of-week"]
    dow = data["checks"][5]
    assert (dow["label"], dow["allowed"]) == ("Tuesday", "Monday and Friday")
    text = result["content"][0]["text"]
    assert text.startswith("NO")
    assert "day-of-week Tuesday is not in Monday and Friday" in text
    # the nearest real fires bracket the probe
    assert data["previous_fire"] == "2026-07-13T09:00:00+00:00"
    assert data["next_fire"] == "2026-07-17T09:00:00+00:00"


async def test_why_no_run_matching_instant_points_at_execution():
    h = _handler(yaml=_SCHED_YAML)
    result = await _call(
        h,
        "cron_why_no_run",
        {"name": "weekday-report", "at": "2026-07-17T09:00:00Z"},
    )
    data = result["structuredContent"]
    assert data["matches"] is True
    assert data["failed"] == []
    assert "cron_list_runs" in result["content"][0]["text"]


async def test_why_no_run_reads_aware_timestamps_in_the_job_zone():
    h = _handler(yaml=_SCHED_YAML)
    # 11:00+02:00 is 09:00 in the job's UTC frame
    result = await _call(
        h,
        "cron_why_no_run",
        {"name": "weekday-report", "at": "2026-07-17T11:00:00+02:00"},
    )
    data = result["structuredContent"]
    assert data["at_in_zone"] == "2026-07-17T09:00:00+00:00"
    assert data["matches"] is True


async def test_why_no_run_and_rule_note():
    h = _handler(yaml=_SCHED_YAML)
    # Monday the 13th: dom matched, dow did not; Vixie would have fired
    result = await _call(
        h, "cron_why_no_run", {"name": "friday13", "at": "2026-07-13T00:00"}
    )
    data = result["structuredContent"]
    assert [n["code"] for n in data["notes"]] == ["day-fields-and-rule"]
    assert "Vixie" in data["notes"][0]["message"]


async def test_why_no_run_disabled_job_is_called_out():
    h = _handler(yaml=_SCHED_YAML)
    result = await _call(
        h, "cron_why_no_run", {"name": "nightly", "at": "2026-07-18T03:00:00"}
    )
    assert result["structuredContent"]["matches"] is True
    assert result["structuredContent"]["enabled"] is False
    assert "disabled" in result["content"][0]["text"]


async def test_why_no_run_reboot_job():
    h = _handler(yaml=_SCHED_YAML)
    result = await _call(
        h, "cron_why_no_run", {"name": "boot", "at": "2026-07-18T03:00"}
    )
    data = result["structuredContent"]
    assert data["reboot"] is True
    assert data["matches"] is False
    assert data["previous_fire"] is None
    assert "@reboot" in result["content"][0]["text"]


async def test_why_no_run_dst_gap_reaches_the_summary():
    h = _handler(yaml=_SCHED_YAML)
    # 2026-03-08 02:30 does not exist in America/New_York
    result = await _call(
        h, "cron_why_no_run", {"name": "ny-early", "at": "2026-03-08T02:30"}
    )
    data = result["structuredContent"]
    assert data["matches"] is True
    assert [n["code"] for n in data["notes"]] == ["dst-skipped-time"]
    assert "does not exist" in result["content"][0]["text"]


async def test_why_no_run_unknown_job_and_bad_timestamp():
    h = _handler(yaml=_SCHED_YAML)
    result = await _call(
        h, "cron_why_no_run", {"name": "nope", "at": "2026-07-18T03:00"}
    )
    assert result["isError"] is True
    # the lookup resolves DAG schedules too, so the reason names both and
    # points at both listers (tests/test_mcp_tools.py holds the full pin)
    assert "no job or workflow schedule named" in result["content"][0]["text"]
    result = await _call(
        h, "cron_why_no_run", {"name": "weekday-report", "at": "yesterday"}
    )
    assert result["isError"] is True
    assert "ISO 8601" in result["content"][0]["text"]
    result = await _call(h, "cron_why_no_run", {"name": "weekday-report"})
    assert result["isError"] is True


# ---------------------------------------------------------------------------
# tool results carry the data in `content` too
# ---------------------------------------------------------------------------

_EVERYTHING = {
    "readOnly": False,
    "toolsets": ["observe", "act", "dags", "state"],
}

#: One call per registered tool. Against the stateless _YAML handler the
#: DAG, pool-mutating and unconfirmed calls end in isError; the success
#: paths of those tools run through test_mcp_tools.py's _call, which checks
#: the same invariant on every result.
_ONE_CALL_EACH = {
    "cron_get_status": {},
    "cron_list_jobs": {},
    "cron_get_job": {"name": "hello"},
    "cron_list_runs": {"name": "hello"},
    "cron_get_job_trends": {"name": "hello"},
    "cron_get_job_resources": {"name": "hello"},
    "cron_get_cluster": {},
    "cron_get_fleet": {},
    "cron_get_node": {},
    "cron_query_metrics": {"limit": 3},
    "cron_get_version": {},
    "cron_tail_job_logs": {"name": "hello"},
    "cron_schedule_pressure": {},
    "cron_schedule_duplicates": {},
    "cron_suggest_slot": {},
    "cron_validate_schedule": {"expression": "*/5 * * * *"},
    "cron_explain_schedule": {"expression": "*/5 * * * *"},
    "cron_why_no_run": {"name": "hello", "at": "2026-07-14T09:00:00"},
    "cron_list_pools": {},
    "cron_list_dags": {},
    "cron_list_dag_runs": {"dag": "ghost"},
    "cron_get_dag_run": {"dag": "ghost", "run_key": "r"},
    "cron_get_dag_xcom": {"dag": "ghost", "run_key": "r"},
    "cron_tail_dag_task_logs": {
        "dag": "ghost",
        "run_key": "r",
        "taskkey": "t",
    },
    "cron_preview_recovery": {"dag": "ghost", "run_key": "r"},
    "cron_inspect_state": {},
    "cron_cancel_queued": {"pool": "p", "id": "i"},
    "cron_run_job": {"name": "hello"},
    "cron_cancel_job": {"name": "hello", "confirm": True},
    "cron_pause_job": {"name": "hello", "confirm": True},
    "cron_resume_job": {"name": "hello", "confirm": True},
    "cron_trigger_dag": {"dag": "ghost", "confirm": True},
    "cron_backfill_dag": {"dag": "ghost", "from": "a", "to": "b"},
    "cron_recover_dag": {"dag": "ghost", "plan_token": "x"},
    "cron_decide_gate": {
        "dag": "ghost",
        "run_key": "r",
        "taskkey": "t",
        "decision": "approve",
    },
}


async def test_every_tool_result_carries_its_json_in_content():
    # A client that passes only `content` to the model (Claude Desktop) must
    # still see the data, so a success adds structuredContent as JSON text
    # after the summary; an error stays a single readable block.
    h = _handler(_EVERYTHING)
    assert set(_ONE_CALL_EACH) == set(await _tool_names(h))
    successes = 0
    for name, arguments in _ONE_CALL_EACH.items():
        result = await _call(h, name, arguments)
        if result.get("isError"):
            assert len(result["content"]) == 1, name
            assert "structuredContent" not in result, name
            continue
        successes += 1
        summary, data = result["content"]
        assert summary["type"] == data["type"] == "text"
        assert json.loads(data["text"]) == result["structuredContent"], name
    assert successes >= 20


# ---------------------------------------------------------------------------
# resource template URIs are percent-decoded
# ---------------------------------------------------------------------------

_ODD_NAMES = ["nightly backup", "a/b", "c#d", "50%", "café ✓"]


def _uri(template, **values):
    for key, value in values.items():
        template = template.replace(
            "{" + key + "}", urllib.parse.quote(value, safe="")
        )
    return template


async def test_job_templates_round_trip_encoded_names():
    yaml = "jobs:\n" + "".join(
        "  - name: {}\n    command: echo\n    schedule: '* * * * *'\n".format(
            json.dumps(name)
        )
        for name in _ODD_NAMES
    )
    h = _handler(yaml=yaml)
    for name in _ODD_NAMES:
        job = await _req(
            h,
            "resources/read",
            {"uri": _uri("cronstable://jobs/{name}", name=name)},
        )
        assert json.loads(job["result"]["contents"][0]["text"])["name"] == name
        runs = await _req(
            h,
            "resources/read",
            {"uri": _uri("cronstable://jobs/{name}/runs", name=name)},
        )
        assert "result" in runs, (name, runs)


async def test_dag_and_state_templates_decode_every_captured_value(
    monkeypatch,
):
    # the loaders are bound when the handler is built, so record what each
    # template hands them.
    cron = Cron(None, config_yaml=_YAML)
    cron.web_config = {}
    seen = []

    async def record(*args):
        seen.append(args)
        return {"ok": True}

    async def dags():
        return [{"name": name} for name in _ODD_NAMES]

    monkeypatch.setattr(cron._dag, "get_run", record)
    monkeypatch.setattr(cron, "state_documents_payload", record)
    monkeypatch.setattr(cron, "dags_payload", dags)
    h = MCPHandler(
        cron,
        _build_mcp_config(
            {"enabled": True, "toolsets": ["observe", "dags", "state"]}
        ),
    )
    for name in _ODD_NAMES:
        detail = await _req(
            h,
            "resources/read",
            {"uri": _uri("cronstable://dags/{name}", name=name)},
        )
        body = json.loads(detail["result"]["contents"][0]["text"])
        assert body == {"name": name}
        run_key = name + " run"
        await _req(
            h,
            "resources/read",
            {
                "uri": _uri(
                    "cronstable://dags/{name}/runs/{run_key}",
                    name=name,
                    run_key=run_key,
                )
            },
        )
        await _req(
            h,
            "resources/read",
            {"uri": "cronstable://state/kv/" + urllib.parse.quote(name)},
        )
        assert seen[-2:] == [(name, run_key), ("kv/" + name,)]


# ---------------------------------------------------------------------------
# prompts name only tools the client can see
# ---------------------------------------------------------------------------

_TOOLSETS = ("observe", "act", "dags", "state")


def _toolset_combos():
    for size in range(1, len(_TOOLSETS) + 1):
        yield from itertools.combinations(_TOOLSETS, size)


@pytest.mark.parametrize("read_only", [True, False])
@pytest.mark.parametrize("scopes", [None, frozenset({"view"})])
async def test_every_served_prompt_names_only_served_tools(read_only, scopes):
    # A prompt that tells the model to call a tool it cannot see wastes the
    # turn, so for every configuration a rendered prompt may name only
    # tools that tools/list returns to the same caller.
    caller = None if scopes is None else mcp_mod._Caller("t", scopes)
    token = mcp_mod._caller.set(caller)
    try:
        for combo in _toolset_combos():
            h = _handler({"readOnly": read_only, "toolsets": list(combo)})
            tools = set(await _tool_names(h))
            listed = (await _req(h, "prompts/list"))["result"]["prompts"]
            for prompt in listed:
                args = {a["name"]: "x" for a in prompt["arguments"]}
                got = await _req(
                    h,
                    "prompts/get",
                    {"name": prompt["name"], "arguments": args},
                )
                text = got["result"]["messages"][0]["content"]["text"]
                named = set(re.findall(r"cron_\w+", text))
                assert named, prompt["name"]
                assert named <= tools, (combo, prompt["name"], named - tools)
    finally:
        mcp_mod._caller.reset(token)


async def test_prompt_gating_under_the_default_and_read_only_configs():
    # default observe-only: blast_radius is served without its DAG and
    # state steps
    h = _handler()
    got = await _req(
        h,
        "prompts/get",
        {"name": "blast_radius", "arguments": {"target": "j"}},
    )
    text = got["result"]["messages"][0]["content"]["text"]
    assert "cron_get_fleet" in text
    assert "cron_list_dags" not in text
    assert "cron_inspect_state" not in text
    # readOnly removes cron_backfill_dag, so backfill_plan goes with it
    h = _handler({"toolsets": ["observe", "dags"]})
    names = {
        p["name"] for p in (await _req(h, "prompts/list"))["result"]["prompts"]
    }
    assert "why_did_dag_run_fail" in names
    assert "backfill_plan" not in names
    resp = await _req(
        h,
        "prompts/get",
        {
            "name": "backfill_plan",
            "arguments": {"dag": "d", "from": "a", "to": "b"},
        },
    )
    assert resp["error"]["code"] == -32602
    h = _handler({"readOnly": False, "toolsets": ["observe", "dags"]})
    names = {
        p["name"] for p in (await _req(h, "prompts/list"))["result"]["prompts"]
    }
    assert "backfill_plan" in names


# ---------------------------------------------------------------------------
# prompts/get validates its arguments
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"name": ["triage_job_failure"]}, id="name-not-string"),
        pytest.param({"name": "triage_job_failure"}, id="required-missing"),
        pytest.param(
            {"name": "triage_job_failure", "arguments": {"job": ""}},
            id="required-empty",
        ),
        pytest.param(
            {"name": "triage_job_failure", "arguments": {"job": ["hello"]}},
            id="value-not-string",
        ),
        pytest.param(
            {"name": "triage_job_failure", "arguments": [1]},
            id="arguments-not-object",
        ),
    ],
)
async def test_prompts_get_rejects_bad_arguments(params, caplog):
    h = _handler()
    resp = await _req(h, "prompts/get", params)
    assert resp["error"]["code"] == -32602
    # a validation failure, not an internal error with a traceback
    assert "internal error" not in caplog.text


async def test_prompts_get_argumentless_prompt_needs_no_arguments():
    h = _handler()
    got = await _req(h, "prompts/get", {"name": "fleet_health_summary"})
    assert "cron_get_fleet" in got["result"]["messages"][0]["content"]["text"]


# ---------------------------------------------------------------------------
# 2025-11-25 wire conformance: responses, request ids, the version header,
# and unknown tool arguments
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "response",
    [
        pytest.param({"jsonrpc": "2.0", "id": 1, "result": {}}, id="result"),
        pytest.param(
            {"jsonrpc": "2.0", "id": 1, "error": {"code": 1, "message": "x"}},
            id="error",
        ),
    ],
)
async def test_posted_response_is_refused(response):
    # the server sends no requests, so a response can only be a client bug:
    # the transport allows 202 or an HTTP error, never a 200 reply.
    h = _handler()
    resp = await h.handle_http(_post_req(response))
    assert resp.status == 400
    assert "responses are not accepted" in json.loads(resp.body)["error"]
    # and nothing answers a reply, whatever the transport
    assert await h.handle_message(response) is None


@pytest.mark.parametrize(
    "bad_id",
    [
        pytest.param(None, id="null"),
        pytest.param(True, id="boolean"),
        pytest.param(1.5, id="float"),
        pytest.param({}, id="object"),
        pytest.param([], id="array"),
    ],
)
async def test_request_id_must_be_a_string_or_integer(bad_id):
    h = _handler()
    resp = await h.handle_message(
        {"jsonrpc": "2.0", "id": bad_id, "method": "ping"}
    )
    assert resp == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": -32600,
            "message": "request id must be a string or an integer",
        },
    }


@pytest.mark.parametrize("good_id", ["abc", 7, 0, -3])
async def test_string_and_integer_ids_work(good_id):
    h = _handler()
    resp = await h.handle_message(
        {"jsonrpc": "2.0", "id": good_id, "method": "ping"}
    )
    assert resp == {"jsonrpc": "2.0", "id": good_id, "result": {}}


@pytest.mark.parametrize(
    ("sent", "echoed"),
    [
        pytest.param(None, "2025-11-25", id="missing"),
        pytest.param("2025-06-18", "2025-06-18", id="negotiated-older"),
        pytest.param("2025-03-26", "2025-03-26", id="oldest"),
    ],
)
async def test_response_echoes_the_negotiated_version(sent, echoed):
    h = _handler()
    headers = {"MCP-Protocol-Version": sent} if sent else {}
    resp = await h.handle_http(
        _post_req({"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers)
    )
    assert resp.status == 200
    assert resp.headers["MCP-Protocol-Version"] == echoed


async def test_initialize_reply_names_the_version_it_negotiated():
    # the first request carries no version header; the reply's header must
    # still name the revision the rest of the session uses
    h = _handler()
    resp = await h.handle_http(
        _post_req(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            }
        )
    )
    assert resp.headers["MCP-Protocol-Version"] == "2025-06-18"


async def test_initialize_ignores_an_unhashable_version():
    h = _handler()
    resp = await _req(h, "initialize", {"protocolVersion": ["2025-06-18"]})
    assert resp["result"]["protocolVersion"] == "2025-11-25"


async def test_unknown_tool_arguments_are_named_with_the_allowed_ones():
    # every input schema declares additionalProperties: false; a model that
    # passes `job` instead of `name` must be told so, not handed a
    # confusing missing-argument error or a silently dropped filter.
    h = _handler()
    result = await _call(h, "cron_get_job", {"job": "hello"})
    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert "'job'" in text and "accepts name" in text
    result = await _call(h, "cron_list_jobs", {"filter": "hel", "stat": "x"})
    assert result["isError"] is True
    assert "filter, state, offset, limit" in result["content"][0]["text"]
    result = await _call(h, "cron_get_version", {"verbose": True})
    assert "accepts no arguments" in result["content"][0]["text"]
    # known keys behave as before
    result = await _call(h, "cron_get_job", {"name": "hello"})
    assert result["structuredContent"]["name"] == "hello"


# ---------------------------------------------------------------------------
# server identity: description, website and the dashboard's icon
# ---------------------------------------------------------------------------


async def test_initialize_server_info_carries_identity_and_icon():
    h = _handler()
    info = (await _req(h, "initialize", {"protocolVersion": "2025-11-25"}))[
        "result"
    ]["serverInfo"]
    assert info["name"] == "cronstable"
    assert info["websiteUrl"] == "https://github.com/ptweezy/cronstable"
    assert info["description"]
    (icon,) = info["icons"]
    assert icon["mimeType"] == "image/png"
    assert icon["sizes"] == ["32x32"]
    prefix = "data:image/png;base64,"
    assert icon["src"].startswith(prefix)
    png = base64.b64decode(icon["src"][len(prefix) :], validate=True)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    # the IHDR chunk's width and height
    assert int.from_bytes(png[16:20], "big") == 32
    assert int.from_bytes(png[20:24], "big") == 32


async def test_server_info_omits_icons_when_the_page_has_none(monkeypatch):
    monkeypatch.setattr(mcp_mod, "_load_index_bytes", lambda: b"<html></html>")
    mcp_mod._server_icons.cache_clear()
    try:
        info = (await _req(_handler(), "initialize", {}))["result"][
            "serverInfo"
        ]
    finally:
        mcp_mod._server_icons.cache_clear()
    assert "icons" not in info
    assert info["websiteUrl"]


# ---------------------------------------------------------------------------
# the documented tool counts match what the configs serve
# ---------------------------------------------------------------------------

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CHECK_LINE = re.compile(r"mcp check: ok - protocol .*?, (\d+) tool\(s\)")


def _documented_counts(path):
    return [int(n) for n in _CHECK_LINE.findall(path.read_text("utf-8"))]


async def test_documented_tool_counts_match_the_configs(monkeypatch):
    # wiki/MCP.md shows a --check against the default config, and the demo
    # README one against the demo config
    assert _documented_counts(_ROOT / "wiki" / "MCP.md") == [
        len(await _tool_names(_handler()))
    ]
    monkeypatch.setenv("CRONSTABLE_WEB_TOKEN", "dev-token")
    example = _ROOT / "example" / "mcp"
    cfg = parse_config_string(
        (example / "cronstable.yaml").read_text("utf-8"), "cronstable.yaml"
    )
    demo = MCPHandler(Cron(None, config_yaml=_YAML), cfg.mcp_config)
    assert _documented_counts(example / "README.md") == [
        len(await _tool_names(demo))
    ]
