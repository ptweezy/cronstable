"""MCP ``2026-07-28`` served beside the legacy revisions (:mod:`cronstable.mcp`).

A request whose ``_meta`` names a protocol version (or whose
MCP-Protocol-Version header names a modern revision) is served statelessly:
``server/discover``, header validation, ``resultType``, cache hints and the
modern error codes. Everything else keeps the ``initialize`` flow that
``test_mcp.py`` covers. The bridge's own header derivation
(:func:`cronstable.mcpcli._modern_headers`) builds the headers here, so the
two sides are checked against each other, and the last tests drive the real
bridge against a real daemon.
"""

import asyncio
import base64
import io
import json
import sys

import pytest

from cronstable import mcp as mcp_mod
from cronstable import mcpcli
from cronstable.config import _build_mcp_config
from cronstable.cron import Cron
from tests.test_mcp import _YAML, _handler, _post_req

_PV = mcp_mod.META_PROTOCOL_VERSION
_CAPS = mcp_mod.META_CLIENT_CAPABILITIES
_INFO = mcp_mod.META_SERVER_INFO
_MODERN = "2026-07-28"


def _modern(method, params=None, *, mid=1, version=_MODERN, meta=None):
    """A modern request: the version and capabilities ride in ``_meta``."""
    body = dict(params or {})
    body["_meta"] = (
        meta if meta is not None else {_PV: version, _CAPS: {}}
    )
    return {"jsonrpc": "2.0", "id": mid, "method": method, "params": body}


async def _post(handler, msg, headers=None, *, derive=True):
    """POST ``msg`` with the headers the bridge would send, plus overrides
    (a None value removes a header)."""
    sent = dict(mcpcli._modern_headers(msg) or {}) if derive else {}
    for key, value in (headers or {}).items():
        if value is None:
            sent.pop(key, None)
        else:
            sent[key] = value
    resp = await handler.handle_http(_post_req(msg, sent))
    return resp, json.loads(resp.body) if resp.body else None


def _everything():
    return _handler(
        {"readOnly": False, "toolsets": ["observe", "act", "dags", "state"]}
    )


# ---------------------------------------------------------------------------
# server/discover
# ---------------------------------------------------------------------------


async def test_discover_result_shape():
    resp, body = await _post(_handler(), _modern("server/discover"))
    assert resp.status == 200
    assert resp.headers["MCP-Protocol-Version"] == _MODERN
    result = body["result"]
    assert result["resultType"] == "complete"
    assert result["supportedVersions"] == [
        "2026-07-28",
        "2025-11-25",
        "2025-06-18",
        "2025-03-26",
    ]
    assert result["capabilities"]["tools"] == {"listChanged": False}
    assert result["capabilities"]["completions"] == {}
    assert "cron_get_status" in result["instructions"]
    info = result["_meta"][_INFO]
    assert info["name"] == "cronstable"
    assert info["websiteUrl"] == mcp_mod.SERVER_WEBSITE
    assert info["icons"][0]["mimeType"] == "image/png"
    assert (result["ttlMs"], result["cacheScope"]) == (60000, "private")


async def test_discover_is_not_a_legacy_method():
    resp = await _handler().handle_message(
        {"jsonrpc": "2.0", "id": 1, "method": "server/discover"}
    )
    assert resp["error"]["code"] == -32601


# ---------------------------------------------------------------------------
# request validation, in the order the checks run
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("msg", "headers", "status", "code"),
    [
        pytest.param(
            _modern("tools/list", meta={_CAPS: {}}),
            {"MCP-Protocol-Version": _MODERN, "Mcp-Method": "tools/list"},
            400,
            -32602,
            id="meta-lacks-version",
        ),
        pytest.param(
            _modern("tools/list", meta={_PV: _MODERN}),
            None,
            400,
            -32602,
            id="meta-lacks-capabilities",
        ),
        pytest.param(
            _modern("tools/list", meta={_PV: 20260728, _CAPS: {}}),
            {"MCP-Protocol-Version": _MODERN, "Mcp-Method": "tools/list"},
            400,
            -32602,
            id="meta-version-not-string",
        ),
        pytest.param(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            {"MCP-Protocol-Version": _MODERN, "Mcp-Method": "tools/list"},
            400,
            -32602,
            id="modern-header-without-meta",
        ),
        pytest.param(
            _modern("tools/list"),
            {"MCP-Protocol-Version": None},
            400,
            -32020,
            id="version-header-missing",
        ),
        pytest.param(
            _modern("tools/list"),
            {"MCP-Protocol-Version": "2025-11-25"},
            400,
            -32020,
            id="version-header-differs",
        ),
        pytest.param(
            _modern("tools/list"),
            {"Mcp-Method": None},
            400,
            -32020,
            id="method-header-missing",
        ),
        pytest.param(
            _modern("tools/list"),
            {"Mcp-Method": "prompts/list"},
            400,
            -32020,
            id="method-header-differs",
        ),
        pytest.param(
            _modern("tools/call", {"name": "cron_get_status"}),
            {"Mcp-Name": None},
            400,
            -32020,
            id="name-header-missing",
        ),
        pytest.param(
            _modern("tools/call", {"name": "cron_get_status"}),
            {"Mcp-Name": "cron_get_version"},
            400,
            -32020,
            id="name-header-differs",
        ),
        pytest.param(
            _modern("tools/call", {"name": "cron_get_status"}),
            {"Mcp-Name": "=?base64?not base64!?="},
            400,
            -32020,
            id="name-header-malformed-base64",
        ),
        pytest.param(
            _modern("resources/read", {"uri": "cronstable://status"}),
            {"Mcp-Name": "cronstable://version"},
            400,
            -32020,
            id="uri-header-differs",
        ),
        pytest.param(
            _modern("prompts/get", {"name": "fleet_health_summary"}),
            {"Mcp-Name": None},
            400,
            -32020,
            id="prompt-name-header-missing",
        ),
        pytest.param(
            _modern("tools/list", version="1900-01-01"),
            None,
            400,
            -32022,
            id="unsupported-version",
        ),
        pytest.param(
            _modern("resources/subscribe"),
            None,
            404,
            -32601,
            id="unknown-method",
        ),
    ],
)
async def test_modern_request_validation(msg, headers, status, code):
    resp, body = await _post(_handler(), msg, headers)
    assert resp.status == status
    assert body["error"]["code"] == code
    # the error names the request it answers
    assert body["id"] == 1


async def test_meta_check_runs_before_the_header_checks():
    # _meta lacks the capabilities AND the headers are absent: -32602
    msg = _modern("tools/list", meta={_PV: _MODERN})
    _resp, body = await _post(_handler(), msg, derive=False)
    assert body["error"]["code"] == -32602


async def test_header_checks_run_before_the_version_check():
    msg = _modern("tools/list", version="1900-01-01")
    _resp, body = await _post(
        _handler(), msg, {"MCP-Protocol-Version": _MODERN}
    )
    assert body["error"]["code"] == -32020


async def test_unsupported_version_lists_what_is_served():
    msg = _modern("tools/list", version="1900-01-01")
    _resp, body = await _post(_handler(), msg)
    assert body["error"]["data"] == {
        "supported": list(mcp_mod.ALL_PROTOCOL_VERSIONS),
        "requested": "1900-01-01",
    }


async def test_base64_names_are_decoded_before_comparing():
    # the bridge encodes a non-ASCII URI; the server must decode it
    msg = _modern("resources/read", {"uri": "cronstable://jobs/café"})
    sent = mcpcli._modern_headers(msg)["Mcp-Name"]
    assert sent.startswith("=?base64?")
    resp, body = await _post(
        _handler(yaml=_YAML.replace("name: hello", "name: café")), msg
    )
    assert resp.status == 200, body
    content = json.loads(body["result"]["contents"][0]["text"])
    assert content["name"] == "café"
    # and a plain value that merely looks encoded is encoded too
    literal = "=?base64?literal?="
    encoded = mcpcli._encode_header_value(literal)
    assert encoded != literal
    assert mcp_mod._decode_header_value(encoded) == literal
    raw = base64.b64encode("Hello, 世界".encode()).decode()
    assert mcp_mod._decode_header_value("=?base64?" + raw + "?=") == (
        "Hello, 世界"
    )


async def test_modern_notification_is_accepted():
    # 2026-07-28 defines no header rules for notifications, and the modern
    # version header must not trip the legacy version check either
    msg = _modern("notifications/cancelled")
    del msg["id"]
    for headers in ({}, {"MCP-Protocol-Version": _MODERN}):
        resp, _body = await _post(_handler(), msg, headers, derive=False)
        assert resp.status == 202, headers
        assert resp.headers["MCP-Protocol-Version"] == (
            headers.get("MCP-Protocol-Version", "2025-11-25")
        )


async def test_unsupported_legacy_header_is_refused_for_any_body():
    resp = await _handler().handle_http(
        _post_req(None, {"MCP-Protocol-Version": "1999-01-01"}, body=b'"hi"')
    )
    assert resp.status == 400


async def test_ping_and_initialize_are_legacy_only():
    for method in ("ping", "initialize"):
        resp, body = await _post(_handler(), _modern(method))
        assert resp.status == 404, method
        assert body["error"]["code"] == -32601


async def test_subscriptions_listen_is_not_implemented():
    # no listChanged and no resource subscriptions are advertised, so the
    # listen stream would never carry a notification
    resp, body = await _post(
        _handler(), _modern("subscriptions/listen", {"notifications": {}})
    )
    assert resp.status == 404
    assert body["error"]["code"] == -32601


async def test_cors_preflight_allows_the_mirrored_headers():
    h = _handler({"allowedOrigins": ["http://ok.example"]})
    resp = await h.handle_options(
        _post_req(None, {"Origin": "http://ok.example"}, body=b"")
    )
    allowed = resp.headers["Access-Control-Allow-Headers"]
    assert "Mcp-Method" in allowed and "Mcp-Name" in allowed


# ---------------------------------------------------------------------------
# result decoration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "params", "ttl"),
    [
        pytest.param("tools/list", None, 60000, id="tools"),
        pytest.param("resources/list", None, 60000, id="resources"),
        pytest.param("resources/templates/list", None, 60000, id="templates"),
        pytest.param("prompts/list", None, 60000, id="prompts"),
        pytest.param(
            "resources/read", {"uri": "cronstable://status"}, 0, id="read"
        ),
    ],
)
async def test_cacheable_results_carry_hints(method, params, ttl):
    resp, body = await _post(_handler(), _modern(method, params))
    assert resp.status == 200, body
    result = body["result"]
    assert result["resultType"] == "complete"
    assert result["ttlMs"] == ttl
    assert result["cacheScope"] == "private"
    assert result["_meta"][_INFO] == {
        "name": "cronstable",
        "title": "cronstable",
        "version": mcp_mod._version.version,
    }


async def test_other_results_carry_result_type_and_server_info_only():
    for method, params in (
        ("tools/call", {"name": "cron_get_version"}),
        ("prompts/get", {"name": "fleet_health_summary"}),
        (
            "completion/complete",
            {
                "ref": {"type": "ref/prompt", "name": "triage_job_failure"},
                "argument": {"name": "job", "value": "h"},
            },
        ),
    ):
        resp, body = await _post(_handler(), _modern(method, params))
        assert resp.status == 200, (method, body)
        result = body["result"]
        assert result["resultType"] == "complete", method
        assert _INFO in result["_meta"], method
        assert "ttlMs" not in result and "cacheScope" not in result, method


async def test_tool_results_keep_their_shape_on_the_modern_path():
    resp, body = await _post(
        _everything(), _modern("tools/call", {"name": "cron_list_jobs"})
    )
    result = body["result"]
    assert len(result["content"]) == 2
    assert json.loads(result["content"][1]["text"]) == (
        result["structuredContent"]
    )


# ---------------------------------------------------------------------------
# error codes by era
# ---------------------------------------------------------------------------


async def test_resource_not_found_code_depends_on_the_era():
    uri = {"uri": "cronstable://jobs/ghost"}
    resp, body = await _post(_handler(), _modern("resources/read", uri))
    assert resp.status == 200
    assert body["error"]["code"] == -32602
    legacy = await _handler().handle_message(
        {"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": uri}
    )
    assert legacy["error"]["code"] == -32002


async def test_legacy_errors_never_carry_modern_codes():
    # a client reads -32020..-32022 as proof of a modern server and stops
    # falling back, so the legacy path keeps its plain HTTP error bodies
    h = _handler()
    resp = await h.handle_http(
        _post_req(
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            {"MCP-Protocol-Version": "1999-01-01"},
        )
    )
    assert resp.status == 400
    assert json.loads(resp.body) == {
        "error": "unsupported MCP-Protocol-Version"
    }


# ---------------------------------------------------------------------------
# the legacy flow is unchanged
# ---------------------------------------------------------------------------


async def test_legacy_initialize_flow_is_undecorated():
    h = _handler()
    init = await h.handle_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        }
    )
    assert "resultType" not in init["result"]
    assert "_meta" not in init["result"]
    assert await h.handle_message(
        {"jsonrpc": "2.0", "method": "notifications/initialized"}
    ) is None
    listed = await h.handle_message(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    )
    assert "ttlMs" not in listed["result"]
    assert listed["result"]["tools"]
    ping = await h.handle_message({"jsonrpc": "2.0", "id": 3, "method": "ping"})
    assert ping["result"] == {}


async def test_handle_message_serves_modern_requests_without_headers():
    # handle_message is transport independent: the _meta alone selects the
    # modern path, and the header checks are HTTP's
    resp = await _handler().handle_message(_modern("tools/list"))
    assert resp["result"]["resultType"] == "complete"
    resp = await _handler().handle_message(_modern("ping"))
    assert resp["error"]["code"] == -32601


# ---------------------------------------------------------------------------
# the real bridge against a real daemon
# ---------------------------------------------------------------------------


@pytest.fixture
async def daemon():
    cron = Cron(None, config_yaml=_YAML.replace("name: hello", "name: café"))
    await cron.start_stop_web_app(
        {"listen": ["http://127.0.0.1:0"]},
        _build_mcp_config({"enabled": True}),
    )
    try:
        port = cron.web_runner.addresses[0][1]
        yield "http://127.0.0.1:{}".format(port)
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)


def _bridge_args(url):
    return mcpcli.argparse.Namespace(
        url=url,
        token=None,
        token_env="CRONSTABLE_TEST_NO_SUCH_TOKEN",
        protocol_version=None,
        timeout=10.0,
        mcp_check=True,
        cacert=None,
        client_cert=None,
        client_key=None,
        insecure=False,
    )


async def test_check_reports_the_modern_era(daemon, capsys):
    loop = asyncio.get_running_loop()
    code = await loop.run_in_executor(
        None, mcpcli._check, _bridge_args(daemon)
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "protocol 2026-07-28 (modern; the daemon serves 2026-07-28" in err
    assert "19 tool(s)" in err


async def test_bridge_carries_both_eras_to_the_daemon(daemon, monkeypatch):
    frames = [
        _modern("server/discover", mid="d"),
        _modern(
            "resources/read", {"uri": "cronstable://jobs/café"}, mid="r"
        ),
        _modern("tools/list", version="1900-01-01", mid="v"),
        {"jsonrpc": "2.0", "id": "i", "method": "initialize", "params": {}},
        {"jsonrpc": "2.0", "id": "p", "method": "ping"},
    ]
    stdin = io.StringIO("".join(json.dumps(f) + "\n" for f in frames))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)
    args = _bridge_args(daemon)
    args.mcp_check = False
    loop = asyncio.get_running_loop()
    assert await loop.run_in_executor(None, mcpcli._run_bridge, args) == 0
    replies = {
        r["id"]: r for r in map(json.loads, stdout.getvalue().splitlines())
    }
    assert replies["d"]["result"]["supportedVersions"][0] == _MODERN
    job = json.loads(replies["r"]["result"]["contents"][0]["text"])
    assert job["name"] == "café"
    # the modern error arrives as the daemon sent it, not as a bridge error
    assert replies["v"]["error"]["code"] == -32022
    assert replies["i"]["result"]["protocolVersion"] == "2025-11-25"
    assert replies["p"]["result"] == {}
