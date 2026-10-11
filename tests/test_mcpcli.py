"""Tests for the ``cronstable mcp`` stdio bridge (:mod:`cronstable.mcpcli`).

The bridge is a synchronous line proxy over two seams: ``_post`` (one HTTP
round-trip via ``webclient.OPENER``) and stdin/stdout.  ``_post``
itself is tested against a fake opener; everything above it monkeypatches
``_post`` with a scripted recorder, in the same single-seam style as
``test_state_job_cli.py``.  Import isolation lives in ``test_mcp.py``.
"""

import argparse
import http.client
import io
import json
import ssl
import sys
import urllib.error

import pytest

from cronstable import _cliargs, mcpcli, webclient


def _args(**overrides):
    ns = argparse.Namespace(
        url=mcpcli.DEFAULT_URL,
        token=None,
        token_env=None,
        protocol_version=None,
        timeout=1.0,
        mcp_check=False,
        cacert=None,
        client_cert=None,
        client_key=None,
        insecure=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


# ---------------------------------------------------------------------------
# token resolution
# ---------------------------------------------------------------------------


def test_resolve_token_prefers_flag(monkeypatch):
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "from-env")
    assert webclient.resolve_token(_args(token="flag")) == "flag"


def test_resolve_token_default_env(monkeypatch):
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "from-env")
    assert webclient.resolve_token(_args()) == "from-env"


def test_resolve_token_custom_env(monkeypatch):
    monkeypatch.delenv(_cliargs.WEB_ENV_TOKEN, raising=False)
    monkeypatch.setenv("OTHER_TOKEN", "other")
    assert webclient.resolve_token(_args(token_env="OTHER_TOKEN")) == "other"


def test_resolve_token_absent(monkeypatch):
    monkeypatch.delenv(_cliargs.WEB_ENV_TOKEN, raising=False)
    assert webclient.resolve_token(_args()) is None


# ---------------------------------------------------------------------------
# TLS resolution and the opener it selects
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_tls_env(monkeypatch):
    """No CRONSTABLE_WEB_* TLS variable leaks in from the developer's shell.

    Autouse because the drivers now resolve TLS on every run: an exported
    CRONSTABLE_WEB_CACERT would otherwise fail tests that have nothing to do
    with TLS.  Tests that want a variable set request the fixture and use the
    monkeypatch it returns.
    """
    for name in (
        _cliargs.WEB_ENV_CACERT,
        _cliargs.WEB_ENV_CLIENT_CERT,
        _cliargs.WEB_ENV_CLIENT_KEY,
        _cliargs.WEB_ENV_INSECURE,
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_resolve_tls_none_without_flags(_no_tls_env):
    # the plaintext default: no context at all, so the bridge keeps the
    # transport it had before TLS existed.
    assert webclient.resolve_tls(_args()) is None


def test_resolve_tls_insecure_warns_on_stderr(_no_tls_env, capsys):
    ctx = webclient.resolve_tls(_args(insecure=True))
    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_NONE
    # the warning is the point: verification is off while the token still
    # goes out, so this must never be silent.
    err = capsys.readouterr().err
    assert "--insecure" in err
    assert "bearer token" in err


def test_resolve_tls_insecure_via_env_also_warns(_no_tls_env, capsys):
    _no_tls_env.setenv(_cliargs.WEB_ENV_INSECURE, "yes")
    assert webclient.resolve_tls(_args()).verify_mode == ssl.CERT_NONE
    assert "--insecure" in capsys.readouterr().err


def test_resolve_tls_cacert_flag_beats_env(_no_tls_env, tmp_path):
    # flag-then-env precedence, asserted through the error: the flag's path is
    # the one that gets opened.
    _no_tls_env.setenv(_cliargs.WEB_ENV_CACERT, str(tmp_path / "from-env.pem"))
    with pytest.raises(webclient.ClientError) as caught:
        webclient.resolve_tls(_args(cacert=str(tmp_path / "from-flag.pem")))
    assert "from-flag.pem" in str(caught.value)


def test_resolve_tls_bad_path_is_a_clean_error(_no_tls_env, tmp_path):
    # an unreadable CA must not exit with a traceback out of ssl.
    with pytest.raises(webclient.ClientError, match="TLS material"):
        webclient.resolve_tls(_args(cacert=str(tmp_path / "absent.pem")))


def test_build_opener_without_context_is_the_shared_global():
    # identity, not equality: OPENER is the monkeypatch seam, so the no-TLS
    # path must hand back that very object.
    assert webclient.build_opener(None) is webclient.OPENER


def test_build_opener_with_context_is_a_separate_opener(monkeypatch):
    # a proxy in the environment while the opener is built: urllib registers
    # a ProxyHandler only when a proxy is configured, so without one the
    # no-proxy assertion below also passes for an opener that reads its
    # proxies from the environment.
    for name in ("http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")
    opener = webclient.build_opener(ssl.create_default_context())
    assert opener is not webclient.OPENER
    handlers = [type(h).__name__ for h in opener.handlers]
    assert "HTTPSHandler" in handlers
    # an empty ProxyHandler evicts urllib's default one and installs no
    # *_open method of its own, so proxy support ends up absent entirely:
    # the same shape OPENER has, which is the point of passing it along.
    assert "ProxyHandler" not in handlers
    assert "ProxyHandler" not in [
        type(h).__name__ for h in webclient.OPENER.handlers
    ]


def test_openers_follow_no_redirect():
    # urllib would resend the Authorization header to a redirect's target,
    # so neither opener keeps the stock handler that follows one.
    for opener in (
        webclient.OPENER,
        webclient.build_opener(ssl.create_default_context()),
    ):
        handlers = [type(h).__name__ for h in opener.handlers]
        assert "NoRedirect" in handlers
        assert "HTTPRedirectHandler" not in handlers


# ---------------------------------------------------------------------------
# _post: one HTTP round-trip through the proxy-free opener
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self.headers = {}
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeOpener:
    """Records the request and returns/raises a scripted outcome."""

    def __init__(self, outcome):
        self._outcome = outcome
        self.request = None
        self.timeout = None

    def open(self, req, timeout=None):
        self.request = req
        self.timeout = timeout
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def test_post_success_builds_request(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b'{"ok": 1}'))
    monkeypatch.setattr(webclient, "OPENER", opener)
    status, body = mcpcli._post(
        "http://127.0.0.1:9/", b'{"a":1}', "sekret", "2025-11-25", 3.0
    )
    assert (status, body) == (200, b'{"ok": 1}')
    req = opener.request
    assert req.full_url == "http://127.0.0.1:9/mcp"
    assert req.get_method() == "POST"
    assert req.get_header("Authorization") == "Bearer sekret"
    assert req.get_header("Mcp-protocol-version") == "2025-11-25"
    assert opener.timeout == 3.0


def test_post_without_token_sends_no_auth_header(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(webclient, "OPENER", opener)
    mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    assert opener.request.get_header("Authorization") is None


def test_post_http_error_returns_status_and_body(monkeypatch):
    err = urllib.error.HTTPError(
        "http://x/mcp", 401, "unauthorized", {}, io.BytesIO(b'{"error":"no"}')
    )
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(err))
    status, body = mcpcli._post(
        "http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0
    )
    assert status == 401
    assert body == b'{"error":"no"}'


@pytest.mark.parametrize(
    "raised",
    [
        urllib.error.URLError(ConnectionRefusedError(61, "refused")),
        TimeoutError("timed out"),
        OSError("broken"),
    ],
)
def test_post_transport_failures_raise_bridge_error(monkeypatch, raised):
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(raised))
    with pytest.raises(webclient.ClientError, match="cannot reach"):
        mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)


def test_post_without_opener_uses_the_module_global(monkeypatch):
    # the five-argument call is the pre-TLS signature; it must still go
    # through webclient.OPENER, read at call time so the monkeypatch takes.
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(webclient, "OPENER", opener)
    mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    assert opener.request is not None


def test_post_uses_the_supplied_opener(monkeypatch):
    unused = _FakeOpener(_FakeResponse(500, b"nope"))
    monkeypatch.setattr(webclient, "OPENER", unused)
    chosen = _FakeOpener(_FakeResponse(200, b'{"ok": 1}'))
    status, _body = mcpcli._post(
        "http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0, opener=chosen
    )
    assert status == 200
    assert chosen.request is not None
    assert unused.request is None


def test_post_tls_failure_names_cacert_not_unreachable(monkeypatch):
    # a verification failure arrives as URLError(reason=SSLError); reporting it
    # as "cannot reach" would send the operator after a network problem.
    failure = urllib.error.URLError(
        ssl.SSLCertVerificationError(1, "certificate verify failed")
    )
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(failure))
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post("https://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    message = str(caught.value)
    assert "TLS verification failed" in message
    assert "--cacert" in message
    assert "cannot reach" not in message


@pytest.mark.parametrize(
    "raised",
    [
        # something other than an HTTP server answered
        http.client.BadStatusLine("SSH-2.0-OpenSSH_9.6\r\n"),
        http.client.IncompleteRead(b"{", 40),
    ],
)
def test_post_reply_that_is_not_http_raises_bridge_error(monkeypatch, raised):
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(raised))
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    message = str(caught.value)
    assert message.startswith("no HTTP reply from the cronstable MCP endpoint")
    # the peer's bytes arrive escaped
    assert repr(raised) in message
    assert "\r" not in message


def test_post_error_body_cut_short_raises_bridge_error(monkeypatch):
    class _CutShort(io.BytesIO):
        def read(self, *args):
            raise http.client.IncompleteRead(b"{", 40)

    err = urllib.error.HTTPError("http://x/mcp", 500, "error", {}, _CutShort())
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(err))
    with pytest.raises(webclient.ClientError, match="IncompleteRead"):
        mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)


def test_post_reports_a_redirect_instead_of_following_it(monkeypatch):
    # what the redirect-free opener raises for a 3xx
    err = urllib.error.HTTPError(
        "http://x/mcp",
        307,
        "redirect",
        {"Location": "https://elsewhere.test/mcp\x1b[2J"},
        io.BytesIO(b""),
    )
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(err))
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post("http://127.0.0.1:9", b"{}", "sekret", "2025-11-25", 1.0)
    message = str(caught.value)
    assert "redirects to 'https://elsewhere.test/mcp\\x1b[2J'" in message
    assert "\x1b" not in message


def test_post_redirect_to_the_endpoint_elsewhere_names_the_base(monkeypatch):
    # --url takes the base, and the bridge appends /mcp to it
    err = urllib.error.HTTPError(
        "http://x/mcp",
        301,
        "moved",
        {"Location": "https://nas.example.test/mcp"},
        io.BytesIO(b""),
    )
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(err))
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post("http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0)
    message = str(caught.value)
    assert "redirects to 'https://nas.example.test/mcp'" in message
    assert "(pass --url 'https://nas.example.test' instead)" in message


@pytest.mark.parametrize("path", ["/x y", "/café"])
def test_post_to_an_address_that_is_not_a_url_raises_bridge_error(path):
    # the unpatched opener: http.client refuses the address before it
    # opens a connection
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post(
            "http://127.0.0.1:9" + path, b"{}", None, "2025-11-25", 1.0
        )
    message = str(caught.value)
    assert message.startswith(
        "cannot send a request to the cronstable MCP endpoint at "
    )
    assert "the address is not a valid URL" in message
    assert "no HTTP reply" not in message


@pytest.mark.parametrize("url", ["localhost", "http://[::1"])
def test_post_to_an_address_that_no_request_can_name_raises_bridge_error(
    monkeypatch, url
):
    # no scheme, or a host cut short: the request is never built
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(webclient, "OPENER", opener)
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post(url, b"{}", "sekret", "2025-11-25", 1.0)
    message = str(caught.value)
    assert message.startswith(
        "cannot send a request to the cronstable MCP endpoint at "
        "{}/mcp: the address is not a valid URL".format(url)
    )
    assert opener.request is None


def test_post_refuses_a_header_that_http_cannot_carry(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(webclient, "OPENER", opener)
    with pytest.raises(webclient.ClientError) as caught:
        mcpcli._post(
            "http://127.0.0.1:9", b"{}", None, "2025-06-18\r\nX-Extra: 1", 1.0
        )
    assert "the MCP-Protocol-Version header holds a character" in str(
        caught.value
    )
    assert opener.request is None


def test_post_returns_a_3xx_that_names_no_target(monkeypatch):
    err = urllib.error.HTTPError(
        "http://x/mcp", 304, "not modified", {}, io.BytesIO(b"")
    )
    monkeypatch.setattr(webclient, "OPENER", _FakeOpener(err))
    status, _body = mcpcli._post(
        "http://127.0.0.1:9", b"{}", None, "2025-11-25", 1.0
    )
    assert status == 304


# ---------------------------------------------------------------------------
# reply sniffing / error message helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body,expected",
    [
        (b'{"result": {"protocolVersion": "2025-06-18"}}', "2025-06-18"),
        (b'{"result": {"protocolVersion": 7}}', None),
        (b'{"result": []}', None),
        (b"[1]", None),
        (b"not json", None),
    ],
)
def test_sniff_protocol_version(body, expected):
    assert mcpcli._sniff_protocol_version(body) == expected


def test_http_error_message_includes_body_error():
    msg = mcpcli._http_error_message(503, b'{"error": "leader only"}')
    assert msg == "MCP endpoint returned HTTP 503: leader only"


def test_http_error_message_plain_on_unparseable_body():
    assert (
        mcpcli._http_error_message(500, b"<html>")
        == "MCP endpoint returned HTTP 500"
    )


def test_http_error_message_ignores_json_without_error():
    assert (
        mcpcli._http_error_message(500, b'{"ok": true}')
        == "MCP endpoint returned HTTP 500"
    )


# ---------------------------------------------------------------------------
# the stdin -> _post -> stdout proxy loop
# ---------------------------------------------------------------------------


class _PostRecorder:
    """Scripted stand-in for ``mcpcli._post``: pops one outcome per call."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.calls = []

    def __call__(
        self,
        url,
        frame,
        token,
        protocol_version,
        timeout,
        opener=None,
        headers=None,
    ):
        self.calls.append(
            {
                "url": url,
                "frame": frame,
                "token": token,
                "pv": protocol_version,
                "timeout": timeout,
                "opener": opener,
                "headers": headers,
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _run(monkeypatch, stdin_text, outcomes, **arg_overrides):
    recorder = _PostRecorder(outcomes)
    monkeypatch.setattr(mcpcli, "_post", recorder)
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin_text))
    code = mcpcli._run_bridge(_args(**arg_overrides))
    return code, recorder


def _frames(captured_out):
    return [json.loads(line) for line in captured_out.splitlines() if line]


def test_bridge_replies_to_a_request(monkeypatch, capsys):
    reply = b'{"jsonrpc": "2.0", "id": 1, "result": {}}\n'
    code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 1, "method": "ping"}\n',
        [(200, reply)],
    )
    assert code == 0
    out = capsys.readouterr().out
    assert _frames(out) == [{"jsonrpc": "2.0", "id": 1, "result": {}}]
    assert recorder.calls[0]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION


def test_bridge_skips_blank_lines_and_parse_errors(monkeypatch, capsys):
    code, recorder = _run(monkeypatch, "\n   \nnot json\n", [])
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames == [
        {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "parse error"},
        }
    ]
    assert recorder.calls == []


def test_bridge_notification_gets_no_reply(monkeypatch, capsys):
    code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "method": "notifications/initialized"}\n',
        [(202, b"")],
    )
    assert code == 0
    assert capsys.readouterr().out == ""
    assert len(recorder.calls) == 1


def test_bridge_forwards_non_object_frame_without_reply(monkeypatch, capsys):
    # a malformed-but-valid-JSON frame (an array) is proxied verbatim but can
    # carry no id, so nothing is written even for an error response.
    code, recorder = _run(monkeypatch, "[1, 2]\n", [(400, b'{"error":"x"}')])
    assert code == 0
    assert capsys.readouterr().out == ""
    assert recorder.calls[0]["frame"] == b"[1, 2]"


def test_bridge_sniffs_negotiated_version_from_initialize(monkeypatch, capsys):
    init_reply = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"protocolVersion": "2025-06-18"},
        }
    ).encode()
    ping_reply = b'{"jsonrpc": "2.0", "id": 2, "result": {}}'
    stdin_text = (
        '{"jsonrpc": "2.0", "id": 1, "method": "initialize"}\n'
        '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
    )
    code, recorder = _run(
        monkeypatch, stdin_text, [(200, init_reply), (200, ping_reply)]
    )
    assert code == 0
    assert recorder.calls[0]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION
    assert recorder.calls[1]["pv"] == "2025-06-18"
    assert len(_frames(capsys.readouterr().out)) == 2


def test_bridge_keeps_pinned_version_when_sniff_fails(monkeypatch, capsys):
    stdin_text = (
        '{"jsonrpc": "2.0", "id": 1, "method": "initialize"}\n'
        '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
    )
    code, recorder = _run(
        monkeypatch,
        stdin_text,
        [(200, b'{"result": {}}'), (200, b'{"id": 2}')],
        protocol_version="2025-03-26",
    )
    assert code == 0
    assert [c["pv"] for c in recorder.calls] == [
        "2025-03-26",
        "2025-03-26",
    ]


def test_bridge_transport_error_on_request_emits_error_frame(
    monkeypatch, capsys
):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 5, "method": "ping"}\n',
        [webclient.ClientError("cannot reach the cronstable MCP endpoint")],
    )
    assert code == 0
    captured = capsys.readouterr()
    frames = _frames(captured.out)
    assert frames[0]["id"] == 5
    assert frames[0]["error"]["code"] == mcpcli._TRANSPORT_ERROR
    assert "cannot reach" in frames[0]["error"]["message"]


def test_bridge_transport_error_on_notification_goes_to_stderr(
    monkeypatch, capsys
):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "method": "notifications/initialized"}\n',
        [webclient.ClientError("daemon is down")],
    )
    assert code == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "daemon is down" in captured.err


def test_bridge_http_error_becomes_error_frame(monkeypatch, capsys):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 9, "method": "ping"}\n',
        [(401, b'{"error": "authentication required"}')],
    )
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames[0]["error"]["code"] == mcpcli._TRANSPORT_ERROR
    assert "HTTP 401" in frames[0]["error"]["message"]
    assert "authentication required" in frames[0]["error"]["message"]


def test_bridge_threads_the_default_opener_into_post(monkeypatch, capsys):
    # with no TLS flags the bridge still resolves an opener; it must be the
    # shared global, so the plaintext path is byte-for-byte what it was.
    _code, recorder = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 1, "method": "ping"}\n',
        [(200, b'{"id": 1}')],
    )
    assert recorder.calls[0]["opener"] is webclient.OPENER


def test_bridge_reports_bad_tls_material_and_exits_nonzero(
    monkeypatch, capsys, tmp_path
):
    # a bad path is fatal before the read loop, not an error frame per request.
    recorder = _PostRecorder([])
    monkeypatch.setattr(mcpcli, "_post", recorder)
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"id": 1}\n'))
    code = mcpcli._run_bridge(_args(cacert=str(tmp_path / "absent.pem")))
    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""  # stdout carries JSON-RPC frames only
    assert "TLS material" in captured.err
    assert recorder.calls == []


def test_bridge_reports_a_token_no_header_can_carry_and_exits_nonzero(
    monkeypatch, capsys
):
    # a variable read from a file with a CRLF line ending, for example
    recorder = _PostRecorder([])
    monkeypatch.setattr(mcpcli, "_post", recorder)
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"id": 1}\n'))
    monkeypatch.setenv("BRIDGE_TOKEN", "s3cr3t-value\r")
    assert mcpcli._run_bridge(_args(token_env="BRIDGE_TOKEN")) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        "the access token from the BRIDGE_TOKEN environment variable"
        in captured.err
    )
    assert "s3cr3t" not in captured.err
    assert recorder.calls == []


def test_bridge_answers_a_url_without_a_scheme_with_a_transport_error(
    monkeypatch, capsys
):
    # --url localhost: every frame gets the error, and the bridge stays up
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(
            '{"jsonrpc": "2.0", "id": 7, "method": "ping"}\n'
            '{"jsonrpc": "2.0", "method": "notifications/initialized"}\n'
        ),
    )
    assert mcpcli._run_bridge(_args(url="localhost")) == 0
    captured = capsys.readouterr()
    [frame] = _frames(captured.out)
    assert frame["id"] == 7
    assert frame["error"]["code"] == -31000
    assert "the address is not a valid URL" in frame["error"]["message"]
    # the notification has no reply frame, so its failure goes to stderr
    assert "the address is not a valid URL" in captured.err


def test_bridge_answers_a_timeout_no_socket_takes_with_a_transport_error(
    monkeypatch, capsys
):
    # a timeout that skipped the argument parser: the unpatched opener's
    # socket raises ValueError for it, so the transport refuses it first
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO('{"jsonrpc": "2.0", "id": 7, "method": "ping"}\n'),
    )
    assert mcpcli._run_bridge(_args(timeout=float("nan"))) == 0
    [frame] = _frames(capsys.readouterr().out)
    assert frame["id"] == 7
    assert frame["error"]["code"] == -31000
    assert frame["error"]["message"].endswith(
        "the timeout of nan seconds is not greater than 0"
    )


def test_bridge_refuses_a_negotiated_version_no_header_can_carry(
    monkeypatch, capsys
):
    # the daemon's initialize reply names the version later requests carry
    init_reply = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"protocolVersion": "2025-06-18\r\nX-Injected: 1"},
        }
    ).encode()
    opener = _FakeOpener(_FakeResponse(200, init_reply))
    monkeypatch.setattr(webclient, "OPENER", opener)
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(
            '{"jsonrpc": "2.0", "id": 1, "method": "initialize"}\n'
            '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
        ),
    )
    assert mcpcli._run_bridge(_args()) == 0
    first, second = _frames(capsys.readouterr().out)
    assert first["id"] == 1 and "result" in first
    assert second["id"] == 2
    assert second["error"]["code"] == -31000
    assert "MCP-Protocol-Version header" in second["error"]["message"]
    # only the initialize request went out
    assert opener.request.data == (
        b'{"jsonrpc": "2.0", "id": 1, "method": "initialize"}'
    )


def test_bridge_empty_200_body_becomes_error_frame(monkeypatch, capsys):
    code, _ = _run(
        monkeypatch,
        '{"jsonrpc": "2.0", "id": 3, "method": "ping"}\n',
        [(200, b"")],
    )
    assert code == 0
    frames = _frames(capsys.readouterr().out)
    assert frames[0]["error"]["code"] == mcpcli._TRANSPORT_ERROR
    assert "HTTP 200" in frames[0]["error"]["message"]


# ---------------------------------------------------------------------------
# UTF-8 stdio discipline: MCP mandates UTF-8 JSON-RPC, but piped stdio on
# Windows defaults to the ANSI codepage (cp1252), which cannot carry an emoji
# or box-drawing char in a tool result.
# ---------------------------------------------------------------------------


def _cp1252_stdio(inbound: bytes):
    """A (stdin, stdout, raw stdout) trio dressed in cp1252, like a Windows
    pipe: text wrappers whose declared encoding is NOT UTF-8."""
    stdin_raw = io.BytesIO(inbound)
    stdout_raw = io.BytesIO()
    fake_stdin = io.TextIOWrapper(stdin_raw, encoding="cp1252")
    fake_stdout = io.TextIOWrapper(
        stdout_raw, encoding="cp1252", write_through=True
    )
    return fake_stdin, fake_stdout, stdout_raw


def test_bridge_stdio_helpers_round_trip_non_ascii(monkeypatch):
    # the helpers must yield UTF-8 streams no matter what encoding the host
    # dressed the stdio pair in; a frame with an accent, a check mark and an
    # emoji must come back byte-identical, terminated by a bare newline.
    frame = '{"jsonrpc": "2.0", "id": 1, "result": {"text": "état ✓ 🚀"}}'
    fake_stdin, fake_stdout, stdout_raw = _cp1252_stdio(
        (frame + "\n").encode("utf-8")
    )
    monkeypatch.setattr(sys, "stdin", fake_stdin)
    monkeypatch.setattr(sys, "stdout", fake_stdout)
    reader, writer = mcpcli._bridge_stdio()
    try:
        line = reader.readline().strip()
        assert json.loads(line)["result"]["text"] == "état ✓ 🚀"
        mcpcli._write_reply(writer, line.encode("utf-8"))
    finally:
        webclient.release_stream(reader, fake_stdin)
        webclient.release_stream(writer, fake_stdout)
    assert stdout_raw.getvalue() == (frame + "\n").encode("utf-8")


def test_bridge_is_utf8_end_to_end_despite_cp1252_stdio(monkeypatch):
    # the whole loop under cp1252 stdio: the inbound frame must reach _post
    # byte-identical (not mojibake), and the daemon's raw UTF-8 reply must go
    # out undamaged instead of raising UnicodeEncodeError mid-session.
    request = (
        '{"jsonrpc": "2.0", "id": 1, "method": "tools/call",'
        ' "params": {"note": "café"}}'
    )
    reply = '{"jsonrpc": "2.0", "id": 1, "result": {"text": "box ┌─┐ 🎉"}}'
    fake_stdin, fake_stdout, stdout_raw = _cp1252_stdio(
        (request + "\n").encode("utf-8")
    )
    monkeypatch.setattr(sys, "stdin", fake_stdin)
    monkeypatch.setattr(sys, "stdout", fake_stdout)
    recorder = _PostRecorder([(200, reply.encode("utf-8"))])
    monkeypatch.setattr(mcpcli, "_post", recorder)
    code = mcpcli._run_bridge(_args())
    assert code == 0
    assert recorder.calls[0]["frame"] == request.encode("utf-8")
    assert stdout_raw.getvalue() == (reply + "\n").encode("utf-8")


def test_bridge_stdio_falls_back_to_bufferless_streams(monkeypatch):
    # the seam the rest of this file uses: a StringIO stdin has no binary
    # buffer to wrap, so the helpers must hand it back usable as-is rather
    # than fail, keeping every capsys-based bridge test working.
    fake_stdin = io.StringIO('{"id": 1}\n')
    fake_stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", fake_stdin)
    monkeypatch.setattr(sys, "stdout", fake_stdout)
    reader, writer = mcpcli._bridge_stdio()
    try:
        assert reader is fake_stdin
        assert writer is fake_stdout
        assert reader.readline() == '{"id": 1}\n'
        writer.write("ok\n")
        assert fake_stdout.getvalue() == "ok\n"
    finally:
        # releasing a passed-through stream must not close or detach it
        webclient.release_stream(reader, fake_stdin)
        webclient.release_stream(writer, fake_stdout)
    assert not fake_stdin.closed
    assert not fake_stdout.closed


# ---------------------------------------------------------------------------
# --check: the initialize + tools/list handshake self-test
# ---------------------------------------------------------------------------


def _check(monkeypatch, outcomes, **arg_overrides):
    recorder = _PostRecorder(outcomes)
    monkeypatch.setattr(mcpcli, "_post", recorder)
    code = mcpcli._check(_args(**arg_overrides))
    return code, recorder


# how a legacy-only daemon answers the modern probe's version header
_LEGACY_PROBE = (400, b'{"error": "unsupported MCP-Protocol-Version"}')


def test_check_probes_discover_and_stays_modern(monkeypatch, capsys):
    discover = json.dumps(
        {"result": {"supportedVersions": ["2026-07-28", "2025-11-25"]}}
    ).encode()
    tools = json.dumps({"result": {"tools": [{"name": "a"}]}}).encode()
    code, recorder = _check(monkeypatch, [(200, discover), (200, tools)])
    assert code == 0
    err = capsys.readouterr().err
    assert (
        "ok - protocol 2026-07-28 (modern; the daemon serves 2026-07-28, "
        "2025-11-25), 1 tool(s)"
    ) in err
    probe, listing = (json.loads(c["frame"]) for c in recorder.calls)
    assert probe["method"] == "server/discover"
    assert listing["method"] == "tools/list"
    # both requests carry the modern _meta and its mirrored headers
    for call, frame in zip(recorder.calls, (probe, listing)):
        assert frame["params"]["_meta"][mcpcli._META_PROTOCOL_VERSION] == (
            "2026-07-28"
        )
        assert call["headers"]["MCP-Protocol-Version"] == "2026-07-28"
        assert call["headers"]["Mcp-Method"] == frame["method"]


def test_check_falls_back_to_initialize(monkeypatch, capsys):
    init_reply = json.dumps(
        {"result": {"protocolVersion": "2025-06-18"}}
    ).encode()
    tools_reply = json.dumps(
        {"result": {"tools": [{"name": "cron_get_status"}]}}
    ).encode()
    code, recorder = _check(
        monkeypatch, [_LEGACY_PROBE, (200, init_reply), (200, tools_reply)]
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "ok - protocol 2025-06-18 (legacy), 1 tool(s)" in err
    # initialize carries the pre-initialize default, tools/list the
    # negotiated version, and neither has modern headers
    assert recorder.calls[1]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION
    assert recorder.calls[2]["pv"] == "2025-06-18"
    assert recorder.calls[1]["headers"] is recorder.calls[2]["headers"] is None
    assert json.loads(recorder.calls[2]["frame"])["method"] == "tools/list"


def test_check_falls_back_when_discover_omits_our_version(
    monkeypatch, capsys
):
    discover = json.dumps({"result": {"supportedVersions": ["2099-01-01"]}})
    code, recorder = _check(
        monkeypatch,
        [
            (200, discover.encode()),
            (200, b'{"result": {"protocolVersion": "2025-11-25"}}'),
            (200, b'{"result": {"tools": []}}'),
        ],
    )
    assert code == 0
    assert "(legacy)" in capsys.readouterr().err
    assert json.loads(recorder.calls[1]["frame"])["method"] == "initialize"


def test_check_threads_the_default_opener_into_post(monkeypatch, capsys):
    code, recorder = _check(
        monkeypatch,
        [_LEGACY_PROBE, (200, b'{"result": {}}'), (200, b'{"result": {}}')],
    )
    assert code == 0
    assert [c["opener"] for c in recorder.calls] == [webclient.OPENER] * 3


def test_check_bad_tls_material_fails_before_any_request(
    monkeypatch, capsys, tmp_path
):
    code, recorder = _check(monkeypatch, [], cacert=str(tmp_path / "no.pem"))
    assert code == 1
    assert "mcp check: " in capsys.readouterr().err
    assert recorder.calls == []


def test_check_token_no_header_can_carry_fails_before_any_request(
    monkeypatch, capsys
):
    code, recorder = _check(monkeypatch, [], token="s3cr3t\nvalue")
    assert code == 1
    err = capsys.readouterr().err
    assert err.startswith("mcp check: the access token from --token holds")
    assert "s3cr3t" not in err
    assert recorder.calls == []


def test_check_unreachable_daemon(monkeypatch, capsys):
    refused = webclient.ClientError("connection refused")
    code, _ = _check(monkeypatch, [refused])
    assert code == 1
    assert "connection refused" in capsys.readouterr().err


def test_check_initialize_http_failure(monkeypatch, capsys):
    code, _ = _check(
        monkeypatch, [(401, b'{"error": "auth"}'), (401, b'{"error": "auth"}')]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "initialize failed" in err
    assert "HTTP 401" in err


def test_check_tools_list_failure_still_ok(monkeypatch, capsys):
    # a broken tools/list downgrades the report to 0 tools, not a failure.
    code, _ = _check(
        monkeypatch,
        [
            _LEGACY_PROBE,
            (200, b'{"result": {"protocolVersion": "2025-11-25"}}'),
            webclient.ClientError("flaky"),
        ],
    )
    assert code == 0
    assert "0 tool(s)" in capsys.readouterr().err


def test_check_tools_list_bad_json_still_ok(monkeypatch, capsys):
    code, _ = _check(
        monkeypatch,
        [_LEGACY_PROBE, (200, b'{"result": {}}'), (200, b"not json")],
    )
    assert code == 0
    err = capsys.readouterr().err
    # sniff fell back to the pre-initialize default version.
    assert mcpcli.DEFAULT_PROTOCOL_VERSION in err
    assert "0 tool(s)" in err


# ---------------------------------------------------------------------------
# era-aware forwarding: modern headers, forwarded errors, dropped replies
# ---------------------------------------------------------------------------

_PV_KEY = mcpcli._META_PROTOCOL_VERSION


def _modern_frame(method, mid=1, **params):
    params["_meta"] = {
        _PV_KEY: "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    return {"jsonrpc": "2.0", "id": mid, "method": method, "params": params}


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        pytest.param(
            _modern_frame("tools/list"),
            {"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/list"},
            id="no-name",
        ),
        pytest.param(
            _modern_frame("tools/call", name="cron_get_status"),
            {
                "MCP-Protocol-Version": "2026-07-28",
                "Mcp-Method": "tools/call",
                "Mcp-Name": "cron_get_status",
            },
            id="tool-name",
        ),
        pytest.param(
            _modern_frame("resources/read", uri="cronstable://status"),
            {
                "MCP-Protocol-Version": "2026-07-28",
                "Mcp-Method": "resources/read",
                "Mcp-Name": "cronstable://status",
            },
            id="resource-uri",
        ),
        pytest.param(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            None,
            id="legacy",
        ),
        pytest.param(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {"_meta": {"progressToken": 1}},
            },
            None,
            id="legacy-with-other-meta",
        ),
    ],
)
def test_modern_headers_mirror_the_body(frame, expected):
    assert mcpcli._modern_headers(frame) == expected


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        pytest.param("us-west1", "us-west1", id="plain"),
        pytest.param(
            "Hello, 世界", "=?base64?SGVsbG8sIOS4lueVjA==?=", id="non-ascii"
        ),
        pytest.param(" padded ", "=?base64?IHBhZGRlZCA=?=", id="whitespace"),
        pytest.param(
            "line1\nline2", "=?base64?bGluZTEKbGluZTI=?=", id="control"
        ),
        pytest.param(
            "=?base64?literal?=",
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=",
            id="sentinel-lookalike",
        ),
    ],
)
def test_header_values_use_the_base64_sentinel_when_needed(value, encoded):
    # the spec's own encoding examples
    assert mcpcli._encode_header_value(value) == encoded


def test_modern_headers_leave_out_what_cannot_be_a_header():
    frame = _modern_frame("tools/list\n")
    frame["params"]["_meta"][_PV_KEY] = 20260728
    # the daemon then names the missing header in its HeaderMismatch error
    assert mcpcli._modern_headers(frame) == {}


def test_post_modern_headers_replace_the_pinned_version(monkeypatch):
    opener = _FakeOpener(_FakeResponse(200, b"{}"))
    monkeypatch.setattr(webclient, "OPENER", opener)
    mcpcli._post(
        "http://127.0.0.1:9",
        b"{}",
        None,
        "2025-11-25",
        1.0,
        headers={"Mcp-Method": "tools/list"},
    )
    req = opener.request
    assert req.get_header("Mcp-method") == "tools/list"
    assert req.get_header("Mcp-protocol-version") is None


def test_bridge_sends_modern_frames_with_their_headers(monkeypatch, capsys):
    frame = _modern_frame("tools/call", name="cron_get_status")
    reply = b'{"jsonrpc": "2.0", "id": 1, "result": {}}'
    _code, recorder = _run(monkeypatch, json.dumps(frame) + "\n", [(200, reply)])
    assert recorder.calls[0]["headers"] == {
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": "tools/call",
        "Mcp-Name": "cron_get_status",
    }
    assert len(_frames(capsys.readouterr().out)) == 1


def test_bridge_forwards_the_daemons_jsonrpc_errors(monkeypatch, capsys):
    # a stdio client needs the real -32022 to pick a version, and a body
    # without an id gets the frame's
    unsupported = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "error": {
                "code": -32022,
                "message": "Unsupported protocol version",
                "data": {"supported": ["2026-07-28"], "requested": "x"},
            },
        }
    ).encode()
    missing = b'{"jsonrpc": "2.0", "error": {"code": -32601, "message": "m"}}'
    stdin_text = (
        json.dumps(_modern_frame("tools/list", mid=4))
        + "\n"
        + json.dumps(_modern_frame("nope", mid=5))
        + "\n"
    )
    _run(monkeypatch, stdin_text, [(400, unsupported), (404, missing)])
    first, second = _frames(capsys.readouterr().out)
    assert first == json.loads(unsupported)
    assert second["id"] == 5
    assert second["error"]["code"] == -32601


def test_bridge_legacy_daemon_error_stays_a_bridge_error(monkeypatch, capsys):
    # an older, legacy-only daemon answers the modern version header with a
    # plain 400; the bridge reports that as a non-modern error, so a
    # dual-era client falls back to initialize.
    _run(
        monkeypatch,
        json.dumps(_modern_frame("server/discover")) + "\n",
        [_LEGACY_PROBE],
    )
    (frame,) = _frames(capsys.readouterr().out)
    assert frame["error"]["code"] == mcpcli._TRANSPORT_ERROR
    assert not -32768 <= mcpcli._TRANSPORT_ERROR <= -32000
    assert "unsupported MCP-Protocol-Version" in frame["error"]["message"]


def test_bridge_drops_responses_from_the_client(monkeypatch, capsys):
    # the daemon sends no requests, so a reply answers nothing: it is
    # neither forwarded nor answered
    stdin_text = (
        '{"jsonrpc": "2.0", "id": 1, "result": {}}\n'
        '{"jsonrpc": "2.0", "id": 2, "error": {"code": 1, "message": "x"}}\n'
    )
    code, recorder = _run(monkeypatch, stdin_text, [])
    assert code == 0
    assert recorder.calls == []
    assert capsys.readouterr().out == ""


def test_bridge_does_not_sniff_a_modern_initialize(monkeypatch, capsys):
    # the negotiated version is a legacy concept; a modern frame's reply
    # must not repin the legacy header
    frame = _modern_frame("initialize")
    reply = b'{"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "x"}}'
    ping = '{"jsonrpc": "2.0", "id": 2, "method": "ping"}\n'
    _code, recorder = _run(
        monkeypatch,
        json.dumps(frame) + "\n" + ping,
        [(200, reply), (200, b'{"id": 2}')],
    )
    assert recorder.calls[1]["pv"] == mcpcli.DEFAULT_PROTOCOL_VERSION


# ---------------------------------------------------------------------------
# dispatch through the real subcommand parser
# ---------------------------------------------------------------------------


def _parse_cli(argv):
    parser = argparse.ArgumentParser(prog="cronstable")
    sub = parser.add_subparsers(dest="subcommand")
    mcpcli.add_mcp_command(sub)
    return parser.parse_args(argv)


def test_dispatch_routes_check(monkeypatch):
    args = _parse_cli(["mcp", "--check", "--url", "http://127.0.0.1:1"])
    monkeypatch.setattr(mcpcli, "_check", lambda a: 42)
    assert mcpcli.dispatch(args) == 42


def test_dispatch_routes_bridge_by_default(monkeypatch):
    args = _parse_cli(["mcp", "--token", "sekret"])
    seen = {}

    def fake_bridge(a):
        seen["token"] = a.token
        return 0

    monkeypatch.setattr(mcpcli, "_run_bridge", fake_bridge)
    assert mcpcli.dispatch(args) == 0
    assert seen == {"token": "sekret"}


def test_parser_defaults(monkeypatch):
    args = _parse_cli(["mcp"])
    assert args.url == mcpcli.DEFAULT_URL
    assert args.timeout == mcpcli.DEFAULT_TIMEOUT
    assert args.mcp_check is False
    assert args.protocol_version is None
    # the TLS flags default to "untouched transport", matching _resolve_tls
    # returning None for them.
    assert args.cacert is None
    assert args.client_cert is None
    assert args.client_key is None
    assert args.insecure is False


@pytest.mark.parametrize(
    "value", ["0", "-0.0", "-1", "nan", "inf", "-inf", "1e999", "soon", ""]
)
def test_parser_refuses_a_timeout_that_no_request_can_wait(capsys, value):
    # a socket refuses a negative deadline and one that is not a number, and
    # a deadline of 0 leaves it non-blocking, so the parser refuses the flag
    # before the bridge starts.  The `=` form hands argparse a leading minus
    # as the value on every Python.
    with pytest.raises(SystemExit) as caught:
        _parse_cli(["mcp", "--timeout=" + value])
    assert caught.value.code == 2  # the usage exit code
    assert (
        "argument --timeout: SECONDS must be a finite number greater than "
        "0, not {!r}".format(value)
    ) in capsys.readouterr().err


@pytest.mark.parametrize(
    "value, seconds", [("0.5", 0.5), ("90", 90.0), ("1e300", 1e300)]
)
def test_parser_takes_a_finite_timeout_above_0(value, seconds):
    assert _parse_cli(["mcp", "--timeout", value]).timeout == seconds


def test_parser_accepts_the_tls_flags():
    args = _parse_cli(
        [
            "mcp",
            "--cacert",
            "ca.pem",
            "--client-cert",
            "c.pem",
            "--client-key",
            "k.pem",
            "--insecure",
        ]
    )
    assert (args.cacert, args.client_cert, args.client_key) == (
        "ca.pem",
        "c.pem",
        "k.pem",
    )
    assert args.insecure is True
