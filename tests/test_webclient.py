"""Tests for the terminal clients' shared transport
(:mod:`cronstable.webclient`).

``cronstable mcp`` and ``cronstable pair`` reach the daemon through
:func:`cronstable.webclient.send`. ``tests/test_mcpcli.py`` and
``tests/test_paircli.py`` cover it through each command; the tests here
cover what the commands share: the opener, the failure messages, and the
detail that a caller reads when it gives its own advice.
"""

import argparse
import http.client
import http.server
import io
import ssl
import threading
import urllib.error
from email.message import Message

import pytest

from cronstable import _cliargs, webclient

BASE = "http://cron.example.test:8080"
WHAT = "the cronstable server at " + BASE


class _Opener:
    """Raises or returns one scripted outcome."""

    def __init__(self, outcome):
        self._outcome = outcome

    def open(self, req, timeout=None):
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def _redirect(location, status=302):
    headers = Message()
    headers["Location"] = location
    return urllib.error.HTTPError(
        BASE + "/whoami", status, "redirect", headers, io.BytesIO(b"")
    )


def _send(outcome, path="/whoami", base=BASE):
    return webclient.send(base + path, _Opener(outcome), 1.0, WHAT, path)


# ---------------------------------------------------------------------------
# the opener
# ---------------------------------------------------------------------------


def test_build_opener_without_context_is_the_shared_one(monkeypatch):
    assert webclient.build_opener(None) is webclient.OPENER
    # read at call time, so a test's replacement takes
    stand_in = object()
    monkeypatch.setattr(webclient, "OPENER", stand_in)
    assert webclient.build_opener(None) is stand_in


def test_openers_use_no_proxy_and_follow_no_redirect():
    for opener in (
        webclient.OPENER,
        webclient.build_opener(ssl.create_default_context()),
    ):
        handlers = [type(h).__name__ for h in opener.handlers]
        assert "NoRedirect" in handlers
        assert "HTTPRedirectHandler" not in handlers
        assert "ProxyHandler" not in handlers


def test_verifies_reads_the_context():
    assert webclient.verifies(None)
    assert webclient.verifies(ssl.create_default_context())
    insecure = ssl.create_default_context()
    insecure.check_hostname = False
    insecure.verify_mode = ssl.CERT_NONE
    assert not webclient.verifies(insecure)


# ---------------------------------------------------------------------------
# failures and their advice
# ---------------------------------------------------------------------------


def test_client_error_keeps_the_detail_apart_from_the_advice():
    plain = webclient.ClientError("cannot reach the server")
    assert str(plain) == plain.detail == "cannot reach the server"
    advised = webclient.ClientError("it failed", "pass --flag")
    assert str(advised) == "it failed (pass --flag)"
    assert advised.detail == "it failed"


def test_tls_failure_advises_the_flags_and_keeps_a_detail_without_them():
    failure = urllib.error.URLError(
        ssl.SSLCertVerificationError("IP address mismatch")
    )
    with pytest.raises(webclient.TLSError) as caught:
        _send(failure)
    message = str(caught.value)
    assert "--cacert" in message and "--insecure" in message
    assert caught.value.detail == (
        "TLS verification failed for {}: {}".format(WHAT, failure.reason)
    )


def test_unreachable_server_is_not_a_tls_error():
    refused = urllib.error.URLError(ConnectionRefusedError("refused"))
    with pytest.raises(webclient.ClientError) as caught:
        _send(refused)
    assert not isinstance(caught.value, webclient.TLSError)
    assert str(caught.value) == "cannot reach {}: refused".format(WHAT)


@pytest.mark.parametrize(
    "location, target, base",
    [
        # the same endpoint on another scheme, host, or port
        (
            "https://cron.example.test/whoami",
            None,
            "https://cron.example.test",
        ),
        (
            "https://nas.example.test:8443/ops/whoami",
            None,
            "https://nas.example.test:8443/ops",
        ),
        # a relative reference to the endpoint under another prefix
        ("/ops/whoami", BASE + "/ops/whoami", BASE + "/ops"),
    ],
)
def test_redirect_to_the_endpoint_elsewhere_names_the_base(
    location, target, base
):
    with pytest.raises(webclient.ClientError) as caught:
        _send(_redirect(location))
    detail = "{} redirects to {!r}, which this client does not follow".format(
        WHAT, target or location
    )
    assert caught.value.detail == detail
    assert str(caught.value) == "{} (pass --url {!r} instead)".format(
        detail, base
    )


@pytest.mark.parametrize(
    "location, target",
    [
        ("/oauth2/sign_in", BASE + "/oauth2/sign_in"),
        ("https://login.example.test/?rd=/whoami", None),
        # the same base: passing it again changes nothing
        ("/whoami", BASE + "/whoami"),
        ("/whoami/", BASE + "/whoami/"),
        ("?next=1", BASE + "/whoami?next=1"),
        # the endpoint on a scheme that --url does not take
        ("ftp://nas.example.test/whoami", None),
        # a target that is not a URL is quoted as the server sent it
        ("http://[bad/whoami", None),
    ],
)
def test_redirect_elsewhere_names_no_base(location, target):
    with pytest.raises(webclient.ClientError) as caught:
        _send(_redirect(location))
    message = str(caught.value)
    assert "redirects to {!r}".format(target or location) in message
    assert "pass --url" not in message
    assert message.endswith(
        "(point --url at an address that answers without a redirect)"
    )


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_malformed_redirect_target_from_a_real_server_is_reported(status):
    # the unpatched opener: urllib's own handler parses the target, and
    # raises ValueError for this one
    location = "http://[bad/whoami"

    class _Redirect(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(status)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), _Redirect)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = "http://127.0.0.1:{}/whoami".format(server.server_port)
        with pytest.raises(webclient.ClientError) as caught:
            webclient.send(url, webclient.OPENER, 5.0, WHAT, "/whoami")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert "redirects to {!r}".format(location) in str(caught.value)


def test_redirect_target_arrives_escaped():
    with pytest.raises(webclient.ClientError) as caught:
        _send(_redirect("https://nas.example.test/whoami\x1b[2J"))
    assert "\x1b" not in str(caught.value)
    assert "\\x1b[2J" in str(caught.value)


def test_redirect_without_a_path_names_no_base():
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(
            BASE + "/whoami",
            _Opener(_redirect("https://cron.example.test/whoami")),
            1.0,
            WHAT,
        )
    assert "pass --url" not in str(caught.value)


@pytest.mark.parametrize(
    "path, reason",
    [
        ("/x y/whoami", "URL can't contain control characters"),
        ("/café/whoami", "'ascii' codec can't encode"),
    ],
)
def test_address_that_is_not_a_url_is_refused_before_any_request(path, reason):
    # the unpatched opener: http.client raises before it connects
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(BASE + path, webclient.OPENER, 1.0, WHAT)
    message = str(caught.value)
    assert message.startswith(
        "cannot send a request to {}: the address is not a valid URL: ".format(
            WHAT
        )
    )
    assert reason in message
    assert "no HTTP reply" not in message


class _Unreached:
    """An opener that no request may reach."""

    def open(self, req, timeout=None):
        pytest.fail("the request was sent")


@pytest.mark.parametrize(
    "url, reason",
    [
        # no scheme, as in --url localhost
        ("localhost/mcp", "unknown url type: 'localhost/mcp'"),
        ("8080", "unknown url type: '8080'"),
        ("", "unknown url type: ''"),
    ],
)
def test_address_without_a_scheme_is_refused_as_no_valid_url(url, reason):
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(url, _Unreached(), 1.0, WHAT)
    assert str(caught.value) == (
        "cannot send a request to {}: the address is not a valid URL: "
        "{}".format(WHAT, reason)
    )


@pytest.mark.parametrize(
    "url",
    [
        "data:text/plain,hi",
        "file:///etc/hostname",
        "FILE:///etc/hostname",
        "ftp://nas.example.test/mcp",
    ],
)
def test_address_that_is_not_http_is_refused_before_any_request(url):
    # urllib opens these itself, and their replies have no HTTP status
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(url, _Unreached(), 1.0, WHAT)
    assert str(caught.value) == (
        "cannot send a request to {}: the address is not an http:// or "
        "https:// URL".format(WHAT)
    )


@pytest.mark.parametrize("url", ["http://[::1/whoami", "http://[fd00::5"])
def test_address_with_a_malformed_host_is_refused_as_no_valid_url(url):
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(url, webclient.OPENER, 1.0, WHAT)
    assert "the address is not a valid URL" in str(caught.value)


@pytest.mark.parametrize(
    "value",
    [
        "Bearer s3cr3t\r",
        "Bearer s3cr3t\nX-Extra: 1",
        # a byte outside UTF-8, as a POSIX environment hands it over
        "Bearer s3cr3t\udcff",
    ],
)
def test_header_that_http_cannot_carry_is_refused_without_its_value(value):
    with pytest.raises(webclient.ClientError) as caught:
        webclient.send(
            BASE + "/whoami",
            _Unreached(),
            1.0,
            WHAT,
            headers={"Accept": "application/json", "Authorization": value},
        )
    assert str(caught.value) == (
        "cannot send a request to {}: the Authorization header holds a "
        "character that HTTP does not allow".format(WHAT)
    )
    # the message leaves the address unblamed
    assert "valid URL" not in str(caught.value)


def test_send_posts_the_data_with_the_headers():
    seen = []

    class _Echo:
        def open(self, req, timeout=None):
            seen.append(
                (req.get_method(), req.full_url, req.data, timeout)
                + (req.get_header("Authorization"),)
            )
            return _Reply()

    class _Reply:
        status, headers = 200, Message()

        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    status, _headers, body = webclient.send(
        BASE + "/mcp",
        _Echo(),
        2.5,
        WHAT,
        headers={"Authorization": "Bearer t\tk"},
        data=b"{}",
    )
    assert (status, body) == (200, b"{}")
    assert seen == [("POST", BASE + "/mcp", b"{}", 2.5, "Bearer t\tk")]
    # without data, the request is a GET
    webclient.send(BASE + "/whoami", _Echo(), 1.0, WHAT)
    assert seen[-1][:3] == ("GET", BASE + "/whoami", None)


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Answers every GET with a fixed body and keeps the raw header."""

    seen: list = []
    body = b'{"ok": true}'

    def do_GET(self):
        # http.server decodes a header as Latin-1, byte for byte
        self.seen.append(self.headers["Authorization"].encode("latin-1"))
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args):
        pass


@pytest.fixture
def recorder():
    """A real HTTP server on loopback; its URL and what it received."""
    _Recorder.seen = []
    server = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
    # a short poll, so that shutdown returns at once
    thread = threading.Thread(
        target=server.serve_forever, args=(0.01,), daemon=True
    )
    thread.start()
    try:
        yield "http://127.0.0.1:{}".format(server.server_port), _Recorder.seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("token", ["s3cr3t", "tøk", "t✓k", "トークン🔑"])
def test_header_goes_out_as_the_utf8_bytes_the_daemon_compares(
    recorder, token
):
    url, seen = recorder
    value = "Bearer " + token
    status, _headers, _body = webclient.send(
        url + "/whoami",
        webclient.OPENER,
        5.0,
        WHAT,
        headers={"Authorization": value},
    )
    assert status == 200
    assert seen == [value.encode("utf-8")]


@pytest.mark.parametrize(
    "limit, expected", [(None, b'{"ok": true}'), (4, b'{"ok'), (0, b"")]
)
def test_limit_bounds_what_is_read_of_the_body(recorder, limit, expected):
    url, _seen = recorder
    status, headers, body = webclient.send(
        url + "/whoami",
        webclient.OPENER,
        5.0,
        WHAT,
        headers={"Authorization": "Bearer t"},
        limit=limit,
    )
    # the headers arrive whole whatever the limit
    assert (status, headers["Content-Length"], body) == (200, "12", expected)


def test_limit_bounds_the_body_of_an_error_reply():
    reply = urllib.error.HTTPError(
        BASE + "/whoami", 401, "error", Message(), io.BytesIO(b"x" * 64)
    )
    status, _headers, body = webclient.send(
        BASE + "/whoami", _Opener(reply), 1.0, WHAT, limit=8
    )
    assert (status, body) == (401, b"x" * 8)


# ---------------------------------------------------------------------------
# the token
# ---------------------------------------------------------------------------


def _token_args(token=None, token_env=None):
    return argparse.Namespace(token=token, token_env=token_env)


def test_resolve_token_reads_the_flag_then_the_variable(monkeypatch):
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "from-default")
    monkeypatch.setenv("OTHER_TOKEN", "from-named")
    assert (
        webclient.resolve_token(_token_args("flag", "OTHER_TOKEN")) == "flag"
    )
    assert webclient.resolve_token(_token_args(None, "OTHER_TOKEN")) == (
        "from-named"
    )
    assert webclient.resolve_token(_token_args()) == "from-default"
    monkeypatch.delenv(_cliargs.WEB_ENV_TOKEN)
    assert webclient.resolve_token(_token_args()) is None
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "")
    assert webclient.resolve_token(_token_args()) is None


@pytest.mark.parametrize(
    "flawed",
    ["tok\r", "tok\n", "to\x0bk", "tok\x7f", "tok\udcff", "\x1b[2Jtok"],
)
def test_resolve_token_refuses_a_token_that_no_header_can_carry(
    monkeypatch, flawed
):
    monkeypatch.setenv("OTHER_TOKEN", flawed)
    for args, source in (
        (_token_args(flawed), "--token"),
        (
            _token_args(None, "OTHER_TOKEN"),
            "the OTHER_TOKEN environment variable",
        ),
    ):
        with pytest.raises(webclient.ClientError) as caught:
            webclient.resolve_token(args)
        message = str(caught.value)
        assert message == (
            "the access token from {} holds a line break or another "
            "character that an HTTP header cannot carry (check it for a "
            "trailing newline)".format(source)
        )
        # the token itself stays out of the message
        assert "tok" not in message.replace("token", "")


@pytest.mark.parametrize(
    "token", ["tok en", "t\tk", "tøk", "t✓k", "トークン🔑", "a.b-c_d~e+f/g="]
)
def test_resolve_token_keeps_a_token_that_a_header_can_carry(token):
    assert webclient.resolve_token(_token_args(token)) == token


def test_reply_that_is_not_http_quotes_the_peer_escaped():
    raised = http.client.BadStatusLine("SSH-2.0-OpenSSH_9.6\r\n")
    with pytest.raises(webclient.ClientError) as caught:
        _send(raised)
    assert str(caught.value) == "no HTTP reply from {}: {!r}".format(
        WHAT, raised
    )
    assert "\r" not in str(caught.value)
