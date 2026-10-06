"""Tests for the ``cronstable pair`` command (:mod:`cronstable.paircli`).

The command is a synchronous client over one seam: the urllib opener that
:func:`cronstable.webclient.build_opener` returns, which is the module-level
``webclient.OPENER`` when no TLS flag is given. A scripted opener stands in
for the daemon here, in the style of ``tests/test_mcpcli.py``.
"""

import argparse
import base64
import contextlib
import http.client
import http.server
import io
import json
import os
import shutil
import socket
import ssl
import sys
import threading
import urllib.error
from email.message import Message

import pytest

from cronstable import (
    _cliargs,
    netutil,
    paircli,
    pairlink,
    pairprobe,
    qr,
    webclient,
)

SERVER = "http://cron.example.test:8080"
LOOPBACK = "http://127.0.0.1:8080"
LAN = "http://192.0.2.7:8080"
TOKEN = "phone-token-0123456789abcdef"
CHALLENGE = 'Bearer realm="cronstable"'
INSTANCE = "k3JxVq9d2mS0aB7cXw1zLQ"
# the header a daemon puts on every reply, and another daemon's
STAMP = {pairlink.INSTANCE_HEADER: INSTANCE}
OTHER_STAMP = {pairlink.INSTANCE_HEADER: "Zz9yXw8vUt7sRq6pOn5mLk"}

PHONE = {
    "authenticated": True,
    "label": "phone",
    "scopes": ["control", "view"],
    "allScopes": False,
    "pairLinkBase": "https://relay.cronstable.com/pair",
    "sealableSuites": ["x25519"],
    "instance": INSTANCE,
    "listeners": ["http://0.0.0.0:8080"],
}
# the reply of a daemon with no token configured
OPEN = dict(
    PHONE,
    authenticated=False,
    label=None,
    allScopes=True,
    scopes=["approve", "control", "view"],
)
STANDALONE = {"enabled": False}


def _args(**overrides):
    ns = argparse.Namespace(
        url=SERVER,
        token=TOKEN,
        token_env=None,
        cacert=None,
        client_cert=None,
        client_key=None,
        insecure=False,
        public_url=None,
        name=None,
        format="json",
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


class _Reply:
    def __init__(self, status, body, headers=None):
        self.status = status
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = value
        self._body = (
            body if isinstance(body, bytes) else json.dumps(body).encode()
        )
        # the size limit of each read, as http.client takes it
        self.limits = []

    def read(self, amt=None):
        self.limits.append(amt)
        return self._body if amt is None else self._body[:amt]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Daemon:
    """A scripted opener: full URL -> reply, error status, or exception.

    A callable route answers from the request. Replies below 300 are
    returned and the rest are raised as ``HTTPError``, as the command's
    redirect-free opener does. An unscripted URL is a refused connection.
    """

    def __init__(self, routes):
        self.routes = routes
        self.requests = []

    def open(self, req, timeout=None):
        self.requests.append(req)
        outcome = self.routes.get(req.full_url)
        if callable(outcome):
            outcome = outcome(req)
        if outcome is None:
            raise urllib.error.URLError(ConnectionRefusedError("refused"))
        if isinstance(outcome, Exception):
            raise outcome
        if outcome.status >= 300:
            raise urllib.error.HTTPError(
                req.full_url,
                outcome.status,
                "error",
                outcome.headers,
                io.BytesIO(outcome.read()),
            )
        return outcome

    def auth_for(self, url):
        return [
            req.get_header("Authorization")
            for req in self.requests
            if req.full_url == url
        ]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """No token or TLS variable leaks in from the developer's shell."""
    for name in (
        _cliargs.WEB_ENV_TOKEN,
        _cliargs.WEB_ENV_CACERT,
        _cliargs.WEB_ENV_CLIENT_CERT,
        _cliargs.WEB_ENV_CLIENT_KEY,
        _cliargs.WEB_ENV_INSECURE,
    ):
        monkeypatch.delenv(name, raising=False)


def _serve(monkeypatch, routes):
    daemon = _Daemon(routes)
    monkeypatch.setattr(webclient, "OPENER", daemon)
    return daemon


def _guarded(whoami, token=TOKEN):
    """A route for a daemon that requires ``token``: its challenge, or
    ``whoami`` for a request that presents the token. Both replies carry
    the instance ID that ``whoami`` names."""
    stamp = {pairlink.INSTANCE_HEADER: whoami["instance"]}

    def answer(req):
        if req.get_header("Authorization") == "Bearer " + token:
            return _Reply(200, whoami, stamp)
        return _Reply(
            401,
            {"error": "401"},
            dict(stamp, **{"WWW-Authenticate": CHALLENGE}),
        )

    return answer


def _routes(base=SERVER, whoami=PHONE, cluster=STANDALONE):
    return {
        base + "/whoami": _Reply(200, whoami),
        base + "/cluster": _Reply(200, cluster),
    }


# ---------------------------------------------------------------------------
# the three formats
# ---------------------------------------------------------------------------


def test_json_format_prints_the_pairing_payload(monkeypatch, capsys):
    daemon = _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args()) == 0
    out, err = capsys.readouterr()
    assert json.loads(out) == {
        "v": 1,
        "name": "cron.example.test:8080",
        "url": SERVER,
        "token": TOKEN,
    }
    assert (
        out == pairlink.payload("cron.example.test:8080", SERVER, TOKEN) + "\n"
    )
    assert err == ""
    assert daemon.auth_for(SERVER + "/whoami") == ["Bearer " + TOKEN]
    assert daemon.auth_for(SERVER + "/cluster") == ["Bearer " + TOKEN]


def test_link_format_prints_the_link_on_the_daemons_base(monkeypatch, capsys):
    base = "https://relay.example.test/pair"
    whoami = dict(PHONE, pairLinkBase=base)
    _serve(monkeypatch, _routes(whoami=whoami))
    assert paircli.dispatch(_args(format="link")) == 0
    link = capsys.readouterr().out.strip()
    assert link.startswith(base + "#")
    fragment = link.split("#")[1]
    decoded = base64.urlsafe_b64decode(fragment + "=" * (-len(fragment) % 4))
    assert json.loads(decoded)["url"] == SERVER


def test_link_format_falls_back_to_the_hosted_relay(monkeypatch, capsys):
    whoami = {k: v for k, v in PHONE.items() if k != "pairLinkBase"}
    _serve(monkeypatch, _routes(whoami=whoami))
    assert paircli.dispatch(_args(format="link")) == 0
    assert capsys.readouterr().out.startswith(
        "https://relay.cronstable.com/pair#"
    )


def _expected_link(name, url=SERVER, token=TOKEN):
    return pairlink.link(
        pairlink.payload(name, url, token), PHONE["pairLinkBase"]
    )


def _code_rows(out):
    """The painted symbol rows of the command's output, colors removed."""
    rows = [line for line in out.splitlines() if line.startswith("\x1b[")]
    assert rows
    for row in rows:
        assert row.startswith(qr.INK_ON_PAPER) and row.endswith(qr.SGR_RESET)
    return [row[len(qr.INK_ON_PAPER) : -len(qr.SGR_RESET)] for row in rows]


def test_qr_format_prints_the_caption_then_the_code(monkeypatch, capsys):
    _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args(format="qr", name="nas")) == 0
    out, err = capsys.readouterr()
    assert err == ""
    lines = out.splitlines()
    assert lines[0] == "Pairing code for nas ({})".format(SERVER)
    assert "access token" in lines[2]
    # the code is the last thing printed, so it stays on screen
    assert lines[-1].startswith(qr.INK_ON_PAPER)
    matrix = qr.encode(_expected_link("nas").encode(), "L")
    # not a terminal: the standard quiet zone
    assert _code_rows(out) == qr.half_block_rows(matrix, 4)


def test_qr_caption_names_an_unnamed_server_once(monkeypatch, capsys):
    _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args(format="qr")) == 0
    first = capsys.readouterr().out.splitlines()[0]
    assert first == "Pairing code for {}".format(SERVER)


def _terminal_size(monkeypatch, cols, lines):
    # pytest's verbose reporter asks for the size too, with a fallback
    monkeypatch.setattr(
        shutil,
        "get_terminal_size",
        lambda fallback=(80, 24): os.terminal_size((cols, lines)),
    )


def _as_terminal(monkeypatch, cols, lines):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)
    _terminal_size(monkeypatch, cols, lines)


def test_qr_on_a_terminal_narrows_the_quiet_zone_to_fit(monkeypatch, capsys):
    _serve(monkeypatch, _routes())
    matrix = qr.encode(_expected_link("nas").encode(), "L")
    side = len(matrix)
    # exactly the lines a quiet zone of 2 needs, plus one for the prompt
    _as_terminal(monkeypatch, 100, (side + 4 + 1) // 2 + 1)
    assert paircli.dispatch(_args(format="qr", name="nas")) == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert _code_rows(out) == qr.half_block_rows(matrix, 2)


def test_qr_on_a_roomy_terminal_keeps_the_standard_quiet_zone(
    monkeypatch, capsys
):
    _serve(monkeypatch, _routes())
    _as_terminal(monkeypatch, 200, 80)
    assert paircli.dispatch(_args(format="qr", name="nas")) == 0
    matrix = qr.encode(_expected_link("nas").encode(), "L")
    assert _code_rows(capsys.readouterr().out) == qr.half_block_rows(matrix, 4)


def test_qr_on_a_small_terminal_says_what_it_needs(monkeypatch, capsys):
    _serve(monkeypatch, _routes())
    _as_terminal(monkeypatch, 40, 10)
    assert paircli.dispatch(_args(format="qr", name="nas")) == 0
    out, err = capsys.readouterr()
    matrix = qr.encode(_expected_link("nas").encode(), "L")
    cols, lines = qr.smallest_fit(matrix)
    assert "at least {} columns by {} lines".format(cols, lines + 1) in err
    assert "this one is 40 by 10" in err
    # still printed, for a window the operator then enlarges: at the
    # narrowest margin, which is the size the warning names
    rows = _code_rows(out)
    assert rows == qr.half_block_rows(matrix, 1)
    assert (len(rows[0]), len(rows)) == (cols, lines)


class _Terminal(io.StringIO):
    """One stream for stdout and stderr, as a terminal shows them."""

    def isatty(self):
        return True


def test_small_terminal_warning_follows_the_code(monkeypatch):
    _serve(monkeypatch, _routes())
    screen = _Terminal()
    monkeypatch.setattr(sys, "stdout", screen)
    monkeypatch.setattr(sys, "stderr", screen)
    _terminal_size(monkeypatch, 80, 24)
    assert paircli.dispatch(_args(format="qr", name="nas")) == 0
    lines = screen.getvalue().splitlines()
    # the last thing on screen, where the code above it has scrolled
    assert lines[-1].startswith("warning: The code needs a terminal of")
    assert lines[-1].endswith("Then run the command again.")
    assert lines[-2].startswith(qr.INK_ON_PAPER)
    assert sum(line.startswith("warning: ") for line in lines) == 1


def test_qr_caption_for_a_code_without_a_token_claims_none(
    monkeypatch, capsys
):
    _serve(monkeypatch, _routes(whoami=OPEN))
    assert paircli.dispatch(_args(format="qr", name="nas", token=None)) == 0
    out, err = capsys.readouterr()
    caption = out.split(qr.INK_ON_PAPER)[0]
    assert "Scan the code" in caption
    assert "access token" not in caption
    assert "the code carries none" in err


def test_a_link_too_long_for_a_code_is_a_clean_error(monkeypatch, capsys):
    _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args(format="qr", name="n" * 3000)) == 1
    err = capsys.readouterr().err
    assert err.startswith("cronstable pair: the pairing link is too long")
    assert "--format link" in err


# ---------------------------------------------------------------------------
# the server name and the phone's address
# ---------------------------------------------------------------------------


def test_name_defaults_to_the_cluster_node(monkeypatch, capsys):
    cluster = {"enabled": True, "node_name": "node-a"}
    _serve(monkeypatch, _routes(cluster=cluster))
    assert paircli.dispatch(_args()) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "node-a"


def test_name_falls_back_when_cluster_is_unavailable(monkeypatch, capsys):
    routes = _routes()
    routes[SERVER + "/cluster"] = _Reply(503, {"error": "busy"})
    _serve(monkeypatch, routes)
    assert paircli.dispatch(_args()) == 0
    assert (
        json.loads(capsys.readouterr().out)["name"] == "cron.example.test:8080"
    )
    del routes[SERVER + "/cluster"]  # now a refused connection
    assert paircli.dispatch(_args()) == 0
    assert (
        json.loads(capsys.readouterr().out)["name"] == "cron.example.test:8080"
    )


def test_name_flag_wins(monkeypatch, capsys):
    daemon = _serve(
        monkeypatch, _routes(cluster={"enabled": True, "node_name": "node-a"})
    )
    assert paircli.dispatch(_args(name="nas")) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "nas"
    assert daemon.auth_for(SERVER + "/cluster") == []


@pytest.mark.parametrize(
    "name, cluster",
    [
        # an argument with a byte outside UTF-8, as a POSIX shell passes it
        ("na\udcffs", STANDALONE),
        # the same character in the daemon's own node name
        (None, {"enabled": True, "node_name": "na\udcffs"}),
    ],
)
def test_a_name_that_utf8_cannot_encode_is_a_clean_error(
    monkeypatch, capsys, name, cluster
):
    _serve(monkeypatch, _routes(cluster=cluster))
    for fmt in ("json", "link", "qr"):
        assert paircli.dispatch(_args(name=name, format=fmt)) == 1
        out, err = capsys.readouterr()
        assert out == ""
        assert err == (
            "cronstable pair: the server name 'na\\udcffs' holds a "
            "character that UTF-8 cannot encode\n"
        )


def test_public_url_goes_into_the_code(monkeypatch, capsys):
    daemon = _serve(monkeypatch, _routes(base=LOOPBACK))
    args = _args(url=LOOPBACK, public_url="https://cron.example.net/")
    assert paircli.dispatch(args) == 0
    out, err = capsys.readouterr()
    assert json.loads(out) == {
        "v": 1,
        "name": "cron.example.net",
        "url": "https://cron.example.net",
        "token": TOKEN,
    }
    # an address the operator gave is no substitution to announce
    assert err == ""
    # the daemon is still reached at --url, and nothing dials the phone's
    assert {req.full_url for req in daemon.requests} == {
        LOOPBACK + "/whoami",
        LOOPBACK + "/cluster",
    }


def test_loopback_url_becomes_the_lan_address_the_daemon_answers_at(
    monkeypatch, capsys
):
    routes = _routes(base=LOOPBACK)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    out, err = capsys.readouterr()
    doc = json.loads(out)
    assert doc["url"] == LAN
    assert doc["name"] == "192.0.2.7:8080"
    assert doc["token"] == TOKEN
    # one request, which carried no token: the daemon's instance ID on its
    # challenge is what identifies it
    assert daemon.auth_for(LAN + "/whoami") == [None]
    # and the operator is told that the address was substituted
    assert err == (
        "The code names this host's address {} in place of {}. Pass "
        "--public-url when the phone reaches the server at another "
        "address.\n".format(LAN, LOOPBACK)
    )


def test_loopback_url_accepts_an_open_daemon_on_the_lan_address(
    monkeypatch, capsys
):
    routes = _routes(base=LOOPBACK, whoami=OPEN)
    routes[LAN + "/whoami"] = _Reply(200, OPEN, STAMP)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK, token=None)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN
    assert daemon.auth_for(LAN + "/whoami") == [None]


def test_a_token_the_daemon_ignored_stays_off_the_lan_address(
    monkeypatch, capsys
):
    routes = _routes(base=LOOPBACK, whoami=OPEN)
    routes[LAN + "/whoami"] = _Reply(200, OPEN, STAMP)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    assert json.loads(capsys.readouterr().out)["token"] == ""
    assert daemon.auth_for(LAN + "/whoami") == [None]


@pytest.mark.parametrize(
    "session, token, lan_route",
    [
        # another daemon that requires a token
        (
            PHONE,
            TOKEN,
            _Reply(
                401, {}, dict(OTHER_STAMP, **{"WWW-Authenticate": CHALLENGE})
            ),
        ),
        # another daemon with the same configuration: it holds the token
        # and describes it as the first one does
        (
            PHONE,
            TOKEN,
            _guarded(dict(PHONE, instance="Zz9yXw8vUt7sRq6pOn5mLk")),
        ),
        # two daemons that require no token give one description too
        (OPEN, None, _Reply(200, OPEN, OTHER_STAMP)),
    ],
)
def test_a_different_daemon_on_the_lan_address_gets_no_code(
    monkeypatch, capsys, session, token, lan_route
):
    routes = _routes(base=LOOPBACK, whoami=session)
    routes[LAN + "/whoami"] = lan_route
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK, token=token)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert (
        "a different cronstable server answers at this host's LAN address "
        "({}).".format(LAN)
        in err
    )
    assert "--public-url" in err
    # the other daemon never gets the token
    assert daemon.auth_for(LAN + "/whoami") == [None]


@pytest.mark.parametrize(
    "listeners",
    [
        # loopback only, the default
        ["http://127.0.0.1:8080"],
        # the LAN address serves the other scheme
        ["http://127.0.0.1:8080", "https://0.0.0.0:8443"],
        # every IPv6 address: that socket takes no IPv4 connection
        ["http://127.0.0.1:8080", "http://[::]:8080"],
        # a daemon with no bound TCP socket
        [],
    ],
)
def test_daemon_without_a_listener_on_the_lan_address_is_not_asked(
    monkeypatch, capsys, listeners
):
    whoami = dict(PHONE, listeners=listeners)
    routes = _routes(base=LOOPBACK, whoami=whoami)
    # another process on this host holds the LAN address and repeats the
    # daemon's replies, instance ID included
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith(
        "cronstable pair: {} is a loopback address, which a phone cannot "
        "reach, and the server reports no http listener on this host's LAN "
        "address (192.0.2.7) or on another address that a phone can "
        "reach.".format(LOOPBACK)
    )
    assert "web.listen" in err and "--public-url" in err
    # nothing was asked of the address, with or without the token
    assert daemon.auth_for(LAN + "/whoami") == []


VPN = "http://10.8.0.1:8080"


def test_listener_on_another_interface_goes_into_the_code(monkeypatch, capsys):
    # loopback and a VPN address, on a host whose default route is the LAN
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", VPN])
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[VPN + "/whoami"] = _guarded(whoami)
    # another process holds the LAN address, which the daemon does not serve
    routes[LAN + "/whoami"] = _guarded(whoami)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    out, err = capsys.readouterr()
    assert json.loads(out)["url"] == VPN
    note = "this host's address {} in place of {}.".format(VPN, LOOPBACK)
    assert note in err
    # the listener's own address was asked, without the token
    assert daemon.auth_for(VPN + "/whoami") == [None]
    assert daemon.auth_for(LAN + "/whoami") == []


def test_listener_on_another_interface_needs_no_lan_address(
    monkeypatch, capsys
):
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", VPN])
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[VPN + "/whoami"] = _guarded(whoami)
    _serve(monkeypatch, routes)
    # a host with no default route
    monkeypatch.setattr(netutil, "lan_address", lambda: None)
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == VPN


def test_lan_address_is_preferred_to_another_interface(monkeypatch, capsys):
    whoami = dict(PHONE, listeners=[VPN, "http://0.0.0.0:8080"])
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[VPN + "/whoami"] = _guarded(whoami)
    routes[LAN + "/whoami"] = _guarded(whoami)
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN


def test_failed_check_of_another_interface_names_it(monkeypatch, capsys):
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", VPN])
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[VPN + "/whoami"] = _Reply(200, PHONE, OTHER_STAMP)
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    err = capsys.readouterr().err
    # the address is this host's, and it is no LAN address
    assert (
        "a different cronstable server answers at this host's address "
        "({}).".format(VPN)
        in err
    )
    assert "LAN address" not in err


def test_anonymous_connection_is_told_to_present_a_token(monkeypatch, capsys):
    # web.anonymousScopes: the reply to a connection without a token has no
    # listeners, so there is nothing to check the LAN address against
    whoami = dict(
        PHONE, authenticated=False, label="anonymous", scopes=["view"]
    )
    del whoami["listeners"]
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK, token=None)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err == (
        "cronstable pair: {} is a loopback address, which a phone cannot "
        "reach, and the server names its listeners only to a connection "
        "that presents an access token. Pass --token-env VAR or set {} to "
        "present one, or pass --public-url with the address the phone "
        "uses.\n".format(LOOPBACK, _cliargs.WEB_ENV_TOKEN)
    )
    # the daemon reported no listener, and the message claims none
    assert "web.listen" not in err
    assert daemon.auth_for(LAN + "/whoami") == []
    # an address the operator gives needs no listeners
    args = _args(url=LOOPBACK, token=None, public_url=LAN)
    assert paircli.dispatch(args) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN


def test_reply_without_listeners_is_reported_as_one(monkeypatch, capsys):
    # a daemon of a release that reports none
    whoami = {k: v for k, v in PHONE.items() if k != "listeners"}
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    err = capsys.readouterr().err
    assert err.startswith(
        "cronstable pair: {} is a loopback address, which a phone cannot "
        "reach, and the server's reply names no listeners.".format(LOOPBACK)
    )
    assert "--public-url" in err
    assert daemon.auth_for(LAN + "/whoami") == []


@pytest.mark.parametrize(
    "lan_reply, reason",
    [
        # nothing listens there
        (None, "cannot reach the cronstable server at " + LAN),
        (
            _Reply(401, {}, {"WWW-Authenticate": 'Basic realm="router"'}),
            LAN + "/whoami returned HTTP 401",
        ),
        # the daemon's challenge is public, so it identifies no daemon
        (
            _Reply(401, {}, {"WWW-Authenticate": CHALLENGE}),
            "another server answers at this host's LAN address",
        ),
        (_Reply(200, {"hello": "world"}), LAN + "/whoami returned HTTP 200"),
        (_Reply(200, PHONE), LAN + "/whoami returned HTTP 200"),
        (
            _Reply(200, b"<html>not json</html>"),
            "another server answers at this host's LAN address",
        ),
        # the listener's certificate does not name the LAN address
        (
            urllib.error.URLError(
                ssl.SSLCertVerificationError("IP address mismatch")
            ),
            "TLS verification failed for the cronstable server at " + LAN,
        ),
        (
            _Reply(302, b"", {"Location": "https://nas.example.test/whoami"}),
            "redirects to 'https://nas.example.test/whoami'",
        ),
        (http.client.BadStatusLine("SSH-2.0\r\n"), "no HTTP reply from"),
    ],
)
def test_lan_address_that_is_not_the_daemon_is_an_error_that_says_why(
    monkeypatch, capsys, lan_reply, reason
):
    routes = _routes(base=LOOPBACK)
    if lan_reply is not None:
        routes[LAN + "/whoami"] = lan_reply
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith(
        "cronstable pair: {} is a loopback address, which a phone cannot "
        "reach, and ".format(LOOPBACK)
    )
    assert reason in err
    assert "--public-url" in err
    # the advice about --url and its TLS flags changes nothing for the phone
    assert "--insecure" not in err and "--cacert" not in err
    assert "--url " not in err
    tls = "TLS verification failed" in reason
    assert ("that the listener's certificate names" in err) == tls
    assert ("web.listen" in err) != tls
    # the address was asked once, without the token
    assert daemon.auth_for(LAN + "/whoami") == [None]


@pytest.mark.parametrize(
    "lan_listener", ["http://0.0.0.0:9090", "http://192.0.2.7:9090"]
)
def test_loopback_url_takes_the_port_of_the_lan_listener(
    monkeypatch, capsys, lan_listener
):
    # one port on loopback, another on the LAN
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", lan_listener])
    other_port = "http://192.0.2.7:9090"
    routes = _routes(base=LOOPBACK, whoami=whoami)
    routes[other_port + "/whoami"] = _guarded(whoami)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    out, err = capsys.readouterr()
    assert json.loads(out)["url"] == other_port
    note = "address {} in place of {}.".format(other_port, LOOPBACK)
    assert note in err
    # the daemon binds the port of --url on loopback alone, so only the
    # listener's port was asked, without the token
    assert daemon.auth_for(LAN + "/whoami") == []
    assert daemon.auth_for(other_port + "/whoami") == [None]


def test_a_port_bound_on_loopback_alone_is_not_asked_on_the_lan_address(
    monkeypatch, capsys
):
    whoami = dict(
        PHONE, listeners=["http://127.0.0.1:8080", "http://0.0.0.0:9090"]
    )
    other_port = "http://192.0.2.7:9090"
    routes = _routes(base=LOOPBACK, whoami=whoami)
    # another process on this host holds the LAN address at the port of
    # --url and repeats the daemon's replies, instance ID included
    routes[LAN + "/whoami"] = _guarded(whoami)
    routes[other_port + "/whoami"] = _guarded(whoami)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    # the code names the port that the daemon serves the address on
    assert json.loads(capsys.readouterr().out)["url"] == other_port
    assert daemon.auth_for(LAN + "/whoami") == []


def test_published_port_is_preferred_to_the_port_the_daemon_binds(
    monkeypatch, capsys
):
    # a container publishes 9000 for the daemon's 8080, and the daemon
    # answers on both
    published, lan = "http://127.0.0.1:9000", "http://192.0.2.7:9000"
    routes = _routes(base=published)
    routes[lan + "/whoami"] = _guarded(PHONE)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=published)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == lan
    # the ports are asked at once, and neither request carries the token
    assert daemon.auth_for(lan + "/whoami") == [None]
    assert set(daemon.auth_for(LAN + "/whoami")) <= {None}


def test_published_port_is_preferred_to_a_listener_on_one_address(
    monkeypatch, capsys
):
    # a container publishes 9000 for the daemon's 8080, which the daemon
    # binds on the container's own address, and the host reaches both
    published, lan = "http://127.0.0.1:9000", "http://192.0.2.7:9000"
    own = "http://172.17.0.2:8080"
    whoami = dict(PHONE, listeners=[own])
    routes = _routes(base=published, whoami=whoami)
    routes[lan + "/whoami"] = _guarded(whoami)
    routes[own + "/whoami"] = _guarded(whoami)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=published)) == 0
    # the code names the address that a phone on the LAN can dial
    assert json.loads(capsys.readouterr().out)["url"] == lan
    assert daemon.auth_for(lan + "/whoami") == [None]


def test_another_server_on_the_port_of_the_url_does_not_end_the_check(
    monkeypatch, capsys
):
    # the daemon binds no port 9000, so both ports are asked
    forwarded, lan = "http://127.0.0.1:9000", "http://192.0.2.7:9000"
    routes = _routes(base=forwarded)
    routes[lan + "/whoami"] = _Reply(200, PHONE, OTHER_STAMP)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=forwarded)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN
    assert daemon.auth_for(lan + "/whoami") == [None]


def test_no_port_that_answers_reports_the_first_port_asked(
    monkeypatch, capsys
):
    forwarded, lan = "http://127.0.0.1:9000", "http://192.0.2.7:9000"
    routes = _routes(base=forwarded)
    routes[LAN + "/whoami"] = _Reply(200, PHONE, OTHER_STAMP)
    daemon = _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=forwarded)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert "cannot reach the cronstable server at " + lan in err
    assert "a different cronstable server" not in err
    # each port was asked once, without the token
    assert daemon.auth_for(lan + "/whoami") == [None]
    assert daemon.auth_for(LAN + "/whoami") == [None]


def test_addresses_are_checked_at_once(monkeypatch, capsys):
    # the port of --url answers only after the listener's port was asked,
    # which a check of one address at a time never reaches
    forwarded, lan = "http://127.0.0.1:9000", "http://192.0.2.7:9000"
    listener_asked = threading.Event()

    def another_server(req):
        assert listener_asked.wait(10)
        return _Reply(200, PHONE, OTHER_STAMP)

    def daemon_on_its_port(req):
        listener_asked.set()
        return _guarded(PHONE)(req)

    routes = _routes(base=forwarded)
    routes[lan + "/whoami"] = another_server
    routes[LAN + "/whoami"] = daemon_on_its_port
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=forwarded)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN


def test_address_that_never_answers_ends_the_check_at_the_deadline(
    monkeypatch, capsys
):
    answered = threading.Event()

    def stall(req):
        answered.wait(30)

    routes = _routes(base=LOOPBACK)
    routes[LAN + "/whoami"] = stall
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    monkeypatch.setattr(pairprobe, "_PROBE_TIMEOUT", 0.05)
    try:
        assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    finally:
        answered.set()
    out, err = capsys.readouterr()
    assert out == ""
    assert (
        "the check of this host's LAN address failed: no reply from {} "
        "within 0.05 seconds.".format(LAN)
    ) in err
    assert "--public-url" in err


def test_address_check_reads_the_headers_and_no_body(monkeypatch, capsys):
    routes = _routes(base=LOOPBACK)
    # a body with no end would follow these headers
    answer = _Reply(200, b"x" * 4096, STAMP)
    routes[LAN + "/whoami"] = answer
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN
    assert answer.limits == [0]
    # the replies at --url are read up to a limit
    assert routes[LOOPBACK + "/whoami"].limits == [paircli._BODY_LIMIT]
    assert routes[LOOPBACK + "/cluster"].limits == [paircli._BODY_LIMIT]


def test_reply_longer_than_the_limit_is_not_the_expected_json(
    monkeypatch, capsys
):
    endless = _Reply(200, b"[" + b"0," * paircli._BODY_LIMIT)
    err = _fails(monkeypatch, capsys, {SERVER + "/whoami": endless})
    assert "/whoami answered HTTP 200 without the expected JSON" in err


def test_reply_nested_too_deep_is_not_the_expected_json(monkeypatch, capsys):
    # the depth at which json.loads gives up differs by interpreter and
    # platform, so the parser's error is raised here
    def give_up(body):
        raise RecursionError("maximum recursion depth exceeded")

    routes = _routes()
    monkeypatch.setattr(paircli.json, "loads", give_up)
    err = _fails(monkeypatch, capsys, routes)
    assert "/whoami answered HTTP 200 without the expected JSON" in err


def test_only_the_address_of_a_listener_reaches_a_request_or_a_message(
    monkeypatch, capsys
):
    # a server whose reply carries terminal escapes after an address
    hostile = "http://0.0.0.0:9090/\x1b]0;x\x07\x1b[2J?\x9b1m#\x1b[0m"
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", hostile])
    daemon = _serve(monkeypatch, _routes(base=LOOPBACK, whoami=whoami))
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    # the scheme, the host, and the port: nothing else of the entry
    asked = "http://192.0.2.7:9090"
    assert daemon.auth_for(asked + "/whoami") == [None]
    assert "cannot reach the cronstable server at {}: ".format(asked) in err
    assert not set("\x1b\x07\x9b") & set(err)


@pytest.mark.parametrize("fmt", ["link", "json", "qr"])
def test_link_base_that_is_no_printable_url_is_a_clean_error(
    monkeypatch, capsys, fmt
):
    hostile = "https://relay.example.test/pair\x1b]0;x\x07"
    _serve(monkeypatch, _routes(whoami=dict(PHONE, pairLinkBase=hostile)))
    assert paircli.dispatch(_args(format=fmt)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith("cronstable pair: the server's pairing link base ")
    # quoted with its escapes spelled out
    assert "pair\\x1b]0;x\\x07" in err
    assert not set("\x1b\x07") & set(err)


def test_loopback_url_without_a_lan_address_is_an_error(monkeypatch, capsys):
    _serve(monkeypatch, _routes(base=LOOPBACK))
    monkeypatch.setattr(netutil, "lan_address", lambda: None)
    assert paircli.dispatch(_args(url=LOOPBACK)) == 1
    err = capsys.readouterr().err
    assert "which a phone cannot reach. Add a LAN" in err


def test_loopback_in_another_spelling_gets_the_same_check(monkeypatch, capsys):
    short = "http://127.1:8080"
    routes = _routes(base=short)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    _serve(monkeypatch, routes)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=short)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN


def test_unverified_https_session_does_not_vouch_for_the_lan_address(
    monkeypatch, capsys
):
    # --insecure would pass a certificate that the phone rejects
    loopback = "https://127.0.0.1:8443"
    lan = "https://192.0.2.7:8443"
    whoami = dict(PHONE, listeners=["https://0.0.0.0:8443"])
    routes = _routes(base=loopback, whoami=whoami)
    routes[lan + "/whoami"] = _guarded(whoami)
    daemon = _Daemon(routes)
    monkeypatch.setattr(webclient, "build_opener", lambda ctx: daemon)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=loopback, insecure=True)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert "this connection skips certificate verification" in err
    assert (
        "Pass --public-url with an address that the listener's certificate "
        "names." in err
    )
    assert daemon.auth_for(lan + "/whoami") == []
    # an explicit address is the operator's own statement
    args = _args(url=loopback, insecure=True, public_url=lan)
    assert paircli.dispatch(args) == 0
    assert json.loads(capsys.readouterr().out)["url"] == lan


def test_unverified_session_over_http_still_checks_the_lan_address(
    monkeypatch, capsys
):
    routes = _routes(base=LOOPBACK)
    routes[LAN + "/whoami"] = _guarded(PHONE)
    daemon = _Daemon(routes)
    monkeypatch.setattr(webclient, "build_opener", lambda ctx: daemon)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    assert paircli.dispatch(_args(url=LOOPBACK, insecure=True)) == 0
    assert json.loads(capsys.readouterr().out)["url"] == LAN


# ---------------------------------------------------------------------------
# notes about the credential
# ---------------------------------------------------------------------------


def test_all_scopes_token_warns_on_stderr(monkeypatch, capsys):
    whoami = dict(PHONE, allScopes=True, scopes=["approve", "control", "view"])
    _serve(monkeypatch, _routes(whoami=whoami))
    assert paircli.dispatch(_args()) == 0
    out, err = capsys.readouterr()
    assert err == (
        "warning: This token grants full access. To limit phone access, "
        "configure a token with fewer permissions in web.authTokens.\n"
    )
    assert json.loads(out)["token"] == TOKEN


def test_open_daemon_pairs_without_a_token(monkeypatch, capsys):
    whoami = dict(
        PHONE,
        authenticated=False,
        label=None,
        allScopes=True,
        scopes=["approve", "control", "view"],
    )
    daemon = _serve(monkeypatch, _routes(whoami=whoami))
    assert paircli.dispatch(_args(token=None)) == 0
    out, err = capsys.readouterr()
    assert json.loads(out)["token"] == ""
    assert err.startswith("warning: The server authenticated no access token")
    assert daemon.auth_for(SERVER + "/whoami") == [None]


@pytest.mark.parametrize("fmt", ["json", "link", "qr"])
def test_open_daemon_keeps_a_stray_token_out_of_the_code(
    monkeypatch, capsys, fmt
):
    # the documented env fallback, exported for another server
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "for-another-server")
    whoami = dict(
        PHONE,
        authenticated=False,
        label=None,
        allScopes=True,
        scopes=["approve", "control", "view"],
    )
    _serve(monkeypatch, _routes(whoami=whoami))
    assert paircli.dispatch(_args(token=None, format=fmt, name="nas")) == 0
    out, err = capsys.readouterr()
    assert "the code carries none" in err
    tokenless = pairlink.payload("nas", SERVER, None)
    link = pairlink.link(tokenless, PHONE["pairLinkBase"])
    if fmt == "json":
        assert out == tokenless + "\n"
    elif fmt == "link":
        assert out == link + "\n"
    else:
        matrix = qr.encode(link.encode(), "L")
        assert _code_rows(out) == qr.half_block_rows(matrix, 4)


def test_view_only_token_warns_that_it_cannot_register(monkeypatch, capsys):
    _serve(monkeypatch, _routes(whoami=dict(PHONE, scopes=["view"])))
    assert paircli.dispatch(_args()) == 0
    assert "requires the control scope" in capsys.readouterr().err


def test_token_comes_from_the_environment(monkeypatch, capsys):
    daemon = _serve(monkeypatch, _routes())
    monkeypatch.setenv(_cliargs.WEB_ENV_TOKEN, "from-env")
    assert paircli.dispatch(_args(token=None)) == 0
    assert json.loads(capsys.readouterr().out)["token"] == "from-env"
    assert daemon.auth_for(SERVER + "/whoami") == ["Bearer from-env"]


def test_token_outside_ascii_goes_out_as_utf8(monkeypatch, capsys):
    token = "tøk-✓"
    daemon = _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args(token=token)) == 0
    assert json.loads(capsys.readouterr().out)["token"] == token
    # http.client writes a header as Latin-1, so these are the UTF-8 bytes
    # that the daemon compares
    [sent] = daemon.auth_for(SERVER + "/whoami")
    assert sent.encode("latin-1") == ("Bearer " + token).encode("utf-8")


# ---------------------------------------------------------------------------
# failures
# ---------------------------------------------------------------------------


def _fails(monkeypatch, capsys, routes, **overrides):
    _serve(monkeypatch, routes)
    assert paircli.dispatch(_args(**overrides)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith("cronstable pair: ")
    return err


def test_rejected_token(monkeypatch, capsys):
    routes = {
        SERVER + "/whoami": _Reply(401, {}, {"WWW-Authenticate": CHALLENGE})
    }
    err = _fails(monkeypatch, capsys, routes)
    assert "rejected the access token" in err


def test_missing_token(monkeypatch, capsys):
    routes = {
        SERVER + "/whoami": _Reply(401, {}, {"WWW-Authenticate": CHALLENGE})
    }
    err = _fails(monkeypatch, capsys, routes, token=None)
    assert "requires an access token" in err
    assert _cliargs.WEB_ENV_TOKEN in err


@pytest.mark.parametrize(
    "flawed", ["s3cr3t-value\r", "s3cr3t\nvalue", "s3cr3t\x7f"]
)
def test_token_that_no_header_can_carry_is_a_clean_error(
    monkeypatch, capsys, flawed
):
    # a variable read from a file with a CRLF line ending, for example
    daemon = _serve(monkeypatch, _routes())
    monkeypatch.setenv("PHONE_TOKEN", flawed)
    assert paircli.dispatch(_args(token=None, token_env="PHONE_TOKEN")) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith(
        "cronstable pair: the access token from the PHONE_TOKEN environment "
        "variable holds a line break or another character"
    )
    assert "trailing newline" in err
    # the message names the source and keeps the token to itself
    assert "s3cr3t" not in err
    assert daemon.requests == []


def test_token_flag_that_no_header_can_carry_names_the_flag(
    monkeypatch, capsys
):
    _serve(monkeypatch, _routes())
    assert paircli.dispatch(_args(token="s3cr3t\x00")) == 1
    err = capsys.readouterr().err
    assert "the access token from --token holds" in err
    assert "s3cr3t" not in err


@pytest.mark.parametrize(
    "reply",
    [
        _Reply(404, {"error": "not found"}),
        # a proxy's refusal: the daemon grants every token the view scope
        _Reply(403, {"error": "forbidden"}),
        _Reply(500, b"boom"),
        _Reply(200, b"<html>some other server</html>"),
        _Reply(200, ["not", "a", "document"]),
    ],
)
def test_a_server_that_is_not_cronstable(monkeypatch, capsys, reply):
    err = _fails(monkeypatch, capsys, {SERVER + "/whoami": reply})
    assert "/whoami answered HTTP {}".format(reply.status) in err
    assert "--url names a cronstable server" in err


def test_unreachable_server(monkeypatch, capsys):
    err = _fails(monkeypatch, capsys, {})
    assert "cannot reach the cronstable server at " + SERVER in err


@pytest.mark.parametrize(
    "raised", [TimeoutError("timed out"), OSError("reset")]
)
def test_transport_errors_below_urllib(monkeypatch, capsys, raised):
    err = _fails(monkeypatch, capsys, {SERVER + "/whoami": raised})
    assert "cannot reach the cronstable server" in err
    assert str(raised) in err


@pytest.mark.parametrize(
    "raised",
    [
        http.client.BadStatusLine("SSH-2.0-OpenSSH_9.6\r\n"),
        http.client.IncompleteRead(b"{", 40),
    ],
)
def test_replies_that_are_not_http(monkeypatch, capsys, raised):
    err = _fails(monkeypatch, capsys, {SERVER + "/whoami": raised})
    assert "no HTTP reply from the cronstable server at " + SERVER in err
    assert repr(raised) in err


def test_redirect_to_the_api_elsewhere_names_the_url_to_pass(
    monkeypatch, capsys
):
    elsewhere = "https://cron.example.test:8443/whoami"
    routes = {SERVER + "/whoami": _Reply(301, b"", {"Location": elsewhere})}
    err = _fails(monkeypatch, capsys, routes)
    assert "{} redirects to {!r}".format(SERVER, elsewhere) in err
    # the base that --url takes, without the endpoint
    assert "(pass --url 'https://cron.example.test:8443' instead)" in err


@pytest.mark.parametrize(
    "location, target",
    [
        # a sign-in proxy, as a relative reference
        (
            "/oauth2/sign_in?rd=%2Fwhoami",
            SERVER + "/oauth2/sign_in?rd=%2Fwhoami",
        ),
        ("https://login.example.test/", "https://login.example.test/"),
        # the same base again
        ("/whoami/", SERVER + "/whoami/"),
        (SERVER + "/whoami", SERVER + "/whoami"),
    ],
)
def test_redirect_elsewhere_names_no_url_to_pass(
    monkeypatch, capsys, location, target
):
    routes = {SERVER + "/whoami": _Reply(302, b"", {"Location": location})}
    err = _fails(monkeypatch, capsys, routes)
    assert "{} redirects to {!r}".format(SERVER, target) in err
    assert "pass --url" not in err
    assert "point --url at an address that answers without a redirect" in err


@contextlib.contextmanager
def _listening(handler):
    """A real HTTP server on loopback for the unpatched opener; its URL."""

    class _Quiet(handler):
        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), _Quiet)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:{}".format(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_the_token_does_not_follow_a_redirect_to_another_origin(capsys):
    seen = []

    class _Target(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.headers.get("Authorization"))
            body = json.dumps(PHONE).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with _listening(_Target) as target:

        class _Redirect(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", target + self.path)
                self.send_header("Content-Length", "0")
                self.end_headers()

        with _listening(_Redirect) as url:
            assert paircli.dispatch(_args(url=url)) == 1
    out, err = capsys.readouterr()
    assert out == ""
    assert "redirects to '{}/whoami'".format(target) in err
    assert "pass --url '{}' instead".format(target) in err
    assert seen == []


def test_a_listener_that_is_not_http_is_a_clean_error(capsys):
    with socket.create_server(("127.0.0.1", 0)) as listener:

        def answer():
            conn, _peer = listener.accept()
            with conn:
                conn.recv(4096)
                conn.sendall(b"SSH-2.0-OpenSSH_9.6\r\n")

        thread = threading.Thread(target=answer, daemon=True)
        thread.start()
        url = "http://127.0.0.1:{}".format(listener.getsockname()[1])
        assert paircli.dispatch(_args(url=url)) == 1
        thread.join()
    out, err = capsys.readouterr()
    assert out == ""
    assert err.startswith(
        "cronstable pair: no HTTP reply from the cronstable server at " + url
    )
    assert "BadStatusLine" in err


def test_tls_failure_names_the_certificate_flags(monkeypatch, capsys):
    failure = urllib.error.URLError(
        ssl.SSLCertVerificationError("certificate verify failed")
    )
    err = _fails(monkeypatch, capsys, {SERVER + "/whoami": failure})
    assert "TLS verification failed" in err
    assert "--cacert" in err
    assert "cannot reach" not in err


def test_bad_tls_material_is_a_clean_error(monkeypatch, capsys, tmp_path):
    err = _fails(
        monkeypatch, capsys, _routes(), cacert=str(tmp_path / "absent.pem")
    )
    assert "TLS material" in err
    assert "absent.pem" in err


@pytest.mark.parametrize("flag", ["url", "public_url"])
def test_urls_that_are_not_http(monkeypatch, capsys, flag):
    err = _fails(monkeypatch, capsys, _routes(), **{flag: "ftp://nas.local"})
    assert "'ftp://nas.local' is not an http:// or https:// URL" in err


@pytest.mark.parametrize("flag", ["url", "public_url"])
def test_host_that_no_request_can_name(monkeypatch, capsys, flag):
    bad = "http://cron example.test:8080"
    err = _fails(monkeypatch, capsys, _routes(), **{flag: bad})
    assert "{!r} is not an http:// or https:// URL".format(bad) in err


@pytest.mark.parametrize(
    "path, encoded",
    [("/x y", "/x%20y"), ("/café", "/caf%C3%A9"), ("/a%20b", "/a%20b")],
)
def test_url_path_is_percent_encoded_for_the_request_and_the_code(
    monkeypatch, capsys, path, encoded
):
    base = SERVER + encoded
    daemon = _serve(monkeypatch, _routes(base=base))
    assert paircli.dispatch(_args(url=SERVER + path)) == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert json.loads(out)["url"] == base
    assert daemon.auth_for(base + "/whoami") == ["Bearer " + TOKEN]


# ---------------------------------------------------------------------------
# output encoding
# ---------------------------------------------------------------------------


def test_emit_writes_utf8_to_a_stream_in_another_encoding(monkeypatch):
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="cp1252", newline="")
    monkeypatch.setattr(sys, "stdout", stream)
    paircli._emit("▀▄█\n")
    assert raw.getvalue() == "▀▄█\n".encode("utf-8")


def test_emit_leaves_stdout_open_for_what_follows(monkeypatch):
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="cp1252", newline="")
    monkeypatch.setattr(sys, "stdout", stream)
    paircli._emit("▀\n")
    stream.write("after\n")
    stream.flush()
    assert raw.getvalue() == "▀\n".encode("utf-8") + b"after\n"


def test_emit_writes_to_a_stream_without_a_byte_layer(monkeypatch):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stream)
    paircli._emit("▀\n")
    assert stream.getvalue() == "▀\n"


def test_emit_flushes_so_that_a_warning_on_stderr_follows_it(monkeypatch):
    raw = io.BytesIO()
    stream = io.TextIOWrapper(io.BufferedWriter(raw), encoding="cp1252")
    monkeypatch.setattr(sys, "stdout", stream)
    paircli._emit("▀\n")
    assert raw.getvalue() == "▀\n".encode("utf-8")


def test_output_that_stdout_refuses_is_left_to_the_entry_point(monkeypatch):
    # cronstable.__main__ reports a closed stdout for every subcommand
    def refuse(*args):
        raise BrokenPipeError(32, "Broken pipe")

    _serve(monkeypatch, _routes())
    stream = io.StringIO()
    monkeypatch.setattr(stream, "flush", refuse, raising=False)
    monkeypatch.setattr(sys, "stdout", stream)
    with pytest.raises(BrokenPipeError):
        paircli.dispatch(_args(format="link"))
