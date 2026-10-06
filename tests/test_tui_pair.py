"""Tests for the terminal dashboard's Pair a device panel.

The panel is the web page's QR panel in a terminal: the same palette
entry, the same payload and link (:mod:`cronstable.pairlink`), and the
symbol drawn by :mod:`cronstable.qr`. The end-to-end tests boot the real
app against the fake daemon from ``tests/test_tui.py``; the layout tests
drive the renderer directly.
"""

import asyncio
import os
import re
import ssl
import threading
import urllib.error

import pytest

from cronstable import netutil, pairlink, pairprobe, qr, tui, webclient
from cronstable.tui import Api, ApiError, Unauthorized, strip_ansi
from tests.test_paircli import OTHER_STAMP, _Daemon, _guarded, _Reply
from tests.test_tui import Harness, _bare_app, _paint, _txt, _wait_for

PAGE = os.path.join(os.path.dirname(tui.__file__), "web", "index.html")

# The fake daemon adds its own instance ID. The listener stands for one on
# every address, which the address check requires before it asks the LAN
# address anything.
PHONE = {
    "authenticated": True,
    "label": "phone",
    "scopes": ["control", "view"],
    "allScopes": False,
    "pairLinkBase": "https://relay.example.test/pair",
    "sealableSuites": ["x25519"],
    "listeners": ["http://0.0.0.0:1"],
}
ADMIN = dict(PHONE, allScopes=True, scopes=["approve", "control", "view"])
# a credential-less connection under a view-only web.anonymousScopes grant
ANONYMOUS = dict(
    PHONE, authenticated=False, label="anonymous", scopes=["view"]
)
# what a loader test's stubbed GET /whoami answers
SESSION = dict(PHONE, instance="k3JxVq9d2mS0aB7cXw1zLQ")


def _code_rows(rows):
    """The symbol's cells out of painted panel rows."""
    out = []
    for row in rows:
        if qr.INK_ON_PAPER in row:
            tail = row.split(qr.INK_ON_PAPER, 1)[1]
            out.append(tail.split(tui.RESET, 1)[0])
    return out


def _ready(app, whoami=PHONE, name="nas", url="http://192.168.1.50:8080"):
    """Give ``app`` a built pairing code without a daemon."""
    text = pairlink.payload(name, url, "tok")
    link = pairlink.link(text, pairlink.link_base(whoami))
    app.pair = {
        "name": name,
        "url": url,
        "payload": text,
        "hint": pairlink.hint("tok"),
        "notes": pairlink.notes(whoami),
        "matrix": qr.encode(link.encode("utf-8"), "L"),
    }
    return app.pair["matrix"]


async def _open_from_palette(h):
    h.keys.send("ctrl+k")
    for ch in "pair a dev":
        h.keys.send(ch)
    h.keys.send("enter")
    await _wait_for(lambda: h.app.is_open("pair"))


# ---------------------------------------------------------------------------
# parity with the page
# ---------------------------------------------------------------------------


def test_palette_entry_is_the_pages():
    with open(PAGE, encoding="utf-8") as fh:
        page = fh.read()
    found = re.search(r'ic: "(.)", lbl: "(Pair a device[^"]*)"', page)
    assert found is not None
    # the row sits after "Set access token", as on the page
    labels = re.findall(r'lbl: "([^"]+)"', page)
    at = labels.index(found.group(2))
    assert labels[at - 1] == "Set access token"


def test_palette_lists_the_panel_after_the_token_row(tmp_path):
    app = _bare_app(tmp_path)
    rows = [(icon, label) for icon, label, _ in app.palette_commands()]
    at = rows.index(("▦", "Pair a device (QR)"))
    assert rows[at - 1][1] == "Set access token"


OVERLAYS = ["token", "settings", "help", "timeline", "drawer", "dags"]


@pytest.mark.parametrize("other", [*OVERLAYS, "palette"])
async def test_esc_closes_an_overlay_opened_over_the_panel(tmp_path, other):
    app = _bare_app(tmp_path)
    app.open("pair")
    _ready(app)
    app.open(other)
    assert app.open_overlays == ["pair", other]
    # Esc closes the overlay on screen, and the panel is back with its code
    await app.handle_key("esc")
    assert app.open_overlays == ["pair"]
    assert app.pair is not None
    # the next Esc closes the panel and drops the token-bearing state
    await app.handle_key("esc")
    assert app.open_overlays == []
    assert app.pair is None


@pytest.mark.parametrize("other", OVERLAYS)
async def test_esc_closes_the_panel_opened_over_another_overlay(
    tmp_path, other
):
    app = _bare_app(tmp_path)
    app.open(other)
    app.open("pair")
    _ready(app)
    assert app.top_overlay() == "pair"
    # the code leaves the screen with the first Esc
    await app.handle_key("esc")
    assert app.open_overlays == [other]
    assert app.pair is None


async def test_esc_closes_the_panel_opened_from_the_palette_over_help(
    tmp_path, monkeypatch
):
    h = Harness()
    h.daemon.whoami = dict(PHONE)
    monkeypatch.setattr(netutil, "lan_address", lambda: "localhost")
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: app.connected)
        h.keys.send("?")
        await _wait_for(lambda: app.is_open("help"))
        await _open_from_palette(h)
        await _wait_for(lambda: app.pair is not None)
        await h.settle()
        assert _code_rows(h.term.frames[-1])
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("pair"))
        await h.settle()
        # the code left the screen, and the shortcuts are back on it
        assert app.pair is None
        assert app.open_overlays == ["help"]
        assert _code_rows(h.term.frames[-1]) == []
    finally:
        await h.stop()


# ---------------------------------------------------------------------------
# end to end against the fake daemon
# ---------------------------------------------------------------------------


async def test_panel_draws_the_code_for_the_lan_address(tmp_path, monkeypatch):
    h = Harness()
    h.daemon.whoami = dict(PHONE)
    # the fake daemon listens on loopback only; "localhost" stands in for
    # the LAN address a real one would also answer at
    monkeypatch.setattr(netutil, "lan_address", lambda: "localhost")
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: app.connected)
        await _open_from_palette(h)
        await _wait_for(lambda: app.pair is not None)
        await h.settle()
        port = h.daemon.url.rsplit(":", 1)[1]
        url = "http://localhost:%s" % port
        assert app.pair["url"] == url
        assert app.pair["notes"] == []
        assert app.pair["payload"] == pairlink.payload(
            "localhost:%s" % port, url, None
        )
        # the caption says that the address stands in for the session's,
        # and claims no token for a code that carries none
        assert app.pair["lan_note"] == pairlink.lan_note(h.daemon.url, url)
        assert app.pair["hint"] == pairlink.hint(None)
        link = pairlink.link(app.pair["payload"], PHONE["pairLinkBase"])
        assert app.pair["matrix"] == qr.encode(link.encode("utf-8"), "L")
        screen = h.term.screen()
        assert "pair a device" in screen
        assert url in screen
        assert "c copy payload" in screen
        # the painted cells are the symbol, black on white on any theme
        painted = _code_rows(h.term.frames[-1])
        assert painted in [
            qr.half_block_rows(app.pair["matrix"], q) for q in (1, 2, 3, 4)
        ]
        # c is the page's Copy button
        h.keys.send("c")
        await _wait_for(lambda: bool(h.term.copied))
        assert h.term.copied[-1] == app.pair["payload"]
        # Esc closes the panel and drops the token-bearing state
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("pair"))
        assert app.pair is None
    finally:
        await h.stop()


async def test_panel_asks_for_the_token_on_401_then_rebuilds(
    tmp_path, monkeypatch
):
    h = Harness()
    h.daemon.token = "s3cr3t"
    h.daemon.whoami = dict(PHONE)
    monkeypatch.setattr(netutil, "lan_address", lambda: "localhost")
    try:
        app = await h.start(tmp_path, token="s3cr3t")
        await _wait_for(lambda: app.connected)
        app.api.token = "stale"
        await _open_from_palette(h)
        # the token modal opens over the panel, as on the page
        await _wait_for(lambda: app.top_overlay() == "token")
        assert app.is_open("pair") and "access token" in app.pair["error"]
        for ch in "s3cr3t":
            h.keys.send(ch)
        h.keys.send("enter")
        await _wait_for(lambda: "payload" in (app.pair or {}), timeout=8)
        assert '"token":"s3cr3t"' in app.pair["payload"]
        # the address check found the daemon by the instance ID on its
        # 401, token-less
        assert app.pair["url"].startswith("http://localhost:")
        assert app.pair["hint"] == pairlink.hint("s3cr3t")
    finally:
        await h.stop()


async def test_panel_explains_a_daemon_reachable_only_on_loopback(
    tmp_path, monkeypatch
):
    h = Harness()
    monkeypatch.setattr(netutil, "lan_address", lambda: None)
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: app.connected)
        await _open_from_palette(h)
        await _wait_for(lambda: app.pair is not None)
        await h.settle()
        assert "matrix" not in app.pair
        screen = h.term.screen()
        assert "loopback address" in screen
        assert "--public-url" in screen
        # nothing to copy
        h.keys.send("c")
        await h.settle()
        assert h.term.copied == []
    finally:
        await h.stop()


# ---------------------------------------------------------------------------
# the loader
# ---------------------------------------------------------------------------


def _stub_whoami(app, reply):
    seen = []

    async def get_json(path, **kwargs):
        seen.append(path)
        if isinstance(reply, Exception):
            raise reply
        return reply

    app.api.get_json = get_json
    return seen


async def test_loader_uses_a_reachable_session_address_as_is(
    tmp_path, monkeypatch
):
    app = _bare_app(tmp_path)
    app.api = Api("https://cron.example.net:8443/", "tok")
    app.cluster = {"enabled": True, "node_name": "node-a"}
    seen = _stub_whoami(app, ADMIN)
    monkeypatch.setattr(
        netutil, "lan_address", lambda: pytest.fail("no address check")
    )
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert seen == ["/whoami"]
    assert app.pair["url"] == "https://cron.example.net:8443"
    assert app.pair["name"] == "node-a"
    assert app.pair["payload"] == pairlink.payload(
        "node-a", "https://cron.example.net:8443", "tok"
    )
    assert [short for short, _ in app.pair["notes"]] == ["full-access token"]


@pytest.mark.parametrize(
    "lan_reply, reason",
    [
        # nothing listens there
        (None, "cannot reach the cronstable server at http://192.0.2.7:1"),
        (
            _Reply(401, {}, {"WWW-Authenticate": 'Basic realm="router"'}),
            "another server answers at this host's LAN address",
        ),
        # the daemon's challenge is public, so it identifies no daemon
        (
            _Reply(401, {}, {"WWW-Authenticate": 'Bearer realm="cronstable"'}),
            "another server answers at this host's LAN address",
        ),
        (_Reply(200, {"hello": "world"}), "/whoami returned HTTP 200"),
        (
            urllib.error.URLError(
                ssl.SSLCertVerificationError("IP address mismatch")
            ),
            "TLS verification failed",
        ),
    ],
)
async def test_loader_says_why_the_lan_address_does_not_serve(
    tmp_path, monkeypatch, lan_reply, reason
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    _stub_whoami(app, SESSION)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    lan = "http://192.0.2.7:1/whoami"
    daemon = _Daemon({} if lan_reply is None else {lan: lan_reply})
    monkeypatch.setattr(webclient, "OPENER", daemon)
    app.open("pair")
    await app._load_pair(app._pair_seq)
    # asked at the LAN address, which never got the token
    assert daemon.auth_for(lan) == [None]
    error = app.pair["error"]
    assert "is a loopback address, which a phone cannot reach" in error
    assert reason in error
    assert "cronstable pair --public-url" in error
    # the command-line advice of cronstable pair has no place in the panel
    assert "--insecure" not in error and "--cacert" not in error
    tls = reason == "TLS verification failed"
    assert ("an address that the listener's certificate names" in error) == tls
    assert ("the server's LAN, VPN, or public address" in error) != tls


async def test_loader_takes_the_lan_address_of_the_same_daemon(
    tmp_path, monkeypatch
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    _stub_whoami(app, SESSION)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    lan = "http://192.0.2.7:1/whoami"
    daemon = _Daemon({lan: _guarded(SESSION, "tok")})
    monkeypatch.setattr(webclient, "OPENER", daemon)
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.pair["url"] == "http://192.0.2.7:1"
    assert '"token":"tok"' in app.pair["payload"]
    # asked once, and the session's token stayed with the session's address
    assert daemon.auth_for(lan) == [None]


@pytest.mark.parametrize(
    "lan_route",
    [
        # another daemon that requires a token
        _Reply(401, {}, OTHER_STAMP),
        # another daemon that holds the token and describes it the same way
        _guarded(dict(SESSION, instance="Zz9yXw8vUt7sRq6pOn5mLk"), "tok"),
    ],
)
async def test_loader_refuses_a_different_daemon_on_the_lan_address(
    tmp_path, monkeypatch, lan_route
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    _stub_whoami(app, SESSION)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    lan = "http://192.0.2.7:1/whoami"
    daemon = _Daemon({lan: lan_route})
    monkeypatch.setattr(webclient, "OPENER", daemon)
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert "matrix" not in app.pair
    assert "a different cronstable server answers" in app.pair["error"]
    assert "cronstable pair --public-url" in app.pair["error"]
    assert daemon.auth_for(lan) == [None]


@pytest.mark.parametrize(
    "listeners",
    [["http://127.0.0.1:1"], ["https://0.0.0.0:8443"], ["http://[::]:1"]],
)
async def test_loader_asks_nothing_of_an_address_the_daemon_does_not_serve(
    tmp_path, monkeypatch, listeners
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    _stub_whoami(app, dict(SESSION, listeners=listeners))
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    monkeypatch.setattr(
        webclient, "build_opener", lambda ctx: pytest.fail("no request")
    )
    app.open("pair")
    await app._load_pair(app._pair_seq)
    error = app.pair["error"]
    assert "is a loopback address, which a phone cannot reach" in error
    assert (
        "the server reports no http listener on this host's LAN address "
        "(192.0.2.7)" in error
    )
    assert "cronstable pair --public-url" in error


async def test_loader_asks_an_anonymous_session_for_a_token(
    tmp_path, monkeypatch
):
    # web.anonymousScopes: the reply to a session without a token names no
    # listeners, so the panel has no address to check
    app = _bare_app(tmp_path)
    anonymous = {k: v for k, v in ANONYMOUS.items() if k != "listeners"}
    _stub_whoami(app, dict(anonymous, instance=SESSION["instance"]))
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    monkeypatch.setattr(
        webclient, "build_opener", lambda ctx: pytest.fail("no request")
    )
    app.open("pair")
    await app._load_pair(app._pair_seq)
    error = app.pair["error"]
    assert (
        "the server names its listeners only to a connection that presents "
        "an access token. Select Set access token in the command palette, "
        "or run cronstable pair --public-url URL." in error
    )
    # the daemon reported no listener, and the panel claims none
    assert "reports no http listener" not in error
    assert "--url set to" not in error


async def test_loader_takes_the_address_of_a_listener_on_another_interface(
    tmp_path, monkeypatch
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    vpn = "http://10.8.0.1:1"
    whoami = dict(SESSION, listeners=["http://127.0.0.1:1", vpn])
    _stub_whoami(app, whoami)
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    daemon = _Daemon({vpn + "/whoami": _guarded(whoami, "tok")})
    monkeypatch.setattr(webclient, "OPENER", daemon)
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.pair["url"] == vpn
    assert app.pair["lan_note"] == pairlink.lan_note("http://127.0.0.1:1", vpn)
    # asked once, without the session's token
    assert [req.full_url for req in daemon.requests] == [vpn + "/whoami"]
    assert daemon.auth_for(vpn + "/whoami") == [None]


async def test_loader_does_not_vouch_for_the_lan_address_unverified(
    tmp_path, monkeypatch
):
    insecure = ssl.create_default_context()
    insecure.check_hostname = False
    insecure.verify_mode = ssl.CERT_NONE
    app = _bare_app(tmp_path)
    app.api = Api("https://127.0.0.1:8443", "tok", insecure)
    _stub_whoami(app, dict(SESSION, listeners=["https://0.0.0.0:8443"]))
    monkeypatch.setattr(netutil, "lan_address", lambda: "192.0.2.7")
    monkeypatch.setattr(
        webclient, "build_opener", lambda ctx: pytest.fail("no request")
    )
    app.open("pair")
    await app._load_pair(app._pair_seq)
    error = app.pair["error"]
    assert "this connection skips certificate verification" in error
    assert "an address that the listener's certificate names" in error


async def test_address_check_runs_on_a_thread_that_exit_leaves_behind(
    tmp_path, monkeypatch
):
    app = _bare_app(tmp_path)
    app.api.token = "tok"
    _stub_whoami(app, PHONE)
    seen = []

    def phone_url(base, whoami, context):
        seen.append((threading.current_thread(), base, whoami, context))
        return "http://192.0.2.7:1"

    monkeypatch.setattr(pairprobe, "phone_url", phone_url)
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.pair["url"] == "http://192.0.2.7:1"
    [(thread, base, whoami, context)] = seen
    # a daemon thread: neither the loop's shutdown nor the interpreter's
    # exit joins it
    assert thread.daemon and thread is not threading.main_thread()
    # the check takes no token: it presents none
    assert (base, whoami) == ("http://127.0.0.1:1", PHONE)
    assert context is app.api.ssl_context


async def test_loader_leaves_out_a_token_the_daemon_did_not_authenticate(
    tmp_path,
):
    app = _bare_app(tmp_path)
    app.api = Api("https://cron.example.net", "for-another-server")
    _stub_whoami(app, dict(PHONE, authenticated=False, allScopes=True))
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert '"token":""' in app.pair["payload"]
    assert [short for short, _ in app.pair["notes"]] == ["no access token"]


async def test_loader_explains_a_401_under_the_token_modal(tmp_path):
    app = _bare_app(tmp_path)
    _stub_whoami(app, Unauthorized())
    # the modal is open already, beneath the panel
    app.open("token")
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.top_overlay() == "token" and app.focus == "token"
    # dismissed, it leaves the panel saying how to go on
    await app.handle_key("esc")
    assert app.top_overlay() == "pair"
    rows = app.render_overlay(_paint(app), "pair", 100, 30)
    text = " ".join(_txt(rows).replace("│", " ").split())
    assert "Select Set access token in the command palette" in text
    assert "Building the pairing code" not in text


async def test_loader_reports_a_failed_request_in_the_panel(tmp_path):
    app = _bare_app(tmp_path)
    _stub_whoami(app, ApiError(503))
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.pair == {"error": "Could not build the pairing code: HTTP 503"}
    rows = app.render_overlay(_paint(app), "pair", 100, 30)
    assert "Could not build the pairing code" in _txt(rows)
    assert "c copy payload" not in _txt(rows)


async def test_loader_names_a_failure_that_has_no_message(tmp_path):
    app = _bare_app(tmp_path)
    # what aiohttp raises when the daemon stalls past the request timeout
    _stub_whoami(app, TimeoutError())
    app.open("pair")
    await app._load_pair(app._pair_seq)
    assert app.pair == {
        "error": "Could not build the pairing code: TimeoutError"
    }


async def test_loader_drops_an_answer_for_a_closed_panel(tmp_path):
    app = _bare_app(tmp_path)
    app.api = Api("http://cron.example.test:8080", "tok")
    _stub_whoami(app, PHONE)
    app.open("pair")
    stale = app._pair_seq
    app.close("pair")
    await app._load_pair(stale)
    assert app.pair is None
    # and a 401 for a closed panel opens no token modal
    _stub_whoami(app, Unauthorized())
    await app._load_pair(stale)
    assert not app.is_open("token")


async def test_token_commit_rebuilds_an_open_panel(tmp_path):
    app = _bare_app(tmp_path)
    app.api = Api("http://cron.example.test:8080", "old")
    _stub_whoami(app, PHONE)
    app.refresh_now = lambda: None
    app._open_pair()
    await _wait_for(lambda: app.pair is not None)
    assert '"token":"old"' in app.pair["payload"]
    app.open("token")
    app.inputs["token"] = "new"
    await app._input_commit("token")
    await _wait_for(
        lambda: app.pair is not None and '"token":"new"' in app.pair["payload"]
    )


async def test_token_commit_leaves_an_overlay_over_the_panel_on_top(tmp_path):
    app = _bare_app(tmp_path)
    app.api = Api("http://cron.example.test:8080", "old")
    _stub_whoami(app, PHONE)
    app.refresh_now = lambda: None
    app._open_pair()
    await _wait_for(lambda: app.pair is not None)
    # a drawer opened over the panel, and then the token modal over both
    app.open("drawer")
    app.open("token")
    app.inputs["token"] = "new"
    await app._input_commit("token")
    # the panel rebuilds its code beneath the drawer
    assert app.open_overlays == ["pair", "drawer"]
    await _wait_for(
        lambda: app.pair is not None and '"token":"new"' in app.pair["payload"]
    )


# ---------------------------------------------------------------------------
# layout
# ---------------------------------------------------------------------------


def test_panel_says_it_is_building_before_the_code_arrives(tmp_path):
    app = _bare_app(tmp_path)
    app.open("pair")
    text = _txt(app.render_overlay(_paint(app), "pair", 100, 30))
    assert "Building the pairing code" in text
    assert "esc close" in text


def test_roomy_panel_shows_the_code_its_caption_and_the_notes(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app, ADMIN)
    rows = app.render_overlay(_paint(app), "pair", 120, 60)
    text = _txt(rows)
    flat = " ".join(text.replace("│", " ").split())
    assert _code_rows(rows) == qr.half_block_rows(matrix, 4)
    assert "nas (http://192.168.1.50:8080)" in text
    # the caption is the one that cronstable pair prints
    assert " ".join(pairlink.hint("tok")) in flat
    assert "in place of" not in flat
    assert "This token grants full access." in text
    assert "c copy payload · esc close" in text
    assert "⚠" not in text
    # every row of the panel is one width, so the frame closes
    assert len({tui.text_width(row) for row in rows}) == 1


def test_caption_follows_what_the_code_carries(tmp_path):
    app = _bare_app(tmp_path)
    _ready(app, ANONYMOUS)
    # a code without a token, for an address that stands in for loopback
    app.pair["hint"] = pairlink.hint(None)
    app.pair["lan_note"] = pairlink.lan_note(
        "http://127.0.0.1:8080", "http://192.168.1.50:8080"
    )
    rows = app.render_overlay(_paint(app), "pair", 120, 60)
    flat = " ".join(_txt(rows).replace("│", " ").split())
    assert "Scan the code with the phone's camera" in flat
    assert "The code contains the access token" not in flat
    assert app.pair["lan_note"] in flat
    assert len({tui.text_width(row) for row in rows}) == 1


def test_panel_keeps_the_symbol_black_on_white_on_every_theme(tmp_path):
    app = _bare_app(tmp_path)
    _ready(app)
    for hue in tui.THEME_HUES:
        for light in (False, True):
            app.theme = tui.Theme(hue, light)
            rows = app.render_overlay(_paint(app), "pair", 120, 60)
            assert sum(qr.INK_ON_PAPER in row for row in rows) > 20


def test_short_window_gives_the_caption_rows_to_the_code(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app, ADMIN)
    need_cols, need_lines = qr.smallest_fit(matrix)
    rows = app.render_overlay(_paint(app), "pair", 120, need_lines + 4)
    text = _txt(rows)
    assert _code_rows(rows) == qr.half_block_rows(matrix, 1)
    assert len(rows) == need_lines + 4
    assert "trusted network" not in text
    # the note survives in its short form on the hint row
    assert "⚠ full-access token · c copy payload · esc close" in text


def test_short_narrow_window_cuts_the_notes_and_keeps_the_keys(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app, ANONYMOUS)
    need_cols, need_lines = qr.smallest_fit(matrix)
    # the smallest window that shows the code: two notes do not fit beside
    # the keys on its hint row
    rows = app.render_overlay(
        _paint(app), "pair", need_cols + 8, need_lines + 4
    )
    text = _txt(rows)
    assert _code_rows(rows) == qr.half_block_rows(matrix, 1)
    assert "… · c copy payload · esc close" in text
    assert "⚠ no access" in text
    assert "no control scope" not in text
    assert len({tui.text_width(row) for row in rows}) == 1
    assert tui.text_width(rows[0]) == need_cols + 4


def test_short_wide_window_widens_the_panel_for_the_notes(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app, ANONYMOUS)
    _cols, need_lines = qr.smallest_fit(matrix)
    rows = app.render_overlay(_paint(app), "pair", 120, need_lines + 4)
    text = _txt(rows)
    footer = (
        "⚠ no access token · no control scope · c copy payload · esc close"
    )
    assert footer in text
    assert "…" not in text
    assert _code_rows(rows) == qr.half_block_rows(matrix, 1)
    assert len({tui.text_width(row) for row in rows}) == 1
    assert tui.text_width(rows[0]) == tui.text_width(footer) + 4
    # the code stays centered in the wider panel
    row = next(strip_ansi(row) for row in rows if qr.INK_ON_PAPER in row)
    inside = row[1:-1]
    left = len(inside) - len(inside.lstrip())
    right = len(inside) - len(inside.rstrip())
    assert abs(left - right) <= 1


def test_hint_row_with_no_room_for_a_note_shows_the_keys_alone(tmp_path):
    app = _bare_app(tmp_path)
    _ready(app, ADMIN)
    # a symbol far smaller than any pairing link's
    app.pair["matrix"] = qr.encode(b"x", "L")
    need_cols, need_lines = qr.smallest_fit(app.pair["matrix"])
    rows = app.render_overlay(
        _paint(app), "pair", need_cols + 8, need_lines + 4
    )
    text = _txt(rows)
    assert "⚠" not in text and "full-access" not in text
    assert "c copy payload" in text
    assert len({tui.text_width(row) for row in rows}) == 1


def test_layout_is_computed_once_for_a_window_size(tmp_path, monkeypatch):
    app = _bare_app(tmp_path)
    _ready(app, ADMIN)
    calls = []
    real = tui.pair_layout

    def counted(pair, cols, lines):
        calls.append((cols, lines))
        return real(pair, cols, lines)

    monkeypatch.setattr(tui, "pair_layout", counted)
    first = app.render_overlay(_paint(app), "pair", 120, 60)
    assert app.render_overlay(_paint(app), "pair", 120, 60) == first
    # another theme repaints the same layout
    app.theme = tui.Theme(tui.THEME_HUES[-1], True)
    assert app.render_overlay(_paint(app), "pair", 120, 60) != first
    assert calls == [(120, 60)]
    # a resized window and a rebuilt code each lay out again
    app.render_overlay(_paint(app), "pair", 100, 60)
    _ready(app, ADMIN)
    app.render_overlay(_paint(app), "pair", 100, 60)
    assert calls == [(120, 60), (100, 60), (100, 60)]
    # closing the panel drops the layout with the code
    app.open("pair")
    app.close("pair")
    assert app.pair is None


def test_short_window_without_notes_keeps_the_plain_hint_row(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app, PHONE)
    _cols, need_lines = qr.smallest_fit(matrix)
    text = _txt(app.render_overlay(_paint(app), "pair", 120, need_lines + 4))
    assert "⚠" not in text
    assert "c copy payload · esc close" in text


def test_window_too_small_for_the_code_says_what_it_needs(tmp_path):
    app = _bare_app(tmp_path)
    matrix = _ready(app)
    need_cols, need_lines = qr.smallest_fit(matrix)
    for cols, lines in ((80, 24), (need_cols + 7, 60), (120, need_lines + 3)):
        rows = app.render_overlay(_paint(app), "pair", cols, lines)
        text = " ".join(_txt(rows).replace("│", " ").split())
        assert _code_rows(rows) == []
        assert (
            "at least %d columns by %d lines" % (need_cols + 8, need_lines + 4)
            in text
        )
        assert "this one is %d by %d" % (cols, lines) in text
        assert "c copy payload" not in text


def test_narrow_window_still_renders_a_panel(tmp_path):
    app = _bare_app(tmp_path)
    _ready(app)
    # narrower than any wrap width: no exception, no symbol
    rows = app.render_overlay(_paint(app), "pair", 12, 40)
    assert rows and _code_rows(rows) == []
    assert "pair" in strip_ansi(rows[0])


async def test_copy_key_is_inert_without_a_code(tmp_path):
    app = _bare_app(tmp_path)
    app.open("pair")
    await app.handle_key("c")
    app.pair = {"error": "nope"}
    await app.handle_key("c")
    assert app.term.copied == []
    _ready(app)
    await app.handle_key("c")
    assert app.term.copied == [app.pair["payload"]]


# ---------------------------------------------------------------------------
# blocking work off the loop
# ---------------------------------------------------------------------------


async def test_in_daemon_thread_returns_the_result_or_raises():
    assert await tui.in_daemon_thread(lambda a, b: a + b, 2, 3) == 5

    def fail():
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        await tui.in_daemon_thread(fail)


async def test_in_daemon_thread_returns_at_once_when_cancelled():
    started, release = threading.Event(), threading.Event()

    def stall():
        started.set()
        release.wait(30)

    task = asyncio.ensure_future(tui.in_daemon_thread(stall))
    await _wait_for(started.is_set)
    task.cancel()
    # the stalled call is still running, and the cancellation does not wait
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert not release.is_set()
    # its late answer finds the caller gone and is dropped
    release.set()
    await _wait_for(
        lambda: (
            not any(t.name == "tui-blocking" for t in threading.enumerate())
        )
    )
