"""Reach's daemon runtime (cronstable.reach.ReachService), end to end.

An in-process fake relay (one aiohttp WebSocket route) plays the relay's
half of docs/reach-protocol.md: it challenges, verifies the daemon's
``hello`` with ``reach.verify_challenge``, answers ``welcome``, and then
lets each test drive OPEN/CANCEL/WINDOW frames and collect the
DATA/END/REJECT frames the daemon sends back.  The app side is
``reach.AppSession``, the reference implementation the shared vectors
are written against.  Also here: the fail-closed config validation, the
cron convergence (start_stop_reach), the /whoami shapes, the dashboard's
pairing payload, the ``cronstable reach`` CLI, and the import-cost
invariant.

PyNaCl (the push extra) is a dev dependency on every CI cell, and the
whole file needs it: the identity itself is an Ed25519 key.
"""

import asyncio
import base64
import json
import logging
import os
import secrets
import stat
import struct
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any, Optional

import pytest
from aiohttp import web

import cronstable.__main__ as main
import cronstable.config as config_mod
import cronstable.platform as platform_mod
import cronstable.version
from cronstable import reach
from cronstable.config import ConfigError, parse_config, parse_config_string
from cronstable.cron import (
    WEB_ANON_REQUEST_KEY,
    WEB_TOKEN_REQUEST_KEY,
    _WebToken,
)
from cronstable.job import JobOutputStream
from tests._configs import _ETCD, job_yaml
from tests.conftest import _cron

pytest.importorskip("nacl", reason="pynacl (the push extra) is not installed")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB_YAML = job_yaml("alpha", "echo alpha")
WEB = {"listen": ["http://127.0.0.1:0"], "authToken": {"value": "secret"}}
# the authority the app would have dialed directly; the daemon sets it
# as the inner request's Host, so the origin gate sees it
AUTHORITY = "nas.local:8080"
# the reconnect schedule every runtime test runs under: no jitter, so
# the timing assertions below have exact floors
FAST = reach.Backoff(
    base=0.02,
    cap=0.05,
    healthy_after=60.0,
    park=0.3,
    superseded_wait=0.5,
    jitter=False,
)
REACH_YAML = """
web:
  listen:
    - http://127.0.0.1:0
  authToken:
    value: secret
  reach:
    relay: https://relay.example
    keyFile: /var/lib/cronstable/reach.json
"""


# ---------------------------------------------------------------- helpers


async def _until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.01)


class _FakeRelay:
    """An in-process relay: the tunnel route runs the relay's half of the
    handshake and then queues every frame the daemon sends, per reqId,
    for the test to read back.

    ``relay_host`` overrides the host the challenge claims (the default
    is the host the daemon dialed).  ``upgrade`` answers the upgrade with
    a plain HTTP response instead of a socket.  ``hello_closes`` is a
    list of close codes to send right after ``welcome``, one per
    connection, for the reconnect tests.
    """

    def __init__(
        self,
        *,
        initial_window: int = reach.DEFAULT_INITIAL_WINDOW,
        max_chunk: int = reach.MAX_CHUNK,
        relay_host: Optional[str] = None,
        upgrade: Optional[tuple[int, dict[str, str]]] = None,
        hello_closes: tuple[int, ...] = (),
    ) -> None:
        self.initial_window = initial_window
        self.max_chunk = max_chunk
        self.relay_host = relay_host
        self.upgrade = upgrade
        self.hello_closes = list(hello_closes)
        self.origin = ""
        self.upgrades: list[tuple[str, dict[str, str], float]] = []
        self.hellos: list[dict[str, Any]] = []
        self.hello_times: list[float] = []
        self.texts: list[dict[str, Any]] = []
        self.closes: list[Optional[int]] = []
        self.ws: Any = None
        self.connected = asyncio.Event()
        self._frames: dict[int, asyncio.Queue] = {}
        self._runner: Optional[web.AppRunner] = None

    async def __aenter__(self) -> "_FakeRelay":
        app = web.Application()
        app.router.add_get("/v1/tunnel/{tid}", self._tunnel)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.origin = "http://127.0.0.1:{}".format(
            self._runner.addresses[0][1]
        )
        return self

    async def __aexit__(self, *exc) -> None:
        assert self._runner is not None
        # close the live socket first, so the runner's shutdown does not
        # wait its handler timeout out on the tunnel handler
        if self.ws is not None:
            await self.ws.close(code=1001, message=b"relay gone")
        await self._runner.cleanup()

    async def _tunnel(self, request: web.Request) -> Any:
        self.upgrades.append(
            (
                request.match_info["tid"],
                dict(request.headers),
                time.monotonic(),
            )
        )
        if self.upgrade is not None:
            status, headers = self.upgrade
            return web.Response(status=status, headers=headers, text="no")
        ws = web.WebSocketResponse(protocols=(reach.SUBPROTOCOL,))
        await ws.prepare(request)
        nonce = secrets.token_bytes(32)
        relay = (
            self.relay_host if self.relay_host is not None else request.host
        )
        await ws.send_str(
            json.dumps(
                {
                    "v": 1,
                    "type": "challenge",
                    "nonce": reach.b64_encode(nonce),
                    "relay": relay,
                }
            )
        )
        msg = await ws.receive()
        if msg.type != web.WSMsgType.TEXT:
            self.closes.append(ws.close_code)
            return ws
        hello = json.loads(msg.data)
        tid = reach.tunnel_id_bytes(request.match_info["tid"])
        if hello.get("type") != "hello" or not reach.verify_challenge(
            tid, relay, nonce, reach.b64_decode(hello["sig"], 64)
        ):
            await ws.close(code=reach.CLOSE_BAD_SIGNATURE)
            self.closes.append(reach.CLOSE_BAD_SIGNATURE)
            return ws
        self.hellos.append(hello)
        self.hello_times.append(time.monotonic())
        await ws.send_str(
            json.dumps(
                {
                    "v": 1,
                    "type": "welcome",
                    "limits": {
                        "maxBody": reach.MAX_BODY,
                        "maxChunk": self.max_chunk,
                        "maxStreamS": 1800,
                        "maxInflight": 32,
                        "initialWindow": self.initial_window,
                    },
                }
            )
        )
        if self.hello_closes:
            code = self.hello_closes.pop(0)
            await ws.close(code=code, message=b"superseded")
            self.closes.append(code)
            return ws
        self.ws = ws
        self.connected.set()
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.BINARY:
                    ftype, req_id, payload = reach.unpack_frame(msg.data)
                    self._queue(req_id).put_nowait((ftype, payload))
                elif msg.type == web.WSMsgType.TEXT:
                    self.texts.append(json.loads(msg.data))
        finally:
            self.connected.clear()
            self.closes.append(ws.close_code)
            if self.ws is ws:
                self.ws = None
        return ws

    def _queue(self, req_id: int) -> asyncio.Queue:
        return self._frames.setdefault(req_id, asyncio.Queue())

    async def wait_connected(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.connected.wait(), timeout)

    async def open(self, req_id: int, body: bytes) -> None:
        await self.ws.send_bytes(
            reach.pack_frame(reach.FRAME_OPEN, req_id, body)
        )

    async def frame(
        self, req_id: int, timeout: float = 5.0
    ) -> tuple[int, bytes]:
        return await asyncio.wait_for(self._queue(req_id).get(), timeout)

    async def response(
        self, req_id: int, timeout: float = 5.0
    ) -> tuple[list[bytes], Optional[tuple[int, str]]]:
        """Every chunk of one response, plus the REJECT that ended it."""
        reader = reach.ChunkReader()
        chunks: list[bytes] = []
        while True:
            ftype, payload = await self.frame(req_id, timeout)
            if ftype == reach.FRAME_DATA:
                chunks.extend(reader.feed(payload))
            elif ftype == reach.FRAME_END:
                assert reader.pending == 0
                return chunks, None
            elif ftype == reach.FRAME_REJECT:
                assert chunks == [], "REJECT after DATA"
                (code,) = struct.unpack(">H", payload[:2])
                return chunks, (code, payload[2:].decode("utf-8"))
            else:
                raise AssertionError("unexpected frame type {}".format(ftype))


class _Reply:
    """One opened response, as the app sees it."""

    def __init__(
        self,
        chunks: list[bytes],
        reject: Optional[tuple[int, str]],
        app: reach.AppSession,
    ) -> None:
        self.reject = reject
        self.nosession = any(c == reach.NOSESSION_CHUNK for c in chunks)
        self.records = [
            app.open_record(c) for c in chunks if c != reach.NOSESSION_CHUNK
        ]
        self.kinds = [r[2] for r in self.records]
        heads = [r for r in self.records if r[2] == reach.KIND_HEAD]
        self.status: Optional[int] = None
        self.headers: list[tuple[str, str]] = []
        if heads:
            self.status, self.headers = reach.decode_head_record(heads[0][4])
        self.body = b"".join(
            r[4] for r in self.records if r[2] == reach.KIND_BODY
        )
        errors = [r for r in self.records if r[2] == reach.KIND_ERROR]
        self.error = json.loads(errors[0][4]) if errors else None


def _head(
    method: str,
    path: str,
    token: Optional[str] = "secret",
    extra: tuple[tuple[str, str], ...] = (),
) -> dict[str, Any]:
    headers: list[list[str]] = [["accept-encoding", "identity"]]
    if token is not None:
        headers.insert(0, ["authorization", "Bearer " + token])
    headers.extend([k, v] for k, v in extra)
    return {"v": 1, "m": method, "u": path, "a": AUTHORITY, "h": headers}


async def _connected(relay: _FakeRelay, service: reach.ReachService) -> None:
    """Both sides agree the tunnel is up: the relay sent ``welcome`` and
    the daemon applied it (the relay's event fires first)."""
    await relay.wait_connected()
    await _until(lambda: service.state == "connected")


async def _handshake(
    relay: _FakeRelay, service: reach.ReachService, req_id: int
) -> reach.AppSession:
    assert service.identity is not None
    app = reach.AppSession(
        service.identity.tunnel_id_bytes, service.identity.dh_public
    )
    await relay.open(req_id, app.begin())
    chunks, reject = await relay.response(req_id)
    assert reject is None and len(chunks) == 1
    app.finish(chunks[0])
    return app


async def _request(
    relay: _FakeRelay,
    app: reach.AppSession,
    req_id: int,
    head: dict[str, Any],
    body: bytes = b"",
) -> _Reply:
    await relay.open(req_id, app.seal_request(head, body))
    chunks, reject = await relay.response(req_id)
    return _Reply(chunks, reject, app)


class _Req:
    """The slice of aiohttp's Request the /whoami handler reads."""

    def __init__(self, token=None, anon=None):
        self._store: dict[str, Any] = {}
        if token is not None:
            self._store[WEB_TOKEN_REQUEST_KEY] = token
        if anon is not None:
            self._store[WEB_ANON_REQUEST_KEY] = anon

    def get(self, key, default=None):
        return self._store.get(key, default)


@pytest.fixture
async def daemon(tmp_path):
    """A cron with its real web app up, plus ``attach``: a ReachService
    bound onto that runner under the fast backoff.  Everything is stopped
    on teardown, services before web apps."""
    crons: list[Any] = []
    services: list[reach.ReachService] = []

    async def boot(web=None, yaml=JOB_YAML):
        cron = _cron(yaml)
        await cron.start_stop_web_app(dict(web or WEB))
        assert cron.web_runner is not None
        crons.append(cron)
        return cron

    async def attach(cron, relay, **kw):
        kw.setdefault("identity_path", str(tmp_path / "reach.json"))
        kw.setdefault("backoff", FAST)
        service = reach.ReachService(
            relay=relay,
            heartbeat=30.0,
            node="nas",
            agent="cronstable/test reach",
            bearers=cron._reach_bearers,
            runner=cron.web_runner,
            **kw,
        )
        services.append(service)
        await service.start()
        return service

    yield SimpleNamespace(
        boot=boot, attach=attach, path=str(tmp_path / "reach.json")
    )
    for service in reversed(services):
        await service.stop()
    for cron in reversed(crons):
        await cron.start_stop_web_app(None)
    await asyncio.sleep(0.05)


# ------------------------------------------------------ config validation


def _validated(yaml: str):
    config = parse_config_string(yaml + JOB_YAML, "")
    config_mod._validate_reach_config(config)
    return config


def test_reach_requires_a_bearer_token():
    yaml = REACH_YAML.replace("  authToken:\n    value: secret\n", "")
    with pytest.raises(ConfigError, match="at least one bearer token"):
        _validated(yaml)
    # push.allowUnauthenticated is the push endpoints' escape hatch; it
    # does not open the tunnel
    yaml += (
        "push:\n"
        "  relay:\n"
        "    url: https://relay.example/v1/notify\n"
        "  devicesFile: /tmp/devices.json\n"
        "  allowUnauthenticated: true\n"
    )
    with pytest.raises(ConfigError, match="at least one bearer token"):
        _validated(yaml)


def test_reach_refuses_client_ca_as_the_only_caller_authentication():
    yaml = (
        "web:\n"
        "  listen:\n"
        "    - https://0.0.0.0:8443\n"
        "  tls:\n"
        "    cert: /etc/cronstable/cert.pem\n"
        "    key: /etc/cronstable/key.pem\n"
        "    clientCa: /etc/cronstable/clients.pem\n"
        "  reach:\n"
        "    relay: https://relay.example\n"
        "    keyFile: /var/lib/cronstable/reach.json\n"
    )
    with pytest.raises(ConfigError, match="clientCa authenticates direct"):
        _validated(yaml)


def test_reach_requires_pynacl(monkeypatch):
    monkeypatch.setattr("cronstable.push.HAVE_PYNACL", False)
    with pytest.raises(ConfigError, match=r"cronstable\[push\]"):
        _validated(REACH_YAML)


def test_reach_refuses_a_cluster_section():
    with pytest.raises(ConfigError, match="Remote-Access"):
        _validated(REACH_YAML + _ETCD)


def test_reach_relay_url_rules():
    refused = {
        "http://relay.example": "must be an https URL",
        "relay.example": "must name a host",
        "https://relay.example/v1": "scheme and host only",
        "https://relay.example/?x=1": "scheme and host only",
        "https://relay.example/#frag": "scheme and host only",
        "https://relay.example:99999": "invalid port",
        "https://": "must name a host",
    }
    for url, message in refused.items():
        with pytest.raises(ConfigError, match=message):
            _validated(REACH_YAML.replace("https://relay.example", url))
    accepted = {
        "https://relay.example": "https://relay.example",
        "https://relay.example/": "https://relay.example",
        "https://Relay.Example:8443": "https://Relay.Example:8443",
        # http is accepted for a loopback host only (wrangler dev)
        "http://127.0.0.1:8787": "http://127.0.0.1:8787",
        "http://localhost:8787": "http://localhost:8787",
        "http://[::1]:8787": "http://[::1]:8787",
        # userinfo stays for the dial (Basic auth on the upgrade)
        "https://op:pw@relay.example": "https://op:pw@relay.example",
    }
    for url, origin in accepted.items():
        config = _validated(REACH_YAML.replace("https://relay.example", url))
        assert config_mod.resolve_reach_config(config) == {
            "relay": origin,
            "keyFile": "/var/lib/cronstable/reach.json",
            "heartbeat": 30.0,
        }
    # a refused URL is echoed with its credentials redacted
    with pytest.raises(ConfigError) as exc:
        _validated(
            REACH_YAML.replace(
                "https://relay.example", "https://op:s3cretpw@relay.example/v1"
            )
        )
    assert "s3cretpw" not in str(exc.value) and "***@" in str(exc.value)


def test_reach_relay_falls_back_to_the_push_relay():
    yaml = REACH_YAML.replace("    relay: https://relay.example\n", "")
    with pytest.raises(ConfigError, match="no push.relay.url"):
        _validated(yaml)
    push = (
        "push:\n"
        "  relay:\n"
        "    url: https://op:pw@relay.example:8443/v1/notify\n"
        "  devicesFile: /tmp/devices.json\n"
    )
    config = _validated(yaml + push)
    assert config_mod.resolve_reach_config(config)["relay"] == (
        "https://op:pw@relay.example:8443"
    )
    # the derived origin obeys the same rules as an explicit one
    with pytest.raises(ConfigError, match="must be an https URL"):
        _validated(
            yaml
            + push.replace(
                "https://op:pw@relay.example:8443", "http://relay.example"
            )
        )
    # an empty relay reads as absent
    assert (
        config_mod.resolve_reach_config(
            _validated(
                REACH_YAML.replace("https://relay.example", '""') + push
            )
        )["relay"]
        == "https://op:pw@relay.example:8443"
    )


def test_reach_heartbeat_range_and_key_file():
    for value in ("4", "301", "nan"):
        with pytest.raises(ConfigError, match="heartbeat must be between"):
            _validated(REACH_YAML + "    heartbeat: {}\n".format(value))
    config = _validated(REACH_YAML + "    heartbeat: 5\n")
    assert config_mod.resolve_reach_config(config)["heartbeat"] == 5.0
    with pytest.raises(ConfigError, match="keyFile must name"):
        _validated(
            REACH_YAML.replace(
                "keyFile: /var/lib/cronstable/reach.json", 'keyFile: "  "'
            )
        )


def test_reach_map_rides_the_web_config_and_the_top_level_parse(tmp_path):
    config = _validated(REACH_YAML + "    heartbeat: 45\n")
    # the raw map stays in web_config, so a change to it trips the web
    # app's restart gate (config inequality) like every other web key
    assert config.web_config["reach"] == {
        "relay": "https://relay.example",
        "keyFile": "/var/lib/cronstable/reach.json",
        "heartbeat": 45.0,
    }
    assert (
        config_mod.resolve_reach_config(parse_config_string(JOB_YAML, ""))
        is None
    )
    # the validator is wired into the top-level parse
    path = tmp_path / "cronstable.yaml"
    path.write_text(
        REACH_YAML.replace("  authToken:\n    value: secret\n", "") + JOB_YAML
    )
    with pytest.raises(ConfigError, match="at least one bearer token"):
        parse_config(str(path))


def test_authority_normalisation_treats_default_ports_as_absent():
    assert reach.dialed_authority("https://Relay.Example") == "relay.example"
    assert reach.dialed_authority("https://relay.example:443") == (
        "relay.example"
    )
    assert reach.claimed_authority("relay.example:443") == "relay.example"
    assert reach.claimed_authority("relay.example") == "relay.example"
    assert reach.dialed_authority("http://127.0.0.1:8787") == (
        reach.claimed_authority("127.0.0.1:8787")
    )
    assert reach.dialed_authority("http://[::1]:8787") == (
        reach.claimed_authority("[::1]:8787")
    )
    assert reach.claimed_authority("relay.example:x") == ""
    assert reach.relay_origin("https://op:pw@relay.example:8443/") == (
        "https://relay.example:8443"
    )


# ---------------------------------------------------- connecting the relay


async def test_start_creates_a_private_identity_and_reports_status(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        assert stat.S_IMODE(os.stat(daemon.path).st_mode) == 0o600
        loaded = reach.load_identity(daemon.path)
        assert service.state == "connecting"
        assert service.status()["id"] == loaded.tunnel_id
        await _connected(relay, service)
        assert service.status() == {
            "state": "connected",
            "relay": relay.origin,
            "id": loaded.tunnel_id,
            "key": reach.b64_encode(loaded.dh_public),
            "salt": reach.b64_encode(loaded.salt),
            "node": "nas",
            "fingerprint": loaded.fingerprint,
        }
        # a second start on the same path reuses the identity
        again = reach.ensure_identity(daemon.path)
        assert again.tunnel_id == loaded.tunnel_id


async def test_hello_signs_the_challenge_and_publishes_the_admit_set(
    daemon, caplog
):
    caplog.set_level(logging.INFO, logger="cronstable")
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        identity = service.identity
        tid, headers, _ = relay.upgrades[0]
        assert tid == identity.tunnel_id
        assert headers["User-Agent"] == "cronstable/test reach"
        assert headers["Sec-WebSocket-Protocol"] == reach.SUBPROTOCOL
        # the fake relay verified the signature before recording the hello
        (hello,) = relay.hellos
        assert hello["v"] == 1
        assert hello["agent"] == "cronstable/test reach"
        assert hello["node"] == "nas"
        assert hello["limits"] == {"maxBody": reach.MAX_BODY}
        assert hello["admit"] == reach.admit_ids(
            ["secret"], identity.salt, identity.tunnel_id_bytes
        )
        assert service.limits["initialWindow"] == reach.DEFAULT_INITIAL_WINDOW
        assert (
            "reach: connected to relay 127.0.0.1:" in caplog.text
            and identity.fingerprint in caplog.text
        )


async def test_userinfo_becomes_basic_auth_and_stays_out_of_logs(
    daemon, caplog
):
    caplog.set_level(logging.INFO, logger="cronstable")
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        dialed = relay.origin.replace("http://", "http://op:s3cretpw%2Fx@")
        service = await daemon.attach(cron, dialed)
        await _connected(relay, service)
        _, headers, _ = relay.upgrades[0]
        assert headers["Authorization"] == "Basic " + base64.b64encode(
            b"op:s3cretpw/x"
        ).decode("ascii")
        assert service.status()["relay"] == relay.origin
        assert "s3cretpw" not in caplog.text


async def test_challenge_from_another_host_is_refused(daemon, caplog):
    async with _FakeRelay(relay_host="other.example") as relay:
        cron = await daemon.boot()
        await daemon.attach(cron, relay.origin)
        await _until(lambda: reach.CLOSE_PROTOCOL_ERROR in relay.closes)
        assert relay.hellos == []
        assert "presented itself as 'other.example'" in caplog.text


async def test_unexpected_control_message_is_a_protocol_error(daemon, caplog):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        await daemon.attach(cron, relay.origin)
        await relay.wait_connected()
        await relay.ws.send_str(
            json.dumps({"v": 1, "type": "admit", "admit": []})
        )
        await _until(lambda: reach.CLOSE_PROTOCOL_ERROR in relay.closes)
        assert "protocol error" in caplog.text


async def test_stop_says_bye_and_unbinds_the_loopback_site(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        sock_dir = service._sock_dir
        assert sock_dir is not None and os.path.isdir(sock_dir)
        assert stat.S_IMODE(os.stat(sock_dir).st_mode) == 0o700
        await service.stop()
        await _until(lambda: any(t["type"] == "bye" for t in relay.texts))
        assert not os.path.exists(sock_dir)
        assert service._site is None
        # the web app outlives the service, and a second stop is a no-op
        assert cron.web_runner is not None
        await service.stop()


# ------------------------------------------------------- serving requests


async def test_requests_are_served_through_the_web_app(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)

        version = await _request(relay, app, 2, _head("GET", "/version"))
        assert version.reject is None
        assert version.kinds[0] == reach.KIND_HEAD
        assert version.kinds[-1] == reach.KIND_END
        assert set(version.kinds[1:-1]) == {reach.KIND_BODY}
        assert version.status == 200
        assert version.body.decode().strip() == cronstable.version.version
        # records are numbered from 0 under the request's own counter
        assert [r[1] for r in version.records] == list(
            range(len(version.records))
        )
        assert {r[0] for r in version.records} == {app._ctr}

        whoami = await _request(relay, app, 3, _head("GET", "/whoami"))
        doc = json.loads(whoami.body)
        assert doc["label"] == "authToken"
        # the whoami handler reads cron._reach, which the converge path
        # sets; this test attached the service directly
        assert doc["reach"] == {"state": "off"}

        jobs = await _request(relay, app, 4, _head("GET", "/jobs"))
        assert jobs.status == 200
        assert [j["name"] for j in json.loads(jobs.body)] == ["alpha"]
        names = [k for k, _ in jobs.headers]
        assert "content-type" in names
        assert not {"transfer-encoding", "content-length", "connection"} & set(
            names
        )

        # a bearer the daemon rejects arrives as an inner 401, never a
        # REJECT: the web app's auth middleware answered it
        denied = await _request(
            relay, app, 5, _head("GET", "/jobs", token="nope")
        )
        assert denied.reject is None and denied.status == 401
        assert json.loads(denied.body)["error"]


async def test_inner_request_drops_hop_by_hop_headers_and_sets_host(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        # hop-by-hop headers in the head would otherwise wedge the inner
        # request (a content-length with no body, a chunked claim)
        noisy = _head(
            "GET",
            "/version",
            extra=(
                ("content-length", "999"),
                ("transfer-encoding", "chunked"),
                ("connection", "close"),
                ("proxy-authorization", "Basic x"),
                ("host", "evil.example"),
            ),
        )
        reply = await _request(relay, app, 2, noisy)
        assert reply.status == 200
        # `a` becomes the inner Host: the origin gate compares a browser
        # Origin against it, so a same-origin POST passes...
        pause = _head(
            "POST",
            "/jobs/alpha/pause",
            extra=(
                ("content-type", "application/json"),
                ("origin", "http://" + AUTHORITY),
            ),
        )
        ok = await _request(
            relay, app, 3, pause, json.dumps({"durationSeconds": 60}).encode()
        )
        assert ok.status == 200, ok.body
        # ...and a cross-site one is refused by the web app itself
        foreign = dict(pause)
        foreign["a"] = "other.example:1"
        refused = await _request(
            relay,
            app,
            4,
            foreign,
            json.dumps({"durationSeconds": 60}).encode(),
        )
        assert refused.status == 403


async def test_unknown_session_gets_nosession_then_a_fresh_hello_works(
    daemon,
):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        forged = (
            bytes([reach.BODY_REQ])
            + secrets.token_bytes(reach.SESSION_ID_BYTES)
            + bytes(8)
            + bytes(40)
        )
        await relay.open(1, forged)
        chunks, reject = await relay.response(1)
        assert reject is None and chunks == [reach.NOSESSION_CHUNK]
        app = await _handshake(relay, service, 2)
        reply = await _request(relay, app, 3, _head("GET", "/version"))
        assert reply.status == 200


async def test_replayed_body_is_rejected(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        sealed = app.seal_request(_head("GET", "/version"))
        await relay.open(2, sealed)
        chunks, reject = await relay.response(2)
        assert reject is None and chunks
        await relay.open(3, sealed)
        chunks, reject = await relay.response(3)
        assert chunks == [] and reject == (reach.REJECT_REPLAY, "replay")


async def test_body_sealed_to_another_identity_is_rejected(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        other = reach.Identity.generate()
        stranger = reach.AppSession(
            service.identity.tunnel_id_bytes, other.dh_public
        )
        await relay.open(1, stranger.begin())
        chunks, reject = await relay.response(1)
        assert chunks == []
        assert reject == (reach.REJECT_CANNOT_DECRYPT, "cannot decrypt")
        # a tampered REQ under a live session does not open either
        app = await _handshake(relay, service, 2)
        body = bytearray(app.seal_request(_head("GET", "/version")))
        body[-1] ^= 1
        await relay.open(3, bytes(body))
        chunks, reject = await relay.response(3)
        assert reject == (reach.REJECT_CANNOT_DECRYPT, "cannot decrypt")
        # an unknown body type is an internal rejection, never a decrypt
        # failure that would make the app pair again
        await relay.open(4, b"\x09" + bytes(80))
        chunks, reject = await relay.response(4)
        assert reject is not None and reject[0] == reach.REJECT_INTERNAL


async def test_body_over_the_cap_is_rejected(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        head = _head(
            "POST",
            "/jobs/alpha/pause",
            extra=(("content-type", "application/json"),),
        )
        await relay.open(2, app.seal_request(head, bytes(reach.MAX_BODY + 1)))
        chunks, reject = await relay.response(2)
        assert chunks == []
        assert reject == (
            reach.REJECT_TOO_LARGE,
            "body over {} bytes".format(reach.MAX_BODY),
        )


async def test_session_rate_bucket_answers_busy(daemon, monkeypatch):
    monkeypatch.setattr(reach, "RATE_BURST", 2)
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        for req_id in (2, 3):
            reply = await _request(
                relay, app, req_id, _head("GET", "/version")
            )
            assert reply.status == 200
        busy = await _request(relay, app, 4, _head("GET", "/version"))
        assert busy.reject is None
        assert busy.kinds == [reach.KIND_ERROR]
        assert busy.error == {"error": "session busy", "status": 429}
        # the bucket refills at RATE_REFILL_PER_SECOND; a new session has
        # its own bucket, so the refusal is scoped, never global
        fresh = await _handshake(relay, service, 5)
        reply = await _request(relay, fresh, 6, _head("GET", "/version"))
        assert reply.status == 200


async def test_inflight_cap_and_cancel_on_a_log_tail(daemon, monkeypatch):
    monkeypatch.setattr(reach, "SESSION_INFLIGHT_CAP", 1)
    stream = JobOutputStream()
    stream.publish("stdout", "line one")
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        monkeypatch.setattr(cron, "_job_output", lambda name: stream)
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        await relay.open(2, app.seal_request(_head("GET", "/jobs/alpha/logs")))
        reader = reach.ChunkReader()
        # HEAD arrives as soon as the SSE response is prepared, and the
        # retained buffer follows as BODY records; the tail then waits
        ftype, payload = await relay.frame(2)
        assert ftype == reach.FRAME_DATA
        (head_chunk,) = reader.feed(payload)
        status, headers = reach.decode_head_record(
            app.open_record(head_chunk)[4]
        )
        assert status == 200
        assert ("content-type", "text/event-stream") in headers
        ftype, payload = await relay.frame(2)
        (body_chunk,) = reader.feed(payload)
        assert b"line one" in app.open_record(body_chunk)[4]
        # a second request on the session is refused while the tail
        # holds the only in-flight slot
        busy = await _request(relay, app, 3, _head("GET", "/version"))
        assert busy.error == {"error": "session busy", "status": 429}
        # CANCEL ends the tail quietly: no END, no further frames
        await relay.ws.send_bytes(
            reach.pack_cancel(2, reach.CANCEL_CLIENT_GONE)
        )
        await _until(lambda: 2 not in service._requests)
        with pytest.raises(asyncio.TimeoutError):
            await relay.frame(2, timeout=0.3)
        # and the slot is free again
        reply = await _request(relay, app, 4, _head("GET", "/version"))
        assert reply.status == 200


async def test_window_credit_gates_data_frames(daemon):
    async with _FakeRelay(initial_window=1500, max_chunk=400) as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        # the dashboard page: hundreds of kilobytes, served as identity
        await relay.open(2, app.seal_request(_head("GET", "/")))

        async def drain() -> tuple[int, list[bytes], bool]:
            total, payloads, ended = 0, [], False
            while True:
                try:
                    ftype, payload = await relay.frame(2, timeout=0.3)
                except asyncio.TimeoutError:
                    return total, payloads, ended
                if ftype == reach.FRAME_END:
                    ended = True
                    return total, payloads, ended
                assert ftype == reach.FRAME_DATA
                total += len(payload)
                payloads.append(payload)

        first, payloads, ended = await drain()
        assert 0 < first <= 1500 and not ended
        assert 2 in service._requests, "the writer is parked on credit"
        # WINDOW adds to whatever credit was left, so the bound is
        # cumulative
        await relay.ws.send_bytes(reach.pack_window(2, 1500))
        second, more, ended = await drain()
        assert 0 < second and first + second <= 3000 and not ended
        payloads += more
        await relay.ws.send_bytes(reach.pack_window(2, 1 << 30))
        rest, more, ended = await drain()
        assert ended and rest > 0
        payloads += more
        reader = reach.ChunkReader()
        chunks = [c for p in payloads for c in reader.feed(p)]
        reply = _Reply(chunks, None, app)
        assert reply.status == 200
        assert b"<html" in reply.body.lower()
        assert reply.kinds[-1] == reach.KIND_END
        # every BODY record honours the relay's chunk cap
        assert (
            max(len(r[4]) for r in reply.records if r[2] == reach.KIND_BODY)
            <= 400
        )


# --------------------------------------------------------- reconnection


async def test_reconnects_after_the_relay_drops_the_socket(daemon, caplog):
    caplog.set_level(logging.INFO, logger="cronstable")
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        await relay.ws.close(code=1001, message=b"deploy")
        await _until(lambda: len(relay.hellos) == 2)
        await _connected(relay, service)
        assert "disconnected from relay" in caplog.text
        assert "close code 1001" in caplog.text
        # requests work on the new socket
        app = await _handshake(relay, service, 1)
        reply = await _request(relay, app, 2, _head("GET", "/version"))
        assert reply.status == 200


async def test_three_superseded_closes_damp_the_reconnect(daemon, caplog):
    async with _FakeRelay(hello_closes=(4409, 4409, 4409)) as relay:
        cron = await daemon.boot()
        await daemon.attach(cron, relay.origin)
        await _until(lambda: len(relay.hellos) == 4, timeout=10)
        gaps = [
            later - earlier
            for earlier, later in zip(
                relay.hello_times, relay.hello_times[1:], strict=False
            )
        ]
        # two ordinary (fast) reconnects, then the damping wait
        assert gaps[2] >= 0.45, gaps
        assert gaps[2] > max(gaps[0], gaps[1]), gaps
        assert "superseded by another daemon" in caplog.text


async def test_relay_without_reach_parks_with_one_log_line(daemon, caplog):
    async with _FakeRelay(upgrade=(404, {})) as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _until(lambda: len(relay.upgrades) >= 3, timeout=10)
        stamps = [t for _, _, t in relay.upgrades]
        assert stamps[1] - stamps[0] >= 0.25
        assert stamps[2] - stamps[1] >= 0.25
        assert caplog.text.count("has no Reach") == 1
        assert service.state == "connecting"
        # the relay gains Reach: the next hourly retry connects
        relay.upgrade = None
        await relay.wait_connected(timeout=5)


async def test_zone_policy_refusal_parks_and_a_plain_refusal_does_not(
    daemon, caplog
):
    async with _FakeRelay(
        upgrade=(403, {"cf-mitigated": "challenge"})
    ) as relay:
        cron = await daemon.boot()
        await daemon.attach(cron, relay.origin)
        await _until(lambda: len(relay.upgrades) >= 2, timeout=10)
        stamps = [t for _, _, t in relay.upgrades]
        assert stamps[1] - stamps[0] >= 0.25
        assert caplog.text.count("zone policy") == 1
    caplog.clear()
    async with _FakeRelay(upgrade=(500, {})) as relay:
        cron = await daemon.boot()
        await daemon.attach(cron, relay.origin)
        await _until(lambda: len(relay.upgrades) >= 3, timeout=10)
        stamps = [t for _, _, t in relay.upgrades]
        assert stamps[2] - stamps[0] < 0.25
        assert "refused the upgrade with HTTP 500" in caplog.text


# ------------------------------------------------------- admit and cron


async def test_refresh_admit_sends_the_rotated_set(daemon):
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        await _connected(relay, service)
        identity = service.identity
        cron._web_token_table = [
            _WebToken(b"rotated", frozenset({"view"}), "phone")
        ]
        await service.refresh_admit()
        await _until(lambda: any(t["type"] == "admit" for t in relay.texts))
        (admit,) = [t for t in relay.texts if t["type"] == "admit"]
        assert admit["v"] == 1
        assert admit["admit"] == reach.admit_ids(
            ["rotated"], identity.salt, identity.tunnel_id_bytes
        )
        # an unchanged set sends nothing more
        await service.refresh_admit()
        await asyncio.sleep(0.05)
        assert len([t for t in relay.texts if t["type"] == "admit"]) == 1


async def test_start_stop_reach_follows_the_web_app(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="cronstable")
    key = str(tmp_path / "reach.json")
    async with _FakeRelay() as relay:
        cron = _cron(JOB_YAML)
        cfg = {"relay": relay.origin, "keyFile": key, "heartbeat": 30.0}
        try:
            # no web app yet: nothing to bind onto, and nothing latched,
            # so the next pass retries
            await cron.start_stop_reach(cfg)
            assert cron._reach is None
            assert cron._applied_reach_config is None
            await cron.start_stop_web_app(dict(WEB))
            await cron.start_stop_reach(cfg)
            first = cron._reach
            assert first is not None
            assert cron._applied_reach_config == cfg
            assert cron._applied_reach_config is not cfg
            await _connected(relay, first)
            identity = first.identity
            assert relay.hellos[-1]["admit"] == reach.admit_ids(
                ["secret"], identity.salt, identity.tunnel_id_bytes
            )
            assert relay.hellos[-1]["agent"] == "cronstable/{} reach".format(
                cronstable.version.__version__
            )
            assert relay.hellos[-1]["node"] == cron._node_name()
            assert "remote access through 127.0.0.1:" in caplog.text
            # the same config keeps the instance
            await cron.start_stop_reach(dict(cfg))
            assert cron._reach is first
            # a token change rebuilds the web app, which stops the
            # service with the old runner; the next pass restarts it onto
            # the new one, and the fresh hello carries the new admit set
            await cron.start_stop_web_app(
                {
                    "listen": ["http://127.0.0.1:0"],
                    "authToken": {"value": "rotated"},
                }
            )
            assert cron._reach is None
            await cron.start_stop_reach(cfg)
            second = cron._reach
            assert second is not None and second is not first
            assert second.runner is cron.web_runner
            await _until(lambda: len(relay.hellos) == 2)
            assert relay.hellos[1]["admit"] == reach.admit_ids(
                ["rotated"], identity.salt, identity.tunnel_id_bytes
            )
            # `cronstable reach rotate` writes a new identity file; the
            # next pass restarts the service under the new fingerprint
            rotated = reach.Identity.generate()
            reach.write_identity(key, rotated, replace=True)
            stamp = os.stat(key)
            os.utime(
                key, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 1_000_000)
            )
            await cron.start_stop_reach(cfg)
            third = cron._reach
            assert third is not second
            assert third.identity.tunnel_id == rotated.tunnel_id
            assert "identity file changed" in caplog.text
            # the section removed: the service stops, the web app stays
            await cron.start_stop_reach(None)
            assert cron._reach is None and cron.web_runner is not None
            assert "section removed" in caplog.text
        finally:
            await cron.start_stop_web_app(None)
    await asyncio.sleep(0.05)


async def test_start_stop_reach_never_raises(monkeypatch, caplog):
    cron = _cron(JOB_YAML)

    async def boom(_reach_config):
        raise RuntimeError("convergence exploded")

    monkeypatch.setattr(cron, "_converge_reach", boom)
    await cron.start_stop_reach(
        {"relay": "https://x", "keyFile": "k", "heartbeat": 30.0}
    )
    assert "could not converge the remote-access service" in caplog.text


async def test_whoami_reach_shapes(daemon):
    token = _WebToken(b"t", frozenset({"view"}), "phone")
    cron = _cron(JOB_YAML)
    body = json.loads((await cron._web_whoami(_Req(token=token))).body)
    assert body["reach"] == {"state": "off"}
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        cron._reach = service
        await _connected(relay, service)
        matched = json.loads((await cron._web_whoami(_Req(token=token))).body)
        assert matched["reach"] == service.status()
        assert matched["reach"]["state"] == "connected"
        assert matched["reach"]["fingerprint"] == service.identity.fingerprint
        anon = json.loads(
            (await cron._web_whoami(_Req(anon=frozenset({"view"})))).body
        )
        assert anon["reach"] == {"state": "connected"}
        assert json.loads((await cron._web_whoami(_Req())).body)["reach"] == {
            "state": "connected"
        }
        await service.stop()
        cron._reach = None


async def test_loopback_falls_back_to_tcp_without_unix_sockets(
    daemon, monkeypatch
):
    monkeypatch.setattr(platform_mod, "supports_unix_sockets", lambda: False)
    async with _FakeRelay() as relay:
        cron = await daemon.boot()
        service = await daemon.attach(cron, relay.origin)
        assert service._sock_path is None
        assert service._tcp_base.startswith("http://127.0.0.1:")
        await _connected(relay, service)
        app = await _handshake(relay, service, 1)
        reply = await _request(relay, app, 2, _head("GET", "/version"))
        assert reply.status == 200
        # the loopback port is a listener like every other: the bearer
        # gate applies inside the tunnel too
        denied = await _request(
            relay, app, 3, _head("GET", "/jobs", token=None)
        )
        assert denied.status == 401


# ------------------------------------------------- dashboard, CLI, imports


def test_dashboard_pairing_payload_carries_the_tunnel():
    with open(
        os.path.join(ROOT, "cronstable", "web", "index.html"),
        encoding="utf-8",
    ) as fh:
        page = fh.read()
    for needle in (
        'if (!r || r.state !== "connected") return null;',
        "return { relay: r.relay, id: r.id, key: r.key, salt: r.salt, "
        "node: r.node };",
        "if (tunnel) p.tunnel = tunnel;",
        '"Remote access key " + r.fingerprint + " through " + host',
        '"Remote access is configured but not connected"',
        'id="pairReach"',
    ):
        assert needle in page, needle
    body = page[
        page.index("function openPair(") : page.index("function closePair(")
    ]
    # rendered from the cached probe, then again from the fresh answer
    assert "pairPayload(state.auth)" in body
    assert "pairPayload(w)" in body
    assert "renderPairReach(w)" in body


def _run_cli(monkeypatch, capsys, *argv: str) -> tuple[int, str, str]:
    monkeypatch.setattr(sys, "argv", ["cronstable", *argv])
    with pytest.raises(SystemExit) as exc:
        main.main_loop()
    out = capsys.readouterr()
    return exc.value.code, out.out, out.err


def test_cli_show_and_rotate(tmp_path, capsys, monkeypatch):
    key = tmp_path / "reach.json"
    cfg = tmp_path / "cronstable.yaml"
    cfg.write_text(
        REACH_YAML.replace("/var/lib/cronstable/reach.json", str(key))
        + JOB_YAML
    )
    code, out, _ = _run_cli(
        monkeypatch, capsys, "reach", "show", "-c", str(cfg)
    )
    assert code == 0, out
    identity = reach.load_identity(str(key))
    assert stat.S_IMODE(os.stat(key).st_mode) == 0o600
    assert "tunnel id " + identity.tunnel_id in out
    assert "fingerprint: " + identity.fingerprint in out
    assert "relay: https://relay.example" in out
    assert "key file: " + str(key) in out
    assert "created: " + identity.created_at in out

    code, out, _ = _run_cli(
        monkeypatch, capsys, "reach", "rotate", "-c", str(cfg)
    )
    assert code == 0, out
    rotated = reach.load_identity(str(key))
    assert rotated.tunnel_id != identity.tunnel_id
    assert "fingerprint: " + rotated.fingerprint in out
    assert "scan the dashboard's pairing QR again" in out
    code, out, _ = _run_cli(
        monkeypatch, capsys, "reach", "show", "-c", str(cfg)
    )
    assert code == 0 and rotated.fingerprint in out

    # no web.reach section: a config error on stderr, exit 1
    bare = tmp_path / "bare.yaml"
    bare.write_text(JOB_YAML)
    code, out, err = _run_cli(
        monkeypatch, capsys, "reach", "show", "-c", str(bare)
    )
    assert code == 1 and out == ""
    assert "no `web.reach` section" in err
    # no action: usage on stderr, exit 2 (the root -c, since only the
    # actions carry their own)
    code, _, err = _run_cli(monkeypatch, capsys, "-c", str(cfg), "reach")
    assert code == 2 and "no action given" in err


def test_importing_the_daemon_loads_neither_aiohttp_nor_libsodium():
    # the cron module imports cronstable.reach lazily, inside the
    # converge step, and reach.py imports aiohttp and nacl inside the
    # functions that need them (tests/test_perf_invariants.py pins the
    # aiohttp half for the whole daemon graph)
    probe = (
        "import sys, cronstable.cron; "
        "print('aiohttp' in sys.modules, 'nacl.bindings' in sys.modules, "
        "'cronstable.reach' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=True,
    )
    assert out.stdout.strip() == "False False False"
    probe = (
        "import sys, cronstable.reach; "
        "print('aiohttp' in sys.modules, 'nacl.bindings' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=True,
    )
    assert out.stdout.strip() == "False False"
