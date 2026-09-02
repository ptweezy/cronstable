"""End-to-end encrypted push alerts: sealing, registry, service, API.

Covers cronstable.push (payload build/fit, sealing round-trips under
both suites, the two device stores, PushService), the PushReporter edge
in cronstable.job, the fail-closed config validation, the /push/devices
and /whoami handlers, scope enforcement on the new routes, the
start_stop_push lifecycle, and the Bonjour advertiser (with a fake
zeroconf).

PyNaCl (x25519 sealed boxes, the push extra) and cryptography (X-Wing
HPKE, the push-pq extra) are dev dependencies: PyNaCl has wheels on
every CI cell, cryptography on every cell except win-arm64 (see
requirements_dev.txt). Only the tests that actually touch key material
skip without them (the ``requires_pynacl`` and ``requires_xwing``
markers); the
store, config-validation, handler-scope, /whoami, lifecycle and Bonjour
tests are crypto-free and run on a bare `pip install -e .` checkout
too. A module-level importorskip here once silently vaporized all of
them on any cell without the wheel; never reintroduce one.
"""

import asyncio
import base64
import copy
import json
import logging
import os
import socket
import sys
import threading
import time
from types import SimpleNamespace
from typing import Any, Optional

import pytest
from aiohttp import web

import cronstable.config as config
import cronstable.cron as cron_mod
import cronstable.discovery as discovery
import cronstable.push as push
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import (
    WEB_ANON_REQUEST_KEY,
    WEB_TOKEN_REQUEST_KEY,
    Cron,
    _WebToken,
)
from cronstable.fingerprint import canonical_job
from cronstable.job import (
    NotifyEventContext,
    PushReporter,
    RunningJob,
    report_config_enabled,
)
from tests._helpers import _xwing_findable, requires_xwing
from tests.conftest import _cron as _shared_cron

try:
    from nacl import public as nacl_public
except ImportError:  # the push extra is optional even in dev checkouts
    nacl_public = None

requires_pynacl = pytest.mark.skipif(
    nacl_public is None,
    reason="pynacl (the push extra) is not installed",
)


def _sealable_now() -> list[str]:
    """What /whoami advertises on this box, keyed to the shared
    ``_xwing_findable`` probe rather than pinned as a constant: the
    post-quantum library is an optional extra.  The daemon
    additionally backs the advertisement with a real probe seal
    (push._xwing_probe); on the healthy installs CI runs, the two
    probes agree, and the divergent case (findable but cannot seal)
    has its own test below."""
    return ["x25519", "xwing"] if _xwing_findable else ["x25519"]


# ---------------------------------------------------------------- helpers


def _device_keypair():
    private = nacl_public.PrivateKey.generate()
    public_b64 = base64.b64encode(bytes(private.public_key)).decode()
    return private, public_b64


def _open_sealed(private, ciphertext_b64: str) -> dict[str, Any]:
    sealed = base64.b64decode(ciphertext_b64)
    plaintext = nacl_public.SealedBox(private).decrypt(sealed)
    return json.loads(plaintext.decode("utf-8"))


def _xwing_keypair():
    """A generated X-Wing device keypair: (private key, wire base64).

    The wire form the app registers: the ML-KEM-768
    encapsulation key (1184 bytes) followed by the X25519 public key
    (32), standard base64.
    """
    from cryptography.hazmat.primitives import hpke
    from cryptography.hazmat.primitives.asymmetric import mlkem, x25519

    mlkem_private = mlkem.MLKEM768PrivateKey.generate()
    x_private = x25519.X25519PrivateKey.generate()
    private = hpke.MLKEM768X25519PrivateKey(mlkem_private, x_private)
    wire = (
        mlkem_private.public_key().public_bytes_raw()
        + x_private.public_key().public_bytes_raw()
    )
    return private, base64.b64encode(wire).decode()


def _open_xwing(sealed_b64: str, private) -> dict[str, Any]:
    """Open one sealed X-Wing alert the way the app does.

    Single-shot HPKE decrypt over the combined enc||ct blob.  The
    ciphersuite and info bytes are spelled out here rather than read
    from push, so the test pins the wire contract instead of the
    module's agreement with itself.
    """
    from cryptography.hazmat.primitives.hpke import AEAD, KDF, KEM, Suite

    suite = Suite(KEM.MLKEM768_X25519, KDF.HKDF_SHA256, AEAD.AES_256_GCM)
    plaintext = suite.decrypt(
        base64.b64decode(sealed_b64),
        private,
        info=b"cronstable-push-xwing",
    )
    return json.loads(plaintext.decode("utf-8"))


class _FakeJobCtx:
    """Quacks like RunningJob as far as build_payload reads one."""

    def __init__(self, name="backup", stderr="boom\nworse", **overrides):
        self.config = SimpleNamespace(name=name)
        self.template_vars = {
            "name": name,
            "success": False,
            "fail_reason": "exit code 1",
            "stdout": None,
            "stderr": stderr,
            "exit_code": 1,
            "host": "node-a",
            "schedule": "*/5 * * * *",
            "started_at": "2026-07-23T01:00:00+00:00",
            "run_id": "run-123",
        }
        self.template_vars.update(overrides)


class _RelayServer:
    """A local stand-in for the hosted push relay; records every POST."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.requests: list[dict[str, Any]] = []
        self.url = ""
        self._runner: Optional[web.AppRunner] = None

    async def __aenter__(self) -> "_RelayServer":
        app = web.Application()
        app.router.add_post("/v1/notify", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        self.url = "http://127.0.0.1:{}/v1/notify".format(port)
        return self

    async def __aexit__(self, *exc) -> None:
        assert self._runner is not None
        await self._runner.cleanup()

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append(await request.json())
        return web.json_response({"ok": True}, status=self.status)


class _BarrierRelayServer(_RelayServer):
    """A relay that answers nobody until ``expected`` POSTs are in flight.

    A sequential fan-out can never satisfy it: the first request would be
    waiting for a second the daemon has not sent yet, so it would sit
    there until the client's own relay timeout. That makes this a
    concurrency assertion with no clock in it.
    """

    def __init__(self, expected: int) -> None:
        super().__init__()
        self.expected = expected
        self.inflight = 0
        self.peak = 0
        self._all_here = asyncio.Event()

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append(await request.json())
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        if self.inflight >= self.expected:
            self._all_here.set()
        try:
            # bounded so a regression fails the assertions below rather
            # than hanging the suite
            await asyncio.wait_for(self._all_here.wait(), timeout=10)
        finally:
            self.inflight -= 1
        return web.json_response({"ok": True})


class _FloorCapRelayServer(_RelayServer):
    """A relay enforcing the protocol's 3000-character floor cap.

    The relay a daemon fitting to CIPHERTEXT_B64_MAX meets when it is
    released ahead of the relay it posts to.
    """

    async def _handle(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.requests.append(body)
        if len(body["ciphertext"]) > push.CIPHERTEXT_B64_FLOOR:
            return web.json_response(
                {
                    "v": 1,
                    "error": "ciphertext exceeds {} characters".format(
                        push.CIPHERTEXT_B64_FLOOR
                    ),
                },
                status=400,
            )
        return web.json_response({"v": 1, "outcome": "forwarded"}, status=202)


class _UndecodableRelayServer(_RelayServer):
    """A relay whose error body is not the utf-8 its header claims."""

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append(await request.json())
        return web.Response(
            body=b"\xff\xfe\xfd",
            status=500,
            content_type="text/plain",
            charset="utf-8",
        )


def _service(store, relay_url="http://127.0.0.1:1/unused") -> push.PushService:
    return push.PushService(
        relay_url=relay_url, relay_timeout=5.0, store=store, host="node-a"
    )


async def _paired_service(tmp_path, relay_url, public_b64):
    service = _service(
        push.FileDeviceStore(str(tmp_path / "devices.json")), relay_url
    )
    record, created = await service.pair(
        {
            "name": "phone",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-abcdef",
        },
        "authToken",
    )
    assert created
    return service, record


# ------------------------------------------------------- payload building


def test_build_payload_job_failure_carries_identity_and_tail():
    payload = push.build_payload(_FakeJobCtx(), False, True)
    assert payload["v"] == push.PUSH_PROTOCOL_VERSION
    assert payload["kind"] == "failure"
    assert payload["name"] == "backup"
    assert payload["host"] == "node-a"
    assert payload["run_id"] == "run-123"
    assert payload["fail_reason"] == "exit code 1"
    assert payload["log_tail"] == ["boom", "worse"]


def test_build_payload_success_and_no_tail_when_disabled():
    ctx = _FakeJobCtx(success=True, fail_reason=None, exit_code=0)
    payload = push.build_payload(ctx, True, False)
    assert payload["kind"] == "success"
    assert "log_tail" not in payload
    assert "fail_reason" not in payload


def test_build_payload_event_kind():
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject="dag etl failed",
        message="task load failed",
        fields={"dag": "etl", "run_key": "sched-1"},
    )
    payload = push.build_payload(ctx, False, True)
    assert payload["kind"] == "event"
    assert payload["event"] == "dag_failure"
    assert payload["subject"] == "dag etl failed"
    assert payload["dag"] == "etl"
    assert payload["run_key"] == "sched-1"
    # events have no process, so never a log tail
    assert "log_tail" not in payload


def test_build_payload_event_omits_absent_text_fields():
    # The sealed-plaintext contract in relay-protocol.md is that a field
    # is a string or missing, never null: the app decodes it that way and
    # every absent field is budget an alert does not spend.
    ctx = NotifyEventContext(
        event="dag_failure",
        success=False,
        name="etl",
        subject=None,
        message=None,
        fields={},
    )
    payload = push.build_payload(ctx, False, True)
    assert payload["kind"] == "event"
    assert "subject" not in payload
    assert "message" not in payload


def test_build_payload_sla_kind():
    yaml = """
jobs:
  - name: late
    command: "true"
    schedule: "* * * * *"
"""
    job_config = parse_config_string(yaml, "").jobs[0]
    from cronstable.job import SlaBreachContext

    ctx = SlaBreachContext(
        job_config,
        check="lateAfterSeconds",
        threshold_seconds=60.0,
        observed_seconds=190.0,
    )
    payload = push.build_payload(ctx, False, True)
    assert payload["kind"] == "sla"
    assert payload["sla_check"] == "lateAfterSeconds"
    assert payload["threshold_seconds"] == 60.0
    assert payload["observed_seconds"] == 190.0


def _tail_ctx(count=5000, width=200):
    """A job context whose stderr overflows the plaintext cap.

    ``width`` matters: build_payload already caps the tail at
    LOG_TAIL_MAX_LINES, so narrow lines produce a payload that fits and never
    reaches the trimming code. Real failure output (a stack trace, a
    subprocess command line) is wide, and width is what forces a trim.
    """
    lines = [
        "line-{:05d} ".format(i).ljust(width, "x") for i in range(count)
    ]
    return _FakeJobCtx(stderr="\n".join(lines)), lines


def _fits(payload, tail_lines):
    """Whether ``payload`` carrying exactly ``tail_lines`` fits the cap."""
    probe = dict(payload, log_tail=list(tail_lines))
    return len(push._encode(probe)) <= push.MAX_PLAINTEXT_BYTES


def test_fit_payload_trims_oldest_tail_lines_first():
    ctx, lines = _tail_ctx()
    payload = push.build_payload(ctx, False, True)
    kept_before = list(payload["log_tail"])
    assert not _fits(payload, kept_before)  # the trim really is needed
    data = push.fit_payload(payload)

    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    # the returned bytes and the mutated payload must agree: fit_payload
    # bisects the drop count and re-applies when the last probe was the
    # failing one just below the answer, so an off-by-one there returns the
    # encoding of a payload the caller does not hold.
    assert push._encode(payload) == data

    fitted = json.loads(data.decode("utf-8"))
    kept = fitted["log_tail"]
    # newest lines survive (a failure's reason lives at the end)
    assert kept[-1] == lines[-1]
    assert kept == kept_before[-len(kept) :]
    # and the fewest were dropped: putting one back would not fit.
    assert len(kept) < len(kept_before)
    assert not _fits(payload, kept_before[-(len(kept) + 1) :])
    # the identity core is intact
    assert fitted["name"] == "backup"
    assert fitted["kind"] == "failure"


def test_fit_payload_leaves_a_fitting_payload_untouched():
    # the common case: a small tail already fits, so fit_payload must leave
    # every line and field in place.
    ctx, _lines = _tail_ctx(count=3, width=20)
    payload = push.build_payload(ctx, False, True)
    before = json.loads(push._encode(payload))
    data = push.fit_payload(payload)
    assert json.loads(data.decode("utf-8")) == before


def test_fit_payload_drops_the_whole_tail_when_no_line_fits():
    # even a single line of this width overflows, so the tail cannot be
    # trimmed into the cap: the key itself goes (that is what frees the last
    # bytes) and the fit falls through to the free-text fields.
    ctx, _lines = _tail_ctx(count=40, width=3000)
    payload = push.build_payload(ctx, False, True)
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    assert push._encode(payload) == data
    fitted = json.loads(data.decode("utf-8"))
    assert "log_tail" not in fitted  # the key goes, rather than an empty list
    assert fitted["name"] == "backup"
    assert fitted["kind"] == "failure"


def test_fit_payload_drops_the_tail_then_halves_a_long_reason():
    # Both shrink stages in one alert: a wide log tail plus a long failure
    # reason, the ordinary shape of a crashed job. No drop count fits while
    # the reason is still there, so the bisect gives up, the tail goes
    # entirely, and the loop moves on to halving the reason.
    ctx, _lines = _tail_ctx(count=40, width=200)
    ctx.template_vars["fail_reason"] = "x" * 50_000
    payload = push.build_payload(ctx, False, True)
    assert payload["log_tail"]  # the precondition: there IS a tail to drop
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    assert push._encode(payload) == data
    fitted = json.loads(data.decode("utf-8"))
    assert "log_tail" not in fitted
    assert 64 <= len(fitted["fail_reason"]) < 50_000
    assert fitted["name"] == "backup"


def test_fit_payload_skips_context_fields_the_alert_does_not_carry():
    # An event alert has no schedule; the fallback must walk past the fields
    # that are absent rather than stopping at the first name in its list.
    payload = {
        "v": push.PUSH_PROTOCOL_VERSION,
        "kind": "event",
        "name": "etl",
        "host": "node-a",
        # Sized off the budget, not a literal, so this stays an
        # overflow whatever the cap is worth.
        "started_at": "z" * (push.MAX_PLAINTEXT_BYTES + 300),
        "run_id": "run-123",
    }
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    assert push._encode(payload) == data
    fitted = json.loads(data.decode("utf-8"))
    assert "started_at" not in fitted
    assert fitted["run_id"] == "run-123"
    assert fitted["name"] == "etl"


def test_fit_payload_drops_context_fields_when_nothing_long_is_left():
    # No tail, and no free-text field long enough to halve, but still over
    # the cap: the optional context fields go one at a time so the alert
    # keeps its identity (name, kind, host).
    payload = {
        "v": push.PUSH_PROTOCOL_VERSION,
        "kind": "failure",
        "name": "backup",
        "host": "node-a",
        "fail_reason": "x" * 60,  # <= 64: not eligible for halving
        # Budget-relative for the same reason as the test above.
        "schedule": "y" * (push.MAX_PLAINTEXT_BYTES + 300),  # first to go
        "started_at": "2026-07-23T01:00:00+00:00",
        "run_id": "run-123",
    }
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    assert push._encode(payload) == data
    fitted = json.loads(data.decode("utf-8"))
    assert "schedule" not in fitted
    # only as much as was needed: the later context fields survive
    assert fitted["run_id"] == "run-123"
    assert fitted["started_at"] == "2026-07-23T01:00:00+00:00"
    # as do the identity core and the untouched short reason.
    assert fitted["name"] == "backup"
    assert fitted["kind"] == "failure"
    assert fitted["host"] == "node-a"
    assert fitted["fail_reason"] == "x" * 60


@pytest.mark.parametrize("width", [40, 55, 56, 57, 80, 111, 200, 512])
def test_fit_payload_is_minimal_at_every_tail_width(width):
    # The bisect's answer must match a line-at-a-time walk's at every shape.
    # Pinned as an invariant (it fits, the bytes agree, and one line more
    # would overflow) rather than as an expected drop count, so it holds if
    # the cap, the field set or the encoding ever changes.
    ctx, _lines = _tail_ctx(count=200, width=width)
    payload = push.build_payload(ctx, False, True)
    kept_before = list(payload["log_tail"])
    data = push.fit_payload(payload)

    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    assert push._encode(payload) == data
    kept = payload.get("log_tail", [])
    if kept:
        assert kept == kept_before[-len(kept) :]  # oldest-first, in order
        if len(kept) < len(kept_before):
            assert not _fits(payload, kept_before[-(len(kept) + 1) :])
    assert json.loads(data.decode("utf-8"))["name"] == "backup"


def test_fit_payload_truncates_long_text_without_tail():
    ctx = _FakeJobCtx(stderr=None, fail_reason="x" * 50_000)
    payload = push.build_payload(ctx, False, True)
    data = push.fit_payload(payload)
    assert len(data) <= push.MAX_PLAINTEXT_BYTES
    fitted = json.loads(data.decode("utf-8"))
    assert fitted["name"] == "backup"
    assert 64 <= len(fitted["fail_reason"]) < 50_000


def test_collapse_id_same_run_same_id_different_run_differs():
    a = push.build_payload(_FakeJobCtx(), False, True)
    b = push.build_payload(_FakeJobCtx(), False, True)
    c = push.build_payload(_FakeJobCtx(run_id="run-456"), False, True)
    assert push.collapse_id(a, "s1") == push.collapse_id(b, "s1")
    assert push.collapse_id(a, "s1") != push.collapse_id(c, "s1")
    assert len(push.collapse_id(a, "s1")) == 32


def test_collapse_id_salt_defeats_wordlist_precomputation():
    # The relay-facing property: without the installation salt the
    # identity fields are low entropy (kind + name on a stateless
    # install), so ids must differ salt-to-salt.
    a = push.build_payload(_FakeJobCtx(run_id=None), False, True)
    assert push.collapse_id(a, "s1") != push.collapse_id(a, "s2")


# ----------------------------------------------------------------- crypto


@requires_pynacl
def test_seal_round_trip():
    private, public_b64 = _device_keypair()
    ciphertext = push.seal_to_device(public_b64, b'{"hello": "world"}')
    assert _open_sealed(private, ciphertext) == {"hello": "world"}


@requires_pynacl
def test_seal_rejects_garbage_key():
    with pytest.raises(push.PushError):
        push.seal_to_device("not base64!!", b"x")


@requires_pynacl
def test_seal_rejects_low_order_key_as_push_error():
    # An all-zero key is 32 valid bytes but libsodium refuses it at
    # ENCRYPT time with a raw nacl exception. It must surface as
    # PushError: anything else escapes _send_payload's per-device catch
    # and kills the whole fan-out (the one-bad-key-pages-nobody bug).
    zero_key = base64.b64encode(b"\x00" * 32).decode()
    with pytest.raises(push.PushError):
        push.seal_to_device(zero_key, b"x")


@requires_pynacl
def test_validate_pairing_rejects_unusable_key():
    # The same all-zero key must already be a 400 at pairing, so it can
    # never become a persistent registry record that fails every alert.
    zero_key = base64.b64encode(b"\x00" * 32).decode()
    with pytest.raises(push.PushError):
        push.validate_pairing(
            {
                "name": "p",
                "platform": "ios",
                "pushToken": "t",
                "publicKey": zero_key,
            }
        )


def test_key_fingerprint_format_and_garbage():
    _hashable = base64.b64encode(b"\x01" * 32).decode()
    fp = push.key_fingerprint(_hashable)
    assert fp is not None
    parts = fp.split("-")
    assert len(parts) == 3 and all(len(p) == 4 for p in parts)
    # deterministic, and distinct keys get distinct prints
    assert fp == push.key_fingerprint(_hashable)
    assert fp != push.key_fingerprint(base64.b64encode(b"\x02" * 32).decode())
    assert push.key_fingerprint(None) is None
    assert push.key_fingerprint("not base64!!") is None


@requires_pynacl
def test_validate_pairing_normalizes_and_rejects():
    _, public_b64 = _device_keypair()
    fields = push.validate_pairing(
        {
            "name": "  phone  ",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok",
        }
    )
    assert fields["name"] == "phone"
    with pytest.raises(push.PushError):
        push.validate_pairing("not a dict")
    with pytest.raises(push.PushError):
        push.validate_pairing(
            {
                "name": "p",
                "platform": "ios",
                "pushToken": "t",
                "publicKey": base64.b64encode(b"short").decode(),
            }
        )
    with pytest.raises(push.PushError):
        push.validate_pairing(
            {
                "name": "x" * 65,
                "platform": "ios",
                "pushToken": "t",
                "publicKey": public_b64,
            }
        )


# ------------------------------------------------------- suites and budget


def test_ciphertext_cap_fits_the_apns_frame_with_its_reserve():
    # Derive the cap here the same way push.py does, so an edit to any
    # of the three inputs has to keep the arithmetic true; the relay's
    # apns-size.spec.ts pins RELAY_ENVELOPE_BYTES against the envelope it
    # really serializes.
    assert (
        push.RELAY_ENVELOPE_BYTES
        + push.CIPHERTEXT_B64_MAX
        + push.RELAY_ENVELOPE_RESERVE
        <= push.APNS_PAYLOAD_MAX
    )
    # And the reserve is real slack, not a rounding artifact.
    assert push.RELAY_ENVELOPE_RESERVE > 0
    # relay-protocol.md quotes both numbers verbatim; the reserve is sized
    # so the cap lands exactly on the quoted one.
    assert push.CIPHERTEXT_B64_MAX == 3800
    assert push.CIPHERTEXT_B64_FLOOR < push.CIPHERTEXT_B64_MAX


def test_suite_budgets_track_their_sealing_overhead():
    # Every suite's budget is the same raw cap minus its own overhead, so
    # a wider ciphertext costs log lines and nothing else.
    raw = push.CIPHERTEXT_B64_MAX // 4 * 3
    for name, spec in push.SUITES.items():
        assert push.max_plaintext_bytes(name) == raw - spec.overhead
    # X-Wing's ciphertext is 1088 bytes wider than a sealed box's, which
    # is the whole cost of the post-quantum swap on this path.
    assert (
        push.SUITES[push.SUITE_X25519].overhead
        + 1088
        == push.SUITES[push.SUITE_XWING].overhead
    )
    assert push.MAX_PLAINTEXT_BYTES == push.max_plaintext_bytes(
        push.SUITE_X25519
    )


def test_unknown_suite_is_named_not_guessed():
    with pytest.raises(push.PushError) as excinfo:
        push.suite_or_error("rot13")
    # The message lists what this daemon does know, so an operator
    # pairing a newer app build learns which side is behind.
    assert "rot13" in str(excinfo.value)
    assert push.SUITE_X25519 in str(excinfo.value)
    # Present-but-empty is malformed input, not "unspecified": the relay
    # rejects it too, and the two sides must agree on what is a suite.
    with pytest.raises(push.PushError):
        push.suite_or_error("")


def test_absent_suite_reads_as_x25519():
    # A record that names no suite is an X25519 pairing.
    assert push.suite_or_error(None).name == push.SUITE_X25519
    assert push.public_device({"id": "d", "publicKey": None})["suite"] == (
        push.SUITE_X25519
    )


def test_public_device_flags_a_suite_this_node_cannot_seal(monkeypatch):
    # The registry is shared across nodes while the libraries are per
    # node, so the fail-closed pairing gate cannot keep an unsealable
    # record out of a node that did not take the pairing. The listing is
    # where the operator sees it: without the flag the row reads healthy
    # on the node that is silently dropping its alerts.
    record = {"id": "d", "publicKey": None, "suite": push.SUITE_XWING}
    monkeypatch.setattr(push, "sealable_suites", lambda: [push.SUITE_X25519])
    assert push.public_device(record)["sealableHere"] is False
    # x25519 is sealable wherever the daemon started at all: PyNaCl is a
    # start-refusing gate.
    assert push.public_device({"id": "d", "publicKey": None})[
        "sealableHere"
    ] is True


async def test_refresh_warns_once_about_records_it_cannot_seal(
    tmp_path, caplog, monkeypatch
):
    # A node that cannot seal a stored record drops every alert to that
    # device with one logger.error per alert and nothing that names the
    # cause. The mirror load is where it gets said out loud, once per
    # change of the stranded set rather than once per refresh.
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    service = _service(store)
    await store.upsert(
        {
            "id": "dev-1",
            "name": "phone",
            "publicKey": "k",
            "suite": push.SUITE_XWING,
            "pushToken": "tok",
        }
    )
    monkeypatch.setattr(push, "sealable_suites", lambda: [push.SUITE_X25519])
    caplog.set_level(logging.INFO, logger="cronstable")

    await service.refresh(force=True)
    warnings = [
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert len(warnings) == 1
    assert "cannot seal" in warnings[0]
    assert "phone (dev-1, xwing)" in warnings[0]
    assert "push-pq" in warnings[0]

    # a standing mismatch does not re-warn on every refresh
    caplog.clear()
    await service.refresh(force=True)
    assert not [
        r for r in caplog.records if r.levelno >= logging.WARNING
    ]

    # ... and recovering the capability says so, so an operator watching
    # the log sees the mismatch close
    caplog.clear()
    monkeypatch.setattr(
        push, "sealable_suites", lambda: [push.SUITE_X25519, push.SUITE_XWING]
    )
    await service.refresh(force=True)
    assert any(
        "every paired device uses a suite this node can seal"
        in r.getMessage()
        for r in caplog.records
    )


async def test_revoking_the_last_stranded_record_closes_the_mismatch(
    tmp_path, caplog, monkeypatch
):
    # Revoking a stranded device is the operator acting on the warning,
    # so the log confirms it there rather than up to a mirror refresh
    # later, when the connection to what they just did is gone.
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    service = _service(store)
    await store.upsert(
        {
            "id": "dev-1",
            "name": "phone",
            "publicKey": "k",
            "suite": push.SUITE_XWING,
            "pushToken": "tok",
        }
    )
    monkeypatch.setattr(push, "sealable_suites", lambda: [push.SUITE_X25519])
    await service.refresh(force=True)

    caplog.clear()
    caplog.set_level(logging.INFO, logger="cronstable")
    await service.revoke("dev-1")
    assert any(
        "every paired device uses a suite this node can seal"
        in r.getMessage()
        for r in caplog.records
    )


@requires_pynacl
def test_pairing_accepts_an_explicit_x25519_suite():
    _, public_b64 = _device_keypair()
    fields = push.validate_pairing(
        {
            "name": "phone",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok",
            "suite": push.SUITE_X25519,
        }
    )
    assert fields["suite"] == push.SUITE_X25519


@requires_xwing
async def test_pairing_accepts_an_xwing_device_and_stores_its_suite(
    tmp_path,
):
    # The pairing probe seals through the real HPKE path, so a generated
    # key passes validation and the stored record names its suite for
    # every later seal and listing.
    _, public_b64 = _xwing_keypair()
    fields = push.validate_pairing(
        {
            "name": "pq-phone",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-pq",
            "suite": push.SUITE_XWING,
        }
    )
    assert fields["suite"] == push.SUITE_XWING
    service = _service(push.FileDeviceStore(str(tmp_path / "d.json")))
    record, created = await service.pair(fields, "authToken")
    assert created
    assert record["suite"] == push.SUITE_XWING
    (listed,) = service.devices_payload()
    assert listed["suite"] == push.SUITE_XWING
    assert listed["fingerprint"] == push.key_fingerprint(public_b64)


def test_sealable_suites_advertises_only_a_proven_seal(monkeypatch):
    # A findable-but-broken cryptography (an OpenSSL without ML-KEM, a
    # half-installed wheel) must not be advertised: the app
    # steers every fresh pairing by this list, and an advertised suite
    # the daemon cannot seal would turn each of those pairings into a
    # 400 with no client-side fallback.  The advertisement therefore
    # rides a real probe seal, and its verdict is cached so the broken
    # import is not retried on every /whoami.  Sealing never consults
    # the cache (seal_to_device builds its suite per seal), so the
    # verdict costs an advertisement and never an alert.
    monkeypatch.setitem(
        push.SUITES,
        push.SUITE_XWING,
        push._Suite(push.SUITE_XWING, 1216, 1136, True),
    )
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    calls = []

    def broken():
        calls.append(True)
        raise push.PushError("cryptography cannot seal X-Wing")

    monkeypatch.setattr(push, "_xwing_sealer", broken)
    assert push.sealable_suites() == [push.SUITE_X25519]
    assert push.sealable_suites() == [push.SUITE_X25519]
    assert len(calls) == 1


@requires_xwing
def test_a_backend_without_mlkem_is_blamed_on_the_library_not_the_key(
    monkeypatch, caplog
):
    # A cryptography whose OpenSSL lacks ML-KEM imports and builds its
    # HPKE suite fine; the refusal comes out of the key builder as
    # UnsupportedAlgorithm (the 47.0.0 wheel does exactly this). Pairing
    # and sealing must answer with the fixed library sentence, never the
    # "unusable key" 400 that would send an operator debugging the phone.
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives.asymmetric import mlkem

    def unsupported(_data):
        raise UnsupportedAlgorithm(
            "ML-KEM-768 is not supported by this backend."
        )

    monkeypatch.setattr(
        mlkem,
        "MLKEM768PublicKey",
        SimpleNamespace(from_public_bytes=unsupported),
    )
    _, public_b64 = _xwing_keypair()
    caplog.set_level(logging.WARNING, logger="cronstable")
    with pytest.raises(push.PushError) as excinfo:
        push.validate_public_key(public_b64, push.SUITE_XWING)
    assert "cannot seal X-Wing" in str(excinfo.value)
    assert "push-pq" in str(excinfo.value)
    with pytest.raises(push.PushError) as excinfo:
        push.seal_to_device(public_b64, b"{}", push.SUITE_XWING)
    assert "cannot seal X-Wing" in str(excinfo.value)
    assert "unusable" not in str(excinfo.value)
    # the probe stands down for the same reason, in the same words
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    assert push.sealable_suites() == [push.SUITE_X25519]
    reasons = [
        r.getMessage()
        for r in caplog.records
        if "cannot seal X-Wing" in r.getMessage()
    ]
    assert len(reasons) == 3
    assert all("not supported by this backend" in m for m in reasons)


@requires_xwing
async def test_start_warms_the_xwing_probe_off_the_loop(
    tmp_path, monkeypatch
):
    # The cold probe is an ML-KEM keygen plus a seal; start() pays for it
    # on a thread once, so the capability line, the mirror's stranded-set
    # check and the first /whoami all read the cache on the loop.
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    hops = []
    real_to_thread = asyncio.to_thread

    async def watched(func, *args, **kwargs):
        hops.append(func)
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(push.asyncio, "to_thread", watched)
    service = _service(push.FileDeviceStore(str(tmp_path / "devices.json")))
    await service.start()
    assert hops.count(push.sealable_suites) == 1
    assert push._XWING_PROBE is True


@requires_xwing
def test_sealable_suites_advertises_a_proven_xwing_seal(monkeypatch):
    # The healthy arm of the probe: a fresh (uncached) probe seal
    # succeeds through the real library and xwing is advertised.
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    assert push.sealable_suites() == [push.SUITE_X25519, push.SUITE_XWING]


@requires_xwing
async def test_sealable_suites_async_answers_a_cold_probe_off_the_loop(
    monkeypatch,
):
    # /whoami serves this on every daemon running the web app, including
    # one with no push section, where no PushService warmed the probe at
    # start-up. The cold answer costs an ML-KEM keygen, which belongs on
    # a thread rather than on the event loop.
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    loop_threads = []
    real_to_thread = asyncio.to_thread

    async def watched(func, *args, **kwargs):
        loop_threads.append(func)
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(push.asyncio, "to_thread", watched)
    assert await push.sealable_suites_async() == [
        push.SUITE_X25519,
        push.SUITE_XWING,
    ]
    assert loop_threads == [push.sealable_suites]
    # warm, it answers inline: no thread hop per request
    loop_threads.clear()
    assert await push.sealable_suites_async() == [
        push.SUITE_X25519,
        push.SUITE_XWING,
    ]
    assert loop_threads == []


@requires_xwing
def test_an_xwing_seal_is_not_blamed_on_a_missing_pynacl(monkeypatch):
    # The PyNaCl gate belongs to the x25519 arm. Sitting ahead of the
    # dispatch it answered for both, so an X-Wing seal on a daemon
    # without PyNaCl reported the wrong library to install.
    monkeypatch.setattr(push, "HAVE_PYNACL", False)
    _, public_b64 = _xwing_keypair()
    sealed = push.seal_to_device(public_b64, b"hi", push.SUITE_XWING)
    assert base64.b64decode(sealed)
    with pytest.raises(push.PushError) as excinfo:
        push.seal_to_device(
            base64.b64encode(b"\x01" * 32).decode(), b"hi", push.SUITE_X25519
        )
    assert "PyNaCl" in str(excinfo.value)


def test_capability_log_separates_an_old_cryptography_from_none(
    monkeypatch, caplog
):
    # The `push` extra is a start-refusing gate, so a daemon that reaches
    # start() always seals x25519. `push-pq` is not: on a platform with
    # no cryptography wheel the extra installs PyNaCl alone, which costs
    # no page and therefore no ConfigError. Start-up is where an operator
    # who asked for post-quantum sealing and did not get it finds out, so
    # the capability line is pinned in every direction it can take.
    # A platform without the library: the module-level flag and the
    # suite's `sealable` bit are one fact read at import, so both move.
    monkeypatch.setattr(push, "HAVE_XWING", False)
    monkeypatch.setitem(
        push.SUITES,
        push.SUITE_XWING,
        push._Suite(push.SUITE_XWING, 1216, 1136, False),
    )
    monkeypatch.setattr(push, "_XWING_PROBE", None)
    caplog.set_level(logging.INFO, logger="cronstable")

    # Two different remedies: one platform has no wheel at all, the
    # other installed a cryptography predating X-Wing. Calling the
    # second absent sends an operator looking for a package that is
    # already there. Both readings of the module lookup are driven from
    # here, so each arm is pinned on every box rather than one arm per
    # box.
    def findable(spec):
        monkeypatch.setattr(
            push,
            "importlib",
            SimpleNamespace(util=SimpleNamespace(find_spec=lambda _: spec)),
        )

    findable(object())
    push.PushService._log_sealing_capability()
    messages = [r.getMessage() for r in caplog.records]
    # Whole-line equality: "x25519, xwing" contains "x25519", so a
    # substring match passes on exactly the advertisement of a suite the
    # daemon cannot seal that the probe exists to keep out.
    assert any("push: sealing suites: x25519" == m for m in messages)
    reason = [m for m in messages if "post-quantum xwing sealing is off" in m]
    assert reason and "push-pq" in reason[0]
    assert "too old" in reason[0]
    assert "no cryptography" not in reason[0]

    caplog.clear()
    findable(None)
    push.PushService._log_sealing_capability()
    reason = [
        r.getMessage()
        for r in caplog.records
        if "post-quantum xwing sealing is off" in r.getMessage()
    ]
    assert reason and "no cryptography" in reason[0]
    assert "too old" not in reason[0]

    # ... and with a working install the offer line stays away entirely.
    caplog.clear()
    monkeypatch.setattr(push, "HAVE_XWING", True)
    monkeypatch.setattr(push, "_XWING_PROBE", True)
    monkeypatch.setitem(
        push.SUITES,
        push.SUITE_XWING,
        push._Suite(push.SUITE_XWING, 1216, 1136, True),
    )
    push.PushService._log_sealing_capability()
    messages = [r.getMessage() for r in caplog.records]
    assert any("push: sealing suites: x25519, xwing" == m for m in messages)
    assert not any("sealing is off" in m for m in messages)


def test_xwing_sealable_flag_is_wired_to_the_real_import():
    # The refusal tests below monkeypatch the flag for determinism, so
    # the wiring needs its own pin: xwing's ``sealable`` bit is the
    # import probe itself, and a hardcoded True would make
    # validate_public_key accept pairings a library-less daemon can
    # never seal (its probe seal is HAVE_XWING-gated too, so nothing
    # else on the pairing path would catch it).
    assert push.SUITES[push.SUITE_XWING].sealable == push.HAVE_XWING
    assert push.SUITES[push.SUITE_X25519].sealable is True


def test_pairing_refuses_a_suite_the_daemon_cannot_seal_to(monkeypatch):
    # Fail closed, exactly like the PyNaCl config gate: accepting a
    # pairing this daemon cannot seal to would store a record that
    # silently fails every later alert (a missed page, the one failure
    # mode this feature exists to prevent).  A daemon without the
    # post-quantum library is in this state for xwing; the monkeypatched
    # flag reproduces it whatever this box has installed.
    monkeypatch.setitem(
        push.SUITES,
        push.SUITE_XWING,
        push._Suite(push.SUITE_XWING, 1216, 1136, False),
    )
    key = base64.b64encode(
        b"\x01" * push.SUITES[push.SUITE_XWING].public_key_bytes
    ).decode()
    with pytest.raises(push.PushError) as excinfo:
        push.validate_pairing(
            {
                "name": "phone",
                "platform": "ios",
                "publicKey": key,
                "pushToken": "tok",
                "suite": push.SUITE_XWING,
            }
        )
    assert "not sealable" in str(excinfo.value)
    # the refusal names what would lift it
    assert "push-pq" in str(excinfo.value)


def test_pairing_checks_key_length_against_its_own_suite():
    # A 32-byte key is correct for x25519 and wrong for xwing; the check
    # must follow the suite rather than a single hard-coded 32.
    short = base64.b64encode(b"\x01" * 32).decode()
    with pytest.raises(push.PushError) as excinfo:
        push.validate_public_key(short, push.SUITE_XWING)
    assert "1216" in str(excinfo.value)
    with pytest.raises(push.PushError) as excinfo:
        push.validate_pairing(
            {
                "name": "p",
                "platform": "ios",
                "pushToken": "t",
                "publicKey": short,
                "suite": 7,
            }
        )
    assert "suite must be a string" in str(excinfo.value)


@requires_xwing
def test_xwing_seal_round_trip():
    private, public_b64 = _xwing_keypair()
    ciphertext = push.seal_to_device(
        public_b64, b'{"hello": "world"}', push.SUITE_XWING
    )
    assert _open_xwing(ciphertext, private) == {"hello": "world"}


@requires_xwing
def test_a_wrong_length_xwing_key_fails_inside_the_seal():
    # The X-Wing encapsulation sits inside the seal's try for the same
    # reason the x25519 arm's encrypt does: a key that decodes but is
    # the wrong length is refused by the library rather than by the
    # decode, and that refusal must come back as one per-device
    # PushError instead of escaping a whole-fleet fan-out.
    with pytest.raises(push.PushError) as excinfo:
        push.seal_to_device(
            base64.b64encode(b"\x01" * 64).decode(), b"{}", push.SUITE_XWING
        )
    assert "unusable" in str(excinfo.value)


def test_seal_rejects_a_suite_with_no_implementation(monkeypatch):
    # The not-sealable branch stays live even though pairing refuses the
    # suite first: an older build or another tool can have written the
    # record, and the refusal must be a per-device PushError naming the
    # fix, never a fan-out escapee.
    monkeypatch.setitem(
        push.SUITES,
        push.SUITE_XWING,
        push._Suite(push.SUITE_XWING, 1216, 1136, False),
    )
    key = base64.b64encode(
        b"\x01" * push.SUITES[push.SUITE_XWING].public_key_bytes
    ).decode()
    with pytest.raises(push.PushError) as excinfo:
        push.seal_to_device(key, b"{}", push.SUITE_XWING)
    assert "cannot seal" in str(excinfo.value)
    assert "push-pq" in str(excinfo.value)


def test_fit_payload_honors_a_narrower_suite_budget():
    # The same alert, fitted twice: the narrower budget keeps fewer log
    # lines and both stay inside their own cap.
    def payload():
        return {
            "v": 1,
            "kind": "failure",
            "name": "etl",
            "host": "node-a",
            "log_tail": ["line {}".format(i) * 6 for i in range(40)],
        }

    wide = payload()
    narrow = payload()
    wide_bytes = push.fit_payload(
        wide, push.max_plaintext_bytes(push.SUITE_X25519)
    )
    narrow_bytes = push.fit_payload(
        narrow, push.max_plaintext_bytes(push.SUITE_XWING)
    )
    assert len(wide_bytes) <= push.max_plaintext_bytes(push.SUITE_X25519)
    assert len(narrow_bytes) <= push.max_plaintext_bytes(push.SUITE_XWING)
    assert len(narrow["log_tail"]) < len(wide["log_tail"])
    # Both keep the newest lines: trimming is oldest-first regardless of
    # how hard the budget bites.
    assert narrow["log_tail"][-1] == wide["log_tail"][-1]


def test_fit_for_does_not_leak_one_devices_trimming_into_another(tmp_path):
    # The regression per-device fitting could have introduced: fit_payload
    # trims in place, so a shared dict (or a shared log_tail list behind a
    # shallow copy) would let the narrowest device decide what every other
    # device sees.
    service = _service(push.FileDeviceStore(str(tmp_path / "d.json")))
    shared = {
        "v": 1,
        "kind": "failure",
        "name": "etl",
        "host": "node-a",
        "log_tail": ["line {}".format(i) * 6 for i in range(40)],
    }
    before = len(shared["log_tail"])
    narrow = service._fit_for(shared, push.SUITE_XWING)
    wide = service._fit_for(shared, push.SUITE_X25519)
    # The caller's payload is untouched by either fitting...
    assert len(shared["log_tail"]) == before
    # ...and the wide device is not charged for the narrow one's trimming.
    assert len(wide) > len(narrow)


def test_collapse_id_is_unchanged_by_fitting():
    # collapse_id hashes run_id, and fit_payload's last resort drops it,
    # so the id has to come off the untrimmed payload: otherwise an
    # oversized alert coalesces under a different id than the same alert
    # from a node whose copy fits, and per-device fitting widens that to
    # one id per suite.
    # The identity core is never trimmed, so it takes a pathologically
    # long job name to push the fit loop all the way down its context
    # list to run_id -- the shape where the coupling would show.
    payload = {
        "v": 1,
        "kind": "failure",
        "name": "etl-" + "n" * push.MAX_PLAINTEXT_BYTES,
        "host": "node-a",
        "run_id": "run-123",
        "schedule": "17 2 * * *",
        "started_at": "2026-08-17T02:14:02+00:00",
    }
    expected = push.collapse_id(payload, "salt")
    trimmed = dict(payload)
    push.fit_payload(trimmed)
    # The trimmed copy really did lose run_id, so the assertion below is
    # live rather than vacuous.
    assert "run_id" not in trimmed
    assert push.collapse_id(payload, "salt") == expected
    # The point: the id the relay coalesces on is the same either way.
    assert push.collapse_id(trimmed, "salt") != expected


# ---------------------------------------------------------- device stores


async def test_file_store_round_trip(tmp_path):
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    assert await store.load() == []
    await store.upsert({"id": "d1", "name": "phone"})
    await store.upsert({"id": "d2", "name": "pad"})
    await store.upsert({"id": "d1", "name": "phone-renamed"})
    loaded = {d["id"]: d for d in await store.load()}
    assert loaded["d1"]["name"] == "phone-renamed"
    assert await store.remove("d2") is True
    assert await store.remove("d2") is False
    assert [d["id"] for d in await store.load()] == ["d1"]


async def test_file_store_corrupt_file_refuses_reads_and_writes(tmp_path):
    path = tmp_path / "devices.json"
    path.write_text("{not json", encoding="utf-8")
    store = push.FileDeviceStore(str(path))
    with pytest.raises(push.PushError):
        await store.load()
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d1"})
    # the corrupt bytes are preserved for hand recovery
    assert path.read_text(encoding="utf-8") == "{not json"


async def test_file_store_removing_a_corrupt_file_restores_writes(tmp_path):
    # The write-refusal message tells the operator to "fix or remove it
    # first"; following it must actually work. The corrupt flag used to
    # outlive the removal (only a successful PARSE cleared it, which a
    # now-missing file can never produce), and the store object lives as
    # long as the push config is unchanged, so every later write kept
    # refusing with the same instruction until a daemon restart.
    path = tmp_path / "devices.json"
    path.write_text("{not json", encoding="utf-8")
    store = push.FileDeviceStore(str(path))
    with pytest.raises(push.PushError):
        await store.load()
    os.unlink(path)
    await store.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await store.load()] == ["d1"]


async def test_file_store_write_failure_is_push_error(tmp_path):
    # A missing (or read-only) directory is store trouble, and the
    # bounded contract says store trouble is PushError: the pairing
    # handler turns that into its documented 503, never an aiohttp 500.
    store = push.FileDeviceStore(str(tmp_path / "no-such-dir" / "d.json"))
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d1"})
    with pytest.raises(push.PushError):
        await store.ensure_salt()


async def test_file_store_runs_on_a_private_daemon_thread(tmp_path):
    # Registry I/O used to ride loop.run_in_executor(None, ...): the same
    # regression the lifecycle-hardening tests in tests/test_state.py guard
    # state against. The default pool is shared with the once-a-minute config
    # reload and its workers are non-daemonic, so an op abandoned on a
    # wedged mount retired one worker per attempt until the reload had no
    # thread to run on, and interpreter exit hung joining them.
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    seen = {}
    real_read = store._read

    def spy():
        thread = threading.current_thread()
        seen["daemon"] = thread.daemon
        seen["name"] = thread.name
        return real_read()

    store._read = spy  # type: ignore[method-assign]
    assert await store.load() == []
    assert seen["daemon"] is True
    assert seen["name"].startswith("cronstable-push")


async def test_file_store_temp_file_is_unique_and_exclusive(
    tmp_path, monkeypatch
):
    # A fixed ".tmp" opened O_TRUNC is a shared mutable file: two writers
    # interleave into one buffer and then rename each other's half-written
    # bytes over the registry, which _read refuses to parse and _write then
    # refuses to repair. Unique name plus O_EXCL, like state._atomic_write.
    path = tmp_path / "devices.json"
    store = push.FileDeviceStore(str(path))
    seen = []
    real_open = os.open

    def spy(target, flags, *args, **kwargs):
        if str(target).startswith(str(path)) and str(target).endswith(".tmp"):
            seen.append((str(target), flags))
        return real_open(target, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", spy)
    await store.upsert({"id": "d1"})
    await store.upsert({"id": "d2"})
    assert len(seen) == 2
    names = {name for name, _ in seen}
    assert len(names) == 2  # never twice the same temp file
    assert str(path) + ".tmp" not in names  # and never the fixed name
    for _, flags in seen:
        assert flags & os.O_EXCL  # a collision is an error, not a join
    # nothing is left behind once the renames are done
    assert [p.name for p in tmp_path.iterdir()] == ["devices.json"]


async def test_file_store_abandoned_op_cannot_interleave(tmp_path):
    # An op that times out leaves its worker mid read-modify-write while
    # the loop-side lock is released, so the next op used to run against
    # the file the abandoned worker was about to overwrite: one of the two
    # pairings simply vanished. The worker-held fence makes the successor
    # wait for the file, not for its caller.
    path = tmp_path / "devices.json"
    store = push.FileDeviceStore(str(path))
    entered = threading.Event()
    release = threading.Event()
    real_write = store._write
    writes = []

    def slow_write(devices):
        writes.append([d["id"] for d in devices])
        if len(writes) == 1:
            entered.set()
            release.wait(timeout=30)  # released below; the bound is a net
        return real_write(devices)

    store._write = slow_write  # type: ignore[method-assign]

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(store.upsert({"id": "d1"}), timeout=0.3)
    assert entered.is_set()  # the abandoned worker really is mid-write
    second = asyncio.create_task(store.upsert({"id": "d2"}))
    await asyncio.sleep(0.3)  # every chance to interleave, if it could
    release.set()
    await second
    # the second write saw the first one's result instead of racing it
    assert writes == [["d1"], ["d1", "d2"]]
    assert sorted(d["id"] for d in await store.load()) == ["d1", "d2"]


async def test_file_store_sweeps_temps_an_earlier_process_abandoned(
    tmp_path,
):
    # Unique temp names are what keep two writers out of one buffer, but
    # nothing reuses the one a process killed mid-write leaves behind, so
    # without a sweep they accrete one per event beside the registry.
    path = tmp_path / "devices.json"
    store = push.FileDeviceStore(str(path))
    orphan = tmp_path / "devices.json.999-deadbeef.tmp"
    orphan.write_text("half a write", encoding="utf-8")
    os.utime(orphan, (0, time.time() - push.TMP_MAX_AGE - 60))
    # a temp from a write still in flight is far too young to touch
    fresh = tmp_path / "devices.json.998-cafebabe.tmp"
    fresh.write_text("in flight", encoding="utf-8")
    # and an unrelated neighbour is never a candidate
    other = tmp_path / "notes.txt"
    other.write_text("keep me", encoding="utf-8")

    await store.upsert({"id": "d1"})

    assert not orphan.exists()
    assert fresh.exists()
    assert other.exists()
    assert [d["id"] for d in await store.load()] == ["d1"]


async def test_file_store_fence_refuses_rather_than_waiting_forever(
    tmp_path, monkeypatch
):
    # The fence is only ever contended after an earlier op already blew
    # its own budget, so the successor waits a bounded slice and then
    # says what is actually wrong instead of burning the rest of its
    # time on a predecessor that is wedged by definition.
    monkeypatch.setattr(push, "STORE_LOCK_WAIT", 0.05)
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))
    held = threading.Event()

    def hold():
        store._file_lock.acquire()
        held.set()

    threading.Thread(target=hold, daemon=True).start()
    await asyncio.get_running_loop().run_in_executor(None, held.wait, 5)
    try:
        with pytest.raises(push.PushError, match="an earlier push devices"):
            await store.load()
    finally:
        store._file_lock.release()


async def test_file_store_wraps_an_unexpected_error_as_push_error(tmp_path):
    # The twin of test_state_store_unreadable_document_is_push_error: the
    # file store owes callers the same bounded contract for an exception
    # its read/write paths do not model (the backend refusing to start a
    # worker thread, say), or one escapee takes the rest of the
    # housekeeping pass with it.
    store = push.FileDeviceStore(str(tmp_path / "devices.json"))

    def boom():
        raise RuntimeError("could not start a worker thread")

    store._read = boom  # type: ignore[method-assign]
    with pytest.raises(push.PushError, match="RuntimeError"):
        await store.load()


async def test_file_store_fence_is_per_path_not_per_instance(tmp_path):
    # Cron rebuilds the store on every push-config change, so a fence
    # owned by the store object would not cover the worker an earlier
    # instance abandoned. A reload is exactly when someone is most likely
    # to be poking at a wedged install.
    path = str(tmp_path / "devices.json")
    first = push.FileDeviceStore(path)
    second = push.FileDeviceStore(path)
    assert first._file_lock is second._file_lock
    # a different registry file gets its own fence
    other = push.FileDeviceStore(str(tmp_path / "other.json"))
    assert other._file_lock is not first._file_lock


async def test_file_store_salt_created_once_and_preserved(tmp_path):
    path = tmp_path / "devices.json"
    store = push.FileDeviceStore(str(path))
    salt = await store.ensure_salt()
    assert salt and salt == await store.ensure_salt()
    # rewrites preserve it; a fresh store instance reads the same salt
    await store.upsert({"id": "d1", "name": "phone"})
    again = push.FileDeviceStore(str(path))
    assert await again.ensure_salt() == salt
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["collapseSalt"] == salt
    assert [d["id"] for d in doc["devices"]] == ["d1"]


class _FakeStateBackend:
    """The document slice of the state backend the registry uses.

    Documents are keyed (namespace, key) and DOC_KEEP/DOC_DELETE honored,
    matching the real mutate_document contract the salt path relies on.
    """

    def __init__(self) -> None:
        self.docs: dict[Any, dict[str, Any]] = {}

    async def list_documents(self, namespace):
        assert namespace == push.PUSH_DOC_NAMESPACE
        return [
            doc for (ns, _key), doc in self.docs.items() if ns == namespace
        ]

    async def mutate_document(self, namespace, key, transform):
        from cronstable.state import DOC_DELETE, DOC_KEEP

        current = self.docs.get((namespace, key))
        new, result = transform(current)
        if new is DOC_DELETE:
            self.docs.pop((namespace, key), None)
            return None, result
        if new is DOC_KEEP:
            return current, result
        self.docs[(namespace, key)] = new
        return new, result

    async def delete_document(self, namespace, key):
        return self.docs.pop((namespace, key), None) is not None


class _BrokenStateBackend:
    """A backend whose I/O fails raw, like an NFS ESTALE mid-operation."""

    async def list_documents(self, namespace):
        raise OSError("stale file handle")

    async def mutate_document(self, namespace, key, transform):
        raise OSError("stale file handle")

    async def delete_document(self, namespace, key):
        raise OSError("stale file handle")


async def test_state_store_round_trip_and_backend_loss():
    backend: list[Any] = [_FakeStateBackend()]
    store = push.StateDeviceStore(lambda: backend[0])
    await store.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await store.load()] == ["d1"]
    assert await store.remove("d1") is True
    assert await store.remove("d1") is False
    backend[0] = None  # the backend went away (reload, outage)
    with pytest.raises(push.PushError):
        await store.load()
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d2"})


async def test_state_store_oserror_is_push_error():
    # Raw backend I/O errors must be normalized to PushError: send_report
    # and the handlers catch exactly that, and anything rawer would either
    # 500 a pairing request or drop an alert that the last-known mirror
    # could still deliver.
    store = push.StateDeviceStore(lambda: _BrokenStateBackend())
    with pytest.raises(push.PushError):
        await store.load()
    with pytest.raises(push.PushError):
        await store.upsert({"id": "d1"})
    with pytest.raises(push.PushError):
        await store.remove("d1")
    with pytest.raises(push.PushError):
        await store.ensure_salt()


class _UnreadableDocBackend:
    """A backend that fails a document read the way the real one does.

    ``cronstable.state`` raises its own private ``_DocumentUnreadable``
    (a plain Exception, not an OSError) when a document exists but cannot
    be trusted: corrupt bytes, a torn write, an unknown schema.
    """

    def _boom(self):
        from cronstable.state import _DocumentUnreadable

        raise _DocumentUnreadable("unknown-schema-or-not-a-document")

    async def list_documents(self, namespace):
        self._boom()

    async def mutate_document(self, namespace, key, transform):
        self._boom()

    async def delete_document(self, namespace, key):
        self._boom()


async def test_state_store_unreadable_document_is_push_error():
    # The finding: _bounded normalized OSError only, so a corrupt
    # pushmeta/collapse document escaped the PushError contract, out of
    # start_stop_push and into the housekeeping pass, which then skipped
    # the durable-state manifest and GC on that pass and every later one.
    store = push.StateDeviceStore(lambda: _UnreadableDocBackend())
    with pytest.raises(push.PushError, match="_DocumentUnreadable"):
        await store.load()
    with pytest.raises(push.PushError, match="_DocumentUnreadable"):
        await store.upsert({"id": "d1"})
    with pytest.raises(push.PushError, match="_DocumentUnreadable"):
        await store.remove("d1")
    with pytest.raises(push.PushError, match="_DocumentUnreadable"):
        await store.ensure_salt()


async def test_state_store_salt_converges_and_stays_out_of_listings():
    backend = _FakeStateBackend()
    a = push.StateDeviceStore(lambda: backend)
    b = push.StateDeviceStore(lambda: backend)
    salt = await a.ensure_salt()
    # a second node sharing the store converges on the same salt (the
    # property cross-node coalescing depends on)
    assert await b.ensure_salt() == salt
    # the salt document is not a device: listings never see it, and a
    # revoke by its key cannot delete it (different namespace)
    await a.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await a.load()] == ["d1"]
    assert await a.remove("collapse") is False
    assert await b.ensure_salt() == salt


# ------------------------------------------------------------ PushService


@requires_pynacl
async def test_pair_revoke_and_repair_keeps_identity(tmp_path):
    _, public_b64 = _device_keypair()
    service, record = await _paired_service(tmp_path, "http://x/", public_b64)
    assert record["id"] and record["createdAt"]
    assert record["createdBy"] == "authToken"
    # same public key pairs again: same id/createdAt, fresh token+name
    repaired, created = await service.pair(
        {
            "name": "phone-2",
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-rotated",
        },
        "other",
    )
    assert created is False
    assert repaired["id"] == record["id"]
    assert repaired["createdAt"] == record["createdAt"]
    assert repaired["pushToken"] == "tok-rotated"
    listing = service.devices_payload()
    assert len(listing) == 1
    assert listing[0]["pushToken"].endswith("otated")
    assert listing[0]["pushToken"] != "tok-rotated"  # redacted
    # the listing carries the key fingerprint for the out-of-band
    # comparison against what the companion app displays
    assert listing[0]["fingerprint"] == push.key_fingerprint(public_b64)
    assert await service.revoke(record["id"]) is True
    assert service.devices_payload() == []


@requires_pynacl
async def test_send_report_seals_to_each_device_and_posts_relay(tmp_path):
    private, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        await service.send_report(
            _FakeJobCtx(),
            False,
            {"enabled": True, "priority": "passive", "includeLogTail": True},
        )
        assert len(relay.requests) == 1
        body = relay.requests[0]
        assert body["v"] == push.PUSH_PROTOCOL_VERSION
        assert body["device"] == "tok-abcdef"
        assert body["priority"] == "passive"
        assert body["event"] is False
        assert len(body["collapseId"]) == 32
        # The suite rides on the envelope so the relay bound-checks the
        # ciphertext against the right one and the app knows which key
        # opens it, neither having to infer it from a length.
        assert body["suite"] == push.SUITE_X25519
        opened = _open_sealed(private, body["ciphertext"])
        assert opened["name"] == "backup"
        assert opened["kind"] == "failure"
        assert opened["log_tail"] == ["boom", "worse"]
        # the relay never saw plaintext
        flat = json.dumps(body)
        assert "backup" not in flat and "boom" not in flat


@requires_pynacl
@requires_xwing
async def test_mixed_suite_fanout_fits_each_device_to_its_own_budget(
    tmp_path,
):
    # One registry, both suites: each envelope names its device's own
    # suite and carries a copy fitted to that suite's budget, so the
    # x25519 device keeps log lines the narrower xwing budget trims
    # (the fan-out sibling of the _fit_for isolation test above).
    x_private, x_public = _device_keypair()
    w_private, w_public = _xwing_keypair()
    async with _RelayServer() as relay:
        service, _ = await _paired_service(tmp_path, relay.url, x_public)
        await service.pair(
            {
                "name": "pq-phone",
                "platform": "ios",
                "publicKey": w_public,
                "pushToken": "tok-pq",
                "suite": push.SUITE_XWING,
            },
            "authToken",
        )
        ctx, lines = _tail_ctx(count=200, width=80)
        await service.send_report(ctx, False, {"enabled": True})
    by_suite = {r["suite"]: r for r in relay.requests}
    assert set(by_suite) == {push.SUITE_X25519, push.SUITE_XWING}
    assert by_suite[push.SUITE_XWING]["device"] == "tok-pq"
    # the xwing copy fits its 1714-byte budget as sealed
    narrow_budget = push.max_plaintext_bytes(push.SUITE_XWING)
    assert narrow_budget == 1714
    sealed = base64.b64decode(by_suite[push.SUITE_XWING]["ciphertext"])
    overhead = push.SUITES[push.SUITE_XWING].overhead
    assert len(sealed) - overhead <= narrow_budget
    wide = _open_sealed(x_private, by_suite[push.SUITE_X25519]["ciphertext"])
    narrow = _open_xwing(by_suite[push.SUITE_XWING]["ciphertext"], w_private)
    assert len(wide["log_tail"]) > len(narrow["log_tail"])
    # trimming stays oldest-first for both: the newest line survives
    assert wide["log_tail"][-1] == lines[-1]
    assert narrow["log_tail"][-1] == lines[-1]


@requires_pynacl
async def test_collapse_id_comes_from_the_persistent_salt(tmp_path):
    # The relay coalesces the same (job, run) across nodes and restarts
    # by collapseId, which only holds if every service over one registry
    # keys ids with the SAME persisted salt. Nothing else pins the
    # service-to-store salt wiring: with it broken, ids silently fall
    # back to the per-process _local_salt, every other test stays green
    # (they check only the id's length), and a fleet re-pages each alert
    # once per node and per restart.
    private, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        service, _ = await _paired_service(tmp_path, relay.url, public_b64)
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
        # a second service over the same registry: a restart, or another
        # node sharing the devices file
        twin = _service(
            push.FileDeviceStore(str(tmp_path / "devices.json")), relay.url
        )
        await twin.send_report(_FakeJobCtx(), False, {"enabled": True})
        first, second = relay.requests
        assert first["collapseId"] == second["collapseId"]
        # and the id is exactly the persisted salt keyed over the sealed
        # payload's identity fields, not any process-local fallback
        doc = json.loads(
            (tmp_path / "devices.json").read_text(encoding="utf-8")
        )
        opened = _open_sealed(private, first["ciphertext"])
        assert first["collapseId"] == push.collapse_id(
            opened, doc["collapseSalt"]
        )


def _alert_over_the_floor() -> dict[str, Any]:
    # Sealed under x25519 this fits CIPHERTEXT_B64_MAX only after trimming,
    # and what survives is still well over CIPHERTEXT_B64_FLOOR: the shape
    # a relay enforcing the floor answers 400 to.
    return {
        "v": 1,
        "kind": "failure",
        "name": "etl",
        "host": "node-a",
        "log_tail": ["line {} ".format(i) * 8 for i in range(40)],
    }


@requires_pynacl
async def test_send_refits_to_a_relay_enforcing_the_floor_cap(
    tmp_path, caplog
):
    # A daemon released ahead of its relay must not bounce the alerts
    # carrying the most log tail -- the failures -- on the channel whose
    # job is to page someone.  Fitted to the floor, the page still lands.
    private, public_b64 = _device_keypair()
    async with _FloorCapRelayServer() as relay:
        service, _ = await _paired_service(tmp_path, relay.url, public_b64)
        payload = _alert_over_the_floor()
        outcomes = await service._send_payload(
            payload, priority="time-sensitive"
        )
        # a second alert: the warning is once per process, the re-fit is
        # per alert
        await service._send_payload(
            _alert_over_the_floor(), priority="time-sensitive"
        )
    first, second, third, fourth = relay.requests
    assert len(first["ciphertext"]) > push.CIPHERTEXT_B64_FLOOR
    assert len(second["ciphertext"]) <= push.CIPHERTEXT_B64_FLOOR
    assert len(fourth["ciphertext"]) <= push.CIPHERTEXT_B64_FLOOR
    assert outcomes[0]["status"] == 202
    assert outcomes[0]["error"] is None
    # Same alert, same id: the relay coalesces the two posts as one.
    assert first["collapseId"] == second["collapseId"]
    opened = _open_sealed(private, second["ciphertext"])
    assert opened["name"] == "etl"
    # Trimmed oldest-first like any other fit: the newest line survives.
    assert opened["log_tail"][-1] == payload["log_tail"][-1]
    assert len(opened["log_tail"]) < len(payload["log_tail"])
    warnings = [
        r for r in caplog.records if "caps ciphertexts" in r.message
    ]
    assert len(warnings) == 1


@requires_pynacl
async def test_send_does_not_refit_for_a_400_that_is_not_about_size(tmp_path):
    # Any other 400 (a malformed token, a bad collapse id) is final: a
    # second post fitted smaller would change nothing.
    _, public_b64 = _device_keypair()
    async with _RelayServer(status=400) as relay:
        service, _ = await _paired_service(tmp_path, relay.url, public_b64)
        outcomes = await service._send_payload(
            _alert_over_the_floor(), priority="passive"
        )
    assert len(relay.requests) == 1
    assert outcomes[0]["status"] == 400


async def test_send_report_with_no_devices_logs_and_returns(tmp_path, caplog):
    service = _service(push.FileDeviceStore(str(tmp_path / "d.json")))
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    assert any("no device is paired" in r.message for r in caplog.records)


@requires_pynacl
async def test_send_test_reports_relay_failure(tmp_path):
    private, public_b64 = _device_keypair()
    async with _RelayServer(status=429) as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        outcome = await service.send_test(record)
        assert outcome["status"] == 429
        assert "429" in outcome["error"]
        opened = _open_sealed(private, relay.requests[0]["ciphertext"])
        assert opened["kind"] == "test"


@requires_pynacl
async def test_send_report_survives_unreachable_relay(tmp_path):
    _, public_b64 = _device_keypair()
    service, _ = await _paired_service(
        # nothing listens on port 9; must log, never raise
        tmp_path,
        "http://127.0.0.1:9/v1/notify",
        public_b64,
    )
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})


@requires_pynacl
async def test_one_bad_registry_key_does_not_break_the_fanout(
    tmp_path, caplog
):
    """The finding-1 regression: a corrupt record must stay per-device.

    An all-zero key pairs nowhere near validate_pairing (it is refused
    there now), but a registry can still hold one: written by an older
    build, another tool, or a compromised companion app. Sealing to it
    raises inside libsodium at encrypt time; that must be one failed
    device outcome, with every other device still paged.
    """
    private, good_b64 = _device_keypair()
    zero_b64 = base64.b64encode(b"\x00" * 32).decode()
    path = tmp_path / "devices.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "devices": [
                    # the bad device first, so the loop must survive it
                    # to ever reach the good one
                    {
                        "id": "bad",
                        "name": "evil",
                        "platform": "ios",
                        "publicKey": zero_b64,
                        "pushToken": "tok-bad",
                    },
                    {
                        "id": "good",
                        "name": "phone",
                        "platform": "ios",
                        "publicKey": good_b64,
                        "pushToken": "tok-good",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    async with _RelayServer() as relay:
        service = _service(push.FileDeviceStore(str(path)), relay.url)
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
        assert [r["device"] for r in relay.requests] == ["tok-good"]
        opened = _open_sealed(private, relay.requests[0]["ciphertext"])
        assert opened["name"] == "backup"
    # and the drop is said out loud once, naming the device it lost
    failures = [
        r.getMessage()
        for r in caplog.records
        if "delivery to device bad" in r.getMessage()
    ]
    assert len(failures) == 1
    assert "sealing failed" in failures[0]


@requires_pynacl
async def test_send_report_relay_5xx_is_logged_not_raised(tmp_path, caplog):
    _, public_b64 = _device_keypair()
    async with _RelayServer(status=500) as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    assert len(relay.requests) == 1
    assert any(
        "delivery to device" in r.message and "500" in r.getMessage()
        for r in caplog.records
    )


@requires_pynacl
async def test_send_report_degraded_registry_uses_last_known(
    tmp_path, caplog
):
    _, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        # the store goes bad after the mirror warmed: corrupt the file
        # and force the mirror stale so the next send must re-read
        (tmp_path / "devices.json").write_text("{not json", encoding="utf-8")
        service._mirror_fresh_until = 0.0
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
        assert len(relay.requests) == 1  # last-known device still paged
    assert any(
        "registry unavailable" in r.message for r in caplog.records
    )


class _WedgedStore:
    """A registry store that always fails, counting the attempts."""

    kind = "state"

    def __init__(self, delay: float = 0.05) -> None:
        self.delay = delay
        self.loads = 0
        # None keeps it wedged; a list makes load() succeed with it
        self.ok = None

    def describe(self) -> str:
        return "wedged"

    async def load(self):
        self.loads += 1
        await asyncio.sleep(self.delay)
        if self.ok is not None:
            return list(self.ok)
        raise push.PushError("the registry store is wedged")

    async def ensure_salt(self):
        raise push.PushError("the registry store is wedged")


async def test_concurrent_refreshes_pay_one_store_trip():
    # The finding: only a SUCCESSFUL load moved the freshness deadline, so
    # every queued caller woke to a still-stale mirror and started its own
    # STORE_OP_TIMEOUT behind the refresh lock. A burst of alerts against a
    # wedged store multiplied the outage instead of absorbing it, and the
    # last one left minutes late, on a paging channel.
    store = _WedgedStore()
    service = _service(store)
    outcomes = await asyncio.gather(
        *(service.refresh() for _ in range(5)), return_exceptions=True
    )
    assert store.loads == 1
    assert sum(isinstance(o, push.PushError) for o in outcomes) == 1
    assert outcomes.count(None) == 4


async def test_refresh_retries_once_the_backoff_window_passes(monkeypatch):
    # The window is a backoff, not a mute: a store that comes back is
    # visible again within seconds.
    monkeypatch.setattr(push, "REGISTRY_RETRY_SECONDS", 0.0)
    store = _WedgedStore(delay=0.0)
    service = _service(store)
    for _ in range(3):
        with pytest.raises(push.PushError):
            await service.refresh()
    assert store.loads == 3


async def test_failed_forced_refresh_keeps_a_still_good_mirror_window():
    # A failed FORCED refresh must not shorten a positive window: the
    # operator opening the pairing page against a wedged store would
    # otherwise cut a healthy 60-second mirror down to the retry window
    # and push the next alert back onto a store already known to be down.
    store = _WedgedStore(delay=0.0)
    store.ok = [{"id": "d1", "name": "phone"}]
    service = _service(store)
    await service.refresh()  # succeeds, arms the full window
    assert service.get_device("d1") is not None
    good_until = service._mirror_fresh_until
    store.ok = None  # the store wedges
    with pytest.raises(push.PushError):
        await service.refresh(force=True)
    assert service._mirror_fresh_until == good_until
    # so the reporting path still rides the good mirror, no store trip
    loads = store.loads
    await service.refresh()
    assert store.loads == loads


async def test_forced_refresh_ignores_the_backoff_window():
    store = _WedgedStore(delay=0.0)
    service = _service(store)
    with pytest.raises(push.PushError):
        await service.refresh()
    # the reporting path now rides the mirror instead of the store
    await service.refresh()
    assert store.loads == 1
    # an operator listing or pairing must still see the store itself
    with pytest.raises(push.PushError):
        await service.refresh(force=True)
    assert store.loads == 2


async def test_backed_off_alert_names_the_outage_not_the_pairing_page(
    caplog,
):
    # Skipping the read must not skip the diagnosis. Inside the retry
    # window the reporting path never touches the store, so an alert that
    # finds the mirror empty would have reported "no device is paired" and
    # pointed at the pairing endpoint while the real fault is the store.
    store = _WedgedStore(delay=0.0)
    service = _service(store)
    for _ in range(3):
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    assert store.loads == 1  # one attempt covered all three alerts
    messages = [r.getMessage() for r in caplog.records]
    assert sum("registry is unavailable" in m for m in messages) == 3
    assert not any("no device is paired" in m for m in messages)


async def test_recovered_registry_stops_blaming_the_store(caplog):
    # The clearing half of _registry_error. Without it the diagnosis
    # inverts: once a store had failed even once, every later alert with
    # an empty registry would blame a store that is now perfectly fine,
    # and nobody would be told to go pair a device.
    store = _WedgedStore(delay=0.0)
    service = _service(store)
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    assert any(
        "registry is unavailable" in r.getMessage() for r in caplog.records
    )
    caplog.clear()
    store.ok = []  # the store is back, and genuinely holds no pairings
    await service.refresh(force=True)
    await service.send_report(_FakeJobCtx(), False, {"enabled": True})
    messages = [r.getMessage() for r in caplog.records]
    assert any("no device is paired" in m for m in messages)
    assert not any("registry is unavailable" in m for m in messages)


async def _pair_extra(service, index):
    _, public_b64 = _device_keypair()
    await service.pair(
        {
            "name": "phone-{}".format(index),
            "platform": "ios",
            "publicKey": public_b64,
            "pushToken": "tok-{}".format(index),
        },
        "authToken",
    )


@requires_pynacl
async def test_relay_fanout_is_concurrent(tmp_path):
    # The finding: one device at a time meant a relay that holds requests
    # open cost the caller one relay_timeout PER PAIRED DEVICE inside
    # report_failure, which delays the run's retry arming and stretches the
    # shutdown drain (unbounded, precisely because every reporter was
    # assumed to be time-bounded).
    _, public_b64 = _device_keypair()
    async with _BarrierRelayServer(expected=4) as relay:
        service, _ = await _paired_service(tmp_path, relay.url, public_b64)
        for index in range(3):
            await _pair_extra(service, index)
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
        assert len(relay.requests) == 4
        # the relay held all four open at once, which it could only do
        # because the daemon sent them without waiting for an answer
        assert relay.peak == 4


@requires_pynacl
async def test_unexpected_error_stays_one_device_s_problem(tmp_path, caplog):
    # Concurrency must not weaken the per-device isolation this module
    # promises. A leg that raises something the send path does not model
    # (here the relay's error body is not the utf-8 it claims, so reading
    # it raises UnicodeDecodeError) has to become that device's outcome;
    # under gather an escapee would cancel the siblings still in flight.
    _, public_b64 = _device_keypair()
    async with _UndecodableRelayServer() as relay:
        service, _ = await _paired_service(tmp_path, relay.url, public_b64)
        await _pair_extra(service, 0)
        await service.send_report(_FakeJobCtx(), False, {"enabled": True})
        assert len(relay.requests) == 2  # neither device was cancelled
    failures = [
        r.getMessage()
        for r in caplog.records
        if "delivery to device" in r.getMessage()
    ]
    assert len(failures) == 2
    assert all("unexpected UnicodeDecodeError" in m for m in failures)


@requires_pynacl
async def test_send_test_collapse_ids_are_unique(tmp_path):
    _, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        first = await service.send_test(record)
        second = await service.send_test(record)
        assert first["status"] == 200 and second["status"] == 200
    ids = [r["collapseId"] for r in relay.requests]
    # identical payload identity, yet never coalesced away by the relay
    assert len(ids) == 2 and ids[0] != ids[1]


# ------------------------------------------------------------ PushReporter


class _StubService:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def send_report(self, ctx, success, push_config):
        self.calls.append((ctx, success, push_config))


@pytest.fixture
def stub_service():
    stub = _StubService()
    push.set_service(stub)
    yield stub
    push.set_service(None)


async def test_push_reporter_disabled_never_touches_service(stub_service):
    reporter = PushReporter()
    await reporter.report(False, _FakeJobCtx(), {"push": {"enabled": False}})
    await reporter.report(False, _FakeJobCtx(), {})
    assert stub_service.calls == []


async def test_push_reporter_enabled_hands_off(stub_service):
    reporter = PushReporter()
    ctx = _FakeJobCtx()
    await reporter.report(
        False, ctx, {"push": {"enabled": True, "priority": "passive"}}
    )
    assert len(stub_service.calls) == 1
    handed_ctx, success, push_config = stub_service.calls[0]
    assert handed_ctx is ctx and success is False
    assert push_config["priority"] == "passive"


async def test_push_reporter_without_service_logs_not_raises(caplog):
    push.set_service(None)
    reporter = PushReporter()
    await reporter.report(False, _FakeJobCtx(), {"push": {"enabled": True}})
    assert any("alert dropped" in r.message for r in caplog.records)


def test_push_is_a_registered_reporter_and_gates_fanout():
    assert any(
        isinstance(r, PushReporter) for r in RunningJob.REPORTERS
    )
    defaults = parse_config_string(
        'jobs:\n  - name: j\n    command: "true"\n    schedule: "* * * * *"\n',
        "",
    ).jobs[0]
    report = defaults.onFailure["report"]
    assert report["push"]["enabled"] is False
    assert report_config_enabled(report) is False
    # deepcopy before mutating: mergedicts shares untouched subtrees with
    # the module-level DEFAULT_CONFIG, so an in-place edit here would
    # poison every job parsed later in this process.
    enabled = copy.deepcopy(report)
    enabled["push"]["enabled"] = True
    assert report_config_enabled(enabled) is True


def test_canonical_job_omits_all_default_push_block():
    plain = """
jobs:
  - name: plain
    command: "true"
    schedule: "* * * * *"
"""
    job = parse_config_string(plain, "").jobs[0]
    assert "push" not in canonical_job(job)["onFailure"]["report"]
    # a job that actually enables push gets it in identity (set at parse
    # time; never mutate the parsed dicts, they share subtrees with the
    # module-level defaults)
    pushy = """
jobs:
  - name: plain
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""
    job = parse_config_string(pushy, "").jobs[0]
    assert canonical_job(job)["onFailure"]["report"]["push"]["enabled"]


# ----------------------------------------------------- config validation


def _parse_validated(yaml: str):
    """Parse plus the cross-section pass the daemon's file entry runs.

    The push/mcp/state fail-closed checks live in
    ``_validate_cross_sections`` (sections may span config-dir files),
    which ``parse_config_string`` alone deliberately does not run.
    """
    cfg = parse_config_string(yaml, "")
    config._validate_cross_sections(cfg)
    return cfg


_PUSH_STATE_YAML = """
push:
  relay:
    url: https://relay.example.net/v1/notify
state:
  path: {path}
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""


def test_push_config_parses_with_state(tmp_path, monkeypatch):
    # crypto-free wiring test: force the probe on so a bare checkout
    # without pynacl exercises it too
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    cfg = _parse_validated(
        _PUSH_STATE_YAML.format(path=(tmp_path / "state").as_posix())
    )
    assert cfg.push_config == {
        "relay": {
            "url": "https://relay.example.net/v1/notify",
            "timeout": 10.0,
        },
        "devicesFile": None,
        "allowUnauthenticated": False,
    }


def test_push_enabled_without_section_is_refused():
    yaml = """
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
    onFailure:
      report:
        push:
          enabled: true
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "job j" in str(exc.value)
    assert "push" in str(exc.value)


def test_notify_push_without_section_is_refused():
    yaml = """
notify:
  report:
    push:
      enabled: true
jobs:
  - name: j
    command: "true"
    schedule: "* * * * *"
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "notify" in str(exc.value)


def test_push_needs_state_or_devices_file(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "devicesFile" in str(exc.value)
    # devicesFile alone satisfies the storage requirement
    _parse_validated(
        yaml + "  devicesFile: /tmp/devices.json\n")


_PUSH_WEB_YAML = """
push:
  relay:
    url: https://relay.example.net/v1/notify
  devicesFile: /tmp/d.json
{extra}web:
  listen:
    - {listen}
{weblines}"""


def _push_web_yaml(listen, weblines="", extra=""):
    return _PUSH_WEB_YAML.format(
        listen=listen, weblines=weblines, extra=extra
    )


def test_push_on_routable_listener_without_token_is_refused(monkeypatch):
    # The twin of the mcp fail-closed gate, for the other mutating surface:
    # with no token there is no auth middleware, so POST /push/devices would
    # let anything that can reach the listener pair its own key and keep
    # receiving alerts long after it left the network.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    with pytest.raises(ConfigError, match="without authentication") as exc:
        _parse_validated(_push_web_yaml("http://0.0.0.0:8080"))
    assert "/push/devices" in str(exc.value)
    assert "push.allowUnauthenticated" in str(exc.value)


def test_push_on_loopback_without_token_is_allowed(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    cfg = _parse_validated(_push_web_yaml("http://127.0.0.1:8080"))
    assert cfg.push_config["allowUnauthenticated"] is False


def test_push_on_routable_listener_with_token_is_allowed(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    cfg = _parse_validated(
        _push_web_yaml(
            "http://0.0.0.0:8080", "  authToken:\n    value: sekret\n"
        )
    )
    assert cfg.push_config["devicesFile"] == "/tmp/d.json"


def test_push_scoped_tokens_satisfy_the_gate(monkeypatch):
    # scoped web.authTokens (no scalar authToken) still install the auth
    # middleware, and /push/devices sits behind the view/control scopes.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    _parse_validated(
        _push_web_yaml(
            "http://0.0.0.0:8080",
            "  authTokens:\n"
            "    - label: phone\n"
            "      scopes:\n        - control\n"
            "      value: s3cret\n",
        )
    )


def test_push_mtls_listener_satisfies_the_gate(monkeypatch):
    # web.tls.clientCa authenticates callers at the transport, which is the
    # same guarantee the gate accepts from an mTLS-terminating proxy.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    _parse_validated(
        _push_web_yaml(
            "https://0.0.0.0:8443",
            "  tls:\n"
            "    cert: /etc/c.pem\n"
            "    key: /etc/k.pem\n"
            "    clientCa: /etc/ca.pem\n",
        )
    )
    # plain https only encrypts, so it is still refused
    with pytest.raises(ConfigError, match="without authentication"):
        _parse_validated(
            _push_web_yaml(
                "https://0.0.0.0:8443",
                "  tls:\n    cert: /etc/c.pem\n    key: /etc/k.pem\n",
            )
        )


def test_push_allow_unauthenticated_escape_hatch(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    cfg = _parse_validated(
        _push_web_yaml(
            "http://0.0.0.0:8080", extra="  allowUnauthenticated: true\n"
        )
    )
    assert cfg.push_config["allowUnauthenticated"] is True


def test_push_without_a_web_section_needs_no_token(monkeypatch):
    # A node that only SENDS alerts exposes no pairing endpoint at all: the
    # cluster shape where one node serves the dashboard and pairs devices
    # into the registry the others share.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    _parse_validated(
        "push:\n"
        "  relay:\n    url: https://relay.example.net/v1/notify\n"
        "  devicesFile: /tmp/d.json\n"
    )


def test_push_without_pynacl_is_refused(monkeypatch):
    monkeypatch.setattr(push, "HAVE_PYNACL", False)
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
  devicesFile: /tmp/devices.json
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "PyNaCl" in str(exc.value)
    assert "fail" in str(exc.value)


@pytest.mark.parametrize(
    "url", ["ftp://x/y", "relay.example.net", "unix:///tmp/x", ""]
)
def test_push_relay_url_must_be_http(url):
    yaml = (
        'push:\n  relay:\n    url: "{}"\n  devicesFile: /tmp/d.json\n'.format(
            url
        )
    )
    with pytest.raises(ConfigError):
        _parse_validated(yaml)


@pytest.mark.parametrize(
    "url",
    [
        # scheme-less: urlparse reads the userinfo as the scheme, so the
        # raw string reaches the bad-scheme raise with the password in it
        "admin:hunter2@relay.example",
        # scheme typo: the netloc (credentials and all) survives parsing
        "htp://admin:hunter2@relay.example/v1/notify",
    ],
)
def test_push_relay_url_error_redacts_credentials(url):
    # The message is printed at startup and logged by the reload loop on
    # every reparse until the config is fixed; like every other URL a
    # ConfigError echoes, it must never carry the secret.
    yaml = (
        'push:\n  relay:\n    url: "{}"\n  devicesFile: /tmp/d.json\n'.format(
            url
        )
    )
    with pytest.raises(ConfigError) as err:
        _parse_validated(yaml)
    assert "hunter2" not in str(err.value)
    assert "***@relay.example" in str(err.value)


def test_push_relay_timeout_must_be_positive():
    yaml = """
push:
  relay:
    url: https://relay.example.net/v1/notify
    timeout: 0
  devicesFile: /tmp/d.json
"""
    with pytest.raises(ConfigError) as exc:
        _parse_validated(yaml)
    assert "timeout" in str(exc.value)


def test_config_dir_push_section_in_its_own_file(tmp_path, monkeypatch):
    """The finding-2 regression: config-dir mode must carry push through.

    _validate_push_config's own contract says the push: section and the
    jobs enabling the reporter may live in different config-dir files;
    a dir loop that drops push_config turns that into a false "no push:
    section" refusal (and a push-only dir into silently-404 endpoints).
    """
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    (tmp_path / "10-jobs.yaml").write_text(
        "jobs:\n"
        "  - name: j\n"
        '    command: "true"\n'
        '    schedule: "* * * * *"\n'
        "    onFailure:\n"
        "      report:\n"
        "        push:\n"
        "          enabled: true\n",
        encoding="utf-8",
    )
    (tmp_path / "20-push.yaml").write_text(
        "push:\n"
        "  relay:\n"
        "    url: https://relay.example.net/v1/notify\n"
        "  devicesFile: {}\n".format(
            (tmp_path / "devices.json").as_posix()
        ),
        encoding="utf-8",
    )
    conf = config.parse_config(str(tmp_path))
    assert conf.push_config is not None
    assert conf.push_config["relay"]["url"].endswith("/v1/notify")


def test_config_dir_web_and_push_split_still_hits_the_auth_gate(
    tmp_path, monkeypatch
):
    # The gate reads two sections that may live in different files, which
    # is why it runs on the assembled config. A dir loop that validated
    # per file could not see a routable web: in one file and a push: in
    # another, and would serve open pairing endpoints.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    (tmp_path / "10-web.yaml").write_text(
        "web:\n  listen:\n    - http://0.0.0.0:8080\n", encoding="utf-8"
    )
    (tmp_path / "20-push.yaml").write_text(
        "push:\n"
        "  relay:\n"
        "    url: https://relay.example.net/v1/notify\n"
        "  devicesFile: {}\n".format((tmp_path / "devices.json").as_posix()),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="without authentication"):
        config.parse_config(str(tmp_path))
    # and the escape hatch reaches it across the same split
    (tmp_path / "20-push.yaml").write_text(
        "push:\n"
        "  relay:\n"
        "    url: https://relay.example.net/v1/notify\n"
        "  allowUnauthenticated: true\n"
        "  devicesFile: {}\n".format((tmp_path / "devices.json").as_posix()),
        encoding="utf-8",
    )
    conf = config.parse_config(str(tmp_path))
    assert conf.push_config["allowUnauthenticated"] is True


def test_config_dir_multiple_push_sections_refused(tmp_path):
    body = (
        "push:\n"
        "  relay:\n"
        "    url: https://relay.example.net/v1/notify\n"
        "  devicesFile: /tmp/d.json\n"
    )
    (tmp_path / "10-push.yaml").write_text(body, encoding="utf-8")
    (tmp_path / "20-push.yaml").write_text(body, encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        config.parse_config(str(tmp_path))
    assert "Multiple 'push' configurations" in str(exc.value)


# ------------------------------------------------------- bonjour config


_BONJOUR_YAML = """
web:
  listen:
    - {listen}
  bonjour: true
"""


def test_bonjour_without_zeroconf_is_refused(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", False)
    with pytest.raises(ConfigError) as exc:
        parse_config_string(
            _BONJOUR_YAML.format(listen="http://127.0.0.1:8080"), ""
        )
    assert "zeroconf" in str(exc.value)


def test_bonjour_with_zeroconf_and_tcp_listen_parses(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    cfg = parse_config_string(
        _BONJOUR_YAML.format(listen="http://127.0.0.1:8080"), ""
    )
    assert config.resolve_bonjour_config(cfg.web_config) == {"name": None}


def test_bonjour_unix_only_listen_is_refused(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    with pytest.raises(ConfigError) as exc:
        parse_config_string(
            _BONJOUR_YAML.format(listen="unix:///tmp/web.sock"), ""
        )
    assert "unix" in str(exc.value)


def test_bonjour_off_forms_need_no_library(monkeypatch):
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", False)
    cfg = parse_config_string(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n  bonjour: false\n",
        "",
    )
    assert config.resolve_bonjour_config(cfg.web_config) is None
    cfg = parse_config_string(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "  bonjour:\n    enabled: false\n    name: attic\n",
        "",
    )
    assert config.resolve_bonjour_config(cfg.web_config) is None


# --------------------------------------------------------- web handlers


class _Req:
    """The slice of aiohttp's Request the push/whoami handlers read.

    ``can_read_body`` arms the shared ``_web_json_body`` reader the
    pairing handler goes through; an Exception body still raises from
    ``json()`` so the malformed-body arms stay reachable.
    """

    def __init__(self, match=None, body=None, token=None, anon=None):
        self.match_info = match or {}
        self._body = body
        self.can_read_body = body is not None
        self._store: dict[str, Any] = {}
        if token is not None:
            self._store[WEB_TOKEN_REQUEST_KEY] = token
        if anon is not None:
            self._store[WEB_ANON_REQUEST_KEY] = anon

    def get(self, key, default=None):
        return self._store.get(key, default)

    async def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


_SEED_JOB = """
jobs:
  - name: seed
    command: "true"
    schedule: "* * * * *"
    enabled: false
"""


def _cron() -> Cron:
    return _shared_cron(_SEED_JOB)


def _pair_body(public_b64: str) -> dict[str, str]:
    return {
        "name": "phone",
        "platform": "ios",
        "publicKey": public_b64,
        "pushToken": "tok-abcdef",
    }


async def test_push_routes_404_without_push_section():
    cron = _cron()
    for call in (
        cron._web_push_devices(_Req()),
        cron._web_push_pair(_Req(body={})),
        cron._web_push_revoke(_Req(match={"id": "x"})),
        cron._web_push_test(_Req(match={"id": "x"})),
    ):
        with pytest.raises(web.HTTPNotFound):
            await call


@requires_pynacl
async def test_pair_list_revoke_via_handlers(tmp_path):
    _, public_b64 = _device_keypair()
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json"))
    )
    token = _WebToken(b"t", frozenset({"view", "control"}), "ops-phone")
    resp = await cron._web_push_pair(
        _Req(body=_pair_body(public_b64), token=token)
    )
    assert resp.status == 201
    created = json.loads(resp.body)
    assert created["created"] is True
    assert created["device"]["createdBy"] == "ops-phone"
    device_id = created["device"]["id"]

    # re-pair: 200, same id
    resp = await cron._web_push_pair(_Req(body=_pair_body(public_b64)))
    assert resp.status == 200
    assert json.loads(resp.body)["device"]["id"] == device_id

    resp = await cron._web_push_devices(_Req())
    devices = json.loads(resp.body)["devices"]
    assert [d["id"] for d in devices] == [device_id]

    resp = await cron._web_push_revoke(_Req(match={"id": device_id}))
    assert json.loads(resp.body)["revoked"] == device_id
    with pytest.raises(web.HTTPNotFound):
        await cron._web_push_revoke(_Req(match={"id": device_id}))


async def test_pair_rejects_bad_bodies(tmp_path):
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json"))
    )
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_push_pair(_Req(body=ValueError("bad json")))
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_push_pair(_Req(body={"name": "x"}))
    # the non-ValueError decode failures too: a bogus `charset=` makes
    # aiohttp's json() raise LookupError, which a bare `except ValueError`
    # let escape the JSON error envelope as a plain-text 500.  The shared
    # _web_json_body reader catches the full decode surface.
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_push_pair(
            _Req(body=LookupError("unknown encoding: x-bogus"))
        )


@requires_pynacl
async def test_pair_store_write_failure_is_503(tmp_path):
    # The docstring/openapi contract: store trouble is 503, never a raw
    # aiohttp 500. A devicesFile in a missing directory is the simplest
    # real store-trouble a pairing can hit.
    _, public_b64 = _device_keypair()
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "no-such-dir" / "d.json"))
    )
    with pytest.raises(web.HTTPServiceUnavailable):
        await cron._web_push_pair(_Req(body=_pair_body(public_b64)))


@requires_pynacl
async def test_store_trouble_503_keeps_the_store_detail_in_the_log(
    tmp_path, caplog
):
    # The 503 body is the fact, not the store's own words: those name the
    # registry's absolute path and quote the OSError/JSON error, and the
    # listing route needs only a `view` scope. One corrupt file reaches
    # every handler, because all four touch the store before answering.
    _, public_b64 = _device_keypair()
    path = tmp_path / "d.json"
    path.write_text("{not json", encoding="utf-8")
    cron = _cron()
    cron._push_service = _service(push.FileDeviceStore(str(path)))
    calls = (
        (cron._web_push_devices, _Req()),
        (cron._web_push_pair, _Req(body=_pair_body(public_b64))),
        (cron._web_push_revoke, _Req(match={"id": "x"})),
        (cron._web_push_test, _Req(match={"id": "x"})),
    )
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        for handler, request in calls:
            with pytest.raises(web.HTTPServiceUnavailable) as raised:
                await handler(request)
            body = raised.value.text or ""
            assert str(tmp_path) not in body
            assert "unreadable" not in body
            assert "the reason is in the cronstable log" in body
    leaked = [r for r in caplog.records if str(path) in r.getMessage()]
    assert len(leaked) == len(calls)
    assert all(r.levelno == logging.WARNING for r in leaked)


async def test_pairing_400_keeps_the_pynacl_import_detail_in_the_log(
    tmp_path, monkeypatch, caplog
):
    # HAVE_PYNACL is find_spec, which answers "findable", not "imports":
    # the half-installed case (libsodium present, _cffi_backend missing)
    # reaches _sealed_box and raises an ImportError naming the install
    # layout, typically an absolute .so/.pyd path or a missing SONAME.
    # validate_public_key re-raises that PushError unchanged to keep a
    # broken library distinct from a broken key, and the pairing handler
    # returns a PushError's text verbatim, so the reason has to stay in
    # the log.  No pynacl needed: the import is forced to fail.
    monkeypatch.setattr(push, "HAVE_PYNACL", True)
    monkeypatch.setitem(sys.modules, "nacl.public", None)
    cron = _cron()
    cron._push_service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json"))
    )
    body = _pair_body(base64.b64encode(b"\x01" * 32).decode("ascii"))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        with pytest.raises(web.HTTPBadRequest) as raised:
            await cron._web_push_pair(_Req(body=body))
    text = raised.value.text or ""
    assert "nacl.public" not in text
    assert "sys.modules" not in text
    # the body is the uniform JSON error envelope now; assert on the
    # decoded message so the inner quotes are not escape-mangled
    message = json.loads(text)["error"]
    assert "the reason is in the cronstable log" in message
    assert 'pip install "cronstable[push]"' in message
    leaked = [r for r in caplog.records if "nacl.public" in r.getMessage()]
    assert len(leaked) == 1
    assert leaked[0].levelno == logging.WARNING


def test_redact_userinfo_in_scrubs_relay_credentials():
    url = "http://user:p4ssw0rd@relay.example:8443/send"
    assert push._userinfo_of(url) == "user:p4ssw0rd"
    assert push._redact_userinfo_in("relay unreachable: " + url, url) == (
        "relay unreachable: http://***@relay.example:8443/send"
    )
    # a password containing '@' is cut at the LAST '@', so no tail of the
    # secret survives
    at_url = "http://user:p@ss@relay.example/send"
    assert push._userinfo_of(at_url) == "user:p@ss"
    assert "p@ss" not in push._redact_userinfo_in(at_url, at_url)
    # a credential-free relay url leaves the text untouched
    plain = "http://relay.example/send"
    assert push._redact_userinfo_in("relay unreachable: x", plain) == (
        "relay unreachable: x"
    )


@pytest.mark.parametrize("char", ["/", "?", "#"])
def test_redact_userinfo_in_survives_an_unescaped_delimiter(char):
    # A password carrying one of these ends the authority early, so the
    # netloc urlparse reads back is a truncated "user:p" with no '@' in
    # it and _userinfo_of has no needle to offer.  The whole URL comes
    # out instead, because "" from _userinfo_of means "cannot name it",
    # not "nothing to hide".  These are not exotic: '/' is in the base64
    # alphabet, so a generated relay password lands here.  They are also
    # exactly the URLs yarl refuses, which is to say exactly the ones
    # that reach this redactor as an InvalidUrlClientError.
    url = "http://user:p{}4ssw0rd@relay.example/send".format(char)
    assert push._userinfo_of(url) == ""
    out = push._redact_userinfo_in("relay unreachable: " + url, url)
    assert "4ssw0rd" not in out
    assert out == "relay unreachable: <relay url>"


def test_redact_userinfo_in_leaves_a_credential_free_url_readable():
    # The blunt whole-URL pass is gated on an '@': a relay with no
    # credentials must keep its host and path in the diagnostic, which is
    # the only thing that tells an operator WHICH endpoint is down.
    url = "http://relay.example:8443/send"
    text = "relay unreachable: Cannot connect to host relay.example:8443"
    assert push._redact_userinfo_in(text, url) == text


def test_redact_userinfo_in_flattens_control_chars_either_way():
    # The strip runs before the credential check, not after it, so two
    # deployments hitting the same error get the same shaped log line
    # whether or not the relay url carries credentials.
    for url in (
        "http://relay.example/send",
        "http://user:p4ssw0rd@relay.example/send",
    ):
        out = push._redact_userinfo_in("relay unreachable:\r\n\tbad", url)
        assert "\r" not in out and "\n" not in out and "\t" not in out


def test_redact_userinfo_in_survives_control_chars_in_the_url():
    # urlsplit deletes tab/CR/LF before parsing, so a needle taken from
    # the parse cannot match an exception text that still carries them
    # (aiohttp quotes the configured string verbatim).
    for char in ("\t", "\r", "\n"):
        url = "http://user:p4ssw0rd{}x@relay.example:99999/s".format(char)
        out = push._redact_userinfo_in("relay unreachable: " + url, url)
        assert "p4ssw0rd" not in out
        assert "***@relay.example" in out


async def test_relay_outcome_redacts_url_credentials(tmp_path, monkeypatch):
    # push.relay.url is the one URL the config accepts with embedded
    # credentials, and aiohttp raises InvalidUrlClientError (a ClientError)
    # for a URL yarl rejects, with the whole URL as its text.  That outcome
    # is logged by send_report and returned as a test alert's 502 body.
    import aiohttp

    monkeypatch.setattr(
        push, "seal_to_device", lambda key, plain, suite=None: "Y2k="
    )
    service = _service(
        push.FileDeviceStore(str(tmp_path / "d.json")),
        # port out of range: yarl refuses it, so this never leaves the box
        relay_url="http://user:p4ssw0rd@relay.example:99999/send",
    )
    device = {"id": "d1", "publicKey": "k", "pushToken": "t"}
    async with aiohttp.ClientSession() as session:
        outcome = await service._send_to_device(
            session,
            device,
            {"v": 1, "kind": "test", "name": "t"},
            "collapse",
            "time-sensitive",
            False,
        )
    assert outcome["error"]
    assert "p4ssw0rd" not in outcome["error"]
    assert "***@relay.example" in outcome["error"]

    # The fan-out's catch-all files whatever those two except clauses did
    # not (an ssl or aiohttp error quotes the URL just as readily), and it
    # reaches the same daemon log line and the same /push/test 502 body.
    async def _unmodelled(*args, **kwargs):
        raise RuntimeError(
            "handshake to {} failed".format(service.relay_url)
        )

    monkeypatch.setattr(service, "_send_to_device", _unmodelled)
    fanned = await service.send_test(device)
    assert fanned["error"].startswith("unexpected RuntimeError: ")
    assert "p4ssw0rd" not in fanned["error"]
    assert "***@relay.example" in fanned["error"]


@requires_pynacl
async def test_push_test_endpoint_round_trip(tmp_path):
    _, public_b64 = _device_keypair()
    async with _RelayServer() as relay:
        cron = _cron()
        service, record = await _paired_service(
            tmp_path, relay.url, public_b64
        )
        cron._push_service = service
        resp = await cron._web_push_test(_Req(match={"id": record["id"]}))
        assert resp.status == 200
        assert json.loads(resp.body)["status"] == 200
        with pytest.raises(web.HTTPNotFound):
            await cron._web_push_test(_Req(match={"id": "nope"}))


async def test_whoami_with_and_without_token():
    cron = _cron()
    token = _WebToken(b"t", frozenset({"view"}), "wallboard")
    body = json.loads((await cron._web_whoami(_Req(token=token))).body)
    assert body == {
        "authenticated": True,
        "label": "wallboard",
        "scopes": ["view"],
        "allScopes": False,
        "pairLinkBase": "https://relay.cronstable.com/pair",
        "sealableSuites": _sealable_now(),
    }
    body = json.loads((await cron._web_whoami(_Req())).body)
    assert body["authenticated"] is False
    assert body["allScopes"] is True
    assert body["scopes"] == sorted(["view", "control", "approve"])
    # every shape advertises the sealable suites (the app
    # picks its pairing suite from this list)
    assert body["sealableSuites"] == _sealable_now()
    assert "x25519" in body["sealableSuites"]


async def test_whoami_reports_an_anonymous_grant():
    # the fourth shape: unauthenticated like the open daemon, but with a
    # restricted scope set. `allScopes` is the discriminator clients key on
    # (the open daemon answers authenticated:false with allScopes:true).
    cron = _cron()
    body = json.loads(
        (await cron._web_whoami(_Req(anon=frozenset({"view"})))).body
    )
    assert body == {
        "authenticated": False,
        "label": "anonymous",
        "scopes": ["view"],
        "allScopes": False,
        "pairLinkBase": "https://relay.cronstable.com/pair",
        "sealableSuites": _sealable_now(),
    }


async def test_whoami_pair_link_base_follows_the_relay_origin():
    # the pairing QR's deep link tracks the configured relay's origin
    # (credentials and path dropped), so a self-hosted relay keeps camera
    # pairing on its own domain; the hosted landing is only the fallback.
    cron = _cron()
    body = json.loads((await cron._web_whoami(_Req())).body)
    assert body["pairLinkBase"] == "https://relay.cronstable.com/pair"
    cron._applied_push_config = {
        "relay": {"url": "https://user:pw@relay.internal:8443/report"}
    }
    body = json.loads((await cron._web_whoami(_Req())).body)
    assert body["pairLinkBase"] == "https://relay.internal:8443/pair"


async def test_all_scopes_token_reports_all_scopes():
    cron = _cron()
    token = _WebToken(
        b"t", frozenset({"view", "control", "approve"}), "authToken"
    )
    body = json.loads((await cron._web_whoami(_Req(token=token))).body)
    assert body["allScopes"] is True
    assert body["sealableSuites"] == _sealable_now()


# --------------------------------------------------- scope enforcement


class _ScopeReq:
    def __init__(self, path, method, canonical, headers):
        self.path = path
        self.method = method
        self.headers = headers
        self.query: dict[str, str] = {}
        resource = SimpleNamespace(canonical=canonical)
        route = SimpleNamespace(resource=resource)
        self.match_info = SimpleNamespace(route=route)
        self._store: dict[str, Any] = {}

    def __setitem__(self, key, value):
        self._store[key] = value

    def get(self, key, default=None):
        return self._store.get(key, default)


async def _run_mw(middleware, request):
    async def handler(req):
        return "ok"

    return await middleware(request, handler)


def _mw(scopes):
    table = [
        _WebToken(
            b"tok", cron_mod._effective_web_scopes(scopes), "phone"
        )
    ]
    return Cron._make_auth_middleware(table)


async def test_view_token_lists_devices_but_cannot_pair():
    headers = {"Authorization": "Bearer tok"}
    mw = _mw(["view"])
    ok = await _run_mw(
        mw, _ScopeReq("/push/devices", "GET", "/push/devices", headers)
    )
    assert ok == "ok"
    with pytest.raises(web.HTTPForbidden):
        await _run_mw(
            mw, _ScopeReq("/push/devices", "POST", "/push/devices", headers)
        )
    with pytest.raises(web.HTTPForbidden):
        await _run_mw(
            mw,
            _ScopeReq(
                "/push/devices/x",
                "DELETE",
                "/push/devices/{id}",
                headers,
            ),
        )


async def test_control_token_can_pair_and_middleware_files_identity():
    headers = {"Authorization": "Bearer tok"}
    mw = _mw(["control"])
    req = _ScopeReq("/push/devices", "POST", "/push/devices", headers)
    assert await _run_mw(mw, req) == "ok"
    filed = req.get(WEB_TOKEN_REQUEST_KEY)
    assert filed is not None and filed.label == "phone"


# ------------------------------------------------------ lifecycle edges


async def test_start_stop_push_builds_and_tears_down(tmp_path):
    cron = _cron()
    push_config = {
        "relay": {"url": "http://127.0.0.1:1/unused", "timeout": 5.0},
        "devicesFile": str(tmp_path / "devices.json"),
    }
    await cron.start_stop_push(push_config)
    assert cron._push_service is not None
    assert push.get_service() is cron._push_service
    assert cron._push_service.store.kind == "file"
    first = cron._push_service
    # unchanged config: same service instance
    await cron.start_stop_push(dict(push_config))
    assert cron._push_service is first
    # section removed: service gone, module seam cleared
    await cron.start_stop_push(None)
    assert cron._push_service is None
    assert push.get_service() is None


async def test_start_stop_push_reload_changed_rebuilds(tmp_path):
    cron = _cron()
    push_config = {
        "relay": {"url": "http://127.0.0.1:1/a", "timeout": 5.0},
        "devicesFile": str(tmp_path / "devices.json"),
    }
    await cron.start_stop_push(push_config)
    first = cron._push_service
    # a changed section rebuilds the service onto the new relay
    changed = copy.deepcopy(push_config)
    changed["relay"]["url"] = "http://127.0.0.1:1/b"
    await cron.start_stop_push(changed)
    assert cron._push_service is not first
    assert cron._push_service.relay_url == "http://127.0.0.1:1/b"
    # the aliasing trap: editing the SAME dict object in place must
    # still read as a change (the applied snapshot is a deep copy, so
    # comparing against it detects the mutation; holding an alias would
    # make this compare equal to itself forever)
    changed["relay"]["url"] = "http://127.0.0.1:1/c"
    await cron.start_stop_push(changed)
    assert cron._push_service.relay_url == "http://127.0.0.1:1/c"
    await cron.start_stop_push(None)


async def test_start_stop_push_absorbs_a_corrupt_registry_document(caplog):
    # The end of the finding-2 chain: an unreadable pushmeta document
    # escaped the PushError contract, out of start_stop_push, and into the
    # housekeeping pass, which skipped everything after it (the durable
    # state manifest and garbage collection) on that pass and on every
    # later one, since the push config never records as applied.
    cron = _cron()
    cron.state_backend = _UnreadableDocBackend()
    push_config = {
        "relay": {"url": "http://127.0.0.1:1/unused", "timeout": 5.0},
        "devicesFile": None,
    }
    await cron.start_stop_push(push_config)
    # the service is up and retries the store on demand; the daemon does
    # not lose its housekeeping over one bad file
    assert cron._push_service is not None
    assert any(
        "could not load the device registry" in r.getMessage()
        for r in caplog.records
    )
    # and the convergence RECORDS as applied, so the next housekeeping
    # pass is a no-op instead of rebuilding and re-failing every minute
    assert cron._applied_push_config == push_config
    first = cron._push_service
    await cron.start_stop_push(dict(push_config))
    assert cron._push_service is first
    await cron.start_stop_push(None)


async def test_start_stop_push_never_raises(monkeypatch, caplog):
    # Belt to the _bounded braces: whatever slips through convergence, the
    # housekeeping pass carries on. Nothing about push is worth the durable
    # state manifest and GC that run immediately after it.
    cron = _cron()

    async def boom(_push_config):
        raise RuntimeError("convergence exploded")

    monkeypatch.setattr(cron, "_converge_push", boom)
    await cron.start_stop_push({"relay": {"url": "http://x/", "timeout": 1}})
    assert any(
        "could not converge the push service" in r.getMessage()
        for r in caplog.records
    )


async def test_start_stop_push_state_store_tracks_backend(tmp_path):
    cron = _cron()
    await cron.start_stop_push(
        {"relay": {"url": "http://x/", "timeout": 5.0}, "devicesFile": None}
    )
    service = cron._push_service
    assert service is not None and service.store.kind == "state"
    # no backend yet: interactive paths fail loudly, not silently
    with pytest.raises(push.PushError):
        await service.store.load()
    cron.state_backend = _FakeStateBackend()
    await service.store.upsert({"id": "d1", "name": "phone"})
    assert [d["id"] for d in await service.store.load()] == ["d1"]
    await cron.start_stop_push(None)


# ----------------------------------------------------- bonjour runtime


class _FakeAsyncZeroconf:
    instances: list["_FakeAsyncZeroconf"] = []

    def __init__(self) -> None:
        self.registered: list[Any] = []
        self.unregistered: list[Any] = []
        self.closed = False
        _FakeAsyncZeroconf.instances.append(self)

    async def async_register_service(self, info):
        self.registered.append(info)

    async def async_unregister_service(self, info):
        self.unregistered.append(info)

    async def async_close(self):
        self.closed = True


class _FakeServiceInfo:
    def __init__(self, type_, name, **kwargs):
        self.type = type_
        self.name = name
        self.kwargs = kwargs


@pytest.fixture
def fake_zeroconf(monkeypatch):
    _FakeAsyncZeroconf.instances = []
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    monkeypatch.setattr(
        discovery, "AsyncZeroconf", _FakeAsyncZeroconf, raising=False
    )
    monkeypatch.setattr(
        discovery, "ServiceInfo", _FakeServiceInfo, raising=False
    )
    monkeypatch.setattr(discovery, "primary_address", lambda: "192.0.2.7")
    return _FakeAsyncZeroconf


async def test_bonjour_register_converge_and_stop(fake_zeroconf):
    advertiser = discovery.BonjourAdvertiser()
    advert = {
        "name": "attic.local",
        "port": 8080,
        "properties": {"v": "1.2.30", "scheme": "http"},
    }
    await advertiser.start_stop(advert)
    assert advertiser.active
    (zc,) = fake_zeroconf.instances
    (info,) = zc.registered
    assert info.name == "attic-local._cronstable._tcp.local."
    assert info.kwargs["port"] == 8080
    assert info.kwargs["properties"] == {"v": "1.2.30", "scheme": "http"}
    # the SRV target is a dedicated hostname, never the machine's own
    # <hostname>.local. (see _server_name)
    assert info.kwargs["server"] == "attic-local-cronstable.local."
    # unchanged advert: no re-registration
    await advertiser.start_stop(dict(advert))
    assert len(fake_zeroconf.instances) == 1
    # changed advert: old one is torn down, a fresh one registered
    await advertiser.start_stop({**advert, "port": 9090})
    assert zc.closed and len(zc.unregistered) == 1
    assert len(fake_zeroconf.instances) == 2
    await advertiser.stop()
    assert not advertiser.active
    assert fake_zeroconf.instances[-1].closed


async def test_bonjour_register_failure_logs_and_stays_off(
    fake_zeroconf, monkeypatch, caplog
):
    async def boom(self, info):
        raise OSError("multicast is off")

    monkeypatch.setattr(
        _FakeAsyncZeroconf, "async_register_service", boom
    )
    advertiser = discovery.BonjourAdvertiser()
    await advertiser.start_stop({"name": "n", "port": 1, "properties": {}})
    assert not advertiser.active
    assert any("failed to register" in r.message for r in caplog.records)
    # The half-built AsyncZeroconf must be closed on the failure path:
    # this runs every housekeeping minute, and one leaked instance per
    # pass exhausts file descriptors within a day.
    (zc,) = fake_zeroconf.instances
    assert zc.closed


async def test_bonjour_reregisters_when_the_address_changes(
    fake_zeroconf, monkeypatch
):
    addresses = iter(["192.0.2.7", "192.0.2.7", "198.51.100.9"])
    monkeypatch.setattr(
        discovery, "primary_address", lambda: next(addresses)
    )
    advertiser = discovery.BonjourAdvertiser()
    advert = {"name": "attic", "port": 8080, "properties": {}}
    await advertiser.start_stop(advert)
    assert len(fake_zeroconf.instances) == 1
    # same advert, same address: converged, nothing re-registers
    await advertiser.start_stop(dict(advert))
    assert len(fake_zeroconf.instances) == 1
    # same advert, new DHCP lease: the old advert (with its dead IP) is
    # torn down and a fresh one registered
    await advertiser.start_stop(dict(advert))
    assert len(fake_zeroconf.instances) == 2
    assert fake_zeroconf.instances[0].closed
    await advertiser.stop()


def test_instance_name_truncates_by_utf8_bytes():
    # mDNS labels cap at 63 BYTES; three bytes per Japanese codepoint
    # means a 40-char name is 120 bytes and must shrink without tearing
    # a codepoint in half.
    label = discovery._instance_name("日" * 40)
    encoded = label.encode("utf-8")
    assert len(encoded) <= 63
    assert encoded.decode("utf-8") == label  # no torn codepoint
    assert discovery._instance_name("") == "cronstable"
    assert discovery._instance_name("a.b.c") == "a-b-c"


def test_server_name_is_distinct_and_ldh_safe():
    # Never the machine's own <hostname>.local.: avahi/mDNSResponder
    # already defend that name with the host's full address list, and a
    # second responder claiming it with one route-derived IPv4 is the
    # RFC 6762 conflict that can rename the whole machine.
    assert discovery._server_name("attic") == "attic-cronstable"
    # an operator display name is a fine instance label but not a
    # hostname: spaces and friends become hyphens for strict resolvers
    assert discovery._server_name("Basement Pi") == "Basement-Pi-cronstable"
    assert discovery._server_name("日本語") == "cronstable"
    label = discovery._server_name("x" * 200)
    assert len(label.encode("utf-8")) <= 63
    assert label.endswith("-cronstable")


async def test_bonjour_advert_address_skips_the_probe(
    fake_zeroconf, monkeypatch
):
    # A specific-bind advert carries the listener's own address; the
    # outbound-route probe (which could name a different interface than
    # the one the socket lives on) must not run at all.
    def _no_probe():
        raise AssertionError("the probe must not run for a specific bind")

    monkeypatch.setattr(discovery, "primary_address", _no_probe)
    advertiser = discovery.BonjourAdvertiser()
    await advertiser.start_stop(
        {
            "name": "attic",
            "port": 8443,
            "properties": {},
            "address": "192.0.2.9",
        }
    )
    (zc,) = fake_zeroconf.instances
    (info,) = zc.registered
    assert info.kwargs["addresses"] == [socket.inet_aton("192.0.2.9")]
    await advertiser.stop()


def _advert_cron(bound) -> Cron:
    """A Cron with only what _bonjour_advert reads: a 'running' web app
    and the recorded (scheme, sockname) of its bound TCP listeners."""
    cron = Cron.__new__(Cron)
    cron._web_tcp_bound = bound
    cron.web_runner = object()
    return cron


def test_bonjour_advert_names_one_coherent_listener():
    # The finding this pins: the advert used to glue the FIRST bound
    # port to an "https if any listen entry is https" scheme and the
    # outbound-route IP, so the natural mixed shape below advertised
    # https at the loopback listener's port on the LAN address, an
    # endpoint nothing served. Port, scheme and address must all come
    # from one LAN-reachable listener.
    cron = _advert_cron(
        [
            ("http", ("127.0.0.1", 8080)),
            ("https", ("0.0.0.0", 8443)),
        ]
    )
    advert = cron._bonjour_advert({"bonjour": True})
    assert advert is not None
    assert advert["port"] == 8443
    assert advert["properties"]["scheme"] == "https"
    # a wildcard bind carries no address: the advertiser probes at
    # register time (and re-probes on DHCP changes)
    assert "address" not in advert


def test_bonjour_advert_prefers_https_and_carries_a_specific_bind():
    cron = _advert_cron(
        [
            ("http", ("0.0.0.0", 8080)),
            ("https", ("192.0.2.7", 8443)),
        ]
    )
    advert = cron._bonjour_advert({"bonjour": True})
    assert advert is not None
    assert (advert["port"], advert["properties"]["scheme"]) == (
        8443,
        "https",
    )
    # bound to one specific IPv4: advertise THAT address, not whatever
    # interface the outbound-route probe happens to name
    assert advert["address"] == "192.0.2.7"


def test_bonjour_advert_skips_undialable_listeners(caplog):
    # loopback-only: the old advert sent LAN peers to <LAN-IP>:8080
    # where nothing (or an unrelated process) listens
    cron = _advert_cron([("http", ("127.0.0.1", 8080))])
    assert cron._bonjour_advert({"bonjour": True}) is None
    assert any("no LAN-reachable" in r.message for r in caplog.records)
    caplog.clear()
    # a specific IPv6 bind: the advert's address record is IPv4-only,
    # so pointing an A record anywhere would name the wrong endpoint
    cron = _advert_cron([("https", ("fd00::5", 8443, 0, 0))])
    assert cron._bonjour_advert({"bonjour": True}) is None
    assert any("IPv4-only" in r.message for r in caplog.records)


async def test_web_app_records_bound_tcp_listeners():
    # the advert's inputs: every successfully bound TCP listener with
    # its scheme and real (post-`:0`) socket name, recorded at start,
    # reset on stop
    cron = _cron()
    await cron.start_stop_web_app({"listen": ["http://127.0.0.1:0"]})
    try:
        assert [
            (scheme, sockname[0])
            for scheme, sockname in cron._web_tcp_bound
        ] == [("http", "127.0.0.1")]
        (_, sockname) = cron._web_tcp_bound[0]
        assert sockname[1] == cron.web_runner.addresses[0][1]
        # loopback-only: bonjour has nothing a LAN peer could dial
        assert cron._bonjour_advert({"bonjour": True}) is None
    finally:
        await cron.start_stop_web_app(None)
    assert cron._web_tcp_bound == []


async def test_bonjour_real_serviceinfo_accepts_our_construction(
    monkeypatch,
):
    """The finding-8 gate: exercise the REAL zeroconf ServiceInfo.

    Every other Bonjour test fakes the library, so a signature or
    name-validation change in python-zeroconf would otherwise ship with
    green CI (the optional-dep blind spot). Only AsyncZeroconf is faked
    here, keeping the network out of the suite; ServiceInfo runs real,
    with a multibyte name that lands on the 63-byte label edge.
    """
    pytest.importorskip(
        "zeroconf", reason="zeroconf (the discovery extra) is not installed"
    )
    monkeypatch.setattr(discovery, "HAVE_ZEROCONF", True)
    monkeypatch.setattr(discovery, "AsyncZeroconf", _FakeAsyncZeroconf)
    monkeypatch.setattr(discovery, "primary_address", lambda: "192.0.2.7")
    advertiser = discovery.BonjourAdvertiser()
    await advertiser.start_stop(
        {
            "name": "日本語ホスト" * 6,
            "port": 8080,
            "properties": {"v": "1.2.30", "scheme": "http"},
        }
    )
    assert advertiser.active
    info = advertiser._info
    assert info is not None
    assert info.type == discovery.SERVICE_TYPE
    await advertiser.stop()


async def test_notify_fanout_reaches_push(stub_service):
    from cronstable.job import report_event

    ctx = NotifyEventContext(
        event="quorum_loss",
        success=False,
        name="cluster",
        subject="quorum lost",
        message="2 of 5 peers visible",
    )
    report_config = config._build_notify_config(
        {"report": {"push": {"enabled": True}}}
    )["report"]
    await report_event(ctx, report_config)
    assert len(stub_service.calls) == 1
    handed_ctx, success, push_config = stub_service.calls[0]
    assert handed_ctx is ctx
    assert push_config["enabled"] is True
