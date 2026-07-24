"""End-to-end encrypted push alerts: the ``push`` reporter's engine.

The daemon seals a compact alert payload to each paired device's X25519
public key (libsodium sealed boxes via PyNaCl) and hands the ciphertext,
plus an opaque coalescing id, to a hosted relay that forwards it to the
platform push service (APNs).  The relay never sees plaintext: it learns
only a device token, a ciphertext and a hash, so a self-hosted daemon
can use a shared relay without trusting it with job names, log lines or
hostnames.  See wiki/Push-Notifications.md and docs/relay-protocol.md.

Three pieces live here:

- sealing and payload sizing (:func:`seal_to_device`,
  :func:`build_payload`, :func:`fit_payload`), bounded so the relay's
  final APNs JSON stays under the 4096-byte APNs cap;
- the paired-device registry: :class:`FileDeviceStore` (a small local
  JSON file, atomic replace-on-write) and :class:`StateDeviceStore`
  (one document per device on the durable state store, so every node
  sharing the store sees the same pairings);
- :class:`PushService`: the daemon-global object shared by the
  ``push`` reporter, the ``notify:`` fan-out and the ``/push/devices``
  web handlers, published through :func:`set_service` /
  :func:`get_service` (reporters are stateless singletons, so they
  reach the daemon's service through this module seam, the same way
  the loop reaches the daemon's config).

PyNaCl is an optional extra (``pip install "cronstable[push]"``): the
import is guarded, and config validation refuses a ``push:`` block when
the library is absent.  Fail closed on purpose: an alerting channel
that silently self-disables is a missed page, the one failure mode a
paging feature must never have.
"""

import asyncio
import base64
import binascii
import contextlib
import datetime
import hashlib
import json
import logging
import os
import secrets
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

try:
    from nacl.public import PublicKey, SealedBox

    HAVE_PYNACL = True
except ImportError:  # pragma: no cover - exercised on the no-push baseline
    HAVE_PYNACL = False

logger = logging.getLogger("cronstable")

#: Version stamp inside every sealed plaintext and relay envelope, so the
#: app and relay can evolve the format without guessing.
PUSH_PROTOCOL_VERSION = 1

#: APNs rejects notifications whose final JSON exceeds 4096 bytes.  The
#: relay wraps our ciphertext in its own envelope (alert stub, headers,
#: the mutable-content marker), so the base64 ciphertext we hand it is
#: capped well below that, leaving the relay ~1 KB of headroom.
CIPHERTEXT_B64_MAX = 3000

#: A libsodium sealed box adds an ephemeral X25519 public key (32 bytes)
#: and a Poly1305 MAC (16 bytes) to the plaintext.
_SEALED_OVERHEAD = 48

#: The largest plaintext whose sealed, base64-encoded form fits the cap.
MAX_PLAINTEXT_BYTES = CIPHERTEXT_B64_MAX // 4 * 3 - _SEALED_OVERHEAD

#: X25519 public keys are exactly 32 bytes.
DEVICE_PUBLIC_KEY_BYTES = 32

#: Durable-state document namespace holding one document per paired
#: device, keyed by device id.  Documents are never swept by state GC,
#: so a pairing survives until it is explicitly revoked.
PUSH_DOC_NAMESPACE = "pushdevice"

#: Namespace for push metadata that is not a device (today: the one
#: ``collapse`` document holding the installation's coalescing salt).
#: Separate from the device namespace so registry listings and revokes
#: can never see or delete it.
PUSH_META_NAMESPACE = "pushmeta"

#: How long the in-memory device mirror is trusted before the next send
#: or listing re-reads the store (a pairing made on another node sharing
#: the state store becomes visible within this window).
REGISTRY_REFRESH_SECONDS = 60.0

#: How long a *failed* registry read is remembered before the reporting
#: path tries again.  Without it a wedged store charges every alert its
#: own :data:`STORE_OP_TIMEOUT`, serialized behind the refresh lock: a
#: burst of 100 alerts would leave the last one roughly 100 timeouts
#: late, on the one channel whose whole job is to page someone now.  Far
#: shorter than the success window, so a store that comes back is
#: visible again within a few seconds rather than a minute.
REGISTRY_RETRY_SECONDS = 5.0

#: Bound every store operation the same way cron's state writes are
#: bounded, so a wedged shared mount cannot stall a report fan-out or a
#: pairing request forever.
STORE_OP_TIMEOUT = 10.0

#: How old an abandoned devices-file write-temp must be before a later
#: write sweeps it.  Unique temp names (see
#: :meth:`FileDeviceStore._write`) mean a process killed mid-write leaves
#: one behind that nothing would ever reuse, so without a sweep the
#: registry directory accretes them one per event forever.  Matches
#: :data:`cronstable.state.TMP_MAX_AGE`, and far longer than any write
#: could legitimately take, so a live write is never a candidate.
TMP_MAX_AGE = 86400.0

#: How long a devices-file operation waits for the one ahead of it to
#: leave the file (see :attr:`FileDeviceStore._file_lock`).  Half the op
#: budget, so an operation that does win the fence still has half its
#: time left to do its own work.  Waiting the full budget instead would
#: be waiting on a predecessor that is wedged by definition: the fence is
#: only ever contended after an earlier op already blew its own
#: :data:`STORE_OP_TIMEOUT`, and the awaiter would time out at the same
#: instant anyway, so it bought nothing and reported the wrong cause.
STORE_LOCK_WAIT = STORE_OP_TIMEOUT / 2

#: How many trailing captured output lines a job alert starts from
#: before size trimming; the fit loop drops oldest-first from there.
LOG_TAIL_MAX_LINES = 40

_FIELD_LIMITS = {"name": 64, "platform": 32, "pushToken": 512}


class PushError(Exception):
    """A push operation failed (bad device material, store trouble)."""


def _utcnow_iso() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def validate_public_key(value: Any) -> str:
    """Normalize and validate a device public key (base64 X25519).

    Returns the canonical (re-encoded) base64 form; raises
    :class:`PushError` with an operator-readable reason otherwise.
    """
    if not isinstance(value, str) or not value.strip():
        raise PushError("publicKey is required (base64 X25519 key)")
    try:
        raw = base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError):
        raise PushError("publicKey is not valid base64") from None
    if len(raw) != DEVICE_PUBLIC_KEY_BYTES:
        raise PushError(
            "publicKey must decode to exactly {} bytes, got {}".format(
                DEVICE_PUBLIC_KEY_BYTES, len(raw)
            )
        )
    if HAVE_PYNACL:
        # 32 bytes is not enough: libsodium refuses to seal to all-zero /
        # low-order points, and it does so at encrypt time, not at key
        # construction. Probe a real seal here so an unusable key is a
        # 400 at pairing instead of a persistent registry record that
        # fails on every alert until an operator revokes it.
        try:
            SealedBox(PublicKey(raw)).encrypt(b"probe")
        except Exception:
            raise PushError(
                "publicKey is not a usable X25519 public key"
            ) from None
    return base64.b64encode(raw).decode("ascii")


def _validate_field(payload: Dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PushError("{} is required".format(field))
    value = value.strip()
    if len(value) > _FIELD_LIMITS[field]:
        raise PushError(
            "{} is longer than {} characters".format(
                field, _FIELD_LIMITS[field]
            )
        )
    return value


def validate_pairing(payload: Any) -> Dict[str, str]:
    """Validate a ``POST /push/devices`` body into a clean field dict.

    Raises :class:`PushError` with a message safe to return in a 400.
    """
    if not isinstance(payload, dict):
        raise PushError("body must be a JSON object")
    return {
        "name": _validate_field(payload, "name"),
        "platform": _validate_field(payload, "platform"),
        "pushToken": _validate_field(payload, "pushToken"),
        "publicKey": validate_public_key(payload.get("publicKey")),
    }


def seal_to_device(public_key_b64: str, plaintext: bytes) -> str:
    """Seal ``plaintext`` to a device public key; return base64 text.

    Anonymous-sender sealed box: an ephemeral key pair per message, so
    the daemon holds no long-lived sending secret and only the device's
    private key (which never leaves the phone) can open it.
    """
    if not HAVE_PYNACL:  # pragma: no cover - config validation gates this
        raise PushError(
            "PyNaCl is not installed; install the push extra "
            '(pip install "cronstable[push]")'
        )
    try:
        raw = base64.b64decode(public_key_b64, validate=True)
        # encrypt stays INSIDE the try: libsodium rejects all-zero /
        # low-order keys at encrypt time (nacl.exceptions.RuntimeError,
        # not a PushError), and one bad registry record must surface as
        # a per-device PushError, never escape a whole-fleet fan-out.
        sealed = SealedBox(PublicKey(raw)).encrypt(plaintext)
    except Exception as exc:
        raise PushError(
            "device public key is unusable: {}".format(exc)
        ) from None
    return base64.b64encode(sealed).decode("ascii")


def _encode(payload: Dict[str, Any]) -> bytes:
    return json.dumps(
        payload, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def build_payload(
    ctx: Any, success: bool, include_log_tail: bool
) -> Dict[str, Any]:
    """The sealed plaintext for one alert, before size fitting.

    ``ctx`` is duck-typed exactly as the other reporters take it: a
    :class:`~cronstable.job.RunningJob`, an SLA breach context or a
    ``notify:`` event context.  All three expose ``template_vars`` with
    the standard key set; the event and SLA contexts are recognized by
    their ``event`` / ``sla_check`` attributes.
    """
    tv = ctx.template_vars
    event = getattr(ctx, "event", None)
    sla_check = getattr(ctx, "sla_check", None)
    if event is not None:
        kind = "event"
    elif sla_check is not None:
        kind = "sla"
    else:
        kind = "success" if success else "failure"
    payload: Dict[str, Any] = {
        "v": PUSH_PROTOCOL_VERSION,
        "kind": kind,
        "name": tv.get("name"),
        "success": bool(success),
        "host": tv.get("host"),
        "ts": _utcnow_iso(),
    }
    for field in (
        "run_id",
        "schedule",
        "started_at",
        "exit_code",
        "fail_reason",
    ):
        value = tv.get(field)
        if value not in (None, ""):
            payload[field] = value
    if kind == "event":
        payload["event"] = event
        payload["subject"] = tv.get("subject")
        payload["message"] = tv.get("message")
        for field in ("dag", "run_key", "taskkey", "role", "leader"):
            value = tv.get(field)
            if value not in (None, ""):
                payload[field] = value
    elif kind == "sla":
        payload["sla_check"] = sla_check
        for field in (
            "threshold_seconds",
            "observed_seconds",
            "last_success_at",
        ):
            value = tv.get(field)
            if value is not None:
                payload[field] = value
    elif include_log_tail:
        # stderr first: it is captured by default and is where a failing
        # job's reason usually lives; stdout only when that's all there
        # is.  Event/SLA alerts have no process, so no tail.
        text = tv.get("stderr") or tv.get("stdout")
        if text:
            payload["log_tail"] = text.splitlines()[-LOG_TAIL_MAX_LINES:]
    return payload


def fit_payload(payload: Dict[str, Any]) -> bytes:
    """Shrink ``payload`` in place until it seals under the APNs cap.

    Trim order: oldest log-tail lines first (the newest lines carry the
    failure), then the long free-text fields by halving, so the alert
    always keeps its identity (name, kind, host) intact.  Returns the
    encoded plaintext.
    """
    data = _encode(payload)
    while len(data) > MAX_PLAINTEXT_BYTES:
        tail = payload.get("log_tail")
        if tail:
            del tail[0]
            if not tail:
                del payload["log_tail"]
        else:
            for field in ("message", "fail_reason", "subject"):
                value = payload.get(field)
                if isinstance(value, str) and len(value) > 64:
                    payload[field] = value[: max(64, len(value) // 2)]
                    break
            else:
                # Nothing long is left; the residual overage can only
                # come from many short fields, so drop the optional
                # context ones until the identity core fits.
                for field in ("schedule", "started_at", "run_id"):
                    if field in payload:
                        del payload[field]
                        break
                else:  # pragma: no cover - identity core is tiny
                    break
        data = _encode(payload)
    return data


def collapse_id(payload: Dict[str, Any], salt: str) -> str:
    """An opaque coalescing key for the relay: same alert, same id.

    A keyed hash of the alert's identity fields, so the relay can
    deduplicate the same (job, run) reported by several nodes without
    learning the job name or run id.  ``salt`` is the per-installation
    secret kept beside the device registry (see ``ensure_salt`` on the
    stores): without it the identity fields are low-entropy (on a
    stateless install ``run_id`` is absent, leaving only kind + name),
    and a relay could recover job names from a precomputed wordlist.
    The salt is shared by every node using the registry, so cross-node
    coalescing still works; it is never sent to the relay.
    """
    ident = {
        key: payload[key]
        for key in (
            "kind",
            "name",
            "run_id",
            "event",
            "dag",
            "run_key",
            "taskkey",
            "sla_check",
        )
        if payload.get(key) not in (None, "")
    }
    blob = json.dumps(ident, sort_keys=True, separators=(",", ":"))
    keyed = salt.encode("utf-8") + b"\x00" + blob.encode("utf-8")
    return hashlib.sha256(keyed).hexdigest()[:32]


def key_fingerprint(public_key_b64: Optional[str]) -> Optional[str]:
    """A short, human-comparable fingerprint of a device public key.

    Pairing happens over whatever transport the operator exposed the
    web API on; nothing in the protocol proves the key the daemon
    stored is the key the phone generated.  This fingerprint is shown
    in the device listing and pairing response so the operator can
    compare it against the one the companion app displays, closing the
    key-substitution hole an on-path attacker (plaintext HTTP, hostile
    LAN) would otherwise have.  SHA-256 over the raw 32 key bytes,
    first 12 hex chars grouped for reading aloud.
    """
    if not public_key_b64:
        return None
    try:
        raw = base64.b64decode(public_key_b64, validate=True)
    except (binascii.Error, ValueError):
        return None
    digest = hashlib.sha256(raw).hexdigest()[:12]
    return "-".join(digest[i : i + 4] for i in range(0, 12, 4))


def public_device(device: Dict[str, Any]) -> Dict[str, Any]:
    """A device record as served by ``GET /push/devices``.

    The push token is redacted to its tail: it is not key material, but
    it is the one field that lets a third party address this device
    through the platform push service, so the listing never echoes it
    whole.  The public key is public by definition and returned intact
    (the app re-checks it against its own on screen), and its
    ``fingerprint`` is included for the human comparison step (see
    :func:`key_fingerprint`).
    """
    token = device.get("pushToken") or ""
    return {
        "id": device.get("id"),
        "name": device.get("name"),
        "platform": device.get("platform"),
        "publicKey": device.get("publicKey"),
        "fingerprint": key_fingerprint(device.get("publicKey")),
        "pushToken": "…" + token[-6:] if token else "",
        "createdAt": device.get("createdAt"),
        "createdBy": device.get("createdBy"),
    }


async def _call_on_daemon_thread(fn: Callable[[], Any], name: str) -> Any:
    """Run blocking ``fn`` on a private daemon thread and await it.

    Deliberately not ``loop.run_in_executor(None, ...)``, mirroring
    :meth:`cronstable.state.FilesystemStateBackend._call` and for the
    same two reasons.  The default executor is shared with the rest of
    the daemon (the once-a-minute config reload runs there, untimed), and
    a registry op abandoned on timeout leaves its thread parked in
    whatever syscall wedged it: against a dead hard mount the shared pool
    loses a worker per attempt until the reload has no thread left to run
    on and the scheduler stops firing jobs.  Its threads are also
    non-daemonic and joined at interpreter exit, so one stuck op would
    hang shutdown until something sends SIGKILL.  A daemon thread per
    call keeps a hung store abandonable: the awaiter times out, the
    thread is reclaimed with the process.  Registry ops are rare (a
    pairing, a listing, one salt read per refresh window), so a thread
    per call costs nothing worth optimizing.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()

    def _resolve(result: Any, exc: Optional[BaseException]) -> None:
        if future.cancelled():
            return  # the awaiter timed out and moved on; nobody to tell
        if exc is not None:
            future.set_exception(exc)
        else:
            future.set_result(result)

    def _runner() -> None:
        result: Any = None
        exc: Optional[BaseException] = None
        try:
            result = fn()
        except BaseException as ex:  # noqa: BLE001 - relayed to the awaiter
            exc = ex
        try:
            loop.call_soon_threadsafe(_resolve, result, exc)
        except RuntimeError:
            # the loop closed while we were blocked (late finish during
            # teardown): nothing is waiting, so drop the result.
            pass

    threading.Thread(target=_runner, daemon=True, name=name).start()
    return await future


#: One fence per devices-file PATH, shared by every store object pointed
#: at it.  ``Cron.start_stop_push`` builds a fresh :class:`FileDeviceStore`
#: on every push-config change, so a per-instance lock would leave a worker
#: abandoned by the old instance racing the new one's writers: a reload is
#: exactly when someone is most likely to be poking at a wedged install.
#: One entry per configured path, so the map cannot grow.
_FILE_LOCKS: Dict[str, threading.Lock] = {}
_FILE_LOCKS_GUARD = threading.Lock()


def _file_lock_for(path: str) -> threading.Lock:
    """The process-wide fence for one devices-file path."""
    key = os.path.normcase(os.path.abspath(path))
    with _FILE_LOCKS_GUARD:
        lock = _FILE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _FILE_LOCKS[key] = lock
        return lock


class FileDeviceStore:
    """Paired devices in one local JSON file (stateless installs).

    Writes are atomic (a uniquely named temp file + ``os.replace``) and
    serialized process-wide per file path, including against an operation
    that already timed out but whose worker is still inside the file; the
    file is created with owner-only permissions where the platform honors
    them.  A corrupt file refuses writes instead of clobbering what might
    still be recoverable pairings.
    """

    kind = "file"

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        # Held by the worker THREAD for the whole read-modify-write, which
        # the loop-side asyncio lock above cannot do: when an op times out
        # its thread is abandoned mid-sequence and the asyncio lock is
        # released, so without this the next op would interleave with a
        # worker still holding a stale device list, resurrecting a revoked
        # pairing or tearing the file.  Keyed by path, not owned by this
        # object, so the fence survives the rebuild a config reload does
        # (see :data:`_FILE_LOCKS`).
        self._file_lock = _file_lock_for(path)
        self._corrupt: Optional[str] = None
        self._salt: Optional[str] = None

    def describe(self) -> str:
        return "file:{}".format(self.path)

    async def _run(self, fn: Callable[[], Any], doing: str) -> Any:
        """One blocking file op, off-loop, fenced and bounded.

        The devices file may live on a network mount; the read/write
        itself runs off the event loop so a wedged mount can never freeze
        it, and the wait is capped at :data:`STORE_OP_TIMEOUT` like every
        other store op.  The op runs under :attr:`_file_lock` so an
        abandoned predecessor still finishes alone (see
        :meth:`__init__`), and on a private daemon thread rather than the
        shared executor (see :func:`_call_on_daemon_thread`).
        """

        def _guarded() -> Any:
            # A bound, not a wait forever: on a permanently wedged mount
            # the holder never returns, and a thread queued behind it must
            # be able to give up and die rather than park for good.
            if not self._file_lock.acquire(timeout=STORE_LOCK_WAIT):
                raise PushError(
                    "timed out waiting for an earlier push devices file "
                    "operation on {} to finish".format(self.path)
                )
            try:
                return fn()
            finally:
                self._file_lock.release()

        try:
            return await asyncio.wait_for(
                _call_on_daemon_thread(_guarded, "cronstable-push"),
                timeout=STORE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise PushError(
                "timed out {} push devices file {}".format(doing, self.path)
            ) from None
        except PushError:
            raise
        except Exception as exc:
            # Same normalization as StateDeviceStore._bounded: every
            # caller up to send_report and the pairing handlers expects
            # PushError and nothing else, and an escapee here would take
            # the rest of the housekeeping pass with it.
            raise PushError(
                "{} push devices file {} failed: {}: {}".format(
                    doing, self.path, type(exc).__name__, exc
                )
            ) from None

    def _read(self) -> List[Dict[str, Any]]:
        try:
            with open(self.path, "rt", encoding="utf-8") as stream:
                doc = json.load(stream)
        except FileNotFoundError:
            # A missing file is a well-defined empty registry, not damage:
            # clear any stale corrupt flag, or an operator who removed a
            # bad file (exactly what the write-refusal message tells them
            # to do) would stay locked out of writes until a daemon
            # restart, since this store object lives as long as the push
            # config is unchanged.
            self._corrupt = None
            return []
        except (OSError, ValueError) as exc:
            self._corrupt = str(exc)
            raise PushError(
                "push devices file {} is unreadable: {}".format(self.path, exc)
            ) from None
        devices = doc.get("devices") if isinstance(doc, dict) else None
        if not isinstance(devices, list):
            self._corrupt = "not a {version, devices} object"
            raise PushError(
                "push devices file {} has an unexpected shape".format(
                    self.path
                )
            )
        self._corrupt = None
        salt = doc.get("collapseSalt")
        if isinstance(salt, str) and salt:
            self._salt = salt
        return [d for d in devices if isinstance(d, dict)]

    def _sweep_stale_temps(self) -> None:
        """Drop write-temps an earlier process died without renaming.

        The unique names that keep two writers out of one buffer also
        mean nothing ever reuses the one an abandoned write leaves: a
        SIGKILL, an OOM kill or a power loss mid-write, or a worker this
        module deliberately abandons on a wedged mount and then never
        joins at exit.  The old fixed ``.tmp`` name self-limited to one
        such file; unique names would accrete one per event forever,
        beside the operator's registry.  Best-effort and entirely
        advisory: a directory we cannot list is not a reason to fail a
        pairing, and the age bound keeps a live write out of range.
        """
        directory = os.path.dirname(self.path) or "."
        prefix = os.path.basename(self.path) + "."
        cutoff = time.time() - TMP_MAX_AGE
        with contextlib.suppress(OSError):
            for name in os.listdir(directory):
                if not (name.startswith(prefix) and name.endswith(".tmp")):
                    continue
                stale = os.path.join(directory, name)
                with contextlib.suppress(OSError):
                    if os.stat(stale).st_mtime < cutoff:
                        os.unlink(stale)

    def _write(self, devices: List[Dict[str, Any]]) -> None:
        if self._corrupt is not None:
            # Never overwrite a file we could not parse: the operator
            # may still recover pairings from it by hand.
            raise PushError(
                "refusing to overwrite unreadable devices file {} "
                "({}); fix or remove it first".format(self.path, self._corrupt)
            )
        doc: Dict[str, Any] = {"version": 1, "devices": devices}
        if self._salt:
            doc["collapseSalt"] = self._salt
        # A unique name plus O_EXCL, the way
        # cronstable.state.FilesystemStateBackend._atomic_write does it.  A
        # fixed ".tmp" opened O_TRUNC is a shared mutable file: two writers
        # (an abandoned op's thread and its successor, or a second daemon
        # pointed at the same path) would interleave into one buffer and
        # then rename each other's half-written bytes over the registry,
        # which _read refuses to parse and _write then refuses to repair.
        # O_EXCL turns a name collision into an error instead of a silent
        # join.
        tmp = "{}.{}-{}.tmp".format(
            self.path, os.getpid(), secrets.token_hex(4)
        )
        self._sweep_stale_temps()
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(fd, "wt", encoding="utf-8") as stream:
                    json.dump(doc, stream, indent=2, sort_keys=True)
                os.replace(tmp, self.path)
            except BaseException:
                # never leave the temp file behind, on a failed write OR a
                # failed rename (the old shape leaked one on the latter)
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
        except OSError as exc:
            # Same bounded-PushError contract as _read: a missing or
            # read-only directory must become the 503 the pairing
            # handlers promise, not an aiohttp 500.
            raise PushError(
                "push devices file {} is not writable: {}".format(
                    self.path, exc
                )
            ) from None

    async def load(self) -> List[Dict[str, Any]]:
        async with self._lock:
            devices: List[Dict[str, Any]] = await self._run(
                self._read, "reading"
            )
            return devices

    async def upsert(self, device: Dict[str, Any]) -> None:
        def _do() -> None:
            devices = self._read()
            devices = [d for d in devices if d.get("id") != device.get("id")]
            devices.append(device)
            self._write(devices)

        async with self._lock:
            await self._run(_do, "writing")

    async def remove(self, device_id: str) -> bool:
        def _do() -> bool:
            devices = self._read()
            kept = [d for d in devices if d.get("id") != device_id]
            if len(kept) == len(devices):
                return False
            self._write(kept)
            return True

        async with self._lock:
            removed: bool = await self._run(_do, "writing")
            return removed

    async def ensure_salt(self) -> str:
        """The per-installation collapse salt, created on first use.

        Persisted as ``collapseSalt`` beside the devices so every
        process using this file derives the same coalescing ids (see
        :func:`collapse_id`); preserved verbatim by every rewrite.
        """

        def _do() -> str:
            devices = self._read()
            if not self._salt:
                self._salt = secrets.token_hex(16)
                self._write(devices)
            return self._salt

        async with self._lock:
            salt: str = await self._run(_do, "initializing")
            return salt


class StateDeviceStore:
    """Paired devices as durable-state documents (one per device).

    One document per device in the :data:`PUSH_DOC_NAMESPACE` namespace,
    so register and revoke are per-key atomic operations (no read-
    modify-write races between nodes pairing at once) and every node
    sharing the store sees the same registry.  Documents are never
    swept by state GC, so a pairing lives until explicitly revoked.

    ``get_backend`` is a callable, not a backend reference: the state
    backend is torn down and rebuilt on config reloads, and the store
    must always talk to the current one (or fail loudly when there is
    none, e.g. mid-reload).
    """

    kind = "state"

    def __init__(self, get_backend: Callable[[], Optional[Any]]) -> None:
        self._get_backend = get_backend

    def describe(self) -> str:
        return "state:{}/".format(PUSH_DOC_NAMESPACE)

    def _backend(self) -> Any:
        backend = self._get_backend()
        if backend is None:
            raise PushError(
                "the durable state store is not available; device "
                "pairing needs it (or configure push.devicesFile)"
            )
        return backend

    async def _bounded(self, operation: str, awaitable: Any) -> Any:
        """Await one store op under the bounded-PushError contract.

        The state backends surface trouble as their own exception types:
        raw OSError for I/O (an NFS ESTALE, a vanished mount), and
        private ones such as ``_DocumentUnreadable`` for a document they
        cannot trust (corrupt bytes, a torn write, an unknown schema).
        Every caller up to send_report, the pairing handlers and
        ``Cron.start_stop_push`` expects PushError and nothing else, so
        this is where a backend's exception vocabulary stops.

        Catching only OSError was not enough, and failed in the quietest
        possible way: one unreadable ``pushmeta/collapse`` document made
        ``ensure_salt`` raise past ``start_stop_push`` into the
        housekeeping pass, which skipped everything after it, including
        the durable-state manifest and garbage collection, on that pass
        and on every later one (the push config never records as applied,
        so the same read is retried and raises again forever).  Note
        ``Exception``, not ``BaseException``: a cancelled housekeeping
        pass must still cancel.
        """
        try:
            return await asyncio.wait_for(awaitable, timeout=STORE_OP_TIMEOUT)
        except asyncio.TimeoutError:
            raise PushError(
                "timed out {} in the state store".format(operation)
            ) from None
        except PushError:
            raise
        except Exception as exc:
            # The type name is part of the message: a backend's internal
            # exception often stringifies to a bare token (an unreadable
            # document says only "unknown-schema-or-not-a-document"),
            # which is unactionable on its own in an operator's log.
            raise PushError(
                "{} in the state store failed: {}: {}".format(
                    operation, type(exc).__name__, exc
                )
            ) from None

    async def load(self) -> List[Dict[str, Any]]:
        backend = self._backend()
        docs = await self._bounded(
            "listing paired devices",
            backend.list_documents(PUSH_DOC_NAMESPACE),
        )
        return [d for d in docs if isinstance(d, dict) and d.get("id")]

    async def upsert(self, device: Dict[str, Any]) -> None:
        backend = self._backend()

        def _put(_current: Optional[Dict[str, Any]]) -> Tuple[Any, None]:
            # Pure and idempotent: mutate_document may retry it on a
            # torn read, and it runs on the store's worker thread.
            return dict(device), None

        await self._bounded(
            "writing the device pairing",
            backend.mutate_document(PUSH_DOC_NAMESPACE, device["id"], _put),
        )

    async def remove(self, device_id: str) -> bool:
        backend = self._backend()
        return bool(
            await self._bounded(
                "revoking the device",
                backend.delete_document(PUSH_DOC_NAMESPACE, device_id),
            )
        )

    async def ensure_salt(self) -> str:
        """The per-installation collapse salt, created on first use.

        One reserved document in :data:`PUSH_META_NAMESPACE`;
        ``mutate_document``'s fleet-wide lock makes create-if-absent
        atomic, so every node sharing the store converges on the same
        salt (the property cross-node coalescing depends on).
        """
        # Lazy import, matching config's treatment of this module: only
        # state-backed installs pay for cronstable.state here.
        from cronstable.state import DOC_KEEP

        backend = self._backend()
        candidate = secrets.token_hex(16)

        def _ensure(current: Optional[Dict[str, Any]]) -> Tuple[Any, str]:
            existing = (current or {}).get("salt")
            if isinstance(existing, str) and existing:
                return DOC_KEEP, existing
            return {"salt": candidate}, candidate

        mutated: Tuple[Any, str] = await self._bounded(
            "reading the collapse salt",
            backend.mutate_document(PUSH_META_NAMESPACE, "collapse", _ensure),
        )
        return mutated[1]


class PushService:
    """The daemon-global push engine: registry mirror + relay client.

    Interactive paths (the ``/push/devices`` handlers) await the store
    directly so a pairing either durably happened or the caller gets an
    error.  The reporting path reads only the in-memory mirror,
    refreshed at most every :data:`REGISTRY_REFRESH_SECONDS`, so a slow
    or absent store can delay a *new* pairing taking effect but can
    never stall a report fan-out.
    """

    def __init__(
        self,
        *,
        relay_url: str,
        relay_timeout: float,
        store: Any,
        host: str,
    ) -> None:
        self.relay_url = relay_url
        self.relay_timeout = relay_timeout
        self.store = store
        self.host = host
        self._devices: Dict[str, Dict[str, Any]] = {}
        self._mirror_fresh_until = 0.0
        # Why the last read failed, or None while the mirror is trusted.
        # The freshness deadline alone cannot say that: inside the retry
        # window it reads exactly like a successful load (see refresh).
        self._registry_error: Optional[str] = None
        self._refresh_lock = asyncio.Lock()
        # The persistent coalescing salt (see collapse_id), fetched with
        # the first successful refresh. Until the store yields one, the
        # process-local fallback keeps ids unpredictable to the relay at
        # the cost of cross-node/restart coalescing -- the safe side of
        # that trade.
        self._collapse_salt: Optional[str] = None
        self._local_salt = secrets.token_hex(16)

    async def start(self) -> None:
        """Warm the device mirror; never fatal (the store may be down)."""
        try:
            await self.refresh(force=True)
        except PushError as exc:
            logger.warning(
                "push: could not load the device registry yet (%s); "
                "will keep retrying on demand",
                exc,
            )
        else:
            logger.info(
                "push: %d paired device(s) loaded from %s",
                len(self._devices),
                self.store.describe(),
            )

    async def refresh(self, force: bool = False) -> None:
        """Re-read the registry unless the mirror is still fresh.

        A failed read starts a short :data:`REGISTRY_RETRY_SECONDS`
        window during which the non-forced (reporting) path keeps the
        mirror it has instead of trying again.  Without that window the
        cost of a wedged store was paid per alert and serially: each
        caller waited out the one ahead of it on ``_refresh_lock``, then
        found the mirror still stale (only success moved the deadline)
        and started its own :data:`STORE_OP_TIMEOUT`, so a burst of
        alerts multiplied the outage instead of absorbing it.  The
        deadline now moves on failure too, so the burst costs one
        timeout between them, which is what this class's docstring
        promises the reporting path.

        ``force`` is the interactive contract (listing, pairing,
        revoking): an operator asking about pairings must see the store
        and may wait for it, so it ignores both windows.
        """
        now = asyncio.get_running_loop().time()
        if not force and now < self._mirror_fresh_until:
            return
        async with self._refresh_lock:
            now = asyncio.get_running_loop().time()
            if not force and now < self._mirror_fresh_until:
                return
            try:
                devices = await self.store.load()
            except PushError as exc:
                # Remember the reason as well as the deadline.  Inside the
                # window the reporting path skips the store entirely, so
                # an alert that then found the mirror empty would report
                # "no device is paired" and send someone to the pairing
                # endpoint while the real fault is the registry store.
                self._registry_error = str(exc)
                # max(), never a plain assignment: this also runs for a
                # FAILED forced refresh, and a still-valid positive window
                # must not be cut short by one.  An operator opening the
                # pairing page against a wedged store would otherwise
                # shorten a good 60-second mirror to 5 and push the next
                # alert back onto a store already known to be down.
                self._mirror_fresh_until = max(
                    self._mirror_fresh_until,
                    asyncio.get_running_loop().time() + REGISTRY_RETRY_SECONDS,
                )
                raise
            self._registry_error = None
            self._devices = {d["id"]: d for d in devices if d.get("id")}
            self._mirror_fresh_until = (
                asyncio.get_running_loop().time() + REGISTRY_REFRESH_SECONDS
            )
            if self._collapse_salt is None:
                try:
                    self._collapse_salt = await self.store.ensure_salt()
                except PushError as exc:
                    logger.warning(
                        "push: no persistent collapse salt yet (%s); "
                        "using a process-local one (alerts still send; "
                        "cross-node coalescing resumes when the "
                        "registry store recovers)",
                        exc,
                    )

    def devices_payload(self) -> List[Dict[str, Any]]:
        devices = sorted(
            self._devices.values(), key=lambda d: d.get("createdAt") or ""
        )
        return [public_device(d) for d in devices]

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        return self._devices.get(device_id)

    async def pair(
        self, fields: Dict[str, str], created_by: Optional[str]
    ) -> Tuple[Dict[str, Any], bool]:
        """Register (or re-register) a device; returns (record, created).

        Re-pairing is keyed on the public key: the same device pairing
        again (APNs tokens rotate; people rename phones) updates its
        record in place instead of accumulating duplicates, keeping its
        id and createdAt so revocation references stay stable.
        """
        await self.refresh(force=True)
        existing = next(
            (
                d
                for d in self._devices.values()
                if d.get("publicKey") == fields["publicKey"]
            ),
            None,
        )
        if existing is not None:
            record = dict(existing)
            record.update(fields)
            created = False
        else:
            record = dict(fields)
            record["id"] = secrets.token_hex(8)
            record["createdAt"] = _utcnow_iso()
            record["createdBy"] = created_by
            created = True
        await self.store.upsert(record)
        self._devices[record["id"]] = record
        return record, created

    async def revoke(self, device_id: str) -> bool:
        await self.refresh(force=True)
        removed = await self.store.remove(device_id)
        self._devices.pop(device_id, None)
        return bool(removed)

    async def send_report(
        self, ctx: Any, success: bool, push_config: Dict[str, Any]
    ) -> None:
        """Fan one alert out to every paired device (the reporter path).

        Failures are logged per device and never raised: this runs
        inside the reporter gather, and a relay outage must not look
        like a reporting crash.
        """
        try:
            await self.refresh()
        except PushError as exc:
            logger.warning(
                "push: device registry unavailable (%s); sending to the "
                "%d last-known device(s)",
                exc,
                len(self._devices),
            )
        if not self._devices:
            name = getattr(getattr(ctx, "config", None), "name", "?")
            if self._registry_error is not None:
                # Not the same diagnosis at all: there may well be
                # pairings, we just cannot read them.
                logger.warning(
                    "push: the device registry is unavailable (%s) and no "
                    "pairing is known here yet; dropping alert for %s",
                    self._registry_error,
                    name,
                )
            else:
                logger.warning(
                    "push: report is enabled but no device is paired; "
                    "dropping alert for %s (pair one at POST /push/devices)",
                    name,
                )
            return
        payload = build_payload(
            ctx, success, bool(push_config.get("includeLogTail", True))
        )
        results = await self._send_payload(
            payload,
            priority=push_config.get("priority", "time-sensitive"),
        )
        for result in results:
            if result.get("error"):
                logger.error(
                    "push: delivery to device %s failed: %s",
                    result["device"],
                    result["error"],
                )

    async def send_test(self, device: Dict[str, Any]) -> Dict[str, Any]:
        """Send a test alert to one device; returns the relay outcome."""
        payload = {
            "v": PUSH_PROTOCOL_VERSION,
            "kind": "test",
            "name": "test",
            "success": True,
            "host": self.host,
            "message": "test alert from cronstable",
            "ts": _utcnow_iso(),
        }
        # A random collapse id per test: every daemon's test alerts would
        # otherwise share one id (constant kind/name) and a coalescing
        # relay would swallow all but the first -- the worst outcome for
        # the one alert whose entire job is "prove delivery works".
        results = await self._send_payload(
            payload,
            priority="time-sensitive",
            only=device,
            collapse=secrets.token_hex(16),
        )
        return results[0]

    async def _send_payload(
        self,
        payload: Dict[str, Any],
        *,
        priority: str,
        only: Optional[Dict[str, Any]] = None,
        collapse: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        plaintext = fit_payload(payload)
        coalesce = collapse or collapse_id(
            payload, self._collapse_salt or self._local_salt
        )
        is_event = payload.get("kind") == "event"
        targets = [only] if only else list(self._devices.values())
        timeout = aiohttp.ClientTimeout(total=self.relay_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # All devices at once, not one after another.  The POSTs are
            # independent, and serially a relay that black-holes packets
            # charged the caller one relay_timeout PER PAIRED DEVICE
            # inside report_failure: on a family of phones that is minutes
            # spent before the run's retry is even armed, and minutes
            # added to the shutdown drain, which is unbounded precisely
            # because every reporter was assumed to be time-bounded.
            # Fanned out, a dead relay costs one timeout no matter how
            # many devices are paired.
            outcomes = await asyncio.gather(
                *(
                    self._send_to_device(
                        session,
                        device,
                        plaintext,
                        coalesce,
                        priority,
                        is_event,
                    )
                    for device in targets
                ),
                return_exceptions=True,
            )
        results = []
        for device, outcome in zip(targets, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                if isinstance(outcome, asyncio.CancelledError):
                    # One leg came back cancelled.  return_exceptions
                    # files that like any other result, and recording it
                    # as a delivery error would swallow a cancellation
                    # the caller still has to see.  (Cancelling the
                    # fan-out itself propagates straight out of the
                    # gather, so this is the odd single-leg case.)
                    raise outcome
                # Anything else is one device's problem and must not cost
                # the others their alert; sequentially this aborted the
                # whole remaining fan-out, concurrently it would cancel
                # the siblings still in flight.
                results.append(
                    {
                        "device": device.get("id"),
                        "status": None,
                        "error": "unexpected {}: {}".format(
                            type(outcome).__name__, outcome
                        ),
                    }
                )
            else:
                results.append(outcome)
        return results

    async def _send_to_device(
        self,
        session: aiohttp.ClientSession,
        device: Dict[str, Any],
        plaintext: bytes,
        coalesce: str,
        priority: str,
        is_event: bool,
    ) -> Dict[str, Any]:
        """Seal and POST one alert; the outcome, never an exception."""
        outcome: Dict[str, Any] = {
            "device": device.get("id"),
            "status": None,
            "error": None,
        }
        try:
            ciphertext = seal_to_device(device["publicKey"], plaintext)
        except (PushError, KeyError) as exc:
            outcome["error"] = "sealing failed: {}".format(exc)
            return outcome
        body = {
            "v": PUSH_PROTOCOL_VERSION,
            "device": device.get("pushToken"),
            "ciphertext": ciphertext,
            "collapseId": coalesce,
            "priority": priority,
            "event": is_event,
        }
        try:
            async with session.post(self.relay_url, json=body) as resp:
                outcome["status"] = resp.status
                if resp.status >= 400:
                    # Body text only; the URL stays out of logs (webhook
                    # reporter convention).
                    outcome["error"] = "relay HTTP {}: {}".format(
                        resp.status, (await resp.text())[:512]
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            outcome["error"] = "relay unreachable: {}".format(exc)
        return outcome


_service: Optional[PushService] = None


def get_service() -> Optional[PushService]:
    """The running daemon's push service, or None when unconfigured."""
    return _service


def set_service(service: Optional[PushService]) -> None:
    global _service
    _service = service
