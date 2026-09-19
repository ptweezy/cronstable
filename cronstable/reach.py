"""Reach: relay-brokered private access to the daemon.

The wire contract is ``docs/reach-protocol.md``.  This module holds the
protocol's cryptography and codec: the node identity file, the tunnel id
and fingerprint, the relay admission credential, the app-to-daemon
session (handshake, sealed requests, sealed response records), the
WebSocket frame layout, and the replay window.  The runtime that dials
the relay and serves relayed requests through the web app is
:class:`ReachService`, further down.

PyNaCl is imported lazily inside the functions that need it, the same
rule ``push.py`` follows: ``cron.py`` imports this module for every
daemon start, and the reporter-less majority must not pay for libsodium.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import collections
import contextlib
import datetime
import hashlib
import hmac
import json
import logging
import os
import secrets
import shutil
import struct
import sys
import tempfile
import time
from collections.abc import Callable
from typing import Any, Optional
from urllib.parse import unquote, urlparse

from cronstable import platform

logger = logging.getLogger("cronstable")

#: The domain label every derivation and associated-data string starts
#: with, and the WebSocket subprotocol the daemon offers the relay.
LABEL = b"cronstable-reach-v1"
SUBPROTOCOL = "cronstable-reach-v1"

# Binary frame types (daemon <-> relay).
FRAME_OPEN = 0x01
FRAME_DATA = 0x02
FRAME_END = 0x03
FRAME_CANCEL = 0x04
FRAME_REJECT = 0x05
FRAME_WINDOW = 0x06

# CANCEL reasons (relay -> daemon).
CANCEL_CLIENT_GONE = 1
CANCEL_STREAM_CAP = 2
CANCEL_RELAY_STOPPING = 3

# REJECT codes (daemon -> relay).
REJECT_CANNOT_DECRYPT = 1
REJECT_REPLAY = 2
REJECT_BUSY = 3
REJECT_TOO_LARGE = 4
REJECT_INTERNAL = 5

# Sealed request bodies (app -> daemon).
BODY_HELLO = 0x01
BODY_REQ = 0x02

# Response chunks (daemon -> app).
CHUNK_HELLO_REPLY = 0x11
CHUNK_RES = 0x12
CHUNK_NOSESSION = 0x13

# Record kinds inside a RES chunk.
KIND_HEAD = 0x01
KIND_BODY = 0x02
KIND_END = 0x03
KIND_ERROR = 0x04

# WebSocket close codes.
CLOSE_PROTOCOL_ERROR = 4400
CLOSE_BAD_SIGNATURE = 4403
CLOSE_HELLO_TIMEOUT = 4408
CLOSE_SUPERSEDED = 4409

#: The largest WebSocket frame payload either side sends.
MAX_FRAME_PAYLOAD = 1_114_112
#: The inner request body cap the daemon advertises in ``hello``.
MAX_BODY = 1_048_576
#: The largest BODY record the daemon emits.
MAX_CHUNK = 65_536
#: The block the request head and the HEAD record are padded to.
PAD_BLOCK = 256

SESSION_ID_BYTES = 16
SESSION_IDLE_SECONDS = 1800
SESSION_ABSOLUTE_SECONDS = 86_400
SESSION_LRU = 256
REPLAY_WINDOW = 256
#: Per-session limits the daemon applies whatever the relay enforces.
SESSION_INFLIGHT_CAP = 8
RATE_BURST = 60
RATE_REFILL_PER_SECOND = 2.0

NONCE_BYTES = 12
KEY_BYTES = 32
TAG_BYTES = 16
SALT_BYTES = 16
_TUNNEL_ID_CHARS = 43


class ReachError(Exception):
    """A Reach operation failed (bad material, malformed bytes)."""


class ReplayError(ReachError):
    """A request counter was seen before, or fell out of the window."""


class DecryptError(ReachError):
    """A sealed body or record did not open under the session's keys."""


# ------------------------------------------------------------- encodings


def b64url_encode(raw: bytes) -> str:
    """RFC 4648 URL-safe base64 with the padding stripped."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    """The inverse of :func:`b64url_encode`; raises ReachError on garbage."""
    padded = text + "=" * (-len(text) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError, UnicodeEncodeError) as exc:
        raise ReachError("not base64url: {}".format(exc)) from None


def b64_encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def b64_decode(text: str, expected: Optional[int] = None) -> bytes:
    """Standard base64, optionally checked against an exact byte length."""
    try:
        raw = base64.b64decode(text.encode("ascii"), validate=True)
    except (binascii.Error, ValueError, UnicodeEncodeError, AttributeError):
        raise ReachError("not base64") from None
    if expected is not None and len(raw) != expected:
        raise ReachError(
            "expected {} bytes, got {}".format(expected, len(raw))
        )
    return raw


def _nacl_bindings() -> Any:
    from nacl import bindings

    return bindings


def _nacl_signing() -> Any:
    from nacl import signing

    return signing


# ------------------------------------------------------------- identity


def tunnel_id(sign_public: bytes) -> str:
    """The tunnel id: the raw Ed25519 public key, base64url."""
    if len(sign_public) != 32:
        raise ReachError("an Ed25519 public key is 32 bytes")
    return b64url_encode(sign_public)


def tunnel_id_bytes(tid: str) -> bytes:
    """The raw public key behind a tunnel id; refuses anything else."""
    if len(tid) != _TUNNEL_ID_CHARS:
        raise ReachError(
            "a tunnel id is {} characters".format(_TUNNEL_ID_CHARS)
        )
    raw = b64url_decode(tid)
    if len(raw) != 32:
        raise ReachError("a tunnel id decodes to 32 bytes")
    return raw


def key_fingerprint(raw_public: bytes) -> str:
    """The push registry's fingerprint derivation, over raw key bytes."""
    digest = hashlib.sha256(raw_public).hexdigest()[:12]
    return "-".join(digest[i : i + 4] for i in range(0, 12, 4))


class Identity:
    """One node's Reach identity: the two seeds and the admission salt."""

    def __init__(
        self,
        sign_seed: bytes,
        dh_seed: bytes,
        salt: bytes,
        created_at: str,
    ) -> None:
        if len(sign_seed) != 32 or len(dh_seed) != 32:
            raise ReachError("identity seeds are 32 bytes")
        if len(salt) != SALT_BYTES:
            raise ReachError("the identity salt is 16 bytes")
        self.sign_seed = sign_seed
        self.dh_seed = dh_seed
        self.salt = salt
        self.created_at = created_at
        signing = _nacl_signing()
        self._sign_key = signing.SigningKey(sign_seed)
        self.sign_public: bytes = bytes(self._sign_key.verify_key)
        self.dh_public, self.dh_secret = kx_keypair(seed=dh_seed)

    @classmethod
    def generate(cls) -> "Identity":
        now = datetime.datetime.now(datetime.timezone.utc)
        return cls(
            secrets.token_bytes(32),
            secrets.token_bytes(32),
            secrets.token_bytes(SALT_BYTES),
            now.replace(microsecond=0).isoformat(),
        )

    @property
    def tunnel_id(self) -> str:
        return tunnel_id(self.sign_public)

    @property
    def tunnel_id_bytes(self) -> bytes:
        return self.sign_public

    @property
    def fingerprint(self) -> str:
        return key_fingerprint(self.sign_public)

    def sign(self, message: bytes) -> bytes:
        return bytes(self._sign_key.sign(message).signature)

    def to_json(self) -> dict[str, Any]:
        return {
            "v": 1,
            "signSeed": b64_encode(self.sign_seed),
            "dhSeed": b64_encode(self.dh_seed),
            "salt": b64_encode(self.salt),
            "createdAt": self.created_at,
        }

    @classmethod
    def from_json(cls, doc: Any) -> "Identity":
        if not isinstance(doc, dict) or doc.get("v") != 1:
            raise ReachError("identity file is not a v1 reach identity")
        try:
            return cls(
                b64_decode(doc["signSeed"], 32),
                b64_decode(doc["dhSeed"], 32),
                b64_decode(doc["salt"], SALT_BYTES),
                str(doc.get("createdAt", "")),
            )
        except KeyError as exc:
            raise ReachError(
                "identity file lacks {}".format(exc.args[0])
            ) from None


def load_identity(path: str) -> Identity:
    """Read an identity file; ReachError names what is wrong with it."""
    try:
        with open(path, "r", encoding="utf-8") as stream:
            doc = json.load(stream)
    except OSError as exc:
        raise ReachError(
            "cannot read reach identity {}: {}".format(path, exc)
        ) from None
    except ValueError as exc:
        raise ReachError(
            "reach identity {} is not JSON: {}".format(path, exc)
        ) from None
    return Identity.from_json(doc)


def write_identity(path: str, identity: Identity, *, replace: bool) -> None:
    """Write an identity file with mode 0600.

    ``replace=False`` creates the file exclusively (``O_EXCL``), so two
    daemons racing on one path cannot both believe they generated it.
    ``replace=True`` (rotation) writes a uniquely named temp file beside
    the target and renames it over, the ``FileDeviceStore`` pattern.
    """
    payload = json.dumps(identity.to_json(), indent=2, sort_keys=True)
    target = (
        path
        if not replace
        else "{}.{}-{}.tmp".format(path, os.getpid(), secrets.token_hex(4))
    )
    try:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wt", encoding="utf-8") as stream:
                stream.write(payload)
                stream.write("\n")
            if replace:
                os.replace(target, path)
        except BaseException:
            if replace:
                try:
                    os.unlink(target)
                except OSError:
                    pass
            raise
    except OSError as exc:
        raise ReachError(
            "cannot write reach identity {}: {}".format(path, exc)
        ) from None


def ensure_identity(path: str) -> Identity:
    """The identity at ``path``, generated on first use."""
    if os.path.exists(path):
        return load_identity(path)
    identity = Identity.generate()
    write_identity(path, identity, replace=False)
    return identity


# ------------------------------------------------------- relay challenge


def signature_message(relay_host: str, nonce: bytes) -> bytes:
    """The bytes the daemon signs to answer a relay challenge."""
    if len(nonce) != 32:
        raise ReachError("a challenge nonce is 32 bytes")
    return LABEL + b"\x00" + relay_host.encode("utf-8") + b"\x00" + nonce


def verify_challenge(
    sign_public: bytes, relay_host: str, nonce: bytes, signature: bytes
) -> bool:
    """Whether ``signature`` answers the challenge; the relay's check,
    kept here so tests and a fake relay share one spelling."""
    signing = _nacl_signing()
    from nacl.exceptions import BadSignatureError

    try:
        signing.VerifyKey(sign_public).verify(
            signature_message(relay_host, nonce), signature
        )
    except (BadSignatureError, ValueError, TypeError):
        return False
    return True


# --------------------------------------------------------------- admission


def reach_token(bearer: str, salt: bytes, tid_bytes: bytes) -> bytes:
    """The app's relay credential for one bearer against one tunnel."""
    if len(salt) != SALT_BYTES or len(tid_bytes) != 32:
        raise ReachError("bad salt or tunnel id length")
    message = LABEL + b"\x00" + salt + tid_bytes
    return hmac.new(bearer.encode("utf-8"), message, hashlib.sha256).digest()


def admit_id(token: bytes) -> bytes:
    """What the relay stores and compares: SHA-256 of the credential."""
    return hashlib.sha256(token).digest()


def credential_header(token: bytes) -> str:
    """The ``Authorization`` header value the app sends."""
    return "Reach " + b64url_encode(token)


def parse_credential(header: str) -> Optional[bytes]:
    """The raw credential behind an ``Authorization: Reach …`` header, or
    None when the header is not one."""
    scheme, _, rest = header.strip().partition(" ")
    if scheme != "Reach" or not rest:
        return None
    try:
        raw = b64url_decode(rest.strip())
    except ReachError:
        return None
    return raw if len(raw) == KEY_BYTES else None


def admit_ids(bearers: list[str], salt: bytes, tid_bytes: bytes) -> list[str]:
    """The base64 admit set for a token table, sorted for stable diffs."""
    ids = {
        b64_encode(admit_id(reach_token(b, salt, tid_bytes))) for b in bearers
    }
    return sorted(ids)


# --------------------------------------------------------------- primitives


def kx_keypair(seed: Optional[bytes] = None) -> tuple[bytes, bytes]:
    """A libsodium ``crypto_kx`` keypair, ``(public, secret)``."""
    bindings = _nacl_bindings()
    if seed is None:
        pk, sk = bindings.crypto_kx_keypair()
    else:
        pk, sk = bindings.crypto_kx_seed_keypair(seed)
    return bytes(pk), bytes(sk)


def client_session_keys(
    client_pk: bytes, client_sk: bytes, server_pk: bytes
) -> tuple[bytes, bytes]:
    """``(rx, tx)`` on the client side of a ``crypto_kx`` exchange."""
    rx, tx = _nacl_bindings().crypto_kx_client_session_keys(
        client_pk, client_sk, server_pk
    )
    return bytes(rx), bytes(tx)


def server_session_keys(
    server_pk: bytes, server_sk: bytes, client_pk: bytes
) -> tuple[bytes, bytes]:
    """``(rx, tx)`` on the server side of a ``crypto_kx`` exchange."""
    rx, tx = _nacl_bindings().crypto_kx_server_session_keys(
        server_pk, server_sk, client_pk
    )
    return bytes(rx), bytes(tx)


def derive_key(purpose: bytes, material: bytes) -> bytes:
    """``BLAKE2b-256(key = LABEL ‖ purpose, message = material)``."""
    return hashlib.blake2b(
        material, key=LABEL + purpose, digest_size=KEY_BYTES
    ).digest()


def seal(key: bytes, nonce: bytes, aad: bytes, plaintext: bytes) -> bytes:
    """ChaCha20-Poly1305 (RFC 8439); the 16-byte tag follows the text."""
    return bytes(
        _nacl_bindings().crypto_aead_chacha20poly1305_ietf_encrypt(
            plaintext, aad, nonce, key
        )
    )


def unseal(key: bytes, nonce: bytes, aad: bytes, sealed: bytes) -> bytes:
    """The inverse of :func:`seal`; DecryptError on any failure."""
    from nacl.exceptions import CryptoError

    try:
        return bytes(
            _nacl_bindings().crypto_aead_chacha20poly1305_ietf_decrypt(
                sealed, aad, nonce, key
            )
        )
    except (CryptoError, ValueError, TypeError):
        raise DecryptError("record did not open") from None


# ------------------------------------------------------------------ frames


def pack_frame(ftype: int, req_id: int, payload: bytes = b"") -> bytes:
    """``u8 type ‖ u32 reqId ‖ payload``."""
    if len(payload) > MAX_FRAME_PAYLOAD:
        raise ReachError("frame payload over the cap")
    return struct.pack(">BI", ftype, req_id) + payload


def unpack_frame(data: bytes) -> tuple[int, int, bytes]:
    """The inverse of :func:`pack_frame`; ReachError on a short frame."""
    if len(data) < 5:
        raise ReachError("frame shorter than its header")
    if len(data) - 5 > MAX_FRAME_PAYLOAD:
        raise ReachError("frame payload over the cap")
    ftype, req_id = struct.unpack(">BI", data[:5])
    return ftype, req_id, bytes(data[5:])


def pack_cancel(req_id: int, reason: int) -> bytes:
    return pack_frame(FRAME_CANCEL, req_id, struct.pack(">H", reason))


def pack_reject(req_id: int, code: int, reason: str) -> bytes:
    return pack_frame(
        FRAME_REJECT, req_id, struct.pack(">H", code) + reason.encode("utf-8")
    )


def pack_window(req_id: int, credit: int) -> bytes:
    return pack_frame(FRAME_WINDOW, req_id, struct.pack(">I", credit))


def pack_chunk(chunk: bytes) -> bytes:
    """One length-prefixed chunk of the response stream."""
    return struct.pack(">I", len(chunk)) + chunk


class ChunkReader:
    """Incremental parser of the ``(u32 len ‖ chunk)*`` response stream.

    Feed it bytes as they arrive, in any split; it hands back every
    complete chunk.  ``max_chunk`` bounds what one length prefix may ask
    for, so a hostile stream cannot make the reader buffer without end.
    """

    def __init__(self, max_chunk: int = MAX_FRAME_PAYLOAD) -> None:
        self._buffer = bytearray()
        self._max_chunk = max_chunk

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer.extend(data)
        out: list[bytes] = []
        while len(self._buffer) >= 4:
            (length,) = struct.unpack(">I", self._buffer[:4])
            if length > self._max_chunk:
                raise ReachError("chunk over the cap")
            if len(self._buffer) < 4 + length:
                break
            out.append(bytes(self._buffer[4 : 4 + length]))
            del self._buffer[: 4 + length]
        return out

    @property
    def pending(self) -> int:
        """Bytes buffered toward an incomplete chunk."""
        return len(self._buffer)


# --------------------------------------------------------------- layouts


def _padding(length: int) -> int:
    return (-length) % PAD_BLOCK


def encode_request_plaintext(head: dict[str, Any], body: bytes) -> bytes:
    """``u32 headLen ‖ head ‖ u32 padLen ‖ pad ‖ body``."""
    return encode_request_plaintext_raw(
        json.dumps(head, separators=(",", ":")).encode("utf-8"), body
    )


def encode_request_plaintext_raw(head: bytes, body: bytes) -> bytes:
    pad = _padding(len(head))
    return (
        struct.pack(">I", len(head))
        + head
        + struct.pack(">I", pad)
        + bytes(pad)
        + body
    )


def decode_request_plaintext(plaintext: bytes) -> tuple[dict[str, Any], bytes]:
    """The inverse of :func:`encode_request_plaintext`."""
    head_raw, body = split_request_plaintext(plaintext)
    try:
        head = json.loads(head_raw.decode("utf-8"))
    except ValueError:
        raise ReachError("request head is not JSON") from None
    if not isinstance(head, dict) or head.get("v") != 1:
        raise ReachError("request head is not a v1 head")
    return head, body


def split_request_plaintext(plaintext: bytes) -> tuple[bytes, bytes]:
    if len(plaintext) < 8:
        raise ReachError("request plaintext too short")
    (head_len,) = struct.unpack(">I", plaintext[:4])
    if 4 + head_len + 4 > len(plaintext):
        raise ReachError("request head length out of range")
    head = plaintext[4 : 4 + head_len]
    (pad_len,) = struct.unpack(">I", plaintext[4 + head_len : 8 + head_len])
    start = 8 + head_len + pad_len
    if start > len(plaintext):
        raise ReachError("request padding out of range")
    return bytes(head), bytes(plaintext[start:])


def encode_head_record(status: int, headers: list[tuple[str, str]]) -> bytes:
    """The data of a HEAD record: ``u32 jsonLen ‖ json ‖ pad``."""
    doc = {"s": status, "h": [[k.lower(), v] for k, v in headers]}
    return encode_head_record_raw(
        json.dumps(doc, separators=(",", ":")).encode("utf-8")
    )


def encode_head_record_raw(payload: bytes) -> bytes:
    pad = _padding(4 + len(payload))
    return struct.pack(">I", len(payload)) + payload + bytes(pad)


def decode_head_record(data: bytes) -> tuple[int, list[tuple[str, str]]]:
    """The inverse of :func:`encode_head_record`."""
    if len(data) < 4:
        raise ReachError("HEAD record too short")
    (length,) = struct.unpack(">I", data[:4])
    if 4 + length > len(data):
        raise ReachError("HEAD record length out of range")
    try:
        doc = json.loads(data[4 : 4 + length].decode("utf-8"))
        status = int(doc["s"])
        headers = [(str(k), str(v)) for k, v in doc.get("h", [])]
    except (ValueError, KeyError, TypeError):
        raise ReachError("HEAD record is not a head") from None
    return status, headers


def _u64(value: int) -> bytes:
    return struct.pack(">Q", value)


def _u32(value: int) -> bytes:
    return struct.pack(">I", value)


def request_nonce(ctr: int) -> bytes:
    return _u64(ctr) + b"\x00\x00\x00\x01"


def request_aad(sid: bytes, ctr: int) -> bytes:
    return LABEL + b"req" + sid + _u64(ctr)


def record_nonce(ctr: int, idx: int) -> bytes:
    return _u64(ctr) + _u32(idx)


def record_aad(sid: bytes, ctr: int, idx: int) -> bytes:
    return LABEL + b"res" + sid + _u64(ctr) + _u32(idx)


def hello_aad(tid_bytes: bytes, e_pk: bytes) -> bytes:
    return LABEL + b"hello" + tid_bytes + e_pk


def hello_reply_aad(tid_bytes: bytes, e_pk: bytes, f_pk: bytes) -> bytes:
    return LABEL + b"hello-reply" + tid_bytes + e_pk + f_pk


# ---------------------------------------------------------- app session


class AppSession:
    """The app's side of a session: the reference implementation the
    daemon tests and the shared vectors are written against."""

    def __init__(self, tid_bytes: bytes, daemon_key: bytes) -> None:
        if len(daemon_key) != 32:
            raise ReachError("the daemon channel key is 32 bytes")
        self.tid_bytes = tid_bytes
        self.daemon_key = daemon_key
        self.e_pk = b""
        self.e_sk = b""
        self.rx1 = b""
        self.tx1 = b""
        self.rx2 = b""
        self.tx2 = b""
        self.k_c2d = b""
        self.k_d2c = b""
        self.sid = b""
        self.ttl = 0
        self._ctr = 0

    def begin(
        self,
        *,
        ephemeral_seed: Optional[bytes] = None,
        nonce: Optional[bytes] = None,
    ) -> bytes:
        """Start a session; returns the HELLO request body."""
        self.e_pk, self.e_sk = kx_keypair(seed=ephemeral_seed)
        self.rx1, self.tx1 = client_session_keys(
            self.e_pk, self.e_sk, self.daemon_key
        )
        n1 = nonce if nonce is not None else secrets.token_bytes(NONCE_BYTES)
        sealed = seal(
            derive_key(b"hello", self.tx1),
            n1,
            hello_aad(self.tid_bytes, self.e_pk),
            b'{"v":1}',
        )
        return bytes([BODY_HELLO]) + self.e_pk + n1 + sealed

    def finish(self, chunk: bytes) -> None:
        """Consume the HELLO-REPLY chunk and derive the session keys."""
        if len(chunk) < 1 + 32 + NONCE_BYTES + TAG_BYTES:
            raise ReachError("HELLO-REPLY too short")
        if chunk[0] != CHUNK_HELLO_REPLY:
            raise ReachError("not a HELLO-REPLY chunk")
        f_pk = chunk[1:33]
        n2 = chunk[33 : 33 + NONCE_BYTES]
        plaintext = unseal(
            derive_key(b"hello-reply", self.rx1),
            n2,
            hello_reply_aad(self.tid_bytes, self.e_pk, f_pk),
            chunk[33 + NONCE_BYTES :],
        )
        try:
            doc = json.loads(plaintext.decode("utf-8"))
            sid = b64_decode(doc["sid"], SESSION_ID_BYTES)
            ttl = int(doc.get("ttl", SESSION_IDLE_SECONDS))
        except (ValueError, KeyError, TypeError):
            raise ReachError("HELLO-REPLY plaintext is malformed") from None
        if not isinstance(doc, dict) or doc.get("v") != 1:
            raise ReachError("HELLO-REPLY is not v1")
        self.rx2, self.tx2 = client_session_keys(self.e_pk, self.e_sk, f_pk)
        self.k_c2d = derive_key(b"c2d", self.tx1 + self.tx2)
        self.k_d2c = derive_key(b"d2c", self.rx1 + self.rx2)
        self.sid = sid
        self.ttl = ttl

    def seal_request(
        self,
        head: dict[str, Any],
        body: bytes = b"",
        *,
        ctr: Optional[int] = None,
    ) -> bytes:
        """A REQ body for one request; ``ctr`` defaults to the next."""
        return self.seal_request_raw(
            json.dumps(head, separators=(",", ":")).encode("utf-8"),
            body,
            ctr=ctr,
        )

    def seal_request_raw(
        self, head: bytes, body: bytes = b"", *, ctr: Optional[int] = None
    ) -> bytes:
        if not self.sid:
            raise ReachError("session has no keys yet")
        if ctr is None:
            self._ctr += 1
            ctr = self._ctr
        else:
            self._ctr = max(self._ctr, ctr)
        sealed = seal(
            self.k_c2d,
            request_nonce(ctr),
            request_aad(self.sid, ctr),
            encode_request_plaintext_raw(head, body),
        )
        return bytes([BODY_REQ]) + self.sid + _u64(ctr) + sealed

    def open_record(self, chunk: bytes) -> tuple[int, int, int, int, bytes]:
        """``(ctr, idx, kind, flags, data)`` of one RES chunk."""
        if len(chunk) < 13 + TAG_BYTES + 2:
            raise ReachError("RES chunk too short")
        if chunk[0] != CHUNK_RES:
            raise ReachError("not a RES chunk")
        (ctr,) = struct.unpack(">Q", chunk[1:9])
        (idx,) = struct.unpack(">I", chunk[9:13])
        plaintext = unseal(
            self.k_d2c,
            record_nonce(ctr, idx),
            record_aad(self.sid, ctr, idx),
            chunk[13:],
        )
        return ctr, idx, plaintext[0], plaintext[1], bytes(plaintext[2:])


# -------------------------------------------------------- daemon session


class ReplayWindow:
    """The highest counter seen plus a bitmap of the window below it."""

    def __init__(self, width: int = REPLAY_WINDOW) -> None:
        self.width = width
        self.highest = 0
        self._seen = 0  # bit i set: counter (highest - i) was seen

    def accept(self, ctr: int) -> None:
        """Mark ``ctr`` as used, or raise ReplayError."""
        if ctr < 1:
            raise ReplayError("counter must be at least 1")
        if ctr > self.highest:
            shift = ctr - self.highest
            self._seen = (self._seen << shift) | 1 if shift < self.width else 1
            self._seen &= (1 << self.width) - 1
            self.highest = ctr
            return
        offset = self.highest - ctr
        if offset >= self.width:
            raise ReplayError("counter fell out of the replay window")
        if self._seen & (1 << offset):
            raise ReplayError("counter already used")
        self._seen |= 1 << offset


class DaemonSession:
    """One app session as the daemon holds it."""

    def __init__(
        self, sid: bytes, k_c2d: bytes, k_d2c: bytes, now: float
    ) -> None:
        self.sid = sid
        self.k_c2d = k_c2d
        self.k_d2c = k_d2c
        self.created = now
        self.last_used = now
        self.replay = ReplayWindow()
        self.inflight = 0
        # the request-rate bucket: tokens left and when they were counted
        self.rate_tokens = float(RATE_BURST)
        self.rate_at = now

    def expired(self, now: float) -> bool:
        return (
            now - self.last_used > SESSION_IDLE_SECONDS
            or now - self.created > SESSION_ABSOLUTE_SECONDS
        )


class SessionTable:
    """An LRU of live sessions with idle and absolute expiry."""

    def __init__(self, capacity: int = SESSION_LRU) -> None:
        self.capacity = capacity
        self._sessions: "collections.OrderedDict[bytes, DaemonSession]" = (
            collections.OrderedDict()
        )

    def add(self, session: DaemonSession) -> None:
        self._sessions[session.sid] = session
        self._sessions.move_to_end(session.sid)
        while len(self._sessions) > self.capacity:
            self._sessions.popitem(last=False)

    def get(self, sid: bytes, now: float) -> Optional[DaemonSession]:
        session = self._sessions.get(sid)
        if session is None:
            return None
        if session.expired(now):
            del self._sessions[sid]
            return None
        session.last_used = now
        self._sessions.move_to_end(sid)
        return session

    def sweep(self, now: float) -> None:
        for sid in [s for s, v in self._sessions.items() if v.expired(now)]:
            del self._sessions[sid]

    def __len__(self) -> int:
        return len(self._sessions)


def answer_hello(
    identity: Identity,
    hello: bytes,
    now: float,
    *,
    ephemeral_seed: Optional[bytes] = None,
    nonce: Optional[bytes] = None,
    sid: Optional[bytes] = None,
    ttl: int = SESSION_IDLE_SECONDS,
) -> tuple[DaemonSession, bytes]:
    """Open a HELLO body and produce ``(session, HELLO-REPLY chunk)``.

    DecryptError when the body was not sealed to this identity's key.
    """
    if len(hello) < 1 + 32 + NONCE_BYTES + TAG_BYTES or hello[0] != BODY_HELLO:
        raise ReachError("not a HELLO body")
    e_pk = hello[1:33]
    n1 = hello[33 : 33 + NONCE_BYTES]
    rx1, tx1 = server_session_keys(
        identity.dh_public, identity.dh_secret, e_pk
    )
    plaintext = unseal(
        derive_key(b"hello", rx1),
        n1,
        hello_aad(identity.tunnel_id_bytes, e_pk),
        hello[33 + NONCE_BYTES :],
    )
    try:
        doc = json.loads(plaintext.decode("utf-8"))
    except ValueError:
        raise ReachError("HELLO plaintext is not JSON") from None
    if not isinstance(doc, dict) or doc.get("v") != 1:
        raise ReachError("HELLO is not v1")
    f_pk, f_sk = kx_keypair(seed=ephemeral_seed)
    rx2, tx2 = server_session_keys(f_pk, f_sk, e_pk)
    session_id = (
        sid if sid is not None else secrets.token_bytes(SESSION_ID_BYTES)
    )
    n2 = nonce if nonce is not None else secrets.token_bytes(NONCE_BYTES)
    reply = json.dumps(
        {"v": 1, "sid": b64_encode(session_id), "ttl": ttl},
        separators=(",", ":"),
    ).encode("utf-8")
    sealed = seal(
        derive_key(b"hello-reply", tx1),
        n2,
        hello_reply_aad(identity.tunnel_id_bytes, e_pk, f_pk),
        reply,
    )
    session = DaemonSession(
        session_id,
        derive_key(b"c2d", rx1 + rx2),
        derive_key(b"d2c", tx1 + tx2),
        now,
    )
    return session, bytes([CHUNK_HELLO_REPLY]) + f_pk + n2 + sealed


def request_session_id(body: bytes) -> bytes:
    """The ``sid`` a REQ body names, before any key is touched."""
    if len(body) < 1 + SESSION_ID_BYTES + 8 + TAG_BYTES:
        raise ReachError("REQ body too short")
    if body[0] != BODY_REQ:
        raise ReachError("not a REQ body")
    return bytes(body[1 : 1 + SESSION_ID_BYTES])


def open_request(
    session: DaemonSession, body: bytes
) -> tuple[int, dict[str, Any], bytes]:
    """``(ctr, head, body)`` of a REQ body under ``session``.

    ReplayError before any decryption for a used counter; DecryptError
    for a body that does not open; ReachError for a malformed head.
    """
    if request_session_id(body) != session.sid:
        raise ReachError("REQ names another session")
    (ctr,) = struct.unpack(">Q", body[17:25])
    session.replay.accept(ctr)
    plaintext = unseal(
        session.k_c2d,
        request_nonce(ctr),
        request_aad(session.sid, ctr),
        body[25:],
    )
    head, inner = decode_request_plaintext(plaintext)
    return ctr, head, inner


def seal_record(
    session: DaemonSession,
    ctr: int,
    idx: int,
    kind: int,
    data: bytes = b"",
    flags: int = 0,
) -> bytes:
    """One RES chunk (without its length prefix)."""
    sealed = seal(
        session.k_d2c,
        record_nonce(ctr, idx),
        record_aad(session.sid, ctr, idx),
        bytes([kind, flags]) + data,
    )
    return bytes([CHUNK_RES]) + _u64(ctr) + _u32(idx) + sealed


def error_record_data(message: str, status: Optional[int] = None) -> bytes:
    doc: dict[str, Any] = {"error": message}
    if status is not None:
        doc["status"] = status
    return json.dumps(doc, separators=(",", ":")).encode("utf-8")


NOSESSION_CHUNK = bytes([CHUNK_NOSESSION])


# ----------------------------------------------------------------- runtime

#: The inner request deadline for everything but a log tail.
REQUEST_TIMEOUT_SECONDS = 20.0
#: How long the daemon waits for the relay's challenge and its welcome.
HANDSHAKE_TIMEOUT_SECONDS = 15.0
#: The most admit ids a ``hello`` or ``admit`` message carries.
ADMIT_LIMIT = 64
#: The relay's initial window until its ``welcome`` says otherwise.
DEFAULT_INITIAL_WINDOW = 262_144
#: Headers the adapter forwards in neither direction: connection
#: management belongs to each hop, and the sealed stream carries its own
#: framing.  ``proxy-*`` names are dropped by prefix.
HOP_BY_HOP = frozenset(
    {
        "connection",
        "transfer-encoding",
        "keep-alive",
        "upgrade",
        "te",
        "trailer",
        "content-length",
    }
)
_SUPERSEDED_WINDOW_SECONDS = 600.0
_SUPERSEDED_STRIKES = 3
_DEFAULT_PORTS = frozenset({80, 443})


def _hop_by_hop(name: str) -> bool:
    lowered = name.lower()
    return lowered in HOP_BY_HOP or lowered.startswith("proxy-")


def relay_origin(url: str) -> str:
    """``url`` reduced to ``scheme://host[:port]``, userinfo dropped."""
    parsed = urlparse(url)
    return "{}://{}".format(parsed.scheme, parsed.netloc.rsplit("@", 1)[-1])


def _authority(host: Optional[str], port: Optional[int]) -> str:
    """``host`` or ``host:port``, lowercase, an explicit 80 or 443
    treated as absent so both spellings of a default port compare equal."""
    name = (host or "").lower()
    if ":" in name:
        name = "[" + name + "]"
    if port is None or port in _DEFAULT_PORTS:
        return name
    return "{}:{}".format(name, port)


def dialed_authority(origin: str) -> str:
    """The authority a relay origin dials, in :func:`_authority` form."""
    parsed = urlparse(origin)
    try:
        port = parsed.port
    except ValueError:
        port = None
    return _authority(parsed.hostname, port)


def claimed_authority(claimed: str) -> str:
    """A challenge's ``relay`` field (``host`` or ``host:port``) in
    :func:`_authority` form; empty when it does not parse."""
    parsed = urlparse("//" + claimed.strip())
    try:
        port = parsed.port
    except ValueError:
        return ""
    return _authority(parsed.hostname, port)


def _control(kind: str, **fields: Any) -> str:
    """One control message: ``{"v": 1, "type": kind, ...fields}``."""
    doc: dict[str, Any] = {"v": 1, "type": kind}
    doc.update(fields)
    return json.dumps(doc, separators=(",", ":"))


def _parse_control(text: Any) -> Optional[dict[str, Any]]:
    """A control message's document, or None when it is not one."""
    try:
        doc = json.loads(text)
    except (TypeError, ValueError):
        return None
    if (
        not isinstance(doc, dict)
        or doc.get("v") != 1
        or not isinstance(doc.get("type"), str)
    ):
        return None
    return doc


def _rate_take(session: DaemonSession, now: float) -> bool:
    """Spend one request from the session's rate bucket, if it has one."""
    elapsed = max(0.0, now - session.rate_at)
    session.rate_tokens = min(
        float(RATE_BURST),
        session.rate_tokens + elapsed * RATE_REFILL_PER_SECOND,
    )
    session.rate_at = now
    if session.rate_tokens < 1.0:
        return False
    session.rate_tokens -= 1.0
    return True


class Backoff:
    """The reconnect schedule, in seconds; tests shorten every field.

    ``delay(attempt)`` doubles from ``base`` up to ``cap`` with a quarter
    of jitter either way.  ``park`` is the wait after a relay without
    Reach (a 404 on the upgrade) or a zone-policy refusal,
    ``superseded_wait`` the wait after three 4409 closes in ten minutes,
    and ``healthy_after`` how long a connection must last for the next
    failure to start again from ``base``.
    """

    def __init__(
        self,
        *,
        base: float = 1.0,
        cap: float = 60.0,
        healthy_after: float = 60.0,
        park: float = 3600.0,
        superseded_wait: float = 300.0,
        jitter: bool = True,
    ) -> None:
        self.base = base
        self.cap = cap
        self.healthy_after = healthy_after
        self.park = park
        self.superseded_wait = superseded_wait
        self.jitter = jitter

    def delay(self, attempt: int) -> float:
        raw = min(self.cap, self.base * (2.0 ** min(attempt, 30)))
        if not self.jitter:
            return raw
        return raw * (0.75 + secrets.randbelow(501) / 1000.0)


class _Credit:
    """Flow-control credit for one relayed request (the protocol's
    "Limits and flow control"): a writer waits until the relay has
    granted room for its whole frame."""

    def __init__(self, initial: int) -> None:
        self.available = initial
        self._grown = asyncio.Event()

    def add(self, amount: int) -> None:
        self.available += amount
        self._grown.set()

    async def take(self, amount: int) -> None:
        while self.available < amount:
            self._grown.clear()
            await self._grown.wait()
        self.available -= amount


class _Request:
    """One relayed request in flight, with its task and its credit."""

    def __init__(self, req_id: int, initial_window: int) -> None:
        self.req_id = req_id
        self.credit = _Credit(initial_window)
        self.task: Optional["asyncio.Task[None]"] = None
        self.session: Optional[DaemonSession] = None
        # holds one of the session's in-flight slots
        self.counted = False
        self.ctr = 0
        self.idx = 0
        # a DATA frame went out, so a REJECT is no longer legal
        self.sent = False


class ReachService:
    """The daemon's Reach runtime: one relay socket, served through the
    web app.

    ``start`` reads (or generates) the identity, binds a private loopback
    site onto the running web ``runner``, and starts the supervisor that
    dials the relay and reconnects with backoff.  Every relayed request
    is opened, proxied through that loopback site (so the web app's
    authentication, scopes, error envelope, and access log apply
    unchanged), and its response sealed back one record per write.
    ``bearers`` returns the bearer secrets the relay may admit;
    :meth:`refresh_admit` re-derives the admit set from it.

    ``backoff`` and ``ws_connect`` are test seams: a shortened schedule
    and a replacement for ``ClientSession.ws_connect``.
    """

    def __init__(
        self,
        *,
        identity_path: str,
        relay: str,
        heartbeat: float,
        node: str,
        agent: str,
        bearers: Callable[[], list[str]],
        runner: Any,
        backoff: Optional[Backoff] = None,
        ws_connect: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.identity_path = identity_path
        self.relay = relay.rstrip("/")
        self.relay_origin = relay_origin(self.relay)
        self.relay_host = urlparse(self.relay_origin).netloc
        self.heartbeat = heartbeat
        self.node = node
        self.agent = agent
        self.runner = runner
        self.identity: Optional[Identity] = None
        self._bearers = bearers
        self._backoff = backoff or Backoff()
        self._ws_connect = ws_connect
        self._dialed = dialed_authority(self.relay_origin)
        self._site: Any = None
        self._sock_dir: Optional[str] = None
        self._sock_path: Optional[str] = None
        self._tcp_base: Optional[str] = None
        self._task: Optional["asyncio.Task[None]"] = None
        self._ws: Any = None
        self._connected = False
        self._stopping = False
        self._sessions = SessionTable()
        self._requests: dict[int, _Request] = {}
        self._initial_window = DEFAULT_INITIAL_WINDOW
        self._max_chunk = MAX_CHUNK
        self.limits: dict[str, Any] = {}
        self._admit: list[str] = []
        self._superseded: "collections.deque[float]" = collections.deque()
        self._parked_for: Optional[str] = None

    # ------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Load the identity, bind the loopback site, start dialing."""
        self.identity = ensure_identity(self.identity_path)
        await self._bind_loopback()
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        """Say goodbye, drop every request, unbind; never raises."""
        self._stopping = True
        ws = self._ws
        if ws is not None and self._connected:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(ws.send_str(_control("bye")), 2.0)
        tasks = [r.task for r in self._requests.values() if r.task]
        for task in tasks:
            task.cancel()
        if self._task is not None:
            self._task.cancel()
            tasks.append(self._task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        await self._unbind_loopback()

    @property
    def state(self) -> str:
        return "connected" if self._connected else "connecting"

    def status(self) -> dict[str, Any]:
        """The ``/whoami`` ``reach`` object for an authenticated caller."""
        payload: dict[str, Any] = {
            "state": self.state,
            "relay": self.relay_origin,
        }
        identity = self.identity
        if identity is not None:
            payload.update(
                id=identity.tunnel_id,
                key=b64_encode(identity.dh_public),
                salt=b64_encode(identity.salt),
                node=self.node,
                fingerprint=identity.fingerprint,
            )
        return payload

    async def _bind_loopback(self) -> None:
        from aiohttp import web

        if platform.supports_unix_sockets():
            self._sock_dir = tempfile.mkdtemp(prefix="cs-reach-")
            self._sock_path = os.path.join(self._sock_dir, "reach.sock")
            self._site = web.UnixSite(self.runner, self._sock_path)
            await self._site.start()
            return
        # Windows: asyncio's Proactor loop has no unix servers, so the
        # adapter dials an ephemeral loopback TCP port instead.  It is a
        # listener like every other one: the same bearer auth gates it.
        before = len(self.runner.addresses)
        self._site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self._site.start()
        port = self.runner.addresses[before][1]
        self._tcp_base = "http://127.0.0.1:{}".format(port)

    async def _unbind_loopback(self) -> None:
        site, self._site = self._site, None
        if site is not None:
            with contextlib.suppress(Exception):
                if site in self.runner.sites:
                    await site.stop()
        if self._sock_dir is not None:
            shutil.rmtree(self._sock_dir, ignore_errors=True)
            self._sock_dir = self._sock_path = None

    # ------------------------------------------------------ supervision

    async def _supervise(self) -> None:
        """Dial, serve, reconnect: the loop that outlives every socket."""
        attempt = 0
        while not self._stopping:
            started = time.monotonic()
            wait: Optional[float] = None
            try:
                wait = await self._connect_once()
            except Exception:
                logger.exception(
                    "reach: the relay connection failed unexpectedly; "
                    "reconnecting"
                )
            if self._stopping:
                break
            if time.monotonic() - started >= self._backoff.healthy_after:
                attempt = 0
            if wait is None:
                wait = self._backoff.delay(attempt)
                attempt += 1
            await asyncio.sleep(wait)

    async def _connect_once(self) -> Optional[float]:
        """One socket's whole life.  Returns the special wait to apply
        before the next dial, or None for the ordinary backoff."""
        import aiohttp

        session = aiohttp.ClientSession()
        try:
            try:
                ws = await self._open_socket(session)
            except aiohttp.WSServerHandshakeError as exc:
                return self._refused(exc.status, exc.headers)
            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "reach: cannot connect to relay %s: %s",
                    self.relay_host,
                    self._redact(str(exc)),
                )
                return None
            self._ws = ws
            try:
                return await self._serve(ws)
            finally:
                self._ws = None
                self._connected = False
                self._abort_requests()
                with contextlib.suppress(Exception):
                    await ws.close()
        finally:
            await session.close()

    async def _open_socket(self, session: Any) -> Any:
        assert self.identity is not None
        parsed = urlparse(self.relay)
        headers = {"User-Agent": self.agent}
        if parsed.username is not None:
            # userinfo in the configured URL becomes Basic authentication
            # on the upgrade; the dialed URL itself carries none
            userinfo = "{}:{}".format(
                unquote(parsed.username), unquote(parsed.password or "")
            )
            headers["Authorization"] = "Basic " + b64_encode(
                userinfo.encode("utf-8")
            )
        url = "{}/v1/tunnel/{}".format(
            self.relay_origin, self.identity.tunnel_id
        )
        connect = self._ws_connect or session.ws_connect
        return await connect(
            url,
            protocols=(SUBPROTOCOL,),
            heartbeat=self.heartbeat,
            headers=headers,
        )

    def _redact(self, text: str) -> str:
        from cronstable.push import _redact_userinfo_in

        return _redact_userinfo_in(text, self.relay)

    def _refused(self, status: Optional[int], headers: Any) -> Optional[float]:
        """The wait after a refused upgrade: a park for the two refusals
        that will not clear on their own, else the ordinary backoff."""
        mitigated = headers is not None and "cf-mitigated" in headers
        if status == 404:
            reason = "has no Reach (404 on the tunnel upgrade)"
        elif status in (403, 503) and mitigated:
            reason = "refused the upgrade by zone policy (cf-mitigated)"
        else:
            self._parked_for = None
            logger.warning(
                "reach: relay %s refused the upgrade with HTTP %s; "
                "reconnecting",
                self.relay_host,
                status,
            )
            return None
        if self._parked_for != reason:
            # one line per outage, however many hourly retries it spans
            self._parked_for = reason
            logger.warning(
                "reach: relay %s %s; retrying every %.0f seconds",
                self.relay_host,
                reason,
                self._backoff.park,
            )
        return self._backoff.park

    async def _serve(self, ws: Any) -> Optional[float]:
        """Answer the challenge, then serve frames until the socket ends."""
        from aiohttp import WSMsgType

        assert self.identity is not None
        challenge = await self._expect_control(ws, "challenge")
        if challenge is None:
            return self._closed(ws)
        try:
            relay = str(challenge["relay"])
            nonce = b64_decode(str(challenge["nonce"]), 32)
        except (KeyError, TypeError, ReachError):
            await self._protocol_error(ws, "malformed challenge")
            return None
        if claimed_authority(relay) != self._dialed:
            logger.warning(
                "reach: relay %s presented itself as %r, not the host this "
                "daemon dialed; refusing its challenge",
                self.relay_host,
                relay,
            )
            await self._protocol_error(ws, "relay host mismatch")
            return None
        self._admit = self._compute_admit()
        await ws.send_str(
            _control(
                "hello",
                sig=b64_encode(
                    self.identity.sign(signature_message(relay, nonce))
                ),
                agent=self.agent,
                node=self.node,
                admit=self._admit,
                limits={"maxBody": MAX_BODY},
            )
        )
        welcome = await self._expect_control(ws, "welcome")
        if welcome is None:
            return self._closed(ws)
        self._apply_limits(welcome.get("limits"))
        self._connected = True
        self._parked_for = None
        logger.info(
            "reach: connected to relay %s as %s (node %s)",
            self.relay_host,
            self.identity.fingerprint,
            self.node,
        )
        while True:
            msg = await ws.receive()
            if msg.type == WSMsgType.BINARY:
                self._on_frame(ws, msg.data)
            elif msg.type == WSMsgType.TEXT:
                if not await self._on_text(ws, msg.data):
                    return None
            else:
                break
        return self._closed(ws)

    async def _expect_control(
        self, ws: Any, expected: str
    ) -> Optional[dict[str, Any]]:
        """The next control message, which must be of type ``expected``.

        None when the socket ended first (the caller reads the close
        code) or after closing it with 4400 for anything else.
        """
        from aiohttp import WSMsgType

        try:
            msg = await ws.receive(timeout=HANDSHAKE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            await self._protocol_error(ws, "no {} in time".format(expected))
            return None
        if msg.type != WSMsgType.TEXT:
            return None
        doc = _parse_control(msg.data)
        if doc is None or doc.get("type") != expected:
            await self._protocol_error(ws, "expected {}".format(expected))
            return None
        return doc

    async def _on_text(self, ws: Any, data: Any) -> bool:
        """A control message while connected: a repeated ``welcome``
        re-applies limits; anything else is a protocol error."""
        doc = _parse_control(data)
        if doc is not None and doc.get("type") == "welcome":
            self._apply_limits(doc.get("limits"))
            return True
        await self._protocol_error(ws, "unexpected control message")
        return False

    async def _protocol_error(self, ws: Any, what: str) -> None:
        logger.warning(
            "reach: protocol error on the relay %s socket (%s); closing",
            self.relay_host,
            what,
        )
        with contextlib.suppress(Exception):
            await ws.close(
                code=CLOSE_PROTOCOL_ERROR, message=what.encode("utf-8")
            )

    def _apply_limits(self, limits: Any) -> None:
        if not isinstance(limits, dict):
            return
        self.limits = dict(limits)
        window = limits.get("initialWindow")
        if isinstance(window, int) and window > 0:
            self._initial_window = window
        chunk = limits.get("maxChunk")
        if isinstance(chunk, int) and chunk > 0:
            self._max_chunk = min(MAX_CHUNK, chunk)

    def _closed(self, ws: Any) -> Optional[float]:
        """Log the close and return the damping wait, if one applies."""
        code = ws.close_code
        logger.info(
            "reach: disconnected from relay %s (close code %s)",
            self.relay_host,
            code,
        )
        if code != CLOSE_SUPERSEDED:
            return None
        now = time.monotonic()
        self._superseded.append(now)
        while (
            self._superseded
            and now - self._superseded[0] > _SUPERSEDED_WINDOW_SECONDS
        ):
            self._superseded.popleft()
        if len(self._superseded) < _SUPERSEDED_STRIKES:
            return None
        self._superseded.clear()
        logger.warning(
            "reach: superseded by another daemon on this tunnel id %d "
            "times in ten minutes (is the identity file shared?); waiting "
            "%.0f seconds before reconnecting",
            _SUPERSEDED_STRIKES,
            self._backoff.superseded_wait,
        )
        return self._backoff.superseded_wait

    # -------------------------------------------------------- admission

    def _compute_admit(self) -> list[str]:
        assert self.identity is not None
        ids = admit_ids(
            list(self._bearers()),
            self.identity.salt,
            self.identity.tunnel_id_bytes,
        )
        if len(ids) > ADMIT_LIMIT:
            logger.warning(
                "reach: %d bearer tokens exceed the relay's admit limit "
                "of %d; only the first %d (sorted by admit id) are admitted",
                len(ids),
                ADMIT_LIMIT,
                ADMIT_LIMIT,
            )
            ids = ids[:ADMIT_LIMIT]
        return ids

    async def refresh_admit(self) -> None:
        """Re-derive the admit set and tell the relay when it changed."""
        if self.identity is None:
            return
        admit = self._compute_admit()
        if admit == self._admit:
            return
        self._admit = admit
        ws = self._ws
        if ws is None or not self._connected:
            return
        try:
            await ws.send_str(_control("admit", admit=admit))
        except Exception as exc:
            logger.warning("reach: could not send the admit update: %s", exc)
        else:
            logger.info(
                "reach: admit set updated (%d credential%s)",
                len(admit),
                "" if len(admit) == 1 else "s",
            )

    # --------------------------------------------------------- requests

    def _on_frame(self, ws: Any, data: bytes) -> None:
        try:
            ftype, req_id, payload = unpack_frame(data)
        except ReachError as exc:
            logger.warning(
                "reach: dropping a malformed frame from relay %s: %s",
                self.relay_host,
                exc,
            )
            return
        if ftype == FRAME_OPEN:
            if req_id in self._requests:
                return
            opened = _Request(req_id, self._initial_window)
            self._requests[req_id] = opened
            opened.task = asyncio.create_task(
                self._run_request(ws, opened, payload)
            )
            return
        request = self._requests.get(req_id)
        if request is None:
            return
        if ftype == FRAME_CANCEL:
            if request.task is not None:
                request.task.cancel()
        elif ftype == FRAME_WINDOW and len(payload) >= 4:
            (credit,) = struct.unpack(">I", payload[:4])
            request.credit.add(credit)

    def _abort_requests(self) -> None:
        for request in list(self._requests.values()):
            if request.task is not None:
                request.task.cancel()
        self._requests.clear()

    async def _run_request(
        self, ws: Any, request: _Request, body: bytes
    ) -> None:
        try:
            await self._dispatch(ws, request, body)
        except asyncio.CancelledError:
            pass  # CANCEL from the relay, a dropped socket, or stop()
        except Exception:
            logger.exception("reach: request %d failed", request.req_id)
            await self._fail(ws, request)
        finally:
            if self._requests.get(request.req_id) is request:
                del self._requests[request.req_id]
            if request.counted and request.session is not None:
                request.session.inflight -= 1

    async def _fail(self, ws: Any, request: _Request) -> None:
        """REJECT 5 when nothing went out yet, else an ERROR record; the
        detail stays in the log, never on the wire."""
        if ws.closed:
            return
        with contextlib.suppress(Exception):
            if not request.sent:
                await self._reject(
                    ws, request, REJECT_INTERNAL, "reach adapter error"
                )
            elif request.session is not None:
                await self._send_record(
                    ws,
                    request,
                    KIND_ERROR,
                    error_record_data("reach adapter error", 502),
                )
                await self._send_end(ws, request)

    async def _dispatch(self, ws: Any, request: _Request, body: bytes) -> None:
        now = time.monotonic()
        kind = body[0] if body else None
        if kind == BODY_HELLO:
            await self._dispatch_hello(ws, request, body, now)
        elif kind == BODY_REQ:
            await self._dispatch_request(ws, request, body, now)
        else:
            await self._reject(
                ws, request, REJECT_INTERNAL, "unknown body type"
            )

    async def _dispatch_hello(
        self, ws: Any, request: _Request, body: bytes, now: float
    ) -> None:
        assert self.identity is not None
        try:
            session, reply = answer_hello(self.identity, body, now)
        except DecryptError:
            await self._reject(
                ws, request, REJECT_CANNOT_DECRYPT, "cannot decrypt"
            )
            return
        except ReachError as exc:
            await self._reject(ws, request, REJECT_INTERNAL, str(exc))
            return
        self._sessions.sweep(now)
        self._sessions.add(session)
        await self._send_data(ws, request, pack_chunk(reply))
        await self._send_end(ws, request)

    async def _dispatch_request(
        self, ws: Any, request: _Request, body: bytes, now: float
    ) -> None:
        try:
            sid = request_session_id(body)
        except ReachError as exc:
            await self._reject(ws, request, REJECT_INTERNAL, str(exc))
            return
        session = self._sessions.get(sid, now)
        if session is None:
            await self._send_data(ws, request, pack_chunk(NOSESSION_CHUNK))
            await self._send_end(ws, request)
            return
        try:
            ctr, head, inner = open_request(session, body)
        except ReplayError:
            await self._reject(ws, request, REJECT_REPLAY, "replay")
            return
        except DecryptError:
            await self._reject(
                ws, request, REJECT_CANNOT_DECRYPT, "cannot decrypt"
            )
            return
        except ReachError as exc:
            await self._reject(ws, request, REJECT_INTERNAL, str(exc))
            return
        if len(inner) > MAX_BODY:
            await self._reject(
                ws,
                request,
                REJECT_TOO_LARGE,
                "body over {} bytes".format(MAX_BODY),
            )
            return
        request.session = session
        request.ctr = ctr
        if session.inflight >= SESSION_INFLIGHT_CAP or not _rate_take(
            session, now
        ):
            await self._send_record(
                ws,
                request,
                KIND_ERROR,
                error_record_data("session busy", 429),
            )
            await self._send_end(ws, request)
            return
        session.inflight += 1
        request.counted = True
        await self._proxy(ws, request, head, inner)

    async def _proxy(
        self, ws: Any, request: _Request, head: dict[str, Any], body: bytes
    ) -> None:
        """Serve one opened request through the loopback site and seal
        the response back: HEAD as soon as the status line arrives, one
        BODY record per read (split to the relay's chunk cap), END on
        EOF, or an ERROR record when the adapter itself fails."""
        import aiohttp

        method = str(head.get("m") or "GET").upper()
        target = str(head.get("u") or "/")
        if not target.startswith("/"):
            target = "/" + target
        headers: list[tuple[str, str]] = []
        for pair in head.get("h") or ():
            try:
                name, value = str(pair[0]), str(pair[1])
            except (TypeError, IndexError, KeyError):
                continue
            if _hop_by_hop(name) or name.lower() == "host":
                continue
            headers.append((name, value))
        authority = head.get("a")
        if isinstance(authority, str) and authority:
            # the origin gate then sees the host a direct caller dials
            headers.append(("Host", authority))
        streaming = target.split("?", 1)[0].endswith("/logs")
        timeout = aiohttp.ClientTimeout(
            total=None if streaming else REQUEST_TIMEOUT_SECONDS
        )
        if self._sock_path is not None:
            connector: Any = aiohttp.UnixConnector(path=self._sock_path)
            base = "http://localhost"
        else:
            connector = aiohttp.TCPConnector()
            base = self._tcp_base or "http://127.0.0.1"
        try:
            async with aiohttp.ClientSession(
                connector=connector,
                auto_decompress=False,
                skip_auto_headers=("Accept-Encoding", "User-Agent"),
                timeout=timeout,
            ) as session:
                async with session.request(
                    method,
                    base + target,
                    headers=headers,
                    data=body or None,
                    allow_redirects=False,
                ) as resp:
                    await self._send_record(
                        ws,
                        request,
                        KIND_HEAD,
                        encode_head_record(
                            resp.status,
                            [
                                (name, value)
                                for name, value in resp.headers.items()
                                if not _hop_by_hop(name)
                            ],
                        ),
                    )
                    async for piece in resp.content.iter_any():
                        for start in range(0, len(piece), self._max_chunk):
                            await self._send_record(
                                ws,
                                request,
                                KIND_BODY,
                                piece[start : start + self._max_chunk],
                            )
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            if ws.closed:
                return
            logger.warning(
                "reach: request %d: inner %s %s failed: %s",
                request.req_id,
                method,
                target.split("?", 1)[0],
                exc,
            )
            await self._fail(ws, request)
            return
        await self._send_record(ws, request, KIND_END)
        await self._send_end(ws, request)

    async def _send_data(
        self, ws: Any, request: _Request, payload: bytes
    ) -> None:
        await request.credit.take(len(payload))
        await ws.send_bytes(pack_frame(FRAME_DATA, request.req_id, payload))
        request.sent = True

    async def _send_record(
        self, ws: Any, request: _Request, kind: int, data: bytes = b""
    ) -> None:
        assert request.session is not None
        chunk = seal_record(
            request.session, request.ctr, request.idx, kind, data
        )
        request.idx += 1
        await self._send_data(ws, request, pack_chunk(chunk))

    async def _send_end(self, ws: Any, request: _Request) -> None:
        await ws.send_bytes(pack_frame(FRAME_END, request.req_id))

    async def _reject(
        self, ws: Any, request: _Request, code: int, reason: str
    ) -> None:
        await ws.send_bytes(pack_reject(request.req_id, code, reason))


# --------------------------------------------------------------------- cli


def _reach_settings(config_arg: str) -> dict[str, Any]:
    """The resolved ``web.reach`` settings the config at ``config_arg``
    carries, through the same parse the daemon runs."""
    from cronstable.config import (
        ConfigError,
        parse_config,
        resolve_reach_config,
    )

    settings = resolve_reach_config(parse_config(config_arg))
    if settings is None:
        raise ConfigError(
            "the configuration has no `web.reach` section; `cronstable "
            "reach` manages the remote-access identity it names"
        )
    return settings


def cmd_show(config_arg: str) -> int:
    """Print the identity, generating it first when the file is absent
    (exactly as the daemon does on its first start)."""
    settings = _reach_settings(config_arg)
    identity = ensure_identity(settings["keyFile"])
    print("reach: tunnel id {}".format(identity.tunnel_id))
    print("  fingerprint: {}".format(identity.fingerprint))
    print("  relay: {}".format(relay_origin(settings["relay"])))
    print("  key file: {}".format(settings["keyFile"]))
    print("  created: {}".format(identity.created_at or "unknown"))
    return 0


def cmd_rotate(config_arg: str) -> int:
    """Write a fresh identity over the current one."""
    settings = _reach_settings(config_arg)
    identity = Identity.generate()
    write_identity(settings["keyFile"], identity, replace=True)
    print("reach: rotated the identity at {}".format(settings["keyFile"]))
    print("  fingerprint: {}".format(identity.fingerprint))
    print(
        "  every paired phone must scan the dashboard's pairing QR again: "
        "the tunnel id, the channel key, and the salt all changed. A "
        "running daemon picks the new identity up on its next "
        "housekeeping pass."
    )
    return 0


def dispatch(args: Any) -> int:
    """Route a parsed `cronstable reach <action>` call; return exit code."""
    from cronstable.config import ConfigError

    action = getattr(args, "reach_command", None)
    try:
        if action == "show":
            return cmd_show(args.config)
        if action == "rotate":
            return cmd_rotate(args.config)
    except (ConfigError, ReachError, OSError) as ex:
        # errors go to stderr, the state_admin convention; the identity
        # summary stays on stdout for piping.
        print("cronstable reach error: {}".format(ex), file=sys.stderr)
        return 1
    print(
        "cronstable reach: no action given (see `cronstable reach --help`)",
        file=sys.stderr,
    )
    return 2
