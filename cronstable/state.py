"""Optional durable state backend: one filesystem seam for local disk and
Amazon S3 Files.

cronstable is stateless by default: run history, retry counters, the
next-fire index and the leadership view live in memory and reset on
restart.  When a ``state`` config section is present, a
:class:`StateBackend` adds the opt-in other half: a durable,
restart-surviving record store and a lock to coordinate on.  Absent the
section the backend is never constructed.

A local filesystem and an Amazon S3 Files / EFS mount are the same kind
of backend, a POSIX filesystem with atomic file rename and advisory
``flock``, so there is one implementation,
:class:`FilesystemStateBackend`, and the mount decides its reach: a
local directory gives single-node restart durability; an S3 Files / EFS
mount adds fleet-wide coordination (its advisory NFSv4 lock and atomic
rename are honoured across every host that mounts it).

Two invariants keep that correct on every backing store:

* one immutable object per record: written once to a unique filename
  (temp file + atomic rename), thereafter only read or deleted.  The
  "last fired" cursor is derived (the max over the records), never a
  mutable file, so nothing depends on rewriting an existing object.
* every record is schema-versioned: a record this build cannot
  understand is quarantined on read, never guessed at, so one poison
  object can never brick startup.

The coordination primitive is a TTL lease guarded by an advisory
``flock`` over a dedicated lock file (never the data file, which the
atomic rename swaps out), with a monotonic ``fence`` for takeover
detection.  The locked read-modify-write runs in a worker thread so a
blocking lock never freezes the event loop.

This module is imported only when ``state`` is configured (see
:func:`cronstable.cron.Cron.start_stop_state`) and uses only the
standard library.
"""

import abc
import asyncio
import contextlib
import functools
import hashlib
import logging
import math
import os
import queue
import stat
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import (
    Any,
    Optional,
    TypeVar,
    cast,
)

# Module scope on purpose, though only _decode_fs_token uses it: that runs
# once per FILENAME per directory listing, and at that rate the
# IMPORT_NAME/IMPORT_FROM pair a function-local import compiles to is
# measurable.  It costs nothing at import time either way, since importing
# this module already pulls urllib.parse in through cronstable.config (whose
# own `from urllib.parse import ParseResult, urlparse` is the real provider;
# check before deferring THAT one, because this line sits above the config
# import and would then become the thing putting urllib.parse on the graph).
from urllib.parse import unquote_to_bytes

from cronstable import _json
from cronstable.config import ConfigError, StateConfig
from cronstable.platform import (
    IS_WINDOWS,
    exclusive_file_lock,
    fsync_directory,
)

_T = TypeVar("_T")

logger = logging.getLogger("cronstable.state")

#: Per-record on-disk schema version, written as
#: ``{"schemaVersion": SCHEMA_VERSION, "data": {...}}``; unrecognised
#: versions are quarantined on read.  Bump when the wrapper (not a caller's
#: ``data``) changes shape.  NOT the unrelated job-set-id
#: ``cronstable.fingerprint.SCHEME_VERSION``.
SCHEMA_VERSION = "v1"

#: Legacy alias for SCHEMA_VERSION.  Keep it: it is a module-level name an
#: operator's script or a downstream tool may import.
SCHEME_VERSION = SCHEMA_VERSION

# Converters for `cronstable state migrate-schema`: OLD wrapper
# schemaVersion -> callable turning that version's ``data`` dict into the
# CURRENT shape (return ``None`` to declare the record unconvertible,
# leaving it to be quarantined on read).  Empty while v1 is the only
# shipped scheme.
RECORD_MIGRATIONS: dict[
    str, Callable[[dict[str, Any]], Optional[dict[str, Any]]]
] = {}

# Streams never garbage collected regardless of manifests: the store's
# version stamp and the manifest anchor stream itself.
PROTECTED_STREAMS = frozenset({"meta", "manifests"})

# Age (seconds) past which an orphaned write-temp file is swept by GC; no
# legitimate in-flight write lives near this long, so older is crash debris.
TMP_MAX_AGE = 86400.0

# Subdirectories under a namespace root: per-stream records, leases,
# quarantined corrupt records, write-temp files, mutable job-facing
# documents (one file per key, atomic rename under an advisory flock), and
# content-addressed blobs (immutable, named by SHA-256).  Directories are
# only ever created, never renamed (a directory rename is the one costly
# operation on an S3 Files mount), so this layout is safe there.
#: The open flags of the temp file behind :meth:`FilesystemStateBackend.
#: _atomic_write`.  A descriptor from ``os.open`` is in text mode on
#: Windows unless ``O_BINARY`` is set, and text mode rewrites every LF in a
#: write as CRLF, which corrupts a byte payload (a record's JSON, an
#: artifact blob).  POSIX has no text mode and no such flag, so the mask
#: is 0 there.
_ATOMIC_WRITE_FLAGS = (
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
)

RECORDS_DIR = "records"
LEASES_DIR = "leases"
QUARANTINE_DIR = "quarantine"
TMP_DIR = "tmp"
DOCS_DIR = "docs"

#: The document-namespace prefix jobstate's idempotency keys live under.
#: Mirrors ``cronstable.jobstate.IDEM_NS_PREFIX`` (importing it here would
#: cycle); a drift guard in tests/test_state.py pins the two equal.  The
#: GC's expired-claim sweep matches namespaces by this prefix and must
#: never guess wider: every other ``docs/`` namespace is durable state.
_IDEM_DOC_NS_PREFIX = "idem/"
BLOBS_DIR = "blobs"

#: The character set of a lowercase sha256 hex digest; blob paths admit
#: exactly these, so a crafted digest can never escape the blob directory.
_HEX_DIGITS = frozenset("0123456789abcdef")

# Worker-thread concurrency caps (see :meth:`FilesystemStateBackend._call`).
# BULK bounds the record/document ops so a wedged mount cannot strand an
# unbounded number of stuck daemon threads.  LEASE is a SEPARATE lane for
# the coordination ops: bulk traffic must never hold every slot and starve
# a lease renew below its TTL, which would expire a live holder's lease and
# hand its fenced work to a standby (split-brain / double-fire).
BULK_CALL_SLOTS = 16
LEASE_CALL_SLOTS = 8

# ``prune_keep``-carrying appends between actual prune passes (see
# append_record): the first such append per stream since boot prunes
# immediately, then one in every K.  A stream can briefly exceed its bound
# by up to K-1 records, invisible to readers.
_PRUNE_EVERY_APPENDS = 8

# Bound on the per-stream append countdown map (see _append_prune_due),
# which otherwise grows one entry per distinct stream name forever.  At the
# cap the map is cleared wholesale: a lost countdown costs at most one
# extra prune on a stream's next append, the safe direction.
_PRUNE_COUNTDOWN_MAX_STREAMS = 4096

# Bounds on the per-backend record CONTENT cache (see _read_record).
# Records are immutable with forever-unique names, so path -> bytes never
# goes stale; entry count and total bytes are capped, and an oversized
# body is not cached at all rather than evicting many small ones.
_RECORD_CACHE_MAX_ENTRIES = 2048
_RECORD_CACHE_MAX_BYTES = 4 * 1024 * 1024
_RECORD_CACHE_MAX_ITEM_BYTES = 16 * 1024

# Sentinels a :meth:`StateBackend.mutate_document` transform returns in
# place of a new body: keep the document as-is (KEEP) or delete it
# (DELETE).  Distinct ``object()`` identities so no real JSON value a
# caller might store can be mistaken for one.
DOC_KEEP: Any = object()
DOC_DELETE: Any = object()

# Network/shared filesystem types (as they appear in /proc/mounts) that a lock
# is honoured across hosts on, so the backend may offer fleet-wide
# coordination.  An Amazon S3 Files / EFS mount presents as nfs4.  Anything not
# listed (ext4/xfs/btrfs/apfs/overlay/tmpfs/...) is treated as single-node.
_SHARED_FSTYPES = frozenset(
    {
        "nfs",
        "nfs4",
        "nfs3",
        "efs",
        "cifs",
        "smb3",
        "smbfs",
        "lustre",
        "glusterfs",
        "ceph",
        "cephfs",
        "fuse.sshfs",
    }
)

# Characters kept as-is in a filename; everything else in a stream/namespace/
# lease name is percent-encoded (see _fs_safe), which is injective, so two
# distinct job names can never collide on one on-disk path.  Deliberately
# lowercase-only (uppercase is encoded) so the mapping stays injective on
# case-INsensitive filesystems (NTFS, APFS), and dot-free (``.`` is encoded)
# so no name can produce ``.``/``..`` path components or the trailing-dot
# aliases Windows strips.
_FS_SAFE = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_-")

# Per-byte escape table for _fs_safe_fragment, and the deletion table its
# inverse uses to recognise an already-canonical token.  Both are derived from
# _FS_SAFE so neither can drift from it.
#
# The escape table carries the weight, because encoding sits inside DECODING:
# _decode_fs_token re-encodes each candidate name to prove it round-trips, and
# _list_document_keys_sync decodes every filename on every keys-only listing,
# one of which the DAG run-summary cache takes per dag per /dags poll.  At that
# rate a table lookup per byte beats a Python conditional per byte by 43% of
# the decode cost of a 50-entry listing of escaped names, measured.
#
# _FS_SAFE_DELETE serves only _decode_fs_token's fast path, a much narrower
# win: see the note there on which token shapes reach it.
_FS_BYTE_ESCAPE = [
    chr(byte) if chr(byte) in _FS_SAFE else "%{:02X}".format(byte)
    for byte in range(256)
]
_FS_SAFE_DELETE = str.maketrans("", "", "".join(sorted(_FS_SAFE)))

# Windows device names, reserved in every directory (case-insensitively).  A
# lowercase job name could otherwise pass through _fs_safe verbatim and make
# every open/mkdir under it fail (or hit the console device) on Windows.
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {"com{}".format(i) for i in range(1, 10)}
    | {"lpt{}".format(i) for i in range(1, 10)}
)

# Longest _fs_safe token emitted, comfortably under NAME_MAX (255) once
# the surrounding prefixes/suffixes are added.  Longer encodings are
# truncated and re-uniqued with a digest; without this a long or non-ASCII
# job name fails every append/list for its stream with ENAMETOOLONG.
_FS_SAFE_MAX = 130

# Marker joining a length-truncated token's kept head to its digest (see
# _fs_safe).  The natural encoding ("%" + 2 uppercase hex) can never emit
# it, so it positively identifies a truncated token, whose logical name is
# NOT recoverable from the token alone.
_FS_TRUNCATION_MARKER = "%."

# Sidecar inside a length-truncated stream's directory holding the exact
# logical stream name (raw UTF-8), the only way such a name round-trips
# out of list_stream_names (a garbled name re-encodes to a DIFFERENT token
# and the stream's state becomes collectable as garbage).  Deliberately
# not ``.json`` so record listing/pruning/migration never mistake it for
# a record.
_STREAM_NAME_SIDECAR = "stream-name.txt"


def _now() -> float:
    """Wall-clock epoch seconds; the one time source, so tests can patch it.

    Lease expiry and filename ordering are judged against this; across a
    shared mount the HA use of leases assumes bounded clock skew (NTP).
    """
    return time.time()


@functools.lru_cache(maxsize=4096)
def _fs_safe(name: str) -> str:
    """Return ``name`` as an injective, filename-safe token.

    A pure function of ``name``, memoized with a bounded cache: the same
    stream and namespace tokens recur on every store op and every
    directory-listing decode re-encodes each candidate name.

    Bytes outside :data:`_FS_SAFE` are percent-encoded from UTF-8, so
    arbitrary job names map to distinct, portable filenames.  Injectivity
    holds even case-insensitively: the safe set has no uppercase and the
    escape hex is fixed-case.  Three escape hatches:

    * a token that IS a reserved Windows device name (``con``, ``nul``,
      ...) gets its first character force-encoded (unambiguous: the
      natural encoding never escapes a safe character);
    * a token longer than :data:`_FS_SAFE_MAX` is truncated and re-uniqued
      with a SHA-256 digest of the original name, joined by ``%.``, which
      the natural encoding can never emit;
    * an empty name maps to ``_``.

    The encode uses ``surrogatepass`` so the function is TOTAL: names
    sourced from argv/filenames (``surrogateescape`` decodes) can carry a
    lone surrogate a strict encode rejects.  Each such code point maps to
    its own three-byte sequence, so injectivity is preserved.
    """
    token = _fs_safe_fragment(name) or "_"
    if token in _WINDOWS_RESERVED:
        token = "%{:02X}".format(ord(token[0])) + token[1:]
    if len(token) > _FS_SAFE_MAX:
        digest = hashlib.sha256(
            name.encode("utf-8", "surrogatepass")
        ).hexdigest()[:32]
        token = token[: _FS_SAFE_MAX - 34] + _FS_TRUNCATION_MARKER + digest
    return token


def _fs_safe_fragment(fragment: str) -> str:
    """Per-byte escape of a stream-name PREFIX, for on-disk prefix matching.

    The byte-escape core of :func:`_fs_safe` without its whole-token
    adjustments; valid for prefix matching because those adjustments only
    rewrite a token's FIRST character (reserved device names, whole-token
    matches only) or its over-length TAIL, so a managed prefix like
    ``runs/`` always survives verbatim at the front.
    """
    return "".join(
        map(
            _FS_BYTE_ESCAPE.__getitem__,
            fragment.encode("utf-8", "surrogatepass"),
        )
    )


def _decode_fs_token(token: str) -> Optional[str]:
    """The logical name a non-truncated ``_fs_safe`` token encodes, or None.

    Inverts :func:`_fs_safe` exactly: unquote to BYTES, then decode with
    the encoder's ``surrogatepass`` so a lone surrogate round-trips.  A
    lossy decode would return a DIFFERENT name that re-encodes to a
    different token, and the GC keep-set builders and orphan-blob sweep
    would then delete state the real stream still references.  ``None``
    for a token the encoder cannot have produced (undecodable bytes, or a
    name that does not re-encode to this exact token): the caller must
    report the entry unnameable, never act on a mangled name.

    A token already made entirely of :data:`_FS_SAFE` characters answers
    itself, with neither half of the round trip run: there is no ``%`` for the
    unquote to undo, and :func:`_fs_safe` would re-emit such a name verbatim,
    because each of its three escape hatches is ruled out by one of the guards
    standing beside the character test.  Each guard earns its place:
    ``_fs_safe("")`` is ``"_"``, ``_fs_safe("con")`` is ``"%63on"``, and an
    over-long name comes back as head + digest, so dropping any one of them
    hands back a name the encoder cannot have produced.
    ``tests/test_state.py`` carries one token per hatch.

    Fewer tokens reach the fast path than it looks.  Every managed stream
    prefix ends in ``/`` (``runs/``, ``manifests/``, ``dagrun/``), and a
    scheduled DAG run key is an ISO instant, so the ordinary on-disk token is
    heavily escaped (``2026-08-07%5412%3A00%3A00%2B00%3A00``) and goes the slow
    way; manual run keys and plain document keys are what hit.  So the branch
    is a trade.  Against carrying no fast path at all, a 50-entry listing of
    escaped names costs 2% to 9% more, and one of unescaped names costs ~75%
    less, which is the figure it is kept for.

    The ``"%"`` test comes first to hold that first cost down: it scans
    without allocating, where the character test allocates, so leading with
    the character test instead costs 13% to 20% on those same escaped
    listings.  Ordering it this way is worth 3% to 16% of an escaped listing.

    Escaped names got faster elsewhere, in :data:`_FS_BYTE_ESCAPE` in the
    re-encode above and in the module-scope ``unquote_to_bytes``.  Both apply
    whether or not this branch exists.
    """
    if (
        "%" not in token
        and token
        and not token.translate(_FS_SAFE_DELETE)
        and len(token) <= _FS_SAFE_MAX
        and token not in _WINDOWS_RESERVED
    ):
        return token
    try:
        name = unquote_to_bytes(token).decode("utf-8", "surrogatepass")
    except UnicodeError:
        # undecodable byte sequence, or a listing entry that itself carries
        # a lone surrogate (unquote_to_bytes encodes its input strictly).
        return None
    if not name or _fs_safe(name) != token:
        return None
    return name


def _record_epoch(name: str) -> float:
    """The write-epoch a record filename sorts by, or ``+inf`` (unknown).

    Unknown/foreign filenames map to ``+inf`` so an age-based sweep keeps
    their stream: never delete what cannot be classified.  ``nan``/``inf``
    spellings are rejected too: NaN compares False everywhere and would
    invert that contract.
    """
    try:
        epoch = float(name.split("-", 1)[0])
    except ValueError:
        return float("inf")
    if math.isnan(epoch) or math.isinf(epoch):
        return float("inf")
    return epoch


def _record_name_epoch_str(stem: str) -> Optional[str]:
    """The canonical zero-padded epoch head of a record filename stem.

    ``None`` for anything not shaped like the ``{:020.6f}`` head every name
    this module generates starts with.  Only canonical stems may seed the
    monotonic-name floor (see ``_next_record_name``): a foreign name's head
    cannot be bumped, and no generated (numeric) name could sort above a
    non-numeric one anyway.
    """
    head = stem.split("-", 1)[0]
    if (
        len(head) == 20
        and head[13] == "."
        and head[:13].isdigit()
        and head[14:].isdigit()
    ):
        return head
    return None


def _bump_record_epoch_str(head: str) -> str:
    """``head`` plus exactly one microsecond, in the same fixed-width form.

    Digit arithmetic, not float: near current epochs one float ULP is about
    a quarter microsecond, so a float round-trip could re-emit the SAME
    formatted head and break the strictly-above contract the name floor
    depends on.
    """
    digits = head.replace(".", "")
    bumped = str(int(digits) + 1).rjust(len(digits), "0")
    return "{}.{}".format(bumped[:-6], bumped[-6:])


def _unescape_mount(field: str) -> str:
    """Decode the octal escapes /proc/mounts uses for spaces/tabs/etc.

    A space in a mountpoint is written ``\\040``: a backslash then three
    octal digits.
    """
    if "\\" not in field:
        return field
    out: list[str] = []
    i = 0
    size = len(field)
    while i < size:
        octal = field[i + 1 : i + 4]
        if field[i] == "\\" and len(octal) == 3 and octal.isdigit():
            try:
                out.append(chr(int(octal, 8)))
                i += 4
                continue
            except ValueError:  # pragma: no cover - malformed escape
                pass
        out.append(field[i])
        i += 1
    return "".join(out)


def _mount_entry(path: str) -> Optional[tuple[str, str]]:
    """The ``(fstype, options)`` of the mount ``path`` lives on, or ``None``.

    Parses ``/proc/mounts``; longest matching mountpoint wins.  Linux-only;
    ``None`` where ``/proc`` is absent, which the caller treats as "cannot
    tell -> single-node".  The options column feeds the lock-fidelity
    check: ``nolock``/``local_lock`` NFS mounts honour flock only
    host-locally, which the fstype alone cannot reveal.
    """
    try:
        with open("/proc/mounts", encoding="utf-8") as fobj:
            lines = fobj.read().splitlines()
    except OSError:
        return None
    real = os.path.realpath(path)
    best_mount = ""
    best: Optional[tuple[str, str]] = None
    for line in lines:
        parts = line.split(" ")
        if len(parts) < 4:
            continue
        mountpoint = _unescape_mount(parts[1])
        fstype = parts[2]
        options = parts[3]
        prefix = mountpoint.rstrip("/") + "/"
        if real == mountpoint or real.startswith(prefix) or mountpoint == "/":
            # longest matching mountpoint wins (>= so "/" is a fallback only)
            if len(mountpoint) >= len(best_mount):
                best_mount = mountpoint
                best = (fstype, options)
    return best


def _mount_fstype(path: str) -> Optional[str]:
    """The filesystem type of the mount ``path`` lives on, or ``None``."""
    entry = _mount_entry(path)
    return entry[0] if entry is not None else None


def _local_lock_reason(path: str) -> Optional[str]:
    """A human reason the mount's locks are host-local, or ``None`` (fine).

    ``nolock`` and ``local_lock=flock``/``all`` NFS options satisfy flock
    locally without consulting the server, so two hosts each "hold" the
    same exclusive lock: the silent double-run a coordination consumer
    must refuse.  Linux-only; an undecidable mount returns ``None``.
    """
    entry = _mount_entry(path)
    if entry is None:
        return None
    fstype, options = entry
    if not fstype.startswith("nfs"):
        return None
    opts = options.split(",")
    if "nolock" in opts:
        return "the NFS mount is mounted with 'nolock'"
    for opt in opts:
        if opt.startswith("local_lock=") and opt.split("=", 1)[1] in (
            "flock",
            "all",
        ):
            return "the NFS mount is mounted with '{}'".format(opt)
    return None


def detect_topology(path: str) -> Optional[str]:
    """Probe: ``"shared"`` | ``"single-node"`` | ``None`` (cannot tell).

    ``None`` means the probe could not decide (no ``/proc``, or Windows), and
    the caller then falls back to ``single-node`` under ``topology: auto`` and
    lets an operator override with an explicit ``topology: shared``.
    """
    if IS_WINDOWS:
        # Windows has no cross-host lock story here and no /proc to probe; an
        # operator wanting shared semantics must assert it explicitly.
        return None
    fstype = _mount_fstype(path)
    if fstype is None:
        return None
    return "shared" if fstype in _SHARED_FSTYPES else "single-node"


class _LeaseUnreadable(Exception):
    """A lease file exists (or may exist) but cannot be trusted right now.

    Raised when the lease file is unreadable for any reason other than
    plain absence.  Lease ops fail closed on it: an acquire/renew is
    denied rather than treating "unreadable" as "no lease" and stealing a
    possibly-valid, unexpired lease from its live holder.
    """


class _DocumentUnreadable(Exception):
    """A document file exists (or may exist) but cannot be trusted right now.

    The document analogue of :class:`_LeaseUnreadable`, raised from the
    strict read inside :meth:`FilesystemStateBackend.mutate_document`.  A
    read-modify-write cannot proceed safely without a trustworthy current
    value, so the mutation fails rather than guessing: it surfaces as an
    error, never as a wrong value.
    """


@dataclass
class Lease:
    """A held (or observed) TTL lease.

    ``fence`` increases on every takeover (fixed across a same-holder
    renew of a still-valid lease), so a stale holder's late writes can be
    fenced off.  It is monotonic for the life of the store: release marks
    the lease expired in place, never deletes the file, so the counter
    survives release/re-acquire cycles instead of re-issuing fence values.
    ``expires_at`` is wall-clock epoch seconds; the lease is free to take
    over once ``_now() > expires_at``.
    """

    name: str
    holder: str
    fence: int
    expires_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "holder": self.holder,
            "fence": self.fence,
            "expiresAt": self.expires_at,
        }


class _TokenBucket:
    """Async token bucket bounding store operations per second.

    Rate/cost control for stores that bill per request.  Refilled from the
    loop's monotonic clock; burst is one second's worth of tokens (min 1).
    Single-loop use only (no lock): every await point is between full
    read-modify-write passes.
    """

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.burst = max(1.0, rate)
        self._tokens = self.burst
        self._last: Optional[float] = None

    async def throttle(self) -> float:
        """Take one token, sleeping until one is available.

        Returns the seconds slept (0.0 when a token was free), so the caller
        can account throttling separately from store latency.
        """
        loop = asyncio.get_running_loop()
        waited = 0.0
        while True:
            now = loop.time()
            if self._last is not None:
                self._tokens = min(
                    self.burst, self._tokens + (now - self._last) * self.rate
                )
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return waited
            need = (1.0 - self._tokens) / self.rate
            waited += need
            await asyncio.sleep(need)


class StateBackend(abc.ABC):
    """The seam every durable-state and coordination call goes through.

    Deliberately small: an append-only record store with derived-max
    cursors, mutable documents, blobs, a TTL lease, topology, lifecycle.
    :class:`FilesystemStateBackend` is the only implementation; a future
    native-S3 backend would use the same surface.
    """

    #: the resolved state config this backend was built from
    config: StateConfig
    #: backend name surfaced in :meth:`view_dict`
    backend_name: str = "state"

    # --- lifecycle -------------------------------------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """Create the store layout, probe topology, verify writability."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Release any resources (best-effort; the store itself persists)."""

    # --- durable immutable records ---------------------------------------

    @abc.abstractmethod
    async def append_record(
        self,
        stream: str,
        data: dict[str, Any],
        *,
        prune_keep: Optional[int] = None,
        prune_latest_by: Optional[str] = None,
    ) -> str:
        """Append one immutable record to ``stream``; return its record id.

        ``prune_keep`` folds the usual follow-up ``prune_records`` into
        the same backend call; the backend may AMORTISE the actual
        re-list-and-delete over several appends (the stream can briefly
        exceed the bound by a small constant no reader observes).  A
        caller that needs the bound enforced exactly NOW still calls
        :meth:`prune_records` directly.

        ``prune_latest_by`` is the NAME-KEYED counterpart for a stream
        whose records supersede one another by a field (the artifact
        store, keyed by ``"name"``): it keeps only the newest record per
        distinct value of that field, bounding the stream to the number of
        distinct values.  Unlike ``prune_keep`` it never removes the
        current version of any value.  Amortised on the same cadence.
        """

    @abc.abstractmethod
    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
        strict: bool = False,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
        max_matches: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Read back a stream's records (corrupt ones quarantined).

        ``strict=True`` makes an environmentally-unreadable record (an NFS
        blip) or one written by a NEWER schema PROPAGATE as an exception
        instead of being silently skipped; needed by any caller for whom a
        missed record is worse than a failed read (the orphan-blob sweep
        must not mistake "a reference I could not read" for "no
        reference").  The default stays best-effort.

        ``limit`` bounds the window of readable records considered;
        ``predicate`` filters which of that window is returned;
        ``max_matches`` stops the scan early once that many matches are
        collected.
        """

    @abc.abstractmethod
    async def list_stream_names(self, prefix: str) -> list[str]:
        """Logical stream names currently on disk starting with ``prefix``.

        Lets a caller discover the members of a per-host/per-scope stream
        family (e.g. :data:`cronstable.cron.MANIFEST_STREAM_PREFIX`)
        before reading each one.  Best-effort: an unreadable store returns
        ``[]`` rather than raising.
        """

    async def list_stream_names_audit(
        self, prefix: str
    ) -> "tuple[list[str], bool]":
        """``(names, complete)``: the listing plus whether it is exhaustive.

        ``complete`` is ``False`` when a matching stream exists but could
        not be NAMED (a truncated directory without its name sidecar).  A
        caller that will DELETE based on the listing (the orphan-blob
        sweep) must keep on any doubt.  The base backend cannot enumerate,
        so it reports an incomplete empty listing.
        """
        return [], False

    @abc.abstractmethod
    async def derive_max(self, stream: str, field: str) -> Optional[Any]:
        """The max value of ``field`` over a stream's records (the cursor).

        Order-independent, so on a shared mount where several nodes append to
        the same stream the result is the deterministic max, never a
        last-writer-wins race.  ``None`` if the stream is empty / the field
        absent.
        """

    @abc.abstractmethod
    async def prune_records(self, stream: str, *, keep: int) -> int:
        """Delete all but the newest ``keep`` records; return the # removed.

        ``keep <= 0`` deletes the whole stream.  Nodes racing on
        individual deletes is harmless (a missing file is ignored).
        """

    # --- mutable documents (job-facing KV / cursor / idempotency) --------

    @abc.abstractmethod
    async def read_document(
        self, namespace: str, key: str
    ) -> Optional[dict[str, Any]]:
        """The current body of document ``key`` in ``namespace``, or ``None``.

        An unlocked, best-effort read: absent, unreadable and corrupt all
        read back as ``None`` (the strict RMW read lives inside
        :meth:`mutate_document`).
        """

    @abc.abstractmethod
    async def mutate_document(
        self,
        namespace: str,
        key: str,
        transform: "Callable[[Optional[dict[str, Any]]], tuple[Any, _T]]",
    ) -> "tuple[Optional[dict[str, Any]], _T]":
        """Atomically read-modify-write document ``key``.

        Runs ``transform(current_body)`` under an advisory ``flock`` over
        the document's lock file, serialising the whole RMW fleet-wide on
        a shared mount (what monotonic cursors and idempotency claims
        depend on).  ``transform`` returns ``(new_body, result)``:
        ``new_body`` is the body to persist, or :data:`DOC_KEEP` /
        :data:`DOC_DELETE`.  Returns ``(stored_body, result)`` with the
        body now on disk (``None`` after a delete).  ``transform`` must be
        pure and side-effect-free: it runs on a worker thread and may be
        retried on a torn read.
        """

    @abc.abstractmethod
    async def delete_document(self, namespace: str, key: str) -> bool:
        """Delete document ``key``; return whether it existed."""

    @abc.abstractmethod
    async def list_documents(self, namespace: str) -> list[dict[str, Any]]:
        """Every readable document body in ``namespace``, order-independent."""

    async def list_document_keys(self, namespace: str) -> Optional[list[str]]:
        """The keys of ``namespace``'s documents WITHOUT reading any body.

        ``None`` means keys cannot be enumerated cheaply and faithfully
        right now (no capability, an unreadable directory, or an on-disk
        name that cannot round-trip); the caller must then fall back to
        :meth:`list_documents`.  An empty namespace is ``[]``, not
        ``None``.
        """
        return None

    async def list_document_namespaces(
        self, prefix: str
    ) -> "tuple[list[str], bool]":
        """``(namespaces, complete)``: namespaces starting with ``prefix``.

        The GC uses this to discover the per-dag run-document namespaces
        (``dagrun/<dag>``).  ``complete`` is ``False`` when a matching
        namespace exists but its logical name is unrecoverable (truncated;
        namespaces have no name sidecar), so a deleting caller keeps
        instead.  The base backend cannot enumerate, so it reports an
        incomplete empty listing.
        """
        return [], False

    # --- content-addressed blobs (job-facing artifact payloads) ----------

    @abc.abstractmethod
    async def put_blob(self, data: bytes) -> str:
        """Store ``data`` (deduplicated by content); return its SHA-256 hex."""

    @abc.abstractmethod
    async def get_blob(self, digest: str) -> Optional[bytes]:
        """Read the blob with SHA-256 ``digest``, or ``None`` if absent."""

    # --- advisory-lock TTL lease -----------------------------------------

    @abc.abstractmethod
    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        """Take (or renew) lease ``name`` for ``ttl``s, else ``None``.

        A caller that bounds this with a timeout must treat a timeout as
        UNKNOWN, not as denied: the abandoned worker may still complete the
        acquisition on disk, leaving the lease held (by this holder) until
        its TTL lapses.
        """

    @abc.abstractmethod
    async def renew_lease(self, lease: Lease, ttl: float) -> Optional[Lease]:
        """Extend a still-held lease; ``None`` if it was taken over."""

    @abc.abstractmethod
    async def release_lease(self, lease: Lease) -> None:
        """Release a lease we hold (a no-op if we no longer hold it)."""

    @abc.abstractmethod
    async def read_lease(self, name: str) -> Optional[Lease]:
        """Observe a lease without taking it (best-effort, unlocked read)."""

    # --- maintenance -------------------------------------------------------

    async def collect_garbage(
        self,
        *,
        keep: dict[str, "set[str]"],
        grace: float,
        ephemeral_lease_prefixes: "tuple[str, ...]" = (),
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Remove streams no recent manifest references (see the filesystem
        backend for semantics).  ``ephemeral_lease_prefixes`` names the
        per-run lease classes whose dead files may be reclaimed; every
        other lease is never touched.  The base backend has nothing to
        collect."""
        return {}

    async def migrate_schema(self, *, dry_run: bool = False) -> dict[str, Any]:
        """Rewrite records of older known schemes to the current one (see
        :data:`RECORD_MIGRATIONS`); the base backend has nothing to walk."""
        return {}

    async def sweep_orphan_blobs(
        self,
        referenced: "set[str]",
        grace: float,
        *,
        dry_run: bool = False,
    ) -> int:
        """Delete artifact blobs no surviving record references (see the
        filesystem backend); the base backend stores no blobs."""
        return 0

    # --- introspection ---------------------------------------------------

    @property
    @abc.abstractmethod
    def topology(self) -> str:
        """``"shared"`` | ``"single-node"`` | ``"unknown"`` (before start)."""

    def supports_shared_locking(self) -> bool:
        """Whether a lease here excludes across hosts (HA-capable)."""
        return self.topology == "shared"

    async def verify_locking(self) -> Optional[str]:
        """Why the store's locks must not be trusted for coordination, or
        ``None`` (they behave, or the backend has no way to tell).  See the
        filesystem backend for the real probe."""
        return None

    def stats(self) -> dict[str, Any]:
        """Self-observability counters (op counts/errors/latency, lock
        contention, throttling, worker-lane occupancy); ``{}`` for a backend
        with none."""
        return {}

    def view_dict(self) -> dict[str, Any]:
        """The state view for a future ``GET /state`` / the dashboard."""
        return {"backend": self.backend_name, "topology": self.topology}

    async def inventory(self) -> dict[str, Any]:
        """A metadata-only snapshot for the dashboard's state inspector:
        health plus, where enumerable, per-prefix stream/document counts,
        scope lists, and active leases.  NEVER returns record payloads or
        document values.  The base backend cannot enumerate, so it reports
        ``enumerable: false`` and the health block alone."""
        return {
            "view": self.view_dict(),
            "stats": self.stats(),
            "enumerable": False,
            "records": {},
            "documents": {},
            "leases": [],
            "quarantine": 0,
        }


class _StateWorker:
    """A persistent daemon thread that runs one state op at a time.

    Pooled because creating an OS thread costs far more than handing a
    callable to one parked on a queue.  The worker returns to the idle
    pool only AFTER its op fully returns, which preserves abandonability:
    a worker wedged in an uninterruptible syscall on a dead mount is never
    in the pool, so the next submit spawns a replacement instead of
    queueing behind it.
    """

    __slots__ = ("_box",)

    def __init__(self) -> None:
        self._box: "queue.SimpleQueue[Callable[[], None]]" = (
            queue.SimpleQueue()
        )
        # daemon: see _call's docstring.  A parked worker is blocked in the
        # queue, not in a syscall, so it can never delay interpreter exit.
        threading.Thread(
            target=self._serve, daemon=True, name="cronstable-state"
        ).start()

    def submit(self, job: Callable[[], None]) -> None:
        """Hand ``job`` to this (idle) worker.  Never blocks."""
        self._box.put(job)

    def _serve(self) -> None:
        while True:
            job = self._box.get()
            try:
                job()
            finally:
                # Drop the closure before parking: it holds the op's args
                # and result.  An exception escaping the job ends this
                # thread without parking it, so a worker is never reused
                # after misbehaving.
                job = None  # type: ignore[assignment]
            if not _park_state_worker(self):
                return


# Idle state workers, parked between ops.  Process-wide because a parked
# worker holds no backend state; how many can be BUSY at once is still
# bounded per backend by the two lanes' semaphores in _call.
_IDLE_WORKERS: list[_StateWorker] = []
_IDLE_WORKERS_LOCK = threading.Lock()

# Cap on parked workers: the most one backend can ever have busy at once.
# A worker finishing into a full pool exits instead of parking, so the
# resident thread count stays bounded.
_MAX_IDLE_WORKERS = BULK_CALL_SLOTS + LEASE_CALL_SLOTS


def _park_state_worker(worker: _StateWorker) -> bool:
    """Return ``worker`` to the idle pool; ``False`` if it should exit."""
    with _IDLE_WORKERS_LOCK:
        if len(_IDLE_WORKERS) >= _MAX_IDLE_WORKERS:
            return False
        _IDLE_WORKERS.append(worker)
        return True


def _take_state_worker() -> _StateWorker:
    """An idle worker, or a fresh one when none is parked."""
    with _IDLE_WORKERS_LOCK:
        if _IDLE_WORKERS:
            # LIFO: reuse the most recently parked worker, whose stack and
            # thread-local allocator arena are the warmest.
            return _IDLE_WORKERS.pop()
    return _StateWorker()


def store_identity(config: Mapping[str, Any]) -> tuple[str, str]:
    """The store a ``state`` section names, as the backend resolves it: the
    absolute root (``~`` expanded; ``path: ~/state`` must mean the home
    directory, not a literal ``~`` under whatever CWD the daemon started
    in) and the namespace, ``default`` when ``deploymentId`` is unset.
    Two sections with one identity are one store.
    """
    return (
        os.path.abspath(os.path.expanduser(config["path"])),
        config.get("deploymentId") or "default",
    )


class FilesystemStateBackend(StateBackend):
    """A durable state backend over any POSIX filesystem.

    Serves both a local directory (single-node durability) and an Amazon S3
    Files / EFS mount (durability + fleet-wide coordination) with identical
    code; see the module docstring for why that works and what it assumes.
    """

    backend_name = "filesystem"

    def __init__(
        self, config: StateConfig, get_job_set_id: Callable[[], str]
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        # a stable namespace so several deployments can share one store
        # without colliding; job-set scoping (like the lease backends'
        # @reboot set) is layered on top by callers through the stream name.
        self.root, self.namespace = store_identity(config)
        # The namespaced root all this backend's files live under, plus the
        # per-lane subroots; every path helper hangs off these, and root and
        # namespace hold for the backend's lifetime.
        self.base = os.path.join(self.root, _fs_safe(self.namespace))
        self._records_root = os.path.join(self.base, RECORDS_DIR)
        self._leases_root = os.path.join(self.base, LEASES_DIR)
        self._docs_root = os.path.join(self.base, DOCS_DIR)
        self._blobs_root = os.path.join(self.base, BLOBS_DIR)
        self._tmp_root = os.path.join(self.base, TMP_DIR)
        self._quarantine_root = os.path.join(self.base, QUARANTINE_DIR)
        self._configured_topology: str = config.get("topology", "auto")
        self._topology = "unknown"
        # a per-process id mixed into every written filename, so records and
        # temp files from different nodes/processes onto one shared mount never
        # collide on a name.  os.urandom is fine (uniqueness, not secrecy).
        self._instance = os.urandom(6).hex()
        # every temp file's fixed path head; only the sequence number varies
        self._tmp_prefix = os.path.join(
            self._tmp_root, "w-{}-".format(self._instance)
        )
        # Worker threads may run several sync halves at once; an unlocked
        # `self._seq += 1` can interleave, and a duplicated seq (plus the
        # coarse Windows clock) means one record silently clobbering
        # another via the atomic rename.
        self._seq = 0
        self._seq_lock = threading.Lock()
        # Bounds concurrent worker threads (see _call); excess calls queue
        # on the semaphore as cheap pending tasks.  Created lazily so
        # construction needs no running event loop.  The LEASE lane is
        # deliberately SEPARATE (see BULK_CALL_SLOTS / LEASE_CALL_SLOTS)
        # so bulk traffic can never starve a lease renew below its TTL.
        self._call_slots: Optional[asyncio.Semaphore] = None
        self._lease_slots: Optional[asyncio.Semaphore] = None
        # Optional request-rate control (state.maxOpsPerSecond): every op
        # takes a token before its worker thread is spawned, so a billing-
        # sensitive mount sees a bounded request rate. 0/absent -> off.
        rate = float(config.get("maxOpsPerSecond") or 0)
        self._rate_limit = _TokenBucket(rate) if rate > 0 else None
        # Self-observability accumulators (see stats()).  Updated from the
        # worker threads, hence the plain lock; read (snapshotted) from the
        # event loop at scrape time.
        self._stats_lock = threading.Lock()
        # op -> [count, errors, seconds-of-store-time]
        self._op_stats: dict[str, list[float]] = {}
        self._lock_acquisitions = 0
        self._lock_wait_seconds = 0.0
        self._throttled_ops = 0
        self._throttle_wait_seconds = 0.0
        # Live worker-thread gauges per lane, plus high-water marks.  A
        # hung mount pins its lane's gauge at capacity: the "store is
        # wedged" signal the completed-op counters (which only tick when
        # an op FINISHES) cannot show.  Guarded by _stats_lock so stats()
        # reads a consistent snapshot.
        self._inflight_bulk = 0
        self._inflight_lease = 0
        self._inflight_peak_bulk = 0
        self._inflight_peak_lease = 0
        # Per-stream countdown gating the prune an append can carry (see
        # append_record's ``prune_keep``).  Written from worker threads,
        # hence the lock.  Keyed by the stream's on-disk TOKEN, not its
        # logical name, so GC (which sees only tokens) can drop the entry
        # when it removes the directory; _fs_safe is injective, so the two
        # keyings are interchangeable otherwise.
        self._prune_countdown: dict[str, int] = {}
        self._prune_gate_lock = threading.Lock()
        # Per-stream record-name floor (see _next_record_name): the
        # highest name stem known per stream, so a backward wall-clock
        # step can never mint a name sorting below retained history (the
        # amortised prune would delete the just-written record; the
        # derive_max watermark would exclude it).  Keyed by on-disk token
        # and bounded like _prune_countdown; absent key means "not seeded
        # yet".  Written from worker threads, hence the lock; _next_seq
        # nests inside it (floor lock then seq lock, never the reverse),
        # so the pair cannot deadlock.
        self._record_name_floor: dict[str, str] = {}
        self._record_name_floor_lock = threading.Lock()
        # Derived-cursor memo (see _derive_max_sync): (stream token,
        # field) -> (newest filename folded so far, best value at or below
        # it), so a repeat derive_max parses only records appended since
        # the last call.  Bounded by the distinct (stream, field) pairs
        # ever derived, so no eviction.  _derive_wipe_gen counts wholesale
        # stream deletions (prune keep <= 0, gc): a derive racing a wipe
        # must not write its stale fold back, so memo writes are gated on
        # the generation observed before scanning.  Read and written from
        # worker threads, hence the lock.
        self._derive_memo: dict[tuple[str, str], tuple[str, Any]] = {}
        self._derive_wipe_gen: dict[str, int] = {}
        self._derive_memo_lock = threading.Lock()
        # Record content cache (see _read_record): record path -> raw
        # bytes of a record that read back valid.  Insertion order is the
        # LRU order.  Locked: the byte total is a running sum, so
        # plain-dict atomicity under the GIL is not enough on its own.
        self._record_cache: dict[str, bytes] = {}
        self._record_cache_bytes = 0
        self._record_cache_lock = threading.Lock()

    # --- paths -----------------------------------------------------------

    def _stream_dir(self, stream: str) -> str:
        return os.path.join(self._records_root, _fs_safe(stream))

    def _lease_paths(self, name: str) -> tuple[str, str]:
        safe = _fs_safe(name)
        leases = self._leases_root
        return (
            os.path.join(leases, safe + ".lock"),
            os.path.join(leases, safe + ".lease"),
        )

    def _doc_dir(self, namespace: str) -> str:
        return os.path.join(self._docs_root, _fs_safe(namespace))

    def _doc_paths(self, namespace: str, key: str) -> tuple[str, str]:
        """The ``(lock file, doc file)`` for one document.

        Like a lease, the flock rides a stable side-file (``.lock``) while the
        value file (``.doc``) is swapped out by the atomic rename; locking
        the value file directly would lock an inode about to be replaced.
        """
        ns_dir = self._doc_dir(namespace)
        safe = _fs_safe(key)
        return (
            os.path.join(ns_dir, safe + ".lock"),
            os.path.join(ns_dir, safe + ".doc"),
        )

    def _blob_path(self, digest: str) -> str:
        """The on-disk path of a content-addressed blob.

        Sharded by the first two hex characters so one namespace's blob
        directory never grows to a single flat directory of millions of
        entries (which some filesystems handle poorly).
        """
        # Reject anything but lowercase 64-char sha256 hex before it
        # reaches the filesystem, so a crafted digest (e.g. from a
        # malicious restore archive) cannot escape the blob directory via
        # ".." or a path separator.
        if len(digest) != 64 or not _HEX_DIGITS.issuperset(digest):
            raise ValueError("invalid blob digest: {!r}".format(digest))
        return os.path.join(self._blobs_root, digest[:2], digest + ".blob")

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq += 1
            return self._seq

    def _tmp_path(self) -> str:
        return self._tmp_prefix + "{:012d}.tmp".format(self._next_seq())

    # --- worker threads ----------------------------------------------------

    async def _call(self, op: str, fn: Callable[..., _T], *args: Any) -> _T:
        """Run blocking ``fn(*args)`` on a *daemon* thread and await it.

        Not ``asyncio.to_thread``: the default executor's threads are
        non-daemonic and joined at interpreter exit, so one worker wedged
        in an uninterruptible NFS syscall would hang process shutdown
        forever.  Daemon threads keep a hung store abandonable: callers
        can time out (``asyncio.wait_for``) and exit; the OS reclaims the
        stuck thread.

        The daemon threads are POOLED (:class:`_StateWorker`) rather than
        created per call.  Abandonability is unchanged: a worker rejoins
        the pool only once its op returns, so a wedged one is never handed
        more work.

        ``op`` labels the call in the stats: count, errors and seconds of
        store time (measured around ``fn`` on the worker thread, excluding
        queueing/throttling) accumulate per label; see :meth:`stats`.
        """
        loop = asyncio.get_running_loop()
        is_lease = op.startswith("lease-")
        if self._rate_limit is not None and not is_lease:
            # take the rate token BEFORE a worker slot, so a throttled op
            # queues as a cheap pending coroutine, not a held thread slot.
            # Lease operations BYPASS the bucket: a renew queued behind a
            # bulk burst could overshoot its TTL, expiring a live holder's
            # lease and double-running the job it exists to fence.
            waited = await self._rate_limit.throttle()
            if waited > 0.0:
                with self._stats_lock:
                    self._throttled_ops += 1
                    self._throttle_wait_seconds += waited
        # Pick the worker lane.  Lease ops get their OWN pool so bulk
        # writes (or bulk threads wedged on a hung mount) can never hold
        # every slot and delay a lease renew past its TTL: the same
        # split-brain hazard the rate-limiter bypass above guards against.
        if is_lease:
            if self._lease_slots is None:
                self._lease_slots = asyncio.Semaphore(LEASE_CALL_SLOTS)
            slots = self._lease_slots
        else:
            if self._call_slots is None:
                self._call_slots = asyncio.Semaphore(BULK_CALL_SLOTS)
            slots = self._call_slots
        # The slot is held for the THREAD's lifetime, released from its
        # completion callback, not scoped to this await, which a wait_for
        # timeout can cancel while the thread is still stuck in a syscall.
        # Scoping it here would un-bound the thread count in exactly the
        # hung-store case the cap exists for.
        await slots.acquire()
        self._enter_inflight(is_lease)
        future: asyncio.Future = loop.create_future()

        def _resolve(result: Any, exc: Optional[BaseException]) -> None:
            slots.release()
            self._exit_inflight(is_lease)
            if future.cancelled():
                return  # the awaiter timed out / went away: nobody to tell
            if exc is not None:
                future.set_exception(exc)
            else:
                future.set_result(result)

        def _runner() -> None:
            result: Any = None
            exc: Optional[BaseException] = None
            began = time.perf_counter()
            try:
                result = fn(*args)
            except BaseException as ex:  # noqa: BLE001 - relayed to awaiter
                exc = ex
            # Stats update on the worker thread (never lost to an abandoned
            # await): an op stuck in a syscall simply reports when it
            # finally returns, which is exactly the latency worth seeing.
            elapsed = time.perf_counter() - began
            with self._stats_lock:
                entry = self._op_stats.get(op)
                if entry is None:
                    entry = self._op_stats[op] = [0, 0, 0.0]
                entry[0] += 1
                if exc is not None:
                    entry[1] += 1
                entry[2] += elapsed
            try:
                loop.call_soon_threadsafe(_resolve, result, exc)
            except RuntimeError:
                # the loop already closed (late finish during teardown):
                # nothing is waiting; drop the result (the slot stays taken,
                # which is moot: the loop is gone).
                pass

        try:
            _take_state_worker().submit(_runner)
        except BaseException:
            slots.release()  # the op never ran; nobody else will free it
            self._exit_inflight(is_lease)
            raise
        return cast(_T, await future)

    def _enter_inflight(self, is_lease: bool) -> None:
        """Count a just-acquired worker slot (and track the lane's peak)."""
        with self._stats_lock:
            if is_lease:
                self._inflight_lease += 1
                self._inflight_peak_lease = max(
                    self._inflight_peak_lease, self._inflight_lease
                )
            else:
                self._inflight_bulk += 1
                self._inflight_peak_bulk = max(
                    self._inflight_peak_bulk, self._inflight_bulk
                )

    def _exit_inflight(self, is_lease: bool) -> None:
        """Release a worker slot from the live gauge (peak is left intact)."""
        with self._stats_lock:
            if is_lease:
                self._inflight_lease -= 1
            else:
                self._inflight_bulk -= 1

    # --- lifecycle -------------------------------------------------------

    @property
    def topology(self) -> str:
        return self._topology

    async def start(self) -> None:
        await self._call("start", self._start_sync)
        logger.info(
            "state: filesystem backend ready at %s "
            "(namespace=%s, topology=%s, shared_locking=%s)",
            self.base,
            self.namespace,
            self._topology,
            self.supports_shared_locking(),
        )

    def _start_sync(self) -> None:
        # 0o700 (further narrowed by the umask): records and archived output
        # can carry job output (secrets, even post-redaction), so nothing
        # here should be world-readable on a multi-user host.  Only applied to
        # directories this process creates; an operator who pre-created the
        # tree with wider modes has made that choice deliberately.
        for sub in (
            RECORDS_DIR,
            LEASES_DIR,
            QUARANTINE_DIR,
            TMP_DIR,
            DOCS_DIR,
            BLOBS_DIR,
        ):
            self._makedirs_durable(os.path.join(self.base, sub))
        self._topology = self._resolve_topology()
        # Fail start() loudly if the store is not actually writable (a bad
        # mount, wrong permissions) rather than silently swallowing every later
        # write: write, fsync and remove a tiny probe file.  start_stop_state
        # catches the OSError, logs it, and keeps running the in-memory path.
        probe = os.path.join(
            self.base, TMP_DIR, "startup-{}.probe".format(self._instance)
        )
        try:
            with open(probe, "wb") as fobj:
                fobj.write(b"ok")
                fobj.flush()
                os.fsync(fobj.fileno())
        finally:
            # remove the probe even when the write/fsync raised (a probe
            # that failed midway would otherwise leak into TMP_DIR on every
            # failed start retry).
            with contextlib.suppress(OSError):
                os.unlink(probe)
        self._stamp_meta_sync()

    def _stamp_meta_sync(self) -> None:
        """Stamp a fresh store with the record-scheme version (once).

        The upfront signal: a store last written by a NEWER scheme logs
        one pointed warning at start instead of quietly quarantining
        history record by record.  Read raw (not via ``_read_record``,
        which would quarantine exactly the mismatched stamp that matters).
        Best-effort throughout: the stamp is advisory, never load-bearing.
        """
        stream_dir = self._stream_dir("meta")
        try:
            names = sorted(
                [n for n in os.listdir(stream_dir) if n.endswith(".json")]
            )
        except OSError:
            names = []
        for name in reversed(names):
            try:
                with open(os.path.join(stream_dir, name), "rb") as fobj:
                    obj = _json.loads(fobj.read())
            except Exception:  # noqa: BLE001 - unreadable stamp: keep looking
                continue
            if isinstance(obj, dict) and "schemaVersion" in obj:
                version = obj.get("schemaVersion")
                if version != SCHEMA_VERSION:
                    logger.warning(
                        "state: the store at %s was last stamped by a build "
                        "writing record scheme %r (this build writes %r); "
                        "records this build cannot read are quarantined; "
                        "consider `cronstable state migrate-schema`",
                        self.base,
                        version,
                        SCHEMA_VERSION,
                    )
                return
        with contextlib.suppress(OSError):
            self._append_sync("meta", {"storeVersion": SCHEMA_VERSION})

    def _resolve_topology(self) -> str:
        configured = self._configured_topology
        detected = detect_topology(self.root)
        if configured in ("shared", "single-node"):
            if detected is not None and detected != configured:
                logger.warning(
                    "state: topology configured as %r but the mount at %s "
                    "looks %r; trusting the configured value (make sure the "
                    "mount really does%s support cross-host locking)",
                    configured,
                    self.root,
                    detected,
                    "" if configured == "shared" else " not",
                )
            return configured
        # auto
        if detected is None:
            logger.info(
                "state: could not determine whether %s is a shared mount; "
                "assuming single-node (set state.topology: shared to enable "
                "fleet-wide coordination on a network mount)",
                self.root,
            )
            return "single-node"
        return detected

    async def stop(self) -> None:
        # Nothing to tear down: no background tasks, no long-lived open
        # handles.  Present for symmetry with the ABC.
        return None

    # --- record store ----------------------------------------------------

    @staticmethod
    def _retry_sharing_violation(op: Callable[[], None]) -> None:
        """Run ``op``, retrying briefly on a Windows sharing violation.

        The retry ladder behind :meth:`_replace` and :meth:`_unlink`:
        such holds clear in milliseconds, so a short backoff beats
        surfacing a spurious error from a healthy store.
        """
        for attempt in range(5):
            try:
                op()
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))

    @staticmethod
    def _replace(src: str, dest: str) -> None:
        """``os.replace`` that rides out Windows sharing violations.

        On Windows, replacing a file another handle has open (the deliberately
        unlocked ``read_lease``, an antivirus/backup scan) raises
        ``PermissionError`` because CPython opens files without
        FILE_SHARE_DELETE; on POSIX this is a single plain replace.
        """
        if not IS_WINDOWS:
            os.replace(src, dest)
            return
        FilesystemStateBackend._retry_sharing_violation(
            lambda: os.replace(src, dest)
        )

    @staticmethod
    def _unlink(path: str) -> None:
        """``os.unlink`` that rides out Windows sharing violations.

        The delete-side twin of :meth:`_replace`, for the same
        missing-FILE_SHARE_DELETE reason; on POSIX a single plain unlink.
        """
        if not IS_WINDOWS:
            os.unlink(path)
            return
        FilesystemStateBackend._retry_sharing_violation(
            lambda: os.unlink(path)
        )

    def _atomic_write(
        self, dest: str, payload: bytes, *, durable_rename: bool = True
    ) -> None:
        """Write ``payload`` to ``dest`` via a temp file + atomic rename.

        The rename is atomic on a local filesystem, on Windows
        (os.replace), and on an Amazon S3 Files mount (file rename is
        atomic there even though the object store has no native rename),
        so a reader never observes a half-written ``dest``.

        Data files are created 0o600: records and archived output can
        carry job output, which is exactly where secrets live.  After the
        rename the parent directory is flushed
        (:func:`cronstable.platform.fsync_directory`): without it the
        rename is not crash-durable, and a power loss could silently drop
        an acknowledged record, regress the derived watermark, and
        double-run jobs on the next boot.

        ``durable_rename=False`` skips only that directory barrier, for
        the one write shape where losing the RENAME is harmless: a
        same-fence lease refresh, whose pre-rename content is a complete,
        merely earlier state (see :meth:`_write_lease_file`).  The
        temp-file fsync is kept even then: a crash committing the rename
        but not the data would leave ``dest`` truncated, and a corrupt
        lease file fails every later acquire closed.
        """
        tmp = self._tmp_path()
        try:
            fdesc = os.open(tmp, _ATOMIC_WRITE_FLAGS, 0o600)
            try:
                # Raw descriptor writes: the payload is one finished bytes
                # object, so a buffered writer's buffer, flush, and object
                # setup add only overhead to every record and lease write.
                # The descriptor is opened in binary mode (see
                # _ATOMIC_WRITE_FLAGS), so the bytes land as given on
                # every platform.  os.write may write short (a signal, an
                # odd mount), hence the loop.
                view = memoryview(payload)
                while view:
                    view = view[os.write(fdesc, view) :]
                os.fsync(fdesc)
            finally:
                os.close(fdesc)
            self._replace(tmp, dest)
        except BaseException:
            # never leave the temp file behind on a failed write/rename
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        if durable_rename:
            fsync_directory(os.path.dirname(dest))

    def _makedirs_durable(self, path: str) -> None:
        """``os.makedirs(path, exist_ok=True)``, but crash-durably.

        A new directory's ENTRY lives in its parent; without flushing the
        parents, a power loss can drop a freshly created subtree with
        every individually-fsynced record inside it.  Walks up to the
        first existing ancestor before creating anything, then flushes
        each newly-created level's PARENT.  The leaf's own entry is
        covered by whichever write follows into it (e.g.
        :meth:`_atomic_write`).
        """
        if os.path.isdir(path):
            return
        created = []
        cur = path
        while cur and not os.path.isdir(cur):
            created.append(cur)
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
        os.makedirs(path, mode=0o700, exist_ok=True)
        for level in created:
            fsync_directory(os.path.dirname(level))

    async def append_record(
        self,
        stream: str,
        data: dict[str, Any],
        *,
        prune_keep: Optional[int] = None,
        prune_latest_by: Optional[str] = None,
    ) -> str:
        return await self._call(
            "append",
            self._append_sync,
            stream,
            data,
            prune_keep,
            prune_latest_by,
        )

    def _append_sync(
        self,
        stream: str,
        data: dict[str, Any],
        prune_keep: Optional[int] = None,
        prune_latest_by: Optional[str] = None,
    ) -> str:
        token = _fs_safe(stream)
        # _stream_dir(stream), spelled out so the token is in hand without
        # a basename of the path on every append.
        stream_dir = os.path.join(self._records_root, token)
        self._makedirs_durable(stream_dir)
        if _FS_TRUNCATION_MARKER in token:
            # a truncated token cannot round-trip through enumeration on its
            # own; land (or lazily repair) the logical-name sidecar so
            # list_stream_names can return the exact name.
            self._ensure_stream_name_sidecar(stream_dir, token, stream)
        # Filename sort key: zero-padded write-time epoch (lexicographic
        # == chronological), then instance+seq for uniqueness; the
        # filename only orders listing.  Generation is floor-clamped
        # (_next_record_name): pruning and the derive_max watermark
        # require names to only ever grow, even across a backward
        # wall-clock step.
        rec_id = self._next_record_name(token, stream_dir)
        payload = _json.dumps_bytes(
            {"schemaVersion": SCHEMA_VERSION, "data": data}, sort_keys=True
        )
        self._atomic_write(os.path.join(stream_dir, rec_id + ".json"), payload)
        # None and a non-positive keep both mean no bounded prune, folded to
        # one int so the gate and the prune call below test it once.
        keep = prune_keep if prune_keep is not None and prune_keep > 0 else 0
        if keep or prune_latest_by:
            # The folded, amortised prune (every K-th append per stream).
            # Best-effort by construction: the append HAS landed, so a
            # prune failure must never make the whole call read as failed
            # (callers make load-bearing decisions, e.g. the @reboot
            # launch gate, from whether the append landed).  One gate
            # check drives both prune kinds so the countdown is consumed
            # once.
            if self._append_prune_due(token):
                try:
                    if keep:
                        self._prune_sync(stream, keep)
                    if prune_latest_by:
                        self._prune_latest_by_sync(stream, prune_latest_by)
                except OSError as ex:
                    logger.warning(
                        "state: could not prune stream %r after an append "
                        "(kept records linger until the next prune): %s",
                        stream,
                        ex,
                    )
        return rec_id

    def _append_prune_due(self, token: str) -> bool:
        with self._prune_gate_lock:
            left = self._prune_countdown.get(token, 0)
            if left <= 0:
                if len(self._prune_countdown) >= _PRUNE_COUNTDOWN_MAX_STREAMS:
                    # Bound the map (see _PRUNE_COUNTDOWN_MAX_STREAMS). Only
                    # countdowns are lost, so the worst case is one extra
                    # prune per stream on its next append.
                    self._prune_countdown.clear()
                self._prune_countdown[token] = _PRUNE_EVERY_APPENDS - 1
                return True
            self._prune_countdown[token] = left - 1
            return False

    def _prune_countdown_forget(self, token: str) -> None:
        """Drop one stream's append-prune countdown after its dir is gone.

        Without this the map only ever grows (crontab ``<file>:<line>``
        job names churn on edits).  GC removing the directory proves the
        name retired; a name that comes back re-seeds at its next append.
        """
        with self._prune_gate_lock:
            self._prune_countdown.pop(token, None)

    def _record_name_floor_scan(self, stream_dir: str) -> str:
        """The highest canonical record stem on disk, ``""`` when none.

        Seeds the name floor once per stream per process.  Run OUTSIDE the
        floor lock: a listing can block on a hung mount and the lock must
        stay memory-only.  An unreadable directory seeds an empty floor:
        never an error on the append path.
        """
        try:
            names = os.listdir(stream_dir)
        except OSError:
            return ""
        best = ""
        for name in names:
            if not name.endswith(".json"):
                continue
            stem = name[: -len(".json")]
            if stem > best and _record_name_epoch_str(stem) is not None:
                best = stem
        return best

    def _next_record_name(self, token: str, stream_dir: str) -> str:
        """A fresh record stem sorting strictly above the stream's floor.

        Two mechanisms assume record names only ever grow:
        :meth:`_prune_sync` keeps the lexicographic tail, and the
        derive_max memo scans only names above its watermark.  A backward
        clock step (NTP, or a fast-clocked peer on a shared store) could
        otherwise mint a name below retained history: the next amortised
        prune would delete the just-acknowledged record and derive_max
        would never see it.  When the natural stem does not clear the
        floor, the floor's epoch is bumped one microsecond; instance+seq
        keeps the name unique.  The floor is in-process on purpose: peers'
        names fold in at seed time and at each amortised prune, and a
        restart re-seeds from the directory itself.
        """
        with self._record_name_floor_lock:
            seeded = token in self._record_name_floor
        disk = "" if seeded else self._record_name_floor_scan(stream_dir)
        with self._record_name_floor_lock:
            if token not in self._record_name_floor:
                if (
                    len(self._record_name_floor)
                    >= _PRUNE_COUNTDOWN_MAX_STREAMS
                ):
                    # bound the map like _prune_countdown: only floors are
                    # lost, and the next append per affected stream re-seeds
                    # from one directory listing.
                    self._record_name_floor.clear()
                self._record_name_floor[token] = disk
            floor = max(self._record_name_floor[token], disk)
            stem = "{:020.6f}-{}-{:012d}".format(
                _now(), self._instance, self._next_seq()
            )
            if stem <= floor:
                stem = "{}-{}-{:012d}".format(
                    _bump_record_epoch_str(floor.split("-", 1)[0]),
                    self._instance,
                    self._next_seq(),
                )
            self._record_name_floor[token] = stem
        return stem

    def _record_name_floor_raise(self, token: str, stem: str) -> None:
        """Raise (never lower, never insert) one stream's name floor.

        Fed by listings the prune already paid for, so other processes'
        names fold in at every amortised prune.  Never inserts: a stream
        this process never appends to needs no entry in the bounded map.
        """
        with self._record_name_floor_lock:
            known = self._record_name_floor.get(token)
            if known is not None and stem > known:
                self._record_name_floor[token] = stem

    def _record_name_floor_forget(self, token: str) -> None:
        """Drop one stream's name floor after its directory is removed.

        Same growth story as :meth:`_prune_countdown_forget`; a returning
        name re-seeds from its directory listing on its next append.
        """
        with self._record_name_floor_lock:
            self._record_name_floor.pop(token, None)

    def _quarantine(self, path: str, name: str, reason: str) -> None:
        dest = os.path.join(
            self.base,
            QUARANTINE_DIR,
            "{}.{}.bad".format(name, self._instance),
        )
        try:
            self._replace(path, dest)
            logger.warning(
                "state: quarantined corrupt record %s (%s)", name, reason
            )
            # stamp the QUARANTINE time: rename preserves the original write
            # mtime, and the GC quarantine sweep ages by mtime; without
            # this, an old-written poison record could be swept in the same
            # pass it was quarantined, losing the forensics window.
            with contextlib.suppress(OSError):
                os.utime(dest, None)
        except OSError:
            # already moved/removed by another pass or node, or unwritable:
            # never let cleanup of a poison record raise into a read.
            pass

    def _record_cache_get(self, path: str) -> Optional[bytes]:
        with self._record_cache_lock:
            raw = self._record_cache.get(path)
            if raw is not None:
                # move to the fresh end: a dict iterates in insertion order,
                # which is the LRU order the eviction below evicts from.
                del self._record_cache[path]
                self._record_cache[path] = raw
            return raw

    def _record_cache_put(self, path: str, raw: bytes) -> None:
        if len(raw) > _RECORD_CACHE_MAX_ITEM_BYTES:
            return
        with self._record_cache_lock:
            if path in self._record_cache:
                return  # another worker read the same record concurrently
            self._record_cache[path] = raw
            self._record_cache_bytes += len(raw)
            while (
                len(self._record_cache) > _RECORD_CACHE_MAX_ENTRIES
                or self._record_cache_bytes > _RECORD_CACHE_MAX_BYTES
            ):
                oldest = next(iter(self._record_cache))
                self._record_cache_bytes -= len(self._record_cache.pop(oldest))

    def _read_record(
        self, stream_dir: str, name: str, *, strict: bool = False
    ) -> Optional[dict[str, Any]]:
        """One record's ``data`` body, or ``None`` if it is not readable.

        Best-effort reads (``strict=False``) are served from an in-memory
        byte cache, sound because a record file is written once to a
        forever-unique name and thereafter only read or deleted: ``path ->
        bytes`` can never go stale.  Only a body that read back VALID is
        cached (never a miss, error, quarantine or unrecognised
        ``schemaVersion``), which also keeps ``migrate_schema``'s in-place
        rewrite (the one sanctioned exception to
        records-are-never-rewritten) invisible to it.  Each read parses
        its own body, so callers get a private dict to mutate.

        A ``strict`` read never touches the cache in either direction: a
        record it cannot read RIGHT NOW must fail the caller closed, so it
        always goes to the store and never substitutes a remembered body.
        """
        # Byte-identical to os.path.join, without its fspath and separator
        # checks, on a path built once per record read: every stream_dir
        # comes from _stream_dir (a root joined to a non-empty _fs_safe
        # token, so it never ends in a separator) and every name from a
        # listing of it.
        path = stream_dir + os.sep + name
        raw = None if strict else self._record_cache_get(path)
        cached = raw is not None
        if raw is None:
            try:
                with open(path, "rb") as fobj:
                    raw = fobj.read()
            except FileNotFoundError:
                # raced away (pruned/quarantined) between listdir and open.
                return None
            except OSError as ex:
                self._log_unreadable_record(name, ex)
                if strict:
                    # A derived-watermark read MUST fail closed on an
                    # environmental error: silently dropping this record
                    # could yield a max strictly BELOW the true one and
                    # replay an occurrence that already ran.  Propagate so
                    # the caller treats the watermark as UNKNOWN.  (A
                    # content-bad record is still skipped even here:
                    # failing closed on it forever would wedge the
                    # watermark.)
                    raise
                return None
        try:
            obj = _json.loads(raw)
        except Exception:  # noqa: BLE001 - any content-driven parse failure
            # Bad CONTENT (invalid/truncated JSON, hostile deep nesting).
            # Must catch everything content-dependent: a poison record
            # raising out of here would escape quarantine and crash the
            # reader ("never fatal" is the invariant).
            self._quarantine(path, name, "unreadable-or-invalid-json")
            return None
        if not isinstance(obj, dict) or not isinstance(obj.get("data"), dict):
            # the SHAPE is wrong regardless of schemaVersion: not a genuine
            # record this or any version of this code ever wrote.
            # Unrecoverable, so quarantine.
            self._quarantine(path, name, "unknown-schema")
            return None
        if obj.get("schemaVersion") != SCHEMA_VERSION:
            # Well-formed but unrecognised schemaVersion: almost always a
            # NEWER version from a peer mid-rolling-upgrade, not
            # corruption.  Quarantining would let an old node erase a new
            # node's records fleet-wide; leave it in place, like the
            # environmental-error branch above.
            logger.warning(
                "state: record %s has unrecognised schemaVersion %r; "
                "leaving it in place (likely written by a newer version)",
                name,
                obj.get("schemaVersion"),
            )
            if strict:
                # Mirrors the environmental-error branch: a watermark read
                # must fail closed rather than compute a max over only the
                # subset it understood.
                raise _DocumentUnreadable(
                    "record {} has unrecognised schemaVersion {!r}".format(
                        name, obj.get("schemaVersion")
                    )
                )
            return None
        if not cached and not strict:
            self._record_cache_put(path, raw)
        data: dict[str, Any] = obj["data"]
        return data

    @staticmethod
    def _log_unreadable_record(name: str, ex: OSError) -> None:
        """Report a record the ENVIRONMENT would not hand over just now.

        An I/O error is the environment failing, not the record: skipped
        for this read but left in place.  Quarantining here would eject
        valid history on every store hiccup.
        """
        hint = ""
        if isinstance(ex, PermissionError):
            # persistent EACCES on the deliberately-0o600 data files
            # usually means two nodes run as DIFFERENT users against one
            # shared store, silently hiding half the history.
            hint = (
                " (records are created 0o600: every node sharing this "
                "store must run as the same user)"
            )
        logger.warning(
            "state: cannot read record %s (%s); leaving it in place%s",
            name,
            ex,
            hint,
        )

    async def list_records(
        self,
        stream: str,
        *,
        limit: Optional[int] = None,
        newest_first: bool = False,
        strict: bool = False,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
        max_matches: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return await self._call(
            "list",
            self._list_sync,
            stream,
            limit,
            newest_first,
            strict,
            predicate,
            max_matches,
        )

    async def list_stream_names(self, prefix: str) -> list[str]:
        return await self._call(
            "list-stream-names", self._list_stream_names_sync, prefix
        )

    async def list_stream_names_audit(
        self, prefix: str
    ) -> tuple[list[str], bool]:
        return await self._call(
            "list-stream-names", self._list_stream_names_audit_sync, prefix
        )

    def _read_stream_name_sidecar(
        self, stream_dir: str, token: str
    ) -> Optional[str]:
        """The logical stream name recorded inside a truncated stream dir.

        ``None`` when the sidecar is absent, unreadable, or fails the
        round-trip check: ``_fs_safe(name)`` must reproduce ``token``
        exactly, or the name is corrupt/foreign and handing it to a keep-set
        builder would protect the WRONG token while this one gets collected.
        """
        path = os.path.join(stream_dir, _STREAM_NAME_SIDECAR)
        try:
            with open(path, "rb") as fobj:
                # surrogatepass mirrors _fs_safe's encode, so a name carrying
                # a lone surrogate round-trips instead of being dropped here.
                name = fobj.read().decode("utf-8", "surrogatepass")
        except (OSError, UnicodeDecodeError):
            return None
        if not name or _fs_safe(name) != token:
            return None
        return name

    def _ensure_stream_name_sidecar(
        self, stream_dir: str, token: str, stream: str
    ) -> None:
        """Durably record a truncated stream's exact logical name.

        Without the sidecar a truncated token cannot round-trip through
        enumeration and the GC keep-set would miss this stream entirely.
        Best-effort: a failed sidecar write must never fail the append it
        rides on (the stream is merely skipped by enumeration until a
        later append lands it).
        """
        if self._read_stream_name_sidecar(stream_dir, token) == stream:
            return
        with contextlib.suppress(OSError):
            self._atomic_write(
                os.path.join(stream_dir, _STREAM_NAME_SIDECAR),
                stream.encode("utf-8", "surrogatepass"),
            )

    def _list_stream_names_sync(self, prefix: str) -> list[str]:
        return self._list_stream_names_audit_sync(prefix)[0]

    def _list_stream_names_audit_sync(
        self, prefix: str
    ) -> tuple[list[str], bool]:
        records_root = self._records_root
        token_prefix = _fs_safe_fragment(prefix)
        try:
            # scandir: the is-a-directory test below rides on the entry's
            # own d_type, so no stat per token.
            with os.scandir(records_root) as listing:
                entries = list(listing)
        except FileNotFoundError:
            # no store written yet: exhaustively empty, not unreadable.
            return [], True
        except OSError:
            return [], False
        names: list[str] = []
        complete = True
        for entry in entries:
            token = entry.name
            if not token.startswith(token_prefix):
                continue
            if not entry.is_dir():
                continue
            stream_dir = entry.path
            if _FS_TRUNCATION_MARKER in token:
                # only the sidecar knows a truncated token's logical name.
                # A stream without a verifiable sidecar is SKIPPED, never
                # returned garbled (a garbled name re-encodes to a
                # different token and the stream's state gets collected as
                # garbage); the skip is reported through ``complete`` so a
                # deleting caller keeps instead.
                name = self._read_stream_name_sidecar(stream_dir, token)
                if name is not None:
                    names.append(name)
                else:
                    complete = False
                continue
            name = _decode_fs_token(token)
            if name is None:
                # not a token _fs_safe produced: same unnameable-entry
                # discipline as above (a garbled name re-encodes to a
                # DIFFERENT token, and a sweep built from it destroys live
                # state).
                complete = False
                continue
            names.append(name)
        return sorted(names), complete

    def _list_sync(
        self,
        stream: str,
        limit: Optional[int],
        newest_first: bool,
        strict: bool = False,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
        max_matches: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        stream_dir = self._stream_dir(stream)
        try:
            # one descending sort: the names are distinct filenames, so
            # the stable sort's reverse= order is exactly the reversed
            # ascending one.
            names = sorted(
                [n for n in os.listdir(stream_dir) if n.endswith(".json")],
                reverse=newest_first,
            )
        except FileNotFoundError:
            return []
        out: list[dict[str, Any]] = []
        # `limit` bounds the WINDOW of readable records considered;
        # `predicate` filters which of that window is returned;
        # `max_matches` stops the scan once enough matches are in hand.
        considered = 0
        for name in names:
            if limit is not None and considered >= limit:
                break
            data = self._read_record(stream_dir, name, strict=strict)
            if data is None:
                continue
            considered += 1
            if predicate is not None and not predicate(data):
                continue
            out.append(data)
            if max_matches is not None and len(out) >= max_matches:
                break
        return out

    async def derive_max(self, stream: str, field: str) -> Optional[Any]:
        return await self._call(
            "derive-max", self._derive_max_sync, stream, field
        )

    def _derive_max_invalidate(self, token: str) -> None:
        """Drop the derive_max memo for one stream after a wholesale wipe.

        The memo's own staleness check cannot see a wipe followed by new
        appends before the next derive (it looks like an ordinary prune),
        and the cached best would leak deleted records' values into the
        recreated stream's cursor.  Bumping the wipe generation also
        fences a derive already in flight: its memo write-back is gated on
        the generation it observed before scanning.
        """
        with self._derive_memo_lock:
            self._derive_wipe_gen[token] = (
                self._derive_wipe_gen.get(token, 0) + 1
            )
            for key in [k for k in self._derive_memo if k[0] == token]:
                del self._derive_memo[key]

    def _derive_max_sync(self, stream: str, field: str) -> Optional[Any]:
        token = _fs_safe(stream)
        stream_dir = os.path.join(self._records_root, token)  # _stream_dir
        memo_key = (token, field)
        try:
            # Unsorted: the anchor is max(listing) and the scan set is
            # watermark-filtered, both order-independent.  The fold's
            # tie-break needs a deterministic order, so `to_scan` alone is
            # sorted below.
            listing = [
                n for n in os.listdir(stream_dir) if n.endswith(".json")
            ]
        except FileNotFoundError:
            listing = []
        if not listing:
            # No records at all: any cached fold describes records that no
            # longer exist, so it must go.
            with self._derive_memo_lock:
                self._derive_memo.pop(memo_key, None)
            return None
        with self._derive_memo_lock:
            gen = self._derive_wipe_gen.get(token, 0)
            cached = self._derive_memo.get(memo_key)
        best: Optional[Any] = None
        to_scan = listing
        # The fold's anchor, and the one comparison the warm path needs:
        # "the watermark survives or something newer exists" is exactly
        # "the newest name is at least the watermark".
        newest = max(listing)
        if cached is not None:
            watermark, best = cached
            if newest == watermark:
                # unchanged stream: nothing to parse.
                to_scan = []
            elif newest > watermark:
                # Incremental fold, correct because names are floor-clamped
                # (appends only create names above the watermark, even
                # across a backward clock step) and a bounded prune only
                # deletes the OLDEST records (deleting an already-folded
                # value cannot lower a monotonic maximum).  The watermark
                # itself may have been pruned once newer records landed,
                # so a surviving newer name is as good as the watermark
                # surviving.
                to_scan = [n for n in listing if n > watermark]
            else:
                # Watermark gone AND nothing newer: not a prune (a bounded
                # prune leaves the newest record standing), the stream was
                # deleted and recreated underneath us.  Rescan from
                # scratch.
                best = None
        # strict=True: an environmental read error must PROPAGATE, never
        # silently shrink the max (see _read_record); a raise also skips
        # the memo write-back, so a half-folded scan is never cached.
        # Fold in filename-sorted order so the incomparable-types
        # tie-break (keep first-seen) is deterministic regardless of
        # listdir order.
        for name in sorted(to_scan):
            data = self._read_record(stream_dir, name, strict=True)
            if data is None or field not in data:
                continue
            value = data[field]
            if best is None:
                best = value
                continue
            try:
                if value > best:
                    best = value
            except TypeError:
                # incomparable types in one stream (a caller bug); keep the
                # first-seen rather than raising out of a cursor read.
                continue
        with self._derive_memo_lock:
            if self._derive_wipe_gen.get(token, 0) == gen:
                # unchanged generation: no wipe raced this scan; anchor
                # the fold to the newest filename (listing is non-empty).
                self._derive_memo[memo_key] = (newest, best)
        return best

    async def prune_records(self, stream: str, *, keep: int) -> int:
        return await self._call("prune", self._prune_sync, stream, keep)

    def _prune_sync(self, stream: str, keep: int) -> int:
        token = _fs_safe(stream)
        stream_dir = os.path.join(self._records_root, token)  # _stream_dir
        try:
            names = sorted(
                [n for n in os.listdir(stream_dir) if n.endswith(".json")]
            )
        except FileNotFoundError:
            return 0
        # Fold the listing's newest canonical stem into the name floor, so
        # names a PEER process appended raise this process's generation
        # floor at every amortised prune, not only at the one-time seed.
        for name in reversed(names):
            stem = name[: -len(".json")]
            if _record_name_epoch_str(stem) is not None:
                self._record_name_floor_raise(token, stem)
                break
        # names sort chronologically (write-epoch filename prefix); keep the
        # newest ``keep`` (the tail), delete the rest.  keep <= 0 -> all.
        to_delete = names if keep <= 0 else names[:-keep]
        deleted = 0
        for name in to_delete:
            try:
                os.unlink(os.path.join(stream_dir, name))
                deleted += 1
            except OSError:
                # already gone (raced with another prune/node): ignore.
                pass
        if keep <= 0:
            # A wholesale wipe: the derive_max memo must not survive it,
            # see _derive_max_invalidate.
            self._derive_max_invalidate(token)
        return deleted

    def _prune_latest_by_sync(self, stream: str, field: str) -> int:
        """Keep only the newest record per distinct value of ``field``.

        The name-keyed prune for the artifact store: only the newest
        record per name is ever read back, so older records for a
        superseded name are dead weight that pin now-orphan blobs.  Never
        deletes the newest record of any name, so no live value is lost.
        A record that cannot be read right now is LEFT IN PLACE and does
        not count as having seen its value: it could be the live newest of
        a name.  Best-effort like :meth:`_prune_sync`.
        """
        stream_dir = self._stream_dir(stream)
        try:
            # newest first: the first record kept per value wins (distinct
            # filenames, so the descending sort is the reversed ascending)
            names = sorted(
                [n for n in os.listdir(stream_dir) if n.endswith(".json")],
                reverse=True,
            )
        except FileNotFoundError:
            return 0
        seen: set = set()
        deleted = 0
        for name in names:
            data = self._read_record(stream_dir, name)
            if data is None:
                continue  # unreadable/raced: never supersede on its account
            value = data.get(field)
            if not isinstance(value, str):
                continue  # unclassifiable: keep it, cannot judge supersession
            if value in seen:
                try:
                    os.unlink(os.path.join(stream_dir, name))
                    deleted += 1
                except OSError:
                    # already gone (raced with another prune/node): ignore.
                    pass
            else:
                seen.add(value)
        return deleted

    # --- lease -----------------------------------------------------------

    @contextlib.contextmanager
    def _locked(
        self,
        lock_path: str,
        *,
        touch: bool = False,
        blocking: bool = True,
    ) -> Iterator[None]:
        """Hold the advisory exclusive lock on ``lock_path`` for the block.

        The lock file is separate from the data file on purpose: the data
        file is replaced by an atomic rename, which would swap the inode
        out from under a lock taken on it.

        Lock files are also RECLAIMED by GC (see
        :meth:`_gc_orphan_locks_sync`), so a waiter can win the flock on
        an inode unlinked while it waited.  After acquiring, re-verify the
        path still names the locked inode and re-open if not; without this
        the mutual exclusion silently splits across two inodes.

        ``touch`` (the document lane only) refreshes the lock file's mtime
        after acquiring: flock never updates mtime, and the orphan-lock
        sweep uses mtime as the activity signal.

        ``blocking=False`` is :meth:`_try_locked`'s lane: contention raises
        ``OSError`` at once, with no wait, and the wait-time stats are left
        alone (a try-lock never waits, so counting it dilutes the
        contention signal).
        """
        self._makedirs_durable(os.path.dirname(lock_path))
        began = time.perf_counter()
        while True:
            fdesc = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                # msvcrt.locking needs a byte present to lock; guarantee one.
                # The same fstat fixes this descriptor's inode identity for
                # the re-verify after acquiring: an open descriptor's inode
                # never changes, so a second fstat there repeats it.
                ours = os.fstat(fdesc)
                if ours.st_size == 0:
                    try:
                        os.write(fdesc, b"\0")
                    except PermissionError:
                        # Windows: a rival won the bootstrap and holds the
                        # byte-range lock over the byte it wrote.  The
                        # byte exists now; fall through and contend
                        # normally (a non-blocking attempt reports that
                        # contention as ``OSError``).
                        pass
                with exclusive_file_lock(fdesc, blocking=blocking):
                    try:
                        same = os.path.samestat(ours, os.stat(lock_path))
                    except OSError:
                        same = False
                    if not same:
                        continue  # ghost inode: re-open and contend afresh
                    if touch:
                        # best-effort: a missed touch only ages the lock
                        # towards the sweep, which still requires the
                        # document ABSENT and runs under this same flock.
                        with contextlib.suppress(OSError):
                            if os.utime in os.supports_fd:
                                os.utime(fdesc)
                            else:
                                os.utime(lock_path)
                    if blocking:
                        # time-to-acquire is the contention signal: near
                        # zero on an idle store, and the cross-host wait on
                        # a fought-over lease.
                        waited = time.perf_counter() - began
                        with self._stats_lock:
                            self._lock_acquisitions += 1
                            self._lock_wait_seconds += waited
                    yield
                    return
            finally:
                os.close(fdesc)

    def _try_locked(
        self, lock_path: str
    ) -> "contextlib.AbstractContextManager[None]":
        """Non-blocking :meth:`_locked`: contention raises ``OSError``.

        The GC sweep's lane: a peer wedged mid-claim can hold a document
        flock indefinitely, and a blocking acquire would park the whole
        ``gc()`` call behind one entry.  A sweep, unlike a mutator, can
        always skip and let a later pass retry.  Same ghost-inode
        re-verify as :meth:`_locked`; no wait-time stats (a try-lock never
        waits).
        """
        return self._locked(lock_path, blocking=False)

    def _read_lease_file(
        self, lease_path: str, *, strict: bool = False
    ) -> Optional[Lease]:
        """Read a lease file; ``None`` means *positively absent*.

        With ``strict`` (the locked RMW paths), anything short of plain
        absence raises :class:`_LeaseUnreadable`: conflating "unreadable"
        with "no lease" would let one NFS blip steal a valid lease from
        its live holder.  The unlocked observer stays best-effort.
        """
        try:
            with open(lease_path, "rb") as fobj:
                obj = _json.loads(fobj.read())
        except FileNotFoundError:
            return None
        except Exception as ex:  # noqa: BLE001 - classified below
            if strict:
                raise _LeaseUnreadable(str(ex)) from ex
            return None
        try:
            if not isinstance(obj, dict):
                raise TypeError("lease file is not a JSON object")
            return Lease(
                name=str(obj["name"]),
                holder=str(obj["holder"]),
                fence=int(obj["fence"]),
                expires_at=float(obj["expiresAt"]),
            )
        except (KeyError, TypeError, ValueError) as ex:
            # corrupt content: _write_lease_file is atomic so this should not
            # happen; failing closed (deny) beats guessing at a fence.
            if strict:
                raise _LeaseUnreadable(str(ex)) from ex
            return None

    def _write_lease_file(
        self, lease_path: str, lease: Lease, *, durable: bool = True
    ) -> None:
        # trusted: a Lease is built entirely from this process's own
        # strings, ints and clock reads, never job or store data, so the
        # recursive portability pre-walk is skipped.
        #
        # ``durable=False`` is passed by exactly the writes that KEEP the
        # fence (renew, release, same-holder still-valid acquire).  Those
        # only move ``expiresAt``, and a crash that loses the rename
        # merely restores an EARLIER expiry: the lease expires sooner and
        # a takeover bumps the fence through the durable path.  Every
        # fence-CHANGING write keeps the full barrier: acknowledging a
        # fence and then losing it would re-issue the same fence and
        # defeat stale-writer detection.
        payload = _json.dumps_bytes(
            lease.to_dict(), sort_keys=True, trusted=True
        )
        self._atomic_write(lease_path, payload, durable_rename=durable)

    async def acquire_lease(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        return await self._call(
            "lease-acquire", self._acquire_sync, name, holder, ttl
        )

    def _acquire_sync(
        self, name: str, holder: str, ttl: float
    ) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable as ex:
                # fail CLOSED: an unreadable lease is not a free lease.
                logger.warning(
                    "state: lease %s unreadable (%s); denying acquire",
                    name,
                    ex,
                )
                return None
            now = _now()
            if (
                current is not None
                and current.holder != holder
                and current.expires_at > now
            ):
                # validly held by someone else: deny.
                return None
            if current is None:
                fence = 1
            elif current.holder == holder and current.expires_at > now:
                # a renew of our own still-valid lease keeps the fence.
                fence = current.fence
            else:
                # taking over an EXPIRED (or released) lease, even our own:
                # bump the fence so any late writes issued under the previous
                # incarnation can be fenced off.  Monotonic because release
                # marks the lease expired in place instead of deleting it.
                fence = current.fence + 1
            lease = Lease(
                name=name,
                holder=holder,
                fence=fence,
                expires_at=now + ttl,
            )
            try:
                # Durable exactly when the fence CHANGES (first issue or
                # takeover bump); the same-holder still-valid arm above is a
                # renew in acquire clothing and gets the renew's cheap write
                # (see _write_lease_file).
                self._write_lease_file(
                    lease_path,
                    lease,
                    durable=current is None or fence != current.fence,
                )
            except OSError as ex:
                # a write that cannot land (Windows sharing violation past
                # the retries, a read-only blip) means we did NOT acquire:
                # deny, never raise out of the lease API.
                logger.warning(
                    "state: lease %s write failed (%s); denying acquire",
                    name,
                    ex,
                )
                return None
            return lease

    async def renew_lease(self, lease: Lease, ttl: float) -> Optional[Lease]:
        return await self._call("lease-renew", self._renew_sync, lease, ttl)

    def _renew_sync(self, lease: Lease, ttl: float) -> Optional[Lease]:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable as ex:
                # fail closed: without a trustworthy read we cannot prove we
                # still hold it.  Losing a renew is safe; renewing a lease
                # someone else took over is not.
                logger.warning(
                    "state: lease %s unreadable (%s); denying renew",
                    lease.name,
                    ex,
                )
                return None
            # Renew only if we still hold it: same holder AND same fence.
            # Allowed even a hair past expiry, but NOT past a release: a
            # released lease keeps holder+fence with expiry 0.0, and a
            # renew landing after our own release (a renew loop racing
            # shutdown) must not silently resurrect it.
            if (
                current is None
                or current.holder != lease.holder
                or current.fence != lease.fence
                or current.expires_at <= 0.0
            ):
                return None
            renewed = Lease(
                name=lease.name,
                holder=lease.holder,
                fence=lease.fence,
                expires_at=_now() + ttl,
            )
            try:
                # fence unchanged by construction (the guard above requires
                # current.fence == lease.fence): the cheap write applies.
                self._write_lease_file(lease_path, renewed, durable=False)
            except OSError as ex:
                # cannot persist the extension: the holder must treat the
                # renew as failed (fail closed), not crash on it.
                logger.warning(
                    "state: lease %s write failed (%s); denying renew",
                    lease.name,
                    ex,
                )
                return None
            return renewed

    async def release_lease(self, lease: Lease) -> None:
        await self._call("lease-release", self._release_sync, lease)

    def _release_sync(self, lease: Lease) -> None:
        lock_path, lease_path = self._lease_paths(lease.name)
        with self._locked(lock_path):
            try:
                current = self._read_lease_file(lease_path, strict=True)
            except _LeaseUnreadable:
                # cannot prove ownership: leave it to expire by TTL.
                return
            if (
                current is not None
                and current.holder == lease.holder
                and current.fence == lease.fence
            ):
                # Mark expired IN PLACE rather than unlinking: the lease
                # file is the fence counter's only home, and deleting it
                # would re-issue fence values already handed out and
                # defeat stale-writer detection.  Cheap write: a release
                # lost to a crash just leaves the lease to expire by TTL.
                with contextlib.suppress(OSError):
                    self._write_lease_file(
                        lease_path,
                        Lease(
                            name=lease.name,
                            holder=lease.holder,
                            fence=lease.fence,
                            expires_at=0.0,
                        ),
                        durable=False,
                    )

    async def read_lease(self, name: str) -> Optional[Lease]:
        _lock_path, lease_path = self._lease_paths(name)
        lease = await self._call(
            "lease-read", self._read_lease_file, lease_path
        )
        if lease is not None and lease.expires_at <= 0.0:
            # a released lease: observers see "nobody holds it".
            return None
        return lease

    # --- mutable documents -----------------------------------------------

    def _read_doc_file(
        self, doc_path: str, *, strict: bool = False
    ) -> Optional[dict[str, Any]]:
        """Read a document body; ``None`` means *positively absent*.

        Mirrors :meth:`_read_lease_file`.  With ``strict`` (the locked
        RMW inside :meth:`mutate_document`), anything short of plain
        absence raises :class:`_DocumentUnreadable` so the mutation fails
        closed rather than clobbering a live value or reading a torn one.
        Without ``strict`` it returns ``None`` for every one of those.
        """
        try:
            with open(doc_path, "rb") as fobj:
                obj = _json.loads(fobj.read())
        except FileNotFoundError:
            return None
        except Exception as ex:  # noqa: BLE001 - classified below
            if strict:
                raise _DocumentUnreadable(str(ex)) from ex
            return None
        if (
            not isinstance(obj, dict)
            or obj.get("schemaVersion") != SCHEMA_VERSION
            or not isinstance(obj.get("data"), dict)
        ):
            if strict:
                raise _DocumentUnreadable("unknown-schema-or-not-a-document")
            return None
        data: dict[str, Any] = obj["data"]
        return data

    async def read_document(
        self, namespace: str, key: str
    ) -> Optional[dict[str, Any]]:
        return await self._call(
            "doc-read", self._read_document_sync, namespace, key
        )

    def _read_document_sync(
        self, namespace: str, key: str
    ) -> Optional[dict[str, Any]]:
        _lock_path, doc_path = self._doc_paths(namespace, key)
        return self._read_doc_file(doc_path)

    async def mutate_document(
        self,
        namespace: str,
        key: str,
        transform: Callable[[Optional[dict[str, Any]]], tuple[Any, _T]],
    ) -> tuple[Optional[dict[str, Any]], _T]:
        return await self._call(
            "doc-mutate", self._mutate_document_sync, namespace, key, transform
        )

    def _mutate_document_sync(
        self,
        namespace: str,
        key: str,
        transform: Callable[[Optional[dict[str, Any]]], tuple[Any, _T]],
    ) -> tuple[Optional[dict[str, Any]], _T]:
        lock_path, doc_path = self._doc_paths(namespace, key)
        # _locked creates the namespace dir before opening the lock file,
        # so the very first write to a fresh namespace has somewhere to
        # land.  ``touch``: every mutate refreshes the lock file's mtime,
        # the idle clock the GC orphan-lock sweep judges a doc ``.lock`` by.
        with self._locked(lock_path, touch=True):
            current = self._read_doc_file(doc_path, strict=True)
            new_body, result = transform(current)
            if new_body is DOC_KEEP:
                return current, result
            if new_body is DOC_DELETE:
                with contextlib.suppress(FileNotFoundError):
                    self._unlink(doc_path)
                    # without this a deleted key can RESURRECT after a
                    # power loss, silently un-doing the delete and
                    # letting guarded once-only work run again.
                    fsync_directory(os.path.dirname(doc_path))
                # the ``.lock`` side-file is deliberately NOT unlinked:
                # on NFS/EFS a waiter's post-acquire stat re-verify can
                # be answered from a stale cache and split the document
                # mutex across nodes.  Orphaned doc locks are reclaimed
                # by :meth:`_gc_orphan_locks_sync` once idle past grace.
                return None, result
            if not isinstance(new_body, dict):
                raise TypeError(
                    "mutate_document transform must return a dict body, "
                    "DOC_KEEP or DOC_DELETE"
                )
            payload = _json.dumps_bytes(
                {"schemaVersion": SCHEMA_VERSION, "data": new_body},
                sort_keys=True,
            )
            self._atomic_write(doc_path, payload)
            return new_body, result

    async def delete_document(self, namespace: str, key: str) -> bool:
        def _delete(current: Optional[dict[str, Any]]) -> tuple[Any, bool]:
            return DOC_DELETE, current is not None

        _stored, existed = await self.mutate_document(namespace, key, _delete)
        return existed

    async def list_documents(self, namespace: str) -> list[dict[str, Any]]:
        return await self._call(
            "doc-list", self._list_documents_sync, namespace
        )

    async def list_document_keys(self, namespace: str) -> Optional[list[str]]:
        return await self._call(
            "doc-list", self._list_document_keys_sync, namespace
        )

    def _list_document_keys_sync(self, namespace: str) -> Optional[list[str]]:
        ns_dir = self._doc_dir(namespace)
        try:
            names = os.listdir(ns_dir)
        except FileNotFoundError:
            return []  # no document ever written: exhaustively empty
        except OSError:
            return None  # unreadable right now: caller takes the full path
        keys: list[str] = []
        for name in names:
            if not name.endswith(".doc"):
                continue
            token = name[: -len(".doc")]
            if _FS_TRUNCATION_MARKER in token:
                # a truncated key cannot round-trip (documents have no
                # name sidecar): the WHOLE listing reports unable, else
                # this key would be invisible to a keys-driven scan.
                return None
            key = _decode_fs_token(token)
            if key is None:
                # not a token our encoder produced: fall back rather than
                # hand back a garbled key addressing a different or
                # nonexistent document (same round-trip discipline as the
                # stream and namespace listings).
                return None
            keys.append(key)
        keys.sort()
        return keys

    async def list_document_namespaces(
        self, prefix: str
    ) -> tuple[list[str], bool]:
        return await self._call(
            "doc-list", self._list_document_namespaces_sync, prefix
        )

    def _list_document_namespaces_sync(
        self, prefix: str
    ) -> tuple[list[str], bool]:
        docs_root = self._docs_root
        token_prefix = _fs_safe_fragment(prefix)
        try:
            with os.scandir(docs_root) as listing:
                entries = list(listing)
        except FileNotFoundError:
            # no document ever written: exhaustively empty, not unreadable.
            return [], True
        except OSError:
            return [], False
        names: list[str] = []
        complete = True
        for entry in entries:
            token = entry.name
            if not token.startswith(token_prefix):
                continue
            if not entry.is_dir():
                continue
            if _FS_TRUNCATION_MARKER in token:
                # truncated namespace tokens have no name sidecar: report
                # the listing incomplete rather than hand the GC a
                # garbled name (it would collect the XCom streams this
                # namespace's run documents still anchor).
                complete = False
                continue
            name = _decode_fs_token(token)
            if name is None:
                # foreign/corrupt token: same unnameable discipline as
                # the truncated case above.
                complete = False
                continue
            names.append(name)
        return sorted(names), complete

    def _list_documents_sync(self, namespace: str) -> list[dict[str, Any]]:
        ns_dir = self._doc_dir(namespace)
        try:
            names = sorted(
                [n for n in os.listdir(ns_dir) if n.endswith(".doc")]
            )
        except FileNotFoundError:
            return []
        out: list[dict[str, Any]] = []
        for name in names:
            data = self._read_doc_file(os.path.join(ns_dir, name))
            if data is not None:
                out.append(data)
        return out

    # --- content-addressed blobs -----------------------------------------

    async def put_blob(self, data: bytes) -> str:
        return await self._call("blob-put", self._put_blob_sync, data)

    def _put_blob_sync(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        path = self._blob_path(digest)
        # content-addressed: an existing blob already holds this payload,
        # so skip the rewrite but refresh its mtime, the orphan-blob
        # sweep's age guard: the new record has not landed yet, and a
        # concurrent sweep must read this blob as too-young, not as an
        # aged orphan.
        if os.path.exists(path):
            with contextlib.suppress(OSError):
                os.utime(path)
            return digest
        self._makedirs_durable(os.path.dirname(path))
        # _atomic_write renames over any existing file; a concurrent writer of
        # the same content is therefore harmless (identical bytes either way).
        self._atomic_write(path, data)
        return digest

    async def get_blob(self, digest: str) -> Optional[bytes]:
        return await self._call("blob-get", self._get_blob_sync, digest)

    def _get_blob_sync(self, digest: str) -> Optional[bytes]:
        try:
            with open(self._blob_path(digest), "rb") as fobj:
                return fobj.read()
        except FileNotFoundError:
            return None
        except OSError as ex:
            # a transient read error is the environment, not a missing blob:
            # surface it (the awaiter can retry) rather than reporting absence.
            logger.warning("state: cannot read blob %s (%s)", digest, ex)
            raise

    # --- lock-fidelity probe -------------------------------------------------

    async def verify_locking(self) -> Optional[str]:
        """Probe whether the store's advisory locks actually exclude.

        ``None`` when the locks behave, else a human-readable reason they
        must not be trusted for coordination.  Two checks:

        * a functional probe: lock a scratch file through one descriptor,
          then attempt a non-blocking exclusive lock through a second
          descriptor of the same file.  A real implementation refuses the
          second; a mount whose locks are silent no-ops (some FUSE
          filesystems) grants it.
        * a mount-option sniff (Linux): an NFS mount carrying ``nolock``
          or ``local_lock=flock``/``all`` satisfies flock host-locally, so
          the functional probe passes on every node while no lock ever
          reaches the server.

        Both checks run on one host, so locks real locally but not
        propagated across hosts are undetectable on platforms without
        ``/proc/mounts``; that residual rests on the operator's
        ``topology`` assertion.  A probe that cannot run is inconclusive
        and reports ``None`` rather than refusing a healthy store on a
        blip.
        """
        return await self._call("lock-probe", self._verify_locking_sync)

    def _verify_locking_sync(self) -> Optional[str]:
        reason = _local_lock_reason(self.root)
        if reason is not None:
            return (
                "{}, so file locks are host-local and cannot fence "
                "other nodes".format(reason)
            )
        probe = os.path.join(
            self.base, TMP_DIR, "lock-probe-{}".format(self._instance)
        )
        fd1 = fd2 = -1
        try:
            fd1 = os.open(probe, os.O_RDWR | os.O_CREAT, 0o600)
            # msvcrt.locking needs a byte present to lock; guarantee one.
            if os.fstat(fd1).st_size == 0:
                os.write(fd1, b"\0")
            fd2 = os.open(probe, os.O_RDWR)
            with exclusive_file_lock(fd1, blocking=False):
                try:
                    with exclusive_file_lock(fd2, blocking=False):
                        return (
                            "the mount at {} grants two exclusive locks on "
                            "one file (its locks are no-ops)".format(self.root)
                        )
                except OSError:
                    # contention: the second descriptor was refused while
                    # the first held the lock; locks genuinely exclude.
                    return None
        except OSError as ex:
            logger.debug("state: lock-fidelity probe inconclusive: %s", ex)
            return None
        finally:
            for fdesc in (fd1, fd2):
                if fdesc != -1:
                    with contextlib.suppress(OSError):
                        os.close(fdesc)
            with contextlib.suppress(OSError):
                os.unlink(probe)

    # --- maintenance -------------------------------------------------------

    async def collect_garbage(
        self,
        *,
        keep: dict[str, set[str]],
        grace: float,
        ephemeral_lease_prefixes: tuple[str, ...] = (),
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Remove durable state nothing references anymore.

        ``keep`` maps a managed stream prefix (``"runs/"``, ``"logs/"``,
        ...) to the set of suffixes (job names, hosts) that must survive;
        the caller derives it from recent manifests plus its own loaded
        config.  A stream is deleted only when it POSITIVELY matches a
        managed prefix, its suffix is not kept, AND its newest record is
        older than ``grace`` seconds.  Anything unclassifiable is kept,
        including a truncated stream directory without a verifiable name
        sidecar; :data:`PROTECTED_STREAMS` are never touched.

        Lease files: only the EPHEMERAL per-run classes named by
        ``ephemeral_lease_prefixes`` are ever reclaimed, and only when
        PROVABLY dead for the whole grace window.  Every other lease file
        is never deleted, whatever its age: a lease file is its fence
        counter's only home, and fence values are PERSISTED beyond it (a
        Replace-cancel record in ``slots/<job>``), so a fence reset after
        ANY grace window can re-collide and silently cancel a healthy
        future run (see :meth:`_gc_leases_sync`).

        Also sweeps orphaned ``.lock`` side-files idle past grace
        (:meth:`_gc_orphan_locks_sync`), write-temp files older than
        :data:`TMP_MAX_AGE`, and quarantined records older than ``grace``.
        """
        return await self._call(
            "gc",
            self._gc_sync,
            keep,
            grace,
            ephemeral_lease_prefixes,
            dry_run,
        )

    def _gc_sync(
        self,
        keep: dict[str, set[str]],
        grace: float,
        ephemeral_lease_prefixes: tuple[str, ...],
        dry_run: bool,
    ) -> dict[str, Any]:
        now = _now()
        cutoff = now - max(0.0, grace)
        keep_tokens = {_fs_safe(stream) for stream in PROTECTED_STREAMS}
        prefix_list: list[str] = []
        for prefix, suffixes in keep.items():
            prefix_list.append(_fs_safe_fragment(prefix))
            for suffix in suffixes:
                keep_tokens.add(_fs_safe(prefix + suffix))
        # str.startswith takes the whole tuple in one call per entry
        prefix_tokens = tuple(prefix_list)
        removed_streams: list[str] = []
        removed_records = 0
        kept_streams = 0
        records_root = self._records_root
        try:
            # scandir: the directory test below rides on the entry's own
            # d_type, so no stat per token; sorted by name, so the walk
            # order is deterministic.
            with os.scandir(records_root) as listing:
                entries = sorted(listing, key=lambda e: e.name)
        except OSError:
            entries = []
        for entry in entries:
            if not entry.is_dir():
                continue
            token = entry.name
            stream_dir = entry.path
            if token in keep_tokens or not token.startswith(prefix_tokens):
                # referenced, protected, or unrecognised: never delete what
                # is still wanted or cannot be classified.
                kept_streams += 1
                continue
            if (
                _FS_TRUNCATION_MARKER in token
                and self._read_stream_name_sidecar(stream_dir, token) is None
            ):
                # a truncated token with no verifiable name sidecar was
                # invisible to the keep-set builder, so its absence from
                # ``keep`` proves nothing: unclassifiable, keep.  The
                # next append lands the sidecar and makes it classifiable.
                kept_streams += 1
                continue
            try:
                names = os.listdir(stream_dir)
            except OSError:
                kept_streams += 1
                continue
            records = [n for n in names if n.endswith(".json")]
            newest = max(
                (_record_epoch(n) for n in records), default=float("-inf")
            )
            if not records:
                # an empty managed dir: usually deletable debris, but a
                # writer may have JUST created it (its first record's temp
                # file is still being renamed in), so age the DIRECTORY
                # itself against the grace instead of deleting on sight.
                try:
                    newest = os.stat(stream_dir).st_mtime
                except OSError:
                    newest = float("inf")
            if newest > cutoff:
                kept_streams += 1
                continue
            removed_streams.append(token)
            removed_records += len(records)
            if dry_run:
                continue
            for name in names:
                with contextlib.suppress(OSError):
                    os.unlink(os.path.join(stream_dir, name))
            # a straggler unlink (Windows sharing hold) leaves the dir
            # non-empty; the rmdir then fails and the next pass converges.
            with contextlib.suppress(OSError):
                os.rmdir(stream_dir)
            # a wholesale stream wipe: the derive_max memo must not
            # survive it, see _derive_max_invalidate.
            self._derive_max_invalidate(token)
            self._prune_countdown_forget(token)
            self._record_name_floor_forget(token)
        # Before the orphan-lock sweep on purpose: an idempotency doc this
        # pass deletes leaves its ``.lock`` behind (same NFS ghost-inode
        # rationale as DOC_DELETE), and running first lets THIS pass's lock
        # sweep reclaim it. A claim whose expiry lapsed a whole grace ago
        # was written at least a grace ago, so its lock's mtime already
        # predates the cutoff and _reclaim_idle_lock_sync takes it below.
        idem_docs_removed = self._gc_idem_docs_sync(cutoff, dry_run)
        leases_removed = self._gc_leases_sync(
            cutoff, dry_run, ephemeral_lease_prefixes
        )
        locks_removed = self._gc_orphan_locks_sync(cutoff, dry_run)
        tmp_removed = self._sweep_dir_sync(
            self._tmp_root, now - TMP_MAX_AGE, dry_run
        )
        quarantine_removed = self._sweep_dir_sync(
            self._quarantine_root, cutoff, dry_run
        )
        return {
            "dry_run": dry_run,
            "streams_removed": len(removed_streams),
            "removed": removed_streams,
            "records_removed": removed_records,
            "streams_kept": kept_streams,
            "idem_docs_removed": idem_docs_removed,
            "leases_removed": leases_removed,
            "locks_removed": locks_removed,
            "tmp_removed": tmp_removed,
            "quarantine_removed": quarantine_removed,
        }

    def _gc_idem_docs_sync(self, cutoff: float, dry_run: bool) -> int:
        """Sweep idempotency documents whose TTL lapsed a whole grace ago.

        A ``ttl > 0`` claim leaves its ``.doc`` behind after expiry, and
        per-event keys grow a flat namespace directory without bound.  An
        EXPIRED claim is provably dead weight: the store would already let
        any caller re-win it, so deleting it changes no outcome.

        Only docs under the idempotency namespace prefix are considered;
        permanent claims (``ttl == 0``, no ``expiresAt``) and anything
        unreadable are kept.  Each candidate is re-judged under its own
        document flock so a claim re-won between the free pre-check and
        the delete is never lost; the flock is a try-lock
        (:meth:`_try_locked`), so a doc a peer holds is skipped this pass.
        No per-file directory fsync: a resurrected deletion brings back an
        EXPIRED doc, still re-winnable, unlike the active-claim DOC_DELETE
        whose loss un-does a release.  The ``.lock`` side-files are left
        to the orphan-lock sweep.
        """
        removed = 0
        docs_root = self._docs_root
        idem_token_prefix = _fs_safe_fragment(_IDEM_DOC_NS_PREFIX)
        try:
            with os.scandir(docs_root) as listing:
                ns_entries = list(listing)
        except OSError:
            return 0
        for ns_entry in ns_entries:
            if not ns_entry.name.startswith(idem_token_prefix):
                continue
            if not ns_entry.is_dir():
                continue
            ns_dir = ns_entry.path
            try:
                names = os.listdir(ns_dir)
            except OSError:
                continue
            for name in names:
                if not name.endswith(".doc"):
                    continue
                doc_path = os.path.join(ns_dir, name)
                # mtime pre-gate, the same cheap one _reclaim_idle_lock_sync
                # takes: a claim's expiry is its write time plus its ttl, so
                # a doc written at or after the cutoff cannot have lapsed a
                # whole grace ago and never needs opening. Without it every
                # pass paid an open + read + parse for every permanent claim
                # and every live TTL, on the one lane the gc budget shares.
                # The gate only ever SKIPS, so a doc whose mtime moved out
                # of band (a restore, an rsync without -t) is swept a later
                # pass, like a try-lock miss; an OSError leaves it alone,
                # matching the never-delete-what-cannot-be-classified rule.
                try:
                    if os.stat(doc_path).st_mtime >= cutoff:
                        continue
                except OSError:
                    continue
                body = self._read_doc_file(doc_path)
                if not self._idem_doc_expired(body, cutoff):
                    continue
                if dry_run:
                    removed += 1
                    continue
                lock_path = doc_path[: -len(".doc")] + ".lock"
                try:
                    with self._try_locked(lock_path):
                        body = self._read_doc_file(doc_path)
                        if not self._idem_doc_expired(body, cutoff):
                            continue  # re-won since the free pre-check
                        with contextlib.suppress(FileNotFoundError):
                            self._unlink(doc_path)
                        removed += 1
                except OSError:
                    # held (a peer mid-claim; the try-lock never waits)
                    # or unopenable: skip this doc now, next pass retries.
                    continue
        return removed

    @staticmethod
    def _idem_doc_expired(
        body: Optional[dict[str, Any]], cutoff: float
    ) -> bool:
        """Whether an idempotency doc's TTL lapsed before ``cutoff``.

        ``None`` (absent or unreadable) and a missing/non-numeric
        ``expiresAt`` (a permanent claim, or a doc this build cannot
        classify) are never expired: the sweep must only ever delete what
        it can prove dead.
        """
        if body is None:
            return False
        expires = body.get("expiresAt")
        # bool is an int subclass: a JSON ``true`` would otherwise read as
        # expiresAt == 1 and delete a doc this build cannot classify. The
        # same explicit reject the peer-payload numbers use.
        if isinstance(expires, bool) or not isinstance(expires, (int, float)):
            return False
        return expires <= cutoff

    def _lease_dead_past_grace(self, lease_path: str, cutoff: float) -> bool:
        """Whether a lease was provably dead for the whole grace window.

        True only when BOTH the recorded expiry and the file's mtime (the
        last acquire/renew/release write; release marks expiry ``0.0`` in
        place, so the expiry alone cannot date it) predate ``cutoff``: no
        live actor can then still hold a stale ``Lease``.  That alone does
        NOT make deletion safe: fences can be persisted in durable
        records, which is why only ephemeral lease classes are eligible
        (see :meth:`_gc_leases_sync`).  Anything unreadable is NOT
        reclaimable: never delete what cannot be classified.
        """
        try:
            mtime = os.stat(lease_path).st_mtime
            lease = self._read_lease_file(lease_path, strict=True)
        except (OSError, _LeaseUnreadable):
            return False
        if lease is None:
            return False
        return lease.expires_at < cutoff and mtime < cutoff

    def _gc_leases_sync(
        self,
        cutoff: float,
        dry_run: bool,
        ephemeral_prefixes: tuple[str, ...],
    ) -> int:
        """Reclaim EPHEMERAL lease files dead past the grace window.

        Only leases matching ``ephemeral_prefixes`` are eligible; every
        other lease file is never deleted, whatever its age: it is its
        fence counter's only home, and a ``slots/<job>`` stream keeps
        cancel records carrying fences that outlive any grace window, so
        a fence reset could re-collide and silently cancel a healthy
        future run.  Only dagrun's per-run ``dagadvance/<dag>/<run_key>``
        leases grow without bound, and no fence for them is persisted
        outside the run document's own lifetime, so reclaiming those is
        safe; :meth:`_lease_dead_past_grace` covers every in-memory
        holder.

        The check-and-delete runs under the per-lease flock so it cannot
        race a concurrent re-acquire; the ``.lock`` sibling goes LAST (an
        acquirer recreating it after the ``.lease`` vanished simply takes
        a fresh fence-1 lease).
        """
        if not ephemeral_prefixes:
            return 0
        # prefix matching on the encoded filename: _fs_safe only rewrites
        # a token's first character or its over-length tail, so an encoded
        # prefix survives verbatim at the front.
        prefix_tokens = tuple(
            _fs_safe_fragment(p) for p in ephemeral_prefixes if p
        )
        if not prefix_tokens:
            return 0
        removed = 0
        lease_root = self._leases_root
        try:
            names = os.listdir(lease_root)
        except OSError:
            return 0
        for name in names:
            if not name.endswith(".lease"):
                continue
            token = name[: -len(".lease")]
            if not token.startswith(prefix_tokens):
                continue  # non-ephemeral: never touched
            lease_path = os.path.join(lease_root, name)
            lock_path = os.path.join(lease_root, token + ".lock")
            # cheap unlocked pre-check: skip anything plausibly live before
            # paying for its flock.
            if not self._lease_dead_past_grace(lease_path, cutoff):
                continue
            if dry_run:
                removed += 1
                continue
            with self._locked(lock_path):
                # re-judge under the lock: a concurrent re-acquire may have
                # just revived (rewritten) it.
                if not self._lease_dead_past_grace(lease_path, cutoff):
                    continue
                with contextlib.suppress(OSError):
                    os.unlink(lease_path)
                    removed += 1
                if not IS_WINDOWS:
                    # drop the lock side-file while STILL HOLDING it:
                    # _locked re-verifies inode identity after acquiring,
                    # so waiters on this unlinked inode re-open instead of
                    # splitting the mutex with the recreated file.
                    with contextlib.suppress(OSError):
                        os.unlink(lock_path)
            if IS_WINDOWS:
                # post-release: our own handle is closed now; a concurrent
                # acquirer's open handle (no FILE_SHARE_DELETE) makes this
                # fail harmlessly; a lost race orphans a BARE .lock, which
                # the orphan-lock sweep (not this .lease-keyed loop)
                # converges on a later pass.
                with contextlib.suppress(OSError):
                    self._unlink(lock_path)
        if removed and not dry_run:
            # make the reclamation itself crash-durable, once per pass.
            fsync_directory(lease_root)
        return removed

    def _gc_orphan_locks_sync(self, cutoff: float, dry_run: bool) -> int:
        """Sweep ``.lock`` side-files whose owner is gone and idle past grace.

        Two orphan classes, both otherwise permanent:

        * a document ``.lock`` whose ``.doc`` is ABSENT: ``DOC_DELETE``
          never unlinks the lock file (an eager unlink split the document
          mutex across nodes on NFS/EFS via a stale-cache samestat).
          mutate_document touches the lock's mtime on every acquire, so
          idle-past-grace means no mutator ran for a whole grace window;
        * a BARE lease ``.lock`` with no ``.lease`` sibling (the Windows
          post-release unlink in :meth:`_gc_leases_sync` can lose to a
          scanner's transient handle).  No ``.lease`` means no durable
          fence, so no prefix restriction is needed here.

        Each candidate is re-judged and deleted under its own flock, with
        :meth:`_locked`'s ghost re-verify protecting any waiter.  Accepted
        residual risk on shared mounts only: deleting a lock idle >= grace
        can in principle race a waiter stat-verifying through a stale NFS
        cache; the idle gate plus the GC cadence bounds this to a
        vanishing window.
        """
        removed = 0
        docs_root = self._docs_root
        try:
            with os.scandir(docs_root) as listing:
                ns_entries = list(listing)
        except OSError:
            ns_entries = []
        for ns_entry in ns_entries:
            if not ns_entry.is_dir():
                continue
            ns_dir = ns_entry.path
            try:
                names = os.listdir(ns_dir)
            except OSError:
                continue
            for name in names:
                if not name.endswith(".lock"):
                    continue
                lock_path = os.path.join(ns_dir, name)
                doc_path = os.path.join(ns_dir, name[: -len(".lock")]) + ".doc"
                if self._reclaim_idle_lock_sync(
                    lock_path, doc_path, cutoff, dry_run
                ):
                    removed += 1
        lease_root = self._leases_root
        try:
            names = os.listdir(lease_root)
        except OSError:
            names = []
        for name in names:
            if not name.endswith(".lock"):
                continue
            lock_path = os.path.join(lease_root, name)
            lease_path = (
                os.path.join(lease_root, name[: -len(".lock")]) + ".lease"
            )
            if self._reclaim_idle_lock_sync(
                lock_path, lease_path, cutoff, dry_run
            ):
                removed += 1
        # no fsync: a lock unlink that never becomes durable merely
        # resurfaces the orphan for the next pass.
        return removed

    def _reclaim_idle_lock_sync(
        self, lock_path: str, sibling_path: str, cutoff: float, dry_run: bool
    ) -> bool:
        """Check-and-delete one orphaned ``.lock`` (see the sweep above).

        Reclaims only when the lock's mtime predates ``cutoff`` AND its
        owning data file (``sibling_path``) is absent, judged both before
        paying for the flock and again while holding it.
        """
        try:
            if os.stat(lock_path).st_mtime >= cutoff:
                return False
        except OSError:
            # gone or unreadable: nothing to reclaim / cannot classify.
            return False
        if os.path.exists(sibling_path):
            return False
        if dry_run:
            return True
        with self._locked(lock_path):
            # re-judge under the flock: a concurrent acquire may have just
            # touched the lock or re-created the sibling (and _locked's
            # O_CREAT may have re-created a fresh file if another sweeper
            # won the race; its new mtime fails the age gate).
            try:
                if os.stat(lock_path).st_mtime >= cutoff:
                    return False
            except OSError:
                return False
            if os.path.exists(sibling_path):
                return False
            if not IS_WINDOWS:
                # unlink while STILL HOLDING the flock: waiters re-verify
                # inode identity and re-open instead of splitting the
                # mutex with a recreated file.
                with contextlib.suppress(OSError):
                    os.unlink(lock_path)
                return True
        # Windows: our own open handle forbids the unlink under the lock;
        # post-release, a concurrent acquirer's open handle (no
        # FILE_SHARE_DELETE) makes this fail harmlessly and a later pass
        # converges.
        with contextlib.suppress(OSError):
            self._unlink(lock_path)
        return True

    @staticmethod
    def _sweep_dir_sync(path: str, cutoff: float, dry_run: bool) -> int:
        """Unlink files under ``path`` last modified before ``cutoff``."""
        removed = 0
        try:
            names = os.listdir(path)
        except OSError:
            return 0
        for name in names:
            full = os.path.join(path, name)
            try:
                # one stat answers both the regular-file test and the age
                # gate; a directory fails S_ISREG and a broken symlink
                # raises OSError, and each skips the entry.
                info = os.stat(full)
                if not stat.S_ISREG(info.st_mode) or info.st_mtime >= cutoff:
                    continue
                if not dry_run:
                    os.unlink(full)
                removed += 1
            except OSError:
                continue
        return removed

    async def migrate_schema(self, *, dry_run: bool = False) -> dict[str, Any]:
        """Rewrite records of OLDER known schemes to the current one.

        For each ``schemaVersion`` with a registered converter
        (:data:`RECORD_MIGRATIONS`), rewrites the file in place via the
        usual temp-file + atomic rename, so a concurrent reader never
        sees a torn record.  The one sanctioned exception to "records are
        never rewritten": an explicit operator-run admin action whose
        rewrite is a pure re-encoding of the same logical record.
        Records with no converter and unreadable files are left alone
        (counted).
        """
        return await self._call("migrate", self._migrate_sync, dry_run)

    def _migrate_sync(self, dry_run: bool) -> dict[str, Any]:
        current = converted = unknown = unreadable = failed = 0
        records_root = self._records_root
        try:
            with os.scandir(records_root) as listing:
                streams = sorted(listing, key=lambda e: e.name)
        except OSError:
            streams = []
        for entry in streams:
            if not entry.is_dir():
                continue
            stream_dir = entry.path
            try:
                names = sorted(
                    [n for n in os.listdir(stream_dir) if n.endswith(".json")]
                )
            except OSError:
                continue
            for name in names:
                path = os.path.join(stream_dir, name)
                try:
                    with open(path, "rb") as fobj:
                        obj = _json.loads(fobj.read())
                except Exception:  # noqa: BLE001 - quarantined on next read
                    unreadable += 1
                    continue
                version = (
                    obj.get("schemaVersion") if isinstance(obj, dict) else None
                )
                if version == SCHEMA_VERSION:
                    current += 1
                    continue
                convert = RECORD_MIGRATIONS.get(str(version))
                data = obj.get("data") if isinstance(obj, dict) else None
                if convert is None or not isinstance(data, dict):
                    unknown += 1
                    continue
                try:
                    new_data = convert(data)
                except Exception:  # noqa: BLE001 - a converter bug, counted
                    failed += 1
                    continue
                if new_data is None:
                    unknown += 1
                    continue
                converted += 1
                if dry_run:
                    continue
                payload = _json.dumps_bytes(
                    {"schemaVersion": SCHEMA_VERSION, "data": new_data},
                    sort_keys=True,
                )
                try:
                    self._atomic_write(path, payload)
                except OSError:
                    converted -= 1
                    failed += 1
        return {
            "dry_run": dry_run,
            "current": current,
            "converted": converted,
            "unknown": unknown,
            "unreadable": unreadable,
            "failed": failed,
        }

    async def sweep_orphan_blobs(
        self,
        referenced: set[str],
        grace: float,
        *,
        dry_run: bool = False,
    ) -> int:
        """Delete artifact blobs no surviving record references.

        ``referenced`` is the set of SHA-256 digests every surviving
        artifact record still names; a blob is removed only when absent
        from that set AND older than ``grace`` seconds.  The age guard
        keeps a just-landed blob whose record has not landed yet (the
        put-blob-then-append-record window) from being swept.
        """
        return await self._call(
            "blob-sweep",
            self._sweep_orphan_blobs_sync,
            referenced,
            grace,
            dry_run,
        )

    def _sweep_orphan_blobs_sync(
        self, referenced: set[str], grace: float, dry_run: bool
    ) -> int:
        cutoff = _now() - max(0.0, grace)
        removed = 0
        blobs_root = self._blobs_root
        try:
            shards = os.listdir(blobs_root)
        except OSError:
            return 0
        for shard in shards:
            shard_dir = os.path.join(blobs_root, shard)
            try:
                names = os.listdir(shard_dir)
            except OSError:
                continue
            for name in names:
                if not name.endswith(".blob"):
                    continue
                digest = name[: -len(".blob")]
                if digest in referenced:
                    continue
                full = os.path.join(shard_dir, name)
                try:
                    if os.stat(full).st_mtime >= cutoff:
                        # too young: an in-flight put not yet recorded
                        continue
                    if not dry_run:
                        os.unlink(full)
                    removed += 1
                except OSError:
                    continue
            # a now-empty shard directory is harmless debris; drop it best
            # effort so the blob tree does not accumulate empty shards.
            if not dry_run:
                with contextlib.suppress(OSError):
                    os.rmdir(shard_dir)
        return removed

    # --- introspection ---------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            ops = {
                op: {
                    "count": int(entry[0]),
                    "errors": int(entry[1]),
                    "seconds": entry[2],
                }
                for op, entry in self._op_stats.items()
            }
            return {
                "ops": ops,
                "lock": {
                    "acquisitions": self._lock_acquisitions,
                    "wait_seconds": self._lock_wait_seconds,
                },
                "throttle": {
                    "count": self._throttled_ops,
                    "wait_seconds": self._throttle_wait_seconds,
                },
                # Live worker-lane occupancy: ``*_inflight`` at capacity
                # is the "store wedged" signal the op counters (which only
                # advance when an op FINISHES) cannot show.
                "workers": {
                    "bulk_inflight": self._inflight_bulk,
                    "bulk_peak": self._inflight_peak_bulk,
                    "bulk_capacity": BULK_CALL_SLOTS,
                    "lease_inflight": self._inflight_lease,
                    "lease_peak": self._inflight_peak_lease,
                    "lease_capacity": LEASE_CALL_SLOTS,
                },
            }

    def view_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend_name,
            "path": self.base,
            "namespace": self.namespace,
            "topology": self._topology,
            "shared_locking": self.supports_shared_locking(),
            "job_set_id": self.get_job_set_id(),
        }

    async def inventory(self) -> dict[str, Any]:
        """Metadata-only topology snapshot (see the base docstring).

        Walks the on-disk tree off the event loop; never returns a record
        payload or document value.  Routed through :meth:`_call`, never
        the default executor: a dashboard polling a hung mount would
        wedge the executor's non-daemon threads one by one until
        interpreter exit hangs behind them.
        """
        base_dict = await self._call("inventory", self._inventory_sync)
        base_dict["view"] = self.view_dict()
        base_dict["stats"] = self.stats()
        base_dict["enumerable"] = True
        return base_dict

    def _inventory_sync(self) -> dict[str, Any]:
        from urllib.parse import unquote

        cap = 200

        def decode(token: str) -> str:
            return unquote(token, errors="replace")

        def walk(root: str, suffix: str) -> dict[str, Any]:
            # group per first path segment: {prefix: {count, streams, scopes}}
            groups: dict[str, dict[str, Any]] = {}
            # os.listdir here, not scandir: `cronstable state check` must
            # degrade an unreadable root to zero streams, and that seam is
            # pinned by injecting the failure through os.listdir.
            try:
                tokens = sorted(os.listdir(root))
            except OSError:
                return groups
            for tok in tokens:
                node = os.path.join(root, tok)
                if not os.path.isdir(node):
                    continue
                try:
                    count = sum(
                        1 for n in os.listdir(node) if n.endswith(suffix)
                    )
                except OSError:
                    continue
                logical = decode(tok)
                prefix, _sep, scope = logical.partition("/")
                bucket = groups.setdefault(
                    prefix, {"count": 0, "streams": 0, "scopes": []}
                )
                bucket["count"] += count
                bucket["streams"] += 1
                if len(bucket["scopes"]) < cap:
                    bucket["scopes"].append({"scope": scope, "count": count})
            return groups

        records = walk(self._records_root, ".json")
        documents = walk(self._docs_root, ".doc")

        leases: list[dict[str, Any]] = []
        lease_root = self._leases_root
        now = _now()
        try:
            lease_files = sorted(os.listdir(lease_root))
        except OSError:
            lease_files = []
        for fname in lease_files:
            if not fname.endswith(".lease") or len(leases) >= cap:
                continue
            try:
                lease = self._read_lease_file(os.path.join(lease_root, fname))
            except Exception:  # noqa: BLE001 - best-effort observe
                lease = None
            if lease is None or lease.expires_at <= 0.0:
                continue  # released/absent lease: nobody holds it
            leases.append(
                {
                    "name": decode(fname[: -len(".lease")]),
                    "holder": lease.holder,
                    "fence": lease.fence,
                    "expiresAt": lease.expires_at,
                    "expired": lease.expires_at <= now,
                }
            )

        try:
            quarantine = len(os.listdir(self._quarantine_root))
        except OSError:
            quarantine = 0

        return {
            "records": records,
            "documents": documents,
            "leases": leases,
            "quarantine": quarantine,
        }


def make_state_backend(
    state_config: StateConfig,
    get_job_set_id: Callable[[], str],
) -> StateBackend:
    """Build the state backend for a ``state`` config section.

    Mirrors :func:`cronstable.leadership.make_backend`.  ``filesystem``
    is the only backend today; the factory keeps the seam ready for a
    future native-S3 backend, imported lazily here so it never enters the
    import graph unless selected.
    """
    backend = state_config.get("backend", "filesystem")
    if backend == "filesystem":
        return FilesystemStateBackend(state_config, get_job_set_id)
    raise ConfigError(  # pragma: no cover - no other backend yet / gated
        "unknown state.backend {!r}".format(backend)
    )
