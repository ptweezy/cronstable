"""Members the etcd and kubernetes backends share byte-for-byte.

Both backends keep the same local election state (a raw win flag, the
observed holder, monotonic deadlines) and track their on-disk client-TLS
material the same way, so the previously duplicated helpers live here
once.  The time helpers are imported into each backend module under
their own names: call sites resolve them through the importing module's
globals, so the tests' per-module monkeypatching (for example
``cronstable.backends.etcd._monotonic``) keeps working.
"""

import datetime
import time
from collections.abc import Sequence

from cronstable.leadership import LeaseBackend

# The on-disk fingerprint is the one the web listeners use; keeping the
# backends on it means one definition of "did this file rotate".
from cronstable.tlsutil import file_signature as _file_signature

# How far in advance of the computed lease expiry a holder stops calling
# itself leader, so a node whose clock runs slightly fast self-demotes
# BEFORE a peer would be entitled to take the lease over: erring
# is_leader toward False.
_CLOCK_SKEW = datetime.timedelta(seconds=1)
_SKEW_SECONDS = _CLOCK_SKEW.total_seconds()

# Reported as the holder when we know another node holds the fence but
# cannot name it (a lost create race, a non-conformant gateway dropping
# the range value).  A non-None holder keeps leader_name() non-None so a
# quorate follower defers its PreferLeader jobs instead of reading
# "holder unknown" as "run anyway" and double-running fleet-wide with no
# partition.  See each backend's _apply_round.
_UNKNOWN_HOLDER = "<unknown holder>"


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _monotonic() -> float:
    """A monotonic clock for the lease fence and freshness deadlines.

    Those must never ride the wall clock: a backward NTP/VM step would
    keep ``is_leader`` true past the server's lease expiry, and a forward
    step would expire quorum early (or, for the kubernetes steal anchor,
    steal a still-valid lease).  The wall clock is used only for the
    times written to the store and the human-readable expiry shown in
    the dashboard.
    """
    return time.monotonic()


def display_deadline(
    now: datetime.datetime, ttl_seconds: float
) -> datetime.datetime:
    """Wall-clock lease expiry, for display only.

    The :data:`_CLOCK_SKEW` margin matches the monotonic fence
    arithmetic in each backend's ``_apply_round``, so the dashboard
    never shows an expiry the fence has already given up on.
    """
    return now + datetime.timedelta(seconds=ttl_seconds) - _CLOCK_SKEW


def _parse_microtime(value: object) -> datetime.datetime | None:
    """Parse a Kubernetes ``MicroTime`` (RFC3339 with a ``Z``) to a datetime.

    Tolerant of fewer/more fractional digits than the canonical six, and of an
    explicit numeric offset, so it survives apiserver formatting variations.
    Returns ``None`` for anything unparseable (treated as "no time observed").
    """
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        date_part, _, frac_and_tz = text.partition(".")
        frac, tzsep = frac_and_tz, ""
        for sep in ("+", "-"):
            idx = frac_and_tz.find(sep)
            if idx != -1:
                frac, tzsep = frac_and_tz[:idx], frac_and_tz[idx:]
                break
        frac = (frac + "000000")[:6]
        text = "{}.{}{}".format(date_part, frac, tzsep)
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _format_microtime(when: datetime.datetime) -> str:
    """Format a datetime as a Kubernetes ``MicroTime`` string.

    Also the expiry format in every backend's ``lease_detail``, so the
    dashboard parses one shape.
    """
    return when.astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


class StoreLeaseBackend(LeaseBackend):
    """Shared base of the store-backed lease backends (etcd, kubernetes).

    Everything here reads only state both subclasses maintain the same
    way: the raw ``_is_leader`` win flag and observed ``_holder`` their
    renew rounds write, and the TLS snapshot taken when the client
    material is loaded.
    """

    # written by the subclass (__init__ and its renew rounds)
    _is_leader: bool
    _holder: str | None
    _tls_files: list[str]
    _tls_signature: dict[str, tuple[int, int] | None]

    def leader_name(self) -> str | None:
        if not self.is_quorate():
            return None
        return self._holder

    def _is_self_demoted_holder(self) -> bool:
        # raw win flag still set (we hold/held the fence and have not
        # observed a loss) but the monotonic fence has lapsed: the brief
        # self-demotion window. See LeadershipBackend.
        return self._is_leader and not self.is_leader()

    def _record_tls_signature(self, paths: Sequence[str | None]) -> None:
        """Snapshot the on-disk TLS files the client material was built from.

        ``None``/empty entries (embedded creds, plain http) are dropped, so
        nothing on disk to rotate leaves :meth:`tls_files_changed` ``False``.
        """
        self._tls_files = [p for p in paths if p]
        self._tls_signature = {p: _file_signature(p) for p in self._tls_files}

    def tls_files_changed(self) -> bool:
        """Whether any tracked on-disk TLS file changed since the snapshot.

        The TLS material is loaded once and never reloaded, so an in-place
        cert/CA rotation (same paths, new bytes; how cert-manager, Vault and
        Kubernetes secret refreshes renew) is otherwise invisible and the
        node silently loses leadership fleet-wide once the old cert expires.
        A reported change lets
        :meth:`cronstable.cron.Cron.start_stop_cluster` rebuild the backend.
        ``False`` when nothing was tracked.
        """
        if not self._tls_files:
            return False
        current = {p: _file_signature(p) for p in self._tls_files}
        return current != self._tls_signature
