"""Inbound heartbeat monitoring: watching work cronstable does not run.

A job cronstable owns is watched from the inside: the daemon launched the
process, so it knows when it started, what it exited with and how long it
took.  A *heartbeat* is the outside-in mirror of that.  Something else --
a classic crontab line on a NAS, a GitHub Actions workflow, a Kubernetes
CronJob, an appliance nobody can install a daemon on -- calls a URL when
it finishes, and cronstable alerts when that call does NOT arrive.  The
signal is an absence: a dead man's switch for schedules the scheduler has
no other way to see.

This module is the whole domain, and deliberately has no I/O in it: token
derivation, the persisted ping record, and the pure function that turns
(config, record, clock) into a verdict.  The daemon side -- the
unauthenticated ``/ping/{token}`` endpoint, the durable store, the report
fan-out and the metrics -- lives in :mod:`cronstable.cron`, which calls
:func:`observe` once per housekeeping pass per heartbeat.  Keeping the
verdict pure is what lets the state machine be tested against a frozen
clock without a daemon, a socket or a store.

Vocabulary
----------

A heartbeat declares WHEN it expects to hear from its job, in one of two
mutually exclusive spellings:

* ``period``: a plain interval.  "I expect a ping at least every 6
  hours."  Anchored on the last ping, so a job that drifts is judged
  against its own last success rather than a fixed grid.
* ``schedule``: a cron expression.  "I expect a ping after each 02:00
  fire."  The due instant is the first fire strictly after the last ping,
  in the heartbeat's own timezone, enumerated by the same engine that
  drives real jobs -- so ``LW``, ``15W`` and ``5#3`` mean here exactly
  what they mean on a job.

``grace`` is the slack allowed past the due instant before the heartbeat
is called down.  It absorbs the ordinary jitter between "the job fired"
and "the ping landed" (queueing, a slow upload, a retried curl).  Being
past due but inside the grace window is :data:`STATE_LATE`, which is
visible everywhere but deliberately silent: only :data:`STATE_DOWN`
reports.

``maxRuntime`` is optional and only means anything to a job that pings
``/start`` before its work: it catches the run that began and never came
back, which no amount of waiting for a finish ping can distinguish from a
run that was never started at all.

The state machine
-----------------

::

    NEW ──first ping──▶ UP ──past due──▶ LATE ──past grace──▶ DOWN
                        ▲                                      │
                        └──────────── a ping arrives ───────────┘

:data:`STATE_NEW` ages exactly like the others: a heartbeat that has
never been pinged is anchored on when it was first seen, so a fresh
daemon (or a newly added heartbeat) gets a full period before it can
report, and a backup that never ran ONCE still eventually pages.  That is
the whole point of the feature, so it must not be special-cased into
silence.

A ``fail`` ping is not an absence but a statement, and short-circuits the
clock: the heartbeat is down the moment it lands, with
:data:`REASON_FAILED`.  Recovery in every case is the next successful
ping.

Cluster semantics
-----------------

A ping may land on any node -- there is one URL per heartbeat, and
whatever resolves it decides.  With a ``state:`` store the accepting node
writes the record through it, so every node reads the same last-ping
instant and only the leader evaluates and reports.  Without a store, each
node keeps its own record and each judges what it personally heard,
which is correct for the single-node install and documented as a
limitation for the clustered one.
"""

import base64
import datetime
import hashlib
import hmac
from dataclasses import dataclass, replace
from typing import Any, NamedTuple, Optional

__all__ = [
    "HEARTBEAT_STATES",
    "PING_KINDS",
    "Observation",
    "PingRateLimiter",
    "PingRecord",
    "derive_token",
    "expected_at",
    "observe",
    "token_is_wellformed",
]

# --- ping kinds -------------------------------------------------------
# What an inbound ping asserts.  These are the URL suffixes as well as the
# stored `lastKind`, so the wire vocabulary and the record vocabulary are
# the same words.

#: the work finished, successfully (bare ``/ping/{token}``, or ``/0``)
PING_SUCCESS = "success"
#: the work finished and failed (``/fail``, or any nonzero ``/{exit}``)
PING_FAIL = "fail"
#: the work is beginning (``/start``); arms the ``maxRuntime`` check
PING_START = "start"

PING_KINDS = (PING_SUCCESS, PING_FAIL, PING_START)

# --- states -----------------------------------------------------------

#: configured, nothing has ever pinged it, still inside its first window
STATE_NEW = "new"
#: heard from within the expected window
STATE_UP = "up"
#: past due, still inside the grace window; visible but never reported
STATE_LATE = "late"
#: past due + grace, or an explicit failure: this is what reports
STATE_DOWN = "down"
#: an operator is deliberately holding it; excused from every check
STATE_PAUSED = "paused"
#: ``enabled: false`` in the config; excused, and kept only so the
#: heartbeat does not vanish from the dashboard the moment it is held
STATE_DISABLED = "disabled"

HEARTBEAT_STATES = (
    STATE_NEW,
    STATE_UP,
    STATE_LATE,
    STATE_DOWN,
    STATE_PAUSED,
    STATE_DISABLED,
)

#: states an operator is expected to act on; the report latch keys on this
#: rather than on ``state == STATE_DOWN`` so a future alerting state
#: cannot be added without deciding whether it pages.
ALERTING_STATES = frozenset({STATE_DOWN})

# --- down reasons -----------------------------------------------------

#: no ping arrived before the due instant plus grace
REASON_MISSED = "missed"
#: a ``/fail`` ping (or a nonzero exit code) said so outright
REASON_FAILED = "failed"
#: ``/start`` arrived, ``maxRuntime`` elapsed, no finish ever landed
REASON_OVERRUN = "overrun"

DOWN_REASONS = (REASON_MISSED, REASON_FAILED, REASON_OVERRUN)

# --- token derivation -------------------------------------------------

#: Characters in a derived token.  base32 (RFC 4648) lowercased and
#: stripped of padding: URL-safe with no escaping, no case to lose in a
#: shell or a copy-paste, and no `-`/`_` to be mangled by a line-wrapping
#: mail client the way base64url is.
_TOKEN_ALPHABET = frozenset("abcdefghijklmnopqrstuvwxyz234567")

#: Derived token length in characters.  base32 packs 5 bits per character,
#: so 26 characters is 130 bits of the HMAC -- far past guessing, and
#: still one double-click to select.
TOKEN_LENGTH = 26

#: Domain separator mixed into every derivation, so a ``pingSecret`` that
#: is accidentally shared with some other HMAC use in the operator's
#: infrastructure cannot produce colliding values.
_TOKEN_DOMAIN = b"cronstable/heartbeat/ping/v1"


def derive_token(secret: str, name: str) -> str:
    """The ping token for heartbeat ``name`` under ``secret``.

    Deterministic, so the URL in a hundred crontabs stays valid across
    restarts, reloads and nodes with no state to synchronize -- and
    rotates for the whole fleet at once when the secret changes.  The
    name is length-prefixed rather than merely concatenated so that no
    two distinct names can build the same HMAC message.
    """
    encoded = name.encode("utf-8")
    message = b"%s\x00%d\x00%s" % (_TOKEN_DOMAIN, len(encoded), encoded)
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
    return (base64.b32encode(digest).decode("ascii").lower().rstrip("="))[
        :TOKEN_LENGTH
    ]


def token_is_wellformed(token: str) -> bool:
    """Whether ``token`` could possibly be a ping token.

    A cheap shape check the ingest path runs BEFORE any lookup, so a
    scanner spraying long junk paths is rejected on a length test rather
    than on a dictionary probe.  Says nothing about whether the token
    names a real heartbeat.
    """
    return 1 <= len(token) <= 128 and not set(token) - _TOKEN_ALPHABET - set(
        "-_"
    )


# --- the persisted record ---------------------------------------------


@dataclass(slots=True)
class PingRecord:
    """Everything the monitor remembers about one heartbeat's pings.

    Serialized into the durable store as a single document per heartbeat
    (see ``Cron._heartbeat_document``), so the field names below are a
    wire contract: rename one and every daemon that already wrote a
    record reads back a default.  ``from_dict`` is therefore total -- it
    never raises on an unexpected, missing or malformed field, because
    the alternative is a corrupt record wedging the monitor for a
    heartbeat that is probably fine.

    Timestamps are timezone-aware UTC.  ``last_ping_at`` is the newest of
    any kind (including ``start``), which is what makes a long-running
    job that pings ``/start`` count as heard-from while it works.
    """

    last_ping_at: Optional[datetime.datetime] = None
    last_kind: Optional[str] = None
    last_success_at: Optional[datetime.datetime] = None
    last_fail_at: Optional[datetime.datetime] = None
    last_start_at: Optional[datetime.datetime] = None
    #: wall time of the last start→finish pair, when a job pings both
    last_duration_seconds: Optional[float] = None
    #: exit code carried by ``/ping/{token}/{exit}``; None for bare pings
    last_exit_code: Optional[int] = None
    #: caller-supplied run correlation id (``?rid=``), echoed in alerts
    last_run_id: Optional[str] = None
    #: the ping's own words: a trimmed body, e.g. `curl --data "$(tail …)"`
    last_body: Optional[str] = None
    #: observability only, and deliberately coarse; see PING_SOURCE_MAX
    last_source: Optional[str] = None
    #: which cluster node accepted the newest ping
    last_host: Optional[str] = None
    total_pings: int = 0
    total_fails: int = 0

    def to_dict(self) -> dict[str, Any]:
        """A JSON-safe body for the durable store."""
        return {
            "v": 1,
            "lastPingAt": _iso(self.last_ping_at),
            "lastKind": self.last_kind,
            "lastSuccessAt": _iso(self.last_success_at),
            "lastFailAt": _iso(self.last_fail_at),
            "lastStartAt": _iso(self.last_start_at),
            "lastDurationSeconds": self.last_duration_seconds,
            "lastExitCode": self.last_exit_code,
            "lastRunId": self.last_run_id,
            "lastBody": self.last_body,
            "lastSource": self.last_source,
            "lastHost": self.last_host,
            "totalPings": self.total_pings,
            "totalFails": self.total_fails,
        }

    @classmethod
    def from_dict(cls, body: Optional[dict[str, Any]]) -> "PingRecord":
        """Rebuild a record from the store; never raises.

        A missing document, a truncated write, a field of the wrong type
        and a document from a future schema all degrade to defaults for
        the fields involved rather than failing the read: an unreadable
        record must not be able to wedge the monitor.
        """
        if not isinstance(body, dict):
            return cls()
        return cls(
            last_ping_at=_parse(body.get("lastPingAt")),
            last_kind=_str_or_none(body.get("lastKind")),
            last_success_at=_parse(body.get("lastSuccessAt")),
            last_fail_at=_parse(body.get("lastFailAt")),
            last_start_at=_parse(body.get("lastStartAt")),
            last_duration_seconds=_float_or_none(
                body.get("lastDurationSeconds")
            ),
            last_exit_code=_int_or_none(body.get("lastExitCode")),
            last_run_id=_str_or_none(body.get("lastRunId")),
            last_body=_str_or_none(body.get("lastBody")),
            last_source=_str_or_none(body.get("lastSource")),
            last_host=_str_or_none(body.get("lastHost")),
            total_pings=_int_or_none(body.get("totalPings")) or 0,
            total_fails=_int_or_none(body.get("totalFails")) or 0,
        )

    @property
    def last_finish_at(self) -> Optional[datetime.datetime]:
        """The newest finishing ping (success or fail), if any.

        ``start`` deliberately does not count: the whole point of the
        ``maxRuntime`` check is to notice a start with no finish behind
        it.
        """
        return _newest(self.last_success_at, self.last_fail_at)

    def apply(
        self,
        kind: str,
        at: datetime.datetime,
        *,
        exit_code: Optional[int] = None,
        run_id: Optional[str] = None,
        body: Optional[str] = None,
        source: Optional[str] = None,
        host: Optional[str] = None,
    ) -> "PingRecord":
        """This record with ``kind`` recorded at ``at``, as a new record.

        Pure, and returns a copy: the durable path runs this inside the
        store's read-modify-write transform, which must be side-effect
        free because it may be retried on a torn read.

        Out-of-order delivery is handled by clamping rather than by
        rejecting: two nodes racing the same ping, or a retried curl
        arriving after a newer one, must never move ``last_ping_at``
        backwards and hand the monitor a stale due instant.  The counters
        still move, because the ping really did happen.
        """
        updated = replace(self)
        updated.total_pings += 1
        if kind == PING_FAIL:
            updated.total_fails += 1
        # A finish that follows a start measures the run; computed before
        # last_start_at is disturbed, and only for a genuinely newer
        # finish, so a replayed ping cannot invent a negative duration.
        if kind in (PING_SUCCESS, PING_FAIL):
            start = self.last_start_at
            if start is not None and at >= start:
                finished = self.last_finish_at
                if finished is None or start > finished:
                    updated.last_duration_seconds = (
                        at - start
                    ).total_seconds()
        if kind == PING_SUCCESS:
            updated.last_success_at = _newest(self.last_success_at, at)
        elif kind == PING_FAIL:
            updated.last_fail_at = _newest(self.last_fail_at, at)
        elif kind == PING_START:
            updated.last_start_at = _newest(self.last_start_at, at)
        newest = _newest(self.last_ping_at, at)
        updated.last_ping_at = newest
        # The descriptive fields describe the NEWEST ping only; a late
        # arrival must not overwrite a newer ping's exit code or body.
        if newest == at:
            updated.last_kind = kind
            updated.last_exit_code = exit_code
            updated.last_run_id = run_id
            updated.last_body = body
            updated.last_source = source
            updated.last_host = host
        return updated


# --- the verdict ------------------------------------------------------


class Observation(NamedTuple):
    """One heartbeat's state at one instant, as the monitor sees it.

    Shared by the monitor (which latches and reports on it), the HTTP
    payloads and the metrics, so every surface answers the same question
    the same way rather than each re-deriving "is this thing late".
    """

    state: str
    #: why it is down; None in every non-down state
    reason: Optional[str]
    #: when the next ping is expected; None only when the check cannot be
    #: anchored (a `schedule` whose fires have run out past the horizon)
    due_at: Optional[datetime.datetime]
    #: due_at + grace: the instant the state flips to DOWN
    down_at: Optional[datetime.datetime]
    #: seconds past ``due_at``; 0.0 while still inside the window
    overdue_seconds: float
    #: when the CURRENT down state began, ``None`` when not down.
    #:
    #: Derived rather than remembered, and that is the point: the
    #: alternative is for the monitor to stamp "when I noticed", which
    #: makes the value depend on the housekeeping cadence and lets a
    #: payload say ``state: down`` beside an empty onset for as long as a
    #: pass takes to come round.  Every surface reads the same exact
    #: instant here, whether or not the monitor has run since.
    since: Optional[datetime.datetime] = None

    @property
    def alerting(self) -> bool:
        """Whether this state is one an operator should be told about."""
        return self.state in ALERTING_STATES


def observe(
    config: Any,
    record: PingRecord,
    now: datetime.datetime,
    *,
    first_seen: datetime.datetime,
    paused: bool = False,
) -> Observation:
    """Judge one heartbeat against the clock.

    ``config`` is duck-typed as a
    :class:`cronstable.config.HeartbeatConfig` -- ``enabled``,
    ``period``, ``schedule_tab``, ``timezone``, ``grace`` and
    ``max_runtime`` -- so tests can pass a stub and the domain module
    need not import the config layer.

    ``first_seen`` anchors a heartbeat that has never been pinged: the
    instant this daemon first loaded it.  A fresh boot therefore grants a
    full window before anything can report, which is the same rule the
    job SLA monitor uses for a job that has never succeeded, and for the
    same reason: a restart is not evidence of a missed run.
    """
    if not config.enabled:
        return Observation(STATE_DISABLED, None, None, None, 0.0)
    if paused:
        return Observation(STATE_PAUSED, None, None, None, 0.0)

    due_at = expected_at(config, record, first_seen=first_seen)
    grace = datetime.timedelta(seconds=config.grace)
    down_at = due_at + grace if due_at is not None else None
    overdue = (
        max(0.0, (now - due_at).total_seconds()) if due_at is not None else 0.0
    )

    # An explicit failure outranks the clock: the job spoke, and waiting
    # out a grace window to believe it would only delay the page.  It has
    # been down since the failing ping landed.
    if record.last_kind == PING_FAIL:
        return Observation(
            STATE_DOWN,
            REASON_FAILED,
            due_at,
            down_at,
            overdue,
            record.last_fail_at,
        )

    # A start with no finish behind it, over its runtime bound.  Checked
    # before the missed-ping clock because it is the more specific
    # diagnosis of the same silence: `maxRuntime` is what distinguishes
    # "began and hung" from "never began at all".
    if config.max_runtime is not None:
        start = record.last_start_at
        finished = record.last_finish_at
        if start is not None and (finished is None or start > finished):
            running = (now - start).total_seconds()
            if running > config.max_runtime:
                return Observation(
                    STATE_DOWN,
                    REASON_OVERRUN,
                    due_at,
                    down_at,
                    overdue,
                    start + datetime.timedelta(seconds=config.max_runtime),
                )

    heard = record.last_ping_at is not None
    if down_at is not None and now > down_at:
        return Observation(
            STATE_DOWN, REASON_MISSED, due_at, down_at, overdue, down_at
        )
    if due_at is not None and now > due_at:
        return Observation(STATE_LATE, None, due_at, down_at, overdue)
    return Observation(
        STATE_UP if heard else STATE_NEW, None, due_at, down_at, overdue
    )


def expected_at(
    config: Any,
    record: PingRecord,
    *,
    first_seen: datetime.datetime,
) -> Optional[datetime.datetime]:
    """When the next ping is due, in UTC.

    Anchored on the last ping of any kind, falling back to when the
    heartbeat was first seen so a never-pinged one still has a deadline.

    In ``schedule`` mode this is the first fire strictly after the
    anchor, resolved in the heartbeat's own zone: a ping that lands after
    a fire therefore satisfies exactly that fire and moves the
    expectation to the next one.  A job that reliably pings a little
    BEFORE its fire (it starts early and finishes early) is judged
    against the fire it has not yet reached, and wants ``period`` instead
    -- the wiki page says so in as many words.

    ``None`` means the expectation cannot be anchored at all: a
    ``schedule`` whose occurrences are exhausted past the engine's 2099
    horizon.  Callers render that as "no further fires" and never report
    on it, which is the same treatment a dead job schedule gets.
    """
    anchor = record.last_ping_at or first_seen
    if config.schedule_tab is None:
        return anchor + datetime.timedelta(seconds=config.period)
    zone = config.timezone
    local = anchor.astimezone(zone) if zone is not None else anchor
    nxt: Optional[datetime.datetime] = next(
        iter(config.schedule_tab.occurrences(local)), None
    )
    if nxt is None:
        return None
    return nxt.astimezone(datetime.timezone.utc)


# --- ingest rate limiting ---------------------------------------------

#: Pings a single heartbeat may burst before the bucket empties.  Sized
#: for a retrying client (``curl --retry 5``) and a job that legitimately
#: pings ``/start`` and a finish moments apart, not for a steady stream.
PING_BURST = 20

#: Sustained pings per second per heartbeat once the burst is spent.  A
#: heartbeat is by definition a periodic signal; anything pinging faster
#: than this is a loop that got loose, and the point of the limit is that
#: such a loop costs the daemon a dict lookup rather than a store write.
PING_RATE = 1.0


class PingRateLimiter:
    """Per-heartbeat token bucket over a caller-supplied monotonic clock.

    Keyed by heartbeat NAME, never by the token the request carried: the
    bucket must survive a secret rotation, and an unknown token is
    refused before it ever reaches here (it has no name to charge).  The
    key set is therefore bounded by the config, not by traffic, so a
    scanner cannot grow this map.

    The clock is passed in rather than read, so the limiter is
    deterministic under test and the daemon can hand it the event loop's
    own monotonic time instead of paying a syscall per ping.
    """

    __slots__ = ("_tokens", "_seen", "_burst", "_rate")

    def __init__(
        self, burst: int = PING_BURST, rate: float = PING_RATE
    ) -> None:
        self._tokens: dict[str, float] = {}
        self._seen: dict[str, float] = {}
        self._burst = float(burst)
        self._rate = rate

    def allow(self, name: str, now: float) -> bool:
        """Charge one ping to ``name``; False when the bucket is empty."""
        last = self._seen.get(name)
        tokens = self._burst if last is None else self._tokens.get(name, 0.0)
        if last is not None:
            tokens = min(self._burst, tokens + (now - last) * self._rate)
        self._seen[name] = now
        if tokens < 1.0:
            self._tokens[name] = tokens
            return False
        self._tokens[name] = tokens - 1.0
        return True

    def retain(self, names: "frozenset[str] | set[str]") -> None:
        """Forget every key outside ``names`` (a reload removed them)."""
        for key in [key for key in self._seen if key not in names]:
            del self._seen[key]
            self._tokens.pop(key, None)


# --- small total parsers ----------------------------------------------
# from_dict must never raise (see its docstring), so every field goes
# through one of these rather than through a bare cast.


def _iso(value: Optional[datetime.datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _parse(value: Any) -> Optional[datetime.datetime]:
    """An ISO-8601 instant from the store, or None if it is not one."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    # A naive timestamp can only come from a hand-edited document; read
    # it as UTC rather than dropping it, since every writer emits UTC.
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def _str_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def _int_or_none(value: Any) -> Optional[int]:
    # bool is an int subclass; a JSON `true` is not an exit code.
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _float_or_none(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _newest(
    *values: Optional[datetime.datetime],
) -> Optional[datetime.datetime]:
    """The latest of the given instants, ignoring None."""
    present = [value for value in values if value is not None]
    return max(present) if present else None
