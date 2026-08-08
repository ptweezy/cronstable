# PEP 563 string annotations: the many `web.Request` / `web.Response`
# signatures below must never evaluate at def time, or importing this module
# would pull in aiohttp (see the _AiohttpDoor block after the imports).
from __future__ import annotations

import asyncio
import asyncio.subprocess
import copy
import datetime
import gc
import hashlib
import heapq
import hmac
import importlib.resources
import json
import logging
import logging.config
import os
import socket
import ssl
import zlib
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from functools import lru_cache, partial
from typing import (  # noqa
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    FrozenSet,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:  # the loopback job-state API is imported lazily at runtime
    import aiohttp
    from aiohttp import web

    from cronstable.jobapi import JobStateAPI

import cronstable.version
from cronstable import _json, discovery, platform, push, statsd, tlsutil
from cronstable.config import (
    WEB_TOKEN_SCOPES,
    ClusterConfig,
    ConfigError,
    CronstableConfig,
    DagConfig,
    JobConfig,
    JobDefaults,
    LoggingConfig,
    MCPConfig,
    StateConfig,
    WebConfig,
    cluster_config_warnings,
    parse_config_string,
    parse_config_with_sources,
    resolve_bonjour_config,
)
from cronstable.cronexpr import CronTab
from cronstable.croninfo import (
    ScheduleEntry,
    describe_cron,
    duplicate_schedules,
    lint_schedule,
    next_fires,
    schedule_pressure,
    suggest_slot,
    why_no_run,
)
from cronstable.dagrun import DAG_CATCHUP_STREAM_PREFIX, DagScheduler
from cronstable.fingerprint import job_digest_cached, job_set_id
from cronstable.ical import CalendarEntry, render_calendar
from cronstable.job import (
    JobOutputStream,
    JobRetryState,
    NotifyEventContext,
    RunningJob,
    SlaBreachContext,
    close_webhook_pool,
    report_config_enabled,
    report_event,
    report_hostname,
    report_sla_breach,
    schedule_string,
)
from cronstable.leadership import LeadershipBackend, make_backend
from cronstable.prometheus import (
    CONTENT_TYPE_OPENMETRICS,
    CONTENT_TYPE_TEXT,
    PrometheusMetrics,
    resolve_metrics_config,
)
from cronstable.redact import redact_lines
from cronstable.resources import (
    NodeResourceSampler,
    ResourceUsage,
    resolve_node_history_config,
)
from cronstable.state import Lease, StateBackend, make_state_backend


class _AiohttpDoor:
    """Stand-in for ``aiohttp`` / ``aiohttp.web`` that imports on first touch.

    aiohttp is 155 ms and 21 MB of RSS, roughly half of what importing this
    module costs, and every one of its consumers here is optional: the web
    listener, the cluster gossip client, the push relay. A daemon with none of
    them configured used to pay for the web stack anyway, and so did every
    offline caller that merely reaches into this module for a constant or a
    helper (``cronstable state gc`` and friends go through state_admin, which
    imports the stream prefixes from here).

    A module-level ``__getattr__`` (PEP 562) cannot do this job: it is
    consulted for attribute access on the module object from outside, not for
    the ``LOAD_GLOBAL`` that a ``web.Response(...)`` inside this file compiles
    to. An instance in the module globals is, so ``web`` and ``aiohttp`` are
    bound to one of these instead of to the modules themselves.

    The first attribute access rebinds BOTH globals to the real modules, so the
    proxy is passed exactly once per process: after that ``web.Response`` is an
    ordinary module attribute lookup with no indirection, which matters because
    it sits on every request path. ``__slots__`` keeps ``_target`` a real class
    attribute so looking it up inside ``__getattr__`` cannot recurse.
    """

    __slots__ = ("_target",)

    def __init__(self, target: str) -> None:
        self._target = target

    def __getattr__(self, name: str) -> Any:
        import aiohttp
        from aiohttp import web

        namespace = globals()
        namespace["aiohttp"] = aiohttp
        namespace["web"] = web
        return getattr(namespace[self._target], name)


if not TYPE_CHECKING:
    aiohttp = _AiohttpDoor("aiohttp")
    web = _AiohttpDoor("web")

logger = logging.getLogger("cronstable")
WAKEUP_INTERVAL = datetime.timedelta(minutes=1)
# Floor on subsystem wake hints (Cron._sleep_interval); only a real due
# fire may drive the sleep below this, so near-zero hints cannot busy-spin.
MIN_TICK_SLEEP = 0.02
# Max lateness replayed after a slow pass or clock jump (Cron._advance);
# past it only the newest occurrence fires, so a freeze cannot burst-launch.
CATCHUP_LIMIT = datetime.timedelta(seconds=10)
# Cap on onMissed: run-all replays per job when no startingDeadlineSeconds
# bounds the window; run-once always launches exactly once.
MAX_CATCHUP_OCCURRENCES = 100
# Finished runs retained per job, in memory only (web UI history/stats).
RUN_HISTORY_LIMIT = 50

#: Boot warm-up read parallelism; matches the filesystem store's 16-slot
#: bulk lane and bounds a hung mount to ~one STATE_OP_TIMEOUT of boot delay.
_REHYDRATE_CONCURRENCY = 16

#: Cap on same-slot subprocess spawns per loop pass (spawn work is
#: synchronous on the loop). Held only across RunningJob.start() itself.
_SPAWN_BURST_LIMIT = 16
# First ledger page _depends_on_past_ok reads; widens to RUN_HISTORY_LIMIT
# only when the whole probe page is non-run outcomes.
DEPENDS_GATE_PROBE = 8
# Run summaries inlined per job in /jobs; full history at /jobs/{name}/runs.
JOBS_INLINE_HISTORY = 20
# Durable finished-run ledger, one stream per job, scoped by JOB NAME so
# history survives an ordinary config reload.
RUN_STREAM_PREFIX = "runs/"
# Archived captured output (opt-in archiveOutput), pruned to maxRunsPerJob.
LOG_STREAM_PREFIX = "logs/"
# Catch-up checkpoints: "open" intent before a backfill, "close" after, so a
# restart resumes from the intent's watermark. At-least-once by design.
CATCHUP_STREAM_PREFIX = "catchup/"
# Checkpoint records retained per job (each cycle writes two).
CATCHUP_STREAM_KEEP = 8
# Cap on awaited state READS from scheduling paths: a hung mount degrades
# stateful features, never stalls scheduling.
STATE_OP_TIMEOUT = 10.0
# Cap on the tracked fire-and-forget write set; sheds new best-effort
# writes against a wedged store instead of growing without bound.
MAX_PENDING_STATE_WRITES = 8192
# Retry cadence when catch-up could not resolve (backend/cluster not ready).
CATCHUP_RECHECK_INTERVAL = 30.0
# Idle wait between serialized backfill launches (pacing, not correctness);
# Forbid waits unbounded because launching would be swallowed.
CATCHUP_IDLE_WAIT_LIMIT = 30.0
# Gate re-check floor so a tiny backoff cannot hot-loop on a closed cluster
# gate. See schedule_retry_job.
RETRY_GATE_RECHECK_FLOOR = 1.0
# Durable retry-ladder stream, newest record wins: "pending" (ABSOLUTE
# notBefore + config digest) re-arms at boot, "settled" resolves it.
RETRY_STREAM_PREFIX = "retries/"
# Ladder records retained per job (only the newest is ever read back).
RETRY_STREAM_KEEP = 8
# @reboot dedupe markers (host + boot + job digest); a redefined job or
# unreadable marker re-runs (at-least-once).
REBOOT_STREAM_PREFIX = "reboot/"
# Markers retained per job; several standalone hosts may share one store.
REBOOT_STREAM_KEEP = 32
# Slack comparing DERIVED boot times (now - uptime) across NTP steps; an
# exact boot_id (Linux) is used instead where available.
BOOT_TIME_TOLERANCE = 60.0
# Per-HOST job manifests anchoring cross-jobset GC. MUST stay per-host: a
# shared count-pruned stream's span shrinks as the fleet grows, eventually
# falling under gcGraceSeconds and deferring GC forever.
MANIFEST_STREAM_PREFIX = "manifests/"
# Per-host retention; GC also refuses to run until retained history
# provably covers one full grace window.
MANIFEST_STREAM_KEEP = 512
# Cap on per-host manifest streams read per GC pass; truncation is logged.
MANIFEST_HOSTS_CAP = 2000
# Manifest re-record and GC cadences. Loop-clock gated, per process.
STATE_MANIFEST_INTERVAL = 21600.0
STATE_GC_INTERVAL = 86400.0
# Store-hit cadence for the paused/ refresh and foreign-retry claim scans
# (single-flight only stops overlap). Just under a minute so a pass landing
# a hair early still sweeps instead of halving the cadence.
PAUSE_REFRESH_INTERVAL = 55.0
RETRY_CLAIM_INTERVAL = 55.0
# Bound on one GC pass so a wedged mount cannot leave the single-flight
# _gc_task pending and silently disable GC for the process.
STATE_GC_TIMEOUT = 600.0
# Durable Prometheus counter snapshots, host-scoped (a restart reclaims the
# host name, unlike the backend's per-process instance id).
COUNTER_STREAM_PREFIX = "counters/"
COUNTER_STREAM_KEEP = 4
# Floor between counter snapshots (they ride the per-run persist task); the
# tail is flushed at shutdown.
COUNTER_SNAPSHOT_INTERVAL = 15.0
# In-flight run stream, newest wins: "open" at first live instance, "closed"
# at last; a crash leaves "open" so takeover surfaces the interrupted run.
INFLIGHT_STREAM_PREFIX = "inflight/"
INFLIGHT_STREAM_KEEP = 8
# Slot-signalling stream (cluster-scoped Replace cancels); the slot LEASE
# shares the same "slots/<name>" name in the lease namespace.
SLOT_STREAM_PREFIX = "slots/"
SLOT_STREAM_KEEP = 8
# Lease serializing cross-node retry claims; TTL bounds a crashed claimer.
RETRY_CLAIM_PREFIX = "retry-claim/"
RETRY_CLAIM_TTL = 30.0
# Staleness before a foreign pending retry may be claimed; covers a live
# owner firing late only. The consume-time newest-record re-check under the
# claim lease is what prevents a double-fire, and is load-bearing.
RETRY_CLAIM_GRACE = 30.0
# Runtime pause defaults, hard ceiling (30 days), and audit-field caps.
PAUSE_DEFAULT_SECONDS = 3600
PAUSE_MAX_SECONDS = 2592000
PAUSE_NOTE_MAX = 500
PAUSE_BY_MAX = 100
# Durable pause stream, newest wins; expiry is reader-enforced (see
# _pause_active) and every node sharing the store honors a pause.
PAUSE_STREAM_PREFIX = "paused/"
PAUSE_STREAM_KEEP = 8
# SLA check labels (the sla config keys minus "Seconds"): one vocabulary for
# the breach latch, metric labels, payload "check" field and {{sla_check}}.
SLA_CHECK_STALE = "maxTimeSinceSuccess"
SLA_CHECK_LATE = "lateAfter"
SLA_CHECK_RUNTIME = "maxRuntime"
# The only check names ever keyed into _sla_state; latch bookkeeping walks
# these three, keeping a widespread breach O(jobs).
SLA_CHECKS = (SLA_CHECK_STALE, SLA_CHECK_LATE, SLA_CHECK_RUNTIME)
# Boot ledger re-read depth when the warm window holds no success at all
# (maxTimeSinceSuccess jobs only).
SLA_SUCCESS_SCAN_LIMIT = 1000
# Pause windows kept per job for the staleness credit; overflow drops the
# oldest, understating the credit rather than overstating it.
SLA_PAUSE_SPANS_MAX = 64
# Aggregation windows for GET /jobs/{name}/trends (label, seconds).
TREND_WINDOWS: tuple[tuple[str, float], ...] = (
    ("1h", 3600.0),
    ("24h", 86400.0),
    ("7d", 604800.0),
    ("30d", 2592000.0),
)
# Newest ledger records read per trends request (bounds unbounded-retention
# scans on every dashboard poll).
TREND_SCAN_LIMIT = 5000
# Trends payload TTL; a locally finished run busts the cache, so this only
# bounds re-reads of other cluster nodes' ledger writes.
JOB_TRENDS_CACHE_TTL = 5.0
# Served without bearer auth: only the UI page, which carries no secrets.
WEB_PUBLIC_PATHS = frozenset({"/"})

# Methods the cross-site Origin gate waves through (reads mutate nothing);
# OPTIONS passes so the /mcp CORS preflight keeps answering.
WEB_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# Paths enforcing their OWN Origin allow-list (/mcp validates against
# mcp.allowedOrigins); gating here too would 403 an allow-listed client.
WEB_ORIGIN_EXEMPT_PATHS = frozenset({"/mcp"})

# What the scalar web.authToken grants; `control` and `approve` imply
# `view`, expanded by _effective_web_scopes at token-resolution time.
_WEB_ALL_SCOPES = frozenset(WEB_TOKEN_SCOPES)

# Routes whose required scope differs from the method default (safe method
# -> `view`, else `control`), keyed by canonical path. Unlisted routes use
# the method default, so new routes cannot slip through unguarded.
_WEB_SCOPE_OVERRIDES = {
    # the DAG approval-gate decision is the one action gated by `approve`.
    "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision": "approve",
    # /mcp is action-capable; a scoped token needs `control` to drive it.
    "/mcp": "control",
}

# The web API's complete route table: (method, path, handler, gate).
# `handler` names a Cron method except "mcp"-gated rows (MCPHandler);
# `gate` marks conditionally registered groups (None, "mcp", "metrics",
# "ui"). start_stop_web_app builds the aiohttp routes from it and
# tests/test_openapi.py diffs it (plus _WEB_SCOPE_OVERRIDES' keys) against
# docs/openapi.yaml. Rows keep registration order; append conditional
# groups at the end.
WEB_ROUTES: "tuple[tuple[str, str, str, Optional[str]], ...]" = (
    ("GET", "/version", "_web_get_version", None),
    ("GET", "/job-set-id", "_web_job_set_id", None),
    ("GET", "/cluster", "_web_get_cluster", None),
    ("GET", "/fleet", "_web_get_fleet", None),
    ("GET", "/node", "_web_get_node", None),
    ("GET", "/node/history", "_web_node_history", None),
    ("GET", "/status", "_web_get_status", None),
    ("GET", "/summary", "_web_get_summary", None),
    ("GET", "/schedule/preview", "_web_schedule_preview", None),
    ("GET", "/schedule/pressure", "_web_schedule_pressure", None),
    ("GET", "/schedule/duplicates", "_web_schedule_duplicates", None),
    ("GET", "/schedule/suggest", "_web_schedule_suggest", None),
    ("GET", "/schedule/why", "_web_schedule_why", None),
    ("GET", "/calendar.ics", "_web_calendar", None),
    ("GET", "/jobs", "_web_list_jobs", None),
    # top-level on purpose: /jobs/activity would shadow a job named
    # "activity" under the /jobs/{name} dynamic route.
    ("GET", "/activity", "_web_get_activity", None),
    ("GET", "/jobs/{name}", "_web_get_job", None),
    ("GET", "/jobs/{name}/runs", "_web_job_runs", None),
    ("GET", "/jobs/{name}/calendar.ics", "_web_job_calendar", None),
    ("GET", "/jobs/{name}/resources", "_web_job_resources", None),
    ("GET", "/jobs/{name}/trends", "_web_job_trends", None),
    ("POST", "/jobs/{name}/start", "_web_start_job", None),
    ("POST", "/jobs/{name}/cancel", "_web_cancel_job", None),
    ("POST", "/jobs/{name}/pause", "_web_pause_job", None),
    ("POST", "/jobs/{name}/resume", "_web_resume_job", None),
    ("GET", "/jobs/{name}/logs", "_web_job_logs", None),
    # DAG introspection + control
    ("GET", "/dags", "_web_list_dags", None),
    ("GET", "/dags/{name}/runs", "_web_dag_runs", None),
    ("GET", "/dags/{name}/runs/{run_key}", "_web_dag_run", None),
    ("GET", "/dags/{name}/runs/{run_key}/xcom", "_web_dag_xcom", None),
    (
        "GET",
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        "_web_dag_task_logs",
        None,
    ),
    ("POST", "/dags/{name}/trigger", "_web_dag_trigger", None),
    ("POST", "/dags/{name}/backfill", "_web_dag_backfill", None),
    (
        "POST",
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
        "_web_dag_decision",
        None,
    ),
    # durable state inspector (metadata-only)
    ("GET", "/state", "_web_state", None),
    ("GET", "/state/documents", "_web_state_documents", None),
    ("GET", "/state/records", "_web_state_records", None),
    # bearer-token introspection: which token authenticated me, what scopes
    ("GET", "/whoami", "_web_whoami", None),
    # push pairing registry; registered unconditionally so a reload adding
    # `push:` needs no web-app restart (handlers 404 until configured).
    ("GET", "/push/devices", "_web_push_devices", None),
    ("POST", "/push/devices", "_web_push_pair", None),
    ("DELETE", "/push/devices/{id}", "_web_push_revoke", None),
    ("POST", "/push/devices/{id}/test", "_web_push_test", None),
    # /mcp is NEVER in WEB_PUBLIC_PATHS: it inherits the bearer-token gate
    # (and the `control` scope override above) on every method.
    ("POST", "/mcp", "handle_http", "mcp"),
    ("GET", "/mcp", "handle_http_get", "mcp"),
    ("OPTIONS", "/mcp", "handle_options", "mcp"),
    ("GET", "/metrics", "_web_metrics", "metrics"),
    ("GET", "/", "_web_index", "ui"),
)


def _effective_web_scopes(scopes: Iterable[str]) -> "frozenset[str]":
    """Expand declared token scopes: ``control`` and ``approve`` imply
    ``view`` (an action UI must read state first)."""
    effective = set(scopes)
    if effective & {"control", "approve"}:
        effective.add("view")
    return frozenset(effective)


def _required_web_scope(request) -> str:
    """The scope a request must hold, from its matched route and method.

    Safe methods default to ``view``, mutating ones to ``control``, with
    _WEB_SCOPE_OVERRIDES on top. An unmatched route (about to 404) falls
    back to the method default but is still authenticated first.
    """
    route = request.match_info.route
    resource = getattr(route, "resource", None)
    if resource is not None:
        override = _WEB_SCOPE_OVERRIDES.get(resource.canonical)
        if override is not None:
            return override
    if request.method in WEB_SAFE_METHODS:
        return "view"
    return "control"


#: Request-storage key the auth middleware files the matched token under,
#: so handlers can see who authenticated the caller without re-deriving it
#: (GET /whoami; the pairing endpoints' createdBy audit field). Absent when
#: no auth middleware is installed (no token configured).
WEB_TOKEN_REQUEST_KEY = "cronstable_web_token"


class _WebToken(NamedTuple):
    """A resolved web bearer token: its raw bytes (for the constant-time
    compare), the scopes it grants (view already implied by control/approve),
    and a human label used to identify and revoke it."""

    token_bytes: bytes
    scopes: "frozenset[str]"
    label: str


def _accepts_json(request: "web.Request") -> bool:
    """Whether the ``Accept`` header explicitly names ``application/json``.

    Parses media ranges (generated clients send compound headers) and
    deliberately ignores ``*/*``/``application/*`` wildcards: honouring
    them would flip curl's default Accept from text to JSON and break
    every script parsing the text form of the dual-format endpoints.
    """
    accept = request.headers.get("Accept", "")
    for media_range in accept.split(","):
        if media_range.split(";", 1)[0].strip().lower() == "application/json":
            return True
    return False


def _origin_matches_host(origin: str, host: Optional[str]) -> bool:
    """Whether a browser ``Origin`` header names this request's own ``Host``.

    The same-origin test behind the CSRF/DNS-rebinding gate. Compares
    authority only (hostname + port, defaults from the Origin scheme); the
    scheme is deliberately NOT compared, or a TLS-terminating reverse proxy
    would 403 the operator's own dashboard. ``Origin: null`` and anything
    unparsable never match: fail closed.
    """
    if not host:
        return False
    try:
        parsed = urlparse(origin)
        if not parsed.scheme or not parsed.hostname:
            return False  # "null", garbage, or a scheme-less token
        default_port = 443 if parsed.scheme == "https" else 80
        origin_port = parsed.port if parsed.port is not None else default_port
        # the Host header is an authority, not a URL; give urlparse a
        # netloc-shaped string so bracketed IPv6 hosts parse correctly.
        hparsed = urlparse("//" + host)
        if not hparsed.hostname:
            return False
        host_port = hparsed.port if hparsed.port is not None else default_port
    except ValueError:
        # urlparse defers port validation to .port; a malformed port in
        # either header can never match anything.
        return False
    # .hostname lowercases both sides already
    return parsed.hostname == hparsed.hostname and origin_port == host_port


class ApiActionError(Exception):
    """A read/control action that failed with a client-facing reason.

    Carries an HTTP-style ``status`` so the shared payload/action helpers can
    raise one place and each surface translate it: the aiohttp handlers into
    the matching ``web.HTTP*`` response, and the MCP layer into an ``isError``
    tool result the model can read and correct.
    """

    def __init__(self, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


def _strip_headers(headers: Optional[Any], *names: str) -> dict[str, str]:
    """A fresh dict of ``headers`` minus ``names``, case-insensitively.

    ``names`` must be given lowercase.  ``None`` or an empty mapping
    yields an empty dict, so callers may mutate the result freely.
    """
    if not headers:
        return {}
    return {
        key: value
        for key, value in dict(headers).items()
        if key.lower() not in names
    }


def _strip_content_type(headers: Optional[Any]) -> dict[str, str]:
    """The mapping minus any Content-Type, in any spelling.

    An endpoint's own Content-Type wins over an operator-configured
    ``web.headers`` one: aiohttp refuses ``content_type=`` when the mapping
    already carries one, and a case-variant leftover would be emitted as a
    second, conflicting header.
    """
    return _strip_headers(headers, "content-type")


def _error_body(message: str) -> str:
    """The web API's ONE error envelope: a JSON object ``{"error": msg}``.

    Every 4xx/5xx body this origin serves carries it (via :func:`_api_error`,
    ``_json_response({"error": ...})``, or :func:`_error_envelope_middleware`
    for anything that would escape as text/plain), matching the jobapi and
    MCP surfaces, so a client parses failures one way.
    """
    return json.dumps({"error": message})


def _api_error(
    factory: "type[web.HTTPException]",
    message: str,
    headers: Optional[Any] = None,
) -> web.HTTPException:
    """An aiohttp error response carrying the uniform JSON envelope.

    The envelope's own Content-Type wins over any configured ``web.headers``
    one, in any spelling (see :func:`_strip_content_type`).
    """
    if headers is not None:
        clean = _strip_content_type(headers)
        return factory(
            text=_error_body(message),
            content_type="application/json",
            headers=clean,
        )
    return factory(text=_error_body(message), content_type="application/json")


def _http_for_action_error(
    ex: "ApiActionError", headers: Optional[Any] = None
) -> web.HTTPException:
    """Map an :class:`ApiActionError` to the matching aiohttp response.

    ``headers`` (the operator's ``web.headers``) is applied only on the 409
    conflict bodies of the job start/cancel routes.
    """
    status_map = {
        400: web.HTTPBadRequest,
        403: web.HTTPForbidden,
        404: web.HTTPNotFound,
        409: web.HTTPConflict,
    }
    factory = status_map.get(ex.status, web.HTTPBadRequest)
    return _api_error(factory, ex.message, headers)


@web.middleware
async def _error_envelope_middleware(request, handler):
    """Give every escaping HTTP error the one JSON envelope.

    Installed outermost so it catches errors that would escape as aiohttp's
    text/plain defaults (bare raises, the auth middleware's 401s, the
    router's 404/405). Errors already carrying the envelope pass through;
    headers the error legitimately owns (a 405's ``Allow``) are preserved.
    """
    try:
        return await handler(request)
    except web.HTTPException as ex:
        if ex.status < 400 or ex.content_type == "application/json":
            raise
        headers = _strip_headers(ex.headers, "content-type", "content-length")
        return web.Response(
            text=_error_body(ex.text or ex.reason),
            status=ex.status,
            headers=headers,
            content_type="application/json",
        )


# Defense-in-depth security headers for the dashboard HTML document. The
# page is fully self-contained (one inline <script>, inline styles, no
# external assets) and only ever talks to its own origin, so this CSP is
# deliberately strict:
#   - 'unsafe-inline' for script/style is unavoidable (everything is inlined),
#     but connect-src 'self' confines any hypothetical injected script to this
#     origin: it cannot exfiltrate to an attacker's server;
#   - frame-ancestors 'none' (plus X-Frame-Options) blocks clickjacking of the
#     run/cancel controls; base-uri/form-action 'none' close those vectors.
# Operators can override any of these via the web.headers config option, which
# is merged on top of these defaults (see _security_headers).
WEB_SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "base-uri 'none'; "
        "form-action 'none'; "
        "frame-ancestors 'none'; "
        "object-src 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
}


@dataclass(slots=True)
class JobRunInfo:
    """In-memory summary of a job's most recent finished run (web UI history).

    Retains the run's output stream so the UI can replay the last run's logs
    after the job is no longer running. Never persisted to disk. Only the
    newest record's stream keeps its ring: when a later run supersedes this
    one, :meth:`Cron._record_run` releases the ring (see
    :meth:`cronstable.job.JobOutputStream.release_lines`), and the object
    stays in ``run_history`` as the summary the history endpoints read.
    """

    # "success" | "failure" | "cancelled" | "skipped" (a pause held the slot
    # back) | "unknown" (rehydrated row whose outcome did not survive)
    outcome: str
    exit_code: Optional[int]
    started_at: Optional[datetime.datetime]
    finished_at: datetime.datetime
    fail_reason: Optional[str]
    output: JobOutputStream
    # sampled CPU time + peak RSS when the job opted into monitorResources;
    # None otherwise. The reaper fills it from the finished RunningJob.
    resource_usage: Optional[ResourceUsage] = None
    # why a synthetic "skipped" row exists ("paused"); None for real runs.
    skip_reason: Optional[str] = None
    # Elapsed seconds, derived once at construction (both operands are
    # immutable). compare=False keeps equality over the recorded fields.
    duration: Optional[float] = field(default=None, init=False, compare=False)

    def __post_init__(self) -> None:
        if self.started_at is not None:
            self.duration = (
                self.finished_at - self.started_at
            ).total_seconds()

    def to_dict(self, *, include_series: bool = False) -> dict[str, Any]:
        """JSON-serializable summary (everything except the output stream).

        ``include_series`` embeds the downsampled CPU/RSS chart series: on
        for the durable ledger record and the resources endpoint, off for
        the polled payloads.

        ``ranAt`` mirrors ``finished_at`` on real runs and is omitted
        entirely (never nulled: ``derive_max`` folds over present values)
        on a synthetic ``skipped`` row. That gives
        :meth:`Cron.durable_last_completed_at` a watermark a pause cannot
        move, while ``finished_at`` stays unfiltered for the catch-up
        watermark, which intentionally advances over pause-skipped slots.
        """
        # one isoformat for the two keys that carry the same instant
        finished = self.finished_at.isoformat()
        data: dict[str, Any] = {
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "started_at": (
                self.started_at.isoformat()
                if self.started_at is not None
                else None
            ),
            "finished_at": finished,
            "duration": self.duration,
            "fail_reason": self.fail_reason,
            "skip_reason": self.skip_reason,
            # omitted (null) for unmonitored runs so the record shape is
            # unchanged for the default config; a monitored run carries the
            # cpu/rss sub-object (see ResourceUsage.to_dict).
            "resources": (
                self.resource_usage.to_dict(include_series=include_series)
                if self.resource_usage is not None
                else None
            ),
        }
        if self.outcome != "skipped":
            data["ranAt"] = finished
        return data


@dataclass(slots=True)
class PauseInfo:
    """One job's runtime pause window (see :meth:`Cron.pause_job_by_name`).

    All datetimes are aware UTC.  A record whose ``until`` has passed is
    treated as absent at every read site (:meth:`Cron._pause_active`);
    auto-expiry is reader-enforced, never stored.
    """

    since: datetime.datetime
    until: datetime.datetime
    note: str
    by: str
    channel: str

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form (the /jobs "paused" object and pause responses)."""
        return {
            "since": self.since.isoformat(),
            "until": self.until.isoformat(),
            "note": self.note,
            "by": self.by,
            "channel": self.channel,
        }


def _run_stats(runs: list[JobRunInfo]) -> dict[str, Any]:
    """Aggregate stats over a job's retained run history, for the web UI."""
    total = len(runs)
    success = sum(1 for r in runs if r.outcome == "success")
    failure = sum(1 for r in runs if r.outcome == "failure")
    cancelled = sum(1 for r in runs if r.outcome == "cancelled")
    # crash-reconciled runs, bucketed on their own so they are neither
    # hidden in `total` nor miscounted as real failures.
    unknown = sum(1 for r in runs if r.outcome == "unknown")
    durations = [r.duration for r in runs if r.duration is not None]
    # resource-monitored runs only (monitorResources); an unmonitored history
    # leaves these all None/absent so the dashboard hides the section.
    monitored = [
        r.resource_usage for r in runs if r.resource_usage is not None
    ]
    cpu_totals = [u.cpu_total_seconds for u in monitored]
    rss_values = [u.max_rss_bytes for u in monitored]
    last_usage = runs[-1].resource_usage if runs else None
    return {
        "total": total,
        "success": success,
        "failure": failure,
        "cancelled": cancelled,
        "unknown": unknown,
        # success rate over runs that ran to completion (excludes
        # cancellations: user-initiated, not a verdict on the job itself).
        "success_rate": (
            success / (success + failure) if (success + failure) else None
        ),
        "avg_duration": (
            (sum(durations) / len(durations)) if durations else None
        ),
        "min_duration": min(durations) if durations else None,
        "max_duration": max(durations) if durations else None,
        "last_duration": runs[-1].duration if runs else None,
        # CPU time (seconds) and peak resident memory (bytes) over the
        # monitored runs; None when no run in the window was monitored.
        "avg_cpu_seconds": (
            (sum(cpu_totals) / len(cpu_totals)) if cpu_totals else None
        ),
        "max_cpu_seconds": max(cpu_totals) if cpu_totals else None,
        "last_cpu_seconds": (
            last_usage.cpu_total_seconds if last_usage is not None else None
        ),
        "avg_rss_bytes": (
            (sum(rss_values) / len(rss_values)) if rss_values else None
        ),
        "max_rss_bytes": max(rss_values) if rss_values else None,
        "last_rss_bytes": (
            last_usage.max_rss_bytes if last_usage is not None else None
        ),
    }


def _parse_iso_utc(value: Any) -> Optional[datetime.datetime]:
    """Parse an ISO-8601 string to an AWARE datetime, or ``None``.

    Ledger records written by cronstable are always aware UTC, but the parsers
    must survive foreign/hand-written records: a naive timestamp is pinned to
    UTC rather than returned naive, because a naive datetime escaping into
    schedule arithmetic (``_compute_next_fire``) or a ``duration`` subtraction
    raises TypeError against the aware datetimes everything else uses, and
    on the catch-up path that would crash the scheduler at every boot until
    the record is deleted.
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _in_pause_window(
    when: datetime.datetime,
    window: tuple[Optional[datetime.datetime], datetime.datetime],
) -> bool:
    """Whether ``when`` falls inside a durable pause window.

    Half-open ``[since, until)``, the same window :meth:`Cron._pause_active`
    enforces live (it reports a pause whose ``until`` has arrived as absent),
    so a slot landing exactly on ``until`` is owed rather than excused.  A
    ``since`` of ``None`` (a record without the field) means the window has
    no known start and covers everything before ``until``.
    """
    since, until = window
    return when < until and (since is None or when >= since)


def _fold_manifest(
    rec: dict[str, Any],
    names: set[str],
    hosts: set[str],
    art_scopes: set[str],
    live_dags: set[str],
) -> None:
    """Accumulate one recent manifest record into the GC keep sets.

    Shared by the daemon pass and `cronstable state gc` so both read a
    manifest identically; a missing/mis-typed key contributes nothing (an
    older node's record simply advertises less; see
    :func:`_manifests_cover_scopes` for why that also gates artifact GC).
    """
    jobs = rec.get("jobs")
    if isinstance(jobs, list):
        names.update(str(job) for job in jobs)
    host = rec.get("host")
    if isinstance(host, str) and host:
        hosts.add(host)
    if isinstance(rec.get("scopes"), list):
        art_scopes.update(str(s) for s in rec["scopes"])
    if isinstance(rec.get("dags"), list):
        live_dags.update(str(d) for d in rec["dags"])


def _manifests_cover_scopes(recent: list[dict[str, Any]]) -> bool:
    """Whether artifact streams / dag-run documents may be managed at all.

    Only once EVERY recent manifest advertises its scopes and dags: a
    pre-scopes node's manifest proves nothing about the shared artifact
    scopes its jobs may write or the dags it runs, so treating its silence
    as absence would collect a live peer's artifacts mid-rolling-upgrade.
    An empty ``recent`` also fails: with no manifest to anchor absence,
    nothing artifact-related may be collected.
    """
    return bool(recent) and all(
        isinstance(rec.get("scopes"), list)
        and isinstance(rec.get("dags"), list)
        for rec in recent
    )


def build_gc_keep_set(
    manifests: list[dict[str, Any]],
    now: datetime.datetime,
    grace: float,
    names: set[str],
    hosts: set[str],
    art_scopes: set[str],
    live_dags: set[str],
) -> tuple[dict[str, set[str]], list[dict[str, Any]]]:
    """Fold recent manifests and build the stream keep-set for one GC pass.

    The ONE spelling of the prefix-to-keep-names map, shared by the
    daemon's automatic pass and `cronstable state gc` so the two cannot
    drift (a prefix missing from one pass would let it reclaim live
    bookkeeping the other protects). The passed sets are mutated in
    place. Returns ``(keep, recent)``; each caller layers its own extras
    on ``keep`` (live-pause streams, dag catch-up checkpoints, artifact
    scopes) since those need I/O or coverage guards the caller owns.
    Abandoned per-host streams (counters, manifests) age out because only
    recently-manifesting hosts land in ``hosts``.
    """
    recent: list[dict[str, Any]] = []
    for rec in manifests:
        at = _parse_iso_utc(rec.get("at"))
        if at is None or (now - at).total_seconds() > grace:
            continue
        recent.append(rec)
        _fold_manifest(rec, names, hosts, art_scopes, live_dags)
    # job names keep their default artifact scope too.
    art_scopes |= names
    keep: dict[str, set[str]] = {
        RUN_STREAM_PREFIX: names,
        LOG_STREAM_PREFIX: names,
        CATCHUP_STREAM_PREFIX: names,
        RETRY_STREAM_PREFIX: names,
        REBOOT_STREAM_PREFIX: names,
        COUNTER_STREAM_PREFIX: hosts,
        INFLIGHT_STREAM_PREFIX: names,
        SLOT_STREAM_PREFIX: names,
        MANIFEST_STREAM_PREFIX: hosts,
    }
    return keep, recent


def _parse_retry_record(
    rec: dict[str, Any],
) -> tuple[int, datetime.datetime, datetime.datetime] | None:
    """Parse ``(attempt, notBefore, armed_at)`` out of a ladder record.

    ``None`` for unparseable content; the caller decides whether that
    settles the ladder (rehydration) or merely declines it (the
    cross-node claim scan). A handoff carries the original arm time in
    ``armedAt``; a pending's own ``at`` IS its arm time (a handoff's
    ``at`` is the hand-off instant, which would hide a completed run).
    """
    attempt = rec.get("attempt")
    not_before = _parse_iso_utc(rec.get("notBefore"))
    if (
        isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or attempt < 1
        or not_before is None
    ):
        return None
    armed_at = (
        _parse_iso_utc(rec.get("armedAt"))
        or _parse_iso_utc(rec.get("at"))
        or not_before
    )
    return attempt, not_before, armed_at


def _job_run_info_from_dict(
    rec: dict[str, Any], *, output: Optional[JobOutputStream] = None
) -> Optional["JobRunInfo"]:
    """Rebuild a :class:`JobRunInfo` from a durable ledger record.

    Inverse of :meth:`JobRunInfo.to_dict`, used to warm in-memory history
    on restart. Output is not persisted, so a rehydrated run gets an empty,
    closed stream; a record with no parseable ``finished_at`` returns None
    rather than crashing the rehydration.

    ``output`` lets a bulk caller (the trends aggregation) supply one
    shared closed placeholder; leave it None for infos entering
    ``run_history``, where log replay expects each run's own stream.
    """
    # _parse_iso_utc pins naive timestamps to UTC: a rehydrated JobRunInfo
    # mixing naive and aware datetimes would raise TypeError from the
    # `duration` property on every dashboard request.
    finished = _parse_iso_utc(rec.get("finished_at"))
    if finished is None:
        # a crash-reconciled record deliberately omits finished_at so the
        # catch-up watermark stays put; its interruption instant stands in
        # for display ordering only.
        finished = _parse_iso_utc(rec.get("interruptedAt"))
    if finished is None:
        return None
    started = _parse_iso_utc(rec.get("started_at"))
    if output is None:
        output = JobOutputStream()
        output.closed = True
    outcome = rec.get("outcome")
    exit_code = rec.get("exit_code")
    fail_reason = rec.get("fail_reason")
    skip_reason = rec.get("skip_reason")
    return JobRunInfo(
        # an absent/corrupt outcome must NOT rehydrate as a fabricated
        # "success" (it would skew stats and could open the depends-on-past
        # gate); "unknown" is skipped by every outcome-sensitive consumer.
        outcome=outcome if isinstance(outcome, str) else "unknown",
        exit_code=exit_code if isinstance(exit_code, int) else None,
        started_at=started,
        finished_at=finished,
        fail_reason=fail_reason if isinstance(fail_reason, str) else None,
        skip_reason=skip_reason if isinstance(skip_reason, str) else None,
        output=output,
        # ResourceUsage.from_dict tolerates absent/foreign "resources" fields
        # (returns None), so a pre-monitoring or hand-edited record rehydrates
        # cleanly with no resource stats.
        resource_usage=ResourceUsage.from_dict(rec.get("resources")),
    )


@lru_cache(maxsize=1)
def load_index_html() -> str:
    """Return the bundled single-page web UI, cached after first load.

    Read from package data so it works identically for pip installs and the
    PyInstaller binary; falls back to a path relative to this module if the
    importlib.resources lookup is unavailable.
    """
    try:
        return (
            importlib.resources.files("cronstable.web")
            .joinpath("index.html")
            .read_text(encoding="utf-8")
        )
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(
            os.path.join(here, "web", "index.html"), encoding="utf-8"
        ) as fobj:
            return fobj.read()


@lru_cache(maxsize=1)
def _index_document() -> tuple[bytes, str]:
    """The dashboard as ``(utf-8 bytes, ETag)``, built once.

    ``load_index_html`` caches the DECODE; this caches the encode, which
    aiohttp's ``text=`` argument would otherwise redo per response. The tag
    is fixed because the document is static package data with no per-request
    or per-viewer content.
    """
    raw = load_index_html().encode("utf-8")
    return raw, '"' + hashlib.sha256(raw).hexdigest()[:32] + '"'


@lru_cache(maxsize=1)
def _index_gzip() -> bytes:
    """The dashboard pre-compressed once, for clients that accept gzip.

    NOTE the cache chain is three deep: ``load_index_html`` (decode),
    ``_index_document`` (encode plus ETag) and this (compression). They are
    independent lru_caches, so a test that swaps the page out must clear
    ALL THREE to change what GET / actually serves.
    """
    return _gzip_body(_index_document()[0])


def schedule_str(job: JobConfig) -> str:
    """Human-readable schedule for the web UI (the original config form).

    The web/prometheus-facing name for :func:`cronstable.job.schedule_string`
    (kept for its established importers); one implementation, so the status
    payload, prometheus and the reporters can never render an object-form
    schedule differently.
    """
    return schedule_string(job)


def _json_response(
    payload: Any,
    *,
    status: int = 200,
    headers: Optional[Any] = None,
) -> web.Response:
    """A JSON ``web.Response`` serialized with the orjson-accelerated encoder.

    A web response is transient, never a durable cross-fleet record, so the
    encode is ``trusted`` and a non-portable value falls back to the stdlib
    instead of 500ing; the stdlib flavour raises bare ``ValueError`` on
    non-finite floats, hence the wide except. The endpoint's own
    Content-Type wins over configured ``web.headers`` (the _api_error rule).
    """
    try:
        body = _json.dumps_bytes(payload, trusted=True)
    except (_json.UnsupportedValue, ValueError, TypeError):
        body = json.dumps(payload, default=str).encode("utf-8")
    if headers is not None:
        headers = _strip_content_type(headers)
    return web.Response(
        body=body,
        status=status,
        headers=headers,
        content_type="application/json",
    )


#: Job count from which /jobs computes its ETag and body on the executor;
#: below it the thread hop costs more than the encode.
_JOBS_SERIALIZE_OFFLOAD_MIN = 200

#: How long one built /jobs response is shared across pollers. Local
#: changes bust the memo; the TTL only bounds foreign staleness.
_JOBS_RESPONSE_TTL = 1.0

#: Config-source count from which the stat fingerprint runs on the
#: executor (os.stat on a network mount is milliseconds).
_CONFIG_SIGNATURE_OFFLOAD_MIN = 16

#: Resident job count at or above which a reload pauses the garbage collector
#: for the reparse.  See :meth:`Cron._quiet_gc_for_reparse`.
_GC_QUIET_RELOAD_MIN = 5000

#: Job count from which /metrics renders on the executor; the family build
#: stays on the loop (it reads live scheduler state), the render is pure
#: CPU over that snapshot.
_METRICS_OFFLOAD_MIN_JOBS = 250

#: How long one rendered /metrics product is shared across scrapers. Safe:
#: Prometheus timestamps samples at SCRAPE time.
_METRICS_RESPONSE_TTL = 1.0

#: How long one built /fleet product is shared; the payload is peer gossip
#: already up to a poll interval old, so a further second is invisible.
_FLEET_RESPONSE_TTL = 1.0

#: Per-node entry count from which the /fleet serialize/hash/gzip run on
#: the executor; the merge stays on the loop (live gossip state).
_FLEET_SERIALIZE_OFFLOAD_MIN = 200

#: How long one built /activity product is shared; changes already bust
#: the memo, so the TTL is a safety net kept uniform with /jobs.
_ACTIVITY_RESPONSE_TTL = 1.0


_ProductT = TypeVar("_ProductT")


class _ResponseMemo(Generic[_ProductT]):
    """State for one memoized endpoint product, and nothing else.

    A plain holder: the policy (TTL, single flight, the generation guard)
    lives in :meth:`Cron._shared_response_product`, the only writer besides
    :meth:`Cron._bust_response_memos` clearing ``cached``.
    """

    __slots__ = ("cached", "inflight", "inflight_gen")

    def __init__(self) -> None:
        # (loop.time stamp, product) of the newest stored build, or None
        self.cached: Optional[tuple[float, _ProductT]] = None
        # the in-flight build's future, for followers to join
        self.inflight: Optional["asyncio.Future[Optional[_ProductT]]"] = None
        # the _memo_gen the in-flight build registered under; a joiner
        # compares it against the current one before trusting the product
        self.inflight_gen = 0

    def finish(
        self,
        fut: "asyncio.Future[Optional[_ProductT]]",
        product: Optional[_ProductT],
    ) -> None:
        """Release the single-flight slot and wake this build's followers.

        It deregisters only while the slot is still ours, the same rule
        :meth:`Cron._install_tail_task`'s done-callback follows, so a
        build started after ours (possible once ours has failed) is never
        evicted by our cleanup.
        """
        if self.inflight is fut:
            self.inflight = None
        if not fut.done():
            fut.set_result(product)


def _etag_matches(header: Optional[str], etag: str) -> bool:
    """Whether an ``If-None-Match`` header carries ``etag``.

    Handles the comma-separated list form, the ``*`` wildcard, and a
    ``W/`` weak-validator prefix a cache may echo (``If-None-Match`` uses
    the weak comparison, so a strong/weak difference on the same opaque
    value still matches).
    """
    if not header:
        return False
    for token in header.split(","):
        token = token.strip()
        if token == "*":
            return True
        if token.startswith("W/"):
            token = token[2:]
        if token == etag:
            return True
    return False


def _accepts_gzip(header: Optional[str]) -> bool:
    """Whether a client's ``Accept-Encoding`` positively allows gzip.

    A bare substring test would also match ``gzip;q=0``, which is the wire
    spelling for "explicitly NOT gzip".
    """
    if not header:
        return False
    for part in header.split(","):
        name, _, params = part.strip().lower().partition(";")
        if name.strip() != "gzip":
            continue
        for param in params.split(";"):
            key, _, value = param.partition("=")
            if key.strip() == "q":
                try:
                    return float(value.strip()) > 0.0
                except ValueError:
                    return True
        return True
    return False


#: Body size at or above which a gzip-capable client gets a compressed
#: response.  Below it the framing overhead is most of the payload.
_GZIP_MIN_BYTES = 1024


def _gzip_body(body: bytes) -> bytes:
    """``body`` as a gzip stream, level 1.

    Level 1 on purpose: the payloads are highly repetitive JSON, and
    higher levels cost multiples of the CPU for little gain. ``wbits=31``
    wraps the deflate stream in a gzip container, so zlib suffices.
    """
    packer = zlib.compressobj(1, zlib.DEFLATED, 31)
    return packer.compress(body) + packer.flush()


def _jobs_response_product(
    payload: list[dict[str, Any]],
    next_fire: dict[str, datetime.datetime],
) -> tuple[str, bytes, Optional[bytes]]:
    """The full /jobs response product: ETag, body, and gzipped body.

    The tag hashes a CANONICAL variant with the volatile relative
    ``scheduled_in`` swapped for its stable absolute next-fire instant, so
    it changes exactly when the displayed data changes, not per countdown
    tick. Pure and free of scheduler state, so it can run on an executor.

    ``trusted=True`` and SHA-256 deliberately differ from durable-record
    rules: two builds disagreeing degrades a 304 into a 200, never a wrong
    answer. The body carries no per-request secret, so BREACH/CRIME has
    nothing to leak; the If-None-Match check stays in the handler because
    one product is shared across every concurrent poller.
    """
    canonical = [
        {**job, "scheduled_in": next_fire.get(job["name"])} for job in payload
    ]
    try:
        raw = _json.dumps_bytes(canonical, trusted=True)
    except (_json.UnsupportedValue, ValueError, TypeError):
        # the stdlib flavour cannot render a datetime, so a no-orjson
        # install always lands here; determinism is all the tag needs.
        raw = json.dumps(canonical, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    etag = '"' + hashlib.sha256(raw).hexdigest()[:32] + '"'
    try:
        body = _json.dumps_bytes(payload, trusted=True)
    except (_json.UnsupportedValue, ValueError, TypeError):
        body = json.dumps(payload, default=str).encode("utf-8")
    if len(body) >= _GZIP_MIN_BYTES:
        return etag, body, _gzip_body(body)
    return etag, body, None


def _conditional_response(
    etag: Optional[str],
    body: bytes,
    gz: Optional[bytes],
    *,
    if_none_match: Optional[str],
    gzip_ok: bool,
    headers: Optional[Any] = None,
) -> web.Response:
    """The ONE conditional-serve tail for the memoized JSON endpoints.

    Strip the operator Content-Type, tag, ``Vary``, the If-None-Match
    compare, then the gzip pick: shared by ``/jobs``, ``/fleet`` and
    everything built on :func:`_cachable_json_response`, so the steps
    cannot drift apart per endpoint.  ``etag=None`` means never tag and
    never 304 (the ``/cluster`` case, whose payload changes every poll).
    """
    hdrs = _strip_content_type(headers)
    # on EVERY representation, compressed or not: a shared cache that
    # missed this would hand a gzipped body to a client that cannot read
    # one.
    hdrs["Vary"] = "Accept-Encoding"
    if etag is not None:
        hdrs["ETag"] = etag
        if _etag_matches(if_none_match, etag):
            return web.Response(status=304, headers=hdrs)
    if gzip_ok and gz is not None:
        hdrs["Content-Encoding"] = "gzip"
        body = gz
    return web.Response(
        body=body,
        status=200,
        headers=hdrs,
        content_type="application/json",
    )


def _cachable_json_response(
    payload: Any,
    *,
    if_none_match: Optional[str],
    gzip_ok: bool,
    headers: Optional[Any] = None,
    use_etag: bool = True,
) -> web.Response:
    """A :func:`_json_response` that can 304 and gzip, for the poll fan-out.

    Unmemoized composition of :func:`_cachable_json_product` and
    :func:`_conditional_response`. ``use_etag=False`` is for a payload
    that legitimately changes every request (``/cluster`` embeds freshly
    sampled CPU/memory), where a tag would never match; such a response
    still negotiates gzip.
    """
    etag, body, gz = _cachable_json_product(payload)
    return _conditional_response(
        etag if use_etag else None,
        body,
        gz,
        if_none_match=if_none_match,
        gzip_ok=gzip_ok,
        headers=headers,
    )


def _cachable_json_product(
    payload: Any,
) -> tuple[str, bytes, Optional[bytes]]:
    """A memoized JSON endpoint's product: ETag, body, and gzipped body.

    Plain sibling of :func:`_jobs_response_product`: the tag hashes the
    body bytes, so a 304 is served only for a byte-identical
    representation. Built once per memo window, however many pollers land.
    """
    try:
        body = _json.dumps_bytes(payload, trusted=True)
    except (_json.UnsupportedValue, ValueError, TypeError):
        body = json.dumps(payload, default=str).encode("utf-8")
    etag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
    gz = _gzip_body(body) if len(body) >= _GZIP_MIN_BYTES else None
    return etag, body, gz


def _metrics_response_product(
    render: Callable[..., str],
    families: Any,
    openmetrics: bool,
) -> tuple[bytes, Optional[bytes]]:
    """The full /metrics response product: body bytes and gzipped body.

    Pure over ``families`` (a freshly built list referenced by nobody
    else), so a large job set runs render AND compression in one executor
    hop. Gzip is ``None`` below the floor.
    """
    body = render(families, openmetrics=openmetrics).encode("utf-8")
    return body, _gzip_body(body) if len(body) >= _GZIP_MIN_BYTES else None


def _sse_frame(stream_name: str, line: str) -> bytes:
    """One ``event: line`` SSE frame, built in bytes throughout.

    ``trusted=True`` is sound by construction rather than by vouching for
    the job's output: both values are ``str``, and the portability walk only
    ever rejects non-finite floats.
    """
    return (
        b"event: line\ndata: "
        + _json.dumps_bytes(
            {"stream": stream_name, "line": line.rstrip("\n")}, trusted=True
        )
        + b"\n\n"
    )


def naturaltime(seconds: float) -> str:
    # only ever used to describe a future instant ("in N seconds")
    if seconds < 120:
        return "in {} second{}".format(
            int(seconds), "s" if seconds >= 2 else ""
        )
    minutes = seconds / 60
    if minutes < 120:
        return "in {} minute{}".format(
            int(minutes), "s" if minutes >= 2 else ""
        )
    hours = minutes / 60
    if hours < 48:
        return "in {} hour{}".format(int(hours), "s" if hours >= 2 else "")
    days = hours / 24
    return "in {} day{}".format(int(days), "s" if days >= 2 else "")


def get_now(timezone: Optional[datetime.tzinfo]) -> datetime.datetime:
    return datetime.datetime.now(timezone)


async def _noop_state_write() -> None:
    """Placeholder body for a shed durable write (see _track_state_write).

    Returned as an already-scheduled, immediately-completing task so callers
    that chain on the tracked-write result behave identically whether or not
    the real write was tracked.
    """
    return None


def next_sleep_interval(subminute: bool = False) -> float:
    """Seconds to sleep until the next scheduling tick.

    Minute mode (``subminute`` False, the default) snaps to the top of the
    next minute.  When any enabled job pins specific
    seconds the scheduler switches to ``subminute`` mode and snaps to the next
    whole-second boundary instead, so a second-level schedule can fire on time.
    """
    now = get_now(datetime.timezone.utc)
    if subminute:
        target = now.replace(microsecond=0) + datetime.timedelta(seconds=1)
    else:
        target = now.replace(second=0) + WAKEUP_INTERVAL
    return (target - now).total_seconds()


def schedule_slot(
    job: JobConfig, now: Optional[datetime.datetime] = None
) -> datetime.datetime:
    """The scheduling instant to test ``job`` against on this tick.

    Truncated to the job's own resolution: the whole second for a
    second-level job (``has_seconds``), otherwise the top of the minute.
    Used both to decide whether the job is due (:meth:`Cron.job_should_run`)
    and to de-duplicate launches (:meth:`Cron.spawn_jobs`): microseconds are
    always zeroed so two ticks within one slot compare equal and the job
    fires once.

    ``now`` is the pass instant supplied by :meth:`Cron._service_slots` (a
    timezone-aware UTC datetime).  Passing it means the whole pass reads the
    clock ONCE: the same instant decides "due" and is recorded for de-dup, so
    the two cannot straddle a slot boundary and double-launch a single-slot
    job.  It is rendered into the job's own frame first: an explicit timezone
    via ``astimezone``, or local time (naive, matching ``get_now(None)``) for
    a job without one.  ``now`` omitted reads the clock fresh per job.
    """
    if now is None:
        now = get_now(job.timezone)
    elif job.timezone is not None:
        now = now.astimezone(job.timezone)
    else:
        # no explicit timezone -> local wall clock, naive, exactly as
        # get_now(None) (datetime.now(None)) would have returned.
        now = now.astimezone().replace(tzinfo=None)
    if job.has_seconds:
        return now.replace(microsecond=0)
    return now.replace(second=0, microsecond=0)


#: Built once, on the first web start (see :func:`_access_log_class`).
_ACCESS_LOG_CLASS: Optional[type] = None


def _redact_query_token(path_qs: str) -> str:
    """``path_qs`` with any ``token`` query value replaced by ``***``.

    The auth middleware lets the web bearer token ride a ``token`` query
    parameter on the calendar-feed paths alone, because a calendar client
    cannot attach an Authorization header, and the dashboard mints exactly
    that URL for an operator to subscribe with.  aiohttp's access log
    renders ``request.path_qs``, so without this every poll of the feed
    (calendar apps refresh on their own cadence, hourly or faster) would
    write a live token into the log at INFO.
    """
    path, sep, query = path_qs.partition("?")
    if not sep:
        return path_qs
    parts = []
    for item in query.split("&"):
        key, eq, _ = item.partition("=")
        parts.append("token=***" if eq and key == "token" else item)
    return path + sep + "&".join(parts)


class _RedactedRequest:
    """A read-only view of a request with a scrubbed ``path_qs``.

    Everything except ``path_qs`` is forwarded to the real request, so
    aiohttp's other log directives (``%a`` reads ``remote``, ``%{...}i``
    reads ``headers``) see exactly what they always did.  A proxy rather
    than ``BaseRequest.clone``: clone refuses a request whose body has
    been read, and rebuilding a whole request per logged line is far more
    work than swapping one string.
    """

    __slots__ = ("_request", "path_qs")

    def __init__(self, request: Any, path_qs: str) -> None:
        self._request = request
        self.path_qs = path_qs

    def __getattr__(self, name: str) -> Any:
        return getattr(self._request, name)


def _access_log_class() -> type:
    """aiohttp's access logger, with the feed token redacted.

    Built lazily and cached: importing ``aiohttp.web_log`` at module scope
    would defeat the lazy aiohttp door above.

    The hook must be ``log``: overriding ``_format_r`` does not work because
    ``AccessLogger.compile_format`` resolves directives against the base
    class and memoizes them, so a subclass's ``_format_r`` is never called.
    Redacting the request before delegating survives that internal.
    """
    global _ACCESS_LOG_CLASS
    if _ACCESS_LOG_CLASS is None:
        from aiohttp.web_log import AccessLogger

        class _RedactingAccessLogger(AccessLogger):  # type: ignore[misc]
            def log(self, request: Any, response: Any, time: float) -> None:
                path_qs = getattr(request, "path_qs", "")
                if "token=" in path_qs:
                    request = _RedactedRequest(
                        request, _redact_query_token(path_qs)
                    )
                super().log(request, response, time)

        _ACCESS_LOG_CLASS = _RedactingAccessLogger
    return _ACCESS_LOG_CLASS


def web_site_from_url(
    runner: web.AppRunner,
    url: str,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> web.BaseSite:
    """One listener for ``url``, TLS-wrapped when the url says ``https``.

    ``ssl_context`` is built once per app (re)start and shared by every
    ``https://`` entry; it is applied per *site*, not per runner, so a listen
    list mixing ``http://`` and ``https://`` serves the same app plaintext on
    one port and over TLS on another. ``unix://`` listeners are always
    plaintext: they are already confined to the host's filesystem, where the
    socket's own permissions (``web.socketMode``) are the access control.
    """
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        if parsed.hostname is None or parsed.port is None:
            # raise ValueError (not AssertionError) so a malformed url is
            # treated as a skippable bad-config entry, not an internal bug.
            # An explicit port is required for https too: aiohttp would
            # otherwise silently default a TLS site to 8443, which is not
            # what an operator who typed "https://0.0.0.0" meant.
            logger.warning(
                "Ignoring web listen url %s: %s url needs host and port",
                url,
                parsed.scheme,
            )
            raise ValueError(url)
        if parsed.scheme == "https" and ssl_context is None:
            # Config validation normally catches this (config._validate_web_tls
            # refuses an https listen with no web.tls), so reaching here means
            # the context failed to BUILD. Skip the listener; serving it in
            # cleartext on the port an operator asked to encrypt would be the
            # one failure mode worse than not serving it.
            logger.warning(
                "Ignoring web listen url %s: no usable web.tls material for "
                "an https listener",
                url,
            )
            raise ValueError(url)
        return web.TCPSite(
            runner,
            parsed.hostname,
            parsed.port,
            ssl_context=ssl_context if parsed.scheme == "https" else None,
        )
    elif parsed.scheme == "unix":
        if not platform.supports_unix_sockets():
            # asyncio's Windows Proactor loop can't serve a unix socket; skip
            # this listener (a skippable bad-config entry) rather than crash.
            logger.warning(
                "Ignoring web listen url %s: unix-socket listeners are not "
                "supported on this platform",
                url,
            )
            raise ValueError(url)
        return web.UnixSite(runner, parsed.path)
    else:
        logger.warning(
            "Ignoring web listen url %s: scheme %r not supported",
            url,
            parsed.scheme,
        )
        raise ValueError(url)


class Cron:
    def __init__(
        self, config_arg: Optional[str], *, config_yaml: Optional[str] = None
    ) -> None:
        # Prometheus accumulators (GET /metrics); owned here so counters
        # survive web-app restarts, and created before update_config.
        self.metrics = PrometheusMetrics()
        # whole-node CPU/memory sampler; long-lived so its CPU% counters
        # stay primed. Yields None without psutil.
        self._node_sampler = NodeResourceSampler()
        # list of cron jobs we /want/ to run
        self.cron_jobs: dict[str, JobConfig] = OrderedDict()
        # orchestration DAGs; empty keeps the classic no-DAG behaviour.
        self.cron_dags: dict[str, DagConfig] = OrderedDict()
        # Memo caches (these four plus _memo_gen and friends below): pure
        # functions of cron_jobs, computed lazily; ALL must be invalidated
        # at every point cron_jobs is reassigned (reload).
        self._job_set_id_cache: str | None = None
        self._needs_subminute_cache: bool | None = None
        self._job_pos_cache: dict[str, int] | None = None
        self._any_sla_cache: bool | None = None
        # list of cron jobs already running
        # name -> list of RunningJob
        self.running_jobs: dict[str, list[RunningJob]] = defaultdict(list)
        # name -> lock serialising maybe_launch_job per job: the
        # Forbid/Replace gate reads running_jobs several awaits before the
        # launch appends, so unserialized entries could double-launch.
        # Pruned on reload, never while held.
        self._launch_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # name -> last launched slot, status/introspection only: the
        # forward-only next-fire index is what de-duplicates launches.
        self._last_run_slot: dict[str, datetime.datetime] = {}
        # most recent finished run / bounded history per job (in-memory
        # only); pruned on reload and initialized HERE, before the first
        # update_config() runs _apply_reload.
        self.last_run: dict[str, JobRunInfo] = {}
        self.run_history: dict[str, deque[JobRunInfo]] = defaultdict(
            lambda: deque(maxlen=RUN_HISTORY_LIMIT)
        )
        # bounds concurrently-executing subprocess spawns (job and DAG-task
        # launches share it); see _SPAWN_BURST_LIMIT for the why.
        self._spawn_gate = asyncio.Semaphore(_SPAWN_BURST_LIMIT)
        # Per-endpoint response-product memos; policy lives in
        # _shared_response_product, busting in _bust_response_memos.
        # /metrics keeps one memo per exposition format.
        self._jobs_response_memo: _ResponseMemo[
            tuple[str, bytes, Optional[bytes]]
        ] = _ResponseMemo()
        self._metrics_response_memo: dict[
            bool, "_ResponseMemo[tuple[bytes, Optional[bytes]]]"
        ] = {False: _ResponseMemo(), True: _ResponseMemo()}
        self._fleet_response_memo: _ResponseMemo[
            tuple[str, bytes, Optional[bytes]]
        ] = _ResponseMemo()
        self._activity_response_memo: _ResponseMemo[
            tuple[str, bytes, Optional[bytes]]
        ] = _ResponseMemo()
        # Bumped by every _bust_response_memos call; builds re-check it
        # before storing and joiners before trusting a shared product, so a
        # bust landing DURING a slow build cannot be undone by the pre-bust
        # product arriving late.
        self._memo_gen = 0
        # active runtime pauses by name; the fire path consults ONLY this
        # map (never store I/O there). Rehydrated at boot, refreshed on
        # housekeeping; expired entries ignored by readers. Pruned by
        # _apply_reload, so it must exist before update_config() below.
        self._paused: dict[str, PauseInfo] = {}
        # monotonic count of local pause/resume writes per job: edge-
        # triggered so the refresh can discard a snapshot taken before a
        # local change landed (the write tail alone cannot show one that
        # started and finished inside the read).
        self._pause_gen: dict[str, int] = {}
        # pause records unwritten because the store is down, newest per
        # job, replayed when it returns (_defer_pause_write). Pruned by
        # _apply_reload, so must exist before update_config().
        self._pause_pending_writes: dict[str, dict[str, Any]] = {}
        # name -> (finished_at, outcome) of the newest success/failure, for
        # the onlyIfLastSucceeded gate: run_history is a bounded ring a
        # long pause floods with synthetic "skipped" rows, so the gate
        # cannot rely on it alone. Must exist before update_config().
        self._last_real_outcome: dict[str, tuple[datetime.datetime, str]] = {}
        # name -> (monotonic deadline, payload): trends cache; busted per
        # job by _record_run so it never outlives a locally finished run.
        self._trends_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        # name -> finished_at of the newest ACTUAL run (not a synthetic
        # "skipped"), for the retry ladder's superseded-by-run guards: a
        # pause-held slot stamps a fresh finished_at on a row where nothing
        # ran. Must exist before update_config().
        self._last_completed_at: dict[str, datetime.datetime] = {}
        # SLA monitor trackers (_sla_periodic); all pruned by
        # _apply_reload, so all must exist before update_config() below.
        # name -> newest known success (the maxTimeSinceSuccess reference);
        # no entry baselines on _process_start so a stateless boot never
        # pages instantly.
        self._sla_last_success: dict[str, datetime.datetime] = {}
        # name -> the newest scheduling slot recorded while the job was NOT
        # paused (pause-skipped slots are excused from lateAfter).
        self._sla_due: dict[str, datetime.datetime] = {}
        # name -> instant of the newest actual launch; any launch at/after
        # the due slot clears the lateAfter breach condition.
        self._sla_last_start: dict[str, datetime.datetime] = {}
        # (name, check) -> instant the breach was first seen: the latch.
        # Present = breached (onLate already fired once); absent = ok.
        # In-memory only, so a restart re-fires a still-breached check once.
        self._sla_state: dict[tuple[str, str], datetime.datetime] = {}
        # the maxTimeSinceSuccess fallback baseline (see _sla_last_success).
        self._process_start = get_now(datetime.timezone.utc)
        # name -> when the job first entered cron_jobs: the staleness
        # reference for a reload-added job, which must age from when it
        # appeared, not a process start it predates.
        self._sla_first_seen: dict[str, datetime.datetime] = {}
        # name -> finished pause windows (disjoint, newest last), credited
        # against maxTimeSinceSuccess so a resumed job gets a full
        # threshold before it can page.
        self._sla_pause_windows: dict[
            str, list[tuple[datetime.datetime, datetime.datetime]]
        ] = {}
        # name -> start of the current DISABLED span; banked as staleness
        # credit on re-enable (via _sla_bank_pause). Node-local.
        self._sla_disabled_since: dict[str, datetime.datetime] = {}
        # Next-fire index: name -> aware-UTC next fire for every enabled
        # CronTab job. _fire_heap is a min-heap over the same data and may
        # hold STALE entries (names reseeded/removed on reload), validated
        # lazily against _next_fire on pop.
        self._next_fire: dict[str, datetime.datetime] = {}
        self._fire_heap: list[tuple[datetime.datetime, str]] = []
        # names already warned about a no-future-occurrence schedule (warn
        # once); pruned on reload so a changed schedule re-warns.
        self._dead_schedules: set[str] = set()
        # wall-clock minute of the last housekeeping pass; gates that work
        # to once per minute even when sub-minute jobs wake the loop.
        self._last_housekeeping_minute: Optional[datetime.datetime] = None
        # Config-reload skip cache: sources, stat fingerprint, and the
        # config produced; reload_config skips the reparse when the
        # fingerprint is unchanged.
        self._config_sources: frozenset[str] = frozenset()
        self._config_sig: Optional[tuple] = None
        self._last_config: Optional[CronstableConfig] = None
        # the optional `notify:` block; None keeps job-runs-only reporting.
        # Set in both the config_yaml (test) path below and _apply_reload.
        self._notify_config: Optional[dict[str, Any]] = None
        # the `push:` section's running service and its source config,
        # managed by start_stop_push; also published module-globally
        # (push.set_service) for the stateless reporter singletons.
        self._push_service: Optional[push.PushService] = None
        self._applied_push_config: Optional[dict[str, Any]] = None
        # the opt-in Bonjour/mDNS advert; follows the web app's lifecycle.
        self._bonjour = discovery.BonjourAdvertiser()
        # cluster-wide concurrency slots: lease per running slot-gated job,
        # renew task, per-job claim/release lock (an unserialized release
        # racing the next claim could revoke it), and single-flight Replace
        # pursuits. Declared ABOVE the config load: _apply_reload's prune
        # block reads all five.
        self._slot_leases: dict[str, Lease] = {}
        self._slot_renewers: dict[str, asyncio.Task] = {}
        self._slot_locks: dict[str, asyncio.Lock] = {}
        self._slot_pursuits: dict[str, asyncio.Task] = {}
        # live users of each job's slot; the lease is released only at
        # zero. A plain running_jobs check would race the window between a
        # claim succeeding and its RunningJob being registered.
        self._slot_refs: dict[str, int] = {}
        self.config_arg = config_arg
        if config_arg is not None:
            self.update_config()
        if config_yaml is not None:
            # config_yaml is for unit testing
            config = parse_config_string(config_yaml, "")
            self.cron_jobs = OrderedDict(
                (job.name, job) for job in config.jobs
            )
            self.cron_dags = OrderedDict((d.name, d) for d in config.dags)
            self._notify_config = config.notify_config
            self._job_set_id_cache = None
            self._needs_subminute_cache = None
            self._job_pos_cache = None
            self._any_sla_cache = None

        self._wait_for_running_jobs_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._jobs_running = asyncio.Event()
        self.retry_state: dict[str, JobRetryState] = {}
        self.web_runner: web.AppRunner | None = None
        self.web_config: WebConfig | None = None
        # (scheme, socket name) per bound TCP listener of the RUNNING web
        # app, in listen order: the Bonjour advert needs a port, scheme and
        # address from ONE listener, unreconstructable after the fact
        # (runner.addresses has no schemes and skips failed binds).
        self._web_tcp_bound: list[tuple[str, Any]] = []
        # fingerprint of the web.tls files as the RUNNING listener loaded
        # them (an in-place rotation is otherwise invisible); cleared on
        # every teardown. See _web_tls_files_changed.
        self._web_tls_signature: dict[str, Any] | None = None
        # the optional MCP config and handler; (re)built inside
        # start_stop_web_app so they track reloads.
        self.mcp_config: MCPConfig | None = None
        self._mcp: Any | None = None
        # live SSE log tails, ended (via the sentinel) BEFORE aiohttp's
        # shutdown wait: a tail never finishes on its own and would freeze
        # teardown for the full 60s timeout. _web_draining refuses tails
        # arriving mid-teardown.
        self._web_sse_queues: "set[asyncio.Queue]" = set()
        self._web_draining = False
        # the leadership backend, when a cluster section is configured
        self.cluster_manager: Optional[LeadershipBackend] = None
        # optional election-inert second gossip manager so non-gossip
        # clusters can share fleet data; None when unused (with backend:
        # gossip the election mesh already IS the fleet backend).
        self.observability_mesh: Optional[LeadershipBackend] = None
        # durable state backend when a `state` section is configured; None
        # keeps the classic stateless behaviour.
        self.state_backend: Optional[StateBackend] = None
        # in-flight fire-and-forget durable writes, tracked so they are not
        # GC'd mid-flight and can be flushed at shutdown; durability never
        # gates the loop.
        self._pending_state_writes: set[asyncio.Task] = set()
        # in-flight notify fan-outs, fire-and-forget (a slow reporter must
        # never stall the loop); cancelled at shutdown.
        self._notify_tasks: set[asyncio.Task] = set()
        # in-flight completion sequences of finished jobs; drained at
        # shutdown and chained per job via _completion_tail so overlapping
        # instances are handled in finish order.
        self._completion_tasks: set[asyncio.Task] = set()
        self._completion_tail: dict[str, asyncio.Task] = {}
        # onLate reports get their OWN per-job tail: ordered behind
        # _completion_tail but never BECOMING it, or a slow reporter would
        # delay a real run's report and retry arming.
        self._sla_report_tail: dict[str, asyncio.Task] = {}
        # whether the in-memory history has been warmed from the durable ledger
        # yet; rehydration runs once, on the first successful backend start.
        self._state_rehydrated = False
        # how many finished runs to retain per job in the durable ledger; set
        # from state.maxRunsPerJob when the backend starts. <= 0 disables.
        self._state_max_runs = 0
        # whether missed-run catch-up has fully resolved; NOT latched while
        # it cannot actually run yet (backend retrying, no positive cluster
        # owner), or the owed backfill would be forfeited. See _catch_up.
        self._caught_up = False
        # names whose catch-up decision is final, so unresolved jobs
        # elsewhere do not re-process them next pass.
        self._catchup_done: set[str] = set()
        # loop-clock gate for re-evaluating unresolved catch-up (see
        # CATCHUP_RECHECK_INTERVAL); 0.0 means "evaluate on the next pass".
        self._catchup_next_retry = 0.0
        # instant of the FIRST catch-up evaluation: deferred retries count
        # missed slots against this, not a later "now" (the live scheduler
        # ran statelessly in between).
        self._catchup_reference: Optional[datetime.datetime] = None
        # in-flight catch-up evaluation; a background task, never inline
        # (a slow mount must degrade catch-up, not delay launches).
        self._catchup_eval_task: Optional[asyncio.Task] = None
        # whether the loaded config HAS a state section, so catch-up can
        # tell "no durability configured" (latch and warn) from
        # "configured but not started yet" (retry).
        self._state_configured = False
        # effective state.onStoreUnavailable: "degrade" (gates fail open,
        # writes drop with a warning) or "fail-closed" (durable-truth gates
        # prefer not running); reset when the section is removed.
        self._state_on_unavailable = "degrade"
        # effective state.gcGraceSeconds; <= 0 disables automatic GC.
        self._state_gc_grace = 0.0
        # host tag for host-scoped durable streams; stable across restarts
        # (unlike the backend's per-process instance id) so a restarted
        # daemon reclaims its own records.
        self._state_host = socket.gethostname() or "localhost"
        # loop-clock instant before which the next durable counter snapshot
        # is skipped (see COUNTER_SNAPSHOT_INTERVAL).
        self._counter_snapshot_next = 0.0
        # whether this PROCESS already seeded the Prometheus accumulators
        # from a durable snapshot. Never reset: seeding ADDS into live
        # counters, so a second seed would double-count.
        self._counters_seeded = False
        # loop-clock instants the next manifest write / GC pass are due.
        self._manifest_next = 0.0
        self._gc_next = 0.0
        # loop-clock instants the next paused/ refresh and foreign-retry
        # claim scan are due (single-flight alone is not a cadence).
        self._pause_refresh_next = 0.0
        self._retry_claim_next = 0.0
        # the in-flight GC pass, if any (single-flight; a slow store must
        # not stack passes).
        self._gc_task: Optional[asyncio.Task] = None
        # newest in-flight retry-ladder write per job, so a settle chains
        # after its pending: unordered appends could land inverted and
        # resurrect a consumed retry on the next boot.
        self._retry_write_tail: dict[str, asyncio.Task] = {}
        # newest in-flight pause write per job (same ordering rationale);
        # also consulted by the refresh, so a store re-read cannot clobber
        # a local change whose write has not landed yet.
        self._pause_write_tail: dict[str, asyncio.Task] = {}
        # the in-flight housekeeping refresh of the paused/ streams, if any
        # (single-flight; a slow store must not stack refresh passes).
        self._pause_refresh_task: Optional[asyncio.Task] = None
        # same ordering guard for the inflight/ stream: a near-instant
        # run's close could sort BEFORE its open, which the next restart
        # would reconcile as a spurious interrupted run.
        self._inflight_write_tail: dict[str, asyncio.Task] = {}
        # latched when a @reboot marker op times out during startup, so the
        # remaining @reboot jobs apply the policy without more I/O instead
        # of serially stalling the first pass on a hung mount.
        self._reboot_gate_sick = False
        # in-flight catch-up launch tasks, tracked so they are not GC'd and
        # can be cancelled on shutdown.
        self._catchup_tasks: set[asyncio.Task] = set()
        # last job-set id we logged, so reloads only log it again on change
        self._logged_job_set_id: str | None = None
        # whether the loaded config asks us to gate jobs on leader election;
        # tracked separately from cluster_manager so the gate can fail closed
        # even when the manager failed to start.
        self._elect_leader_configured = False
        # last leadership state we logged, so we only log on transition
        self._was_leader = False
        # last quorum-membership state we logged; tracked on every node so a
        # follower losing quorum logs it too (not just the ex-leader)
        self._was_quorate = False
        # last duplicate-nodeName state we logged, so we only log on transition
        self._was_conflict = False
        # last cluster-size-disagreement state we logged (same rationale)
        self._was_size_conflict = False
        # last coordination-policy-divergence state we logged (same rationale)
        self._was_policy_conflict = False
        # @reboot Leader/PreferLeader jobs that could not start at boot because
        # the cluster had not yet elected an owner; run once on convergence.
        # name -> JobConfig; see _process_pending_reboots.
        self._pending_reboot_jobs: dict[str, JobConfig] = {}
        # @reboot runs a pause DEFERRED (never forfeited): the marker stays
        # unwritten so the run is still owed after a same-boot restart;
        # _process_paused_reboots fires it once the pause lifts.
        self._paused_reboot_jobs: set[str] = set()
        # per-PROCESS token stamped into in-flight run records: the state
        # backend's instance id will not do, since a state-section reload
        # rebuilds the backend while this process's runs are still live.
        self._proc_token = os.urandom(6).hex()
        # effective state.slotTtlSeconds while a state section is configured
        self._slot_ttl = 30.0
        # lock-fidelity latch for the slot gate: None until probed, "" when
        # locks behave, else the reason they cannot be trusted. Reset when
        # the backend is rebuilt.
        self._slot_fidelity: Optional[str] = None
        # the in-flight cross-node retry claim scan, if any (single-flight;
        # see _retry_claim_scan).
        self._retry_claim_task: Optional[asyncio.Task] = None
        # the loopback job-state API, built when state.jobApi starts and
        # torn down with the backend; None keeps the classic behaviour (no
        # endpoint, no injected CRONSTABLE_STATE_* env).
        self._job_api: Optional["JobStateAPI"] = None
        # job-API analogue of _web_tls_signature (same in-place-rotation
        # rationale); cleared by _stop_job_api.
        self._job_api_tls_signature: dict[str, Any] | None = None
        # the durable DAG orchestrator; inert until `dags:` plus a state
        # backend are configured. Constructed here so every path has it.
        self._dag = DagScheduler(self)
        # whether the last _sleep_interval() was shortened by a DAG wake;
        # read via _wakes_subminute. False until the first sleep, the safe
        # direction (the startup pass housekeeps unconditionally).
        self._dag_shortens_sleep = False

    async def run(self) -> None:
        self._wait_for_running_jobs_task = asyncio.create_task(
            self._wait_for_running_jobs()
        )

        startup = True
        applied_logging_config: Optional[LoggingConfig] = None
        while not self._stop_event.is_set():
            # Housekeeping runs at most once per wall-clock minute even when
            # a sub-minute schedule ticks faster. In pure minute-tick mode
            # `not _wakes_subminute()` forces it every iteration, so a
            # frozen-clock test still reloads every loop.
            now_minute = get_now(datetime.timezone.utc).replace(
                second=0, microsecond=0
            )
            # None when housekeeping is skipped or the reload failed; on
            # failure we keep running the previously loaded jobs.
            config: Optional[CronstableConfig] = None
            if (
                startup
                or not self._wakes_subminute()
                or now_minute != self._last_housekeeping_minute
            ):
                self._last_housekeeping_minute = now_minute
                try:
                    # reload_config reparses OFF the loop; the job set is
                    # applied here BEFORE _service_slots, so the cluster
                    # gate is in place before the first spawn_jobs (a
                    # reload enabling electLeader gates that same tick).
                    config = await self.reload_config()
                    self._log_job_set_id()
                    await self.start_stop_cluster(config.cluster_config)
                    # gossip observability overlay; after start_stop_cluster
                    # so the election backend exists first.
                    await self.start_stop_observability(config.cluster_config)
                    await self.start_stop_state(config.state_config)
                    # after the state backend (the device registry may ride
                    # its store); never raises, which is load-bearing:
                    # anything escaping here would skip _state_periodic on
                    # this and every later pass.
                    await self.start_stop_push(config.push_config)
                    # periodic durable-state chores (manifest, GC): cheap
                    # due-checks that spawn tracked background tasks.
                    self._state_periodic()
                except ConfigError as err:
                    logger.error(
                        "Error in configuration file(s), so not updating "
                        "any of the config.:\n%s",
                        str(err),
                    )
                except Exception:  # pragma: nocover
                    logger.exception("please report this as a bug (1)")
                self._pause_and_sla_periodic()
                if config is not None:
                    # The web app starts AFTER the cluster, under its OWN
                    # error handling: a shared try would let a web
                    # ConfigError skip start_stop_cluster and fail the
                    # Leader gate OPEN.
                    try:
                        await self.start_stop_web_app(
                            config.web_config, config.mcp_config
                        )
                    except ConfigError as err:
                        logger.error(
                            "Error in the web configuration, so not starting "
                            "the web API:\n%s",
                            str(err),
                        )
                    except Exception:  # pragma: nocover
                        logger.exception("please report this as a bug (4)")
                if (
                    config is not None
                    and config.logging_config is not None
                    and config.logging_config != applied_logging_config
                ):
                    try:
                        logging.config.dictConfig(config.logging_config)
                    except Exception as ex:
                        logger.error(
                            "Error while configuring logging: %s\n"
                            "Check for correct format at "
                            "https://docs.python.org/3/library/logging.config"
                            ".html#dictionary-schema-details\n%s",
                            ex,
                            config.logging_config,
                        )
                    else:
                        # mark applied only on success, so a fixed-after-
                        # error logging section is picked up on reload.
                        applied_logging_config = config.logging_config
            # Service the due job(s). _service_slots re-reads the clock AFTER
            # the (possibly slow) housekeeping above, so a fire the reload
            # pushed past is still serviced instead of silently dropped.
            await self._service_slots(startup)
            startup = False
            # Sleep until the soonest fire or the next housekeeping minute.
            # wait_for realizes the wall-derived length on the loop's
            # MONOTONIC clock, and next-fire instants are fixed and
            # forward-only, so an NTP step neither stretches the sleep nor
            # re-fires already-fired slots.
            sleep_interval = self._sleep_interval()
            logger.debug("Will sleep for %.1f seconds", sleep_interval)
            try:
                await asyncio.wait_for(self._stop_event.wait(), sleep_interval)
            except asyncio.TimeoutError:
                pass

        logger.info("Shutting down (after currently running jobs finish)...")
        while self.retry_state:
            # settle=None: a graceful stop must NOT settle the durable
            # ladder records; re-arming on the next boot is the point of
            # restart-durable retries.
            cancel_all = [
                self.cancel_job_retries(name, settle=None)
                for name in self.retry_state
            ]
            await asyncio.gather(*cancel_all)
        # Stop the launch-adjacent background work before the drain: a
        # Replace pursuit could otherwise LAUNCH a job mid-shutdown, and
        # the retry claim scan could arm a ladder nobody will run.
        self._cancel_coordination_tasks()
        # Release leadership BEFORE the unbounded running-job drain, or
        # every Leader job cluster-wide would stall until the slowest
        # local job finishes. Retries were all cancelled above. Cost: the
        # new owner may start a still-draining job (same overlap a crash
        # produces).
        if self.cluster_manager is not None:
            logger.info("Stopping cluster manager")
            await self.cluster_manager.stop()
            self.cluster_manager = None
        # the overlay holds no leadership, so order vs the drain does not
        # matter; stop it alongside the election manager.
        if self.observability_mesh is not None:
            logger.info("Stopping cluster observability overlay")
            await self.observability_mesh.stop()
            self.observability_mesh = None
        await self._wait_for_running_jobs_task
        # drain the reaper-spawned completion tasks so failure/success
        # reports still go out on a graceful stop.
        await self._drain_completions()
        # the drain released every slot (each finish cancels its renewer);
        # belt-and-braces for renewers whose release write raced teardown.
        for task in list(self._slot_renewers.values()):
            task.cancel()
        self._slot_renewers.clear()

        # cancel pending catch-up backfills and in-flight notifications
        # (best-effort; each reporter is time-bounded).
        for task in list(self._catchup_tasks) + list(self._notify_tasks):
            task.cancel()

        if self.state_backend is not None:
            # one last unthrottled counter snapshot; joins the pending
            # writes and is flushed (bounded) below.
            self._track_state_write(self._persist_counter_snapshot())
            # flush in-flight run-record writes, bounded so a stuck store
            # cannot hang the exit.
            if self._pending_state_writes:
                logger.info(
                    "Flushing %d pending state write(s)",
                    len(self._pending_state_writes),
                )
                await asyncio.wait(set(self._pending_state_writes), timeout=5)
            # release held DAG advance leases while the backend is still
            # up, so a peer adopts the runs at once rather than waiting
            # out a lease TTL; the runs' tasks drained above.
            await self._dag.shutdown()
            # stop the loopback job API while the backend is alive, so it
            # releases held job locks (not pinned for a whole TTL).
            await self._stop_job_api()
            logger.info("Stopping state backend")
            await self.state_backend.stop()
            self.state_backend = None

        await self._node_sampler.stop_history()
        # the mDNS advert must go before the listener it points at.
        await self._bonjour.stop()
        if self.web_runner is not None:
            logger.info("Stopping http server")
            await self.web_runner.cleanup()
        # close the pooled statsd UDP endpoints (otherwise reclaimed only
        # at loop GC, with a ResourceWarning). Safe to call twice.
        statsd.close_endpoints()
        # same for the pooled webhook connections; safe here because the
        # reports went out during _drain_completions, so the pool holds
        # only idle keepalive sockets.
        await close_webhook_pool()

    def _cancel_coordination_tasks(self) -> None:
        """Cancel the Replace pursuits, retry claim scan and pause refresh.

        All three are scoped to the current state backend; the pause refresh
        is cancelled rather than awaited because it mutates the pause gate
        map, which nothing left in the shutdown path reads.
        """
        for task in list(self._slot_pursuits.values()):
            task.cancel()
        self._slot_pursuits.clear()
        if self._retry_claim_task is not None:
            self._retry_claim_task.cancel()
            self._retry_claim_task = None
        if self._pause_refresh_task is not None:
            self._pause_refresh_task.cancel()
            self._pause_refresh_task = None

    def signal_shutdown(self) -> None:
        logger.debug("Signalling shutdown")
        self._stop_event.set()
        # Wake the job reaper if it is parked on the idle wait below, so it
        # re-evaluates the loop condition and exits promptly instead of after
        # its next poll. Harmless when a job is running (the reaper clears this
        # each busy iteration); the only other setter is a job launch.
        self._jobs_running.set()

    @staticmethod
    def _empty_config() -> CronstableConfig:
        """The config used when no config source is set (config_arg is None).

        Empty job set, no web/cluster/logging, so applying it is a no-op that
        leaves any test-injected cron_jobs (config_yaml) untouched. Kept as a
        factory rather than a constant because JobDefaults({}) is mutable.
        """
        return CronstableConfig(
            jobs=[],
            web_config=None,
            job_defaults=JobDefaults({}),
            logging_config=None,
        )

    def _config_signature(self, files: frozenset[str]) -> tuple:
        """A cheap stat fingerprint of the files a parse read.

        ``(abspath, st_mtime_ns, st_size)`` per file, sorted; a vanished
        file collapses to a sentinel so a deletion still registers. A
        DIRECTORY config source folds in its own mtime, so a brand-new
        entry (touching no tracked file) is still noticed.
        """
        parts: list[tuple] = []
        for f in sorted(files):
            try:
                st = os.stat(f)
                parts.append((f, st.st_mtime_ns, st.st_size))
            except OSError:
                parts.append((f, None, None))
        if self.config_arg is not None and os.path.isdir(self.config_arg):
            try:
                parts.append(("\0dir", os.stat(self.config_arg).st_mtime_ns))
            except OSError:
                parts.append(("\0dir", None))
        return tuple(parts)

    def _quiet_gc_for_reparse(self) -> bool:
        """Whether this reload should run with the collector paused.

        A large reparse is pure allocation, and GC passes during it hold
        the GIL and stall the loop. Gated on job count because
        ``gc.disable()`` is process-global and spans an await a wedged
        mount can stretch; the common deployment never touches it.
        """
        return len(self.cron_jobs) >= _GC_QUIET_RELOAD_MIN

    async def _current_config_signature(
        self, loop: asyncio.AbstractEventLoop
    ) -> tuple:
        """:meth:`_config_signature` of the live sources, off the loop when it
        is big enough to be worth a thread hop.

        The stat probe is a pure function of a frozenset of paths returning an
        immutable tuple, so a worker thread touches nothing shared.  See
        :data:`_CONFIG_SIGNATURE_OFFLOAD_MIN` for why the small case stays
        inline.
        """
        sources = self._config_sources
        if len(sources) < _CONFIG_SIGNATURE_OFFLOAD_MIN:
            return self._config_signature(sources)
        return await loop.run_in_executor(
            None, self._config_signature, sources
        )

    def _record_config(
        self, config: CronstableConfig, sources: frozenset[str]
    ) -> None:
        """Cache a successful parse for the unchanged-config skip.

        Fingerprints ``sources`` immediately after the parse, so the next
        pass compares against the on-disk state actually parsed; an edit
        inside that microsecond window is picked up on a later change.
        """
        self._config_sources = sources
        self._config_sig = self._config_signature(sources)
        self._last_config = config

    def update_config(self) -> CronstableConfig:
        """Reload the config from disk and apply it, synchronously.

        Used at construction (no running loop yet) and by tests; the run
        loop uses :meth:`reload_config` instead. Always parses (no
        unchanged-config skip): it establishes the baseline the skip later
        compares against.
        """
        if self.config_arg is None:
            return self._empty_config()
        try:
            config, sources = parse_config_with_sources(self.config_arg)
        except ConfigError:
            # feeds cronstable_config_last_reload_successful, the standard
            # "config broken on disk" alert signal.
            self.metrics.config_parse(False)
            raise
        result = self._apply_reload(config)
        self._record_config(config, sources)
        return result

    async def reload_config(self) -> CronstableConfig:
        """:meth:`update_config`, but the reparse runs OFF the event loop
        and is skipped when the stat fingerprint is unchanged (downstream
        (re)starts are idempotent on the same config object).

        Applying the result stays on the loop thread via _apply_reload, so
        there is no cross-thread access to ``self``. The caller applies
        this BEFORE servicing due slots.
        """
        if self.config_arg is None:
            return self._empty_config()
        loop = asyncio.get_running_loop()
        if self._last_config is not None and (
            await self._current_config_signature(loop) == self._config_sig
        ):
            logger.debug("config unchanged on disk; skipping reparse")
            return self._last_config
        collecting = gc.isenabled() and self._quiet_gc_for_reparse()
        if collecting:
            gc.disable()
        try:
            config, sources = await loop.run_in_executor(
                None, parse_config_with_sources, self.config_arg
            )
        except ConfigError:
            # feeds cronstable_config_last_reload_successful; recorded here
            # on the loop thread (the parse ran in the worker).
            self.metrics.config_parse(False)
            raise
        finally:
            if collecting:
                gc.enable()
        result = self._apply_reload(config)
        self._record_config(config, sources)
        return result

    def _apply_reload(self, config: CronstableConfig) -> CronstableConfig:
        """Swap in a freshly parsed config's job set (event-loop thread only).

        Records the successful reload, installs the new jobs and prunes the
        per-job maps of jobs the reload removed. Kept separate from the parse
        itself so the parse can run in a worker thread (see :meth:`run`) while
        this mutation of shared scheduler state stays on the loop thread.
        """
        self.metrics.config_parse(True)
        old_jobs = self.cron_jobs
        self.cron_jobs = OrderedDict((job.name, job) for job in config.jobs)
        # DagScheduler reads this live each pass; in-flight runs of a
        # removed DAG finish and are GC'd.
        self.cron_dags = OrderedDict((d.name, d) for d in config.dags)
        # read live by _dispatch_notify, so a reload takes effect at once.
        self._notify_config = config.notify_config
        # The job set changed: invalidate the memo caches. A failed parse
        # raises before this point, so a bad reload never stales them.
        self._job_set_id_cache = None
        self._needs_subminute_cache = None
        self._job_pos_cache = None
        self._any_sla_cache = None
        # Drop metric series for removed jobs. A removed-but-still-running
        # job keeps its accumulator until the run finishes: pruning it
        # mid-run would let the finishing run recreate the series from zero
        # (a phantom counter reset).
        self.metrics.prune(set(self.cron_jobs) | set(self.running_jobs))
        # Drop last-run slots for removed jobs (churning names must not
        # grow the map); a still-running job keeps its slot until a later
        # reload, matching the metrics prune above.
        keep = set(self.cron_jobs) | set(self.running_jobs)
        self._last_run_slot = {
            name: slot
            for name, slot in self._last_run_slot.items()
            if name in keep
        }
        # a REMOVED job never runs again under that name, so its trends
        # entry would orphan forever; prune with the other per-job maps.
        self._trends_cache = {
            name: entry
            for name, entry in self._trends_cache.items()
            if name in keep
        }
        # the job set itself changed: the shared /jobs product is stale
        self._bust_response_memos()
        # Pause state survives a job-config edit (deliberately no digest
        # check, unlike retries: the operator paused the NAME, not one
        # definition of it); only a job the reload removed is pruned.
        self._paused = {
            name: info for name, info in self._paused.items() if name in keep
        }
        self._pause_gen = {
            name: gen for name, gen in self._pause_gen.items() if name in keep
        }
        self._pause_pending_writes = {
            name: rec
            for name, rec in self._pause_pending_writes.items()
            if name in keep
        }
        # Slot mutex prune is narrower: handing out a second Lock for a
        # slot somebody still holds would defeat the mutual exclusion, so
        # forget a name only when nothing can still take its mutex (no
        # config entry, instance, refcount, lease, renewer, pursuit, holder
        # or waiter).
        slot_live = (
            keep
            | set(self._slot_refs)
            | set(self._slot_leases)
            | set(self._slot_renewers)
            | set(self._slot_pursuits)
        )
        self._slot_locks = {
            name: lock
            for name, lock in self._slot_locks.items()
            if name in slot_live or lock.locked()
        }
        # launch locks: same rule, never drop one somebody holds (a waiter
        # would mint a fresh lock and race the launch it serialises).
        self._launch_locks = defaultdict(
            asyncio.Lock,
            {
                name: lock
                for name, lock in self._launch_locks.items()
                if name in keep or lock.locked()
            },
        )
        self._last_real_outcome = {
            name: outcome
            for name, outcome in self._last_real_outcome.items()
            if name in keep
        }
        self._last_completed_at = {
            name: at
            for name, at in self._last_completed_at.items()
            if name in keep
        }
        # SLA trackers survive a job edit (history did not change); only
        # removed jobs are pruned. A check dropped from a surviving job's
        # sla block is cleared by the next _sla_periodic pass instead.
        self._sla_last_success = {
            name: at
            for name, at in self._sla_last_success.items()
            if name in keep
        }
        self._sla_due = {
            name: at for name, at in self._sla_due.items() if name in keep
        }
        self._sla_last_start = {
            name: at
            for name, at in self._sla_last_start.items()
            if name in keep
        }
        self._sla_state = {
            key: since
            for key, since in self._sla_state.items()
            if key[0] in keep
        }
        self._sla_pause_windows = {
            name: spans
            for name, spans in self._sla_pause_windows.items()
            if name in keep
        }
        self._sla_disabled_since = {
            name: at
            for name, at in self._sla_disabled_since.items()
            if name in keep
        }
        # first-seen is the one tracker this pass also SEEDS: a just-added
        # job ages into maxTimeSinceSuccess from now, not process start.
        self._sla_first_seen = {
            name: at
            for name, at in self._sla_first_seen.items()
            if name in keep
        }
        seen_at = get_now(datetime.timezone.utc)
        for name in self.cron_jobs:
            self._sla_first_seen.setdefault(name, seen_at)
        # a removed job's last_run/run_history is unreachable (payload
        # builders iterate cron_jobs only), so keeping it is leaked memory.
        self.last_run = {
            name: info for name, info in self.last_run.items() if name in keep
        }
        for name in [n for n in self.run_history if n not in keep]:
            del self.run_history[name]
        # Re-sync the next-fire index: drop removed jobs, reseed changed
        # schedules, KEEP unchanged ones (a reseed computes a strictly-
        # future fire and could skip one on this reload's minute boundary).
        self._refresh_schedule(get_now(datetime.timezone.utc), old_jobs)
        return config

    def job_set_id(self) -> str:
        """Order-independent fingerprint of the currently-loaded job set.

        Same value iff two instances hold the same effective job set; see
        cronstable.fingerprint. Memoized per reload, keeping the deepcopy/
        JSON/SHA-256 work off the scrape/gossip/lease-renew paths.
        """
        cached = self._job_set_id_cache
        if cached is None:
            cached = job_set_id(self.cron_jobs.values())
            self._job_set_id_cache = cached
        return cached

    def _log_job_set_id(self) -> None:
        """Log the job-set id at startup and whenever a reload changes it."""
        current = self.job_set_id()
        if current != self._logged_job_set_id:
            logger.info(
                "Job set id: %s (%d job%s)",
                current,
                len(self.cron_jobs),
                "" if len(self.cron_jobs) == 1 else "s",
            )
            self._logged_job_set_id = current

    async def _web_get_version(self, request: web.Request) -> web.Response:
        return web.Response(
            text=cronstable.version.version,
            headers=self._web_headers(),
        )

    async def _web_job_set_id(self, request: web.Request) -> web.Response:
        job_set = self.job_set_id()
        headers = self._web_headers()
        if _accepts_json(request):
            return _json_response(
                {"job_set_id": job_set, "jobs": len(self.cron_jobs)},
                headers=headers,
            )
        return web.Response(text=job_set, headers=headers)

    def cluster_payload(self) -> dict[str, Any]:
        """This node's cluster/leadership view.

        Behind ``GET /cluster`` and MCP ``cron_get_cluster``.
        ``enabled: false`` when no cluster section is configured.
        """
        if self.cluster_manager is None:
            return {"enabled": False, "peers": []}
        payload = dict(self.cluster_manager.view_dict())
        payload["enabled"] = True
        # lease backends have no fleet view of their own; tell the
        # dashboard whether the observability overlay provides one. The
        # gossip payload stays unchanged (its UI always shows the view).
        if payload.get("backend") != "gossip":
            payload["fleet"] = self.observability_mesh is not None
        # this node's own live CPU/memory, always shown (local and free);
        # peer load rides view_dict's per-peer node_stats.
        payload["node_stats"] = self.node_resource_snapshot()
        return payload

    async def _web_get_cluster(self, request: web.Request) -> web.Response:
        # no ETag: the payload embeds this node's freshly sampled CPU/memory,
        # so a tag would churn every poll and 304 would never fire; gzip is
        # the whole win here (peer summaries compress well).
        return _cachable_json_response(
            self.cluster_payload(),
            if_none_match=None,
            gzip_ok=_accepts_gzip(request.headers.get("Accept-Encoding")),
            headers=self._web_headers(),
            use_etag=False,
        )

    async def _web_get_fleet(self, request: web.Request) -> web.Response:
        """The cluster-wide per-job run view (the dashboard's fleet view).

        Merged entirely from state this node already holds (own snapshot
        plus gossip-piggybacked peer summaries); serving it triggers no
        peer traffic. ``enabled: false`` when no cluster or no channel
        carried summaries; the dashboard then hides the view. Conditional,
        compressed and memoized like the other poll legs.
        """
        return await self._memoized_conditional_response(
            request,
            self._fleet_response_memo,
            _FLEET_RESPONSE_TTL,
            self._build_fleet_product,
        )

    def fleet_payload(self) -> dict[str, Any]:
        """The cluster-wide per-job run view.

        Behind ``GET /fleet`` and MCP ``cron_get_fleet``.
        ``enabled: false`` when there is no cluster, or the backend has no
        node-to-node channel to have carried summaries.
        """
        # the overlay mesh when a lease cluster opted into observability, else
        # the leadership backend (gossip provides the view; lease backends
        # return None -> feature unavailable).
        mgr = self._fleet_backend()
        fleet = mgr.fleet_view() if mgr is not None else None
        if fleet is None:
            return {"enabled": False, "nodes": []}
        return fleet

    async def _build_fleet_product(
        self,
    ) -> tuple[str, bytes, Optional[bytes]]:
        # the merge reads live gossip state so it stays on the loop; the
        # product over the merged dict is pure and offloads for a large
        # fleet (see _FLEET_SERIALIZE_OFFLOAD_MIN).
        payload = self.fleet_payload()
        entries = sum(
            len(node.get("jobs") or {}) for node in payload.get("nodes") or []
        )
        if entries >= _FLEET_SERIALIZE_OFFLOAD_MIN:
            return await asyncio.get_running_loop().run_in_executor(
                None, _cachable_json_product, payload
            )
        return _cachable_json_product(payload)

    async def _web_get_node(self, request: web.Request) -> web.Response:
        """This node's live CPU/memory (the dashboard's node readout).

        Whole-host CPU% and memory plus this daemon's own footprint, sampled
        fresh per request from
        :class:`cronstable.resources.NodeResourceSampler`.
        ``resources`` is ``null`` when sampling is unavailable (psutil could
        not read the host); the dashboard then hides the node meter.
        """
        return _json_response(self.node_payload(), headers=self._web_headers())

    def _node_name(self) -> str:
        """The cluster node name when clustered, else the plain hostname the
        durable-state layer already uses as this node's identity."""
        mgr = self.cluster_manager
        if mgr is not None and getattr(mgr, "node_name", None):
            return mgr.node_name
        return self._state_host

    def _node_history_block(self) -> dict[str, Any]:
        """The retained CPU/memory ring as its wire shape.

        ``enabled`` is false (with no points) when the sampler is off
        (``nodeHistory: false``) or psutil is unavailable, so the
        dashboard hides the chart instead of showing an eternally-empty
        one.
        """
        hist = self._node_sampler.history()
        return {
            "enabled": hist is not None,
            "interval": hist["interval"] if hist is not None else None,
            "points": hist["points"] if hist is not None else [],
        }

    def node_payload(self, history: bool = False) -> dict[str, Any]:
        """This node's live CPU/memory (`GET /node`, MCP `cron_get_node`).

        ``resources`` is ``None`` when sampling is unavailable.  With
        ``history=True`` a ``history`` block (the retained ring the dashboard
        chart uses) rides along.
        """
        payload: dict[str, Any] = {
            "node_name": self._node_name(),
            "resources": self._node_sampler.snapshot(),
        }
        if history:
            payload["history"] = self._node_history_block()
        return payload

    async def _web_node_history(self, request: web.Request) -> web.Response:
        """The node's retained CPU/memory history (the dashboard node chart).

        Oldest-first ``[t, cpu%, mem%]`` points from the background sampler
        (see ``web.nodeHistory``), fetched lazily when the chart is opened
        rather than riding the /node poll.
        """
        return _json_response(
            {"node_name": self._node_name(), **self._node_history_block()},
            headers=self._web_headers(),
        )

    async def _shared_response_product(
        self,
        memo: "_ResponseMemo[_ProductT]",
        ttl: float,
        build: Callable[[], Awaitable[_ProductT]],
    ) -> _ProductT:
        """Serve ``memo``'s product, building (or joining a build) on a miss.

        The ONE memoized-response scaffold (/jobs, /metrics, /fleet,
        /activity). Single-flight: followers await the leader's future,
        shielded so a hung-up client cannot cancel the shared build; a
        failed build resolves the future to None (never set_exception) and
        the failure propagates only to the builder.

        The generation guard closes the bust races: the leader reads
        _memo_gen BEFORE build() and re-checks before storing; a joiner
        compares the future's recorded generation before trusting the
        product. After a failed or invalidated build the waiter loops and
        exactly one self-promotes. No await sits between the cache check,
        inflight check and registration, so leadership is decided
        atomically on the loop; the store happens before finish(), so
        woken followers find the cache warm.
        """
        loop = asyncio.get_running_loop()
        while True:
            cached = memo.cached
            if cached is not None and loop.time() - cached[0] < ttl:
                return cached[1]
            pending = memo.inflight
            if pending is None:
                break
            pending_gen = memo.inflight_gen
            shared = await asyncio.shield(pending)
            if shared is not None and pending_gen == self._memo_gen:
                return shared
            # the leader failed, or a bust invalidated its product: loop,
            # so exactly one waiter (the first to see the free slot)
            # self-promotes and the rest join its build
        gen = self._memo_gen
        fut: "asyncio.Future[Optional[_ProductT]]" = loop.create_future()
        memo.inflight = fut
        memo.inflight_gen = gen
        try:
            product = await build()
        except BaseException:
            memo.finish(fut, None)
            raise
        if gen == self._memo_gen:
            memo.cached = (loop.time(), product)
        memo.finish(fut, product)
        return product

    async def _memoized_conditional_response(
        self,
        request: web.Request,
        memo: "_ResponseMemo[tuple[str, bytes, bytes | None]]",
        ttl: float,
        build: Callable[[], Awaitable[tuple[str, bytes, bytes | None]]],
    ) -> web.Response:
        """One conditional GET over a shared memoized (etag, body, gz).

        The common tail of the /jobs, /fleet and /activity handlers:
        fetch (or join the build of) the shared product, then answer
        304/200 per request. Handlers pass their TTL global per request,
        so a deployment-wide tweak (or a test monkeypatch) takes effect
        on the next poll.
        """
        etag, body, gz = await self._shared_response_product(memo, ttl, build)
        return _conditional_response(
            etag,
            body,
            gz,
            if_none_match=request.headers.get("If-None-Match"),
            gzip_ok=_accepts_gzip(request.headers.get("Accept-Encoding")),
            headers=self._web_headers(),
        )

    async def _metrics_product(
        self, openmetrics: bool
    ) -> tuple[bytes, Optional[bytes]]:
        """One shared ``/metrics`` product (body, gz) per exposition format.

        The TTL global is read at call time, not bound at definition, so a
        deployment-wide tweak (or a test monkeypatch) takes effect on the
        next scrape.
        """
        return await self._shared_response_product(
            self._metrics_response_memo[openmetrics],
            _METRICS_RESPONSE_TTL,
            partial(self._build_metrics_product, openmetrics),
        )

    async def _build_metrics_product(
        self, openmetrics: bool
    ) -> tuple[bytes, Optional[bytes]]:
        # The family build reads live scheduler state, so it stays on the
        # loop; the render and the compression are pure over that
        # freshly-built list, so a large job set does both on the executor
        # (see _METRICS_OFFLOAD_MIN_JOBS), like the calendar builder.
        families = self.metrics.families(self)
        if len(self.cron_jobs) >= _METRICS_OFFLOAD_MIN_JOBS:
            return await asyncio.get_running_loop().run_in_executor(
                None,
                partial(
                    _metrics_response_product,
                    self.metrics.render_prepared,
                    families,
                    openmetrics,
                ),
            )
        return _metrics_response_product(
            self.metrics.render_prepared, families, openmetrics
        )

    async def _web_metrics(self, request: web.Request) -> web.Response:
        accept = request.headers.get("Accept", "")
        openmetrics = "application/openmetrics-text" in accept
        gzip_ok = _accepts_gzip(request.headers.get("Accept-Encoding"))
        # one product is shared across scrapers per exposition format;
        # only the representation pick below is per-request.
        body, gz = await self._metrics_product(openmetrics)
        # the Content-Type is the endpoint's contract (scrapers parse it
        # for the format version), so it wins over web.headers.
        headers = _strip_content_type(self._web_headers())
        headers["Content-Type"] = (
            CONTENT_TYPE_OPENMETRICS if openmetrics else CONTENT_TYPE_TEXT
        )
        # Vary on both: a shared cache must not hand a gzipped body to a
        # client that cannot read one, nor an openmetrics body to a text
        # scraper (the format is negotiated on Accept).
        headers["Vary"] = "Accept, Accept-Encoding"
        if gzip_ok and gz is not None:
            headers["Content-Encoding"] = "gzip"
            body = gz
        return web.Response(body=body, headers=headers)

    def status_payload(self) -> list[dict[str, Any]]:
        """Per-job status rows (running / disabled / scheduled).

        The data behind ``GET /status`` and the MCP ``cron_get_status`` tool.
        """
        # explicit annotation: the rows are mixed-shape.
        out: list[dict[str, Any]] = []
        # one clock read for the whole pass; see _scheduled_in's `now`
        now = get_now(datetime.timezone.utc)
        for name, job in self.cron_jobs.items():
            running = self.running_jobs.get(name, None)
            if running:
                row: dict[str, Any] = {
                    "job": name,
                    "status": "running",
                    "pid": [
                        runjob.proc.pid
                        for runjob in running
                        if runjob.proc is not None
                    ],
                }
                if self._schedule_never_fires(name, job):
                    # a running job with a dead schedule still never fires
                    # again; flag it here exactly as /jobs does, so the two
                    # surfaces agree (the text renderer keeps saying
                    # "running": only the JSON row gains the field).
                    row["never_fires"] = True
                out.append(row)
            elif not job.enabled:
                # disabled jobs never run on schedule; report that honestly
                # instead of an inapplicable "scheduled (in N seconds)".
                out.append({"job": name, "status": "disabled"})
            else:
                crontab: CronTab | str = job.schedule
                scheduled_in = (
                    self._scheduled_in(name, job, False, now)
                    if isinstance(crontab, CronTab)
                    else str(crontab)
                )
                row = {
                    "job": name,
                    "status": "scheduled",
                    "scheduled_in": scheduled_in,
                }
                if isinstance(crontab, CronTab) and scheduled_in is None:
                    # "scheduled" with no instant means NEVER; say so
                    # instead of leaving a null for consumers to guess at
                    row["never_fires"] = True
                out.append(row)
        return out

    async def _web_get_status(self, request: web.Request) -> web.Response:
        out = self.status_payload()
        if _accepts_json(request):
            return _json_response(out, headers=self._web_headers())
        else:
            lines = []
            for jobstat in out:
                if jobstat["status"] == "running":
                    status = "running (pid: {pid})".format(
                        pid=", ".join(str(pid) for pid in jobstat["pid"])
                    )
                elif jobstat["status"] == "disabled":
                    status = "disabled"
                elif jobstat.get("never_fires"):
                    status = "never fires (schedule has no future occurrence)"
                else:
                    status = "scheduled ({})".format(
                        (
                            jobstat["scheduled_in"]
                            if isinstance(jobstat["scheduled_in"], str)
                            else naturaltime(jobstat["scheduled_in"])
                        )
                    )
                lines.append(
                    "{name}: {status}".format(
                        name=jobstat["job"], status=status
                    )
                )
            return web.Response(
                text="\n".join(lines),
                headers=self._web_headers(),
            )

    def summary_payload(self) -> dict[str, Any]:
        """A single batched fleet overview (behind ``GET /summary``).

        Job counts, soonest upcoming fire, node identity and cluster role
        in one call, derived from the same live scheduler state /jobs and
        /status read, so the surfaces cannot disagree.
        """
        now = get_now(datetime.timezone.utc)
        total = enabled = running = paused = failing = never_fires = 0
        soonest_name: Optional[str] = None
        soonest_in: Optional[float] = None
        for name, job in self.cron_jobs.items():
            total += 1
            is_running = bool(self.running_jobs.get(name))
            if is_running:
                running += 1
            if job.enabled:
                enabled += 1
            pause = self._pause_active(name)
            if pause is not None:
                paused += 1
            last = self.last_run.get(name)
            # "failing" is a last-outcome verdict (matching the dashboard's
            # failing-count badge), not a count of jobs mid-retry.
            if last is not None and last.outcome == "failure":
                failing += 1
            if self._schedule_never_fires(name, job):
                never_fires += 1
            scheduled_in = self._scheduled_in(name, job, is_running, now)
            if scheduled_in is not None and pause is not None:
                # a fire the pause window covers is skipped at the gate, so
                # it must not be reported as the fleet's next fire; a fire
                # past the pause's `until` still counts.
                fire_at = now + datetime.timedelta(seconds=scheduled_in)
                if fire_at < pause.until:
                    scheduled_in = None
            if scheduled_in is not None and (
                soonest_in is None or scheduled_in < soonest_in
            ):
                soonest_in = scheduled_in
                soonest_name = name
        # the cluster node name when clustered, else the plain hostname (the
        # same identity node_payload reports).
        mgr = self.cluster_manager
        node_name = (
            mgr.node_name
            if mgr is not None and getattr(mgr, "node_name", None)
            else self._state_host
        )
        next_fire: Optional[dict[str, Any]] = None
        if soonest_name is not None and soonest_in is not None:
            when = self._next_fire.get(soonest_name)
            next_fire = {
                "job": soonest_name,
                "in": soonest_in,
                # from the next-fire index; derived from the countdown in
                # the pre-seed startup window so it is never null.
                "at": (
                    when.isoformat()
                    if when is not None
                    else (
                        now + datetime.timedelta(seconds=soonest_in)
                    ).isoformat()
                ),
            }
        summary: dict[str, Any] = {
            "version": cronstable.version.version,
            "node_name": node_name,
            "generated_at": now.isoformat(),
            "jobs": {
                "total": total,
                "enabled": enabled,
                "disabled": total - enabled,
                "running": running,
                "paused": paused,
                "failing": failing,
                "never_fires": never_fires,
            },
            "next_fire": next_fire,
        }
        if self.cron_dags:
            summary["dags"] = {"total": len(self.cron_dags)}
        if mgr is not None:
            summary["cluster"] = {
                "enabled": True,
                "distribution": mgr.distribution,
                "quorate": mgr.is_quorate(),
                "is_leader": mgr.is_leader(),
                "leader": mgr.leader_name(),
            }
        else:
            summary["cluster"] = {"enabled": False}
        return summary

    async def _web_get_summary(self, request: web.Request) -> web.Response:
        return _json_response(
            self.summary_payload(),
            headers=self._web_headers(),
        )

    async def _web_whoami(self, request: web.Request) -> web.Response:
        """Describe the bearer token that authenticated this request.

        Label and scopes of the matched token (filed by the auth
        middleware), so the dashboard can warn when its pairing QR would
        hand a phone the all-scopes token, and a companion app can show
        what it is allowed to do. With no auth middleware installed
        there is no token to describe: ``authenticated`` is false and
        every scope is effectively granted.
        """
        matched = request.get(WEB_TOKEN_REQUEST_KEY)
        if matched is None:
            payload: dict[str, Any] = {
                "authenticated": False,
                "label": None,
                "scopes": sorted(_WEB_ALL_SCOPES),
                "allScopes": True,
            }
        else:
            payload = {
                "authenticated": True,
                "label": matched.label,
                "scopes": sorted(matched.scopes),
                "allScopes": matched.scopes == _WEB_ALL_SCOPES,
            }
        return _json_response(payload, headers=self._web_headers())

    def _push_service_required(self) -> "push.PushService":
        """The running push service, or the 404 the route contract says.

        The /push/devices routes are registered unconditionally (so a
        reload that adds the section needs no web-app restart); until a
        `push:` section is applied they answer 404 with a reason.
        """
        service = self._push_service
        if service is None:
            raise _api_error(
                web.HTTPNotFound,
                "no `push:` section is configured on this daemon",
            )
        return service

    @staticmethod
    def _push_store_unavailable(
        exc: "push.PushError", doing: str
    ) -> web.HTTPException:
        """The 503 for registry-store trouble, detail kept to the log.

        Store PushError messages name absolute paths and quote backend
        text; echoing them would publish the filesystem layout to any
        view-scoped caller. Build the body from what the caller already
        knows, never from the exception (the _timezone_error move).
        """
        logger.warning("push: %s failed: %s", doing, exc)
        return _api_error(
            web.HTTPServiceUnavailable,
            "the device registry's store is unavailable; "
            "the reason is in the cronstable log",
        )

    async def _web_push_devices(self, request: web.Request) -> web.Response:
        service = self._push_service_required()
        try:
            # force=True: a listing is the operator checking their
            # pairings; it must reflect the store, not a 60s mirror.
            await service.refresh(force=True)
        except push.PushError as exc:
            raise self._push_store_unavailable(
                exc, "listing paired devices"
            ) from None
        return _json_response(
            {"devices": service.devices_payload()},
            headers=self._web_headers(),
        )

    async def _web_push_pair(self, request: web.Request) -> web.Response:
        service = self._push_service_required()
        try:
            body = await request.json()
        except ValueError:
            raise _api_error(
                web.HTTPBadRequest, "body must be a JSON object"
            ) from None
        try:
            fields = push.validate_pairing(body)
        except push.PushError as exc:
            # Safe to echo, unlike the store's PushErrors either side of
            # it: every message validate_pairing can raise is a statically
            # authored sentence about the caller's own body. No path, no
            # errno, no library text; its PyNaCl brushes keep their detail
            # in the log and raise a fixed string.
            raise _api_error(web.HTTPBadRequest, str(exc)) from None
        matched = request.get(WEB_TOKEN_REQUEST_KEY)
        try:
            record, created = await service.pair(
                fields, matched.label if matched is not None else None
            )
        except push.PushError as exc:
            raise self._push_store_unavailable(
                exc, "pairing a device"
            ) from None
        logger.info(
            "push: device %r (%s) %s by %s",
            record["name"],
            record["id"],
            "paired" if created else "re-paired",
            matched.label if matched is not None else "unauthenticated",
        )
        return _json_response(
            {"device": push.public_device(record), "created": created},
            status=201 if created else 200,
            headers=self._web_headers(),
        )

    async def _web_push_revoke(self, request: web.Request) -> web.Response:
        service = self._push_service_required()
        device_id = request.match_info["id"]
        try:
            removed = await service.revoke(device_id)
        except push.PushError as exc:
            raise self._push_store_unavailable(
                exc, "revoking a device"
            ) from None
        if not removed:
            raise _api_error(
                web.HTTPNotFound,
                "no paired device with id {!r}".format(device_id),
            )
        logger.info("push: device %s revoked", device_id)
        return _json_response(
            {"revoked": device_id},
            headers=self._web_headers(),
        )

    async def _web_push_test(self, request: web.Request) -> web.Response:
        """Round-trip one test alert through the relay to one device.

        200 with the relay outcome on success; 502 when sealing or the
        relay failed (the outcome body says which), so "my phone is
        silent" is debuggable from the dashboard instead of the logs.
        """
        service = self._push_service_required()
        device_id = request.match_info["id"]
        try:
            await service.refresh(force=True)
        except push.PushError as exc:
            raise self._push_store_unavailable(
                exc, "refreshing the registry for a test alert"
            ) from None
        device = service.get_device(device_id)
        if device is None:
            raise _api_error(
                web.HTTPNotFound,
                "no paired device with id {!r}".format(device_id),
            )
        outcome = await service.send_test(device)
        return _json_response(
            outcome,
            status=502 if outcome.get("error") else 200,
            headers=self._web_headers(),
        )

    async def start_stop_push(
        self, push_config: Optional[dict[str, Any]]
    ) -> None:
        """Converge the push service onto ``push_config``, never raising.

        Runs every housekeeping pass after start_stop_state (the registry
        may ride that store). Never-raising is load-bearing: anything
        escaping here silently stops the durable-state manifest and GC for
        the life of the process (see the run() call site). ``Exception``,
        not ``BaseException``: a cancelled pass must still cancel.
        """
        try:
            await self._converge_push(push_config)
        except Exception:
            logger.exception(
                "push: could not converge the push service; leaving it as "
                "it was and continuing the housekeeping pass"
            )

    async def _converge_push(
        self, push_config: Optional[dict[str, Any]]
    ) -> None:
        """The convergence itself; see :meth:`start_stop_push`."""
        if push_config == self._applied_push_config and (
            push_config is None
        ) == (self._push_service is None):
            return
        if push_config is None:
            if self._push_service is not None:
                logger.info("push: section removed; stopping the service")
            self._push_service = None
            self._applied_push_config = None
            push.set_service(None)
            return
        devices_file = push_config.get("devicesFile")
        if devices_file:
            store: Any = push.FileDeviceStore(devices_file)
        else:
            # a callable, not a reference: the state backend is torn
            # down/rebuilt on reload and the store must track it live.
            store = push.StateDeviceStore(lambda: self.state_backend)
        service = push.PushService(
            relay_url=push_config["relay"]["url"],
            relay_timeout=push_config["relay"]["timeout"],
            store=store,
            host=report_hostname(),
        )
        await service.start()
        self._push_service = service
        # A deep copy, never the caller's dict: the convergence guard
        # above compares by equality, and holding an alias would make a
        # config edit that mutates the same dict in place compare equal
        # to itself forever (the mergedicts/DEFAULT_CONFIG sharing trap);
        # a changed push: section would then never be re-applied.
        self._applied_push_config = copy.deepcopy(push_config)
        push.set_service(service)
        logger.info("push: service running (registry %s)", store.describe())

    @staticmethod
    def _zone_from_name(tz_name: Optional[str]) -> datetime.tzinfo:
        """A ``?tz=`` query value as a tzinfo (UTC when absent).

        Raises ValueError for an unknown name; the handlers turn that
        into a 400.
        """
        if not tz_name:
            return datetime.timezone.utc
        try:
            return ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
            # ZoneInfo maps its key onto a filesystem path, so an over-long
            # (ENAMETOOLONG) or otherwise unopenable key surfaces as OSError
            # rather than ZoneInfoNotFoundError: still just an unknown name.
            raise ValueError("unknown timezone: {}".format(tz_name)) from None

    @staticmethod
    def _timezone_error(tz_name: Optional[str]) -> Optional[str]:
        """The 400 message for a bad ``?tz=`` value, or None when valid.

        Built from the requested name, never the caught exception (whose
        text can carry a server-side ZoneInfo filesystem path). Mirrors
        :meth:`_zone_from_name` so the two agree on unknown names.
        """
        if not tz_name:
            return None
        try:
            ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
            return "unknown timezone: {}".format(tz_name)
        return None

    @staticmethod
    def _period_error(period: str) -> Optional[str]:
        """The 400 message for an unsupported ``?period=`` value, or
        ``None`` when it is valid.

        Mirrors :func:`suggest_slot`'s own check so ``/schedule/suggest``
        can answer 400 from the request value without stringifying the
        builder's exception.
        """
        if period not in ("hourly", "daily"):
            return "period must be 'hourly' or 'daily', got {!r}".format(
                period
            )
        return None

    @staticmethod
    def _invalid_timestamp_message(text: str) -> str:
        """The 400 message for an unparseable ``?at=`` value.

        Built from the requested value (its :func:`repr` plus the accepted
        forms), so ``/schedule/why`` can answer 400 from the request string
        without stringifying the parse exception.  Shared with
        :meth:`_parse_probe_timestamp` so the raised and the handler-built
        text never diverge.
        """
        return (
            "invalid timestamp {!r}: pass ISO 8601, e.g. "
            "2026-07-14T09:00, 2026-07-14T09:00:00+02:00 or "
            "2026-07-14T09:00:00Z".format((text or "").strip())
        )

    @staticmethod
    def _parse_probe_timestamp(text: str) -> datetime.datetime:
        """An ``at=`` timestamp as a datetime (aware when it has an offset).

        Accepts ISO 8601 as :func:`datetime.datetime.fromisoformat` does,
        plus the ubiquitous trailing ``Z`` (which the py310 parser still
        rejects).  Raises ValueError, with the accepted forms in the
        message, for anything else; the handlers turn that into a 400.
        """
        raw = (text or "").strip()
        iso = raw
        if iso.endswith(("Z", "z")):
            iso = iso[:-1] + "+00:00"
        try:
            return datetime.datetime.fromisoformat(iso)
        except ValueError:
            raise ValueError(Cron._invalid_timestamp_message(raw)) from None

    def schedule_preview_payload(
        self,
        expr: str,
        tz_name: Optional[str] = None,
        count: int = 12,
        seed: Optional[str] = None,
    ) -> dict[str, Any]:
        """Parse, describe, preview and lint one schedule expression.

        Behind ``GET /schedule/preview``, computed by the same engine,
        describer and linter the scheduler runs, so a preview cannot
        disagree with the daemon. Raises ValueError for an unknown
        timezone. ``seed`` is the hash key for the ``H`` form; without it
        an ``H`` expression comes back invalid.
        """
        zone = self._zone_from_name(tz_name)
        text = (expr or "").strip()
        payload: dict[str, Any] = {
            "expression": text,
            "timezone": str(zone),
        }
        if seed is not None:
            payload["seed"] = seed
        if text.lower() == "@reboot":
            payload.update(
                {
                    "valid": True,
                    "reboot": True,
                    "description": describe_cron(text),
                    "fires": [],
                    "never_fires": False,
                    "lint": [],
                }
            )
            return payload
        try:
            tab = CronTab(text, hash_key=seed)
        except (ValueError, KeyError) as err:
            payload.update({"valid": False, "error": str(err)})
            return payload
        fires = next_fires(text, count, tz=zone, hash_key=seed)
        payload.update(
            {
                "valid": True,
                "reboot": False,
                "normalized": str(tab),
                "description": describe_cron(text, hash_key=seed),
                "fires": [when.isoformat() for when in fires],
                "never_fires": not fires,
                "lint": [
                    finding._asdict()
                    for finding in lint_schedule(
                        text, timezone=zone, hash_key=seed
                    )
                ],
            }
        )
        if tab.resolved_differs:
            payload["resolved"] = tab.resolved_source
        return payload

    async def _web_schedule_preview(
        self, request: web.Request
    ) -> web.Response:
        """Decode an arbitrary expression for the dashboards' sandboxes."""
        headers = self._web_headers()
        expr = request.query.get("expr", "")
        if not expr.strip():
            return _json_response(
                {"error": "missing ?expr= query parameter"},
                status=400,
                headers=headers,
            )
        tz = request.query.get("tz") or None
        # validate the one input that would raise; expression parse errors
        # come back as payload data (valid=False), so no exception text
        # ever reaches the response body.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        count = self._web_int_query(
            request, "limit", default=12, lo=1, hi=60, alias="count"
        )
        payload = self.schedule_preview_payload(
            expr, tz, count, request.query.get("seed")
        )
        return _json_response(payload, headers=headers)

    def _job_or_dag_schedule(self, name: str) -> Optional[JobConfig]:
        """The named job, or a DAG's synthetic ``dag:<name>`` schedule job.

        DAG schedule jobs launch real work on real instants, so "why did
        this not run?" is as answerable for them as for any job; they are
        just not in ``cron_jobs``.
        """
        job = self.cron_jobs.get(name)
        if job is not None:
            return job
        for dag in self.cron_dags.values():
            sched = dag.schedule_job
            if sched is not None and sched.name == name:
                return sched
        return None

    def schedule_why_payload(
        self, name: str, at: str
    ) -> Optional[dict[str, Any]]:
        """Why a job's schedule did (not) select a timestamp
        (``GET /schedule/why``, MCP ``cron_why_no_run``).

        Decomposes the engine's own match test via why_no_run in the job's
        OWN resolved timezone (aware ``at`` converts, naive reads as wall
        time there), plus job-level facts and the nearest real fire on
        each side. None for an unknown job; ValueError for a bad
        timestamp.
        """
        job = self._job_or_dag_schedule(name)
        if job is None:
            return None
        # a schedule match says nothing about whether the fire would LAUNCH:
        # an active pause skips it, so the answer must carry that fact.
        pause = self._pause_active(name)
        pause_note: Optional[dict[str, str]] = None
        if pause is not None:
            message = "job is paused until {} (by {})".format(
                pause.until.isoformat(), pause.by
            )
            if pause.note:
                message += ": " + pause.note
            pause_note = {
                "code": "paused",
                "level": "note",
                "message": message,
            }
        zone = job.timezone or datetime.datetime.now().astimezone().tzinfo
        payload: dict[str, Any] = {
            "job": name,
            "enabled": job.enabled,
            "timezone": (
                str(job.timezone) if job.timezone is not None else "local"
            ),
            "at": at,
        }
        if not isinstance(job.schedule, CronTab):
            # "@reboot": no timetable exists for any timestamp to match.
            payload.update(
                {
                    "expression": str(job.schedule),
                    "reboot": True,
                    "description": describe_cron(str(job.schedule)),
                    "matches": False,
                    "checks": [],
                    "failed": [],
                    "notes": [
                        {
                            "code": "reboot",
                            "level": "note",
                            "message": "@reboot runs once when the daemon "
                            "starts; it never fires on a timetable, so no "
                            "timestamp can match",
                        }
                    ],
                    "previous_fire": None,
                    "next_fire": None,
                }
            )
            if pause_note is not None:
                payload["notes"].append(pause_note)
            return payload
        tab = job.schedule
        probe = self._parse_probe_timestamp(at)
        if probe.tzinfo is not None:
            aware = probe.astimezone(zone)
            civil = aware.replace(tzinfo=None)
        else:
            civil = probe
            # fold=0, the engine's own rule for resolving a wall time.
            aware = probe.replace(tzinfo=zone)
        payload.update(
            {
                "expression": str(tab),
                "reboot": False,
                "description": describe_cron(str(tab), hash_key=job.name),
                "at_in_zone": aware.isoformat(),
            }
        )
        if tab.resolved_differs:
            payload["resolved"] = tab.resolved_source
        payload.update(why_no_run(tab, civil, timezone=zone))
        next_fire = next(iter(tab.occurrences(aware)), None)
        prev_delta = tab.prev(now=aware)
        previous_fire = None
        if prev_delta is not None:
            fire_utc = aware.astimezone(
                datetime.timezone.utc
            ) - datetime.timedelta(seconds=prev_delta)
            # fires are whole seconds; round away float/microsecond residue
            # from the elapsed-seconds reconstruction.
            if fire_utc.microsecond >= 500000:
                fire_utc += datetime.timedelta(seconds=1)
            previous_fire = (
                fire_utc.replace(microsecond=0).astimezone(zone).isoformat()
            )
        payload["previous_fire"] = previous_fire
        payload["next_fire"] = (
            next_fire.isoformat() if next_fire is not None else None
        )
        if pause_note is not None:
            payload.setdefault("notes", []).append(pause_note)
        return payload

    async def _web_schedule_why(self, request: web.Request) -> web.Response:
        """Explain one job's schedule against one timestamp."""
        headers = self._web_headers()
        name = request.query.get("job", "").strip()
        at = request.query.get("at", "").strip()
        if not name or not at:
            return _json_response(
                {"error": "missing ?job= or ?at= query parameter"},
                status=400,
                headers=headers,
            )
        try:
            payload = self.schedule_why_payload(name, at)
        except ValueError:
            # The only ValueError here is the unparseable ``at=`` timestamp;
            # answer 400 from the requested value, not the exception text
            # (kept inside the try so an unknown job still 404s below).
            return _json_response(
                {"error": self._invalid_timestamp_message(at)},
                status=400,
                headers=headers,
            )
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(payload, headers=headers)

    def _schedule_entries(self) -> list[ScheduleEntry]:
        """The analyzable fleet: every enabled, cron-scheduled job.

        DAG schedules ride along as their synthetic ``dag:<name>`` job
        (they launch real work on real instants, so they belong in the
        collision picture).  @reboot jobs and disabled jobs are excluded:
        neither fires on a schedule, so neither can collide.
        """
        entries = [
            ScheduleEntry(name, job.schedule, job.timezone)
            for name, job in self.cron_jobs.items()
            if job.enabled and isinstance(job.schedule, CronTab)
        ]
        for dag in self.cron_dags.values():
            sched = dag.schedule_job
            if (
                sched is not None
                and sched.enabled
                and isinstance(sched.schedule, CronTab)
            ):
                entries.append(
                    ScheduleEntry(sched.name, sched.schedule, sched.timezone)
                )
        return entries

    def schedule_pressure_payload(
        self,
        hours: int = 24,
        tz_name: Optional[str] = None,
        entries: Optional[list[ScheduleEntry]] = None,
    ) -> dict[str, Any]:
        """The fleet collision heatmap (``GET /schedule/pressure``).

        Every enabled schedule's fires over the next ``hours``, bucketed
        into the hour-by-minute grid by :func:`schedule_pressure`, plus
        how many jobs were excluded as disabled or @reboot.  Raises
        ValueError for an unknown timezone name.  ``entries`` is an
        optional pre-built fleet snapshot (see
        :meth:`schedule_pressure_payload_async`); when None it is built
        here.
        """
        if entries is None:
            entries = self._schedule_entries()
        payload = schedule_pressure(
            entries,
            hours=hours,
            tz=self._zone_from_name(tz_name),
        )
        # a plain-list snapshot: this builder can run on an executor thread
        # (see the async wrapper), so do not iterate the live mapping from
        # off the loop.
        jobs = list(self.cron_jobs.values())
        payload["excluded"] = {
            "disabled": sum(1 for job in jobs if not job.enabled),
            "reboot": sum(
                1
                for job in jobs
                if job.enabled and not isinstance(job.schedule, CronTab)
            ),
        }
        return payload

    def schedule_duplicates_payload(
        self, entries: Optional[list[ScheduleEntry]] = None
    ) -> dict[str, Any]:
        """Semantically identical schedules (``GET /schedule/duplicates``).

        Groups via the engine's own equality (``*/5`` == ``0-59/5``), so
        the answer can never disagree with how the scheduler itself
        compares schedules across reloads.  ``entries`` is an optional
        pre-built fleet snapshot (see
        :meth:`schedule_duplicates_payload_async`); when None it is
        built here.
        """
        if entries is None:
            entries = self._schedule_entries()
        return {
            "jobs": len(entries),
            "groups": duplicate_schedules(entries),
        }

    def schedule_suggest_payload(
        self,
        period: str = "hourly",
        tz_name: Optional[str] = None,
        entries: Optional[list[ScheduleEntry]] = None,
    ) -> dict[str, Any]:
        """The least-loaded slot for a new job (``GET /schedule/suggest``).

        Raises ValueError for an unknown period or timezone name.
        ``entries`` is an optional pre-built fleet snapshot (see
        :meth:`schedule_suggest_payload_async`); when None it is built
        here.
        """
        if entries is None:
            entries = self._schedule_entries()
        return suggest_slot(
            entries,
            period=period,
            tz=self._zone_from_name(tz_name),
        )

    # The three builders above are pure CPU that can take seconds at fleet
    # scale. The wrappers below snapshot entries on the loop, then walk on
    # the executor; web handlers AND MCP tools go through them, so the
    # offload lives in one place. ValueError propagates unchanged.

    async def _schedule_payload_offload(
        self, build: Callable[..., dict[str, Any]], **kwargs: Any
    ) -> dict[str, Any]:
        entries = self._schedule_entries()
        return await asyncio.get_running_loop().run_in_executor(
            None, partial(build, entries=entries, **kwargs)
        )

    async def schedule_pressure_payload_async(
        self, hours: int = 24, tz_name: Optional[str] = None
    ) -> dict[str, Any]:
        """Executor-offloaded :meth:`schedule_pressure_payload`."""
        return await self._schedule_payload_offload(
            self.schedule_pressure_payload, hours=hours, tz_name=tz_name
        )

    async def schedule_duplicates_payload_async(self) -> dict[str, Any]:
        """Executor-offloaded :meth:`schedule_duplicates_payload`."""
        return await self._schedule_payload_offload(
            self.schedule_duplicates_payload
        )

    async def schedule_suggest_payload_async(
        self, period: str = "hourly", tz_name: Optional[str] = None
    ) -> dict[str, Any]:
        """Executor-offloaded :meth:`schedule_suggest_payload`."""
        return await self._schedule_payload_offload(
            self.schedule_suggest_payload, period=period, tz_name=tz_name
        )

    async def _web_schedule_pressure(
        self, request: web.Request
    ) -> web.Response:
        headers = self._web_headers()
        hours = self._web_int_query(request, "hours", default=24, lo=1, hi=168)
        tz = request.query.get("tz") or None
        # Validate up front and answer 400 from the requested name, so the
        # builder's exception text never reaches the response body.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        payload = await self.schedule_pressure_payload_async(hours, tz)
        return _json_response(payload, headers=headers)

    async def _web_schedule_duplicates(
        self, request: web.Request
    ) -> web.Response:
        return _json_response(
            await self.schedule_duplicates_payload_async(),
            headers=self._web_headers(),
        )

    async def _web_schedule_suggest(
        self, request: web.Request
    ) -> web.Response:
        headers = self._web_headers()
        period = request.query.get("period") or "hourly"
        tz = request.query.get("tz") or None
        # Validate both user inputs and answer 400 from the requested values,
        # so no builder exception text reaches the response body.  Timezone
        # first: the builder resolves the zone (a call argument) before the
        # period check runs, so a bad zone wins when both are wrong.
        tz_error = self._timezone_error(tz)
        if tz_error is not None:
            return _json_response(
                {"error": tz_error}, status=400, headers=headers
            )
        period_error = self._period_error(period)
        if period_error is not None:
            return _json_response(
                {"error": period_error}, status=400, headers=headers
            )
        payload = await self.schedule_suggest_payload_async(period, tz)
        return _json_response(payload, headers=headers)

    def _avg_duration(self, name: str) -> Optional[float]:
        """Mean runtime in seconds over retained history, or ``None``.

        The dashboard's own definition (:func:`_run_stats`), so the .ics
        feed's event lengths can never disagree with the run drawer.
        """
        runs = list(self.run_history.get(name) or [])
        avg = _run_stats(runs)["avg_duration"]
        return float(avg) if avg is not None else None

    def _calendar_entries(
        self, name: Optional[str] = None
    ) -> Optional[list[CalendarEntry]]:
        """The calendar renderer's rows: the fleet, or one job when ``name``.

        ``None`` for an unknown job; a known job with no timetable or a
        fleet of none is an empty list. Both feeds filter the same
        _schedule_entries snapshot, so they cannot disagree. Reads live
        state, so it runs on the loop; the render walks the immutable
        result on an executor.
        """
        if name is None:
            schedule_entries = sorted(
                self._schedule_entries(), key=lambda entry: entry.name
            )
        else:
            if self._job_or_dag_schedule(name) is None:
                return None
            schedule_entries = [
                entry
                for entry in self._schedule_entries()
                if entry.name == name
            ]
        return [
            CalendarEntry(
                entry.name,
                entry.tab,
                entry.timezone,
                self._avg_duration(entry.name),
            )
            for entry in schedule_entries
        ]

    def calendar_payload(
        self,
        name: Optional[str] = None,
        days: int = 14,
        per_job: int = 100,
        start: Optional[datetime.datetime] = None,
        now: Optional[datetime.datetime] = None,
        entries: Optional[list[CalendarEntry]] = None,
    ) -> Optional[str]:
        """The iCalendar feed text: the fleet, or one job when ``name``.

        One VEVENT per upcoming fire over ``[start, start+days)``, from
        the same occurrence walk the scheduler runs. ``None`` for an
        unknown job; no timetable renders as a valid empty calendar.
        ``start``/``now`` pin the window and DTSTAMP for tests; ``entries``
        is an optional pre-built snapshot.
        """
        if entries is None:
            entries = self._calendar_entries(name)
        if entries is None:
            return None
        calname = (
            "cronstable" if name is None else "cronstable: {}".format(name)
        )
        if start is None:
            start = get_now(datetime.timezone.utc)
        return render_calendar(
            entries,
            start=start,
            days=days,
            per_job_cap=per_job,
            calname=calname,
            now=now,
            prodid_version=cronstable.version.version,
        )

    async def _web_calendar_response(
        self, name: Optional[str], request: web.Request
    ) -> web.Response:
        days = self._web_int_query(request, "days", default=14, lo=1, hi=60)
        per_job = self._web_int_query(
            request, "limit", default=100, lo=1, hi=1000, alias="per_job"
        )
        # the entries snapshot reads live state, so it is taken on the
        # loop; the walk (jobs x fires, pure CPU) then runs on the
        # executor over the immutable snapshot, like the pressure/suggest
        # builders
        entries = self._calendar_entries(name)
        if entries is None:
            raise web.HTTPNotFound()
        text = await asyncio.get_running_loop().run_in_executor(
            None,
            partial(
                self.calendar_payload, name, days, per_job, entries=entries
            ),
        )
        # the feed's own Content-Type wins, in any spelling: see
        # _strip_content_type
        headers = _strip_content_type(self._web_headers())
        headers["Content-Disposition"] = 'inline; filename="cronstable.ics"'
        return web.Response(
            text=text,
            content_type="text/calendar",
            charset="utf-8",
            headers=headers,
        )

    async def _web_calendar(self, request: web.Request) -> web.Response:
        """The fleet-wide iCal feed (``GET /calendar.ics``)."""
        return await self._web_calendar_response(None, request)

    async def _web_job_calendar(self, request: web.Request) -> web.Response:
        """One job's iCal feed (``GET /jobs/{name}/calendar.ics``)."""
        return await self._web_calendar_response(
            request.match_info["name"], request
        )

    async def start_job_by_name(self, name: str) -> None:
        """Launch a job now (`POST /jobs/{name}/start`, MCP `cron_run_job`).

        Raises :class:`ApiActionError` for an unknown (404) or disabled (409)
        job; otherwise honours the job's concurrencyPolicy exactly as the
        scheduler would.  A PAUSED job may still be started manually: a pause
        skips scheduled fires only, and the operator asking by hand is the
        operator overriding their own pause (unlike `enabled: false`, which
        is config the API must not silently override).
        """
        try:
            job = self.cron_jobs[name]
        except KeyError as ex:
            raise ApiActionError(
                "job {!r} not found".format(name), status=404
            ) from ex
        if not job.enabled:
            # a disabled job behaves "as if it isn't there"; refuse to launch
            # it manually rather than silently overriding the config.
            raise ApiActionError(
                "job {!r} is disabled".format(name), status=409
            )
        # A manual start of a job still pending as a deferred @reboot
        # one-shot IS its boot run: retire the pending entry and record the
        # run with the cluster, or _process_pending_reboots would run the
        # one-shot a second time (possibly on another node). Recording
        # BEFORE spawning mirrors the deferred-launch path's at-most-once
        # ordering.
        if name in self._pending_reboot_jobs:
            mgr = self.cluster_manager
            if mgr is not None:
                await mgr.mark_reboot_ran(name)
            # pop, not del: a concurrent manual start can retire the entry
            # while the await above yields, and the loser of that race must
            # not 500 on a KeyError; mark_reboot_ran is idempotent and both
            # requests still launch below.
            if self._pending_reboot_jobs.pop(name, None) is not None:
                logger.info(
                    "cluster: manual start of deferred @reboot job %s counts "
                    "as its boot run; retiring the pending entry",
                    name,
                )
        # Same for a boot run a pause deferred: the manual start IS it, so
        # record the boot marker (the deferral left it unwritten) or the
        # pause lifting would run the one-shot a second time.
        if name in self._paused_reboot_jobs:
            self._paused_reboot_jobs.discard(name)
            logger.info(
                "manual start of @reboot job %s counts as the boot run its "
                "pause deferred",
                name,
            )
            if self._state_configured:
                await self._reboot_boot_gate(job)
        await self.maybe_launch_job(job)

    async def cancel_job_by_name(self, name: str) -> int:
        """Cancel a job's running instances; return how many were signalled.

        Behind ``POST /jobs/{name}/cancel`` and MCP ``cron_cancel_job``.
        Raises :class:`ApiActionError` for an unknown (404) or not-running
        (409) job.
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        running = list(self.running_jobs.get(name) or [])
        if not running:
            # nothing to cancel: report a conflict rather than a silent no-op
            # so the caller can tell the user the job was not running.
            raise ApiActionError(
                "job {!r} is not running".format(name), status=409
            )
        for runjob in running:
            # mark before cancelling so the reaper records this as a deliberate
            # "cancelled" run rather than a job failure (no report, no retry).
            runjob.cancelled = True
        # cancel instances concurrently: a job with several running instances
        # then costs at most one killTimeout, not one per instance.
        await asyncio.gather(
            *(rj.cancel() for rj in running if rj.proc is not None)
        )
        return len(running)

    def _set_pause(self, name: str, info: PauseInfo) -> None:
        """Install a pause window; the ONE writer that adds to ``_paused``.

        Every install (except ``_apply_reload``'s bulk prune and the ctor)
        funnels through here so the memo bust can never be forgotten: the
        pause must render on the next poll instead of aging out by TTL,
        the same discipline dagrun's ``_mutate`` applies to its summary
        memo.  The source-shape test pins the funnel.
        """
        self._paused[name] = info
        self._bust_response_memos()

    def _clear_pause(self, name: str) -> Optional[PauseInfo]:
        """Drop a pause window; the ONE writer that removes from ``_paused``.

        The removing half of :meth:`_set_pause`, under the same funnel
        discipline and the same source-shape pin.
        """
        # busts only when a window was actually dropped: popping an absent
        # entry changes no payload
        was = self._paused.pop(name, None)
        if was is not None:
            self._bust_response_memos()
        return was

    async def pause_job_by_name(
        self,
        name: str,
        *,
        duration: Optional[int] = None,
        until: Optional[datetime.datetime] = None,
        note: str = "",
        by: str = "api",
        channel: str = "api",
    ) -> dict[str, Any]:
        """Pause a job's scheduled fires (`POST /jobs/{name}/pause`).

        The single pause path (web, MCP, tests). While paused, due fires
        are skipped (synthetic "skipped" rows keep the catch-up watermark
        advancing), retries defer, and catch-up owes nothing; manual
        start/cancel and running instances are unaffected. One of
        ``duration`` or ``until``; neither means PAUSE_DEFAULT_SECONDS.
        Pausing a paused job overwrites the window. Raises ApiActionError
        404/400.
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        now = get_now(datetime.timezone.utc)
        if duration is not None and until is not None:
            raise ApiActionError(
                "give durationSeconds or until, not both", status=400
            )
        if until is None:
            seconds = PAUSE_DEFAULT_SECONDS if duration is None else duration
            if not 1 <= seconds <= PAUSE_MAX_SECONDS:
                raise ApiActionError(
                    "durationSeconds must be between 1 and {}".format(
                        PAUSE_MAX_SECONDS
                    ),
                    status=400,
                )
            until = now + datetime.timedelta(seconds=seconds)
        else:
            if until.tzinfo is None:
                until = until.replace(tzinfo=datetime.timezone.utc)
            if until <= now:
                raise ApiActionError("until is in the past", status=400)
            if (until - now).total_seconds() > PAUSE_MAX_SECONDS:
                raise ApiActionError(
                    "until is more than {} seconds away".format(
                        PAUSE_MAX_SECONDS
                    ),
                    status=400,
                )
        if len(note) > PAUSE_NOTE_MAX:
            raise ApiActionError(
                "note is longer than {} characters".format(PAUSE_NOTE_MAX),
                status=400,
            )
        if len(by) > PAUSE_BY_MAX:
            raise ApiActionError(
                "by is longer than {} characters".format(PAUSE_BY_MAX),
                status=400,
            )
        info = PauseInfo(
            since=now, until=until, note=note, by=by, channel=channel
        )
        replaced = self._paused.get(name)
        if replaced is not None:
            # pausing a paused job overwrites the window: close the old one
            # out at `now` so the time it already held is still credited, and
            # so the stretch the two windows share is not credited twice.
            self._sla_bank_pause(name, replaced, now)
        self._set_pause(name, info)
        # the job is excused from here on: drop any breach it latched while it
        # was still being evaluated, so the late gauge, the /jobs sla block and
        # the OVERDUE chip clear on this response rather than a minute later.
        self._sla_clear_latches(name)
        self.metrics.job_pause_state(name, True)
        self._persist_pause(name, info)
        logger.info(
            "Job %s paused until %s by %s (%s)%s",
            name,
            until.isoformat(),
            by,
            channel,
            ": " + note if note else "",
        )
        return info.to_dict()

    async def resume_job_by_name(
        self, name: str, *, by: str = "api", channel: str = "api"
    ) -> None:
        """Resume a paused job (`POST /jobs/{name}/resume`).

        A no-op for a job that is not paused, EXCEPT that the durable
        "resumed" record is appended regardless when a store is configured:
        a peer's pause this node has not refreshed into memory yet must
        still be revoked, or the fleet would keep skipping a job the
        operator just resumed.  Raises :class:`ApiActionError` for an
        unknown job (404).
        """
        if name not in self.cron_jobs:
            raise ApiActionError("job {!r} not found".format(name), status=404)
        was = self._clear_pause(name)
        self.metrics.job_pause_state(name, False)
        self._persist_resume(name, by, channel)
        if was is not None:
            self._sla_bank_pause(name, was, get_now(datetime.timezone.utc))
            logger.info("Job %s resumed by %s (%s)", name, by, channel)

    def _pause_active(self, name: str) -> Optional[PauseInfo]:
        """The job's live pause window, or ``None``; expiry enforced HERE.

        The one pause read every consumer goes through: an expired window
        reads as absent everywhere at once. The stale entry is swept by
        housekeeping; only memory is consulted, never store I/O on a
        scheduling path.
        """
        info = self._paused.get(name)
        if info is None:
            return None
        if info.until <= get_now(datetime.timezone.utc):
            return None
        return info

    def _pause_and_sla_periodic(self) -> None:
        """Per-minute pause and SLA housekeeping, guarded on its own.

        Deliberately NOT inside run()'s reload try/except: both passes
        need nothing the reload produces, and sharing that block would let
        an unparseable config silently stop the late-run monitor (the
        exact failure SLA exists to report) and strand expired pauses.
        """
        try:
            # pause expiry sweep + cross-node pause propagation; the sweep is
            # stateless, the durable refresh spawns a tracked task only when a
            # backend is up.
            self._pause_periodic()
            # per-job SLA checks: purely in-memory, so they run with or
            # without a state backend (hence a sibling of _state_periodic,
            # not part of it).
            self._sla_periodic()
        except Exception:  # pragma: nocover
            logger.exception("please report this as a bug (5)")

    def _pause_periodic(self) -> None:
        """Sweep expired pauses; refresh pause state from a shared store.

        The sweep is purely in-memory (expiry is already enforced at every
        read); the durable refresh re-reads the ``paused/`` streams as a
        tracked task so peers converge within about a minute. Buffered
        records are replayed BEFORE the refresh is spawned: the write tail
        is installed synchronously, so the same pass's refresh sees the
        write in flight and leaves the job's memory alone.
        """
        now = get_now(datetime.timezone.utc)
        for name, info in list(self._paused.items()):
            if info.until <= now:
                self._clear_pause(name)
                self._sla_bank_pause(name, info, info.until)
                self.metrics.job_pause_state(name, False)
                logger.info(
                    "Job %s: pause expired at %s; scheduled runs resume",
                    name,
                    info.until.isoformat(),
                )
        if self.state_backend is None:
            return
        self._replay_pending_pause_writes()
        loop_now = asyncio.get_running_loop().time()
        if loop_now >= self._pause_refresh_next and (
            self._pause_refresh_task is None or self._pause_refresh_task.done()
        ):
            self._pause_refresh_next = loop_now + PAUSE_REFRESH_INTERVAL
            self._pause_refresh_task = self._track_state_write(
                self._refresh_pauses_from_store()
            )

    async def _refresh_pauses_from_store(self) -> None:
        """Converge the in-memory pause map on the durable ``paused/`` streams.

        Cross-node propagation and the boot rehydrate: newest record per
        stream wins. A job with its own write in flight, or whose write
        generation moved mid-read, is skipped: memory is newer than the
        store for it. On any store trouble the LAST KNOWN in-memory state
        is kept (under both onStoreUnavailable policies): a pause is a
        convenience, not a correctness fence, and must never block firing.
        One unreadable stream keeps only that job's state; only a failed
        enumeration ends the pass.
        """
        backend = self.state_backend
        if backend is None:
            return
        try:
            streams = await asyncio.wait_for(
                backend.list_stream_names(PAUSE_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - keep last known state
            logger.warning(
                "state: cannot refresh pause state (keeping the last known "
                "in-memory state): %s",
                ex,
            )
            return
        now = get_now(datetime.timezone.utc)
        for stream in streams:
            name = stream[len(PAUSE_STREAM_PREFIX) :]
            if name not in self.cron_jobs:
                continue  # a removed job's stream; GC's business
            tail = self._pause_write_tail.get(name)
            if tail is not None and not tail.done():
                continue  # our own newer write has not landed yet
            if name in self._pause_pending_writes:
                # a buffered record (store down, or an append that failed)
                # means memory is newer than the stream for this job and the
                # write is still owed: applying the record it supersedes
                # would silently revoke the operator's pause or resume.
                continue
            gen = self._pause_gen.get(name, 0)
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(stream, limit=1, newest_first=True),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - keep last known state
                logger.warning(
                    "state: cannot refresh pause state for %s (keeping the "
                    "last known in-memory state): %s",
                    name,
                    ex,
                )
                # this job only: one unreadable stream must not starve
                # every job after it (the order is stable, so it would be
                # the same jobs every pass).
                continue
            if name not in self.cron_jobs:
                # a reload removed the job mid-read. The generation guard
                # below does not cover this (_apply_reload prunes
                # _pause_gen, so both generations read 0); installing here
                # would leave a permanent stale _paused entry and recreate
                # the pruned metric series, and no later sweep cleans up.
                continue
            if self._pause_gen.get(name, 0) != gen:
                # a local pause/resume landed while we were reading: memory
                # is newer than the snapshot in hand. Re-checking the write
                # tail is NOT enough, since a write that also completed
                # inside that window has already deleted its tail entry.
                continue
            info = self._pause_info_from_record(recs[0] if recs else None)
            if info is not None and info.until > now:
                known = self._paused.get(name)
                if known is not None and known.until != info.until:
                    # a different window replaces the known one: bank what
                    # the old one already held (see _sla_bank_pause).
                    self._sla_bank_pause(name, known, now)
                self._set_pause(name, info)
                self.metrics.job_pause_state(name, True)
                if known is None or known.until != info.until:
                    logger.info(
                        "Job %s: paused until %s via the shared state store",
                        name,
                        info.until.isoformat(),
                    )
            else:
                was = self._clear_pause(name)
                if was is not None:
                    self._sla_bank_pause(name, was, now)
                    self.metrics.job_pause_state(name, False)
                    logger.info(
                        "Job %s: resumed via the shared state store", name
                    )

    @staticmethod
    def _pause_info_from_record(
        rec: Optional[dict[str, Any]],
    ) -> Optional[PauseInfo]:
        """Rebuild a :class:`PauseInfo` from a durable ``paused`` record.

        ``None`` for anything else on top of the stream (a ``resumed``
        record, a foreign/corrupt record, an empty stream): the caller
        treats those identically as "not paused".  Expiry is NOT judged
        here; the caller compares ``until`` against its own clock.
        """
        if not rec or rec.get("kind") != "paused":
            return None
        until = _parse_iso_utc(rec.get("until"))
        if until is None:
            return None
        since = _parse_iso_utc(rec.get("since"))
        note = rec.get("note")
        by = rec.get("by")
        channel = rec.get("channel")
        return PauseInfo(
            since=since if since is not None else until,
            until=until,
            note=note if isinstance(note, str) else "",
            by=by if isinstance(by, str) else "",
            channel=channel if isinstance(channel, str) else "",
        )

    def _sla_periodic(self) -> None:
        """Evaluate the per-job SLA checks; latch, meter and report breaches.

        Purely in-memory, so it runs with no state backend (hence NOT part
        of _state_periodic). A disabled, paused, or not-owned job is
        excused: its latches are DROPPED, and a still-breaching job
        re-latches and pages once when eligible again. The (job, check)
        latch drives transitions: ok to breached fires onLate ONCE;
        breached to ok clears and logs, no report. Reporters are queued,
        never awaited on the scheduler loop.
        """
        if not self._any_sla() and not self._sla_state:
            # nothing declares an sla and nothing is latched: the walk
            # below would be a no-op, so skip it. The reload that adds an
            # sla block clears _any_sla_cache, so the next pass walks.
            return
        now = get_now(datetime.timezone.utc)
        for name, job in self.cron_jobs.items():
            if not job.has_sla:
                # no sla block (or a reload blanked it): latches must not
                # stay stuck at breached. has_sla is precomputed, so a
                # no-SLA deployment pays O(1) per job per pass.
                self._sla_clear_latches(name)
                continue
            if not job.enabled or self._pause_active(name) is not None:
                # excused: a pre-pause/disable breach would otherwise pin
                # the gauge, sla block and OVERDUE chip for the window.
                self._sla_clear_latches(name)
                if not job.enabled:
                    # roll the staleness baseline forward so re-enabling
                    # gives a full threshold (never-succeeded arm); a job
                    # that HAS succeeded gets its span banked below.
                    self._sla_first_seen[name] = now
                    self._sla_disabled_since.setdefault(name, now)
                continue
            # Just left a disabled span: bank it as a staleness credit like
            # a lifted pause, once, at the transition, BEFORE the cluster
            # gate so every node records the credit.
            disabled_since = self._sla_disabled_since.pop(name, None)
            if disabled_since is not None:
                self._sla_bank_pause(
                    name,
                    PauseInfo(
                        since=disabled_since,
                        until=now,
                        note="",
                        by="",
                        channel="",
                    ),
                    now,
                )
            if not self._cluster_allows(job):
                # not this node's job right now: same latch drop, and drop
                # the lateAfter reference too. A slot recorded while this
                # node owned the job would page the moment ownership came
                # back, for a slot the owner of the day ran on time.
                self._sla_clear_latches(name)
                self._sla_due.pop(name, None)
                continue
            observations = self._sla_observations(name, job, now)
            for check, (threshold, observed, breached) in observations.items():
                since = self._sla_state.get((name, check))
                # restated every pass (an idempotent dict write) so the
                # late series exists at 0, and the breach counter is
                # zero-filled, from the first evaluation: increase() then
                # has a baseline sample before the first breach.
                self.metrics.job_sla_late(name, check, breached)
                if breached and since is None:
                    self._sla_state[(name, check)] = now
                    self.metrics.job_sla_breach(name, check)
                    logger.warning(
                        "Job %s: SLA check %s breached: observed %.0fs "
                        "exceeds the threshold of %ss",
                        name,
                        check,
                        observed,
                        threshold,
                    )
                    self._queue_sla_report(job, check, threshold, observed)
                elif not breached and since is not None:
                    del self._sla_state[(name, check)]
                    logger.info(
                        "Job %s: SLA check %s recovered (observed %.0fs is "
                        "back within the threshold of %ss)",
                        name,
                        check,
                        observed,
                        threshold,
                    )
            # a reload can drop ONE check while keeping the sla block: clear
            # its stale latch the same way. Walks the fixed SLA_CHECKS set,
            # not the whole latch map, to stay O(checks) per job.
            for check in SLA_CHECKS:
                if check in observations:
                    continue
                if self._sla_state.pop((name, check), None) is not None:
                    self.metrics.job_sla_late(name, check, False)

    def _sla_clear_latches(self, name: str) -> None:
        """Drop every breach latch of ``name`` and clear its late gauge.

        Walks the fixed :data:`SLA_CHECKS` set rather than scanning the whole
        (name, check) latch map for this name's half, so clearing one job's
        latches stays O(checks) even when many other jobs are latched.
        """
        if not self._sla_state:
            return
        for check in SLA_CHECKS:
            if self._sla_state.pop((name, check), None) is not None:
                self.metrics.job_sla_late(name, check, False)

    def _sla_peer_owns_slot(self, name: str) -> None:
        """Excuse this node's lateAfter slot: a peer holds the run.

        A slot denied by a LIVE foreign holder was never this node's to
        launch; dropping the reference stops every race loser paging. The
        fail-closed denial (store did not answer) deliberately keeps the
        reference: no peer is known to have run it, so a breach is real.
        """
        self._sla_due.pop(name, None)

    def _sla_stale_reference(self, name: str) -> datetime.datetime:
        """The instant maxTimeSinceSuccess measures the job's staleness from.

        The job's newest known success, fed by :meth:`_record_run` and
        warmed from the durable ledger at rehydrate.  With none on record
        (a stateless daemon, or a job that has never succeeded) it falls
        back to when the job was first seen, so a fresh boot and a job a
        reload just added both age into the breach instead of paging
        instantly.  Process start is the last resort, for a name that
        somehow predates the first-seen map.
        """
        reference = self._sla_last_success.get(name)
        if reference is not None:
            return reference
        return self._sla_first_seen.get(name, self._process_start)

    def _sla_bank_pause(
        self, name: str, was: PauseInfo, ended_at: datetime.datetime
    ) -> None:
        """Bank a pause window that just ended, for the staleness credit.

        Called wherever a window leaves ``self._paused``. Spans are kept
        sorted and disjoint (insert + coalesce), so a shared stretch is
        never credited twice and an out-of-order window still merges
        whole. Windows the staleness reference has passed are dropped, and
        the rest capped: dropping the OLDEST understates the credit, which
        fails toward paging.
        """
        if ended_at > was.until:
            ended_at = was.until
        if ended_at <= was.since:
            return
        raw = sorted(
            self._sla_pause_windows.get(name, []) + [(was.since, ended_at)]
        )
        merged: list[tuple[datetime.datetime, datetime.datetime]] = []
        for start, end in raw:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        reference = self._sla_stale_reference(name)
        merged = [span for span in merged if span[1] > reference]
        del merged[:-SLA_PAUSE_SPANS_MAX]
        if merged:
            self._sla_pause_windows[name] = merged
        else:
            self._sla_pause_windows.pop(name, None)

    def _sla_paused_seconds(
        self, name: str, reference: datetime.datetime, now: datetime.datetime
    ) -> float:
        """Seconds of ``[reference, now]`` the job spent deliberately paused.

        Credited against maxTimeSinceSuccess so a held job gets a full
        threshold once the pause lifts, rather than paging unattended the
        instant it does.  The banked windows are disjoint
        (:meth:`_sla_bank_pause`) and clamped to the measured interval
        here, so no window counts twice and none counts before the
        reference the check is measuring from.
        """
        spans = self._sla_pause_windows.get(name)
        if not spans:
            return 0.0
        credit = 0.0
        for start, end in spans:
            overlap = (min(now, end) - max(reference, start)).total_seconds()
            if overlap > 0:
                credit += overlap
        return credit

    def _sla_observations(
        self, name: str, job: JobConfig, now: datetime.datetime
    ) -> dict[str, tuple[int, float, bool]]:
        """The job's configured SLA checks, freshly measured against ``now``.

        check label -> (threshold_seconds, observed_seconds, breached),
        one entry per non-null sla key, in the config block's order.
        Shared by the monitor (which latches on it) and the /jobs payload
        (which reports live observed values for latched checks), so both
        surfaces measure the same way.
        """
        out: dict[str, tuple[int, float, bool]] = {}
        threshold = job.sla.get("maxTimeSinceSuccessSeconds")
        if threshold is not None:
            reference = self._sla_stale_reference(name)
            # time the operator deliberately held the job is not staleness:
            # excluding it gives a resumed job a full threshold before it can
            # page, the same excusal lateAfter already gets in _launch_plan.
            observed = (now - reference).total_seconds() - (
                self._sla_paused_seconds(name, reference, now)
            )
            out[SLA_CHECK_STALE] = (threshold, observed, observed > threshold)
        threshold = job.sla.get("lateAfterSeconds")
        if threshold is not None:
            due = self._sla_due.get(name)
            if due is None:
                # no unexcused slot recorded yet this process: nothing to
                # be late FOR (also the restart baseline).
                out[SLA_CHECK_LATE] = (threshold, 0.0, False)
            else:
                started = self._sla_last_start.get(name)
                observed = (now - due).total_seconds()
                out[SLA_CHECK_LATE] = (
                    threshold,
                    observed,
                    observed > threshold
                    and (started is None or started < due)
                    # an instance is still running, so the slot the
                    # concurrency policy dropped is not an unserved slot:
                    # an overrun is maxRuntime's to report, once. Read with
                    # .get(): running_jobs is a defaultdict, and a bare
                    # subscript here would mint a phantom key.
                    and not self.running_jobs.get(name),
                )
        threshold = job.sla.get("maxRuntimeSeconds")
        if threshold is not None:
            observed = 0.0
            for runjob in self.running_jobs.get(name) or []:
                # started_at is the run's aware-UTC launch instant (the
                # same field the /jobs/{name}/resources payload reports);
                # never killed here, the check only observes.
                if runjob.started_at is None:
                    continue
                running_for = (now - runjob.started_at).total_seconds()
                if running_for > observed:
                    observed = running_for
            out[SLA_CHECK_RUNTIME] = (
                threshold,
                observed,
                observed > threshold,
            )
        return out

    def _install_tail_task(
        self,
        tail: dict[str, asyncio.Task],
        name: str,
        body: Callable[[], Coroutine[Any, Any, None]],
        *,
        spawn: Callable[[Coroutine[Any, Any, None]], asyncio.Task],
        after: Optional[Iterable[Optional[asyncio.Task]]] = None,
    ) -> asyncio.Task:
        """Spawn ``body()`` behind ``name``'s current tail and become the tail.

        The ONE implementation of the per-name chained-tail idiom (the
        completion, SLA-report, inflight, retry-write and pause-write
        paths).  Predecessors are awaited for ordering only (``asyncio.wait``,
        so their errors stay their own); ``after`` overrides the default
        single-predecessor list for a chain that must order behind another
        chain too.  ``spawn`` turns the ordered coroutine into the tracked
        task.  The done-callback drops the registration only while it is
        still this task's, so a successor installed meanwhile is never
        clobbered.

        ``body`` MUST be a factory, not a coroutine, called inside the
        ordered wrapper: the wrapper is what :meth:`_track_state_write`
        closes when it sheds a write under overload, and a coroutine built
        eagerly by the caller would then never be awaited nor closed (a
        ``RuntimeWarning`` per shed write, an error under ``-W error``).
        """
        earlier = list(after) if after is not None else [tail.get(name)]

        async def _ordered() -> None:
            for prev in earlier:
                if prev is not None and not prev.done():
                    await asyncio.wait({prev})
            await body()

        task = spawn(_ordered())
        tail[name] = task

        def _clear(done: asyncio.Task) -> None:
            if tail.get(name) is done:
                del tail[name]

        task.add_done_callback(_clear)
        return task

    def _spawn_completion(
        self, coro: Coroutine[Any, Any, None]
    ) -> asyncio.Task:
        """A task in ``_completion_tasks``, so shutdown drains it
        (:meth:`_drain_completions`)."""
        task = asyncio.create_task(coro)
        self._completion_tasks.add(task)
        task.add_done_callback(self._completion_tasks.discard)
        return task

    def _queue_sla_report(
        self, job: JobConfig, check: str, threshold: int, observed: float
    ) -> None:
        """Dispatch one onLate report as a tracked, per-job-chained task.

        Reporters can take tens of seconds and never run inline on the
        loop. The report orders behind ``_completion_tail`` but installs
        into its OWN ``_sla_report_tail``: a monitor report must never sit
        in FRONT of a real run's report+retry-arm sequence (maxRuntime
        breaches mid-run, making that the ordinary case). Membership in
        ``_completion_tasks`` still means shutdown drains it.
        """
        name = job.name
        last_success = self._sla_last_success.get(name)
        ctx = SlaBreachContext(
            job,
            check=check,
            threshold_seconds=threshold,
            observed_seconds=observed,
            last_success_at=(
                last_success.isoformat() if last_success is not None else None
            ),
        )
        report_config = job.onLate["report"]

        async def _report() -> None:
            try:
                await report_sla_breach(ctx, report_config)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error reporting the SLA breach of job %s; "
                    "please report this as a bug (7)",
                    name,
                )

        self._install_tail_task(
            self._sla_report_tail,
            name,
            _report,
            spawn=self._spawn_completion,
            after=[
                self._completion_tail.get(name),
                self._sla_report_tail.get(name),
            ],
        )

    async def _web_start_job(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        try:
            await self.start_job_by_name(name)
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        # a minimal JSON ack in the MCP cron_run_job shape; this route once
        # returned an empty 200 while every sibling action returned JSON.
        return _json_response({"started": name}, headers=self._web_headers())

    async def _web_cancel_job(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        try:
            count = await self.cancel_job_by_name(name)
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        # the MCP cron_cancel_job shape, instances included
        return _json_response(
            {"cancelled": name, "instances": count},
            headers=self._web_headers(),
        )

    async def _web_pause_job(self, request: web.Request) -> web.Response:
        body = await self._web_json_body(request)
        duration = body.get("durationSeconds")
        # bool is an int subclass; `true` must not read as one second.
        if duration is not None and (
            not isinstance(duration, int) or isinstance(duration, bool)
        ):
            raise _api_error(
                web.HTTPBadRequest, "durationSeconds must be an integer"
            )
        until_raw = body.get("until")
        until = None
        if until_raw is not None:
            if not isinstance(until_raw, str):
                raise _api_error(
                    web.HTTPBadRequest,
                    "until must be an ISO-8601 timestamp string",
                )
            until = _parse_iso_utc(until_raw)
            if until is None:
                raise _api_error(
                    web.HTTPBadRequest,
                    "until is not a valid ISO-8601 timestamp",
                )
        note = body.get("note")
        if note is not None and not isinstance(note, str):
            raise _api_error(web.HTTPBadRequest, "note must be a string")
        by = body.get("by")
        if by is not None and not isinstance(by, str):
            raise _api_error(web.HTTPBadRequest, "by must be a string")
        try:
            paused = await self.pause_job_by_name(
                request.match_info["name"],
                duration=duration,
                until=until,
                note=note or "",
                by=by or "api",
                channel="api",
            )
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return _json_response({"paused": paused}, headers=self._web_headers())

    async def _web_resume_job(self, request: web.Request) -> web.Response:
        body = await self._web_json_body(request)
        by = body.get("by")
        if by is not None and not isinstance(by, str):
            raise _api_error(web.HTTPBadRequest, "by must be a string")
        try:
            await self.resume_job_by_name(
                request.match_info["name"], by=by or "api", channel="api"
            )
        except ApiActionError as ex:
            raise self._action_http_error(ex) from ex
        return _json_response({"paused": None}, headers=self._web_headers())

    def _action_http_error(self, ex: "ApiActionError") -> web.HTTPException:
        # historical parity: web.headers ride the 409 conflict bodies of the
        # start/cancel routes, but NOT their 404 (unknown job).
        headers = self._web_headers() if ex.status == 409 else None
        return _http_for_action_error(ex, headers)

    def _security_headers(self) -> dict[str, str]:
        """Security headers for the dashboard HTML page.

        Secure defaults (CSP, anti-clickjacking, nosniff) with any operator
        ``web.headers`` merged on top, so an operator who deliberately sets one
        of these (e.g. a relaxed CSP or framing policy) still wins.
        """
        headers = dict(WEB_SECURITY_HEADERS)
        custom = self._web_headers()
        if custom:
            headers.update(custom)
        return headers

    async def _web_index(self, request: web.Request) -> web.Response:
        raw, etag = _index_document()
        # the page's own Content-Type wins, in any spelling: see
        # _strip_content_type
        headers = _strip_content_type(self._security_headers())
        # a validator a cache or proxy can revalidate against; stable for the
        # life of the process, since the document is package data.
        headers["ETag"] = etag
        headers["Cache-Control"] = "no-cache"
        # the body varies with Accept-Encoding, so a shared cache must key on
        # it. Set after the operator's header merge (like GET /jobs) because
        # this is a correctness requirement, not a preference.
        headers["Vary"] = "Accept-Encoding"
        if request.headers.get("If-None-Match") == etag:
            # the document is immutable for the process lifetime, so a repeat
            # load revalidates into an empty 304 instead of resending 573 KB.
            return web.Response(status=304, headers=headers)
        if _accepts_gzip(request.headers.get("Accept-Encoding")):
            headers["Content-Encoding"] = "gzip"
            body = _index_gzip()
        else:
            body = raw
        return web.Response(
            body=body,
            content_type="text/html",
            charset="utf-8",
            headers=headers,
        )

    def _scheduled_in(
        self,
        name: str,
        job: JobConfig,
        running: bool,
        now: Optional[datetime.datetime] = None,
    ) -> Optional[float]:
        """Seconds until the job's next scheduled run.

        ``None`` when not applicable: disabled, currently running, a
        one-off ``@reboot`` schedule, or a schedule with no future
        occurrence.  Steady state reads the loop's own next-fire index (the
        same source prometheus.py's next-run gauge reads) rather than
        re-walking the crontab; the engine search survives only as the
        fallback for the startup window before the index is seeded.

        ``now`` (aware UTC) lets a caller looping over the whole job set
        read the clock ONCE for the pass, which also makes the snapshot
        internally consistent: every countdown measured from one instant.
        """
        if not job.enabled or running:
            return None
        crontab: CronTab | str = job.schedule
        if not isinstance(crontab, CronTab):
            return None
        when = self._next_fire.get(name)
        if when is not None:
            # clamped at zero: a job due this very instant reads as
            # marginally past until the pass advances it beyond its slot
            if now is None:
                now = get_now(datetime.timezone.utc)
            return max(0.0, (when - now).total_seconds())
        if name in self._dead_schedules:
            # no future occurrence; the engine's answer would be None too,
            # found only after walking the remaining horizon
            return None
        seconds: Optional[float] = crontab.next(
            now=get_now(job.timezone), default_utc=job.utc
        )
        return seconds

    def _schedule_never_fires(self, name: str, job: JobConfig) -> bool:
        """True when an enabled cron job's schedule has no future instant.

        Holds for running jobs too (a running job with a dead schedule
        still never fires again).  Shared by the /jobs and /status
        payloads so the two surfaces cannot drift.  Steady state is two
        set probes (the next-fire index and the dead-schedules latch); the
        full engine search survives only as the unseeded-startup fallback.
        """
        if not (job.enabled and isinstance(job.schedule, CronTab)):
            return False
        if name in self._next_fire:
            return False
        if name in self._dead_schedules:
            return True
        return (
            job.schedule.next(now=get_now(job.timezone), default_utc=job.utc)
            is None
        )

    def fleet_job_summaries(self) -> dict[str, Any]:
        """Compact per-job snapshot gossiped to peers for the fleet view.

        Piggybacked on every ``/peer`` response. Deliberately lean (it
        travels in a byte-capped gossip payload): no command line, no
        fail_reason, no history; those stay on the owning node's API.
        """
        out: dict[str, Any] = {}
        # one clock read for the whole pass; see _scheduled_in's `now`
        now = get_now(datetime.timezone.utc)
        for name, job in self.cron_jobs.items():
            running = bool(self.running_jobs.get(name))
            last = self.last_run.get(name)
            out[name] = {
                "running": running,
                "enabled": job.enabled,
                "scheduled_in": self._scheduled_in(name, job, running, now),
                "last": (
                    None
                    if last is None
                    else {
                        "outcome": last.outcome,
                        "finished_at": last.finished_at.isoformat(),
                        "duration": last.duration,
                        "exit_code": last.exit_code,
                    }
                ),
            }
        return out

    def _job_to_dict(
        self,
        name: str,
        job: JobConfig,
        now: Optional[datetime.datetime] = None,
    ) -> dict[str, Any]:
        running = self.running_jobs.get(name) or []
        # next scheduled run, in seconds; None when not applicable (disabled,
        # currently running, or a one-off @reboot schedule).  ``now`` is the
        # caller's pass instant when it is looping the job set; see
        # _scheduled_in.
        scheduled_in = self._scheduled_in(name, job, bool(running), now)
        # a dead schedule's None means NEVER, distinct from the running/
        # disabled Nones. For a non-running job _scheduled_in already
        # answered; only a running job needs the direct probe.
        if running:
            never_fires = self._schedule_never_fires(name, job)
        else:
            never_fires = (
                job.enabled
                and isinstance(job.schedule, CronTab)
                and scheduled_in is None
            )

        last = self.last_run.get(name)
        last_run = last.to_dict() if last is not None else None

        history = self.run_history.get(name)
        # compact, oldest-first tail of recent runs for the inline sparkline:
        # only outcome + duration are needed there, so the per-poll payload
        # stays small. Full per-run detail comes from /jobs/{name}/runs.
        recent = (
            [
                {"outcome": r.outcome, "duration": r.duration}
                for r in list(history)[-JOBS_INLINE_HISTORY:]
            ]
            if history
            else []
        )

        result: dict[str, Any] = {
            "name": name,
            "enabled": job.enabled,
            # pure functions of the config, precomputed on the JobConfig at
            # build time (see JobConfig._precompute_payload_views).
            "schedule": job.schedule_display,
            "command": job.command_display,
            "captureStdout": job.captureStdout,
            "captureStderr": job.captureStderr,
            # the schedule's reference frame, so the dashboard can compute and
            # label upcoming run times (utc=True is the default; timezone, when
            # set, is an IANA name like "America/Los_Angeles").
            "utc": job.utc,
            "timezone": (
                str(job.timezone) if job.timezone is not None else None
            ),
            "running": bool(running),
            "pids": [
                runjob.proc.pid
                for runjob in running
                if runjob.proc is not None
            ],
            "scheduled_in": scheduled_in,
            "never_fires": never_fires,
            # advisory lint from config load (see JobConfig), so the
            # dashboards can badge footguns without re-deriving them
            "schedule_findings": job.schedule_findings_json,
            "last_run": last_run,
            "history": recent,
            # the active pause window, or null. Always present (unlike the
            # conditional blocks below) so the dashboards can bind to it
            # without probing: paused is first-class job state, not an
            # optional feature's extra.
            "paused": (
                pause.to_dict()
                if (pause := self._pause_active(name)) is not None
                else None
            ),
        }
        if job.schedule_resolved_or_none is not None:
            # the H hash form: also ship the plain-dialect spelling it
            # resolved to, so the dashboards display the H the user wrote
            # while their client-side engines (which know no H) compute
            # previews from this. Omitted for every other schedule, which is
            # why the precomputed attribute is None-or-string rather than a
            # bool plus a lookup.
            result["schedule_resolved"] = job.schedule_resolved_or_none
        # live CPU/memory of the currently-running instances (monitorResources
        # jobs only). Summed across instances so a job running N copies shows
        # its aggregate footprint; omitted entirely when nothing is monitored
        # or no sample has landed yet, so an unmonitored job's payload is
        # unchanged.
        live_snaps = [
            snap
            for runjob in running
            if (snap := runjob.live_resources()) is not None
        ]
        if live_snaps:
            result["running_resources"] = {
                "cpu_percent": sum(s["cpu_percent"] for s in live_snaps),
                "cpu_seconds": sum(s["cpu_seconds"] for s in live_snaps),
                "rss_bytes": sum(s["rss_bytes"] for s in live_snaps),
                "instances": len(live_snaps),
            }
        # armed retry ladder: attempt/backoff for the dashboard chip.
        # Gated on count > 0: the ladder is created eagerly at launch with
        # count 0, so presence alone would flag healthy jobs.
        retry_state = self.retry_state.get(name)
        if (
            retry_state is not None
            and not retry_state.cancelled
            and retry_state.count > 0
        ):
            retry_cfg = job.onFailure.get("retry", {}) if job.onFailure else {}
            max_retries = retry_cfg.get("maximumRetries")
            result["retry"] = {
                "attempt": retry_state.count,
                # -1 means unlimited; surface as null (no ceiling to render).
                "maxAttempts": None if max_retries == -1 else max_retries,
                "nextRetryAt": (
                    retry_state.next_retry_at.isoformat()
                    if retry_state.next_retry_at is not None
                    else None
                ),
                "delaySeconds": retry_state.scheduled_delay,
            }
        # per-job SLA introspection, only when a check is configured:
        # thresholds, latched verdict, and live breach detail (observed is
        # re-measured at payload time, not the minute-old latch snapshot).
        # has_sla is precomputed, keeping the allocation off no-SLA jobs.
        if job.has_sla:
            thresholds = job.sla_thresholds
            observations = self._sla_observations(
                name, job, get_now(datetime.timezone.utc)
            )
            breaches = []
            for check, (
                threshold,
                observed,
                _breached,
            ) in observations.items():
                since = self._sla_state.get((name, check))
                if since is None:
                    continue
                breaches.append(
                    {
                        "check": check,
                        "since": since.isoformat(),
                        "observed_seconds": observed,
                        "threshold_seconds": threshold,
                    }
                )
            result["sla"] = {
                "thresholds": thresholds,
                "state": "late" if breaches else "ok",
                "breaches": breaches,
            }
        # a deferred @reboot one-shot still awaiting its boot run (the cluster
        # had not elected an owner at boot, or a pause is holding it): lets
        # the dashboard distinguish "pending boot run" from "already ran".
        if (
            name in self._pending_reboot_jobs
            or name in self._paused_reboot_jobs
        ):
            result["rebootPending"] = True
        # cluster-wide concurrency slot (concurrencyScope: cluster): whether
        # THIS node holds the job's slot lease and how many live instances
        # reference it. Only emitted for cluster-scoped jobs.
        if job.concurrencyScope == "cluster":
            # _slot_leases/_slot_refs are keyed by plain JOB name (only the
            # on-disk lease/stream name carries the "slots/" prefix; see
            # _slot_name and _claim_cluster_slot).
            lease = self._slot_leases.get(name)
            result["concurrencyScope"] = "cluster"
            result["slot"] = {
                "held": lease is not None,
                "holder": lease.holder if lease is not None else None,
                "refs": self._slot_refs.get(name, 0),
            }
        # only relevant when leader election is on, so omit it otherwise to
        # keep the per-poll payload lean for the common single-instance case.
        if self._elect_leader_configured:
            result["clusterPolicy"] = job.clusterPolicy
            # Under spread distribution each leader-gated job has its own
            # owner, so surface it for the dashboard (None = no quorum)
            # and EveryNode has no single owner.
            mgr = self.cluster_manager
            if (
                mgr is not None
                and mgr.distribution == "spread"
                and job.clusterPolicy != "EveryNode"
            ):
                result["clusterOwner"] = (
                    mgr.available_job_owner(job.name)
                    if job.clusterPolicy == "PreferLeader"
                    else mgr.job_owner(job.name)
                )
        return result

    def jobs_payload(self) -> list[dict[str, Any]]:
        """Full per-job dicts for ``GET /jobs`` and MCP ``cron_list_jobs``."""
        # one clock read for the whole pass; see _scheduled_in's `now`
        now = get_now(datetime.timezone.utc)
        return [
            self._job_to_dict(name, job, now)
            for name, job in self.cron_jobs.items()
        ]

    def job_detail_payload(self, name: str) -> Optional[dict[str, Any]]:
        """One job's full dict, or ``None`` when there is no such job."""
        job = self.cron_jobs.get(name)
        if job is None:
            return None
        return self._job_to_dict(name, job)

    async def _web_list_jobs(self, request: web.Request) -> web.Response:
        # One product (payload build + ETag + body + gzip) is shared across
        # every poller for _JOBS_RESPONSE_TTL, busted by the local events
        # that change the payload (_bust_response_memos). Only the
        # If-None-Match compare and the representation pick are per-request.
        return await self._memoized_conditional_response(
            request,
            self._jobs_response_memo,
            _JOBS_RESPONSE_TTL,
            self._build_jobs_product,
        )

    async def _build_jobs_product(
        self,
    ) -> tuple[str, bytes, Optional[bytes]]:
        # Build on the loop (it reads live scheduler state), then hash +
        # serialize off it for a large fleet.  The tag is keyed on the
        # ABSOLUTE next-fire, not the relative scheduled_in, so it stays
        # put while the countdown ticks and moves when a fire lands.
        payload = self.jobs_payload()
        # A plain snapshot of the index, NOT a per-job isoformat sweep:
        # the instants change at most once per job per fire. The
        # canonical dump renders them itself, inside the executor.
        # Snapshotted rather than passed by reference so the product
        # stays free of scheduler state in the executor branch.
        next_fire = dict(self._next_fire)
        if len(payload) >= _JOBS_SERIALIZE_OFFLOAD_MIN:
            return await asyncio.get_running_loop().run_in_executor(
                None, _jobs_response_product, payload, next_fire
            )
        return _jobs_response_product(payload, next_fire)

    def _bust_response_memos(self) -> None:
        """Drop the shared endpoint products so a local change renders now.

        Called from exactly the local events that change the payloads (run
        recorded, launch, pause set/cleared, reload); everything else ages
        out by TTL. All four memos bust together: they render the same
        local facts and must not disagree. The generation bump stops a
        build that STARTED pre-bust from re-populating the slot late (see
        _shared_response_product).
        """
        self._memo_gen += 1
        self._jobs_response_memo.cached = None
        for memo in self._metrics_response_memo.values():
            memo.cached = None
        self._fleet_response_memo.cached = None
        self._activity_response_memo.cached = None

    async def _web_get_job(self, request: web.Request) -> web.Response:
        """One job's full detail dict (``GET /jobs/{name}``).

        The identical per-job shape _job_to_dict builds for /jobs, so a
        client can refresh one job without pulling the fleet. 404 for an
        unknown job.
        """
        payload = self.job_detail_payload(request.match_info["name"])
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(payload, headers=self._web_headers())

    # --- DAG introspection + control --------------------------------------

    def _web_headers(self) -> Any:
        """The operator-configured ``web.headers`` map (or ``None``).

        The ONE spelling of this lookup: every handler reads it through
        here, so a future policy change edits one method, not every route.
        """
        assert self.web_config is not None
        return self.web_config.get("headers", None)

    async def dags_payload(self) -> list[dict[str, Any]]:
        """Configured DAGs + tasks (`GET /dags`, MCP `cron_list_dags`)."""
        dags = await self._dag.list_dags()
        # graft the human-readable schedule string here (schedule_str lives in
        # this module; dagrun cannot import it without a cycle).
        for entry in dags:
            cfg = self.cron_dags.get(entry["name"])
            if cfg is not None and cfg.schedule_job is not None:
                entry["schedule"] = schedule_str(cfg.schedule_job)
        return dags

    async def _web_list_dags(self, request: web.Request) -> web.Response:
        # Between run advances this body is byte-stable (static graphs plus
        # the memoized rollup), and it is the third leg of the dashboard's
        # per-poll fan-out: the conditional path lets an unchanged poll cost
        # a 304 instead of re-shipping every dag's task graph, and a changed
        # one ship gzipped.
        return _cachable_json_response(
            await self.dags_payload(),
            if_none_match=request.headers.get("If-None-Match"),
            gzip_ok=_accepts_gzip(request.headers.get("Accept-Encoding")),
            headers=self._web_headers(),
        )

    async def _web_dag_runs(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        limit = self._web_int_query(request, "limit", default=50, lo=1, hi=500)
        runs = await self._dag.list_runs(name, limit=limit)
        if runs is None:
            raise _api_error(
                web.HTTPNotFound, "dag {!r} not found".format(name)
            )
        # the subject rides under "name" too, the key the job runs payload
        # uses, so generic clients can read both runs endpoints one way
        return _json_response(
            {"dag": name, "name": name, "runs": runs},
            headers=self._web_headers(),
        )

    async def _web_dag_run(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        body = await self._dag.get_run(name, run_key)
        if body is None:
            raise _api_error(
                web.HTTPNotFound,
                "dag {!r} has no run {!r}".format(name, run_key),
            )
        return _json_response(body, headers=self._web_headers())

    async def _web_dag_xcom(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        result = await self._dag.xcom_for_run(name, run_key)
        if result is None:
            raise _api_error(
                web.HTTPNotFound,
                "dag {!r} has no run {!r}".format(name, run_key),
            )
        return _json_response(result, headers=self._web_headers())

    # --- durable state inspector (metadata-only) --------------------------

    async def _web_state(self, request: web.Request) -> web.Response:
        return _json_response(
            await self.state_payload(),
            headers=self._web_headers(),
        )

    async def state_payload(self) -> dict[str, Any]:
        """Store health + topology for the state inspector, metadata only.

        Per-prefix stream/document counts, capped scope lists, and active
        leases, never a record payload or a KV value.  Also carries THIS
        node's live retry ladder and held concurrency slots (straight from
        memory).  ``enabled: false`` when no state backend is configured.
        Behind ``GET /state`` and MCP ``cron_inspect_state``.
        """
        backend = self.state_backend
        if backend is None:
            return {"enabled": False}
        try:
            inv = await asyncio.wait_for(
                backend.inventory(), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade to health only
            logger.warning("state: inventory failed (%s)", ex)
            inv = {
                "view": backend.view_dict(),
                "stats": backend.stats(),
                "enumerable": False,
                "records": {},
                "documents": {},
                "leases": [],
                "quarantine": 0,
            }
        inv["enabled"] = True
        # this node's freshest HA state, straight from memory (no store read).
        inv["node"] = {
            "host": self._state_host,
            "retries": [
                {
                    "job": name,
                    "attempt": st.count,
                    "nextRetryAt": (
                        st.next_retry_at.isoformat()
                        if st.next_retry_at is not None
                        else None
                    ),
                    "delaySeconds": st.scheduled_delay,
                }
                for name, st in self.retry_state.items()
                if not st.cancelled and st.count > 0
            ],
            "slots": [
                {
                    "slot": slot_name,
                    "holder": lease.holder,
                    "fence": lease.fence,
                    "expiresAt": lease.expires_at,
                    "refs": self._slot_refs.get(slot_name, 0),
                }
                for slot_name, lease in self._slot_leases.items()
            ],
        }
        return inv

    async def state_documents_payload(self, ns: str) -> dict[str, Any]:
        """Documents of one KV/cursor/idempotency namespace, redacted.

        KV values are stripped to a ``valueSize``/``valueType`` summary
        (metadata-only stance); cursor watermarks and idempotency claim
        metadata are returned verbatim (no user secret there).  Behind
        ``GET /state/documents`` and MCP ``cron_inspect_state``.  Raises
        :class:`ApiActionError` when there is no store or ``ns`` is not an
        inspectable namespace.
        """
        backend = self.state_backend
        if backend is None:
            raise ApiActionError("state store is not configured", status=404)
        if not ns.startswith(("kv/", "cursor/", "idem/")):
            raise ApiActionError(
                "ns must be a kv/, cursor/ or idem/ namespace", status=400
            )
        try:
            docs = await asyncio.wait_for(
                backend.list_documents(ns), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade to empty
            docs = []
        # KV values are stripped to a size/type summary (a KV value is
        # arbitrary job-authored data that may be sensitive). Cursor
        # watermarks are returned verbatim ON PURPOSE: they are small
        # progress markers (a timestamp / offset / id), the operator opted
        # into seeing them, and hiding them would gut the cursor panel.
        # Idempotency docs carry only key/claimedAt/expiresAt (no value).
        redact_values = ns.startswith("kv/")
        out = []
        for doc in docs:
            if redact_values and "value" in doc:
                value = doc.get("value")
                summary = {k: v for k, v in doc.items() if k != "value"}
                try:
                    summary["valueSize"] = len(
                        json.dumps(value).encode("utf-8")
                    )
                except (TypeError, ValueError):
                    summary["valueSize"] = None
                summary["valueType"] = type(value).__name__
                out.append(summary)
            else:
                out.append(doc)
        return {"namespace": ns, "documents": out}

    async def _web_state_documents(self, request: web.Request) -> web.Response:
        try:
            payload = await self.state_documents_payload(
                request.query.get("ns", "")
            )
        except ApiActionError as ex:
            raise _http_for_action_error(ex) from ex
        return _json_response(payload, headers=self._web_headers())

    async def state_records_payload(
        self, stream: str, limit: int = 100
    ) -> dict[str, Any]:
        """Newest records of one stream, metadata-only.

        Archived-output (``logs/``) streams are refused: they carry raw job
        output, which the metadata-only stance keeps off this surface.  Behind
        ``GET /state/records`` and MCP ``cron_inspect_state``.  Raises
        :class:`ApiActionError` for a missing store, empty stream, or a log
        stream.
        """
        backend = self.state_backend
        if backend is None:
            raise ApiActionError("state store is not configured", status=404)
        if not stream:
            raise ApiActionError("a stream is required", status=400)
        if stream.startswith("logs/") or stream == "logs":
            # archived job output: raw content, excluded from the metadata
            # inspector on purpose.
            raise ApiActionError(
                "log streams carry raw output and are not inspectable",
                status=403,
            )
        try:
            recs = await asyncio.wait_for(
                backend.list_records(stream, limit=limit, newest_first=True),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade to empty
            recs = []
        return {"stream": stream, "records": recs}

    async def _web_state_records(self, request: web.Request) -> web.Response:
        limit = self._web_int_query(
            request, "limit", default=100, lo=1, hi=500
        )
        try:
            payload = await self.state_records_payload(
                request.query.get("stream", ""), limit=limit
            )
        except ApiActionError as ex:
            raise _http_for_action_error(ex) from ex
        return _json_response(payload, headers=self._web_headers())

    async def _web_dag_trigger(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = await self._dag.trigger_run(name)
        if run_key is None:
            raise _api_error(
                web.HTTPNotFound, "dag {!r} not found".format(name)
            )
        return _json_response(
            {"dag": name, "name": name, "runKey": run_key},
            headers=self._web_headers(),
        )

    async def _web_dag_backfill(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        payload = await self._web_json_body(request)
        start = payload.get("from")
        end = payload.get("to")
        if not isinstance(start, str) or not isinstance(end, str):
            raise _api_error(
                web.HTTPBadRequest,
                "backfill needs string `from` and `to` ISO dates",
            )
        result = await self._dag.backfill(name, start, end)
        if not result.get("ok"):
            raise _api_error(web.HTTPBadRequest, str(result.get("reason")))
        return _json_response(result, headers=self._web_headers())

    async def _web_dag_decision(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        taskkey = request.match_info["taskkey"]
        payload = await self._web_json_body(request)
        decision = payload.get("decision")
        if decision not in ("approve", "reject"):
            raise _api_error(
                web.HTTPBadRequest,
                "decision must be 'approve' or 'reject'",
            )
        by = str(payload.get("by") or "api")
        result = await self._dag.approve(
            name, run_key, taskkey, approved=(decision == "approve"), by=by
        )
        if not result.get("ok"):
            raise _api_error(web.HTTPConflict, str(result.get("reason")))
        return _json_response(result, headers=self._web_headers())

    @staticmethod
    def _web_int_query(
        request: web.Request,
        name: str,
        *,
        default: int,
        lo: int,
        hi: int,
        alias: Optional[str] = None,
    ) -> int:
        """A clamped integer query param; falls back to ``default`` on a
        missing or unparseable value (a bad query is never a 400 here).

        ``alias`` is a legacy spelling read only when ``name`` is absent;
        every capped listing reads ``limit`` first.
        """
        raw = request.query.get(name)
        if raw is None and alias is not None:
            raw = request.query.get(alias)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, value))

    @staticmethod
    async def _web_json_body(request: web.Request) -> dict[str, Any]:
        if not request.can_read_body:
            return {}
        try:
            body = await request.json()
        except Exception as ex:  # noqa: BLE001 - a malformed body is a 400
            raise _api_error(
                web.HTTPBadRequest, "request body is not valid JSON"
            ) from ex
        if not isinstance(body, dict):
            raise _api_error(
                web.HTTPBadRequest, "request body must be a JSON object"
            )
        return body

    def job_runs_payload(self, name: str) -> Optional[dict[str, Any]]:
        """Retained run history + stats for one job, or ``None`` if unknown.

        Behind ``GET /jobs/{name}/runs`` and MCP ``cron_list_runs``.
        """
        if name not in self.cron_jobs:
            return None
        runs = list(self.run_history.get(name) or [])
        return {
            "name": name,
            "runs": [r.to_dict() for r in runs],  # oldest first
            "stats": _run_stats(runs),
        }

    async def _web_job_runs(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        payload = self.job_runs_payload(name)
        if payload is None:
            raise _api_error(
                web.HTTPNotFound, "job {!r} not found".format(name)
            )
        # the same clamped `limit` the DAG runs route (and the MCP
        # cron_list_runs twin) applies; the retained history is bounded by
        # RUN_HISTORY_LIMIT, so the default serves it whole, exactly the
        # old unparameterised behavior.
        limit = self._web_int_query(
            request,
            "limit",
            default=RUN_HISTORY_LIMIT,
            lo=1,
            hi=RUN_HISTORY_LIMIT,
        )
        if len(payload["runs"]) > limit:
            payload["runs"] = payload["runs"][-limit:]  # newest retained
        return _json_response(payload, headers=self._web_headers())

    def activity_payload(self) -> dict[str, Any]:
        """Every job's retained runs, cut down to what the heatmap plots.

        Behind ``GET /activity``: ``jobs`` maps each job name to its
        retained runs oldest first, each row exactly ``{started_at,
        finished_at, outcome}``.  Every configured job is present (``[]``
        for one with no history) so a client can tell "no runs" from
        "unknown job".
        """
        jobs: dict[str, list[dict[str, Any]]] = {}
        for name in self.cron_jobs:
            # .get, never a subscript: run_history is a defaultdict and a
            # read must not grow it one empty deque per never-run job
            history = self.run_history.get(name)
            jobs[name] = [
                {
                    "started_at": (
                        r.started_at.isoformat()
                        if r.started_at is not None
                        else None
                    ),
                    "finished_at": r.finished_at.isoformat(),
                    "outcome": r.outcome,
                }
                for r in (history or ())
            ]
        return {"jobs": jobs}

    async def _web_get_activity(self, request: web.Request) -> web.Response:
        """Batched recent run outcomes for every job (the heatmap's feed).

        Serves in one response what would otherwise be a
        ``GET /jobs/{name}/runs`` per job per refresh, reduced to the three
        fields the overlay plots, on the same memo, ETag and gzip scaffold
        as the other poll legs, busted by the same local events.
        """
        return await self._memoized_conditional_response(
            request,
            self._activity_response_memo,
            _ACTIVITY_RESPONSE_TTL,
            self._build_activity_product,
        )

    async def _build_activity_product(
        self,
    ) -> tuple[str, bytes, Optional[bytes]]:
        # the projection reads the live run histories so it stays on the
        # loop; the serialize/hash/gzip over it is pure CPU and offloads
        # for a large fleet, at the same gate as /jobs.
        payload = self.activity_payload()
        if len(payload["jobs"]) >= _JOBS_SERIALIZE_OFFLOAD_MIN:
            return await asyncio.get_running_loop().run_in_executor(
                None, _cachable_json_product, payload
            )
        return _cachable_json_product(payload)

    async def _web_job_resources(self, request: web.Request) -> web.Response:
        """Chart-grade CPU/RSS series for one job (monitorResources jobs).

        Fetched lazily when the dashboard opens the chart, never on the
        poll loop. ``live`` is the run-so-far series per running instance;
        ``runs`` the recorded series of recent finished runs. Both empty,
        and ``monitored`` false, for a job that never opted in.
        """
        name = request.match_info["name"]
        max_runs = self._web_int_query(
            request,
            "limit",
            default=20,
            lo=0,
            hi=RUN_HISTORY_LIMIT,
            alias="runs",
        )
        payload = self.job_resources_payload(name, max_runs)
        if payload is None:
            raise _api_error(
                web.HTTPNotFound, "job {!r} not found".format(name)
            )
        return _json_response(payload, headers=self._web_headers())

    def job_resources_payload(
        self, name: str, max_runs: int
    ) -> Optional[dict[str, Any]]:
        """CPU/RSS series for a job's live + recent runs, or ``None``.

        Behind ``GET /jobs/{name}/resources`` and MCP
        ``cron_get_job_resources``.  ``max_runs`` caps the recorded-run tail.
        """
        job = self.cron_jobs.get(name)
        if job is None:
            return None
        live = []
        for runjob in self.running_jobs.get(name) or []:
            series = runjob.live_resource_series()
            snap = runjob.live_resources()
            if series is None and snap is None:
                continue  # unmonitored / not yet sampled
            live.append(
                {
                    "started_at": (
                        runjob.started_at.isoformat()
                        if runjob.started_at is not None
                        else None
                    ),
                    "pid": (
                        runjob.proc.pid if runjob.proc is not None else None
                    ),
                    "current": snap,
                    "series": series or [],
                }
            )
        history = list(self.run_history.get(name) or [])
        monitored = [r for r in history if r.resource_usage is not None]
        runs = [
            r.to_dict(include_series=True)
            for r in monitored[len(monitored) - max_runs :]
        ]
        return {
            "name": name,
            "monitored": bool(job.monitorResources),
            "interval": job.monitorResourcesInterval,
            "live": live,
            "runs": runs,
        }

    async def _web_job_trends(self, request: web.Request) -> web.Response:
        """SLA trend aggregates over the durable run ledger.

        The long-horizon sibling of ``/jobs/{name}/runs``: the same stats
        shape (:func:`_run_stats`), computed per :data:`TREND_WINDOWS`
        window (plus ``all``) over the ledger, which survives restarts and
        (on a shared mount) merges every node's runs.  Bounded by the
        store's ``maxRunsPerJob`` retention.  Degrades to the in-memory
        history (``source: memory``) without a healthy backend, so the
        endpoint always answers.
        """
        name = request.match_info["name"]
        payload = await self.job_trends_payload(name)
        if payload is None:
            raise web.HTTPNotFound()
        return _json_response(payload, headers=self._web_headers())

    async def job_trends_payload(self, name: str) -> Optional[dict[str, Any]]:
        """SLA trend aggregates over the durable run ledger, or ``None``.

        Behind ``GET /jobs/{name}/trends`` and MCP ``cron_get_job_trends``
        (both routes share this method, so the executor offload below lives
        in exactly one place).  Only the backend read is awaited on the
        loop; the record parse and window aggregation (up to
        :data:`TREND_SCAN_LIMIT` records, pure CPU) run on the default
        executor over immutable snapshots, like the schedule
        pressure/suggest/calendar builders, so a dashboard poll cannot
        stall job dispatch.
        """
        if name not in self.cron_jobs:
            return None
        loop = asyncio.get_running_loop()
        cached = self._trends_cache.get(name)
        if cached is not None and loop.time() < cached[0]:
            # a recent poll already read and aggregated this job's ledger;
            # serve that within the TTL instead of re-scanning up to
            # TREND_SCAN_LIMIT records again (see JOB_TRENDS_CACHE_TTL).
            return cached[1]
        recs: Optional[list[dict[str, Any]]] = None
        backend = self.state_backend
        if backend is not None:
            try:
                # newest-first with a cap: an unbounded-retention stream
                # must not hold a backend worker slot for a whole scan on
                # every dashboard poll.
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=TREND_SCAN_LIMIT,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - degrade, never 500
                logger.warning(
                    "state: cannot read the run ledger for trends on %s "
                    "(%s); serving the in-memory history",
                    name,
                    ex,
                )
        # The in-memory fallback reads live state, so its snapshot is taken
        # on the loop; the ledger snapshot above is already ours alone.  The
        # builder then touches nothing but its arguments.
        fallback = (
            list(self.run_history.get(name) or []) if recs is None else None
        )
        payload = await loop.run_in_executor(
            None, partial(self._job_trends_build, name, recs, fallback)
        )
        self._trends_cache[name] = (
            loop.time() + JOB_TRENDS_CACHE_TTL,
            payload,
        )
        return payload

    def _job_trends_build(
        self,
        name: str,
        recs: Optional[list[dict[str, Any]]],
        fallback: Optional[list[JobRunInfo]],
    ) -> dict[str, Any]:
        """Parse and aggregate for :meth:`job_trends_payload` (executor side).

        Pure CPU over snapshots captured on the loop: ``recs`` is the
        newest-first ledger listing when the backend read succeeded,
        otherwise ``fallback`` is the in-memory history copy.
        """
        if recs is not None:
            source = "durable"
            infos: list[JobRunInfo] = []
            recs.reverse()  # oldest first, matching _run_stats
            # one shared, already-closed output stream for every rehydrated
            # record: the trends aggregation never reads output, so the
            # per-record buffer allocation would be pure waste at
            # TREND_SCAN_LIMIT scale.
            empty = JobOutputStream()
            empty.closed = True
            for rec in recs:
                restored = _job_run_info_from_dict(rec, output=empty)
                if restored is not None:
                    infos.append(restored)
        else:
            source = "memory"
            infos = fallback or []
        now = get_now(datetime.timezone.utc)
        # One pass instead of one filter pass per window: each info's age is
        # computed once and the info appended to every window it fits, which
        # reproduces the per-window filters exactly (same members, same
        # relative order).  Bucketing rather than a bisect slice because the
        # listing's append order is not provably finished_at order: run
        # records persist fire-and-forget, a shared mount interleaves other
        # nodes' appends, and a crash-reconciled record carries a past
        # interruption instant but lands in the stream at boot.
        window_runs: dict[str, list[JobRunInfo]] = {
            label: [] for label, _ in TREND_WINDOWS
        }
        for info in infos:
            age = (now - info.finished_at).total_seconds()
            for label, seconds in TREND_WINDOWS:
                if age <= seconds:
                    window_runs[label].append(info)
        windows = {
            label: _run_stats(runs) for label, runs in window_runs.items()
        }
        windows["all"] = _run_stats(infos)
        return {
            "name": name,
            "source": source,
            "generated_at": now.isoformat(),
            "windows": windows,
        }

    def _job_output(self, name: str) -> Optional[JobOutputStream]:
        # the live output of the most recent running instance, else the last
        # finished run's retained output, else nothing captured yet.
        running = self.running_jobs.get(name) or []
        if running:
            return running[-1].output
        last = self.last_run.get(name)
        return last.output if last is not None else None

    def _dag_task_output(
        self, dag_name: str, run_key: str, taskkey: str
    ) -> Optional[JobOutputStream]:
        """The live output stream of a DAG task instance, or ``None``.

        A DAG task runs as a :class:`RunningJob` under the template name
        ``<dag>.<task_id>`` (its instances share that key), so locate the one
        whose ``dag_ref`` matches this run + instance key.  Only a *currently
        running* instance has a reachable buffer: a finished DAG task's
        output is not retained under the template name (its completion routes
        to the DAG driver, not the per-job last_run), so this returns ``None``
        once the task is done.
        """
        # the base task id: a mapped instance key is ``id#<index>``.
        task_id = taskkey.split("#", 1)[0]
        template_name = "{}.{}".format(dag_name, task_id)
        for running in self.running_jobs.get(template_name, []) or []:
            dref = getattr(running, "dag_ref", None)
            if (
                dref is not None
                and dref.run_key == run_key
                and dref.taskkey == taskkey
            ):
                return running.output
        return None

    async def _web_on_shutdown(self, app: web.Application) -> None:
        """End every live SSE tail so the web app can tear down promptly.

        Runs inside ``web_runner.cleanup()`` BEFORE aiohttp waits its 60s
        shutdown timeout: a tail handler never returns on its own, so an
        open tail would stall scheduling for the full timeout. The queue
        sentinel reuses the end-of-output path.
        """
        self._web_draining = True
        for queue in list(self._web_sse_queues):
            queue.put_nowait(None)

    def _sse_headers(self) -> dict[str, str]:
        # Like /metrics, the stream framing is this endpoint's contract: an
        # operator-configured Content-Type, cache policy, or proxy buffering
        # override in web.headers would silently break every live tail, so
        # the protocol headers win, in any spelling: see _strip_headers.
        headers = _strip_headers(
            self._web_headers(),
            "content-type",
            "cache-control",
            "x-accel-buffering",
        )
        headers["Content-Type"] = "text/event-stream"
        headers["Cache-Control"] = "no-cache"
        # tell reverse proxies (nginx) not to buffer the event stream
        headers["X-Accel-Buffering"] = "no"
        return headers

    async def _pump_output(
        self, resp: web.StreamResponse, output: JobOutputStream
    ) -> None:
        """Replay the retained buffer then live-tail an output stream over SSE.

        Shared by the job- and DAG-task log endpoints.  The response must
        already be prepared.
        """
        # Subscribe first, then snapshot the buffer: there is no await between
        # the two, so no line can slip through the gap. The snapshot holds
        # everything captured before now; the queue receives only lines
        # published after: together, no duplicates and no gaps.
        queue = output.subscribe()
        # Registered (no await since the drain check) so a teardown can end
        # this tail; a tail arriving after the broadcast must not enter a
        # loop nothing would ever wake again (see _web_on_shutdown).
        if self._web_draining:
            output.unsubscribe(queue)
            await resp.write(b"event: end\ndata: {}\n\n")
            return
        self._web_sse_queues.add(queue)
        try:
            # One write for the whole retained buffer, not one per line: a tab
            # opening on a chatty job replays up to LIVE_LOG_LIMIT lines, and
            # each awaited write is a coroutine step plus a transport write
            # (and potentially its own small TCP segment).
            replay = b"".join(
                _sse_frame(stream_name, line)
                for stream_name, line in list(output.lines)
            )
            if replay:
                await resp.write(replay)
            ended = False
            while not ended:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    # SSE comment as keep-alive (also detects disconnects)
                    await resp.write(b": ping\n\n")
                    continue
                if item is None:  # end-of-output sentinel
                    break
                # Drain the burst behind the first line before writing: a
                # chatty job publishes lines faster than one wait_for +
                # write round trip each, and per-line delivery costs a
                # timer task, frame build and transport write PER LINE PER
                # SUBSCRIBER on the scheduler's loop. One joined write per
                # drained burst mirrors the replay path above.
                frames = [_sse_frame(item[0], item[1])]
                while True:
                    try:
                        nxt = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if nxt is None:  # sentinel inside the burst
                        ended = True
                        break
                    frames.append(_sse_frame(nxt[0], nxt[1]))
                await resp.write(b"".join(frames))
            await resp.write(b"event: end\ndata: {}\n\n")
        except (ConnectionResetError, asyncio.CancelledError):
            # client navigated away / closed the tab: nothing to do
            pass
        finally:
            self._web_sse_queues.discard(queue)
            output.unsubscribe(queue)

    @staticmethod
    def _tail_payload(
        output: Optional[JobOutputStream],
        tail: int,
        cursor: Optional[int],
    ) -> dict[str, Any]:
        """Poll-friendly snapshot of a retained output buffer.

        The non-streaming counterpart of :meth:`_pump_output`, backing the MCP
        log-tail tools (a client polls with the returned ``cursor`` to fetch
        only newly appended lines).  Without a ``cursor`` returns the last
        ``tail`` lines; with one, the lines after that offset (capped at
        ``tail``).  ``cursor`` is a line offset into the bounded in-memory
        buffer, so after the buffer rotates it can only ever skip forward,
        never resurrect dropped lines.
        """
        lines = list(output.lines) if output is not None else []
        total = len(lines)
        if cursor is None:
            start = max(0, total - tail)
        else:
            start = min(max(cursor, 0), total)
        selected = lines[start : start + tail]
        return {
            "lines": [
                {"stream": stream, "line": line} for stream, line in selected
            ],
            "cursor": start + len(selected),
            # older retained lines exist above what we returned (only
            # meaningful on a cursor-less "give me the tail" call).
            "truncated": cursor is None and start > 0,
        }

    def job_logs_tail_payload(
        self, name: str, tail: int = 100, cursor: Optional[int] = None
    ) -> Optional[dict[str, Any]]:
        """Last retained log lines of a job, or ``None`` if unknown.

        Behind MCP ``cron_tail_job_logs``, the poll/cursor projection of the
        SSE stream at ``GET /jobs/{name}/logs``.
        """
        if name not in self.cron_jobs:
            return None
        payload = self._tail_payload(self._job_output(name), tail, cursor)
        payload["name"] = name
        payload["running"] = bool(self.running_jobs.get(name))
        return payload

    def dag_task_logs_tail_payload(
        self,
        dag: str,
        run_key: str,
        taskkey: str,
        tail: int = 100,
        cursor: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Last retained log lines of a running DAG task instance, or ``None``.

        Behind MCP ``cron_tail_dag_task_logs``.  Only a currently-running
        instance has a reachable buffer (a finished task's output is not
        retained under the template name), so ``lines`` is empty once it ends.
        """
        if dag not in self.cron_dags:
            return None
        output = self._dag_task_output(dag, run_key, taskkey)
        payload = self._tail_payload(output, tail, cursor)
        payload["dag"] = dag
        payload["run_key"] = run_key
        payload["taskkey"] = taskkey
        return payload

    async def _web_job_logs(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        if name not in self.cron_jobs:
            raise web.HTTPNotFound()

        resp = web.StreamResponse(headers=self._sse_headers())
        await resp.prepare(request)

        output = self._job_output(name)
        if output is None:
            await resp.write(b'event: end\ndata: {"reason": "no-output"}\n\n')
            return resp
        await self._pump_output(resp, output)
        return resp

    async def _web_dag_task_logs(
        self, request: web.Request
    ) -> web.StreamResponse:
        """Live-tail a DAG task instance's stdout/stderr over SSE.

        Serves the LIVE output of a currently-running task instance (a
        finished instance's buffer is not retained); the dashboard shows
        "no live output" otherwise.
        """
        name = request.match_info["name"]
        run_key = request.match_info["run_key"]
        taskkey = request.match_info["taskkey"]
        if name not in self.cron_dags:
            raise web.HTTPNotFound()

        resp = web.StreamResponse(headers=self._sse_headers())
        await resp.prepare(request)

        output = self._dag_task_output(name, run_key, taskkey)
        if output is None:
            await resp.write(b'event: end\ndata: {"reason": "no-output"}\n\n')
            return resp
        await self._pump_output(resp, output)
        return resp

    def _web_restart_reason(
        self,
        web_config: Optional[WebConfig],
        mcp_config: Optional[MCPConfig],
    ) -> Optional[str]:
        """Why the running web app must be torn down, or None to keep it.

        The same reason-string triage :meth:`start_stop_cluster` uses, for the
        same reasons: three distinguishable causes, one of which (a
        certificate rotation) is gated on the new material actually loading.
        """
        if web_config is None or web_config != self.web_config:
            reason = "configuration changed"
        # an mcp-only change (e.g. readOnly flipped, a toolset added) must
        # also restart the app: the /mcp route set is fixed at build time,
        # so it only picks up config through a fresh routes list.
        elif mcp_config != self.mcp_config:
            reason = "mcp configuration changed"
        # an in-place certificate rotation leaves the config bytes identical
        # but the on-disk material new; without this the listener keeps
        # serving the old certificate until it expires.
        elif self._web_tls_files_changed():
            reason = "TLS certificate files changed"
        else:
            return None
        if web_config is not None and not tlsutil.listener_tls_loadable(
            web_config.get("tls")
        ):
            # Make-before-break is infeasible (the new runner binds the
            # same port the old one holds), so only proceed once the NEW
            # material loads: a half-written rotation would otherwise tear
            # down a working listener and fail to rebuild.
            logger.warning(
                "web: new TLS material is not yet loadable (a "
                "partial/half-written rotation, or a config edit racing "
                "one?); keeping the running listener and retrying next reload"
            )
            return None
        return reason

    def _build_web_tls(
        self, web_tls: Optional[dict[str, Any]]
    ) -> "tuple[Optional[ssl.SSLContext], Optional[dict[str, Any]], bool]":
        """``(context, file signature, failed)`` for a listener about to start.

        On failure the caller must NOT fall back to a plaintext listener and
        must NOT latch ``web_config``: leaving it unchanged is exactly what
        makes the next reload retry, instead of concluding "nothing changed"
        and never trying again.
        """
        # Callers gate on tlsutil.listener_tls_configured, so cert and key
        # are present here; the Optional is only so the guarded call site
        # type-checks.
        assert web_tls is not None
        # Snapshot the files BEFORE loading them: a rotation landing in the
        # gap then compares unequal on the next reload, which is a spurious
        # restart rather than a missed one. (cluster.py latches AFTER its
        # load; this order is the safer one.)
        signature = tlsutil.tls_file_signature(
            web_tls, tlsutil.LISTENER_TLS_KEYS
        )
        try:
            context = tlsutil.build_listener_ssl_context(
                web_tls["cert"],
                web_tls["key"],
                client_ca=web_tls.get("clientCa"),
            )
        except (OSError, ssl.SSLError) as ex:
            logger.error(
                "web: TLS material is not loadable, so the web API is not "
                "starting (retrying on the next config reload): %s",
                ex,
            )
            return None, None, True
        return context, signature, False

    async def start_stop_web_app(
        self,
        web_config: Optional[WebConfig],
        mcp_config: Optional[MCPConfig] = None,
    ):
        if self.web_runner is not None:
            reason = self._web_restart_reason(web_config, mcp_config)
            if reason is not None:
                logger.info("web: %s, stopping http server", reason)
                await self.web_runner.cleanup()
                self.web_runner = None
                self._web_tcp_bound = []
                self._web_tls_signature = None

        # Build the listener's TLS context ONCE per (re)start, before anything
        # is bound, so a context failure never leaves a half-built runner.
        # Guarded on a start actually being about to happen: a healthy running
        # listener must not re-read and re-parse the PEMs every housekeeping
        # tick.
        start_wanted = bool(
            web_config is not None
            and web_config["listen"]
            and self.web_runner is None
        )
        web_tls = web_config.get("tls") if web_config is not None else None
        # listener_tls_configured, not a bare truthiness test on the dict: a
        # `tls:` block whose values are blank parses to a truthy dict of
        # Nones, and building a context from that would raise a TypeError
        # rather than the OSError the failure path expects. Such a block also
        # cannot coexist with an https:// listener (config validation refuses
        # that pair), so skipping it here serves the plaintext listeners the
        # config actually asks for.
        tls_context, tls_signature, tls_failed = (
            self._build_web_tls(web_tls)
            if start_wanted and tlsutil.listener_tls_configured(web_tls)
            else (None, None, False)
        )

        # `web_config is not None` is implied by start_wanted; it is repeated
        # so the narrowing survives for the whole start branch below.
        if start_wanted and not tls_failed and web_config is not None:
            ui_enabled = web_config.get("ui", True)
            metrics_config = resolve_metrics_config(web_config)
            # Envelope first, so it is outermost and wraps the errors the
            # origin/auth middlewares below raise as well as the router's
            # own 404/405 (see _error_envelope_middleware).
            middlewares = [_error_envelope_middleware]
            # Cross-site request defense for the mutating endpoints, ALWAYS
            # installed: with authToken unset this is the only thing between
            # a localhost-bound daemon and any web page the operator visits
            # (see _make_origin_middleware). Outermost, so a foreign page is
            # refused before auth or handlers run. An operator who explicitly
            # published the API cross-origin via a wildcard
            # Access-Control-Allow-Origin response header keeps that (loudly);
            # a specific ACAO origin is folded into the allow-list so an
            # existing deliberate cross-origin dashboard survives the upgrade.
            allowed_origins = set(web_config.get("allowedOrigins") or [])
            custom_headers = web_config.get("headers") or {}
            acao = next(
                (
                    value
                    for key, value in custom_headers.items()
                    if key.lower() == "access-control-allow-origin"
                ),
                None,
            )
            if acao == "*":
                logger.warning(
                    "web: headers set Access-Control-Allow-Origin: '*'; "
                    "honouring it and NOT enforcing the cross-site Origin "
                    "check on mutating endpoints"
                )
            else:
                if acao:
                    allowed_origins.add(acao)
                middlewares.append(
                    self._make_origin_middleware(frozenset(allowed_origins))
                )
            token_table = self._resolve_web_tokens(web_config)
            if token_table is not None:
                logger.info(
                    "web: requiring bearer-token authentication (%d token%s)",
                    len(token_table),
                    "" if len(token_table) == 1 else "s",
                )
                # the UI page is served unauthenticated (it holds no data); the
                # browser then sends the token on every data request.
                public = set(WEB_PUBLIC_PATHS) if ui_enabled else set()
                if metrics_config is not None and metrics_config["public"]:
                    # deliberate operator opt-out for scrapers that cannot
                    # send a bearer token; everything else stays gated.
                    public.add("/metrics")
                middlewares.append(
                    self._make_auth_middleware(token_table, frozenset(public))
                )
            app = web.Application(middlewares=middlewares)
            # New app generation: tails may subscribe again, and the
            # on_shutdown hook is what ends them at the NEXT teardown (see
            # _web_on_shutdown for why cleanup would otherwise stall).
            self._web_draining = False
            app.on_shutdown.append(self._web_on_shutdown)
            # The MCP server (POST /mcp) rides these same listeners and the
            # auth middleware above: /mcp is NEVER added to `public`, so it
            # inherits the bearer-token gate. Built here (not in __init__) so a
            # reload rebuilds it against the current config. The fail-closed
            # no-token-on-a-routable-listener check ran at parse time
            # (config._validate_mcp_config); this only wires it up.
            self._mcp = None
            if mcp_config is not None and mcp_config.get("enabled"):
                from cronstable.mcp import MCPHandler

                self._mcp = MCPHandler(self, mcp_config)
                logger.info("mcp: serving the MCP endpoint at POST /mcp")
            if metrics_config is not None:
                # buckets apply from here on; a changed bucket set restarts
                # the histograms (an ordinary counter reset to Prometheus).
                self.metrics.set_duration_buckets(
                    metrics_config["durationBuckets"]
                )
            # Routes come from the declarative WEB_ROUTES table (the spec's
            # single source of truth; see its comment), in table order, with
            # the conditional groups skipped when their feature is off.
            routes = []
            for method, path, handler_name, gate in WEB_ROUTES:
                if gate == "mcp":
                    if self._mcp is None:
                        continue
                    handler = getattr(self._mcp, handler_name)
                else:
                    if gate == "metrics" and metrics_config is None:
                        continue
                    if gate == "ui" and not ui_enabled:
                        continue
                    handler = getattr(self, handler_name)
                routes.append(web.route(method, path, handler))
            app.add_routes(routes)
            self.web_runner = web.AppRunner(
                app, access_log_class=_access_log_class()
            )
            await self.web_runner.setup()
            socket_mode = web_config.get("socketMode")
            self._web_tcp_bound = []
            bound_any = False
            for addr in web_config["listen"]:
                # everything `addresses` gains from start() below belongs
                # to this entry (see _record_bound_listeners)
                bound_before = len(self.web_runner.addresses)
                try:
                    site = web_site_from_url(
                        self.web_runner, addr, tls_context
                    )
                    await site.start()
                except (ValueError, OSError) as ex:
                    # bad scheme/url (ValueError) or bind failure (OSError):
                    # skip this address rather than aborting the whole config
                    # update or reporting it as an internal bug.
                    logger.warning("web: could not listen on %s: %s", addr, ex)
                    continue
                bound_any = True
                self._record_bound_listeners(
                    urlparse(addr).scheme, bound_before
                )
                logger.info("web: started listening on %s", addr)
                if socket_mode:
                    self._apply_socket_mode(addr, socket_mode)
            if not bound_any:
                # Every listen entry failed to bind (a predecessor still
                # draining its jobs and holding the port is the classic).
                # Tear the runner back down and do NOT latch web_config: the
                # same rule _build_web_tls documents, an unchanged latch is
                # what makes the next housekeeping pass retry the bind
                # instead of concluding "nothing changed" and leaving the
                # dashboard, /metrics and /mcp down for the daemon's life.
                logger.error(
                    "web: no listen address could be bound, so the web API "
                    "is not up; retrying on the next housekeeping pass"
                )
                await self.web_runner.cleanup()
                self.web_runner = None
                self._mcp = None
                self._web_tcp_bound = []
            else:
                self.web_config = web_config
                self.mcp_config = mcp_config
                self._web_tls_signature = tls_signature

        # Node history sampling follows the web API's lifecycle: the ring
        # only feeds the dashboard's node chart, so it runs whenever the web
        # app does (subject to web.nodeHistory) and stops with it. Both
        # branches are idempotent: start_history no-ops when the running
        # task already matches the config, stop_history when there is none.
        history_config = (
            resolve_node_history_config(web_config)
            if web_config is not None and self.web_runner is not None
            else None
        )
        if history_config is not None:
            self._node_sampler.start_history(
                interval=history_config["interval"],
                points=history_config["points"],
            )
        else:
            await self._node_sampler.stop_history()

        # The Bonjour advert follows the web app's lifecycle exactly like
        # the node-history sampler: advertise while (and only while) a TCP
        # listener is actually bound, so the advertised port is the real
        # one even for an ephemeral `:0` listen. Converge is cheap (a
        # signature compare) and never raises.
        await self._bonjour.start_stop(self._bonjour_advert(web_config))

    def _record_bound_listeners(self, scheme: str, bound_before: int) -> None:
        """File the sockets one just-started site added to the runner.

        Everything ``runner.addresses`` gained past ``bound_before``
        belongs to that site, and its start is the only moment its
        scheme and its bound sockets are both in hand (a hostname bind
        can add several).
        """
        assert self.web_runner is not None
        for sockname in self.web_runner.addresses[bound_before:]:
            # TCP sites report (host, port[, flowinfo, scope]); unix
            # sockets report their path string.
            if isinstance(sockname, (tuple, list)):
                self._web_tcp_bound.append((scheme, sockname))

    def _bonjour_advert(
        self, web_config: Optional[WebConfig]
    ) -> Optional[dict[str, Any]]:
        """The `_cronstable._tcp` advert the current web state calls for.

        None whenever there is nothing (or no wish) to advertise: the
        advert is off, the web app is not running, or no bound listener
        is dialable from another machine (see
        :meth:`_advertisable_listener`, which logs why).
        """
        if web_config is None or self.web_runner is None:
            return None
        bonjour = resolve_bonjour_config(web_config)
        if bonjour is None:
            return None
        chosen = self._advertisable_listener()
        if chosen is None:
            return None
        scheme, port, address = chosen
        advert: dict[str, Any] = {
            "name": bonjour.get("name") or report_hostname(),
            "port": port,
            "properties": {
                "v": cronstable.version.version,
                "scheme": scheme,
            },
        }
        if address is not None:
            advert["address"] = address
        return advert

    def _advertisable_listener(
        self,
    ) -> Optional[tuple[str, int, Optional[str]]]:
        """The one (scheme, port, address) worth advertising, or None.

        All three must describe the SAME listener: the advert exists to
        be dialed by another machine, and mixing one listener's port with
        another's scheme or address names an endpoint nothing serves.
        Selection: the first bound https
        listener a LAN peer can reach, else the first such http one.
        A loopback-bound listener is never advertised (its port is
        unreachable from any other machine), and a listener bound to
        one specific IPv6 address is skipped because the advert's A
        record is IPv4-only.

        The address element is the listener's own IP for a specific
        IPv4 bind (the outbound-route probe could name a different
        interface than the one the socket lives on) and None for a
        wildcard bind, where the advertiser probes the primary address.
        """
        candidates: list[tuple[str, int, Optional[str]]] = []
        skipped_v6 = False
        for scheme, sockname in self._web_tcp_bound:
            host, port = str(sockname[0]), int(sockname[1])
            if host.startswith("127.") or host == "::1":
                continue
            if host in ("0.0.0.0", "::"):
                candidates.append((scheme, port, None))
            elif ":" in host:
                skipped_v6 = True
            else:
                candidates.append((scheme, port, host))
        if not candidates:
            if skipped_v6:
                logger.warning(
                    "bonjour: the only LAN-reachable web listeners are "
                    "bound to specific IPv6 addresses and the advert's "
                    "address record is IPv4-only; skipping the advert"
                )
            else:
                logger.warning(
                    "bonjour: no LAN-reachable TCP web listener to "
                    "advertise (loopback and unix listeners cannot be "
                    "dialed from another machine); skipping the advert"
                )
            return None
        for candidate in candidates:
            if candidate[0] == "https":
                return candidate
        return candidates[0]

    @staticmethod
    def _election_relevant(cluster_config: ClusterConfig) -> dict[str, Any]:
        """The cluster config minus its observability-only keys.

        ``shareNodeStats`` and ``observabilityMesh`` are election-inert;
        restarting the manager on a difference in them would drop the
        leadership lease and pause Leader jobs fleet-wide for an edit that
        changes nothing about election, so the restart comparison strips
        them from both sides.
        """
        return {
            key: value
            for key, value in cluster_config.items()
            if key not in ("shareNodeStats", "observabilityMesh")
        }

    async def start_stop_cluster(
        self, cluster_config: Optional[ClusterConfig]
    ) -> None:
        # Track the election intent up front so the leader gate can fail closed
        # even if the manager (below) is absent or fails to start.
        self._elect_leader_configured = bool(
            cluster_config and cluster_config.get("electLeader")
        )
        # Restart the manager only on a cluster-section change or an
        # in-place TLS cert rotation (config bytes identical, on-disk
        # material new); without the latter the cluster keeps serving the
        # old cert until it expires and loses quorum fleet-wide.
        mgr = self.cluster_manager
        if mgr is not None:
            # observability-only edits never require an election restart
            # (see _election_relevant); the overlay lifecycle picks them up.
            if cluster_config is None or self._election_relevant(
                cluster_config
            ) != self._election_relevant(mgr.config):
                reason = "configuration changed"
            elif mgr.tls_files_changed():
                reason = "TLS certificate files changed"
            else:
                reason = None
            if (
                reason == "TLS certificate files changed"
                and not mgr.tls_files_loadable()
            ):
                # Validate the NEW material BEFORE tearing the old manager
                # down: rotations are not atomic across the files, and
                # stop-then-fail-to-rebuild would wedge Leader/PreferLeader
                # closed for up to a reload. Only this reason is gated; a
                # genuine configuration change tears down regardless.
                logger.warning(
                    "cluster: TLS certificate files changed but the new "
                    "material is not yet loadable (a partial/half-written "
                    "rotation?); keeping the running manager and retrying "
                    "next reload"
                )
                reason = None
            # local import so cluster.py stays out of the import graph
            # until a running manager is actually being reconfigured.
            from cronstable.cluster import gossip_tls_loadable

            if (
                reason == "configuration changed"
                and cluster_config is not None
                and not gossip_tls_loadable(cluster_config)
            ):
                # a config change racing a cert rotation: dry-run the NEW
                # config's gossip TLS first; a stale-but-functional cluster
                # beats no manager. Non-gossip/tls-less configs always pass.
                logger.warning(
                    "cluster: configuration changed but the new TLS material "
                    "is not yet loadable (a config edit racing a cert "
                    "rotation?); keeping the running manager and retrying "
                    "next reload"
                )
                reason = None
            if reason is not None:
                logger.info("cluster: %s, stopping", reason)
                # Record losing leadership/quorum HERE if we held it: the
                # flag resets below would otherwise suppress the transition
                # log, leaving the ex-leader silent about why it stopped
                # Leader jobs.
                node = getattr(mgr, "node_name", None) or self._state_host
                if self._was_leader:
                    # a real leadership loss (the rebuilt manager re-elects
                    # from scratch), so it counts as a transition too
                    self.metrics.cluster_leader_transition()
                    logger.info(
                        "cluster: this node lost scheduled-job leadership "
                        "(leadership manager stopped for reload)"
                    )
                    self._dispatch_notify(
                        "leader_change",
                        success=False,
                        name=node,
                        subject="node {} lost scheduled-job leadership".format(
                            node
                        ),
                        message=(
                            "This node is no longer the scheduled-job leader "
                            "(leadership manager stopped for a config reload)."
                        ),
                        role="follower",
                        is_leader=False,
                        leader=None,
                    )
                if self._was_quorate:
                    self.metrics.cluster_quorum_transition()
                    logger.info(
                        "cluster: this node left quorum (leadership manager "
                        "stopped for reload); Leader jobs cannot run until it "
                        "is rebuilt"
                    )
                    self._dispatch_notify(
                        "quorum_loss",
                        success=False,
                        name=node,
                        subject="node {} left quorum".format(node),
                        message=(
                            "This node left quorum (leadership manager "
                            "stopped for a config reload); Leader jobs "
                            "cannot run until it is rebuilt."
                        ),
                        quorate=False,
                    )
                await mgr.stop()
                self.cluster_manager = None
                # the transition flags track the OLD manager's last-logged
                # state; reset so the replacement logs a clean transition.
                self._was_leader = False
                self._was_quorate = False
                self._was_conflict = False
                self._was_size_conflict = False
                self._was_policy_conflict = False
        if cluster_config is not None and self.cluster_manager is not None:
            # a KEPT manager latched the share flag at its last call;
            # re-reconcile unconditionally or a shareNodeStats toggle would
            # never reach a running gossip mesh. No-op on lease backends.
            self.cluster_manager.set_node_stats_provider(
                self.node_resource_snapshot,
                share=bool(cluster_config.get("shareNodeStats"))
                and cluster_config.get("observabilityMesh") is None,
            )
        if cluster_config is not None and self.cluster_manager is None:
            # Emit non-fatal advisories here (only when a manager is actually
            # (re)started) rather than at parse time, which runs every reload
            # and would repeat the same warning every minute.
            for warning in cluster_config_warnings(cluster_config):
                logger.warning("%s", warning)
            try:
                # Construct INSIDE the try: __init__/start can raise on
                # operational misconfiguration (TLS files, listen parse,
                # bind, lease-store credentials). All are logged and run
                # through, not bugs, so they must not escape to the run
                # loop's "please report this as a bug" handler.
                manager = make_backend(cluster_config, self.job_set_id)
                # Install the summaries provider BEFORE start(): peers may
                # poll us during start()'s first round and their first
                # absorbed snapshot should carry our jobs.
                manager.set_job_summaries_provider(self.fleet_job_summaries)
                # Always install the node-stats provider (local readouts
                # are free); `share` gates gossiping it to peers, on only
                # for observability with backend: gossip (the lease+overlay
                # case installs on the overlay instead). No-op on lease
                # backends.
                manager.set_node_stats_provider(
                    self.node_resource_snapshot,
                    share=bool(cluster_config.get("shareNodeStats"))
                    and cluster_config.get("observabilityMesh") is None,
                )
                await manager.start()
            except (
                OSError,
                ssl.SSLError,
                ValueError,
                ConfigError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ) as ex:
                # bad cert/credential files / bad listen address / port already
                # in use / unreachable setup: log and keep running jobs rather
                # than aborting the reload. (A backend cleans up its own
                # half-started state on failure.) aiohttp.ClientError /
                # asyncio.TimeoutError cover a lease backend that cannot reach
                # or authenticate to its store at start(), an operational
                # misconfiguration to log, not the generic "report a bug" path
                # (a ClientResponseError on a rejected token is not OSError).
                logger.error("cluster: failed to start: %s", ex)
                return
            self.cluster_manager = manager

    def node_resource_snapshot(self) -> Optional[dict[str, Any]]:
        """This node's live CPU/memory for gossip and GET /node.

        The callable installed as the gossip node-stats provider; also used by
        the /node endpoint. Best-effort: returns None when psutil is
        unavailable.
        """
        return self._node_sampler.snapshot()

    def _fleet_backend(self) -> Optional[LeadershipBackend]:
        """The backend that answers the fleet view / carries fleet gossip.

        The observability overlay mesh when one is running (a lease cluster
        that opted into ``cluster.observability``), else the leadership backend
        itself, which provides the fleet view directly under ``backend:
        gossip`` and returns ``None`` for the lease backends (no fleet).
        """
        return (
            self.observability_mesh
            if self.observability_mesh is not None
            else self.cluster_manager
        )

    async def start_stop_observability(
        self, cluster_config: Optional[ClusterConfig]
    ) -> None:
        """(Re)build the gossip observability overlay to match the config.

        A SECOND, election-inert gossip manager a lease cluster stands up
        purely to exchange fleet data; None in the resolved
        ``observabilityMesh`` config means no overlay is wanted. Mirrors
        start_stop_cluster's rebuild logic but simpler (no leadership to
        log); a start failure is logged and swallowed so a misconfigured
        overlay never stops jobs.
        """
        mesh_config = (
            cluster_config.get("observabilityMesh")
            if cluster_config is not None
            else None
        )
        mesh = self.observability_mesh
        if mesh is not None:
            if mesh_config is None or mesh_config != mesh.config:
                reason = "configuration changed"
            elif mesh.tls_files_changed():
                reason = "TLS certificate files changed"
            else:
                reason = None
            # make-before-break is infeasible for gossip (same listen port), so
            # a cert rotation only tears down once the NEW material loads;
            # otherwise keep the running overlay serving the valid old cert.
            if reason is not None and mesh_config is not None:
                from cronstable.cluster import gossip_tls_loadable

                if not gossip_tls_loadable(mesh_config):
                    logger.warning(
                        "cluster.observability: new TLS material is not yet "
                        "loadable (a partial rotation?); keeping the running "
                        "overlay and retrying next reload"
                    )
                    reason = None
            if reason is not None:
                logger.info("cluster.observability: %s, stopping", reason)
                await mesh.stop()
                self.observability_mesh = None
        if mesh_config is not None and self.observability_mesh is not None:
            # The overlay was KEPT across this reload, and shareNodeStats
            # lives on the CLUSTER config, not the resolved mesh config the
            # keep/rebuild comparison sees, so a toggle always lands here:
            # re-reconcile unconditionally (the same rationale as the
            # election-mesh case above). Same share expression as the build
            # path below.
            self.observability_mesh.set_node_stats_provider(
                self.node_resource_snapshot,
                share=bool(
                    cluster_config is not None
                    and cluster_config.get("shareNodeStats")
                ),
            )
        if mesh_config is not None and self.observability_mesh is None:
            try:
                mgr = make_backend(mesh_config, self.job_set_id)
                # fleet providers BEFORE start(): its first poll round may race
                # peers polling us back, and their first absorbed snapshot
                # should already carry our jobs + load, not an empty block.
                mgr.set_job_summaries_provider(self.fleet_job_summaries)
                # always install (so the overlay's own /fleet self readout
                # shows this node's load); share gates gossiping it to peers.
                mgr.set_node_stats_provider(
                    self.node_resource_snapshot,
                    share=bool(
                        cluster_config is not None
                        and cluster_config.get("shareNodeStats")
                    ),
                )
                await mgr.start()
            except (
                OSError,
                ssl.SSLError,
                ValueError,
                ConfigError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ) as ex:
                # same swallow-and-keep-running contract as the election
                # backend: a bad overlay cert/listen/peer must not stop jobs.
                logger.error("cluster.observability: failed to start: %s", ex)
                return
            self.observability_mesh = mgr

    async def start_stop_state(
        self, state_config: Optional[StateConfig]
    ) -> None:
        """(Re)build the durable state backend to match the config.

        Rebuilt only when the ``state`` section is added, removed or
        changed (an ordinary job edit does not disturb it). A start
        failure is logged and swallowed, so misconfigured durability never
        stops in-memory job running. An in-place rotation of the job API's
        TLS cert restarts just that listener (see
        _maybe_restart_job_api_for_tls).
        """
        self._state_configured = state_config is not None
        if state_config is not None:
            self._state_on_unavailable = str(
                state_config.get("onStoreUnavailable") or "degrade"
            )
            self._state_gc_grace = float(
                state_config.get("gcGraceSeconds") or 0
            )
            self._slot_ttl = float(state_config.get("slotTtlSeconds") or 30)
        else:
            self._state_on_unavailable = "degrade"
            self._state_gc_grace = 0.0
            self._slot_ttl = 30.0
        backend = self.state_backend
        if backend is not None and (
            state_config is None or state_config != backend.config
        ):
            logger.info("state: configuration changed, stopping")
            # The pause refresh reads the OLD store through a backend
            # binding it captured before the swap: left to finish, it
            # re-installs that store's pauses on top of whatever the new
            # store's rehydrate resolves, and the new rehydrate cannot undo
            # that (it walks only the NEW store's streams). Cancel it
            # BEFORE the first await of this teardown, which is the only
            # point the race is closed.
            if self._pause_refresh_task is not None:
                self._pause_refresh_task.cancel()
                self._pause_refresh_task = None
            await backend.stop()
            self.state_backend = None
            # the loopback job-state API belongs to this backend generation
            # (its per-run tokens and staged secrets are meaningless against a
            # different store): stop it here, and a replacement is started
            # below if the new config still wants one.
            await self._stop_job_api()
            # a replacement backend (different path/namespace) serves a
            # different store: let it warm the dashboard history for jobs
            # that have none in memory yet, instead of serving the old
            # store's history forever.
            self._state_rehydrated = False
            # the concurrency slots live in the OLD store: drop the held
            # leases (they lapse there by TTL) and stop their renewers and
            # any Replace pursuits; re-claiming in the new store is the
            # next launch's business. The lock-fidelity verdict is also
            # per-store.
            for task in list(self._slot_renewers.values()):
                task.cancel()
            self._slot_renewers.clear()
            self._slot_leases.clear()
            for task in list(self._slot_pursuits.values()):
                task.cancel()
            self._slot_pursuits.clear()
            self._slot_fidelity = None
            # The @reboot gate's "store timed out, stop probing" latch is a
            # per-store health verdict just like the lock-fidelity one above:
            # a replacement store earns fresh probes, it does not inherit the
            # dead store's degraded no-dedupe mode for the life of the
            # process.
            self._reboot_gate_sick = False
            if self._retry_claim_task is not None:
                self._retry_claim_task.cancel()
                self._retry_claim_task = None
            # the DAG advance leases and next-fire index also belong
            # to the old store; drop them (renewers cancelled, leases lapse by
            # TTL) so the new store's active runs are re-adopted from scratch
            # by reconcile_on_boot (re-run because _state_rehydrated cleared).
            self._dag.forget()
        elif backend is not None and state_config is not None:
            # config byte-identical, so the teardown above did not fire: the
            # store backend stays, but the job API's TLS certificate may have
            # rotated in place under it. Cheap stat-compare once per pass.
            await self._maybe_restart_job_api_for_tls(state_config)
        if state_config is not None and self.state_backend is None:
            try:
                # Construct INSIDE the try (dir creation + write probe can
                # raise OSError). BOUNDED: a hard-hung mount blocks the
                # probe uninterruptibly, and an unbounded await would stall
                # run() before it ever schedules a job; timing out degrades
                # to the in-memory path and retries next pass.
                backend = make_state_backend(state_config, self.job_set_id)
                await asyncio.wait_for(
                    backend.start(), timeout=STATE_OP_TIMEOUT
                )
            except (OSError, ConfigError, asyncio.TimeoutError) as ex:
                # an operational misconfiguration (unwritable path, bad mount)
                # to log and keep running through, not the run loop's generic
                # "report a bug" path.
                logger.error(
                    "state: failed to start: %s",
                    str(ex) or type(ex).__name__,
                )
                return
            self.state_backend = backend
            self._state_max_runs = state_config.get("maxRunsPerJob", 0)
            # a fresh backend generation re-anchors the periodic chores:
            # record this node's manifest immediately (the GC anchor), and
            # let the first GC pass run on the next housekeeping tick;
            # gcGraceSeconds is what protects young state, not a delay here.
            self._manifest_next = 0.0
            self._gc_next = 0.0
            # ...and re-anchor the two store sweeps so a store that just came
            # up (or was swapped) converges on the next housekeeping pass
            # rather than waiting out the interval the dead one started.
            self._pause_refresh_next = 0.0
            self._retry_claim_next = 0.0
            # land any pause/resume taken while the store was down BEFORE
            # the rehydrate below re-reads the paused/ streams, so it cannot
            # revert those jobs to the records they supersede.
            self._replay_pending_pause_writes()
            # warm the in-memory history from the ledger the first time a
            # backend comes up, so a restart's dashboard/status is populated at
            # once instead of blank until each job next runs.
            await self._rehydrate_from_state()
            # expose this backend to job commands over a loopback
            # endpoint (opt-out via state.jobApi.enabled). A start failure is
            # logged and swallowed: the scheduler's own durable features do
            # not depend on it.
            await self._start_job_api(state_config)

    async def _start_job_api(self, state_config: StateConfig) -> None:
        """Stand up the loopback job-state API for this backend, if enabled."""
        job_api_cfg = dict(state_config.get("jobApi") or {})
        if not job_api_cfg.get("enabled", True):
            return
        # lazy import (like state_admin and the lease backends): the module
        # never enters the graph unless a job API is actually configured.
        from cronstable.jobapi import JobStateAPI

        # Snapshot the TLS files BEFORE api.start() loads them, mirroring
        # _build_web_tls: a rotation landing in the gap then compares unequal
        # on the next pass, which is a spurious restart (the safe direction),
        # not a missed one. None for a plaintext endpoint (no cert/key), which
        # therefore never triggers a rotation restart.
        job_api_tls = job_api_cfg.get("tls")
        tls_signature: Optional[dict[str, Any]] = None
        if tlsutil.listener_tls_configured(job_api_tls):
            # listener_tls_configured is truthy only when cert and key are
            # present, so the block is non-None here; the assert is only so
            # the call type-checks (it is not a TypeGuard).
            assert job_api_tls is not None
            tls_signature = tlsutil.tls_file_signature(
                job_api_tls, tlsutil.JOB_API_TLS_KEYS
            )
        api = JobStateAPI(
            lambda: self.state_backend,
            base_holder=self._slot_holder(),
            config=job_api_cfg,
        )
        try:
            await asyncio.wait_for(api.start(), timeout=STATE_OP_TIMEOUT)
        except (OSError, asyncio.TimeoutError) as ex:
            logger.error(
                "state: job API failed to start (jobs will run without the "
                "loopback state endpoint): %s",
                str(ex) or type(ex).__name__,
            )
            return
        self._job_api = api
        self._job_api_tls_signature = tls_signature

    async def _stop_job_api(self) -> None:
        api = self._job_api
        if api is None:
            return
        self._job_api = None
        self._job_api_tls_signature = None
        try:
            await asyncio.wait_for(api.stop(), timeout=STATE_OP_TIMEOUT)
        except (OSError, asyncio.TimeoutError) as ex:
            logger.warning("state: job API did not stop cleanly: %s", ex)

    def _job_api_tls_files_changed(
        self, tls: Optional[dict[str, Any]]
    ) -> bool:
        """Whether the running job API's TLS files differ from what it loaded.

        The only thing that makes an in-place cert rotation of this
        listener visible (the state config is byte-identical across one).
        Only cert/key are watched: the ``ca`` is handed to jobs as a path
        and read fresh by each. None signature (plaintext endpoint) never
        restarts on this.
        """
        if self._job_api_tls_signature is None or not tls:
            return False
        return (
            tlsutil.tls_file_signature(tls, tlsutil.JOB_API_TLS_KEYS)
            != self._job_api_tls_signature
        )

    async def _maybe_restart_job_api_for_tls(
        self, state_config: StateConfig
    ) -> None:
        """Restart the job API listener if its TLS cert/key rotated in place.

        Called on an otherwise no-op reload; only the listener is rebuilt,
        the backend and its leases are untouched. Gated on the new
        material actually loading: a half-written rotation would tear a
        working endpoint down and fail to rebuild it. The rebind itself is
        not pre-validated (make-before-break is infeasible on one port); a
        failed rebind leaves the endpoint down until the state config next
        changes.
        """
        tls = (state_config.get("jobApi") or {}).get("tls")
        if self._job_api is None or not self._job_api_tls_files_changed(tls):
            return
        if not tlsutil.listener_tls_loadable(tls):
            logger.warning(
                "state: new job API TLS material is not yet loadable (a "
                "partial/half-written rotation, or a config edit racing "
                "one?); keeping the running endpoint and retrying next reload"
            )
            return
        logger.info(
            "state: job API TLS certificate files changed, restarting the "
            "loopback state endpoint"
        )
        await self._stop_job_api()
        await self._start_job_api(state_config)

    def _track_state_write(
        self, coro: Coroutine[Any, Any, None]
    ) -> asyncio.Task:
        """Run a durable-state write as a tracked fire-and-forget task.

        The single idiom for every durable write: tracked so it is not
        GC'd mid-flight and the shutdown flush can bound-wait it; never
        awaited on a scheduling path. The coroutine logs its own failures.

        Past MAX_PENDING_STATE_WRITES the new write is shed: its coroutine
        closed, the drop counted, and a resolved placeholder returned so
        callers that chain on the result keep working. Safe because every
        state write is best-effort; the alternative is unbounded growth.
        """
        if len(self._pending_state_writes) >= MAX_PENDING_STATE_WRITES:
            coro.close()
            self.metrics.state_write_dropped("overflow")
            task = asyncio.create_task(_noop_state_write())
        else:
            task = asyncio.create_task(coro)
        self._pending_state_writes.add(task)
        task.add_done_callback(self._pending_state_writes.discard)
        return task

    def _dispatch_notify(
        self,
        event: str,
        *,
        success: bool,
        name: str,
        subject: str,
        message: str,
        **fields: Any,
    ) -> None:
        """Fire the ``notify:`` reporters for a daemon/orchestration event.

        Synchronous and callable from any loop: the fan-out runs as a
        tracked fire-and-forget task so a slow reporter never stalls the
        caller. No-op when notify: is unconfigured or the event is off
        its allow-list.
        """
        cfg = self._notify_config
        if cfg is None:
            return
        allowed = cfg.get("events")
        if allowed is not None and event not in allowed:
            return
        ctx = NotifyEventContext(
            event=event,
            success=success,
            name=name,
            subject=subject,
            message=message,
            fields=fields,
        )
        task = asyncio.create_task(report_event(ctx, cfg["report"]))
        self._notify_tasks.add(task)
        task.add_done_callback(self._notify_tasks.discard)

    def _state_periodic(self) -> None:
        """Kick off the periodic durable-state chores that are due.

        Called from the housekeeping pass; spawns tracked background
        tasks. No-op without a running backend.
        """
        if self.state_backend is None:
            return
        now = asyncio.get_running_loop().time()
        if now >= self._manifest_next:
            self._manifest_next = now + STATE_MANIFEST_INTERVAL
            self._track_state_write(self._persist_manifest())
        if (
            self._state_gc_grace > 0
            and now >= self._gc_next
            and (self._gc_task is None or self._gc_task.done())
        ):
            self._gc_next = now + STATE_GC_INTERVAL
            self._gc_task = self._track_state_write(
                self._collect_state_garbage()
            )
        if (
            now >= self._retry_claim_next
            and self._retry_resume_active()
            and (
                self._retry_claim_task is None or self._retry_claim_task.done()
            )
        ):
            # cross-node retry resume: scan for claimable foreign ladders
            # about once a minute (the housekeeping cadence).
            self._retry_claim_next = now + RETRY_CLAIM_INTERVAL
            self._retry_claim_task = self._track_state_write(
                self._retry_claim_scan()
            )

    def _manifest_stream(self) -> str:
        return MANIFEST_STREAM_PREFIX + self._state_host

    def _artifact_scope_names(self) -> set[str]:
        """Every artifact scope this config can write beyond its job names.

        Advertised in the manifest and folded into the GC keep map so a
        scope stays alive while any node's config still names it.
        """
        from cronstable.jobstate import GLOBAL_SCOPE

        scopes: set[str] = {GLOBAL_SCOPE}
        for job in self.cron_jobs.values():
            scopes.update(job.stateAllowedScopes)
        for dagcfg in self.cron_dags.values():
            for template in dagcfg.task_templates.values():
                scopes.update(template.stateAllowedScopes)
        return scopes

    async def _persist_manifest(self) -> None:
        """Record this node's loaded job set to its OWN manifest stream.

        GC anchor: a job's streams are garbage only when NO recent
        manifest from any host references its name. Per-host streams keep
        each host's count-based prune independent of the rest of the
        fleet.
        """
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "jobSetId": self.job_set_id(),
            "host": self._state_host,
            "jobs": sorted(self.cron_jobs),
            # Load-bearing for GC: while any recent manifest lacks these
            # keys, artifact streams and removed dags' runs stay unmanaged
            # (see _collect_state_garbage).
            "scopes": sorted(self._artifact_scope_names()),
            "dags": sorted(self.cron_dags),
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._manifest_stream()
        try:
            await backend.append_record(
                stream, record, prune_keep=MANIFEST_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - best-effort; log, survive
            self.metrics.state_write_dropped("manifest")
            logger.warning("state: failed to record the job manifest: %s", ex)

    async def _live_pause_keep(
        self, backend: StateBackend, names: set[str], now: datetime.datetime
    ) -> set[str]:
        """The pause-stream keep-set: kept jobs that hold a LIVE pause.

        Drops only names whose newest paused/<job> record is a resume,
        expired, or foreign; collection stays grace-gated on the record's
        own age, which covers the fresh-peer-write race. Fail-safe: any
        doubt keeps the name, so GC never eats a live pause.
        """
        keep = set(names)
        try:
            streams = await asyncio.wait_for(
                backend.list_stream_names(PAUSE_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - keep everything on any doubt
            logger.warning(
                "state: not reclaiming dead pause streams this GC pass: "
                "cannot enumerate them (%s)",
                ex,
            )
            return keep
        for stream in streams:
            name = stream[len(PAUSE_STREAM_PREFIX) :]
            if name not in keep:
                continue  # a removed job's stream: already collected by name
            if name in self._paused:
                # paused on THIS node: keep without a read; the pause write
                # may still be in flight with the prior resume as the
                # newest durable record.
                continue
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(stream, limit=1, newest_first=True),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - keep this one on doubt
                logger.warning(
                    "state: keeping the pause stream of %s this GC pass "
                    "(cannot read it): %s",
                    name,
                    ex,
                )
                continue
            info = self._pause_info_from_record(recs[0] if recs else None)
            if info is None or info.until <= now:
                # resumed / expired / foreign: no live pause to protect.
                keep.discard(name)
        return keep

    async def _collect_state_garbage(self) -> None:
        """One automatic garbage-collection pass (see state.gcGraceSeconds).

        Keep-set = recent manifests from every host plus this node's own
        loaded config, so GC cannot eat live jobs even when a manifest
        stream is unreadable. Also manages artifact streams and removed
        dags' runs (_gc_dag_state) and sweeps orphan blobs. Every failure
        degrades to "collect nothing this pass".
        """
        backend = self.state_backend
        grace = self._state_gc_grace
        if backend is None or grace <= 0:
            return
        try:
            stream_names = await asyncio.wait_for(
                backend.list_stream_names(MANIFEST_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping garbage collection: cannot enumerate the "
                "manifest streams (%s)",
                ex,
            )
            return
        # this node's own stream must always be included even if the
        # enumeration above raced its very first write.
        stream_names = sorted(set(stream_names) | {self._manifest_stream()})
        if len(stream_names) > MANIFEST_HOSTS_CAP:
            logger.warning(
                "state: %d manifest streams found, reading only the first "
                "%d this pass (a churning fleet with never-reused host "
                "identities?); the rest are considered this GC pass only "
                "once a run drops the count back under the cap",
                len(stream_names),
                MANIFEST_HOSTS_CAP,
            )
            stream_names = stream_names[:MANIFEST_HOSTS_CAP]
        manifests: list[dict[str, Any]] = []
        try:
            for name in stream_names:
                manifests.extend(
                    await asyncio.wait_for(
                        backend.list_records(
                            name,
                            limit=MANIFEST_STREAM_KEEP,
                            newest_first=True,
                        ),
                        timeout=STATE_OP_TIMEOUT,
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping garbage collection: cannot read the "
                "manifest streams (%s)",
                ex,
            )
            return
        now = get_now(datetime.timezone.utc)
        # Prove absence before deleting: unless manifest history spans a
        # full grace window, a job could be missing simply because nobody
        # recorded manifests yet; defer rather than collect.
        oldest: Optional[datetime.datetime] = None
        for rec in manifests:
            at = _parse_iso_utc(rec.get("at"))
            if at is not None and (oldest is None or at < oldest):
                oldest = at
        if oldest is None or (now - oldest).total_seconds() < grace:
            logger.info(
                "state: garbage collection deferred: the manifest history "
                "does not yet span gcGraceSeconds, so absence cannot be "
                "proven"
            )
            return
        names = set(self.cron_jobs)
        hosts = {self._state_host}
        live_dags = set(self.cron_dags)
        art_scopes = self._artifact_scope_names()
        keep, recent = build_gc_keep_set(
            manifests, now, grace, names, hosts, art_scopes, live_dags
        )
        scopes_covered = _manifests_cover_scopes(recent)
        # Keep pause streams only for jobs holding a LIVE pause; dead
        # paused/<job> streams age out like any other (fail-safe, see
        # _live_pause_keep).
        keep[PAUSE_STREAM_PREFIX] = await self._live_pause_keep(
            backend, names, now
        )
        if scopes_covered:
            # only under this guard: live_dags is complete only once every
            # recent manifest advertises its dags; a partial view would
            # collect a live checkpoint and re-run fenced work.
            keep[DAG_CATCHUP_STREAM_PREFIX] = live_dags
            # folds artifacts/<scope> into keep and collects removed dags'
            # run documents.
            await self._gc_dag_state(
                backend, keep, art_scopes, live_dags, grace
            )
        else:
            logger.info(
                "state: leaving artifact streams, dag catch-up checkpoints "
                "and dag-run documents unmanaged this GC pass: a recent "
                "manifest does not advertise its scopes/dags (a node "
                "predating them, or the first grace window after upgrading)"
            )
        from cronstable.dag import DAG_LEASE_PREFIX

        try:
            # bounded: a wedged worker thread must not leave _gc_task
            # pending forever (single-flight would then disable auto GC).
            result = await asyncio.wait_for(
                backend.collect_garbage(
                    keep=keep,
                    grace=grace,
                    # only the per-run advance leases are reclaimable: every
                    # other lease's fence can outlive the grace window inside
                    # persisted slot cancel records (see the backend's GC
                    # docstring).
                    ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
                ),
                timeout=STATE_GC_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning("state: garbage collection failed: %s", ex)
            return
        if result.get("streams_removed") or result.get("tmp_removed"):
            logger.info(
                "state: garbage collected %s stream(s) (%s record(s)), "
                "%s temp file(s), %s quarantined record(s)",
                result.get("streams_removed", 0),
                result.get("records_removed", 0),
                result.get("tmp_removed", 0),
                result.get("quarantine_removed", 0),
            )
        # only after a successful collect pass: the records deleted above
        # (and the XCom streams dagrun's retention pruned since the last
        # pass) are what release their blobs for the sweep.
        await self._sweep_orphan_artifact_blobs(backend, grace)

    async def _gc_dag_state(
        self,
        backend: StateBackend,
        keep: dict[str, set[str]],
        art_scopes: set[str],
        live_dags: set[str],
        grace: float,
    ) -> None:
        """Extend one GC pass over artifact streams and dag run documents.

        Hands removed dags to DagScheduler.gc_removed_dags, then keys
        ``artifacts/`` retention on the live scopes (job names, shared
        scopes, XCom scopes of run documents still on disk). Any doubt
        leaves artifact streams unmanaged this pass.
        """
        from cronstable.dag import DAG_RUN_NS_PREFIX, xcom_scope
        from cronstable.jobstate import ARTIFACT_STREAM_PREFIX

        try:
            namespaces, complete = await asyncio.wait_for(
                backend.list_document_namespaces(DAG_RUN_NS_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: "
                "cannot enumerate the dag-run namespaces (%s)",
                ex,
            )
            return
        if not complete:
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: a "
                "dag-run namespace exists whose name cannot be recovered, "
                "so its runs' XCom scopes cannot be protected"
            )
            return
        removed_dags = {
            ns[len(DAG_RUN_NS_PREFIX) :] for ns in namespaces
        } - live_dags
        if removed_dags:
            await self._dag.gc_removed_dags(backend, removed_dags, grace)
        try:
            for ns in namespaces:
                dag_name = ns[len(DAG_RUN_NS_PREFIX) :]
                docs = await asyncio.wait_for(
                    backend.list_documents(ns),
                    timeout=STATE_OP_TIMEOUT,
                )
                for body in docs:
                    run_id = body.get("runId")
                    if run_id:
                        art_scopes.add(xcom_scope(dag_name, str(run_id)))
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: leaving artifact streams unmanaged this GC pass: "
                "cannot read the dag-run documents (%s)",
                ex,
            )
            return
        keep[ARTIFACT_STREAM_PREFIX] = art_scopes

    async def _sweep_orphan_artifact_blobs(
        self, backend: StateBackend, grace: float
    ) -> None:
        """Reclaim artifact/XCom payload blobs no surviving record names.

        Biased to KEEP on every doubt: skipped when any artifact stream is
        unenumerable or any record unreadable, and the backend's age guard
        keeps blobs younger than grace (payload landed, record not yet
        appended).
        """
        from cronstable.jobstate import (
            ARTIFACT_STREAM_PREFIX,
            referenced_blob_digests,
        )

        try:
            stream_names, complete = await asyncio.wait_for(
                backend.list_stream_names_audit(ARTIFACT_STREAM_PREFIX),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping the orphan-blob sweep: cannot enumerate "
                "the artifact streams (%s)",
                ex,
            )
            return
        if not complete:
            logger.warning(
                "state: skipping the orphan-blob sweep: an artifact stream "
                "exists whose records cannot be enumerated, so its blob "
                "references cannot be ruled out"
            )
            return
        scopes = [name[len(ARTIFACT_STREAM_PREFIX) :] for name in stream_names]
        try:
            referenced = await asyncio.wait_for(
                referenced_blob_digests(backend, scopes, strict=True),
                timeout=STATE_GC_TIMEOUT,
            )
            removed = await asyncio.wait_for(
                backend.sweep_orphan_blobs(referenced, grace),
                timeout=STATE_GC_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: skipping the orphan-blob sweep: an artifact record "
                "could not be read, so its blob reference cannot be ruled "
                "out (%s)",
                ex,
            )
            return
        if removed:
            logger.info("state: swept %d orphaned artifact blob(s)", removed)

    @staticmethod
    def _resolve_web_secret(spec: dict, what: str) -> str:
        """Resolve one ``{value|fromFile|fromEnvVar}`` bearer secret.

        Fails closed: a configured source that resolves empty raises
        ConfigError rather than leaving the web API unauthenticated.
        ``what`` names the config key for the error message.
        """
        if spec.get("value"):
            token = str(spec["value"])
        elif spec.get("fromFile"):
            try:
                with open(spec["fromFile"], "rt") as token_file:
                    token = token_file.read().strip()
            # UnicodeDecodeError too: a binary token file raises it from
            # read(), and only ConfigError gets the clean "not starting
            # the web API" handling in run(). Mirrors config._resolve_secret.
            except (OSError, UnicodeDecodeError) as ex:
                raise ConfigError(
                    "{}.fromFile could not be read: {}".format(what, ex)
                ) from ex
        elif spec.get("fromEnvVar"):
            token = os.environ.get(spec["fromEnvVar"], "")
        else:
            token = ""
        if not token:
            raise ConfigError(
                "{} is configured but resolved to an empty token; refusing "
                "to start the web API without authentication".format(what)
            )
        return token

    @staticmethod
    def _resolve_web_token(web_config: WebConfig) -> Optional[str]:
        # The scalar web.authToken: an all-scopes token, or None when not
        # configured. _resolve_web_tokens builds on it.
        auth = web_config.get("authToken")
        if not auth:
            return None
        return Cron._resolve_web_secret(auth, "web.authToken")

    @staticmethod
    def _resolve_web_tokens(
        web_config: WebConfig,
    ) -> "Optional[list[_WebToken]]":
        """Resolve every configured web bearer token into a lookup table.

        Scalar authToken becomes an all-scopes token; each authTokens
        entry a scoped one. Returns None when neither is configured (the
        sentinel for "no auth middleware"). Fails closed on empty sources
        and on two tokens sharing one secret: matching is by secret, so
        only one entry's scopes could apply.
        """
        tokens: list[_WebToken] = []
        scalar = Cron._resolve_web_token(web_config)
        if scalar is not None:
            tokens.append(
                _WebToken(scalar.encode("utf-8"), _WEB_ALL_SCOPES, "authToken")
            )
        for index, entry in enumerate(web_config.get("authTokens") or []):
            what = "web.authTokens[{}]".format(index)
            secret = Cron._resolve_web_secret(entry, what)
            scopes = _effective_web_scopes(entry.get("scopes") or [])
            label = entry.get("label") or what
            tokens.append(_WebToken(secret.encode("utf-8"), scopes, label))
        seen: dict[bytes, str] = {}
        for token in tokens:
            first = seen.get(token.token_bytes)
            if first is not None:
                # names only, never the secret
                raise ConfigError(
                    "web token {!r} resolves to the same secret as {!r}; "
                    "duplicate secrets are refused because requests match "
                    "tokens by secret, so only one entry's scopes could "
                    "apply".format(token.label, first)
                )
            seen[token.token_bytes] = token.label
        return tokens or None

    @staticmethod
    def _make_auth_middleware(
        tokens, public_paths: "frozenset[str]" = frozenset()
    ):
        # A bare string is a single all-scopes token (callers/tests rely
        # on this).
        if isinstance(tokens, str):
            tokens = [
                _WebToken(tokens.encode("utf-8"), _WEB_ALL_SCOPES, "authToken")
            ]
        token_table = list(tokens)

        @web.middleware
        async def auth_middleware(request, handler):
            if public_paths and request.path in public_paths:
                return await handler(request)
            # CORS preflights pass without a token: the Fetch standard
            # strips credentials from them, and a preflight response
            # carries only CORS policy. Matched by the defining header,
            # not by path, so a bare OPTIONS probe stays a 401.
            if (
                request.method == "OPTIONS"
                and "Access-Control-Request-Method" in request.headers
            ):
                return await handler(request)
            header = request.headers.get("Authorization", "")
            scheme, _, presented = header.partition(" ")
            # RFC 7235: the auth scheme is case-insensitive (Bearer/bearer).
            # Compare only the token, in constant time, to avoid leaking it via
            # timing (the scheme is not secret).
            if scheme.lower() != "bearer":
                # Calendar clients cannot attach a bearer header, so only
                # the calendar-feed paths accept the token as a `token`
                # query parameter (access log redacts it, see
                # _access_log_class). Matched precisely so no future route
                # gains URL-token auth by accident.
                if request.path.endswith("/calendar.ics"):
                    presented = request.query.get("token", "")
                else:
                    raise web.HTTPUnauthorized()
            if not presented:
                raise web.HTTPUnauthorized()
            try:
                # compare as bytes: compare_digest raises TypeError on any
                # non-ASCII str (turning a garbage token into a 500, not a
                # 401), and a header that cannot even encode (surrogates
                # from raw header bytes) can never match a real token.
                presented_bytes = presented.encode("utf-8")
            except UnicodeEncodeError:
                raise web.HTTPUnauthorized() from None
            # Match against every configured token in constant time, with no
            # early return, so timing does not reveal which token (if any)
            # matched. A 401 means no token matched; authorization (scope)
            # is a separate, later check.
            matched: _WebToken | None = None
            for entry in token_table:
                if hmac.compare_digest(presented_bytes, entry.token_bytes):
                    matched = entry
            if matched is None:
                raise web.HTTPUnauthorized()
            # Full-scope tokens skip the per-route scope lookup. A scoped
            # token lacking the route's required scope is 403, distinct
            # from the 401 for an unrecognised token.
            if matched.scopes != _WEB_ALL_SCOPES:
                required = _required_web_scope(request)
                if required not in matched.scopes:
                    raise _api_error(
                        web.HTTPForbidden,
                        "token {!r} lacks the {!r} scope required for "
                        "this endpoint".format(matched.label, required),
                    )
            request[WEB_TOKEN_REQUEST_KEY] = matched
            return await handler(request)

        return auth_middleware

    @staticmethod
    def _make_origin_middleware(allowed_origins: "frozenset[str]"):
        """Refuse cross-site browser requests to the mutating endpoints.

        CSRF/DNS-rebinding gate: the POST control routes are CORS "simple
        requests" (no preflight), so without this any web page could fire
        them at a localhost-bound daemon. Safe methods, exempt paths
        (/mcp 403s foreign Origins itself), and Origin-less clients pass;
        an Origin matching this daemon's authority or web.allowedOrigins
        passes; anything else is 403. Residual: a rebinding page served on
        the daemon's OWN port passes the equality test; web.authToken
        closes that completely.
        """

        @web.middleware
        async def origin_middleware(request, handler):
            if request.method in WEB_SAFE_METHODS:
                return await handler(request)
            if request.path in WEB_ORIGIN_EXEMPT_PATHS:
                return await handler(request)
            origin = request.headers.get("Origin")
            if origin is None:
                return await handler(request)
            if origin in allowed_origins or _origin_matches_host(
                origin, request.host
            ):
                return await handler(request)
            raise _api_error(
                web.HTTPForbidden,
                "Origin not allowed: cross-site requests to the "
                "cronstable control API are refused; add the origin to "
                "web.allowedOrigins if it is trusted",
            )

        return origin_middleware

    def _web_tls_files_changed(self) -> bool:
        """Whether the running listener's TLS files differ from what it loaded.

        The only thing that makes an in-place cert rotation visible:
        config bytes are identical across one, so the web_config
        inequality gate never fires. Gated on _web_tls_signature because
        a teardown leaves web_config stale while clearing the signature.
        """
        if self.web_config is None or self._web_tls_signature is None:
            return False
        tls = self.web_config.get("tls")
        if not tls:
            return False
        return (
            tlsutil.tls_file_signature(tls, tlsutil.LISTENER_TLS_KEYS)
            != self._web_tls_signature
        )

    @staticmethod
    def _apply_socket_mode(addr: str, socket_mode: str) -> None:
        parsed = urlparse(addr)
        if parsed.scheme != "unix":
            return
        try:
            os.chmod(parsed.path, int(socket_mode, 8))
        except (OSError, ValueError) as ex:
            logger.warning(
                "web: could not set socketMode %r on %s: %s",
                socket_mode,
                parsed.path,
                ex,
            )

    def _needs_subminute(self) -> bool:
        """Whether any enabled job fires at second granularity.

        Gates run()'s once-per-minute housekeeping. Memoized on
        _needs_subminute_cache: a pure function of the job set; every
        site that reassigns cron_jobs clears the cache.
        """
        cached = self._needs_subminute_cache
        if cached is None:
            cached = any(
                job.has_seconds
                for job in self.cron_jobs.values()
                if job.enabled
            )
            self._needs_subminute_cache = cached
        return cached

    def _wakes_subminute(self) -> bool:
        """Whether this loop is waking more often than once a minute.

        The predicate run()'s housekeeping gate wants: covers both the
        cron job set and a DAG-shortened sleep, preserving the "at most
        once per wall-clock minute" contract _pause_periodic and
        _sla_periodic are documented on.
        """
        return self._needs_subminute() or self._dag_shortens_sleep

    def _job_pos(self) -> dict[str, int]:
        """Job name -> its position in the loaded config.

        Memoized on _job_pos_cache with the same lifecycle as the other
        job-set memos; _spawn_due_jobs uses it for launch-plan order.
        """
        cached = self._job_pos_cache
        if cached is None:
            cached = {name: i for i, name in enumerate(self.cron_jobs)}
            self._job_pos_cache = cached
        return cached

    def _any_sla(self) -> bool:
        """Whether any loaded job carries an ``sla`` block.

        Memoized alongside _job_pos; lets the SLA pass skip the walk when
        nothing declares an SLA.
        """
        cached = self._any_sla_cache
        if cached is None:
            cached = any(job.has_sla for job in self.cron_jobs.values())
            self._any_sla_cache = cached
        return cached

    # ---- next-fire index: each enabled CronTab job's next fire (aware
    # UTC) lives in _next_fire, mirrored into the _fire_heap min-heap; the
    # loop sleeps until the soonest entry and touches only due jobs.
    # Firing compares the clock against fixed forward-only instants, which
    # makes the cadence immune to clock steps (see run()).

    def _compute_next_fire(
        self, job: JobConfig, after: datetime.datetime
    ) -> Optional[datetime.datetime]:
        """The aware-UTC instant ``job`` next fires strictly after ``after``.

        The frame stays timezone-AWARE (job tz or system-local) so
        CronTab.next returns a real duration corrected for DST; a naive
        frame would land an hour off across a DST change. None when the
        schedule has no further occurrence.
        """
        crontab = job.schedule
        assert isinstance(crontab, CronTab)
        if job.timezone is not None:
            frame: datetime.datetime = after.astimezone(job.timezone)
        else:
            # no explicit timezone -> the system-local wall clock, but kept
            # AWARE (not .replace(tzinfo=None)) so the engine applies its
            # DST correction. default_utc is inert for an aware `now`.
            frame = after.astimezone()
        delay = crontab.next(now=frame, default_utc=job.utc)
        if delay is None:
            return None
        return after + datetime.timedelta(seconds=delay)

    def _set_next_fire(self, name: str, when: datetime.datetime) -> None:
        """Record ``name``'s next fire and mirror it into the heap."""
        self._next_fire[name] = when
        heapq.heappush(self._fire_heap, (when, name))

    def _ensure_seeded(self, now: datetime.datetime) -> None:
        """Seed the index for any enabled CronTab job missing from it.

        Seeds strictly-future (the next boundary after ``now``), so a job just
        added on a reload (or every job at start-up) skips the in-progress
        slot rather than firing once for the partial period already under way.
        """
        for name, job in self.cron_jobs.items():
            if name in self._next_fire:
                continue
            if job.enabled and isinstance(job.schedule, CronTab):
                nxt = self._compute_next_fire(job, now)
                if nxt is not None:
                    self._set_next_fire(name, nxt)
                    self._dead_schedules.discard(name)
                elif name not in self._dead_schedules:
                    # without this, a schedule with no future occurrence
                    # (Feb 30, a past year) would just never enter the fire
                    # index and vanish without a trace
                    self._dead_schedules.add(name)
                    logger.warning(
                        "job %r: schedule %r has no future occurrence and "
                        "will NEVER fire; fix the schedule or disable the "
                        "job (its status reports never_fires)",
                        name,
                        schedule_str(job),
                    )

    @staticmethod
    def _same_schedule(a: JobConfig, b: JobConfig) -> bool:
        """Whether two job configs fire on the same wall-clock instants.

        Compares the schedule and the RESOLVED timezone by canonical
        string (so utc and ZoneInfo("UTC") compare equal). The raw
        ``utc`` field is deliberately NOT compared: the resolved timezone
        carries it, and comparing it would only cause spurious reseeds.
        """
        return a.schedule == b.schedule and (
            a.timezone is b.timezone or str(a.timezone) == str(b.timezone)
        )

    def _refresh_schedule(
        self, now: datetime.datetime, old_jobs: dict[str, JobConfig]
    ) -> None:
        """Reconcile the next-fire index with a reloaded job set.

        Keeps the existing next-fire for unchanged schedules (so a reload
        never skips a fire on its own minute boundary), drops
        gone/disabled/changed jobs, then reseeds anything missing. Stale
        heap entries are discarded lazily on pop (_due_names).
        """
        for name in list(self._next_fire):
            job = self.cron_jobs.get(name)
            old = old_jobs.get(name)
            if (
                job is None
                or not job.enabled
                or not isinstance(job.schedule, CronTab)
                or old is None
                or not self._same_schedule(old, job)
            ):
                del self._next_fire[name]
        # keep the never-fires warning latch only for jobs whose schedule
        # survived the reload unchanged; anything removed or edited gets a
        # fresh chance to warn (or to seed) in _ensure_seeded below
        self._dead_schedules = {
            name
            for name in self._dead_schedules
            if name in self.cron_jobs
            and (old := old_jobs.get(name)) is not None
            and self._same_schedule(old, self.cron_jobs[name])
        }
        self._ensure_seeded(now)
        # Compact when stale tuples outnumber live ones: stale entries are
        # only discarded at the TOP, so long-horizon schedules and
        # frequently reminted names accumulate them across reloads.
        if len(self._fire_heap) > 2 * len(self._next_fire):
            self._fire_heap = [
                (when, name) for name, when in self._next_fire.items()
            ]
            heapq.heapify(self._fire_heap)

    def _peek_soonest_fire(self) -> Optional[datetime.datetime]:
        """The soonest valid next-fire instant, or ``None`` if nothing is
        scheduled.  Discards stale heap entries from the top as it goes."""
        heap = self._fire_heap
        while heap:
            when, name = heap[0]
            if self._next_fire.get(name) == when:
                return when
            heapq.heappop(heap)  # stale: superseded or removed
        return None

    def _sleep_interval(self) -> float:
        """Seconds to sleep until the next wake.

        The soonest job's next fire, capped by the next housekeeping
        boundary. Never negative. The cap goes through
        next_sleep_interval so tests can patch that one function.
        """
        housekeeping = next_sleep_interval(False)
        # Wake sooner when a DAG poke/retry/run is due, floored at
        # MIN_TICK_SLEEP (an already-due hint stays due until its pass
        # rewrites the entry; unfloored it would spin the loop). Whether
        # the DAG shortened the sleep is recorded for run()'s housekeeping
        # gate (see _wakes_subminute).
        self._dag_shortens_sleep = False
        dag_wake = self._dag.next_wake_delay()
        if dag_wake is not None:
            dag_wake = max(MIN_TICK_SLEEP, dag_wake)
            if dag_wake < housekeeping:
                housekeeping = dag_wake
                self._dag_shortens_sleep = True
        soonest = self._peek_soonest_fire()
        if soonest is None:
            return housekeeping
        now = get_now(datetime.timezone.utc)
        delta = (soonest - now).total_seconds()
        return max(0.0, min(housekeeping, delta))

    def _due_names(self, now: datetime.datetime) -> list[str]:
        """Names of every job whose next fire is at or before ``now``.

        Pops matching heap entries (stale ones discarded, duplicates
        returned once); the popped names' next-fire entries are left for
        _advance to read the fired slot and push the replacement.
        """
        heap = self._fire_heap
        due: list[str] = []
        seen: set[str] = set()
        while heap:
            when, name = heap[0]
            if when > now:
                break
            heapq.heappop(heap)
            if name in seen:
                continue
            if self._next_fire.get(name) == when:
                due.append(name)
                seen.add(name)
        return due

    def _advance(
        self,
        job: JobConfig,
        fire_slot: datetime.datetime,
        now: datetime.datetime,
    ) -> tuple[list[datetime.datetime], Optional[datetime.datetime]]:
        """The slots a due job launches this pass, plus its new next-fire.

        Within CATCHUP_LIMIT of ``now``, walk occurrence by occurrence
        (bounded) so a job overrun by a slow pass is not dropped. A
        larger gap is a stall/suspend/clock jump and is handled WITHOUT
        walking the window (unbounded for a per-second job): fire the
        current slot only if now itself matches, then resync to the next
        occurrence after now. This is cron's no-catch-up-after-an-outage
        rule.
        """
        if now - fire_slot >= CATCHUP_LIMIT:
            logger.warning(
                "job %s: the scheduler fell behind by %.0fs (a slow pass, "
                "stall, suspend, or clock change); resuming at the current "
                "slot instead of replaying the interval",
                job.name,
                (now - fire_slot).total_seconds(),
            )
            # Resume at the current slot, firing only if it matches, and
            # resync to the first occurrence after now (no enumeration).
            crontab = job.schedule
            assert isinstance(crontab, CronTab)
            now_slot = schedule_slot(job, now)
            # Record the fired slot as aware UTC, matching the normal
            # branch, so _last_run_slot never mixes naive and aware
            # values. schedule_slot renders now into the job's OWN frame
            # (what crontab.test matches); astimezone(utc) reads a naive
            # slot as local and is a no-op for an already-UTC one.
            fires = (
                [now_slot.astimezone(datetime.timezone.utc)]
                if crontab.test(now_slot)
                else []
            )
            return fires, self._compute_next_fire(job, now)
        fires = [fire_slot]
        nxt = self._compute_next_fire(job, fire_slot)
        while nxt is not None and nxt <= now:
            fires.append(nxt)
            nxt = self._compute_next_fire(job, nxt)
        return fires, nxt

    @staticmethod
    def _catchup_offset(name: str, jitter: int) -> float:
        """Deterministic per-job start offset in ``[0, jitter)`` seconds.

        Derived from the job name (crc32) so the spread is stable across boots
        and across the fleet, and needs no RNG.  ``0.0`` when jitter is off.
        """
        if jitter <= 0:
            return 0.0
        return (zlib.crc32(name.encode("utf-8")) % (jitter * 1000)) / 1000.0

    @staticmethod
    def _catchup_stream(name: str) -> str:
        """The durable checkpoint stream for a job's catch-up cycles."""
        return CATCHUP_STREAM_PREFIX + name

    async def _pending_catchup_watermark(self, name: str) -> Optional[str]:
        """The watermark of an unfinished backfill cycle, if one is open.

        An ``open`` without a following ``close`` means a previous
        backfill never completed; catch-up resumes from ITS watermark,
        not the run ledger's (later runs advanced the derived watermark
        past the unreplayed slots).
        """
        backend = self.state_backend
        if backend is None:
            return None
        recs = await asyncio.wait_for(
            backend.list_records(
                self._catchup_stream(name), limit=1, newest_first=True
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        if recs and recs[0].get("kind") == "open":
            watermark = recs[0].get("watermark")
            if isinstance(watermark, str):
                return watermark
        return None

    async def _pause_excusal_window(
        self, name: str
    ) -> Optional[tuple[Optional[datetime.datetime], datetime.datetime]]:
        """The newest durable pause window ``(since, until)``, for catch-up.

        Slots inside a pause window while the daemon was DOWN are not
        owed. A window, NOT a floor: backlog from before ``since`` is
        still owed once the pause lifts. Read from the store, not
        self._paused: an EXPIRED pause is absent from memory yet still
        excuses its window. A missing ``since`` excuses everything up to
        ``until``. Degrades to no window on store trouble (pause is a
        convenience, not a correctness fence).
        """
        backend = self.state_backend
        if backend is None:
            return None
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._pause_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade to no window
            logger.warning(
                "catch-up: cannot read the %s pause stream (%s); assuming "
                "no pause window",
                name,
                ex,
            )
            return None
        if recs and recs[0].get("kind") == "paused":
            until = _parse_iso_utc(recs[0].get("until"))
            if until is not None:
                return _parse_iso_utc(recs[0].get("since")), until
        return None

    async def _checkpoint_catchup(
        self, name: str, kind: str, watermark: Optional[str]
    ) -> None:
        """Append an ``open``/``close`` catch-up checkpoint (best-effort).

        A failure never blocks the backfill (costs crash-resume fidelity
        only). At-least-once: a timed-out ``open`` write can land AFTER
        the cycle's ``close`` and sort newer; the next restart then
        replays a completed cycle. Bounded replay, never a loss.
        """
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "kind": kind,
            "watermark": watermark or "",
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._catchup_stream(name)
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=CATCHUP_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - checkpoint is best-effort
            self.metrics.state_write_dropped("checkpoint")
            logger.warning(
                "catch-up: could not checkpoint %r for %s (%s); a restart "
                "mid-backfill may not resume it",
                kind,
                name,
                ex,
            )

    async def _missed_occurrences(
        self, job: JobConfig, now: datetime.datetime
    ) -> tuple[int, Optional[str]]:
        """How many catch-up launches ``job`` is owed, and from where.

        Steps the schedule forward (DST-safe) from the durable last-run
        watermark, hoisted back to an open checkpoint's older watermark,
        bounded by startingDeadlineSeconds and MAX_CATCHUP_OCCURRENCES.
        Occurrences inside a durable pause window are stepped over.
        Returns (0, ...) when nothing was missed or the job never ran
        under this store; (1, ...) for run-once; the bounded count for
        run-all. Second element is the reference watermark for the
        cycle's checkpoint. Store errors propagate: callers treat them
        as "cannot evaluate yet", never "nothing owed".
        """
        watermark = await asyncio.wait_for(
            self.durable_last_run_at(job.name), timeout=STATE_OP_TIMEOUT
        )
        after = _parse_iso_utc(watermark)
        pending = await self._pending_catchup_watermark(job.name)
        pending_dt = _parse_iso_utc(pending)
        if pending_dt is not None and (after is None or pending_dt < after):
            after, watermark = pending_dt, pending
        if after is None:
            return 0, None
        # Slots inside a (possibly expired) pause window are never owed,
        # and ONLY those: the window is skipped while walking, never used
        # as a floor on `after` (see _pause_excusal_window).
        window = await self._pause_excusal_window(job.name)
        if window is not None and after is not None:
            # Belt and braces beside the open-checkpoint pin in
            # _evaluate_catch_up: an unpinned pause can have walked
            # durable_last_run_at past the pre-pause backlog via held-slot
            # skip rows. Fall back to the skip-blind
            # durable_last_completed_at if OLDER; the `real < after` guard
            # keeps the checkpoint hoist winning when both apply.
            real = _parse_iso_utc(
                await asyncio.wait_for(
                    self.durable_last_completed_at(job.name),
                    timeout=STATE_OP_TIMEOUT,
                )
            )
            if real is not None and real < after:
                after, watermark = real, real.isoformat()
        deadline = job.startingDeadlineSeconds
        if deadline:
            cutoff = now - datetime.timedelta(seconds=deadline)
            if cutoff > after:
                after = cutoff  # only the recent window (bounds run-all)
        count = 0
        nxt = self._compute_next_fire(job, after)
        while nxt is not None and nxt <= now:
            if window is not None and _in_pause_window(nxt, window):
                # Jump the cursor to the window's end rather than stepping
                # each excused slot. One microsecond back so a slot landing
                # exactly on `until` (outside the half-open window) is not
                # stepped past. Only one window ever; drop once crossed.
                nxt = self._compute_next_fire(
                    job, window[1] - datetime.timedelta(microseconds=1)
                )
                window = None
                continue
            count += 1
            if job.onMissed == "run-once":
                return 1, watermark  # all missed slots coalesce into one
            # run-all: count each missed occurrence, hard-capped.
            if count >= MAX_CATCHUP_OCCURRENCES:
                logger.warning(
                    "catch-up: %s missed at least %d runs; replaying %d and "
                    "dropping the rest (set startingDeadlineSeconds to bound "
                    "the window, or use onMissed: run-once)",
                    job.name,
                    MAX_CATCHUP_OCCURRENCES,
                    MAX_CATCHUP_OCCURRENCES,
                )
                break
            nxt = self._compute_next_fire(job, nxt)
        return count, watermark

    async def _catch_up(self, now: datetime.datetime) -> None:
        """Replay (or coalesce) runs missed while down, on start-up.

        Stateful-only (needs a durable watermark); no-op without a
        ``state`` section. Owner-gated and spread over
        catchupJitterSeconds. Resolution is NOT latched while it cannot
        happen yet (backend not started, no positive owner, live pause):
        pending jobs are re-evaluated every CATCHUP_RECHECK_INTERVAL;
        latching would forfeit the owed backfill. Final per-job decisions
        are remembered in _catchup_done. Every evaluation is anchored to
        the FIRST attempt's instant: the live scheduler kept firing
        statelessly while the backend was down, so a later "now" would
        replay runs that actually ran.
        """
        if self._caught_up:
            return
        if asyncio.get_running_loop().time() < self._catchup_next_retry:
            return
        if self._catchup_reference is None:
            self._catchup_reference = now
        now = self._catchup_reference
        if not self._state_configured:
            wants = [
                j for j in self.cron_jobs.values() if j.onMissed != "skip"
            ]
            if wants:
                logger.warning(
                    "onMissed catch-up is set on %d job(s) but needs a "
                    "`state` backend for the last-run watermark; skipping",
                    len(wants),
                )
            inert = [j for j in self.cron_jobs.values() if j.archiveOutput]
            if inert:
                logger.warning(
                    "archiveOutput is set on %d job(s) but archives nothing "
                    "without a `state` backend",
                    len(inert),
                )
            gated = [
                j for j in self.cron_jobs.values() if j.onlyIfLastSucceeded
            ]
            if gated:
                logger.info(
                    "onlyIfLastSucceeded is set on %d job(s) with no `state` "
                    "backend: the gate works from in-memory history only and "
                    "resets on restart",
                    len(gated),
                )
            self._caught_up = True
            return
        unresolved = False
        if self.state_backend is None:
            # configured but not (yet) running (a bad mount at boot that
            # start_stop_state keeps retrying): keep the whole evaluation
            # pending rather than forfeiting the backfill.
            unresolved = True
        else:
            try:
                unresolved = await self._evaluate_catch_up(now)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - defer, never surface: this
                # runs as a detached task, so an escaped exception would be
                # an unretrieved-task warning and a silently dead catch-up.
                logger.exception(
                    "catch-up: unexpected error evaluating; will retry"
                )
                unresolved = True
        if unresolved:
            self._catchup_next_retry = (
                asyncio.get_running_loop().time() + CATCHUP_RECHECK_INTERVAL
            )
        else:
            self._caught_up = True
            self._catchup_done.clear()

    async def _evaluate_catch_up(self, now: datetime.datetime) -> bool:
        """One catch-up evaluation pass; returns whether jobs stay pending."""
        unresolved = False
        for name, job in list(self.cron_jobs.items()):
            if name in self._catchup_done:
                continue
            if (
                job.onMissed == "skip"
                or not job.enabled
                or not isinstance(job.schedule, CronTab)
            ):
                self._catchup_done.add(name)
                continue
            if self._pause_active(name) is not None:
                # No backfill while paused, but a pause excuses only its
                # own window: defer, never latch, or the pre-pause backlog
                # is forfeited. Pin the pre-pause watermark first: held
                # slots write synthetic "skipped" rows that advance
                # durable_last_run_at through the window, so an open
                # checkpoint fixes the watermark at the last real run
                # (durable_last_completed_at is skip-blind) and survives a
                # manual resume or mid-pause restart. The is-None guard
                # keeps the recheck loop from re-pinning.
                try:
                    if await self._pending_catchup_watermark(name) is None:
                        real = await asyncio.wait_for(
                            self.durable_last_completed_at(name),
                            timeout=STATE_OP_TIMEOUT,
                        )
                        if real is not None:
                            await self._checkpoint_catchup(name, "open", real)
                except asyncio.CancelledError:
                    raise
                except Exception as ex:  # noqa: BLE001 - defer, never latch
                    logger.warning(
                        "catch-up: cannot pin the pre-pause watermark for %s "
                        "(%s); will retry",
                        name,
                        ex,
                    )
                logger.debug(
                    "catch-up: %s is paused; deferring its evaluation until "
                    "the pause lifts",
                    name,
                )
                unresolved = True
                continue
            # Gate before the durable read: no store I/O for a job this
            # node may not run.
            if not self._cluster_allows(job):
                if self._cluster_owner_moved(job):
                    # positive confirmation another node owns it: its
                    # owner sees the same ledger and does the backfill.
                    logger.info(
                        "catch-up: %s is owned by another node; leaving "
                        "any backfill to its owner",
                        name,
                    )
                    self._catchup_done.add(name)
                else:
                    # transient denial (no owner elected yet, no quorum,
                    # conflict): nobody would backfill if we latched now.
                    unresolved = True
                continue
            try:
                count, watermark = await self._missed_occurrences(job, now)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - defer, never crash
                logger.warning(
                    "catch-up: cannot read the %s watermark (%s); will retry",
                    name,
                    ex,
                )
                unresolved = True
                continue
            if count <= 0:
                self._catchup_done.add(name)
                continue
            # Checkpoint the intent BEFORE scheduling: a crash/restart
            # mid-jitter or mid-backfill then resumes from `watermark`
            # instead of losing the owed slots to the advancing ledger.
            await self._checkpoint_catchup(name, "open", watermark)
            offset = self._catchup_offset(name, job.catchupJitterSeconds)
            task = asyncio.create_task(
                self._run_catch_up(job, count, offset, now)
            )
            self._catchup_tasks.add(task)
            task.add_done_callback(self._catchup_tasks.discard)
            self._catchup_done.add(name)
        return unresolved

    async def _run_catch_up(
        self,
        job: JobConfig,
        count: int,
        offset: float,
        now: datetime.datetime,
    ) -> None:
        """Launch a job's catch-up runs, after its jitter offset.

        Sleeps out the jitter (interruptibly), then REVALIDATES what the
        sleep may have invalidated: reload (the live definition is
        launched), moved ownership (the new owner resumes from the open
        checkpoint), changed owed count. Launches are SERIALIZED so
        Forbid cannot swallow owed runs and run-all cannot stampede.
        Uses maybe_launch_job with with_retries=False: a backfill must
        not arm retries nor capture a live retry ladder armed by a
        concurrent scheduled fire. Each finished run advances the
        watermark; the checkpoint closes when the backfill completes.
        """
        try:
            if offset > 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=offset
                    )
                except asyncio.TimeoutError:
                    pass  # normal: the jitter elapsed without a shutdown
            if self._stop_event.is_set():
                return
            current = self.cron_jobs.get(job.name)
            if (
                current is None
                or current.onMissed == "skip"
                or not current.enabled
                or not isinstance(current.schedule, CronTab)
            ):
                logger.info(
                    "catch-up: %s was removed or disabled during its jitter "
                    "window; dropping the backfill",
                    job.name,
                )
                return
            job = current  # the live definition, not the boot-time capture
            if not self._cluster_allows(job):
                logger.info(
                    "catch-up: ownership of %s moved during its jitter "
                    "window; leaving the backfill to the new owner",
                    job.name,
                )
                return
            # Recompute against the ORIGINAL pass instant: a fresh `now`
            # would stretch the window over slots the live scheduler
            # already fired during the jitter and replay them.
            try:
                count, watermark = await self._missed_occurrences(job, now)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - drop, resume on restart
                logger.warning(
                    "catch-up: cannot re-read the %s watermark after its "
                    "jitter (%s); dropping the backfill (the open checkpoint "
                    "resumes it on the next restart)",
                    job.name,
                    ex,
                )
                return
            if count <= 0:
                # someone else (another node, or ordinary runs) already
                # covered it: close the cycle so restarts stop resuming it.
                await self._checkpoint_catchup(job.name, "close", watermark)
                return
            logger.info(
                "catch-up: replaying %d missed run(s) for %s", count, job.name
            )
            # Only Forbid must wait for TOTAL idleness. For Allow/Replace
            # the wait is anti-stampede pacing, so it is bounded; an
            # always-overlapping job would otherwise starve the backfill
            # forever and leave its checkpoint open.
            max_wait: Optional[float] = (
                None
                if job.concurrencyPolicy == "Forbid"
                else CATCHUP_IDLE_WAIT_LIMIT
            )
            for _ in range(count):
                if not await self._wait_job_idle(job.name, max_wait=max_wait):
                    return  # shutdown while draining
                # Revalidate EVERY iteration, not just after the jitter: a
                # serialized run-all backfill spans count x run-duration,
                # plenty of time for a reload to remove/disable the job or
                # for ownership to move; launching on after either would
                # run a dead definition or double-run against the new owner
                # (which resumes from the still-open checkpoint).
                live = self.cron_jobs.get(job.name)
                if (
                    live is None
                    or live.onMissed == "skip"
                    or not live.enabled
                    or not isinstance(live.schedule, CronTab)
                ):
                    logger.info(
                        "catch-up: %s was removed or disabled mid-backfill; "
                        "dropping the remaining runs",
                        job.name,
                    )
                    return
                job = live
                if not self._cluster_allows(job):
                    logger.info(
                        "catch-up: ownership of %s moved mid-backfill; "
                        "leaving the remainder to the new owner",
                        job.name,
                    )
                    return
                if self._pause_active(job.name) is not None:
                    # dropping WITHOUT closing the checkpoint: slots owed
                    # from before the pause stay resumable after it expires
                    # (the pause window excuses only its own slots).
                    logger.info(
                        "catch-up: %s was paused mid-backfill; dropping "
                        "the remaining runs",
                        job.name,
                    )
                    return
                await self.maybe_launch_job(job, with_retries=False)
            # drain the final launch so its run record lands before the
            # checkpoint closes (a crash in between merely replays: the
            # checkpoint is at-least-once by design).
            if not await self._wait_job_idle(job.name, max_wait=max_wait):
                return
            await self._checkpoint_catchup(job.name, "close", watermark)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a backfill must never kill the loop
            logger.exception(
                "catch-up: unexpected error backfilling %s", job.name
            )

    async def _wait_job_idle(
        self, name: str, *, max_wait: Optional[float] = None
    ) -> bool:
        """Wait until no instance of ``name`` is running (backfill pacing).

        False when shutdown was signalled while waiting; True means "go
        ahead" (idle, or max_wait elapsed for the non-Forbid policies
        where the wait is pacing, not correctness).
        """
        waited = 0.0
        while self.running_jobs.get(name):
            if max_wait is not None and waited >= max_wait:
                return not self._stop_event.is_set()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                waited += 0.5
                continue
            return False
        return not self._stop_event.is_set()

    async def _service_slots(self, startup: bool) -> None:
        """Service the jobs due on this pass.

        Reads the clock once, AFTER any slow housekeeping, so a fire the
        reload pushed past is still serviced. The start-up pass seeds
        the next-fire index strictly-future and runs catch-up.
        """
        now = get_now(datetime.timezone.utc)
        if startup:
            self._ensure_seeded(now)
        await self.spawn_jobs(startup, now)
        # Every pass, not just start-up: _catch_up latches itself once
        # resolved but must retry when backend/cluster were not ready at
        # boot. Spawned, not awaited: a slow-but-alive mount must not
        # stall this pass. Tracked so shutdown cancels a straggler.
        if not self._caught_up and (
            self._catchup_eval_task is None or self._catchup_eval_task.done()
        ):
            task = asyncio.create_task(self._catch_up(now))
            self._catchup_eval_task = task
            self._catchup_tasks.add(task)
            task.add_done_callback(self._catchup_tasks.discard)
        # DAG scheduler: single-flight and self-gated, so a pass with no
        # DAG work due is a couple of cheap in-memory checks.
        self._dag.service()

    async def spawn_jobs(
        self, startup: bool, now: Optional[datetime.datetime] = None
    ) -> None:
        """Launch the jobs due on this pass.

        At start-up only ``@reboot`` jobs are due (CronTab jobs are
        seeded strictly-future). Normal passes pull due jobs from the
        next-fire index, each advanced with bounded catch-up, so a job
        fires at most once per slot. ``now`` may be omitted by a direct
        caller for a fresh read.
        """
        if now is None:
            now = get_now(datetime.timezone.utc)
        self._log_cluster_role()
        if startup:
            await self._spawn_reboot_jobs()
        else:
            await self._spawn_due_jobs(now)
        await self._process_pending_reboots()
        await self._process_paused_reboots()

    async def _spawn_reboot_jobs(self) -> None:
        """Launch ``@reboot`` jobs at start-up, in config order.

        Leader/PreferLeader @reboot jobs under election are deferred
        until an owner is elected (running now would skip forever or run
        on every node); EveryNode is not deferred. A PAUSED @reboot job
        is also deferred rather than handed to the launcher, whose pause
        gate would skip it after the boot marker was already burnt: a
        pause defers the boot run, it does not forfeit it.
        """
        to_launch: list[JobConfig] = []
        for job in self.cron_jobs.values():
            # job_should_run(startup=True) is True only for an enabled @reboot
            # job, so everything below concerns @reboot one-shots.
            if not self.job_should_run(True, job, None):
                continue
            if self._is_deferrable_reboot(job):
                self._pending_reboot_jobs[job.name] = job
                logger.info(
                    "cluster: deferring @reboot job %s until the cluster "
                    "elects an owner",
                    job.name,
                )
                continue
            if self._cluster_allows(job):
                if self._pause_active(job.name) is not None:
                    self._defer_paused_reboot(job.name)
                    continue
                to_launch.append(job)
        # Deferred Leader/PreferLeader jobs never reach here; their dedupe
        # is the cluster's reboot_ran path.
        await self._launch_boot_gated(to_launch)

    async def _launch_boot_gated(self, to_launch: list[JobConfig]) -> None:
        """Launch @reboot jobs behind the state-backed boot dedupe.

        A daemon restart within one OS boot must not re-run boot
        one-shots (standalone / EveryNode); stateless installs launch
        ungated.
        """
        if to_launch and self._state_configured:
            gated = []
            for job in to_launch:
                if await self._reboot_boot_gate(job):
                    gated.append(job)
            to_launch = gated
        await self._launch_concurrently(to_launch)

    def _defer_paused_reboot(self, name: str) -> None:
        """Hold a paused @reboot job's boot run until the pause lifts."""
        if name in self._paused_reboot_jobs:
            return
        self._paused_reboot_jobs.add(name)
        pause = self._pause_active(name)
        logger.info(
            "Job %s (@reboot) is paused%s; its boot run is deferred, not "
            "lost: no boot marker is recorded, so it runs once the pause "
            "lifts (or after a restart, still once per OS boot)",
            name,
            (
                " until {}".format(pause.until.isoformat())
                if pause is not None
                else ""
            ),
        )

    async def _process_paused_reboots(self) -> None:
        """Run a paused @reboot job's deferred boot run once the pause lifts.

        Called every scheduling pass. The boot marker is written HERE by
        _reboot_boot_gate, so the deferred run stays once per OS boot.
        Retirement mirrors _process_pending_reboots: a transiently absent
        name stays owed; a name reused for a non-@reboot job is dropped.
        """
        if not self._paused_reboot_jobs:
            return
        to_launch: list[JobConfig] = []
        for name in list(self._paused_reboot_jobs):
            job = self.cron_jobs.get(name)
            if job is None:
                continue  # transiently absent -> still owed, re-check later
            if (
                not isinstance(job.schedule, str)
                or job.schedule != "@reboot"
                or not job.enabled
            ):
                self._paused_reboot_jobs.discard(name)
                continue
            if self._pause_active(name) is not None:
                continue
            if not self._cluster_allows(job):
                continue  # ownership moved: keep it owed, as the gated path
            self._paused_reboot_jobs.discard(name)
            logger.info(
                "Job %s (@reboot): its pause lifted; running the boot run "
                "it deferred",
                name,
            )
            to_launch.append(job)
        await self._launch_boot_gated(to_launch)

    async def _spawn_due_jobs(self, now: datetime.datetime) -> None:
        """Launch the jobs whose next fire has arrived, in config order.

        Due jobs come from the next-fire index; each is advanced past its fired
        slot (with bounded catch-up) BEFORE any launch awaits, so the index is
        already current if the launches yield.
        """
        due = self._due_names(now)
        if not due:
            return
        # Build the launch plan in config order via the memoized position
        # index (keeps the pass O(due), not O(all jobs)). A name the index
        # holds but the config no longer does is skipped.
        pos = self._job_pos()
        ordered = [name for name in due if name in pos]
        ordered.sort(key=pos.__getitem__)
        plan: list[tuple[JobConfig, list[datetime.datetime]]] = []
        for name in ordered:
            job = self.cron_jobs[name]
            fires, new_next = self._advance(job, self._next_fire[name], now)
            if new_next is not None:
                self._set_next_fire(name, new_next)
            else:
                # no further occurrence: drop from the index and latch
                # _dead_schedules (what _scheduled_in and
                # _schedule_never_fires consult) so status polls do not
                # re-walk the schedule horizon.
                self._next_fire.pop(name, None)
                if name not in self._dead_schedules:
                    self._dead_schedules.add(name)
                    logger.warning(
                        "job %r: schedule %r has no further occurrence "
                        "and will NEVER fire again; fix the schedule or "
                        "disable the job (its status reports "
                        "never_fires)",
                        name,
                        schedule_str(job),
                    )
            plan.append((job, fires))
        await self._launch_plan(plan)

    async def _launch_plan(
        self, plan: list[tuple[JobConfig, list[datetime.datetime]]]
    ) -> None:
        """Launch a due-job plan.

        One round per catch-up depth: within a round due jobs launch
        concurrently in config order; a single job's own catch-up replays
        run in successive rounds so its concurrencyPolicy still applies
        between them.
        """
        rounds = max((len(fires) for _, fires in plan), default=0)
        for r in range(rounds):
            to_launch: list[JobConfig] = []
            for job, fires in plan:
                if r >= len(fires):
                    continue
                # record the slot this fire is for (status/introspection),
                # recorded whether or not this node runs it.
                self._last_run_slot[job.name] = fires[r]
                if self._cluster_allows(job):
                    # lateAfter reference, recorded INSIDE the ownership
                    # gate: only the node that would run the slot owes it
                    # (a follower recording slots would page a false
                    # breach on failover). A pause-skipped slot is excused.
                    if self._pause_active(job.name) is None:
                        self._sla_due[job.name] = fires[r]
                    to_launch.append(job)
            await self._launch_concurrently(to_launch)

    async def _launch_concurrently(self, to_launch: list[JobConfig]) -> None:
        """Launch every job in ``to_launch`` concurrently, in config order.

        Gathering collapses N spawns to about one spawn-time; launches
        are independent (each touches only its own name's entries).
        """
        if len(to_launch) == 1:
            await self.launch_scheduled_job(to_launch[0])
        elif to_launch:
            await asyncio.gather(
                *(self.launch_scheduled_job(job) for job in to_launch)
            )

    def _is_deferrable_reboot(self, job: JobConfig) -> bool:
        """Whether ``job`` is an @reboot job whose start must wait for the
        cluster to elect an owner (a ``Leader``/``PreferLeader`` job under
        ``electLeader``)."""
        return (
            self._elect_leader_configured
            and isinstance(job.schedule, str)
            and job.schedule == "@reboot"
            and job.clusterPolicy in ("Leader", "PreferLeader")
        )

    def _same_boot(self, rec: dict[str, Any]) -> bool:
        """Whether a boot-marker record was written during THIS OS boot.

        Prefers the exact per-boot UUID (Linux); falls back to comparing
        derived boot times within :data:`BOOT_TIME_TOLERANCE` (the
        derivation rides the wall clock, so NTP steps shift it slightly).
        ``False`` when neither side can be identified: an unprovable "same
        boot" must run the job (today's behaviour) rather than eat it.
        """
        boot_id = platform.os_boot_id()
        rec_id = rec.get("bootId")
        if boot_id is not None and isinstance(rec_id, str) and rec_id:
            return rec_id == boot_id
        boot_time = platform.os_boot_time()
        rec_time = rec.get("bootTime")
        if boot_time is not None and isinstance(rec_time, (int, float)):
            return abs(float(rec_time) - boot_time) <= BOOT_TIME_TOLERANCE
        return False

    async def _reboot_marker_covers(self, job: JobConfig) -> bool:
        """Whether the durable marker shows ``job``'s boot run already
        happened on THIS host during THIS OS boot, for THIS job definition.

        Raises whatever the store read raises (bounded); callers map that
        to their own policy.  A marker for a different definition (digest)
        answers ``False``: a redefined @reboot job runs again, mirroring
        the cluster reboot_ran path's job-set scoping.
        """
        backend = self.state_backend
        if backend is None:
            return False
        recs = await asyncio.wait_for(
            backend.list_records(
                self._reboot_stream(job.name),
                limit=REBOOT_STREAM_KEEP,
                newest_first=True,
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        for rec in recs:
            if rec.get("host") != self._state_host:
                continue
            # newest marker from this host decides; older ones are moot.
            if rec.get("jobDigest") != job_digest_cached(job):
                return False
            return self._same_boot(rec)
        return False

    async def _reboot_boot_gate(self, job: JobConfig) -> bool:
        """Record-then-run boot dedupe for a non-deferred @reboot job.

        True -> launch, with the marker recorded FIRST so a crash between
        record and spawn errs toward not re-running (at-most-once).
        False -> skip: the marker proves this boot ran, or the store is
        unavailable under fail-closed. The default degrade policy runs
        the job on store trouble (at-least-once). A store TIMEOUT latches
        _reboot_gate_sick so the remaining @reboot jobs apply the policy
        without serially stalling on a hung mount.
        """
        backend = self.state_backend
        fail_closed = self._state_on_unavailable == "fail-closed"
        if backend is None or self._reboot_gate_sick:
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: the state store is "
                    "unavailable and onStoreUnavailable is fail-closed",
                    job.name,
                )
                return False
            if self._reboot_gate_sick:
                logger.warning(
                    "state: store unhealthy; running @reboot job %s "
                    "without boot-marker dedupe",
                    job.name,
                )
            return True
        try:
            covered = await self._reboot_marker_covers(job)
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            if isinstance(ex, asyncio.TimeoutError):
                self._reboot_gate_sick = True
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: cannot read its boot marker "
                    "(%s) and onStoreUnavailable is fail-closed",
                    job.name,
                    ex,
                )
                return False
            logger.warning(
                "state: cannot read the @reboot marker for %s (%s); "
                "running it (may repeat a boot run)",
                job.name,
                ex,
            )
            covered = False
        if covered:
            logger.info(
                "Job %s (@reboot) already ran during this OS boot; "
                "skipping (state-backed dedupe)",
                job.name,
            )
            return False
        record = {
            "host": self._state_host,
            "bootId": platform.os_boot_id(),
            "bootTime": platform.os_boot_time(),
            "jobDigest": job_digest_cached(job),
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._reboot_stream(job.name)
        try:
            # prune_keep rides the append's worker call, applied only
            # after the append LANDED, so a prune failure can never
            # re-decide the launch.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=REBOOT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            self.metrics.state_write_dropped("reboot-marker")
            if isinstance(ex, asyncio.TimeoutError):
                self._reboot_gate_sick = True
                if fail_closed:
                    # A timed-out append is NOT a failed append: the
                    # worker thread can still land the marker later, and
                    # skipping now would lose the boot run for this whole
                    # boot. Re-check once: marker visible -> launch;
                    # still absent -> skip under the policy.
                    try:
                        if await self._reboot_marker_covers(job):
                            return True
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001 - stays unknown
                        pass
            if fail_closed:
                logger.warning(
                    "Job %s (@reboot) skipped: cannot record its boot "
                    "marker (%s) and onStoreUnavailable is fail-closed "
                    "(if the write lands late, this boot's run is lost)",
                    job.name,
                    ex,
                )
                return False
            logger.warning(
                "state: cannot record the @reboot marker for %s (%s); "
                "running it anyway (may re-run after a daemon restart)",
                job.name,
                ex,
            )
            return True
        # the marker landed: the boot run is committed to happen.
        return True

    async def _process_pending_reboots(self) -> None:
        """Run each deferred @reboot job once the cluster has elected an owner.

        A pending job retires only two ways: this node runs it as the
        elected owner, or reboot_ran gossip positively confirms it ran
        elsewhere. Never dropped because another node merely LOOKS like
        the owner (never-lose). A name transiently absent from cron_jobs
        stays pending; the launch is gated on presence and on still being
        a deferrable @reboot, and always runs the CURRENT cron_jobs[name].
        A PAUSED job keeps its entry untouched on every branch: a pause
        defers the boot run, so neither the mark_reboot_ran token nor the
        entry is spent on a run the pause gate would skip.
        """
        if not self._pending_reboot_jobs:
            return
        if not self._elect_leader_configured:
            # election removed on reload: gating is gone, so run the
            # CURRENT job for any present name still defining an @reboot
            # one-shot; absent names stay pending (never-lose), reused
            # names retire without running.
            for name in list(self._pending_reboot_jobs):
                job = self.cron_jobs.get(name)
                if job is None:
                    continue  # transiently absent -> keep pending, re-check
                if self._pause_active(name) is not None:
                    # a pause DEFERS the boot run: keep it pending rather
                    # than retiring it unrun, and re-check next wakeup.
                    continue
                del self._pending_reboot_jobs[name]
                # a disabled job retires without running, mirroring
                # job_should_run; enabled is checked last so a name reused
                # for a non-@reboot job short-circuits on the schedule
                # check.
                if (
                    isinstance(job.schedule, str)
                    and job.schedule == "@reboot"
                    and job.enabled
                ):
                    await self.launch_scheduled_job(job)
            return
        mgr = self.cluster_manager
        if mgr is None:
            # Election wanted but no manager. Leader one-shots stay
            # fail-closed (pending). PreferLeader is NEVER-SKIP: it must
            # run even with the store unreachable (accepting a possible
            # double-run), the same asymmetry _cluster_allows applies to
            # scheduled PreferLeader jobs in this mgr-is-None case.
            for name in list(self._pending_reboot_jobs):
                if name not in self.cron_jobs:
                    continue  # transiently absent -> keep pending (never-lose)
                job = self.cron_jobs[name]
                if not self._is_deferrable_reboot(job) or not job.enabled:
                    # reused or disabled -> retire without running.
                    del self._pending_reboot_jobs[name]
                    continue
                if self._pause_active(name) is not None:
                    continue  # deferred by the pause; still owed
                if job.clusterPolicy == "PreferLeader":
                    del self._pending_reboot_jobs[name]
                    logger.info(
                        "cluster: running deferred @reboot PreferLeader job "
                        "%s (no leadership manager; never-skip semantics)",
                        name,
                    )
                    await self.launch_scheduled_job(job)
                # Leader one-shots: keep pending, fail closed, re-check next
                # wakeup once a manager is available.
            return
        for name in list(self._pending_reboot_jobs):
            if name not in self.cron_jobs:
                # transiently absent mid-reload: keep pending (never-lose);
                # the launch below is gated on presence, so a genuinely
                # removed job never runs.
                continue
            job = self.cron_jobs[name]
            if not self._is_deferrable_reboot(job) or not job.enabled:
                # reused for a non-deferrable job, or disabled: retire the
                # stale entry without running it.
                del self._pending_reboot_jobs[name]
                continue
            try:
                already_ran = mgr.reboot_ran(name)
            except Exception:
                # A backend read must not escape: this runs OUTSIDE run()'s
                # try/except, so a raise would kill the scheduler. Treat as
                # "not known to have run", keep pending (never-lose).
                logger.exception(
                    "cluster: error checking whether @reboot job %s already "
                    "ran; keeping it pending",
                    name,
                )
                continue
            if already_ran:
                # positive confirmation -> retire without re-running.
                del self._pending_reboot_jobs[name]
                logger.info(
                    "cluster: deferred @reboot job %s already ran in the "
                    "cluster; standing down here",
                    name,
                )
                continue
            if self._pause_active(name) is not None:
                # a pause DEFERS the boot run: keep pending; do not burn
                # the once-per-boot token on a run the pause gate skips.
                continue
            # Gate on the SAME boolean owner check as a scheduled job
            # (_cluster_allows), not a name comparison: leader_name()
            # reports a display identity that may differ from node_name,
            # so comparing names could make the holder fail to recognise
            # itself and never run the one-shot on any node.
            if self._cluster_allows(job):
                del self._pending_reboot_jobs[name]
                logger.info(
                    "cluster: running deferred @reboot job %s (this node is "
                    "the elected owner)",
                    name,
                )
                # Record intent-to-run BEFORE spawning: worst case is a
                # recorded-but-not-run one-shot (at-most-once preserved),
                # not a double-run after failover.
                await mgr.mark_reboot_ran(name)
                await self.launch_scheduled_job(job)
            # else: another node owns it or the cluster has not converged
            # -> keep pending. Never drop a one-shot on another node's
            # behalf.

    def _cluster_allows(self, job: JobConfig) -> bool:
        """Whether this node may run *scheduled* ``job`` this cycle.

        Always true without electLeader. EveryNode runs everywhere;
        Leader runs only on the quorum-gated elected owner (at-most-once,
        skips without quorum); PreferLeader runs on the reachable
        agreeing owner ignoring quorum (never skips, may double-run
        across a partition). Under distribution: spread the owner is
        per-job rendezvous-hashed. With no manager running, Leader fails
        CLOSED and PreferLeader runs anyway. Manual triggers are
        deliberately NOT gated; retries re-check this gate in
        schedule_retry_job (see _cluster_owner_moved). A detected
        conflict (has_conflict: duplicate nodeName, differing N, or
        differing coordination policy) also fails Leader closed;
        PreferLeader keeps running.
        """
        if not self._elect_leader_configured:
            return True
        if job.clusterPolicy == "EveryNode":
            return True
        mgr = self.cluster_manager
        if mgr is None:
            # Election is configured but no manager is running (it failed to
            # start, or a reload tore the old one down and the rebuild raised).
            # That is precisely the "store/quorum unreachable" condition, so
            # honour each policy's contract: Leader fails CLOSED
            # (at-most-once), but PreferLeader is never-skip; it must run
            # anyway (accepting a possible double-run) rather than be silently
            # skipped on every replica, which for a fleet-wide start failure
            # would drop the job to at-most-ZERO, defeating the whole point of
            # PreferLeader.
            return bool(job.clusterPolicy == "PreferLeader")
        try:
            if mgr.distribution == "spread":
                if job.clusterPolicy == "PreferLeader":
                    return mgr.is_available_job_owner(job.name)
                if mgr.has_conflict():
                    return False  # "Leader": fail closed on duplicate nodeName
                return mgr.is_job_owner(job.name)  # "Leader"
            if job.clusterPolicy == "PreferLeader":
                return mgr.is_available_leader()
            if mgr.has_conflict():
                return False  # "Leader": fail closed on a duplicate nodeName
            return mgr.is_leader()  # "Leader"
        except Exception:
            # A backend read should never raise, but a bug in one (a bad gossip
            # payload reaching election, a KeyError in rendezvous hashing) must
            # not escape: spawn_jobs runs OUTSIDE the run loop's try/except, so
            # an exception here would kill the whole scheduler, including the
            # EveryNode jobs meant to survive a broken manager. Fail closed
            # (skip this leader-gated job) and keep scheduling.
            logger.exception(
                "cluster: error evaluating the leader gate for job %s; "
                "failing closed (skipping it this cycle)",
                job.name,
            )
            return False

    def _log_cluster_role(self) -> None:
        """Log this node's run-eligibility transitions (once per change).

        Quorum membership is logged in both modes, so any node, leader or
        follower, records losing (and regaining) quorum, the gate that
        decides whether the cluster can run leader-gated work at all.
        Single-leader mode additionally logs this node acquiring or losing the
        one scheduled-job leadership.
        """
        if not self._elect_leader_configured:
            return
        # Runs OUTSIDE run()'s try/except, so a backend-read exception
        # here would kill the scheduler. It only logs, so swallow errors
        # and keep scheduling.
        try:
            self._emit_cluster_role_logs()
        except Exception:
            logger.exception(
                "cluster: error while logging cluster role; continuing"
            )

    def _emit_cluster_role_logs(self) -> None:
        mgr = self.cluster_manager
        if mgr is not None:
            # Duplicate nodeName pauses Leader jobs cluster-wide; log the
            # edge (and recovery) once per transition.
            conflict = mgr.conflict_names()
            if bool(conflict) != self._was_conflict:
                if conflict:
                    logger.error(
                        "cluster: duplicate nodeName detected (%s) -- Leader "
                        "jobs will stand down until every node has a unique "
                        "cluster.nodeName",
                        ", ".join(conflict),
                    )
                else:
                    logger.info(
                        "cluster: nodeName conflict resolved; Leader jobs may "
                        "run again"
                    )
                self._was_conflict = bool(conflict)
            # A cluster-size disagreement breaks the quorum proof the same
            # way; log it just as loudly.
            size_conflict = mgr.conflicting_sizes()
            if bool(size_conflict) != self._was_size_conflict:
                if size_conflict:
                    logger.error(
                        "cluster: cluster-size disagreement -- agreeing peers "
                        "declare %s but we declare %d; Leader jobs will stand "
                        "down until every node's cluster.peers agree on the "
                        "member set",
                        ", ".join(str(s) for s in size_conflict),
                        mgr.cluster_size(),
                    )
                else:
                    logger.info(
                        "cluster: cluster-size disagreement resolved; Leader "
                        "jobs may run again"
                    )
                self._was_size_conflict = bool(size_conflict)
            # A coordination-policy divergence also fails the Leader gate
            # closed but would otherwise leave nothing in the log; surface
            # it just as loudly, once per change.
            policy_conflict = mgr.conflicting_policies()
            if bool(policy_conflict) != self._was_policy_conflict:
                if policy_conflict:
                    logger.error(
                        "cluster: coordination-policy divergence -- agreeing "
                        "peers declare %s; Leader jobs will stand down until "
                        "every node's cluster.distribution and "
                        "cluster.electLeader agree",
                        "; ".join(policy_conflict),
                    )
                else:
                    logger.info(
                        "cluster: coordination-policy divergence resolved; "
                        "Leader jobs may run again"
                    )
                self._was_policy_conflict = bool(policy_conflict)
        # Quorum membership is logged on EVERY node: without this a
        # follower logs nothing on quorum loss (only the ex-leader's
        # is_leader() flips in single-leader mode).
        spread = mgr is not None and mgr.distribution == "spread"
        quorate = mgr is not None and mgr.is_quorate()
        if quorate != self._was_quorate:
            self.metrics.cluster_quorum_transition()
            if spread and quorate:
                logger.info(
                    "cluster: this node joined quorum; "
                    "per-job ownership active"
                )
            elif spread:
                logger.info(
                    "cluster: this node left quorum; per-job ownership "
                    "suspended"
                )
            elif quorate:
                logger.info("cluster: this node joined quorum")
            else:
                logger.info(
                    "cluster: this node left quorum; no majority reachable, "
                    "so Leader jobs cannot run until one is"
                )
            if not quorate and self._notify_config is not None:
                # quorum loss is alert-worthy (regain is logged, not
                # paged). Guarded so an unconfigured daemon builds no
                # payload.
                node = getattr(mgr, "node_name", None) or self._state_host
                self._dispatch_notify(
                    "quorum_loss",
                    success=False,
                    name=node,
                    subject="node {} left quorum".format(node),
                    message=(
                        "No majority reachable; Leader jobs cannot run on "
                        "this node until quorum is restored."
                    ),
                    quorate=False,
                )
            self._was_quorate = quorate
        if spread:
            return  # no single leader in spread mode
        leader = mgr is not None and mgr.is_leader()
        if leader != self._was_leader:
            self.metrics.cluster_leader_transition()
            logger.info(
                "cluster: this node %s scheduled-job leadership",
                "acquired" if leader else "lost",
            )
            # Guarded so an unconfigured daemon builds no payload.
            if self._notify_config is not None:
                node = getattr(mgr, "node_name", None) or self._state_host
                self._dispatch_notify(
                    "leader_change",
                    success=False,
                    name=node,
                    subject="node {} {} scheduled-job leadership".format(
                        node, "acquired" if leader else "lost"
                    ),
                    message=(
                        "This node is now the scheduled-job leader."
                        if leader
                        else "This node is no longer the scheduled-job leader."
                    ),
                    role="leader" if leader else "follower",
                    is_leader=leader,
                    leader=mgr.leader_name() if mgr is not None else None,
                )
            self._was_leader = leader

    @staticmethod
    def job_should_run(
        startup: bool,
        job: JobConfig,
        slot: Optional[datetime.datetime] = None,
    ) -> bool:
        if not job.enabled:
            logger.debug(
                "Job %s (%s) is disabled in the config",
                job.name,
                job.schedule_unparsed,
            )
            return False
        if startup:
            if isinstance(job.schedule, str) and job.schedule == "@reboot":
                logger.debug(
                    "Job %s (%s) is scheduled for startup (@reboot)",
                    job.name,
                    job.schedule_unparsed,
                )
                return True
            else:
                return False
        if isinstance(job.schedule, CronTab):
            crontab: CronTab = job.schedule
            # schedule_slot truncates to the job's resolution. `slot` is
            # the pass instant precomputed by spawn_jobs (due-test and
            # de-dup key are the same read); None means a fresh read.
            if slot is None:
                slot = schedule_slot(job)
            if crontab.test(slot):
                logger.debug(
                    "Job %s (%s) is scheduled for now",
                    job.name,
                    job.schedule_unparsed,
                )
                return True
            else:
                logger.debug(
                    "Job %s (%s) not scheduled for now",
                    job.name,
                    job.schedule_unparsed,
                )
                return False
        else:
            return False

    async def launch_scheduled_job(self, job: JobConfig) -> None:
        # The pause gate for scheduled fires (manual start and catch-up
        # use maybe_launch_job directly). The synthetic "skipped" ledger
        # row keeps the catch-up watermark advancing across the pause; it
        # carries no ranAt and never enters _last_completed_at, so the
        # retry ladder's superseded-by-run guards cannot mistake a held
        # slot for a run.
        #
        # @reboot jobs are EXEMPT: they only arrive here from the
        # pause-aware reboot callers, which already spent the
        # once-per-boot token across an await. Skipping on a pause that
        # lands in that window would FORFEIT the boot run instead of
        # deferring it. Safe because no scheduled fire can arrive here
        # for an @reboot job.
        pause = self._pause_active(job.name)
        is_reboot = isinstance(job.schedule, str) and job.schedule == "@reboot"
        if pause is not None and not is_reboot:
            logger.info(
                "Job %s skipped: paused until %s%s",
                job.name,
                pause.until.isoformat(),
                " ({})".format(pause.note) if pause.note else "",
            )
            output = JobOutputStream()
            output.closed = True
            self._record_run(
                job.name,
                JobRunInfo(
                    outcome="skipped",
                    exit_code=None,
                    started_at=None,
                    finished_at=get_now(datetime.timezone.utc),
                    fail_reason=None,
                    output=output,
                    skip_reason="paused",
                ),
            )
            return
        if not await self._depends_on_past_ok(job):
            logger.info(
                "Job %s skipped: onlyIfLastSucceeded and its last run did "
                "not succeed",
                job.name,
            )
            return
        await self.cancel_job_retries(job.name)
        assert job.name not in self.retry_state

        retry = job.onFailure["retry"]
        logger.debug("Job %s retry config: %s", job.name, retry)
        if retry["maximumRetries"]:
            retry_state = JobRetryState(
                retry["initialDelay"],
                retry["backoffMultiplier"],
                retry["maximumDelay"],
            )
            self.retry_state[job.name] = retry_state

        await self.maybe_launch_job(job)

    async def maybe_launch_job(
        self, job: JobConfig, *, with_retries: bool = True
    ) -> bool:
        """Launch ``job`` unless concurrencyPolicy forbids it.

        Returns whether a new instance was launched (False only for the
        Forbid skip). with_retries=False (catch-up backfills) launches
        WITHOUT the retry state: a backfill must not attach to a live
        retry ladder and burn its budget. The whole method holds the
        per-job launch lock: the concurrency gate reads running_jobs
        several awaits before the launch appends to it, so two concurrent
        entries for the same job would otherwise double-launch a Forbid
        job. Distinct jobs still launch concurrently.
        """
        async with self._launch_locks[job.name]:
            return await self._launch_job_locked(job, with_retries)

    async def _launch_job_locked(
        self, job: JobConfig, with_retries: bool
    ) -> bool:
        """The body of :meth:`maybe_launch_job`, under its per-job lock."""
        # .get(), not a bare subscript: subscripting this defaultdict
        # would INSERT a phantom empty-list key, which makes running_jobs
        # truthy with nothing to reap and spins the reaper hot at
        # shutdown.
        if self.running_jobs.get(job.name):
            logger.warning(
                "Job %s: still running and concurrencyPolicy is %s",
                job.name,
                job.concurrencyPolicy,
            )
            if job.concurrencyPolicy == "Allow":
                pass
            elif job.concurrencyPolicy == "Forbid":
                # the slot is deliberately DROPPED, not late (an overrun
                # is maxRuntime's to report). Pop the due slot so the
                # stale due cannot latch lateAfter in the window between
                # the run finishing and the next slot launching; mirrors
                # the cluster-slot path's _sla_peer_owns_slot excuse.
                self._sla_due.pop(job.name, None)
                return False
            elif job.concurrencyPolicy == "Replace":
                # over a SNAPSHOT: the reaper concurrently remove()s from
                # the live list; shrinking it mid-iteration would skip an
                # instance, leaving it running beside the replacement.
                for running_job in list(self.running_jobs[job.name]):
                    # mark before cancelling so the reaper treats the forced
                    # termination as a replacement, not a job failure.
                    running_job.replaced = True
                    await running_job.cancel()
            else:
                raise AssertionError  # pragma: no cover
        if job.concurrencyScope == "cluster":
            # cluster-wide half of Forbid/Replace: a TTL slot lease on the
            # shared store excludes instances on OTHER nodes. Bounded; a
            # foreign Replace holder is pursued by a background task,
            # never waited out on the scheduler path.
            if not await self._claim_cluster_slot(job):
                return False
        logger.info("Starting job %s", job.name)
        retry_state = self.retry_state.get(job.name) if with_retries else None
        # register with the loopback state API BEFORE the child launches,
        # so the child's first callback is already authorised.
        run_token, extra_env = await self._prepare_job_api_run(
            job, retry_state
        )
        running_job = RunningJob(
            job,
            retry_state,
            extra_env=extra_env,
            state_token=run_token,
            run_id=extra_env.get("CRONSTABLE_RUN_ID"),
        )
        try:
            # the gate releases before the except arm runs, so cleanup
            # below never holds a spawn permit.
            async with self._spawn_gate:
                await running_job.start()
        except BaseException:
            # start() handles expected spawn failures itself; anything
            # escaping here never registers, so the slot claim must be
            # handed back or its refcount (and renew task) would outlive
            # the launch forever.
            if job.concurrencyScope == "cluster":
                await self._release_cluster_slot(job)
            # likewise the job-API registration: a launch that never
            # registers is never reaped, so drop its token/secrets here.
            if run_token is not None and self._job_api is not None:
                await self._job_api.finish_run(run_token)
            raise
        first_instance = self._add_running_instance(running_job)
        # every actual launch (scheduled, manual, catch-up, retry) clears
        # the lateAfter breach condition (see _sla_periodic).
        self._sla_last_start[job.name] = get_now(datetime.timezone.utc)
        if self.state_backend is not None and first_instance:
            # record the run as in-flight (0 -> 1 instances) so a crash
            # leaves an "open" record for reconciliation; closed again when
            # the LAST instance finishes (see _handle_finished_job). Ordered
            # via the per-job inflight tail so the close cannot sort ahead.
            self._queue_inflight_write(
                job.name,
                lambda: self._persist_inflight_open(job, running_job),
            )
        logger.info("Job %s spawned", job.name)
        self._jobs_running.set()
        return True

    # --- cluster-wide concurrency slots (concurrencyScope: cluster) -------

    def _slot_holder(self) -> str:
        """The slot lease holder string: host plus a per-process token.

        Process-unique so a restarted daemon (or a second daemon on this
        host) can never adopt the other's slot; the host prefix is display
        only and never compared for gating.
        """
        return "{}#{}".format(self._state_host, self._proc_token)

    async def _prepare_job_api_run(
        self, job: JobConfig, retry_state: Optional[JobRetryState]
    ) -> tuple[Optional[str], dict[str, str]]:
        """Register this run with the loopback state API; return its env.

        Mints the run id + token, stages the job's secrets, registers the
        RunContext, and returns (token, injected_env). (None, {}) when no
        job API is running. Secret staging lives in jobapi.stage_secrets.
        """
        api = self._job_api
        if api is None or api.base_url is None:
            return None, {}
        from cronstable.jobapi import (
            RunContext,
            run_environment,
            stage_secrets,
        )

        secrets = await stage_secrets(job.secrets, "job {}".format(job.name))
        slot = self._last_run_slot.get(job.name)
        ctx = RunContext(
            token=os.urandom(32).hex(),
            run_id=os.urandom(16).hex(),
            job_name=job.name,
            attempt=retry_state.count if retry_state is not None else 0,
            scheduled_at=slot.isoformat() if slot is not None else None,
            host=self._state_host,
            default_scope=job.name,
            allowed_scopes=set(job.stateAllowedScopes),
            secrets=secrets,
        )
        api.register_run(ctx)
        return ctx.token, run_environment(ctx, api.base_url, api.cacert)

    @staticmethod
    def _slot_name(name: str) -> str:
        """Both the slot LEASE name and the cancel-record stream name."""
        return SLOT_STREAM_PREFIX + name

    def _slot_mutex(self, name: str) -> asyncio.Lock:
        return self._slot_locks.setdefault(name, asyncio.Lock())

    async def _slot_fidelity_reason(self) -> Optional[str]:
        """The reason the store's locks cannot fence, or ``None`` (they can).

        Probed once per backend generation (see
        :meth:`cronstable.state.FilesystemStateBackend.verify_locking`) and
        latched; a probe that cannot run right now latches nothing and is
        retried on the next claim.
        """
        backend = self.state_backend
        if backend is None:
            return None
        if self._slot_fidelity is None:
            try:
                reason = await asyncio.wait_for(
                    backend.verify_locking(), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - inconclusive; retry later
                return None
            self._slot_fidelity = reason or ""
            if reason:
                logger.error(
                    "state: the store's file locks cannot be trusted for "
                    "cluster-wide concurrency (%s); concurrencyScope: "
                    "cluster claims degrade per onStoreUnavailable",
                    reason,
                )
        return self._slot_fidelity or None

    async def _acquire_slot_lease(
        self, backend: StateBackend, lease_name: str
    ) -> Optional[Lease]:
        """``acquire_lease`` for a cluster slot; timeout or store error
        maps to ``None`` so the caller's read-back-and-policy path
        decides. Never raises (bar cancellation): an escaped store error
        here would terminate the whole scheduler loop.
        """
        try:
            return await asyncio.wait_for(
                backend.acquire_lease(
                    lease_name, self._slot_holder(), self._slot_ttl
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a raised store error is as
            # ambiguous as a timeout; fail closed via the read-back path
            # rather than letting it escape and crash the loop.
            return None

    async def _claim_cluster_slot(self, job: JobConfig) -> bool:
        """Claim the cluster-wide concurrency slot for one launch of ``job``.

        True means launch (holding the lease, or degraded to node-local
        per onStoreUnavailable); False means skipped (Forbid: foreign
        holder; Replace: background pursuit re-attempts; fail-closed:
        store did not answer). Runs under a per-job lock, serialized
        against the finish-path release, which could otherwise revoke a
        fresh claim's lease. Honesty contract: at-least-once, not
        exactly-once; a holder that loses its lease to a store outage
        keeps running, and degrade trades the cluster gate for
        availability.
        """
        backend = self.state_backend
        if not self._state_configured:
            # unreachable via the parse-time cross-check; bypassing test
            # configs fall back to node-local enforcement.
            return True
        fail_closed = self._state_on_unavailable == "fail-closed"
        name = job.name

        def _unavailable(why: str) -> bool:
            if fail_closed:
                logger.warning(
                    "Job %s skipped: cannot claim its cluster concurrency "
                    "slot (%s) and onStoreUnavailable is fail-closed",
                    name,
                    why,
                )
                return False
            logger.warning(
                "Job %s: cannot claim its cluster concurrency slot (%s); "
                "enforcing concurrencyPolicy on this node only for this "
                "run (onStoreUnavailable: degrade)",
                name,
                why,
            )
            self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
            return True

        if backend is None:
            return _unavailable("the state store is unavailable")
        fidelity = await self._slot_fidelity_reason()
        if fidelity is not None:
            return _unavailable(fidelity)
        async with self._slot_mutex(name):
            held = self._slot_leases.get(name)
            renewer = self._slot_renewers.get(name)
            if held is not None and renewer is not None and not renewer.done():
                # already holding (a Replace re-launch, or Allow-scoped
                # overlap after a reload): adopt the live lease.
                self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
                return True
            lease_name = self._slot_name(name)
            got = await self._acquire_slot_lease(backend, lease_name)
            if got is None:
                # denied, sick, or timed out: a bounded read tells a
                # foreign holder apart from a store that cannot answer
                # (the lease API fails closed; None alone proves nothing).
                observed: Optional[Lease] = None
                answered = False
                try:
                    observed = await asyncio.wait_for(
                        backend.read_lease(lease_name),
                        timeout=STATE_OP_TIMEOUT,
                    )
                    answered = observed is not None
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - a raised store error here
                    # is as ambiguous as a timeout: leave answered=False so the
                    # "store did not answer" branch below returns _unavailable
                    # (fail closed) instead of letting it crash the loop.
                    pass
                if observed is not None:
                    if observed.holder == self._slot_holder():
                        # our own acquire landed after its timeout was
                        # abandoned (the documented UNKNOWN case): adopt it.
                        got = observed
                    elif (
                        get_now(datetime.timezone.utc).timestamp()
                        > observed.expires_at
                    ):
                        # expired but unreclaimed: treat as unanswered and
                        # let the policy decide (the next attempt acquires).
                        answered = False
                        observed = None
                    elif job.concurrencyPolicy == "Forbid":
                        logger.warning(
                            "Job %s skipped: its cluster concurrency slot "
                            "is held by %s (concurrencyPolicy: Forbid, "
                            "concurrencyScope: cluster)",
                            name,
                            observed.holder.rsplit("#", 1)[0],
                        )
                        self._sla_peer_owns_slot(name)
                        return False
                    else:  # Replace
                        self._spawn_slot_pursuit(job, observed)
                        self._sla_peer_owns_slot(name)
                        return False
                if got is None and not answered:
                    return _unavailable("the store did not answer")
            if got is None:  # pragma: no cover - defensive; handled above
                return _unavailable("the store did not answer")
            self._slot_leases[name] = got
            self._slot_refs[name] = self._slot_refs.get(name, 0) + 1
            if renewer is not None and not renewer.done():
                renewer.cancel()
            self._slot_renewers[name] = asyncio.create_task(
                self._slot_renewer(name)
            )
            # a fresh slot win is the one moment a foreign orphaned run is
            # provably unrenewed: reconcile its in-flight record (does not
            # prove the process died; see _reconcile_open_record).
            await self._reconcile_takeover_inflight(job)
            return True

    def _spawn_slot_pursuit(self, job: JobConfig, observed: Lease) -> None:
        """Start (or keep) the background Replace pursuit for ``job``.

        The pursuit (asking the foreign holder to yield, waiting it out,
        then re-attempting the launch) takes up to ~2 slot TTLs, so it
        must never run inline on the scheduler pass (one held slot would
        stall every other due job); single-flight per job.
        """
        name = job.name
        existing = self._slot_pursuits.get(name)
        if existing is not None and not existing.done():
            return
        logger.info(
            "Job %s: cluster Replace: asking the current slot holder (%s) "
            "to yield; the launch is re-attempted when the slot frees",
            name,
            observed.holder.rsplit("#", 1)[0],
        )
        task = asyncio.create_task(self._pursue_replace_slot(job, observed))
        self._slot_pursuits[name] = task

        def _clear(done: asyncio.Task) -> None:
            if self._slot_pursuits.get(name) is done:
                del self._slot_pursuits[name]

        task.add_done_callback(_clear)

    async def _pursue_replace_slot(
        self, job: JobConfig, observed: Lease
    ) -> None:
        """Ask a foreign slot holder to yield, wait, then re-attempt.

        The cancel record targets the holder's exact FENCE, so a stale
        request from a previous incarnation is inert. The holder's renew
        task observes it within one renew period; the re-launch goes back
        through every normal gate. Bounded: a holder that never yields
        forfeits this launch (no-run over double-run).
        """
        backend = self.state_backend
        name = job.name
        if backend is None:
            return
        cancel = {
            "kind": "cancel",
            "fence": observed.fence,
            "by": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._slot_name(name)
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, cancel, prune_keep=SLOT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - give up, log, no launch
            logger.warning(
                "Job %s: could not record the cluster Replace cancel "
                "request: %s",
                name,
                ex,
            )
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2 * self._slot_ttl
        poll = max(1.0, self._slot_ttl / 6)
        while True:
            if self._stop_event.is_set():
                return
            await asyncio.sleep(poll)
            current: Optional[Lease] = observed
            try:
                current = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep waiting
                pass
            now = get_now(datetime.timezone.utc).timestamp()
            if (
                current is None
                or now > current.expires_at
                or current.holder == self._slot_holder()
            ):
                break
            if loop.time() >= deadline:
                logger.warning(
                    "Job %s: the foreign holder (%s) did not yield its "
                    "cluster concurrency slot within %.0fs; skipping this "
                    "launch (no-run over double-run)",
                    name,
                    current.holder.rsplit("#", 1)[0],
                    2 * self._slot_ttl,
                )
                return
        if await self.maybe_launch_job(job):
            logger.info(
                "Job %s: launched after the previous cluster slot holder "
                "yielded (concurrencyPolicy: Replace)",
                name,
            )

    async def _slot_renewer(self, name: str) -> None:
        """Keep a held slot lease alive while the job runs here.

        Renews at a third of the TTL and doubles as the Replace listener
        (a cancel record for our exact fence cancels local instances,
        marked replaced). A positively refused renew stops renewing but
        NEVER cancels the running work; an ambiguous refusal keeps
        retrying (same-fence renew slightly past expiry is allowed, so a
        single blip self-heals).
        """
        period = max(1.0, self._slot_ttl / 3)
        me = asyncio.current_task()
        while True:
            await asyncio.sleep(period)
            backend = self.state_backend
            lease = self._slot_leases.get(name)
            if backend is None or lease is None:
                return
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._slot_name(name), limit=1, newest_first=True
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the listener is best-effort
                recs = []
            rec = recs[0] if recs else None
            if (
                rec is not None
                and rec.get("kind") == "cancel"
                and rec.get("fence") == lease.fence
                and self.running_jobs.get(name)
            ):
                logger.info(
                    "Job %s: node %s requested this instance be replaced "
                    "(concurrencyPolicy: Replace, concurrencyScope: "
                    "cluster); cancelling",
                    name,
                    rec.get("by"),
                )
                for running_job in list(self.running_jobs.get(name) or []):
                    running_job.replaced = True
                    await running_job.cancel()
                # the finish path releases the slot; keep renewing till then
            try:
                renewed = await asyncio.wait_for(
                    backend.renew_lease(lease, self._slot_ttl),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue  # unknown; retry next period
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a RAISED store error is as
                # ambiguous as a timeout and must NOT kill the renewer: a
                # dead renewer lets the lease expire under a live holder
                # and a standby double-fires the fenced job. Retry.
                logger.warning(
                    "Job %s: cluster concurrency slot renewal errored; "
                    "will retry next period",
                    name,
                )
                continue
            if self._slot_renewers.get(name) is not me:
                # Retired mid-renew (release or re-acquire replaced us).
                # A cancel racing this point does NOT reliably raise: on
                # Python <=3.11 asyncio.wait_for returns the resolved
                # result instead of propagating CancelledError. Stand down
                # WITHOUT touching _slot_leases: re-populating the entry
                # the release just popped would leak the slot a whole TTL,
                # and the takeover branch below could pop a genuine
                # re-claim's lease. The finish path owns the release.
                return
            if renewed is not None:
                self._slot_leases[name] = renewed
                continue
            observed: Optional[Lease] = None
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - ambiguous; retry
                continue
            if observed is not None and (
                observed.holder != lease.holder
                or observed.fence != lease.fence
            ):
                logger.warning(
                    "Job %s: its cluster concurrency slot was taken over "
                    "by %s while it is still running here (a store outage "
                    "outlasted the slot TTL?); the run continues -- the "
                    "overlap is the documented at-least-once trade",
                    name,
                    observed.holder.rsplit("#", 1)[0],
                )
                self._slot_leases.pop(name, None)
                return
            # our lease on disk (blip) or unreadable: keep trying.

    async def _release_cluster_slot(self, job: JobConfig) -> None:
        """Hand back the slot when a cluster-scoped job's last user is done.

        Refcounted (_slot_refs): released only at zero AND with no
        registered instances, so an overlap or mid-spawn claim cannot
        lose its lease to a stale release. Fire-and-forget; with no
        recorded lease a phantom check releases any lease held by THIS
        process so a phantom cannot block other nodes for a whole TTL.
        """
        name = job.name
        async with self._slot_mutex(name):
            refs = self._slot_refs.get(name, 0) - 1
            if refs > 0:
                self._slot_refs[name] = refs
                return
            self._slot_refs.pop(name, None)
            if self.running_jobs.get(name):
                return
            renewer = self._slot_renewers.pop(name, None)
            if renewer is not None and not renewer.done():
                renewer.cancel()
            lease = self._slot_leases.pop(name, None)
            if lease is not None:
                self._track_state_write(self._release_slot_lease(name, lease))
            else:
                self._track_state_write(self._release_phantom_slot(name))

    async def _release_slot_lease(self, name: str, lease: Lease) -> None:
        backend = self.state_backend
        if backend is None:
            return
        # Serialized under the per-job slot mutex: a same-holder
        # re-acquire KEEPS the fence, so this stale fire-and-forget
        # release could otherwise revoke a fresh claim's lease. Once
        # _slot_leases[name] is present (installed under this mutex) this
        # release is stale by definition and stands down.
        async with self._slot_mutex(name):
            if self._slot_leases.get(name) is not None:
                return  # a fresh claim adopted the on-disk lease; keep it
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - TTL is the fallback
                logger.warning(
                    "state: failed to release the concurrency slot for %s "
                    "(%s); it frees by TTL",
                    name,
                    ex,
                )

    async def _release_phantom_slot(self, name: str) -> None:
        backend = self.state_backend
        if backend is None:
            return
        # Serialized under the per-job slot mutex: the phantom check
        # matches only on the process-wide holder string, so without the
        # mutex it could release a fresh claim's live lease out from
        # under a run. Once _slot_leases[name] is present this cleanup is
        # a no-op.
        async with self._slot_mutex(name):
            if self._slot_leases.get(name) is not None:
                return  # a live claim owns the slot now; not a phantom
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._slot_name(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
                if (
                    observed is not None
                    and observed.holder == self._slot_holder()
                    and self._slot_leases.get(name) is None
                ):
                    await asyncio.wait_for(
                        backend.release_lease(observed),
                        timeout=STATE_OP_TIMEOUT,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass

    # --- in-flight run records and crash reconciliation -------------------

    @staticmethod
    def _inflight_stream(name: str) -> str:
        return INFLIGHT_STREAM_PREFIX + name

    def _queue_inflight_write(
        self, name: str, make_coro: Callable[[], Coroutine[Any, Any, None]]
    ) -> asyncio.Task:
        """Run an inflight-stream write ordered behind the job's previous one.

        Unordered open/close pairs could land filename-inverted for a
        near-instant run, leaving "open" newest: a spurious interrupted
        run on restart. Takes a factory, not a coroutine, per
        _install_tail_task: a shed write would leave an eager coroutine
        neither awaited nor closed.
        """
        return self._install_tail_task(
            self._inflight_write_tail,
            name,
            make_coro,
            spawn=self._track_state_write,
        )

    async def _persist_inflight_open(
        self, job: JobConfig, running_job: RunningJob
    ) -> None:
        """Record that ``job`` went 0 -> 1 live instances on this node."""
        backend = self.state_backend
        if backend is None:
            return
        proc = getattr(running_job, "proc", None)
        pid = proc.pid if proc is not None else None
        record = {
            "kind": "open",
            "host": self._state_host,
            "proc": self._proc_token,
            "pid": pid,
            "startedAt": get_now(datetime.timezone.utc).isoformat(),
            "jobDigest": job_digest_cached(job),
        }
        stream = self._inflight_stream(job.name)
        try:
            # Bounded: a wedged mount must not hang this tracked task and
            # pile up _pending_state_writes.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=INFLIGHT_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("inflight")
            logger.warning(
                "state: failed to record the in-flight run of %s: %s",
                job.name,
                ex,
            )

    async def _persist_inflight_closed(
        self, name: str, reason: str = "finished"
    ) -> None:
        """Record that ``name`` went 1 -> 0 live instances on this node."""
        backend = self.state_backend
        if backend is None:
            return
        record = {
            "kind": "closed",
            "host": self._state_host,
            "proc": self._proc_token,
            "reason": reason,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        try:
            await asyncio.wait_for(
                backend.append_record(
                    self._inflight_stream(name),
                    record,
                    prune_keep=INFLIGHT_STREAM_KEEP,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("inflight")
            logger.warning(
                "state: failed to close the in-flight record of %s: %s",
                name,
                ex,
            )

    async def _bounded_boot_scan(
        self,
        items: list[tuple[str, JobConfig]],
        step: Callable[[str, JobConfig], Awaitable[Optional[str]]],
        timeout_message: str,
    ) -> int:
        """Run ``step`` over ``items`` behind a small bounded worker pool.

        The one boot-scan pool (history warm-up, in-flight
        reconciliation, retry re-arm). Workers share one iterator
        (next() is synchronous, so items partition race-free). ``step``
        returns "timeout" to abandon the whole pass (timeout_message
        logged once, with the item name interpolated); "counted" adds to
        the returned tally; None counts nothing.
        """
        it = iter(items)
        aborted = False
        counted = 0

        async def _worker() -> None:
            nonlocal aborted, counted
            for name, job in it:
                if aborted:
                    break
                outcome = await step(name, job)
                if outcome == "timeout":
                    if not aborted:
                        aborted = True
                        logger.warning(timeout_message, name)
                    break
                if outcome == "counted":
                    counted += 1

        workers = min(_REHYDRATE_CONCURRENCY, max(1, len(items)))
        await asyncio.gather(*(_worker() for _ in range(workers)))
        return counted

    async def _reconcile_inflight(self) -> None:
        """Close runs the PREVIOUS daemon on this host left in flight.

        Runs once per rehydration (after the ledger warm, before the
        retry re-arm): an ``open`` record from this host whose writing
        process is gone becomes an ``unknown``-outcome ledger row. Three
        guards keep live runs safe: a record by THIS process is skipped,
        live local instances outrank the ledger, and a recorded pid that
        still exists is left alone. Uses the bounded boot-scan pool; safe
        because each per-job step touches only its own stream/keys and
        per-job write order still goes through _queue_inflight_write.
        """
        backend = self.state_backend
        if backend is None:
            return
        await self._bounded_boot_scan(
            list(self.cron_jobs.items()),
            self._reconcile_one_inflight,
            "state: in-flight reconciliation timed out reading %s; "
            "skipping the rest (store unhealthy?)",
        )

    async def _reconcile_one_inflight(
        self, name: str, job: JobConfig
    ) -> Optional[str]:
        """One job's step of the boot reconciliation scan (see the caller).

        Returns ``"timeout"`` when the in-flight read timed out, which tells
        the scan's worker pool to abandon the rest of the pass; ``None`` in
        every other case, reconciled and skipped alike.
        """
        backend = self.state_backend
        if backend is None:
            return None
        if self.running_jobs.get(name):
            return None
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._inflight_stream(name),
                    limit=1,
                    newest_first=True,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return "timeout"
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: cannot read the in-flight record of %s: %s",
                name,
                ex,
            )
            return None
        rec = recs[0] if recs else None
        if rec is None or rec.get("kind") != "open":
            return None
        if rec.get("host") != self._state_host:
            return None  # another node's business (see the slot takeover)
        if rec.get("proc") == self._proc_token:
            return None  # our own live run; the backend was just rebuilt
        pid = rec.get("pid")
        if (
            isinstance(pid, int)
            and not isinstance(pid, bool)
            and platform.pid_alive(pid)
        ):
            logger.warning(
                "Job %s: the previous daemon's run (pid %d) still "
                "appears to be running; leaving its in-flight record "
                "open",
                name,
                pid,
            )
            return None
        self._reconcile_open_record(name, job, rec, "reconciled-crash")
        return None

    async def _reconcile_takeover_inflight(self, job: JobConfig) -> None:
        """On a fresh slot win, close a foreign holder's orphaned run.

        A just-acquired slot proves the previous holder made NO successful
        renewal for a full TTL, not that its process died (it may still
        be running if it lost store access; that overlap is the documented
        at-least-once trade).  The fence supersession is what makes closing
        the record safe: any late write the old incarnation makes is
        detectable against the bumped fence.
        """
        backend = self.state_backend
        if backend is None:
            return
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._inflight_stream(job.name),
                    limit=1,
                    newest_first=True,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - reconciliation is best-effort
            return
        rec = recs[0] if recs else None
        if rec is None or rec.get("kind") != "open":
            return
        if rec.get("host") == self._state_host:
            # same-host orphan: our own live run is never reconciled, and
            # a still-alive recorded pid means the job outlived its daemon
            # (NOT interrupted). Only a foreign host's record is judged
            # purely by fence supersession (its pid names another machine).
            if rec.get("proc") == self._proc_token:
                return  # our own live run
            pid = rec.get("pid")
            if (
                isinstance(pid, int)
                and not isinstance(pid, bool)
                and platform.pid_alive(pid)
            ):
                logger.warning(
                    "Job %s: a previous daemon's run (pid %d) on this host "
                    "still appears to be running; leaving its in-flight "
                    "record open on the slot takeover",
                    job.name,
                    pid,
                )
                return
        self._reconcile_open_record(job.name, job, rec, "reconciled-takeover")

    def _reconcile_open_record(
        self,
        name: str,
        job: Optional[JobConfig],
        rec: dict[str, Any],
        reason: str,
    ) -> None:
        """Close an orphaned ``open`` record and make the run visible.

        Appends a ``closed`` record and a synthetic ``unknown``-outcome
        ledger row (a non-verdict everywhere; no started_at, so no skewed
        durations). Policy-aware watermark: onMissed: skip carries
        finished_at (watermark advances over the interrupted slot);
        run-once/run-all carry interruptedAt instead so the occurrence
        stays owed to catch-up.
        """
        started_iso = rec.get("startedAt")
        if not isinstance(started_iso, str):
            started_iso = get_now(datetime.timezone.utc).isoformat()
        fail_reason = (
            "run interrupted: no completion was recorded for the run "
            "started at {} on {} (daemon crash, or the node lost access "
            "to the state store mid-run)".format(started_iso, rec.get("host"))
        )
        data: dict[str, Any] = {
            "outcome": "unknown",
            "exit_code": None,
            "started_at": None,
            "duration": None,
            "fail_reason": fail_reason,
        }
        if job is None or job.onMissed == "skip":
            data["finished_at"] = started_iso
            # the run-instant mirror the durable superseded-by-run guard
            # folds over (see JobRunInfo.to_dict): an interrupted run IS a
            # run, so a ladder armed before it started is resolved.
            data["ranAt"] = started_iso
        else:
            data["interruptedAt"] = started_iso
        self._queue_inflight_write(
            name, lambda: self._persist_inflight_closed(name, reason)
        )
        self._track_state_write(self._persist_reconciled_record(name, data))
        # make it visible on this node's dashboard immediately (bypassing
        # _record_run: no metric emission, no double-persist).
        finished = _parse_iso_utc(started_iso) or get_now(
            datetime.timezone.utc
        )
        output = JobOutputStream()
        output.close()
        info = JobRunInfo(
            outcome="unknown",
            exit_code=None,
            started_at=None,
            finished_at=finished,
            fail_reason=fail_reason,
            output=output,
        )
        self._install_run_info(name, info)
        # a takeover can reconcile a foreign record older than a run this
        # node already recorded, so advance the supersede watermark rather
        # than assigning it (the durable side is a derive_max, i.e. already
        # monotonic).
        previous = self._last_completed_at.get(name)
        if previous is None or finished > previous:
            self._last_completed_at[name] = finished
        logger.warning(
            "Job %s: reconciled an interrupted run (%s): %s",
            name,
            reason,
            fail_reason,
        )

    async def _persist_reconciled_record(
        self, name: str, data: dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:
            return
        stream = self._run_stream(name)
        try:
            await backend.append_record(
                stream,
                data,
                prune_keep=(
                    self._state_max_runs if self._state_max_runs > 0 else None
                ),
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget
            self.metrics.state_write_dropped("run-record")
            logger.warning(
                "state: failed to persist the reconciled run record for "
                "%s: %s",
                name,
                ex,
            )

    # continually watches for the running jobs, clean them up when they exit
    async def _wait_for_running_jobs(self) -> None:
        # job -> wait task
        wait_tasks: dict[RunningJob, asyncio.Task] = {}
        # standing wait on _jobs_running in the busy branch's wait set: a
        # launch or the shutdown signal wakes the reaper immediately, so
        # the loop is fully event-driven (no poll timeout).
        event_wait: asyncio.Task | None = None
        try:
            while self.running_jobs or not self._stop_event.is_set():
                try:
                    for jobs in self.running_jobs.values():
                        for job in jobs:
                            if job not in wait_tasks:
                                wait_tasks[job] = asyncio.create_task(
                                    job.wait()
                                )
                    if not wait_tasks:
                        # Nothing running: block until a launch or shutdown
                        # (the only events that change the loop condition).
                        await self._jobs_running.wait()
                        continue
                    # Every job now running has its wait task registered
                    # above, with no await in between, so clearing here
                    # cannot swallow a launch notification for a job the
                    # wait set does not cover.
                    self._jobs_running.clear()
                    if event_wait is None or event_wait.done():
                        event_wait = asyncio.create_task(
                            self._jobs_running.wait()
                        )
                    done_tasks, _ = await asyncio.wait(
                        [event_wait, *wait_tasks.values()],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    done_jobs = set()
                    for job, task in list(wait_tasks.items()):
                        if task in done_tasks:
                            done_jobs.add(job)
                    try:
                        for job in done_jobs:
                            task = wait_tasks.pop(job)
                            try:
                                task.result()
                            except Exception:  # pragma: no cover
                                logger.exception(
                                    "Unexpected error while waiting on job "
                                    "%s; please report this as a bug (2)",
                                    job.config.name,
                                )
                            try:
                                await self._handle_finished_job(job)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                # Per-job, so one job's failure to finish
                                # does not skip the rest of the batch. No
                                # pragma: this arm is covered by
                                # test_reaper_finishes_whole_batch_when_
                                # one_job_raises.
                                logger.exception(
                                    "Unexpected error finishing job %s; "
                                    "please report this as a bug (6)",
                                    job.config.name,
                                )
                    finally:
                        # Flush buffered DAG-task completions in one RMW
                        # per run. In a finally: the buffer holds
                        # completions from jobs ALREADY handled, and
                        # nothing else drains it, so skipping the flush on
                        # a later job's exception would strand their
                        # dag_run entries as RUNNING indefinitely.
                        await self._dag.flush_completions()
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover
                    logger.exception("please report this as a bug (3)")
                    await asyncio.sleep(1)
        finally:
            if event_wait is not None and not event_wait.done():
                event_wait.cancel()

    def _add_running_instance(self, running_job: RunningJob) -> bool:
        """Register a launched instance; the ONE writer adding to
        ``running_jobs``.

        Every launch funnels through here so the memo bust cannot be
        forgotten (running must flip on the next poll, not age out by
        TTL). The source-shape test pins the funnel.
        """
        # returns True when this is the job's first live instance
        name = running_job.config.name
        first = not self.running_jobs.get(name)
        self.running_jobs[name].append(running_job)
        self._bust_response_memos()
        return first

    def _remove_running_instance(
        self, running_job: RunningJob, *, missing_ok: bool = False
    ) -> bool:
        """Drop a finished instance; the ONE writer removing from
        ``running_jobs``.

        The removing half of :meth:`_add_running_instance`, under the same
        funnel discipline and the same source-shape pin.
        """
        # returns True when it was the last live instance. missing_ok is the
        # DAG reaper's defensive shape; the default keeps
        # _handle_finished_job's strict KeyError/ValueError crash-on-bug
        # behavior
        name = running_job.config.name
        if missing_ok:
            jobs_list = self.running_jobs.get(name)
            if jobs_list is None:
                return False
            try:
                jobs_list.remove(running_job)
            except ValueError:  # pragma: no cover - defensive
                return False
        else:
            jobs_list = self.running_jobs[name]
            jobs_list.remove(running_job)
        last = not jobs_list
        if last:
            del self.running_jobs[name]
        self._bust_response_memos()
        return last

    def _install_run_info(self, name: str, info: JobRunInfo) -> None:
        """Record one finished-run row; the ONE writer of ``run_history``
        and ``last_run``.

        Every recording path funnels through here so the memo bust
        cannot be forgotten (the shared /jobs product folds over
        last_run and the history slice). The source-shape test pins the
        funnel.
        """
        self.run_history[name].append(info)
        self.last_run[name] = info
        self._bust_response_memos()

    def _record_run(self, name: str, info: JobRunInfo) -> None:
        # the latest finished run (for status/log replay) plus the bounded
        # history (for the dashboard's history/stats view); in-memory only.
        prev = self.last_run.get(name)
        self._install_run_info(name, info)
        # this run changes the trends aggregate, so drop any cached payload
        # for the job rather than serve it stale out to the TTL.
        self._trends_cache.pop(name, None)
        # every recorded run also feeds the Prometheus counters/histogram,
        # so /metrics and the run-history API always agree on outcomes.
        self.metrics.job_run_recorded(
            name, info.outcome, info.duration, info.resource_usage
        )
        # the maxTimeSinceSuccess reference (see _sla_periodic): every
        # recorded success moves it, whatever path ran the job.
        if info.outcome == "success" and info.finished_at is not None:
            self._sla_last_success[name] = info.finished_at
        # onlyIfLastSucceeded's eviction-proof memo (_depends_on_past_ok):
        # a long pause's synthetic rows can push the last real outcome out
        # of the bounded ring and reopen a gate a failure had closed.
        if info.outcome in ("success", "failure") and (
            info.finished_at is not None
        ):
            self._last_real_outcome[name] = (info.finished_at, info.outcome)
        # retry ladder's superseded-by-run watermark
        # (_validate_pending_retry). Every outcome but "skipped" counts:
        # a pause-held slot ran nothing and must not settle a ladder.
        if info.outcome != "skipped" and info.finished_at is not None:
            self._last_completed_at[name] = info.finished_at
        # persist the run to the ledger (fire-and-forget: a slow store
        # must never stall run handling). No-op on the stateless default.
        if self.state_backend is not None:
            # The archive's line snapshot is taken HERE, synchronously: a
            # back-to-back completion can supersede this record and
            # release its ring before the persist task body runs.
            job = self.cron_jobs.get(name)
            archive_lines = (
                list(info.output.lines)
                if job is not None and job.archiveOutput
                else None
            )
            self._track_state_write(
                self._persist_run_record(name, info, archive_lines)
            )
        # Release the superseded record's ring buffer: only the NEWEST
        # finished run's output is replayable, yet `prev`'s ring would
        # otherwise sit in run_history for many more completions. The
        # identity guards keep odd construction shapes safe.
        if (
            prev is not None
            and prev is not info
            and prev.output is not info.output
        ):
            prev.output.release_lines()

    @staticmethod
    def _run_stream(name: str) -> str:
        """The durable ledger stream name for a job's finished runs."""
        return RUN_STREAM_PREFIX + name

    @staticmethod
    def _log_stream(name: str) -> str:
        """The durable stream name for a job's archived captured output."""
        return LOG_STREAM_PREFIX + name

    @staticmethod
    def _retry_stream(name: str) -> str:
        """The durable stream name for a job's retry-ladder records."""
        return RETRY_STREAM_PREFIX + name

    @staticmethod
    def _reboot_stream(name: str) -> str:
        """The durable stream name for a job's @reboot boot markers."""
        return REBOOT_STREAM_PREFIX + name

    @staticmethod
    def _pause_stream(name: str) -> str:
        """The durable stream name for a job's pause/resume records."""
        return PAUSE_STREAM_PREFIX + name

    def _counters_stream(self) -> str:
        """The durable stream name for this host's counter snapshots."""
        return COUNTER_STREAM_PREFIX + self._state_host

    async def _persist_run_record(
        self,
        name: str,
        info: JobRunInfo,
        archive_lines: Optional[list[tuple[str, str]]] = None,
    ) -> None:
        """Append one finished run to the durable ledger, prune, and archive.

        Background task; errors are logged and swallowed (durability
        failures must never break job handling). Archives from
        ``archive_lines``, the record-time ring snapshot, never the live
        ring (it may have been released by a newer completion); None
        means the job did not archive at record time and a later
        archiveOutput flip must not archive an unsnapshotted ring.
        """
        backend = self.state_backend
        if backend is None:  # torn down between scheduling and running
            return
        stream = self._run_stream(name)
        try:
            # include_series: the ledger rehydrates the resource charts
            # after a restart; bounded per record and per stream.
            await asyncio.wait_for(
                backend.append_record(
                    stream,
                    info.to_dict(include_series=True),
                    prune_keep=(
                        self._state_max_runs
                        if self._state_max_runs > 0
                        else None
                    ),
                ),
                timeout=STATE_OP_TIMEOUT,
            )
            job = self.cron_jobs.get(name)
            if (
                job is not None
                and job.archiveOutput
                and archive_lines is not None
            ):
                await self._archive_output(job, info, archive_lines)
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("run-record")
            logger.warning(
                "state: failed to persist run record for %s: %s", name, ex
            )
        # piggyback the throttled counter snapshot: a finished run is the
        # moment the counters changed. Has its own error handling.
        await self._persist_counter_snapshot(throttled=True)

    async def _persist_counter_snapshot(
        self, *, throttled: bool = False
    ) -> None:
        """Append a durable snapshot of the Prometheus counter accumulators.

        Host-scoped stream; newest wins on rehydration. ``throttled``
        skips the write within COUNTER_SNAPSHOT_INTERVAL; shutdown writes
        one final unthrottled snapshot. Lossy by design (a crash reads as
        an ordinary counter reset). Gated on _counters_seeded so the seed
        can never read back a snapshot this process wrote and
        double-count.
        """
        backend = self.state_backend
        if backend is None or not self._counters_seeded:
            return
        if throttled:
            now = asyncio.get_running_loop().time()
            if now < self._counter_snapshot_next:
                return
            self._counter_snapshot_next = now + COUNTER_SNAPSHOT_INTERVAL
        record = self.metrics.counters_snapshot()
        record["at"] = get_now(datetime.timezone.utc).isoformat()
        stream = self._counters_stream()
        try:
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=COUNTER_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("counters")
            logger.warning(
                "state: failed to persist the counter snapshot: %s", ex
            )

    async def _archive_output(
        self,
        job: JobConfig,
        info: JobRunInfo,
        raw: list[tuple[str, str]],
    ) -> None:
        """Write a finished run's captured output to the durable log store.

        Opt-in per job (archiveOutput). Archives ``raw``, the record-time
        ring snapshot (evicted lines are counted in dropped_lines).
        saveLimit: 0 archives nothing. Lines are scrubbed via
        redact.redact_lines unless redactArchivedSecrets: false;
        encryption-at-rest is the mount's job, this only redacts. Pruned
        to the same per-job bound as the ledger.
        """
        backend = self.state_backend
        if backend is None:
            return
        if job.saveLimit == 0:
            return
        redact = job.redactArchivedSecrets
        if redact:
            # Executor-offloaded: a pathological line must degrade THIS
            # archive write, not stall dispatch/web/heartbeats. Patterns
            # are linear (see cronstable.redact); this is defence in
            # depth.
            texts = await asyncio.get_running_loop().run_in_executor(
                None, redact_lines, [line for _stream, line in raw]
            )
        else:
            texts = [line for _stream, line in raw]
        lines = [
            {"stream": stream_name, "line": text}
            for (stream_name, _), text in zip(raw, texts, strict=True)
        ]
        record = {
            "finished_at": info.finished_at.isoformat(),
            "outcome": info.outcome,
            "exit_code": info.exit_code,
            "redacted": redact,
            "dropped_lines": max(0, info.output.published - len(raw)),
            "lines": lines,
        }
        stream = self._log_stream(job.name)
        # Bounded; the caller catches a timeout as a dropped write.
        await asyncio.wait_for(
            backend.append_record(
                stream,
                record,
                prune_keep=(
                    self._state_max_runs if self._state_max_runs > 0 else None
                ),
            ),
            timeout=STATE_OP_TIMEOUT,
        )

    async def _warm_last_success_beyond_history(
        self, name: str, history: Iterable[JobRunInfo]
    ) -> None:
        """Find the staleness reference outside the warmed history window.

        A job failing more often than RUN_HISTORY_LIMIT since its last
        success has no success in the warmed window, and an unset
        reference re-baselines maxTimeSinceSuccess every restart. Re-read
        deeper once; failing that fall back to the OLDEST record seen (a
        lower bound on staleness: pages later than the truth, never
        earlier). Only jobs configuring the check reach here.
        """
        backend = self.state_backend
        seen = [r.finished_at for r in history if r.finished_at is not None]
        if backend is not None:
            deeper = max(SLA_SUCCESS_SCAN_LIMIT, self._state_max_runs)
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=deeper,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - the floor below stands
                logger.warning(
                    "state: cannot widen the last-success scan for %s "
                    "(falling back to the oldest warmed record): %s",
                    name,
                    ex,
                )
                recs = []
            successes = []
            for restored in (_job_run_info_from_dict(r) for r in recs):
                if restored is None or restored.finished_at is None:
                    continue
                if restored.outcome == "success":
                    successes.append(restored.finished_at)
                seen.append(restored.finished_at)
            if successes:
                self._sla_last_success.setdefault(name, max(successes))
                return
        if seen:
            self._sla_last_success.setdefault(name, min(seen))

    async def _seed_stale_reference(
        self, name: str, history: Iterable[JobRunInfo]
    ) -> None:
        """Warm the maxTimeSinceSuccess staleness reference from ``history``.

        Split out so the warm-up's early-continue guards still seed the
        reference (a job that only FAILED during a state outage must not
        re-baseline on process start). By finished_at, not position:
        run-record writes are unserialized, so the last-APPENDED success
        can be older than one appended before it. setdefault throughout,
        so a live success recorded mid-await is never clobbered.
        """
        history = list(history)
        successes = [
            r.finished_at
            for r in history
            if r.outcome == "success" and r.finished_at is not None
        ]
        if successes:
            self._sla_last_success.setdefault(name, max(successes))
            return
        # a reload during one of the awaits above can drop the job, so .get()
        # rather than a subscript.
        warmed_job = self.cron_jobs.get(name)
        if warmed_job is not None and (
            warmed_job.sla.get("maxTimeSinceSuccessSeconds") is not None
        ):
            await self._warm_last_success_beyond_history(name, history)

    async def _rehydrate_from_state(self) -> None:
        """Warm the in-memory history from the durable ledger, once, on boot.

        Loads each job's newest records into last_run/run_history so the
        status surfaces are correct from the first scrape. Bypasses
        _record_run deliberately: rehydration must not re-emit counters
        or re-persist what it just read. Poison records are skipped,
        never fatal. Reads via the bounded boot-scan pool.
        """
        backend = self.state_backend
        if backend is None or self._state_rehydrated:
            return
        self._state_rehydrated = True

        async def _warm_one(name: str, _job: JobConfig) -> Optional[str]:
            # existing in-memory history stays the source of truth, but
            # the staleness reference is still seeded from it (a job that
            # only FAILED during a state outage must not re-baseline).
            if self.run_history.get(name):
                await self._seed_stale_reference(name, self.run_history[name])
                return None
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._run_stream(name),
                        limit=RUN_HISTORY_LIMIT,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                # abandoning the warm-up costs little: the dashboard
                # fills in as jobs run.
                return "timeout"
            except OSError as ex:
                logger.warning(
                    "state: failed to rehydrate history for %s: %s", name, ex
                )
                return None
            if self.run_history.get(name):
                # a run finished while we awaited the read: the live run
                # is fresher; appending old records would regress
                # last_run and scramble history order.
                await self._seed_stale_reference(name, self.run_history[name])
                return None
            recs.reverse()  # oldest-first, to match the append order
            for rec in recs:
                restored = _job_run_info_from_dict(rec)
                if restored is not None:
                    self._install_run_info(name, restored)
            history = self.run_history.get(name)
            if not history:
                return None
            # warm the staleness reference too: with a durable ledger
            # maxTimeSinceSuccess must page from the REAL last success,
            # not re-baseline on process start.
            await self._seed_stale_reference(name, history)
            # and the onlyIfLastSucceeded memo: a pause across the
            # restart can fill the warmed ring with "skipped" rows. By
            # finished_at, not position (unserialized writes): the
            # last-APPENDED real outcome could pick an older success
            # over a newer failure and reopen the gate.
            reals = [
                r
                for r in history
                if r.outcome in ("success", "failure")
                and r.finished_at is not None
            ]
            if reals:
                newest = max(reals, key=lambda r: r.finished_at)
                self._last_real_outcome.setdefault(
                    name, (newest.finished_at, newest.outcome)
                )
            # and the retry ladder's supersede watermark: a pause across
            # the restart makes history[-1] a "skipped" row whose fresh
            # finished_at would settle every pending retry.
            for restored in reversed(history):
                if (
                    restored.outcome != "skipped"
                    and restored.finished_at is not None
                ):
                    self._last_completed_at.setdefault(
                        name, restored.finished_at
                    )
                    break
            return "counted"

        warmed = await self._bounded_boot_scan(
            list(self.cron_jobs.items()),
            _warm_one,
            "state: rehydration timed out reading %s; skipping the rest "
            "of the warm-up (store unhealthy?)",
        )
        if warmed:
            logger.info(
                "state: rehydrated run history for %d job(s) from the ledger",
                warmed,
            )
        # BEFORE the retry re-arm: a reconciled interrupted run updates
        # last_run, and the superseded-by-run guard must see it.
        await self._reconcile_inflight()
        await self._rehydrate_counters()
        # pause state before the retry re-arm too: a re-armed ladder's gate
        # loop must defer on a durable pause from its very first check.
        await self._refresh_pauses_from_store()
        await self._rehydrate_retries()
        # adopt this node's active DAG runs from durable state (the DAG
        # analogue of _reconcile_inflight); resumed from that state,
        # never from memory.
        await self._dag.reconcile_on_boot()

    async def _rehydrate_counters(self) -> None:
        """Seed the Prometheus accumulators from the newest durable snapshot.

        At most ONCE per process: seeding ADDS, so seeding twice (or from
        a snapshot THIS process wrote) would double-count. The latch is
        set BEFORE the read and also gates _persist_counter_snapshot. An
        unreadable store forfeits the seed (an ordinary counter reset to
        Prometheus).
        """
        backend = self.state_backend
        if backend is None or self._counters_seeded:
            return
        self._counters_seeded = True
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._counters_stream(), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: cannot rehydrate the metric counters (the seed is "
                "forfeited for this process): %s",
                ex,
            )
            return
        if not recs:
            return
        seeded = self.metrics.seed_counters(recs[0], set(self.cron_jobs))
        if seeded:
            logger.info(
                "state: rehydrated Prometheus counters for %d job(s) from "
                "the durable snapshot",
                seeded,
            )

    async def _rehydrate_retries(self) -> None:
        """Re-arm pending durable retries after a restart.

        ABSOLUTE-deadline re-arming: notBefore is an instant, so the
        re-armed task sleeps only the remaining time. Invalidation is by
        PER-JOB config digest (job_digest), not job-set id. Every
        ambiguous case settles the ladder: no-run-on-ambiguity is the
        documented bias. An @reboot ladder re-arms only when the boot
        marker proves this boot already ran (a fresh boot run supersedes
        the stale ladder). Foreign-host pending records are neither
        re-armed nor settled; a record older than the newest KNOWN run is
        settled as superseded. Re-armed via the ordinary
        schedule_retry_job, so gate re-checks and shutdown behave as for
        a never-restarted ladder.
        """
        backend = self.state_backend
        if backend is None:
            return
        await self._bounded_boot_scan(
            list(self.cron_jobs.items()),
            self._rearm_pending_retry,
            "state: retry re-arm timed out reading %s; "
            "skipping the rest (store unhealthy?)",
        )

    async def _rearm_pending_retry(
        self, name: str, job: JobConfig
    ) -> Optional[str]:
        """One job's step of the retry re-arm scan (see the caller).

        "timeout" abandons the rest of the pass; None in every other
        case, re-armed or settled or skipped alike.
        """
        backend = self.state_backend
        if backend is None:
            return None
        if name in self.retry_state or self.running_jobs.get(name):
            # live activity always outranks the ledger
            return None
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._retry_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return "timeout"
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - degrade, never crash
            logger.warning(
                "state: cannot read pending retries for %s: %s", name, ex
            )
            return None
        if not recs or recs[0].get("kind") != "pending":
            return None
        rec = recs[0]
        rec_host = rec.get("host")
        if isinstance(rec_host, str) and rec_host != self._state_host:
            # another node's live ladder (shared store): not ours to
            # re-arm OR settle.
            return None
        if name not in self._last_completed_at:
            # A long pause can flood the warmed ring with "skipped" rows,
            # leaving this memo unset; the superseded-by-run guard would
            # then re-arm a ladder a real run already settled. Seed the
            # memo from the flood-independent durable fold (derive_max
            # over ranAt) first. One extra read, only when a pending
            # record exists.
            try:
                durable_at = await asyncio.wait_for(
                    self.durable_last_completed_at(name),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unknown -> guard stays open
                durable_at = None
            parsed = (
                _parse_iso_utc(durable_at)
                if isinstance(durable_at, str)
                else None
            )
            if parsed is not None:
                self._last_completed_at[name] = parsed
        validated = self._validate_pending_retry(name, job, rec)
        if validated is None:
            return None
        attempt, not_before = validated
        if isinstance(job.schedule, str) and job.schedule == "@reboot":
            try:
                covered = await self._reboot_marker_covers(job)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unknown -> not covered
                covered = False
            if not covered:
                self._persist_retry_settled(
                    name, "superseded-by-reboot", attempt
                )
                return None
        retry = job.onFailure["retry"]
        state = JobRetryState(
            retry["initialDelay"],
            retry["backoffMultiplier"],
            retry["maximumDelay"],
        )
        # replay the ladder to the persisted position: count == attempt,
        # delay == what the NEXT failure would sleep.
        for _ in range(attempt):
            state.next_delay()
        now = get_now(datetime.timezone.utc)
        remaining = max(0.0, (not_before - now).total_seconds())
        self.retry_state[name] = state
        state.task = asyncio.create_task(
            self.schedule_retry_job(name, remaining, attempt)
        )
        logger.info(
            "Job %s: re-armed pending retry #%d from the durable "
            "ledger (due in %.1f seconds)",
            name,
            attempt,
            remaining,
        )
        return None

    def _validate_pending_retry(
        self, name: str, job: JobConfig, rec: dict[str, Any]
    ) -> Optional[tuple[int, datetime.datetime]]:
        """Judge a pending-retry record against the LIVE job definition.

        Returns ``(attempt, notBefore)`` when the ladder may be re-armed;
        ``None`` after settling it with the reason it must not be:
        unparseable content, retries disabled or the job's config digest
        changed since arming, a newer run proving the ladder resolved,
        the job disabled, the budget exhausted, or the record staler than
        the job's own startingDeadlineSeconds window.
        """
        retry = job.onFailure["retry"]
        parsed = _parse_retry_record(rec)
        if parsed is None:
            self._persist_retry_settled(name, "invalid-record")
            return None
        attempt, not_before, armed_at = parsed
        rec_digest = rec.get("jobDigest")
        if not retry["maximumRetries"] or rec_digest != job_digest_cached(job):
            # retries disabled since arming, or any behaviour-affecting
            # field changed: the old ladder must not run the new definition
            # (nor lurk until a later config revert).
            self._persist_retry_settled(name, "config-changed", attempt)
            return None
        # the newest ACTUAL run, not last_run: a pause-held slot's
        # "skipped" row would settle every ladder the pause is only
        # holding, while a real run buried under a later pause must
        # still be seen.
        last_at = self._last_completed_at.get(name)
        if last_at is not None and last_at > armed_at:
            # a run finished AFTER this retry was armed: the ladder was
            # resolved some way (its settle may have been dropped while
            # the store was down). No-run beats double-run.
            self._persist_retry_settled(name, "superseded-by-run", attempt)
            return None
        if not job.enabled:
            self._persist_retry_settled(name, "disabled", attempt)
            return None
        maximum = retry["maximumRetries"]
        if maximum != -1 and attempt > maximum:
            self._persist_retry_settled(name, "exhausted", attempt)
            return None
        now = get_now(datetime.timezone.utc)
        deadline = job.startingDeadlineSeconds
        if deadline and (now - not_before).total_seconds() > deadline:
            # same bound catch-up honours: a retry stale beyond the job's
            # own catch-up window is not worth replaying.
            self._persist_retry_settled(name, "deadline-passed", attempt)
            return None
        return attempt, not_before

    async def durable_last_run_at(self, name: str) -> Optional[str]:
        """The last finished-run timestamp for a job, from the durable ledger.

        Max finished_at over the immutable records (order-independent;
        ISO-8601 UTC, so lexicographic max is chronological max). None
        with no backend or records. Consumed by missed-run catch-up.
        """
        backend = self.state_backend
        if backend is None:
            return None
        result = await backend.derive_max(
            self._run_stream(name), "finished_at"
        )
        return result if isinstance(result, str) else None

    async def durable_last_completed_at(self, name: str) -> Optional[str]:
        """The last ACTUAL-run timestamp for a job, from the durable ledger.

        durable_last_run_at's skip-blind twin, for the superseded-by-run
        guards. Folds ``ranAt`` (written on run rows only), never
        finished_at, which synthetic "skipped" rows also carry: folding
        that would let a pause settle every ladder it is only holding.
        Pre-``ranAt`` records (finished_at only) are folded in by outcome
        so an upgrade does not re-arm resolved ladders. Read by the claim
        scan and the local retry rehydrate, so both paths see the same
        truth; the deeper re-read fires only on the pre-ranAt None path.
        """
        backend = self.state_backend
        if backend is None:
            return None
        stream = self._run_stream(name)
        derived = await backend.derive_max(stream, "ranAt")
        best = derived if isinstance(derived, str) else None

        def _fold_pre_ranat(
            records: list[dict[str, Any]], acc: Optional[str]
        ) -> Optional[str]:
            # A row with no ``ranAt`` and outcome != skipped is a real run
            # from before ``ranAt`` existed (a pause-skip row never took that
            # older shape); fold its finished_at, a row carrying ``ranAt`` is
            # already in the derive_max above.
            for rec in records:
                if "ranAt" in rec or rec.get("outcome") == "skipped":
                    continue
                at = rec.get("finished_at")
                if isinstance(at, str) and (acc is None or at > acc):
                    acc = at
            return acc

        recs = await backend.list_records(
            stream, limit=RUN_HISTORY_LIMIT, newest_first=True
        )
        best = _fold_pre_ranat(recs, best)
        if best is None:
            # A pre-``ranAt`` ledger buried under RUN_HISTORY_LIMIT
            # pause-skip rows folds to None and would re-arm a resolved
            # ladder. Re-read ONCE, deeper, on this None path only; one
            # post-upgrade real run keeps this off the steady state.
            deeper = max(SLA_SUCCESS_SCAN_LIMIT, self._state_max_runs)
            deep = await backend.list_records(
                stream, limit=deeper, newest_first=True
            )
            best = _fold_pre_ranat(deep, best)
        return best

    async def _list_gate_records(
        self, backend: Any, name: str
    ) -> list[dict[str, Any]]:
        """Newest durable run records for the onlyIfLastSucceeded gate.

        Probes DEPENDS_GATE_PROBE newest records and widens to
        RUN_HISTORY_LIMIT only when that page is entirely non-run
        outcomes. Raises through to the caller's fail-closed/degrade
        handling on a store error or timeout.
        """
        stream = self._run_stream(name)
        probe = min(DEPENDS_GATE_PROBE, RUN_HISTORY_LIMIT)
        recs: list[dict[str, Any]] = await asyncio.wait_for(
            backend.list_records(stream, limit=probe, newest_first=True),
            timeout=STATE_OP_TIMEOUT,
        )
        # a real outcome in the probe page, or a page shorter than the probe
        # (the stream is exhausted), means widening would reveal nothing newer.
        if len(recs) < probe or any(
            r.get("outcome") in ("success", "failure") for r in recs
        ):
            return recs
        widened: list[dict[str, Any]] = await asyncio.wait_for(
            backend.list_records(
                stream, limit=RUN_HISTORY_LIMIT, newest_first=True
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        return widened

    async def _depends_on_past_ok(self, job: JobConfig) -> bool:
        """Whether ``job``'s depends-on-past gate permits a scheduled fire.

        True unless onlyIfLastSucceeded is set AND the most recent run
        outcome was a failure, or the previous instance is STILL RUNNING
        (else the gate is a race). Judges the NEWEST of two sources by
        finished_at: the in-memory history (the ledger alone can be a
        beat stale) and the durable ledger (sees other nodes; a store
        error degrades fail-open to the in-memory view). Non-run
        outcomes are skipped in both; no prior run allows. The
        still-running block is SKIPPED for concurrencyPolicy: Replace
        (a hung run must not freeze the job). Applies to scheduled and
        @reboot fires; retries, backfills, and manual triggers bypass it.
        """
        if not job.onlyIfLastSucceeded:
            return True
        if job.concurrencyPolicy != "Replace" and self.running_jobs.get(
            job.name
        ):
            return False
        # Newest real outcome by finished_at, NOT list position: records
        # are written unserialized, so the last-APPENDED run can be OLDER
        # than one appended before it.
        reals = [
            info
            for info in (self.run_history.get(job.name) or ())
            if info.outcome in ("success", "failure")
            and info.finished_at is not None
        ]
        newest = max(reals, key=lambda i: i.finished_at, default=None)
        latest: Optional[tuple[datetime.datetime, str]] = (
            (newest.finished_at, newest.outcome)
            if newest is not None
            else None
        )
        # the bounded ring can lose the last real outcome to consecutive
        # non-run rows; the memo survives that eviction. Take whichever
        # is newer.
        memo = self._last_real_outcome.get(job.name)
        if memo is not None and (latest is None or memo[0] > latest[0]):
            latest = memo
        backend = self.state_backend
        if (
            backend is None
            and self._state_configured
            and self._state_on_unavailable == "fail-closed"
        ):
            # store configured but down under fail-closed: prefer not
            # running over deciding from possibly-stale memory.
            logger.warning(
                "Job %s: onlyIfLastSucceeded blocked: the state store is "
                "configured but unavailable and onStoreUnavailable is "
                "fail-closed",
                job.name,
            )
            return False
        if backend is not None:
            try:
                recs = await self._list_gate_records(backend, job.name)
            except asyncio.CancelledError:
                raise
            except Exception as ex:  # noqa: BLE001 - policy decides below
                if self._state_on_unavailable == "fail-closed":
                    logger.warning(
                        "Job %s: onlyIfLastSucceeded blocked: cannot read "
                        "the run ledger (%s) and onStoreUnavailable is "
                        "fail-closed",
                        job.name,
                        ex,
                    )
                    return False
                logger.warning(
                    "state: cannot read the run ledger for the "
                    "onlyIfLastSucceeded gate on %s (%s); deciding from the "
                    "in-memory history",
                    job.name,
                    ex,
                )
                recs = []
            for rec in recs:
                outcome = rec.get("outcome")
                if outcome not in ("success", "failure"):
                    continue
                finished = _parse_iso_utc(rec.get("finished_at"))
                candidate = (
                    finished
                    or datetime.datetime.min.replace(
                        tzinfo=datetime.timezone.utc
                    ),
                    str(outcome),
                )
                # Fold over ALL real records, max by finished_at, NOT
                # first-by-sequence: an out-of-order write could clear
                # the gate on a stale success ahead of a newer failure.
                if latest is None or candidate[0] > latest[0]:
                    latest = candidate
        if latest is None:
            return True
        return latest[1] == "success"

    async def _handle_finished_job(self, job: RunningJob) -> None:
        if getattr(job, "dag_ref", None) is not None:
            # a DAG task instance: route to the DAG scheduler and skip
            # the job record/retry/inflight/cluster-slot path; a task's
            # lifecycle lives in its dag_run document.
            await self._handle_finished_dag_task(job)
            return
        last_instance = self._remove_running_instance(job)
        if last_instance and self.state_backend is not None:
            # 1 -> 0 live instances: close the in-flight record. Before
            # the replaced/cancelled early-returns on purpose; ordered
            # behind the open (see _inflight_write_tail).
            self._queue_inflight_write(
                job.config.name,
                lambda: self._persist_inflight_closed(job.config.name),
            )
        if job.config.concurrencyScope == "cluster":
            # every claimed launch pairs with exactly one finish here;
            # before the early-returns for the same reason as above.
            await self._release_cluster_slot(job.config)

        if self._job_api is not None and job.state_token is not None:
            # revoke the run's loopback token/secrets and release any
            # lock it holds; before the early returns, paired one-to-one
            # with _prepare_job_api_run.
            await self._job_api.finish_run(job.state_token)

        if job.replaced:
            # deliberately cancelled to make way for a newer instance
            # (concurrencyPolicy=Replace); not a failure, so don't report it
            # or trigger retries.
            logger.info(
                "Job %s was replaced by a newer instance", job.config.name
            )
            return

        if job.cancelled:
            # explicitly cancelled by a user via the web UI: record it (as
            # "cancelled" in the dashboard) but, like a replacement, do not
            # report it as a failure or schedule retries.
            logger.info("Job %s was cancelled via the web UI", job.config.name)
            self._record_run(
                job.config.name,
                JobRunInfo(
                    outcome="cancelled",
                    exit_code=job.retcode,
                    started_at=job.started_at,
                    finished_at=get_now(datetime.timezone.utc),
                    fail_reason="cancelled via web UI",
                    output=job.output,
                    resource_usage=getattr(job, "resource_usage", None),
                ),
            )
            await self.cancel_job_retries(job.config.name, settle="cancelled")
            return

        if job.start_failed:
            # counted separately from ordinary failures: a command that
            # cannot launch at all (recorded below as a failure with the
            # conventional exit code 127) is usually a deploy/config bug,
            # not a job bug.
            self.metrics.job_start_failed(job.config.name)

        fail_reason = job.fail_reason
        logger.info(
            "Job %s exit code %s; has stdout: %s, "
            "has stderr: %s; fail_reason: %r",
            job.config.name,
            job.retcode,
            str(bool(job.stdout)).lower(),
            str(bool(job.stderr)).lower(),
            fail_reason,
        )
        # record this run for the web UI's "latest status / latest logs" view
        self._record_run(
            job.config.name,
            JobRunInfo(
                outcome="failure" if fail_reason is not None else "success",
                exit_code=job.retcode,
                started_at=job.started_at,
                finished_at=get_now(datetime.timezone.utc),
                fail_reason=fail_reason,
                output=job.output,
                resource_usage=getattr(job, "resource_usage", None),
            ),
        )
        self._queue_job_completion(job, failed=fail_reason is not None)

    def _queue_job_completion(self, job: RunningJob, *, failed: bool) -> None:
        """Run a finished job's report+retry-arm sequence as a tracked task.

        Spawned per finished run so a slow reporter cannot stall the
        reaper daemon-wide, chained behind the same job's previous
        sequence so overlapping instances keep serial retry semantics.
        Tracked in _completion_tasks so shutdown drains in-flight reports
        (_drain_completions).
        """
        name = job.config.name

        async def _handle() -> None:
            try:
                if failed:
                    await self.handle_job_failure(job)
                else:
                    await self.handle_job_success(job)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error handling the completion of job %s; "
                    "please report this as a bug (8)",
                    name,
                )

        self._install_tail_task(
            self._completion_tail,
            name,
            _handle,
            spawn=self._spawn_completion,
        )

    async def _drain_completions(self) -> None:
        """Await every in-flight report+retry-arm sequence.

        Run by shutdown after the running-job drain, unbounded: reports
        for runs that finished before the stop signal must still go out.
        Also the seam tests use to observe completion side effects.
        """
        while self._completion_tasks:
            await asyncio.wait(set(self._completion_tasks))

    async def _handle_finished_dag_task(self, job: RunningJob) -> None:
        """Reap one finished DAG task instance (see ``_handle_finished_job``).

        Removes it from the running set, drops its loopback token (and
        any lock it still holds), then hands the outcome to the DAG scheduler,
        which records the durable per-task transition and advances the graph.
        Writes no ``runs/`` / ``retries/`` / ``inflight/`` records: a DAG
        task's whole lifecycle lives in its ``dag_run`` document.
        """
        self._remove_running_instance(job, missing_ok=True)
        if self._job_api is not None and job.state_token is not None:
            await self._job_api.finish_run(job.state_token)
        try:
            await self._dag.on_task_finished(job)
        except Exception:  # noqa: BLE001 - never kill the reaper
            logger.exception("dag: failed to record a task completion")
        # Fire the task's own run reporters after the durable transition
        # so a report can never delay it. No retry arming and no
        # onPermanentFailure: a task's attempts are graph-driven.
        # Cancelled/replaced runs are not failures; shutdown skips
        # reporting. The enabled-probe keeps the unconfigured case at
        # dict lookups (a mapped fan-out lands many completions at once).
        if not (job.cancelled or job.replaced or self._stop_event.is_set()):
            failed = job.fail_reason is not None
            hook = job.config.onFailure if failed else job.config.onSuccess
            if report_config_enabled(hook["report"]):
                self._queue_dag_task_report(job, failed=failed)

    def _queue_dag_task_report(self, job: RunningJob, *, failed: bool) -> None:
        """Spawn one DAG task run's report fan-out as a tracked task.

        The DAG sibling of :meth:`_queue_job_completion`, minus the per-name
        sequencing: a task report carries no retry-arm step, so mapped
        instances of one task finishing together may report concurrently.
        Tracked in ``_completion_tasks`` so shutdown's
        :meth:`_drain_completions` awaits in-flight reports (and tests observe
        them deterministically).
        """

        async def _report() -> None:
            try:
                if failed:
                    await job.report_failure()
                else:
                    await job.report_success()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Unexpected error reporting the dag task run %s",
                    job.config.name,
                )

        task = asyncio.create_task(_report())
        self._completion_tasks.add(task)
        task.add_done_callback(self._completion_tasks.discard)

    async def handle_job_failure(self, job: RunningJob) -> None:
        if self._stop_event.is_set():
            return
        if job.stdout:
            logger.info(
                "Job %s STDOUT:\n%s", job.config.name, job.stdout.rstrip()
            )
        if job.stderr:
            logger.info(
                "Job %s STDERR:\n%s", job.config.name, job.stderr.rstrip()
            )
        await job.report_failure()

        # Handle retries...
        state = job.retry_state
        if state is None or state.cancelled:
            self.metrics.job_permanent_failure(job.config.name)
            await job.report_permanent_failure()
            return

        logger.debug(
            "Job %s has been retried %i times", job.config.name, state.count
        )
        if state.task is not None:
            if state.task.done():
                self._reap_retry_task(job.config.name, state.task)
            else:
                state.task.cancel()
        retry = job.config.onFailure["retry"]
        if (
            state.count >= retry["maximumRetries"]
            and retry["maximumRetries"] != -1
        ):
            await self.cancel_job_retries(job.config.name, settle="exhausted")
            self.metrics.job_permanent_failure(job.config.name)
            await job.report_permanent_failure()
        else:
            retry_delay = state.next_delay()
            state.task = asyncio.create_task(
                self.schedule_retry_job(
                    job.config.name, retry_delay, state.count
                )
            )

    async def schedule_retry_job(
        self, job_name: str, delay: float, retry_num: int
    ) -> None:
        logger.info(
            "Cron job %s scheduled to be retried (#%i) in %.1f seconds",
            job_name,
            retry_num,
            delay,
        )
        # Persist the pending retry with its ABSOLUTE deadline so a
        # restart re-arms only the remaining delay (_rehydrate_retries).
        # A write that never lands loses only durability; later ladder
        # writes are ordered after it (_queue_retry_write).
        pending_job = self.cron_jobs.get(job_name)
        if pending_job is not None:
            now_arm = get_now(datetime.timezone.utc)
            not_before = now_arm + datetime.timedelta(seconds=delay)
            self._persist_retry_pending(pending_job, retry_num, not_before)
            # record the absolute fire time (GET /jobs countdown) and the
            # arm instant (a cross-node hand-off anchors its
            # superseded-by-run guard on ARM time, see _abandon_retry).
            armed_state = self.retry_state.get(job_name)
            if armed_state is not None:
                armed_state.next_retry_at = not_before
                armed_state.scheduled_delay = delay
                armed_state.armed_at = now_arm
        await asyncio.sleep(delay)
        deferrals = 0
        while True:
            try:
                job = self.cron_jobs[job_name]
            except KeyError:
                logger.warning(
                    "Cron job %s was scheduled for retry, but "
                    "disappeared from the configuration",
                    job_name,
                )
                # clear the now-stale retry state and stop; falling through
                # here would call maybe_launch_job(job) with an unbound 'job'.
                self.retry_state.pop(job_name, None)
                self._persist_retry_settled(job_name, "job-removed", retry_num)
                return
            # A paused job DEFERS its pending retry: the attempt fires
            # after the resume, never consumed by the pause. Covers
            # boot-rehydrated ladders too (they re-arm through here).
            pause = self._pause_active(job_name)
            if pause is not None:
                state = self.retry_state.get(job_name)
                if (
                    state is None
                    or state.cancelled
                    or self._stop_event.is_set()
                ):
                    return
                recheck = max(delay, RETRY_GATE_RECHECK_FLOOR)
                log = logger.info if deferrals == 0 else logger.debug
                deferrals += 1
                log(
                    "Cron job %s retry (#%i) deferred: the job is paused "
                    "until %s; re-checking in %.1f seconds",
                    job_name,
                    retry_num,
                    pause.until.isoformat(),
                    recheck,
                )
                await asyncio.sleep(recheck)
                continue
            # Re-check the leadership gate before relaunching: a retry
            # can outlive the leadership it started under, and
            # maybe_launch_job does NOT gate; relaunching unconditionally
            # would double-run a Leader job against the new owner.
            if self._cluster_allows(job):
                # Settle the durable pending record BEFORE launching
                # (record-before-run, like the @reboot marker): a crash
                # after the launch must not re-arm the attempt that ran.
                # fail-closed defers on an unsettleable record; degrade
                # launches anyway. With cross-node retry resume active,
                # the decision serializes on the claim lease and
                # re-checks the newest ladder record is still OUR OWN
                # pending; a peer's claim ends it here ("abort") without
                # settling.
                decision = await self._retry_consume_decision(
                    job, retry_num, quiet=deferrals > 0
                )
                if decision == "launch":
                    break
                if decision == "abort":
                    state = self.retry_state.get(job_name)
                    if state is not None:
                        state.cancelled = True
                    self.retry_state.pop(job_name, None)
                    logger.warning(
                        "Cron job %s retry (#%i) dropped: another node "
                        "claimed this retry ladder (cross-node retry "
                        "resume); it fires there",
                        job_name,
                        retry_num,
                    )
                    return
            elif self._cluster_owner_moved(job):
                # ownership genuinely moved: end this node's retry
                # sequence (hand-off or forfeit; see _abandon_retry).
                self._abandon_retry(job, retry_num)
                return
            # A transient fail-closed denial: this node may still be the
            # rightful owner, and ending the sequence would end it
            # EVERYWHERE for an @reboot keep-alive (reboot_ran was
            # recorded, so no other node restarts it). Keep it alive and
            # re-check.
            state = self.retry_state.get(job_name)
            if state is None or state.cancelled or self._stop_event.is_set():
                # the sequence ended (success / cancellation / shutdown)
                # while we deliberated: nothing left to keep alive.
                return
            recheck = max(delay, RETRY_GATE_RECHECK_FLOOR)
            # first deferral at INFO, repeats at DEBUG (a long outage
            # with a tiny initialDelay would otherwise spam this line).
            log = logger.info if deferrals == 0 else logger.debug
            deferrals += 1
            log(
                "Cron job %s retry (#%i) deferred: the cluster does not "
                "currently allow this node to run it and no other node "
                "positively owns it; re-checking in %.1f seconds",
                job_name,
                retry_num,
                recheck,
            )
            await asyncio.sleep(recheck)
        # counted on the launch result (not where the retry is armed) so the
        # counter reports retries actually launched, net of cancellations,
        # abandonments, and a concurrencyPolicy=Forbid skip.
        if await self.maybe_launch_job(job):
            self.metrics.job_retry_launched(job_name)

    def _persist_retry_pending(
        self,
        job: JobConfig,
        attempt: int,
        not_before: datetime.datetime,
    ) -> Optional[asyncio.Task]:
        """Fire-and-forget append of a pending-retry record for ``job``.

        Carries the ABSOLUTE deadline and the job's config digest:
        everything a restart needs to re-arm the ladder. Returns the
        write task so the caller can ORDER later ladder writes after it
        (never to gate on its success).
        """
        if self.state_backend is None:
            self._note_retry_write_dropped(job.name, "pending")
            return None
        record = {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest_cached(job),
            # the arming node: on a shared store another node's boot must
            # neither re-arm this ladder (its owner is alive) nor settle it.
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        return self._queue_retry_write(job.name, record)

    def _persist_retry_settled(
        self, name: str, reason: str, attempt: Optional[int] = None
    ) -> None:
        """Fire-and-forget append of a settled-ladder record for ``name``.

        Whatever ended the ladder (success, supersession, exhaustion,
        abandonment, an invalidation at re-arm time) writes one of these on
        top of the stream so the next boot finds nothing pending.
        """
        if self.state_backend is None:
            self._note_retry_write_dropped(name, reason)
            return
        record: dict[str, Any] = {
            "kind": "settled",
            "reason": reason,
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        if attempt is not None:
            record["attempt"] = attempt
        self._queue_retry_write(name, record)

    def _note_retry_write_dropped(self, name: str, what: str) -> None:
        """Make a retry-ladder write dropped for want of a backend VISIBLE.

        Only when a ``state`` section is configured: a stale pending left
        newest could be resurrected but for the superseded-by-run guard;
        worth a counter and a line, never silence.
        """
        if not self._state_configured:
            return
        self.metrics.state_write_dropped("retry")
        logger.warning(
            "state: dropping retry-ladder record (%s) for %s: the state "
            "store is unavailable",
            what,
            name,
        )

    def _queue_retry_write(
        self, name: str, record: dict[str, Any]
    ) -> asyncio.Task:
        """Queue a retry-stream write ORDERED after the job's previous one.

        Newest-record-wins makes ordering load-bearing: an unordered
        pending/settle pair could land filename-inverted, leaving
        ``pending`` newest and resurrecting a consumed retry on the next
        boot.
        """
        return self._install_tail_task(
            self._retry_write_tail,
            name,
            lambda: self._append_retry_record(name, record),
            spawn=self._track_state_write,
        )

    async def _append_retry_record(
        self, name: str, record: dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:  # torn down between scheduling and running
            self._note_retry_write_dropped(name, str(record.get("kind")))
            return
        stream = self._retry_stream(name)
        try:
            await backend.append_record(
                stream, record, prune_keep=RETRY_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            self.metrics.state_write_dropped("retry")
            logger.warning(
                "state: failed to persist retry state for %s: %s", name, ex
            )

    def _persist_pause_record(self, name: str, record: dict[str, Any]) -> None:
        """Fire-and-forget append of one pause-stream record.

        Stamps the shared audit fields (``host`` is audit info ONLY: a
        pause is honored by every node sharing the store). Without a
        backend the record holds in memory; with a configured store it
        is held for replay (_defer_pause_write).
        """
        record["at"] = get_now(datetime.timezone.utc).isoformat()
        record["host"] = self._state_host
        if self.state_backend is None:
            self._defer_pause_write(name, record)
            return
        self._queue_pause_write(name, record)

    def _persist_pause(self, name: str, info: "PauseInfo") -> None:
        """Fire-and-forget append of a durable ``paused`` record."""
        self._persist_pause_record(
            name,
            {
                "kind": "paused",
                "since": info.since.isoformat(),
                "until": info.until.isoformat(),
                "note": info.note,
                "by": info.by,
                "channel": info.channel,
            },
        )

    def _persist_resume(self, name: str, by: str, channel: str) -> None:
        """Fire-and-forget append of a durable ``resumed`` record."""
        self._persist_pause_record(
            name, {"kind": "resumed", "by": by, "channel": channel}
        )

    def _defer_pause_write(self, name: str, record: dict[str, Any]) -> None:
        """Hold a pause record for replay once the store comes back.

        Dropping it would let the post-return refresh quietly revert the
        operator's pause (or resume) to the stale record still newest in
        the stream. A failed append against a live store buffers the same
        way. Newest-per-job wins, so a pause/resume pair during the
        outage collapses to the final intent. Stateless installs buffer
        nothing, and a buffer left from before the state section was
        removed is DISCARDED here. The generation bump keeps an in-flight
        refresh from applying its now-stale snapshot.
        """
        if not self._state_configured:
            self._pause_pending_writes.pop(name, None)
            return
        self._pause_gen[name] = self._pause_gen.get(name, 0) + 1
        self._pause_pending_writes[name] = record
        self.metrics.state_write_dropped("pause")
        logger.warning(
            "state: cannot write the %s of job %s; it holds in memory only "
            "and will be written when the store accepts it",
            record.get("kind"),
            name,
        )

    def _replay_pending_pause_writes(self) -> None:
        """Queue the pause records buffered by an outage or a failed write.

        Called from start_stop_state BEFORE the rehydrate's refresh pass
        and from every _pause_periodic pass. Both run it before the
        refresh, so each replayed write installs its _pause_write_tail
        entry first and the refresh leaves the job's memory alone.
        """
        pending = list(self._pause_pending_writes.items())
        self._pause_pending_writes.clear()
        for name, record in pending:
            logger.info(
                "state: writing the %s of job %s held in memory only",
                record.get("kind"),
                name,
            )
            self._queue_pause_write(name, record)

    def _queue_pause_write(
        self, name: str, record: dict[str, Any]
    ) -> asyncio.Task:
        """Queue a pause-stream write ORDERED after the job's previous one.

        The _queue_retry_write idiom (a resume racing its pause could
        land filename-inverted). The tail entry doubles as the "local
        write in flight" signal the housekeeping refresh consults; the
        generation bump is its edge-triggered half (see _pause_gen).
        """
        self._pause_gen[name] = self._pause_gen.get(name, 0) + 1
        return self._install_tail_task(
            self._pause_write_tail,
            name,
            lambda: self._append_pause_record(name, record),
            spawn=self._track_state_write,
        )

    async def _append_pause_record(
        self, name: str, record: dict[str, Any]
    ) -> None:
        backend = self.state_backend
        if backend is None:  # torn down between queueing and running
            self._defer_pause_write(name, record)
            return
        stream = self._pause_stream(name)
        try:
            await backend.append_record(
                stream, record, prune_keep=PAUSE_STREAM_KEEP
            )
        except Exception as ex:  # noqa: BLE001 - fire-and-forget; log, survive
            logger.warning(
                "state: failed to persist pause state for %s: %s", name, ex
            )
            # not just a lost audit row: the superseded record is still
            # newest, so the next refresh would revert the operator's
            # intent. Buffer and let housekeeping retry.
            self._defer_pause_write(name, record)

    async def _retry_consume_ok(
        self, job_name: str, retry_num: int, *, quiet: bool
    ) -> bool:
        """Settle the pending-retry record ahead of the launch; may defer.

        True -> launch. The settle is the record-before-run half of
        restart-durable retries: once it lands, a crash cannot re-arm the
        attempt about to run. When it cannot land, degrade launches
        anyway (at-least-once) and fail-closed defers like a closed
        cluster gate. Stateless is always True with no I/O.
        """
        backend = self.state_backend
        fail_closed = (
            self._state_configured
            and self._state_on_unavailable == "fail-closed"
        )
        if backend is None:
            if fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: the state store "
                        "is unavailable and onStoreUnavailable is "
                        "fail-closed",
                        job_name,
                        retry_num,
                    )
                return False
            return True
        # Order behind any in-flight ladder write (the pending append for
        # this very attempt, with a tiny/zero delay): the settle below must
        # sort newest. Bounded; a wedged earlier write only costs ordering,
        # and the superseded-by-run re-arm guard is the backstop.
        prev = self._retry_write_tail.get(job_name)
        if prev is not None and not prev.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(prev), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.TimeoutError:
                pass
        record = {
            "kind": "settled",
            "reason": "launched",
            "attempt": retry_num,
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
        }
        stream = self._retry_stream(job_name)
        try:
            # prune_keep rides the append, applied only after it LANDED,
            # so it cannot affect the settle decision below.
            await asyncio.wait_for(
                backend.append_record(
                    stream, record, prune_keep=RETRY_STREAM_KEEP
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - policy decides below
            self.metrics.state_write_dropped("retry")
            if fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: cannot settle "
                        "its durable record (%s) and onStoreUnavailable "
                        "is fail-closed",
                        job_name,
                        retry_num,
                        ex,
                    )
                return False
            logger.warning(
                "state: cannot settle the durable record for %s retry "
                "(#%i) (%s); launching anyway (a crash could replay this "
                "attempt after a restart)",
                job_name,
                retry_num,
                ex,
            )
            return True
        return True

    # --- cross-node retry resume -------------------------------------------

    def _retry_resume_active(self) -> bool:
        """Whether cross-node retry resume applies right now.

        Needs a SHARED store (other nodes can see the ladder records at
        all), leader election (ownership is what moves), and a live
        manager (the claim scan gates on ``_cluster_allows``).
        """
        backend = self.state_backend
        return (
            backend is not None
            and backend.supports_shared_locking()
            and self._elect_leader_configured
            and self.cluster_manager is not None
        )

    def _retry_cross_node_eligible(self, job: JobConfig) -> bool:
        """Whether ``job``'s retry ladder may move between nodes.

        ``EveryNode`` ladders are strictly per-node (every node runs its
        own copy; a foreign pending on the shared stream is another node's
        live ladder, exactly as in rehydration).  ``@reboot`` ladders are
        anchored to a HOST's boot (the re-arm validity is judged against
        this host's boot marker), so they never move either: an
        abandoned @reboot keep-alive still ends cluster-wide, as
        documented.
        """
        return (
            self._retry_resume_active()
            and job.clusterPolicy != "EveryNode"
            and not (
                isinstance(job.schedule, str) and job.schedule == "@reboot"
            )
        )

    @staticmethod
    def _retry_claim_lease(name: str) -> str:
        return RETRY_CLAIM_PREFIX + name

    async def _acquire_retry_claim(
        self,
        backend: StateBackend,
        job: JobConfig,
        retry_num: int,
        *,
        quiet: bool,
    ) -> Optional[Lease]:
        """``acquire_lease`` for a retry claim; timeout or store error
        maps to ``None`` (same containment as _acquire_slot_lease). An
        escape here drops the due retry AND re-raises outside run()'s
        try/except via cancel_job_retries, crashing the daemon.
        """
        try:
            return await asyncio.wait_for(
                backend.acquire_lease(
                    self._retry_claim_lease(job.name),
                    self._slot_holder(),
                    RETRY_CLAIM_TTL,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001 - flock ENOLCK/EIO/ESTALE on
            # a sick shared mount is as ambiguous as a timeout; the policy
            # fork (defer under fail-closed, unserialized proceed under
            # degrade) decides, never the exception.
            if not quiet:
                logger.warning(
                    "Cron job %s retry (#%i): the retry-claim store call "
                    "raised (%s); treating the claim as unanswered",
                    job.name,
                    retry_num,
                    ex,
                )
            return None

    async def _retry_consume_decision(
        self, job: JobConfig, retry_num: int, *, quiet: bool
    ) -> str:
        """Decide a due retry's fate: ``launch`` | ``defer`` | ``abort``.

        Without cross-node resume this is exactly _retry_consume_ok.
        With it, two additions close the claim/consume race: the consume
        serializes on the SAME per-job claim lease the scan uses, and
        the newest ladder record must still be one THIS host wrote; a
        foreign newest record means the ladder positively moved, and the
        only safe move is ``abort`` without settling (our settle on top
        would bury the claimer's pending). RETRY_CLAIM_GRACE cannot
        protect a gate-deferred owner (its re-check cadence is its own
        ladder delay), so this re-check is load-bearing for
        at-most-once. Read/acquire failures follow onStoreUnavailable:
        degrade proceeds unserialized, fail-closed defers.
        """
        if not self._retry_cross_node_eligible(job):
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        backend = self.state_backend
        if backend is None:
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        fail_closed = self._state_on_unavailable == "fail-closed"
        lease = await self._acquire_retry_claim(
            backend, job, retry_num, quiet=quiet
        )
        if lease is None:
            observed: Optional[Lease] = None
            try:
                observed = await asyncio.wait_for(
                    backend.read_lease(self._retry_claim_lease(job.name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - as ambiguous as a timeout;
                # observed stays None so the policy fork below decides.
                pass
            if observed is not None and observed.holder != self._slot_holder():
                # a live claimer is working this very ladder: defer and
                # re-check; if it claimed, the next pass aborts.
                return "defer"
            if observed is None and fail_closed:
                if not quiet:
                    logger.warning(
                        "Cron job %s retry (#%i) deferred: cannot "
                        "serialize with cross-node claims (store "
                        "unavailable) and onStoreUnavailable is "
                        "fail-closed",
                        job.name,
                        retry_num,
                    )
                return "defer"
            if observed is not None:
                lease = observed  # our own late-landing acquire: adopt it
        try:
            try:
                recs = await asyncio.wait_for(
                    backend.list_records(
                        self._retry_stream(job.name),
                        limit=1,
                        newest_first=True,
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - policy fork below
                if fail_closed:
                    return "defer"
                recs = []
            rec = recs[0] if recs else None
            if (
                rec is not None
                and isinstance(rec.get("host"), str)
                and rec.get("host") != self._state_host
            ):
                return "abort"
            ok = await self._retry_consume_ok(job.name, retry_num, quiet=quiet)
            return "launch" if ok else "defer"
        finally:
            if lease is not None:
                try:
                    await asyncio.wait_for(
                        backend.release_lease(lease),
                        timeout=STATE_OP_TIMEOUT,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - TTL is the fallback
                    pass

    async def _retry_claim_scan(self) -> None:
        """Scan for foreign retry ladders this node should resume.

        The cross-node half of restart-surviving retries: a pending record
        whose owner crashed (stale past its deadline plus
        :data:`RETRY_CLAIM_GRACE`) or a ``handoff`` record from an owner
        that positively relinquished is claimed (under a per-job TTL
        lease, with a re-read inside it) and re-armed locally exactly
        like rehydration re-arms this host's own pendings.  Spawned from
        the housekeeping pass about once a minute; every failure degrades
        to "not this pass".
        """
        if not self._retry_resume_active():
            return
        # Enumerate first, read second (the _refresh_pauses_from_store
        # shape): almost every retry stream does not exist, so one
        # enumeration beats a per-job read per minute per node. A failed
        # enumeration degrades to "not this pass".
        backend = self.state_backend
        if backend is None:
            names = list(self.cron_jobs)
        else:
            try:
                streams = await asyncio.wait_for(
                    backend.list_stream_names(RETRY_STREAM_PREFIX),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - not this pass
                return
            names = [stream[len(RETRY_STREAM_PREFIX) :] for stream in streams]
        for name in names:
            job = self.cron_jobs.get(name)
            if job is None:
                continue  # a removed job's stream; GC's business
            try:
                await self._maybe_claim_retry(name, job)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one job must not end the scan
                logger.exception(
                    "state: error scanning job %s for a claimable retry",
                    name,
                )

    async def _newest_retry_record(
        self, backend: StateBackend, name: str
    ) -> dict[str, Any] | None:
        """The newest retry-ladder record, or None.

        None for an empty stream AND for a failed/timed-out read: the
        claim flows treat both as "not this pass" (never settle or claim
        on a read they could not complete).
        """
        try:
            recs = await asyncio.wait_for(
                backend.list_records(
                    self._retry_stream(name), limit=1, newest_first=True
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - not this pass
            return None
        return recs[0] if recs else None

    async def _maybe_claim_retry(self, name: str, job: JobConfig) -> None:
        backend = self.state_backend
        if backend is None or not self._retry_cross_node_eligible(job):
            return
        if not job.enabled or not job.onFailure["retry"]["maximumRetries"]:
            return
        if self.running_jobs.get(name):
            return
        state = self.retry_state.get(name)
        if state is not None and (state.task is not None or state.count > 0):
            # a live local ladder outranks; a count-0, taskless leftover
            # (a slot-denied scheduled fire armed it and never launched)
            # does not block a claim.
            return
        if not self._cluster_allows(job):
            return
        rec = await self._newest_retry_record(backend, name)
        if rec is None:
            return
        claimable = self._retry_record_claimable(name, job, rec)
        if claimable is None:
            return
        attempt, not_before = claimable
        lease: Optional[Lease] = None
        try:
            lease = await asyncio.wait_for(
                backend.acquire_lease(
                    self._retry_claim_lease(name),
                    self._slot_holder(),
                    RETRY_CLAIM_TTL,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            lease = None
        if lease is None:
            return  # a rival claimer or a sick store: next scan retries
        try:
            claimed = await self._claim_retry_under_lease(
                name, job, rec, attempt, not_before
            )
        finally:
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - TTL is the fallback
                pass
        if not claimed:
            return
        # Re-apply the top-guard invariant: during the awaits above a
        # scheduled fire could have armed a LIVE local ladder.
        # Overwriting retry_state[name] would strand that task as a
        # second same-node ladder (same host, so the foreign-record
        # abort never fires) and double-fire on ONE node. The live
        # ladder outranks; drop the just-made claim (its durable pending
        # is host-local and the live ladder settles it on consume).
        existing = self.retry_state.get(name)
        if self.running_jobs.get(name) or (
            existing is not None
            and (existing.task is not None or existing.count > 0)
        ):
            logger.info(
                "Job %s: dropping a just-made retry claim; a local retry "
                "ladder was armed while claiming (it supersedes)",
                name,
            )
            return
        retry = job.onFailure["retry"]
        state = JobRetryState(
            retry["initialDelay"],
            retry["backoffMultiplier"],
            retry["maximumDelay"],
        )
        for _ in range(attempt):
            state.next_delay()
        now = get_now(datetime.timezone.utc)
        remaining = max(0.0, (not_before - now).total_seconds())
        self.retry_state[name] = state
        state.task = asyncio.create_task(
            self.schedule_retry_job(name, remaining, attempt)
        )
        logger.info(
            "Job %s: claimed pending retry #%d from host %s (cross-node "
            "retry resume); due in %.1f seconds",
            name,
            attempt,
            rec.get("host") or rec.get("fromHost"),
            remaining,
        )

    async def _claim_retry_under_lease(
        self,
        name: str,
        job: JobConfig,
        rec: dict[str, Any],
        attempt: int,
        not_before: datetime.datetime,
    ) -> bool:
        """The claim's critical section: re-check, validate, append.

        Runs while holding the per-job claim lease.  ``True`` means the
        claim record landed and the caller may arm the local ladder.
        """
        backend = self.state_backend
        if backend is None:
            return False
        # re-read under the lease: the record must not have moved.
        recheck = await self._newest_retry_record(backend, name)
        if recheck is None or recheck != rec:
            return False
        # superseded-by-run against the DURABLE ledger (the resolving run
        # likely happened on ANOTHER host). A handoff carries the arm
        # time in armedAt; a pending's own ``at`` is its arm time.
        armed_at = rec.get("armedAt") or rec.get("at") or rec.get("notBefore")
        try:
            # run-only watermark: pause-skip rows must not count here
            # (see durable_last_completed_at).
            last_durable = await asyncio.wait_for(
                self.durable_last_completed_at(name),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - ambiguity settles: no claim
            return False
        if (
            isinstance(last_durable, str)
            and isinstance(armed_at, str)
            and last_durable > armed_at
        ):
            # a run finished after the ladder was armed: it resolved (its
            # settle may have been dropped). Settle it here so the scan
            # stops revisiting it; no-run beats double-run.
            self._persist_retry_settled(name, "superseded-by-run", attempt)
            return False
        claim = {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before.isoformat(),
            "jobDigest": job_digest_cached(job),
            "host": self._state_host,
            "at": get_now(datetime.timezone.utc).isoformat(),
            "claimedFrom": rec.get("host") or rec.get("fromHost"),
        }
        write = self._queue_retry_write(name, claim)
        try:
            # the claim record must LAND before the lease is released, or
            # a rival's post-release re-read could still see the old
            # record and claim it too.
            await asyncio.wait_for(
                asyncio.shield(write), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.TimeoutError:
            # Abandoning the claim: without this cancel the shielded
            # write could land LATER as an own-host pending that nothing
            # re-arms while a live original owner aborts its ladder (an
            # unreclaimable orphan). Cancel so the foreign record stays
            # newest and the next scan can re-claim cleanly; if the
            # append already completed the cancel is a harmless no-op.
            write.cancel()
            return False
        return True

    def _retry_record_claimable(
        self, name: str, job: JobConfig, rec: dict[str, Any]
    ) -> Optional[tuple[int, datetime.datetime]]:
        """Judge whether a ladder record is another node's claimable retry.

        Mirrors _validate_pending_retry's checks with the cross-node
        rules on top: only a FOREIGN pending stale past
        RETRY_CLAIM_GRACE (a crashed owner) or a handoff (positively
        relinquished; no grace) qualifies. Every failure just declines:
        settling another host's record on local suspicion would race its
        live owner; the durable superseded-by-run check happens under
        the claim lease instead.
        """
        kind = rec.get("kind")
        if kind not in ("pending", "handoff"):
            return None
        parsed = _parse_retry_record(rec)
        if parsed is None:
            return None
        attempt, not_before, armed_at = parsed
        if rec.get("jobDigest") != job_digest_cached(job):
            return None
        retry = job.onFailure["retry"]
        maximum = retry["maximumRetries"]
        if maximum != -1 and attempt > maximum:
            return None
        now = get_now(datetime.timezone.utc)
        deadline = job.startingDeadlineSeconds
        if deadline and (now - not_before).total_seconds() > deadline:
            return None
        # the newest ACTUAL run (see _validate_pending_retry): a pause-held
        # slot's "skipped" row is not evidence anything ran.
        last_at = self._last_completed_at.get(name)
        if last_at is not None and last_at > armed_at:
            return None  # locally-known newer run; the ladder resolved
        if kind == "handoff":
            return attempt, max(not_before, now)
        host = rec.get("host")
        if not isinstance(host, str) or host == self._state_host:
            # our own pending is rehydration's business, never the scan's
            return None
        due_anchor = max(not_before, armed_at)
        if (now - due_anchor).total_seconds() <= RETRY_CLAIM_GRACE:
            return None
        return attempt, not_before

    def _cluster_owner_moved(self, job: JobConfig) -> bool:
        """Whether another node is *positively* identified as ``job``'s owner.

        Tells a genuine ownership move (retry may be abandoned) from a
        transient fail-closed denial of _cluster_allows (where abandoning
        would end the sequence for good). Decided from self-recognising
        is_available_* reads, never a display-name comparison.
        """
        mgr = self.cluster_manager
        if mgr is None:
            return False  # election fails closed here; nobody else owns it
        try:
            if mgr.has_conflict():
                # the election is unsafe while nodes conflict: nobody is
                # positively the owner, so treat the denial as transient
                return False
            if not mgr.is_quorate():
                # no trustworthy view of leadership -> no positive owner
                return False
            if not mgr.view_settled():
                # a freshly rebuilt gossip manager holds the available_*
                # gates closed while peers re-attest it, even on the
                # rightful owner; a False is then the hold, not a move.
                # Bounded (~2 poll intervals), so defer and re-check.
                return False
            if mgr.distribution == "spread":
                return not mgr.is_available_job_owner(job.name)
            return not mgr.is_available_leader()
        except Exception:
            # mirrors _cluster_allows: a backend read error is a transient
            # fail-closed condition, never a confirmed ownership move.
            logger.exception(
                "cluster: error checking whether ownership of job %s moved; "
                "treating the denial as transient",
                job.name,
            )
            return False

    def _abandon_retry(self, job: JobConfig, retry_num: int) -> None:
        """End a pending retry sequence whose job's ownership moved off-node.

        Marks the state cancelled BEFORE dropping it: a RunningJob that
        captured this JobRetryState could otherwise re-arm an orphan
        ladder cancel_job_retries can never find. With cross-node retry
        resume active the ladder is HANDED OFF instead of settled dead
        (a handoff record the new owner's claim scan picks up, no
        staleness grace); no cancelled run-history record on that path:
        the attempt is moving, not ending.
        """
        job_name = job.name
        state = self.retry_state.get(job_name)
        if state is not None:
            state.cancelled = True
        self.retry_state.pop(job_name, None)
        if self._retry_cross_node_eligible(job):
            now = get_now(datetime.timezone.utc)
            # Anchor the new owner's superseded-by-run guard on when the
            # attempt was ARMED, not the hand-off instant: a peer may
            # have already claimed and RUN it, and a now-stamped anchor
            # would double-fire across failover. notBefore stays now so
            # an unresolved ladder still runs promptly.
            armed_at = state.armed_at if state is not None else None
            self._queue_retry_write(
                job_name,
                {
                    "kind": "handoff",
                    "attempt": retry_num,
                    "notBefore": now.isoformat(),
                    "jobDigest": job_digest_cached(job),
                    "fromHost": self._state_host,
                    "at": now.isoformat(),
                    "armedAt": (
                        armed_at.isoformat()
                        if armed_at is not None
                        else now.isoformat()
                    ),
                },
            )
            logger.warning(
                "Cron job %s retry (#%i) handed off: the cluster moved "
                "ownership of it to another node; the new owner resumes "
                "the ladder from its durable record (cross-node retry "
                "resume)",
                job_name,
                retry_num,
            )
            return
        # settle the durable ladder: re-arming this attempt on OUR next
        # boot would be the cross-node double-run abandonment avoids.
        self._persist_retry_settled(job_name, "owner-moved", retry_num)
        # Wording: the new owner picks up future SCHEDULED firings only;
        # the message must not promise the job "runs elsewhere".
        logger.warning(
            "Cron job %s retry (#%i) abandoned: the cluster moved ownership "
            "of it to another node; onPermanentFailure will not fire for "
            "this sequence, this attempt is not re-run elsewhere, and any "
            "future scheduled firings happen on the new owner (an @reboot "
            "one-shot has none)",
            job_name,
            retry_num,
        )
        # Record the abandonment in run history, like a web-UI
        # cancellation. No RunningJob exists here, so no report hook or
        # statsd metric fires; the record and WARNING are the trace.
        output = JobOutputStream()
        output.close()
        self._record_run(
            job_name,
            JobRunInfo(
                outcome="cancelled",
                exit_code=None,
                started_at=None,
                finished_at=get_now(datetime.timezone.utc),
                fail_reason="retry abandoned: cluster ownership moved to "
                "another node",
                output=output,
            ),
        )

    async def handle_job_success(self, job: RunningJob) -> None:
        await self.cancel_job_retries(job.config.name, settle="succeeded")
        await job.report_success()

    @staticmethod
    def _reap_retry_task(name: str, task: "asyncio.Task[None]") -> None:
        """Retrieve (never re-raise) a finished retry task's outcome.

        Both awaiters (here and in ``handle_job_failure``) run on launch/
        finish paths outside ``run()``'s try/except, so re-raising an
        exception stored in a dead retry task would crash the whole
        scheduler.  ``.exception()`` also marks the exception retrieved,
        silencing the event loop's "never retrieved" report.
        """
        if task.cancelled():
            return
        ex = task.exception()
        if ex is not None:
            logger.error(
                "Cron job %s: its retry task died with an unexpected "
                "error; that pending retry was lost",
                name,
                exc_info=ex,
            )

    async def cancel_job_retries(
        self, name: str, *, settle: Optional[str] = "superseded"
    ) -> None:
        try:
            state = self.retry_state.pop(name)
        except KeyError:
            return
        state.cancelled = True
        # Settle the durable ladder record so a pending retry is not
        # re-armed on the next boot. Skipped when settle is None (the
        # graceful-shutdown path: surviving the restart is the point) and
        # when count == 0 (nothing durable was written).
        if settle is not None and state.count > 0:
            self._persist_retry_settled(name, settle, state.count)
        if state.task is not None:
            if state.task.done():
                self._reap_retry_task(name, state.task)
            else:
                state.task.cancel()
