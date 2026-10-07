#!/usr/bin/env python3
"""Performance benchmark suite for cronstable.

The suite measures the paths that determine how cronstable feels on small
machines: process startup, import cost, cron expression parsing and next-fire
search, config parsing, schedule seeding at 100k-job scale, DAG graph
construction and planning, durable-state I/O, JSON, fingerprinting, redaction,
calendar rendering, and memory footprint.

The harness is stdlib-only and imports cronstable from whichever interpreter
runs it, so the same script (from the current checkout) can benchmark an older
installed release for a paired comparison: any benchmark whose API that
version lacks is recorded as skipped, never failed.  Results are written as a
JSON document consumed by benchmarks/compare.py.

Usage:
    python benchmarks/bench.py --json out.json      # full suite (CI)
    python benchmarks/bench.py --quick              # roughly 10x smaller
    python benchmarks/bench.py --smoke              # minimal, for unit tests
    python benchmarks/bench.py --only cronexpr      # substring filter
    python benchmarks/bench.py --tier inprocess     # skip the subprocess tier
    python benchmarks/bench.py --list               # list benchmarks

Every timed benchmark returns the wall-clock seconds of a fixed workload
(lower is better); memory benchmarks return MB.  Per-benchmark repeats give
the distribution; compare.py uses each metric's declared estimator ("min" for
time, "median" for memory) so one noisy repeat cannot fake a regression.

To keep that estimator honest the harness works to lower the measurement
noise floor: it runs untimed warm-up passes before the measured repeats, and
(best-effort) pins itself to one CPU and raises its priority to cut scheduling
jitter.  Benchmarks split into two tiers -- the fast in-process metrics and
the noisier subprocess metrics (cold start, import, peak RSS) -- selectable
with --tier so CI can give each its own round count.
"""

import argparse
import atexit
import base64
import gc
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from datetime import datetime, timedelta, timezone

SCHEMA = 1

# Workload scale and repeat column per mode: full is the CI configuration,
# quick is for local iteration, smoke keeps the unit test under a few seconds.
_MODES = {"full": (1.0, 0), "quick": (0.1, 1), "smoke": (0.01, 2)}
_MODE = "full"

# Untimed warm-up passes per mode, discarded before the measured repeats: they
# page in code and data and let the CPU reach a steady clock so first-call
# effects never enter the distribution.  Smoke runs none, keeping the unit test
# fast.  Overridable with --warmup.
_WARMUPS = {"full": 1, "quick": 1, "smoke": 0}
_WARMUP_OVERRIDE = None


def _scale() -> float:
    return _MODES[_MODE][0]


def _n(base: int, floor: int = 1) -> int:
    return max(floor, int(base * _scale()))


def _reps(spec) -> int:
    return spec[_MODES[_MODE][1]]


def _warmups() -> int:
    if _WARMUP_OVERRIDE is not None:
        return _WARMUP_OVERRIDE
    return _WARMUPS[_MODE]


class Skip(Exception):
    """Raised by a benchmark that cannot run in this environment."""


_BENCHMARKS = []
_FIX = {}
_FIX_FINAL = {}
_SESSION_TMP = None
_SRC_FALLBACK = None


def _ensure_importable():
    """Prefer the installed cronstable; fall back to the source checkout.

    In CI each side runs from its own venv, where cronstable is installed and
    this is a no-op.  Running the script straight from a checkout without an
    install would otherwise skip every in-process benchmark (a script's
    sys.path[0] is benchmarks/, not the repo root).
    """
    global _SRC_FALLBACK
    try:
        import cronstable  # noqa: F401

        return
    except ImportError:
        pass
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.path.isdir(os.path.join(root, "cronstable")):
        sys.path.insert(0, root)
        _SRC_FALLBACK = root
        print(
            "note: cronstable is not installed in this interpreter; "
            "benchmarking the source tree at %s" % root,
            file=sys.stderr,
        )


def _measured_package():
    """The directory of the cronstable package this run imports.

    An interpreter with cronstable installed measures that copy even when
    the script runs from a checkout, so each run names what it measured.
    """
    try:
        import cronstable
    except ImportError:
        return None
    return os.path.dirname(os.path.abspath(cronstable.__file__))


def _tmpdir() -> str:
    global _SESSION_TMP
    if _SESSION_TMP is None:
        _SESSION_TMP = tempfile.mkdtemp(prefix="cronstable-bench-")
        atexit.register(shutil.rmtree, _SESSION_TMP, ignore_errors=True)
    return _SESSION_TMP


def fixture(name, builder, finalizer=None):
    """Build-once shared setup, excluded from every timed region.

    ``finalizer`` is called with the built value when the harness evicts the
    group's fixtures (group boundary and end of suite).  It is for fixtures
    that hold external state a plain drop-the-reference eviction cannot
    release: the Playwright fixture parks a RUNNING event loop on the harness
    thread between sync-API calls, and only its finalizer (pw.stop()) frees
    the thread for the asyncio.run() benchmarks in later groups.  Finalizers
    must be idempotent: the atexit safety net may call them again.
    """
    if name not in _FIX:
        _FIX[name] = builder()
        if finalizer is not None:
            _FIX_FINAL[name] = finalizer
    return _FIX[name]


def _evict_fixtures(group):
    """Finalize and drop every cached fixture, then audit loop hygiene.

    Groups are the eviction boundary (see the caller in main()), so this is
    also where the harness thread must be clean again.  A fixture that parks
    a running event loop and lacks a finalizer would otherwise silently skip
    every later asyncio.run() benchmark, on BOTH sides of the paired CI
    comparison, which a release run then fails as dead gates (the 1.2.31
    webui/Playwright incident).  Failing here instead names the offending
    group while the innocent downstream benchmarks are still unharmed.
    """
    import asyncio

    for name, fin in _FIX_FINAL.items():
        try:
            fin(_FIX[name])
        except Exception as exc:
            print(
                "note: fixture %r finalizer failed: %r" % (name, exc),
                file=sys.stderr,
            )
    _FIX_FINAL.clear()
    _FIX.clear()
    gc.collect()
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise SystemExit(
        "fixture hygiene: group %r left an event loop running on the "
        "harness thread after eviction; every benchmark calling "
        "asyncio.run() after this group would silently skip. Give the "
        "offending fixture a finalizer that shuts the loop down (see "
        "_web_page)." % group
    )


def bench(
    name,
    group,
    detail="",
    unit="s",
    gate_pct=15.0,
    gate_floor=0.010,
    compare="min",
    repeats=(5, 2, 1),
    info=False,
    subprocess=False,
):
    """Register a benchmark.  The function returns one measured value.

    ``subprocess=True`` marks a benchmark that measures a child process (cold
    start, import, peak RSS): it belongs to the noisier subprocess tier, which
    ``--tier`` can select on its own so CI can run it with its own round count.

    The default ``gate_pct`` (15%) suits the deterministic in-process compute
    metrics, which are rock-steady across the five CI rounds; the noisier tiers
    (subprocess process-spawn, real-disk state I/O, peak-RSS) set a looser
    limit of their own so ordinary jitter never trips them.  A regression must
    also clear the measured noise band regardless (see compare.py), so a tight
    percentage does not mean a jumpy gate.
    """

    def deco(fn):
        _BENCHMARKS.append(
            {
                "name": name,
                "group": group,
                "detail": detail,
                "unit": unit,
                "gate_pct": None if info else gate_pct,
                "gate_floor": gate_floor,
                "compare": compare,
                "repeats": repeats,
                "info": info,
                "subprocess": subprocess,
                "fn": fn,
            }
        )
        return fn

    return deco


# ---------------------------------------------------------------------------
# Shared workload generators (deterministic; no randomness, no clock reads
# inside timed regions beyond the measured work itself).
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 15, 12, 30, 45, tzinfo=timezone.utc)
_NAIVE = datetime(2026, 7, 18, 12, 30)

_SIMPLE_EXPRS = [
    "* * * * *",
    "*/5 * * * *",
    "0 * * * *",
    "15 3 * * *",
    "0 9 * * 1-5",
    "30 6 1 * *",
    "0 0 * * 0",
    "45 23 * * 6",
]

_COMPLEX_EXPRS = [
    "*/7 8-18 * * 1-5",
    "0,15,30,45 */2 1,15 * *",
    "5 4 L * *",
    "0 12 15W * *",
    "0 8 * * 1#2",
    "0 22 * * L5",
    "30 2 * 1,4,7,10 *",
    "0 0 1 1 * 2030",
    "*/30 * * * * * *",
    "H H(2-5) * * *",
    "H/15 * * * *",
]


# Step values that divide the minute field's span evenly, so generated
# schedules are lint-clean (a lint finding per job would flood the log and
# add unrepresentative logging cost to config benchmarks).
_EVEN_STEPS = (2, 3, 4, 5, 6, 10, 12, 15, 20, 30)


def _varied_exprs(n):
    """A deterministic mix of realistic 5-field schedules (no H, no L/W,
    valid for classic crontab lowering too)."""
    out = []
    for i in range(n):
        r = i % 10
        if r < 4:
            out.append("%d %d * * *" % (i % 60, (i * 7) % 24))
        elif r < 6:
            out.append("*/%d * * * *" % _EVEN_STEPS[i % len(_EVEN_STEPS)])
        elif r < 8:
            out.append("%d 8-18 * * 1-5" % (i % 60))
        else:
            out.append("%d %d 1,15 * *" % (i % 60, (i * 3) % 24))
    return out


def _crontab_cls():
    try:
        from cronstable.cronexpr import CronTab
    except ImportError as exc:  # pragma: no cover
        raise Skip("cronstable.cronexpr unavailable: %r" % exc) from None
    return CronTab


def _parse_tabs(exprs):
    CronTab = _crontab_cls()
    return [CronTab(e, hash_key="job-%d" % i) for i, e in enumerate(exprs)]


def _config_yaml(n_jobs):
    lines = ["jobs:"]
    for i, expr in enumerate(_varied_exprs(n_jobs)):
        lines.append("  - name: job%05d" % i)
        lines.append("    command: echo job%05d" % i)
        lines.append('    schedule: "%s"' % expr)
        if i % 3 == 0:
            lines.append("    captureStdout: true")
    lines.append("")
    return "\n".join(lines)


def _config_path(n_jobs):
    path = os.path.join(_tmpdir(), "bench-config-%d.yaml" % n_jobs)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(_config_yaml(n_jobs))
    return path


def _job_dicts(n):
    return [
        {"name": "job%05d" % i, "command": "true", "schedule": expr}
        for i, expr in enumerate(_varied_exprs(n))
    ]


def _job_configs(n):
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    return [
        JobConfig(mergedicts(DEFAULT_CONFIG, raw)) for raw in _job_dicts(n)
    ]


def _schedule_entries(n):
    try:
        from cronstable.croninfo import ScheduleEntry
    except ImportError as exc:
        raise Skip("croninfo.ScheduleEntry unavailable: %r" % exc) from None
    CronTab = _crontab_cls()
    entries = []
    for i in range(n):
        if i % 2 == 0:
            expr = "%d * * * *" % (i % 60)  # hourly
        else:
            expr = "%d %d * * *" % (i % 60, (i * 7) % 24)  # daily
        entries.append(ScheduleEntry("job%05d" % i, CronTab(expr), None))
    return entries


# ---------------------------------------------------------------------------
# startup: cold process starts, timed as real subprocess wall clock.
# ---------------------------------------------------------------------------


def _child_env():
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    if _SRC_FALLBACK:
        prior = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            _SRC_FALLBACK + os.pathsep + prior if prior else _SRC_FALLBACK
        )
    return env


def _timed_child(args):
    t0 = time.perf_counter()
    # cwd is a neutral temp dir so the child resolves cronstable from its
    # interpreter's site-packages, never from a checkout it happens to sit
    # in.  In the paired CI run the old side's children must import the old
    # release, not the repo working tree.
    proc = subprocess.run(
        [sys.executable] + args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=_child_env(),
        cwd=_tmpdir(),
    )
    dt = time.perf_counter() - t0
    if proc.returncode != 0:
        raise Skip("child exited %d: %s" % (proc.returncode, " ".join(args)))
    return dt


@bench(
    "startup.python_baseline",
    "startup",
    detail="python -c pass",
    repeats=(40, 5, 1),
    info=True,
    subprocess=True,
)
def bench_python_baseline():
    return _timed_child(["-c", "pass"])


@bench(
    "startup.version",
    "startup",
    detail="cronstable --version",
    gate_pct=25.0,
    repeats=(40, 5, 2),
    subprocess=True,
)
def bench_startup_version():
    return _timed_child(["-m", "cronstable", "--version"])


@bench(
    "startup.import_cronexpr",
    "startup",
    detail="import cronstable.cronexpr",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_import_cronexpr():
    return _timed_child(["-c", "import cronstable.cronexpr"])


@bench(
    "startup.import_config",
    "startup",
    detail="import cronstable.config",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_import_config():
    return _timed_child(["-c", "import cronstable.config"])


@bench(
    "startup.import_daemon",
    "startup",
    detail="import cronstable.cron (full daemon graph)",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_import_daemon():
    return _timed_child(["-c", "import cronstable.cron"])


@bench(
    "startup.validate_config_100",
    "startup",
    detail="cronstable --validate-config, 100 jobs",
    gate_pct=25.0,
    repeats=(8, 2, 1),
    subprocess=True,
)
def bench_validate_config():
    path = _config_path(_n(100))
    return _timed_child(["-m", "cronstable", "-c", path, "--validate-config"])


@bench(
    "startup.job_set_id_100",
    "startup",
    detail="cronstable --job-set-id, 100 jobs",
    gate_pct=25.0,
    repeats=(8, 2, 1),
    subprocess=True,
)
def bench_job_set_id_cli():
    path = _config_path(_n(100))
    return _timed_child(["-m", "cronstable", "-c", path, "--job-set-id"])


@bench(
    "startup.daemon_first_pass_2k",
    "startup",
    detail="process start to the end of the first idle pass, 2k-job config",
    gate_pct=25.0,
    repeats=(2, 1, 1),
    subprocess=True,
)
def bench_startup_daemon_first_pass():
    """How long a restarted daemon takes to reach its first scheduling pass.

    Wall clock of a child that imports the daemon, loads a 2k-job config
    file and completes one pass of Cron.run(): the import, the parse, the
    reload that applies the job set, the schedule seeding and the first
    housekeeping.  The import and the parse have metrics of their own; the
    boot work between them and the first pass has none, and a slow step
    there delays every job after every restart.
    """
    return _timed_child(_daemon_child_args(_n(2000, floor=20), 1))


def _timed_client_child(args, extra_env=None):
    """_timed_child for a client command of a running daemon.

    The child reads end-of-file on stdin, which is how the MCP bridge is
    told to stop, and gets ``extra_env`` on top of an environment cleared
    of the developer's own CRONSTABLE_STATE_* and CRONSTABLE_WEB_*
    settings, so it talks to the benchmark's endpoint or to none.
    """
    env = _child_env()
    for name in list(env):
        if name.startswith(("CRONSTABLE_STATE_", "CRONSTABLE_WEB_")):
            del env[name]
    env.update(extra_env or {})
    t0 = time.perf_counter()
    proc = subprocess.run(
        [sys.executable] + args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=_tmpdir(),
    )
    dt = time.perf_counter() - t0
    if proc.returncode != 0:
        raise Skip("child exited %d: %s" % (proc.returncode, " ".join(args)))
    return dt


def _state_stub_server():
    """A loopback HTTP server that answers the job client's key read.

    Returns ``(server, thread, requests)``; ``requests`` collects one
    ``(path, authorization)`` pair per request served.
    """

    def build():
        import http.server
        import threading

        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append((self.path, self.headers.get("Authorization")))
                body = b'{"value": 1}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="bench-state-stub",
            daemon=True,
        )
        thread.start()
        return server, thread, requests

    def close(built):
        server, thread, _requests = built
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    return fixture("state_stub_server", build, finalizer=close)


@bench(
    "startup.jobcli_state_get",
    "startup",
    detail="cronstable state get, against a loopback stub endpoint",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_startup_jobcli_state_get():
    """The cold start a script inside a job pays for every state call.

    ``cronstable state get`` builds the whole argument parser, imports the
    job client with urllib, http.client and ssl, and makes one request.
    startup.version exits before the parser exists and nothing else
    imports the client, so a heavier parser or an import that leaks into
    this path shows up here and nowhere else.  The endpoint is a stub
    thread in the harness; the child is the real command.
    """
    server, _thread, requests = _state_stub_server()
    served = len(requests)
    dt = _timed_client_child(
        ["-m", "cronstable", "state", "get", "bench-key"],
        {
            "CRONSTABLE_STATE_URL": "http://127.0.0.1:%d"
            % server.server_address[1],
            "CRONSTABLE_STATE_TOKEN": "bench-token",
        },
    )
    new = requests[served:]
    if (
        len(new) != 1
        or "key=bench-key" not in new[0][0]
        or new[0][1] != "Bearer bench-token"
    ):
        raise RuntimeError("state get made the requests %r" % (new,))
    return dt


@bench(
    "startup.mcp_bridge",
    "startup",
    detail="cronstable mcp, stdin at end-of-file",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_startup_mcp_bridge():
    """The start of the stdio bridge an MCP host spawns per session.

    With stdin at end-of-file the bridge resolves its token and TLS
    posture, builds its opener, reads no frame and exits 0 without a
    request, so this is its import graph and setup alone.
    """
    return _timed_client_child(["-m", "cronstable", "mcp"])


@bench(
    "startup.import_tui",
    "startup",
    detail="import cronstable.tui",
    gate_pct=25.0,
    repeats=(12, 3, 1),
    subprocess=True,
)
def bench_startup_import_tui():
    """The terminal dashboard's own module, before its first request.

    The module is the largest in the package and defers aiohttp to the
    first API call, so this times what ``cronstable tui`` pays before it
    can draw, and catches a module-scope import that undoes the deferral.
    """
    return _timed_child(["-c", "import cronstable.tui"])


# ---------------------------------------------------------------------------
# cronexpr: the scheduling engine itself.
# ---------------------------------------------------------------------------


@bench(
    "cronexpr.parse_simple",
    "cronexpr",
    detail="parse 20k plain 5-field expressions",
)
def bench_parse_simple():
    CronTab = _crontab_cls()
    n = _n(20000)
    exprs = [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(n)]
    t0 = time.perf_counter()
    for e in exprs:
        CronTab(e)
    return time.perf_counter() - t0


@bench(
    "cronexpr.parse_complex",
    "cronexpr",
    detail="parse 5k expressions with ranges/steps/L/W/#/H/seconds",
)
def bench_parse_complex():
    CronTab = _crontab_cls()
    n = _n(5000)
    exprs = [_COMPLEX_EXPRS[i % len(_COMPLEX_EXPRS)] for i in range(n)]
    t0 = time.perf_counter()
    for i, e in enumerate(exprs):
        CronTab(e, hash_key="job-%d" % i)
    return time.perf_counter() - t0


@bench(
    "cronexpr.next_simple",
    "cronexpr",
    detail="next() over 20k pre-parsed plain tabs",
)
def bench_next_simple():
    tabs = fixture(
        "tabs_simple_20k",
        lambda: _parse_tabs(
            [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(_n(20000))]
        ),
    )
    t0 = time.perf_counter()
    for tab in tabs:
        tab.next(_NOW)
    return time.perf_counter() - t0


@bench(
    "cronexpr.next_complex",
    "cronexpr",
    detail="next() over 5k pre-parsed complex tabs",
)
def bench_next_complex():
    tabs = fixture(
        "tabs_complex_5k",
        lambda: _parse_tabs(
            [_COMPLEX_EXPRS[i % len(_COMPLEX_EXPRS)] for i in range(_n(5000))]
        ),
    )
    t0 = time.perf_counter()
    for tab in tabs:
        tab.next(_NOW)
    return time.perf_counter() - t0


@bench(
    "cronexpr.occurrences_1k",
    "cronexpr",
    detail="enumerate 1k fires from 8 generators",
)
def bench_occurrences():
    from itertools import islice

    tabs = fixture("tabs_occ", lambda: _parse_tabs(_SIMPLE_EXPRS))
    count = _n(1000)
    start = _NOW
    t0 = time.perf_counter()
    for tab in tabs:
        for _ in islice(tab.occurrences(start), count):
            pass
    return time.perf_counter() - t0


# Rescaled (id bumped from the legacy cronexpr.test_match, which was ~7ms and
# noise-dominated): ten passes over the 20k fixture put the timed region near
# 70ms so scheduler/GC jitter is a small fraction.  The id carries the new
# scale so the name keeps meaning one workload across releases; see
# benchmarks/README.md.
@bench(
    "cronexpr.test_match_200k",
    "cronexpr",
    detail="test() one instant against 20k tabs, 10 passes (200k matches)",
)
def bench_test_match():
    tabs = fixture(
        "tabs_simple_20k",
        lambda: _parse_tabs(
            [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(_n(20000))]
        ),
    )
    t0 = time.perf_counter()
    for _ in range(10):
        for tab in tabs:
            tab.test(_NAIVE)
    return time.perf_counter() - t0


def _zoneinfo_ny():
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    except ImportError as exc:  # pragma: no cover - stdlib since 3.9
        raise Skip("zoneinfo unavailable: %r" % exc) from None
    try:
        return ZoneInfo("America/New_York")
    except ZoneInfoNotFoundError as exc:
        raise Skip("tzdata absent: %r" % exc) from None


def _dst_window_exprs(n):
    """Deterministic schedules whose fires cluster in the DST changeover
    window (01:00-03:59), so an aware search does real work near the
    transition instead of skipping clean over it."""
    out = []
    for i in range(n):
        r = i % 4
        minute = i % 60
        if r == 0:
            out.append("%d %d * * *" % (minute, 1 + i % 3))
        elif r == 1:
            out.append(
                "*/%d 1-3 * * *" % _EVEN_STEPS[i % len(_EVEN_STEPS)]
            )
        elif r == 2:
            out.append("%d 2 * * 0" % minute)  # Sunday 02:xx: changeover hour
        else:
            out.append("%d %d * 3,11 *" % (minute, i % 6))
    return out


@bench(
    "cronexpr.next_dst_2k",
    "cronexpr",
    detail="aware next() under real DST transitions, 2k zoned tabs x 3",
    repeats=(3, 2, 1),
)
def bench_next_dst():
    """The ZoneInfo branch of the aware next() search.

    Every other datetime in the suite is timezone.utc, and the fixed-offset
    fast path (``type(tzinfo) is datetime.timezone``) provably skips the
    spring-forward gap probe, so the code a production DST fleet runs on
    every fire had zero coverage.  Three fixed aware instants per tab:

    - 2026-03-08 03:30 America/New_York: POST-transition, which verifiably
      takes the gap-rewind branch (a pre-gap 01:59 instant returns early and
      never executes the code this metric exists to guard);
    - 2026-11-01 01:30 at fold=0 and fold=1: the repeated fall-back hour on
      both of its readings.

    Skips when the tzdata database is unavailable.
    """
    tz = _zoneinfo_ny()
    tabs = fixture(
        "tabs_dst_2k", lambda: _parse_tabs(_dst_window_exprs(_n(2000)))
    )
    spring_post = datetime(2026, 3, 8, 3, 30, tzinfo=tz)
    fall_first = datetime(2026, 11, 1, 1, 30, tzinfo=tz, fold=0)
    fall_second = datetime(2026, 11, 1, 1, 30, tzinfo=tz, fold=1)
    t0 = time.perf_counter()
    for tab in tabs:
        tab.next(spring_post)
        tab.next(fall_first)
        tab.next(fall_second)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# config: YAML and classic-crontab parsing, JobConfig construction.
# ---------------------------------------------------------------------------


@bench(
    "config.parse_yaml_300",
    "config",
    detail="parse_config_string, 300-job YAML",
    repeats=(3, 2, 1),
)
def bench_parse_yaml():
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    text = fixture("yaml_300", lambda: _config_yaml(_n(300)))
    t0 = time.perf_counter()
    parse_config_string(text, "")
    return time.perf_counter() - t0


# The complexity tripwire for the YAML parse, and the reason a second size of
# the same call is worth its CI minutes.  strictyaml's vendored
# CommentedSeq.__deepcopy__ ran copy_attributes INSIDE its element loop, which
# made every Seq validation quadratic in the number of jobs; the whole bill
# lands on the one big sequence a config has, jobs:.  Measured on the
# developer machine, 3k jobs: 6.27s with the slip, 1.18s with the call hoisted
# (config._patch_strictyaml_seq_deepcopy).  At the 300 jobs parse_yaml_300
# uses, the same slip is only 0.161s against 0.117s, a 38% bulge any loaded
# runner can argue with, so the metric that was supposed to be watching this
# call had almost no signal: 5.3x here against 1.4x there.  Sizes below
# roughly 3k stay in the regime where the linear term dominates and hide it.
#
# repeats=1 in full mode, the only benchmark in the suite that measures once,
# because the BASELINE side of the CI pairing runs the RELEASED parser and so
# still pays the quadratic cost until a fixed release is the baseline.  Per
# round per side the harness pays one untimed warm-up plus the repeats: a
# round costs 14.8s on the quadratic side and 2.3s here, so across the 5
# in-process rounds this metric is roughly 3 CI minutes for one release cycle
# and under a minute after that.  Measuring once loses nothing the gate uses:
# compare.py's noise band is round-to-round scatter (_rel_cov reads
# round_values, one value per round) and its reported figure is the
# best-of-rounds minimum, so the 5 CI rounds already supply both; the repeats
# only sharpen a single round's own estimate.
@bench(
    "config.parse_yaml_3k",
    "config",
    detail="parse_config_string, 3k-job YAML (parse-complexity tripwire)",
    repeats=(1, 1, 1),
)
def bench_parse_yaml_3k():
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    text = fixture("yaml_3k", lambda: _config_yaml(_n(3000)))
    t0 = time.perf_counter()
    parse_config_string(text, "")
    return time.perf_counter() - t0


@bench(
    "config.jobconfig_3k",
    "config",
    detail="JobConfig over merged defaults, 3k jobs",
    repeats=(3, 2, 1),
)
def bench_jobconfig():
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    raws = fixture("job_dicts_3k", lambda: _job_dicts(_n(3000)))
    t0 = time.perf_counter()
    for raw in raws:
        JobConfig(mergedicts(DEFAULT_CONFIG, raw))
    return time.perf_counter() - t0


@bench(
    "config.parse_crontab_1k",
    "config",
    detail="parse_crontab_string, 1k classic lines",
    repeats=(3, 2, 1),
)
def bench_parse_crontab():
    try:
        from cronstable.config import parse_crontab_string
    except ImportError as exc:
        raise Skip("parse_crontab_string unavailable: %r" % exc) from None
    n = _n(1000)
    text = fixture(
        "crontab_1k",
        lambda: (
            "\n".join(
                "%s echo line-%d" % (expr, i)
                for i, expr in enumerate(_varied_exprs(n))
            )
            + "\n"
        ),
    )
    t0 = time.perf_counter()
    parse_crontab_string(text, "bench-crontab")
    return time.perf_counter() - t0


def _reload_dir():
    """A config directory of three ~17-job files, parsed once so the
    per-file cache is warm for every unchanged file."""

    def build():
        try:
            from cronstable.config import parse_config_with_sources
        except ImportError as exc:
            raise Skip(
                "parse_config_with_sources unavailable: %r" % exc
            ) from None
        path = os.path.join(_tmpdir(), "reload-dir")
        os.makedirs(path, exist_ok=True)
        per_file = max(2, _n(50) // 3)
        for f in range(3):
            lines = ["jobs:"]
            for i in range(per_file):
                lines.append("  - name: f%djob%03d" % (f, i))
                lines.append("    command: echo f%dj%03d" % (f, i))
                lines.append(
                    '    schedule: "%d %d * * *"' % (i % 60, (i * 7) % 24)
                )
            lines.append("")
            with open(
                os.path.join(path, "bench-%d.yaml" % f), "w", encoding="utf-8"
            ) as handle:
                handle.write("\n".join(lines))
        parse_config_with_sources(path)  # warm the per-file cache
        return path, per_file

    return fixture("reload_dir_50", build)


@bench(
    "config.reload_warm_50",
    "config",
    detail="4 operator edits + warm directory reparses, 50 jobs in 3 files",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_config_reload_warm():
    """The per-file reload cache under a realistic operator edit.

    The content-hash-keyed per-file cache has no coverage; a dead cache
    degrades every edit to a whole-directory strictyaml reparse (measured
    ~40x).  Timed through the PUBLIC parse_config_with_sources entry (a
    private-name drift would turn this metric into a perpetual Skip), and
    every timed edit writes a deterministic NEW content variant -- under
    content hashing, rewriting identical bytes is a cache HIT and would
    time nothing but signature checks.
    """
    try:
        from cronstable.config import parse_config_with_sources
    except ImportError as exc:
        raise Skip("parse_config_with_sources unavailable: %r" % exc) from None
    path, per_file = _reload_dir()
    target = os.path.join(path, "bench-0.yaml")
    edits = 4

    def variant(seq):
        lines = ["jobs:"]
        for i in range(per_file):
            lines.append("  - name: f0job%03d" % i)
            lines.append("    command: echo f0j%03d-v%d" % (i, seq))
            lines.append(
                '    schedule: "%d %d * * *"' % (i % 60, (i * 7) % 24)
            )
        lines.append("")
        return "\n".join(lines)

    # Pre-render the variants so the timed region is edit + reparse, not
    # string building.  The final untimed write below resets the file so
    # every repeat sees the same starting content.
    variants = [variant(seq) for seq in range(edits)]
    t0 = time.perf_counter()
    for text in variants:
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(text)
        config, _sources = parse_config_with_sources(path)
    dt = time.perf_counter() - t0
    if len(config.jobs) != per_file * 3:
        raise RuntimeError(
            "warm reload parsed %d jobs, expected %d"
            % (len(config.jobs), per_file * 3)
        )
    return dt


@bench(
    "config.interp_2k",
    "config",
    detail="env-interpolation walk over a 2k-reference config document",
    repeats=(3, 2, 1),
)
def bench_config_interp():
    """The hand-written linear ${VAR} scanner, previously unmeasured.

    parse_yaml_300's fixture contains no '$' at all, so only the
    no-reference fast path was ever timed -- a revert to the quadratic
    re.sub shape (the fuzz-found '${x:-' trap) keeps every functional test
    green.  _interpolate_env reads os.environ directly (no env parameter
    exists), so the fixture seeds fixed entries untimed and uses a
    guaranteed-unset prefix for the :-default cases; every bare ${VAR}
    resolves or the region raises.  Unterminated '${NAME:-' tails (the
    trap's own shape) are included, capped at 64 chars so a reintroduced
    quadratic gates in seconds instead of hanging CI.
    """
    try:
        from cronstable import config as config_mod
    except ImportError as exc:
        raise Skip("cronstable.config unavailable: %r" % exc) from None
    interp = getattr(config_mod, "_interpolate_env", None)
    if interp is None:
        raise Skip("config._interpolate_env not present")
    n = _n(2000)
    for k in range(8):
        os.environ.setdefault("CRONSTABLE_BENCH_INTERP_%d" % k, "value-%d" % k)
    trap_tail = "${CRONSTABLE_BENCH_UNSET_TRAP:-" + "x" * 64

    def build():
        jobs = []
        for i in range(n):
            k = i % 8
            env = {
                "SET_%d" % i: "${CRONSTABLE_BENCH_INTERP_%d}/path-%d" % (k, i),
                "DFL_%d" % i: "${CRONSTABLE_BENCH_UNSET_%d:-fallback-%d}"
                % (i, i),
                "MIX_%d" % i: "a$$b ${CRONSTABLE_BENCH_INTERP_%d} %s"
                % (k, trap_tail),
            }
            jobs.append(
                {"name": "job%05d" % i, "command": "true", "environment": env}
            )
        return {"jobs": jobs}

    doc = fixture("interp_doc_2k", build)
    # three passes: one walk of the 2k-job doc measures under the harness's
    # 50ms floor on CI, which would leave the metric floor-bound (the exact
    # defect the effective-gate column exists to flag)
    t0 = time.perf_counter()
    for _ in range(3):
        out = interp(doc, "bench-config")
    dt = time.perf_counter() - t0
    probe = out["jobs"][0]["environment"]["SET_0"]
    if probe != "value-0/path-0":
        raise RuntimeError(
            "interpolation produced %r; the walk did not resolve" % probe
        )
    return dt


def _include_part(f, per_file, seq):
    """One included jobs file; ``seq`` varies the bytes so a rewrite is a
    real cache MISS under content hashing."""
    lines = ["jobs:"]
    for i in range(per_file):
        lines.append("  - name: inc%djob%03d" % (f, i))
        lines.append("    command: echo inc%dj%03d-v%d" % (f, i, seq))
        lines.append('    schedule: "%d %d * * *"' % (i % 60, (i * 7) % 24))
    lines.append("")
    return "\n".join(lines)


def _reload_include_dir():
    """An entry file that ``include:``s three ~17-job files, parsed once so
    the per-file cache is warm for every unchanged member of the tree."""

    def build():
        try:
            from cronstable.config import parse_config_with_sources
        except ImportError as exc:
            raise Skip(
                "parse_config_with_sources unavailable: %r" % exc
            ) from None
        path = os.path.join(_tmpdir(), "reload-include")
        os.makedirs(path, exist_ok=True)
        per_file = max(2, _n(50) // 3)
        for f in range(3):
            with open(
                os.path.join(path, "part-%d.yaml" % f), "w", encoding="utf-8"
            ) as handle:
                handle.write(_include_part(f, per_file, 0))
        entry = os.path.join(path, "entry.yaml")
        with open(entry, "w", encoding="utf-8") as handle:
            handle.write(
                "include:\n"
                + "".join("  - part-%d.yaml\n" % f for f in range(3))
            )
        try:
            config, _sources = parse_config_with_sources(entry)
        except Exception as exc:
            raise Skip("include tree failed to parse: %r" % exc) from None
        if len(config.jobs) != per_file * 3:
            raise RuntimeError(
                "include tree yielded %d jobs, expected %d"
                % (len(config.jobs), per_file * 3)
            )
        return entry, per_file

    return fixture("reload_include_50", build)


@bench(
    "config.reload_warm_include_50",
    "config",
    detail="8 operator edits + warm reparses of an include: tree, 50 jobs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_config_reload_warm_include():
    """The reload cache over an ``include:`` tree, the other config layout.

    config.reload_warm_50 builds a config DIRECTORY, where the entry is a
    directory listing and each sibling is parsed on its own.  An include
    tree is a different code path: one entry file drives a recursive
    parse_config_file walk, with cycle detection, a transitive source set
    and a per-file defaults merge per level.  A cache keyed only on the
    entry (or a signature that stops at the entry's own stat) reparses the
    whole tree on every pass here and stays invisible to the directory
    metric.  One of the three parts is rewritten with new bytes per timed
    edit, so the other two must come from the cache or nothing is being
    measured.
    """
    try:
        from cronstable.config import parse_config_with_sources
    except ImportError as exc:
        raise Skip("parse_config_with_sources unavailable: %r" % exc) from None
    entry, per_file = _reload_include_dir()
    target = os.path.join(os.path.dirname(entry), "part-0.yaml")
    # eight, where the directory twin uses four: one warm include reparse is
    # roughly half a directory reparse, so four measured under the harness's
    # 50ms rule and would have shipped floor-bound
    edits = 8
    variants = [_include_part(0, per_file, seq) for seq in range(1, edits + 1)]
    t0 = time.perf_counter()
    for text in variants:
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(text)
        config, _sources = parse_config_with_sources(entry)
    dt = time.perf_counter() - t0
    if len(config.jobs) != per_file * 3:
        raise RuntimeError(
            "warm include reload parsed %d jobs, expected %d"
            % (len(config.jobs), per_file * 3)
        )
    return dt


def _gc_job_map(n):
    """``{name: JobConfig}`` at fleet scale: the graph a reload retains.

    The daemon's largest long-lived structure by tracked-object count, which
    is what a full collection actually walks (bytes are irrelevant to the
    collector).  Built through the public JobConfig/mergedicts pair, so a
    version lacking either records as a skip.
    """
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    jobs = {}
    for i in range(n):
        name = "job%06d" % i
        jobs[name] = JobConfig(
            mergedicts(
                DEFAULT_CONFIG,
                {
                    "name": name,
                    "command": "echo %d" % i,
                    "schedule": "%d %d * * *" % (i % 60, (i * 7) % 24),
                },
            )
        )
    return jobs


@bench(
    "config.reload_gc_100k",
    "config",
    detail="rebuild + swap a 100k-job set with the COLLECTOR LIVE",
    repeats=(2, 1, 1),
    gate_pct=25.0,
    compare="median",
)
def bench_config_reload_gc():
    """The one timed region in the suite that runs with GC enabled.

    _run_one collects and then DISABLES the collector around every warm-up
    and every measured repetition, which is the right call for a
    microbenchmark, and is why no other metric here (nor any absolute
    ceiling in budgets.json) has ever included a millisecond of collector
    time.  The blindness is scale-dependent: at the sizes the other fixtures
    use, GC-on and GC-off agree within noise.  It is the RETAINED
    fleet-scale graph that changes the arithmetic, and that shape is
    precisely what a changed reload executes: the old job set stays reachable
    while the replacement is built, so every allocation-triggered collection
    walks both.

    compare='median', not the usual 'min': a full collection lands on some
    repetitions and not others, so 'min' would select the repetition that
    happened to dodge it and reinstate exactly the blindness this metric
    exists to remove.  The collector's enabled state is restored afterwards
    whatever the harness had set.

    Two repetitions, not the usual three: one rebuild of a 100k-job set is
    the most expensive timed region in the suite (measured 1.0s of work
    carrying 3.4s of collector time), and the CI bill is per round per side.
    Cutting a repetition costs nothing here, since the five CI rounds
    already give compare.py its noise band.  Cutting the SCALE would not be
    free: the pause is steeply superlinear (measured 6 / 13 / 77 / 371 ms at
    20k / 40k / 60k / 100k resident jobs), so a smaller fixture measures a
    different phenomenon rather than a cheaper one.
    """
    n = _n(100000)
    resident = fixture("gc_job_map_100k", lambda: [_gc_job_map(n)])
    was_enabled = gc.isenabled()
    gc.enable()
    try:
        t0 = time.perf_counter()
        fresh = _gc_job_map(n)
        old = resident[0]
        resident[0] = fresh
        del old
        dt = time.perf_counter() - t0
    finally:
        if not was_enabled:
            gc.disable()
    if len(resident[0]) != n:
        raise RuntimeError(
            "rebuilt job set holds %d entries, expected %d"
            % (len(resident[0]), n)
        )
    return dt


# ---------------------------------------------------------------------------
# configcli: the rich config shape, many-file config directories, and the
# cold start of the clients a job or an MCP host spawns.
# ---------------------------------------------------------------------------
def _config_yaml_rich(n_jobs, n_dags=2, tasks_per_dag=8):
    """A config whose jobs carry what a production job carries.

    _config_yaml's jobs set three or four scalar keys at depth one.  A job
    here sets a timezone, timeouts, an environment list, failsWhen, an SLA,
    a retry ladder and three report hooks, so its mapping holds sixteen
    keys and nests five deep, and the document also has pattern-keyed
    blocks (pools, web.headers, webhook headers) and a few DAGs.
    """
    lines = [
        "defaults:",
        "  shell: /bin/sh",
        "  captureStderr: true",
        "  environment:",
        "    - key: TZ",
        "      value: UTC",
        "  onSuccess:",
        "    report:",
        "      webhook:",
        "        url:",
        "          fromEnvVar: BENCH_HEARTBEAT_URL",
        "pools:",
        "  db:",
        "    slots: 8",
        "  api:",
        "    slots: 4",
        "    maxQueued: 50",
        "web:",
        "  listen:",
        "    - http://127.0.0.1:8080",
        "  headers:",
        "    X-Frame-Options: DENY",
        "    X-Content-Type-Options: nosniff",
        "    Referrer-Policy: no-referrer",
        "  authTokens:",
        "    - value: bench-token-aaaaaaaaaaaaaaaaaaaaaaaa",
        "      label: phone",
        "      scopes:",
        "        - view",
        "        - approve",
        "jobs:",
    ]
    exprs = _varied_exprs(n_jobs)
    for i in range(n_jobs):
        lines += [
            "  - name: rich%05d" % i,
            "    command: /opt/app/bin/run-task --id %d --mode nightly" % i,
            '    schedule: "%s"' % exprs[i],
            "    timezone: America/New_York",
            "    concurrencyPolicy: Forbid",
            "    captureStdout: true",
            "    executionTimeout: 1800",
            "    killTimeout: 15",
            "    saveLimit: 200",
            "    onMissed: run-once",
            "    startingDeadlineSeconds: 900",
            "    workingDirectory: /srv/app/job%d" % i,
            "    environment:",
        ]
        for k in range(6):
            lines += [
                "      - key: APP_VAR_%d" % k,
                "        value: v%d-%d" % (k, i),
            ]
        lines += [
            "    failsWhen:",
            "      producesStdout: false",
            "      producesStderr: true",
            "      nonzeroReturn: true",
            "    sla:",
            "      maxRuntimeSeconds: 3600",
            "      lateAfterSeconds: 300",
            "    onFailure:",
            "      retry:",
            "        maximumRetries: 3",
            "        initialDelay: 5",
            "        maximumDelay: 300",
            "        backoffMultiplier: 2",
            "      report:",
            "        mail:",
            "          from: cron@example.com",
            "          to: oncall@example.com",
            "          smtpHost: smtp.example.com",
            "          smtpPort: 587",
            "          starttls: true",
            '          subject: "[cron] {{ name }} failed"',
            "          body: |",
            "            Job {{ name }} failed with {{ exit_code }}.",
            "            {% if stderr %}STDERR: {{ stderr }}{% endif %}",
            "        webhook:",
            "          url:",
            "            fromEnvVar: BENCH_WEBHOOK_URL",
            "          headers:",
            "            X-Team: platform",
            "            X-Source: cronstable",
            '          body: \'{"text": "{{ name }} failed"}\'',
            "    onPermanentFailure:",
            "      report:",
            "        webhook:",
            "          url:",
            "            fromEnvVar: BENCH_PAGER_URL",
            "    onLate:",
            "      report:",
            "        webhook:",
            "          url:",
            "            fromEnvVar: BENCH_WEBHOOK_URL",
        ]
        if i % 4 == 0:
            lines += ["    pool: db", "    poolSlots: 2"]
    if n_dags:
        lines.append("dags:")
    for d in range(n_dags):
        lines += [
            "  - name: dag%03d" % d,
            '    schedule: "%d 2 * * *"' % (d % 60),
            "    timezone: America/New_York",
            "    tasks:",
        ]
        for i in range(tasks_per_dag):
            lines += [
                "      - id: t%03d" % i,
                "        command: /opt/etl/step --n %d" % i,
                "        captureStdout: true",
                "        retries: 2",
                "        environment:",
                "          - key: STEP",
                "            value: s%d" % i,
            ]
            if i:
                lines += ["        dependsOn:", "          - t%03d" % (i - 1)]
            if i % 4 == 3:
                lines.append("        pool: api")
    lines.append("")
    return "\n".join(lines)


def _rich_shape(scale):
    """``(jobs, dags, tasks per dag)`` of the rich fixture at ``scale``."""
    jobs = _n(scale, 2)
    return jobs, 2, max(2, jobs // 2)


@bench(
    "config.parse_yaml_rich_20",
    "config",
    detail="parse_config_string, 20 production-shaped jobs + 2 DAGs",
    repeats=(3, 2, 1),
)
def bench_parse_yaml_rich():
    """The YAML parse over nested mappings, the shape the plain fixture lacks.

    A job with sixteen keys, nested hooks and pattern-keyed headers costs
    about twenty times a three-key job, nearly all of it in strictyaml's
    mapping validation.  This guards the per-key lookup shim in
    cronstable.config and any schema change that makes a nested block
    expensive, both of which the flat config.parse_yaml_* fixtures cannot
    see.
    """
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    jobs, dags, tasks = _rich_shape(20)
    text = fixture(
        "yaml_rich_20", lambda: _config_yaml_rich(jobs, dags, tasks)
    )
    t0 = time.perf_counter()
    parsed = parse_config_string(text, "")
    dt = time.perf_counter() - t0
    # tests/test_benchmarks.py swaps the parser for a recorder returning
    # None to read the workload's size, so only a real result is checked
    if parsed is not None and (
        len(parsed.jobs) != jobs
        or len(parsed.dags) != dags
        or sum(len(d.tasks) for d in parsed.dags) != dags * tasks
    ):
        raise RuntimeError(
            "rich parse yielded %d jobs and %d dags, expected %d and %d"
            % (len(parsed.jobs), len(parsed.dags), jobs, dags)
        )
    return dt


def _one_job_file(index, seq):
    """A config file holding one job; ``seq`` varies the bytes."""
    return (
        "jobs:\n"
        "  - name: job%05d\n"
        "    command: echo job%05d-v%06d\n"
        '    schedule: "%d %d * * *"\n'
        % (index, index, seq, index % 60, (index * 7) % 24)
    )


def _write_one_job_dir(path, files, seq):
    os.makedirs(path, exist_ok=True)
    for i in range(files):
        with open(
            os.path.join(path, "job-%05d.yaml" % i),
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(_one_job_file(i, seq))


def _reload_dir_1k():
    """A config directory of 1,000 one-job files with a warm parse cache."""

    def build():
        try:
            from cronstable.config import parse_config_with_sources
        except ImportError as exc:
            raise Skip(
                "parse_config_with_sources unavailable: %r" % exc
            ) from None
        files = _n(1000, 8)
        path = os.path.join(_tmpdir(), "reload-dir-1k")
        _write_one_job_dir(path, files, 0)
        config, _sources = parse_config_with_sources(path)
        if len(config.jobs) != files:
            raise RuntimeError(
                "directory yielded %d jobs, expected %d"
                % (len(config.jobs), files)
            )
        # [next edit number]: every timed write carries bytes no earlier
        # write had, so each one is a content-hash miss for that file
        return path, files, [1]

    return fixture("reload_dir_1k", build)


@bench(
    "config.reload_warm_dir_1k",
    "config",
    detail="3 operator edits + warm reparses of a 1k-file config directory",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_config_reload_warm_dir():
    """One edited file in a large directory of one-job files.

    A reload reparses the edited file and serves the other 999 from the
    per-file cache, after re-reading and hashing each of them and merging
    its defaults, pools and sections.  config.reload_warm_50 has three
    files, so that per-file sweep is invisible there; here it is the whole
    cost.  It also sits just under the cache's fixed floor of 1,024
    entries, where a cache that stops holding the directory whole turns
    every reload into a full reparse.
    """
    try:
        from cronstable.config import parse_config_with_sources
    except ImportError as exc:
        raise Skip("parse_config_with_sources unavailable: %r" % exc) from None
    path, files, counter = _reload_dir_1k()
    index = files // 2
    target = os.path.join(path, "job-%05d.yaml" % index)
    edits = 3
    first = counter[0]
    counter[0] += edits
    variants = [_one_job_file(index, first + n) for n in range(edits)]
    t0 = time.perf_counter()
    for text in variants:
        with open(target, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        config, _sources = parse_config_with_sources(path)
    dt = time.perf_counter() - t0
    edited = [job for job in config.jobs if job.name == "job%05d" % index]
    wanted = "echo job%05d-v%06d" % (index, first + edits - 1)
    if len(config.jobs) != files or [j.command for j in edited] != [wanted]:
        raise RuntimeError(
            "warm directory reload returned %d jobs (expected %d) and the "
            "edited job reads %r, expected %r"
            % (len(config.jobs), files, [j.command for j in edited], wanted)
        )
    return dt


@bench(
    "config.parse_dir_cold_200",
    "config",
    detail="cold parse of a 200-file config directory, one job per file",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_config_parse_dir_cold():
    """The first load of a directory of small files, as a boot pays it.

    Each file is a separate strictyaml document, and a document costs more
    to set up than the one job in it costs to validate, so 200 one-job
    files take about twice the time of one 200-job file.  Every file is
    rewritten with new bytes before the timed region, which makes each a
    cache miss through the public entry point.
    """
    try:
        from cronstable.config import parse_config_with_sources
    except ImportError as exc:
        raise Skip("parse_config_with_sources unavailable: %r" % exc) from None
    files = _n(200, 4)
    path, counter = fixture(
        "parse_dir_cold_200",
        lambda: (os.path.join(_tmpdir(), "parse-dir-cold"), [0]),
    )
    counter[0] += 1
    _write_one_job_dir(path, files, counter[0])
    t0 = time.perf_counter()
    config, _sources = parse_config_with_sources(path)
    dt = time.perf_counter() - t0
    wanted = "echo job%05d-v%06d" % (0, counter[0])
    if len(config.jobs) != files or config.jobs[0].command != wanted:
        raise RuntimeError(
            "cold directory parse returned %d jobs (expected %d), first "
            "command %r" % (len(config.jobs), files, config.jobs[0].command)
        )
    return dt


# ---------------------------------------------------------------------------
# schedule: seeding and analyzing the fleet schedule, 100k jobs.
# ---------------------------------------------------------------------------


@bench(
    "schedule.cold_build_100k",
    "schedule",
    detail="parse + next() + heapify, 100k jobs from cold",
    repeats=(3, 2, 1),
)
def bench_schedule_cold():
    import heapq

    CronTab = _crontab_cls()
    exprs = fixture("exprs_100k", lambda: _varied_exprs(_n(100000)))
    t0 = time.perf_counter()
    heap = []
    for i, e in enumerate(exprs):
        tab = CronTab(e)
        delay = tab.next(_NOW)
        if delay is not None:
            heap.append((delay, i))
    heapq.heapify(heap)
    return time.perf_counter() - t0


@bench(
    "schedule.reseed_100k",
    "schedule",
    detail="next() + heapify over 100k pre-parsed jobs",
    repeats=(3, 2, 1),
)
def bench_schedule_reseed():
    import heapq

    tabs = fixture(
        "tabs_100k",
        lambda: _parse_tabs(
            fixture("exprs_100k", lambda: _varied_exprs(_n(100000)))
        ),
    )
    t0 = time.perf_counter()
    heap = []
    for i, tab in enumerate(tabs):
        delay = tab.next(_NOW)
        if delay is not None:
            heap.append((delay, i))
    heapq.heapify(heap)
    return time.perf_counter() - t0


# Rescaled TWICE from the retired schedule.pressure_5k_24h, which was
# floor-bound on CI at an effective ~39% against its declared 15% (so
# effectively ungated); 20k entries over 24h measured ~30ms and was STILL
# floor-bound, hence the 48h horizon.  Id carries the new workload; see
# benchmarks/README.md on rescales.
@bench(
    "schedule.pressure_20k_48h",
    "schedule",
    detail="schedule_pressure, 20k entries over 48h",
    repeats=(3, 2, 1),
)
def bench_schedule_pressure():
    """schedule_pressure at a scale where its declared gate is real.

    The entries sit on the host clock. _fire_cells walks them in a fixed
    offset whenever the host offset is constant across the window and the
    engine's look-back, so the timing is zone-independent on any window
    without a DST transition, and a paired run on one runner sees the
    same host zone on both sides.
    """
    try:
        from cronstable.croninfo import schedule_pressure
    except ImportError as exc:
        raise Skip("schedule_pressure unavailable: %r" % exc) from None
    entries = fixture("entries_dup_20k", lambda: _schedule_entries(_n(20000)))
    t0 = time.perf_counter()
    schedule_pressure(entries, start=_NOW, hours=48)
    return time.perf_counter() - t0


@bench(
    "schedule.next_fires_2k",
    "schedule",
    detail="next_fires(count=5) for 2k schedules",
    repeats=(3, 2, 1),
)
def bench_next_fires():
    try:
        from cronstable.croninfo import next_fires
    except ImportError as exc:
        raise Skip("next_fires unavailable: %r" % exc) from None
    exprs = fixture("exprs_next_fires", lambda: _varied_exprs(_n(2000)))
    t0 = time.perf_counter()
    for e in exprs:
        next_fires(e, 5, start=_NOW)
    return time.perf_counter() - t0


# Rescaled (id bumped from the legacy schedule.duplicates_5k, ~9ms and noisy):
# its own 20k-entry fixture puts the timed region near 40ms.  Id carries the
# new scale; see benchmarks/README.md.
@bench(
    "schedule.duplicates_20k",
    "schedule",
    detail="duplicate_schedules over 20k entries",
    repeats=(3, 2, 1),
)
def bench_duplicates():
    try:
        from cronstable.croninfo import duplicate_schedules
    except ImportError as exc:
        raise Skip("duplicate_schedules unavailable: %r" % exc) from None
    entries = fixture("entries_dup_20k", lambda: _schedule_entries(_n(20000)))
    t0 = time.perf_counter()
    duplicate_schedules(entries)
    return time.perf_counter() - t0


# --- rescales of existing benchmarks ---------------------------------------
#
# Each definition below takes the place of the one its marker names.
@bench(
    "schedule.suggest_slot_5k_x20",
    "schedule",
    detail="suggest_slot against 5k entries, 20 calls",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_suggest_slot():
    """suggest_slot at a scale where its declared gate is real.

    One call over 5k entries takes a few milliseconds, under the gate floor,
    so the call is repeated twenty times; each call does the full fire walk
    and the slot ranking, as the web and MCP suggest endpoints do per
    request.
    """
    try:
        from cronstable.croninfo import suggest_slot
    except ImportError as exc:
        raise Skip("suggest_slot unavailable: %r" % exc) from None
    entries = fixture("entries_5k", lambda: _schedule_entries(_n(5000)))
    t0 = time.perf_counter()
    for _ in range(20):
        found = suggest_slot(entries, period="hourly", start=_NOW)
    dt = time.perf_counter() - t0
    if not isinstance(found, dict) or not found.get("expression"):
        raise RuntimeError("suggest_slot returned no slot")
    return dt


@bench(
    "schedule.lint_2500_zoned",
    "schedule",
    detail="lint_schedule for 2,500 zoned restricted-hour dailies",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_schedule_lint_zoned():
    """The DST linter's zoned path, which no fixture anywhere else enters.

    No bench fixture sets a timezone, and _lint_dst returns [] for
    fixed-offset zones, so the memoized per-(zone, year) transition scan is
    exercised by nothing: a cache-defeat regression means a full 366-day
    offset scan per parse and stays invisible to every functional test.
    The schedules are restricted-hour fixed-time dailies with wall times
    OUTSIDE 01:00-03:00, deliberately NOT the suite's _EVEN_STEPS shapes,
    whose unrestricted hours hit the len(hours)>=24 fast-exit and would
    false-green the metric on its own target.  ``now`` is pinned so the
    scanned year window never moves.
    """
    try:
        from cronstable.croninfo import lint_schedule
    except ImportError as exc:
        raise Skip("croninfo.lint_schedule unavailable: %r" % exc) from None
    tz = _zoneinfo_ny()
    lint_now = datetime(2026, 1, 15, 12, 0, tzinfo=tz)
    n = _n(2500)
    exprs = fixture(
        "lint_zoned_exprs_%d" % n,
        lambda: ["%d %d * * *" % (i % 60, 5 + (i * 3) % 18) for i in range(n)],
    )
    try:
        findings = lint_schedule(exprs[0], timezone=tz, now=lint_now)
    except TypeError as exc:
        raise Skip("lint_schedule signature changed: %r" % exc) from None
    if findings is None:
        raise RuntimeError("lint_schedule returned None")
    t0 = time.perf_counter()
    for expr in exprs:
        lint_schedule(expr, timezone=tz, now=lint_now)
    return time.perf_counter() - t0


#: A pass instant a week clear of the 2026 US transitions, so the host-clock
#: seed below takes the fixed-offset path on any host zone; also the
#: due-pass base further down.
_DUE_BASE = datetime(2026, 3, 15, 12, 31, 0, tzinfo=timezone.utc)


@bench(
    "schedule.reseed_local_20k",
    "schedule",
    detail="_ensure_seeded over 20k host-clock (utc: false) jobs",
    repeats=(3, 2, 1),
)
def bench_schedule_reseed_local():
    """The seed pass over jobs framed in the host clock.

    schedule.reseed_100k walks the engine on a UTC frame, the fixed-offset
    path.  A ``utc: false`` job (what the Task Scheduler importer emits for
    every task) arms through CronTab.next_local, whose fixed-offset shortcut
    holds only while the host offset is provably constant across the
    answer, so a regression on that path shows here and nowhere else.
    """
    Cron = _cron_cls()
    if not hasattr(Cron, "_ensure_seeded"):
        raise Skip("Cron._ensure_seeded not present")

    def build():
        try:
            from cronstable.config import (
                DEFAULT_CONFIG,
                JobConfig,
                mergedicts,
            )
        except ImportError as exc:
            raise Skip("cronstable.config API unavailable: %r" % exc) from None
        jobs = {}
        for i in range(_n(20000)):
            name = "local%05d" % i
            jobs[name] = JobConfig(
                mergedicts(
                    DEFAULT_CONFIG,
                    {
                        "name": name,
                        "command": "true",
                        "schedule": "%d %d * * *" % (i % 60, (i * 7) % 24),
                        "utc": False,
                    },
                )
            )
        return jobs

    jobs = fixture("local_jobs_20k", build)
    try:
        cron = Cron(
            None,
            config_yaml="jobs:\n  - name: seed\n    command: 'x'\n"
            "    schedule: '0 0 * * *'\n",
        )
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    cron.cron_jobs = jobs
    t0 = time.perf_counter()
    cron._ensure_seeded(_DUE_BASE)
    return time.perf_counter() - t0


# The forever-loop's fire pass at marketed scale.  The fixture is the single
# most expensive in the suite (~100k JobConfigs, built once per process), so
# it sits LAST in the schedule group: the group boundary evicts it before the
# dag group starts.


def _due_pass_jobs():
    """(jobs_dict, cohort_names): ~50 every-minute jobs spread through 100k
    sparse dailies, as JobConfigs keyed in config order."""

    def build():
        try:
            from cronstable.config import (
                DEFAULT_CONFIG,
                JobConfig,
                mergedicts,
            )
        except ImportError as exc:
            raise Skip("cronstable.config API unavailable: %r" % exc) from None
        n = _n(100000)
        step = max(1, n // 50)
        jobs = {}
        cohort = []
        for i in range(n):
            name = "job%06d" % i
            if i % step == 0:
                expr = "* * * * *"
                cohort.append(name)
            else:
                expr = "%d %d * * *" % (i % 60, (i * 7) % 24)
            jobs[name] = JobConfig(
                mergedicts(
                    DEFAULT_CONFIG,
                    {"name": name, "command": "true", "schedule": expr},
                )
            )
        return jobs, cohort

    return fixture("due_pass_jobs_100k", build)


@bench(
    "schedule.due_pass_100k",
    "schedule",
    detail="10 due-fire passes over a 100k-job index (~50 due per pass)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_schedule_due_pass():
    """The daemon's forever-loop fire pass, which no other metric touches.

    _spawn_due_jobs walks the ENTIRE cron_jobs dict per pass to preserve
    config order, and cold_build/reseed never construct a Cron at all.  The
    100k jobs are injected straight into cron_jobs/_next_fire/_fire_heap (a
    100k-job YAML parse costs tens of seconds even now that it is linear, and
    parsing is config.py's business: config.parse_yaml_300 and
    config.parse_yaml_3k gate it at sizes CI can afford).

    Two corrections this spec was vetted into: the pass instants sit NINE
    seconds past each minute boundary -- CATCHUP_LIMIT is 10s, and anything
    later silently benchmarks the fell-behind warning path once per due job
    -- and the launch seam is checked POSITIVELY before it is neutered (the
    class must still define an async _launch_plan): a silent rename would
    otherwise un-neuter the launch path and spawn ~500 real processes per
    timed call.  The captured plans are asserted afterwards, so a pass that
    silently fired nothing hard-fails instead of timing a no-op.
    """
    import asyncio
    import heapq
    import inspect

    Cron = _cron_cls()
    seam = Cron.__dict__.get("_launch_plan")
    if seam is None or not inspect.iscoroutinefunction(seam):
        raise Skip(
            "Cron._launch_plan seam absent or not async; refusing to run "
            "(an un-neutered pass would spawn real processes)"
        )
    if not hasattr(Cron, "_spawn_due_jobs"):
        raise Skip("Cron._spawn_due_jobs not present")
    jobs, cohort = _due_pass_jobs()
    passes = 10

    try:
        cron = Cron(
            None,
            config_yaml="jobs:\n  - name: seed\n    command: 'x'\n"
            "    schedule: '0 0 * * *'\n",
        )
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    cron.cron_jobs = jobs
    # Fresh index per repeat (the pass advances it): cohort due at the first
    # boundary, dailies parked safely in the future.
    far = _DUE_BASE + timedelta(days=2)
    next_fire = {}
    for name in jobs:
        next_fire[name] = far
    for name in cohort:
        next_fire[name] = _DUE_BASE
    cron._next_fire = next_fire
    heap = [(when, name) for name, when in next_fire.items()]
    heapq.heapify(heap)
    cron._fire_heap = heap

    plans = []

    async def _capture(plan):
        plans.append(plan)

    cron._launch_plan = _capture

    async def run():
        t0 = time.perf_counter()
        for k in range(passes):
            now = _DUE_BASE + timedelta(minutes=k, seconds=9)
            await cron._spawn_due_jobs(now)
        return time.perf_counter() - t0

    dt = asyncio.run(run())
    fired = sum(
        1 for plan in plans for _job, fires in plan if fires
    )
    if fired != passes * len(cohort):
        raise RuntimeError(
            "due passes fired %d of the expected %d launches; the fixture "
            "or the fire path broke and the region timed the wrong work"
            % (fired, passes * len(cohort))
        )
    return dt


def _dag_module():
    try:
        from cronstable import dag
    except ImportError as exc:
        raise Skip("cronstable.dag unavailable: %r" % exc) from None
    for attr in ("TaskSpec", "DagSpec", "validate_graph"):
        if not hasattr(dag, attr):
            raise Skip("cronstable.dag lacks %s" % attr)
    return dag


@bench(
    "dag.build_chain_10k",
    "dag",
    detail="build + validate a 10k-task linear chain",
    repeats=(3, 2, 1),
)
def bench_dag_chain():
    dag = _dag_module()
    n = _n(10000)
    t0 = time.perf_counter()
    tasks = [dag.TaskSpec(id="t0")]
    for i in range(1, n):
        tasks.append(dag.TaskSpec(id="t%d" % i, depends_on=("t%d" % (i - 1),)))
    spec = dag.DagSpec.build("chain", tasks)
    dag.validate_graph(spec)
    return time.perf_counter() - t0


@bench(
    "dag.build_layered_10k",
    "dag",
    detail="build + validate 100 layers x 100 tasks, 3 deps each",
    repeats=(3, 2, 1),
)
def bench_dag_layered():
    dag = _dag_module()
    layers = max(2, int(100 * _scale() ** 0.5))
    width = max(2, int(100 * _scale() ** 0.5))
    t0 = time.perf_counter()
    tasks = []
    for layer in range(layers):
        for w in range(width):
            if layer == 0:
                deps = ()
            else:
                deps = tuple(
                    "L%dW%d" % (layer - 1, (w + k) % width) for k in range(3)
                )
            tasks.append(
                dag.TaskSpec(id="L%dW%d" % (layer, w), depends_on=deps)
            )
    spec = dag.DagSpec.build("layered", tasks)
    dag.validate_graph(spec)
    return time.perf_counter() - t0


# Rescaled (id bumped from the legacy dag.plan_claim_2k, ~5ms): a 10k-task run
# puts the timed transform near 25ms.  Id carries the new scale; see
# benchmarks/README.md.
@bench(
    "dag.plan_claim_10k",
    "dag",
    detail="plan_and_claim over a fresh 10k-task run",
    repeats=(3, 2, 1),
)
def bench_dag_plan():
    dag = _dag_module()
    if not hasattr(dag, "new_run_body") or not hasattr(dag, "plan_and_claim"):
        raise Skip("dag planning API not present")
    n = _n(10000)
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("wide", tasks)
    try:
        body = dag.new_run_body(
            dag="wide",
            run_key="bench",
            run_id="bench-run",
            logical_date=None,
            kind="scheduled",
            now=1700000000.0,
            spec=spec,
        )
        transform = dag.plan_and_claim(
            spec, 1700000000.0, "bench-proc", "bench-host", {}
        )
    except TypeError as exc:
        raise Skip("dag planning signature changed: %r" % exc) from None
    t0 = time.perf_counter()
    transform(body)
    return time.perf_counter() - t0


@bench(
    "dag.finish_fanin_20x1k",
    "dag",
    detail="record 1k mapped-task completions to each of 20 run docs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_finish_fanin():
    """A mapped fan-out's N instances finishing together, for 20 runs.

    Each run records its 1k completions the way the reaper's flush does: one
    batched read-modify-write (mark_tasks_finished) of the run document.  A
    release that records them one by one pays a full-document serialize and
    fsync per completion.  One flush is a few milliseconds, so 20 run
    documents make the timed region, and every completion is checked to
    have landed.
    """
    import asyncio

    dag = _dag_module()
    if not hasattr(dag, "new_run_body") or not hasattr(
        dag, "mark_tasks_finished"
    ):
        raise Skip("dag batched completion API not present")
    n = _n(1000, floor=4)
    docs = _n(20, floor=2)
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("d", tasks)
    now = 1700000000.0
    ns = dag.DAG_RUN_NS_PREFIX + "d"

    def _running_body(key):
        body = dag.new_run_body(
            dag="d",
            run_key=key,
            run_id="rid-" + key,
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        for task in tasks:
            entry = body["tasks"][task.id]
            entry["state"] = "running"
            entry["proc"] = "p"
            entry["attempt"] = 0
        return body

    marks = [
        {
            "taskkey": t.id,
            "success": True,
            "exit_code": 0,
            "fail_reason": None,
            "task": t,
            "jitter": 0.0,
            "expected_proc": "p",
            "expected_attempt": 0,
            "expected_poke": None,
            "resources": None,
        }
        for t in tasks
    ]

    async def run():
        path = tempfile.mkdtemp(prefix="dagfin-", dir=_tmpdir())
        backend = _state_backend(path)
        await backend.start()
        try:
            keys = ["r%02d" % i for i in range(docs)]
            for key in keys:
                body = _running_body(key)
                await backend.mutate_document(
                    ns, key, lambda cur, b=body: (b, None)
                )
            applied = 0
            t0 = time.perf_counter()
            for key in keys:
                _body, done = await backend.mutate_document(
                    ns, key, dag.mark_tasks_finished(marks, now)
                )
                applied += len(done)
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
            shutil.rmtree(path, ignore_errors=True)
        if applied != n * docs:
            raise RuntimeError(
                "%d of %d completions were recorded; the region timed a "
                "partial flush" % (applied, n * docs)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("dag completion signature changed: %r" % exc) from None


@bench(
    "dag.mapped_drain_256",
    "dag",
    detail="drain a 256-wide mapped fan-out to full claim (8 passes)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_mapped_drain():
    """A wide mapped launch driven the way the daemon drives it.

    Guards the deliberately-deferred O(M^2) architectural finding: every
    claim pass and every pid-stamp batch is a full-run-document locked RMW
    (MAX_CLAIMS_PER_PASS caps a pass at 32 claims, so M=256 pays 8 of each),
    and the document being rewritten holds all M instances the whole time.
    dag.plan_claim_10k times ONE transform on a fresh in-memory body and
    cannot see any of that.  When per-entry storage lands, this metric shows
    the drop, then pins the new baseline.

    A release without the batched ``set_task_pids`` stamps each pid through
    its own RMW (the same convention dag.finish_fanin_1k uses), so old sides
    run their own era's real shape.  The final claim-count check hard-fails
    (never skips) if the drain stops early: a fixture that silently claimed
    nothing would otherwise time a no-op.

    dag transforms return dag's PRIVATE keep sentinel (state.py compares by
    identity to its own), which the daemon's driver maps back before the
    backend sees it; the wrapper below is that same mapping, and the drain's
    closing no-claim pass needs it.
    """
    import asyncio

    dag = _dag_module()
    if not hasattr(dag, "new_run_body") or not hasattr(dag, "plan_and_claim"):
        raise Skip("dag planning API not present")
    if not hasattr(dag, "ExpandSpec"):
        raise Skip("dag mapped-task API not present")
    if not hasattr(dag, "set_task_pid") and not hasattr(dag, "set_task_pids"):
        raise Skip("dag pid-stamp API not present")
    m = _n(256)
    now = 1700000000.0
    ns = dag.DAG_RUN_NS_PREFIX + "mapped"
    run_key = "r"
    batched = getattr(dag, "set_task_pids", None)
    try:
        src = dag.TaskSpec(id="src")
        fan = dag.TaskSpec(
            id="fan",
            depends_on=("src",),
            expand=dag.ExpandSpec(from_task="src", key="items"),
        )
        spec = dag.DagSpec.build("mapped", [src, fan])
        dag.validate_graph(spec)
    except TypeError as exc:
        raise Skip("dag mapped-spec signature changed: %r" % exc) from None
    items = list(range(m))

    def _seed_body():
        body = dag.new_run_body(
            dag="mapped",
            run_key=run_key,
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        entry = body["tasks"]["src"]
        entry["state"] = "success"
        entry["exitCode"] = 0
        entry["finishedAt"] = now
        return body

    try:
        from cronstable.state import DOC_KEEP
    except ImportError as exc:
        raise Skip("state DOC_KEEP sentinel unavailable: %r" % exc) from None

    def _mapped_keep(transform):
        def wrapped(cur):
            new_body, result = transform(cur)
            if not isinstance(new_body, dict):  # dag's private keep sentinel
                return DOC_KEEP, result
            return new_body, result

        return wrapped

    async def run():
        path = tempfile.mkdtemp(prefix="dagdrain-", dir=_tmpdir())
        backend = _state_backend(path)
        await backend.start()
        try:
            body = _seed_body()
            await backend.mutate_document(
                ns, run_key, lambda cur, b=body: (b, None)
            )
            expansions = {"fan": items}
            claimed = 0
            t0 = time.perf_counter()
            while claimed <= m:
                transform = _mapped_keep(
                    dag.plan_and_claim(
                        spec, now, "bench-proc", "bench-host", expansions
                    )
                )
                _, result = await backend.mutate_document(
                    ns, run_key, transform
                )
                launches = getattr(result, "launches", None) or []
                if not launches:
                    break
                claimed += len(launches)
                if batched is not None:
                    await backend.mutate_document(
                        ns,
                        run_key,
                        _mapped_keep(
                            batched(
                                [
                                    (
                                        li.taskkey,
                                        "bench-proc",
                                        4242,
                                        li.attempt,
                                    )
                                    for li in launches
                                ],
                                now,
                            )
                        ),
                    )
                else:
                    for li in launches:
                        await backend.mutate_document(
                            ns,
                            run_key,
                            _mapped_keep(
                                dag.set_task_pid(
                                    li.taskkey,
                                    "bench-proc",
                                    4242,
                                    now,
                                    attempt=li.attempt,
                                )
                            ),
                        )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
            shutil.rmtree(path, ignore_errors=True)
        if claimed < m:
            raise RuntimeError(
                "mapped drain claimed %d of %d instances; the fixture or "
                "the claim path broke and the region timed a no-op" % (claimed, m)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("dag planning signature changed: %r" % exc) from None


def _run_quiescent_advance(
    dag, spec, build_body, *, variant, now, assert_unchanged
):
    """The shared harness of the two dag.advance_quiescent_* metrics.

    Seeds build_body()'s in-flight run document, times 40 wrapped
    reconcile_and_plan advances through backend.mutate_document, and
    asserts every pass was quiescent (nothing reconciled, nothing
    launched): a pass that did work means the fixture or the planner
    broke and the region timed the wrong shape.  ``variant`` names the
    run namespace, the tempdir prefix and the failure messages.
    ``assert_unchanged`` adds the dependency-free variant's keep-path
    contract: an advance reporting ``changed`` also counts as activity,
    and the document files must be byte-identical afterwards.  The
    chain variant deliberately asserts neither (its docstring says why).
    """
    import asyncio
    import hashlib

    try:
        from cronstable.state import DOC_KEEP
    except ImportError as exc:
        raise Skip("state DOC_KEEP sentinel unavailable: %r" % exc) from None
    ns = dag.DAG_RUN_NS_PREFIX + variant
    run_key = "r"
    passes = 40  # 20 measured under the 50ms rule on Linux

    def _wrap_keep(transform):
        def wrapped(cur):
            new_body, result = transform(cur)
            if not isinstance(new_body, dict):
                return DOC_KEEP, result
            return new_body, result

        return wrapped

    def _digest(files):
        digest = hashlib.sha256()
        for f in sorted(files):
            with open(f, "rb") as handle:
                digest.update(handle.read())
        return digest.hexdigest()

    async def run():
        path = tempfile.mkdtemp(prefix="dag%s-" % variant, dir=_tmpdir())
        backend = _state_backend(path)
        await backend.start()
        try:
            body = build_body()
            await backend.mutate_document(
                ns, run_key, lambda cur, b=body: (b, None)
            )
            before = after = None
            if assert_unchanged:
                doc_files = [
                    os.path.join(root, f)
                    for root, _dirs, files in os.walk(path)
                    for f in files
                ]
                before = _digest(doc_files)
            results = []
            t0 = time.perf_counter()
            for _ in range(passes):
                transform = _wrap_keep(
                    dag.reconcile_and_plan(
                        spec,
                        now + 60.0,
                        "bench-proc",
                        "bench-host",
                        lambda pid: True,
                    )
                )
                _, result = await backend.mutate_document(
                    ns, run_key, transform
                )
                results.append(result)
            dt = time.perf_counter() - t0
            if assert_unchanged:
                after = _digest(doc_files)
        finally:
            await backend.stop()
            shutil.rmtree(path, ignore_errors=True)
        for result in results:
            reconciled = getattr(result, "reconciled", 0)
            advance = getattr(result, "advance", None)
            changed = (
                assert_unchanged
                and advance is not None
                and getattr(advance, "changed", False)
            )
            launches = advance is not None and getattr(advance, "launches", [])
            if reconciled or changed or launches:
                raise RuntimeError(
                    "%s advance was not quiescent (%r); the metric would "
                    "time the wrong shape" % (variant, result)
                )
        if assert_unchanged and before != after:
            raise RuntimeError(
                "quiescent advances rewrote the document; the keep path "
                "did not hold"
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("reconcile_and_plan signature changed: %r" % exc) from None


@bench(
    "dag.advance_quiescent_1k",
    "dag",
    detail="40 quiescent advances of a 1k-task in-flight run document",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_advance_quiescent():
    """The steady-state advance of a large run idling in flight.

    Guards the single-RMW quiescent advance: _is_quiescent's one-sided
    contract means a conservatively-False predicate (or a re-added deep
    copy) stays green everywhere else while every active run pays a full
    1k-entry document copy at least once a minute.  Times
    backend.mutate_document with a wrapped reconcile_and_plan directly, NOT
    advance_one, whose locked wrapper swallows all exceptions -- a broken
    fixture there would silently time a no-op.  Every pass must prove
    quiescence (nothing reconciled, nothing changed) and the document file
    must be byte-unchanged afterwards.
    """
    dag = _dag_module()
    if not hasattr(dag, "reconcile_and_plan"):
        raise Skip("dag.reconcile_and_plan not present")
    n = _n(1000)
    now = 1700000000.0
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("quiet", tasks)

    def _inflight_body():
        body = dag.new_run_body(
            dag="quiet",
            run_key="r",
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        for task in tasks:
            entry = body["tasks"][task.id]
            entry["state"] = "running"
            entry["proc"] = "bench-proc"
            entry["pid"] = 4242
            entry["attempt"] = 0
        return body

    return _run_quiescent_advance(
        dag,
        spec,
        _inflight_body,
        variant="quiet",
        now=now,
        assert_unchanged=True,
    )


@bench(
    "dag.advance_quiescent_chain",
    "dag",
    detail="40 quiescent advances of a 1k-task CHAIN mid-flight",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_advance_quiescent_chain():
    """The same steady-state advance over a DEPENDENT graph.

    dag.advance_quiescent_1k builds 1000 tasks with no depends_on at all,
    which is the one topology whose readiness check is a constant: an empty
    dependency list short-circuits before any upstream state is consulted.
    Every real orchestration DAG is the other shape, and there the per-pass
    cost is driven by the dependency walk, resolved once per pending task
    per pass, every pass, for the whole life of the run.  A chain is that
    walk's worst realistic case and its fan-in twin (the last task of a
    chain is a 1-wide fan-in resolved 1000 times).

    The run is parked mid-flight: the first half finished, one task running
    under a live pid, the rest blocked behind it.  Nothing can become ready,
    so a pass that reconciles or launches anything means the fixture (or the
    planner) broke and the region timed the wrong shape.  Unlike the
    dependency-free twin this does NOT assert the document is byte-identical
    afterwards: whether a mixed-state graph takes the keep path is the
    planner's business, and pinning it here would make the metric fail on a
    legitimate change instead of measuring it.
    """
    dag = _dag_module()
    if not hasattr(dag, "reconcile_and_plan"):
        raise Skip("dag.reconcile_and_plan not present")
    n = _n(1000, floor=4)
    now = 1700000000.0
    tasks = [dag.TaskSpec(id="t0")]
    for i in range(1, n):
        tasks.append(dag.TaskSpec(id="t%d" % i, depends_on=("t%d" % (i - 1),)))
    spec = dag.DagSpec.build("chain", tasks)
    running_at = n // 2

    def _inflight_body():
        body = dag.new_run_body(
            dag="chain",
            run_key="r",
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        for i, task in enumerate(tasks):
            entry = body["tasks"][task.id]
            if i < running_at:
                entry["state"] = "success"
                entry["attempt"] = 1
                entry["startedAt"] = now
                entry["finishedAt"] = now + 1.0
                entry["exitCode"] = 0
            elif i == running_at:
                entry["state"] = "running"
                entry["proc"] = "bench-proc"
                entry["pid"] = 4242
                entry["attempt"] = 1
                entry["startedAt"] = now
        return body

    return _run_quiescent_advance(
        dag,
        spec,
        _inflight_body,
        variant="chain",
        now=now,
        assert_unchanged=False,
    )


@bench(
    "dag.adopt_scan_500",
    "dag",
    detail="8 warm keys-only adoption scans over 500 runs (50 foreign)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.020,
)
def bench_dag_adopt_scan():
    """The every-30s orphan-adoption sweep each node pays per dag.

    Guards the terminal-key cache: without it every pass body-reads every
    run again.  One UNTIMED full pass warms _terminal_run_keys first --
    cold-healthy and broken-cache are indistinguishable (both body-read
    everything), so timing the cold shape would miss the metric's own
    target.  The warm timed passes then pay one key listing plus body
    reads and lease probes for only the foreign-held active runs.  Foreign
    leases carry an effectively infinite TTL: a lapse mid-suite would
    trigger real adoption plus a renew-loop task.  _adopt_one_dag is
    called directly (_adopt_orphans swallows per-dag exceptions and would
    false-green a broken fixture), and post-conditions are asserted.
    """
    import asyncio

    dag = _dag_module()
    Cron = _cron_cls()
    total = _n(500)
    foreign = max(1, total // 10)
    runs_terminal = total - foreign
    passes = 8
    ns = dag.DAG_RUN_NS_PREFIX + "benchdag"

    def _seeded_adopt_store():
        """The 500-run store, built ONCE per process: the timed scans never
        mutate it (foreign leases hold, nothing adopts), so per-repeat
        seeding would be pure fixture waste.  Documents and lease files are
        independent, so seeding gathers in chunks."""

        def build():
            path = os.path.join(_tmpdir(), "dag-adopt")
            os.makedirs(path, exist_ok=True)

            def _body(i, active):
                key = "r%05d" % i
                return {
                    "dag": "benchdag",
                    "runKey": key,
                    "runId": "id%d" % i,
                    "state": "running" if active else "success",
                    "kind": "scheduled",
                    "createdAt": 1700000000.0 + i,
                    "updatedAt": 1700000000.0 + i,
                    "tasks": {},
                    "mapped": {},
                }

            async def seed():
                backend = _state_backend(path)
                await backend.start()
                try:
                    for base in range(0, total, 64):
                        await asyncio.gather(
                            *(
                                backend.mutate_document(
                                    ns,
                                    "r%05d" % i,
                                    lambda cur, b=_body(
                                        i, i < foreign
                                    ): (b, None),
                                )
                                for i in range(base, min(base + 64, total))
                            )
                        )
                    leases = await asyncio.gather(
                        *(
                            backend.acquire_lease(
                                dag.DAG_LEASE_PREFIX
                                + "benchdag/r%05d" % i,
                                "foreign-node",
                                10.0**9,
                            )
                            for i in range(foreign)
                        )
                    )
                    if any(lease is None for lease in leases):
                        raise RuntimeError(
                            "could not seed the foreign leases"
                        )
                finally:
                    await backend.stop()

            asyncio.run(seed())
            return path

        return fixture("adopt_store_500", build)

    path = _seeded_adopt_store()

    async def run():
        cfg = "state:\n  path: %s\n%s" % (
            path.replace("\\", "/"),
            _BENCH_DAG_YAML,
        )
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        dagsched = getattr(cron, "_dag", None)
        if dagsched is None or not hasattr(dagsched, "_adopt_one_dag"):
            raise Skip("dag scheduler adoption seam not present")
        dagcfg = cron.cron_dags.get("benchdag")
        if dagcfg is None:
            raise Skip("benchdag not configured")
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        try:
            # untimed full pass: warm the terminal-key cache
            await dagsched._adopt_one_dag(
                backend, "benchdag", dagcfg, full=True
            )
            t0 = time.perf_counter()
            for _ in range(passes):
                await dagsched._adopt_one_dag(
                    backend, "benchdag", dagcfg, full=False
                )
            dt = time.perf_counter() - t0
            # post-conditions BEFORE teardown (shutdown may clear this state)
            owned = getattr(dagsched, "_owned", None)
            if owned:
                raise RuntimeError(
                    "adoption scan adopted %d runs; the foreign leases did "
                    "not hold and the region timed real adoption"
                    % len(owned)
                )
            known = getattr(dagsched, "_terminal_run_keys", {}).get(
                "benchdag"
            )
            if known is None or len(known) != runs_terminal:
                raise RuntimeError(
                    "terminal-key cache holds %r keys, expected %d; the "
                    "warm pass did not warm"
                    % (None if known is None else len(known), runs_terminal)
                )
        finally:
            # the store is a shared per-process fixture; only the Cron and
            # its backend binding are per-repeat
            await _teardown_cron(cron)
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("adoption scan signature changed: %r" % exc) from None


def _cron_cls():
    try:
        from cronstable.cron import Cron
    except ImportError as exc:
        raise Skip("cronstable.cron unavailable: %r" % exc) from None
    return Cron


async def _teardown_cron(cron):
    """Best-effort release of a bench Cron's resources (no job API was started
    here, so just the dag scheduler and the state backend)."""
    import contextlib

    dagsched = getattr(cron, "_dag", None)
    if dagsched is not None and hasattr(dagsched, "shutdown"):
        with contextlib.suppress(Exception):
            await dagsched.shutdown()
    backend = getattr(cron, "state_backend", None)
    if backend is not None:
        with contextlib.suppress(Exception):
            await backend.stop()


_BENCH_DAG_YAML = (
    "dags:\n  - name: benchdag\n    tasks:\n"
    "      - id: a\n        command: 'x'\n"
)


def _seeded_dag_runs():
    """A dag namespace pre-seeded with terminal run documents, built once."""

    def build():
        import asyncio

        from cronstable import dag

        path = os.path.join(_tmpdir(), "dag-runs")
        os.makedirs(path, exist_ok=True)
        runs = _n(50)
        ns = dag.DAG_RUN_NS_PREFIX + "benchdag"

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            for i in range(runs):
                body = {
                    "dag": "benchdag",
                    "runKey": "r%05d" % i,
                    "runId": "id%d" % i,
                    "state": "success",
                    "kind": "scheduled",
                    "createdAt": 1700000000.0 + i,
                    "updatedAt": 1700000000.0 + i,
                    "tasks": {},
                    "mapped": {},
                }
                await backend.mutate_document(
                    ns, "r%05d" % i, lambda cur, b=body: (b, None)
                )
            await backend.stop()

        asyncio.run(seed())
        return path

    return fixture("seeded_dag_runs", build)


@bench(
    "dag.list_dags_warm_x300",
    "dag",
    detail="300 list_dags steady polls over a dag with 50 terminal runs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_list_dags_warm():
    """The /dags dashboard poll's rollup for one dag with many terminal runs.

    A release that caches immutable terminal runs re-reads nothing on the
    steady poll; one that re-reads every run document each call pays the
    full scan every time.  One poll is a fraction of a millisecond, so 300
    of them make the timed region.
    """
    import asyncio

    Cron = _cron_cls()
    path = _seeded_dag_runs()
    runs = _n(50)
    polls = _n(300, floor=4)
    cfg = "state:\n  path: %s\n%s" % (
        path.replace("\\", "/"),
        _BENCH_DAG_YAML,
    )

    async def run():
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        dagsched = getattr(cron, "_dag", None)
        if dagsched is None or not hasattr(dagsched, "list_dags"):
            await _teardown_cron(cron)
            raise Skip("cron._dag.list_dags not present")
        try:
            await dagsched.list_dags()  # warm any terminal-run cache
            t0 = time.perf_counter()
            for _ in range(polls):
                # Keep measuring the ROLLUP (keys listing + cached
                # summaries): the short-TTL result memo would otherwise
                # serve every timed call from one dict hit.  getattr, so a
                # release predating the memo clears a throwaway dict.
                getattr(dagsched, "_summaries_memo", {}).clear()
                listing = await dagsched.list_dags()
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        total = listing[0].get("totalRuns") if listing else None
        if total != runs:
            raise RuntimeError(
                "list_dags rolled up %r of %d seeded runs" % (total, runs)
            )
        return dt

    return asyncio.run(run())


@bench(
    "dag.list_runs_warm",
    "dag",
    detail="60 GET /dags/{name}/runs over a dag with 50 terminal runs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_dag_list_runs_warm():
    """The run-list poll behind the dashboard's DAG runs tab.

    dag.list_dags_warm measures the ROLLUP, which lists keys and consults a
    per-key terminal cache; list_runs is the sibling that gets no such help.
    It reads every run document's body on every call, sorts them all, and
    then returns the newest `limit`, so the read grows with retention
    while the answer does not, and the dashboard asks again on every poll
    for as long as the tab is open.  A terminal run is immutable, which is
    what makes the rollup's cache correct and is exactly the property this
    path does not exploit yet.

    Driven through the scheduler's own list_runs (the handler adds routing
    and a JSON encode measured elsewhere), and the row count is asserted so
    a namespace that failed to seed cannot time an empty scan.
    """
    import asyncio

    Cron = _cron_cls()
    path = _seeded_dag_runs()
    runs = _n(50)
    cfg = "state:\n  path: %s\n%s" % (
        path.replace("\\", "/"),
        _BENCH_DAG_YAML,
    )

    async def run():
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        dagsched = getattr(cron, "_dag", None)
        if dagsched is None or not hasattr(dagsched, "list_runs"):
            await _teardown_cron(cron)
            raise Skip("cron._dag.list_runs not present")
        try:
            try:
                rows = await dagsched.list_runs("benchdag", limit=25)
            except TypeError as exc:
                raise Skip(
                    "list_runs signature changed: %r" % exc
                ) from None
            if not rows:
                raise RuntimeError(
                    "list_runs returned %r for a namespace seeded with %d "
                    "runs" % (rows, runs)
                )
            # 60 polls: a minute of an open runs tab, and one call measures
            # far under the harness's 50ms rule (the metric would gate at an
            # effective ~230% against its declared 25%)
            t0 = time.perf_counter()
            for _ in range(60):
                # Keep measuring the READ-EVERY-BODY path this docstring
                # promises: the short-TTL summaries memo would otherwise
                # serve all 60 calls from the untimed warm call's product
                # and a regression in the real uncached path could no
                # longer fire the gate.  getattr, so a release predating
                # the memo clears a throwaway dict and changes nothing.
                getattr(dagsched, "_summaries_memo", {}).clear()
                await dagsched.list_runs("benchdag", limit=25)
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        return dt

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# dag runtime and durable state: a run through the real DagScheduler, run GC,
# the full-listing sweeps, the fan-in barrier, recovery planning, document
# sizes, and the record store's prune, lookup and inventory paths.
# ---------------------------------------------------------------------------
def _dagstate_wide_spec(dag, name, tasks):
    return dag.DagSpec.build(
        name, [dag.TaskSpec(id="t%d" % i) for i in range(tasks)]
    )


def _dagstate_finished_body(dag, spec, name, index):
    """One finished run document of ``spec``, sized like a real run."""
    stamp = 1700000000.0 + index
    logical = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        minutes=index
    )
    body = dag.new_run_body(
        dag=name,
        run_key="r%05d" % index,
        run_id="id%05d" % index,
        logical_date=logical.isoformat(),
        kind="scheduled",
        now=stamp,
        spec=spec,
    )
    for entry in body["tasks"].values():
        entry.update(
            state="success",
            startedAt=stamp,
            finishedAt=stamp + 1.5,
            exitCode=0,
            host="bench-host",
            updatedAt=stamp + 1.5,
        )
    body["state"] = "success"
    body["updatedAt"] = stamp + 2.0
    return body


def _dagstate_seed_runs(path, dag, spec, name, count):
    """Write ``count`` finished runs of ``spec`` into the store at ``path``."""
    import asyncio

    ns = dag.DAG_RUN_NS_PREFIX + name

    async def seed():
        backend = _state_backend(path)
        await backend.start()
        try:
            for base in range(0, count, 32):
                bodies = [
                    _dagstate_finished_body(dag, spec, name, i)
                    for i in range(base, min(base + 32, count))
                ]
                await asyncio.gather(
                    *(
                        backend.mutate_document(
                            ns, body["runKey"], lambda cur, b=body: (b, None)
                        )
                        for body in bodies
                    )
                )
        finally:
            await backend.stop()

    asyncio.run(seed())


def _dagstate_dag_yaml(path, retain):
    return (
        "state:\n  path: %s\n"
        "dags:\n  - name: benchdag\n    retainRuns: %d\n    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    ) % (path.replace("\\", "/"), retain)


async def _dagstate_cron(path, yaml):
    """A Cron bound to a started backend at ``path``, and its scheduler."""
    Cron = _cron_cls()
    try:
        cron = Cron(None, config_yaml=yaml)
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    dagsched = getattr(cron, "_dag", None)
    if dagsched is None:
        raise Skip("cron._dag not present")
    backend = _state_backend(path)
    await backend.start()
    cron.state_backend = backend
    cron._state_configured = True
    return cron, backend, dagsched


@bench(
    "dag.gc_excess_60",
    "dag",
    detail="_gc_one_dag deleting 60 of 110 finished runs of 200 tasks",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_gc_excess():
    """The retention pass of a DAG that ran 60 times since the last one.

    The pass runs inside the scheduler's single-flight service task, so
    scheduled fires, sensor pokes and due retries of every DAG wait for it.
    It guards the cost of one deleted run: a pass that lists the run
    namespace again for each deletion parses every retained document that
    many times.  Each repeat works on its own copy of the seeded store,
    because the pass consumes it, and the surviving key count is asserted.
    """
    import asyncio

    dag = _dag_module()
    retained = _n(50, floor=2)
    excess = _n(60, floor=2)
    tasks = _n(200, floor=4)

    def build():
        path = os.path.join(_tmpdir(), "dagstate-gc-template")
        os.makedirs(path, exist_ok=True)
        spec = _dagstate_wide_spec(dag, "benchdag", tasks)
        _dagstate_seed_runs(path, dag, spec, "benchdag", retained + excess)
        return path

    template = fixture("dagstate_gc_template", build)
    path = tempfile.mkdtemp(prefix="dagstate-gc-", dir=_tmpdir())
    shutil.copytree(template, path, dirs_exist_ok=True)

    async def run():
        cron, backend, dagsched = await _dagstate_cron(
            path, _dagstate_dag_yaml(path, retained)
        )
        try:
            if not hasattr(dagsched, "_gc_one_dag"):
                raise Skip("dag scheduler GC seam not present")
            dagcfg = cron.cron_dags["benchdag"]
            t0 = time.perf_counter()
            await dagsched._gc_one_dag(backend, "benchdag", dagcfg)
            dt = time.perf_counter() - t0
            left = await backend.list_document_keys(
                dag.DAG_RUN_NS_PREFIX + "benchdag"
            )
        finally:
            await _teardown_cron(cron)
        if left is None or len(left) != retained:
            raise RuntimeError(
                "GC left %r runs, expected %d; the region did not delete "
                "the excess" % (None if left is None else len(left), retained)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("dag GC signature changed: %r" % exc) from None
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "dag.full_sweep_50x200",
    "dag",
    detail="boot reconcile + full adopt + cold list_dags x3, 50 runs x 200",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_full_sweep():
    """The scheduler passes that parse every retained run document.

    Boot reconciliation, the full adoption pass (the first pass after boot,
    then every ten minutes per DAG per node) and the first /dags rollup each
    list a DAG's whole run namespace.  dag.adopt_scan_500 times the warm
    keys-only scan over empty documents, so the full passes over documents
    of real size are measured here.  Nothing is active, so a pass that
    adopts a run, or a rollup that misses one, fails the benchmark.
    """
    import asyncio

    dag = _dag_module()
    runs = _n(50, floor=3)
    tasks = _n(200, floor=4)
    rounds = 3

    def build():
        path = os.path.join(_tmpdir(), "dagstate-sweep")
        os.makedirs(path, exist_ok=True)
        spec = _dagstate_wide_spec(dag, "benchdag", tasks)
        _dagstate_seed_runs(path, dag, spec, "benchdag", runs)
        return path

    path = fixture("dagstate_sweep_store", build)

    async def run():
        cron, backend, dagsched = await _dagstate_cron(
            path, _dagstate_dag_yaml(path, runs)
        )
        try:
            for attr in ("reconcile_on_boot", "_adopt_one_dag", "list_dags"):
                if not hasattr(dagsched, attr):
                    raise Skip("dag scheduler lacks %s" % attr)
            dagcfg = cron.cron_dags["benchdag"]
            t0 = time.perf_counter()
            for _ in range(rounds):
                await dagsched.reconcile_on_boot()
                await dagsched._adopt_one_dag(
                    backend, "benchdag", dagcfg, full=True
                )
                # a cold rollup: no memo and no per-run summary to reuse
                getattr(dagsched, "_summaries_memo", {}).clear()
                getattr(dagsched, "_dag_summary_cache", {}).clear()
                listing = await dagsched.list_dags()
            dt = time.perf_counter() - t0
            owned = len(getattr(dagsched, "_owned", ()))
            known = getattr(dagsched, "_terminal_run_keys", {}).get(
                "benchdag", ()
            )
        finally:
            await _teardown_cron(cron)
        total = listing[0].get("totalRuns") if listing else None
        if owned or len(known) != runs or total != runs:
            raise RuntimeError(
                "full sweep saw owned=%d terminal=%d listed=%r of %d "
                "finished runs" % (owned, len(known), total, runs)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("dag sweep signature changed: %r" % exc) from None


def _dagstate_fanout(dag, sinks):
    """``src`` feeds the mapped ``fan``; ``sinks`` tasks wait on ``fan``."""
    tasks = [
        dag.TaskSpec(id="src"),
        dag.TaskSpec(
            id="fan",
            depends_on=("src",),
            expand=dag.ExpandSpec(from_task="src", key="items"),
        ),
    ]
    tasks.extend(
        dag.TaskSpec(id="s%d" % i, depends_on=("fan",)) for i in range(sinks)
    )
    spec = dag.DagSpec.build("fanin", tasks)
    dag.validate_graph(spec)
    return spec


@bench(
    "dag.fanin_barrier_1k",
    "dag",
    detail="40 advances of 12 tasks waiting on a 1k-wide fan-out's last item",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_fanin_barrier():
    """The fan-in barrier: tasks downstream of a mapped task mid-drain.

    A task that depends on a mapped task is ready once every instance of
    the fan-out is terminal, so each advance folds the instances for each
    waiting dependent, and the fold runs to the first instance still going.
    Here 999 of 1000 instances succeeded and the last is in flight, which
    is the longest fold a pass can pay.  dag.mapped_drain_256 has nothing
    downstream of its fan-out and dag.advance_quiescent_chain has no mapped
    upstream, so no other metric reaches the fold.
    """
    dag = _dag_module()
    for attr in ("reconcile_and_plan", "plan_and_claim", "ExpandSpec"):
        if not hasattr(dag, attr):
            raise Skip("dag lacks %s" % attr)
    width = _n(1000, floor=8)
    now = 1700000000.0
    try:
        spec = _dagstate_fanout(dag, 12)
    except TypeError as exc:
        raise Skip("dag mapped-spec signature changed: %r" % exc) from None

    def _draining_body():
        body = dag.new_run_body(
            dag="fanin",
            run_key="r",
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        body["tasks"]["src"].update(
            state="success", exitCode=0, finishedAt=now
        )
        expand = dag.plan_and_claim(
            spec, now, "bench-proc", "bench-host", {"fan": list(range(width))}
        )
        body, _result = expand(body)
        for i in range(width):
            entry = body["tasks"]["fan#%d" % i]
            if i < width - 1:
                entry.update(
                    state="success", exitCode=0, finishedAt=now, proc=None
                )
            else:
                entry.update(
                    state="running", proc="bench-proc", pid=4242, attempt=0
                )
        return body

    return _run_quiescent_advance(
        dag,
        spec,
        _draining_body,
        variant="fanin",
        now=now,
        assert_unchanged=True,
    )


@bench(
    "dag.advance_waiting_mapped_1k",
    "dag",
    detail="40 advances of a 1k-task run with a mapped task still to expand",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_advance_waiting_mapped():
    """The advance of a run that waits on a mapped task's upstream.

    A mapped placeholder can be moved by expansion or by propagation while
    its upstreams are still running, so the read-only quiescence scan hands
    such a run to the full pass: a deep copy and a walk of every entry that
    end by keeping the document.  dag.advance_quiescent_1k times the run
    the scan settles on its own; this one times the run it cannot.
    """
    dag = _dag_module()
    for attr in ("reconcile_and_plan", "ExpandSpec"):
        if not hasattr(dag, attr):
            raise Skip("dag lacks %s" % attr)
    n = _n(1000, floor=4)
    now = 1700000000.0
    try:
        tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
        tasks.append(
            dag.TaskSpec(
                id="fan",
                depends_on=("t0",),
                expand=dag.ExpandSpec(from_task="t0", key="items"),
            )
        )
        spec = dag.DagSpec.build("waitmap", tasks)
        dag.validate_graph(spec)
    except TypeError as exc:
        raise Skip("dag mapped-spec signature changed: %r" % exc) from None

    def _inflight_body():
        body = dag.new_run_body(
            dag="waitmap",
            run_key="r",
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        for i in range(n):
            body["tasks"]["t%d" % i].update(
                state="running", proc="bench-proc", pid=4242, attempt=0
            )
        return body

    return _run_quiescent_advance(
        dag,
        spec,
        _inflight_body,
        variant="waitmap",
        now=now,
        assert_unchanged=True,
    )


def _dagstate_drive_run(tasks, chain):
    """Take one run of ``tasks`` tasks to success through the DagScheduler.

    The subprocess is the only stub: a RunningJob subclass whose start()
    records a pid and an exit code where the real one forks.  The reaper's
    part is played here: each launched instance goes to on_task_finished,
    flush_completions runs once per batch as the reaper runs it, and the
    advance it spawns is awaited.  No service tick runs, so a pass that
    left work for the next tick is advanced directly.  Returns the seconds
    from trigger_run to the released lease.
    """
    import asyncio

    try:
        from cronstable import dagrun
    except ImportError as exc:
        raise Skip("cronstable.dagrun unavailable: %r" % exc) from None
    real_job = getattr(dagrun, "RunningJob", None)
    if real_job is None:
        raise Skip("dagrun.RunningJob seam not present")

    class _Proc:
        pid = 4242

    class _Started(real_job):
        async def start(self):
            self.proc = _Proc()
            self.retcode = 0

    path = tempfile.mkdtemp(prefix="dagstate-run-", dir=_tmpdir())
    lines = [
        "state:",
        "  path: %s" % path.replace("\\", "/"),
        "dags:",
        "  - name: benchdag",
        "    tasks:",
    ]
    for i in range(tasks):
        lines.append("      - id: t%d" % i)
        lines.append("        command: 'x'")
        if chain and i:
            lines.append("        dependsOn:")
            lines.append("          - t%d" % (i - 1))
    yaml = "\n".join(lines) + "\n"

    async def run():
        cron, _backend, dagsched = await _dagstate_cron(path, yaml)
        launched = []
        try:
            for attr in (
                "trigger_run",
                "advance_one",
                "on_task_finished",
                "flush_completions",
                "get_run",
                "_owned",
            ):
                if not hasattr(dagsched, attr):
                    raise Skip("dag scheduler lacks %s" % attr)
            if not hasattr(cron, "_pending_state_writes"):
                raise Skip("cron._pending_state_writes not present")
            cron._add_running_instance = launched.append
            dagrun.RunningJob = _Started
            passes = 0
            t0 = time.perf_counter()
            run_key = await dagsched.trigger_run("benchdag")
            ref = ("benchdag", run_key)
            while ref in dagsched._owned:
                batch = launched[:]
                del launched[:]
                if batch:
                    for running in batch:
                        await dagsched.on_task_finished(running)
                    await dagsched.flush_completions()
                    while cron._pending_state_writes:
                        await asyncio.wait(set(cron._pending_state_writes))
                else:
                    await dagsched.advance_one(ref)
                passes += 1
                if passes > 4 * tasks + 16:
                    raise RuntimeError(
                        "the run did not finish in %d passes" % passes
                    )
            dt = time.perf_counter() - t0
            body = await dagsched.get_run("benchdag", run_key)
        finally:
            dagrun.RunningJob = real_job
            await _teardown_cron(cron)
        entries = (body or {}).get("tasks", {})
        done = sum(1 for e in entries.values() if e.get("state") == "success")
        if body is None or body.get("state") != "success" or done != tasks:
            raise RuntimeError(
                "the run ended %r with %d of %d tasks succeeded; the "
                "region timed the wrong shape"
                % (None if body is None else body.get("state"), done, tasks)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("dag scheduler signature changed: %r" % exc) from None
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "dag.run_chain_100",
    "dag",
    detail="one 100-task chain run through DagScheduler, process stubbed",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_run_chain():
    """A whole run through the real scheduler: the task hand-off cost.

    Every other dag metric stops at dag.py's transforms.  This one runs
    create, own, advance, launch, pid stamp, completion and flush for each
    task of a chain, which is three document rewrites per task plus the
    driver's own walks of the document after each advance.  It guards the
    time from one task's exit to the next task's start, summed over the
    chain.  The final state and the success count are asserted.
    """
    return _dagstate_drive_run(_n(100, floor=3), chain=True)


@bench(
    "dag.run_wide_500",
    "dag",
    detail="one 500-task dependency-free run through DagScheduler",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_dag_run_wide():
    """The same end-to-end run over a wide DAG, claimed 32 tasks a pass.

    The chain pays its rewrites on a small document many times; this run
    pays them on a 500-entry document once per claim batch, with the launch
    loop and the batched completion flush carrying 32 instances each.
    """
    return _dagstate_drive_run(_n(500, floor=3), chain=False)


@bench(
    "dag.recover_plan_from_1k",
    "dag",
    detail="6 recovery plans from the head of a 1k-task chain",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_dag_recover_plan_from():
    """Planning a rerun from the first task of a deep chain.

    recovery.plan runs on the event loop, twice per operator recovery (the
    preview and the accept).  A rerun from the head selects every task
    downstream, so this is the deepest closure a 1k-task graph has, next to
    the configuration digest and the plan token every plan computes.
    """
    dag = _dag_module()
    try:
        from cronstable import recovery
        from cronstable.config import DagConfig
    except ImportError as exc:
        raise Skip("recovery planning unavailable: %r" % exc) from None
    n = _n(1000, floor=4)

    def build():
        tasks = []
        for i in range(n):
            task = {"id": "t%d" % i, "command": "x"}
            if i:
                task["dependsOn"] = ["t%d" % (i - 1)]
            tasks.append(task)
        try:
            config = DagConfig({"name": "chain", "tasks": tasks})
        except TypeError as exc:
            raise Skip("DagConfig signature changed: %r" % exc) from None
        source = _dagstate_finished_body(dag, config.spec, "chain", 0)
        return config, source

    config, source = fixture("dagstate_recover_chain", build)
    plan = None
    try:
        t0 = time.perf_counter()
        for _ in range(6):
            plan = recovery.plan(config, source, mode="from", tasks=("t0",))
        dt = time.perf_counter() - t0
    except TypeError as exc:
        raise Skip("recovery.plan signature changed: %r" % exc) from None
    if plan is None or len(plan["tasks"]) != n:
        raise RuntimeError(
            "the plan selected %r of %d tasks"
            % (None if plan is None else len(plan["tasks"]), n)
        )
    return dt


def _dagstate_stored_kb(body):
    """The size, in KB, of ``body`` as the store writes a document."""
    try:
        from cronstable import _json
        from cronstable.state import SCHEMA_VERSION
    except ImportError as exc:
        raise Skip("document encoding unavailable: %r" % exc) from None
    raw = _json.dumps_bytes(
        {"schemaVersion": SCHEMA_VERSION, "data": body}, sort_keys=True
    )
    return len(raw) / 1024.0


@bench(
    "dag.run_doc_bytes_1k",
    "dag",
    detail="stored size of a finished 1k-task run document",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_pct=5.0,
    gate_floor=1.0,
)
def bench_dag_run_doc_bytes():
    """The bytes one task entry adds to a run document.

    Every advance, pid stamp and completion reads and rewrites the whole
    document, so a field added to each entry is paid on every one of them.
    The duration metrics see that only through their noise; the size is
    exact, which is why this gate is 5%.
    """
    dag = _dag_module()
    if not hasattr(dag, "new_run_body"):
        raise Skip("dag.new_run_body not present")
    spec = _dagstate_wide_spec(dag, "wide", _n(1000, floor=10))
    return _dagstate_stored_kb(_dagstate_finished_body(dag, spec, "wide", 0))


@bench(
    "dag.mapped_doc_bytes_1k",
    "dag",
    detail="stored size of a run document after a 1k x 256 B fan-out",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_pct=5.0,
    gate_floor=1.0,
)
def bench_dag_mapped_doc_bytes():
    """What a mapped fan-out's item list costs the run document.

    Expansion records the list on the mapped task and each item again on
    its instance, so the document carries every item twice and each later
    rewrite of the run carries both copies.  The size is exact; a third
    copy, or a per-instance field, moves it.
    """
    dag = _dag_module()
    for attr in ("plan_and_claim", "ExpandSpec", "new_run_body"):
        if not hasattr(dag, attr):
            raise Skip("dag lacks %s" % attr)
    width = _n(1000, floor=10)
    now = 1700000000.0
    try:
        spec = _dagstate_fanout(dag, 0)
        body = dag.new_run_body(
            dag="fanin",
            run_key="r",
            run_id="rid",
            logical_date=None,
            kind="scheduled",
            now=now,
            spec=spec,
        )
        body["tasks"]["src"].update(
            state="success", exitCode=0, finishedAt=now
        )
        items = ["%06d" % i + "x" * 250 for i in range(width)]
        expand = dag.plan_and_claim(
            spec, now, "bench-proc", "bench-host", {"fan": items}
        )
        body, _result = expand(body)
    except TypeError as exc:
        raise Skip("dag planning signature changed: %r" % exc) from None
    if len(body.get("mapped", {}).get("fan", {}).get("items", ())) != width:
        raise RuntimeError("the fan-out did not expand to %d items" % width)
    return _dagstate_stored_kb(body)


# ---------------------------------------------------------------------------
# state: the durable filesystem backend (async, real disk I/O).
# ---------------------------------------------------------------------------


def _state_backend(path):
    try:
        from cronstable.state import FilesystemStateBackend
    except ImportError as exc:
        raise Skip("cronstable.state unavailable: %r" % exc) from None
    config = {"path": path, "topology": "single-node", "deploymentId": None}
    try:
        return FilesystemStateBackend(config, lambda: "bench-jobset")
    except Exception as exc:
        raise Skip("state backend construction failed: %r" % exc) from None


def _state_dir_with_records():
    """A store pre-seeded with records, built once (untimed)."""

    def build():
        import asyncio

        path = os.path.join(_tmpdir(), "state-seeded")
        os.makedirs(path, exist_ok=True)
        n = _n(2000)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            for i in range(n):
                await backend.append_record(
                    "runs", {"outcome": "success", "seq": i, "duration": 1.25}
                )
            await backend.stop()

        asyncio.run(seed())
        return path, n

    return fixture("state_seeded", build)


@bench(
    "state.append_1k",
    "state",
    detail="append_record x1k to a fresh store",
    repeats=(3, 2, 1),
    gate_floor=0.050,
)
def bench_state_append():
    import asyncio

    n = _n(1000)
    path = tempfile.mkdtemp(prefix="append-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        for i in range(n):
            await backend.append_record(
                "runs", {"outcome": "success", "seq": i, "duration": 1.25}
            )
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "state.derive_max_cold",
    "state",
    detail="first derive_max over 2k records (no memo)",
    repeats=(3, 2, 1),
)
def bench_derive_max_cold():
    import asyncio

    path, _ = _state_dir_with_records()

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        await backend.derive_max("runs", "seq")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.derive_max_warm",
    "state",
    detail="200 memoized derive_max calls",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_derive_max_warm():
    import asyncio

    path, _ = _state_dir_with_records()
    n = _n(200)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        await backend.derive_max("runs", "seq")  # warm the memo
        t0 = time.perf_counter()
        for _ in range(n):
            await backend.derive_max("runs", "seq")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.list_records_2k",
    "state",
    detail="list_records over 2k records",
    repeats=(3, 2, 1),
)
def bench_list_records():
    import asyncio

    path, _ = _state_dir_with_records()

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        await backend.list_records("runs")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.list_records_warm",
    "state",
    detail="20 repeat list_records over an UNCHANGED 2k-record stream",
    repeats=(3, 2, 1),
    gate_floor=0.010,
)
def bench_list_records_warm():
    """Repeat reads of a stream nothing has written to.

    state.list_records_2k reads each record exactly once, so it cannot
    distinguish "the read is fast" from "the read happens every time".  The
    daemon's readers are the opposite shape: the retry claim scan, the
    depends-on-past gate, the run-history and artifact views and the state
    inspector all re-read the same streams on a poll cadence, and between
    two polls the file on disk is usually byte-identical.  A stat- or
    generation-keyed short circuit would collapse this metric and leave the
    cold one untouched; without it the two are the same number times
    twenty, which is exactly the fact worth pinning.

    Deliberately re-read through ONE backend instance: a per-call backend
    would defeat any in-process memo before it could be measured.
    """
    import asyncio

    path, seeded = _state_dir_with_records()
    n = _n(20, floor=2)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            first = await backend.list_records("runs")
            if len(first) != seeded:
                raise RuntimeError(
                    "seeded stream holds %d records, expected %d"
                    % (len(first), seeded)
                )
            t0 = time.perf_counter()
            for _ in range(n):
                await backend.list_records("runs")
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.mutate_document_1k",
    "state",
    detail="100 read-modify-writes of a 1k-entry durable document",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.050,
)
def bench_state_mutate_document():
    """The durable document RMW, the state backend's other write shape.

    state.append_1k covers the record stream: an append is a short line onto
    the end of a file.  A document write is the opposite: read the WHOLE
    body, run the transform, re-serialize the whole body, write it
    atomically.  It is what every DAG run, every job kv namespace, the
    manifest and the counter snapshot use.  The cost is superlinear in
    document size and paid per mutation, so the entry count matters as much
    as the call count; 1k entries is a mapped fan-out or a busy job's kv
    namespace.  dag.advance_quiescent_1k measures the shape that DOES NOT
    write (the keep path); this one measures the shape that does.

    The mutated field is a counter, not a growing list, so every pass writes
    the same number of bytes and the repeats are comparable.  The final
    counter value is asserted, so a transform whose result was discarded
    cannot time a no-op.
    """
    import asyncio

    entries = _n(1000, floor=10)
    passes = _n(100, floor=2)
    path = tempfile.mkdtemp(prefix="mutdoc-", dir=_tmpdir())

    def _seed(cur):
        body = {
            "kind": "bench",
            "counter": 0,
            "entries": {
                "e%05d" % i: {
                    "state": "success" if i % 3 else "running",
                    "attempt": i % 4,
                    "startedAt": 1700000000.0 + i,
                    "note": "entry %d of the bench document" % i,
                }
                for i in range(entries)
            },
        }
        return body, None

    def _bump(cur):
        if not isinstance(cur, dict):
            raise RuntimeError("document vanished mid-benchmark")
        body = dict(cur)
        body["counter"] = body.get("counter", 0) + 1
        return body, body["counter"]

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            await backend.mutate_document("benchdoc", "d", _seed)
            t0 = time.perf_counter()
            for _ in range(passes):
                _body, counter = await backend.mutate_document(
                    "benchdoc", "d", _bump
                )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        if counter != passes:
            raise RuntimeError(
                "document counter reached %r after %d mutations; the "
                "region did not write" % (counter, passes)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("mutate_document signature changed: %r" % exc) from None
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "state.kv_roundtrip_200",
    "state",
    detail="jobstate kv_set + kv_get x200",
    repeats=(3, 2, 1),
)
def bench_kv_roundtrip():
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    kv_set = getattr(jobstate, "kv_set", None)
    kv_get = getattr(jobstate, "kv_get", None)
    if kv_set is None or kv_get is None:
        raise Skip("jobstate kv API not present")
    n = _n(200)
    path = tempfile.mkdtemp(prefix="kv-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for i in range(n):
                await kv_set(backend, "bench", "key-%d" % (i % 20), {"v": i})
                await kv_get(backend, "bench", "key-%d" % (i % 20))
            dt = time.perf_counter() - t0
        except TypeError as exc:
            raise Skip("jobstate kv signature changed: %r" % exc) from None
        await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "state.lease_renew_200",
    "state",
    detail="renew_lease x200 with an interleaved read_lease (TTL 30s)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.050,
)
def bench_state_lease_renew():
    """Lease renewal is clustering's continuous heartbeat.

    A running cluster renews the leader lease, one lease per cluster-scoped
    running job, one per owned DAG run, and one per jobapi hold, each on a
    ~10s cadence, forever -- and no other metric touches ANY lease
    operation, though the lease lane has its own call pool and its own
    flock + fence verification path.  Slow renewal is correctness-adjacent:
    a blown TTL is lost leadership fleet-wide, or a double-run.

    Public API only (acquire_lease / renew_lease / read_lease), never the
    private renew internals, so the metric survives refactors and times
    what production actually calls.  The renewed lease is threaded into the
    next call the way a real renewer does; a mid-run takeover on a private
    store is impossible, so a None renew hard-fails rather than skips.
    """
    import asyncio

    n = _n(200)
    path = tempfile.mkdtemp(prefix="lease-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        for attr in ("acquire_lease", "renew_lease", "read_lease"):
            if not hasattr(backend, attr):
                raise Skip("state lease API lacks %s" % attr)
        await backend.start()
        try:
            try:
                lease = await backend.acquire_lease(
                    "bench-leader", "bench-holder", 30.0
                )
            except TypeError as exc:
                raise Skip("lease signature changed: %r" % exc) from None
            if lease is None:
                raise RuntimeError(
                    "lease acquire denied on a fresh private store"
                )
            t0 = time.perf_counter()
            for _ in range(n):
                renewed = await backend.renew_lease(lease, 30.0)
                if renewed is None:
                    raise RuntimeError(
                        "renew_lease lost a lease nobody else can hold"
                    )
                lease = renewed
                await backend.read_lease("bench-leader")
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "state.fanout_gather_100",
    "state",
    detail="asyncio.gather of 100 append_record over 20 streams",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_state_fanout_gather():
    """The one shape in the suite that can see the state backend's
    concurrency architecture at all.

    Every blocking op runs on a worker thread behind two semaphores (the
    bulk and lease lanes) -- an anti-starvation design defended by pages of
    comment -- yet every other state metric is a sequential await loop, so
    exactly one worker thread is ever alive and the semaphores never
    contend (measured: a sequential shape moves ~0% between slots=16 and
    slots=1, while a gather moves 13.8x).  A lock held across the await, a
    slot-count change, or a serializing rewrite of the call dispatch moves
    this metric and nothing else.
    """
    import asyncio

    n = _n(100)
    streams = 20
    path = tempfile.mkdtemp(prefix="fanout-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            await asyncio.gather(
                *(
                    backend.append_record(
                        "runs/s%02d" % (i % streams),
                        {"outcome": "success", "seq": i},
                    )
                    for i in range(n)
                )
            )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


_BOOT_JOBS = 60
_BOOT_RECORDS = 25


def _boot_store_yaml(path):
    lines = [
        "state:",
        "  path: %s" % path.replace("\\", "/"),
        "  jobApi:",
        "    enabled: false",
        "jobs:",
    ]
    for i in range(max(2, _n(_BOOT_JOBS))):
        lines.append("  - name: bootjob%03d" % i)
        lines.append("    command: echo boot%03d" % i)
        lines.append('    schedule: "%d %d * * *"' % (i % 60, (i * 7) % 24))
    lines.append("")
    return "\n".join(lines)


def _boot_populated_store():
    """A non-empty store for the boot chain: run ledgers, inflight and
    retry streams for every job, all with terminal/settled records NEWEST
    and an older open/pending record buried underneath.

    The burial is the newest-first regression detector: a boot that reads
    oldest-first sees the buried open/pending records, reconciles phantom
    interrupted runs and re-arms dead retry ladders -- all of which write,
    and the metric asserts the store is byte-identical after every boot.
    (With open/pending NEWEST the first boot would do exactly that for
    real, and min-of-repeats would silently measure the post-mutation
    store.)
    """

    def build():
        import asyncio
        import socket

        Cron = _cron_cls()
        path = os.path.join(_tmpdir(), "boot-store")
        os.makedirs(path, exist_ok=True)
        jobs = max(2, _n(_BOOT_JOBS))
        host = socket.gethostname() or "localhost"

        async def seed_job(backend, i):
            # WITHIN one stream the appends stay sequential: record order is
            # the filename sort (a wall-clock read per worker thread), so
            # parallel appends to one stream could land inverted and put the
            # buried open/pending record on top.  Across JOBS the streams
            # are independent, so jobs seed concurrently.
            name = "bootjob%03d" % i
            run_stream = Cron._run_stream(name)
            for r in range(_BOOT_RECORDS):
                await backend.append_record(
                    run_stream,
                    {
                        "outcome": ("success" if (i + r) % 5 else "failure"),
                        "exit_code": 0 if (i + r) % 5 else 1,
                        "started_at": (
                            "2026-07-01T09:%02d:%02d+00:00"
                            % (r // 60, r % 60)
                        ),
                        "finished_at": (
                            "2026-07-01T10:%02d:%02d+00:00"
                            % (r // 60, r % 60)
                        ),
                        "duration": 12.5,
                        "fail_reason": None,
                    },
                )
            inflight = Cron._inflight_stream(name)
            await backend.append_record(
                inflight,
                {
                    "kind": "open",
                    "host": host,
                    "proc": "dead-proc-token",
                    "pid": 2**22 + i,  # far past any live pid
                },
            )
            await backend.append_record(
                inflight, {"kind": "closed", "host": host}
            )
            retries = Cron._retry_stream(name)
            await backend.append_record(
                retries,
                {
                    "kind": "pending",
                    "attempt": 1,
                    "deadline": 1700000000.0,
                    "host": host,
                },
            )
            await backend.append_record(
                retries, {"kind": "settled", "outcome": "success"}
            )

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                for base in range(0, jobs, 16):
                    await asyncio.gather(
                        *(
                            seed_job(backend, i)
                            for i in range(base, min(base + 16, jobs))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return path, jobs

    return fixture("boot_populated_store", build)


def _store_records_digest(path):
    import hashlib

    root = os.path.join(path, "records")
    if not os.path.isdir(root):
        root = path
    digest = hashlib.sha256()
    for base, _dirs, files in sorted(os.walk(root)):
        for f in sorted(files):
            full = os.path.join(base, f)
            digest.update(full.encode("utf-8", "replace"))
            with open(full, "rb") as handle:
                digest.update(handle.read())
    return digest.hexdigest()


@bench(
    "state.boot_rehydrate_populated",
    "state",
    detail="start_stop_state against a populated store (60 jobs x 25 runs)",
    repeats=(3, 2, 1),
)
def bench_state_boot_rehydrate():
    """The whole restart-to-first-fire boot chain against a NON-EMPTY store:
    backend start + write probe, ledger rehydrate, inflight reconcile,
    counter/pause warm, retry re-arm.  Nothing else in the suite ever boots
    a state backend at all, let alone against existing data.

    Gate 15%, not 25%: the flagship regression class (an extra per-job read
    on the boot chain) measures ~+21%, because added limit=1 reads are
    cheap against the fixed cost -- at 25% the metric could not fail on the
    case it exists for; round scatter is under 1% on Linux.  Honest scope:
    most of the region is worker-thread hops and page-cached file reads;
    what scales through it at full proportion is the CALL COUNT, which is
    exactly the regression class it guards.  A fresh Cron per repeat is
    structural here (one is built inside every call); the store must be
    byte-identical after every boot (see _boot_populated_store).
    """
    import asyncio

    Cron = _cron_cls()
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    if not hasattr(Cron, "start_stop_state"):
        raise Skip("Cron.start_stop_state not present")
    path, jobs = _boot_populated_store()
    yaml_text = _boot_store_yaml(path)
    state_config = parse_config_string(yaml_text, "").state_config
    if state_config is None:
        raise Skip("parsed config carries no state section")
    before = _store_records_digest(path)

    async def run():
        try:
            cron = Cron(None, config_yaml=yaml_text)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        try:
            t0 = time.perf_counter()
            await cron.start_stop_state(state_config)
            dt = time.perf_counter() - t0
            if cron.state_backend is None:
                raise RuntimeError(
                    "start_stop_state left no backend; the boot failed and "
                    "the region timed an error path"
                )
            if len(cron.last_run) != jobs:
                raise RuntimeError(
                    "rehydrate warmed %d of %d jobs" % (len(cron.last_run), jobs)
                )
            armed = [
                name
                for name, st in cron.retry_state.items()
                if not getattr(st, "cancelled", False)
            ]
            if armed:
                raise RuntimeError(
                    "boot armed retry ladders %r against settled-newest "
                    "streams (newest-first read order broke)" % armed
                )
        finally:
            for attr in ("_pause_refresh_task", "_retry_claim_task"):
                task = getattr(cron, attr, None)
                if task is not None:
                    task.cancel()
            await _teardown_cron(cron)
        return dt

    dt = asyncio.run(run())
    if _store_records_digest(path) != before:
        raise RuntimeError(
            "the boot mutated the store; a phantom reconcile/re-arm wrote "
            "records and later repeats would measure a different store"
        )
    return dt


@bench(
    "state.list_documents_600",
    "state",
    detail="8 uncached list_documents sweeps over a 600-document namespace",
    repeats=(3, 2, 1),
    gate_pct=15.0,
    gate_floor=0.012,
)
def bench_state_list_documents():
    """The read-every-document-body sweep behind GET /dags on every cold
    cache (so after every restart), GET /state/documents, the GC XCom
    keep-set and jobstate.kv_list.  Touched by nothing else in the suite;
    instrumentation shows the region does 8x600 real body reads with no
    cache.  Honest scope: roughly half is stdlib json decode of page-cached
    files, but per-document CPU added in the state layer (a validation
    walk, a defensive deepcopy) scales straight through it.  Seeded via
    mutate_document (never kv_set, which stamps a wall-clock time and
    would persist different bytes on the two sides), gathered in chunks.
    """
    import asyncio

    docs_n = max(_n(600), 8)

    def build():
        path = os.path.join(_tmpdir(), "list-docs")
        os.makedirs(path, exist_ok=True)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                for base in range(0, docs_n, 64):
                    await asyncio.gather(
                        *(
                            backend.mutate_document(
                                "benchdocs",
                                "d%05d" % i,
                                lambda cur, i=i: (
                                    {
                                        "key": "d%05d" % i,
                                        "value": {"seq": i, "payload": "x" * 64},
                                        "updatedAt": 1700000000.0,
                                    },
                                    None,
                                ),
                            )
                            for i in range(base, min(base + 64, docs_n))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return path

    path = fixture("list_docs_600", build)

    async def run():
        backend = _state_backend(path)
        if not hasattr(backend, "list_documents"):
            raise Skip("list_documents not present")
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(8):
                docs = await backend.list_documents("benchdocs")
                if docs is None or len(docs) != docs_n:
                    raise RuntimeError(
                        "list_documents returned %r of %d documents"
                        % (None if docs is None else len(docs), docs_n)
                    )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.gc_sweep_2k_streams",
    "state",
    detail="collect_garbage over 2k unkept-but-fresh streams",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.015,
)
def bench_state_gc_sweep():
    """The GC sweep's classification cost, which grows with store age
    exactly when nobody watches.

    Cost is driven by STREAM count, not record count (5k records over 200
    streams measures single-digit ms: a permanently green dead gate), so
    the fixture is ~2000 streams of 1-2 records each.  They sit under a
    managed prefix but NOT in the keep set, with FRESH records: kept
    streams short-circuit before any listing, and actually-deletable ones
    would be consumed by the warm-up pass -- fresh-but-unkept is the one
    shape where every pass does the full classify-and-date work
    idempotently.  The keep dict is prebuilt; only collect_garbage is
    timed, and the pass must delete nothing.
    """
    import asyncio

    def build():
        path = os.path.join(_tmpdir(), "gc-streams")
        os.makedirs(path, exist_ok=True)
        n = max(_n(2000), 4)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                for base in range(0, n, 64):
                    await asyncio.gather(
                        *(
                            backend.append_record(
                                "runs/s%05d" % i,
                                {"outcome": "success", "seq": i},
                            )
                            for i in range(base, min(base + 64, n))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return path, n

    path, _n_streams = fixture("gc_streams_2k", build)
    keep = {"runs/": set()}

    async def run():
        backend = _state_backend(path)
        if not hasattr(backend, "collect_garbage"):
            raise Skip("collect_garbage not present")
        await backend.start()
        try:
            try:
                # two passes: one sweep of 2k streams measures under the
                # harness's 50ms rule on CI, and the fixture is built so a
                # pass is idempotent (nothing deletable), so a second pass
                # is byte-for-byte the same work
                t0 = time.perf_counter()
                result = await backend.collect_garbage(
                    keep=keep, grace=10.0**7
                )
                result2 = await backend.collect_garbage(
                    keep=keep, grace=10.0**7
                )
                dt = time.perf_counter() - t0
            except TypeError as exc:
                raise Skip(
                    "collect_garbage signature changed: %r" % exc
                ) from None
        finally:
            await backend.stop()
        removed = list(result.get("removed_streams") or ()) + list(
            result2.get("removed_streams") or ()
        )
        if removed:
            raise RuntimeError(
                "GC deleted %r; the fixture must stay idempotent (fresh "
                "records inside the grace window)" % (removed,)
            )
        return dt

    return asyncio.run(run())


def _artifact_scope_churned():
    """A store where one scope has had many artifact puts over a few names,
    built once (untimed).  Newest-per-name is all any reader wants, so a
    release that prunes superseded records at put time leaves a small stream
    here while an older one accumulates every version -- the difference this
    benchmark surfaces on the read side."""

    def build():
        import asyncio

        from cronstable import jobstate

        path = os.path.join(_tmpdir(), "artifact-churn")
        os.makedirs(path, exist_ok=True)
        n = _n(2000)
        names = 8

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            for i in range(n):
                await jobstate.artifact_put(
                    backend, "bench", "report-%d" % (i % names), b"payload"
                )
            await backend.stop()

        asyncio.run(seed())
        return path

    return fixture("artifact_churned", build)


@bench(
    "state.artifact_list_churn_x500",
    "state",
    detail="500 artifact_list calls after 2k puts over 8 names",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_artifact_list_churn():
    """Listing a scope whose few names were republished many times.

    Newest-per-name is all a reader wants, so a store that prunes
    superseded records at put time lists a handful of records here, and one
    that keeps every version lists two thousand.  One call is a fraction of
    a millisecond, so 500 of them make the timed region.
    """
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    if not hasattr(jobstate, "artifact_put") or not hasattr(
        jobstate, "artifact_list"
    ):
        raise Skip("jobstate artifact API not present")
    path = _artifact_scope_churned()
    calls = _n(500, floor=4)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(calls):
                listing = await jobstate.artifact_list(backend, "bench")
            dt = time.perf_counter() - t0
        except TypeError as exc:
            raise Skip("artifact_list signature changed: %r" % exc) from None
        finally:
            await backend.stop()
        if len(listing) != 8:
            raise RuntimeError(
                "artifact_list returned %d names, expected 8" % len(listing)
            )
        return dt

    return asyncio.run(run())


@bench(
    "state.artifact_get_newest",
    "state",
    detail="artifact_get_record newest-name lookup x200",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_artifact_get_newest():
    # The mapped-XCom / artifact-pull read path: artifact_get_record scans the
    # scope's records newest-first for a name. The early-stopping predicate
    # (list_records predicate + max_matches=1) stops at the first record
    # carrying the name -- one parse in the common case -- where the old
    # two-step page scan materialised and iterated a whole page. Times the
    # newest name, so the match is the first record read.
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    if not hasattr(jobstate, "artifact_get_record"):
        raise Skip("jobstate.artifact_get_record not present")
    path = _artifact_scope_churned()
    n = _n(200)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(n):
                await jobstate.artifact_get_record(backend, "bench", "report-0")
            dt = time.perf_counter() - t0
        except TypeError as exc:
            raise Skip("artifact_get_record signature changed: %r" % exc)
        await backend.stop()
        return dt

    return asyncio.run(run())


_BENCH_GATE_YAML = (
    "jobs:\n  - name: gated\n    command: 'x'\n"
    "    schedule: '* * * * *'\n    onlyIfLastSucceeded: true\n"
)


def _seeded_run_ledger():
    """A job's durable run ledger pre-seeded with success records, built once.
    The newest is a real outcome, so a release that probes a small newest page
    reads a few records where one reading the full window reads them all."""

    def build():
        import asyncio

        Cron = _cron_cls()
        path = os.path.join(_tmpdir(), "run-ledger")
        os.makedirs(path, exist_ok=True)
        n = max(_n(60), 2)
        stream = Cron._run_stream("gated")

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            for i in range(n):
                await backend.append_record(
                    stream,
                    {
                        "outcome": "success",
                        "exit_code": 0,
                        "started_at": None,
                        "finished_at": "2026-07-01T10:%02d:%02d+00:00"
                        % (i // 60, i % 60),
                        "duration": None,
                        "fail_reason": None,
                    },
                )
            await backend.stop()

        asyncio.run(seed())
        return path

    return fixture("seeded_run_ledger", build)


@bench(
    "state.depends_on_past_gate",
    "state",
    detail="onlyIfLastSucceeded gate read against a 60-record ledger",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_depends_on_past_gate():
    """The onlyIfLastSucceeded fire gate's durable read.

    The gate needs only the newest real outcome; a release that probes a small
    newest page reads a few records, one that always materialises the full
    RUN_HISTORY_LIMIT window reads them all -- what this measures, per fire.
    """
    import asyncio

    Cron = _cron_cls()
    path = _seeded_run_ledger()
    cfg = "state:\n  path: %s\n%s" % (
        path.replace("\\", "/"),
        _BENCH_GATE_YAML,
    )

    async def run():
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        if not hasattr(cron, "_depends_on_past_ok"):
            raise Skip("_depends_on_past_ok not present")
        job = cron.cron_jobs.get("gated")
        if job is None:
            raise Skip("gated job not configured")
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        try:
            await cron._depends_on_past_ok(job)  # warm imports/paths
            t0 = time.perf_counter()
            for _ in range(20):
                await cron._depends_on_past_ok(job)
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        return dt

    return asyncio.run(run())


def _dagstate_artifact_scope():
    """A store whose one artifact scope holds 2k distinct names.

    Built once and shared: ``(path, stream_dir, seeded file names, names)``.
    The names follow the XCom shape, one per task instance.  ``t0`` is
    appended on its own first, so its record sorts below every other.
    """

    def build():
        import asyncio

        from cronstable import jobstate

        path = os.path.join(_tmpdir(), "dagstate-artifacts")
        os.makedirs(path, exist_ok=True)
        names = _n(2000, floor=12)
        stream = jobstate.ARTIFACT_STREAM_PREFIX + "bench"

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                digest = await backend.put_blob(b'{"v": 1}')

                def record(i):
                    return {
                        "name": "t%d/return_value" % i,
                        "sha256": digest,
                        "size": 8,
                        "at": 1700000000.0 + i,
                    }

                await backend.append_record(stream, record(0))
                for base in range(1, names, 64):
                    await asyncio.gather(
                        *(
                            backend.append_record(stream, record(i))
                            for i in range(base, min(base + 64, names))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        stream_dir = None
        for root, _dirs, files in os.walk(path):
            records = [f for f in files if f.endswith(".json")]
            if len(records) == names:
                stream_dir = root
        if stream_dir is None:
            raise RuntimeError("the seeded artifact stream was not found")
        return path, stream_dir, frozenset(os.listdir(stream_dir)), names

    return fixture("dagstate_artifact_scope", build)


@bench(
    "state.artifact_get_oldest_2k",
    "state",
    detail="oldest-name and missing-name artifact lookups x100, 2k names",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_artifact_get_oldest():
    """The artifact lookup's worst cases in a scope of many names.

    XCom publishes one name per task instance, so a run's scope holds as
    many names as the run has instances, and a task that pulls an early
    value asks for one of the oldest.  state.artifact_get_newest times the
    best case, the newest name.  This one times the oldest name (best
    effort, then strict, the read a mapped expansion makes) and a name
    nobody published.  One untimed lookup comes first: a backend's first
    scan of a stream reads every record, and the steady state is what
    repeats for the life of the daemon.
    """
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    if not hasattr(jobstate, "artifact_get_record"):
        raise Skip("jobstate.artifact_get_record not present")
    path, _stream_dir, _seeded, _names = _dagstate_artifact_scope()
    oldest = "t0/return_value"
    lookups = _n(60, floor=3)
    strict_lookups = _n(10, floor=1)
    misses = _n(30, floor=2)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            first = await jobstate.artifact_get_record(
                backend, "bench", oldest
            )
            if first is None or first.get("name") != oldest:
                raise RuntimeError(
                    "the oldest artifact read back as %r" % (first,)
                )
            t0 = time.perf_counter()
            for _ in range(lookups):
                got = await jobstate.artifact_get_record(
                    backend, "bench", oldest
                )
            for _ in range(strict_lookups):
                strict = await jobstate.artifact_get_record(
                    backend, "bench", oldest, strict=True
                )
            for _ in range(misses):
                missing = await jobstate.artifact_get_record(
                    backend, "bench", "never/published"
                )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        if got != first or strict != first or missing is not None:
            raise RuntimeError(
                "artifact lookups disagreed: %r, %r, %r"
                % (got, strict, missing)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("artifact_get_record signature changed: %r" % exc) from None


@bench(
    "state.artifact_put_distinct_2k",
    "state",
    detail="256 artifact puts of new names into a 2k-name scope",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_artifact_put_distinct():
    """Publishing under new names in a scope that already holds many.

    Each put carries the name-keyed prune, and a pass of that prune reads
    every record of the stream.  With every name distinct no pass can
    delete anything, so what this guards is how often the pass runs: at a
    fixed cadence a put costs a share of the whole stream, and the cost of
    a run's XCom grows with the square of its instances.  The untimed first
    put carries the pass a backend runs on its first append to a stream.
    The record count is asserted afterwards, then the puts are removed so
    the scope stays at its seeded size for the next repeat.
    """
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    if not hasattr(jobstate, "artifact_put"):
        raise Skip("jobstate.artifact_put not present")
    path, stream_dir, seeded, names = _dagstate_artifact_scope()
    puts = _n(256, floor=4)
    payload = b'{"v": 1}'

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            await jobstate.artifact_put(
                backend, "bench", "warm/return_value", payload
            )
            t0 = time.perf_counter()
            for i in range(puts):
                await jobstate.artifact_put(
                    backend, "bench", "extra%d/return_value" % i, payload
                )
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        return dt

    try:
        dt = asyncio.run(run())
        resident = [n for n in os.listdir(stream_dir) if n.endswith(".json")]
        if len(resident) != names + 1 + puts:
            raise RuntimeError(
                "%d records resident after %d puts over %d names; a put "
                "was lost or a live record was pruned"
                % (len(resident), puts + 1, names)
            )
        return dt
    except TypeError as exc:
        raise Skip("artifact_put signature changed: %r" % exc) from None
    finally:
        for name in os.listdir(stream_dir):
            if name not in seeded:
                os.unlink(os.path.join(stream_dir, name))


@bench(
    "state.append_pruned_1k",
    "state",
    detail="400 append_record with prune_keep=1000 on a full 1k stream",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_state_append_pruned():
    """The append every finished run makes: one that carries its prune.

    The daemon appends each run-history record with prune_keep set to
    maxRunsPerJob (1000 by default), so a busy job's stream sits at its
    limit and every eighth append lists it and drops the oldest records.
    state.append_1k never passes prune_keep, so the prune pass, and a
    change that runs it on every append, is invisible there.  The stream
    is seeded to its limit once and stays there, which the final record
    count asserts.
    """
    import asyncio

    keep = _n(1000, floor=9)
    appends = _n(400, floor=9)
    record = {
        "outcome": "success",
        "exit_code": 0,
        "started_at": "2026-07-01T09:00:00+00:00",
        "finished_at": "2026-07-01T10:00:00+00:00",
        "duration": 12.5,
        "fail_reason": None,
    }

    def build():
        path = os.path.join(_tmpdir(), "dagstate-pruned")
        os.makedirs(path, exist_ok=True)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                for base in range(0, keep, 64):
                    await asyncio.gather(
                        *(
                            backend.append_record("runs/pruned", record)
                            for _ in range(base, min(base + 64, keep))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return path

    path = fixture("dagstate_pruned_stream", build)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(appends):
                await backend.append_record(
                    "runs/pruned", record, prune_keep=keep
                )
            dt = time.perf_counter() - t0
            left = len(await backend.list_records("runs/pruned"))
        finally:
            await backend.stop()
        # between two passes a stream holds up to cadence - 1 extra records
        if not keep <= left < keep + 8:
            raise RuntimeError(
                "the stream holds %d records around a bound of %d; the "
                "appends did not carry their prune" % (left, keep)
            )
        return dt

    try:
        return asyncio.run(run())
    except TypeError as exc:
        raise Skip("append_record lacks prune_keep: %r" % exc) from None


@bench(
    "state.inventory_2k_streams",
    "state",
    detail="3 inventory() walks of a 2k-stream store",
    repeats=(3, 2, 1),
    info=True,
)
def bench_state_inventory():
    """The metadata walk behind GET /state and MCP cron_inspect_state.

    The dashboard asks on every poll while the state inspector is open, and
    nothing memoizes the answer, so the walk repeats every few seconds per
    viewer: one directory listing per stream, counting its records.  Almost
    all of the time is the operating system enumerating directories, which
    measures the runner, so the metric is info only.  The count that keeps
    it honest (no record or document file is opened) is a test.
    """
    import asyncio

    def build():
        path = os.path.join(_tmpdir(), "gc-streams")
        os.makedirs(path, exist_ok=True)
        n = max(_n(2000), 4)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                for base in range(0, n, 64):
                    await asyncio.gather(
                        *(
                            backend.append_record(
                                "runs/s%05d" % i,
                                {"outcome": "success", "seq": i},
                            )
                            for i in range(base, min(base + 64, n))
                        )
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return path, n

    # the store state.gc_sweep_2k_streams seeds, under the same fixture name
    path, n_streams = fixture("gc_streams_2k", build)

    async def run():
        backend = _state_backend(path)
        if not hasattr(backend, "inventory"):
            raise Skip("inventory not present")
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(3):
                inv = await backend.inventory()
            dt = time.perf_counter() - t0
        finally:
            await backend.stop()
        seen = inv.get("records", {}).get("runs", {}).get("streams")
        if seen != n_streams:
            raise RuntimeError(
                "inventory counted %r of %d streams" % (seen, n_streams)
            )
        return dt

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# json / fingerprint / redact / ical
# ---------------------------------------------------------------------------


def _sample_doc():
    return {
        "schemaVersion": "v1",
        "run": {
            "dag": "nightly-etl",
            "runId": "r-000123",
            "state": "running",
            "startedAt": 1700000000.0,
            "tasks": {
                "t%d" % i: {
                    "state": "success",
                    "attempt": 1,
                    "exitCode": 0,
                    "host": "node-%d" % (i % 4),
                    "startedAt": 1700000000.0 + i,
                    "finishedAt": 1700000042.0 + i,
                }
                for i in range(50)
            },
        },
    }


@bench(
    "json.roundtrip_3k",
    "json",
    detail="dumps_bytes + loads of a run document x3k (stdlib backend)",
)
def bench_json_roundtrip():
    """The STDLIB flavour of the shared JSON helpers, pinned.

    The perf venvs now install orjson (production's default backend in the
    binaries and Docker images), which would silently turn this metric into
    a duplicate of json.roundtrip_orjson_3k and drop stdlib-fallback
    coverage -- the flavour every lean architecture without orjson wheels
    still runs.  So orjson is masked (sys.modules + reload) around the
    region and restored afterwards; on a venv without orjson this is the
    plain pre-split metric.
    """
    import importlib

    try:
        from cronstable import _json as json_mod
    except ImportError as exc:
        raise Skip("cronstable._json unavailable: %r" % exc) from None
    doc = _sample_doc()
    n = _n(3000)
    masked = getattr(json_mod, "orjson", None) is not None
    saved = None
    if masked:
        saved = sys.modules.pop("orjson", None)
        sys.modules["orjson"] = None  # import now raises ImportError
        importlib.reload(json_mod)
    try:
        if getattr(json_mod, "orjson", None) is not None:
            raise RuntimeError("orjson mask failed; still on the fast path")
        dumps_bytes, loads = json_mod.dumps_bytes, json_mod.loads
        t0 = time.perf_counter()
        for _ in range(n):
            loads(dumps_bytes(doc))
        return time.perf_counter() - t0
    finally:
        if masked:
            if saved is not None:
                sys.modules["orjson"] = saved
            else:
                sys.modules.pop("orjson", None)
            importlib.reload(json_mod)


@bench(
    "json.roundtrip_orjson_3k",
    "json",
    detail="dumps_bytes + loads of a run document x3k (orjson backend)",
)
def bench_json_roundtrip_orjson():
    """The orjson dispatch path, which had never once run on the scale.

    The perf venvs historically installed plain '.', so production's
    default backend in the binaries and Docker images -- including the
    _ensure_finite pre-walk and the wrapper paths, the exact regression
    class the 1.2.25 hardening hit -- had zero coverage.  Skips (never
    fails) when orjson is absent, e.g. on a lean-architecture local run.
    """
    try:
        from cronstable import _json as json_mod
    except ImportError as exc:
        raise Skip("cronstable._json unavailable: %r" % exc) from None
    if getattr(json_mod, "orjson", None) is None:
        raise Skip("orjson not installed; the stdlib flavour is "
                   "json.roundtrip_3k")
    doc = _sample_doc()
    n = _n(3000)
    t0 = time.perf_counter()
    for _ in range(n):
        json_mod.loads(json_mod.dumps_bytes(doc))
    return time.perf_counter() - t0


@bench(
    "json.loads_wide_int_2k",
    "json",
    detail="loads of a run document carrying one 19-digit integer x2k",
)
def bench_json_loads_wide_int():
    """The verification parse behind an integer the backends disagree on.

    A run of 19 digits anywhere in a payload (a nanosecond timestamp, a
    snowflake id, digits inside a string) makes loads run the stdlib
    parser with an integer hook as well, because orjson narrows an
    out-of-window integer to a float in silence.  _sample_doc holds no
    such run, so json.roundtrip_* never time this arm, which costs about
    2.7 times a plain orjson load.
    """
    try:
        from cronstable import _json as json_mod
    except ImportError as exc:
        raise Skip("cronstable._json unavailable: %r" % exc) from None
    doc = _sample_doc()
    doc["run"]["startedAtNs"] = 1700000000123456789
    raw = json_mod.dumps_bytes(doc)
    n = _n(2000, 20)
    loads = json_mod.loads
    t0 = time.perf_counter()
    for _ in range(n):
        out = loads(raw)
    dt = time.perf_counter() - t0
    if out["run"]["startedAtNs"] != 1700000000123456789:
        raise RuntimeError("wide integer did not round-trip: %r" % (out,))
    return dt


@bench(
    "fingerprint.job_set_id_10k",
    "fingerprint",
    detail="job_set_id over 10k JobConfigs",
    repeats=(3, 2, 1),
)
def bench_fingerprint():
    try:
        from cronstable.fingerprint import job_set_id
    except ImportError as exc:
        raise Skip("cronstable.fingerprint unavailable: %r" % exc) from None
    jobs = fixture("jobconfigs_10k", lambda: _job_configs(_n(10000)))
    t0 = time.perf_counter()
    job_set_id(jobs)
    return time.perf_counter() - t0


def _job_configs_rich(n):
    """JobConfigs that each own their hook blocks, unlike _job_configs."""
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    jobs = []
    for i, expr in enumerate(_varied_exprs(n)):
        raw = {
            "name": "job%05d" % i,
            "command": "true",
            "schedule": expr,
            "onFailure": {
                "retry": {
                    "maximumRetries": 3,
                    "initialDelay": 5.0,
                    "maximumDelay": 300.0,
                    "backoffMultiplier": 2.0,
                },
                "report": {
                    "mail": {
                        "from": "cron@example.com",
                        "to": "team%d@example.com" % (i % 50),
                        "smtpHost": "smtp.example.com",
                    },
                    "webhook": {
                        "url": {"fromEnvVar": "BENCH_WEBHOOK_URL"},
                        "headers": {"X-Team": "team%d" % (i % 50)},
                    },
                },
            },
            "onPermanentFailure": {
                "report": {"webhook": {"url": {"fromEnvVar": "BENCH_PAGER"}}}
            },
            "onSuccess": {
                "report": {"webhook": {"url": {"fromEnvVar": "BENCH_BEAT"}}}
            },
        }
        jobs.append(JobConfig(mergedicts(DEFAULT_CONFIG, raw)))
    return jobs


@bench(
    "fingerprint.job_set_id_rich_2k",
    "fingerprint",
    detail="job_set_id over 2k JobConfigs that each own their hook blocks",
)
def bench_fingerprint_rich():
    """The job-set id where no two jobs share a hook block.

    job_set_id memoizes the redaction, normalization and JSON of a hook
    block by identity, and fingerprint.job_set_id_10k's jobs all inherit
    the same three default blocks, so that fixture times the memo hit
    alone.  A job that configures its own reporters misses the memo three
    times, which costs about 3.5 times as much per job, and the daemon
    pays it on its event loop after every reload.
    """
    try:
        from cronstable.fingerprint import job_set_id
    except ImportError as exc:
        raise Skip("fingerprint API unavailable: %r" % exc) from None
    n = _n(2000, 4)
    jobs = fixture("job_configs_rich_2k", lambda: _job_configs_rich(n))
    t0 = time.perf_counter()
    result = job_set_id(jobs)
    dt = time.perf_counter() - t0
    if not (isinstance(result, str) and result.startswith("v")):
        raise RuntimeError("job_set_id returned %r" % (result,))
    return dt


@bench(
    "redact.clean_20k",
    "redact",
    detail="redact_lines over 20k secret-free log lines",
)
def bench_redact_clean():
    try:
        from cronstable.redact import redact_lines
    except ImportError as exc:
        raise Skip("cronstable.redact unavailable: %r" % exc) from None
    n = _n(20000)
    lines = fixture(
        "clean_lines",
        lambda: [
            "2026-07-18 12:00:%02d INFO worker %d: processed batch in 12ms"
            % (i % 60, i)
            for i in range(n)
        ],
    )
    t0 = time.perf_counter()
    redact_lines(lines)
    return time.perf_counter() - t0


@bench(
    "redact.secrets_5k",
    "redact",
    detail="redact_lines over 5k secret-bearing lines",
)
def bench_redact_secrets():
    try:
        from cronstable.redact import redact_lines
    except ImportError as exc:
        raise Skip("cronstable.redact unavailable: %r" % exc) from None
    n = _n(5000)

    def build():
        pem = [
            "-----BEGIN RSA PRIVATE KEY-----",
            "MIIEowIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF0qJps5MTvEV0G4RFY0PGpfx0000",
            "-----END RSA PRIVATE KEY-----",
        ]
        out = []
        for i in range(n):
            r = i % 5
            if r == 0:
                out.append(
                    "export AWS_SECRET_ACCESS_KEY="
                    "wJalrXUtnFEMIbPxRfiCYEXAMPLEKEY%03d" % i
                )
            elif r == 1:
                out.append("PASSWORD=hunter%d" % i)
            elif r == 2:
                out.append(
                    "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.pay%04d.sig"
                    % i
                )
            elif r == 3:
                out.extend(pem)
            else:
                out.append("plain line %d with nothing sensitive" % i)
        return out

    lines = fixture("secret_lines", build)
    t0 = time.perf_counter()
    redact_lines(lines)
    return time.perf_counter() - t0


@bench(
    "redact.adversarial_10k",
    "redact",
    detail="redact_lines over 10k hostile-shaped lines (URL creds, "
    "compound keys, quoted JSON, near-PEM)",
)
def bench_redact_adversarial():
    """The redaction shapes that have actually regressed, none of which the
    other two redact fixtures contain.

    Neither existing fixture has a "://" anywhere, so the URL-password
    pattern (quadratic before its 1.2.25 fix; its prefilter gate skips it on
    every non-URL line) never executes in the suite.  Also here: compound
    prefix keys (PGPASSWORD=), quoted JSON values with escapes, and near-PEM
    marker lines -- the documented re-introduction traps.

    Hostile scheme runs and @-less tails are CAPPED (64/256 chars):
    bench.py has no per-metric timeout, so a reintroduced quadratic must
    show up as a gated slowdown in seconds, never hang CI.  The builder
    hard-fails (never skips) if any fixture line matches _PEM_BEGIN --
    one accidental match would flip redact_lines' in_pem state and gut
    the workload for every line after it.
    """
    try:
        from cronstable import redact as redact_mod
        from cronstable.redact import redact_lines
    except ImportError as exc:
        raise Skip("cronstable.redact unavailable: %r" % exc) from None
    n = _n(10000)

    def build():
        scheme_run = "a" * 64  # capped hostile scheme-char run
        tail = "x" * 256  # capped @-less tail after ://
        out = []
        for i in range(n):
            r = i % 8
            if r == 0:
                out.append(
                    "db url mongodb://user%d:hunter%d@db-%d.internal:27017/x"
                    % (i, i, i % 5)
                )
            elif r == 1:
                # a scheme-char run + :// + a long tail with no @: the
                # backtracking shape the anchored/bounded pattern exists for
                out.append("retry %s://%s status=timeout" % (scheme_run, tail))
            elif r == 2:
                out.append("PGPASSWORD=swordfish%d pg_dump --host prod" % i)
            elif r == 3:
                out.append(
                    '{"password": "hu\\"nter%d", "user": "app", '
                    '"level": "info"}' % i
                )
            elif r == 4:
                out.append("redis://:p%d@cache-%d.internal:6379/0" % (i, i % 3))
            elif r == 5:
                # near-PEM: passes the "-----" cheap gate, must NOT match
                out.append(
                    "cert body -----BEGIN CERTIFICATE----- MIIB%04d"
                    % (i % 10000)
                )
            elif r == 6:
                out.append(
                    "https://%s:%s@host-%d.example.com/api"
                    % ("u" * 24, "s" * 48, i % 100)
                )
            else:
                out.append("plain worker %d finished batch in 12ms" % i)
        pem_begin = getattr(redact_mod, "_PEM_BEGIN", None)
        if pem_begin is not None:
            for line in out:
                if pem_begin.search(line):
                    raise RuntimeError(
                        "adversarial fixture line matches _PEM_BEGIN; "
                        "the PEM state flip would gut the workload: %r"
                        % line
                    )
        return out

    lines = fixture("adversarial_lines", build)
    t0 = time.perf_counter()
    redact_lines(lines)
    return time.perf_counter() - t0


@bench(
    "ical.render_500x7d",
    "ical",
    detail="render_calendar, 500 entries over 7 days",
    repeats=(3, 2, 1),
)
def bench_ical():
    try:
        from cronstable.ical import CalendarEntry, render_calendar
    except ImportError as exc:
        raise Skip("cronstable.ical unavailable: %r" % exc) from None
    CronTab = _crontab_cls()
    n = _n(500)

    def build():
        entries = []
        for i in range(n):
            if i % 2 == 0:
                expr = "%d * * * *" % (i % 60)
            else:
                expr = "%d %d * * *" % (i % 60, (i * 7) % 24)
            entries.append(
                CalendarEntry("job%05d" % i, CronTab(expr), timezone.utc)
            )
        return entries

    entries = fixture("ical_entries", build)
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    t0 = time.perf_counter()
    render_calendar(entries, start=start, days=7, per_job_cap=50)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# tui: the terminal dashboard's per-frame string work.  The log drawer
# re-measures, re-cuts and re-inks its whole buffer each frame, and the log
# search re-scans it, so these functions -- text_width / cut_to_width /
# rewrite_sgr / strip_ansi -- are the terminal UI's hottest per-frame cost
# (and where the printable-ASCII fast paths live).  Measured in-process; no
# terminal, no app loop.
# ---------------------------------------------------------------------------


def _tui_module():
    try:
        from cronstable import tui
    except ImportError as exc:
        raise Skip("cronstable.tui unavailable: %r" % exc) from None
    return tui


def _tui_log_lines(n):
    """A realistic log buffer: coloured (SGR) lines, plain ASCII, a wide-glyph
    line, and one carrying control characters -- the mix a real job emits."""
    plain = "2026-07-18 12:00:%02d INFO worker %d processed batch in 12ms"
    colored = (
        "\x1b[32m2026-07-18 12:00:%02d\x1b[0m \x1b[1mworker %d\x1b[0m "
        "\x1b[36mOK\x1b[0m done"
    )
    wide = "进度 %d%% ▕████████▏ 完了 \x1b[33mwarn\x1b[0m"
    hostile = "line %d \x07\x08 spinner \r\x1b[2K progress"
    out = []
    for i in range(n):
        r = i % 4
        if r == 0:
            out.append(colored % (i % 60, i))
        elif r == 1:
            out.append(plain % (i % 60, i))
        elif r == 2:
            out.append(wide % (i % 100))
        else:
            out.append(hostile % i)
    return out


@bench(
    "tui.log_restyle_5k",
    "tui",
    detail="text_width + cut_to_width + rewrite_sgr over a 5k-line drawer",
    repeats=(5, 2, 1),
)
def bench_tui_log_restyle():
    tui = _tui_module()
    for attr in ("text_width", "cut_to_width", "rewrite_sgr", "Theme"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)
    try:
        theme = tui.Theme("carolina", False)
    except Exception as exc:  # pragma: no cover - signature drift
        raise Skip("tui.Theme construction failed: %r" % exc) from None
    lines = fixture("tui_log_lines_5k", lambda: _tui_log_lines(_n(5000)))
    width = 110
    t0 = time.perf_counter()
    for line in lines:
        tui.text_width(line)
        row = tui.cut_to_width(line, width)
        tui.rewrite_sgr(row, theme)
    return time.perf_counter() - t0


@bench(
    "tui.log_search_20k",
    "tui",
    detail="strip_ansi + substring match over a 20k-line drawer",
    repeats=(5, 2, 1),
)
def bench_tui_log_search():
    tui = _tui_module()
    if not hasattr(tui, "strip_ansi"):
        raise Skip("cronstable.tui lacks strip_ansi")
    lines = fixture("tui_log_lines_20k", lambda: _tui_log_lines(_n(20000)))
    needle = "worker"
    t0 = time.perf_counter()
    for line in lines:
        tui.strip_ansi(line).lower().find(needle)
    return time.perf_counter() - t0


@bench(
    "tui.drawer_paint_5k",
    "tui",
    detail="log-drawer paint: 2500-row scroll walk + 3000 steady paints",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_drawer_paint():
    """The drawer's real paint path, guarding the project's largest single
    measured win (the 1.2.24 steady-state paint: 8.6ms to 0.02ms), which
    tui.log_restyle_5k structurally cannot see -- it times the shape that
    optimization REMOVED (measured overlap ~10%).

    Two shapes, both load-bearing: the scroll walk steps ONE ROW at a time
    (a window-stepped walk renders each line exactly once, so a fully
    removed ANSI memo moves it only +21%; the 1-row step moves it +402%),
    and the steady paints at a fixed scroll are where the visible-window
    slice lives (reverting it measured +12,567%).  _ansi_cache is cleared
    and log_scroll reset UNTIMED before the region: without that the
    region ends warm and compare='min' locks onto the warmest repeat.

    Leans on a deliberately private surface (_drawer_logs, _ansi_line, the
    App constructor), so it is in the tests' never-skip net; do not share
    its fixture with any future TUI frame metric (they mutate the same
    scroll/cache state, making values order-dependent).
    """
    tui = _tui_module()
    for attr in ("TuiApp", "Painter", "LogTail", "PREF_DEFAULTS"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)
    if not hasattr(tui.TuiApp, "_drawer_logs"):
        raise Skip("TuiApp._drawer_logs not present")
    walk = _n(2500)
    paints = _n(3000)
    buffer_lines = _n(5000)

    def build():
        try:
            app = tui.TuiApp(None, None, None, dict(tui.PREF_DEFAULTS))
            tail = tui.LogTail(None, "/bench", "bench", lambda: None)
        except TypeError as exc:
            raise Skip(
                "TuiApp/LogTail construction changed: %r" % exc
            ) from None
        raw = _tui_log_lines(buffer_lines)
        tail.lines = [
            ("stderr" if i % 5 == 0 else "stdout", line, 1700000000.0 + i)
            for i, line in enumerate(raw)
        ]
        app.log_tail = tail
        return app

    app = fixture("tui_drawer_app", build)
    paint = tui.Painter(app.theme)
    width, body_lines = 120, 40
    # untimed reset: the region must start cold every repeat
    app._ansi_cache.clear()
    app.log_scroll = 0
    t0 = time.perf_counter()
    for step in range(walk):
        app.log_scroll = step
        app._drawer_logs(paint, width, body_lines)
    app.log_scroll = 0
    for _ in range(paints):
        rows = app._drawer_logs(paint, width, body_lines)
    dt = time.perf_counter() - t0
    if not rows or len(rows) > body_lines + 1:
        raise RuntimeError(
            "drawer paint produced %d rows for a %d-line body"
            % (len(rows) if rows else 0, body_lines)
        )
    return dt


@bench(
    "tui.dag_graph_paint_2k",
    "tui",
    detail="2k-task chain and fan-in graphs, 500 frames each",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_tui_dag_graph_paint():
    """Measure DAG layout and repeated rendering within a terminal viewport.

    A chain exercises the row limit; a wide fan-in exercises the column
    limit and edge rows. Each graph starts with a cold layout and is
    painted repeatedly from the same metadata and run-state snapshots.
    """
    tui = _tui_module()
    if not hasattr(tui.TuiApp, "_dag_graph_tab"):
        raise Skip("TuiApp._dag_graph_tab not present")
    try:
        app = tui.TuiApp(None, None, None, dict(tui.PREF_DEFAULTS))
    except TypeError as exc:
        raise Skip("TuiApp construction changed: %r" % exc) from None
    n = _n(2000, floor=2)
    paints = _n(500)
    names = ["t%05d" % i for i in range(n)]
    chain = [
        {"id": name, "dependsOn": [names[i - 1]] if i else []}
        for i, name in enumerate(names)
    ]
    fanin = [{"id": name} for name in names]
    fanin[-1]["dependsOn"] = names[:-1]
    run_tasks = [{"key": name, "state": "running"} for name in names]
    graphs = [
        ({"tasks": chain}, {"tasks": run_tasks}),
        (
            {"tasks": fanin},
            {"tasks": {task["key"]: task for task in run_tasks}},
        ),
    ]
    paint = tui.Painter(app.theme)
    width, body_lines = 120, 40
    t0 = time.perf_counter()
    for dag, run in graphs:
        app.dag_run = run
        for _ in range(paints):
            rows = app._dag_graph_tab(paint, dag, width, body_lines)
        if (
            not rows
            or len(rows) > body_lines
            or "t00000 running" not in tui.strip_ansi(rows[0])
        ):
            raise RuntimeError("DAG graph paint did not render the fixture")
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# tui: the jobs board.  tui.log_restyle_5k through tui.dag_graph_paint_2k
# time the log drawer and the DAG graph; these time what every session runs
# all day: the table frame, the poll fold, and the rebuilds a keystroke
# triggers.  Measured in process against a TuiApp with no daemon, key source
# or event loop behind it, painting through the real Term differ into a
# counting sink.
# ---------------------------------------------------------------------------
def _tui_jobs(n):
    """``n`` rows in the shape of the daemon's ``GET /jobs`` payload.

    A fixed mix of running, failing, cancelled, paused, late, disabled and
    never-run jobs on a spread cluster, each carrying the 20-run inline
    history of a settled fleet.  Every timestamp derives from ``_NOW``.
    """
    teams = (
        "billing",
        "etl",
        "backup",
        "report",
        "sync",
        "cache",
        "audit",
        "ml",
    )
    schedules = (
        "* * * * *",
        "*/5 * * * *",
        "0 * * * *",
        "15 3 * * *",
        "0 0 * * 0",
        "30 2 1 * *",
        "*/15 9-17 * * mon-fri",
        "7 */6 * * *",
        "0 4 * * 1-5",
        "45 23 * * *",
    )
    jobs = []
    for i in range(n):
        team = teams[i % len(teams)]
        name = "%s-%s-%05d" % (team, "nightly" if i % 3 else "hourly", i)
        slot = i % 20
        running = slot == 3
        enabled = slot != 7
        last_run = None
        history = []
        if slot < 16:  # the last four slots have never run
            outcome = "success"
            if slot == 5:
                outcome = "failure"
            elif slot == 9:
                outcome = "cancelled"
            failed = outcome == "failure"
            finished = _NOW - timedelta(seconds=5 + (i * 37) % 86400)
            duration = 0.2 + (i % 97) * 1.3
            last_run = {
                "outcome": outcome,
                "exit_code": 1 if failed else 0,
                "started_at": (
                    finished - timedelta(seconds=duration)
                ).isoformat(),
                "finished_at": finished.isoformat(),
                "duration": duration,
                "fail_reason": "exit 1" if failed else None,
                "skip_reason": None,
                "resources": None,
                "ranAt": finished.isoformat(),
            }
            history = [
                {
                    "outcome": "failure" if (i + k) % 9 == 0 else "success",
                    "duration": 1.0 + (i * 7 + k * 13) % 50,
                }
                for k in range(20)
            ]
        job = {
            "name": name,
            "enabled": enabled,
            "schedule": schedules[i % len(schedules)],
            "command": "/opt/%s/bin/run --task %s --shard %d"
            % (team, name, i % 16),
            "captureStdout": True,
            "captureStderr": True,
            "utc": True,
            "timezone": "UTC",
            "running": running,
            "pids": [1000 + i] if running else [],
            "scheduled_in": float((i * 53) % 7200) if enabled else None,
            "never_fires": False,
            "schedule_findings": [],
            "last_run": last_run,
            "history": history,
            "paused": None,
            "clusterOwner": "node-%02d" % (i % 15),
        }
        if slot == 11:
            job["paused"] = {
                "since": (_NOW - timedelta(hours=1)).isoformat(),
                "until": (_NOW + timedelta(hours=9)).isoformat(),
                "note": "",
                "by": "ops",
                "channel": "api",
            }
        if slot == 13:
            job["sla"] = {"state": "late"}
        if running:
            job["running_resources"] = {
                "cpu_percent": 12.5,
                "rss_bytes": 52428800,
            }
        jobs.append(job)
    return jobs


def _tui_jobs_5k():
    # floor: more jobs than a 60-line terminal shows, so --smoke scrolls too
    return fixture("tui_jobs_5k", lambda: _tui_jobs(_n(5000, floor=80)))


class _TuiSink:
    """A write-only stream that counts what ``Term`` sends to the terminal:
    one write per frame, and the UTF-8 bytes when ``measure`` is set."""

    def __init__(self, measure=False):
        self.measure = measure
        self.writes = 0
        self.size = 0

    def write(self, data):
        self.writes += 1
        if self.measure:
            self.size += len(data.encode("utf-8"))

    def flush(self):
        pass


def _tui_board(tui, jobs, cols, lines, sink, api=None):
    """A ``TuiApp`` showing ``jobs`` on a ``cols`` by ``lines`` terminal.

    The terminal is the real ``Term`` (the row differ) with a fixed size,
    writing to ``sink``.
    """
    for attr in ("TuiApp", "Term", "Painter", "PREF_DEFAULTS"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)

    class _FixedTerm(tui.Term):
        def size(self):
            return (cols, lines)

    try:
        term = _FixedTerm(stream=sink)
        app = tui.TuiApp(api, term, None, dict(tui.PREF_DEFAULTS))
    except TypeError as exc:
        raise Skip("TuiApp/Term construction changed: %r" % exc) from None
    for attr in ("recompute_view", "paint", "render_overlay", "jobs"):
        if not hasattr(app, attr):
            raise Skip("TuiApp lacks %s" % attr)
    app.jobs = jobs
    app.by_name = {job["name"]: job for job in jobs}
    app.connected = True
    app.fetched_mono = time.monotonic()
    app.recompute_view()
    return app


@bench(
    "tui.table_paint_5k",
    "tui",
    detail="jobs table at 200x60, 5k jobs: 30 steady + 30 one-row-scroll "
    "frames",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_table_paint():
    """The frame every session paints at least once a second: header,
    toolbar, verdict bar, the visible job rows and the footer, through the
    row differ.

    A frame costs the visible rows, whatever the fleet size.  This guards
    that (a per-frame walk of every job shows here at 5k jobs) and the cost
    of styling and cutting one row.  The scroll frames move the window one
    row each, so every body row changes and the differ writes them all.
    """
    tui = _tui_module()
    jobs = _tui_jobs_5k()
    sink = _TuiSink()
    cols, lines = 200, 60
    app = _tui_board(tui, jobs, cols, lines, sink)
    frames = _n(30, floor=2)
    # untimed: park the selection below the first window, so each later
    # step scrolls, and fill the chrome and spark memos
    app.sel = lines
    app.paint()
    offset = app.table_offset
    t0 = time.perf_counter()
    for _ in range(frames):
        app.paint()
    for _ in range(frames):
        app.sel += 1
        app.paint()
    dt = time.perf_counter() - t0
    if sink.writes != 2 * frames + 1 or app.table_offset != offset + frames:
        raise RuntimeError(
            "table paint wrote %d frames and scrolled %d rows; expected "
            "%d and %d"
            % (sink.writes, app.table_offset - offset, 2 * frames + 1, frames)
        )
    return dt


@bench(
    "tui.frame_bytes_5k",
    "tui",
    detail="bytes of one full jobs-table repaint at 200x60, 5k jobs",
    unit="KB",
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_tui_frame_bytes():
    """The size of a full repaint: what a resize, a theme change, or one
    scroll step past the window edge sends down an SSH link.

    A byte count moves when a cell or a separator gains an SGR span, which
    a timing does not show.  Only the full repaint is measured.  Its size
    is a pure function of the fixture, because every clock-dependent cell
    is ASCII padded to a fixed width, while a differential frame also
    depends on whether the header clock ticked.
    """
    tui = _tui_module()
    jobs = _tui_jobs_5k()
    sink = _TuiSink(measure=True)
    cols, lines = 200, 60
    app = _tui_board(tui, jobs, cols, lines, sink)
    app.paint()  # the first frame on a fresh Term repaints every row
    if sink.writes != 1 or sink.size < cols * lines:
        raise RuntimeError(
            "full repaint wrote %d frame(s), %d bytes for %d cells"
            % (sink.writes, sink.size, cols * lines)
        )
    return sink.size / 1024.0


@bench(
    "tui.poll_absorb_5k",
    "tui",
    detail="2 x App._poll_once over a 5k-job /jobs body: decode, fold, "
    "sort, verdict",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_poll_absorb():
    """What one poll costs the dashboard's own event loop: the JSON decode
    aiohttp's ``resp.json()`` performs, the by-name index, the failure
    diff, the aggregates fold, the view sort and the verdict.

    The poll runs every 3 seconds by default and is the largest idle cost
    at fleet scale.  The fake API decodes a pre-serialized body on every
    call, as the client does, so a payload-shaped regression (a second
    walk of every job, a costlier sort key) lands in the timed region.
    """
    import asyncio

    tui = _tui_module()
    jobs = _tui_jobs_5k()
    body = fixture(
        "tui_jobs_body_5k",
        lambda: json.dumps(jobs, separators=(",", ":")).encode("utf-8"),
    )

    class _Daemon:
        url = "http://127.0.0.1:1"
        token = None
        decoded = 0

        async def get_json(self, path, timeout_s=10.0):
            if path == "/jobs":
                self.decoded += 1
                return json.loads(body.decode("utf-8"))
            return {}

    daemon = _Daemon()
    app = _tui_board(tui, [], 200, 60, _TuiSink(), api=daemon)
    if not hasattr(app, "_poll_once"):
        raise Skip("TuiApp._poll_once not present")
    polls = 2

    async def run():
        t0 = time.perf_counter()
        for _ in range(polls):
            await app._poll_once()
        return time.perf_counter() - t0

    dt = asyncio.run(run())
    if (
        daemon.decoded != polls
        or len(app.jobs) != len(jobs)
        or len(app.view) != len(jobs)
        or app.verdict is None
    ):
        raise RuntimeError(
            "%d polls absorbed %d of %d jobs into a %d-row view (verdict %r)"
            % (
                daemon.decoded,
                len(app.jobs),
                len(jobs),
                len(app.view),
                app.verdict,
            )
        )
    return dt


@bench(
    "tui.palette_type_5k",
    "tui",
    detail="command palette ranking for 3 successive queries, 5k jobs",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_palette_type():
    """Typing into the command palette: each new query rebuilds the row
    list (about six rows per job) and ranks it.

    The rebuild is the dashboard's largest synchronous step per keystroke
    and it also runs once per poll while the palette is open.  The three
    queries are prefixes of one typed command: the first matches most
    labels as a substring, the others send most labels through the
    subsequence scan.
    """
    tui = _tui_module()
    jobs = _tui_jobs_5k()
    app = _tui_board(tui, jobs, 200, 60, _TuiSink())
    if not hasattr(app, "palette_matches") or "palette" not in app.inputs:
        raise Skip("TuiApp.palette_matches not present")
    app.dags = [{"name": "dag%02d" % i} for i in range(20)]
    queries = ("ru", "run: e", "run: etl-night")
    t0 = time.perf_counter()
    for query in queries:
        app.inputs["palette"] = query
        matches = app.palette_matches()
    dt = time.perf_counter() - t0
    if not matches or not matches[0][1].lower().startswith(queries[-1]):
        raise RuntimeError(
            "palette ranked %d rows for %r, top %r"
            % (len(matches), queries[-1], matches[0][1] if matches else None)
        )
    return dt


@bench(
    "tui.view_sort_5k",
    "tui",
    detail="compute_view over 5k jobs: 5 sort keys x 2 directions, 12 "
    "filter prefixes, 5 status segments",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_view_sort():
    """The filter and sort behind the jobs table, which runs for a sort or
    status key, for a filter edit, and for every poll.

    The poll benchmark only reaches the name sort with no filter.  This
    one covers every sort key in both directions, a filter narrowing one
    character at a time, and each status segment.
    """
    tui = _tui_module()
    for attr in ("compute_view", "SORT_KEYS", "STATUS_SEGMENTS"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)
    jobs = _tui_jobs_5k()
    text = "billing-nigh"
    t0 = time.perf_counter()
    whole = [
        len(tui.compute_view(jobs, "", "all", key, direction))
        for key in tui.SORT_KEYS
        for direction in (1, -1)
    ]
    narrowed = [
        len(tui.compute_view(jobs, text[:i], "all", "name", 1))
        for i in range(1, len(text) + 1)
    ]
    segments = [
        len(tui.compute_view(jobs, "", segment, "name", 1))
        for segment in tui.STATUS_SEGMENTS
    ]
    dt = time.perf_counter() - t0
    if (
        set(whole) != {len(jobs)}
        or not 0 < narrowed[-1] < narrowed[0]
        or sorted(narrowed, reverse=True) != narrowed
        or segments[0] != len(jobs)
        or min(segments) <= 0
    ):
        raise RuntimeError(
            "compute_view returned %r / %r / %r rows for %d jobs"
            % (whole, narrowed, segments, len(jobs))
        )
    return dt


def _tui_fleet(nodes, jobs):
    """A ``GET /fleet`` payload: ``nodes`` nodes that each report ``jobs``
    jobs, with running, disabled, failed and finished cells."""
    out = []
    for n in range(nodes):
        cells = {}
        for j in range(jobs):
            slot = (n * 31 + j) % 11
            if slot == 0:
                cell = {"running": True, "enabled": True}
            elif slot == 1:
                cell = {"running": False, "enabled": False}
            else:
                finished = _NOW - timedelta(seconds=30 + (j * 17 + n) % 90000)
                cell = {
                    "running": False,
                    "enabled": True,
                    "last": {
                        "outcome": "failure" if slot == 2 else "success",
                        "finished_at": finished.isoformat(),
                    },
                }
            cells["job%04d" % j] = cell
        out.append(
            {"node_name": "node-%02d" % n, "self": n == 0, "jobs": cells}
        )
    return {"enabled": True, "nodes": out}


@bench(
    "tui.fleet_paint_15x400",
    "tui",
    detail="fleet matrix panel at 200x60, 15 nodes x 400 jobs: 15 scrolled "
    "+ 15 steady frames",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_fleet_paint():
    """The fleet matrix, the costliest overlay per frame: every visible
    job row styles one cell per node, each with an age.

    The matrix is folded once per payload and the visible window is styled
    per frame, so the cost follows rows times nodes and is independent of
    the job count.  The terminal twin of ``webui.render_fleet_15x400``.
    """
    tui = _tui_module()
    nodes = 15
    per_node = _n(400, floor=80)
    fleet = fixture("tui_fleet_15x400", lambda: _tui_fleet(nodes, per_node))
    cols, lines = 200, 60
    app = _tui_board(tui, [], cols, lines, _TuiSink())
    if not hasattr(app, "render_fleet"):
        raise Skip("TuiApp.render_fleet not present")
    app.fleet = fleet
    app.open("fleet")
    paint = tui.Painter(app.theme)
    frames = _n(15, floor=2)
    rows = app.render_overlay(paint, "fleet", cols, lines)  # folds the matrix
    t0 = time.perf_counter()
    for step in range(frames):
        app.panel_scroll = step
        app.render_overlay(paint, "fleet", cols, lines)
    for _ in range(frames):
        rows = app.render_overlay(paint, "fleet", cols, lines)
    dt = time.perf_counter() - t0
    head = tui.strip_ansi("".join(rows[:3]))
    if (
        len(rows) > lines
        or "%d nodes" % nodes not in head
        or "%d jobs" % per_node not in head
        or app.panel_scroll != frames - 1
    ):
        raise RuntimeError(
            "fleet panel painted %d rows, scroll %d, head %r"
            % (len(rows), app.panel_scroll, head[:120])
        )
    return dt


def _tui_week(schedules):
    """The week calendar's payload for ``schedules`` schedules, shaped as
    ``App._recompute_week`` leaves it: most fire daily, a tenth weekly,
    and a twentieth too often to chart."""
    start = _NOW.replace(hour=0, minute=0, second=0, microsecond=0)
    grid = [[0] * 24 for _ in range(7)]
    items = []
    frequent = []
    for i in range(schedules):
        name = "job-%04d" % i
        if i % 20 == 19:
            frequent.append((name, 200, True))
            continue
        days = (i % 7,) if i % 10 == 9 else range(7)
        for day in days:
            when = start + timedelta(
                days=day, hours=i % 24, minutes=(i * 7) % 60
            )
            items.append((when, name))
            grid[day][when.hour] += 1
    items.sort()
    return {
        "start": start,
        "grid": grid,
        "items": items,
        "frequent": frequent,
        "schedules": schedules,
    }


@bench(
    "tui.week_rows_500",
    "tui",
    detail="week calendar body, 500 schedules (about 3k agenda rows) x12",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_week_rows():
    """The week calendar's row build, which runs on the event loop once
    per recompute and once per wall-clock minute while the panel is open.

    It styles every agenda row, so its cost follows the fire count of the
    whole week.  The payload is built by hand with a fixed clock, which
    keeps the workload the same on every run date; the engine walk that
    produces the payload in the app is under ``ical.render_500x7d``.
    """
    tui = _tui_module()
    schedules = _n(500, floor=40)
    week = fixture("tui_week_500", lambda: _tui_week(schedules))
    app = _tui_board(tui, [], 200, 60, _TuiSink())
    if not hasattr(app, "_week_rows"):
        raise Skip("TuiApp._week_rows not present")
    paint = tui.Painter(app.theme)
    now = week["start"] + timedelta(days=3, hours=12)
    builds = 12
    t0 = time.perf_counter()
    try:
        for _ in range(builds):
            rows = app._week_rows(paint, week, 96, now)
    except TypeError as exc:
        raise Skip("TuiApp._week_rows signature changed: %r" % exc) from None
    dt = time.perf_counter() - t0
    expected = len(week["items"]) + len(week["frequent"])
    if len(rows) < expected or "today" not in tui.strip_ansi("".join(rows)):
        raise RuntimeError(
            "week body has %d rows for %d fires and hum entries"
            % (len(rows), expected)
        )
    return dt


@bench(
    "tui.wallboard_paint_5k",
    "tui",
    detail="wallboard at 200x60, 5k jobs: 4 polls x 8 frames, then 16 zen "
    "frames",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_wallboard_paint():
    """The TV board and its screensaver, the two screens left running
    unattended.

    A tile frame reads the per-poll fold; the first frame after a poll
    also picks the worst tiles out of every job.  The zen field places one
    dot per job on every frame, so its cost has to stay linear in the job
    count.
    """
    tui = _tui_module()
    jobs = _tui_jobs_5k()
    sink = _TuiSink()
    app = _tui_board(tui, jobs, 200, 60, sink)
    for attr in ("wallboard", "zen_on", "render_wallboard", "render_zen"):
        if not hasattr(app, attr):
            raise Skip("TuiApp lacks %s" % attr)
    app.wallboard = True
    polls = _n(4, floor=1)
    frames = _n(8, floor=2)
    zen = _n(16, floor=2)
    app.paint()
    t0 = time.perf_counter()
    for _ in range(polls):
        app.recompute_view()  # a poll's fold: drops the tile selection
        for _ in range(frames):
            app.paint()
    app.zen_on = True
    for _ in range(zen):
        app.paint()
    dt = time.perf_counter() - t0
    if sink.writes != 1 + polls * frames + zen or app.stale():
        raise RuntimeError(
            "wallboard wrote %d frames (stale=%r); expected %d live ones"
            % (sink.writes, app.stale(), 1 + polls * frames + zen)
        )
    return dt


def _tui_sse_wire(n):
    """``n`` log lines as the daemon's SSE frames, one bytes object per
    wire line, then the end-of-run event."""
    wire = []
    for i, line in enumerate(_tui_log_lines(n)):
        payload = {
            "stream": "stderr" if i % 5 == 0 else "stdout",
            "line": line,
        }
        wire += [
            b"event: line\n",
            ("data: %s\n" % json.dumps(payload)).encode("utf-8"),
            b"\n",
        ]
    wire += [b"event: end\n", b'data: {"reason": ""}\n', b"\n"]
    return wire


@bench(
    "tui.tail_ingest_30k",
    "tui",
    detail="live log tail: SSE parse + buffer for 30k streamed lines "
    "(5k-line cap)",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_tail_ingest():
    """The per-line path of a live log tail on the client side: the SSE
    frame parse in ``Api.stream``, then ``LogTail`` sanitizing, buffering
    and trimming at its 5,000-line cap.

    This runs once per streamed line while a log drawer or tail pane is
    open, on the loop that also paints.  The terminal twin of
    ``webui.append_line_5k``; the frames a flood paints are under
    ``tui.drawer_paint_5k``.
    """
    import asyncio

    tui = _tui_module()
    for attr in ("Api", "LogTail"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)
    try:
        import aiohttp  # noqa: F401  (Api.stream imports it; keep it untimed)
    except ImportError as exc:
        raise Skip("aiohttp unavailable: %r" % exc) from None
    n = _n(30000, floor=200)
    wire = fixture("tui_sse_wire_30k", lambda: _tui_sse_wire(n))

    class _Content:
        def __aiter__(self):
            self._lines = iter(wire)
            return self

        async def __anext__(self):
            try:
                return next(self._lines)
            except StopIteration:
                raise StopAsyncIteration from None

    class _Response:
        status = 200

        def __init__(self):
            self.content = _Content()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Session:
        def get(self, url, **kwargs):
            return _Response()

        async def close(self):
            pass

    try:
        api = tui.Api("http://127.0.0.1:1", None)
        tail = tui.LogTail(api, "/jobs/bench/logs", "bench", lambda: None)
    except TypeError as exc:
        raise Skip("Api/LogTail construction changed: %r" % exc) from None
    if not hasattr(api, "_session") or not hasattr(tail, "_run"):
        raise Skip("Api._session / LogTail._run not present")
    api._session = _Session()
    tail.follow = False  # one attach: the stream ends with the run

    async def run():
        t0 = time.perf_counter()
        await tail._run()
        return time.perf_counter() - t0

    dt = asyncio.run(run())
    kept = min(n + 1, tail.MAX_LINES)  # the lines plus the end marker
    if tail.error is not None or tail.ended != "" or len(tail.lines) != kept:
        raise RuntimeError(
            "tail kept %d of %d lines (error=%r, ended=%r)"
            % (len(tail.lines), n, tail.error, tail.ended)
        )
    return dt


@bench(
    "tui.mark_idle_300",
    "tui",
    detail="header mark physics: 300 one-second breaths of a balanced, "
    "connected mark",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_tui_mark_idle():
    """Five idle minutes of the living header mark: one ``step(1.0)`` per
    tick, 120 integrator substeps each.

    The step runs every second of every session, and on an 80x24 terminal
    it costs more than the frame it animates.  A fixed seed makes the
    breeze, and so the workload, the same on every run.
    """
    tui = _tui_module()
    if not hasattr(tui, "PendulumMark"):
        raise Skip("cronstable.tui lacks PendulumMark")
    try:
        sim = tui.PendulumMark(seed=1234, connected=True)
    except TypeError as exc:
        raise Skip("PendulumMark construction changed: %r" % exc) from None
    steps = _n(300, floor=3)
    t0 = time.perf_counter()
    for _ in range(steps):
        sim.step(1.0)
    dt = time.perf_counter() - t0
    if abs(sim.t - steps) > 1e-6 or sim.frame()[0] not in "l/\\_":
        raise RuntimeError(
            "mark simulated %.3f s in %d steps (frame %r)"
            % (sim.t, steps, sim.frame())
        )
    return dt


# ---------------------------------------------------------------------------
# pair: the QR symbol behind `cronstable pair` and the dashboard's Pair a
# device panel.  One encode per pairing link, on the event loop in the
# dashboard, by a pure-Python encoder whose mask search scores eight
# candidate symbols.
# ---------------------------------------------------------------------------
@bench(
    "pair.qr_encode_14",
    "pair",
    detail="qr.encode at level L, versions 7 to 13: the 14 golden-vector "
    "lengths, default mask search",
    repeats=(5, 2, 1),
    gate_floor=0.005,
)
def bench_pair_qr_encode():
    """The pairing code's encode at the sizes real links land on: version
    7 (no token) through version 13 (a long token and host name), level L,
    the only level the clients ask for.

    The inputs are the ones ``tests/data/qr_golden.json`` covers for those
    versions (the smallest and the largest length each holds), rebuilt
    here from the same formula.  The golden replay pins the mask and so
    skips the penalty scoring, which is nine tenths of a real encode; this
    runs the default search.
    """
    try:
        from cronstable import qr
    except ImportError as exc:
        raise Skip("cronstable.qr unavailable: %r" % exc) from None
    for attr in ("encode", "data_capacity", "symbol_size"):
        if not hasattr(qr, attr):
            raise Skip("cronstable.qr lacks %s" % attr)

    def build():
        inputs = []
        for version in range(7, 14):
            largest = qr.data_capacity(version, "L")
            smallest = qr.data_capacity(version - 1, "L") + 1
            for length in (smallest, largest):
                data = bytes(
                    (i * 167 + length * 13 + 1) % 256 for i in range(length)
                )
                inputs.append((version, data))
        return inputs

    inputs = fixture("pair_qr_inputs", build)[: _n(14, floor=2)]
    t0 = time.perf_counter()
    sizes = [len(qr.encode(data, "L")) for _version, data in inputs]
    dt = time.perf_counter() - t0
    expected = [qr.symbol_size(version) for version, _data in inputs]
    if sizes != expected:
        raise RuntimeError(
            "qr.encode produced symbols of %r modules; expected %r"
            % (sizes, expected)
        )
    return dt


# ---------------------------------------------------------------------------
# webui: the browser dashboard's render hot paths, timed inside a headless
# Chromium via the page's ?perf=1 __perf hook.  The whole group skips unless
# Playwright + its Chromium build are installed AND the page carries the hook
# (an older release predates it), and never runs in --smoke (the unit test must
# not launch a browser).  These are the client-side twins of the tui.* metrics.
# ---------------------------------------------------------------------------


def _web_page():
    """Launch headless Chromium once and load the hooked dashboard page.

    Cached for the webui group and shut down by its fixture finalizer at the
    group boundary: Playwright's sync API parks a RUNNING asyncio loop on the
    calling thread between calls, so leaving the session open (the pre-1.2.31
    "torn down at interpreter exit" design) made every later asyncio.run()
    benchmark skip on both sides of the CI pairing, a dead-gate failure on
    release runs.  For the same reason every early exit here (failed launch,
    hookless page) stops the session before raising Skip; a missing
    dependency, a failed launch, or a hookless page each raise Skip so the
    metrics record as skipped, never failed.

    ``CRONSTABLE_TEST_BROWSER_CHANNEL`` names an installed browser to drive
    (``msedge``, ``chrome``), the variable the browser tests read; unset, the
    bundled Chromium launches.  An init script turns the page's startup
    self-test off, so the app starts at load and its keyboard handlers
    answer from the first benchmark on.  The Skip reason is the first line
    of the launch error, which is plain ASCII on every console.
    """

    def build():
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise Skip("playwright not installed: %r" % exc) from None
        try:
            import cronstable.web

            page_path = os.path.join(
                os.path.dirname(cronstable.web.__file__), "index.html"
            )
        except ImportError as exc:
            raise Skip("cronstable.web unavailable: %r" % exc) from None
        if not os.path.exists(page_path):
            raise Skip("web/index.html not found next to cronstable.web")
        pw = None
        browser = None
        done = []

        def close():
            # Idempotent: called by the fixture finalizer at the group
            # boundary, and again by the atexit net if the suite dies first.
            if done:
                return
            done.append(True)
            if browser is not None:
                try:
                    browser.close()
                except Exception:  # noqa: BLE001 - teardown must not raise
                    pass
            if pw is not None:
                try:
                    pw.stop()
                except Exception:  # noqa: BLE001 - teardown must not raise
                    pass

        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch(
                channel=os.environ.get("CRONSTABLE_TEST_BROWSER_CHANNEL")
                or None
            )
            page = browser.new_page()
            page.add_init_script(_WEB_NO_BOOT)
            page.goto(_web_file_url(page_path) + "?perf=1")
            page.wait_for_timeout(300)
            if not page.evaluate("() => !!(window.__perf)"):
                raise Skip(
                    "page lacks the ?perf=1 __perf hook (older release)"
                )
        except Skip:
            close()
            raise
        except Exception as exc:  # noqa: BLE001 - any failure is a skip
            close()
            # the first line names the cause; the rest is a launcher banner
            reason = str(exc).splitlines()[0] if str(exc) else repr(exc)
            raise Skip(
                "chromium launch/page load failed: %s" % reason
            ) from None
        atexit.register(close)
        return page, close

    # A failed build is not cached by fixture(), so the reason is kept here:
    # the rest of the group skips at once without another launch attempt.
    if _WEB_UNAVAILABLE:
        raise Skip(_WEB_UNAVAILABLE[0])
    try:
        page, _close = fixture(
            "web_page", build, finalizer=lambda pair: pair[1]()
        )
    except Skip as exc:
        _WEB_UNAVAILABLE.append(str(exc))
        raise
    return page


def _web_time(page, setup_js, op_js, batch=10, batches=12, warm=2):
    """Min per-op wall time (seconds) of ``op_js`` after ``setup_js``.

    Timed with the page's own ``performance.now()`` so only the render work is
    measured, never the Python<->browser round trip.  Each batch times
    ``batch`` ops together and divides: Chromium clamps ``performance.now()``
    to ~100us, so a single fast render reads as zero; a batch clears the clamp.
    The MIN batch-mean over ``batches`` is the least-noisy statistic, matching
    the suite's ``compare='min'``.  ``warm`` untimed ops run first; an op
    that is itself tens of milliseconds of repeated work needs one.
    """
    ms = page.evaluate(
        "() => { %s; for (let w=0; w<%d; w++) { %s; }"
        " let best = Infinity;"
        " for (let b=0; b<%d; b++) {"
        "   const a = performance.now();"
        "   for (let i=0; i<%d; i++) { %s; }"
        "   best = Math.min(best, (performance.now() - a) / %d); }"
        " return best; }"
        % (setup_js, warm, op_js, batches, batch, op_js, batch)
    )
    return ms / 1000.0


@bench(
    "webui.render_rows_500",
    "webui",
    detail="renderRows full rebuild over 500 jobs (headless Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_render_rows():
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    return _web_time(
        page,
        "__perf.seedJobs(%d)" % _n(500),
        "__perf.renderRows()",
    )


@bench(
    "webui.render_fleet_15x400",
    "webui",
    detail="renderFleet full rebuild, 15 nodes x 400 jobs (headless Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_render_fleet():
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    return _web_time(
        page,
        "__perf.seedJobs(%d); __perf.seedFleet(15, %d)" % (_n(400), _n(400)),
        "__perf.renderFleet()",
    )


@bench(
    "webui.log_count_5k",
    "webui",
    detail="updateLogCount over a 5k-line buffer with a search (Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_log_count():
    """The match-count upkeep beside the log view.

    Deliberately narrow, and retained rather than retargeted: it is the one
    metric that can see the incremental match-count fold (appendLine keeps
    state.log.matchCount in step per line, so only a query CHANGE rescans),
    and it costs a fraction of a millisecond by design.  The expensive
    function next to it, the full DOM rebuild in renderTerm that the same
    query change also triggers, is measured by webui.render_term_5k; the
    pair is what a "search keystroke" actually costs.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    return _web_time(
        page,
        "__perf.seedLog(%d, 'worker')" % _n(5000),
        "__perf.updateLogCount()",
    )


@bench(
    "webui.render_term_5k",
    "webui",
    detail="renderTerm full log rebuild, 5k lines with a search (Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_render_term():
    """The log view's full rebuild: one <span> tree per buffered line.

    Every search keystroke (debounced), every ansi/timestamp/regex toggle
    and every stream (re)attach runs this over the whole retained buffer,
    and at the shipped 5000-line cap it is the single most expensive
    function in the dashboard, two orders of magnitude above the
    updateLogCount call that sits next to it and that webui.log_count_5k
    measures.

    Driven through the page's OWN listener rather than a __perf hook: the
    'matches only' toggle calls renderTerm bare (no pref write, no query
    change, both directions build every line), so the metric works against
    any release that has the checkbox.  A hook-only drive would measure
    nothing on the baseline side of a paired run and gate nothing.  The
    render is asserted to have produced line nodes so a listener that
    stopped being wired cannot time an event dispatch.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    setup = (
        "__perf.seedLog(%d, 'worker');"
        " const _only = document.getElementById('optOnly');"
        " if (!_only) throw new Error('no optOnly toggle');" % _n(5000)
    )
    op = (
        "_only.checked = !_only.checked;"
        " _only.dispatchEvent(new Event('change'))"
    )
    value = _web_time(page, setup, op, batch=2, batches=8)
    rendered = page.evaluate(
        "() => document.querySelectorAll('#term .ln').length"
    )
    if not rendered:
        raise RuntimeError(
            "renderTerm produced no line nodes; the toggle is no longer "
            "wired to it and the region timed an event dispatch"
        )
    return value


@bench(
    "webui.append_line_5k",
    "webui",
    detail="appendLine per streamed line into a full 5k buffer (Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    # microseconds per line by design, so the webui group's 2ms floor (sized
    # for whole-view rebuilds) would leave this gating only on a ~300x move.
    # 2us is the batch-mean's own jitter band, which is what a floor is for.
    gate_floor=0.000002,
)
def bench_web_append_line():
    """The per-streamed-line browser cost, paid once per line per viewer.

    A chatty job emits thousands of lines a minute and each one runs
    appendLine: a scrollHeight/scrollTop read (a forced layout against the
    whole buffer), the O(1) ring trim, one node build, and the incremental
    match-count fold.  Nothing else in the suite measures it; the server
    side of the same line is webapi.sse_burst_20k.

    Needs a ``__perf.appendLine`` hook, because appendLine lives inside the
    page's module closure and no DOM event reaches it (its only callers are
    the SSE readers, which need a backend).  A release without the hook
    records as skipped, never failed, exactly like the ?perf=1 gate itself.
    Both hook shapes are driven: the index form the page ships and the
    (stream, text) form, chosen from the function's own arity so a paired
    run cannot end up timing two different calls.

    The batch is large deliberately.  Chromium clamps performance.now() to
    ~100us and a fixed append is well under that, so a small batch reads as
    a flat zero, which is how a metric silently stops measuring.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    arity = page.evaluate(
        "() => (typeof __perf.appendLine === 'function')"
        " ? __perf.appendLine.length : -1"
    )
    if arity < 0:
        raise Skip("page lacks the __perf.appendLine hook")
    op = (
        "__perf.appendLine('stdout', 'worker ' + (_i++) + ' ok')"
        if arity >= 2
        else "__perf.appendLine(_i++)"
    )
    n = _n(5000)
    # seed to the shipped cap first, so every timed append also pays the
    # ring trim and the layout read against a FULL buffer
    value = _web_time(
        page,
        "let _i = 0; __perf.seedLog(%d, 'worker');"
        " if (__perf.renderTerm) __perf.renderTerm()" % n,
        op,
        batch=100,
        batches=6,
    )
    rendered = page.evaluate(
        "() => document.querySelectorAll('#term .ln').length"
    )
    if not rendered:
        raise RuntimeError(
            "the buffer holds no line nodes after the appends; the hook "
            "did not reach appendLine"
        )
    return value


@bench(
    "webui.week_walk_500x20",
    "webui",
    detail="20 cold computeWeek walks over 500 varied schedules, UTC and "
    "America/New_York alternating (headless Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_week_walk():
    """The week calendar's seven-day enumeration of every enabled schedule.

    The page keeps each distinct (schedule, frame)'s fires for the day
    window, so a poll on an unchanged fleet walks nothing; this times the
    cold walk the panel pays on enable, on a schedule edit and at midnight,
    over the suite's schedule mix on both frame kinds the engine has: UTC
    (Date arithmetic) and an IANA zone (one Intl offset read per hour the
    walk touches).  One walk is a few milliseconds, so the timed region is
    20 of them; the hook clears the memos before each.  Needs the
    ``__perf.computeWeek`` hook; a release without it records as skipped,
    never failed.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    if not page.evaluate("() => typeof __perf.computeWeek === 'function'"):
        raise Skip("page lacks the __perf.computeWeek hook")
    value = _web_time(
        page,
        "__perf.seedJobs(%d); __perf.varySchedules()" % _n(500),
        "for (let k = 0; k < 20; k++) __perf.computeWeek()",
        batch=1,
        batches=4,
        warm=1,
    )
    walked = page.evaluate("() => __perf.state().week.items.length")
    if not walked:
        raise RuntimeError("computeWeek placed no fires on the grid")
    return value


@bench(
    "webui.radar_walk_500x150",
    "webui",
    detail="150 cold computeRadar walks over 500 varied schedules, UTC "
    "and America/New_York alternating (headless Chromium)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_radar_walk():
    """The radar's next fire per enabled schedule, the walk the poll loop
    runs while the panel is open.

    The page keeps each distinct (schedule, frame)'s next fire until it
    passes, so a poll re-walks only the schedules whose fire has elapsed;
    this times the cold walk over the whole fleet.  One walk is a fraction
    of a millisecond, so the timed region is 150 of them; the hook clears
    the memos before each.  Needs the ``__perf.computeRadar`` hook; a
    release without it records as skipped.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    if not page.evaluate("() => typeof __perf.computeRadar === 'function'"):
        raise Skip("page lacks the __perf.computeRadar hook")
    n = _n(500)
    value = _web_time(
        page,
        "__perf.seedJobs(%d); __perf.varySchedules()" % n,
        "for (let k = 0; k < 150; k++) __perf.computeRadar()",
        batch=1,
        batches=4,
        warm=1,
    )
    found = page.evaluate("() => __perf.state().radar.items.length")
    if found != n:
        raise RuntimeError(
            "computeRadar found %r next fires, expected %d" % (found, n)
        )
    return value


#: The Skip reason of a browser session that failed to start, if one did.
_WEB_UNAVAILABLE = []


#: Stored before the page's own script runs: no startup self-test screen.
_WEB_NO_BOOT = (
    "try { localStorage.setItem('cronstable.boot', 'false'); } catch (_) {}"
)


def _web_file_url(path):
    return "file://" + path.replace("\\", "/")


# Every benchmark below shares the one page _web_page() loads, so each setup
# starts from this known state: connected, unfiltered, sorted by name, and
# holding no fleet matrix from an earlier benchmark.
_WEB_RESET = (
    "const _s = __perf.state();"
    " _s.connected = true; _s.seenFirst = true;"
    " _s.filter = ''; _s.statusFilter = 'all';"
    " _s.sort = 'name'; _s.sortDir = 1; _s.selected = null;"
    " document.getElementById('search').value = '';"
    " _s.fleet.data = null; _s.fleet.lastSig = null;"
    " document.getElementById('fleetPanel').textContent = '';"
)


# The 20-run history tail a real /jobs payload carries per job.  rowSig joins
# it and the sparkline draws it, so a seeded fleet without one understates
# both.
_WEB_HISTORY = (
    " _s.jobs.forEach((j, i) => { j.history = Array.from({ length: 20 },"
    " (_, k) => ({ outcome: (i + k) % 9 ? 'success' : 'failure',"
    " duration: 1 + ((i * 7 + k * 13) % 50) / 10 })); });"
)


def _web_guard(page, *hooks):
    """Raise Skip when the page lacks one of the ``__perf`` ``hooks``."""
    for hook in hooks:
        if not page.evaluate(
            "(name) => typeof __perf[name] === 'function'", hook
        ):
            raise Skip("page lacks the __perf.%s hook" % hook)


def _web_seed(n):
    return _WEB_RESET + " __perf.seedJobs(%d);" % n + _WEB_HISTORY


@bench(
    "webui.rows_diff_500",
    "webui",
    detail="25 polls through the keyed row diff, 10 of 500 rows moved "
    "per poll (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_rows_diff():
    """The path a poll takes through the jobs table.

    renderRows signs every row and swaps the ones whose signature moved.
    webui.render_rows_500 times the wholesale rebuild, which runs on the
    first paint only.  Guards the per-row signature and the single-row
    swap: a signature that reads the clock, or a diff that falls back to
    the rebuild, moves this.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "touchJob", "renderRowsDiff")
    n = _n(500)
    value = _web_time(
        page,
        _web_seed(n) + " __perf.renderRows(); let _c = 1;",
        "for (let p = 0; p < 25; p++) {"
        " for (let t = 0; t < 10; t++) __perf.touchJob(_c++ * 53);"
        " __perf.renderRowsDiff(); }",
        batch=1,
        batches=4,
        warm=1,
    )
    swapped = page.evaluate(
        "() => { const rows = document.getElementById('rows');"
        " const before = new Set(rows.children);"
        " for (let t = 0; t < 10; t++) __perf.touchJob(t * 53 + 7);"
        " __perf.renderRowsDiff(); let fresh = 0;"
        " for (const tr of rows.children) if (!before.has(tr)) fresh++;"
        " return [fresh, rows.children.length]; }"
    )
    if swapped != [min(10, n), n]:
        raise RuntimeError(
            "the diff swapped %r rows of %r, expected %d of %d"
            % (swapped[0], swapped[1], min(10, n), n)
        )
    return value


@bench(
    "webui.tick_500",
    "webui",
    detail="8 one-second ticks with their layout: 500 job rows and a "
    "15x400 fleet matrix (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_tick():
    """The per-second pass that keeps every relative time current.

    tick sweeps each [data-ago], [data-ago-short] and [data-next] cell in
    the document and writes the ones whose text changed.  The page clock
    moves one second per tick, so the countdown cells really change, and a
    layout read after each tick charges the writes their cost.  Guards the
    sweep: more cells per row or a costlier formatter moves this.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "tick", "seedFleet", "renderFleet")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _WEB_RESET
            + " __perf.seedJobs(%d); __perf.renderRows();" % n
            + " __perf.seedFleet(15, %d); __perf.renderFleet();" % _n(400)
            + " const _base = _s.fetchedAt;",
            "_s.fetchedAt = _base;"
            " for (let k = 0; k < 8; k++) { _s.fetchedAt -= 1000;"
            " __perf.tick(); void document.body.offsetHeight; }",
            batch=1,
            batches=4,
            warm=1,
        )
        swept = page.evaluate(
            "() => { const s = __perf.state();"
            " const cell = document.querySelector('#rows [data-next]');"
            " const before = cell.textContent;"
            " s.fetchedAt -= 3600000; __perf.tick();"
            " return [document.querySelectorAll("
            "'#rows [data-ago]').length, document.querySelectorAll("
            "'[data-ago-short]').length, cell.textContent !== before]; }"
        )
    finally:
        page.evaluate("() => { %s }" % _WEB_RESET)
    if swept[0] != n or not swept[1] or not swept[2]:
        raise RuntimeError(
            "tick swept %r job cells and %r fleet cells (countdown moved: "
            "%r); the seeded panels are not what it walks" % tuple(swept)
        )
    return value


@bench(
    "webui.rows_layout_200",
    "webui",
    detail="renderRows full rebuild over 200 jobs plus the style and "
    "layout pass it leaves behind (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_rows_layout():
    """The whole cost of painting the jobs table for the first time.

    webui.render_rows_500 stops when the markup is parsed; the browser
    then styles and lays out every cell, which costs several times the
    parse.  A layout read after the rebuild puts that pass inside the timed
    region.  Guards the table's CSS as well as its markup: a selector or a
    layout mode that makes each row dearer moves this and leaves the
    script-only metric flat.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "renderRows")
    n = _n(200)
    value = _web_time(
        page,
        _web_seed(n),
        "__perf.renderRows(); void document.body.offsetHeight",
        batch=1,
        batches=4,
        warm=1,
    )
    shape = page.evaluate(
        "() => { const rows = document.getElementById('rows');"
        " return [rows.children.length,"
        " rows.getBoundingClientRect().height > 0]; }"
    )
    if shape != [n, True]:
        raise RuntimeError(
            "the rebuild left %r rows (laid out: %r), expected %d"
            % (shape[0], shape[1], n)
        )
    return value


def _web_boot_script(n):
    """An init script that answers the page's first requests in-process.

    The /jobs body holds ``n`` jobs with fixed timestamps.  The script
    records when the first job rows are in the document and laid out,
    on the page's own clock, whose origin is the start of navigation.
    """

    def build():
        jobs = []
        for i in range(n):
            failed = i % 5 == 0
            jobs.append(
                {
                    "name": "job%d" % i,
                    "enabled": True,
                    "running": i % 7 == 0,
                    "schedule": "%d %d * * *" % (i % 60, (i * 7) % 24),
                    "command": "echo %d" % i,
                    "captureStdout": True,
                    "captureStderr": True,
                    "utc": True,
                    "timezone": None,
                    "pids": [1] if i % 7 == 0 else [],
                    "scheduled_in": 30 + i % 100,
                    "never_fires": False,
                    "last_run": {
                        "outcome": "failure" if failed else "success",
                        "exit_code": 1 if failed else 0,
                        "started_at": "2026-03-15T12:%02d:00+00:00" % (i % 60),
                        "finished_at": "2026-03-15T12:%02d:01+00:00"
                        % (i % 60),
                        "duration": 1.2,
                        "fail_reason": None,
                        "resources": None,
                    },
                    "history": [
                        {
                            "outcome": "success" if (i + k) % 9 else "failure",
                            "duration": 1 + ((i * 7 + k * 13) % 50) / 10,
                        }
                        for k in range(20)
                    ],
                    "paused": None,
                }
            )
        return (
            "(() => {"
            " try { localStorage.setItem('cronstable.boot', 'false'); }"
            " catch (_) {}"
            " const jobs = %s;"
            " const answers = { '/jobs': jobs, '/version': '0.0.0',"
            " '/whoami': '{\"authenticated\":false,\"allScopes\":true}',"
            ' \'/job-set-id\': \'{"job_set_id":"v1:0","jobs":%d}\','
            " '/cluster': '{\"enabled\":false}', '/dags': '[]',"
            " '/pools': '[]', '/node': '{\"resources\":null}' };"
            " window.fetch = async (path) => {"
            " const body = answers[String(path).split('?')[0]];"
            " return body === undefined"
            " ? new Response('{}', { status: 404 })"
            " : new Response(body, { status: 200 }); };"
            " new MutationObserver((list, observer) => {"
            " const rows = document.getElementById('rows');"
            " if (!rows || !rows.firstElementChild) return;"
            " observer.disconnect();"
            " void document.body.offsetHeight;"
            " window.__bootRows = [performance.now(),"
            " rows.children.length]; })"
            ".observe(document, { childList: true, subtree: true });"
            " })();"
        ) % (json.dumps(json.dumps(jobs)), n)

    return fixture("web_boot_script_%d" % n, build)


@bench(
    "webui.boot_rows_500",
    "webui",
    detail="navigation to the first laid-out jobs table, 500 jobs, "
    "requests answered in-page (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
    info=True,
)
def bench_web_boot_rows():
    """One cold load of the dashboard, start of navigation to first table.

    Parses and runs the single-file page, wires it, applies the first
    /jobs answer, and lays the rows out, in a fresh browser context per
    call.  Every other webui metric starts from a loaded page, so only
    this one moves with the size of the page and the work it does at
    startup.  It drives no hook, so it runs against any release.

    Recorded, never gated: a page load spans several browser processes,
    and under the harness's one-CPU pin its value swings by a factor of
    two between rounds.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    shared = _web_page()
    n = _n(500)
    script = _web_boot_script(n)
    context = shared.context.browser.new_context()
    try:
        context.add_init_script(script)
        page = context.new_page()
        page.goto(shared.url.split("?")[0])
        page.wait_for_function("() => !!window.__bootRows", timeout=30000)
        millis, rows = page.evaluate("() => window.__bootRows")
    finally:
        context.close()
    if rows != n:
        raise RuntimeError(
            "the first table held %r rows, expected %d" % (rows, n)
        )
    return millis / 1000.0


@bench(
    "webui.filter_keys_500",
    "webui",
    detail="filter box keystrokes over 500 jobs: narrow to the 'job1' "
    "rows and widen back, twice (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_filter_keys():
    """What one keystroke in the filter box costs.

    The input listener calls renderRows on every keystroke, with no
    debounce.  Narrowing drops the rows that stop matching; widening
    builds each returning row again, one template parse per row.  Driven
    through the page's own input listener, so it runs against any release.
    Guards the filter pass and the per-row build path.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "renderRows")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _web_seed(n) + " __perf.renderRows();"
            " const _q = document.getElementById('search');"
            " const _type = (v) => { _q.value = v;"
            " _q.dispatchEvent(new Event('input')); };",
            "for (let k = 0; k < 2; k++) { _type('job1'); _type('job'); }",
            batch=1,
            batches=4,
            warm=1,
        )
        counts = page.evaluate(
            "() => { const q = document.getElementById('search');"
            " const rows = document.getElementById('rows');"
            " const type = (v) => { q.value = v;"
            " q.dispatchEvent(new Event('input'));"
            " return rows.children.length; };"
            " return [type('job1'), type('job')]; }"
        )
    finally:
        page.evaluate(
            "() => { const q = document.getElementById('search');"
            " q.value = ''; q.dispatchEvent(new Event('input')); }"
        )
    narrowed = sum(1 for i in range(n) if str(i).startswith("1"))
    if counts != [narrowed, n]:
        raise RuntimeError(
            "the filter showed %r rows, expected %r" % (counts, [narrowed, n])
        )
    return value


@bench(
    "webui.wallboard_poll_500",
    "webui",
    detail="2 wallboard polls with one job finished each, 500 tiles "
    "(headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_wallboard_poll():
    """The wallboard's work on a poll that carries one finished job.

    The grid has one signature for the whole fleet, so a single changed
    job rebuilds every tile and runs the fit governor again.  A wallboard
    stays up for days, and on a busy fleet most polls carry a change.
    Driven by a hashchange event while the hash is #tv, which makes the
    page call renderWallboard the way a poll does; no hook is involved.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "touchJob")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _web_seed(n)
            + " if (!_s.wallboard) document.getElementById('tvBtn').click();"
            " const _poll = () => window.dispatchEvent("
            "new HashChangeEvent('hashchange'));"
            " _poll(); let _c = 1;",
            "for (let k = 0; k < 2; k++) {"
            " __perf.touchJob(_c++ * 53); _poll(); }",
            batch=1,
            batches=4,
            warm=1,
        )
        grid = page.evaluate(
            "() => { const grid = document.getElementById('wbGrid');"
            " const poll = () => window.dispatchEvent("
            "new HashChangeEvent('hashchange'));"
            " poll(); const steady = grid.firstElementChild;"
            " poll(); const kept = grid.firstElementChild === steady;"
            " __perf.touchJob(11); poll();"
            " return [grid.children.length, kept,"
            " grid.firstElementChild !== steady]; }"
        )
    finally:
        page.evaluate(
            "() => { if (__perf.state().wallboard)"
            " document.getElementById('tvBtn').click(); }"
        )
    if grid != [n, True, True]:
        raise RuntimeError(
            "wallboard grid: %r tiles, steady poll kept them: %r, changed "
            "poll rebuilt them: %r" % tuple(grid)
        )
    return value


# A merged tail buffer of N_LINES lines from four jobs, two thirds of them
# colored, with a search that matches those; the shape seedLog gives the
# single-job buffer.
_WEB_TAIL = (
    " const _tail = _s.tail; const _colors = ['var(--ansi-34)',"
    " 'var(--ansi-32)', 'var(--ansi-33)', 'var(--ansi-35)'];"
    " _tail.jobs = ['job1', 'job2', 'job3', 'job4'];"
    " _tail.lines = Array.from({ length: N_LINES }, (_, i) => ({"
    " job: 'job' + (1 + i % 4), color: _colors[i % 4],"
    " stream: 'stdout', kind: null, ts: 1773577845000, n: i,"
    " text: i % 3 ? '\\x1b[32mINFO\\x1b[0m worker ' + i + ' ok'"
    " : 'plain line ' + i }));"
    " _tail.total = N_LINES;"
    " document.getElementById('tailSearch').value = 'worker';"
    " const _ts = document.getElementById('tailTs');"
    " if (!_ts) throw new Error('no tailTs toggle');"
)


@bench(
    "webui.tail_term_2k",
    "webui",
    detail="renderTailTerm full rebuild, 2k lines from 4 jobs with a "
    "search (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_tail_term():
    """The merged tail console's full rebuild.

    The search box, the timestamp toggle and Clear each rebuild every
    buffered line: a job label, an ANSI pass and a search highlight per
    line, then one layout to pin the scroll.  It is a separate function
    from the single-job renderTerm that webui.render_term_5k times.
    Driven through the page's own timestamp checkbox, so it runs against
    any release.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state")
    n = _n(2000)
    try:
        value = _web_time(
            page,
            _WEB_RESET + _WEB_TAIL.replace("N_LINES", str(n)),
            "_ts.checked = !_ts.checked;"
            " _ts.dispatchEvent(new Event('change'))",
            batch=1,
            batches=4,
            warm=1,
        )
        shape = page.evaluate(
            "() => [document.querySelectorAll('#tailTerm .ln').length,"
            " document.querySelectorAll('#tailTerm mark').length]"
        )
    finally:
        # an even number of toggles leaves the timestamp setting as it was
        page.evaluate(
            "() => { const ts = document.getElementById('tailTs');"
            " if (ts && ts.checked) { ts.checked = false;"
            " ts.dispatchEvent(new Event('change')); }"
            " const tail = __perf.state().tail;"
            " tail.lines = []; tail.jobs = []; tail.total = 0;"
            " document.getElementById('tailSearch').value = '';"
            " document.getElementById('tailTerm').textContent = ''; }"
        )
    if shape[0] != n or not shape[1]:
        raise RuntimeError(
            "the tail console holds %r lines and %r highlights, expected "
            "%d lines; the toggle is not wired to renderTailTerm"
            % (shape[0], shape[1], n)
        )
    return value


# A chain DAG of n tasks with a run in progress and a 500-run table, placed
# on the page's state object the way the DAG drawer's loaders leave it.
_WEB_DAG = (
    " const _ids = Array.from({ length: N_TASKS },"
    " (_, i) => 't' + String(i).padStart(5, '0'));"
    " const _dag = { name: 'bench', enabled: true, schedule: '0 * * * *',"
    " tasks: _ids.map((id, i) => ({ id, type: 'task', retries: 2,"
    " dependsOn: i ? [_ids[i - 1]] : [] })), totalRuns: N_RUNS,"
    " runCounts: { success: N_RUNS } };"
    " const _tasks = {}; _ids.forEach((id, i) => { _tasks[id] = {"
    " state: i % 7 ? 'success' : 'running', attempt: 1,"
    " exitCode: i % 7 ? 0 : null, startedAt: 1773577000 + i,"
    " finishedAt: i % 7 ? 1773577100 + i : null, host: 'node1' }; });"
    " _s.dags.list = [_dag]; _s.dags.byName = { bench: _dag };"
    " Object.assign(_s.dagDrawer, { dag: 'bench', runKey: 'r0',"
    " collapsed: {}, run: { state: 'running', kind: 'manual',"
    " createdAt: 1773577000, tasks: _tasks, mapped: {} },"
    " runs: Array.from({ length: N_RUNS }, (_, i) => ({ runKey: 'r' + i,"
    " state: i % 9 ? 'success' : 'failed', kind: 'schedule',"
    " taskStates: { success: N_TASKS - 3, failed: 2, running: 1 },"
    " logicalDate: '2026-03-15T12:00:00+00:00',"
    " createdAt: 1773577000 - i * 3600,"
    " updatedAt: 1773577060 - i * 3600 })) });"
    " const _tab = (k) => document.querySelector("
    "'#dagTabs button[data-dtab=\"' + k + '\"]').click();"
)


@bench(
    "webui.dag_tasks_2k",
    "webui",
    detail="DAG drawer poll for a 2,000-task run: the Tasks tab and the "
    "500-run table rebuilt, twice (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_dag_tasks():
    """What the DAG drawer rebuilds every three seconds for a live run.

    While the open run is running, the drawer polls it and rebuilds the
    active tab and the run table in full.  The Graph tab refuses a DAG
    over 140 tasks, so for a large DAG the Tasks tab is the view that
    scales, which makes this the browser twin of tui.dag_graph_paint_2k.
    Seeded through the page's state object and driven through the tab
    buttons, so it runs against any release that has the drawer.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state")
    tasks, runs = _n(2000), _n(500)
    if not page.evaluate(
        "() => !!(__perf.state().dagDrawer"
        " && document.querySelector('#dagTabs button[data-dtab=tasks]'))"
    ):
        raise Skip("page has no DAG drawer")
    try:
        value = _web_time(
            page,
            _WEB_RESET
            + _WEB_DAG.replace("N_TASKS", str(tasks)).replace(
                "N_RUNS", str(runs)
            ),
            "for (let k = 0; k < 2; k++) { _tab('tasks'); _tab('runs'); }",
            batch=1,
            batches=4,
            warm=1,
        )
        shape = page.evaluate(
            "() => [document.querySelectorAll('#dgTasks tbody tr').length,"
            " document.querySelectorAll('#dgRuns tbody tr').length]"
        )
    finally:
        page.evaluate(
            "() => { const s = __perf.state();"
            " Object.assign(s.dagDrawer, { dag: null, runKey: null,"
            " run: null, runs: [] });"
            " s.dags.list = []; s.dags.byName = {}; s.dags.sig = null;"
            " for (const id of ['dgTasks', 'dgRuns'])"
            " document.getElementById(id).textContent = ''; }"
        )
    if shape != [tasks, runs]:
        raise RuntimeError(
            "the drawer drew %r task rows and %r run rows, expected %r"
            % (shape[0], shape[1], [tasks, runs])
        )
    return value


@bench(
    "webui.dirty_frame_rows_500",
    "webui",
    detail="main-thread time per repainted frame with 500 job rows in "
    "the document, over 30 frames (headless Chromium, CDP)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.0005,
    info=True,
)
def bench_web_dirty_frame():
    """What one repainted frame costs while a large table is on screen.

    The logo sways on every animation frame, and the per-second tick
    repaints too, so the cost of a frame that touches the header is paid
    continuously.  The jobs table is an isolated stacking context, which
    lets the browser reuse its recorded painting on such a frame; without
    that, each frame walks every row.  The workload parks the logo and
    rewrites the header clock once per frame, then reads the main thread's
    task time for those frames through the DevTools protocol.  It is a
    rate over a wall-clock window, so it reports without gating.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "renderRows")
    try:
        session = page.context.new_cdp_session(page)
        session.send("Performance.enable")
    except Exception as exc:  # noqa: BLE001 - any failure is a skip
        raise Skip("no DevTools session: %r" % exc) from None
    frames = max(10, _n(30))

    def task_seconds():
        for entry in session.send("Performance.getMetrics")["metrics"]:
            if entry["name"] == "TaskDuration":
                return entry["value"]
        raise Skip("the browser reports no TaskDuration metric")

    try:
        page.evaluate(
            "() => { %s __perf.renderRows();"
            " const motion = document.getElementById('setMotion');"
            " window.__motionWas = motion.checked;"
            " if (!motion.checked) { motion.checked = true;"
            " motion.dispatchEvent(new Event('change')); }"
            " void document.body.offsetHeight; }" % _web_seed(_n(500))
        )
        page.evaluate(_WEB_FRAMES, 5)
        before = task_seconds()
        drawn = page.evaluate(_WEB_FRAMES, frames)
        spent = task_seconds() - before
    finally:
        try:
            page.evaluate(
                "() => { const motion = document.getElementById('setMotion');"
                " if (motion.checked && window.__motionWas === false) {"
                " motion.checked = false;"
                " motion.dispatchEvent(new Event('change')); } }"
            )
            session.detach()
        except Exception:  # noqa: BLE001 - teardown must not raise
            pass
    if drawn != frames or spent <= 0:
        raise RuntimeError(
            "drew %r of %d frames in %r s of task time"
            % (drawn, frames, spent)
        )
    return spent / frames


# Rewrites the header clock on each of ``count`` animation frames and
# resolves with the number of frames that ran.
_WEB_FRAMES = (
    "(count) => new Promise((done) => {"
    " const clock = document.getElementById('clock'); let n = 0;"
    " const frame = () => { clock.textContent = 'frame ' + n;"
    " if (++n < count) requestAnimationFrame(frame);"
    " else requestAnimationFrame(() => done(n)); };"
    " requestAnimationFrame(frame); })"
)


@bench(
    "webui.select_move_500",
    "webui",
    detail="12 j keypresses down a 500-row table: row diff, lookup and "
    "scroll into view (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_select_move():
    """Keyboard row navigation, the cost per held-key repeat.

    Each j or k press re-signs the table, swaps the two rows whose
    highlight moved, finds the selected row and scrolls it into view,
    which forces a layout.  Driven through the page's keydown handler.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "renderRows")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _web_seed(n) + " __perf.renderRows(); window.scrollTo(0, 0);"
            " const _key = () => document.dispatchEvent("
            "new KeyboardEvent('keydown', { key: 'j', bubbles: true }));",
            "_s.selected = null; for (let k = 0; k < 12; k++) _key();",
            batch=1,
            batches=4,
            warm=1,
        )
        selected = page.evaluate(
            "() => [__perf.state().selected,"
            " document.querySelectorAll('#rows tr.sel').length]"
        )
    finally:
        page.evaluate(
            "() => { __perf.state().selected = null;"
            " __perf.renderRowsDiff(); window.scrollTo(0, 0); }"
        )
    # twelve presses from no selection land on the twelfth row by name
    twelfth = sorted("job%d" % i for i in range(n))[min(11, n - 1)]
    if selected != [twelfth, 1]:
        raise RuntimeError(
            "selection is %r, expected %r on one row" % (selected, twelfth)
        )
    return value


@bench(
    "webui.palette_keys_500",
    "webui",
    detail="20 command palette keystrokes over 500 jobs (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_palette_keys():
    """What one keystroke in the command palette costs.

    Every keystroke and arrow key rebuilds the command list (about six
    commands per job), scores each label, sorts, and renders the top 60.
    Driven through the palette's own input listener.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _WEB_RESET + " __perf.seedJobs(%d);" % n + " if (!_s.palette.open)"
            " document.getElementById('paletteBtn').click();"
            " const _in = document.getElementById('paletteInput');"
            " const _type = (v) => { _in.value = v;"
            " _in.dispatchEvent(new Event('input')); };",
            "for (let k = 0; k < 15; k++) { _type('job1'); _type('job12'); }",
            batch=1,
            batches=4,
            warm=1,
        )
        listed = page.evaluate(
            "() => { const input = document.getElementById('paletteInput');"
            " input.value = 'job1';"
            " input.dispatchEvent(new Event('input'));"
            " return [__perf.state().palette.open,"
            " document.querySelectorAll('#paletteList .item').length]; }"
        )
    finally:
        page.evaluate(
            "() => { if (__perf.state().palette.open)"
            " document.getElementById('paletteWrap').click(); }"
        )
    if listed[0] is not True or not listed[1]:
        raise RuntimeError(
            "palette open: %r, items listed: %r" % (listed[0], listed[1])
        )
    return value


@bench(
    "webui.timeline_500",
    "webui",
    detail="40 incident timeline rebuilds over 500 jobs, all runs and "
    "failures alternating (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_timeline():
    """The incident timeline's rebuild, which every poll repeats while
    the overlay is open.

    renderTimeline sorts each job's last run and writes one row per run,
    with no signature to skip an unchanged frame.  Driven through the
    overlay's failures-only checkbox, which calls it directly.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _WEB_RESET
            + " __perf.seedJobs(%d);" % n
            + " if (!_s.timeline.open) document.dispatchEvent("
            "new KeyboardEvent('keydown', { key: 'i', bubbles: true }));"
            " if (!_s.timeline.open) throw new Error('timeline is shut');"
            " const _only = document.getElementById('tlFailOnly');",
            "for (let k = 0; k < 40; k++) { _only.checked = !_only.checked;"
            " _only.dispatchEvent(new Event('change')); }",
            batch=1,
            batches=4,
            warm=1,
        )
        rows = page.evaluate(
            "() => { const only = document.getElementById('tlFailOnly');"
            " only.checked = false;"
            " only.dispatchEvent(new Event('change'));"
            " return document.querySelectorAll('#tlBody .tlrow').length; }"
        )
    finally:
        page.evaluate(
            "() => { const only = document.getElementById('tlFailOnly');"
            " if (only.checked) { only.checked = false;"
            " only.dispatchEvent(new Event('change')); }"
            " if (__perf.state().timeline.open)"
            " document.getElementById('tlClose').click(); }"
        )
    if rows != n:
        raise RuntimeError(
            "the timeline drew %r rows, expected %d" % (rows, n)
        )
    return value


@bench(
    "webui.ledger_analyze_500x600",
    "webui",
    detail="ledgerAnalyze full pass, 500 jobs x 600 saved runs "
    "(headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_ledger_analyze():
    """The run ledger's baseline pass over every job's full window.

    One median and one median absolute deviation per job over its
    successful durations, at the 600-run cap the page keeps per job.  It
    runs when the ledger loads or is enabled; a poll analyzes only the
    jobs that finished, which tests/test_web_perf_e2e.py pins.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "seedLedger", "ledgerAnalyze")
    jobs, runs = _n(500), _n(600, 20)
    try:
        value = _web_time(
            page,
            "__perf.seedLedger(%d, %d)" % (jobs, runs),
            "__perf.ledgerAnalyze()",
            batch=1,
            batches=4,
            warm=1,
        )
        stats = page.evaluate(
            "() => { const stats = __perf.state().ledger.stats;"
            " const names = Object.keys(stats);"
            " return [names.length, names.filter("
            "(name) => stats[name].median > 0).length]; }"
        )
    finally:
        page.evaluate(
            "() => { const ledger = __perf.state().ledger;"
            " ledger.on = false; ledger.runs = {}; ledger.stats = {};"
            " ledger.count = 0; }"
        )
    if stats != [jobs, jobs]:
        raise RuntimeError(
            "ledgerAnalyze left %r stats, %r with a median; expected %d"
            % (stats[0], stats[1], jobs)
        )
    return value


@bench(
    "webui.sort_500",
    "webui",
    detail="12 sort changes over 500 jobs, last run and name "
    "alternating (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_sort():
    """Changing the sort order of the jobs table.

    The sort itself is a fraction of a millisecond; the cost is the
    reorder, one node move per row that changes place, after every row
    is signed again.  Driven through the page's sort select.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "renderRows")
    n = _n(500)
    try:
        value = _web_time(
            page,
            _web_seed(n) + " __perf.renderRows();"
            " const _sel = document.getElementById('sortSel');"
            " const _sort = (k) => { _sel.value = k;"
            " _sel.dispatchEvent(new Event('change')); };",
            "for (let k = 0; k < 6; k++) { _sort('last'); _sort('name'); }",
            batch=1,
            batches=4,
            warm=1,
        )
        order = page.evaluate(
            "() => { const sel = document.getElementById('sortSel');"
            " const first = () => document.querySelector("
            "'#rows tr').getAttribute('data-job');"
            " sel.value = 'last'; sel.dispatchEvent(new Event('change'));"
            " const byLast = first();"
            " sel.value = 'name'; sel.dispatchEvent(new Event('change'));"
            " return [byLast, first()]; }"
        )
    finally:
        page.evaluate(
            "() => { const sel = document.getElementById('sortSel');"
            " sel.value = 'name'; sel.dispatchEvent(new Event('change')); }"
        )
    # seeded finish times fall one second per job, so the highest job
    # index finished first
    if order != ["job%d" % (n - 1), "job0"]:
        raise RuntimeError("sorted first rows are %r" % (order,))
    return value


@bench(
    "webui.logo_recovery",
    "webui",
    detail="pendulum logo: 4 s offline, then 150 frames of reconnect "
    "recovery, the last 26 planning, seeded (headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_logo_recovery():
    """The logo simulation's planner, the dearest frames the page runs.

    A reconnect swings the fallen mark back upright: each frame scores 80
    candidate plans of 112 integration steps and may verify a catch with a
    two-second rollout.  The page budgets that planning against the frame
    time, so the benchmark pins the budget off and the seed fixed, which
    makes every run integrate the same trajectory.  The planner engages
    124 frames after the reconnect, so the frame count is the workload and
    stays the same in every mode.  Uses the page's CronstableLogo.Sim
    class directly; no hook is involved.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    if not page.evaluate(
        "() => !!(window.CronstableLogo && window.CronstableLogo.Sim)"
    ):
        raise Skip("page exposes no CronstableLogo.Sim")
    frames = 150
    value = _web_time(
        page,
        "const _run = () => { const sim = new CronstableLogo.Sim(null,"
        " { seed: 1, planBudgetMs: 0 });"
        " for (let i = 0; i < 300; i++) sim.step(1 / 60);"
        " sim.setConnected(false);"
        " for (let i = 0; i < 240; i++) sim.step(1 / 60);"
        " sim.setConnected(true); let planned = 0;"
        " for (let i = 0; i < %d; i++) { sim.step(1 / 60);"
        % frames
        + " if (sim.plan) planned++; }"
        " window.__logoPlanned = [planned, sim.mode]; };",
        "_run()",
        batch=1,
        batches=4,
        warm=1,
    )
    planned = page.evaluate("() => window.__logoPlanned")
    if not planned or not planned[0]:
        raise RuntimeError(
            "the recovery planned on %r of %d frames; the workload did "
            "not reach the planner" % (planned and planned[0], frames)
        )
    return value


@bench(
    "webui.swim_400x49",
    "webui",
    detail="6 peer swimlane rebuilds, 400 snapshots x 49 peers "
    "(headless Chromium)",
    repeats=(2, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_web_swim():
    """The cluster swimlane, redrawn on every cluster poll while open.

    renderSwim rebuilds the whole SVG from the snapshot buffer: one lane
    per peer, each walked across all 400 snapshots with a per-snapshot
    peer lookup.  Needs the ``__perf.seedSwim`` and ``__perf.renderSwim``
    hooks; a release without them records as skipped, never failed.
    """
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    _web_guard(page, "state", "seedSwim", "renderSwim")
    snaps, peers = _n(400, 8), _n(49, 4)
    try:
        value = _web_time(
            page,
            "__perf.seedSwim(%d, %d)" % (snaps, peers),
            "for (let k = 0; k < 6; k++) __perf.renderSwim()",
            batch=1,
            batches=4,
            warm=1,
        )
        lanes = page.evaluate(
            "() => document.querySelectorAll('#swimPanel .lane-bg').length"
        )
    finally:
        page.evaluate("() => { __perf.seedSwim(0, 0); __perf.renderSwim(); }")
    if lanes != peers:
        raise RuntimeError(
            "the swimlane drew %r lanes, expected %d" % (lanes, peers)
        )
    return value


# ---------------------------------------------------------------------------
# loop / webapi: the daemon's request-serving surface, driven in-process
# through the real aiohttp handlers (make_mocked_request; no sockets).  The
# scheduler shares the event loop with every one of these code paths, which
# is why the loop group's stall gauge exists at all.
# ---------------------------------------------------------------------------


def _seeded_web_cron(n, history_every=0):
    """A Cron with ``n`` config jobs, web serving state set, the next-fire
    index pre-seeded with fixed instants, and (optionally) run history on
    every ``history_every``-th job.  All untimed fixture work.

    Seeding ``_next_fire`` is load-bearing twice over: an unseeded Cron's
    payload build takes the startup fallback (a per-job engine search --
    the wrong branch, with a wall-clock-dependent cost), and the seeded
    instants are in the fixed past so every ``scheduled_in`` clamps to
    exactly 0.0, keeping the payload bytes deterministic.
    """
    Cron = _cron_cls()
    try:
        cron = Cron(None, config_yaml=_config_yaml(n))
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    if not hasattr(cron, "_next_fire"):
        raise Skip("Cron._next_fire index not present")
    cron.web_config = {}
    for i, name in enumerate(cron.cron_jobs):
        cron._next_fire[name] = _NOW + timedelta(seconds=60 + (i % 3600))
    if history_every:
        try:
            from cronstable.cron import JobRunInfo
            from cronstable.job import JobOutputStream
        except ImportError as exc:
            raise Skip("run-history API unavailable: %r" % exc) from None
        for i, name in enumerate(list(cron.cron_jobs)):
            if i % history_every:
                continue
            try:
                for k in range(5):
                    started = _NOW - timedelta(minutes=k + 1)
                    info = JobRunInfo(
                        outcome="success" if k % 4 else "failure",
                        exit_code=0 if k % 4 else 1,
                        started_at=started,
                        finished_at=started + timedelta(seconds=12),
                        fail_reason=None if k % 4 else "exit 1",
                        output=JobOutputStream(),
                    )
                    cron.run_history[name].append(info)
                    cron.last_run[name] = info
            except TypeError as exc:
                raise Skip(
                    "JobRunInfo signature changed: %r" % exc
                ) from None
    return cron


def _mocked_get(path):
    try:
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    return make_mocked_request("GET", path)


@bench(
    "loop.stall_jobs_500",
    "loop",
    detail="max event-loop scheduling gap under 20 /jobs polls, 500 jobs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.003,
    info=True,
)
def bench_loop_stall_jobs():
    """MAX event-loop scheduling gap while /jobs requests are served.

    The one metric shape that can see work moving BACK onto the scheduler
    loop: such a move is timing-neutral for the work itself, so every
    workload-duration metric is structurally blind to it, and the class has
    shipped repeatedly (the trends stall, the redaction quadratic, prev()
    starvation, reporters inlined on the reaper, per-line loop writes).  A
    heartbeat sleeping 1ms records its worst wake-up lag while 20 /jobs
    polls run against a 500-job fleet; the >=200-job serialize offload keeps
    the measured gap small, and re-inlining it (measured 4.9x) is exactly
    what this gauge catches.

    One request is served untimed first: the offload's first call spawns the
    executor thread, and that one-time spawn otherwise pollutes the max.
    ``info=True`` for one release to observe CI variance, then arm at the
    declared 25% / 3ms floor.  Windows-local runs are blind (15ms timer
    granularity); the gate lives on ubuntu-latest.
    """
    import asyncio

    cron = fixture("loop_cron_500", lambda: _seeded_web_cron(_n(500)))
    if not hasattr(cron, "_web_list_jobs"):
        raise Skip("Cron._web_list_jobs not present")

    async def run():
        await cron._web_list_jobs(_mocked_get("/jobs"))  # executor spawn
        stop = False
        max_gap = 0.0

        async def heartbeat():
            nonlocal max_gap
            loop = asyncio.get_running_loop()
            last = loop.time()
            while not stop:
                await asyncio.sleep(0.001)
                now_t = loop.time()
                gap = now_t - last - 0.001
                if gap > max_gap:
                    max_gap = gap
                last = now_t

        async def poll():
            # Keep the loop actually serving builds (see
            # webapi.jobs_payload_500): the cross-poller response memo
            # primed by the untimed spawn call would otherwise serve all
            # 20 polls from cache and the heartbeat would gauge an idle
            # loop. The memo has two spellings across releases
            # (_jobs_response_memo since the scaffold,
            # _jobs_response_cache before it): clear whichever exists;
            # the plain write is the documented no-op on releases with
            # neither.
            memo = getattr(cron, "_jobs_response_memo", None)
            if memo is not None:
                memo.cached = None
            cron._jobs_response_cache = None
            await cron._web_list_jobs(_mocked_get("/jobs"))

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)  # let the heartbeat take its first timestamp
        await asyncio.gather(*(poll() for _ in range(20)))
        stop = True
        await beat
        return max_gap

    return asyncio.run(run())


def _idle_loop_config():
    """A small config FILE on disk, so the idle loop's per-pass reload takes
    the real stat-fingerprint path instead of the config_arg-is-None
    shortcut."""

    def build():
        path = os.path.join(_tmpdir(), "idle-loop")
        os.makedirs(path, exist_ok=True)
        entry = os.path.join(path, "cronstable.yaml")
        lines = ["jobs:"]
        for i in range(20):
            lines.append("  - name: idle%02d" % i)
            lines.append("    command: 'true'")
            lines.append('    schedule: "%d 4 * * *"' % (i % 60))
        lines.append("")
        with open(entry, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
        return entry

    return fixture("idle_loop_config", build)


@bench(
    "loop.idle_wake_rate",
    "loop",
    detail="500 idle Cron.run() iterations (full housekeeping pass each)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_loop_idle_wake():
    """The cost of ONE idle pass of the forever loop, which nothing else in
    the suite touches: no benchmark runs Cron.run() at all.

    Every other loop-adjacent metric measures a subsystem in isolation
    (_spawn_due_jobs, a handler, a gossip absorb).  What determines whether
    a daemon idles at 0% or pins a core is the pass ITSELF: the reload
    stat fingerprint, the four idempotent start_stop_* calls, the pause/SLA
    and durable-state periodics, _service_slots and the DAG service probe,
    multiplied by however often the sleep computation lets it run.  A wake
    hint that returns zero turns that pass into a spin, and the damage is
    exactly (pass cost x wake rate); this metric owns the first factor.

    The loop is spun through the seam its own docstring nominates: the
    module-level next_sleep_interval, which _sleep_interval calls for the
    housekeeping cap and which "a test can still patch to spin the loop
    fast".  Patching it (rather than the bound _sleep_interval) leaves the
    real floor/min arithmetic in the timed path.  The launch seam is checked
    positively and then neutered before the loop starts, so a rename cannot
    quietly turn this into a process-spawning benchmark, and the iteration
    count is asserted afterwards so a pass that exited early cannot time a
    no-op.
    """
    import asyncio

    Cron = _cron_cls()
    try:
        from cronstable import cron as cron_mod
    except ImportError as exc:
        raise Skip("cronstable.cron unavailable: %r" % exc) from None
    real_sleep = getattr(cron_mod, "next_sleep_interval", None)
    if real_sleep is None:
        raise Skip("cron.next_sleep_interval seam not present")
    for attr in ("run", "_sleep_interval", "signal_shutdown"):
        if not hasattr(Cron, attr):
            raise Skip("Cron lacks %s" % attr)
    seam = Cron.__dict__.get("_launch_plan")
    import inspect

    if seam is None or not inspect.iscoroutinefunction(seam):
        raise Skip(
            "Cron._launch_plan seam absent or not async; refusing to run "
            "(an un-neutered idle loop could spawn real processes)"
        )
    entry = _idle_loop_config()
    passes = _n(500, floor=5)
    state = {"n": 0, "t0": 0.0, "t1": 0.0}

    async def run():
        try:
            cron = Cron(entry)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None

        async def _capture(plan):
            state["launched"] = True

        cron._launch_plan = _capture

        def _spin(subminute=False, now=None):
            state["n"] += 1
            if state["n"] == 1:
                state["t0"] = time.perf_counter()
            elif state["n"] > passes or (
                # hard stop: a seam that stops being called must not hang CI
                time.perf_counter() - state["t0"] > 30.0
            ):
                if not state["t1"]:
                    state["t1"] = time.perf_counter()
                # signal_shutdown, not a bare _stop_event.set(): the reaper
                # parks on _jobs_running with no job running, and only the
                # public signal wakes it, so the shutdown drain would hang.
                cron.signal_shutdown()
            return 0.0

        cron_mod.next_sleep_interval = _spin
        try:
            # Belt to the in-seam brace: if _sleep_interval stops consulting
            # the module function at all, _spin never runs, the loop sleeps
            # out its real minute and this would hang the suite rather than
            # record a drifted seam.
            await asyncio.wait_for(cron.run(), timeout=120.0)
        except asyncio.TimeoutError:
            raise Skip(
                "the idle loop did not reach %d passes in 120s; the "
                "next_sleep_interval seam no longer drives it" % passes
            ) from None
        finally:
            cron_mod.next_sleep_interval = real_sleep
            await _teardown_cron(cron)
        return state["t1"] - state["t0"]

    dt = asyncio.run(run())
    if state["n"] <= passes:
        raise RuntimeError(
            "the idle loop ran %d of the expected %d passes; the spin seam "
            "did not hold and the region timed the wrong work"
            % (state["n"] - 1, passes)
        )
    if state.get("launched"):
        raise RuntimeError("an idle pass launched a job; the fixture is due")
    return dt


@bench(
    "loop.stall_metrics_2000",
    "loop",
    detail="max event-loop scheduling gap under 4 /metrics scrapes, 2k jobs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.003,
    info=True,
)
def bench_loop_stall_metrics():
    """MAX event-loop scheduling gap while /metrics is scraped.

    /metrics is the largest synchronous handler with no executor offload:
    the whole exposition (every family, every job, every histogram
    bucket) is rendered inline on the scheduler's loop, once every 15-60s
    forever, and the render is O(jobs x families x buckets).
    prometheus.render_500 measures the render's DURATION, which is blind to
    where it runs; this measures what the scheduler feels while it happens,
    the same gauge loop.stall_jobs_500 provides for the offloaded /jobs
    handler.  Two thousand jobs, because the gap only becomes legible past
    the point where the render exceeds a heartbeat tick.

    One scrape is served untimed first (the label-block and escape memos
    warm on it).  info=True to observe CI variance before arming, exactly
    as loop.stall_jobs_500 did; Windows-local runs are blind (15ms timer
    granularity).
    """
    import asyncio

    def build():
        cron = _seeded_web_cron(_n(2000))
        metrics = getattr(cron, "metrics", None)
        if metrics is None or not hasattr(metrics, "job_run_recorded"):
            raise Skip("PrometheusMetrics accumulators not present")
        try:
            for i, name in enumerate(cron.cron_jobs):
                metrics.job_run_recorded(name, "success", 1.5 + (i % 20))
                if i % 7 == 0:
                    metrics.job_run_recorded(name, "failure", 0.5)
        except TypeError as exc:
            raise Skip(
                "job_run_recorded signature changed: %r" % exc
            ) from None
        return cron

    cron = fixture("metrics_cron_2000", build)
    if not hasattr(cron, "_web_metrics"):
        raise Skip("Cron._web_metrics not present")

    async def run():
        await cron._web_metrics(_mocked_get("/metrics"))  # warm the memos
        stop = False
        max_gap = 0.0

        async def heartbeat():
            nonlocal max_gap
            loop = asyncio.get_running_loop()
            last = loop.time()
            while not stop:
                await asyncio.sleep(0.001)
                now_t = loop.time()
                gap = now_t - last - 0.001
                if gap > max_gap:
                    max_gap = gap
                last = now_t

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)  # let the heartbeat take its first timestamp
        for _ in range(4):
            # Keep the loop actually rendering: the cross-scraper response
            # memo primed by the untimed warm call would otherwise serve
            # all 4 scrapes from cache and the heartbeat would gauge an
            # idle loop. The memo has two spellings across releases
            # (_metrics_response_memo since the scaffold,
            # _metrics_response_cache before it): getattr both, so a
            # release with either (or neither) clears what it has and
            # changes nothing else.
            for memo in getattr(cron, "_metrics_response_memo", {}).values():
                memo.cached = None
            getattr(cron, "_metrics_response_cache", {}).clear()
            resp = await cron._web_metrics(_mocked_get("/metrics"))
            await asyncio.sleep(0)
        stop = True
        await beat
        if not resp.body:
            raise RuntimeError("/metrics returned an empty exposition")
        return max_gap

    return asyncio.run(run())


class _BenchFinishedJob:
    """A completion the reaper can wait on, with no process behind it.

    Quacks like a RunningJob exactly as far as _wait_for_running_jobs reads
    one: hashable identity, an awaitable wait(), and a config carrying the
    name its error path would log.  _handle_finished_job is neutered
    separately (it is the whole record/report/retry pipeline, measured by
    other metrics), so the timed region is the reaper's own bookkeeping.
    """

    class _Config:
        def __init__(self, name):
            self.name = name

    def __init__(self, name):
        self.config = self._Config(name)
        self._done = None

    def arm(self):
        import asyncio

        self._done = asyncio.get_running_loop().create_future()

    def finish(self):
        if not self._done.done():
            self._done.set_result(None)

    async def wait(self):
        await self._done


@bench(
    "loop.stall_completions_500",
    "loop",
    detail="reaper drain of 500 completions, one at a time (500 running)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_loop_stall_completions():
    """Measure reaper bookkeeping across staggered job completions.

    The fleet starts with all jobs running, then finishes them one at a
    time. Two event-loop turns between finishes let the reaper process
    completions as the running set shrinks.

    A stub completion handler records handled jobs. Durable writes,
    reports, and retries have separate metrics, so this measurement
    isolates the reaper's bookkeeping.
    """
    import asyncio
    import inspect

    Cron = _cron_cls()
    if not hasattr(Cron, "_wait_for_running_jobs"):
        raise Skip("Cron._wait_for_running_jobs not present")
    handler = Cron.__dict__.get("_handle_finished_job")
    if handler is None or not inspect.iscoroutinefunction(handler):
        raise Skip(
            "Cron._handle_finished_job seam absent or not async; refusing "
            "to run (the un-neutered reaper would take the durable path)"
        )
    n = _n(500, floor=5)

    async def run():
        try:
            cron = Cron(
                None,
                config_yaml="jobs:\n  - name: seed\n    command: 'x'\n"
                "    schedule: '0 0 * * *'\n",
            )
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        handled = []

        async def _neutered(job):
            handled.append(job)

        cron._handle_finished_job = _neutered
        jobs = [_BenchFinishedJob("bench%04d" % i) for i in range(n)]
        for job in jobs:
            job.arm()
            cron.running_jobs[job.config.name].append(job)
        cron._jobs_running.set()
        reaper = asyncio.create_task(cron._wait_for_running_jobs())
        await asyncio.sleep(0)
        t0 = time.perf_counter()
        for job in jobs:
            job.finish()
            cron.running_jobs.pop(job.config.name, None)
            # Give the reaper two event-loop turns between completions.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        # bounded: a wedged reaper must record as a broken benchmark, not
        # spin the loop until the CI job times out
        for _ in range(10 * n + 1000):
            if len(handled) >= n:
                break
            await asyncio.sleep(0)
        dt = time.perf_counter() - t0
        cron._stop_event.set()
        cron._jobs_running.set()
        try:
            await asyncio.wait_for(reaper, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            reaper.cancel()
        await _teardown_cron(cron)
        if len(handled) != n:
            raise RuntimeError(
                "the reaper handled %d of %d completions; the region timed "
                "the wrong work" % (len(handled), n)
            )
        return dt

    return asyncio.run(run())


@bench(
    "loop.stall_launch_herd_8k",
    "loop",
    detail="max event-loop gap while 8k due jobs launch (OS spawn stubbed)",
    repeats=(3, 2, 1),
    gate_floor=0.003,
    info=True,
)
def bench_loop_stall_launch_herd():
    """MAX event-loop gap while a herd of due jobs launches.

    _launch_concurrently gathers every due job, and each launch runs
    unsuspended until it reaches the spawn gate, so the first loop turn of a
    herd grows with the number of due jobs however few spawns the gate lets
    through.  The stubbed spawn suspends once, as a real one does, so the
    gate is exercised.  A heartbeat that reschedules itself every loop turn
    records the longest turn: that is how long a web request, a signal or a
    timer waits behind the herd.

    info: a gap gauge mostly measures the runner, as loop.stall_jobs_500
    notes.  The call_soon heartbeat reads the same on Windows, where a
    sleeping heartbeat only sees the 15 ms timer.
    """
    import asyncio

    n = _n(8000, floor=20)
    jobs = _herd_jobs(n)
    cron = _herd_cron()
    cron.cron_jobs = jobs
    plan = [(job, [_NOW]) for job in jobs.values()]
    counts = {"spawned": 0}
    beat = {"last": 0.0, "gap": 0.0, "stop": False}

    async def run():
        loop = asyncio.get_running_loop()

        def heartbeat():
            now = time.perf_counter()
            if now - beat["last"] > beat["gap"]:
                beat["gap"] = now - beat["last"]
            beat["last"] = now
            if not beat["stop"]:
                loop.call_soon(heartbeat)

        reaper = asyncio.create_task(cron._wait_for_running_jobs())
        await asyncio.sleep(0)
        beat["last"] = time.perf_counter()
        loop.call_soon(heartbeat)
        try:
            await _herd_cycle(cron, plan, counts, yielding=True)
        finally:
            beat["stop"] = True
            await _herd_stop(cron, reaper)

    asyncio.run(run())
    _herd_check(cron, counts, n, "success")
    return beat["gap"]


@bench(
    "loop.sla_pass_5k_x10",
    "loop",
    detail="10 _sla_periodic passes over 5k jobs with three sla checks each",
    repeats=(3, 2, 1),
    gate_pct=15.0,
    gate_floor=0.005,
)
def bench_loop_sla_pass():
    """The per-minute SLA evaluation at fleet scale.

    _sla_periodic runs inline on the scheduler loop once per housekeeping
    minute and visits every job with an sla block: the pause and ownership
    excusals, the three observations, and the late gauges.  It is linear in
    the SLA jobs, so the pass is repeated ten times over 5k jobs to make
    the per-job cost legible.  Nothing breaches (no reporter task is
    spawned), which is the steady state the pass is in almost always.
    """
    import asyncio

    Cron = _cron_cls()
    for attr in ("_sla_periodic", "_sla_observations"):
        if not hasattr(Cron, attr):
            raise Skip("Cron.%s not present" % attr)
    n = _n(5000, floor=20)
    passes = 10

    def build():
        try:
            from cronstable.config import (
                DEFAULT_CONFIG,
                JobConfig,
                mergedicts,
            )
        except ImportError as exc:
            raise Skip("cronstable.config API unavailable: %r" % exc) from None
        jobs = {}
        for i in range(n):
            name = "sla%05d" % i
            try:
                jobs[name] = JobConfig(
                    mergedicts(
                        DEFAULT_CONFIG,
                        {
                            "name": name,
                            "command": "true",
                            "schedule": "%d %d * * *" % (i % 60, (i * 7) % 24),
                            "sla": {
                                "maxTimeSinceSuccessSeconds": 86400,
                                "lateAfterSeconds": 600,
                                "maxRuntimeSeconds": 3600,
                            },
                        },
                    )
                )
            except Exception as exc:
                raise Skip("sla job config rejected: %r" % exc) from None
        return jobs

    jobs = fixture("sla_jobs_%d" % n, build)
    try:
        cron = Cron(None, config_yaml=_HERD_SEED_YAML)
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    cron.cron_jobs = jobs
    cron._sla_jobs_cache = None

    measured = []
    observe = cron._sla_observations

    def counting(name, job, now):
        measured.append(name)
        return observe(name, job, now)

    async def run():
        # The first pass is untimed: it sees every job for the first time,
        # and it is the one pass that counts the jobs it measures.
        cron._sla_observations = counting
        cron._sla_periodic()
        del cron._sla_observations
        t0 = time.perf_counter()
        for _ in range(passes):
            cron._sla_periodic()
        dt = time.perf_counter() - t0
        tasks = getattr(cron, "_completion_tasks", ())
        if tasks or getattr(cron, "_sla_state", None):
            raise RuntimeError("an sla check breached; the fixture is stale")
        await _teardown_cron(cron)
        return dt

    dt = asyncio.run(run())
    if len(measured) != n:
        raise RuntimeError(
            "a pass measured %d of %d sla jobs" % (len(measured), n)
        )
    return dt


# ---------------------------------------------------------------------------
# loop: what the scheduler feels while the MCP listing is served.
# ---------------------------------------------------------------------------
@bench(
    "loop.stall_mcp_jobs_500",
    "loop",
    detail="max loop gap under 20 MCP cron_list_jobs calls, a 500-row page",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.003,
    info=True,
)
def bench_loop_stall_mcp_jobs():
    """MAX event-loop scheduling gap while cron_list_jobs calls are served.

    The MCP listing builds and serializes its page on the scheduler's
    loop, where GET /jobs hands its serialization to the executor, so the
    gap here is one whole call.  The handler's maxRows is 500, which
    makes one page the 500 jobs loop.stall_jobs_500 serves over REST and
    the two gauges comparable.  mcp.list_jobs_500 gives a call's
    duration; this gives what job dispatch waits behind, and it is the
    number an executor offload of the MCP path would move.  The calls run
    one after another with a yield between them, as separate requests
    arrive, so the heartbeat reads a single call's block.  Windows-local
    runs are blind (15ms timer granularity).
    """
    import asyncio

    cron = fixture("loop_cron_500", lambda: _seeded_web_cron(_n(500)))
    try:
        from cronstable.mcp import MCPHandler
    except ImportError as exc:
        raise Skip("cronstable.mcp unavailable: %r" % exc) from None

    def build():
        try:
            handler = MCPHandler(
                cron,
                {
                    "readOnly": True,
                    "toolsets": ["observe"],
                    "maxRows": 500,
                    "maxBodyBytes": 1048576,
                    "allowedOrigins": [],
                },
            )
        except (TypeError, KeyError) as exc:
            raise Skip("MCPHandler construction changed: %r" % exc) from None
        if not hasattr(handler, "handle_http"):
            raise Skip("MCPHandler.handle_http not present")
        return handler

    handler = fixture("loop_mcp_handler_500", build)
    _mcp_require_tool(handler, "cron_list_jobs")
    jobs = len(cron.cron_jobs)

    async def run():
        request = _mcp_post(_mcp_call("cron_list_jobs"))
        first = _mcp_structured((await handler.handle_http(request)).body)
        if len(first["jobs"]) != jobs:
            raise RuntimeError(
                "cron_list_jobs returned %d of %d rows; not the whole job "
                "set" % (len(first["jobs"]), jobs)
            )
        stop = False
        max_gap = 0.0

        async def heartbeat():
            nonlocal max_gap
            loop = asyncio.get_running_loop()
            last = loop.time()
            while not stop:
                await asyncio.sleep(0.001)
                now_t = loop.time()
                gap = now_t - last - 0.001
                if gap > max_gap:
                    max_gap = gap
                last = now_t

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)  # let the heartbeat take its first timestamp
        for _ in range(20):
            await handler.handle_http(request)
            await asyncio.sleep(0)
        stop = True
        await beat
        return max_gap

    return asyncio.run(run())


@bench(
    "webapi.jobs_payload_500",
    "webapi",
    detail="GET /jobs handler end to end x20, 500 jobs with history",
    repeats=(3, 2, 1),
)
def bench_webapi_jobs_payload():
    """The server-side /jobs cost at fleet scale, previously untimed.

    jobs_payload + per-job _job_to_dict/_scheduled_in + the content-hash
    ETag + the JSON encode + the >=200-job executor hop, driven through the
    real aiohttp handler.  Paid per dashboard poll, per TUI poll, per MCP
    list, on the loop the scheduler shares; the webui group drives Chromium
    against a FAKE backend and never touches any of it.  Run history is
    seeded on every 5th job so the last-run/history slices do real work
    instead of hitting their empty-fleet no-ops.
    """
    import asyncio

    cron = fixture(
        "webapi_cron_500",
        lambda: _seeded_web_cron(_n(500), history_every=5),
    )
    if not hasattr(cron, "_web_list_jobs"):
        raise Skip("Cron._web_list_jobs not present")

    async def run():
        await cron._web_list_jobs(_mocked_get("/jobs"))  # executor spawn
        request = _mocked_get("/jobs")
        t0 = time.perf_counter()
        for _ in range(20):
            # Keep measuring the BUILD: the cross-poller response memo
            # would otherwise serve 19 of these 20 straight from cache and
            # the metric would stop gating the payload/encode cost its id
            # promises. The memo has two spellings across releases
            # (_jobs_response_memo since the scaffold,
            # _jobs_response_cache before it): clear whichever exists;
            # the plain write is the documented no-op on releases with
            # neither.
            memo = getattr(cron, "_jobs_response_memo", None)
            if memo is not None:
                memo.cached = None
            cron._jobs_response_cache = None
            await cron._web_list_jobs(request)
        return time.perf_counter() - t0

    return asyncio.run(run())


@bench(
    "webapi.jobs_bytes_500",
    "webapi",
    detail="GET /jobs response body size, 500 jobs with history",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_jobs_bytes():
    """The suite's first non-time metric: the SIZE of the /jobs body.

    Guards field creep in _job_to_dict, which no timing metric can see: the
    payload build is under a millisecond, so a 10% byte increase (a new
    field on every job of every poll, paid again by every dashboard and TUI
    client forever) moves the timing metrics by low single digits and gates
    nothing.  The fixture's seeded past instants clamp every scheduled_in
    to 0.0, so the byte count is deterministic; the floor is 1 KB (the unit
    here is KB, not seconds).
    """
    import asyncio

    cron = fixture(
        "webapi_cron_500",
        lambda: _seeded_web_cron(_n(500), history_every=5),
    )
    if not hasattr(cron, "_web_list_jobs"):
        raise Skip("Cron._web_list_jobs not present")

    async def run():
        resp = await cron._web_list_jobs(_mocked_get("/jobs"))
        body = resp.body
        if not body or len(body) < 2:
            raise RuntimeError("GET /jobs returned an empty body")
        return len(body) / 1024.0

    return asyncio.run(run())


@bench(
    "webapi.jobs_gzip_500",
    "webapi",
    detail="GET /jobs body size after gzip, 500 jobs with history",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=0.5,
)
def bench_webapi_jobs_gzip():
    """The same body as webapi.jobs_bytes_500, over the wire.

    The raw byte count answers "how much did the payload grow"; this one
    answers "how much of that reaches the client", and the two move
    independently.  A new per-job field whose value repeats across the fleet
    (a constant, an enum, another copy of a label already present) is nearly
    free once compressed, while a field carrying per-job entropy (an id, a
    timestamp, a hash) costs its full weight on every poll of every client
    forever, and only this metric can tell those two apart.  It is also
    the number a response-compression change would move, in either
    direction.

    Compression is done here rather than read off the response: the daemon
    does not compress today, so reading a Content-Encoding would make the
    metric a permanent no-op.  Level 6 is zlib's default and what every
    server default (aiohttp, nginx) lands on.
    """
    import asyncio
    import gzip

    cron = fixture(
        "webapi_cron_500",
        lambda: _seeded_web_cron(_n(500), history_every=5),
    )
    if not hasattr(cron, "_web_list_jobs"):
        raise Skip("Cron._web_list_jobs not present")

    async def run():
        resp = await cron._web_list_jobs(_mocked_get("/jobs"))
        body = resp.body
        if not body or len(body) < 2:
            raise RuntimeError("GET /jobs returned an empty body")
        # mtime=0: the gzip header otherwise carries a timestamp, which
        # would make the byte count differ run to run.
        packed = gzip.compress(bytes(body), compresslevel=6, mtime=0)
        return len(packed) / 1024.0

    return asyncio.run(run())


@bench(
    "webapi.gzip_body_500",
    "webapi",
    detail="the daemon's response gzip x1000 on the 500-job /jobs body",
    repeats=(3, 2, 1),
)
def bench_webapi_gzip_body():
    """The CPU the daemon spends gzipping a poll response.

    Every memoized /jobs, /fleet and /metrics build pays this once per memo
    window, on the loop or its executor hop.  webapi.jobs_payload_500 folds
    it into a larger total; this one isolates the compressor, which is the
    number a gzip backend change (cronstable._gzip) moves.
    """
    import asyncio

    import cronstable.cron

    gzip_body = getattr(cronstable.cron, "_gzip_body", None)
    if gzip_body is None:
        raise Skip("cron._gzip_body not present")
    cron = fixture(
        "webapi_cron_500",
        lambda: _seeded_web_cron(_n(500), history_every=5),
    )
    if not hasattr(cron, "_web_list_jobs"):
        raise Skip("Cron._web_list_jobs not present")
    resp = asyncio.run(cron._web_list_jobs(_mocked_get("/jobs")))
    body = bytes(resp.body)
    reps = _n(1000)
    gzip_body(body)  # resolve the backend outside the timed region
    t0 = time.perf_counter()
    for _ in range(reps):
        gzip_body(body)
    return time.perf_counter() - t0


class _NullStreamResponse:
    """A StreamResponse that swallows what is written to it.

    The SSE framing metric is about the per-line CPU the daemon spends
    before the bytes reach the socket; a real transport would add a
    scheduler-dependent write cost and make the number a network
    measurement.  ``write`` still returns an awaitable, so the framing
    coroutine takes exactly the path it takes in production.
    """

    def __init__(self):
        self.written = 0

    async def write(self, data):
        self.written += len(data)


@bench(
    "webapi.sse_burst_20k",
    "webapi",
    detail="SSE burst framing, 4 subscribers x 20k lines, 32-line bursts",
    repeats=(3, 2, 1),
    # ~40ms of genuinely per-line work: the 10ms default floor would set the
    # real sensitivity at ~25% against a declared 15%.  Same call as
    # dag.finish_fanin_1k and tui.drawer_paint_5k, and cheaper than inflating
    # the subscriber count past anything a real tail sees.
    gate_floor=0.005,
)
def bench_webapi_sse_burst():
    """The live log tail's per-line, per-subscriber cost.

    Successor to ``webapi.sse_frame_20k``.  Delivery used to be one framed
    ``resp.write`` per line through ``_sse_send_line``; the live loop now
    drains each wake's burst and writes the joined frames once, and the old
    per-line seam is gone.  The workload changed with the code, so the id
    changed with it (the harness rule for rescales): the old number meant
    one write per line and this one does not.  What stays per line PER
    attached client is the ``_sse_frame`` build: every captured line of
    every tailed run is JSON-encoded into an ``event: line`` frame once per
    dashboard, on the scheduler's loop, so its cost is paid by every job
    waiting to fire while somebody watches a chatty run.
    job.stream_capture_120k measures the capture leg and stops at the ring
    buffer; this metric owns the leg past it.

    Bursts are a fixed 32 lines so the workload is deterministic: real
    burst size is whatever piled up behind one queue wake, which is a
    producer-rate fact the harness must not model with a clock.  32 keeps
    the join-and-write amortization visible without hiding the per-line
    frame builds that dominate.

    The line mix is the capture fixture's: mostly ASCII, a wide-glyph line
    every sixteenth, so the encoder's ASCII fast path is exercised without
    being the only thing measured.  The written byte count is asserted so a
    framing function that quietly stopped writing cannot time a no-op.

    Four subscribers, not one: the cost is per line PER attached client, so
    a fan-out is the honest shape, and one pass of 20k lines measures under
    the harness's 50ms rule on CI (which would leave the metric floor-bound
    at an effective ~50% against its declared 15%).
    """
    import asyncio

    try:
        from cronstable.cron import _sse_frame
    except ImportError as exc:
        raise Skip("cron._sse_frame unavailable: %r" % exc) from None
    try:
        probe = _sse_frame("stdout", "probe\n")
    except TypeError as exc:
        raise Skip("_sse_frame signature changed: %r" % exc) from None
    if not isinstance(probe, bytes):
        raise Skip("_sse_frame no longer returns bytes")
    n = _n(20000)
    lines = fixture(
        "sse_lines_20k",
        lambda: [
            (
                "wide 进度 %d%% done\n" % (i % 100)
                if i % 16 == 15
                else "2026-07-18 12:00:%02d INFO worker %d processed batch\n"
                % (i % 60, i)
            )
            for i in range(n)
        ],
    )

    subscribers = 4
    burst = 32

    async def run():
        resp = _NullStreamResponse()
        t0 = time.perf_counter()
        for _ in range(subscribers):
            for start in range(0, n, burst):
                frames = []
                for i in range(start, min(start + burst, n)):
                    stream = "stderr" if i % 5 == 0 else "stdout"
                    frames.append(_sse_frame(stream, lines[i]))
                await resp.write(b"".join(frames))
        dt = time.perf_counter() - t0
        if resp.written < n * subscribers * 20:
            raise RuntimeError(
                "SSE framing wrote %d bytes for %d lines; the region did "
                "not frame the stream" % (resp.written, n * subscribers)
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.auth_scope_20k",
    "webapi",
    detail="bearer auth middleware x20k: 8-token table + scope check",
)
def bench_webapi_auth_scope():
    """The per-request auth tax: constant-time compare over the WHOLE token
    table (no early return, by design) plus per-route scope resolution, on
    every request forever.  Value honestly capped -- tens of microseconds in
    a single-digit-RPS daemon -- but it is the sole per-request gate on this
    path, taken because the webapi group exists anyway."""
    import asyncio

    Cron = _cron_cls()
    try:
        from cronstable import cron as cron_mod
    except ImportError as exc:
        raise Skip("cronstable.cron unavailable: %r" % exc) from None
    web_token = getattr(cron_mod, "_WebToken", None)
    eff_scopes = getattr(cron_mod, "_effective_web_scopes", None)
    if web_token is None or eff_scopes is None:
        raise Skip("web token internals not present")
    if not hasattr(Cron, "_make_auth_middleware"):
        raise Skip("Cron._make_auth_middleware not present")
    n = _n(20000)
    # 8 scoped tokens; the presented one is LAST so a broken constant-time
    # loop that early-returns would still match, but the fixture shape stays
    # the worst (and only) case: every request walks the whole table.
    tokens = [
        web_token(
            b"bench-token-%d" % i,
            eff_scopes(["view"] if i % 2 else ["control"]),
            "t%d" % i,
        )
        for i in range(8)
    ]
    middleware = Cron._make_auth_middleware(tokens)

    async def handler(request):
        return None

    async def run():
        try:
            from aiohttp.test_utils import make_mocked_request
        except ImportError as exc:
            raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
        request = make_mocked_request(
            "GET",
            "/jobs",
            headers={"Authorization": "Bearer bench-token-7"},
        )
        await middleware(request, handler)  # warm; also fail fast on a 401
        t0 = time.perf_counter()
        for _ in range(n):
            await middleware(request, handler)
        return time.perf_counter() - t0

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# webapi (continued): the polled endpoints beyond /jobs, the live log tail,
# and the dashboard page's weight.
# ---------------------------------------------------------------------------
def _httpapi_history_cron(n, runs):
    """``_seeded_web_cron(n)`` with ``runs`` retained runs on every job.

    Fixed instants, so the payloads built from the history are byte
    stable.  One shared output stream: the history endpoints never read
    it, and a ring per row would be most of the fixture.
    """
    cron = _seeded_web_cron(n)
    try:
        from cronstable.cron import JobRunInfo
        from cronstable.job import JobOutputStream
    except ImportError as exc:
        raise Skip("run-history API unavailable: %r" % exc) from None
    output = JobOutputStream()
    try:
        for i, name in enumerate(cron.cron_jobs):
            info = None
            for k in range(runs):
                started = _NOW - timedelta(
                    minutes=5 * (runs - k), seconds=i % 60
                )
                failed = (i + k) % 7 == 0
                info = JobRunInfo(
                    outcome="failure" if failed else "success",
                    exit_code=1 if failed else 0,
                    started_at=started,
                    finished_at=started + timedelta(seconds=12 + k % 30),
                    fail_reason="exit 1" if failed else None,
                    output=output,
                )
                cron.run_history[name].append(info)
            if info is not None:
                cron.last_run[name] = info
    except TypeError as exc:
        raise Skip("JobRunInfo signature changed: %r" % exc) from None
    return cron


def _httpapi_activity_cron():
    return fixture(
        "httpapi_activity_cron_500",
        lambda: _httpapi_history_cron(_n(500, floor=5), 50),
    )


def _httpapi_activity_jobs(body, what):
    """The ``jobs`` object of an /activity body, checked for content."""
    jobs = json.loads(bytes(body)).get("jobs")
    if not jobs or not all(jobs.values()):
        raise RuntimeError(
            "%s carried no runs for some job; the fixture did not seed and "
            "the metric would describe an empty feed" % what
        )
    return jobs


@bench(
    "webapi.activity_build_500",
    "webapi",
    detail="GET /activity handler x3 uncached, 500 jobs x 50 retained runs",
    repeats=(3, 2, 1),
)
def bench_webapi_activity_build():
    """The heatmap feed's full build: every job's retained runs projected
    to three fields, then serialized, hashed and gzipped.

    Each open heatmap asks once a minute and every recorded run busts the
    shared product, so in a busy fleet each request is a build.  It guards
    the per-row projection (two isoformat calls and a dict per run), which
    webapi.jobs_payload_500 never reaches: /jobs ships a 20-entry inline
    tail of two fields per job.
    """
    import asyncio

    cron = _httpapi_activity_cron()
    if not hasattr(cron, "_web_get_activity"):
        raise Skip("Cron._web_get_activity not present")
    memo = getattr(cron, "_activity_response_memo", None)
    if memo is None or not hasattr(memo, "cached"):
        raise Skip("Cron._activity_response_memo not present")

    async def run():
        request = _mocked_get("/activity")
        # the executor's first call spawns its thread
        await cron._web_get_activity(request)
        t0 = time.perf_counter()
        for _ in range(3):
            # keep measuring the build: the shared product would otherwise
            # serve the later requests from the first one's bytes
            memo.cached = None
            resp = await cron._web_get_activity(request)
        dt = time.perf_counter() - t0
        jobs = _httpapi_activity_jobs(resp.body, "GET /activity")
        if len(jobs) != len(cron.cron_jobs):
            raise RuntimeError(
                "GET /activity carried %d of %d jobs"
                % (len(jobs), len(cron.cron_jobs))
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.activity_bytes_500",
    "webapi",
    detail="GET /activity body size, 500 jobs x 50 retained runs",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_activity_bytes():
    """The size of the default /activity body, which carries every job.

    It grows with jobs times retained runs and both dashboards parse all
    of it on each refresh, so a field added to the row, or a longer
    timestamp spelling, costs its weight 25,000 times at this scale and
    moves no timing metric.
    """
    import asyncio

    cron = _httpapi_activity_cron()
    if not hasattr(cron, "_web_get_activity"):
        raise Skip("Cron._web_get_activity not present")

    async def run():
        resp = await cron._web_get_activity(_mocked_get("/activity"))
        _httpapi_activity_jobs(resp.body, "GET /activity")
        return len(resp.body) / 1024.0

    return asyncio.run(run())


@bench(
    "webapi.activity_capped_bytes_500",
    "webapi",
    detail="GET /activity?jobs=80 body size, 500 jobs x 50 retained runs",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_activity_capped_bytes():
    """The size of the response the dashboards ask for: 80 jobs of 500.

    Both dashboards draw at most 80 heatmap rows and send that cap, so
    this is the body a large fleet transfers per refresh, and the size a
    wider row or a leaking cap moves.  A release whose /activity has no
    `jobs` parameter answers with every job, and the metric skips there.
    """
    import asyncio

    cron = _httpapi_activity_cron()
    if not hasattr(cron, "_web_get_activity"):
        raise Skip("Cron._web_get_activity not present")
    total = len(cron.cron_jobs)
    cap = _n(80)
    if cap >= total:
        raise RuntimeError(
            "the cap (%d) does not bite on a %d-job fixture" % (cap, total)
        )

    async def run():
        resp = await cron._web_get_activity(
            _mocked_get("/activity?jobs=%d" % cap)
        )
        jobs = _httpapi_activity_jobs(resp.body, "GET /activity?jobs=")
        if len(jobs) == total and not hasattr(cron, "_activity_job_names"):
            raise Skip("GET /activity has no jobs cap")
        if len(jobs) != cap:
            raise RuntimeError(
                "GET /activity?jobs=%d carried %d jobs" % (cap, len(jobs))
            )
        return len(resp.body) / 1024.0

    return asyncio.run(run())


_HTTPAPI_POOLS = 4


def _httpapi_pools_yaml(path, jobs_per_pool):
    lines = ["state:", "  path: %s" % path.replace("\\", "/"), "pools:"]
    for p in range(_HTTPAPI_POOLS):
        lines.append("  pool%d:" % p)
        lines.append("    slots: 4")
    lines.append("jobs:")
    for p in range(_HTTPAPI_POOLS):
        for j in range(jobs_per_pool):
            lines.append("  - name: p%d-job%03d" % (p, j))
            lines.append("    command: echo p%d-job%03d" % (p, j))
            lines.append(
                '    schedule: "%d %d * * *"' % (j % 60, (j * 7) % 24)
            )
            lines.append("    pool: pool%d" % p)
    lines.append("")
    return "\n".join(lines)


def _httpapi_pools_store():
    """(config yaml, queued entries per pool): a store whose four pool
    documents each hold a backlog, written through ``enqueue_job``."""

    def build():
        import asyncio

        Cron = _cron_cls()
        path = os.path.join(_tmpdir(), "httpapi-pools")
        os.makedirs(path, exist_ok=True)
        jobs_per_pool = _n(40, floor=2)
        per_pool = _n(130, floor=3)
        cfg = _httpapi_pools_yaml(path, jobs_per_pool)

        async def seed():
            try:
                cron = Cron(None, config_yaml=cfg)
            except TypeError as exc:
                raise Skip("Cron signature changed: %r" % exc) from None
            pools = getattr(cron, "_pools", None)
            if pools is None or not hasattr(pools, "enqueue_job"):
                raise Skip("resource pools not present")
            # the queue service would admit and launch what is enqueued
            pools.service = lambda: None
            backend = _state_backend(path)
            await backend.start()
            cron.state_backend = backend
            cron._state_configured = True
            try:
                names = list(cron.cron_jobs)
                for p in range(_HTTPAPI_POOLS):
                    mine = names[p * jobs_per_pool : (p + 1) * jobs_per_pool]
                    for e in range(per_pool):
                        await pools.enqueue_job(
                            cron.cron_jobs[mine[e % len(mine)]],
                            manual=True,
                            key="p%d-e%04d" % (p, e),
                        )
            except TypeError as exc:
                raise Skip("enqueue_job signature changed: %r" % exc) from None
            finally:
                await _teardown_cron(cron)

        asyncio.run(seed())
        return path, cfg, per_pool

    return fixture("httpapi_pools_store", build)


@bench(
    "webapi.pools_poll_4x130",
    "webapi",
    detail="10 x (GET /pools + uncached GET /jobs), 4 pools x 130 queued",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_webapi_pools_poll():
    """One dashboard poll's pool work: the /pools snapshot, and the same
    snapshot again inside the /jobs build that attaches each job's queue.

    A snapshot reads every pool document under its lock, deep-copies it,
    runs queue maintenance and sorts the entries.  Neither /pools nor the
    /jobs build caches it, so the cost recurs on every poll and grows with
    the backlog.  No other benchmark configures a pool:
    webapi.jobs_payload_500 returns from the queue attachment on its first
    line.
    """
    import asyncio

    Cron = _cron_cls()
    path, cfg, per_pool = _httpapi_pools_store()
    queued = per_pool * _HTTPAPI_POOLS

    async def run():
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        memo = getattr(cron, "_jobs_response_memo", None)
        if not hasattr(cron, "_web_pools") or memo is None:
            raise Skip("Cron._web_pools or the /jobs memo not present")
        cron.web_config = {}
        for i, name in enumerate(cron.cron_jobs):
            cron._next_fire[name] = _NOW + timedelta(seconds=60 + i)
        cron._pools.service = lambda: None
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        pools_req = _mocked_get("/pools")
        jobs_req = _mocked_get("/jobs")
        try:
            # the store's worker threads start on the first call
            await cron._web_pools(pools_req)
            t0 = time.perf_counter()
            for _ in range(_n(10)):
                pools_resp = await cron._web_pools(pools_req)
                memo.cached = None
                jobs_resp = await cron._web_list_jobs(jobs_req)
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        listed = json.loads(bytes(pools_resp.body))
        if sum(pool.get("queued", 0) for pool in listed) != queued:
            raise RuntimeError(
                "GET /pools listed %r queued entries, expected %d"
                % ([pool.get("queued") for pool in listed], queued)
            )
        attached = sum(
            len((job.get("pool") or {}).get("queued") or ())
            for job in json.loads(bytes(jobs_resp.body))
        )
        if attached != queued:
            raise RuntimeError(
                "GET /jobs attached %d queued entries, expected %d"
                % (attached, queued)
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.sse_tail_pump_4x20k",
    "webapi",
    detail="live log tail, 4 subscribers x 20k lines in 32-line bursts",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_webapi_sse_tail_pump():
    """The live tail as the daemon runs it: publish, one queue hop per
    subscriber, the wake, the burst drain, the frame build and the write.

    webapi.sse_burst_20k times the frame build and the joined write in a
    loop of its own, so the tail's wait for each wake, its queue drain and
    the publish fan-out run in no other metric.  They run per line per
    open log tab on the scheduler's loop, and they are the larger share of
    the cost.

    The producer yields once per 32-line burst, so every subscriber sees
    the same bursts on every run.  The written bytes and the drop counter
    are asserted, so a tail that stops delivering cannot time a no-op.
    """
    import asyncio

    Cron = _cron_cls()
    if not hasattr(Cron, "_pump_output"):
        raise Skip("Cron._pump_output not present")
    try:
        from aiohttp.test_utils import make_mocked_request

        from cronstable.job import JobOutputStream
    except ImportError as exc:
        raise Skip("live-tail API unavailable: %r" % exc) from None
    n = _n(20000, floor=64)
    lines = fixture(
        "httpapi_tail_lines_20k",
        lambda: [
            (
                "wide 进度 %d%% done\n" % (i % 100)
                if i % 16 == 15
                else "2026-07-18 12:00:%02d INFO worker %d processed batch\n"
                % (i % 60, i)
            )
            for i in range(n)
        ],
    )
    cron = fixture("httpapi_tail_cron", lambda: _seeded_web_cron(1))
    subscribers = 4
    burst = 32

    async def run():
        output = JobOutputStream()
        request = make_mocked_request("GET", "/jobs/job00000/logs")
        sinks = [_NullStreamResponse() for _ in range(subscribers)]
        try:
            tails = [
                asyncio.ensure_future(cron._pump_output(request, sink, output))
                for sink in sinks
            ]
        except TypeError as exc:
            raise Skip("_pump_output signature changed: %r" % exc) from None
        await asyncio.sleep(0)  # every tail subscribes before the first line
        t0 = time.perf_counter()
        for start in range(0, n, burst):
            for i in range(start, min(start + burst, n)):
                output.publish("stderr" if i % 5 == 0 else "stdout", lines[i])
            await asyncio.sleep(0)
        output.close()
        await asyncio.wait_for(asyncio.gather(*tails), timeout=30)
        dt = time.perf_counter() - t0
        short = [sink.written for sink in sinks if sink.written < n * 20]
        if short or getattr(output, "dropped", 0):
            raise RuntimeError(
                "the tail delivered %r bytes to its short subscribers and "
                "dropped %r lines; the region did not pump the stream"
                % (short, getattr(output, "dropped", None))
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.trends_build_5k",
    "webapi",
    detail="trends parse + window aggregation x10 over 5k ledger records",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_webapi_trends_build():
    """The CPU half of GET /jobs/{name}/trends on a cache miss: rebuild a
    run row from every ledger record, bucket the rows into the trend
    windows, and fold each window's statistics.

    The ledger read in front of it is state.list_records_2k's subject.
    This part runs on an executor for up to 5,000 records per miss; its
    cost per record is what moves when the row parser or the statistics
    fold changes.
    """
    Cron = _cron_cls()
    if not hasattr(Cron, "_job_trends_build"):
        raise Skip("Cron._job_trends_build not present")
    try:
        from cronstable import cron as cron_mod
        from cronstable.cron import JobRunInfo
        from cronstable.job import JobOutputStream
    except ImportError as exc:
        raise Skip("run-history API unavailable: %r" % exc) from None
    if not hasattr(cron_mod, "get_now"):
        raise Skip("cron.get_now not present")
    n = _n(5000, floor=50)

    def build():
        output = JobOutputStream()
        records = []
        try:
            # newest first, ten minutes apart: every window has members
            for i in range(n):
                started = _NOW - timedelta(minutes=10 * i + 1)
                failed = i % 9 == 0
                info = JobRunInfo(
                    outcome="failure" if failed else "success",
                    exit_code=1 if failed else 0,
                    started_at=started,
                    finished_at=started + timedelta(seconds=12 + i % 30),
                    fail_reason="exit 1" if failed else None,
                    output=output,
                )
                records.append(info.to_dict(include_series=True))
        except TypeError as exc:
            raise Skip("JobRunInfo signature changed: %r" % exc) from None
        return records

    records = fixture("httpapi_trends_records_5k", build)
    cron = fixture("httpapi_tail_cron", lambda: _seeded_web_cron(1))
    name = next(iter(cron.cron_jobs))
    real_now = cron_mod.get_now
    # the windows are measured back from the clock: pin it to the fixture's
    # instant so each one holds the same rows on every run
    cron_mod.get_now = lambda *args, **kwargs: _NOW
    try:
        try:
            payload = cron._job_trends_build(name, list(records), None)
        except TypeError as exc:
            raise Skip(
                "_job_trends_build signature changed: %r" % exc
            ) from None
        t0 = time.perf_counter()
        for _ in range(10):
            # a fresh list each call: the build reverses its argument
            cron._job_trends_build(name, list(records), None)
        dt = time.perf_counter() - t0
    finally:
        cron_mod.get_now = real_now
    windows = payload.get("windows") or {}
    totals = [stats.get("total") for stats in windows.values()]
    if windows.get("all", {}).get("total") != n or not all(totals):
        raise RuntimeError(
            "the trend windows hold %r of %d records; the build did not "
            "aggregate the ledger" % (totals, n)
        )
    return dt


def _httpapi_dag_yaml(n_dags, n_tasks):
    lines = ["dags:"]
    for d in range(n_dags):
        lines.append("  - name: flow%03d" % d)
        lines.append('    schedule: "%d 2 * * *"' % (d % 60))
        lines.append("    tasks:")
        for t in range(n_tasks):
            lines.append("      - id: t%04d" % t)
            lines.append("        command: 'x'")
            if t:
                lines.append("        dependsOn:")
                for dep in range(max(0, t - 3), t):
                    lines.append("          - t%04d" % dep)
    lines.append("")
    return "\n".join(lines)


@bench(
    "webapi.dags_poll_1k",
    "webapi",
    detail="GET /dags revalidated (304) x200, 20 dags x 50 tasks",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_webapi_dags_poll():
    """The /dags leg of every web dashboard poll, on its steady path: the
    handler rebuilds each dag's task graph, serializes it and hashes it to
    answer 304.

    Nothing memoizes that work, it runs on the scheduler's loop, and it
    grows with the task count across every dag.  dag.list_dags_warm times
    the run rollup for a one-task dag and stops before the handler's
    serialize and hash.
    """
    import asyncio

    Cron = _cron_cls()
    if not hasattr(Cron, "_web_list_dags"):
        raise Skip("Cron._web_list_dags not present")
    try:
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    n_dags = _n(20, floor=2)
    n_tasks = 50

    def build():
        try:
            cron = Cron(None, config_yaml=_httpapi_dag_yaml(n_dags, n_tasks))
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        cron.web_config = {}
        return cron

    cron = fixture("httpapi_dags_cron_1k", build)

    async def run():
        first = await cron._web_list_dags(_mocked_get("/dags"))
        etag = first.headers.get("ETag")
        listed = json.loads(bytes(first.body))
        tasks = sum(len(entry.get("tasks") or ()) for entry in listed)
        if not etag or tasks != n_dags * n_tasks:
            raise RuntimeError(
                "GET /dags listed %d tasks (ETag %r), expected %d"
                % (tasks, etag, n_dags * n_tasks)
            )
        request = make_mocked_request(
            "GET",
            "/dags",
            headers={"If-None-Match": etag, "Accept-Encoding": "gzip"},
        )
        t0 = time.perf_counter()
        for _ in range(_n(200)):
            resp = await cron._web_list_dags(request)
        dt = time.perf_counter() - t0
        if resp.status != 304:
            raise RuntimeError(
                "GET /dags answered %d to a matching validator" % resp.status
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.summary_status_500",
    "webapi",
    detail="GET /summary + /status as text and as JSON, x80, 500 jobs",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_webapi_summary_status():
    """The two fleet rollups an external poller reads: a widget's
    /summary and a health check's /status.

    Each walks every job on the scheduler's loop per request with no
    memo, and mcp.handle_200 reaches the status rows only on their
    unseeded startup branch.  This runs the seeded steady-state branch,
    and the text rendering of /status that a plain curl receives.
    """
    import asyncio

    cron = fixture(
        "webapi_cron_500",
        lambda: _seeded_web_cron(_n(500), history_every=5),
    )
    if not hasattr(cron, "_web_get_summary") or not hasattr(
        cron, "_web_get_status"
    ):
        raise Skip("Cron._web_get_summary or _web_get_status not present")
    try:
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    total = len(cron.cron_jobs)

    async def run():
        summary_req = _mocked_get("/summary")
        text_req = _mocked_get("/status")
        json_req = make_mocked_request(
            "GET", "/status", headers={"Accept": "application/json"}
        )
        t0 = time.perf_counter()
        for _ in range(_n(80)):
            summary = await cron._web_get_summary(summary_req)
            text = await cron._web_get_status(text_req)
            rows = await cron._web_get_status(json_req)
        dt = time.perf_counter() - t0
        counted = json.loads(bytes(summary.body))["jobs"]["total"]
        lines = len(text.text.splitlines())
        listed = len(json.loads(bytes(rows.body)))
        if not counted == lines == listed == total:
            raise RuntimeError(
                "/summary counted %d jobs and /status listed %d and %d, "
                "expected %d" % (counted, lines, listed, total)
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.resources_poll_20x240",
    "webapi",
    detail="GET /jobs/{name}/resources x150, 20 runs x 240-point series",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_webapi_resources_poll():
    """The Resources tab's poll, the fastest in the web page (every 2 s by
    default, 0.5 s at the minimum, while a monitored job runs).

    Each request copies and serializes the recorded CPU and memory series
    of the newest 20 monitored runs, so the cost is runs times points per
    request on the scheduler's loop.
    """
    import asyncio

    Cron = _cron_cls()
    if not hasattr(Cron, "_web_job_resources"):
        raise Skip("Cron._web_job_resources not present")
    try:
        from aiohttp.test_utils import make_mocked_request

        from cronstable.cron import JobRunInfo
        from cronstable.job import JobOutputStream
        from cronstable.resources import ResourceUsage
    except ImportError as exc:
        raise Skip("resource-series API unavailable: %r" % exc) from None
    points = 240

    def build():
        cron = _seeded_web_cron(1)
        name = next(iter(cron.cron_jobs))
        output = JobOutputStream()
        try:
            for k in range(50):
                started = _NOW - timedelta(minutes=5 * (50 - k))
                series = [
                    [
                        1773577845.0 + i,
                        12.5 + (i + k) % 40,
                        150000000 + i * 4096,
                    ]
                    for i in range(points)
                ]
                cron.run_history[name].append(
                    JobRunInfo(
                        outcome="success",
                        exit_code=0,
                        started_at=started,
                        finished_at=started + timedelta(seconds=240),
                        fail_reason=None,
                        output=output,
                        resource_usage=ResourceUsage(
                            3.5, 0.5, 200000000, points, series=series
                        ),
                    )
                )
        except TypeError as exc:
            raise Skip("run or usage signature changed: %r" % exc) from None
        return cron, name

    cron, name = fixture("httpapi_resources_cron", build)

    async def run():
        request = make_mocked_request(
            "GET", "/jobs/%s/resources" % name, match_info={"name": name}
        )
        t0 = time.perf_counter()
        for _ in range(_n(150)):
            resp = await cron._web_job_resources(request)
        dt = time.perf_counter() - t0
        runs = json.loads(bytes(resp.body)).get("runs") or []
        series = [
            len((run.get("resources") or {}).get("series") or ())
            for run in runs
        ]
        if series != [points] * 20:
            raise RuntimeError(
                "the resources payload carried series of %r points, "
                "expected 20 runs of %d" % (series, points)
            )
        return dt

    return asyncio.run(run())


def _httpapi_dag_run_store():
    """(store path, config yaml, tasks): one in-flight run document of a
    1k-task dag, built by the dag module and stored once."""

    def build():
        import asyncio

        dag = _dag_module()
        for attr in (
            "new_run_body",
            "TaskSpec",
            "DagSpec",
            "DAG_RUN_NS_PREFIX",
        ):
            if not hasattr(dag, attr):
                raise Skip("dag.%s not present" % attr)
        path = os.path.join(_tmpdir(), "httpapi-dag-run")
        os.makedirs(path, exist_ok=True)
        n = _n(1000, floor=10)
        tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
        try:
            spec = dag.DagSpec.build("benchdag", tasks)
            body = dag.new_run_body(
                dag="benchdag",
                run_key="r",
                run_id="rid",
                logical_date=None,
                kind="scheduled",
                now=1700000000.0,
                spec=spec,
            )
        except TypeError as exc:
            raise Skip("dag run-body API changed: %r" % exc) from None
        for task in tasks:
            entry = body["tasks"][task.id]
            entry["state"] = "running"
            entry["proc"] = "bench-proc"
            entry["pid"] = 4242
            entry["attempt"] = 0

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            try:
                await backend.mutate_document(
                    dag.DAG_RUN_NS_PREFIX + "benchdag",
                    "r",
                    lambda cur: (body, None),
                )
            finally:
                await backend.stop()

        asyncio.run(seed())
        cfg = "state:\n  path: %s\n%s" % (
            path.replace("\\", "/"),
            _BENCH_DAG_YAML,
        )
        return path, cfg, n

    return fixture("httpapi_dag_run_store", build)


@bench(
    "webapi.dag_run_poll_1k",
    "webapi",
    detail="GET /dags/{name}/runs/{key} x60, a 1k-task in-flight run",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_webapi_dag_run_poll():
    """The open run drawer's poll: every 3 s while the run is in flight,
    the handler reads the whole run document from the store and
    serializes it back out.

    The document grows with the task count (and with each mapped
    instance), and the response has no cache in front of it, so the cost
    recurs per poll.  dag.advance_quiescent_1k times the scheduler's own
    read of the same document, which stops before any serialize.
    """
    import asyncio

    Cron = _cron_cls()
    if not hasattr(Cron, "_web_dag_run"):
        raise Skip("Cron._web_dag_run not present")
    try:
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    path, cfg, n = _httpapi_dag_run_store()

    async def run():
        try:
            cron = Cron(None, config_yaml=cfg)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        cron.web_config = {}
        backend = _state_backend(path)
        await backend.start()
        cron.state_backend = backend
        cron._state_configured = True
        request = make_mocked_request(
            "GET",
            "/dags/benchdag/runs/r",
            match_info={"name": "benchdag", "run_key": "r"},
        )
        try:
            # the store's worker threads start on the first call
            await cron._web_dag_run(request)
            t0 = time.perf_counter()
            for _ in range(_n(60)):
                resp = await cron._web_dag_run(request)
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        tasks = json.loads(bytes(resp.body)).get("tasks") or {}
        if len(tasks) != n:
            raise RuntimeError(
                "the run document carried %d tasks, expected %d"
                % (len(tasks), n)
            )
        return dt

    return asyncio.run(run())


@bench(
    "webapi.fleet_bytes_15x400",
    "webapi",
    detail="GET /fleet body size, 15 nodes x 400 real job summaries",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_fleet_bytes():
    """The size of the fleet view's body at the marketed 15 x 400 fleet.

    Every node's block is the daemon's own per-job gossip summary, built
    by Cron.fleet_job_summaries, so a field added there is counted once
    per job per node here.  The same block rides every gossip exchange
    under a byte cap, which is the other reason its size matters.
    cluster.fleet_view_15x400 times the merge over a synthetic block and
    cannot see the real block grow.
    """
    import asyncio

    try:
        from cronstable import cluster as cluster_mod
    except ImportError as exc:
        raise Skip("cronstable.cluster unavailable: %r" % exc) from None
    for attr in ("ClusterManager", "SCHEME_VERSION"):
        if not hasattr(cluster_mod, attr):
            raise Skip("cronstable.cluster lacks %s" % attr)
    nodes = 15
    jobs = _n(400, floor=4)

    def build():
        ca = os.path.join(_CERT_DIR, "bench-ca.pem")
        cert = os.path.join(_CERT_DIR, "bench-node.pem")
        key = os.path.join(_CERT_DIR, "bench-node-key.pem")
        if not (
            os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key)
        ):
            raise Skip("benchmarks/certs fixtures missing")
        cron = _seeded_web_cron(jobs, history_every=1)
        if not hasattr(cron, "fleet_job_summaries") or not hasattr(
            cron, "_build_fleet_product"
        ):
            raise Skip("Cron fleet view not present")
        names = ["fnode-%02d" % i for i in range(nodes)]
        hosts = [
            "fnode-%02d.bench.internal:29999" % i for i in range(1, nodes)
        ]
        config = {
            "nodeName": names[0],
            "peers": [{"host": host} for host in hosts],
            "driftAfter": 3,
            "distribution": "spread",
            "electLeader": True,
            "interval": 5,
            "tls": {"ca": ca, "cert": cert, "key": key},
        }
        try:
            mgr = cluster_mod.ClusterManager(config, lambda: "bench-jobset")
            mgr.set_job_summaries_provider(cron.fleet_job_summaries)
            block = cron.fleet_job_summaries()
            for i, host in enumerate(hosts, start=1):
                mgr.view.record_success(
                    host,
                    peer_name=names[i],
                    peer_id="bench-jobset",
                    peer_scheme=cluster_mod.SCHEME_VERSION,
                    my_id="bench-jobset",
                    now=_NOW,
                    my_name=names[0],
                    peer_instance="finst-%02d" % i,
                    my_instance=mgr.instance_id,
                    peer_members=[(names[0], mgr.instance_id, True)],
                    peer_size=nodes,
                    peer_distribution="spread",
                    peer_elect_leader=True,
                    peer_reports_members=True,
                    peer_job_summaries=block,
                    peer_job_summaries_at=_NOW,
                )
        except (AttributeError, TypeError, KeyError) as exc:
            raise Skip("cluster fleet API changed: %r" % exc) from None
        cron.cluster_manager = mgr
        return cron

    cron = fixture("httpapi_fleet_cron_15x400", build)

    async def run():
        _etag, body, _gz = await cron._build_fleet_product()
        listed = json.loads(bytes(body)).get("nodes") or []
        counts = [len(node.get("jobs") or ()) for node in listed]
        if counts != [jobs] * nodes:
            raise RuntimeError(
                "GET /fleet carried %r jobs per node, expected %d nodes "
                "of %d" % (counts, nodes, jobs)
            )
        return len(body) / 1024.0

    return asyncio.run(run())


@bench(
    "webapi.index_bytes",
    "webapi",
    detail="dashboard page size as served to a client without gzip",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_index_bytes():
    """The weight of the single-page dashboard, which every first load
    downloads and every browser then parses.

    The page is one file that grows with each UI feature and nothing else
    measures it.  A byte count is exact, so the gate sees growth a timing
    metric cannot.
    """
    try:
        from cronstable import cron as cron_mod
    except ImportError as exc:
        raise Skip("cronstable.cron unavailable: %r" % exc) from None
    document = getattr(cron_mod, "_index_document", None)
    if document is None:
        raise Skip("cron._index_document not present")
    raw = document()[0]
    if len(raw) < 1024:
        raise RuntimeError("the dashboard page is %d bytes" % len(raw))
    return len(raw) / 1024.0


@bench(
    "webapi.index_gzip_bytes",
    "webapi",
    detail="dashboard page size as served to a client that accepts gzip",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_webapi_index_gzip_bytes():
    """The dashboard page over the wire: what a browser downloads.

    The daemon compresses the page once per process with the standard
    library's zlib at level 9, whichever backend compresses the JSON
    responses, so the value is the same with or without the ISA-L
    package.  It guards both the page weight and the level: a page
    compressed at the per-response level 1 reads about half again as
    large.
    """
    import zlib

    try:
        from cronstable import cron as cron_mod
    except ImportError as exc:
        raise Skip("cronstable.cron unavailable: %r" % exc) from None
    document = getattr(cron_mod, "_index_document", None)
    packed_page = getattr(cron_mod, "_index_gzip", None)
    if document is None or packed_page is None:
        raise Skip("cron._index_gzip not present")
    packed = packed_page()
    if zlib.decompress(packed, wbits=31) != document()[0]:
        raise RuntimeError("the gzipped page does not decode to the page")
    return len(packed) / 1024.0


# ---------------------------------------------------------------------------
# jobapi: the loopback state API a running job calls.
# ---------------------------------------------------------------------------
def _httpapi_job_api(backend_getter):
    try:
        from cronstable.jobapi import JobStateAPI, RunContext
    except ImportError as exc:
        raise Skip("cronstable.jobapi unavailable: %r" % exc) from None
    try:
        api = JobStateAPI(
            backend_getter, base_holder="bench-host#proc", config={}
        )
    except TypeError as exc:
        raise Skip("JobStateAPI signature changed: %r" % exc) from None

    def register(i):
        try:
            ctx = RunContext(
                token="%064x" % (0xB0B0 + i),
                run_id="%032x" % i,
                job_name="job%05d" % i,
                attempt=0,
                scheduled_at=None,
                host="bench-host",
                default_scope="job%05d" % i,
                secrets={"api-key": "s3cret-%d" % i},
            )
        except TypeError as exc:
            raise Skip("RunContext signature changed: %r" % exc) from None
        api.register_run(ctx)
        return ctx

    return api, register


@bench(
    "jobapi.read_path_10k",
    "jobapi",
    detail="GET /v1/run + /v1/secret/get x5k each, 100 live runs",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_jobapi_read_path():
    """The per-request cost of the state API's read path, in process: the
    error envelope, the bearer-token scan over every live run, and a
    handler that touches no store.

    The scan is linear in the runs alive on the daemon, and it fronts
    every request a job makes.  The presented token belongs to the run
    registered last, so each request walks the whole table.  The store
    primitives behind the other routes are state.kv_roundtrip_200's and
    state.artifact_*'s subject; this is the layer in front of them.
    """
    import asyncio

    try:
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    api, register = _httpapi_job_api(lambda: None)
    for attr in ("_middlewares", "_h_run", "_h_secret_get"):
        if not hasattr(api, attr):
            raise Skip("JobStateAPI.%s not present" % attr)
    for i in range(100):
        ctx = register(i)
    headers = {"Authorization": "Bearer " + ctx.token}
    n = _n(5000, floor=20)

    async def run():
        envelope = api._middlewares()[0]
        run_req = make_mocked_request("GET", "/v1/run", headers=headers)
        secret_req = make_mocked_request(
            "GET", "/v1/secret/get?name=api-key", headers=headers
        )
        t0 = time.perf_counter()
        for _ in range(n):
            who = await envelope(run_req, api._h_run)
            secret = await envelope(secret_req, api._h_secret_get)
        dt = time.perf_counter() - t0
        if (
            json.loads(bytes(who.body)).get("job") != ctx.job_name
            or json.loads(bytes(secret.body)).get("value")
            != ctx.secrets["api-key"]
        ):
            raise RuntimeError(
                "the state API answered %r and %r for the last run"
                % (who.body, secret.body)
            )
        return dt

    return asyncio.run(run())


def _httpapi_semaphore_store():
    """(store path, permits): a semaphore whose every permit is held, by
    leases a first API instance took and left unreleased."""

    def build():
        import asyncio

        path = os.path.join(_tmpdir(), "httpapi-semaphore")
        os.makedirs(path, exist_ok=True)
        permits = _n(64, floor=4)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            api, register = _httpapi_job_api(lambda: backend)
            holder = register(0)
            try:
                for _ in range(permits):
                    got = await api.locks.acquire(
                        holder.token,
                        "global",
                        "bench-sem",
                        permits=permits,
                        ttl=86400.0,
                    )
                    if not got.get("acquired"):
                        raise RuntimeError(
                            "could not fill the semaphore: %r" % got
                        )
            except (AttributeError, TypeError) as exc:
                raise Skip("job lock API changed: %r" % exc) from None
            finally:
                # the leases outlive this loop: the renewers die with it,
                # and a day-long TTL keeps every permit held
                await backend.stop()

        asyncio.run(seed())
        return path, permits

    return fixture("httpapi_semaphore_store", build)


@bench(
    "jobapi.sem_denied_64x10",
    "jobapi",
    detail="10 denied acquires on a saturated 64-permit semaphore",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_jobapi_sem_denied():
    """What a job pays to learn that a semaphore is full: one lease
    attempt per permit, each a locked read of that permit's lease.

    A waiting acquire repeats the pass every second until its deadline,
    so the store work is waiters times permits per second, on the lease
    lane the cluster's own leases use.  state.lease_renew_200 times the
    renewal of one held lease and never the denied attempt.
    """
    import asyncio

    path, permits = _httpapi_semaphore_store()

    async def run():
        backend = _state_backend(path)
        await backend.start()
        api, register = _httpapi_job_api(lambda: backend)
        waiter = register(1)
        try:
            # the store's worker threads start on the first call
            await backend.read_lease("bench-warm")
            t0 = time.perf_counter()
            for _ in range(_n(10)):
                got = await api.locks.acquire(
                    waiter.token, "global", "bench-sem", permits=permits
                )
            dt = time.perf_counter() - t0
        except (AttributeError, TypeError) as exc:
            raise Skip("job lock API changed: %r" % exc) from None
        finally:
            await backend.stop()
        if got != {"acquired": False}:
            raise RuntimeError(
                "the saturated semaphore answered %r; the fixture did not "
                "hold every permit" % (got,)
            )
        return dt

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# cluster: election-derived ownership and gossip absorption.  cluster.py is
# the largest optimized-and-unguarded module in the tree; nothing here opens
# a socket (the manager is never start()ed) -- construction only needs real
# TLS files, pre-minted 100-year fixtures in benchmarks/certs/.
# ---------------------------------------------------------------------------


_CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")


def _cluster_manager_50():
    """A ClusterManager observing a healthy 50-member spread cluster.

    Every peer is recorded as mutually agreeing (it lists our instance_id as
    AGREED), declares the matching size/policy, and gossips a quorate
    mutual_agreeing set -- the healthy steady state, where ownership is
    sha256-rendezvous compute over the member set (layout-safe).
    """

    def build():
        try:
            from cronstable import cluster as cluster_mod
        except ImportError as exc:
            raise Skip("cronstable.cluster unavailable: %r" % exc) from None
        for attr in ("ClusterManager", "SCHEME_VERSION"):
            if not hasattr(cluster_mod, attr):
                raise Skip("cronstable.cluster lacks %s" % attr)
        ca = os.path.join(_CERT_DIR, "bench-ca.pem")
        cert = os.path.join(_CERT_DIR, "bench-node.pem")
        key = os.path.join(_CERT_DIR, "bench-node-key.pem")
        if not (
            os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key)
        ):
            raise Skip("benchmarks/certs fixtures missing")
        members = 50
        names = ["node-%02d" % i for i in range(members)]
        hosts = [
            "node-%02d.bench.internal:29999" % i for i in range(1, members)
        ]
        config = {
            "nodeName": names[0],
            "peers": [{"host": host} for host in hosts],
            "driftAfter": 3,
            "distribution": "spread",
            "electLeader": True,
            "tls": {"ca": ca, "cert": cert, "key": key},
        }
        try:
            mgr = cluster_mod.ClusterManager(config, lambda: "bench-jobset")
        except (TypeError, KeyError) as exc:
            raise Skip(
                "ClusterManager construction changed: %r" % exc
            ) from None
        all_names = set(names)
        try:
            for i, host in enumerate(hosts, start=1):
                mgr.view.record_success(
                    host,
                    peer_name=names[i],
                    peer_id="bench-jobset",
                    peer_scheme=cluster_mod.SCHEME_VERSION,
                    my_id="bench-jobset",
                    now=_NOW,
                    my_name=names[0],
                    peer_instance="inst-%02d" % i,
                    my_instance=mgr.instance_id,
                    peer_members=[(names[0], mgr.instance_id, True)],
                    peer_size=members,
                    peer_mutual_agreeing=all_names - {names[i]},
                    peer_distribution="spread",
                    peer_elect_leader=True,
                    peer_reports_members=True,
                )
            except_probe = mgr.job_owner("job00000")
        except TypeError as exc:
            raise Skip("cluster observation API changed: %r" % exc) from None
        if except_probe is None:
            # not-quorate returns None BEFORE any hashing: a fixture that
            # fails this would silently time nothing at all.
            raise RuntimeError(
                "cluster fixture is not quorate; job_owner() returned None "
                "and the timed region would measure a no-op"
            )
        return mgr

    return fixture("cluster_mgr_50", build)


@bench(
    "cluster.job_owner_2k",
    "cluster",
    detail="2k alternating job_owner/available_job_owner at 50 members",
    repeats=(3, 2, 1),
)
def bench_cluster_job_owner():
    """Per-job ownership derivation on a healthy 50-member spread cluster.

    The election memoization (ownership derived once per view-mutation
    generation, then per-job rendezvous hashing only) is behaviour-invariant
    by design, so its loss is functionally invisible everywhere else;
    broken-memo measured 3.2x here.  The healthy path is sha256-rendezvous
    compute-dominated, so the metric is layout-safe.
    """
    mgr = _cluster_manager_50()
    n = _n(2000)
    # Roll the peer table's mutation generation so this repeat measures a COLD
    # ownership pass. The fixture is cached across the warm-up and every
    # repeat, and ownership is now memoized per view-mutation generation, so
    # without this every repeat after the first would time 2000 dict hits
    # (~0.08 ms) instead of the rendezvous hashing this metric exists to
    # watch. Any PeerState field write bumps the counter (see
    # PeerState.__setattr__), and it is done BEFORE t0 so the invalidation
    # itself is not part of the measurement.
    some_host = next(iter(mgr.view.peers))
    mgr.view.peers[some_host].last_seen = _NOW
    t0 = time.perf_counter()
    for i in range(n):
        name = "job%05d" % i
        if i % 2:
            mgr.available_job_owner(name)
        else:
            mgr.job_owner(name)
    return time.perf_counter() - t0


@bench(
    "cluster.parse_summaries_6k",
    "cluster",
    detail="absorb 15 peers' gossiped job summaries x12 (400 jobs each)",
    repeats=(3, 2, 1),
)
def bench_cluster_parse_summaries():
    """Gossip payload validation on the absorption path, at the marketed
    15x400 fleet scale.

    _parse_job_summaries type-checks and rebuilds every field of every
    entry of every peer's gossiped block (a peer is CA-vouched, not
    trusted); per-entry hardening added here is the classic byte-identical-
    output regression shape.  Parse-only by design: the originally proposed
    fleet_job_summaries leg is wall-clock dependent and would break paired
    comparison whenever one side skips it.
    """
    try:
        from cronstable.cluster import _parse_job_summaries
    except ImportError as exc:
        raise Skip("_parse_job_summaries unavailable: %r" % exc) from None
    peers = 15
    jobs = _n(400)

    def build():
        payloads = []
        for p in range(peers):
            block = {}
            for j in range(jobs):
                name = "job%05d" % j
                entry = {
                    "running": (j + p) % 7 == 0,
                    "enabled": (j + p) % 11 != 0,
                    "scheduled_in": float((j * 13 + p) % 3600),
                    "last": {
                        "outcome": "success" if (j + p) % 5 else "failure",
                        "finished_at": "2026-07-01T10:%02d:%02d+00:00"
                        % (j // 60 % 60, j % 60),
                        "duration": 1.5 + (j % 20),
                        "exit_code": 0 if (j + p) % 5 else 1,
                    },
                }
                if j % 9 == 0:
                    entry["junk_key"] = {"nested": [1, 2, 3]}
                if j % 13 == 0:
                    entry["scheduled_in"] = "not-a-number"
                block[name] = entry
            payloads.append(block)
        return payloads

    payloads = fixture("gossip_payloads_15x400", build)
    t0 = time.perf_counter()
    for _ in range(12):
        for block in payloads:
            parsed = _parse_job_summaries(block)
            if parsed is None:
                raise RuntimeError("gossip block failed to parse at all")
    return time.perf_counter() - t0


def _fleet_summary_block(jobs, salt):
    """One node's advertised per-job summary block."""
    block = {}
    for j in range(jobs):
        block["job%05d" % j] = {
            "running": (j + salt) % 7 == 0,
            "enabled": (j + salt) % 11 != 0,
            "scheduled_in": float((j * 13 + salt) % 3600),
            "last": {
                "outcome": "success" if (j + salt) % 5 else "failure",
                "finished_at": "2026-07-01T10:%02d:%02d+00:00"
                % (j // 60 % 60, j % 60),
                "duration": 1.5 + (j % 20),
                "exit_code": 0 if (j + salt) % 5 else 1,
            },
        }
    return block


@bench(
    "cluster.fleet_view_15x400",
    "cluster",
    detail="60 fleet_view merges over 15 nodes x 400 absorbed summaries",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_cluster_fleet_view():
    """The GET /fleet merge, which nothing measured before.

    cluster.parse_summaries_6k covers ABSORPTION: validating a peer's
    gossiped block once per poll round.  This is the other half: every
    dashboard poll of the fleet view walks each peer's stored snapshot and
    re-derives every advertised countdown from the snapshot's true age
    (_aged_job_summaries copies each entry rather than mutating the stored
    one, so the work is per job per node per poll and no cache can be
    smuggled in without noticing).  At the marketed 15x400 fleet that is
    6000 dict copies per poll, on the scheduler's loop, at whatever rate
    the open dashboards ask.

    A dedicated 15-node manager, NOT the 50-member ownership fixture: this
    one carries absorbed summaries, and sharing would make both metrics
    order-dependent.  Wall-clock ageing is bounded by construction (the
    snapshots are stamped at the suite's fixed _NOW, so `elapsed` is a
    large constant and every countdown clamps at 0) which keeps the timed
    work identical run to run.
    """
    try:
        from cronstable import cluster as cluster_mod
    except ImportError as exc:
        raise Skip("cronstable.cluster unavailable: %r" % exc) from None
    for attr in ("ClusterManager", "SCHEME_VERSION"):
        if not hasattr(cluster_mod, attr):
            raise Skip("cronstable.cluster lacks %s" % attr)
    nodes = 15
    jobs = _n(400)

    def build():
        ca = os.path.join(_CERT_DIR, "bench-ca.pem")
        cert = os.path.join(_CERT_DIR, "bench-node.pem")
        key = os.path.join(_CERT_DIR, "bench-node-key.pem")
        if not (
            os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key)
        ):
            raise Skip("benchmarks/certs fixtures missing")
        names = ["fnode-%02d" % i for i in range(nodes)]
        hosts = [
            "fnode-%02d.bench.internal:29999" % i for i in range(1, nodes)
        ]
        config = {
            "nodeName": names[0],
            "peers": [{"host": host} for host in hosts],
            "driftAfter": 3,
            "distribution": "spread",
            "electLeader": True,
            # fleet_view reads the poll cadence (it publishes it, and scales
            # the node-stats staleness window by it); the ownership fixture
            # never touches it, which is why only this one sets it
            "interval": 5,
            "tls": {"ca": ca, "cert": cert, "key": key},
        }
        try:
            mgr = cluster_mod.ClusterManager(config, lambda: "bench-jobset")
        except (TypeError, KeyError) as exc:
            raise Skip(
                "ClusterManager construction changed: %r" % exc
            ) from None
        # our own advertised block: the self leg of the merge is real work
        # too, and without a provider it is an empty-dict early return
        if not hasattr(mgr, "_job_summaries_provider"):
            raise Skip("cluster job-summary provider seam not present")
        own = _fleet_summary_block(jobs, 0)
        mgr._job_summaries_provider = lambda: own
        try:
            for i, host in enumerate(hosts, start=1):
                mgr.view.record_success(
                    host,
                    peer_name=names[i],
                    peer_id="bench-jobset",
                    peer_scheme=cluster_mod.SCHEME_VERSION,
                    my_id="bench-jobset",
                    now=_NOW,
                    my_name=names[0],
                    peer_instance="finst-%02d" % i,
                    my_instance=mgr.instance_id,
                    peer_members=[(names[0], mgr.instance_id, True)],
                    peer_size=nodes,
                    peer_distribution="spread",
                    peer_elect_leader=True,
                    peer_reports_members=True,
                    peer_job_summaries=_fleet_summary_block(jobs, i),
                    peer_job_summaries_at=_NOW,
                )
        except TypeError as exc:
            raise Skip(
                "cluster observation API changed: %r" % exc
            ) from None
        return mgr

    mgr = fixture("cluster_fleet_mgr_15", build)
    if not hasattr(mgr, "fleet_view"):
        raise Skip("ClusterManager.fleet_view not present")
    try:
        probe = mgr.fleet_view()
    except TypeError as exc:
        raise Skip("fleet_view signature changed: %r" % exc) from None
    seen = sum(1 for node in probe["nodes"] if node.get("jobs"))
    if seen != nodes:
        raise RuntimeError(
            "fleet_view merged %d of %d nodes' summaries; the fixture did "
            "not absorb and the region would time a walk over nothing"
            % (seen, nodes)
        )
    t0 = time.perf_counter()
    for _ in range(60):
        mgr.fleet_view()
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# cluster: the gossip round end to end.  cluster.parse_summaries_6k times one
# validator of the absorb leg and cluster.fleet_view_15x400 the dashboard
# merge; these drive the poll round and the /peer responder themselves, over
# a stubbed client session (no socket, the manager is never start()ed).
# ---------------------------------------------------------------------------


# A fixed stand-in for the per-process uuid4().hex, so every gossip fixture
# serves and absorbs identical bytes run to run.
_GOSSIP_INSTANCE = "be" * 16


def _gossip_manager(members, prefix):
    """A ClusterManager for a ``members``-node spread cluster, nobody polled.

    Returns ``(cluster module, manager, node names, peer hosts)``.
    """
    try:
        from cronstable import cluster as cluster_mod
    except ImportError as exc:
        raise Skip("cronstable.cluster unavailable: %r" % exc) from None
    for attr in ("ClusterManager", "SCHEME_VERSION", "STATUS_AGREED"):
        if not hasattr(cluster_mod, attr):
            raise Skip("cronstable.cluster lacks %s" % attr)
    ca = os.path.join(_CERT_DIR, "bench-ca.pem")
    cert = os.path.join(_CERT_DIR, "bench-node.pem")
    key = os.path.join(_CERT_DIR, "bench-node-key.pem")
    if not (
        os.path.exists(ca) and os.path.exists(cert) and os.path.exists(key)
    ):
        raise Skip("benchmarks/certs fixtures missing")
    names = ["%s-%02d" % (prefix, i) for i in range(members)]
    hosts = [
        "%s-%02d.bench.internal:29999" % (prefix, i) for i in range(1, members)
    ]
    config = {
        "nodeName": names[0],
        "peers": [{"host": host} for host in hosts],
        "driftAfter": 3,
        "distribution": "spread",
        "electLeader": True,
        "interval": 5,
        "tls": {"ca": ca, "cert": cert, "key": key},
    }
    try:
        mgr = cluster_mod.ClusterManager(config, lambda: "bench-jobset")
    except (TypeError, KeyError) as exc:
        raise Skip("ClusterManager construction changed: %r" % exc) from None
    mgr.instance_id = _GOSSIP_INSTANCE
    return cluster_mod, mgr, names, hosts


def _gossip_bodies(cluster_mod, names, hosts, jobs):
    """The /peer body each peer of a healthy full mesh serves, by host.

    Every peer lists every node as agreed, vouches the rest of the mesh,
    and advertises ``jobs`` run summaries, which is what a converged
    cluster gossips every round.
    """
    bodies = {}
    for index, host in enumerate(hosts, start=1):
        members = [
            {
                "node_name": names[other],
                "instance_id": (
                    _GOSSIP_INSTANCE if other == 0 else "inst-%02d" % other
                ),
                "agreed": True,
            }
            for other in range(len(names))
        ]
        others = sorted(set(names) - {names[index]})
        payload = {
            "node_name": names[index],
            "job_set_id": "bench-jobset",
            "scheme_version": cluster_mod.SCHEME_VERSION,
            "instance_id": "inst-%02d" % index,
            "cluster_size": len(names),
            "distribution": "spread",
            "elect_leader": True,
            "members": members,
            "ran_reboot_jobs": [],
            "mutual_agreeing": others,
            "quorate_vouched": others,
            "job_summaries": _fleet_summary_block(jobs, index),
            "job_summaries_truncated": False,
        }
        bodies[host] = json.dumps(payload, separators=(",", ":")).encode(
            "utf-8"
        )
    return bodies


class _GossipResponse:
    """The response surface ClusterManager._observe_peer reads: a 200 that
    carries one peer's full body."""

    def __init__(self, body, etag):
        self.status = 200
        self.headers = {"ETag": etag}
        self.content = self
        self._body = body

    def raise_for_status(self):
        pass

    async def iter_chunked(self, size):
        body = self._body
        for start in range(0, len(body), size):
            yield body[start : start + size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _GossipSession:
    """A client-session stand-in.  Every GET answers 200 with the peer's
    full body, the round a cluster pays whenever its peers' jobs ran."""

    def __init__(self, bodies):
        self.bodies = bodies
        self.gets = 0

    def get(self, url, **_kwargs):
        self.gets += 1
        host = url.split("//", 1)[-1].split("/", 1)[0]
        return _GossipResponse(self.bodies[host], '"bench-%s"' % host)


def _gossip_round_done(cluster_mod, mgr, hosts, jobs):
    """Raise unless every peer is agreed and holds ``jobs`` summaries."""
    for host in hosts:
        peer = mgr.view.peers[host]
        held = len(peer.job_summaries or ())
        if peer.status != cluster_mod.STATUS_AGREED or held != jobs:
            raise RuntimeError(
                "gossip round left %s %s with %d of %d summaries; the "
                "stubbed session does not feed _observe_peer"
                % (host, peer.status, held, jobs)
            )


def _gossip_converged(members, prefix, jobs):
    """A manager that polled a healthy ``members``-node mesh three times.

    Three rounds settle the view the way a running daemon's is settled:
    every peer agreed and attesting this node back, the convergence hold
    latched open.  Returns ``(cluster module, manager, hosts, session)``;
    the session stays installed for further rounds.
    """
    import asyncio

    cluster_mod, mgr, names, hosts = _gossip_manager(members, prefix)
    if not hasattr(mgr, "_poll_all") or not hasattr(mgr, "_session"):
        raise Skip("ClusterManager poll seam not present")
    session = _GossipSession(_gossip_bodies(cluster_mod, names, hosts, jobs))
    mgr._session = session

    async def settle():
        for _ in range(3):
            await mgr._poll_all()

    asyncio.run(settle())
    _gossip_round_done(cluster_mod, mgr, hosts, jobs)
    if mgr.job_owner("job00000") is None:
        raise RuntimeError(
            "gossip fixture is not quorate after three rounds; the "
            "election gates would time their fail-closed early returns"
        )
    return cluster_mod, mgr, hosts, session


@bench(
    "cluster.poll_round_15x400",
    "cluster",
    detail="12 gossip poll rounds, 14 peers x 400 job summaries, full bodies",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_cluster_poll_round():
    """A whole peer-poll round: read, decode, validate and record every
    peer's /peer body.

    cluster.parse_summaries_6k times one validator out of this path; the
    body read, the JSON decode, the members and name-set validation, the
    observation record and the per-peer task fan-out run here as well.  A
    per-entry or per-member cost added anywhere on the absorb path lands
    on every node, for every peer, every interval.
    """
    import asyncio

    jobs = _n(400)
    cluster_mod, mgr, hosts, session = fixture(
        "cluster_gossip_15x400",
        lambda: _gossip_converged(15, "pnode", jobs),
    )
    rounds = 12

    async def run():
        gets = session.gets
        t0 = time.perf_counter()
        for _ in range(rounds):
            await mgr._poll_all()
        dt = time.perf_counter() - t0
        if session.gets - gets != rounds * len(hosts):
            raise RuntimeError(
                "%d rounds issued %d GETs for %d peers"
                % (rounds, session.gets - gets, len(hosts))
            )
        return dt

    elapsed = asyncio.run(run())
    _gossip_round_done(cluster_mod, mgr, hosts, jobs)
    return elapsed


def _cluster_serve_fixture(jobs):
    """A converged 15-node manager advertising a seeded Cron's summaries.

    Returns ``(manager, hosts, provider call counter, job count)``.
    """

    def build():
        _cluster_mod, mgr, hosts, _session = _gossip_converged(15, "snode", 0)
        cron = _seeded_web_cron(_n(jobs), history_every=1)
        if not hasattr(cron, "fleet_job_summaries"):
            raise Skip("Cron.fleet_job_summaries not present")
        for attr in ("set_job_summaries_provider", "_handle_peer"):
            if not hasattr(mgr, attr):
                raise Skip("ClusterManager.%s not present" % attr)
        built = [0]

        def provider():
            built[0] += 1
            return cron.fleet_job_summaries()

        mgr.set_job_summaries_provider(provider)
        return mgr, hosts, built, len(cron.cron_jobs)

    return fixture("cluster_serve_15x%d" % jobs, build)


@bench(
    "cluster.peer_serve_400",
    "cluster",
    detail="60 /peer responses built cold, 15 members x 400 job summaries",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_cluster_peer_serve():
    """The responder half of gossip: summary snapshot, payload, body and
    ETag for one /peer answer.

    A node rebuilds this whenever its own view changed or the one-second
    reuse window lapsed, which at the default interval is most inbound
    polls.  The work is O(jobs) on the scheduler's loop: the per-job
    summary snapshot, the body encode and the canonical ETag projection
    (a second, sorted serialization of the whole payload).  A write to any
    peer's state before each call forces the rebuild, as an absorbed poll
    does.
    """
    import asyncio

    mgr, hosts, built, _jobs = _cluster_serve_fixture(400)
    peer = mgr.view.peers[hosts[0]]
    calls = 60

    async def run():
        request = _mocked_get("/peer")
        resp = await mgr._handle_peer(request)
        before = built[0]
        t0 = time.perf_counter()
        for _ in range(calls):
            peer.last_seen = _NOW
            resp = await mgr._handle_peer(request)
        dt = time.perf_counter() - t0
        if built[0] - before != calls:
            raise RuntimeError(
                "%d /peer calls built %d payloads; the region timed "
                "cache hits" % (calls, built[0] - before)
            )
        if resp.status != 200 or not resp.body:
            raise RuntimeError("/peer answered %s with no body" % resp.status)
        return dt

    return asyncio.run(run())


@bench(
    "cluster.peer_bytes_512",
    "cluster",
    detail="/peer response body size, 15 members x 512 job summaries",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_cluster_peer_bytes():
    """The size of a /peer body at the advertised cap of 512 job summaries.

    Every node sends this to every peer on every round whose content
    changed, and a poller refuses a body over 256 KB, which drops the
    sender from quorum.  A field added to the per-job summary or to the
    envelope moves this number and no timing metric.  The seeded Cron
    clamps every countdown to 0.0 and the fixtures pin the instance id,
    so the byte count is exact.
    """
    import asyncio

    mgr, _hosts, _built, jobs = _cluster_serve_fixture(512)

    async def run():
        resp = await mgr._handle_peer(_mocked_get("/peer"))
        return resp.body

    body = asyncio.run(run())
    doc = json.loads(body)
    summaries = doc.get("job_summaries") or {}
    if len(summaries) != jobs or doc.get("job_summaries_truncated"):
        raise RuntimeError(
            "/peer advertised %d of %d job summaries; the body is not the "
            "full-cap payload" % (len(summaries), jobs)
        )
    if len(doc.get("members") or ()) != 15:
        raise RuntimeError("/peer does not list the 15-node mesh")
    return len(body) / 1024.0


@bench(
    "cluster.derive_cold_50",
    "cluster",
    detail="150 election recomputes after a view change, 50 members",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_cluster_derive_cold():
    """The election-derived state rebuilt from a changed peer table.

    Every recorded observation drops the memoized derivations, and the
    next gate read rebuilds them: mutual agreement, bridge candidates,
    vouched contenders and duplicate-name detection each walk every peer's
    gossiped member list, so the rebuild is quadratic in members.
    cluster.job_owner_2k pays it once inside two thousand rendezvous
    hashes, where it is under 1% of the value.  Each iteration here is one
    view change followed by the conflict gate, both ownership gates and
    the /cluster view.
    """

    def build():
        _cluster_mod, mgr, hosts, _session = _gossip_converged(50, "dnode", 0)
        for attr in (
            "has_conflict",
            "is_job_owner",
            "is_available_job_owner",
            "view_dict",
        ):
            if not hasattr(mgr, attr):
                raise Skip("ClusterManager.%s not present" % attr)
        return mgr, hosts

    mgr, hosts = fixture("cluster_derive_mgr_50", build)
    peer = mgr.view.peers[hosts[0]]
    if mgr.has_conflict():
        raise RuntimeError("derive fixture reports a conflict")
    state_key = getattr(mgr, "_derived_state_key", None)
    if state_key is not None:
        before = state_key()
        peer.last_seen = _NOW
        if state_key() == before:
            raise RuntimeError(
                "a peer-state write did not roll the derived-state key; "
                "the region would time memo hits"
            )
    rolls = _n(150, 2)
    t0 = time.perf_counter()
    for _ in range(rolls):
        peer.last_seen = _NOW
        mgr.has_conflict()
        mgr.is_job_owner("job00000")
        mgr.is_available_job_owner("job00000")
        mgr.view_dict()
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# prometheus / statsd / mcp / job: the remaining always-on daemon surfaces.
# ---------------------------------------------------------------------------


@bench(
    "prometheus.render_500",
    "prometheus",
    detail="/metrics exposition render x2, 500 jobs with run counters",
    repeats=(3, 2, 1),
)
def bench_prometheus_render():
    """Scrape rendering, which runs synchronously on the event loop every
    15-60s forever at O(jobs x families x buckets).

    prometheus.py had no group despite the escape-memo optimization, whose
    loss is byte-identical output.  The Cron's next-fire index is pre-seeded
    (an unstarted Cron otherwise takes the per-job engine-search fallback:
    the wrong branch, with wall-clock-dependent cost), every job carries a
    few recorded runs so the histogram/counter families are populated, and
    escape-needing label values are a small minority (as in production).
    """
    cron = fixture(
        "prom_cron_500", lambda: _seeded_web_cron(_n(500))
    )
    try:
        from cronstable.prometheus import PrometheusMetrics
    except ImportError as exc:
        raise Skip("cronstable.prometheus unavailable: %r" % exc) from None

    def build():
        metrics = PrometheusMetrics()
        try:
            for i, name in enumerate(cron.cron_jobs):
                metrics.job_run_recorded(name, "success", 1.5 + (i % 20))
                if i % 7 == 0:
                    metrics.job_run_recorded(name, "failure", 0.5)
        except TypeError as exc:
            raise Skip(
                "job_run_recorded signature changed: %r" % exc
            ) from None
        # the escape-needing minority: state-dropped kinds are the one label
        # source that is free text rather than a config-validated name
        if hasattr(metrics, "_state_dropped"):
            metrics._state_dropped = {
                "run-record": 3,
                'kind"with\\escapes': 1,
            }
        return metrics

    metrics = fixture("prom_metrics_500", build)
    try:
        text = metrics.render(cron)
    except TypeError as exc:
        raise Skip(
            "PrometheusMetrics.render signature changed: %r" % exc
        ) from None
    if "cronstable" not in text:
        raise RuntimeError("exposition render produced no cronstable families")
    t0 = time.perf_counter()
    metrics.render(cron)
    metrics.render(cron)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# prometheus (continued): the durable counter snapshot.
# ---------------------------------------------------------------------------
def _httpapi_pinned_registry(names):
    """A PrometheusMetrics holding run counters for ``names`` at fixed
    instants.

    Built through the public round trip (record, snapshot, seed) so the
    last-success and last-failure stamps, which ``job_run_recorded`` takes
    from the wall clock, are pinned and the bytes rendered from the
    registry are stable.
    """
    try:
        from cronstable.prometheus import PrometheusMetrics
    except ImportError as exc:
        raise Skip("cronstable.prometheus unavailable: %r" % exc) from None
    live = PrometheusMetrics()
    if not hasattr(live, "counters_snapshot") or not hasattr(
        live, "seed_counters"
    ):
        raise Skip("PrometheusMetrics counter snapshots not present")
    try:
        for i, name in enumerate(names):
            for k in range(1 + i % 5):
                live.job_run_recorded(name, "success", 1.5 + (i + k) % 20)
            if i % 7 == 0:
                live.job_run_recorded(name, "failure", 0.5)
    except TypeError as exc:
        raise Skip("job_run_recorded signature changed: %r" % exc) from None
    doc = live.counters_snapshot()
    for i, name in enumerate(names):
        entry = doc["jobs"][name]
        entry["last_success_time"] = 1773577845.25 + i
        if entry.get("last_failure_time") is not None:
            entry["last_failure_time"] = 1773577800.5 + i
    pinned = PrometheusMetrics()
    if pinned.seed_counters(doc, names) != len(names):
        raise RuntimeError("seed_counters did not seed every job")
    return pinned


def _httpapi_counter_registry():
    return fixture(
        "httpapi_counter_registry_2k",
        lambda: _httpapi_pinned_registry(
            ["job%05d" % i for i in range(_n(2000, floor=20))]
        ),
    )


def _httpapi_counter_encoder():
    try:
        from cronstable import _json
    except ImportError as exc:
        raise Skip("cronstable._json unavailable: %r" % exc) from None
    return _json.dumps_bytes


@bench(
    "prometheus.counter_snapshot_2k",
    "prometheus",
    detail="counters_snapshot + record encode x20, 2k jobs with counters",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_prometheus_counter_snapshot():
    """The durable counter snapshot's CPU: the whole-fleet dict build the
    scheduler does on its loop, then the record encode on a store worker.

    The daemon writes one snapshot per 15 seconds while runs finish, and
    each one covers every job, so the cost is linear in the fleet and
    recurs for the life of the process.  prometheus.render_500 covers the
    scrape; nothing covers this path.
    """
    metrics = _httpapi_counter_registry()
    encode = _httpapi_counter_encoder()
    snapshot = metrics.counters_snapshot()
    if len(snapshot.get("jobs") or ()) != _n(2000, floor=20):
        raise RuntimeError("the counter snapshot does not cover every job")
    t0 = time.perf_counter()
    for _ in range(20):
        encode(metrics.counters_snapshot())
    return time.perf_counter() - t0


@bench(
    "prometheus.counter_snapshot_bytes_2k",
    "prometheus",
    detail="one durable counter snapshot record, 2k jobs with counters",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_prometheus_counter_snapshot_bytes():
    """The size of one counter snapshot record, which the daemon appends
    to the state store every 15 seconds while runs finish.

    The record carries every job, so a field added to the per-job block
    is written again for each job in each snapshot.  The count of
    snapshots per window is a test (tests/test_perf_invariants.py); the
    size of each is this metric.
    """
    metrics = _httpapi_counter_registry()
    snapshot = metrics.counters_snapshot()
    if len(snapshot.get("jobs") or ()) != _n(2000, floor=20):
        raise RuntimeError("the counter snapshot does not cover every job")
    return len(_httpapi_counter_encoder()(snapshot)) / 1024.0


def _httpapi_scrape_fixture():
    """(cron, pinned registry) for 500 jobs.  The Cron is the fixture
    prometheus.render_500 builds, under the same name, so the two share
    it; the registry is this module's own, since the cold-scrape metric
    clears its memos."""
    cron = fixture("prom_cron_500", lambda: _seeded_web_cron(_n(500)))
    metrics = fixture(
        "httpapi_scrape_registry_500",
        lambda: _httpapi_pinned_registry(list(cron.cron_jobs)),
    )
    return cron, metrics


@bench(
    "prometheus.exposition_bytes_500",
    "prometheus",
    detail="/metrics exposition size, 500 jobs with run counters",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_prometheus_exposition_bytes():
    """The size of one scrape body, which Prometheus stores as series.

    Each job contributes every per-job family and every histogram bucket,
    so a family or a label added to the per-job set is multiplied by the
    fleet in both the scrape and the time-series database.  The render's
    duration barely moves for one more family; the byte count does.
    """
    cron, metrics = _httpapi_scrape_fixture()
    try:
        text = metrics.render(cron)
    except TypeError as exc:
        raise Skip(
            "PrometheusMetrics.render signature changed: %r" % exc
        ) from None
    if text.count('job_name="') < len(cron.cron_jobs):
        raise RuntimeError("the exposition carries no per-job series")
    return len(text.encode("utf-8")) / 1024.0


@bench(
    "prometheus.first_scrape_after_reload_500",
    "prometheus",
    detail="prune + full scrape render x8, 500 jobs (label memos cold)",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_prometheus_first_scrape_after_reload():
    """The scrape that follows a config reload.

    A reload prunes the registry, which drops the per-job label dicts and
    the rendered label blocks whether or not a job went away, so the next
    scrape rebuilds both.  prometheus.render_500 warms those memos before
    it times and so measures every scrape except this one.
    """
    cron, metrics = _httpapi_scrape_fixture()
    if not hasattr(metrics, "prune"):
        raise Skip("PrometheusMetrics.prune not present")
    keep = set(cron.cron_jobs)
    try:
        text = metrics.render(cron)
    except TypeError as exc:
        raise Skip(
            "PrometheusMetrics.render signature changed: %r" % exc
        ) from None
    if text.count('job_name="') < len(keep):
        raise RuntimeError("the exposition carries no per-job series")
    t0 = time.perf_counter()
    for _ in range(8):
        metrics.prune(keep)
        metrics.render(cron)
    return time.perf_counter() - t0


@bench(
    "statsd.emit_2k",
    "statsd",
    detail="send_to_statsd x2k over loopback UDP (unread receiver)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
)
def bench_statsd_emit():
    """The statsd delivery path exactly as shipped: a fresh datagram
    endpoint per message, twice per job run, inline on the scheduler loop.

    Pins the open endpoint-churn finding; when endpoint reuse lands, the
    drop becomes visible here and the new baseline pins it.  The one
    sanctioned bend of the no-network rule: loopback UDP to a socket that
    is bound but never read, which kills ICMP port-unreachable
    nondeterminism.  Functional tests check wire format only, so cost has
    no other guard.  (Scaled to 2k sends: 500 measured under the harness's
    50ms rule on Linux and would have shipped floor-bound.)
    """
    import asyncio
    import socket

    try:
        from cronstable.statsd import send_to_statsd
    except ImportError as exc:
        raise Skip("cronstable.statsd unavailable: %r" % exc) from None
    n = _n(2000)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        port = receiver.getsockname()[1]
        message = (
            "cronstable.bench.stop:1|g\n"
            "cronstable.bench.success:1|g\n"
            "cronstable.bench.duration:1250|ms\n"
        )

        async def run():
            await send_to_statsd("127.0.0.1", port, message)  # warm
            t0 = time.perf_counter()
            for _ in range(n):
                await send_to_statsd("127.0.0.1", port, message)
            return time.perf_counter() - t0

        try:
            return asyncio.run(run())
        except TypeError as exc:
            raise Skip("send_to_statsd signature changed: %r" % exc) from None
    finally:
        receiver.close()


@bench(
    "mcp.handle_200",
    "mcp",
    detail="MCP tools/call of cron_get_status x200, 200-job Cron",
    repeats=(3, 2, 1),
)
def bench_mcp_handle():
    """First-ever coverage of the MCP dispatch seam AND status_payload.

    handle_message is the documented transport-independent seam; a
    tools/call of cron_get_status walks every job per call.  The tool name
    is HARDCODED (fallback cron_list_jobs, both named in the server's own
    shipped instructions): runtime discovery would let the two release
    sides time different tools.  The private next-fire index is
    deliberately NOT pre-seeded here -- a silent absence on one side would
    desynchronize the pair -- so the schedules are dense simple ones whose
    fallback walk is wall-clock-stable.
    """
    import asyncio

    Cron = _cron_cls()
    try:
        from cronstable.mcp import MCPHandler
    except ImportError as exc:
        raise Skip("cronstable.mcp unavailable: %r" % exc) from None
    n = _n(200)

    def build():
        try:
            cron = Cron(None, config_yaml=_config_yaml(_n(200)))
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        try:
            handler = MCPHandler(
                cron,
                {
                    "readOnly": True,
                    "toolsets": ["observe"],
                    "maxRows": 500,
                    "maxBodyBytes": 1048576,
                    "allowedOrigins": [],
                },
            )
        except (TypeError, KeyError) as exc:
            raise Skip("MCPHandler construction changed: %r" % exc) from None
        return cron, handler

    _cron, handler = fixture("mcp_handler_200", build)
    tool = None
    for candidate in ("cron_get_status", "cron_list_jobs"):
        if candidate in getattr(handler, "_tool_by_name", {}):
            tool = candidate
            break
    if tool is None:
        raise Skip("neither cron_get_status nor cron_list_jobs is registered")
    msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": {}},
    }

    async def run():
        first = await handler.handle_message(msg)
        if not isinstance(first, dict) or "result" not in first:
            raise RuntimeError(
                "tools/call of %s did not return a result: %r"
                % (tool, first)
            )
        t0 = time.perf_counter()
        for _ in range(n):
            await handler.handle_message(msg)
        return time.perf_counter() - t0

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# mcp: the large tool responses, through the HTTP entry point.  mcp.handle_200
# times one small tool at the transport-independent seam and stops before the
# response is encoded; these go through handle_http, so the header checks,
# the JSON-RPC parse, the tool, both result serializations and the response
# object are all inside the region.
# ---------------------------------------------------------------------------
def _mcp_call(tool, arguments=None):
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments or {}},
    }


def _mcp_post(message):
    """A mocked ``POST /mcp`` carrying ``message``.  Needs a running loop.

    aiohttp caches a request's body after the first read, so one request
    object serves repeated ``handle_http`` calls.
    """
    import asyncio
    from unittest import mock

    try:
        from aiohttp import streams
        from aiohttp.test_utils import make_mocked_request
    except ImportError as exc:
        raise Skip("aiohttp.test_utils unavailable: %r" % exc) from None
    body = json.dumps(message).encode("utf-8")
    payload = streams.StreamReader(
        mock.Mock(), 2**16, loop=asyncio.get_running_loop()
    )
    payload.feed_data(body)
    payload.feed_eof()
    return make_mocked_request(
        "POST",
        "/mcp",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Content-Length": str(len(body)),
        },
        payload=payload,
    )


def _mcp_structured(body):
    """The ``structuredContent`` of a tool response body, or raise."""
    doc = json.loads(body)
    result = doc.get("result")
    if not isinstance(result, dict) or result.get("isError"):
        raise RuntimeError("MCP call did not succeed: %r" % (body[:200],))
    return result["structuredContent"]


def _mcp_served_cron():
    """The shared MCP fixture: a seeded 500-job Cron with run counters, the
    default read-only handler over it, and a handler serving every toolset.

    Returns ``(cron, handler, full handler, counters seeded)``.
    """

    def build():
        cron = _seeded_web_cron(_n(500), history_every=5)
        try:
            from cronstable.mcp import MCPHandler
        except ImportError as exc:
            raise Skip("cronstable.mcp unavailable: %r" % exc) from None
        seeded = False
        metrics = getattr(cron, "metrics", None)
        if metrics is not None and hasattr(metrics, "job_run_recorded"):
            try:
                for i, name in enumerate(cron.cron_jobs):
                    metrics.job_run_recorded(name, "success", 1.5 + (i % 20))
                    if i % 7 == 0:
                        metrics.job_run_recorded(name, "failure", 0.5)
                seeded = True
            except TypeError:
                seeded = False
        config = {
            "readOnly": True,
            "toolsets": ["observe"],
            "maxRows": 200,
            "maxBodyBytes": 1048576,
            "allowedOrigins": [],
        }
        try:
            handler = MCPHandler(cron, config)
            full = MCPHandler(
                cron,
                dict(
                    config,
                    readOnly=False,
                    toolsets=["observe", "act", "dags", "state"],
                ),
            )
        except (TypeError, KeyError) as exc:
            raise Skip("MCPHandler construction changed: %r" % exc) from None
        if not hasattr(handler, "handle_http"):
            raise Skip("MCPHandler.handle_http not present")
        return cron, handler, full, seeded

    return fixture("mcp_cron_500", build)


def _mcp_require_tool(handler, tool):
    if tool not in getattr(handler, "_tool_by_name", {}):
        raise Skip("%s is not registered" % tool)


@bench(
    "mcp.list_jobs_500",
    "mcp",
    detail="POST /mcp cron_list_jobs x80, 500 jobs, a full 200-row page",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_mcp_list_jobs():
    """The tool the server's instructions tell an agent to start with, at
    the default page size.

    One call selects over the whole job set, builds the page's full rows,
    serializes them twice (the JSON text block and structuredContent
    inside the envelope) and never yields the loop.  mcp.handle_200 stops
    at the tool result of a status call, so the job-row build, the
    envelope encode and the HTTP shell are guarded here.
    """
    import asyncio

    cron, handler, _full, _seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_list_jobs")
    jobs = len(cron.cron_jobs)
    calls = 80

    async def run():
        request = _mcp_post(_mcp_call("cron_list_jobs"))
        first = _mcp_structured((await handler.handle_http(request)).body)
        if (
            len(first["jobs"]) != min(200, jobs)
            or first["page"]["total"] != jobs
        ):
            raise RuntimeError(
                "cron_list_jobs returned %d rows of %r; not the full page"
                % (len(first["jobs"]), first["page"])
            )
        t0 = time.perf_counter()
        for _ in range(calls):
            await handler.handle_http(request)
        return time.perf_counter() - t0

    return asyncio.run(run())


@bench(
    "mcp.list_jobs_bytes_500",
    "mcp",
    detail="cron_list_jobs response body size, 500 jobs, a 200-row page",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=1.0,
)
def bench_mcp_list_jobs_bytes():
    """The size of the default cron_list_jobs page, which is what the
    calling agent pays for in context tokens.

    The body carries the page twice, as escaped JSON text and as
    structuredContent, so a field added to a job row costs more than
    twice its REST weight here.  webapi.jobs_bytes_500 sees the row; only
    this metric sees the envelope and the page size.  Deterministic for
    the reason that one is: the seeded instants clamp every countdown.
    """
    import asyncio

    cron, handler, _full, _seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_list_jobs")
    jobs = len(cron.cron_jobs)

    async def run():
        request = _mcp_post(_mcp_call("cron_list_jobs"))
        return (await handler.handle_http(request)).body

    body = asyncio.run(run())
    page = _mcp_structured(body)
    if len(page["jobs"]) != min(200, jobs):
        raise RuntimeError(
            "cron_list_jobs returned %d rows; not the full page"
            % len(page["jobs"])
        )
    return len(body) / 1024.0


def _mcp_fleet_manager():
    """A 15-node manager holding 400 absorbed summaries per peer, for the
    fleet tool.  Returns ``(manager, job count)``.

    Every snapshot and contact time is restamped to the suite's fixed
    instant after the rounds, so the merge ages each countdown by a large
    constant (they all clamp at 0) and prints a constant ``as_of`` for
    every peer.
    """

    def build():
        jobs = _n(400)
        _cluster_mod, mgr, hosts, _session = _gossip_converged(
            15, "mnode", jobs
        )
        if not hasattr(mgr, "_job_summaries_provider"):
            raise Skip("cluster job-summary provider seam not present")
        own = _fleet_summary_block(jobs, 0)
        mgr._job_summaries_provider = lambda: own
        for host in hosts:
            peer = mgr.view.peers[host]
            peer.last_seen = _NOW
            peer.job_summaries_at = _NOW
        return mgr, jobs

    return fixture("mcp_fleet_mgr_15", build)


async def _mcp_fleet_body(cron, handler, mgr, jobs, calls):
    """Serve cron_get_fleet ``calls`` times over ``mgr``'s view.

    Returns ``(last response body, seconds for the calls)``.  The manager
    is attached to the shared Cron for the duration only, so the other mcp
    metrics see the fixture as they built it.
    """
    request = _mcp_post(_mcp_call("cron_get_fleet"))
    previous = cron.cluster_manager
    cron.cluster_manager = mgr
    try:
        body = (await handler.handle_http(request)).body
        nodes = _mcp_structured(body).get("nodes") or []
        absorbed = sum(1 for n in nodes if len(n.get("jobs") or ()) == jobs)
        if len(nodes) != 15 or absorbed != 15:
            raise RuntimeError(
                "cron_get_fleet merged %d of 15 nodes with %d jobs each; "
                "the fixture did not absorb" % (absorbed, jobs)
            )
        t0 = time.perf_counter()
        for _ in range(calls):
            body = (await handler.handle_http(request)).body
        return body, time.perf_counter() - t0
    finally:
        cron.cluster_manager = previous


@bench(
    "mcp.fleet_15x400",
    "mcp",
    detail="POST /mcp cron_get_fleet x10, 15 nodes x 400 absorbed summaries",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_mcp_fleet():
    """The largest tool response: the whole fleet matrix in one call.

    cron_get_fleet takes no paging arguments, so one call merges every
    node's summaries (cluster.fleet_view_15x400 times that part alone),
    serializes the merge twice and returns it without yielding the loop.
    Two of the server's five prompts tell the agent to call it.
    """
    import asyncio

    cron, handler, _full, _seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_get_fleet")
    if not hasattr(cron, "cluster_manager"):
        raise Skip("Cron.cluster_manager not present")
    mgr, jobs = _mcp_fleet_manager()
    _body, elapsed = asyncio.run(_mcp_fleet_body(cron, handler, mgr, jobs, 10))
    return elapsed


@bench(
    "mcp.fleet_bytes_15x400",
    "mcp",
    detail="cron_get_fleet response body size, 15 nodes x 400 summaries",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=10.0,
)
def bench_mcp_fleet_bytes():
    """The size of the unpaged fleet response, in the agent's context.

    The one live field is this node's own ``as_of``, whose ISO form drops
    its microseconds when they are zero; the count is normalized to the
    32-character form wherever that string appears, which makes it exact.
    """
    import asyncio

    cron, handler, _full, _seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_get_fleet")
    if not hasattr(cron, "cluster_manager"):
        raise Skip("Cron.cluster_manager not present")
    mgr, jobs = _mcp_fleet_manager()
    body, _elapsed = asyncio.run(_mcp_fleet_body(cron, handler, mgr, jobs, 1))
    as_of = _mcp_structured(body)["nodes"][0]["as_of"]
    copies = body.count(as_of.encode("ascii"))
    if not copies:
        raise RuntimeError("this node's as_of is not in the fleet body")
    return (len(body) + copies * (32 - len(as_of))) / 1024.0


@bench(
    "mcp.tools_list_bytes",
    "mcp",
    detail="tools/list response body size, every toolset served",
    unit="KB",
    repeats=(3, 2, 1),
    compare="median",
    gate_floor=0.25,
)
def bench_mcp_tools_list_bytes():
    """The size of the tool catalog a client loads at the start of every
    session and keeps in the model's context for all of it.

    Each tool's description, input schema and output schema is in here,
    so the number moves with every tool added and every schema widened.
    """
    import asyncio

    _cron, _handler, full, _seeded = _mcp_served_cron()

    async def run():
        request = _mcp_post(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        )
        return (await full.handle_http(request)).body

    body = asyncio.run(run())
    tools = (json.loads(body).get("result") or {}).get("tools") or []
    if len(tools) < 2 or any("inputSchema" not in tool for tool in tools):
        raise RuntimeError("tools/list returned %d usable tools" % len(tools))
    return len(body) / 1024.0


@bench(
    "mcp.query_metrics_500",
    "mcp",
    detail="POST /mcp cron_query_metrics x12 from a cold snapshot, 500 jobs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_mcp_query_metrics():
    """A metrics query that has to walk the whole metric universe.

    The tool returns a handful of samples but visits every sample of every
    family to find them: the family build on the loop, the label and value
    formatting on the executor, then a name filter over the full snapshot.
    prometheus.render_500 shares the family build; the sample iterator and
    the filter are timed nowhere else.  The shared snapshot is dropped
    before each call, which is the query an agent makes a second or more
    after the previous one.
    """
    import asyncio

    cron, handler, _full, seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_query_metrics")
    if not seeded:
        raise Skip("PrometheusMetrics accumulators not present")
    jobs = len(cron.cron_jobs)
    calls = 12

    async def run():
        request = _mcp_post(
            _mcp_call("cron_query_metrics", {"match": "runs_total"})
        )
        # untimed: the first offloaded walk spawns the executor thread
        first = _mcp_structured((await handler.handle_http(request)).body)
        if first.get("totalMatched", 0) < jobs or not first.get("samples"):
            raise RuntimeError(
                "cron_query_metrics matched %r samples for %d jobs"
                % (first.get("totalMatched"), jobs)
            )
        memo = getattr(cron, "_metric_samples_memo", None)
        t0 = time.perf_counter()
        for _ in range(calls):
            if memo is not None:
                memo.cached = None
            await handler.handle_http(request)
        return time.perf_counter() - t0

    return asyncio.run(run())


# Keep this one last in the mcp group.  Run against a release whose listing
# builds a row for every job before it pages, the region builds a quarter of
# a million job rows, and the allocator state that leaves behind slows the
# benchmark that follows it (at twice the calls, mcp.fleet_15x400 read 37%
# high when it ran next).
@bench(
    "mcp.list_jobs_page_500",
    "mcp",
    detail="POST /mcp cron_list_jobs x500, state filter, limit 20, 500 jobs",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_mcp_list_jobs_page():
    """A small page of a large job set: the call an agent makes when it
    pages or filters and takes twenty rows at a time.

    The state filter walks all 500 jobs, reading each job's enabled flag
    and live-run list, and full rows are built for the twenty returned.
    A filter that builds a row per job to decide, or a page cut taken
    after the rows exist, multiplies this metric by the job count over
    the page size.
    """
    import asyncio

    cron, handler, _full, _seeded = _mcp_served_cron()
    _mcp_require_tool(handler, "cron_list_jobs")
    jobs = len(cron.cron_jobs)
    calls = _n(500, 5)

    async def run():
        request = _mcp_post(
            _mcp_call("cron_list_jobs", {"state": "scheduled", "limit": 20})
        )
        first = _mcp_structured((await handler.handle_http(request)).body)
        if (
            len(first["jobs"]) != min(20, jobs)
            or first["page"]["total"] != jobs
        ):
            raise RuntimeError(
                "cron_list_jobs returned %d rows of %r; not a 20-row page "
                "of every scheduled job" % (len(first["jobs"]), first["page"])
            )
        t0 = time.perf_counter()
        for _ in range(calls):
            await handler.handle_http(request)
        return time.perf_counter() - t0

    return asyncio.run(run())


@bench(
    "job.stream_capture_120k",
    "job",
    detail="per-line capture pipeline over a 120k-line stream",
    repeats=(3, 2, 1),
)
def bench_job_stream_capture():
    """The per-log-line capture pipeline: readline + utf-8 decode + the
    capture ring, run per output line of every captured job ON the event
    loop (captureStderr defaults true).  No bench previously imported
    cronstable.job at all.

    stream_name 'capture' disables the stdout/stderr passthrough mirror,
    so the region is the capture leg alone; the on_line live-tail leg is a
    known accepted residual (see the SSE gap note in benchmarks/README.md).
    The discard count is asserted so a fixture that fed nothing cannot
    time an instant EOF.  Scaled from the originally-specced 40k, which
    measured under the harness's 50ms rule on Linux; 120k is also the
    scale a future passthrough twin would share (the round-2 note).
    """
    import asyncio

    try:
        from cronstable.job import StreamReader
    except ImportError as exc:
        raise Skip("cronstable.job unavailable: %r" % exc) from None
    n = _n(120000)
    # scaled with the mode so the ring always evicts (full: the production
    # default of 1000 retained lines) and the discard assertion holds in
    # --quick/--smoke too
    save_limit = max(2, n // 120)

    def build():
        lines = []
        for i in range(n):
            if i % 16 == 15:
                lines.append("wide 进度 %d%% done\n" % (i % 100))
            else:
                lines.append(
                    "2026-07-18 12:00:%02d INFO worker %d processed batch\n"
                    % (i % 60, i)
                )
        return "".join(lines).encode("utf-8")

    blob = fixture("capture_blob_40k", build)

    async def run():
        stream = asyncio.StreamReader()
        stream.feed_data(blob)
        stream.feed_eof()
        t0 = time.perf_counter()
        try:
            reader = StreamReader(
                "bench", "capture", stream, "", save_limit
            )
        except TypeError as exc:
            raise Skip("StreamReader signature changed: %r" % exc) from None
        output, discarded = await reader.join()
        dt = time.perf_counter() - t0
        if discarded != n - save_limit or not output:
            raise RuntimeError(
                "capture pipeline discarded %d of %d lines (expected %d); "
                "the region did not process the stream"
                % (discarded, n, n - save_limit)
            )
        return dt

    return asyncio.run(run())


@bench(
    "job.report_noop_100k",
    "job",
    detail="report_success x100k with NO reporter configured (the default)",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_job_report_noop():
    """What a job completion pays for reporting when nothing is configured.

    The default config enables no reporter at all, so this is the path
    essentially every completion in every deployment takes, and today it
    still logs a line and gathers across all five reporters, spawning a Task
    per reporter, before each of them looks at its own config and returns.
    A mapped DAG fan-out finishing hundreds of instances at once pays it
    hundreds of times in one reaper batch; the shipped
    report_config_enabled probe exists precisely because that mattered on
    the DAG path, and this metric is what makes the same cost visible on the
    ordinary one.

    A RunningJob is constructed but never started (no process, no streams),
    which is exactly the state a completion handler holds when it reports.
    The reporters are asserted to be all-disabled first: with one
    accidentally live, the region would time an SMTP or HTTP attempt
    instead.
    """
    import asyncio

    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    try:
        from cronstable.job import RunningJob, report_config_enabled
    except ImportError as exc:
        raise Skip(
            "cronstable.job reporting API unavailable: %r" % exc
        ) from None
    # 100k, not the 20k this was specced at: with the disabled-reporter probe
    # in place a completion costs well under a microsecond, and 20k measured
    # far under the harness's 50ms rule (an effective ~110% gate against a
    # declared 15%).  The id carries the scale, per benchmarks/README.md.
    n = _n(100000)

    def build():
        config = JobConfig(
            mergedicts(
                DEFAULT_CONFIG,
                {
                    "name": "bench-report",
                    "command": "true",
                    "schedule": "0 4 * * *",
                },
            )
        )
        try:
            job = RunningJob(config, None)
        except TypeError as exc:
            raise Skip("RunningJob signature changed: %r" % exc) from None
        return job

    job = fixture("report_noop_job", build)
    if not hasattr(job, "report_success"):
        raise Skip("RunningJob.report_success not present")
    try:
        if report_config_enabled(job.config.onSuccess["report"]):
            raise RuntimeError(
                "the default onSuccess.report has a live reporter; the "
                "region would time real delivery"
            )
    except (KeyError, TypeError) as exc:
        raise Skip("report config shape changed: %r" % exc) from None

    async def run():
        await job.report_success()  # warm
        t0 = time.perf_counter()
        for _ in range(n):
            await job.report_success()
        return time.perf_counter() - t0

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# Scheduler core and job lifecycle: the launch herd, pool admission, the
# resource series of monitored runs, and a daemon booted in a child process.
# ---------------------------------------------------------------------------


# --- the launch herd: every job due in the same second ---------------------
#
# Shared by job.launch_reap_2k, job.launch_fail_retry_2k,
# loop.stall_launch_herd_8k and mem.launch_reap_steady_20x55.  The OS spawn
# is the only stub: asyncio.create_subprocess_shell/exec return a child that
# has already exited, so the launch path, RunningJob.start() and wait(), the
# reaper, the run record and the completion sequence all run for real.
_HERD_SEED_YAML = (
    "jobs:\n  - name: seed\n    command: 'x'\n    schedule: '0 0 * * *'\n"
)


#: The process environment while a herd runs.  Every launch scans
#: os.environ (cronstable.job.pyinstaller_env_leaks), so a fixed set keeps
#: the runner's own environment size out of the value.
_HERD_ENV = dict(
    ("CRONSTABLE_BENCH_%02d" % i, "value-%02d" % i) for i in range(32)
)


class _HerdProc:
    """A child process that has already exited, as RunningJob reads one."""

    pid = None  # no priority call, no resource monitor

    def __init__(self, returncode, kwargs):
        import asyncio

        self.returncode = None
        self._returncode = returncode
        self.stdout = self.stderr = None
        for name in ("stdout", "stderr"):
            if kwargs.get(name) is not None:
                pipe = asyncio.StreamReader()
                pipe.feed_eof()
                setattr(self, name, pipe)

    async def wait(self):
        self.returncode = self._returncode
        return self._returncode


def _herd_jobs(n, retry=False):
    """``n`` every-minute JobConfigs keyed by name, built once per group."""

    def build():
        try:
            from cronstable.config import (
                DEFAULT_CONFIG,
                JobConfig,
                mergedicts,
            )
        except ImportError as exc:
            raise Skip("cronstable.config API unavailable: %r" % exc) from None
        jobs = {}
        for i in range(n):
            raw = {
                "name": "herd%05d" % i,
                "command": "true",
                "schedule": "* * * * *",
            }
            if retry:
                raw["onFailure"] = {
                    "retry": {
                        "maximumRetries": 3,
                        "initialDelay": 60,
                        "maximumDelay": 300,
                        "backoffMultiplier": 2,
                    }
                }
            jobs[raw["name"]] = JobConfig(mergedicts(DEFAULT_CONFIG, raw))
        return jobs

    return fixture(
        "herd_jobs_%d_%s" % (n, "retry" if retry else "plain"), build
    )


def _herd_cron():
    """A stateless Cron whose launch, reap and completion seams exist."""
    import inspect

    Cron = _cron_cls()
    for attr in (
        "_launch_plan",
        "_wait_for_running_jobs",
        "_drain_completions",
    ):
        if not inspect.iscoroutinefunction(Cron.__dict__.get(attr)):
            raise Skip("Cron.%s seam absent or not async" % attr)
    try:
        return Cron(None, config_yaml=_HERD_SEED_YAML)
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None


async def _herd_cycle(cron, plan, counts, returncode=0, yielding=False):
    """Launch ``plan`` at once and reap every run; return the elapsed time.

    Runs inside the caller's event loop with the reaper already started.
    ``counts["spawned"]`` grows by one per stubbed spawn.
    """
    import asyncio

    async def spawn(*args, **kwargs):
        counts["spawned"] += 1
        if yielding:
            # a real spawn suspends once, which is what lets the spawn gate
            # hold later launches back
            await asyncio.sleep(0)
        return _HerdProc(returncode, kwargs)

    real = (asyncio.create_subprocess_shell, asyncio.create_subprocess_exec)
    asyncio.create_subprocess_shell = spawn
    asyncio.create_subprocess_exec = spawn
    environ = dict(os.environ)
    try:
        os.environ.clear()
        os.environ.update(_HERD_ENV)
        t0 = time.perf_counter()
        await cron._launch_plan(plan)
        # bounded: a wedged reaper must fail the benchmark, not hang CI
        for _ in range(20 * len(plan) + 1000):
            if not cron.running_jobs:
                break
            await asyncio.sleep(0)
        await cron._drain_completions()
        return time.perf_counter() - t0
    finally:
        os.environ.clear()
        os.environ.update(environ)
        asyncio.create_subprocess_shell, asyncio.create_subprocess_exec = real


async def _herd_stop(cron, reaper):
    """Stop the reaper and cancel the retry timers a failing herd armed."""
    import asyncio

    timers = [
        state.task
        for state in cron.retry_state.values()
        if state.task is not None
    ]
    for task in timers:
        task.cancel()
    await asyncio.gather(*timers, return_exceptions=True)
    cron._stop_event.set()
    cron._jobs_running.set()
    try:
        await asyncio.wait_for(reaper, timeout=5.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        reaper.cancel()
    await _teardown_cron(cron)


def _herd_check(cron, counts, n, outcome):
    recorded = [
        info.outcome for info in cron.last_run.values() if info is not None
    ]
    if (
        counts["spawned"] != n
        or cron.running_jobs
        or recorded != [outcome] * n
    ):
        raise RuntimeError(
            "the herd spawned %d and recorded %d %s runs of %d; the region "
            "timed the wrong work"
            % (counts["spawned"], recorded.count(outcome), outcome, n)
        )


def _herd_launch_reap(n, retry=False):
    import asyncio

    jobs = _herd_jobs(n, retry=retry)
    cron = _herd_cron()
    cron.cron_jobs = jobs
    plan = [(job, [_NOW]) for job in jobs.values()]
    counts = {"spawned": 0}

    async def run():
        reaper = asyncio.create_task(cron._wait_for_running_jobs())
        await asyncio.sleep(0)
        try:
            return await _herd_cycle(
                cron, plan, counts, returncode=1 if retry else 0
            )
        finally:
            armed = sum(
                1 for s in cron.retry_state.values() if s.task is not None
            )
            counts["armed"] = armed
            await _herd_stop(cron, reaper)

    dt = asyncio.run(run())
    _herd_check(cron, counts, n, "failure" if retry else "success")
    if retry and counts["armed"] != n:
        raise RuntimeError(
            "the failing herd armed %d retry timers for %d jobs"
            % (counts["armed"], n)
        )
    return dt


@bench(
    "job.launch_reap_2k",
    "job",
    detail="launch 2k due jobs at once and reap them (OS spawn stubbed)",
    repeats=(3, 2, 1),
    gate_pct=15.0,
    gate_floor=0.005,
)
def bench_job_launch_reap():
    """What the daemon itself pays to run a job, from due to recorded.

    Every job of a 2k fleet is due in the same second: _launch_plan launches
    them (launch_scheduled_job, maybe_launch_job, _launch_job_locked,
    RunningJob.start), the reaper collects them (RunningJob.wait,
    _record_finished_job, _record_run) and the completion sequence runs
    (handle_job_success).  Only the OS spawn is stubbed, with a child that
    has already exited, so the value is the scheduler's own per-run work,
    paid once per job per fire, and a regression anywhere on that path
    moves it.

    The process environment is swapped for a fixed 32-variable one around
    the timed region: every launch scans os.environ
    (cronstable.job.pyinstaller_env_leaks), and the runner's own environment
    size would otherwise show in the value.
    """
    return _herd_launch_reap(_n(2000, floor=20))


@bench(
    "job.launch_fail_retry_2k",
    "job",
    detail="2k due jobs fail at once and each arms its retry timer",
    repeats=(3, 2, 1),
    gate_pct=15.0,
    gate_floor=0.005,
)
def bench_job_launch_fail_retry():
    """The launch herd when every run fails and has a retry ladder.

    The same path as job.launch_reap_2k up to the exit code, then
    handle_job_failure and the arming of one schedule_retry_job timer per
    job.  A fleet-wide outage takes exactly this shape (every job failing in
    the same second), so the failure branch of the completion sequence is
    what this adds.  The timers (60 s) are cancelled untimed.
    """
    return _herd_launch_reap(_n(2000, floor=20), retry=True)


class _MirrorSink:
    """Stands in for the daemon's stderr: counts the bytes, keeps none."""

    encoding = "utf-8"

    def __init__(self):
        self.buffer = self
        self.size = 0

    def write(self, payload):
        self.size += len(payload)
        return len(payload)

    def flush(self):
        pass


@bench(
    "job.stream_passthrough_240k",
    "job",
    detail="per-line pipeline with the stderr mirror and live buffer, 240k",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_job_stream_passthrough():
    """The per-line pipeline as a default job runs it.

    job.stream_capture_120k times the capture leg alone.  A job with the
    default config also mirrors every stderr line to the daemon's own
    stderr (the prefix, the batch join, the hand-off to the mirror thread
    and its write) and publishes it to the live log buffer, which together
    cost more than the capture.  Both run per output line on the event
    loop, so a per-line cost added to either leg shows here.

    The daemon's stderr is replaced by a sink that counts bytes, and the
    region ends when the mirror thread has drained.  No subscriber is
    attached: the per-client fan-out stays with webapi.sse_burst_20k.
    """
    import asyncio

    try:
        from cronstable import job as job_mod
        from cronstable.job import JobOutputStream, StreamReader
    except ImportError as exc:
        raise Skip("cronstable.job unavailable: %r" % exc) from None
    mirror = getattr(job_mod, "_MIRROR", None)
    if mirror is None or not hasattr(mirror, "drain"):
        raise Skip("the passthrough mirror writer is not present")
    n = _n(240000, floor=240)
    save_limit = max(2, n // 240)
    prefix = "[{job_name} {stream_name}] "

    def build():
        lines = []
        for i in range(n):
            if i % 16 == 15:
                lines.append("wide 进度 %d%% done\n" % (i % 100))
            else:
                lines.append(
                    "2026-07-18 12:00:%02d INFO worker %d processed batch\n"
                    % (i % 60, i)
                )
        return "".join(lines).encode("utf-8")

    blob = fixture("passthrough_blob_%d" % n, build)
    mirrored = len(blob) + n * len("[bench stderr] ")

    async def run():
        stream = asyncio.StreamReader()
        stream.feed_data(blob)
        stream.feed_eof()
        output = JobOutputStream()
        sink = _MirrorSink()
        real_stderr = sys.stderr
        sys.stderr = sink
        try:
            t0 = time.perf_counter()
            try:
                reader = StreamReader(
                    "bench",
                    "stderr",
                    stream,
                    prefix,
                    save_limit,
                    on_line=output.publish,
                )
            except TypeError as exc:
                raise Skip(
                    "StreamReader signature changed: %r" % exc
                ) from None
            _saved, discarded = await reader.join()
            drained = mirror.drain(30.0)
            dt = time.perf_counter() - t0
        finally:
            sys.stderr = real_stderr
        if (
            not drained
            or sink.size != mirrored
            or output.published != n
            or discarded != n - save_limit
        ):
            raise RuntimeError(
                "the pipeline mirrored %d of %d bytes and published %d of "
                "%d lines; the region did not process the stream"
                % (sink.size, mirrored, output.published, n)
            )
        return dt

    return asyncio.run(run())


# --- pools: the durable admission queue ------------------------------------
#
# A pool is one document.  Every enqueue, claim, completion and lease
# renewal reads the whole document and writes it back, so each operation
# costs in proportion to the queue it sits in.
_POOL_NAME = "bench"


_POOL_SLOTS = 8


def _pool_yaml(path):
    lines = [
        "state:",
        "  path: %s" % path.replace("\\", "/"),
        "  jobApi:",
        "    enabled: false",
        "pools:",
        "  %s:" % _POOL_NAME,
        "    slots: %d" % _POOL_SLOTS,
        "    maxQueued: 10000",
        "jobs:",
    ]
    for i in range(4):
        lines.append("  - name: pooled%d" % i)
        lines.append("    command: 'true'")
        lines.append('    schedule: "%d 4 * * *"' % i)
        lines.append("    pool: %s" % _POOL_NAME)
    lines.append("")
    return "\n".join(lines)


async def _pool_cron(tag):
    """A booted Cron on a store of its own, pool dispatcher switched off."""
    import inspect

    Cron = _cron_cls()
    try:
        from cronstable.config import parse_config_string
        from cronstable.pools import NAMESPACE, PoolScheduler
    except ImportError as exc:
        raise Skip("cronstable.pools unavailable: %r" % exc) from None
    for attr in ("_tick_pool", "acquire", "finish", "enqueue"):
        if not inspect.iscoroutinefunction(PoolScheduler.__dict__.get(attr)):
            raise Skip("PoolScheduler.%s seam absent or not async" % attr)
    path = os.path.join(_tmpdir(), "pool-store-%s" % tag)
    os.makedirs(path, exist_ok=True)
    text = _pool_yaml(path)
    state_config = parse_config_string(text, "").state_config
    try:
        cron = Cron(None, config_yaml=text)
    except TypeError as exc:
        raise Skip("Cron signature changed: %r" % exc) from None
    await cron.start_stop_state(state_config)
    if cron.state_backend is None or _POOL_NAME not in cron.pool_config:
        raise RuntimeError("the pool fixture did not boot a state backend")
    # the benchmark drives the queue itself: no dispatcher, no heartbeat
    cron._pools.service = lambda: None
    await cron.state_backend.delete_document(NAMESPACE, _POOL_NAME)
    return cron, NAMESPACE


async def _pool_stop(cron):
    import asyncio

    cron._pools.held.clear()
    pending = []
    for attr in ("_pause_refresh_task", "_retry_claim_task"):
        task = getattr(cron, attr, None)
        if task is not None:
            task.cancel()
            pending.append(task)
    await asyncio.gather(*pending, return_exceptions=True)
    await _teardown_cron(cron)


@bench(
    "pools.admit_cycle_1k",
    "pools",
    detail="6 finish-then-tick cycles, each admitting one entry, 1k queued",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.010,
)
def bench_pools_admit_cycle():
    """What a busy pool pays per completion with a long queue behind it.

    The pool is full and 1,000 entries wait.  Each cycle records one
    completion (PoolScheduler.finish) and runs the tick that follows it
    (_tick_pool), which admits exactly the head of the queue: three
    read-modify-writes of the whole pool document, two of them durable
    writes.  A tick that makes futile admission attempts, or a per-entry
    cost added to _maintain or the claim, scales straight through it.  The
    launch is stubbed (job.launch_reap_2k measures it).

    The queue is written as one document, cloned from entries the real
    enqueue produced, so the fixture does not pay 1,000 durable writes.
    """
    import asyncio

    queued = _n(1000, floor=8)
    cycles = 6

    async def run():
        cron, namespace = await _pool_cron("admit")
        pools = cron._pools
        backend = cron.state_backend
        try:
            for job in cron.cron_jobs.values():
                await pools.enqueue(job, payload={"kind": "job"})

            def grow(current):
                body = dict(current)
                templates = list(current["entries"].values())
                entries = {}
                for i in range(queued + _POOL_SLOTS):
                    entry = json.loads(json.dumps(templates[i % 4]))
                    entry["id"] = "%032x" % i
                    entry["queuedAt"] = templates[0]["queuedAt"] + i * 0.001
                    entries[entry["id"]] = entry
                body["entries"] = entries
                return body, sorted(entries)

            _, order = await backend.mutate_document(
                namespace, _POOL_NAME, grow
            )
            tickets = []
            for key in order[:_POOL_SLOTS]:
                tickets.append(await pools.acquire(_POOL_NAME, key))
            if not all(tickets):
                raise RuntimeError("the fixture could not fill the pool")
            admitted = []

            async def launch(job, **kwargs):
                admitted.append(kwargs["pool_ticket"].key)
                return True

            cron.maybe_launch_job = launch
            t0 = time.perf_counter()
            for ticket in tickets[:cycles]:
                await pools.finish(ticket)
                await pools._tick_pool(_POOL_NAME)
            dt = time.perf_counter() - t0
            if admitted != order[_POOL_SLOTS : _POOL_SLOTS + cycles]:
                raise RuntimeError(
                    "%d cycles admitted %d entries, or not the head of the "
                    "queue; the region timed the wrong work"
                    % (cycles, len(admitted))
                )
            body = await backend.read_document(namespace, _POOL_NAME)
            states = [e["state"] for e in body["entries"].values()]
            if states.count("queued") != queued - cycles:
                raise RuntimeError(
                    "%d entries still wait, expected %d"
                    % (states.count("queued"), queued - cycles)
                )
            return dt
        finally:
            await _pool_stop(cron)

    return asyncio.run(run())


@bench(
    "pools.enqueue_burst_200",
    "pools",
    detail="200 enqueues into one pool, each a durable whole-document write",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.015,
)
def bench_pools_enqueue_burst():
    """A burst of work entering one pool.

    Every enqueue rewrites the pool document, so the cost of the n-th grows
    with the n-1 already waiting and a burst is quadratic in its length.  A
    fleet whose pooled jobs share a schedule enqueues exactly such a burst
    once per fire; DAG fan-outs take the same path through admit_task.  The
    metric holds the per-entry factor of that curve: anything added per
    stored entry per operation shows here squared.
    """
    import asyncio

    n = _n(200, floor=8)

    async def run():
        cron, namespace = await _pool_cron("burst")
        pools = cron._pools
        try:
            jobs = list(cron.cron_jobs.values())
            t0 = time.perf_counter()
            for i in range(n):
                await pools.enqueue(jobs[i % 4], payload={"kind": "job"})
            dt = time.perf_counter() - t0
            body = await cron.state_backend.read_document(
                namespace, _POOL_NAME
            )
            states = [e["state"] for e in body["entries"].values()]
            if states != ["queued"] * n:
                raise RuntimeError(
                    "the burst left %d queued entries of %d"
                    % (states.count("queued"), n)
                )
            return dt
        finally:
            await _pool_stop(cron)

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# resources: the per-run CPU/memory accounting that monitorResources turns on.
# Nothing here samples on a timer; the metric is the per-completion final
# reading, which is the only part every monitored run pays exactly once.
# ---------------------------------------------------------------------------


@bench(
    "resources.monitor_stop_100",
    "resources",
    detail="100 ResourceMonitor.stop() final readings (process-table walk)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.020,
)
def bench_resources_monitor_stop():
    """The per-completion process-table walk.

    Every monitored run's stop() takes one last opportunistic reading, and
    without the shared ticker's snapshot to derive from, that reading walks
    the WHOLE process table (psutil's children() call is a full ppid map on
    every platform).  A batch of runs finishing together therefore pays K
    independent full-table scans, the exact cost the shared ticker was
    built to remove from the periodic path, still present on the final one.
    The walk is threaded, so it does not block the loop; what it does cost
    is a worker-thread hop plus a table scan per completion, and no metric
    saw either.

    The monitor is attached to THIS process rather than started against a
    child: start() would register with the loop's shared ticker and its
    background sampling would land inside the timed region non
    deterministically, and the pid under observation does not change the
    shape (the scan is whole-table either way).  Skips without psutil.
    """
    import asyncio

    try:
        from cronstable.resources import ResourceMonitor
    except ImportError as exc:
        raise Skip("cronstable.resources unavailable: %r" % exc) from None
    try:
        import psutil
    except ImportError as exc:
        raise Skip("psutil not installed: %r" % exc) from None
    n = _n(100, floor=2)

    def _monitor():
        # The baseline side of a release comparison runs THIS file against
        # an older install, which may still require the job_name kwarg the
        # current tree dropped.  Construct under either signature, or the
        # metric never compares and expected_gated.txt calls it a dead
        # gate.  A second TypeError is real drift and reaches the Skip.
        try:
            return ResourceMonitor(os.getpid())
        except TypeError:
            return ResourceMonitor(os.getpid(), job_name="bench")

    async def run():
        try:
            probe = _monitor()
        except TypeError as exc:
            raise Skip("ResourceMonitor signature changed: %r" % exc) from None
        if not hasattr(probe, "_proc") or not hasattr(probe, "stop"):
            raise Skip("ResourceMonitor internals not present")
        monitors = []
        for _ in range(n + 1):
            monitor = _monitor()
            # attached WITHOUT start(): see the docstring
            monitor._proc = psutil.Process(os.getpid())
            monitors.append(monitor)
        await monitors[0].stop()  # warm psutil's caches
        if monitors[0]._samples == 0:
            raise RuntimeError(
                "the final reading sampled nothing; the region would time "
                "an early return, not a table walk"
            )
        t0 = time.perf_counter()
        for monitor in monitors[1:]:
            await monitor.stop()
        return time.perf_counter() - t0

    return asyncio.run(run())


# --- resources: the chart series a monitored run leaves behind -------------
#
# A monitorResources run persists a downsampled [t, cpu%, rss] series in its
# ledger record (240 points by default).  Every reader of the ledger parses
# and validates it point by point: the boot rehydrate on the event loop, per
# job and per retained run, and the trends build per poll.
def _series_record(points=240):
    """A ledger run record carrying a ``points``-point resource series."""
    series = [
        [
            1773577800.0 + i,
            round((i * 7919) % 400 / 3.0, 2),
            50000000 + (i * 104729) % 9000000,
        ]
        for i in range(points)
    ]
    return {
        "outcome": "success",
        "exit_code": 0,
        "started_at": "2026-03-15T12:00:00+00:00",
        "finished_at": "2026-03-15T12:05:00+00:00",
        "ranAt": "2026-03-15T12:05:00+00:00",
        "duration": 300.0,
        "fail_reason": None,
        "skip_reason": None,
        "resources": {
            "cpu_user_seconds": 1.5,
            "cpu_system_seconds": 0.5,
            "cpu_total_seconds": 2.0,
            "max_rss_bytes": 59000000,
            "samples": points,
            "series": series,
        },
    }


@bench(
    "resources.usage_from_dict_500",
    "resources",
    detail="ResourceUsage.from_dict x500 records with a 240-point series",
    repeats=(3, 2, 1),
    gate_pct=15.0,
    gate_floor=0.005,
)
def bench_resources_usage_from_dict():
    """Parsing the resource series out of ledger records.

    ResourceUsage.from_dict (and _parse_series under it) runs once per
    record read back from the ledger: 50 records per monitored job at boot,
    on the event loop, and up to 5,000 per trends build.  With the default
    240-point series it is the larger part of rebuilding a run row, so a
    per-point cost added to the validation shows here at full proportion.
    Each record is decoded from JSON first (untimed), as the store hands
    it over.
    """
    try:
        from cronstable.resources import ResourceUsage
    except ImportError as exc:
        raise Skip("cronstable.resources unavailable: %r" % exc) from None
    if not hasattr(ResourceUsage, "from_dict"):
        raise Skip("ResourceUsage.from_dict not present")
    n = _n(500, floor=5)

    def build():
        raw = json.dumps(_series_record()["resources"])
        return [json.loads(raw) for _ in range(n)]

    records = fixture("usage_records_%d" % n, build)
    t0 = time.perf_counter()
    usages = [ResourceUsage.from_dict(record) for record in records]
    dt = time.perf_counter() - t0
    series = getattr(usages[-1], "series", None)
    if None in usages or not series or len(series) != 240:
        raise RuntimeError(
            "from_dict did not rebuild the 240-point series; the region "
            "timed a rejection"
        )
    return dt


# ---------------------------------------------------------------------------
# push: the E2E-encrypted alert path (PyNaCl; skips without the push extra,
# so CI must install pynacl into BOTH perf venvs or this is a dead gate --
# the documented webui/playwright trap).
# ---------------------------------------------------------------------------


class _PushCtx:
    """A failure-kind reporter context, duck-typed like the real ones."""

    def __init__(self, template_vars):
        self.template_vars = template_vars


@bench(
    "push.seal_500",
    "push",
    detail="build + fit + seal 500 failure alerts to a device key",
    repeats=(3, 2, 1),
)
def bench_push_seal():
    """The per-device per-event sealing cost, which bursts exactly during
    incidents (every failure fans out to every paired device).

    The fixture event is a FAILURE ctx with an oversized stderr tail (60
    lines x 120 chars, well over MAX_PLAINTEXT_BYTES), so fit_payload's
    iterative trim loop -- the same iterative-trim class as the fixed
    env-interpolation quadratic -- actually executes; a tail-less event
    times ~90% pinned libsodium C that cronstable code cannot regress.
    Skips (never fails) when PyNaCl is absent; not in the never-skip net
    for the same reason as the orjson twin (optional dependency).
    """
    try:
        from cronstable import push as push_mod
    except ImportError as exc:
        raise Skip("cronstable.push unavailable: %r" % exc) from None
    if not getattr(push_mod, "HAVE_PYNACL", False):
        raise Skip("PyNaCl not installed (push extra)")
    for attr in ("build_payload", "fit_payload", "seal_to_device"):
        if not hasattr(push_mod, attr):
            raise Skip("cronstable.push lacks %s" % attr)
    from nacl.public import PrivateKey

    n = _n(500)
    public_key = base64.b64encode(
        bytes(PrivateKey.generate().public_key)
    ).decode("ascii")
    stderr_tail = "\n".join(
        "line %03d " % i + "x" * 110 for i in range(60)
    )
    ctx = _PushCtx(
        {
            "name": "bench-job",
            "host": "bench-host",
            "run_id": "r-000123",
            "schedule": "*/5 * * * *",
            "started_at": "2026-07-01T10:00:00+00:00",
            "exit_code": 1,
            "fail_reason": "exited with status 1",
            "stderr": stderr_tail,
        }
    )
    try:
        probe = push_mod.build_payload(ctx, False, True)
        fitted = push_mod.fit_payload(probe)
    except TypeError as exc:
        raise Skip("push payload signature changed: %r" % exc) from None
    if len(fitted) > push_mod.MAX_PLAINTEXT_BYTES:
        raise RuntimeError("fit_payload left the plaintext over the cap")
    if len(probe.get("log_tail") or ()) >= 60:
        raise RuntimeError(
            "the oversized tail was not trimmed; the region is not "
            "exercising the fit loop"
        )
    t0 = time.perf_counter()
    for _ in range(n):
        payload = push_mod.build_payload(ctx, False, True)
        data = push_mod.fit_payload(payload)
        push_mod.seal_to_device(public_key, data)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# push: the alert fan-out and the post-quantum sealing arm.  push.seal_500
# calls the payload and X25519 sealing functions directly; these cover the
# service entry point the reporter calls and the X-Wing suite.
# ---------------------------------------------------------------------------
def _push_failure_ctx():
    """The failure context push.seal_500 uses: an oversized stderr tail, so
    the per-device fit loop runs."""
    stderr_tail = "\n".join("line %03d " % i + "x" * 110 for i in range(60))
    return _PushCtx(
        {
            "name": "bench-job",
            "host": "bench-host",
            "run_id": "r-000123",
            "schedule": "*/5 * * * *",
            "started_at": "2026-07-01T10:00:00+00:00",
            "exit_code": 1,
            "fail_reason": "exited with status 1",
            "stderr": stderr_tail,
        }
    )


@bench(
    "push.fanout_8x100",
    "push",
    detail="send_report x100 to 8 paired X25519 devices, relay POST stubbed",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_push_fanout():
    """One failure alert from the reporter's entry point to the relay
    boundary, for a household of eight phones.

    Per alert: the registry freshness check, the payload build, the
    coalescing id, a client session, and per device a private copy of the
    payload fitted to that device's suite and sealed to its key.  The
    POST to the relay is replaced with a stub, so the region stops at the
    network.  The sealing is the same work push.seal_500 times; what this
    adds is everything PushService wraps around it, which is where a
    per-alert registry read or a per-device session would land.
    """
    import asyncio

    try:
        from cronstable import push as push_mod
    except ImportError as exc:
        raise Skip("cronstable.push unavailable: %r" % exc) from None
    if not getattr(push_mod, "HAVE_PYNACL", False):
        raise Skip("PyNaCl not installed (push extra)")
    for attr in ("PushService", "FileDeviceStore"):
        if not hasattr(push_mod, attr):
            raise Skip("cronstable.push lacks %s" % attr)
    from nacl.public import PrivateKey

    devices = 8
    alerts = _n(100)

    def build():
        records = [
            {
                "id": "dev%02d" % i,
                "name": "phone %d" % i,
                "platform": "ios",
                "pushToken": "%064x" % i,
                "publicKey": base64.b64encode(
                    bytes(PrivateKey.from_seed(bytes([i + 1]) * 32).public_key)
                ).decode("ascii"),
                "suite": "x25519",
                "createdAt": "2026-07-01T10:00:%02d+00:00" % i,
                "createdBy": "bench",
            }
            for i in range(devices)
        ]
        path = os.path.join(_tmpdir(), "push-devices.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(
                {"version": 1, "devices": records, "collapseSalt": "ab" * 16},
                handle,
            )
        return path

    path = fixture("push_devices_file_8", build)
    ctx = _push_failure_ctx()
    report_config = {"enabled": True, "includeLogTail": True}

    async def run():
        try:
            service = push_mod.PushService(
                relay_url="https://relay.invalid/push",
                relay_timeout=5.0,
                store=push_mod.FileDeviceStore(path),
                host="bench-host",
            )
        except TypeError as exc:
            raise Skip("PushService signature changed: %r" % exc) from None
        if not hasattr(service, "_post_envelope"):
            raise Skip("PushService._post_envelope not present")
        posted = [0]

        async def post(session, body, outcome):
            outcome["error"] = None
            outcome["status"] = 200
            posted[0] += 1

        service._post_envelope = post
        # untimed: the one registry read, and the first send's imports
        await service.refresh(force=True)
        await service.send_report(ctx, False, report_config)
        before = posted[0]
        t0 = time.perf_counter()
        for _ in range(alerts):
            await service.send_report(ctx, False, report_config)
        dt = time.perf_counter() - t0
        if posted[0] - before != alerts * devices:
            raise RuntimeError(
                "%d alerts reached the relay stub %d times for %d devices"
                % (alerts, posted[0] - before, devices)
            )
        return dt

    return asyncio.run(run())


@bench(
    "push.seal_xwing_500",
    "push",
    detail="build + fit + seal 500 failure alerts to an X-Wing device key",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_push_seal_xwing():
    """push.seal_500 for a device paired under the post-quantum suite.

    An X-Wing ciphertext is 1088 bytes wider than a sealed box, so the
    same alert is fitted to a smaller plaintext budget (more of the log
    tail goes) and sealed with single-shot HPKE over ML-KEM-768 plus
    X25519.  Skips where cryptography cannot seal the suite.
    """
    try:
        from cronstable import push as push_mod
    except ImportError as exc:
        raise Skip("cronstable.push unavailable: %r" % exc) from None
    for attr in (
        "build_payload",
        "fit_payload",
        "seal_to_device",
        "max_plaintext_bytes",
        "sealable_suites",
    ):
        if not hasattr(push_mod, attr):
            raise Skip("cronstable.push lacks %s" % attr)
    suite = getattr(push_mod, "SUITE_XWING", "xwing")
    if suite not in push_mod.sealable_suites():
        raise Skip("this install cannot seal the xwing suite")
    try:
        from cryptography.hazmat.primitives.asymmetric import mlkem, x25519
    except ImportError as exc:
        raise Skip(
            "cryptography X-Wing primitives missing: %r" % exc
        ) from None

    def build():
        wire = (
            mlkem.MLKEM768PrivateKey.generate().public_key().public_bytes_raw()
            + x25519.X25519PrivateKey.generate()
            .public_key()
            .public_bytes_raw()
        )
        return base64.b64encode(wire).decode("ascii")

    public_key = fixture("push_xwing_key", build)
    n = _n(500)
    ctx = _push_failure_ctx()
    limit = push_mod.max_plaintext_bytes(suite)
    probe = push_mod.build_payload(ctx, False, True)
    fitted = push_mod.fit_payload(probe, limit)
    if len(fitted) > limit:
        raise RuntimeError("fit_payload left the plaintext over the cap")
    if len(probe.get("log_tail") or ()) >= 40:
        raise RuntimeError(
            "the oversized tail was not trimmed; the region is not "
            "exercising the fit loop"
        )
    t0 = time.perf_counter()
    for _ in range(n):
        payload = push_mod.build_payload(ctx, False, True)
        data = push_mod.fit_payload(payload, limit)
        push_mod.seal_to_device(public_key, data, suite)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# memory: deterministic traced allocations plus real child-process RSS.
# ---------------------------------------------------------------------------


@bench(
    "mem.crontab_10k",
    "memory",
    detail="traced MB held by 10k parsed CronTabs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_crontab():
    CronTab = _crontab_cls()
    exprs = _varied_exprs(_n(10000))
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        tabs = [CronTab(e) for e in exprs]
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del tabs
    return (after - before) / 1048576.0


@bench(
    "mem.gc_pause_100k",
    "memory",
    detail="one full gc.collect(2) with a 100k-job set resident",
    gate_pct=25.0,
    gate_floor=0.005,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_gc_pause():
    """The steady-state stop-the-world pause at the marketed fleet scale.

    The collector walks tracked CONTAINERS, not bytes, so this is the one
    memory metric that moves on object COUNT: a change that halves a job
    set's footprint while doubling the number of dicts and tuples it holds
    reads as a win on mem.crontab_10k / mem.jobconfig_2k and as a
    regression here.  It is also the only metric anywhere that measures the
    collector itself, which every other timed region in the suite runs with
    disabled (see config.reload_gc_100k).

    The harness already ran a full collect immediately before this call, so
    the pass measured here frees nothing: it is the pure traversal of a
    resident fleet, which is exactly the pause a live daemon pays on its
    event loop every time gen 2 comes due.  gc.collect() is explicit and
    runs whether or not the collector is enabled, so the harness's
    gc.disable() is left alone here.
    """
    n = _n(100000)
    jobs = fixture("gc_pause_job_map_100k", lambda: _gc_job_map(n))
    if len(jobs) != n:
        raise RuntimeError(
            "resident job set holds %d entries, expected %d" % (len(jobs), n)
        )
    t0 = time.perf_counter()
    gc.collect(2)
    return time.perf_counter() - t0


@bench(
    "mem.jobconfig_2k",
    "memory",
    detail="traced MB held by 2k JobConfigs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_jobconfig():
    raws = _job_dicts(_n(2000))
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        jobs = [JobConfig(mergedicts(DEFAULT_CONFIG, raw)) for raw in raws]
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del jobs
    return (after - before) / 1048576.0


# The value is about 0.12 MB, so the floor sits well under it and the 15%
# limit is what binds.
@bench(
    "mem.retired_output_1k",
    "memory",
    detail="traced MB retained by 1k superseded, formerly full log streams",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.01,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_retired_output():
    from cronstable.job import JobOutputStream

    if not hasattr(JobOutputStream, "release_lines"):
        raise Skip("JobOutputStream.release_lines unavailable")
    n = _n(1000)
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        streams = []
        for _ in range(n):
            stream = JobOutputStream(limit=1000)
            for _ in range(1000):
                stream.publish("stdout", "a captured line\n")
            stream.close()
            stream.release_lines()
            streams.append(stream)
        gc.collect()
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del streams
    return (after - before) / 1048576.0


_RSS_WRAPPER = (
    "import resource,subprocess,sys\n"
    "r=subprocess.run(sys.argv[1:],stdout=subprocess.DEVNULL,"
    "stderr=subprocess.DEVNULL)\n"
    "print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)\n"
    "sys.exit(r.returncode)\n"
)


def _child_peak_rss_mb(args):
    """Peak RSS in MB of one child process, POSIX only.

    A wrapper child runs the target and reports getrusage(RUSAGE_CHILDREN),
    which is scoped to the wrapper's own children, so earlier benchmark
    subprocesses cannot pollute the reading.
    """
    if sys.platform == "win32":
        raise Skip("peak-RSS benchmark requires POSIX getrusage")
    proc = subprocess.run(
        [sys.executable, "-c", _RSS_WRAPPER, sys.executable] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=_child_env(),
        cwd=_tmpdir(),
    )
    if proc.returncode != 0:
        raise Skip("child exited %d: %s" % (proc.returncode, " ".join(args)))
    raw = int(proc.stdout.split()[0])
    # ru_maxrss is bytes on macOS, KiB on Linux and the BSDs.
    return raw / 1048576.0 if sys.platform == "darwin" else raw / 1024.0


@bench(
    "mem.rss_version",
    "memory",
    detail="peak RSS of cronstable --version",
    unit="MB",
    gate_pct=25.0,
    gate_floor=3.0,
    compare="median",
    repeats=(5, 2, 1),
    subprocess=True,
)
def bench_rss_version():
    return _child_peak_rss_mb(["-m", "cronstable", "--version"])


@bench(
    "mem.rss_daemon_import",
    "memory",
    detail="peak RSS of importing the full daemon graph",
    unit="MB",
    gate_pct=25.0,
    gate_floor=3.0,
    compare="median",
    repeats=(5, 2, 1),
    subprocess=True,
)
def bench_rss_daemon():
    return _child_peak_rss_mb(["-c", "import cronstable.cron"])


@bench(
    "mem.run_history_series_5",
    "memory",
    detail="traced MB of 5 monitored jobs' run history (50 runs, 240 points)",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_run_history_series():
    """What a monitored job's retained run history holds.

    The in-memory history keeps 50 finished runs per job, and each row of a
    monitorResources job keeps its chart series for the resources endpoint.
    At the default 240 points that is about 2 MB per job, a hundred times
    what an unmonitored job holds, for the life of the daemon.
    The rows are rebuilt through the boot rehydrate's own parser
    (_job_run_info_from_dict), so a fatter point, row or stream object
    shows here.
    """
    try:
        from collections import deque

        from cronstable.cron import _job_run_info_from_dict
    except ImportError as exc:
        raise Skip("run-history rehydrate API unavailable: %r" % exc) from None
    jobs = _n(5, floor=1)
    retained = 50
    raw = json.dumps(_series_record())
    try:
        probe = _job_run_info_from_dict(json.loads(raw))
    except TypeError as exc:
        raise Skip("_job_run_info_from_dict signature changed: %r" % exc)
    usage = getattr(probe, "resource_usage", None)
    if usage is None or not getattr(usage, "series", None):
        raise Skip("run records do not carry a resource series")
    del probe, usage
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        history = []
        for _ in range(jobs):
            ring = deque(maxlen=retained)
            for _ in range(retained):
                ring.append(_job_run_info_from_dict(json.loads(raw)))
            history.append(ring)
        gc.collect()
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    rows = sum(len(ring) for ring in history)
    del history
    if rows != jobs * retained:
        raise RuntimeError("built %d of %d rows" % (rows, jobs * retained))
    return (after - before) / 1048576.0


# --- a booted daemon in a child process ------------------------------------
#
# The child builds Cron(<config file>), switches the launch off, and spins
# Cron.run() through the next_sleep_interval seam (the one loop.idle_wake_rate
# uses) for a fixed number of idle passes, then shuts down.  It imports the
# cronstable its interpreter resolves, as _timed_child arranges.  Exit codes:
# 3 the passes did not run, 4 a seam is absent, 5 the job set or the web
# listener is not what the config asked for.
_DAEMON_CHILD = r"""
import asyncio
import sys

import cronstable.cron as cron_mod

entry, passes, jobs, web = sys.argv[1:5]
passes, jobs, web = int(passes), int(jobs), web == "web"
if not hasattr(cron_mod, "next_sleep_interval") or not hasattr(
    cron_mod.Cron, "_launch_plan"
):
    sys.exit(4)
state = {"n": 0, "ok": False}


async def run():
    cron = cron_mod.Cron(entry)

    async def _capture(plan):
        pass

    cron._launch_plan = _capture

    def _spin(subminute=False, now=None):
        state["n"] += 1
        if state["n"] >= passes:
            state["ok"] = len(cron.cron_jobs) == jobs and (
                not web or cron.web_runner is not None
            )
            cron.signal_shutdown()
        return 0.0

    cron_mod.next_sleep_interval = _spin
    await asyncio.wait_for(cron.run(), timeout=300.0)


asyncio.run(run())
if state["n"] < passes:
    sys.exit(3)
sys.exit(0 if state["ok"] else 5)
"""


def _daemon_config(n, web=False):
    """A config FILE of ``n`` jobs, optionally with a loopback listener."""

    def build():
        path = os.path.join(
            _tmpdir(), "daemon-%d-%s.yaml" % (n, "web" if web else "plain")
        )
        text = _config_yaml(n)
        if web:
            # port 0: the OS assigns one, so parallel runs cannot collide
            text = "web:\n  listen:\n    - http://127.0.0.1:0\n" + text
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        return path

    return fixture("daemon_config_%d_%s" % (n, web), build)


def _daemon_child_args(n, passes, web=False):
    return [
        "-c",
        _DAEMON_CHILD,
        _daemon_config(n, web=web),
        str(passes),
        str(n),
        "web" if web else "plain",
    ]


@bench(
    "mem.rss_daemon_10k",
    "memory",
    detail="peak RSS of a daemon booted on a 10k-job config, 5 idle passes",
    unit="MB",
    gate_pct=25.0,
    gate_floor=5.0,
    compare="median",
    repeats=(1, 1, 1),
    subprocess=True,
)
def bench_rss_daemon_jobs():
    """Peak resident memory of a running daemon with a large job set.

    mem.rss_daemon_import stops at the import; this boots the daemon on a
    10k-job config file and runs five idle passes, so the reading holds the
    config parse, the job set, the schedule index and everything the first
    housekeeping pass builds.  About two thirds of it scales with the job
    count, which is what a per-job retention anywhere in the boot moves.

    One repeat: the child takes about four seconds (the YAML parse of 10k
    jobs), and peak RSS repeats within a megabyte.
    """
    return _child_peak_rss_mb(_daemon_child_args(_n(10000, floor=20), 5))


@bench(
    "mem.rss_daemon_web_100",
    "memory",
    detail="peak RSS of a daemon with a loopback web listener, 100 jobs",
    unit="MB",
    gate_pct=25.0,
    gate_floor=3.0,
    compare="median",
    repeats=(1, 1, 1),
    subprocess=True,
)
def bench_rss_daemon_web():
    """Peak resident memory of a small daemon that serves the web API.

    A web listener is the first thing that loads aiohttp and builds the
    application, the routes and the TLS-free site, so this is the footprint
    of the most common deployment: a modest job set with the dashboard on.
    The listener binds 127.0.0.1 on an OS-assigned port.  The job set is
    small on purpose, so the reading is the daemon's fixed cost with the web
    stack loaded.
    """
    return _child_peak_rss_mb(
        _daemon_child_args(_n(100, floor=10), 5, web=True)
    )


@bench(
    "mem.launch_reap_steady_20x55",
    "memory",
    detail="traced MB held after 55 launch-and-reap cycles of 20 jobs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.02,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_launch_reap_steady():
    """What the daemon still holds once every job has run past its history.

    Twenty jobs run 55 times each through the real launch and reap path
    (the job.launch_reap_2k herd), five more than the run-history ring
    keeps, so the reading is the steady state: one full ring of run rows
    per job, the newest run's output stream, and whatever else a finished
    run leaves behind.  A reference kept per run (a leak) grows the value
    with the cycle count, and a fatter row or a finished batch pinned by
    the idle reaper raises it outright.
    """
    import asyncio

    n = _n(20, floor=2)
    cycles = 55
    jobs = _herd_jobs(n)
    cron = _herd_cron()
    if not hasattr(cron, "run_history"):
        raise Skip("Cron.run_history not present")
    cron.cron_jobs = jobs
    plan = [(job, [_NOW]) for job in jobs.values()]
    counts = {"spawned": 0}

    async def run():
        reaper = asyncio.create_task(cron._wait_for_running_jobs())
        await asyncio.sleep(0)
        gc.collect()
        tracemalloc.start()
        try:
            before, _ = tracemalloc.get_traced_memory()
            for _ in range(cycles):
                await _herd_cycle(cron, plan, counts)
            # let the reaper park and the last done callbacks run
            for _ in range(5):
                await asyncio.sleep(0)
            gc.collect()
            after, _ = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        rows = sorted(len(ring) for ring in cron.run_history.values())
        await _herd_stop(cron, reaper)
        return (after - before) / 1048576.0, rows

    held, rows = asyncio.run(run())
    if counts["spawned"] != n * cycles or len(rows) != n or rows[0] < 50:
        raise RuntimeError(
            "%d of %d runs were spawned and the shortest history holds %s "
            "rows; the steady state was not reached"
            % (counts["spawned"], n * cycles, rows[:1])
        )
    return held


def _gc_manifest_store(records, names):
    """A store whose manifest stream holds ``records`` manifests, each
    naming ``names`` jobs, plus the YAML that boots a Cron on it."""

    def build():
        import asyncio

        Cron = _cron_cls()
        try:
            from cronstable.config import parse_config_string
        except ImportError as exc:
            raise Skip("parse_config_string unavailable: %r" % exc) from None
        if not hasattr(Cron, "_manifest_stream"):
            raise Skip("Cron._manifest_stream not present")
        path = os.path.join(_tmpdir(), "gc-manifests")
        os.makedirs(path, exist_ok=True)
        text = (
            "state:\n  path: %s\n  jobApi:\n    enabled: false\n"
            "jobs:\n  - name: live\n    command: 'true'\n"
            "    schedule: '0 4 * * *'\n" % path.replace("\\", "/")
        )
        state_config = parse_config_string(text, "").state_config
        jobs = ["job%05d" % i for i in range(names)]
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)

        async def seed():
            cron = Cron(None, config_yaml=text)
            backend = _state_backend(path)
            await backend.start()
            try:
                stream = cron._manifest_stream()
                for r in range(records):
                    await backend.append_record(
                        stream,
                        {
                            "jobSetId": "0" * 64,
                            "host": cron._state_host,
                            "jobs": jobs,
                            "scopes": ["global"],
                            "dags": [],
                            "at": (base + timedelta(hours=6 * r)).isoformat(),
                        },
                    )
            finally:
                await backend.stop()

        asyncio.run(seed())
        return text, state_config

    return fixture("gc_manifest_store_%dx%d" % (records, names), build)


@bench(
    "mem.gc_manifest_scan_128x2k",
    "memory",
    detail="traced peak MB of one GC pass over 128 manifests of 2k job names",
    unit="MB",
    gate_pct=15.0,
    gate_floor=1.0,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_gc_manifest_scan():
    """The transient a state garbage-collection pass allocates.

    _collect_state_garbage runs after every boot and then daily.  It reads
    every retained manifest of every host (up to 512 each) before it folds
    the ones inside the grace window, and each manifest lists every job
    name, so the peak grows with hosts x manifests x jobs.  The reading is
    tracemalloc's peak over one real pass; a pass that holds more per
    manifest, or reads a stream twice, raises it.
    """
    import asyncio

    Cron = _cron_cls()
    if not hasattr(Cron, "_collect_state_garbage"):
        raise Skip("Cron._collect_state_garbage not present")
    records = _n(128, floor=4)
    names = _n(2000, floor=20)
    text, state_config = _gc_manifest_store(records, names)

    async def run():
        try:
            cron = Cron(None, config_yaml=text)
        except TypeError as exc:
            raise Skip("Cron signature changed: %r" % exc) from None
        try:
            await cron.start_stop_state(state_config)
            backend = cron.state_backend
            if backend is None:
                raise RuntimeError("the GC fixture did not boot a backend")
            seen = []
            real_list = backend.list_records

            async def spying_list(stream, **kwargs):
                found = await real_list(stream, **kwargs)
                if stream == cron._manifest_stream():
                    seen.append(len(found))
                return found

            backend.list_records = spying_list
            gc.collect()
            tracemalloc.start()
            try:
                before, _ = tracemalloc.get_traced_memory()
                tracemalloc.reset_peak()
                await cron._collect_state_garbage()
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            backend.list_records = real_list
            kept = await real_list(cron._manifest_stream())
            if seen != [records] or len(kept) != records:
                raise RuntimeError(
                    "the pass read %r manifests and left %d of %d; the "
                    "region did not scan the stream"
                    % (seen, len(kept), records)
                )
            return (peak - before) / 1048576.0
        finally:
            for attr in ("_pause_refresh_task", "_retry_claim_task"):
                task = getattr(cron, attr, None)
                if task is not None:
                    task.cancel()
            await _teardown_cron(cron)

    return asyncio.run(run())


@bench(
    "mem.parse_peak_rich_20",
    "memory",
    detail="traced peak MB while parsing 20 production-shaped jobs + 2 DAGs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.25,
    compare="median",
    repeats=(1, 1, 1),
)
def bench_mem_parse_peak_rich():
    """The transient a config load allocates, which sets the daemon's RSS.

    Parsing holds the YAML document, strictyaml's validated copy of it and
    one wrapper per node until the load returns: about 100 times the file
    size.  That peak is what a boot or a reload needs from a memory-limited
    container, and mem.jobconfig_2k, which builds JobConfigs from dicts,
    never allocates any of it.
    """
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    jobs, dags, tasks = _rich_shape(20)
    text = _config_yaml_rich(jobs, dags, tasks)
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        parsed = parse_config_string(text, "")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if len(parsed.jobs) != jobs:
        raise RuntimeError(
            "rich parse yielded %d jobs, expected %d"
            % (len(parsed.jobs), jobs)
        )
    del parsed
    return (peak - before) / 1048576.0


@bench(
    "mem.jobconfig_envfile_2k",
    "memory",
    detail="traced MB held by 2k JobConfigs sharing a 40-variable env_file",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_jobconfig_envfile():
    """What a shared env_file adds to every job that inherits it.

    The file is read once per document, and each JobConfig then holds its
    own list of key/value entries built from it: about 8 KB a job for 40
    variables, eight times the rest of a plain JobConfig.  The env_file
    usually arrives through ``defaults:``, so the whole fleet pays, and
    mem.jobconfig_2k, whose jobs name no env_file, never sees it.
    """
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    env_path = os.path.join(_tmpdir(), "bench-shared.env")
    if not os.path.exists(env_path):
        with open(env_path, "w", encoding="utf-8", newline="\n") as handle:
            for i in range(40):
                handle.write("SHARED_%02d=value-%d\n" % (i, i))
    raws = _job_dicts(_n(2000, 4))
    for raw in raws:
        raw["env_file"] = env_path
    env_cache = {}
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        try:
            jobs = [
                JobConfig(mergedicts(DEFAULT_CONFIG, raw), env_cache=env_cache)
                for raw in raws
            ]
        except TypeError as exc:
            raise Skip("JobConfig takes no env_cache: %r" % exc) from None
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if len(jobs[-1].environment) != 40 or len(env_cache) != 1:
        raise RuntimeError(
            "env_file merge gave %d variables from %d cached files"
            % (len(jobs[-1].environment), len(env_cache))
        )
    del jobs
    return (after - before) / 1048576.0


# ---------------------------------------------------------------------------
# memory: what a node retains per gossip round.
# ---------------------------------------------------------------------------
@bench(
    "mem.cluster_view_50x400",
    "memory",
    detail="traced MB held after one gossip round, 49 peers x 400 summaries",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_cluster_view():
    """The peer table a node of a 50-member fleet keeps resident.

    Every node holds every peer's validated job summaries for the fleet
    view, plus the member lists and name sets the election reads, for as
    long as it runs.  One poll round over a fresh manager fills that
    state; the traced growth is what the round left behind.  It moves if
    absorption keeps the raw body, a second copy of the parsed one, or a
    wider per-job entry.
    """
    import asyncio

    members = 50
    jobs = _n(400)
    cluster_mod, mgr, names, hosts = _gossip_manager(members, "vnode")
    if not hasattr(mgr, "_poll_all") or not hasattr(mgr, "_session"):
        raise Skip("ClusterManager poll seam not present")
    bodies = fixture(
        "mem_gossip_bodies_50x400",
        lambda: _gossip_bodies(cluster_mod, names, hosts, jobs),
    )
    mgr._session = _GossipSession(bodies)

    async def run():
        gc.collect()
        tracemalloc.start()
        try:
            before, _ = tracemalloc.get_traced_memory()
            await mgr._poll_all()
            gc.collect()
            after, _ = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        return (after - before) / 1048576.0

    held = asyncio.run(run())
    _gossip_round_done(cluster_mod, mgr, hosts, jobs)
    return held


# ---------------------------------------------------------------------------
# memory: the dashboard's own state.  Group "memory", so it belongs beside
# the other mem.* benchmarks; it uses the tui helpers above.
# ---------------------------------------------------------------------------
@bench(
    "mem.tui_state_5k",
    "memory",
    detail="traced MB the dashboard holds beside a 5k-job payload: poll "
    "folds, one frame, a scrolled 5k-line log drawer",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.25,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_tui_state():
    """What the dashboard itself retains at fleet scale, with the decoded
    ``/jobs`` payload left out.

    The payload is decoded before tracing starts, because its size belongs
    to the daemon and ``webapi.jobs_bytes_500`` gates it at the source.
    What is traced is the client's own state: the by-name index, the view,
    the per-poll folds and failure diff, one painted frame, a full log
    buffer, and the per-line ANSI memo after the drawer has shown every
    buffered line once.
    """
    import asyncio

    tui = _tui_module()
    for attr in ("LogTail", "sanitize_log_line"):
        if not hasattr(tui, attr):
            raise Skip("cronstable.tui lacks %s" % attr)
    if not hasattr(tui.TuiApp, "_drawer_logs"):
        raise Skip("TuiApp._drawer_logs not present")
    jobs = fixture("mem_tui_jobs_5k", lambda: _tui_jobs(_n(5000, floor=80)))
    buffered = _n(5000, floor=100)

    class _Daemon:
        url = "http://127.0.0.1:1"
        token = None

        async def get_json(self, path, timeout_s=10.0):
            return jobs if path == "/jobs" else {}

    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        app = _tui_board(tui, [], 200, 60, _TuiSink(), api=_Daemon())
        if not hasattr(app, "_poll_once"):
            raise Skip("TuiApp._poll_once not present")
        asyncio.run(app._poll_once())
        app.paint()
        tail = tui.LogTail(None, "/jobs/bench/logs", "bench", lambda: None)
        tail.lines = [
            (
                "stderr" if i % 5 == 0 else "stdout",
                tui.sanitize_log_line(line),
                1700000000.0 + i,
            )
            for i, line in enumerate(_tui_log_lines(buffered))
        ]
        app.log_tail = tail
        # the log pane, a page at a time: tracing slows a full frame too
        # much to scroll the whole buffer through app.paint()
        paint = tui.Painter(app.theme)
        page = 500
        for scroll in range(0, buffered, page):
            app.log_scroll = scroll
            app._drawer_logs(paint, 120, page + 1)
        after, _ = tracemalloc.get_traced_memory()
        memo = len(app._ansi_cache) if hasattr(app, "_ansi_cache") else -1
    finally:
        tracemalloc.stop()
    if len(app.view) != len(jobs) or 0 <= memo < buffered // 2:
        raise RuntimeError(
            "state holds a %d-row view and %d memoized lines for %d jobs "
            "and %d buffered lines"
            % (len(app.view), memo, len(jobs), buffered)
        )
    del app, tail
    return (after - before) / 1048576.0


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _cronstable_meta():
    meta = {"version": None, "orjson": False, "uvloop": False, "isal": False}
    try:
        from cronstable.version import version as ver

        meta["version"] = str(ver)
    except Exception:
        try:
            from importlib.metadata import version as md_version

            meta["version"] = md_version("cronstable")
        except Exception:
            pass
    try:
        import orjson  # noqa: F401

        meta["orjson"] = True
    except ImportError:
        pass
    # Recorded, not exercised: every shipped Linux binary and Docker image
    # prefers uvloop, while the whole suite runs on stock asyncio (a full
    # uvloop lane is waived in benchmarks/README.md).  Stamping the flag
    # keeps the result document honest about what the harness could not see,
    # and lets compare.py refuse a pairing where the two sides differ.
    try:
        import uvloop  # noqa: F401

        meta["uvloop"] = True
    except ImportError:
        pass
    # isal swaps the gzip backend behind every compressed-size and gzip-time
    # metric.  Probed without importing it: the daemon loads it on first
    # use, and the harness must leave that to the code under test.
    import importlib.util

    meta["isal"] = importlib.util.find_spec("isal") is not None
    return meta


def _run_one(spec):
    # Wall clock for the WHOLE benchmark -- fixture builds, warm-ups, and
    # repeats -- stamped into the result row.  Fixtures are paid once per
    # process and rounds re-run the process, so a 10-second fixture is a
    # hundred seconds of CI; without a per-benchmark figure the job's
    # timeout ceiling cannot be triaged until it fails a release.
    t_started = time.perf_counter()
    reps = _reps(spec["repeats"])
    values = []
    error = None
    # Untimed warm-up passes, discarded: page in code/data and let the CPU
    # settle so first-call effects never land in the measured distribution.  A
    # warm-up that raises the same Skip/error the measured pass would raise
    # short-circuits to the skip path without running the timed loop.
    for _ in range(_warmups()):
        gc.collect()
        try:
            spec["fn"]()
        except Skip as exc:
            error = str(exc)
            break
        except Exception as exc:
            error = "error: %r" % exc
            break
    for _ in range(reps if error is None else 0):
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            values.append(float(spec["fn"]()))
        except Skip as exc:
            error = str(exc)
            break
        except Exception as exc:  # a broken benchmark must not kill the run
            error = "error: %r" % exc
            break
        finally:
            if gc_was_enabled:
                gc.enable()
    result = {
        "name": spec["name"],
        "group": spec["group"],
        "detail": spec["detail"],
        "unit": spec["unit"],
        "gate_pct": spec["gate_pct"],
        "gate_floor": spec["gate_floor"],
        "compare": spec["compare"],
        "info": spec["info"],
    }
    if not values:
        result.update({"skipped": True, "reason": error or "no data"})
        result["elapsed_seconds"] = round(time.perf_counter() - t_started, 3)
        return result
    value = (
        min(values) if spec["compare"] == "min" else statistics.median(values)
    )
    result.update(
        {
            "skipped": False,
            "reason": None,
            "runs": len(values),
            "values": values,
            "value": value,
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "min": min(values),
            "max": max(values),
        }
    )
    result["elapsed_seconds"] = round(time.perf_counter() - t_started, 3)
    return result


def _fmt(value, unit):
    if unit == "MB":
        return "%.2f MB" % value
    if unit == "KB":
        return "%.2f KB" % value
    if value < 0.001:
        return "%.1f us" % (value * 1e6)
    if value < 1.0:
        return "%.2f ms" % (value * 1e3)
    return "%.3f s" % value


def _stabilize():
    """Best-effort: pin to one CPU and raise priority to cut scheduling jitter.

    Every step is optional and independently guarded: a platform (or a runner
    without the privilege) that refuses one simply runs without it.  Pinning
    the parent also pins the children it spawns, so the startup-tier subprocess
    timings inherit the same steady core.  Returns the labels applied, for the
    result document's provenance.
    """
    applied = []
    try:
        import psutil

        proc = psutil.Process()
        ncpu = os.cpu_count() or 1
        try:
            if ncpu >= 2 and hasattr(proc, "cpu_affinity"):
                core = ncpu - 1
                proc.cpu_affinity([core])
                applied.append("affinity=cpu%d" % core)
        except Exception:
            pass
        try:
            if sys.platform == "win32":
                proc.nice(psutil.HIGH_PRIORITY_CLASS)
                applied.append("priority=high")
            else:
                os.nice(-5)  # needs CAP_SYS_NICE; ignored where unavailable
                applied.append("nice=-5")
        except Exception:
            pass
    except Exception:
        pass
    if applied:
        print(
            "note: perf environment pinned: %s" % ", ".join(applied),
            file=sys.stderr,
        )
    return applied


def main(argv=None):
    global _MODE, _WARMUP_OVERRIDE
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", help="write results to this JSON file")
    parser.add_argument(
        "--quick", action="store_true", help="roughly 10x smaller workloads"
    )
    parser.add_argument(
        "--smoke", action="store_true", help="minimal workloads, for tests"
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="run benchmarks whose name or group contains this substring",
    )
    parser.add_argument(
        "--tier",
        choices=["all", "inprocess", "subprocess"],
        default="all",
        help="run only the in-process tier or only the subprocess "
        "(startup / peak-RSS) tier; the two have different noise profiles "
        "and CI runs them with different round counts",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=None,
        metavar="N",
        help="override the per-mode untimed warm-up passes (default: "
        "1 full/quick, 0 smoke)",
    )
    parser.add_argument(
        "--no-stabilize",
        action="store_true",
        help="do not pin CPU affinity or raise process priority",
    )
    parser.add_argument(
        "--list", action="store_true", help="list benchmarks and exit"
    )
    args = parser.parse_args(argv)
    _MODE = "smoke" if args.smoke else "quick" if args.quick else "full"
    _WARMUP_OVERRIDE = args.warmup
    _ensure_importable()

    if args.list:
        for spec in _BENCHMARKS:
            print(
                "%-28s %-12s %s"
                % (spec["name"], spec["group"], spec["detail"])
            )
        return 0

    def _in_tier(spec):
        if args.tier == "all":
            return True
        return bool(spec.get("subprocess")) == (args.tier == "subprocess")

    selected = [
        spec
        for spec in _BENCHMARKS
        if _in_tier(spec)
        and (
            not args.only
            or any(s in spec["name"] or s in spec["group"] for s in args.only)
        )
    ]
    if not selected:
        print(
            "no benchmark matches %r (tier=%s)" % (args.only, args.tier),
            file=sys.stderr,
        )
        return 2

    print(
        "note: measuring the cronstable package at %s" % _measured_package(),
        file=sys.stderr,
    )
    stabilized = [] if args.no_stabilize else _stabilize()
    meta = _cronstable_meta()
    started = time.perf_counter()
    results = []
    prev_group = None
    for spec in selected:
        # Release the finished group's fixtures before starting the next one.
        # Fixtures are large (100k CronTabs, 100k-job configs) and keyed within
        # a group; without this the cache held the UNION of every group's
        # fixtures resident for the whole suite. Benchmarks are registered
        # grouped, so a group change is a safe eviction boundary (a fixture a
        # later group still needs is simply rebuilt, untimed).  Eviction runs
        # finalizers and audits the harness thread for a parked event loop;
        # see _evict_fixtures.
        if prev_group is not None and spec["group"] != prev_group:
            _evict_fixtures(prev_group)
        prev_group = spec["group"]
        result = _run_one(spec)
        results.append(result)
        if result["skipped"]:
            line = "SKIP (%s)" % result["reason"]
        else:
            line = _fmt(result["value"], result["unit"])
        print("%-28s %s" % (result["name"], line), flush=True)
    # The last group's fixtures get the same finalize-and-audit pass; without
    # this a leak in the final group would only surface once it stopped being
    # final.
    _evict_fixtures(prev_group)

    # The job's timeout ceiling is a budget; this is the itemized bill.  The
    # top of the list is where CI seconds actually go (fixtures included),
    # so an addition that blows the budget is triaged from the log, not by
    # bisecting a timed-out release run.
    slowest = sorted(
        results, key=lambda r: -r.get("elapsed_seconds", 0.0)
    )[:10]
    if slowest and slowest[0].get("elapsed_seconds", 0.0) > 0:
        print("slowest benchmarks (fixtures + warm-up + repeats):")
        for r in slowest:
            print(
                "  %-32s %7.1fs" % (r["name"], r.get("elapsed_seconds", 0.0))
            )

    import platform as _platform

    doc = {
        "schema": SCHEMA,
        "mode": _MODE,
        "tier": args.tier,
        "warmups": _warmups(),
        "stabilized": stabilized,
        "cronstable_version": meta["version"],
        "orjson": meta["orjson"],
        "uvloop": meta["uvloop"],
        "isal": meta["isal"],
        "python": _platform.python_version(),
        "implementation": _platform.python_implementation(),
        "platform": sys.platform,
        "machine": _platform.machine(),
        "cpu_count": os.cpu_count(),
        "suite_seconds": round(time.perf_counter() - started, 3),
        "results": results,
    }
    if args.json:
        out_dir = os.path.dirname(os.path.abspath(args.json))
        os.makedirs(out_dir, exist_ok=True)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
            f.write("\n")
        print("wrote %s" % args.json)
    ran = sum(1 for r in results if not r["skipped"])
    print(
        "%d benchmarks, %d skipped, %.1fs total (%s mode, cronstable %s)"
        % (
            ran,
            len(results) - ran,
            doc["suite_seconds"],
            _MODE,
            meta["version"],
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
