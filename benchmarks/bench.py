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


def _tmpdir() -> str:
    global _SESSION_TMP
    if _SESSION_TMP is None:
        _SESSION_TMP = tempfile.mkdtemp(prefix="cronstable-bench-")
        atexit.register(shutil.rmtree, _SESSION_TMP, ignore_errors=True)
    return _SESSION_TMP


def fixture(name, builder):
    """Build-once shared setup, excluded from every timed region."""
    if name not in _FIX:
        _FIX[name] = builder()
    return _FIX[name]


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

    Note on the timezone half of the round-2 finding: _local_tzinfo()
    always returns a fixed-offset datetime.timezone, so a zone dependence
    here is a docstring nit, not a determinism hazard -- and whatever
    effect exists cancels between the two sides of a paired run on one
    runner.
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


@bench(
    "schedule.suggest_slot_5k",
    "schedule",
    detail="suggest_slot against 5k entries",
    repeats=(3, 2, 1),
)
def bench_suggest_slot():
    try:
        from cronstable.croninfo import suggest_slot
    except ImportError as exc:
        raise Skip("suggest_slot unavailable: %r" % exc) from None
    entries = fixture("entries_5k", lambda: _schedule_entries(_n(5000)))
    t0 = time.perf_counter()
    suggest_slot(entries, period="hourly", start=_NOW)
    return time.perf_counter() - t0


@bench(
    "schedule.lint_250_zoned",
    "schedule",
    detail="lint_schedule for 250 zoned restricted-hour dailies",
    repeats=(3, 2, 1),
)
def bench_schedule_lint_zoned():
    """The DST linter's zoned path, which no fixture anywhere else enters.

    No bench fixture sets a timezone, and _lint_dst returns [] for
    fixed-offset zones, so the memoized per-(zone, year) transition scan is
    exercised by nothing: a cache-defeat regression means 250 full 366-day
    offset scans per parse and stays invisible to every functional test.
    The schedules are restricted-hour fixed-time dailies with wall times
    OUTSIDE 01:00-03:00 -- deliberately NOT the suite's _EVEN_STEPS shapes,
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
    n = _n(250)
    exprs = fixture(
        "lint_zoned_exprs",
        lambda: [
            "%d %d * * *" % (i % 60, 5 + (i * 3) % 18) for i in range(n)
        ],
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


# The forever-loop's fire pass at marketed scale.  The fixture is the single
# most expensive in the suite (~100k JobConfigs, built once per process), so
# it sits LAST in the schedule group: the group boundary evicts it before the
# dag group starts.
_DUE_BASE = datetime(2026, 3, 15, 12, 31, 0, tzinfo=timezone.utc)


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
    100k-job YAML parse costs tens of seconds and is config.py's business).

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
    "dag.finish_fanin_1k",
    "dag",
    detail="record 1k mapped-task completions to a run doc (durable)",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_dag_finish_fanin():
    """A mapped fan-out's N instances finishing together.

    Fixed workload -- record N task completions durably to one run document --
    run the way this release records them: a single batched read-modify-write
    (``mark_tasks_finished``) where that exists, else N separate ones
    (``mark_task_finished``).  A release that batches pays one full-document
    serialize + fsync instead of N, so the comparison surfaces exactly that.
    """
    import asyncio

    dag = _dag_module()
    if not hasattr(dag, "new_run_body") or not hasattr(
        dag, "mark_task_finished"
    ):
        raise Skip("dag completion API not present")
    n = _n(1000)
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("d", tasks)
    now = 1700000000.0
    ns = dag.DAG_RUN_NS_PREFIX + "d"
    run_key = "r"
    batched = getattr(dag, "mark_tasks_finished", None)

    def _running_body():
        body = dag.new_run_body(
            dag="d",
            run_key=run_key,
            run_id="rid",
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

    async def run():
        path = tempfile.mkdtemp(prefix="dagfin-", dir=_tmpdir())
        backend = _state_backend(path)
        await backend.start()
        try:
            body = _running_body()
            await backend.mutate_document(
                ns, run_key, lambda cur, b=body: (b, None)
            )
            if batched is not None:
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
                t0 = time.perf_counter()
                await backend.mutate_document(ns, run_key, batched(marks, now))
                dt = time.perf_counter() - t0
            else:
                t0 = time.perf_counter()
                for t in tasks:
                    transform = dag.mark_task_finished(
                        t.id,
                        success=True,
                        exit_code=0,
                        fail_reason=None,
                        now=now,
                        task=t,
                        expected_proc="p",
                        expected_attempt=0,
                    )
                    await backend.mutate_document(ns, run_key, transform)
                dt = time.perf_counter() - t0
        finally:
            await backend.stop()
            shutil.rmtree(path, ignore_errors=True)
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
    import asyncio
    import hashlib

    dag = _dag_module()
    if not hasattr(dag, "reconcile_and_plan"):
        raise Skip("dag.reconcile_and_plan not present")
    try:
        from cronstable.state import DOC_KEEP
    except ImportError as exc:
        raise Skip("state DOC_KEEP sentinel unavailable: %r" % exc) from None
    n = _n(1000)
    now = 1700000000.0
    ns = dag.DAG_RUN_NS_PREFIX + "quiet"
    run_key = "r"
    passes = 40  # 20 measured under the 50ms rule on Linux
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("quiet", tasks)

    def _inflight_body():
        body = dag.new_run_body(
            dag="quiet",
            run_key=run_key,
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

    def _wrap_keep(transform):
        def wrapped(cur):
            new_body, result = transform(cur)
            if not isinstance(new_body, dict):
                return DOC_KEEP, result
            return new_body, result

        return wrapped

    async def run():
        path = tempfile.mkdtemp(prefix="dagquiet-", dir=_tmpdir())
        backend = _state_backend(path)
        await backend.start()
        try:
            body = _inflight_body()
            await backend.mutate_document(
                ns, run_key, lambda cur, b=body: (b, None)
            )
            doc_files = [
                os.path.join(root, f)
                for root, _dirs, files in os.walk(path)
                for f in files
            ]
            before = hashlib.sha256()
            for f in sorted(doc_files):
                with open(f, "rb") as handle:
                    before.update(handle.read())
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
            after = hashlib.sha256()
            for f in sorted(doc_files):
                with open(f, "rb") as handle:
                    after.update(handle.read())
        finally:
            await backend.stop()
            shutil.rmtree(path, ignore_errors=True)
        for result in results:
            reconciled = getattr(result, "reconciled", 0)
            advance = getattr(result, "advance", None)
            changed = advance is not None and getattr(
                advance, "changed", False
            )
            launches = advance is not None and getattr(
                advance, "launches", []
            )
            if reconciled or changed or launches:
                raise RuntimeError(
                    "advance pass was not quiescent (%r); the metric would "
                    "time the wrong shape" % (result,)
                )
        if before.hexdigest() != after.hexdigest():
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
    "dag.list_dags_warm",
    "dag",
    detail="list_dags steady poll over a dag with 50 terminal runs",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.002,
)
def bench_dag_list_dags_warm():
    """The /dags dashboard poll's rollup for one dag with many terminal runs.

    A release that caches immutable terminal runs re-reads nothing on the
    steady poll; one that re-reads every run document each call pays the full
    scan every time -- the difference this steady-state measurement surfaces.
    """
    import asyncio

    Cron = _cron_cls()
    path = _seeded_dag_runs()
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
            for _ in range(5):
                await dagsched.list_dags()
            dt = time.perf_counter() - t0
        finally:
            await _teardown_cron(cron)
        return dt

    return asyncio.run(run())


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
    "state.artifact_list_churn",
    "state",
    detail="artifact_list after 2k puts over 8 names",
    repeats=(3, 2, 1),
    gate_pct=25.0,
    gate_floor=0.005,
)
def bench_artifact_list_churn():
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

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for _ in range(10):
                await jobstate.artifact_list(backend, "bench")
            dt = time.perf_counter() - t0
        except TypeError as exc:
            raise Skip("artifact_list signature changed: %r" % exc) from None
        await backend.stop()
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


# ---------------------------------------------------------------------------
# webui: the browser dashboard's render hot paths, timed inside a headless
# Chromium via the page's ?perf=1 __perf hook.  The whole group skips unless
# Playwright + its Chromium build are installed AND the page carries the hook
# (an older release predates it), and never runs in --smoke (the unit test must
# not launch a browser).  These are the client-side twins of the tui.* metrics.
# ---------------------------------------------------------------------------


def _web_page():
    """Launch headless Chromium once and load the hooked dashboard page.

    Cached for the whole webui group (torn down at interpreter exit); a missing
    dependency, a failed launch, or a hookless page each raise Skip so the
    metrics record as skipped, never failed.
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
        try:
            pw = sync_playwright().start()
            browser = pw.chromium.launch()
        except Exception as exc:
            raise Skip("chromium launch failed: %r" % exc) from None
        atexit.register(lambda: (browser.close(), pw.stop()))
        page = browser.new_page()
        page.goto("file://" + page_path.replace("\\", "/") + "?perf=1")
        page.wait_for_timeout(300)
        if not page.evaluate("() => !!(window.__perf)"):
            raise Skip("page lacks the ?perf=1 __perf hook (older release)")
        return page

    return fixture("web_page", build)


def _web_time(page, setup_js, op_js, batch=10, batches=12):
    """Min per-op wall time (seconds) of ``op_js`` after ``setup_js``.

    Timed with the page's own ``performance.now()`` so only the render work is
    measured, never the Python<->browser round trip.  Each batch times
    ``batch`` ops together and divides -- Chromium clamps ``performance.now()``
    to ~100us, so a single fast render reads as zero; a batch clears the clamp.
    The MIN batch-mean over ``batches`` is the least-noisy statistic, matching
    the suite's ``compare='min'``.
    """
    ms = page.evaluate(
        "() => { %s; for (let w=0; w<2; w++) { %s; }"
        " let best = Infinity;"
        " for (let b=0; b<%d; b++) {"
        "   const a = performance.now();"
        "   for (let i=0; i<%d; i++) { %s; }"
        "   best = Math.min(best, (performance.now() - a) / %d); }"
        " return best; }" % (setup_js, op_js, batches, batch, op_js, batch)
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
    if _MODE == "smoke":
        raise Skip("webui metrics do not run in smoke mode")
    page = _web_page()
    return _web_time(
        page,
        "__perf.seedLog(%d, 'worker')" % _n(5000),
        "__perf.updateLogCount()",
    )


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

        beat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)  # let the heartbeat take its first timestamp
        await asyncio.gather(
            *(cron._web_list_jobs(_mocked_get("/jobs")) for _ in range(20))
        )
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
            "cronstable.bench.duration:1250|ms|@0.1\n"
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


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _cronstable_meta():
    meta = {"version": None, "orjson": False, "uvloop": False}
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
        # later group still needs is simply rebuilt, untimed).
        if prev_group is not None and spec["group"] != prev_group:
            _FIX.clear()
            gc.collect()
        prev_group = spec["group"]
        result = _run_one(spec)
        results.append(result)
        if result["skipped"]:
            line = "SKIP (%s)" % result["reason"]
        else:
            line = _fmt(result["value"], result["unit"])
        print("%-28s %s" % (result["name"], line), flush=True)

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
