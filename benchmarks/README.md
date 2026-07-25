# Performance benchmarks

This directory holds the performance regression harness that CI runs on every
commit and enforces on every release. It exists to keep cronstable fast and
small enough for old machines: startup cost, schedule math at 100k-job scale,
config parsing, DAG planning, durable-state I/O, memory footprint, the terminal
dashboard's per-frame string work, and the web dashboard's render hot paths are
all measured, and a release that regresses past a metric's limit does not ship.

## The two tools

- `bench.py` runs the suite and writes one JSON document. The harness is
  stdlib-only and benchmarks whatever cronstable the invoking interpreter can
  import, so the same script can measure an older installed release. A
  benchmark whose API the measured version lacks is recorded as skipped,
  never failed. To keep the measurement honest it runs untimed warm-up passes
  before the timed repeats and (best-effort) pins itself to one CPU and raises
  its priority; benchmarks split into an in-process tier and a noisier
  subprocess tier (cold start, import, peak RSS), selectable with `--tier`.
- `compare.py` takes baseline and current JSON files (several rounds per
  side), merges the rounds, renders a markdown summary and an SVG diverging
  bar chart of every compared metric, and exits nonzero when a gated metric
  regressed. A regression gates only when it clears both its declared limit
  and a couple of its measured noise bands (the per-metric round-to-round
  scatter), so jitter alone can never fail the gate.

## Running locally

```sh
python benchmarks/bench.py --quick --json before.json
# ...make your change, then...
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json \
    --md diff.md --svg diff.svg
```

`--quick` cuts workloads to roughly a tenth for a fast local loop; CI runs
the full suite. `--only <substring>` selects benchmarks by name or group
(for example `--only cronexpr`), `--tier inprocess` (or `subprocess`) selects
one tier, `--warmup N` overrides the warm-up passes, `--no-stabilize` skips
the CPU pin, `--list` prints the inventory, and `--smoke` is the minimal mode
the unit tests use. If cronstable is not installed in the interpreter, the
harness falls back to the source tree it lives in and says so on stderr.

Local numbers are only comparable to other runs on the same machine in the
same session. The CI comparison is paired for exactly that reason: both
versions run interleaved on one runner, in the same weather.

## What CI does with this

The `perf` job in `.github/workflows/release.yml` runs on every push and PR,
in parallel with the build matrix:

1. installs the current commit into one venv and the latest release tag into
   another;
2. runs `bench.py` against both, interleaved, per tier: five rounds of the
   in-process tier and two of the subprocess tier (the harness always comes
   from the current checkout, so both sides run identical measurement code);
3. runs `compare.py` over all the result files.

Per metric, rounds merge with the metric's estimator: best-of-rounds for
time (the minimum is the least noisy statistic of a fixed workload) and
median for memory. A metric fails its gate only when it slows down by more
than its declared percentage limit AND by more than its absolute floor AND by
more than a couple of its measured noise bands, where the noise band is the
two sides' round-to-round scatter combined in quadrature. So microsecond
jitter on a sub-millisecond metric can never gate, and neither can a metric's
own run-to-run wobble; a change that clears the raw limit but sits inside the
noise band is reported (not silently dropped) but does not fail the release.
More in-process rounds deepen the best-of-rounds estimate and give the robust
scatter statistic enough points to work with; they do not tighten the noise
band itself (the band estimates single-round scatter, while the comparison
diffs min-of-rounds, so measured bands can even widen as rounds are added).

Three refinements keep the gate both tight and honest:

- **Robust noise band.** From three rounds up, the round-to-round scatter is
  the median absolute deviation (scaled to a standard-deviation equivalent),
  not a plain standard deviation. One throttled or GC-stalled round can no
  longer inflate the band and hide a real regression behind it.
- **Interpreter-startup subtraction.** The `startup.*` metrics are dominated
  by Python's own process spawn and interpreter init, which cronstable cannot
  regress. Each side's `startup.python_baseline` is subtracted before the
  delta is computed, so the gate sees cronstable's OWN contribution and a real
  couple-of-ms import regression is not diluted below the limit by ~40ms of
  un-regressable overhead.
- **A tighter default limit.** The deterministic in-process compute metrics
  gate at 15%; the noisier tiers (subprocess process-spawn, real-disk state
  I/O, peak-RSS, browser render) set a looser limit of their own. The noise
  band above still protects every one of them from jitter.

On an ordinary commit the comparison prints warnings only. On a release the
gate is enforced: the `release` job requires `perf`, so a gated regression
blocks publishing. The release then appends the comparison to its notes and
attaches `perf-summary.md` (the full table) and `perf-results.json` (the
merged raw numbers). `perf-chart.svg` (the diff chart) ships in the run's
`perf-report` artifact.

To ship an intentional regression, start a pushed commit's subject with
`[perf:accept]`. The regression is still measured and reported in the
release notes, but it does not gate. Only subject lines are scanned, same as
the `[release]` marker. `[perf:accept]` excuses RELATIVE regressions only:
an absolute-budget breach or a gate-integrity failure (below) each has its
own ritual, a reviewed edit to the corresponding checked-in file.

## Beyond the relative gate

Four properties of the comparator itself, each added after the 2026-07
audits found the relative gate alone could not deliver them:

- **Absolute budgets** (`budgets.json`, `compare.py --budgets`). The
  relative gate diffs HEAD against the latest tag only, so a slow drift
  across many quick releases compounds under a green gate (seven release
  hops in five days at a legal 15% each is 2.7x). A handful of headline
  metrics carry absolute ceilings; a breach fails the run even when the
  relative gate passes. Raising a ceiling is a deliberate edit to
  `budgets.json`, made in the same PR as the change that needs it.
- **Gate integrity** (`expected_gated.txt`, `compare.py --expected-gated`).
  A metric whose BASELINE side skips is filed as first-release coverage and
  warned about by nothing, so a gate that died because a private seam
  drifted was silently ungated forever. The checked-in list names every
  metric that must actually be compared; a listed metric that was not is an
  integrity failure. The companion net is `tests/test_benchmarks.py`'s
  never-skip list, which catches a drifted seam in the ordinary test run.
- **The effective gate column.** The percentage and absolute-floor tests
  are ANDed, so a metric whose value sits near its floor really gates at
  `100*floor/value`, however tight its declared `gate_pct` reads. The floor
  is deliberate harness policy (jitter on a tiny metric must never gate);
  what was missing was anything REPORTING when it binds. The comparison
  table's "Gate (eff.)" column and a job-log notice now name every
  floor-bound metric, so an undersized workload is visible and fixable
  instead of silently ungated.
- **Comparability.** The two sides must agree on python version, platform,
  run mode, and the optional-backend state (orjson, uvloop). A pairing that
  differs is refused outright (exit 2, never a verdict): a one-sided
  backend would report a backend swap as a large code regression, or mask a
  real one. The CI perf job installs orjson into BOTH venvs -- production's
  default backend in the binaries and Docker images -- and deliberately NOT
  uvloop (see the waivers below).

`bench.py` also stamps per-benchmark wall clock (fixtures included) into
every result row and prints its ten slowest benchmarks per run, so the CI
job's timeout ceiling is triaged from the log rather than by bisecting a
timed-out release.

## Waivers: what is deliberately not measured

Naming an absence makes it a decision instead of an oversight. A future
reader counting metrics against modules should not assume these were
missed:

- **uvloop.** Every shipped Linux binary and Docker image prefers uvloop;
  the whole suite runs on stock asyncio. A full uvloop lane would double
  the perf job and most of any delta would be uvloop's own. Waived;
  `bench.py` records a `uvloop` flag in every result document so the
  absence stays visible, and comparability (above) refuses a mixed pair.
- **Windows and macOS.** The Linux-only pipeline never times the
  platform-divergent paths. A measured `--quick` Windows lane was refuted
  as a dead gate (34 of 38 metrics floor-bound at that scale) and the
  full-scale shape does not fit any timeout. The one real correctness risk
  found there -- the halved directory-barrier count -- is a COUNT
  invariant and is gated by `tests/test_perf_invariants.py` on every
  platform instead.
- **The artifact users actually run.** Startup/RSS metrics time a CPython
  venv; every distributed artifact is a PyInstaller onefile binary or a
  Docker image. A binary startup metric would need the build job's
  artifact and break the perf job's independence; the SIZE half is not a
  perf metric at all and belongs as a byte ceiling in the binary job.
- **Tail latency.** The suite measures best-of-rounds throughput and peak
  memory; the user-visible failure for a cron daemon is a scheduler stall,
  which is a tail. A p99 over 5 rounds is not statistically supportable,
  so the general case is waived -- and `loop.stall_jobs_500` (a MAX
  scheduling-gap gauge, `info` for its first release, then armed) covers
  the one stall class that has actually shipped, five separate times.
- **Leadership backends and operator CLIs** (`state_admin`, `jobcli`,
  `mcpcli`, `discovery`, `tlsutil`). Run by hand once, or one-shot startup
  paths; their pure per-call functions are microsecond-scale. Tests, not
  metrics.
- **The report/notify delivery pipeline.** A reporter metric would be ~95%
  jinja2/stdlib/socket and would break the no-network rule; the one owned
  risk (a multi-MB capture rendered into a report body) is a bounds
  question for a test.
- **resources.py's per-tick process-table walk.** It walks the REAL
  machine's process table, so a timing gate can only measure the runner.
  The invariant that matters -- one table snapshot per sample batch,
  however many runs are monitored -- is a count, gated by
  `tests/test_perf_invariants.py`.
- **Per-line SSE live-tail fan-out.** `job.stream_capture_40k` deliberately
  disables the passthrough mirror and excludes the `on_line` live-tail leg,
  so per-line x per-client delivery on the shared loop stays untimed (a
  zero-subscriber LiveLogBuffer variant could close this later).

The rule those waivers keep applying: **a COUNT or ORDERING invariant gets
a test, never a metric.** Five benchmark candidates from the 2026-07 audits
(fsync barriers, the once-per-batch process-table walk, 413-before-fetch,
the per-run durable-write count, artifact prune residency) are exactly that
and live in `tests/test_perf_invariants.py`, where they gate in both
directions, on every platform, in milliseconds.

## Terminal and web UI benchmarks

The dashboards have their own hot paths, and both are measured.

The **terminal UI** (`tui.*`) is pure Python, so it is benchmarked in process
like everything else: the log drawer re-measures, re-cuts and re-inks its whole
buffer each frame and the log search re-scans it, so `tui.log_restyle_5k` and
`tui.log_search_20k` drive `text_width` / `cut_to_width` / `rewrite_sgr` /
`strip_ansi` over a realistic buffer (coloured, plain, wide-glyph and
control-character lines). No terminal and no app loop.

The **web UI** (`webui.*`) is browser JavaScript, so it is timed inside a
headless Chromium via Playwright. The page exposes a `window.__perf` hook ONLY
under the `?perf=1` query string (it is entirely inert otherwise — no global is
defined), giving the harness seed helpers and the real render functions;
`bench.py` seeds synthetic jobs / fleet / log data and times `renderRows`,
`renderFleet` and `updateLogCount` with the page's own `performance.now()`
(batched, because Chromium clamps that clock to ~100us). The whole `webui`
group **skips cleanly** when Playwright or its Chromium build is absent, when
the page predates the `?perf=1` hook (an older release), and in `--smoke`
(the unit test must not launch a browser). The CI `perf` job installs
Playwright + Chromium into the current-side venv (best-effort) so `webui.*`
runs there; to run them locally:

```sh
pip install playwright && playwright install chromium
python benchmarks/bench.py --quick --only webui
```

Because an older release's page carries no `?perf=1` hook, the `webui` metrics
compare new-against-new (a forward-looking gate and a recorded number), not an
old-vs-new delta; the `tui.*` and backend metrics do diff across releases.

## Adding a benchmark

Register a function in `bench.py` with the `@bench(...)` decorator:

```python
@bench(
    "group.short_name",       # stable metric id; renaming loses history
    "group",
    detail="one line of what the workload is",
    repeats=(5, 2, 1),        # full / quick / smoke repeats
    gate_pct=25.0,            # regression limit, percent
    gate_floor=0.010,         # and the absolute floor, in the metric's unit
)
def bench_thing():
    ...setup (untimed)...
    t0 = time.perf_counter()
    ...the workload...
    return time.perf_counter() - t0
```

Ground rules:

- Time only the workload; do setup outside the timed region, and use
  `fixture(name, builder)` for expensive setup shared across repeats.
- Scale the workload with `_n(base)` so `--quick` and `--smoke` stay cheap.
- Import cronstable inside the function and raise `Skip` when an API is
  missing, so the harness still runs against older releases.
- Keep workloads deterministic: fixed datetimes, fixed inputs, no network.
- Memory metrics use `unit="MB"` and `compare="median"`.
- A benchmark that measures a child process (cold start, import, peak RSS)
  passes `subprocess=True` so it lands in the subprocess tier.
- Size the timed region so it runs long enough (roughly 50ms+) that
  scheduler and GC jitter are a small fraction; a sub-10ms metric is
  dominated by noise. Rescaling an existing benchmark is safe for the gate
  (the comparison re-measures BOTH sides with the current definition, so it
  never diffs a new workload against a stored old number), but bump the metric
  id anyway so the name keeps meaning one fixed workload across releases and a
  release-notes trend is never silently redefined. `cronexpr.test_match_200k`,
  `schedule.duplicates_20k`, `dag.plan_claim_10k` and
  `schedule.pressure_20k_48h` are such rescales: the id suffix carries the
  new scale, and the old ids drop out.
- A metric need not be a duration: `webapi.jobs_bytes_500` gates the SIZE
  of a response body (`unit="KB"`; remember the floor is then in KB), and
  `loop.stall_jobs_500` gates a worst-case scheduling GAP. What matters is
  that the value is deterministic and the regression it guards moves it.
- If what you are guarding is a COUNT or an ORDERING (fsyncs per append,
  calls per batch, a check that must precede a fetch), stop: write a test
  in `tests/test_perf_invariants.py` instead. See the waivers above.
- When a benchmark lands, add it to `tests/test_benchmarks.py`'s never-skip
  list (mandatory when it leans on any private seam) and, once it is known
  to compare against the current baseline release, to
  `benchmarks/expected_gated.txt`.

The suite's own smoke test is `tests/test_benchmarks.py`; it fails if a
headline benchmark starts skipping, so a refactor that breaks a measured API
surfaces in the ordinary test run, not at release time.
