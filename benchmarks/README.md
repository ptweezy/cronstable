# Performance benchmarks

This directory holds the performance regression harness that CI runs on every
commit and enforces on every release. It exists to keep cronstable fast and
small enough for old machines: startup cost, schedule math at 100k-job scale,
config parsing, job launch and reaping, pool admission, DAG planning and runs,
durable-state I/O, the HTTP, MCP and gossip response paths, memory footprint,
the terminal dashboard's frames, and the web dashboard's render and poll paths
are all measured, and a release that regresses past a metric's limit does not
ship.

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
the unit tests use.

The harness measures the cronstable package that the interpreter imports, and
each run names that package's directory on stderr. An interpreter with
cronstable installed measures the installed copy, which is how CI benchmarks
an older release with the current harness. To measure a checkout from such an
interpreter, such as a tox environment, set `PYTHONPATH` to the checkout's
root. A relative path works: the subprocess benchmarks run in a temporary
directory, and the harness gives them each `PYTHONPATH` entry as an absolute
path. If cronstable is not installed in the interpreter, the harness falls
back to the source tree it lives in and says so on stderr.

Local numbers are only comparable to other runs on the same machine in the
same session. The CI comparison is paired for exactly that reason: both
versions run interleaved on one runner, in the same weather.

`--only mem.retired_output_1k` measures memory retained by 1,000 completed
log streams after their output is superseded. Each stream first fills a
1,000-line ring, then releases it while its history summary stays alive.
This catches deque storage that remains allocated after the lines are
cleared, which a cold-start RSS measurement cannot see. The byte and
object-lifetime checks for idle output writers and the dashboard cache
live in `tests/test_perf_invariants.py`.

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

To publish regardless of what the perf job finds, start a pushed commit's
subject with `[perf:ignore]`, or pick `ignore` for the `perf` input of a
manual run (the same input offers `accept`). The comparison still runs and
still lands in the release notes, headed by a line naming the override, but
nothing in it gates: relative regressions, budget breaches and dead gates all
downgrade to warnings, and the perf job itself may fail without holding the
release. The strongest request in a push wins (`ignore` over `accept` over
the default).

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
  table's "Regression limit" column and a job-log notice now name every
  floor-bound metric, so an undersized workload is visible and fixable
  instead of silently ungated.
- **Comparability.** The two sides must agree on python version, platform,
  run mode, and the optional-backend state (orjson, uvloop, isal). A pairing
  that differs is refused outright (exit 2, never a verdict): a one-sided
  backend would report a backend swap as a large code regression, or mask a
  real one. The CI perf job installs orjson and isal into BOTH venvs, the
  backends the binaries and Docker images ship, and deliberately NOT uvloop
  (see the waivers below).

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
  `mcpcli`, `paircli`, `pairlink`, `pairprobe`, `webclient`, `discovery`,
  `tlsutil`). Their pure per-call functions are microsecond-scale, and the
  rest of each call is a network round trip or a store operation that
  `state.*` measures. Tests, not metrics. The exception is process start,
  which a script pays on every call: `startup.jobcli_state_get` and
  `startup.mcp_bridge` time the cold start of the job-side CLI and of the
  MCP bridge.
- **The report/notify delivery pipeline.** A reporter metric would be ~95%
  jinja2/stdlib/socket and would break the no-network rule; the one owned
  risk (a multi-MB capture rendered into a report body) is a bounds
  question for a test.
- **resources.py's per-tick process-table walk.** It walks the REAL
  machine's process table, so a timing gate can only measure the runner.
  The invariant that matters -- one table snapshot per sample batch,
  however many runs are monitored -- is a count, gated by
  `tests/test_perf_invariants.py`. The parse of a stored resource series
  takes synthetic input, and `resources.usage_from_dict_500` times it.

The rule those waivers keep applying: **a COUNT or ORDERING invariant gets
a test, never a metric.** Five benchmark candidates from the 2026-07 audits
(fsync barriers, the once-per-batch process-table walk, 413-before-fetch,
the per-run durable-write count, artifact prune residency) are exactly that
and live in `tests/test_perf_invariants.py`, where they gate in both
directions, on every platform, in milliseconds.

## Terminal and web UI benchmarks

The dashboards have their own hot paths, and both are measured.

The terminal UI (`tui.*`) is benchmarked in process, without a terminal or app
loop. `tui.log_restyle_5k` and `tui.log_search_20k` measure text transforms and
search across buffers containing ANSI colors, wide glyphs, and control
characters. `tui.drawer_paint_5k` measures the log drawer's scroll and redraw
paths, including its ANSI cache.

`tui.dag_graph_paint_2k` paints a 2,000-task chain and a wide fan-in graph
500 times each. Each graph starts with a cold layout cache. The renderer
computes dependency layers iteratively and caches them for the current task
snapshot. Redraws build visible rows and stop styling task labels at the right
edge of the viewport. Run-state snapshots use a task lookup so redraws can
read the states of visible tasks directly.

`tui.table_paint_5k` paints full frames of the jobs table over 5,000 jobs,
steady and while the selection scrolls, and `tui.frame_bytes_5k` gates the
bytes that one full repaint writes to the terminal. `tui.poll_absorb_5k`
times what one `/jobs` poll costs the dashboard: the JSON decode, the health
fold, the sort, and the verdict. `tui.view_sort_5k` and `tui.palette_type_5k`
time the filter and sort pass and the command palette's ranking, which run on
each keystroke. `tui.fleet_paint_15x400`, `tui.week_rows_500`, and
`tui.wallboard_paint_5k` cover the overlays, `tui.tail_ingest_30k` covers
the live log tail, and `tui.mark_idle_300` covers the header mark's idle
animation.

The web UI (`webui.*`) is browser JavaScript, so it is timed inside a
headless Chromium through Playwright. The page exposes a `window.__perf` hook
only under the `?perf=1` query string, and defines no global otherwise. The
hook gives the harness seed helpers and the real render functions. `bench.py`
seeds synthetic jobs, fleet, and log data, and times each operation with the
page's own `performance.now()`, in batches, because Chromium clamps that
clock to about 100 microseconds.

The group follows what the page does on each trigger:

- First load: `webui.boot_rows_500` times navigation to the first laid-out
  jobs table.
- Each poll: `webui.rows_diff_500` (the keyed row diff),
  `webui.wallboard_poll_500`, `webui.dag_tasks_2k`, `webui.timeline_500`, and
  `webui.swim_400x49`. `webui.render_rows_500`, `webui.rows_layout_200`, and
  `webui.render_fleet_15x400` time the full rebuilds.
- Each second: `webui.tick_500`.
- Each frame: `webui.dirty_frame_rows_500` reads the main-thread time of a
  repainted frame through a CDP session, and `webui.logo_recovery` steps the
  logo's simulation.
- Each keystroke: `webui.filter_keys_500`, `webui.select_move_500`,
  `webui.sort_500`, and `webui.palette_keys_500`.
- Logs: `webui.render_term_5k`, `webui.tail_term_2k`, `webui.append_line_5k`,
  and `webui.log_count_5k`.
- Schedule walks and run analysis: `webui.week_walk_500x20`,
  `webui.radar_walk_500x150`, and `webui.ledger_analyze_500x600`.

The whole `webui` group skips cleanly when Playwright or a browser is absent,
and in `--smoke`, because the unit test must not launch a browser. A
benchmark that needs a hook the measured page lacks skips on that side. The
CI `perf` job installs Playwright and Chromium for both sides so `webui.*`
runs there. To run the group locally:

```sh
pip install playwright && playwright install chromium
python benchmarks/bench.py --quick --only webui
```

To use an installed browser in place of Playwright's Chromium build, set
`CRONSTABLE_TEST_BROWSER_CHANNEL` to its channel name, for example `msedge`.
The browser tests read the same variable.

A benchmark that drives the page's own controls, or a hook that the baseline
release's page carries, compares across releases like the `tui.*` and backend
metrics. A benchmark that needs a new hook compares from the first release
whose page has it.

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
- A fixture holding external state that outlives a dropped reference (a
  subprocess, a session, and above all anything that parks a RUNNING event
  loop on the harness thread the way Playwright's sync API does) must pass
  `fixture(name, builder, finalizer=...)`. Fixtures are evicted at group
  boundaries, and the harness then audits the thread for a leftover running
  loop: a parked loop hard-fails the run right there, because it would
  otherwise silently skip every later `asyncio.run()` benchmark on both
  sides of the comparison (the 1.2.31 release's six dead gates).
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
- When a benchmark lands, add it to `benchmarks/expected_gated.txt` once it
  is known to compare against the current baseline release.  The smoke
  net's never-skip set is derived from that file, so listing it there is
  also what makes `tests/test_benchmarks.py` fail if it ever starts
  skipping (the guard that matters when it leans on any private seam).  A
  seam-leaning benchmark that cannot enter `expected_gated.txt` yet
  (its surface is new this release) has no net until it does; prefer
  public-surface workloads for anything that must wait.

The suite's own smoke test is `tests/test_benchmarks.py`; it fails if a
headline benchmark starts skipping, so a refactor that breaks a measured API
surfaces in the ordinary test run, not at release time.
