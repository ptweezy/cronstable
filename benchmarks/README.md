# Performance benchmarks

This directory holds the performance regression harness that CI runs on every
commit and enforces on every release. It exists to keep cronstable fast and
small enough for old machines: startup cost, schedule math at 100k-job scale,
config parsing, DAG planning, durable-state I/O, and memory footprint are all
measured, and a release that regresses past a metric's limit does not ship.

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
  bar chart of the largest changes, and exits nonzero when a gated metric
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
two sides' round-to-round scatter (coefficient of variation) combined in
quadrature. So microsecond jitter on a sub-millisecond metric can never gate,
and neither can a metric's own run-to-run wobble; a change that clears the
raw limit but sits inside the noise band is reported (not silently dropped)
but does not fail the release. More in-process rounds exist precisely to
tighten that noise-band estimate.

On an ordinary commit the comparison prints warnings only. On a release the
gate is enforced: the `release` job requires `perf`, so a gated regression
blocks publishing. The release then embeds the comparison in its notes,
attaches `perf-chart.svg` (the diff chart), `perf-summary.md` (the full
table), and `perf-results.json` (the merged raw numbers).

To ship an intentional regression, start a pushed commit's subject with
`[perf:accept]`. The regression is still measured and reported in the
release notes, but it does not gate. Only subject lines are scanned, same as
the `[release]` marker.

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
  `schedule.duplicates_20k` and `dag.plan_claim_10k` are such rescales: the id
  suffix carries the new scale, and the old ids drop out.

The suite's own smoke test is `tests/test_benchmarks.py`; it fails if a
headline benchmark starts skipping, so a refactor that breaks a measured API
surfaces in the ordinary test run, not at release time.
