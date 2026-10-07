# Performance benchmarks

CI benchmarks every commit and blocks releases that exceed regression
limits. The suite measures startup, scheduling at 100,000-job scale,
config parsing, job launch, DAG planning and runs, durable-state I/O, the
HTTP and MCP response paths, both dashboards, JSON, redaction, calendar
rendering, and memory use to track performance on older and smaller machines.

## What is measured

The suite lives in [`benchmarks/`](https://github.com/ptweezy/cronstable/blob/main/benchmarks)
and registers about 200 benchmarks across these groups
(`python benchmarks/bench.py --list` prints them all):

| Group | Examples |
|---|---|
| `startup` | wall clock of `cronstable --version`, importing the scheduling engine, importing the full daemon graph, `--validate-config` over a 100-job file, the cold start of the job state commands and the MCP bridge, a daemon's boot to its first idle pass |
| `cronexpr` | parsing plain and extended expressions (ranges, steps, `L`, `W`, `#`, `H`, seconds), `next()` search, enumerating occurrences, instant matching |
| `config` | YAML parsing of a 300-job config and of jobs that set many options, per-job `JobConfig` construction, classic crontab parsing, reloading a 1,000-file config directory |
| `schedule` | building the fire schedule for 100,000 jobs from cold, reseeding it pre-parsed, schedule load over 48 hours, duplicate detection, slot suggestion |
| `dag` | building and validating 10k-task graphs, the plan-and-claim transform over a 10k-task run, a run through the scheduler from trigger to finish, run retention, recovery planning |
| `state` | appending durable records with and without pruning, cold and memoized `derive_max`, listing records, job KV round trips, artifact lookups and publishes across 2,000 names |
| `json`, `fingerprint`, `redact`, `ical` | serialization round trips, job-set fingerprinting at 10k jobs, log redaction with and without secrets, iCal rendering |
| `tui` | painting the jobs table over 5,000 jobs and the bytes of a full repaint, absorbing a poll, filtering and sorting, restyling, searching, and painting a log drawer of 5k to 20k lines, painting 2k-task graphs |
| `webui` | web dashboard work in headless Chromium: page boot, the per-poll row diff, the once-per-second tick, filter keystrokes, 500 job rows, a 15-node fleet matrix, 5k-line logs, the week and upcoming-runs walks |
| `loop` | the longest event-loop stall under `/jobs` polls, `/metrics` scrapes, MCP calls, a herd of launches, and bursts of completions; idle loop iterations |
| `webapi` | the `GET /jobs` handler and body size at 500 jobs, `/activity`, `/pools`, the live log tail, response gzip, SSE framing, bearer-token auth, the dashboard page's size |
| `cluster` | job-owner lookups, a full gossip round, serving `/peer` and its body size, absorbing gossiped job summaries, fleet-view merges |
| `prometheus`, `statsd`, `mcp` | `/metrics` rendering and its size, the durable counter snapshot, StatsD emission, MCP job and fleet listings and their response sizes |
| `job`, `resources`, `push` | launching and reaping 2,000 jobs at once, the per-line output capture pipeline, reporting with no reporter configured, resource-monitor final readings, sealing push alerts and fanning one out to eight devices |
| `pools`, `jobapi`, `pair` | pool admission and enqueue with 1,000 entries queued, the job state API's read path and denied lock acquires, QR encoding of a pairing link |
| `memory` | traced bytes held by parsed schedules, job configs, run history, the gossip view, and the terminal dashboard; peak RSS of a real `--version` process, of the daemon import, and of a daemon booted on a 10,000-job config |

Time metrics report the wall clock of a fixed workload, memory metrics report
MB, and size metrics report KB. Lower is always better.

## How the comparison works

Runner hardware in CI is noisy, so absolute times from different runs are not
compared. Instead the `perf` job makes a paired measurement on one runner:

1. The current commit is installed into one virtualenv, and the latest
   release tag into another.
2. The suite runs against both in interleaved rounds: five for in-process
   metrics and two for subprocess (startup, peak-RSS) metrics. The harness
   itself always comes from the current checkout, so both sides run
   identical measurement code. A benchmark whose API the old release lacks
   is recorded as skipped for that side.
3. `benchmarks/compare.py` merges each side's rounds (best-of-rounds for
   time, median for memory) and diffs the two.

A metric fails its gate only when it slows down by more than its declared
percentage limit (15% for most metrics, 25% for noisier ones such as the
subprocess, disk I/O, and peak-RSS metrics), by more than its absolute floor,
and by more than twice its measured round-to-round noise. Microsecond jitter
on a tiny metric can therefore never gate.

On an ordinary commit or pull request the comparison only warns. On a release
the gate is enforced: the publish jobs require `perf`, so a gated regression
stops the release before anything ships.

## The release report

Each GitHub Release carries the comparison against the previous release:

- the gate verdict and the full metric table, in the performance section of
  the release notes;
- `perf-summary.md` (that same table) and `perf-results.json` (the merged raw
  numbers for the release), attached as assets.

Every comparison also renders `perf-chart.svg`, a diverging bar chart with a
row for every compared metric. It ships in the `perf-report` artifact on the
workflow run, alongside the same two files, and stays there for 30 days.

The first release after the suite was introduced records numbers without a
comparison. Every release after that diffs against the one before it.

## Accepting an intentional regression

A feature can be worth a measured cost. To ship one, start a pushed commit's
subject line with `[perf:accept]`. The regression is still measured and
listed in the release notes, but it does not fail the gate. Only commit
subjects are scanned, exactly like the `[release]` marker described in
[release pipeline](Release-Pipeline#triggering-a-release).

The markers in this section and the next take effect when a commit reaches
`main`, so the maintainer applies them. In a pull request, explain an
intentional regression in the description instead.

## Overriding the gate

`[perf:accept]` excuses relative regressions only. To publish regardless of
what the perf job finds, start a pushed commit's subject with `[perf:ignore]`,
or pick `ignore` in the `perf` dropdown of a manual run (which also offers
`accept`). The comparison still runs and still lands in the release notes,
under a heading that names the override, but nothing in it gates: relative
regressions, absolute budget breaches and dead gates all become warnings, and
a perf job that fails outright does not hold the release either. When a push
carries both markers, or a marker and a dropdown choice, the strongest request
wins: `ignore` over `accept` over the default.

## Running the suite yourself

```sh
python benchmarks/bench.py --quick --json before.json
# make a change
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json --md diff.md
```

`--quick` trims workloads to roughly a tenth for a fast local loop, and
`--only <substring>` runs one group (for example `--only cronexpr`). Compare
only runs made on the same machine. The full harness reference, including how
to add a benchmark, is in
[`benchmarks/README.md`](https://github.com/ptweezy/cronstable/blob/main/benchmarks/README.md).

## Related pages

- [Release Pipeline](Release-Pipeline): the pipeline this gate is part of,
  and the `[release]` marker syntax.
- [Architecture and Internals](Architecture-and-Internals): the components
  the benchmark groups map onto.
- [Schedule Pressure](Schedule-Pressure), [Duplicate Schedule Detection](Duplicate-Schedule-Detection),
  [Suggest a Slot](Suggest-a-Slot): the fleet analyzers several `schedule`
  metrics exercise.
