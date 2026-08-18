# Failure detection and retries

This page describes how cronstable decides whether a job run failed (`failsWhen`), the exact precedence order of failure reasons, the retry mechanism with exponential backoff (`onFailure.retry`), and when each of the three report hooks (`onFailure`, `onPermanentFailure`, `onSuccess`) fires.

## Overview

After a job process exits, cronstable computes a single failure reason from the run's exit code and captured output. If the reason is non-empty, the run *failed*. Otherwise it *succeeded*.

A failure triggers `onFailure` reporting. If a retry is configured and not yet exhausted, the daemon schedules another run after a backoff delay. When retries are exhausted, or none was configured, `onPermanentFailure` reporting fires. A success cancels any pending retry and fires `onSuccess` reporting.

## Determining failure: `failsWhen`

`failsWhen` is a per-job (or per-`defaults`) block of four booleans. `RunningJob.fail_reason` (`cronstable/job.py`) evaluates it after the process exits and its streams have been read.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `producesStdout` | Bool | `false` | If true, any captured standard output marks the run as failed. |
| `producesStderr` | Bool | `true` | If true, any captured standard error marks the run as failed. |
| `nonzeroReturn` | Bool | `true` | If true, an exit code other than `0` marks the run as failed. |
| `always` | Bool | `false` | If true, the run always counts as failed, regardless of exit code or output. |

In the strictyaml schema (`cronstable/config.py`), a `failsWhen` map requires only `producesStdout`. `producesStderr`, `nonzeroReturn`, and `always` are `Opt(...)`. Defaults come from `DEFAULT_CONFIG["failsWhen"]` and are merged in before a `failsWhen` block is applied, so a partial `failsWhen` block inherits the defaults for the keys it omits.

Output detection considers both retained and discarded lines. A stream counts as non-empty if it has saved content *or* if any lines were discarded (`saveLimit` exhausted, or `saveLimit: 0`). For how `captureStdout`/`captureStderr` and `saveLimit` govern what is captured, see [output capturing](Output-Capturing).

An uncaptured stream cannot produce a failure reason. `producesStderr` fires only when `captureStderr` is enabled, and `producesStdout` fires only when `captureStdout` is enabled.

### Precedence order

`fail_reason` returns the first matching condition, in this fixed order, or `None` if none match:

1. `always` is true -> `"failsWhen=always"`.
2. `nonzeroReturn` is true and `retcode != 0` -> `"failsWhen=nonzeroReturn and retcode={retcode}"`.
3. `producesStdout` is true and stdout is non-empty or any stdout lines were discarded -> `"failsWhen=producesStdout and stdout is not empty"`.
4. `producesStderr` is true and stderr is non-empty or any stderr lines were discarded -> `"failsWhen=producesStderr and stderr is not empty"`.

The first match wins, and later conditions are not evaluated. Report templates receive the resulting string as the `fail_reason` variable, and the shell reporter receives it as `CRONSTABLE_FAIL_REASON`. The boolean `failed` is `fail_reason is not None`.

### Special exit codes

The runtime, not the child process, sets two synthetic exit codes:

- **`127`**: the subprocess could not be launched at all, for example because the command does not exist or the argv could not be encoded. `RunningJob` sets `start_failed` and, in `wait()`, assigns `retcode = 127`, so the run counts as an ordinary failure instead of raising an internal error. With the default `nonzeroReturn: true`, this is a failure.
- **`-100`**: the run exceeded its `executionTimeout` and was canceled. `wait()` sets `retcode = -100` before terminating the process. With the default `nonzeroReturn: true`, this is a failure. See [concurrency and timeouts](Concurrency-and-Timeouts).

A run canceled to make way for a newer instance (`concurrencyPolicy: Replace`) is marked `replaced` and is *not* evaluated for failure, reported, or retried.

### Example

```yaml
jobs:
  - name: strict-job
    command: ./run.sh
    schedule: "*/5 * * * *"
    captureStdout: true
    captureStderr: true
    failsWhen:
      producesStdout: false
      producesStderr: true
      nonzeroReturn: true
      always: false
```

## Retries: `onFailure.retry`

Configure retries under `onFailure.retry`. Retry orchestration lives in `cronstable/cron.py` (`launch_scheduled_job`, `handle_job_failure`, `schedule_retry_job`, `cancel_job_retries`). The per-job backoff state is `JobRetryState` in `cronstable/job.py`.

Retry state is in-memory by default, so a pending retry dies with the process. With a `state:` section configured, it also survives daemon restarts (see "Restart-surviving retries" later). On a shared store under leader election, a ladder can move to the node that now owns the job (see "Cross-node retry resume" later).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `maximumRetries` | Int | `0` | Number of retries after the initial failed run. `0` disables retrying. `-1` retries forever. |
| `initialDelay` | Float | `1` | Delay in seconds before the first retry |
| `maximumDelay` | Float | `300` | Upper bound in seconds on the backoff delay |
| `backoffMultiplier` | Float | `2` | Factor the delay is multiplied by after each retry |

In the schema, a `retry` map requires all four keys (no `Opt(...)`), but the `retry` map as a whole is optional, and the preceding `DEFAULT_CONFIG` values are merged in, so a job that omits `retry` entirely gets these defaults. If you do supply a `retry` map, strictyaml requires all four keys, and a partial `retry` block is a validation error.

`JobConfig._validate_numeric_ranges` validates the numeric ranges and raises `ConfigError` on violation:

- `maximumRetries >= -1`
- `initialDelay >= 0`
- `maximumDelay > 0`
- `backoffMultiplier > 0`

### Exponential backoff

`JobRetryState.next_delay()` returns the current delay, then advances it for the next retry:

```
delay      = current delay (returned, used to sleep)
next delay = min(current delay * backoffMultiplier, maximumDelay)
```

The first retry waits `initialDelay`. Each later retry waits the previous delay times `backoffMultiplier`, capped at `maximumDelay`. With `initialDelay: 1`, `backoffMultiplier: 2`, and `maximumDelay: 30`, the delay sequence is 1, 2, 4, 8, 16, 30, 30, ... seconds. The retry counter (`count`) increments on each `next_delay()` call.

The ladder is a pure function of the retry config and the attempt number. That is what lets a durable pending retry be re-armed at its exact position after a daemon restart (see "Restart-surviving retries" later).

### Retry lifecycle

- A retry state exists only when `maximumRetries` is truthy (non-zero). With `maximumRetries: 0`, no state is created and a failed run goes straight to permanent failure.
- `launch_scheduled_job` calls `cancel_job_retries(name)` before starting a scheduled run, then creates a fresh `JobRetryState`. A scheduled run therefore resets any in-progress retry sequence for that job. A manually triggered run (`POST /jobs/{name}/start`, see the [HTTP control API](HTTP-API)) goes through `maybe_launch_job` directly. It does *not* reset or create retry state, and reuses whatever retry state currently exists.
- On each failed run, `handle_job_failure` fires `onFailure` reporting. If no retry state exists or it was canceled, it fires `onPermanentFailure` and stops. Otherwise, if `count >= maximumRetries` and `maximumRetries != -1`, it cancels the retry state and fires `onPermanentFailure`. Otherwise, it schedules the next retry after `next_delay()` seconds.
- A success (`handle_job_success`) calls `cancel_job_retries` and fires `onSuccess`, ending the sequence.
- If a job is removed from the configuration while a retry is pending, `schedule_retry_job` logs a warning, discards the stale retry state, and skips the run cleanly (no exception).
- When leader election is enabled (`cluster.electLeader`), `schedule_retry_job` re-checks the cluster gate before relaunching. A transient fail-closed condition (lost quorum, a detected conflict, a rebuilt gossip manager's still-converging view, a backend read error) does *not* end the sequence. `schedule_retry_job` keeps the retry state and re-checks the gate after another delay of the same length, floored at one second, so a keep-alive job survives the interruption. The first deferral of a wait is logged at `INFO` and repeats at `DEBUG`. The pending retry leaves this node only when another node is *positively* identified as the job's owner, and what happens then depends on the store:
  - When cross-node retry resume is active (a shared-topology state store under leader election, see "Cross-node retry resume" later), the ladder is **handed off**. The local retry state is canceled, a `handoff` record supersedes the durable pending one, and a `WARNING` is logged. *No* `cancelled` run-history record is written: the attempt is not ending but moving, and the new owner *resumes the same attempt* from its durable record.
  - Otherwise the pending retry is **abandoned**. The retry state is canceled and discarded, a `WARNING` is logged, and the abandonment is recorded in the run history as `cancelled`. The failed attempt does not run again elsewhere, and the new owner picks up only the job's *future scheduled firings*.

  Neither path fires `onPermanentFailure`. An `@reboot` ladder is anchored to its host's boot and is never handed off, and an `@reboot` one-shot has no future firing either because its boot run is already recorded. An abandoned `@reboot` keep-alive therefore ends cluster-wide even when resume is active. `EveryNode` ladders stay strictly per-node. See [clustering and leader election](Clustering-and-Leader-Election).
- On shutdown, all pending retries are canceled before cronstable exits. Without a `state:` section, that ends the sequence for good. With one, the graceful-shutdown cancellation deliberately does *not* settle the durable pending record, so the next start re-arms it (see the next section).

### Restart-surviving retries

Everything in this subsection applies only when a `state:` config section is present (the [durable state store](Durable-State)). Without one, the preceding lifecycle is the whole story and a pending retry dies with the process. The store is server-side, on `state.path`, and is unrelated to the web dashboard's browser-side IndexedDB run ledger.

With `state:` configured, every job with a non-zero `maximumRetries` gets a durable retry ladder alongside the in-memory one:

- When a retry is armed, a *pending* record is appended (fire-and-forget) to the job's durable retry stream, carrying the attempt number, the **absolute** `notBefore` deadline, and the job's per-job config digest (`cronstable.fingerprint.job_digest`). A write that never lands loses only the durability: the retry dies with the process, exactly the stateless behavior. A ladder that never scheduled a retry gets no durable record, so a retry-armed job that keeps succeeding costs no store writes.
- Every way a ladder can resolve appends a *settled* record on top, so the next boot finds nothing pending. The settle reasons are `launched` (the settle-before-launch write described next), `succeeded` (the run succeeded), `superseded` (a fresh scheduled fire reset the sequence), `cancelled` (for example, a run canceled from the dashboard), `exhausted` (`maximumRetries` reached), `owner-moved` (the cluster abandonment described earlier; on a shared store the ladder is handed off instead of settled, see "Cross-node retry resume" later), `superseded-by-run` (a claim scan found a durable run newer than the ladder), and `job-removed` (the job disappeared from a reloaded config while the retry slept), plus the boot-time invalidation reasons described later.
- Just before a retry launches, its pending record is settled with reason `launched`. Record-before-run means a crash right after the launch cannot re-arm the attempt that already ran. This is the at-most-once bias. One caveat: under the default `onStoreUnavailable: degrade`, a settle write that cannot land launches anyway. That is at-least-once: a crash in that narrow window could replay that one attempt after a restart. Under `onStoreUnavailable: fail-closed`, the launch is deferred and re-checked instead, exactly like a closed cluster gate.
- A graceful shutdown does **not** settle. The shutdown drain cancels the in-process retry tasks but leaves the pending record on top of the stream, and the next boot re-arms exactly that record.
- On boot, a job whose newest retry record is pending has its ladder re-armed at the persisted position. The retry counter and the next backoff delay are replayed to the recorded attempt. The task sleeps only the time remaining until the absolute `notBefore` deadline: zero if it passed while the daemon was down, in which case the retry is due immediately. The re-armed task is the ordinary `schedule_retry_job`, so the cluster-gate re-check, job-removed cleanup, and shutdown behavior match a never-restarted ladder. Live activity outranks the ledger: a job already retrying or running when the store comes up is left alone.
- A pending record is *settled instead of re-armed* (invalidation) when:
  - The job's per-job config digest changed. It is stricter than the whole-set job-set id, so editing an *unrelated* job does not drop the retry.
  - The job was removed or disabled.
  - The recorded attempt already exhausts `maximumRetries`.
  - The record is older than the job's `startingDeadlineSeconds` (when set).
  - For an `@reboot` job, the machine itself rebooted, so the fresh boot run supersedes the stale ladder.

  Any ambiguous case also settles: the bias is always no-run over double-run.
- `@reboot` keep-alive continuity: if an `@reboot` job with `maximumRetries: -1` has a durable boot marker showing its boot run already happened during *this* OS boot, its pending retry is re-armed instead of a fresh boot run. A keep-alive supervisor (see "Restart a long-running process" later) therefore survives daemon restarts.

### Cross-node retry resume

Restart-surviving retries have a cluster-wide half. When the state store's resolved topology is `shared` (see [durable state](Durable-State)), leader election is configured (`cluster.electLeader`), and the cluster manager is running, a durable retry ladder can move to the node that now owns the job instead of ending with the old one.

Only `Leader` and `PreferLeader` ladders that are not `@reboot` are eligible. `EveryNode` ladders stay strictly per-node, because a foreign pending record on the shared stream is another node's live ladder. An `@reboot` ladder is anchored to its host's boot, so it never moves.

This is the operator-level view. The record-level mechanics live in [Durable State's "Restart-surviving retries"](Durable-State#restart-surviving-retries).

- **Graceful moves hand off.** When the owning node itself observes the ownership move (the earlier abandonment check under "Retry lifecycle"), it appends a `handoff` record carrying the attempt, the job digest, and a now-due deadline, instead of settling the ladder dead. It writes *no* `cancelled` run-history record on this path: the attempt is moving, not ending. The `WARNING` reads "handed off: the cluster moved ownership of it to another node; the new owner resumes the ladder from its durable record (cross-node retry resume)".
- **Crashed owners are claimed after a grace.** A crashed owner leaves its *pending* record newest on the stream. The new owner's claim scan, spawned from the housekeeping pass about once a minute, claims a `handoff` immediately (the owner positively relinquished it), but a foreign *pending* only after it is stale 30 seconds past due. A live owner fires within moments of its deadline, so the grace tolerates only a slightly late fire. It cannot cover a gate-deferred owner, whose re-check cadence is its own ladder delay; the consume-time re-check described later protects that case.
- **Claims serialize on a lease.** A claim validates the record: digest match, job enabled, retry budget, `startingDeadlineSeconds`, and no locally-known newer run. It acquires the per-job `retry-claim/<name>` lease (TTL 30 seconds), re-reads the newest record under the lease (the record must be unchanged), and checks the *durable* run ledger for a run newer than the ladder. A newer run settles the record `superseded-by-run` instead of claiming it (no-run beats double-run). Only then does the claimer append its own *pending*, wait for that write to land before releasing the lease, and re-arm the local ladder exactly as a restart would: an absolute deadline, sleeping only the remaining delay. The claim is logged at `INFO`: "claimed pending retry #N from host H (cross-node retry resume); due in S seconds".
- **The consume re-checks ownership.** With resume active, a due retry's launch decision serializes on the same claim lease and re-checks that the newest ladder record still belongs to *this* host. A foreign newest record, either a claimer's pending or its `launched` settle after it already fired, stops the local ladder silently (no settle is written, so the claimer's record stays newest) with a `WARNING`: "dropped: another node claimed this retry ladder (cross-node retry resume); it fires there". Read or acquire failures follow `onStoreUnavailable`: `degrade` proceeds unserialized, and `fail-closed` defers the launch.
- **The honest contract is at-least-once**, not exactly-once. An owner that is gate-deferred or cut off from the store can still fire an attempt that a claimer also fires. Record ordering (newest-wins) and lease expiry compare wall clocks across hosts, so the clock-skew requirement in [durable state](Durable-State) (run NTP on every node sharing the mount) covers resume too. In a mixed-version fleet, older builds treat the unknown `handoff` kind as not pending and skip it, which is safe.

### Retry example

```yaml
jobs:
  - name: flaky-job
    command: ./flaky.sh
    schedule: "*/10 * * * *"
    captureStderr: true
    onFailure:
      report:
        mail:
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
      retry:
        maximumRetries: 10
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
```

### Restart a long-running process

A schedule of `@reboot` runs the job once at cronstable startup. Combined with `maximumRetries: -1`, this relaunches the process whenever it exits with a failure, indefinitely: a way to keep a long-running process alive under cronstable.

```yaml
jobs:
  - name: keep-alive
    command: ./long-running-server
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: -1
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
```

By default the keep-alive lasts only as long as the cronstable process: a daemon restart runs the `@reboot` job afresh and loses any pending retry. With a `state:` section configured, both halves become durable: the boot run is deduplicated to once per OS boot, and a pending retry is re-armed across daemon restarts, so the supervisor pattern survives them (see "Restart-surviving retries" earlier).

For `@reboot` semantics, see [schedules and timezones](Schedules-and-Timezones).

## Report hooks

Each hook has its own independent `report` block (Sentry, mail, shell, webhook, push, and eventlog), defaulted from `_REPORT_DEFAULTS` and deep-copied per hook so they do not alias. All six reporters in a block run for the relevant outcome. Reporting errors are logged and do not stop the others. For the report block options, see [reporting (mail, Sentry, shell, webhook)](Reporting).

| Hook | Fires when | Frequency |
| --- | --- | --- |
| `onFailure.report` | Every failed run | Once per failed attempt, including each retry that fails |
| `onPermanentFailure.report` | Retries are exhausted, no retry was configured, or the retry state was canceled. | Once, at the end of a failing sequence |
| `onSuccess.report` | The run succeeded (`fail_reason is None`). | Once per successful run |

With no retry configured, a single failed run fires both `onFailure.report` (always) and then `onPermanentFailure.report` (because there is no retry state). To report only after all retries are exhausted, leave `onFailure.report` empty and configure `onPermanentFailure.report` instead, as in the following example.

```yaml
jobs:
  - name: eventually-consistent
    command: ./run.sh
    schedule: "*/10 * * * *"
    captureStderr: true
    onFailure:
      retry:
        maximumRetries: 10
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
    onPermanentFailure:
      report:
        mail:
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
```

If an `onSuccess` mail has an empty rendered body (after `strip()`), it is suppressed and no email is sent. This applies only to success reports.

## Notes

- `nonzeroReturn` checks `retcode != 0`, so both the synthetic `127` (launch failure) and `-100` (timeout) codes count as non-zero failures under the default.
- The `failsWhen` evaluation runs once per completed run, including each retried run, so a retry that still produces stderr (with `producesStderr: true`) fails again and continues the backoff sequence.
- Output-based failure (`producesStdout`/`producesStderr`) depends on stream capturing. Without `captureStdout`/`captureStderr`, the corresponding condition can never trigger, because nothing is captured.
- During shutdown, `handle_job_failure` returns early if the stop event is set. A job that finishes failing while cronstable is shutting down is *not* reported (`onFailure`/`onPermanentFailure` do not fire) and is not retried. A job that finishes successfully during shutdown still cancels its retries and fires `onSuccess`.
