# Failure detection and retries

This page describes how cronstable decides whether a job run failed (`failsWhen`), the exact precedence order of failure reasons, the retry mechanism with exponential backoff (`onFailure.retry`), and when each of the three report hooks (`onFailure`, `onPermanentFailure`, `onSuccess`) fires.

## Overview

After a job process exits, cronstable computes a single failure reason from the run's exit code and captured output. If the reason is nonempty, the run *failed*. Otherwise it *succeeded*.

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

Output detection considers both retained and discarded lines. A stream counts as nonempty if it has saved content *or* if any lines were discarded (`saveLimit` exhausted, or `saveLimit: 0`). For how `captureStdout`/`captureStderr` and `saveLimit` govern what is captured, see [output capturing](Output-Capturing).

An uncaptured stream cannot produce a failure reason. `producesStderr` fires only when `captureStderr` is enabled, and `producesStdout` fires only when `captureStdout` is enabled.

### Precedence order

`fail_reason` returns the first matching condition, in this fixed order, or `None` if none match:

1. A [`verify`](Result-Verification) check failed -> `"verification failed: <reason>"`.
2. `always` is true -> `"configured to mark every run as failed (failsWhen.always)"`.
3. `nonzeroReturn` is true and `retcode != 0` -> `"command exited with code <n>"`.
4. `producesStdout` is true and stdout is nonempty or any stdout lines were discarded -> `"command wrote to stdout (configured to count as a failure)"`.
5. `producesStderr` is true and stderr is nonempty or any stderr lines were discarded -> `"command wrote to stderr (configured to count as a failure)"`.

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

Retry state is kept in memory by default, so restarting the daemon discards pending retries. Configure a `state:` section to preserve them across restarts (see "Restart-surviving retries" later). With a shared store and leader election, another node can take over the remaining retry attempts when it becomes the job's owner (see "Cross-node retry resume" later).

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

The delay sequence depends only on the retry configuration and attempt number. A saved retry can therefore resume at the same attempt and delay after a daemon restart (see "Restart-surviving retries" later).

### Retry lifecycle

- A retry state exists only when `maximumRetries` is truthy (nonzero). With `maximumRetries: 0`, no state is created and a failed run goes straight to permanent failure.
- `launch_scheduled_job` calls `cancel_job_retries(name)` before starting a scheduled run, then creates a fresh `JobRetryState`. A scheduled run therefore resets any in-progress retry sequence for that job. A manually triggered run (`POST /jobs/{name}/start`, see the [HTTP control API](HTTP-API)) goes through `maybe_launch_job` directly. It does *not* reset or create retry state, and reuses whatever retry state currently exists.
- On each failed run, `handle_job_failure` fires `onFailure` reporting. If no retry state exists or it was canceled, it fires `onPermanentFailure` and stops. Otherwise, if `count >= maximumRetries` and `maximumRetries != -1`, it cancels the retry state and fires `onPermanentFailure`. Otherwise, it schedules the next retry after `next_delay()` seconds.
- A success (`handle_job_success`) calls `cancel_job_retries` and fires `onSuccess`, ending the sequence. It leaves a retry state alone when that state belongs to another run's launch and has scheduled no retry. The success of an earlier run that overlaps a newer scheduled run therefore leaves the newer run's retries in place.
- If a job is removed from the configuration while a retry is pending, `schedule_retry_job` logs a warning, discards the stale retry state, and skips the run cleanly (no exception).
- When leader election is enabled (`cluster.electLeader`), `schedule_retry_job` re-checks the cluster gate before relaunching. A transient fail-closed condition (lost quorum, a detected conflict, a rebuilt gossip manager's still-converging view, a backend read error) does *not* end the sequence. `schedule_retry_job` keeps the retry state and re-checks the gate after another delay of the same length, floored at one second, so a keep-alive job survives the interruption. The first deferral of a wait is logged at `INFO` and repeats at `DEBUG`. The pending retry leaves this node only when another node is *positively* identified as the job's owner, and what happens then depends on the store:
  - When cross-node retry resume is active (a shared-topology state store under leader election, see "Cross-node retry resume" later), the retry sequence is **handed off**. The local retry state is canceled, a `handoff` record supersedes the durable pending one, and a `WARNING` is logged. The new owner *resumes the same attempt* from that record, so no `cancelled` run-history record is written.
  - Otherwise the pending retry is **abandoned**. The retry state is canceled and discarded, a `WARNING` is logged, and the abandonment is recorded in the run history as `cancelled`. The failed attempt does not run again elsewhere, and the new owner picks up only the job's *future scheduled firings*.

  Neither path fires `onPermanentFailure`. Retries for `@reboot` jobs belong to the host's current boot and are never handed off. An `@reboot` one-shot has no future scheduled run because its boot run is already recorded. An abandoned `@reboot` keep-alive therefore ends cluster-wide even when resume is active. `EveryNode` retry sequences also stay on their original nodes. See [clustering and leader election](Clustering-and-Leader-Election).
- On shutdown, all pending retries are canceled before cronstable exits. Without a `state:` section, that ends the sequence for good. With one, the graceful-shutdown cancellation deliberately does *not* settle the durable pending record, so the next start re-arms it (see the next section).

### Restart-surviving retries

Everything in this subsection applies only when a `state:` config section is present (the [durable state store](Durable-State)). Without one, the preceding lifecycle is the whole story and a pending retry dies with the process. The store is server-side, on `state.path`, and is unrelated to the web dashboard's browser-side IndexedDB run ledger.

With `state:` configured, jobs with a nonzero `maximumRetries` persist retry records alongside their in-memory retry state:

- When a retry is scheduled, cronstable asynchronously appends a *pending* record to the job's durable retry stream. It contains the attempt number, the **absolute** `notBefore` deadline, and the job's configuration digest (`cronstable.fingerprint.job_digest`). If the write fails, the retry remains in memory but cannot survive a restart. A job that never needs a retry writes no retry records.
- When a pending retry is resolved, cronstable appends a *settled* record so the next startup will not restore that attempt. Reasons include `launched` (the write described next), `succeeded` (the run succeeded), `superseded` (a fresh scheduled run reset the sequence), `cancelled` (for example, a run canceled from the dashboard), `exhausted` (`maximumRetries` reached), `owner-moved` (ownership changed without cross-node resume), `superseded-by-run` (a claim scan found a newer durable run), and `job-removed` (the job was removed while the retry waited). Startup validation can also settle a record, as described below. On a shared store, an ownership change writes a `handoff` record instead; see "Cross-node retry resume".
- Just before launching a retry, cronstable settles its pending record with reason `launched`. Writing this first prevents a restart from replaying an attempt that already launched. Under the default `onStoreUnavailable: degrade`, a failed write still allows the retry to launch, so a subsequent crash could cause that attempt to run again after restart. Under `onStoreUnavailable: fail-closed`, cronstable defers the launch and checks again later.
- A graceful shutdown does **not** settle. The shutdown drain cancels the in-process retry tasks but leaves the pending record on top of the stream, and the next boot re-arms exactly that record.
- At startup, a job whose newest retry record is pending resumes from the saved attempt number and backoff delay. It waits only for the time remaining until `notBefore`; if that time has passed, the retry is due immediately. It uses the ordinary `schedule_retry_job` task, with the same leadership checks, cleanup for removed jobs, and shutdown behavior. A job already retrying or running when the store becomes available keeps its current state.
- A pending record is *settled instead of re-armed* (invalidation) when:
  - The job's configuration digest changed. This check covers only that job's fingerprinted settings, so editing an *unrelated* job does not discard the retry.
  - The job was removed or disabled.
  - The recorded attempt already exhausts `maximumRetries`.
  - The record is older than the job's `startingDeadlineSeconds` (when set).
  - For an `@reboot` job, the machine itself rebooted, so the new boot run supersedes the previous retry sequence.

  Any ambiguous case also settles: the bias is always no-run over double-run.
- `@reboot` keep-alive continuity: if an `@reboot` job with `maximumRetries: -1` has a durable boot marker showing its boot run already happened during *this* OS boot, its pending retry is re-armed instead of a fresh boot run. A keep-alive supervisor (see "Restart a long-running process" later) therefore survives daemon restarts.

### Cross-node retry resume

Pending retries can also survive a change of node. When the state store's resolved topology is `shared` (see [durable state](Durable-State)), leader election is configured (`cluster.electLeader`), and the cluster manager is running, the new job owner can resume the saved retry sequence.

Only retries for `Leader` and `PreferLeader` jobs whose schedule is not `@reboot` are eligible. Each node manages its own `EveryNode` retries, so it must not claim another node's pending record. Retries for `@reboot` jobs belong to the host's current boot and never move to another node.

This is the operator-level view. The record-level mechanics live in [Durable State's "Restart-surviving retries"](Durable-State#restart-surviving-retries).

- **An ownership change transfers the retry sequence.** When the owning node detects the change, it writes a `handoff` record containing the attempt, job digest, and an immediately due deadline. The new owner can resume that attempt, so the old owner writes no `cancelled` run-history record. It logs a `WARNING`: "handed off: the cluster moved ownership of it to another node; the new owner resumes the ladder from its durable record (cross-node retry resume)".
- **Retries from a crashed owner have a grace period.** The new owner scans for claims about once a minute. It can claim a `handoff` record immediately because the previous owner explicitly released it. Another node's *pending* record becomes eligible 30 seconds after its deadline, allowing time for a slightly late retry. A retry blocked by a leadership check may wait longer than this grace period; the ownership check immediately before launch handles that case.
- **A lease coordinates claims.** The claiming node validates the digest, enabled state, retry budget, `startingDeadlineSeconds`, and absence of a newer local run. It acquires the per-job `retry-claim/<name>` lease (TTL 30 seconds), re-reads the newest retry record to confirm it is unchanged, and checks the *durable* run ledger for a run newer than the retry sequence. If one exists, it settles the record as `superseded-by-run`. Otherwise, it writes its own *pending* record and waits for the write to complete before releasing the lease. It then resumes the retry with only the remaining delay before the saved deadline. The claim is logged at `INFO`: "claimed pending retry #N from host H (cross-node retry resume); due in S seconds".
- **Ownership is checked again before launch.** With resume active, the node acquires the same claim lease before launching a due retry and confirms that the newest retry record still belongs to it. If another node has claimed or already launched the attempt, this node stops its local retry sequence and logs a `WARNING`: "dropped: another node claimed this retry ladder (cross-node retry resume); it fires there". It writes no settled record, leaving the new owner's record unchanged. If store errors prevent reading the record or coordinating through the lease, `onStoreUnavailable: degrade` allows the launch without this coordination; `fail-closed` defers it.
- **Cross-node resume provides at-least-once semantics.** A node waiting on leadership or cut off from the store can still launch an attempt that another node has claimed. Record ordering and lease expiry compare wall clocks across hosts, so run NTP on every node sharing the mount, as described in [durable state](Durable-State). Older builds in a mixed-version fleet skip the unrecognized `handoff` record kind; they cannot resume those handoffs.

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

- `nonzeroReturn` checks `retcode != 0`, so both the synthetic `127` (launch failure) and `-100` (timeout) codes count as nonzero failures under the default.
- The `failsWhen` evaluation runs once per completed run, including each retried run, so a retry that still produces stderr (with `producesStderr: true`) fails again and continues the backoff sequence.
- Output-based failure (`producesStdout`/`producesStderr`) depends on stream capturing. Without `captureStdout`/`captureStderr`, the corresponding condition can never trigger, because nothing is captured.
- During shutdown, `handle_job_failure` returns early if the stop event is set. A job that finishes failing while cronstable is shutting down is *not* reported (`onFailure`/`onPermanentFailure` do not fire) and is not retried. A job that finishes successfully during shutdown still cancels its retries and fires `onSuccess`.
