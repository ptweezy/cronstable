# Late-run detection (SLA monitoring)

Service level agreement (SLA) monitoring detects jobs that have gone too long without success, missed their scheduled start, or exceeded an expected runtime. Set per-job thresholds in an `sla:` block. Each breach triggers `onLate` once, using the same six reporters as [failure reporting](Reporting). The Windows Event Log reporter writes event 1003 at Warning level.

The monitor evaluates checks once per wall-clock minute. It needs no [state store](Durable-State), though a store preserves the last successful run across restarts. Breaches appear in the [HTTP API](HTTP-API#get-jobs) (`sla` on `GET /jobs`), the [web dashboard](Web-Dashboard) and [terminal dashboard](Terminal-Dashboard) (**OVERDUE**), [Prometheus](Metrics-with-Prometheus#per-job), and [MCP](MCP) observe tools.

The historical "SLA aggregates" in `GET /jobs/{name}/trends` summarize completed runs. The `sla:` checks described here monitor current thresholds.

## Configuring it

```yaml
jobs:
  - name: nightly-etl
    command: python -m etl.run
    schedule: "0 4 * * *"
    sla:
      maxTimeSinceSuccessSeconds: 129600   # page when no success for 36h
      lateAfterSeconds: 900                # page when a due slot has not started within 15min
      maxRuntimeSeconds: 7200              # page when a run exceeds 2h
    onLate:
      report:
        webhook:
          url:
            fromEnvVar: SLACK_WEBHOOK_URL
```

### Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `sla.maxTimeSinceSuccessSeconds` | int or null | `null` (off) | Breach when this many seconds pass without a successful finish. Must be `> 0` when set. |
| `sla.lateAfterSeconds` | int or null | `null` (off) | Breach when a due scheduled slot has not started a run within this many seconds. Must be `> 0` when set. |
| `sla.maxRuntimeSeconds` | int or null | `null` (off) | Breach while any running instance has been running longer than this. Observes only; this check never stops the run (to enforce a limit, use [`executionTimeout`](Concurrency-and-Timeouts)). Must be `> 0` when set. |
| `onLate.report` | report block | reporter defaults | The [reporters](Reporting) fired once per breach: `mail`, `sentry`, `shell`, `webhook`. The schema matches `onFailure.report`, with overdue-specific default templates. |

The three thresholds are independent; set any subset. Configuring an `onLate` reporter with no thresholds raises a load-time `ConfigError` (`onLate requires sla`). Both keys merge under a [`defaults:` block](Includes-and-Defaults) and are excluded from the [job-set ID](Job-Set-ID) fingerprint.

## The three checks

Check names are the config keys minus their `Seconds` suffix: `maxTimeSinceSuccess`, `lateAfter`, `maxRuntime`. The same vocabulary appears everywhere a check is named: the metric `check` label, the payload's `check` field, and the `{{sla_check}}` template variable.

1. **`maxTimeSinceSuccess`**: breached when `now - last successful finish` exceeds the threshold. With no recorded success, the check measures from daemon startup. A [durable run ledger](Durable-State) restores the last success at boot, so an already-overdue job may trigger a report soon after restart.
2. **`lateAfter`**: a scheduled slot falls due, and no run of the job has started since it. Breached when `now - due` exceeds the threshold. Any start (scheduled, catch-up, retry, or manual) clears it. Slots skipped because the job was [paused](Pausing-Jobs) do not count as late, and a restart baselines on the next due slot.
3. **`maxRuntime`**: breached while any running instance has been running longer than the threshold, measured from the run's launch instant. Clears when the run ends. It never stops anything.

A disabled or [paused](Pausing-Jobs) job is not evaluated at all. Under [leader election](Clustering-and-Leader-Election), only the node that owns the job evaluates it, so one breach pages once, not once per node.

## Breaches latch

Each `(job, check)` pair carries a latch. On the transition into breach, cronstable fires the `onLate` reporters once, sets `cronstable_job_late{job_name, check}` to `1`, increments `cronstable_job_sla_breaches_total`, and logs a warning naming the observed and threshold seconds. While the breach persists, nothing re-fires. On recovery, the gauge clears, an info line is logged, and no report fires. The latch is in-memory, so after a daemon restart a still-breached check fires its report once more.

Reports are dispatched off the scheduler loop and ordered after the same job's in-flight completion reports, so a slow SMTP server can never stall scheduling.

## The onLate report

`onLate.report` takes the exact schema of the other [reporting hooks](Reporting), with defaults reworded for a breach (there is no run outcome to describe). The default mail subject is:

```text
Cron job '{{name}}' is overdue ({{sla_check}})
```

The default body names the check, the threshold, the observed value, and the last success (or `(none recorded)`). The default webhook body wraps the same text in the Slack-compatible `{"text": ...}` shape. The default sentry fingerprint is `["cronstable", "sla", "{{ name }}"]`, so breaches group as their own Sentry issue per job instead of folding into run failures.

Templates receive the full standard [template variable set](Reporting#templating), with the run-shaped fields empty: `success` is `false`, `fail_reason` is `sla: <check> breached`, and `stdout`/`stderr`/`exit_code` are `null`. They also receive four breach variables, which the shell reporter additionally receives as environment variables:

| Template variable | Environment variable | Value |
| --- | --- | --- |
| `sla_check` | `CRONSTABLE_SLA_CHECK` | The check name: `maxTimeSinceSuccess`, `lateAfter`, or `maxRuntime`. |
| `threshold_seconds` | `CRONSTABLE_SLA_THRESHOLD_SECONDS` | The configured threshold. |
| `observed_seconds` | `CRONSTABLE_SLA_OBSERVED_SECONDS` | The measured value that breached it. |
| `last_success_at` | `CRONSTABLE_LAST_SUCCESS_AT` | ISO-8601 instant of the last known success, or `null` (empty string in the environment). |

## Where breaches show

- **`GET /jobs`** carries an `sla` object for every job with a configured check (and only those): `thresholds` (the non-null keys), `state` (`"ok"` or `"late"`), and `breaches`, a list of `{check, since, observed_seconds, threshold_seconds}`, where `since` is when the monitor latched the breach. The `observed_seconds` value is re-measured at payload time, so dashboards show a moving number. See [HTTP API](HTTP-API#get-jobs).
- **[Prometheus](Metrics-with-Prometheus#per-job)**: `cronstable_job_late{job_name, check}` (0/1 per check) and `cronstable_job_sla_breaches_total{job_name, check}`, both emitted after the monitor first evaluates the job's checks.
- **The [web dashboard](Web-Dashboard)** shows an **OVERDUE** badge on late jobs (row chip, drawer, wallboard). The [terminal dashboard](Terminal-Dashboard) paints the same suffix.
- **[MCP](MCP)** observe tools (`cron_list_jobs`, `cron_get_job`) return the same `sla` object.

## The monitor cannot report its own death

`onLate` runs inside the daemon, so it cannot report when the daemon or host stops. Pair it with the external alerts in [metrics with Prometheus](Metrics-with-Prometheus#example-alerts): `time() - cronstable_job_last_success_timestamp_seconds` detects stale successes, and `up == 0` detects failed scrapes. These checks can alert when the daemon cannot send notifications.

## See also

- [Pausing Jobs](Pausing-Jobs): pausing suppresses a job's SLA checks.
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): the reporter options `onLate.report` accepts.
- [Metrics with Prometheus](Metrics-with-Prometheus): the metric families and the external staleness alert to pair with.
- [Failure Detection and Retries](Failure-Detection-and-Retries): the hooks for runs that happened and failed.
- [Hashed Schedules](Hashed-Schedules): stable `H` slots keep "was this run late?" answerable.
- [Configuration Reference](Configuration-Reference#sla-monitoring-and-the-onlate-hook): the schema and load-time validation.
