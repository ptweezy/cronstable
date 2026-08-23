# Metrics with statsd

cronstable can emit per-job lifecycle metrics to a [statsd](https://github.com/statsd/statsd) server over UDP. This page documents the `statsd` config block, the exact wire format cronstable emits, and the delivery guarantees (best-effort, fire-and-forget, idempotent stop). statsd is the push-side metrics option. For pull-side scraping, cronstable also serves a built-in [Prometheus endpoint](Metrics-with-Prometheus) on the web API, and you can enable both at once.

## Enabling statsd for a job

Configure statsd per job, or in a [`defaults` block](Includes-and-Defaults), with a `statsd` mapping. The block is optional. If you omit it, no metrics are sent (`DEFAULT_CONFIG["statsd"]` is `None`). When the block is present, the schema requires all three keys.

```yaml
jobs:
  - name: test01
    command: echo "hello"
    schedule: "* * * * *"
    statsd:
      host: my-statsd.example.com
      port: 8125
      prefix: my.cron.jobs.prefix.test01
```

### Options

The schema for the block is `Map({"prefix": Str(), "host": Str(), "port": Int()})`. None of the keys is wrapped in `Opt(...)`, so all three are required whenever `statsd` is set.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | string | (required when `statsd` set) | Hostname or IP of the statsd server. Resolved per send. An unresolvable host produces a warning, not a crash (see [best-effort delivery](#best-effort-delivery)). |
| `port` | int | (required when `statsd` set) | UDP port of the statsd server (commonly `8125`). |
| `prefix` | string | (required when `statsd` set) | Metric name prefix, prepended verbatim to each metric name. No separator is added, so the prefix should not have a trailing dot (cronstable inserts the `.` between the prefix and the metric suffix, for example `<prefix>.start`). |

The whole `statsd` block defaults to absent (`None`). There is no partial default. If you set it, you must provide `host`, `port`, and `prefix`.

## Wire format

Metrics are encoded as UTF-8 statsd lines, one metric per line, ending with a newline (`\n`). They are sent in two UDP datagrams per job run: one at start, one at stop.

### On start

After the subprocess launches, cronstable records `time.perf_counter()` as the start time and sends a single datagram:

```text
<prefix>.start:1|g
```

`|g` is the statsd gauge type. The value is always `1`.

The start metric is sent by `_on_start`, which runs only after the subprocess was successfully created. If the command fails to launch at all (for example, the executable does not exist), `_on_start` is never reached and no start metric is emitted. That run still produces no stop metric either (see the next section).

### On stop

When the job stops (normal exit, timeout, or cancellation), cronstable computes the duration as `time.perf_counter() - start_time`, converts it to milliseconds with `int(round(duration_seconds * 1000))`, and sends a single datagram containing three metrics:

```text
<prefix>.stop:1|g
<prefix>.success:<1|0>|g
<prefix>.duration:<ms>|ms
```

- `<prefix>.stop:1|g`: gauge, always `1`.
- `<prefix>.success:<1|0>|g`: gauge. `1` if the job did **not** fail, `0` if it failed. The value comes from `0 if job.failed else 1`, where `job.failed` is the [failure-detection](Failure-Detection-and-Retries) result (`failsWhen`). A nonzero exit code, output on a watched stream, or `failsWhen: always` therefore produces `success:0`.
- `<prefix>.duration:<ms>|ms`: timer (`|ms`) with no sample-rate suffix. The numeric value is the integer wall-clock duration in milliseconds, measured with `perf_counter` between start and stop. Every run sends exactly one duration observation. (Releases through 1.2.36 appended a literal `@0.1` sample-rate flag here. A dashboard that corrected for it should drop the correction.)

When the job has [`monitorResources`](Configuration-Reference#metrics) on (see [resource monitoring](Resource-Monitoring)), the same stop datagram also carries the run's resource accounting:

```text
<prefix>.cpu:<ms>|ms
<prefix>.max_rss:<bytes>|g
```

- `<prefix>.cpu:<ms>|ms`: timer, the total CPU time (user + system) of the run's process tree in milliseconds. Like `duration`, it is sent once per run with no sample-rate suffix.
- `<prefix>.max_rss:<bytes>|g`: gauge, the peak resident-set size observed during the run, in bytes.

These two lines are omitted for a run that was not monitored (or where sampling caught nothing), so an unmonitored job's stop datagram is unchanged.

A run whose command never launches (the subprocess could not be spawned; `start_failed` is set) emits neither metric. On that path, `wait()` returns early, `_on_stop` (hence `job_stopped`) is never called, and `_on_start` was likewise never reached. Separately, `job_stopped` itself guards on a recorded start time (`if self.start_time is None: return`). That guard suppresses a stop metric for a run stopped without a corresponding `job_started` (for example, the `cancel()`/`wait()` race under `concurrencyPolicy: Replace`).

## Best-effort delivery

statsd metrics are telemetry and never affect job execution or the scheduler.

- **Fire-and-forget UDP.** Each send opens a datagram endpoint (`loop.create_datagram_endpoint(... remote_addr=(host, port))`), writes the message in `connection_made` with `sendto`, then immediately closes the transport. There is no acknowledgment, retry, or delivery confirmation. The network or OS silently drops lost datagrams. Inbound datagrams are ignored.
- **Send failures are caught and logged, never fatal.** Both `_on_start` and `_on_stop` wrap the send in `try/except OSError`. A failure, such as an unresolvable `host`, is logged with `logger.warning(...)` plus `exc_info=True`, and the job proceeds normally. The messages are:
  - `Job <name>: failed to send statsd job_started metric`
  - `Job <name>: failed to send statsd job_stopped metric`
- **UDP protocol errors are logged separately.** The `statsd` logger records asyncio-level UDP errors surfaced through the datagram protocol as `UDP error received: <exc>` (see [logging configuration](Logging-Configuration) for logger names).

## Stop metrics are emitted exactly once per run

The stop metrics are guaranteed to be sent at most once per run, even when two code paths could both reach the stop logic. `_on_stop` is idempotent: it checks a `_stopped` flag and returns early if already stopped, setting the flag before any `await`.

```python
async def _on_stop(self) -> None:
    if self._stopped:
        return
    self._stopped = True
    ...
```

This matters for `concurrencyPolicy: Replace`, where the scheduler may cancel a running job (`cancel()`) while its `wait()` task is also completing. Both call `_on_stop`, but only the first one emits metrics. This guarantee is intentional and tested. Duplicate stop metrics under cancellation were fixed in a prior release. See [concurrency and timeouts](Concurrency-and-Timeouts) for the concurrency policies, and [failure detection and retries](Failure-Detection-and-Retries) for how `success` is computed.

> The `start_time` guard means a forced cancellation also yields a correct duration. The duration is measured from the recorded `perf_counter` start to the moment `job_stopped` runs, regardless of how the process ended (normal exit, `executionTimeout`, or `Replace` cancellation).

## Version notes

- Sending job metrics to statsd was added in yacron 0.6.0 (inherited by cronstable; see `HISTORY.md`).
- statsd reporting is strictly best-effort. A failure to send `job_started`/`job_stopped`, such as an unresolvable statsd host, is logged as a warning instead of propagating out of job start/stop.
- Job stop metrics are emitted exactly once per run. An idempotency guard on `_on_stop` prevents duplicate metrics when `cancel` races `wait` (for example, `concurrencyPolicy=Replace`).
- statsd UDP errors are logged with their detail (`UDP error received: %s`) rather than being dropped.

## See also

- [Metrics with Prometheus](Metrics-with-Prometheus): the pull-side option, a scrapeable `/metrics` endpoint on the web API.
- [Resource Monitoring](Resource-Monitoring): the `monitorResources` sampling behind the `cpu` / `max_rss` lines.
- [Configuration Reference](Configuration-Reference): full per-job option list.
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting): the other outbound notification channels.
- [Failure Detection and Retries](Failure-Detection-and-Retries): how `job.failed` (and thus `success:0`/`success:1`) is determined.
- [Concurrency and Timeouts](Concurrency-and-Timeouts): `concurrencyPolicy` and `executionTimeout`, which interact with stop metrics.
