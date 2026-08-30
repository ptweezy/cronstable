# Reporting (mail, Sentry, shell, webhook)

Six reporters can deliver a job's outcome: Sentry, email (SMTP), an arbitrary
shell command, an HTTP webhook (Slack-compatible by default), end-to-end
encrypted push to paired devices, and the Windows Event Log. Configure them
under the `report` block of the `onFailure`, `onPermanentFailure`,
`onSuccess`, and `onLate` hooks.

This page documents every reporter option, its type and default, the
secret-resolution rules, the jinja2 template variables, and the environment
variables passed to the shell reporter.

## Hooks and when each fires

Each job has four reporting hooks, each with the same `report` block schema:

| Hook | Fires when |
| --- | --- |
| `onFailure` | A job run is detected as failed (see [failure detection and retries](Failure-Detection-and-Retries)). When retries are configured, this fires on each failed attempt. |
| `onPermanentFailure` | A job has failed and all configured retries are exhausted. |
| `onSuccess` | A job run is detected as succeeded. |
| `onLate` | The in-process service level agreement (SLA) monitor latches a breach of one of the job's `sla:` thresholds: too long without a success, a due slot that never started, or a run exceeding its runtime bound. Fires once per breach, not per evaluation. See [late-run detection](Late-Run-Detection). |

All four hooks accept the identical `report` block (`sentry`, `mail`, `shell`,
`webhook`, `push`). The default report configuration applies independently to
each hook (`_REPORT_DEFAULTS` is deep-copied into each), so configuring one hook
does not affect the others.

`onLate`'s defaults differ in wording only, because an SLA breach has no run
outcome to describe:

- Its default mail subject is `Cron job '{{name}}' is overdue ({{sla_check}})`.
- Its default body names the check, threshold, observed value, and last success.
- Its default webhook body wraps the same text in the Slack-compatible
  `{"text": ...}` shape.
- Its default sentry fingerprint is `["cronstable", "sla", "{{ name }}"]`, which
  groups breaches apart from run failures.

A run that is deliberately terminated to make way for a newer instance
(`concurrencyPolicy: Replace`) is not treated as a failure and is neither
reported nor retried. See [concurrency and timeouts](Concurrency-and-Timeouts).

### All six reporters always run

For any given hook, cronstable always invokes all six reporters concurrently
(`asyncio.gather` with `return_exceptions=True`). A reporter that is not
configured returns early and does nothing:

- Sentry returns if no `dsn` source is set.
- Mail returns if `to` or `from` is unset.
- Shell returns if `command` is unset (`None`).
- Webhook returns if no `url` source is set.
- Push returns if `enabled` is `false` (the default).
- Event Log returns if `enabled` is `false` (the default), and again on
  any platform that is not Windows.

An exception raised by one reporter is logged at `ERROR` level (with traceback)
and does not prevent the other reporters from running, nor does it propagate to
the scheduler.

## Templating

`subject`, `body` (mail), `body` and each `fingerprint` entry (sentry), and
`body` (webhook) are
[jinja2](https://jinja.palletsprojects.com/) templates. Each distinct template
source is compiled once and cached for the process lifetime (`lru_cache`), so
the same template string is not recompiled on every report.

These variables are available when rendering any report template:

| Variable | Type | Description |
| --- | --- | --- |
| `name` | str | Job name. |
| `success` | bool | `True` when the job is considered successful (no fail reason). |
| `fail_reason` | str or None | Human-readable reason the job failed, or `None` on success. |
| `stdout` | str or None | Captured standard output (`None` if `captureStdout` is off). |
| `stderr` | str or None | Captured standard error (`None` if `captureStderr` is off). |
| `exit_code` | int or None | Process exit code (`retcode`). |
| `command` | str or list | The job's command. |
| `shell` | str | The job's shell. |
| `environment` | dict or None | The subprocess environment (`None` when the job defines no `environment`). |
| `host` | str | The daemon's host name (`os.environ["HOSTNAME"]`, forced to the system hostname at startup). It names which node ran the job. Always set. |
| `schedule` | str | The job's schedule as a crontab line; an object-form `schedule:` is rendered to the same string every other consumer shows (such as `CRONSTABLE_JOB_SCHEDULE`). |
| `started_at` | str or None | ISO-8601 instant the run started, or `None` before it starts / on a failed launch (and always `None` on an [`onLate`](Late-Run-Detection) breach, which describes a run that did not happen). |
| `run_id` | str or None | The run's id in the [durable run ledger](Durable-State), or `None` when no `state:` store is configured (and `None` on an `onLate` breach). |
| `cpu_seconds` | float or None | Total CPU time (user + system) of the run's process tree, or `None` when the job is not [`monitorResources`](Configuration-Reference#metrics)-monitored (see [resource monitoring](Resource-Monitoring)). |
| `cpu_user_seconds` / `cpu_system_seconds` | float or None | The user- and system-mode components of `cpu_seconds`. |
| `max_rss_bytes` | int or None | Peak resident-set size (bytes) observed during the run, or `None` when unmonitored. |

An [`onLate`](Late-Run-Detection) dispatch renders the same variable set with
the run-shaped fields empty: `success` is `False`, `fail_reason` is
`sla: <check> breached`, and `stdout`/`stderr`/`exit_code`/`started_at`/`run_id`
are `None`. It adds four breach variables: `sla_check`, `threshold_seconds`,
`observed_seconds`, and `last_success_at`. `host` and `schedule` are still
populated, because they describe the job even when it did not run. See
[late-run detection](Late-Run-Detection#the-onlate-report).

The README's variable list omits `fail_reason`; the code provides it, and the
default body template uses it. To capture output for reports, enable
`captureStderr` (on by default), `captureStdout`, or both. See
[output capturing](Output-Capturing).

### Default templates

The mail subject, also prepended to the sentry body, uses:

```text
Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}
```

The default body (`DEFAULT_BODY_TEMPLATE`) prints the fail reason (when set)
followed by captured stdout/stderr, or `(no output was captured)`:

```text
{% if fail_reason -%}
(job failed because {{fail_reason}})
{% endif %}
{% if stdout and stderr -%}
STDOUT:
---
{{stdout}}
---
STDERR:
{{stderr}}
{% elif stdout -%}
{{stdout}}
{% elif stderr -%}
{{stderr}}
{% else -%}
(no output was captured)
{% endif %}
```

The default sentry `body` concatenates the default subject template, a newline,
and the default body template.

## Mail reporter

Sends an email over SMTP with `aiosmtplib`. Reporting is enabled only when both
`to` and `from` are set; otherwise the mail reporter returns without sending.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `from` | str (required in block) | `None` | Envelope/`From` header. Required for mail reporting to occur. |
| `to` | str (required in block) | `None` | Comma-separated recipient list, used directly as the `To` header. Required for mail reporting to occur. |
| `smtpHost` | str (Opt) | `None` | SMTP server hostname. |
| `smtpPort` | int (Opt) | `25` | SMTP server port. |
| `tls` | bool (Opt) | `false` | Use TLS for the connection (`aiosmtplib` `use_tls`). |
| `starttls` | bool (Opt) | `false` | Issue `STARTTLS` after connecting. |
| `validate_certs` | bool (Opt) | `true` | Validate the server's TLS certificate. See the following note. |
| `html` | bool (Opt) | `false` | Send the body as `text/html` (`set_content` subtype `html`) instead of plain text. |
| `username` | str (Opt) | `None` | SMTP login username. The reporter attempts a login only when both `username` and a resolved `password` are present. |
| `password` | secret block (Opt) | unset | SMTP login password; see [secrets](#secrets). |
| `subject` | str (Opt) | default subject template | jinja2 template for the `Subject` header. |
| `body` | str (Opt) | default body template | jinja2 template for the message body. |

In the strictyaml schema, `from` and `to` are required keys when a `mail` block
is present (they accept an empty value, mapping to `None`), while the remaining
keys are optional. Behaviorally, mail reporting is skipped unless both resolve to
a non-empty value.

Notes on behavior:

- **`validate_certs` defaults to `true`** (changed so that SMTP TLS certificate
  validation is on by default). Unless you set `validate_certs: false`,
  connections to servers with self-signed or otherwise untrusted certificates
  fail.
- The reporter sets an `RFC 5322` `Date` header
  (`email.utils.format_datetime(datetime.now(timezone.utc))`), for example
  `Wed, 18 Jun 2026 12:34:56 +0000`, not ISO-8601.
- **Empty-body success emails are skipped.** On a success report, if the
  rendered body is empty after stripping whitespace, no email is sent. (Failure
  reports are sent even with an empty body.)
- The reporter always closes the SMTP connection, even if `STARTTLS`, login, or
  sending raises, so a misbehaving server cannot leak one connection per report.
- `html: true` uses `set_content(body, subtype="html")`, which sets the correct
  charset and transfer-encoding for non-ASCII HTML.

Minimal failure-mail example:

```yaml
jobs:
  - name: backup
    command: /usr/local/bin/backup.sh
    schedule: "0 3 * * *"
    captureStderr: true
    onFailure:
      report:
        mail:
          from: cron@example.com
          to: ops@example.com, oncall@example.com
          smtpHost: 127.0.0.1
          smtpPort: 25
          starttls: false
          validate_certs: false
```

HTML success mail with login (password from environment):

```yaml
jobs:
  - name: report-build
    command: render-report
    schedule: "@reboot"
    captureStdout: true
    onSuccess:
      report:
        mail:
          from: cron@example.com
          to: team@example.com
          smtpHost: smtp.example.com
          smtpPort: 587
          starttls: true
          username: cron
          password:
            fromEnvVar: SMTP_PASSWORD
          html: true
          subject: "Build report for {{ name }}"
          body: "{{ stdout }}"
```

## Sentry reporter

Captures a message to [Sentry](https://sentry.io/) with `sentry-sdk`. Reporting
is enabled only when a `dsn` source resolves to a value; otherwise the reporter
returns early.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | secret block (Opt) | all sources `None` | Sentry DSN; see [secrets](#secrets). With no DSN, Sentry reporting is disabled. |
| `fingerprint` | list of str (Opt) | `["cronstable", "{{ environment.HOSTNAME }}", "{{ name }}"]` | jinja2-templated fingerprint lines controlling Sentry issue grouping. **Replaces, not appends, on merge**; see the following note. |
| `level` | str (Opt) | unset (effective `error`) | Sentry severity level (for example, `error`, `warning`, `info`). When unset, the reporter calls `sentry_sdk.capture_message` with `level="error"`. |
| `extra` | map of str to (str/int/bool) (Opt) | unset | Additional key/value context attached to the event. Your map is merged on top of cronstable's always-attached `job`/`exit_code`/`command`/`shell`/`success` context. |
| `body` | str (Opt) | default subject + body template | jinja2 template for the captured message text. |
| `environment` | str (Opt) | `None` | Sentry environment tag. |
| `maxStringLength` | int (Opt) | `8192` | Sets `sentry_sdk.utils.MAX_STRING_LENGTH` (max length before Sentry truncates strings). |

Notes on behavior:

- The reporter initializes the Sentry client once per `(dsn, environment)` pair
  and caches it, rebuilding only when one of those changes, not on every
  report.
- When set (and truthy), `maxStringLength` mutates the process-global
  `sentry_sdk.utils.MAX_STRING_LENGTH`.
- In addition to any `extra` you supply, cronstable always attaches `job`,
  `exit_code`, `command`, `shell`, and `success` to the event's extra context.
  Your `extra` map is merged on top of these.
- Capture uses an isolated scope (`sentry_sdk.new_scope()`); the reporter
  applies the configured `fingerprint` and extras per event.

**Fingerprint merge semantics:** `fingerprint` is a replace-not-append setting.
When a `defaults` block or a job supplies its own `fingerprint`, it overrides the
default list entirely instead of being concatenated onto the three default
entries. This is a deliberate special case in the config merge, so you can
customize Sentry issue grouping.

All other list-valued options merge by concatenation; `environment` (the job env
list) merges by key. See
[includes, defaults, and multi-file config](Includes-and-Defaults).

Example:

```yaml
jobs:
  - name: ingest
    command: run-ingest
    schedule: "*/5 * * * *"
    captureStderr: true
    onFailure:
      report:
        sentry:
          dsn:
            fromEnvVar: SENTRY_DSN
          level: warning
          environment: production
          fingerprint:
            - ingest-job
            - "{{ name }}"
          extra:
            datacenter: dc1
            shard: 3
```

## Shell reporter

Runs a user-supplied command, passing job state through `CRONSTABLE_*` environment
variables.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `shell` | str (Opt) | `/bin/sh` (POSIX); empty (Windows) | Shell used when `command` is a string. The default is platform-specific (`platform.DEFAULT_SHELL`) and mirrors the job-level `shell` default: `/bin/sh` on POSIX, but empty (`""`) on Windows. An empty default routes a string `command` through the native command processor (`%ComSpec%` / `cmd.exe`). See [running on Windows](Running-on-Windows). |
| `command` | str or list of str (required in block) | `None` | The command to run. A list runs directly (argv). A string runs through `shell -c` when `shell` is set, or through the system default shell when `shell` is empty (the Windows default; see the following execution model). Required key when a `shell` block is present; reporting is skipped if it resolves to nothing. |
| `timeout` | float (Opt) | `60` | Hard bound, in seconds, on the reporter command. Reports run inline on the daemon's single job-completion loop, so a notify script that never exits (`curl` with no `--max-time`, a script that reads stdin) would otherwise freeze completion handling for *every* job in the daemon. On expiry, the daemon stops the reporter's whole process group (descendants included) and logs the timeout at `ERROR` level. |

Execution model:

- If `command` is a **list**, the daemon runs it directly with
  `asyncio.create_subprocess_exec` (no shell).
- If `command` is a **string** and `shell` is set (on POSIX the default
  `/bin/sh` applies), the daemon runs it as `[shell, <flag>, command]` with
  `asyncio.create_subprocess_exec`. The flag is per-shell: `/c` for
  `cmd`/`cmd.exe`, `-c` for everything else.
- If `command` is a **string** and `shell` resolves to a falsy value (such as
  `shell: ""`), the daemon passes the string to
  `asyncio.create_subprocess_shell`, which runs it through the system default
  shell. **This is the Windows default**, where the empty `shell` makes the
  string run through the native command processor (`cmd.exe` through
  `%ComSpec%`).

  To use PowerShell or another interpreter on Windows, set `shell:` explicitly,
  or pass `command` as a list to bypass the shell entirely. See
  [running on Windows](Running-on-Windows).
- The reporter does not fail the job. A failure to launch the command is logged
  with a traceback, and the reporter returns. A nonzero exit code from the
  command is logged at `ERROR` level, without a spurious traceback.
- The reporter command runs in its own process group. If it exceeds `timeout`
  (default 60 seconds), the daemon stops it together with anything it spawned,
  so an unresponsive notify script cannot stall the daemon's job-completion
  handling.

### Environment variables

The shell command inherits the full environment of the cronstable process, plus the
following variables describing the job outcome:

| Variable | Value |
| --- | --- |
| `CRONSTABLE_FAIL_REASON` | The fail reason string, or empty string on success. |
| `CRONSTABLE_FAILED` | `"1"` if the job failed, `"0"` otherwise. |
| `CRONSTABLE_JOB_NAME` | The job name. |
| `CRONSTABLE_JOB_COMMAND` | The job command; a list command is joined with spaces. |
| `CRONSTABLE_JOB_SCHEDULE` | The job's unparsed schedule string. |
| `CRONSTABLE_RETCODE` | The process exit code, as a string. |
| `CRONSTABLE_STDERR` | Captured stderr (possibly truncated; see truncation later). |
| `CRONSTABLE_STDOUT` | Captured stdout (possibly truncated; see truncation later). |
| `CRONSTABLE_STDERR_TRUNCATED` | `"1"` if `CRONSTABLE_STDERR` was truncated, `"0"` otherwise. |
| `CRONSTABLE_STDOUT_TRUNCATED` | `"1"` if `CRONSTABLE_STDOUT` was truncated, `"0"` otherwise. |
| `CRONSTABLE_CPU_SECONDS` | Total CPU seconds of the run's process tree, or empty string when the job is not [`monitorResources`](Configuration-Reference#metrics)-monitored (see [resource monitoring](Resource-Monitoring)). |
| `CRONSTABLE_MAX_RSS_BYTES` | Peak resident-set size in bytes, or empty string when unmonitored. |
| `CRONSTABLE_HOST` | The daemon's host name: which node ran the job. Always set. |
| `CRONSTABLE_RUN_ID` | The run's id in the [durable run ledger](Durable-State), or empty string when no `state:` store is configured, and on an `onLate` dispatch, which has no run. |
| `CRONSTABLE_STARTED_AT` | ISO-8601 instant the run started, or empty string on an `onLate` dispatch, which describes a run that did not happen. |
| `CRONSTABLE_SLA_CHECK` | On an [`onLate`](Late-Run-Detection) dispatch, the breached check (`maxTimeSinceSuccess`, `lateAfter`, or `maxRuntime`); empty string on run reports. |
| `CRONSTABLE_SLA_THRESHOLD_SECONDS` | The breached check's configured threshold; empty string on run reports. |
| `CRONSTABLE_SLA_OBSERVED_SECONDS` | The measured value that breached it; empty string on run reports. |
| `CRONSTABLE_LAST_SUCCESS_AT` | ISO-8601 instant of the job's last known success, or empty string on run reports and when no success is on record. |

**Truncation:** stdout and stderr can be large, and there are OS limits on
argument/environment sizes. cronstable truncates each stream to a maximum of
**16 KiB** (`1024 * 16`) when either stream individually, or the two combined,
exceeds that limit. `CRONSTABLE_STDERR_TRUNCATED` / `CRONSTABLE_STDOUT_TRUNCATED`
indicate per-stream whether truncation occurred. The README lists the first eight
variables but omits the `*_TRUNCATED` pair; the code sets both.

Example:

```yaml
jobs:
  - name: ping-job
    command: do-work
    shell: /bin/bash
    schedule: "* * * * *"
    onFailure:
      report:
        shell:
          shell: /bin/bash
          command: echo "job $CRONSTABLE_JOB_NAME failed with code $CRONSTABLE_RETCODE"
```

This POSIX-shaped example (a `/bin/bash` shell and `$VAR` syntax) won't run as
written on Windows. There, either leave `shell` unset (the command runs through
`cmd.exe`, with `%VAR%` syntax) or set `shell:` to a PowerShell path. See
[running on Windows](Running-on-Windows).

List form (no shell):

```yaml
        shell:
          command:
            - /usr/local/bin/notify
            - --job
            - "failed"
```

## Webhook reporter

Sends an HTTP request (POST by default) to a configured URL, with a
jinja2-templated body. The default body is a JSON `{"text": ...}` payload
carrying the same subject-plus-body text as the default mail/sentry templates.
[Slack](https://api.slack.com/messaging/webhooks), Mattermost, and Microsoft
Teams incoming webhooks accept that shape with no further configuration.

Override `body` for services expecting a different shape. For example, Discord
expects `{"content": ...}`, and [ntfy](https://ntfy.sh/) takes a plain-text
body. Reporting is enabled only when a `url` source resolves to a value;
otherwise the reporter returns early.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | secret block (Opt) | all sources `None` | The webhook URL; see [secrets](#secrets). Treated as a secret because Slack- and Discord-style webhook URLs embed their token; it is never logged. With no URL, webhook reporting is disabled. |
| `method` | str (Opt) | `POST` | HTTP method for the request. |
| `contentType` | str (Opt) | `application/json` | Value of the `Content-Type` header. |
| `headers` | map of str to str (Opt) | `{}` | Extra request headers, such as `Authorization`. They merge over the `Content-Type` header, so a `Content-Type` entry here wins. |
| `body` | str (Opt) | default webhook body template | jinja2 template for the request body. |
| `timeout` | float (Opt) | `10` | Total request timeout in seconds. |

The default body template JSON-encodes the rendered text with jinja2's
`tojson` filter, so job output containing quotes, newlines, or non-ASCII text
always produces valid JSON:

```text
{"text": {% filter tojson %}<default subject>
<default body>{% endfilter %}}
```

Notes on behavior:

- A response status of 400 or above is logged at `ERROR` level, with up to
  1 KiB of the response text. As with all reporters, it neither fails the job
  nor blocks the other reporters. The report dispatcher logs connection errors
  and timeouts the same way.
- The webhook URL is never written to the logs, in either the success or the
  error path.

Slack (or Mattermost/Teams) failure notification:

```yaml
jobs:
  - name: backup
    command: /usr/local/bin/backup.sh
    schedule: "0 3 * * *"
    captureStderr: true
    onFailure:
      report:
        webhook:
          url:
            fromEnvVar: SLACK_WEBHOOK_URL
```

Discord (custom body shape), with the URL read from a mounted secret file:

```yaml
        webhook:
          url:
            fromFile: /etc/secrets/discord-webhook
          body: >-
            {"content": {% filter tojson %}Cron job '{{ name }}'
            {% if success %}completed{% else %}failed:
            {{ fail_reason }}{% endif %}{% endfilter %}}
```

ntfy (plain-text body, priority in a header):

```yaml
        webhook:
          url:
            value: https://ntfy.sh/my-alerts
          contentType: text/plain
          headers:
            Title: cronstable job failure
            Priority: high
          body: "Cron job '{{ name }}' failed: {{ fail_reason }}"
```

`headers` values are sent verbatim; only `body` is a jinja2 template.

## Push reporter

Sends an end-to-end encrypted alert to every paired device through a hosted
relay. The payload is sealed to each device's public key, under the sealing
suite the device registered at pairing (see
[push notifications](Push-Notifications#pairing-devices)), before it leaves
the daemon, so the relay forwards ciphertext it cannot read. The companion app decrypts and renders the notification on the
device. Reporting occurs only when `enabled` is `true`; otherwise the reporter
returns early.

It requires the following, enforced at config load:

- The `push` extra (`pip install "cronstable[push]"`).
- A daemon-global `push:` section naming the relay and the device-registry
  storage.

It also refuses a routable web listener with no `web.authToken`, because the
pairing endpoints would be unauthenticated.

The alert goes to every paired device at once, so an unreachable relay costs one
`push.relay.timeout` in total rather than one per device. Pairing, sealing
suites, storage, size limits, and the trust model are documented on
[push notifications](Push-Notifications).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool (Opt) | `false` | Opt this hook into the push channel. Enabling it anywhere requires a `push:` section (a `ConfigError` otherwise). |
| `priority` | `time-sensitive` or `passive` (Opt) | `time-sensitive` | Relayed to APNs as the interruption level: `time-sensitive` breaks through scheduled summaries, `passive` does not. |
| `includeLogTail` | bool (Opt) | `true` | Carry the last captured output lines (stderr when captured, else stdout, up to 40 lines) inside the sealed payload, trimmed oldest-first to fit the size cap. |

Example:

```yaml
push:
  relay:
    url: https://relay.example.net/v1/notify
  devicesFile: /var/lib/cronstable/devices.json

jobs:
  - name: backup
    command: /usr/local/bin/backup.sh
    schedule: "0 3 * * *"
    captureStderr: true
    onFailure:
      report:
        push:
          enabled: true
          priority: time-sensitive
```

Notes on behavior:

- This block only opts a hook in. The relay endpoint and the paired-device
  registry live in the daemon-global `push:` section (see
  [the push section](Push-Notifications#the-push-section)).
- Unlike the other reporters, there is no template. The sealed payload is a
  fixed JSON shape (name, kind, host, timestamp, run context, optional log
  tail) that the companion app renders after decrypting. See
  [what an alert contains](Push-Notifications#size-limits-and-what-an-alert-contains).
- The alert fans out to every paired device. A sealing failure or relay
  outage is logged per device and, like every reporter error, neither fails
  the job nor blocks the other reporters. With no device paired, the alert
  is dropped with a warning naming the pairing endpoint.
- The same block under `notify.report`, described later, pushes daemon events:
  directed acyclic graph (DAG) failures, approval gates, and leadership/quorum
  changes.

## Windows Event Log reporter

Writes each outcome to the Windows Event Log. Windows only: on any other
platform the reporter returns early, and the configuration load warns once,
naming every hook that enabled it. It needs no extra and no dependency.

```yaml
onFailure:
  report:
    eventlog:
      enabled: true
      source: cronstable
      includeOutput: false
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Write records for this hook. |
| `source` | string | `cronstable` | Event source name. Refused at load when empty, when it contains a path separator, or when it names a log (`Application`, `System`, `Security`); only enabled blocks are checked. |
| `includeOutput` | bool | `false` | Carry a bounded output tail as the last insertion string. |

Notes on behavior:

- Unlike the other reporters, there is no template. A record is a stable
  event ID plus a fixed, ordered set of insertion strings, because consumers
  key on the ID and the field positions. A free-text override of either is a
  contract this reporter cannot keep. For prose, use the shell or webhook
  reporter.
- Writes go to a background thread, so a busy Event Log service can never
  delay a job's completion handling. A graceful shutdown drains the queue
  with a five-second bound. A hard kill drops whatever it still held.
- `includeOutput` is off by default, the opposite of push's
  `includeLogTail`, because every local account can read the Application log,
  whereas a push payload is sealed to a paired device's key.

See [Windows Event Log](Windows-Event-Log) for the event-ID table, the
insertion-string positions, the "description cannot be found" preamble, and
the optional source registration.

## Daemon event notifications (`notify:`)

The four preceding hooks report on **job runs**. A separate top-level `notify:`
block reports on **daemon and orchestration events** that are not job runs, over
the same six reporters:

| Event | Fires when |
| --- | --- |
| `dag_failure` | A [DAG](Orchestration-and-DAGs) run reaches a terminal `failed` state. |
| `approval_waiting` | An [approval gate](Orchestration-and-DAGs#approval-gates) begins awaiting a decision (fired once per gate). |
| `leader_change` | This node acquires or loses scheduled-job [leadership](Clustering-and-Leader-Election). |
| `quorum_loss` | This node leaves quorum, so its `Leader` jobs stand down. |

The block carries a `report` block (the identical `sentry` / `mail` / `shell` /
`webhook` / `push` / `eventlog` schema documented earlier) and an optional
`events` allowlist. To report on every event, omit `events`; to filter, list a
subset. A config holds at most one `notify:` block (like `web:`), and it is
picked up on a reload.

```yaml
notify:
  events:              # optional; default is every event
    - dag_failure
    - quorum_loss
  report:
    webhook:
      url:
        fromEnvVar: OPS_WEBHOOK_URL
```

Because a daemon event has no job run, `notify`'s default templates key on the
event instead of the completed/failed job wording. The template variables are:

| Variable | Description |
| --- | --- |
| `event` | The event name (one of the four listed earlier). |
| `subject` | A one-line headline (for example, `DAG 'etl' run 2026-… failed`). |
| `message` | The body detail (for example, `2 task(s) failed: extract, load`). |
| `name` | The subject's name: the DAG name, or this node's name for cluster events. |
| `host` | The daemon's host name. |
| `success` | Always `False` (these are alert-worthy events). |
| event extras | `dag_failure`/`approval_waiting`: `dag`, `run_key`, `run_id`, `taskkey`, `failed_tasks`. `leader_change`: `role`, `is_leader`, `leader`. `quorum_loss`: `quorate`. |

The default mail subject is `cronstable {{ event }}: {{ subject }}`, and the
default body is `{{ message }}`. The default webhook body wraps them in the
Slack-compatible `{"text": …}` shape, and the default sentry fingerprint is
`["cronstable", "{{ event }}", "{{ name }}"]`, which groups events by type and
subject. Override any of them exactly as for a job report.

Notes:

- **DAG task *runs* are reported by the job hooks, not `notify:`.** A DAG task
  is a job invocation, so its own `onFailure`/`onSuccess` hooks fire on its
  runs, whether inherited from the file's
  [`defaults:`](Includes-and-Defaults#defaults-also-cover-dag-tasks) or set per
  task. `notify.dag_failure` fires once for the whole DAG *run* reaching
  `failed`, naming the failed tasks.
- **Regaining leadership or quorum is not paged.** `leader_change` fires on both
  acquire and lose (differentiated by `is_leader`), but a quorum *recovery* is
  logged, not notified. The loss is the alert.
- Notifications are fire-and-forget and never block the scheduler, cluster loop,
  or a DAG advance. A reporter failure is logged and dropped.

## Secrets

The `mail.password`, `sentry.dsn`, and `webhook.url` options are secret blocks
with three mutually exclusive sources. Each is an optional key accepting a
string or empty value:

| Source | Description |
| --- | --- |
| `value` | The secret inline in the config. |
| `fromFile` | Path to a file whose contents (stripped of surrounding whitespace) are the secret. Read as UTF-8, with a leading byte-order mark stripped as well. |
| `fromEnvVar` | Name of an environment variable holding the secret. |

Resolution order is `value`, then `fromFile`, then `fromEnvVar`; the first
non-empty source wins. If none is set, the reporter treats the secret as absent
(Sentry: disabled; mail: no login).

If `fromEnvVar` is set but the named environment variable is unset or empty, the
report is **skipped** and an error is logged; cronstable no longer raises
`KeyError` in this case. For mail, the password env-var *name* is not echoed to
the logs, because it is tied to a secret. For sentry, the DSN env-var name is
logged.

```yaml
        sentry:
          dsn:
            fromFile: /etc/secrets/sentry-dsn
        mail:
          from: cron@example.com
          to: ops@example.com
          smtpHost: 127.0.0.1
          username: cron
          password:
            fromEnvVar: SMTP_PASSWORD
```

## Related pages

- [Push Notifications](Push-Notifications): the end-to-end encrypted channel behind the push reporter, and how devices are paired
- [Configuration Reference](Configuration-Reference)
- [Failure Detection and Retries](Failure-Detection-and-Retries)
- [Late-Run Detection](Late-Run-Detection): the `sla:` thresholds behind the `onLate` hook
- [Output Capturing](Output-Capturing)
- [Metrics with statsd](Metrics-with-Statsd)
- [Resource Monitoring](Resource-Monitoring)
- [Includes, Defaults, and Multi-File Config](Includes-and-Defaults)
- [Running on Windows](Running-on-Windows)
