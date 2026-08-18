# Troubleshooting and FAQ

A problem -> cause -> fix reference for common cronstable failures, grounded in the
source. Each entry names the exact configuration option, default, or code path
involved. For full option semantics, see the
[configuration reference](Configuration-Reference). For deployment specifics, see
[production and container deployment](Production-Deployment).

## Startup and configuration loading

### "configuration file not found"

**Symptom.** cronstable exits immediately with (the resolved default path filled
in):

```text
cronstable error: configuration file not found at the default location (<default path>). Run `cronstable init` to create a starter configuration there, or point -c/--config at an existing file or directory.
```

followed by the argument help, and exit code `1`.

**Cause.** `__main__.py` defaults `-c`/`--config` to a platform-specific
`CONFIG_DEFAULT = platform.DEFAULT_CONFIG_PATH`. On POSIX that is
`/etc/cronstable.d`. On Windows it is `%ProgramData%\cronstable` when that
directory holds configuration, otherwise `%APPDATA%\cronstable` (for example
`C:\Users\<you>\AppData\Roaming\cronstable`, falling back to the user profile `~`
if `APPDATA` is unset).

When that default is in effect *and* the path does not exist
(`args.config == CONFIG_DEFAULT and not os.path.exists(args.config)`), cronstable
prints the error and exits before constructing the scheduler. The not-found special
case keys off the *platform default value*, not the literal string
`/etc/cronstable.d`. See [running on Windows](Running-on-Windows).

**Fix.** Run `cronstable init` (optionally with a target directory) to create the
directory with a commented starter file, or create it yourself and place
`*.yaml`/`*.yml` files in it, or pass an explicit path with `-c FILE-OR-DIR` (a
single file or a directory). See the [command-line reference](CLI-Reference).

The error text appears only for the *default* path. An explicit `-c` pointing at
a missing file instead surfaces as a `ConfigError` (see the next entry).

### "Configuration error" / a missing or unreadable explicit config file

**Symptom.** `Configuration error: <message>` is logged, and cronstable exits `1`.

**Cause.** Any `ConfigError` raised during initial parse stops startup
(`__main__.py` wraps `Cron(args.config)` and exits on `ConfigError`). When `-c`
points at a single file that is missing or unreadable, `parse_config` catches the
`OSError` and re-raises it as a clean `ConfigError` with the OS message.

**Fix.** Correct the path, permissions, or YAML. Run `cronstable -v -c <path>` to
validate without starting the scheduler. On success it logs `Configuration is valid.`
and exits `0`. See
[includes, defaults, and multi-file config](Includes-and-Defaults).

### Reload errors do not crash a running daemon

**Symptom.** After editing a live config, the log shows
`Error in configuration file(s), so not updating any of the config.:` (followed by
the parse error) but jobs keep running with the old config.

**Cause.** This is by design. The scheduler re-reads the config each wakeup. If
`update_config()` raises `ConfigError`, the loop logs it and keeps the
previously-loaded `cron_jobs` (the assignment only happens on a successful parse).
This applies only to reloads. A parse failure at *initial* startup still exits `1`.

**Fix.** Fix the YAML; the next wakeup picks it up. A `logging` section that was
broken and is later corrected is also re-applied on reload without a restart
(logging config is only marked applied on success).

## Standalone binary under a read-only root filesystem

### Binary aborts at startup: "Could not create temporary directory" / "Operation not permitted"

The standalone binary self-extracts on each start and needs a temp directory
that is both writable and executable. Under a read-only root filesystem, `/tmp`
fails that test and startup stops with `Could not create temporary directory` or
`Operation not permitted`.

For the Docker `--tmpfs` `exec` recipe, Kubernetes `emptyDir` mount, and
`TMPDIR` override, see the
[standalone binary temp-directory requirement](Installation#standalone-binary-temp-directory-requirement)
on the Installation page.

## Per-job user/group switching

### "cronstable is not running as superuser"

**Symptom.** Startup (or reload) fails with:

```text
Job <name> wants to change user or group, but cronstable is not running as superuser
```

**Cause.** A job sets `user` and/or `group`. `_resolve_user_group` raises this
`ConfigError` whenever `self.uid` or `self.gid` is set and `os.geteuid() != 0`.
Dropping to another user requires the daemon itself to start as root.

**Windows note.** Per-job `user`/`group` switching is POSIX-only (Windows has no
setuid/setgid model). On Windows a job with `user` or `group` set raises a config
error *before* the superuser/`geteuid` check is ever reached, verbatim:
`Job <name>: changing user/group is not supported on Windows`.

The "not running as superuser", "User not found", and "Group not found" errors and
the numeric-uid passwd-database behavior described later are therefore all
POSIX-only (they require the `pwd`/`grp` databases and `os.geteuid()`). The fix on
Windows is to remove the `user`/`group` fields. See
[running on Windows](Running-on-Windows).

**Fix.** If you need per-job privilege drop, run the daemon as root, or remove the
`user`/`group` fields from the job. Related `ConfigError`s from the same code
path:

- `User not found: '<user>'`: a string `user` is not in the passwd database
  (`getpwnam` raised `KeyError`).
- `Group not found: '<group>'`: a string `group` is not in the group database.

A numeric `user` without an explicit `group` derives its primary gid (and login name,
used for supplementary groups) from the passwd database. If the uid is not in the
database, the gid is left unset and supplementary groups are cleared. See
[commands and environment](Commands-and-Environment).

## Web control API

### API serves without authentication / refuses to start

**Symptom (intended hardening).** With `web.authToken` configured, cronstable either
logs `web: requiring bearer-token authentication` and requires
`Authorization: Bearer <token>` on every route, or raises:

```text
web.authToken is configured but resolved to an empty token; refusing to start the web API without authentication
```

That bearer-token requirement has one exception. With `web.anonymousScopes` set,
credential-less requests hold read-only view scope, and the log line gains
`; anonymous requests granted scopes: view`.

**Cause.** `_resolve_web_token` fails closed. If `authToken` is present but resolves
to an empty string (an unset `fromEnvVar`, an empty/missing `fromFile`, or an empty
`value`), it raises `ConfigError` rather than silently serving the control API
unauthenticated. A `fromFile` that cannot be read raises
`web.authToken.fromFile could not be read: …`. If `authToken` is entirely absent, the
API listens without auth (this is the default).

**Fix.** Ensure exactly one of `value`/`fromFile`/`fromEnvVar` resolves to a
non-empty secret. Precedence is `value`, then `fromFile`, then `fromEnvVar`. The
`Bearer` scheme is matched case-insensitively and the token is compared in constant
time (`hmac.compare_digest`). A wrong or absent token returns `401`. See the
[HTTP control API](HTTP-API).

### Web listen URL is ignored

**Symptom.** A `web.listen` entry never accepts connections; the log shows
`web: could not listen on <url>: <error>` or
`Ignoring web listen url <url>: …`.

**Cause.** Per-address failures are warned-and-skipped, not fatal. A malformed
`http://` URL (missing host or port) or an unsupported scheme raises `ValueError`
internally and is skipped. A bind `OSError` (port in use, permission, bad socket
path) is likewise skipped. Only `http://` and `unix://` schemes are supported. The
`web: started listening on <url>` message is logged only after the bind succeeds.

**Windows note.** `unix://` listeners are *not* supported on Windows (the Proactor
event loop lacks `create_unix_server`). Such a listen URL is skipped with the
verbatim warning
`Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`.
Use an `http://` listener instead. See [running on Windows](Running-on-Windows).

**Fix.** Use a supported scheme with host and port (`http://127.0.0.1:8080`) or a
`unix://` path, and resolve the bind error. For a `unix://` socket on a read-only
root filesystem, point it at a writable volume and optionally set `web.socketMode`
(octal string) for permissions.

This `unix://`/`web.socketMode` guidance is POSIX-only. On Windows, unix
sockets are unavailable, and `socketMode` is irrelevant because it only ever
applies to unix sockets. Use an `http://` listener. See
[running on Windows](Running-on-Windows).

### Duplicate `web` or `logging` across a config directory

**Symptom.** Startup fails with `Multiple 'web' configurations found: first in …, now
in …` (or the same for `logging`).

**Cause.** When `-c` is a directory, `_parse_config_dir` aggregates across files but
allows at most one `web` block and one `logging` block total. A second one raises
`ConfigError`. (Within `include` chains the equivalent errors are `multiple web
configs` / `multiple logging configs`.)

**Fix.** Keep `web` and `logging` in a single file. See
[includes, defaults, and multi-file config](Includes-and-Defaults) and
[logging configuration](Logging-Configuration).

## Scheduling

### A job runs immediately on startup (or never runs at startup)

**Symptom.** A job fires the moment cronstable starts, or you expected one to fire at
startup and it did not.

**Cause.** At startup (`startup=True`), `job_should_run` returns `True` *only* for
jobs whose schedule is the literal string `@reboot`; all CronTab-scheduled jobs return
`False` and wait for their next matching minute. The daemon wakes aligned to the start
of each minute and runs a CronTab job when `crontab.test(now.replace(second=0))`
matches.

**Fix.** Use `schedule: "@reboot"` for run-on-start behavior; use a normal crontab
expression or schedule object otherwise. See
[schedules and timezones](Schedules-and-Timezones).

### A disabled job never runs

**Symptom.** A job with `enabled: false` is skipped entirely. `GET /status` reports it
as `disabled`, and `POST /jobs/<name>/start` returns `409 Conflict`
(`job '<name>' is disabled`).

**Cause.** `enabled` defaults to `true`. Apart from config validation, cronstable
ignores `enabled: false` jobs: `job_should_run` short-circuits to `False`, and the
web API refuses to launch them.

**Fix.** Set `enabled: true` (or remove the field) to run the job.

### A `second` schedule does not fire more than once a minute

**Symptom.** A schedule with a `second` field (or a seven-field crontab string) only
seems to run once a minute.

**Cause.** Second-level scheduling requires a **seven-field** crontab string
(`second minute hour dayOfMonth month dayOfWeek year`) or the object `second:` key.
A common mistake is writing a **six-field** string like `"*/15 * * * * *"` expecting
"every 15 seconds".

A six-field line has no seconds column: its *leading* field is still the **minute**,
and the extra *trailing* sixth field is the **year** (`*` = any year). So
`"*/15 * * * * *"` actually runs every 15 **minutes** (at second 0), not every 15
seconds. Add the seventh field (`"*/15 * * * * * *"`, whose leading field is the
second), or use the object form (`second: "*/15"`).

**Fix.** Use the object `second:` key, or a full seven-field string. See
[second-level schedules](Schedules-and-Timezones#second-level-schedules).
Second-level scheduling is a YAML feature; [classic crontab files](Classic-Crontabs)
stay five-field and minute-granular.

### Unknown timezone

**Symptom.** Startup fails with `unknown timezone: <value>`.

**Cause.** `_resolve_timezone` calls `ZoneInfo(timezone)`; a
`ZoneInfoNotFoundError`/`ValueError` is re-raised as `ConfigError`. On slim images
that lack the system tz database, cronstable depends on the bundled `tzdata` package to
resolve names.

**Fix.** Use a valid IANA name (for example `America/Los_Angeles`). The `timezone`
option takes precedence over `utc`. With neither set, scheduling uses local time only
when `utc: false` (the default `utc` is `true`, meaning UTC). See
[schedules and timezones](Schedules-and-Timezones).

### "include cycle detected"

**Symptom.** Startup fails with `include cycle detected at <path>`.

**Cause.** `parse_config_file` tracks visited absolute paths in a per-top-level-parse
`_seen` set; a file that includes itself directly or transitively raises this
`ConfigError` instead of recursing to `RecursionError`. Two independent files
including a common file are *not* flagged (the set is scoped per top-level parse).

**Fix.** Break the include cycle. Remember that a top-level `defaults` block does not
retro-apply to jobs pulled in by `include`; included jobs arrive fully constructed
with only their own file's defaults. See
[includes, defaults, and multi-file config](Includes-and-Defaults).

### Jobs in a config directory are not loaded

**Symptom.** Some files in the `-c` directory are silently ignored.

**Cause.** `_parse_config_dir` skips an entry when the base name starts with `_`
or `.` (so `_inc.yaml` and dotfiles are excluded), or when its name
is neither YAML (`.yml`/`.yaml` extension) nor a classic crontab (`.crontab`/`.cron`
extension, or a file named `crontab`; see [classic crontabs](Classic-Crontabs)).
Entries are processed in sorted filename order.

**Fix.** Name loadable configs with a non-`_`/non-`.` leading character and a
recognized name (`.yml`/`.yaml` for YAML, `.crontab`/`.cron`/`crontab` for classic
crontabs). Files meant only to be `include`d (conventionally `_*.yaml`) are
intentionally skipped as top-level configs and pulled in by the file that includes
them.

## Failure detection and output

### A job is marked failed on nonzero exit or any stderr

**Symptom.** A job that looks successful is reported as failed. The log shows a
`fail_reason` such as `failsWhen=nonzeroReturn and retcode=<n>` or
`failsWhen=producesStderr and stderr is not empty`.

**Cause.** `fail_reason` is computed from `failsWhen`. The defaults are:

| failsWhen key   | Type | Default | Effect when `true`                                  |
| --------------- | ---- | ------- | --------------------------------------------------- |
| `producesStdout`| Bool | `false` | Any captured stdout marks the job failed            |
| `producesStderr`| Bool | `true`  | Any captured stderr marks the job failed            |
| `nonzeroReturn` | Bool | `true`  | A nonzero exit code marks the job failed            |
| `always`        | Bool | `false` | The job is always considered failed when it exits   |

A job whose command cannot be launched at all is reported as an ordinary failure with
exit code `127`, not an internal error. Either the executable does not exist, or the
job's `workingDirectory` does not. The missing directory is the harder one to read off
a log: it reaches the message only through the `kwargs=` repr of the spawn line, and on
Windows the OS error there (`WinError 267`) names no path of its own. See
[workingDirectory](Commands-and-Environment#workingdirectory).

`producesStderr`/`producesStdout` only apply when the corresponding stream is
captured, and they also fire when output was *discarded* (`saveLimit: 0` still counts
discarded lines as output).

**Fix.** Adjust `failsWhen`. To stop stderr from marking a job failed, set
`producesStderr: false`. `producesStdout` is the only required key in the `failsWhen`
map (strictyaml-required); the other three are optional and take the table's
defaults. See
[failure detection and retries](Failure-Detection-and-Retries).

### A job's `priority` does not take effect on POSIX

**Symptom.** The config says `priority: high` (or `above-normal`), but `top` or
`ps -o ni` shows the job at the same nice value as everything else. Nothing is
logged, and cronstable reports the run as a success.

**Cause.** Lowering a nice value is a raise in priority, and raising a priority
needs `CAP_SYS_NICE` or `RLIMIT_NICE` headroom. A kernel that refuses does not
fail the job: the run goes on at the priority it inherited. That is deliberate,
so an unprivileged host does not turn a minutely job into some 1,440 warnings a day
about a condition that will not change until the deployment does.

A raise is relative to cronstable's own nice value, so on a daemon started at
nice 15 even `below-normal` (nice 10) is a raise.

**Fix.** Look for the one-shot config-load `WARNING` naming the job, where
cronstable flags a priority it may not be able to apply. To see the
refusal itself, run at `DEBUG`: `platform.apply_priority` logs one line per
refused renice naming the level, the pid, and the errno.

To grant the headroom, give the daemon `CAP_SYS_NICE`
(`AmbientCapabilities=CAP_SYS_NICE` in a systemd unit) or raise its `RLIMIT_NICE`.
On Windows an unprivileged account can set every class in the vocabulary, so
nothing is refused there. See [priority](Commands-and-Environment#priority).

### A Windows job's `high` priority does not reach the programs it launches

**Symptom.** A `.cmd` file or a `shell: cmd` job at `priority: high` shows
cmd.exe at High in Task Manager, but the program cmd.exe launches sits at
Normal.

**Cause.** When a child carries no priority-class flag of its own,
`CreateProcess` gives it the creator's class only if the creator is `idle` or
`below-normal`, and `NORMAL` otherwise. The daemon sets the class on the job's own
process, and cmd.exe launches its programs with no class flag, so a raised
class stops at cmd.exe. By that same rule, lowered classes (`idle`,
`below-normal`) do carry down the whole tree.

**Fix.** Name the program that needs the priority as the job's `command`
instead of wrapping it in a `.cmd`, so cronstable creates it directly. POSIX
has no equivalent split: `setpriority(PRIO_PGRP)` covers the group and a later
fork inherits the nice value. See
[process priority](Running-on-Windows#process-priority).

### stderr capture vs. routing

**Symptom.** A job's output does not appear where expected, or is not in failure
reports.

**Cause.** Defaults are `captureStderr: true`, `captureStdout: false`. A stream that
is *not* captured is passed through to cronstable's own stdout/stderr (job stderr to
cronstable stderr). Only captured streams are saved and available to `failsWhen` and
to report templates. Captured lines are prefixed per `streamPrefix`
(default `"[{job_name} {stream_name}] "`).

**Fix.** Enable `captureStdout: true` or keep `captureStderr: true` for the streams
you need in reports or in `producesStdout`/`producesStderr` checks. See
[output capturing](Output-Capturing).

### "ignored a very long line"

**Symptom.** Log warning `job <name>: ignored a very long line`, and that line is
absent from captured output.

**Cause.** Captured streams use an asyncio reader limited to `maxLineLength`
(default `16 * 1024 * 1024` = 16 MiB). A line longer than the limit raises
`ValueError` in the reader, which logs the warning and skips that line.

**Fix.** Raise `maxLineLength` (must be `> 0`; `_validate_numeric_ranges` rejects
non-positive values with a `ConfigError`). Distinct from saved-output truncation:
`saveLimit` (default `4096` lines, `0` disables saving) caps how many lines are
kept, inserting a `[.... N lines discarded ...]` marker. See
[output capturing](Output-Capturing).

### Invalid numeric option values

**Symptom.** Startup fails with `Job <name>: <field> must be …`.

**Cause.** `_validate_numeric_ranges` enforces ranges strictyaml's type check cannot:
`saveLimit >= 0`, `maxLineLength > 0`, `killTimeout >= 0`, `executionTimeout > 0` when
set, `onFailure.retry.maximumRetries >= -1` (`-1` = retry forever),
`initialDelay >= 0`, `maximumDelay > 0`, `backoffMultiplier > 0`.

**Fix.** Use values within those ranges. See
[concurrency and timeouts](Concurrency-and-Timeouts) and
[failure detection and retries](Failure-Detection-and-Retries).

## Reporting

### SMTP TLS certificate failures after upgrading from yacron

**Symptom.** Mail that delivered fine under yacron now fails with a TLS/certificate
validation error.

**Cause.** The mail `validate_certs` default is `True` (a change from yacron;
`_REPORT_DEFAULTS["mail"]["validate_certs"] = True`). The mail reporter passes
`validate_certs=mail["validate_certs"]` to `aiosmtplib.SMTP`, so delivery to servers
with self-signed or otherwise invalid certificates that previously worked silently now
fails.

**Fix.** Fix the server certificate, or set `validate_certs: false` on the `mail`
report block to restore the old behavior. Related mail TLS keys: `tls` (default
`false`, implicit TLS), `starttls` (default `false`). See
[reporting (mail, Sentry, shell, webhook)](Reporting).

Reporter exceptions are logged (`Problem reporting job <name> failure`) and never
crash the scheduler. Reporters run concurrently with `return_exceptions=True`.

### A report (Sentry / mail / shell / webhook) is silently skipped

**Symptom.** No report is sent and there is no exception.

**Cause.** Each reporter early-returns when not configured:

- Sentry returns unless a DSN resolves, and logs
  `sentry: dsn env var '<name>' is not set; not reporting` when `fromEnvVar` is unset.
- Mail returns unless both `to` and `from` are set, and logs
  `mail: password env var is not set; not sending email` when a `fromEnvVar` password
  is unset.
- The shell reporter returns when `command` is `None`.
- The webhook reporter returns unless a `url` resolves, and logs
  `webhook: url env var '<name>' is not set; not reporting` when `fromEnvVar` is unset.

A successful-job mail with an empty rendered body is also skipped.

**Fix.** Provide the required fields. See
[reporting (mail, Sentry, shell, webhook)](Reporting).

### A webhook report logs `request failed` and the URL is not shown

**Symptom.** The log carries `webhook reporter of job <name>: request failed
(<ExceptionType>); check webhook.url and the network`, with no URL and no traceback.

**Cause.** The request never completed. The exception type names which kind of failure
it was:

- `InvalidUrlClientError`: the configured `url` is not a URL aiohttp can build
  (a missing scheme, a non-numeric or out-of-range port, an empty host).
- `ClientConnectorError` and `ClientConnectorDNSError`: the host was not reachable.
- `TimeoutError`: nothing answered within `timeout`. This one covers name
  resolution as well as the endpoint: resolving the host happens inside the same
  `timeout`, so a resolver that does not answer reads exactly like a slow server.
- `UnicodeEncodeError`: the host resolves but idna rejects it (a doubled dot, a
  label over 63 characters).

The URL is deliberately withheld. A Slack or Discord webhook URL embeds its own
token, so `webhook.url` is treated as a secret and never written to the log, in the
same way the HTTP-error branch above it logs the status and response body but not
the URL.

**Fix.** Check `webhook.url` for a typo, starting with the scheme and the port, then
check that the host resolves and is reachable from the daemon. See
[reporting (mail, Sentry, shell, webhook)](Reporting).

## Metrics

### No statsd metrics arrive

**Symptom.** Expected metrics never reach statsd. The daemon emits four per job:
`start`, `stop`, and `success` as gauges (`|g`) and `duration` as a timer
(`|ms`), prefixed by the job's `statsd.prefix`. The log shows at most a warning
`Job <name>: failed to send statsd … metric` or an error
`UDP error received: <exc>`.

**Cause.** statsd is best-effort: metrics go out over fire-and-forget UDP, and an
`OSError` on send (for example an unresolvable host) is logged as a warning rather than
propagated. If the `statsd` block is absent, no statsd metrics are emitted at
all (the [Prometheus endpoint](Metrics-with-Prometheus) is independent of it).

**Fix.** Configure the job's `statsd` block (`host`, `port`, `prefix`, all required)
and verify the host resolves and the UDP path is open. See
[metrics with statsd](Metrics-with-Statsd).

### Prometheus scrapes return 401

**Symptom.** Prometheus marks the cronstable target down with
`server returned HTTP status 401 Unauthorized`; `curl http://host:port/metrics`
returns `401`.

**Cause.** `web.authToken` is configured, and `GET /metrics` requires the bearer
token like every other data endpoint. The only exemption is
`web.metrics.public: true`, which exempts `/metrics` (and only `/metrics`) from
authentication.

**Fix.** Send the token from the scrape configuration (an `authorization` block
with `type: Bearer` and `credentials`). If the scraper cannot send credentials, set
`web.metrics.public: true`; everything else stays gated. See
[metrics with Prometheus](Metrics-with-Prometheus).

### `/metrics` returns 404

**Symptom.** `GET /metrics` returns `404 Not Found`, or nothing listens on the
expected port at all.

**Cause.** The endpoint is served by the web API and is on by default whenever
the web API is enabled, so a `404` means it was disabled explicitly
(`web.metrics: false`, or `web.metrics.enabled: false` in the map form). If the
connection is refused instead, there is no `web` section (or no working
`web.listen` entry), so no web API (and no `/metrics`) is served.

**Fix.** Remove the `metrics: false` or `enabled: false` override (`web.metrics`
left unset means enabled), and make sure a `web.listen` entry binds
successfully. See [metrics with Prometheus](Metrics-with-Prometheus).

## Clustering

These cover the optional `cluster` section (peer attestation, leader election,
and the lease backends). For the full model, see
[clustering and leader election](Clustering-and-Leader-Election).

### `quorate: false` and `Leader` jobs stop running

**Symptom.** `GET /cluster` reports `"quorate": false` and `Leader`-policy jobs
stop firing on every replica (`PreferLeader` jobs keep running).

**Cause.** This node cannot see a quorum (a strict majority) of the cluster, so
it deliberately **stands down** rather than risk a second leader.

On the `gossip` backend that means a minority-side network partition or too many
peers `unreachable`/`untrusted`. On a **lease** backend (`kubernetes`/`etcd`) it
means the coordination store is unreachable or the last read has gone stale (past
one `leaseDurationSeconds`/`ttl`). There, `quorate` means "has a fresh read of the
store", not "sees a majority".

Either way `Leader` **fails closed** (skips) while `PreferLeader` keeps running (it
may double-run during the outage).

**Fix.** Restore a quorum: heal the partition or bring failed peers back so a
majority is mutually reachable (gossip), or restore reachability to the apiserver
or etcd (lease backends). See
[why the quorum gate is safe](Clustering-and-Leader-Election#why-the-quorum-gate-is-safe)
and, for the lease backends, [failure modes](Clustering-and-Leader-Election#failure-modes).

### Duplicate `nodeName` conflict (`conflict: true` with `conflict_names`)

**Symptom.** `GET /cluster` shows `"conflict": true` with the offending name in
`"conflict_names"`, one or more peers show status `conflict`, a banner appears in
the dashboard cluster panel, and an `ERROR` log line reports the duplicate.
`Leader` jobs stand down.

**Cause.** Two processes are running with the **same `nodeName`** (distinguished
by their random per-process instance ids). Because each would elect itself as the
lowest name in its own view, both could lead: a silent double-run. The daemon
detects this and **fails `Leader` jobs closed** until it clears (`PreferLeader`
and `EveryNode` are not gated).

**Fix.** Give every node a distinct `nodeName` (or distinct hostnames, because the
default `nodeName` is the system hostname). The gate is self-healing: it clears
automatically after the duplicate is renamed. See
[unique node names](Clustering-and-Leader-Election#unique-node-names).

### Coordination-policy conflict (`policy_conflict: true` with `conflicting_policies`)

**Symptom.** `GET /cluster` shows `"policy_conflict": true` with the differing
descriptors in `"conflicting_policies"` (and the umbrella `"conflict": true`), a
banner appears in the dashboard cluster panel, and `Leader` jobs stand down.

**Cause.** A quorate peer is advertising a different `distribution` or
`elect_leader` setting than this node. Because those are cluster-wide coordination
settings (not part of the job-set id, so they do not surface as drift), a mismatch
would let nodes coordinate differently and double-run.

It is the **third** trigger of the umbrella `conflict` flag, alongside a duplicate
`nodeName` (`conflict_names`) and a cluster-size disagreement (`size_conflict` /
`conflicting_sizes`). All three stand `Leader` jobs down.

**Fix.** Align `electLeader` and `distribution` across every node and roll the
change out uniformly (one node at a time). The gate clears automatically after the
cluster reconverges on one policy. See
[distribution: one leader, or spread the load](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load)
and [consistent cluster size](Clustering-and-Leader-Election#consistent-cluster-size).

### A peer shows `untrusted`

**Symptom.** A peer's status on `GET /cluster` is `untrusted` (with a TLS error in
`last_error`), and the `peer … is untrusted` `WARNING` appears in the logs. That
peer never counts toward agreement, so quorum can drop.

**Cause (gossip only).** The peer's certificate did not verify: it does not chain
to the configured `tls.ca`, or its SAN/hostname does not match the address it was
reached at (standard TLS hostname pinning, for example the cert at
`cronstable-b.internal:8443` must carry `cronstable-b.internal` as a SAN). This most
often follows a CA roll where the overlap step was skipped, or a node whose cert
refresh lagged.

**Fix.** Restore trust overlap (distribute a CA bundle covering whichever CA the
`untrusted` peers were issued from) or finish rolling the lagging nodes. Ensure
each node's cert SAN matches its peer-list host. Each node reloads within about
1 minute and peers return to `agreed`. See
[cluster peer attestation](Clustering-and-Leader-Election#cluster-peer-attestation)
and [cluster certificate operations](Production-Deployment#cluster-certificate-operations).

### `electLeader` on a 2-node cluster refuses to start

**Symptom.** Startup fails with a `ConfigError`:

```text
cluster.electLeader needs a fault-tolerant cluster, but this config declares
only 2 nodes (1 peer). A quorum of 2 requires both nodes up for either to run,
so it is strictly worse than a single replica. …
```

**Cause.** With `electLeader` and a 2-node cluster the quorum is 2, so **both**
nodes must be up for either to run: lower availability than a single replica and
no failover. The daemon refuses it outright rather than silently degrade. (A 2-node
cluster is fine for attestation-only, without `electLeader`.)

**Fix.** Use 3 or more nodes (an odd count is best), or run a single replica
without `electLeader`. See
[sizing the cluster](Clustering-and-Leader-Election#sizing-the-cluster).

### Even-size warning with `electLeader`

**Symptom.** Startup logs a warning that an even cluster size tolerates no more
failures than the next-lower odd size.

**Cause.** For `size > 2` an even size (4, 6, 10, …) needs the same quorum as the
odd size below it, so the extra node only adds something that can fail. Its
`P(runs)` is equal-or-worse, never better. This is a non-fatal advisory (unlike
the preceding 2-node case, which is rejected).

**Fix.** Prefer an odd size: shrink by one for the same tolerance with one fewer
node, or grow by one to tolerate an extra failure. See
[sizing the cluster](Clustering-and-Leader-Election#sizing-the-cluster).

## Concurrency and termination

### Overlapping runs, skipped runs, or a job killed mid-run

**Symptom.** Two instances of a job run at once, the next run is skipped while one is
in progress, or a running instance is terminated when the next is due.

**Cause.** `concurrencyPolicy` (default `Allow`) governs overlap:

- `Allow` runs concurrently.
- `Forbid` skips the new run while one is still running.
- `Replace` cancels the running instance (marking it `replaced` so it is not
  reported as a failure or retried) and starts a new one.

**Fix.** Set `concurrencyPolicy` to the policy you want. See
[concurrency and timeouts](Concurrency-and-Timeouts).

### A timed-out job is killed forcefully

**Symptom.** Log shows `Job <name> exceeded its executionTimeout …, cancelling it…`
and possibly `Job <name> did not gracefully terminate after <n> seconds, killing it…`.

**Cause.** `executionTimeout` (default unset/`None`) cancels a job still running after
N seconds (recorded internally as retcode `-100`). Cancellation signals the job's
whole process group (each job runs in its own session): `SIGTERM` to the group, then
after `killTimeout` seconds (default `30`) an unconditional `SIGKILL` to the group,
sent even if the main process already exited, so background helpers the command
left behind go down with it.

**Windows note.** The same two-step runs with Windows primitives: the graceful
step is a trappable `CTRL_BREAK_EVENT` to the job's process group
(`signal.SIGBREAK` in Python), and the forced step is a `taskkill /F /T` of
the job's live process tree, `killTimeout` seconds later. A daemon with no
console cannot deliver the break, and a service has no console. There the
graceful step becomes the tree kill immediately, and `killTimeout` adds
nothing. See
[running on Windows](Running-on-Windows).

**Fix.** Raise `executionTimeout`, or give the process more graceful-shutdown time with
`killTimeout` (have the job handle `SIGTERM` on POSIX / `SIGBREAK` on Windows to
use that grace). See
[cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout)
on [concurrency and timeouts](Concurrency-and-Timeouts).

### The Windows service will not start

Read the bootstrap log first: by default
`<config directory>\logs\cronstable-service.log`. A service has no stderr,
so that file is where a startup failure is recorded.

`cronstable service status` decodes the last failure without opening
anything:

| It says | Fix |
| --- | --- |
| `the configuration did not parse` | Run `cronstable -v -c <path>` interactively against the same path. |
| `the service log could not be opened` | The log directory is not writable by LocalSystem. Pass `--log-file` somewhere it is, or `--no-log-file` with a `logging:` section. |
| `the scheduler stopped with an error` | The scheduler raised. The bootstrap log has the traceback. |

If it starts and schedules nothing, check which configuration it read. A
service runs as LocalSystem, whose `%APPDATA%` is
`C:\Windows\System32\config\systemprofile\AppData\Roaming`, not yours, so a
per-user path is not the path you tested. `cronstable service install`
refuses to install the per-user default for that reason. Use a machine-wide
directory and name it with `-c`.

### `install` says a one-file build cannot host a service

It cannot, and this is not a cronstable limitation to work around. The
published one-file `.exe` (which is what winget installs) unpacks itself and
runs the program in a **child** process. The process the Service Control
Manager starts and watches therefore never registers with the service
dispatcher, and the start fails on the Service Control Manager's timeout.

Use one of these:

- Download `cronstable-windows-<arch>.zip` (a one-directory build) and run
  `service install` from its extracted `cronstable.exe`.
- Install the [MSI](Windows-MSI), which registers the service itself.
- Install with pip or pipx.

The `schtasks` recipe on the
[running on Windows](Running-on-Windows#running-unattended) page remains the
fallback for the one-file executable.

### A reinstall fails with "the service is pending deletion"

Something still holds a handle to the removed service, so Windows marked it
for deletion rather than deleting it. The usual causes are the Services
console (`services.msc`) and Task Manager's Services tab. Close them and run
`cronstable service install` again.

### `killTimeout` does nothing under the service

Expected, unless the service was installed with `--console`. The graceful
step of stopping a job is a console control event, and a service has no
console, so stopping a job goes straight to the forced tree kill and there is
nothing for `killTimeout` to bound.

`cronstable service install --console` allocates one. It is off by default
because an allocated console changes what a job inherits. See
[Windows service](Windows-Service#--console-and-job-termination).

## Reference: exit codes used internally

| Code   | Meaning                                                                    |
| ------ | -------------------------------------------------------------------------- |
| `127`  | Command could not be launched (executable or `workingDirectory` not found) |
| `-100` | Job cancelled because it exceeded `executionTimeout`                       |

A missing `workingDirectory` gets the same `127` as a missing executable, and
the path it tried shows up only inside the `kwargs=` repr of the spawn log
line. See [workingDirectory](Commands-and-Environment#workingdirectory).

Two Windows codes worth recognizing in run history:

- `1` is what `taskkill /F` leaves behind. A run reaped by the forced tree kill
  reports it, indistinguishable from a job's own `exit 1`.
- `3221225786` (`0xC000013A`, `STATUS_CONTROL_C_EXIT`) means the process was ended
  by a console control event, typically a job that received the graceful break and
  did not trap it.

These appear in logs and in report template `exit_code`. See
[architecture and internals](Architecture-and-Internals) for the scheduler and
job-lifecycle details, and [logging configuration](Logging-Configuration) for raising
the log level (`-l DEBUG`) when diagnosing scheduling decisions.
