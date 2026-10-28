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

When that default is in effect and the path is missing, requests to load
configuration (`--validate-config`, `--job-set-id`, `-l`, or `-c`) print the
error and exit before constructing the scheduler. A bare `cronstable` with
no default configuration prints setup guidance and help to stdout and exits
`0`. See [running on Windows](Running-on-Windows).

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
`OSError` and re-raises it as a clean `ConfigError` with the OS message. A
configuration file that is not valid UTF-8 is a `ConfigError` as well:
`Could not load config file '<path>': 'utf-8' codec can't decode byte ...`. A
redirect (`>`) in Windows PowerShell 5.1 writes UTF-16, which is the usual cause
(see [output encoding](Running-on-Windows#output-encoding)).

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

**Cause.** A job sets `user`, `group`, or both. `_resolve_user_group` raises this
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
nonempty secret. Precedence is `value`, then `fromFile`, then `fromEnvVar`. The
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
path) is likewise skipped. Only `http://`, `https://` (which needs `web.tls`), and
`unix://` schemes are supported. The
`web: started listening on <url>` message is logged only after the bind succeeds.

**Windows note.** `unix://` listeners are *not* supported on Windows (the Proactor
event loop lacks `create_unix_server`). Such a listen URL is skipped with the
verbatim warning
`Ignoring web listen url <url>: unix-socket listeners are not supported on this platform`.
Use an `http://` listener instead. See [running on Windows](Running-on-Windows).

**Fix.** Use a supported scheme with host and port (`http://127.0.0.1:8080`, or
`https://127.0.0.1:8443` with `web.tls`) or a `unix://` path, and resolve the bind error. For a `unix://` socket on a read-only
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

### An embedded status badge shows a broken image

**Symptom.** A [status badge](Status-Badges) in a README, a wiki page, or a
home dashboard shows a broken image.

**Cause.** With `web.authToken` or `web.authTokens` configured, the badge
routes require the bearer token like every other data route, and image
fetchers, such as GitHub's image proxy and the browsers that render a wiki
page, can't send one. Unless the badges are public, a badge also carries
`Cross-Origin-Resource-Policy: same-origin` and `frame-ancestors 'self'`,
so a browser refuses to load it from the daemon into a page from another
origin, as an image or in a frame.

**Fix.** Run `curl` on the image URL, with the token if the daemon has one.
A `404` means that no job has the name in the URL, or that
`web.badges: false` (or `web.badges.enabled: false`) turned the routes off,
and `public` has no effect until they are back on. A `200` means that the
badge itself works. A page that loads the badge straight from the daemon,
such as a wiki page or a home dashboard, needs `web.badges.public: true`,
which exempts only the two badge routes from the token and lets pages on
other origins load them. A badge in a GitHub README needs an address that
the internet can reach, and it needs `public` only on a daemon that requires
a token.
See [making badges public](Status-Badges#make-badges-public).

## Scheduling

### A job runs immediately on startup (or never runs at startup)

**Symptom.** A job fires the moment cronstable starts, or you expected one to fire at
startup and it did not.

**Cause.** At startup (`startup=True`), `job_should_run` returns `True` *only* for
jobs whose schedule is the literal string `@reboot`; all CronTab-scheduled jobs return
`False` and wait for their next matching minute. The daemon tests the job's schedule
against the current slot, truncated to the job's resolution (the minute, or the second
for a seven-field schedule), and runs the job on a match.

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

**Cause.** `parse_config_file` receives in `_seen` the absolute paths of the files
whose parse is in progress, from the top-level file down to the file that includes
this one. A file that includes itself, directly or through other files, raises this
`ConfigError` instead of recursing to `RecursionError`. `<path>` is the include path
as joined to the including file's directory. Two files that include a common file
are *not* flagged, because a file under one include is never in the set that another
include receives.

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
`fail_reason` such as `command exited with code <n>` or
`command wrote to stderr (configured to count as a failure)`.

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

`_validate_resource_limits` checks [`resourceLimits`](Resource-Limits):
`cpu` between `0.01` and `1000000` CPUs, `memory` at least 1 MiB and below
8 EiB, and `processes` between `1` and `4194304`. `max` lifts a cap. A value
that does not parse fails with `Job <name>: resourceLimits.<key>: cannot
read '<value>' as ...`, followed by the forms the key accepts.

**Fix.** Use values within those ranges. See
[concurrency and timeouts](Concurrency-and-Timeouts) and
[failure detection and retries](Failure-Detection-and-Retries).

## Reporting

### SMTP TLS certificate validation failures

**Symptom.** Mail delivery fails with a TLS or certificate validation error.

**Cause.** `validate_certs` defaults to `True`
(`_REPORT_DEFAULTS["mail"]["validate_certs"] = True`). The mail reporter passes
`validate_certs=mail["validate_certs"]` to `aiosmtplib.SMTP`, so delivery to servers
with self-signed or otherwise invalid certificates fails.

**Fix.** Fix the server certificate, or set `validate_certs: false` on the `mail`
report block to skip certificate validation. Related mail TLS keys: `tls` (default
`false`, implicit TLS) and `starttls` (default `false`, upgrades the connection with
STARTTLS). With both `false`, the connection is unencrypted and no certificate is
checked. See [reporting (mail, Sentry, shell, webhook)](Reporting).

Reporter exceptions are logged (`Problem reporting job <name> failure`) and never
crash the scheduler. Reporters run concurrently with `return_exceptions=True`.

### "sets both tls and starttls"

**Symptom.** The config fails to load with
`Job <name>: <hook>.report.mail sets both tls and starttls, which are mutually exclusive`
(or `notify.report.mail ...` for the `notify:` block).

**Cause.** A mail block with `to` and `from` has `tls: true` and `starttls: true`
after `defaults:` is merged in. `tls` uses TLS from the start of the connection, and
`starttls` upgrades a plain connection, so one connection can't do both.

**Fix.** Keep the key that matches the server's port, usually `tls` for 465 and
`starttls` for 587, and set the other to `false` in the block the error names.

### "mail reporting sends its SMTP login unencrypted"

**Symptom.** The config load logs the warning
`mail reporting sends its SMTP login unencrypted to <host>:<port> as '<username>'`.

**Cause.** A mail block with `to`, `from`, `username`, and a `password` source has
`tls` and `starttls` both `false` after `defaults:` is merged in, so the reporter
sends the login in plain text. Only an `smtpHost` that is unset, `localhost`, or a
loopback address skips the warning. cronstable doesn't resolve names, so the
warning also appears for a name that points at this machine. Each load logs one
warning per host, port, and username, however many jobs share them.

**Fix.** Set `starttls: true` if the server offers STARTTLS (usually port 587), or
`tls: true` for implicit TLS (usually port 465).

### A report (Sentry / mail / shell / webhook) is silently skipped

**Symptom.** No report is sent and there is no exception.

**Cause.** Each reporter early-returns when not configured:

- Sentry returns unless a DSN resolves, and logs
  `sentry: sentry.dsn is configured but resolved to an empty secret; not reporting`
  when the configured source (for example, an unset `fromEnvVar`) is empty.
- Mail returns unless both `to` and `from` are set, and logs
  `mail: mail.password is configured but resolved to an empty secret; not sending email`
  when a configured password source is empty.
- The shell reporter returns when `command` is `None`.
- The webhook reporter returns unless a `url` resolves, and logs
  `webhook: webhook.url is configured but resolved to an empty secret; not reporting`
  when the configured source is empty.

These messages name the setting and omit the environment variable name. When a `fromFile`
source cannot be read, the message reads `<key>.fromFile could not be read: <error>`
in place of the empty-secret text.

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
[why the quorum gate limits double-runs](Clustering-and-Leader-Election#why-the-quorum-gate-limits-double-runs)
and, for the lease backends, [failure modes](Clustering-and-Leader-Election#failure-modes).

### Duplicate `nodeName` conflict (`conflict: true` with `conflict_names`)

**Symptom.** `GET /cluster` shows `"conflict": true` with the offending name in
`"conflict_names"`, one or more peers show status `conflict`, a banner appears in
the dashboard cluster panel, and an `ERROR` log line reports the duplicate.
`Leader` jobs stand down.

**Cause.** Two processes are running with the **same `nodeName`** (distinguished
by their random per-process instance ids). Because each would elect itself as the
lowest name in its own view, both could lead and cause a silent double-run. The daemon
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

**Cause.** A reachable peer is advertising a different `distribution` or
`elect_leader` setting than this node. Because those are cluster-wide coordination
settings (not part of the job-set ID, so they do not surface as drift), a mismatch
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
an unconditional `SIGKILL` to the group when the job's own process has exited or
`killTimeout` seconds (default `30`) have passed, whichever comes first. The daemon
sends the `SIGKILL` even when the main process has already exited, so background
helpers the command left behind go down with it. A run lasts until its captured
output closes, so the timeout also ends a run whose own process has exited while a
process that it started holds a captured stream open. That run's exit code is `-100`
in place of the process's own.

**Windows note.** The same two-step runs with Windows primitives: the graceful
step is a trappable `CTRL_BREAK_EVENT` to the job's process group
(`signal.SIGBREAK` in Python), and the forced step is a `taskkill /F /T` of
the job's live process tree when the job's own process has exited or
`killTimeout` seconds have passed. A daemon with no
console cannot deliver the break, and a service has no console. There the
graceful step becomes the tree kill immediately, and `killTimeout` adds
nothing. See
[running on Windows](Running-on-Windows).

**Resource limits note.** For a run with [resource limits](Resource-Limits)
in place, the forced step goes through the run's container. On Linux it
writes the run's `cgroup.kill` and then sends the group `SIGKILL`. On
Windows, `TerminateJobObject` on the run's Job Object replaces the tree
kill. The container kill also ends descendants that left the process group
or the process tree.

**Fix.** Raise `executionTimeout`, or give the job's own process more time to shut
down with `killTimeout` (have it handle `SIGTERM` on POSIX / `SIGBREAK` on Windows
to use that grace). The grace period lasts while the job's own process runs, which
is the shell for a `command` given as a string. On POSIX, a program that needs the
whole `killTimeout` must be the job's own process: pass `command` as a list, or end
the shell command with `exec`. See
[cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout)
on [concurrency and timeouts](Concurrency-and-Timeouts).

### The Windows service will not start

Read the bootstrap log first: by default
`<config directory>\logs\cronstable-service.log`. A service has no stderr,
so that file is where a startup failure is recorded. The service refuses an
untrusted `-c` path before that log opens, and records the reason in the
Application event log as event ID 1020. It refuses a path that does not exist,
a path that Everyone, Users, Authenticated Users, or another any-user group can
add files to, a path owned by an account other than SYSTEM, Administrators, or
TrustedInstaller (unless an OWNER RIGHTS entry holds that owner to read), and a
junction or symbolic link. The check ignores a write grant to a single named
account. See [where a service logs](Windows-Service#where-a-service-logs).

`cronstable service status` decodes the last failure without opening
anything:

| It says | Fix |
| --- | --- |
| `the configuration did not parse` | Run `cronstable -v -c <path>` interactively against the same path. |
| `the configuration can be written by an account other than SYSTEM and Administrators` | Run the `icacls` commands from event ID 1020, or see [who may write the config directory](Running-on-Windows#who-may-write-the-config-directory). |
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
published one-file `.exe` unpacks itself and
runs the program in a **child** process. The process the Service Control
Manager starts and watches therefore never registers with the service
dispatcher, and the start fails on the Service Control Manager's timeout.

Use one of these:

- Download `cronstable-windows-<arch>.zip` (a one-directory build) and run
  `service install` from its extracted `cronstable.exe`.
- Install the [MSI](Windows-MSI), which registers the service itself.
- Install with winget, which installs the MSI.
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

## Resource limits

### Caps are not enforced

**Symptom.** A job sets [`resourceLimits`](Resource-Limits), and its runs go
ahead without caps. The log shows a `WARNING` like `resourceLimits is set
(job nightly-etl), but cronstable cannot enforce it: <reason>. <hint>. Those
jobs run without caps.` (or `cannot enforce the memory cap: ...` when only
some caps are missing). The web dashboard marks each run with a
**caps not enforced** chip and the terminal dashboard with `caps off`. The
run record's `limits.enforced` is `false`, and
[`GET /node`](HTTP-API#get-node) reports the same `reason` and `hint` under
`limits`.

**Cause.** The host cannot give the job's runs a cgroup or a Job Object with
those caps. The daemon logs the warning once per change of the host's
capability or of the set of capped jobs, at startup and on a reload, and not
once per run. The reason names the cause:

| Reason begins with | Cause | Fix |
| --- | --- | --- |
| `no cgroup v2 hierarchy is mounted at /sys/fs/cgroup` | The Linux host boots with cgroup v1 or a hybrid layout. | Boot with the unified hierarchy (`systemd.unified_cgroup_hierarchy=1`). |
| `cannot find this process's cgroup under /sys/fs/cgroup` | `/proc/self/cgroup` cannot be read, or the cgroup path it gives for the daemon is not a directory under `/sys/fs/cgroup`. The cgroupfs mounted there does not match the daemon's cgroup namespace, as with a custom cgroup mount in a container. | Mount the cgroup v2 filesystem at `/sys/fs/cgroup` from cronstable's own cgroup namespace. In a container, keep the runtime's own cgroup mount (see [resource limits in a container](Production-Deployment#resource-limits-in-a-container)). |
| `cgroup <path> is not delegated to cronstable (<file> is not writable)` | cronstable's cgroup belongs to another user: a unit without `Delegate=`, a daemon started from a login session's shell (its `session-<n>.scope` belongs to root), or the OpenRC service. When `<path>` is `/sys/fs/cgroup`, cronstable runs in a container whose cgroupfs is read-only, as in the published image. | Run cronstable under the packaged systemd unit, which sets `Delegate=memory pids`, or add `Delegate=` to your own unit. From a shell, start it in a delegated scope: `systemd-run --user --scope -p Delegate=yes cronstable -c <path>`. For a container, see [resource limits in a container](Production-Deployment#resource-limits-in-a-container). |
| `cgroup <path> is not delegated to cronstable (it has no trusted.delegate or user.delegate attribute)` | cronstable runs as root in a cgroup with no delegation mark: systemd did not delegate it, or delegated it without the mark (systemd before 244, or a user manager's `systemd-run --user` scope). Root owns every cgroup that the system manager creates, so for a root daemon the mark is the only sign of delegation. | Add `Delegate=` to the system unit. Only the system manager sets the mark: `trusted.delegate` from systemd 244, and `user.delegate` from systemd 251. On older systemd, run the daemon as a non-root `User=` with `Delegate=`. |
| `cronstable runs in the root cgroup` | No service manager placed the daemon in a cgroup of its own. | Run it under a systemd unit with `Delegate=`, or in a delegated scope. |
| `cgroup <path> offers none of the cpu, memory, and pids controllers`, or `does not offer the <name> controller` | The parent cgroup did not delegate those controllers. With some missing, the other caps still apply, and the warning names the missing ones. | `Delegate=memory pids`, as in the packaged unit, delegates memory and pids, and `Delegate=yes` delegates every controller. A `systemd-run --user` scope gets the controllers that `user@.service` has: systemd 252 and later delegate `cpu`, `memory`, and `pids` to it, 238 to 251 leave out `cpu`, and 236 and 237 leave out `memory`. |
| `the cpu controller is not delegated to cronstable` | cronstable's cgroup has no cpu controller: its unit delegates only memory and pids, as the packaged unit does, or it is a user scope on systemd 238 to 251. Only `cpu` caps go unenforced. | Delegate the cpu controller with `Delegate=yes`. For the packaged unit, run `systemctl edit cronstable`, add `Delegate=yes` under `[Service]`, and restart the service. That makes cronstable a CPU group of its own among the system's services (see [CPU caps and priority](Resource-Limits#cpu-caps-and-priority)). |
| `could not enable the cpu controller in <path>: <error>` | The kernel refused to turn on the cpu controller, for example with real-time group scheduling (`CONFIG_RT_GROUP_SCHED`). Only `cpu` caps go unenforced. | Fix the error it names. The daemon tries again at the next configuration apply, and the start of a run with a `cpu` cap tries again at most every 30 seconds. |
| `could not set up cgroup <path>: process <pid> in it could not be moved (it may be exiting)` | A process in cronstable's cgroup stayed there through about 2 seconds of retries, usually one that was exiting. | None, usually. The start of a capped run 30 seconds or more later sets up the cgroup again, as does the next configuration apply. |
| `could not set up cgroup <path>: <error>` | Creating the `daemon/` and `runs/` child cgroups, moving the processes into `daemon/`, or enabling the controllers failed. | Fix the error it names. The daemon tries again at the next configuration apply, and the start of a capped run tries again at most every 30 seconds. |
| `this platform has neither cgroup v2 nor Job Objects` | macOS, FreeBSD, OpenBSD, NetBSD, or illumos. Each configuration load also logs a warning that names the capped jobs. | None: caps apply on Linux and Windows only. |
| `could not load cronstable._cgroups: <error>` (or `cronstable._jobobjects` on Windows) | The module that applies caps on this platform failed to import, so no cap applies. The daemon also logs a `WARNING`, `could not load cronstable._cgroups`, with the traceback. | Reinstall cronstable. If the error persists, report it with the traceback. |
| `Windows refused the CPU rate cap: ...` with `Dynamic Fair Share Scheduling` | A Remote Desktop Services host blocks CPU rate caps. Only `cpu` goes unenforced. | Drop the `cpu` cap on that host, or turn off Dynamic Fair Share Scheduling there. |
| `the Job Object watcher stopped: <reason>`, or `Windows could not open the completion port that reports memory-cap hits` | The thread that turns memory-cap hits into kills stopped, or could not start. A watcher that stopped also logged one `ERROR` (`Runs that start from now on get no memory cap until cronstable restarts.`). CPU and process caps still apply. | Restart cronstable. |

A run whose cap fails on its own, although the host offers it, logs
`resource limits could not be applied to a run of <name>: <reason> (each
distinct failure is logged once)`. On Linux such a reason reads like `could
not set memory.max: <error>` or `could not move the job into its cgroup`.
When the caps apply, the daemon logs `resource limits are enforced with
cgroup v2` (or `with Windows Job Objects`) at `INFO` instead.

A non-root daemon treats any cgroup that its user owns as delegated,
including the scope of a desktop terminal, so it logs no warning there and
moves the terminal's processes into its `daemon/` cgroup. Start it in a scope
of its own: `systemd-run --user --scope -p Delegate=yes cronstable -c <path>`.

**Fix.** Use the row for the reason. [Linux setup](Resource-Limits#linux-setup)
lists what cronstable needs from a Linux host. A job that must never run
uncapped sets `resourceLimits.required: true`, which fails its runs instead
(see the next entries).

### A run failed with "memory limit exceeded"

**Symptom.** A run fails with the fail reason `memory limit exceeded
(512 MiB)` and exit code `-9` on Linux or `1` on Windows. The web dashboard
shows a red **memory cap** chip on the run, the terminal dashboard shows
`memory cap`, and `cronstable_job_memory_limit_exceeded_total` counts it.

**Cause.** The run's processes together reached the job's
`resourceLimits.memory`, and the whole run was killed. On Linux the kernel's
OOM kill takes every process in the run's cgroup (`memory.oom.group`), and,
where swap accounting is on, swap does not stretch the cap. On Windows the
daemon ends the run's Job Object. The run fails whatever `failsWhen` says,
and a `verify` check is skipped. The cap counts what the kernel charges to
the run, which is cgroup memory on Linux and commit charge on Windows. The
**Resources** chart plots sampled RSS, so a run can hit the cap while its
chart reads lower. The run record's `limits.peak_memory_bytes` holds the
kernel's figure.

When the fail reason reads `verification failed: memory limit exceeded
(512 MiB)`, the cap killed the run's verify check, and the run itself stayed
within its cap. The check's own record is `verification.limits`, and the
dashboards show a **check memory cap** chip or marker. The run's
**memory cap** chip and the counter count only kills of the run itself (see
[verify checks and DAG tasks](Resource-Limits#verify-checks-and-dag-tasks)).

An OOM kill that the run's own cap did not cause, such as one from a
container's memory limit or from host-wide memory pressure, is not a
memory-cap hit. That run fails on its signal exit code.

**Fix.** Raise `resourceLimits.memory`, or make the job use less memory.

### A job with `resourceLimits.required` fails at start

**Symptom.** Every run of the job fails at once with `resource limits could
not be enforced: <reason> (resourceLimits.required is set)`. When a cap
cannot apply before the spawn, no process starts: the run gets exit code
`127`, and `cronstable_job_start_failures_total` counts it.

**Cause.** `required: true` refuses a run when any configured cap cannot
apply. The reason is the one in
[caps are not enforced](#caps-are-not-enforced). A run is also refused when
its process could not be put in its cgroup or Job Object. On Linux that
process never starts the command, so the run is a start failure with exit
code `127`. On Windows cronstable ends the process, so the run reports exit
code `1` and does not count as a start failure. The same goes for a Linux
process that joins its cgroup but leaves it before cronstable confirms the
join, with exit code `-9`.

**Fix.** Fix the cause that the reason names, or remove `required` so the
runs go ahead uncapped.

### A `processes` cap breaks a multi-threaded program on Linux

**Symptom.** On Linux, a job with `resourceLimits.processes` fails with
errors such as `Resource temporarily unavailable` from `fork` or
`pthread_create`, `can't start new thread` from Python, or `unable to create
native thread` from Java, although it starts few processes. The same cap
works on Windows.

**Cause.** Linux enforces `processes` with the `pids` controller, which
counts threads as well as processes. A thread pool or a JVM can need dozens
of them. The run record's `limits.process_limit_hits` counts the refused
forks and thread starts.

**Fix.** Raise `processes` to cover the threads, or set it to `max`.

## Reference: exit codes used internally

| Code   | Meaning                                                                    |
| ------ | -------------------------------------------------------------------------- |
| `127`  | Command could not be launched (executable or `workingDirectory` not found) |
| `-100` | Job cancelled because it exceeded `executionTimeout`                       |

A missing `workingDirectory` gets the same `127` as a missing executable, and
the path it tried shows up only inside the `kwargs=` repr of the spawn log
line. See [workingDirectory](Commands-and-Environment#workingdirectory). A
run that `resourceLimits.required` refuses before it starts also gets `127`,
with the fail reason `resource limits could not be enforced: ...`.

A run killed at its [memory cap](Resource-Limits) reports the kill's code:
`-9` on Linux (the SIGKILL of the cgroup's OOM kill) and `1` on Windows
(`TerminateJobObject`). Its fail reason, `memory limit exceeded (<cap>)`,
tells it apart from any other exit with that code.

Two Windows codes worth recognizing in run history:

- `1` is what `taskkill /F` leaves behind, and what `TerminateJobObject`
  leaves for a capped run's forced stop or memory-cap kill. A run reaped by
  a forced stop reports it, indistinguishable from a job's own `exit 1`.
- `3221225786` (`0xC000013A`, `STATUS_CONTROL_C_EXIT`) means the process was ended
  by a console control event, typically a job that received the graceful break and
  did not trap it.

These appear in logs and in report template `exit_code`. See
[architecture and internals](Architecture-and-Internals) for the scheduler and
job-lifecycle details, and [logging configuration](Logging-Configuration) for raising
the log level (`-l DEBUG`) when diagnosing scheduling decisions.
