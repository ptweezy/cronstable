# Command-Line Reference

This page documents the `yacron2` command and every argument it accepts, the
`yacron2 state` administration subcommands, the runtime model (foreground
execution, signal handling, exit codes), and common invocations. Behavior is
taken from `yacron2/__main__.py` and `yacron2/state_admin.py`.

## Synopsis

```
yacron2 [-c FILE-OR-DIR] [-l LOG_LEVEL] [-v] [--job-set-id] [--version]
yacron2 state ACTION [options] [-c FILE-OR-DIR]
```

Without a subcommand, `yacron2` is the scheduler daemon described below. With
the `state` subcommand it is an offline administration tool for the durable
state store; see [The `state` subcommand](#the-state-subcommand).

`yacron2` runs as a single foreground process. It does not daemonize, does not
fork, and does not write a PID file. Diagnostics go to stdout/stderr via the
standard library `logging` module. To run it as a service, place it under a
process supervisor (systemd, a container runtime, etc.); see
[Production and Container Deployment](Production-Deployment).

## Arguments

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-c`, `--config` | path (file or directory) | platform default[^cfgdefault] | Configuration file, or a directory containing configuration files. When a directory, every `*.yml`/`*.yaml` file, plus every classic crontab (`*.crontab`, `*.cron`, or a file named `crontab`), is loaded (entries whose name starts with `_` or `.` are skipped). See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) and [Classic Crontabs](Classic-Crontabs). |
| `-l`, `--log-level` | string | `INFO` | Root log level. Passed to `logging.basicConfig(level=getattr(logging, LOG_LEVEL))`, so the value must name an attribute of the `logging` module (e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). |
| `-v`, `--validate-config` | flag | off | Parse and validate the configuration, then exit. Exits `0` if valid, `1` on a configuration error. Does not start the scheduler or web server. |
| `--job-set-id` | flag | off | Parse the configuration, print the [job-set id](Clustering-and-Leader-Election#the-job-set-id-foundation) (an order-independent hash of every job's effective configuration) to stdout, and exit `0`. Identical across instances running the same set of jobs. Exits `1` on a configuration error. |
| `--version` | flag | off | Print the yacron2 version to stdout and exit `0`. |
| `-h`, `--help` | flag | — | Print usage (argparse builtin) and exit `0`. |

The only other command-line surface is the `state` subcommand,
[documented below](#the-state-subcommand), which administers the durable state
store. Job schedules, commands, environment, reporting, and the web API are
configured entirely in YAML, not on the command line; see the
[Configuration Reference](Configuration-Reference).

[^cfgdefault]: The default config path is platform-specific (`DEFAULT_CONFIG_PATH`
    in `yacron2/platform.py`): `/etc/yacron2.d` on POSIX, and `%APPDATA%\yacron2`
    (e.g. `C:\Users\<you>\AppData\Roaming\yacron2`, falling back to the user
    profile `~` if `APPDATA` is unset) on Windows. See
    [Running on Windows](Running-on-Windows).

### `-c` / `--config`

The argument may be a single file or a directory:

- **File:** parsed directly. YAML by default; a classic crontab when the name
  says so (`*.crontab`, `*.cron`, or a file named `crontab`, e.g. a
  `crontab -l > crontab` export) or, for a file with a neutral name such as
  `-c /var/spool/cron/crontabs/root`, when the content unmistakably is one
  (see [Classic Crontabs](Classic-Crontabs); the six-field *system* crontab
  format of `/etc/crontab` is not supported). An I/O error (for example, the
  file does not exist) is reported as a configuration error and exits `1`.
- **Directory:** each non-hidden `*.yml`/`*.yaml` or crontab-named entry is
  parsed in name-sorted order. An empty directory (or one whose files are all
  skipped) yields an empty configuration with no jobs rather than an error.

#### Default-path special case

The default is the platform default config path (`DEFAULT_CONFIG_PATH` from
`yacron2/platform.py`): `/etc/yacron2.d` on POSIX, `%APPDATA%\yacron2` on
Windows. The special case is triggered by the condition
`args.config == DEFAULT_CONFIG_PATH and not os.path.exists(args.config)`: if the
config argument equals the platform default and that path does not exist,
yacron2 prints the following to stderr, prints the usage help, and exits `1`:

```
yacron2 error: configuration file not found, please provide one with the --config option
```

Because the check compares the argument value (not whether `-c` was supplied),
it fires both when `-c` is omitted and when you pass `-c` set to the platform
default explicitly (`-c /etc/yacron2.d` on POSIX, `-c %APPDATA%\yacron2` on
Windows). For any other non-existent path passed with `-c`, you instead get the
generic configuration-error path (a logged `Configuration error: ...` and exit
`1`).

### `-l` / `--log-level`

The log level is applied with `logging.basicConfig` before the configuration is
loaded, so it governs yacron2's own startup and runtime logging. The value is
resolved with `getattr(logging, args.log_level)`; an unknown name (e.g. a
lowercase or misspelled level) raises `AttributeError` and the process aborts
with a traceback rather than a clean error. Use a canonical level name such as
`DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

A `logging:` section in the configuration can reconfigure logging after startup
via `logging.config.dictConfig`; see [Logging Configuration](Logging-Configuration).

### `-v` / `--validate-config`

Validation works by constructing the scheduler from the resolved config
(`Cron(config)`), which parses and schema-checks every file. On success it logs
`Configuration is valid.` and exits `0`. On any `ConfigError` (schema violation,
unknown timezone, invalid numeric range, missing user/group, include cycle,
multiple `web`/`logging` sections, etc.) it logs `Configuration error: <detail>`
and exits `1`. The scheduler loop and web server are never started in this mode.

The default-path special case above still applies: it is checked before
`Cron(config)` is constructed, so validating while the config argument equals
the platform default (`DEFAULT_CONFIG_PATH`) and that path is absent exits `1`
with the not-found message rather than the `Configuration error: ...` message.

### `--job-set-id`

Constructs the scheduler from the resolved config exactly like
`--validate-config`, then prints the job-set id to stdout and exits `0`: an
order-independent hash of every job's effective configuration, identical
across instances running the same set of jobs regardless of file order or
how the jobs are split across files. This is the same value served by the
[`GET /job-set-id`](HTTP-API) endpoint and compared between cluster peers; see
[Clustering and Leader Election](Clustering-and-Leader-Election#the-job-set-id-foundation).

Because the config is fully parsed first, a configuration error exits `1`, and
the [default-path special case](#default-path-special-case) applies just as it
does for `--validate-config`.

### `--version`

Prints the version string (e.g. `1.0.13`) to stdout and exits `0`. This check
runs before the config is touched, so `--version` succeeds even when no
configuration exists.

## The `state` subcommand

```
yacron2 state ACTION [options] [-c FILE-OR-DIR]
```

`yacron2 state` administers the durable state store defined by the
configuration's `state:` section (the daemon-side store on disk or on a shared
mount -- not the [Web Dashboard](Web-Dashboard)'s browser-side IndexedDB run
ledger, which is a separate, purely client-side feature). Every action works
offline, straight from the configuration, with no running daemon required; and
every action stays safe against a *running* daemon, because records are
immutable and copies/reads never lock. A backup taken mid-write is a
point-in-time-ish snapshot rather than an exact one.

Each action accepts its own `-c`/`--config`, with the same meaning and default
as the daemon flag, so both positions work: `yacron2 -c /etc/yacron2.d state gc`
and `yacron2 state gc -c /etc/yacron2.d` are equivalent. (`-c` between `state`
and the action name is not accepted.) If the resolved configuration has no
`state:` section, or cannot be read, the action prints
`yacron2 state error: <detail>` to stdout and exits `1`; the
[default-path special case](#default-path-special-case) does not apply here.

| Action | Description |
| --- | --- |
| `backup` | Write a `.tar.gz` backup of the store. |
| `restore` | Restore a backup into the store. |
| `migrate` | Copy the store to another path or mount (local disk <-> S3 Files / EFS). |
| `gc` | Garbage-collect state of unreferenced jobs. |
| `check` | Verify the store is usable and print an inventory. |
| `migrate-schema` | Rewrite records of older known record schemes. |

### `state backup`

```
yacron2 state backup -o FILE.tar.gz [-c FILE-OR-DIR]
```

Writes a gzipped tar of the store's namespace to `-o`/`--output` (required).
The archive carries the immutable records (`records/`) and the lease files
(`leases/`) -- a lease file is the only home of its fence counter, so dropping
it would re-issue fence values. Deliberately *not* carried: `tmp/` (transient
write debris) and `quarantine/` (poison records; forensics stay with the
source store). Against a live daemon, a file that disappears mid-backup (a
prune, a lease rewrite) is skipped, by design. Exits `1` when the store
directory does not exist (`nothing to back up`).

### `state restore`

```
yacron2 state restore FILE.tar.gz [--force] [-c FILE-OR-DIR]
```

Extracts a backup archive into the configured store. It refuses to restore
into a store that already contains records or leases and exits `1`; pass
`--force` to merge the archive into it. Archive members are sanitised: only
plain files that extract strictly inside the store are honored (no absolute
paths, no `..` escapes, no symlinks or devices), and each file lands with
mode `0600`.

### `state migrate`

```
yacron2 state migrate --dest PATH [--dest-deployment-id ID] [--force]
                      [-c FILE-OR-DIR]
```

Copies the store to another path or mount. A local directory and an Amazon
S3 Files / EFS mount share one on-disk layout, so migration in either
direction is a faithful file copy. `--dest` (required) is the destination
`state.path`; `--dest-deployment-id` selects a different namespace at the
destination (default: keep the current one). Each file lands via a temp
sibling plus atomic rename, so a reader of the *destination* never observes a
torn record -- important when cutting over to a shared mount that other nodes
already watch. Refused with exit `1`: migrating a store onto (or into) itself,
and a destination namespace that already holds records or leases unless
`--force` is given -- overwriting a live destination's lease files would
regress their fence counters under any daemon already using that store.
After a successful copy, point `state.path` (and `deploymentId`, if you
changed it) at the new location to cut over.

### `state gc`

```
yacron2 state gc [--dry-run] [-c FILE-OR-DIR]
```

Runs one manual garbage-collection pass with the same rules as the daemon's
automatic periodic pass: it removes the streams of jobs that no recent
manifest references and whose newest record is older than
`state.gcGraceSeconds`, plus counter streams of unmanifested hosts, crashed
write-temp files, and quarantined records older than the grace. It prints
what was removed (or, with `--dry-run`, what would be) and the kept-stream
count. Like the automatic pass, it defers (exit `0`, with a message) until
the store's manifest history spans one full grace window -- a store that
cannot yet prove absence deletes nothing. When GC is disabled
(`gcGraceSeconds` <= 0) the command reports that there is nothing to collect
and exits `1`.

### `state check`

```
yacron2 state check [-c FILE-OR-DIR]
```

Verifies the store is usable -- starting the backend probes writability --
and prints an inventory: the store path, backend, namespace, topology,
shared-locking mode, the number of streams and records (broken down by stream
prefix, e.g. `runs`, `logs`, `retries`), and the quarantined-record count. A
store that cannot be started or probed exits `1`.

### `state migrate-schema`

```
yacron2 state migrate-schema [--dry-run] [-c FILE-OR-DIR]
```

Rewrites records written under *older known* record-scheme versions to the
current one, and reports how many records were converted, already current,
unknown, unreadable, or failed. `v1` is the only scheme so far, so today this
reports and converts nothing; it becomes useful only after a future scheme
bump. Records with unknown versions are left in place for the daemon's usual
quarantine-on-read handling. `--dry-run` counts without rewriting.

### `state` exit codes

Every action exits `0` on success and `1` on any error: a missing or invalid
configuration, no `state:` section, an I/O failure, or a refusal (restoring
into a non-empty store without `--force`, migrating a store onto itself, GC
with `gcGraceSeconds` disabled). Errors print `yacron2 state error: <detail>`.
`yacron2 state` with no action prints a pointer to `yacron2 state --help` and
exits `2`, the same code argparse itself uses for usage errors (an unknown
option, or a missing required one such as `backup` without `-o`).

## Runtime model

When started normally (no `--version`, no `--validate-config`, no
`--job-set-id`, no `state` subcommand, with a usable config), yacron2:

1. Configures logging from `-l`.
2. Resolves and parses the configuration (`-c`), exiting `1` on error.
3. Installs shutdown handlers. On POSIX these are bound to `SIGINT` and
   `SIGTERM` on the event loop; on Windows yacron2 instead uses `signal.signal`
   for `SIGINT` (Ctrl-C) and `SIGBREAK` (Ctrl-Break) plus a heartbeat timer,
   because the Proactor loop has no `add_signal_handler`.
4. Runs the asyncio scheduler loop in the foreground until shutdown.

The scheduler re-reads the configuration on every loop iteration, so editing the
config files takes effect without a restart. A configuration that becomes
invalid after a successful start is logged and ignored; the previously loaded
jobs keep running. See [Architecture and Internals](Architecture-and-Internals).

### Signal handling and graceful shutdown

`SIGINT` (Ctrl-C) and `SIGTERM` are both bound to the same graceful-shutdown
path: they set an internal stop event. The scheduler loop notices the event,
stops scheduling new job runs, logs `Shutting down (after currently running
jobs finish)...`, and then yacron2:

1. Cancels all pending retry timers.
2. Waits for currently running jobs to finish.
3. Stops the HTTP control server if it is running (logged as
   `Stopping http server`).

yacron2 does not force-kill its own running jobs on shutdown. Individual jobs
have their own kill behavior (`killTimeout`) when they are stopped; see
[Concurrency and Timeouts](Concurrency-and-Timeouts). Sending a second signal
does not change the shutdown sequence; if you need an immediate stop, kill the
process with `SIGKILL` (POSIX-only; there is no Windows equivalent, so use Task
Manager or `taskkill /F` there).

On Windows, press Ctrl-C or Ctrl-Break (`SIGINT`/`SIGBREAK`) to trigger the same
graceful shutdown: it finishes the currently-running jobs first, exactly as
`SIGTERM` does on POSIX. The wiring differs only internally: `signal.signal`
plus a heartbeat timer, because the Proactor loop lacks `add_signal_handler`.
See [Running on Windows](Running-on-Windows).

### Exit codes

| Code | Condition |
| --- | --- |
| `0` | `--version` printed; `--validate-config` succeeded; `--job-set-id` printed; `--help`; a `state` action succeeded; or normal shutdown after a signal. |
| `1` | Configuration error (parse/schema/validation failure or unreadable config); the default `-c` path (platform-specific: `/etc/yacron2.d` on POSIX, `%APPDATA%\yacron2` on Windows) does not exist and no `-c` was given; or a `state` action failed (see [`state` exit codes](#state-exit-codes)). |
| `2` | Usage error (argparse builtin): unknown option or missing required option (e.g. `state backup` without `-o`); or `yacron2 state` invoked with no action. |

A traceback (non-zero, not the clean `1` path) results from an invalid
`--log-level` value, since the level is resolved before error handling is in
place.

## Examples

Run with a single config file in the foreground:

```shell
yacron2 -c /tmp/my-crontab.yaml
```

Run against a config directory (the conventional container entrypoint):

```shell
yacron2 -c /etc/yacron2.d
```

On Windows the config path uses Windows paths and the default is
`%APPDATA%\yacron2` rather than `/etc/yacron2.d`:

```bat
yacron2.exe -c %APPDATA%\yacron2
```

See [Running on Windows](Running-on-Windows) for Windows-specific CLI behavior
(default config path, default shell, Ctrl-C / Ctrl-Break shutdown).

Validate a config and exit (suitable for CI or a container healthcheck/preflight):

```shell
yacron2 -v -c /etc/yacron2.d
```

Increase log verbosity:

```shell
yacron2 -l DEBUG -c /tmp/my-crontab.yaml
```

Print the version:

```shell
yacron2 --version
```

Back up the durable state store defined by a config (the `-c` may equally go
before `state`):

```shell
yacron2 state backup -o /backups/yacron2-state.tar.gz -c /etc/yacron2.d
```

For installation and packaging details (pip, PyInstaller binary, Docker), see
[Installation](Installation). For deploying yacron2 as a long-running service,
see [Production and Container Deployment](Production-Deployment). For
Windows-specific CLI behavior (default config path, default shell, Ctrl-C /
Ctrl-Break shutdown), see [Running on Windows](Running-on-Windows).
