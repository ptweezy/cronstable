# Command-Line Reference

This page documents the `cronstable` command and every argument it accepts, the
`cronstable state` administration subcommands, the job-facing state commands a
running job uses (`state get|set|delete|keys`, `cursor`, `lock`, `artifact`,
`idempotent`, `secret`, `xcom`), the `mcp` and `tui` client subcommands, the
runtime model (foreground execution, signal handling, exit codes), and common
invocations. Behavior is taken from `cronstable/__main__.py`,
`cronstable/state_admin.py`, `cronstable/jobcli.py`, `cronstable/mcpcli.py`, and
`cronstable/tui.py`.

## Synopsis

```
cronstable [-c FILE-OR-DIR] [-l LOG_LEVEL] [-v] [--job-set-id] [--version]
cronstable state ACTION [options] [-c FILE-OR-DIR]
cronstable state get|set|delete|keys ...  [--scope NAME | --global]
cronstable cursor|lock|artifact|idempotent|secret ...  [--scope NAME | --global]
cronstable xcom push|pull|list ...
cronstable mcp [--url URL] [--token TOKEN | --token-env VAR] [--check]
cronstable tui [--url URL] [--token TOKEN | --token-env VAR] [options]
```

Without a subcommand, `cronstable` is the scheduler daemon described below. With
the `state` subcommand it is an offline administration tool for the durable
state store; see [The `state` subcommand](#the-state-subcommand). The
`state get|set|delete|keys`, `cursor`, `lock`, `artifact`, `idempotent`,
`secret`, and `xcom` commands are a different surface: a *running job* uses
them to reach the daemon's store through its loopback endpoint; see
[Job-facing state commands](#job-facing-state-commands). The `mcp` and `tui`
subcommands are clients of a running daemon's web listener: the MCP stdio
bridge and the terminal dashboard; see [The `mcp` subcommand](#the-mcp-subcommand)
and [The `tui` subcommand](#the-tui-subcommand).

`cronstable` runs as a single foreground process. It does not daemonize, does not
fork, and does not write a PID file. Diagnostics go to stdout/stderr via the
standard library `logging` module. To run it as a service, place it under a
process supervisor: systemd or a container runtime on POSIX (see
[Production and Container Deployment](Production-Deployment)), Task Scheduler
or a service wrapper on Windows (see
[Running unattended](Running-on-Windows#running-unattended) on
[Running on Windows](Running-on-Windows)). A supervised daemon is stopped
gracefully with `SIGTERM` on POSIX or the authenticated `POST /shutdown`
route on any platform (see the [HTTP Control API](HTTP-API)).

## Arguments

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-c`, `--config` | path (file or directory) | platform default[^cfgdefault] | Configuration file, or a directory containing configuration files. When a directory, every `*.yml`/`*.yaml` file, plus every classic crontab (`*.crontab`, `*.cron`, or a file named `crontab`), is loaded (entries whose name starts with `_` or `.` are skipped). See [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) and [Classic Crontabs](Classic-Crontabs). |
| `-l`, `--log-level` | string | `INFO` | Root log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`, upper or lower case (aliases the `logging` module knows, such as `WARN` and `FATAL`, also resolve). An unknown name exits `2` as a usage error. |
| `-v`, `--validate-config` | flag | off | Parse and validate the configuration, then exit. Exits `0` if valid, `1` on a configuration error. Does not start the scheduler or web server. |
| `--job-set-id` | flag | off | Parse the configuration, print the [job-set id](Job-Set-ID) (an order-independent hash of every job's effective configuration) to stdout, and exit `0`. Identical across instances running the same set of jobs. Exits `1` on a configuration error. |
| `--version` | flag | off | Print the cronstable version to stdout and exit `0`. |
| `-h`, `--help` | flag | — | Print usage (argparse builtin) and exit `0`. |

The other command-line surfaces are the `state` subcommand,
[documented below](#the-state-subcommand), which administers the durable state
store; the [job-facing state commands](#job-facing-state-commands) a running
job uses; and the client subcommands [`mcp`](#the-mcp-subcommand) and
[`tui`](#the-tui-subcommand), which talk to a running daemon's web listener.
Job schedules, commands, environment, reporting, and the web API are
configured entirely in YAML, not on the command line; see the
[Configuration Reference](Configuration-Reference).

[^cfgdefault]: The default config path is platform-specific (`DEFAULT_CONFIG_PATH`
    in `cronstable/platform.py`): `/etc/cronstable.d` on POSIX; on Windows the
    machine-wide `%ProgramData%\cronstable` when that directory holds
    configuration, otherwise the per-user `%APPDATA%\cronstable`
    (e.g. `C:\Users\<you>\AppData\Roaming\cronstable`, falling back to the user
    profile `~` if `APPDATA` is unset). See
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
`cronstable/platform.py`; see the footnote above for the per-platform values).
The special case is triggered by the condition
`args.config == DEFAULT_CONFIG_PATH and not os.path.exists(args.config)`: if the
config argument equals the platform default and that path does not exist,
cronstable prints the following to stderr (with the resolved default filled in),
prints the usage help, and exits `1`:

```
cronstable error: configuration file not found at the default location (<default path>). Run `cronstable init` to create a starter configuration there, or point -c/--config at an existing file or directory.
```

Because the check compares the argument value (not whether `-c` was supplied),
it fires both when `-c` is omitted and when you pass `-c` set to the platform
default explicitly. For any other non-existent path passed with `-c`, you
instead get the generic configuration-error path (a logged
`Configuration error: ...` and exit `1`).

#### The `init` subcommand

`cronstable init [DIRECTORY]` writes a commented starter configuration
(`cronstable.yaml`, one working job plus the most commonly wanted next steps
left commented out) into `DIRECTORY`, creating the directory when needed.
With no argument it targets a root-level `-c`/`--config` if you gave one, so
`cronstable -c D:\jobs init` writes to `D:\jobs`, the same flag the not-found
error above points you at. Otherwise it targets the platform default config
path, which makes `cronstable init` followed by `cronstable` a working first
run. Naming both a `DIRECTORY` and a different `-c` writes to `DIRECTORY` and
says so on stderr. It never touches an existing setup: a target that is a
file, already holds `*.yaml`/`*.yml`/crontab config files, already has a
`cronstable.yaml`, or cannot be read or written is refused with the reason
and exit `1`; success prints the written path and the command to start the
scheduler, and exits `0`.

### `-l` / `--log-level`

The log level is applied with `logging.basicConfig` before the configuration is
loaded, so it governs cronstable's own startup and runtime logging. The value is
upper-cased and resolved against the standard level names: `DEBUG`, `INFO`,
`WARNING`, `ERROR`, and `CRITICAL` work in upper or lower case, as do the
aliases the `logging` module defines (`WARN`, `FATAL`). A misspelled level
exits `2` with a usage error naming the valid choices.

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
[`GET /job-set-id`](HTTP-API) endpoint and compared between cluster peers
([Clustering and Leader Election](Clustering-and-Leader-Election)); the full
treatment -- what the hash covers, what it excludes, and why -- lives on the
[job-set id](Job-Set-ID) page.

Because the config is fully parsed first, a configuration error exits `1`, and
the [default-path special case](#default-path-special-case) applies just as it
does for `--validate-config`.

### `--version`

Prints the version string (e.g. `1.0.13`) to stdout and exits `0`. This check
runs before the config is touched, so `--version` succeeds even when no
configuration exists.

## The `state` subcommand

```
cronstable state ACTION [options] [-c FILE-OR-DIR]
```

`cronstable state` administers the durable state store defined by the
configuration's `state:` section (the daemon-side store on disk or on a shared
mount -- not the [Web Dashboard](Web-Dashboard)'s browser-side IndexedDB run
ledger, which is a separate, purely client-side feature). Every action works
offline, straight from the configuration, with no running daemon required.
Actions that read or copy *out of* the store (`backup`, `check`,
`migrate-schema`) and the `gc` pass stay safe against a *running* daemon,
because records are immutable and copies/reads never lock; a backup taken
mid-write is a point-in-time-ish snapshot rather than an exact one.
Restoring or migrating *into* a store a daemon is actively using is **not**
safe (see [`state restore`](#state-restore) / [`state migrate`](#state-migrate)).

Each action accepts its own `-c`/`--config`, with the same meaning and default
as the daemon flag, so both positions work: `cronstable -c /etc/cronstable.d state gc`
and `cronstable state gc -c /etc/cronstable.d` are equivalent. (`-c` between `state`
and the action name is not accepted.) If the resolved configuration has no
`state:` section, or cannot be read, the action prints
`cronstable state error: <detail>` to stderr and exits `1`; the
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
cronstable state backup -o FILE.tar.gz [-c FILE-OR-DIR]
```

Writes a gzipped tar of the store's namespace to `-o`/`--output` (required).
The archive carries the full store: the immutable records (`records/`), the
mutable documents (`docs/` -- KV entries, cursors, idempotency claims, and
dag_run documents), the content-addressed artifact payloads (`blobs/`), and
the lease files (`leases/` -- a lease file is the only home of its fence
counter, so dropping it would re-issue fence values). Deliberately *not*
carried: `tmp/` (transient write debris) and `quarantine/` (poison records;
forensics stay with the source store). The archive is created owner-only
(mode `0600`): it flattens captured job output, KV values, and artifact
payloads into a single file. Against a live daemon, a file that disappears
mid-backup (a prune, a lease rewrite) is skipped, by design. Exits `1` when
the store directory does not exist (`nothing to back up`).

### `state restore`

```
cronstable state restore FILE.tar.gz [--force] [-c FILE-OR-DIR]
```

Extracts a backup archive into the configured store. It refuses to restore
into a store that already contains data and exits `1`; pass `--force` to
merge the archive into it. Restoring is **not** safe while a daemon uses the
store -- stop the daemon first. Archive members are sanitised: only plain
files that extract strictly inside the store are honored (no absolute paths,
no `..` escapes, no symlinks or devices), and each file lands with mode
`0600` via a temp sibling plus atomic replace, so a concurrent reader never
sees a torn record. When merging into a populated store, `.lock` side-files
are skipped (a live daemon may hold an OS lock on that very inode), and a
lease file replaces the current one only when its archived fence counter is
provably not older -- a fence-max merge; regressing a fence would re-issue
fence values already handed out. The kept-lease count is reported.

### `state migrate`

```
cronstable state migrate --dest PATH [--dest-deployment-id ID] [--force]
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
cronstable state gc [--dry-run] [-c FILE-OR-DIR]
```

Runs one manual garbage-collection pass with the same rules as the daemon's
automatic periodic pass: it removes the streams of jobs (and artifact
scopes) that no recent manifest references and whose newest record is older
than `state.gcGraceSeconds`, plus counter and manifest streams of
unmanifested hosts, provably dead per-run DAG advance leases (the only
lease class ever deleted), orphaned lock side-files idle past the grace,
crashed write-temp files, and quarantined records older than the grace,
then sweeps artifact payload blobs no surviving record references. It prints what was removed (or, with
`--dry-run`, what would be), the kept-stream count, and the reclaimed
orphan-blob count -- or the reason the blob sweep stood down (an
unenumerable artifact stream or an unreadable record keeps every blob). Run
documents of removed DAGs are left to the running daemon's own pass, which
alone knows what it owns. Like the automatic pass, it defers (exit `0`,
with a message) until
the store's manifest history spans one full grace window -- a store that
cannot yet prove absence deletes nothing. When GC is disabled
(`gcGraceSeconds` <= 0) the command reports that there is nothing to collect
and exits `1`.

### `state check`

```
cronstable state check [-c FILE-OR-DIR]
```

Verifies the store is usable -- starting the backend probes writability --
and prints an inventory: the store path, backend, namespace, topology,
shared-locking mode, the number of streams and records (broken down by stream
prefix, e.g. `runs`, `logs`, `retries`), and the quarantined-record count. A
store that cannot be started or probed exits `1`.

### `state migrate-schema`

```
cronstable state migrate-schema [--dry-run] [-c FILE-OR-DIR]
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
with `gcGraceSeconds` disabled). Error and refusal messages go to stderr;
configuration and I/O errors print as `cronstable state error: <detail>`.
Success summaries and inventories stay on stdout, so piped output is clean.
`cronstable state` with no action prints a pointer to `cronstable state --help` and
exits `2`, the same code argparse itself uses for usage errors (an unknown
option, or a missing required one such as `backup` without `-o`).

## Job-facing state commands

Alongside the offline `state` admin actions above, a *running job's* command
line reaches the daemon's durable store through the job-facing commands
`state get|set|delete|keys`, `cursor`, `lock`, `artifact`, `idempotent`,
`secret`, and `xcom`: thin clients of the
[loopback state endpoint](HTTP-API#job-facing-state-endpoints-loopback) that
read the injected `CRONSTABLE_STATE_URL` / `CRONSTABLE_STATE_TOKEN` and speak
HTTP over the standard library (no aiohttp, no event loop), so they start
instantly and need no config file (behavior is from `cronstable/jobcli.py`).
The `state` job actions share the `cronstable state` name with the
[admin actions](#the-state-subcommand); the action name selects the handler,
and the job commands take no `-c`. Outside a job the injected environment is
absent and every command exits `1` with `not running inside a cronstable job:
CRONSTABLE_STATE_URL is not set`. This section keeps to each command's
synopsis, flags, and exit codes; the semantics live in
[Durable State](Durable-State#job-facing-state), which also covers the
enabling `state.jobApi` config.

### Scope and exit codes

Every KV, cursor, artifact, lock, and idempotency command acts in a *scope*, a
namespace defaulting to the calling job's own name, and takes two mutually
exclusive override flags: `--scope NAME` (act in the named scope) and
`--global` (act in the shared `global` scope, for deliberate cross-job
coordination). `secret` and `xcom` take neither flag: a run's secrets are
always its own, and the daemon injects the DAG run's XCom scope.

The commands share one exit-code convention, made for shell branching:

| Code | Meaning |
| --- | --- |
| `0` | Success (or, for `idempotent`, the claim was fresh). |
| `1` | An error (a transport or store failure). |
| `2` | Usage error (argparse; e.g. a command invoked with no action). |
| `3` | A `lock acquire` / `lock run` did not get the lock. |
| `4` | The looked-up key, cursor, artifact, secret, or XCom key does not exist (`state delete` of an absent key too). |
| `5` | The `idempotent` key was already claimed (a duplicate). |

### `state get|set|delete|keys` (durable key/value)

```
cronstable state get KEY [--scope NAME | --global]
cronstable state set KEY VALUE [--json] [--scope NAME | --global]
cronstable state delete KEY [--scope NAME | --global]
cronstable state keys [--scope NAME | --global]
```

`--json` stores `VALUE` as a parsed JSON document rather than a string.

### `cursor get|advance` (ETL watermark)

```
cronstable cursor get NAME [--scope NAME | --global]
cronstable cursor advance NAME VALUE [--force] [--scope NAME | --global]
```

`advance` refuses to move the monotonic cursor backwards; `--force` sets it
unconditionally (a deliberate rewind).

### `lock acquire|release|run` (distributed mutex/semaphore)

```
cronstable lock acquire NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global]
cronstable lock run NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global] -- COMMAND...
cronstable lock release TOKEN
```

`acquire` prints a hold token, or exits `3` when no permit is free; `--wait`
first blocks up to `--timeout` seconds (default `0`, so `--wait` alone makes
one pass and gives up). `release` takes the token, and no scope flags. `run`
holds the lock while `COMMAND...` (everything after `--`) runs and exits with
the command's own exit code, or `3` if the lock was not acquired. `--permits`
accepts `1` (the default, a mutex) through `1024`; a count outside that range
exits `1`. `--ttl` overrides the lease TTL (default
`state.jobApi.lockTtlSeconds`). The `--` separator belongs to `lock run`
alone: cronstable splits the trailing command off at the first `--` before
argument parsing, so a bare `--` in any other invocation is rejected with
`` `--` is only valid before a `lock run` command `` and exits `2`.

### `artifact put|get|list` (named blob store)

```
cronstable artifact put NAME [FILE] [--scope NAME | --global]
cronstable artifact get NAME [-o FILE] [--scope NAME | --global]
cronstable artifact list [--scope NAME | --global]
```

`put` reads `FILE`, or stdin when `FILE` is omitted or `-`; `get` writes to
`-o FILE`, or stdout when omitted or `-`.

### `idempotent` (run-once guard)

```
cronstable idempotent KEY [--ttl S] [--release] [--scope NAME | --global]
```

`--ttl S` expires the claim after `S` seconds (`0`, the default, is a
permanent claim); `--release` drops the claim instead of making it.

### `secret get|list` (run-scoped secrets)

```
cronstable secret get NAME
cronstable secret list
```

No flags. `get` exits `4` if no secret of that name is staged for this run.

### `xcom push|pull|list` (DAG cross-task data)

```
cronstable xcom push --key KEY [FILE]
cronstable xcom pull --task TASK --key KEY [--map-index I] [-o FILE]
cronstable xcom list
```

Pass data between the tasks of a
[DAG run](Orchestration-and-DAGs#xcom-passing-data-between-tasks); outside a
task the DAG scheduler launched, the commands print a clean error and exit
non-zero. `push` reads `FILE` or stdin; `pull` writes to `-o FILE` or stdout,
`--map-index I` selecting one instance of a
[mapped](Orchestration-and-DAGs#fan-out-dynamic-mapping) upstream.

## The `mcp` subcommand

```
cronstable mcp [--url URL] [--token TOKEN | --token-env VAR]
               [--protocol-version REV] [--timeout SECONDS] [--check]
```

`cronstable mcp` runs the MCP stdio bridge: a thin standard-library client that
connects a desktop MCP client (stdio transport) to a running daemon's `/mcp`
endpoint (`--url`, default `http://127.0.0.1:8080`). Like the job-facing
commands it needs no config file and never imports the daemon graph. The
bridge, every flag (including `--protocol-version` and `--timeout`), and
client setup are documented on the [MCP](MCP) page.

## The `tui` subcommand

```
cronstable tui [--url URL] [--token TOKEN | --token-env VAR] [--theme NAME]
               [--tv] [--job NAME] [--boot | --no-boot] [--ascii]
               [--poll SECONDS]
```

`cronstable tui` opens the terminal dashboard -- the
[Web Dashboard](Web-Dashboard)'s keyboard-driven TUI sibling -- against a
running daemon's web listener (`--url`, default `http://127.0.0.1:8080`). It
requires an interactive terminal. Every option, key, and panel is documented
on the [Terminal Dashboard](Terminal-Dashboard) page.

## Runtime model

When started normally (no `--version`, no `--validate-config`, no
`--job-set-id`, no subcommand, with a usable config), cronstable:

1. Configures logging from `-l`.
2. Resolves and parses the configuration (`-c`), exiting `1` on error.
3. Installs shutdown handlers. On POSIX these are bound to `SIGINT` and
   `SIGTERM` on the event loop; on Windows cronstable instead uses `signal.signal`
   for `SIGINT` (Ctrl-C) and `SIGBREAK` (Ctrl-Break) plus a heartbeat timer,
   because the Proactor loop has no `add_signal_handler`, and a native
   console-control handler that turns console close and OS shutdown into the
   same graceful drain (bounded by the OS's own grace period). Logoff is
   deliberately not one of them; see
   [Running on Windows](Running-on-Windows#graceful-shutdown).
4. Runs the asyncio scheduler loop in the foreground until shutdown.

The scheduler re-reads the configuration on every loop iteration, so editing the
config files takes effect without a restart. A configuration that becomes
invalid after a successful start is logged and ignored; the previously loaded
jobs keep running. See [Architecture and Internals](Architecture-and-Internals).

### Signal handling and graceful shutdown

`SIGINT` (Ctrl-C) and `SIGTERM` are both bound to the same graceful-shutdown
path: they set an internal stop event. The scheduler loop notices the event,
stops scheduling new job runs, logs `Shutting down (after currently running
jobs finish)...`, and then cronstable:

1. Cancels all pending retry timers.
2. Waits for currently running jobs to finish.
3. Stops the HTTP control server if it is running (logged as
   `Stopping http server`).

cronstable does not force-kill its own running jobs on shutdown. Individual jobs
have their own kill behavior (`killTimeout`) when they are stopped; see
[Concurrency and Timeouts](Concurrency-and-Timeouts). Sending a second signal
does not change the shutdown sequence. A deployment that cannot deliver a
signal (a supervised or console-less daemon) gets the same graceful drain
from the authenticated `POST /shutdown` route; see the
[HTTP Control API](HTTP-API). If you need an immediate, ungraceful stop, kill
the process with `SIGKILL` on POSIX or Task Manager / `taskkill /F` on
Windows; either skips the drain, and on Windows also leaves any spawned job
trees running.

On Windows, press Ctrl-C (`SIGINT`) to trigger the same graceful shutdown: it
finishes the currently-running jobs first, exactly as `SIGTERM` does on
POSIX, and jobs run in their own console process groups so the keystroke
never reaches them directly. Ctrl-Break (`SIGBREAK`) drains the daemon the
same way, but a console-generated break also reaches the jobs sharing the
console, so prefer Ctrl-C. Closing the console window and OS shutdown trigger
the drain too, on the few seconds of grace Windows grants. Logging off does
not: an unattended daemon receives that event for every user on the machine
and would stop on the first RDP sign-out. See
[Running on Windows](Running-on-Windows).

### Exit codes

| Code | Condition |
| --- | --- |
| `0` | `--version` printed; `--validate-config` succeeded; `--job-set-id` printed; `--help`; a `state` action succeeded; or normal shutdown after a signal. |
| `1` | Configuration error (parse/schema/validation failure or unreadable config); the default `-c` path (platform-specific; see the footnote under [Arguments](#arguments)) does not exist and no `-c` was given; an `init` refusal; or a `state` action failed (see [`state` exit codes](#state-exit-codes)). |
| `2` | Usage error (argparse builtin): unknown option or missing required option (e.g. `state backup` without `-o`); an invalid `--log-level` value; `cronstable state` invoked with no action; or a `--` separator in any invocation other than `lock run` (see [`lock`](#lock-acquirereleaserun-distributed-mutexsemaphore)). |

## Examples

Run with a single config file in the foreground:

```shell
cronstable -c /tmp/my-crontab.yaml
```

Run against a config directory (the conventional container entrypoint):

```shell
cronstable -c /etc/cronstable.d
```

On Windows the config path uses Windows paths, and the machine-wide default
directory is `%ProgramData%\cronstable` (falling back to `%APPDATA%\cronstable`
until it holds a config file):

```bat
cronstable.exe -c C:\ProgramData\cronstable
```

See [Running on Windows](Running-on-Windows) for Windows-specific CLI behavior
(default config path, default shell, shutdown semantics, running unattended).

Validate a config and exit (suitable for CI or a container healthcheck/preflight):

```shell
cronstable -v -c /etc/cronstable.d
```

Increase log verbosity:

```shell
cronstable -l DEBUG -c /tmp/my-crontab.yaml
```

Print the version:

```shell
cronstable --version
```

Back up the durable state store defined by a config (the `-c` may equally go
before `state`):

```shell
cronstable state backup -o /backups/cronstable-state.tar.gz -c /etc/cronstable.d
```

For installation and packaging details (pip, PyInstaller binary, Docker), see
[Installation](Installation). For deploying cronstable as a long-running service,
see [Production and Container Deployment](Production-Deployment). For
Windows-specific CLI behavior (default config path, default shell, Ctrl-C /
Ctrl-Break shutdown), see [Running on Windows](Running-on-Windows).
