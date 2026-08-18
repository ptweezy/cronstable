# Command-line reference

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
cronstable service install|remove|start|stop|reload|status|run [options]
cronstable import-taskscheduler PATH... [-o FILE] [--timezone NAME]
```

Without a subcommand, `cronstable` is the scheduler daemon described later.
With the `state` subcommand it is an offline administration tool for the
durable state store; see the [`state` subcommand](#the-state-subcommand).

The `state get|set|delete|keys`, `cursor`, `lock`, `artifact`, `idempotent`,
`secret`, and `xcom` commands are a different surface: a *running job* uses
them to reach the daemon's store through its loopback endpoint. See
[job-facing state commands](#job-facing-state-commands).

The `mcp` and `tui` subcommands are clients of a running daemon's web listener:
the MCP stdio bridge and the terminal dashboard. See the
[`mcp` subcommand](#the-mcp-subcommand) and the
[`tui` subcommand](#the-tui-subcommand).

`cronstable` runs as a single foreground process. It does not daemonize, does not
fork, and does not write a PID file. Diagnostics go to stdout/stderr through
the standard library `logging` module.

To run it as a service, place it under a process supervisor: systemd or a
container runtime on POSIX (see
[production and container deployment](Production-Deployment)), Task Scheduler or
a service wrapper on Windows (see
[running unattended](Running-on-Windows#running-unattended) in
[running on Windows](Running-on-Windows)). To stop a supervised daemon
gracefully, send `SIGTERM` on POSIX or call the authenticated `POST /shutdown`
route on any platform (see the [HTTP control API](HTTP-API)).

## Arguments

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-c`, `--config` | path (file or directory) | platform default[^cfgdefault] | Configuration file, or a directory containing configuration files. From a directory, cronstable loads every `*.yml`/`*.yaml` file and every classic crontab (`*.crontab`, `*.cron`, or a file named `crontab`), skipping entries whose name starts with `_` or `.`. See [includes, defaults, and multi-file config](Includes-and-Defaults) and [classic crontabs](Classic-Crontabs). |
| `-l`, `--log-level` | string | `INFO` | Root log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`, in upper or lower case. `logging` module aliases such as `WARN` and `FATAL` also resolve. An unknown name is a usage error (exit `2`). |
| `-v`, `--validate-config` | flag | off | Parse and validate the configuration, then exit: `0` if valid, `1` on a configuration error. Does not start the scheduler or web server. |
| `--job-set-id` | flag | off | Parse the configuration, print the [job-set id](Job-Set-ID) (an order-independent hash of every job's effective configuration) to stdout, and exit `0`. Identical across instances running the same set of jobs. Exits `1` on a configuration error. |
| `--version` | flag | off | Print the cronstable version to stdout and exit `0`. |
| `-h`, `--help` | flag | — | Print usage (argparse builtin) and exit `0`. |

The other command-line surfaces are the
[`state` subcommand](#the-state-subcommand) described later, which administers
the durable state store; the
[job-facing state commands](#job-facing-state-commands) a running job uses; and
the client subcommands [`mcp`](#the-mcp-subcommand) and
[`tui`](#the-tui-subcommand), which talk to a running daemon's web listener.
Job schedules, commands, environment, reporting, and the web API are
configured entirely in YAML, not on the command line; see the
[configuration reference](Configuration-Reference).

[^cfgdefault]: The default configuration path is platform-specific
    (`DEFAULT_CONFIG_PATH` in `cronstable/platform.py`): `/etc/cronstable.d` on
    POSIX; on Windows the machine-wide `%ProgramData%\cronstable` when that
    directory holds configuration, otherwise the per-user `%APPDATA%\cronstable`
    (for example, `C:\Users\<you>\AppData\Roaming\cronstable`, falling back to
    the user profile `~` if `APPDATA` is unset). See
    [running on Windows](Running-on-Windows).

### `-c` / `--config`

The argument may be a single file or a directory:

- **File:** parsed directly, YAML by default. It is parsed as a classic
  crontab when the name says so (`*.crontab`, `*.cron`, or a file named
  `crontab`, such as a `crontab -l > crontab` export), or, for a file with a
  neutral name such as `-c /var/spool/cron/crontabs/root`, when the content
  unmistakably is one. See [classic crontabs](Classic-Crontabs); the six-field
  *system* crontab format of `/etc/crontab` is not supported. An I/O error
  (for example, the file does not exist) is reported as a configuration error
  and exits `1`.
- **Directory:** each non-hidden `*.yml`/`*.yaml` or crontab-named entry is
  parsed in name-sorted order. An empty directory (or one whose files are all
  skipped) yields an empty configuration with no jobs rather than an error.

#### Default-path special case

The default is the platform default configuration path (`DEFAULT_CONFIG_PATH`
from `cronstable/platform.py`; see the preceding footnote for the per-platform
values). The condition
`args.config == DEFAULT_CONFIG_PATH and not os.path.exists(args.config)`
triggers the special case. If the configuration argument equals the platform
default and that path does not exist, cronstable prints the following to stderr
(with the resolved default filled in), prints the usage help, and exits `1`:

```
cronstable error: configuration file not found at the default location (<default path>). Run `cronstable init` to create a starter configuration there, or point -c/--config at an existing file or directory.
```

Because the check compares the argument value (not whether `-c` was supplied),
it fires both when `-c` is omitted and when you pass `-c` set to the platform
default explicitly. For any other non-existent path passed with `-c`, you
instead get the generic configuration-error path (a logged
`Configuration error: ...` and exit `1`).

#### The `init` subcommand

`cronstable init [DIRECTORY]` writes a commented starter configuration,
`cronstable.yaml`, into `DIRECTORY`, creating the directory when needed. The
file holds one working job plus the most commonly wanted next steps, left
commented out.

With no argument it targets a root-level `-c`/`--config` if you gave one, so
`cronstable -c D:\jobs init` writes to `D:\jobs`, the same flag the earlier
not-found error points you at. Otherwise it targets the platform default
configuration path, which makes `cronstable init` followed by `cronstable` a
working first run. Naming both a `DIRECTORY` and a different `-c` writes to
`DIRECTORY` and says so on stderr.

It never touches an existing setup. `cronstable init` refuses a target that is
a file, already holds `*.yaml`/`*.yml`/crontab configuration files, already has
a `cronstable.yaml`, or cannot be read or written, giving the reason and exit
`1`. Success prints the written path and the command to start the scheduler,
and exits `0`.

### `-l` / `--log-level`

The log level is applied with `logging.basicConfig` before the configuration is
loaded, so it governs cronstable's own startup and runtime logging. The value is
upper-cased and resolved against the standard level names. `DEBUG`, `INFO`,
`WARNING`, `ERROR`, and `CRITICAL` work in upper or lower case, as do the
aliases the `logging` module defines (`WARN`, `FATAL`). If the level is
misspelled, cronstable exits `2` with a usage error naming the valid choices.

A `logging:` section in the configuration can reconfigure logging after startup
through `logging.config.dictConfig`; see
[logging configuration](Logging-Configuration).

### `-v` / `--validate-config`

Validation constructs the scheduler from the resolved configuration
(`Cron(config)`), which parses and schema-checks every file. On success it logs
`Configuration is valid.` and exits `0`. On any `ConfigError` (schema
violation, unknown time zone, invalid numeric range, missing user/group,
include cycle, multiple `web`/`logging` sections, and other faults) it logs
`Configuration error: <detail>` and exits `1`. The scheduler loop and web
server never start in this mode.

The earlier default-path special case still applies: the check runs before
`Cron(config)` is constructed, so validating while the configuration argument
equals the platform default (`DEFAULT_CONFIG_PATH`) and that path is absent
exits `1` with the not-found message rather than the
`Configuration error: ...` message.

### `--job-set-id`

Constructs the scheduler from the resolved configuration exactly like
`--validate-config`, then prints the job-set id to stdout and exits `0`. The id
is an order-independent hash of every job's effective configuration, identical
across instances running the same set of jobs regardless of file order or how
the jobs are split across files.

The [`GET /job-set-id`](HTTP-API) endpoint serves the same value, and cluster
peers compare it (see
[clustering and leader election](Clustering-and-Leader-Election)). The
[job-set id](Job-Set-ID) page has the full treatment: what the hash covers,
what it excludes, and why.

Because the configuration is fully parsed first, a configuration error exits
`1`, and the [default-path special case](#default-path-special-case) applies
exactly as it does for `--validate-config`.

### `--version`

Prints the version string (for example, `1.0.13`) to stdout and exits `0`. This
check runs before the configuration is touched, so `--version` succeeds even
when no configuration exists.

## The `state` subcommand

```
cronstable state ACTION [options] [-c FILE-OR-DIR]
```

`cronstable state` administers the durable state store defined by the
configuration's `state:` section. That is the daemon-side store on disk or on a
shared mount, not the [web dashboard](Web-Dashboard)'s browser-side IndexedDB
run ledger, a separate, purely client-side feature. Every action works offline,
straight from the configuration, with no running daemon required.

Actions that read or copy *out of* the store (`backup`, `check`,
`migrate-schema`) and the `gc` pass stay safe against a *running* daemon,
because records are immutable and copies/reads never lock. A backup taken
mid-write is an approximate point-in-time snapshot rather than an exact one.
Restoring or migrating *into* a store a daemon is actively using is **not**
safe (see [`state restore`](#state-restore) / [`state migrate`](#state-migrate)).

Each action accepts its own `-c`/`--config`, with the same meaning and default
as the daemon flag, so both positions work: `cronstable -c /etc/cronstable.d state gc`
and `cronstable state gc -c /etc/cronstable.d` are equivalent. (`-c` between `state`
and the action name is not accepted.) If the resolved configuration has no
`state:` section, or cannot be read, the action prints
`cronstable state error: <detail>` to stderr and exits `1`. The
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
mutable documents (`docs/`: KV entries, cursors, idempotency claims, and
dag_run documents), the content-addressed artifact payloads (`blobs/`), and the
lease files (`leases/`). A lease file is the only home of its fence counter, so
dropping it would re-issue fence values.

Deliberately *not* carried: `tmp/` (transient write debris) and `quarantine/`
(poison records; forensics stay with the source store). The archive is created
owner-only (mode `0600`): it flattens captured job output, KV values, and
artifact payloads into a single file.

Against a live daemon, `state backup` skips a file that disappears mid-backup
(a prune, a lease rewrite), by design. Exits `1` when the store directory does
not exist (`nothing to back up`).

### `state restore`

```
cronstable state restore FILE.tar.gz [--force] [-c FILE-OR-DIR]
```

Extracts a backup archive into the configured store. It refuses to restore
into a store that already contains data and exits `1`; pass `--force` to
merge the archive into it. Restoring is **not** safe while a daemon uses the
store. Stop the daemon first.

Archive members are sanitized: only plain files that extract strictly inside
the store are honored (no absolute paths, no `..` escapes, no symlinks or
devices). Each file lands with mode `0600` through a temp sibling plus atomic
replace, so a concurrent reader never sees a torn record.

When merging into a populated store, `.lock` side-files are skipped (a live
daemon may hold an OS lock on that very inode). A lease file replaces the
current one only when its archived fence counter is provably not older (a
fence-max merge). Regressing a fence would re-issue fence values already handed
out. The kept-lease count is reported.

### `state migrate`

```
cronstable state migrate --dest PATH [--dest-deployment-id ID] [--force]
                      [-c FILE-OR-DIR]
```

Copies the store to another path or mount. A local directory and an Amazon
S3 Files / EFS mount share one on-disk layout, so migration in either
direction is a faithful file copy. `--dest` (required) is the destination
`state.path`; `--dest-deployment-id` selects a different namespace at the
destination (default: keep the current one).

Each file lands through a temp sibling plus atomic rename, so a reader of the
*destination* never observes a torn record. That matters when cutting over to a
shared mount that other nodes already watch.

Refused with exit `1`: migrating a store onto (or into) itself, and a
destination namespace that already holds records or leases unless `--force` is
given. Overwriting a live destination's lease files would regress their fence
counters under any daemon already using that store.

After a successful copy, point `state.path` (and `deploymentId`, if you
changed it) at the new location to cut over.

### `state gc`

```
cronstable state gc [--dry-run] [-c FILE-OR-DIR]
```

Runs one manual garbage-collection pass with the same rules as the daemon's
automatic periodic pass, removing:

- The streams of jobs (and artifact scopes) that no recent manifest references
  and whose newest record is older than `state.gcGraceSeconds`.
- Counter and manifest streams of unmanifested hosts.
- Provably dead per-run DAG advance leases, the only lease class ever deleted.
- Orphaned lock side-files idle past the grace.
- Crashed write-temp files.
- Quarantined records older than the grace.

It then sweeps artifact payload blobs no surviving record references.

It prints what was removed (or, with `--dry-run`, what would be), the
kept-stream count, and either the reclaimed orphan-blob count or, if the blob
sweep did not run, the reason: an unenumerable artifact stream or an
unreadable record keeps every blob.

Run documents of removed DAGs are left to the running daemon's own pass, which
alone knows what it owns. Like the automatic pass, it defers (exit `0`, with a
message) until the store's manifest history spans one full grace window: a
store that cannot yet prove absence deletes nothing. When GC is disabled
(`gcGraceSeconds` <= 0), the command reports that there is nothing to collect
and exits `1`.

### `state check`

```
cronstable state check [-c FILE-OR-DIR]
```

Verifies the store is usable (starting the backend probes writability) and
prints an inventory: the store path, backend, namespace, topology,
shared-locking mode, the number of streams and records (broken down by stream
prefix, such as `runs`, `logs`, `retries`), and the quarantined-record count.
If the store cannot be started or probed, the command exits `1`.

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

Alongside the earlier offline `state` admin actions, a *running job's* command
line reaches the daemon's durable store through the job-facing commands
`state get|set|delete|keys`, `cursor`, `lock`, `artifact`, `idempotent`,
`secret`, and `xcom`.

These thin clients of the
[loopback state endpoint](HTTP-API#job-facing-state-endpoints-loopback) read
the injected `CRONSTABLE_STATE_URL` / `CRONSTABLE_STATE_TOKEN` and speak HTTP
over the standard library (no aiohttp, no event loop), so they start instantly
and need no configuration file. Behavior comes from `cronstable/jobcli.py`.

The `state` job actions share the `cronstable state` name with the
[admin actions](#the-state-subcommand); the action name selects the handler,
and the job commands take no `-c`. Outside a job the injected environment is
absent and every command exits `1` with `not running inside a cronstable job:
CRONSTABLE_STATE_URL is not set`.

This section keeps to each command's synopsis, flags, and exit codes. The
semantics live in [durable state](Durable-State#job-facing-state), which also
covers the enabling `state.jobApi` configuration.

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
| `2` | Usage error (argparse; such as a command invoked with no action). |
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

`advance` refuses to move the monotonic cursor backward; `--force` sets it
unconditionally (a deliberate rewind).

### `lock acquire|release|run` (distributed mutex/semaphore)

```
cronstable lock acquire NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global]
cronstable lock run NAME [--permits N] [--wait --timeout S] [--ttl S] [--scope NAME | --global] -- COMMAND...
cronstable lock release TOKEN
```

`acquire` prints a hold token, or exits `3` when no permit is free. `--wait`
first blocks up to `--timeout` seconds (default `0`, so `--wait` alone makes
one pass and gives up). `release` takes the token, and no scope flags. `run`
holds the lock while `COMMAND...` (everything after `--`) runs and exits with
the command's own exit code, or `3` if the lock was not acquired.

`--permits` accepts `1` (the default, a mutex) through `1024`; outside that
range the command exits `1`. `--ttl` overrides the lease TTL (default
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
[DAG run](Orchestration-and-DAGs#xcom-passing-data-between-tasks). Outside a
task the DAG scheduler launched, the commands print a clean error and exit
non-zero. `push` reads `FILE` or stdin; `pull` writes to `-o FILE` or stdout,
with `--map-index I` selecting one instance of a
[mapped](Orchestration-and-DAGs#fan-out-dynamic-mapping) upstream.

## The `mcp` subcommand

```
cronstable mcp [--url URL] [--token TOKEN | --token-env VAR]
               [--protocol-version REV] [--timeout SECONDS] [--check]
```

`cronstable mcp` runs the MCP stdio bridge: a thin standard-library client that
connects a desktop MCP client (stdio transport) to a running daemon's `/mcp`
endpoint (`--url`, default `http://127.0.0.1:8080`). Like the job-facing
commands it needs no configuration file and never imports the daemon graph. The
[MCP](MCP) page documents the bridge, every flag (including
`--protocol-version` and `--timeout`), and client setup.

## The `tui` subcommand

```
cronstable tui [--url URL] [--token TOKEN | --token-env VAR] [--theme NAME]
               [--tv] [--job NAME] [--boot | --no-boot] [--ascii]
               [--poll SECONDS]
```

`cronstable tui` opens the terminal dashboard (the
[web dashboard](Web-Dashboard)'s keyboard-driven TUI sibling) against a running
daemon's web listener (`--url`, default `http://127.0.0.1:8080`). It requires
an interactive terminal. The [terminal dashboard](Terminal-Dashboard) page
documents every option, key, and panel.

## The `import-taskscheduler` subcommand

```
cronstable import-taskscheduler PATH... [-o FILE] [--timezone NAME]
```

Converts Windows Task Scheduler XML exports into cronstable jobs and exits.
`PATH` may be an export file, a directory of `*.xml`, or `-` for standard
input. The configuration goes to stdout or `-o`; a report of everything that
could not be carried across goes to stderr, so the two separate cleanly:

```shell
schtasks /query /XML ONE > tasks.xml
cronstable import-taskscheduler tasks.xml -o jobs.yaml 2> report.txt
cronstable -v -c jobs.yaml
```

Exit `0` when something converted, `1` when the input could not be read or
nothing usable came out, `2` for a usage error. It runs on any platform, not
only Windows.

Review the output before loading it: exporting a task does not unregister it.
Expect a whole-machine export to convert only in part, because most registered
tasks on a stock Windows install are COM-handler or event-driven internals.
[Importing from Task Scheduler](Importing-Task-Scheduler) has the full
mapping table and the reason behind every refusal.

## The `service` subcommand

```
cronstable service install [--name NAME] [-c FILE-OR-DIR]
                           [--start-type auto|delayed|demand]
                           [--log-level LEVEL] [--log-file PATH | --no-log-file]
                           [--console] [--restart-delay SECONDS] [--no-restart]
cronstable service remove|start|stop|reload|status [--name NAME]
                                                   [--timeout SECONDS]
cronstable service run [--name NAME] [-c FILE-OR-DIR] [options]
```

Windows only. On any other platform every action prints one line and exits `2`.
`cronstable service install` registers the scheduler with the Service Control
Manager so it starts at boot and runs whether or not anyone is logged on, and
needs an elevated prompt. `reload` makes the running service reparse its
configuration immediately, the same forced reload `SIGHUP` performs on POSIX.
`run` is the entry point the SCM itself invokes and is not meant to be typed.
When you run it by hand, it says so and exits `2`.

Exit codes for the whole subcommand: `0` success, `1` a Windows failure
(the message names the fix), `2` a refusal or a usage error.

The [Windows service](Windows-Service) page covers the full command set, the
recovery-action behavior, where a service logs, the configuration-path rule,
and the one install shape that cannot host a service.

## Runtime model

When started normally (no `--version`, no `--validate-config`, no
`--job-set-id`, no subcommand, with a usable configuration), cronstable:

1. Configures logging from `-l`.
2. Resolves and parses the configuration (`-c`), exiting `1` on error.
3. Installs shutdown handlers. On POSIX these are bound to `SIGINT` and
   `SIGTERM` on the event loop. On Windows cronstable instead uses
   `signal.signal` for `SIGINT` (Ctrl-C) and `SIGBREAK` (Ctrl-Break) plus a
   heartbeat timer, because the Proactor loop has no `add_signal_handler`. On
   Windows, cronstable also installs a native console-control handler that
   turns console close and OS shutdown into the same graceful drain (bounded
   by the OS's own grace period). Logoff is deliberately not one of them; see
   [graceful shutdown on Windows](Running-on-Windows#graceful-shutdown).
4. Runs the asyncio scheduler loop in the foreground until shutdown.

The scheduler re-reads the configuration on every loop iteration, so editing
the configuration files takes effect without a restart. A configuration that
becomes invalid after a successful start is logged and ignored, and the
previously loaded jobs keep running. See
[architecture and internals](Architecture-and-Internals).

### Signal handling and graceful shutdown

`SIGINT` (Ctrl-C) and `SIGTERM` are both bound to the same graceful-shutdown
path: they set an internal stop event. The scheduler loop notices the event,
stops scheduling new job runs, logs `Shutting down (after currently running
jobs finish)...`, and then cronstable:

1. Cancels all pending retry timers.
2. Waits for currently running jobs to finish.
3. Stops the HTTP control server if it is running (logged as
   `Stopping http server`).

On shutdown, cronstable does not force-kill its own running jobs. Individual
jobs have their own `killTimeout` behavior when they are stopped; see
[concurrency and timeouts](Concurrency-and-Timeouts). Sending a second signal
does not change the shutdown sequence.

A deployment that cannot deliver a signal (a supervised or console-less daemon)
gets the same graceful drain from the authenticated `POST /shutdown` route; see
the [HTTP control API](HTTP-API). If you need an immediate, ungraceful stop,
end the process with `SIGKILL` on POSIX or Task Manager / `taskkill /F` on
Windows; either skips the drain, and on Windows also leaves any spawned job
trees running.

On Windows, press Ctrl-C (`SIGINT`) to trigger the same graceful shutdown: it
finishes the currently running jobs first, exactly as `SIGTERM` does on POSIX,
and jobs run in their own console process groups so the keystroke never reaches
them directly.

Ctrl-Break (`SIGBREAK`) drains the daemon the same way, but a console-generated
break also reaches the jobs sharing the console, so prefer Ctrl-C. Closing the
console window and OS shutdown trigger the drain too, on the few seconds of
grace Windows grants. Logging off does not: an unattended daemon receives that
event for every user on the machine and would stop on the first RDP sign-out.
See [running on Windows](Running-on-Windows).

### Exit codes

| Code | Condition |
| --- | --- |
| `0` | `--version` printed; `--validate-config` succeeded; `--job-set-id` printed; `--help`; a `state` action succeeded; or normal shutdown after a signal. |
| `1` | Configuration error (parse/schema/validation failure or unreadable configuration); the default `-c` path (platform-specific; see the footnote under [arguments](#arguments)) does not exist and no `-c` was given; an `init` refusal; or a `state` action failed (see [`state` exit codes](#state-exit-codes)). |
| `2` | Usage error (argparse builtin): unknown option or missing required option (such as `state backup` without `-o`); an invalid `--log-level` value; `cronstable state` invoked with no action; or a `--` separator in any invocation other than `lock run` (see [`lock`](#lock-acquirereleaserun-distributed-mutexsemaphore)). |

## Examples

Run with a single configuration file in the foreground:

```shell
cronstable -c /tmp/my-crontab.yaml
```

Run against a configuration directory (the conventional container entrypoint):

```shell
cronstable -c /etc/cronstable.d
```

On Windows the configuration path uses Windows paths, and the machine-wide
default directory is `%ProgramData%\cronstable` (falling back to
`%APPDATA%\cronstable` until it holds a configuration file):

```bat
cronstable.exe -c C:\ProgramData\cronstable
```

See [running on Windows](Running-on-Windows) for Windows-specific CLI behavior
(default configuration path, default shell, shutdown semantics, running
unattended).

Validate a configuration and exit (suitable for CI or a container
healthcheck/preflight):

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

Back up the durable state store defined by a configuration (the `-c` may
equally go before `state`):

```shell
cronstable state backup -o /backups/cronstable-state.tar.gz -c /etc/cronstable.d
```

For installation and packaging details (pip, PyInstaller binary, Docker), see
[installation](Installation). For deploying cronstable as a long-running
service, see [production and container deployment](Production-Deployment). For
Windows-specific CLI behavior (default configuration path, default shell,
Ctrl-C / Ctrl-Break shutdown), see [running on Windows](Running-on-Windows).
