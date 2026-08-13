# Commands and Environment

This page documents how a job's command is invoked (shell vs. direct exec), how
its environment is constructed from `environment`, `env_file`, and inherited
defaults, and how privilege switching with `user`/`group` works. For schedule
syntax see [Schedules and Timezones](Schedules-and-Timezones); for how defaults
merge into jobs see [Includes, Defaults, and Multi-File Config](Includes-and-Defaults).

## Options

These options are members of each job (and may also be set in a `defaults` block).
Types and defaults are taken from the strictyaml schema and `DEFAULT_CONFIG`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `command` | `Str` or `Seq(Str)` | required | The program to run. A string is run through a shell (on Windows, with the default empty `shell`, through the native command processor `cmd.exe` via `%ComSpec%`); a list is executed directly with no shell on every platform. |
| `shell` | `Str` | `/bin/sh` (POSIX) / empty (Windows) | Shell used when `command` is a string. The default is platform-specific: `/bin/sh` on POSIX, empty on Windows (an empty default routes a string command through the native command processor `%ComSpec%` / `cmd.exe`). To use PowerShell or another interpreter, set `shell:` explicitly, or pass `command` as a list (which bypasses the shell on every platform). See [Running on Windows](Running-on-Windows). |
| `environment` | `Seq(Map({key, value}))` | `[]` | Environment variables (each an item with `key` and `value`, both `Str`) added to the subprocess environment. |
| `env_file` | `Str` | `None` | Path to a `KEY=VALUE` file whose variables are merged into `environment`. |
| `workingDirectory` | `Str` or null | `None` | Directory the subprocess starts in. With it unset, the job inherits cronstable's own working directory. See [workingDirectory](#workingdirectory) below. |
| `priority` | `Enum(idle, below-normal, normal, above-normal, high)` | `normal` | Scheduling priority of the subprocess. Descendants inherit a lowered level; a raised level applies to the subprocess itself on Windows. cronstable never applies the default level: the job keeps cronstable's own nice on POSIX, and cronstable's own class on Windows only when cronstable is at idle or below-normal, otherwise NORMAL. See [priority](#priority) below. |
| `user` | `Str` or `Int` | unset | User (login name or numeric uid) to run the subprocess as. POSIX-only; a job setting it raises a configuration error on Windows (see [Running on Windows](Running-on-Windows)). |
| `group` | `Str` or `Int` | unset | Group (group name or numeric gid) to run the subprocess as. POSIX-only; a job setting it raises a configuration error on Windows (see [Running on Windows](Running-on-Windows)). |

`command` is required on every job. `shell` has a platform-specific
schema/`DEFAULT_CONFIG` default: `/bin/sh` on POSIX and an empty string on
Windows (`DEFAULT_SHELL` in `cronstable/platform.py`), which makes a string command
run via `cmd.exe`. `environment` defaults to an empty list. `env_file`,
`user`, and `group` are optional (`Opt(...)` in the schema) and unset by default;
`environment` and `env_file` are also inheritable via `defaults`, but `user` and
`group` are job-only fields in the schema (they appear in `_job_defaults_common`,
so they are technically accepted in `defaults`, but resolution and the
root-required check happen per job).

## command and shell

`command` may be either a string or a list of strings, and the form determines
how the process is launched (`RunningJob.start` in `cronstable/job.py`):

- **String**: run through a shell.
  - If `shell` is set, cronstable launches `asyncio.create_subprocess_exec` with
    argv `[shell, "-c", command]` (PowerShell reads `-c` as an abbreviation of
    `-Command`). With the default `shell` on POSIX, that is
    `["/bin/sh", "-c", command]`.
  - Windows naming `cmd`/`cmd.exe` is the exception (`shell_spawn` in
    `cronstable/job.py`): cmd.exe wants `/c` rather than `-c`, and it parses
    its command line by its own rules instead of the `CommandLineToArgvW`
    rules that argv rendering is built to be undone by, so a command with
    embedded double quotes would reach it with the escaping still in place.
    It goes through `asyncio.create_subprocess_shell` instead, which hands the
    command string to `%ComSpec% /c` untouched. An absolute `shell:` path is
    passed as the subprocess `executable` so it still wins over `%ComSpec%`.
  - If `shell` is falsy, cronstable also uses `asyncio.create_subprocess_shell`
    with the bare command string. On POSIX the default `/bin/sh` makes the
    `exec`-with-`-c` path the one that runs; on Windows the default `shell` is
    empty (`DEFAULT_SHELL` in `cronstable/platform.py`), so the
    `create_subprocess_shell` path is the default: the command is handed to the
    native command processor `cmd.exe` via `%ComSpec%`, which is the same path
    an explicit `shell: cmd` takes. See
    [Running on Windows](Running-on-Windows).
- **List**: executed directly with `asyncio.create_subprocess_exec`, with no
  shell involved. The argv is taken verbatim from the list; no word splitting,
  globbing, quoting, or `$VAR` expansion is performed.

In all cases the argv is passed through `platform.encode_argv` before the
subprocess is created: on POSIX each element is encoded to UTF-8 bytes (so the
child's argv is independent of the locale); on Windows the strings are passed
through unchanged (`CreateProcessW` works from `str` and rejects `bytes`).

Whatever the command form, the subprocess is spawned with
`platform.new_process_group_kwargs()` applied: on POSIX the job starts in a
fresh session (`start_new_session`), placing it and every descendant in their
own process group, so a cancellation (an `executionTimeout` expiry,
`concurrencyPolicy: Replace`, or an API cancel) can take the whole tree down
as a unit; on Windows the job is created with `CREATE_NEW_PROCESS_GROUP`,
which shields it from the daemon console's Ctrl-C and makes it a trappable
`CTRL_BREAK_EVENT` target ahead of the forced tree kill. Those same Windows
creation flags carry the job's [`priority`](#priority). See
[Cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout).

```yaml
jobs:
  # string form: run via /bin/bash -c "..."
  - name: via-shell
    command: echo "$HOME" && date
    shell: /bin/bash
    schedule: "*/5 * * * *"

  # list form: executed directly, no shell, no variable expansion
  - name: direct-exec
    command:
      - echo
      - foobar
    schedule: "*/5 * * * *"
```

### Launch failures

If the process cannot be launched at all (for example, a list-form `command`
whose executable does not exist, raising `FileNotFoundError`, or a
`SubprocessError`/`UnicodeEncodeError`), the launch error is logged,
`start_failed` is set, and the run is treated as a normal job failure with exit
code `127` rather than crashing the scheduler. See
[Failure Detection and Retries](Failure-Detection-and-Retries).

## environment

`environment` is a list of `{key, value}` maps; both `key` and `value` are
strings in the schema. When `environment` is non-empty, the subprocess
environment is built from the **full current process environment**
(`dict(os.environ)`), the PyInstaller fixup is applied (see below), and then each
configured variable is set/overwritten by key:

```python
env = dict(os.environ)
fixup_pyinstaller_env(env)
for envvar in self.config.environment:
    env[envvar["key"]] = envvar["value"]
```

If `environment` is empty (the default) **and** there is no `env_file`, no `env`
is passed to the subprocess, so it inherits cronstable's environment unchanged.

```yaml
jobs:
  - name: with-env
    command: printenv PATH
    schedule: "*/5 * * * *"
    environment:
      - key: PATH
        value: /bin:/usr/bin
```

### HOSTNAME injection

When the `cronstable.job` module is imported, if `HOSTNAME` is not already present
in `os.environ`, it is set to `socket.gethostname()`:

```python
if "HOSTNAME" not in os.environ:
    os.environ["HOSTNAME"] = gethostname()
```

Because this mutates the process environment, `HOSTNAME` is therefore present in
the inherited environment of every job. Note that the reporting templates'
`environment` variable is the constructed subprocess `env` dict (`self.env`),
which is only populated when the job has a non-empty `environment` or an
`env_file`; for a job with neither, `environment` is `None` in templates and
`{{ environment.HOSTNAME }}` renders empty. See
[Reporting (Mail, Sentry, Shell, Webhook)](Reporting).

## env_file

`env_file` names a file of `KEY=VALUE` lines. Parsing is done by
`parse_environment_file` in `cronstable/config.py`:

- The file is opened as **UTF-8**.
- Each line is stripped of surrounding spaces and a trailing newline.
- Lines beginning with `#` and blank lines are **ignored**.
- A line without an `=` raises `ConfigError` (`"Invalid line in env_file: ..."`).
- Each remaining line is split on the **first** `=` into key and value; both key
  and value are then space-stripped. There is no quote handling and no `#`
  inline-comment handling beyond whole-line comments.
- `parse_environment_file` itself raises a bare `OSError` if the file cannot be
  opened; the caller `_merge_env_file` wraps that as
  `ConfigError("Could not load env_file: ...")`.

### environment overrides env_file

When `env_file` is set, it is merged at config-parse time (`_merge_env_file`):
the file is parsed into a dict, then the job's own `environment` entries are
applied on top, so **`environment` overrides `env_file` per key**. The merged
result replaces `self.environment` as a list of `{key, value}` items, which is
then applied to the subprocess as described in [environment](#environment)
above. (This means that when `env_file` is set, an `env` is always passed to the
subprocess even if `environment` was originally empty.)

```yaml
jobs:
  - name: with-env-file
    command: printenv
    schedule: "*/5 * * * *"
    env_file: /etc/cronstable/job.env
    environment:
      - key: LOG_LEVEL
        value: debug   # overrides LOG_LEVEL from job.env, if present
```

Example `env_file` contents:

```
# comment lines and blank lines are ignored

PATH=/bin:/usr/bin
LOG_LEVEL=info
```

## defaults.environment merge semantics

`environment` set in a `defaults` block merges into each job by key, not by list
concatenation. In `mergedicts` (`cronstable/config.py`), the `environment` list is
special-cased: the default's entries and the job's entries are folded into a
single key-to-value mapping, with the job's value winning on conflict, then
re-expanded to a `{key, value}` list. The result has **no duplicate keys**: a
job variable overrides the same-named default rather than appearing twice.

```yaml
defaults:
  environment:
    - key: PATH
      value: /bin:/usr/bin
    - key: LANG
      value: C
jobs:
  - name: job-a
    command: printenv
    schedule: "*/5 * * * *"
    environment:
      - key: PATH
        value: /usr/local/bin:/bin   # overrides the default PATH
        # LANG=C is inherited from defaults
```

The precedence chain for a variable is therefore: inherited
process environment (including injected `HOSTNAME`) < `env_file` < merged
`environment` (defaults then job, job winning). See
[Includes, Defaults, and Multi-File Config](Includes-and-Defaults) for how
`defaults` and includes are merged overall.

## workingDirectory

`workingDirectory` is the directory the job's process starts in. It is the
`cwd` of the spawn, and the equivalent of the "Start in (optional)" box on a
Task Scheduler action or of systemd's `WorkingDirectory=`.

```yaml
jobs:
  - name: nightly-report
    command: python report.py
    schedule: "0 2 * * *"
    workingDirectory: /srv/reports
```

With the key unset, the job inherits cronstable's own working directory. Like
the other launch fields it can be set in a `defaults:` block and on a DAG
task; under a `defaults:` block that sets it, a bare `workingDirectory:` on
one job opts that job back out to inheriting.

At load, cronstable expands `~` and makes the result absolute:

- `~/jobs` means the home directory, not a directory literally named `~`
  under wherever cronstable was started. On a job that also sets `user`, the
  expansion resolves against cronstable's own user, because it happens once
  at load while the demotion happens per run.
- cronstable expands `${VAR}` from its own environment like any other config
  scalar, unlike `command` and `shell`, which it leaves verbatim for the
  runtime shell to expand. See
  [Environment Variable Interpolation](Environment-Variable-Interpolation).
- cronstable resolves a relative value against its own working directory at
  load rather than at each fire, so the directory a job runs in is settled
  once and the logs show the resolved form. Nothing requires the value to be
  absolute: on Windows, `ntpath.isabs` answers what counts as an absolute
  path differently across the Python versions cronstable supports, and which
  directory a job runs in must not depend on the interpreter that scheduled
  it.

cronstable does not check at load whether the directory exists. Config load
runs on every hot reload, and under `--validate-config` on machines that are
not the target host, so one job naming a share that is not mounted yet must
not fail the whole load. The OS checks at spawn instead, and a directory that
is not there is an ordinary [launch failure](#launch-failures): the run
records exit `127` and the logged spawn line carries the `cwd` that was
attempted. On Windows that log line is the only place the directory appears,
since the error the OS returns there (`WinError 267`, "The directory name is
invalid") carries no filename of its own.

`workingDirectory` is deliberately not part of the
[job-set ID](Job-Set-ID), so replicas that run the same jobs from paths their
own hosts spell differently still agree on what they are running.

Two behaviors to know before relying on it:

- The child changes directory **before** `preexec_fn` runs, so on a job that
  also sets `user`/`group` the change uses cronstable's privileges, not the
  target user's. A demoted child can therefore end up sitting in a directory
  it cannot itself read.
- A list-form `command` that names its program by a **relative path**, one
  containing a separator such as `./import.sh`, resolves that path against
  `workingDirectory` on POSIX and fails on Windows, where `CreateProcessW`
  searches the calling process's directory instead. A **bare** name carrying
  no separator is looked up on `PATH` on both platforms and never comes from
  the working directory, because CPython builds the `PATH` candidate list in
  the parent, before the child changes directory. Name the program by full
  path, or set `shell:` and let the shell resolve it. See
  [Running on Windows](Running-on-Windows#workingdirectory-does-not-change-program-lookup).

## priority

`priority` is how the job is scheduled against everything else on the box. It
takes one of five levels, lowest first:

```yaml
jobs:
  - name: reindex
    command: reindex.sh
    schedule: "0 3 * * *"
    priority: idle
```

| Level | Windows priority class | POSIX nice |
| --- | --- | --- |
| `idle` | `IDLE_PRIORITY_CLASS` | 19 |
| `below-normal` | `BELOW_NORMAL_PRIORITY_CLASS` | 10 |
| `normal` (default) | no class set; see below | inherited, no renice |
| `above-normal` | `ABOVE_NORMAL_PRIORITY_CLASS` | -5 |
| `high` | `HIGH_PRIORITY_CLASS` | -10 |

`normal` is the one level cronstable never applies. On POSIX that means the
job keeps cronstable's own nice, which on a daemon started at nice 10 is nice
10, not nice 0. On Windows it means the job gets cronstable's own class when
cronstable runs at idle or below normal, and NORMAL when cronstable runs at
normal or above, because that is what `CreateProcess` gives a child with no
class flag. Either way cronstable never promotes a job that did not ask to be
promoted. Like the other launch fields it can be set in a `defaults:` block
and on a DAG task.

The other four levels are **absolute**, not offsets: `idle` means nice 19
whatever the daemon sits at.

The two platforms apply it at different moments:

- **Windows** sets the priority class at `CreateProcess` time, which is the
  one race-free place to set it, on the same creation flags that give the job
  its own process group. `normal` emits no class flag rather than
  `NORMAL_PRIORITY_CLASS`, because a child only defaults to NORMAL when its
  creator is not itself idle or below-normal; emitting the flag would
  silently *promote* the jobs of a daemon that was launched below normal,
  which is what Task Scheduler does by default.
- **POSIX** has no such spawn-time knob, so cronstable renices the job's
  process *group* (`setpriority` with `PRIO_PGRP`) as the first thing after a
  successful spawn. It renices the group rather than the process, so a helper
  the shell forked in the microseconds before the call is reniced too. This
  does not happen in a `preexec_fn`: that hook runs between fork and exec,
  where only async-signal-safe calls are sound, and it would put a fork-time
  hook on every spawn including the jobs that have no privilege to drop.

How far the level reaches also differs, and the difference comes from the
same `CreateProcess` rule. Descendants inherit a lowered level (`idle`,
`below-normal`) on both platforms. A raised level (`above-normal`, `high`)
applies to the job's own process on Windows and not to the tree it spawns,
because an unflagged child of an above-normal or high parent gets NORMAL.
That shows up in the commonest Windows job shape: a `shell: cmd` job or a
`.cmd` file at `priority: high` runs cmd.exe at HIGH, and every program
cmd.exe launches runs at NORMAL. POSIX has no such asymmetry, because
`setpriority(PRIO_PGRP)` covers the whole group and anything forked after it
inherits the nice value. `high` is still worth asking for where the job's own
process is the work, but on Windows it will not raise a tree of helpers.

Lowering a priority is always allowed. **Raising** one on POSIX (any level
whose nice sits below cronstable's own; from the usual nice 0 that means
`above-normal` and `high`) needs `CAP_SYS_NICE` or `RLIMIT_NICE` headroom,
and cronstable cannot know at load whether the kernel will grant it. What it
does instead:

- at config load, if the level is a raise from cronstable's own nice and
  cronstable is not root, it logs one `WARNING` naming the job, so the
  warning lands on the deployment that introduces the ask;
- at run time, a refusal from the kernel does **not** fail the run. The job
  runs at the priority it inherited and cronstable logs the refusal at
  `DEBUG`. A minutely job on an unprivileged host would otherwise emit some
  1,440 warnings a day about a condition that will not change until the
  deployment does.

Windows grants all five classes to an unprivileged account, so none of that
applies there.

cronstable deliberately does not offer `realtime`. It outranks the threads
that service disk, keyboard and mouse, so one runaway job at REALTIME can put
the host out of reach of the operator who has to stop it.
`priority: realtime` is a load error listing the accepted levels, not a
silent downgrade.

When a job sets it, the level (never the nice number or priority class it
resolves to) is part of the [job-set ID](Job-Set-ID), so replicas that
disagree about how a job is scheduled show as drift. A set level also appears
on the [`GET /jobs`](HTTP-API#get-jobs) payload. See
[Running on Windows](Running-on-Windows#process-priority) for how the levels
compare with Task Scheduler's own `-Priority` numbers.

## user and group (privilege switching)

`user` and `group` request that the subprocess run under a different identity.
Resolution happens in `JobConfig._resolve_user_group` (`cronstable/config.py`):

> **Windows:** this whole feature is POSIX-only. Windows has no setuid/setgid
> model, so a job with `user` or `group` set raises the configuration error
> `Job <name>: changing user/group is not supported on Windows`
> (`config.py` `_resolve_user_group`). The Root requirement, Resolution rules,
> and Demotion ordering below all apply to POSIX only. See
> [Running on Windows](Running-on-Windows).

### Resolution rules

- **`user` as a name (`Str`)**: looked up with `getpwnam`. Sets `uid` from
  `pw_uid`, `gid` from `pw_gid` (the user's primary group), and the resolved
  login name (`pw_name`). A missing user raises `ConfigError("User not found: ...")`.
- **`user` as a number (`Int`)**: `uid` is set to the number directly. cronstable
  additionally looks the uid up with `getpwuid` to derive the user's **primary
  gid** and **login name**; if `group` was not given, the derived primary gid is
  used (so a numeric `user` without `group` does not silently keep cronstable's
  gid 0). If the uid is not in the passwd database, no login name or derived gid
  is available (and that is not an error here).
- **`group` as a name (`Str`)**: looked up with `getgrnam`; `gid` set from
  `gr_gid`. A missing group raises `ConfigError("Group not found: ...")`.
- **`group` as a number (`Int`)**: `gid` is set to the number directly.
- If only `user` is given, the group defaults to the main group of that user.
  An explicit `group` overrides any gid derived from `user`.

The resolved login name (`username`) matters for supplementary-group handling in
`_demote` (below); it is `None` when the user is unknown.

### Root requirement

If, after resolution, either `uid` or `gid` is set and the cronstable process is not
running as root (`os.geteuid() != 0`), config parsing fails with:

```
Job <name> wants to change user or group, but cronstable is not running as superuser
```

On POSIX, any use of `user` **or** `group` therefore requires cronstable to run as
root. cronstable needs no special privileges otherwise; `user`/`group` switching is
the only feature that requires root. (On Windows `user`/`group` are rejected
outright with a configuration error, so this root requirement is a POSIX-only
statement; see the Windows note above.)

```yaml
jobs:
  - name: as-www-data
    command: id
    schedule:
      minute: "*"
    captureStderr: true
    user: www-data        # group defaults to www-data's primary group
```

### Demotion ordering (_demote)

When `uid` or `gid` is set, `start` passes `preexec_fn=self._demote`, which runs
in the **child process** after fork while still privileged. The order is
deliberate and is required for safety:

1. **Supplementary groups first.** If both a login name and a gid are known,
   `os.initgroups(username, gid)` gives the child exactly the target user's
   supplementary groups. Otherwise `os.setgroups([])` drops all supplementary
   groups. A failure raises `RuntimeError("setgroups/initgroups: ...")`.
2. **Primary gid next.** If `gid` is set, `os.setgid(gid)`. A failure raises
   `RuntimeError("setgid: ...")`.
3. **uid last.** If `uid` is set, `os.setuid(uid)`. A failure raises
   `RuntimeError("setuid: ...")`.

Supplementary groups and the gid must be changed **before** `setuid`, because
once the process drops root via `setuid` it can no longer call
`setgroups`/`setgid`. Performing them in the other order would leave the child
holding root's supplementary group memberships (the classic
"forgot `setgroups()` before `setuid()`" privilege-escalation bug).

## PyInstaller environment fixup

`fixup_pyinstaller_env` is applied to the subprocess environment (only when an
`env` is being constructed, i.e. when `environment`/`env_file` produced
variables). It only does anything when running as a frozen PyInstaller binary
(`getattr(sys, "frozen", False)`):

```python
for env_var in "LD_LIBRARY_PATH", "LIBPATH":
    env[env_var] = env.get(f"{env_var}_ORIG", "")
```

PyInstaller's bootloader overwrites `LD_LIBRARY_PATH` and `LIBPATH` so the
bundled binary can find its own libraries, saving the caller's original values in
`LD_LIBRARY_PATH_ORIG`/`LIBPATH_ORIG`. This fixup restores those originals (or
empties the variable if there was no `_ORIG`) for the subprocess, so a child
process does not inherit the frozen interpreter's library paths. See
[Production and Container Deployment](Production-Deployment) for the frozen-binary
build. (Because the fixup is only applied when an `env` is constructed, jobs with
no `environment` and no `env_file` inherit the process environment as-is,
including any PyInstaller-clobbered values.)
