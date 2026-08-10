# Running on Windows

cronstable runs natively on Windows, alongside Linux and macOS. This page is the
canonical reference for the behaviors that differ on Windows: how to install
it, where it looks for configuration, how a string `command` is fed to a
shell, how job output is decoded, how to stop the daemon, how to run it
unattended (at boot, surviving logoff), how a job is terminated, the
POSIX-only features that are reported (never silently dropped) on Windows,
and what file-lock coordination over a shared mount can and cannot verify.
Everything not listed here behaves exactly as it does on POSIX, so the rest of
this wiki applies unchanged.

All of the OS-specific behavior is isolated in a single module
(`cronstable/platform.py`); the scheduler, job runner, config loader, and entry
point read the same on every platform.

## Supported platforms and architectures

cronstable supports Windows on two CPU architectures: `amd64` (x64) and `arm64`
(ARM64). You can install it either as a normal Python package or as a
self-contained executable.

| Architecture | pip / pipx | Standalone binary |
| --- | --- | --- |
| `amd64` (x64) | `pip install cronstable` | `cronstable-windows-amd64.exe` |
| `arm64` (ARM64) | `pip install cronstable` | `cronstable-windows-arm64.exe` |

The test suite runs on Windows (both x64 and ARM64) in CI on every commit,
with a small set of POSIX-only tests (per-job user/group switching, privilege
drop, POSIX signal delivery, POSIX file modes) skipped there, each with a
stated reason; every release builds both Windows binaries. See
[Contributing and Releasing](Contributing-and-Releasing) for the build and
release workflow.

## Installation

There are three ways to install cronstable on Windows.

### winget

The Windows Package Manager installs the standalone binary and puts
`cronstable` on your `PATH`:

```shell
winget install ptweezy.cronstable
```

Upgrade later with `winget upgrade ptweezy.cronstable`. The winget package is
a per-user install of the portable executable; for a machine-wide deployment
use the standalone binary below and place it yourself.

### pip / pipx

`pip install cronstable` works on Windows just as it does on POSIX, installing the
`cronstable` console script into your environment. A supported Python (3.10 or
newer) must be present. See [Installation](Installation) for the Python and
dependency requirements that apply on every platform.

```shell
pip install cronstable
cronstable --version
```

### Standalone binary (no Python required)

Every release attaches self-contained executables
(`cronstable-windows-amd64.exe` (x64) and `cronstable-windows-arm64.exe` (ARM64)) on
the [releases page](https://github.com/ptweezy/cronstable/releases). Python is
**not** required on the target system; the interpreter is embedded in the
executable. Download the asset for your architecture, then run it:

```shell
cronstable-windows-amd64.exe --version
```

The binaries are built natively on Windows runners (the ARM64 binary on a
`windows-11-arm` runner). As on every platform, the standalone binary is a
self-extracting executable; for the writable-and-executable temp-directory
detail (which matters only under unusual locked-down filesystems) see
[Installation](Installation).

The Windows executables carry a version resource (Properties > Details shows
the product and version) but are **not Authenticode-signed**, so the first
run of a browser-downloaded .exe trips SmartScreen ("Windows protected your
PC"): choose "More info", then "Run anyway", and verify the download against
the release's `SHA256SUMS` if your policy requires it. Environments that
allowlist by publisher (AppLocker/WDAC publisher rules) cannot admit an
unsigned binary; use a hash or path rule there, or install through pip.

There is no Windows container image; the published Docker image is Linux-only.
See [Installation](Installation) for the Linux image and its supported
architectures.

## Default configuration location

When `-c`/`--config` is omitted, the directory cronstable looks in is
platform-specific:

| Platform | Default `-c` path |
| --- | --- |
| POSIX | `/etc/cronstable.d` |
| Windows, machine-wide | `%ProgramData%\cronstable` (e.g. `C:\ProgramData\cronstable`), whenever that directory holds configuration |
| Windows, per-user | `%APPDATA%\cronstable` (e.g. `C:\Users\<you>\AppData\Roaming\cronstable`), otherwise |

`%ProgramData%\cronstable` is the Windows analog of `/etc/cronstable.d`:
machine-scoped and shared by every account. cronstable never creates it on its
own; once you (or `cronstable init C:\ProgramData\cronstable`, run from an
elevated prompt) put a config file in it, it takes precedence for everyone, so
an interactive administrator and a service account resolve to the same
config. The directory has to hold a config file, not merely exist. An empty
`%ProgramData%\cronstable`, left behind by an interrupted `init` or an
uninstall that took the files but not the folder, would otherwise take over
and load zero jobs, and because the path exists the "configuration file not
found" error would stay quiet too, so a working per-user setup would come up
healthy and running nothing. Until the machine-wide directory holds a config,
the default stays the historical per-user
`%APPDATA%\cronstable`; note that a service account's `APPDATA` points into
its own profile (LocalSystem: `C:\Windows\System32\config\systemprofile\...`),
which is why a per-user config that works interactively is not found when
the same command runs as a service, and why unattended deployments should
use the machine-wide directory. If `APPDATA` is somehow unset (rare, for
example a bare service account with no roaming profile), cronstable falls back
to the user profile directory (`~`, i.e. `os.path.expanduser("~")`) and uses
`<profile>\cronstable`.

You can point `-c` anywhere (a single YAML file or a directory of `*.yaml` /
`*.yml` files) exactly as on POSIX:

```shell
cronstable -c C:\path\to\cronstable.yaml
```

`cronstable init` writes a commented starter configuration into the default
directory (or any directory you name, either positionally or with `-c`),
creating it if needed and refusing to touch one that already holds
configuration:

```shell
cronstable init
```

### "Configuration file not found" applies to this path

cronstable has a special-case exit for a missing **default** config path: when the
`-c` argument is left at the platform default and that path does not exist,
cronstable prints the following to stderr (with the resolved default path filled
in), prints the usage help, and exits `1`:

```text
cronstable error: configuration file not found at the default location (<default path>). Run `cronstable init` to create a starter configuration there, or point -c/--config at an existing file or directory.
```

This check keys off the **platform default value**, not the literal string
`/etc/cronstable.d`. On Windows it therefore fires when `-c` resolves to the
platform default above (whether you omit `-c` or pass that path explicitly)
and the directory does not exist. For any *other* non-existent path you pass
with `-c`, you instead get the generic configuration-error path (a logged
`Configuration error: ...` and exit `1`). See the
[Command-Line Reference](CLI-Reference) for the full argument and exit-code
reference, and [Troubleshooting and FAQ](Troubleshooting) for the
problem/cause/fix entry.

## Default shell and running commands

How a string `command` is handed to a shell is platform-specific. The `shell`
field accepts any shell that exists on the machine; its default, and the flag
used to hand the command over, differ per platform and per shell:

| Platform | Default `shell` | A string `command` runs as |
| --- | --- | --- |
| POSIX | `/bin/sh` | `["/bin/sh", "-c", command]` |
| Windows | empty | `command` through the native command processor `%ComSpec%` (cmd.exe) |

An explicit `shell:` runs as `[shell, "-c", command]` (PowerShell reads `-c`
as an abbreviation of `-Command`), except for `cmd` / `cmd.exe`, which takes
the same route as the empty default below: the command string goes to
`%ComSpec% /c` whole, with no argv rendering in between. cmd.exe needs both
halves of that: it wants `/c` rather than `-c`, and it parses its command
line by its own rules instead of the `CommandLineToArgvW` rules that argv
rendering is built to be undone by, so a command containing double quotes
would otherwise arrive with the escaping still visible (`echo "hello world"`
printing `\"hello world\"`). Naming an absolute path to a particular cmd.exe
still uses that one rather than `%ComSpec%`. So `shell: cmd`,
`shell: powershell` and `shell: pwsh` all work as written on Windows. A
`shell:` naming a binary
that does not exist on the machine (for example `/bin/sh` carried over from a
POSIX config) fails at spawn with a `start_failed` run, visible in the
dashboard and reports; only the `SHELL=` line of a classic crontab is refused
earlier, at config load (see
[Features not supported on Windows](#features-not-supported-on-windows)).

On Windows the default `shell` is empty. An empty `shell` routes a string
`command` through the native command processor (`%ComSpec%`, i.e. `cmd.exe`)
via `asyncio.create_subprocess_shell`, the closest equivalent to the POSIX
`/bin/sh -c` path. A bare string command therefore runs under `cmd.exe` by
default:

```yaml
jobs:
  - name: hello
    command: echo Hello from cmd.exe
    schedule: "*/5 * * * *"
    captureStdout: true
```

### Working directory

A job inherits cronstable's own working directory unless it says otherwise,
and on Windows that is rarely where you want to be: a daemon started from an
elevated console starts in the system directory, and one started by Task
Scheduler starts wherever that action was configured to start. Either way,
every relative path inside a `.bat` or `.ps1` resolves somewhere the author
did not mean. `workingDirectory` names the directory the job's process starts
in, and is the equivalent of the "Start in (optional)" box on a Task
Scheduler action:

```yaml
jobs:
  - name: importer
    command: import.bat
    schedule: "0 * * * *"
    workingDirectory: C:\jobs\importer
```

`~` and `${VAR}` are expanded and the result is made absolute at config load.
A directory that does not exist is not a load error: the OS reports it at
spawn and the run is recorded as a launch failure with exit `127`. See
[Commands and Environment](Commands-and-Environment#workingdirectory) for
the full semantics, and the note below on program lookup, which is the one
place Windows and POSIX disagree about what `workingDirectory` covers.

### Using PowerShell or another interpreter

To run a command under PowerShell, or any interpreter other than `cmd.exe`, you
have two options. Set `shell:` explicitly:

```yaml
jobs:
  - name: powershell-shell
    command: Get-Date
    shell: powershell
    schedule: "*/5 * * * *"
    captureStdout: true
```

…or pass `command` as a **list**, which bypasses the shell entirely on every
platform (the argv is taken verbatim: no word splitting, globbing, quoting, or
variable expansion is performed):

```yaml
jobs:
  - name: powershell-list
    command:
      - powershell
      - -Command
      - Get-Date
    schedule: "*/5 * * * *"
    captureStdout: true
```

For the full shell-vs.-list semantics (including how `defaults.shell` is
inherited and how launch failures are handled), see
[Commands and Environment](Commands-and-Environment).

### Output encoding

Captured job output is decoded as UTF-8 first, strictly; a line that is not
valid UTF-8 is retried as the console's OEM code page (cp437, cp850, ...),
which is what cmd.exe builtins, `dir`, and localized OS error messages
actually emit, so accented output from a non-English install survives with
its characters intact. Bytes that decode under neither are kept with U+FFFD
replacement characters rather than failing the run. The passthrough mirror
(the copy of job output written to the daemon's own stdout/stderr) is encoded
with that stream's own encoding, so a redirected daemon log stays readable in
the code page its reader expects. Two YAML notes for Windows configs: quote
paths with single quotes (`'C:\scripts\nightly.bat'`; in double-quoted YAML a
backslash starts an escape sequence, so `"C:\temp\x"` silently becomes
something else), and prefer `utf-8` without BOM for `env_file` files
(files with a BOM are handled too).

## Graceful shutdown

To stop cronstable on Windows, press `Ctrl-C`. As on POSIX, this is a *graceful*
shutdown: cronstable stops scheduling new runs and finishes the currently running
jobs first, exactly as `SIGTERM` does on POSIX. It does not force-kill its own
running jobs on shutdown; each job runs in its own console process group, so
the Ctrl-C that stops the daemon is never delivered to the jobs themselves,
and an in-flight run completes and is recorded normally while the daemon
waits for it. (`Ctrl-Break` also stops the daemon, but a console-generated
break reaches every process attached to the console, jobs included, so
`Ctrl-C` is the stop that leaves running jobs untouched.)

Closing the console window and an OS shutdown or restart trigger the same
graceful drain, on a shorter leash: cronstable registers a native
console-control handler for those events (Python's signal module never
surfaces them), and Windows grants the process only a few seconds before
terminating it regardless, so the drain is best-effort; jobs still running
when that grace expires are killed by the OS with the daemon.

Logging off deliberately does not stop the daemon. Windows sends its logoff
event only to session-0 processes, which is to say services and scheduled
tasks, and the event says nothing about which user signed out; treating it as
a shutdown would stop an unattended daemon the first time anyone closed an
RDP session on the box. An interactive daemon is unaffected either way, since
Windows terminates interactive processes at logoff before that event is ever
sent. To stop a daemon that has no console of its own, use `POST /shutdown`.

Internally, POSIX wires `SIGINT`/`SIGTERM` onto the asyncio event loop. The
Windows Proactor loop has no `add_signal_handler`, so on Windows cronstable
instead installs `signal.signal` handlers for `SIGINT` (Ctrl-C) and `SIGBREAK`
(Ctrl-Break), runs a lightweight heartbeat timer so the interpreter observes
the pending handler promptly even while the loop is blocked in I/O, and adds
the `SetConsoleCtrlHandler` hook above for console close and OS shutdown. The
user-visible behavior is identical to POSIX. For the shutdown
sequence in detail, see
[Signal handling and graceful shutdown](CLI-Reference#signal-handling-and-graceful-shutdown)
in the [Command-Line Reference](CLI-Reference).

A daemon with no console (started by a service wrapper or scheduled task with
no window) has no keystroke to stop it: give it a web listener with an auth
token and stop it with `POST /shutdown`, the same graceful drain over HTTP.
See [Running unattended](#running-unattended) below and the
[HTTP Control API](HTTP-API).

## Running unattended

cronstable is a single foreground process and does not install itself as a
Windows service. To run it unattended (starting at boot, surviving logoff),
register it with the in-box Task Scheduler or wrap it in a service manager.
Use the machine-wide config directory for either, so the daemon reads the
same configuration no matter which account runs it.

One-time setup, from an elevated prompt:

```shell
cronstable init C:\ProgramData\cronstable
```

Then register a boot-time task running as `SYSTEM` (adjust the .exe path to
where you installed it):

```shell
schtasks /Create /TN cronstable /SC ONSTART /RU SYSTEM /RL HIGHEST /TR "\"C:\Program Files\cronstable\cronstable.exe\" -c C:\ProgramData\cronstable"
```

`schtasks /Run /TN cronstable` starts it immediately without a reboot, and
Task Scheduler's own settings (taskschd.msc, the task's Settings tab) add
restart-on-failure. A service wrapper such as
[WinSW](https://github.com/winsw/winsw) or
[NSSM](https://nssm.cc) works the same way and additionally supervises
crashes; point it at the same command line.

Three things every unattended deployment should add:

**A stop path.** With no console there is no Ctrl-C. Configure a loopback
listener with an auth token, and stop the daemon gracefully over HTTP; this
drains exactly as Ctrl-C does (running jobs finish first):

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromFile: C:\ProgramData\cronstable\_token   # any secret source works
```

```shell
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:8080/shutdown
```

Killing the process instead (`taskkill /F`, Task Manager) skips the drain and
leaves any spawned job trees running.

**A log file.** An unattended daemon's stderr goes nowhere. cronstable writes no
Windows Event Log entries; give the root logger a rotating file via the
`logging:` section (a plain `logging.config.dictConfig` passthrough, stdlib
only, works in the frozen .exe):

```yaml
logging:
  version: 1
  disable_existing_loggers: false
  formatters:
    file:
      format: "%(asctime)s %(levelname)s %(name)s %(message)s"
  handlers:
    file:
      class: logging.handlers.RotatingFileHandler
      filename: C:\ProgramData\cronstable\cronstable.log
      maxBytes: 10485760
      backupCount: 5
      formatter: file
  root:
    level: INFO
    handlers: [file]
```

See [Logging Configuration](Logging-Configuration) for the section's
semantics (including that `--validate-config` does not exercise it).

**A firewall rule, only if you bind beyond loopback.** Loopback-only
listeners (`127.0.0.1`) need no rule. A routable `web.listen` or
`cluster.listen` needs an inbound allow rule, created ahead of time for an
unattended box (the interactive Defender prompt has nobody to click it):

```shell
netsh advfirewall firewall add rule name="cronstable web" dir=in action=allow program="C:\Program Files\cronstable\cronstable.exe" protocol=TCP localport=8080
```

With the daemon starting at boot, `@reboot` jobs fire once per OS boot as
documented: the durable boot marker dedupes daemon restarts within one boot.
One platform caveat: Windows Fast Startup ("Shut down" with hiberboot
enabled, the client default) resumes the same kernel session at the next
power-on, and cronstable treats it as the same OS boot, so `@reboot` does not
re-fire after it; a Restart always begins a new boot. See
[Durable State](Durable-State) for the boot-identity mechanics.

## Job termination semantics

When cronstable stops a job (because its `executionTimeout` expired, because of
`concurrencyPolicy: Replace`, or because of a cancel request through the
[HTTP Control API](HTTP-API)) it performs a graceful step, waits up to
`killTimeout` seconds, then force-kills the run. The scope and meaning of the
two steps differ by platform:

| Platform | Graceful step | Forced step (after `killTimeout`) |
| --- | --- | --- |
| POSIX | `SIGTERM` to the job's whole process group (trappable; each job is spawned in its own session) | `SIGKILL` to the whole group, sent unconditionally |
| Windows | `CTRL_BREAK_EVENT` to the job's whole process group (trappable; each job is spawned in its own group) | `taskkill /F /T` on the job's live process tree |

Each job is spawned into its own console process group
(`CREATE_NEW_PROCESS_GROUP`), so the graceful step is a real, trappable
request: the job receives `CTRL_BREAK_EVENT`, which a Python program handles
as `signal.SIGBREAK`, a native program via `SetConsoleCtrlHandler`, and an
untrapped cmd.exe or console program by terminating. The job gets
`killTimeout` seconds to flush and exit on its own terms before the forced
step shells out to `taskkill /F /T /PID <pid>`, which walks the job's live
parent/child process tree so descendants the command left behind go down with
it.

The break needs a console shared between the daemon and the job. Where there
is none (a daemon started by a service wrapper with no console), the graceful
step cannot be delivered and becomes the tree kill immediately; the job gets
no notice there, and `killTimeout` has nothing left to bound.

Honest bounds on the sequence: a descendant that moved itself into a new
process group (`start /b` does) never receives the break, though the forced
tree walk still reaches it while its parent chain is alive; the `taskkill`
run itself is given 10 seconds before cronstable abandons it and falls back to
killing the direct child alone; and a descendant that was already orphaned
when `taskkill` ran (its parent exited first) is no longer in the tree and
survives. A survivor cannot strand the run, though: a killed run's wait for
its output pipes to drain is separately bounded.

For the full cancellation sequence (the process-group signalling, the
unconditional force kill, the bounded drain), the `-100` timeout return code,
and how `concurrencyPolicy: Replace` cancels the outgoing instance, see
[Cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout)
on [Concurrency and Timeouts](Concurrency-and-Timeouts).

## Process priority

A job can say how it should be scheduled against everything else on the box:

```yaml
jobs:
  - name: reindex
    command: reindex.cmd
    schedule: "0 3 * * *"
    priority: idle
```

The level becomes the process's Windows priority class, set on the creation
flags at `CreateProcess` time. That is the one race-free place to set it:

| Level | Windows priority class | Task Scheduler `-Priority` |
| --- | --- | --- |
| `idle` | `IDLE_PRIORITY_CLASS` | 9 to 10 |
| `below-normal` | `BELOW_NORMAL_PRIORITY_CLASS` | 7 to 8 (7 is its default) |
| `normal` (default) | no class flag; see below | 4 to 6 |
| `above-normal` | `ABOVE_NORMAL_PRIORITY_CLASS` | 2 to 3 |
| `high` | `HIGH_PRIORITY_CLASS` | 1 |

Windows hands every one of these classes to an unprivileged account, so
nothing here needs an elevated daemon. That is the sharp difference from
POSIX, where raising a priority needs `CAP_SYS_NICE` or `RLIMIT_NICE`
headroom and a refusal leaves the job at the priority it inherited.

`realtime` is not reachable, at any level of the config. REALTIME outranks the
threads that service disk, keyboard and mouse; one runaway job at that class
can put the machine out of reach of the operator who has to stop it, and
nothing a scheduled job does is worth that. `priority: realtime` is a load
error naming the five levels that are accepted.

### A lowered class covers the tree, a raised one does not

Windows gives a child created with no class flag of its own the creator's
class only when the creator is idle or below-normal, and NORMAL otherwise.
That single rule decides how far a job's level reaches:

- `idle` and `below-normal` cover the whole helper tree. A `.cmd` file at
  `priority: idle` runs cmd.exe at IDLE and every program cmd.exe launches at
  IDLE too.
- `above-normal` and `high` apply to the spawned process only. A `.cmd` file
  or a `shell: cmd` job at `priority: high` runs cmd.exe at HIGH, and every
  program cmd.exe actually launches runs at NORMAL.

Measured on Windows 11: an unflagged grandchild of a HIGH parent comes back
NORMAL, and an unflagged grandchild of an IDLE parent comes back IDLE.

That does not make `high` the wrong thing to ask for. It is still the level
that gets the job's own process scheduled ahead of the rest of the box, and
for a job whose command is the work (a `.exe`, a `python` script) the tree is
one process deep anyway. It does mean a `.cmd` wrapper does not hand its
level down, and a job that needs a raised program should name that program as
the `command` rather than wrap it. POSIX has no such asymmetry: cronstable
renices the whole process group, and anything forked afterwards inherits the
nice value.

### The default level

Task Scheduler numbers run the other way (0 highest, 10 lowest), and its
per-task default is 7, which is BELOW_NORMAL. cronstable's default is neither
a number nor a class: `normal` emits no class flag at all. That is
deliberate. Windows defaults a child to NORMAL only when its creator is not
itself idle or below-normal, so emitting `NORMAL_PRIORITY_CLASS` would
silently *promote* every job of a daemon that Task Scheduler had started at
its own default 7. Emitting nothing instead means a daemon at idle or below
normal hands that class to its jobs, and a daemon at normal or above hands
them NORMAL. Either way cronstable never promotes a job the operator did not
ask to promote, which is the case the flag exists to avoid.

See [priority](Commands-and-Environment#priority) for the POSIX half and the
full semantics.

## Features not supported on Windows

Three POSIX-specific features cannot work on Windows. None is silently
dropped: each is reported clearly. Two further platform differences (program
lookup under `workingDirectory`, and POSIX file modes) are not reported at
config load, and are called out at the end of this section so they are on the
record.

### Per-job `user` / `group` switching

Windows has no `setuid`/`setgid` model, so a job cannot drop to another user or
group. A job with `user` or `group` set raises a configuration error at config
load, verbatim:

```text
Job <name>: changing user/group is not supported on Windows
```

Remove the `user`/`group` fields from the job to run it on Windows. For the
POSIX semantics of these fields (resolution rules, the root requirement, and the
demotion ordering), see
[Commands and Environment](Commands-and-Environment).

### `unix://` web listeners

aiohttp's `UnixSite` needs `create_unix_server`, which the Windows Proactor
event loop does not provide, so `unix://` web listeners cannot be bound. Such a
`web.listen` URL is skipped (not fatal) with a warning, verbatim:

```text
Ignoring web listen url <url>: unix-socket listeners are not supported on this platform
```

Use an `http://` listener instead; it (and the entire HTTP control API and
[Web Dashboard](Web-Dashboard)) behaves identically on Windows:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
```

Because `web.socketMode` only ever applies to `unix://` sockets, it is
irrelevant on Windows. See the [HTTP Control API](HTTP-API) for the listener
configuration and [Web Dashboard](Web-Dashboard) for the browser UI.

Note that this limitation is specific to `unix://` **web** listeners. Gossip
clustering (the mTLS peer listener) does work on Windows: `cluster.listen` binds
a TCP `host:port`, not a unix socket, so the Proactor unix-socket restriction
does not apply. See [Clustering and Leader Election](Clustering-and-Leader-Election).

### Classic crontab `SHELL=` lines naming a POSIX path

A classic crontab that assigns `SHELL=` an absolute POSIX path (as
`/etc/crontab` and most exported crontabs do: `SHELL=/bin/sh`) is refused at
config load on Windows, with the assignment's own `file:line`, because that
shell cannot exist there and every entry below the line would otherwise load
cleanly and then fail at spawn. Remove the `SHELL` line to run the entries
through `%ComSpec%` (cmd.exe), or assign a shell that exists on the machine
(`SHELL=powershell`, or `SHELL=bash` where git-bash is installed). A `PATH=`
assignment with a POSIX value is kept but warned about, since it replaces the
Windows `PATH` for the entries below it. See
[Classic Crontabs](Classic-Crontabs) for the format's full semantics.

### `workingDirectory` does not change program lookup

Setting a job's
[`workingDirectory`](Commands-and-Environment#workingdirectory) changes where
the process starts, on Windows exactly as on POSIX. What it does not change on
Windows is where the *program* is looked up: `CreateProcessW` searches the
calling process's directory (cronstable's) rather than the child's working
directory. A list-form `command` naming its program by a relative path, one
carrying a separator such as `.\import.bat`, therefore resolves against the
working directory on POSIX and fails on Windows with a `start_failed` run and
a `FileNotFoundError`, from the same config. A bare name with no separator is
looked up on `PATH` on both platforms and never comes from the working
directory at all, since CPython assembles the `PATH` candidates in the parent,
before the child changes directory.

Name the program by full path, or leave `command` as a string so the command
processor resolves it, since `cmd.exe` starts in the working directory and
searches there:

```yaml
jobs:
  # cmd.exe starts in workingDirectory and finds import.bat there
  - name: importer
    command: import.bat
    schedule: "0 * * * *"
    workingDirectory: C:\jobs\importer

  # the list form bypasses the command processor, and CreateProcessW searches
  # cronstable's own directory rather than the child's, so this fails to launch
  - name: importer-argv
    command:
      - import.bat
    schedule: "0 * * * *"
    workingDirectory: C:\jobs\importer
```

### POSIX file modes are advisory only

The durable state store creates its directories `0o700` and its files
`0o600` on POSIX. Windows does not map POSIX mode bits onto ACLs: the
requested modes are effectively ignored and every file simply inherits its
parent directory's ACL. The store's records and archives can hold job output
and staged secrets, so on a multi-user Windows host put `state.path` (and any
`cronstable state backup` output) under a directory whose ACL is already
restricted to the account running the daemon, for example:

```shell
icacls C:\ProgramData\cronstable\state /inheritance:r /grant "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F"
```

See [Durable State](Durable-State) for what the store holds.

## Shared-mount coordination

The [durable state store](Durable-State) and the `filesystem` leadership
backend ([Clustering and Leader Election](Clustering-and-Leader-Election))
coordinate through advisory file locks. On Windows the lock primitive is an
`msvcrt.locking` byte-range lock rather than the POSIX `fcntl.flock`; between
processes on the *same* Windows host it excludes exactly as on POSIX, so
leases, leader election, and `concurrencyScope: cluster` slots all work fully
when the coordinating processes share one machine. What Windows cannot do is
*verify* cross-host reach: there is no `/proc/mounts` to probe, so
`topology: auto` resolves to `single-node`, and the lock-fidelity probe
(which runs on one host) cannot detect a mount whose locks are real locally
but never reach the file server.

Left at `topology: auto`, the state store logs an info line telling you to
set `state.topology: shared`, and the filesystem election backend warns at
startup that its locks only exclude local processes, verbatim (`<path>`
filled in):

```text
cluster: the filesystem election store at <path> resolved topology 'single-node', so its locks only exclude processes on THIS host (Windows/macOS cannot probe the mount); if the directory really is a shared network mount, set cluster.filesystem.topology: shared
```

Coordinating *across* Windows hosts over a shared mount therefore requires
both an explicit assertion (`state.topology: shared` and/or
`cluster.filesystem.topology: shared`) **and** a mount that truly honours
byte-range locks across hosts. cronstable cannot check the second half on
Windows, so with `topology: shared` asserted the election still logs a loud
startup advisory, verbatim, and the residual risk rests on your assertion:

```text
cluster: filesystem election on a Windows shared mount: cross-host lock fidelity cannot be verified on this platform (no /proc/mounts); the election is safe only if the mount honours byte-range locks across hosts
```

The same limit applies to `concurrencyScope: cluster`: over a mount that does
not propagate locks between hosts, a "cluster-wide" claim only guards
processes on the same host. Asserting `shared` over a mount that fakes its
locks is how you get two leaders or overlapping `Forbid` runs, so verify the
mount's lock semantics before trusting it. See [Durable State](Durable-State)
and [Clustering and Leader Election](Clustering-and-Leader-Election) for the
full coordination semantics and guarantees.

One neighbouring durable-state mechanism needs no caveat: crash
reconciliation's same-host pid-liveness check (an in-flight run left open by
a previous daemon is not declared dead while its recorded pid still exists,
because a daemon crash does not kill the job processes it spawned) works
fully on Windows, via `OpenProcess` in place of the POSIX `kill(pid, 0)`
probe.

## Everything else behaves identically

Apart from the differences above, cronstable behaves the same on Windows as on
POSIX. The YAML crontab, classic crontabs (with the `SHELL=` guard above),
schedules and timezones, environment variables and env files, output
capturing, concurrency, failure detection and retries, reporting
(mail / Sentry / shell / webhook), statsd metrics, the Prometheus `/metrics` endpoint,
the HTTP control API, the web dashboard, and the `cronstable tui` terminal
dashboard (which enables VT mode on the Windows console and reads keys via
`msvcrt`) all work as documented elsewhere in this wiki:

- [Classic Crontabs](Classic-Crontabs)
- [Schedules and Timezones](Schedules-and-Timezones)
- [Commands and Environment](Commands-and-Environment)
- [Output Capturing](Output-Capturing)
- [Concurrency and Timeouts](Concurrency-and-Timeouts)
- [Failure Detection and Retries](Failure-Detection-and-Retries)
- [Reporting (Mail, Sentry, Shell, Webhook)](Reporting)
- [Metrics with statsd](Metrics-with-Statsd) and
  [Metrics with Prometheus](Metrics-with-Prometheus)
- [HTTP Control API](HTTP-API) and [Web Dashboard](Web-Dashboard)
- [Terminal Dashboard](Terminal-Dashboard)

See [Installation](Installation) and the
[Command-Line Reference](CLI-Reference) to get started, and
[Troubleshooting and FAQ](Troubleshooting) if something does not behave as
expected.
