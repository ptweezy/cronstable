# Running on Windows

cronstable runs natively on Windows, alongside Linux and macOS. This page is the
canonical reference for the behaviors that differ on Windows: how to install
it, where it looks for configuration, how a string `command` is fed to a
shell, and how job output is decoded. It covers how to stop the daemon,
how to run it unattended (at boot, surviving logoff), how a job is terminated,
the POSIX-only features that are reported (never silently dropped) on Windows,
and what file-lock coordination over a shared mount can and cannot verify.
Everything not listed here behaves exactly as it does on POSIX, so the rest of
this wiki applies unchanged.

A single module (`cronstable/platform.py`) holds all the OS-specific
behavior. The scheduler, job runner, config loader, and entry point read the
same on every platform.

## Supported platforms and architectures

cronstable supports Windows on three CPU architectures: `amd64` (x64),
`arm64` (ARM64), and `i686` (32-bit x86). You can install it as a normal
Python package or as a self-contained executable. Use `i686` only on a
32-bit Windows install; on 64-bit Windows, choose `amd64` or `arm64`.

| Architecture | pip / pipx | Standalone binary | Zip (one-directory) | MSI |
| --- | --- | --- | --- | --- |
| `amd64` (x64) | `pip install cronstable` | `cronstable-windows-amd64.exe` | `cronstable-windows-amd64.zip` | `cronstable-windows-amd64.msi` |
| `arm64` (ARM64) | `pip install cronstable` | `cronstable-windows-arm64.exe` | `cronstable-windows-arm64.zip` | `cronstable-windows-arm64.msi` |
| `i686` (32-bit x86) | `pip install cronstable` | `cronstable-windows-i686.exe` | `cronstable-windows-i686.zip` | `cronstable-windows-i686.msi` |

The test suite runs on Windows (both x64 and ARM64) in CI on every commit. A
small set of POSIX-only tests is skipped there, each with a stated reason: the
tests for per-job user/group switching, privilege drop, POSIX signal delivery,
and POSIX file modes. Every release builds all three Windows architectures.
See
[contributing and releasing](Contributing-and-Releasing) for the build and
release workflow.

## Installation

There are five ways to install cronstable on Windows.

### MSI (machine-wide, hosts the service)

Every release attaches an MSI per architecture. It installs to
`C:\Program Files\cronstable` (the 32-bit `i686` package installs to
`C:\Program Files (x86)\cronstable` when run on 64-bit Windows), registers
the
[Windows service](Windows-Service), and puts the install directory on the
system `PATH`. It is the path for managed deployment through GPO, Intune, or
SCCM:

```shell
msiexec /i cronstable-windows-amd64.msi /qn
```

See [Windows MSI](Windows-MSI) for the silent-install properties, upgrade
behavior, and first-start steps.

### winget

The Windows Package Manager installs the standalone binary and puts
`cronstable` on your `PATH`:

```shell
winget install ptweezy.cronstable
```

Upgrade later with `winget upgrade ptweezy.cronstable`. The winget package is
a per-user install of the portable executable. For a machine-wide deployment,
use the preceding MSI or the following zip.

### pip / pipx

`pip install cronstable` works on Windows as it does on POSIX, installing the
`cronstable` console script into your environment. A supported Python (3.10 or
newer) must be present. See [installation](Installation) for the Python and
dependency requirements that apply on every platform.

```shell
pip install cronstable
cronstable --version
```

### Standalone binary (no Python required)

Every release attaches self-contained executables on the
[releases page](https://github.com/ptweezy/cronstable/releases):
`cronstable-windows-amd64.exe` (x64), `cronstable-windows-arm64.exe`
(ARM64), and `cronstable-windows-i686.exe` (32-bit x86). Python is **not**
required on the target system, because the
executable embeds the interpreter. Download the asset for your architecture,
then run it:

```shell
cronstable-windows-amd64.exe --version
```

The binaries are built natively on Windows runners (the ARM64 binary on a
`windows-11-arm` runner, the 32-bit binary against a 32-bit interpreter on
the x64 runner). As on every platform, the standalone binary is a
self-extracting executable. For the writable-and-executable temp-directory
detail, which matters only under unusual locked-down filesystems, see
[installation](Installation).

The Windows executables carry a version resource (Properties > Details shows
the product and version) and are Authenticode-signed with Azure Artifact
Signing. Each signature is timestamped, so it outlives the short-lived signing
certificates.

While the signing identity's reputation accrues, the first run of a
browser-downloaded `.exe` can still raise SmartScreen ("Windows protected your
PC"): choose "More info", then "Run anyway". If your policy requires it,
verify the download against the release's `SHA256SUMS`. Environments that
allowlist by publisher (AppLocker/WDAC) can use a publisher rule instead of
per-release hash rules.

### One-directory zip (hosts the service)

`cronstable-windows-amd64.zip`, `cronstable-windows-arm64.zip` and
`cronstable-windows-i686.zip` hold the same program as the standalone binary
in a one-directory layout: a single
`cronstable\` folder with `cronstable.exe` beside an `_internal\` directory,
running in place with no self-extraction. This is the download that can host
the [Windows service](Windows-Service). The one-file `.exe` cannot, and
`cronstable service install` refuses it by name.

A browser download carries the Mark of the Web, and extracting with
Explorer stamps it onto every extracted file, so the first run of the
extracted `cronstable.exe` would raise SmartScreen file by file. Clearing it
from the zip before extraction clears it for everything at once. From an
elevated PowerShell (writing into `C:\Program Files` needs one):

```powershell
Unblock-File .\cronstable-windows-amd64.zip
Expand-Archive .\cronstable-windows-amd64.zip -DestinationPath 'C:\Program Files'
& 'C:\Program Files\cronstable\cronstable.exe' --version
```

Extracting into `C:\Program Files` yields
`C:\Program Files\cronstable\cronstable.exe`, the path the recipes on this
page use. The preceding SmartScreen and AppLocker/WDAC notes apply to the
zip's executable the same way they apply to the one-file `.exe`.

There is no Windows container image. The published Docker image is Linux-only.
See [installation](Installation) for the Linux image and its supported
architectures.

## Default configuration location

When `-c`/`--config` is omitted, the directory cronstable looks in is
platform-specific:

| Platform | Default `-c` path |
| --- | --- |
| POSIX | `/etc/cronstable.d` |
| Windows, machine-wide | `%ProgramData%\cronstable` (for example, `C:\ProgramData\cronstable`), whenever that directory holds configuration |
| Windows, per-user | `%APPDATA%\cronstable` (for example, `C:\Users\<you>\AppData\Roaming\cronstable`), otherwise |

`%ProgramData%\cronstable` is the Windows analog of `/etc/cronstable.d`:
machine-scoped and shared by every account, and cronstable never creates it on
its own. After you put a configuration file in it, by hand or with
`cronstable init C:\ProgramData\cronstable` from an elevated prompt, it takes
precedence for everyone, so an interactive administrator and a service account
resolve to the same configuration.

It is not enough for the directory to exist; it has to hold a configuration
file. An empty `%ProgramData%\cronstable`, left behind by an interrupted
`init` or an uninstall that took the files but not the folder, would otherwise
take over and load zero jobs. Because the path exists, the "configuration file
not found" error would stay quiet too, so a working per-user setup would start
normally and run nothing.

Until the machine-wide directory holds a configuration file, the default stays
the historical per-user `%APPDATA%\cronstable`. A service account's `APPDATA`
points into its own profile (LocalSystem:
`C:\Windows\System32\config\systemprofile\...`). That is why a per-user
configuration that works interactively is not found when the same command runs
as a service, and why unattended deployments should use the machine-wide
directory.

If `APPDATA` is somehow unset (rare, for example a bare service account with
no roaming profile), cronstable falls back to the user profile directory
(`~`, that is `os.path.expanduser("~")`) and uses `<profile>\cronstable`.

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

### Who may write the config directory

Whoever can write the config directory decides what the scheduler runs, and a
service runs it as SYSTEM. On Windows that is easy to get wrong without
touching anything. `%ProgramData%` grants `BUILTIN\Users` the right to create
files, and a directory made under it inherits that, so every local account can
drop a `.yaml` there. A directory created at the root of a drive is worse again,
because `C:\` hands new subdirectories `Modify` for Authenticated Users.

`cronstable init` therefore checks the directory it created. If any local
account could write to it, `init` restricts it and reports the change: full
control for LocalSystem and the local Administrators group, read and execute
for everyone else, inheritance from the parent severed, and the directory
handed to the Administrators group. The hand-over is what makes the
restriction stick: Windows lets a directory's owner rewrite its permissions
whatever they say, and `%ProgramData%` lets any local account create a
directory and become its owner, so a directory that is merely restricted
stays that account's to reopen. `init` also writes an `OWNER RIGHTS` entry that holds
the owner to read, so the restriction holds even where the hand-over is
refused. An unelevated prompt gets that outcome, and `init` then names the
owner it could not change together with the fix. A per-user
`%APPDATA%\cronstable` carries no such permission to begin with, so `init`
leaves it alone.

An existing machine-wide directory is handed to Administrators before the
starter is written and restricted after it. `init` refuses such a directory
when another account owns it and the hand-over fails, and it refuses a
junction or symbolic link standing at the path, because the scheduler reads
whatever the link's target holds. Nothing is written in either case, and the
directory's permissions stay as they were. Run `init` again from an elevated
prompt, or remove the directory first.

`init` deliberately leaves read in place. The boundary this defends is who may
*write* a job. Removing read would stop an unelevated account from listing the
directory at all, which makes the machine-wide path stop resolving for
that account. If your configuration holds secrets inline rather than in
[`fromFile`](Reporting#secrets) sources, tighten it further.

For a directory that already exists, the daemon says so once at startup and
prints the same fix, and `cronstable service install` prints the same note.
The finding names the account that can write, or the owner of a machine-wide
directory when that owner is neither SYSTEM, Administrators nor
TrustedInstaller:

```text
C:\ProgramData\cronstable can be written by BUILTIN\Users, so any local
account can add or change a job, and a service runs them as SYSTEM.
Restrict it with: icacls "C:\ProgramData\cronstable" /inheritance:r
/grant *S-1-5-18:(OI)(CI)F /grant *S-1-5-32-544:(OI)(CI)F
/grant *S-1-5-11:(OI)(CI)RX /grant *S-1-3-4:(OI)(CI)RX, then icacls
"C:\ProgramData\cronstable" /setowner *S-1-5-32-544
```

A junction or symbolic link standing at the path gets its own sentence, asking
for a real directory in its place: no permission on a link changes what its
target holds, and the finding stands whether or not that target lets the
daemon read a descriptor. Other reparse points, such as a cloud-file
placeholder, redirect nothing, and the check leaves them alone. A machine-wide
path is one outside the user profiles. A service runs from a profile of its
own under `system32`, so every user's directory counts as outside for it, and
a service reading one is told who owns it.

The recipe names SIDs rather than group names so it pastes unchanged on a
localized install, where `BUILTIN\Administrators` is spelled in the local
language. `S-1-5-11` keeps read for Authenticated Users, `S-1-3-4` is
`OWNER RIGHTS`, and `/setowner` is its own `icacls` command because `icacls`
refuses it beside `/grant`. Both commands need an elevated prompt.

### "Configuration file not found" applies to this path

There is a special-case exit for a missing **default** configuration path. If
the `-c` argument is left at the platform default and that path does not
exist, cronstable prints the following to stderr (with the resolved default
path filled in), prints the usage help, and exits `1`:

```text
cronstable error: configuration file not found at the default location (<default path>). Run `cronstable init` to create a starter configuration there, or point -c/--config at an existing file or directory.
```

This check keys off the **platform default value**, not the literal string
`/etc/cronstable.d`. On Windows it therefore fires when the directory does not
exist and `-c` resolves to the platform default given earlier,
whether you omit `-c` or pass that path explicitly. If you pass any *other*
non-existent path with `-c`, you get the generic configuration-error path
instead: a logged `Configuration error: ...` and exit `1`.

See the [command-line reference](CLI-Reference) for the full argument and
exit-code reference, and [troubleshooting and FAQ](Troubleshooting) for the
problem/cause/fix entry.

## Default shell and running commands

How a string `command` is handed to a shell is platform-specific. The `shell`
field accepts any shell that exists on the machine. Its default, and the flag
used to hand the command over, differ per platform and per shell:

| Platform | Default `shell` | A string `command` runs as |
| --- | --- | --- |
| POSIX | `/bin/sh` | `["/bin/sh", "-c", command]` |
| Windows | empty | `command` through the native command processor `%ComSpec%` (cmd.exe) |

An explicit `shell:` runs as `[shell, "-c", command]`; PowerShell reads `-c`
as an abbreviation of `-Command`. The exception is `cmd` / `cmd.exe`, which
takes the same route as the empty default described later in this section: the
command string goes to `%ComSpec% /c` whole, with no argv rendering in
between.

cmd.exe needs both halves of that. It takes `/c` rather than `-c`, and it
parses its command line by its own rules instead of the `CommandLineToArgvW`
rules that argv rendering is built to be undone by. A command containing
double quotes would otherwise arrive with the escaping still visible:
`echo "hello world"` printing `\"hello world\"`.

Naming an absolute path to a particular cmd.exe still uses that one rather
than `%ComSpec%`. So `shell: cmd`, `shell: powershell` and `shell: pwsh` all
work as written on Windows.

A `shell:` naming a binary that does not exist on the machine, for example
`/bin/sh` carried over from a POSIX configuration, fails at spawn with a
`start_failed` run, visible in the dashboard and reports. Only the `SHELL=`
line of a classic crontab is refused earlier, at config load (see
[features not supported on Windows](#features-not-supported-on-windows)).

On Windows the default `shell` is empty. An empty `shell` routes a string
`command` through the native command processor (`%ComSpec%`, that is,
`cmd.exe`) with `asyncio.create_subprocess_shell`, the closest equivalent to
the POSIX `/bin/sh -c` path. A bare string command therefore runs under
`cmd.exe` by default:

```yaml
jobs:
  - name: hello
    command: echo Hello from cmd.exe
    schedule: "*/5 * * * *"
    captureStdout: true
```

### Working directory

A job inherits cronstable's own working directory unless it sets one,
and on Windows that is rarely the directory you want. A daemon started from an
elevated console starts in the system directory, and one started by Task
Scheduler starts wherever that action was configured to. Either way, every
relative path inside a `.bat` or `.ps1` resolves somewhere the author did not
mean. `workingDirectory` names the directory the job's process starts in, the
equivalent of the **Start in (optional)** box on a Task Scheduler action:

```yaml
jobs:
  - name: importer
    command: import.bat
    schedule: "0 * * * *"
    workingDirectory: C:\jobs\importer
```

At config load cronstable expands `~` and `${VAR}` and makes the result
absolute. A directory that does not exist is not a load error: the OS reports
it at spawn, and cronstable records the run as a launch failure with exit
`127`. See [commands and environment](Commands-and-Environment#workingdirectory)
for the full semantics, and the later note on program lookup, the one place
where Windows and POSIX differ about what `workingDirectory` covers.

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

Or pass `command` as a **list**, which bypasses the shell entirely on every
platform. The argv is taken verbatim, with no word splitting, globbing,
quoting, or variable expansion:

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
[commands and environment](Commands-and-Environment).

### Output encoding

Captured job output is decoded as UTF-8 first, strictly. A line that is not
valid UTF-8 is retried as the console's OEM code page (cp437, cp850, ...),
which is what cmd.exe builtins, `dir`, and localized OS error messages emit,
so accented output from a non-English install survives with its characters
intact. Bytes that decode under neither are kept with U+FFFD replacement
characters rather than failing the run.

The passthrough mirror (the copy of job output written to the daemon's own
stdout/stderr) is encoded with that stream's own encoding, so a redirected
daemon log stays readable in the code page its reader expects.

Two YAML notes apply to Windows configuration files. Quote paths with single
quotes (`'C:\scripts\nightly.bat'`): in double-quoted YAML a backslash starts
an escape sequence, so `"C:\temp\x"` silently becomes something else. Prefer
`utf-8` without a BOM for `env_file` files; files with a BOM are handled too.

## Graceful shutdown

To stop cronstable on Windows, press `Ctrl-C`. As on POSIX, this is a
*graceful* shutdown: the daemon stops scheduling new runs and finishes the
currently running jobs first, exactly as `SIGTERM` does on POSIX. It does not
force-stop its own running jobs on shutdown.

Each job runs in its own console process group, so the `Ctrl-C` that stops the
daemon is never delivered to the jobs themselves, and a run in progress
completes and is recorded normally while the daemon waits for it.

`Ctrl-Break` also stops the daemon, but a console-generated break reaches
every process attached to the console, jobs included, so `Ctrl-C` is the stop
that leaves running jobs untouched.

Closing the console window and an OS shutdown or restart trigger the same
graceful drain, on a shorter deadline. The daemon registers a Win32
console-control handler for those events, which Python's signal module never
surfaces. Windows grants the process only a few seconds before ending it
regardless, so the drain is best-effort: when that grace expires, the OS ends
the daemon and any jobs still running.

Logging off deliberately does not stop the daemon. Windows sends its logoff
event only to session-0 processes, which is to say services and scheduled
tasks, and the event says nothing about which user signed out. Treating it as
a shutdown would stop an unattended daemon the first time anyone closed an RDP
session on the machine.

An interactive daemon is unaffected either way, because Windows ends
interactive processes at logoff before that event is ever sent. To stop a
daemon that has no console of its own, use `POST /shutdown`.

Internally, cronstable wires `SIGINT`/`SIGTERM` onto the asyncio event loop on
POSIX. The Windows Proactor loop has no `add_signal_handler`, so on Windows
the daemon instead:

- Installs `signal.signal` handlers for `SIGINT` (Ctrl-C) and `SIGBREAK`
  (Ctrl-Break).
- Runs a lightweight heartbeat timer, so the interpreter observes the pending
  handler promptly even while the loop is blocked in I/O.
- Adds the earlier `SetConsoleCtrlHandler` hook for console close and OS
  shutdown.

The user-visible behavior is identical to POSIX. For the shutdown
sequence in detail, see
[signal handling and graceful shutdown](CLI-Reference#signal-handling-and-graceful-shutdown)
in the [command-line reference](CLI-Reference).

A daemon with no console (started by a service wrapper or scheduled task with
no window) has no keystroke to stop it. Give it a web listener with an auth
token and stop it with `POST /shutdown`, the same graceful drain over HTTP.
See the following [running unattended](#running-unattended) section and the
[HTTP control API](HTTP-API).

## Running unattended

The supported way to run cronstable unattended is as a Windows service.
From an elevated prompt:

```shell
cronstable init C:\ProgramData\cronstable
cronstable service install -c C:\ProgramData\cronstable
cronstable service start
```

That registers cronstable with the Service Control Manager, so it
starts at boot, keeps running after you log off, appears in `services.msc`,
and gets Windows' own recovery actions. Stopping it drains running jobs first,
and the SCM is told the stop is in progress for as long as that takes.

The commands work from a pip or pipx install and from the extracted
[one-directory zip](#one-directory-zip-hosts-the-service), but not at all from
the one-file `.exe`. The [MSI](Windows-MSI) registers the service by itself,
so none of them are needed there. A service host therefore needs no Python
installed.

See [Windows service](Windows-Service) for the full command set, the logging
story, and the one install shape that cannot host a service: the published
one-file `.exe` (also what winget installs), whose bootloader runs the program
in a child process the SCM never sees.

### Task Scheduler, for the one-file executable

Where the service is not available, the built-in Task Scheduler starts
cronstable at boot and survives logoff too. It gives you neither the
**Services** console nor a real stop control, but it works. Use the
machine-wide config directory either way, so the daemon reads the same
configuration no matter which account runs it.

Register a boot-time task running as `SYSTEM`. Adjust the `.exe` path to where
you installed it; extracting the one-directory zip into `C:\Program Files`
produces exactly the path this recipe uses:

```shell
schtasks /Create /TN cronstable /SC ONSTART /RU SYSTEM /RL HIGHEST /TR "\"C:\Program Files\cronstable\cronstable.exe\" -c C:\ProgramData\cronstable"
```

`schtasks /Run /TN cronstable` starts it immediately without a reboot, and
Task Scheduler's own settings (`taskschd.msc`, the task's **Settings** tab)
add restart-on-failure. A service wrapper such as
[WinSW](https://github.com/winsw/winsw) or
[NSSM](https://nssm.cc) works the same way and additionally supervises
crashes. Point it at the same command line.

Three things a Task Scheduler or wrapper deployment should add. A service
installed with `cronstable service install` already has the first two.

**A stop path.** With no console there is no `Ctrl-C`. Configure a loopback
listener with an auth token, and stop the daemon gracefully over HTTP. This
drains exactly as `Ctrl-C` does, and running jobs finish first:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromFile: C:\ProgramData\cronstable\_token   # any secret source works
```

A file inside the config directory inherits its permissions, which keep read
for Authenticated Users, so every local account can read a token kept there.
Give the token file its own permissions:

```shell
icacls C:\ProgramData\cronstable\_token /inheritance:r /grant *S-1-5-18:F /grant *S-1-5-32-544:F
```

```shell
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:8080/shutdown
```

Ending the process outright (`taskkill /F`, Task Manager) skips the drain and
leaves any spawned job trees running.

**A log file.** An unattended daemon's stderr goes nowhere. Give the root
logger a rotating file through the `logging:` section (a plain
`logging.config.dictConfig` passthrough, standard library only, works in the
frozen `.exe`):

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

See [logging configuration](Logging-Configuration) for the section's
semantics, including that `--validate-config` does not exercise it.

That log file and the later Event Log reporter cover different things, and on
an unattended machine you usually want both. The `logging:` section captures
the daemon's own log, and `report.eventlog` publishes per-run outcomes where
monitoring can pick them up.

**A firewall rule, only if you bind beyond loopback.** Loopback-only
listeners (`127.0.0.1`) need no rule. A routable `web.listen` or
`cluster.listen` needs an inbound allow rule. Create it ahead of time on an
unattended machine, where nobody can answer the interactive Defender prompt:

```shell
netsh advfirewall firewall add rule name="cronstable web" dir=in action=allow program="C:\Program Files\cronstable\cronstable.exe" protocol=TCP localport=8080
```

With the daemon starting at boot, `@reboot` jobs fire once per OS boot as
documented: the durable boot marker dedupes daemon restarts within one boot.

One platform caveat: Windows Fast Startup ("Shut down" with hiberboot
enabled, the client default) resumes the same kernel session at the next
power-on, and cronstable treats it as the same OS boot, so `@reboot` does not
re-fire after it. A Restart always begins a new boot. See
[durable state](Durable-State) for the boot-identity mechanics.

## Windows Event Log

Job outcomes can be written to the Windows Event Log, where a Windows shop's
monitoring already looks: Event Viewer, a Windows Event Forwarding
subscription, SCOM, and every SIEM connector. It is a sixth reporter in the
same `report` block as the other five, so it fires from the same hooks:

```yaml
defaults:
  onFailure:
    report:
      eventlog:
        enabled: true
```

Records carry a stable event ID (1000 succeeded, 1001 failed, 1002 failed
permanently, 1003 overdue, 1010 and 1011 for daemon events) and a fixed set
of insertion strings, so a rule written against them keeps working:

```powershell
Get-WinEvent -FilterHashtable @{ LogName = 'Application'; ProviderName = 'cronstable'; ID = 1001, 1002 }
```

The daemon does not register its event source, because that needs an HKLM
write and buys nothing without a message DLL. Event Viewer therefore prefixes
the rendered text with its generic "description cannot be found" note. That
costs the rendered prose only: the provider, the ID, the level, and every
insertion string are all present, so the XML view, `wevtutil`, forwarding, and
SIEM connectors read the record normally.

See [Windows Event Log](Windows-Event-Log) for the full tables, the optional
source registration, and why `includeOutput` is off by default.

## Job termination semantics

When the daemon stops a job, because its `executionTimeout` expired, because
of `concurrencyPolicy: Replace`, or because of a cancel request through the
[HTTP control API](HTTP-API), it performs a graceful step, waits up to
`killTimeout` seconds, then force-stops the run. The scope and meaning of the
two steps differ by platform:

| Platform | Graceful step | Forced step (after `killTimeout`) |
| --- | --- | --- |
| POSIX | `SIGTERM` to the job's whole process group (trappable; each job is spawned in its own session) | `SIGKILL` to the whole group, sent unconditionally |
| Windows | `CTRL_BREAK_EVENT` to the job's whole process group (trappable; each job is spawned in its own group) | `taskkill /F /T` on the job's live process tree |

Each job is spawned into its own console process group
(`CREATE_NEW_PROCESS_GROUP`), so the graceful step is a real, trappable
request. The job receives `CTRL_BREAK_EVENT`. A Python program handles that as
`signal.SIGBREAK`, a native program through `SetConsoleCtrlHandler`, and an
untrapped cmd.exe or console program by terminating.

The job gets `killTimeout` seconds to flush and exit on its own terms. The
forced step then runs `taskkill /F /T /PID <pid>`, which walks the job's live
parent/child process tree and ends descendants the command left behind.

The break needs a console shared between the daemon and the job. Where there
is none, such as a daemon started by a service wrapper with no console, the
graceful step cannot be delivered and becomes the forced step immediately. The
job gets no notice there, and `killTimeout` has nothing left to bound.

A service installed with `cronstable service install --console` allocates one
so the two-step sequence keeps working. That option is off by default
because an allocated console changes what a job inherits. See
[Windows service](Windows-Service#--console-and-job-termination).

Honest bounds on the sequence:

- A descendant that moved itself into a new process group (`start /b` does)
  never receives the break, though the forced tree walk still reaches it while
  its parent chain is alive.
- The `taskkill` run itself is given 10 seconds before cronstable abandons it
  and falls back to ending the direct child alone.
- A descendant that was already orphaned when `taskkill` ran (its parent
  exited first) is no longer in the tree and survives.

A survivor cannot strand the run: the wait for a stopped run's output pipes to
drain is separately bounded.

For the full cancellation sequence (the process-group signaling, the
unconditional forced step, the bounded drain), the `-100` timeout return code,
and how `concurrencyPolicy: Replace` cancels the outgoing instance, see
[cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout)
on [concurrency and timeouts](Concurrency-and-Timeouts).

## Process priority

A job's `priority` sets the class it runs at, relative to everything else on
the machine:

```yaml
jobs:
  - name: reindex
    command: reindex.cmd
    schedule: "0 3 * * *"
    priority: idle
```

The level becomes the process's Windows priority class, set on the creation
flags at `CreateProcess` time, which is the one race-free place to set it:

| Level | Windows priority class | Task Scheduler `-Priority` |
| --- | --- | --- |
| `idle` | `IDLE_PRIORITY_CLASS` | 9 to 10 |
| `below-normal` | `BELOW_NORMAL_PRIORITY_CLASS` | 7 to 8 (7 is its default) |
| `normal` (default) | no class flag; see later | 4 to 6 |
| `above-normal` | `ABOVE_NORMAL_PRIORITY_CLASS` | 2 to 3 |
| `high` | `HIGH_PRIORITY_CLASS` | 1 |

Windows hands every one of these classes to an unprivileged account, so
nothing here needs an elevated daemon. POSIX differs: raising a priority
there needs `CAP_SYS_NICE` or `RLIMIT_NICE` headroom, and a refusal leaves
the job at the priority it inherited.

`realtime` is not reachable, at any level of the configuration. REALTIME
outranks the threads that service disk, keyboard, and mouse. One runaway job
at that class can put the machine out of reach of the operator who has to stop
it, and nothing a scheduled job does is worth that. `priority: realtime` is a
load error that names the five accepted levels.

### A lowered class covers the tree, a raised one does not

A child created with no class flag of its own gets the creator's class only
when the creator is idle or below-normal; otherwise Windows gives it NORMAL.
That one rule decides how far a job's level reaches:

- `idle` and `below-normal` cover the whole helper tree. A `.cmd` file at
  `priority: idle` runs cmd.exe at IDLE and every program cmd.exe launches at
  IDLE too.
- `above-normal` and `high` apply to the spawned process only. A `.cmd` file
  or a `shell: cmd` job at `priority: high` runs cmd.exe at HIGH, and every
  program cmd.exe launches runs at NORMAL.

Measured on Windows 11: an unflagged grandchild of a HIGH parent comes back
NORMAL, and an unflagged grandchild of an IDLE parent comes back IDLE.

`high` is still worth asking for. It gets the job's own process scheduled
ahead of the rest of the machine, and for a job whose command is the work (a
`.exe`, a `python` script) the tree is one process deep anyway. A `.cmd`
wrapper, though, does not hand its level down, so a job that needs a raised
program should name that program as the `command` rather than wrap it. POSIX
has no such asymmetry: the daemon renices the whole process group, and
anything forked afterward inherits the nice value.

### The default level

Task Scheduler numbers run the other way (0 highest, 10 lowest), and its
per-task default is 7, which is BELOW_NORMAL. The cronstable default is
deliberately neither a number nor a class: `normal` emits no class flag at
all.

Windows defaults a child to NORMAL only when its creator is not itself idle or
below-normal, so emitting `NORMAL_PRIORITY_CLASS` would silently *promote*
every job of a daemon that Task Scheduler had started at its own default 7.
Emitting nothing instead means a daemon at idle or below normal hands that
class to its jobs, and a daemon at normal or above hands them NORMAL. Either
way cronstable never promotes a job the operator did not ask to promote.

See [priority](Commands-and-Environment#priority) for the POSIX half and the
full semantics.

## Features not supported on Windows

Three POSIX-specific features cannot work on Windows. None is silently
dropped: each is reported clearly. Two further platform differences (program
lookup under `workingDirectory`, and POSIX file modes) are not reported at
config load, and are called out at the end of this section for the record.

### Per-job `user` / `group` switching

Windows has no `setuid`/`setgid` model, so a job cannot drop to another user or
group. A job with `user` or `group` set raises a configuration error at config
load, verbatim:

```text
Job <name>: changing user/group is not supported on Windows
```

To run the job on Windows, remove the `user`/`group` fields. For the POSIX
semantics of these fields (resolution rules, the root requirement, and the
demotion ordering), see
[commands and environment](Commands-and-Environment).

### `unix://` web listeners

aiohttp's `UnixSite` needs `create_unix_server`, which the Windows Proactor
event loop does not provide, so `unix://` web listeners cannot be bound. Such a
`web.listen` URL is skipped (not fatal) with a warning, verbatim:

```text
Ignoring web listen url <url>: unix-socket listeners are not supported on this platform
```

Use an `http://` listener instead. It behaves identically on Windows, as do
the entire HTTP control API and the [web dashboard](Web-Dashboard):

```yaml
web:
  listen:
    - http://127.0.0.1:8080
```

Because `web.socketMode` only ever applies to `unix://` sockets, it is
irrelevant on Windows. See the [HTTP control API](HTTP-API) for the listener
configuration and the [web dashboard](Web-Dashboard) for the browser UI.

This limitation is specific to `unix://` **web** listeners. Gossip
clustering (the mTLS peer listener) does work on Windows: `cluster.listen` binds
a TCP `host:port`, not a unix socket, so the Proactor unix-socket restriction
does not apply. See [clustering and leader election](Clustering-and-Leader-Election).

### Classic crontab `SHELL=` lines naming a POSIX path

A classic crontab that assigns `SHELL=` an absolute POSIX path, as
`/etc/crontab` and most exported crontabs do (`SHELL=/bin/sh`), is refused at
config load on Windows, with the assignment's own `file:line`, because that
shell cannot exist there and every entry below the line would otherwise load
cleanly and then fail at spawn.

To run the entries through `%ComSpec%` (cmd.exe), remove the `SHELL` line. To
use a different shell, assign one that exists on the machine
(`SHELL=powershell`, or `SHELL=bash` where git-bash is installed). A `PATH=`
assignment with a POSIX value is kept but warned about, because it replaces
the Windows `PATH` for the entries below it. See
[classic crontabs](Classic-Crontabs) for the format's full semantics.

### `workingDirectory` does not change program lookup

Setting a job's
[`workingDirectory`](Commands-and-Environment#workingdirectory) changes where
the process starts, on Windows exactly as on POSIX. On Windows it does not
change where the *program* is looked up: `CreateProcessW` searches the
calling process's directory (cronstable's) rather than the child's working
directory.

A list-form `command` naming its program by a relative path, one carrying a
separator such as `.\import.bat`, therefore resolves against the working
directory on POSIX and fails on Windows with a `start_failed` run and a
`FileNotFoundError`, from the same configuration. A bare name with no
separator is looked up on `PATH` on both platforms and never comes from the
working directory at all, because CPython assembles the `PATH` candidates in
the parent, before the child changes directory.

Name the program by full path, or leave `command` as a string so the command
processor resolves it, because `cmd.exe` starts in the working directory and
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
requested modes are effectively ignored, and every file inherits its parent
directory's ACL.

The store's records and archives can hold job output and staged secrets. On a
multi-user Windows host, put `state.path` (and any `cronstable state backup`
output) under a directory whose ACL is already restricted to the account
running the daemon, for example:

```shell
icacls C:\ProgramData\cronstable\state /inheritance:r /grant "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F"
```

See [durable state](Durable-State) for what the store holds.

## Shared-mount coordination

The [durable state store](Durable-State) and the `filesystem` leadership
backend ([clustering and leader election](Clustering-and-Leader-Election))
coordinate through advisory file locks. On Windows the lock primitive is an
`msvcrt.locking` byte-range lock rather than the POSIX `fcntl.flock`. Between
processes on the *same* Windows host it excludes exactly as on POSIX, so
leases, leader election, and `concurrencyScope: cluster` slots all work fully
when the coordinating processes share one machine.

What Windows cannot do is *verify* cross-host reach. There is no
`/proc/mounts` to probe, so `topology: auto` resolves to `single-node`, and
the lock-fidelity probe (which runs on one host) cannot detect a mount whose
locks are real locally but never reach the file server.

Left at `topology: auto`, the state store logs an info line telling you to
set `state.topology: shared`, and the filesystem election backend warns at
startup that its locks only exclude local processes, verbatim (`<path>`
filled in):

```text
cluster: the filesystem election store at <path> resolved topology 'single-node', so its locks only exclude processes on THIS host (Windows/macOS cannot probe the mount); if the directory really is a shared network mount, set cluster.filesystem.topology: shared
```

Coordinating *across* Windows hosts over a shared mount therefore requires
both an explicit assertion (`state.topology: shared`,
`cluster.filesystem.topology: shared`, or both) **and** a mount that truly
honors byte-range locks across hosts. The daemon cannot check the second half
on Windows, so with `topology: shared` asserted the election still logs a loud
startup advisory, verbatim, and the residual risk rests on your assertion:

```text
cluster: filesystem election on a Windows shared mount: cross-host lock fidelity cannot be verified on this platform (no /proc/mounts); the election is safe only if the mount honours byte-range locks across hosts
```

The same limit applies to `concurrencyScope: cluster`. Over a mount that does
not propagate locks between hosts, a "cluster-wide" claim only guards
processes on the same host. Asserting `shared` over a mount that fakes its
locks is how you get two leaders or overlapping `Forbid` runs, so verify the
mount's lock semantics before trusting it. See [durable state](Durable-State)
and [clustering and leader election](Clustering-and-Leader-Election) for the
full coordination semantics and guarantees.

One neighboring durable-state mechanism needs no caveat: crash
reconciliation's same-host pid-liveness check works fully on Windows, using
`OpenProcess` in place of the POSIX `kill(pid, 0)` probe. An in-flight run
left open by a previous daemon is not declared dead while its recorded pid
still exists, because a daemon crash does not end the job processes it
spawned.

## Everything else behaves identically

Apart from the preceding differences, cronstable behaves the same on Windows
as on POSIX. The YAML crontab, classic crontabs (with the earlier `SHELL=`
guard), schedules and time zones, environment variables and env files, output
capturing, concurrency, failure detection and retries, and reporting
(mail / Sentry / shell / webhook / push, plus the Windows-only
[Event Log reporter](Windows-Event-Log)) are identical.

So are statsd metrics, the Prometheus `/metrics` endpoint, the HTTP control
API, the web dashboard, and the `cronstable tui` terminal dashboard, which
enables VT mode on the Windows console and reads keys with `msvcrt`. All of it
works as documented elsewhere in this wiki:

- [Classic Crontabs](Classic-Crontabs)
- [Importing from Task Scheduler](Importing-Task-Scheduler)
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

See [installation](Installation) and the
[command-line reference](CLI-Reference) to get started, and
[troubleshooting and FAQ](Troubleshooting) if something does not behave as
expected.
