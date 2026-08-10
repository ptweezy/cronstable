# Windows Service

`cronstable service` registers the scheduler with the Windows Service
Control Manager, so it starts at boot and keeps running whether or not
anyone is logged on. "Run whether user is logged on or not" is the first
checkbox on a Task Scheduler task's General tab.

See [Running on Windows](Running-on-Windows) for the rest of the platform's
behavior, and [CLI Reference](CLI-Reference) for every other subcommand.

## Quick start

From an elevated prompt:

```shell
cronstable init C:\ProgramData\cronstable
cronstable service install -c C:\ProgramData\cronstable
cronstable service start
```

`cronstable service status` reports the state and the process ID; `sc query
cronstable` and the Services console (`services.msc`) see it like any other
service.

## Which installs can host a service

| Install shape | Can host a service |
| --- | --- |
| `pip install cronstable` / pipx | yes |
| A PyInstaller one-directory build | yes |
| The published one-file `.exe` (also what winget installs) | **no** |

The one-file executable cannot host a Windows service, and `install`
refuses it by name rather than producing something that fails at start. A
one-file build unpacks itself to a temporary directory and runs the program
in a **child** process; the SCM starts and watches the parent bootloader,
which never registers with the service dispatcher, so the start fails on the
SCM's timeout while the real program's own registration is refused with
error 1063. Nothing in cronstable can change that, because it is the
bootloader that forks.

Install with pip or pipx to run as a service. The
[`schtasks` recipe](Running-on-Windows#running-unattended) remains the
answer for the one-file executable, and it does start at boot and survive
logoff; what it does not give you is the Services console, recovery actions
and a real stop control.

## Commands

| Command | What it does |
| --- | --- |
| `cronstable service install` | Register the service. Needs elevation. |
| `cronstable service remove` | Stop it if running, then unregister. Needs elevation. |
| `cronstable service start` | Start it and wait until it reports running. |
| `cronstable service stop` | Ask it to stop, and wait for the drain. |
| `cronstable service status` | Print the state, the process ID, and why it last failed. |
| `cronstable service run` | What the SCM invokes. Not meant to be typed. |

`cronstable service run` typed by hand prints an explanation and exits `2`,
rather than appearing to hang.

### install

| Flag | Default | Meaning |
| --- | --- | --- |
| `--name NAME` | `cronstable` | Service name, for more than one instance on a host. |
| `-c`, `--config` | the platform default | Configuration the service will read. Baked into the command line as an absolute path. |
| `--start-type` | `auto` | `auto`, `delayed` (after the other automatic services) or `demand`. |
| `--log-level LEVEL` | `INFO` | Baked into the service's command line. |
| `--log-file PATH` | a `logs` directory beside the configuration | The bootstrap log. |
| `--no-log-file` | off | Do not open a bootstrap log. |
| `--console` | off | Allocate a console so job termination keeps its graceful step. See below. |
| `--restart-delay SECONDS` | `60` | How long Windows waits before restarting after a failure. |
| `--no-restart` | off | Do not configure recovery actions at all. |

The service is installed to run as LocalSystem. Per-job and per-service
identity (a named account, a gMSA, run levels) is not implemented; cronstable
is single-principal on Windows.

### The configuration path matters more than usual

A service runs as LocalSystem, whose `%APPDATA%` points into
`C:\Windows\System32\config\systemprofile`, not into your profile. So a
per-user configuration that works perfectly when you run `cronstable`
interactively is simply not found when the SCM starts the same program.

`install` therefore refuses when `-c` was left at the platform default
and that default resolved inside your own user profile: you did not choose
that path, and installing it would produce a service that starts cleanly and
schedules nothing. Name a machine-wide directory instead:

```shell
cronstable init C:\ProgramData\cronstable
cronstable service install -c C:\ProgramData\cronstable
```

A per-user path you name explicitly is a deliberate act and works, since
LocalSystem can read the profile; it prints a note about the fragility and
proceeds.

## Recovery

Unless `--no-restart` is given, `install` configures Windows' own recovery:
restart after `--restart-delay` seconds for the first two failures, then
stop trying, with the failure count resetting daily. It also sets the flag
that makes recovery apply to a clean exit with a nonzero code, not only
to a crash. Without that flag the recovery actions would be decorative here,
because this host reports its own failures as an orderly stop carrying an
exit code rather than by crashing.

`cronstable service status` decodes the last failure:

| Reported as | Meaning |
| --- | --- |
| `the configuration did not parse` | The `-c` path is missing or the YAML is invalid. |
| `the service log could not be opened` | The bootstrap log path is not writable. |
| `the scheduler stopped with an error` | The scheduler itself raised. |

A stop you asked for is a clean stop and does not trigger recovery. That
includes [`POST /shutdown`](HTTP-API): it drains and stops the service, and
the service stays stopped rather than being restarted.

## Where a service logs

A service has no console. Its `sys.stdout` and `sys.stderr` are not
redirected somewhere unhelpful, they are `None`, so the default logging
setup writes into nothing and cannot even report that it failed.

`service run` therefore opens a rotating bootstrap log before it does
anything else, by default at `<config directory>\logs\cronstable-service.log`
(10 MB, five backups). It records the command line, the resolved
configuration path, the console decision and the log level; it is the first
thing to read when a service will not start. If that file cannot be opened,
the service refuses to start rather than running mute, because a service
with no console and no log cannot be diagnosed at all.

A `logging:` section in the configuration still wins: it is applied on the
first housekeeping pass and replaces the bootstrap handler. Configure it as
the real log and treat the bootstrap file as the startup record. See
[Logging Configuration](Logging-Configuration) and the rotating-file recipe
in [Running on Windows](Running-on-Windows#running-unattended).

## `--console` and job termination

By default a service has no console, and that has one visible consequence:
the graceful step of stopping a job cannot be delivered.

cronstable normally terminates a job in two steps, a trappable
`CTRL_BREAK_EVENT` to the job's process group and then, `killTimeout`
seconds later, a `taskkill /F /T` tree kill. The break needs a console
shared between the daemon and the job. Without one the graceful step is
skipped and the kill is immediate, so `killTimeout` bounds nothing. That
applies to `executionTimeout`, to `concurrencyPolicy: Replace` and to a
cancel from the API.

`--console` allocates a console for the service so the two-step works. It is
off by default, because a console changes what a job inherits:

- the job is genuinely attached to that console. It can open `CONIN$` and
  `CONOUT$`, `GetConsoleWindow` no longer returns NULL, and a `pause` or
  `set /p` in a `.cmd` can still block;
- cronstable points the service's own standard handles at nothing straight
  after allocating, so an uncaptured job does not inherit a console `stdin`
  that blocks instead of reaching end of file. That closes the handle half;
  it does not undo console membership, which is inherited separately and is
  the entire reason for allocating one;
- it costs one extra `conhost.exe` per service.

Turn it on when `killTimeout` has to mean something. Leave it off otherwise.

## Removing

```shell
cronstable service stop
cronstable service remove
```

`remove` stops the service first and tolerates its already being stopped.
If anything else holds a handle to the service (the Services console and
Task Manager's Services tab both do), Windows marks it for deletion instead
of deleting it, and a reinstall then fails with "the service is pending
deletion". Close those windows and retry; `status` and `install` both name
that condition when they hit it.

## What the service does not change

Everything else behaves as it does in the foreground. The same
configuration, the same jobs, the same
[HTTP control API](HTTP-API) and [dashboard](Web-Dashboard), the same
durable state. `@reboot` jobs fire once per OS boot as documented. The
scheduler drains running jobs on stop, and the SCM is told the stop is still
in progress for as long as that takes, so a job that runs for an hour does
not make the service look hung.

## Related pages

- [Running on Windows](Running-on-Windows) for the rest of the platform
- [Windows Event Log](Windows-Event-Log) for publishing outcomes where
  Windows monitoring looks
- [Logging Configuration](Logging-Configuration) for the `logging:` section
- [CLI Reference](CLI-Reference) for every subcommand
- [Troubleshooting](Troubleshooting) for problem, cause and fix entries
