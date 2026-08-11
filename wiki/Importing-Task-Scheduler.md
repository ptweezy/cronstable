# Importing from Task Scheduler

`cronstable import-taskscheduler` converts Windows Task Scheduler XML
exports into cronstable jobs, so an existing estate does not have to be
retyped.

It is a one-shot converter, not a loader. It writes YAML you read, edit and
commit; cronstable never reads Task Scheduler XML at run time. The main
reason is that exporting a task does not unregister it, so an export
describes tasks Task Scheduler is still running. Loading the output without
reviewing it first means both schedulers run the same work.

## Exporting

Whole machine, in one file:

```shell
schtasks /query /XML ONE > tasks.xml
cronstable import-taskscheduler tasks.xml -o jobs.yaml
```

One task, or a folder of them, from PowerShell:

```powershell
Export-ScheduledTask -TaskName "Nightly Backup" -TaskPath "\Contoso\" |
    Out-File -FilePath backup.xml
cronstable import-taskscheduler backup.xml -o jobs.yaml
```

A directory of exports, or a pipe:

```shell
cronstable import-taskscheduler C:\exports -o jobs.yaml
schtasks /query /XML ONE | cronstable import-taskscheduler - -o jobs.yaml
```

Two export quirks are handled for you rather than left to trip you up.
`schtasks /query /XML` **without** `ONE` writes one XML declaration per task
inside a single root, which is not well-formed XML; those declarations are
stripped. And the declaration routinely lies about the encoding, because
`Export-ScheduledTask` returns a string stamped `UTF-16` that PowerShell then
writes as UTF-8; the encoding is decided from the bytes instead.

## What comes out

The converted configuration goes to stdout, or to `-o FILE`. A report of
everything that could not be carried across goes to stderr, so the two can be
separated:

```shell
cronstable import-taskscheduler tasks.xml -o jobs.yaml 2> report.txt
cronstable -v -c jobs.yaml
```

| Flag | Meaning |
| --- | --- |
| `-o`, `--output FILE` | Write the configuration here instead of stdout. |
| `--timezone NAME` | Evaluate every converted schedule in this IANA zone. |

Exit `0` when something was converted, `1` when the input could not be read
or nothing usable came out, `2` for a usage error. There is deliberately no
separate code for a partial conversion, because on a whole-machine export a
partial conversion is the normal outcome.

## Expect most of a whole-machine export not to convert

This is the part worth knowing before the first run. Measured on a stock
Windows 11 machine, of 195 registered tasks:

- **111** act through a COM handler rather than a command line;
- **97** use `WnfStateChangeTrigger`, an internal Windows notification;
- **57** have no trigger at all, and are launched on demand or by another
  task;
- **30** fire on logon, **12** on an event-log event, **11** on a session
  change.

24 converted. That is not the tool failing; almost all of those tasks are
Windows' own internal plumbing, which has no business in a cron scheduler.
On a folder of tasks somebody actually wrote, the ratio is very different.
Every task that does not convert gets a line in the report saying which
element stopped it and, where one exists, what to do instead.

## What converts

| Task Scheduler | cronstable |
| --- | --- |
| `TimeTrigger` | a one-shot `M H D Mo * YYYY` schedule |
| `TimeTrigger` with `Repetition` | the repetition's minute and hour, with the date columns cleared |
| `CalendarTrigger` / `ScheduleByDay`, interval 1 | `M H * * *` |
| ...interval 7 | `M H * * <weekday>`, exact, because seven divides the week |
| `ScheduleByWeek`, interval 1 | `M H * * mon,fri` |
| `ScheduleByMonth` | `M H <days> <months> *`, with `Last` becoming `L` |
| `ScheduleByMonthDayOfWeek` | `M H * <months> tue#2`, with `Last` becoming `L2` |
| `BootTrigger` | `@reboot` |
| `Exec` action | a list-form `command`, split by the Windows argument rules |
| `WorkingDirectory` | [`workingDirectory`](Commands-and-Environment#workingdirectory) |
| `ExecutionTimeLimit` | [`executionTimeout`](Concurrency-and-Timeouts), in seconds |
| `MultipleInstancesPolicy` | `concurrencyPolicy`: `IgnoreNew` to `Forbid`, `Parallel` to `Allow`, `StopExisting` to `Replace` |
| `Priority` | [`priority`](Running-on-Windows#process-priority), by the documented band |
| `Enabled` false, on the task or a trigger | `enabled: false` |

A task with more than one trigger becomes one job per trigger, suffixed
`-t2`, `-t3`.

## What does not, and why

Nothing is dropped silently. Everything below is a line in the report,
naming the element responsible.

Six trigger types have no cron equivalent at all: logon, idle, event-log,
session change, registration, and the internal notification trigger.

Three kinds of calendar schedule cannot be written as a cron expression
either. A day interval other than 1 or 7 is one, because a cron day-of-month
step restarts each month: "every 3 days" would fire on the 1st, the 4th and
so on to the 31st, then the 1st again, leaving a one-day gap. Week intervals
above 1 fail for a related reason, since cron has no week-of-year phase. In
both cases you can run the job daily or weekly and gate it on a durable
[`cronstable cursor`](CLI-Reference#cursor-getadvance-etl-watermark).
Repetitions are the third: a cron minute field and hour field multiply, so a
repetition converts only when its occurrences over a day are exactly that
product. `PT1H` becomes `0 * * * *`, while `PT90M` does not, because the
product would include times it never fires at, and widening it would double
the job's rate. A repetition with a bounded `Duration` is refused on the
same grounds.

Actions cronstable cannot run are reported too. A COM handler, an e-mail and
a message box have no command line between them. A task with more than one
action does convert, but its jobs are written out commented, because Task
Scheduler runs a task's actions in sequence inside one instance while
separate cronstable jobs on one schedule run at once. Chain them as a
[DAG](Orchestration-and-DAGs) if the order matters.

`UserId`, `GroupId` and `RunLevel` are reported and never written as `user:`
or `group:`. Those keys are a config-load error on Windows, so emitting them
would produce a file that cannot load on the platform it was converted for.
cronstable runs every job as the account the daemon runs as.

What is left is settings with no counterpart. cronstable schedules on time
and does not test machine state, so run only if idle, only on AC power, only
when a network is available and wake to run are all reported;
`RestartOnFailure` is the one of those with a near equivalent worth naming,
[`onFailure.retry`](Failure-Detection-and-Retries). `RandomDelay` has none.
[Hashed schedules](Hashed-Schedules) spread jobs deterministically, which is
usually what the delay was for, but it is not the same thing. Nor does
`EndBoundary`, since a cronstable schedule has no end date.

## Details worth knowing

A command line containing `%SystemRoot%` or any other `%VAR%` is emitted as a
single string rather than as a `command` list. Task Scheduler expands
environment variables itself before it launches anything; cronstable does not,
and a list is passed to the OS as an argv, so `%windir%\system32\foo.exe`
would be looked up as a file with a percent sign in its name and fail on every
run. A string `command` with no `shell` set runs through `%ComSpec% /c` on
Windows, which performs the same expansion, at run time on the target machine
rather than baking this machine's values in. On the 195-task export above, 18
of the 31 emitted jobs took this form, and the report notes each one.

The program is quoted on the way out even when the original was not. Task
Scheduler decides quoting on the literal text it stored, and it calls
`CreateProcess` rather than a shell, so a path that only gains its space once
the variable expands is stored bare. `%ProgramFiles%\App\app.exe` handed to
`cmd.exe` unquoted runs `C:\Program`.

Two things change with the shell in the middle. `&`, `|`, `<`, `>` and `^` in
the arguments were text to Task Scheduler and are operators to `cmd.exe`, so
check any job whose arguments contain them. And the job runs `cmd.exe` as its
own process with the real program underneath it, which is worth knowing if you
also set [`priority`](Configuration-Reference#process-priority): Windows only
propagates a lowered priority class to children.

Three labels appear in the report. `NOT CONVERTED` means no job was emitted
for that task, or that its jobs were written commented out. `PARTIAL` means
jobs were emitted and are live in the file, but something else about the task
did not carry across, usually one trigger of several. `note` means the jobs
are complete and something is worth reading anyway.

One task that cannot be converted does not stop the others. A duration or
timestamp the converter cannot read becomes a blocking note for that task, so
the rest of the export still converts and still gets written.

Seconds are always dropped. A `StartBoundary` carries a seconds field and a
registration artefact like `:38` is common, but a cronstable schedule with a
seconds column makes the whole daemon wake every second, for every other job
on the box as well. That is an expensive thing to import by accident. Add
the column by hand to the one job that genuinely needs it.

`ExecutionTimeLimit: PT0S` means no limit in Task Scheduler, while cronstable
requires `executionTimeout` to be greater than zero, so it is emitted as
nothing rather than as zero. Mapping it literally would have made 34 of those
195 tasks fail to load.

Task Scheduler means machine local time unless the boundary says otherwise,
so a naive start time becomes `utc: false`, and one already in UTC emits
nothing. A stored numeric offset such as `-04:00` is reported rather than
converted: it is not an IANA zone name, so it cannot be written as
`timezone:`, and inferring one would be a guess about daylight saving. Name
the real zone with `--timezone` if you want one.

A one-shot whose instant has passed still loads. cronstable reports it as
`never-fires` on `/status` and `/jobs` rather than refusing it, so a lapsed
`TimeTrigger` shows up as a job to delete instead of as a load failure.

A task's URI becomes the job name, with the folder separator becoming `.` and
spaces becoming `-`, so `\Contoso\Nightly Backup` becomes
`Contoso.Nightly-Backup`, and a collision gets a numeric suffix. One
consequence is worth knowing: a `<folder>.<task>` name looks exactly like a
DAG task name, so if you later add a DAG named after a Task Scheduler folder,
the load refuses the pair.

A directory scan skips what it does not recognise. `.xml` is a name half the
tooling on a Windows box writes, so a stray file in a scanned directory is a
report line and a skip. A file you name on the command line is not: that is a
mistake worth stopping for.

## Related pages

- [Running on Windows](Running-on-Windows)
- [Classic Crontabs](Classic-Crontabs) for the other migration path
- [Schedules and Timezones](Schedules-and-Timezones)
- [Configuration Reference](Configuration-Reference)
- [CLI Reference](CLI-Reference)
