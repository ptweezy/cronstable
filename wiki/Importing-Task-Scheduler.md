# Importing from Task Scheduler

`cronstable import-taskscheduler` converts Windows Task Scheduler XML
exports into cronstable jobs, so an existing estate does not have to be
retyped.

It is a one-shot converter, not a loader. It writes YAML you read, edit and
commit; cronstable never reads Task Scheduler XML at run time. That is
deliberate, and the first reason is the one that matters most: **exporting a
task does not unregister it**, so an export describes tasks Task Scheduler is
still running. Loading the output without reviewing it first means both
schedulers run the same work.

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

Every one of these is a line in the report, never a silent drop.

**Triggers that are not schedules.** Logon, idle, event-log, session change,
registration and the internal notification trigger have no cron equivalent.

**Day intervals other than 1 or 7.** A cron day-of-month step restarts each
month, so "every 3 days" would fire on the 1st, 4th and so on to the 31st,
then the 1st again, a one-day gap. Run it daily and gate it on a durable
[`cronstable cursor`](CLI-Reference#cursor-getadvance-etl-watermark) instead.
Week intervals above 1 are refused for the same kind of reason: cron has no
week-of-year phase.

**Repetitions that are not one cron expression.** A cron minute field and
hour field multiply, so a repetition converts only when its occurrences over
a day are exactly that product. `PT1H` converts to `0 * * * *`; `PT90M` does
not, because its minute-by-hour product would include times it never fires
at, and widening it would double the job's rate. A repetition with a bounded
`Duration` is refused on the same grounds.

**Tasks with more than one action.** Task Scheduler runs a task's actions in
sequence inside one instance, while separate cronstable jobs on one schedule
run at once. The jobs are still written out, commented, so the command lines
reach your editor; chain them as a [DAG](Orchestration-and-DAGs) if the order
matters.

**COM handler, e-mail and message-box actions.** cronstable runs a command
line, and those have none.

**Principals.** `UserId`, `GroupId` and `RunLevel` are reported and never
emitted as `user:` or `group:`, because those keys are a config-load error on
Windows, so writing them would produce a file that cannot load on the
platform it was converted for. cronstable runs every job as the account the
daemon runs as.

**Machine conditions.** Run only if idle, only on AC power, only when a
network is available, wake to run, and the rest are reported. cronstable
schedules on time and does not test machine state. `RestartOnFailure` has a
near equivalent worth naming:
[`onFailure.retry`](Failure-Detection-and-Retries).

**`RandomDelay`.** cronstable does not delay a fire randomly.
[Hashed schedules](Hashed-Schedules) spread jobs deterministically, which is
usually what the delay was for, but it is not the same thing.

**`EndBoundary`.** cronstable schedules have no end date.

## Details worth knowing

**Seconds are dropped.** A `StartBoundary` carries a seconds field, and a
registration artefact like `:38` is common. A cronstable schedule with a
seconds column makes the **whole daemon** wake every second, for every other
job too, so an imported second would be an expensive accident. Add the column
by hand to the one job that genuinely needs it.

**`ExecutionTimeLimit` of `PT0S` means no limit** in Task Scheduler, while
cronstable requires `executionTimeout` to be greater than zero. It is
therefore emitted as nothing rather than as zero, which would have made 34 of
those 195 tasks fail to load.

**Clocks.** Task Scheduler means machine local time unless the boundary says
otherwise, so a naive start time becomes `utc: false`. A start time in UTC
emits nothing, since that is already the default. A stored numeric offset
such as `-04:00` is reported rather than converted: it is not an IANA zone
name, so it cannot be written as `timezone:`, and inferring one would be a
guess about daylight saving. Use `--timezone` to name the real zone.

**A one-shot in the past still loads.** cronstable reports it as
`never-fires` on `/status` and `/jobs` rather than refusing it, so a lapsed
`TimeTrigger` shows up as a job to delete rather than as a load failure.

**Names.** A task's URI becomes the job name, with the folder separator
becoming `.` and spaces becoming `-`, so `\Contoso\Nightly Backup` becomes
`Contoso.Nightly-Backup`. Collisions get a numeric suffix. Note that a
`<folder>.<task>` name looks exactly like a DAG task name, so if you later
add a DAG named after a Task Scheduler folder the load will refuse the pair.

**A directory scan skips what it does not recognise.** `.xml` is a name half
the tooling on a Windows box writes, so a stray file in a scanned directory
is a report line and a skip. A file you name on the command line is not: that
is a mistake worth stopping for.

## Related pages

- [Running on Windows](Running-on-Windows)
- [Classic Crontabs](Classic-Crontabs) for the other migration path
- [Schedules and Timezones](Schedules-and-Timezones)
- [Configuration Reference](Configuration-Reference)
- [CLI Reference](CLI-Reference)
