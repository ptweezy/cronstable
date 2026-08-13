# Windows Event Log

The `eventlog` reporter writes each job outcome to the Windows Event Log,
where a Windows shop's monitoring already looks: Event Viewer, a Windows
Event Forwarding subscription, SCOM, and every SIEM connector that ingests
Windows events. It is the sixth reporter, sitting beside `sentry`, `mail`,
`shell`, `webhook` and `push` in the same `report` block, so it fires from
the same hooks and on the same outcomes as the rest. See
[Reporting](Reporting) for the hooks and the block's shared shape, and
[Running on Windows](Running-on-Windows) for the rest of the platform's
behavior.

It is Windows only. On any other platform it does nothing, and the
configuration load says so once at startup rather than dropping reports
silently.

## Enabling it

```yaml
jobs:
  - name: nightly-backup
    command: backup.cmd
    schedule: "0 3 * * *"
    onFailure:
      report:
        eventlog:
          enabled: true
    onSuccess:
      report:
        eventlog:
          enabled: true
```

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Write records for this hook. |
| `source` | string | `cronstable` | The event source name records are written under. |
| `includeOutput` | bool | `false` | Carry a bounded tail of the run's captured output as the last insertion string. |

`source` must be a plain name: an empty value, one containing `\` or `/`,
and the three log names `Application`, `System` and `Security` are refused
at config load, because a source names a key underneath a log rather than a
log or a path. The check applies only to blocks that are enabled, since
`source` carries its default into every report block of every hook.

## What it writes

### Event IDs

These are a contract. A SIEM rule, an Event Viewer custom view and a
forwarding subscription all key on the number, so a shipped ID keeps its
meaning.

| Event ID | Level | Category | Fires on |
| --- | --- | --- | --- |
| 1000 | Information | 1 | a run succeeded (`onSuccess`) |
| 1001 | Error | 1 | a run failed (`onFailure`) |
| 1002 | Error | 1 | a run failed for the last time (`onPermanentFailure`) |
| 1003 | Warning | 1 | an SLA threshold was breached (`onLate`) |
| 1010 | Information | 2 | a daemon or orchestration event (`notify:`) |
| 1011 | Error | 2 | a daemon or orchestration event reporting failure |

Jobs occupy a contiguous 1000 to 1003 band so one rule can express "anything
that happened to a job", and daemon events start a fresh decade at 1010. An
overdue job is a Warning rather than an Error on purpose: it has not failed,
and a shop paging on `Level=2` in the Application log should not be paged by
a threshold it set to be advisory.

The IDs are plain small integers with no severity or customer bits folded
in. Event Viewer and the modern EventLog API report an ID masked to its low
16 bits, so a number carrying `0x20000000` would be documented as one value
and displayed as another. Severity travels in the event Level, which is
where every consumer already reads it.

### Insertion strings

Every record carries the same eleven insertion strings in the same order.
Position is the contract: without a registered message DLL there is no
message table to name the fields, and a forwarder ships them as `<Data>`
elements in order. A field that does not apply to an outcome is an empty
string rather than absent, so the arity never changes.

| # | Field | Contents |
| --- | --- | --- |
| 0 | `summary` | the one line a human reads in the Event Viewer list |
| 1 | `name` | the job name (a DAG task's is `<dag>.<taskId>`) |
| 2 | `outcome` | `success`, `failure`, `permanent-failure`, `late`, `event` or `event-alert` |
| 3 | `host` | the reporting host |
| 4 | `exitCode` | the run's exit code |
| 5 | `failReason` | why cronstable judged the run failed |
| 6 | `runId` | the durable run ID, when a `state:` store is configured |
| 7 | `startedAt` | ISO 8601 start instant |
| 8 | `schedule` | the job's crontab line |
| 9 | `detail` | per-outcome extras (SLA numbers, the event name, resource usage) |
| 10 | `output` | a bounded output tail, when `includeOutput` is on |

Field 2 repeats the outcome as text so a consumer that only rule-matches
event ID 1001 can still tell a failure from a permanent failure.

Fields are capped so that a record can never exceed what the API accepts.
Everything except `output` is capped at 1024 characters and `output` at
8000, and a field cut short ends with `...[truncated]`. The cap is
arithmetic rather than a runtime trim for a reason worth knowing: when the
combined vector is too long `ReportEventW` writes no record at all, so an
uncapped reporter would drop precisely the alerts for the jobs that produced
the most output.

## The "description not found" preamble

cronstable does not register its event source, so Event Viewer has no
message table to render and shows each record with a preamble like:

```text
The description for Event ID 1001 from source cronstable cannot be found.
Either the component that raises this event is not installed on your local
computer or the installation is corrupted. You can install or repair the
component on the local computer.
```

followed by the insertion strings themselves. That preamble costs the
rendered prose and nothing else. The provider name, the event ID, the level,
the timestamp and every insertion string are all present and unchanged, so
the XML view, `wevtutil`, `Get-WinEvent`, a Windows Event Forwarding
subscription and every SIEM connector read the record exactly as they would
a registered one.

Registering a source is an HKLM write, which needs Administrator, and it
buys nothing without a message DLL to point `EventMessageFile` at. Building
one means an `mc.exe` and `rc.exe` step per architecture, shipping a second
binary beside the executable, and the code-signing question that binary
would raise. cronstable therefore writes as an unregistered source and
documents the preamble rather than asking every install for Administrator
to remove a cosmetic line.

A shop that has its own message DLL can register the source itself; the
reporter writes the same records either way:

```shell
reg add "HKLM\SYSTEM\CurrentControlSet\Services\EventLog\Application\cronstable" /v EventMessageFile /t REG_EXPAND_SZ /d "C:\path\to\your-messages.dll" /f
reg add "HKLM\SYSTEM\CurrentControlSet\Services\EventLog\Application\cronstable" /v TypesSupported /t REG_DWORD /d 7 /f
```

This is optional and unsupported: the message file is yours, and its message
IDs have to match the table above.

## Reading the records

An unregistered source writes into the Application log. To read only
cronstable's records:

```powershell
Get-WinEvent -FilterHashtable @{ LogName = 'Application'; ProviderName = 'cronstable' } -MaxEvents 20
```

```shell
wevtutil qe Application /q:"*[System[Provider[@Name='cronstable']]]" /f:text /c:20 /rd:true
```

For only the failures, filter on the event IDs:

```powershell
Get-WinEvent -FilterHashtable @{ LogName = 'Application'; ProviderName = 'cronstable'; ID = 1001, 1002 }
```

The same XPath works as an Event Viewer custom view and as the `<Select>`
clause of a Windows Event Forwarding subscription.

## Things to know before turning it on

**The Application log is readable by every local account.** That is why
`includeOutput` is off by default, and it is the opposite default from
push's `includeLogTail`: a push payload is sealed to a paired device's key,
while anything written here is visible to anyone who can open Event Viewer
on the box. Turning `includeOutput` on is a decision to publish that job's
output locally.

**Queued records are not durable.** Writes are handed to a background
thread so a busy Event Log service can never delay a job's completion
handling, and a graceful shutdown drains that queue with a five second
bound. A hard kill drops whatever had not yet reached the service.

**It complements a log file, it does not replace one.** The `logging:`
section captures the daemon's own log (see
[Logging Configuration](Logging-Configuration)); this reporter publishes
per-run outcomes. An unattended deployment usually wants both. The standard
library's `logging.handlers.NTEventLogHandler` is not an alternative, since
it needs pywin32, which the frozen executable does not carry.

**On POSIX it does nothing**, and the load says so once, naming every hook
that enabled it:

```text
report.eventlog is enabled (job nightly-backup) but there is no Windows Event Log on this platform, so those reports are dropped; the rest of each report block still fires normally
```

A warning rather than a refusal, so one configuration directory can serve a
mixed fleet. There is nothing an operator could install on Linux to satisfy
a refusal, which is what separates this from the `user`/`group` rejection on
Windows.

## Related pages

- [Reporting](Reporting) for the hooks, the shared `report` block and the
  other five reporters
- [Running on Windows](Running-on-Windows) for the rest of the platform's
  behavior
- [Late Run Detection](Late-Run-Detection) for the `sla:` thresholds behind
  event 1003
- [Logging Configuration](Logging-Configuration) for the daemon's own log
- [Configuration Reference](Configuration-Reference) for the full schema
