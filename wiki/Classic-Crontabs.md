# Classic crontabs

cronstable reads classic (Vixie-style) crontabs in the
`m h dom mon dow command` format described by `man 5 crontab`. Pass a
crontab to `-c`, place it beside YAML files in a config directory, or load
it with `include:`.

**Entries use cronstable's defaults**: UTC schedules, stderr and exit-status
failure detection, `concurrencyPolicy: Allow`, and no retries. Cron's
environment and mail behavior are not emulated.

See [deviations from cron](#deviations-from-cron) before migrating an
existing crontab. The loader is implemented in `cronstable/crontabs.py`
and `cronstable/config.py`.

## How a crontab is recognised

The file *name* decides whenever it can:

| Name | Treated as |
| --- | --- |
| `*.crontab`, `*.cron` (case-insensitive) | classic crontab |
| a file named exactly `crontab` (case-insensitive), for example, a `crontab -l > crontab` export | classic crontab |
| `*.yml`, `*.yaml` | YAML, always; never content-sniffed |
| anything else, passed explicitly with `-c` or pulled in with `include:` | content-sniffed (described later) |
| anything else, inside a config directory | skipped, as before |

Name recognition also fires on `/etc/crontab`, but *system* crontabs
(`/etc/crontab`, `/etc/cron.d`) carry a sixth user column that cronstable
does not parse. Only the five-field *user*-crontab format runs as-is (see
[deviations](#deviations-from-cron)).

In a config directory, crontab-named files load right alongside
`*.yml`/`*.yaml` files, in the same name-sorted order. The usual skip rule
still applies: entries whose name starts with `_` or `.` are ignored (see
[includes, defaults, and multi-file config](Includes-and-Defaults)).

The content sniff exists so that `cronstable -c /var/spool/cron/crontabs/root`
works even though the file has no telling name. It looks at the first
meaningful (non-blank, non-comment) line only, and accepts only shapes no
valid cronstable YAML document can open with: a `NAME=value` assignment, a
line starting with `@`, or five valid cron fields followed by a command.
Anything inconclusive is parsed as YAML, so extensionless YAML configs keep
their exact pre-existing behavior. When in doubt, name the file `*.crontab`
and the question never arises.

A YAML configuration can also pull a crontab in directly:

```yaml
include:
  - legacy.crontab
```

`cronstable -v -c legacy.crontab` validates a crontab the same way it validates
YAML. Parse errors are reported with the offending `file:line`. A runnable
example mixing a crontab with a YAML file (and the web dashboard) ships in
the repository as `example/crontab`.

## Accepted syntax

The user-crontab format from `man 5 crontab`:

```crontab
# comments and blank lines are ignored
SHELL=/bin/bash
PATH = /usr/local/bin:/usr/bin:/bin
MAILTO="ops@example.com"

# m h dom mon dow command
*/15 * * * *  /usr/local/bin/backup --incremental
30 4 * * mon-fri  /usr/local/bin/report --daily
0 0 1 jan *  /usr/local/bin/happy-new-year

CRON_TZ=Europe/Berlin
0 6 * * *  echo "6am in Berlin, not UTC"

@daily  /usr/local/bin/rotate-logs
@reboot  echo "cronstable started"
0 0 * * *  pg_dump mydb > /backup/mydb-$(date +\%F).sql
```

Specifically:

- **Entries:** five time fields, then the rest of the line is the command.
  Ranges (`1-5`), steps (`*/5`), lists (`1,15,30`), and month/weekday names
  (`jan`, `mon-fri`) are supported. Day-of-week accepts both `0` and `7` as
  Sunday. The field dialect is the same built-in cron engine that parses
  YAML `schedule` strings, so both formats accept identical expressions
  (see [schedules and time zones](Schedules-and-Timezones)).
- **Nicknames:** `@reboot`, `@yearly`, `@annually`, `@monthly`, `@weekly`,
  `@daily`, `@midnight`, `@hourly`. `@midnight` is rewritten to its synonym
  `@daily` at load time. `@reboot` behaves exactly like a YAML `@reboot`
  schedule: it runs once at startup, and it understands leadership under
  [clustering](Clustering-and-Leader-Election).
- **Environment assignments:** `NAME = value` lines apply to the entries
  *below* them, exactly as in cron. A later reassignment affects later
  entries only. Values may be single- or double-quoted to preserve leading
  or trailing blanks. All assignments are exported to the job's
  environment, on top of the environment cronstable itself runs with.
- **Escaped percent signs:** `\%` in a command becomes a literal `%`, so the
  ubiquitous `date +\%F` idiom works unchanged. An *unescaped* `%` is a
  load-time error; see [deviations](#deviations-from-cron).

Two assignments are interpreted as well as exported:

| Variable | Effect |
| --- | --- |
| `SHELL` | Sets the job's `shell` option, so the command runs as `$SHELL -c "command"`, as in cron. Without it, cronstable's standard default applies (`/bin/sh` on POSIX, the native command processor on Windows). On Windows, a `SHELL` naming an absolute POSIX path (`SHELL=/bin/sh`, as `/etc/crontab` exports always carry) is a load-time error at the assignment's line, because that shell cannot exist there and every entry below it would fail at spawn. A bare name (`SHELL=powershell`) is kept. A POSIX-style `PATH=` assignment is kept but warned about on Windows, because it replaces the Windows `PATH` for the entries below it. See [running on Windows](Running-on-Windows). |
| `CRON_TZ` | Sets the job's `timezone` option: schedules below it are evaluated in that IANA zone (cronie's `CRON_TZ` semantics). An unknown zone is a load-time error at the assignment's line. |

## What each entry becomes

Every entry is lowered to a plain job definition, merged over the same
built-in defaults as a YAML job (`DEFAULT_CONFIG`), and validated by the
same `JobConfig` code path. From that point on, nothing downstream can tell
the two formats apart: crontab jobs appear in the
[web dashboard and HTTP API](HTTP-API), participate in the
[job-set fingerprint](Configuration-Reference) and
[clustering](Clustering-and-Leader-Election), and report failures like any
other job.

The following defaults matter most for a migrated crontab. Each row names a
behavior (with the per-job YAML option behind it), what the entry does now
that cronstable runs it, and what the same line did under classic cron:

| Behavior | Under cronstable | Under classic cron |
| --- | --- | --- |
| time basis (`utc` / `timezone`) | **UTC** (set `CRON_TZ` to change) | local time |
| failure detection (`failsWhen`) | non-zero exit **or any stderr output** is a failure | exit status ignored; output mailed |
| output (`captureStderr` / `captureStdout`) | stderr is read by cronstable (for failure detection, reports, and the dashboard log tail) and re-emitted into its log with a `[<job> stderr]` prefix. By contrast, stdout is not read: it flows straight through to cronstable's own stdout, visible there but not to reports or the dashboard | both mailed to `MAILTO` |
| concurrency (`concurrencyPolicy`) | `Allow` (overlapping runs permitted) | overlapping runs permitted |
| retries (`onFailure.retry`) | none | none |
| user (`user`) | the user cronstable runs as | the crontab's owner |

For reporting, retries, timeouts, or other per-job options, move the entry
to YAML and use the [configuration reference](Configuration-Reference).
A `defaults:` section in a sibling or including YAML file does **not**
apply to crontab entries; defaults are scoped to their file (see
[includes, defaults, and multi-file config](Includes-and-Defaults)).

### Job names

Entries are named `<file name>:<line number>`, for example
`legacy.crontab:9`. The name is unique within a file and stable across
reloads while the file is unchanged. It appears in logs, the dashboard, and
the HTTP API like any other job name, and points you straight at the source
line. Inserting or removing lines renumbers the entries below the edit, which
cronstable treats the same way as renaming a YAML job: the old name's run
history ends and the new name starts fresh.

## Deviations from cron

Check these differences when migrating:

- **Schedules default to UTC, not local time.** Put `CRON_TZ=<zone>`
  above the entries that need a specific zone.
- **`MAILTO` does not send mail.** It is exported to the job's environment
  but not interpreted. Configure [reporting](Reporting) in YAML to receive
  email. Failures remain visible in logs, the dashboard, and the HTTP API.
  stderr is captured; stdout passes through to cronstable's stdout
  (see the preceding table).
- **An unescaped `%` is a load-time error, not stdin.** In cron, `%` ends
  the command, and everything after it is fed to the command as standard
  input. cronstable does not support that syntax. The escaped form `\%`
  (such as `date +\%F`) works as in cron. To supply stdin, use a YAML job
  with a heredoc or file redirect.
- **The system-crontab user column is not parsed.** `/etc/crontab` and
  `/etc/cron.d` files carry a sixth field naming the user to run as. A
  parser cannot reliably tell that column from the first word of a command,
  so cronstable reads the five-field user-crontab format only. A user column
  would land at the start of the command and typically fail with
  `root: command not found` at run time. Remove the column, or move the
  entry to YAML and use the `user:` option
  ([commands and environment](Commands-and-Environment)).
- **Cron's implicit environment is not injected.** cron gives jobs a
  near-empty environment with `LOGNAME`, `HOME`, and `SHELL=/bin/sh`
  defaults. Under cronstable, jobs inherit cronstable's own environment plus
  the crontab's assignments, the same rule as YAML jobs. A crontab that
  relied on cron's minimal `PATH` behaves the same after it sets `PATH=`
  itself, as most already do.

## Migrating to YAML

When an entry outgrows the crontab format, its YAML equivalent is mechanical.
This entry:

```crontab
CRON_TZ=Europe/Berlin
SHELL=/bin/bash
30 4 * * mon-fri  /usr/local/bin/report --daily
```

is exactly:

```yaml
jobs:
  - name: report
    command: /usr/local/bin/report --daily
    shell: /bin/bash
    schedule: "30 4 * * mon-fri"
    timezone: Europe/Berlin
```

plus whatever per-job options prompted the move. Both forms can coexist in
one config directory for as long as the migration takes.
