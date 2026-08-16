# Windows: what this branch changed, and what is still open

Working document for the `windows` branch. Records why the branch exists, what
it delivered and with what evidence, and every piece of follow-up work the
review identified but this branch did not do.

Not a user-facing page. The user-facing documentation for everything below
lives in `wiki/Running-on-Windows.md` and its sibling pages.

## Why the branch exists

A gap review on 2026-08-05 asked one question: what would stop cronstable
being marketed as "Cron, for Windows", given that Windows already ships Task
Scheduler? Eight dimensions were probed and each finding was adversarially
re-verified: 64 findings, 14 blockers, 26 majors.

The review's conclusion was that nothing structural stops the claim. What
stopped it was one large absence (no unattended-execution story) plus three
defects, two of which failed silently. Those defects were not inferred from
reading code; they were reproduced on a real Windows 11 box.

This branch fixes all three defects and roughly twenty other confirmed
findings, and corrects the Windows documentation everywhere it disagreed with
the code. The large absence is still open; see
"Ship a native Windows service host", the first entry under
[Filed as ready-to-pick-up tasks](#filed-as-ready-to-pick-up-tasks).

## What shipped

Branch commits (the rest of the branch is merged `performance-3` work):

* `3feb522` windows gap review fixes: graceful lifecycle, cmd /c, unattended story
* `e5a183e` order each job's run-ledger writes, as the other three streams already are

A second pass on 2026-08-09 and 2026-08-10 closed five more of the items below,
one commit each:

* `07c9949` add a per-job workingDirectory key (item 12)
* `f5dd164` add a per-job process priority key (item 11)
* `67747df` serve every API error as the JSON envelope (item 4)
* `91de749` fold the run-ledger readers by finish time (item 5)
* `a37561d` measure each OS against its own coverage profile (item 16)

A third pass on 2026-08-10 closed the three remaining FILED items, which were
the three largest:

* `0946c66` teach the onLate destination check about push (a defect found
  while reading for item 3, shipped on its own)
* `084bf1b` add a Windows Event Log reporter (item 3)
* `6acc877` run cronstable as a Windows service (item 1)
* `4465dd1` convert Task Scheduler exports into jobs (item 2)

A fourth pass on 2026-08-13 closed item 13, both halves:

* `3c6db5c` fix the recovery-actions access rights in service install (a
  1.2.39 defect found by the first elevated install this work ever ran
  against the real SCM, shipped on its own)
* `dbb3d52` ship one-directory Windows builds: a zip asset and an MSI
  (item 13)

Same method as the second pass, and it paid the same way: each item turned up
defects its own entry had not predicted, and in two cases the entry's premise
was wrong. The one that changes the shipping story is item 1's: a one-file
PyInstaller binary cannot host a Windows service at all, and that binary is
the release asset and the winget package.

Each was planned against the real code, adversarially reviewed, and gated on the
full suite before it landed. Three of the five turned up a defect their own
roadmap entry had not predicted: the Windows priority class is inherited by
grandchildren only when it is LOWERED (so `priority: high` on the common
`shell: cmd` shape reaches cmd.exe and not the program it runs); a bare aiohttp
error that is RETURNED rather than raised escapes the envelope middleware
entirely; and the crash-reconciliation path was installing a foreign run's start
instant as `last_run`, so a job whose newest run succeeded could report
`unknown`. Those three are the reason the pass took the shape it did, and each is
written up under its item.

### The three live defects

**`shell: cmd` reported success having run nothing.** `job.py` hardcoded the
POSIX `-c` flag for any non-empty shell. cmd.exe takes `/c`; handed `-c` it
starts an interactive shell, prints its banner, reads EOF on stdin and exits
0, so the job recorded a clean success forever and the dashboard showed solid
green. `cmd` is the single most natural value a Windows admin would type, and
`wiki/Running-on-Windows.md` claimed outright that "the `shell` field itself
works on every OS". Fixed with a per-shell spawn helper (`job.shell_spawn`,
which returns the spawn call, argv and kwargs together) applied to both the
job spawn and the shell reporter: cmd.exe goes through
`create_subprocess_shell`, so the command string reaches `%ComSpec% /c`
without an argv round trip that its own quoting rules would mangle. Verified
end to end: the command now runs, and `exit 7` propagates as 7.

**Ctrl-C killed every in-flight job and reported each one failed.** Jobs were
spawned with no `CREATE_NEW_PROCESS_GROUP`, so they sat in the daemon's
console process group and received the console control event themselves. The
daemon logged "Shutting down (after currently running jobs finish)..." and
then exited in 0.0 seconds, while each job died at `0xC000013A`
(`STATUS_CONTROL_C_EXIT`, retcode 3221225786) with a non-null `fail_reason`,
firing every configured reporter. Restarting to pick up a config change
produced an alert storm. Fixed by spawning jobs into their own process group.
Verified live against the real daemon: a 6 second job now completes,
`exit code 0`, `fail_reason: None`, daemon drains 5 seconds and exits 0.

**With no console there was no graceful stop at all.** Scheduling and
execution worked when detached, but the only shutdown surface was
`signal.signal` for SIGINT and SIGBREAK, both console-delivered, and there was
no HTTP shutdown route. A hard kill was the only option and it leaked orphaned
job trees. Fixed in three layers, below.

### Everything else

**Job termination is a real two step on Windows.** The graceful call now
delivers a trappable `CTRL_BREAK_EVENT` to the job's process group and the
forced call escalates to `taskkill /F /T` after `killTimeout`, so `killTimeout`
means on Windows what it means on POSIX. Where no console is shared (a service
context) the graceful step degrades to the immediate tree kill, which was the
prior behavior of every call. Verified live with a child that trapped the
break and chose its own exit code, and with a child that ignored it and got
escalated after exactly `killTimeout`.

**Console close, logoff and OS shutdown drain gracefully.** Python's signal
module never surfaces those events, so a native `SetConsoleCtrlHandler` was
added. Bounded by the few seconds of grace Windows grants before it terminates
the process regardless. This also corrects a false claim in the code comments
and the wiki, both of which said SIGBREAK covered console close.

**`POST /shutdown`.** An authenticated route running the same graceful drain
as Ctrl-C and SIGTERM, for supervised and console-less deployments. Refused
with 403 unless the request carries a configured bearer token, even where the
rest of the API is unauthenticated: an open listener must not hand every local
process a stop switch for the scheduler.

**Machine-wide config default.** `%ProgramData%\cronstable` is the real Windows
analog of `/etc/cronstable.d`, and it now wins whenever that directory exists,
falling back to the historical per-user `%APPDATA%\cronstable` otherwise. This
is the wall every unattended deployment hit: a service account's `%APPDATA%`
points into an invisible `systemprofile` directory, so a config that worked
interactively was not found when the same command ran as a service.

**`cronstable init`, and a config-not-found error that names the path.** The
first run of a new install used to end in an error that did not say which
directory it had looked in. It now names the resolved path and suggests
`cronstable init`, which writes a commented starter config into the platform
default (or a directory you name), refusing to touch a directory that already
holds configuration.

**Output decoding.** Captured job output decodes as strict UTF-8 with a
Windows OEM code-page retry, so `dir` output and localized OS messages keep
their accents instead of collapsing to replacement characters. The passthrough
mirror now writes the daemon's own stream encoding rather than hardcoded
UTF-8, which was mojibake in a redirected log.

**Classic crontab guard.** A crontab assigning `SHELL=` an absolute POSIX path
(`/etc/crontab` always carries `SHELL=/bin/sh`) is refused at config load on
Windows with the assignment's own file:line, because every entry below it
would otherwise load cleanly and fail at spawn. This was the only migration
path the project advertised. A POSIX `PATH=` assignment warns, since it
replaces the Windows `PATH` for the entries below it.

**`env_file` BOM.** Files opened with `utf-8-sig`, so a BOM (Notepad, a
PowerShell `>` redirect) no longer rides into the first variable's name and
leaves the expected variable silently absent.

**Distribution.** The Windows executables carry a VERSIONINFO resource, so
Properties > Details shows product, version and copyright instead of a blank
tab. The winget job's three failure suppressions were removed per its own
written removal plan, so a failed manifest update now fails the release
instead of warning on a job that could not fail.

**Documentation.** The job-termination table on three pages described the
pre-1.2.36 kill order; `killTimeout` was documented as bounding a wait that
did not exist; console close was claimed to reach SIGBREAK. All corrected.
`wiki/Running-on-Windows.md` gained a "Running unattended" section (a
`schtasks` recipe, a rotating log-file config, the `/shutdown` stop path, a
firewall rule), a SmartScreen note, the POSIX-file-modes advisory note with an
`icacls` recipe, and the Fast Startup caveat for `@reboot`.

**Per-job working directory (item 12).** A new `workingDirectory` job key
becomes the `cwd` of the job's spawn, the equivalent of "Start in (optional)"
on a Task Scheduler action, so a `.bat` or `.ps1` full of relative paths no
longer depends on where the daemon was launched from. It works in a
`defaults:` block and on a DAG task like every other launch field, and `~` and
`${VAR}` are expanded with the result made absolute at load. There is
deliberately no absolute-only gate: `ntpath.isabs` answers differently for the
same string across the supported interpreters, which is the lesson
`job.shell_spawn` already records, so gating on it would make a UNC share path
legal config on one Python and a load failure on another. There is no
existence check at load either, since load runs on hosts that are not the
target; the OS decides at spawn and a bad directory is an ordinary launch
failure whose log line names the `cwd`. The key is not part of the job-set id,
for the reason environment values are not.

**Per-job process priority (item 11, priority half).** A new `priority` job
key takes one of `idle | below-normal | normal | above-normal | high`, named
after the Windows priority classes because those are the fixed points: nice is
a continuum that maps onto any vocabulary, the classes are not. On Windows the
level is OR-ed into the creation flags beside `CREATE_NEW_PROCESS_GROUP`, so
it is set at `CreateProcess` (race-free). How far it then reaches is
asymmetric, measured on Windows 11: `idle` and `below-normal` are inherited
by descendants, `above-normal` and `high` are not, because `CreateProcess`
gives an unflagged child the creator's class only when the creator is idle or
below-normal and NORMAL otherwise. So a `shell: cmd` job at `high` is cmd.exe
at HIGH running its programs at NORMAL, and the docs say so rather than
promising tree-wide propagation. On
POSIX the job's process *group* is reniced to an absolute value right after
the spawn, not in a `preexec_fn` (that hook runs between fork and exec, where
only async-signal-safe calls are sound, and a test exists specifically to keep
the hook off jobs with no privilege to drop). `normal`, the default, is never
applied, which is what keeps the existing spawn byte for byte what it was on
both platforms, and it is why `_POSIX_NICE` has no `normal` row: nice 0 would
misdescribe it on a daemon started at nice 10. Raising on POSIX needs
`CAP_SYS_NICE` or `RLIMIT_NICE` headroom, so the config load warns once,
naming the job, and a refusal at run time is DEBUG and never fails the run; a
per-run WARNING would be ~1,440 lines a day on a minutely job for a condition
that does not change. `realtime` is unreachable on purpose. The level enters
the job-set id only when set (the `concurrencyScope` precedent), so no golden
digest moves, and `GET /jobs` carries it on the same rule.

Deliberately not done in this change: no dashboard chip (it drags in the
`docs/demo` regenerate and its verification, and the `/jobs` key is what other
tools consume); nothing added to `docs/openapi.yaml`, whose `Job` schema
documents none of its conditional siblings and carries
`additionalProperties: true` for that reason; and no `priority` label on the
Prometheus per-job info metric, since an info metric spells one label set for
every job, so adding it would mean `priority="normal"` on every existing
series. The last is recorded as a comment at the metric.

### One non-Windows fix that came along

While merging `performance-3` a test failed intermittently under full-suite
load. It turned out not to be a flake but a real ordering defect, and it
predated both this branch and the merge.

Each job's run-ledger writes were unchained, so two completions close enough
to overlap (a `concurrencyPolicy: Allow` pair, a retry firing straight after
its parent's failure, a catch-up burst, crash reconciliation racing a live
completion) could land filename-inverted in `runs/<job>`. The filename that
orders the stream is minted inside `append_record`, on whichever pooled worker
thread runs it, so the older run could end up newest: the record a restart
restores as `last_run`, and the one an at-the-bound prune keeps while deleting
the newer.

Measured against the real filesystem store, back-to-back `_record_run` pairs:
28 of 60 inverted before, 0 of 60 after. Fixed with a fourth per-job tail
(`_run_write_tail`) using the existing `_install_tail_task` idiom, matching the
guard the in-flight, retry-ladder and pause streams already applied to the
identical hazard.

### Verification

Full suite green on Windows throughout: 4,020 passed, 26 skipped, coverage
96.70% against the 92% floor, with ruff, mypy and the OpenAPI structural check
clean. The touched test files were additionally run on Linux under WSL.

Both new ordering tests are red-checked: they fail on the parent commit and
pass on the fix. The first version of the ordering test passed on the broken
tree, because the natural race only inverts about half the time, so it was
rewritten to delay the first append and make the inversion deterministic.

After the third pass: 4,660 passed, 29 skipped, 97% coverage against the same
floor, with ruff (check and format), mypy, bandit at medium and the OpenAPI
check clean. Three red-checks were run rather than assumed. The push clause in
the onLate destination check fails on its parent and passes on the fix. The
two golden fingerprint digests were confirmed RED after the eventlog block
joined the report defaults and green again once the omit-when-default rule
covered it, which proves the tripwire fires rather than merely existing. And
the service host's `shutdown_handlers` flag is red on its parent, where the
keyword does not exist.

Three surfaces were additionally exercised against the real thing rather than
against a double: `service status` against the live SCM (a missing service and
a service every Windows host has), the event-log leaf calls against the real
Application log including the size ceiling and the invalid-handle path, and
the whole importer against this machine's own 195-task export, whose output
was fed back through `cronstable -v` and loaded.

## What is still open

Items 1, 2, 3, 4, 5, 11, 12, 13 and 16 are DONE and are kept below in their
original numbered places, each rewritten to record what actually shipped and
where the original entry was wrong. They are left in position so the numbering
stays stable for anyone holding a reference to it. Genuinely open: 6 (the
Azure onboarding half; its CI half shipped 2026-08-13), 7, 8, 9, 10, 14, 15,
plus the resource-limits half of 11.

Nothing remains in "Filed as ready-to-pick-up tasks". What is left is either
blocked on an Azure onboarding only Parker can run (6, see
`windows-signing-runbook.md`) or unfiled and larger.

### Filed as ready-to-pick-up tasks

This section is now empty of open work; the three entries below all shipped
on 2026-08-10 and are kept in place so the numbering stays stable.

**1. Ship a native Windows service host.** DONE on this branch (`6acc877`).
`cronstable service install|remove|start|stop|status|run`, a ctypes shim over
advapi32 in a new `cronstable/winservice.py`, dispatched lazily like every
other heavyweight subcommand. The decision recorded here held up: no pywin32,
no WinSW, no second binary.

The entry was right about the shape and wrong, or silent, about five things.

The one that matters commercially: **a one-file frozen binary cannot host a
service at all**, and that binary is the release asset and the winget package.
Its bootloader unpacks itself and runs the application in a CHILD process, so
the process the SCM starts and watches never calls the dispatcher; the start
fails on the SCM timeout while the real program's own registration comes back
1063. Nothing in cronstable can change that, because the fork belongs to the
bootloader. `install` refuses that shape by name and points at pip, pipx or
the `schtasks` recipe. So the headline gap is closed for pip and pipx installs
and for a one-directory build, and NOT for the published .exe. Producing a
one-directory Windows artifact is the follow-up, and it is a packaging change
(release.yml, the winget manifests, the download page) rather than a code one,
which is why it is not in this commit. The same reasoning rules out the pip
console-script shim as an ImagePath: it launches the interpreter as a child
and waits, reproducing the problem exactly, so a source install is spelled
`python -m cronstable`.

The daemon's own shutdown wiring cannot run in a service.
`install_shutdown_handlers` reaches `signal.signal`, which the interpreter
refuses anywhere but the main thread, and ServiceMain runs on a thread the SCM
creates. `_run_daemon` grew a `shutdown_handlers` flag; the service's stop
surface is the control handler instead.

ctypes truncates a handle unless every prototype is declared. Its default
restype is `c_int`, so a 64-bit SC_HANDLE comes back cut in half. This cost a
real failure on the first error path exercised, where an undeclared
`CloseServiceHandle` raised OverflowError instead of printing a message.
Prototypes are declared in one place for that reason: one by one at the call
sites is how one gets missed.

A service has no stderr, and not harmlessly. Its standard streams are `None`,
so the default logging setup formats each record, fails to write it, and has
that failure swallowed because the fallback is itself guarded on stderr
existing. `service run` opens a rotating bootstrap log first and refuses to
start if it cannot.

The SCM does not require a service to start within a fixed time; it requires
the checkpoint to keep advancing. Both pending states get a pumper, so a slow
config parse is not a failed start and an hour-long drain is not a hung
service. The pumper is stopped AND joined before the terminal report, or a
late tick puts the service back into a pending state after STOPPED.

Two things the entry raised and this resolved. `AllocConsole` ships behind
`--console`, off by default, and the docs say what it costs rather than
claiming it is transparent: the std handles are detached so an uncaptured job
does not inherit a blocking stdin, but console MEMBERSHIP is inherited
separately and cannot be undone, which is the entire reason for allocating
one. And recovery actions are configured at install along with the flag that
makes them apply to an orderly nonzero exit, without which they would have
been decorative, since this host reports failures as a clean stop carrying a
service-specific code.

Deliberately not done: per-account identity (that is item 7), and any
`service:` YAML section. Everything the service needs is install-time SCM
registry metadata, and a config key would be a second source of truth for
values only `install` can apply, re-read every housekeeping minute while the
SCM reads its own copy once at start.

**2. Build a Task Scheduler XML importer.** DONE on this branch (`4465dd1`).
`cronstable import-taskscheduler`, a new `cronstable/taskxml.py`, OS
independent so an estate converts anywhere.

One deliberate departure from the entry: it is a CONVERTER, not a
config-directory front end. `.xml` is added nowhere and the module never runs
inside a config parse. Exporting a task does not unregister it, so an export
describes tasks Task Scheduler is still firing and a loader would silently
double-run an estate; `.xml` is also a name half the tooling on a Windows box
writes, and adding it to the loadable set would let one stray file decide
which directory becomes the Windows default config location.

Three things measured against this machine's 195 real tasks that no amount of
schema reading predicts.

`schtasks /query /XML`, the command this entry names, does not emit
well-formed XML: without `ONE` it writes one XML declaration per task inside a
single root and a parser stops at the second. They are stripped.

The encoding declaration lies whenever an export passed through a redirect.
`Export-ScheduledTask` returns a string stamped UTF-16 that PowerShell writes
as UTF-8, and expat honours the declaration only for byte input, so parsing
those bytes fails while parsing the same content as text succeeds. Only text
reaches the parser. A UTF-16 trial decode also succeeds on almost any
even-length byte string, so the result has to look like a document first.

Most of a whole-machine export does not convert, and the entry's trigger list
missed the two commonest classes: `WnfStateChangeTrigger` (97 of 195, in none
of the documented lists) and tasks with NO trigger at all (57). `ComHandler`
is the majority action type (111). 24 converted. The documentation leads with
those numbers so a first run does not read as a broken tool.

Corrections to the entry's mapping claims. `ExecutionTimeLimit` does NOT map
cleanly: `PT0S` means no limit in Task Scheduler while cronstable requires one
above zero, and it is `PT0S` in 34 of 195 tasks, so the literal mapping makes
17% of an estate a load failure. Repetition-with-duration is not the only
unconvertible repetition: one converts only when its occurrences over a day
are exactly the product of an hour set and a minute set, because those two
cron fields multiply, so `PT90M` is refused despite dividing 1440.
`ScheduleByMonthDayOfWeek` DOES convert, contrary to the assumption that only
daily, weekly and monthly do: the cron dialect has `d#n` and `L<n>`, so
"second Tuesday" and "last Tuesday" are exact.

Two hazards the entry did not raise. Seconds must always be dropped, because
the daemon's subminute decision is `any()` over its WHOLE job set, so one
imported registration artefact makes every other job on the box tick once a
second. And a principal must never be emitted as `user:`, which is a
config-load error on Windows and would produce a file that cannot load on the
platform it was converted for.

On the hardening the entry asked for: external entities need no disabling
(ElementTree installs no handler for them), and expat's amplification cap is
not ours to assert since a Linux build links the system expat. The mitigation
is refusing a DOCTYPE at the parse target, which is where every custom entity
must be declared. defusedxml's own spelling of that reaches an attribute
current CPython does not have. bandit's B314 is Medium and fires on both the
parser construction and the parse, so this carries the tree's first `# nosec`,
scoped to one call rather than waived in `pyproject.toml`.

**3. Add a Windows Event Log reporter.** DONE on this branch (`084bf1b`).
A sixth reporter beside sentry/mail/shell/webhook/push, ctypes over advapi32,
no pywin32, with the "description not found" preamble documented plainly as
the entry asked.

Four things the entry did not predict, all measured rather than read.

`RegisterEventSourceW` blocks too, not just `ReportEventW`. Both are RPC into
the EventLog service, and reports run inline on the completion path, so the
source is opened on the writer thread rather than lazily on first use.

There is a combined size ceiling, and overshooting it is not a soft failure.
The call is refused unless the sum of every insertion string's length plus its
terminator is at most 32,732 wide characters (measured at one, two, four and
eight strings, which is what shows the terminator is inside the budget), and a
refused call writes NO record. An uncapped reporter would therefore drop
exactly the alerts for the jobs that produced the most output, so fields are
capped by arithmetic that cannot reach the ceiling.

A handle the process never opened faults inside advapi32 and surfaces as
OSError, while a handle that was opened and has since been released reports
ERROR_INVALID_HANDLE properly. The first has to be swallowed or it takes the
writer thread down; the second is the real "the service restarted" case and is
repaired by re-registering and retrying once.

The writer registry is keyed on a config string, so a reload that renames
`source` would leak an OS thread and a source handle. It is retired from the
reload path and capped besides.

Two decisions the entry left open. The source is never registered: that needs
an HKLM write and buys nothing without a message DLL, which would mean an
mc.exe and rc.exe step per architecture and a second binary to sign. And on
POSIX the reporter warns once at load rather than refusing, because nothing
can be installed on Linux to satisfy a refusal, so one config directory still
serves a mixed fleet.

`includeOutput` is off by default, the opposite of push's `includeLogTail`,
because the Application log is readable by every local account while a push
payload is sealed to a device key.

While reading for this item a sibling defect turned up and shipped separately
(`0946c66`): the check deciding whether an `onLate` block would really fire
lists every reporter by hand and never learned about push, so an onLate
enabling only push skipped the "requires sla" rejection and loaded as a hook
that could never fire.

**4. Fix bare 404s that bypass the JSON error envelope.** DONE on this branch.
Ten sites, not six: `cron.py`'s six now raise through `_api_error`, and
`jobapi.py`'s four raise `JobStateError(..., status=404)`. Two corrections to
the premise above. First, `cron.py`'s six were already served as
`application/json`: `_error_envelope_middleware` is installed outermost and
rewraps anything escaping as text/plain, so what they actually served was
`{"error": "404: Not Found"}`, a body whose reason names nothing. The
`text/plain` symptom was real only on `jobapi.py`, whose `error_mw` re-raised
`web.HTTPException` unwrapped; that arm is now the local twin of cron's, so
its reasonless 401, the router's 404/405, the oversized-body 413 and (via a
new catch-all) an unexpected 500 all carry the envelope. Second, two of the
six resolve through `_job_or_dag_schedule`, not `cron_jobs`, so
`/schedule/why` and `/jobs/{name}/calendar.ics` answer
`no job or DAG schedule named ...`.

No `WEB_ROUTES` walk: that lockstep contract already has two owners
(`tests/test_openapi.py`, `tests/test_mcp_tools.py`) and a third would make
one added route fail three tests in three files. Instead
`tests/_helpers.py:bare_http_raises` AST-scans both modules for
`raise web.HTTP*`, allowlisting only `HTTPUnauthorized` (deliberately
reasonless, rationale written at all seven raise sites), with a parametrized
behavior test per module over the converted routes.

Transport-level 4xx are deliberately not chased: a malformed request line, an
unparseable method token and oversized headers are each a text/plain 400 from
aiohttp's own `RequestHandler.handle_error`, and an unrecognised `Expect:` a
text/plain 417, all before any middleware exists. The claim is scoped in
`wiki/HTTP-API.md`, `docs/openapi.yaml` and the 1.2.39 changelog to what the
application serves.

**5. Make ledger readers order-tolerant on shared mounts.** DONE on this
branch. All three named readers plus a fourth the item missed, and
`maxRunsPerJob: 1` answered with a floor rather than a warning.

`_run_finish_key` is the one place that says what "newest" means, and
everything folds through it. Reader (a), the rehydrated `last_run`, is fixed
at `_install_run_info` (the funnel the AST tripwire already forces every
`last_run` write through) by promoting only a row that is not older, which
makes it correct as rows stream in rather than only if the caller sorted
them; `_warm_one` additionally sorts the parsed rows by finish time so the
warmed ring itself is oldest first. Reader (b), the `_last_completed_at`
seed, became a `max` fold over the non-skipped rows instead of a backwards
walk off the end of the deque, mirroring the `_last_real_outcome` fold ten
lines above it. Reader (c) is `max(reversed(runs), key=_run_finish_key,
default=None)` computed once in `_run_stats` and shared by
`last_duration`, `last_cpu_seconds` and `last_rss_bytes`, so those three can
never describe two different runs. `reversed()` is load-bearing: `max`
returns the first maximum it walks, so walking backwards reproduces
`runs[-1]`'s equal-instant answer and keeps
`test_run_stats_cpu_and_memory_aggregates` (three rows sharing one instant)
green.

The fourth site: `_reconcile_open_record` installs a synthetic `unknown` row
whose instant is the interrupted run's START, and on a slot takeover that
run belongs to another node and can predate a run this node already
recorded. It was overwriting `last_run`, so the dashboard, `GET /jobs` and
`cronstable_job_last_run_*` all reported `unknown` for a job whose newest run
succeeded. It now loses to the newer row. The same guard exposed three
watermarks in `_record_run` (`_sla_last_success`, `_last_real_outcome`,
`_last_completed_at`) still assigning last-write-wins while their own
siblings elsewhere already fold with `max` or advance rather than assign;
they now advance too.

Whose crash it was decides that, and getting it wrong in the other direction
is the trap this site sets. Applying the fold to THIS host's own crashed run
is a regression, not a fix: the synthetic row is keyed on the interrupted
run's start, `concurrencyPolicy: Allow` is the default, and any job whose
runtime exceeds its interval routinely has an overlapping instance finish
after that instant, which would then suppress the `unknown` on `GET /jobs`,
the status tile, every `cronstable_job_last_run_*` gauge and the
`/jobs/{name}/logs` replay target. It would also contradict the argument the
same change makes for keeping the display tails positional (that sorting
files an interruption before the runs that outlived it, which is not where an
operator looks for the run that just died) while doing exactly that to the
one reader where a wrong answer is a wrong verdict. So `_install_run_info`
takes a `promote: Optional[bool] = None` parameter: `None` (every other
caller) folds by finish time, and the reconcile passes
`rec.get("host") == self._state_host`, which `_reconcile_one_inflight`
already computes. This host's own crashed run is the newest thing that
happened here whatever finished around it; a foreign node's row keeps taking
the fold and so keeps losing to a newer local run. The two directions are
pinned separately, by
`test_rehydrate_local_crash_outranks_the_runs_that_outlived_it` and
`test_rehydrate_foreign_crash_loses_to_a_newer_local_run`, which seed the
identical shape and differ only in the record's `host`.

`_record_run`'s ring release had to become symmetric in the same change,
which is a two-directional trap. Releasing `prev` unconditionally empties the
output ring of the run still serving log replay once the promotion is
conditional; releasing nothing when the promotion is declined leaks the
un-promoted row's 1000-line ring until it falls out of `RUN_HISTORY_LIMIT`,
the exact waste the release exists to prevent. It now releases whichever of
the pair is not `last_run`. Both wrong versions were built and both fail
`test_record_run_releases_the_ring_of_whichever_row_is_not_newest`, on
different assertions. `_reconcile_open_record` got the same release after its
own install, which it never had: on a RUNTIME takeover the outgoing
`last_run` is a real local run holding a full ring. The release stays at the
two call sites rather than being hoisted into `_install_run_info`, because
`_record_run` snapshots `archive_lines` AFTER the install and an early
release would silently empty every `archiveOutput` write on the
declined-promotion path.

`maxRunsPerJob: 1` is floored to 2 with a warning logged once at backend
start, not merely warned about: a warning leaves the newer record of an
inverted pair deleted and unrecoverable, the floor prevents it, and logging
keeps a config number the daemon is not honouring from changing silently.
The check is deliberately not gated on `supports_shared_locking()`, which is
`topology == "shared"`; `auto` resolves to `single-node` on Windows because
the probe cannot answer there, so a topology gate would be dead on the
platform this roadmap belongs to, and a retention of 1 is a poor setting on
one node anyway since the prune is amortised every eight appends and the
stream oscillates between 1 and 8 records regardless. The floor is applied
through one helper, `_run_prune_keep`, so all three `prune_keep` call sites
move together (`_persist_run_record`, `_archive_output`, and
`_persist_reconciled_record`, which the plan for this item had missed).

The prune itself is deliberately still write-ordered. The one real reason: a
crash-reconciled row under `onMissed` run-once or run-all carries
`interruptedAt` and no `finished_at`, so a finish-keyed prune would have no
sort key for exactly the records the reconciliation path writes. Two
rationales that sound good and are false, recorded so nobody re-derives them:
a field-keyed prune axis already exists (`prune_latest_by`, implemented at
`_prune_latest_by_sync`), and it already reads every record in the stream, so
neither "new axis" nor "converts a listdir into a full read" is an argument.

No future-timestamp clamping was added for clock skew. The record filename
epoch and `finished_at` both come from the writing host's wall clock, so
folding by `finished_at` is exactly as skew-exposed as trusting position, not
worse, and a future timestamp already poisons the catch-up watermark and the
depends-on-past gate. `wiki/Durable-State.md` already requires NTP across a
shared mount.

Explicitly NOT fixed, and not billed as fixed: the three display tails,
`GET /jobs`' inline sparkline slice, `/activity`'s per-job rows, and
`/jobs/{name}/resources`' monitored tail. Sorting the rehydrated rows does
not repair them, because `_install_run_info` appends unconditionally and
`_reconcile_inflight` runs after the warm loop and installs a reconciled row
carrying a past instant, so the ring is out of finish order by the end of
boot regardless. They stay positional on purpose: every row in those cuts is
rendered, so an inverted pair moves a cell one column rather than producing a
wrong verdict; and sorting them would file a crash-reconciled row before runs
that finished while the interrupted run was still going, which is not where
an operator looks for the run that just died. The rationale is written at
`_run_finish_key` and cross-referenced from all three sites. Stated precisely
in `HISTORY.md`, `wiki/Durable-State.md` and the three affected rows of
`wiki/HTTP-API.md`, because "the order this node observed" alone is not the
whole truth: `_warm_one` re-sorts, so immediately after a restart a listing
IS finish-ordered, and it reverts to observation order as live runs append
and `_reconcile_inflight` adds its past-instant row. What holds unqualified
is that nothing re-orders a listing at read time.

One existing assertion moved as a result:
`test_web_job_runs_endpoint_returns_runs_and_stats` recorded four runs
sharing a start instant with durations 2, 4, 6 and 1, so `last_duration` was
1.0 (the last appended) and is now 6.0 (the last finished). The run listing
itself is unchanged.

### Not yet filed

Ranked roughly by payoff.

**6. Authenticode signing for the Windows binaries.** CI half DONE
(2026-08-13); the certificate half is an Azure onboarding only Parker can
run, and releases ship unsigned until it completes. The steps, the six
repo secrets and the activation checklist live in
`windows-signing-runbook.md` at the repo root. The provider decision from
the 2026-08-09 research held: Azure Artifact Signing, Basic tier, OIDC
from the workflow, no stored client secret. The urgency changed since
this entry was written: the live end-user Defender detection on 1.2.38
(2026-08-12) traces to exactly the unsigned zero-reputation onefile shape
this item ends.

The entry's one instruction ("add the step to `binaries-windows`
mirroring the macOS one") was wrong twice. The signing client does not
support Windows ARM runners, so the arm64 lane cannot sign its own build;
and item 13 made the payload exe part of two containers, so signing must
precede packaging. Signing is therefore a separate release-only x64 job
(`sign-windows`) that signs both arches' PEs, re-zips, rebuilds both MSIs
from the signed payload with the same pinned WiX, signs those, verifies
every signature (the action can exit 0 having signed nothing), and
re-smokes the signed amd64 MSI against msiexec. The release job overlays
the signed set before SHA256SUMS, so the sums, the Release assets and the
winget manifests describe the signed bytes. The shape also keeps
`id-token: write` away from the build lanes.

Two facts the entry did not know, both fenced in
`tests/test_ci_fences.py` (red-checked against mutants): Artifact Signing
rotates leaf certificates within days, so an untimestamped signature dies
with its certificate; and the signed artifact's name must dodge the
release job's `cronstable-*` merge, or signed and unsigned copies of the
same filenames fight over download order. Missing secrets warn and ship
unsigned (the macOS rule); a signing failure with the secrets present
fails the release. Signing still does not clear SmartScreen on day one.
Reputation accrues per release and now carries forward, so the documented
SmartScreen workaround stays until signed releases have shipped.

**7. Per-job identity (principal and run level).**
Task Scheduler attaches a principal to every task: a named local or domain
account with a stored password, SYSTEM, LOCAL SERVICE, NETWORK SERVICE, or a
gMSA whose password Windows rotates. cronstable has one identity per daemon,
and `user:`/`group:` is a fatal config-load error on Windows, so a Linux
config carrying it does not degrade, it refuses to load. There is no
elevation control either: to run one job as admin you elevate the whole
daemon, which is the inverse of the POSIX story (run as root, demote per job).
Needs `CreateProcessAsUser`/`CreateProcessWithLogonW` plus credential storage
(DPAPI or Credential Manager), which is a new security surface. The honest
interim is documenting that cronstable is single-principal on Windows.

**8. Non-time triggers and conditions.**
`schedule` is the only trigger key in the job schema. Task Scheduler ships six
other trigger types (logon, startup, idle, event-log event, workstation
lock/unlock, on-demand) and four conditions (AC power, wake the computer,
network available, idle for N minutes). The event-log trigger is the one a
Windows admin names first. A Windows-shaped subset is tractable: an `onlyIf:`
condition block (power via `GetSystemPowerStatus`, idle via
`GetLastInputInfo`, network via NLM) reuses the existing per-job gate plumbing
next to `onlyIfLastSucceeded`. An event-log trigger (`EvtSubscribe`) is a new
trigger axis and is large.

**9. Sleep and resume.**
No wake timer, and a running daemon never catches up after a resume:
`_catch_up` latches `_caught_up` at boot and never re-arms, and `_advance`
past `CATCHUP_LIMIT` resyncs to the current slot rather than replaying. A
`0 3 * * *` job on a laptop asleep 23:00 to 08:00 simply does not run. Task
Scheduler covers this with two checkboxes ("Wake the computer to run this
task", "Run task as soon as possible after a scheduled start is missed"). The
catch-up half is medium (detect resume, clear the latch or run a resume-scoped
missed-occurrences pass); a real wake timer (`SetWaitableTimerEx`) is large.
Faithful cron behavior, so this is a platform-fit gap rather than a bug, but
it is the first failure a laptop reviewer hits.

**10. Windows Job Objects for job trees.**
The current kill path shells out to `taskkill.exe`, bounded at 10 seconds,
with a direct-child fallback that orphans the real workload when it times out;
the project's own CI notes record Windows process spawns exceeding that bound
on a degraded runner. A descendant already orphaned before the tree walk
survives it. `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` kills the whole set
atomically because membership is by assignment, not live parentage. Note the
review found no evidence Task Scheduler does this better, so this is an
internal robustness item, not a parity gap. Pairs naturally with item 11 (the
same object carries the limits).

**11. Resource limits.**
The priority half of this item shipped (see What shipped above), which closes
the only real parity gap here: Task Scheduler's per-task `-Priority` (0
highest to 10 lowest, default 7). What remains is memory and CPU caps, and
`monitorResources` is still observability only. Task Scheduler has no memory
or CPU cap either, so this is a capability nobody else on the platform offers
rather than a parity gap. Real limits only work through a Job Object, so they
depend on item 10.

**13. MSI packaging for managed deployment, and a one-directory build.**
DONE on this branch, both halves. The zip half: a `CRONSTABLE_BUNDLE=onedir`
knob in `pyinstaller/cronstable.spec` (shared Analysis, the EXE branch
becomes `EXE(exclude_binaries=True)` plus `COLLECT`; the default stays
byte-for-byte the one-file build, because six knob-less consumers read
`dist/cronstable`), a second PyInstaller invocation in `binaries-windows`
with its own workpath, and `cronstable-windows-<arch>.zip` assets holding a
single top-level `cronstable/` folder, so extraction into
`C:\Program Files` produces the exact path the schtasks and firewall
recipes already used. The lane proves each zip against the real SCM
(`service install`, `status`, `remove` under a CI-scoped name; the hosted
runners are elevated, and nothing is ever started) before uploading. The
refusal message in `winservice.install` names the zip and the MSI now.

The MSI half: `packaging/msi/cronstable.wxs`, WiX v6 installed as a pinned
.NET tool in the lane (WiX is NOT preinstalled on windows-latest or
windows-11-arm, contrary to old lore), one .wxs for both arches selected by
`-arch`. Declarative service registration in full parity with
`winservice.install()`, fenced by `tests/test_msi_parity.py` (identity,
ImagePath shape through `host_argv`/`image_path`, recovery plan, the
failure-actions flag via the native ServiceConfig element, and an AST walk
that pins the set of api calls `install()` makes so a new setting reddens
the fence). Decisions worth keeping: the MSI never starts an unconfigured
service (first install registers only; an upgrade that finds
`%ProgramData%\cronstable` restarts; `STARTSERVICE=1` overrides), it
creates no config directory and sets no ACLs (`cronstable init` owns that
audited logic), and `MajorUpgrade` is scheduled `afterInstallInitialize`
because early RemoveExistingProducts is what makes the wildcard-harvested
payload's auto GUIDs safe across releases. Two things only building it
revealed: WiX v6's `Files` element has no `Exclude` attribute (the harvest
targets `_internal\**` under its own Directory instead, with the exe
hand-authored), and bind path variables resolve at bind time while `Files`
harvesting runs at compile time, so the payload path is a `-d Payload=`
preprocessor variable, not a `-b` bind path (the bindpath spelling
harvests zero files and still builds).

Two deliberate departures from the entry as written. winget stays on the
portable one-file exe: a winget zip would still be a per-user
`%LOCALAPPDATA%` install, which is a user-writable ImagePath for a
LocalSystem service, and `wingetcreate update` cannot express a
zip/NestedInstallerFiles manifest anyway; if winget ever carries the
machine shape it should carry the MSI, after item 6 signs it. And the MSI
did not wait for the certificate: GPO/Intune/SCCM deployment does not
involve SmartScreen, so an unsigned MSI serves the managed path today and
the Authenticode step slots in between build and smoke when item 6 lands.
The entry's "packaging change with no code in it" was wrong in one small
way: the refusal message and its test moved too.

**14. A PowerShell module.**
A Windows estate drives Task Scheduler with the in-box `ScheduledTasks` module
(`Get-ScheduledTask`, `Register-ScheduledTask`), which is also how DSC and
Ansible's `win_scheduled_task` reach it. cronstable's equivalent is the HTTP API,
which `Invoke-RestMethod` deserializes natively, so this is polish rather than
a capability gap. A thin module wrapping the control API as
`Get-CronstableJob`/`Start-CronstableJob` would suit a scripted estate. Shell
completion is absent on every platform, not just PowerShell, so treat that
separately.

**15. Windows-native monitoring surfaces.**
No PerfMon counters, no WMI provider, no toast notifications. Prometheus and
statsd both work on Windows, so this only matters for shops whose monitoring
does not already scrape Prometheus. A `report.toast` reporter is cheap and
demos well; a windows_exporter textfile writer gets cronstable metrics into an
existing Windows Prometheus setup with no new scrape target. Real PerfMon
counters or a WMI provider are large and probably not worth it. Item 3 is the
one that actually matters here.

**16. Per-OS coverage profiles.** DONE on this branch, coverage half only.
The premise was right and its arithmetic was stale. `platform.py` is 301
statements today, not 255; the 21 Windows-tagged sites hide 150 of them and
bare pragmas two more, so a Windows run measured 149 statements at 69.14%,
not 58% and not 60%. Items 11 and 12 had already moved the number by adding
POSIX-side statements that were measured.

Three pragma forms now, not one: a bare `# pragma: no cover` is hidden on
every OS, `(windows)` is hidden on POSIX and measured on Windows, `(posix)`
is the mirror. One coverage configuration serves both, because
`pyproject.toml`'s `exclude_lines` goes through coverage's own
`${CRONSTABLE_COVERAGE_SKIP-windows}` substitution rather than a second
`.coveragerc` that would duplicate `source`, `branch` and `omit` and ride
into the sdist. Rule 1 is coverage's `DEFAULT_EXCLUDE[0]` with a lookahead
appended, so the vocabulary is not narrowed by a hand-rolled third spelling,
and both rules take the token anywhere after `cover` so a site keeps the
prose that explains it. `tox.ini` gives every interpreter row a `windows`
and a `posix` arm; `platform` skips the arm that does not match, and a skip
is a pass as long as some selected env did run, so `tox -e
py-windows,py-posix` is correct on all three runner kinds and either arm
named alone is correct on exactly one of them.

`tests/test_coverage_profiles.py` scans every file under `cronstable/`, not
`platform.py` alone, and builds its pragma scanner from coverage's own
`DEFAULT_EXCLUDE[0]` rather than restating it, so a site spelled `# pragma
no cover (windwos)` cannot be excluded by the gate and invisible to the
vocabulary tests at the same time. Rule 1 is still pinned against
`DEFAULT_EXCLUDE[0]` verbatim, because that relationship is what rule 1 is;
the two copied-in defaults are pinned by what they exclude instead, since
coverage rewords them between patch releases (7.14.1 spells the stub rule
`?\)(\s*->`, 7.14.3 `?[\])]+(\s*->`) and a verbatim pin reds every cell
whose resolved coverage is a patch out of step. Pinning `coverage==` in
`requirements_dev.txt` would have fixed the same symptom, and was not done:
that file pins nothing at all today, on purpose, and a pin bought to satisfy
a test assertion is a standing upgrade obligation for a cosmetic upstream
edit. The assertion moved instead.

Measured on a Windows box, one suite run re-reported under both profiles.
`platform.py` goes from 149 statements / 50 missed / 69.14% to 250 / 29 /
87.67%. `config.py` goes from 1,388 / 57 / 94.82% to 1,358 / 27 / 97.02%,
and `tui.py` from 3,953 / 155 / 94.66% to 3,993 / 168 / 94.27% (it gains
`WindowsKeyReader`, which no test touches). The project total goes from
22,329 statements at 96.72% to 22,442 at 96.84%. (The missed count wanders
by one between runs on this box, so the statement count and the percentage
are the figures worth quoting.)

The POSIX side moves by four statements and no more. It reported 22,333
statements at 96.71% before the retag; under the default profile it now
reports 22,329 at 96.72%, and the four that left are `config.py`'s
Windows-only user/group rejection and priority advisory, which cannot
execute on a POSIX cell and were being counted there as permanently missed.
That is the same defect the item exists to remove, showing up on the side it
was not aimed at.

Two traps found by proving the mechanism rather than reading it. The POSIX
platform selector has to be `posix: (?!win32).*`: tox matches with
`re.fullmatch`, so the bare zero-width lookahead `(?!win32)` can only match
the empty string, which means it matches no platform at all and the POSIX
arm skips everywhere, and the suite stops running on Linux. That failure is
loud, not silent: measured on tox 4.58.0 on both OSes, an invocation whose
every selected env skips prints `evaluation failed :(` and exits 1, so the
Linux cells would have gone red rather than green-with-no-suite. A skip
counts as a pass only when at least one selected env actually ran, which is
also why `tox -e py-posix` alone exits 1 on a Windows box and why the
contributor docs name both arms. And `commands` stays unconditional, with
only the floor parameterized through `{env:CRONSTABLE_COV_FLOOR}`: a fully
factored `commands` resolves to nothing in an unfactored env, so `tox -e py`
and `tox -e py312`, the documented local invocations, would have built a
venv, run no tests and reported OK. `tests/test_coverage_profiles.py` pins
both, asserting the selectors by what they match rather than by their text
so a test can never freeze the broken form in.

The merged Codecov number will drop when this lands, and that is the union,
not a regression. The two profiles no longer measure the same line set, so a
`(windows)` line is excluded from every POSIX report and included in every
Windows one, and several such lines are genuinely unhit (the list below).
`.github/codecov.yml` keeps `informational: true` on both the project and
the patch status, so the drop can annotate a PR but cannot fail one; the tox
gate remains the only pass/fail.

Still open, both deliberate:

* **The floors.** Unchanged at 92 on both arms. A number measured here is an
  upper bound, not the gating cell's: `requirements_dev.txt` excludes
  `cryptography`, `orjson` and `playwright` on win-arm64, the
  `windows-11-arm`/3.14 row gates, and Chromium installs on one Linux cell
  only, so no Windows cell runs the web E2E suites. Read the real per-cell
  numbers off one CI run and ratchet each arm separately; `tox.ini` already
  carries a per-arm `CRONSTABLE_COV_FLOOR` so that is a one-number edit.
* **`tox -e mypy-win`.** Not wired, and it is further away than it looked.
  `mypy -p cronstable --platform win32` improves with `config.py`'s POSIX
  body becoming an `else` clause, and `--platform linux` stays clean, but the
  remaining errors are not all prunable. `platform.apply_priority`'s
  `os.setpriority(os.PRIO_PGRP, ...)` is reached behind the plain `windows=`
  bool that item 11 gave it precisely so a test can drive the POSIX arm from
  Windows; no `sys.platform` spelling can prune that without making the arm
  unreachable on Windows and deleting the test's point, so the only green
  spelling today is a `type: ignore` the Linux run considers unused, and
  there is no per-run ignore syntax. `kill_process_group` is the same story
  for the same reason and its guard deliberately stays `IS_WINDOWS`: four
  tests drive its Windows arm from POSIX by monkeypatching that name, which
  a `sys.platform` comparison cannot express, and rewriting it sent
  `os.killpg` a real signal on the runner. The `else:` header the profiles
  need is orthogonal to the guard's spelling; keep the header, keep the
  name. Wire the env when those calls have a better shape, not before.

Newly visible and genuinely untested on Windows, worth tests in their own
right rather than as a coverage chase: `_taskkill_tree`'s spawn failure and
timeout arms, the ctypes callback in `_install_windows_console_handler`, the
`GetTickCount64` and `OpenProcess` failure returns, `exclusive_file_lock`'s
non-contention re-raise, `fsync_directory`'s `CreateFileW` failure,
`filesystem.py`'s Windows shared-mount advisory (unhit on any OS), and
`tui.py`'s `WindowsKeyReader._pump`, the single largest untested Windows
block in the tree.

Platform branches outside `cronstable/platform.py`, `config.py` and `tui.py`
are untagged on purpose, and the documented rule says so rather than
promising a sweep the tree does not follow. A tag is a claim that the code
truly cannot run on the other OS, which has to be checked one site at a time.
`state.py`'s six `IS_WINDOWS` branches (`detect_topology`, `_replace`,
`_unlink`, the two lock-sweep arms and `_reclaim_orphan_lock`) and
`job.py`'s OEM decode fail that test: their arms are plain Python that the
suite drives from either box by monkeypatching `IS_WINDOWS`, so both sides
are genuinely measured on both profiles and tagging either would delete real
coverage. What `tests/test_coverage_profiles.py` does enforce, across every
file rather than `platform.py` alone, is that a branch tagged on one side is
tagged on the other: half-tagging is what actually happened, twice, in
`config.py`.

### Resolved, do not re-open

**One-off schedules already exist.** The review flagged the absence of an
at(1) equivalent and a verifier refuted it. The cron engine has a year column
(`cronexpr.py`, `_LAYOUTS` for 5, 6 and 7 fields), so `"0 2 8 8 * 2026"` fires
once and then returns `None`; there is also an object-form `year:` key, and a
lapsed one-shot is surfaced as `never_fires` at config load, on `/status` and
on `/jobs`. What is genuinely missing is narrower: an arbitrary date-range
window, and 6/7-field schedules inside classic crontab files (`crontabs.py`
takes exactly five schedule fields).

**The archive-snapshot test flake.** Root-caused and fixed on this branch; see
the ordering fix above. It was a real product defect, not a flake.

## Notes for whoever picks this up

Local-run gotchas on the Windows dev box, both of which cost time here:

* A plain PowerShell `PATH` has no `ls.exe`, so
  `tests/test_perf_invariants.py::test_one_run_writes_open_record_close_and_nothing_else`
  fails with "'ls' is not recognized". Prepend `C:\Program Files\Git\usr\bin`,
  which is effectively what the CI runners have.
* A green local Windows run does not mean CI is green: a set of POSIX-only
  tests skip on Windows. Reproduce on Linux under WSL before concluding
  anything about a cross-platform change. Note the WSL system Python carries
  sentry_sdk 1.45, while `job.py` needs 2.x for `sentry_sdk.new_scope`, so five
  sentry-reporter tests fail there regardless of the diff unless you use a venv.

Two behaviors worth knowing before changing the shutdown paths:

* Keyboard Ctrl-Break, unlike Ctrl-C, still reaches jobs, because a
  console-generated break goes to every process attached to the console. The
  docs therefore steer users to Ctrl-C. This is not something the process-group
  change can fix.
* A Python `SIGBREAK` handler in a child only runs between bytecodes, so a long
  `time.sleep` delays it. Relevant when writing tests that expect a job to trap
  the graceful signal.
