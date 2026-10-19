# Workflow recovery

Recovery creates a new DAG run from a finished run. It reuses retained task
results and their XCom artifacts, then executes the selected tasks and their
downstream dependants. The source run remains available for inspection.

In the dashboard, open a failed run and select **Retry failed tasks**. To
repeat a larger portion of a finished run, select **Recover from here** on a
task. The preview lists tasks to run, results to reuse, retained artifacts,
and the configuration revision. Review it, then select **Start recovery**.
A results card then lists the new runs with their states and counts the ones
that have finished. It updates until every run has finished.

For mapped tasks, retrying failures preserves successful instances and their
map items. Recovering from the mapping producer discards the old expansion
and builds it from the producer's new output. Recovered tasks start with a
fresh retry budget. Retained approval decisions stay with reused tasks;
selected approval gates require a new decision.

## API

Preview a failed-task recovery:

```http
POST /dags/orders/runs/source-run/recover
Content-Type: application/json

{"mode":"failed","dryRun":true}
```

For a specific starting task, use `{"mode":"from","tasks":["extract"]}`.
The `tasks` list contains task-instance keys, including keys such as
`load#2` for mapped instances. A mapped group key selects its instances.

The response includes `planToken`. Submit the same selection with
`dryRun: false` and that token to execute it. A changed source, artifact
inventory, or configuration invalidates the preview. When the current
configuration differs from the source's recorded revision, execution also
requires `allowConfigChange: true`. Runs without a recorded revision require
this acknowledgement. The revision covers each task's launch settings and the
names of its environment variables. It leaves out environment values, so the
revision shown to view readers reveals no secret. Changing a value, such as
rotating a password, does not require the acknowledgement. Adding or removing
task IDs, or changing whether a task is mapped, requires a full run.

Recovery accepts a source run that holds at most 10,000 artifacts. The limit
counts artifact names, so a name that was published more than once counts
once. For a run with more names, a preview or an execution answers `409`
with `recovery supports at most 10000 artifacts`. A date-range request gives
the same answer when one of its source runs has more.

Identical accepted plans use the same recovery run key. Retrying a request
therefore returns that run while it remains retained. Missing retained
artifacts prevent execution. If preparation fails after acceptance, the new
run records the failure before launching tasks: `recovery.status` becomes
`failed`, `recovery.error` holds the reason, the run's `state` becomes
`failed`, and each task that had not finished becomes `failed` with the
reason as its `failReason`. The reason is
`recovery configuration changed before preparation` or
`recovery artifact is missing: <artifact name>`. When the daemon cannot read
the recovery run's own document as it prepares the run, the run stays in
preparation (`recovery.status` is `preparing`). The daemon logs
`dag run <workflow>/<run key>: advance failed` at `ERROR`, with a traceback
that ends in the unreadable document, and advances the run again 5 seconds
later.

Retention protects a source while its recovery copies artifact references.
When retention can't read a recovery run or a recovery batch, it keeps every
run of the workflow and logs a warning that names the document. A document
that stays unreadable holds the runs for seven days after its last write.

To create a recovery run, an execution holds the lease of the source run
while it confirms the preview and writes the run. While retention or another
recovery holds that lease, the request answers `409` with
`source run is busy; retry shortly`. Once the lease is free, the same token
returns the recovery run, or answers `409` with `workflow run not found`
when retention has deleted the source.

When the write that creates the recovery run times out, the request answers
`503` with `recovery state is unavailable`. A write that timed out, or whose
request was canceled, can still land, so the source run's lease stays until
it lapses, 60 seconds after it was taken. Until then the same token answers
`source run is busy; retry shortly`, unless the write landed, in which case
it returns the recovery run with `created: false`. After the lease lapses,
the same token creates the run if the write did not land.

An execution answers `source run is busy; retry shortly` whenever the store
denies the lease of a source run. The store denies the lease in three
cases:

- Another holder has it: retention, another recovery, or a lease acquire
  that an earlier request abandoned on a timeout. The daemon logs nothing.
- The store cannot read the lease file. Its content is damaged, or the open
  fails: on Linux for a file whose permissions deny the read, and on Windows
  for a file that another handle holds without sharing. The daemon logs
  `state: lease dagadvance/<workflow>/<run key> unreadable (<cause>);
  denying acquire` at `WARNING` from logger `cronstable.state`.
- The write of the lease file fails: a full disk, for a source whose lease
  files the store still holds; a lease directory that the daemon cannot
  write; or, on Windows, a lease file that is read-only or that another
  process has open (a sharing violation that outlasts the retries). The
  daemon logs `state: lease dagadvance/<workflow>/<run key> write failed
  (<cause>); denying acquire` the same way.

In the last two cases the answer stays `409` on every attempt until the
fault clears, so these lease-file faults are an exception to the `503`
answer for store failures that this page describes later.

An execution answers `503` with `recovery state is unavailable` when the
store fails before it reads the lease file. That is the case when the
lease's lock file cannot be opened or created: on a read-only store, on a
full disk for a source that has no lease files in the store, or when the
lock file's permissions deny the open. On a read-only store every execution
answers `503`, and previews, which take no lease, answer `200`. On a full
disk the answer depends on the source. The store keeps a run's lease files
from the time a node first advances the run until
[garbage collection](Durable-State#garbage-collection-and-manifests)
removes them, once the lease's expiry and last write are both older than
`gcGraceSeconds` (604800 seconds by default). A source whose lease files
the store holds answers `409` with the `write failed` warning, and a source
whose lease files are gone answers `503`. A lease operation that takes
longer than 10 seconds also answers `503`. An acquire that timed out can
still land. The source run's lease then stays until it lapses, 60 seconds
after it was taken, and until then the same token answers `409` with
`source run is busy; retry shortly`. The daemon logs nothing for these
`503` answers.

## Failed dates

The dashboard's backfill form has a **Failed dates only** option. It previews
recovery for the latest retained run at each logical date in the requested
range. Successful dates and dates whose latest run is still active are
excluded. Dates without retained runs are excluded too.

Use `POST /dags/{name}/recover` with `from`, `to`, and `dryRun: true` for the
same preview. At most 100 failed dates are accepted per batch. Execute with
the returned `planToken` and `dryRun: false`. A batch records each created run;
resubmitting the same token continues an interrupted batch without creating
duplicates for completed entries. Batch progress is retained for seven days.
Incomplete batches protect their source runs for that period. Date bounds
are inclusive; a bound without a time zone uses UTC.

The first execution of a preview takes the lease of every source run before
it records the batch, so retention keeps the sources of a batch that is
accepted while a retention pass runs. These answers about the leases and
the presence of the source runs come before any batch is recorded:

- `409` with `source run is busy; retry shortly` while retention or another
  recovery holds the lease of a source run, or while the store denies that
  lease for one of the other reasons given earlier. Once the lease is free,
  the same token succeeds when a concurrent request with the same token has
  recorded the batch, or when the source is still the latest failed run of
  its date, as after a retention batch that kept the run or a write that
  timed out. It
  answers `recovery preview is stale; preview again` when retention has
  deleted the source, or when another recovery has created a run for that
  date (a single-run recovery, or a date-range recovery with another token).
  Retention leases a run in order to delete it, so the stale answer is the
  usual one after retention held the lease. On a
  [rate-limited or slow store](Durable-State#rate-limiting-maxopspersecond),
  a retention batch can hold a run for about 30 seconds. A retention delete
  that timed out holds its run's lease until the lease lapses, 60 seconds
  after it was taken.
- `409` with `recovery preview is stale; preview again` when a source run is
  gone or its key holds another run.
- `409` with `workflow run not found` when a source run is deleted between
  the request's listing of the workflow's runs and its planning of that run.
  A preview can give the same answer. Preview again in either case.
- `409` with `leasing the source runs took too long; retry or select fewer
  dates` when taking the leases and confirming the sources takes 30 seconds
  or longer. On a store that slow, a smaller date range needs fewer leases.

When the write of the batch itself times out, the request answers `503` with
`recovery state is unavailable`. A write that timed out, or whose request was
canceled, can still land, so the source leases stay until they lapse, 60
seconds after they were taken. Until then the same token answers
`source run is busy; retry shortly`. After that it continues the batch if the
write landed, and records the batch if it did not.

A request whose batch is already recorded continues the batch, and leases
each source run only while it creates that run's recovery. The first
execution does the same once it has recorded its batch. When another holder
has a source's lease at that point, such as retention or a single-run
recovery, the request answers `409` with `source run is busy; retry shortly`.
The batch stays recorded with the dates already recovered, and the same
token continues it. A create that times out leaves that source's lease to
lapse, as in a single-run recovery. A node on an older build records its
batch without the leases, so move the nodes that share a store to one build
together.

Both endpoints require the `control` scope, and both answer `503` with
`recovery state is unavailable` when the state store cannot answer: the
configured store has not started, a store operation fails or times out, or a
recovery record or document cannot be read. A lease-file fault is the
exception and answers `409`, as described earlier. A request for a run that
the store does not hold answers `409` with `workflow run not found`, and a
date-range request for a workflow that the configuration lacks answers `409`
with `workflow not found` before any read. A daemon whose configuration has
no `state` section has no workflow and no store to wait for, so it gives
those two `409` answers in place of the `503`. MCP exposes the same
operations through `cron_preview_recovery` and `cron_recover_dag`, which
report these answers as tool errors with the same messages.

A single-run preview of a workflow that the configuration lacks answers
`409` with `workflow run not found` without reading the run, so a damaged
document that a removed workflow left behind does not turn the answer into
`503`. A single-run execution for such a workflow first reads the recovery
run of its token. It returns that run with `created: false` when the store
holds it, answers `503` when that document cannot be read, and otherwise
answers `409` with `workflow run not found` once it holds the source lease.

A run document or a batch document cannot be read when its file has a
damaged body, carries a schema version that this build does not know, or
fails to open with an I/O error (on Windows, a sharing violation that
outlasts the retries, five attempts over about 200 ms). The documents that
a request reads depend on its kind:

- A single-run preview reads the source run.
- A single-run execution reads the recovery run of its token,
  `recovery-<planToken>`. When the store holds that run, the request returns
  it and reads no other run. When the store holds none, the request leases
  the source run and reads it.
- A date-range preview lists every run document of the workflow, then reads
  the source run of each failed date.
- The first execution of a date-range token, whose batch the store does not
  hold, reads the batch document, lists every run document of the workflow
  to plan, and lists them once more while it holds the source leases. It
  then records the batch and recovers each date as a single-run execution
  does.
- A date-range execution whose batch is recorded reads the batch document
  and plans from it. For each date that the batch has yet to record, it
  does what a single-run execution does. It lists no runs.

A date-range preview and the first execution of a date-range token list
every run document of the workflow, inside the requested range or outside
it. While one run document of the workflow cannot be read, both answer
`503`, whatever the range. Recovery skips no unreadable document after a
time, so the answer stays `503` until the file named in the daemon's
warning can be read or is removed. A single-run recovery and a date-range
execution whose batch is recorded are unaffected by an unreadable document
of another run. Such an execution completes while the source run of a date
that the batch has recovered cannot be read. It answers `503` while the
batch document cannot be read, or while the recovery run or the source run
of a date that it has yet to recover cannot be read.

When a request answers `503` because a run document or a batch document
cannot be read, the daemon logs one warning from logger `cronstable.dagrun`:
`dag <workflow>: recovery is unavailable while a document cannot be read:
<document>: <cause>`. `<document>` is `document '<key>' in '<namespace>'`
when the request read one document (a source run, a recovery run, or a
batch, whose key is the plan token). It is
`document file '<file name>' in '<namespace>'` when the request listed the
runs. The file name is the run key's file in the store and ends in `.doc`,
and a listing names the first unreadable file in name order. The namespace
is `dagrun/<workflow>` for a run and `recoverybatch/<workflow>` for a batch.
`<cause>` is the parser's or the operating system's text, or
`unknown-schema-or-not-a-document` for an unknown schema version. For
example:

```text
dag flow: recovery is unavailable while a document cannot be read: document file 'rerun-1.doc' in 'dagrun/flow': unexpected end of data: line 1 column 368 (char 367)
```

The warning covers only these document reads. A `503` with another cause
logs no line from the recovery request: a run directory that cannot be
listed, a lease lock file that cannot be opened, a write that fails, or a
timeout.

Recovery repeats selected commands and their external side effects. The
preview identifies the work to repeat; use commands whose writes tolerate
that replay. Recovery depends on the current configuration and retained
state, rather than an archived copy of the executable or its environment.
