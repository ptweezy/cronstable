# Workflow recovery

Recovery creates a new DAG run from a finished run. It reuses retained task
results and their XCom artifacts, then executes the selected tasks and their
downstream dependants. The source run remains available for inspection.

In the dashboard, open a failed run and select **Retry failed tasks**. To
repeat a larger portion of a finished run, select **Recover from here** on a
task. The preview lists tasks to run, results to reuse, retained artifacts,
and the configuration revision. Review it, then select **Start recovery**.

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
run records the failure before launching tasks. Retention protects a source
while its recovery copies artifact references. When retention can't read a
recovery run or a recovery batch, it keeps every run of the workflow and logs
a warning that names the document. A document that stays unreadable holds the
runs for seven days after its last write.

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
  recovery holds the lease of a source run. Once the lease is free, the same
  token succeeds when a concurrent request with the same token has recorded
  the batch, or when the source is still the latest failed run of its date,
  as after a retention batch that kept the run or a write that timed out. It
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
recovery record or document cannot be read. A request for a run that the
store does not hold answers `409` with `workflow run not found`, and a
date-range request for a workflow that the configuration lacks answers `409`
with `workflow not found`. A daemon whose configuration has no `state`
section has no workflow and no store to wait for, so it gives those two
`409` answers in place of the `503`. MCP exposes the same operations through
`cron_preview_recovery` and `cron_recover_dag`, which report these answers
as tool errors with the same messages.

Recovery repeats selected commands and their external side effects. The
preview identifies the work to repeat; use commands whose writes tolerate
that replay. Recovery depends on the current configuration and retained
state, rather than an archived copy of the executable or its environment.
