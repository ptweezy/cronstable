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
this acknowledgement. Adding or removing task IDs, or changing whether a
task is mapped, requires a full run.

Identical accepted plans use the same recovery run key. Retrying a request
therefore returns that run while it remains retained. Missing retained
artifacts prevent execution. If preparation fails after acceptance, the new
run records the failure before launching tasks. Retention protects a source
while its recovery copies artifact references.

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

Both endpoints require the `control` scope. MCP exposes the same operations
through `cron_preview_recovery` and `cron_recover_dag`.

Recovery repeats selected commands and their external side effects. The
preview identifies the work to repeat; use commands whose writes tolerate
that replay. Recovery depends on the current configuration and retained
state, rather than an archived copy of the executable or its environment.
