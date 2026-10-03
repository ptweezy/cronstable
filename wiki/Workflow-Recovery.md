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

## Skipped branches

A workflow that [branches](Orchestration-and-DAGs#conditional-branching) can
finish with skipped tasks. Recovery treats each one by where it sits:

- A task that was skipped and is not downstream of a selected task is reused
  as `skipped`, with its recorded `skipReason`. Retrying failed tasks
  therefore leaves a branch that skipped itself closed, and a branch that a
  `when:` condition skipped.
- A join that ended `upstream_failed` is downstream of the failure, so it
  runs again. Its trigger rule reads the reused skip and the rerun branch.
- A failure handler (`all_done_min_one_failed`) that ran is downstream of the
  failed task, so it is reset. If the rerun succeeds, the handler ends
  `skipped`.

To decide a branch again, recover from the task that skipped itself:
`{"mode":"from","tasks":["incremental-load"]}`. Its command runs again and
chooses again whether to skip.

A task with a [`when:` condition](Orchestration-and-DAGs#conditions-on-parameters-and-xcom-values) gets a fresh entry when recovery
selects it, so its condition is read again:

- A recovery run reuses the source's parameters, so a comparison on a
  parameter gives the same answer unless the configuration changed.
- To read an XCom comparison against a new value, recover from the task that
  publishes the value: `{"mode":"from","tasks":["extract"]}`. The publisher
  is upstream of the conditional task, so the selection includes both.
- When the publisher is reused, recovery copies its published value into the
  recovery run, and the comparison reads that copy.
- A mapped task that already fanned out keeps its recorded result when
  recovery reruns its instances. Recover from its expand source to decide
  the fan-out again.

The preview lists the reused tasks that stay skipped under `preservedSkipped`,
an object that maps each task key to its `skipReason`, or to `null` for a run
that recorded none. The key is absent when no reused task is skipped. The
dashboard shows the same list as **Stays skipped**.

## Run parameters

A recovery run reuses the source run's
[parameters](Orchestration-and-DAGs#run-parameters) and takes no new values.
The preview lists them under `params`.

- The declaration is part of the configuration revision. When it changed
  after the source run was created, execution requires
  `allowConfigChange: true`. A parameter's `description` stays out of the
  revision.
- The source's values have to fit the current declaration as a complete map.
  The request answers `409` and names each parameter when a stored value
  fails a constraint, when the declaration has a parameter the source lacks,
  or when the source has one the declaration dropped. Create a full run for
  that workflow instead.
- To run the same date with other values, trigger a manual run with
  `logicalDate` and `params`.

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
this acknowledgement. The revision covers each task's launch settings, its
graph fields such as `triggerRule`, `skipExitCodes`, and `when`, and the
names of its environment variables. It leaves out environment values, so the
revision shown to view readers reveals no secret. Changing a value, such as
rotating a password, does not require the acknowledgement. Adding or removing
task IDs, or changing whether a task is mapped, requires a full run.

Identical accepted plans use the same recovery run key. Retrying a request
therefore returns that run while it remains retained. Missing retained
artifacts prevent execution. If preparation fails after acceptance, the new
run records the failure before launching tasks. Retention protects a source
while its recovery copies artifact references.

A node answers `409` for a source run above its
[run engine level](Orchestration-and-DAGs#run-engine-levels). Send the
request to a node at that level.

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
