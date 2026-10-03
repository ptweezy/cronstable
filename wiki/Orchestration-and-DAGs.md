# Orchestration and DAGs

Use the optional `dags:` section to define **workflows** whose tasks run in
dependency order. Each workflow is a **DAG** (directed acyclic graph) backed
by [durable state](Durable-State), allowing it to resume after restarts and
coordinate across a fleet. DAGs use the existing job runner and state store:

- a **dag_run** (one execution of a DAG) is a single mutable *document* in the
  [state store](Durable-State), holding every task's state;
- a **task** is an ordinary job invocation: the same command/shell/env/
  timeout machinery, launched the same way, with the same
  [loopback state endpoint](Durable-State#job-facing-state) injected,
  so a task can call `cronstable xcom|artifact|state|lock|...`;
- **cross-task data** (XCom, cross-communication) rides the artifact store,
  scoped per dag_run;
- the scheduler advances each run under a single **lease**, so a healthy fleet
  does not launch a task twice or double-advance a run (crash recovery is
  at-least-once; see [crash-resume and the fleet](#crash-resume-and-the-fleet)).

> **Opt-in and store-backed.** DAGs require a `state` section with the loopback
> endpoint (`state.jobApi.enabled`, on by default). Without `dags:` none of
> this exists. Adding it leaves plain scheduled jobs unchanged.

**On this page:** [A first DAG](#a-first-dag) ·
[Tasks and dependencies](#tasks-and-dependencies) ·
[The task state machine](#the-task-state-machine) ·
[XCom: passing data between tasks](#xcom-passing-data-between-tasks) ·
[Fan-out: dynamic mapping](#fan-out-dynamic-mapping) ·
[Sensors](#sensors) · [Approval gates](#approval-gates) ·
[Conditional branching](#conditional-branching) ·
[Run parameters](#run-parameters) ·
[Scheduling, catch-up, and backfill](#scheduling-catch-up-and-backfill) ·
[Crash-resume and the fleet](#crash-resume-and-the-fleet) ·
[Retention and GC](#retention-and-gc) ·
[Inspecting and controlling runs](#inspecting-and-controlling-runs)

## A first DAG

```yaml
state:
  path: /var/lib/cronstable/state      # DAGs need a state store + jobApi
dags:
  - name: nightly-etl
    schedule: "0 2 * * *"
    tasks:
      - id: extract
        command: "echo '[1,2,3]' | cronstable xcom push --key ids"
      - id: transform
        dependsOn:
          - extract
        command: "cronstable xcom pull --task extract --key ids"
      - id: load
        dependsOn:
          - transform
        command: "echo loading"
```

At 02:00 the daemon creates a dag_run and advances it: `extract` runs first,
then `transform` (after `extract` succeeds), then `load`. Every transition is
durable, so a restart resumes the run from exactly where it was.

## Tasks and dependencies

Each task has an `id` (unique within the DAG) and, except for an approval gate,
a `command` (the same string-or-list command a job takes). You declare edges
with `dependsOn:`, a list of upstream task ids. The graph must be **acyclic**
and every dependency must resolve. A cycle or a dangling edge is a
configuration error at load, never a run that stops responding at runtime.

A task's `triggerRule` governs its readiness. Every rule waits until each
upstream reaches a terminal state, then reads those states:

| `triggerRule` | The task runs when… | Otherwise |
| --- | --- | --- |
| `all_success` (default) | every upstream succeeded | an upstream failure makes it `upstream_failed`, and an upstream skip cascades a `skipped` |
| `all_done` | every upstream is terminal, whatever the outcome | it always runs |
| `none_failed` | no upstream failed | an upstream failure makes it `upstream_failed` |
| `none_failed_min_one_success` | no upstream failed and at least one succeeded | an upstream failure makes it `upstream_failed`, and it is `skipped` when every upstream was skipped |
| `all_done_min_one_failed` | at least one upstream failed | it is `skipped` |

[Conditional branching](#conditional-branching) shows the last three rules
in use.

Per-task launch fields mirror a job: `shell`, `environment`, `captureStdout` /
`captureStderr`, `executionTimeout`, `killTimeout`, `user` / `group`,
`workingDirectory`, `priority`, `failsWhen`, run-scoped `secrets`,
`monitorResources`, and the rest of the shared launch keys. The
[configuration reference](Configuration-Reference#dags) has the complete list,
with types and defaults.

Because a task **is** a job invocation, its launch fields inherit the file's
[`defaults:` block](Includes-and-Defaults#the-defaults-section) the same way a
job does: a global `shell`, `environment`, `monitorResources`, run-scoped
`secrets`, or reporter block covers DAG tasks too, and the task's own value
wins on any key it sets.

A task's `onFailure` / `onSuccess` reporters (set per-task or inherited) fire
on each of its runs, every failed attempt included. Per-task the two hooks
accept a `report` block only, because a task's retries come from the node's
`retries` field, not a job-level `onFailure.retry` policy (an inherited one is
ignored for tasks).

Only the **launch** fields inherit. The DAG-node fields that shape the graph
(`dependsOn`, `triggerRule`, `skipExitCodes`, `when`, `retries`,
`retryDelaySeconds`, `expand`, `onReject`, the poke settings) are never
touched by a `defaults:` block.

The DAG's own schedule frame is separate too: the synthetic trigger job that
fires the DAG on schedule stays on the built-in defaults, so a global
`onSuccess`/`onFailure` reporter does not alert on every DAG tick.

A monitored task instance's sampled CPU time and peak resident set size (RSS)
land in the `resources` object of its task record in the `dag_run` document,
and in the task's statsd sink if one is configured. Task instances do not
appear in the per-job Prometheus families.

Per-task **retries** are DAG-owned (independent of a job's `onFailure.retry`):

```yaml
      - id: load
        command: "..."
        retries: 3                # up to 3 retries -> 4 attempts
        retryDelaySeconds: 30     # wait between attempts
```

## The task state machine

Each task instance moves through:

```text
pending ─▶ running ─▶ success
                   ├▶ up_for_retry ─▶ running (after retryDelaySeconds)
                   ├▶ failed              (retries exhausted)
                   └▶ skipped             (the command exited with a skipExitCodes code)
pending ─▶ upstream_failed   (the trigger rule fails on an upstream failure)
pending ─▶ skipped           (the trigger rule skips on these upstream states)
pending ─▶ skipped           (a when comparison does not hold)
```

A dag_run is `success` after every task is terminal and none failed. It is
`failed` if any task ended `failed` or `upstream_failed`. `skipped` is not a
failure.

A `skipped` task entry records why in `skipReason`, an object with a `kind`
and a `detail` string:

| `kind` | The task was skipped because… | Example `detail` |
| --- | --- | --- |
| `exit_code` | its command exited with one of its `skipExitCodes` | `exit code 99` |
| `upstream` | an upstream was skipped and its rule is `all_success` | `upstream skipped: full-load` |
| `trigger_rule` | its rule skips on the states its upstreams reached | `all_done_min_one_failed: no upstream failed` |
| `approval` | its gate was rejected with `onReject: skip` | `rejected by alice` |
| `condition` | one of its `when` comparisons did not hold | `param mode equals full: the value is incremental` |

## XCom: passing data between tasks

A task publishes a small output under a key. A downstream task reads it. XCom
is a thin, task-keyed convention over the [artifact store](Durable-State),
scoped to the dag_run, driven by the `cronstable xcom` CLI the daemon makes
reachable in every task:

```bash
# in an upstream task:
echo '{"rows": 42}' | cronstable xcom push --key summary
cronstable xcom push --key summary producer_output_file.json    # or from a file

# in a downstream task:
cronstable xcom pull --task upstream_id --key summary            # -> stdout
cronstable xcom pull --task upstream_id --key summary -o out.json
cronstable xcom list                                            # keys in this run
```

Outputs are content-addressed and versioned (newest wins by key). The daemon
injects the run's identity so the CLI needs no arguments beyond the key:
`CRONSTABLE_DAG_NAME`, `CRONSTABLE_DAG_RUN_ID`, `CRONSTABLE_DAG_RUN_KEY`,
`CRONSTABLE_DAG_TASK`, `CRONSTABLE_DAG_TASKKEY`, `CRONSTABLE_DAG_MAP_INDEX`, `CRONSTABLE_DAG_MAP_ITEM`,
`CRONSTABLE_DAG_XCOM_SCOPE`.

## Fan-out: dynamic mapping

A task can **expand** into N parallel instances, one per item of an upstream's
XCom list (Airflow's `.expand()`):

```yaml
      - id: list-work
        command: "echo '[\"a\",\"b\",\"c\"]' | cronstable xcom push --key items"
      - id: process
        dependsOn:
          - list-work
        expand:
          fromTask: list-work      # a direct, non-mapped dependency
          key: items               # its XCom list
        command: "echo processing $CRONSTABLE_DAG_MAP_ITEM (#$CRONSTABLE_DAG_MAP_INDEX)"
```

When `list-work` succeeds, the scheduler reads its `items` list and materializes
`process#0`, `process#1`, `process#2`, each with its own state, retries, and
XCom, and its item in `$CRONSTABLE_DAG_MAP_ITEM`. A downstream task that
lists `process` in `dependsOn` waits for **all** the mapped instances (fan-in). An
empty list resolves the mapped task to `success` immediately.

The expanded item set is recorded **once** in the dag_run and never recomputed,
so a crash-resumed run reconstructs the identical set of mapped instances
rather than re-deriving it from a possibly-changed upstream output.

Because the expansion is permanent, the read that derives it is **strict**
about store trouble. A store that cannot answer (an I/O error or timeout on a
shared mount, a record only a newer node's schema can read) leaves the fan-out
**unknown**: the task stays unexpanded and the scheduler retries the read on a
later pass, regardless of
[`onStoreUnavailable`](Durable-State#when-the-store-is-unavailable-onstoreunavailable).
This deliberately overrides the store's usual
[skip-on-read-error](Durable-State#the-store-model) rule: one blip must not
freeze the task into a permanently empty, vacuously successful fan-out.

The empty fan-out is reserved for a *definitive* answer:

- the upstream finished without publishing the key;
- the published value is not a usable JSON list (invalid JSON, not a list, or
  carrying a non-portable value); or
- the record survives but its payload blob is gone (`410`), possible only
  through external interference with the store, such as a partial restore,
  because GC never sweeps a blob a surviving record references.

A warning names each mapping-to-empty that indicates a problem.

A fan-out is capped at **1000 items**. A larger XCom list fails the mapped task
with an explanatory reason instead of materializing that many instances (its
`all_success` downstream sees `upstream_failed`). A single scheduler pass also
launches at most 32 instances at a time, so a large fan-out ramps up in bounded
bursts rather than one burst of subprocesses.

## Sensors

A `type: sensor` task polls an external condition on a bounded, jittered,
durable schedule instead of running once. Its command's exit code is the
verdict: **0 = condition met** (the task succeeds); nonzero = not yet, poke
again after `pokeIntervalSeconds` plus a random 0 to `pokeJitterSeconds` until
`pokeTimeoutSeconds` elapses, after which the sensor fails.

```yaml
      - id: wait-for-file
        type: sensor
        command: "test -f /data/$(date +%F).ready"
        pokeIntervalSeconds: 60
        pokeTimeoutSeconds: 7200
        pokeJitterSeconds: 10
```

The poke schedule (`nextPokeAt`, `pokeCount`) is durable, so a restart resumes
polling on time rather than restarting the timeout window.

## Approval gates

A `type: approval` task blocks the graph until a human or an API call decides
it. It runs no command. Approve or reject it over the
[control API](HTTP-API#dag-endpoints) (or the [dashboard](Web-Dashboard)):

```bash
curl -X POST .../dags/nightly-etl/runs/<run_key>/tasks/publish-gate/decision \
     -H 'Content-Type: application/json' \
     -d '{"decision": "approve", "by": "alice"}'
```

`approve` succeeds the gate and the graph proceeds. `reject` fails it (or, with
`onReject: skip`, marks it `skipped`, cascading `skipped` to its `all_success`
downstream). The decision (`by`, timestamp) is recorded durably.
[Conditional branching](#conditional-branching) covers the trigger rules that
let a downstream task run after a skipped gate.

You can be paged when a gate begins waiting: configure the
[`notify:` block](Reporting#daemon-event-notifications-notify) with the
`approval_waiting` event. The daemon then fires a reporter (webhook, mail, …)
the first time each gate parks awaiting a decision. A whole DAG run reaching
`failed` similarly fires the `dag_failure` event.

## Conditional branching

A workflow branches when a task ends `skipped` and a later task joins the
branches with a trigger rule. A task is skipped on purpose in one of two
ways. Its command can exit with a code listed in `skipExitCodes`. Or a
[`when:` condition](#conditions-on-parameters-and-xcom-values) can fail to
hold, and then the command never starts.

A task skips itself by exiting with a code listed in its `skipExitCodes`.
The list has no default, so a command that exits `99` fails unless the task
lists `99`. Each code is from `1` to `255`.

```yaml
state:
  path: /var/lib/cronstable/state
dags:
  - name: nightly-load
    schedule: "0 2 * * *"
    tasks:
      - id: extract
        command: ./extract.sh
      - id: full-load
        dependsOn:
          - extract
        command: '[ "$(date +%u)" = 7 ] || exit 99; ./load.sh --full'
        skipExitCodes:
          - 99
      - id: incremental-load
        dependsOn:
          - extract
        command: '[ "$(date +%u)" != 7 ] || exit 99; ./load.sh --incremental'
        skipExitCodes:
          - 99
      - id: publish
        dependsOn:
          - full-load
          - incremental-load
        triggerRule: none_failed_min_one_success
        command: ./publish.sh
      - id: alert
        dependsOn:
          - full-load
          - incremental-load
          - publish
        triggerRule: all_done_min_one_failed
        command: ./page-oncall.sh
```

On Sunday the full load runs and the incremental load skips itself. On every
other day the two swap. `publish` joins the branches, and `alert` is a
failure handler:

| Case | `full-load` | `incremental-load` | `publish` | `alert` | Run |
| --- | --- | --- | --- | --- | --- |
| Sunday | `success` | `skipped` | `success` | `skipped` | `success` |
| Another day | `skipped` | `success` | `success` | `skipped` | `success` |
| Sunday, the full load fails | `failed` | `skipped` | `upstream_failed` | `success` | `failed` |
| Both loads skip | `skipped` | `skipped` | `skipped` | `skipped` | `success` |
| `publish` fails | `success` | `skipped` | `failed` | `success` | `failed` |

Choosing the join's rule:

- `none_failed_min_one_success` runs the join after the branch that ran, and
  skips it when every branch skipped. Use it for a join below alternative
  branches.
- `none_failed` also runs the join when every branch skipped.
- `all_success`, the default, skips the join whenever one branch skips. At
  load, cronstable logs a warning that names an `all_success` task with two
  or more upstreams that can end `skipped`.

A branch longer than one task needs no extra keys. A skipped task cascades
`skipped` to its `all_success` successors, and the join's rule ends the
cascade.

A failure handler uses `all_done_min_one_failed`. It waits for every
upstream to finish, runs when at least one of them failed, and is `skipped`
otherwise. The run verdict keeps its rule: in the third and fifth rows the
handler succeeds and the run is still `failed`, because a task failed. The
`dag_failure` event fires for it, and
[recovery](Workflow-Recovery) can retry the failed task.

How a skip interacts with the rest of a task:

- The skip is decided ahead of `failsWhen`. A command that exits with a
  listed code ends the task `skipped`, whatever `failsWhen` says.
- Only a command that started and exited by itself can skip. A run stopped by
  `executionTimeout` or a cancel is never a skip.
- A skip runs no `verify` step and fires neither `onFailure` nor `onSuccess`.
- A skip is terminal and uses no attempt. A task that failed twice and then
  exits with a listed code ends `skipped`.
- For a sensor, exit `0` means the condition is met, a listed code ends the
  sensor `skipped`, and any other code pokes again.
- An approval gate runs no command, so `skipExitCodes` on a gate is a
  configuration error.
- In a fan-out, each mapped instance decides for itself. A group with skipped
  instances and no failed one reads `skipped`. For
  `none_failed_min_one_success`, such a group counts as a success when at
  least one of its instances succeeded.

A reload applies to a run in progress the way it does for every node field:

- A changed `triggerRule` applies to tasks that are still `pending`.
- Changed `skipExitCodes` apply to instances launched after the reload. An
  instance that is running keeps the codes it launched with.
- A changed `when:` applies to tasks whose condition has no recorded result.
  A task entry that carries `whenMet` keeps its decision.
- A task that is already `skipped` stays `skipped`.

A workflow that sets `skipExitCodes`, or a rule other than `all_success` and
`all_done`, needs run engine level 2. On a store that several nodes share,
upgrade every node before a configuration uses these keys (see
[run engine levels](#run-engine-levels)).

### Conditions on parameters and XCom values

A task with `when:` runs only when every comparison in the list holds. The
scheduler reads the comparisons when the task becomes ready. If one does not
hold, the task ends `skipped` and its command never starts.

```yaml
state:
  path: /var/lib/cronstable/state
dags:
  - name: nightly-load
    params:
      - name: mode
        default: incremental
        allowed:
          - incremental
          - full
    tasks:
      - id: extract
        command: ./extract.sh
      - id: full-load
        dependsOn:
          - extract
        when:
          - param: mode
            equals: full
        command: ./load.sh --full
      - id: incremental-load
        dependsOn:
          - extract
        when:
          - param: mode
            notEquals: full
          - xcom:
              task: extract
              key: row_count
            notIn:
              - "0"
        command: ./load.sh --incremental
      - id: publish
        dependsOn:
          - full-load
          - incremental-load
        triggerRule: none_failed_min_one_success
        command: ./publish.sh
```

A run started with `mode` set to `full` runs `full-load` and skips
`incremental-load`. A run with the default `mode` skips `full-load`. It runs
`incremental-load` unless `extract` published `0` as its `row_count`, and in
that case `publish` is skipped too, because no branch ran.

Each entry names one source and one operator:

| Key | Meaning |
| --- | --- |
| `param` | The name of a [run parameter](#run-parameters) the workflow declares. |
| `xcom` | A map with `task` and `key`: the value that task published with `cronstable xcom push --key KEY`. |
| `equals`, `notEquals` | One value to compare the source with. |
| `in`, `notIn` | A list of values. `in` holds when the source equals one of them, and `notIn` holds when it equals none of them. |

What a comparison reads:

- A parameter comparison reads the value the run stores, in the parameter's
  declared type. Write each comparison value the way you write the
  parameter's `default`. A boolean equals only a boolean, so `true` does not
  equal `1`.
- An XCom comparison reads the published bytes as UTF-8 text, with one
  trailing newline removed. `xcom.task` is a task upstream of this one, and
  it cannot be a mapped task, because a mapped task publishes one value per
  instance. Each comparison value is text of at most 4096 bytes, so quote a
  value such as `"0"`.
- A source with no value fails `equals` and `in`, and passes `notEquals` and
  `notIn`. An XCom source has no value when the task did not publish the
  key, when the published value is larger than 4096 bytes, and when it is
  not UTF-8 text. A parameter has no value in a run that was created before
  the workflow declared it.

The loader checks every entry. A comparison value that the parameter can
never hold is a configuration error, so a misspelled value such as
`equals: ful` fails the load when the parameter's `allowed` list is
`incremental` and `full`.

How a condition is decided:

- The trigger rule is read first. A task below a failed upstream ends
  `upstream_failed`, whatever its condition says.
- An unmet condition uses no attempt and takes no
  [pool](Resource-Pools) slot. It runs no `verify` step and fires neither
  `onFailure` nor `onSuccess`. The task's `skipReason` has the kind
  `condition` and names the comparison, for example
  `param mode equals full: the value is incremental`.
- A met condition is recorded on the task entry as `whenMet` and is not read
  again, so a retry keeps the decision.
- An XCom value is read once the task that published it has finished. If the
  store cannot answer, the task stays `pending` and the read is retried
  after five seconds.
- On a sensor, the condition is read once, before the first poke. On an
  approval gate, an unmet condition skips the gate and nobody is asked to
  decide.
- On a mapped task, the condition is read once for the whole fan-out, before
  it expands. An unmet condition creates no instances.
- A task can set both `when:` and `skipExitCodes`. The condition decides
  whether the command starts, and a command that starts can still skip
  itself.

A workflow that uses `when:` needs run engine level 2 (see
[run engine levels](#run-engine-levels)).

## Run parameters

A workflow declares the values that can differ from run to run under
`params:`. A caller supplies values when it starts a run, cronstable checks
them against the declaration, and every task of the run reads the same
values.

```yaml
state:
  path: /var/lib/cronstable/state
dags:
  - name: deploy
    params:
      - name: target
        type: string
        default: staging
        allowed:
          - staging
          - prod
        description: Environment to deploy to
      - name: batch_size
        type: integer
        default: 500
        minimum: 1
        maximum: 10000
      - name: ticket
        type: string
        required: true
        pattern: "OPS-[0-9]+"
        maxLength: 32
    tasks:
      - id: release
        command: ./release.sh "$CRONSTABLE_PARAM_TARGET" "$CRONSTABLE_PARAM_BATCH_SIZE"
```

### Declaring parameters

| Key | Meaning |
| --- | --- |
| `name` | Required. Starts with a letter and holds at most 64 letters, digits, and underscores. Names are unique within the workflow, ignoring case. |
| `type` | `string` (the default), `integer`, `number`, or `boolean`. |
| `default` | The value a run stores when the caller supplies none. It has to pass the parameter's own constraints. |
| `required` | `true` means the caller supplies the value on every run. A required parameter takes no `default`, and a workflow with a `schedule` cannot have one. |
| `allowed` | The complete list of accepted values, for a `string`, an `integer`, or a `number`. |
| `minimum`, `maximum` | Inclusive bounds for an `integer` or a `number`. |
| `pattern` | A regular expression in Python syntax that the whole string has to match. The daemon evaluates it for each supplied value, so keep it free of nested repetition such as `(a+)+`. |
| `maxLength` | The longest accepted string, in characters, from 1 to 4096. |
| `description` | Text that the dashboard shows under the field. |

Every parameter sets either `default` or `required: true`, so every run of a
workflow stores the same parameter names. A fault in the declaration is a
configuration error at load.

The limits:

- A workflow declares at most 32 parameters.
- A string is at most 4096 bytes of UTF-8 and holds no control character
  and no Unicode line or paragraph separator.
- An integer is from -9007199254740991 to 9007199254740991, the range a JSON
  client reads exactly. A number is finite.
- The values of one run are at most 16 KiB as JSON.

### Supplying values

Send the values as a `params` object when you start a run:

```bash
curl -X POST http://127.0.0.1:8080/dags/deploy/trigger \
     -H "Authorization: Bearer $TOKEN" \
     -d '{"params": {"target": "prod", "ticket": "OPS-4412"}}'
```

- Each value has the declared JSON type. Nothing is coerced, so `"500"` is
  refused for an integer. A form converts its text before it sends.
- A name the workflow does not declare is refused, and a workflow with no
  `params:` accepts no values.
- A refused request creates no run. It answers `400` with the reason for
  each parameter under [`paramErrors`](HTTP-API#post-dagsnametrigger).
- A parameter the caller leaves out takes its `default`.
- A scheduled run and a catch-up run store the defaults.
- A [backfill](#scheduling-catch-up-and-backfill) takes the same `params`
  object and applies it to the runs it creates. A date that already has a
  run keeps that run's values, and the response lists those dates under
  `existingRunKeys`.
- To run a date that already has a run with other values, trigger a manual
  run with `logicalDate` and `params`. It gets its own run key, and the
  scheduled run stays as it is.
- `requestId` makes a trigger repeatable. The run key is derived from it, so
  a repeated request returns the first run with `created: false`. The repeat
  is compared with the stored values after defaults are applied, so it
  answers `409` when a reload changed a default in between.

The [web dashboard](Web-Dashboard#dag-orchestration) opens a form for a workflow
that declares parameters, the [terminal dashboard](Terminal-Dashboard) takes
`name=value` pairs, and the [MCP](MCP) tools `cron_trigger_dag` and
`cron_backfill_dag` take a `params` argument.

With [scoped tokens](HTTP-API#scoped-tokens-webauthtokens), supplying values
needs the `params` scope in addition to `control`. A token that holds
`control` alone starts runs with the declared defaults. Use `allowed` for
every parameter that selects a target, because the declaration bounds what
any caller can choose.

### Reading values in a task

Each parameter reaches every task of the run as an environment variable
named `CRONSTABLE_PARAM_` plus the parameter name in upper case:

| Declared type | Variable text |
| --- | --- |
| `string` | The text |
| `integer`, `number` | The JSON text, such as `500` or `0.5` |
| `boolean` | `true` or `false` |

The variables are applied after the task's own `environment:`, so a
task-level variable of the same name loses. A task receives the parameters
its run stores and no others: a `CRONSTABLE_PARAM_*` variable in the
daemon's own environment is not passed on.

`cronstable param` reads the same values over the
[loopback endpoint](Durable-State#job-facing-state):

```bash
cronstable param get target     # prints one value; exits 4 for an unknown name
cronstable param list           # prints the parameter names
cronstable param dump           # prints every value as one JSON object
```

cronstable never substitutes a value into the command text. Quote the
variable in a POSIX shell (`"$CRONSTABLE_PARAM_TARGET"`), which does not
parse an expanded variable again. `cmd.exe` does parse `%VAR%` again, so a
Windows task reads parameters from PowerShell or with delayed expansion
(`!VAR!`), and its declaration constrains each value with `allowed` or
`pattern`.

### What a run stores

- The run document holds the checked values under `params`, written when the
  run is created. Nothing changes them afterward. A reload that changes the
  declaration leaves every existing run with the values it has. A run that
  was created before a parameter was declared has no variable for it, so
  let in-flight runs finish before a task command starts to read a new
  parameter.
- A run that a trigger or a backfill request created records the label of
  the requesting token under `triggeredBy`.
- [Recovery](Workflow-Recovery#run-parameters) reuses the source run's
  values.
- The daemon log names the parameters a request supplied and never prints a
  value. The built-in report templates carry no parameters. A custom report
  template that renders the task's `environment` includes the
  `CRONSTABLE_PARAM_*` variables.

A run's parameter values are visible to every reader with the `view` scope,
to MCP clients, and in state backups. Keep a secret in the task's
[`secrets:`](Durable-State#run-scoped-secrets) block. When a run needs a
different credential, pass the secret's name as a parameter and let the task
resolve it.

A parameter name that reads as a secret is a configuration error. That is a
name that contains one of these words, in any letter case and with or
without the underscore: `password`, `passwd`, `pwd`, `secret`, `token`,
`credential`, `api_key`, `access_key`, `private_key`, or `rediscli_auth`.

A workflow that declares `params:` needs run engine level 2 (see
[run engine levels](#run-engine-levels)).

## Scheduling, catch-up, and backfill

A scheduled DAG reuses the job [schedule grammar](Schedules-and-Timezones) with
one restriction: the schedule must parse to a cron expression, so `@reboot` is
rejected at config load (`dag 'd': schedule '@reboot' is not a cron
expression; DAG schedules must be cron expressions (@reboot is not supported
for dags)`), although `@daily` / `@hourly`-style aliases still
work.

A scheduled DAG follows the
[catch-up discipline](Durable-State#missed-run-catch-up): `onMissed`
(`skip` / `run-once` / `run-all`) and `startingDeadlineSeconds` bound how many
missed logical dates a restart replays, capped like a job's catch-up.

`catchupJitterSeconds` spreads the replays, with the same checkpointed
at-least-once resume the job engine has. The owed watermark goes into a
`catchup-dag/<dag>` stream (the twin of the job's `catchup/<job>`) before the
jitter offset starts, so a restart during the offset resumes the backfill
rather than losing it. A DAG with no `schedule` is manual-only.

**Backfill** replays a DAG across a historical range on demand. It is a
deliberate operation that ignores the automatic deadline but is still bounded
and idempotent (each date's run is create-if-absent, so re-running a backfill
never duplicates runs):

```bash
curl -X POST .../dags/nightly-etl/backfill \
     -d '{"from": "2026-01-01T00:00:00+00:00", "to": "2026-01-07T00:00:00+00:00"}'
```

The response counts the runs the request created (`created`, `runKeys`)
separately from the dates that already had a run (`existing`,
`existingRunKeys`). A run that already exists is left as it is, whatever its
state. One request covers at most 100 scheduled dates. For a workflow that
declares [run parameters](#run-parameters), the request also takes a `params`
object.

## Crash-resume and the fleet

The durable per-task state, not memory, is the source of truth. A dag_run is
advanced only by the node holding that run's **advance lease**, a
time-to-live (TTL) lease on the shared store renewed while the run is active,
so in a healthy fleet only one node advances a given run and a task does not
launch twice (crash recovery is at-least-once; see the delivery contract
below). The claim that flips a task `pending → running` is a single
atomic compare-and-set on the run document, a correctness backstop underneath
the lease.

The owner also keeps an in-memory record of every task instance it claimed
and was handed to launch. A store write that lands after the owner's own
timeout stopped waiting for it (a stalled disk or network mount) leaves the
task claimed with nothing launched. On its next pass the owner recognizes
that claim, because the record has no entry for it, releases the task back
to `pending` (a sensor back to its idle shape between pokes) and re-claims it
at once. Nothing ran, so the task's attempt count is unchanged. A launch the
owner cancels before its subprocess starts (a `state` section reload while
the task waits behind the spawn gate) is released the same way. The record
forgets a run once the store shows it finished or collected, whichever node
finished it.

If a node crashes, its lease lapses and a peer adopts the run, reconciling from
the durable state:

- a task recorded `running` whose process is gone (a dead pid, or a foreign
  owner proven dead by the lease lapse) is retried if attempts remain, else
  failed;
- a sensor mid-poke is re-poked;
- an approval gate keeps waiting.

This mirrors the job-level
[crash reconciliation](Durable-State#in-flight-runs-and-crash-reconciliation)
seam. Like every cronstable coordination primitive it is **at-least-once**, not
exactly-once: a task whose process outlives a crashed daemon may run again on
resume. A task that must be exactly-once should guard its side effect with an
[idempotency key](Durable-State#idempotency-keys).

### Run engine levels

During an upgrade, the nodes on a shared store run different cronstable
versions. Each build supports workflow features up to a numbered run engine
level. The current level is 2. A workflow needs the highest level any of its
features needs.

| Level | A workflow needs it when… |
| --- | --- |
| 1 | always |
| 2 | a task sets `skipExitCodes`, [`when:`](#conditions-on-parameters-and-xcom-values), or a `triggerRule` other than `all_success` and `all_done`, or the workflow declares [`params:`](#run-parameters) |

- A run document records the level its DAG needed at creation in an `engine`
  field. A document with no `engine` field is at level 1.
- A node adopts and advances only runs at or below its own level. It leaves
  the document of a higher run unchanged, releases any advance lease it holds
  on the run, and logs one warning. The run stays `running` until a node at
  its level adopts it.
- A node refuses to [recover](Workflow-Recovery) a run above its level.
- Each node records its level as `dagEngine` in its
  [manifest](Durable-State#garbage-collection-and-manifests). A manifest with
  no `dagEngine` is at level 1, and the build that wrote it advances any run
  under the rules it knows.
- When another live host's newest manifest is below the level a loaded DAG
  needs, the node logs a warning that names the host, and the DAG's entry in
  `GET /dags` lists the same text under `fleetWarnings`. A host counts as
  live while its newest manifest is less than 12 hours old.

Upgrade every node on a shared store before a configuration uses a feature
that needs a higher level.

## Retention and GC

A dag_run document is durable and, while its DAG is configured, is **not**
swept by the record garbage collector (GC). Instead each DAG keeps its newest
`retainRuns` **terminal** runs (default 50) and prunes the rest, along with
their XCom, on a periodic DAG-owned pass.

A DAG *removed from every config* ages out like a removed job. After it has
been absent from every config and recent manifest for a full
`state.gcGraceSeconds`, the daemon's
[GC pass](Durable-State#garbage-collection-and-manifests) deletes its terminal
run documents (an active run is never touched, so a re-added DAG resumes it)
and its aged XCom streams.

Artifact payload blobs are content-addressed. A blob any surviving record still
references is never swept, so a retained run's XCom can never dangle. Only
blobs no surviving record references, and older than the grace, are reclaimed.

## Inspecting and controlling runs

Over the [HTTP control API](HTTP-API#dag-endpoints):

- `GET /dags`: the configured DAGs and their tasks
- `GET /dags/{name}/runs`: recent runs and their per-task state counts
- `GET /dags/{name}/runs/{run_key}`: one run's full document
- `POST /dags/{name}/trigger`: start a manual run now, with
  [run parameters](#run-parameters) when the workflow declares them
- `POST /dags/{name}/backfill`: replay a date range
- `POST /dags/{name}/runs/{run_key}/tasks/{taskkey}/decision`: approve/reject a gate

The [web dashboard](Web-Dashboard) drives the same endpoints from a DAG
orchestration UI: a DAG card and a per-DAG drawer (runs, tasks, graph, XCom,
logs) with trigger, backfill, and approval decisions. Its page documents that
UI.

See [example/dag/](https://github.com/ptweezy/cronstable/tree/main/example/dag)
for a complete configuration exercising every node type.
