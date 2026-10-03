"""Plan selective DAG replays while preserving the source run."""

import copy
import hashlib
import json
from dataclasses import asdict

from cronstable import dag
from cronstable import params as run_params


class RecoveryError(Exception):
    pass


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


_LAUNCH_KEYS = (
    "command",
    "shell",
    "workingDirectory",
    "user",
    "group",
    "executionTimeout",
    "killTimeout",
    "failsWhen",
    "verify",
    "pool",
    "poolSlots",
    "queueTimeout",
    "queuePriority",
)


_SPEC_KEYS = (
    "id",
    "type",
    "depends_on",
    "trigger_rule",
    "max_attempts",
    "retry_delay",
    "expand",
    "poke_interval",
    "poke_timeout",
    "poke_jitter",
    "on_reject",
)

# TaskSpec fields the digest reads only when a task sets them, keyed by
# field name with the default that is left out. A field added here leaves
# the revision of every DAG that does not set it unchanged.
_OPTIONAL_SPEC_KEYS = {"skip_exit_codes": (), "when": ()}


def configuration_revision(config):
    """Digest a DAG's launch configuration.

    View readers see the digest, so environment contributes variable names
    only, as in the job-set ID.

    The digest reads the task fields named in ``_SPEC_KEYS``, plus each
    ``_OPTIONAL_SPEC_KEYS`` field a task sets to something other than its
    default. A ``TaskSpec`` field in neither list leaves every stored
    revision as it is.

    A DAG that declares run parameters also digests the declaration, minus
    each description. The task list is wrapped in an object only for such a
    DAG, so every other DAG keeps the digest of the bare list.
    """
    tasks = []
    for task in config.tasks:
        job = task.job_template
        launch = {key: getattr(job, key) for key in _LAUNCH_KEYS}
        launch["environment"] = sorted(e["key"] for e in job.environment)
        spec = {key: getattr(task.spec, key) for key in _SPEC_KEYS}
        if spec["expand"] is not None:
            spec["expand"] = asdict(spec["expand"])
        for key, default in _OPTIONAL_SPEC_KEYS.items():
            value = getattr(task.spec, key)
            if value != default:
                spec[key] = value
        if "when" in spec:
            spec["when"] = [asdict(cond) for cond in spec["when"]]
        tasks.append({"spec": spec, "launch": launch})
    tasks.sort(key=lambda task: task["spec"]["id"])
    if not config.spec.params:
        return digest(tasks)
    declared = []
    for param in config.spec.params:
        entry = asdict(param)
        del entry["description"]
        declared.append(entry)
    return digest({"tasks": tasks, "params": declared})


def plan(config, source, *, mode="failed", tasks=(), artifacts=()):
    if not dag.is_terminal_run(source):
        raise RecoveryError("recovery requires a finished run")
    if not dag.supports_run(source):
        raise RecoveryError(
            "the run needs run engine level {!r} and this node supports "
            "level {}; recover it from an upgraded node".format(
                source.get("engine"), dag.ENGINE_LEVEL
            )
        )
    if mode not in ("failed", "from"):
        raise RecoveryError("mode must be 'failed' or 'from'")
    # A recovery run reuses the source's parameters as they are, so they
    # have to be a complete, valid map under the current declaration.
    stored = source.get("params")
    if not isinstance(stored, dict):
        stored = {}
    refused = run_params.check_stored(config.spec.params, stored)
    if refused:
        raise RecoveryError(
            "the source run's parameters do not fit the current "
            "declaration ({}); create a full run instead".format(
                "; ".join(
                    "{} {}".format(name, reason)
                    for name, reason in sorted(refused.items())
                )
            )
        )
    entries = source["tasks"]
    if {e["id"] for e in entries.values()} != set(config.spec.by_id):
        raise RecoveryError(
            "task definitions differ; create a full run for the current graph"
        )
    for task in config.spec.tasks:
        if bool(task.expand) != bool(entries[task.id].get("mapped")):
            raise RecoveryError(
                "task mapping differs; create a full run for the current graph"
            )
    if mode == "failed":
        if tasks:
            raise RecoveryError("tasks is only valid for mode 'from'")
        selected = {
            key
            for key, e in entries.items()
            if e["state"] in dag.FAILURE_STATES
        }
    else:
        if not tasks or any(key not in entries for key in tasks):
            raise RecoveryError("select at least one existing task instance")
        selected = set(tasks)
        for key in list(selected):
            if entries[key]["state"] == dag.EXPANDED:
                selected.update(k for k in entries if k.startswith(key + "#"))
    if not selected:
        raise RecoveryError("no tasks to recover")
    unknown = {entries[k]["id"] for k in selected} - config.spec.by_id.keys()
    if unknown:
        raise RecoveryError("selected tasks are absent from the configuration")
    affected = {entries[k]["id"] for k in selected}
    downstream = set()
    while True:
        more = {
            t.id
            for t in config.spec.tasks
            if set(t.depends_on) & affected and t.id not in affected
        }
        if not more:
            break
        downstream.update(more)
        affected.update(more)
    selected.update(k for k, e in entries.items() if e["id"] in downstream)
    reset = {
        t.id
        for t in config.spec.mapped_tasks
        if t.expand.from_task in affected
    }
    selected.update(k for k, e in entries.items() if e["id"] in reset)
    preserved = sorted(k for k in entries if k not in selected)
    kept_artifacts = [
        {key: record.get(key) for key in ("name", "sha256", "size", "meta")}
        for record in artifacts
        if str(record.get("name", "")).partition("/")[0] in preserved
    ]
    revision = configuration_revision(config)
    result = {
        "sourceRunKey": source["runKey"],
        "sourceRunId": source["runId"],
        "mode": mode,
        "requestedTasks": sorted(tasks),
        "tasks": sorted(selected),
        "preserved": preserved,
        "resetMappings": sorted(reset),
        "artifacts": kept_artifacts,
        "configurationRevision": revision,
        "sourceConfigurationRevision": source.get("configurationRevision"),
        "configurationChanged": source.get("configurationRevision")
        != revision,
    }
    # A reused skipped task stays skipped, so the preview names each one
    # with its recorded reason (None on a run that recorded none).
    skipped = {
        key: entries[key].get("skipReason")
        for key in preserved
        if entries[key]["state"] == dag.SKIPPED
    }
    if skipped:
        result["preservedSkipped"] = skipped
    if config.spec.params:
        # the values the recovery run reuses
        result["params"] = stored
    result["planToken"] = digest({"plan": result, "source": source})
    return result


def new_run(config, source, plan, now):
    token = plan["planToken"]
    run_key = "recovery-" + token
    body = dag.new_run_body(
        dag=config.name,
        run_key=run_key,
        run_id=token,
        logical_date=source.get("logicalDate"),
        kind="recovery",
        now=now,
        spec=config.spec,
        params=copy.deepcopy(plan["params"]) if "params" in plan else None,
    )
    selected = set(plan["tasks"])
    reset = set(plan["resetMappings"])
    for key, mapping in source.get("mapped", {}).items():
        if key in config.spec.by_id and key not in reset:
            body["mapped"][key] = copy.deepcopy(mapping)
    for key, entry in source["tasks"].items():
        task_id = entry["id"]
        if task_id not in config.spec.by_id or task_id in reset:
            continue
        if key not in selected:
            preserved = copy.deepcopy(entry)
            preserved["reusedFrom"] = source["runKey"]
            body["tasks"][key] = preserved
        elif "#" in key:
            fresh = dag._new_task_entry(config.spec.by_id[task_id], now)
            fresh.pop("mapped", None)
            fresh["mapIndex"] = entry["mapIndex"]
            fresh["mapItem"] = entry.get("mapItem")
            body["tasks"][key] = fresh
        elif entry["state"] == dag.EXPANDED:
            body["tasks"][key] = copy.deepcopy(entry)
    body["configurationRevision"] = plan["configurationRevision"]
    body["recovery"] = {**copy.deepcopy(plan), "status": "preparing"}
    return body
