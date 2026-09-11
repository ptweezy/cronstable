"""Plan selective DAG replays while preserving the source run."""

import copy
import hashlib
import json
from dataclasses import asdict

from cronstable import dag


class RecoveryError(Exception):
    pass


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def configuration_revision(config):
    tasks = []
    for task in config.tasks:
        job = task.job_template
        tasks.append(
            {
                "spec": asdict(task.spec),
                "launch": {
                    key: getattr(job, key)
                    for key in (
                        "command",
                        "shell",
                        "environment",
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
                },
            }
        )
    return digest(sorted(tasks, key=lambda task: task["spec"]["id"]))


def plan(config, source, *, mode="failed", tasks=(), artifacts=()):
    if not dag.is_terminal_run(source):
        raise RecoveryError("recovery requires a finished run")
    if mode not in ("failed", "from"):
        raise RecoveryError("mode must be 'failed' or 'from'")
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
