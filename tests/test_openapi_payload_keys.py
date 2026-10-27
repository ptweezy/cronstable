"""Every key a payload builder emits is a declared property of its schema.

docs/openapi.yaml types the response objects of the routes below as open
schemas (``additionalProperties: true``): a generated client keeps decoding
a key the spec does not name, but only as an untyped extra, so a field the
builders grow without a spec edit silently stays out of the client's typed
model.  This file builds each payload from a fixture daemon and walks it
against its schema, nested objects included, and fails on a key the schema
does not declare.  It also fails on a ``required`` key the payload lacks.

The walk is a subset check, never equality, the same rule as
``test_limits_objects_match_the_payloads_they_describe`` in
tests/test_openapi.py: a schema may declare keys these fixtures never emit
(ones a configuration they do not set up adds).  Each fixture asserts the
conditional keys it sets up, so a walk cannot pass by finding nothing.
Payloads whose shape their writer chooses (state records, fan-out items,
cursor values) are open objects in the spec, and the walk stops at them.
"""

import copy
import datetime
import json
import os
import pathlib
import sys
import types

from strictyaml.ruamel import YAML

from cronstable import jobstate
from cronstable.backends.etcd import EtcdBackend
from cronstable.backends.filesystem import FilesystemBackend
from cronstable.backends.kubernetes import KubernetesBackend
from cronstable.cluster import (
    STATUS_AGREED,
    STATUS_UNREACHABLE,
    ClusterManager,
    _parse_job_summaries,
    _parse_node_stats,
)
from cronstable.config import DEFAULT_CLUSTER, parse_config_string
from cronstable.cron import JobRunInfo
from cronstable.dag import xcom_name, xcom_scope
from cronstable.job import JobOutputStream, JobRetryState
from cronstable.limits import LimitsRecord
from cronstable.resources import ResourceMonitor, ResourceUsage
from cronstable.state import Lease
from tests._helpers import _drain_pending
from tests.conftest import Req, _cron
from tests.test_state_dag_run import _drive, _set_cmd

_SPEC = YAML(typ="safe").load(
    (pathlib.Path(__file__).parent.parent / "docs" / "openapi.yaml").read_text(
        encoding="utf-8"
    )
)
_SCHEMAS = _SPEC["components"]["schemas"]
_UTC = datetime.timezone.utc
_PY = sys.executable


# ---------------------------------------------------------------------------
# the schema walk
# ---------------------------------------------------------------------------


def _resolve(schema):
    while "$ref" in schema:
        schema = _SCHEMAS[schema["$ref"].rsplit("/", 1)[-1]]
    return schema


def _parts(schema):
    """The schema and every schema it composes, each ``$ref`` followed."""
    schema = _resolve(schema)
    out = [schema]
    for key in ("allOf", "anyOf", "oneOf"):
        for part in schema.get(key) or ():
            out.extend(_parts(part))
    return out


def _walk(schema, value, where, problems):
    parts = _parts(schema)
    if isinstance(value, dict):
        props: dict = {}
        required: set = set()
        maps = []
        for part in parts:
            props.update(part.get("properties") or {})
            required.update(part.get("required") or ())
            extra = part.get("additionalProperties")
            if isinstance(extra, dict):
                maps.append(extra)
        if props:
            for key in sorted(required - value.keys()):
                problems.append(
                    "{}: lacks required key {!r}".format(where, key)
                )
        for key, sub in value.items():
            path = "{}.{}".format(where, key)
            if key in props:
                _walk(props[key], sub, path, problems)
            elif maps:
                for typed in maps:
                    _walk(typed, sub, "{}[{!r}]".format(where, key), problems)
            elif props:
                problems.append("{}: undeclared key".format(path))
    elif isinstance(value, list):
        for part in parts:
            if "items" in part:
                for i, sub in enumerate(value):
                    _walk(
                        part["items"], sub, "{}[{}]".format(where, i), problems
                    )


def _declares(schema, value, where):
    """Assert that ``schema`` declares every key of ``value``, recursively.

    ``schema`` is a component name or a schema object.  ``value`` goes
    through a JSON round trip first, so the walk sees what the wire carries.
    """
    if isinstance(schema, str):
        schema = {"$ref": "#/components/schemas/" + schema}
    problems: list[str] = []
    _walk(schema, json.loads(json.dumps(value)), where, problems)
    assert not problems, "docs/openapi.yaml and the payload disagree:\n  " + (
        "\n  ".join(problems)
    )


def _route(path, method="get", status="200"):
    op = _SPEC["paths"][path][method]
    return op["responses"][status]["content"]["application/json"]["schema"]


def _body(resp):
    return json.loads(resp.text)


# ---------------------------------------------------------------------------
# /jobs entries
# ---------------------------------------------------------------------------

_JOBS_YAML = """
jobs:
  - name: plain
    command: echo hi
    schedule: "*/5 * * * *"
  - name: hashed
    command: echo hi
    schedule: "H * * * *"
  - name: flaky
    command: 'false'
    schedule: "*/5 * * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 5
        maximumDelay: 60
        backoffMultiplier: 2
  - name: watched
    command: echo hi
    schedule: "*/5 * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 60
      maxRuntimeSeconds: 300
  - name: boot
    command: echo hi
    schedule: "@reboot"
  - name: clustered
    command: echo hi
    schedule: "*/5 * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Forbid
  - name: leader
    command: echo hi
    schedule: "*/5 * * * *"
    clusterPolicy: Leader
  - name: prefer
    command: echo hi
    schedule: "*/5 * * * *"
    clusterPolicy: PreferLeader
  - name: everywhere
    command: echo hi
    schedule: "*/5 * * * *"
    clusterPolicy: EveryNode
  - name: quiet
    command: echo hi
    schedule: "*/5 * * * *"
    priority: idle
  - name: capped
    command: echo hi
    schedule: "*/5 * * * *"
    resourceLimits:
      cpu: "1"
  - name: checked
    command: echo hi
    schedule: "*/5 * * * *"
    verify:
      command: echo ok
  - name: busy
    command: echo hi
    schedule: "*/5 * * * *"
    monitorResources: true
"""


class _Running:
    """A running instance as the job builders read one.

    With ``monitor``, a real :class:`ResourceMonitor`, the live readings are
    that monitor's, read the way ``RunningJob`` reads its own.  Otherwise
    ``sample`` and ``series`` stand in for them.
    """

    verification = None

    def __init__(self, pid, sample=None, series=None, monitor=None):
        self.proc = types.SimpleNamespace(pid=pid)
        self.started_at = datetime.datetime.now(_UTC)
        self._sample = sample
        self._series = series
        self._monitor = monitor

    def live_resources(self):
        if self._monitor is not None:
            return self._monitor.snapshot()
        return self._sample

    def live_resource_series(self):
        if self._monitor is not None:
            return self._monitor.series()
        return self._series


def _usage(series=None):
    return ResourceUsage(
        cpu_user_seconds=1.25,
        cpu_system_seconds=0.5,
        max_rss_bytes=48_185_344,
        samples=3,
        series=series,
    )


def _finished(outcome, *, seconds=1.0, usage=None, limits=None, skip=None):
    now = datetime.datetime.now(_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=None if outcome == "skipped" else 0,
        started_at=None
        if outcome == "skipped"
        else now - datetime.timedelta(seconds=seconds),
        finished_at=now,
        fail_reason=None,
        output=JobOutputStream(),
        resource_usage=usage,
        skip_reason=skip,
        limits=limits,
    )


def _limits_record():
    return LimitsRecord(
        cpu=1.0,
        memory_bytes=None,
        processes=None,
        required=False,
        enforced=True,
        mechanism="cgroup-v2",
        unenforced=(),
        reason=None,
    )


async def test_job_entries_declare_every_key():
    cron = _cron(_JOBS_YAML)
    now = datetime.datetime.now(_UTC)
    retry = JobRetryState(5.0, 2.0, 60.0)
    retry.count = 1
    retry.next_retry_at = now + datetime.timedelta(seconds=5)
    retry.scheduled_delay = 5.0
    cron.retry_state["flaky"] = retry
    cron._sla_state[("watched", "maxTimeSinceSuccess")] = now
    cron._pending_reboot_jobs["boot"] = cron.cron_jobs["boot"]
    cron._slot_leases["clustered"] = Lease(
        "slots/clustered", "host-a#tok", 3, 9e18
    )
    cron._slot_refs["clustered"] = 1
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="spread",
        job_owner=lambda name: None,
        available_job_owner=lambda name: "node-b",
    )
    cron.running_jobs["busy"] = [
        _Running(
            4242,
            {"cpu_percent": 12.5, "cpu_seconds": 0.75, "rss_bytes": 3_260_416},
            None,
        )
    ]
    cron.run_history["plain"].append(_finished("success", usage=_usage()))
    cron.last_run["plain"] = cron.run_history["plain"][-1]
    await cron.pause_job_by_name("plain", duration=600, note="maintenance")

    jobs = cron.jobs_payload()
    await cron._attach_job_queues(jobs)
    by_name = {job["name"]: job for job in jobs}
    for job in jobs:
        _declares("Job", job, "/jobs[{}]".format(job["name"]))
    _declares("Job", cron.job_detail_payload("watched"), "/jobs/watched")

    # each conditional key the fixtures set up is on the wire
    assert "schedule_resolved" in by_name["hashed"]
    assert by_name["busy"]["running_resources"]["instances"] == 1
    assert by_name["flaky"]["retry"]["attempt"] == 1
    assert by_name["watched"]["sla"]["breaches"]
    assert by_name["boot"]["rebootPending"] is True
    assert by_name["clustered"]["concurrencyScope"] == "cluster"
    assert by_name["clustered"]["slot"]["held"] is True
    assert by_name["leader"]["clusterOwner"] is None
    assert by_name["prefer"]["clusterOwner"] == "node-b"
    assert "clusterOwner" not in by_name["everywhere"]
    assert by_name["everywhere"]["clusterPolicy"] == "EveryNode"
    assert by_name["quiet"]["priority"] == "idle"
    assert "limits" in by_name["capped"]
    assert by_name["checked"]["verification"]["configured"] is True
    assert by_name["plain"]["paused"]["note"] == "maintenance"
    assert by_name["plain"]["last_run"]["resources"]["samples"] == 3


async def test_job_runs_and_stats_declare_every_key():
    cron = _cron(_JOBS_YAML)
    history = cron.run_history["plain"]
    history.append(_finished("success", usage=_usage([[1.0, 5.0, 1024]])))
    history.append(
        _finished("failure", usage=_usage(), limits=_limits_record())
    )
    history.append(_finished("skipped", skip="paused"))
    history.append(_finished("cancelled"))
    body = _body(await cron._web_job_runs(Req(match={"name": "plain"})))
    _declares("JobRuns", body, "/jobs/plain/runs")
    runs = body["runs"]
    assert any("ranAt" in run for run in runs)
    assert any(run["skip_reason"] == "paused" for run in runs)
    assert any("limits" in run for run in runs)
    stats = body["stats"]
    for key in ("avg_cpu_seconds", "max_rss_bytes", "last_rss_bytes"):
        assert key in stats
    # a mean over bytes, so a client must read it as a number
    assert stats["avg_rss_bytes"] is not None


async def test_job_resources_declare_every_key():
    cron = _cron(_JOBS_YAML)
    series = [[1.0, 0.0, 2_211_840], [2.0, 0.49, 3_276_800]]
    # a real monitor on this test process, so `current` and the live series
    # are what ResourceMonitor.snapshot() and series() build; _sample() is
    # the direct-call path its docstring names
    monitor = ResourceMonitor(os.getpid(), interval=3600)
    monitor.start()
    try:
        assert monitor.available
        monitor._sample()
        cron.running_jobs["busy"] = [
            _Running(os.getpid(), monitor=monitor),
            _Running(16421, None, series),
        ]
        cron.run_history["busy"].append(
            _finished("success", usage=_usage(series))
        )
        cron.run_history["busy"].append(
            _finished("failure", limits=_limits_record())
        )
        body = _body(
            await cron._web_job_resources(Req(match={"name": "busy"}))
        )
    finally:
        await monitor.stop()
    _declares(_route("/jobs/{name}/resources"), body, "/jobs/busy/resources")
    live = body["live"][0]
    assert live["pid"] == os.getpid()
    assert live["current"]["rss_bytes"] > 0 and live["series"]
    assert body["live"][1]["current"] is None
    assert body["runs"][0]["resources"]["series"] == series


# ---------------------------------------------------------------------------
# DAGs, run documents, XCom, decisions, recovery, and pools
# ---------------------------------------------------------------------------

_DAG_YAML = """
pools:
  disks:
    slots: 1
jobs:
  - name: hog
    command: ignored
    schedule: "@reboot"
    pool: disks
  - name: waiter
    command: ignored
    schedule: "@reboot"
    pool: disks
dags:
  - name: shape
    tasks:
      - id: gen
        command: x
        verify:
          command: x
      - id: work
        command: x
        monitorResources:
          interval: 0.1
        dependsOn:
          - gen
        expand:
          fromTask: gen
          key: items
      - id: probe
        type: sensor
        command: x
        pokeIntervalSeconds: 0
      - id: flaky
        command: x
        retries: 1
        retryDelaySeconds: 0
      - id: gate
        type: approval
        dependsOn:
          - work
          - probe
      - id: tidy
        command: x
        triggerRule: all_done
        dependsOn:
          - gate
          - flaky
  - name: pooled
    tasks:
      - id: wait
        command: x
        pool: disks
"""


async def test_dag_payloads_declare_every_key(dag_cron, tmp_path, monkeypatch):
    cron = await dag_cron(_DAG_YAML, web=True)
    # this test drives admission itself, so no background pool service
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    items = tmp_path / "items.json"
    items.write_text('["alpha", "beta"]')
    push = [_PY, "-m", "cronstable", "xcom", "push", "--key", "items"]
    _set_cmd(cron, "shape", "gen", push + [str(items)])
    cron.cron_dags["shape"].task_templates["gen"].verify = {
        "command": [_PY, "-c", "pass"],
        "timeout": 60.0,
    }
    for task in ("probe", "tidy", "wait"):
        dag = "pooled" if task == "wait" else "shape"
        _set_cmd(cron, dag, task, [_PY, "-c", "pass"])
    # long enough for the 0.1 s sampler to record the instances
    _set_cmd(
        cron, "shape", "work", [_PY, "-c", "import time; time.sleep(0.4)"]
    )
    _set_cmd(cron, "shape", "flaky", [_PY, "-c", "raise SystemExit(3)"])

    # hold the pool's only slot, and queue a job and a DAG task behind it
    held_entry = await cron._pools.enqueue(cron.cron_jobs["hog"])
    held = await cron._pools.acquire("disks", held_entry["id"])
    await cron._pools.enqueue(cron.cron_jobs["waiter"])
    pooled_key = _body(
        await cron._web_dag_trigger(Req(match={"name": "pooled"}, body={}))
    )["runKey"]
    await _drain_pending(cron)

    jobs = cron.jobs_payload()
    await cron._attach_job_queues(jobs)
    for job in jobs:
        _declares("Job", job, "/jobs[{}]".format(job["name"]))
    waiter = next(job for job in jobs if job["name"] == "waiter")
    assert waiter["pool"]["queued"][0]["digest"]
    pools = _body(await cron._web_pools(Req()))
    _declares(_route("/pools"), pools, "/pools")
    queued_doc = _body(
        await cron._web_dag_run(
            Req(match={"name": "pooled", "run_key": pooled_key})
        )
    )
    _declares("DagRunDocument", queued_doc, "pooled run")
    assert queued_doc["tasks"]["wait"]["queued"]["pool"] == "disks"
    await cron._pools.finish(held)

    # drive the shape run to its approval gate
    key = _body(
        await cron._web_dag_trigger(Req(match={"name": "shape"}, body={}))
    )["runKey"]
    waiting = await _drive(cron, "shape", key)
    tasks = waiting["tasks"]
    assert tasks["gate"]["awaitingApproval"] is True
    assert tasks["work"]["state"] == "expanded" and tasks["work"]["mapped"]
    assert tasks["work#1"]["mapItem"] == "beta"
    assert "firstPokeAt" in tasks["probe"] and tasks["probe"]["pokeCount"]
    assert tasks["flaky"]["state"] == "failed"
    assert "nextRetryAt" in tasks["flaky"]
    assert tasks["work#0"]["resources"]["samples"] > 0
    assert tasks["gen"]["verification"]["outcome"] == "success"
    assert waiting["mapped"]["work"]["items"] == ["alpha", "beta"]
    served = _body(
        await cron._web_dag_run(Req(match={"name": "shape", "run_key": key}))
    )
    _declares("DagRunDocument", served, "run document at the gate")

    # beside gen's text value, a value that is not UTF-8 and one over the
    # 65536-byte inline cap, so the walk sees every entry shape
    scope = xcom_scope("shape", waiting["runId"])
    backend = cron.state_backend
    await jobstate.artifact_put(
        backend, scope, xcom_name("probe", "raw"), b"\xff\xfe"
    )
    await jobstate.artifact_put(
        backend, scope, xcom_name("probe", "big"), b"x" * 65537
    )
    xcom = _body(
        await cron._web_dag_xcom(Req(match={"name": "shape", "run_key": key}))
    )
    _declares("DagXcomPage", xcom, "xcom")
    entries = xcom["entries"]
    assert any("value" in entry for entry in entries)
    assert any(entry.get("binary") for entry in entries)
    assert any(entry.get("oversize") for entry in entries)

    decided = _body(
        await cron._web_dag_decision(
            Req(
                match={"name": "shape", "run_key": key, "taskkey": "gate"},
                body={"decision": "approve", "by": "tester"},
            )
        )
    )
    _declares("DagDecision", decided, "decision")
    assert decided == {"ok": True, "state": "success"}
    finished = await _drive(cron, "shape", key)
    assert finished["state"] == "failed"
    assert finished["tasks"]["gate"]["approval"]["decision"] == "approved"
    _declares("DagRunDocument", finished, "finished run document")

    dags = _body(await cron._web_list_dags(Req()))
    for entry in dags:
        _declares("Dag", entry, "/dags[{}]".format(entry["name"]))
    shape = next(entry for entry in dags if entry["name"] == "shape")
    assert shape["latestRun"]["runKey"] == key
    assert shape["totalRuns"] == 1 and "runCounts" in shape
    rules = {task["id"]: task for task in shape["tasks"]}
    assert rules["tidy"]["triggerRule"] == "all_done"
    assert rules["flaky"]["retries"] == 1 and rules["work"]["mapped"] is True
    runs = _body(await cron._web_dag_runs(Req(match={"name": "shape"})))
    _declares(_route("/dags/{name}/runs"), runs, "/dags/shape/runs")
    assert "createdAt" in runs["runs"][0] and "updatedAt" in runs["runs"][0]

    # selective recovery: preview, execute, and the run it creates
    preview = _body(
        await cron._web_dag_recover(
            Req(match={"name": "shape", "run_key": key}, body={})
        )
    )
    _declares("RecoveryRunResult", preview, "recovery preview")
    assert preview["dryRun"] is True and preview["artifacts"]
    executed = _body(
        await cron._web_dag_recover(
            Req(
                match={"name": "shape", "run_key": key},
                body={"dryRun": False, "planToken": preview["planToken"]},
            )
        )
    )
    _declares("RecoveryRunResult", executed, "recovery execution")
    assert executed["created"] is True
    # a client that retries the POST gets the accepted run back
    again = _body(
        await cron._web_dag_recover(
            Req(
                match={"name": "shape", "run_key": key},
                body={"dryRun": False, "planToken": preview["planToken"]},
            )
        )
    )
    _declares("RecoveryRunResult", again, "recovery re-execution")
    assert again["created"] is False
    assert again["runKey"] == executed["runKey"]
    recovered = await cron._dag.get_run("shape", executed["runKey"])
    _declares("DagRunDocument", recovered, "recovery run document")
    assert recovered["recovery"]["status"]
    assert any("reusedFrom" in entry for entry in recovered["tasks"].values())
    assert "configurationRevision" in recovered
    # a failed scheduled run inside the window, which the range selects
    dated = copy.deepcopy(finished)
    dated.update(
        runKey="date-2",
        runId="date-2",
        logicalDate="2026-09-02T00:00:00+00:00",
        state="failed",
    )
    await cron.state_backend.mutate_document(
        cron._dag._ns("shape"), "date-2", lambda _, b=dated: (b, None)
    )
    window = {"from": "2026-09-01T00:00:00+00:00", "to": "2026-09-03"}
    dry = _body(
        await cron._web_dag_recover_range(
            Req(match={"name": "shape"}, body=dict(window))
        )
    )
    _declares("RecoveryRangeResult", dry, "range preview")
    done = _body(
        await cron._web_dag_recover_range(
            Req(
                match={"name": "shape"},
                body=dict(window, dryRun=False, planToken=dry["planToken"]),
            )
        )
    )
    _declares("RecoveryRangeResult", done, "range execution")
    assert [plan["sourceRunKey"] for plan in dry["plans"]] == ["date-2"]
    assert len(done["runs"]) == 1 and done["complete"] is True


# ---------------------------------------------------------------------------
# /state, /state/documents, /state/records
# ---------------------------------------------------------------------------

_STATE_YAML = """
jobs:
  - name: tick
    command: echo hi
    schedule: "*/5 * * * *"
"""


async def test_state_payloads_declare_every_key(dag_cron, monkeypatch):
    stateless = _cron(_STATE_YAML)
    off = _body(await stateless._web_state(Req()))
    _declares("StateInventory", off, "/state without a store")
    assert off == {"enabled": False}

    cron = await dag_cron(_STATE_YAML, web=True)
    backend = cron.state_backend
    await jobstate.kv_set(backend, "heartbeat", "beats", "x" * 7)
    await jobstate.cursor_advance(backend, "meter", "rows", 275_257)
    await jobstate.idempotency_claim(backend, "journal", "once")
    await jobstate.idempotency_claim(backend, "journal", "lapsing", ttl=600)
    await backend.append_record(
        "runs/tick",
        _finished("success", usage=_usage([[1.0, 2.0, 3]])).to_dict(
            include_series=True
        ),
    )
    assert await backend.acquire_lease("lock/demo", "host-a#tok", 60)
    retry = JobRetryState(5.0, 2.0, 60.0)
    retry.count = 2
    retry.next_retry_at = datetime.datetime.now(_UTC)
    retry.scheduled_delay = 10.0
    cron.retry_state["tick"] = retry
    cron._slot_leases["tick"] = Lease("slots/tick", "host-a#tok", 4, 9e18)
    cron._slot_refs["tick"] = 1

    inventory = _body(await cron._web_state(Req()))
    _declares("StateInventory", inventory, "/state")
    assert inventory["enumerable"] is True
    assert inventory["records"]["runs"]["scopes"][0]["scope"] == "tick"
    assert inventory["documents"]["kv"]["count"] == 1
    assert "lock/demo" in [lease["name"] for lease in inventory["leases"]]
    assert inventory["node"]["retries"][0]["job"] == "tick"
    assert inventory["node"]["slots"][0]["slot"] == "tick"
    assert inventory["stats"]["ops"] and inventory["stats"]["workers"]
    assert "path" in inventory["view"]

    async def wedged():
        raise TimeoutError("store is not answering")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "inventory", wedged)
        degraded = _body(await cron._web_state(Req()))
    _declares("StateInventory", degraded, "/state degraded")
    assert degraded["enumerable"] is False

    pages = {}
    for ns in ("kv/heartbeat", "cursor/meter", "idem/journal"):
        page = _body(await cron._web_state_documents(Req(query={"ns": ns})))
        _declares("StateDocumentsPage", page, "/state/documents?ns=" + ns)
        pages[ns] = page["documents"]
    assert pages["kv/heartbeat"][0]["valueType"] == "str"
    assert pages["cursor/meter"][0]["value"] == 275_257
    assert any("expiresAt" in doc for doc in pages["idem/journal"])

    records = _body(
        await cron._web_state_records(Req(query={"stream": "runs/tick"}))
    )
    _declares("StateRecordsPage", records, "/state/records")
    assert records["records"][0]["resources"]["series"]


# ---------------------------------------------------------------------------
# /cluster and /fleet
# ---------------------------------------------------------------------------

_CLUSTER_JOBS = """
jobs:
  - name: alpha
    command: echo hi
    schedule: "*/5 * * * *"
  - name: beta
    command: echo hi
    schedule: "@reboot"
"""


def _gossip_manager(monkeypatch):
    # constructible without certificates, as tests/test_cluster.py's no_tls
    monkeypatch.setattr(
        "cronstable.cluster.build_client_ssl_context", lambda tls: None
    )
    monkeypatch.setattr(
        "cronstable.cluster.build_server_ssl_context", lambda tls: None
    )
    cfg = dict(DEFAULT_CLUSTER)
    cfg.update(
        {
            "listen": "127.0.0.1:1",
            "tls": {"ca": "ca", "cert": "cert", "key": "key"},
            "peers": [{"host": "b:8443"}, {"host": "c:8443"}],
            "nodeName": "node-a",
            "interval": 3600,
            "connectTimeout": 5,
            "distribution": "spread",
            "electLeader": True,
        }
    )
    return ClusterManager(cfg, lambda: "v1:mine")


async def test_cluster_and_fleet_payloads_declare_every_key(
    monkeypatch, tmp_path
):
    cron = _cron(_CLUSTER_JOBS)
    cron.run_history["alpha"].append(_finished("success"))
    cron.last_run["alpha"] = cron.run_history["alpha"][-1]
    mgr = _gossip_manager(monkeypatch)
    mgr.set_job_summaries_provider(cron.fleet_job_summaries)
    mgr.set_node_stats_provider(cron.node_resource_snapshot, True)
    now = datetime.datetime.now(_UTC)
    agreed = mgr.view.peers["b:8443"]
    agreed.status = STATUS_AGREED
    agreed.node_name = "node-b"
    agreed.instance_id = "inst-b"
    agreed.job_set_id = "v1:mine"
    agreed.last_seen = now
    # a peer's load and job summaries arrive validated, bytes as floats
    agreed.node_stats = _parse_node_stats(
        {"cpu_percent": 3.5, "cpu_count": 8, "mem_used_bytes": 2_090_278_912}
    )
    agreed.node_stats_at = now
    agreed.job_summaries = _parse_job_summaries(
        {
            "alpha": {
                "running": False,
                "enabled": True,
                "scheduled_in": 120.0,
                "last": {
                    "outcome": "failure",
                    "finished_at": now.isoformat(),
                    "duration": 0.5,
                    "exit_code": 7,
                },
            }
        }
    )
    agreed.job_summaries_at = now
    lost = mgr.view.peers["c:8443"]
    lost.status = STATUS_UNREACHABLE
    lost.last_error = "connection refused"
    cron.cluster_manager = mgr

    cluster = cron.cluster_payload()
    _declares("ClusterView", cluster, "/cluster (gossip)")
    assert cluster["peers"] and "candidates_truncated" in cluster
    assert cluster["peers"][0]["node_stats"]["mem_used_bytes"] == 2_090_278_912
    assert "conflicting_policies" in cluster and "node_stats" in cluster
    fleet = cron.fleet_payload()
    _declares("FleetView", fleet, "/fleet (gossip)")
    nodes = {node["host"]: node for node in fleet["nodes"]}
    assert nodes[None]["jobs"]["alpha"]["last"]["outcome"] == "success"
    assert nodes["b:8443"]["jobs"]["alpha"]["last"]["exit_code"] == 7
    assert nodes["c:8443"]["jobs"] is None
    assert "interval" in fleet

    # the three lease backends, built without a store to elect against
    path = str(tmp_path).replace("\\", "/")
    leases = (
        (FilesystemBackend, "filesystem:\n    path: " + path + "\n"),
        (
            KubernetesBackend,
            "kubernetes:\n    leaseName: yl\n    leaseNamespace: ns\n",
        ),
        (
            EtcdBackend,
            "etcd:\n    endpoints:\n      - http://127.0.0.1:2379\n"
            "    electionName: cronstable/leader\n",
        ),
    )
    for backend_class, section in leases:
        yaml = "cluster:\n  backend: {}\n  nodeName: node-a\n  {}".format(
            backend_class.backend_name, section
        )
        config = parse_config_string(yaml, "").cluster_config
        cron.cluster_manager = backend_class(config, lambda: "v1:job")
        view = cron.cluster_payload()
        _declares(
            "ClusterView", view, "/cluster ({})".format(backend_class.__name__)
        )
        assert view["lease"] and "fleet" in view
    off = _cron(_CLUSTER_JOBS)
    _declares("ClusterView", off.cluster_payload(), "/cluster unclustered")
    _declares("FleetView", off.fleet_payload(), "/fleet unclustered")
