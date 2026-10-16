"""The scheduler side of ``resourceLimits``.

Run records carry an optional ``limits`` object, ``/jobs`` and the
resources endpoint carry the configured caps, ``/node`` reports what the
host can enforce, and every applied config hands its capped jobs to the
limits manager.  Every test that could reach the process-wide manager
installs a fake one (``limits._MANAGER``), so nothing here probes or
migrates a real cgroup.
"""

import asyncio
import datetime
import json
import logging
import threading

import pytest

import cronstable.cron
from cronstable import limits
from cronstable.cron import JobRunInfo, _job_run_info_from_dict
from cronstable.job import JobOutputStream
from cronstable.limits import Capability, LimitsRecord
from cronstable.resources import ResourceUsage
from tests._commands import cmd_print, cmd_print_sleep_print, yaml_command
from tests._helpers import _wait_until
from tests.conftest import Req, _cron

MIB = 1024 * 1024
UTC = datetime.timezone.utc

_CAPPED = """
jobs:
  - name: capped
    command: echo hi
    schedule: "0 0 1 1 *"
    enabled: false
    resourceLimits:
      cpu: 1.5
      memory: 512M
      processes: 64
  - name: plain
    command: echo hi
    schedule: "0 0 1 1 *"
    enabled: false
"""

_UNCAPPED = """
jobs:
  - name: plain
    command: echo hi
    schedule: "0 0 1 1 *"
    enabled: false
"""

_CAPPED_UNMONITORED = """
jobs:
  - name: capped
    command: echo hi
    schedule: "0 0 1 1 *"
    resourceLimits:
      memory: 64M
"""

_CAPPED_MONITORED = """
jobs:
  - name: both
    command: echo hi
    schedule: "0 0 1 1 *"
    monitorResources: true
    resourceLimits:
      cpu: 2
"""

_CAPPED_DAG = """
defaults:
  resourceLimits:
    processes: 32
jobs:
  - name: lifted
    command: echo hi
    schedule: "0 0 1 1 *"
    enabled: false
    resourceLimits:
      processes: max
dags:
  - name: d
    tasks:
      - id: t
        command: echo hi
        resourceLimits:
          memory: 64M
      - id: gate
        type: approval
        dependsOn:
          - t
"""

_FULL = frozenset({"cpu", "memory", "processes"})


class _FakeManager:
    """The two calls the scheduler makes on the limits manager."""

    def __init__(self, error=None):
        self.error = error
        self.weighting = False
        self.converged = []
        self.threads = []
        self.probes = 0
        self.cap = Capability(
            available=True,
            mechanism="fake-mech",
            resources=_FULL,
            reason=None,
            hint=None,
        )

    def converge(self, capped):
        self.threads.append(threading.current_thread())
        self.converged.append(list(capped))
        if self.error is not None:
            raise self.error

    def capability(self):
        self.probes += 1
        return self.cap


@pytest.fixture
def manager(monkeypatch):
    fake = _FakeManager()
    monkeypatch.setattr(limits, "_MANAGER", fake)
    return fake


@pytest.fixture
def no_manager(monkeypatch):
    """No process-wide manager yet; teardown drops any that gets built."""
    monkeypatch.setattr(limits, "_MANAGER", None)


def _record(**over):
    fields = {
        "cpu": 1.5,
        "memory_bytes": 512 * MIB,
        "processes": 64,
        "required": False,
        "enforced": True,
        "mechanism": "cgroup-v2",
        "unenforced": (),
        "reason": None,
        "memory_exceeded": True,
        "process_limit_hits": 0,
        "peak_memory_bytes": 512 * MIB,
        "cpu_throttled_seconds": 1.25,
    }
    fields.update(over)
    return LimitsRecord(**fields)


def _run(outcome="success", *, minute=0, **kw):
    start = datetime.datetime(2026, 1, 1, 0, minute, 0, tzinfo=UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0,
        started_at=start,
        finished_at=start + datetime.timedelta(seconds=2),
        fail_reason=None,
        output=JobOutputStream(),
        **kw,
    )


# --- JobRunInfo ------------------------------------------------------------


def test_run_record_carries_limits_and_round_trips():
    record = _record()
    info = _run(limits=record)
    data = info.to_dict(include_series=True)
    assert data["limits"] == record.to_dict()
    # the durable ledger stores JSON; the record survives the trip
    restored = _job_run_info_from_dict(json.loads(json.dumps(data)))
    assert restored is not None
    assert restored.limits == record


def test_uncapped_run_record_has_no_limits_key():
    data = _run().to_dict(include_series=True)
    assert "limits" not in data
    restored = _job_run_info_from_dict(data)
    assert restored is not None and restored.limits is None


@pytest.mark.parametrize(
    "bad", ["junk", 7, [], {}, {"enforced": "yes"}, {"cpu": 1.0}]
)
def test_malformed_ledger_limits_read_as_none(bad):
    data = _run().to_dict()
    data["limits"] = bad
    restored = _job_run_info_from_dict(data)
    assert restored is not None
    assert restored.limits is None
    assert restored.outcome == "success"
    assert restored.duration == 2.0


async def test_reaper_records_the_runs_limits():
    yaml = (
        "jobs:\n  - name: ok\n" + yaml_command(cmd_print(out="hi")) + "\n"
        '    schedule: "0 0 1 1 *"\n'
    )
    cron = _cron(yaml)
    await cron.maybe_launch_job(cron.cron_jobs["ok"])
    running_job = cron.running_jobs["ok"][-1]
    await running_job.wait()
    record = _record()
    running_job.limits_record = record
    await cron._handle_finished_job(running_job)
    await cron._drain_completions()
    assert cron.last_run["ok"].limits is record
    assert cron.last_run["ok"].to_dict()["limits"] == record.to_dict()


async def test_reaper_records_a_cancelled_runs_limits():
    yaml = (
        "jobs:\n  - name: slow\n"
        + yaml_command(cmd_print_sleep_print("ready", 60, "done"))
        + "\n    captureStdout: true\n"
        '    schedule: "0 0 1 1 *"\n'
        "    killTimeout: 5\n"
    )
    cron = _cron(yaml)
    await cron.maybe_launch_job(cron.cron_jobs["slow"])
    running_job = cron.running_jobs["slow"][0]
    await _wait_until(lambda: bool(running_job.output.lines))
    running_job.cancelled = True  # what _web_cancel_job sets
    await running_job.cancel()
    await running_job.wait()
    record = _record(memory_exceeded=False)
    running_job.limits_record = record
    await cron._handle_finished_job(running_job)
    last = cron.last_run["slow"]
    assert last.outcome == "cancelled"
    assert last.limits is record


# --- /jobs -------------------------------------------------------------------


async def test_jobs_payload_carries_caps_only_for_capped_jobs():
    cron = _cron(_CAPPED)
    cron.last_run["capped"] = _run(limits=_record())
    cron.last_run["plain"] = _run()
    resp = await cron._web_list_jobs(Req())
    jobs = {job["name"]: job for job in json.loads(resp.text)}
    assert jobs["capped"]["limits"] == {
        "cpu": 1.5,
        "memory_bytes": 512 * MIB,
        "processes": 64,
        "required": False,
    }
    assert jobs["capped"]["last_run"]["limits"] == _record().to_dict()
    assert "limits" not in jobs["plain"]
    assert "limits" not in jobs["plain"]["last_run"]


def test_job_detail_omits_limits_without_caps():
    cron = _cron(_UNCAPPED)
    assert "limits" not in cron.job_detail_payload("plain")


# --- /jobs/{name}/resources -------------------------------------------------


def test_resources_payload_of_a_capped_unmonitored_job():
    cron = _cron(_CAPPED_UNMONITORED)
    capped = _record(memory_bytes=64 * MIB)
    cron.run_history["capped"].append(_run(minute=1, limits=capped))
    # a run without caps or monitoring stays out
    cron.run_history["capped"].append(_run(minute=2))
    payload = cron.job_resources_payload("capped", max_runs=20)
    assert payload["monitored"] is False
    assert payload["limits"] == {
        "cpu": None,
        "memory_bytes": 64 * MIB,
        "processes": None,
        "required": False,
    }
    assert [run["limits"] for run in payload["runs"]] == [capped.to_dict()]
    assert payload["runs"][0]["resources"] is None


def test_resources_payload_tail_spans_monitored_and_capped_runs():
    cron = _cron(_CAPPED_MONITORED)
    usage = ResourceUsage(1.0, 0.5, 2048, 2, series=[[1.0, 5.0, 1024]])
    history = cron.run_history["both"]
    history.append(_run(minute=1, resource_usage=usage, limits=_record()))
    history.append(_run(minute=2, limits=_record(memory_exceeded=False)))
    history.append(_run(minute=3, resource_usage=usage))
    payload = cron.job_resources_payload("both", max_runs=2)
    runs = payload["runs"]
    assert [run["started_at"][14:16] for run in runs] == ["02", "03"]
    assert runs[0]["resources"] is None
    assert runs[0]["limits"]["memory_exceeded"] is False
    assert runs[1]["resources"]["series"] == [[1.0, 5.0, 1024]]
    assert "limits" not in runs[1]
    assert payload["limits"]["cpu"] == 2.0


def test_resources_payload_of_an_uncapped_job_has_null_limits():
    cron = _cron(_UNCAPPED)
    cron.run_history["plain"].append(_run())
    payload = cron.job_resources_payload("plain", max_runs=20)
    assert payload["limits"] is None
    assert payload["runs"] == []


# --- /node ---------------------------------------------------------------


async def test_node_reports_the_limits_capability(manager):
    cron = _cron(_UNCAPPED)
    resp = await cron._web_get_node(Req())
    data = json.loads(resp.text)
    assert data["limits"] == {
        "available": True,
        "mechanism": "fake-mech",
        "resources": ["cpu", "memory", "processes"],
        "reason": None,
        "hint": None,
    }
    # a probe, never a convergence
    assert manager.converged == []


async def test_node_probe_of_the_real_manager(no_manager):
    cron = _cron(_UNCAPPED)
    data = cron.node_payload()
    assert isinstance(data["limits"]["available"], bool)
    assert set(data["limits"]) == {
        "available",
        "mechanism",
        "resources",
        "reason",
        "hint",
    }
    # the probe is cached on the manager it built
    assert cron.node_payload()["limits"] == data["limits"]


# --- convergence -------------------------------------------------------------


async def test_converge_hands_the_capped_list_to_the_manager(manager):
    cron = _cron(_CAPPED_DAG)
    await cron._converge_limits()
    (capped,) = manager.converged
    assert [name for name, _ in capped] == ["dag d task t"]
    assert capped[0][1].memory == 64 * MIB
    assert capped[0][1].processes == 32
    # off the event loop
    assert manager.threads[0] is not threading.current_thread()
    # once per applied config
    await cron._converge_limits()
    assert len(manager.converged) == 1


async def test_converge_without_caps_never_builds_a_manager(no_manager):
    cron = _cron(_UNCAPPED)
    await cron._converge_limits()
    assert limits._MANAGER is None


async def test_converge_follows_each_config_apply(manager, tmp_path):
    path = tmp_path / "cronstable.yaml"
    path.write_text(_CAPPED, encoding="utf-8")
    cron = cronstable.cron.Cron(str(path))
    await cron._converge_limits()
    assert [n for n, _ in manager.converged[-1]] == ["job capped"]
    state = "state:\n  path: {}\n".format(json.dumps(str(tmp_path / "s")))
    path.write_text(state + _CAPPED_DAG, encoding="utf-8")
    cron.signal_reload()
    await cron.reload_config()
    await cron._converge_limits()
    assert [n for n, _ in manager.converged[-1]] == ["dag d task t"]
    # a reload that removes every cap converges nothing
    path.write_text(_UNCAPPED, encoding="utf-8")
    cron.signal_reload()
    await cron.reload_config()
    await cron._converge_limits()
    assert len(manager.converged) == 2
    # unless cpu weighting is on: the empty list turns it off
    manager.weighting = True
    path.write_text(_UNCAPPED + "\n", encoding="utf-8")
    cron.signal_reload()
    await cron.reload_config()
    await cron._converge_limits()
    assert manager.converged[-1] == []


async def test_a_failing_manager_is_logged_not_raised(manager, caplog):
    manager.error = RuntimeError("boom")
    cron = _cron(_CAPPED)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._converge_limits()
    assert any(
        "could not check whether resource limits can be enforced"
        in r.getMessage()
        for r in caplog.records
    )


async def test_the_run_loop_converges_and_survives_a_failure(
    manager, caplog
):
    manager.error = RuntimeError("boom")
    cron = _cron(_CAPPED)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        task = asyncio.create_task(cron.run())
        try:
            await _wait_until(lambda: manager.converged)
            await asyncio.sleep(0.05)
            assert not task.done()
        finally:
            cron.signal_shutdown()
            await asyncio.wait_for(task, timeout=5)
    assert [n for n, _ in manager.converged[0]] == ["job capped"]
    assert len(manager.converged) == 1
    messages = [r.getMessage() for r in caplog.records]
    assert any("resource limits can be enforced" in m for m in messages)
    # the helper absorbed it; the pass's catch-all never saw it
    assert not any("please report this as a bug" in m for m in messages)


async def test_the_run_loop_converges_after_the_gate_before_launches(
    manager, monkeypatch
):
    # the job set and the election gate change with no yield between them,
    # and the caps are converged before the pass launches anything
    cron = _cron(_CAPPED)
    order = []
    real_cluster = cron.start_stop_cluster
    real_slots = cron._service_slots
    real_converge = manager.converge

    async def start_stop_cluster(cluster_config):
        order.append("cluster")
        await real_cluster(cluster_config)

    async def service_slots(startup):
        order.append("slots")
        await real_slots(startup)

    def converge(capped):
        order.append("converge")
        real_converge(capped)

    monkeypatch.setattr(cron, "start_stop_cluster", start_stop_cluster)
    monkeypatch.setattr(cron, "_service_slots", service_slots)
    monkeypatch.setattr(manager, "converge", converge)
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: "slots" in order)
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)
    assert order[:3] == ["cluster", "converge", "slots"]
