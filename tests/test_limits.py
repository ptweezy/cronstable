"""cronstable.limits: records, the prepare decisions, logging, RunLimits.

Every test drives a fake backend (the 2.7 interface), so nothing here
touches a cgroup or a Job Object.  The backends have their own tests:
tests/test_cgroups.py and the Job Object tests.
"""

import ast
import asyncio
import dataclasses
import inspect
import json
import logging
import math
import subprocess
import sys
import threading
import types

import pytest

import cronstable
from cronstable import limits
from cronstable._cgroups import Backend as CgroupBackend
from cronstable.config import ResourceLimits
from cronstable.limits import (
    Capability,
    LimitsManager,
    LimitsRecord,
)

MIB = 1024 * 1024
FULL = frozenset({"cpu", "memory", "processes"})


def _cap(available=True, resources=FULL, reason=None, hint=None, mech="fake"):
    return Capability(available, mech, frozenset(resources), reason, hint)


AVAILABLE = _cap()
UNAVAILABLE = _cap(
    available=False,
    resources=(),
    reason="no cgroup v2 hierarchy is mounted at /sys/fs/cgroup",
    hint="Boot the host with cgroup v2",
)
NO_MEMORY = _cap(
    resources={"cpu", "processes"},
    reason="cgroup /x does not offer the memory controller",
    hint="Delegate the memory controller",
)


class FakeContainer:
    """A container that records calls and returns what it is told."""

    def __init__(self, unenforced=None, applied=FULL):
        self.unenforced = dict(unenforced or {})
        self.applied = frozenset(applied)
        self.calls = []
        self.attach_result = None
        self.attach_error = None
        self.attach_gate = None
        self.attach_loses = None
        self.kill_result = True
        self.kill_error = None
        self.finish_error = None
        self.abort_error = None
        self.abort_loses = None
        self.stats = {
            "memory_exceeded": False,
            "process_limit_hits": 0,
            "peak_memory_bytes": 4 * MIB,
            "cpu_throttled_seconds": 0.5,
        }

    def spawn_kwargs(self, kwargs, *, required):
        self.calls.append(("spawn_kwargs", required))
        kwargs["joined"] = True

    def attach(self, pid, *, required):
        self.calls.append(("attach", pid, required))
        if self.attach_gate is not None:
            assert self.attach_gate.wait(10)
        if self.attach_error is not None:
            raise self.attach_error
        if self.attach_loses is not None:
            self.unenforced.update(self.attach_loses)
        return self.attach_result

    def kill(self):
        self.calls.append(("kill",))
        if self.kill_error is not None:
            raise self.kill_error
        return self.kill_result

    def finish(self):
        self.calls.append(("finish",))
        if self.finish_error is not None:
            raise self.finish_error
        return dict(self.stats)

    def abort(self):
        self.calls.append(("abort",))
        if self.abort_error is not None:
            raise self.abort_error
        if self.abort_loses is not None:
            self.unenforced.update(self.abort_loses)

    def members(self):
        return [1]

    def cpu_times(self):
        return (0.0, 0.0)


class FakeBackend:
    mechanism = "fake-mech"

    def __init__(self, capability=AVAILABLE, setup=None, container=None):
        self.cap = capability
        self.setup_cap = setup if setup is not None else capability
        self.container = container
        self.created = []
        self.probes = 0
        self.setups = 0
        self.collects = 0
        self.setup_error = None
        self.setup_gate = None
        self.create_error = None
        self.create_gate = None
        self.create_returned = threading.Event()

    def capability(self):
        self.probes += 1
        return self.cap

    def setup(self):
        self.setups += 1
        if self.setup_gate is not None:
            assert self.setup_gate.wait(10)
        if self.setup_error is not None:
            raise self.setup_error
        return self.setup_cap

    def create(self, caps, name):
        try:
            return self._create(caps, name)
        finally:
            self.create_returned.set()

    def _create(self, caps, name):
        self.created.append((caps, name))
        if self.create_gate is not None:
            assert self.create_gate.wait(10)
        if self.create_error is not None:
            raise self.create_error
        if self.container is None:
            self.container = FakeContainer(
                applied=[r for r in limits.RESOURCES if getattr(caps, r)]
            )
        return self.container

    def collect(self):
        self.collects += 1


class WeighingBackend(FakeBackend):
    """A backend with cpu control, as the cgroup v2 one has.

    ``cpu_error`` is the reason each attempt to turn cpu on fails with; a
    failed attempt repeats only with ``retry``, as in the real backend.
    ``cpu_calls`` lists ``(want, retry)`` per ``set_cpu`` and
    ``("need", retry)`` per ``need_cpu``.
    """

    def __init__(self, **kw):
        super().__init__(**kw)
        self.weighting = False
        self.cpu_error = None
        self.cpu_failure = None
        self.cpu_calls = []
        self.cpu_attempts = 0
        self.nices = []

    def setup(self):
        super().setup()
        return self._current()

    def set_cpu(self, want, *, retry=False):
        self.cpu_calls.append((want, retry))
        if not want:
            self.weighting = False
            self.cpu_failure = None
            return self._current()
        return self._on(retry)

    def need_cpu(self, *, retry=False):
        self.cpu_calls.append(("need", retry))
        return self._on(retry)

    def _on(self, retry):
        if (
            "cpu" in self.setup_cap.resources
            and not self.weighting
            and (retry or self.cpu_failure is None)
        ):
            self.cpu_attempts += 1
            self.cpu_failure = self.cpu_error
            self.weighting = self.cpu_error is None
        return self._current()

    def _current(self):
        cap = self.setup_cap
        if self.cpu_failure is None:
            return cap
        return dataclasses.replace(
            cap, resources=cap.resources - {"cpu"}, reason=self.cpu_failure
        )

    def create(self, caps, name, **kwargs):
        self.nices.append(kwargs.get("nice", "not given"))
        return super().create(caps, name)


def _manager(**kw):
    backend = FakeBackend(**kw)
    return LimitsManager(backend), backend


def _weighing(**kw):
    backend = WeighingBackend(**kw)
    return LimitsManager(backend), backend


def _record(**over):
    fields = {
        "cpu": 1.5,
        "memory_bytes": 512 * MIB,
        "processes": 64,
        "required": True,
        "enforced": False,
        "mechanism": "cgroup-v2",
        "unenforced": ("memory",),
        "reason": "cgroup /x does not offer the memory controller",
        "refused": False,
        "memory_exceeded": True,
        "process_limit_hits": 3,
        "peak_memory_bytes": 511 * MIB,
        "cpu_throttled_seconds": 2.25,
    }
    fields.update(over)
    return LimitsRecord(**fields)


# --- LimitsRecord ----------------------------------------------------------


def test_record_round_trips_through_json():
    record = _record()
    data = json.loads(json.dumps(record.to_dict(), allow_nan=False))
    assert data["unenforced"] == ["memory"]
    assert LimitsRecord.from_dict(data) == record


def test_record_to_dict_drops_non_finite_numbers():
    record = _record(cpu=math.inf, cpu_throttled_seconds=math.nan)
    data = record.to_dict()
    assert data["cpu"] is None and data["cpu_throttled_seconds"] is None
    json.dumps(data, allow_nan=False)


@pytest.mark.parametrize(
    "bad",
    [None, "x", [], {}, {"enforced": "yes"}, {"enforced": 1}],
)
def test_record_from_dict_rejects_malformed(bad):
    assert LimitsRecord.from_dict(bad) is None


def test_record_from_dict_reads_bad_fields_as_unset():
    record = LimitsRecord.from_dict(
        {
            "enforced": True,
            "cpu": True,
            "memory_bytes": -5,
            "processes": 2.5,
            "required": "yes",
            "mechanism": 7,
            "unenforced": ["processes", "disk", ["cpu"], "cpu", "cpu"],
            "reason": None,
            "refused": 1,
            "memory_exceeded": "true",
            "process_limit_hits": False,
            "peak_memory_bytes": "big",
            "cpu_throttled_seconds": float("nan"),
        }
    )
    assert record == LimitsRecord(
        cpu=None,
        memory_bytes=None,
        processes=None,
        required=False,
        enforced=True,
        mechanism=None,
        unenforced=("cpu", "processes"),
        reason=None,
    )


def test_record_from_dict_tolerates_a_non_list_unenforced():
    record = LimitsRecord.from_dict(
        {"enforced": False, "unenforced": "memory", "cpu": 2}
    )
    assert record.unenforced == ()
    assert record.cpu == 2.0


def test_failure_of_a_refused_record():
    record = _record(refused=True, memory_exceeded=False)
    assert record.failure == (
        "resource limits could not be enforced: cgroup /x does not offer "
        "the memory controller (resourceLimits.required is set)"
    )
    assert _record(refused=True, reason=None).failure == (
        "resource limits could not be enforced "
        "(resourceLimits.required is set)"
    )


def test_failure_of_a_memory_kill():
    assert _record().failure == "memory limit exceeded (512 MiB)"
    assert _record(memory_bytes=1536 * MIB).failure == (
        "memory limit exceeded (1.5 GiB)"
    )
    assert _record(memory_bytes=None).failure == "memory limit exceeded"


def test_no_failure_otherwise():
    assert _record(memory_exceeded=False).failure is None


def test_words():
    assert limits._words(["a"]) == "a"
    assert limits._words(["a", "b"]) == "a and b"
    assert limits._words(["a", "b", "c"]) == "a, b, and c"


def test_capability_to_dict():
    assert NO_MEMORY.to_dict() == {
        "available": True,
        "mechanism": "fake",
        "resources": ["cpu", "processes"],
        "reason": "cgroup /x does not offer the memory controller",
        "hint": "Delegate the memory controller",
    }


# --- backend selection -----------------------------------------------------


def test_windows_selects_the_job_object_backend(monkeypatch):
    class Backend:
        pass

    fake = types.ModuleType("cronstable._jobobjects")
    fake.Backend = Backend
    monkeypatch.setitem(sys.modules, "cronstable._jobobjects", fake)
    monkeypatch.setattr(cronstable, "_jobobjects", fake, raising=False)
    assert isinstance(limits._select_backend(windows=True), Backend)


def test_both_backends_are_static_imports():
    # A frozen build bundles only modules an import statement names, so
    # the selection must name both backends rather than compute them.
    tree = ast.parse(inspect.getsource(limits._select_backend))
    imported = {
        (node.module, alias.name)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert imported == {
        ("cronstable", "_cgroups"),
        ("cronstable", "_jobobjects"),
    }
    assert "importlib" not in vars(limits)


def test_linux_selects_the_cgroup_backend():
    backend = limits._select_backend(windows=False, system="linux")
    assert isinstance(backend, CgroupBackend)


def test_other_platforms_get_an_unsupported_capability():
    backend = limits._select_backend(windows=False, system="darwin")
    cap = backend.capability()
    assert cap == backend.setup()
    assert not cap.available and cap.mechanism is None
    assert cap.reason == "this platform has neither cgroup v2 nor Job Objects"
    assert backend.collect() is None


def test_a_backend_that_fails_to_load_degrades(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, "cronstable._jobobjects", None)
    monkeypatch.delattr(cronstable, "_jobobjects", raising=False)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        backend = limits._select_backend(windows=True)
    cap = backend.capability()
    assert not cap.available
    assert cap.reason.startswith("could not load cronstable._jobobjects: ")
    assert "could not load cronstable._jobobjects" in caplog.text


def test_selection_defaults_come_from_the_host(monkeypatch):
    monkeypatch.setattr(limits.platform, "IS_WINDOWS", False)
    monkeypatch.setattr(
        limits, "sys", types.SimpleNamespace(platform="freebsd14")
    )
    assert not limits._select_backend().capability().available


def test_importing_limits_loads_no_backend():
    code = (
        "import sys, cronstable.limits; "
        "print(sorted(m for m in sys.modules if m in "
        "('cronstable._cgroups', 'cronstable._jobobjects')))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "[]"


# --- manager, capability and the process-wide instance ----------------------


def test_capability_probes_once_until_setup():
    manager, backend = _manager(capability=NO_MEMORY, setup=AVAILABLE)
    assert manager.capability() is NO_MEMORY
    assert manager.capability() is NO_MEMORY
    assert backend.probes == 1 and backend.setups == 0
    assert manager.setup() is AVAILABLE
    assert manager.capability() is AVAILABLE


def test_setup_runs_once_unless_retried_while_a_resource_is_missing():
    manager, backend = _manager(setup=UNAVAILABLE)
    manager.setup()
    manager.setup()
    assert backend.setups == 1
    manager.setup(retry=True)
    assert backend.setups == 2
    backend.setup_cap = NO_MEMORY
    assert manager.setup(retry=True) is NO_MEMORY
    assert manager.setup(retry=True) is NO_MEMORY  # still partial: repeated
    assert backend.setups == 4
    assert manager.setup() is NO_MEMORY  # no retry asked: the cached one
    assert backend.setups == 4
    backend.setup_cap = AVAILABLE
    manager.setup(retry=True)
    manager.setup(retry=True)  # complete now: never repeated
    assert backend.setups == 5


class LiveBackend(FakeBackend):
    """A backend whose capability can drop while it runs (Job Objects)."""

    live_capability = True


def test_a_live_capability_is_asked_on_every_call():
    backend = LiveBackend()
    manager = LimitsManager(backend)
    assert manager.capability() is AVAILABLE
    assert manager.setup() is AVAILABLE
    backend.cap = NO_MEMORY  # as when the Job Object watcher stops
    assert manager.capability() is NO_MEMORY
    assert manager.setup(retry=True) is NO_MEMORY
    assert backend.setups == 1  # the setup itself was complete
    assert backend.probes == 4


async def test_a_live_capability_decides_each_prepare():
    backend = LiveBackend()
    manager = LimitsManager(backend)
    caps = ResourceLimits(memory=MIB)
    first = await manager.prepare(caps, "j")
    assert first.container is backend.container and first.record.enforced
    backend.cap = NO_MEMORY
    later = await manager.prepare(caps, "j")
    # every configured cap is missing now, so no container is made
    assert later.container is None
    assert len(backend.created) == 1
    assert later.record.unenforced == ("memory",)
    assert later.record.reason == NO_MEMORY.reason
    assert later.record.mechanism is None
    required = await manager.prepare(
        ResourceLimits(memory=MIB, required=True), "j"
    )
    assert required.refused and len(backend.created) == 1


def test_converge_reports_a_live_capability_that_dropped(caplog):
    backend = LiveBackend(capability=_cap(mech="job-object"))
    manager = LimitsManager(backend)
    capped = [("job a", ResourceLimits(memory=MIB))]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
        backend.cap = _cap(
            mech="job-object",
            resources={"cpu", "processes"},
            reason="the Job Object watcher stopped: it broke",
        )
        manager.converge(capped)
        manager.converge(capped)
    assert backend.setups == 1
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with Windows Job Objects"
    ]
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job a), but cronstable cannot enforce the "
        "memory cap: the Job Object watcher stopped: it broke. Those jobs "
        "run without that cap."
    ]


def test_the_backend_is_selected_lazily(monkeypatch):
    chosen = FakeBackend()
    monkeypatch.setattr(limits, "_select_backend", lambda: chosen)
    manager = LimitsManager()
    assert manager.backend is chosen
    assert manager.backend is chosen


def test_get_and_set_manager(monkeypatch):
    monkeypatch.setattr(limits, "_MANAGER", None)
    first = limits.get_manager()
    assert limits.get_manager() is first
    mine, _ = _manager()
    limits.set_manager(mine)
    assert limits.get_manager() is mine
    limits.set_manager(None)
    assert limits.get_manager() is not mine


# --- prepare ---------------------------------------------------------------


async def test_prepare_with_everything_available(monkeypatch):
    manager, backend = _manager()
    monkeypatch.setattr(limits, "_MANAGER", manager)
    caps = ResourceLimits(cpu=1.5, memory=512 * MIB, processes=64)
    run = await limits.prepare(caps, "backup")
    assert backend.created == [(caps, "backup")]
    assert backend.collects == 1
    assert run.container is backend.container
    assert not run.refused
    assert run.record == LimitsRecord(
        cpu=1.5,
        memory_bytes=512 * MIB,
        processes=64,
        required=False,
        enforced=True,
        mechanism="fake-mech",
        unenforced=(),
        reason=None,
    )


async def test_prepare_skips_what_the_host_lacks():
    manager, backend = _manager(capability=NO_MEMORY)
    caps = ResourceLimits(cpu=2, memory=MIB)
    run = await manager.prepare(caps, "j")
    assert backend.created == [(ResourceLimits(cpu=2), "j")]
    record = run.record
    assert record.enforced is False
    assert record.unenforced == ("memory",)
    assert record.reason == NO_MEMORY.reason
    assert record.mechanism == "fake-mech"


async def test_prepare_refuses_a_partial_host_when_required():
    manager, backend = _manager(capability=NO_MEMORY)
    run = await manager.prepare(
        ResourceLimits(cpu=2, memory=MIB, required=True), "j"
    )
    assert run.refused
    assert backend.created == []
    assert run.container is None
    assert run.record.refused and run.record.unenforced == ("memory",)
    assert run.record.failure.startswith(
        "resource limits could not be enforced: cgroup /x"
    )


async def test_prepare_on_an_unavailable_host_creates_nothing():
    manager, backend = _manager(capability=UNAVAILABLE)
    run = await manager.prepare(ResourceLimits(cpu=2, processes=3), "j")
    assert backend.created == []
    assert not run.refused and run.container is None
    assert run.record.unenforced == ("cpu", "processes")
    assert run.record.mechanism is None
    assert run.record.reason == UNAVAILABLE.reason
    assert run.record.failure is None


async def test_prepare_refuses_an_unavailable_host_when_required():
    manager, _ = _manager(capability=UNAVAILABLE)
    run = await manager.prepare(ResourceLimits(cpu=2, required=True), "j")
    assert run.refused and run.record.refused


async def test_a_capability_without_a_reason_still_explains():
    manager, _ = _manager(capability=_cap(resources=()))
    run = await manager.prepare(ResourceLimits(cpu=2), "j")
    assert run.record.reason == "not supported"


async def test_a_create_time_failure_is_recorded_and_logged_once(caplog):
    reason = "could not set pids.max: Invalid argument"
    container = FakeContainer({"processes": reason}, applied={"cpu"})
    manager, backend = _manager(container=container)
    caps = ResourceLimits(cpu=1, processes=4)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        first = await manager.prepare(caps, "j")
        second = await manager.prepare(caps, "j")
    assert first.container is container and second.container is container
    assert first.record.unenforced == ("processes",)
    assert first.record.reason == reason
    warnings = [r for r in caplog.records if reason in r.getMessage()]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == (
        "resource limits could not be applied to a run of j: " + reason
        + " (each distinct failure is logged once)"
    )


async def test_a_create_time_failure_refuses_a_required_run():
    reason = "could not set memory.max: Permission denied"
    container = FakeContainer({"memory": reason}, applied={"cpu"})
    manager, backend = _manager(container=container)
    collected = []
    backend.collect = lambda: collected.append(list(container.calls))
    run = await manager.prepare(
        ResourceLimits(cpu=1, memory=MIB, required=True), "j"
    )
    assert container.calls == [("abort",)]
    # collect follows the abort, so the refused run's cgroup goes at once
    assert collected == [[("abort",)]]
    assert run.refused and run.container is None
    assert run.record.unenforced == ("memory",)
    assert run.record.reason == reason


async def test_prepare_survives_a_broken_backend(caplog):
    manager, backend = _manager()
    backend.create_error = RuntimeError("boom")
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        run = await manager.prepare(ResourceLimits(cpu=1), "j")
        required = await manager.prepare(
            ResourceLimits(cpu=1, required=True), "j"
        )
    assert not run.refused and run.record.reason == "unexpected error: boom"
    assert required.refused
    warnings = _messages(caplog, logging.WARNING)
    assert warnings == [
        "could not prepare the resource limits of j "
        "(each distinct failure is logged once)"
    ]
    assert "RuntimeError: boom" in caplog.text  # with the traceback


async def test_an_unexpected_os_error_leaves_its_path_out(caplog):
    manager, backend = _manager()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        for path in ("/x/runs/ab-j", "/x/runs/cd-j"):
            backend.create_error = OSError(5, "Input/output error", path)
            run = await manager.prepare(ResourceLimits(cpu=1), "j")
            assert run.record.reason == "unexpected error: Input/output error"
    assert len(caplog.records) == 1  # the same failure: logged once


@pytest.mark.parametrize(
    "exc, text",
    [
        (OSError(2, "No such file", "/x/runs/ab-j"), "No such file"),
        (OSError("gone"), "gone"),
        (RuntimeError(), "RuntimeError"),
        (ValueError("bad"), "bad"),
    ],
)
def test_error_text(exc, text):
    assert limits._error_text(exc) == text


def _asyncio_errors(caplog):
    """ERROR records from asyncio itself (a shielded future, a callback)."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "asyncio" and r.levelno >= logging.ERROR
    ]


async def _quiesce():
    """Wait for every other task to end and its done callbacks to run."""
    me = asyncio.current_task()
    for _ in range(1000):
        if not asyncio.all_tasks() - {me}:
            break
        await asyncio.sleep(0.01)
    for _ in range(5):
        await asyncio.sleep(0)


class _CreateFailed(RuntimeError):
    """A create() failure that counts attempts to release it as a run."""

    def __init__(self):
        super().__init__("boom")
        self.aborts = 0

    def abort(self):
        self.aborts += 1


async def test_cancelling_prepare_releases_what_it_created(caplog):
    container = FakeContainer()
    manager, backend = _manager(container=container)
    backend.create_gate = threading.Event()
    task = asyncio.ensure_future(manager.prepare(ResourceLimits(cpu=1), "j"))
    while not backend.created:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert container.calls == []  # still creating on its worker thread
    backend.create_gate.set()
    await _quiesce()
    assert container.calls == [("abort",)]
    assert _asyncio_errors(caplog) == []


async def test_cancelling_a_prepare_whose_create_fails(caplog):
    manager, backend = _manager()
    backend.create_gate = threading.Event()
    backend.create_error = error = _CreateFailed()
    task = asyncio.ensure_future(manager.prepare(ResourceLimits(cpu=1), "j"))
    while not backend.created:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    backend.create_gate.set()
    await _quiesce()
    # no container exists, so nothing is released; the shielded task
    # returns the worker's error instead of raising it, so asyncio logs
    # nothing
    assert error.aborts == 0
    assert _asyncio_errors(caplog) == []


async def test_a_cancelled_prepare_worker_releases_nothing():
    cancelled = asyncio.get_running_loop().create_future()
    cancelled.cancel()
    limits._release_abandoned(cancelled)


async def test_an_empty_cap_set_needs_no_container():
    manager, backend = _manager()
    run = await manager.prepare(ResourceLimits(), "j")
    assert backend.created == [] and run.record.enforced


async def test_the_mechanism_is_null_when_no_cap_applied():
    # A container whose every cap failed stays (for the kill and the
    # monitor), but the record names a mechanism only for a cap in force.
    reason = "could not create a cgroup in /x/runs: No space left on device"
    container = FakeContainer(
        {"cpu": reason, "memory": reason}, applied=()
    )
    run, _ = await _run(container=container)
    assert run.container is container
    record = run.record
    assert record.unenforced == ("cpu", "memory")
    assert record.mechanism is None and not record.enforced


async def test_the_mechanism_names_a_partial_container():
    reason = "could not set memory.max: Permission denied"
    run, _ = await _run(container=FakeContainer({"memory": reason}, {"cpu"}))
    assert run.record.mechanism == "fake-mech"
    assert run.record.unenforced == ("memory",)


async def test_a_cap_lost_during_the_run_counts_as_not_applied():
    container = FakeContainer(applied={"memory"})
    run, _ = await _run(ResourceLimits(memory=MIB), container)
    assert run.record.mechanism == "fake-mech"
    container.unenforced["memory"] = "the Job Object watcher stopped"
    assert (await run.finish()).mechanism is None


async def test_prepare_retries_a_failed_setup_at_most_every_30_seconds(caplog):
    # A setup can fail for a moment (a process in its exit path); runs
    # retry it on their own, so a failure lasts no longer than that.
    manager, backend = _manager(capability=UNAVAILABLE)
    now = [1000.0]
    manager._clock = lambda: now[0]
    caps = ResourceLimits(memory=MIB, required=True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge([("job a", caps)])
        assert (await manager.prepare(caps, "a")).refused
        now[0] += 29.9
        assert (await manager.prepare(caps, "a")).refused
        assert backend.setups == 1
        backend.setup_cap = _cap(mech="cgroup-v2")
        now[0] += 0.2
        run = await manager.prepare(caps, "a")
        assert backend.setups == 2
        assert not run.refused and run.record.enforced
        # /node reads the new setup too
        assert manager.capability() is backend.setup_cap
        now[0] += 60
        await manager.prepare(caps, "a")  # complete: never repeated
    assert backend.setups == 2
    assert len(_messages(caplog, logging.WARNING)) == 1
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with cgroup v2"
    ]


async def test_a_first_setup_in_prepare_starts_the_retry_clock():
    manager, backend = _manager(setup=NO_MEMORY)
    now = [50.0]
    manager._clock = lambda: now[0]
    await manager.prepare(ResourceLimits(cpu=1), "a")
    now[0] += 10
    await manager.prepare(ResourceLimits(cpu=1), "a")
    assert backend.setups == 1
    now[0] += 30
    await manager.prepare(ResourceLimits(cpu=1), "a")
    # partial still, and nobody to report it to: no converge ran
    assert backend.setups == 2


# --- converge --------------------------------------------------------------


def _messages(caplog, level):
    return [r.getMessage() for r in caplog.records if r.levelno == level]


def test_converge_without_capped_jobs_does_nothing(caplog):
    manager, backend = _manager(capability=UNAVAILABLE)
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        manager.converge([])
    assert backend.probes == backend.setups == 0
    assert caplog.records == []


def test_converge_warns_once_when_caps_cannot_apply(caplog):
    manager, backend = _manager(capability=UNAVAILABLE)
    capped = [
        ("job a", ResourceLimits(cpu=1)),
        ("dag d task t", ResourceLimits(memory=MIB, required=True)),
        ("job c", ResourceLimits(processes=2, required=True)),
    ]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
        manager.converge(list(capped))
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job a, dag d task t, job c), but cronstable "
        "cannot enforce it: no cgroup v2 hierarchy is mounted at "
        "/sys/fs/cgroup. Boot the host with cgroup v2. Those jobs run "
        "without caps. Runs of dag d task t, job c fail until the caps can "
        "be enforced (resourceLimits.required)."
    ]


def test_converge_logs_again_when_the_capped_set_changes(caplog):
    manager, _ = _manager(capability=UNAVAILABLE)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.converge([("job a", ResourceLimits(cpu=1))])
        manager.converge([("job b", ResourceLimits(cpu=1))])
        manager.converge([("job b", ResourceLimits(cpu=2))])
    assert len(_messages(caplog, logging.WARNING)) == 3


def test_converge_without_a_hint_or_required_jobs(caplog):
    cap = _cap(available=False, resources=(), reason="nope")
    manager, _ = _manager(capability=cap)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.converge([("job a", ResourceLimits(cpu=1))])
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job a), but cronstable cannot enforce it: "
        "nope. Those jobs run without caps."
    ]


def test_converge_names_only_the_jobs_a_partial_host_affects(caplog):
    manager, _ = _manager(capability=NO_MEMORY)
    capped = [
        ("job cpu", ResourceLimits(cpu=1)),
        ("job mem", ResourceLimits(memory=MIB, required=True)),
    ]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job mem), but cronstable cannot enforce the "
        "memory cap: cgroup /x does not offer the memory controller. "
        "Delegate the memory controller. Those jobs run without that cap. "
        "Runs of job mem fail until the caps can be enforced "
        "(resourceLimits.required)."
    ]


def test_converge_names_every_missing_resource(caplog):
    cap = _cap(resources={"cpu"}, reason="r")
    manager, _ = _manager(capability=cap)
    capped = [
        ("job a", ResourceLimits(memory=MIB)),
        ("job b", ResourceLimits(processes=3, cpu=1)),
    ]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.converge(capped)
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job a, job b), but cronstable cannot enforce "
        "the memory and processes caps: r. Those jobs run without those caps."
    ]


def test_converge_of_a_partial_host_nobody_needs_is_info(caplog):
    cap = _cap(resources={"cpu"}, reason="r", mech="cgroup-v2")
    manager, _ = _manager(capability=cap)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge([("job a", ResourceLimits(cpu=1))])
    assert _messages(caplog, logging.WARNING) == []
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with cgroup v2"
    ]


@pytest.mark.parametrize(
    "mech, label",
    [
        ("cgroup-v2", "cgroup v2"),
        ("job-object", "Windows Job Objects"),
        ("other", "other"),
    ],
)
def test_converge_announces_the_mechanism(caplog, mech, label):
    manager, _ = _manager(capability=_cap(mech=mech))
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge([("job a", ResourceLimits(cpu=1))])
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with " + label
    ]


def test_converge_retries_a_failed_setup_and_reports_the_change(caplog):
    manager, backend = _manager(capability=UNAVAILABLE)
    capped = [("job a", ResourceLimits(cpu=1))]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
        backend.setup_cap = _cap(mech="cgroup-v2")
        manager.converge(capped)
    assert len(_messages(caplog, logging.WARNING)) == 1
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with cgroup v2"
    ]


async def test_a_converge_on_a_thread_never_blocks_the_loop(caplog):
    # Cron converges on a worker thread; the loop's calls answer at once
    # while its setup runs, and runs prepared meanwhile wait for it.
    manager, backend = _manager(setup=_cap(mech="cgroup-v2"))
    backend.setup_gate = threading.Event()
    capped = [("job a", ResourceLimits(cpu=1))]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        first = asyncio.ensure_future(
            asyncio.to_thread(manager.converge, capped)
        )
        while not backend.setups:
            await asyncio.sleep(0.01)
        assert manager.capability() is AVAILABLE  # the probe
        manager.note_failures("job a", ["boom"])
        second = asyncio.ensure_future(
            asyncio.to_thread(manager.converge, capped)
        )
        preparing = asyncio.ensure_future(
            manager.prepare(ResourceLimits(cpu=1), "a")
        )
        await asyncio.sleep(0.05)
        assert not (first.done() or second.done() or preparing.done())
        backend.setup_gate.set()
        await asyncio.gather(first, second)
        run = await preparing
    assert backend.setups == 1
    assert run.container is backend.container and run.record.enforced
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with cgroup v2"
    ]
    assert manager.capability().mechanism == "cgroup-v2"


def test_converge_never_raises(caplog):
    manager, backend = _manager()
    backend.setup_error = RuntimeError("kaput")
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.converge([("job a", ResourceLimits(cpu=1))])
    assert "could not check whether resource limits can be enforced" in (
        caplog.text
    )


# --- cpu weighting ---------------------------------------------------------


def test_cpu_control_follows_the_cpu_caps():
    manager, backend = _weighing()
    manager.converge([("job a", ResourceLimits(memory=MIB))])
    assert backend.cpu_calls == [(False, True)]
    assert not manager.weighting
    manager.converge(
        [
            ("job a", ResourceLimits(memory=MIB)),
            ("dag d task t", ResourceLimits(cpu=0.5)),
        ]
    )
    assert backend.cpu_calls[-1] == (True, True) and manager.weighting
    manager.converge([("job a", ResourceLimits(processes=3))])
    assert backend.cpu_calls[-1] == (False, True)
    assert not manager.weighting


def test_converge_without_capped_jobs_turns_cpu_off(caplog):
    manager, backend = _weighing()
    manager.converge([("job a", ResourceLimits(cpu=1))])
    setups = backend.setups
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        manager.converge([])
    assert backend.cpu_calls[-1] == (False, True)
    assert not manager.weighting
    assert backend.setups == setups and caplog.records == []


def test_cpu_control_needs_a_set_up_backend():
    manager, backend = _weighing(setup=UNAVAILABLE)
    manager.converge([("job a", ResourceLimits(cpu=1))])
    assert backend.cpu_calls == [] and not manager.weighting


def test_weighting_active_reads_the_process_wide_manager(monkeypatch):
    monkeypatch.setattr(limits, "_MANAGER", None)
    assert limits.weighting_active() is False
    assert limits._MANAGER is None  # nothing was built to answer
    manager, _ = _weighing()
    other, _ = _weighing()
    monkeypatch.setattr(limits, "_MANAGER", manager)
    assert limits.weighting_active() is False
    other.converge([("job a", ResourceLimits(cpu=1))])
    assert limits.weighting_active() is False  # not the installed one
    manager.converge([("job a", ResourceLimits(cpu=1))])
    assert limits.weighting_active() is True
    manager.converge([("job a", ResourceLimits(memory=MIB))])
    assert limits.weighting_active() is False
    manager.converge([("job a", ResourceLimits(cpu=1))])
    manager.converge([])
    assert limits.weighting_active() is False


def test_weighting_asks_no_backend_into_being():
    manager = LimitsManager()
    assert manager.weighting is False
    assert manager._backend is None
    # a backend without cpu control never weighs
    assert LimitsManager(FakeBackend()).weighting is False


def test_a_cpu_that_cannot_go_on_is_reported_and_retried(caplog):
    manager, backend = _weighing(capability=_cap(mech="cgroup-v2"))
    reason = "could not enable the cpu controller in /x: Invalid argument"
    backend.cpu_error = reason
    capped = [
        ("job c", ResourceLimits(cpu=1)),
        ("job m", ResourceLimits(memory=MIB)),
    ]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
        assert backend.cpu_attempts == 1
        assert manager.capability().resources == {"memory", "processes"}
        manager.converge(capped)  # every config apply tries again
        assert backend.cpu_attempts == 2
        backend.cpu_error = None
        manager.converge(capped)
    assert manager.weighting
    assert manager.capability().resources == FULL
    assert _messages(caplog, logging.WARNING) == [
        "resourceLimits is set (job c), but cronstable cannot enforce the "
        "cpu cap: " + reason + ". Those jobs run without that cap."
    ]
    assert _messages(caplog, logging.INFO) == [
        "resource limits are enforced with cgroup v2"
    ]


async def test_a_refused_cpu_leaves_the_other_caps():
    manager, backend = _weighing()
    backend.cpu_error = reason = "could not enable the cpu controller: busy"
    manager.converge([("job a", ResourceLimits(cpu=1, memory=MIB))])
    run = await manager.prepare(ResourceLimits(cpu=1, memory=MIB), "a")
    assert backend.created == [(ResourceLimits(memory=MIB), "a")]
    assert run.record.unenforced == ("cpu",)
    assert run.record.reason == reason
    assert run.record.mechanism == "fake-mech"


async def test_prepare_turns_cpu_on_when_no_converge_did():
    manager, backend = _weighing()
    await manager.prepare(ResourceLimits(memory=MIB), "a", nice=10)
    assert backend.cpu_calls == []
    run = await manager.prepare(ResourceLimits(cpu=1), "a", nice=19)
    assert backend.cpu_calls == [("need", False)]
    assert manager.weighting and run.record.enforced
    # each run's nice goes with it, for when cpu turns on mid-run
    assert backend.nices == [10, 19]
    await manager.prepare(ResourceLimits(cpu=1), "a")
    assert backend.cpu_calls == [("need", False)]


async def test_a_cpu_run_after_a_converge_without_cpu_only_needs_cpu():
    # a verify check under the previous config: cpu goes on for that run,
    # and what the config wants stays the backend's to act on
    manager, backend = _weighing()
    manager.converge([("job a", ResourceLimits(memory=MIB))])
    run = await manager.prepare(ResourceLimits(cpu=1), "a check")
    assert backend.cpu_calls == [(False, True), ("need", False)]
    assert manager.weighting and run.record.enforced


async def test_prepare_retries_a_failed_cpu_only_on_the_retry_clock():
    manager, backend = _weighing(capability=_cap(mech="cgroup-v2"))
    now = [0.0]
    manager._clock = lambda: now[0]
    backend.cpu_error = "could not enable the cpu controller: busy"
    caps = ResourceLimits(cpu=1)
    manager.converge([("job a", caps)])
    assert backend.cpu_attempts == 1
    backend.cpu_error = None
    run = await manager.prepare(caps, "a")
    assert backend.cpu_attempts == 1 and run.record.unenforced == ("cpu",)
    now[0] += 30
    run = await manager.prepare(caps, "a")
    assert backend.cpu_attempts == 2 and run.record.enforced
    assert manager.weighting


async def test_a_backend_without_weights_gets_no_nice():
    # Job Objects (and other backends) take create(caps, name) only
    manager, backend = _manager()
    run = await manager.prepare(ResourceLimits(cpu=1), "a", nice=19)
    assert run.record.enforced and backend.created == [
        (ResourceLimits(cpu=1), "a")
    ]


async def test_an_uncapped_run_needs_nothing_while_weighting_is_off():
    manager = LimitsManager()
    assert await manager.prepare(None, "plain", nice=10) is None
    assert manager._backend is None  # no worker hop, no backend
    manager, backend = _weighing()
    assert await manager.prepare(None, "plain") is None
    assert backend.created == [] and backend.setups == 0


def _weighing_on(**kw):
    manager, backend = _weighing(**kw)
    manager.converge([("job c", ResourceLimits(cpu=1))])
    assert manager.weighting
    return manager, backend


async def test_an_uncapped_run_gets_a_weight_only_container(monkeypatch):
    manager, backend = _weighing_on(container=FakeContainer(applied=()))
    monkeypatch.setattr(limits, "_MANAGER", manager)
    collects = backend.collects
    run = await limits.prepare(None, "plain", nice=10)
    assert run is not None and run.recorded is False
    assert run.container is backend.container
    assert backend.created[-1] == (ResourceLimits(), "plain")
    assert backend.nices[-1] == 10
    assert backend.collects == collects + 1
    kwargs = {}
    run.spawn_kwargs(kwargs)
    await run.attach(42)
    # the container never fails the spawn of a run that has no caps
    assert run.container.calls[:2] == [
        ("spawn_kwargs", False),
        ("attach", 42, False),
    ]
    assert not run.refused
    assert await run.kill() is True
    record = await run.finish()
    # a record that names no caps, which the job keeps out of its run
    assert record.cpu is None and record.memory_bytes is None
    assert record.processes is None and record.mechanism is None
    assert run.abort() is record


async def test_a_weight_only_container_that_failed_is_released(caplog):
    container = FakeContainer(applied=())
    container.weight_problem = reason = "the cpu controller is off"
    manager, backend = _weighing_on(container=container)
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        assert await manager.prepare(None, "plain") is None
        assert await manager.prepare(None, "plain") is None
    assert container.calls == [("abort",), ("abort",)]
    assert _messages(caplog, logging.DEBUG) == [
        "the cpu weight of a run of plain does not apply: " + reason
        + " (each distinct failure is logged once)"
    ]
    assert _messages(caplog, logging.WARNING) == []


async def test_a_weight_only_run_logs_its_failures_at_debug(caplog):
    container = FakeContainer(applied=())
    container.attach_error = RuntimeError("bad handle")
    container.finish_error = OSError("EIO")
    manager, _ = _weighing_on(container=container)
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        run = await manager.prepare(None, "plain")
        await run.attach(42)
        await run.finish()
    assert not run.refused
    assert [r for r in caplog.records if r.levelno > logging.DEBUG] == []
    assert _messages(caplog, logging.DEBUG) == [
        "the cpu weight of a run of plain does not apply: could not attach "
        "the job to its container: bad handle (each distinct failure is "
        "logged once)",
        "could not release the container of a run of plain (each distinct "
        "failure is logged once)",
    ]


async def test_a_weight_lost_at_attach_is_logged_at_debug(caplog):
    container = FakeContainer(applied=())
    manager, _ = _weighing_on(container=container)
    run = await manager.prepare(None, "plain")
    container.weight_problem = "could not move the job into its cgroup"
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        await run.attach(42)
    assert _messages(caplog, logging.DEBUG) == [
        "the cpu weight of a run of plain does not apply: could not move "
        "the job into its cgroup (each distinct failure is logged once)"
    ]


async def test_an_uncapped_prepare_survives_a_broken_backend(caplog):
    manager, backend = _weighing_on()
    backend.create_error = RuntimeError("boom")
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        assert await manager.prepare(None, "plain") is None
    assert _messages(caplog, logging.WARNING) == []
    assert "unexpected error: boom" in caplog.text


async def test_cancelling_an_uncapped_prepare_releases_its_container():
    container = FakeContainer(applied=())
    manager, backend = _weighing_on(container=container)
    backend.create_gate = threading.Event()
    task = asyncio.ensure_future(manager.prepare(None, "plain"))
    while len(backend.created) < 1:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    backend.create_gate.set()
    await _quiesce()
    assert container.calls == [("abort",)]


async def test_a_capped_run_the_host_cannot_cap_still_gets_its_weight():
    # while cpu is on, a run whose caps all fail still gets a cgroup for
    # its weight; its record names no mechanism
    manager, backend = _weighing_on(
        capability=NO_MEMORY, container=FakeContainer(applied=())
    )
    run = await manager.prepare(ResourceLimits(memory=MIB), "m", nice=10)
    assert backend.created[-1] == (ResourceLimits(), "m")
    assert run.container is backend.container and run.recorded
    assert run.record.unenforced == ("memory",)
    assert run.record.mechanism is None
    assert run.record.reason == NO_MEMORY.reason


async def test_a_capped_run_whose_weight_container_failed_goes_without():
    container = FakeContainer(applied=())
    container.weight_problem = "the cpu controller is off"
    manager, _ = _weighing_on(capability=NO_MEMORY, container=container)
    run = await manager.prepare(ResourceLimits(memory=MIB), "m")
    assert run.container is None and container.calls == [("abort",)]
    assert run.record.unenforced == ("memory",)


async def test_a_capped_run_logs_a_weight_problem_once(caplog):
    container = FakeContainer(applied={"memory"})
    container.weight_problem = "could not set cpu.weight.nice: Permission"
    manager, _ = _weighing_on(container=container)
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        first = await manager.prepare(ResourceLimits(memory=MIB), "m")
        await manager.prepare(ResourceLimits(memory=MIB), "m")
    # the caps apply: the run keeps its container
    assert first.container is container and first.record.enforced
    assert len(_messages(caplog, logging.DEBUG)) == 1
    assert _messages(caplog, logging.WARNING) == []


# --- RunLimits -------------------------------------------------------------


async def _run(caps=None, container=None, **kw):
    manager, backend = _manager(container=container or FakeContainer(), **kw)
    run = await manager.prepare(caps or ResourceLimits(cpu=1, memory=MIB), "j")
    return run, run.container


async def test_spawn_kwargs_forwards_to_the_container():
    run, container = await _run(
        ResourceLimits(cpu=1, required=True), FakeContainer(applied={"cpu"})
    )
    kwargs = {}
    run.spawn_kwargs(kwargs)
    assert kwargs == {"joined": True}
    assert container.calls == [("spawn_kwargs", True)]


async def test_a_run_without_a_container_is_inert():
    manager, _ = _manager(capability=UNAVAILABLE)
    run = await manager.prepare(ResourceLimits(cpu=1), "j")
    kwargs = {}
    run.spawn_kwargs(kwargs)
    await run.attach(42)
    assert kwargs == {}
    assert await run.kill() is False
    record = await run.finish()
    assert record is run.record and record is await run.finish()
    assert run.abort() is record


async def test_attach_forwards_once():
    run, container = await _run()
    await run.attach(42)
    await run.attach(42)
    assert container.calls == [("attach", 42, False)]
    assert run.record.enforced


async def test_attach_records_lost_containment(caplog):
    container = FakeContainer(applied={"cpu", "memory"})
    lost = "could not move the job into its cgroup"
    container.attach_loses = {"cpu": lost, "memory": lost}
    run, _ = await _run(container=container)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await run.attach(42)
    assert run.record.unenforced == ("cpu", "memory")
    assert run.record.reason == lost
    assert not run.refused
    assert caplog.text.count(lost) == 1


async def test_attach_refusal_fails_the_run(caplog):
    container = FakeContainer(applied={"cpu"})
    container.attach_result = "Windows could not assign the job"
    run, _ = await _run(ResourceLimits(cpu=1, required=True), container)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await run.attach(42)
    assert run.refused
    record = run.record
    assert record.refused and record.unenforced == ("cpu",)
    assert record.failure == (
        "resource limits could not be enforced: Windows could not assign "
        "the job (resourceLimits.required is set)"
    )
    assert "Windows could not assign the job" in caplog.text


async def test_attach_errors_never_reach_the_caller(caplog):
    container = FakeContainer()
    container.attach_error = RuntimeError("bad handle")
    run, _ = await _run(container=container)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await run.attach(42)
    assert run.record.unenforced == ("cpu", "memory")
    assert run.record.reason == (
        "could not attach the job to its container: bad handle"
    )
    assert not run.refused and run.record.failure is None
    assert "bad handle" in caplog.text


async def test_an_attach_error_refuses_a_required_run():
    # the container killed the child before raising (the backend rule)
    container = FakeContainer()
    container.attach_error = OSError(9, "Bad file descriptor", "/x/ab-j")
    run, _ = await _run(
        ResourceLimits(cpu=1, memory=MIB, required=True), container
    )
    await run.attach(42)
    assert run.refused
    record = await run.finish()
    assert record.refused and record.unenforced == ("cpu", "memory")
    assert record.failure == (
        "resource limits could not be enforced: could not attach the job "
        "to its container: Bad file descriptor "
        "(resourceLimits.required is set)"
    )


async def test_cancelling_attach_still_lets_it_finish(caplog):
    container = FakeContainer()
    container.attach_gate = threading.Event()
    container.attach_result = "terminated the child"
    run, _ = await _run(ResourceLimits(cpu=1, required=True), container)
    task = asyncio.ensure_future(run.attach(42))
    while not container.calls:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not run.refused  # still running on its worker thread
    finishing = asyncio.ensure_future(run.finish())
    await asyncio.sleep(0.05)
    assert not finishing.done()  # finish waits for the attach
    container.attach_gate.set()
    record = await finishing
    await _quiesce()
    assert run.refused and record.refused
    assert container.calls == [("attach", 42, True), ("finish",)]
    assert _asyncio_errors(caplog) == []


@pytest.mark.parametrize("required", [True, False])
async def test_cancelling_an_attach_that_fails(caplog, required):
    container = FakeContainer()
    container.attach_gate = threading.Event()
    container.attach_error = RuntimeError("bad handle")
    run, _ = await _run(ResourceLimits(cpu=1, required=required), container)
    task = asyncio.ensure_future(run.attach(42))
    while not container.calls:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    container.attach_gate.set()
    record = await run.finish()
    await _quiesce()
    assert record.unenforced == ("cpu",)
    assert record.reason == (
        "could not attach the job to its container: bad handle"
    )
    assert record.refused is required
    assert _asyncio_errors(caplog) == []


async def test_kill_waits_for_a_running_attach():
    container = FakeContainer()
    container.attach_gate = threading.Event()
    run, _ = await _run(container=container)
    attaching = asyncio.ensure_future(run.attach(42))
    while not container.calls:
        await asyncio.sleep(0.01)
    killing = asyncio.ensure_future(run.kill())
    await asyncio.sleep(0.05)
    assert not killing.done()
    container.attach_gate.set()
    assert await killing is True
    await attaching
    assert container.calls == [("attach", 42, False), ("kill",)]


async def test_kill_reports_the_containers_answer():
    container = FakeContainer()
    container.kill_result = False
    run, _ = await _run(container=container)
    assert await run.kill() is False
    container.kill_error = OSError("gone")
    assert await run.kill() is False


async def test_finish_reads_stats_once_and_collects():
    container = FakeContainer()
    container.stats.update(
        memory_exceeded=True, process_limit_hits=2, cpu_throttled_seconds=1.25
    )
    manager, backend = _manager(container=container)
    run = await manager.prepare(ResourceLimits(cpu=1, memory=64 * MIB), "j")
    first, second = await asyncio.gather(run.finish(), run.finish())
    assert first is second
    assert container.calls == [("finish",)]
    assert backend.collects == 2  # prepare, then finish
    assert first.memory_exceeded and first.failure == (
        "memory limit exceeded (64 MiB)"
    )
    assert first.process_limit_hits == 2
    assert first.peak_memory_bytes == 4 * MIB
    assert first.cpu_throttled_seconds == 1.25
    assert run.record is first
    assert await run.finish() is first


async def test_finish_without_stats_still_records(caplog):
    container = FakeContainer()
    container.finish_error = OSError("EIO")
    manager, _ = _manager(container=container)
    caps = ResourceLimits(cpu=1)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        record = await (await manager.prepare(caps, "j")).finish()
        again = await (await manager.prepare(caps, "j")).finish()
    assert record.enforced and record.peak_memory_bytes is None
    assert again.enforced
    assert _messages(caplog, logging.WARNING) == [
        "could not read the resource limits of a run of j "
        "(each distinct failure is logged once)"
    ]


def test_the_once_only_set_is_bounded(caplog):
    manager, _ = _manager()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.note_failures("j", ["first"])
        manager.note_failures(
            "j", ["r{}".format(i) for i in range(limits._WARNED_MAX)]
        )
        manager.note_failures("j", ["first"])  # forgotten when full
    assert len(manager._warned) <= limits._WARNED_MAX
    assert len(caplog.records) == limits._WARNED_MAX + 2


async def test_finish_of_a_backend_that_returns_nothing():
    container = FakeContainer()
    container.stats = None
    run, _ = await _run(container=container)
    record = await run.finish()
    assert record.memory_exceeded is False


async def test_a_refused_run_is_final():
    manager, _ = _manager(capability=UNAVAILABLE)
    run = await manager.prepare(ResourceLimits(cpu=1, required=True), "j")
    record = run.record
    assert await run.finish() is record
    assert run.abort() is record


async def test_abort_without_a_container():
    manager, _ = _manager(capability=UNAVAILABLE)
    run = await manager.prepare(ResourceLimits(cpu=1), "j")
    record = run.abort()
    assert record.unenforced == ("cpu",)
    assert await run.finish() is record


async def test_a_cancelled_attach_worker_changes_nothing():
    run, _ = await _run()
    cancelled = asyncio.get_running_loop().create_future()
    cancelled.cancel()
    run._attached(cancelled)
    assert run.record.enforced and not run.refused


async def test_abort_releases_the_container_once():
    run, container = await _run()
    record = run.abort()
    assert run.abort() is record
    assert await run.finish() is record
    assert container.calls == [("abort",)]


@pytest.mark.parametrize("required", [True, False])
async def test_abort_after_a_failed_join(required):
    container = FakeContainer(applied={"cpu"})
    container.abort_loses = {"cpu": "could not move the job into its cgroup"}
    run, _ = await _run(ResourceLimits(cpu=1, required=required), container)
    record = run.abort()
    assert record.unenforced == ("cpu",)
    assert run.refused is required and record.refused is required
    if required:
        assert record.failure == (
            "resource limits could not be enforced: could not move the job "
            "into its cgroup (resourceLimits.required is set)"
        )


async def test_abort_survives_a_failing_container():
    container = FakeContainer()
    container.abort_error = OSError("busy")
    run, _ = await _run(container=container)
    assert run.abort().enforced


async def test_the_record_tracks_the_run_until_it_is_final():
    container = FakeContainer(applied={"cpu"})
    run, _ = await _run(ResourceLimits(cpu=1), container)
    assert run.record.enforced
    container.unenforced["cpu"] = "lost"
    assert run.record.unenforced == ("cpu",)
    final = await run.finish()
    container.unenforced.clear()
    assert run.record is final and final.unenforced == ("cpu",)
