"""The Windows Job Object backend (cronstable._jobobjects).

The hermetic tests drive the backend through a fake kernel32 and run on
every OS.  The live tests at the end make real Win32 calls; they run on
every Windows box and are skipped elsewhere.
"""

import asyncio
import contextlib
import ctypes
import itertools
import logging
import os
import queue
import subprocess
import sys
import threading
import time

import pytest

from cronstable import _jobobjects, limits, platform
from cronstable.config import ResourceLimits
from cronstable.limits import Capability
from tests._helpers import close_at_test_end

MiB = 1 << 20
GiB = 1 << 30
ALL = frozenset({"cpu", "memory", "processes"})
Win32Error = _jobobjects.Win32Error

# JOBOBJECTINFOCLASS values the backend sets and queries.
ACCOUNTING, PID_LIST, PORT, EXTENDED, CPU_RATE = 1, 3, 7, 9, 15

# JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, set from creation until the resume.
GUARD = 0x2000

# GetQueuedCompletionStatus last-error values.
WAIT_TIMEOUT, ABANDONED_WAIT_0, INVALID_HANDLE = 258, 735, 6


class _FakeJob:
    def __init__(self):
        self.info = {}
        self.pids = []
        self.peak = 0
        self.user = 0
        self.kernel = 0
        self.terminated = None


class _FakeApi:
    """kernel32 in memory: jobs, suspended children and one port.

    ``failures`` maps a call name, or ``(name, detail)`` where the detail
    is the information class or the access mask, to the exception it
    raises.
    """

    size_t_max = 2**64 - 1

    def __init__(self):
        self.calls = []
        self.failures = {}
        self.jobs = {}
        self.processes = {}
        self.suspended = set()
        self.exit_codes = {}
        self.closed = []
        self.packets = queue.Queue()
        self.waits = []
        self.load_error = None
        self.resume_hook = None
        self._next = 0x100

    def _check(self, name, detail=None):
        self.calls.append((name, detail))
        failure = self.failures.get((name, detail), self.failures.get(name))
        if failure is not None:
            raise failure

    def _handle(self):
        self._next += 4
        return self._next

    def _end(self, pid, code):
        self.exit_codes.setdefault(pid, code)
        self.suspended.discard(pid)

    def spawn(self, pid):
        self.suspended.add(pid)

    def alive(self, pid):
        return pid not in self.exit_codes

    def names(self):
        return [name for name, _ in self.calls]

    def load(self):
        if self.load_error is not None:
            raise self.load_error

    def create_job(self):
        self._check("create_job")
        handle = self._handle()
        self.jobs[handle] = _FakeJob()
        return handle

    def close_handle(self, handle):
        """Close ``handle``; a job's last handle under KILL_ON_JOB_CLOSE
        ends its processes with exit code 0, as Windows does."""
        self._check("close_handle")
        self.closed.append(handle)
        if self.flags(handle) & GUARD:
            for pid in self.jobs[handle].pids:
                self._end(pid, 0)

    def flags(self, job):
        """The LimitFlags last written to ``job``, or 0."""
        state = self.jobs.get(job)
        extended = None if state is None else state.info.get(EXTENDED)
        if extended is None:
            return 0
        return extended.BasicLimitInformation.LimitFlags

    def set_information(self, job, info_class, info):
        self._check("set_information", info_class)
        self.jobs[job].info[info_class] = type(info).from_buffer_copy(info)

    def query_information(self, job, info_class, info):
        self._check("query_information", info_class)
        state = self.jobs[job]
        live = [pid for pid in state.pids if self.alive(pid)]
        if info_class == EXTENDED:
            info.PeakJobMemoryUsed = state.peak
        elif info_class == ACCOUNTING:
            info.TotalUserTime = state.user
            info.TotalKernelTime = state.kernel
            info.ActiveProcesses = len(live)
        else:
            info.NumberOfAssignedProcesses = len(live)
            if len(live) > len(info.ProcessIdList):
                raise Win32Error(234, "More data is available.")
            info.NumberOfProcessIdsInList = len(live)
            for index, pid in enumerate(live):
                info.ProcessIdList[index] = pid

    def assign(self, job, process):
        self._check("assign")
        self.jobs[job].pids.append(self.processes[process])

    def terminate_job(self, job, code):
        self._check("terminate_job")
        self.jobs[job].terminated = code
        for pid in self.jobs[job].pids:
            self._end(pid, code)

    def open_process(self, access, pid):
        self._check("open_process", access)
        handle = self._handle()
        self.processes[handle] = pid
        return handle

    def terminate_process(self, process, code):
        self._check("terminate_process")
        self._end(self.processes[process], code)

    def resume_threads(self, pid):
        self._check("resume_threads")
        if self.resume_hook is not None:
            self.resume_hook(pid)
        self.suspended.discard(pid)
        return 1

    def create_port(self):
        self._check("create_port")
        return self._handle()

    def wait_port(self, port, timeout):
        """The next queued packet: (ok, message, key, overlapped, error)."""
        self.waits.append(timeout)
        packet = self.packets.get(timeout=60)
        if isinstance(packet, BaseException):
            raise packet
        return packet

    def post_port(self, port, key):
        self._check("post_port")
        self.packets.put((True, 0, key, None, 0))


def _backend(api=None, cpu_count=16):
    api = _FakeApi() if api is None else api
    backend = _jobobjects.Backend(api, cpu_count=lambda: cpu_count)
    close_at_test_end(backend.close)
    return api, backend


def _attached(caps, *, required=False, api=None, pid=4242):
    """A container for ``caps`` whose suspended child ``pid`` attached."""
    api, backend = _backend(api)
    container = backend.create(caps, "job")
    api.spawn(pid)
    container.spawn_kwargs({}, required=required)
    reason = container.attach(pid, required=required)
    return api, backend, container, reason


def _job(api):
    (handle,) = api.jobs
    return handle, api.jobs[handle]


def _eventually(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition never held"
        time.sleep(0.01)


def _blocking_resume(api):
    """Make the next resume wait: (started, release) events."""
    started, release = threading.Event(), threading.Event()

    def hook(pid):
        started.set()
        assert release.wait(10)

    api.resume_hook = hook
    return started, release


# --- structures and arithmetic -------------------------------------------


def test_structures_have_the_sizes_windows_gives_them():
    structs = _jobobjects._structs()
    wide = ctypes.sizeof(ctypes.c_void_p) == 8
    for name, sizes in _jobobjects._LAYOUT.items():
        assert ctypes.sizeof(getattr(structs, name)) == sizes[0 if wide else 1]
    if wide:
        basic = structs.basic_limit
        offsets = [getattr(basic, name).offset for name, _ in basic._fields_]
        assert offsets == [0, 8, 16, 24, 32, 40, 48, 56, 60]
        extended = structs.extended_limit
        assert extended.IoInfo.offset == 64
        assert extended.JobMemoryLimit.offset == 120
        assert extended.PeakJobMemoryUsed.offset == 136


def test_a_structure_of_the_wrong_size_refuses_to_load():
    built = _jobobjects._build_structs()
    size = ctypes.sizeof(built.cpu_rate)
    with pytest.raises(RuntimeError, match="cpu_rate is {} bytes".format(size)):
        _jobobjects._check_layout(built, {"cpu_rate": (size + 1, size + 1)})


@pytest.mark.parametrize(
    "cpu, count, rate",
    [
        pytest.param(0.5, 16, 312, id="half-of-16-rounds-to-even"),
        pytest.param(1, 16, 625, id="one-of-16"),
        pytest.param(1.5, 4, 3750, id="fraction"),
        pytest.param(0.01, 16, 6, id="floor-value"),
        pytest.param(0.01, 256, 1, id="never-zero"),
        pytest.param(16, 16, 10000, id="whole-machine"),
        pytest.param(32, 16, 10000, id="clamped"),
    ],
)
def test_cpu_rate_is_a_share_of_the_whole_machine(cpu, count, rate):
    assert _jobobjects.cpu_rate(cpu, count) == rate


def test_win32_error_text():
    error = Win32Error(50, "The request is not supported.\r\n")
    assert isinstance(error, OSError)
    assert error.code == 50
    assert str(error) == "The request is not supported (error 50)"
    assert str(Win32Error(87)) == "error 87"
    assert str(Win32Error(0, "the job's process has no threads")) == (
        "the job's process has no threads"
    )


# --- the thread walk ---------------------------------------------------------


class _FakeToolhelp:
    """The kernel32 calls of a thread walk over ``threads``.

    ``threads`` holds (owner pid, thread id) pairs, each suspended once.
    OpenThread fails for the ids in ``refused`` and ResumeThread for those
    in ``stuck``; those in ``running`` were never suspended.
    """

    SNAPSHOT = 0x50

    def __init__(self, threads, refused=(), stuck=(), running=()):
        self.threads = threads
        self.refused = set(refused)
        self.stuck = set(stuck)
        self.running = set(running)
        self.resumed = []
        self.closed = []
        self._next = 0

    def CreateToolhelp32Snapshot(self, flags, pid):
        assert flags == 0x4  # TH32CS_SNAPTHREAD
        return self.SNAPSHOT

    def Thread32First(self, snapshot, ref):
        self._next = 0
        return self.Thread32Next(snapshot, ref)

    def Thread32Next(self, snapshot, ref):
        if self._next == len(self.threads):
            return False
        entry = ref._obj  # the THREADENTRY32 behind ctypes.byref
        owner, thread = self.threads[self._next]
        entry.th32OwnerProcessID, entry.th32ThreadID = owner, thread
        self._next += 1
        return True

    def OpenThread(self, access, inherit, thread_id):
        return 0 if thread_id in self.refused else thread_id

    def ResumeThread(self, thread):
        """The thread's suspend count before the call, or (DWORD)-1."""
        if thread in self.stuck:
            return 0xFFFFFFFF
        if thread in self.running:
            return 0
        self.resumed.append(thread)
        return 1

    def CloseHandle(self, handle):
        self.closed.append(handle)


def _walker(threads, **failures):
    """A real Kernel32 whose kernel32 is a :class:`_FakeToolhelp`.

    Each failure reads as the next error code, from 5 up.
    """
    lib = _FakeToolhelp(threads, **failures)
    api = _jobobjects.Kernel32()
    codes = itertools.count(5)
    api.load = lambda: lib
    api._error = lambda: Win32Error(next(codes), "Refused.")
    return api, lib


def test_resume_threads_walks_past_a_thread_that_fails():
    # failing threads ahead of and behind the one that resumes, and one
    # that was never suspended, which does not count
    threads = [(4242, 1), (99, 2), (4242, 3), (4242, 4), (4242, 5)]
    api, lib = _walker(threads, refused={1}, stuck={4}, running={5})
    assert api.resume_threads(4242) == 1
    assert lib.resumed == [3]
    assert lib.closed == [3, 4, 5, lib.SNAPSHOT]


def test_resume_threads_counts_only_a_thread_that_was_suspended():
    # the primary thread is refused and the other was never suspended, so
    # the child still sits suspended
    api, lib = _walker([(4242, 1), (4242, 2)], refused={1}, running={2})
    with pytest.raises(Win32Error) as caught:
        api.resume_threads(4242)
    assert str(caught.value) == "Refused (error 5)"
    assert lib.resumed == []


def test_resume_threads_of_a_process_that_was_not_suspended():
    api, lib = _walker([(4242, 1), (4242, 2)], running={1, 2})
    with pytest.raises(Win32Error) as caught:
        api.resume_threads(4242)
    assert str(caught.value) == "the job's process has no suspended threads"
    assert lib.closed == [1, 2, lib.SNAPSHOT]


def test_resume_threads_raises_the_first_failure_when_none_resumed():
    api, lib = _walker([(4242, 1), (4242, 2)], refused={1}, stuck={2})
    with pytest.raises(Win32Error) as caught:
        api.resume_threads(4242)
    assert str(caught.value) == "Refused (error 5)"
    assert lib.resumed == []
    assert lib.SNAPSHOT in lib.closed


def test_resume_threads_of_a_pid_without_threads_names_no_pid():
    api, lib = _walker([(99, 1)])
    with pytest.raises(Win32Error) as caught:
        api.resume_threads(4242)
    assert str(caught.value) == "the job's process has no threads"
    assert lib.resumed == [] and lib.closed == [lib.SNAPSHOT]


# --- capability ------------------------------------------------------------


def test_the_backend_caps_all_three_resources():
    api, backend = _backend()
    want = Capability(
        available=True,
        mechanism="job-object",
        resources=ALL,
        reason=None,
        hint=None,
    )
    assert backend.mechanism == "job-object"
    assert backend.capability() == want
    assert backend.setup() == want
    assert backend.setup() == want  # idempotent: one watcher, one port
    assert api.names().count("create_port") == 1


def test_the_backend_is_unavailable_without_kernel32():
    api = _FakeApi()
    api.load_error = OSError("kernel32 is missing")
    _, backend = _backend(api)
    cap = backend.capability()
    assert not cap.available
    assert cap.resources == frozenset()
    assert "kernel32 is missing" in cap.reason


def test_the_default_seam_reports_this_host():
    backend = _jobobjects.Backend()
    close_at_test_end(backend.close)
    cap = backend.capability()
    assert cap.available is platform.IS_WINDOWS
    assert cap.resources == (ALL if platform.IS_WINDOWS else frozenset())
    bits = 8 * ctypes.sizeof(ctypes.c_size_t)
    assert _jobobjects.Kernel32().size_t_max == 2**bits - 1


def test_a_watcher_that_cannot_start_leaves_memory_uncapped():
    api = _FakeApi()
    api.failures["create_port"] = Win32Error(87, "The parameter is incorrect.")
    _, backend = _backend(api)
    cap = backend.setup()
    assert cap.available
    assert cap.resources == {"cpu", "processes"}
    assert "completion port" in cap.reason and "error 87" in cap.reason

    caps = ResourceLimits(cpu=1, memory=100 * MiB, processes=4)
    container = backend.create(caps, "job")
    assert container.applied == {"cpu", "processes"}
    assert container.unenforced == {"memory": cap.reason}
    assert container._entry.memory is None  # nothing for the peak check
    handle, job = _job(api)
    assert PORT not in job.info
    assert api.flags(handle) == 0x8 | GUARD
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    assert container.attach(4242, required=False) is None
    # unwatched, a process-cap hit cannot be counted
    assert container.finish()["process_limit_hits"] is None


# --- create ------------------------------------------------------------------


def test_create_sets_every_cap_on_one_job():
    api, backend = _backend()
    caps = ResourceLimits(cpu=0.5, memory=100 * MiB, processes=2)
    container = backend.create(caps, "job")
    assert container.applied == ALL
    assert container.unenforced == {}
    assert container._entry.memory == 100 * MiB  # the peak check's cap
    _, job = _job(api)
    # the port first, while the job is still empty; no notification limit
    # (class 12), which at the cap's value never fires
    order = [d for name, d in api.calls if name == "set_information"]
    assert order == [PORT, EXTENDED, CPU_RATE]
    port = job.info[PORT]
    assert port.CompletionKey == container._entry.key >= 1
    assert port.CompletionPort == backend._watcher.port
    extended = job.info[EXTENDED]
    flags = extended.BasicLimitInformation.LimitFlags
    # kill-on-close until the child runs; no breakaway flag, so the job's
    # processes stay in its Job Object
    assert flags == 0x200 | 0x8 | GUARD
    assert not flags & (0x800 | 0x1000)
    assert extended.JobMemoryLimit == 100 * MiB
    assert extended.BasicLimitInformation.ActiveProcessLimit == 2
    rate = job.info[CPU_RATE]
    assert rate.ControlFlags == 0x1 | 0x4
    assert rate.CpuRate == 312


def test_a_cpu_cap_of_the_whole_machine_sets_no_rate():
    api, backend = _backend(cpu_count=16)
    container = backend.create(ResourceLimits(cpu=16), "job")
    assert container.applied == {"cpu"}
    handle, job = _job(api)
    assert set(job.info) == {PORT, EXTENDED}
    assert api.flags(handle) == GUARD  # the guard alone


def test_a_huge_process_cap_saturates_the_dword():
    api, backend = _backend()
    backend.create(ResourceLimits(processes=2**40), "job")
    _, job = _job(api)
    limit = job.info[EXTENDED].BasicLimitInformation.ActiveProcessLimit
    assert limit == 0xFFFFFFFF


@pytest.mark.parametrize(
    "code, dfss",
    [
        pytest.param(50, True, id="dynamic-fair-share-scheduling"),
        pytest.param(87, False, id="other"),
    ],
)
def test_a_refused_cpu_rate_leaves_only_cpu_unenforced(code, dfss):
    api = _FakeApi()
    api.failures[("set_information", CPU_RATE)] = Win32Error(code, "Refused.")
    _, backend = _backend(api)
    container = backend.create(ResourceLimits(cpu=0.5, memory=64 * MiB), "job")
    assert container.applied == {"memory"}
    reason = container.unenforced["cpu"]
    assert reason.startswith(
        "Windows refused the CPU rate cap: Refused (error {})".format(code)
    )
    assert ("Dynamic Fair Share Scheduling" in reason) is dfss


def test_cpu_stays_unenforced_when_the_cpus_cannot_be_counted():
    _, backend = _backend(cpu_count=None)
    container = backend.create(ResourceLimits(cpu=1), "job")
    assert container.applied == frozenset()
    assert container.unenforced == {
        "cpu": "cronstable could not count this machine's CPUs"
    }


def test_a_memory_cap_above_size_t_is_unenforced():
    api = _FakeApi()
    api.size_t_max = 2**32 - 1  # a 32-bit build
    _, backend = _backend(api)
    caps = ResourceLimits(memory=8 * GiB, processes=4)
    container = backend.create(caps, "job")
    assert container.applied == {"processes"}
    # the exact limit: 4 GiB itself is one byte past it
    assert container.unenforced == {
        "memory": "8 GiB is above the largest memory cap Windows can set "
        "on this build (4294967295 bytes)"
    }
    assert container._entry.memory is None
    handle, _ = _job(api)
    assert api.flags(handle) == 0x8 | GUARD


@pytest.mark.parametrize(
    "caps, refused, applied",
    [
        pytest.param(
            ResourceLimits(cpu=1, memory=64 * MiB, processes=3),
            {"memory", "processes"},
            {"cpu"},
            id="both",
        ),
        pytest.param(
            ResourceLimits(processes=3), {"processes"}, set(), id="one"
        ),
    ],
)
def test_refused_extended_limits_leave_their_caps_unenforced(
    caps, refused, applied
):
    api = _FakeApi()
    api.failures[("set_information", EXTENDED)] = Win32Error(
        5, "Access is denied."
    )
    _, backend = _backend(api)
    container = backend.create(caps, "job")
    assert set(container.unenforced) == refused
    names = {"memory": "memory cap", "processes": "process cap"}
    for resource in refused:
        assert container.unenforced[resource] == (
            "Windows refused the {}: Access is denied (error 5)".format(
                names[resource]
            )
        )
    assert container.applied == applied
    assert container._entry.memory is None
    # the guard alone is refused too
    assert api.calls.count(("set_information", EXTENDED)) == 2
    assert container._entry.guard is None


def test_a_job_that_cannot_be_watched_leaves_memory_uncapped():
    api = _FakeApi()
    api.failures[("set_information", PORT)] = Win32Error(
        87, "The parameter is incorrect."
    )
    caps = ResourceLimits(memory=64 * MiB, processes=2)
    api, _, container, reason = _attached(caps, api=api)
    assert reason is None
    assert container.applied == {"processes"}
    assert container.unenforced["memory"] == (
        "Windows could not watch the Job Object for memory-cap hits: "
        "The parameter is incorrect (error 87)"
    )
    assert container.finish()["process_limit_hits"] is None


def test_a_job_object_windows_will_not_create_contains_nothing():
    api = _FakeApi()
    api.failures["create_job"] = Win32Error(1450, "Insufficient resources.")
    _, backend = _backend(api)
    container = backend.create(ResourceLimits(cpu=1, memory=64 * MiB), "job")
    reason = "Windows could not create a Job Object: Insufficient resources "
    reason += "(error 1450)"
    assert container.applied == frozenset()
    assert container.unenforced == {"cpu": reason, "memory": reason}
    kwargs = {"creationflags": 0x200}
    container.spawn_kwargs(kwargs, required=False)
    assert kwargs == {"creationflags": 0x200}  # never suspended
    assert container.attach(4242, required=False) is None
    assert container.kill() is False
    assert container.members() is None
    assert container.cpu_times() is None
    assert container.finish() == _jobobjects._no_stats()
    container.abort()
    assert "open_process" not in api.names()


# --- spawn and attach ------------------------------------------------------


def test_spawn_kwargs_asks_for_a_suspended_child(monkeypatch):
    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    _, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    kwargs = platform.new_process_group_kwargs("idle", windows=True)
    container.spawn_kwargs(kwargs, required=True)
    assert kwargs == {"creationflags": 0x200 | 0x40 | 0x4}


def test_attach_puts_the_child_in_its_job_before_resuming_it():
    caps = ResourceLimits(memory=64 * MiB, processes=2)
    api, _, container, reason = _attached(caps, required=True)
    assert reason is None
    names = api.names()
    assert (
        names.index("open_process")
        < names.index("assign")
        < names.index("resume_threads")
    )
    assert ("open_process", 0x100 | 0x1) in api.calls
    (process,) = api.processes
    assert process in api.closed
    assert 4242 not in api.suspended and api.alive(4242)
    assert container.applied == {"memory", "processes"}
    assert container.members() == [4242]
    # attach runs once
    assert container.attach(4242, required=True) is None
    assert api.names().count("assign") == 1
    assert container.kill() is True
    assert api.exit_codes[4242] == 1
    assert _job(api)[1].terminated == 1


def test_attach_without_spawn_kwargs_assigns_but_resumes_nothing():
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    assert container.attach(4242, required=False) is None
    assert "assign" in api.names()
    assert "resume_threads" not in api.names()
    handle, _ = _job(api)
    assert api.flags(handle) == 0x200  # the running child loses the guard


@pytest.mark.parametrize("required", [True, False])
def test_a_child_that_cannot_be_opened(required):
    api = _FakeApi()
    api.failures[("open_process", 0x101)] = Win32Error(5, "Access is denied.")
    caps = ResourceLimits(memory=64 * MiB)
    api, _, container, reason = _attached(caps, required=required, api=api)
    text = "cronstable could not open the job's process: Access is denied "
    text += "(error 5)"
    assert container.unenforced == {"memory": text}
    assert container.applied == frozenset()
    assert 4242 not in api.suspended
    if required:
        assert reason == text
        assert api.exit_codes[4242] == 1
        assert "resume_threads" not in api.names()
    else:
        assert reason is None
        assert api.alive(4242)  # runs uncapped
        assert container.kill() is False
        assert container.members() is None
        assert container.cpu_times() is None
        assert container.finish() == _jobobjects._no_stats()


@pytest.mark.parametrize("required", [True, False])
def test_a_child_windows_will_not_assign(required):
    api = _FakeApi()
    api.failures["assign"] = Win32Error(1816, "Not enough quota.")
    caps = ResourceLimits(memory=64 * MiB, processes=4)
    api, _, container, reason = _attached(caps, required=required, api=api)
    text = "Windows could not put the job's process in its Job Object: "
    text += "Not enough quota (error 1816)"
    assert container.unenforced == {"memory": text, "processes": text}
    assert 4242 not in api.suspended
    if required:
        assert reason == text
        assert api.exit_codes[4242] == 1
        assert "resume_threads" not in api.names()
        assert "terminate_job" not in api.names()  # it never joined
    else:
        assert reason is None
        assert api.alive(4242)
        assert container.kill() is False


def test_an_unassigned_child_that_cannot_resume_is_terminated():
    api = _FakeApi()
    api.failures["assign"] = Win32Error(1816, "Not enough quota.")
    api.failures["resume_threads"] = Win32Error(5, "Access is denied.")
    caps = ResourceLimits(memory=64 * MiB)
    api, _, container, reason = _attached(caps, api=api)
    assert reason is None
    assert container.unenforced["memory"] == (
        "Windows could not put the job's process in its Job Object: Not "
        "enough quota (error 1816); cronstable could not resume it either: "
        "Access is denied (error 5)"
    )
    assert api.exit_codes[4242] == 1


@pytest.mark.parametrize("required", [True, False])
def test_a_child_that_cannot_resume_is_terminated(required):
    api = _FakeApi()
    api.failures["resume_threads"] = Win32Error(5, "Access is denied.")
    caps = ResourceLimits(memory=64 * MiB)
    api, _, container, reason = _attached(caps, required=required, api=api)
    text = "cronstable could not resume the job's process: Access is denied "
    text += "(error 5)"
    assert reason == (text if required else None)
    assert container.unenforced == {"memory": text}
    assert _job(api)[1].terminated == 1  # through its job
    assert api.exit_codes[4242] == 1


def test_a_child_the_job_cannot_end_is_ended_through_its_process():
    api = _FakeApi()
    api.failures["resume_threads"] = Win32Error(5, "Access is denied.")
    api.failures["terminate_job"] = Win32Error(5, "Access is denied.")
    api, _, container, _reason = _attached(
        ResourceLimits(memory=64 * MiB), api=api
    )
    assert api.exit_codes[4242] == 1
    assert "terminate_process" in api.names()


def test_a_child_that_cannot_be_ended_is_resumed_rather_than_left():
    api = _FakeApi()
    api.failures["open_process"] = Win32Error(5, "Access is denied.")
    caps = ResourceLimits(memory=64 * MiB)
    api, _, container, reason = _attached(caps, required=True, api=api)
    assert reason.startswith("cronstable could not open the job's process")
    assert api.alive(4242)
    assert 4242 not in api.suspended


def test_a_fresh_handle_that_cannot_terminate_falls_back_to_resuming():
    api = _FakeApi()
    api.failures[("open_process", 0x101)] = Win32Error(5, "Access is denied.")
    api.failures["terminate_process"] = Win32Error(5, "Access is denied.")
    caps = ResourceLimits(memory=64 * MiB)
    api, _, container, _reason = _attached(caps, required=True, api=api)
    assert ("open_process", 0x1) in api.calls
    assert 4242 not in api.suspended


def test_an_unopenable_child_that_was_never_suspended_is_left_running():
    api = _FakeApi()
    api.failures[("open_process", 0x101)] = Win32Error(5, "Access is denied.")
    api, backend = _backend(api)
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    assert container.attach(4242, required=False) is None
    assert "resume_threads" not in api.names()
    assert container.applied == frozenset()


def test_an_unexpected_error_in_attach_ends_the_child_and_propagates():
    api = _FakeApi()
    api.failures["resume_threads"] = RuntimeError("boom")
    api, backend = _backend(api)
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    with pytest.raises(RuntimeError, match="boom"):
        container.attach(4242, required=False)
    assert api.exit_codes[4242] == 1
    assert container._attaching is False


def test_an_attach_after_its_job_closed_fails_cleanly():
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    backend.close()
    assert container.attach(4242, required=False) is None
    assert container.unenforced["memory"].endswith(
        "the run's Job Object is closed (error 6)"
    )
    assert "assign" not in api.names()
    assert 4242 not in api.suspended


# --- the kill-on-close guard ------------------------------------------------


@pytest.mark.parametrize(
    "caps, flags",
    [
        pytest.param(
            ResourceLimits(cpu=0.5, memory=64 * MiB, processes=3),
            0x200 | 0x8,
            id="caps",
        ),
        pytest.param(ResourceLimits(cpu=0.5), 0, id="cpu-only"),
    ],
)
def test_the_guard_comes_off_once_the_child_runs(caps, flags):
    api, _, container, reason = _attached(caps, required=True)
    assert reason is None
    handle, job = _job(api)
    writes = [
        index
        for index, call in enumerate(api.calls)
        if call == ("set_information", EXTENDED)
    ]
    # on at creation, off after the resume: a hard stop between the
    # assign and the resume ends the child
    assert len(writes) == 2
    assert writes[0] < api.calls.index(("resume_threads", None)) < writes[1]
    assert api.flags(handle) == flags  # every other limit as written
    extended = job.info[EXTENDED]
    assert extended.JobMemoryLimit == (caps.memory or 0)
    assert extended.BasicLimitInformation.ActiveProcessLimit == (
        caps.processes or 0
    )
    assert job.info[CPU_RATE].CpuRate == 312  # another class: untouched
    assert container._entry.guard is None
    assert container.applied == {
        r for r in ALL if getattr(caps, r) is not None
    }


def test_handles_closed_before_the_resume_end_the_child(caplog):
    # a daemon that dies after the assign closes its handles with the
    # child suspended in the job, and KILL_ON_JOB_CLOSE ends the child
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    started, release = _blocking_resume(api)
    worker = threading.Thread(
        target=container.attach,
        args=(4242,),
        kwargs={"required": False},
        daemon=True,
    )
    worker.start()
    assert started.wait(10)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        backend.close()
        assert api.exit_codes[4242] == 0
        release.set()
        worker.join(10)
    assert not worker.is_alive()
    # the guard closed with the job, so there is nothing to clear or log
    assert container.unenforced == {} and caplog.records == []
    assert api.calls.count(("set_information", EXTENDED)) == 1


def _stuck_guard(api, backend, required, name="job"):
    """Attach a capped run whose guard Windows will not clear."""
    caps = ResourceLimits(memory=64 * MiB, processes=2)
    api.failures.pop(("set_information", EXTENDED), None)
    container = backend.create(caps, name)
    api.failures[("set_information", EXTENDED)] = Win32Error(
        5, "Access is denied."
    )
    api.spawn(4242)
    container.spawn_kwargs({}, required=required)
    return container, container.attach(4242, required=required)


#: The warning a guard that will not clear logs, for a run of ``job``.
STUCK = (
    "Windows could not clear the kill-on-close limit of the Job Object of "
    "a run of job: Access is denied (error 5). The run keeps its caps, and "
    "it ends if cronstable exits first (each distinct failure is logged "
    "once)"
)


@pytest.mark.parametrize("required", [True, False])
def test_a_guard_windows_will_not_clear_leaves_the_run_running(
    caplog, required
):
    api, backend = _backend()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        container, reason = _stuck_guard(api, backend, required)
    # the caps still hold; the run just ends when the daemon exits
    assert reason is None
    assert container.unenforced == {}
    assert container.applied == {"memory", "processes"}
    assert 4242 not in api.suspended and api.alive(4242)
    assert "terminate_job" not in api.names()
    handle, _ = _job(api)
    assert api.flags(handle) == GUARD | 0x200 | 0x8
    assert container._entry.guard is not None
    assert [r.getMessage() for r in caplog.records] == [STUCK]
    assert caplog.records[0].levelno == logging.WARNING


def test_a_guard_that_will_not_clear_is_logged_once_per_error(caplog):
    api, backend = _backend()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        _stuck_guard(api, backend, False)
        _stuck_guard(api, backend, True, name="other")
        assert len(caplog.records) == 1
        api.failures.pop(("set_information", EXTENDED))
        container = backend.create(ResourceLimits(memory=64 * MiB), "other")
        api.failures[("set_information", EXTENDED)] = Win32Error(
            1450, "Insufficient system resources exist."
        )
        api.spawn(4343)
        container.spawn_kwargs({}, required=False)
        assert container.attach(4343, required=False) is None
    assert [r.getMessage() for r in caplog.records] == [
        STUCK,
        STUCK.replace("run of job", "run of other").replace(
            "Access is denied (error 5)",
            "Insufficient system resources exist (error 1450)",
        ),
    ]


def test_refused_caps_still_leave_the_guard_on():
    api = _FakeApi()
    write = api.set_information

    def refuse_caps(job, info_class, info):
        flags = getattr(info, "BasicLimitInformation", None)
        if flags is not None and flags.LimitFlags & (0x200 | 0x8):
            raise Win32Error(87, "The parameter is incorrect.")
        write(job, info_class, info)

    api.set_information = refuse_caps
    _, backend = _backend(api)
    container = backend.create(
        ResourceLimits(memory=64 * MiB, processes=3), "job"
    )
    handle, _ = _job(api)
    assert set(container.unenforced) == {"memory", "processes"}
    assert api.flags(handle) == GUARD  # the guard alone
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    assert container.attach(4242, required=False) is None
    assert api.flags(handle) == 0
    assert 4242 not in api.suspended and api.alive(4242)


def test_a_job_windows_will_not_guard_still_runs_its_child():
    api = _FakeApi()
    api.failures[("set_information", EXTENDED)] = Win32Error(
        5, "Access is denied."
    )
    api, _, container, reason = _attached(
        ResourceLimits(cpu=0.5), required=True, api=api
    )
    assert reason is None
    assert container.applied == {"cpu"} and container.unenforced == {}
    assert container._entry.guard is None
    # nothing to clear: one refused write, none after the resume
    assert api.calls.count(("set_information", EXTENDED)) == 1
    assert 4242 not in api.suspended and api.alive(4242)


def test_create_takes_a_nice_it_does_not_use():
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job", nice=10)
    assert container.applied == {"memory"}
    # priority classes order Windows runs; nothing is written for a weight
    order = [d for name, d in api.calls if name == "set_information"]
    assert order == [PORT, EXTENDED]


# --- the never-suspended guarantee ------------------------------------------


@pytest.mark.parametrize("end", ["abort", "finish"])
def test_an_attach_after_the_run_ended_terminates_the_child(end):
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    getattr(container, end)()
    assert container.attach(4242, required=False) is None
    assert api.exit_codes[4242] == 1
    assert 4242 not in api.suspended
    assert "assign" not in api.names()
    assert "resume_threads" not in api.names()


@pytest.mark.parametrize("end", ["abort", "finish"])
def test_ending_the_run_mid_attach_terminates_the_child(end):
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    started, release = _blocking_resume(api)
    worker = threading.Thread(
        target=container.attach,
        args=(4242,),
        kwargs={"required": False},
        daemon=True,
    )
    worker.start()
    assert started.wait(10)
    getattr(container, end)()
    assert api.exit_codes[4242] == 1
    release.set()
    worker.join(10)
    assert not worker.is_alive()
    assert 4242 not in api.suspended


async def test_cancelling_the_caller_mid_attach_still_resumes_the_child():
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    api.spawn(4242)
    container.spawn_kwargs({}, required=False)
    started, release = _blocking_resume(api)
    task = asyncio.ensure_future(
        asyncio.to_thread(container.attach, 4242, required=False)
    )
    assert await asyncio.to_thread(started.wait, 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.to_thread(_eventually, lambda: not container._attaching)
    # the worker finished on its own: the child runs, inside its job
    assert 4242 not in api.suspended and api.alive(4242)
    assert container.kill() is True


async def test_run_limits_cancelled_mid_attach_resumes_the_child():
    # RunLimits shields the worker call, so a cancelled caller still gets
    # the child resumed, and finish waits for it.
    api, backend = _backend()
    caps = ResourceLimits(memory=64 * MiB)
    run = limits.RunLimits(
        caps,
        manager=limits.LimitsManager(backend),
        name="job",
        unenforced={},
        container=backend.create(caps, "job"),
        mechanism=backend.mechanism,
    )
    api.spawn(4242)
    kwargs = {}
    run.spawn_kwargs(kwargs)
    started, release = _blocking_resume(api)
    task = asyncio.ensure_future(run.attach(4242))
    assert await asyncio.to_thread(started.wait, 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    record = await run.finish()
    assert 4242 not in api.suspended and api.alive(4242)
    assert record.enforced is True


# --- kill, finish and release ----------------------------------------------


def test_kill_reports_a_job_windows_will_not_terminate():
    api = _FakeApi()
    api, _, container, _ = _attached(ResourceLimits(processes=4), api=api)
    api.failures["terminate_job"] = Win32Error(5, "Access is denied.")
    assert container.kill() is False


def test_finish_counts_a_memory_hit_the_watcher_saw():
    caps = ResourceLimits(memory=100 * MiB, processes=2)
    api, backend, container, _ = _attached(caps)
    _, job = _job(api)
    backend._dispatch(container._entry.key, 10)  # JOB_MEMORY_LIMIT
    assert job.terminated == 1
    assert api.exit_codes[4242] == 1
    job.peak = 50 * MiB
    assert container.finish() == {
        "memory_exceeded": True,
        "process_limit_hits": 0,
        "peak_memory_bytes": 50 * MiB,
        "cpu_throttled_seconds": None,
    }


def test_finish_reads_a_refused_commit_from_the_peak():
    api, _, container, _ = _attached(ResourceLimits(memory=100 * MiB))
    _job(api)[1].peak = 300 * MiB  # Windows counts the refused commit
    stats = container.finish()
    assert stats["memory_exceeded"] is True
    assert stats["peak_memory_bytes"] == 100 * MiB


def test_finish_reports_the_peak_of_a_run_within_its_cap():
    api, _, container, _ = _attached(ResourceLimits(memory=100 * MiB))
    _job(api)[1].peak = 60 * MiB
    stats = container.finish()
    assert stats["memory_exceeded"] is False
    assert stats["peak_memory_bytes"] == 60 * MiB
    assert stats["process_limit_hits"] is None


def test_finish_without_a_memory_cap_reports_the_raw_peak():
    api, backend, container, _ = _attached(ResourceLimits(processes=2))
    _job(api)[1].peak = 300 * MiB
    backend._dispatch(container._entry.key, 3)  # ACTIVE_PROCESS_LIMIT
    stats = container.finish()
    assert stats["memory_exceeded"] is False
    assert stats["peak_memory_bytes"] == 300 * MiB
    assert stats["process_limit_hits"] == 1


#: The largest SIZE_T on a 32-bit build, where WOW64 saturates the peak.
SIZE_T_32 = 2**32 - 1


@pytest.mark.parametrize(
    "caps, peak, want, exceeded",
    [
        pytest.param(
            ResourceLimits(processes=4), SIZE_T_32, None, False, id="uncapped"
        ),
        pytest.param(
            ResourceLimits(memory=8 * GiB, processes=4),
            SIZE_T_32,
            None,
            False,
            id="cap-above-size-t",
        ),
        pytest.param(
            ResourceLimits(processes=4),
            SIZE_T_32 - 1,
            SIZE_T_32 - 1,
            False,
            id="below-saturation",
        ),
        # a cap fits in SIZE_T and refuses commits past it, so the clamp
        # reads true
        pytest.param(
            ResourceLimits(memory=3 * GiB),
            SIZE_T_32,
            3 * GiB,
            True,
            id="capped",
        ),
    ],
)
def test_a_saturated_peak_on_a_32_bit_build(caps, peak, want, exceeded):
    api = _FakeApi()
    api.size_t_max = SIZE_T_32
    api, _, container, _ = _attached(caps, api=api)
    _job(api)[1].peak = peak
    stats = container.finish()
    assert stats["peak_memory_bytes"] == want
    assert stats["memory_exceeded"] is exceeded


def test_finish_survives_a_failed_peak_query():
    api = _FakeApi()
    api.failures[("query_information", EXTENDED)] = Win32Error(6)
    api, _, container, _ = _attached(ResourceLimits(memory=64 * MiB), api=api)
    stats = container.finish()
    assert stats["memory_exceeded"] is False
    assert stats["peak_memory_bytes"] is None


def test_finish_is_idempotent():
    api, _, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    api.exit_codes[4242] = 0
    first = container.finish()
    calls = len(api.calls)
    assert container.finish() == first
    assert len(api.calls) == calls


def test_finish_after_the_handles_closed_reads_nothing():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    backend.close()
    assert container.finish()["peak_memory_bytes"] is None


def test_finish_closes_an_empty_job_at_once():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, _ = _job(api)
    api.exit_codes[4242] = 0  # the child exited
    container.finish()
    assert handle in api.closed
    assert backend._entries == {}
    assert container.kill() is False
    assert container.members() is None


def test_a_job_with_leftovers_stays_open_until_it_empties():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, job = _job(api)
    key = container._entry.key
    container.finish()  # 4242 is a leftover that still runs
    assert handle not in api.closed
    assert container.members() == [4242]
    assert container.kill() is True  # still killable
    backend._dispatch(key, 4)  # ACTIVE_PROCESS_ZERO
    assert handle in api.closed
    assert backend._entries == {}
    # a late message for a forgotten key is ignored
    terminations = api.names().count("terminate_job")
    backend._dispatch(key, 10)
    assert api.names().count("terminate_job") == terminations


def test_zero_before_finish_closes_without_another_query():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, _ = _job(api)
    backend._dispatch(container._entry.key, 4)  # ACTIVE_PROCESS_ZERO
    assert handle not in api.closed  # the run is not finished yet
    container.finish()
    assert handle in api.closed
    assert ("query_information", ACCOUNTING) not in api.calls


def test_collect_closes_a_released_job_once_it_empties():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, _ = _job(api)
    container.finish()
    backend.collect()
    assert handle not in api.closed
    api.exit_codes[4242] = 0
    backend.collect()
    assert handle in api.closed


def test_a_failed_accounting_query_keeps_the_job_for_later():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, _ = _job(api)
    api.exit_codes[4242] = 0
    api.failures[("query_information", ACCOUNTING)] = Win32Error(6)
    container.finish()
    assert handle not in api.closed
    del api.failures[("query_information", ACCOUNTING)]
    backend.collect()
    assert handle in api.closed


def test_abort_releases_the_job_of_a_spawn_that_failed():
    api, backend = _backend()
    container = backend.create(ResourceLimits(memory=64 * MiB), "job")
    handle, _ = _job(api)
    container.spawn_kwargs({}, required=False)
    container.abort()
    assert handle in api.closed
    assert container.finish() == _jobobjects._no_stats()


def test_close_closes_every_handle_and_stops_the_watcher(caplog):
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    handle, _ = _job(api)
    watcher = backend._watcher
    backend.close()
    assert handle in api.closed and watcher.port in api.closed
    assert not watcher._thread.is_alive()
    assert caplog.records == []  # the shutdown key is no failure
    backend.close()  # idempotent
    # the guard came off at the resume: the job's processes outlive the
    # handles
    assert api.alive(4242)


def test_close_skips_a_job_the_watcher_closed_meanwhile():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    entry = container._entry
    entry.handle = None  # as if ACTIVE_PROCESS_ZERO closed it mid-close
    closes = api.names().count("close_handle")
    backend.close()
    assert api.names().count("close_handle") == closes + 1  # the port


# --- watcher messages --------------------------------------------------------


def test_watcher_messages_update_the_run():
    caps = ResourceLimits(memory=64 * MiB, processes=2)
    api, backend, container, _ = _attached(caps)
    entry = container._entry
    backend._dispatch(entry.key, 3)  # ACTIVE_PROCESS_LIMIT
    backend._dispatch(entry.key, 3)
    assert entry.process_limit_hits == 2
    backend._dispatch(entry.key, 6)  # NEW_PROCESS: not one the backend uses
    backend._dispatch(entry.key, 11)  # NOTIFICATION_LIMIT: none is set
    backend._dispatch(entry.key + 100, 10)  # a key no run holds
    assert not entry.memory_exceeded
    assert api.alive(4242)
    backend._dispatch(entry.key, 10)  # JOB_MEMORY_LIMIT
    assert entry.memory_exceeded
    assert api.exit_codes[4242] == 1


def test_a_memory_message_for_a_closed_job_terminates_nothing():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    entry = container._entry
    backend.close()
    backend._on_message(entry, 10)
    assert entry.memory_exceeded
    assert "terminate_job" not in api.names()


def test_zero_for_a_closed_released_job_closes_nothing_twice():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    entry = container._entry
    api.exit_codes[4242] = 0
    container.finish()
    closes = api.names().count("close_handle")
    backend._on_message(entry, 4)
    assert api.names().count("close_handle") == closes


def test_a_failed_terminate_on_a_memory_hit_is_survived():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    api.failures["terminate_job"] = Win32Error(5)
    backend._dispatch(container._entry.key, 10)
    assert container._entry.memory_exceeded


def test_the_watcher_thread_drains_the_port_until_the_shutdown_key():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    key = container._entry.key
    api.packets.put((False, 0, 0, 0x1234, 87))  # a failed I/O packet: skipped
    api.packets.put((True, 6, key + 100, 4242, 0))  # no run holds the key
    api.packets.put((True, 10, key, None, 0))  # the run hit its memory cap
    _eventually(lambda: not api.alive(4242))
    assert container._entry.memory_exceeded
    assert api.waits[0] == 1000  # ms, so a quiet port still sweeps
    watcher = backend._watcher
    backend.close()  # posts key 0
    assert not watcher._thread.is_alive()
    assert ("post_port", None) in api.calls


def _watcher(api, dispatch=None, **kwargs):
    """A watcher whose messages, sweeps and end reasons land in lists."""
    seen, swept, ended = [], [], []

    def record(key, message):
        seen.append((key, message))

    watcher = _jobobjects._Watcher(
        api,
        dispatch or record,
        lambda: swept.append(True),
        ended.append,
        **kwargs,
    )
    close_at_test_end(watcher.stop)
    return watcher, seen, swept, ended


@pytest.mark.parametrize(
    "packet, cause",
    [
        pytest.param(
            (False, 0, 0, None, ABANDONED_WAIT_0),
            "error 735",
            id="closed-mid-wait",
        ),
        pytest.param(
            (False, 0, 0, None, INVALID_HANDLE), "error 6", id="closed"
        ),
        pytest.param(
            OSError("the port is gone"), "the port is gone", id="raises"
        ),
        pytest.param(RuntimeError(), "RuntimeError", id="raises-blank"),
    ],
)
def test_the_watcher_thread_ends_when_its_port_fails(packet, cause):
    api = _FakeApi()
    watcher, _, swept, ended = _watcher(api)
    api.packets.put(packet)
    watcher._thread.join(10)
    assert not watcher._thread.is_alive()
    assert swept == []  # a closed port is no timeout
    assert ended == [
        "waiting on its completion port failed ({})".format(cause)
    ]
    api.failures["post_port"] = Win32Error(6)
    api.failures["close_handle"] = Win32Error(6)
    watcher.stop()  # survives a port that is already broken
    assert len(ended) == 1


def _errors(caplog):
    return [
        r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR
    ]


@pytest.mark.parametrize(
    "packet, cause",
    [
        pytest.param(
            (False, 0, 0, None, ABANDONED_WAIT_0), "error 735", id="closed"
        ),
        pytest.param(
            OSError("the port is gone"), "the port is gone", id="raises"
        ),
    ],
)
def test_a_stopped_watcher_leaves_new_runs_without_a_memory_cap(
    caplog, packet, cause
):
    api, backend = _backend()
    caps = ResourceLimits(cpu=1, memory=64 * MiB, processes=4)
    assert backend.create(caps, "job").applied == ALL
    watcher = backend._watcher
    api.packets.put(packet)
    watcher._thread.join(10)
    assert not watcher._thread.is_alive()
    reason = (
        "the Job Object watcher stopped: waiting on its completion port "
        "failed ({})".format(cause)
    )
    assert _errors(caplog) == [
        reason + ". Runs that start from now on get no memory cap until "
        "cronstable restarts."
    ]
    cap = backend.capability()
    assert cap.available and cap.resources == {"cpu", "processes"}
    assert cap.reason == reason
    assert backend.setup() == cap
    later = backend.create(caps, "job")
    assert later.applied == {"cpu", "processes"}
    assert later.unenforced == {"memory": reason}
    assert later._entry.memory is None  # nothing for the peak check
    assert PORT not in api.jobs[later._entry.handle].info
    assert api.flags(later._entry.handle) == 0x8 | GUARD
    assert api.names().count("create_port") == 1  # never restarted
    assert len(_errors(caplog)) == 1


#: The reasons a watcher whose port closed (error 735) leaves behind.
STOPPED = (
    "the Job Object watcher stopped: waiting on its completion port failed "
    "(error 735)"
)
LOST = (
    "the Job Object watcher stopped during the run: waiting on its "
    "completion port failed (error 735)"
)


def _stop_watcher(api, backend):
    """Close the watcher's port under its wait and join its thread."""
    watcher = backend._watcher
    api.packets.put((False, 0, 0, None, ABANDONED_WAIT_0))
    watcher._thread.join(10)
    assert not watcher._thread.is_alive()


def test_a_run_alive_when_the_watcher_stops_loses_its_memory_cap():
    caps = ResourceLimits(cpu=1, memory=64 * MiB, processes=4)
    api, backend, container, _ = _attached(caps)
    entry = container._entry
    backend._dispatch(entry.key, 3)  # ACTIVE_PROCESS_LIMIT, still counted
    _stop_watcher(api, backend)
    assert entry.watched is False and entry.lost == LOST
    # the commit limit stays on the job: Windows still refuses past it
    _job(api)[1].peak = 100 * MiB
    assert api.alive(4242)  # but nothing kills the run at the cap
    stats = container.finish()
    assert stats["memory_exceeded"] is True
    assert stats["peak_memory_bytes"] == 64 * MiB
    assert stats["process_limit_hits"] is None  # not the 1 seen before
    assert container.unenforced == {"memory": LOST}
    assert container.applied == ALL


def test_a_run_without_a_memory_cap_loses_only_its_count():
    api, backend, container, _ = _attached(ResourceLimits(processes=4))
    _stop_watcher(api, backend)
    assert container.finish()["process_limit_hits"] is None
    assert container.unenforced == {}


async def test_the_record_of_a_run_the_watcher_lost():
    api, backend = _backend()
    manager = limits.LimitsManager(backend)
    caps = ResourceLimits(memory=64 * MiB, processes=4, required=True)
    run = await manager.prepare(caps, "job")
    api.spawn(4242)
    run.spawn_kwargs({})
    await run.attach(4242)
    assert not run.refused
    _stop_watcher(api, backend)
    record = await run.finish()
    assert record.enforced is False and record.unenforced == ("memory",)
    assert record.reason == LOST
    assert record.mechanism == "job-object"
    assert record.process_limit_hits is None
    assert record.failure is None  # it ran; only a refusal fails it


def test_a_watcher_that_stops_during_the_association_watches_nothing():
    api, backend = _backend()
    backend.setup()
    associate = api.set_information

    def stopping_first(job, info_class, info):
        if info_class == PORT:
            _stop_watcher(api, backend)
        associate(job, info_class, info)

    api.set_information = stopping_first
    caps = ResourceLimits(memory=64 * MiB, processes=4)
    container = backend.create(caps, "job")
    assert container.applied == {"processes"}
    assert container.unenforced == {"memory": STOPPED}
    entry = container._entry
    assert entry.watched is False and entry.lost is None
    assert entry.memory is None  # nothing for the peak check
    assert api.flags(entry.handle) == 0x8 | GUARD


async def test_the_manager_follows_a_watcher_that_stopped(caplog):
    api, backend = _backend()
    manager = limits.LimitsManager(backend)
    memory = ResourceLimits(memory=64 * MiB)
    capped = [("job a", memory)]
    with caplog.at_level(logging.INFO, logger="cronstable"):
        manager.converge(capped)
        assert manager.capability().resources == ALL
        _stop_watcher(api, backend)
        cap = manager.capability()
        assert cap.available and cap.resources == {"cpu", "processes"}
        assert cap.reason == STOPPED
        manager.converge(capped)
    warnings = [
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert warnings == [
        "resourceLimits is set (job a), but cronstable cannot enforce the "
        "memory cap: " + STOPPED + ". Those jobs run without that cap."
    ]
    jobs = len(api.jobs)
    run = await manager.prepare(memory, "job a")
    # every configured cap is missing: no Job Object, no suspended child
    assert run.container is None and len(api.jobs) == jobs
    assert run.record.mechanism is None
    assert run.record.unenforced == ("memory",)
    assert run.record.reason == STOPPED
    required = await manager.prepare(
        ResourceLimits(memory=64 * MiB, required=True), "job a"
    )
    assert required.refused and len(api.jobs) == jobs
    mixed = await manager.prepare(
        ResourceLimits(cpu=1, memory=64 * MiB), "job a"
    )
    assert mixed.record.mechanism == "job-object"
    assert mixed.record.unenforced == ("memory",)
    assert mixed.container.applied == {"cpu"}
    assert api.names().count("create_port") == 1  # never restarted
    mixed.abort()


def test_closing_the_backend_is_no_watcher_failure(caplog):
    api, backend = _backend()
    backend.setup()
    watcher = backend._watcher

    def closed_under_the_wait(port, key):
        # the port breaks before the shutdown key arrives
        api.packets.put((False, 0, 0, None, ABANDONED_WAIT_0))

    api.post_port = closed_under_the_wait
    backend.close()
    assert not watcher._thread.is_alive()
    assert _errors(caplog) == []
    assert backend.capability().resources == ALL


def test_one_failing_message_never_stops_the_watcher():
    api = _FakeApi()
    seen = []

    def dispatch(key, message):
        if message == 99:
            raise RuntimeError("bad message")
        seen.append((key, message))

    _watcher(api, dispatch)
    api.packets.put((True, 99, 1, None, 0))
    api.packets.put((True, 3, 1, None, 0))
    _eventually(lambda: seen == [(1, 3)])


def test_a_wait_that_times_out_sweeps_and_keeps_watching():
    api = _FakeApi()
    watcher, seen, swept, ended = _watcher(api, interval=3600)
    api.packets.put((False, 0, 0, None, WAIT_TIMEOUT))
    api.packets.put((False, 0, 0, None, WAIT_TIMEOUT))
    api.packets.put((True, 3, 1, None, 0))  # not due: no sweep
    _eventually(lambda: seen == [(1, 3)])
    watcher.stop()
    assert swept == [True, True]
    assert ended == []  # the shutdown key is no failure


@pytest.mark.parametrize(
    "interval, sweeps",
    [
        pytest.param(0, 2, id="due"),
        pytest.param(3600, 0, id="not-due"),
    ],
)
def test_a_busy_port_still_sweeps_once_a_sweep_is_due(interval, sweeps):
    # messages that keep the wait from timing out cannot starve the sweep
    api = _FakeApi()
    watcher, seen, swept, _ = _watcher(api, interval=interval)
    api.packets.put((True, 6, 1, None, 0))
    api.packets.put((True, 7, 1, None, 0))
    _eventually(lambda: seen == [(1, 6), (1, 7)])
    watcher.stop()  # joins the thread, so the sweeps are all in
    assert len(swept) == sweeps


def test_a_failing_sweep_never_stops_the_watcher():
    api = _FakeApi()
    seen, ended = [], []

    def sweep():
        raise RuntimeError("bad sweep")

    watcher = _jobobjects._Watcher(
        api, lambda key, message: seen.append(message), sweep, ended.append
    )
    close_at_test_end(watcher.stop)
    api.packets.put((False, 0, 0, None, WAIT_TIMEOUT))
    api.packets.put((True, 3, 1, None, 0))
    _eventually(lambda: seen == [3])
    assert watcher._thread.is_alive()
    assert ended == []


# --- the peak check ----------------------------------------------------------


def test_the_peak_check_kills_a_job_past_its_memory_cap():
    caps = ResourceLimits(memory=100 * MiB, processes=4)
    api, backend, container, _ = _attached(caps)
    _, job = _job(api)
    job.peak = 100 * MiB  # at the cap: no hit
    backend._sweep()
    assert api.alive(4242) and not container._entry.memory_exceeded
    job.peak = 100 * MiB + 1  # a refused commit counts in the peak
    backend._sweep()
    assert container._entry.memory_exceeded
    assert job.terminated == 1 and api.exit_codes[4242] == 1
    assert container.finish()["memory_exceeded"] is True


def test_a_quiet_port_kills_a_job_whose_memory_message_was_lost():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    _job(api)[1].peak = GiB  # and no JOB_MEMORY_LIMIT arrives
    api.packets.put((False, 0, 0, None, WAIT_TIMEOUT))
    _eventually(lambda: not api.alive(4242))
    assert container._entry.memory_exceeded
    assert backend._watcher._thread.is_alive()


def test_a_failed_peak_query_leaves_the_job_running():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    _job(api)[1].peak = GiB
    api.failures[("query_information", EXTENDED)] = Win32Error(6)
    backend._sweep()
    assert api.alive(4242) and not container._entry.memory_exceeded


def test_a_failed_terminate_on_a_peak_hit_is_survived():
    api, backend, container, _ = _attached(ResourceLimits(memory=64 * MiB))
    _job(api)[1].peak = GiB
    api.failures["terminate_job"] = Win32Error(5)
    backend._sweep()
    assert container._entry.memory_exceeded


def test_the_peak_check_queries_only_live_memory_capped_jobs():
    api, backend = _backend()
    uncapped = backend.create(ResourceLimits(processes=4), "job")
    unattached = backend.create(ResourceLimits(memory=64 * MiB), "job")
    hit = backend.create(ResourceLimits(memory=64 * MiB), "job")
    closed = backend.create(ResourceLimits(memory=64 * MiB), "job")
    for pid, container in enumerate((uncapped, hit, closed), 1):
        api.spawn(pid)
        container.spawn_kwargs({}, required=False)
        assert container.attach(pid, required=False) is None
    for job in api.jobs.values():
        job.peak = GiB
    backend._dispatch(hit._entry.key, 10)  # killed by its message already
    closed._entry.handle = None  # as if closed after the sweep's snapshot
    terminations = api.names().count("terminate_job")
    backend._sweep()
    assert ("query_information", EXTENDED) not in api.calls
    assert api.names().count("terminate_job") == terminations
    assert not unattached._entry.memory_exceeded
    assert api.alive(1) and api.alive(3)


# --- members and CPU totals --------------------------------------------------


def test_members_grows_its_buffer_for_a_large_job():
    api, _, container, _ = _attached(ResourceLimits(processes=64))
    _, job = _job(api)
    job.pids.extend(range(5000, 5040))
    assert container.members() == [4242] + list(range(5000, 5040))
    assert api.calls.count(("query_information", PID_LIST)) == 2


def test_members_gives_up_on_other_errors_and_a_job_that_keeps_growing():
    api, _, container, _ = _attached(ResourceLimits(processes=64))
    api.failures[("query_information", PID_LIST)] = Win32Error(24)
    assert container.members() is None
    api.failures[("query_information", PID_LIST)] = Win32Error(234)
    assert container.members() is None
    attempts = api.calls.count(("query_information", PID_LIST)) - 1
    assert attempts == _jobobjects._PID_LIST_ATTEMPTS


def test_cpu_times_are_job_totals_in_seconds():
    api, _, container, _ = _attached(ResourceLimits(cpu=1))
    _, job = _job(api)
    job.user, job.kernel = 15_000_000, 2_500_000  # 100 ns ticks
    assert container.cpu_times() == (1.5, 0.25)
    api.failures[("query_information", ACCOUNTING)] = Win32Error(6)
    assert container.cpu_times() is None


# --- live: real Job Objects -------------------------------------------------

_LIVE = pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="Job Objects are Windows-only"
)

#: One process per interpreter: a venv's python.exe is a launcher that runs
#: the base interpreter as a second process, which a process cap counts.
_PYTHON = getattr(sys, "_base_executable", None) or sys.executable

#: A child that starts a grandchild first thing and prints its pid.
_GRANDCHILD = (
    "import subprocess, sys, time\n"
    "grandchild = subprocess.Popen(\n"
    "    [sys.executable, '-c', 'import time; time.sleep(60)']{flags}\n"
    ")\n"
    "print(grandchild.pid, flush=True)\n"
    "time.sleep(60)\n"
)


def _live_backend():
    backend = _jobobjects.Backend()
    close_at_test_end(backend.close)
    assert backend.setup().resources == ALL
    return backend


async def _live_spawn(backend, caps, code):
    """Spawn ``code`` the way a capped run spawns, and attach it."""
    container = backend.create(caps, "live")
    assert container.unenforced == {}
    kwargs = platform.new_process_group_kwargs()
    container.spawn_kwargs(kwargs, required=True)
    assert kwargs["creationflags"] & 0x4  # CREATE_SUSPENDED
    proc = await asyncio.create_subprocess_exec(
        _PYTHON, "-c", code, stdout=asyncio.subprocess.PIPE, **kwargs
    )
    try:
        reason = await asyncio.to_thread(
            container.attach, proc.pid, required=True
        )
    except BaseException:
        await _live_reap(container, proc)
        raise
    assert reason is None
    return container, proc


async def _live_reap(container, proc):
    """End whatever the run left, then check nothing is left running."""
    await asyncio.to_thread(container.kill)
    if proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
    await asyncio.wait_for(proc.communicate(), 30)
    deadline = time.monotonic() + 10
    while container.members() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert container.members() in ([], None)


async def _alive_until(pid, timeout=10.0):
    deadline = time.monotonic() + timeout
    while platform.pid_alive(pid) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    return platform.pid_alive(pid)


@_LIVE
@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda api, s: api.set_information(0, 15, s.cpu_rate()),
            id="set_information",
        ),
        # NULL names the caller's own job here, so an unknown class fails
        pytest.param(
            lambda api, s: api.query_information(0, 999, s.accounting()),
            id="query_information",
        ),
        pytest.param(lambda api, s: api.assign(0, 0), id="assign"),
        pytest.param(lambda api, s: api.terminate_job(0, 1), id="terminate"),
        pytest.param(
            lambda api, s: api.is_process_in_job(0, None), id="in-job"
        ),
        pytest.param(lambda api, s: api.open_process(0x1, 0), id="open"),
        pytest.param(
            lambda api, s: api.terminate_process(0, 1), id="end-process"
        ),
        pytest.param(lambda api, s: api.post_port(0, 0), id="post"),
        # the idle process's threads have no id OpenThread accepts
        pytest.param(lambda api, s: api.resume_threads(0), id="open-thread"),
        pytest.param(
            lambda api, s: api.resume_threads(0xFFFFFFFC), id="no-threads"
        ),
    ],
)
def test_live_a_failed_kernel32_call_raises_win32_error(call):
    api = _jobobjects.Kernel32()
    with pytest.raises(Win32Error) as caught:
        call(api, _jobobjects._structs())
    assert str(caught.value)


@_LIVE
async def test_live_a_child_runs_to_completion_inside_its_job():
    backend = _live_backend()
    caps = ResourceLimits(cpu=1, memory=256 * MiB, processes=4)
    container, proc = await _live_spawn(backend, caps, "print('ran')")
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), 30)
        stats = await asyncio.to_thread(container.finish)
    finally:
        await _live_reap(container, proc)
    assert out.strip() == b"ran" and proc.returncode == 0
    assert stats["memory_exceeded"] is False
    assert 0 < stats["peak_memory_bytes"] < 256 * MiB
    assert stats["process_limit_hits"] == 0
    assert container._entry.handle is None  # empty, so released at once


@_LIVE
async def test_live_a_grandchild_started_at_once_is_in_the_job():
    backend = _live_backend()
    caps = ResourceLimits(memory=256 * MiB, processes=8)
    code = _GRANDCHILD.format(flags="")
    container, proc = await _live_spawn(backend, caps, code)
    try:
        grandchild = int(await asyncio.wait_for(proc.stdout.readline(), 30))
        api, job = backend._api, container._entry.handle
        for pid in (proc.pid, grandchild):
            handle = api.open_process(0x1000, pid)  # QUERY_LIMITED_INFO
            try:
                assert api.is_process_in_job(handle, job), pid
            finally:
                api.close_handle(handle)
        assert {proc.pid, grandchild} <= set(container.members())
        user, system = container.cpu_times()
        assert user >= 0 and system >= 0
    finally:
        await _live_reap(container, proc)
    assert proc.returncode == 1
    assert not await _alive_until(grandchild)


@_LIVE
async def test_live_kill_reaches_a_grandchild_in_a_new_process_group():
    backend = _live_backend()
    code = _GRANDCHILD.format(flags=", creationflags=0x200")
    container, proc = await _live_spawn(
        backend, ResourceLimits(processes=8), code
    )
    try:
        grandchild = int(await asyncio.wait_for(proc.stdout.readline(), 30))
        assert await asyncio.to_thread(container.kill) is True
        await asyncio.wait_for(proc.wait(), 30)
        assert not await _alive_until(grandchild)
    finally:
        await _live_reap(container, proc)
    assert proc.returncode == 1


#: Allocates past a 100 MiB cap, then outlives the refusal: only the cap's
#: kill ends it within a minute.
_HOG = (
    "import time\n"
    "try:\n"
    "    hog = bytearray(400 << 20)\n"
    "except MemoryError:\n"
    "    time.sleep(60)\n"
)


async def _live_hog(backend):
    """Run :data:`_HOG` under a 100 MiB cap: (seconds to die, stats)."""
    started = time.monotonic()
    container, proc = await _live_spawn(
        backend, ResourceLimits(memory=100 * MiB), _HOG
    )
    try:
        await asyncio.wait_for(proc.wait(), 30)
        elapsed = time.monotonic() - started
        stats = await asyncio.to_thread(container.finish)
    finally:
        await _live_reap(container, proc)
    assert proc.returncode == 1
    return elapsed, stats


@_LIVE
async def test_live_a_run_that_hits_its_memory_cap_is_killed(monkeypatch):
    backend = _live_backend()
    # no peak check: only the JOB_MEMORY_LIMIT message can kill the hog
    monkeypatch.setattr(backend._watcher, "_sweep", lambda: None)
    elapsed, stats = await _live_hog(backend)
    assert elapsed < 10
    assert stats["memory_exceeded"] is True
    assert stats["peak_memory_bytes"] == 100 * MiB


@_LIVE
async def test_live_the_peak_check_kills_a_run_whose_message_is_lost(
    monkeypatch,
):
    backend = _live_backend()
    deliver = backend._on_message

    def lose_memory_messages(entry, message):
        if message != 10:  # JOB_MEMORY_LIMIT
            deliver(entry, message)

    monkeypatch.setattr(backend, "_on_message", lose_memory_messages)
    elapsed, stats = await _live_hog(backend)
    assert elapsed < 10  # a sweep about a second after the port goes quiet
    assert stats["memory_exceeded"] is True
    assert stats["peak_memory_bytes"] == 100 * MiB


@_LIVE
def test_live_a_timed_out_wait_is_told_from_a_closed_port():
    api = _jobobjects.Kernel32()
    port = api.create_port()
    results = []
    waiter = threading.Thread(
        target=lambda: results.append(api.wait_port(port, 10_000)),
        daemon=True,
    )
    try:
        ok, _, _, overlapped, error = api.wait_port(port, 0)
        assert (ok, overlapped, error) == (False, None, WAIT_TIMEOUT)
        waiter.start()
        time.sleep(0.3)  # let the wait begin
    finally:
        api.close_handle(port)
    waiter.join(10)
    ok, _, _, overlapped, error = results[0]
    assert (ok, overlapped) == (False, None)
    # ERROR_INVALID_HANDLE only if the close beat the wait
    assert error in (ABANDONED_WAIT_0, INVALID_HANDLE)


@_LIVE
async def test_live_a_cpu_cap_holds_a_busy_loop_near_its_share():
    backend = _live_backend()
    busy = (
        "import time\n"
        "end = time.perf_counter() + 2\n"
        "while time.perf_counter() < end:\n"
        "    pass\n"
    )
    started = time.perf_counter()
    container, proc = await _live_spawn(
        backend, ResourceLimits(cpu=0.5), busy
    )
    try:
        await asyncio.wait_for(proc.wait(), 60)
        wall = time.perf_counter() - started
        user, system = container.cpu_times()
    finally:
        await _live_reap(container, proc)
    cores = (user + system) / wall
    # one busy thread runs a whole core uncapped; the cap is half of one
    assert 0.1 < cores < 0.75, (cores, os.cpu_count())


@_LIVE
async def test_live_a_process_cap_refuses_the_spawn_past_it():
    backend = _live_backend()
    forks = (
        "import subprocess, sys\n"
        "first = subprocess.Popen(\n"
        "    [sys.executable, '-c', 'import time; time.sleep(60)']\n"
        ")\n"
        "try:\n"
        "    subprocess.Popen([sys.executable, '-c', 'pass']).wait()\n"
        "except OSError as ex:\n"
        "    print('refused', ex.winerror, flush=True)\n"
        "else:\n"
        "    print('started', flush=True)\n"
        "first.kill()\n"
        "first.wait()\n"
    )
    container, proc = await _live_spawn(
        backend, ResourceLimits(processes=2), forks
    )
    try:
        line = await asyncio.wait_for(proc.stdout.readline(), 30)
        await asyncio.wait_for(proc.wait(), 30)
        entry = container._entry
        deadline = time.monotonic() + 10  # the message arrives on its own
        while not entry.process_limit_hits and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        stats = await asyncio.to_thread(container.finish)
    finally:
        await _live_reap(container, proc)
    assert line.split() == [b"refused", b"1816"]
    assert proc.returncode == 0
    assert stats["process_limit_hits"] >= 1


@_LIVE
async def test_live_a_child_whose_run_ended_before_attach_is_terminated():
    backend = _live_backend()
    container = backend.create(ResourceLimits(memory=256 * MiB), "live")
    kwargs = platform.new_process_group_kwargs()
    container.spawn_kwargs(kwargs, required=False)
    proc = await asyncio.create_subprocess_exec(
        _PYTHON, "-c", "print('ran')", stdout=asyncio.subprocess.PIPE, **kwargs
    )
    try:
        container.abort()
        assert await asyncio.to_thread(
            container.attach, proc.pid, required=False
        ) is None
        out, _ = await asyncio.wait_for(proc.communicate(), 30)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    assert proc.returncode == 1
    assert out == b""


# --- live: a hard stop of the daemon -----------------------------------------

_SLEEPER = "import time; time.sleep(60)"


@_LIVE
async def test_live_the_guard_clears_on_a_job_nested_in_another():
    backend = _live_backend()
    api = backend._api
    outer = api.create_job()
    close_at_test_end(lambda: api.close_handle(outer))
    caps = ResourceLimits(memory=256 * MiB, processes=4)
    container = backend.create(caps, "live")
    kwargs = platform.new_process_group_kwargs()
    container.spawn_kwargs(kwargs, required=True)
    proc = await asyncio.create_subprocess_exec(
        _PYTHON, "-c", _SLEEPER, **kwargs
    )
    try:
        # SET_QUOTA | TERMINATE | QUERY_LIMITED_INFORMATION
        process = api.open_process(0x100 | 0x1 | 0x1000, proc.pid)
        try:
            api.assign(outer, process)  # the run's job nests under this one
            assert await asyncio.to_thread(
                container.attach, proc.pid, required=True
            ) is None
            assert api.is_process_in_job(process, outer)
            assert api.is_process_in_job(process, container._entry.handle)
        finally:
            api.close_handle(process)
        info = _jobobjects._structs().extended_limit()
        api.query_information(container._entry.handle, EXTENDED, info)
        assert info.BasicLimitInformation.LimitFlags == 0x200 | 0x8
        assert info.JobMemoryLimit == 256 * MiB
        assert info.BasicLimitInformation.ActiveProcessLimit == 4
        backend.close()  # the daemon's handles close, as when it exits
        await asyncio.sleep(0.5)
        assert platform.pid_alive(proc.pid)
    finally:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        await asyncio.wait_for(proc.wait(), 30)


#: A daemon stand-in.  It starts a capped run suspended, prints the child's
#: pid, waits for a line on stdin, runs ``{stop}``, attaches the run, and
#: dies with os._exit, closing its handles as a hard stop does.
_HARD_STOP = (
    "import os, subprocess, sys\n"
    "sys.path.insert(0, {root!r})\n"
    "from cronstable import _jobobjects, platform\n"
    "from cronstable.config import ResourceLimits\n"
    "backend = _jobobjects.Backend()\n"
    "backend.setup()\n"
    "caps = ResourceLimits(memory=256 << 20, processes=4)\n"
    "container = backend.create(caps, 'live')\n"
    "assert container.unenforced == {{}}, container.unenforced\n"
    "kwargs = platform.new_process_group_kwargs()\n"
    "container.spawn_kwargs(kwargs, required=True)\n"
    "child = subprocess.Popen(\n"
    "    [{python!r}, '-c', {sleeper!r}],\n"
    "    stdin=subprocess.DEVNULL,\n"
    "    stdout=subprocess.DEVNULL,\n"
    "    stderr=subprocess.DEVNULL,\n"
    "    **kwargs,\n"
    ")\n"
    "print(child.pid, flush=True)\n"
    "sys.stdin.readline()\n"
    "{stop}"
    "print(container.attach(child.pid, required=True), flush=True)\n"
    "os._exit(9)\n"
)

#: Dies after the assign, where the resume would start.
_DIE_BEFORE_RESUME = (
    "def die(pid):\n"
    "    print('assigned', flush=True)\n"
    "    os._exit(9)\n"
    "backend._api.resume_threads = die\n"
)


def _live_hard_stop(stop):
    """Run :data:`_HARD_STOP` with ``stop``: (output after the pid, pid).

    The test holds a handle to the child from before the stop, so the pid
    names the same process while the test checks it, and ends it last.
    """
    root = os.path.dirname(os.path.dirname(_jobobjects.__file__))
    code = _HARD_STOP.format(
        root=os.path.abspath(root), python=_PYTHON, sleeper=_SLEEPER, stop=stop
    )
    helper = subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    close_at_test_end(helper.kill)
    line = helper.stdout.readline()
    assert line.strip().isdigit(), helper.communicate(timeout=60)
    pid = int(line)
    api = _jobobjects.Kernel32()
    child = api.open_process(0x1000 | 0x1, pid)  # QUERY_LIMITED, TERMINATE

    def reap():
        with contextlib.suppress(OSError):
            api.terminate_process(child, 1)
        api.close_handle(child)

    close_at_test_end(reap)
    out, _ = helper.communicate("go\n", timeout=60)
    assert helper.returncode == 9, out
    return out.split(), pid


@_LIVE
def test_live_a_hard_stop_before_the_resume_ends_the_child():
    out, pid = _live_hard_stop(_DIE_BEFORE_RESUME)
    assert out == ["assigned"]
    # the dead daemon held the job's last handle: KILL_ON_JOB_CLOSE
    _eventually(lambda: not platform.pid_alive(pid))


@_LIVE
def test_live_a_run_that_started_outlives_a_hard_stop():
    out, pid = _live_hard_stop("")
    assert out == ["None"]  # attached and resumed, then the stop
    time.sleep(1)  # time enough for a kill-on-close to land
    assert platform.pid_alive(pid)
