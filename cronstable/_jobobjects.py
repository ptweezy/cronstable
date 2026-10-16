"""Windows Job Objects for per-run resource caps.

The Windows backend of :mod:`cronstable.limits`.  Each capped run gets an
unnamed Job Object that carries its caps.  The child starts suspended, joins
the job, and then resumes, so every process it creates is in the job too.
Until the child resumes, the job kills its processes when its last handle
closes, so a daemon that dies before the resume takes the child with it.
One completion-port thread per process turns the jobs' messages into memory
kills and process-cap counts.  Windows does not guarantee those messages, so
the same thread also checks each memory-capped job's peak about once a
second and kills a job whose peak passed its cap.  Every kernel32 call goes
through :class:`Kernel32`, which tests replace with a fake.  Everything here
is synchronous; :mod:`cronstable.limits` calls it off the event loop.

A run with no console to inherit (under a service, for example) gets a
conhost.exe that Windows starts inside the run's job.  It is listed by
:meth:`Container.members` and its commit counts toward the memory cap, but
the process cap does not count it.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import os
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from cronstable import platform

if TYPE_CHECKING:
    from cronstable.config import ResourceLimits
    from cronstable.limits import Capability

logger = logging.getLogger("cronstable")

MECHANISM = "job-object"
RESOURCES = frozenset({"cpu", "memory", "processes"})

#: The exit code of a forced or memory kill, the one ``taskkill /F`` leaves.
KILL_EXIT_CODE = 1

# JOBOBJECTINFOCLASS values.
_BASIC_ACCOUNTING = 1
_BASIC_PROCESS_ID_LIST = 3
_ASSOCIATE_COMPLETION_PORT = 7
_EXTENDED_LIMIT = 9
_CPU_RATE_CONTROL = 15

# Limit flags.  BREAKAWAY_OK and SILENT_BREAKAWAY_OK are never set: a run
# must not escape its caps.  KILL_ON_JOB_CLOSE holds from the job's creation
# until its child runs, so a hard daemon kill ends a child it catches
# suspended in the job, and a run that has started outlives it.
_LIMIT_ACTIVE_PROCESS = 0x8
_LIMIT_JOB_MEMORY = 0x200
_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
_CPU_RATE_ENABLE = 0x1
_CPU_RATE_HARD_CAP = 0x4

#: ``CpuRate`` units in the whole machine.
_FULL_RATE = 10000

# JOB_OBJECT_MSG_* values.
_MSG_ACTIVE_PROCESS_LIMIT = 3
_MSG_ACTIVE_PROCESS_ZERO = 4
_MSG_JOB_MEMORY_LIMIT = 10

_PROCESS_TERMINATE = 0x1
_PROCESS_SET_QUOTA = 0x100
_THREAD_SUSPEND_RESUME = 0x2
_TH32CS_SNAPTHREAD = 0x4
_RESUME_FAILED = 0xFFFFFFFF

_ERROR_INVALID_HANDLE = 6
_ERROR_NOT_SUPPORTED = 50
_ERROR_MORE_DATA = 234
_WAIT_TIMEOUT = 258

#: The completion key that stops the watcher; run keys count up from 1.
_SHUTDOWN_KEY = 0

#: Seconds between the watcher's checks of the memory-capped jobs' peaks,
#: and its longest wait for a message.
_SWEEP_SECONDS = 1.0
_WAIT_MS = round(_SWEEP_SECONDS * 1000)

#: Process-list queries before :meth:`Container.members` gives up.
_PID_LIST_ATTEMPTS = 4

_WAIT_FAILED = "waiting on its completion port failed ({})"


class Win32Error(OSError):
    """A failed kernel32 call; ``code`` is the thread's last-error value."""

    def __init__(self, code: int, text: str = "") -> None:
        super().__init__(code, text)
        self.code = code
        self.message = text.strip().rstrip(".")

    def __str__(self) -> str:
        if not self.message:
            return "error {}".format(self.code)
        if not self.code:
            return self.message
        return "{} (error {})".format(self.message, self.code)


# --- Structures ----------------------------------------------------------
#: ``ctypes.sizeof`` of each structure with 8-byte and 4-byte pointers, from
#: the Windows SDK headers.
_LAYOUT = {
    "basic_limit": (64, 48),
    "extended_limit": (144, 112),
    "cpu_rate": (8, 8),
    "accounting": (48, 48),
    "completion_port": (16, 8),
    "thread_entry": (28, 28),
}

_STRUCTS: Any = None
_STRUCTS_LOCK = threading.Lock()


def _structs() -> Any:
    """The Win32 structures, built once and checked against :data:`_LAYOUT`.

    Plain ctypes types only, so they build and test on every OS.
    """
    global _STRUCTS
    with _STRUCTS_LOCK:
        if _STRUCTS is None:
            structs = _build_structs()
            _check_layout(structs, _LAYOUT)
            _STRUCTS = structs
        return _STRUCTS


def _build_structs() -> Any:
    import ctypes
    import types

    dword = ctypes.c_uint32
    large = ctypes.c_int64
    size_t = ctypes.c_size_t

    class BasicLimit(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", large),
            ("PerJobUserTimeLimit", large),
            ("LimitFlags", dword),
            ("MinimumWorkingSetSize", size_t),
            ("MaximumWorkingSetSize", size_t),
            ("ActiveProcessLimit", dword),
            ("Affinity", size_t),
            ("PriorityClass", dword),
            ("SchedulingClass", dword),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_uint64)
            for name in (
                "ReadOperationCount",
                "WriteOperationCount",
                "OtherOperationCount",
                "ReadTransferCount",
                "WriteTransferCount",
                "OtherTransferCount",
            )
        ]

    class ExtendedLimit(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimit),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", size_t),
            ("JobMemoryLimit", size_t),
            ("PeakProcessMemoryUsed", size_t),
            ("PeakJobMemoryUsed", size_t),
        ]

    class CpuRate(ctypes.Structure):
        _fields_ = [("ControlFlags", dword), ("CpuRate", dword)]

    class Accounting(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", large),
            ("TotalKernelTime", large),
            ("ThisPeriodTotalUserTime", large),
            ("ThisPeriodTotalKernelTime", large),
            ("TotalPageFaultCount", dword),
            ("TotalProcesses", dword),
            ("ActiveProcesses", dword),
            ("TotalTerminatedProcesses", dword),
        ]

    class CompletionPort(ctypes.Structure):
        _fields_ = [
            ("CompletionKey", size_t),
            ("CompletionPort", ctypes.c_void_p),
        ]

    class ThreadEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", dword),
            ("cntUsage", dword),
            ("th32ThreadID", dword),
            ("th32OwnerProcessID", dword),
            ("tpBasePri", ctypes.c_int32),
            ("tpDeltaPri", ctypes.c_int32),
            ("dwFlags", dword),
        ]

    return types.SimpleNamespace(
        basic_limit=BasicLimit,
        extended_limit=ExtendedLimit,
        cpu_rate=CpuRate,
        accounting=Accounting,
        completion_port=CompletionPort,
        thread_entry=ThreadEntry,
    )


def _check_layout(structs: Any, layout: dict[str, tuple[int, int]]) -> None:
    """Raise unless every structure has the size Windows gives it.

    A plain ``raise``, not an assert, since the release binary runs -OO.
    """
    import ctypes

    wide = ctypes.sizeof(ctypes.c_void_p) == 8
    for name, (wide_size, narrow_size) in layout.items():
        want = wide_size if wide else narrow_size
        got = ctypes.sizeof(getattr(structs, name))
        if got != want:
            raise RuntimeError(
                "the Job Object structure {} is {} bytes, but Windows "
                "expects {}".format(name, got, want)
            )


@functools.lru_cache(maxsize=8)
def _pid_list(slots: int) -> Any:
    """JOBOBJECT_BASIC_PROCESS_ID_LIST with room for ``slots`` pids."""
    import ctypes

    class ProcessIdList(ctypes.Structure):
        _fields_ = [
            ("NumberOfAssignedProcesses", ctypes.c_uint32),
            ("NumberOfProcessIdsInList", ctypes.c_uint32),
            ("ProcessIdList", ctypes.c_size_t * slots),
        ]

    return ProcessIdList


def cpu_rate(cpu: float, cpu_count: int) -> int:
    """``cpu`` CPUs as a ``CpuRate``: 1/10000ths of all ``cpu_count`` CPUs."""
    return min(_FULL_RATE, max(1, round(cpu / cpu_count * _FULL_RATE)))


# --- The kernel32 seam ---------------------------------------------------
class Kernel32:
    """Every kernel32 call the backend makes, with full prototypes.

    Its own ``ctypes.WinDLL`` with ``use_last_error``: the prototypes on
    ``ctypes.windll`` are shared, and other modules redeclare them.
    Handles are declared ``wintypes.HANDLE``, so none is truncated to 32
    bits.  Failures raise :class:`Win32Error`.
    """

    def __init__(self) -> None:
        self._lib: Any = None
        self._lock = threading.Lock()

    @property
    def size_t_max(self) -> int:
        """The largest value a ``SIZE_T`` field holds on this build."""
        import ctypes

        return (1 << (8 * ctypes.sizeof(ctypes.c_size_t))) - 1

    def load(self) -> Any:  # pragma: no cover (windows)
        """kernel32 with every prototype declared, loaded once."""
        import ctypes
        from ctypes import wintypes

        with self._lock:
            if self._lib is not None:
                return self._lib
            entry = ctypes.POINTER(_structs().thread_entry)
            handle, boolean = wintypes.HANDLE, wintypes.BOOL
            dword, void = wintypes.DWORD, ctypes.c_void_p
            prototypes: dict[str, tuple[Any, list[Any]]] = {
                "CreateJobObjectW": (handle, [void, wintypes.LPCWSTR]),
                "SetInformationJobObject": (
                    boolean,
                    [handle, ctypes.c_int, void, dword],
                ),
                "QueryInformationJobObject": (
                    boolean,
                    [handle, ctypes.c_int, void, dword, void],
                ),
                "AssignProcessToJobObject": (boolean, [handle, handle]),
                "TerminateJobObject": (boolean, [handle, wintypes.UINT]),
                "IsProcessInJob": (
                    boolean,
                    [handle, handle, ctypes.POINTER(boolean)],
                ),
                "OpenProcess": (handle, [dword, boolean, dword]),
                "TerminateProcess": (boolean, [handle, wintypes.UINT]),
                "CreateToolhelp32Snapshot": (handle, [dword, dword]),
                "Thread32First": (boolean, [handle, entry]),
                "Thread32Next": (boolean, [handle, entry]),
                "OpenThread": (handle, [dword, boolean, dword]),
                "ResumeThread": (dword, [handle]),
                "CreateIoCompletionPort": (
                    handle,
                    [handle, handle, ctypes.c_size_t, dword],
                ),
                "GetQueuedCompletionStatus": (
                    boolean,
                    [
                        handle,
                        ctypes.POINTER(dword),
                        ctypes.POINTER(ctypes.c_size_t),
                        ctypes.POINTER(void),
                        dword,
                    ],
                ),
                "PostQueuedCompletionStatus": (
                    boolean,
                    [handle, dword, ctypes.c_size_t, void],
                ),
                "CloseHandle": (boolean, [handle]),
            }
            lib = ctypes.WinDLL(  # type: ignore[attr-defined]
                "kernel32", use_last_error=True
            )
            for name, (restype, argtypes) in prototypes.items():
                function = getattr(lib, name)
                function.restype = restype
                function.argtypes = argtypes
            self._lib = lib
            return lib

    def _error(self) -> Win32Error:  # pragma: no cover (windows)
        import ctypes

        code = ctypes.get_last_error()  # type: ignore[attr-defined]
        return Win32Error(
            code,
            ctypes.FormatError(code),  # type: ignore[attr-defined]
        )

    def create_job(self) -> int:  # pragma: no cover (windows)
        job = self.load().CreateJobObjectW(None, None)
        if not job:
            raise self._error()
        return int(job)

    def close_handle(self, handle: int) -> None:  # pragma: no cover (windows)
        self.load().CloseHandle(handle)

    def set_information(  # pragma: no cover (windows)
        self, job: int, info_class: int, info: Any
    ) -> None:
        import ctypes

        if not self.load().SetInformationJobObject(
            job, info_class, ctypes.byref(info), ctypes.sizeof(info)
        ):
            raise self._error()

    def query_information(  # pragma: no cover (windows)
        self, job: int, info_class: int, info: Any
    ) -> None:
        import ctypes

        if not self.load().QueryInformationJobObject(
            job, info_class, ctypes.byref(info), ctypes.sizeof(info), None
        ):
            raise self._error()

    def assign(  # pragma: no cover (windows)
        self, job: int, process: int
    ) -> None:
        if not self.load().AssignProcessToJobObject(job, process):
            raise self._error()

    def terminate_job(  # pragma: no cover (windows)
        self, job: int, code: int
    ) -> None:
        if not self.load().TerminateJobObject(job, code):
            raise self._error()

    def is_process_in_job(  # pragma: no cover (windows)
        self, process: int, job: int | None
    ) -> bool:
        import ctypes
        from ctypes import wintypes

        result = wintypes.BOOL()
        if not self.load().IsProcessInJob(process, job, ctypes.byref(result)):
            raise self._error()
        return bool(result.value)

    def open_process(  # pragma: no cover (windows)
        self, access: int, pid: int
    ) -> int:
        process = self.load().OpenProcess(access, False, pid)
        if not process:
            raise self._error()
        return int(process)

    def terminate_process(  # pragma: no cover (windows)
        self, process: int, code: int
    ) -> None:
        if not self.load().TerminateProcess(process, code):
            raise self._error()

    def resume_threads(self, pid: int) -> int:  # pragma: no cover (windows)
        """Resume ``pid``'s threads; raise when no suspended one resumed.

        It skips a thread that fails and keeps walking.  A thread that was
        not suspended does not count.  A Toolhelp32 walk, because
        ``subprocess`` closes the primary thread handle and keeps no thread
        id.  It reads every thread on the system, about 30 ms.
        """
        import ctypes

        lib = self.load()
        snapshot = lib.CreateToolhelp32Snapshot(_TH32CS_SNAPTHREAD, 0)
        if not snapshot or snapshot == ctypes.c_void_p(-1).value:
            raise self._error()
        threads = resumed = 0
        failure: OSError | None = None
        try:
            entry = _structs().thread_entry()
            entry.dwSize = ctypes.sizeof(entry)
            more = lib.Thread32First(snapshot, ctypes.byref(entry))
            while more:
                if entry.th32OwnerProcessID == pid:
                    threads += 1
                    try:
                        if self._resume_thread(lib, entry.th32ThreadID) > 0:
                            resumed += 1
                    except OSError as ex:
                        failure = failure or ex
                entry.dwSize = ctypes.sizeof(entry)
                more = lib.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            lib.CloseHandle(snapshot)
        if resumed:
            return resumed
        if failure is not None:
            raise failure
        if threads:
            raise Win32Error(0, "the job's process has no suspended threads")
        raise Win32Error(0, "the job's process has no threads")

    def _resume_thread(  # pragma: no cover (windows)
        self, lib: Any, thread_id: int
    ) -> int:
        """Resume one thread; its suspend count before the call."""
        thread = lib.OpenThread(_THREAD_SUSPEND_RESUME, False, thread_id)
        if not thread:
            raise self._error()
        previous = int(lib.ResumeThread(thread))
        error = self._error() if previous == _RESUME_FAILED else None
        lib.CloseHandle(thread)
        if error is not None:
            raise error
        return previous

    def create_port(self) -> int:  # pragma: no cover (windows)
        from ctypes import wintypes

        port = self.load().CreateIoCompletionPort(
            wintypes.HANDLE(-1), None, 0, 1
        )
        if not port:
            raise self._error()
        return int(port)

    def wait_port(  # pragma: no cover (windows)
        self, port: int, timeout: int
    ) -> tuple[bool, int, int, int | None, int]:
        """Wait up to ``timeout`` ms for a packet.

        Returns (ok, message, key, overlapped, error); ``error`` is the
        last-error value when ``ok`` is False, else 0.
        """
        import ctypes
        from ctypes import wintypes

        message = wintypes.DWORD()
        key = ctypes.c_size_t()
        overlapped = ctypes.c_void_p()
        ok = bool(
            self.load().GetQueuedCompletionStatus(
                port,
                ctypes.byref(message),
                ctypes.byref(key),
                ctypes.byref(overlapped),
                timeout,
            )
        )
        error = 0
        if not ok:
            error = ctypes.get_last_error()  # type: ignore[attr-defined]
        return ok, int(message.value), int(key.value), overlapped.value, error

    def post_port(  # pragma: no cover (windows)
        self, port: int, key: int
    ) -> None:
        if not self.load().PostQueuedCompletionStatus(port, 0, key, None):
            raise self._error()


# --- The completion-port watcher -----------------------------------------
class _Entry:
    """One run's Job Object.

    ``lock`` guards ``handle``: whoever closes it holds the lock, so no
    call uses a handle value Windows has already handed to something else.
    ``memory`` is the job's memory cap, when one is in place.  ``lost``
    says why the watcher stopped while it watched the job.  ``guard`` is
    the extended limits as written with KILL_ON_JOB_CLOSE, until the run's
    attach clears that flag.
    """

    __slots__ = (
        "lock",
        "key",
        "handle",
        "guard",
        "memory",
        "watched",
        "lost",
        "assigned",
        "memory_exceeded",
        "process_limit_hits",
        "empty",
        "released",
    )

    def __init__(self, key: int, handle: int) -> None:
        self.lock = threading.Lock()
        self.key = key
        self.handle: int | None = handle
        self.guard: Any = None
        self.memory: int | None = None
        self.watched = False
        self.lost: str | None = None
        self.assigned = False
        self.memory_exceeded = False
        self.process_limit_hits = 0
        self.empty = False
        self.released = False


class _Watcher:
    """The completion port every Job Object reports to, and its thread.

    The port is its own: asyncio's Proactor closes completion keys it does
    not know as handles.  A ctypes ``GetQueuedCompletionStatus`` drains it,
    because the ``_overlapped`` version loses packets with a NULL
    overlapped pointer, which job messages carry.  ``sweep`` runs when a
    wait times out and after a message once ``interval`` seconds have passed
    since the last sweep, so a busy port cannot starve it.  ``stopped`` gets
    the reason when the thread ends for anything but :meth:`stop`.
    """

    def __init__(
        self,
        api: Any,
        dispatch: Callable[[int, int], None],
        sweep: Callable[[], None],
        stopped: Callable[[str], None],
        *,
        interval: float = _SWEEP_SECONDS,
    ) -> None:
        self._api = api
        self._dispatch = dispatch
        self._sweep = sweep
        self._stopped = stopped
        self._interval = interval
        self._stopping = False
        self.port: int = api.create_port()
        self._thread = threading.Thread(
            target=self._run, name="cronstable-job-objects", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        try:
            reason = self._drain()
        except Exception as ex:  # noqa: BLE001 - a broken port ends the watch
            reason = _WAIT_FAILED.format(str(ex) or type(ex).__name__)
        if reason is not None and not self._stopping:
            self._stopped(reason)

    def _drain(self) -> str | None:
        """Handle packets until the shutdown key; else why the port failed."""
        due = time.monotonic() + self._interval
        while True:
            ok, message, key, overlapped, error = self._api.wait_port(
                self.port, _WAIT_MS
            )
            if ok and key == _SHUTDOWN_KEY:
                return None
            # a failing message or sweep never ends the watch
            if ok:
                with contextlib.suppress(Exception):
                    self._dispatch(key, message)
            elif overlapped is not None:
                continue  # a failed I/O packet; jobs post none
            elif error != _WAIT_TIMEOUT:
                # the port closed (ERROR_ABANDONED_WAIT_0) or broke
                return _WAIT_FAILED.format("error {}".format(error))
            if not ok or time.monotonic() >= due:
                with contextlib.suppress(Exception):
                    self._sweep()
                due = time.monotonic() + self._interval

    def stop(self) -> None:
        """Post the shutdown key, join the thread, and close the port."""
        self._stopping = True
        with contextlib.suppress(OSError):
            self._api.post_port(self.port, _SHUTDOWN_KEY)
        self._thread.join(5.0)
        with contextlib.suppress(OSError):
            self._api.close_handle(self.port)


# --- The backend ---------------------------------------------------------
class Backend:
    """Per-run Job Objects, the :mod:`cronstable.limits` backend on Windows.

    Thread-safe.  ``api`` and ``cpu_count`` are test seams.
    """

    mechanism = MECHANISM
    #: :meth:`capability` drops memory once the watcher stops, so
    #: :class:`~cronstable.limits.LimitsManager` asks it on every call.
    live_capability = True

    def __init__(
        self,
        api: Any = None,
        *,
        cpu_count: Callable[[], int | None] | None = None,
    ) -> None:
        self._api: Any = Kernel32() if api is None else api
        self._cpu_count = os.cpu_count if cpu_count is None else cpu_count
        self._lock = threading.Lock()
        self._watcher: _Watcher | None = None
        # why memory-cap hits cannot be watched, once the watcher failed
        self._unwatched: str | None = None
        self._entries: dict[int, _Entry] = {}
        self._entries_lock = threading.Lock()
        self._next_key = _SHUTDOWN_KEY + 1
        # the guard-clear errors already logged
        self._stuck: set[str] = set()

    def capability(self) -> Capability:
        """What this host can cap, without side effects.

        All three resources wherever kernel32 loads; memory drops out when
        the watcher could not start or has stopped.
        """
        from cronstable.limits import Capability

        try:
            self._api.load()
        except Exception as ex:  # noqa: BLE001 - no kernel32, no caps
            return Capability(
                available=False,
                mechanism=MECHANISM,
                resources=frozenset(),
                reason="cronstable could not load the Job Object API "
                "({})".format(ex),
                hint=None,
            )
        unwatched = self._unwatched
        if unwatched is not None:
            return Capability(
                available=True,
                mechanism=MECHANISM,
                resources=RESOURCES - {"memory"},
                reason=unwatched,
                hint=None,
            )
        return Capability(
            available=True,
            mechanism=MECHANISM,
            resources=RESOURCES,
            reason=None,
            hint=None,
        )

    def setup(self) -> Capability:
        """Start the watcher (idempotent) and report the capability."""
        self._ensure_watcher()
        return self.capability()

    def create(
        self, caps: ResourceLimits, name: str, *, nice: int | None = None
    ) -> Container:
        """A new Job Object carrying ``caps``; a worker-thread call.

        A cap Windows refuses lands in the container's ``unenforced``;
        nothing raises.  ``nice`` is the cgroup backend's CPU weight;
        Windows orders runs by priority class and ignores it.
        """
        container = Container(self, name)
        try:
            handle = self._api.create_job()
        except Exception as ex:  # noqa: BLE001 - recorded, never raised
            reason = "Windows could not create a Job Object: {}".format(ex)
            for resource in _configured(caps):
                container.unenforced[resource] = reason
            return container
        entry = self._register(handle)
        applied, unenforced = self._apply(
            entry, handle, caps, self._watch(entry)
        )
        memory = caps.memory if "memory" in applied else None
        entry.memory = memory
        container._entry = entry
        container.applied = frozenset(applied)
        container.unenforced.update(unenforced)
        container.memory = memory
        return container

    def collect(self) -> None:
        """Close released jobs whose last process has exited."""
        with self._entries_lock:
            pending = [e for e in self._entries.values() if e.released]
        for entry in pending:
            self._release(entry)

    def close(self) -> None:
        """Stop the watcher and close every handle.

        The jobs and their caps outlive the handles while processes remain.
        """
        with self._lock:
            watcher, self._watcher = self._watcher, None
        if watcher is not None:
            watcher.stop()
        with self._entries_lock:
            entries = list(self._entries.values())
        for entry in entries:
            with entry.lock:
                if entry.handle is not None:
                    self._close_locked(entry)

    # -- creation ---------------------------------------------------------
    def _ensure_watcher(self) -> _Watcher | None:
        """The watcher, started on first use; None once it failed."""
        with self._lock:
            if self._watcher is None and self._unwatched is None:
                try:
                    self._watcher = _Watcher(
                        self._api,
                        self._dispatch,
                        self._sweep,
                        self._watcher_stopped,
                    )
                except Exception as ex:  # noqa: BLE001 - reported, not raised
                    self._unwatched = (
                        "Windows could not open the completion port that "
                        "reports memory-cap hits ({})".format(ex)
                    )
            return self._watcher if self._unwatched is None else None

    def _watcher_stopped(self, reason: str) -> None:
        """The watcher thread ended on its own: new runs get no memory cap.

        The runs it was watching lose their memory kill and their
        process-cap count; their finish records that.
        """
        unwatched = "the Job Object watcher stopped: {}".format(reason)
        lost = "the Job Object watcher stopped during the run: {}".format(
            reason
        )
        with self._lock:
            self._unwatched = unwatched
            with self._entries_lock:
                watched = [e for e in self._entries.values() if e.watched]
        for entry in watched:
            with entry.lock:
                entry.watched = False
                entry.lost = lost
        logger.error(
            "%s. Runs that start from now on get no memory cap until "
            "cronstable restarts.",
            unwatched,
        )

    def _guard_stuck(self, name: str, ex: OSError) -> None:
        """Log a guard Windows would not clear, once per distinct error."""
        reason = str(ex)
        with self._lock:
            if reason in self._stuck:
                return
            self._stuck.add(reason)
        logger.warning(
            "Windows could not clear the kill-on-close limit of the Job "
            "Object of a run of %s: %s. The run keeps its caps, and it ends "
            "if cronstable exits first (each distinct failure is logged "
            "once)",
            name,
            reason,
        )

    def _register(self, handle: int) -> _Entry:
        with self._entries_lock:
            entry = _Entry(self._next_key, handle)
            self._next_key += 1
            self._entries[entry.key] = entry
        return entry

    def _watch(self, entry: _Entry) -> str | None:
        """Report ``entry``'s job to the watcher; why not, when it cannot."""
        watcher = self._ensure_watcher()
        if watcher is None:
            return self._unwatched
        info = _structs().completion_port()
        info.CompletionKey = entry.key
        info.CompletionPort = watcher.port
        try:
            self._api.set_information(
                entry.handle, _ASSOCIATE_COMPLETION_PORT, info
            )
        except OSError as ex:
            return (
                "Windows could not watch the Job Object for memory-cap "
                "hits: {}".format(ex)
            )
        with self._lock:
            if self._unwatched is not None:
                return self._unwatched  # it stopped during the association
            entry.watched = True
        return None

    def _apply(
        self,
        entry: _Entry,
        handle: int,
        caps: ResourceLimits,
        watch_error: str | None,
    ) -> tuple[set[str], dict[str, str]]:
        """Set ``caps`` and the guard on the job: (applied, unenforced)."""
        applied: set[str] = set()
        unenforced: dict[str, str] = {}
        memory = caps.memory
        if memory is not None:
            problem = watch_error or self._memory_problem(memory)
            if problem is not None:
                unenforced["memory"] = problem
                memory = None
        processes = caps.processes
        refusal = self._guard(entry, handle, memory, processes)
        for resource, cap in (("memory", memory), ("processes", processes)):
            if cap is None:
                continue
            if refusal is None:
                applied.add(resource)
            else:
                unenforced[resource] = "Windows refused the {}: {}".format(
                    _CAP_NAMES[resource], refusal
                )
        if caps.cpu is not None:
            problem = self._set_cpu(handle, caps.cpu)
            if problem is None:
                applied.add("cpu")
            else:
                unenforced["cpu"] = problem
        return applied, unenforced

    def _memory_problem(self, memory: int) -> str | None:
        limit = int(self._api.size_t_max)
        if memory <= limit:
            return None
        from cronstable.config import format_size

        return (
            "{} is above the largest memory cap Windows can set on this "
            "build ({} bytes)".format(format_size(memory), limit)
        )

    def _guard(
        self,
        entry: _Entry,
        handle: int,
        memory: int | None,
        processes: int | None,
    ) -> OSError | None:
        """Write the extended caps with KILL_ON_JOB_CLOSE; Windows' refusal.

        When Windows refuses the caps, the flag still goes on alone.
        """
        try:
            entry.guard = self._set_extended(handle, memory, processes)
        except OSError as ex:
            if memory is not None or processes is not None:
                with contextlib.suppress(OSError):
                    entry.guard = self._set_extended(handle, None, None)
            return ex
        return None

    def _set_extended(
        self, handle: int, memory: int | None, processes: int | None
    ) -> Any:
        """Write the extended limits with KILL_ON_JOB_CLOSE; what it wrote."""
        info = _structs().extended_limit()
        flags = _LIMIT_KILL_ON_JOB_CLOSE
        if memory is not None:
            flags |= _LIMIT_JOB_MEMORY
            info.JobMemoryLimit = memory  # commit charge, job-wide
        if processes is not None:
            flags |= _LIMIT_ACTIVE_PROCESS
            info.BasicLimitInformation.ActiveProcessLimit = min(
                processes, 0xFFFFFFFF
            )
        info.BasicLimitInformation.LimitFlags = flags
        self._api.set_information(handle, _EXTENDED_LIMIT, info)
        return info

    def _set_cpu(self, handle: int, cpu: float) -> str | None:
        """Hard-cap the job's CPU rate; why not, when Windows refuses."""
        count = self._cpu_count()
        if not count:
            return "cronstable could not count this machine's CPUs"
        rate = cpu_rate(cpu, count)
        if rate >= _FULL_RATE:
            return None  # the whole machine or more: nothing to limit
        info = _structs().cpu_rate()
        info.ControlFlags = _CPU_RATE_ENABLE | _CPU_RATE_HARD_CAP
        info.CpuRate = rate
        try:
            self._api.set_information(handle, _CPU_RATE_CONTROL, info)
        except OSError as ex:
            reason = "Windows refused the CPU rate cap: {}".format(ex)
            if getattr(ex, "code", None) == _ERROR_NOT_SUPPORTED:
                reason += (
                    "; Dynamic Fair Share Scheduling, which Remote Desktop "
                    "Services turns on, blocks CPU rate caps"
                )
            return reason
        return None

    # -- messages and release ---------------------------------------------
    def _dispatch(self, key: int, message: int) -> None:
        with self._entries_lock:
            entry = self._entries.get(key)
        if entry is not None:
            self._on_message(entry, message)

    def _on_message(self, entry: _Entry, message: int) -> None:
        with entry.lock:
            if message == _MSG_JOB_MEMORY_LIMIT:
                self._exceeded_locked(entry)
            elif message == _MSG_ACTIVE_PROCESS_LIMIT:
                entry.process_limit_hits += 1
            elif message == _MSG_ACTIVE_PROCESS_ZERO:
                entry.empty = True
                if entry.released and entry.handle is not None:
                    self._close_locked(entry)

    def _sweep(self) -> None:
        """Kill each memory-capped job whose peak passed its cap.

        The backstop for a JOB_MEMORY_LIMIT message Windows did not deliver.
        A refused commit counts in the peak, so above the cap is a hit.
        """
        with self._entries_lock:
            capped = [
                e for e in self._entries.values() if e.memory is not None
            ]
        for entry in capped:
            with entry.lock:
                memory, handle = entry.memory, entry.handle
                if (
                    memory is None
                    or handle is None
                    or not entry.assigned
                    or entry.memory_exceeded
                ):
                    continue
                peak = self._peak(handle)
                if peak is not None and peak > memory:
                    self._exceeded_locked(entry)

    def _exceeded_locked(self, entry: _Entry) -> None:
        """Record a memory hit and kill the job; ``entry.lock`` is held."""
        entry.memory_exceeded = True
        if entry.handle is not None:
            with contextlib.suppress(OSError):
                self._api.terminate_job(entry.handle, KILL_EXIT_CODE)

    def _release(self, entry: _Entry) -> None:
        """Close ``entry``'s handle once its job is empty.

        A job that still has processes keeps its handle, so a memory hit
        can still kill it; ACTIVE_PROCESS_ZERO or :meth:`collect` closes it.
        """
        with entry.lock:
            entry.released = True
            if entry.handle is None:
                return
            if not entry.empty:
                entry.empty = self._is_empty(entry.handle)
            if entry.empty:
                self._close_locked(entry)

    def _is_empty(self, handle: int) -> bool:
        info = _structs().accounting()
        try:
            self._api.query_information(handle, _BASIC_ACCOUNTING, info)
        except OSError:
            return False
        return int(info.ActiveProcesses) == 0

    def _close_locked(self, entry: _Entry) -> None:
        """Close the handle and forget the key; ``entry.lock`` is held."""
        handle, entry.handle = entry.handle, None
        with contextlib.suppress(OSError):
            self._api.close_handle(handle)
        with self._entries_lock:
            self._entries.pop(entry.key, None)

    # -- queries ----------------------------------------------------------
    def _peak(self, handle: int) -> int | None:
        info = _structs().extended_limit()
        try:
            self._api.query_information(handle, _EXTENDED_LIMIT, info)
        except OSError:
            return None
        return int(info.PeakJobMemoryUsed)

    def _members(self, handle: int) -> list[int] | None:
        slots = 16
        for _attempt in range(_PID_LIST_ATTEMPTS):
            info = _pid_list(slots)()
            try:
                self._api.query_information(
                    handle, _BASIC_PROCESS_ID_LIST, info
                )
            except OSError as ex:
                if getattr(ex, "code", None) != _ERROR_MORE_DATA:
                    return None
                # the job grew past the buffer; leave room for more
                slots = max(
                    slots * 2, int(info.NumberOfAssignedProcesses) + 16
                )
                continue
            count = int(info.NumberOfProcessIdsInList)
            return [int(pid) for pid in info.ProcessIdList[:count]]
        return None

    def _cpu_times(self, handle: int) -> tuple[float, float] | None:
        info = _structs().accounting()
        try:
            self._api.query_information(handle, _BASIC_ACCOUNTING, info)
        except OSError:
            return None
        # 100 ns ticks, totals of every process the job ever held
        return info.TotalUserTime / 1e7, info.TotalKernelTime / 1e7


_CAP_NAMES = {"memory": "memory cap", "processes": "process cap"}


def _configured(caps: ResourceLimits) -> list[str]:
    return [
        resource
        for resource, cap in (
            ("cpu", caps.cpu),
            ("memory", caps.memory),
            ("processes", caps.processes),
        )
        if cap is not None
    ]


def _ended(terminate: Callable[[int, int], None], handle: int) -> bool:
    """Whether ``terminate(handle, KILL_EXIT_CODE)`` succeeded."""
    try:
        terminate(handle, KILL_EXIT_CODE)
    except OSError:
        return False
    return True


def _no_stats() -> dict[str, Any]:
    return {
        "memory_exceeded": False,
        "process_limit_hits": None,
        "peak_memory_bytes": None,
        "cpu_throttled_seconds": None,
    }


# --- One run -------------------------------------------------------------
class Container:
    """One capped run's Job Object.

    While the daemon runs, the child is never left suspended: :meth:`attach`
    resumes or terminates it on every path, an attach that starts after
    :meth:`finish` or :meth:`abort` terminates it, and either of those
    terminates a child whose attach is still running.

    The job holds KILL_ON_JOB_CLOSE until :meth:`attach` has resumed the
    child, so a daemon that dies outright between the assign and the
    resume closes the job's last handle and the child dies with it.  A
    death between the spawn and the assign, which includes the wait for a
    worker thread, leaves the child suspended outside any job.  A run
    whose guard Windows will not clear keeps it and its caps, and ends
    when the daemon exits.
    """

    def __init__(self, backend: Backend, name: str) -> None:
        self.applied: frozenset[str] = frozenset()
        self.unenforced: dict[str, str] = {}
        self.memory: int | None = None
        self._entry: _Entry | None = None
        self._backend = backend
        self._name = name
        self._api = backend._api
        self._lock = threading.Lock()
        self._suspended = False
        self._pid: int | None = None
        self._attaching = False
        self._closed = False
        self._result: dict[str, Any] | None = None

    def spawn_kwargs(self, kwargs: dict[str, Any], *, required: bool) -> None:
        """Have the child created suspended, so it joins the job first."""
        if self._entry is not None:
            platform.suspend_at_spawn(kwargs)
            self._suspended = True

    def attach(self, pid: int, *, required: bool) -> str | None:
        """Put ``pid`` in the job and resume it.

        Returns why when ``required`` and the child could not be contained;
        the child is terminated then.  Otherwise returns None, with any lost
        containment recorded in ``unenforced``.
        """
        with self._lock:
            entry = self._entry
            if entry is None or self._pid is not None:
                return None
            self._pid = pid
            closed = self._closed
            self._attaching = not closed
        if closed:
            self._end_child(entry, pid, None)  # the run ended before it joined
            return None
        try:
            return self._attach(entry, pid, required)
        except BaseException:
            self._end_child(entry, pid, None)
            raise
        finally:
            with self._lock:
                self._attaching = False

    def abort(self) -> None:
        """The spawn failed or the run ended early: release the job."""
        self._end(read=False)

    def kill(self) -> bool:
        """Terminate every process in the job; False when none is in it."""
        entry = self._entry
        if entry is None:
            return False
        with entry.lock:
            if not entry.assigned or entry.handle is None:
                return False
            try:
                self._api.terminate_job(entry.handle, KILL_EXIT_CODE)
            except OSError:
                return False
        return True

    def finish(self) -> dict[str, Any]:
        """Read the run's figures and release the job.  Idempotent."""
        return self._end(read=True)

    def members(self) -> list[int] | None:
        """The pids in the job, or None when it cannot tell."""
        entry = self._entry
        if entry is None:
            return None
        with entry.lock:
            if not entry.assigned or entry.handle is None:
                return None
            return self._backend._members(entry.handle)

    def cpu_times(self) -> tuple[float, float] | None:
        """Job-wide (user, system) CPU seconds, exited processes included."""
        entry = self._entry
        if entry is None:
            return None
        with entry.lock:
            if not entry.assigned or entry.handle is None:
                return None
            return self._backend._cpu_times(entry.handle)

    # -- attach -----------------------------------------------------------
    def _attach(self, entry: _Entry, pid: int, required: bool) -> str | None:
        try:
            process = self._api.open_process(
                _PROCESS_SET_QUOTA | _PROCESS_TERMINATE, pid
            )
        except OSError as ex:
            return self._uncontained(
                entry,
                pid,
                None,
                required,
                "cronstable could not open the job's process: {}".format(ex),
            )
        try:
            return self._assign_and_resume(entry, pid, process, required)
        finally:
            with contextlib.suppress(OSError):
                self._api.close_handle(process)

    def _assign_and_resume(
        self, entry: _Entry, pid: int, process: int, required: bool
    ) -> str | None:
        try:
            with entry.lock:
                if entry.handle is None:
                    raise Win32Error(
                        _ERROR_INVALID_HANDLE, "the run's Job Object is closed"
                    )
                self._api.assign(entry.handle, process)
                entry.assigned = True
        except OSError as ex:
            return self._uncontained(
                entry,
                pid,
                process,
                required,
                "Windows could not put the job's process in its Job Object: "
                "{}".format(ex),
            )
        problem = self._start(entry, pid)
        if problem is None:
            return None
        self._end_child(entry, pid, process)
        return self._lose(required, problem)

    def _start(self, entry: _Entry, pid: int) -> str | None:
        """Resume the child and clear the guard; why the resume failed."""
        if self._suspended:
            try:
                self._api.resume_threads(pid)
            except OSError as ex:
                reason = "cronstable could not resume the job's process: {}"
                return reason.format(ex)
        try:
            self._unguard(entry)
        except OSError as ex:
            # the caps still hold; the run just ends when the daemon exits
            self._backend._guard_stuck(self._name, ex)
        return None

    def _unguard(self, entry: _Entry) -> None:
        """Clear KILL_ON_JOB_CLOSE and keep every other limit as written."""
        with entry.lock:
            guard = entry.guard
            if guard is None or entry.handle is None:
                return  # refused at creation, or gone with the closed job
            info = type(guard).from_buffer_copy(guard)
            info.BasicLimitInformation.LimitFlags &= ~_LIMIT_KILL_ON_JOB_CLOSE
            self._api.set_information(entry.handle, _EXTENDED_LIMIT, info)
            entry.guard = None

    def _uncontained(
        self,
        entry: _Entry,
        pid: int,
        process: int | None,
        required: bool,
        reason: str,
    ) -> str | None:
        """The child is outside the job: end it, or run it uncapped."""
        if required:
            self._end_child(entry, pid, process)
        elif self._suspended:
            try:
                self._api.resume_threads(pid)
            except OSError as ex:
                self._end_child(entry, pid, process)
                reason = (
                    "{}; cronstable could not resume it either: {}".format(
                        reason, ex
                    )
                )
        return self._lose(required, reason)

    def _lose(self, required: bool, reason: str) -> str | None:
        for resource in self.applied:
            self.unenforced[resource] = reason
        self.applied = frozenset()
        return reason if required else None

    def _end_child(self, entry: _Entry, pid: int, process: int | None) -> None:
        """Terminate the child, or resume it where nothing can; never raises.

        Either way the child is not left suspended.
        """
        if not self._terminate(entry, pid, process):
            with contextlib.suppress(OSError):
                self._api.resume_threads(pid)

    def _terminate(self, entry: _Entry, pid: int, process: int | None) -> bool:
        """Terminate through the job, ``process``, or a fresh handle."""
        with entry.lock:
            if entry.assigned and entry.handle is not None:
                if _ended(self._api.terminate_job, entry.handle):
                    return True
        if process is not None and _ended(
            self._api.terminate_process, process
        ):
            return True
        try:
            handle = self._api.open_process(_PROCESS_TERMINATE, pid)
        except OSError:
            return False
        try:
            return _ended(self._api.terminate_process, handle)
        finally:
            with contextlib.suppress(OSError):
                self._api.close_handle(handle)

    # -- finish -----------------------------------------------------------
    def _end(self, *, read: bool) -> dict[str, Any]:
        entry = self._entry
        with self._lock:
            self._closed = True
            if self._result is None:
                self._result = self._stats() if read else _no_stats()
            result = dict(self._result)
            unattached = self._pid if self._attaching else None
        if unattached is not None and entry is not None:
            self._end_child(entry, unattached, None)  # attach still running
        if entry is not None:
            self._backend._release(entry)
        return result

    def _stats(self) -> dict[str, Any]:
        stats = _no_stats()
        entry = self._entry
        if entry is None or not entry.assigned:
            return stats
        with entry.lock:
            handle = entry.handle
            peak = None if handle is None else self._backend._peak(handle)
            exceeded = entry.memory_exceeded
            # a count without the watcher would miss hits
            hits = entry.process_limit_hits if entry.watched else None
            lost = entry.lost
        memory = self.memory if "memory" in self.applied else None
        if memory is not None and lost is not None:
            # Windows still refuses commits past the cap, but nothing kills
            # the run there
            self.unenforced.setdefault("memory", lost)
        if memory is not None and peak is not None:
            # a refused commit counts in the peak: above the cap is a hit
            exceeded = exceeded or peak > memory
            peak = min(peak, memory)
        elif peak is not None and peak >= self._api.size_t_max:
            # a 32-bit build reads a larger peak as SIZE_T's largest value
            peak = None
        stats["memory_exceeded"] = exceeded
        stats["peak_memory_bytes"] = peak
        if "processes" in self.applied:
            stats["process_limit_hits"] = hits
        return stats
