"""Per-run CPU, memory and process-count caps (``resourceLimits``).

A capped run gets its own container: a cgroup v2 directory on Linux
(:mod:`cronstable._cgroups`) or a Job Object on Windows
(:mod:`cronstable._jobobjects`).  :func:`prepare` creates it before the
spawn, :class:`RunLimits` puts the child in it and, when the run ends,
reads what the kernel recorded.  Where the host cannot enforce a cap the
run goes ahead without it and its :class:`LimitsRecord` says so, unless the
job sets ``resourceLimits.required``, which refuses the run instead.

On Linux the cpu controller is on only while some job or DAG task caps
CPU, or a run with a cpu cap is still going: it makes every run's cgroup
a CPU group of its own, where nice orders only the threads inside.
Meanwhile (:func:`weighting_active`)
every run gets a cgroup weighted by its priority; an uncapped run's
cgroup holds only that weight, and the run has no limits record.

Backends are synchronous and thread-safe.  Per-run calls run on worker
threads; the one-time setup runs in whichever of
:meth:`LimitsManager.converge` and :func:`prepare` needs it first.  A
container whose ``attach`` raises for a required run has killed the child
first; the run is then refused like one whose ``attach`` returned a
refusal.
"""

import asyncio
import dataclasses
import logging
import math
import sys
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from cronstable import platform
from cronstable.config import ResourceLimits, format_size

logger = logging.getLogger("cronstable")

#: The cappable resources, in record order.  Each is also the name of its
#: :class:`~cronstable.config.ResourceLimits` field.
RESOURCES = ("cpu", "memory", "processes")

_MECHANISM_LABELS = {
    "cgroup-v2": "cgroup v2",
    "job-object": "Windows Job Objects",
}

# Distinct per-run failures remembered for the once-only warning; the set
# starts over when full.
_WARNED_MAX = 256

# A prepare repeats a setup that left a resource out at most this often
# (seconds); converge repeats it at every config apply.
_SETUP_RETRY_SECONDS = 30.0


def _error_text(exc: BaseException) -> str:
    """``exc`` as text, without the file name an OSError carries."""
    if isinstance(exc, OSError) and exc.strerror:
        return exc.strerror
    return str(exc) or type(exc).__name__


async def _outcome(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    """``func`` on a worker thread: its result, or the Exception it raised.

    A shielded task must not end in an exception: asyncio logs one at
    ERROR when the awaiting caller was cancelled first.
    """
    try:
        return await asyncio.to_thread(func, *args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - handed back to the caller
        return exc


def _opt_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _opt_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return float(value)


def _opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _configured(caps: ResourceLimits) -> tuple[str, ...]:
    """The resources ``caps`` sets, in :data:`RESOURCES` order."""
    return tuple(r for r in RESOURCES if getattr(caps, r) is not None)


def _incomplete(cap: "Capability") -> bool:
    """Whether ``cap`` leaves out any of :data:`RESOURCES`."""
    return not cap.available or not cap.resources.issuperset(RESOURCES)


def _words(items: list[str]) -> str:
    """``a``, ``a and b``, ``a, b, and c``."""
    if len(items) < 3:
        return " and ".join(items)
    return "{}, and {}".format(", ".join(items[:-1]), items[-1])


@dataclass(slots=True)
class LimitsRecord:
    """A run's caps and what the kernel did with them.

    ``enforced`` is true when every configured cap applied for the whole
    run; ``unenforced`` names the ones that did not and ``reason`` says
    why.  ``refused`` marks a required run whose caps could not apply:
    it never spawned, or its container ended the process.  The
    counters are the kernel's: ``peak_memory_bytes`` is the figure the
    memory cap is measured on, not the sampled RSS.
    """

    cpu: float | None
    memory_bytes: int | None
    processes: int | None
    required: bool
    enforced: bool
    mechanism: str | None
    unenforced: tuple[str, ...]
    reason: str | None
    refused: bool = False
    memory_exceeded: bool = False
    process_limit_hits: int | None = None
    peak_memory_bytes: int | None = None
    cpu_throttled_seconds: float | None = None

    @property
    def failure(self) -> str | None:
        """The run's failure reason from its caps, or ``None``."""
        if self.refused:
            detail = ": " + self.reason if self.reason else ""
            return (
                "resource limits could not be enforced{} "
                "(resourceLimits.required is set)".format(detail)
            )
        if self.memory_exceeded:
            if self.memory_bytes is None:
                return "memory limit exceeded"
            return "memory limit exceeded ({})".format(
                format_size(self.memory_bytes)
            )
        return None

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form for the run record and the API."""
        return {
            "cpu": _opt_float(self.cpu),
            "memory_bytes": self.memory_bytes,
            "processes": self.processes,
            "required": self.required,
            "enforced": self.enforced,
            "mechanism": self.mechanism,
            "unenforced": list(self.unenforced),
            "reason": self.reason,
            "refused": self.refused,
            "memory_exceeded": self.memory_exceeded,
            "process_limit_hits": self.process_limit_hits,
            "peak_memory_bytes": self.peak_memory_bytes,
            "cpu_throttled_seconds": _opt_float(self.cpu_throttled_seconds),
        }

    @classmethod
    def from_dict(cls, data: Any) -> "LimitsRecord | None":
        """Rebuild from :meth:`to_dict` output; ``None`` if malformed.

        A record without a boolean ``enforced`` is malformed.  Any other
        missing or wrong-typed field reads as unset.
        """
        if not isinstance(data, dict):
            return None
        enforced = data.get("enforced")
        if not isinstance(enforced, bool):
            return None
        raw = data.get("unenforced")
        if not isinstance(raw, list):
            raw = []
        names = {r for r in raw if isinstance(r, str)}
        return cls(
            cpu=_opt_float(data.get("cpu")),
            memory_bytes=_opt_int(data.get("memory_bytes")),
            processes=_opt_int(data.get("processes")),
            required=data.get("required") is True,
            enforced=enforced,
            mechanism=_opt_str(data.get("mechanism")),
            unenforced=tuple(r for r in RESOURCES if r in names),
            reason=_opt_str(data.get("reason")),
            refused=data.get("refused") is True,
            memory_exceeded=data.get("memory_exceeded") is True,
            process_limit_hits=_opt_int(data.get("process_limit_hits")),
            peak_memory_bytes=_opt_int(data.get("peak_memory_bytes")),
            cpu_throttled_seconds=_opt_float(
                data.get("cpu_throttled_seconds")
            ),
        )


@dataclass(frozen=True, slots=True)
class Capability:
    """What this host can cap, and why not or why only partly."""

    available: bool
    mechanism: str | None
    resources: frozenset[str]
    reason: str | None
    hint: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "mechanism": self.mechanism,
            "resources": [r for r in RESOURCES if r in self.resources],
            "reason": self.reason,
            "hint": self.hint,
        }


_UNSUPPORTED = Capability(
    available=False,
    mechanism=None,
    resources=frozenset(),
    reason="this platform has neither cgroup v2 nor Job Objects",
    hint=None,
)


class _UnavailableBackend:
    """The backend where no mechanism applies; it caps nothing."""

    mechanism = None

    def __init__(self, capability: Capability) -> None:
        self._capability = capability

    def capability(self) -> Capability:
        return self._capability

    def setup(self) -> Capability:
        return self._capability

    def collect(self) -> None:
        return None


def _select_backend(
    windows: bool | None = None, system: str | None = None
) -> Any:
    """The backend for this host: Job Objects, cgroup v2, or none."""
    if windows is None:
        windows = platform.IS_WINDOWS
    if system is None:
        system = sys.platform
    if windows:
        module = "cronstable._jobobjects"
    elif system.startswith("linux"):
        module = "cronstable._cgroups"
    else:
        return _UnavailableBackend(_UNSUPPORTED)
    try:
        # Static imports, so a frozen build bundles both backends; inside
        # the function, because each backend imports this module.
        if windows:
            from cronstable import _jobobjects

            return _jobobjects.Backend()
        from cronstable import _cgroups

        return _cgroups.Backend()
    except Exception as exc:  # noqa: BLE001 - caps degrade, never crash
        logger.warning("could not load %s", module, exc_info=True)
        return _UnavailableBackend(
            dataclasses.replace(
                _UNSUPPORTED,
                reason="could not load {}: {}".format(module, exc),
            )
        )


class RunLimits:
    """One run's container, if any, and the record of its caps.

    ``container`` is the backend's per-run object (or ``None`` when the run
    has none); a :class:`~cronstable.resources.ResourceMonitor` reads
    members and CPU totals from it.  ``refused`` is true when the job
    requires its caps and they cannot be enforced; such a run must not
    spawn, or, when set by :meth:`attach`, has already been terminated.
    ``recorded`` is false for a run without caps whose container holds
    only its CPU weight: its :attr:`record` names no caps and belongs in
    no run record.
    """

    def __init__(
        self,
        caps: ResourceLimits,
        *,
        manager: "LimitsManager",
        name: str,
        unenforced: dict[str, str],
        container: Any = None,
        mechanism: str | None = None,
        refused: bool = False,
        recorded: bool = True,
    ) -> None:
        self._caps = caps
        self._manager = manager
        self._name = name
        self._unenforced = dict(unenforced)
        self._container = container
        self._mechanism = mechanism
        self.refused = refused
        self.recorded = recorded
        self._stats: dict[str, Any] = {}
        self._final: LimitsRecord | None = None
        self._attach_task: asyncio.Future | None = None
        self._finish_task: asyncio.Future | None = None
        if refused:
            self._final = self._build()

    @property
    def container(self) -> Any:
        return self._container

    @property
    def record(self) -> LimitsRecord:
        """The current record; final once the run is refused or done."""
        return self._final if self._final is not None else self._build()

    def _build(self) -> LimitsRecord:
        caps = self._caps
        reasons = dict(self._unenforced)
        if self._container is not None:
            for resource, reason in self._container.unenforced.items():
                reasons.setdefault(resource, reason)
        configured = _configured(caps)
        names = tuple(r for r in configured if r in reasons)
        text = "; ".join(dict.fromkeys(reasons[r] for r in names))
        # null unless some configured cap is in force
        in_force = self._container is not None and len(names) < len(configured)
        stats = self._stats
        return LimitsRecord(
            cpu=caps.cpu,
            memory_bytes=caps.memory,
            processes=caps.processes,
            required=caps.required,
            enforced=not names,
            mechanism=self._mechanism if in_force else None,
            unenforced=names,
            reason=text or None,
            refused=self.refused,
            memory_exceeded=stats.get("memory_exceeded") is True,
            process_limit_hits=_opt_int(stats.get("process_limit_hits")),
            peak_memory_bytes=_opt_int(stats.get("peak_memory_bytes")),
            cpu_throttled_seconds=_opt_float(
                stats.get("cpu_throttled_seconds")
            ),
        )

    def spawn_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Prepare the spawn keyword arguments (loop thread, no I/O)."""
        if self._container is not None:
            self._container.spawn_kwargs(kwargs, required=self._caps.required)

    async def attach(self, pid: int) -> None:
        """Put the spawned child in the container.

        The worker call is shielded: cancelling the caller still lets it
        finish, so the child never stays half attached.
        """
        container = self._container
        if container is None or self._attach_task is not None:
            return
        task = asyncio.ensure_future(
            _outcome(container.attach, pid, required=self._caps.required)
        )
        self._attach_task = task
        task.add_done_callback(self._attached)
        await asyncio.shield(task)

    def _attached(self, task: asyncio.Future) -> None:
        if task.cancelled():
            return
        outcome = task.result()
        manager = self._manager
        manager.note_weight(
            self._name, getattr(self._container, "weight_problem", None)
        )
        if isinstance(outcome, Exception):
            reason = "could not attach the job to its container: {}".format(
                _error_text(outcome)
            )
            if self._caps.required:
                # the container killed the child before raising
                self.refused = True
            for resource in _configured(self._caps):
                self._unenforced.setdefault(resource, reason)
            if self.recorded:
                manager.note_failures(self._name, [reason])
            else:
                manager.note_weight(self._name, reason)
            return
        refusal = outcome
        failures = list(self._container.unenforced.values())
        if refusal is not None:
            self.refused = True
            for resource in _configured(self._caps):
                self._unenforced.setdefault(resource, refusal)
            failures.append(refusal)
        self._manager.note_failures(self._name, failures)

    async def _settled(self) -> None:
        """Wait for an attach still running on its worker thread."""
        task = self._attach_task
        if task is not None and not task.done():
            await asyncio.wait([task])

    async def kill(self) -> bool:
        """Force-kill everything in the container; True when it did."""
        container = self._container
        if container is None:
            return False
        await self._settled()
        try:
            return bool(await asyncio.to_thread(container.kill))
        except Exception:  # noqa: BLE001 - the caller falls back to killpg
            logger.debug("container kill failed", exc_info=True)
            return False

    async def finish(self) -> LimitsRecord:
        """Read the kernel's counters and release the container.

        Idempotent; concurrent callers share one finish.
        """
        if self._final is not None:
            return self._final
        if self._finish_task is None:
            self._finish_task = asyncio.ensure_future(self._finish())
        return await asyncio.shield(self._finish_task)

    async def _finish(self) -> LimitsRecord:
        await self._settled()
        container = self._container
        if container is not None:
            try:
                self._stats = await asyncio.to_thread(
                    self._finish_sync, container
                )
            except Exception as exc:  # noqa: BLE001 - recorded without stats
                key = "finish: " + _error_text(exc)
                if self._manager.should_warn(key):
                    # a run without caps loses nothing it records
                    recorded = self.recorded
                    logger.log(
                        logging.WARNING if recorded else logging.DEBUG,
                        "could not %s of a run of %s (each distinct failure "
                        "is logged once)",
                        "read the resource limits"
                        if recorded
                        else "release the container",
                        self._name,
                        exc_info=True,
                    )
        self._final = self._build()
        return self._final

    def _finish_sync(self, container: Any) -> dict[str, Any]:
        stats = container.finish()
        self._manager.collect()
        return dict(stats or {})

    def abort(self) -> LimitsRecord:
        """Release the container of a run whose spawn failed.

        A required run whose caps the failed spawn left unenforced (its
        child could not join) is refused, and the failure is logged as
        :meth:`LimitsManager.note_failures` logs it.
        """
        if self._final is not None:
            return self._final
        container = self._container
        if container is not None:
            try:
                container.abort()
            except Exception:  # noqa: BLE001 - spawn already failed
                logger.debug("container abort failed", exc_info=True)
        record = self._build()
        if self._caps.required and record.unenforced:
            self.refused = True
            record = self._build()
            if container is not None:
                self._manager.note_failures(
                    self._name, container.unenforced.values()
                )
        self._final = record
        return record


class LimitsManager:
    """The process-wide view of the backend: capability, setup, logging.

    Thread-safe.  Setup, cpu control and the converge report hold one
    lock; the event-loop calls (:meth:`capability`, :attr:`weighting`,
    :meth:`note_failures`) never wait for a setup running on another
    thread.
    """

    def __init__(self, backend: Any = None) -> None:
        self._backend = backend
        self._backend_lock = threading.Lock()
        self._lock = threading.RLock()
        self._probed: Capability | None = None
        self._setup: Capability | None = None
        # when the backend's setup last ran, by _clock
        self._tried = 0.0
        self._clock = time.monotonic
        # the capped jobs of the last converge, for a report after a retry
        self._capped: list[tuple[str, ResourceLimits]] | None = None
        self._logged: Any = None
        self._warned_lock = threading.Lock()
        self._warned: set[str] = set()

    @property
    def backend(self) -> Any:
        with self._backend_lock:
            if self._backend is None:
                self._backend = _select_backend()
            return self._backend

    @property
    def weighting(self) -> bool:
        """Whether the backend weighs CPU between runs; no I/O.

        Every run then gets a container, an uncapped run one that holds
        only its CPU weight.
        """
        return getattr(self._backend, "weighting", False) is True

    def capability(self) -> Capability:
        """The setup result, or a cached read-only probe before setup.

        A backend whose capability can drop while it runs
        (``live_capability``) is asked on every call instead.
        """
        backend = self.backend
        if getattr(backend, "live_capability", False):
            live: Capability = backend.capability()
            return live
        setup = self._setup
        if setup is not None:
            return setup
        if self._probed is None:
            # read-only and thread-safe: a racing probe only repeats it
            self._probed = backend.capability()
        return self._probed

    def setup(self, *, retry: bool = False) -> Capability:
        """Set the backend up once, and return :meth:`capability`.

        ``retry`` repeats a setup whose result lacks a resource.
        """
        with self._lock:
            done = self._setup
            if done is None or (retry and _incomplete(done)):
                self._setup = self.backend.setup()
                self._tried = self._clock()
            return self.capability()

    def _cpu(self, call: str, *args: bool, retry: bool) -> None:
        """Call the backend's cpu control ``call`` (under ``_lock``).

        ``set_cpu`` sets what the config wants, and ``need_cpu`` turns cpu
        on for one run.  Only a backend with cpu control (cgroup v2) has
        them.
        """
        cap = self._setup
        method = getattr(self.backend, call, None)
        if method is not None and cap is not None and cap.available:
            self._setup = method(*args, retry=retry)

    def _ready(self, caps: ResourceLimits) -> Capability:
        """The capability for one run's prepare (worker thread).

        Sets the backend up on first use and repeats a setup that left a
        resource out at most every :data:`_SETUP_RETRY_SECONDS`.  A run
        that caps CPU turns cpu control on, also after a converge that
        wanted it off (a verify check under the previous config keeps its
        caps); it goes off again once that run ends.
        """
        with self._lock:
            done = self._setup
            retry = (
                done is not None
                and _incomplete(done)
                and self._clock() - self._tried >= _SETUP_RETRY_SECONDS
            )
            self.setup(retry=retry)
            if caps.cpu is not None and not self.weighting:
                self._cpu("need_cpu", retry=retry)
            cap = self.capability()
            if retry and self._capped is not None:
                self._report(cap, self._capped)
            return cap

    def collect(self) -> None:
        """Retry the backend's pending removals."""
        self.backend.collect()

    def converge(self, capped: list[tuple[str, ResourceLimits]]) -> None:
        """Set the backend up on this thread and report whether caps apply.

        ``capped`` holds ``(display name, caps)`` for every job and DAG
        task that sets caps; the cpu controller is on while one of them
        caps CPU, and after that until the runs with a cpu cap end.  A
        setup that left a resource out runs again on each call.  The
        report is logged once per change of capability or capped jobs.
        Without capped jobs it only turns cpu control off, and before any
        setup it does nothing at all.  Never raises.
        """
        if not capped and self._setup is None:
            return
        want_cpu = any(caps.cpu is not None for _, caps in capped)
        try:
            with self._lock:
                self._capped = capped or None
                if not capped:
                    self._cpu("set_cpu", False, retry=True)
                    return
                self.setup(retry=True)
                self._cpu("set_cpu", want_cpu, retry=True)
                self._report(self.capability(), capped)
        except Exception:  # noqa: BLE001 - never stops a config apply
            logger.warning(
                "could not check whether resource limits can be enforced",
                exc_info=True,
            )

    def _report(
        self, cap: Capability, capped: list[tuple[str, ResourceLimits]]
    ) -> None:
        key = (cap, frozenset(capped))
        if key == self._logged:
            return
        self._logged = key
        affected = [
            (name, caps)
            for name, caps in capped
            if not cap.available or set(_configured(caps)) - cap.resources
        ]
        if not affected:
            mechanism = cap.mechanism or ""
            logger.info(
                "resource limits are enforced with %s",
                _MECHANISM_LABELS.get(mechanism, mechanism),
            )
            return
        logger.warning("%s", _warning(cap, affected))

    def should_warn(self, reason: str) -> bool:
        """True the first time ``reason`` comes up; remembers it."""
        with self._warned_lock:
            if reason in self._warned:
                return False
            if len(self._warned) >= _WARNED_MAX:
                self._warned.clear()
            self._warned.add(reason)
            return True

    def note_failures(self, name: str, reasons: Iterable[str]) -> None:
        """Log each unexpected per-run failure once per distinct reason."""
        for reason in reasons:
            if self.should_warn(reason):
                logger.warning(
                    "resource limits could not be applied to a run of %s: "
                    "%s (each distinct failure is logged once)",
                    name,
                    reason,
                )

    def note_weight(self, name: str, reason: str | None) -> None:
        """Log why a run's CPU weight does not apply, once per reason.

        At DEBUG: such a run goes ahead as an uncapped run does.
        """
        if reason is not None and self.should_warn("weight: " + reason):
            logger.debug(
                "the cpu weight of a run of %s does not apply: %s (each "
                "distinct failure is logged once)",
                name,
                reason,
            )

    async def prepare(
        self,
        caps: ResourceLimits | None,
        name: str,
        *,
        nice: int | None = None,
    ) -> RunLimits | None:
        """See :func:`prepare`.

        Cancelling the caller lets the worker finish; what it created is
        then released.
        """
        if caps is None and not self.weighting:
            return None
        task = asyncio.ensure_future(_outcome(self._prepare, caps, name, nice))
        try:
            outcome = await asyncio.shield(task)
        except asyncio.CancelledError:
            task.add_done_callback(_release_abandoned)
            raise
        if not isinstance(outcome, Exception):
            run: RunLimits | None = outcome
            return run
        reason = "unexpected error: {}".format(_error_text(outcome))
        if caps is None:
            self.note_weight(name, reason)
            return None
        if self.should_warn("prepare: " + reason):
            logger.warning(
                "could not prepare the resource limits of %s "
                "(each distinct failure is logged once)",
                name,
                exc_info=outcome,
            )
        return RunLimits(
            caps,
            manager=self,
            name=name,
            unenforced=dict.fromkeys(_configured(caps), reason),
            refused=caps.required,
        )

    def _prepare(
        self, caps: ResourceLimits | None, name: str, nice: int | None
    ) -> RunLimits | None:
        if caps is None:
            return self._prepare_weight(name, nice)
        cap = self._ready(caps)
        configured = _configured(caps)
        missing = [r for r in configured if r not in cap.resources]
        unenforced = dict.fromkeys(missing, cap.reason or "not supported")
        if missing and caps.required:
            return RunLimits(
                caps,
                manager=self,
                name=name,
                unenforced=unenforced,
                refused=True,
            )
        if len(missing) == len(configured) and not self.weighting:
            return RunLimits(
                caps, manager=self, name=name, unenforced=unenforced
            )
        applicable = ResourceLimits(
            cpu=None if "cpu" in missing else caps.cpu,
            memory=None if "memory" in missing else caps.memory,
            processes=None if "processes" in missing else caps.processes,
            required=caps.required,
        )
        container = self._create(applicable, name, nice)
        try:
            return self._contain(caps, name, unenforced, container)
        finally:
            # earlier runs' cgroups, and this one if it was just aborted
            self.backend.collect()

    def _contain(
        self,
        caps: ResourceLimits,
        name: str,
        unenforced: dict[str, str],
        container: Any,
    ) -> RunLimits:
        """The RunLimits of a capped run whose container was just made."""
        failures = dict(container.unenforced)
        self.note_failures(name, failures.values())
        weight = getattr(container, "weight_problem", None)
        self.note_weight(name, weight)
        unenforced.update(failures)
        if caps.required and failures:
            container.abort()
            return RunLimits(
                caps,
                manager=self,
                name=name,
                unenforced=unenforced,
                refused=True,
            )
        if weight is not None and not container.applied:
            # it was there for the weight alone
            container.abort()
            return RunLimits(
                caps, manager=self, name=name, unenforced=unenforced
            )
        return RunLimits(
            caps,
            manager=self,
            name=name,
            unenforced=unenforced,
            container=container,
            mechanism=self.backend.mechanism,
        )

    def _prepare_weight(self, name: str, nice: int | None) -> RunLimits | None:
        """A container that holds only the run's CPU weight, or ``None``."""
        container = self._create(ResourceLimits(), name, nice)
        try:
            problem = getattr(container, "weight_problem", None)
            if problem is not None:
                # the run spawns in the daemon's leaf, as when unweighted
                self.note_weight(name, problem)
                container.abort()
                return None
            return RunLimits(
                ResourceLimits(),
                manager=self,
                name=name,
                unenforced={},
                container=container,
                mechanism=self.backend.mechanism,
                recorded=False,
            )
        finally:
            self.backend.collect()

    def _create(
        self, caps: ResourceLimits, name: str, nice: int | None
    ) -> Any:
        """``backend.create``; only a backend that weighs runs gets ``nice``.

        One with cpu control gets it while cpu is off too, for when cpu
        turns on mid-run.
        """
        backend = self.backend
        if self.weighting or getattr(backend, "set_cpu", None) is not None:
            return backend.create(caps, name, nice=nice)
        return backend.create(caps, name)


def _release_abandoned(task: asyncio.Future) -> None:
    """Release the container of a prepare whose caller was cancelled."""
    if task.cancelled():
        return
    outcome = task.result()
    if isinstance(outcome, RunLimits):
        outcome.abort()


def _warning(
    cap: Capability, affected: list[tuple[str, ResourceLimits]]
) -> str:
    """The converge warning for the jobs whose caps cannot all apply."""
    names = ", ".join(dict.fromkeys(name for name, _ in affected))
    reason = cap.reason or "unknown reason"
    if cap.available:
        missing = [
            r
            for r in RESOURCES
            if r not in cap.resources
            and any(getattr(caps, r) is not None for _, caps in affected)
        ]
        plural = len(missing) > 1
        parts = [
            "resourceLimits is set ({}), but cronstable cannot enforce the "
            "{} cap{}: {}.".format(
                names, _words(missing), "s" if plural else "", reason
            )
        ]
        tail = "Those jobs run without {}.".format(
            "those caps" if plural else "that cap"
        )
    else:
        parts = [
            "resourceLimits is set ({}), but cronstable cannot enforce "
            "it: {}.".format(names, reason)
        ]
        tail = "Those jobs run without caps."
    if cap.hint:
        parts.append(cap.hint + ".")
    parts.append(tail)
    required = ", ".join(
        dict.fromkeys(name for name, caps in affected if caps.required)
    )
    if required:
        parts.append(
            "Runs of {} fail until the caps can be enforced "
            "(resourceLimits.required).".format(required)
        )
    return " ".join(parts)


_MANAGER: LimitsManager | None = None
_MANAGER_LOCK = threading.Lock()


def get_manager() -> LimitsManager:
    """The process-wide :class:`LimitsManager`, built on first use."""
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = LimitsManager()
        return _MANAGER


def set_manager(manager: LimitsManager | None) -> None:
    """Replace the process-wide manager (``None`` rebuilds it lazily)."""
    global _MANAGER
    with _MANAGER_LOCK:
        _MANAGER = manager


def weighting_active() -> bool:
    """Whether cronstable weighs CPU between runs (its cpu controller is on).

    Every run then needs :func:`prepare`, uncapped runs included.  Linux
    only.  It reads the process-wide manager, if one exists, without
    building one, and does no I/O.
    """
    return getattr(_MANAGER, "weighting", False) is True


async def prepare(
    caps: ResourceLimits | None, name: str, *, nice: int | None = None
) -> RunLimits | None:
    """Create the container for one run of ``name`` capped by ``caps``.

    Caps the host lacks are recorded as unenforced up front.  When none
    can apply, no container is made unless CPU weighting is on.  When
    ``caps.required`` is set and any cap cannot apply, the result is
    refused and nothing is created.  ``nice`` is the run's absolute nice,
    which its CPU weight follows; ``None`` stands for the daemon's own.
    For ``caps`` ``None`` the result is ``None`` unless weighting is on,
    and then a run whose ``recorded`` is false, or ``None`` when its
    cgroup could not be made.  With caps, it is never ``None``.
    """
    return await get_manager().prepare(caps, name, nice=nice)
