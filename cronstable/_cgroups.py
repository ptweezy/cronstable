"""cgroup v2 backend for per-run resource limits (Linux).

The layout under the daemon's delegated cgroup ``base``::

    base/                +memory +pids, and +cpu while a job or a run caps CPU
      daemon/            the daemon and every process that was in base
      runs/              the same controllers as base
        <hex>-<slug>/    one per capped run, and one per run while +cpu is on

The cpu controller makes each cgroup below it a CPU group of its own, and
nice then orders only the threads inside one group.  So it is on only
while some job caps CPU or a run with a cpu cap is still going
(:meth:`Backend.set_cpu`), and meanwhile every run gets a cgroup whose
``cpu.weight.nice`` carries the run's nice: an uncapped run gets one that
holds only that weight.

A child joins its run's cgroup between fork and exec
(:class:`_ChildJoin`), before any uid change, so the job never runs
outside it.  All file access goes through :class:`CgroupFS`; tests supply
a model of the kernel instead.  Every method is synchronous and
thread-safe: :mod:`cronstable.limits` calls the blocking ones on worker
threads.  ``rmdir`` can block, so only setup, :meth:`Container.finish`
and :meth:`Backend.collect` call it; abort queues its cgroup for collect.
"""

import errno
import logging
import os
import re
import signal
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from cronstable.config import ResourceLimits
from cronstable.limits import Capability
from cronstable.resources import (
    CGROUP_ROOT,
    DAEMON_LEAF,
    RUNS_DIR,
    _CgroupV2Reader,
)

logger = logging.getLogger("cronstable")

MECHANISM = "cgroup-v2"

# resource -> the cgroup v2 controller that caps it
_CONTROLLERS = {"cpu": "cpu", "memory": "memory", "processes": "pids"}

# Rounds of "move every process out, then enable the controllers".  A
# process that arrives in between, or an exiting one that cannot move,
# makes the enable fail with EBUSY.  The pause before each new round
# starts at 5 ms and doubles up to 0.5 s, about 2 s in all.
_SETUP_ROUNDS = 11
_SETUP_PAUSE = 0.005
_SETUP_PAUSE_MAX = 0.5

# the range cpu.weight.nice accepts
_NICE_MIN = -20
_NICE_MAX = 19

# How long an empty run cgroup that this daemon did not make still holds
# cpu on: another daemon's run is empty until its child joins.
_FOREIGN_GRACE = 60.0

_PERIOD_USEC = 100_000
_MIN_QUOTA_USEC = 1000  # the kernel's floor (EINVAL below)
_MAX_QUOTA_USEC = 2**44 - 1  # MAX_BW; kernels before 5.8 do not check it

_SLUG_MAX = 48
_SLUG_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")

_NOT_MOVED = "could not move the job into its cgroup"
_WEIGHT_OFF = "the cpu controller is off"

# the value that lifts each file a rollback undoes (memory.oom.group is
# always written last, so a rollback never reaches it)
_UNSET = {"memory.max": "max", "memory.swap.max": "max"}

_SIGKILL = getattr(signal, "SIGKILL", 9)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_OOM_SCORE_ADJ_MIN = -1000

_DELEGATE_HINT = (
    "Run cronstable in a delegated cgroup: Delegate= on its systemd unit "
    "(the packaged unit sets Delegate=memory pids), or a container with a "
    "writable cgroup namespace"
)
_V2_HINT = (
    "Boot the host with the unified cgroup v2 hierarchy "
    "(systemd.unified_cgroup_hierarchy=1)"
)
_CONTROLLER_HINT = (
    "Delegate the memory and pids controllers to cronstable's cgroup"
)
_CPU_HINT = (
    "Delegate the cpu controller to cronstable's cgroup, for example with "
    "Delegate=yes on its systemd unit"
)
_CPU_NOT_DELEGATED = "the cpu controller is not delegated to cronstable"


def _describe(exc: OSError) -> str:
    return exc.strerror or str(exc)


def _in_thread(func: Callable[[], Any]) -> None:
    """Run ``func`` on a daemon thread of its own."""
    threading.Thread(
        target=func, name="cronstable-cgroups", daemon=True
    ).start()


def _cpu_hint(base: str) -> str:
    """How to delegate the cpu controller to the cgroup ``base``."""
    parts = base.split(os.sep)
    unit = parts[-1]
    if not unit.endswith(".service"):
        return _CPU_HINT
    name = unit[: -len(".service")]
    user = any(
        p.startswith("user@") and p.endswith(".service") for p in parts[:-1]
    )
    return (
        "To cap CPU, run systemctl {}edit {} and add Delegate=yes under "
        "[Service]; that makes {} a CPU group of its own among the {} "
        "services".format(
            "--user " if user else "",
            name,
            name,
            "user's" if user else "system's",
        )
    )


class _Stuck(OSError):
    """Processes that stayed in a cgroup though every round moved them."""

    def __init__(self, pids: list[int]) -> None:
        many = len(pids) > 1
        super().__init__(
            errno.EBUSY,
            "process{} {} in it could not be moved ({} may be exiting)".format(
                "es" if many else "",
                ", ".join(str(p) for p in pids),
                "they" if many else "it",
            ),
        )


def _unavailable(reason: str, hint: str | None = _DELEGATE_HINT) -> Capability:
    return Capability(
        available=False,
        mechanism=MECHANISM,
        resources=frozenset(),
        reason=reason,
        hint=hint,
    )


def _slug(name: str) -> str:
    return _SLUG_UNSAFE.sub("_", name)[:_SLUG_MAX]


def _no_stats() -> dict[str, Any]:
    return {
        "memory_exceeded": False,
        "process_limit_hits": None,
        "peak_memory_bytes": None,
        "cpu_throttled_seconds": None,
    }


def _above_stdio(fd: int, floor: int = 3) -> int:
    """``fd``, duplicated up to ``floor`` or above, the original closed.

    The child dup2s its stdio over 0-2 before ``preexec_fn`` runs, so a
    daemon started with a closed stdio fd must not hand the join one.  On
    failure every descriptor involved is closed, the original included.
    """
    low = []
    try:
        while fd < floor:
            low.append(fd)
            fd = os.dup(fd)
    finally:
        for held in low:
            os.close(held)
    return fd


def _keyed(text: str) -> dict[str, int]:
    """A flat keyed file (``key value`` lines) as a dict."""
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return out


class CgroupFS:
    """Kernel file access for the backend; tests substitute a model.

    ``root`` is the cgroup2 mount and ``proc`` the procfs mount.  Calls
    raise :class:`OSError` with the kernel's errno.
    """

    def __init__(self, root: str = CGROUP_ROOT, proc: str = "/proc") -> None:
        self.root = os.path.normpath(root)
        self.proc = proc

    def read(self, path: str) -> str:
        with open(path, encoding="ascii", errors="replace") as fh:
            return fh.read()

    def write(self, path: str, data: str) -> None:
        # no O_CREAT: a missing interface file is ENOENT, not a new file
        fd = os.open(path, os.O_WRONLY | _O_CLOEXEC)
        try:
            os.write(fd, data.encode("ascii"))
        finally:
            os.close(fd)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def isdir(self, path: str) -> bool:
        return os.path.isdir(path)

    def writable(self, path: str) -> bool:
        # a read-only mount reports EROFS here, so False
        return os.access(path, os.W_OK)

    def subdirs(self, path: str) -> list[str]:
        with os.scandir(path) as entries:
            return [e.name for e in entries if e.is_dir(follow_symlinks=False)]

    def mkdir(self, path: str) -> None:
        os.mkdir(path)

    def rmdir(self, path: str) -> None:
        os.rmdir(path)

    def getxattr(self, path: str, name: str) -> bytes:
        getxattr = getattr(os, "getxattr", None)
        if getxattr is None:
            raise OSError(errno.ENOTSUP, "extended attributes unsupported")
        return bytes(getxattr(path, name))

    def geteuid(self) -> int:
        geteuid = getattr(os, "geteuid", None)
        return int(geteuid()) if geteuid is not None else -1

    def open_procs(self, path: str) -> int:
        return os.open(path, os.O_WRONLY | _O_CLOEXEC)

    def kill(self, pid: int, sig: int) -> None:
        os.kill(pid, sig)

    def page_size(self) -> int:
        try:
            return int(os.sysconf("SC_PAGE_SIZE"))
        except (AttributeError, ValueError, OSError):
            return 4096

    def nice(self) -> int:
        """This process's nice value; 0 where it has none."""
        getpriority = getattr(os, "getpriority", None)
        if getpriority is None:
            return 0
        try:
            return int(getpriority(getattr(os, "PRIO_PROCESS", 0), 0))
        except OSError:
            return 0

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class _ChildJoin:
    """``preexec_fn`` of a run with a cgroup: join it, then demote.

    Runs in the forked child before exec and makes only raw syscalls
    itself.  Writing ``0`` to the run's ``cgroup.procs`` moves the
    writer.  A failed join writes a byte to ``marker`` for the parent,
    then raises when the caps are required (the spawn fails) and is
    ignored otherwise; the job then keeps the inherited
    ``oom_score_adj``, as an uncapped run does.  ``then`` is the uid/gid
    demotion hook: the demoted user could not write the delegated
    ``cgroup.procs``.
    """

    __slots__ = ("fd", "marker", "fix_oom_adj", "required", "then", "oom")

    def __init__(
        self,
        fd: int,
        marker: int,
        fix_oom_adj: bool,
        required: bool,
        then: Callable[[], Any] | None,
        oom: str = "/proc/self/oom_score_adj",
    ) -> None:
        self.fd = fd
        self.marker = marker
        self.fix_oom_adj = fix_oom_adj
        self.required = required
        self.then = then
        self.oom = oom

    def __call__(self) -> None:
        joined = True
        try:
            os.write(self.fd, b"0")
        except OSError:
            joined = False
            try:
                os.write(self.marker, b"!")
            except OSError:
                pass
            if self.required:
                raise
        if joined and self.fix_oom_adj:
            # the daemon's -1000 is inherited and makes the job OOM-proof;
            # raising it needs no privilege.
            try:
                adj = os.open(self.oom, os.O_WRONLY)
                try:
                    os.write(adj, b"0")
                finally:
                    os.close(adj)
            except OSError:
                pass
        if self.then is not None:
            self.then()


class Backend:
    """Per-run cgroups under the daemon's delegated cgroup."""

    mechanism = MECHANISM

    def __init__(self, fs: CgroupFS | None = None) -> None:
        self._fs = fs if fs is not None else CgroupFS()
        self._lock = threading.Lock()
        # guards _pending and _live
        self._pending_lock = threading.Lock()
        self._setup: Capability | None = None
        self._base: str | None = None
        self._fix_oom_adj = False
        self._pending: set[str] = set()
        # this daemon's run cgroups whose runs are going -> each run's nice
        self._live: dict[str, int | None] = {}
        # when each run cgroup that this daemon did not make was first seen
        self._foreign: dict[str, float] = {}
        self._clock: Callable[[], float] = time.monotonic
        # whether pids.events counts only limits at or below its cgroup
        self._scoped_pids_events = False
        # whether base's subtree_control holds cpu
        self._cpu_base = False
        # why the last attempt to turn cpu on failed
        self._cpu_failure: str | None = None
        # the config wants cpu off; it goes off once no run has a cpu cap
        self._cpu_unwanted = False
        # runs a call off the caller's thread (abort can run on the loop)
        self._soon: Callable[[Callable[[], Any]], None] = _in_thread
        #: True while runs/ enables cpu: each run's cgroup is then a CPU
        #: group of its own, weighted by the run's nice
        self.weighting = False

    # --- probe ------------------------------------------------------------

    def capability(self) -> Capability:
        """Read-only probe of what this process's cgroup allows."""
        return self._probe()[0]

    def _probe(self) -> tuple[Capability, str | None]:
        fs = self._fs
        root = fs.root
        if not fs.exists(os.path.join(root, "cgroup.controllers")):
            reason = "no cgroup v2 hierarchy is mounted at {}".format(root)
            return _unavailable(reason, _V2_HINT), None
        try:
            base = _CgroupV2Reader._resolve_own_dir(
                root, os.path.join(fs.proc, "self", "cgroup")
            )
        except OSError:
            base = None
        if base is None:
            reason = "cannot find this process's cgroup under {}".format(root)
            return _unavailable(reason), None
        ns_root = base == root
        if ns_root and not fs.exists(os.path.join(base, "cgroup.type")):
            # the real root has no cgroup.type; a namespace root does
            return _unavailable("cronstable runs in the root cgroup"), None
        gap = self._delegation_gap(base, ns_root)
        if gap is not None:
            return _unavailable(gap), None
        return self._resources(base), base

    def _delegation_gap(self, base: str, ns_root: bool) -> str | None:
        """Why ``base`` is not delegated to this process, or ``None``."""
        fs = self._fs
        for name in ("", "cgroup.procs", "cgroup.subtree_control"):
            path = os.path.join(base, name) if name else base
            if not fs.writable(path):
                return (
                    "cgroup {} is not delegated to cronstable ({} is not "
                    "writable)".format(base, path)
                )
        if fs.geteuid() != 0 or ns_root:
            # Ownership is the delegation for a non-root daemon (systemd
            # chowns these files for Delegate=yes with User=).
            return None
        for attr in ("trusted.delegate", "user.delegate"):
            try:
                if fs.getxattr(base, attr) == b"1":
                    return None
            except OSError:
                continue
        # systemd strips controllers from undelegated units on reload
        return (
            "cgroup {} is not delegated to cronstable (it has no "
            "trusted.delegate or user.delegate attribute)".format(base)
        )

    def _resources(self, base: str) -> Capability:
        try:
            offered = set(
                self._fs.read(os.path.join(base, "cgroup.controllers")).split()
            )
        except OSError:
            offered = set()
        resources = frozenset(
            r for r, c in _CONTROLLERS.items() if c in offered
        )
        if not resources:
            return _unavailable(
                "cgroup {} offers none of the cpu, memory, and pids "
                "controllers".format(base),
                _CONTROLLER_HINT,
            )
        reasons = []
        hints = []
        missing = [
            c
            for r, c in _CONTROLLERS.items()
            if r != "cpu" and r not in resources
        ]
        if missing:
            plural = "s" if len(missing) > 1 else ""
            reasons.append(
                "cgroup {} does not offer the {} controller{}".format(
                    base, " and ".join(missing), plural
                )
            )
            hints.append(_CONTROLLER_HINT)
        if "cpu" not in resources:
            reasons.append(_CPU_NOT_DELEGATED)
            hints.append(_cpu_hint(base))
        if not reasons:
            return Capability(True, MECHANISM, resources, None, None)
        return Capability(
            True, MECHANISM, resources, "; ".join(reasons), ". ".join(hints)
        )

    # --- setup ------------------------------------------------------------

    def setup(self) -> Capability:
        """Build the layout once; a failed setup runs again next call.

        It enables the memory and pids controllers; :meth:`set_cpu` turns
        cpu on and off.
        """
        with self._lock:
            if self._setup is not None:
                return self._current()
            cap, base = self._probe()
            if base is None or not cap.available:
                return cap
            controllers = [
                _CONTROLLERS[r] for r in sorted(cap.resources) if r != "cpu"
            ]
            try:
                self._build_layout(base, controllers)
            except OSError as exc:
                # a process that cannot move is no delegation problem
                return _unavailable(
                    "could not set up cgroup {}: {}".format(
                        base, _describe(exc)
                    ),
                    None if isinstance(exc, _Stuck) else _DELEGATE_HINT,
                )
            self._base = base
            self._fix_oom_adj = self._daemon_oom_adj() == _OOM_SCORE_ADJ_MIN
            self._scoped_pids_events = self._pids_events_scoped(
                os.path.join(base, RUNS_DIR)
            )
            self._setup = cap
            return cap

    def _current(self) -> Capability:
        """The setup's capability, less cpu when turning it on failed."""
        cap = self._setup
        failure = self._cpu_failure
        if cap is None:
            return _unavailable("the cgroup layout is not set up", None)
        if failure is None:
            return cap
        return Capability(
            True,
            MECHANISM,
            cap.resources - {"cpu"},
            failure if cap.reason is None else cap.reason + "; " + failure,
            cap.hint,
        )

    def _build_layout(self, base: str, controllers: list[str]) -> None:
        daemon = os.path.join(base, DAEMON_LEAF)
        runs = os.path.join(base, RUNS_DIR)
        for path in (daemon, runs):
            try:
                self._fs.mkdir(path)
            except FileExistsError:
                pass
        self._evacuate(base, daemon, controllers)
        self._enable(runs, controllers)
        # an earlier daemon may have left cpu on
        self._cpu_base = "cpu" in self._enabled(base)
        self.weighting = "cpu" in self._enabled(runs)
        self._sweep(runs)

    def _sweep(self, runs: str) -> None:
        """Remove the empty run cgroups an earlier daemon left, once.

        One with processes in it stays: it can belong to another
        cronstable that shares this cgroup (one started as a job, or by
        ``docker exec``).
        """
        try:
            names = self._fs.subdirs(runs)
        except OSError:
            return
        for name in names:
            path = os.path.join(runs, name)
            try:
                busy = bool(self._tree_pids(path))
            except (OSError, ValueError):
                continue
            if not busy:
                self._remove(path)

    def _evacuate(
        self, base: str, daemon: str, controllers: list[str]
    ) -> None:
        """Move every process in ``base`` to ``daemon``; enable controllers.

        That includes a one-file bootloader parent, a container's init and
        leftover jobs.  An exiting process cannot move but counts until it
        is gone, so each new round waits longer.
        """
        fs = self._fs
        procs = os.path.join(daemon, "cgroup.procs")
        pause = _SETUP_PAUSE
        before: list[int] = []
        left: list[int] = []
        busy = OSError(errno.EBUSY, os.strerror(errno.EBUSY))
        for attempt in range(_SETUP_ROUNDS):
            if attempt:
                fs.sleep(pause)
                pause = min(2 * pause, _SETUP_PAUSE_MAX)
            before, left = left, sorted(self._pids(base))
            for pid in left:
                try:
                    fs.write(procs, str(pid))
                except ProcessLookupError:
                    continue
            try:
                self._enable(base, controllers)
                return
            except OSError as exc:
                if exc.errno != errno.EBUSY:
                    raise
                busy = exc
        if left and left == before:
            raise _Stuck(left)
        if left:
            raise OSError(errno.EBUSY, "processes keep arriving in it")
        raise busy

    def _pids_events_scoped(self, runs: str) -> bool:
        """Whether ``pids.events`` counts only limits at or below its cgroup.

        Linux 6.11 scoped it and added ``pids.events.local``.  Before that,
        and under the ``pids_localevents`` mount option, it counts every
        fork refused in the cgroup, whichever ancestor's limit refused it.
        """
        fs = self._fs
        if not fs.exists(os.path.join(runs, "pids.events.local")):
            return False
        try:
            mounts = fs.read(os.path.join(fs.proc, "self", "mountinfo"))
        except OSError:
            return True
        for line in mounts.splitlines():
            _, sep, rest = line.partition(" - ")
            fields = rest.split()
            if (
                sep
                and len(fields) >= 3
                and fields[0] == "cgroup2"
                and "pids_localevents" in fields[2].split(",")
            ):
                return False
        return True

    def _enabled(self, path: str) -> set[str]:
        """The controllers ``path`` enables for its children."""
        control = os.path.join(path, "cgroup.subtree_control")
        return set(self._fs.read(control).split())

    def _enable(self, path: str, controllers: list[str]) -> None:
        """Enable the missing controllers in one write.

        A lone threaded controller (cpu, pids) enabled on a populated
        cgroup turns it into a threaded domain that can never take memory.
        """
        enabled = self._enabled(path)
        missing = [c for c in controllers if c not in enabled]
        if missing:
            self._fs.write(
                os.path.join(path, "cgroup.subtree_control"),
                " ".join("+" + c for c in missing),
            )

    # --- cpu control ------------------------------------------------------

    def set_cpu(self, want: bool, *, retry: bool = False) -> Capability:
        """Turn cpu on or off in base and runs/, as the config wants.

        It goes on only where the host offers it, and an attempt that
        failed runs again only with ``retry``.  A failure keeps the memory
        and pids caps and takes cpu out of the capability.  Off waits
        while a run cgroup has a cpu cap, so each run keeps its cap to the
        end; the last such run's end turns cpu off.  Off is also
        best-effort: a busy cgroup keeps cpu until a later try.  Returns
        the capability after the change.
        """
        with self._lock:
            if self._setup is None:
                return self._current()
            self._cpu_unwanted = not want
            if want:
                self._cpu_up(retry)
            else:
                self._cpu_failure = None
                self._read_cpu()
                if self._cpu_down():
                    logger.debug(
                        "the cpu controller stays on under %s until the "
                        "runs with a cpu cap end",
                        self._base,
                    )
            return self._current()

    def need_cpu(self, *, retry: bool = False) -> Capability:
        """Turn cpu on for a run with a cpu cap, whatever the config wants.

        When the config wants cpu off, it goes off again after that run.
        Returns the capability after the change.
        """
        with self._lock:
            self._cpu_up(retry)
            return self._current()

    def _cpu_up(self, retry: bool) -> None:
        """Turn cpu on where the host offers it (under ``_lock``)."""
        cap = self._setup
        if (
            cap is None
            or "cpu" not in cap.resources
            or (self._cpu_failure is not None and not retry)
        ):
            return
        self._read_cpu()
        self._cpu_failure = None if self.weighting else self._cpu_on()

    def _cpu_down(self) -> bool:
        """Turn cpu off unless a run has a cpu cap (under ``_lock``).

        Returns whether a cpu cap keeps it on.
        """
        if not (self.weighting or self._cpu_base):
            return False
        if self._cpu_in_use():
            return True
        self._cpu_off()
        return False

    def _settle_cpu(self) -> None:
        """Turn cpu off if the config wants it off and no cpu cap is left."""
        with self._lock:
            if self._cpu_unwanted:
                self._cpu_down()

    def _cpu_in_use(self) -> bool:
        """Whether a cgroup in runs/ has a cpu cap (under ``_lock``).

        This daemon's live runs count, and its cgroups that wait for
        :meth:`collect` do not: their runs are over.  A cgroup it did not
        make may be a run of another daemon that shares base, so it counts
        while it holds processes, and for :data:`_FOREIGN_GRACE` seconds
        after it is first seen.
        """
        runs = os.path.join(str(self._base), RUNS_DIR)
        try:
            names = self._fs.subdirs(runs)
        except OSError:
            return False
        with self._pending_lock:
            done = frozenset(self._pending)
            live = frozenset(self._live)
        now = self._clock()
        foreign = {}
        in_use = False
        for name in names:
            path = os.path.join(runs, name)
            if path in done:
                continue
            if path not in live:
                foreign[path] = first = self._foreign.get(path, now)
                if now - first >= _FOREIGN_GRACE and not self._busy(path):
                    continue
            in_use = in_use or self._cpu_capped(path)
        self._foreign = foreign
        return in_use

    def _busy(self, path: str) -> bool:
        """Whether ``path`` or a child cgroup holds a process."""
        try:
            return bool(self._tree_pids(path))
        except (OSError, ValueError):
            return True

    def _cpu_capped(self, path: str) -> bool:
        """Whether the cgroup ``path`` has a cpu cap (none without cpu.max)."""
        try:
            quota = self._fs.read(os.path.join(path, "cpu.max")).split()
        except OSError:
            return False
        return bool(quota) and quota[0] != "max"

    def _read_cpu(self) -> None:
        """Read where cpu is on; another daemon sharing base may change it.

        Cpu that another daemon turned on weighs this daemon's runs too.
        """
        base = str(self._base)
        was = self.weighting
        try:
            self._cpu_base = "cpu" in self._enabled(base)
            self.weighting = "cpu" in self._enabled(
                os.path.join(base, RUNS_DIR)
            )
        except OSError:
            return
        if self.weighting and not was:
            self._weigh_live()

    def _cpu_on(self) -> str | None:
        """Enable cpu in base, then runs/; why it failed, or ``None``."""
        base = str(self._base)
        daemon = os.path.join(base, DAEMON_LEAF)
        try:
            # setup emptied base; a process that arrived since moves first
            self._evacuate(base, daemon, ["cpu"])
            self._cpu_base = True
            self._enable(os.path.join(base, RUNS_DIR), ["cpu"])
        except OSError as exc:
            self._cpu_off()
            return "could not enable the cpu controller in {}: {}".format(
                base, _describe(exc)
            )
        self.weighting = True
        problem = self._write_weight(daemon, None)
        if problem is not None:
            logger.debug("the daemon's cpu weight is not set: %s", problem)
        self._weigh_live()
        return None

    def _weigh_live(self) -> None:
        """Weigh this daemon's runs in progress once cpu turns on."""
        with self._pending_lock:
            live = list(self._live.items())
        for path, nice in live:
            problem = self._write_weight(path, nice)
            if problem is not None:
                logger.debug(
                    "the cpu weight of %s is not set: %s", path, problem
                )

    def _cpu_off(self) -> None:
        """Disable cpu in runs/, then base (which needs runs/ off first)."""
        base = str(self._base)
        try:
            if self.weighting:
                self._disable(os.path.join(base, RUNS_DIR), "cpu")
                self.weighting = False
            if self._cpu_base:
                self._disable(base, "cpu")
                self._cpu_base = False
        except OSError as exc:
            # EBUSY: a cgroup below enables cpu for its own children
            logger.debug(
                "the cpu controller stays on under %s: %s",
                base,
                _describe(exc),
            )

    def _disable(self, path: str, controller: str) -> None:
        self._fs.write(
            os.path.join(path, "cgroup.subtree_control"), "-" + controller
        )

    def _daemon_oom_adj(self) -> int:
        path = os.path.join(self._fs.proc, "self", "oom_score_adj")
        try:
            return int(self._fs.read(path).strip())
        except (OSError, ValueError):
            return 0

    # --- per run ----------------------------------------------------------

    def create(
        self, caps: ResourceLimits, name: str, *, nice: int | None = None
    ) -> "Container":
        """Make one run's cgroup and write its caps (worker thread).

        While :attr:`weighting` is on, it also writes ``cpu.weight.nice``:
        ``nice``, or the daemon's own nice for ``None``.  When cpu turns on
        during the run, :meth:`_cpu_on` writes it.  Without caps the
        cgroup holds only that weight.  A cpu cap turns cpu on, as
        :meth:`need_cpu` does, and cpu cannot go off before the cap is
        written.  Never raises: a failed cap write leaves that resource
        unenforced, and a weight that does not apply is the container's
        ``weight_problem``.
        """
        if caps.cpu is None:
            return self._create(caps, name, nice)
        with self._lock:
            self._cpu_up(retry=False)
            return self._create(caps, name, nice)

    def _create(
        self, caps: ResourceLimits, name: str, nice: int | None
    ) -> "Container":
        configured = [r for r in _CONTROLLERS if getattr(caps, r) is not None]
        path, problem = self._run_dir(name)
        if path is None:
            return Container(
                self,
                None,
                dict.fromkeys(configured, problem),
                weight_problem=None if configured else problem,
            )
        # before weighting is read, so a _cpu_on from here on weighs it
        with self._pending_lock:
            self._live[path] = nice
        unenforced: dict[str, str] = {}
        for resource, writes in self._cap_writes(caps, path):
            failure = self._write_cap(path, writes)
            if failure is not None:
                unenforced[resource] = failure
        if self.weighting:
            weight = self._write_weight(path, nice)
        else:
            weight = None if configured else _WEIGHT_OFF
        applied = frozenset(r for r in configured if r not in unenforced)
        return Container(
            self, path, unenforced, applied, weight_problem=weight
        )

    def _run_dir(self, name: str) -> tuple[str | None, str]:
        """A new run cgroup's path, or ``None`` and why it is missing."""
        base = self._base
        if base is None:
            return None, "the cgroup layout is not set up"
        runs = os.path.join(base, RUNS_DIR)
        path = os.path.join(
            runs, "{}-{}".format(os.urandom(6).hex(), _slug(name))
        )
        try:
            self._fs.mkdir(path)
        except OSError as exc:
            return None, "could not create a cgroup in {}: {}".format(
                runs, _describe(exc)
            )
        return path, ""

    def _write_weight(self, path: str, nice: int | None) -> str | None:
        """Write ``cpu.weight.nice`` in ``path``; the failure, or ``None``."""
        value = self._fs.nice() if nice is None else nice
        value = max(_NICE_MIN, min(_NICE_MAX, value))
        try:
            self._fs.write(os.path.join(path, "cpu.weight.nice"), str(value))
        except OSError as exc:
            return "could not set cpu.weight.nice: {}".format(_describe(exc))
        return None

    def _write_cap(
        self, path: str, writes: list[tuple[str, str]]
    ) -> str | None:
        """Write one resource's files; the failure, or ``None``.

        When a write fails, the files written before it are lifted again,
        last first; a lift that fails is skipped.
        """
        for index, (filename, value) in enumerate(writes):
            try:
                self._fs.write(os.path.join(path, filename), value)
            except OSError as exc:
                for written, _ in reversed(writes[:index]):
                    try:
                        self._fs.write(
                            os.path.join(path, written), _UNSET[written]
                        )
                    except OSError:
                        continue
                return "could not set {}: {}".format(filename, _describe(exc))
        return None

    def _cap_writes(
        self, caps: ResourceLimits, path: str
    ) -> Iterator[tuple[str, list[tuple[str, str]]]]:
        if caps.cpu is not None:
            quota = min(
                max(_MIN_QUOTA_USEC, round(caps.cpu * _PERIOD_USEC)),
                _MAX_QUOTA_USEC,
            )
            yield "cpu", [("cpu.max", "{} {}".format(quota, _PERIOD_USEC))]
        if caps.memory is not None:
            page = self._fs.page_size()
            writes = [("memory.max", str(caps.memory - caps.memory % page))]
            # absent when swap accounting is off; the cap applies anyway
            if self._fs.exists(os.path.join(path, "memory.swap.max")):
                writes.append(("memory.swap.max", "0"))
            writes.append(("memory.oom.group", "1"))
            yield "memory", writes
        if caps.processes is not None:
            # pids.max counts threads as well as processes
            yield "processes", [("pids.max", str(caps.processes))]

    def _pids(self, path: str) -> list[int]:
        text = self._fs.read(os.path.join(path, "cgroup.procs"))
        return [int(p) for p in text.split()]

    def _tree_pids(self, path: str) -> list[int]:
        """Pids in ``path`` and its child cgroups."""
        pids = self._pids(path)
        for name in self._fs.subdirs(path):
            pids.extend(self._tree_pids(os.path.join(path, name)))
        return pids

    # --- removal ----------------------------------------------------------

    def _remove(self, path: str) -> bool:
        """Remove ``path`` and child cgroups; False while it is busy."""
        fs = self._fs
        try:
            fs.rmdir(path)
            return True
        except FileNotFoundError:
            return True
        except OSError as exc:
            if exc.errno not in (errno.EBUSY, errno.ENOTEMPTY):
                return False
        try:
            children = fs.subdirs(path)
        except OSError:
            return False
        if not children:
            return False
        if not all(self._remove(os.path.join(path, n)) for n in children):
            return False
        return self._remove(path)

    def _remove_or_defer(self, path: str) -> None:
        if not self._remove(path):
            self._defer([path])

    def _forget(self, path: str) -> None:
        """Drop a run cgroup from the live runs once its run is over."""
        with self._pending_lock:
            self._live.pop(path, None)

    def _defer(self, paths: list[str]) -> None:
        """Queue cgroups for :meth:`collect` to remove."""
        with self._pending_lock:
            self._pending.update(paths)

    def collect(self) -> None:
        """Remove the queued cgroups that no longer hold processes.

        Then cpu goes off if the config wants it off and no cpu cap is
        left.
        """
        with self._pending_lock:
            pending = list(self._pending)
        for path in pending:
            if self._remove(path):
                with self._pending_lock:
                    self._pending.discard(path)
        self._settle_cpu()

    @property
    def pending(self) -> frozenset[str]:
        with self._pending_lock:
            return frozenset(self._pending)


class Container:
    """One run's cgroup: its caps, its cpu weight, or both."""

    def __init__(
        self,
        backend: Backend,
        path: str | None,
        unenforced: dict[str, str],
        applied: frozenset[str] = frozenset(),
        *,
        weight_problem: str | None = None,
    ) -> None:
        self._backend = backend
        self._fs = backend._fs
        self.path = path
        self.applied = applied
        self.unenforced = unenforced
        #: why the run's cpu weight does not apply, or ``None``
        self.weight_problem = weight_problem
        # no caps: the cgroup is there for the cpu weight alone
        self._weight_only = not applied and not unenforced
        self._lock = threading.Lock()
        # (cgroup.procs fd, marker read end, marker write end) until the
        # child's join is settled
        self._join: tuple[int, int, int] | None = None
        # False when the join was never set up or the child reported that
        # it failed
        self._joined = False
        # whether attach found the job inside; None before attach, or when
        # attach raised (``_attach_failed``)
        self._member: bool | None = None
        self._attach_failed = False
        self._stats: dict[str, Any] | None = None
        if path is not None:
            self._open_join()

    def _open_join(self) -> None:
        procs = os.path.join(str(self.path), "cgroup.procs")
        held: list[int] = []
        try:
            held.append(_above_stdio(self._fs.open_procs(procs)))
            read_end, write_end = os.pipe()
            held.append(read_end)
            held.append(_above_stdio(write_end))
        except OSError as exc:
            for fd in held:
                os.close(fd)
            self._lost(
                "could not open cgroup.procs: {}".format(_describe(exc))
            )
            return
        self._join = (held[0], held[1], held[2])
        self._joined = True

    def _lost(self, reason: str) -> None:
        """Record that the job is outside the cgroup: nothing there applies."""
        for resource in self.applied:
            self.unenforced.setdefault(resource, reason)
        if self._weight_only and self.weight_problem is None:
            self.weight_problem = reason

    def _settle_join(self) -> bool:
        """Close the join fds; False when the child could not join."""
        with self._lock:
            join, self._join = self._join, None
            if join is None:
                return self._joined
            fd, read_end, write_end = join
            os.close(fd)
            os.close(write_end)
            try:
                # The child exec'd or exited before the spawn returned and
                # every write end is closed, so this never blocks.
                self._joined = os.read(read_end, 1) != b"!"
            finally:
                os.close(read_end)
            return self._joined

    def spawn_kwargs(self, kwargs: dict[str, Any], *, required: bool) -> None:
        """Install the child-side join as ``preexec_fn`` (no I/O)."""
        join = self._join
        if join is None:
            return
        fix = self._backend._fix_oom_adj and "memory" in self.applied
        kwargs["preexec_fn"] = _ChildJoin(
            join[0], join[2], fix, required, kwargs.get("preexec_fn")
        )

    def attach(self, pid: int, *, required: bool) -> str | None:
        """Confirm the child joined; see the backend interface.

        On an error it kills a required run's child, then re-raises.
        Membership then stays unknown: the monitor and the stats skip the
        cgroup, and :meth:`kill` still kills everything in it.
        """
        try:
            self._member = self._settle_join() and self._is_member(pid)
        except Exception:
            self._attach_failed = True
            if required:
                self._kill_pid(pid)
            raise
        if self._member:
            return None
        if required:
            self._kill_pid(pid)
            return _NOT_MOVED
        self._lost(_NOT_MOVED)
        return None

    def _kill_pid(self, pid: int) -> None:
        try:
            self._fs.kill(pid, _SIGKILL)
        except OSError:
            pass

    def _contained(self) -> bool:
        """Whether the job joined and attach did not find it elsewhere."""
        return (
            self._joined
            and self._member is not False
            and not self._attach_failed
        )

    def _is_member(self, pid: int) -> bool:
        fs = self._fs
        try:
            text = fs.read(os.path.join(fs.proc, str(pid), "cgroup"))
        except OSError:
            return True  # already gone; the marker says it joined
        for line in text.splitlines():
            if line.startswith("0::"):
                rel = line[3:].strip().lstrip("/")
                where = os.path.normpath(os.path.join(fs.root, rel))
                # a child cgroup the job moved into is still the run's
                return where == self.path or where.startswith(
                    str(self.path) + os.sep
                )
        return False

    def abort(self) -> None:
        """Release the cgroup of a run that never spawned.

        A child that failed its join before the spawn failed leaves every
        applied cap unenforced.  The cgroup is queued for collect: abort
        can run on the event loop, where ``rmdir`` must not run.  If its
        cpu cap may be holding cpu on, it starts a collect on a thread.
        """
        if not self._settle_join():
            self._lost(_NOT_MOVED)
        if self._stats is None:
            self._stats = _no_stats()
            if self.path is not None:
                backend = self._backend
                backend._forget(self.path)
                backend._defer([self.path])
                if "cpu" in self.applied and backend._cpu_unwanted:
                    backend._soon(backend.collect)

    def kill(self) -> bool:
        """SIGKILL everything in the cgroup; True when that reached the job.

        False when attach has not confirmed the job is inside, and before
        5.14, where each listed pid gets SIGKILL in place of
        ``cgroup.kill``.  The caller then signals the process group too.
        """
        path = self.path
        if path is None or self._member is False:
            return False
        return self._kill_cgroup(path) and self._member is True

    def _kill_cgroup(self, path: str) -> bool:
        try:
            self._fs.write(os.path.join(path, "cgroup.kill"), "1")
            return True
        except OSError as exc:
            if exc.errno == errno.ENODEV:
                return True
            if exc.errno == errno.ENOENT and not self._fs.isdir(path):
                return True
        try:
            # the cgroup's own list: membership may be unknown here
            pids = self._backend._tree_pids(path)
        except (OSError, ValueError):
            pids = []
        for pid in pids:
            try:
                self._fs.kill(pid, _SIGKILL)
            except OSError:
                continue
        return False

    def finish(self) -> dict[str, Any]:
        """Read the run's counters, then remove the cgroup or queue it.

        It tries ``rmdir`` once; a busy cgroup (a leftover, or the EBUSY
        that 6.19 can report just after the last process was reaped) goes
        to :meth:`Backend.collect`.  A run with a cpu cap may have held
        cpu on, so cpu then goes off if the config wants it off.
        """
        if self._stats is not None:
            return dict(self._stats)
        self._settle_join()
        stats = self._read_stats()
        self._stats = stats
        if self.path is not None:
            self._backend._forget(self.path)
            self._backend._remove_or_defer(self.path)
            if "cpu" in self.applied:
                self._backend._settle_cpu()
        return dict(stats)

    def _read_stats(self) -> dict[str, Any]:
        stats = _no_stats()
        if self.path is None or not self._contained():
            return stats
        active = self.applied - set(self.unenforced)
        if "memory" in active:
            events = self._keyed("memory.events")
            # "oom" counts this cgroup's own limit; the kill counters also
            # count kills by a host-wide OOM
            stats["memory_exceeded"] = events.get("oom", 0) > 0 and (
                events.get("oom_kill", 0) > 0
                or events.get("oom_group_kill", 0) > 0
            )
        stats["peak_memory_bytes"] = self._int_file("memory.peak")
        if "cpu" in active:
            throttled = self._keyed("cpu.stat").get("throttled_usec")
            if throttled is not None:
                stats["cpu_throttled_seconds"] = throttled / 1_000_000
        if "processes" in active:
            hits = self._keyed("pids.events").get("max")
            if (
                hits
                and not self._backend._scoped_pids_events
                and self._below_pids_cap()
            ):
                # a limit above the run (such as TasksMax) refused them all
                hits = 0
            stats["process_limit_hits"] = hits
        return stats

    def _below_pids_cap(self) -> bool:
        """Whether ``pids.peak`` (Linux 6.1) stayed below ``pids.max``."""
        peak = self._int_file("pids.peak")
        cap = self._int_file("pids.max")
        return peak is not None and cap is not None and peak < cap

    def _keyed(self, name: str) -> dict[str, int]:
        try:
            return _keyed(self._fs.read(os.path.join(str(self.path), name)))
        except OSError:
            return {}

    def _int_file(self, name: str) -> int | None:
        try:
            return int(self._fs.read(os.path.join(str(self.path), name)))
        except (OSError, ValueError):
            return None

    def members(self) -> list[int] | None:
        """Pids in the run's cgroup and its child cgroups.

        ``None`` when the job is not inside, so the monitor walks the
        process tree instead.
        """
        if self.path is None or not self._contained():
            return None
        try:
            return self._backend._tree_pids(self.path)
        except (OSError, ValueError):
            return None

    def cpu_times(self) -> tuple[float, float] | None:
        """``(user, system)`` CPU seconds of everything that ran here."""
        if self.path is None or not self._contained():
            return None
        stat = self._keyed("cpu.stat")
        user = stat.get("user_usec")
        system = stat.get("system_usec")
        if user is None or system is None:
            return None
        return user / 1_000_000, system / 1_000_000
