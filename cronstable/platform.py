"""OS-specific behavior, isolated so the rest of cronstable stays portable.

cronstable began life POSIX-only.  Everything that genuinely differs between
Unix and Windows lives here behind a small, uniform surface, so the
scheduler, the job runner, the config loader and the entry point read the
same on every platform and only this module needs a per-OS branch:

* :data:`DEFAULT_SHELL`: how a string command is handed to a shell;
* :data:`DEFAULT_CONFIG_PATH`: where ``-c`` looks by default;
* :func:`supports_unix_sockets`: whether ``unix://`` web listeners work;
* :func:`encode_argv`: the argv form the platform's subprocess layer wants;
* :func:`new_process_group_kwargs` / :func:`apply_priority` /
  :func:`kill_process_group`: spawning a job so its descendants are reachable
  as one unit, giving that unit one of :data:`PRIORITY_LEVELS` to run at, and
  taking it down;
* :func:`install_shutdown_handlers`: wiring Ctrl-C / termination to a
  graceful-shutdown callback on whichever event loop the platform provides.

Per-job ``user``/``group`` switching stays in :mod:`cronstable.config` (it
needs the ``grp``/``pwd`` databases), but is likewise gated on
:data:`IS_WINDOWS`.
"""

from __future__ import annotations

import contextlib
import errno
import logging
import os
import re
import signal
import sys
import time
from collections.abc import Callable, Iterator, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

# asyncio is imported lazily (inside _taskkill_tree, the one function that
# reaches for it at runtime) and only for typing here: this module is what
# ``cronstable`` the entry point imports for DEFAULT_CONFIG_PATH, so every
# `cronstable --version` and every job-spawned `cronstable state get` used to
# pay ~50ms and several MB of RSS for an event-loop package it never touches.
# The annotations below are strings thanks to the __future__ import above.
if TYPE_CHECKING:
    import asyncio

# Platform-specific file-locking primitive, imported behind a ``sys.platform``
# guard so each OS pulls in only the module it has (``fcntl`` is Unix-only,
# ``msvcrt`` Windows-only) and mypy, pinned to ``platform = linux``, checks
# just the POSIX branch, exactly as the signal handling below is arranged.
if sys.platform == "win32":  # pragma: no cover (windows)
    import msvcrt
else:  # pragma: no cover (posix) - fcntl exists nowhere else
    import fcntl

logger = logging.getLogger("cronstable")

#: True on Windows, where the absent POSIX facilities (signals on the event
#: loop, unix sockets, ``grp``/``pwd``, ``setuid``) are routed around.
IS_WINDOWS = sys.platform == "win32"


# --- Default shell --------------------------------------------------------
# On POSIX a string command runs as ``/bin/sh -c "<command>"``.  Windows has no
# /bin/sh; an empty default tells the job runner (and the shell reporter) to
# hand the command to the native command processor (``%ComSpec%``, i.e.
# cmd.exe) via :func:`asyncio.create_subprocess_shell`, the closest equivalent.
# Either platform's default can still be overridden per job with ``shell:``.
DEFAULT_SHELL = "" if IS_WINDOWS else "/bin/sh"


# --- Default config location (the ``-c`` default) -------------------------
#: File names a configuration directory can be loaded from, kept in step
#: with the filter in :func:`cronstable.config._parse_config_dir` (YAML by
#: extension, classic crontabs by the markers in
#: :func:`cronstable.crontabs.is_crontab_path`).  Duplicated rather than
#: imported because this module resolves the default at import time and has
#: to stay cheap for every thin-client invocation; a test pins the two
#: definitions together.
_CONFIG_EXTENSIONS = frozenset({".yml", ".yaml", ".crontab", ".cron"})
_CONFIG_BASENAME = "crontab"


def _holds_config(directory: str) -> bool:
    """Whether ``directory`` contains a file the config loader would read."""
    try:
        names = os.listdir(directory)
    except OSError:
        return False
    for name in names:
        lowered = name.lower()
        base, ext = os.path.splitext(lowered)
        # leading _ or . is the loader's own "skip this file" convention
        if not base or base[0] in {"_", "."}:
            continue
        if ext in _CONFIG_EXTENSIONS or lowered == _CONFIG_BASENAME:
            return True
    return False


def _windows_config_home(environ: Mapping[str, str]) -> str:
    """The Windows ``-c`` default, resolved against ``environ``.

    Machine-wide first: ``%ProgramData%\\cronstable`` is the actual Windows
    analog of ``/etc/cronstable.d`` (machine-scoped, shared by every
    account), so when it holds configuration it wins.  It is never created
    implicitly; ``cronstable init`` or an administrator creates it, and from
    then on the same command resolves to the same config for an interactive
    admin and for a service account, whose ``%APPDATA%`` points into an
    invisible ``systemprofile`` directory nobody edits.  Otherwise the
    default stays the historical per-user location: roaming AppData, falling
    back to the user profile if APPDATA is somehow unset (rare; e.g. a bare
    service account with no roaming profile).

    The directory has to hold configuration, not merely exist.  An empty
    ``%ProgramData%\\cronstable`` parses to zero jobs, and because the
    directory does exist the daemon's configuration-not-found guard stays
    quiet, so a machine with a working per-user config would come up
    healthy and silently scheduling nothing.  An interrupted ``init`` or
    an uninstall that took the files but not the folder is enough to leave
    that directory behind.
    """
    program_data = environ.get("PROGRAMDATA")
    if program_data:
        machine_wide = os.path.join(program_data, "cronstable")
        if _holds_config(machine_wide):
            return machine_wide
    base = environ.get("APPDATA") or os.path.expanduser("~")
    return os.path.join(base, "cronstable")


def _default_config_path() -> str:
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        return _windows_config_home(os.environ)
    else:  # pragma: no cover (posix) - /etc has no Windows analogue
        return "/etc/cronstable.d"


#: The directory (or file) ``-c`` defaults to when not given on the command
#: line.  Platform-appropriate so the daemon has a sensible home on each OS.
DEFAULT_CONFIG_PATH = _default_config_path()


# --- Unix-domain socket support ------------------------------------------
def supports_unix_sockets() -> bool:
    """Whether ``unix://`` web listeners can be bound on this platform.

    asyncio's Windows Proactor loop has no ``create_unix_server``, so aiohttp's
    ``UnixSite`` cannot bind there (AF_UNIX exists on recent Windows, but
    asyncio does not drive it).  Such listeners are skipped with a warning.
    """
    return not IS_WINDOWS


# --- Subprocess argv ------------------------------------------------------
def encode_argv(argv: list[str]) -> list[str | bytes]:
    """Return ``argv`` in the form this platform's subprocess layer expects.

    On POSIX the arguments are encoded to UTF-8 bytes so the child's argv is
    independent of the (possibly non-UTF-8) locale.  On Windows processes are
    created with the wide ``CreateProcessW`` API, which works from ``str`` and
    rejects ``bytes`` (``subprocess`` would fail building the command line), so
    the strings are passed through unchanged.
    """
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        return list(argv)
    else:  # pragma: no cover (posix) - CreateProcessW rejects bytes
        return [arg.encode() for arg in argv]


# --- Subprocess process groups -------------------------------------------
#: How long :func:`kill_process_group` waits for a Windows ``taskkill`` to
#: report back before giving up on it.  Generous: the caller's fallback is to
#: kill the direct child only, so a slow-but-working taskkill is worth
#: waiting for, but it must never park the job runner indefinitely.
TASKKILL_TIMEOUT = 10.0


#: ``subprocess.CREATE_NEW_PROCESS_GROUP``, spelled as a literal so this
#: module still imports (and type-checks, pinned to ``platform = linux``)
#: where the ``subprocess`` module does not define the constant.
_CREATE_NEW_PROCESS_GROUP = 0x00000200


# --- Job scheduling priority ----------------------------------------------
#: The scheduling priorities a job can ask for, lowest first.  Named after
#: the Windows priority classes because those are the fixed points: POSIX
#: nice is a continuum that can be mapped onto any vocabulary, the Windows
#: classes cannot, so borrowing their names is what stops ``high`` meaning
#: one thing in the config and another in Task Manager.  REALTIME is
#: deliberately not on the list: it outranks the threads that service disk,
#: keyboard and mouse, so one runaway job at that class can put the host out
#: of reach of the operator who has to stop it.
PRIORITY_LEVELS = ("idle", "below-normal", "normal", "above-normal", "high")

#: What a job gets when its config says nothing.  Not a level that is
#: applied, a level that is *skipped*, which is what keeps the default spawn
#: byte for byte what it was on both platforms.  What that resolves to is not
#: the same sentence on each: on POSIX nothing is reniced, so the job simply
#: keeps the daemon's own nice; on Windows CreateProcess gives an unflagged
#: child the creator's class only when the creator is idle or below-normal
#: and NORMAL otherwise, so a daemon at normal or above launches its jobs at
#: NORMAL rather than at its own class.  Either way cronstable never promotes
#: a job the operator did not ask to promote, which is the case skipping the
#: level exists to avoid.
#: Named (rather than inlined at each comparison) because cronstable.config,
#: cronstable.cron and cronstable.fingerprint all compare against it, the same
#: reason cronstable.config.DEFAULT_PUSH_REPORT is named.
DEFAULT_PRIORITY = "normal"

#: Windows priority-class creation flags, spelled as literals for the same
#: reason as _CREATE_NEW_PROCESS_GROUP above.  ``normal`` maps to no bit at
#: all rather than to NORMAL_PRIORITY_CLASS (0x20): CreateProcess defaults a
#: child to NORMAL only when the creator is not itself idle or below-normal,
#: so emitting the bit would silently *promote* every job of a daemon that
#: was launched below normal, which is what Task Scheduler does by default
#: (its priority 7 is BELOW_NORMAL_PRIORITY_CLASS).
_WINDOWS_PRIORITY_CLASS = {
    "idle": 0x00000040,  # IDLE_PRIORITY_CLASS
    "below-normal": 0x00004000,  # BELOW_NORMAL_PRIORITY_CLASS
    "normal": 0x00000000,  # inherit; see above
    "above-normal": 0x00008000,  # ABOVE_NORMAL_PRIORITY_CLASS
    "high": 0x00000080,  # HIGH_PRIORITY_CLASS
}

#: Absolute POSIX nice values for the same levels.  Absolute, not a delta,
#: so a level describes where the job sits rather than how far it moved from
#: whatever the daemon happened to be started at.  DEFAULT_PRIORITY has no
#: entry on purpose: it is never applied, and nice 0 would be an actively
#: wrong description of it (``priority: normal`` means "the daemon's own
#: nice", which on a daemon started at nice 10 is 10, not 0).
_POSIX_NICE = {
    "idle": 19,
    "below-normal": 10,
    "above-normal": -5,
    "high": -10,
}


def _priority_table_drift(
    levels: tuple[str, ...],
    default: str,
    windows_table: dict[str, int],
    posix_table: dict[str, int],
) -> list[str]:
    """The level names the two per-OS tables and ``levels`` disagree about.

    A level the schema accepts but a table has no row for would reach a
    spawn and raise KeyError there, one job at a time, so the tables are
    checked against the vocabulary at import.

    ``default`` has no POSIX row on purpose (it is never applied), which is
    why the two halves are asked different questions.
    """
    return sorted(
        (set(windows_table) ^ set(levels))
        | (set(posix_table) ^ (set(levels) - {default}))
    )


def _refuse_drifted_priority_tables(
    levels: tuple[str, ...],
    default: str,
    windows_table: dict[str, int],
    posix_table: dict[str, int],
) -> None:
    """Raise unless the priority tables cover exactly ``levels``.

    Dev invariant, in the style of config.py's _DAG_TASK_LAUNCH_KEYS guard.
    A plain ``if``/``raise``, not an assert, since the release binary runs
    under -OO.  It takes the tables as arguments rather than reading the
    module globals so a test can drive both outcomes with a deliberately
    drifted pair; the call below can only ever be the quiet one in a tree
    that imports at all.
    """
    drift = _priority_table_drift(levels, default, windows_table, posix_table)
    if drift:
        raise RuntimeError(
            "platform: the per-OS priority tables and PRIORITY_LEVELS "
            "disagree about these levels, so one of them would fail at "
            "spawn time with a KeyError: {}".format(", ".join(drift))
        )


_refuse_drifted_priority_tables(
    PRIORITY_LEVELS, DEFAULT_PRIORITY, _WINDOWS_PRIORITY_CLASS, _POSIX_NICE
)


def posix_nice_for(priority: str) -> Optional[int]:
    """The absolute POSIX nice value ``priority`` maps to, or ``None``.

    ``None`` for :data:`DEFAULT_PRIORITY`, the one level that is never
    applied.  Exported so the config layer can tell a raise from a lowering
    without keeping a second copy of the table: whether a level needs
    privilege is a question about the number, and the numbers live here.
    """
    return _POSIX_NICE.get(priority)


def new_process_group_kwargs(
    priority: str = DEFAULT_PRIORITY, *, windows: Optional[bool] = None
) -> dict[str, Any]:
    """Subprocess kwargs that isolate a job in its own process group.

    A job command routinely leaves descendants behind (``sh -c 'helper &
    main'``), and each one inherits the write-end of the job's stdout/stderr
    pipe.  Signalling only the direct child on an ``executionTimeout``
    (which is all ``Popen.terminate`` can do) kills the shell but not the
    helper, so the pipe never reaches EOF, the run never finishes draining,
    and the job's slot is held forever (see
    :meth:`cronstable.job.RunningJob._read_job_streams`).

    On POSIX ``start_new_session`` puts the child in a brand-new session, so
    it and every descendant share one process-group id (the child's own
    pid), which :func:`kill_process_group` can then signal as a unit.  On
    Windows ``CREATE_NEW_PROCESS_GROUP`` is the analog, and it earns its
    keep twice over:

    * the child's group no longer receives the daemon console's own Ctrl-C,
      so a graceful daemon shutdown actually waits for running jobs instead
      of the console event killing them first and recording every in-flight
      run as failed (exit 0xC000013A, STATUS_CONTROL_C_EXIT);
    * the child's pid becomes a valid ``GenerateConsoleCtrlEvent`` target,
      which gives :func:`kill_process_group` a *trappable* graceful step
      (CTRL_BREAK) before the forced ``taskkill /F /T``.

    Standard Windows behavior worth naming: a process created in its own
    group starts with Ctrl-C delivery disabled (Ctrl-Break is unaffected),
    which is exactly the console shielding the first bullet describes.

    ``priority`` is the second thing those Windows flags carry, because
    CreateProcess is the only race-free place to set a priority class: the
    child is never scheduled at the wrong one.  How far the class then
    reaches is asymmetric, and it is the same CreateProcess rule quoted at
    :data:`_WINDOWS_PRIORITY_CLASS` above: a grandchild created with no
    class flag of its own inherits the creator's class only
    when the creator is idle or below-normal, and gets NORMAL otherwise.  So
    ``idle`` and ``below-normal`` are inherited by the job's descendants,
    while ``above-normal`` and ``high`` apply to the process cronstable
    spawned and not to what that process goes on to launch: a ``shell: cmd``
    job at ``high`` is cmd.exe at HIGH running its programs at NORMAL.
    POSIX has no such split, because :func:`apply_priority` renices the
    whole group and anything forked later inherits the nice value.

    It is a parameter rather than a caller-assembled flag so that this
    stays the only function in the tree that writes ``creationflags`` (a
    test holds that), which is what makes it impossible for a caller setting
    one of the two bits to drop the other.  POSIX has no spawn-time
    equivalent and is served by :func:`apply_priority` just after the spawn.

    ``windows`` overrides the platform detection so both arms can be
    exercised from either OS, the same injection
    :func:`cronstable.job.shell_spawn` takes.
    """
    if windows is None:
        windows = IS_WINDOWS
    if windows:
        return {
            "creationflags": _CREATE_NEW_PROCESS_GROUP
            | _WINDOWS_PRIORITY_CLASS[priority]
        }
    return {"start_new_session": True}


def apply_priority(
    pid: int, priority: str, *, windows: Optional[bool] = None
) -> bool:
    """Give an already-spawned job's process group its priority, on POSIX.

    Windows needs nothing here: the class rode in on the creation flags
    (:func:`new_process_group_kwargs`).  POSIX has no such knob in
    ``subprocess``, so the group is reniced from the parent the moment the
    child exists, which leaves a brief window where the job runs at the
    priority it inherited; the caller closes that window as far as it can by
    calling this first thing after a successful spawn.

    Renicing from the parent rather than through a ``preexec_fn`` is
    deliberate.  That hook runs between fork and exec, where only
    async-signal-safe calls are sound, and wiring one would put a fork-time
    hook on every spawn including the jobs that have no privilege to drop,
    which :meth:`cronstable.job.RunningJob.start` keeps clear of one on
    purpose (a test asserts the kwarg is absent without ``user``/``group``).

    ``PRIO_PGRP`` rather than ``PRIO_PROCESS``, so a descendant the shell
    already forked in the microseconds before this call is reniced too.
    ``pid`` is by construction the pgid (see
    :func:`new_process_group_kwargs`), and it cannot reach an unrelated
    group for the reason :func:`kill_process_group` sets out.

    Returns ``False`` only when the kernel refused a renice that was
    attempted.  That is not fatal and never fails the run: a refusal is the
    daemon's own privilege ceiling (no CAP_SYS_NICE, no RLIMIT_NICE
    headroom), not anything wrong with this job, so it is logged here once
    at DEBUG and the job goes on at the priority it inherited.  The operator
    is told at config load instead, by
    :meth:`cronstable.config.JobConfig._warn_if_priority_needs_privilege`,
    because a per-run WARNING on a minutely job would be ~1,440 lines a day
    describing a condition that will not change until the deployment does.

    ``windows`` overrides the platform detection, as it does above.
    """
    if windows is None:
        windows = IS_WINDOWS
    if priority == DEFAULT_PRIORITY:
        return True  # inherit: nothing to apply on either platform
    if windows:
        return True
    try:
        os.setpriority(os.PRIO_PGRP, pid, _POSIX_NICE[priority])
    except ProcessLookupError:
        # The group emptied between the spawn and this call.  A run that
        # short did not care what priority it had, and nothing was refused.
        return True
    except OSError as ex:
        logger.debug(
            "could not set priority %r on the process group of pid %s (%s); "
            "the job runs at the priority it inherited",
            priority,
            pid,
            ex,
        )
        return False
    return True


async def kill_process_group(pid: int, *, force: bool) -> bool:
    """Signal the whole process group / tree rooted at ``pid``.

    ``force`` selects an unconditional kill (POSIX ``SIGKILL``, Windows
    ``taskkill /F /T``) over a graceful request to exit (POSIX ``SIGTERM``,
    Windows ``CTRL_BREAK_EVENT``).  Returns whether the group was signalled:
    ``False`` means the caller should fall back to signalling the direct
    child on its own (:meth:`asyncio.subprocess.Process.terminate`), which
    is all this module could do before.

    ``pid`` must be the child spawned with :func:`new_process_group_kwargs`,
    whose pid is by construction its own pgid (a POSIX session leader, a
    Windows ``CREATE_NEW_PROCESS_GROUP`` root).  Signalling the *group*
    rather than the pid is what reaches an orphaned descendant, and it keeps
    working after the leader itself has exited.

    Nothing here can address an unrelated group on either platform, but
    they rule that out by different means.  POSIX does it through the group
    itself: one lives as long as any member does, and the kernel will not
    recycle a pid still in use as a pgid.  Windows reserves nothing once
    the root exits, and what rules out a recycled id there is asyncio
    holding the child's process handle open until it reaps the child, since
    Windows frees a pid for reuse only after the last handle to it closes.
    The caller's contract is therefore narrower than POSIX makes it look:
    signal through the ``Process`` object that owns the handle, as
    :meth:`cronstable.job.RunningJob.cancel` does, never through a bare pid
    remembered across a reap.

    The Windows sequence mirrors the POSIX one.  The graceful call delivers
    ``CTRL_BREAK_EVENT`` to the job's process group; that event is trappable
    (``signal.SIGBREAK`` in Python, ``SetConsoleCtrlHandler`` natively), so
    a job gets ``killTimeout`` seconds to flush and exit before the forced
    call escalates to the ``taskkill /F /T`` tree kill.  Delivering a break
    needs a console shared with the daemon: where there is none (a detached
    or service context), or ``pid`` is not a group root, delivery fails and
    the graceful call becomes the tree kill immediately, this function's
    previous behavior on every call (:func:`_windows_graceful_break`).  A
    service can opt back into the two-step with ``cronstable service
    install --console``, which allocates one
    (:meth:`cronstable.winservice.ServiceHost._prepare_console`); it is off
    by default because a console changes what a job inherits.  The
    forced tree walk resolves descendants through their live parents, so the
    root is deliberately never killed first: that would orphan every
    descendant beyond the walk's reach, and the string-form ``command:``
    runs via ``cmd.exe /c``, so the actual workload is always a grandchild.
    Honest bounds: a descendant that moved itself into a new process group
    (``start /b`` does) never receives the break, and one already orphaned
    before the tree walk runs is no longer in the walkable tree and survives
    it, which is why the stream drain is separately bounded rather than
    trusting this to always succeed.
    """
    # An explicit ``else`` rather than a fall-through tail, because a clause
    # with a header is a clause the coverage profiles can tag and a tail has
    # no header to tag.  The guard stays spelled ``IS_WINDOWS``: the tests
    # drive the Windows arm from POSIX by monkeypatching that name, which a
    # ``sys.platform`` comparison cannot express, and the gating mypy config
    # is pinned to ``platform = linux`` where the POSIX arm type-checks
    # either way.
    if IS_WINDOWS:  # pragma: no cover (windows)
        if force:
            return await _taskkill_tree(pid)
        return await _windows_graceful_break(pid)
    else:  # pragma: no cover (posix) - POSIX signals
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            # the whole group is already gone: nothing to signal
            return False
        except OSError as ex:
            logger.warning(
                "could not signal the process group of pid %s (%s); "
                "falling back to signalling that process alone",
                pid,
                ex,
            )
            return False
        return True


async def _windows_graceful_break(
    pid: int,
) -> bool:  # pragma: no cover (windows) - Windows-only
    """The graceful half of the Windows kill: CTRL_BREAK to ``pid``'s group.

    ``os.kill`` with ``CTRL_BREAK_EVENT`` calls ``GenerateConsoleCtrlEvent``,
    which can only address a process group sharing the daemon's console;
    :func:`new_process_group_kwargs` sets exactly that up at spawn.  Where
    delivery fails (no shared console, or ``pid`` was not spawned as a group
    root) there is nothing trappable to send, so the graceful step degrades
    to the immediate tree kill, the pre-CTRL_BREAK behavior of every call.
    """
    ctrl_break = getattr(signal, "CTRL_BREAK_EVENT", None)
    if ctrl_break is not None:
        try:
            os.kill(pid, ctrl_break)
        except OSError as ex:
            logger.debug(
                "could not deliver CTRL_BREAK_EVENT to the process group "
                "of pid %s (%s); tree-killing it without the graceful step",
                pid,
                ex,
            )
        else:
            return True
    return await _taskkill_tree(pid)


async def _taskkill_tree(pid: int) -> bool:  # pragma: no cover (windows)
    """Kill ``pid`` and its process tree via ``taskkill /F /T``."""
    import asyncio

    try:
        proc = await asyncio.create_subprocess_exec(
            "taskkill",
            "/F",
            "/T",
            "/PID",
            str(pid),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError as ex:
        logger.warning("could not run taskkill for pid %s (%s)", pid, ex)
        return False
    try:
        retcode = await asyncio.wait_for(proc.wait(), TASKKILL_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(
            "taskkill for pid %s did not finish; abandoning it", pid
        )
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        return False
    # 128 is taskkill's "process not found": the tree is already gone, so
    # nothing was signalled and the caller's fallback is a no-op either way.
    return retcode == 0


# --- Graceful shutdown signalling ----------------------------------------
def _install_loop_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    sigs: "tuple[signal.Signals, ...]",
    callback: Callable[[], None],
) -> Callable[[], None]:
    """Install ``callback`` for ``sigs`` on ``loop``; return the remover.

    The one place the install + removal-closure pair lives, so the
    shutdown and reload installers cannot drift apart.
    """
    for sig in sigs:
        loop.add_signal_handler(sig, callback)

    def remove_loop_handlers() -> None:
        for sig in sigs:
            loop.remove_signal_handler(sig)

    return remove_loop_handlers


def install_shutdown_handlers(
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Arrange for ``callback`` to run on a shutdown request (Ctrl-C / TERM).

    Returns a zero-argument cleanup function that removes whatever was
    installed; call it once the loop has finished.

    On POSIX this uses the event loop's native signal handling for SIGINT and
    SIGTERM.  On Windows, where ``loop.add_signal_handler`` raises
    ``NotImplementedError`` on the Proactor loop, it falls back to
    ``signal.signal`` for SIGINT (Ctrl-C) and SIGBREAK (Ctrl-Break),
    marshalling the callback back onto the loop thread with
    ``call_soon_threadsafe`` and ticking a short timer so the interpreter runs
    the pending handler promptly even while the loop is blocked in IOCP; a
    native console-control handler additionally covers the events Python's
    signal module never surfaces (console close, user logoff, OS shutdown),
    turning them into the same graceful drain, bounded by the few seconds
    Windows grants before terminating the process regardless.
    """
    if not IS_WINDOWS:  # pragma: no cover (posix) - loop signal handlers
        return _install_loop_signal_handlers(
            loop, (signal.SIGINT, signal.SIGTERM), callback
        )

    # Windows path lives in its own helper so it is measured only where it can
    # run (like :func:`_taskkill_tree`); this delegation never executes on
    # POSIX, where the branch above has already returned.
    return _install_windows_shutdown_handlers(  # pragma: no cover (windows)
        loop, callback
    )


def install_reload_handler(
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Arrange for ``callback`` to run on a reload request (SIGHUP).

    Returns a zero-argument cleanup function, like
    :func:`install_shutdown_handlers`; call it once the loop has finished.

    POSIX only: Windows has no SIGHUP, and the service accepts no SCM
    reload control (``PARAMCHANGE`` is outside its accepted set), so
    Windows reloads ride the config-file stat watch on the housekeeping
    pass; nothing is installed there and the cleanup is a no-op.
    """
    if not IS_WINDOWS:  # pragma: no cover (posix) - loop signal handler
        return _install_loop_signal_handlers(loop, (signal.SIGHUP,), callback)
    return lambda: None  # pragma: no cover (windows) - nothing to install


def _install_windows_shutdown_handlers(  # pragma: no cover (windows)
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Windows fallback for :func:`install_shutdown_handlers`.

    Installs plain C-level handlers and hops onto the loop thread, because
    ``loop.add_signal_handler`` raises ``NotImplementedError`` on the Proactor
    loop.  (getattr, not signal.SIGBREAK, so this module also type-checks on
    POSIX, where SIGBREAK does not exist.)
    """
    win_sigs = [signal.SIGINT]
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        win_sigs.append(sigbreak)  # Ctrl-Break (Windows-only)
    previous = {}

    def handler(signum, frame):  # runs in the main thread
        loop.call_soon_threadsafe(callback)

    for sig in win_sigs:
        previous[sig] = signal.signal(sig, handler)

    # Console close and OS shutdown never reach signal.signal (the
    # interpreter only maps Ctrl-C and Ctrl-Break onto Python signals), so
    # they get a native console-control handler of their own.
    remove_console_handler = _install_windows_console_handler(loop, callback)

    # A Python signal handler only runs when the main thread returns to the
    # interpreter; while the Proactor loop is blocked in GetQueuedCompletion
    # Status that can be delayed indefinitely.  A lightweight repeating timer
    # keeps the loop ticking so Ctrl-C is observed within the interval.
    heartbeat: asyncio.TimerHandle | None = None

    def _tick() -> None:
        nonlocal heartbeat
        heartbeat = loop.call_later(0.25, _tick)

    heartbeat = loop.call_later(0.25, _tick)

    def restore_signal_handlers() -> None:
        if heartbeat is not None:
            heartbeat.cancel()
        remove_console_handler()
        for sig, prev in previous.items():
            signal.signal(sig, prev)

    return restore_signal_handlers


# Windows console-control events beyond the two the interpreter maps onto
# Python signals (CTRL_C_EVENT -> SIGINT, CTRL_BREAK_EVENT -> SIGBREAK).
# Closing the console window and an OS shutdown or restart arrive as these;
# a process without a handler for them is simply terminated, which for a
# scheduler means jobs truncated mid-write and a reporter storm of "failed"
# runs on every patch-day restart.
_CTRL_CLOSE_EVENT = 2
_CTRL_SHUTDOWN_EVENT = 6

# Deliberately absent from the pair above.  CTRL_LOGOFF_EVENT (5) reaches
# only session-0 processes (services, i.e. exactly the unattended install
# the scheduler is meant to be), and it carries no indication of WHICH user
# logged off, so a service cannot tell "my console is going away" from "some
# administrator closed an RDP session".  Draining on it would stop the
# daemon the first time anyone signed out of the box, which is the opposite
# of surviving logoff.  An interactive run is unaffected either way: those
# processes are terminated at logoff before the event is ever sent.
_CTRL_LOGOFF_EVENT = 5

#: How long the native console-control handler parks its (system-created)
#: thread after scheduling the graceful shutdown.  The process is terminated
#: the moment the handler returns, while a parked handler lets the loop keep
#: draining until Windows' own patience (about five seconds for a closed
#: window, registry-configurable) expires and it terminates the process
#: regardless; the park only has to outlast every grace period the OS might
#: grant.
_CONSOLE_EVENT_HANDLER_PARK = 60.0


def _console_event_requests_shutdown(event: int) -> bool:
    """Whether console-control ``event`` is one the daemon must treat as a
    shutdown request itself (console close / OS shutdown), rather than
    leave to the interpreter's own handler chain, which turns Ctrl-C and
    Ctrl-Break into the Python-level signals handled above.  Logoff is not
    one of them; see :data:`_CTRL_LOGOFF_EVENT`."""
    return event in (
        _CTRL_CLOSE_EVENT,
        _CTRL_SHUTDOWN_EVENT,
    )


def _install_windows_console_handler(  # pragma: no cover (windows)
    loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
) -> Callable[[], None]:
    """Catch console close and OS shutdown and drain gracefully.

    Without this handler those events are a hard kill: in-flight jobs die
    mid-write and nothing is recorded.  The handler schedules the same
    graceful-shutdown callback the signal handlers use, then parks its
    thread: Windows ends the process either when the handler returns or when
    the OS grace period expires, so parking is what buys the loop its
    drain time.  Best effort by design; the OS grace period bounds how much
    draining can happen.
    """
    import ctypes
    from ctypes import wintypes

    # The ignore is for the same reason as every windll use in this module:
    # mypy is pinned to platform = linux, where the Windows-only ctypes
    # surface does not exist.
    handler_type = ctypes.WINFUNCTYPE(  # type: ignore[attr-defined]
        wintypes.BOOL, wintypes.DWORD
    )

    def _handle(event: int) -> bool:
        # Runs on a thread the system creates per event.  Nothing may
        # escape (an exception would tear through the ctypes callback), and
        # Ctrl-C / Ctrl-Break must pass to the next handler in the chain,
        # where the interpreter turns them into Python signals.
        try:
            if not _console_event_requests_shutdown(event):
                return False
            try:
                loop.call_soon_threadsafe(callback)
            except RuntimeError:
                return False  # loop already closed: nothing left to drain
            time.sleep(_CONSOLE_EVENT_HANDLER_PARK)
            return True
        except Exception:  # noqa: BLE001 - never raise into the OS callback
            return True

    handler = handler_type(_handle)
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    if not kernel32.SetConsoleCtrlHandler(handler, True):
        return lambda: None

    def remove() -> None:
        # This closure keeps ``handler`` (and its ctypes thunk) alive until
        # removal; without a live reference the thunk would be collected
        # while still registered with the OS.
        kernel32.SetConsoleCtrlHandler(handler, False)

    return remove


# --- OS boot identity ------------------------------------------------------
def os_boot_id() -> Optional[str]:
    """A stable, unique identifier of the current OS boot, or ``None``.

    Linux publishes a fresh UUID per boot; where the file is unavailable
    (Windows, macOS, BSD) callers fall back to :func:`os_boot_time`.  Used by
    the state-backed standalone ``@reboot`` dedupe: a daemon restart within
    one OS boot must not re-run a boot one-shot, while a genuine reboot must.
    """
    path = "/proc/sys/kernel/random/boot_id"
    try:
        with open(path, encoding="ascii") as fobj:
            value = fobj.read().strip()
    except (OSError, ValueError):
        return None
    return value or None


def os_boot_time() -> Optional[float]:
    """Wall-clock epoch seconds the OS booted at, or ``None`` (cannot tell).

    Derived as ``now - uptime``: on Windows from ``GetTickCount64`` (a 64-bit
    millisecond tick count that keeps running across sleep/hibernate and is
    unaffected by wall-clock steps), on POSIX from ``/proc/uptime``.  The
    derivation rides the *current* wall clock, so an NTP step shifts the
    result by the step size, which is why consumers compare boot times with
    a tolerance rather than exactly.  Where ``/proc/uptime`` is absent or
    unreadable (macOS/BSD), the boot instant comes from
    ``psutil.boot_time()`` (``kern.boottime``); the kernel adjusts that
    record when the wall clock steps, so the same tolerance applies to
    it.  ``None`` only when every source failed: the
    caller then treats every daemon start as a fresh boot, so ``@reboot``
    one-shots run on every daemon start.
    """
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            ticks = kernel32.GetTickCount64
            ticks.restype = ctypes.c_uint64
            uptime = float(ticks()) / 1000.0
        except Exception:  # noqa: BLE001 - any ctypes failure -> cannot tell
            return None
        return time.time() - uptime
    else:  # pragma: no cover (posix) - /proc/uptime
        try:
            with open("/proc/uptime", encoding="ascii") as fobj:
                uptime = float(fobj.read().split()[0])
        except (OSError, ValueError, IndexError):
            # No usable /proc/uptime (macOS/BSD): the kernel records the
            # boot instant itself and psutil (a core dependency) reads it
            # portably (sysctl kern.boottime).  Imported lazily so the
            # thin-client commands that import this module early do not
            # pay for psutil on every invocation.
            try:
                import psutil

                return float(psutil.boot_time())
            except Exception:  # noqa: BLE001 - any failure -> cannot tell
                return None
        return time.time() - uptime


# --- Process liveness -------------------------------------------------------
def pid_alive(pid: int) -> Optional[bool]:
    """Whether a process with ``pid`` currently exists, or ``None``.

    Used by in-flight run reconciliation (:mod:`cronstable.cron`) as a
    same-host safety check before declaring a previous daemon's run dead: a
    daemon crash does NOT kill the job processes it spawned, so an ``open``
    in-flight record whose recorded pid is still running must be left alone.
    PID reuse can make this report ``True`` for an unrelated process; that
    errs toward *not* reconciling, the safe direction.  ``None`` means the
    platform could not answer (treated by callers the same as dead, since
    the per-process token in the record already proved a different daemon
    wrote it).
    """
    if pid <= 0:
        return None
    if sys.platform == "win32":  # pragma: no cover (windows)
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if handle:
                # the pid may name a zombie whose handles are still open;
                # check the exit code: STILL_ACTIVE (259) means running.
                STILL_ACTIVE = 259
                code = ctypes.c_ulong()
                alive: bool | None = None
                if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                    alive = code.value == STILL_ACTIVE
                kernel32.CloseHandle(handle)
                return alive
            return False
        except Exception:  # noqa: BLE001 - any ctypes failure -> cannot tell
            return None
    else:  # pragma: no cover (posix) - signal 0 probe
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # exists, owned by someone else (should not happen for our own
            # spawned jobs, but existence is what was asked).
            return True
        except OSError:
            return None
        return True


def process_start_time(pid: int) -> Optional[float]:
    """Wall-clock epoch seconds ``pid`` started at, or ``None`` (cannot tell).

    The pid-reuse disambiguator for :func:`pid_alive` consumers that must
    not mistake a recycled pid for a recorded process: two reads that
    return the same start time name the same process. ``None`` wherever
    the platform refuses the read; callers treat that as "cannot tell"
    and fall back to bare existence.
    """
    if pid <= 0:
        return None
    try:
        import psutil

        return float(psutil.Process(pid).create_time())
    except Exception:  # noqa: BLE001 - any failure -> cannot tell
        return None


# --- Advisory exclusive file locking -------------------------------------
@contextlib.contextmanager
def exclusive_file_lock(
    fileno: int, *, blocking: bool = True
) -> Iterator[None]:
    """Hold an advisory, exclusive lock on ``fileno`` for the block.

    Used by :class:`cronstable.state.FilesystemStateBackend` to serialise the
    read-modify-write of a lease file.  The reach of the lock is a property of
    the *mount*, not this code, which is what lets one backend serve both
    deployment shapes:

    * on a **local** filesystem the lock excludes other processes on the same
      host, exactly right for single-node durability;
    * on a **shared** NFSv4 mount (an Amazon S3 Files / EFS mount) the same
      lock is honoured *across hosts*, so it excludes the fleet, exactly
      right for HA.

    On POSIX this is ``fcntl.flock`` (whole-file, advisory: it does not block
    I/O by non-cooperating processes, which is fine because cronstable owns
    both sides).  On Windows it is ``msvcrt.locking`` over the first byte;
    Windows has no cross-host story, so it only ever serialises same-host
    processes (single-node), which is all the Windows target needs.  Blocking
    (the default): a stuck holder would wait here, so callers run the whole
    locked section in a worker thread (``asyncio.to_thread``) to keep it off
    the event loop, and the section itself only rewrites a tiny file, so
    contention is brief.

    With ``blocking=False`` a contended lock raises ``OSError`` immediately
    instead of waiting (``EWOULDBLOCK``/``EAGAIN`` on POSIX, ``EACCES`` on
    Windows).  Two callers want that: the lock-fidelity probe
    (:meth:`cronstable.state.FilesystemStateBackend.verify_locking`), whose
    whole point is observing that a second lock attempt on an already-locked
    file *fails* (a mount whose locks are silent no-ops would grant it), and
    the state GC sweep's try-lock
    (:meth:`cronstable.state.FilesystemStateBackend._try_locked`), which
    skips a contended document rather than park the whole sweep behind one
    wedged holder.
    """
    if sys.platform == "win32":  # pragma: no cover (windows)
        # msvcrt.locking locks ``nbytes`` from the current file position; lock
        # the first byte (the caller guarantees the lock file has one).
        # msvcrt has no true blocking mode: LK_LOCK retries internally about
        # once a second for ~10 attempts and then raises OSError, which
        # would surface as a spurious lease failure whenever another process
        # held the lock a little long.  Emulate flock's indefinite block with
        # a non-blocking attempt loop instead; callers already run this on a
        # worker thread, so the sleep never touches the event loop.
        os.lseek(fileno, 0, os.SEEK_SET)
        while True:
            try:
                msvcrt.locking(fileno, msvcrt.LK_NBLCK, 1)
                break
            except OSError as ex:
                # retry only CONTENTION (EACCES from LK_NBLCK, EDEADLOCK
                # from the CRT); any other error (a closed/invalid fd,
                # say) must surface, not become an infinite spin.
                if ex.errno not in (errno.EACCES, errno.EDEADLOCK):
                    raise
                if not blocking:
                    raise
                time.sleep(0.05)
        try:
            yield
        finally:
            os.lseek(fileno, 0, os.SEEK_SET)
            msvcrt.locking(fileno, msvcrt.LK_UNLCK, 1)
    else:  # pragma: no cover (posix) - fcntl.flock
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(fileno, flags)
        try:
            yield
        finally:
            fcntl.flock(fileno, fcntl.LOCK_UN)


def fsync_directory(path: str) -> None:
    """Best-effort flush of a directory's own durability to disk.

    A file's own fsync only guarantees ITS bytes are durable; the directory
    ENTRY that makes the file (or a freshly created subdirectory) reachable
    from its parent is separate metadata, and needs its own flush: without
    it a power loss can drop a perfectly-fsynced file because the directory
    forgot it was ever created.  Used by
    :class:`cronstable.state.FilesystemStateBackend` after an atomic rename, a
    document delete, and when a stream/namespace/blob-shard directory is
    freshly created.

    POSIX opens the directory like any other file handle and fsyncs it. The
    ``os`` module has no equivalent for Windows, so this reaches for the
    underlying Win32 calls via ctypes: ``CreateFileW`` with
    ``FILE_FLAG_BACKUP_SEMANTICS`` to obtain a directory handle at all
    (``GENERIC_WRITE`` access: a directory handle opened read-only is
    accepted but ``FlushFileBuffers`` on it fails with ACCESS_DENIED), then
    ``FlushFileBuffers`` on it.  Best-effort either way: any failure (a
    filesystem that does not support it, a permissions quirk, a path that
    vanished) is swallowed, because the data this guards is still correct
    without it, just not crash-durable for this one write.
    """
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        try:
            import ctypes
            from ctypes import wintypes

            GENERIC_WRITE = 0x40000000
            FILE_SHARE_READ = 0x00000001
            FILE_SHARE_WRITE = 0x00000002
            FILE_SHARE_DELETE = 0x00000004
            OPEN_EXISTING = 3
            FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
            INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            kernel32.CreateFileW.restype = wintypes.HANDLE
            kernel32.CreateFileW.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.LPVOID,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ]
            kernel32.FlushFileBuffers.restype = wintypes.BOOL
            kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

            handle = kernel32.CreateFileW(
                path,
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
            if handle in (0, INVALID_HANDLE_VALUE):
                return
            try:
                kernel32.FlushFileBuffers(handle)
            finally:
                kernel32.CloseHandle(handle)
        except Exception:  # noqa: BLE001 - best-effort; never raise
            return
        return
    else:  # pragma: no cover (posix) - fsync on a directory fd
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)


# --- Config-directory permissions (Windows) --------------------------------
#: The DACL :func:`harden_config_dir` writes: full control for LocalSystem
#: and the local Administrators group, read and execute for everyone else,
#: all of it inherited by whatever is created inside.
#:
#: ``D:P`` is the load-bearing part.  It PROTECTS the DACL, which severs
#: inheritance from the parent, and inheritance is the entire problem:
#: ``%ProgramData%`` grants ``BUILTIN\\Users`` the ``DCLCRPCR`` set (create
#: file, create folder, write EA, write attributes) with container
#: inheritance, so a directory made under it with a plain ``mkdir`` hands
#: every local account the right to plant a job there.  Measured without
#: ``P``: the new ACEs are added, the inherited ones stay, and the call
#: still reports success, so the hardening reads as done and is a no-op.
#:
#: Read is deliberately kept.  Removing it closes nothing that matters
#: (the trust boundary this defends is who may WRITE a job) and breaks two
#: things measurably: an unelevated caller, which a split-token
#: administrator is, cannot afterwards list the directory it just created,
#: and :func:`_holds_config` turns that ``OSError`` into "no config here",
#: so the machine-wide path silently stops resolving for exactly the
#: accounts that are not elevated.  ``C:\\ProgramData\\ssh`` makes the same
#: trade.  An operator keeping secrets inline can tighten it further, and
#: the documentation says so.
#:
#: ``FA``/``FRFX`` rather than ``GA``/``GRGX``: the generic forms are not
#: mapped on this path and read back as six split ACEs carrying unmapped
#: generic bits, which Windows' own tools cannot render as a permission.
_CONFIG_DIR_SDDL = "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FRFX;;;AU)"


def config_dir_icacls_recipe(path: str) -> str:
    """The paste-able icacls line for :data:`_CONFIG_DIR_SDDL`.

    Kept beside the SDDL so the printed advice cannot drift from what
    :func:`harden_config_dir` applies.  ``RX`` is the icacls spelling of
    ``FRFX``; the AU grant stays for the reason given above.
    """
    return (
        'icacls "{}" /inheritance:r /grant *S-1-5-18:(OI)(CI)F '
        "/grant *S-1-5-32-544:(OI)(CI)F "
        "/grant *S-1-5-11:(OI)(CI)RX".format(path)
    )


#: SDDL aliases for "any account on this machine", the only principals a
#: write grant to is reported as a finding.  CREATOR OWNER (``CO``) is
#: deliberately absent: it appears on ``%ProgramData%`` itself and resolves
#: per object to whoever made it, so counting it would flag every directory
#: on the system.  A grant to one named non-admin account is also not
#: reported, because nothing here can tell that apart from a deliberate
#: delegation, and a check that cries wolf on a correct machine at every
#: start is worse than no check.
_ANY_USER_SIDS = {
    "WD": "Everyone",
    "BU": "BUILTIN\\Users",
    "AU": "Authenticated Users",
    "IU": "INTERACTIVE",
    "BG": "BUILTIN\\Guests",
    "AN": "Anonymous Logon",
    "DU": "Domain Users",
    "S-1-1-0": "Everyone",
    "S-1-5-32-545": "BUILTIN\\Users",
    "S-1-5-11": "Authenticated Users",
    "S-1-5-4": "INTERACTIVE",
}

#: SDDL rights tokens that let the holder create a file in a directory.
#: ``DC`` is FILE_ADD_FILE and ``LC`` is FILE_ADD_SUBDIRECTORY, which is
#: how the ``%ProgramData%`` ACE spells itself; the rest are the coarser
#: aliases that contain them.
_WRITE_TOKENS = {"FA", "FW", "GA", "GW", "KA", "KW", "DC", "LC"}

#: The same rights as a numeric mask: FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY,
#: GENERIC_WRITE and GENERIC_ALL.  A DACL read back from disk gives either
#: form, sometimes both in one ACL (measured on C:\\Windows\\System32).
_WRITE_MASK = 0x00000002 | 0x00000004 | 0x40000000 | 0x10000000


def _sddl_write_grantee(sddl: str) -> Optional[str]:
    """The first any-user principal an SDDL string lets create files.

    Parses the ACE list rather than walking ACEs through ``GetAce``: the
    text form is what ``ConvertSecurityDescriptorToStringSecurityDescriptorW``
    already produces, and reading it needs no struct layouts, no SID
    comparisons and no ``MapGenericMask``.

    Deny ACEs are honoured, coarsely: a principal denied any of the same
    rights is skipped entirely rather than intersected properly.  That errs
    toward silence, which is the right direction for a warning that fires
    at every start.
    """
    aces = re.findall(r"\(([^)]*)\)", sddl)
    denied = set()
    for ace in aces:
        parts = ace.split(";")
        if len(parts) < 6 or not parts[0].startswith("D"):
            continue
        if _sddl_rights_write(parts[2]):
            denied.add(parts[5])
    for ace in aces:
        parts = ace.split(";")
        if len(parts) < 6 or parts[0] != "A":
            continue
        who = parts[5]
        if who in denied or who not in _ANY_USER_SIDS:
            continue
        if _sddl_rights_write(parts[2]):
            return _ANY_USER_SIDS[who]
    return None


def _sddl_rights_write(rights: str) -> bool:
    """Whether one ACE's rights field grants create-a-file."""
    rights = rights.strip()
    if rights[:2].lower() == "0x":
        try:
            return bool(int(rights, 16) & _WRITE_MASK)
        except ValueError:
            return False
    tokens = {rights[i : i + 2] for i in range(0, len(rights) - 1, 2)}
    return bool(tokens & _WRITE_TOKENS)


def any_user_write_grantee(path: str) -> Optional[str]:
    """Who, other than an administrator, may write to ``path``.

    Returns the principal's familiar name, or ``None`` when nobody can and
    on every non-Windows host.  Reads one DACL and asks one question of
    it, so it is cheap enough to run at startup, and it answers for
    directories created long before anything hardened them, which is the
    case that matters: the hole is inherited rather than written.

    Correct for a configuration FILE as well as a directory, and not by
    accident: FILE_ADD_FILE and FILE_WRITE_DATA are the same bit, as are
    FILE_ADD_SUBDIRECTORY and FILE_APPEND_DATA, so one mask answers "can
    add a job here" for a directory and "can rewrite this" for a file.

    Never raises.  A path on a filesystem with no ACLs, a disconnected
    share and a revoked READ_CONTROL all mean "cannot tell", and cannot
    tell has to read as no finding.
    """
    if not IS_WINDOWS:  # pragma: no cover (posix) - no ACLs to read
        return None
    sddl = _read_dacl_sddl(path)  # pragma: no cover (windows)
    return _sddl_write_grantee(sddl) if sddl else None


def _read_dacl_sddl(path: str) -> Optional[str]:
    """One directory's DACL as SDDL text, or ``None`` if it cannot be read."""
    if not IS_WINDOWS:  # pragma: no cover (posix) - Windows-only path
        return None
    try:  # pragma: no cover (windows) - measured live, not in the suite
        import ctypes
        from ctypes import wintypes

        SE_FILE_OBJECT = 1
        DACL_SECURITY_INFORMATION = 0x00000004
        SDDL_REVISION_1 = 1

        advapi32 = ctypes.windll.advapi32  # type: ignore[attr-defined]
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        advapi32.GetNamedSecurityInfoW.restype = wintypes.DWORD
        advapi32.GetNamedSecurityInfoW.argtypes = [
            wintypes.LPCWSTR,
            ctypes.c_int,
            wintypes.DWORD,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        convert = advapi32.ConvertSecurityDescriptorToStringSecurityDescriptorW
        convert.restype = wintypes.BOOL
        convert.argtypes = [
            ctypes.c_void_p,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.LPWSTR),
            ctypes.POINTER(wintypes.ULONG),
        ]
        kernel32.LocalFree.restype = wintypes.HLOCAL
        kernel32.LocalFree.argtypes = [wintypes.HLOCAL]

        dacl = ctypes.c_void_p()
        descriptor = ctypes.c_void_p()
        if advapi32.GetNamedSecurityInfoW(
            path,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            None,
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(descriptor),
        ):
            return None
        try:
            text = wintypes.LPWSTR()
            length = wintypes.ULONG()
            if not convert(
                descriptor,
                SDDL_REVISION_1,
                DACL_SECURITY_INFORMATION,
                ctypes.byref(text),
                ctypes.byref(length),
            ):
                return None
            try:
                return text.value
            finally:
                kernel32.LocalFree(text)
        finally:
            # GetNamedSecurityInfoW allocates ONE buffer holding the whole
            # descriptor; `dacl` points inside it and must not be freed.
            kernel32.LocalFree(descriptor)
    except Exception:  # noqa: BLE001 - cannot tell is not a finding
        return None


def harden_config_dir(path: str) -> bool:
    """Give ``path`` a DACL only administrators and LocalSystem can write.

    True when the directory now carries it, False when it could not be
    applied and on every non-Windows host, where the mode the directory
    was created with already answers the question.

    Best-effort by contract: the caller has already written a working
    configuration, so a filesystem with no ACL support or a caller without
    WRITE_DAC is a warning and not a failure.
    """
    if not IS_WINDOWS:  # pragma: no cover (posix) - POSIX modes, not DACLs
        return False
    try:  # pragma: no cover (windows) - measured live, not in the suite
        import ctypes
        from ctypes import wintypes

        SE_FILE_OBJECT = 1
        DACL_SECURITY_INFORMATION = 0x00000004
        PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
        SDDL_REVISION_1 = 1

        advapi32 = ctypes.windll.advapi32  # type: ignore[attr-defined]
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        parse = advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW
        parse.restype = wintypes.BOOL
        parse.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(wintypes.ULONG),
        ]
        advapi32.GetSecurityDescriptorDacl.restype = wintypes.BOOL
        advapi32.GetSecurityDescriptorDacl.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(wintypes.BOOL),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(wintypes.BOOL),
        ]
        advapi32.SetNamedSecurityInfoW.restype = wintypes.DWORD
        advapi32.SetNamedSecurityInfoW.argtypes = [
            wintypes.LPWSTR,
            ctypes.c_int,
            wintypes.DWORD,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        kernel32.LocalFree.restype = wintypes.HLOCAL
        kernel32.LocalFree.argtypes = [wintypes.HLOCAL]

        descriptor = ctypes.c_void_p()
        size = wintypes.ULONG()
        if not parse(
            _CONFIG_DIR_SDDL,
            SDDL_REVISION_1,
            ctypes.byref(descriptor),
            ctypes.byref(size),
        ):
            return False
        try:
            present = wintypes.BOOL()
            dacl = ctypes.c_void_p()
            defaulted = wintypes.BOOL()
            if not advapi32.GetSecurityDescriptorDacl(
                descriptor,
                ctypes.byref(present),
                ctypes.byref(dacl),
                ctypes.byref(defaulted),
            ):
                return False
            if not present.value or not dacl:
                # An ABSENT DACL is not an empty one.  SetNamedSecurityInfoW
                # given NULL here writes a null DACL, which grants every
                # account full control, and returns 0 for it: the exact
                # inverse of this function, reported as success.  Only a
                # malformed _CONFIG_DIR_SDDL reaches this, which is why it
                # is a refusal rather than a fallback: the converter accepts
                # the empty string and answers DaclPresent=False for it.
                return False
            # DWORD, not BOOL: this returns a Win32 error code and does not
            # touch the thread's last error, so ERROR_ACCESS_DENIED (5)
            # would read as truthy success under the ctypes default.
            return not advapi32.SetNamedSecurityInfoW(
                path,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
                None,
                None,
                dacl,
                None,
            )
        finally:
            kernel32.LocalFree(descriptor)
    except Exception:  # noqa: BLE001 - the configuration is written already
        return False


# --- Windows Event Log ----------------------------------------------------
#: ``ReportEventW``'s ``wType``, the severity that Event Viewer's Level
#: column, a Windows Event Forwarding subscription and every SIEM connector
#: all read.  Three of the six documented values: the two audit types mean
#: something only in the Security log, which needs a registered source and a
#: privilege the daemon does not hold, and ``EVENTLOG_SUCCESS`` (0) is shown
#: as "Information" by one viewer and "LogAlways" by another, so a success
#: is written as information and 0 is never emitted.
EVENTLOG_ERROR_TYPE = 0x0001
EVENTLOG_WARNING_TYPE = 0x0002
EVENTLOG_INFORMATION_TYPE = 0x0004

#: ``ERROR_INVALID_HANDLE``.  Named because it is the one write failure a
#: caller can repair: the EventLog service restarting invalidates a source
#: handle opened before it, and registering the source again is the fix.
EVENTLOG_ERROR_INVALID_HANDLE = 6

#: What :func:`write_event_log` reports where there is no Event Log at all.
#: Nonzero, because the contract is that 0 means written, and 0x1FFFF, which
#: sits above every documented Win32 system error code, so it can never
#: collide with a real one a caller later comes to special-case.
EVENTLOG_ERROR_UNSUPPORTED = 0x1FFFF

#: How many wide characters one ``ReportEventW`` call may carry across ALL
#: of its insertion strings together.
#:
#: Measured on Windows 11 rather than taken from the documentation, because
#: the real constraint is a shared budget and not a per-string length: the
#: call fails with ``ERROR_INVALID_PARAMETER`` unless ``sum(len(s) + 1)``
#: over the strings is at most 32,732.  One string of 32,731 is accepted and
#: 32,732 refused; two of 16,365 accepted and 16,366 refused; four of 8,182
#: and eight of 4,090 land on that same total, which is what shows the
#: ``+ 1`` is each string's own terminator rather than a per-string cap.
#:
#: The number here is deliberately well inside that, because an over-long
#: vector is not a soft failure: ReportEventW writes no record at all, so
#: the alert most worth having (the run that produced the most output) is
#: exactly the one that would go missing.  Callers size their own field
#: ceilings against this; nothing trims to it at write time.
EVENTLOG_MAX_TOTAL_CHARS = 30000

#: advapi32 with the three Event Log prototypes declared, built once.
#:
#: Cached because :func:`write_event_log` runs once per reported run on a
#: long-lived daemon, and ``ctypes.WinDLL`` builds a fresh wrapper object
#: (and re-declares every prototype) each time it is called.  A module
#: global rather than an ``lru_cache`` so the whole thing stays inside the
#: Windows-only clause it belongs in; ``None`` means "not built yet", never
#: "could not be built", since a failure to build is not cached.
_EVENT_LOG_API: Any = None


def _event_log_api() -> Any:  # pragma: no cover (windows) - Windows-only
    """Build (or return) advapi32 with the Event Log prototypes declared.

    ``ctypes.WinDLL("advapi32", use_last_error=True)`` rather than
    ``ctypes.windll.advapi32``, which is the shape the rest of this module
    uses for kernel32.  The difference matters here and nowhere else in the
    package: ``windll`` caches library wrappers built with
    ``use_last_error=False``, which do not snapshot the calling thread's
    last-error value around the foreign call, so a later unrelated call can
    overwrite the code before it is read.  :func:`write_event_log` acts on
    exactly one code (:data:`EVENTLOG_ERROR_INVALID_HANDLE` means close,
    re-open and retry), so it has to read the snapshotted copy.

    Declaring ``restype`` on ``RegisterEventSourceW`` is load-bearing, not
    tidiness: ctypes defaults every foreign function to ``c_int``, which
    truncates a 64-bit handle on Win64 to something ``ReportEventW`` then
    rejects.  :func:`fsync_directory` guards the identical hazard on
    ``CreateFileW`` above; this is the second site in the file.

    The arguments that are always the same, and why:

    * ``lpUNCServerName`` is NULL (this machine).  Logging to another host
      needs a share, credentials and a firewall path, none of which an
      unattended daemon can be expected to have.
    * ``lpUserSid`` is NULL, typed ``LPVOID``.  Attaching the daemon's SID
      costs an ``OpenProcessToken`` plus a ``GetTokenInformation`` for a
      field nothing consumes; Event Viewer's User column reads N/A.
    * ``dwDataSize`` is 0 and ``lpRawData`` NULL.  Binary event data is
      opaque in Event Viewer and meaningless to a SIEM connector, so
      everything cronstable writes rides the insertion strings.
    """
    global _EVENT_LOG_API
    if _EVENT_LOG_API is not None:
        return _EVENT_LOG_API
    import ctypes
    from ctypes import wintypes

    advapi32 = ctypes.WinDLL(  # type: ignore[attr-defined]
        "advapi32", use_last_error=True
    )
    advapi32.RegisterEventSourceW.restype = wintypes.HANDLE
    advapi32.RegisterEventSourceW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
    ]
    advapi32.ReportEventW.restype = wintypes.BOOL
    advapi32.ReportEventW.argtypes = [
        wintypes.HANDLE,
        wintypes.WORD,
        wintypes.WORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.WORD,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.LPCWSTR),
        wintypes.LPVOID,
    ]
    advapi32.DeregisterEventSource.restype = wintypes.BOOL
    advapi32.DeregisterEventSource.argtypes = [wintypes.HANDLE]
    _EVENT_LOG_API = advapi32
    return advapi32


def open_event_log(source: str) -> Optional[int]:
    """Register ``source`` with the Event Log and return its handle.

    ``None`` where there is no Event Log to open, or the service refused.
    A source that was never registered under
    ``HKLM\\SYSTEM\\CurrentControlSet\\Services\\EventLog`` still opens and
    still writes; its records land in the Application log and render with
    the generic "The description for Event ID ... cannot be found" preamble,
    which costs the rendered prose and nothing else (the provider name, the
    event id, the level and the insertion strings are all still there for
    the XML view, a forwarder and a SIEM connector).  Registering would
    need HKLM writes, so it is the operator's decision, not the daemon's.

    Blocking, and that is the reason this is a separate call rather than
    something :func:`write_event_log` does lazily: ``RegisterEventSourceW``
    is an RPC to the EventLog service, not a local call, so it must be
    reached from the same worker thread as the writes and never from the
    event loop.
    """
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        try:
            advapi32 = _event_log_api()
            handle: Optional[int] = advapi32.RegisterEventSourceW(None, source)
        except Exception:  # noqa: BLE001 - any ctypes failure -> no log
            return None
        return int(handle) if handle else None
    else:  # pragma: no cover (posix) - there is no event log here
        return None


def write_event_log(
    handle: int,
    *,
    event_type: int,
    category: int,
    event_id: int,
    strings: list[str],
) -> int:
    """Write one Event Log record.  Returns 0 written, else an error code.

    An error code rather than a bool because exactly one code is
    actionable: :data:`EVENTLOG_ERROR_INVALID_HANDLE` tells the caller to
    re-open the source and retry the record once.  Everything else is worth
    logging and dropping.

    ``strings`` must not contain ``None``.  A NULL in the ``LPCWSTR`` array
    renders as nothing at that position, which silently shifts every later
    field an operator (or a parser reading ``EventData/Data`` by index)
    reads; an empty field is ``""``.  The caller is also responsible for
    keeping the vector inside :data:`EVENTLOG_MAX_TOTAL_CHARS`, since
    overshooting it writes no record at all rather than a truncated one.
    """
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        try:
            import ctypes
            from ctypes import wintypes

            advapi32 = _event_log_api()
            # Held in a local across the call: ctypes pins the converted
            # wide buffers on the array's own _objects, so letting the
            # array go first would free them under the callee.
            array = (wintypes.LPCWSTR * len(strings))(*strings)
            written: int = advapi32.ReportEventW(
                handle,
                event_type,
                category,
                event_id,
                None,
                len(strings),
                0,
                array,
                None,
            )
            if written:
                return 0
            code: int = ctypes.get_last_error()  # type: ignore[attr-defined]
        # Broader than it looks, and deliberately so.  A handle this
        # function never opened does not come back as an error code: passing
        # a made-up integer faults inside advapi32 and ctypes reports the
        # access violation as OSError (measured).  A handle that WAS opened
        # and has since been closed or invalidated, which is the case that
        # actually happens when the EventLog service restarts, returns
        # ERROR_INVALID_HANDLE properly (also measured).  So the first shape
        # has to be swallowed here or it would escape into the writer thread
        # and take it down, and the second stays an actionable code below.
        except Exception:  # noqa: BLE001 - see above
            return EVENTLOG_ERROR_UNSUPPORTED
        # `or` guards the pathological "returned FALSE but last error is 0"
        # case, so 0 never comes to mean written when nothing was.
        return int(code) or EVENTLOG_ERROR_UNSUPPORTED
    else:  # pragma: no cover (posix) - there is no event log here
        return EVENTLOG_ERROR_UNSUPPORTED


def close_event_log(handle: int) -> None:
    """Release a source handle.  Never raises, on either platform."""
    if IS_WINDOWS:  # pragma: no cover (windows) - Windows-only path
        # Best effort on purpose: this is reached while shutting down or
        # after the EventLog service went away, and in either case the
        # handle dies with the process anyway.
        try:
            _event_log_api().DeregisterEventSource(handle)
        except Exception:  # noqa: BLE001 - see above
            return
    else:  # pragma: no cover (posix) - there is no event log here
        return
