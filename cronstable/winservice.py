"""Run cronstable as a Windows service, over ctypes rather than pywin32.

"Run whether user is logged on or not" is the first checkbox on the Task
Scheduler General tab, and until this module existed cronstable's documented
lifecycle on Windows was a console window stopped with Ctrl-C.  ``cronstable
service install`` registers the program with the Service Control Manager,
and ``cronstable service run`` is what the SCM then invokes.

Three implementations were possible and this is the third:

* pywin32's ``win32serviceutil`` would add a Windows-only runtime
  dependency, PyInstaller hiddenimports, and a win-arm64 wheel question on
  an architecture where ``requirements_dev.txt`` already has to exclude
  three packages;
* bundling WinSW ships a second binary, needs licensing and notice work
  like the existing zeroconf kit, and its stop path would go through
  ``POST /shutdown`` rather than a real control handler;
* ctypes costs more code and no dependency, and it is the shape
  :mod:`cronstable.platform` already uses for its advapi32 and kernel32
  work, so a service host is a new module speaking Win32 rather than a new
  entry in the wheel.

The module is split so that almost all of it is testable on any OS.  Every
decision (what to put in the ImagePath, which controls to accept in which
state, which of the seven SERVICE_STATUS fields to report, how a Win32 error
reads as a sentence) is a pure function; every OS call lives behind
:class:`WinApi`, which the tests replace with a recording double.  Only
``WinApi``'s method bodies are Windows-only, and they are tagged so the
coverage profiles measure them on the Windows rows.

What this module deliberately does not do is described in
``wiki/Windows-Service.md``: it installs as LocalSystem only (per-account
identity is its own piece of work), and it refuses to install from a
one-file frozen binary, which cannot host a service at all because the
PyInstaller bootloader runs the application in a child process the SCM
never sees.  The published ``cronstable-windows-<arch>.zip`` and ``.msi``
are one-directory builds and host a service normally; the one-file ``.exe``
(also what winget installs) is the shape ``install`` refuses.  The MSI
registers the service itself with the same settings ``install`` writes,
fenced by ``tests/test_msi_parity.py``.
"""

from __future__ import annotations

import logging
import ntpath
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, Optional

from cronstable import platform

# The service name the parser defaults to, imported from the stdlib-only CLI
# leaf so this module's idea of it and the parser's cannot drift. The leaf
# costs nothing to import: it is what every invocation already builds the
# parser from.
from cronstable._cliargs import SERVICE_NAME_DEFAULT

logger = logging.getLogger("cronstable")


# --- Win32 constants -------------------------------------------------------
# Spelled as literals, like cronstable.platform's creation flags and priority
# classes, so this module imports and type-checks on POSIX where the Windows
# headers do not exist.

SERVICE_WIN32_OWN_PROCESS = 0x00000010

SERVICE_STOPPED = 1
SERVICE_START_PENDING = 2
SERVICE_STOP_PENDING = 3
SERVICE_RUNNING = 4

SERVICE_ACCEPT_STOP = 0x0001
SERVICE_ACCEPT_SHUTDOWN = 0x0004
SERVICE_ACCEPT_PARAMCHANGE = 0x0008
SERVICE_ACCEPT_PRESHUTDOWN = 0x0100

SERVICE_CONTROL_STOP = 1
SERVICE_CONTROL_INTERROGATE = 4
SERVICE_CONTROL_SHUTDOWN = 5
SERVICE_CONTROL_PARAMCHANGE = 6
SERVICE_CONTROL_PRESHUTDOWN = 0x0F

SERVICE_AUTO_START = 2
SERVICE_DEMAND_START = 3
SERVICE_ERROR_NORMAL = 1

SC_MANAGER_CONNECT = 0x0001
SC_MANAGER_CREATE_SERVICE = 0x0002
SERVICE_QUERY_STATUS = 0x0004
SERVICE_CHANGE_CONFIG = 0x0002
SERVICE_START = 0x0010
SERVICE_STOP = 0x0020
SERVICE_PAUSE_CONTINUE = 0x0040
DELETE = 0x00010000

SERVICE_CONFIG_DESCRIPTION = 1
SERVICE_CONFIG_FAILURE_ACTIONS = 2
SERVICE_CONFIG_DELAYED_AUTO_START_INFO = 3
SERVICE_CONFIG_FAILURE_ACTIONS_FLAG = 4

SC_ACTION_NONE = 0
SC_ACTION_RESTART = 1
SC_STATUS_PROCESS_INFO = 0

NO_ERROR = 0
ERROR_ACCESS_DENIED = 5
ERROR_CALL_NOT_IMPLEMENTED = 120
ERROR_SERVICE_SPECIFIC_ERROR = 1066
ERROR_INVALID_SERVICE_CONTROL = 1052
ERROR_SERVICE_ALREADY_RUNNING = 1056
ERROR_SERVICE_CANNOT_ACCEPT_CTRL = 1061
ERROR_SERVICE_DOES_NOT_EXIST = 1060
ERROR_SERVICE_NOT_ACTIVE = 1062
ERROR_FAILED_SERVICE_CONTROLLER_CONNECT = 1063
ERROR_SERVICE_MARKED_FOR_DELETE = 1072
ERROR_SERVICE_EXISTS = 1073
ERROR_DUPLICATE_SERVICE_NAME = 1078

STD_INPUT_HANDLE = -10
STD_OUTPUT_HANDLE = -11
STD_ERROR_HANDLE = -12

CTRL_C_EVENT = 0
CTRL_BREAK_EVENT = 1
CTRL_CLOSE_EVENT = 2
CTRL_SHUTDOWN_EVENT = 6


#: What the Services console shows in its Description column.
SERVICE_DESCRIPTION = (
    "Runs the cronstable job scheduler, so scheduled jobs run whether or "
    "not a user is logged on."
)

#: The one-directory release assets the one-file refusal in ``install``
#: points at.  Owned by release.yml's attach list;
#: tests/test_ci_fences.py holds the two spellings equal.
ONEDIR_RELEASE_ASSETS = (
    "cronstable-windows-amd64.zip",
    "cronstable-windows-arm64.zip",
    "cronstable-windows-i686.zip",
)

#: How often the pending-state pumper reports progress, and the wait hint it
#: reports.  The SCM's rule is not "start within 30 seconds": it is that
#: while a state is pending, dwCheckPoint must advance before dwWaitHint
#: elapses.  A pumper removes the guess entirely, so a scheduler whose
#: config parse or whose job drain takes minutes is never declared hung.
_STATUS_PUMP_INTERVAL = 2.0
_STATUS_WAIT_HINT_MS = 10_000

#: Bootstrap log rotation.  Matches the recipe the Windows documentation
#: already recommends for the `logging:` section.
_LOG_BYTES = 10 * 1024 * 1024
_LOG_BACKUPS = 5

#: Service-specific exit codes, reported alongside ERROR_SERVICE_SPECIFIC_
#: ERROR so `sc query` distinguishes the three ways a start can fail from
#: each other and from a clean stop.
EXIT_RUN_FAILED = 1
EXIT_CONFIG_FAILED = 2
EXIT_LOG_FAILED = 3


class ServiceError(Exception):
    """A Win32 call failed, carrying the code so the caller can explain it."""

    def __init__(self, message: str, *, winerror: int = 0) -> None:
        super().__init__(message)
        self.winerror = winerror


# --- Pure decisions --------------------------------------------------------
# Everything below is ordinary Python with no OS call in it, so both arms of
# every decision are measured on both coverage profiles.


def display_name(name: str) -> str:
    """What the Services console lists the service as."""
    if name == SERVICE_NAME_DEFAULT:
        return "cronstable scheduler"
    return "cronstable scheduler ({})".format(name)


def frozen_layout(executable: str, meipass: Optional[str]) -> str:
    """Which packaging shape this process is: source, onedir or onefile.

    It decides whether ``install`` can succeed at all, so it is a three-way
    answer rather than a bool: the refusal must not fire on a shape that
    works, and the same value picks the sentence explaining the one that
    does not.

    A one-file PyInstaller build extracts itself to a temporary directory
    and runs the application in a CHILD process.  The SCM starts and watches
    the parent bootloader, which never calls StartServiceCtrlDispatcherW, so
    the start fails on the SCM's timeout while the child's own dispatcher
    call is refused with error 1063.  A one-dir build has no such child, and
    a source install runs the interpreter directly, so both host a service
    normally.  The test is where ``_MEIPASS`` points: inside the executable's
    own directory for one-dir, and off in the temporary directory for
    one-file.
    """
    if not meipass:
        return "source"
    # ntpath, not os.path: these are Windows paths by definition (the whole
    # subcommand refuses to run anywhere else), and os.path on a POSIX box
    # reads "C:\app\cronstable.exe" as one long filename with no
    # directory, which quietly turns every layout into onedir. Being
    # explicit also lets the decision be tested on any machine.
    exe_dir = ntpath.normcase(ntpath.dirname(ntpath.normpath(executable)))
    bundle = ntpath.normcase(ntpath.normpath(meipass))
    if bundle == exe_dir or bundle.startswith(exe_dir + ntpath.sep):
        return "onedir"
    return "onefile"


def host_argv(
    *,
    config: str,
    name: str,
    log_file: Optional[str],
    no_log_file: bool,
    console: bool,
    log_level: str,
    executable: str,
    frozen: bool,
) -> list[str]:
    """The argv the SCM should launch, as a list.

    ``-l`` comes BEFORE the ``service`` token because it is a root flag, and
    argparse only accepts root flags ahead of the subcommand.  Getting that
    wrong leaves every installed service pinned at INFO, with a hand edit of
    the registry as the only way to raise it, in a feature whose bootstrap
    log is the operator's sole diagnostic.

    ``executable`` is ``sys.executable``, which is the frozen program under
    PyInstaller and the interpreter otherwise.  A source install is spelled
    ``python -m cronstable`` rather than the ``Scripts\\cronstable.exe``
    console-script shim: that shim launches the interpreter as a child
    process and waits, which reproduces the one-file problem exactly.

    ``config`` is always baked in absolute, never left to the platform
    default, because that default is resolved from the *running* process's
    environment and a service account's ``%APPDATA%`` points into an
    invisible ``systemprofile`` directory nobody edits.
    """
    argv = [executable]
    if not frozen:
        argv += ["-m", "cronstable"]
    argv += ["-l", log_level, "service", "run", "--name", name]
    argv += ["-c", os.path.abspath(config)]
    if no_log_file:
        argv.append("--no-log-file")
    elif log_file:
        argv += ["--log-file", os.path.abspath(log_file)]
    if console:
        argv.append("--console")
    return argv


def image_path(argv: Sequence[str]) -> str:
    """``argv`` as the single command-line string the SCM stores.

    ``subprocess.list2cmdline``, not a hand-rolled join: it emits the exact
    CommandLineToArgvW form CPython itself re-parses, and it quotes any
    argument containing a space.  That quoting is what closes the unquoted
    service path hole, where ``C:\\Program Files\\...`` lets an unprivileged
    ``C:\\Program.exe`` be started as LocalSystem instead.
    """
    return subprocess.list2cmdline(list(argv))


def control_action(control: int) -> str:
    """What an SCM control code means to this host.

    ``stop`` for STOP, SHUTDOWN and PRESHUTDOWN, ``reload`` for
    PARAMCHANGE, ``report`` for INTERROGATE, ``unhandled`` otherwise.

    Accepting PRESHUTDOWN is the reason the handler is registered in its
    extended form.  A plain shutdown control gives a service only
    ``WaitToKillServiceTimeout``, five seconds by default, while preshutdown
    grants the much longer preshutdown timeout.  A scheduler that drains
    running jobs on the way out wants the second one.

    PARAMCHANGE is the SCM's "your parameters changed" notification, and
    this host reads it as the reload verb SIGHUP is on POSIX: the same
    forced reparse, reachable as ``cronstable service reload`` or ``sc
    control <name> paramchange``.  Without it, Windows would have no way
    to demand a reload the stat fingerprint cannot justify (a credential
    file rewritten in place).
    """
    if control in (
        SERVICE_CONTROL_STOP,
        SERVICE_CONTROL_SHUTDOWN,
        SERVICE_CONTROL_PRESHUTDOWN,
    ):
        return "stop"
    if control == SERVICE_CONTROL_PARAMCHANGE:
        return "reload"
    if control == SERVICE_CONTROL_INTERROGATE:
        return "report"
    return "unhandled"


def accepted_controls(state: int) -> int:
    """The dwControlsAccepted mask to report in ``state``.

    Zero while pending, which is not a detail.  It is what makes the SCM
    refuse a stop request during startup with "the service cannot accept
    control messages at this time", and that removes the whole class of race
    where a stop arrives before the event loop it would signal exists.
    Accepting STOP while pending looks harmless and creates it.  The same
    gate covers PARAMCHANGE: advertised only here, a reload can never
    arrive before the loop it would wake exists.
    """
    if state == SERVICE_RUNNING:
        return (
            SERVICE_ACCEPT_STOP
            | SERVICE_ACCEPT_SHUTDOWN
            | SERVICE_ACCEPT_PARAMCHANGE
            | SERVICE_ACCEPT_PRESHUTDOWN
        )
    return 0


def status_fields(
    state: int,
    *,
    checkpoint: int = 0,
    wait_hint_ms: int = 0,
    win32_exit: int = 0,
    specific_exit: int = 0,
) -> tuple[int, int, int, int, int, int, int]:
    """The seven SERVICE_STATUS DWORDs, in the struct's own order.

    ``dwServiceType`` is always SERVICE_WIN32_OWN_PROCESS; a report carrying
    the wrong type is ignored by the SCM, which looks exactly like a service
    that never reported.  The checkpoint is forced to zero in the two
    settled states, because a nonzero checkpoint there means "still working"
    and would keep the SCM waiting on a service that has arrived.
    """
    if state in (SERVICE_RUNNING, SERVICE_STOPPED):
        checkpoint = 0
        wait_hint_ms = 0
    return (
        SERVICE_WIN32_OWN_PROCESS,
        state,
        accepted_controls(state),
        win32_exit,
        specific_exit,
        checkpoint,
        wait_hint_ms,
    )


def start_type_code(name: str) -> tuple[int, bool]:
    """``--start-type`` as its SCM code plus the delayed-start flag.

    Two values because delayed auto-start is not a start type: it is
    SERVICE_AUTO_START plus a separate configuration call.
    """
    if name == "demand":
        return SERVICE_DEMAND_START, False
    return SERVICE_AUTO_START, name == "delayed"


def failure_actions_plan(
    restart_delay_s: float,
) -> tuple[list[tuple[int, int]], int]:
    """Recovery actions: restart twice, then give up, resetting daily.

    The two units in the return value are the point of this function.  An
    SC_ACTION's delay is in MILLISECONDS while the failure-action reset
    period is in SECONDS, so passing a caller's ``--restart-delay 60``
    straight through as the action delay would produce a 60 millisecond
    restart, which is a crash loop wearing a one minute label.
    """
    delay_ms = max(0, int(restart_delay_s * 1000))
    actions = [
        (SC_ACTION_RESTART, delay_ms),
        (SC_ACTION_RESTART, delay_ms),
        (SC_ACTION_NONE, 0),
    ]
    return actions, 86_400


def bootstrap_log_path(
    config: str,
    log_file: Optional[str],
    *,
    config_is_dir: bool,
    program_data: Optional[str],
) -> str:
    """Where ``service run`` writes before the configuration is parsed.

    A named ``--log-file`` wins.  Otherwise the log goes in a ``logs``
    directory beside the configuration, which is the place an operator
    already knows about, falling back to ``%ProgramData%\\cronstable\\logs``
    when there is no usable configuration path to sit beside.

    ``config_is_dir`` is a parameter rather than an ``os.path.isdir`` call so
    the whole function stays pure and both branches are measured on every
    OS.
    """
    if log_file:
        return os.path.abspath(log_file)
    if config:
        base = config if config_is_dir else os.path.dirname(config)
        if base:
            return os.path.join(os.path.abspath(base), "logs", _LOG_NAME)
    root = program_data or "C:\\ProgramData"
    return os.path.join(root, "cronstable", "logs", _LOG_NAME)


#: The bootstrap log's file name.  Named rather than inlined because the
#: Troubleshooting page tells operators to go and read it.
_LOG_NAME = "cronstable-service.log"


def config_is_user_scoped(config: str, user_profile: Optional[str]) -> bool:
    """Whether ``config`` lives under the installing user's own profile.

    A service runs as LocalSystem, whose profile is not this one, so a
    per-user configuration path is a deployment that works when tested
    interactively and finds nothing when the SCM starts it.  Whether that is
    fatal depends on whether the operator CHOSE the path, which is the
    caller's decision, not this function's.
    """
    return platform.path_is_user_scoped(config, user_profile)


#: One sentence per Win32 error this module can actually produce.  These are
#: the entire user interface of `service install` and its siblings: an
#: operator who gets "error 1072" learns nothing, and one who is told the
#: service is pending deletion until every open handle closes knows to shut
#: the Services console.
_ERROR_SENTENCES = {
    ERROR_ACCESS_DENIED: (
        "access denied. Run this from an elevated prompt (right click, "
        "Run as administrator)"
    ),
    ERROR_SERVICE_EXISTS: (
        "a service of that name is already installed. Use `cronstable "
        "service remove` first, or pass a different --name"
    ),
    ERROR_DUPLICATE_SERVICE_NAME: (
        "another service already uses that display name. Pass a different "
        "--name"
    ),
    ERROR_SERVICE_MARKED_FOR_DELETE: (
        "the service is pending deletion and cannot be recreated until "
        "every open handle to it closes. Close the Services console "
        "(services.msc) and any Task Manager Services tab, then retry"
    ),
    ERROR_SERVICE_DOES_NOT_EXIST: (
        "no service of that name is installed. Check --name, or install it "
        "with `cronstable service install`"
    ),
    ERROR_SERVICE_ALREADY_RUNNING: "the service is already running",
    ERROR_SERVICE_NOT_ACTIVE: "the service is not running",
    ERROR_INVALID_SERVICE_CONTROL: (
        "the running service does not accept that control. A service "
        "started from an older cronstable keeps running the old binary; "
        "restart it (`cronstable service stop`, then `start`) to run "
        "the current one"
    ),
    ERROR_SERVICE_CANNOT_ACCEPT_CTRL: (
        "the service is starting or stopping and takes no controls "
        "until it settles. Retry in a moment"
    ),
}


def describe_error(winerror: int, action: str) -> str:
    """``action`` plus the reason, as one sentence an operator can act on."""
    sentence = _ERROR_SENTENCES.get(winerror)
    if sentence is None:
        sentence = "Windows reported error {}".format(winerror)
    return "cronstable service: could not {}: {}".format(action, sentence)


def state_name(state: int) -> str:
    """A service state code as the word `sc query` would print."""
    return {
        SERVICE_STOPPED: "stopped",
        SERVICE_START_PENDING: "start-pending",
        SERVICE_STOP_PENDING: "stop-pending",
        SERVICE_RUNNING: "running",
    }.get(state, "unknown-{}".format(state))


# --- The OS seam -----------------------------------------------------------
class WinApi:
    """Every advapi32 and kernel32 call this module makes, and nothing else.

    A seam rather than an abstraction: there is one implementation, and its
    reason for existing is that :class:`ServiceHost` can then be driven in
    tests by a recording double.  Without it the status sequence, the
    control mapping and the stop path could only be exercised by actually
    installing a service, which needs Administrator and would make the whole
    subsystem untested on every CI runner.

    ctypes and ctypes.wintypes are imported inside the methods, never at
    module scope: ``ctypes.wintypes`` cannot be imported on Linux at all, so
    a module-level structure declaration would break ``cronstable service
    --help`` and the POSIX refusal path, and would stop every test in this
    file from even collecting there.

    Handles are 64 bit on Win64 and ctypes defaults every foreign function's
    restype to ``c_int``, which truncates them silently.  Every prototype
    here therefore sets ``restype`` and ``argtypes`` explicitly.
    """

    def __init__(self) -> None:
        # The ctypes thunks are handed to the OS and must outlive the call
        # that registers them; a local would be collected while the OS still
        # held the pointer. Same hazard cronstable.platform documents for
        # its console-control handler.
        self._thunks: list[Any] = []
        self._status_handle: Any = None
        self._advapi32: Any = None
        self._kernel32: Any = None

    # -- library loading ---------------------------------------------------
    def _libs(self) -> tuple[Any, Any]:  # pragma: no cover (windows)
        """advapi32 and kernel32, loaded once with every prototype declared.

        ``ctypes.WinDLL(..., use_last_error=True)`` rather than
        ``ctypes.windll``: the cached wrappers behind ``windll`` do not
        snapshot the calling thread's last error around the call, and here
        the error code IS the behavior, since every message this module
        prints is chosen by it.

        Every prototype is declared HERE rather than at its call site, and
        that is not tidiness.  ctypes defaults a foreign function's
        ``restype`` to ``c_int``, so an undeclared call that takes or
        returns a HANDLE truncates it to 32 bits on Win64.  Handing such a
        handle back to Windows can address a different object; handing a
        real one to an undeclared parameter raises ``OverflowError: int too
        long to convert``, which is what this code did before, on the very
        first ``CloseServiceHandle`` of an error path.  Declaring them one
        by one at the call sites is how that gets missed, so there is one
        list and it covers everything.

        Three prototypes are NOT here, because their argument types are
        built per call: the two callback registrations and SetServiceStatus,
        which needs a structure defined alongside it.
        """
        import ctypes
        from ctypes import wintypes

        if self._advapi32 is not None:
            return self._advapi32, self._kernel32
        advapi32 = ctypes.WinDLL(  # type: ignore[attr-defined]
            "advapi32", use_last_error=True
        )
        kernel32 = ctypes.WinDLL(  # type: ignore[attr-defined]
            "kernel32", use_last_error=True
        )
        handle_returning: dict[str, list[Any]] = {
            "OpenSCManagerW": [
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.DWORD,
            ],
            "OpenServiceW": [
                wintypes.HANDLE,
                wintypes.LPCWSTR,
                wintypes.DWORD,
            ],
            "CreateServiceW": [
                wintypes.HANDLE,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.LPVOID,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
            ],
        }
        bool_returning: dict[str, list[Any]] = {
            "CloseServiceHandle": [wintypes.HANDLE],
            "DeleteService": [wintypes.HANDLE],
            "StartServiceW": [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.LPVOID,
            ],
            "ControlService": [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.LPVOID,
            ],
            "QueryServiceStatusEx": [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.LPVOID,
                wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ],
            "ChangeServiceConfig2W": [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.LPVOID,
            ],
        }
        for symbol, argtypes in handle_returning.items():
            function = getattr(advapi32, symbol)
            function.restype = wintypes.HANDLE
            function.argtypes = argtypes
        for symbol, argtypes in bool_returning.items():
            function = getattr(advapi32, symbol)
            function.restype = wintypes.BOOL
            function.argtypes = argtypes
        kernel32.AllocConsole.restype = wintypes.BOOL
        kernel32.AllocConsole.argtypes = []
        kernel32.SetStdHandle.restype = wintypes.BOOL
        kernel32.SetStdHandle.argtypes = [wintypes.DWORD, wintypes.HANDLE]
        self._advapi32, self._kernel32 = advapi32, kernel32
        return advapi32, kernel32

    def _fail(self, action: str) -> None:  # pragma: no cover (windows)
        import ctypes

        code = ctypes.get_last_error()  # type: ignore[attr-defined]
        raise ServiceError(describe_error(code, action), winerror=code)

    # -- the service side --------------------------------------------------
    def dispatch_service(  # pragma: no cover (windows)
        self, name: str, service_main: Callable[[list[str]], None]
    ) -> None:
        """Hand this process to the SCM.  Blocks until the service stops.

        Raises :class:`ServiceError` with error 1063 when the process was
        not started by the SCM, which is the clean way to tell an operator
        who typed ``service run`` by hand what that command is for.
        """
        import ctypes
        from ctypes import wintypes

        advapi32, _ = self._libs()
        main_type = ctypes.WINFUNCTYPE(  # type: ignore[attr-defined]
            None, wintypes.DWORD, ctypes.POINTER(wintypes.LPWSTR)
        )

        def _entry(argc: int, argv: Any) -> None:
            service_main([argv[i] for i in range(argc)])

        thunk = main_type(_entry)
        self._thunks.append(thunk)

        class SERVICE_TABLE_ENTRYW(ctypes.Structure):
            _fields_ = [
                ("lpServiceName", wintypes.LPWSTR),
                ("lpServiceProc", main_type),
            ]

        # Two entries, the second zeroed: the table is NULL terminated and
        # the call does not return cleanly without the terminator.
        table = (SERVICE_TABLE_ENTRYW * 2)()
        table[0].lpServiceName = name
        table[0].lpServiceProc = thunk
        advapi32.StartServiceCtrlDispatcherW.restype = wintypes.BOOL
        advapi32.StartServiceCtrlDispatcherW.argtypes = [
            ctypes.POINTER(SERVICE_TABLE_ENTRYW)
        ]
        if not advapi32.StartServiceCtrlDispatcherW(table):
            self._fail("connect to the service control manager")

    def register_handler(  # pragma: no cover (windows)
        self, name: str, handler: Callable[[int, int, int, int], int]
    ) -> None:
        """Register the control handler and remember its status handle.

        The callback takes FOUR parameters (control, event type, event data,
        context) and returns a DWORD.  A two-parameter prototype is the
        wrong callback type: it drops the trailing arguments and puts the
        stdcall frame wrong.
        """
        import ctypes
        from ctypes import wintypes

        advapi32, _ = self._libs()
        handler_type = ctypes.WINFUNCTYPE(  # type: ignore[attr-defined]
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.LPVOID,
        )

        def _entry(control: int, event: int, data: Any, context: Any) -> int:
            return handler(control, event, 0, 0)

        thunk = handler_type(_entry)
        self._thunks.append(thunk)
        advapi32.RegisterServiceCtrlHandlerExW.restype = wintypes.HANDLE
        advapi32.RegisterServiceCtrlHandlerExW.argtypes = [
            wintypes.LPCWSTR,
            handler_type,
            wintypes.LPVOID,
        ]
        handle = advapi32.RegisterServiceCtrlHandlerExW(name, thunk, None)
        if not handle:
            self._fail("register the service control handler")
        self._status_handle = handle

    def set_status(  # pragma: no cover (windows)
        self, fields: Sequence[int]
    ) -> None:
        import ctypes
        from ctypes import wintypes

        advapi32, _ = self._libs()

        class SERVICE_STATUS(ctypes.Structure):
            _fields_ = [
                ("dwServiceType", wintypes.DWORD),
                ("dwCurrentState", wintypes.DWORD),
                ("dwControlsAccepted", wintypes.DWORD),
                ("dwWin32ExitCode", wintypes.DWORD),
                ("dwServiceSpecificExitCode", wintypes.DWORD),
                ("dwCheckPoint", wintypes.DWORD),
                ("dwWaitHint", wintypes.DWORD),
            ]

        advapi32.SetServiceStatus.restype = wintypes.BOOL
        advapi32.SetServiceStatus.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(SERVICE_STATUS),
        ]
        status = SERVICE_STATUS(*fields)
        advapi32.SetServiceStatus(self._status_handle, ctypes.byref(status))

    # -- the control side --------------------------------------------------
    def _open_service(  # pragma: no cover (windows)
        self, name: str, manager_access: int, service_access: int, action: str
    ) -> tuple[Any, Any, Any]:
        advapi32, _ = self._libs()
        manager = advapi32.OpenSCManagerW(None, None, manager_access)
        if not manager:
            self._fail(action)
        service = advapi32.OpenServiceW(manager, name, service_access)
        if not service:
            advapi32.CloseServiceHandle(manager)
            self._fail(action)
        return advapi32, manager, service

    def create_service(  # pragma: no cover (windows)
        self, *, name: str, display: str, image: str, start_type: int
    ) -> None:
        advapi32, _ = self._libs()
        manager = advapi32.OpenSCManagerW(
            None, None, SC_MANAGER_CONNECT | SC_MANAGER_CREATE_SERVICE
        )
        if not manager:
            self._fail("open the service control manager")
        try:
            service = advapi32.CreateServiceW(
                manager,
                name,
                display,
                SERVICE_CHANGE_CONFIG,
                SERVICE_WIN32_OWN_PROCESS,
                start_type,
                SERVICE_ERROR_NORMAL,
                image,
                None,
                None,
                None,
                # lpServiceStartName NULL is LocalSystem. Per-account
                # identity is a separate piece of work with its own
                # credential-storage question.
                None,
                None,
            )
            if not service:
                self._fail("install the service")
            advapi32.CloseServiceHandle(service)
        finally:
            advapi32.CloseServiceHandle(manager)

    def _config2(  # pragma: no cover (windows)
        self,
        name: str,
        level: int,
        payload: Any,
        action: str,
        access: int = SERVICE_CHANGE_CONFIG,
    ) -> None:
        import ctypes

        advapi32, manager, service = self._open_service(
            name,
            SC_MANAGER_CONNECT,
            access,
            action,
        )
        try:
            if not advapi32.ChangeServiceConfig2W(
                service, level, ctypes.byref(payload)
            ):
                self._fail(action)
        finally:
            advapi32.CloseServiceHandle(service)
            advapi32.CloseServiceHandle(manager)

    def set_description(  # pragma: no cover (windows)
        self, name: str, text: str
    ) -> None:
        import ctypes
        from ctypes import wintypes

        class SERVICE_DESCRIPTIONW(ctypes.Structure):
            _fields_ = [("lpDescription", wintypes.LPWSTR)]

        self._config2(
            name,
            SERVICE_CONFIG_DESCRIPTION,
            SERVICE_DESCRIPTIONW(text),
            "set the service description",
        )

    def set_delayed_auto_start(  # pragma: no cover (windows)
        self, name: str, delayed: bool
    ) -> None:
        import ctypes
        from ctypes import wintypes

        class SERVICE_DELAYED_AUTO_START_INFO(ctypes.Structure):
            _fields_ = [("fDelayedAutostart", wintypes.BOOL)]

        self._config2(
            name,
            SERVICE_CONFIG_DELAYED_AUTO_START_INFO,
            SERVICE_DELAYED_AUTO_START_INFO(1 if delayed else 0),
            "set the delayed start flag",
        )

    def set_failure_actions(  # pragma: no cover (windows)
        self, name: str, actions: Sequence[tuple[int, int]], reset_s: int
    ) -> None:
        import ctypes
        from ctypes import wintypes

        class SC_ACTION(ctypes.Structure):
            _fields_ = [
                ("Type", wintypes.DWORD),
                ("Delay", wintypes.DWORD),
            ]

        class SERVICE_FAILURE_ACTIONSW(ctypes.Structure):
            _fields_ = [
                ("dwResetPeriod", wintypes.DWORD),
                ("lpRebootMsg", wintypes.LPWSTR),
                ("lpCommand", wintypes.LPWSTR),
                ("cActions", wintypes.DWORD),
                ("lpsaActions", ctypes.POINTER(SC_ACTION)),
            ]

        array = (SC_ACTION * len(actions))()
        for index, (kind, delay_ms) in enumerate(actions):
            array[index].Type = kind
            array[index].Delay = delay_ms
        payload = SERVICE_FAILURE_ACTIONSW(
            reset_s, None, None, len(actions), array
        )
        # SERVICE_START on top of SERVICE_CHANGE_CONFIG: ChangeServiceConfig2W
        # refuses a failure-actions payload containing SC_ACTION_RESTART with
        # ERROR_ACCESS_DENIED unless the handle carries the start right, even
        # from an elevated caller.
        self._config2(
            name,
            SERVICE_CONFIG_FAILURE_ACTIONS,
            payload,
            "set the recovery actions",
            access=SERVICE_CHANGE_CONFIG | SERVICE_START,
        )

    def set_failure_actions_flag(  # pragma: no cover (windows)
        self, name: str, on: bool
    ) -> None:
        """Make recovery apply to a clean nonzero exit, not just a crash.

        Without this flag the exit codes this host reports are decorative:
        a service that stops itself after a failure has "stopped normally"
        as far as the SCM is concerned, and no recovery action runs.
        """
        import ctypes
        from ctypes import wintypes

        class SERVICE_FAILURE_ACTIONS_FLAG(ctypes.Structure):
            _fields_ = [("fFailureActionsOnNonCrashFailures", wintypes.BOOL)]

        self._config2(
            name,
            SERVICE_CONFIG_FAILURE_ACTIONS_FLAG,
            SERVICE_FAILURE_ACTIONS_FLAG(1 if on else 0),
            "set the recovery trigger",
        )

    def delete_service(self, name: str) -> None:  # pragma: no cover (windows)
        advapi32, manager, service = self._open_service(
            name, SC_MANAGER_CONNECT, DELETE, "remove the service"
        )
        try:
            if not advapi32.DeleteService(service):
                self._fail("remove the service")
        finally:
            advapi32.CloseServiceHandle(service)
            advapi32.CloseServiceHandle(manager)

    def start_service(self, name: str) -> None:  # pragma: no cover (windows)
        advapi32, manager, service = self._open_service(
            name, SC_MANAGER_CONNECT, SERVICE_START, "start the service"
        )
        try:
            if not advapi32.StartServiceW(service, 0, None):
                self._fail("start the service")
        finally:
            advapi32.CloseServiceHandle(service)
            advapi32.CloseServiceHandle(manager)

    def control_service(  # pragma: no cover (windows)
        self,
        name: str,
        control: int,
        *,
        access: int = SERVICE_STOP,
        action: str = "stop the service",
    ) -> None:
        """Send ``control``, over a handle carrying ``access``.

        ControlService checks a per-control access right on the handle
        (SERVICE_STOP for a stop, SERVICE_PAUSE_CONTINUE for
        PARAMCHANGE), so the caller names the right along with the
        control; a mismatched pair is refused as access denied even from
        an elevated prompt.
        """
        import ctypes
        from ctypes import wintypes

        class SERVICE_STATUS(ctypes.Structure):
            _fields_ = [
                ("dwServiceType", wintypes.DWORD),
                ("dwCurrentState", wintypes.DWORD),
                ("dwControlsAccepted", wintypes.DWORD),
                ("dwWin32ExitCode", wintypes.DWORD),
                ("dwServiceSpecificExitCode", wintypes.DWORD),
                ("dwCheckPoint", wintypes.DWORD),
                ("dwWaitHint", wintypes.DWORD),
            ]

        advapi32, manager, service = self._open_service(
            name, SC_MANAGER_CONNECT, access, action
        )
        try:
            status = SERVICE_STATUS()
            if not advapi32.ControlService(
                service, control, ctypes.byref(status)
            ):
                self._fail(action)
        finally:
            advapi32.CloseServiceHandle(service)
            advapi32.CloseServiceHandle(manager)

    def query_status(  # pragma: no cover (windows)
        self, name: str
    ) -> tuple[int, ...]:
        import ctypes
        from ctypes import wintypes

        class SERVICE_STATUS_PROCESS(ctypes.Structure):
            _fields_ = [
                ("dwServiceType", wintypes.DWORD),
                ("dwCurrentState", wintypes.DWORD),
                ("dwControlsAccepted", wintypes.DWORD),
                ("dwWin32ExitCode", wintypes.DWORD),
                ("dwServiceSpecificExitCode", wintypes.DWORD),
                ("dwCheckPoint", wintypes.DWORD),
                ("dwWaitHint", wintypes.DWORD),
                ("dwProcessId", wintypes.DWORD),
                ("dwServiceFlags", wintypes.DWORD),
            ]

        advapi32, manager, service = self._open_service(
            name,
            SC_MANAGER_CONNECT,
            SERVICE_QUERY_STATUS,
            "query the service",
        )
        try:
            status = SERVICE_STATUS_PROCESS()
            needed = wintypes.DWORD()
            if not advapi32.QueryServiceStatusEx(
                service,
                SC_STATUS_PROCESS_INFO,
                ctypes.byref(status),
                ctypes.sizeof(status),
                ctypes.byref(needed),
            ):
                self._fail("query the service")
            return (
                status.dwServiceType,
                status.dwCurrentState,
                status.dwControlsAccepted,
                status.dwWin32ExitCode,
                status.dwServiceSpecificExitCode,
                status.dwCheckPoint,
                status.dwWaitHint,
                status.dwProcessId,
                status.dwServiceFlags,
            )
        finally:
            advapi32.CloseServiceHandle(service)
            advapi32.CloseServiceHandle(manager)

    # -- the optional console ----------------------------------------------
    def alloc_console(self) -> bool:  # pragma: no cover (windows)
        _, kernel32 = self._libs()
        return bool(kernel32.AllocConsole())

    def detach_std_handles(self) -> None:  # pragma: no cover (windows)
        """Point the process's three standard handles at nothing.

        A freshly allocated console hands the process live stdin, stdout and
        stderr, and a job spawned without capture inherits whatever they are
        (CPython omits STARTF_USESTDHANDLES when none of the three is
        redirected).  An uncaptured job would then gain a console stdin that
        blocks instead of reaching end of file.  This closes that half.  It
        does NOT undo console membership, which is inherited separately and
        is the entire reason for allocating the console.
        """
        _, kernel32 = self._libs()
        for handle_id in (
            STD_INPUT_HANDLE,
            STD_OUTPUT_HANDLE,
            STD_ERROR_HANDLE,
        ):
            # The ids are documented as (DWORD)-10 and friends, so they
            # travel unsigned; the declared DWORD parameter refuses the
            # negative spelling outright.
            kernel32.SetStdHandle(handle_id & 0xFFFFFFFF, None)

    def swallow_console_events(  # pragma: no cover (windows)
        self, on_shutdown: Callable[[], None]
    ) -> None:
        """Absorb control events on the console this process allocated.

        The inverse of the handler cronstable.platform installs for an
        interactive daemon, and for a reason: that one returns FALSE for
        Ctrl-C and Ctrl-Break so the interpreter's signal chain sees them,
        while a service deliberately installs no Python signal handlers at
        all, which would leave the default action (terminate) in force for
        any stray event on the allocated console.
        """
        import ctypes
        from ctypes import wintypes

        _, kernel32 = self._libs()
        handler_type = ctypes.WINFUNCTYPE(  # type: ignore[attr-defined]
            wintypes.BOOL, wintypes.DWORD
        )

        def _handle(event: int) -> bool:
            try:
                if event in (CTRL_CLOSE_EVENT, CTRL_SHUTDOWN_EVENT):
                    on_shutdown()
                return True
            except Exception:  # noqa: BLE001 - never raise into the OS
                return True

        thunk = handler_type(_handle)
        self._thunks.append(thunk)
        kernel32.SetConsoleCtrlHandler(thunk, True)


# --- The service host ------------------------------------------------------
class ServiceHost:
    """Runs the scheduler under the Service Control Manager.

    Three threads are involved and each does exactly one thing.  The main
    thread calls :meth:`run` and then blocks inside the SCM's dispatcher for
    the life of the service.  A thread the SCM creates runs
    :meth:`_service_main`, which reports status, builds the scheduler and
    runs the event loop.  Another SCM thread delivers each control to
    :meth:`_control`, which never does the work itself: it hands the stop
    or the reload onto the loop thread and returns at once, because a
    control handler that blocks is a service the SCM reports as hung.
    """

    def __init__(
        self,
        args: Any,
        *,
        run_daemon: Callable[..., None],
        new_event_loop: Callable[[], Any],
        api: WinApi,
    ) -> None:
        self.args = args
        self.name = getattr(args, "name", SERVICE_NAME_DEFAULT)
        self._run_daemon = run_daemon
        self._new_event_loop = new_event_loop
        self._api = api
        self._cron: Any = None
        self._loop: Any = None
        self._checkpoint = 0
        self._pump_stop: Optional[threading.Event] = None
        self._pump: Optional[threading.Thread] = None
        self._exit_code = 0
        self._specific_exit = 0

    # -- status reporting --------------------------------------------------
    def _report(self, state: int, **kwargs: Any) -> None:
        self._api.set_status(status_fields(state, **kwargs))

    def _start_pump(self, state: int) -> None:
        """Report progress in a pending state until told to stop.

        The SCM does not require a service to become ready within a fixed
        time; it requires the checkpoint to keep advancing.  Pumping it
        removes every guess about how long a config parse or a job drain
        might take, which is the difference between a scheduler that may
        take minutes to stop and one Windows kills for hanging.
        """
        self._stop_pump()
        stop = threading.Event()

        def _tick() -> None:
            while not stop.wait(_STATUS_PUMP_INTERVAL):
                self._checkpoint += 1
                self._report(
                    state,
                    checkpoint=self._checkpoint,
                    wait_hint_ms=_STATUS_WAIT_HINT_MS,
                )

        self._pump_stop = stop
        self._pump = threading.Thread(
            target=_tick, name="cronstable-scm-status", daemon=True
        )
        self._pump.start()

    def _stop_pump(self) -> None:
        """Stop the pumper AND wait for it.

        The join is load-bearing.  A tick that lands after the terminal
        report would put the service back into a pending state seconds
        after it said STOPPED, and the SCM would then wait out the whole
        wait hint on a process that had already gone.
        """
        if self._pump_stop is not None:
            self._pump_stop.set()
        if self._pump is not None:
            self._pump.join(_STATUS_PUMP_INTERVAL * 2)
        self._pump_stop = None
        self._pump = None

    # -- the SCM entry points ----------------------------------------------
    def run(self) -> int:
        """Hand the process to the SCM.  Returns a process exit code."""
        try:
            self._api.dispatch_service(self.name, self._service_main)
        except ServiceError as ex:
            if ex.winerror == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT:
                print(
                    "cronstable service run: this command is started by the "
                    "Windows Service Control Manager, not by hand. Install "
                    "the service with `cronstable service install` and start "
                    "it with `cronstable service start`, or run the "
                    "scheduler in the foreground with plain `cronstable`.",
                    file=sys.stderr,
                )
                return 2
            print(str(ex), file=sys.stderr)
            return 1
        return self._exit_code and 1 or 0

    def _service_main(self, argv: list[str]) -> None:
        # The argv the SCM passes is the service name plus whatever
        # StartService supplied; this process's real configuration is its
        # own command line, already parsed. Logged and otherwise ignored.
        try:
            self._api.register_handler(self.name, self._control)
            self._checkpoint = 1
            self._report(
                SERVICE_START_PENDING,
                checkpoint=self._checkpoint,
                wait_hint_ms=_STATUS_WAIT_HINT_MS,
            )
            self._start_pump(SERVICE_START_PENDING)
            self._open_bootstrap_log()
            logger.info(
                "cronstable service %s starting: argv=%r config=%r",
                self.name,
                list(sys.argv),
                getattr(self.args, "config", None),
            )
            logger.debug("service control manager argv: %r", argv)
            self._prepare_console()
            self._build_and_run()
        except Exception:  # noqa: BLE001 - nothing may escape into the SCM
            logger.exception("cronstable service failed")
            if not self._specific_exit:
                self._specific_exit = EXIT_RUN_FAILED
        finally:
            self._stop_pump()
            if self._specific_exit:
                self._report(
                    SERVICE_STOPPED,
                    win32_exit=ERROR_SERVICE_SPECIFIC_ERROR,
                    specific_exit=self._specific_exit,
                )
                self._exit_code = 1
            else:
                self._report(SERVICE_STOPPED)

    def _build_and_run(self) -> None:
        from cronstable.config import ConfigError
        from cronstable.cron import Cron

        try:
            self._cron = Cron(self.args.config)
        except ConfigError as err:
            logger.error("Configuration error: %s", err)
            self._specific_exit = EXIT_CONFIG_FAILED
            return
        loop = self._new_event_loop()
        self._loop = loop
        try:
            # Queued BEFORE run_until_complete, so it runs on the loop's
            # first tick: the loop is genuinely up by then, and nothing
            # unbounded has happened yet. Reporting later would let a hung
            # mount look like a failed start; reporting earlier would turn a
            # startup crash into "terminated unexpectedly" rather than a
            # start failure carrying a code.
            loop.call_soon(self._mark_running)
            self._run_daemon(self._cron, loop, shutdown_handlers=False)
        finally:
            self._loop = None
            loop.close()

    def _mark_running(self) -> None:
        self._stop_pump()
        self._checkpoint = 0
        self._report(SERVICE_RUNNING)
        logger.info("cronstable service %s is running", self.name)

    def _control(
        self, control: int, event_type: int, data: int, context: int
    ) -> int:
        """The SCM control handler.  Returns fast, always.

        Runs on a thread the SCM created, so nothing may escape it and
        nothing may block in it.
        """
        try:
            action = control_action(control)
            if action == "unhandled":
                return ERROR_CALL_NOT_IMPLEMENTED
            if action == "report":
                return NO_ERROR
            if action == "reload":
                # No state transition: PARAMCHANGE leaves the service
                # RUNNING, and the mask only advertises it in that state,
                # so the loop exists by the time one can arrive. The
                # guard covers a stop draining the loop away under it.
                loop, cron = self._loop, self._cron
                if loop is not None and cron is not None:
                    try:
                        loop.call_soon_threadsafe(
                            cron.signal_reload,
                            "service control PARAMCHANGE",
                        )
                    except RuntimeError:
                        pass
                return NO_ERROR
            self._checkpoint = 1
            self._report(
                SERVICE_STOP_PENDING,
                checkpoint=self._checkpoint,
                wait_hint_ms=_STATUS_WAIT_HINT_MS,
            )
            self._start_pump(SERVICE_STOP_PENDING)
            loop, cron = self._loop, self._cron
            if loop is not None and cron is not None:
                try:
                    loop.call_soon_threadsafe(cron.signal_shutdown)
                except RuntimeError:
                    # the loop closed between the read and the call; the
                    # drain is already over.
                    pass
            return NO_ERROR
        except Exception:  # noqa: BLE001 - never raise into the OS callback
            logger.exception("cronstable service control handler failed")
            return NO_ERROR

    # -- startup helpers ---------------------------------------------------
    def _open_bootstrap_log(self) -> None:
        """Give the service somewhere to report before the config is read.

        A service process has no console: measured, its sys.stdout,
        sys.stderr and sys.stdin are all None, so the StreamHandler the
        entry point installed formats every record, fails writing it to
        None, and has that failure swallowed because the fallback path is
        itself guarded on sys.stderr.  Nothing is written and nothing says
        so.  Without a file, a service that cannot start is undiagnosable,
        which is why failing to open one refuses the start rather than
        continuing quietly.
        """
        if getattr(self.args, "no_log_file", False):
            return
        import logging.handlers

        path = bootstrap_log_path(
            getattr(self.args, "config", "") or "",
            getattr(self.args, "log_file", None),
            config_is_dir=os.path.isdir(
                getattr(self.args, "config", "") or ""
            ),
            program_data=os.environ.get("PROGRAMDATA"),
        )
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                path,
                maxBytes=_LOG_BYTES,
                backupCount=_LOG_BACKUPS,
                encoding="utf-8",
            )
        except OSError as ex:
            self._specific_exit = EXIT_LOG_FAILED
            raise ServiceError(
                "could not open the service log {}: {}".format(path, ex)
            ) from ex
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        # force=True: this replaces the handler the entry point installed
        # over a stream that is None in a service.
        logging.basicConfig(
            force=True,
            level=logging.getLogger().level,
            handlers=[handler],
        )

    def _prepare_console(self) -> None:
        """Allocate a console, when asked, so job kills stay two-step.

        Off by default.  Without a console the graceful CTRL_BREAK step of a
        job kill cannot be delivered and the kill degrades to the immediate
        tree kill, so killTimeout bounds nothing; with one, the two-step
        works as it does interactively.  What it costs is that a job is then
        genuinely attached to a console: it can open CONIN$ and CONOUT$,
        GetConsoleWindow no longer returns NULL, and a `pause` in a .cmd can
        still block.  That is a real behavior change, which is why it is a
        flag rather than the default.
        """
        if not getattr(self.args, "console", False):
            return
        if not self._api.alloc_console():
            logger.warning(
                "could not allocate a console for the service; job "
                "termination will skip the graceful CTRL_BREAK step and "
                "go straight to the tree kill, so killTimeout will not "
                "bound anything"
            )
            return
        self._api.detach_std_handles()
        self._api.swallow_console_events(self._request_shutdown)
        logger.info(
            "allocated a console: job termination keeps its graceful "
            "CTRL_BREAK step"
        )

    def _request_shutdown(self) -> None:
        loop, cron = self._loop, self._cron
        if loop is not None and cron is not None:
            try:
                loop.call_soon_threadsafe(cron.signal_shutdown)
            except RuntimeError:
                pass


# --- The control verbs -----------------------------------------------------
def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 2


def install(args: Any, api: WinApi) -> int:
    """Register the service with the SCM."""
    layout = frozen_layout(sys.executable, getattr(sys, "_MEIPASS", None))
    if layout == "onefile":
        return _fail(
            "cronstable service install: a one-file build cannot host a "
            "Windows service. Its bootloader unpacks itself and runs the "
            "program in a child process, so the process the Service "
            "Control Manager starts never registers, and the start fails "
            "on the SCM's timeout. Download the one-directory build for "
            "your architecture ({}, or {} on the releases page), extract "
            "it, and run `cronstable service install` from its "
            "cronstable.exe; or install the .msi, which registers the "
            "service itself; or install cronstable with pip or pipx; or "
            "keep using the schtasks recipe in the Windows "
            "documentation.".format(
                ", ".join(ONEDIR_RELEASE_ASSETS[:-1]),
                ONEDIR_RELEASE_ASSETS[-1],
            )
        )
    config = getattr(args, "config", None)
    if not config:
        return _fail(
            "cronstable service install: no configuration path. Pass -c "
            "with the directory the service should read, for example -c "
            "C:\\ProgramData\\cronstable."
        )
    if not os.path.exists(config):
        return _fail(
            "cronstable service install: {} does not exist. Create it "
            "first with `cronstable init {}`.".format(config, config)
        )
    if config_is_user_scoped(config, os.environ.get("USERPROFILE")):
        # Whether a per-user path is fatal depends on whether it was
        # chosen. Left at the platform default it is not a choice at all,
        # it is the per-user fallback the resolver lands on, and a service
        # running as LocalSystem resolves that same default somewhere else
        # entirely; installing it would produce a service that starts and
        # schedules nothing. Named explicitly it is a deliberate act that
        # works, because LocalSystem can read the profile, so it earns a
        # warning about the fragility rather than a refusal.
        if config == platform.DEFAULT_CONFIG_PATH:
            return _fail(
                "cronstable service install: {} is your own per-user "
                "configuration directory, and a service runs as "
                "LocalSystem, which resolves that default to a different "
                "directory entirely, so the installed service would "
                "schedule nothing. Put the configuration somewhere "
                "machine-wide and name it: `cronstable init "
                "C:\\ProgramData\\cronstable`, then `cronstable service "
                "install -c C:\\ProgramData\\cronstable`.".format(config)
            )
        print(
            "cronstable service install: note, {} is inside your user "
            "profile. The service runs as LocalSystem and can still read "
            "it, but a machine-wide directory such as "
            "%ProgramData%\\cronstable is the durable place for "
            "it.".format(config),
            file=sys.stderr,
        )
    grantee = platform.any_user_write_grantee(config)
    if grantee is not None:
        # A note rather than a refusal: the service reads the directory
        # the operator named, and the daemon says the same thing once at
        # every start until the recipe is applied.
        print(
            "cronstable service install: note, "
            + platform.writable_config_advice(config, grantee),
            file=sys.stderr,
        )
    start_type, delayed = start_type_code(getattr(args, "start_type", "auto"))
    argv = host_argv(
        config=config,
        name=args.name,
        log_file=getattr(args, "log_file", None),
        no_log_file=getattr(args, "no_log_file", False),
        console=getattr(args, "console", False),
        log_level=getattr(args, "log_level", "INFO"),
        executable=sys.executable,
        frozen=layout != "source",
    )
    try:
        api.create_service(
            name=args.name,
            display=display_name(args.name),
            image=image_path(argv),
            start_type=start_type,
        )
        api.set_description(args.name, SERVICE_DESCRIPTION)
        if delayed:
            api.set_delayed_auto_start(args.name, True)
        if not getattr(args, "no_restart", False):
            actions, reset_s = failure_actions_plan(
                getattr(args, "restart_delay", 60.0)
            )
            api.set_failure_actions(args.name, actions, reset_s)
            # Without the flag the actions above only fire on a crash, and
            # this host reports its failures as a clean stop carrying an
            # exit code, which would never trigger them.
            api.set_failure_actions_flag(args.name, True)
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    print("installed service {}".format(args.name))
    print("  command: {}".format(image_path(argv)))
    print("  start it with: cronstable service start")
    return 0


def remove(args: Any, api: WinApi) -> int:
    """Stop the service if it is running, then unregister it."""
    try:
        try:
            api.control_service(args.name, SERVICE_CONTROL_STOP)
        except ServiceError as ex:
            # already stopped is the ordinary case, not a failure
            if ex.winerror != ERROR_SERVICE_NOT_ACTIVE:
                raise
        _wait_for_state(api, args.name, SERVICE_STOPPED, 0.0)
        api.delete_service(args.name)
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    print("removed service {}".format(args.name))
    return 0


def start(args: Any, api: WinApi) -> int:
    try:
        api.start_service(args.name)
        reached = _wait_for_state(
            api, args.name, SERVICE_RUNNING, getattr(args, "timeout", 30.0)
        )
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    if not reached:
        print(
            "cronstable service start: {} did not report running in time. "
            "Check the service log.".format(args.name),
            file=sys.stderr,
        )
        return 1
    print("started service {}".format(args.name))
    return 0


def stop(args: Any, api: WinApi) -> int:
    try:
        api.control_service(args.name, SERVICE_CONTROL_STOP)
        reached = _wait_for_state(
            api, args.name, SERVICE_STOPPED, getattr(args, "timeout", 0.0)
        )
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    if not reached:
        print(
            "cronstable service stop: {} is still draining. It stops once "
            "its running jobs finish.".format(args.name),
            file=sys.stderr,
        )
        return 1
    print("stopped service {}".format(args.name))
    return 0


def reload(args: Any, api: WinApi) -> int:
    """Ask the running service to reload its configuration now.

    The Windows spelling of SIGHUP: PARAMCHANGE reaches the host's
    control handler, which posts the same forced reparse onto the loop
    (`sc control <name> paramchange` sends the identical control).
    Nothing is waited for; the reload is the loop thread's next act.
    """
    try:
        api.control_service(
            args.name,
            SERVICE_CONTROL_PARAMCHANGE,
            access=SERVICE_PAUSE_CONTINUE,
            action="reload the service",
        )
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    print("asked service {} to reload its configuration".format(args.name))
    return 0


def status(args: Any, api: WinApi) -> int:
    try:
        fields = api.query_status(args.name)
    except ServiceError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    state = fields[1]
    print("{}: {}".format(args.name, state_name(state)))
    if state == SERVICE_RUNNING and len(fields) > 7 and fields[7]:
        print("  pid: {}".format(fields[7]))
    if state == SERVICE_STOPPED and fields[3] == (
        ERROR_SERVICE_SPECIFIC_ERROR
    ):
        print(
            "  last exit: {}".format(
                {
                    EXIT_RUN_FAILED: "the scheduler stopped with an error",
                    EXIT_CONFIG_FAILED: "the configuration did not parse",
                    EXIT_LOG_FAILED: "the service log could not be opened",
                }.get(fields[4], "service-specific code {}".format(fields[4]))
            )
        )
    return 0


def _wait_for_state(
    api: WinApi, name: str, wanted: int, timeout: float
) -> bool:
    """Poll until the service reaches ``wanted``.  ``timeout`` 0 waits.

    Waiting forever is the right default for a stop: the drain finishes when
    the running jobs do, and a scheduler with a two hour job is not hung.
    """
    deadline = None if timeout <= 0 else time.monotonic() + timeout
    while True:
        try:
            if api.query_status(name)[1] == wanted:
                return True
        except ServiceError:
            # a delete that already took effect, or a status we may no
            # longer read: nothing left to wait for.
            return True
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(0.25)


_ACTIONS: dict[str, Callable[[Any, WinApi], int]] = {
    "install": install,
    "remove": remove,
    "start": start,
    "stop": stop,
    "reload": reload,
    "status": status,
}


def dispatch(
    args: Any,
    *,
    run_daemon: Callable[..., None],
    new_event_loop: Callable[[], Any],
    api: Optional[WinApi] = None,
) -> int:
    """Route ``cronstable service <action>``.

    ``run_daemon`` and ``new_event_loop`` arrive as parameters rather than
    imports.  The ImagePath a source install writes is ``python -m
    cronstable``, and importing the entry point by module name from inside
    that process would execute it a second time under its package name,
    leaving two module objects with two resolved config defaults.  Passing
    them also leaves the uvloop-versus-stock decision with its single
    documented owner.
    """
    if not platform.IS_WINDOWS:
        return _fail(
            "cronstable service: Windows services exist only on Windows. "
            "On POSIX, run cronstable under systemd, launchd or your "
            "process supervisor of choice."
        )
    action = getattr(args, "service_command", None)
    if action is None:
        return _fail(
            "cronstable service: name an action "
            "(install, remove, start, stop, reload, status)."
        )
    if api is None:
        api = WinApi()
    if action == "run":
        return ServiceHost(
            args,
            run_daemon=run_daemon,
            new_event_loop=new_event_loop,
            api=api,
        ).run()
    return _ACTIONS[action](args, api)
