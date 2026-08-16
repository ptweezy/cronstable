"""The Windows service host: decisions, status sequence and control path.

Almost none of this needs Windows. Every decision the host makes is a pure
function, and every OS call goes through :class:`cronstable.winservice.WinApi`,
which is replaced here by a recording double. That is deliberate: installing
a real service needs Administrator, which no CI runner reliably has and no
test should require, so the parts that would otherwise go untested (the
status sequence, the control mapping, the stop path, the exit-code contract)
are exercised on every OS instead.

The handful of tests that do need Windows are marked, and they only probe
what any Windows host can answer.
"""

import os
import types

import pytest

from cronstable import platform, winservice
from cronstable.winservice import (
    ERROR_ACCESS_DENIED,
    ERROR_FAILED_SERVICE_CONTROLLER_CONNECT,
    ERROR_SERVICE_MARKED_FOR_DELETE,
    ERROR_SERVICE_SPECIFIC_ERROR,
    SERVICE_ACCEPT_PRESHUTDOWN,
    SERVICE_ACCEPT_SHUTDOWN,
    SERVICE_ACCEPT_STOP,
    SERVICE_AUTO_START,
    SERVICE_CONTROL_INTERROGATE,
    SERVICE_CONTROL_PRESHUTDOWN,
    SERVICE_CONTROL_SHUTDOWN,
    SERVICE_CONTROL_STOP,
    SERVICE_DEMAND_START,
    SERVICE_RUNNING,
    SERVICE_START_PENDING,
    SERVICE_STOP_PENDING,
    SERVICE_STOPPED,
    SERVICE_WIN32_OWN_PROCESS,
    ServiceError,
    ServiceHost,
    accepted_controls,
    bootstrap_log_path,
    config_is_user_scoped,
    control_action,
    describe_error,
    display_name,
    failure_actions_plan,
    frozen_layout,
    host_argv,
    image_path,
    start_type_code,
    status_fields,
)


# ---------------------------------------------------------------------------
# Packaging shape
# ---------------------------------------------------------------------------


def test_frozen_layout_source_when_not_frozen():
    assert frozen_layout(r"C:\Python\python.exe", None) == "source"


def test_frozen_layout_onedir_when_the_bundle_sits_beside_the_exe():
    assert (
        frozen_layout(r"C:\app\cronstable.exe", r"C:\app")
        == "onedir"
    )
    assert (
        frozen_layout(r"C:\app\cronstable.exe", r"C:\app\_internal")
        == "onedir"
    )


def test_frozen_layout_onefile_when_the_bundle_is_elsewhere():
    # A one-file build unpacks to %TEMP% and runs the program in a CHILD
    # process, so the process the SCM starts never registers. This is the
    # value install refuses on.
    assert (
        frozen_layout(r"C:\app\cronstable.exe", r"C:\Temp\_MEI213682")
        == "onefile"
    )


# ---------------------------------------------------------------------------
# The command line the SCM is given
# ---------------------------------------------------------------------------


def _argv(**over):
    kwargs = {
        "config": r"C:\ProgramData\cronstable",
        "name": "cronstable",
        "log_file": None,
        "no_log_file": False,
        "console": False,
        "log_level": "INFO",
        "executable": r"C:\app\cronstable.exe",
        "frozen": True,
    }
    kwargs.update(over)
    return host_argv(**kwargs)


def test_host_argv_puts_the_root_log_flag_before_the_subcommand():
    # -l is a ROOT flag, so argparse only accepts it ahead of `service`.
    # Behind it, every installed service is pinned at INFO for good.
    argv = _argv(log_level="DEBUG")
    assert argv.index("-l") < argv.index("service")
    assert argv[argv.index("-l") + 1] == "DEBUG"


def test_host_argv_for_a_source_install_runs_the_module():
    # never the Scripts\cronstable.exe console-script shim: that launches
    # the interpreter as a child and waits, which is the one-file problem.
    argv = _argv(executable=r"C:\Python\python.exe", frozen=False)
    assert argv[:3] == [r"C:\Python\python.exe", "-m", "cronstable"]


def test_host_argv_bakes_an_absolute_config_path():
    argv = _argv(config="relative-dir")
    assert os.path.isabs(argv[argv.index("-c") + 1])


def test_host_argv_carries_the_optional_flags():
    argv = _argv(console=True, log_file="log.txt")
    assert "--console" in argv
    assert os.path.isabs(argv[argv.index("--log-file") + 1])
    assert "--no-log-file" not in argv


def test_host_argv_no_log_file_wins_over_a_log_file():
    argv = _argv(no_log_file=True, log_file="log.txt")
    assert "--no-log-file" in argv
    assert "--log-file" not in argv


def test_image_path_quotes_a_path_with_spaces():
    # an unquoted service path lets C:\Program.exe be started as
    # LocalSystem instead of the intended program.
    quoted = image_path([r"C:\Program Files\cronstable\cronstable.exe", "-c"])
    assert quoted.startswith('"C:\\Program Files\\cronstable')


# ---------------------------------------------------------------------------
# Controls and status
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "control, expected",
    [
        (SERVICE_CONTROL_STOP, "stop"),
        (SERVICE_CONTROL_SHUTDOWN, "stop"),
        (SERVICE_CONTROL_PRESHUTDOWN, "stop"),
        (SERVICE_CONTROL_INTERROGATE, "report"),
        (0x20, "unhandled"),
    ],
)
def test_control_action(control, expected):
    assert control_action(control) == expected


def test_accepted_controls_is_zero_while_pending():
    # what makes the SCM refuse a stop during startup, which removes the
    # "stop arrived before the loop existed" race rather than narrowing it.
    assert accepted_controls(SERVICE_START_PENDING) == 0
    assert accepted_controls(SERVICE_STOP_PENDING) == 0
    assert accepted_controls(SERVICE_STOPPED) == 0


def test_accepted_controls_when_running_includes_preshutdown():
    # preshutdown is why the handler is registered in its extended form: a
    # plain shutdown grants about five seconds, preshutdown far more, and
    # this daemon drains running jobs on the way out.
    mask = accepted_controls(SERVICE_RUNNING)
    assert mask & SERVICE_ACCEPT_STOP
    assert mask & SERVICE_ACCEPT_SHUTDOWN
    assert mask & SERVICE_ACCEPT_PRESHUTDOWN


def test_status_fields_order_and_service_type():
    fields = status_fields(
        SERVICE_START_PENDING, checkpoint=3, wait_hint_ms=10_000
    )
    assert len(fields) == 7
    # a report carrying the wrong service type is ignored by the SCM, which
    # looks exactly like a service that never reported at all.
    assert fields[0] == SERVICE_WIN32_OWN_PROCESS
    assert fields[1] == SERVICE_START_PENDING
    assert fields[5] == 3
    assert fields[6] == 10_000


def test_status_fields_clears_progress_in_a_settled_state():
    fields = status_fields(SERVICE_RUNNING, checkpoint=9, wait_hint_ms=5000)
    assert fields[5] == 0 and fields[6] == 0


def test_start_type_code_separates_delayed_from_the_type():
    assert start_type_code("auto") == (SERVICE_AUTO_START, False)
    assert start_type_code("delayed") == (SERVICE_AUTO_START, True)
    assert start_type_code("demand") == (SERVICE_DEMAND_START, False)


def test_failure_actions_plan_uses_milliseconds_for_the_delay():
    # the trap this function exists for: an SC_ACTION delay is in
    # MILLISECONDS while the reset period is in SECONDS, so passing 60
    # straight through would be a 60 millisecond restart loop.
    actions, reset_s = failure_actions_plan(60.0)
    assert actions[0] == (winservice.SC_ACTION_RESTART, 60_000)
    assert actions[1] == (winservice.SC_ACTION_RESTART, 60_000)
    assert actions[2] == (winservice.SC_ACTION_NONE, 0)
    assert reset_s == 86_400


# ---------------------------------------------------------------------------
# Paths and messages
# ---------------------------------------------------------------------------


def test_bootstrap_log_path_prefers_an_explicit_file():
    path = bootstrap_log_path(
        "cfg", "custom.log", config_is_dir=True, program_data=None
    )
    assert path == os.path.abspath("custom.log")


def test_bootstrap_log_path_sits_beside_a_config_directory():
    path = bootstrap_log_path(
        r"C:\ProgramData\cronstable",
        None,
        config_is_dir=True,
        program_data=None,
    )
    assert path.endswith(os.path.join("cronstable", "logs",
                                      "cronstable-service.log"))


def test_bootstrap_log_path_uses_the_parent_of_a_config_file():
    path = bootstrap_log_path(
        os.path.join("cfgdir", "cronstable.yaml"),
        None,
        config_is_dir=False,
        program_data=None,
    )
    assert os.path.dirname(path).endswith(os.path.join("cfgdir", "logs"))


def test_bootstrap_log_path_falls_back_to_program_data():
    path = bootstrap_log_path(
        "", None, config_is_dir=False, program_data=r"D:\PD"
    )
    assert path.startswith(r"D:\PD")


def test_config_is_user_scoped():
    assert config_is_user_scoped(r"C:\Users\p\cfg", r"C:\Users\p")
    assert not config_is_user_scoped(r"C:\ProgramData\cs", r"C:\Users\p")
    assert not config_is_user_scoped(r"C:\Users\p\cfg", None)


def test_describe_error_names_the_fix_not_the_number():
    assert "elevated" in describe_error(ERROR_ACCESS_DENIED, "install")
    assert "handle" in describe_error(
        ERROR_SERVICE_MARKED_FOR_DELETE, "install"
    )
    # an unmapped code still produces a sentence rather than a traceback
    assert "1234" in describe_error(1234, "install")


def test_display_name_distinguishes_extra_instances():
    assert display_name("cronstable") == "cronstable scheduler"
    assert "second" in display_name("second")


# ---------------------------------------------------------------------------
# The host, driven by a recording double
# ---------------------------------------------------------------------------


class _FakeApi:
    """Records every OS call the host would make."""

    def __init__(self, *, dispatch_error=None):
        self.statuses = []
        self.handler = None
        self.dispatch_error = dispatch_error
        self.console = []
        self.alloc_ok = True

    # the service side
    def dispatch_service(self, name, service_main):
        if self.dispatch_error is not None:
            raise ServiceError("no", winerror=self.dispatch_error)
        service_main([name])

    def register_handler(self, name, handler):
        self.handler = handler

    def set_status(self, fields):
        self.statuses.append(tuple(fields))

    # the console side
    def alloc_console(self):
        self.console.append("alloc")
        return self.alloc_ok

    def detach_std_handles(self):
        self.console.append("detach")

    def swallow_console_events(self, on_shutdown):
        self.console.append("handler")

    def states(self):
        return [fields[1] for fields in self.statuses]


class _FakeLoop:
    def __init__(self):
        self.callbacks = []
        self.threadsafe = []
        self.closed = False

    def call_soon(self, callback):
        self.callbacks.append(callback)

    def call_soon_threadsafe(self, callback):
        self.threadsafe.append(callback)

    def close(self):
        self.closed = True


class _FakeCron:
    def __init__(self, config):
        self.config = config
        self.signalled = 0

    def signal_shutdown(self):
        self.signalled += 1


def _args(**over):
    values = {
        "name": "cronstable",
        "config": "cfg",
        "log_file": None,
        "no_log_file": True,
        "console": False,
        "service_command": "run",
    }
    values.update(over)
    return types.SimpleNamespace(**values)


def _host(monkeypatch, api, *, during=None, cron_error=None):
    loop = _FakeLoop()
    seen = {}

    class _Cron(_FakeCron):
        def __init__(self, config):
            if cron_error is not None:
                raise cron_error
            super().__init__(config)
            seen["cron"] = self

    monkeypatch.setattr("cronstable.cron.Cron", _Cron)

    def run_daemon(cron, run_loop, *, shutdown_handlers=True):
        seen["shutdown_handlers"] = shutdown_handlers
        for callback in list(run_loop.callbacks):
            callback()
        if during is not None:
            during()

    host = ServiceHost(
        _args(),
        run_daemon=run_daemon,
        new_event_loop=lambda: loop,
        api=api,
    )
    return host, loop, seen


def test_host_reports_the_full_status_sequence(monkeypatch):
    api = _FakeApi()
    host, loop, seen = _host(monkeypatch, api)
    assert host.run() == 0
    assert api.states() == [
        SERVICE_START_PENDING,
        SERVICE_RUNNING,
        SERVICE_STOPPED,
    ]
    assert loop.closed


def test_host_never_installs_the_console_shutdown_handlers(monkeypatch):
    # install_shutdown_handlers reaches signal.signal, which the
    # interpreter refuses off the main thread, and ServiceMain is not the
    # main thread. The service's stop surface is the SCM control instead.
    api = _FakeApi()
    host, _loop, seen = _host(monkeypatch, api)
    host.run()
    assert seen["shutdown_handlers"] is False


def test_host_stop_control_drains_through_the_loop(monkeypatch):
    api = _FakeApi()
    captured = {}

    def during():
        captured["result"] = api.handler(SERVICE_CONTROL_STOP, 0, 0, 0)

    host, loop, seen = _host(monkeypatch, api, during=during)
    host.run()
    assert captured["result"] == winservice.NO_ERROR
    assert SERVICE_STOP_PENDING in api.states()
    # the handler must not do the draining itself: it hands the signal to
    # the loop thread and returns, or the SCM reports the service hung.
    assert loop.threadsafe == [seen["cron"].signal_shutdown]


def test_host_interrogate_is_answered_without_stopping(monkeypatch):
    api = _FakeApi()
    seen_codes = {}

    def during():
        seen_codes["code"] = api.handler(
            SERVICE_CONTROL_INTERROGATE, 0, 0, 0
        )

    host, loop, _ = _host(monkeypatch, api, during=during)
    host.run()
    assert seen_codes["code"] == winservice.NO_ERROR
    assert SERVICE_STOP_PENDING not in api.states()
    assert loop.threadsafe == []


def test_host_unknown_control_reports_not_implemented(monkeypatch):
    api = _FakeApi()
    seen_codes = {}

    def during():
        seen_codes["code"] = api.handler(0x20, 0, 0, 0)

    host, _loop, _ = _host(monkeypatch, api, during=during)
    host.run()
    assert seen_codes["code"] == winservice.ERROR_CALL_NOT_IMPLEMENTED


def test_host_reports_a_config_failure_as_a_specific_exit_code(monkeypatch):
    from cronstable.config import ConfigError

    api = _FakeApi()
    host, _loop, _ = _host(
        monkeypatch, api, cron_error=ConfigError("bad yaml")
    )
    assert host.run() == 1
    final = api.statuses[-1]
    assert final[1] == SERVICE_STOPPED
    assert final[3] == ERROR_SERVICE_SPECIFIC_ERROR
    assert final[4] == winservice.EXIT_CONFIG_FAILED


def test_host_reports_a_run_failure_as_a_specific_exit_code(monkeypatch):
    api = _FakeApi()

    def during():
        raise RuntimeError("the scheduler fell over")

    host, _loop, _ = _host(monkeypatch, api, during=during)
    assert host.run() == 1
    final = api.statuses[-1]
    assert final[3] == ERROR_SERVICE_SPECIFIC_ERROR
    assert final[4] == winservice.EXIT_RUN_FAILED


def test_host_final_report_is_never_overwritten_by_the_pumper(monkeypatch):
    # the pumper is stopped AND joined before the terminal report; a late
    # tick would put the service back into a pending state after STOPPED
    # and the SCM would wait out the whole wait hint on a dead process.
    api = _FakeApi()
    host, _loop, _ = _host(monkeypatch, api)
    host.run()
    assert api.states()[-1] == SERVICE_STOPPED
    assert host._pump is None


def test_host_run_by_hand_explains_itself(monkeypatch, capsys):
    api = _FakeApi(
        dispatch_error=ERROR_FAILED_SERVICE_CONTROLLER_CONNECT
    )
    host, _loop, _ = _host(monkeypatch, api)
    assert host.run() == 2
    err = capsys.readouterr().err
    assert "started by the Windows Service Control Manager" in err


def test_host_console_is_off_unless_asked(monkeypatch):
    api = _FakeApi()
    host, _loop, _ = _host(monkeypatch, api)
    host.run()
    assert api.console == []


def test_host_console_detaches_the_inherited_std_handles(monkeypatch):
    # a freshly allocated console hands the process live std handles, and a
    # job spawned without capture would inherit them, gaining a console
    # stdin that blocks instead of reaching end of file.
    api = _FakeApi()
    loop = _FakeLoop()
    monkeypatch.setattr("cronstable.cron.Cron", _FakeCron)
    host = ServiceHost(
        _args(console=True),
        run_daemon=lambda cron, run_loop, **kw: None,
        new_event_loop=lambda: loop,
        api=api,
    )
    host.run()
    assert api.console == ["alloc", "detach", "handler"]


def test_host_survives_a_console_it_cannot_allocate(monkeypatch, caplog):
    api = _FakeApi()
    api.alloc_ok = False
    loop = _FakeLoop()
    monkeypatch.setattr("cronstable.cron.Cron", _FakeCron)
    host = ServiceHost(
        _args(console=True),
        run_daemon=lambda cron, run_loop, **kw: None,
        new_event_loop=lambda: loop,
        api=api,
    )
    host.run()
    assert api.console == ["alloc"]
    assert "killTimeout" in caplog.text


# ---------------------------------------------------------------------------
# dispatch and the control verbs
# ---------------------------------------------------------------------------


def test_dispatch_refuses_on_posix(monkeypatch, capsys):
    monkeypatch.setattr(platform, "IS_WINDOWS", False)
    code = winservice.dispatch(
        _args(service_command="status"),
        run_daemon=lambda *a, **k: None,
        new_event_loop=lambda: None,
    )
    assert code == 2
    assert "only on Windows" in capsys.readouterr().err


def test_dispatch_without_an_action_names_them(monkeypatch, capsys):
    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    code = winservice.dispatch(
        _args(service_command=None),
        run_daemon=lambda *a, **k: None,
        new_event_loop=lambda: None,
        api=_FakeApi(),
    )
    assert code == 2
    assert "install" in capsys.readouterr().err


def test_install_refuses_a_one_file_binary(monkeypatch, capsys):
    monkeypatch.setattr(winservice.sys, "executable", r"C:\app\cs.exe")
    monkeypatch.setattr(
        winservice.sys, "_MEIPASS", r"C:\Temp\_MEI1", raising=False
    )
    code = winservice.install(_args(config="cfg"), _FakeApi())
    assert code == 2
    out = capsys.readouterr().err
    assert "one-file build cannot host a Windows service" in out
    assert "pip" in out
    # The pointer to the shipped one-directory artifact, so the message
    # cannot regress to pip/pipx being the only named way out.
    assert "zip" in out


def test_install_refuses_a_missing_config(monkeypatch, capsys, tmp_path):
    monkeypatch.delattr(winservice.sys, "_MEIPASS", raising=False)
    code = winservice.install(
        _args(config=str(tmp_path / "nope")), _FakeApi()
    )
    assert code == 2
    assert "does not exist" in capsys.readouterr().err


def test_install_refuses_the_accidental_per_user_default(
    monkeypatch, capsys, tmp_path
):
    # left at the platform default, a per-user path is not a choice: a
    # service running as LocalSystem resolves that same default elsewhere,
    # so the installed service would schedule nothing.
    monkeypatch.delattr(winservice.sys, "_MEIPASS", raising=False)
    profile = tmp_path / "profile"
    config = profile / "cronstable"
    config.mkdir(parents=True)
    monkeypatch.setenv("USERPROFILE", str(profile))
    monkeypatch.setattr(platform, "DEFAULT_CONFIG_PATH", str(config))
    code = winservice.install(_args(config=str(config)), _FakeApi())
    assert code == 2
    assert "schedule nothing" in capsys.readouterr().err


class _RecordingInstallApi(_FakeApi):
    def __init__(self):
        super().__init__()
        self.created = None
        self.described = None
        self.delayed = None
        self.actions = None
        self.flag = None

    def create_service(self, *, name, display, image, start_type):
        self.created = (name, display, image, start_type)

    def set_description(self, name, text):
        self.described = text

    def set_delayed_auto_start(self, name, delayed):
        self.delayed = delayed

    def set_failure_actions(self, name, actions, reset_s):
        self.actions = (actions, reset_s)

    def set_failure_actions_flag(self, name, on):
        self.flag = on


def test_install_configures_recovery_and_its_trigger(
    monkeypatch, tmp_path, capsys
):
    # the flag matters as much as the actions: without it recovery fires
    # only on a crash, and this host reports its failures as a clean stop
    # carrying an exit code, which would never trigger them.
    monkeypatch.delattr(winservice.sys, "_MEIPASS", raising=False)
    monkeypatch.setattr(platform, "DEFAULT_CONFIG_PATH", r"C:\elsewhere")
    monkeypatch.delenv("USERPROFILE", raising=False)
    api = _RecordingInstallApi()
    args = _args(
        config=str(tmp_path),
        start_type="delayed",
        log_level="DEBUG",
        restart_delay=30.0,
        no_restart=False,
    )
    assert winservice.install(args, api) == 0
    assert api.created[3] == SERVICE_AUTO_START
    assert api.delayed is True
    assert api.actions[0][0] == (winservice.SC_ACTION_RESTART, 30_000)
    assert api.flag is True
    assert "cronstable" in api.described


def test_install_no_restart_skips_recovery(monkeypatch, tmp_path):
    monkeypatch.delattr(winservice.sys, "_MEIPASS", raising=False)
    monkeypatch.setattr(platform, "DEFAULT_CONFIG_PATH", r"C:\elsewhere")
    monkeypatch.delenv("USERPROFILE", raising=False)
    api = _RecordingInstallApi()
    args = _args(
        config=str(tmp_path),
        start_type="auto",
        log_level="INFO",
        restart_delay=60.0,
        no_restart=True,
    )
    assert winservice.install(args, api) == 0
    assert api.actions is None
    assert api.flag is None


def test_install_translates_a_win32_failure(monkeypatch, tmp_path, capsys):
    monkeypatch.delattr(winservice.sys, "_MEIPASS", raising=False)
    monkeypatch.setattr(platform, "DEFAULT_CONFIG_PATH", r"C:\elsewhere")
    monkeypatch.delenv("USERPROFILE", raising=False)

    class _Denied(_RecordingInstallApi):
        def create_service(self, **kwargs):
            raise ServiceError(
                describe_error(ERROR_ACCESS_DENIED, "install the service"),
                winerror=ERROR_ACCESS_DENIED,
            )

    args = _args(
        config=str(tmp_path),
        start_type="auto",
        log_level="INFO",
        restart_delay=60.0,
        no_restart=True,
    )
    assert winservice.install(args, _Denied()) == 1
    assert "elevated" in capsys.readouterr().err


class _StateApi(_FakeApi):
    def __init__(self, states):
        super().__init__()
        self.queue = list(states)
        self.controlled = []
        self.started = 0
        self.deleted = 0

    def query_status(self, name):
        state = self.queue.pop(0) if self.queue else SERVICE_STOPPED
        return (SERVICE_WIN32_OWN_PROCESS, state, 0, 0, 0, 0, 0, 4242, 0)

    def control_service(self, name, control):
        self.controlled.append(control)

    def start_service(self, name):
        self.started += 1

    def delete_service(self, name):
        self.deleted += 1


def test_status_prints_the_state_and_pid(capsys):
    api = _StateApi([SERVICE_RUNNING])
    assert winservice.status(_args(), api) == 0
    out = capsys.readouterr().out
    assert "running" in out and "4242" in out


def test_status_explains_a_service_specific_exit(capsys):
    class _Failed(_FakeApi):
        def query_status(self, name):
            return (
                SERVICE_WIN32_OWN_PROCESS,
                SERVICE_STOPPED,
                0,
                ERROR_SERVICE_SPECIFIC_ERROR,
                winservice.EXIT_CONFIG_FAILED,
                0,
                0,
                0,
                0,
            )

    assert winservice.status(_args(), _Failed()) == 0
    assert "configuration did not parse" in capsys.readouterr().out


def test_stop_waits_for_the_drain():
    # a scheduler with a two hour job is draining, not hung, so the default
    # is to wait rather than to declare failure.
    api = _StateApi([SERVICE_RUNNING, SERVICE_STOP_PENDING, SERVICE_STOPPED])
    assert winservice.stop(_args(timeout=0.0), api) == 0
    assert api.controlled == [SERVICE_CONTROL_STOP]


def test_remove_tolerates_an_already_stopped_service():
    class _NotActive(_StateApi):
        def control_service(self, name, control):
            raise ServiceError(
                "not running", winerror=winservice.ERROR_SERVICE_NOT_ACTIVE
            )

    api = _NotActive([SERVICE_STOPPED])
    assert winservice.remove(_args(), api) == 0
    assert api.deleted == 1


def test_start_reports_a_service_that_never_comes_up(capsys):
    api = _StateApi([SERVICE_START_PENDING] * 50)
    assert winservice.start(_args(timeout=0.3), api) == 1
    assert "did not report running" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Windows only
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="talks to the real SCM"
)
def test_query_status_of_a_missing_service_is_translated():
    # Also the regression guard for the handle-truncation trap: without an
    # explicit argtypes declaration, CloseServiceHandle on this error path
    # raised OverflowError instead of producing a message.
    with pytest.raises(ServiceError) as caught:
        WinApiUnderTest().query_status("cronstable-definitely-not-installed")
    assert "no service of that name" in str(caught.value)


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="talks to the real SCM"
)
def test_query_status_of_a_service_every_windows_host_has():
    # Dnscache exists on every Windows install and needs no elevation to
    # read, so this proves the SERVICE_STATUS_PROCESS layout is right.
    fields = WinApiUnderTest().query_status("Dnscache")
    assert len(fields) == 9
    assert fields[0] & SERVICE_WIN32_OWN_PROCESS or fields[0]
    assert fields[1] in (
        SERVICE_STOPPED,
        SERVICE_START_PENDING,
        SERVICE_STOP_PENDING,
        SERVICE_RUNNING,
    )


def WinApiUnderTest():
    return winservice.WinApi()
