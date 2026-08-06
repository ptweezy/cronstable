import asyncio
import logging
import sys
import threading
from pathlib import Path

import pytest

import cronstable.__main__
import cronstable.__main__ as main
import cronstable.version
from cronstable.config import parse_config
from cronstable.cron import ConfigError
from cronstable.fingerprint import SCHEME_VERSION


class FakeCron:
    def __init__(self, config_arg):
        parse_config(config_arg)

    async def run(self):
        return

    def signal_shutdown(self):
        pass


class ExitError(RuntimeError):
    pass


def exit(num):
    raise ExitError(num)


def test_good_config(monkeypatch):
    loop = asyncio.new_event_loop()
    # main_loop imports Cron lazily (from cronstable.cron, inside the function)
    # so a job-facing CLI call never drags in the daemon graph; patch it at its
    # source module, not on cronstable.__main__.
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    cronstable.__main__.main_loop(loop)


def test_broken_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "testbrokenconfig.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)


def test_missing_config(monkeypatch):
    loop = asyncio.new_event_loop()
    monkeypatch.setattr("cronstable.cron.Cron", FakeCron)
    config_file = str(Path(__file__).parent / "doesnotexist.yaml")
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", config_file])
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)


def test_job_set_id_flag(monkeypatch, capsys):
    # uses the real Cron so the printed id reflects the parsed config
    loop = asyncio.new_event_loop()
    config_file = str(Path(__file__).parent / "testconfig.yaml")
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "-c", config_file, "--job-set-id"]
    )
    monkeypatch.setattr(sys, "exit", exit)
    with pytest.raises(ExitError):
        cronstable.__main__.main_loop(loop)
    out = capsys.readouterr().out.strip()
    assert out.startswith(SCHEME_VERSION + ":")
    assert len(out.split(":", 1)[1]) == 64


# --- main_loop arg handling and dispatch routing ---
# Exercises the CLI entry's portable branches: --version, the `--`
# trailing-command split (both the `lock run` success path and the error
# path), the state / cursor / mcp / tui routing branches, the missing-default-
# config exit, and the Cron-backed --job-set-id / --validate-config /
# run-and-shutdown wiring. Each daemon-graph import is patched at its source
# module (as the tests above do with cronstable.cron.Cron) so a job-facing
# branch never drags in the real daemon.


def _loop():
    return asyncio.new_event_loop()


def test_version_prints_and_exits(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["cronstable", "--version"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == cronstable.version.version


def test_third_party_licenses_prints_and_exits(monkeypatch, capsys):
    # The notice is package data so it rides inside every artifact; this
    # also proves importlib.resources can actually reach it (a rename or
    # a package-data regression would fail here, long before a frozen
    # binary ships without its LGPL notice).
    monkeypatch.setattr(sys, "argv", ["cronstable", "--third-party-licenses"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "python-zeroconf" in out
    assert "GNU LESSER GENERAL PUBLIC LICENSE" in out


def test_trailing_dashdash_without_lock_run_errors(monkeypatch, capsys):
    # `--` before anything other than a `lock run` command is rejected by the
    # hand-rolled split (argparse would already have exited otherwise).
    monkeypatch.setattr(sys, "argv", ["cronstable", "--", "echo", "hi"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 2  # argparse's parser.error() exit status
    assert "only valid before a `lock run`" in capsys.readouterr().err


def test_bad_log_level_is_a_clean_usage_error(monkeypatch, capsys):
    # a typo'd level used to escape as an AttributeError traceback out of a
    # bare getattr(logging, ...); it must exit as an argparse usage error.
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "--log-level", "VERBOSE", "--version"]
    )
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 2  # argparse's parser.error() exit status
    assert "invalid --log-level 'VERBOSE'" in capsys.readouterr().err


def test_log_level_accepts_any_case_and_aliases(monkeypatch, capsys):
    # the WARN/FATAL aliases resolved under the old getattr by accident of
    # the logging module's namespace: keep them working. Lowercase never
    # worked (getattr found the logging.debug FUNCTION and basicConfig
    # blew up on it); the lenient upper-casing makes it work now.
    for spelling in ("debug", "WARN", "Error"):
        monkeypatch.setattr(
            sys, "argv", ["cronstable", "-l", spelling, "--version"]
        )
        with pytest.raises(SystemExit) as exc:
            main.main_loop(_loop())
        assert exc.value.code == 0, spelling
        capsys.readouterr()


def test_state_get_routes_to_jobcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["state_command"] = args.state_command
        return 7

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "get", "mykey"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 7
    assert seen == {"command": "state", "state_command": "get"}


def test_state_check_routes_to_state_admin(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["state_command"] = args.state_command
        return 3

    monkeypatch.setattr("cronstable.state_admin.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "check"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 3
    assert seen == {"command": "state", "state_command": "check"}


def test_cursor_routes_to_jobcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 5

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "cursor", "get", "wm"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 5
    assert seen["command"] == "cursor"


def test_lock_run_trailing_command_captured(monkeypatch):
    # The `lock run NAME -- CMD...` success path: the tokens after `--` are
    # split off and stored on args.run_command before dispatch.
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        seen["lock_command"] = args.lock_command
        seen["run_command"] = args.run_command
        return 0

    monkeypatch.setattr("cronstable.jobcli.dispatch", fake_dispatch)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cronstable", "lock", "run", "mylock", "--", "echo", "hi"],
    )
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "lock"
    assert seen["lock_command"] == "run"
    assert seen["run_command"] == ["echo", "hi"]


def test_mcp_routes_to_mcpcli(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 0

    monkeypatch.setattr("cronstable.mcpcli.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "mcp"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "mcp"


def test_tui_routes_to_tui(monkeypatch):
    seen = {}

    def fake_dispatch(args):
        seen["command"] = args.command
        return 0

    monkeypatch.setattr("cronstable.tui.dispatch", fake_dispatch)
    monkeypatch.setattr(sys, "argv", ["cronstable", "tui"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert seen["command"] == "tui"


def test_missing_default_config_exits_1(monkeypatch, capsys):
    # No -c given (args.config stays at CONFIG_DEFAULT) and the default path
    # does not exist -> print an error, dump help, exit 1.
    monkeypatch.setattr("cronstable.__main__.os.path.exists", lambda p: False)
    monkeypatch.setattr(sys, "argv", ["cronstable"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "configuration file not found" in err
    # the error names the path it looked in and the way out; before it did
    # neither, and the reader had to find the default in the wiki.
    assert main.CONFIG_DEFAULT in err
    assert "cronstable init" in err


def test_init_writes_a_starter_the_daemon_can_load(
    monkeypatch, tmp_path, capsys
):
    target = tmp_path / "confdir"
    monkeypatch.setattr(sys, "argv", ["cronstable", "init", str(target)])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    written = target / "cronstable.yaml"
    assert written.is_file()
    out = capsys.readouterr().out
    assert str(written) in out
    assert "cronstable -c" in out  # a non-default target needs the flag
    # the starter is a real, loadable config carrying the hello job
    conf = parse_config(str(target))
    assert [job.name for job in conf.jobs] == ["hello"]


def test_init_refuses_a_directory_with_existing_config(
    monkeypatch, tmp_path, capsys
):
    target = tmp_path / "confdir"
    target.mkdir()
    (target / "live.yaml").write_text("jobs: []\n")
    monkeypatch.setattr(sys, "argv", ["cronstable", "init", str(target)])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 1
    assert "live.yaml" in capsys.readouterr().err
    assert not (target / "cronstable.yaml").exists()


def test_init_defaults_to_the_platform_config_directory(
    monkeypatch, tmp_path, capsys
):
    fake_default = str(tmp_path / "default-confdir")
    monkeypatch.setattr(main, "CONFIG_DEFAULT", fake_default)
    monkeypatch.setattr(sys, "argv", ["cronstable", "init"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert (tmp_path / "default-confdir" / "cronstable.yaml").is_file()
    # the default location needs no -c to start
    out = capsys.readouterr().out
    assert "start the scheduler with: cronstable\n" in out


def test_config_error_exits_1(monkeypatch):
    # A ConfigError from constructing Cron is caught and turned into exit 1.
    class BadCron:
        def __init__(self, config):
            raise ConfigError("boom")

    monkeypatch.setattr("cronstable.cron.Cron", BadCron)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 1


def _fake_parsed_config(jobs=()):
    """Stand-in for a successful parse_config_with_sources() call.

    --job-set-id and --validate-config answer straight from the config
    parser and never construct a Cron (building the whole daemon graph to
    answer a config question doubled the runtime of both flags), so these
    tests stub the parse rather than the scheduler.
    """

    class FakeConfig:
        def __init__(self):
            self.jobs = list(jobs)

    return lambda config_arg: (FakeConfig(), frozenset())


def test_job_set_id_prints_and_exits(monkeypatch, capsys):
    monkeypatch.setattr(
        "cronstable.config.parse_config_with_sources", _fake_parsed_config()
    )
    monkeypatch.setattr(
        "cronstable.fingerprint.job_set_id", lambda jobs: "deadbeef"
    )
    monkeypatch.setattr(
        sys, "argv", ["cronstable", "-c", "config.yaml", "--job-set-id"]
    )
    with pytest.raises(SystemExit) as exc:
        main.main_loop(_loop())
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == "deadbeef"


def test_validate_config_exits_0(monkeypatch, caplog):
    monkeypatch.setattr(
        "cronstable.config.parse_config_with_sources", _fake_parsed_config()
    )
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml", "-v"])
    with caplog.at_level(logging.INFO, logger="cronstable"):
        with pytest.raises(SystemExit) as exc:
            main.main_loop(_loop())
    assert exc.value.code == 0
    assert "Configuration is valid." in caplog.text


def test_validate_config_reports_a_config_error_and_exits_1(
    monkeypatch, caplog
):
    # The parse error must still surface with the same message and exit code
    # now that the flag no longer goes through Cron.
    from cronstable.config import ConfigError

    def boom(config_arg):
        raise ConfigError("bad schedule")

    monkeypatch.setattr("cronstable.config.parse_config_with_sources", boom)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml", "-v"])
    with caplog.at_level(logging.INFO, logger="cronstable"):
        with pytest.raises(SystemExit) as exc:
            main.main_loop(_loop())
    assert exc.value.code == 1
    assert "Configuration error: bad schedule" in caplog.text


def test_main_loop_builds_and_closes_its_own_loop(monkeypatch):
    # No loop passed (how main() calls it now): one is built for the daemon
    # branch and closed again, so every branch that exits earlier -- --version,
    # --third-party-licenses, the job-facing thin clients -- never builds one
    # and never imports asyncio at all.
    built = []

    class RunCron:
        def __init__(self, config):
            pass

        async def run(self):
            pass

        def signal_shutdown(self):
            pass

    def fake_new_event_loop():
        loop = asyncio.new_event_loop()
        built.append(loop)
        return loop

    monkeypatch.setattr("cronstable.cron.Cron", RunCron)
    monkeypatch.setattr(main, "_new_event_loop", fake_new_event_loop)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml"])
    main.main_loop()
    assert len(built) == 1
    assert built[0].is_closed()


def test_version_never_builds_an_event_loop(monkeypatch):
    # The pairing check for the above: an early-exit branch must not reach
    # _new_event_loop, which is where `import asyncio` now lives.
    def boom():  # pragma: no cover - the assertion is that it is not called
        raise AssertionError("--version must not build an event loop")

    monkeypatch.setattr(main, "_new_event_loop", boom)
    monkeypatch.setattr(sys, "argv", ["cronstable", "--version"])
    with pytest.raises(SystemExit) as exc:
        main.main_loop()
    assert exc.value.code == 0


def test_daemon_executor_is_sized_and_named():
    # The shared default pool carries the config reparse, the leadership lease
    # round-trip and every per-completion offload, so a 1-2 vCPU container must
    # not be left at CPython's cpu_count-derived 5-6 slots.
    assert 8 <= main.executor_workers() <= 32
    loop = asyncio.new_event_loop()
    try:
        main._install_default_executor(loop)
        name = loop.run_until_complete(
            loop.run_in_executor(None, lambda: threading.current_thread().name)
        )
    finally:
        loop.close()
    assert name.startswith(main.EXECUTOR_THREAD_PREFIX)


def test_run_and_shutdown_wiring(monkeypatch):
    # The daemon path: install shutdown handlers, drive cron.run() to
    # completion on the loop, then tear the handlers down in the finally.
    ran = {"value": False}

    class RunCron:
        def __init__(self, config):
            pass

        async def run(self):
            ran["value"] = True

        def signal_shutdown(self):
            pass

    monkeypatch.setattr("cronstable.cron.Cron", RunCron)
    monkeypatch.setattr(sys, "argv", ["cronstable", "-c", "config.yaml"])
    loop = asyncio.new_event_loop()
    try:
        # returns normally (no sys.exit) once cron.run() finishes
        main.main_loop(loop)
    finally:
        loop.close()
    assert ran["value"] is True
