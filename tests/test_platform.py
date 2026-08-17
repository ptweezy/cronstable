import asyncio
import logging
import os
import signal
import sys
import time

import pytest
from aiohttp import web

import cronstable.config
import cronstable.cron
from cronstable import platform


def test_encode_argv_matches_platform():
    argv = ["echo", "héllo"]
    encoded = platform.encode_argv(argv)
    if platform.IS_WINDOWS:
        # CreateProcessW takes str; bytes would break list2cmdline.
        assert encoded == argv
        assert all(isinstance(a, str) for a in encoded)
    else:
        # locale-independent UTF-8 argv on POSIX.
        assert encoded == [a.encode() for a in argv]
        assert all(isinstance(a, bytes) for a in encoded)


def test_default_shell_matches_platform():
    if platform.IS_WINDOWS:
        # empty -> route through create_subprocess_shell (cmd.exe /c)
        assert platform.DEFAULT_SHELL == ""
    else:
        assert platform.DEFAULT_SHELL == "/bin/sh"


def test_default_config_path_matches_platform():
    if platform.IS_WINDOWS:
        assert platform.DEFAULT_CONFIG_PATH.endswith("cronstable")
        assert platform.DEFAULT_CONFIG_PATH != "cronstable"  # has a parent dir
    else:
        assert platform.DEFAULT_CONFIG_PATH == "/etc/cronstable.d"


def test_windows_config_home_is_per_user_until_machine_wide_exists(tmp_path):
    # the historical default: roaming AppData, when no machine-wide
    # directory has been created.
    env = {
        "PROGRAMDATA": str(tmp_path / "ProgramData"),
        "APPDATA": str(tmp_path / "Roaming"),
    }
    assert platform._windows_config_home(env) == os.path.join(
        str(tmp_path / "Roaming"), "cronstable"
    )


@pytest.mark.parametrize(
    "filename", ["jobs.yaml", "jobs.yml", "nightly.crontab", "crontab"]
)
def test_windows_config_home_prefers_machine_wide(tmp_path, filename):
    # %ProgramData%\cronstable is the real analog of /etc/cronstable.d:
    # once an administrator populates it, every account (interactive or
    # service, whose APPDATA points into systemprofile) resolves to the
    # same machine-wide config. Every name the loader reads hands over.
    machine_wide = tmp_path / "ProgramData" / "cronstable"
    machine_wide.mkdir(parents=True)
    (machine_wide / filename).write_text("jobs: []\n")
    env = {
        "PROGRAMDATA": str(tmp_path / "ProgramData"),
        "APPDATA": str(tmp_path / "Roaming"),
    }
    assert platform._windows_config_home(env) == str(machine_wide)


@pytest.mark.parametrize(
    "leftover", [None, "README.md", "_disabled.yaml", ".hidden.yaml"]
)
def test_windows_config_home_ignores_machine_wide_without_config(
    tmp_path, leftover
):
    # Existing is not enough: an empty (or config-free) machine-wide
    # directory parses to zero jobs, and since the path DOES exist the
    # daemon's configuration-not-found guard stays quiet, so handing over
    # to it would silently stop a working per-user install from running
    # anything. An interrupted `init` or an uninstall that took the files
    # but not the folder is enough to leave one behind.
    machine_wide = tmp_path / "ProgramData" / "cronstable"
    machine_wide.mkdir(parents=True)
    if leftover is not None:
        # names the loader itself skips: not config, so not a handover
        (machine_wide / leftover).write_text("jobs: []\n")
    env = {
        "PROGRAMDATA": str(tmp_path / "ProgramData"),
        "APPDATA": str(tmp_path / "Roaming"),
    }
    assert platform._windows_config_home(env) == os.path.join(
        str(tmp_path / "Roaming"), "cronstable"
    )


def test_machine_wide_config_names_match_the_loader():
    # platform.py duplicates the config-directory filter rather than
    # importing config/crontabs (it resolves the default at import time and
    # has to stay cheap for thin clients). Pin the copies together: a new
    # extension on either side alone would either hand over to a directory
    # the loader ignores, or refuse one it would happily read.
    from cronstable import crontabs

    assert platform._CONFIG_BASENAME == crontabs.CRONTAB_BASENAME
    assert platform._CONFIG_EXTENSIONS == (
        crontabs.CRONTAB_EXTENSIONS | cronstable.config._YAML_EXTENSIONS
    )


@pytest.mark.parametrize(
    "filename",
    [
        # the plain forms, then the same names as a Windows editor may
        # spell them: case is preserved on that filesystem but carries no
        # meaning, so every one of these has to be judged identically by
        # the handover check and by the loader it speaks for.
        "jobs.yaml",
        "jobs.yml",
        "JOBS.YAML",
        "jobs.YAML",
        "Jobs.Yml",
        "nightly.crontab",
        "NIGHTLY.CRONTAB",
        "jobs.cron",
        "crontab",
        "CRONTAB",
        # and names neither side may claim
        "README.md",
        "notes.txt",
        "jobs.yaml.bak",
        "_disabled.yaml",
        ".hidden.yaml",
    ],
)
def test_holds_config_agrees_with_the_loader_per_name(tmp_path, filename):
    # The set equality above pins the VOCABULARY; this pins the decision.
    # A divergence here is silent in the worst way: _holds_config saying
    # yes where the loader says no hands the Windows default to a
    # directory that parses to zero jobs, and because the directory
    # exists the configuration-not-found guard never fires, so the daemon
    # comes up healthy and schedules nothing.
    directory = tmp_path / "cronstable"
    directory.mkdir()
    (directory / filename).write_text("jobs: []\n")

    assert platform._holds_config(str(directory)) == _loader_reads(
        str(directory)
    )


def _loader_reads(directory: str) -> bool:
    """Whether the real loader consults the single file in ``directory``.

    Asks _parse_config_dir itself rather than restating its filter, which
    would just be a third copy to drift.  The probe content (``jobs: []``)
    is deliberately readable by one front end and not the other: claimed
    as YAML it parses to zero jobs and the file lands in the reported
    sources, claimed as a crontab it is not a valid entry line and raises.
    Either outcome proves the loader opened it; only a skipped file
    produces neither.
    """
    try:
        _, sources = cronstable.config.parse_config_with_sources(directory)
    except cronstable.config.ConfigError:
        return True
    return bool(sources)


def test_windows_config_home_survives_bare_environments(tmp_path):
    # no APPDATA (a bare service account): fall back under the profile.
    got = platform._windows_config_home({})
    assert got.endswith("cronstable")
    assert os.path.dirname(got)  # anchored somewhere, never bare


def test_supports_unix_sockets_matches_platform():
    assert platform.supports_unix_sockets() == (not platform.IS_WINDOWS)


def test_new_process_group_kwargs_matches_platform():
    kwargs = platform.new_process_group_kwargs()
    if platform.IS_WINDOWS:
        # CREATE_NEW_PROCESS_GROUP: shields the job from the daemon
        # console's Ctrl-C, and makes the pid a CTRL_BREAK target.
        assert kwargs == {"creationflags": 0x00000200}
    else:
        assert kwargs == {"start_new_session": True}


# ---------------------------------------------------------------------------
# Job scheduling priority (the `priority:` job key).
#
# The Windows half is set at spawn, so it is tested through the flags;
# the POSIX half is a renice after the spawn, so it is tested through
# os.setpriority.  Both are driven with the `windows=` injection rather than
# by monkeypatching IS_WINDOWS, so each arm is measured on every box instead
# of only on the OS it ships for.
# ---------------------------------------------------------------------------

# Neither name exists on Windows, and the POSIX arm is exercised there too
# (windows=False), so the stand-in is what lets one test cover both.
_PRIO_PGRP = getattr(os, "PRIO_PGRP", 1)


def _record_setpriority(monkeypatch, calls, failing=None):
    def fake_setpriority(which, who, value):
        calls.append((which, who, value))
        if failing is not None:
            raise failing

    monkeypatch.setattr(os, "PRIO_PGRP", _PRIO_PGRP, raising=False)
    monkeypatch.setattr(os, "setpriority", fake_setpriority, raising=False)


@pytest.mark.parametrize(
    "level, expected_class",
    [
        pytest.param("idle", 0x00000040, id="idle"),
        pytest.param("below-normal", 0x00004000, id="below-normal"),
        # no class bit at all: CreateProcess defaults a child to NORMAL only
        # when the creator is not itself idle or below-normal, so emitting
        # NORMAL_PRIORITY_CLASS here would silently promote every job of a
        # daemon that was launched below normal.
        pytest.param("normal", 0x00000000, id="normal"),
        pytest.param("above-normal", 0x00008000, id="above-normal"),
        pytest.param("high", 0x00000080, id="high"),
    ],
)
def test_new_process_group_kwargs_ors_the_priority_class(
    level, expected_class
):
    kwargs = platform.new_process_group_kwargs(level, windows=True)
    flags = kwargs["creationflags"]
    # The process-group bit survives every level: it and the class are OR-ed
    # in one place, so a priority can never cost a job its group (and with
    # it the CTRL_BREAK target and the console shielding).
    assert flags & 0x00000200
    assert flags == 0x00000200 | expected_class
    # POSIX takes no flags from here at any level; the renice does that work.
    assert platform.new_process_group_kwargs(level, windows=False) == {
        "start_new_session": True
    }


def test_the_default_level_has_no_posix_nice_of_its_own():
    # DEFAULT_PRIORITY resolves to no number on purpose: it is never applied,
    # and nice 0 would misdescribe it (on a daemon started at nice 10,
    # `normal` means 10).  posix_nice_for is what the config layer's advisory
    # asks "is this a raise", so the None has to be the answer for it.
    assert platform.posix_nice_for(platform.DEFAULT_PRIORITY) is None
    assert platform.posix_nice_for("idle") == 19


def test_the_table_drift_guard_names_the_levels_that_drifted():
    # Asserting the real tables agree with the real vocabulary would be a
    # tautology: platform.py's import-time call raises before the module
    # finishes importing, so a drifted table fails collection of this whole
    # file rather than this assertion.  Drive the helper with a deliberately
    # drifted pair instead, which is the only way to see what it reports.
    assert (
        platform._priority_table_drift(
            ("idle", "normal"),
            "normal",
            {"idle": 0x40, "normal": 0x00},
            {"idle": 19},
        )
        == []
    )
    # a level in the vocabulary that no Windows row maps, and a POSIX row for
    # the one level that must never have one: both would KeyError at a spawn.
    drifted = (
        ("idle", "normal", "high"),
        "normal",
        {"idle": 0x40, "normal": 0x00},
        {"idle": 19, "normal": 0, "high": -10},
    )
    assert platform._priority_table_drift(*drifted) == ["high", "normal"]

    # and the import-time guard turns that into the RuntimeError that stops
    # the module loading, naming both levels so the fix is in the message.
    # The real call is the quiet one, so this arm has no other way to run.
    with pytest.raises(RuntimeError) as exc:
        platform._refuse_drifted_priority_tables(*drifted)
    assert "high, normal" in str(exc.value)
    assert (
        platform._refuse_drifted_priority_tables(
            ("idle", "normal"),
            "normal",
            {"idle": 0x40, "normal": 0x00},
            {"idle": 19},
        )
        is None
    )


@pytest.mark.parametrize(
    "level, expected_nice",
    [
        # Pinned as literals, because three doc tables publish these four
        # numbers; a typo in _POSIX_NICE would otherwise ship green and
        # silently contradict them.
        pytest.param("idle", 19, id="idle"),
        pytest.param("below-normal", 10, id="below-normal"),
        pytest.param("above-normal", -5, id="above-normal"),
        pytest.param("high", -10, id="high"),
    ],
)
def test_apply_priority_renices_the_whole_group(
    monkeypatch, level, expected_nice
):
    calls = []
    _record_setpriority(monkeypatch, calls)

    assert platform.apply_priority(4242, level, windows=False) is True
    # PRIO_PGRP, not PRIO_PROCESS: a descendant the shell forked in the
    # microseconds before the call has to be reniced with the leader.
    assert calls == [(_PRIO_PGRP, 4242, expected_nice)]
    # and the same number the config layer's raise/lowering test reads.
    assert platform.posix_nice_for(level) == expected_nice


def test_apply_priority_applies_nothing_at_the_default_or_on_windows(
    monkeypatch,
):
    calls = []
    _record_setpriority(monkeypatch, calls)

    # The default level is skipped, not applied as nice 0, which is what
    # keeps a config that says nothing spawning exactly as it always did.
    assert (
        platform.apply_priority(
            4242, platform.DEFAULT_PRIORITY, windows=False
        )
        is True
    )
    # Windows had its priority class set at CreateProcess time, so there is
    # nothing left to do after the spawn (and nothing that could reach the
    # descendants a raised class does not carry to anyway).
    assert platform.apply_priority(4242, "high", windows=True) is True
    assert calls == []


@pytest.mark.parametrize(
    "failing, expected",
    [
        # EPERM: no CAP_SYS_NICE and no RLIMIT_NICE headroom for the raise.
        pytest.param(PermissionError(1, "denied"), False, id="eperm"),
        # ESRCH: the group emptied between the spawn and the renice, so
        # nothing was refused and there is nothing to report.
        pytest.param(ProcessLookupError(3, "gone"), True, id="esrch"),
    ],
)
def test_apply_priority_never_raises_on_a_refusal(
    monkeypatch, caplog, failing, expected
):
    calls = []
    _record_setpriority(monkeypatch, calls, failing=failing)

    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        got = platform.apply_priority(4242, "high", windows=False)
    assert got is expected
    assert calls == [(_PRIO_PGRP, 4242, -10)]
    # A refusal never fails the run and never raises a WARNING: on a minutely
    # job that would be ~1,440 lines a day for a condition that does not
    # change until the deployment does.  The config layer says it once, at
    # load; here it is DEBUG for whoever goes looking.
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    said = [r for r in caplog.records if "priority" in r.getMessage()]
    assert bool(said) is (expected is False)
    # DEBUG exactly, not merely "below WARNING": three docs promise that
    # level by name, so a quiet promotion to INFO has to go red here.
    assert all(rec.levelno == logging.DEBUG for rec in said)


def test_creationflags_has_exactly_one_writer():
    # The whole reason new_process_group_kwargs takes a LEVEL rather than
    # handing callers a flag to OR in themselves: while it is the only place
    # that writes creationflags, the process-group bit and the priority
    # class cannot be set independently, so neither can be dropped by a
    # caller that meant to set the other.  A second writer somewhere in the
    # package would quietly retire that guarantee.
    package = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "cronstable",
    )
    writers = []
    for dirpath, _dirnames, filenames in os.walk(package):
        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding="utf-8") as fobj:
                for lineno, line in enumerate(fobj, 1):
                    if line.lstrip().startswith("#"):
                        continue  # prose about the key is not a writer
                    if '"creationflags"' in line or "creationflags=" in line:
                        writers.append(
                            (os.path.relpath(path, package), lineno)
                        )
    # Distinct FILES, not the per-occurrence list: the invariant is "one
    # writer module", so a second mention inside platform.py itself must not
    # turn it red and invite someone to weaken the guard.  `writers` stays as
    # the message, so a real second writer still names the file and line.
    assert sorted({name for name, _ in writers}) == ["platform.py"], writers


@pytest.mark.asyncio
async def test_kill_process_group_windows_break_first_taskkill_on_force(
    monkeypatch,
):
    # the Windows two-step mirrors the POSIX one: the non-forced call
    # delivers a trappable CTRL_BREAK to the job's process group (spawned
    # as its own group root by new_process_group_kwargs) and never
    # taskkills; the forced call is the taskkill /F /T tree kill.
    taskkills = []
    breaks = []

    async def fake_taskkill(pid):
        taskkills.append(pid)
        return True

    def fake_kill(pid, sig):
        breaks.append((pid, sig))

    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    monkeypatch.setattr(platform, "_taskkill_tree", fake_taskkill)
    monkeypatch.setattr(
        platform.signal, "CTRL_BREAK_EVENT", 99, raising=False
    )
    monkeypatch.setattr(platform.os, "kill", fake_kill)
    assert await platform.kill_process_group(4242, force=False) is True
    assert breaks == [(4242, 99)]
    assert taskkills == []
    assert await platform.kill_process_group(4242, force=True) is True
    assert taskkills == [4242]


@pytest.mark.asyncio
async def test_kill_process_group_windows_break_failure_tree_kills(
    monkeypatch,
):
    # regression, carried over from the pre-CTRL_BREAK design: where no
    # break can be delivered (no shared console, e.g. a service context),
    # the graceful call must not report False and leave the root to the
    # caller's direct-child terminate. The forced taskkill /T would then
    # run against a dead root, whose descendants are no longer in any
    # walkable tree; string-form commands run via cmd.exe /c, so the
    # actual workload is always such a descendant. The tree kill therefore
    # happens on the graceful call itself, while the root is alive to
    # anchor the walk.
    taskkills = []

    async def fake_taskkill(pid):
        taskkills.append(pid)
        return True

    def fail_kill(pid, sig):
        raise OSError("the handle is invalid (no console)")

    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    monkeypatch.setattr(platform, "_taskkill_tree", fake_taskkill)
    monkeypatch.setattr(
        platform.signal, "CTRL_BREAK_EVENT", 99, raising=False
    )
    monkeypatch.setattr(platform.os, "kill", fail_kill)
    assert await platform.kill_process_group(4242, force=False) is True
    assert taskkills == [4242]


def _windows_pid_alive(pid):
    # OpenProcess/GetExitCodeProcess, not a tasklist subprocess per poll: on a
    # degraded CI runner every process spawn crawled, and the polling in the
    # grandchild test below stalled past the 20-minute job timeout.
    import ctypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(
        PROCESS_QUERY_LIMITED_INFORMATION, False, pid
    )
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return False
        return code.value == STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def _windows_terminate(pid):
    # best-effort cleanup so a failing run doesn't leak the sleeping
    # grandchild for the rest of its 300s lifetime.
    import ctypes

    PROCESS_TERMINATE = 0x0001
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
    if handle:
        kernel32.TerminateProcess(handle, 1)
        kernel32.CloseHandle(handle)


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="Windows kill sequence is Windows-only"
)
@pytest.mark.asyncio
async def test_kill_process_group_windows_reaches_the_grandchild(tmp_path):
    # end to end on a real tree, spawned exactly as the daemon spawns a
    # string-form `command:` job (cmd.exe /c in its own process group, so
    # the workload is a grandchild of nothing we hold a handle to), taken
    # down with the same two-step sequence RunningJob.cancel runs: the
    # graceful break first, then the forced tree kill. Whichever step a
    # process survives, the sequence as a whole must reap the grandchild.
    #
    # The grandchild announces its own pid through a file rather than the
    # test walking the process tree with a Get-CimInstance poll: on a
    # degraded CI runner each PowerShell+WMI spawn took ~15s, so 50 polls
    # burned 13 minutes and outlived the previous 59s ping workload before
    # ever seeing it, timing out every Windows cell (2026-08-03).
    pid_file = tmp_path / "grandchild.pid"
    script = tmp_path / "grandchild.py"
    script.write_text(
        "import os, time\n"
        "tmp = r'{pf}' + '.tmp'\n"
        "open(tmp, 'w').write(str(os.getpid()))\n"
        "os.replace(tmp, r'{pf}')\n"
        "time.sleep(300)\n".format(pf=pid_file)
    )
    proc = await asyncio.create_subprocess_shell(
        '"{}" "{}"'.format(sys.executable, script),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        **platform.new_process_group_kwargs(),
    )
    grandchild = None
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:  # cmd needs a moment to spawn it
            if pid_file.exists():
                grandchild = int(pid_file.read_text())
                break
            await asyncio.sleep(0.1)
        assert grandchild is not None, (
            "the shell never spawned its python child"
        )
        assert grandchild != proc.pid  # a real descendant, not the shell

        # graceful step: CTRL_BREAK to the group (cmd.exe and a default
        # python both terminate on it; a process is free to trap it)
        assert await platform.kill_process_group(proc.pid, force=False)
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except asyncio.TimeoutError:
            pass
        # forced step: the taskkill /F /T tree kill (reports False when the
        # break already took the whole tree down, like taskkill's 128)
        forced = await platform.kill_process_group(proc.pid, force=True)
        assert forced or proc.returncode is not None
        await asyncio.wait_for(proc.wait(), 10)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:  # delivery is async either way
            if not _windows_pid_alive(grandchild):
                break
            await asyncio.sleep(0.1)
        assert not _windows_pid_alive(grandchild), (
            "descendant %d survived the kill sequence" % grandchild
        )
    finally:
        if grandchild is not None and _windows_pid_alive(grandchild):
            _windows_terminate(grandchild)
        if proc.returncode is None:
            proc.kill()


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="killpg / process groups are POSIX-only"
)
@pytest.mark.asyncio
async def test_kill_process_group_signals_the_group_then_reports_it_gone():
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(30)",
        **platform.new_process_group_kwargs(),
    )
    assert await platform.kill_process_group(proc.pid, force=True)
    await asyncio.wait_for(proc.wait(), 10)
    # the group is empty now: nothing was signalled, so the caller is told to
    # fall back rather than being left thinking the kill landed.
    assert not await platform.kill_process_group(proc.pid, force=True)


def test_config_uses_platform_default_shell():
    conf = cronstable.config.parse_config_string(
        """
jobs:
  - name: t
    command: echo hi
    schedule: "* * * * *"
""",
        "",
    )
    assert conf.jobs[0].shell == platform.DEFAULT_SHELL


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="user/group rejection is Windows-specific"
)
def test_user_group_rejected_on_windows():
    with pytest.raises(cronstable.config.ConfigError) as exc:
        cronstable.config.parse_config_string(
            """
jobs:
  - name: t
    command: echo hi
    schedule: "* * * * *"
    user: someuser
""",
            "",
        )
    assert "Windows" in str(exc.value)


def test_web_site_from_url_unix_socket():
    url = "unix:///tmp/cronstable.sock"
    if platform.IS_WINDOWS:
        # asyncio can't serve a unix socket on Windows: skipped as a bad entry
        # (raises before the runner is ever touched).
        with pytest.raises(ValueError):
            cronstable.cron.web_site_from_url(None, url)
    else:
        # POSIX: a unix listener is accepted. UnixSite.__init__ dereferences
        # runner.server, so pass a minimal stand-in instead of None.
        class _FakeRunner:
            server = object()

        site = cronstable.cron.web_site_from_url(_FakeRunner(), url)
        assert isinstance(site, web.UnixSite)


@pytest.mark.parametrize(
    "installer",
    [platform.install_shutdown_handlers, platform.install_reload_handler],
)
def test_install_handlers_roundtrip(installer):
    # Exercises install + the returned cleanup on both platforms (loop
    # signal handlers on POSIX; for shutdown on Windows the signal.signal +
    # console handler + heartbeat fallback, for reload a documented no-op)
    # without firing a real signal.  Must run on the main thread
    # (signal.signal requires it, and add_signal_handler refuses
    # elsewhere).
    loop = asyncio.new_event_loop()
    try:
        called = []
        cleanup = installer(loop, lambda: called.append(1))
        assert callable(cleanup)
        cleanup()
    finally:
        loop.close()


def _assert_signal_delivered(installer, sig):
    # shared end-to-end harness: a real signal raised while the loop is
    # parked must run the installed callback on the loop thread.
    loop = asyncio.new_event_loop()
    called = []
    cleanup = installer(loop, lambda: called.append(1))
    try:

        async def fire_and_park():
            signal.raise_signal(sig)
            # generous park: the Windows heartbeat only guarantees the
            # pending C handler is observed within its 0.25s tick.
            deadline = time.monotonic() + 5
            while not called and time.monotonic() < deadline:
                await asyncio.sleep(0.05)

        loop.run_until_complete(fire_and_park())
    finally:
        cleanup()
        loop.close()
    assert called == [1]


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="Windows signal-delivery path"
)
def test_windows_sigint_delivery_reaches_the_callback():
    # End to end through the signal.signal fallback and its heartbeat (the
    # docs' Ctrl-C promise; the POSIX sibling is test_cron.py's SIGTERM
    # test). Before this test existed, the only Windows coverage installed
    # the handlers without ever firing one.
    _assert_signal_delivered(
        platform.install_shutdown_handlers, signal.SIGINT
    )


def test_console_events_route_close_and_shutdown_to_the_drain():
    # CTRL_CLOSE / CTRL_SHUTDOWN are the daemon's to drain on (Python's
    # signal module never surfaces them); CTRL_C (0) and CTRL_BREAK (1)
    # must pass down the chain to the interpreter's own handler, which
    # turns them into the Python signals handled above.
    assert platform._console_event_requests_shutdown(2)  # close
    assert platform._console_event_requests_shutdown(6)  # shutdown
    assert not platform._console_event_requests_shutdown(0)  # Ctrl-C
    assert not platform._console_event_requests_shutdown(1)  # Ctrl-Break


def test_console_logoff_does_not_stop_the_daemon():
    # Only session-0 processes (services, the unattended install this is
    # for) receive CTRL_LOGOFF_EVENT, and it fires for ANY user's logoff
    # with no way to tell whose. Draining on it would stop the scheduler
    # the first time anyone signed out of the machine, so it must pass
    # through like Ctrl-C does.
    assert platform._CTRL_LOGOFF_EVENT == 5
    assert not platform._console_event_requests_shutdown(
        platform._CTRL_LOGOFF_EVENT
    )


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="GetTickCount64 path is Windows-only"
)
def test_os_boot_time_windows_returns_a_plausible_epoch():
    # The sole boot identity on Windows (there is no /proc boot_id), and
    # the value the @reboot once-per-boot dedupe compares. It must be a
    # real epoch in the past, within a plausible uptime, and stable across
    # calls (up to clock jitter far below the dedupe's 60s tolerance).
    first = platform.os_boot_time()
    assert isinstance(first, float)
    now = time.time()
    assert first < now  # booted before now
    assert now - first < 400 * 24 * 3600  # and within a plausible uptime
    second = platform.os_boot_time()
    assert second is not None
    assert abs(second - first) < 5.0


def test_nonblocking_lock_raises_on_contention(tmp_path):
    # blocking=False must surface contention as an immediate OSError (the
    # lock-fidelity probe DEPENDS on the second attempt failing; a store
    # where it succeeds has no-op locks). Two descriptors of one file
    # contend on both platforms: POSIX flock is per-open-file-description,
    # Windows byte-range locks are per-handle.
    path = tmp_path / "lockfile"
    path.write_bytes(b"\0")
    fd1 = os.open(str(path), os.O_RDWR)
    fd2 = os.open(str(path), os.O_RDWR)
    try:
        with platform.exclusive_file_lock(fd1, blocking=False):
            with pytest.raises(OSError):
                with platform.exclusive_file_lock(fd2, blocking=False):
                    pass
        # released: the second descriptor may now take it
        with platform.exclusive_file_lock(fd2, blocking=False):
            pass
    finally:
        os.close(fd1)
        os.close(fd2)


def test_pid_alive_own_and_bogus_pid():
    # our own process exists; a hugely out-of-range pid does not. None is
    # reserved for "cannot tell" (treated as dead by reconciliation, which
    # the per-process token already vouches for).
    assert platform.pid_alive(os.getpid()) is True
    assert platform.pid_alive(2**22 + 12345) in (False, None)
    assert platform.pid_alive(0) is None


def test_fsync_directory_on_existing_and_nested_dir(tmp_path):
    # must not raise for a plain existing dir, nor for a directory nested
    # several levels deep and freshly created in this same test (the case
    # that matters: a stream/namespace dir a state write just makedirs'd).
    platform.fsync_directory(str(tmp_path))
    nested = tmp_path / "a" / "b" / "c"
    nested.mkdir(parents=True)
    platform.fsync_directory(str(nested))


def test_fsync_directory_swallows_missing_path(tmp_path):
    # best-effort: a vanished/never-existed path must not raise.
    platform.fsync_directory(str(tmp_path / "does" / "not" / "exist"))


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="killpg is POSIX-only"
)
@pytest.mark.asyncio
async def test_kill_process_group_falls_back_when_killpg_errors(monkeypatch):
    # A killpg that fails with a generic OSError (not ProcessLookupError) is
    # logged and reported as "not signalled" so the caller falls back to
    # terminating the direct child alone, rather than assuming the kill landed.
    def boom(pid, sig):
        raise OSError("no permission to signal that group")

    monkeypatch.setattr(platform.os, "killpg", boom)
    assert not await platform.kill_process_group(4242, force=True)


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc, POSIX-only"
)
def test_os_boot_id_reads_the_kernel_value_or_none():
    # On Linux this file publishes a fresh UUID per boot; the reader returns a
    # non-empty string (or None where the file is unavailable, e.g. a container
    # that does not expose it). Either way it must never raise.
    value = platform.os_boot_id()
    assert value is None or (isinstance(value, str) and value)


def _hide_proc_file(monkeypatch, needle):
    # make open() refuse any path containing `needle`, as if the /proc
    # entry did not exist on this platform.
    import builtins

    real_open = builtins.open

    def refuse(path, *a, **k):
        if needle in str(path):
            raise OSError("no such file")
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", refuse)


def test_os_boot_id_returns_none_when_unreadable(monkeypatch):
    # A missing/unreadable boot_id file yields None (callers fall back to
    # os_boot_time), not an exception.
    _hide_proc_file(monkeypatch, "boot_id")
    assert platform.os_boot_id() is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc/uptime, POSIX-only"
)
def test_os_boot_time_returns_none_when_uptime_unreadable(monkeypatch):
    # /proc/uptime unreadable AND the psutil fallback failing -> cannot
    # tell -> None, so callers treat the daemon start as a fresh boot
    # instead of crashing.
    import psutil

    def boom():
        raise RuntimeError("no boottime either")

    _hide_proc_file(monkeypatch, "uptime")
    monkeypatch.setattr(psutil, "boot_time", boom)
    assert platform.os_boot_time() is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc/uptime, POSIX-only"
)
def test_os_boot_time_uses_psutil_without_proc_uptime(monkeypatch):
    # The /proc-less POSIX systems (macOS/BSD) read the kernel's own record
    # of the boot instant via psutil instead of returning None -- the boot
    # identity that keeps the @reboot once-per-boot dedupe live there
    # (without it, every daemon restart re-runs the one-shots).
    import psutil

    _hide_proc_file(monkeypatch, "uptime")
    monkeypatch.setattr(psutil, "boot_time", lambda: 1_755_000_000)
    assert platform.os_boot_time() == 1_755_000_000.0


def test_process_start_time_pins_pid_identity():
    # the pid-reuse disambiguator: a live pid reports a readable start
    # time that stays identical across reads (same process), and an
    # impossible pid reports None (cannot tell), never raises.
    first = platform.process_start_time(os.getpid())
    assert first is not None and first > 0
    assert platform.process_start_time(os.getpid()) == first
    assert platform.process_start_time(-1) is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="SIGHUP is POSIX-only"
)
def test_posix_sighup_delivery_reaches_the_callback():
    # End to end: a real SIGHUP raised while the loop is parked must run
    # the reload callback on the loop thread. The installed handler is all
    # that stands between SIGHUP and its default action, which kills the
    # daemon outright with no drain and orphans its running jobs.
    _assert_signal_delivered(platform.install_reload_handler, signal.SIGHUP)


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="uses os.kill(pid, 0), POSIX-only"
)
def test_pid_alive_reports_permission_denied_as_alive(monkeypatch):
    # A pid we cannot signal but that exists (EPERM) counts as alive: existence
    # is what reconciliation asks, not ownership.
    def eperm(pid, sig):
        raise PermissionError("operation not permitted")

    monkeypatch.setattr(platform.os, "kill", eperm)
    assert platform.pid_alive(4242) is True


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="uses os.kill(pid, 0), POSIX-only"
)
def test_pid_alive_reports_none_on_other_oserror(monkeypatch):
    # Any other OSError means the platform could not answer -> None ("cannot
    # tell"), which reconciliation treats as dead.
    def oserr(pid, sig):
        raise OSError("something unexpected")

    monkeypatch.setattr(platform.os, "kill", oserr)
    assert platform.pid_alive(4242) is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="POSIX fsync path"
)
def test_fsync_directory_swallows_fsync_failure(monkeypatch, tmp_path):
    # A directory that opens but whose fsync fails (some filesystems reject it)
    # is swallowed: the data it guards is correct without it, just not
    # crash-durable for this one write.
    def refuse(fd):
        raise OSError("fsync not supported here")

    monkeypatch.setattr(platform.os, "fsync", refuse)
    platform.fsync_directory(str(tmp_path))


# ---------------------------------------------------------------------------
# Windows Event Log leaf calls
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="the POSIX arms of the event log shim"
)
def test_event_log_calls_are_inert_on_posix():
    # There is no Event Log here, so opening reports "no handle" and a write
    # reports a nonzero code that is not a Win32 error, which is what keeps
    # the caller's "0 means written" contract honest on both platforms.
    assert platform.open_event_log("cronstable") is None
    assert (
        platform.write_event_log(
            1, event_type=4, category=1, event_id=1000, strings=["a"]
        )
        == platform.EVENTLOG_ERROR_UNSUPPORTED
    )
    assert platform.EVENTLOG_ERROR_UNSUPPORTED != 0
    platform.close_event_log(1)  # never raises


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="drives the Windows arm from POSIX"
)
def test_open_event_log_survives_a_ctypes_failure(monkeypatch):
    # With IS_WINDOWS forced true on a box that has no ctypes.wintypes, the
    # import inside the clause raises. That has to come back as "no handle"
    # rather than escape into the writer thread and kill it.
    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    assert platform.open_event_log("cronstable") is None
    assert (
        platform.write_event_log(
            1, event_type=4, category=1, event_id=1000, strings=["a"]
        )
        == platform.EVENTLOG_ERROR_UNSUPPORTED
    )
    platform.close_event_log(1)


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="needs a real Windows Event Log"
)
def test_event_log_direct_calls_on_windows():
    # Deliberately unskippable beyond the platform check, because the
    # `(windows)` coverage profile MEASURES these bodies on the Windows
    # rows: a test that self-skips on a locked-down runner would leave them
    # measured and missed. It asserts only what is true on any Windows host,
    # so it needs no policy, no elevation and no readback.
    source = "cronstable-test-direct"
    handle = platform.open_event_log(source)
    assert handle is None or isinstance(handle, int)
    if handle is None:  # a policy-locked host may refuse the source
        return
    try:
        code = platform.write_event_log(
            handle,
            event_type=platform.EVENTLOG_INFORMATION_TYPE,
            category=1,
            event_id=1000,
            strings=["cronstable self-test", "direct-call"],
        )
        assert code == 0
        # A vector past the API's combined ceiling writes NO record at all,
        # which is exactly why the reporter caps its fields by arithmetic
        # rather than trusting the call to truncate.
        assert (
            platform.write_event_log(
                handle,
                event_type=platform.EVENTLOG_INFORMATION_TYPE,
                category=1,
                event_id=1000,
                strings=["x" * (platform.EVENTLOG_MAX_TOTAL_CHARS * 2)],
            )
            != 0
        )
    finally:
        platform.close_event_log(handle)
    # Writing through a handle that has been released reports the one code
    # the writer acts on, by re-registering the source and retrying once.
    assert (
        platform.write_event_log(
            handle,
            event_type=platform.EVENTLOG_INFORMATION_TYPE,
            category=1,
            event_id=1000,
            strings=["after close"],
        )
        == platform.EVENTLOG_ERROR_INVALID_HANDLE
    )


# ---------------------------------------------------------------------------
# Config-directory permissions
#
# The SDDL reading is a pure function on purpose, split from the ctypes call
# that produces the string, so the decision every one of these pins runs on
# Linux and macOS too.  Every string below was read off a real Windows 11
# box with ConvertSecurityDescriptorToStringSecurityDescriptorW.
# ---------------------------------------------------------------------------

# C:\ProgramData itself. The last ACE is the hole: DCLCRPCR on BUILTIN\Users
# is FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY, FILE_WRITE_EA and
# FILE_WRITE_ATTRIBUTES, with (CI) so every directory made under it inherits
# it. icacls spells the same ACE (CI)(WD,AD,WEA,WA).
_PROGRAMDATA_SDDL = (
    "D:PAI(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICIIO;GA;;;CO)"
    "(A;OICI;0x1200a9;;;BU)(A;CI;DCLCRPCR;;;BU)"
)
# C:\ProgramData\ssh: OpenSSH resets inheritance and grants read only.
_SSH_SDDL = "D:PAI(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;0x1200a9;;;AU)"
# What harden_config_dir writes.
_HARDENED_SDDL = "D:PAI(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"


def test_programdata_grants_every_local_account_a_write():
    assert (
        platform._sddl_write_grantee(_PROGRAMDATA_SDDL) == r"BUILTIN\Users"
    )


@pytest.mark.parametrize(
    "sddl, why",
    [
        pytest.param(_HARDENED_SDDL, "hardened", id="hardened"),
        pytest.param(_SSH_SDDL, "read only for AU", id="programdata-ssh"),
        # C:\Windows\System32, the widest real DACL on the box: BUILTIN\Users
        # and the app-package SIDs get 0x1200a9, which is read and execute,
        # and the GA grants are all CO or IO-inherit-only.
        pytest.param(
            "D:PAI(A;;0x1301bf;;;SY)(A;OICIIO;GA;;;SY)(A;;0x1301bf;;;BA)"
            "(A;OICIIO;GA;;;BA)(A;;0x1200a9;;;BU)(A;OICIIO;GXGR;;;BU)"
            "(A;OICIIO;GA;;;CO)(A;;0x1200a9;;;AC)",
            "read and execute only",
            id="system32",
        ),
    ],
)
def test_a_correctly_permissioned_directory_raises_no_alarm(sddl, why):
    # A check that cries wolf at every start on a correct machine is worse
    # than no check, so the false-alarm side gets more pins than the true
    # one. CREATOR OWNER carries GENERIC_ALL on %ProgramData% itself and
    # resolves per object to whoever made it, so counting CO would flag
    # every directory on the system.
    assert platform._sddl_write_grantee(sddl) is None


def test_a_deny_ace_beats_the_allow_it_precedes():
    denied = "D:P(D;OICI;FA;;;BU)(A;OICI;FA;;;BU)(A;OICI;FA;;;SY)"
    assert platform._sddl_write_grantee(denied) is None


def test_write_rights_are_read_in_both_forms():
    # A DACL read back from disk gives numeric and token rights, sometimes
    # both in one ACL, so both forms have to answer the same question.
    assert platform._sddl_rights_write("DCLCRPCR")  # FILE_ADD_FILE et al
    assert platform._sddl_rights_write("FA")
    assert platform._sddl_rights_write("0x1301bf")
    assert platform._sddl_rights_write("0x40000000")  # GENERIC_WRITE
    assert not platform._sddl_rights_write("0x1200a9")  # read and execute
    assert not platform._sddl_rights_write("FR")
    assert not platform._sddl_rights_write("GXGR")
    assert not platform._sddl_rights_write("nonsense")


def test_the_helpers_are_inert_on_posix(monkeypatch):
    # Both are called unconditionally from the startup and init paths, so
    # a POSIX run has to fall straight through rather than reach ctypes.
    monkeypatch.setattr(platform, "IS_WINDOWS", False)
    assert platform.any_user_write_grantee("/etc/cronstable.d") is None
    assert platform.harden_config_dir("/etc/cronstable.d") is False


def test_the_hardened_dacl_keeps_read_and_removes_write():
    # What harden_config_dir writes, checked by the same reader the
    # startup warning uses: nobody but an administrator may write, and
    # Authenticated Users keep read so an unelevated caller can still
    # list the directory it just created. Dropping read makes
    # _holds_config answer "no config here" for every unelevated account,
    # which silently moves DEFAULT_CONFIG_PATH back to %APPDATA%.
    assert platform._sddl_write_grantee(platform._CONFIG_DIR_SDDL) is None
    assert "FRFX;;;AU" in platform._CONFIG_DIR_SDDL
    assert platform._CONFIG_DIR_SDDL.startswith("D:P")
