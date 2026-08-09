import asyncio
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


def test_install_shutdown_handlers_roundtrip():
    # Exercises install + the returned cleanup on both platforms (loop signal
    # handlers on POSIX; signal.signal + console handler + heartbeat on
    # Windows) without firing a real signal.  Must run on the main thread
    # (signal.signal requires it).
    loop = asyncio.new_event_loop()
    try:
        called = []
        cleanup = platform.install_shutdown_handlers(
            loop, lambda: called.append(1)
        )
        assert callable(cleanup)
        cleanup()
    finally:
        loop.close()


@pytest.mark.skipif(
    not platform.IS_WINDOWS, reason="Windows signal-delivery path"
)
def test_windows_sigint_delivery_reaches_the_callback():
    # End to end through the signal.signal fallback and its heartbeat: a
    # real SIGINT raised while the loop is parked must run the callback on
    # the loop thread (the docs' Ctrl-C promise; the POSIX sibling is
    # test_cron.py's SIGTERM test). Before this test existed, the only
    # Windows coverage installed the handlers without ever firing one.
    loop = asyncio.new_event_loop()
    called = []
    cleanup = platform.install_shutdown_handlers(
        loop, lambda: called.append(1)
    )
    try:

        async def fire_and_park():
            signal.raise_signal(signal.SIGINT)
            # generous park: the heartbeat only guarantees the pending C
            # handler is observed within its 0.25s tick.
            deadline = time.monotonic() + 5
            while not called and time.monotonic() < deadline:
                await asyncio.sleep(0.05)

        loop.run_until_complete(fire_and_park())
    finally:
        cleanup()
        loop.close()
    assert called == [1]


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


def test_os_boot_id_returns_none_when_unreadable(monkeypatch):
    # A missing/unreadable boot_id file yields None (callers fall back to
    # os_boot_time), not an exception.
    import builtins

    real_open = builtins.open

    def refuse(path, *a, **k):
        if "boot_id" in str(path):
            raise OSError("no such file")
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", refuse)
    assert platform.os_boot_id() is None


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="reads /proc/uptime, POSIX-only"
)
def test_os_boot_time_returns_none_when_uptime_unreadable(monkeypatch):
    # /proc/uptime unreadable (or garbage) -> cannot tell -> None, so callers
    # treat the daemon start as a fresh boot instead of crashing.
    import builtins

    real_open = builtins.open

    def refuse(path, *a, **k):
        if "uptime" in str(path):
            raise OSError("no such file")
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", refuse)
    assert platform.os_boot_time() is None


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
