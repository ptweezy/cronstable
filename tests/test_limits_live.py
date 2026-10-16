"""Capped runs end to end: RunningJob on a real kernel.

Linux runs these only when asked, inside a delegated cgroup v2, because the
manager's setup moves the test process into ``<base>/daemon``::

    systemd-run --user --scope -p Delegate=yes --quiet \\
        env CRONSTABLE_LIMITS_LIVE=1 python -m pytest tests/test_limits_live.py

``CRONSTABLE_LIMITS_LIVE=1``
    Run the Linux tests.  A test skips when the cgroup cannot enforce the
    caps it uses: under the packaged unit's ``Delegate=memory pids``, only
    the tests that need the cpu controller skip.
``CRONSTABLE_LIVE_REQUIRED=1``
    Fail any test that would otherwise skip.

Job Objects need no privileges, so the tests always run on Windows.  The
backend-level counterparts are tests/test_cgroups_live.py and the live tests
in tests/test_jobobjects.py; tests/test_job.py drives the same RunningJob
paths against a fake backend.
"""

import asyncio
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time

import pytest

import cronstable.config
import cronstable.job
from cronstable import limits, platform
from cronstable.platform import IS_WINDOWS
from tests._commands import yaml_command

_LIVE = os.environ.get("CRONSTABLE_LIMITS_LIVE") == "1"
_REQUIRED = os.environ.get("CRONSTABLE_LIVE_REQUIRED") == "1"
_ALL = frozenset({"cpu", "memory", "processes"})
MIB = 1024 * 1024
_DEADLINE = 30.0
#: How soon a killed run ends and its helpers are gone.  Well under the
#: drain bound (cronstable.job.KILLED_STREAM_DRAIN_TIMEOUT) and the helpers'
#: own lifetime, so a kill that missed them cannot pass by waiting.
_SETTLE = 10.0
_HELPER_SLEEP = 300

#: On Windows, the base interpreter: a venv's python.exe is a launcher that
#: runs it as a second process, which a process cap would count.
_PYTHON = (
    (getattr(sys, "_base_executable", None) or sys.executable)
    if IS_WINDOWS
    else sys.executable
)

#: one manager for the module: on Linux its setup migrates this process once
_STATE = {}


def _need(present, reason):
    """Skip if ``present`` is false, or fail if live tests are required."""
    if present:
        return
    if _REQUIRED:
        pytest.fail(reason + " (and CRONSTABLE_LIVE_REQUIRED=1)")
    pytest.skip(reason)


def _installed(needed):
    """The live manager, installed process-wide where ``needed`` applies."""
    if not IS_WINDOWS:
        _need(_LIVE, "CRONSTABLE_LIMITS_LIVE is not set")
        _need(sys.platform.startswith("linux"), "cgroup v2 needs Linux")
    manager = _STATE.get("manager")
    if manager is None:
        manager = _STATE["manager"] = limits.LimitsManager()
    cap = manager.setup(retry=True)
    if IS_WINDOWS:
        assert cap.available and cap.resources == _ALL, cap
    else:
        _need(
            cap.available and needed <= cap.resources,
            "no delegated cgroup v2 that caps {}: {}".format(
                ", ".join(sorted(needed)), cap.reason
            ),
        )
    previous = limits._MANAGER
    limits.set_manager(manager)
    try:
        yield manager
    finally:
        limits.set_manager(previous)


@pytest.fixture
def live():
    """The host's real limits backend behind the process-wide manager."""
    yield from _installed(_ALL)


@pytest.fixture
def live_memory():
    """``live`` for memory and process caps alone.

    It needs no cpu controller, as under ``Delegate=memory pids``.
    """
    yield from _installed(frozenset({"memory", "processes"}))


@pytest.fixture(scope="module", autouse=True)
def _close_the_backend():
    yield
    manager = _STATE.pop("manager", None)
    if manager is not None:
        close = getattr(manager.backend, "close", None)
        if close is not None:
            close()  # the Job Object watcher thread


def _job(command, caps, extra=""):
    """A RunningJob of one job named ``live`` under ``caps`` (None: none)."""
    if isinstance(command, list):
        lines = yaml_command(command)
    else:
        lines = "    command: " + json.dumps(command)
    text = (
        "jobs:\n  - name: live\n"
        + lines
        + '\n    schedule: "* * * * *"\n'
        + extra
    )
    if caps is not None:
        text += "    resourceLimits:\n" + "".join(
            "      {}: {}\n".format(k, v) for k, v in caps.items()
        )
    conf = cronstable.config.parse_config_string(text, "")
    return cronstable.job.RunningJob(conf.jobs[0], None)


def _py(code):
    """``code`` run by the interpreter, as one shell command.

    ``code`` must hold no double quote, ``$`` or ``%``: the line goes
    through /bin/sh on Linux and cmd.exe on Windows.
    """
    assert not set(code) & set('"$%\n'), code
    return '"{}" -c "{}"'.format(_PYTHON, code)


def _in_background(command):
    """``command`` started in the background of a shell command line."""
    if IS_WINDOWS:
        return 'start "" /b ' + command + " &"
    return command + " &"


def _writes_pid(path, pid="os.getpid()"):
    """Code that publishes ``pid`` at ``path`` in one step."""
    tmp = str(path) + ".tmp"
    return (
        "open({tmp!r}, 'w').write(str({pid})); os.replace({tmp!r}, {path!r})"
    ).format(tmp=tmp, path=str(path), pid=pid)


async def _pid_from(path):
    give_up = time.monotonic() + _DEADLINE
    while not os.path.exists(path):
        assert time.monotonic() < give_up, "no pid at {}".format(path)
        await asyncio.sleep(0.05)
    with open(path, encoding="ascii") as fh:
        return int(fh.read())


async def _gone(pid, deadline=_SETTLE):
    """Whether ``pid`` exits (or is a zombie) within ``deadline``."""
    give_up = time.monotonic() + deadline
    while platform.pid_alive(pid) is True:
        if time.monotonic() >= give_up:
            return False
        await asyncio.sleep(0.05)
    return True


def _kill(pid):
    try:
        os.kill(pid, signal.SIGTERM if IS_WINDOWS else signal.SIGKILL)
    except OSError:
        pass


async def _run(job):
    await asyncio.wait_for(job.start(), _DEADLINE)
    await asyncio.wait_for(job.wait(), _DEADLINE)


async def test_a_memory_hog_fails_the_run_and_takes_its_helper(
    live_memory, tmp_path
):
    pid_file = tmp_path / "helper.pid"
    sleeper = "import os, time; {}; time.sleep({})".format(
        _writes_pid(pid_file), _HELPER_SLEEP
    )
    # waits for the helper's pid, then allocates until the cap stops it
    hog = (
        "import os, time; "
        "[time.sleep(0.02) for _ in iter(lambda: os.path.exists({!r}), True)]"
        "; h = [b'x' * (1 << 20) for _ in iter(int, 1)]"
    ).format(str(pid_file))
    job = _job(
        _in_background(_py(sleeper)) + " " + _py(hog), {"memory": "96M"}
    )
    try:
        await _run(job)
        helper = await _pid_from(pid_file)
        assert await _gone(helper), "the helper outlived the memory kill"
    finally:
        if os.path.exists(pid_file):
            _kill(await _pid_from(pid_file))
    record = job.limits_record
    assert record.enforced and record.memory_exceeded, record
    assert job.retcode != 0
    assert job.fail_reason == "memory limit exceeded (96 MiB)"
    assert record.peak_memory_bytes >= 48 * MIB


async def test_a_busy_run_is_held_to_its_cpu_cap(live):
    code = (
        "import time\n"
        "end = time.monotonic() + 1.5\n"
        "while time.monotonic() < end:\n"
        "    pass\n"
    )
    job = _job(
        [_PYTHON, "-c", code],
        {"cpu": 0.2},
        extra="    monitorResources:\n      interval: 30\n      history: 0\n",
    )
    await _run(job)
    assert job.retcode == 0 and not job.failed
    usage = job.resource_usage
    # 0.2 CPUs for 1.5 s is about 0.3 s, far below the 1.5 s it asked for
    assert 0 < usage.cpu_total_seconds < 0.9, usage
    record = job.limits_record
    assert record.enforced, record
    if IS_WINDOWS:
        assert record.cpu_throttled_seconds is None
    else:
        assert record.cpu_throttled_seconds > 0


async def test_a_process_cap_refuses_extra_processes(live_memory):
    code = (
        "import subprocess, sys\n"
        "kids, failed = [], 0\n"
        "for _ in range(5):\n"
        "    try:\n"
        "        kids.append(subprocess.Popen(\n"
        "            [sys.executable, '-c', 'import time; time.sleep(2)']\n"
        "        ))\n"
        "    except OSError:\n"
        "        failed += 1\n"
        "for kid in kids:\n"
        "    kid.wait()\n"
        "print(len(kids), failed, flush=True)\n"
    )
    job = _job(
        [_PYTHON, "-c", code],
        {"processes": 3},
        extra="    captureStdout: true\n",
    )
    await _run(job)
    assert job.retcode == 0, job.stderr
    started, failed = (int(n) for n in job.stdout.split())
    # the job itself is one of the three
    assert started <= 2 and failed >= 3, job.stdout
    assert job.limits_record.process_limit_hits > 0


async def test_cancel_reaches_a_descendant_outside_the_group(
    live_memory, tmp_path
):
    pid_file = tmp_path / "escapee.pid"
    flags = (
        "creationflags=subprocess.CREATE_NEW_PROCESS_GROUP"
        if IS_WINDOWS
        else "start_new_session=True"
    )
    # starts the escapee in a group (POSIX: session) of its own, publishes
    # its pid and exits, so no live parent links the escapee to the job
    intermediate = (
        "import os, subprocess, sys\n"
        "p = subprocess.Popen(\n"
        "    [sys.executable, '-c', 'import time; time.sleep({sleep})'],\n"
        "    {flags},\n"
        ")\n"
        "{publish}\n"
    ).format(
        sleep=_HELPER_SLEEP,
        flags=flags,
        publish=_writes_pid(pid_file, pid="p.pid"),
    )
    code = (
        "import subprocess, sys, time\n"
        "subprocess.run(\n"
        "    [sys.executable, '-c', {intermediate!r}], check=True\n"
        ")\n"
        "time.sleep({sleep})\n"
    ).format(intermediate=intermediate, sleep=_HELPER_SLEEP)
    # the escapee holds the captured stdout on POSIX, so the run ends only
    # once the kill reaches it
    job = _job(
        [_PYTHON, "-c", code],
        {"memory": "256M"},
        extra="    killTimeout: 2\n    captureStdout: true\n",
    )
    await asyncio.wait_for(job.start(), _DEADLINE)
    escapee = await _pid_from(pid_file)
    try:
        collected = asyncio.ensure_future(job.wait())
        await asyncio.sleep(0)
        await asyncio.wait_for(job.cancel(), _SETTLE)
        await asyncio.wait_for(collected, _SETTLE)
        assert job.stopped
        # neither the group signals nor a taskkill /T walk from the leader
        # reaches it; only the container's kill does
        assert await _gone(escapee), "the escapee outlived the cancel"
    finally:
        _kill(escapee)
    assert job.limits_record.enforced


async def test_the_monitor_counts_short_lived_children_exactly(live_memory):
    burn = (
        "import time\\n"
        "end = time.process_time() + 0.15\\n"
        "while time.process_time() < end:\\n"
        "    pass\\n"
    )
    code = (
        "import subprocess, sys\n"
        "burn = '" + burn + "'\n"
        "for _ in range(4):\n"
        "    subprocess.run([sys.executable, '-c', burn], check=True)\n"
    )
    # one sample at the start and one at the end: every child runs and exits
    # between the two, where a process-tree walk never sees it
    job = _job(
        [_PYTHON, "-c", code],
        {"processes": 16},
        extra="    monitorResources:\n      interval: 30\n      history: 0\n",
    )
    await _run(job)
    assert job.retcode == 0, job.stderr
    assert job.resource_usage.cpu_total_seconds >= 0.55, job.resource_usage


async def test_a_verify_check_runs_under_the_caps(live_memory):
    job = _job(
        [_PYTHON, "-c", "pass"],
        {"memory": "96M"},
        extra=(
            "    verify:\n"
            + yaml_command(
                [_PYTHON, "-c", "big = b'x' * (512 << 20)"], indent=6
            )
            + "\n      timeout: 30\n"
        ),
    )
    await _run(job)
    assert job.retcode == 0
    # the run itself stayed within its cap; its check did not
    assert not job.limits_record.memory_exceeded
    assert job.verification["outcome"] == "failure"
    assert job.fail_reason == (
        "verification failed: memory limit exceeded (96 MiB)"
    )


async def test_required_caps_run_normally_where_they_apply(live):
    job = _job(
        [_PYTHON, "-c", "print('ok')"],
        {"cpu": 2, "memory": "512M", "processes": 32, "required": "true"},
        extra="    captureStdout: true\n",
    )
    await _run(job)
    assert job.retcode == 0 and not job.failed, job.fail_reason
    assert job.stdout.strip() == "ok"
    record = job.limits_record
    assert record.enforced and not record.refused, record
    assert record.required
    assert record.mechanism == ("job-object" if IS_WINDOWS else "cgroup-v2")
    assert record.peak_memory_bytes > 0
    if not IS_WINDOWS:
        # the run's cgroup is gone with it, or a later collect removes it
        await _removed(live, job._limits.container.path)


async def _removed(manager, path):
    """Wait until the cgroup at ``path`` is gone, collecting meanwhile."""
    give_up = time.monotonic() + _DEADLINE
    while os.path.exists(path):
        assert time.monotonic() < give_up, path
        manager.collect()
        await asyncio.sleep(0.05)


async def test_an_uncapped_run_has_no_limits_record_while_cpu_is_weighed(
    live,
):
    # a cpu cap anywhere in the config turns cronstable's CPU weighting on
    live.converge([("capped", cronstable.config.ResourceLimits(cpu=1.0))])
    code = (
        "import sys\n"
        "if sys.platform.startswith('linux'):\n"
        "    print(open('/proc/self/cgroup').read().strip())\n"
    )
    job = _job(
        [_PYTHON, "-c", code], None, extra="    captureStdout: true\n"
    )
    await _run(job)
    assert job.retcode == 0 and not job.failed, job.fail_reason
    assert job.limits_record is None
    assert job.template_vars["limits"] is None
    if IS_WINDOWS:
        # a Job Object's CPU cap groups no other process
        assert not limits.weighting_active()
        assert job._limits is None
    else:
        assert limits.weighting_active()
        run = job._limits
        assert run is not None and not run.recorded
        # a cgroup of its own under runs/, out of the daemon's leaf
        path = run.container.path
        assert os.path.basename(os.path.dirname(path)) == "runs", path
        own = job.stdout.strip().split("::", 1)[1]
        assert path.endswith(own), (path, own)
        await _removed(live, path)


def _burner(start, end):
    """Code that keeps one CPU busy from wall time ``start`` to ``end``.

    It pins itself to one CPU, the same one on every call, and prints the
    CPU seconds it got.
    """
    if IS_WINDOWS:
        pin = (
            "import ctypes\n"
            "k = ctypes.windll.kernel32\n"
            "k.GetCurrentProcess.restype = ctypes.c_void_p\n"
            "k.SetProcessAffinityMask.argtypes = "
            "[ctypes.c_void_p, ctypes.c_size_t]\n"
            "assert k.SetProcessAffinityMask(k.GetCurrentProcess(), 1)\n"
        )
    else:
        pin = "os.sched_setaffinity(0, {{{}}})\n".format(
            min(os.sched_getaffinity(0))
        )
    return (
        "import os, time\n"
        + pin
        + "time.sleep(max(0.0, {!r} - time.time()))\n".format(start)
        + "used = time.process_time()\n"
        + "while time.time() < {!r}:\n".format(end)
        + "    pass\n"
        + "print(time.process_time() - used, flush=True)\n"
    )


@pytest.mark.parametrize("idle", ["capped", "uncapped"])
@pytest.mark.parametrize(
    "caps",
    # a cpu cap of every CPU never throttles one busy thread
    [{"cpu": os.cpu_count() or 2}, {"memory": "256M"}],
    ids=["cpu-cap", "memory-cap"],
)
async def test_priority_orders_a_capped_run_against_an_uncapped_one(
    request, idle, caps
):
    # A cpu cap turns cronstable's CPU weighting on, and a memory cap
    # alone has no need of it.  Idle yields to normal either way.
    live = request.getfixturevalue("live" if "cpu" in caps else "live_memory")
    configured = cronstable.config.ResourceLimits(
        cpu=caps.get("cpu"),
        memory=256 * MIB if "memory" in caps else None,
    )
    live.converge([("live", configured)])
    start = time.time() + 1.5
    code = _burner(start, start + 2.0)
    out = "    captureStdout: true\n"
    slow = out + "    priority: idle\n"
    capped = _job(
        [_PYTHON, "-c", code], caps, extra=slow if idle == "capped" else out
    )
    uncapped = _job(
        [_PYTHON, "-c", code], None, extra=out if idle == "capped" else slow
    )
    for job in (capped, uncapped):
        await asyncio.wait_for(job.start(), _DEADLINE)
    await asyncio.wait_for(
        asyncio.gather(capped.wait(), uncapped.wait()), _DEADLINE
    )
    for job in (capped, uncapped):
        assert job.retcode == 0 and not job.failed, job.stderr
    assert capped.limits_record.enforced, capped.limits_record
    assert uncapped.limits_record is None
    got = {
        "capped": float(capped.stdout),
        "uncapped": float(uncapped.stdout),
    }
    # the two shared one CPU; with priority ignored, each gets half of it
    assert sum(got.values()) > 1.0, got
    assert got[idle] / sum(got.values()) < 0.2, got


def _user_scopes():
    """Whether ``systemd-run --user --scope`` can start a command here."""
    if IS_WINDOWS or shutil.which("systemd-run") is None:
        return False
    try:
        done = subprocess.run(
            ["systemd-run", "--user", "--scope", "--quiet", "true"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=_DEADLINE,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return done.returncode == 0


def _leaves_its_cgroup(go, pid_file, scope):
    """A shell command whose process ignores SIGTERM and leaves its cgroup.

    Once ``go`` holds a ``cgroup.procs`` path, it publishes its pid and
    moves itself: into a scope of its own with ``systemd-run --user`` when
    ``scope`` is set, else into the cgroup ``go`` names.  It keeps its
    process group either way, and then sleeps.
    """
    go = shlex.quote(str(go))
    if scope:
        leave = "exec systemd-run --user --scope --quiet"
    else:
        leave = 'echo $$ > "$(cat {})" && exec'.format(go)
    return (
        "trap '' TERM; "
        "while [ ! -s {go} ]; do sleep 0.05; done; "
        "echo $$ > {tmp} && mv {tmp} {pid}; "
        "{leave} sleep {sleep}"
    ).format(
        go=go,
        tmp=shlex.quote(str(pid_file) + ".tmp"),
        pid=shlex.quote(str(pid_file)),
        leave=leave,
        sleep=_HELPER_SLEEP,
    )


async def _left(pid, path):
    """Wait until ``pid`` is in a cgroup other than the one at ``path``."""
    give_up = time.monotonic() + _DEADLINE
    while True:
        with open("/proc/{}/cgroup".format(pid), encoding="ascii") as fh:
            own = fh.read().strip().split("::", 1)[1]
        if not path.endswith(own):
            return
        assert time.monotonic() < give_up, "{} stayed in {}".format(pid, path)
        await asyncio.sleep(0.05)


async def test_a_forced_cancel_kills_a_leader_that_left_its_container(
    live_memory, tmp_path
):
    # The leader ignores the graceful stop.  On Linux it also leaves its
    # cgroup but keeps its process group, which the forced kill still
    # signals.  A Job Object kill ends the whole job.
    pid_file = tmp_path / "leader.pid"
    go = tmp_path / "go"
    scope = _user_scopes()
    if IS_WINDOWS:
        command = [
            _PYTHON,
            "-c",
            "import os, signal, time; "
            "signal.signal(signal.SIGBREAK, signal.SIG_IGN); "
            "{}; time.sleep({})".format(_writes_pid(pid_file), _HELPER_SLEEP),
        ]
    else:
        command = _leaves_its_cgroup(go, pid_file, scope)
    job = _job(command, {"memory": "256M"}, extra="    killTimeout: 1\n")
    await asyncio.wait_for(job.start(), _DEADLINE)
    leader = None
    try:
        if not IS_WINDOWS:
            run_cgroup = job._limits.container.path
            daemon = os.path.join(
                os.path.dirname(os.path.dirname(run_cgroup)), "daemon"
            )
            tmp = str(go) + ".tmp"
            with open(tmp, "w", encoding="ascii") as fh:
                fh.write(os.path.join(daemon, "cgroup.procs"))
            os.replace(tmp, go)
        leader = await _pid_from(pid_file)
        assert leader == job.proc.pid
        if not IS_WINDOWS:
            await _left(leader, run_cgroup)
            assert os.getpgid(leader) == leader, "it left its group"
            os.kill(leader, signal.SIGTERM)
            await asyncio.sleep(0.3)
            assert platform.pid_alive(leader) is True, "SIGTERM ended it"
        collected = asyncio.ensure_future(job.wait())
        await asyncio.sleep(0)
        await asyncio.wait_for(job.cancel(), _SETTLE)
        assert await _gone(leader), "the leader outlived the forced cancel"
        await asyncio.wait_for(collected, _SETTLE)
    finally:
        if leader is not None:
            _kill(leader)
        elif job.proc is not None and job.proc.returncode is None:
            job.proc.kill()
    assert job.stopped
    assert not job.limits_record.refused, (job.limits_record, scope)
