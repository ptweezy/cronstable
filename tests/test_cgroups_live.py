"""The cgroup v2 backend against a real kernel (Linux, delegated cgroup).

Opt-in, because setting up moves the test process itself into
``<base>/daemon``.  Run inside a delegated cgroup, for example::

    systemd-run --user --scope -p Delegate=yes --quiet \\
        env CRONSTABLE_LIMITS_LIVE=1 python -m pytest tests/test_cgroups_live.py

``CRONSTABLE_LIMITS_LIVE=1``
    Run these tests.  They skip when the cgroup cannot enforce memory and
    process caps, and the CPU tests skip without the cpu controller (as
    under the packaged unit's ``Delegate=memory pids``).
``CRONSTABLE_LIVE_REQUIRED=1``
    Fail any test that would otherwise skip.

The hermetic counterpart is tests/test_cgroups.py.
"""

import asyncio
import json
import os
import stat
import subprocess
import sys
import time

import pytest

from cronstable import _cgroups, limits
from cronstable._cgroups import Backend
from cronstable.config import ResourceLimits
from cronstable.resources import DAEMON_LEAF, RUNS_DIR, ResourceMonitor

_LIVE = os.environ.get("CRONSTABLE_LIMITS_LIVE") == "1"
_REQUIRED = os.environ.get("CRONSTABLE_LIVE_REQUIRED") == "1"
_NEEDED = frozenset({"memory", "processes"})
MIB = 1024 * 1024
_DEADLINE = 30.0
_NOT_DELEGATED = "the cpu controller is not delegated to cronstable"

#: one manager for the module: its setup migrates this process once
_STATE = {}


def _need(present, reason):
    """Skip if ``present`` is false, or fail if live tests are required."""
    if present:
        return
    if _REQUIRED:
        pytest.fail(reason + " (and CRONSTABLE_LIVE_REQUIRED=1)")
    pytest.skip(reason)


def _manager():
    _need(_LIVE, "CRONSTABLE_LIMITS_LIVE is not set")
    _need(sys.platform.startswith("linux"), "cgroup v2 needs Linux")
    manager = _STATE.get("manager")
    if manager is None:
        manager = _STATE["manager"] = limits.LimitsManager(Backend())
    cap = manager.setup()
    _need(
        cap.available and cap.resources >= _NEEDED,
        "no delegated cgroup v2 with memory and pids: {}".format(cap.reason),
    )
    return manager


def _offers_cpu(manager):
    base = manager.backend._base
    return "cpu" in _read(os.path.join(base, "cgroup.controllers")).split()


def _cpu_manager():
    """The manager, on a cgroup that offers the cpu controller."""
    manager = _manager()
    _need(_offers_cpu(manager), "the cgroup does not offer the cpu controller")
    return manager


def _own_cgroup():
    with open("/proc/self/cgroup", encoding="ascii") as fh:
        for line in fh:
            if line.startswith("0::"):
                return os.path.normpath("/sys/fs/cgroup/" + line[3:].strip())
    return None


def _gone(pid):
    """Whether ``pid`` exited (reaped, or a zombie)."""
    try:
        with open("/proc/{}/stat".format(pid), encoding="ascii") as fh:
            stat = fh.read()
    except (FileNotFoundError, ProcessLookupError):
        # ESRCH: the task exited between the open and the read
        return True
    return stat.rsplit(")", 1)[1].split()[0] == "Z"


async def _until(done, deadline=_DEADLINE):
    give_up = time.monotonic() + deadline
    while not done():
        assert time.monotonic() < give_up, "condition not met in time"
        await asyncio.sleep(0.05)


async def _start(manager, caps, code, nice=None, **popen):
    run = await manager.prepare(caps, "live", nice=nice)
    assert not run.refused, run.record
    assert run.record.enforced, run.record
    kwargs = {"start_new_session": True, **popen}
    run.spawn_kwargs(kwargs)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", code, **kwargs
    )
    await run.attach(proc.pid)
    assert run.record.enforced, run.record
    return run, proc


def _sleeper(seconds, **extra):
    args = ", ".join(
        ["[sys.executable, '-c', 'import time; time.sleep({})']".format(
            seconds
        )]
        + ["{}={}".format(k, v) for k, v in extra.items()]
    )
    return "subprocess.Popen({})".format(args)


def _read(path):
    with open(path, encoding="ascii") as fh:
        return fh.read()


async def _collected(manager, path):
    """Wait until a later collect removed the run cgroup ``path``.

    A finished run's cgroup gets one ``rmdir``; when that is busy (6.19
    can report EBUSY just after the last process was reaped), a later
    collect removes it.
    """

    def removed():
        manager.collect()
        return not os.path.exists(path)

    await _until(removed)
    assert path not in manager.backend.pending


def _enabled(path):
    return set(_read(os.path.join(path, "cgroup.subtree_control")).split())


async def test_setup_moves_this_process_into_the_daemon_leaf():
    manager = _manager()
    base = manager.backend._base
    assert _own_cgroup() == os.path.join(base, DAEMON_LEAF)
    for path in (base, os.path.join(base, RUNS_DIR)):
        assert {"memory", "pids"} <= _enabled(path), path
    assert _read(os.path.join(base, "cgroup.type")).strip() == "domain"


# --- cpu control and priority ----------------------------------------------


def _ticks(pid):
    """User plus system clock ticks ``pid`` has used."""
    with open("/proc/{}/stat".format(pid), encoding="ascii") as fh:
        fields = fh.read().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def _burner(nice):
    """Code that busies one CPU, the same one on every call, at ``nice``."""
    cpu = min(os.sched_getaffinity(0))
    return (
        "import os\n"
        "os.sched_setaffinity(0, {{{}}})\n"
        "os.nice({})\n"
        "while True:\n"
        "    pass\n".format(cpu, nice)
    )


async def _share(low, high, seconds=3.0):
    """``low``'s share of the CPU time the two processes got together."""
    await asyncio.sleep(0.5)
    low0, high0 = _ticks(low.pid), _ticks(high.pid)
    await asyncio.sleep(seconds)
    used = _ticks(low.pid) - low0, _ticks(high.pid) - high0
    # both ran, sharing one CPU
    assert sum(used) > 0.5 * seconds * os.sysconf("SC_CLK_TCK"), used
    return used[0] / sum(used)


async def _stop(*procs):
    for proc in procs:
        if proc.returncode is None:
            proc.kill()
        await asyncio.wait_for(proc.wait(), _DEADLINE)


async def test_memory_caps_leave_cpu_off_and_priority_working():
    # Without the cpu controller every process shares one CPU group, so a
    # capped run at nice 19 still yields to an uncapped one at nice 0.
    manager = _manager()
    caps = ResourceLimits(memory=1024 * MIB)
    manager.converge([("job live", caps)])
    base = manager.backend._base
    assert not manager.weighting
    for path in (base, os.path.join(base, RUNS_DIR)):
        assert "cpu" not in _enabled(path), path
    run, low = await _start(manager, caps, _burner(19))
    assert "cpu.weight.nice" not in os.listdir(run.container.path)
    high = await asyncio.create_subprocess_exec(
        sys.executable, "-c", _burner(0)
    )
    try:
        share = await _share(low, high)
    finally:
        await _stop(low, high)
        await run.finish()
    assert share < 0.2, share


async def test_a_cpu_cap_weighs_runs_by_their_nice():
    # With a cpu cap configured, cpu is on and each run is a CPU group
    # whose weight follows its nice.
    manager = _cpu_manager()
    caps = ResourceLimits(cpu=8)
    manager.converge([("job live", caps)])
    base = manager.backend._base
    assert manager.weighting
    for path in (base, os.path.join(base, RUNS_DIR)):
        assert "cpu" in _enabled(path), path
    own_nice = os.getpriority(os.PRIO_PROCESS, 0)
    daemon = os.path.join(base, DAEMON_LEAF)
    assert int(_read(os.path.join(daemon, "cpu.weight.nice"))) == own_nice
    low_run, low = await _start(manager, caps, _burner(19), nice=19)
    high_run, high = await _start(manager, caps, _burner(0), nice=0)
    try:
        for run, nice in ((low_run, 19), (high_run, 0)):
            weight = os.path.join(run.container.path, "cpu.weight.nice")
            assert int(_read(weight)) == nice
        share = await _share(low, high)
    finally:
        await _stop(low, high)
        await low_run.finish()
        await high_run.finish()
    assert share < 0.2, share
    # with no cpu cap left, cpu goes off again
    manager.converge([("job live", ResourceLimits(memory=256 * MIB))])
    assert not manager.weighting
    for path in (base, os.path.join(base, RUNS_DIR)):
        assert "cpu" not in _enabled(path), path


async def test_an_uncapped_run_gets_a_cgroup_for_its_weight():
    manager = _cpu_manager()
    manager.converge([("job live", ResourceLimits(cpu=1))])
    run = await manager.prepare(None, "plain", nice=10)
    assert run is not None and not run.recorded
    path = run.container.path
    assert os.path.dirname(path) == os.path.join(
        manager.backend._base, RUNS_DIR
    )
    assert int(_read(os.path.join(path, "cpu.weight.nice"))) == 10
    for name in ("memory.max", "pids.max", "cpu.max"):
        assert _read(os.path.join(path, name)).split()[0] == "max", name
    kwargs = {"start_new_session": True, "stdout": asyncio.subprocess.PIPE}
    run.spawn_kwargs(kwargs)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "print(open('/proc/self/cgroup').read())",
        **kwargs,
    )
    await run.attach(proc.pid)
    out, _ = await asyncio.wait_for(proc.communicate(), _DEADLINE)
    assert out.decode().strip().endswith(os.path.basename(path))
    await run.finish()
    await _collected(manager, path)
    manager.converge([("job live", ResourceLimits(memory=256 * MIB))])
    assert await manager.prepare(None, "plain") is None


async def test_a_running_cpu_cap_outlasts_a_converge_without_one():
    # A reload drops the last cpu cap while a run with one is going: the
    # run keeps its cap, and cpu goes off once it ends.
    manager = _cpu_manager()
    caps = ResourceLimits(cpu=0.5)
    manager.converge([("job live", caps)])
    run, proc = await _start(manager, caps, "import time; time.sleep(60)")
    base = manager.backend._base
    layout = (base, os.path.join(base, RUNS_DIR))
    try:
        manager.converge([("job live", ResourceLimits(memory=256 * MIB))])
        assert manager.weighting
        for path in layout:
            assert "cpu" in _enabled(path), path
        quota = _read(os.path.join(run.container.path, "cpu.max"))
        assert quota.split() == ["50000", "100000"], quota
    finally:
        await _stop(proc)
    record = await run.finish()
    assert record.enforced, record
    assert not manager.weighting
    for path in layout:
        assert "cpu" not in _enabled(path), path


async def _without_cpu(manager):
    """What a cpu and memory cap get from ``manager`` (cpu not offered)."""
    caps = ResourceLimits(cpu=1, memory=64 * MIB)
    manager.converge([("job live", caps)])
    run = await manager.prepare(caps, "live")
    try:
        return {
            "capability": manager.capability().to_dict(),
            "record": run.record.to_dict(),
            "weighting": manager.weighting,
            "subtree": sorted(_enabled(manager.backend._base)),
            "memory_max": _read(
                os.path.join(run.container.path, "memory.max")
            ).strip(),
        }
    finally:
        run.abort()
        manager.collect()


def _report_without_cpu():
    """Print :func:`_without_cpu` for a fresh manager (a subprocess)."""
    manager = limits.LimitsManager(Backend())
    print(json.dumps(asyncio.run(_without_cpu(manager))))


def _write(path, text):
    with open(path, "w", encoding="ascii") as fh:
        fh.write(text)


def _in_a_cgroup_without_cpu(manager):
    """:func:`_without_cpu` in a subprocess whose cgroup lacks cpu.

    Its cgroup sits below one under the test's base whose
    ``cgroup.subtree_control`` enables memory and pids only, as the
    packaged unit's ``Delegate=memory pids`` does, so it needs no systemd
    user manager.  The subprocess moves itself there first.
    """
    parent = os.path.join(
        manager.backend._base, RUNS_DIR, "nocpu-{}".format(os.getpid())
    )
    leaf = os.path.join(parent, "cronstable")
    os.mkdir(parent)
    try:
        _write(os.path.join(parent, "cgroup.subtree_control"), "+memory +pids")
        os.mkdir(leaf)
        if os.geteuid() == 0:
            # a root daemon also needs the delegation mark
            os.setxattr(leaf, "trusted.delegate", b"1")
        code = (
            "import os\n"
            "open({!r}, 'w').write(str(os.getpid()))\n"
            "from tests.test_cgroups_live import _report_without_cpu\n"
            "_report_without_cpu()\n"
        ).format(os.path.join(leaf, "cgroup.procs"))
        done = subprocess.run(
            [sys.executable, "-c", code],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_DEADLINE * 2,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
    finally:
        give_up = time.monotonic() + _DEADLINE
        while not manager.backend._remove(parent):
            assert time.monotonic() < give_up, "{} stayed".format(parent)
            time.sleep(0.05)
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


async def test_a_cpu_cap_without_the_cpu_controller_is_unenforced():
    # The packaged unit delegates memory and pids only: a cpu cap is then
    # unenforced, with the reason and hint, and the memory cap applies.
    manager = _manager()
    got = await asyncio.to_thread(_in_a_cgroup_without_cpu, manager)
    cap = got["capability"]
    assert cap["available"] and cap["resources"] == ["memory", "processes"]
    assert cap["reason"] == _NOT_DELEGATED
    assert cap["hint"] == _cgroups._CPU_HINT
    record = got["record"]
    assert record["unenforced"] == ["cpu"] and not record["enforced"]
    assert record["reason"] == _NOT_DELEGATED
    assert record["mechanism"] == "cgroup-v2"
    assert got["weighting"] is False
    assert got["subtree"] == ["memory", "pids"]
    assert got["memory_max"] == str(64 * MIB)


async def test_a_memory_hog_is_killed_with_its_helper():
    manager = _manager()
    code = (
        "import subprocess, sys, time\n"
        "helper = " + _sleeper(60) + "\n"
        "print(helper.pid, flush=True)\n"
        "time.sleep(0.2)\n"
        "hog = []\n"
        "while True:\n"
        "    hog.append(b'x' * (1 << 20))\n"
    )
    run, proc = await _start(
        manager,
        ResourceLimits(memory=64 * MIB),
        code,
        stdout=asyncio.subprocess.PIPE,
    )
    helper = int(await asyncio.wait_for(proc.stdout.readline(), _DEADLINE))
    await asyncio.wait_for(proc.communicate(), _DEADLINE)
    assert proc.returncode == -9
    await _until(lambda: _gone(helper))  # memory.oom.group took it too
    record = await run.finish()
    assert record.memory_exceeded
    assert record.failure == "memory limit exceeded (64 MiB)"
    # it reached the cap (usage may briefly overshoot memory.max)
    assert record.peak_memory_bytes >= 48 * MIB


async def test_a_busy_loop_is_throttled():
    manager = _cpu_manager()
    code = (
        "import time\n"
        "end = time.monotonic() + 1.0\n"
        "while time.monotonic() < end:\n"
        "    pass\n"
    )
    run, proc = await _start(manager, ResourceLimits(cpu=0.2), code)
    await asyncio.wait_for(proc.wait(), _DEADLINE)
    user, system = run.container.cpu_times()
    record = await run.finish()
    assert record.cpu_throttled_seconds > 0
    # 0.2 CPUs for about a second, far below the full core it asked for
    assert user + system < 0.6


async def test_extra_forks_fail_at_the_process_cap():
    manager = _manager()
    code = (
        "import os, time\n"
        "kids, failed = [], 0\n"
        "for _ in range(6):\n"
        "    try:\n"
        "        pid = os.fork()\n"
        "    except OSError:\n"
        "        failed += 1\n"
        "        continue\n"
        "    if pid == 0:\n"
        "        time.sleep(1)\n"
        "        os._exit(0)\n"
        "    kids.append(pid)\n"
        "for pid in kids:\n"
        "    os.waitpid(pid, 0)\n"
        "print(len(kids), failed, flush=True)\n"
    )
    run, proc = await _start(
        manager,
        ResourceLimits(processes=3),
        code,
        stdout=asyncio.subprocess.PIPE,
    )
    out, _ = await asyncio.wait_for(proc.communicate(), _DEADLINE)
    started, failed = (int(n) for n in out.split())
    assert started <= 2 and failed >= 4
    record = await run.finish()
    assert record.process_limit_hits >= failed


async def test_kill_reaches_a_setsid_escapee():
    manager = _manager()
    code = (
        "import subprocess, sys, time\n"
        "helper = " + _sleeper(60, start_new_session=True) + "\n"
        "print(helper.pid, flush=True)\n"
        "time.sleep(60)\n"
    )
    run, proc = await _start(
        manager,
        ResourceLimits(memory=256 * MIB),
        code,
        stdout=asyncio.subprocess.PIPE,
    )
    escapee = int(await asyncio.wait_for(proc.stdout.readline(), _DEADLINE))
    assert os.getsid(escapee) == escapee  # out of the job's session
    assert await run.kill() is True
    await asyncio.wait_for(proc.communicate(), _DEADLINE)
    assert proc.returncode == -9
    await _until(lambda: _gone(escapee))
    await run.finish()


async def test_the_cgroup_is_removed_after_the_run():
    manager = _manager()
    run, proc = await _start(
        manager,
        ResourceLimits(processes=16),
        "print('hi')",
        stdout=asyncio.subprocess.DEVNULL,
    )
    path = run.container.path
    await asyncio.wait_for(proc.wait(), _DEADLINE)
    record = await run.finish()
    assert record.enforced
    # finish removed it or queued it for collect
    assert not os.path.exists(path) or path in manager.backend.pending
    await _collected(manager, path)


async def test_a_leftover_keeps_its_cgroup_until_collected():
    manager = _manager()
    code = (
        "import subprocess, sys\n"
        + _sleeper(
            3,
            stdout="subprocess.DEVNULL",
            stderr="subprocess.DEVNULL",
            start_new_session=True,
        )
        + "\n"
    )
    run, proc = await _start(
        manager,
        ResourceLimits(memory=256 * MIB),
        code,
        stdout=asyncio.subprocess.DEVNULL,
    )
    container = run.container
    path = container.path
    await asyncio.wait_for(proc.wait(), _DEADLINE)
    await run.finish()
    # the leftover still runs, inside its cgroup and still capped
    assert os.path.isdir(path)
    assert path in manager.backend.pending
    members = container.members()
    assert members and proc.pid not in members
    await _until(lambda: not container.members())
    await _collected(manager, path)


async def test_a_required_run_whose_child_cannot_join_never_starts():
    manager = _manager()
    _need(os.geteuid() != 0, "root ignores the file mode this test relies on")
    # the join needs write access to the common ancestor's cgroup.procs
    procs = os.path.join(manager.backend._base, "cgroup.procs")
    run = await manager.prepare(
        ResourceLimits(memory=256 * MIB, required=True), "live"
    )
    path = run.container.path
    kwargs = {"start_new_session": True}
    run.spawn_kwargs(kwargs)
    mode = stat.S_IMODE(os.stat(procs).st_mode)
    os.chmod(procs, 0o444)
    try:
        with pytest.raises(subprocess.SubprocessError):
            await asyncio.create_subprocess_exec(
                sys.executable, "-c", "pass", **kwargs
            )
    finally:
        os.chmod(procs, mode)
    record = run.abort()
    assert record.failure == (
        "resource limits could not be enforced: could not move the job into "
        "its cgroup (resourceLimits.required is set)"
    )
    # abort leaves the removal to collect, off the event loop
    assert path in manager.backend.pending
    manager.collect()
    assert not os.path.exists(path)


async def test_a_job_outside_its_cgroup_is_monitored_by_its_tree():
    manager = _manager()
    _need(os.geteuid() != 0, "root ignores the file mode this test relies on")
    procs = os.path.join(manager.backend._base, "cgroup.procs")
    run = await manager.prepare(ResourceLimits(memory=256 * MIB), "live")
    kwargs = {"start_new_session": True}
    run.spawn_kwargs(kwargs)
    code = (
        "import time\n"
        "buf = bytearray(16 << 20)\n"
        "buf[::4096] = b'x' * len(buf[::4096])\n"
        "end = time.monotonic() + 0.5\n"
        "while time.monotonic() < end:\n"
        "    pass\n"
    )
    mode = stat.S_IMODE(os.stat(procs).st_mode)
    os.chmod(procs, 0o444)
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code, **kwargs
        )
    finally:
        os.chmod(procs, mode)
    await run.attach(proc.pid)
    assert run.record.unenforced == ("memory",)
    assert run.container.members() is None
    monitor = ResourceMonitor(
        proc.pid, interval=0.05, history=0, container=run.container
    )
    monitor.start()
    await asyncio.wait_for(proc.wait(), _DEADLINE)
    usage = await monitor.stop()
    record = await run.finish()
    assert usage is not None and usage.max_rss_bytes >= 16 * MIB
    assert record.peak_memory_bytes is None
    assert record.memory_exceeded is False


async def test_the_monitor_counts_children_between_samples():
    manager = _manager()
    code = (
        "import os, time\n"
        "for _ in range(5):\n"
        "    pid = os.fork()\n"
        "    if pid == 0:\n"
        "        end = time.process_time() + 0.1\n"
        "        while time.process_time() < end:\n"
        "            pass\n"
        "        os._exit(0)\n"
        "    os.waitpid(pid, 0)\n"
    )
    run, proc = await _start(manager, ResourceLimits(processes=16), code)
    monitor = ResourceMonitor(
        proc.pid, interval=30.0, history=0, container=run.container
    )
    monitor.start()
    await asyncio.wait_for(proc.wait(), _DEADLINE)
    usage = await monitor.stop()
    await run.finish()
    assert usage is not None
    # five 0.1 s children, none alive at a sample after the first
    assert usage.cpu_total_seconds >= 0.45
