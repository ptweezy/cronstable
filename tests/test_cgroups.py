"""The cgroup v2 backend (cronstable._cgroups) against a kernel model.

Hermetic: tests/_fake_cgroupfs.py models the cgroupfs rules the backend
relies on over a tmp_path tree, so every test runs on every OS.  The
real-kernel counterpart is tests/test_cgroups_live.py.
"""

import asyncio
import errno
import logging
import os
import re
import signal
import threading

import pytest

from cronstable import _cgroups
from cronstable._cgroups import Backend, CgroupFS, Container, _ChildJoin
from cronstable.config import ResourceLimits
from cronstable.limits import LimitsManager
from cronstable.resources import DAEMON_LEAF, RUNS_DIR, ResourceMonitor
from tests._fake_cgroupfs import FakeKernel

MIB = 1024 * 1024
SIGKILL = getattr(signal, "SIGKILL", 9)
ALL = ResourceLimits(cpu=1.5, memory=512 * MIB + 123, processes=64)


def _kernel(tmp_path, **kw):
    return FakeKernel(tmp_path, **kw)


def _ready(tmp_path, *, cpu=True, **kw):
    """A kernel and a backend whose layout is set up.

    ``cpu`` also turns cpu control on, as a job with a cpu cap does.
    """
    kernel = _kernel(tmp_path, **kw)
    backend = Backend(kernel)
    cap = backend.setup()
    assert cap.available, cap
    if cpu and "cpu" in cap.resources:
        backend.set_cpu(True)
        assert backend.weighting
    return kernel, backend


def _subtree_writes(kernel):
    return [
        (rel, data)
        for rel, name, data in kernel.writes
        if name == "cgroup.subtree_control"
    ]


def _base(kernel):
    return kernel.path(kernel.own)


def _runs(kernel):
    return os.path.join(_base(kernel), RUNS_DIR)


def _close(container):
    container._settle_join()


# --- probe -----------------------------------------------------------------


def test_probe_finds_a_delegated_cgroup(tmp_path):
    kernel = _kernel(tmp_path)
    cap = Backend(kernel).capability()
    assert cap.available
    assert cap.mechanism == "cgroup-v2"
    assert cap.resources == {"cpu", "memory", "processes"}
    assert cap.reason is None and cap.hint is None
    # read-only: nothing written, no layout made
    assert kernel.writes == []
    assert not os.path.exists(os.path.join(_base(kernel), DAEMON_LEAF))


def test_probe_needs_a_v2_mount(tmp_path):
    kernel = _kernel(tmp_path)
    os.unlink(kernel.path("", "cgroup.controllers"))
    cap = Backend(kernel).capability()
    assert not cap.available
    assert cap.reason == "no cgroup v2 hierarchy is mounted at {}".format(
        kernel.root
    )
    assert "unified cgroup v2" in cap.hint


@pytest.mark.parametrize(
    "line",
    [
        pytest.param("12:memory:/foo\n", id="v1-only"),
        pytest.param("0::/not/mounted/here\n", id="own-dir-missing"),
        pytest.param(None, id="unreadable"),
    ],
)
def test_probe_needs_its_own_cgroup(tmp_path, line):
    kernel = _kernel(tmp_path)
    self_cgroup = os.path.join(kernel.proc, "self", "cgroup")
    if line is None:
        os.unlink(self_cgroup)
    else:
        with open(self_cgroup, "w", encoding="ascii") as fh:
            fh.write(line)
    cap = Backend(kernel).capability()
    assert not cap.available
    assert cap.reason.startswith("cannot find this process's cgroup")
    assert cap.hint == _cgroups._DELEGATE_HINT
    assert "Delegate=memory pids" in cap.hint


def test_probe_refuses_the_true_root(tmp_path):
    kernel = _kernel(tmp_path, own="")
    cap = Backend(kernel).capability()
    assert not cap.available
    assert cap.reason == "cronstable runs in the root cgroup"


def test_probe_accepts_a_namespace_root_as_root(tmp_path):
    # a cgroup-namespace root has cgroup.type, and is its own delegation
    # evidence for root (no xattrs inside a container)
    kernel = _kernel(tmp_path, own="", ns_root=True, euid=0)
    cap = Backend(kernel).capability()
    assert cap.available, cap


@pytest.mark.parametrize(
    "name", ["", "cgroup.procs", "cgroup.subtree_control"]
)
def test_probe_needs_writable_delegated_files(tmp_path, name):
    kernel = _kernel(tmp_path)
    base = _base(kernel)
    path = os.path.join(base, name) if name else base
    kernel.unwritable.add(path)
    cap = Backend(kernel).capability()
    assert not cap.available
    assert cap.reason == (
        "cgroup {} is not delegated to cronstable ({} is not "
        "writable)".format(base, path)
    )


def test_probe_reports_a_read_only_mount(tmp_path):
    kernel = _kernel(tmp_path, euid=0, own="", ns_root=True)
    kernel.readonly = True
    cap = Backend(kernel).capability()
    assert not cap.available
    assert "is not writable" in cap.reason


@pytest.mark.parametrize(
    "xattrs, available",
    [
        pytest.param({}, False, id="none"),
        pytest.param({"trusted.delegate": b"1"}, True, id="trusted"),
        pytest.param({"user.delegate": b"1"}, True, id="user"),
        pytest.param({"trusted.delegate": b"0"}, False, id="trusted-0"),
    ],
)
def test_probe_as_root_needs_the_delegate_attribute(
    tmp_path, xattrs, available
):
    kernel = _kernel(tmp_path, euid=0)
    for name, value in xattrs.items():
        kernel.xattrs[(kernel.own, name)] = value
    cap = Backend(kernel).capability()
    assert cap.available is available
    if not available:
        assert cap.reason == (
            "cgroup {} is not delegated to cronstable (it has no "
            "trusted.delegate or user.delegate attribute)".format(
                _base(kernel)
            )
        )


def test_probe_as_root_survives_unsupported_xattrs(tmp_path):
    # pre-5.7 kernels: EOPNOTSUPP for user.*; ENODATA for an absent name
    kernel = _kernel(tmp_path, euid=0)
    kernel.faults[("getxattr", kernel.own)] = [errno.EOPNOTSUPP]
    kernel.xattrs[(kernel.own, "user.delegate")] = b"1"
    assert Backend(kernel).capability().available


def test_probe_reports_missing_controllers(tmp_path):
    kernel = _kernel(tmp_path, controllers=("cpu", "memory"))
    cap = Backend(kernel).capability()
    assert cap.available
    assert cap.resources == {"cpu", "memory"}
    assert cap.reason == (
        "cgroup {} does not offer the pids controller".format(_base(kernel))
    )
    assert cap.hint == (
        "Delegate the memory and pids controllers to cronstable's cgroup"
    )


def test_probe_reports_several_missing_controllers(tmp_path):
    kernel = _kernel(tmp_path, controllers=("cpu", "io"))
    cap = Backend(kernel).capability()
    assert cap.resources == {"cpu"}
    assert cap.reason.endswith("the memory and pids controllers")


@pytest.mark.parametrize(
    "own, hint",
    [
        pytest.param(
            "system.slice/cronstable.service",
            "To cap CPU, run systemctl edit cronstable and add Delegate=yes "
            "under [Service]; that makes cronstable a CPU group of its own "
            "among the system's services",
            id="system-service",
        ),
        pytest.param(
            "user.slice/user-1000.slice/user@1000.service/app.slice/"
            "cron.service",
            "To cap CPU, run systemctl --user edit cron and add Delegate=yes "
            "under [Service]; that makes cron a CPU group of its own among "
            "the user's services",
            id="user-service",
        ),
        pytest.param(
            "user.slice/app.scope",
            "Delegate the cpu controller to cronstable's cgroup, for example "
            "with Delegate=yes on its systemd unit",
            id="elsewhere",
        ),
    ],
)
def test_probe_without_the_cpu_controller(tmp_path, own, hint):
    # the packaged unit's Delegate=memory pids: memory and process caps
    # apply, and a cpu cap names what to change
    kernel = _kernel(tmp_path, own=own, controllers=("memory", "pids"))
    cap = Backend(kernel).capability()
    assert cap.available
    assert cap.resources == {"memory", "processes"}
    assert cap.reason == "the cpu controller is not delegated to cronstable"
    assert cap.hint == hint


def test_probe_without_cpu_or_pids_names_both(tmp_path):
    kernel = _kernel(
        tmp_path, own="system.slice/c.service", controllers=("memory",)
    )
    cap = Backend(kernel).capability()
    assert cap.resources == {"memory"}
    assert cap.reason == (
        "cgroup {} does not offer the pids controller; the cpu controller is "
        "not delegated to cronstable".format(_base(kernel))
    )
    assert cap.hint.startswith(
        "Delegate the memory and pids controllers to cronstable's cgroup. "
        "To cap CPU, run systemctl edit c "
    )


def test_probe_without_any_controller_is_unavailable(tmp_path):
    kernel = _kernel(tmp_path, controllers=("io",))
    cap = Backend(kernel).capability()
    assert not cap.available
    assert "offers none of the cpu, memory, and pids" in cap.reason


def test_probe_with_unreadable_controllers_is_unavailable(tmp_path):
    kernel = _kernel(tmp_path)
    os.unlink(kernel.path(kernel.own, "cgroup.controllers"))
    assert not Backend(kernel).capability().available


def test_probe_recognizes_the_layout_it_built(tmp_path):
    # after setup the daemon sits in base/daemon; the probe (and the node
    # reader) still resolve base.
    kernel, backend = _ready(tmp_path)
    assert kernel.cgroup_of(kernel.daemon_pid) == kernel.own + "/daemon"
    assert backend.capability().available
    assert backend._probe()[1] == _base(kernel)


# --- setup -----------------------------------------------------------------


def test_setup_moves_every_process_and_enables_in_one_write(tmp_path):
    kernel = _kernel(tmp_path, others=(99, 101))
    backend = Backend(kernel)
    cap = backend.setup()
    assert cap.available
    # cpu is offered, and stays off until a job caps CPU
    assert cap.resources == {"cpu", "memory", "processes"}
    daemon = kernel.own + "/" + DAEMON_LEAF
    runs = kernel.own + "/" + RUNS_DIR
    for pid in (99, 100, 101):
        assert kernel.cgroup_of(pid) == daemon
    assert _subtree_writes(kernel) == [
        (kernel.own, "+memory +pids"),
        (runs, "+memory +pids"),
    ]
    assert kernel.file(kernel.own, "cgroup.type") == "domain\n"
    assert kernel.subtree(kernel.own) == {"memory", "pids"}
    assert kernel.subtree(runs) == {"memory", "pids"}
    assert not backend.weighting
    assert kernel.sleeps == []


def test_setup_enables_only_what_is_missing(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.set_file(kernel.own, "cgroup.subtree_control", "memory\n")
    assert Backend(kernel).setup().available
    assert _subtree_writes(kernel)[0] == (kernel.own, "+pids")


def test_one_controller_at_a_time_would_spring_the_trap(tmp_path):
    # the model's threaded-domain rule, which the single write avoids
    kernel = _kernel(tmp_path)
    kernel.write(kernel.path(kernel.own, "cgroup.subtree_control"), "+cpu")
    assert kernel.file(kernel.own, "cgroup.type") == "domain threaded\n"
    with pytest.raises(OSError) as raised:
        kernel.write(
            kernel.path(kernel.own, "cgroup.subtree_control"), "+memory"
        )
    assert raised.value.errno == errno.EBUSY


def test_setup_retries_when_a_process_arrives(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.arrivals = [555]
    assert Backend(kernel).setup().available
    assert kernel.cgroup_of(555) == kernel.own + "/" + DAEMON_LEAF
    enables = [w for w in _subtree_writes(kernel) if w[0] == kernel.own]
    assert len(enables) == 2
    assert kernel.sleeps == [0.005]


# the pause before each round after the first: 5 ms doubling to 0.5 s
_PAUSES = [0.005, 0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.5, 0.5, 0.5]


def test_setup_gives_up_after_bounded_rounds(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.arrivals = list(range(1000, 1000 + _cgroups._SETUP_ROUNDS))
    backend = Backend(kernel)
    cap = backend.setup()
    assert not cap.available
    assert cap.reason == (
        "could not set up cgroup {}: processes keep arriving in it".format(
            _base(kernel)
        )
    )
    assert cap.hint == _cgroups._DELEGATE_HINT
    assert kernel.sleeps == pytest.approx(_PAUSES)
    assert sum(kernel.sleeps) == pytest.approx(2.135)
    # not cached: the next setup tries again and succeeds
    assert backend.setup().available


def test_setup_waits_for_an_exiting_process(tmp_path):
    # A task in its exit path cannot move, yet counts until it is gone
    # (a large one takes a while to free its memory).
    kernel = _kernel(tmp_path, others=(300,))
    kernel.exiting.add(300)

    def gone(pauses):
        if pauses == 4:
            kernel.exit(300)

    kernel.on_sleep = gone
    assert Backend(kernel).setup().available
    assert kernel.sleeps == pytest.approx(_PAUSES[:4])
    assert kernel.subtree(kernel.own) == {"memory", "pids"}


@pytest.mark.parametrize(
    "stuck, text",
    [
        pytest.param(
            (300,), "process 300 in it could not be moved (it may be exiting)",
            id="one",
        ),
        pytest.param(
            (300, 301),
            "processes 300, 301 in it could not be moved (they may be "
            "exiting)",
            id="several",
        ),
    ],
)
def test_setup_names_a_process_that_never_leaves(tmp_path, stuck, text):
    kernel = _kernel(tmp_path, others=stuck)
    kernel.exiting.update(stuck)
    backend = Backend(kernel)
    cap = backend.setup()
    assert not cap.available
    assert cap.reason == "could not set up cgroup {}: {}".format(
        _base(kernel), text
    )
    # delegation is fine: no hint sends the operator to check it
    assert cap.hint is None
    assert kernel.sleeps == pytest.approx(_PAUSES)
    for pid in stuck:
        kernel.exit(pid)
    assert backend.setup().available


def test_setup_ignores_a_process_that_exited(tmp_path):
    kernel = _kernel(tmp_path, others=(99,))
    kernel.faults[("write", "daemon/cgroup.procs")] = [errno.ESRCH]
    assert Backend(kernel).setup().available


def test_setup_reports_an_unexpected_errno(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.faults[("write", "daemon/cgroup.procs")] = errno.EACCES
    cap = Backend(kernel).setup()
    assert not cap.available
    assert cap.reason == "could not set up cgroup {}: {}".format(
        _base(kernel), os.strerror(errno.EACCES)
    )
    assert cap.hint == _cgroups._DELEGATE_HINT


@pytest.mark.parametrize(
    "where", ["app.scope/cgroup.subtree_control", "runs/cgroup.subtree_control"]
)
def test_setup_failure_while_enabling_is_reported(tmp_path, where):
    kernel = _kernel(tmp_path)
    kernel.faults[("write", where)] = errno.EACCES
    cap = Backend(kernel).setup()
    assert not cap.available
    assert cap.reason.endswith(os.strerror(errno.EACCES))


@pytest.mark.parametrize("cpu", [True, False], ids=["cpu-on", "cpu-off"])
def test_a_restarted_daemon_reuses_the_layout(tmp_path, cpu):
    kernel, _ = _ready(tmp_path, cpu=cpu)
    before = len(kernel.writes)
    # a new process in the same cgroup (a daemon restart in place)
    restarted = Backend(kernel)
    assert restarted.setup().available
    assert [w for w in kernel.writes[before:] if w[1] != "cgroup.procs"] == []
    # it finds cpu as the earlier daemon left it, and can turn it off
    assert restarted.weighting is cpu
    restarted.set_cpu(False)
    assert not restarted.weighting
    assert "cpu" not in kernel.subtree(kernel.own)


def test_setup_of_an_unavailable_host_does_nothing(tmp_path):
    kernel = _kernel(tmp_path, controllers=("io",))
    assert not Backend(kernel).setup().available
    assert kernel.writes == []


def test_setup_is_idempotent(tmp_path):
    kernel, backend = _ready(tmp_path)
    before = list(kernel.writes)
    assert backend.setup().available
    assert kernel.writes == before


def test_setup_keeps_an_existing_layout(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.mkdir(os.path.join(_base(kernel), DAEMON_LEAF))
    kernel.mkdir(_runs(kernel))
    assert Backend(kernel).setup().available


def test_setup_sweeps_empty_leftover_runs_once(tmp_path):
    # Setup runs on a worker thread, so it removes an earlier daemon's
    # empty run cgroups itself (child cgroups too).  One with a process in
    # it is left alone and never queued: it may be another daemon's.
    kernel = _kernel(tmp_path)
    runs = _runs(kernel)
    kernel.mkdir(runs)
    empty, nested, busy = (
        os.path.join(runs, n) for n in ("old-empty", "old-nested", "old-busy")
    )
    for path in (empty, nested, os.path.join(nested, "sub"), busy):
        kernel.mkdir(path)
    kernel.mkdir(os.path.join(busy, "idle"))
    kernel.spawn(300, kernel.rel_of(busy))
    backend = Backend(kernel)
    assert backend.setup().available
    assert not os.path.exists(empty) and not os.path.exists(nested)
    assert os.path.isdir(os.path.join(busy, "idle"))
    assert busy not in kernel.rmdirs
    assert backend.pending == frozenset()
    kernel.exit(300)
    backend.collect()
    assert os.path.isdir(busy)  # not this daemon's to remove


def test_a_second_daemon_leaves_the_first_ones_runs_alone(tmp_path):
    # A cronstable started as an uncapped job of another lands in its
    # daemon leaf and shares its base.  Its setup must not take the first
    # daemon's live run cgroups, or their counters are gone at finish.
    kernel, first = _ready(tmp_path)
    container = _running(kernel, first, caps=ResourceLimits(memory=MIB))
    kernel.spawn(600, kernel.own + "/" + DAEMON_LEAF)
    second = Backend(kernel)
    assert second.setup().available
    assert second.pending == frozenset()
    rel = kernel.rel_of(container.path)
    kernel.set_file(rel, "memory.events", "oom 1\noom_kill 1\n")
    kernel.exit(500)
    second.collect()
    assert os.path.isdir(container.path)
    assert container.finish()["memory_exceeded"] is True
    assert not os.path.exists(container.path)


def test_setup_survives_an_unlistable_runs_dir(tmp_path, monkeypatch):
    kernel = _kernel(tmp_path)
    listed = kernel.subdirs

    def broken(path):
        if path.endswith(RUNS_DIR):
            raise OSError(errno.EIO, "io")
        return listed(path)

    monkeypatch.setattr(kernel, "subdirs", broken)
    assert Backend(kernel).setup().available


def test_setup_skips_a_leftover_it_cannot_read(tmp_path):
    kernel = _kernel(tmp_path)
    runs = _runs(kernel)
    kernel.mkdir(runs)
    odd = os.path.join(runs, "old-odd")
    kernel.mkdir(odd)
    kernel.set_file(kernel.rel_of(odd), "cgroup.procs", "x\n")
    assert Backend(kernel).setup().available
    assert os.path.isdir(odd) and odd not in kernel.rmdirs


@pytest.mark.parametrize(
    "text, fix", [("-1000\n", True), ("0\n", False), ("junk\n", False)]
)
def test_setup_reads_the_daemon_oom_score_adj(tmp_path, text, fix):
    kernel = _kernel(tmp_path)
    with open(
        os.path.join(kernel.proc, "self", "oom_score_adj"),
        "w",
        encoding="ascii",
    ) as fh:
        fh.write(text)
    backend = Backend(kernel)
    backend.setup()
    assert backend._fix_oom_adj is fix


def test_missing_oom_score_adj_reads_as_zero(tmp_path):
    kernel = _kernel(tmp_path)
    os.unlink(os.path.join(kernel.proc, "self", "oom_score_adj"))
    backend = Backend(kernel)
    backend.setup()
    assert backend._fix_oom_adj is False


# --- cpu control -----------------------------------------------------------


def _cpu_files(kernel, rel):
    return sorted(
        n for n in os.listdir(kernel.path(rel)) if n.startswith("cpu.")
    )


def test_cpu_goes_on_in_base_then_runs(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.nice_value = 5
    before = len(kernel.writes)
    cap = backend.set_cpu(True)
    assert cap.resources == {"cpu", "memory", "processes"}
    assert backend.weighting
    runs = kernel.own + "/" + RUNS_DIR
    daemon = kernel.own + "/" + DAEMON_LEAF
    assert [(r, n, d) for r, n, d in kernel.writes[before:]] == [
        (kernel.own, "cgroup.subtree_control", "+cpu"),
        (runs, "cgroup.subtree_control", "+cpu"),
        # the daemon's leaf weighs as the daemon's own nice
        (daemon, "cpu.weight.nice", "5"),
    ]
    assert kernel.subtree(kernel.own) == {"cpu", "memory", "pids"}
    assert kernel.subtree(runs) == {"cpu", "memory", "pids"}
    # already on: nothing more to write
    before = len(kernel.writes)
    assert backend.set_cpu(True) == cap
    assert kernel.writes[before:] == []


def test_cpu_goes_off_in_runs_then_base(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(memory=MIB), "j")
    rel = kernel.rel_of(container.path)
    assert "cpu.weight.nice" in _cpu_files(kernel, rel)
    before = len(kernel.writes)
    assert backend.set_cpu(False).resources == {"cpu", "memory", "processes"}
    assert not backend.weighting
    assert _subtree_writes(kernel)[-2:] == [
        (kernel.own + "/" + RUNS_DIR, "-cpu"),
        (kernel.own, "-cpu"),
    ]
    assert len(kernel.writes) == before + 2
    assert kernel.subtree(kernel.own) == {"memory", "pids"}
    # the run's cgroup is no CPU group any more
    assert _cpu_files(kernel, rel) == ["cpu.stat"]
    backend.set_cpu(False)  # already off
    assert len(kernel.writes) == before + 2
    _close(container)


def test_cpu_moves_a_stray_process_out_of_base_first(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.spawn(777, kernel.own)
    assert "cpu" in backend.set_cpu(True).resources
    assert kernel.cgroup_of(777) == kernel.own + "/" + DAEMON_LEAF


def test_cpu_stays_on_while_a_cgroup_below_enables_it(tmp_path, caplog):
    # a job that enabled cpu for child cgroups of its own run cgroup
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, caps=ResourceLimits(memory=MIB))
    rel = kernel.rel_of(container.path)
    kernel.write(kernel.path(rel, "cgroup.subtree_control"), "+cpu")
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        assert "cpu" in backend.set_cpu(False).resources
    assert backend.weighting
    assert "cpu" in kernel.subtree(kernel.own)
    assert "the cpu controller stays on under {}: {}".format(
        _base(kernel), os.strerror(errno.EBUSY)
    ) in caplog.text
    assert all(r.levelno == logging.DEBUG for r in caplog.records)
    kernel.write(kernel.path(rel, "cgroup.subtree_control"), "-cpu")
    backend.set_cpu(False)
    assert not backend.weighting
    assert "cpu" not in kernel.subtree(kernel.own)


@pytest.mark.parametrize(
    "code", [errno.EINVAL, errno.EBUSY], ids=["rt-group-sched", "busy"]
)
def test_a_refused_cpu_keeps_memory_and_process_caps(tmp_path, code):
    # A kernel with RT group scheduling refuses +cpu while an RT task
    # runs: the cpu cap is off with the errno text, the others apply.
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.refuse["cpu"] = code
    cap = backend.set_cpu(True)
    assert cap.available
    assert cap.resources == {"memory", "processes"}
    reason = "could not enable the cpu controller in {}: {}".format(
        _base(kernel), os.strerror(code)
    )
    assert cap.reason == reason
    assert backend.setup() == cap
    assert not backend.weighting
    assert kernel.subtree(kernel.own) == {"memory", "pids"}
    container = backend.create(ResourceLimits(memory=MIB, processes=4), "j")
    assert container.applied == {"memory", "processes"}
    _close(container)
    # a failed attempt repeats only when asked to
    before = len(kernel.writes)
    assert backend.set_cpu(True) == cap
    assert kernel.writes[before:] == []
    del kernel.refuse["cpu"]
    assert backend.set_cpu(True, retry=True).resources == {
        "cpu",
        "memory",
        "processes",
    }
    assert backend.weighting and backend.setup().reason is None


def test_turning_cpu_off_forgets_a_failure(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.refuse["cpu"] = errno.EINVAL
    assert "cpu" not in backend.set_cpu(True).resources
    assert backend.set_cpu(False).reason is None
    assert "cpu" in backend.setup().resources


def test_a_failure_in_runs_turns_base_off_again(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.faults[("write", "runs/cgroup.subtree_control")] = [errno.EIO]
    cap = backend.set_cpu(True)
    assert cap.reason.endswith(os.strerror(errno.EIO))
    assert _subtree_writes(kernel)[-1] == (kernel.own, "-cpu")
    assert kernel.subtree(kernel.own) == {"memory", "pids"}
    assert not backend.weighting


def test_cpu_failure_joins_an_earlier_reason(tmp_path):
    kernel, backend = _ready(
        tmp_path, cpu=False, controllers=("cpu", "memory")
    )
    kernel.refuse["cpu"] = errno.EINVAL
    cap = backend.set_cpu(True)
    assert cap.resources == {"memory"}
    assert cap.reason == (
        "cgroup {} does not offer the pids controller; could not enable the "
        "cpu controller in {}: {}".format(
            _base(kernel), _base(kernel), os.strerror(errno.EINVAL)
        )
    )
    assert cap.hint == _cgroups._CONTROLLER_HINT


def test_cpu_needs_a_host_that_offers_it(tmp_path):
    kernel, backend = _ready(tmp_path, controllers=("memory", "pids"))
    before = len(kernel.writes)
    cap = backend.set_cpu(True)
    assert cap.reason == "the cpu controller is not delegated to cronstable"
    assert kernel.writes[before:] == [] and not backend.weighting


def test_cpu_control_reads_what_another_daemon_did(tmp_path):
    # two daemons sharing one base (one started as a job of the other)
    kernel, first = _ready(tmp_path, cpu=False)
    second = Backend(kernel)
    assert second.setup().available
    first.set_cpu(True)
    assert not second.weighting  # it has not looked yet
    second.set_cpu(False)
    assert "cpu" not in kernel.subtree(kernel.own)
    first.set_cpu(True)
    assert first.weighting
    assert "cpu" in kernel.subtree(kernel.own + "/" + RUNS_DIR)


def test_cpu_control_survives_an_unreadable_subtree(tmp_path, monkeypatch):
    kernel, backend = _ready(tmp_path, cpu=False)
    read = kernel.read

    def broken(path):
        if path.endswith("cgroup.subtree_control"):
            raise OSError(errno.EIO, os.strerror(errno.EIO))
        return read(path)

    monkeypatch.setattr(kernel, "read", broken)
    cap = backend.set_cpu(True)
    assert cap.reason == "could not enable the cpu controller in {}: {}".format(
        _base(kernel), os.strerror(errno.EIO)
    )
    assert not backend.weighting


def test_cpu_control_before_setup_does_nothing(tmp_path):
    kernel = _kernel(tmp_path)
    backend = Backend(kernel)
    cap = backend.set_cpu(True)
    assert not cap.available
    assert cap.reason == "the cgroup layout is not set up"
    assert kernel.writes == []


def test_an_unwritable_daemon_weight_is_only_logged(tmp_path, caplog):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.faults[("write", "daemon/cpu.weight.nice")] = errno.EACCES
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        assert "cpu" in backend.set_cpu(True).resources
    assert backend.weighting
    assert "the daemon's cpu weight is not set: could not set " in (
        caplog.text
    )


# --- cpu control while runs have cpu caps ----------------------------------


def _quota(kernel, container):
    return kernel.file(kernel.rel_of(container.path), "cpu.max")


def _soon_calls(backend):
    """Record what the backend hands to a thread, and start none."""
    calls = []
    backend._soon = calls.append
    return calls


def test_cpu_off_waits_for_a_running_cpu_cap(tmp_path, caplog):
    # a reload drops the last cpu cap while a run with one is going
    kernel, backend = _ready(tmp_path)
    caps = ResourceLimits(cpu=1, memory=MIB)
    container = _running(kernel, backend, caps=caps)
    before = len(kernel.writes)
    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        cap = backend.set_cpu(False)
    assert cap.resources == {"cpu", "memory", "processes"}
    assert kernel.writes[before:] == []
    # the run keeps its cap, and cpu stays on until it ends
    assert _quota(kernel, container) == "100000 100000\n"
    assert backend.weighting
    assert "cpu" in kernel.subtree(kernel.own)
    assert (
        "the cpu controller stays on under {} until the runs with a cpu cap "
        "end".format(_base(kernel))
    ) in caplog.text
    kernel.exit(500)
    container.finish()
    assert not backend.weighting
    assert _subtree_writes(kernel)[-2:] == [
        (kernel.own + "/" + RUNS_DIR, "-cpu"),
        (kernel.own, "-cpu"),
    ]
    assert "cpu" not in kernel.subtree(kernel.own)


def test_the_last_cpu_cap_to_end_turns_cpu_off(tmp_path):
    kernel, backend = _ready(tmp_path)
    first = _running(kernel, backend, caps=ResourceLimits(cpu=1), pids=(500,))
    second = _running(kernel, backend, caps=ResourceLimits(cpu=2), pids=(501,))
    # neither a weight nor a memory cap holds cpu on
    weighed = backend.create(ResourceLimits(), "plain")
    memory = backend.create(ResourceLimits(memory=MIB), "m")
    backend.set_cpu(False)
    kernel.exit(500)
    first.finish()
    assert backend.weighting
    assert _quota(kernel, second) == "200000 100000\n"
    kernel.exit(501)
    second.finish()
    assert not backend.weighting
    assert "cpu" not in kernel.subtree(kernel.own)
    for container in (weighed, memory):
        container.finish()


def test_a_cpu_run_after_cpu_went_off_turns_it_on_and_off_again(tmp_path):
    # a verify check under the previous config keeps its cpu cap
    kernel, backend = _ready(tmp_path)
    backend.set_cpu(False)
    assert not backend.weighting
    cap = backend.need_cpu()
    assert "cpu" in cap.resources and backend.weighting
    container = _running(kernel, backend, caps=ResourceLimits(cpu=0.5))
    assert container.applied == {"cpu"}
    assert _quota(kernel, container) == "50000 100000\n"
    backend.collect()  # another run's start or end
    assert backend.weighting
    kernel.exit(500)
    container.finish()
    assert not backend.weighting
    assert "cpu" not in kernel.subtree(kernel.own)


def test_a_cpu_cap_turns_cpu_on_as_it_is_written(tmp_path):
    # cpu went off between a run's prepare and its create
    kernel, backend = _ready(tmp_path, cpu=False)
    container = backend.create(ResourceLimits(cpu=1), "j")
    assert container.applied == {"cpu"} and container.unenforced == {}
    assert backend.weighting
    assert _quota(kernel, container) == "100000 100000\n"
    _close(container)


def test_another_daemons_cpu_cap_keeps_cpu_on(tmp_path):
    # two daemons sharing one base: one's converge must not lift the
    # other's cap
    kernel, first = _ready(tmp_path)
    second = Backend(kernel)
    assert second.setup().available
    container = _running(kernel, first, caps=ResourceLimits(cpu=1))
    second.set_cpu(False)
    assert first.weighting and second.weighting
    assert _quota(kernel, container) == "100000 100000\n"
    kernel.exit(500)
    container.finish()  # the first daemon still wants cpu
    assert "cpu" in kernel.subtree(kernel.own)
    second.collect()  # the second daemon's next run start or end
    assert not second.weighting
    assert "cpu" not in kernel.subtree(kernel.own)


def test_this_daemons_leftovers_hold_no_cpu(tmp_path):
    # a leftover keeps a finished run's cgroup, but that run is over
    kernel, backend = _ready(tmp_path)
    container = _running(
        kernel, backend, caps=ResourceLimits(cpu=1), pids=(500, 501)
    )
    kernel.exit(500)
    container.finish()
    assert backend.pending == {container.path}
    backend.set_cpu(False)
    assert not backend.weighting


def _clocked(backend):
    """Give ``backend`` a clock that the test moves; returns its reading."""
    now = [0.0]
    backend._clock = lambda: now[0]
    return now


def test_an_earlier_daemons_cpu_cgroup_holds_cpu_only_while_busy(tmp_path):
    # a restart in place leaves a cpu-capped run's leftover behind
    kernel, first = _ready(tmp_path)
    stale = _running(
        kernel, first, caps=ResourceLimits(cpu=0.5), pids=(500, 501)
    )
    kernel.exit(500)
    restarted = Backend(kernel)
    now = _clocked(restarted)
    assert restarted.setup().available and restarted.weighting
    restarted.set_cpu(False)
    now[0] += 120
    restarted.collect()
    assert restarted.weighting  # the leftover keeps its cap
    kernel.exit(501)
    restarted.collect()
    assert not restarted.weighting
    assert "cpu" not in kernel.subtree(kernel.own)
    assert os.path.isdir(stale.path)  # not this daemon's to remove


def test_this_daemons_own_cpu_run_holds_cpu_past_the_grace(tmp_path):
    kernel, backend = _ready(tmp_path)
    now = _clocked(backend)
    container = backend.create(ResourceLimits(cpu=1), "j")  # not joined
    backend.set_cpu(False)
    now[0] += 120
    backend.collect()
    assert backend.weighting
    assert _quota(kernel, container) == "100000 100000\n"
    container.finish()
    assert not backend.weighting


def test_another_daemons_cgroup_holds_cpu_before_its_child_joins(tmp_path):
    kernel, first = _ready(tmp_path)
    second = Backend(kernel)
    now = _clocked(second)
    assert second.setup().available
    joining = first.create(ResourceLimits(cpu=1), "j")  # empty until the join
    second.set_cpu(False)
    now[0] += 59
    second.collect()
    assert second.weighting
    assert _quota(kernel, joining) == "100000 100000\n"
    # a child that never joined leaves it empty for good
    now[0] += 1
    second.collect()
    assert not second.weighting
    _close(joining)


def _weight(kernel, container):
    return kernel.file(kernel.rel_of(container.path), "cpu.weight.nice")


def test_cpu_turning_on_weighs_the_runs_already_going(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    kernel.nice_value = 7
    idle = backend.create(ResourceLimits(memory=MIB), "idle", nice=19)
    normal = backend.create(ResourceLimits(processes=8), "normal")
    done = _running(
        kernel, backend, caps=ResourceLimits(memory=MIB), pids=(500, 501)
    )
    kernel.exit(500)
    done.finish()  # its leftover keeps the cgroup, but the run is over
    aborted = backend.create(ResourceLimits(memory=MIB), "aborted", nice=19)
    aborted.abort()  # queued for collect, and its run never went
    backend.set_cpu(True)
    assert _weight(kernel, idle) == "19\n"
    assert _weight(kernel, normal) == "7\n"
    assert _weight(kernel, done) == "0\n"
    assert _weight(kernel, aborted) == "0\n"
    for container in (idle, normal):
        _close(container)


def test_cpu_that_another_daemon_turned_on_weighs_this_ones_runs(tmp_path):
    kernel, first = _ready(tmp_path, cpu=False)
    second = Backend(kernel)
    assert second.setup().available
    idle = first.create(ResourceLimits(memory=MIB), "idle", nice=19)
    second.set_cpu(True)
    assert _weight(kernel, idle) == "0\n"  # the other daemon weighs its own
    first.set_cpu(True)  # this daemon's next config apply
    assert first.weighting
    assert _weight(kernel, idle) == "19\n"
    _close(idle)


def test_a_weight_comes_back_when_cpu_turns_on_again(tmp_path):
    kernel, backend = _ready(tmp_path)
    plain = backend.create(ResourceLimits(), "plain", nice=10)
    backend.set_cpu(False)
    assert not backend.weighting
    backend.need_cpu()
    assert backend.weighting
    assert _weight(kernel, plain) == "10\n"
    _close(plain)


@pytest.mark.parametrize("what", ["cpu.max", "runs"])
def test_what_cannot_be_read_holds_no_cpu(tmp_path, monkeypatch, what):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, caps=ResourceLimits(cpu=1))
    read, subdirs = kernel.read, kernel.subdirs

    def no_quota(path):
        if path.endswith("cpu.max"):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        return read(path)

    def no_listing(path):
        if path == _runs(kernel):
            raise OSError(errno.EIO, os.strerror(errno.EIO), path)
        return subdirs(path)

    if what == "cpu.max":
        monkeypatch.setattr(kernel, "read", no_quota)
    else:
        monkeypatch.setattr(kernel, "subdirs", no_listing)
    backend.set_cpu(False)
    assert not backend.weighting
    _close(container)


def test_an_aborted_cpu_run_lets_cpu_go_off_from_a_thread(tmp_path):
    kernel, backend = _ready(tmp_path)
    calls = _soon_calls(backend)
    container = backend.create(ResourceLimits(cpu=1), "j")
    backend.set_cpu(False)
    assert backend.weighting  # the run about to start keeps its cap
    container.abort()
    # abort can run on the event loop: the collect waits for a thread
    assert calls == [backend.collect] and backend.weighting
    calls[0]()
    assert not backend.weighting
    assert not os.path.exists(container.path)


def test_an_aborted_run_leaves_cpu_on_while_it_is_wanted(tmp_path):
    kernel, backend = _ready(tmp_path)
    calls = _soon_calls(backend)
    backend.create(ResourceLimits(cpu=1), "j").abort()
    backend.set_cpu(False)
    backend.create(ResourceLimits(memory=MIB), "j").abort()
    assert calls == []


def test_the_backend_hands_calls_to_a_daemon_thread(tmp_path):
    assert Backend(_kernel(tmp_path))._soon is _cgroups._in_thread
    ran = []
    done = threading.Event()

    def call():
        ran.append(threading.current_thread())
        done.set()

    _cgroups._in_thread(call)
    assert done.wait(10)
    assert ran[0] is not threading.current_thread() and ran[0].daemon


def test_cpu_caps_hold_through_converges_on_other_threads(tmp_path):
    # Converge runs on a worker thread while runs prepare and finish on
    # others: no run loses its cpu cap, and cpu goes off after the last.
    kernel, backend = _ready(tmp_path)
    lifted = []

    def runs():
        for _ in range(25):
            container = backend.create(ResourceLimits(cpu=1), "j")
            try:
                quota = kernel.read(os.path.join(container.path, "cpu.max"))
            except OSError as exc:
                quota = exc.strerror
            if quota != "100000 100000\n":
                lifted.append(quota)
            container.finish()

    workers = [threading.Thread(target=runs) for _ in range(4)]
    for worker in workers:
        worker.start()
    want = False
    while any(worker.is_alive() for worker in workers):
        backend.set_cpu(want)
        want = not want
    for worker in workers:
        worker.join()
    backend.set_cpu(False)
    assert lifted == []
    assert not backend.weighting
    assert kernel.subdirs(_runs(kernel)) == []


# --- per-run create --------------------------------------------------------


def test_create_writes_every_cap(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "nightly backup/db")
    try:
        name = os.path.basename(container.path)
        assert re.fullmatch(r"[0-9a-f]{12}-nightly_backup_db", name), name
        assert os.path.dirname(container.path) == _runs(kernel)
        rel = kernel.rel_of(container.path)
        assert container.applied == {"cpu", "memory", "processes"}
        assert container.unenforced == {}
        assert kernel.file(rel, "cpu.max") == "150000 100000\n"
        assert kernel.file(rel, "memory.max") == "{}\n".format(512 * MIB)
        assert kernel.file(rel, "memory.swap.max") == "0\n"
        assert kernel.file(rel, "memory.oom.group") == "1\n"
        assert kernel.file(rel, "pids.max") == "64\n"
        assert kernel.opened == [os.path.join(container.path, "cgroup.procs")]
    finally:
        _close(container)


@pytest.mark.parametrize(
    "cpu, written",
    [
        pytest.param(0.001, "1000 100000", id="floor"),
        pytest.param(0.25, "25000 100000", id="quarter"),
        pytest.param(1e12, "{} 100000".format(2**44 - 1), id="ceiling"),
    ],
)
def test_create_clamps_the_cpu_quota(tmp_path, cpu, written):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=cpu), "j")
    _close(container)
    assert kernel.file(kernel.rel_of(container.path), "cpu.max") == (
        written + "\n"
    )
    assert container.applied == {"cpu"}


def test_create_without_swap_accounting_still_caps_memory(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False, swap=False)
    container = backend.create(ResourceLimits(memory=64 * MIB), "j")
    _close(container)
    assert container.applied == {"memory"}
    files = [w[1] for w in kernel.writes if w[0] == kernel.rel_of(container.path)]
    assert files == ["memory.max", "memory.oom.group"]


def test_create_records_a_failed_write(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("write", "pids.max")] = errno.EINVAL
    kernel.faults[("write", "memory.swap.max")] = errno.EACCES
    container = backend.create(ALL, "j")
    _close(container)
    assert container.applied == {"cpu"}
    assert container.unenforced == {
        "memory": "could not set memory.swap.max: "
        + os.strerror(errno.EACCES),
        "processes": "could not set pids.max: " + os.strerror(errno.EINVAL),
    }
    rel = kernel.rel_of(container.path)
    # the memory cap that did apply is lifted again: the record says off
    assert kernel.file(rel, "memory.max") == "max\n"
    assert kernel.file(rel, "pids.max") == "max\n"


def test_a_failed_group_kill_lifts_the_memory_cap(tmp_path):
    # without memory.oom.group the record says memory is off, so no hidden
    # cap may stay behind (under a -1000 daemon the job could stall at it)
    kernel, backend = _ready(tmp_path, cpu=False)
    backend._fix_oom_adj = True
    kernel.faults[("write", "memory.oom.group")] = errno.EACCES
    container = backend.create(ResourceLimits(memory=64 * MIB), "j")
    rel = kernel.rel_of(container.path)
    assert kernel.file(rel, "memory.max") == "max\n"
    assert kernel.file(rel, "memory.swap.max") == "max\n"
    # lifted last write first
    assert [(n, d) for r, n, d in kernel.writes if r == rel] == [
        ("memory.max", str(64 * MIB)),
        ("memory.swap.max", "0"),
        ("memory.swap.max", "max"),
        ("memory.max", "max"),
    ]
    assert container.applied == frozenset()
    assert container.unenforced == {
        "memory": "could not set memory.oom.group: "
        + os.strerror(errno.EACCES)
    }
    kwargs = {}
    container.spawn_kwargs(kwargs, required=False)
    assert kwargs["preexec_fn"].fix_oom_adj is False
    _close(container)


@pytest.mark.parametrize(
    "stuck, value, lifted",
    [
        pytest.param("memory.max", str(64 * MIB), "memory.swap.max", id="cap"),
        pytest.param("memory.swap.max", "0", "memory.max", id="swap"),
    ],
)
def test_lifting_a_cap_is_best_effort(tmp_path, stuck, value, lifted):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("write", "memory.oom.group")] = errno.EACCES
    kernel.faults[("write", "/" + stuck)] = [0, errno.EIO]
    container = backend.create(ResourceLimits(memory=64 * MIB), "j")
    _close(container)
    rel = kernel.rel_of(container.path)
    # the other file still lifts when one fails
    assert kernel.file(rel, stuck) == value + "\n"
    assert kernel.file(rel, lifted) == "max\n"
    assert container.unenforced["memory"].startswith(
        "could not set memory.oom.group"
    )


def test_create_before_setup_applies_nothing(tmp_path):
    backend = Backend(_kernel(tmp_path))
    container = backend.create(ResourceLimits(cpu=1, processes=3), "j")
    assert container.path is None
    assert container.unenforced == {
        "cpu": "the cgroup layout is not set up",
        "processes": "the cgroup layout is not set up",
    }


def test_create_reports_a_failed_mkdir(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("mkdir", "-j")] = errno.ENOSPC
    container = backend.create(ResourceLimits(memory=MIB), "j")
    assert container.path is None
    assert container.unenforced == {
        "memory": "could not create a cgroup in {}: {}".format(
            _runs(kernel), os.strerror(errno.ENOSPC)
        )
    }
    kwargs = {}
    container.spawn_kwargs(kwargs, required=False)
    assert kwargs == {}
    assert container.members() is None
    assert container.cpu_times() is None
    assert container.kill() is False
    assert container.finish()["memory_exceeded"] is False
    container.abort()


def test_create_reports_an_unopenable_cgroup_procs(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("open", "cgroup.procs")] = errno.EACCES
    container = backend.create(ResourceLimits(cpu=1, processes=4), "j")
    reason = "could not open cgroup.procs: " + os.strerror(errno.EACCES)
    assert container.unenforced == {"cpu": reason, "processes": reason}
    kwargs = {"preexec_fn": print}
    container.spawn_kwargs(kwargs, required=True)
    assert kwargs == {"preexec_fn": print}
    # attach without a join: every applied cap stays unenforced
    assert container.attach(4242, required=False) is None
    container.finish()


# --- cpu weights -----------------------------------------------------------


@pytest.mark.parametrize(
    "nice, written",
    [
        pytest.param(19, "19", id="idle"),
        pytest.param(-10, "-10", id="high"),
        pytest.param(None, "7", id="normal-is-the-daemons"),
        pytest.param(25, "19", id="clamped-high"),
        pytest.param(-30, "-20", id="clamped-low"),
    ],
)
def test_a_weighted_run_carries_its_nice(tmp_path, nice, written):
    kernel, backend = _ready(tmp_path)
    kernel.nice_value = 7
    container = backend.create(ResourceLimits(memory=MIB), "j", nice=nice)
    _close(container)
    rel = kernel.rel_of(container.path)
    assert kernel.file(rel, "cpu.weight.nice") == written + "\n"
    assert container.weight_problem is None
    assert container.applied == {"memory"}


def test_no_weight_while_cpu_is_off(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    container = backend.create(ResourceLimits(memory=MIB), "j", nice=19)
    _close(container)
    rel = kernel.rel_of(container.path)
    assert [n for r, n, _ in kernel.writes if r == rel] == [
        "memory.max",
        "memory.swap.max",
        "memory.oom.group",
    ]
    assert container.weight_problem is None


def test_a_failed_weight_never_unenforces_a_cap(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("write", "cpu.weight.nice")] = errno.EACCES
    container = backend.create(ResourceLimits(cpu=1, memory=MIB), "j")
    _close(container)
    assert container.applied == {"cpu", "memory"}
    assert container.unenforced == {}
    assert container.weight_problem == (
        "could not set cpu.weight.nice: " + os.strerror(errno.EACCES)
    )


def test_a_weight_only_cgroup(tmp_path):
    # an uncapped run while cpu is on: a cgroup with no caps, only a weight
    kernel, backend = _ready(tmp_path)
    with open(
        os.path.join(kernel.proc, "self", "oom_score_adj"),
        "w",
        encoding="ascii",
    ) as fh:
        fh.write("-1000")
    backend._fix_oom_adj = True
    container = backend.create(ResourceLimits(), "plain", nice=10)
    rel = kernel.rel_of(container.path)
    assert [(n, d) for r, n, d in kernel.writes if r == rel] == [
        ("cpu.weight.nice", "10")
    ]
    assert container.applied == frozenset()
    assert container.unenforced == {} and container.weight_problem is None
    kwargs = {}
    container.spawn_kwargs(kwargs, required=False)
    join = kwargs["preexec_fn"]
    # never fails the spawn, and keeps the -1000 an uncapped run keeps
    assert join.required is False and join.fix_oom_adj is False
    kernel.spawn(500, kernel.own + "/" + DAEMON_LEAF)
    kernel.join(500, container.path)
    assert container.attach(500, required=False) is None
    assert container.weight_problem is None
    assert container.members() == [500]
    assert container.kill() is True
    container.finish()
    assert not os.path.exists(container.path)


def test_a_weight_only_cgroup_needs_cpu_on(tmp_path):
    kernel, backend = _ready(tmp_path, cpu=False)
    container = backend.create(ResourceLimits(), "plain")
    assert container.weight_problem == "the cpu controller is off"
    container.abort()


def test_a_weight_only_cgroup_that_could_not_be_made(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("mkdir", "-plain")] = errno.ENOSPC
    container = backend.create(ResourceLimits(), "plain")
    assert container.path is None
    assert container.unenforced == {}
    assert container.weight_problem == (
        "could not create a cgroup in {}: {}".format(
            _runs(kernel), os.strerror(errno.ENOSPC)
        )
    )


@pytest.mark.parametrize("how", ["outside", "marker", "unopened"])
def test_a_weight_only_run_outside_its_cgroup(tmp_path, how):
    kernel, backend = _ready(tmp_path)
    if how == "unopened":
        kernel.faults[("open", "cgroup.procs")] = errno.EACCES
    container = backend.create(ResourceLimits(), "plain")
    kernel.spawn(500, kernel.own + "/" + DAEMON_LEAF)
    if how == "marker":
        os.write(container._join[2], b"!")
    assert container.attach(500, required=False) is None
    assert container.unenforced == {}
    if how == "unopened":
        assert container.weight_problem == (
            "could not open cgroup.procs: " + os.strerror(errno.EACCES)
        )
    else:
        assert container.weight_problem == (
            "could not move the job into its cgroup"
        )
    assert container.members() is None
    assert kernel.killed == []
    container.finish()


def test_a_weight_only_abort_after_a_failed_join(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(), "plain")
    os.write(container._join[2], b"!")
    container.abort()
    assert container.weight_problem == "could not move the job into its cgroup"


def test_join_fds_stay_clear_of_the_childs_stdio(tmp_path):
    fd = _fd(tmp_path / "f")
    assert _cgroups._above_stdio(fd) == fd  # already above 0-2
    moved = _cgroups._above_stdio(fd, floor=fd + 2)
    try:
        assert moved >= fd + 2
        with pytest.raises(OSError):
            os.fstat(fd)  # the low original is closed
        os.write(moved, b"ok")
    finally:
        os.close(moved)
    assert _text(tmp_path / "f") == "ok"


def test_a_failed_move_above_stdio_closes_everything(tmp_path, monkeypatch):
    fd = _fd(tmp_path / "f")

    def broken(fd):
        raise OSError(errno.EMFILE, os.strerror(errno.EMFILE))

    monkeypatch.setattr(_cgroups.os, "dup", broken)
    with pytest.raises(OSError):
        _cgroups._above_stdio(fd, floor=fd + 1)
    monkeypatch.undo()
    with pytest.raises(OSError):
        os.fstat(fd)


def test_a_failed_pipe_leaves_the_caps_unjoined(tmp_path, monkeypatch):
    kernel, backend = _ready(tmp_path)

    def broken():
        raise OSError(errno.EMFILE, os.strerror(errno.EMFILE))

    monkeypatch.setattr(_cgroups.os, "pipe", broken)
    container = backend.create(ResourceLimits(cpu=1), "j")
    monkeypatch.undo()
    assert container._join is None
    assert container.unenforced == {
        "cpu": "could not open cgroup.procs: " + os.strerror(errno.EMFILE)
    }
    container.finish()


def test_slug_is_safe_and_bounded():
    assert _cgroups._slug("a b/c:d.e_f-g") == "a_b_c_d.e_f-g"
    assert _cgroups._slug("x" * 80) == "x" * 48
    assert _cgroups._slug("caf\N{LATIN SMALL LETTER E WITH ACUTE}") == "caf_"


# --- spawn kwargs and the child-side join -----------------------------------


def _demote():
    return None


def test_spawn_kwargs_wraps_the_demote_hook(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    kwargs = {"preexec_fn": _demote, "start_new_session": True}
    container.spawn_kwargs(kwargs, required=True)
    join = kwargs["preexec_fn"]
    assert isinstance(join, _ChildJoin)
    assert join.then is _demote and join.required is True
    assert join.fd == container._join[0] and join.marker == container._join[2]
    assert join.fix_oom_adj is False
    assert kwargs["start_new_session"] is True
    _close(container)


def test_spawn_kwargs_without_a_demote_hook(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    kwargs = {}
    container.spawn_kwargs(kwargs, required=False)
    assert kwargs["preexec_fn"].then is None
    assert kwargs["preexec_fn"].required is False
    _close(container)


@pytest.mark.parametrize(
    "adj, caps, fix",
    [
        pytest.param("-1000", ResourceLimits(memory=MIB), True, id="fixed"),
        pytest.param("-1000", ResourceLimits(cpu=1), False, id="no-memory"),
        pytest.param("-500", ResourceLimits(memory=MIB), False, id="killable"),
    ],
)
def test_the_oom_score_fix_is_for_memory_caps_under_minus_1000(
    tmp_path, adj, caps, fix
):
    kernel = _kernel(tmp_path)
    with open(
        os.path.join(kernel.proc, "self", "oom_score_adj"),
        "w",
        encoding="ascii",
    ) as fh:
        fh.write(adj)
    backend = Backend(kernel)
    backend.setup()
    container = backend.create(caps, "j")
    kwargs = {}
    container.spawn_kwargs(kwargs, required=False)
    assert kwargs["preexec_fn"].fix_oom_adj is fix
    _close(container)


def _fd(path):
    return os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)


def _text(path):
    with open(path, encoding="ascii") as fh:
        return fh.read()


def test_child_join_writes_then_fixes_then_demotes(tmp_path):
    procs, marker, oom = (tmp_path / n for n in ("procs", "marker", "oom"))
    oom.write_text("")  # procfs replaces the value; a plain file appends
    seen = []

    def then():
        seen.append((_text(procs), _text(oom)))

    fd, mark = _fd(procs), _fd(marker)
    try:
        _ChildJoin(fd, mark, True, True, then, oom=str(oom))()
    finally:
        os.close(fd)
        os.close(mark)
    # the join and the oom fix both happened before the demote hook ran
    assert seen == [("0", "0")]
    assert _text(marker) == ""


def test_child_join_without_oom_fix_or_hook(tmp_path):
    fd, mark = _fd(tmp_path / "procs"), _fd(tmp_path / "marker")
    try:
        _ChildJoin(fd, mark, False, False, None)()
    finally:
        os.close(fd)
        os.close(mark)
    assert _text(tmp_path / "procs") == "0"


# never a valid descriptor: every write is EBADF
_BAD_FD = 987654


def test_failed_child_join_marks_and_carries_on(tmp_path):
    mark = _fd(tmp_path / "marker")
    ran = []
    try:
        _ChildJoin(_BAD_FD, mark, False, False, lambda: ran.append(1))()
    finally:
        os.close(mark)
    assert _text(tmp_path / "marker") == "!"
    assert ran == [1]


def test_failed_child_join_raises_when_required(tmp_path):
    mark = _fd(tmp_path / "marker")
    ran = []
    try:
        with pytest.raises(OSError):
            _ChildJoin(_BAD_FD, mark, False, True, lambda: ran.append(1))()
    finally:
        os.close(mark)
    assert ran == []
    assert _text(tmp_path / "marker") == "!"


def test_a_failed_marker_is_ignored():
    _ChildJoin(_BAD_FD, _BAD_FD, True, False, None)()


def test_a_failed_oom_fix_is_ignored(tmp_path):
    missing = str(tmp_path / "no" / "such" / "oom_score_adj")
    fd = _fd(tmp_path / "procs")
    try:
        _ChildJoin(fd, _BAD_FD, True, False, None, oom=missing)()
    finally:
        os.close(fd)
    assert _text(tmp_path / "procs") == "0"


def test_a_failed_join_keeps_the_inherited_oom_score_adj(tmp_path):
    # outside every memory cap, the job stays as OOM-proof as an uncapped
    # run of a -1000 daemon
    oom = tmp_path / "oom_score_adj"
    oom.write_text("-1000")
    mark = _fd(tmp_path / "marker")
    try:
        _ChildJoin(_BAD_FD, mark, True, False, None, oom=str(oom))()
    finally:
        os.close(mark)
    assert _text(tmp_path / "marker") == "!"
    assert _text(oom) == "-1000"


# --- attach ----------------------------------------------------------------


def test_attach_confirms_the_child_joined(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "j")
    kernel.spawn(500, kernel.own + "/daemon")
    kernel.join(500, container.path)
    assert container.attach(500, required=True) is None
    assert container.unenforced == {}
    assert kernel.killed == []
    # settled once: a second attach reads nothing again
    assert container.attach(500, required=False) is None


@pytest.mark.parametrize("required", [True, False])
def test_attach_counts_a_child_cgroup_of_the_run(tmp_path, required):
    # A job that moves itself into a cgroup below its run's (as nested
    # runtimes do) before attach looks is still inside: the caps bind it.
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(memory=64 * MIB), "j")
    kernel.spawn(500, kernel.own + "/" + DAEMON_LEAF)
    kernel.join(500, container.path)
    sub = os.path.join(container.path, "sub")
    kernel.mkdir(sub)
    kernel.join(500, sub)
    assert container.attach(500, required=required) is None
    assert container.unenforced == {}
    assert kernel.killed == []
    assert container.members() == [500]
    assert container.kill() is True
    assert kernel.killed == [(500, "cgroup.kill")]


def test_attach_tells_a_sibling_with_a_longer_name_apart(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(memory=MIB), "j")
    sibling = container.path + "x"
    kernel.mkdir(sibling)
    kernel.spawn(500, kernel.rel_of(sibling))
    assert container.attach(500, required=False) is None
    assert container.unenforced == {
        "memory": "could not move the job into its cgroup"
    }


def test_attach_records_a_child_outside_its_cgroup(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1, memory=MIB), "j")
    kernel.spawn(500, kernel.own + "/daemon")
    assert container.attach(500, required=False) is None
    reason = "could not move the job into its cgroup"
    assert container.unenforced == {"cpu": reason, "memory": reason}
    assert kernel.killed == []


def test_attach_kills_an_uncontained_child_when_required(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    kernel.spawn(500, kernel.own + "/daemon")
    reason = container.attach(500, required=True)
    assert reason == "could not move the job into its cgroup"
    assert kernel.killed == [(500, SIGKILL)]


def test_attach_kill_of_a_vanished_child_is_quiet(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    os.write(container._join[2], b"!")  # the child reported a failed join
    assert container.attach(777, required=True) is not None
    assert kernel.killed == [(777, SIGKILL)]


def test_attach_trusts_the_marker_for_an_exited_child(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    # no /proc entry: the child already exited, and wrote no marker
    assert container.attach(9999, required=False) is None
    assert container.unenforced == {}


def test_attach_believes_a_failed_join_marker(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    os.write(container._join[2], b"!")
    assert container.attach(9999, required=False) is None
    assert container.unenforced == {
        "cpu": "could not move the job into its cgroup"
    }


def test_attach_rejects_a_proc_entry_without_a_v2_line(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    os.makedirs(os.path.join(kernel.proc, "600"))
    with open(
        os.path.join(kernel.proc, "600", "cgroup"), "w", encoding="ascii"
    ) as fh:
        fh.write("1:name=systemd:/x\n")
    assert container.attach(600, required=False) is None
    assert "cpu" in container.unenforced


@pytest.mark.parametrize(
    "kill_file", [True, False], ids=["cgroup-kill", "before-5.14"]
)
@pytest.mark.parametrize("required", [True, False])
def test_an_attach_error_kills_a_required_child_first(
    tmp_path, monkeypatch, required, kill_file
):
    kernel, backend = _ready(tmp_path, kill_file=kill_file)
    container = backend.create(ResourceLimits(cpu=1), "j")
    kernel.spawn(500, kernel.own + "/daemon")
    kernel.join(500, container.path)
    # a setsid escapee: in the run's cgroup, out of the job's process group
    kernel.spawn(501, kernel.rel_of(container.path))

    def broken():
        raise OSError(errno.EBADF, os.strerror(errno.EBADF))

    monkeypatch.setattr(container, "_settle_join", broken)
    with pytest.raises(OSError):
        container.attach(500, required=required)
    assert kernel.killed == ([(500, SIGKILL)] if required else [])
    monkeypatch.undo()
    _close(container)
    # membership is unknown: the monitor and the stats skip the cgroup, and
    # a forced kill empties it but leaves the group signal to the caller
    assert container.members() is None
    assert container.cpu_times() is None
    assert container.kill() is False
    rel = kernel.rel_of(container.path)
    assert ((rel, "cgroup.kill", "1") in kernel.writes) is kill_file
    # before 5.14 each pid the cgroup lists gets SIGKILL instead
    how = "cgroup.kill" if kill_file else SIGKILL
    first = [(500, SIGKILL)] if required else [(500, how)]
    assert kernel.killed == first + [(501, how)]
    assert kernel.cgroup_of(500) is None and kernel.cgroup_of(501) is None
    assert container.finish() == _cgroups._no_stats()


def test_an_attach_error_ignores_a_child_that_already_exited(
    tmp_path, monkeypatch
):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")

    def broken():
        raise RuntimeError("boom")

    monkeypatch.setattr(container, "_settle_join", broken)
    with pytest.raises(RuntimeError):
        container.attach(777, required=True)
    assert kernel.killed == [(777, SIGKILL)]  # ESRCH, swallowed
    monkeypatch.undo()
    _close(container)


# --- kill ------------------------------------------------------------------


def _running(kernel, backend, caps=ALL, pids=(500,)):
    container = backend.create(caps, "j")
    for pid in pids:
        kernel.spawn(pid, kernel.own + "/daemon")
        kernel.join(pid, container.path)
    assert container.attach(pids[0], required=False) is None
    return container


def test_kill_writes_cgroup_kill(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, pids=(500, 501))
    assert container.kill() is True
    assert sorted(kernel.killed) == [(500, "cgroup.kill"), (501, "cgroup.kill")]
    assert (kernel.rel_of(container.path), "cgroup.kill", "1") in kernel.writes


def test_kill_without_cgroup_kill_signals_every_member(tmp_path):
    kernel, backend = _ready(tmp_path, kill_file=False)
    container = _running(kernel, backend, pids=(500, 501))
    sub = os.path.join(container.path, "sub")
    kernel.mkdir(sub)
    kernel.spawn(502, kernel.rel_of(sub))
    assert container.kill() is False  # the caller still signals the group
    assert sorted(kernel.killed) == [
        (500, SIGKILL),
        (501, SIGKILL),
        (502, SIGKILL),
    ]


def test_kill_fallback_skips_members_that_already_exited(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.faults[("write", "cgroup.kill")] = errno.EACCES
    # listed in cgroup.procs but gone by the time it is signalled
    kernel.set_file(kernel.rel_of(container.path), "cgroup.procs", "500\n4\n")
    assert container.kill() is False
    assert (4, SIGKILL) in kernel.killed


@pytest.mark.parametrize("procs", ["x\n", None], ids=["malformed", "missing"])
def test_kill_fallback_of_an_unlistable_cgroup_signals_nothing(
    tmp_path, procs
):
    kernel, backend = _ready(tmp_path, kill_file=False)
    container = _running(kernel, backend)
    rel = kernel.rel_of(container.path)
    if procs is None:
        os.unlink(kernel.path(rel, "cgroup.procs"))
    else:
        kernel.set_file(rel, "cgroup.procs", procs)
    assert container.kill() is False
    assert kernel.killed == []


def test_kill_of_a_removed_cgroup_succeeds(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.exit(500)
    container.finish()
    assert not os.path.exists(container.path)
    assert container.kill() is True


def test_kill_before_attach_leaves_the_group_signal_to_the_caller(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "j")
    kernel.spawn(500, kernel.own + "/daemon")
    kernel.join(500, container.path)
    assert container.kill() is False
    assert kernel.killed == [(500, "cgroup.kill")]
    _close(container)


def test_kill_of_a_job_outside_its_cgroup_is_left_to_the_caller(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "j")
    kernel.spawn(500, kernel.own + "/daemon")
    container.attach(500, required=False)  # never joined
    before = len(kernel.writes)
    assert container.kill() is False
    assert kernel.writes[before:] == [] and kernel.killed == []


def test_kill_through_a_dead_handle_succeeds(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.faults[("write", "cgroup.kill")] = errno.ENODEV
    assert container.kill() is True


# --- finish ----------------------------------------------------------------


def test_finish_reads_the_counters_and_removes_the_cgroup(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    rel = kernel.rel_of(container.path)
    kernel.set_file(
        rel,
        "memory.events",
        "low 0\nhigh 0\nmax 4\noom 1\noom_kill 1\noom_group_kill 0\n",
    )
    kernel.set_file(rel, "memory.peak", "536870912\n")
    kernel.set_file(
        rel,
        "cpu.stat",
        "usage_usec 9\nuser_usec 5\nsystem_usec 4\nnice_usec 0\n"
        "nr_periods 3\nnr_throttled 2\nthrottled_usec 2500000\n",
    )
    kernel.set_file(rel, "pids.events", "max 7\n")
    kernel.exit(500)
    stats = container.finish()
    assert stats == {
        "memory_exceeded": True,
        "process_limit_hits": 7,
        "peak_memory_bytes": 512 * MIB,
        "cpu_throttled_seconds": 2.5,
    }
    assert not os.path.exists(container.path)
    assert kernel.rmdirs == [container.path]
    # idempotent: the same answer, no second removal
    assert container.finish() == stats
    assert kernel.rmdirs == [container.path]


@pytest.mark.parametrize(
    "events, exceeded",
    [
        pytest.param("oom 1\noom_group_kill 1\n", True, id="group"),
        pytest.param("oom 2\noom_kill 1\n", True, id="kill"),
        # a host-wide OOM kill: the run's own limit was never reached
        pytest.param(
            "oom 0\noom_kill 1\noom_group_kill 1\n", False, id="host"
        ),
        pytest.param("oom_kill 1\n", False, id="no-oom-line"),
        # the limit was reached, but no process was killed
        pytest.param("oom 3\noom_kill 0\n", False, id="spared"),
    ],
)
def test_memory_exceeded_needs_the_runs_own_oom(tmp_path, events, exceeded):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.set_file(kernel.rel_of(container.path), "memory.events", events)
    kernel.exit(500)
    assert container.finish()["memory_exceeded"] is exceeded


def test_finish_reads_only_the_counters_of_applied_caps(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, caps=ResourceLimits(processes=5))
    rel = kernel.rel_of(container.path)
    kernel.set_file(rel, "memory.events", "oom_kill 3\n")
    kernel.set_file(rel, "pids.events", "max 2\n")
    kernel.exit(500)
    stats = container.finish()
    assert stats["memory_exceeded"] is False  # no memory cap was set
    assert stats["cpu_throttled_seconds"] is None
    assert stats["process_limit_hits"] == 2
    assert stats["peak_memory_bytes"] == 0


@pytest.mark.parametrize(
    "peak, scoped, hits",
    [
        # before 6.11 the run's count takes in a limit above it (TasksMax)
        pytest.param("40", False, 0, id="peak-below-the-cap"),
        pytest.param("64", False, 3, id="peak-at-the-cap"),
        pytest.param(None, False, 3, id="no-pids-peak"),
        # from 6.11 the count holds only limits at or below the run
        pytest.param("40", True, 3, id="scoped-events"),
    ],
)
def test_process_limit_hits_need_the_runs_own_cap(
    tmp_path, peak, scoped, hits
):
    kernel, backend = _ready(tmp_path)
    backend._scoped_pids_events = scoped
    container = _running(kernel, backend, caps=ResourceLimits(processes=64))
    rel = kernel.rel_of(container.path)
    kernel.set_file(rel, "pids.events", "max 3\n")
    if peak is not None:
        kernel.set_file(rel, "pids.peak", peak + "\n")
    kernel.exit(500)
    assert container.finish()["process_limit_hits"] == hits


@pytest.mark.parametrize(
    "local, options, scoped",
    [
        pytest.param(False, "rw,nsdelegate", False, id="before-6.11"),
        pytest.param(True, "rw,nsdelegate,favordynmods", True, id="6.11"),
        pytest.param(True, "rw,pids_localevents", False, id="localevents"),
        pytest.param(True, None, True, id="no-mountinfo"),
    ],
)
def test_setup_reads_whether_pids_events_are_scoped(
    tmp_path, local, options, scoped
):
    kernel = _kernel(tmp_path)
    runs = _runs(kernel)
    kernel.mkdir(runs)
    if local:
        kernel.set_file(kernel.rel_of(runs), "pids.events.local", "max 0\n")
    if options is not None:
        with open(
            os.path.join(kernel.proc, "self", "mountinfo"),
            "w",
            encoding="ascii",
            newline="\n",
        ) as fh:
            fh.write(
                "25 1 0:22 / /proc rw,nosuid - proc proc rw\n"
                "30 25 0:26 / /sys/fs/cgroup rw,nosuid shared:4 - cgroup2 "
                "cgroup2 {}\n".format(options)
            )
    backend = Backend(kernel)
    assert backend.setup().available
    assert backend._scoped_pids_events is scoped


def test_finish_of_a_cpu_cap_reads_no_process_counters(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, caps=ResourceLimits(cpu=1))
    kernel.set_file(kernel.rel_of(container.path), "pids.events", "max 2\n")
    kernel.exit(500)
    stats = container.finish()
    assert stats["process_limit_hits"] is None
    assert stats["cpu_throttled_seconds"] == 0.0
    assert stats["peak_memory_bytes"] == 0  # contained: the peak is read


def test_finish_ignores_the_counters_of_a_job_outside(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(memory=MIB, processes=4), "j")
    kernel.spawn(500, kernel.own + "/daemon")
    container.attach(500, required=False)  # never joined
    rel = kernel.rel_of(container.path)
    kernel.set_file(rel, "memory.events", "oom 1\noom_kill 3\n")
    kernel.set_file(rel, "memory.peak", "4096\n")
    kernel.set_file(rel, "pids.events", "max 2\n")
    # the cgroup's figures are not the job's
    assert container.finish() == _cgroups._no_stats()


def test_finish_without_a_join_reads_no_counters(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("open", "cgroup.procs")] = errno.EMFILE
    container = backend.create(ResourceLimits(cpu=1), "j")
    assert container.finish()["peak_memory_bytes"] is None


def test_finish_tolerates_missing_and_malformed_files(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    rel = kernel.rel_of(container.path)
    for name in ("memory.events", "pids.events"):
        os.unlink(kernel.path(rel, name))
    kernel.set_file(rel, "memory.peak", "lots\n")
    kernel.set_file(rel, "cpu.stat", "usage_usec x\nbroken line here\n")
    kernel.exit(500)
    assert container.finish() == {
        "memory_exceeded": False,
        "process_limit_hits": None,
        "peak_memory_bytes": None,
        "cpu_throttled_seconds": None,
    }


def test_finish_leaves_a_transient_ebusy_to_collect(tmp_path):
    # 6.19 can report EBUSY just after the last process was reaped; finish
    # tries rmdir once and leaves a busy cgroup to collect
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.exit(500)
    kernel.faults[("rmdir", os.path.basename(container.path))] = [errno.EBUSY]
    container.finish()
    assert os.path.isdir(container.path)
    assert kernel.rmdirs == [container.path]
    assert backend.pending == {container.path}
    backend.collect()
    assert not os.path.exists(container.path)
    assert backend.pending == frozenset()


async def test_finishing_a_run_collects_right_after_its_removal(tmp_path):
    # RunLimits.finish calls collect after the container's finish, which
    # gives a transient EBUSY a second try at once
    kernel, backend = _ready(tmp_path)
    run = await LimitsManager(backend).prepare(ResourceLimits(cpu=1), "j")
    path = run.container.path
    kernel.spawn(500, kernel.own + "/daemon")
    kernel.join(500, path)
    await run.attach(500)
    kernel.exit(500)
    kernel.faults[("rmdir", os.path.basename(path))] = [errno.EBUSY]
    assert (await run.finish()).enforced
    assert kernel.rmdirs == [path, path]
    assert not os.path.exists(path)
    assert backend.pending == frozenset()


def test_finish_with_a_leftover_queues_the_cgroup(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, pids=(500, 501))
    kernel.exit(500)  # the main process exits; 501 lives on, still capped
    container.finish()
    assert os.path.isdir(container.path)
    assert kernel.rmdirs == [container.path]  # one try, then the queue
    assert backend.pending == {container.path}
    backend.collect()  # still busy
    assert backend.pending == {container.path}
    kernel.exit(501)
    backend.collect()
    assert backend.pending == frozenset()
    assert not os.path.exists(container.path)


def test_finish_removes_child_cgroups_the_job_made(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    sub = os.path.join(container.path, "sub", "deeper")
    kernel.mkdir(os.path.dirname(sub))
    kernel.mkdir(sub)
    kernel.exit(500)
    container.finish()
    assert not os.path.exists(container.path)


def test_a_busy_child_cgroup_keeps_the_run_pending(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    sub = os.path.join(container.path, "sub")
    kernel.mkdir(sub)
    kernel.spawn(502, kernel.rel_of(sub))
    kernel.exit(500)
    container.finish()
    assert backend.pending == {container.path}


def test_an_unexpected_rmdir_errno_queues_without_listing(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.exit(500)
    kernel.faults[("rmdir", os.path.basename(container.path))] = errno.EACCES
    container.finish()
    assert backend.pending == {container.path}


def test_an_unlistable_busy_cgroup_stays_pending(tmp_path, monkeypatch):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)

    def broken(path):
        raise OSError(errno.EIO, "io")

    monkeypatch.setattr(kernel, "subdirs", broken)
    container.finish()
    assert backend.pending == {container.path}


def test_abort_queues_the_cgroup_for_collect(tmp_path):
    # abort can run on the event loop, where rmdir must not run
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "j")
    container.abort()
    assert container.unenforced == {}
    assert kernel.rmdirs == []
    assert container._join is None
    assert backend.pending == {container.path}
    container.abort()  # idempotent
    assert container.finish() == _cgroups._no_stats()
    backend.collect()
    assert not os.path.exists(container.path)
    assert backend.pending == frozenset()


def test_abort_after_a_failed_join_unenforces_the_caps(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1, processes=2), "j")
    os.write(container._join[2], b"!")  # the child's join failed
    container.abort()
    reason = "could not move the job into its cgroup"
    assert container.unenforced == {"cpu": reason, "processes": reason}


def test_abort_of_a_busy_cgroup_queues_it(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ALL, "j")
    kernel.spawn(500, kernel.own + "/daemon")
    kernel.join(500, container.path)
    container.abort()
    assert backend.pending == {container.path}
    assert kernel.rmdirs == []
    backend.collect()
    assert backend.pending == {container.path}


def test_collect_without_pending_touches_nothing(tmp_path):
    kernel, backend = _ready(tmp_path)
    before = len(kernel.writes)
    backend.collect()
    assert len(kernel.writes) == before


def test_a_pending_cgroup_removed_elsewhere_is_dropped(tmp_path):
    kernel, backend = _ready(tmp_path)
    backend._pending.add(os.path.join(_runs(kernel), "gone"))
    backend.collect()
    assert backend.pending == frozenset()


def test_abort_without_a_cgroup(tmp_path):
    kernel, backend = _ready(tmp_path)
    kernel.faults[("mkdir", "-j")] = errno.EEXIST
    container = backend.create(ResourceLimits(cpu=1), "j")
    container.abort()
    assert container.finish() == _cgroups._no_stats()
    assert backend.pending == frozenset()


# --- monitor view ----------------------------------------------------------


def test_members_and_cpu_times(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend, pids=(500, 501))
    sub = os.path.join(container.path, "sub")
    kernel.mkdir(sub)
    kernel.spawn(502, kernel.rel_of(sub))
    assert sorted(container.members()) == [500, 501, 502]
    kernel.set_file(
        kernel.rel_of(container.path),
        "cpu.stat",
        "usage_usec 3500000\nuser_usec 2500000\nsystem_usec 1000000\n",
    )
    assert container.cpu_times() == (2.5, 1.0)
    kernel.set_file(kernel.rel_of(container.path), "cpu.stat", "usage_usec 1\n")
    assert container.cpu_times() is None
    kernel.set_file(kernel.rel_of(container.path), "cgroup.procs", "x\n")
    assert container.members() is None


def test_members_of_a_removed_cgroup_is_none(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = _running(kernel, backend)
    kernel.exit(500)
    container.finish()
    assert container.members() is None
    assert container.cpu_times() is None


def _uncontained(tmp_path, how, pid):
    """A container whose child (``pid``) is known to be outside it."""
    kernel, backend = _ready(tmp_path)
    if how == "unopened":
        kernel.faults[("open", "cgroup.procs")] = errno.EACCES
    container = backend.create(ResourceLimits(cpu=1, memory=MIB), "j")
    kernel.spawn(pid, kernel.own + "/daemon")
    if how == "marker":
        os.write(container._join[2], b"!")
    assert container.attach(pid, required=False) is None
    return container


@pytest.mark.parametrize("how", ["outside", "marker", "unopened"])
def test_a_job_outside_its_cgroup_has_no_members(tmp_path, how):
    container = _uncontained(tmp_path, how, 500)
    # the cgroup itself is readable, but it is not the job's
    assert container.members() is None
    assert container.cpu_times() is None
    container.finish()


def test_members_before_attach_read_the_cgroup(tmp_path):
    kernel, backend = _ready(tmp_path)
    container = backend.create(ResourceLimits(cpu=1), "j")
    assert container.members() == []
    assert container.cpu_times() == (0.0, 0.0)
    _close(container)


@pytest.mark.parametrize("how", ["outside", "unopened"])
async def test_the_monitor_walks_the_tree_of_a_job_outside(tmp_path, how):
    # a real ResourceMonitor over this test process, which the model puts
    # outside the run's cgroup: it samples the process tree instead
    pid = os.getpid()
    container = _uncontained(tmp_path, how, pid)
    monitor = ResourceMonitor(
        pid, interval=30.0, history=0, container=container
    )
    monitor.start()
    usage = await monitor.stop()
    assert usage is not None and usage.samples >= 1
    assert usage.max_rss_bytes > 0
    assert usage.cpu_total_seconds > 0
    container.finish()


# --- through the manager ---------------------------------------------------


async def test_a_cancelled_prepare_leaves_no_cgroup_behind(tmp_path):
    kernel, backend = _ready(tmp_path)
    manager = LimitsManager(backend)
    gate = threading.Event()
    made = []
    create = backend.create

    def gated(caps, name, **kwargs):
        made.append(create(caps, name, **kwargs))
        assert gate.wait(10)
        return made[0]

    backend.create = gated
    task = asyncio.ensure_future(manager.prepare(ResourceLimits(cpu=1), "j"))
    while not made:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    gate.set()
    container = made[0]
    while container._join is not None:
        await asyncio.sleep(0.01)
    # the join fds are closed and the cgroup waits for collect
    assert backend.pending == {container.path}
    backend.collect()
    assert not os.path.exists(container.path)


async def test_converge_on_a_thread_alongside_runs(tmp_path, caplog):
    # Cron converges on a worker thread while runs prepare and finish on
    # others: setup and the cpu enable happen once, before any run's
    # cgroup, and the leftovers are swept off the loop.
    kernel = _kernel(tmp_path)
    runs = _runs(kernel)
    kernel.mkdir(runs)
    for name in ("old-1", "old-2"):
        kernel.mkdir(os.path.join(runs, name))
    backend = Backend(kernel)
    manager = LimitsManager(backend)
    capped = [("job j", ResourceLimits(cpu=1))]
    caps = ResourceLimits(cpu=1, processes=8)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        done = await asyncio.gather(
            asyncio.to_thread(manager.converge, capped),
            *(manager.prepare(caps, "j{}".format(i)) for i in range(8)),
        )
        prepared = done[1:]
        assert all(r.record.enforced for r in prepared)
        assert len({r.container.path for r in prepared}) == 8
        await asyncio.gather(
            asyncio.to_thread(manager.converge, capped),
            *(r.finish() for r in prepared),
        )
    enables = [d for r, d in _subtree_writes(kernel) if r == kernel.own]
    assert enables == ["+memory +pids", "+cpu"]
    assert [
        r.getMessage() for r in caplog.records if r.levelno >= logging.INFO
    ] == ["resource limits are enforced with cgroup v2"]
    assert kernel.subdirs(runs) == []
    assert backend.pending == frozenset()


def test_converge_turns_cpu_on_and_off_with_the_cpu_caps(tmp_path):
    kernel = _kernel(tmp_path)
    backend = Backend(kernel)
    manager = LimitsManager(backend)
    runs = kernel.own + "/" + RUNS_DIR
    manager.converge([("job m", ResourceLimits(memory=MIB))])
    assert kernel.subtree(runs) == {"memory", "pids"}
    manager.converge([("job c", ResourceLimits(cpu=1))])
    assert kernel.subtree(runs) == {"cpu", "memory", "pids"}
    assert manager.weighting
    manager.converge([("job m", ResourceLimits(memory=MIB))])
    assert "cpu" not in kernel.subtree(kernel.own)
    manager.converge([("job c", ResourceLimits(cpu=1))])
    manager.converge([])  # the last capped job is gone
    assert "cpu" not in kernel.subtree(kernel.own)
    assert not manager.weighting


async def test_runs_under_the_previous_config_keep_their_cpu_caps(tmp_path):
    kernel = _kernel(tmp_path)
    manager = LimitsManager(Backend(kernel))
    caps = ResourceLimits(cpu=0.5)
    manager.converge([("job c", caps)])
    run = await manager.prepare(caps, "c")
    # a reload drops the cpu cap while the run is going, and its verify
    # check then starts under the previous config
    manager.converge([("job c", ResourceLimits(memory=MIB))])
    check = await manager.prepare(caps, "c")
    assert manager.weighting
    for each in (run, check):
        assert _quota(kernel, each.container) == "50000 100000\n"
    await run.finish()
    assert manager.weighting
    record = await check.finish()
    assert record.enforced
    assert not manager.weighting
    assert "cpu" not in kernel.subtree(kernel.own)
    # a later one turns cpu on for itself alone, too
    late = await manager.prepare(caps, "c")
    assert manager.weighting and late.record.enforced
    await late.finish()
    assert not manager.weighting


def test_converge_says_how_to_delegate_cpu_to_a_service(tmp_path, caplog):
    kernel = _kernel(
        tmp_path,
        own="system.slice/cronstable.service",
        controllers=("memory", "pids"),
    )
    manager = LimitsManager(Backend(kernel))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        manager.converge(
            [
                ("job c", ResourceLimits(cpu=1)),
                ("job m", ResourceLimits(memory=MIB)),
            ]
        )
    assert [r.getMessage() for r in caplog.records] == [
        "resourceLimits is set (job c), but cronstable cannot enforce the "
        "cpu cap: the cpu controller is not delegated to cronstable. To cap "
        "CPU, run systemctl edit cronstable and add Delegate=yes under "
        "[Service]; that makes cronstable a CPU group of its own among the "
        "system's services. Those jobs run without that cap."
    ]


async def test_an_uncapped_run_through_the_manager(tmp_path):
    kernel = _kernel(tmp_path)
    kernel.nice_value = 3
    manager = LimitsManager(Backend(kernel))
    assert await manager.prepare(None, "plain") is None  # cpu is off
    manager.converge([("job c", ResourceLimits(cpu=1))])
    run = await manager.prepare(None, "plain", nice=19)
    assert run is not None and not run.recorded
    path = run.container.path
    assert kernel.file(kernel.rel_of(path), "cpu.weight.nice") == "19\n"
    kernel.spawn(500, kernel.own + "/" + DAEMON_LEAF)
    kernel.join(500, path)
    await run.attach(500)
    kernel.exit(500)
    await run.finish()
    assert not os.path.exists(path)
    # cpu off again: uncapped runs go back to the daemon's leaf
    manager.converge([("job m", ResourceLimits(memory=MIB))])
    assert await manager.prepare(None, "plain") is None


# --- the real file layer ---------------------------------------------------


def test_real_fs_reads_and_writes_without_creating(tmp_path):
    fs = CgroupFS(str(tmp_path), str(tmp_path / "proc"))
    assert fs.root == os.path.normpath(str(tmp_path))
    target = tmp_path / "cpu.max"
    target.write_text("max 100000\n")
    fs.write(str(target), "1000 100000")
    assert fs.read(str(target)).startswith("1000 100000")
    with pytest.raises(FileNotFoundError):
        fs.write(str(tmp_path / "missing"), "1")
    assert not (tmp_path / "missing").exists()
    assert fs.exists(str(target)) and not fs.isdir(str(target))
    assert fs.writable(str(target))
    fs.mkdir(str(tmp_path / "child"))
    assert fs.subdirs(str(tmp_path)) == ["child"]
    fs.rmdir(str(tmp_path / "child"))
    fd = fs.open_procs(str(target))
    os.close(fd)
    assert fs.page_size() > 0
    assert isinstance(fs.geteuid(), int)


def test_real_fs_kill_of_an_unused_pid_raises():
    with pytest.raises(OSError):
        CgroupFS().kill(2**22 + 12345, 0)


def test_real_fs_page_size_falls_back(monkeypatch):
    def broken(name):
        raise ValueError(name)

    monkeypatch.setattr(os, "sysconf", broken, raising=False)
    assert CgroupFS().page_size() == 4096


def test_real_fs_getxattr(tmp_path, monkeypatch):
    fs = CgroupFS()
    monkeypatch.delattr(os, "getxattr", raising=False)
    with pytest.raises(OSError) as raised:
        fs.getxattr(str(tmp_path), "user.delegate")
    assert raised.value.errno == errno.ENOTSUP

    def fake(path, name):
        return b"1"

    monkeypatch.setattr(os, "getxattr", fake, raising=False)
    assert fs.getxattr(str(tmp_path), "user.delegate") == b"1"


def test_real_fs_geteuid_without_the_call(monkeypatch):
    monkeypatch.delattr(os, "geteuid", raising=False)
    assert CgroupFS().geteuid() == -1


def test_real_fs_nice(monkeypatch):
    asked = []

    def getpriority(which, who):
        asked.append((which, who))
        return 7

    monkeypatch.setattr(os, "getpriority", getpriority, raising=False)
    monkeypatch.setattr(os, "PRIO_PROCESS", 0, raising=False)
    assert CgroupFS().nice() == 7
    assert asked == [(0, 0)]

    def broken(which, who):
        raise OSError(errno.ESRCH, "gone")

    monkeypatch.setattr(os, "getpriority", broken)
    assert CgroupFS().nice() == 0
    monkeypatch.delattr(os, "getpriority")
    assert CgroupFS().nice() == 0


def test_real_fs_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(_cgroups.time, "sleep", slept.append)
    CgroupFS().sleep(0.25)
    assert slept == [0.25]


def test_container_reports_every_cap_without_a_path(tmp_path):
    backend = Backend(_kernel(tmp_path))
    container = Container(backend, None, {"cpu": "x"})
    assert container.attach(1, required=False) is None
    assert container.unenforced == {"cpu": "x"}
