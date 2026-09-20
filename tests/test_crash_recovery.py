"""Test state recovery after forced daemon termination.

Each test starts a daemon subprocess from source with a filesystem state
store in ``tmp_path``. It terminates the daemon with SIGKILL on POSIX or
TerminateProcess on Windows, then starts a new daemon using the same store.
Assertions check stored records through the package API, control API
responses, logs, and each job's launch log.

Jobs run in separate POSIX sessions or Windows process groups and survive
daemon termination. Tests that simulate a host crash also terminate the
job processes.
"""

import json
import random
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from cronstable import dagrun, state
from tests._crash_helpers import (
    HOLD_JOB,
    NEVER,
    QUICK_JOB,
    WAIT,
    CrashDaemon,
    command_yaml,
    kill_pid,
    lines,
    pid_gone,
    source_env,
    wait_for,
)


@pytest.fixture
def daemon(tmp_path):
    app = CrashDaemon(tmp_path / "node")
    try:
        yield app
    finally:
        app.close()


def _job(name, command, schedule=NEVER, extra=""):
    return (
        "  - name: {}\n    {}    {}"
        "    captureStdout: false\n    captureStderr: false\n{}"
    ).format(name, command, schedule, extra)


def _kill_job(app, tag):
    """Take the job child down too, as a host crash would."""
    for pid in app.job_pids(tag):
        kill_pid(pid)
        wait_for(
            "reaping job pid {}".format(pid),
            lambda pid=pid: pid_gone(pid),
        )


def _open_inflight(app, job):
    recs = app.records("inflight/" + job, newest_first=True)
    return bool(recs) and recs[0]["kind"] == "open"


# --- 1. kill while a job is running -----------------------------------------


def test_kill_mid_run_reconciles_one_interrupted_record(daemon):
    hold = daemon.script("hold.py", HOLD_JOB)
    daemon.configure("jobs:\n" + _job("hold", command_yaml(hold, "hold")))
    daemon.start()
    daemon.start_job("hold")
    daemon.wait("launching the job", lambda: daemon.job_pids("hold"))
    daemon.wait(
        "recording the in-flight run", lambda: _open_inflight(daemon, "hold")
    )
    opened = daemon.records("inflight/hold")[-1]
    assert opened["pid"] == daemon.job_pids("hold")[0]
    # nothing has completed, so the run ledger is empty at the kill.
    assert daemon.records("runs/hold") == []
    daemon.kill()
    _kill_job(daemon, "hold")

    daemon.start()
    # the API answers from the reconciled row at once: reconciliation
    # runs inside rehydration, before the web listener binds.
    runs = daemon.runs("hold")
    assert len(runs) == 1
    (run,) = runs
    assert run["outcome"] == "unknown"
    assert run["exit_code"] is None
    assert run["started_at"] is None and run["duration"] is None
    assert run["fail_reason"].startswith("run interrupted: no completion")
    # onMissed defaults to skip, so the row closes the slot at the run's
    # own start instant.
    assert run["finished_at"] == opened["startedAt"]
    assert "reconciled an interrupted run (reconciled-crash)" in (
        daemon.log_text()
    )
    # the durable side lands behind the per-job write chains.
    daemon.wait(
        "persisting the reconciled record",
        lambda: len(daemon.records("runs/hold")) == 1,
    )
    (durable,) = daemon.records("runs/hold")
    assert durable["outcome"] == "unknown"
    assert durable["ranAt"] == opened["startedAt"]
    daemon.wait(
        "closing the in-flight record",
        lambda: not _open_inflight(daemon, "hold"),
    )
    closed = daemon.records("inflight/hold")[-1]
    assert closed["kind"] == "closed"
    assert closed["reason"] == "reconciled-crash"
    assert closed["proc"] != opened["proc"]

    # a third daemon finds nothing left to reconcile: still one record,
    # still one launch.
    daemon.kill()
    daemon.start()
    assert len(daemon.runs("hold")) == 1
    assert len(daemon.records("runs/hold")) == 1
    assert "reconciled an interrupted run" not in daemon.log_text()
    assert len(daemon.job_pids("hold")) == 1


def test_kill_mid_reboot_run_does_not_fire_it_again(daemon):
    # Save the @reboot marker before launching the job. Restarting the daemon
    # during the same OS boot must not launch it again, even if it was
    # interrupted during the first run.
    hold = daemon.script("hold.py", HOLD_JOB)
    daemon.configure(
        "jobs:\n"
        + _job("boot", command_yaml(hold, "boot"), 'schedule: "@reboot"\n')
    )
    daemon.start()
    daemon.wait("launching the boot job", lambda: daemon.job_pids("boot"))
    daemon.wait(
        "recording the in-flight run", lambda: _open_inflight(daemon, "boot")
    )
    assert len(daemon.records("reboot/boot")) == 1
    daemon.kill()
    _kill_job(daemon, "boot")

    daemon.start()
    daemon.wait(
        "reconciling the interrupted boot run",
        lambda: len(daemon.records("runs/boot")) == 1,
    )
    assert [r["outcome"] for r in daemon.runs("boot")] == ["unknown"]
    # a graceful stop drains every launch this daemon made; it made none.
    daemon.stop()
    assert len(daemon.job_pids("boot")) == 1
    assert len(daemon.records("reboot/boot")) == 1


# --- 2. the job child that outlived its daemon ------------------------------


def test_orphaned_child_is_reported_and_left_open_until_it_exits(daemon):
    hold = daemon.script("hold.py", HOLD_JOB)
    daemon.configure("jobs:\n" + _job("hold", command_yaml(hold, "hold")))
    daemon.start()
    daemon.start_job("hold")
    (pid,) = daemon.wait("launching the job", lambda: daemon.job_pids("hold"))
    daemon.wait(
        "recording the in-flight run", lambda: _open_inflight(daemon, "hold")
    )
    daemon.kill()
    assert not pid_gone(pid), "the job died with its daemon"

    # The survivor is neither adopted nor killed: the next daemon reports
    # it and leaves the record open, so no interrupted row is invented
    # for a run that is still going.
    daemon.start()
    assert (
        "Job hold: the previous daemon's run (pid {}) still appears to "
        "be running; leaving its in-flight record open".format(pid)
    ) in daemon.log_text()
    assert daemon.runs("hold") == []
    assert daemon.request("/jobs/hold")["running"] is False
    assert _open_inflight(daemon, "hold")
    assert not pid_gone(pid)

    # the survivor runs to completion on its own; with no daemon attached
    # nothing records that, and the record stays open.
    daemon.release("hold")
    wait_for("the orphan finishing", lambda: pid_gone(pid))
    assert lines(daemon.work / "hold.finished.log") == [str(pid)]
    assert daemon.runs("hold") == []
    assert _open_inflight(daemon, "hold")

    # the daemon after that finds the pid gone and closes the record.
    daemon.kill()
    daemon.start()
    assert [r["outcome"] for r in daemon.runs("hold")] == ["unknown"]
    daemon.wait(
        "closing the in-flight record",
        lambda: not _open_inflight(daemon, "hold"),
    )
    assert len(daemon.job_pids("hold")) == 1


def test_surviving_reboot_run_is_not_launched_beside(daemon):
    hold = daemon.script("hold.py", HOLD_JOB)
    daemon.configure(
        "jobs:\n"
        + _job("boot", command_yaml(hold, "boot"), 'schedule: "@reboot"\n')
    )
    daemon.start()
    (pid,) = daemon.wait("launching the job", lambda: daemon.job_pids("boot"))
    daemon.wait(
        "recording the in-flight run", lambda: _open_inflight(daemon, "boot")
    )
    daemon.kill()
    daemon.start()
    assert "still appears to be running" in daemon.log_text()
    daemon.stop()
    assert daemon.job_pids("boot") == [pid]
    assert not pid_gone(pid)


# --- 3. kill with a retry pending -------------------------------------------

RETRY_JOB = """\
import os, sys
with open("retry.starts.log", "a") as f:
    f.write("%d\\n" % os.getpid())
if os.path.exists("failed-once"):
    sys.exit(0)
open("failed-once", "w").close()
sys.exit(23)
"""

RETRY_DELAY = 30


def _seconds_apart(iso, instant):
    return abs((datetime.fromisoformat(iso) - instant).total_seconds())


def test_kill_with_retry_pending_rearms_the_same_deadline(daemon):
    script = daemon.script("retry.py", RETRY_JOB)
    daemon.configure(
        "jobs:\n"
        + _job(
            "retry",
            command_yaml(script),
            # a scheduled fire arms the ladder (an API start does not),
            # and @reboot fires exactly once, as soon as the daemon is up.
            'schedule: "@reboot"\n',
            extra=(
                "    onFailure:\n      retry:\n        maximumRetries: 2\n"
                "        initialDelay: {0}\n        maximumDelay: {0}\n"
                "        backoffMultiplier: 1\n"
            ).format(RETRY_DELAY),
        )
    )
    daemon.start()

    def pending():
        job = daemon.request("/jobs/retry")
        return (job.get("last_run") or {}).get("exit_code") == 23 and (
            job.get("retry")
        )

    before = daemon.wait("arming the retry", pending)
    assert before["attempt"] == 1
    daemon.wait(
        "persisting the pending retry",
        lambda: (
            [r["kind"] for r in daemon.records("retries/retry")] == ["pending"]
        ),
    )
    (durable,) = daemon.records("retries/retry")
    daemon.kill()
    assert len(daemon.job_pids("retry")) == 1

    daemon.start()
    after = daemon.request("/jobs/retry")["retry"]
    # the ladder resumes where it stood: same attempt, and the deadline is
    # the absolute instant the dead daemon saved, not a fresh full delay.
    assert after["attempt"] == 1
    not_before = datetime.fromisoformat(durable["notBefore"])
    # the API derives its instant from the remaining sleep, so it lands
    # within scheduling noise of the saved one, a full delay short of what
    # a re-armed-from-scratch ladder would show.
    assert _seconds_apart(before["nextRetryAt"], not_before) < 1
    assert _seconds_apart(after["nextRetryAt"], not_before) < 1
    assert len(daemon.job_pids("retry")) == 1

    daemon.wait(
        "running the restored retry",
        lambda: any(r["outcome"] == "success" for r in daemon.runs("retry")),
        timeout=WAIT + RETRY_DELAY,
    )
    daemon.wait(
        "settling the ladder",
        lambda: not daemon.request("/jobs/retry").get("retry"),
    )
    runs = daemon.runs("retry")
    assert [(r["outcome"], r["exit_code"]) for r in runs] == [
        ("failure", 23),
        ("success", 0),
    ]
    assert datetime.fromisoformat(runs[1]["started_at"]) >= not_before
    assert len(daemon.job_pids("retry")) == 2
    daemon.wait(
        "persisting the settled ladder",
        lambda: daemon.records("retries/retry")[-1]["kind"] != "pending",
    )

    # and the settled ladder stays settled across one more crash.
    daemon.kill()
    daemon.start()
    assert not daemon.request("/jobs/retry").get("retry")
    daemon.stop()
    assert len(daemon.job_pids("retry")) == 2


# --- 4. kill while a job is paused ------------------------------------------


def test_kill_while_paused_keeps_the_pause(daemon):
    quick = daemon.script("quick.py", QUICK_JOB)
    daemon.configure(
        "jobs:\n"
        + _job(
            "tick",
            command_yaml(quick, "tick"),
            'schedule:\n      second: "*"\n',
        )
        + _job("idle", command_yaml(quick, "idle"))
    )
    daemon.start()
    daemon.wait("running on schedule", lambda: daemon.job_pids("tick"))
    paused = daemon.request(
        "/jobs/tick/pause",
        method="POST",
        body={"note": "crash test", "by": "pytest"},
    )["paused"]
    assert paused
    daemon.wait(
        "persisting the pause",
        lambda: (
            [r["kind"] for r in daemon.records("paused/tick")] == ["paused"]
        ),
    )

    def skipped():
        return [r for r in daemon.runs("tick") if r["skip_reason"] == "paused"]

    # the launch count stabilizes once the pause takes effect.
    daemon.wait("skipping a paused slot", skipped)
    launched = len(daemon.job_pids("tick"))
    daemon.kill()

    daemon.start()
    job = daemon.request("/jobs/tick")
    assert job["paused"]["note"] == "crash test"
    assert job["paused"]["by"] == "pytest"
    assert job["paused"]["since"] == paused["since"]
    assert not daemon.request("/jobs/idle").get("paused")
    seen = len(skipped())
    daemon.wait(
        "skipping paused slots after the restart",
        lambda: len(skipped()) >= seen + 2,
    )
    assert len(daemon.job_pids("tick")) == launched

    # resuming is durable the same way.
    daemon.request("/jobs/tick/resume", method="POST", body={})
    daemon.wait(
        "running again once resumed",
        lambda: len(daemon.job_pids("tick")) > launched,
    )
    daemon.wait(
        "persisting the resume",
        lambda: daemon.records("paused/tick")[-1]["kind"] == "resumed",
    )
    daemon.kill()
    daemon.start()
    assert not daemon.request("/jobs/tick").get("paused")


# --- 5. kill during a DAG run -----------------------------------------------


#: A killed daemon releases nothing, so its lease on the run stands until
#: it expires; only then can the next daemon adopt the run and advance it.
DAG_ADOPT = WAIT + 2 * dagrun.DAG_LEASE_TTL


def _dag(name, hold, quick, *, retries):
    return (
        "dags:\n  - name: {name}\n    tasks:\n"
        "      - id: first\n        {first}"
        "      - id: middle\n        dependsOn:\n          - first\n"
        "        retries: {retries}\n        retryDelaySeconds: 0\n"
        "        {middle}"
        "      - id: last\n        dependsOn:\n          - middle\n"
        "        {last}"
    ).format(
        name=name,
        retries=retries,
        first=command_yaml(quick, "first", indent=10),
        middle=command_yaml(hold, "middle", indent=10),
        last=command_yaml(quick, "last", indent=10),
    )


def _run_doc(app, name, run_key):
    return app.request("/dags/{}/runs/{}".format(name, run_key))


def _task(doc, task_id):
    return doc["tasks"][task_id]


def _kill_mid_dag(daemon, retries):
    hold = daemon.script("hold.py", HOLD_JOB)
    quick = daemon.script("quick.py", QUICK_JOB)
    daemon.configure(_dag("flow", hold, quick, retries=retries), job_api=True)
    daemon.start()
    run_key = daemon.request("/dags/flow/trigger", method="POST")["runKey"]
    daemon.wait("reaching the middle task", lambda: daemon.job_pids("middle"))
    daemon.wait(
        "recording the middle task's pid",
        lambda: (
            _task(_run_doc(daemon, "flow", run_key), "middle").get("pid")
            == daemon.job_pids("middle")[0]
        ),
    )
    doc = _run_doc(daemon, "flow", run_key)
    assert _task(doc, "first")["state"] == "success"
    assert _task(doc, "middle")["state"] == "running"
    daemon.kill()
    _kill_job(daemon, "middle")
    daemon.start()
    return run_key


def test_kill_mid_dag_run_fails_the_interrupted_task(daemon):
    run_key = _kill_mid_dag(daemon, retries=0)
    doc = daemon.wait(
        "finishing the rehydrated run",
        lambda: (
            (d := _run_doc(daemon, "flow", run_key))["state"] == "failed" and d
        ),
        timeout=DAG_ADOPT,
    )
    middle = _task(doc, "middle")
    assert middle["state"] == "failed"
    assert middle["failReason"] == "reconciled-crash"
    assert middle["pid"] is None and middle["proc"] is None
    assert _task(doc, "first")["state"] == "success"
    assert _task(doc, "last")["state"] == "upstream_failed"
    assert "reconciled 1 interrupted task(s)" in daemon.log_text()
    # nothing ran twice, and the task behind the failure never ran.
    daemon.stop()
    assert len(daemon.job_pids("first")) == 1
    assert len(daemon.job_pids("middle")) == 1
    assert daemon.job_pids("last") == []


def test_kill_mid_dag_run_retries_the_interrupted_task(daemon):
    run_key = _kill_mid_dag(daemon, retries=1)
    # the crash spent one attempt; the second launches without a trigger.
    daemon.wait(
        "relaunching the interrupted task",
        lambda: len(daemon.job_pids("middle")) == 2,
        timeout=DAG_ADOPT,
    )
    daemon.release("middle")
    doc = daemon.wait(
        "finishing the rehydrated run",
        lambda: (
            (d := _run_doc(daemon, "flow", run_key))["state"] == "success"
            and d
        ),
    )
    assert _task(doc, "middle")["attempt"] == 1
    assert [_task(doc, t)["state"] for t in ("first", "middle", "last")] == [
        "success"
    ] * 3
    daemon.stop()
    # the completed task stayed completed: one launch, before the crash.
    assert len(daemon.job_pids("first")) == 1
    assert len(daemon.job_pids("middle")) == 2
    assert len(daemon.job_pids("last")) == 1


def test_kill_with_gate_pending_keeps_the_gate(daemon):
    quick = daemon.script("quick.py", QUICK_JOB)
    daemon.configure(
        "dags:\n  - name: gated\n    tasks:\n"
        "      - id: first\n        {}"
        "      - id: gate\n        type: approval\n"
        "        dependsOn:\n          - first\n"
        "      - id: last\n        dependsOn:\n          - gate\n"
        "        {}".format(
            command_yaml(quick, "first", indent=10),
            command_yaml(quick, "last", indent=10),
        ),
        job_api=True,
    )
    daemon.start()
    run_key = daemon.request("/dags/gated/trigger", method="POST")["runKey"]

    def waiting():
        gate = _task(_run_doc(daemon, "gated", run_key), "gate")
        return gate.get("awaitingApproval") and gate

    before = daemon.wait("reaching the gate", waiting)
    daemon.kill()

    daemon.start()
    doc = _run_doc(daemon, "gated", run_key)
    assert doc["state"] == "running"
    gate = _task(doc, "gate")
    # an approval gate retains its state after the crash and keeps
    # waiting, untouched.
    assert gate["state"] == "running" and gate["awaitingApproval"]
    assert gate["startedAt"] == before["startedAt"]
    assert "interrupted task" not in daemon.log_text()
    assert daemon.job_pids("last") == []

    decided = daemon.request(
        "/dags/gated/runs/{}/tasks/gate/decision".format(run_key),
        method="POST",
        body={"decision": "approve", "by": "pytest"},
    )
    assert decided["ok"]
    daemon.wait(
        "finishing the approved run",
        lambda: _run_doc(daemon, "gated", run_key)["state"] == "success",
        timeout=DAG_ADOPT,
    )
    daemon.stop()
    assert len(daemon.job_pids("first")) == 1
    assert len(daemon.job_pids("last")) == 1


# --- 6. repeated kills ------------------------------------------------------

KILL_ROUNDS = 5


def _assert_store_is_clean(state_dir):
    """Check that visible state files contain complete, valid records.

    Read files directly because the package readers would quarantine invalid
    records before this check could detect them. Exclude quarantine and
    temporary directories, which can contain incomplete or damaged files.
    """
    base = Path(state_dir) / "default"
    checked = 0
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        top = path.relative_to(base).parts[0]
        if top in (state.QUARANTINE_DIR, state.TMP_DIR):
            continue
        assert not path.name.endswith(".tmp"), path
        if path.suffix in (".json", ".doc", ".lease"):
            raw = path.read_bytes()
            assert raw, "empty visible record: {}".format(path)
            obj = json.loads(raw)
            assert isinstance(obj, dict), path
            if top == state.RECORDS_DIR:
                assert obj["schemaVersion"] == state.SCHEMA_VERSION, path
                assert isinstance(obj["data"], dict), path
            checked += 1
    return checked


def test_kill_loop_leaves_a_clean_store_and_a_working_scheduler(daemon):
    hold = daemon.script("hold.py", HOLD_JOB)
    quick = daemon.script("quick.py", QUICK_JOB)
    every_second = 'schedule:\n      second: "*"\n'
    daemon.configure(
        "jobs:\n"
        + _job("ok", command_yaml(quick, "ok"), every_second)
        + _job("bad", command_yaml(quick, "bad", 3), every_second)
        + _job("hold", command_yaml(hold, "hold"))
        + _dag("flow", hold, quick, retries=0),
        job_api=True,
    )
    # a fixed seed keeps the kill points reproducible; they are counted
    # in completed writes, not in wall time.
    rng = random.Random(20260919)
    for _ in range(KILL_ROUNDS):
        daemon.start()
        base = len(daemon.records("runs/ok"))
        target = base + rng.randint(1, 4)
        if rng.random() < 0.5:
            daemon.start_job("hold")
        if rng.random() < 0.5:
            daemon.request("/dags/flow/trigger", method="POST")
        daemon.wait(
            "writing more run records",
            lambda target=target: len(daemon.records("runs/ok")) >= target,
        )
        daemon.kill()
        _kill_job(daemon, "hold")
        _kill_job(daemon, "middle")

    assert _assert_store_is_clean(daemon.state_dir) > 10
    # the offline checker agrees, through the real CLI.
    check = subprocess.run(
        [
            sys.executable,
            "-m",
            "cronstable",
            "state",
            "check",
            "-c",
            str(daemon.config),
        ],
        cwd=daemon.work,
        env=source_env(),
        capture_output=True,
        text=True,
        timeout=WAIT,
    )
    assert check.returncode == 0, check.stdout + check.stderr
    assert "quarantined: 0 record(s)" in check.stdout

    daemon.start()
    log = daemon.log_text()
    assert "Traceback" not in log
    assert "quarantin" not in log
    before = len(daemon.job_pids("ok"))
    daemon.wait(
        "scheduling again after the last kill",
        lambda: len(daemon.job_pids("ok")) >= before + 2,
    )
    outcomes = {r["outcome"] for r in daemon.runs("bad")}
    assert outcomes <= {"failure", "unknown"}
    assert {r["outcome"] for r in daemon.runs("ok")} <= {"success", "unknown"}
    daemon.stop()
    assert _assert_store_is_clean(daemon.state_dir) > 10
    quarantine = daemon.state_dir / "default" / state.QUARANTINE_DIR
    assert list(quarantine.iterdir()) == []


# --- 7. two daemons, one store ----------------------------------------------

LEASE_TTL = 3


def _cluster_head(store, node):
    return (
        "cluster:\n  backend: filesystem\n  nodeName: {}\n"
        "  filesystem:\n    path: {}\n    ttl: {}\n    topology: shared\n"
    ).format(node, json.dumps(str(store)), LEASE_TTL)


def _leader(app):
    return app.request("/cluster").get("leader")


def test_second_daemon_defers_then_takes_over_a_killed_leader(tmp_path):
    store = tmp_path / "shared"
    nodes = []
    try:
        for node in ("alpha", "beta"):
            app = CrashDaemon(tmp_path / node, state_dir=store)
            nodes.append(app)
            quick = app.script("quick.py", QUICK_JOB)
            app.configure(
                "jobs:\n"
                + _job(
                    "led",
                    command_yaml(quick, "led"),
                    'schedule:\n      second: "*"\n',
                    extra="    clusterPolicy: Leader\n",
                ),
                head=_cluster_head(store, node),
            )
        alpha, beta = nodes
        alpha.start()
        alpha.wait("alpha winning", lambda: _leader(alpha) == "alpha")
        alpha.wait("alpha running the job", lambda: alpha.job_pids("led"))

        # the second daemon on the same store sees the live lease, stays
        # a follower and runs nothing.
        beta.start()
        beta.wait("beta seeing the leader", lambda: _leader(beta) == "alpha")
        view = beta.request("/cluster")
        assert view["is_leader"] is False
        ran = len(alpha.job_pids("led"))
        alpha.wait(
            "alpha still running the job",
            lambda: len(alpha.job_pids("led")) >= ran + 2,
        )
        assert _leader(beta) == "alpha"
        assert beta.job_pids("led") == []
        fence = beta.request("/cluster")["lease"]["fence"]

        # a killed holder releases nothing; the follower waits out the
        # lease and then takes over with a higher fence.
        alpha.kill()
        assert _leader(beta) == "alpha"
        beta.wait("beta taking over", lambda: _leader(beta) == "beta")
        view = beta.request("/cluster")
        assert view["is_leader"] is True
        assert view["lease"]["fence"] > fence
        beta.wait("beta running the job", lambda: beta.job_pids("led"))

        # the old leader comes back as a follower of the new one.
        ran = len(alpha.job_pids("led"))
        alpha.start()
        alpha.wait("alpha deferring", lambda: _leader(alpha) == "beta")
        assert alpha.request("/cluster")["is_leader"] is False
        seen = len(beta.job_pids("led"))
        beta.wait(
            "beta still running the job",
            lambda: len(beta.job_pids("led")) >= seen + 2,
        )
        assert len(alpha.job_pids("led")) == ran
    finally:
        for app in nodes:
            app.close()
