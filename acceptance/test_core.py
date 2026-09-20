import time
import urllib.error
from datetime import datetime

import pytest


def test_startup_and_public_surfaces(daemon, request):
    version = daemon.cli("--version").strip()
    assert version
    expected = request.config.getoption("--expected-version")
    if expected is not None:
        assert version == expected
    daemon.configure("probe", "retry")
    daemon.cli("--validate-config", "-c", str(daemon.config))
    invalid = daemon.work / "invalid.yaml"
    invalid.write_text("jobs: [broken\n", encoding="utf-8")
    daemon.cli("--validate-config", "-c", str(invalid), expected=1)
    daemon.start()
    with pytest.raises(urllib.error.HTTPError) as denied:
        daemon.request("/jobs", authenticated=False)
    assert denied.value.code == 401
    assert [job["name"] for job in daemon.request("/jobs")] == ["probe"]
    page = daemon.request("/", raw=True)
    assert "<html" in page.lower() and "cronstable" in page.lower()
    daemon.stop()


def test_scheduled_cli_state_survives_restart(daemon):
    daemon.configure("roundtrip", "state", scheduled=True)
    daemon.start()
    # A read happens BEFORE every write. Only another scheduled run can
    # supply the random value; successful exit alone is not sufficient.
    daemon.wait(
        "reading a prior scheduled run's KV write",
        lambda: daemon.value in daemon.lines("reads.log"),
    )
    daemon.wait(
        "recording two successful scheduled runs",
        lambda: (
            len(
                [
                    r
                    for r in daemon.runs("roundtrip")
                    if r["outcome"] == "success"
                ]
            )
            >= 2
        ),
    )
    daemon.stop()
    previous = daemon.lines("reads.log")
    # The SAME configuration now executes only state get. A restarted daemon
    # cannot pass by writing the expected value back into an empty store.
    (daemon.work / "read-only").touch()
    daemon.start()
    history = daemon.runs("roundtrip")
    assert sum(r["outcome"] == "success" for r in history) >= 2
    daemon.wait(
        "reading persisted KV after restart",
        lambda: len(daemon.lines("reads.log")) > len(previous),
    )
    assert all(value == daemon.value for value in daemon.lines("reads.log"))
    daemon.stop()


def test_pending_retry_survives_restart(daemon):
    daemon.configure("retry", "retry", retry=True)
    daemon.start()
    (daemon.work / "retry.release").touch()

    def pending_failure():
        job = daemon.request("/jobs/retry")
        return (job.get("last_run") or {}).get("exit_code") == 23 and job.get(
            "retry"
        )

    retry = daemon.wait(
        "arming a retry after a deliberate failure", pending_failure
    )
    assert retry["attempt"] == 1 and retry["nextRetryAt"]
    not_before = datetime.fromisoformat(retry["nextRetryAt"])
    assert daemon.lines("attempts.log") == ["attempt"]
    daemon.stop()
    assert daemon.lines("attempts.log") == ["attempt"], (
        "Retry ran before restart"
    )
    daemon.start()  # The durable @reboot marker prevents a fresh boot run.
    assert daemon.runs("retry")[0]["exit_code"] == 23
    daemon.wait(
        "completing the restored retry",
        lambda: any(r["outcome"] == "success" for r in daemon.runs("retry")),
    )
    daemon.wait(
        "settling the retry ladder",
        lambda: not daemon.request("/jobs/retry").get("retry"),
    )
    runs = daemon.runs("retry")
    assert [(r["outcome"], r["exit_code"]) for r in runs] == [
        ("failure", 23),
        ("success", 0),
    ]
    assert datetime.fromisoformat(runs[1]["started_at"]) >= not_before
    daemon.stop()
    assert daemon.lines("attempts.log") == ["attempt", "attempt"]


def test_shutdown_drains_running_job(daemon):
    daemon.configure("drain", "drain")
    daemon.start()
    daemon.request("/jobs/drain/start", method="POST")
    daemon.wait(
        "starting the drain workload", lambda: daemon.lines("started.log")
    )
    assert not daemon.lines("finished.log")
    daemon.begin_shutdown()
    # Hold the barrier long enough to observe an early-exit regression before
    # allowing the workload to complete. wait() checks liveness every poll.
    requested = time.monotonic()
    daemon.wait(
        "keeping the daemon alive while the job is draining",
        lambda: time.monotonic() - requested >= 1,
        timeout=5,
    )
    (daemon.work / "drain.release").touch()
    daemon.finish_shutdown()
    assert daemon.lines("finished.log") == ["finished"]
    assert not daemon.lines("expired.log"), (
        "Workload expired instead of draining"
    )


def test_hard_kill_mid_run_reconciles_on_restart(daemon):
    daemon.configure("crash", "drain")
    daemon.start()
    daemon.request("/jobs/crash/start", method="POST")
    daemon.wait("starting the workload", lambda: daemon.lines("started.log"))
    assert daemon.runs("crash") == []
    # No handler runs: the daemon cannot record a completion, close the
    # in-flight record, or release anything.
    daemon.kill()
    # Let the surviving job finish so the next daemon can reconcile its
    # record after the process exits.
    (daemon.work / "drain.release").touch()
    daemon.wait_for_orphans()

    daemon.start()
    (run,) = daemon.runs("crash")
    assert run["outcome"] == "unknown" and run["exit_code"] is None
    assert run["started_at"] is None and run["duration"] is None
    assert run["fail_reason"].startswith("run interrupted")
    assert "reconciled an interrupted run" in daemon.log_text()
    assert daemon.request("/jobs/crash")["running"] is False
    daemon.stop()

    # The reconciled row is durable and final: a third daemon adds
    # nothing to it, and nothing launched the job a second time.
    daemon.start()
    assert [r["outcome"] for r in daemon.runs("crash")] == ["unknown"]
    assert "reconciled an interrupted run" not in daemon.log_text()
    daemon.stop()
    assert daemon.lines("started.log") == ["started"]
    assert daemon.store_files() > 0


def test_hard_kill_loop_leaves_a_clean_store(daemon):
    daemon.configure("tick", "tick", scheduled=True)
    for extra in (1, 3, 2, 1):
        daemon.start()
        target = len(daemon.runs("tick")) + extra
        daemon.wait(
            "recording more scheduled runs",
            lambda target=target: len(daemon.runs("tick")) >= target,
        )
        daemon.kill()
        daemon.wait_for_orphans()

    # Whatever each kill interrupted, every file a reader can reach is a
    # complete record, and the offline checker agrees.
    assert daemon.store_files() > 0
    report = daemon.cli("state", "check", "-c", str(daemon.config))
    assert "quarantined: 0 record(s)" in report

    daemon.start()
    log = daemon.log_text()
    assert "Traceback" not in log and "quarantin" not in log
    ticks = len(daemon.lines("ticks.log"))
    daemon.wait(
        "scheduling again after the last kill",
        lambda: len(daemon.lines("ticks.log")) >= ticks + 2,
    )
    outcomes = {r["outcome"] for r in daemon.runs("tick")}
    assert "success" in outcomes and outcomes <= {"success", "unknown"}
    daemon.stop()
    assert daemon.store_files() > 0
