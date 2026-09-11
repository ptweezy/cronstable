import urllib.error

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
    daemon.request("/jobs/retry/start", method="POST")

    def pending_failure():
        job = daemon.request("/jobs/retry")
        return (job.get("last_run") or {}).get("exit_code") == 23 and job.get(
            "retry"
        )

    retry = daemon.wait(
        "arming a retry after a deliberate failure", pending_failure
    )
    assert retry["attempt"] == 1 and retry["nextRetryAt"]
    assert daemon.lines("attempts.log") == ["attempt"]
    daemon.stop()
    assert daemon.lines("attempts.log") == ["attempt"], (
        "Retry ran before restart"
    )
    daemon.start()  # No second POST: only the durable retry can start work.
    assert daemon.runs("retry")[0]["exit_code"] == 23
    daemon.wait(
        "completing the restored retry",
        lambda: any(r["outcome"] == "success" for r in daemon.runs("retry")),
    )
    daemon.wait(
        "settling the retry ladder",
        lambda: not daemon.request("/jobs/retry").get("retry"),
    )
    assert [(r["outcome"], r["exit_code"]) for r in daemon.runs("retry")] == [
        ("failure", 23),
        ("success", 0),
    ]
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
    assert daemon.process.poll() is None, (
        "Daemon exited before the active job finished"
    )
    (daemon.work / "drain.release").touch()
    daemon.finish_shutdown()
    assert daemon.lines("finished.log") == ["finished"]
    assert not daemon.lines("expired.log"), (
        "Workload expired instead of draining"
    )
