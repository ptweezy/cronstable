import asyncio
import datetime
import gc
import os
import signal
import time
import warnings
from collections import OrderedDict
from pathlib import Path

import pytest

import cronstable.cron
from cronstable import cron as cron_mod
from cronstable import platform
from cronstable.config import ConfigError, JobConfig, parse_config_string
from cronstable.cron import Cron, JobRunInfo, _job_run_info_from_dict
from cronstable.job import JobOutputStream, JobRetryState, RunningJob
from cronstable.redact import REDACTED
from cronstable.state import make_state_backend
from tests._commands import cmd_hang, cmd_print, yaml_command
from tests._configs import _DEP_JOB, _ONE_JOB, _TLS_CLUSTER_YAML
from tests._cron_helpers import (
    _WEB_ONE_JOB,
    CONCURRENT_JOB,
    DT,
    JOB_THAT_SUCCEEDS,
    TWO_JOBS,
    UTC,
    _FakeMesh,
    _noop,
    _reboot_job,
    _reboot_mgr,
    _wait_until,
    fixed_current_time,  # noqa: F401
)
from tests._helpers import _UTC, _state_cfg
from tests.conftest import Req
from tests.test_state import (
    _NOW,
    _catchup_yaml,
    _count_launcher,
    _cron_with_watermark,
)


@pytest.fixture()
def tracing_running_job(monkeypatch):
    TracingRunningJob._TRACE = asyncio.Queue()
    monkeypatch.setattr(cronstable.cron, "RunningJob", TracingRunningJob)
    yield TracingRunningJob
    TracingRunningJob._TRACE = asyncio.Queue()


@pytest.fixture
async def run_cron():
    """Drive cron.run() as a background task with a guaranteed graceful
    stop (finding B1: the signal_shutdown try/finally idiom), the local
    twin of tests/test_cron_web.py's fixture.  Teardown signals shutdown
    and drains the task with the same 5s bound the try/finally sites used;
    a test that already stopped its own cron is unaffected (signal_shutdown
    is idempotent and the task already done).
    """
    running = []

    def start(cron):
        task = asyncio.create_task(cron.run())
        running.append((cron, task))
        return task

    yield start
    for cron, task in reversed(running):
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


class TracingRunningJob(RunningJob):
    _TRACE = asyncio.Queue()

    def __init__(self, config: JobConfig, retry_state, **kwargs) -> None:
        super().__init__(config, retry_state, **kwargs)
        self._TRACE.put_nowait((time.perf_counter(), "create", self))

    async def start(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "start", self))
        await super().start()
        self._TRACE.put_nowait((time.perf_counter(), "started", self))

    async def wait(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "wait", self))
        await super().wait()
        self._TRACE.put_nowait((time.perf_counter(), "waited", self))

    async def cancel(self) -> None:
        self._TRACE.put_nowait((time.perf_counter(), "cancel", self))
        await super().cancel()
        self._TRACE.put_nowait((time.perf_counter(), "cancelled", self))

    async def report_failure(self):
        self._TRACE.put_nowait((time.perf_counter(), "report_failure", self))
        await super().report_failure()

    async def report_permanent_failure(self):
        self._TRACE.put_nowait(
            (time.perf_counter(), "report_permanent_failure", self)
        )
        await super().report_permanent_failure()

    async def report_success(self):
        self._TRACE.put_nowait((time.perf_counter(), "report_success", self))
        await super().report_success()


JOB_THAT_FAILS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + '\n    schedule: "@reboot"\n'
)


@pytest.mark.parametrize(
    "config_yaml, expected_events",
    [
        (
            JOB_THAT_SUCCEEDS,
            ["create", "start", "started", "wait", "waited", "report_success"],
        ),
        (
            JOB_THAT_FAILS,
            [
                "create",
                "start",
                "started",
                "wait",
                "waited",
                "report_failure",
                "report_permanent_failure",
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_simple(tracing_running_job, config_yaml, expected_events):
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)

    events = []

    async def wait_and_quit():
        the_job = None
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            print(ts, event)
            if the_job is None:
                job = the_job
            else:
                assert job is the_job
            events.append(event)
            if event in {"report_success", "report_permanent_failure"}:
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == expected_events


RETRYING_JOB_THAT_FAILS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 2
        initialDelay: 0.1
        maximumDelay: 1
        backoffMultiplier: 2
"""
)


@pytest.mark.asyncio
async def test_fail_retry(tracing_running_job):
    cron = cronstable.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS)

    events = []

    async def wait_and_quit():
        known_jobs = {}
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
            print(ts, event, jobnum)
            events.append((jobnum, event))
            if jobnum == 3 and event == "report_permanent_failure":
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == [
        # initial attempt
        (1, "create"),
        (1, "start"),
        (1, "started"),
        (1, "wait"),
        (1, "waited"),
        (1, "report_failure"),
        # first retry
        (2, "create"),
        (2, "start"),
        (2, "started"),
        (2, "wait"),
        (2, "waited"),
        (2, "report_failure"),
        # second retry
        (3, "create"),
        (3, "start"),
        (3, "started"),
        (3, "wait"),
        (3, "waited"),
        (3, "report_failure"),
        (3, "report_permanent_failure"),
    ]


JOB_THAT_HANGS = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_hang("starting...", 10))
    + """
    schedule: "@reboot"
    captureStdout: true
    executionTimeout: 0.25
    killTimeout: 0.25
"""
)


@pytest.mark.asyncio
async def test_execution_timeout(tracing_running_job):
    cron = cronstable.cron.Cron(None, config_yaml=JOB_THAT_HANGS)

    events = []
    jobs_stdout = {}

    async def wait_and_quit():
        known_jobs = {}
        while True:
            ts, event, job = await tracing_running_job._TRACE.get()
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
            print(ts, event, jobnum)
            events.append((jobnum, event))
            if jobnum == 1 and event == "report_permanent_failure":
                jobs_stdout[jobnum] = job.stdout
                break
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    assert events == [
        # initial attempt
        (1, "create"),
        (1, "start"),
        (1, "started"),
        (1, "wait"),
        (1, "cancel"),
        (1, "cancelled"),
        (1, "waited"),
        (1, "report_failure"),
        (1, "report_permanent_failure"),
    ]
    assert jobs_stdout[1] == "starting...\n"


@pytest.mark.parametrize("policy", ["Allow", "Forbid", "Replace"])
@pytest.mark.asyncio
async def test_concurrency_policy(policy):
    # Launch the same long-running job twice and assert the second launch is
    # handled per the policy. Driven directly (no wall-clock dependence) so it
    # is deterministic.
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy=policy)
    )
    job = cron.cron_jobs["test"]
    try:
        await cron.maybe_launch_job(job)  # first instance
        first = cron.running_jobs["test"][0]
        assert first.proc.returncode is None

        await cron.maybe_launch_job(job)  # second launch, subject to policy
        running = cron.running_jobs["test"]

        if policy == "Allow":
            assert len(running) == 2
            assert all(rj.proc.returncode is None for rj in running)
            assert first.replaced is False
        elif policy == "Forbid":
            # second launch skipped; the original instance is untouched
            assert running == [first]
            assert first.proc.returncode is None
            assert first.replaced is False
        else:  # Replace
            assert first.replaced is True
            # the first instance was actually terminated...
            assert first.proc.returncode is not None
            # ...and exactly one fresh instance is now running
            others = [rj for rj in running if rj is not first]
            assert len(others) == 1
            assert others[0].proc.returncode is None
    finally:
        for rj in list(cron.running_jobs.get("test", [])):
            if rj.proc is not None and rj.proc.returncode is None:
                await rj.cancel()


@pytest.mark.asyncio
async def test_concurrent_launches_cannot_double_start_a_forbid_job(
    monkeypatch,
):
    # regression: the Forbid/Replace gate reads running_jobs several awaits
    # BEFORE the launch appends to it (the cluster slot claim, the
    # subprocess spawn inside start()), so two concurrent entries (a
    # dashboard double-click, a manual start racing the scheduled fire)
    # both saw the gate open and double-launched a Forbid job. The per-job
    # launch lock serialises them: exactly one instance starts, the loser
    # takes the ordinary Forbid drop.
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy="Forbid")
    )
    job = cron.cron_jobs["test"]

    # widen the gate-to-append window deterministically: the first launch
    # parks inside start() (a slow spawn) until released.
    release = asyncio.Event()
    real_start = RunningJob.start

    async def slow_start(self):
        await release.wait()
        await real_start(self)

    monkeypatch.setattr(RunningJob, "start", slow_start)
    try:
        first = asyncio.ensure_future(cron.maybe_launch_job(job))
        second = asyncio.ensure_future(cron.maybe_launch_job(job))
        # let both tasks run up to their park/lock-wait before releasing,
        # so the second entry genuinely overlaps the first's spawn window.
        for _ in range(10):
            await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second)

        assert sorted(results) == [False, True]
        assert len(cron.running_jobs["test"]) == 1
    finally:
        for rj in list(cron.running_jobs.get("test", [])):
            if rj.proc is not None and rj.proc.returncode is None:
                await rj.cancel()


FAILED_SPAWN_REPLACE_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(["cronstable-no-such-binary-xyz"])
    + """
    schedule: "@reboot"
    concurrencyPolicy: Replace
"""
)


@pytest.mark.asyncio
async def test_replace_policy_survives_failed_spawn():
    # A spawn failure registers the instance with proc=None (start_failed);
    # the next fire's Replace branch then cancels whatever running_jobs holds.
    # cancel() raising RuntimeError("process is not running") there used to
    # escape maybe_launch_job -- which spawn_jobs runs OUTSIDE run()'s
    # try/except -- and kill the whole scheduler on the second fire after a
    # bad deploy. (The cluster slot-renewer cancels through the same method,
    # so this guards that path too.)
    cron = cronstable.cron.Cron(None, config_yaml=FAILED_SPAWN_REPLACE_JOB)
    job = cron.cron_jobs["test"]

    await cron.maybe_launch_job(job)
    first = cron.running_jobs["test"][0]
    assert first.proc is None
    assert first.start_failed is True

    await cron.maybe_launch_job(job)  # Replace branch: must not raise
    assert first.replaced is True

    # the reaper still completes the never-spawned instance normally
    await first.wait()
    assert first.retcode == 127


@pytest.mark.asyncio
async def test_handle_finished_job_skips_replaced(monkeypatch):
    # a job cancelled to make way for a replacement must not be reported as a
    # success or failure (and must not trigger retries).
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=True,
        cancelled=False,
        fail_reason="failsWhen=nonzeroReturn and retcode=-15",
        retcode=-15,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == []  # replaced -> neither reported
    assert "test" not in cron.running_jobs  # still cleaned up
    assert "test" not in cron.last_run  # replaced runs aren't recorded
    assert "test" not in cron.run_history  # nor added to history


@pytest.mark.asyncio
async def test_handle_finished_job_replaced_busts_memos(monkeypatch):
    # a replaced instance records no run row, but its removal still flips
    # the payload's running flag: the memo bust must fire before the
    # replaced early return, not be skipped with the run recording.
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    busts = []
    monkeypatch.setattr(
        cron, "_bust_response_memos", lambda: busts.append(1)
    )

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=True,
        cancelled=False,
        fail_reason=None,
        retcode=None,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert busts  # the running flag flip renders on the next poll
    assert "test" not in cron.running_jobs


@pytest.mark.asyncio
async def test_handle_finished_job_reports_normal_failure(monkeypatch):
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=False,
        cancelled=False,
        start_failed=False,
        fail_reason="failsWhen=nonzeroReturn and retcode=2",
        retcode=2,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)
    # the report+retry-arm sequence runs as a spawned per-job task now
    await cron._drain_completions()

    assert calls == [("failure", job)]
    # the finished run is recorded for the web UI
    assert cron.last_run["test"].outcome == "failure"
    assert cron.last_run["test"].exit_code == 2
    # ...and appended to the bounded run history
    assert [r.outcome for r in cron.run_history["test"]] == ["failure"]


@pytest.mark.asyncio
async def test_reaper_finishes_whole_batch_when_one_job_raises(
    monkeypatch, caplog
):
    # Regression: one job failing to finish must not take the rest of the
    # reaper's batch with it, nor strand the DAG-task completions that batch
    # has already buffered.
    #
    # Reachable in production: _handle_finished_job awaits
    # _job_api.finish_run, which touches the state backend (locks.release_all)
    # and can raise JobStateError("state backend is unavailable", 503). That
    # call used to be unguarded, with flush_completions after it inside the
    # same try, so a single raise (a) abandoned the jobs the batch had not
    # reached yet and (b) skipped the flush, leaving the completions buffered
    # by the jobs already handled RUNNING in their dag_run until some
    # unrelated later job reached the next flush; nothing else drains
    # _completion_buffer (_retry_completions only sees _pending_completions).
    # Batching is what turned a per-job failure into a cross-job one.
    import logging
    from types import SimpleNamespace

    from cronstable.jobstate import JobStateError

    cron = cronstable.cron.Cron(None)
    # shutdown already signalled, so the reaper returns as soon as the running
    # set drains: one batch is all this test needs.
    cron._stop_event.set()

    class FakeRunningJob:
        # only what the reaper touches: a wait() for an already-exited
        # process and a name for the log line. A class rather than a
        # SimpleNamespace because the reaper keys its wait-task map (and its
        # done set) by job, and SimpleNamespace is unhashable.
        def __init__(self, name):
            self.config = SimpleNamespace(name=name)

        async def wait(self):
            return None

    for name in ("t1", "t2", "t3"):
        cron.running_jobs[name] = [FakeRunningJob(name)]

    ref = ("dag", "run-key")
    handled = []

    async def fake_handle_finished_job(job):
        # mirrors the real handler's order: the instance leaves running_jobs
        # first, then finish_run (which is what 503s), then the completion is
        # buffered for the batch flush.
        cron.running_jobs.pop(job.config.name, None)
        handled.append(job.config.name)
        if len(handled) == 2:
            # the second job of the batch to be handled fails. done_jobs is a
            # set, so which job that is is not fixed; failing on the second
            # one guarantees both a completion already buffered (to strand)
            # and a job not yet reached (to abandon), whatever the order.
            raise JobStateError("state backend is unavailable", status=503)
        cron._dag._completion_buffer.setdefault(ref, []).append(
            {"taskkey": job.config.name}
        )

    recorded = []

    async def fake_flush_run_completions(run_ref, entries):
        recorded.extend((run_ref, entry["taskkey"]) for entry in entries)

    monkeypatch.setattr(cron, "_handle_finished_job", fake_handle_finished_job)
    monkeypatch.setattr(
        cron._dag, "_flush_run_completions", fake_flush_run_completions
    )

    reaper = asyncio.create_task(cron._wait_for_running_jobs())
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        try:
            # the fixed reaper awaits no timer: it drains the batch, flushes
            # and returns. Only a regression is still running here, parked in
            # the whole-loop handler's 1-second back-off.
            await asyncio.wait_for(reaper, timeout=0.5)
        except asyncio.TimeoutError:
            reaper.cancel()
            await asyncio.gather(reaper, return_exceptions=True)

    # (a) the raise did not abandon the rest of the batch
    assert sorted(handled) == ["t1", "t2", "t3"]
    # (b) ...nor skip the flush: every completion buffered around it was
    # recorded (the raiser never got as far as buffering its own), leaving
    # the buffer drained rather than stranded.
    assert sorted(key for _, key in recorded) == sorted(
        name for name in handled if name != handled[1]
    )
    assert all(run_ref == ref for run_ref, _ in recorded)
    assert cron._dag._completion_buffer == {}
    # and it stayed a per-job event: the whole-loop handler (whose back-off
    # stalls the reaper for a second) never saw it.
    messages = [r.message for r in caplog.records]
    assert any("bug (6)" in m for m in messages)
    assert not any("bug (3)" in m for m in messages)


@pytest.mark.asyncio
async def test_reaper_flushes_completions_even_when_the_batch_unwinds(
    monkeypatch,
):
    # The companion to the test above, pinning the OTHER half of the fix.
    # That one is satisfied by the per-job try/except alone: it swallows the
    # JobStateError before anything can escape the batch loop, so it would
    # still pass with the try/finally removed. This one uses CancelledError,
    # which the per-job guard deliberately re-raises, so the flush is reached
    # only because it sits in a finally. Without it, the completions the
    # batch had already buffered are lost on the way out.
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    cron._stop_event.set()

    class FakeRunningJob:
        def __init__(self, name):
            self.config = SimpleNamespace(name=name)

        async def wait(self):
            return None

    for name in ("t1", "t2"):
        cron.running_jobs[name] = [FakeRunningJob(name)]

    ref = ("dag", "run-key")
    handled = []

    async def fake_handle_finished_job(job):
        cron.running_jobs.pop(job.config.name, None)
        handled.append(job.config.name)
        if len(handled) == 2:
            # escapes the per-job guard by design
            raise asyncio.CancelledError()
        cron._dag._completion_buffer.setdefault(ref, []).append(
            {"taskkey": job.config.name}
        )

    recorded = []

    async def fake_flush_run_completions(run_ref, entries):
        recorded.extend((run_ref, entry["taskkey"]) for entry in entries)

    monkeypatch.setattr(cron, "_handle_finished_job", fake_handle_finished_job)
    monkeypatch.setattr(
        cron._dag, "_flush_run_completions", fake_flush_run_completions
    )

    reaper = asyncio.create_task(cron._wait_for_running_jobs())
    # the cancellation propagates out of the reaper, which is correct: what
    # matters is that the buffered completion was flushed on the way.
    await asyncio.gather(reaper, return_exceptions=True)

    assert recorded == [(ref, handled[0])]
    assert cron._dag._completion_buffer == {}


def test_simple_config_file(tracing_running_job):
    config_arg = str(Path(__file__).parent / "testconfig.yaml")
    cronstable.cron.Cron(config_arg)


RETRYING_JOB_THAT_FAILS2 = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="foobar", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 1
        initialDelay: 0.4
        maximumDelay: 1
        backoffMultiplier: 1
"""
)


@pytest.mark.asyncio
async def test_concurrency_and_backoff(monkeypatch, tracing_running_job):  # noqa: C901
    # This test runs against the REAL wall clock (get_now maps perf_counter
    # 1:1 onto simulated time from START_TIME), spawning two real subprocesses:
    # the @reboot job fails, then its single retry fires initialDelay (0.4s)
    # later. numjobs must reach 2 before the wait_and_quit loop stops at
    # STOP_TIME. Keep this window WIDE (2s): the retry's launch is anchored to
    # when the FIRST job's subprocess finishes failing, and Windows/CI process
    # spawn latency is both large and variable (100-300ms). A tight window
    # (the retry delay nearly filling it) leaves only tens of ms of slack, so a
    # slow spawn pushes the retry's launch past STOP_TIME and the second job is
    # never counted -- a load-sensitive flake seen on the windows-latest CI
    # runners (assert 1 == 2). Do not shrink it back.
    START_TIME = datetime.datetime(
        year=1999,
        month=12,
        day=31,
        hour=12,
        minute=0,
        second=59,
        microsecond=750000,
    )
    STOP_TIME = datetime.datetime(
        year=1999,
        month=12,
        day=31,
        hour=12,
        minute=1,
        second=1,
        microsecond=750000,
    )

    t0 = time.perf_counter()

    def get_now(timezone):
        now = START_TIME + datetime.timedelta(
            seconds=(time.perf_counter() - t0)
        )
        if timezone is not None:
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone)
            else:
                now = now.astimezone(timezone)
        return now

    def get_reltime(ts):
        return START_TIME + datetime.timedelta(seconds=(ts - t0))

    monkeypatch.setattr("cronstable.cron.get_now", get_now)

    cron = cronstable.cron.Cron(None, config_yaml=RETRYING_JOB_THAT_FAILS2)

    events = []
    numjobs = 0

    async def wait_and_quit():
        nonlocal numjobs
        known_jobs = {}
        pending_jobs = set()
        running_jobs = set()
        while get_now(None) < STOP_TIME:
            try:
                ts, event, job = await asyncio.wait_for(
                    tracing_running_job._TRACE.get(), 0.1
                )
            except asyncio.TimeoutError:
                continue
            try:
                jobnum = known_jobs[job]
            except KeyError:
                if known_jobs:
                    jobnum = max(known_jobs.values()) + 1
                else:
                    jobnum = 1
                known_jobs[job] = jobnum
                pending_jobs.add(jobnum)
                running_jobs.add(jobnum)
                numjobs += 1
            print(get_reltime(ts), event, jobnum)
            events.append((jobnum, event))
            if event in {"report_success", "report_permanent_failure"}:
                pending_jobs.discard(jobnum)
            if event in {
                "report_success",
                "report_permanent_failure",
                "cancelled",
            }:
                running_jobs.discard(jobnum)
        cron.signal_shutdown()

    await asyncio.gather(wait_and_quit(), cron.run())
    import pprint

    pprint.pprint(events)
    assert numjobs == 2


@pytest.mark.parametrize(
    "value_in, out",
    [
        (10, "in 10 seconds"),
        (305.0, "in 5 minutes"),
        (5000.0, "in 83 minutes"),
        (50000.0, "in 13 hours"),
        (500000.0, "in 5 days"),
    ],
)
def test_naturaltime(value_in, out):
    got_out = cronstable.cron.naturaltime(value_in)
    assert got_out == out


@pytest.mark.asyncio
async def test_schedule_retry_job_disappeared():
    # a job removed from config while a retry is pending must not raise
    # UnboundLocalError; the retry is simply skipped.
    cron = cronstable.cron.Cron(None)
    await cron.schedule_retry_job("nonexistent", 0.0, 0)
    assert "nonexistent" not in cron.retry_state


@pytest.mark.asyncio
async def test_schedule_retry_job_abandoned_when_no_longer_owner():
    # H1 regression: a retry can outlive the leadership it started under (a
    # partition / quorum loss moved ownership while it slept). It must re-check
    # the gate and abandon rather than relaunch -- relaunching here while the
    # new owner also runs it on its next tick is the split-brain double-run
    # the abstraction exists to prevent. Abandonment requires ANOTHER node to
    # be positively identified as the owner (a quorate, conflict-free view);
    # a transient denial defers instead (see the blip test below).
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,  # leadership moved away...
        is_quorate=lambda: True,  # ...under a trustworthy quorate view
        is_available_leader=lambda: False,  # another node positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,  # a converged view, not the settle hold
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.1, 2, 1)
    cron.retry_state["j"] = state  # a pending retry
    await cron.schedule_retry_job("j", 0.0, 0)
    # abandoned (not relaunched) and the stale retry state cleared
    assert "j" not in cron.retry_state
    assert "j" not in cron.running_jobs
    # ...and the escaped-state relaunch path is closed (see the test below)
    assert state.cancelled is True


@pytest.mark.asyncio
async def test_schedule_retry_job_survives_transient_gate_blip(monkeypatch):
    # A retry waking during a TRANSIENT fail-closed condition (lost quorum, a
    # nodeName/size/policy conflict, a backend read error) must NOT abandon
    # the chain: this node may still be the rightful owner, and for the
    # wiki's keep-alive pattern (@reboot + maximumRetries: -1 + Leader) there
    # is no next scheduled firing -- reboot_ran was recorded before the first
    # launch, so an abandonment during a one-interval blip would mean no node
    # ever restarts the process. The retry defers and re-checks the gate,
    # relaunching once the blip clears.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    quorate = False  # a one-interval quorum blip

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: quorate,  # this node leads again once quorum returns
        is_quorate=lambda: quorate,
        is_available_leader=lambda: True,  # nobody else positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,
    )
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.01, 1, 0.01)
    cron.retry_state["j"] = state
    task = asyncio.create_task(cron.schedule_retry_job("j", 0.01, 1))
    await asyncio.sleep(0.1)  # the retry wakes mid-blip and defers
    assert launched == []  # not relaunched while the gate is closed...
    assert "j" in cron.retry_state  # ...but the chain survives the blip
    assert state.cancelled is False
    quorate = True  # blip over: this node is the owner again
    await asyncio.wait_for(task, timeout=5)
    assert launched == ["j"]  # the kept retry relaunched the job


@pytest.mark.asyncio
async def test_schedule_retry_job_defers_during_unsettled_view(
    monkeypatch, caplog
):
    # Reviewer regression on the abandon/defer split: a QUORATE but not yet
    # SETTLED view (a freshly rebuilt gossip manager whose current-build
    # agreeing peers have not all re-attested its new instance_id; quorum only
    # needs a majority, the settle hold waits for every such peer) holds
    # is_available_leader() False even on the rightful owner. That hold is a
    # transient fail-closed denial, not a positive ownership move: abandoning
    # there would end an @reboot keep-alive (maximumRetries: -1) chain
    # cluster-wide, since reboot_ran was recorded before the first launch.
    # The retry must defer and relaunch once the view settles.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    settled = False  # the ~2-interval re-attestation window

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,
        is_quorate=lambda: True,  # quorate the whole time
        is_available_leader=lambda: settled,  # held closed while unsettled
        has_conflict=lambda: False,
        view_settled=lambda: settled,
    )
    # while unsettled, the gate denial must read as transient, never a move
    job = types.SimpleNamespace(name="j", clusterPolicy="PreferLeader")
    assert cron._cluster_owner_moved(job) is False
    launched = []
    monkeypatch.setattr(
        cron,
        "maybe_launch_job",
        lambda job: launched.append(job.name) or _noop(),
    )
    monkeypatch.setattr(cronstable.cron, "RETRY_GATE_RECHECK_FLOOR", 0.01)
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.01, 1, 0.01)
    cron.retry_state["j"] = state
    import logging

    with caplog.at_level(logging.DEBUG, logger="cronstable"):
        task = asyncio.create_task(cron.schedule_retry_job("j", 0.01, 1))
        await asyncio.sleep(0.1)  # the retry wakes mid-hold and defers
        assert launched == []  # not relaunched while the gates are held...
        assert "j" in cron.retry_state  # ...but the chain was NOT abandoned
        assert state.cancelled is False
        settled = True  # peers re-attested: the hold lifts, this node owns it
        await asyncio.wait_for(task, timeout=5)
    assert launched == ["j"]  # the kept retry relaunched the job
    # log cadence: the deferral is announced once at INFO; the re-checks that
    # follow (about one per second at the recheck floor) repeat only at DEBUG,
    # so a long gate-closed outage cannot spam the log.
    deferred = [r for r in caplog.records if "deferred" in r.message]
    assert len(deferred) > 1  # the loop really re-checked several times
    assert [r.levelno for r in deferred].count(logging.INFO) == 1
    assert all(r.levelno in (logging.INFO, logging.DEBUG) for r in deferred)
    # (a settled view where the gate STAYS False is the genuine move case,
    # covered by test_schedule_retry_job_abandoned_when_no_longer_owner)


@pytest.mark.asyncio
async def test_retry_abandonment_cancels_state_and_records(caplog):
    # The ownership-move abandonment must (a) set state.cancelled BEFORE
    # dropping the state: a RunningJob launched while the retry sat pending
    # (a manual API start, a concurrencyPolicy Allow overlap) captured this
    # same JobRetryState, and its own later failure would otherwise re-arm a
    # retry on the untracked state -- which cancel_job_retries can never find
    # or cancel, so the orphan would relaunch the job after a later success;
    # and (b) end the sequence loudly: a WARNING naming the actual cause plus
    # a run-history record, not one INFO line.
    import logging
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=lambda: False,
        is_quorate=lambda: True,
        is_available_leader=lambda: False,  # another node positively owns it
        has_conflict=lambda: False,
        view_settled=lambda: True,
    )
    job = types.SimpleNamespace(name="j", clusterPolicy="Leader")
    cron.cron_jobs["j"] = job
    state = JobRetryState(0.1, 2, 1)
    cron.retry_state["j"] = state
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.schedule_retry_job("j", 0.0, 1)
    assert "j" not in cron.retry_state
    assert state.cancelled is True  # kills the rogue-relaunch path
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "abandoned" in r.message
    ]
    assert len(warnings) == 1
    assert "moved ownership" in warnings[0].message
    # the sequence's end is visible in the run history / dashboard
    assert cron.last_run["j"].outcome == "cancelled"
    assert "ownership moved" in cron.last_run["j"].fail_reason
    assert [r.outcome for r in cron.run_history["j"]] == ["cancelled"]

    # rogue-relaunch closure: a concurrent RunningJob that captured this same
    # state must now end its own failing run permanently instead of re-arming
    # a retry on the untracked state.
    reported = []

    async def _report_failure():
        reported.append("failure")

    async def _report_permanent_failure():
        reported.append("permanent_failure")

    running = types.SimpleNamespace(
        config=types.SimpleNamespace(
            name="j",
            onFailure={
                "retry": {
                    "maximumRetries": -1,
                    "initialDelay": 0.1,
                    "maximumDelay": 1,
                    "backoffMultiplier": 2,
                }
            },
        ),
        stdout=None,
        stderr=None,
        retry_state=state,
        report_failure=_report_failure,
        report_permanent_failure=_report_permanent_failure,
    )
    await cron.handle_job_failure(running)
    assert reported == ["failure", "permanent_failure"]
    assert "j" not in cron.retry_state  # no orphan retry was re-armed


def test_cluster_allows_fails_closed_on_backend_error():
    # crash-safety: a backend read that raises must not escape _cluster_allows
    # (spawn_jobs runs outside the run loop's try/except, so it would kill the
    # scheduler); the gate fails closed instead.
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def boom():
        raise RuntimeError("backend bug")

    cron.cluster_manager = types.SimpleNamespace(
        distribution="single-leader",
        is_leader=boom,
        is_available_leader=boom,
        has_conflict=lambda: False,
    )
    leader = types.SimpleNamespace(clusterPolicy="Leader", name="j")
    prefer = types.SimpleNamespace(clusterPolicy="PreferLeader", name="j")
    assert cron._cluster_allows(leader) is False
    assert cron._cluster_allows(prefer) is False
    # EveryNode never touches the backend, so it still runs
    every = types.SimpleNamespace(clusterPolicy="EveryNode", name="j")
    assert cron._cluster_allows(every) is True


@pytest.mark.asyncio
async def test_run_survives_config_error(tmp_path, monkeypatch, run_cron):
    # If the reparse raises (e.g. the config became invalid on reload), run()
    # must log it and keep running the previously-loaded jobs, not crash with
    # UnboundLocalError when the housekeeping block later inspects `config`.
    # The reparse now runs off the event loop (reload_config ->
    # run_in_executor(parse_config)), so make parse_config itself fail after a
    # clean load at construction.
    cfg = tmp_path / "c.yaml"
    cfg.write_text(TWO_JOBS)
    cron = cronstable.cron.Cron(str(cfg))
    assert set(cron.cron_jobs) == {"alpha", "beta"}
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)

    def boom(*args, **kwargs):
        raise ConfigError("boom")

    # reload_config now skips the reparse when the file is unchanged on disk,
    # so touch it (a real "config edited to something invalid on reload"
    # scenario bumps mtime) to defeat the skip; the failed parse never records
    # a new fingerprint, so every subsequent tick still sees the change and
    # retries.
    cfg.write_text(TWO_JOBS + "\n# edited\n")
    monkeypatch.setattr("cronstable.cron.parse_config_with_sources", boom)

    task = run_cron(cron)
    # the reparse fails on every housekeeping tick, but the daemon must
    # stay up (no UnboundLocalError, no escape) and keep the jobs it had.
    await asyncio.sleep(0.1)
    assert not task.done()
    assert set(cron.cron_jobs) == {"alpha", "beta"}  # unchanged
    # the failed reload flips the standard "config broken on disk" signal
    # (cronstable_config_last_reload_successful) even though the parse ran
    # off the loop, in a worker thread.
    assert cron.metrics._last_reload_ok is False


def test_cluster_allows_per_policy():
    import types

    cron = cronstable.cron.Cron(None)

    def job(policy):
        return types.SimpleNamespace(clusterPolicy=policy)

    # election not configured: every policy runs here (today's behavior)
    for p in ("Leader", "PreferLeader", "EveryNode"):
        assert cron._cluster_allows(job(p)) is True

    cron._elect_leader_configured = True

    # no manager running (e.g. failed to start): EveryNode jobs are immune and
    # still run; Leader fails CLOSED so we don't risk every replica firing; but
    # PreferLeader is never-skip -- a node with no manager is the "store
    # unreachable" case its contract already accepts a double-run for, so it
    # must still run rather than drop to at-most-zero fleet-wide (F14).
    cron.cluster_manager = None
    assert cron._cluster_allows(job("EveryNode")) is True
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, leader, avail):
            self._leader, self._avail = leader, avail

        def is_leader(self):
            return self._leader

        def is_available_leader(self):
            return self._avail

        def has_conflict(self):
            return False

    # available leader but not quorum leader (e.g. a minority partition):
    # Leader skips, PreferLeader runs, EveryNode runs.
    cron.cluster_manager = _Mgr(leader=False, avail=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True
    assert cron._cluster_allows(job("EveryNode")) is True

    # the quorum leader: everything runs here
    cron.cluster_manager = _Mgr(leader=True, avail=True)
    assert cron._cluster_allows(job("Leader")) is True
    assert cron._cluster_allows(job("PreferLeader")) is True


def test_cluster_allows_spread_distribution():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    # spread mode consults per-job ownership instead of one leader:
    # is_job_owner is keyed on job name, is_available_job_owner ignores quorum.
    class _SpreadMgr:
        distribution = "spread"

        def is_job_owner(self, name):
            return name == "mine"

        def is_available_job_owner(self, name):
            return name == "mine-avail"

        def has_conflict(self):
            return False

    cron.cluster_manager = _SpreadMgr()
    # Leader: runs only on the per-job owner
    assert cron._cluster_allows(job("Leader", "mine")) is True
    assert cron._cluster_allows(job("Leader", "other")) is False
    # PreferLeader: runs on the reachable owner (no quorum gate)
    assert cron._cluster_allows(job("PreferLeader", "mine-avail")) is True
    assert cron._cluster_allows(job("PreferLeader", "other")) is False
    # EveryNode: always runs, regardless of distribution
    assert cron._cluster_allows(job("EveryNode", "other")) is True


def test_cluster_role_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, leader, quorate=None):
            self._leader = leader
            # in single-leader mode the leader is by definition quorate; a
            # follower may be quorate without leading (default to leader state)
            self._quorate = leader if quorate is None else quorate

        def is_leader(self):
            return self._leader

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records if "leadership" in r.message]
    assert msgs == [
        "cluster: this node acquired scheduled-job leadership",
        "cluster: this node lost scheduled-job leadership",
    ]


def test_cluster_quorum_logged_on_follower_single_leader(caplog):
    # C1 regression: a follower (never leader) that loses quorum must still log
    # it -- in single-leader mode only the ex-leader's is_leader() flips, so
    # without this a whole cluster dropping below quorum leaves followers
    # silent.
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Follower:
        distribution = "single-leader"

        def __init__(self, quorate):
            self._quorate = quorate

        def is_leader(self):
            return False  # this node never leads (a higher-priority node does)

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _Follower(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()  # joins quorum as a follower
        cron.cluster_manager = _Follower(False)
        cron._log_cluster_role()  # loses quorum -> must log here
    msgs = [r.message for r in caplog.records if "quorum" in r.message]
    assert msgs == [
        "cluster: this node joined quorum",
        "cluster: this node left quorum; no majority reachable, so Leader "
        "jobs cannot run until one is",
    ]
    # and a follower never logs a leadership line (it never led)
    assert not [r for r in caplog.records if "leadership" in r.message]


def test_cluster_role_logged_spread_quorum(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _SpreadMgr:
        distribution = "spread"

        def __init__(self, quorate):
            self._quorate = quorate

        def is_quorate(self):
            return self._quorate

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

    cron.cluster_manager = _SpreadMgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _SpreadMgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records if "quorum" in r.message]
    assert msgs == [
        "cluster: this node joined quorum; per-job ownership active",
        "cluster: this node left quorum; per-job ownership suspended",
    ]


# ---------------------------------------------------------------------------
# duplicate-nodeName conflict gate + @reboot deferral
# ---------------------------------------------------------------------------


def test_cluster_allows_leader_stands_down_on_conflict():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def has_conflict(self):
            return self._conflict

        def is_leader(self):
            return True

        def is_available_leader(self):
            return True

    # a duplicate nodeName fails Leader closed; PreferLeader still runs (its
    # contract already tolerates double-runs), EveryNode is unaffected.
    cron.cluster_manager = _Mgr(conflict=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True
    assert cron._cluster_allows(job("EveryNode")) is True
    # once it clears, Leader runs again
    cron.cluster_manager = _Mgr(conflict=False)
    assert cron._cluster_allows(job("Leader")) is True


def test_cluster_allows_spread_leader_stands_down_on_conflict():
    import types

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    def job(policy, name="j"):
        return types.SimpleNamespace(clusterPolicy=policy, name=name)

    class _SpreadMgr:
        distribution = "spread"

        def __init__(self, conflict):
            self._conflict = conflict

        def has_conflict(self):
            return self._conflict

        def is_job_owner(self, name):
            return True

        def is_available_job_owner(self, name):
            return True

    cron.cluster_manager = _SpreadMgr(conflict=True)
    assert cron._cluster_allows(job("Leader")) is False
    assert cron._cluster_allows(job("PreferLeader")) is True  # ungated
    cron.cluster_manager = _SpreadMgr(conflict=False)
    assert cron._cluster_allows(job("Leader")) is True


def test_cluster_conflict_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return ["dup"] if self._conflict else []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("duplicate nodeName detected" in m for m in msgs) == 1
    assert sum("conflict resolved" in m for m in msgs) == 1


def test_cluster_size_conflict_logged_on_transition(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return [5] if self._conflict else []

        def conflicting_policies(self):
            return []

        def cluster_size(self):
            return 3

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    detected = "agreeing peers declare 5 but we declare 3"
    assert sum(detected in m for m in msgs) == 1
    assert sum("cluster-size disagreement resolved" in m for m in msgs) == 1


def test_cluster_policy_conflict_logged_on_transition(caplog):
    # a coordination-policy divergence (a peer running a different distribution
    # / electLeader) stands Leader jobs down cluster-wide; it must leave a
    # breadcrumb just like a duplicate-name or size conflict, once per change.
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Mgr:
        distribution = "single-leader"

        def __init__(self, conflict):
            self._conflict = conflict

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return (
                ["distribution 'spread' != 'single-leader'"]
                if self._conflict
                else []
            )

        def is_leader(self):
            return False

        def is_quorate(self):
            return True

    cron.cluster_manager = _Mgr(True)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_cluster_role()
        cron._log_cluster_role()  # unchanged: no second log
        cron.cluster_manager = _Mgr(False)
        cron._log_cluster_role()
    msgs = [r.message for r in caplog.records]
    assert sum("coordination-policy divergence --" in m for m in msgs) == 1
    assert (
        sum("coordination-policy divergence resolved" in m for m in msgs) == 1
    )
    assert any("distribution 'spread' != 'single-leader'" in m for m in msgs)


def test_is_deferrable_reboot():
    import types

    from cronstable.cronexpr import CronTab

    cron = cronstable.cron.Cron(None)

    def job(policy, sched):
        return types.SimpleNamespace(clusterPolicy=policy, schedule=sched)

    # not deferrable until election is configured
    assert cron._is_deferrable_reboot(job("Leader", "@reboot")) is False
    cron._elect_leader_configured = True
    assert cron._is_deferrable_reboot(job("Leader", "@reboot")) is True
    assert cron._is_deferrable_reboot(job("PreferLeader", "@reboot")) is True
    # EveryNode @reboot is meant to run on every node at boot -> not deferred
    assert cron._is_deferrable_reboot(job("EveryNode", "@reboot")) is False
    # a real cron schedule (not @reboot) is never a deferrable reboot
    assert (
        cron._is_deferrable_reboot(job("Leader", CronTab("* * * * *")))
        is False
    )


@pytest.fixture
def reboot_cron(monkeypatch):
    """The deferred-@reboot prologue shared by the test_deferred_reboot_*
    family (finding B8): a Cron with leader election configured and its
    launch seam captured into a list.

    ``make()`` returns ``(cron, launched)``.  ``election=False`` models the
    election-removed-on-reload branch; ``launch="maybe_launch_job"``
    captures the manual-start seam the web-trigger tests drive instead;
    ``record="job"`` appends the launched JobConfig itself where a test
    must tell a stale snapshot from the current config; ``web=True`` also
    enables the web surface for direct handler calls (finding B6).
    """

    def make(
        *,
        election=True,
        launch="launch_scheduled_job",
        record="name",
        web=False,
    ):
        cron = cronstable.cron.Cron(None)
        cron._elect_leader_configured = election
        if web:
            cron.web_config = {}
        launched = []

        def capture(job):
            launched.append(job.name if record == "name" else job)
            return _noop()

        monkeypatch.setattr(cron, launch, capture)
        return cron, launched

    return make


def _pend_reboot(cron, job=None, *, present=True):
    """Register a deferred @reboot one-shot under the name "boot" (the B8
    prologue's tail): held pending, and present in cron_jobs unless the
    test models a name absent from the currently-loaded config."""
    if job is None:
        job = _reboot_job()
    if present:
        cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    return job


# The single-pass rows of the deferred-@reboot gate (finding B8): build the
# pending one-shot, run one _process_pending_reboots pass, then check what
# launched, whether the pending entry survived, and (where the case pins
# it) whether the once-per-boot token was recorded on the manager.
# mgr=None is the no-manager state (election configured but the backend
# never started, or election removed on reload).  Multi-step siblings
# (pause deferral, record-before-launch ordering, ownership handoff, name
# reuse, transient absence) remain their own tests below.
@pytest.mark.parametrize(
    "election, job_kwargs, mgr_kwargs, expect_launched, expect_pending,"
    " expect_ran",
    [
        # the owner runs it and retires it; running it records + advertises
        # the run, so peers won't re-run it
        pytest.param(
            True,
            {},
            {"leader": "node-a"},
            ["boot"],
            False,
            True,
            id="runs_on_owner",
        ),
        # A deferred @reboot Leader/PreferLeader job DISABLED via a reload
        # while it sat pending must be retired without running, even on the
        # elected owner -- the same way job_should_run and the manual web
        # trigger refuse a disabled job. Otherwise an operator-disabled
        # init/migration one-shot still runs once cluster-wide on
        # convergence.
        pytest.param(
            True,
            {"enabled": False},
            {"leader": "node-a"},
            [],
            False,
            None,
            id="disabled_on_owner_is_not_run",
        ),
        # The never-skip mgr-is-None PreferLeader branch must also refuse a
        # job disabled on reload (it otherwise runs every such one-shot
        # here).
        pytest.param(
            True,
            {"policy": "PreferLeader", "enabled": False},
            None,
            [],
            False,
            None,
            id="disabled_no_manager_preferleader",
        ),
        # The election-removed branch (no longer gated) must also refuse a
        # disabled job rather than running it once on the way out.
        pytest.param(
            False,
            {"enabled": False},
            None,
            [],
            False,
            None,
            id="disabled_after_election_removed",
        ),
        # the gossip-ack: once the cluster reports the job already ran, a
        # node retires it WITHOUT running -- even if this node is now the
        # elected owner. This is what stops a re-run when leadership lands
        # on a node that still held the one-shot pending.
        pytest.param(
            True,
            {},
            {"leader": "node-a", "ran": ("boot",)},
            [],
            False,
            None,
            id="retired_on_ack_without_rerun",
        ),
        # #8: a non-owner must NOT drop the one-shot just because some other
        # node currently looks like the owner -- that node may itself be
        # unable to run it (reachable from us but not quorate from its own
        # view), and dropping would lose the boot job forever; we keep
        # waiting instead.
        pytest.param(
            True,
            {},
            {"leader": "node-b"},
            [],
            True,
            None,
            id="kept_when_other_owns",
        ),
        # #9: a PreferLeader @reboot must run even with no quorum (its
        # contract is to never skip while a node is up). The gate
        # (_cluster_allows) uses the quorum-free is_available_leader(),
        # true on an isolated/minority node.
        pytest.param(
            True,
            {"policy": "PreferLeader"},
            {"leader": None, "available": "node-a"},
            ["boot"],
            False,
            None,
            id="preferleader_runs_without_quorum",
        ),
        # H1 regression: election configured but the backend never started
        # (store unreachable / bad creds -> cluster_manager is None). A
        # deferred PreferLeader @reboot must STILL run here -- its contract
        # is never-skip, exactly the store-unreachable case it exists to
        # survive. Previously the mgr-is-None branch returned early for ALL
        # jobs, dropping it forever.
        pytest.param(
            True,
            {"policy": "PreferLeader"},
            None,
            ["boot"],
            False,
            None,
            id="preferleader_runs_when_no_manager",
        ),
        # H1 (cont.): a Leader @reboot in the SAME no-manager state must
        # NOT run -- it stays fail-closed and pending, re-evaluated once a
        # manager comes up. Asymmetric with PreferLeader above, mirroring
        # _cluster_allows.
        pytest.param(
            True,
            {"policy": "Leader"},
            None,
            [],
            True,
            None,
            id="leader_pending_no_manager",
        ),
        # the quorum-free availability owner can still be another node (a
        # lower name we mutually agree with); that node runs it, so we keep
        # waiting.
        pytest.param(
            True,
            {"policy": "PreferLeader"},
            {"leader": None, "available": "node-b"},
            [],
            True,
            None,
            id="preferleader_waits_for_available_owner",
        ),
        # no quorum yet: keep waiting
        pytest.param(
            True,
            {},
            {"leader": None},
            [],
            True,
            None,
            id="waits_without_quorum",
        ),
        # owner is undecided during a conflict even though leader_name() is
        # us: keep waiting
        pytest.param(
            True,
            {},
            {"leader": "node-a", "conflict": True},
            [],
            True,
            None,
            id="waits_on_conflict",
        ),
        # election removed on a reload: nothing gates these anymore -> run
        # here (and the pending set fully drains)
        pytest.param(
            False,
            {},
            None,
            ["boot"],
            False,
            None,
            id="runs_when_election_disabled",
        ),
    ],
)
@pytest.mark.asyncio
async def test_deferred_reboot_gate(
    reboot_cron,
    election,
    job_kwargs,
    mgr_kwargs,
    expect_launched,
    expect_pending,
    expect_ran,
):
    cron, launched = reboot_cron(election=election)
    _pend_reboot(cron, _reboot_job(**job_kwargs))
    mgr = _reboot_mgr(**mgr_kwargs) if mgr_kwargs is not None else None
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert launched == expect_launched
    assert ("boot" in cron._pending_reboot_jobs) is expect_pending
    if expect_ran is not None:
        assert mgr.reboot_ran("boot") is expect_ran


@pytest.mark.asyncio
async def test_deferred_reboot_paused_owner_keeps_it_pending(reboot_cron):
    # A pause defers a deferred @reboot one-shot's boot run instead of
    # forfeiting it: the cluster's once-per-boot token must not be spent on
    # a run the launcher's pause gate would only skip.
    cron, launched = reboot_cron()
    _pend_reboot(cron)
    cron._paused["boot"] = cronstable.cron.PauseInfo(
        since=datetime.datetime.now(UTC),
        until=datetime.datetime.now(UTC) + datetime.timedelta(hours=1),
        note="",
        by="op",
        channel="api",
    )
    mgr = _reboot_mgr(leader="node-a")  # we are the owner
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # still owed
    assert mgr.reboot_ran("boot") is False  # token not burnt
    # the pause lifts -> the boot run happens, exactly once
    cron._paused.pop("boot")
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs
    assert mgr.reboot_ran("boot") is True


@pytest.mark.asyncio
async def test_deferred_reboot_records_before_launch(reboot_cron, monkeypatch):
    # At-most-once crash safety: the deferred-@reboot owner MUST record
    # intent-to-run (mark_reboot_ran, which eagerly gossips/persists) BEFORE
    # spawning the job. A crash in a launch->record window would leave no
    # peer/store aware it ran, so a failover owner would re-run a Leader
    # one-shot (a double-run). Pin the RELATIVE ORDER, not just the end state,
    # so swapping the two production lines (launch then record) fails here.
    # This test interleaves TWO seams into one events list, so it re-patches
    # the launch seam over the fixture's list-append.
    cron, _ = reboot_cron()
    events = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda job: events.append("launch") or _noop(),
    )
    mgr = _reboot_mgr(leader="node-a")  # we are the owner
    orig_mark = mgr.mark_reboot_ran

    async def _recording_mark(name):
        events.append("record")
        await orig_mark(name)

    mgr.mark_reboot_ran = _recording_mark
    _pend_reboot(cron)
    cron.cluster_manager = mgr
    await cron._process_pending_reboots()
    assert events == ["record", "launch"]


@pytest.mark.asyncio
async def test_deferred_reboot_leader_runs_when_identity_differs(reboot_cron):
    # H3 regression: a lease backend reports leader_name() as the holder's
    # display *identity* (e.g. cluster.kubernetes.identity), which may
    # legitimately differ from node_name. The deferred-@reboot gate must
    # self-recognise the holder via the is_leader() boolean, NOT by comparing
    # that identity string to node_name -- otherwise a one-shot Leader @reboot
    # job never runs on ANY node (the holder's identity != its node_name, and
    # every follower's leader_name() is that identity too).
    cron, launched = reboot_cron()

    class _LeaseMgr:
        node_name = "pod-a"
        distribution = "single-leader"

        def has_conflict(self):
            return False

        def is_leader(self):
            return True  # this node holds the lease

        def leader_name(self):
            return "my-app"  # display identity, != node_name -- the trap

        def reboot_ran(self, name):
            return False

        async def mark_reboot_ran(self, name):
            pass

    _pend_reboot(cron)
    cron.cluster_manager = _LeaseMgr()
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_leader_runs_after_owner_lands_here(reboot_cron):
    # #8 (continued from the kept_when_other_owns row): because we kept
    # waiting instead of dropping, the one-shot still runs when leadership
    # later lands on this node -- so a deferred boot job is never silently
    # lost.
    cron, launched = reboot_cron()
    _pend_reboot(cron)
    cron.cluster_manager = _reboot_mgr(leader="node-b")  # not us yet
    await cron._process_pending_reboots()
    assert launched == [] and "boot" in cron._pending_reboot_jobs
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # now we are leader
    await cron._process_pending_reboots()
    assert launched == ["boot"] and "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_kept_when_absent_election_disabled(reboot_cron):
    # #4 (election-disabled path): the same never-lose rule holds when election
    # was turned off on a reload -- a momentarily-absent name is kept pending,
    # not popped, and runs the current job once the name returns.
    cron, launched = reboot_cron(election=False, record="job")
    stale = _pend_reboot(cron, present=False)  # absent right now
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # kept, not lost
    # name returns -> runs the CURRENT job (not the stale snapshot)
    current = _reboot_job()
    cron.cron_jobs["boot"] = current
    await cron._process_pending_reboots()
    assert launched == [current]
    assert launched[0] is not stale
    assert not cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_election_disabled_skips_non_reboot_reuse(
    reboot_cron,
):
    # #4 (election-off path): if a deferred name was reused for a non-@reboot
    # job by the time election is turned off, the stale one-shot is retired
    # WITHOUT running here -- the reused job schedules itself normally. Only a
    # name still mapping to an @reboot job runs on the election-off drain path,
    # mirroring the gated path's _is_deferrable_reboot retirement.
    import types

    cron, launched = reboot_cron(election=False)
    _pend_reboot(cron, present=False)  # stale @reboot one-shot
    # the name now maps to a normally-scheduled job (reused)
    cron.cron_jobs["boot"] = types.SimpleNamespace(
        name="boot", clusterPolicy="Leader", schedule="0 * * * *"
    )
    await cron._process_pending_reboots()
    assert launched == []  # the reused non-@reboot job is not run here
    assert "boot" not in cron._pending_reboot_jobs  # stale entry retired


@pytest.mark.asyncio
async def test_deferred_reboot_kept_on_transient_absence(reboot_cron):
    # #4: @reboot only defers at startup, so if a name momentarily vanishes
    # from cron_jobs mid-reload (templating glitch, transient remove-then-
    # re-add) before the cluster converges, it must NOT be dropped -- dropping
    # would lose the one-shot forever and break the never-lose property. It
    # stays pending while absent and runs once the name returns and we own it.
    cron, launched = reboot_cron()
    # the name is transiently absent from cron_jobs (cron.cron_jobs is empty)
    job = _pend_reboot(cron, present=False)
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    await cron._process_pending_reboots()
    assert launched == []  # did not run while absent...
    assert "boot" in cron._pending_reboot_jobs  # ...and was NOT dropped
    # the name comes back on a later reload; now it runs (we are the owner)
    cron.cron_jobs["boot"] = job
    await cron._process_pending_reboots()
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_absent_job_never_runs(reboot_cron):
    # a deliberately-removed @reboot job that never returns must never run,
    # even though we keep it pending: the launch is gated on presence.
    cron, launched = reboot_cron()
    _pend_reboot(cron, present=False)  # pending, but absent from config
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    for _ in range(3):
        await cron._process_pending_reboots()
    assert launched == []  # removed-and-gone -> never runs


@pytest.mark.asyncio
async def test_deferred_reboot_runs_current_config_on_name_reuse(reboot_cron):
    # #4 name-reuse edge: if a name is removed and later re-added for a
    # DIFFERENT @reboot job, the owner runs the CURRENT cron_jobs[name], never
    # the stale JobConfig captured at boot.
    cron, launched = reboot_cron(record="job")
    stale = _reboot_job()  # captured at startup, then the name was reused
    fresh = _reboot_job()  # a different object with the same name
    _pend_reboot(cron, stale, present=False)
    cron.cron_jobs["boot"] = fresh
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we are the owner
    await cron._process_pending_reboots()
    assert launched == [fresh]  # the live config, not the stale captured one
    assert launched[0] is not stale
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_deferred_reboot_retired_when_name_reused_non_deferrable(
    reboot_cron,
):
    # #4 name-reuse edge: if a name is reused for a job that is no longer a
    # deferrable @reboot (e.g. EveryNode, or a real schedule), the stale
    # pending entry is retired WITHOUT running through the owner path -- the
    # new job is left to its own scheduling.
    import types

    cron, launched = reboot_cron()
    _pend_reboot(cron, present=False)  # stale @reboot Leader
    # the name now belongs to an EveryNode @reboot job (not deferrable)
    cron.cron_jobs["boot"] = types.SimpleNamespace(
        name="boot", clusterPolicy="EveryNode", schedule="@reboot"
    )
    cron.cluster_manager = _reboot_mgr(leader="node-a")  # we would own it
    await cron._process_pending_reboots()
    assert launched == []  # the owner path did not run the reused name
    assert "boot" not in cron._pending_reboot_jobs  # stale entry retired


@pytest.mark.asyncio
async def test_spawn_jobs_defers_reboot_leader_at_startup(reboot_cron):
    config = parse_config_string(
        "jobs:\n  - name: boot\n    command: echo hi\n"
        '    schedule: "@reboot"\n    clusterPolicy: Leader\n',
        "",
    )
    cron, launched = reboot_cron()
    cron.cron_jobs = OrderedDict((j.name, j) for j in config.jobs)

    class _Mgr:
        node_name = "node-a"
        distribution = "single-leader"

        def conflict_names(self):
            return []

        def conflicting_sizes(self):
            return []

        def conflicting_policies(self):
            return []

        def is_leader(self):
            return False

        def is_quorate(self):
            return False  # no quorum at the startup instant

        def has_conflict(self):
            return False

        def leader_name(self):
            return None  # no quorum at the startup instant

        def reboot_ran(self, name):
            return False

    cron.cluster_manager = _Mgr()
    await cron.spawn_jobs(startup=True)
    assert launched == []  # deferred, not run at boot
    assert "boot" in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_retires_pending_and_marks_ran(
    reboot_cron,
):
    # The Run button (POST /jobs/{name}/start) used to launch a job still
    # pending as a deferred @reboot one-shot WITHOUT retiring the pending
    # entry or recording the run, so once the cluster converged
    # _process_pending_reboots saw reboot_ran(name) False and ran the
    # one-shot a second time -- possibly on another node, since the manual
    # run was never gossiped/persisted. A manual start IS the boot run: it
    # must retire the entry and mark it ran on the manager.
    cron, launched = reboot_cron(launch="maybe_launch_job", web=True)
    _pend_reboot(cron)
    # not converged yet (no quorum): exactly the window in which the
    # dashboard shows the job as pending and an operator clicks Run.
    mgr = _reboot_mgr(leader=None)
    cron.cluster_manager = mgr

    resp = await cron._web_start_job(Req(match={"name": "boot"}))
    assert resp.status == 200
    assert launched == ["boot"]  # the manual run happened
    assert "boot" not in cron._pending_reboot_jobs  # retired locally...
    assert mgr.reboot_ran("boot") is True  # ...and recorded cluster-wide
    # convergence later must not re-run the one-shot: nothing is pending here
    # and a peer still holding it pending stands down on the recorded run.
    await cron._process_pending_reboots()
    assert launched == ["boot"]


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_without_manager(reboot_cron):
    # the same manual start with no manager running (backend failed to start)
    # must still retire the pending entry -- the local re-run protection --
    # and launch, without tripping on the absent manager.
    cron, launched = reboot_cron(launch="maybe_launch_job", web=True)
    cron.cluster_manager = None
    _pend_reboot(cron)

    resp = await cron._web_start_job(Req(match={"name": "boot"}))
    assert resp.status == 200
    import json as _json_mod

    # the MCP cron_run_job ack shape (was an empty 200)
    assert _json_mod.loads(resp.text) == {"started": "boot"}
    assert launched == ["boot"]
    assert "boot" not in cron._pending_reboot_jobs


@pytest.mark.asyncio
async def test_web_start_deferred_reboot_concurrent_requests(reboot_cron):
    # Reviewer race: two concurrent POST /jobs/{name}/start for the SAME
    # still-pending @reboot name can both pass the pending check before the
    # awaited mark_reboot_ran yields (the gossip push awaits peers). The
    # loser must not 500 on a KeyError retiring an entry the winner already
    # retired: the entry is retired exactly once, and BOTH manual starts
    # still launch -- exactly as two manual starts of any other job would.
    cron, launched = reboot_cron(launch="maybe_launch_job", web=True)
    _pend_reboot(cron)
    mgr = _reboot_mgr(leader=None)
    orig_mark = mgr.mark_reboot_ran

    async def _slow_mark(name):
        # model the real gossip push: the record awaits peers, yielding to
        # the event loop while the pending entry is still present
        await asyncio.sleep(0.01)
        await orig_mark(name)

    mgr.mark_reboot_ran = _slow_mark
    cron.cluster_manager = mgr

    req = Req(match={"name": "boot"})
    r1, r2 = await asyncio.gather(
        cron._web_start_job(req), cron._web_start_job(req)
    )
    assert (r1.status, r2.status) == (200, 200)  # the loser must not 500
    assert launched == ["boot", "boot"]  # both operator actions ran the job
    assert "boot" not in cron._pending_reboot_jobs  # retired exactly once
    assert mgr.reboot_ran("boot") is True  # ...and recorded cluster-wide


@pytest.mark.asyncio
async def test_cluster_start_survives_bad_cert_files(caplog):
    # #6: a missing/unreadable cert file is an operational misconfiguration --
    # start_stop_cluster must log it and keep running (no manager), NOT let the
    # exception escape to the run loop's generic "please report this as a bug"
    # handler. ClusterManager is constructed inside the try for exactly this.
    import logging

    config = parse_config_string(_TLS_CLUSTER_YAML, "")
    cron = cronstable.cron.Cron(None)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron.start_stop_cluster(config.cluster_config)  # must not raise
    assert cron.cluster_manager is None
    # election intent is tracked regardless, so the Leader gate fails closed
    assert cron._elect_leader_configured is True
    assert any("failed to start" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cluster_restarts_on_in_place_cert_rotation(caplog):
    # an in-place cert rotation leaves the config bytes identical, so the
    # restart-on-config-change check alone never fires; the manager must also
    # restart on the TLS-file-change signal -- but only once the new material
    # is actually loadable (#6), so a half-written cert mid-rotation cannot
    # wedge it. Loadable case: the rotation restart proceeds and the old
    # manager is stopped.
    import logging

    cfg = parse_config_string(_TLS_CLUSTER_YAML, "").cluster_config

    class _FakeMgr:
        def __init__(self, config):
            self.config = config
            self.stopped = False

        def tls_files_changed(self):
            return True

        def tls_files_loadable(self):
            return True  # new material loads cleanly -> proceed with restart

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    # same config object -> the config-change branch is skipped; only the
    # TLS-change signal can trigger the restart.
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    assert fake.stopped is True
    assert any(
        "TLS certificate files changed" in r.message for r in caplog.records
    )
    # reconstruction uses the (here deliberately bad) cert paths and fails
    # closed, so no new manager replaces the stopped one.
    assert cron.cluster_manager is None


@pytest.mark.asyncio
async def test_cluster_cert_rotation_keeps_manager_when_unloadable(caplog):
    # #6: a half-written / briefly-absent cert observed mid-rotation must NOT
    # tear the manager down. The rotation signal fires (tls_files_changed) but
    # the new material is not yet loadable, so the running manager is kept
    # (still serving the valid old cert) and we retry next reload -- Leader /
    # PreferLeader stay up the whole time instead of failing closed for ~1
    # reload while the rebuild fails on the same bad files.
    import logging

    cfg = parse_config_string(_TLS_CLUSTER_YAML, "").cluster_config

    class _FakeMgr:
        def __init__(self, config):
            self.config = config
            self.stopped = False

        def tls_files_changed(self):
            return True

        def tls_files_loadable(self):
            return False  # half-written rotation: cannot load yet

        def set_node_stats_provider(self, provider, share=True):
            # the kept-manager path re-reconciles the share flag every reload
            self.node_stats_share = share

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None)
    fake = _FakeMgr(cfg)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    # the old manager is kept and was never stopped or replaced
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert any("not yet loadable" in r.message for r in caplog.records)


def _config_change_yamls():
    # a DIFFERENT peer set -> cluster_config != mgr.config -> config change
    yaml_b = _TLS_CLUSTER_YAML.replace("host: c:8443", "host: d:8443")
    return (
        parse_config_string(_TLS_CLUSTER_YAML, "").cluster_config,
        parse_config_string(yaml_b, "").cluster_config,
    )


class _ConfigChangeFakeMgr:
    def __init__(self, config):
        self.config = config
        self.stopped = False

    def tls_files_changed(self):
        return False  # config changed; the TLS-rotation path is moot here

    def tls_files_loadable(self):  # pragma: no cover - not reached
        return False

    def set_node_stats_provider(self, provider, share=True):
        # the kept-manager path re-reconciles the share flag every reload
        self.node_stats_share = share

    async def stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_cluster_config_change_keeps_manager_when_new_tls_unloadable(
    caplog,
):
    import logging

    # RELOAD-TLS-COMBINED: a genuine config change (different peer set) that
    # coincides with an in-flight cert rotation (new TLS material not yet
    # loadable) must NOT tear the old manager down and then fail to rebuild --
    # which would wedge Leader/PreferLeader closed for up to a reload. The
    # pre-teardown dry-run keeps the running manager (still serving the valid
    # old cert) and retries next reload. The certs here are absent (the
    # mid-rotation case), so gossip_tls_loadable(cfg_b) is False.
    cfg_a, cfg_b = _config_change_yamls()
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg_b)
    assert fake.stopped is False  # kept, not torn down
    assert cron.cluster_manager is fake
    assert any("not yet loadable" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cluster_config_change_tears_down_when_new_tls_loadable(
    monkeypatch,
):
    # the dry-run gate is specific to UNLOADABLE new TLS: when the new config's
    # TLS loads cleanly, a config change still tears the old manager down (the
    # operator changed config; the old manager no longer applies), and
    # reconstruction then fails closed on the (here deliberately absent) certs.
    cfg_a, cfg_b = _config_change_yamls()
    monkeypatch.setattr(
        "cronstable.cluster.gossip_tls_loadable", lambda cfg: True
    )
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_a)
    cron.cluster_manager = fake
    await cron.start_stop_cluster(cfg_b)
    assert fake.stopped is True  # config change tears down
    assert cron.cluster_manager is None  # reconstruction fails closed


def _observability_toggle_yamls():
    yaml_on = (
        _TLS_CLUSTER_YAML + "  observability:\n    shareNodeStats: true\n"
    )
    return (
        parse_config_string(_TLS_CLUSTER_YAML, "").cluster_config,
        parse_config_string(yaml_on, "").cluster_config,
    )


@pytest.mark.asyncio
async def test_cluster_observability_only_change_keeps_manager_reconciles():
    # An observability-only edit (shareNodeStats toggled; the election
    # section untouched) must NOT restart the election manager -- on a lease
    # backend that would drop the leadership lease and pause Leader jobs
    # fleet-wide for an election-inert change. Instead the kept manager's
    # LATCHED share flag is re-reconciled to the new config every reload, so
    # the toggle actually reaches the running gossip mesh.
    cfg_off, cfg_on = _observability_toggle_yamls()
    cron = cronstable.cron.Cron(None)
    fake = _ConfigChangeFakeMgr(cfg_off)
    cron.cluster_manager = fake
    # toggle ON: manager kept, share flag reconciled to True
    await cron.start_stop_cluster(cfg_on)
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert fake.node_stats_share is True
    # toggle back OFF: still kept, flag reconciled to False
    await cron.start_stop_cluster(cfg_off)
    assert fake.stopped is False
    assert cron.cluster_manager is fake
    assert fake.node_stats_share is False
    # (a genuine election-relevant change still restarting is covered by
    # test_cluster_config_change_tears_down_when_new_tls_loadable)


# ---------------------------------------------------------------------------
# Daemon lifecycle: config hot-reload, graceful shutdown drain, real signal.
#
# These drive the actual run() loop end-to-end. Reload through run() (vs the
# existing tests all use config_arg=None, so the job set never changes) and
# the retry-drain-on-shutdown path were untested; a regression in either breaks
# headline daemon behavior silently.
# ---------------------------------------------------------------------------


# schedules fire at midnight; the suite's clock is fixed at noon, so these jobs
# never actually spawn -- the test exercises reload, not execution.
_RELOAD_V1 = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "0 0 * * *"
  - name: beta
    command: echo beta
    schedule: "0 0 * * *"
"""

_RELOAD_V2 = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "0 0 * * *"
  - name: gamma
    command: echo gamma
    schedule: "0 0 * * *"
"""


@pytest.mark.asyncio
async def test_run_reloads_changed_config(tmp_path, monkeypatch, run_cron):
    # tiny sleep so the reload loop iterates quickly instead of waiting out the
    # real ~60s to the next minute boundary.
    # accept the subminute flag arg the loop now passes to next_sleep_interval
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.02)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_V1)

    cron = cronstable.cron.Cron(str(cfg))
    assert set(cron.cron_jobs) == {"alpha", "beta"}
    id1 = cron.job_set_id()

    task = run_cron(cron)
    # let the loop load v1 at least once, then change the file on disk
    await _wait_until(lambda: cron._logged_job_set_id is not None)
    cfg.write_text(_RELOAD_V2)
    # the running daemon must pick up the new job set on its own
    await _wait_until(lambda: set(cron.cron_jobs) == {"alpha", "gamma"})
    # stop here (not in teardown) so the post-shutdown state is what's pinned
    cron.signal_shutdown()
    await asyncio.wait_for(task, timeout=5)

    assert set(cron.cron_jobs) == {"alpha", "gamma"}
    assert cron.job_set_id() != id1


_RETRY_DRAIN_JOB = (
    "jobs:\n  - name: test\n"
    + yaml_command(cmd_print(out="x", code=2))
    + """
    schedule: "@reboot"
    onFailure:
      retry:
        maximumRetries: 5
        initialDelay: 30
        maximumDelay: 30
        backoffMultiplier: 1
"""
)


@pytest.mark.asyncio
async def test_run_drains_pending_retry_on_shutdown(run_cron):
    # the @reboot job fails at once and schedules a retry with a long delay,
    # so a pending (sleeping) retry task sits in retry_state when we shut down.
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_DRAIN_JOB)

    task = run_cron(cron)
    await _wait_until(lambda: bool(cron.retry_state))
    # stop here (not in teardown): the shutdown drain is what's under test
    cron.signal_shutdown()
    await asyncio.wait_for(task, timeout=5)

    # graceful shutdown must cancel and drain the pending retry, not orphan a
    # task or leave retry_state populated.
    assert cron.retry_state == {}


_GATED_CLUSTER_BAD_WEB_TOKEN = """
jobs:
  - name: gated
    command: echo hi
    schedule: "0 0 * * *"
    clusterPolicy: Leader
web:
  listen:
    - http://127.0.0.1:0
  authToken:
    fromEnvVar: CRONSTABLE_TEST_MISSING_TOKEN
cluster:
  listen: "127.0.0.1:18443"
  tls:
    ca: /nonexistent/ca.pem
    cert: /nonexistent/cert.pem
    key: /nonexistent/key.pem
  peers:
    - host: b:8443
    - host: c:8443
  electLeader: true
"""


@pytest.mark.asyncio
async def test_web_config_error_does_not_disengage_cluster_gate(
    tmp_path, monkeypatch, caplog, run_cron
):
    # start_stop_web_app and start_stop_cluster used to share one try/except
    # ConfigError, web first: a web misconfiguration raising ConfigError (an
    # authToken resolving empty -- a deploy forgetting the env var) skipped
    # start_stop_cluster on EVERY iteration, left _elect_leader_configured
    # False, and ran every Leader job ungated on every node -- the gate
    # failed OPEN on an unrelated web error. The cluster gate must engage
    # (fail CLOSED) regardless of the web app's fate, and the daemon must
    # keep running with the web API down.
    import logging

    monkeypatch.delenv("CRONSTABLE_TEST_MISSING_TOKEN", raising=False)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_GATED_CLUSTER_BAD_WEB_TOKEN)
    cron = cronstable.cron.Cron(str(cfg))

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        task = run_cron(cron)
        await _wait_until(lambda: cron._elect_leader_configured)
        assert not task.done()  # the daemon keeps running
        # stop inside the caplog window, exactly as the try/finally did
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert cron.web_runner is None  # the web API stayed down (fail closed)
    assert any("web.authToken" in r.message for r in caplog.records)
    # the manager itself failed to start (bad certs) but the gate still
    # engaged, so the Leader job fails CLOSED instead of running everywhere
    assert cron.cluster_manager is None
    assert cron._cluster_allows(cron.cron_jobs["gated"]) is False


@pytest.mark.asyncio
async def test_shutdown_stops_cluster_manager_before_job_drain():
    # run() used to stop the cluster manager only AFTER awaiting all running
    # jobs, so a draining leader kept its gossip liveness / lease renewal
    # alive for the whole (unbounded) drain and every Leader job cluster-wide
    # stalled until the slowest local job finished. Leadership must be
    # released after retries are cancelled but BEFORE the drain, so failover
    # proceeds while the jobs finish.
    cron = cronstable.cron.Cron(
        None, config_yaml=CONCURRENT_JOB.format(policy="Allow")
    )
    events = []

    class _Mgr:
        async def stop(self):
            running = cron.running_jobs.get("test") or []
            alive = any(
                rj.proc is not None and rj.proc.returncode is None
                for rj in running
            )
            events.append(("cluster-stopped", alive))
            # leadership released; now let the drain finish by terminating
            # the still-running job (marked cancelled so the reaper records
            # a deliberate cancellation, not a failure).
            for rj in running:
                rj.cancelled = True
                await rj.cancel()

    cron.cluster_manager = _Mgr()
    await cron.maybe_launch_job(cron.cron_jobs["test"])
    assert cron.running_jobs["test"][0].proc.returncode is None
    # stop before the loop's first iteration: run() goes straight to the
    # shutdown sequence with a job still running and a manager installed.
    cron.signal_shutdown()
    await asyncio.wait_for(cron.run(), timeout=10)
    # the manager was stopped while the job was still draining...
    assert events == [("cluster-stopped", True)]
    assert cron.cluster_manager is None
    assert not cron.running_jobs  # ...and the drain then completed


@pytest.mark.asyncio
async def test_shutdown_closes_the_pooled_webhook_connections():
    # WebhookReporter keeps one connection pool per loop so reports stop
    # paying a connect and a TLS handshake each. Nothing reclaims that pool
    # on its own (aiohttp's connector holds the loop it was built on, so the
    # weak key never expires), which leaves the shutdown sequence to close
    # it, for the same reason it closes the pooled statsd endpoints beside
    # it: the sockets are otherwise released only when the loop is
    # collected, and aiohttp logs "Unclosed connector" on the way out. It
    # goes last, after _drain_completions has sent the final reports.
    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    loop = asyncio.get_running_loop()
    pooled = cronstable.job._webhook_connector()
    assert cronstable.job._WEBHOOK_CONNECTORS[loop] is pooled
    cron.signal_shutdown()
    await asyncio.wait_for(cron.run(), timeout=10)
    assert pooled.closed
    assert loop not in cronstable.job._WEBHOOK_CONNECTORS


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="POSIX signal delivery (SIGTERM)"
)
def test_sigterm_triggers_graceful_shutdown():
    # End-to-end of the systemd/`docker stop` path: a real SIGTERM, routed
    # through the installed handler, must drive run() to a clean return. Uses a
    # dedicated loop (like the platform handler roundtrip test) so the handler
    # owns the signal and it does not reach the default disposition.
    loop = asyncio.new_event_loop()
    try:
        cron = cronstable.cron.Cron(
            None
        )  # no jobs: run() idles until signalled
        remove = platform.install_shutdown_handlers(loop, cron.signal_shutdown)
        try:
            loop.call_later(0.05, lambda: os.kill(os.getpid(), signal.SIGTERM))
            loop.run_until_complete(asyncio.wait_for(cron.run(), timeout=5))
            assert cron._stop_event.is_set()
        finally:
            remove()
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_fleet_job_summaries_snapshot():
    # the compact per-job snapshot gossiped to peers for the fleet view:
    # lean fixed-shape entries only -- notably no fail_reason (arbitrary
    # operator text) and no command line, which stay on this node's own API.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    out = JobOutputStream()
    out.close()
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="failure",
        exit_code=3,
        started_at=DT(1999, 12, 31, 11, 59, 58, tzinfo=UTC),
        finished_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        fail_reason="boom",
        output=out,
    )
    summaries = cron.fleet_job_summaries()
    assert set(summaries) == {"alpha", "beta"}
    alpha = summaries["alpha"]
    assert alpha["running"] is False
    assert alpha["enabled"] is True
    assert isinstance(alpha["scheduled_in"], float)
    assert alpha["last"] == {
        "outcome": "failure",
        "finished_at": "1999-12-31T12:00:00+00:00",
        "duration": 2.0,
        "exit_code": 3,
    }
    assert "fail_reason" not in alpha["last"]
    # beta is disabled (and an @reboot one-shot): no next fire, no last run
    beta = summaries["beta"]
    assert beta == {
        "running": False,
        "enabled": False,
        "scheduled_in": None,
        "last": None,
    }
    # a running instance flips the flag and suppresses the next-fire estimate
    cron.running_jobs["alpha"] = ["sentinel"]
    alpha = cron.fleet_job_summaries()["alpha"]
    assert alpha["running"] is True
    assert alpha["scheduled_in"] is None


# ==========================================================================
# The cron.py start/stop lifecycle and
# durable-state garbage-collection paths.  Targets start_stop_web_app,
# start_stop_cluster, start_stop_observability, start_stop_state, the job-API
# seams (_start_job_api / _stop_job_api), _persist_manifest, _live_pause_keep,
# and the three GC helpers (_collect_state_garbage / _gc_dag_state /
# _sweep_orphan_artifact_blobs).  Most of these are degrade-and-survive
# branches reached by driving a lifecycle transition or by monkeypatching a
# backend method to raise, then asserting the observable side effect.
# ==========================================================================

class _LifecycleFakeSite:
    """A web site whose start() never binds a real socket (isolation)."""

    def __init__(self, url):
        self.url = url

    async def start(self):
        return None


def _lifecycle_state_config(tmp_path):
    return _state_cfg("state:\n  path: " + str(tmp_path))


@pytest.fixture
async def state_cron(tmp_path):
    """A Cron on the shared one-job YAML with a started filesystem state
    layer, stopped on teardown (finding B1: the former
    _lifecycle_start_state/_lifecycle_stop_state try/finally idiom, 21
    sites).  The guard keeps the teardown a no-op for a test that already
    tore its own state layer down."""
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_lifecycle_state_config(tmp_path))
    assert cron.state_backend is not None
    yield cron
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


@pytest.fixture
async def start_web_app():
    """Start a cron's web app with teardown guaranteed (finding B1), the
    local twin of tests/test_cron_web.py's fixture.  Every consumer here
    fakes web_site_from_url, so no real socket exists and no Proactor
    close grace is needed on teardown."""
    crons = []

    async def start(cron, config, *mcp):
        crons.append(cron)
        await cron.start_stop_web_app(config, *mcp)

    yield start
    for cron in reversed(crons):
        await cron.start_stop_web_app(None)


# The GC-anchor seeder these tests share, _seed_gc_anchor, lives in the
# merged hardening section below (the former _lifecycle_seed_anchor_frozen
# collapsed onto it: same two manifests, and only recent manifests count
# for the scope-coverage guard, so decorating the stale one was inert).


# --- start_stop_web_app ----------------------------------------------------


async def test_lifecycle_web_app_wildcard_acao_and_socket_mode(
    monkeypatch, caplog, start_web_app
):
    # A wildcard Access-Control-Allow-Origin header disables the cross-site
    # Origin gate (loudly), and socketMode drives the post-listen apply hook.
    # web_site_from_url is faked so no real socket is bound.
    import logging

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await start_web_app(
            cron,
            {
                "listen": ["http://127.0.0.1:1"],
                "headers": {"Access-Control-Allow-Origin": "*"},
                "socketMode": "0660",
            },
        )
    assert cron.web_runner is not None
    assert any(
        "Access-Control-Allow-Origin" in r.message
        for r in caplog.records
    )
    # clearing the config stops the server (teardown re-clear is a no-op)
    await cron.start_stop_web_app(None)
    assert cron.web_runner is None


async def test_lifecycle_web_app_specific_acao_folded_into_allowlist(
    monkeypatch, start_web_app
):
    # A specific (non-wildcard) ACAO response header is folded into the
    # cross-site allow-list so a deliberate cross-origin dashboard survives.
    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:1"],
            "headers": {
                "Access-Control-Allow-Origin": "https://dash.example"
            },
        },
    )
    assert cron.web_runner is not None


async def test_lifecycle_web_app_mounts_mcp_endpoint(
    monkeypatch, start_web_app
):
    # An enabled MCP config wires the POST/GET/OPTIONS /mcp routes and builds
    # the handler against the current config.
    from cronstable.config import _build_mcp_config

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: _LifecycleFakeSite(url),
    )
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    mcp_config = _build_mcp_config({"enabled": True})
    await start_web_app(
        cron, {"listen": ["http://127.0.0.1:1"]}, mcp_config
    )
    assert cron.web_runner is not None
    assert cron._mcp is not None


# --- start_stop_cluster ----------------------------------------------------


async def test_lifecycle_cluster_build_installs_providers_and_warns(
    monkeypatch, caplog
):
    # A fresh cluster build installs both fleet providers and starts the
    # manager, and an even cluster size emits a (once-per-(re)start) advisory.
    import logging

    from cronstable.config import parse_config_string

    # a third peer makes the cluster size even (this node + 3), which is
    # what drives the advisory below
    yaml = _TLS_CLUSTER_YAML.replace(
        "    - host: c:8443\n",
        "    - host: c:8443\n    - host: d:8443\n",
    )
    cfg = parse_config_string(yaml, "").cluster_config
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda c, jsid: made.append(_FakeMesh(c)) or made[-1],
    )
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_cluster(cfg)
    assert cron.cluster_manager is made[0]
    assert made[0].started is True
    assert made[0].job_summaries_provider == cron.fleet_job_summaries
    assert made[0].node_stats_provider == cron.node_resource_snapshot
    assert cron._elect_leader_configured is True
    assert any("even cluster size" in r.message for r in caplog.records)


async def test_lifecycle_cluster_reload_logs_leader_and_quorum_loss(caplog):
    # Removing the cluster section stops the running manager; if this node
    # held leadership/quorum, the ex-leader logs the transition here (before
    # the flags reset) rather than going silent about why it stopped.
    import logging

    class _Mgr:
        def __init__(self):
            self.config = {"backend": "gossip"}
            self.stopped = False

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    mgr = _Mgr()
    cron.cluster_manager = mgr
    cron._was_leader = True
    cron._was_quorate = True
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron.start_stop_cluster(None)
    assert mgr.stopped is True
    assert cron.cluster_manager is None
    assert cron._was_leader is False and cron._was_quorate is False
    assert any(
        "lost scheduled-job leadership" in r.message for r in caplog.records
    )
    assert any("left quorum" in r.message for r in caplog.records)


# --- start_stop_observability ----------------------------------------------


async def test_lifecycle_observability_keeps_mesh_when_new_tls_unloadable(
    monkeypatch, caplog
):
    # A TLS-file rotation signals a rebuild, but make-before-break is
    # infeasible for gossip: while the new material is not yet loadable the
    # running overlay is kept (serving the valid old cert) and the share flag
    # is still re-reconciled on it.
    import logging

    monkeypatch.setattr(
        "cronstable.cluster.gossip_tls_loadable", lambda cfg: False
    )
    mesh_cfg = {"backend": "gossip", "marker": 7}

    class _Mesh:
        def __init__(self, config):
            self.config = config
            self.stopped = False
            self.share = None

        def tls_files_changed(self):
            return True

        def set_node_stats_provider(self, provider, share=True):
            self.share = share

        async def stop(self):
            self.stopped = True

    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    mesh = _Mesh(mesh_cfg)
    cron.observability_mesh = mesh
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron.start_stop_observability(
            {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
        )
    assert cron.observability_mesh is mesh
    assert mesh.stopped is False
    assert mesh.share is True
    assert any("not yet loadable" in r.message for r in caplog.records)


async def test_lifecycle_observability_start_failure_swallowed(
    monkeypatch, caplog
):
    # A misconfigured overlay whose start() raises must be logged and
    # swallowed: durability/observability being broken never stops jobs.
    import logging

    class _FailMesh:
        def __init__(self, config):
            self.config = config

        def set_job_summaries_provider(self, p):
            pass

        def set_node_stats_provider(self, p, share=True):
            pass

        async def start(self):
            raise OSError("overlay bind failed")

    monkeypatch.setattr(
        cronstable.cron, "make_backend", lambda c, jsid: _FailMesh(c)
    )
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron.start_stop_observability(
            {
                "observabilityMesh": {"backend": "gossip"},
                "shareNodeStats": False,
            }
        )
    assert cron.observability_mesh is None
    assert any("failed to start" in r.message for r in caplog.records)


# --- start_stop_state teardown ---------------------------------------------


async def test_lifecycle_state_teardown_cancels_slot_and_retry_tasks(
    state_cron,
):
    # Removing the state section tears the backend down and cancels every
    # per-store background task (slot renewers, Replace pursuits, the
    # cross-node retry-claim scan): they belong to the old store generation.
    cron = state_cron

    async def _idle():
        await asyncio.sleep(3600)

    renewer = asyncio.ensure_future(_idle())
    pursuit = asyncio.ensure_future(_idle())
    claim = asyncio.ensure_future(_idle())
    cron._slot_renewers["j"] = renewer
    cron._slot_pursuits["j"] = pursuit
    cron._retry_claim_task = claim
    await asyncio.sleep(0)  # let the tasks reach their await points

    await cron.start_stop_state(None)

    assert cron.state_backend is None
    assert cron._slot_renewers == {}
    assert cron._slot_pursuits == {}
    assert cron._retry_claim_task is None
    await asyncio.sleep(0)
    assert renewer.cancelled()
    assert pursuit.cancelled()
    assert claim.cancelled()


# --- _start_job_api / _stop_job_api ----------------------------------------


async def test_lifecycle_start_job_api_swallows_start_failure(
    monkeypatch, tmp_path, caplog
):
    # The loopback job-state API is best-effort: a start failure is logged and
    # swallowed (jobs run without the endpoint), leaving _job_api unset.
    import logging

    import cronstable.jobapi

    class _FailApi:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            raise OSError("loopback bind failed")

        async def stop(self):
            return None

    monkeypatch.setattr(cronstable.jobapi, "JobStateAPI", _FailApi)
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._start_job_api(_lifecycle_state_config(tmp_path))
    assert cron._job_api is None
    assert any("job API failed to start" in r.message for r in caplog.records)


async def test_lifecycle_stop_job_api_noop_when_absent():
    # No API running: stop is a clean no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    assert cron._job_api is None
    await cron._stop_job_api()
    assert cron._job_api is None


async def test_lifecycle_stop_job_api_warns_on_unclean_stop(caplog):
    # A stop that raises is logged as an unclean shutdown, and the handle is
    # cleared regardless so the generation cannot leak.
    import logging

    class _Api:
        async def stop(self):
            raise OSError("did not close")

    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    cron._job_api = _Api()
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._stop_job_api()
    assert cron._job_api is None
    assert any("did not stop cleanly" in r.message for r in caplog.records)


# --- _persist_manifest -----------------------------------------------------


async def test_lifecycle_persist_manifest_noop_without_backend():
    # No backend: the manifest write is a no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    assert cron.state_backend is None
    await cron._persist_manifest()


async def test_lifecycle_persist_manifest_swallows_append_error(
    state_cron, caplog
):
    # A failed manifest append is counted as a dropped write and logged, not
    # raised (it runs as a fire-and-forget background task).
    import logging

    cron = state_cron

    async def _boom(*a, **k):
        raise OSError("append failed")

    cron.state_backend.append_record = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._persist_manifest()
    assert any(
        "failed to record the job manifest" in r.message
        for r in caplog.records
    )


# --- _live_pause_keep ------------------------------------------------------


async def test_lifecycle_live_pause_keep_keeps_all_on_enumerate_error(
    state_cron, caplog
):
    # If the pause-stream listing cannot be enumerated, every kept job is kept
    # unconditionally: GC never eats a live pause on doubt.
    import logging

    cron = state_cron

    async def _boom(*a, **k):
        raise OSError("cannot list")

    cron.state_backend.list_stream_names = _boom
    now = cronstable.cron.get_now(datetime.timezone.utc)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        keep = await cron._live_pause_keep(
            cron.state_backend, {"j"}, now
        )
    assert keep == {"j"}
    assert any(
        "not reclaiming dead pause streams" in r.message
        for r in caplog.records
    )


async def test_lifecycle_live_pause_keep_skips_removed_and_unreadable_streams(
    state_cron, caplog
):
    # A pause stream for a job not in the keep set is skipped (its name was
    # already collected), and a kept job's unreadable pause stream is kept on
    # doubt rather than dropped.
    import logging

    cron = state_cron
    backend = cron.state_backend
    await backend.append_record(
        cronstable.cron.PAUSE_STREAM_PREFIX + "removed",
        {"until": "2099-01-01T00:00:00+00:00"},
    )
    await backend.append_record(
        cronstable.cron.PAUSE_STREAM_PREFIX + "keeper",
        {"until": "2099-01-01T00:00:00+00:00"},
    )

    async def _boom(*a, **k):
        raise OSError("cannot read")

    backend.list_records = _boom
    now = cronstable.cron.get_now(datetime.timezone.utc)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        keep = await cron._live_pause_keep(backend, {"keeper"}, now)
    assert "keeper" in keep
    assert any(
        "keeping the pause stream of keeper" in r.message
        for r in caplog.records
    )


# --- _collect_state_garbage ------------------------------------------------


async def test_lifecycle_collect_garbage_noop_without_grace():
    # gcGraceSeconds unset (0) or no backend: the whole pass is a no-op.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    cron._state_gc_grace = 0.0
    await cron._collect_state_garbage()


async def test_lifecycle_collect_garbage_degrades_on_enumerate_error(
    state_cron, caplog
):
    # Cannot enumerate the manifest streams: collect nothing this pass.
    import logging

    cron = state_cron
    cron._state_gc_grace = 3600.0

    async def _boom(*a, **k):
        raise OSError("cannot enumerate")

    cron.state_backend.list_stream_names = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._collect_state_garbage()
    assert any(
        "cannot enumerate the manifest streams" in r.message
        for r in caplog.records
    )


async def test_lifecycle_collect_garbage_caps_manifest_hosts(
    state_cron, monkeypatch, caplog
):
    # More manifest host streams than the cap: warn and read only the first
    # cap-many this pass (a churning fleet with never-reused host identities).
    import logging

    cron = state_cron
    await _seed_gc_anchor(cron)
    monkeypatch.setattr(cronstable.cron, "MANIFEST_HOSTS_CAP", 1)
    cron._state_gc_grace = 3600.0
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._collect_state_garbage()
    assert any(
        "reading only the first" in r.message for r in caplog.records
    )


async def test_lifecycle_collect_garbage_degrades_on_manifest_read_error(
    state_cron, caplog
):
    # The streams enumerate but a record read fails: collect nothing.
    import logging

    cron = state_cron
    await _seed_gc_anchor(cron)
    cron._state_gc_grace = 3600.0

    async def _boom(*a, **k):
        raise OSError("cannot read")

    cron.state_backend.list_records = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._collect_state_garbage()
    assert any(
        "cannot read the manifest streams" in r.message
        for r in caplog.records
    )


async def test_lifecycle_collect_garbage_degrades_on_collect_failure(
    state_cron, caplog
):
    # The pass reaches the backend collect step (history spans grace, scopes
    # advertised) and that step raises: degrade to "collected nothing".
    import logging

    cron = state_cron
    await _seed_gc_anchor(cron)
    cron._state_gc_grace = 3600.0

    async def _boom(*a, **k):
        raise OSError("collect failed")

    cron.state_backend.collect_garbage = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._collect_state_garbage()
    assert any(
        "garbage collection failed" in r.message for r in caplog.records
    )


# --- _gc_dag_state ---------------------------------------------------------


async def test_lifecycle_gc_dag_state_degrades_on_namespace_error(
    state_cron, caplog
):
    # Cannot enumerate the dag-run namespaces: leave artifact streams wholly
    # unmanaged this pass (the keep map is untouched).
    import logging

    cron = state_cron
    backend = cron.state_backend

    async def _boom(*a, **k):
        raise OSError("cannot enumerate namespaces")

    backend.list_document_namespaces = _boom
    keep = {}
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._gc_dag_state(backend, keep, set(), set(), 3600.0)
    assert "artifacts/" not in keep
    assert any(
        "cannot enumerate the dag-run namespaces" in r.message
        for r in caplog.records
    )


async def test_lifecycle_gc_dag_state_defers_when_namespace_incomplete(
    state_cron, caplog
):
    # A dag-run namespace exists whose name cannot be recovered: its runs'
    # XCom scopes cannot be protected, so artifacts stay unmanaged this pass.
    import logging

    cron = state_cron
    backend = cron.state_backend

    async def _incomplete(*a, **k):
        return ([], False)

    backend.list_document_namespaces = _incomplete
    keep = {}
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._gc_dag_state(backend, keep, set(), set(), 3600.0)
    assert "artifacts/" not in keep
    assert any("cannot be recovered" in r.message for r in caplog.records)


async def test_lifecycle_gc_dag_state_degrades_on_document_read_error(
    state_cron, caplog
):
    # The namespaces enumerate but a run document read fails: unmanaged.
    import logging

    from cronstable.dag import DAG_RUN_NS_PREFIX

    cron = state_cron
    backend = cron.state_backend

    async def _ns(*a, **k):
        return ([DAG_RUN_NS_PREFIX + "d"], True)

    async def _boom(*a, **k):
        raise OSError("cannot read documents")

    backend.list_document_namespaces = _ns
    backend.list_documents = _boom
    keep = {}
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        # live_dags carries "d" so gc_removed_dags is not invoked and the
        # test targets only the document-read degrade branch.
        await cron._gc_dag_state(backend, keep, set(), {"d"}, 3600.0)
    assert "artifacts/" not in keep
    assert any(
        "cannot read the dag-run documents" in r.message
        for r in caplog.records
    )


# --- _sweep_orphan_artifact_blobs ------------------------------------------


async def test_lifecycle_sweep_blobs_degrades_on_audit_error(
    state_cron, caplog
):
    # Cannot enumerate the artifact streams: skip the sweep this pass.
    import logging

    cron = state_cron
    backend = cron.state_backend

    async def _boom(*a, **k):
        raise OSError("cannot audit")

    backend.list_stream_names_audit = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._sweep_orphan_artifact_blobs(backend, 3600.0)
    assert any(
        "cannot enumerate" in r.message and "artifact streams" in r.message
        for r in caplog.records
    )


async def test_lifecycle_sweep_blobs_degrades_on_sweep_error(
    state_cron, caplog
):
    # The reference set builds but the blob sweep itself raises: skip, biased
    # to keep, so a live payload is never deleted on doubt.
    import logging

    cron = state_cron
    backend = cron.state_backend

    async def _boom(*a, **k):
        raise OSError("cannot sweep")

    backend.sweep_orphan_blobs = _boom
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._sweep_orphan_artifact_blobs(backend, 3600.0)
    assert any(
        "skipping the orphan-blob sweep" in r.message
        and "cannot be ruled" in r.message
        for r in caplog.records
    )


# --- cancellation must propagate (never swallowed as a degrade) ------------
#
# Every GC/state degrade block re-raises asyncio.CancelledError ahead of its
# broad "log and survive" except, so a shutdown cancel is honoured rather than
# mistaken for a store error.  These drive that re-raise on each block.


async def _lifecycle_cancel(*a, **k):
    raise asyncio.CancelledError()


async def test_lifecycle_live_pause_keep_propagates_enumerate_cancel(
    state_cron,
):
    cron = state_cron
    cron.state_backend.list_stream_names = _lifecycle_cancel
    now = cronstable.cron.get_now(datetime.timezone.utc)
    with pytest.raises(asyncio.CancelledError):
        await cron._live_pause_keep(cron.state_backend, {"j"}, now)


async def test_lifecycle_live_pause_keep_propagates_read_cancel(state_cron):
    cron = state_cron
    backend = cron.state_backend
    await backend.append_record(
        cronstable.cron.PAUSE_STREAM_PREFIX + "keeper",
        {"until": "2099-01-01T00:00:00+00:00"},
    )
    backend.list_records = _lifecycle_cancel
    now = cronstable.cron.get_now(datetime.timezone.utc)
    with pytest.raises(asyncio.CancelledError):
        await cron._live_pause_keep(backend, {"keeper"}, now)


async def test_lifecycle_collect_garbage_propagates_enumerate_cancel(
    state_cron,
):
    cron = state_cron
    cron._state_gc_grace = 3600.0
    cron.state_backend.list_stream_names = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._collect_state_garbage()


async def test_lifecycle_collect_garbage_propagates_read_cancel(state_cron):
    cron = state_cron
    await _seed_gc_anchor(cron)
    cron._state_gc_grace = 3600.0
    cron.state_backend.list_records = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._collect_state_garbage()


async def test_lifecycle_gc_dag_state_propagates_namespace_cancel(state_cron):
    cron = state_cron
    cron.state_backend.list_document_namespaces = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._gc_dag_state(
            cron.state_backend, {}, set(), set(), 3600.0
        )


async def test_lifecycle_gc_dag_state_propagates_document_cancel(state_cron):
    from cronstable.dag import DAG_RUN_NS_PREFIX

    cron = state_cron
    backend = cron.state_backend

    async def _ns(*a, **k):
        return ([DAG_RUN_NS_PREFIX + "d"], True)

    backend.list_document_namespaces = _ns
    backend.list_documents = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._gc_dag_state(backend, {}, set(), {"d"}, 3600.0)


async def test_lifecycle_sweep_blobs_propagates_audit_cancel(state_cron):
    cron = state_cron
    cron.state_backend.list_stream_names_audit = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._sweep_orphan_artifact_blobs(
            cron.state_backend, 3600.0
        )


async def test_lifecycle_sweep_blobs_propagates_sweep_cancel(state_cron):
    cron = state_cron
    cron.state_backend.sweep_orphan_blobs = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._sweep_orphan_artifact_blobs(
            cron.state_backend, 3600.0
        )


async def test_lifecycle_collect_garbage_propagates_collect_cancel(state_cron):
    # Reach the backend collect step (history spans grace, scopes advertised)
    # and cancel there: the re-raise must win over the broad degrade except.
    cron = state_cron
    await _seed_gc_anchor(cron)
    cron._state_gc_grace = 3600.0
    cron.state_backend.collect_garbage = _lifecycle_cancel
    with pytest.raises(asyncio.CancelledError):
        await cron._collect_state_garbage()


# ========================================================================
# Hardened durable-state plumbing in cron.py
# (formerly tests/test_cron_state_hardening.py; merged per finding B18,
# rationale below kept verbatim from that module's docstring.  Its local
# _ONE_JOB/_DEP_JOB copies collapsed onto tests/_configs, and
# _seed_gc_anchor now dates its manifests via get_now so it works under
# the autouse frozen clock; the GC sections above share it.)
# ========================================================================
# Regression tests for the hardened durable-state plumbing in cron.py.
#
# Each test pins a bug confirmed by the adversarial review of the
# scheduler-side state integration (:mod:`cronstable.cron`); if one of those
# fixes regresses, the matching test here must fail.  Covered:
#
# * store errors and hung reads must degrade the stateful features, never
#   crash or stall the scheduling paths that call them;
# * foreign/naive ledger records must not poison schedule arithmetic;
# * the depends-on-past gate's freshness rules (memory vs ledger);
# * the catch-up latch (retry vs forfeit) and checkpointed resume;
# * backfill serialization under ``concurrencyPolicy: Forbid`` and the
#   live retry-ladder capture;
# * output-archival edge cases and rehydration races.


_FORBID_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    concurrencyPolicy: Forbid\n"
    "    onMissed: run-all\n"
)


def _state_yaml(path):
    return "state:\n  path: " + str(path)


async def _dep_cron(tmp_path):
    cron = Cron(None, config_yaml=_DEP_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


def _mem_run(outcome, minute):
    """A finished-run entry as _record_run would put it in memory."""
    dt = datetime.datetime(2026, 7, 1, 10, minute, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=JobOutputStream(),
    )


async def _put_ledger(cron, outcome, iso, name="j"):
    await cron.state_backend.append_record(
        cron._run_stream(name),
        {
            "outcome": outcome,
            "exit_code": 0,
            "started_at": None,
            "finished_at": iso,
            "duration": None,
            "fail_reason": None,
        },
    )


async def _raise_oserror(*args, **kwargs):
    raise OSError("state store went away")


async def _seed_gc_anchor(cron, covered=True):
    """Manifests letting a GC pass with grace 3600 prove absence.

    One manifest older than the grace (history-depth guard) plus one recent
    one; ``covered`` controls whether the recent manifest advertises its
    scopes/dags (all-new-fleet) or predates them (mid-rolling-upgrade).

    Timestamps ride ``get_now`` (which _collect_state_garbage judges
    manifest ages against), so the anchor also holds under this module's
    autouse frozen clock; on a real clock it is the wall time it always was.
    """
    now = cron_mod.get_now(datetime.timezone.utc)
    backend = cron.state_backend
    await backend.append_record(
        "manifests/old-host",
        {
            "jobSetId": "v1:old",
            "host": "old-host",
            "jobs": [],
            "at": (now - datetime.timedelta(seconds=7200)).isoformat(),
        },
    )
    recent = {
        "jobSetId": "v1:other",
        "host": "other-host",
        "jobs": [],
        "at": now.isoformat(),
    }
    if covered:
        recent["scopes"] = []
        recent["dags"] = []
    await backend.append_record("manifests/other-host", recent)


# --- scheduler-crash containment on store errors --------------------------


async def test_depends_on_past_fails_open_on_store_error(tmp_path):
    # The CRITICAL from the review: an OSError out of the ledger read
    # used to escape _depends_on_past_ok, and the launch path runs
    # outside run()'s try/except -- a flaky mount took the scheduler
    # down.  It must degrade to the in-memory view (empty here, so
    # allow) instead of raising.
    cron = await _dep_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror  # type: ignore[method-assign]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_catch_up_defers_on_store_error(tmp_path):
    # A store error during the catch-up pass used to either crash the
    # pass or latch _caught_up, silently forfeiting the owed backfill.
    # It must defer: no exception, no latch (so a later pass retries),
    # nothing scheduled, and the job left unresolved.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron.state_backend.list_records = _raise_oserror  # type: ignore[method-assign]
    await cron._catch_up(_NOW)  # must not raise
    assert cron._caught_up is False
    assert cron._catchup_tasks == set()
    assert "j" not in cron._catchup_done


async def test_catch_up_survives_hung_store_read(tmp_path, monkeypatch):
    # A hung mount (dead NFS server) is worse than an error: without a
    # bound on the read, _catch_up would block the scheduler loop
    # indefinitely.  The watermark read is capped by STATE_OP_TIMEOUT
    # and the timeout defers the evaluation like any store error.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def hang(stream, field):
        await asyncio.sleep(999)

    cron.state_backend.derive_max = hang  # type: ignore[method-assign]
    monkeypatch.setattr(cron_mod, "STATE_OP_TIMEOUT", 0.2)
    # generous outer bound: on regression (no per-read timeout) this
    # fails the test instead of hanging the suite for the full sleep.
    await asyncio.wait_for(cron._catch_up(_NOW), timeout=20)
    assert cron._caught_up is False
    assert cron._catchup_tasks == set()


# --- naive-watermark poison record -----------------------------------------


async def test_missed_occurrences_pins_naive_watermark(tmp_path):
    # A foreign/hand-written record with a NAIVE finished_at used to
    # raise TypeError out of the schedule arithmetic on every boot -- a
    # crash loop until the record was deleted by hand.  The parser pins
    # it to UTC, so the count comes out as for the aware equivalent.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00", onmissed="run-all"
    )
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 10  # slots 10:01..10:10, as with an aware watermark


# --- depends-on-past gate ---------------------------------------------------


async def test_depends_on_past_blocks_while_still_running(tmp_path):
    # An unfinished previous instance has not "succeeded", and letting
    # the answer depend on whether it happens to finish before the gate
    # is read would make the gate a race: a running instance must close
    # it outright, even over a ledger that says success.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.running_jobs["j"].append(object())  # a live RunningJob stand-in
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_memory_beats_stale_ledger(tmp_path):
    # The durable write behind _record_run is fire-and-forget, so the
    # ledger can be a beat stale: a failure already recorded in memory
    # but not yet flushed must still close the gate, or the job re-runs
    # right behind its own failure.  Appended straight to run_history
    # (the in-memory effect of _record_run) so the ledger STAYS stale.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.run_history["j"].append(_mem_run("failure", 5))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_newer_ledger_beats_memory(tmp_path):
    # The other direction: on a shared mount another node's NEWER
    # success must re-open the gate over this node's older in-memory
    # failure, or the job stays blocked on every node but the one that
    # saw the success.
    cron = await _dep_cron(tmp_path)
    cron.run_history["j"].append(_mem_run("failure", 0))
    await _put_ledger(cron, "success", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_skips_non_run_outcomes(tmp_path):
    # cancelled entries are not verdicts on the job: both sources must
    # skip them when finding the last real run, or a newest "cancelled"
    # record would mask the decisive success/failure beneath it.
    cron = await _dep_cron(tmp_path)
    cron.run_history["j"].append(_mem_run("failure", 0))
    cron.run_history["j"].append(_mem_run("cancelled", 10))
    await _put_ledger(cron, "success", "2026-07-01T10:05:00+00:00")
    await _put_ledger(cron, "cancelled", "2026-07-01T10:12:00+00:00")
    # last REAL run is the ledger's 10:05 success (memory's real run is
    # the older 10:00 failure); the two cancelled entries are noise.
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_survives_a_pause_flooding_the_ring():
    # A pause writes one synthetic "skipped" row per held slot, and
    # run_history is a bounded ring: a pause longer than the ring evicts the
    # failure that closed the gate. Without the eviction-proof memo the gate
    # would find no real outcome, fall through to "nothing to depend on", and
    # run the job against exactly the unrepaired state onlyIfLastSucceeded
    # exists to protect. No backend here, so the ring is the only source.
    cron = Cron(None, config_yaml=_DEP_JOB)
    cron._record_run("j", _mem_run("failure", 0))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False

    paused_at = datetime.datetime(2026, 7, 1, 11, 0, 0, tzinfo=_UTC)
    for _ in range(cron_mod.RUN_HISTORY_LIMIT + 5):
        cron._record_run(
            "j",
            JobRunInfo(
                outcome="skipped",
                exit_code=None,
                started_at=None,
                finished_at=paused_at,
                fail_reason=None,
                output=JobOutputStream(),
                skip_reason="paused",
            ),
        )
    assert not [
        info
        for info in cron.run_history["j"]
        if info.outcome in ("success", "failure")
    ]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False

    # and a genuine success after the pause still reopens the gate
    cron._record_run("j", _mem_run("success", 30))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_widens_past_non_run_probe_page(tmp_path):
    # More than a probe page of cancelled records sits at the head of the
    # ledger, above the decisive failure. The gate probes a small page first;
    # a probe-only read would see only cancels and wrongly ALLOW. It must widen
    # to the full window, find the buried failure, and block.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "failure", "2026-07-01T10:00:00+00:00")
    for i in range(cron_mod.DEPENDS_GATE_PROBE + 3):
        await _put_ledger(
            cron, "cancelled", "2026-07-01T10:{:02d}:00+00:00".format(10 + i)
        )
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_probe_page_suffices_without_widening(tmp_path):
    # Common case: the newest ledger record is a real outcome, so the gate
    # reads a single probe page and never widens to the full 50-record window.
    cron = await _dep_cron(tmp_path)
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    await _put_ledger(cron, "failure", "2026-07-01T10:05:00+00:00")
    calls = []
    real = cron.state_backend.list_records

    async def counting(stream, **kw):
        calls.append(kw.get("limit"))
        return await real(stream, **kw)

    cron.state_backend.list_records = counting  # type: ignore[method-assign]
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False
    assert calls == [cron_mod.DEPENDS_GATE_PROBE]  # one read, no widening


# --- catch-up latch fixes ---------------------------------------------------


async def test_catch_up_retries_when_backend_not_started(tmp_path, caplog):
    # `state` IS configured but the backend failed to start (bad mount
    # at boot; start_stop_state retries it every housekeeping pass).
    # Latching here forfeited the backfill forever, and warning "needs
    # a state backend" was wrong -- one is configured.
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    cron._state_configured = True
    assert cron.state_backend is None
    await cron._catch_up(_NOW)
    assert cron._caught_up is False  # stays pending, retried later
    assert cron._catchup_next_retry > 0.0  # a recheck was scheduled
    assert not any("needs a" in r.getMessage() for r in caplog.records)


async def test_catch_up_warns_and_latches_without_state_config(caplog):
    # No `state` section at all: there is no watermark and never will
    # be, so catch-up warns and latches.  This is the only correct
    # latch-on-unresolved case; retrying would just warn forever.
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    await cron._catch_up(_NOW)
    assert cron._caught_up is True
    assert any(
        "needs a" in r.getMessage() and "state" in r.getMessage()
        for r in caplog.records
    )


async def test_catch_up_retries_transient_cluster_denial(tmp_path):
    # A fail-closed cluster denial with NO positive owner elsewhere
    # (still electing at boot, lost quorum) is transient: latching, or
    # marking the job done, would mean nobody ever backfills it.  The
    # job must stay unresolved and be re-evaluated.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    cron._cluster_owner_moved = lambda job: False  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    assert calls == []
    assert cron._caught_up is False
    assert "j" not in cron._catchup_done
    # ownership resolves to this node: the retry pass must schedule the
    # backfill (force the recheck gate open; the 30s interval itself is
    # not under test).
    cron._cluster_allows = lambda job: True  # type: ignore[method-assign]
    cron._catchup_next_retry = 0.0
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j"]
    assert cron._caught_up is True


async def test_catch_up_resolves_when_owner_is_elsewhere(tmp_path):
    # A POSITIVE observation that another node owns the job is final:
    # that owner reads the same ledger and does the backfill itself, so
    # this node resolves the job without launching -- and with every
    # job resolved the whole evaluation may latch.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    cron._cluster_owner_moved = lambda job: True  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    assert calls == []
    assert cron._caught_up is True


# --- catch-up checkpoint (intent) resume ------------------------------------


async def test_open_checkpoint_anchors_watermark_until_closed(tmp_path):
    # A backfill records an "open" intent before launching.  Ordinary
    # runs finishing afterwards advance the run ledger's derived
    # watermark past the still-missing slots, so without the checkpoint
    # a restart mid-backfill would silently forfeit the owed runs.
    t0 = "2026-07-01T10:00:00+00:00"
    cron = await _cron_with_watermark(tmp_path, t0, onmissed="run-all")
    await cron.state_backend.append_record(
        cron._catchup_stream("j"),
        {"kind": "open", "watermark": t0, "at": _NOW.isoformat()},
    )
    # an ordinary run lands, advancing the ledger past the missed slots
    await _put_ledger(cron, "success", "2026-07-01T10:10:00+00:00")
    job = cron.cron_jobs["j"]
    count, watermark = await cron._missed_occurrences(job, _NOW)
    assert watermark == t0  # anchored at the open intent, not the ledger
    assert count == 10
    # once the cycle closes, the (newer) run-ledger watermark rules
    # again and nothing is owed.
    await cron.state_backend.append_record(
        cron._catchup_stream("j"),
        {"kind": "close", "watermark": t0, "at": _NOW.isoformat()},
    )
    count, watermark = await cron._missed_occurrences(job, _NOW)
    assert count == 0
    assert watermark == "2026-07-01T10:10:00+00:00"


# --- backfill serialization + Forbid ----------------------------------------


async def test_run_catch_up_serializes_forbid_backfill(tmp_path):
    # run-all under concurrencyPolicy: Forbid used to fire its launches
    # back to back: the first instance was still running, so Forbid
    # swallowed the other N-1 and the "replayed" runs never happened.
    # The backfill must drain the previous instance before each launch.
    cron = Cron(None, config_yaml=_FORBID_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    now = datetime.datetime(2026, 7, 1, 10, 3, 30, tzinfo=_UTC)  # 3 owed
    launched = []
    swallowed = []

    async def fake_launch(job, *, with_retries=True):
        # mirror the real Forbid gate: a still-running instance swallows
        # the launch, which is exactly the regression this guards.
        if cron.running_jobs.get(job.name):
            swallowed.append(job.name)
            return False
        launched.append(job.name)
        marker = object()
        cron.running_jobs[job.name].append(marker)
        # the "run" lasts a few event-loop ticks, then finishes; purely
        # event-based, no duration is ever asserted.
        asyncio.get_running_loop().call_later(
            0.05, lambda: cron.running_jobs[job.name].remove(marker)
        )
        return True

    cron.maybe_launch_job = fake_launch  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, now)
    assert launched == ["j", "j", "j"]
    assert swallowed == []


# --- backfill must not capture the live retry ladder ------------------------


async def test_backfill_does_not_capture_live_retry_ladder(monkeypatch):
    # A backfill launching while a scheduled fire's retry ladder is
    # armed used to hand that live JobRetryState to its RunningJob: the
    # backfill's failures then burned the scheduled run's retry budget
    # toward a premature onPermanentFailure.  with_retries=False must
    # launch bare; the default path still carries the armed state.
    cron = Cron(None, config_yaml=_ONE_JOB)
    job = cron.cron_jobs["j"]
    armed = JobRetryState(1.0, 2.0, 10.0)
    cron.retry_state["j"] = armed
    captured = []

    class FakeRunningJob:
        def __init__(self, config, retry_state, **kwargs):
            captured.append(retry_state)
            self.config = config

        async def start(self):
            return None

    monkeypatch.setattr(cron_mod, "RunningJob", FakeRunningJob)
    assert await cron.maybe_launch_job(job, with_retries=False) is True
    assert captured == [None]
    cron.running_jobs.clear()  # a fresh, idle launch for the default
    assert await cron.maybe_launch_job(job) is True
    assert captured[1] is armed


# --- archival ---------------------------------------------------------------


def _archive_yaml(save_limit=None, redact=True):
    lines = [
        "jobs:",
        "  - name: j",
        "    command: 'true'",
        "    schedule: '* * * * *'",
        "    archiveOutput: true",
        "    redactArchivedSecrets: " + ("true" if redact else "false"),
    ]
    if save_limit is not None:
        lines.append("    saveLimit: " + str(save_limit))
    return "\n".join(lines) + "\n"


async def _archive_cron(tmp_path, **kw):
    cron = Cron(None, config_yaml=_archive_yaml(**kw))
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


def _output_run(pairs, limit=None):
    out = JobOutputStream() if limit is None else JobOutputStream(limit)
    for stream_name, line in pairs:
        out.publish(stream_name, line)
    dt = datetime.datetime(2026, 7, 1, 10, 0, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=out,
    )


async def test_archive_save_limit_zero_writes_nothing(tmp_path):
    # saveLimit: 0 is the operator's explicit "retain no output"; the
    # archive must honour it rather than persist the live-tail ring the
    # web UI keeps anyway.
    cron = await _archive_cron(tmp_path, save_limit=0)
    info = _output_run([("stdout", "must never be stored")])
    await cron._archive_output(
        cron.cron_jobs["j"], info, list(info.output.lines)
    )
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert logs == []


async def test_archive_accounts_dropped_lines(tmp_path):
    # lines evicted from the ring before archiving must be accounted
    # for in dropped_lines, not silently lost -- otherwise the archived
    # tail presents itself as the whole output.
    cron = await _archive_cron(tmp_path)
    pairs = [("stdout", "line-%d" % i) for i in range(1, 9)]
    info = _output_run(pairs, limit=5)
    await cron._archive_output(
        cron.cron_jobs["j"], info, list(info.output.lines)
    )
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert rec["dropped_lines"] == 3
    stored = [ln["line"] for ln in rec["lines"]]
    assert stored == ["line-4", "line-5", "line-6", "line-7", "line-8"]


async def test_archive_redacts_multiline_pem_body(tmp_path):
    # the base64 body lines ARE the key material; per-line patterns
    # cannot recognise them in isolation, so the whole block (header,
    # body, footer) must come out redacted.
    cron = await _archive_cron(tmp_path)
    info = _output_run(
        [
            ("stdout", "-----BEGIN RSA PRIVATE KEY-----"),
            ("stdout", "MIIEpAIBAAKCAQEA7v0Kq1QYb3x2"),
            ("stdout", "u5m3o9CqkQxJ0Zb2n8T4w6YcAaBb"),
            ("stdout", "-----END RSA PRIVATE KEY-----"),
        ]
    )
    await cron._archive_output(
        cron.cron_jobs["j"], info, list(info.output.lines)
    )
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert [ln["line"] for ln in rec["lines"]] == [REDACTED] * 4


async def test_archive_verbatim_when_redaction_off(tmp_path):
    # redactArchivedSecrets: false is an explicit opt-out: the archive
    # must be exactly what the job printed.
    cron = await _archive_cron(tmp_path, redact=False)
    info = _output_run([("stdout", "password=hunter2")])
    await cron._archive_output(
        cron.cron_jobs["j"], info, list(info.output.lines)
    )
    (rec,) = await cron.state_backend.list_records(cron._log_stream("j"))
    assert rec["redacted"] is False
    assert rec["lines"][0]["line"] == "password=hunter2"


# --- rehydration ------------------------------------------------------------


def test_rehydrate_corrupt_outcome_is_unknown():
    # a record missing (or corrupting) `outcome` must NOT rehydrate as
    # a fabricated "success": that skewed the dashboard stats and could
    # wrongly open the depends-on-past gate.
    info = _job_run_info_from_dict(
        {"finished_at": "2026-07-01T10:00:00+00:00"}
    )
    assert info is not None
    assert info.outcome == "unknown"


def test_rehydrate_mixed_naive_aware_duration():
    # a naive started_at next to an aware finished_at used to make the
    # .duration property raise TypeError on every dashboard request;
    # both timestamps are pinned aware now.
    info = _job_run_info_from_dict(
        {
            "finished_at": "2026-07-01T10:00:00+00:00",
            "started_at": "2026-07-01T09:59:00",
        }
    )
    assert info is not None
    assert info.duration == 60.0


async def test_rehydration_does_not_regress_fresh_run(tmp_path):
    # the ledger read awaits (and so yields): a run can finish in that
    # window.  Appending the snapshot's OLD records behind the fresh
    # run would regress last_run and scramble the history's order, so
    # rehydration must re-check after the await and stand down.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    cron._state_rehydrated = False  # force a fresh warm-up below
    fresh = _mem_run("failure", 9)
    old_rec = {
        "outcome": "success",
        "exit_code": 0,
        "started_at": None,
        "finished_at": "2026-07-01T00:00:00+00:00",
        "duration": None,
        "fail_reason": None,
    }

    async def racing_list(stream, *, limit=None, newest_first=False):
        # rehydration also reads the counters/retries streams these days;
        # only the run-history read carries this test's race.
        if not stream.startswith("runs/"):
            return []
        for _ in range(3):
            await asyncio.sleep(0)  # the read is "in flight"
        # a run finishes while the read is in flight: _record_run's
        # in-memory effect lands before the snapshot returns.
        cron.last_run["j"] = fresh
        cron.run_history["j"].append(fresh)
        return [old_rec]

    cron.state_backend.list_records = racing_list  # type: ignore[method-assign]
    await cron._rehydrate_from_state()
    assert cron.last_run["j"] is fresh  # not regressed to the old record
    assert list(cron.run_history["j"]) == [fresh]


async def test_state_path_change_rewarms_from_new_store(tmp_path):
    # switching the state path tears the old backend down; without
    # resetting _state_rehydrated, the replacement store never warmed
    # the dashboard history -- the old store's (here: empty) view was
    # served forever.
    path_a = tmp_path / "a"
    path_b = tmp_path / "b"
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(path_a)))
    assert cron._state_rehydrated is True
    assert not cron.run_history.get("j")  # store A is empty
    # seed store B out of band, as a previous deployment would have
    seed = make_state_backend(_state_cfg(_state_yaml(path_b)), lambda: "s")
    await seed.start()
    await seed.append_record(
        "runs/j",
        {"outcome": "success", "finished_at": "2026-06-30T00:00:00+00:00"},
    )
    await cron.start_stop_state(_state_cfg(_state_yaml(path_b)))
    assert cron._state_rehydrated is True  # re-latched by the new warm-up
    assert len(cron.run_history["j"]) == 1
    assert (
        cron.last_run["j"].finished_at.isoformat()
        == "2026-06-30T00:00:00+00:00"
    )


async def test_state_swap_resets_the_reboot_gate_health_latch(tmp_path):
    # a store-op timeout latches _reboot_gate_sick so the rest of that boot
    # pass stops probing the hung store; the latch is a per-store health
    # verdict (like _slot_fidelity, reset in the same teardown) and must not
    # survive into a replacement store brought up by a state-section reload,
    # or @reboot dedupe stays degraded for the life of the process.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path / "a")))
    cron._reboot_gate_sick = True  # as a hung store's timeout would latch
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path / "b")))
    assert cron._reboot_gate_sick is False


_REPLACE_DEP_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
    "    concurrencyPolicy: Replace\n"
)


async def test_depends_on_past_replace_policy_skips_running_block(tmp_path):
    # Replace's contract is that a new fire supersedes the running instance
    # (maybe_launch_job cancels it), so the gate's still-running block must
    # not apply: otherwise one hung run freezes a gated Replace job forever
    # (the fire never reaches the policy that would reap it).  The gate then
    # judges the last FINISHED outcome, exactly as before the hardening.
    cron = Cron(None, config_yaml=_REPLACE_DEP_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    await _put_ledger(cron, "success", "2026-07-01T10:00:00+00:00")
    cron.running_jobs["j"].append(object())  # a hung RunningJob stand-in
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True
    # ...while a last-finished FAILURE still closes the gate for Replace.
    await _put_ledger(cron, "failure", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_deferred_catch_up_anchors_to_first_evaluation_instant(
    tmp_path,
):
    # The backend can come up minutes after boot (start_stop_state retries).
    # In between, the live scheduler fired jobs statelessly, so a deferred
    # evaluation must count missed slots against the FIRST attempt's
    # instant: counting up to the (later) recovery instant would replay
    # runs that actually ran.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    backend = cron.state_backend
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    # first attempt: backend "not started yet" -> deferred, reference pinned.
    cron.state_backend = None
    await cron._catch_up(_NOW)  # 10 slots missed as of _NOW (10:10:30)
    assert cron._caught_up is False and calls == []
    # backend recovers; the retry arrives 5 minutes later.
    cron.state_backend = backend
    cron._catchup_next_retry = 0.0
    later = _NOW + datetime.timedelta(minutes=5)
    await cron._catch_up(later)
    await asyncio.gather(*list(cron._catchup_tasks))
    # still the 10 pre-boot slots -- not 15.
    assert len(calls) == 10


async def test_backfill_revalidates_between_launches(tmp_path):
    # A serialized run-all backfill spans count x run-duration: a reload
    # disabling/removing the job mid-backfill must stop the remaining
    # launches (the old code revalidated only once, after the jitter).
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls = []

    async def fake(job, *, with_retries=True):
        calls.append(job.name)
        if len(calls) == 2:
            del cron.cron_jobs["j"]  # a reload removes the job
        return True

    cron.maybe_launch_job = fake  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 5, 0.0, _NOW)
    assert calls == ["j", "j"]  # the remaining 3 launches were dropped


async def test_backfill_idle_wait_is_bounded_for_allow_policy(
    tmp_path, monkeypatch
):
    # An Allow job whose scheduled instances always overlap keeps
    # running_jobs non-empty forever; the idle wait between backfill
    # launches is pacing there, not correctness, so it must give up and
    # launch rather than starve the backfill and hold the checkpoint open.
    monkeypatch.setattr(
        "cronstable.cron.CATCHUP_IDLE_WAIT_LIMIT", 0.0
    )  # give up immediately: no wall-clock waiting in the test
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron.running_jobs["j"].append(object())  # ever-running scheduled instance
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == ["j"]


# --- cluster slot: stale release vs fresh same-fence re-claim ----------------


_FORBID_CLUSTER_JOB = (
    "jobs:\n"
    "  - name: j\n"
    "    command: 'true'\n"
    "    schedule: '* * * * *'\n"
    "    concurrencyPolicy: Forbid\n"
    "    concurrencyScope: cluster\n"
)


async def _cluster_cron(tmp_path):
    cron = Cron(None, config_yaml=_FORBID_CLUSTER_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    return cron


async def _stop_cluster_cron(cron):
    for task in list(cron._slot_renewers.values()):
        task.cancel()
    cron._slot_renewers.clear()
    cron._slot_leases.clear()
    cron._slot_refs.clear()
    await asyncio.gather(*list(cron._pending_state_writes))
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_stale_slot_release_stands_down_for_fresh_reclaim(tmp_path):
    # regression (slot-protocol): _release_cluster_slot pops the lease under
    # the per-job mutex but writes the on-disk release fire-and-forget, and
    # a same-holder re-acquire KEEPS the fence -- so a stale release landing
    # after a fresh re-claim still matched on disk and revoked the new
    # claim's lease, letting a peer's Forbid claim double-run. The release
    # must re-check under the mutex and stand down for a live claim.
    cron = await _cluster_cron(tmp_path)
    try:
        backend = cron.state_backend
        holder = cron._slot_holder()
        stale = await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        fresh = await backend.acquire_lease("slots/j", holder, cron._slot_ttl)
        assert stale is not None and fresh is not None
        assert fresh.fence == stale.fence  # the kept-fence re-acquire
        cron._slot_leases["j"] = fresh
        cron._slot_refs["j"] = 1
        await cron._release_slot_lease("j", stale)
        assert await backend.read_lease("slots/j") is not None
        # ...while with no live claim the release still frees the slot
        cron._slot_leases.pop("j", None)
        cron._slot_refs.pop("j", None)
        await cron._release_slot_lease("j", fresh)
        assert await backend.read_lease("slots/j") is None
    finally:
        await _stop_cluster_cron(cron)


async def test_gc_reclaims_removed_scope_artifacts_and_orphan_blobs(
    tmp_path, monkeypatch
):
    # regression (GC review): artifact streams were absent from the daemon
    # GC's keep map ("unrecognised: kept forever") and the fully-implemented
    # blob sweep had no production caller, so a removed job's artifacts --
    # and every orphaned payload blob -- leaked without bound.  One pass
    # must age out a removed scope's stream and sweep its blob, while a
    # config job's scope, the shared scope, a referenced blob, and a
    # just-written (not-yet-recorded) blob all survive.
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            gone = await jobstate.artifact_put(
                backend, "gone", "a", b"gone-payload"
            )
            kept = await jobstate.artifact_put(
                backend, "j", "k", b"job-payload"
            )
            shared = await jobstate.artifact_put(
                backend, "global", "g", b"shared-payload"
            )
        finally:
            monkeypatch.undo()
        for rec in (gone, kept, shared):
            path = backend._blob_path(rec["sha256"])
            os.utime(path, (old_epoch, old_epoch))
        # unreferenced but young: the put-then-record window's blob.
        young = await backend.put_blob(b"just-put-no-record-yet")
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert await backend.list_records("artifacts/gone") == []
        assert await backend.get_blob(gone["sha256"]) is None
        assert len(await backend.list_records("artifacts/j")) == 1
        assert await backend.get_blob(kept["sha256"]) == b"job-payload"
        assert len(await backend.list_records("artifacts/global")) == 1
        assert await backend.get_blob(shared["sha256"]) == b"shared-payload"
        assert await backend.get_blob(young) is not None
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_blob_sweep_skipped_when_artifact_stream_hidden(
    tmp_path, monkeypatch
):
    # the fail-safe: a legacy length-truncated stream directory without its
    # name sidecar is skipped by enumeration, so its records -- and the blob
    # references inside them -- are invisible.  The sweep must then not run
    # at all this pass (the hidden stream's blob would otherwise read as an
    # orphan and a LIVE payload would be deleted).
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        hidden_scope = "S" * 200
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            hidden = await jobstate.artifact_put(
                backend, hidden_scope, "a", b"hidden-payload"
            )
        finally:
            monkeypatch.undo()
        os.utime(
            backend._blob_path(hidden["sha256"]), (old_epoch, old_epoch)
        )
        stream_dir = backend._stream_dir("artifacts/" + hidden_scope)
        os.unlink(os.path.join(stream_dir, state_mod._STREAM_NAME_SIDECAR))
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        # the unclassifiable stream is kept (existing collect_garbage rule)
        # AND its blob survived, proving the sweep stood down.
        recs = await backend.list_records("artifacts/" + hidden_scope)
        assert len(recs) == 1
        assert await backend.get_blob(hidden["sha256"]) == b"hidden-payload"
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_leaves_artifacts_unmanaged_without_scope_manifests(
    tmp_path, monkeypatch
):
    # rolling-upgrade safety: while any recent manifest predates scope/dag
    # advertising, its node's shared artifact scopes are unknowable, so
    # artifact streams must stay wholly unmanaged (kept) -- even an aged,
    # unreferenced scope -- while ordinary job streams still collect.
    import cronstable.state as state_mod
    from cronstable import jobstate

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            gone = await jobstate.artifact_put(
                backend, "gone", "a", b"gone-payload"
            )
            await backend.append_record("runs/orphan", {"finished_at": "x"})
        finally:
            monkeypatch.undo()
        os.utime(backend._blob_path(gone["sha256"]), (old_epoch, old_epoch))
        await _seed_gc_anchor(cron, covered=False)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert await backend.list_records("runs/orphan") == []
        assert len(await backend.list_records("artifacts/gone")) == 1
        # its record survived, so its blob is referenced and kept too.
        assert await backend.get_blob(gone["sha256"]) == b"gone-payload"
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_keeps_catchup_dag_streams_without_dag_manifests(
    tmp_path, monkeypatch
):
    # rolling-upgrade safety, the dag catch-up twin of the artifact test
    # above: while any recent manifest predates dag advertising, live_dags
    # is a partial view, so a peer's rarely-written catchup-dag/ checkpoint
    # stream must stay unmanaged (kept) while ordinary job streams still
    # collect; deleting it live would re-run the work it exists to fence.
    import cronstable.state as state_mod

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            await backend.append_record(
                "catchup-dag/peerdag", {"watermark": "x"}
            )
            await backend.append_record("runs/orphan", {"finished_at": "x"})
        finally:
            monkeypatch.undo()
        await _seed_gc_anchor(cron, covered=False)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        # the orphan run stream collected, proving the pass really ran...
        assert await backend.list_records("runs/orphan") == []
        # ...while the aged peer checkpoint survived the partial view.
        assert len(await backend.list_records("catchup-dag/peerdag")) == 1
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_collects_removed_dag_catchup_streams_when_covered(
    tmp_path, monkeypatch
):
    # the guard must not overcorrect into never managing the prefix: once
    # every recent manifest advertises its dags, an aged checkpoint stream
    # for a dag no config or manifest knows ages out exactly as a removed
    # job's catchup/<job> stream does.
    import cronstable.state as state_mod

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            await backend.append_record(
                "catchup-dag/peerdag", {"watermark": "x"}
            )
        finally:
            monkeypatch.undo()
        await _seed_gc_anchor(cron, covered=True)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert await backend.list_records("catchup-dag/peerdag") == []
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_gc_pass_reclaims_only_ephemeral_leases(tmp_path, monkeypatch):
    # the daemon pass wires the ephemeral-lease prefix through to the
    # backend: a dead-past-grace dagadvance/ per-run lease is reclaimed
    # while a slots/ lease of the same age survives -- its fence can live
    # on in durable Replace-cancel records (cron._request_replace /
    # _slot_renewer), so no grace window ever makes a slot fence reset
    # safe.
    import cronstable.state as state_mod

    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg(_state_yaml(tmp_path)))
    try:
        backend = cron.state_backend
        old_epoch = state_mod._now() - 7200.0
        monkeypatch.setattr(state_mod, "_now", lambda: old_epoch)
        try:
            assert await backend.acquire_lease(
                "dagadvance/d/r1", "A", ttl=10.0
            )
            assert await backend.acquire_lease("slots/j", "A", ttl=10.0)
        finally:
            monkeypatch.undo()
        dag_lock, dag_lease = backend._lease_paths("dagadvance/d/r1")
        slot_lock, slot_lease = backend._lease_paths("slots/j")
        for path in (dag_lease, slot_lease):
            os.utime(path, (old_epoch, old_epoch))
        await _seed_gc_anchor(cron)
        cron._state_gc_grace = 3600.0
        await cron._collect_state_garbage()
        assert not os.path.exists(dag_lease)
        assert not os.path.exists(dag_lock)
        assert os.path.exists(slot_lease)  # the fence line's only home
        assert os.path.exists(slot_lock)
    finally:
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_slot_release_write_yields_to_racing_reclaim(tmp_path):
    # the same hazard through the production path: the finish-path release
    # schedules its write fire-and-forget, the job's next fire re-claims
    # immediately (same holder, fence kept), and only then does the
    # scheduled write run. The slot must still be held on disk afterwards,
    # under the original fence -- the new run's claim survived.
    cron = await _cluster_cron(tmp_path)
    try:
        job = cron.cron_jobs["j"]
        backend = cron.state_backend
        assert await cron._claim_cluster_slot(job) is True
        first = cron._slot_leases["j"]
        await cron._release_cluster_slot(job)  # schedules the stale write
        assert await cron._claim_cluster_slot(job) is True  # fresh re-claim
        await asyncio.gather(*list(cron._pending_state_writes))
        observed = await backend.read_lease("slots/j")
        assert observed is not None
        assert observed.holder == cron._slot_holder()
        assert observed.fence == first.fence
    finally:
        await _stop_cluster_cron(cron)


async def test_track_state_write_sheds_when_pending_set_full(monkeypatch):
    # A wedged store must not let the tracked fire-and-forget write set grow
    # without bound (-> OOM). Past MAX_PENDING_STATE_WRITES a new best-effort
    # write is SHED: its coroutine is closed and the drop counted, and a
    # placeholder task is returned so callers that chain on the result keep
    # working. Every state write is best-effort, so shedding is safe.
    cron = Cron(None, config_yaml=_ONE_JOB)
    monkeypatch.setattr(cron_mod, "MAX_PENDING_STATE_WRITES", 2)

    async def _block():
        await asyncio.sleep(3600)

    fillers = [asyncio.ensure_future(_block()) for _ in range(2)]
    cron._pending_state_writes = set(fillers)
    before = cron.metrics._state_dropped.get("overflow", 0)

    ran = False

    async def _would_write():
        nonlocal ran
        ran = True

    task = cron._track_state_write(_would_write())
    await task  # the shed placeholder completes immediately
    assert cron.metrics._state_dropped.get("overflow", 0) == before + 1
    assert ran is False  # the real write coroutine was closed, never run

    # under the cap the write is tracked and actually runs.
    for f in fillers:
        f.cancel()
    cron._pending_state_writes = set()
    real_task = cron._track_state_write(_would_write())
    await real_task
    assert ran is True


async def test_a_shed_chained_write_leaves_no_unawaited_coroutine(monkeypatch):
    # The chained-tail helper builds its body INSIDE the ordered wrapper, so
    # that shedding closes the only coroutine that was ever created. Built at
    # the call site instead, the shed closes the wrapper and the inner
    # coroutine is left neither awaited nor closed: a RuntimeWarning per shed
    # write, and an outright error under -W error, at exactly the moment the
    # store is already in trouble.
    cron = Cron(None, config_yaml=_ONE_JOB)
    monkeypatch.setattr(cron_mod, "MAX_PENDING_STATE_WRITES", 0)
    appended = []
    monkeypatch.setattr(
        Cron,
        "_append_retry_record",
        lambda self, name, record: appended.append(name),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # every chained-write entry point, through its real caller
        await cron._queue_retry_write("alpha", {"kind": "armed"})
        await cron._queue_pause_write("alpha", {"kind": "paused"})
        gc.collect()
    assert appended == []  # shed, as the cap demands
    assert [w for w in caught if "never awaited" in str(w.message)] == []
