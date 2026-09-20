"""Same-process suspend/stall recovery through the real scheduling path."""

import asyncio
import datetime

import pytest

from cronstable.cron import Cron
from cronstable.job import JobRetryState
from tests._cron_helpers import _set_now
from tests._helpers import (
    start_state,
    _drain_state_writes,
    _reap_running,
    _state_cfg,
    _wait_until,
)

UTC = datetime.timezone.utc
BOOT = datetime.datetime(2026, 7, 1, 10, 0, 30, tzinfo=UTC)
RESUME = BOOT + datetime.timedelta(minutes=9)


async def _drain(cron):
    while cron._catchup_tasks:
        await asyncio.gather(*list(cron._catchup_tasks))


async def _daemon(
    tmp_path,
    monkeypatch,
    *,
    policy="run-once",
    schedule="*/5 * * * *",
    extra="",
    state=True,
    seed=True,
    outcome="success",
):
    clock = {"now": BOOT}
    _set_now(monkeypatch, clock)
    yaml = (
        "jobs:\n  - name: j\n    command: echo recovered\n"
        f"    schedule: '{schedule}'\n    utc: true\n"
        f"    onMissed: {policy}\n{extra}"
    )
    cron = Cron(None, config_yaml=yaml)
    if state:
        await start_state(
        cron,
        _state_cfg("state:\n  path: " + str(tmp_path))
        )
        if seed:
            await cron.state_backend.append_record(
                cron._run_stream("j"),
                {
                    "outcome": "success",
                    "finished_at": BOOT.isoformat(),
                },
            )
    calls = []

    async def launch(job, *, with_retries=True, catchup_after=None):
        calls.append((clock["now"], with_retries))
        cron._sla_last_start[job.name] = clock["now"]
        if cron.state_backend is not None:
            await cron.state_backend.append_record(
                cron._run_stream(job.name),
                {
                    "outcome": outcome,
                    "finished_at": clock["now"].isoformat(),
                },
            )
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", launch)
    await cron._service_slots(startup=True)
    await _drain(cron)
    assert cron._caught_up
    assert calls == []
    return cron, clock, calls, yaml


async def _resume(cron, clock, now=RESUME):
    clock["now"] = now
    await cron._service_slots(startup=False)
    await _drain(cron)


@pytest.mark.parametrize(
    "policy,schedule,expected",
    [("run-once", "*/5 * * * *", 1), ("run-all", "*/2 * * * *", 4)],
)
async def test_resume_nonmatching_minute_recovers_skipped_slots(
    tmp_path, monkeypatch, policy, schedule, expected
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy=policy, schedule=schedule
    )
    await _resume(cron, clock)
    assert calls == [(RESUME, False)] * expected
    assert cron._next_fire["j"] == BOOT.replace(minute=10, second=0)
    assert await cron._pending_catchup_watermark("j") is None
    assert cron._resume_catchup == {}


@pytest.mark.parametrize("outcome", ["success", "failure"])
async def test_resume_attempt_is_final_across_passes_and_restart(
    tmp_path, monkeypatch, outcome
):
    cron, clock, calls, yaml = await _daemon(
        tmp_path, monkeypatch, outcome=outcome
    )
    await _resume(cron, clock)
    for _ in range(3):
        await _resume(cron, clock)
    assert calls == [(RESUME, False)]
    restarted = Cron(None, config_yaml=yaml)
    await start_state(
        restarted,
        _state_cfg("state:\n  path: " + str(tmp_path))
    )
    restarted_calls = []

    async def launch(job, **kwargs):
        restarted_calls.append(job.name)

    monkeypatch.setattr(restarted, "maybe_launch_job", launch)
    await restarted._service_slots(startup=True)
    await _drain(restarted)
    assert restarted_calls == []
    # A later, genuinely separate gap still receives its own attempt.
    await _resume(cron, clock, RESUME + datetime.timedelta(minutes=10))
    assert len(calls) == 2


@pytest.mark.parametrize("policy,expected", [("run-once", 1), ("run-all", 10)])
async def test_matching_resume_slot_is_not_replayed(
    tmp_path, monkeypatch, policy, expected
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy=policy, schedule="* * * * *"
    )
    now = BOOT.replace(minute=10)
    await _resume(cron, clock, now)
    # The normal fire has already finished and advanced the durable ledger.
    # run-all still owes 10:01..10:09; run-once is satisfied by that fire.
    assert calls == [(now, True)] + [(now, False)] * (expected - 1)


@pytest.mark.parametrize(
    "policy,state,extra",
    [
        ("skip", True, ""),
        ("run-once", False, ""),
        ("run-once", True, "    startingDeadlineSeconds: 60\n"),
        ("run-once", True, "    enabled: false\n"),
    ],
)
async def test_resume_honors_opt_in_and_deadline(
    tmp_path, monkeypatch, policy, state, extra
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy=policy, state=state, extra=extra
    )
    await _resume(cron, clock)
    assert calls == []
    assert cron._resume_catchup == {}


async def test_resume_first_observed_due_slot_needs_no_previous_run(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch, seed=False)
    await _resume(cron, clock)
    assert calls == [(RESUME, False)]


async def test_short_delay_and_same_minute_do_not_arm_durable_recovery(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    await _resume(cron, clock, BOOT.replace(minute=5, second=9))
    assert len(calls) == 1
    assert cron._resume_catchup == {}
    await _resume(cron, clock, BOOT.replace(minute=10, second=30))
    assert len(calls) == 2
    assert cron._resume_catchup == {}


async def test_resume_forbid_waits_for_existing_run_and_keeps_retry_ladder(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, extra="    concurrencyPolicy: Forbid\n"
    )
    retry = JobRetryState(1, 2, 60)
    cron.retry_state["j"] = retry
    cron.running_jobs["j"] = [object()]
    clock["now"] = RESUME
    await cron._service_slots(startup=False)
    await _wait_until(lambda: "j" in cron._catchup_running)
    await asyncio.sleep(0.02)
    assert calls == []
    assert await cron._pending_catchup_watermark("j") is not None
    cron.running_jobs.pop("j")
    await _drain(cron)
    assert calls == [(RESUME, False)]
    assert cron.retry_state["j"] is retry


async def test_ordinary_start_while_recovery_waits_satisfies_run_once(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait_idle(name, **kwargs):
        entered.set()
        await release.wait()
        return True

    monkeypatch.setattr(cron, "_wait_job_idle", wait_idle)
    clock["now"] = RESUME
    await cron._service_slots(startup=False)
    await asyncio.wait_for(entered.wait(), 2)
    await cron.maybe_launch_job(cron.cron_jobs["j"])
    release.set()
    await _drain(cron)
    assert calls == [(RESUME, True)]
    assert await cron._pending_catchup_watermark("j") is None


async def test_resume_waits_for_startup_backfill_without_touching_its_pin(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    release = asyncio.Event()
    worker = asyncio.create_task(release.wait())
    cron._catchup_running["j"] = worker
    await _resume(cron, clock)
    assert calls == []
    assert cron._resume_catchup
    assert await cron._pending_catchup_watermark("j") is None
    release.set()
    await worker
    cron._resume_catchup_next_retry = 0
    await _resume(cron, clock)
    assert calls == [(RESUME, False)]


async def test_store_recovery_keeps_original_window_and_excludes_live_fires(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy="run-all", schedule="*/2 * * * *"
    )
    backend = cron.state_backend
    cron.state_backend = None
    await _resume(cron, clock)
    assert calls == []
    await _resume(cron, clock, BOOT.replace(minute=10, second=0))
    assert len(calls) == 1  # live scheduling kept working without the store
    cron.state_backend = backend
    cron._resume_catchup_next_retry = 0
    await _resume(cron, clock, BOOT.replace(minute=10, second=1))
    assert len(calls) == 5  # only 10:02, 10:04, 10:06, 10:08 were skipped


@pytest.mark.parametrize("moved", [False, True])
async def test_resume_cluster_gate_defers_or_retires(
    tmp_path, monkeypatch, moved
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    monkeypatch.setattr(cron, "_cluster_owner_moved", lambda job: moved)
    await _resume(cron, clock)
    assert calls == []
    assert bool(cron._resume_catchup) is not moved
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._resume_catchup_next_retry = 0
    await _resume(cron, clock)
    assert len(calls) == (0 if moved else 1)


async def test_interrupted_resume_checkpoint_recovers_on_restart(
    tmp_path, monkeypatch
):
    cron, clock, calls, yaml = await _daemon(tmp_path, monkeypatch)
    entered = asyncio.Event()

    async def wait_idle(name, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(cron, "_wait_job_idle", wait_idle)
    clock["now"] = RESUME
    await cron._service_slots(startup=False)
    await asyncio.wait_for(entered.wait(), 2)
    task = cron._catchup_running["j"]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert calls == []
    assert await cron._pending_catchup_watermark("j") is not None
    restarted = Cron(None, config_yaml=yaml)
    await start_state(
        restarted,
        _state_cfg("state:\n  path: " + str(tmp_path))
    )
    # Keep the shared fake launcher for counting, but use the new daemon's
    # real evaluator and checkpoint stream.
    monkeypatch.setattr(restarted, "maybe_launch_job", cron.maybe_launch_job)
    await restarted._service_slots(startup=True)
    await _drain(restarted)
    assert calls == [(RESUME, False)]
    assert await restarted._pending_catchup_watermark("j") is None


@pytest.mark.parametrize("paused_before_gap,expected", [(True, 0), (False, 1)])
async def test_resume_pause_excuses_only_its_own_window(
    tmp_path, monkeypatch, paused_before_gap, expected
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    clock["now"] = BOOT if paused_before_gap else RESUME
    await cron.pause_job_by_name("j", duration=3600)
    await _drain_state_writes(cron)
    await _resume(cron, clock)
    assert calls == []
    assert bool(cron._resume_catchup) is not paused_before_gap
    await cron.resume_job_by_name("j")
    await _drain_state_writes(cron)
    cron._resume_catchup_next_retry = 0
    await _resume(cron, clock)
    assert len(calls) == expected
    assert await cron._pending_catchup_watermark("j") is None


@pytest.mark.parametrize("deadline,backfills", [(180, 2), (None, 100)])
async def test_resume_run_all_is_bounded(
    tmp_path, monkeypatch, deadline, backfills
):
    cron, clock, calls, _ = await _daemon(
        tmp_path,
        monkeypatch,
        policy="run-all",
        schedule="* * * * *",
        extra=f"    startingDeadlineSeconds: {deadline}\n" if deadline else "",
    )
    await _resume(cron, clock, BOOT + datetime.timedelta(hours=8))
    # The current minute fires normally; the rest are bounded recovery.
    assert len(calls) == backfills + 1
    assert sum(not retry for _, retry in calls) == backfills


async def test_resume_window_read_does_not_block_live_scheduling(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(tmp_path, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = cron.durable_last_run_at

    async def read(name):
        entered.set()
        await release.wait()
        return await original(name)

    monkeypatch.setattr(cron, "durable_last_run_at", read)
    clock["now"] = RESUME
    await asyncio.wait_for(cron._service_slots(startup=False), 1)
    await asyncio.wait_for(entered.wait(), 1)
    task = cron._catchup_eval_task
    clock["now"] = BOOT.replace(minute=10, second=0)
    await asyncio.wait_for(cron._service_slots(startup=False), 1)
    assert cron._catchup_eval_task is task  # evaluation is single-flight
    assert len(calls) == 1
    release.set()
    await _drain(cron)
    assert len(calls) == 1


async def test_disjoint_resume_windows_do_not_replay_intervening_live_slots(
    tmp_path, monkeypatch
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy="run-all", schedule="*/2 * * * *"
    )
    backend = cron.state_backend
    cron.state_backend = None
    await _resume(cron, clock)
    await _resume(cron, clock, BOOT.replace(minute=10, second=0))
    await _resume(cron, clock, BOOT.replace(minute=12, second=0))
    await _resume(cron, clock, BOOT.replace(minute=19))
    assert len(calls) == 2
    cron.state_backend = backend
    for _ in range(2):
        cron._resume_catchup_next_retry = 0
        await _resume(cron, clock, BOOT.replace(minute=19))
    assert len(calls) == 9  # four skipped before 10:10, three after 10:12


async def test_run_once_rechecks_after_waiting_for_launch_lock(
    tmp_path, monkeypatch
):
    cron, _, _, _ = await _daemon(tmp_path, monkeypatch)
    lock = cron._launch_locks["j"]
    await lock.acquire()
    task = asyncio.create_task(
        Cron.maybe_launch_job(
            cron, cron.cron_jobs["j"], with_retries=False, catchup_after=BOOT
        )
    )
    await asyncio.sleep(0)
    cron._sla_last_start["j"] = RESUME  # the normal spawn holding the lock
    lock.release()
    assert await asyncio.wait_for(task, 1) is False
    assert not cron.running_jobs


async def test_resume_runs_real_failed_process_once(tmp_path, monkeypatch):
    cron, clock, _, _ = await _daemon(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cron, "maybe_launch_job", Cron.maybe_launch_job.__get__(cron)
    )
    cron.cron_jobs["j"].command = "exit 7"
    clock["now"] = RESUME
    await cron._service_slots(startup=False)
    await _wait_until(lambda: bool(cron.running_jobs.get("j")))
    await _reap_running(cron)
    await _drain(cron)
    await _drain_state_writes(cron)
    assert cron.last_run["j"].outcome == "failure"
    assert cron.last_run["j"].exit_code == 7
    assert "j" not in cron.retry_state
    assert await cron._pending_catchup_watermark("j") is None
    await _resume(cron, clock)
    assert not cron.running_jobs
    assert len(cron.run_history["j"]) == 1


@pytest.mark.parametrize("policy", ["run-once", "run-all"])
@pytest.mark.parametrize(
    "schedule,extra,when",
    [
        ("5 * * * * * *", "", BOOT + datetime.timedelta(minutes=1)),
        ("5 6 * * *", "    timezone: America/New_York\n", RESUME),
    ],
)
async def test_resume_uses_seconds_and_job_timezone(
    tmp_path, monkeypatch, policy, schedule, extra, when
):
    cron, clock, calls, _ = await _daemon(
        tmp_path, monkeypatch, policy=policy, schedule=schedule, extra=extra
    )
    await _resume(cron, clock, when)
    assert calls == [(when, False)]
