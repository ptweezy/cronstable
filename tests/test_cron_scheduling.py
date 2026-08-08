import asyncio
import datetime
import time
from zoneinfo import ZoneInfo

import pytest

import cronstable.cron
from cronstable.job import JobOutputStream, JobRetryState
from tests._cron_helpers import (
    _EVERY_SECOND_AND_MINUTE,
    _PAUSABLE_JOB,
    _RELOAD_AFTER,
    _RELOAD_BEFORE,
    _SECONDS_JOB,
    _SLA_RUNTIME_JOB,
    _SLA_STALE_JOB,
    _SUBMINUTE_NOFIRE,
    _THREE_DUE,
    DT,
    LATE,
    RUNTIME,
    STALE,
    TWO_JOBS,
    UTC,
    _drive_cron,
    _noop,
    _reboot_job,
    _seed_due,
    _set_now,
    _sla_report_recorder,
    _wait_until,
    fixed_current_time,  # noqa: F401
)
from tests.test_state import (
    _NOW,
    _count_launcher,
    _cron_with_watermark,
    _state_cfg,
)

LONDON = ZoneInfo("Europe/London")


@pytest.mark.parametrize(
    "schedule, timezone, utc, now, startup, enabled, result",
    [
        (
            "* * * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            True,  # startup
            "",
            False,
        ),
        (
            "49 14 * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "59 14 * * *",
            "",
            "utc: true",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "utc: true",  # London is UTC+1 during DST
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC).astimezone(LONDON),
            False,
            "",
            True,
        ),
        (
            "59 14 * * *",
            "",
            "utc: false",  # London is UTC+1 during DST
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC).astimezone(LONDON),
            False,
            "",
            False,
        ),
        (
            "1 8 * * *",
            "timezone: America/Los_Angeles",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            True,
        ),
        (
            "1 8 * * *",
            "timezone: Europe/London",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "@reboot",
            "",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            False,
            "",
            False,
        ),
        (
            "@reboot",
            "",
            "",
            DT(2020, 7, 20, 15, 1, 1, tzinfo=UTC),
            True,
            "",
            True,
        ),
        # enabled: false
        (
            "* * * * *",
            "",
            "",
            DT(2020, 7, 20, 14, 59, 1, tzinfo=UTC),
            False,
            "enabled: false",
            False,
        ),
    ],
)
def test_job_should_run(
    monkeypatch, schedule, timezone, utc, now, startup, enabled, result
):
    def get_now(timezone):
        print("timezone: ", timezone)
        retval = now
        if timezone is not None:
            retval = retval.astimezone(timezone)
        print("now: ", retval)
        return retval

    monkeypatch.setattr("cronstable.cron.get_now", get_now)

    config_yaml = f"""
jobs:
  - name: test
    command: |
      echo "foobar"
    schedule: "{schedule}"
    {timezone}
    {utc}
    {enabled}
                            """
    print(config_yaml)
    cron = cronstable.cron.Cron(None, config_yaml=config_yaml)
    job = list(cron.cron_jobs.values())[0]
    assert cron.job_should_run(startup, job) == result


# =====================================================================
#  second-level (sub-minute) scheduling
# =====================================================================


def test_schedule_slot_resolution(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 4, 500000)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    sec = cron.cron_jobs["sec"]
    minute = cron.cron_jobs["min"]
    # a second-level job truncates to the whole second (microseconds zeroed)
    assert cronstable.cron.schedule_slot(sec) == DT(
        2020, 1, 1, 0, 0, 4, tzinfo=UTC
    )
    # a minute-level job truncates to the top of the minute, as always
    assert cronstable.cron.schedule_slot(minute) == DT(
        2020, 1, 1, 0, 0, 0, tzinfo=UTC
    )


def test_needs_subminute():
    # an enabled second-level job makes the scheduler tick per-second
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    assert cron._needs_subminute() is True
    # minute-only config does not
    cron2 = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron2._needs_subminute() is False
    # a DISABLED second-level job must not force per-second ticking
    disabled = """
jobs:
  - name: sec
    command: echo sec
    schedule: "*/15 * * * * * *"
    enabled: false
"""
    cron3 = cronstable.cron.Cron(None, config_yaml=disabled)
    assert cron3._needs_subminute() is False


@pytest.mark.parametrize(
    "second, should_run",
    [(0, True), (1, False), (14, False), (15, True), (45, True), (46, False)],
)
def test_job_should_run_at_seconds(monkeypatch, second, should_run):
    holder = {"now": DT(2020, 1, 1, 0, 0, second)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)
    job = cron.cron_jobs["sec"]  # "*/15 * * * * * *"
    assert cron.job_should_run(False, job) is should_run


def test_next_sleep_interval_modes(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 30, 30, 500000)}
    _set_now(monkeypatch, holder)
    # minute mode snaps to the next minute (preserving the sub-second offset,
    # exactly as the historical behaviour did): from :30.5 that is 30.0s away.
    assert cronstable.cron.next_sleep_interval(False) == pytest.approx(30.0)
    # sub-minute mode snaps to the next whole-second boundary: :30.5 -> :31.0
    assert cronstable.cron.next_sleep_interval(True) == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_spawn_jobs_subminute_dedup(monkeypatch):
    # A minute-level job fires exactly once per minute and a second-level job
    # exactly once per matching second, even when the loop wakes more than once
    # in a second (a duplicate tick). The forward-only next-fire index de-dupes
    # structurally: once a slot has fired the job's next fire has already
    # advanced past it. Start-up seeds strictly-future, so the second and
    # minute in progress at start-up are skipped, not fired for a partial run.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SECONDS_JOB)

    launched = []

    async def fake_launch(job):
        launched.append((holder["now"].second, holder["now"].minute, job.name))

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    async def tick(minute, second, *, startup=False):
        holder["now"] = DT(2020, 1, 1, 0, minute, second)
        await cron._service_slots(startup)

    await tick(0, 0, startup=True)  # start-up: seed strictly-future, fire none
    await tick(0, 15)  # sec fires (min not due until :01:00)
    await tick(0, 15)  # duplicate tick in the same second: de-duped
    await tick(0, 30)  # sec only
    await tick(0, 45)  # sec only
    await tick(1, 0)  # new minute: sec + min both fire

    sec_fires = [(m, s) for (s, m, n) in launched if n == "sec"]
    min_fires = [(m, s) for (s, m, n) in launched if n == "min"]
    assert sec_fires == [(0, 15), (0, 30), (0, 45), (1, 0)]
    assert min_fires == [(1, 0)]  # once, despite the ticks through the minute


@pytest.mark.asyncio
async def test_service_slots_catches_up_overrun_seconds(monkeypatch):
    # A pass that overruns by a couple of seconds (the clock jumps forward
    # between passes) must not silently drop the seconds it skipped: the next
    # pass services each skipped whole-second slot too.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)  # startup at :00 seeds, fires none
    assert launched == []
    holder["now"] = DT(2020, 1, 1, 0, 0, 3)  # the :00 pass overran to :03
    await cron._service_slots(startup=False)

    # every skipped second :01, :02, :03 is serviced (the every-second job
    # fires once for each), rather than only :03.
    assert [s for (n, s) in launched if n == "tick"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_service_slots_bounds_catchup_after_long_gap(monkeypatch):
    # A gap larger than CATCHUP_LIMIT is a stall/suspend, not tick overhead:
    # resume at the current second instead of replaying a burst.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)
    gap = int(cronstable.cron.CATCHUP_LIMIT.total_seconds()) + 5
    holder["now"] = DT(2020, 1, 1, 0, 0, gap)
    await cron._service_slots(startup=False)

    # only the current second fires -- no backdated storm of the skipped ones
    assert [s for (n, s) in launched if n == "tick"] == [gap]


@pytest.mark.asyncio
async def test_startup_seeding_skips_in_progress_minute(monkeypatch):
    # Restarting partway through a minute must not fire a minute-level job for
    # the minute already under way, even though a second-level job is present
    # (which forces per-second ticking). Regression: without startup seeding
    # the minute job fired ~1s after a mid-minute restart.
    holder = {"now": DT(2020, 1, 1, 0, 5, 30)}
    cron, launched = _drive_cron(monkeypatch, holder, _SECONDS_JOB)

    await cron._service_slots(startup=True)  # restart at 00:05:30
    holder["now"] = DT(2020, 1, 1, 0, 5, 31)
    await cron._service_slots(startup=False)
    # the in-progress minute is skipped -- "min" must not have fired
    assert "min" not in [n for (n, s) in launched]

    holder["now"] = DT(2020, 1, 1, 0, 6, 0)  # next minute boundary
    await cron._service_slots(startup=False)
    assert ("min", 0) in launched  # now it fires, once, at the fresh boundary


@pytest.mark.asyncio
async def test_single_slot_job_fires_once_across_boundary(monkeypatch):
    # A single-slot job (noon) serviced tick-by-tick across the minute boundary
    # fires exactly once. Regression for the two-clock-read TOCTOU: the due
    # test and the de-dup key are now one and the same read, so the boundary
    # cannot double-launch it.
    holder = {"now": DT(2020, 1, 1, 11, 59, 58)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)

    await cron._service_slots(startup=True)
    for sec in (59, 0, 1, 2):
        minute = 59 if sec == 59 else 0
        hour = 11 if sec == 59 else 12
        holder["now"] = DT(2020, 1, 1, hour, minute, sec)
        await cron._service_slots(startup=False)

    noon_fires = [s for (n, s) in launched if n == "noon"]
    assert noon_fires == [0]  # exactly one launch, at second 0 of 12:00


# =====================================================================
#  concurrent launches + off-loop config reparse
# =====================================================================


@pytest.mark.asyncio
async def test_spawn_jobs_launches_concurrently(monkeypatch):
    # Jobs due in the same slot are launched concurrently, not one at a time:
    # all three enter their (blocking) launch before any of them completes, so
    # a slot's wall time is ~one spawn-time instead of N x spawn-time. Under
    # the old sequential loop only the first launch would ever start (it blocks
    # on `release`), so `started` would never fire and this would time out.
    cron = cronstable.cron.Cron(None, config_yaml=_THREE_DUE)

    order = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_launch(job):
        order.append(job.name)
        if len(order) == 3:
            started.set()
        await release.wait()

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    _seed_due(cron, "a", "b", "c")  # all three due this pass
    task = asyncio.create_task(cron.spawn_jobs(False))
    try:
        # all three launches are in flight at once (would hang if sequential)
        await asyncio.wait_for(started.wait(), timeout=2)
        assert order == ["a", "b", "c"]  # scheduled in config order
    finally:
        release.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_single_due_job_still_launches(monkeypatch):
    # The len == 1 fast path (await directly, no gather) still launches the one
    # due job, so the optimisation does not regress the common single-job slot.
    # Only "alpha" is due here ("beta" is a disabled @reboot one-shot).
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    # alpha is "*/5 * * * *"; the frozen clock is 12:00, a multiple of 5.
    # Seed it due so this single-pass call services it (beta is a disabled
    # @reboot one-shot and never enters the next-fire index).
    _seed_due(cron, "alpha")
    await cron.spawn_jobs(False)
    assert launched == ["alpha"]


@pytest.mark.asyncio
async def test_reload_runs_off_event_loop(tmp_path, monkeypatch):
    # The once-a-minute reparse is offloaded to a worker thread so a slow disk
    # read + parse cannot freeze the event loop (and stall the scheduling
    # tick). Prove every run-loop reparse executes off the event-loop thread.
    import itertools
    import threading

    cfg = tmp_path / "c.yaml"
    cfg.write_text(TWO_JOBS)
    # construction parses once, synchronously, on this (the loop) thread
    cron = cronstable.cron.Cron(str(cfg))
    main_thread = threading.get_ident()

    seen = []
    real_parse = cronstable.cron.parse_config_with_sources

    def recording_parse(arg):
        seen.append(threading.get_ident())
        return real_parse(arg)

    monkeypatch.setattr(
        "cronstable.cron.parse_config_with_sources", recording_parse
    )
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)
    # Force every housekeeping pass to reparse by defeating the
    # unchanged-config skip cache: an ever-incrementing signature never equals
    # the stored one, so reload_config always treats the config as changed and
    # offloads the parse. This test is about WHERE the reparse runs (a worker
    # thread), not about the skip cache's change detection -- which
    # test_run_reloads_changed_config already covers via a real on-disk edit.
    # Driving the reparse this way keeps the test off filesystem timing
    # entirely: relying on real size/mtime changes to trigger successive
    # reparses races the parse->record window (reload_config re-stats the file
    # when recording the parse result, so a second rapid rewrite lands inside
    # that window and is absorbed into the record -- the next reparse never
    # fires). That race is benign in production (reloads are ~60s apart) but is
    # deterministic under this test's 10ms ticks on Windows / Python <= 3.12,
    # whose coarse ~15.6ms asyncio timer lands every rewrite inside the window
    # -- which hung this test in CI.
    _sig_counter = itertools.count()
    monkeypatch.setattr(
        cron, "_config_signature", lambda files: next(_sig_counter)
    )

    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: len(seen) >= 2)  # a couple of reload ticks
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert len(seen) >= 2  # the loop reparsed on each on-disk change
    assert all(t != main_thread for t in seen)  # ...always off the loop thread


_LEADER_REBOOT_BAD_CLUSTER = """
jobs:
  - name: boot
    command: echo boot
    schedule: "@reboot"
    clusterPolicy: Leader
cluster:
  listen: "127.0.0.1:18444"
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
async def test_startup_gates_reboot_before_servicing(tmp_path, monkeypatch):
    # Housekeeping (which sets the cluster gate _elect_leader_configured via
    # start_stop_cluster) must run BEFORE the first spawn_jobs, so a Leader
    # @reboot job is deferred to the elected owner rather than run ungated on
    # every node. This must hold even though the reparse is now offloaded to a
    # worker thread (reload_config): it is still awaited and applied before
    # _service_slots. Here the manager fails to start (bad certs), so the
    # Leader one-shot must stay deferred and fail closed -- never launched.
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LEADER_REBOOT_BAD_CLUSTER)
    cron = cronstable.cron.Cron(str(cfg))

    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 0.01)

    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(lambda: cron._elect_leader_configured)
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)

    assert cron.cluster_manager is None  # bad certs -> no manager
    assert "boot" in cron._pending_reboot_jobs  # deferred, not run ungated
    assert launched == []  # never launched anywhere


# =====================================================================
#  next-fire index + monotonic-sleep behaviour, and a perf demonstration
# =====================================================================

_ONE_MINUTE_JOB = """
jobs:
  - name: m
    command: echo m
    schedule: "* * * * *"
"""

_NOON_DAILY = """
jobs:
  - name: noon
    command: echo noon
    schedule: "0 12 * * *"
"""

_TZ_JOBS = """
jobs:
  - name: utc
    command: echo utc
    schedule: "*/10 * * * *"
  - name: local
    command: echo local
    schedule: "*/10 * * * *"
    utc: false
  - name: la
    command: echo la
    schedule: "*/10 * * * *"
    timezone: America/Los_Angeles
"""


def test_compute_next_fire_is_now_plus_delay_utc(monkeypatch):
    # The index instant is exactly now + the cron engine's delay-to-next-match,
    # the same formula the dashboard countdown and the Prometheus next-run
    # gauge use, so all three agree.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_MINUTE_JOB)
    now = cronstable.cron.get_now(UTC)
    job = cron.cron_jobs["m"]
    delay = job.schedule.next(now=now, default_utc=True)
    assert cron._compute_next_fire(job, now) == now + datetime.timedelta(
        seconds=delay
    )
    # strictly-future: the in-progress minute (:00) is skipped for :01
    assert cron._compute_next_fire(job, now) == DT(
        2020, 1, 1, 0, 1, tzinfo=UTC
    )


def test_compute_next_fire_lands_on_a_matching_slot(monkeypatch):
    # Whatever the timezone, the computed next-fire instant, rendered back into
    # the job's own frame, satisfies the cron expression -- so the heap fires
    # the job exactly when the old test()-based tick would have matched. Uses a
    # */10 schedule so the next fire is minutes away (no DST boundary crossed).
    holder = {"now": DT(2020, 6, 1, 12, 34, 56)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_TZ_JOBS)
    now = cronstable.cron.get_now(UTC)
    for name, job in cron.cron_jobs.items():
        fire = cron._compute_next_fire(job, now)
        assert fire is not None and fire.tzinfo is not None
        assert fire > now
        if job.timezone is not None:
            frame = fire.astimezone(job.timezone)
        else:
            frame = fire.astimezone().replace(tzinfo=None)
        assert job.schedule.test(frame.replace(microsecond=0)), name


def test_sleep_interval_uses_soonest_fire(monkeypatch):
    # The loop sleeps until the soonest job's next fire, not a fixed tick.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(
        None, config_yaml=_SECONDS_JOB
    )  # sec */15 + min
    cron._ensure_seeded(cronstable.cron.get_now(UTC))
    # soonest is the */15 job at :15 -> 15s away (the minute job is 60s away)
    assert cron._sleep_interval() == pytest.approx(15.0, abs=0.05)


def test_sleep_interval_capped_by_housekeeping(monkeypatch):
    # A sparse job hours away still wakes the loop within the next wall-minute,
    # so config reload / cluster upkeep stays ~once a minute.
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(
        None, config_yaml=_NOON_DAILY
    )  # next fire 12:00
    cron._ensure_seeded(cronstable.cron.get_now(UTC))
    # capped at the next minute boundary (03:01:00), i.e. 45s
    assert cron._sleep_interval() == pytest.approx(45.0, abs=0.05)


def test_sleep_interval_no_jobs_uses_housekeeping(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 3, 0, 15)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None)  # nothing scheduled
    assert cron._peek_soonest_fire() is None
    assert cron._sleep_interval() == pytest.approx(45.0, abs=0.05)


@pytest.mark.asyncio
async def test_backward_clock_step_does_not_refire(monkeypatch):
    # The heart of the clock-step immunity: next-fire advances forward-only, so
    # an NTP/clock step BACKWARD defers the next fire rather than re-firing an
    # already-fired slot. The old tick+test scheduler re-matched the earlier
    # second and fired it a second time.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)
    await cron._service_slots(startup=True)  # seed tick->:01, noon->12:00

    holder["now"] = DT(2020, 1, 1, 0, 0, 5)
    await cron._service_slots(startup=False)  # tick fires :01..:05
    assert [s for (n, s) in launched if n == "tick"] == [1, 2, 3, 4, 5]

    launched.clear()
    # the wall clock jumps BACK 3 seconds
    holder["now"] = DT(2020, 1, 1, 0, 0, 2)
    await cron._service_slots(startup=False)
    assert [s for (n, s) in launched if n == "tick"] == []  # no re-fire

    # ...and it resumes cleanly once the clock passes the pending fire again
    holder["now"] = DT(2020, 1, 1, 0, 0, 6)
    await cron._service_slots(startup=False)
    assert [s for (n, s) in launched if n == "tick"] == [6]


_EVERY_15MIN = """
jobs:
  - name: j15
    command: echo j15
    schedule: "*/15 * * * *"
"""


@pytest.mark.asyncio
async def test_long_gap_sparse_job_fires_only_if_current_slot_matches(
    monkeypatch,
):
    # After a gap beyond CATCHUP_LIMIT, a sparse job resumes EXACTLY where the
    # old tick would: it fires only if NOW's own slot matches the schedule, not
    # a stale most-recent occurrence. Regression: an earlier draft fired the
    # most recent missed slot (00:30), backdating a launch the old scheduler
    # never made.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_15MIN)
    await cron._service_slots(startup=True)  # seed j15 -> 00:15:00

    # froze ~37 minutes, resuming at 00:37 -- NOT a */15 slot
    holder["now"] = DT(2020, 1, 1, 0, 37, 0)
    await cron._service_slots(startup=False)
    assert launched == []  # nothing backdated (00:37 is not a */15 slot)

    # ...and it resyncs: the next real fire at 00:45 still happens
    holder["now"] = DT(2020, 1, 1, 0, 45, 0)
    await cron._service_slots(startup=False)
    assert [n for (n, s) in launched] == ["j15"]


@pytest.mark.asyncio
async def test_long_gap_resumes_at_matching_current_slot(monkeypatch):
    # The mirror of the above: when the resume instant DOES land on a matching
    # slot, the job fires once there (the frequently-scheduled / on-boundary
    # case), matching the old "fire the current slot" behaviour.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_15MIN)
    await cron._service_slots(startup=True)  # seed j15 -> 00:15:00
    holder["now"] = DT(2020, 1, 1, 0, 45, 0)  # froze to a */15 boundary
    await cron._service_slots(startup=False)
    assert [(n, s) for (n, s) in launched] == [("j15", 0)]  # once, at 00:45


@pytest.mark.asyncio
async def test_large_forward_jump_does_not_enumerate_window(monkeypatch):
    # A large forward clock jump / long suspend must NOT walk the missed window
    # occurrence-by-occurrence: for a per-second job an 8h gap is ~28,800
    # occurrences (and an RTC-less 1970->now boot is billions), which would
    # block the event loop and exhaust memory. The long-gap branch resumes at
    # the current slot with O(1) crontab work. Regression guard for the
    # review's
    # high-severity finding.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _EVERY_SECOND_AND_MINUTE)
    await cron._service_slots(startup=True)  # seed tick -> 00:00:01
    counts = _count_crontab_calls(monkeypatch)

    holder["now"] = DT(2020, 1, 1, 8, 0, 0)  # jump forward 8 hours
    await cron._service_slots(startup=False)

    # O(1), not ~28,800 (one crontab.next per second of the gap)
    assert counts["next"] <= 3
    # ...and the per-second job still fires once, at the current second
    assert [s for (n, s) in launched if n == "tick"] == [0]


_LOCAL_MINUTE_JOB = """
jobs:
  - name: loc
    command: echo loc
    schedule: "* * * * *"
    utc: false
"""


@pytest.mark.asyncio
async def test_last_run_slot_is_aware_utc_in_both_advance_branches(
    monkeypatch,
):
    # _last_run_slot must never mix naive and aware datetimes. The normal
    # catch-up branch records the aware-UTC next-fire instant; the long-gap
    # branch records schedule_slot(), which is NAIVE local for a utc:false /
    # no-timezone job, so it converts back to UTC before recording. Regression
    # for the review finding: an earlier draft stored the naive slot, leaving
    # _last_run_slot[name] after a long-gap resume mutually incomparable with
    # the value the normal branch stores (a TypeError on any ordering).
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _LOCAL_MINUTE_JOB)
    await cron._service_slots(startup=True)  # seed loc -> next minute boundary

    # normal branch: a short overrun within CATCHUP_LIMIT
    holder["now"] = DT(2020, 1, 1, 0, 1, 2)
    await cron._service_slots(startup=False)
    normal = cron._last_run_slot["loc"]
    assert normal.tzinfo == UTC

    # long-gap branch: a gap beyond CATCHUP_LIMIT resumes at the current slot
    holder["now"] = DT(2020, 1, 1, 0, 30, 0)
    await cron._service_slots(startup=False)
    longgap = cron._last_run_slot["loc"]
    assert longgap.tzinfo == UTC
    # both values are now comparable (a naive one would raise TypeError here)
    assert longgap > normal


def test_reload_utc_to_timezone_utc_preserves_next_fire(tmp_path, monkeypatch):
    # utc:true and an explicit `timezone: UTC` fire on identical instants, so a
    # reconfiguration between them must NOT be treated as a schedule change (an
    # object-identity timezone compare made datetime.timezone.utc != ZoneInfo
    # ("UTC") and forced a reseed that could skip a boundary fire). Regression
    # guard for the review's _same_schedule finding.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)  # utc:true by default
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    # reload AT the boundary with an explicit `timezone: UTC` added
    holder["now"] = DT(2020, 1, 1, 0, 1, 0)
    cfg.write_text(_ONE_MINUTE_JOB.rstrip() + "\n    timezone: UTC\n")
    cron.update_config()
    # kept at 00:01:00 (a spurious reseed would jump it to 00:02:00)
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_minute_job_missed_minutes_fires_once(monkeypatch):
    # A minute-level job whose scheduler froze across several minutes fires
    # ONCE on resume (no backdated storm), matching cron's outage semantics --
    # the per-job catch-up bound (CATCHUP_LIMIT) unifies this with sub-minute
    # catch-up.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    cron, launched = _drive_cron(monkeypatch, holder, _ONE_MINUTE_JOB)
    await cron._service_slots(startup=True)  # seed m -> 00:01:00
    holder["now"] = DT(2020, 1, 1, 0, 5, 30)  # froze ~4.5 minutes
    await cron._service_slots(startup=False)
    assert [n for (n, s) in launched] == ["m"]  # once, not five times


def test_reload_preserves_unchanged_next_fire(tmp_path, monkeypatch):
    # A reload that does NOT change a job's schedule keeps its next-fire, so a
    # reload landing on the job's own boundary minute never recomputes a
    # strictly-future fire and skips that fire.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    # reload AT the boundary minute, same schedule
    holder["now"] = DT(2020, 1, 1, 0, 1, 0)
    cron.update_config()
    # kept at 00:01:00 (a reseed would have jumped it to 00:02:00, dropping the
    # fire due this very minute)
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)


def test_reload_reseeds_changed_schedule(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 1, tzinfo=UTC)

    cfg.write_text(_ONE_MINUTE_JOB.replace('"* * * * *"', '"*/5 * * * *"'))
    cron.update_config()
    # reseeded strictly-future for the NEW schedule
    assert cron._next_fire["m"] == DT(2020, 1, 1, 0, 5, tzinfo=UTC)


def test_reload_refreshes_index(tmp_path, monkeypatch):
    # One reload exercises all three reconciliations: an unchanged job keeps
    # its fire, a removed job leaves the index, a new job is seeded
    # strictly-future.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    keep_before = cron._next_fire["keep"]
    assert set(cron._next_fire) == {"keep", "drop"}

    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    assert set(cron._next_fire) == {"keep", "added"}  # drop gone, added in
    assert cron._next_fire["keep"] == keep_before  # unchanged kept
    assert cron._next_fire["added"] == DT(2020, 1, 1, 0, 5, tzinfo=UTC)


def test_reload_drops_disabled_job(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_ONE_MINUTE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    assert "m" in cron._next_fire

    cfg.write_text(_ONE_MINUTE_JOB.rstrip() + "\n    enabled: false\n")
    cron.update_config()
    assert "m" not in cron._next_fire  # disabled -> not scheduled


def test_reload_prunes_finished_run_maps(tmp_path, monkeypatch):
    # _apply_reload prunes _last_run_slot and the metric series of removed
    # jobs; last_run and run_history must go with them. A removed job's
    # display data is unreachable (every payload guards on cron_jobs
    # membership first), so keeping it is a pure leak -- worst under classic
    # crontabs, whose <file>:<line> job names are reminted by every line
    # added or removed above them.
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    for name in ("keep", "drop"):
        info = cronstable.cron.JobRunInfo(
            outcome="success",
            exit_code=0,
            started_at=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            finished_at=DT(2020, 1, 1, 0, 0, 1, tzinfo=UTC),
            fail_reason=None,
            output=JobOutputStream(),
        )
        cron.last_run[name] = info
        cron.run_history[name].append(info)

    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    assert "drop" not in cron.last_run  # removed job's data pruned
    assert "drop" not in cron.run_history
    assert "keep" in cron.last_run  # surviving job's data kept
    assert len(cron.run_history["keep"]) == 1


def _count_crontab_calls(monkeypatch):
    import cronstable.cronexpr as crontab_mod

    counts = {"test": 0, "next": 0}
    orig_test = crontab_mod.CronTab.test
    orig_next = crontab_mod.CronTab.next

    def counting_test(self, entry):
        counts["test"] += 1
        return orig_test(self, entry)

    def counting_next(self, *a, **k):
        counts["next"] += 1
        return orig_next(self, *a, **k)

    monkeypatch.setattr(crontab_mod.CronTab, "test", counting_test)
    monkeypatch.setattr(crontab_mod.CronTab, "next", counting_next)
    return counts


@pytest.mark.asyncio
async def test_perf_wake_is_o_due_not_o_all(monkeypatch, capsys):
    # PERFORMANCE DEMONSTRATION. The next-fire index turns a wake from
    # O(all jobs) into O(due jobs): over a large fleet, a wake where nothing is
    # due performs ZERO crontab matches (a heap peek), and a wake with a cohort
    # due matches only that cohort -- independent of fleet size. The old
    # tick+test loop called CronTab.test once per job per tick, i.e. O(all).
    N = 2000
    jobs = "\n".join(
        "  - name: j{0}\n    command: echo {0}\n"
        '    schedule: "{1} * * * *"'.format(i, i % 60)
        for i in range(N)
    )
    holder = {"now": DT(2020, 1, 1, 0, 30, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml="jobs:\n" + jobs)

    async def fake_launch(job):
        pass

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)

    # one-time seeding (startup) is O(all); every wake AFTER it is O(due)
    await cron._service_slots(startup=True)
    assert len(cron._next_fire) == N

    counts = _count_crontab_calls(monkeypatch)

    # a wake where nothing is due: no crontab work at all
    holder["now"] = DT(2020, 1, 1, 0, 30, 1)
    await cron._service_slots(startup=False)
    assert counts["test"] == 0  # the O(all) per-tick scan primitive is gone
    assert counts["next"] == 0  # the heap said nothing is due -> no work

    # a wake where the minute-31 cohort (~N/60 jobs) is due
    holder["now"] = DT(2020, 1, 1, 0, 31, 0)
    await cron._service_slots(startup=False)
    due = sum(1 for i in range(N) if i % 60 == 31)
    assert counts["test"] == 0  # still never scans-and-tests the whole fleet
    assert counts["next"] == due  # exactly one advance per due job -> O(due)

    # wall-time: an idle heap wake over N jobs vs the old O(all) test() scan
    idle_reps = 50
    t0 = time.perf_counter()
    for _ in range(idle_reps):
        await cron._service_slots(startup=False)  # nothing due now
    new_idle = (time.perf_counter() - t0) / idle_reps

    sample = next(iter(cron.cron_jobs.values()))
    slot = cronstable.cron.schedule_slot(sample, holder["now"])
    scan_reps = 50
    t0 = time.perf_counter()
    for _ in range(scan_reps):
        for job in cron.cron_jobs.values():  # what the old tick did every wake
            job.schedule.test(slot)
    old_scan = (time.perf_counter() - t0) / scan_reps

    with capsys.disabled():
        print(
            "\n[perf] fleet={0} jobs | idle heap wake {1:.1f}us "
            "vs old O(all) test scan {2:.0f}us  (~{3:.0f}x faster)".format(
                N,
                new_idle * 1e6,
                old_scan * 1e6,
                old_scan / max(new_idle, 1e-12),
            )
        )
    # the idle wake must be dramatically cheaper than scanning every job
    assert new_idle < old_scan


# ---------------------------------------------------------------------------
# runtime pause/resume: the scheduler-side core
# ---------------------------------------------------------------------------


def _launch_recorder(monkeypatch, cron):
    launched = []

    async def fake(job, *, with_retries=True):
        launched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", fake)
    return launched


@pytest.mark.asyncio
async def test_pause_gate_skips_fire_and_writes_skipped_row(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p", note="maint", by="op")
    await cron.launch_scheduled_job(cron.cron_jobs["p"])
    assert launched == []  # the due fire was skipped, not launched
    info = cron.last_run["p"]
    assert info.outcome == "skipped"
    assert info.skip_reason == "paused"
    assert info.started_at is None
    assert info.exit_code is None
    # finished_at is SET (the skip instant): this is what advances the
    # derived catch-up watermark across the pause window.
    assert info.finished_at == DT(2020, 1, 1, 0, 0, 30, tzinfo=UTC)
    assert info.output.closed is True
    # the synthetic row round-trips through the ledger record shape
    restored = cronstable.cron._job_run_info_from_dict(info.to_dict())
    assert restored is not None
    assert restored.outcome == "skipped"
    assert restored.skip_reason == "paused"


@pytest.mark.asyncio
async def test_pause_expiry_resumes_firing(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p", duration=60)
    assert cron._pause_active("p") is not None
    # ... the window ends; expiry is reader-enforced, no resume call needed
    holder["now"] = DT(2020, 1, 1, 0, 2, 0)
    assert cron._pause_active("p") is None
    await cron.launch_scheduled_job(cron.cron_jobs["p"])
    assert launched == ["p"]
    assert "p" not in cron.last_run  # no skipped row for a launched fire


def test_pause_periodic_sweeps_expired_entries(monkeypatch, caplog):
    import logging

    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    cron._paused["p"] = cronstable.cron.PauseInfo(
        since=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
        until=DT(2020, 1, 1, 0, 0, 10, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._pause_periodic()
    assert "p" not in cron._paused
    assert any("pause expired" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_manual_start_allowed_while_paused(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p")
    # a pause skips SCHEDULED fires only: the operator asking by hand is the
    # operator overriding their own pause (unlike the disabled 409).
    await cron.start_job_by_name("p")
    assert launched == ["p"]


@pytest.mark.asyncio
async def test_retry_defers_across_pause_and_fires_after_resume(monkeypatch):
    monkeypatch.setattr(cronstable.cron, "RETRY_GATE_RECHECK_FLOOR", 0.02)
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    launched = _launch_recorder(monkeypatch, cron)
    await cron.pause_job_by_name("p")
    state = JobRetryState(0.01, 2, 60)
    state.next_delay()
    cron.retry_state["p"] = state
    task = asyncio.create_task(cron.schedule_retry_job("p", 0.0, 1))
    state.task = task
    # the due attempt DEFERS while paused: neither launched nor cancelled
    await asyncio.sleep(0.2)
    assert launched == []
    assert "p" in cron.retry_state
    await cron.resume_job_by_name("p")
    await asyncio.wait_for(task, timeout=5)
    assert launched == ["p"]


def test_reload_keeps_pause_and_prunes_removed_jobs(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    for name in ("keep", "drop"):
        cron._paused[name] = cronstable.cron.PauseInfo(
            since=DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            until=DT(2020, 1, 1, 6, 0, 0, tzinfo=UTC),
            note="",
            by="op",
            channel="api",
        )
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # pause survives the reload for the surviving job (a config edit does
    # not clear it: no digest check, unlike retries)...
    assert cron._pause_active("keep") is not None
    # ...and the paused job STAYS in the fire heap, so its slots keep being
    # skipped and the watermark keeps advancing.
    assert "keep" in cron._next_fire
    # the removed job's entry is pruned, not leaked
    assert "drop" not in cron._paused


@pytest.mark.asyncio
async def test_catch_up_defers_a_paused_job_instead_of_latching_it(
    monkeypatch,
):
    # A pause is transient and excuses only the slots inside its own window,
    # so catch-up must DEFER a paused job (like a transient cluster denial),
    # never latch it done: latching forfeits a backlog owed from before the
    # pause began, and catch-up is one-shot per process.
    holder = {"now": DT(2020, 1, 1, 0, 10, 0)}
    _set_now(monkeypatch, holder)
    yaml = (
        "jobs:\n  - name: p\n    command: echo hi\n"
        '    schedule: "* * * * *"\n    onMissed: run-all\n'
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    await cron.pause_job_by_name("p")
    unresolved = await cron._evaluate_catch_up(
        DT(2020, 1, 1, 0, 10, 0, tzinfo=UTC)
    )
    assert unresolved is True
    assert "p" not in cron._catchup_done
    assert not cron._catchup_tasks
    # once the pause lifts the job is evaluated for real
    await cron.resume_job_by_name("p")
    unresolved = await cron._evaluate_catch_up(
        DT(2020, 1, 1, 0, 10, 0, tzinfo=UTC)
    )
    assert unresolved is False
    assert "p" in cron._catchup_done


@pytest.mark.asyncio
async def test_origin_middleware_covers_pause_and_resume_routes():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_origin_middleware(frozenset())

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, path):
            self.method = "POST"
            self.headers = {"Origin": "https://evil.example"}
            self.host = "localhost:8021"
            self.path = path

    # the new mutating routes get the same CSRF/origin gate as start/cancel
    for path in ("/jobs/x/pause", "/jobs/x/resume"):
        with pytest.raises(web.HTTPForbidden):
            await middleware(FakeRequest(path), handler)


# ---------------------------------------------------------------------------
# per-job SLA monitor (_sla_periodic) and onLate dispatch
# ---------------------------------------------------------------------------


_SLA_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
"""


_SLA_EXEMPT_JOBS = """
jobs:
  - name: pd
    command: echo hi
    schedule: "* * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 60
  - name: dd
    command: echo hi
    schedule: "* * * * *"
    enabled: false
    sla:
      maxTimeSinceSuccessSeconds: 60
"""


@pytest.mark.asyncio
async def test_pause_and_sla_pass_survives_a_broken_config(monkeypatch):
    # The pause sweep and the SLA monitor must NOT share run()'s reload
    # try/except: a broken config file on disk raises out of reload_config,
    # which would skip every later statement in that block. Going quiet about
    # jobs that stopped running is the exact failure late-run detection
    # exists to report, so the pass is guarded on its own.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)

    # a reload that raises must not stop the pass from latching the breach
    def boom():
        raise cronstable.config.ConfigError("bad yaml on disk")

    monkeypatch.setattr(cron, "reload_config", boom)
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._pause_and_sla_periodic()
    assert cron._sla_state[("s", STALE)] == DT(
        2020, 1, 1, 13, 30, 0, tzinfo=UTC
    )
    assert cron.metrics._job("s").sla_late == {STALE: 1}


@pytest.mark.asyncio
async def test_sla_stale_check_breaches_and_clears(monkeypatch, caplog):
    import logging

    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    # within threshold: no latch, but the series exist at 0 from the first
    # evaluation (so increase() has a baseline before the first breach)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert cron.metrics._job("s").sla_breaches == {STALE: 0}

    # the success ages past the threshold: latch, gauge, counter, warning
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        cron._sla_periodic()
    assert cron._sla_state[("s", STALE)] == DT(2020, 1, 1, 13, 30, 0,
                                               tzinfo=UTC)
    assert cron.metrics._job("s").sla_late == {STALE: 1}
    assert cron.metrics._job("s").sla_breaches == {STALE: 1}
    assert any("SLA check" in r.getMessage() for r in caplog.records)

    # a recorded success (any path: _record_run feeds the tracker) clears it
    caplog.clear()
    info = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=DT(2020, 1, 1, 13, 31, 0, tzinfo=UTC),
        finished_at=DT(2020, 1, 1, 13, 31, 5, tzinfo=UTC),
        fail_reason=None,
        output=JobOutputStream(),
    )
    cron._record_run("s", info)
    assert cron._sla_last_success["s"] == info.finished_at
    holder["now"] = DT(2020, 1, 1, 13, 32, 0)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._sla_periodic()
    assert ("s", STALE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert any("recovered" in r.getMessage() for r in caplog.records)
    # the breach fired onLate exactly once, on the ok-to-breached transition
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_late_after_check_breaches_and_clears(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    _sla_report_recorder(monkeypatch)

    due = DT(2020, 1, 1, 11, 55, 0, tzinfo=UTC)
    cron._sla_due["s"] = due
    # a start BEFORE the due slot does not excuse it
    cron._sla_last_start["s"] = DT(2020, 1, 1, 11, 50, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 1}

    # any actual launch at/after the due slot clears it on the next pass
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 30, tzinfo=UTC)
    holder["now"] = DT(2020, 1, 1, 12, 1, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_late_after_within_grace_is_not_breached(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 1, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    _sla_report_recorder(monkeypatch)
    # 60s past the slot, threshold 120: within the grace window
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state


@pytest.mark.asyncio
async def test_sla_max_runtime_check_breaches_and_clears(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)

    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_periodic()
    assert ("s", RUNTIME) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {RUNTIME: 1}
    await cron._drain_completions()
    (ctx, _), = reports
    assert ctx.sla_check == RUNTIME
    assert ctx.observed_seconds == 1800.0
    assert ctx.threshold_seconds == 600

    # the run ends: the check observes nothing running and clears (the
    # monitor never kills anything)
    cron.running_jobs["s"] = []
    cron._sla_periodic()
    assert ("s", RUNTIME) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {RUNTIME: 0}
    await cron._drain_completions()
    assert len(reports) == 1  # the clear reports nothing


@pytest.mark.asyncio
async def test_sla_latch_fires_onlate_exactly_once(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    cron._sla_periodic()
    holder["now"] = DT(2020, 1, 1, 12, 1, 0)
    cron._sla_periodic()  # still breached: the latch holds, no re-report
    await cron._drain_completions()

    assert len(reports) == 1
    ctx, report_config = reports[0]
    assert ctx.config is cron.cron_jobs["s"]
    assert ctx.sla_check == STALE
    assert ctx.threshold_seconds == 3600
    assert ctx.observed_seconds == 7200.0
    assert ctx.last_success_at == "2020-01-01T10:00:00+00:00"
    assert report_config is cron.cron_jobs["s"].onLate["report"]
    # latched once: the counter shows one incident, not one per pass
    assert cron.metrics._job("s").sla_breaches == {STALE: 1}


@pytest.mark.asyncio
async def test_sla_paused_and_disabled_jobs_are_exempt(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_EXEMPT_JOBS)
    reports = _sla_report_recorder(monkeypatch)
    await cron.pause_job_by_name("pd", duration=7200)

    # both jobs are far past the 60s threshold (no success on record and
    # the process is an hour old), but paused/disabled are excused
    holder["now"] = DT(2020, 1, 1, 13, 0, 0)
    cron._sla_periodic()
    assert not cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # resume: the same condition latches again, proving it was real. The
    # hour it spent paused is credited against the staleness measurement
    # (see test_sla_pause_time_is_credited_against_staleness), so the clock
    # has to move past the threshold once more before it can page.
    await cron.resume_job_by_name("pd")
    cron._sla_periodic()
    assert not cron._sla_state
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._sla_periodic()
    assert ("pd", STALE) in cron._sla_state
    assert ("dd", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_cluster_gate_is_per_job(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 13, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    cron._sla_periodic()
    assert not cron._sla_state  # not the owner: not evaluated, no page

    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_restart_baseline_without_a_store(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)

    # stateless boot, no success ever known: baselined on process start,
    # so the check does NOT page instantly...
    cron._sla_periodic()
    assert not cron._sla_state

    # ...nor within the threshold...
    holder["now"] = DT(2020, 1, 1, 12, 30, 0)
    cron._sla_periodic()
    assert not cron._sla_state

    # ...but it ages into the breach like a normal miss
    holder["now"] = DT(2020, 1, 1, 13, 30, 0)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warmed_last_success_drives_the_check(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    # a durable ledger rehydrate warmed the real last success (see
    # _rehydrate_from_state): a genuinely stale job pages right after
    # boot, process-start grace does not apply
    cron._sla_last_success["s"] = DT(2020, 1, 1, 9, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()
    (ctx, _), = reports
    assert ctx.last_success_at == "2020-01-01T09:00:00+00:00"


@pytest.mark.asyncio
async def test_sla_due_slot_excused_while_paused(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)

    async def fake_launch(job):
        return None

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    job = cron.cron_jobs["s"]

    await cron.pause_job_by_name("s")
    slot = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    await cron._launch_plan([(job, [slot])])
    # the pause-skipped slot is excused from lateAfter, but the slot
    # bookkeeping itself still advances
    assert "s" not in cron._sla_due
    assert cron._last_run_slot["s"] == slot

    await cron.resume_job_by_name("s")
    slot2 = DT(2020, 1, 1, 12, 1, 0, tzinfo=UTC)
    await cron._launch_plan([(job, [slot2])])
    assert cron._sla_due["s"] == slot2


@pytest.mark.asyncio
async def test_sla_last_start_set_on_actual_launch(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)

    class FakeRunningJob:
        def __init__(self, config, retry_state, **kwargs):
            self.config = config
            self.started_at = None

        async def start(self):
            pass

    monkeypatch.setattr(cronstable.cron, "RunningJob", FakeRunningJob)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    assert "s" not in cron._sla_last_start
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is True
    assert cron._sla_last_start["s"] == DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_sla_payload_shape(monkeypatch):
    holder = {"now": DT(2020, 1, 1, 12, 30, 0)}
    _set_now(monkeypatch, holder)
    yaml = (
        "jobs:\n"
        "  - name: s\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "    sla:\n"
        "      maxTimeSinceSuccessSeconds: 3600\n"
        "      lateAfterSeconds: 120\n"
        "  - name: plain\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)

    # no sla block: no "sla" key at all
    plain = cron._job_to_dict("plain", cron.cron_jobs["plain"])
    assert "sla" not in plain

    # configured, nothing latched: thresholds only carry the non-null keys
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"] == {
        "thresholds": {
            "maxTimeSinceSuccessSeconds": 3600,
            "lateAfterSeconds": 120,
        },
        "state": "ok",
        "breaches": [],
    }

    # one latched breach: state flips and the entry carries the latch
    # instant with a LIVE observed value (measured at payload time)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)
    cron._sla_state[("s", STALE)] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"]["state"] == "late"
    assert payload["sla"]["breaches"] == [
        {
            "check": STALE,
            "since": "2020-01-01T12:00:00+00:00",
            "observed_seconds": 9000.0,
            "threshold_seconds": 3600,
        }
    ]


@pytest.mark.asyncio
async def test_sla_dropped_check_latch_is_cleared(monkeypatch):
    # a reload can drop one check while keeping the sla block; its stale
    # latch must clear instead of showing late forever
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_state[("s", RUNTIME)] = DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", RUNTIME) not in cron._sla_state
    assert cron.metrics._job("s").sla_late[RUNTIME] == 0
    await cron._drain_completions()


def test_reload_prunes_sla_trackers(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    at = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    for name in ("keep", "drop"):
        cron._sla_last_success[name] = at
        cron._sla_due[name] = at
        cron._sla_last_start[name] = at
        cron._sla_state[(name, STALE)] = at
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # the surviving job's trackers survive the edit (history is history)...
    assert cron._sla_last_success["keep"] == at
    assert cron._sla_due["keep"] == at
    assert cron._sla_last_start["keep"] == at
    assert ("keep", STALE) in cron._sla_state
    # ...and the removed job's are pruned, latch included
    assert "drop" not in cron._sla_last_success
    assert "drop" not in cron._sla_due
    assert "drop" not in cron._sla_last_start
    assert ("drop", STALE) not in cron._sla_state


# ---------------------------------------------------------------------------
# SLA: exemption clears the latch, and false lateAfter pages
# ---------------------------------------------------------------------------

_SLA_CLUSTER_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    concurrencyScope: cluster
    concurrencyPolicy: Forbid
    sla:
      lateAfterSeconds: 120
"""

_SLA_FORBID_LATE_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "*/10 * * * *"
    concurrencyPolicy: Forbid
    sla:
      lateAfterSeconds: 300
"""


@pytest.mark.asyncio
async def test_sla_pause_clears_a_latch_taken_before_the_pause(monkeypatch):
    # regression (#17/#33/#38): a job that latched a breach and is THEN
    # paused was skipped whole by the monitor, so cronstable_job_late, the
    # /jobs sla block and the OVERDUE chip stayed pinned at breached for the
    # entire pause window, for a job the operator deliberately silenced.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)

    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 1}

    # the pause drops the latch on the API call itself, not a minute later
    await cron.pause_job_by_name("s", duration=86400)
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    payload = cron._job_to_dict("s", cron.cron_jobs["s"])
    assert payload["sla"]["state"] == "ok"
    assert payload["paused"] is not None

    # and the monitor keeps it clear for the whole window
    holder["now"] = DT(2020, 1, 1, 23, 0, 0)
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_latch_clears_when_disabled_or_not_owned(monkeypatch):
    # regression (#17): the same freeze through the other two exemptions.
    # Disabling a job, or losing it to another node under election, must
    # drop its latch rather than leave the gauge asserting a live breach.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state

    cron.cron_jobs["s"].enabled = False
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}

    # re-enabled and still breaching: it re-latches and pages once
    cron.cron_jobs["s"].enabled = True
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state

    # losing ownership excuses it the same way, and drops the lateAfter
    # reference with it so regaining ownership cannot page for a slot the
    # owner of the day ran on time
    cron._sla_due["s"] = DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    cron._sla_periodic()
    assert not cron._sla_state
    assert cron.metrics._job("s").sla_late == {STALE: 0}
    assert "s" not in cron._sla_due
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_due_is_only_recorded_by_the_owning_node(monkeypatch):
    # regression (#18): _launch_plan recorded the due slot BEFORE the
    # ownership gate, so a follower accumulated due slots it never launched
    # and had no matching _sla_last_start. The first leader failover then
    # paged a false lateAfter breach on the incoming owner, for slots the
    # dead leader had run on time.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    launched = []

    async def fake_launch(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake_launch)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)

    job = cron.cron_jobs["s"]
    for minute in (55, 56, 57, 58, 59):
        slot = DT(2020, 1, 1, 11, minute, 0, tzinfo=UTC)
        await cron._launch_plan([(job, [slot])])
    assert launched == []
    # the follower launched nothing, so it owes nothing: no due reference
    assert "s" not in cron._sla_due
    # ...but the slot bookkeeping the status payload reads still advances
    assert cron._last_run_slot["s"] == DT(2020, 1, 1, 11, 59, 0, tzinfo=UTC)

    # this node wins the election three minutes later: nothing to page for
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    holder["now"] = DT(2020, 1, 1, 12, 3, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_due_excused_when_a_peer_holds_the_cluster_slot(monkeypatch):
    # regression (#16): a node that records the slot as due and is then
    # denied the cluster concurrency slot by a LIVE peer never launches, so
    # its _sla_last_start never advances and lateAfter latches on every node
    # that lost the race, for a job the fleet is running normally.
    import cronstable.state as state_mod

    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_CLUSTER_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _PeerHeldBackend:
        async def read_lease(self, name):
            return state_mod.Lease(
                name=name, holder="peer#1", fence=1, expires_at=1e12
            )

    async def no_reason():
        return None

    async def denied(backend, lease_name):
        return None

    cron.state_backend = _PeerHeldBackend()
    cron._state_configured = True
    monkeypatch.setattr(cron, "_slot_fidelity_reason", no_reason)
    monkeypatch.setattr(cron, "_acquire_slot_lease", denied)

    cron._sla_due["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False
    assert "s" not in cron._sla_due

    holder["now"] = DT(2020, 1, 1, 12, 5, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_late_after_excused_while_an_instance_runs(monkeypatch):
    # regression (#23): a slot Forbid dropped because the previous instance is
    # STILL RUNNING is not a late slot, so one healthy long run must not page
    # lateAfter (which, with maxRuntime also set, paged it twice). The excuse
    # is now RECORDED by maybe_launch_job's Forbid drop (it pops the due slot),
    # not only inferred live from running_jobs -- see the residual test below.
    holder = {"now": DT(2020, 1, 1, 12, 16, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_FORBID_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)

    # the 12:10 slot fires while the 12:00 instance still runs: _launch_plan
    # records it as due, and the Forbid drop in maybe_launch_job pops it
    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False
    assert "s" not in cron._sla_due
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    assert cron.metrics._job("s").sla_late == {LATE: 0}
    await cron._drain_completions()
    assert reports == []

    # the live running_jobs guard still excuses a due slot recorded for a
    # round while an instance is running (belt-and-braces alongside the pop)
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []


@pytest.mark.asyncio
async def test_sla_late_after_forbid_drop_survives_the_run_ending(monkeypatch):
    # regression (#23 residual): the running_jobs guard only excused the
    # dropped slot WHILE the instance was alive; once the run ended and the
    # reaper emptied running_jobs, a stale _sla_due from the Forbid-dropped
    # slot latched lateAfter and dispatched onLate, in the window between the
    # run finishing and the next slot launching. Recording the excuse (popping
    # _sla_due in maybe_launch_job's Forbid drop) makes it survive the ending.
    holder = {"now": DT(2020, 1, 1, 12, 16, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_FORBID_LATE_JOB)
    reports = _sla_report_recorder(monkeypatch)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)

    # the 12:00 run is still going; the 12:10 slot fires and is Forbid-dropped
    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 10, 0, tzinfo=UTC)
    cron._sla_last_start["s"] = DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert await cron.maybe_launch_job(cron.cron_jobs["s"]) is False

    # the run ends and the reaper empties running_jobs; without the pop the
    # stale 12:10 due latches lateAfter here (observed 420s > 300s)
    cron.running_jobs["s"] = []
    holder["now"] = DT(2020, 1, 1, 12, 17, 0)
    cron._sla_periodic()
    assert ("s", LATE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # a genuinely unserved slot -- recorded for a round with no running
    # instance and never launched -- still latches lateAfter normally
    cron._sla_due["s"] = DT(2020, 1, 1, 12, 20, 0, tzinfo=UTC)
    holder["now"] = DT(2020, 1, 1, 12, 26, 0)
    cron._sla_periodic()
    assert ("s", LATE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


# ---------------------------------------------------------------------------
# SLA: the maxTimeSinceSuccess staleness baseline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sla_first_seen_baselines_a_reload_added_job(
    tmp_path, monkeypatch
):
    # regression (#19): a job ADDED by a reload was baselined on process
    # start, so on a long-running daemon it paged maxTimeSinceSuccess on the
    # very tick it appeared, before it had any chance to run.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "jobs:\n  - name: other\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
    )
    cron = cronstable.cron.Cron(str(cfg))
    _sla_report_recorder(monkeypatch)

    # a week of uptime, then the operator adds a job with a 25h threshold
    holder["now"] = DT(2020, 1, 8, 9, 0, 0)
    cfg.write_text(
        "jobs:\n  - name: other\n    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "  - name: db-vacuum\n    command: echo hi\n"
        '    schedule: "0 3 * * *"\n'
        "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
    )
    cron.update_config()
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 8, 9, 0, 0, tzinfo=UTC
    )
    cron._sla_periodic()
    assert not cron._sla_state

    # it ages into the breach from when it appeared, like any other job
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()


def test_reload_prunes_sla_first_seen_and_pause_windows(tmp_path, monkeypatch):
    holder = {"now": DT(2020, 1, 1, 0, 0, 30)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_RELOAD_BEFORE)
    cron = cronstable.cron.Cron(str(cfg))
    at = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert set(cron._sla_first_seen) == {"keep", "drop"}
    for name in ("keep", "drop"):
        cron._sla_pause_windows[name] = [(at, at)]
    holder["now"] = DT(2020, 1, 1, 0, 1, 30)
    cfg.write_text(_RELOAD_AFTER)
    cron.update_config()
    # the removed job's entries are pruned, and the job the reload ADDED gets
    # its own first-seen baseline rather than inheriting the process start
    assert set(cron._sla_first_seen) == {"keep", "added"}
    assert cron._sla_first_seen["keep"] == DT(2020, 1, 1, 0, 0, 30, tzinfo=UTC)
    assert cron._sla_first_seen["added"] == DT(
        2020, 1, 1, 0, 1, 30, tzinfo=UTC
    )
    assert set(cron._sla_pause_windows) == {"keep"}


_SLA_DISABLED_STALE_JOB = (
    "jobs:\n  - name: db-vacuum\n    command: echo hi\n"
    '    schedule: "0 3 * * *"\n'
    "    enabled: false\n"
    "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
)

_SLA_ENABLED_STALE_JOB = (
    "jobs:\n  - name: db-vacuum\n    command: echo hi\n"
    '    schedule: "0 3 * * *"\n'
    "    sla:\n      maxTimeSinceSuccessSeconds: 90000\n"
)


@pytest.mark.asyncio
async def test_sla_reenabled_after_a_disabled_span_gets_a_fresh_baseline(
    tmp_path, monkeypatch
):
    # regression (#19 residual): a job present at boot with enabled: false and
    # re-enabled by a later reload kept the process-start baseline it entered
    # with, so it paged maxTimeSinceSuccess instantly for the whole disabled
    # span before it had any chance to run. A disabled job cannot run, so its
    # staleness baseline must roll forward while it is switched off -- the same
    # credit a pause banks -- and it should age in only AFTER re-enabling.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_SLA_DISABLED_STALE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    reports = _sla_report_recorder(monkeypatch)
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 1, 9, 0, 0, tzinfo=UTC
    )

    # a week switched off: each housekeeping tick rolls the baseline forward,
    # and a disabled job never pages
    for day in range(2, 9):
        holder["now"] = DT(2020, 1, day, 9, 0, 0)
        cron._sla_periodic()
        assert not cron._sla_state
    assert cron._sla_first_seen["db-vacuum"] == DT(
        2020, 1, 8, 9, 0, 0, tzinfo=UTC
    )

    # the operator re-enables it; the first tick after must NOT page for the
    # week it was deliberately off
    holder["now"] = DT(2020, 1, 8, 9, 1, 0)
    cfg.write_text(_SLA_ENABLED_STALE_JOB)
    cron.update_config()
    assert cron.cron_jobs["db-vacuum"].enabled is True
    cron._sla_periodic()
    assert ("db-vacuum", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []

    # ...but it ages into the breach a full threshold (25h) after re-enabling,
    # from when it was turned on rather than from process start
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


@pytest.mark.asyncio
async def test_sla_reenabled_after_a_disabled_span_credits_a_prior_success(
    tmp_path, monkeypatch
):
    # regression (#19 residual, the previously-succeeded arm): a job that HAD
    # recorded a success, then sat disabled longer than
    # maxTimeSinceSuccessSeconds, then re-enabled, paged the whole disabled
    # span instantly on the first tick. The _sla_first_seen roll-forward only
    # reaches the never-succeeded arm; _sla_stale_reference here returns the
    # week-old _sla_last_success. The disabled span must be banked as a
    # staleness credit (the same #22 pause-credit machinery) so a job the
    # operator deliberately switched off does not page for that span.
    holder = {"now": DT(2020, 1, 1, 9, 0, 0)}
    _set_now(monkeypatch, holder)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_SLA_DISABLED_STALE_JOB)
    cron = cronstable.cron.Cron(str(cfg))
    reports = _sla_report_recorder(monkeypatch)
    # the job succeeded once, an hour before the first disabled housekeeping
    # tick records the disabled-span start
    cron._sla_last_success["db-vacuum"] = DT(2020, 1, 1, 8, 0, 0, tzinfo=UTC)

    # a week switched off: a disabled job never pages, and the disabled-span
    # start is banked from the first disabled tick (2020-01-01 09:00)
    for day in range(1, 9):
        holder["now"] = DT(2020, 1, day, 9, 0, 0)
        cron._sla_periodic()
        assert not cron._sla_state
    assert cron._sla_disabled_since["db-vacuum"] == DT(
        2020, 1, 1, 9, 0, 0, tzinfo=UTC
    )

    # the operator re-enables it; the first tick must NOT page for the week it
    # was off: the disabled span is banked as a credit against the old success
    holder["now"] = DT(2020, 1, 8, 9, 1, 0)
    cfg.write_text(_SLA_ENABLED_STALE_JOB)
    cron.update_config()
    assert cron.cron_jobs["db-vacuum"].enabled is True
    cron._sla_periodic()
    assert ("db-vacuum", STALE) not in cron._sla_state
    await cron._drain_completions()
    assert reports == []
    # the span was banked and the tracker cleared at the transition
    assert "db-vacuum" not in cron._sla_disabled_since
    assert cron._sla_pause_windows["db-vacuum"] == [
        (
            DT(2020, 1, 1, 9, 0, 0, tzinfo=UTC),
            DT(2020, 1, 8, 9, 1, 0, tzinfo=UTC),
        )
    ]

    # ...but it still ages into the breach a full threshold (25h) after
    # re-enabling, measured from re-enable rather than the old success
    holder["now"] = DT(2020, 1, 9, 10, 30, 0)
    cron._sla_periodic()
    assert ("db-vacuum", STALE) in cron._sla_state
    await cron._drain_completions()
    assert len(reports) == 1


def test_sla_bank_pause_coalesces_out_of_order_overlapping_windows():
    # regression (#19 residual, the overlap arm): a job can be disabled first
    # (older since) and paused later (newer since), and _pause_periodic banks
    # the pause BEFORE _sla_periodic banks the older disabled span, so windows
    # reach _sla_bank_pause out of `since` order. The old merge only extended
    # the newest span's END, so the earlier disabled stretch was dropped and
    # the job paged the whole switched-off span on re-enable. The banked spans
    # must be the true disjoint union regardless of arrival order.
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    def pause(since, until):
        return cronstable.cron.PauseInfo(
            since=since, until=until, note="", by="", channel="test"
        )

    # the later pause is banked first (newer since), then the older disabled
    # span that overlaps it: [10:00,10:05] then [09:00,10:02]
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 10, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 5, tzinfo=UTC)),
        DT(2020, 1, 8, 10, 5, tzinfo=UTC),
    )
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 9, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 2, tzinfo=UTC)),
        DT(2020, 1, 8, 10, 2, tzinfo=UTC),
    )
    # one coalesced span covering the whole union, not just the pause
    assert cron._sla_pause_windows["s"] == [
        (DT(2020, 1, 8, 9, 0, tzinfo=UTC), DT(2020, 1, 8, 10, 5, tzinfo=UTC))
    ]
    # and the credit is the full 65 minutes, counted once (no double-count of
    # the shared 10:00..10:02 stretch, no loss of the 09:00..10:00 stretch)
    credit = cron._sla_paused_seconds(
        "s",
        DT(2020, 1, 8, 8, 0, tzinfo=UTC),
        DT(2020, 1, 8, 11, 0, tzinfo=UTC),
    )
    assert credit == 65 * 60

    # a third window overlapping TWO existing spans coalesces them all
    cron._sla_bank_pause(
        "s",
        pause(DT(2020, 1, 8, 8, 30, tzinfo=UTC), DT(2020, 1, 8, 12, 0, tzinfo=UTC)),
        DT(2020, 1, 8, 12, 0, tzinfo=UTC),
    )
    assert cron._sla_pause_windows["s"] == [
        (DT(2020, 1, 8, 8, 30, tzinfo=UTC), DT(2020, 1, 8, 12, 0, tzinfo=UTC))
    ]


@pytest.mark.asyncio
async def test_sla_pause_time_is_credited_against_staleness(monkeypatch):
    # regression (#22): the staleness clock ran at full rate across a pause,
    # so an unattended job paged the first pass after the window expired,
    # for time the operator had declared it should not run.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    # paused for four hours, well past the 1h threshold
    await cron.pause_job_by_name("s", duration=14400)
    holder["now"] = DT(2020, 1, 1, 4, 0, 1)
    cron._pause_and_sla_periodic()
    assert "s" not in cron._paused  # the window was swept
    assert not cron._sla_state  # ...and did not page as it lifted

    # the credit is exactly the window: half a threshold later, still quiet
    holder["now"] = DT(2020, 1, 1, 4, 30, 1)
    cron._sla_periodic()
    assert not cron._sla_state

    # a full threshold after the resume it pages, as a stale job should
    holder["now"] = DT(2020, 1, 1, 5, 0, 30)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_pause_credit_never_counts_a_window_twice(monkeypatch):
    # regression (#22): repeated and OVERLAPPING pauses must each be
    # credited once. Re-pausing a paused job replaces the window, so the
    # stretch the two share would otherwise be banked by both.
    holder = {"now": DT(2020, 1, 1, 0, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)

    # pause 00:00 to 02:00, then re-pause at 01:00 for another two hours
    await cron.pause_job_by_name("s", duration=7200)
    holder["now"] = DT(2020, 1, 1, 1, 0, 0)
    await cron.pause_job_by_name("s", duration=7200)
    holder["now"] = DT(2020, 1, 1, 3, 0, 0)
    await cron.resume_job_by_name("s")
    # 00:00 to 03:00 held once, not 2h + 2h
    assert cron._sla_pause_windows["s"] == [
        (
            DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC),
            DT(2020, 1, 1, 3, 0, 0, tzinfo=UTC),
        )
    ]

    # a second, disjoint pause banks its own span
    holder["now"] = DT(2020, 1, 1, 3, 30, 0)
    await cron.pause_job_by_name("s", duration=1800)
    holder["now"] = DT(2020, 1, 1, 4, 0, 0)
    await cron.resume_job_by_name("s")
    assert len(cron._sla_pause_windows["s"]) == 2

    # 4h elapsed, 3.5h of it paused: half an hour of real staleness
    now = DT(2020, 1, 1, 4, 0, 0, tzinfo=UTC)
    obs = cron._sla_observations("s", cron.cron_jobs["s"], now)
    assert obs[STALE] == (3600, 1800.0, False)

    # and the credit retires once a success moves the reference past it
    info = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=now,
        finished_at=now,
        fail_reason=None,
        output=JobOutputStream(),
    )
    cron._record_run("s", info)
    holder["now"] = DT(2020, 1, 1, 5, 30, 0)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_pause_credit_covers_a_window_spanning_a_restart(
    monkeypatch,
):
    # regression (#22): a pause rehydrated from the store after a restart
    # carries its original `since`, so the part of the window that elapsed
    # before the restart is credited too.
    holder = {"now": DT(2020, 1, 1, 6, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    # the ledger warm supplies a last success from before the pause began
    cron._sla_last_success["s"] = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    # ...and the pause refresh a window that started five hours ago
    cron._paused["s"] = cronstable.cron.PauseInfo(
        since=DT(2020, 1, 1, 1, 0, 0, tzinfo=UTC),
        until=DT(2020, 1, 1, 6, 30, 0, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )
    holder["now"] = DT(2020, 1, 1, 6, 30, 0)
    cron._pause_and_sla_periodic()
    # 6.5h since the success, 5.5h of it paused: an hour of real staleness,
    # exactly at the threshold rather than six times past it
    assert "s" not in cron._paused
    assert not cron._sla_state
    holder["now"] = DT(2020, 1, 1, 7, 0, 30)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


# ---------------------------------------------------------------------------
# SLA: warming the staleness reference from the durable ledger
# ---------------------------------------------------------------------------


class _RecordBackend:
    """A ledger stub for the rehydrate warm-up: run records, nothing else."""

    def __init__(self, records):
        self._records = list(records)
        self.reads = []

    async def list_records(self, stream, limit=None, newest_first=False):
        self.reads.append((stream, limit))
        if not stream.startswith("runs/"):
            return []
        recs = sorted(
            self._records, key=lambda r: r["_seq"], reverse=newest_first
        )
        return recs[: limit or len(recs)]

    async def list_stream_names(self, prefix):
        return []


def _run_record(seq, outcome, finished_at):
    return {
        "_seq": seq,
        "outcome": outcome,
        "exit_code": 0 if outcome == "success" else 1,
        "started_at": finished_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "fail_reason": None,
    }


async def _warm_ledger(cron, records):
    backend = _RecordBackend(records)
    cron.state_backend = backend
    cron._state_rehydrated = False
    await cron._rehydrate_from_state()
    return backend


@pytest.mark.asyncio
async def test_sla_warm_takes_the_newest_success_by_finished_at(monkeypatch):
    # regression (#21): the warm walked the ledger by APPEND position, but
    # record files are named on write time and run-record writes are
    # unserialized, so the last-appended success can be older than one
    # appended before it. The reference must be the newest by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    await _warm_ledger(
        cron,
        [
            _run_record(1, "success", DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)),
            _run_record(2, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
        ],
    )
    assert cron._sla_last_success["s"] == DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_sla_warm_widens_past_a_success_free_window(monkeypatch):
    # regression (#20): a job failing more often than the warm window is
    # wide has no success among the newest RUN_HISTORY_LIMIT records, and
    # the reference was left unset, re-baselining maxTimeSinceSuccess on
    # process start: every restart bought a genuinely stale job another
    # silent threshold.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    success_at = DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC)
    records = [_run_record(0, "success", success_at)]
    records += [
        _run_record(
            n,
            "failure",
            DT(2019, 12, 30, 0, 0, 0, tzinfo=UTC)
            + datetime.timedelta(minutes=5 * n),
        )
        for n in range(1, 61)
    ]
    backend = await _warm_ledger(cron, records)
    # the restart does not buy it another silent threshold
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron._sla_last_success["s"] == success_at
    # exactly one deep re-read, on top of the ordinary warm read
    assert len([r for r in backend.reads if r[0] == "runs/s"]) == 2
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warm_floors_on_the_oldest_record_without_a_success(
    monkeypatch,
):
    # regression (#20): with no success anywhere in the ledger the oldest
    # record still bounds the staleness from below (the true last success is
    # at or before it), which beats resetting the clock to process start.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    oldest = DT(2019, 12, 31, 0, 0, 0, tzinfo=UTC)
    records = [
        _run_record(n, "failure", oldest + datetime.timedelta(hours=n))
        for n in range(6)
    ]
    await _warm_ledger(cron, records)
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    assert cron._sla_last_success["s"] == oldest
    await cron._drain_completions()


def _mem_run(outcome, finished_at):
    """An in-memory JobRunInfo, for pre-seeding run_history before a warm."""
    return cronstable.cron.JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=finished_at,
        finished_at=finished_at,
        fail_reason=None,
        output=JobOutputStream(),
    )


@pytest.mark.asyncio
async def test_job_trends_payload_caches_within_ttl_and_busts_on_run():
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)  # job "s"
    backend = _RecordBackend(
        [_run_record(1, "success", DT(2020, 1, 1, 12, 0, 0, tzinfo=UTC))]
    )
    cron.state_backend = backend

    first = await cron.job_trends_payload("s")
    assert first is not None
    reads = len(backend.reads)
    assert reads >= 1

    # a second poll inside the TTL is served from cache: no new ledger read,
    # and the very same payload object comes back.
    again = await cron.job_trends_payload("s")
    assert len(backend.reads) == reads
    assert again is first

    # a locally finished run must bust the cache so the next poll re-reads;
    # detach the backend across _record_run so its fire-and-forget ledger
    # persist does not leave a pending task in the test.
    cron.state_backend = None
    cron._record_run("s", _mem_run("failure", DT(2020, 1, 1, 12, 5, tzinfo=UTC)))
    cron.state_backend = backend
    fresh = await cron.job_trends_payload("s")
    assert len(backend.reads) > reads
    assert fresh is not first

    # an unknown job never touches the cache or the backend.
    reads2 = len(backend.reads)
    assert await cron.job_trends_payload("nope") is None
    assert len(backend.reads) == reads2


def test_apply_reload_prunes_trends_cache_for_removed_jobs():
    # _trends_cache is busted per job by _record_run, but a job the reload
    # REMOVED (or a classic-crontab name reminted when a line shifts) never
    # runs again under that name, so without a reload-time prune its entry
    # would orphan forever -- a slow leak under name churn. It must be pruned
    # exactly like every other per-job map in _apply_reload.
    two = "jobs:\n" + "".join(
        "  - name: {n}\n    command: echo {n}\n    schedule: '* * * * *'\n".format(n=n)
        for n in ("keep", "gone")
    )
    one = "jobs:\n  - name: keep\n    command: echo keep\n    schedule: '* * * * *'\n"
    cron = cronstable.cron.Cron(None, config_yaml=two)
    cron._trends_cache["keep"] = (1e18, {"name": "keep"})
    cron._trends_cache["gone"] = (1e18, {"name": "gone"})
    cron._apply_reload(cronstable.config.parse_config_string(one, "t.yaml"))
    assert "keep" in cron._trends_cache
    assert "gone" not in cron._trends_cache


@pytest.mark.asyncio
async def test_sla_warm_seeds_reference_past_the_in_memory_history_guard(
    monkeypatch,
):
    # regression (#20 residual): the staleness seed lived INSIDE the two
    # early-continue guards that skip a job already carrying in-memory history.
    # A job that only FAILED during a state outage has non-empty run_history
    # when the store finally comes up, so the pre-await guard skipped seeding,
    # _sla_last_success stayed unset, and _sla_stale_reference fell back to
    # process start: a 3-day-stale job reported ~0s observed and stayed silent.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)
    # a failure recorded while the store was down: the pre-await guard fires
    cron.run_history["s"].append(
        _mem_run("failure", DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC))
    )
    backend = _RecordBackend(
        [_run_record(1, "success", DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC))]
    )
    cron.state_backend = backend
    cron._state_rehydrated = False
    await cron._rehydrate_from_state()
    # the guard no longer hides the real last success behind the live history
    assert cron._sla_last_success.get("s") == DT(
        2019, 12, 29, 12, 0, 0, tzinfo=UTC
    )
    # ...so the 3-day-stale job pages instead of re-baselining on process start
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


@pytest.mark.asyncio
async def test_sla_warm_seeds_reference_when_a_run_lands_during_the_read(
    monkeypatch,
):
    # regression (#20 residual): the SECOND guard (a run finishing while the
    # rehydrate read awaited) skipped the seed the same way. The reference must
    # still be seeded from the live in-memory history before that continue.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    _sla_report_recorder(monkeypatch)

    class _AppendDuringRead(_RecordBackend):
        # a run for "s" finishes (a failure) while the first runs/s read is in
        # flight, populating run_history exactly as the post-await guard tests.
        def __init__(self, records, cron):
            super().__init__(records)
            self._cron = cron
            self._fired = False

        async def list_records(
            self, stream, limit=None, newest_first=False
        ):
            recs = await super().list_records(stream, limit, newest_first)
            if stream == "runs/s" and not self._fired:
                self._fired = True
                self._cron.run_history["s"].append(
                    _mem_run(
                        "failure", DT(2020, 1, 1, 11, 0, 0, tzinfo=UTC)
                    )
                )
            return recs

    backend = _AppendDuringRead(
        [_run_record(1, "success", DT(2019, 12, 29, 12, 0, 0, tzinfo=UTC))],
        cron,
    )
    cron.state_backend = backend
    cron._state_rehydrated = False
    # run_history["s"] is empty at the loop's pre-await guard, so guard 1 does
    # not fire; the read side-effect trips guard 2 after the await
    assert not cron.run_history.get("s")
    await cron._rehydrate_from_state()
    assert cron._sla_last_success.get("s") == DT(
        2019, 12, 29, 12, 0, 0, tzinfo=UTC
    )
    cron._sla_periodic()
    assert ("s", STALE) in cron._sla_state
    await cron._drain_completions()


_ONLY_IF_LAST_JOB = """
jobs:
  - name: s
    command: echo hi
    schedule: "* * * * *"
    onlyIfLastSucceeded: true
"""


@pytest.mark.asyncio
async def test_sla_warm_last_real_outcome_is_newest_by_finished_at(monkeypatch):
    # regression (#21 residual): the onlyIfLastSucceeded memo (_last_real_outcome)
    # was seeded by a positional walk of the warmed ring (the last-APPENDED
    # real outcome). Record files are named on WRITE time and run-record writes
    # are unserialized, so a success appended AFTER a newer failure would seed
    # a success as the last real outcome and reopen the gate this memo holds.
    # It must be the newest real outcome by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    _sla_report_recorder(monkeypatch)
    # seq2 (the success) is appended AFTER seq1 (the failure) but finished
    # EARLIER, the write-order inversion the finding established
    await _warm_ledger(
        cron,
        [
            _run_record(1, "failure", DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC)),
            _run_record(2, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
        ],
    )
    assert cron._last_real_outcome["s"] == (
        DT(2020, 1, 1, 10, 5, 0, tzinfo=UTC),
        "failure",
    )


@pytest.mark.asyncio
async def test_depends_on_past_folds_all_ledger_records_by_finished_at(
    monkeypatch,
):
    # regression (#21 residual, the peer / shared-mount arm): the ledger arm
    # of _depends_on_past_ok took the FIRST real record newest-by-SEQUENCE
    # then broke, so an out-of-order write (a peer on the shared mount, or two
    # concurrencyPolicy: Allow runs racing) that landed a newer success ahead
    # of the true-newest failure cleared the gate on the stale success. The
    # memo cannot cover it (only THIS node's runs update the memo). The arm
    # must fold ALL records and pick the max by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    # this node saw only its own success@10:00; that seeds the local memo
    await _warm_ledger(
        cron,
        [_run_record(1, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC))],
    )
    assert cron._last_real_outcome["s"] == (
        DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC),
        "success",
    )
    # a peer then appended to the shared ledger out of order: the TRUE newest
    # real outcome is a failure@10:20, but a later-sequence success@10:10 sits
    # ahead of it in newest-by-sequence order.
    cron.state_backend = _RecordBackend(
        [
            _run_record(1, "success", DT(2020, 1, 1, 10, 0, 0, tzinfo=UTC)),
            _run_record(2, "failure", DT(2020, 1, 1, 10, 20, 0, tzinfo=UTC)),
            _run_record(3, "success", DT(2020, 1, 1, 10, 10, 0, tzinfo=UTC)),
        ]
    )
    # the gate must BLOCK: the newest real outcome by finished_at is a failure
    assert await cron._depends_on_past_ok(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_depends_on_past_picks_newest_in_memory_run_by_finished_at(
    monkeypatch,
):
    # regression (#21 residual, the in-memory arm): the ring walk took the
    # first real outcome by reversed() list position then broke. Two
    # concurrencyPolicy: Allow runs whose unserialized record writes land out
    # of order put an older success LAST in the ring behind a newer failure,
    # so the positional walk cleared the gate on the stale success. With no
    # backend this arm decides alone; it must pick the max by finished_at.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_ONLY_IF_LAST_JOB)
    assert cron.state_backend is None
    # the failure finished LATER (10:20) but the success was appended AFTER it
    # (10:10), so the success sits last in the ring
    cron.run_history["s"].append(
        _mem_run("failure", DT(2020, 1, 1, 10, 20, 0, tzinfo=UTC))
    )
    cron.run_history["s"].append(
        _mem_run("success", DT(2020, 1, 1, 10, 10, 0, tzinfo=UTC))
    )
    assert await cron._depends_on_past_ok(cron.cron_jobs["s"]) is False


# ---------------------------------------------------------------------------
# SLA: report ordering against real run completions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sla_report_never_blocks_a_run_completion(monkeypatch):
    # regression (#4): the onLate report installed itself as the job's
    # _completion_tail, which _queue_job_completion blocks on, so a slow
    # onLate reporter delayed the finished run's failure report AND its
    # retry arming. maxRuntime makes that the ordinary case: it breaches
    # while the run is still executing.
    holder = {"now": DT(2020, 1, 1, 12, 0, 0)}
    _set_now(monkeypatch, holder)
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    gate = asyncio.Event()
    handled = []

    async def hanging_report(ctx, report_config):
        await gate.wait()

    async def fake_failure(job):
        handled.append(job)

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", hanging_report)
    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)

    class _FakeRun:
        started_at = DT(2020, 1, 1, 11, 30, 0, tzinfo=UTC)
        config = cron.cron_jobs["s"]

    cron.running_jobs["s"] = [_FakeRun()]
    cron._sla_periodic()
    assert ("s", RUNTIME) in cron._sla_state

    # that same run now finishes while the reporter is still hung
    cron._queue_job_completion(_FakeRun(), failed=True)
    for _ in range(20):
        await asyncio.sleep(0)
    assert len(handled) == 1

    # the report waits on its own tail; the completion tail stays owned by
    # real completions
    assert "s" in cron._sla_report_tail
    gate.set()
    await cron._drain_completions()
    assert cron._sla_report_tail == {}


# =====================================================================
#  Scheduling, catch-up, and reboot paths in cronstable/cron.py
# =====================================================================


def test_catchup_smoke_sanity():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron is not None


_CATCHUP_REBOOT_YAML = """
jobs:
  - name: boot
    command: echo hi
    schedule: "@reboot"
"""


def _catchup_pause(hours_from=1):
    # a pause window live against the frozen 1999-12-31 12:00 clock.
    return cronstable.cron.PauseInfo(
        since=DT(1999, 12, 31, 11, 0, 0, tzinfo=UTC),
        until=DT(1999, 12, 31, 12 + hours_from, 0, 0, tzinfo=UTC),
        note="",
        by="op",
        channel="api",
    )


async def _catchup_reboot_cron(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_CATCHUP_REBOOT_YAML)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    return cron


# --- _peek_soonest_fire / _sleep_interval / _due_names ---------------


def test_catchup_peek_soonest_fire_discards_stale_top():
    # a heap entry the next-fire index has since superseded is popped from the
    # top; the next live entry is returned.
    cron = cronstable.cron.Cron(None)
    now = cronstable.cron.get_now(UTC)
    w1 = now + datetime.timedelta(seconds=10)
    w2 = now + datetime.timedelta(seconds=20)
    cron._set_next_fire("a", w1)  # heap holds (w1, a)
    cron._next_fire["a"] = w2  # supersede without touching the heap
    cron._set_next_fire("a", w2)  # push the live (w2, a)
    assert cron._peek_soonest_fire() == w2  # (w1, a) discarded as stale


def test_catchup_sleep_interval_capped_by_dag_wake(monkeypatch):
    # a due DAG wake pulls the sleep below the once-a-minute housekeeping cap.
    cron = cronstable.cron.Cron(None)
    monkeypatch.setattr(cron._dag, "next_wake_delay", lambda: 0.3)
    assert cron._sleep_interval() == pytest.approx(0.3)


def test_dag_wake_counts_as_a_subminute_tick(monkeypatch):
    # run()'s housekeeping gate consulted only the CRON job set, but
    # _sleep_interval shortens the sleep for the DAG orchestrator too
    # (next_wake_delay always carries a 20s schedule check and a 5s approval
    # poll, and floors at 0.2s while an advance is in flight). A deployment
    # with DAGs and no second-level cron job therefore woke several times a
    # minute while answering "not sub-minute", so the gate fell through to its
    # every-iteration branch and re-ran the whole reload / cluster / web /
    # push / state / SLA block on every DAG wake, falsifying the "at most
    # once per wall-clock minute" contract _pause_periodic and _sla_periodic
    # are documented on.
    cron = cronstable.cron.Cron(None)
    assert cron._needs_subminute() is False
    # nothing has computed a sleep yet, and no DAGs: the pure minute-tick
    # deployment keeps housekeeping every iteration, exactly as before.
    assert cron._wakes_subminute() is False
    monkeypatch.setattr(cron._dag, "next_wake_delay", lambda: 5.0)
    cron._sleep_interval()
    assert cron._wakes_subminute() is True


def test_dag_wake_that_does_not_shorten_the_sleep_is_not_subminute(
    monkeypatch,
):
    # the flag tracks whether the DAG wake actually WON the min(), not merely
    # that the orchestrator answered: a hint further out than the next
    # housekeeping boundary leaves the loop on its minute tick, where
    # housekeeping every iteration is the documented behaviour.
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 10.0
    )
    cron = cronstable.cron.Cron(None)
    monkeypatch.setattr(cron._dag, "next_wake_delay", lambda: 30.0)
    assert cron._sleep_interval() == pytest.approx(10.0)
    assert cron._wakes_subminute() is False


def test_subminute_cron_job_still_gates_housekeeping_without_dags():
    # the original predicate is untouched: a second-level cron job alone still
    # puts the loop in sub-minute mode with no DAGs in sight.
    cron = cronstable.cron.Cron(None, config_yaml=_SUBMINUTE_NOFIRE)
    assert cron._needs_subminute() is True
    assert cron._wakes_subminute() is True


def test_catchup_due_names_dedupes_duplicate_live_entries():
    # a name that somehow holds two live heap entries for the same instant is
    # returned exactly once.
    cron = cronstable.cron.Cron(None)
    when = cronstable.cron.get_now(UTC)
    cron._set_next_fire("a", when)
    cron._set_next_fire("a", when)  # a second live entry for the same slot
    assert cron._due_names(when) == ["a"]


# --- _pause_excusal_window -------------------------------------------


async def test_catchup_pause_excusal_window_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    assert await cron._pause_excusal_window("p") is None


async def test_catchup_pause_excusal_window_store_error_degrades(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise RuntimeError("store down")

    cron.state_backend.list_records = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._pause_excusal_window("j") is None
    assert any("pause stream" in r.getMessage() for r in caplog.records)


# --- _checkpoint_catchup ---------------------------------------------


async def test_catchup_checkpoint_catchup_no_backend_is_noop():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    # no state backend -> returns without touching anything (no raise).
    await cron._checkpoint_catchup("p", "open", "wm")


async def test_catchup_checkpoint_catchup_write_error_is_best_effort(
    tmp_path, caplog
):
    import logging

    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise RuntimeError("append failed")

    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._checkpoint_catchup("j", "open", "wm")  # swallowed
    assert any(
        "could not checkpoint" in r.getMessage() for r in caplog.records
    )


# --- _catch_up orchestration edges -----------------------------------


async def test_catchup_catch_up_defers_before_retry_interval(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")
    cron._catchup_next_retry = asyncio.get_running_loop().time() + 1000
    await cron._catch_up(_NOW)
    assert cron._caught_up is False  # bailed out before evaluating
    assert cron._catchup_tasks == set()


async def test_catchup_catch_up_no_state_warns_archive_and_gate(caplog):
    import logging

    yaml = (
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
        "    archiveOutput: true\n    onlyIfLastSucceeded: true\n"
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)  # no state backend
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron._catch_up(_NOW)
    assert cron._caught_up is True
    msgs = [r.getMessage() for r in caplog.records]
    assert any("archiveOutput" in m for m in msgs)
    assert any("onlyIfLastSucceeded" in m for m in msgs)


async def test_catchup_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(now):
        raise asyncio.CancelledError()

    cron._evaluate_catch_up = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._catch_up(_NOW)


async def test_catchup_catch_up_defers_on_unexpected_error(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(now):
        raise RuntimeError("kaboom")

    cron._evaluate_catch_up = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._catch_up(_NOW)
    assert cron._caught_up is False  # unresolved -> will retry
    assert cron._catchup_next_retry > 0
    assert any("evaluating" in r.getMessage() for r in caplog.records)


# --- _evaluate_catch_up edges ----------------------------------------


async def test_catchup_evaluate_catch_up_skips_already_done(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._catchup_done.add("j")  # already resolved on an earlier pass
    assert await cron._evaluate_catch_up(_NOW) is False  # nothing pending


async def test_catchup_evaluate_catch_up_pins_pre_pause_watermark(tmp_path):
    # a paused job with no open checkpoint pins the pre-pause watermark (an
    # `open` checkpoint) and defers rather than latching.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()
    assert await cron._evaluate_catch_up(_NOW) is True  # deferred
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert recs and recs[0]["kind"] == "open"


async def test_catchup_evaluate_catch_up_pause_pin_error_defers(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()

    async def boom(name):
        raise RuntimeError("store down")

    cron._pending_catchup_watermark = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._evaluate_catch_up(_NOW) is True
    assert any("pin the pre-pause" in r.getMessage() for r in caplog.records)


async def test_catchup_evaluate_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(job, now):
        raise asyncio.CancelledError()

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._evaluate_catch_up(_NOW)


# --- _run_catch_up edges ---------------------------------------------


async def test_catchup_run_catch_up_reread_error_drops(tmp_path, caplog):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def boom(job, now):
        raise RuntimeError("watermark read failed")

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)
    assert calls == []
    assert any("re-read" in r.getMessage() for r in caplog.records)


async def test_catchup_run_catch_up_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )

    async def boom(job, now):
        raise asyncio.CancelledError()

    cron._missed_occurrences = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)


async def test_catchup_run_catch_up_nothing_owed_closes_cycle(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def zero(job, now):
        return 0, "2026-07-01T10:00:00+00:00"

    cron._missed_occurrences = zero  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 3, 0.0, _NOW)
    assert calls == []
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert recs and recs[0]["kind"] == "close"


async def test_catchup_run_catch_up_bails_when_idle_wait_signals_stop(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_false(name, *, max_wait=None):
        return False  # shutdown signalled while draining

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_false  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []


async def test_catchup_run_catch_up_ownership_moves_mid_backfill(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_true(name, *, max_wait=None):
        return True

    seen = {"n": 0}

    def allows(job):
        seen["n"] += 1
        return seen["n"] <= 1  # owner after jitter, then ownership moves

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_true  # type: ignore[method-assign]
    cron._cluster_allows = allows  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []  # left to the new owner before any launch


async def test_catchup_run_catch_up_paused_mid_backfill(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_true(name, *, max_wait=None):
        return True

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_true  # type: ignore[method-assign]
    cron._paused["j"] = _catchup_pause()
    await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)
    assert calls == []  # dropped without closing the checkpoint


async def test_catchup_run_catch_up_final_drain_signals_stop(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def one(job, now):
        return 1, "wm"

    idle = {"n": 0}

    async def idle_cb(name, *, max_wait=None):
        idle["n"] += 1
        return idle["n"] == 1  # go for the loop, stop on the final drain

    cron._missed_occurrences = one  # type: ignore[method-assign]
    cron._wait_job_idle = idle_cb  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == ["j"]  # the one launch happened
    recs = await cron.state_backend.list_records(cron._catchup_stream("j"))
    assert not any(r.get("kind") == "close" for r in recs)  # not closed


async def test_catchup_run_catch_up_outer_error_never_kills_loop(
    tmp_path, caplog
):
    import logging

    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]

    async def two(job, now):
        return 2, "wm"

    async def idle_boom(name, *, max_wait=None):
        raise RuntimeError("unexpected")

    cron._missed_occurrences = two  # type: ignore[method-assign]
    cron._wait_job_idle = idle_boom  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._run_catch_up(cron.cron_jobs["j"], 2, 0.0, _NOW)  # no raise
    assert calls == []
    assert any("backfilling" in r.getMessage() for r in caplog.records)


# --- _wait_job_idle --------------------------------------------------


async def test_catchup_wait_job_idle_returns_false_on_stop():
    cron = cronstable.cron.Cron(None, config_yaml=_PAUSABLE_JOB)
    cron.running_jobs["p"] = ["sentinel"]  # still busy
    cron._stop_event.set()  # shutdown while waiting
    assert await cron._wait_job_idle("p") is False


# --- _defer_paused_reboot / _process_paused_reboots ------------------


def test_catchup_defer_paused_reboot_is_idempotent():
    cron = cronstable.cron.Cron(None)
    cron._defer_paused_reboot("boot")
    cron._defer_paused_reboot("boot")  # already held -> early return
    assert cron._paused_reboot_jobs == {"boot"}


async def test_catchup_process_paused_reboots_absent_stays_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron._paused_reboot_jobs.add("ghost")  # name not in cron_jobs
    await cron._process_paused_reboots()
    assert "ghost" in cron._paused_reboot_jobs  # transiently absent -> owed
    assert launched == []


async def test_catchup_process_paused_reboots_retires_non_reboot(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job(enabled=False)  # disabled on reload
    cron._paused_reboot_jobs.add("boot")
    await cron._process_paused_reboots()
    assert "boot" not in cron._paused_reboot_jobs  # retired without running
    assert launched == []


async def test_catchup_process_paused_reboots_still_paused_keeps_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job()
    cron._paused_reboot_jobs.add("boot")
    cron._paused["boot"] = _catchup_pause()  # pause has not lifted yet
    await cron._process_paused_reboots()
    assert "boot" in cron._paused_reboot_jobs  # still deferred
    assert launched == []


async def test_catchup_process_paused_reboots_ownership_moved_keeps_owed(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None)
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    cron.cron_jobs["boot"] = _reboot_job()
    cron._paused_reboot_jobs.add("boot")
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    await cron._process_paused_reboots()
    assert "boot" in cron._paused_reboot_jobs  # ownership moved -> still owed
    assert launched == []


# --- _spawn_due_jobs dead schedule / _launch_plan --------------------


async def test_catchup_spawn_due_jobs_latches_dead_schedule(monkeypatch, caplog):
    import logging

    yaml = (
        "jobs:\n  - name: dead\n    command: echo x\n"
        "    schedule: '0 0 30 2 *'\n"  # February 30th: never fires again
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    monkeypatch.setattr(cron, "launch_scheduled_job", lambda j: _noop())
    now = cronstable.cron.get_now(UTC)
    cron._set_next_fire("dead", now)  # force it due this pass
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._spawn_due_jobs(now)
    assert "dead" in cron._dead_schedules
    assert "dead" not in cron._next_fire
    assert any("NEVER fire again" in r.getMessage() for r in caplog.records)


async def test_catchup_launch_plan_skips_shallow_jobs_in_later_rounds(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_THREE_DUE)
    launched = []

    async def fake(job):
        launched.append(job.name)

    monkeypatch.setattr(cron, "launch_scheduled_job", fake)
    now = cronstable.cron.get_now(UTC)
    later = now + datetime.timedelta(minutes=1)
    plan = [
        (cron.cron_jobs["a"], [now, later]),  # two catch-up rounds
        (cron.cron_jobs["b"], [now]),  # only one -> skipped in round 2
    ]
    await cron._launch_plan(plan)
    # round 0: a, b concurrently; round 1: a alone (b past its fire list)
    assert launched == ["a", "b", "a"]


# --- _reboot_marker_covers / _reboot_boot_gate -----------------------


async def test_catchup_reboot_marker_covers_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_CATCHUP_REBOOT_YAML)
    assert await cron._reboot_marker_covers(cron.cron_jobs["boot"]) is False


async def test_catchup_reboot_marker_covers_ignores_foreign_host(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._reboot_stream("boot"),
        {"host": "some-other-host", "jobDigest": "x", "bootId": "y"},
    )
    # only this host's markers decide; a foreign one is skipped -> not covered.
    assert await cron._reboot_marker_covers(cron.cron_jobs["boot"]) is False


async def test_catchup_reboot_gate_sick_runs_without_dedupe(tmp_path, caplog):
    import logging

    cron = await _catchup_reboot_cron(tmp_path)
    cron._reboot_gate_sick = True  # a prior op timed out this pass
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert any("without boot-marker" in r.getMessage() for r in caplog.records)


async def test_catchup_reboot_gate_reraises_cancelled_read(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def boom(job):
        raise asyncio.CancelledError()

    cron._reboot_marker_covers = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])


async def test_catchup_reboot_gate_read_timeout_marks_sick_then_runs(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def boom(job):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = boom  # type: ignore[method-assign]
    # default degrade policy: a read timeout latches sick and runs the job.
    assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert cron._reboot_gate_sick is True


async def test_catchup_reboot_gate_reraises_cancelled_write(tmp_path):
    cron = await _catchup_reboot_cron(tmp_path)

    async def not_covered(job):
        return False

    async def boom(*a, **k):
        raise asyncio.CancelledError()

    cron._reboot_marker_covers = not_covered  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])


async def test_catchup_reboot_gate_write_timeout_fail_closed_recheck_visible(
    tmp_path,
):
    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        return seen["n"] > 1  # absent at the gate, visible on the re-check

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    # the abandoned append landed late: the re-check sees it -> launch.
    assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is True
    assert cron._reboot_gate_sick is True


async def test_catchup_reboot_gate_write_timeout_fail_closed_recheck_absent(
    tmp_path, caplog
):
    import logging

    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        if seen["n"] == 1:
            return False  # absent at the gate
        raise RuntimeError("still unknown")  # re-check cannot decide

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        assert await cron._reboot_boot_gate(cron.cron_jobs["boot"]) is False
    assert any("cannot record" in r.getMessage() for r in caplog.records)


# --- _process_pending_reboots edges ----------------------------------


async def test_catchup_pending_reboots_election_removed_paused_keeps_owed(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = False  # election turned off on reload
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron._paused["boot"] = _catchup_pause()
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # pause defers, keeps it owed


async def test_catchup_pending_reboots_no_manager_absent_kept():
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # manager never came up
    cron._pending_reboot_jobs["ghost"] = _reboot_job("ghost")  # not in jobs
    await cron._process_pending_reboots()
    assert "ghost" in cron._pending_reboot_jobs  # never-lose


async def test_catchup_pending_reboots_no_manager_paused_keeps_owed(monkeypatch):
    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True
    cron.cluster_manager = None
    launched = []
    monkeypatch.setattr(
        cron,
        "launch_scheduled_job",
        lambda j: launched.append(j.name) or _noop(),
    )
    job = _reboot_job(policy="PreferLeader")
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    cron._paused["boot"] = _catchup_pause()
    await cron._process_pending_reboots()
    assert launched == []
    assert "boot" in cron._pending_reboot_jobs  # deferred by the pause


async def test_catchup_pending_reboots_reboot_ran_error_keeps_owed(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _RaisingMgr:
        node_name = "node-a"
        distribution = "single-leader"

        def reboot_ran(self, name):
            raise RuntimeError("backend read failed")

    cron.cluster_manager = _RaisingMgr()
    job = _reboot_job()
    cron.cron_jobs["boot"] = job
    cron._pending_reboot_jobs["boot"] = job
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._process_pending_reboots()
    assert "boot" in cron._pending_reboot_jobs  # kept pending on read error
    assert any("already ran" in r.getMessage() for r in caplog.records)


# --- CancelledError re-raise paths (defensive) -----------------------


async def test_catchup_pause_excusal_window_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")

    async def boom(*a, **k):
        raise asyncio.CancelledError()

    cron.state_backend.list_records = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._pause_excusal_window("j")


async def test_catchup_evaluate_catch_up_pause_pin_reraises_cancelled(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    cron._paused["j"] = _catchup_pause()

    async def boom(name):
        raise asyncio.CancelledError()

    cron._pending_catchup_watermark = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._evaluate_catch_up(_NOW)


async def test_catchup_reboot_gate_write_timeout_recheck_reraises_cancelled(
    tmp_path,
):
    cron = await _catchup_reboot_cron(tmp_path)
    cron._state_on_unavailable = "fail-closed"
    seen = {"n": 0}

    async def marker(job):
        seen["n"] += 1
        if seen["n"] == 1:
            return False  # absent at the gate
        raise asyncio.CancelledError()  # cancelled during the re-check

    async def boom(*a, **k):
        raise asyncio.TimeoutError()

    cron._reboot_marker_covers = marker  # type: ignore[method-assign]
    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await cron._reboot_boot_gate(cron.cron_jobs["boot"])
