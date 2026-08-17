import asyncio
import datetime

import pytest

import cronstable.cron
from cronstable.cron import JobRunInfo
from cronstable.fingerprint import job_digest
from cronstable.job import JobOutputStream, JobRetryState
from tests._configs import _DEP_JOB, _ONE_JOB, job_yaml
from tests._cron_helpers import (
    UTC,
    fixed_current_time,  # noqa: F401
)
from tests._helpers import _seed_orphan_open, _state_cfg

# ===================== rehydrate additions =====================
#
# The durable-state plumbing in cronstable/cron.py:
# the inflight open/close persistence, crash reconciliation, run-record and
# counter-snapshot writes, the rehydrate/reconcile boot paths, retry re-arming
# and validation, and the completion/failure handlers. The degrade branches
# (backend torn down, store error, timeout, cancellation) are exercised
# alongside the real happy-path behaviour so the in-memory maps are asserted.


# the one-job and onlyIfLastSucceeded bases are the shared _ONE_JOB and
# _DEP_JOB from tests._configs; only the variants below stay local.

# the retry ladder shared by the minute-schedule and @reboot variants
_REHYDRATE_RETRY_BLOCK = (
    "    onFailure:\n"
    "      retry:\n"
    "        maximumRetries: 2\n"
    "        initialDelay: 0.1\n"
    "        maximumDelay: 1\n"
    "        backoffMultiplier: 2\n"
)

_RUNALL_REHYDRATE = _ONE_JOB + "    onMissed: run-all\n"

_RETRY_REHYDRATE = _ONE_JOB + _REHYDRATE_RETRY_BLOCK

_REBOOT_RETRY_REHYDRATE = job_yaml(
    "j", "'true'", schedule="@reboot", extra=_REHYDRATE_RETRY_BLOCK
)


def _rehydrate_cfg(tmp_path):
    return _state_cfg("state:\n  path: " + str(tmp_path))


async def _rehydrate_state_cron(tmp_path, yaml=_ONE_JOB):
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    await cron.start_stop_state(_rehydrate_cfg(tmp_path))
    return cron


async def _raise_oserror5(*args, **kwargs):
    raise OSError("state store went away")


async def _raise_cancelled5(*args, **kwargs):
    raise asyncio.CancelledError


async def _raise_timeout5(*args, **kwargs):
    raise asyncio.TimeoutError


def _mem_run5(outcome, minute):
    dt = datetime.datetime(2026, 7, 1, 10, minute, 0, tzinfo=UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=JobOutputStream(),
    )


async def _rehydrate_seed_pending_retry(
    cron, *, attempt=1, not_before="2026-07-01T10:00:00+00:00", host=None
):
    job = cron.cron_jobs["j"]
    await cron.state_backend.append_record(
        cron._retry_stream("j"),
        {
            "kind": "pending",
            "attempt": attempt,
            "notBefore": not_before,
            "host": host if host is not None else cron._state_host,
            "jobDigest": job_digest(job),
            "at": not_before,
        },
    )


class _FakeRun5:
    """A minimal RunningJob stand-in carrying just a config with a name.

    The reaped-run flags are the ones every real RunningJob carries and the
    DAG-task report dispatch reads (cancelled/replaced skip reporting; a
    None fail_reason reads as success).
    """

    def __init__(self, config, *, state_token=None):
        self.config = config
        self.state_token = state_token
        self.cancelled = False
        self.replaced = False
        self.fail_reason = None


# --- inflight open/closed persistence --------------------------------------


async def test_rehydrate_persist_inflight_open_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    assert cron.state_backend is None
    await cron._persist_inflight_open(cron.cron_jobs["j"], object())


async def test_rehydrate_persist_inflight_open_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5

    class _RJ:
        proc = None

    await cron._persist_inflight_open(cron.cron_jobs["j"], _RJ())  # no raise


async def test_rehydrate_persist_inflight_closed_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._persist_inflight_closed("j")


async def test_rehydrate_persist_inflight_closed_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5
    await cron._persist_inflight_closed("j")  # no raise


# --- inflight reconciliation ------------------------------------------------


async def test_rehydrate_reconcile_inflight_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._reconcile_inflight()


async def test_rehydrate_reconcile_inflight_skips_running(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.running_jobs["j"].append(object())
    await cron._reconcile_inflight()  # the only job is running -> skipped
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_timeout5
    await cron._reconcile_inflight()  # a hung store aborts the whole pass
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_error_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    await cron._reconcile_inflight()  # a store error skips the job, no crash
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_inflight_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._reconcile_inflight()


def _many_jobs_yaml(count):
    return "jobs:\n" + "".join(
        "  - name: j%d\n    command: x\n    schedule: '@reboot'\n" % i
        for i in range(count)
    )


# _seed_orphan_open moved to tests/_helpers.py (shared with the durability
# suite); imported above.


async def test_rehydrate_reconcile_inflight_reads_jobs_concurrently(tmp_path):
    # The boot reconciliation must overlap its per-job in-flight reads: it
    # sits on the boot path between the history warm-up and the retry
    # re-arm, both of which already use the worker pool, so a strictly
    # sequential pass here put back the jobs x per-read latency those two
    # avoid. The rendezvous below only clears once 4 reads are in flight AT
    # THE SAME TIME; a sequential pass never gets past its first read and
    # times out instead. The high-water mark is checked against the pool
    # bound too: the pool must not degenerate into a task per job, which on
    # a large crontab would only queue on the store's bulk lane anyway. That
    # upper bound is why the crontab is seeded with MORE jobs than the pool
    # has workers; with fewer, the job count itself caps the high-water mark
    # and a task-per-job implementation passes too.
    cron = await _rehydrate_state_cron(
        tmp_path, _many_jobs_yaml(cronstable.cron._REHYDRATE_CONCURRENCY + 4)
    )
    need = 4
    state = {"in_flight": 0, "high": 0}
    opened = asyncio.Event()
    release = asyncio.Event()

    async def _list(stream, **kw):
        state["in_flight"] += 1
        state["high"] = max(state["high"], state["in_flight"])
        if state["high"] >= need:
            opened.set()
        try:
            # every read parks here until the test releases it, so the
            # high-water mark counts workers rather than scheduling luck.
            # An event that reopened as soon as `need` reads overlapped would
            # measure nothing: past the first opening, awaiting an already-set
            # event does not yield, so ONE worker would drain the whole item
            # iterator without ever letting a second one in, and the bound
            # below would hold for any implementation, pool or not.
            await release.wait()
        finally:
            state["in_flight"] -= 1
        return []

    cron.state_backend.list_records = _list
    pass_ = asyncio.ensure_future(cron._reconcile_inflight())
    try:
        # a sequential implementation never gets a second read in flight, so
        # it fails here rather than hanging the suite.
        await asyncio.wait_for(opened.wait(), timeout=2.0)
        # let every worker that is going to start reach its read, so the
        # plateau below is the real one.
        for _ in range(50):
            await asyncio.sleep(0)
        assert state["high"] >= need
        assert state["high"] <= cronstable.cron._REHYDRATE_CONCURRENCY
    finally:
        release.set()
        await pass_


async def test_rehydrate_reconcile_inflight_reconciles_every_job(tmp_path):
    # The outcome invariant the worker pool must preserve, with more jobs
    # than workers so the shared item iterator is drawn from several times
    # per worker: every orphaned run is reconciled, exactly once, whichever
    # worker happens to draw it. A pool that let two workers draw the same
    # name would append two synthetic rows for one interrupted run; a pool
    # that dropped names would leave crashed runs invisible forever, the
    # whole failure this pass exists to prevent.
    count = cronstable.cron._REHYDRATE_CONCURRENCY + 4
    cron = await _rehydrate_state_cron(tmp_path, _many_jobs_yaml(count))
    for i in range(count):
        await _seed_orphan_open(cron, "j%d" % i)
    await cron._reconcile_inflight()
    for i in range(count):
        name = "j%d" % i
        assert [r.outcome for r in cron.run_history[name]] == ["unknown"]
        assert cron.last_run[name].outcome == "unknown"
    # drain the fire-and-forget closes/ledger appends the pass queued, then
    # confirm each job's stream really did get its own single close (the
    # per-job write chain keeps open/closed ordered).
    while cron._pending_state_writes:
        await asyncio.gather(*list(cron._pending_state_writes))
    for i in range(count):
        recs = await cron.state_backend.list_records(
            cron._inflight_stream("j%d" % i)
        )
        assert [r["kind"] for r in recs] == ["open", "closed"]


# --- takeover reconciliation ------------------------------------------------


async def test_rehydrate_reconcile_takeover_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])


async def test_rehydrate_reconcile_takeover_error_returns(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])  # no raise


async def test_rehydrate_reconcile_takeover_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])


async def test_rehydrate_reconcile_takeover_skips_own_live_run(tmp_path):
    # a record this very process wrote (same host AND proc token) is our own
    # live run: the takeover must stand down and reconcile nothing.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": cron._state_host,
            "proc": cron._proc_token,
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert "j" not in cron.last_run


async def test_rehydrate_reconcile_takeover_closes_foreign_orphan(tmp_path):
    # a foreign host's open record is judged purely by fence supersession:
    # the takeover closes it and surfaces a synthetic unknown-outcome run.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": "other-host",
            "proc": "deadbeef",
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T10:00:00+00:00"
    )
    await asyncio.gather(*list(cron._pending_state_writes))


async def test_rehydrate_reconcile_open_record_defaults_missing_started(tmp_path):
    # a record with no startedAt string falls back to "now" for the
    # interruption instant rather than crashing the reconcile.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {"kind": "open", "host": "other-host", "proc": "deadbeef", "pid": None},
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    await asyncio.gather(*list(cron._pending_state_writes))


async def test_rehydrate_reconcile_open_record_runall_leaves_watermark(tmp_path):
    # under onMissed run-all the interrupted slot is still owed to catch-up,
    # so the synthetic row carries interruptedAt (no finished_at) and the
    # rehydrated info's outcome is still unknown.
    cron = await _rehydrate_state_cron(tmp_path, _RUNALL_REHYDRATE)
    await cron.state_backend.append_record(
        cron._inflight_stream("j"),
        {
            "kind": "open",
            "host": "other-host",
            "proc": "deadbeef",
            "pid": None,
            "startedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._reconcile_takeover_inflight(cron.cron_jobs["j"])
    assert cron.last_run["j"].outcome == "unknown"
    await asyncio.gather(*list(cron._pending_state_writes))
    (rec,) = await cron.state_backend.list_records(cron._run_stream("j"))
    assert rec.get("finished_at") is None
    assert rec["interruptedAt"] == "2026-07-01T10:00:00+00:00"


async def test_rehydrate_persist_reconciled_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._persist_reconciled_record("j", {"outcome": "unknown"})


async def test_rehydrate_persist_reconciled_degrades_on_error(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.append_record = _raise_oserror5
    await cron._persist_reconciled_record("j", {"outcome": "unknown"})


# --- run record / counter snapshot / archive: no-backend guards -------------


async def test_rehydrate_persist_run_record_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._persist_run_record("j", _mem_run5("success", 0))


async def test_rehydrate_persist_counter_snapshot_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._persist_counter_snapshot()


async def test_rehydrate_persist_counter_snapshot_unseeded(tmp_path):
    # the seed gate: a run finishing before _rehydrate_counters ran must not
    # write a snapshot the seed would then double-ingest.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    await cron._persist_counter_snapshot()
    assert await cron.state_backend.list_records(cron._counters_stream()) == []


async def test_rehydrate_archive_output_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    info = _mem_run5("success", 0)
    await cron._archive_output(
        cron.cron_jobs["j"], info, list(info.output.lines)
    )


# --- SLA last-success warm scan ---------------------------------------------


async def test_rehydrate_warm_last_success_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._warm_last_success_beyond_history("j", [])


async def test_rehydrate_warm_last_success_error_falls_back_to_oldest(tmp_path):
    # the deeper re-read errors: the reference falls back to the oldest
    # finished_at seen in the warmed history (a lower bound on staleness).
    cron = await _rehydrate_state_cron(tmp_path)
    cron.state_backend.list_records = _raise_oserror5
    history = [_mem_run5("failure", 5), _mem_run5("failure", 2)]
    await cron._warm_last_success_beyond_history("j", history)
    assert cron._sla_last_success["j"] == datetime.datetime(
        2026, 7, 1, 10, 2, 0, tzinfo=UTC
    )


async def test_rehydrate_warm_last_success_finds_deeper_success(tmp_path):
    # a poison record (no finished_at) is skipped; a real deeper success is
    # taken as the staleness reference.
    cron = await _rehydrate_state_cron(tmp_path)
    await cron.state_backend.append_record(
        cron._run_stream("j"), {"outcome": "success"}
    )
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {"outcome": "success", "finished_at": "2026-07-01T09:00:00+00:00"},
    )
    await cron._warm_last_success_beyond_history("j", [])
    assert cron._sla_last_success["j"].isoformat() == (
        "2026-07-01T09:00:00+00:00"
    )


# --- rehydrate-from-state degrade branches ----------------------------------


async def test_rehydrate_rehydrate_from_state_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False

    async def _list(stream, **kw):
        if stream.startswith("runs/"):
            raise asyncio.TimeoutError
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_from_state()  # a hung store aborts the warm-up
    assert not cron.run_history.get("j")


async def test_rehydrate_reads_jobs_concurrently(tmp_path):
    # The warm-up must overlap its per-job ledger reads: strictly
    # sequential reads made boot delay scale linearly with job count (the
    # whole point of the worker pool). The rendezvous below only clears
    # once 4 reads are in flight AT THE SAME TIME; a sequential warm-up
    # never gets past its first read and times out instead.
    yaml = "jobs:\n" + "".join(
        "  - name: j%d\n    command: x\n    schedule: '@reboot'\n" % i
        for i in range(8)
    )
    cron = await _rehydrate_state_cron(tmp_path, yaml)
    cron._state_rehydrated = False
    need = 4
    state = {"in_flight": 0, "high": 0}
    gate = asyncio.Event()

    async def _list(stream, **kw):
        if not stream.startswith("runs/"):
            return []
        state["in_flight"] += 1
        state["high"] = max(state["high"], state["in_flight"])
        if state["high"] >= need:
            gate.set()
        try:
            # cleared only when `need` reads overlap; the 2s bound makes a
            # sequential implementation fail fast (each lone read times
            # out) rather than hang the test.
            await asyncio.wait_for(gate.wait(), timeout=2.0)
        finally:
            state["in_flight"] -= 1
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_from_state()
    assert state["high"] >= need


async def test_rehydrate_rehydrate_from_state_oserror_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False

    async def _list(stream, **kw):
        if stream.startswith("runs/"):
            raise OSError("boom")
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_from_state()  # a store error skips this job
    assert not cron.run_history.get("j")


async def test_rehydrate_rehydrate_from_state_warms_history(tmp_path):
    # the happy path: seeded run records warm run_history and last_run, and
    # the real onlyIfLastSucceeded / last-completed memos are seeded too.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "failure",
            "exit_code": 1,
            "finished_at": "2026-07-01T09:00:00+00:00",
            "ranAt": "2026-07-01T09:00:00+00:00",
        },
    )
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "success",
            "exit_code": 0,
            "finished_at": "2026-07-01T09:05:00+00:00",
            "ranAt": "2026-07-01T09:05:00+00:00",
        },
    )
    busts = []
    real_bust = cron._bust_response_memos

    def counting_bust():
        busts.append(1)
        real_bust()

    cron._bust_response_memos = counting_bust
    await cron._rehydrate_from_state()
    # warmed rows must bust the response memos, or a poll served just
    # before the warm-up keeps rendering blank history out to the TTL
    assert busts
    assert len(cron.run_history["j"]) == 2
    assert cron.last_run["j"].outcome == "success"
    assert cron._last_real_outcome["j"][1] == "success"
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T09:05:00+00:00"
    )


# --- rehydrate: order tolerance on a shared mount ---------------------------
#
# The per-job write chain orders THIS node's appends to runs/<job>; it cannot
# order appends a peer node sharing the mount issues through its own process.
# These seeds invert a pair deterministically: the record with the NEWER
# finished_at is appended FIRST, so the last record in the stream is the older
# run and any reader trusting stream position picks it.  Sequential awaits plus
# the backend's monotonic record-name floor fix the file order, so there is no
# race to lose (unlike the write-side ordering tests, which had to delay the
# first append to make the natural inversion deterministic).


async def _seed_inverted_pair(cron):
    # newer FIRST...
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "success",
            "exit_code": 0,
            "finished_at": "2026-07-01T09:05:00+00:00",
            "ranAt": "2026-07-01T09:05:00+00:00",
        },
    )
    # ...older SECOND, so it is last in the stream
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "failure",
            "exit_code": 1,
            "finished_at": "2026-07-01T09:00:00+00:00",
            "ranAt": "2026-07-01T09:00:00+00:00",
        },
    )


async def test_rehydrate_last_run_is_newest_by_finished_at_not_position(
    tmp_path,
):
    # the highest-stakes case in the whole item: taking the last record in
    # the stream reports the job FAILED after a restart when its newest run
    # succeeded, and /jobs, the dashboard and the Prometheus last-run gauges
    # all read it.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False
    await _seed_inverted_pair(cron)
    await cron._rehydrate_from_state()
    assert cron.last_run["j"].outcome == "success"
    assert cron.last_run["j"].finished_at.isoformat() == (
        "2026-07-01T09:05:00+00:00"
    )
    # and the warmed ring itself is in finish order, so the history the
    # sparkline renders reads oldest to newest
    assert [r.finished_at.isoformat() for r in cron.run_history["j"]] == [
        "2026-07-01T09:00:00+00:00",
        "2026-07-01T09:05:00+00:00",
    ]


async def test_rehydrate_last_completed_at_is_newest_by_finished_at(tmp_path):
    # separate test from the one above so a half-fix is caught: the retry
    # ladder's superseded-by-run watermark must not rewind to the older run
    # of an inverted pair, or a ladder armed between the two settles.
    # `failure` (not `skipped`) for the older row, so the walk's own outcome
    # filter cannot mask the ordering.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._state_rehydrated = False
    await _seed_inverted_pair(cron)
    await cron._rehydrate_from_state()
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T09:05:00+00:00"
    )


async def test_rehydrate_orders_a_reconciled_row_by_its_interruption(
    tmp_path,
):
    # a crash-reconciled row under onMissed run-all carries interruptedAt and
    # NO finished_at, which is the record shape a finished_at fold has to
    # survive: _job_run_info_from_dict substitutes the interruption instant,
    # so the row sorts where the interrupted run STARTED and can never make
    # the fold raise.
    cron = await _rehydrate_state_cron(tmp_path, _RUNALL_REHYDRATE)
    cron._state_rehydrated = False
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "success",
            "exit_code": 0,
            "finished_at": "2026-07-01T11:00:00+00:00",
            "ranAt": "2026-07-01T11:00:00+00:00",
        },
    )
    await cron.state_backend.append_record(
        cron._run_stream("j"),
        {
            "outcome": "unknown",
            "exit_code": None,
            "started_at": None,
            "duration": None,
            "fail_reason": "run interrupted",
            "interruptedAt": "2026-07-01T10:00:00+00:00",
        },
    )
    await cron._rehydrate_from_state()
    assert cron.last_run["j"].outcome == "success"
    assert [r.outcome for r in cron.run_history["j"]] == ["unknown", "success"]
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T11:00:00+00:00"
    )


async def test_rehydrate_reconciled_takeover_does_not_regress_last_run(
    tmp_path,
):
    # a slot takeover reconciles a FOREIGN node's interrupted run, and the
    # synthetic row's instant is that run's START, which can predate a run
    # this node already recorded.  Installing it as last_run made the whole
    # status surface report `unknown` for a job whose newest run succeeded.
    cron = await _rehydrate_state_cron(tmp_path)
    cron._record_run("j", _mem_run5("success", 5))
    cron._reconcile_open_record(
        "j",
        cron.cron_jobs["j"],
        {"startedAt": "2026-07-01T10:00:00+00:00", "host": "other-node"},
        "reconciled-takeover",
    )
    assert cron.last_run["j"].outcome == "success"
    # the interrupted run is still VISIBLE; only the "which is newest"
    # answer changed
    assert [r.outcome for r in cron.run_history["j"]] == [
        "success",
        "unknown",
    ]
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T10:05:00+00:00"
    )
    await asyncio.gather(*list(cron._pending_state_writes))


async def _seed_two_overlapping_successes(cron):
    # concurrencyPolicy: Allow is the default, so a job whose runtime exceeds
    # its interval routinely has instances finishing while an earlier one is
    # still going.  These two finish at 10:03 and 10:04, after the 10:02
    # start the reconcile rows below stand in.
    cron._record_run("j", _mem_run5("success", 3))
    cron._record_run("j", _mem_run5("success", 4))


async def test_rehydrate_local_crash_outranks_the_runs_that_outlived_it(
    tmp_path,
):
    # THIS host's own interrupted run is the newest thing that happened here,
    # whatever finished around it.  The synthetic row is keyed on the run's
    # START, so folding it against the overlapping instances that finished
    # after that instant would report `success` for a job that just died, on
    # GET /jobs, the dashboard tile, every cronstable_job_last_run_* gauge
    # and the /jobs/{name}/logs replay target.
    cron = await _rehydrate_state_cron(tmp_path)
    await _seed_two_overlapping_successes(cron)
    cron._reconcile_open_record(
        "j",
        cron.cron_jobs["j"],
        {
            "startedAt": "2026-07-01T10:02:00+00:00",
            "host": cron._state_host,
        },
        "reconciled-crash",
    )
    assert cron.last_run["j"].outcome == "unknown"
    # the watermark is a separate question and stays monotonic: a run that
    # started before the newest completion must not rewind the retry
    # ladder's supersede reference.
    assert cron._last_completed_at["j"].isoformat() == (
        "2026-07-01T10:04:00+00:00"
    )
    await asyncio.gather(*list(cron._pending_state_writes))


async def test_rehydrate_foreign_crash_loses_to_a_newer_local_run(tmp_path):
    # the sibling of the test above, and the case the conditional promotion
    # was written for: a slot takeover reconciles ANOTHER node's interrupted
    # run, which says nothing about what ran here, so it must not displace a
    # local run that finished after it started.
    cron = await _rehydrate_state_cron(tmp_path)
    await _seed_two_overlapping_successes(cron)
    cron._reconcile_open_record(
        "j",
        cron.cron_jobs["j"],
        {"startedAt": "2026-07-01T10:02:00+00:00", "host": "other-node"},
        "reconciled-takeover",
    )
    assert cron.last_run["j"].outcome == "success"
    assert cron.last_run["j"].finished_at.isoformat() == (
        "2026-07-01T10:04:00+00:00"
    )
    # still visible in the history either way; only the "which is newest"
    # answer differs between the two hosts
    assert [r.outcome for r in cron.run_history["j"]] == [
        "success",
        "success",
        "unknown",
    ]
    # losing the fold is not the same as being barred from it: a foreign
    # row whose run really did start after everything here recorded IS the
    # newest thing known about the job.
    cron._reconcile_open_record(
        "j",
        cron.cron_jobs["j"],
        {"startedAt": "2026-07-01T10:06:00+00:00", "host": "other-node"},
        "reconciled-takeover",
    )
    assert cron.last_run["j"].outcome == "unknown"
    await asyncio.gather(*list(cron._pending_state_writes))


# The ring-release half of the conditional promotion lives with its sibling
# invariant in tests/test_cron_web.py, beside
# test_record_run_releases_superseded_ring.


# --- rehydrate counters -----------------------------------------------------


async def test_rehydrate_rehydrate_counters_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._rehydrate_counters()


async def test_rehydrate_rehydrate_counters_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_counters()


async def test_rehydrate_rehydrate_counters_error_forfeits_seed(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path)
    cron._counters_seeded = False
    cron.state_backend.list_records = _raise_oserror5
    await cron._rehydrate_counters()  # the seed is forfeited, latch still set
    assert cron._counters_seeded is True


# --- rehydrate retries ------------------------------------------------------


async def test_rehydrate_rehydrate_retries_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_skips_live_ladder(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    cron.retry_state["j"] = JobRetryState(1.0, 2.0, 10.0)
    await cron._rehydrate_retries()  # live in-memory ladder outranks ledger
    assert cron.retry_state["j"].task is None


async def test_rehydrate_rehydrate_retries_timeout_breaks(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise asyncio.TimeoutError
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_retries()  # hung store aborts the re-arm pass
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise asyncio.CancelledError
        return []

    cron.state_backend.list_records = _list
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_error_continues(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)

    async def _list(stream, **kw):
        if stream.startswith("retries/"):
            raise OSError("boom")
        return []

    cron.state_backend.list_records = _list
    await cron._rehydrate_retries()  # a store error skips the job
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_durable_lookup_error(tmp_path):
    # the superseded-by-run memo seed errors: durable_at stays None (guard
    # left open) and the invalid pending record is then settled, not re-armed.
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=0)  # invalid -> no re-arm
    cron.durable_last_completed_at = _raise_oserror5
    await cron._rehydrate_retries()
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_durable_lookup_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=0)
    cron.durable_last_completed_at = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


async def test_rehydrate_rehydrate_retries_reboot_marker_error(tmp_path):
    # an @reboot pending whose boot-marker probe errors reads as not-covered:
    # the stale ladder is settled (superseded-by-reboot), never re-armed.
    cron = await _rehydrate_state_cron(tmp_path, _REBOOT_RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=1)
    cron._reboot_marker_covers = _raise_oserror5
    await cron._rehydrate_retries()
    assert "j" not in cron.retry_state


async def test_rehydrate_rehydrate_retries_reboot_marker_cancelled(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _REBOOT_RETRY_REHYDRATE)
    await _rehydrate_seed_pending_retry(cron, attempt=1)
    cron._reboot_marker_covers = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._rehydrate_retries()


# --- _validate_pending_retry verdicts ---------------------------------------


def test_rehydrate_validate_pending_retry_invalid_record(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    assert cron._validate_pending_retry("j", job, {"attempt": 0}) is None


def test_rehydrate_validate_pending_retry_config_changed(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    rec = {
        "attempt": 1,
        "notBefore": "1999-01-01T00:00:00+00:00",
        "jobDigest": "stale-digest",
    }
    assert cron._validate_pending_retry("j", job, rec) is None


def test_rehydrate_validate_pending_retry_ok(tmp_path):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)
    job = cron.cron_jobs["j"]
    rec = {
        "attempt": 1,
        "notBefore": "1999-01-01T00:00:00+00:00",
        "jobDigest": job_digest(job),
    }
    validated = cron._validate_pending_retry("j", job, rec)
    assert validated is not None
    attempt, not_before = validated
    assert attempt == 1
    assert not_before == datetime.datetime(1999, 1, 1, tzinfo=UTC)


# --- depends-on-past cancellation propagation -------------------------------


async def test_rehydrate_depends_on_past_cancelled_propagates(tmp_path):
    cron = await _rehydrate_state_cron(tmp_path, _DEP_JOB)
    cron.state_backend.list_records = _raise_cancelled5
    with pytest.raises(asyncio.CancelledError):
        await cron._depends_on_past_ok(cron.cron_jobs["j"])


# --- completion sequencing --------------------------------------------------


async def test_rehydrate_queue_completion_chains_behind_prev():
    # the second completion for one job waits on the first: the serial
    # per-job retry-arm ordering the reaper used to give inline.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    job = _FakeRun5(cron.cron_jobs["j"])
    gate = asyncio.Event()
    calls = []

    async def _slow(j):
        calls.append("slow-start")
        await gate.wait()
        calls.append("slow-end")

    async def _fast(j):
        calls.append("fast")

    cron.handle_job_success = _slow
    cron._queue_job_completion(job, failed=False)
    for _ in range(5):
        await asyncio.sleep(0)
    cron.handle_job_success = _fast
    cron._queue_job_completion(job, failed=False)  # chains behind the slow one
    for _ in range(5):
        await asyncio.sleep(0)
    assert calls == ["slow-start"]  # the fast one is blocked on its prev
    gate.set()
    await cron._drain_completions()
    assert calls == ["slow-start", "slow-end", "fast"]


async def test_rehydrate_queue_completion_reraises_cancelled():
    # a cancellation inside the sequenced handler propagates (it is not
    # swallowed by the defensive except), ending the task cancelled.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    job = _FakeRun5(cron.cron_jobs["j"])
    cron.handle_job_failure = _raise_cancelled5
    cron._queue_job_completion(job, failed=True)
    await cron._drain_completions()
    assert cron._completion_tasks == set()


# --- finished DAG task reaping ----------------------------------------------


async def test_rehydrate_handle_finished_dag_task_survives_dag_error():
    # a DAG scheduler error while recording a task completion is logged, never
    # allowed to kill the reaper; the task is still removed from running_jobs.
    cron = cronstable.cron.Cron(None, config_yaml=_ONE_JOB)
    rj = _FakeRun5(cron.cron_jobs["j"], state_token=None)
    cron.running_jobs["j"].append(rj)

    async def _boom(job):
        raise RuntimeError("dag exploded")

    cron._dag.on_task_finished = _boom
    await cron._handle_finished_dag_task(rj)  # no raise
    assert "j" not in cron.running_jobs


# --- handle_job_failure: stderr log + live retry-task cancel ----------------


async def test_rehydrate_handle_job_failure_logs_stderr_and_cancels_task():
    # a failing run with captured stderr logs it, then an armed-but-live
    # retry task is cancelled before the exhausted ladder is settled.
    cron = cronstable.cron.Cron(None, config_yaml=_RETRY_REHYDRATE)

    async def _sleeper():
        await asyncio.sleep(100)

    state = JobRetryState(0.1, 2.0, 1.0)
    state.count = 5  # already past maximumRetries (2): exhausted branch
    state.task = asyncio.create_task(_sleeper())

    class _FailJob5:
        def __init__(self, config, retry_state):
            self.config = config
            self.retry_state = retry_state
            self.stdout = ""
            self.stderr = "an error happened"

        async def report_failure(self):
            return None

        async def report_permanent_failure(self):
            return None

    job = _FailJob5(cron.cron_jobs["j"], state)
    await cron.handle_job_failure(job)
    # the live task was cancelled; let the cancellation settle, then confirm.
    try:
        await state.task
    except asyncio.CancelledError:
        pass
    assert state.task.cancelled()
