import asyncio
import datetime

import pytest

import cronstable.cron
from cronstable.job import JobRetryState
from tests._cron_helpers import (
    _SLA_RUNTIME_JOB,
    _SLA_STALE_JOB,
    DT,
    RUNTIME,
    STALE,
    TWO_JOBS,
    UTC,
    _noop,
    _sla_report_recorder,
    fixed_current_time,  # noqa: F401
)

# ===================================================================
# Job start/pause/resume, SLA, and the cross-node retry claim machinery
# (start_job_by_name, pause/resume, pause-store refresh, SLA banking/
# observations/report, and the cross-node retry claim/consume machinery)
# ===================================================================

_RETRYCLAIM_RETRY_JOB = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRYCLAIM_RETRY_JOB_DEADLINE = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
    startingDeadlineSeconds: 60
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 60
        backoffMultiplier: 2
"""

_RETRYCLAIM_RETRY_JOB_NO_RETRY = """
jobs:
  - name: j
    command: ls
    schedule: "0 0 * * *"
"""


async def _retryclaim_stateful(tmp_path, yaml, extra=""):
    from tests.test_state import _state_cfg

    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    cfg = _state_cfg("state:\n  path: {}\n{}".format(tmp_path, extra))
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    return cron


async def _retryclaim_stop(cron):
    from tests.test_state import _drain_state_writes

    await _drain_state_writes(cron)
    if cron.state_backend is not None:
        await cron.state_backend.stop()
        cron.state_backend = None


def _retryclaim_foreign(cron, job, host="node-a", secs_stale=120):
    from cronstable.fingerprint import job_digest

    now = cronstable.cron.get_now(datetime.timezone.utc)
    stale = now - datetime.timedelta(seconds=secs_stale)
    return {
        "kind": "pending",
        "attempt": 1,
        "notBefore": stale.isoformat(),
        "jobDigest": job_digest(job),
        "host": host,
        "at": stale.isoformat(),
    }


# --- start_job_by_name ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_start_job_unknown_raises_404():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    with pytest.raises(cronstable.cron.ApiActionError) as ei:
        await cron.start_job_by_name("ghost")
    assert ei.value.status == 404


@pytest.mark.asyncio
async def test_retryclaim_start_job_counts_as_pause_deferred_boot_run(monkeypatch):
    # a manual start of a job whose boot run a pause deferred IS the boot run:
    # the paused-reboot entry is retired and the durable boot marker written.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._paused_reboot_jobs.add("alpha")
    cron._state_configured = True
    gated = []

    async def _gate(job):
        gated.append(job.name)
        return True

    monkeypatch.setattr(cron, "_reboot_boot_gate", _gate)
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job", lambda j: launched.append(j.name) or _noop()
    )
    await cron.start_job_by_name("alpha")
    assert "alpha" not in cron._paused_reboot_jobs
    assert gated == ["alpha"]
    assert launched == ["alpha"]


# --- pause_job_by_name / _refresh_pauses_from_store / _pause_info ----------


@pytest.mark.asyncio
async def test_retryclaim_pause_job_naive_until_gets_utc():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    naive = DT(1999, 12, 31, 13, 0, 0)  # naive, one hour past the frozen now
    await cron.pause_job_by_name("alpha", until=naive)
    got = cron._paused["alpha"].until
    assert got.tzinfo is not None
    assert got == DT(1999, 12, 31, 13, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_retryclaim_refresh_pauses_no_backend_returns():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    assert cron.state_backend is None
    # returns immediately with no backend (no raise)
    await cron._refresh_pauses_from_store()


def test_retryclaim_pause_info_from_record_variants():
    f = cronstable.cron.Cron._pause_info_from_record
    assert f(None) is None
    assert f({"kind": "resumed"}) is None
    # a paused record with an unparseable `until` reads as not paused
    assert f({"kind": "paused", "until": "not-a-date"}) is None
    info = f(
        {
            "kind": "paused",
            "until": "1999-12-31T13:00:00+00:00",
            "since": None,
            "note": 5,
            "by": None,
            "channel": 7,
        }
    )
    assert info is not None
    # non-string audit fields normalise to ""; a missing since defaults to until
    assert info.note == "" and info.by == "" and info.channel == ""
    assert info.since == info.until


@pytest.mark.asyncio
async def test_retryclaim_refresh_pauses_from_store_skip_removed_and_replace(
    tmp_path,
):
    cron = await _retryclaim_stateful(tmp_path, TWO_JOBS)
    try:
        now = cronstable.cron.get_now(datetime.timezone.utc)
        until1 = now + datetime.timedelta(hours=1)
        until2 = now + datetime.timedelta(hours=2)
        # a stream for a job not in the config: the sweep skips it entirely
        await cron.state_backend.append_record(
            "paused/ghost",
            {
                "kind": "paused",
                "since": now.isoformat(),
                "until": until1.isoformat(),
                "note": "",
                "by": "",
                "channel": "",
                "at": now.isoformat(),
                "host": "h",
            },
        )
        # alpha already paused in memory with a DIFFERENT window: the store's
        # newer window replaces it and banks the one it superseded.
        cron._paused["alpha"] = cronstable.cron.PauseInfo(
            since=now - datetime.timedelta(minutes=30),
            until=until1,
            note="",
            by="",
            channel="",
        )
        await cron.state_backend.append_record(
            "paused/alpha",
            {
                "kind": "paused",
                "since": now.isoformat(),
                "until": until2.isoformat(),
                "note": "",
                "by": "",
                "channel": "",
                "at": now.isoformat(),
                "host": "h",
            },
        )
        await cron._refresh_pauses_from_store()
        assert "ghost" not in cron._paused  # removed-job stream skipped
        assert cron._paused["alpha"].until == until2  # window replaced
    finally:
        await _retryclaim_stop(cron)


# --- SLA banking / observations / report ----------------------------------


def test_retryclaim_sla_bank_pause_clamps_ended_at_to_until():
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    cron._sla_last_success["s"] = now  # pin the staleness reference at `now`
    was = cronstable.cron.PauseInfo(
        since=now,
        until=now + datetime.timedelta(hours=1),
        note="",
        by="",
        channel="",
    )
    # ended_at AFTER `until`: it is clamped down to `until` before banking
    cron._sla_bank_pause("s", was, now + datetime.timedelta(hours=2))
    spans = cron._sla_pause_windows.get("s")
    assert spans is not None
    assert spans[-1][1] == now + datetime.timedelta(hours=1)


def test_retryclaim_sla_observations_skips_runjob_without_started_at():
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_RUNTIME_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)

    class _R:
        started_at = None

    cron.running_jobs["s"] = [_R()]
    obs = cron._sla_observations("s", cron.cron_jobs["s"], now)
    threshold, observed, breached = obs[RUNTIME]
    assert observed == 0.0 and breached is False


@pytest.mark.asyncio
async def test_retryclaim_queue_sla_report_waits_for_earlier_tail(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)
    reports = _sla_report_recorder(monkeypatch)
    gate = asyncio.Event()

    async def _blocked():
        await gate.wait()

    prev = asyncio.create_task(_blocked())
    cron._completion_tail["s"] = prev  # an in-flight completion report
    cron._queue_sla_report(cron.cron_jobs["s"], STALE, 3600, 4000.0)
    task = cron._sla_report_tail["s"]
    await asyncio.sleep(0)
    assert not task.done()  # ordered behind the earlier tail
    assert reports == []
    gate.set()
    await asyncio.wait_for(task, timeout=5)
    assert len(reports) == 1
    prev.cancel()


@pytest.mark.asyncio
async def test_retryclaim_queue_sla_report_reraises_cancelled(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_SLA_STALE_JOB)

    async def _cancel(ctx, cfg):
        raise asyncio.CancelledError()

    monkeypatch.setattr(cronstable.cron, "report_sla_breach", _cancel)
    cron._queue_sla_report(cron.cron_jobs["s"], STALE, 3600, 4000.0)
    task = cron._sla_report_tail["s"]
    with pytest.raises(asyncio.CancelledError):
        await task


# --- web resume validation ------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_web_resume_job_rejects_nonstring_by():
    from aiohttp import web

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {}

    class Req:
        can_read_body = True
        match_info = {"name": "alpha"}

        async def json(self):
            return {"by": 123}

    with pytest.raises(web.HTTPBadRequest):
        await cron._web_resume_job(Req())


# --- schedule_retry_job gate/pause returns --------------------------------


@pytest.mark.asyncio
async def test_retryclaim_schedule_retry_paused_returns_when_state_gone():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    cron._paused["alpha"] = cronstable.cron.PauseInfo(
        since=now,
        until=now + datetime.timedelta(hours=1),
        note="",
        by="",
        channel="",
    )
    # no retry_state entry: the paused branch sees state None and returns
    await cron.schedule_retry_job("alpha", 0.0, 1)
    assert "alpha" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_schedule_retry_transient_gate_returns_when_cancelled(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._elect_leader_configured = True
    cron.cluster_manager = None  # Leader fails closed; no positive owner
    state = JobRetryState(0.01, 1, 0.01)
    state.cancelled = True
    cron.retry_state["alpha"] = state
    launched = []
    monkeypatch.setattr(
        cron, "maybe_launch_job", lambda j: launched.append(j.name) or _noop()
    )
    await cron.schedule_retry_job("alpha", 0.0, 1)
    assert launched == []  # returned at the cancelled-state guard, no launch


# --- retry write plumbing -------------------------------------------------


def test_retryclaim_note_retry_write_dropped_warns_when_state_configured(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._state_configured = True
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        cron._note_retry_write_dropped("alpha", "pending")
    assert any(
        "dropping retry-ladder record" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_retryclaim_queue_retry_write_orders_behind_prev_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    gate = asyncio.Event()

    async def _blocked():
        await gate.wait()

    prev = asyncio.create_task(_blocked())
    cron._retry_write_tail["alpha"] = prev
    task = cron._queue_retry_write("alpha", {"kind": "settled"})
    await asyncio.sleep(0)
    assert not task.done()  # ordered behind the in-flight previous write
    gate.set()
    # the append runs with no backend: it notes the drop and returns cleanly
    await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_retryclaim_append_retry_record_survives_backend_error(
    tmp_path, caplog
):
    import logging

    cron = await _retryclaim_stateful(tmp_path, TWO_JOBS)
    try:

        async def _boom(*a, **k):
            raise OSError("disk gone")

        cron.state_backend.append_record = _boom
        with caplog.at_level(logging.WARNING, logger="cronstable"):
            await cron._append_retry_record("alpha", {"kind": "settled"})
        assert any(
            "failed to persist retry state" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_append_pause_record_defers_without_backend():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron._state_configured = True  # a store is configured but torn down
    assert cron.state_backend is None
    await cron._append_pause_record(
        "alpha", {"kind": "paused", "until": "1999-12-31T13:00:00+00:00"}
    )
    assert "alpha" in cron._pause_pending_writes


# --- _retry_consume_ok ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_retry_consume_ok_tolerates_slow_prev_tail(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    appended = []

    async def _append(stream, record, **k):
        appended.append(record)

    cron.state_backend = types.SimpleNamespace(append_record=_append)
    slow = asyncio.create_task(asyncio.sleep(10))
    cron._retry_write_tail["alpha"] = slow
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    ok = await cron._retry_consume_ok("alpha", 1, quiet=True)
    assert ok is True  # the settle wrote once the prev-wait timed out
    assert appended and appended[0]["reason"] == "launched"
    slow.cancel()


@pytest.mark.asyncio
async def test_retryclaim_retry_consume_ok_reraises_cancelled():
    import types

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)

    async def _append(stream, record, **k):
        raise asyncio.CancelledError()

    cron.state_backend = types.SimpleNamespace(append_record=_append)
    with pytest.raises(asyncio.CancelledError):
        await cron._retry_consume_ok("alpha", 1, quiet=True)


# --- _acquire_retry_claim -------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_acquire_retry_claim_timeout_returns_none(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)

    async def _slow(*a, **k):
        await asyncio.sleep(10)

    backend = types.SimpleNamespace(acquire_lease=_slow)
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    got = await cron._acquire_retry_claim(
        backend, cron.cron_jobs["j"], 1, quiet=True
    )
    assert got is None


@pytest.mark.asyncio
async def test_retryclaim_acquire_retry_claim_error_returns_none(caplog):
    import logging
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)

    async def _boom(*a, **k):
        raise OSError("no locks")

    backend = types.SimpleNamespace(acquire_lease=_boom)
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        got = await cron._acquire_retry_claim(
            backend, cron.cron_jobs["j"], 1, quiet=False
        )
    assert got is None
    assert any(
        "retry-claim store call raised" in r.getMessage()
        for r in caplog.records
    )


# --- _retry_consume_decision (cross-node) ---------------------------------


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_eligible_but_no_backend(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    assert cron.state_backend is None
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "launch"  # degrades to the classic consume_ok


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_aborts_on_foreign_record(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    released = []
    lease = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq(*a, **k):
        return lease

    async def _list(*a, **k):
        return [{"host": "another-node", "kind": "pending"}]

    async def _rel(lz):
        released.append(lz)
        raise OSError("release failed")  # swallowed by the finally guard

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq, list_records=_list, release_lease=_rel
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "abort"  # a foreign newest record moved the ladder
    assert released == [lease]


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_defers_for_live_claimer(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read(name):
        return Lease(name=name, holder="rival#1", fence=1, expires_at=9e18)

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # a live claimer holds the lease


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_read_timeout_fail_closed_defers(
    monkeypatch,
):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    cron._state_on_unavailable = "fail-closed"
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read_timeout(name):
        raise asyncio.TimeoutError()

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read_timeout
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # cannot serialize + fail-closed


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_read_cancelled_propagates(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)

    async def _acq_none(*a, **k):
        return None

    async def _read_cancel(name):
        raise asyncio.CancelledError()

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none, read_lease=_read_cancel
    )
    with pytest.raises(asyncio.CancelledError):
        await cron._retry_consume_decision(cron.cron_jobs["j"], 1, quiet=True)


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_adopts_late_lease_and_launches(
    monkeypatch,
):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    released = []
    own = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq_none(*a, **k):
        return None

    async def _read(name):
        return own  # our own late-landing acquire, observed on read-back

    async def _list_boom(*a, **k):
        raise OSError("read fail")  # degrade -> recs=[]

    async def _append(stream, record, **k):
        pass

    async def _rel(lz):
        released.append(lz)

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq_none,
        read_lease=_read,
        list_records=_list_boom,
        release_lease=_rel,
        append_record=_append,
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "launch"
    assert released == [own]  # the adopted lease is released


@pytest.mark.asyncio
async def test_retryclaim_consume_decision_list_error_fail_closed_defers(
    monkeypatch,
):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    cron._state_on_unavailable = "fail-closed"
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    lease = Lease(
        name="l", holder=cron._slot_holder(), fence=1, expires_at=9e18
    )

    async def _acq(*a, **k):
        return lease

    async def _list_boom(*a, **k):
        raise OSError("read fail")

    async def _rel(lz):
        pass

    cron.state_backend = types.SimpleNamespace(
        acquire_lease=_acq, list_records=_list_boom, release_lease=_rel
    )
    decision = await cron._retry_consume_decision(
        cron.cron_jobs["j"], 1, quiet=True
    )
    assert decision == "defer"  # unreadable ladder + fail-closed


# --- _retry_claim_scan ----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_retry_claim_scan_inactive_returns():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    # cross-node resume inactive (no backend) -> returns without scanning
    await cron._retry_claim_scan()


@pytest.mark.asyncio
async def test_retryclaim_retry_claim_scan_logs_and_continues_on_error(
    monkeypatch, caplog
):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    monkeypatch.setattr(cron, "_retry_resume_active", lambda: True)

    async def _boom(name, job):
        raise RuntimeError("scan bug")

    monkeypatch.setattr(cron, "_maybe_claim_retry", _boom)
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        await cron._retry_claim_scan()
    assert any("scanning job" in r.getMessage() for r in caplog.records)


# --- _maybe_claim_retry ---------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_guards(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    # no backend -> returns
    await cron._maybe_claim_retry("j", job)

    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    cron.state_backend = types.SimpleNamespace()  # non-None sentinel
    # a running instance outranks a claim
    cron.running_jobs["j"] = ["run"]
    await cron._maybe_claim_retry("j", job)
    cron.running_jobs["j"] = []
    # a live local ladder (count > 0) outranks
    st = JobRetryState(1, 2, 60)
    st.next_delay()
    cron.retry_state["j"] = st
    await cron._maybe_claim_retry("j", job)
    cron.retry_state.pop("j")
    # the cluster does not currently allow this node to run it
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: False)
    await cron._maybe_claim_retry("j", job)
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_disabled_or_no_retries(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB_NO_RETRY)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    cron.state_backend = types.SimpleNamespace()
    # maximumRetries defaults to 0 for a job with no onFailure.retry block
    await cron._maybe_claim_retry("j", cron.cron_jobs["j"])
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_list_error_returns(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)

    async def _list_boom(*a, **k):
        raise OSError("read fail")

    cron.state_backend = types.SimpleNamespace(list_records=_list_boom)
    await cron._maybe_claim_retry("j", cron.cron_jobs["j"])
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_acquire_timeout_returns(monkeypatch):
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._state_host = "node-b"
    job = cron.cron_jobs["j"]
    foreign = _retryclaim_foreign(cron, job)

    async def _list_ok(*a, **k):
        return [foreign]

    async def _acq_slow(*a, **k):
        await asyncio.sleep(10)

    cron.state_backend = types.SimpleNamespace(
        list_records=_list_ok, acquire_lease=_acq_slow
    )
    monkeypatch.setattr(cronstable.cron, "STATE_OP_TIMEOUT", 0.02)
    await cron._maybe_claim_retry("j", job)  # acquire times out -> no claim
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_release_error_swallowed(monkeypatch):
    import types

    from cronstable.state import Lease

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    monkeypatch.setattr(cron, "_retry_cross_node_eligible", lambda job: True)
    monkeypatch.setattr(cron, "_cluster_allows", lambda job: True)
    cron._state_host = "node-b"
    job = cron.cron_jobs["j"]
    foreign = _retryclaim_foreign(cron, job)

    async def _list_ok(*a, **k):
        return [foreign]

    async def _acq(*a, **k):
        return Lease(name="l", holder="x", fence=1, expires_at=9e18)

    async def _rel_boom(lz):
        raise OSError("release failed")

    async def _claim_false(*a, **k):
        return False

    cron.state_backend = types.SimpleNamespace(
        list_records=_list_ok, acquire_lease=_acq, release_lease=_rel_boom
    )
    monkeypatch.setattr(cron, "_claim_retry_under_lease", _claim_false)
    await cron._maybe_claim_retry("j", job)  # release error is swallowed
    assert "j" not in cron.retry_state


@pytest.mark.asyncio
async def test_retryclaim_maybe_claim_retry_claims_and_arms(tmp_path, monkeypatch):
    import types

    from cronstable.fingerprint import job_digest

    cron = await _retryclaim_stateful(
        tmp_path, _RETRYCLAIM_RETRY_JOB, extra="  topology: shared\n"
    )
    try:
        cron._elect_leader_configured = True
        cron.cluster_manager = types.SimpleNamespace(
            distribution="single-leader",
            is_leader=lambda: True,
            is_quorate=lambda: True,
            has_conflict=lambda: False,
            view_settled=lambda: True,
            is_available_leader=lambda: True,
        )
        assert cron._retry_resume_active() is True
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        armed = []

        async def _fake_sched(name, delay, attempt):
            armed.append((name, delay, attempt))

        monkeypatch.setattr(cron, "schedule_retry_job", _fake_sched)
        await cron._maybe_claim_retry("j", job)
        assert "j" in cron.retry_state  # claimed and armed a local ladder
        # the ladder arms via asyncio.create_task; let it run once so the
        # scheduling call lands before we assert on it.
        await cron.retry_state["j"].task
        assert armed and armed[0][0] == "j" and armed[0][2] == 1
        from tests.test_state import _drain_state_writes

        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records(
            "retries/j", limit=1, newest_first=True
        )
        assert recs[0]["host"] == "node-b"
        assert recs[0]["claimedFrom"] == "node-a"
        assert recs[0]["jobDigest"] == job_digest(job)
    finally:
        await _retryclaim_stop(cron)


# --- _claim_retry_under_lease ---------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_no_backend_false():
    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    now = cronstable.cron.get_now(datetime.timezone.utc)
    ok = await cron._claim_retry_under_lease(
        "j", cron.cron_jobs["j"], {}, 1, now
    )
    assert ok is False


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_recheck_mismatch_false(tmp_path):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        now = cronstable.cron.get_now(datetime.timezone.utc)
        # the record we "saw" differs from what is now newest -> declined
        stale_view = dict(foreign, attempt=2)
        ok = await cron._claim_retry_under_lease(
            "j", job, stale_view, 2, now
        )
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_list_error_false(tmp_path, monkeypatch):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        job = cron.cron_jobs["j"]
        now = cronstable.cron.get_now(datetime.timezone.utc)

        async def _boom(*a, **k):
            raise OSError("read fail")

        cron.state_backend.list_records = _boom
        ok = await cron._claim_retry_under_lease("j", job, {}, 1, now)
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_durable_read_error_false(
    tmp_path, monkeypatch
):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)

        async def _boom(name):
            raise OSError("ledger read fail")

        monkeypatch.setattr(cron, "durable_last_completed_at", _boom)
        ok = await cron._claim_retry_under_lease(
            "j", job, foreign, 1, foreign_notbefore(foreign)
        )
        assert ok is False
    finally:
        await _retryclaim_stop(cron)


@pytest.mark.asyncio
async def test_retryclaim_claim_under_lease_superseded_by_run(tmp_path, monkeypatch):
    cron = await _retryclaim_stateful(tmp_path, _RETRYCLAIM_RETRY_JOB)
    try:
        cron._state_host = "node-b"
        job = cron.cron_jobs["j"]
        foreign = _retryclaim_foreign(cron, job, host="node-a")
        await cron.state_backend.append_record("retries/j", foreign)
        now = cronstable.cron.get_now(datetime.timezone.utc)
        later = (now + datetime.timedelta(minutes=1)).isoformat()

        async def _durable(name):
            return later  # a run finished AFTER the ladder was armed

        monkeypatch.setattr(cron, "durable_last_completed_at", _durable)
        ok = await cron._claim_retry_under_lease(
            "j", job, foreign, 1, foreign_notbefore(foreign)
        )
        assert ok is False
        from tests.test_state import _drain_state_writes

        await _drain_state_writes(cron)
        recs = await cron.state_backend.list_records(
            "retries/j", limit=1, newest_first=True
        )
        assert recs[0]["kind"] == "settled"
        assert recs[0]["reason"] == "superseded-by-run"
    finally:
        await _retryclaim_stop(cron)


def foreign_notbefore(rec):
    return datetime.datetime.fromisoformat(rec["notBefore"])


# --- _retry_record_claimable ----------------------------------------------


def test_retryclaim_retry_record_claimable_variants():
    from cronstable.fingerprint import job_digest

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    f = cron._retry_record_claimable
    now = cronstable.cron.get_now(datetime.timezone.utc)
    stale = (now - datetime.timedelta(seconds=120)).isoformat()
    dig = job_digest(job)
    # not a pending/handoff record
    assert f("j", job, {"kind": "settled"}) is None
    # a bool attempt (bool is an int subclass) is rejected
    assert (
        f("j", job, {"kind": "pending", "attempt": True, "notBefore": stale})
        is None
    )
    # attempt beyond maximumRetries
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 99,
                "notBefore": stale,
                "jobDigest": dig,
                "host": "node-a",
            },
        )
        is None
    )
    # our own pending is rehydration's business, not the scan's
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": stale,
                "jobDigest": dig,
                "host": cron._state_host,
            },
        )
        is None
    )
    # a foreign pending still within the staleness grace: too fresh to claim
    fresh = (now - datetime.timedelta(seconds=1)).isoformat()
    assert (
        f(
            "j",
            job,
            {
                "kind": "pending",
                "attempt": 1,
                "notBefore": fresh,
                "jobDigest": dig,
                "host": "node-a",
                "at": fresh,
            },
        )
        is None
    )
    # a handoff record is immediately claimable (no grace)
    claim = f(
        "j",
        job,
        {
            "kind": "handoff",
            "attempt": 2,
            "notBefore": stale,
            "jobDigest": dig,
            "fromHost": "node-a",
            "at": stale,
        },
    )
    assert claim is not None and claim[0] == 2


def test_retryclaim_retry_record_claimable_deadline_exceeded():
    from cronstable.fingerprint import job_digest

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB_DEADLINE)
    job = cron.cron_jobs["j"]
    now = cronstable.cron.get_now(datetime.timezone.utc)
    # notBefore is older than startingDeadlineSeconds (60): past its deadline
    old = (now - datetime.timedelta(seconds=600)).isoformat()
    rec = {
        "kind": "pending",
        "attempt": 1,
        "notBefore": old,
        "jobDigest": job_digest(job),
        "host": "node-a",
        "at": old,
    }
    assert cron._retry_record_claimable("j", job, rec) is None


# --- _cluster_owner_moved -------------------------------------------------


def test_retryclaim_cluster_owner_moved_variants():
    import types

    cron = cronstable.cron.Cron(None, config_yaml=_RETRYCLAIM_RETRY_JOB)
    job = cron.cron_jobs["j"]
    # a nodeName conflict: nobody positively owns it -> transient, not a move
    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=lambda: True,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="single-leader",
        is_available_leader=lambda: False,
    )
    assert cron._cluster_owner_moved(job) is False
    # spread distribution consults the per-job availability owner
    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=lambda: False,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="spread",
        is_available_job_owner=lambda n: False,
    )
    assert cron._cluster_owner_moved(job) is True
    # a raising manager is a transient fail-closed condition, never a move
    def _boom():
        raise RuntimeError("mgr bug")

    cron.cluster_manager = types.SimpleNamespace(
        has_conflict=_boom,
        is_quorate=lambda: True,
        view_settled=lambda: True,
        distribution="single-leader",
        is_available_leader=lambda: False,
    )
    assert cron._cluster_owner_moved(job) is False


# --- _reap_retry_task -----------------------------------------------------


@pytest.mark.asyncio
async def test_retryclaim_reap_retry_task_ignores_cancelled():
    async def _forever():
        await asyncio.sleep(100)

    task = asyncio.create_task(_forever())
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # a cancelled retry task is retrieved without logging or re-raising
    cronstable.cron.Cron._reap_retry_task("j", task)


@pytest.mark.asyncio
async def test_retryclaim_reap_retry_task_logs_exception(caplog):
    import logging

    async def _die():
        raise RuntimeError("retry boom")

    task = asyncio.create_task(_die())
    await asyncio.wait({task})
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        cronstable.cron.Cron._reap_retry_task("j", task)
    assert any("retry task died" in r.getMessage() for r in caplog.records)
