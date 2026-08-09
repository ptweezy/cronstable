import asyncio

import pytest

import cronstable.cron
from cronstable.state import Lease
from tests._configs import job_yaml
from tests._cron_helpers import (
    fixed_current_time,  # noqa: F401
)

# ---------------------------------------------------------------------------
# Cluster concurrency slot leasing.
#   _log_cluster_role error swallow, maybe_launch_job cluster start-failure
#   cleanup, _prepare_job_api_run secret staging, _slot_fidelity_reason,
#   _acquire_slot_lease, _claim_cluster_slot, _spawn_slot_pursuit,
#   _pursue_replace_slot, _slot_renewer, and the release paths.
# ---------------------------------------------------------------------------


_SLOTLEASE_REAL_SLEEP = asyncio.sleep


async def _slotlease_fast_sleep(_delay=0, *args, **kwargs):
    # collapse the slot renewer / pursuit poll waits (floored at 1.0s) so the
    # loops iterate instantly; loop.time() still advances a hair each pass.
    await _SLOTLEASE_REAL_SLEEP(0)


async def _slotlease_cancel(*tasks):
    for task in tasks:
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except BaseException:
            pass


@pytest.fixture
async def slotlease_reaper():
    """Teardown for the tasks a test leaves running (finding B1): the
    replacement for the four try/finally _slotlease_cancel sites.

    Register an asyncio task directly, or a zero-arg callable resolved at
    teardown for a task the claim under test creates later (for example
    ``lambda: cron._slot_renewers.get("s")``).  Everything registered is
    cancelled and awaited after the test body, pass or fail.
    """
    deferred = []
    yield deferred.append
    await _slotlease_cancel(
        *(item() if callable(item) else item for item in deferred)
    )


def _slotlease_lease(name="slots/s", holder="peer#1", fence=1, expires_at=1e12):
    return Lease(
        name=name, holder=holder, fence=fence, expires_at=expires_at
    )


_SLOTLEASE_CLUSTER_FORBID = job_yaml(
    "s",
    "echo hi",
    extra="    concurrencyScope: cluster\n    concurrencyPolicy: Forbid\n",
)

_SLOTLEASE_CLUSTER_REPLACE = job_yaml(
    "s",
    "echo hi",
    extra="    concurrencyScope: cluster\n    concurrencyPolicy: Replace\n",
)


class _SlotleaseBackend:
    """A single-shot fake state backend for the slot-leasing paths."""

    def __init__(
        self,
        *,
        acquire=None,
        read=None,
        renew=None,
        acquire_exc=None,
        read_exc=None,
        renew_exc=None,
        release_exc=None,
        append_exc=None,
        records=None,
    ):
        self._acquire = acquire
        self._read = read
        self._renew = renew
        self.acquire_exc = acquire_exc
        self.read_exc = read_exc
        self.renew_exc = renew_exc
        self.release_exc = release_exc
        self.append_exc = append_exc
        self.records = records if records is not None else []
        self.released = []
        self.appended = []

    async def acquire_lease(self, name, holder, ttl):
        if self.acquire_exc is not None:
            raise self.acquire_exc
        return self._acquire

    async def read_lease(self, name):
        if self.read_exc is not None:
            raise self.read_exc
        return self._read

    async def renew_lease(self, lease, ttl):
        if self.renew_exc is not None:
            raise self.renew_exc
        return self._renew

    async def release_lease(self, lease):
        if self.release_exc is not None:
            raise self.release_exc
        self.released.append(lease)

    async def append_record(
        self, stream, data, *, prune_keep=None, prune_latest_by=None
    ):
        if self.append_exc is not None:
            raise self.append_exc
        self.appended.append((stream, data))
        return "rid"

    async def list_records(self, stream, *, limit=None, newest_first=False):
        return list(self.records)


# --- _log_cluster_role: swallow a backend read error (7441-7442) -----------


def test_slotlease_log_cluster_role_swallows_backend_error(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    cron._elect_leader_configured = True

    class _Boom:
        def conflict_names(self):
            raise RuntimeError("store unreachable")

    cron.cluster_manager = _Boom()
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        cron._log_cluster_role()  # must not raise
    assert any(
        "error while logging cluster role" in r.message for r in caplog.records
    )


# --- _prepare_job_api_run: stage secrets, skip an unresolvable one ----------


@pytest.mark.asyncio
async def test_slotlease_prepare_job_api_run_skips_unresolvable_secret(
    monkeypatch, caplog
):
    import logging
    import types

    cron = cronstable.cron.Cron(None)
    registered = []

    class _Api:
        base_url = "http://127.0.0.1:65500"
        cacert = None

        def register_run(self, ctx):
            registered.append(ctx)

    cron._job_api = _Api()
    monkeypatch.delenv("SLOTLEASE_UNSET_SECRET", raising=False)
    job = types.SimpleNamespace(
        name="s",
        secrets=[
            {"name": "good", "value": "v1"},
            {"name": "bad", "fromEnvVar": "SLOTLEASE_UNSET_SECRET"},
        ],
        stateAllowedScopes=[],
    )
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        token, env = await cron._prepare_job_api_run(job, None)
    assert token is not None
    assert registered and registered[0].secrets == {"good": "v1"}
    assert any(
        "could not stage secret" in r.message for r in caplog.records
    )
    assert "CRONSTABLE_STATE_URL" in env or env  # env was built


@pytest.mark.asyncio
async def test_prepare_job_api_run_stages_fromfile_secrets_off_loop(
    tmp_path, monkeypatch
):
    # A fromFile secret is a blocking open()/read() inside the awaited
    # launch chain: on a slow or hung secret mount it must stall a worker
    # thread, never the event loop. Value/env secrets stay inline, where
    # the thread hop would cost more than the dict it builds.
    import threading
    import types

    cron = cronstable.cron.Cron(None)
    registered = []

    class _Api:
        base_url = "http://127.0.0.1:65500"
        cacert = None

        def register_run(self, ctx):
            registered.append(ctx)

    cron._job_api = _Api()

    from cronstable import jobapi

    staged_on_loop_thread = []
    real_stage = jobapi._stage_secrets_sync

    def spying_stage(specs, owner):
        staged_on_loop_thread.append(
            threading.current_thread() is threading.main_thread()
        )
        return real_stage(specs, owner)

    monkeypatch.setattr(jobapi, "_stage_secrets_sync", spying_stage)

    secret_file = tmp_path / "token.txt"
    secret_file.write_text("filed-value\n")
    filed = types.SimpleNamespace(
        name="s",
        secrets=[
            {"name": "v", "value": "plain"},
            {"name": "f", "fromFile": str(secret_file)},
        ],
        stateAllowedScopes=[],
    )
    token, _env = await cron._prepare_job_api_run(filed, None)
    assert token is not None
    assert registered[-1].secrets == {"v": "plain", "f": "filed-value"}
    assert staged_on_loop_thread == [False]  # the file read left the loop

    inline = types.SimpleNamespace(
        name="s2",
        secrets=[{"name": "v", "value": "plain"}],
        stateAllowedScopes=[],
    )
    token2, _env2 = await cron._prepare_job_api_run(inline, None)
    assert token2 is not None
    assert registered[-1].secrets == {"v": "plain"}
    assert staged_on_loop_thread[-1] is True  # no hop for memory-only specs


@pytest.mark.asyncio
async def test_same_slot_spawn_burst_is_gated(monkeypatch):
    # A slot that launches many jobs at once must not execute every spawn's
    # synchronous fork/exec setup in one contiguous ready-queue burst; the
    # daemon-wide gate caps how many run at a time so web/SSE/gossip
    # callbacks interleave. The cap is a hard semaphore bound, so the
    # green assertion is timing-independent.
    yaml = "jobs:\n" + "".join(
        "  - name: b%02d\n    command: x\n    schedule: '* * * * *'\n" % i
        for i in range(40)
    )
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    state = {"in_flight": 0, "high": 0, "started": 0}

    async def fake_start(self):
        state["in_flight"] += 1
        state["high"] = max(state["high"], state["in_flight"])
        state["started"] += 1
        await asyncio.sleep(0.01)
        state["in_flight"] -= 1

    monkeypatch.setattr(cronstable.cron.RunningJob, "start", fake_start)
    await cron._launch_concurrently(list(cron.cron_jobs.values()))
    assert state["started"] == 40  # every job still launched
    assert state["high"] <= cronstable.cron._SPAWN_BURST_LIMIT


# --- _slot_fidelity_reason -------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_no_backend():
    cron = cronstable.cron.Cron(None)
    cron.state_backend = None
    assert await cron._slot_fidelity_reason() is None


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_probe_error_is_inconclusive():
    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            raise RuntimeError("probe blip")

    cron.state_backend = _B()
    cron._slot_fidelity = None
    assert await cron._slot_fidelity_reason() is None
    assert cron._slot_fidelity is None  # nothing latched; retried next claim


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_cancelled_propagates():
    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    cron._slot_fidelity = None
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_fidelity_reason()


@pytest.mark.asyncio
async def test_slotlease_slot_fidelity_reason_latches_and_logs(caplog):
    import logging

    cron = cronstable.cron.Cron(None)

    class _B:
        async def verify_locking(self):
            return "locks are advisory only"

    cron.state_backend = _B()
    cron._slot_fidelity = None
    with caplog.at_level(logging.ERROR, logger="cronstable"):
        assert await cron._slot_fidelity_reason() == "locks are advisory only"
    assert cron._slot_fidelity == "locks are advisory only"
    assert any(
        "file locks cannot be trusted" in r.message for r in caplog.records
    )


# --- _acquire_slot_lease: map timeout/error to None, re-raise cancel --------


@pytest.mark.asyncio
async def test_slotlease_acquire_slot_lease_maps_failures_to_none():
    cron = cronstable.cron.Cron(None)
    timed_out = _SlotleaseBackend(acquire_exc=asyncio.TimeoutError())
    errored = _SlotleaseBackend(acquire_exc=RuntimeError("flock ENOLCK"))
    assert await cron._acquire_slot_lease(timed_out, "slots/s") is None
    assert await cron._acquire_slot_lease(errored, "slots/s") is None


@pytest.mark.asyncio
async def test_slotlease_acquire_slot_lease_cancel_propagates():
    cron = cronstable.cron.Cron(None)
    cancelling = _SlotleaseBackend(acquire_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._acquire_slot_lease(cancelling, "slots/s")


# --- _claim_cluster_slot ---------------------------------------------------


def _slotlease_cluster_cron(policy_yaml=_SLOTLEASE_CLUSTER_FORBID, monkeypatch=None):
    cron = cronstable.cron.Cron(None, config_yaml=policy_yaml)
    cron._state_configured = True
    cron._slot_fidelity = ""  # verified: locks fence, skip the probe
    if monkeypatch is not None:

        async def _noop_reconcile(job):
            return None

        monkeypatch.setattr(
            cron, "_reconcile_takeover_inflight", _noop_reconcile
        )
    return cron


@pytest.mark.asyncio
async def test_slotlease_claim_returns_true_when_state_not_configured():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = False
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True


@pytest.mark.asyncio
async def test_slotlease_claim_degrades_when_backend_is_none():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = True
    cron.state_backend = None
    cron._state_on_unavailable = "degrade"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1  # node-local enforcement refcount


@pytest.mark.asyncio
async def test_slotlease_claim_fails_closed_when_backend_is_none():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._state_configured = True
    cron.state_backend = None
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    assert "s" not in cron._slot_refs


@pytest.mark.asyncio
async def test_slotlease_claim_degrades_when_locks_cannot_fence(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend()

    async def _bad_fidelity():
        return "locks are advisory only"

    monkeypatch.setattr(cron, "_slot_fidelity_reason", _bad_fidelity)
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1


@pytest.mark.asyncio
async def test_slotlease_claim_adopts_live_local_lease(
    monkeypatch, slotlease_reaper
):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend()
    live = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    slotlease_reaper(live)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    cron._slot_renewers["s"] = live
    cron._slot_refs["s"] = 1
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 2  # adopted the live lease


@pytest.mark.asyncio
async def test_slotlease_claim_forbid_when_peer_holds_slot(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=None, read=_slotlease_lease())
    seen = []
    monkeypatch.setattr(cron, "_sla_peer_owns_slot", lambda name: seen.append(name))
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    assert seen == ["s"]


@pytest.mark.asyncio
async def test_slotlease_claim_replace_spawns_pursuit(monkeypatch):
    cron = _slotlease_cluster_cron(_SLOTLEASE_CLUSTER_REPLACE, monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=None, read=_slotlease_lease())
    spawned = []

    async def _fake_pursue(job, observed):
        spawned.append((job.name, observed))

    monkeypatch.setattr(cron, "_pursue_replace_slot", _fake_pursue)
    monkeypatch.setattr(cron, "_sla_peer_owns_slot", lambda name: None)
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False
    pursuit = cron._slot_pursuits.get("s")
    if pursuit is not None:
        await pursuit
    assert spawned and spawned[0][0] == "s"


@pytest.mark.asyncio
async def test_slotlease_claim_adopts_own_late_acquire(
    monkeypatch, slotlease_reaper
):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    # acquire timed out (None) but the read shows OUR holder landed the write.
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read=_slotlease_lease(holder=cron._slot_holder())
    )
    # the adopting claim installs a renewer; reap whatever is there at exit
    slotlease_reaper(lambda: cron._slot_renewers.get("s"))
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_leases["s"].holder == cron._slot_holder()
    assert cron._slot_refs["s"] == 1


@pytest.mark.asyncio
async def test_slotlease_claim_expired_unreclaimed_falls_to_policy(
    monkeypatch,
):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    # a foreign lease whose TTL already lapsed: treated as unanswered, so the
    # degrade policy grants a node-local run.
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read=_slotlease_lease(expires_at=1.0)
    )
    cron._state_on_unavailable = "degrade"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    assert cron._slot_refs["s"] == 1


@pytest.mark.asyncio
async def test_slotlease_claim_read_timeout_is_unanswered(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=asyncio.TimeoutError()
    )
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_slotlease_claim_read_error_is_unanswered(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=RuntimeError("EIO")
    )
    cron._state_on_unavailable = "fail-closed"
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is False


@pytest.mark.asyncio
async def test_slotlease_claim_success_cancels_stale_renewer(
    monkeypatch, slotlease_reaper
):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(acquire=_slotlease_lease(holder=cron._slot_holder()))
    # a live renewer with no recorded lease (the adoption branch is skipped):
    # the fresh acquire must cancel it and install a replacement.
    stale = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_renewers["s"] = stale
    slotlease_reaper(stale)
    slotlease_reaper(lambda: cron._slot_renewers.get("s"))
    assert await cron._claim_cluster_slot(cron.cron_jobs["s"]) is True
    new = cron._slot_renewers["s"]
    assert new is not stale  # replaced by a fresh renewer
    await _slotlease_cancel(stale, new)
    assert stale.cancelled()


# --- _spawn_slot_pursuit: single-flight ------------------------------------


@pytest.mark.asyncio
async def test_slotlease_spawn_slot_pursuit_is_single_flight(
    slotlease_reaper,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    job = cron.cron_jobs["s"]
    existing = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_pursuits["s"] = existing
    slotlease_reaper(existing)
    cron._spawn_slot_pursuit(job, _slotlease_lease())
    assert cron._slot_pursuits["s"] is existing  # not replaced


# --- _pursue_replace_slot --------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_no_backend_returns():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = None
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_append_failure_gives_up(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(append_exc=RuntimeError("no write"))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert any(
        "could not record the cluster Replace cancel" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_stops_on_shutdown():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    backend = _SlotleaseBackend(read=_slotlease_lease())
    cron.state_backend = backend
    cron._stop_event.set()
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert backend.appended  # the cancel request was recorded before stopping


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_relaunches_when_slot_frees(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    # append ok, then the slot reads back free (holder released) -> relaunch.
    cron.state_backend = _SlotleaseBackend(read=None)
    relaunched = []

    async def _fake_launch(job, **kwargs):
        relaunched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", _fake_launch)
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert relaunched == ["s"]


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_read_error_is_ignored(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    # append ok; read raises (kept as observed = still foreign held), the
    # deadline (2 * ttl == 0) then trips and the launch is skipped.
    cron.state_backend = _SlotleaseBackend(read_exc=RuntimeError("blip"))
    cron._slot_ttl = 0.0
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert any("did not yield" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_read_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(read_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


# --- _slot_renewer ---------------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_returns_when_lease_gone(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend()
    # no lease recorded -> the renewer stands down on its first cycle.
    await asyncio.wait_for(cron._slot_renewer("s"), timeout=5)


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_retires_when_superseded(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(renew=_slotlease_lease(holder="me#x"))
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    # _slot_renewers has NO entry for "s": the renewer sees it was retired
    # mid-renew and stands down without touching _slot_leases.
    await asyncio.wait_for(cron._slot_renewer("s"), timeout=5)
    assert "s" in cron._slot_leases  # left for the finish path to release


class _SlotleaseRenewBackend:
    """Stateful renewer backend driven by a per-call script."""

    def __init__(self, cron, *, list_script, renew_script, read_script):
        self.cron = cron
        self.list_script = list(list_script)
        self.renew_script = list(renew_script)
        self.read_script = list(read_script)
        self.n = 0

    async def list_records(self, stream, *, limit=None, newest_first=False):
        self.n += 1
        item = self.list_script[min(self.n - 1, len(self.list_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    async def renew_lease(self, lease, ttl):
        item = self.renew_script[min(self.n - 1, len(self.renew_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    async def read_lease(self, name):
        item = self.read_script[min(self.n - 1, len(self.read_script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_list_error_then_taken_over(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine
    backend = _SlotleaseRenewBackend(
        cron,
        # cycle 1: list raises -> recs=[]; renew succeeds -> stored, continue.
        # cycle 2: list ok empty; renew denied (None) -> read shows a peer
        #          took the slot over -> pop + return.
        list_script=[RuntimeError("list blip"), []],
        renew_script=[
            _slotlease_lease(holder=cron._slot_holder(), fence=6),
            None,
        ],
        read_script=[None, _slotlease_lease(holder="peer#9", fence=9)],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases  # dropped on the takeover


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_renew_timeout_then_error_then_taken_over(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine
    backend = _SlotleaseRenewBackend(
        cron,
        list_script=[[], [], []],
        # cycle 1: renew times out -> continue; cycle 2: renew errors ->
        # warn+continue; cycle 3: renew denied -> read shows takeover -> return.
        renew_script=[asyncio.TimeoutError(), RuntimeError("EIO"), None],
        read_script=[None, None, _slotlease_lease(holder="peer#3", fence=7)],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_replace_request_cancels_instance(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    mine = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    cron._slot_leases["s"] = mine

    class _FakeRun:
        def __init__(self):
            self.replaced = False
            self.cancelled = False

        async def cancel(self):
            self.cancelled = True

    run = _FakeRun()
    cron.running_jobs["s"] = [run]
    backend = _SlotleaseRenewBackend(
        cron,
        # cycle 1: a cancel record aimed at our fence -> cancel the instance;
        #          renew denied -> read shows our own lease -> keep going.
        # cycle 2: no record; renew denied -> read shows takeover -> return.
        list_script=[
            [{"kind": "cancel", "fence": 5, "by": "peerZ"}],
            [],
        ],
        renew_script=[None, None],
        read_script=[
            _slotlease_lease(holder=cron._slot_holder(), fence=5),
            _slotlease_lease(holder="peer#4", fence=8),
        ],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert run.replaced is True and run.cancelled is True


# --- release paths ---------------------------------------------------------


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_decrements_refcount():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_refs["s"] = 2
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    assert cron._slot_refs["s"] == 1  # still one user; lease kept


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_kept_while_instance_runs():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_refs["s"] = 1
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    cron.running_jobs["s"] = [object()]  # a spawning instance still present
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    assert "s" in cron._slot_leases  # not released out from under the run
    assert "s" not in cron._slot_refs


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_releases_lease():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    lease = _slotlease_lease(holder=cron._slot_holder())
    backend = _SlotleaseBackend()
    cron.state_backend = backend
    cron._slot_refs["s"] = 1
    cron._slot_leases["s"] = lease
    renewer = asyncio.create_task(_SLOTLEASE_REAL_SLEEP(30))
    cron._slot_renewers["s"] = renewer
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    for task in list(cron._pending_state_writes):
        await task
    assert renewer.cancelled() or renewer.done()
    assert backend.released == [lease]


@pytest.mark.asyncio
async def test_slotlease_release_cluster_slot_phantom_cleanup():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend(read=None)  # no lease on disk -> nothing to free
    cron.state_backend = backend
    cron._slot_refs["s"] = 1  # a degraded launch left a ref but no lease
    await cron._release_cluster_slot(cron.cron_jobs["s"])
    for task in list(cron._pending_state_writes):
        await task
    assert backend.released == []


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = None
    await cron._release_slot_lease("s", _slotlease_lease())  # returns, no error


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_skips_when_reclaimed():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend()
    cron.state_backend = backend
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    await cron._release_slot_lease("s", _slotlease_lease())
    assert backend.released == []  # a fresh claim adopted the on-disk lease


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_warns_on_error(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(release_exc=RuntimeError("EROFS"))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._release_slot_lease("s", _slotlease_lease())
    assert any(
        "failed to release the concurrency slot" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_no_backend():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = None
    await cron._release_phantom_slot("s")  # returns, no error


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_releases_own_lease():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    mine = _slotlease_lease(holder=cron._slot_holder())
    backend = _SlotleaseBackend(read=mine)
    cron.state_backend = backend
    await cron._release_phantom_slot("s")
    assert backend.released == [mine]


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_swallows_error():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(read_exc=RuntimeError("EIO"))
    await cron._release_phantom_slot("s")  # best-effort; no raise


# --- maybe_launch_job: cluster start-failure hands the slot back -----------


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_job_releases_slot_on_start_failure(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    job = cron.cron_jobs["s"]

    async def _claim(j):
        return True

    monkeypatch.setattr(cron, "_claim_cluster_slot", _claim)
    released = []

    async def _release(j):
        released.append(j.name)

    monkeypatch.setattr(cron, "_release_cluster_slot", _release)
    finished = []

    class _Api:
        async def finish_run(self, token):
            finished.append(token)

    cron._job_api = _Api()

    async def _fake_prepare(j, rs):
        return ("tok123", {"CRONSTABLE_RUN_ID": "rid"})

    monkeypatch.setattr(cron, "_prepare_job_api_run", _fake_prepare)

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)
    assert released == ["s"]
    assert finished == ["tok123"]


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_job_releases_slot_on_prepare_cancel(
    monkeypatch,
):
    """Cancellation inside _prepare_job_api_run hands the claim back.

    Staging fromFile secrets awaits the executor, so a client gone
    mid-POST cancels the launch right there; the claim made a few lines
    above must not outlive it (the leak pinned here kept the refcount and
    renew task forever, wedging a Forbid job cluster-wide).  finish_run
    stays uncalled: the await precedes register_run, so no token exists.
    """
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    job = cron.cron_jobs["s"]

    async def _claim(j):
        return True

    monkeypatch.setattr(cron, "_claim_cluster_slot", _claim)
    released = []

    async def _release(j):
        released.append(j.name)

    monkeypatch.setattr(cron, "_release_cluster_slot", _release)
    finished = []

    class _Api:
        async def finish_run(self, token):
            finished.append(token)

    cron._job_api = _Api()
    parked = asyncio.Event()

    async def _hung_prepare(j, rs):
        parked.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(cron, "_prepare_job_api_run", _hung_prepare)
    task = asyncio.create_task(cron.maybe_launch_job(job))
    await parked.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert released == ["s"]
    assert finished == []


# --- cancellation propagates through every store call (never swallowed) -----


@pytest.mark.asyncio
async def test_slotlease_claim_read_lease_cancel_propagates(monkeypatch):
    cron = _slotlease_cluster_cron(monkeypatch=monkeypatch)
    cron.state_backend = _SlotleaseBackend(
        acquire=None, read_exc=asyncio.CancelledError()
    )
    with pytest.raises(asyncio.CancelledError):
        await cron._claim_cluster_slot(cron.cron_jobs["s"])


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_append_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron.state_backend = _SlotleaseBackend(append_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_list_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_renewer("s")


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_renew_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            return []

        async def renew_lease(self, lease, ttl):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    with pytest.raises(asyncio.CancelledError):
        await cron._slot_renewer("s")


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_readback_error_then_takeover(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)
    backend = _SlotleaseRenewBackend(
        cron,
        list_script=[[], []],
        # renew denied both cycles; cycle 1 read-back errors -> continue,
        # cycle 2 read-back shows a takeover -> pop + return.
        renew_script=[None, None],
        read_script=[
            RuntimeError("blip"),
            _slotlease_lease(holder="peer#2", fence=8),
        ],
    )
    cron.state_backend = backend
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    await asyncio.wait_for(task, timeout=5)
    assert "s" not in cron._slot_leases


@pytest.mark.asyncio
async def test_slotlease_slot_renewer_readback_cancel_propagates(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder(), fence=5)

    class _B:
        async def list_records(self, stream, *, limit=None, newest_first=False):
            return []

        async def renew_lease(self, lease, ttl):
            return None  # denied -> falls through to the read-back

        async def read_lease(self, name):
            raise asyncio.CancelledError

    cron.state_backend = _B()
    task = asyncio.create_task(cron._slot_renewer("s"))
    cron._slot_renewers["s"] = task
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_slotlease_release_slot_lease_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(release_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._release_slot_lease("s", _slotlease_lease())


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_skips_when_claim_present():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    backend = _SlotleaseBackend(read=_slotlease_lease(holder=cron._slot_holder()))
    cron.state_backend = backend
    cron._slot_leases["s"] = _slotlease_lease(holder=cron._slot_holder())
    await cron._release_phantom_slot("s")
    assert backend.released == []  # a live claim owns the slot; not a phantom


@pytest.mark.asyncio
async def test_slotlease_release_phantom_slot_cancel_propagates():
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_FORBID)
    cron.state_backend = _SlotleaseBackend(read_exc=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await cron._release_phantom_slot("s")


# Non-cluster start-failure cleanup and the slot pursuit poll loop.

_SLOTLEASE_NODE_JOB = job_yaml("s", "echo hi")


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_node_scope_start_failure_finishes_run(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_NODE_JOB)
    job = cron.cron_jobs["s"]
    released = []

    async def _release(j):
        released.append(j.name)

    monkeypatch.setattr(cron, "_release_cluster_slot", _release)
    finished = []

    class _Api:
        async def finish_run(self, token):
            finished.append(token)

    cron._job_api = _Api()

    async def _fake_prepare(j, rs):
        return ("tokN", {})

    monkeypatch.setattr(cron, "_prepare_job_api_run", _fake_prepare)

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)
    assert released == []  # node scope: no cluster slot to hand back
    assert finished == ["tokN"]  # but the job-API run registration is dropped


@pytest.mark.asyncio
async def test_slotlease_maybe_launch_start_failure_without_job_api(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_NODE_JOB)
    job = cron.cron_jobs["s"]

    async def _fake_prepare(j, rs):
        return (None, {})

    monkeypatch.setattr(cron, "_prepare_job_api_run", _fake_prepare)
    cron._job_api = None

    class _BoomRun:
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            raise RuntimeError("spawn failed")

    monkeypatch.setattr(cronstable.cron, "RunningJob", _BoomRun)
    with pytest.raises(RuntimeError):
        await cron.maybe_launch_job(job)


@pytest.mark.asyncio
async def test_slotlease_pursue_replace_polls_until_slot_frees(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _slotlease_fast_sleep)
    cron = cronstable.cron.Cron(None, config_yaml=_SLOTLEASE_CLUSTER_REPLACE)
    cron._slot_ttl = 100.0  # deadline is far off, so the poll loop iterates
    reads = [_slotlease_lease(), None]  # foreign held, then the holder yields

    class _B:
        async def append_record(self, stream, data, *, prune_keep=None,
                                 prune_latest_by=None):
            return "rid"

        async def read_lease(self, name):
            return reads.pop(0) if reads else None

    cron.state_backend = _B()
    relaunched = []

    async def _launch(job, **kwargs):
        relaunched.append(job.name)
        return True

    monkeypatch.setattr(cron, "maybe_launch_job", _launch)
    await cron._pursue_replace_slot(cron.cron_jobs["s"], _slotlease_lease())
    assert relaunched == ["s"]
