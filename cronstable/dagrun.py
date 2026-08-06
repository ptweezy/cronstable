"""The DAG runtime: the driver that turns the pure state machine durable.

:mod:`cronstable.dag` is the pure, I/O-free state machine; this module is the
daemon-side driver that gives it a store, a clock, leases and subprocesses.  It
is the DAG analogue of the retry / catch-up / slot machinery on
:class:`cronstable.cron.Cron`, kept in its own module so the (large)
orchestration
surface does not bloat cron.py; a :class:`DagScheduler` holds a back-reference
to the owning ``Cron`` and reuses its seams (the state backend, the loopback
job-state API, ``_compute_next_fire``, ``_cluster_allows``, ``running_jobs``,
the ``_proc_token`` / ``_state_host`` identity, ``_track_state_write``).

The durable model, in one paragraph: each ``dag_run`` is a single mutable
*document* (``dagrun/<dag>`` keyed by a run key), advanced only by the node
that holds that run's advance *lease* (``dagadvance/<dag>/<key>``).  An advance
is one flock-guarded read-modify-write that atomically claims every ready task
``pending -> running``; the driver then launches a real subprocess per claimed
task (through the same :class:`~cronstable.job.RunningJob` path a job uses,
with the durable env injected so the task can call ``cronstable xcom`` /
``artifact`` /
``state``), and records the pid in a second RMW.  A task's completion is routed
back here by the reaper, recorded, and triggers a fresh advance.  Because the
lease gates who advances *and* who reconciles, and the RMW claim is atomic, the
fleet never double-advances or double-launches a task; on a crash the durable
per-task state is the source of truth and a resumed (or adopting) node
reconciles interrupted tasks from it -- at-least-once, never at-most-once.
"""

import asyncio
import datetime
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from cronstable import _json, dag, platform
from cronstable.cronexpr import CronTab
from cronstable.dag import DagSpec
from cronstable.job import RunningJob
from cronstable.state import DOC_KEEP, Lease, StateBackend

logger = logging.getLogger("cronstable.dagrun")

# Every awaited backend op is capped so a wedged store cannot hang the advance
# loop forever (mirrors cron.STATE_OP_TIMEOUT; kept local so this module does
# not import cron, which imports it).
STATE_OP_TIMEOUT = 10.0

# TTL of the per-run advance lease; renewed at a third of it while the run is
# active on this node, so only its owner advances/reconciles it.  A lapse
# (owner stopped renewing == owner gone) lets a peer adopt the run.
DAG_LEASE_TTL = 30.0

# How often the scheduler re-checks for due scheduled runs and orphaned runs to
# adopt (leaderless per-node chore; the per-run lease does the real gating).
SCHEDULE_CHECK_INTERVAL = 20.0
ADOPT_SCAN_INTERVAL = 30.0
GC_INTERVAL = 3600.0

# How often the adopt scan does a FULL body listing instead of the cheap
# keys-only pass.  Terminality is monotonic, so the per-dag terminal-key cache
# lets the ordinary scan skip re-reading runs already known finished (with
# retainRuns: 50 the old scan re-read and re-parsed all ~50 documents per dag
# every 30s mostly to rediscover that).  The periodic full pass, plus the
# hourly GC's full listing, rebuilds the cache from actual bodies, bounding
# how long the one known stale-cache corner (a terminal run GC-deleted and
# re-created under the SAME key by an operator backfill within a single scan
# interval, then orphaned) can delay that run's adoption.
ADOPT_FULL_REFRESH = 600.0

# How often the owner re-advances a run that is blocked on an approval gate, so
# a decision recorded on a peer node (which cannot advance a run it does not
# own) is acted on within a few seconds rather than a full idle re-advance.
APPROVAL_POLL_INTERVAL = 5.0

# Hard cap on how many missed occurrences a single catch-up replays, mirroring
# cron.MAX_CATCHUP_OCCURRENCES so a long outage cannot stampede.
DAG_MAX_CATCHUP = 100

# Durable checkpoint stream for a dag's catch-up cycles, the dag twin of the
# job engine's ``catchup/<job>`` stream (cron.CATCHUP_STREAM_PREFIX; a
# distinct prefix because dag and job names would otherwise share one stream
# namespace).  An ``open`` record persists the owed watermark BEFORE the
# jitter sleeper is spawned: the replay targets live only in task memory
# across the sleep, while the scheduled path (and _fire_past_gap's
# current-slot create) keeps landing NEWER run documents, so a restart
# mid-jitter would otherwise recompute "nothing missed" from the advanced
# document watermark and the backfill would vanish without a log line.  The
# boot-time _catch_up hoists its watermark back to an open checkpoint's,
# exactly as Cron._missed_occurrences does.  Keep depth mirrors
# cron.CATCHUP_STREAM_KEEP.
DAG_CATCHUP_STREAM_PREFIX = "catchup-dag/"
DAG_CATCHUP_STREAM_KEEP = 8

# The furthest back _fire_forward will retroactively replay occurrence by
# occurrence, mirroring cron.CATCHUP_LIMIT for jobs: a gap within this bound
# is tick overhead (a slow advance pass, many simultaneous fires) and every
# occurrence in it is fired so a frequent DAG is not silently dropped; a
# larger gap is a stall/suspend/clock jump WHILE RUNNING, and replaying it
# blind would stampede backdated runs the operator's onMissed policy never
# asked for. Such a gap is instead resumed past like a boot outage: fire the
# current slot if it matches, resync strictly future, and leave the gap to
# the same onMissed catch-up the boot seed uses (see _fire_past_gap).
DAG_CATCHUP_LIMIT = datetime.timedelta(seconds=10)

# When an advance cannot proceed (a failed pass, or a lapsed lease that cannot
# be verified against the store), re-check after this long instead of leaving
# a due wake in place -- a fast-failing store must not spin the main loop.
ADVANCE_RETRY_DELAY = 5.0

# How often the main loop re-reads a wake hint whose advance is already in
# flight (see _hold_advancing).  Such a hint says "this run wants servicing but
# the pass that will say when has not reported back yet": left due, it makes
# next_wake_delay return 0.0 for the pass's whole duration, so the loop re-runs
# its entire housekeeping block thousands of times a second.  Skipping it
# outright is not an option either -- nothing wakes the loop when the pass
# lands, and the fresh hint can be nearer than the sleep already committed to
# (an approval poll, a due retry, a sensor poke) -- so it is POLLED at this
# interval: the loop notices the pass's real hint within one poll of it being
# written, and pays a bounded handful of passes a second meanwhile.
ADVANCE_POLL_FLOOR = 0.2

# A failed completion RMW is re-attempted on later service passes with this
# bounded backoff.  Retrying until it lands is safe (mark_task_finished is
# fenced and idempotent) and necessary: giving up would wedge the run forever,
# since the RUNNING entry is protected from reconciliation by its own proc
# token while this daemon lives.
COMPLETION_RETRY_DELAY = 5.0
COMPLETION_RETRY_MAX_DELAY = 60.0

#: In list_dags' cached rollup, when this many run docs of one dag still need a
#: body read (cold cache, or a burst of new/non-terminal runs), fetch them in
#: one list_documents sweep instead of that many individual read_document round
#: trips. The steady state (a handful of running runs, the rest terminal and
#: cached) stays below this and reads only the few that changed.
DAG_ROLLUP_BULK_THRESHOLD = 8

#: How long one dag's run-summary LIST may be served from memory before the
#: store is listed again.  The per-run summary cache above already avoids
#: re-PARSING terminal runs, but the keys listing itself still hit the store
#: once per dag per request: with the dashboard's /dags poll (and any open
#: run drawer) that was N_dags listings every ~3s per viewer, forever, on a
#: quiescent store.  Every LOCAL write funnels through _mutate/_delete_run_doc
#: and pops the memo, so this-node changes render immediately; the TTL only
#: bounds how late another node's writes appear in the index rollup, which is
#: well inside the gossip staleness the fleet view already tolerates.  Sits
#: beside the same tradeoff cron's per-job trends cache (5s TTL, busted on
#: local completion) already made.
DAG_SUMMARY_LIST_TTL = 5.0

RunRef = Tuple[str, str]  # (dag_name, run_key)

#: XCom payload size at or above which a mapped fan-out's parse (and its
#: portability walk) runs on a worker thread instead of the scheduler's
#: event loop.  The payload cap is dag.MAX_MAPPED_XCOM_BYTES (16 MiB), and
#: parsing a multi-MB list inline stalled every handler and heartbeat for
#: tens of ms per expansion; below this floor the parse is cheaper than the
#: thread hop it would buy (a 64 KiB bytes-path parse is a few hundred us,
#: about the hop's own dispatch cost).
_XCOM_PARSE_OFFLOAD_MIN = 64 * 1024


def _parse_portable_xcom(data: bytes) -> Any:
    """Parse a mapped-task XCom payload; portability-check a usable list.

    Factored out of :meth:`DagScheduler._mapped_items` so the large-payload
    branch can run the WHOLE thing on a worker thread.  The bytes go
    straight to ``_json.loads``: the old ``data.decode()`` allocated a full
    second copy and forced the str branch, whose wide-int prescan is a
    whole-payload regex rather than the bytes branch's translate fast path.
    An over-long list skips the portability walk on purpose:
    ``_apply_expansions`` rejects it at its own MAX_MAPPED_ITEMS check, so
    the O(len) walk would be pure waste on a list never embedded in the run
    document.  Raises ``_json.UnsupportedValue`` for a non-portable value
    and any other ``ValueError`` for undecodable bytes or invalid JSON,
    which the caller maps to its existing warn-and-empty arms.
    """
    parsed = _json.loads(data)
    if isinstance(parsed, list) and len(parsed) <= dag.MAX_MAPPED_ITEMS:
        _json.ensure_portable(parsed)
    return parsed


def _now() -> float:
    """Wall-clock epoch seconds for document timestamps and poke schedules.

    A seam (like :func:`cronstable.jobstate._now`) so tests drive poke/retry
    timing without touching the lease clock; plain ``time.time`` in production.
    """
    return time.time()


def _jitter(max_jitter: float) -> float:
    """A random poke jitter in ``[0, max_jitter]`` (0 when disabled)."""
    if max_jitter <= 0:
        return 0.0
    return random.uniform(0.0, max_jitter)  # noqa: S311 - not cryptographic


@dataclass
class _DagRef:
    """The marker a launched DAG-task :class:`RunningJob` carries.

    Lets the reaper route the task's completion back to the right run/task
    without the scheduler having to track every live subprocess itself.
    """

    dag_name: str
    run_key: str
    run_id: str
    task_id: str
    taskkey: str
    # the claim identity of THIS instance: the proc token that claimed it and
    # the attempt it is running.  Carried back to mark_task_finished so a
    # superseded attempt's late completion cannot terminalise a re-claimed one.
    proc: str
    attempt: int
    # for a sensor, the pokeCount observed at this poke's claim (None for a
    # plain task).  Extends the completion fence to pokes: a re-poke re-stamps
    # the SAME proc token and never bumps attempt, so only the poke number
    # distinguishes a stale queued completion from the live in-flight poke.
    poke: Optional[int] = None


class DagScheduler:
    """Drives every DAG's runs: schedules, advances, launches, reconciles."""

    def __init__(self, cron: Any) -> None:
        self._cron = cron
        # runs this node owns (holds the advance lease for) -> the held lease.
        self._owned: Dict[RunRef, Lease] = {}
        self._renewers: Dict[RunRef, asyncio.Task] = {}
        self._locks: Dict[RunRef, asyncio.Lock] = {}
        # refs whose in-flight advance must run once more before its lock
        # is released: the burst-coalescing latch (see advance_one).
        self._advance_again: Set[RunRef] = set()
        # soonest wall-clock instant an owned run wants another advance (a due
        # sensor poke or task retry); drives the loop's sleep cap.
        self._wake: Dict[RunRef, float] = {}
        # refs whose advance pass is in flight, refcounted (a periodic sweep
        # holds its whole due batch while each ref's own advance_one holds it
        # again).  A held ref's wake hint is polled at ADVANCE_POLL_FLOOR
        # instead of read as due: the pass ALREADY under way is what decides
        # when this run next wants advancing, so a hint left due until it
        # reports back buys nothing and pins the main loop's sleep at zero --
        # a full-core housekeeping spin for the pass's whole duration.  Every
        # hold is paired with a drop in a ``finally``, so a failed or
        # cancelled advance cannot leave a run polling forever.
        self._advance_pending: Dict[RunRef, int] = {}
        # deferred catch-up replays sleeping out their per-dag jitter offset
        # (see _catch_up); cancelled by shutdown() and forget(), since a
        # replay must never land on a torn-down or swapped store.
        self._catchup_tasks: Set[asyncio.Task] = set()
        # dag name -> run keys this node has SEEN terminal.  Terminality is
        # monotonic, so the adopt scan skips re-reading these (see
        # _adopt_one_dag); pruned against each key listing, rebuilt from
        # bodies by every full adopt pass and every GC pass, and a key is
        # evicted when this node (re-)creates a run under it.
        self._terminal_run_keys: Dict[str, Set[str]] = {}
        # dag name -> {run key -> cached per-run summary} backing list_dags'
        # rollup. A terminal run's summary is immutable, so it is cached and
        # never re-read; non-terminal (running/pending) runs are re-read each
        # call. Pruned against each key listing (GC'd runs drop out) and the
        # entry is evicted when a run is (re-)created under the key (see
        # _create_doc). Note this is only PART of _terminal_run_keys'
        # invalidation: that one is additionally rebuilt from store bodies by
        # every full adopt and GC pass, whereas nothing rebuilds this cache on
        # a timer. A terminal entry therefore survives until something evicts
        # it by key, which is why forget() has to clear it explicitly on a
        # backend swap: run keys are deterministic, so the new store's runs
        # would otherwise read the old store's cached terminal state.
        self._dag_summary_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # dag name -> (monotonic stamp, summaries): the short-TTL memo over
        # _run_summaries' RESULT, so the /dags + run-drawer poll traffic of
        # a quiescent dag costs zero store listings between local writes.
        # Popped by _mutate/_delete_run_doc (local changes must render at
        # once), swept with the summary cache in forget(), and pruned of
        # removed dags by list_dags.  See DAG_SUMMARY_LIST_TTL.
        self._summaries_memo: Dict[
            str, Tuple[float, List[Dict[str, Any]]]
        ] = {}
        self._next_full_adopt = 0.0
        # in-memory forward next-fire index per scheduled dag (like the job
        # next-fire index); catch-up of missed runs is a one-time seed step.
        self._next_logical: Dict[str, datetime.datetime] = {}
        # dag name -> the schedule signature it was seeded under, so a reload
        # that changes a schedule (or disables a dag) re-seeds strictly-future
        # instead of replaying the gap (mirrors the job _refresh_schedule).
        self._seeded: Dict[str, str] = {}
        # dag name -> the schedule signature whose seed raised, so a poisoned
        # dag is logged once and skipped (a reload changing its schedule
        # retries) instead of failing -- and spamming -- every seed cadence.
        self._seed_failed: Dict[str, str] = {}
        # (ref, taskkey) -> a completion whose RMW failed, queued for retry on
        # later service passes (see COMPLETION_RETRY_DELAY).
        self._pending_completions: Dict[
            Tuple[RunRef, str], Dict[str, Any]
        ] = {}
        # run ref -> task completions the reaper has handed over but not yet
        # recorded. on_task_finished buffers here; flush_completions (called
        # once the reaper has drained a whole batch of finished jobs) records
        # each run's buffered completions in ONE document RMW instead of one
        # per task -- the win for a mapped fan-out finishing together.
        self._completion_buffer: Dict[RunRef, List[Dict[str, Any]]] = {}
        # (dag, run_key, taskkey) approval gates this node has already fired an
        # `approval_waiting` notification for.  _do_advance observes a waiting
        # gate on every pass while it is parked, so this dedups to one alert
        # per gate; a run's entries drop when it reaches a terminal state.
        self._approval_notified: Set[Tuple[str, str, str]] = set()
        self._service_task: Optional[asyncio.Task] = None
        self._next_sched_check = 0.0
        self._next_adopt = 0.0
        self._next_gc = 0.0

    # --- accessors -------------------------------------------------------

    def _backend(self) -> Optional[StateBackend]:
        backend: Optional[StateBackend] = self._cron.state_backend
        return backend

    def _dags(self) -> Dict[str, Any]:
        return getattr(self._cron, "cron_dags", {})

    def has_dags(self) -> bool:
        return bool(self._dags())

    @staticmethod
    def _ns(dag_name: str) -> str:
        return dag.DAG_RUN_NS_PREFIX + dag_name

    @staticmethod
    def _lease_name(ref: RunRef) -> str:
        return "{}{}/{}".format(dag.DAG_LEASE_PREFIX, ref[0], ref[1])

    def _wrap(self, transform):
        """Adapt a :mod:`cronstable.dag` transform to the backend sentinel.

        The pure module returns its own keep sentinel (to stay import-free of
        :mod:`cronstable.state`); translate it to the real ``DOC_KEEP`` the
        backend compares by identity.
        """

        def wrapped(body):
            new_body, result = transform(body)
            if dag.is_keep(new_body):
                return DOC_KEEP, result
            return new_body, result

        return wrapped

    # --- backend op helpers (all bounded) --------------------------------

    async def _mutate(
        self, dag_name: str, key: str, transform
    ) -> "Tuple[Optional[Dict[str, Any]], Any]":
        backend = self._backend()
        if backend is None:
            return None, None
        result = await asyncio.wait_for(
            backend.mutate_document(self._ns(dag_name), key, transform),
            timeout=STATE_OP_TIMEOUT,
        )
        # every local run-doc write funnels through here: pop the summary
        # memo so this node's own changes (create, advance, finish, adopt)
        # render on the very next poll instead of aging out by TTL.
        self._summaries_memo.pop(dag_name, None)
        return result

    async def _read(self, dag_name: str, key: str) -> Optional[Dict[str, Any]]:
        backend = self._backend()
        if backend is None:
            return None
        return await asyncio.wait_for(
            backend.read_document(self._ns(dag_name), key),
            timeout=STATE_OP_TIMEOUT,
        )

    # =====================================================================
    # Periodic entry point (called each scheduling tick from cron)
    # =====================================================================

    def service(self) -> None:
        """Spawn a single-flight service pass if there is DAG work to do.

        Synchronous and cheap, like ``Cron._state_periodic``: it only decides
        whether to spawn the async pass (scheduling due, an owned run's wake
        due, or an adoption/GC interval elapsed), never blocks the loop.
        """
        if self._backend() is None or not self.has_dags():
            return
        if self._service_task is not None and not self._service_task.done():
            return
        now = _now()
        due = (
            now >= self._next_sched_check
            or now >= self._next_adopt
            or now >= self._next_gc
            # a ref whose advance is already in flight is not work this pass
            # could do: advance_one would only latch its rerun flag, making
            # the holder redo a pass nothing has changed under.
            or any(
                w <= now
                for ref, w in self._wake.items()
                if ref not in self._advance_pending
            )
            or any(
                pc["nextTryAt"] <= now
                for pc in self._pending_completions.values()
            )
            or any(w.timestamp() <= now for w in self._next_logical.values())
        )
        if not due:
            return
        self._service_task = self._cron._track_state_write(self._run_service())

    async def _run_service(self) -> None:
        try:
            now = _now()
            # (re)seed new/changed dags + run one-time catch-up on the coarse
            # cadence (the seed is the one expensive durable read).
            if now >= self._next_sched_check:
                self._next_sched_check = now + SCHEDULE_CHECK_INTERVAL
                await self._seed_dags(now)
            # fire due scheduled runs EVERY pass (a cheap in-memory index
            # walk), so a fire lands at its instant, not a cadence late.
            await self._fire_scheduled(now)
            if now >= self._next_adopt:
                self._next_adopt = now + ADOPT_SCAN_INTERVAL
                await self._adopt_orphans()
            await self._retry_completions(now)
            await self._advance_owned(now)
            if now >= self._next_gc:
                self._next_gc = now + GC_INTERVAL
                await self._gc_runs()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a bad pass must not kill the loop
            logger.exception("dag: unexpected error in the service pass")

    def next_wake_delay(self) -> Optional[float]:
        """Seconds until the scheduler next wants to run, or ``None``.

        Caps the main loop's sleep so a due sensor poke, task retry, or the
        next scheduled run is serviced on time even when no job is due.
        """
        if self._backend() is None or not self.has_dags():
            return None
        now = _now()
        # prune wake hints for runs this node does not own (a decision or a
        # completion recorded here for a peer-owned run): a stale 0.0 entry
        # would pin the loop's sleep at 0 forever.
        for ref in list(self._wake):
            if ref not in self._owned:
                del self._wake[ref]
        # prune per-run advance locks the same way: advance_one setdefaults
        # an entry for every ref ever advanced here -- including peer-owned
        # runs reached via an approval or a recorded completion -- and
        # nothing else removes them (only a backend swap clears the map), so
        # a long-lived daemon would hold one Lock per run it ever touched.
        # Owned refs stay (they are hot), and a lock is never dropped while
        # held or awaited: a waiter resumes holding the OLD object, so
        # dropping it would let the next arrival mint a fresh Lock and
        # advance the same run concurrently.  asyncio.Lock has no public
        # waiter count; if the private _waiters peek ever stops resolving,
        # getattr's None keeps this pruning (fail-open) rather than the leak.
        for ref in list(self._locks):
            lock = self._locks[ref]
            if (
                ref not in self._owned
                and not lock.locked()
                and not getattr(lock, "_waiters", None)
            ):
                del self._locks[ref]
        candidates = [self._next_sched_check]
        # A hint whose advance is in flight is stale by construction (the pass
        # rewrites it on the way out), so it is polled rather than honoured as
        # due -- see ADVANCE_POLL_FLOOR.
        poll_floor = now + ADVANCE_POLL_FLOOR
        for ref, hint in self._wake.items():
            if ref in self._advance_pending and hint < poll_floor:
                hint = poll_floor
            candidates.append(hint)
        for pc in self._pending_completions.values():
            candidates.append(pc["nextTryAt"])
        for when in self._next_logical.values():
            candidates.append(when.timestamp())
        if self._owned:
            candidates.append(self._next_adopt)
        soonest = min(candidates)
        return max(0.0, soonest - now)

    # =====================================================================
    # Scheduling: create due runs (forward firing + one-time catch-up)
    # =====================================================================

    @staticmethod
    def _sched_sig(dagcfg: Any) -> str:
        """A signature of a dag's schedule + resolved timezone.

        Two configs fire on the same instants iff this matches (mirrors the job
        ``_same_schedule``), so a reload that changes the schedule re-seeds and
        one that leaves it alone keeps the existing next-fire (never skipping a
        fire on the reload's own boundary).
        """
        sched = dagcfg.schedule_job
        return "{}|{}".format(sched.schedule, sched.timezone)

    async def _seed_dags(self, now: float) -> None:
        """Reconcile the next-fire index with the (reloaded) dag set.

        Drops the index for a removed or disabled dag (so a later re-enable
        seeds strictly-future rather than backfilling the disabled gap), and
        (re)seeds a new dag or one whose schedule changed, running its one-time
        missed-run catch-up.
        """
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        live = self._dags()
        for name in list(self._seeded):
            dagcfg = live.get(name)
            if (
                dagcfg is None
                or dagcfg.schedule_job is None
                or not dagcfg.enabled
                or self._seeded.get(name) != self._sched_sig(dagcfg)
            ):
                self._next_logical.pop(name, None)
                self._seeded.pop(name, None)
        for name in list(self._seed_failed):
            dagcfg = live.get(name)
            if (
                dagcfg is None
                or dagcfg.schedule_job is None
                or self._seed_failed.get(name) != self._sched_sig(dagcfg)
            ):
                self._seed_failed.pop(name, None)  # removed/changed: retry
        for name, dagcfg in live.items():
            sched = dagcfg.schedule_job
            if sched is None or not dagcfg.enabled:
                continue
            if not self._cron._cluster_allows(sched):
                continue
            if name in self._seeded or name in self._seed_failed:
                continue
            try:
                await self._seed_dag(dagcfg, now_dt)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - isolate the poisoned dag
                # one dag's bad seed must not starve every other dag's
                # fire/adopt/advance; logged once per schedule signature.
                self._seed_failed[name] = self._sched_sig(dagcfg)
                logger.exception(
                    "dag %s: seeding its schedule failed; it will not fire "
                    "until a reload changes its schedule",
                    name,
                )

    def _next_fire(
        self, sched: Any, after: datetime.datetime
    ) -> Optional[datetime.datetime]:
        """``Cron._compute_next_fire``, guarded for a non-crontab schedule.

        A schedule string the parser passes through verbatim (the documented
        "@reboot") has no computable occurrences; ``None`` (never fires) keeps
        the service pass alive instead of crashing it -- the isinstance assert
        inside ``_compute_next_fire`` is stripped in the -OO release binary,
        leaving a raw AttributeError.
        """
        if not isinstance(sched.schedule, CronTab):
            return None
        nxt: Optional[datetime.datetime] = self._cron._compute_next_fire(
            sched, after
        )
        return nxt

    async def _seed_dag(self, dagcfg: Any, now_dt: datetime.datetime) -> None:
        sched = dagcfg.schedule_job
        nxt = self._next_fire(sched, now_dt)
        if nxt is not None:
            self._next_logical[dagcfg.name] = nxt
        self._seeded[dagcfg.name] = self._sched_sig(dagcfg)
        if sched.onMissed != "skip":
            try:
                await self._catch_up(dagcfg, now_dt)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("dag %s: catch-up seed failed", dagcfg.name)

    async def _fire_scheduled(self, now: float) -> None:
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        for name, dagcfg in list(self._dags().items()):
            sched = dagcfg.schedule_job
            if sched is None or not dagcfg.enabled:
                continue
            if name not in self._seeded:
                continue  # not seeded yet (waits for the next seed cadence)
            if not self._cron._cluster_allows(sched):
                continue
            try:
                await self._fire_forward(dagcfg, now_dt)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - isolate per-dag failures
                logger.exception(
                    "dag %s: firing its scheduled runs failed", name
                )

    async def _fire_forward(
        self, dagcfg: Any, now_dt: datetime.datetime
    ) -> None:
        sched = dagcfg.schedule_job
        stale = self._next_logical.get(dagcfg.name)
        if stale is not None and now_dt - stale >= DAG_CATCHUP_LIMIT:
            # A stall/suspend/forward clock jump, not tick overhead: the
            # replay loop below would fire a backdated run per missed
            # occurrence (per-pass capped, but every pass immediately due
            # again until the whole gap is drained) regardless of onMissed.
            # Only the one-time boot seed honoured that policy; a mid-life
            # gap must too, or a laptop resume unleashes the exact stampede
            # DAG_MAX_CATCHUP's cap claims to prevent.
            await self._fire_past_gap(dagcfg, stale, now_dt)
            return
        fired = 0
        while fired < DAG_MAX_CATCHUP:
            nxt = self._next_logical.get(dagcfg.name)
            if nxt is None or nxt > now_dt:
                break
            await self._create_run(dagcfg, nxt, "scheduled")
            following = self._next_fire(sched, nxt)
            if following is None:
                # the schedule has no further occurrence (a fixed past year):
                # drop the index rather than poisoning it with None, which the
                # loop's ``.timestamp()`` sleep/due candidates would crash on
                # (mirrors the job next-fire index dropping an exhausted job).
                self._next_logical.pop(dagcfg.name, None)
                break
            self._next_logical[dagcfg.name] = following
            fired += 1

    async def _fire_past_gap(
        self,
        dagcfg: Any,
        stale: datetime.datetime,
        now_dt: datetime.datetime,
    ) -> None:
        """Resume a dag whose next-fire fell far behind (see Cron._advance).

        The same decision the job scheduler makes for a stall: fire the
        CURRENT slot only if now itself matches the schedule, resync to the
        first occurrence strictly after now, and hand the gap to the dag's
        ``onMissed`` policy: the boot catch-up, which already applies
        ``startingDeadlineSeconds``, the ``run-once`` coalesce and the
        ``DAG_MAX_CATCHUP`` bound, and whose create-if-absent dedup makes
        re-covering the current slot harmless. ``onMissed: skip`` (the
        default) thus skips a mid-life gap exactly as it skips a boot gap.
        """
        sched = dagcfg.schedule_job
        logger.warning(
            "dag %s: its next fire fell %.0fs behind (a slow pass, stall, "
            "suspend, or clock change); resuming at the current slot and "
            "leaving the gap to onMissed=%s instead of replaying it",
            dagcfg.name,
            (now_dt - stale).total_seconds(),
            sched.onMissed,
        )
        # Imported here: cron imports this module at load, so a top-level
        # import back into cron would be a cycle. schedule_slot renders now
        # into the schedule's own frame (timezone, second resolution);
        # sched is the dag's schedule carrier, a JobConfig, exactly what it
        # expects.
        from cronstable.cron import schedule_slot

        fired_now: Optional[datetime.datetime] = None
        if isinstance(sched.schedule, CronTab):
            now_slot = schedule_slot(sched, now_dt)
            if sched.schedule.test(now_slot):
                # record as aware-UTC like every other run key; astimezone
                # reads a naive slot as local, as schedule_slot produced it.
                fired_now = now_slot.astimezone(datetime.timezone.utc)
        # Create THEN resync, like the replay loop above: a create that
        # raises (into _fire_scheduled's per-dag isolation) leaves the stale
        # index in place, so the next pass re-enters this branch and retries;
        # at most one scheduled run per pass by construction, so the
        # retry cannot stampede, and create-if-absent dedups the slot if the
        # raise landed after the document was written.
        if fired_now is not None:
            await self._create_run(dagcfg, fired_now, "scheduled")
        following = self._next_fire(sched, now_dt)
        if following is None:
            # no further occurrence (a fixed past year): drop the index
            # rather than poisoning it with None (see the loop above).
            self._next_logical.pop(dagcfg.name, None)
        else:
            self._next_logical[dagcfg.name] = following
        if sched.onMissed != "skip":
            await self._catch_up(dagcfg, now_dt)

    async def _catch_up(self, dagcfg: Any, now_dt: datetime.datetime) -> None:
        sched = dagcfg.schedule_job
        after = await self._durable_watermark(dagcfg)
        # Hoist back to an open checkpoint's (older) watermark when a
        # previous deferred replay never closed (a restart mid-jitter, here
        # or on a crashed node): ordinary scheduled fires landing after that
        # cycle opened have advanced the document-derived watermark past the
        # still-unreplayed slots.  Mirrors Cron._missed_occurrences.
        pending = await self._pending_catchup_watermark(dagcfg.name)
        pending_dt = _parse_iso(pending) if pending else None
        if pending_dt is not None and (after is None or pending_dt < after):
            after = pending_dt
        if after is None:
            return  # never ran: nothing missed to replay
        # The cycle's reference watermark: pre-deadline-cutoff, like the job
        # engine's (the cutoff moves with the clock, so recomputation on
        # resume re-applies it against the resume-time now).
        watermark = after.isoformat()
        deadline = sched.startingDeadlineSeconds
        if deadline:
            cutoff = now_dt - datetime.timedelta(seconds=deadline)
            if cutoff > after:
                after = cutoff
        missed: List[datetime.datetime] = []
        nxt = self._next_fire(sched, after)
        while (
            nxt is not None and nxt <= now_dt and len(missed) < DAG_MAX_CATCHUP
        ):
            missed.append(nxt)
            nxt = self._next_fire(sched, nxt)
        if not missed:
            if pending is not None:
                # the open cycle was covered meanwhile (another node, or the
                # deadline expired the slots): close it so restarts stop
                # resuming it.
                await self._checkpoint_catchup(dagcfg.name, "close", watermark)
            return
        targets = missed[-1:] if sched.onMissed == "run-once" else missed
        if (
            sched.onMissed != "run-once"
            and len(missed) >= DAG_MAX_CATCHUP
            and nxt is not None
            and nxt <= now_dt
        ):
            # the job engine's cap (cron._missed_occurrences) warns when it
            # drops occurrences; truncating a dag's replay must be exactly
            # as loud, never silent.
            logger.warning(
                "dag catch-up: %s missed at least %d runs; replaying %d "
                "and dropping the rest (set startingDeadlineSeconds to "
                "bound the window, or use onMissed: run-once)",
                dagcfg.name,
                DAG_MAX_CATCHUP,
                DAG_MAX_CATCHUP,
            )
        # The same deterministic per-name spread the job engine applies
        # (Cron._catchup_offset, stable across boots and the fleet):
        # ``catchupJitterSeconds`` is accepted and validated on dag
        # schedules, so it must spread their replays too, not silently
        # apply to plain jobs only.
        offset = self._cron._catchup_offset(
            dagcfg.name, sched.catchupJitterSeconds
        )
        logger.info(
            "dag %s: catch-up replaying %d missed run(s)%s",
            dagcfg.name,
            len(targets),
            " after a %.1fs jitter offset" % offset if offset > 0 else "",
        )
        if offset <= 0:
            # Inline, on the service pass: no checkpoint needed for a fresh
            # cycle (creates ascend, so a crash mid-loop leaves the document
            # watermark at the last created target and the next boot's
            # recompute covers the remainder), but a RESUMED cycle must
            # close its checkpoint once the targets have all landed.
            for when in targets:
                await self._create_run(dagcfg, when, "catchup")
            if pending is not None:
                await self._checkpoint_catchup(dagcfg.name, "close", watermark)
            return
        # Deferred on a spawned task, like Cron._run_catch_up: the seed and
        # gap-resume paths run on the service pass, which walks every dag
        # serially and must not sleep out one dag's offset inline.
        # Checkpoint the intent BEFORE spawning the sleeper (the job twin's
        # rule, cron._evaluate_catch_up): a crash/restart mid-jitter then
        # resumes from `watermark` instead of losing the owed slots to the
        # advancing run-document ledger.
        await self._checkpoint_catchup(dagcfg.name, "open", watermark)
        task = asyncio.create_task(
            self._replay_catch_up(dagcfg, targets, offset, watermark)
        )
        self._catchup_tasks.add(task)
        task.add_done_callback(self._catchup_tasks.discard)

    async def _replay_catch_up(
        self,
        dagcfg: Any,
        targets: List[datetime.datetime],
        offset: float,
        watermark: str,
    ) -> None:
        """Create ``dagcfg``'s catch-up runs after its jitter offset.

        The dag twin of ``Cron._run_catch_up``'s deferred start.  The dag is
        re-read after the sleep and REVALIDATED like the job twin: a reload
        during the offset can remove or disable the dag, drop its schedule,
        or flip ``onMissed`` to ``skip`` (the operator saying "do not
        backfill"), and cluster ownership can move, in which case the new
        owner resumes from the open checkpoint and creating here too would
        double-run the backfill.  The run documents are materialised from
        the re-read object, not the one captured when the offset was
        scheduled: a reload can equally well rewrite the task graph, and
        creating from the captured config would seed runs against a spec the
        daemon no longer has.  The missed INSTANTS still come from the
        original computation, which is correct: the schedule they were
        derived from is what was actually missed.  The cycle's checkpoint is
        closed only after the last ``_create_run`` lands; every early return
        leaves it open on purpose, so a restart (or the new owner, or a
        re-enable) resumes the still-owed slots instead of losing them.
        (A backend swap cancels this task outright, see :meth:`forget`.)
        """
        try:
            await asyncio.sleep(offset)
            current = self._dags().get(dagcfg.name)
            sched = current.schedule_job if current is not None else None
            if (
                current is None
                or not current.enabled
                or sched is None
                or sched.onMissed == "skip"
                or not isinstance(sched.schedule, CronTab)
            ):
                logger.info(
                    "dag %s: removed or disabled during its catch-up jitter "
                    "window; dropping the backfill",
                    dagcfg.name,
                )
                return
            if not self._cron._cluster_allows(sched):
                logger.info(
                    "dag %s: ownership moved during its catch-up jitter "
                    "window; leaving the backfill to the new owner (which "
                    "resumes from the open checkpoint)",
                    dagcfg.name,
                )
                return
            for when in targets:
                await self._create_run(current, when, "catchup")
            await self._checkpoint_catchup(dagcfg.name, "close", watermark)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - per-dag isolation, like the seed
            logger.exception(
                "dag %s: deferred catch-up replay failed", dagcfg.name
            )

    async def _durable_watermark(
        self, dagcfg: Any
    ) -> Optional[datetime.datetime]:
        backend = self._backend()
        if backend is None:
            return None
        docs = await asyncio.wait_for(
            backend.list_documents(self._ns(dagcfg.name)),
            timeout=STATE_OP_TIMEOUT,
        )
        latest: Optional[datetime.datetime] = None
        for body in docs:
            iso = body.get("logicalDate")
            when = _parse_iso(iso) if isinstance(iso, str) else None
            if when is not None and (latest is None or when > latest):
                latest = when
        return latest

    @staticmethod
    def _catchup_stream(dag_name: str) -> str:
        """The durable checkpoint stream for a dag's catch-up cycles."""
        return DAG_CATCHUP_STREAM_PREFIX + dag_name

    async def _pending_catchup_watermark(self, dag_name: str) -> Optional[str]:
        """The watermark of an unfinished backfill cycle, if one is open.

        The dag twin of ``Cron._pending_catchup_watermark``: an ``open``
        without a following ``close`` means a previous deferred replay (here
        or on a crashed node) never completed, and catch-up should resume
        from ITS watermark rather than the run documents': scheduled fires
        landing after that boot advanced the derived watermark past the
        still-unreplayed slots.
        """
        backend = self._backend()
        if backend is None:
            return None
        recs = await asyncio.wait_for(
            backend.list_records(
                self._catchup_stream(dag_name), limit=1, newest_first=True
            ),
            timeout=STATE_OP_TIMEOUT,
        )
        if recs and recs[0].get("kind") == "open":
            watermark = recs[0].get("watermark")
            if isinstance(watermark, str) and watermark:
                return watermark
        return None

    async def _checkpoint_catchup(
        self, dag_name: str, kind: str, watermark: str
    ) -> None:
        """Append an ``open``/``close`` catch-up checkpoint (best-effort).

        The dag twin of ``Cron._checkpoint_catchup``, with the same
        contract: a failure to checkpoint must never block the backfill
        itself (it only costs crash-resume fidelity, which is logged), and
        the stream is at-least-once by design; a resumed cycle merely
        re-creates run documents the create-if-absent dedup already has.
        """
        backend = self._backend()
        if backend is None:
            return
        record = {
            "kind": kind,
            "watermark": watermark,
            "at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        try:
            await asyncio.wait_for(
                backend.append_record(
                    self._catchup_stream(dag_name),
                    record,
                    prune_keep=DAG_CATCHUP_STREAM_KEEP,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except Exception as ex:  # noqa: BLE001 - checkpoint is best-effort
            logger.warning(
                "dag %s: could not checkpoint catch-up %r (%s); a restart "
                "mid-backfill may not resume it",
                dag_name,
                kind,
                ex,
            )

    async def _create_run(
        self, dagcfg: Any, logical_dt: datetime.datetime, kind: str
    ) -> Optional[RunRef]:
        # Canonicalise the instant to UTC before it becomes the run key. The
        # scheduled/catch-up paths already hand in UTC-aware instants, but
        # backfill preserves whatever offset the operator's ISO range carried,
        # so 14:00Z and 09:00-05:00 (the SAME instant) would otherwise derive
        # different keys and defeat the create-if-absent dedup -- re-running
        # every task for an instant that already executed. A naive instant is
        # read as UTC (matching the scheduled path), never shifted by local
        # time.
        if logical_dt.tzinfo is None:
            logical_dt = logical_dt.replace(tzinfo=datetime.timezone.utc)
        else:
            logical_dt = logical_dt.astimezone(datetime.timezone.utc)
        run_key = dag.run_key_for_logical(logical_dt.isoformat())
        created = await self._create_doc(
            dagcfg, run_key, logical_dt.isoformat(), kind
        )
        ref = (dagcfg.name, run_key)
        if created:
            await self._try_own(dagcfg, ref)
        return ref

    async def _create_doc(
        self, dagcfg: Any, run_key: str, logical_iso: Optional[str], kind: str
    ) -> bool:
        run_id = os.urandom(16).hex()
        now = _now()
        spec = dagcfg.spec

        def _create(current):
            if current is not None:
                return DOC_KEEP, False
            body = dag.new_run_body(
                dag=dagcfg.name,
                run_key=run_key,
                run_id=run_id,
                logical_date=logical_iso,
                kind=kind,
                now=now,
                spec=spec,
            )
            return body, True

        _stored, created = await self._mutate(dagcfg.name, run_key, _create)
        if created:
            # a fresh run now lives under this key: it must not inherit a
            # stale "known terminal" marking from a GC'd predecessor (an
            # operator backfill legitimately re-creates a logical date's key).
            known = self._terminal_run_keys.get(dagcfg.name)
            if known is not None:
                known.discard(run_key)
            # same reason for list_dags' rollup cache: a re-created key must
            # not keep serving the GC'd predecessor's terminal summary.
            summaries = self._dag_summary_cache.get(dagcfg.name)
            if summaries is not None:
                summaries.pop(run_key, None)
        return bool(created)

    # =====================================================================
    # Ownership: the per-run advance lease (the TTL lease trio, per run)
    # =====================================================================

    async def _try_own(self, dagcfg: Any, ref: RunRef) -> bool:
        """Take ``ref``'s advance lease; on success reconcile + advance it.

        A ``None`` from ``acquire_lease`` (held elsewhere, or the store could
        not answer) means "not mine" -- fail closed and do not advance, exactly
        like the cluster slot claim.
        """
        if ref in self._owned:
            return True
        backend = self._backend()
        if backend is None:
            return False
        holder = self._cron._slot_holder()
        try:
            lease = await asyncio.wait_for(
                backend.acquire_lease(
                    self._lease_name(ref), holder, DAG_LEASE_TTL
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return False
        if lease is None:
            return False
        self._owned[ref] = lease
        self._locks.setdefault(ref, asyncio.Lock())
        self._renewers[ref] = asyncio.ensure_future(self._renew_loop(ref))
        self._wake[ref] = _now()  # advance promptly
        self._hold_advancing(ref)  # ...but the loop must not spin meanwhile
        try:
            await self._reconcile_run(dagcfg, ref)
            await self.advance_one(ref)
        finally:
            self._drop_advancing(ref)
        return True

    async def _renew_loop(self, ref: RunRef) -> None:
        period = max(1.0, DAG_LEASE_TTL / 3)
        while True:
            await asyncio.sleep(period)
            lease = self._owned.get(ref)
            backend = self._backend()
            if lease is None or backend is None:
                return
            try:
                renewed = await asyncio.wait_for(
                    backend.renew_lease(lease, DAG_LEASE_TTL),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                continue  # unknown: retry next period
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - renewal is best-effort
                continue
            if renewed is not None:
                self._owned[ref] = renewed
                continue
            # positively taken over: a peer adopted the run (our lease lapsed).
            # Stop advancing it; in-flight tasks keep running and their
            # completions RMW the doc harmlessly (the new owner's state wins).
            logger.warning(
                "dag run %s/%s: advance lease was taken over; stopping "
                "advancement here (at-least-once)",
                ref[0],
                ref[1],
            )
            self._drop_owned(ref)
            return

    def _drop_owned(self, ref: RunRef) -> None:
        self._owned.pop(ref, None)
        self._wake.pop(ref, None)
        self._advance_again.discard(ref)
        renewer = self._renewers.pop(ref, None)
        if renewer is not None and not renewer.done():
            renewer.cancel()

    async def _release(self, ref: RunRef) -> None:
        lease = self._owned.get(ref)
        self._drop_owned(ref)
        backend = self._backend()
        if lease is not None and backend is not None:
            try:
                await asyncio.wait_for(
                    backend.release_lease(lease), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the TTL frees it regardless
                pass

    async def _adopt_orphans(self) -> None:
        """Adopt active runs whose owner is gone (its lease lapsed)."""
        backend = self._backend()
        if backend is None:
            return
        now = _now()
        full = now >= self._next_full_adopt
        if full:
            self._next_full_adopt = now + ADOPT_FULL_REFRESH
        for name, dagcfg in list(self._dags().items()):
            try:
                await self._adopt_one_dag(backend, name, dagcfg, full=full)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - isolate per-dag failures
                logger.exception("dag %s: orphan adoption failed", name)

    async def _adopt_one_dag(
        self, backend: StateBackend, name: str, dagcfg: Any, *, full: bool
    ) -> None:
        if not full:
            try:
                keys = await asyncio.wait_for(
                    backend.list_document_keys(self._ns(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                return
            if keys is not None:
                # Keys-only pass: one directory listing, plus a body read for
                # only the runs not already owned here or known terminal;
                # the steady state re-reads nothing.  A key that vanished
                # from the listing (GC'd, here or on a peer) is dropped from
                # the cache by the intersection below.
                known = self._terminal_run_keys.setdefault(name, set())
                known.intersection_update(keys)
                for key in keys:
                    if key in known or (name, key) in self._owned:
                        continue
                    try:
                        body = await asyncio.wait_for(
                            backend.read_document(self._ns(name), key),
                            timeout=STATE_OP_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        return
                    if body is None:
                        continue  # deleted (or unreadable) since the listing
                    if dag.is_terminal_run(body):
                        known.add(key)
                        continue
                    if not isinstance(body.get("runKey"), str):
                        continue
                    await self._try_own(dagcfg, (name, key))
                return
        try:
            docs = await asyncio.wait_for(
                backend.list_documents(self._ns(name)),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return
        terminal: Set[str] = set()
        for body in docs:
            run_key = body.get("runKey")
            if dag.is_terminal_run(body):
                if isinstance(run_key, str):
                    terminal.add(run_key)
                continue
            if not isinstance(run_key, str):
                continue
            ref = (name, run_key)
            if ref in self._owned:
                continue
            await self._try_own(dagcfg, ref)
        # a full pass parsed every body: rebuild the cache from truth (also
        # the self-heal for the stale-terminal corner ADOPT_FULL_REFRESH
        # documents).
        self._terminal_run_keys[name] = terminal

    # =====================================================================
    # Advancing an owned run
    # =====================================================================

    def _hold_advancing(self, ref: RunRef) -> None:
        """Take ``ref``'s wake hint out of the loop's sleep index."""
        self._advance_pending[ref] = self._advance_pending.get(ref, 0) + 1

    def _drop_advancing(self, ref: RunRef) -> None:
        """Release one :meth:`_hold_advancing` hold on ``ref``."""
        left = self._advance_pending.get(ref, 0) - 1
        if left > 0:
            self._advance_pending[ref] = left
        else:
            self._advance_pending.pop(ref, None)

    async def _advance_owned(self, now: float) -> None:
        due = [ref for ref in self._owned if self._wake.get(ref, 0.0) <= now]
        if not due:
            return
        # Hold the WHOLE due batch, not one ref at a time: the walk is
        # sequential and every advance awaits the store, so each ref this pass
        # has not reached yet still carries a due wake and would keep
        # next_wake_delay pinned at 0 for the pass's whole duration.
        for ref in due:
            self._hold_advancing(ref)
        try:
            for ref in due:
                await self.advance_one(ref)
        finally:
            for ref in due:
                self._drop_advancing(ref)

    async def advance_one(self, ref: RunRef) -> None:
        """Advance ``ref`` once, coalescing concurrent requests.

        Completions arrive in bursts (a mapped fan-in can finish many
        instances near-simultaneously) and each spawns an advance.
        Queueing them all behind the per-ref lock would still run one
        full reconcile+claim pass per completion against the same
        document, most finding nothing left to claim.  Instead, a call
        that finds an advance already in flight latches a rerun flag and
        returns; the in-flight holder loops one more time when the flag
        was set while it worked.  A burst of any size therefore costs at
        most the pass already running plus one fresh pass that observes
        everything the burst recorded.
        """
        lock = self._locks.setdefault(ref, asyncio.Lock())
        if lock.locked():
            # No await between this check and the holder's own re-check
            # under the lock, so the flag cannot be missed.
            self._advance_again.add(ref)
            return
        self._hold_advancing(ref)
        try:
            async with lock:
                while True:
                    self._advance_again.discard(ref)
                    await self._advance_locked(ref)
                    if ref not in self._advance_again:
                        return
        finally:
            self._drop_advancing(ref)

    def _spawn_advance(self, ref: RunRef) -> None:
        """Advance ``ref`` soon, off the caller's critical path.

        Used by the completion and approval paths, which must not block the
        reaper (or an HTTP handler) on the per-run lock a periodic advance may
        be holding.  The hint left behind is one poll out, not the "due now"
        0.0 this used to write: the advance is already spawned, so the hint is
        only the backstop for the window before that task starts (and for the
        case where it never does -- ``_track_state_write`` sheds writes under
        overload), whereas a due-now hint pinned :meth:`next_wake_delay` at 0
        and spun the whole main loop until the advance rewrote it.
        """
        self._wake[ref] = _now() + ADVANCE_POLL_FLOOR
        self._cron._track_state_write(self.advance_one(ref))

    async def _advance_locked(self, ref: RunRef) -> None:
        lease = self._owned.get(ref)
        if lease is None:
            # not ours to advance (e.g. a decision/completion recorded
            # here for a run a peer owns): drop the wake hint too, or
            # next_wake_delay() would return 0.0 forever and busy-spin
            # the main loop.  The durable record itself is safe -- the
            # owner picks it up via its own poll/advance wakes.
            self._wake.pop(ref, None)
            return
        dagcfg = self._dags().get(ref[0])
        if dagcfg is None:
            await self._release(ref)  # dag removed on reload
            return
        if not await self._lease_usable(ref, lease):
            if ref in self._owned:
                # unverifiable (store unreachable) or expired-but-untaken:
                # skip this advance and re-check shortly; the renew loop
                # re-establishes a live TTL or learns of the takeover.
                self._wake[ref] = _now() + ADVANCE_RETRY_DELAY
            return
        try:
            await self._do_advance(dagcfg, ref)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - never kill the loop
            logger.exception("dag run %s/%s: advance failed", ref[0], ref[1])
            if ref in self._owned:
                # a due wake left in place would retry instantly every
                # loop pass against a fast-failing store; back off a bit.
                self._wake[ref] = _now() + ADVANCE_RETRY_DELAY

    async def _lease_usable(self, ref: RunRef, lease: Lease) -> bool:
        """Whether our advance lease still plausibly gates ``ref``.

        ``ref in _owned`` alone is not enough: while the store is unreachable
        the renew loop cannot positively learn of a takeover (renew raises
        instead of returning ``None``), so a lapsed lease lingers in
        ``_owned`` -- and advancing on it would reconcile-fail the new owner's
        live tasks.  A lease past its ``expires_at`` is verified against the
        store's fence (the field exists exactly for stale-holder detection):
        positively superseded -> drop ownership; expired-but-untaken or
        unverifiable -> fail closed and skip the advance (renewing an expired
        lease nobody took over is still allowed, so the renew loop recovers).
        """
        if _now() < lease.expires_at:
            return True
        backend = self._backend()
        if backend is None:
            return False
        try:
            observed = await asyncio.wait_for(
                backend.read_lease(self._lease_name(ref)),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - unverifiable: fail closed
            return False
        if observed is not None and (
            observed.holder != lease.holder or observed.fence != lease.fence
        ):
            logger.warning(
                "dag run %s/%s: advance lease lapsed and was taken over; "
                "dropping ownership here (at-least-once)",
                ref[0],
                ref[1],
            )
            self._drop_owned(ref)
            return False
        return False

    async def _do_advance(self, dagcfg: Any, ref: RunRef) -> None:
        spec = dagcfg.spec
        now = _now()
        proc = self._cron._proc_token
        host = self._cron._state_host
        # 1. reconcile AND claim in one RMW (dag.reconcile_and_plan): fail
        # any task a crash left running with a dead process (protects our
        # own live tasks by proc token), then, unless mapped tasks are
        # awaiting expansion, continue straight into propagate/claim/
        # terminalise on the same body.  In the common case this is the
        # advance's ONLY document RMW (and on a quiescent run it keeps the
        # document without a rewrite); the old shape paid a reconcile RMW
        # plus a claim RMW on every owned run's wake and after every task
        # completion.
        transform = self._wrap(
            dag.reconcile_and_plan(spec, now, proc, host, platform.pid_alive)
        )
        body, combined = await self._mutate(ref[0], ref[1], transform)
        if body is None:
            # the run document does not exist (or no backend answered):
            # nothing to advance, exactly like the old reconcile step
            # observing no document.
            await self._release(ref)
            return
        if combined.reconciled:
            logger.info(
                "dag run %s/%s: reconciled %d interrupted task(s)",
                ref[0],
                ref[1],
                combined.reconciled,
            )
        if dag.is_terminal_run(body):
            await self._on_terminal(ref, body)
            return
        run_id = str(body.get("runId"))
        result = combined.advance
        if combined.expansions_needed:
            # 2. mapped tasks await their upstream lists: pre-read them from
            # the reconciled body (outside any document lock, exactly as
            # before), then run the classic claim RMW as the second step.
            expansions = await self._read_expansions(dagcfg, run_id, body)
            now = _now()
            transform = self._wrap(
                dag.plan_and_claim(spec, now, proc, host, expansions)
            )
            claimed, result = await self._mutate(ref[0], ref[1], transform)
            if result is None:
                return
            if claimed is not None:
                body = claimed
        # 3. launch each claimed task (subprocess), collecting the launched
        # pids to stamp in one batched RMW below; a launch that fails is
        # failed explicitly (exit 127) per task.  Each launch is independent:
        # one failing must not skip the rest of the batch (which would
        # strand them claimed-but-unlaunched).
        pid_stamps: List[Tuple[str, str, Optional[int], Optional[int]]] = []
        for intent in result.launches:
            try:
                stamp = await self._launch_task(dagcfg, ref, run_id, intent)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - fail just this task, keep going
                logger.exception(
                    "dag run %s/%s: launching task %s failed",
                    ref[0],
                    ref[1],
                    intent.taskkey,
                )
                await self._finish_task(
                    dagcfg,
                    ref,
                    intent.taskkey,
                    intent.task_id,
                    success=False,
                    exit_code=127,
                    # the one vocabulary for a task that never started, shared
                    # with _launch_task's own cleanup path below
                    fail_reason="launch failed",
                    proc=proc,
                    attempt=intent.attempt,
                    poke=intent.poke_number if intent.is_sensor else None,
                )
            else:
                if stamp is not None:
                    pid_stamps.append(stamp)
        if pid_stamps:
            # 4. ONE RMW stamps every launched pid.  Per-launch stamping
            # cost a full document parse+rewrite+fsync per subprocess, so a
            # mapped fan-out paid up to MAX_CLAIMS_PER_PASS full rewrites of
            # a document holding up to MAX_MAPPED_ITEMS entries per pass.
            # Best-effort like the old per-task write: each task already
            # owns its slot (proc was set at claim, so reconciliation
            # protects it even without a pid), and the reaper will record
            # its completion; a failed batch write must not fail
            # already-running tasks.
            try:
                await self._set_pids(ref, pid_stamps)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the pids are an optimisation
                logger.warning(
                    "dag run %s/%s: could not record pids for %d launched "
                    "task(s)",
                    ref[0],
                    ref[1],
                    len(pid_stamps),
                )
        # 5. terminal? release the lease; else schedule the next wake.
        if dag.is_terminal_run(body):
            await self._on_terminal(ref, body)
        else:
            # a non-terminal run may have just parked an approval gate; alert
            # on it once (deduped) before scheduling the next wake.
            self._notify_pending_approvals(dagcfg, ref, run_id, body)
            if result.deferred:
                # the claim quota capped this pass (dag.MAX_CLAIMS_PER_PASS):
                # more instances are claimable now, so re-service promptly.
                self._wake[ref] = now
            else:
                self._wake[ref] = self._compute_wake(spec, body, now)

    async def _read_expansions(
        self, dagcfg: Any, run_id: str, body: Dict[str, Any]
    ) -> Dict[str, Optional[List[Any]]]:
        expansions: Dict[str, Optional[List[Any]]] = {}
        for tid, from_task, key in dag.tasks_awaiting_expansion(
            dagcfg.spec, body
        ):
            expansions[tid] = await self._read_xcom_list(
                run_id, dagcfg.name, from_task, key
            )
        return expansions

    async def _read_xcom_list(
        self, run_id: str, dag_name: str, taskkey: str, key: str
    ) -> Optional[List[Any]]:
        """The JSON list an upstream published, for a mapped task to fan out.

        Only ever read after the upstream has *succeeded*, so its output is
        final: a genuine list expands to itself; a **definitively** absent,
        non-list or unrecoverable output (including a swept blob) expands to
        the **empty list** (a mapped task with no items -> success), so a
        mis-publishing upstream cannot wedge the run forever.

        A store failure that says nothing about what the upstream published --
        a timeout, an I/O error, a record this build cannot read -- returns
        ``None`` instead, leaving the task unexpanded to retry on a later pass.
        The distinction is the whole point: the expansion is recorded once and
        never recomputed, so guessing "empty" on a bad instant would silently
        skip the task's entire fan-out and still report success downstream.
        Hence the strict read below -- best-effort, an unreadable record is
        skipped, and absence becomes indistinguishable from a blip.
        """
        backend = self._backend()
        if backend is None:
            return None
        from cronstable import jobstate

        scope = dag.xcom_scope(dag_name, run_id)
        name = dag.xcom_name(taskkey, key)
        try:
            # strict: an unreadable record must NOT read back as "never
            # published". The expansion below is recorded once and never
            # recomputed, so a best-effort read that swallowed an ESTALE/EIO
            # blip would turn one bad instant into a permanent, vacuously
            # successful empty fan-out -- the task's whole work silently
            # skipped, with downstream tasks seeing success. Strict turns that
            # blip back into the exception this returns None for.
            got = await asyncio.wait_for(
                jobstate.artifact_get(
                    backend,
                    scope,
                    name,
                    strict=True,
                    max_bytes=dag.MAX_MAPPED_XCOM_BYTES,
                ),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None  # transient: retry next pass
        except jobstate.JobStateError as ex:
            if getattr(ex, "status", None) == 413:
                # the blob is larger than a fan-out can safely materialise
                # (an upstream with maxArtifactBytes: 0): refuse it BEFORE it
                # is loaded rather than OOM the daemon, and map to empty like
                # the other definitively-unusable-output cases below.  The
                # blob was never fetched.
                logger.warning(
                    "dag %s: xcom %r from %r is too large to fan out (%s); "
                    "mapping to an empty fan-out",
                    dag_name,
                    key,
                    taskkey,
                    ex,
                )
                return []
            # the record survives but its blob is gone (410): definitively
            # unrecoverable, so map to empty rather than retry forever.  The
            # orphan-blob sweep never deletes a blob a surviving record
            # references, so this arises only from external interference
            # (a partial restore, manual deletion).
            logger.warning(
                "dag %s: xcom %r from %r has a missing blob; mapping to an "
                "empty fan-out",
                dag_name,
                key,
                taskkey,
            )
            return []
        except Exception as ex:  # noqa: BLE001 - the store, not the xcom
            # A store that could not be read (an I/O error, a record only a
            # newer node understands) leaves the fan-out UNKNOWN, never empty:
            # stay unexpanded and retry on a later pass.
            logger.warning(
                "dag %s: xcom %r from %r could not be read (%s); leaving the "
                "task unexpanded to retry",
                dag_name,
                key,
                taskkey,
                ex,
            )
            return None
        if got is None:
            # upstream succeeded without publishing this key: no items to map.
            return []
        _record, data = got
        try:
            # parse + portability walk together (see _parse_portable_xcom);
            # a large payload takes them both to a worker thread, because a
            # multi-MB inline parse stalls dispatch, every web handler and
            # the cluster heartbeats for its whole duration.
            if len(data) >= _XCOM_PARSE_OFFLOAD_MIN:
                parsed = await asyncio.to_thread(_parse_portable_xcom, data)
            else:
                parsed = _parse_portable_xcom(data)
        except _json.UnsupportedValue as exc:
            # a value that parses but is not fleet-portable (an int outside
            # the 64-bit window, a non-finite float): embedding it in the
            # run document would make _json.dumps_bytes raise on EVERY
            # advance, wedging the run forever. Treat a mis-published
            # upstream like the not-a-list case -- warn and map to empty.
            # (Ordered before ValueError: UnsupportedValue subclasses it.)
            logger.warning(
                "dag %s: xcom %r from %r contains a non-portable value "
                "(%s); mapping it to an empty fan-out",
                dag_name,
                key,
                taskkey,
                exc,
            )
            return []
        except ValueError:
            # undecodable bytes land here too: the bytes-path loads folds
            # what used to be the decode()'s UnicodeDecodeError into its
            # own ValueError.
            logger.warning(
                "dag %s: xcom %r from %r is not valid JSON; mapping it to an "
                "empty fan-out",
                dag_name,
                key,
                taskkey,
            )
            return []
        if isinstance(parsed, list):
            return parsed
        logger.warning(
            "dag %s: xcom %r from %r is a %s, not a list; mapping it to an "
            "empty fan-out",
            dag_name,
            key,
            taskkey,
            type(parsed).__name__,
        )
        return []

    def _compute_wake(
        self, spec: DagSpec, body: Dict[str, Any], now: float
    ) -> float:
        """The soonest instant this run wants advancing again.

        The nearest due sensor poke or task retry; a short poll while a gate is
        awaiting a decision (so an approval made on a *different* node -- which
        cannot advance a run it does not own -- is picked up by the owner in a
        few seconds, not a full idle cycle); a longer floor otherwise (each
        task completion on the owning node also forces an advance).
        """
        soonest = now + 60.0
        for entry in body.get("tasks", {}).values():
            state = entry.get("state")
            if state == dag.RUNNING and entry.get("awaitingApproval"):
                soonest = min(soonest, now + APPROVAL_POLL_INTERVAL)
            elif state == dag.RUNNING and entry.get("nextPokeAt") is not None:
                # only an IDLE sensor's due instant is a wake candidate: with
                # a poke in flight (proc/pid set) a stale past nextPokeAt --
                # written before claims cleared it -- would pin the loop's
                # sleep at 0 for the poke's whole duration.  The completion
                # itself forces the next advance.
                if entry.get("proc") is None and entry.get("pid") is None:
                    soonest = min(soonest, float(entry["nextPokeAt"]))
            elif state == dag.UP_FOR_RETRY and entry.get("nextRetryAt"):
                soonest = min(soonest, float(entry["nextRetryAt"]))
        return soonest

    async def _on_terminal(self, ref: RunRef, body: Dict[str, Any]) -> None:
        logger.info("dag run %s/%s reached a terminal state", ref[0], ref[1])
        # terminality is monotonic: remember it so the adopt scan never
        # re-reads this run's document just to rediscover it finished.
        seen = self._terminal_run_keys.setdefault(ref[0], set())
        first_time = ref[1] not in seen
        seen.add(ref[1])
        # a terminal run has no waiting gates: drop its approval-dedup entries
        # so a long-lived daemon does not accumulate them run after run.
        self._approval_notified = {
            k for k in self._approval_notified if (k[0], k[1]) != ref
        }
        # fire the notify `dag_failure` event once, only on the transition this
        # node observed (first_time), never on a re-service or a re-adopt of an
        # already-known-terminal run.
        if first_time and body.get("state") == dag.FAILED:
            self._notify_dag_failure(ref, body)
        await self._release(ref)

    def _notify_dag_failure(self, ref: RunRef, body: Dict[str, Any]) -> None:
        """Fire the notify ``dag_failure`` event for a FAILED run."""
        dag_name, run_key = ref
        failed = sorted(
            tk
            for tk, entry in body.get("tasks", {}).items()
            if entry.get("state") in (dag.FAILED, dag.UPSTREAM_FAILED)
        )
        detail = ", ".join(failed) if failed else "(no task detail)"
        self._cron._dispatch_notify(
            "dag_failure",
            success=False,
            name=dag_name,
            subject="DAG {!r} run {} failed".format(dag_name, run_key),
            message="{} task(s) failed: {}".format(len(failed), detail),
            dag=dag_name,
            run_key=run_key,
            run_id=str(body.get("runId")),
            failed_tasks=failed,
        )

    def _notify_pending_approvals(
        self, dagcfg: Any, ref: RunRef, run_id: str, body: Dict[str, Any]
    ) -> None:
        """Fire ``approval_waiting`` once per gate that has begun waiting.

        ``_do_advance`` re-reads the run body every pass, so a parked gate is
        observed repeatedly; :attr:`_approval_notified` dedups to one alert per
        gate.  Best-effort and synchronous (the dispatch itself is
        fire-and-forget), so it never delays the advance.
        """
        for taskkey, entry in body.get("tasks", {}).items():
            if entry.get("state") != dag.RUNNING or not entry.get(
                "awaitingApproval"
            ):
                continue
            key = (ref[0], ref[1], taskkey)
            if key in self._approval_notified:
                continue
            self._approval_notified.add(key)
            self._cron._dispatch_notify(
                "approval_waiting",
                success=False,
                name=dagcfg.name,
                subject="DAG {!r} run {} awaiting approval: {}".format(
                    dagcfg.name, ref[1], taskkey
                ),
                message=(
                    "Task {} is an approval gate awaiting a decision; "
                    "approve or reject it to let the run continue.".format(
                        taskkey
                    )
                ),
                dag=dagcfg.name,
                run_key=ref[1],
                run_id=run_id,
                taskkey=taskkey,
            )

    # =====================================================================
    # Launching a task instance (reuses the RunningJob/job-API machinery)
    # =====================================================================

    async def _launch_task(
        self, dagcfg: Any, ref: RunRef, run_id: str, intent
    ) -> Optional[Tuple[str, str, Optional[int], Optional[int]]]:
        template = dagcfg.task_templates[intent.task_id]
        taskkey = intent.taskkey
        token, env = await self._prepare_task_run(
            dagcfg, run_id, ref[1], intent, template
        )
        dref = _DagRef(
            dag_name=dagcfg.name,
            run_key=ref[1],
            run_id=run_id,
            task_id=intent.task_id,
            taskkey=taskkey,
            proc=self._cron._proc_token,
            attempt=intent.attempt,
            poke=intent.poke_number if intent.is_sensor else None,
        )
        running = RunningJob(
            template,
            None,
            extra_env=env,
            state_token=token,
            run_id=env.get(dag.ENV_DAG_RUN_ID),
            dag_ref=dref,
        )
        try:
            # a mapped fan-out launches up to MAX_CLAIMS_PER_PASS instances
            # back to back: share the daemon-wide spawn gate so the burst's
            # synchronous fork/exec work interleaves with other loop work
            # (see cron._SPAWN_BURST_LIMIT).
            async with self._cron._spawn_gate:
                await running.start()
        except BaseException:  # noqa: BLE001 - mirror maybe_launch_job cleanup
            if token is not None and self._cron._job_api is not None:
                await self._cron._job_api.finish_run(token)
            await self._finish_task(
                dagcfg,
                ref,
                taskkey,
                intent.task_id,
                success=False,
                exit_code=127,
                fail_reason="launch failed",
                proc=dref.proc,
                attempt=dref.attempt,
                poke=dref.poke,
            )
            return None
        self._cron.running_jobs[template.name].append(running)
        self._cron._jobs_running.set()
        pid = running.proc.pid if running.proc is not None else None
        # the pid is NOT stamped here: the caller collects every launched
        # (taskkey, proc, pid, attempt) and stamps the whole batch in one
        # RMW after the launch loop (see _do_advance), instead of one full
        # document rewrite per subprocess.  Deferring it is safe because the
        # pid is only an optimisation: the task already owns its slot (proc
        # was set at claim, so reconciliation protects it even without a
        # pid), and the reaper will record its completion.
        return (taskkey, dref.proc, pid, dref.attempt)

    @staticmethod
    def _stage_task_secrets(dagcfg: Any, intent, template) -> Dict[str, str]:
        """Resolve a task template's ``secrets`` blocks into a fresh map.

        Pure sync (``fromFile`` opens and reads), so the launch loop can
        push it to the default executor when a file read is involved; see
        :meth:`_prepare_task_run`.
        """
        from cronstable.config import _resolve_secret

        secrets: Dict[str, str] = {}
        for spec in template.secrets:
            name = spec.get("name")
            try:
                value = _resolve_secret(
                    spec,
                    "dag {} task {} secret {}".format(
                        dagcfg.name, intent.task_id, name
                    ),
                )
            except Exception:  # noqa: BLE001 - a bad secret is skipped, 404s
                continue
            if name and value is not None:
                secrets[name] = value
        return secrets

    async def _prepare_task_run(
        self, dagcfg: Any, run_id: str, run_key: str, intent, template
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """Register the task run with the loopback API; return its env.

        Mirrors ``Cron._prepare_job_api_run`` but scopes the run's default
        namespace to the DAG run's XCom scope and injects the
        ``CRONSTABLE_DAG_*``
        vars, so ``cronstable xcom`` / ``artifact`` land in the run scope.
        Like the mirror, ``fromFile`` secrets are staged on the default
        executor so a slow secret mount stalls only this launch, never the
        event loop; a mapped fan-out launches many instances back to back,
        which is exactly where an inline blocking read would compound.
        """
        scope = dag.xcom_scope(dagcfg.name, run_id)
        dag_env = {
            dag.ENV_DAG_NAME: dagcfg.name,
            dag.ENV_DAG_RUN_ID: run_id,
            dag.ENV_DAG_RUN_KEY: run_key,
            dag.ENV_DAG_TASK: intent.task_id,
            dag.ENV_DAG_TASKKEY: intent.taskkey,
            dag.ENV_DAG_MAP_INDEX: (
                "" if intent.map_index is None else str(intent.map_index)
            ),
            dag.ENV_DAG_MAP_ITEM: (
                "" if intent.map_item is None else json.dumps(intent.map_item)
            ),
            dag.ENV_DAG_XCOM_SCOPE: scope,
        }
        api = self._cron._job_api
        if api is None or api.base_url is None:
            return None, dag_env
        from cronstable.jobapi import RunContext, run_environment

        if any(spec.get("fromFile") for spec in template.secrets):
            secrets = await asyncio.get_running_loop().run_in_executor(
                None, self._stage_task_secrets, dagcfg, intent, template
            )
        else:
            secrets = self._stage_task_secrets(dagcfg, intent, template)
        ctx = RunContext(
            token=os.urandom(32).hex(),
            run_id=os.urandom(16).hex(),
            job_name=template.name,
            attempt=intent.attempt,
            scheduled_at=None,
            host=self._cron._state_host,
            default_scope=scope,
            allowed_scopes=set(template.stateAllowedScopes),
            secrets=secrets,
        )
        api.register_run(ctx)
        env = run_environment(ctx, api.base_url, api.cacert)
        env.update(dag_env)
        return ctx.token, env

    async def _set_pids(
        self,
        ref: RunRef,
        stamps: List[Tuple[str, str, Optional[int], Optional[int]]],
    ) -> None:
        """Record a whole launch loop's pids in one batched RMW.

        ``stamps`` carries ``(taskkey, proc, pid, attempt)`` per launched
        instance; :func:`dag.set_task_pids` applies each under the same
        per-entry proc-token/attempt fence the old per-task
        :func:`dag.set_task_pid` write used, so batching only removes RMWs,
        never a fence.
        """
        transform = self._wrap(dag.set_task_pids(stamps, _now()))
        await self._mutate(ref[0], ref[1], transform)

    # =====================================================================
    # Completion (called by the reaper via cron._handle_finished_job)
    # =====================================================================

    async def on_task_finished(self, running: RunningJob) -> None:
        dref = running.dag_ref
        assert dref is not None  # only called for a DAG-task RunningJob
        if dref.dag_name not in self._dags():
            return  # dag removed mid-run; the doc is orphaned, GC handles it
        success = running.fail_reason is None
        # sampled usage (monitorResources) rides the completion into the
        # dag_run document, serialised here so dag.py stays data-only.
        usage = running.resource_usage
        ref = (dref.dag_name, dref.run_key)
        # Buffer, don't record: the reaper hands over each finished task one at
        # a time, and flush_completions (invoked once it has drained the whole
        # batch) folds a run's completions into a single RMW. Recording here
        # would be one full-document write+fsync per task again.
        self._completion_buffer.setdefault(ref, []).append(
            {
                "taskkey": dref.taskkey,
                "taskId": dref.task_id,
                "success": success,
                "exitCode": running.retcode,
                "failReason": running.fail_reason,
                "proc": dref.proc,
                "attempt": dref.attempt,
                "poke": dref.poke,
                "resources": usage.to_dict() if usage is not None else None,
            }
        )

    async def flush_completions(self) -> None:
        """Record every buffered task completion, one batched RMW per run.

        Called by the reaper after it has handled a whole batch of finished
        jobs (see ``Cron._run``'s reaper loop).  A mapped fan-out whose N
        instances finish together is recorded in one full-document
        read-modify-write + fsync per run instead of N, and the run gets a
        single graph advance rather than one per task.  Robust by
        construction: a run whose flush raises has its whole batch re-queued
        for retry, never dropped, and one run's failure never affects another.
        """
        if not self._completion_buffer:
            return
        buffered = self._completion_buffer
        self._completion_buffer = {}
        for ref, entries in buffered.items():
            try:
                await self._flush_run_completions(ref, entries)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - never lose a completion
                # An unrecorded completion leaves its entry RUNNING forever
                # (trusted by reconciliation while this daemon lives); queue
                # the whole batch for retry rather than dropping it. A
                # removed-task entry self-drops on the retry (see
                # _finish_task), so queueing unconditionally is safe.
                logger.exception(
                    "dag run %s/%s: flushing task completions failed; "
                    "re-queued for retry",
                    ref[0],
                    ref[1],
                )
                for entry in entries:
                    self._queue_completion(
                        ref,
                        entry["taskkey"],
                        entry["taskId"],
                        success=entry["success"],
                        exit_code=entry["exitCode"],
                        fail_reason=entry["failReason"],
                        proc=entry["proc"],
                        attempt=entry["attempt"],
                        poke=entry["poke"],
                        resources=entry.get("resources"),
                    )

    async def _flush_run_completions(
        self, ref: RunRef, entries: List[Dict[str, Any]]
    ) -> None:
        dagcfg = self._dags().get(ref[0])
        if dagcfg is None:
            # dag removed on reload: drop these completions (and any queued
            # retry of them), exactly as _finish_task drops a removed task's.
            for entry in entries:
                self._pending_completions.pop((ref, entry["taskkey"]), None)
            return
        marks: List[Dict[str, Any]] = []
        live: List[Dict[str, Any]] = []
        for entry in entries:
            task = dagcfg.spec.by_id.get(entry["taskId"])
            if task is None:
                # the task was removed from the DAG (a config reload) while its
                # instance was running: drop the stale completion (and a queued
                # retry of it, which would otherwise re-run forever).
                self._pending_completions.pop((ref, entry["taskkey"]), None)
                continue
            jitter = (
                _jitter(task.poke_jitter) if task.type == dag.SENSOR else 0.0
            )
            marks.append(
                {
                    "taskkey": entry["taskkey"],
                    "success": entry["success"],
                    "exit_code": entry["exitCode"],
                    "fail_reason": entry["failReason"],
                    "task": task,
                    "jitter": jitter,
                    "expected_proc": entry["proc"],
                    "expected_attempt": entry["attempt"],
                    "expected_poke": entry["poke"],
                    "resources": entry.get("resources"),
                }
            )
            live.append(entry)
        if not marks:
            return  # every entry was a removed task: nothing to record/advance
        transform = self._wrap(dag.mark_tasks_finished(marks, _now()))
        try:
            _, applied = await self._mutate(ref[0], ref[1], transform)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one failed RMW must not wedge the run
            # Unlike a pid write, a lost completion is NOT best-effort:
            # unrecorded, the entry stays RUNNING under our proc token, which
            # reconciliation trusts forever while this daemon lives (and the
            # lease keeps peers out). So EVERY entry in the batch is queued for
            # retry, not just one.
            for entry in live:
                self._queue_completion(
                    ref,
                    entry["taskkey"],
                    entry["taskId"],
                    success=entry["success"],
                    exit_code=entry["exitCode"],
                    fail_reason=entry["failReason"],
                    proc=entry["proc"],
                    attempt=entry["attempt"],
                    poke=entry["poke"],
                    resources=entry.get("resources"),
                )
        else:
            applied_set = set(applied or [])
            for entry in live:
                # settled (applied, or fenced out as a duplicate/superseded/
                # stale-poke completion): a queued copy must not retry forever.
                self._pending_completions.pop((ref, entry["taskkey"]), None)
                if entry["taskkey"] not in applied_set:
                    logger.debug(
                        "dag run %s/%s: task %s completion dropped as "
                        "stale/duplicate by the fence",
                        ref[0],
                        ref[1],
                        entry["taskkey"],
                    )
        # one fresh advance for the whole batch, off the reaper's critical path
        # (a concurrent periodic advance may hold the per-run lock).
        self._spawn_advance(ref)

    async def _finish_task(
        self,
        dagcfg: Any,
        ref: RunRef,
        taskkey: str,
        task_id: str,
        *,
        success: bool,
        exit_code: Optional[int],
        fail_reason: Optional[str],
        proc: Optional[str] = None,
        attempt: Optional[int] = None,
        poke: Optional[int] = None,
        resources: Optional[Dict[str, Any]] = None,
    ) -> None:
        task = dagcfg.spec.by_id.get(task_id)
        if task is None:
            # the task was removed from the DAG (a config reload) while its
            # instance was running: drop the stale completion (including a
            # queued retry of it, which would otherwise re-run forever).
            self._pending_completions.pop((ref, taskkey), None)
            return
        jitter = _jitter(task.poke_jitter) if task.type == dag.SENSOR else 0.0
        transform = self._wrap(
            dag.mark_task_finished(
                taskkey,
                success=success,
                exit_code=exit_code,
                fail_reason=fail_reason,
                now=_now(),
                task=task,
                jitter=jitter,
                expected_proc=proc,
                expected_attempt=attempt,
                expected_poke=poke,
                resources=resources,
            )
        )
        try:
            _, applied = await self._mutate(ref[0], ref[1], transform)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one failed RMW must not wedge
            # the run: unrecorded, the entry stays RUNNING under our proc
            # token, which reconciliation trusts forever while this daemon
            # lives (and the lease keeps peers out) -- so queue the
            # completion for retry on later service passes.
            self._queue_completion(
                ref,
                taskkey,
                task_id,
                success=success,
                exit_code=exit_code,
                fail_reason=fail_reason,
                proc=proc,
                attempt=attempt,
                poke=poke,
                resources=resources,
            )
        else:
            # fenced out (a duplicate, a superseded attempt, or a stale poke's
            # queued retry) or applied: either way this completion is settled,
            # so a queued copy must not retry forever.
            self._pending_completions.pop((ref, taskkey), None)
            if not applied:
                logger.debug(
                    "dag run %s/%s: task %s completion dropped as "
                    "stale/duplicate by the fence",
                    ref[0],
                    ref[1],
                    taskkey,
                )
        # trigger a fresh advance without blocking the reaper on the per-run
        # lock (a concurrent periodic advance may hold it).
        self._spawn_advance(ref)

    def _queue_completion(
        self,
        ref: RunRef,
        taskkey: str,
        task_id: str,
        *,
        success: bool,
        exit_code: Optional[int],
        fail_reason: Optional[str],
        proc: Optional[str],
        attempt: Optional[int],
        poke: Optional[int],
        resources: Optional[Dict[str, Any]] = None,
    ) -> None:
        key = (ref, taskkey)
        prior = self._pending_completions.get(key)
        delay = COMPLETION_RETRY_DELAY
        if prior is not None:
            delay = min(prior["delay"] * 2.0, COMPLETION_RETRY_MAX_DELAY)
        self._pending_completions[key] = {
            "ref": ref,
            "taskkey": taskkey,
            "taskId": task_id,
            "success": success,
            "exitCode": exit_code,
            "failReason": fail_reason,
            "proc": proc,
            "attempt": attempt,
            "poke": poke,
            "resources": resources,
            "delay": delay,
            "nextTryAt": _now() + delay,
        }
        logger.warning(
            "dag run %s/%s: recording task %s completion failed; retrying "
            "in %.0fs",
            ref[0],
            ref[1],
            taskkey,
            delay,
        )

    async def _retry_completions(self, now: float) -> None:
        """Re-attempt completion records an earlier store hiccup dropped.

        ``mark_task_finished`` is fenced and idempotent (a duplicate or
        superseded apply is a no-op), so re-running the whole transform is
        safe even when the failed mutate partially landed: a sensor
        completion that landed despite the timeout bumped ``pokeCount``, so
        the queued copy fails the poke fence and is dropped instead of being
        applied to a later in-flight poke (proc/attempt alone cannot tell
        pokes apart).  A settled entry (applied OR fenced out as stale) pops
        the queue entry; another failure re-queues it with a bounded backoff.
        """
        for key in list(self._pending_completions):
            pc = self._pending_completions.get(key)
            if pc is None or pc["nextTryAt"] > now:
                continue
            ref = pc["ref"]
            dagcfg = self._dags().get(ref[0])
            if dagcfg is None:
                # dag removed on reload: the stale completion is dropped,
                # like on_task_finished drops it.
                self._pending_completions.pop(key, None)
                continue
            await self._finish_task(
                dagcfg,
                ref,
                pc["taskkey"],
                pc["taskId"],
                success=pc["success"],
                exit_code=pc["exitCode"],
                fail_reason=pc["failReason"],
                proc=pc["proc"],
                attempt=pc["attempt"],
                poke=pc["poke"],
                resources=pc.get("resources"),
            )

    # =====================================================================
    # Crash reconciliation
    # =====================================================================

    async def reconcile_on_boot(self) -> None:
        """Adopt and reconcile this node's active runs after a restart.

        Called from rehydration (after the job reconciler).  Lists every dag's
        active runs and tries to take each one's lease; a run still owned by a
        live peer stays with it, one whose owner is gone is adopted here and
        its interrupted tasks reconciled from durable state -- the DAG analogue
        of ``Cron._reconcile_inflight``.
        """
        backend = self._backend()
        if backend is None or not self.has_dags():
            return
        for name, dagcfg in list(self._dags().items()):
            try:
                docs = await asyncio.wait_for(
                    backend.list_documents(self._ns(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "dag %s: boot reconciliation timed out reading runs", name
                )
                continue
            except Exception as ex:
                logger.warning(
                    "dag %s: boot reconciliation could not read runs "
                    "(continuing with the remaining dags): %s",
                    name,
                    ex,
                )
                continue
            for body in docs:
                if dag.is_terminal_run(body):
                    continue
                run_key = body.get("runKey")
                if not isinstance(run_key, str):
                    continue
                try:
                    await self._try_own(dagcfg, (name, run_key))
                except Exception as ex:
                    # One stalled or strict-unreadable run document must not
                    # abort boot reconciliation: this loop runs inside the
                    # state-rehydration tail, and an escaping raise skipped
                    # every remaining run AND the _start_job_api call after
                    # it, for the life of the backend generation (the
                    # rehydrated latch is already set, so no later pass
                    # retries). The run stays unowned here; the next service
                    # pass retries owning it.
                    logger.warning(
                        "dag %s: boot reconciliation of run %s failed "
                        "(continuing with the remaining runs): %s",
                        name,
                        run_key,
                        ex,
                    )

    async def _reconcile_run(
        self, dagcfg: Any, ref: RunRef
    ) -> Optional[Dict[str, Any]]:
        """Fail tasks a crash left running; return the observed document.

        The RMW already read the run document under its lock (and
        ``mutate_document`` hands the current body back even on a kept
        document), so the caller reuses the returned body instead of
        paying a second full read of the same document right after.
        ``None`` when the document does not exist (or no backend).
        """
        transform = self._wrap(
            dag.reconcile_crashed(
                dagcfg.spec,
                _now(),
                self._cron._proc_token,
                self._cron._state_host,
                platform.pid_alive,
            )
        )
        stored, changed = await self._mutate(ref[0], ref[1], transform)
        if changed:
            logger.info(
                "dag run %s/%s: reconciled %d interrupted task(s)",
                ref[0],
                ref[1],
                changed,
            )
        return stored

    # =====================================================================
    # Control-API surface: approvals, introspection, manual trigger, backfill
    # =====================================================================

    async def approve(
        self,
        dag_name: str,
        run_key: str,
        taskkey: str,
        *,
        approved: bool,
        by: str,
    ) -> Dict[str, Any]:
        """Record an approval-gate decision, then advance the run."""
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None:
            return {"ok": False, "reason": "no such dag"}
        task_id = taskkey.split("#", 1)[0]
        task = dagcfg.spec.by_id.get(task_id)
        on_reject = task.on_reject if task is not None else dag.FAILED
        transform = self._wrap(
            dag.apply_approval(
                taskkey,
                approved=approved,
                by=by,
                now=_now(),
                on_reject=on_reject,
            )
        )
        _stored, result = await self._mutate(dag_name, run_key, transform)
        if result and result.get("ok"):
            self._spawn_advance((dag_name, run_key))
        return result or {"ok": False, "reason": "no such run"}

    async def trigger_run(
        self, dag_name: str, *, logical_date: Optional[str] = None
    ) -> Optional[str]:
        """Create a manual run of ``dag_name`` now; return its run key.

        ``None`` for an unknown dag; raises when the run document could not
        be created (no state backend available), so the caller never gets a
        run key for a run that does not exist -- the web handler surfaces the
        exception instead of a false 200.
        """
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None:
            return None
        run_key = "manual-" + os.urandom(6).hex()
        created = await self._create_doc(
            dagcfg, run_key, logical_date, "manual"
        )
        if not created:
            # the key is random, so "already exists" is not a real case:
            # not-created means no backend was available to write it.
            raise RuntimeError(
                "dag {}: the manual run could not be recorded (state "
                "backend unavailable)".format(dag_name)
            )
        await self._try_own(dagcfg, (dag_name, run_key))
        return run_key

    async def backfill(
        self, dag_name: str, start_iso: str, end_iso: str
    ) -> Dict[str, Any]:
        """Create runs for every scheduled instant in ``[start, end]``.

        A deliberate replay: it is bounded by ``DAG_MAX_CATCHUP`` but ignores
        the automatic catch-up deadline (the operator asked for it).
        Idempotent -- each date's run key create-if-absents, so re-running a
        backfill does not duplicate runs.
        """
        dagcfg = self._dags().get(dag_name)
        if dagcfg is None or dagcfg.schedule_job is None:
            return {"ok": False, "reason": "no such scheduled dag"}
        sched = dagcfg.schedule_job
        if not isinstance(sched.schedule, CronTab):
            # e.g. the literal "@reboot": no computable instants to replay --
            # a clean refusal (-> 400), not a 500 out of _compute_next_fire.
            return {
                "ok": False,
                "reason": "the dag's schedule has no computable instants",
            }
        start = _parse_iso(start_iso)
        end = _parse_iso(end_iso)
        if start is None or end is None or end < start:
            return {"ok": False, "reason": "bad date range"}
        created = 0
        # step from just before start so an instant exactly at start counts
        cursor = start - datetime.timedelta(seconds=1)
        nxt = self._next_fire(sched, cursor)
        while nxt is not None and nxt <= end and created < DAG_MAX_CATCHUP:
            await self._create_run(dagcfg, nxt, "backfill")
            created += 1
            nxt = self._next_fire(sched, nxt)
        return {"ok": True, "created": created}

    async def list_dags(self) -> List[Dict[str, Any]]:
        """Per-DAG summary for the dashboard index.

        Carries the static graph (nodes + edges + per-task type/triggerRule/
        retries/fan-out marker) plus, when a backend is present, the latest
        run's state and a run-state histogram -- enough to render a health
        card without an N+1 of per-DAG ``/runs`` calls.  The durable read is
        best-effort: a slow/absent backend simply omits the run rollup rather
        than failing the whole index (mirrors ``_web_job_trends``).  The
        human-readable schedule string is grafted on by the web handler, which
        owns ``schedule_str`` (avoiding a cron<->dagrun import cycle).
        """
        backend = self._backend()
        # a reload that removed (or renamed) a dag must not leave its run
        # summaries pinned in the caches for the life of the process; the
        # live config is the authority on which keys may stay.
        live = self._dags()
        for stale in [k for k in self._summaries_memo if k not in live]:
            del self._summaries_memo[stale]
        for stale in [k for k in self._dag_summary_cache if k not in live]:
            del self._dag_summary_cache[stale]
        out = []
        for name, dagcfg in live.items():
            entry: Dict[str, Any] = {
                "name": name,
                "enabled": dagcfg.enabled,
                "scheduled": dagcfg.schedule_job is not None,
                "retainRuns": dagcfg.retain_runs,
                "tasks": [
                    {
                        "id": t.spec.id,
                        "type": t.spec.type,
                        "dependsOn": list(t.spec.depends_on),
                        "triggerRule": t.spec.trigger_rule,
                        "retries": max(0, t.spec.max_attempts - 1),
                        "mapped": t.spec.expand is not None,
                    }
                    for t in dagcfg.tasks
                ],
            }
            if backend is not None:
                rollup = await self._dag_run_rollup(backend, name)
                if rollup:
                    entry.update(rollup)
            out.append(entry)
        return out

    @staticmethod
    def _summarize_run(body: Dict[str, Any]) -> Dict[str, Any]:
        """Everything the run listings need from a run document, plus whether
        the run is terminal (so its summary can be cached).

        Serves both list_dags' rollup and :meth:`list_runs`, so it carries the
        run-list fields (``runId``/``logicalDate``/``taskStates``) too: one
        cache entry per run, filled once, rather than each caller re-reading
        and re-parsing every retained document.  The task histogram is a walk
        of the body the caller has already parsed, which is cheap next to the
        parse it saves on later calls.
        """
        counts: Dict[str, int] = {}
        for entry in body.get("tasks", {}).values():
            st = entry.get("state", "unknown")
            counts[st] = counts.get(st, 0) + 1
        return {
            "runKey": body.get("runKey"),
            "runId": body.get("runId"),
            "state": str(body.get("state", "running")),
            "kind": body.get("kind"),
            "logicalDate": body.get("logicalDate"),
            "createdAt": body.get("createdAt"),
            "updatedAt": body.get("updatedAt"),
            "taskStates": counts,
            "terminal": dag.is_terminal_run(body),
        }

    @staticmethod
    def _rollup_from_summaries(
        summaries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """latestRun (newest by createdAt), runCounts histogram, totalRuns."""
        if not summaries:
            return {}
        latest = max(summaries, key=lambda s: float(s.get("createdAt") or 0))
        counts: Dict[str, int] = {}
        for s in summaries:
            counts[s["state"]] = counts.get(s["state"], 0) + 1
        return {
            "latestRun": {
                "runKey": latest.get("runKey"),
                "state": latest.get("state"),
                "kind": latest.get("kind"),
                "createdAt": latest.get("createdAt"),
                "updatedAt": latest.get("updatedAt"),
            },
            "runCounts": counts,
            "totalRuns": len(summaries),
        }

    async def _bulk_summaries(
        self, backend: StateBackend, ns: str, name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """One list_documents sweep: rebuild the cache from every body. Used
        for the cold cache / large-delta case and when the backend cannot list
        keys only. Returns None on a hiccup, matching the old degrade
        behaviour."""
        try:
            docs = await asyncio.wait_for(
                backend.list_documents(ns), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade, never fail /dags
            return None
        cache = self._dag_summary_cache.setdefault(name, {})
        cache.clear()
        summaries = []
        for body in docs:
            s = self._summarize_run(body)
            summaries.append(s)
            if isinstance(s["runKey"], str):
                cache[s["runKey"]] = s
        return summaries

    async def _bulk_rollup(
        self, backend: StateBackend, ns: str, name: str
    ) -> Optional[Dict[str, Any]]:
        """The full-sweep rollup: :meth:`_bulk_summaries`, rolled up."""
        summaries = await self._bulk_summaries(backend, ns, name)
        if summaries is None:
            return None
        return self._rollup_from_summaries(summaries)

    async def _run_summaries(
        self, backend: StateBackend, name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Every retained run's summary, memoized for DAG_SUMMARY_LIST_TTL.

        The memo serves the poll traffic of a QUIESCENT dag from memory:
        without it the keys listing below still hit the store once per dag
        per /dags poll (and per run-drawer refresh) per viewer.  A fresh
        shallow copy is returned each time because list_runs sorts its
        result in place.  Failures are never memoized, and local writes pop
        the entry (see _mutate), so the TTL delays only remote nodes'
        changes.
        """
        memo = self._summaries_memo.get(name)
        if (
            memo is not None
            and time.monotonic() - memo[0] < DAG_SUMMARY_LIST_TTL
        ):
            return list(memo[1])
        summaries = await self._run_summaries_uncached(backend, name)
        if summaries is not None:
            self._summaries_memo[name] = (time.monotonic(), summaries)
            return list(summaries)
        return None

    async def _run_summaries_uncached(
        self, backend: StateBackend, name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Every retained run's summary, caching immutable terminal runs.

        Lists keys only, drops cache entries for GC'd runs, and re-reads just
        the new or still-running documents (terminal ones are served from
        cache) -- so a ~3s /dags poll, and every run-list request, stop
        re-parsing every retained run of every dag. Falls back to a single
        full parse when the backend cannot list keys only or when many bodies
        need reading at once (cold cache). Best-effort: returns None (the
        caller degrades) rather than failing the request.
        """
        ns = self._ns(name)
        try:
            keys = await asyncio.wait_for(
                backend.list_document_keys(ns), timeout=STATE_OP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade, never fail /dags
            return None
        if keys is None:
            # backend has no keys-only listing: one full parse, no caching win
            return await self._bulk_summaries(backend, ns, name)
        cache = self._dag_summary_cache.setdefault(name, {})
        keyset = set(keys)
        for gone in [k for k in cache if k not in keyset]:
            del cache[gone]
        to_read = [
            k for k in keys if not (k in cache and cache[k]["terminal"])
        ]
        if len(to_read) > DAG_ROLLUP_BULK_THRESHOLD:
            return await self._bulk_summaries(backend, ns, name)
        for key in to_read:
            try:
                body = await asyncio.wait_for(
                    backend.read_document(ns, key), timeout=STATE_OP_TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - degrade, never fail /dags
                return None
            if body is None:
                cache.pop(key, None)  # deleted since the listing
                continue
            cache[key] = self._summarize_run(body)
        return list(cache.values())

    async def _dag_run_rollup(
        self, backend: StateBackend, name: str
    ) -> Optional[Dict[str, Any]]:
        """Per-dag run rollup for list_dags, over the cached summaries."""
        summaries = await self._run_summaries(backend, name)
        if summaries is None:
            return None
        return self._rollup_from_summaries(summaries)

    async def get_run(
        self, dag_name: str, run_key: str
    ) -> Optional[Dict[str, Any]]:
        if dag_name not in self._dags():
            return None
        return await self._read(dag_name, run_key)

    async def xcom_for_run(
        self,
        dag_name: str,
        run_key: str,
        *,
        max_value_bytes: int = 65536,
        max_entries: int = 500,
    ) -> Optional[Dict[str, Any]]:
        """Every XCom value published by this run's tasks, for the dashboard.

        XCom lives in the artifact store under ``dagxcom/<dag>/<run_id>`` with
        each hand-off named ``<taskkey>/<key>`` (see :func:`dag.xcom_scope` /
        :func:`dag.xcom_name`); this reassembles those into a flat list with
        small values inlined (decoded as text) and larger ones surfaced as
        metadata only.  ``None`` if the dag or run is unknown; degrades to an
        empty list on a backend hiccup rather than failing.
        """
        from cronstable import jobstate

        backend = self._backend()
        if backend is None or dag_name not in self._dags():
            return None
        body = await self._read(dag_name, run_key)
        if body is None:
            return None
        run_id = body.get("runId")
        result: Dict[str, Any] = {
            "dag": dag_name,
            "runKey": run_key,
            "runId": run_id,
            "entries": [],
            "truncated": False,
        }
        if not run_id:
            return result
        scope = dag.xcom_scope(dag_name, str(run_id))
        try:
            records = await asyncio.wait_for(
                jobstate.artifact_list(backend, scope),
                timeout=STATE_OP_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - degrade, never 500 the tab
            return result
        result["truncated"] = len(records) > max_entries
        for rec in records[:max_entries]:
            full = str(rec.get("name") or "")
            taskkey, _, key = full.partition("/")
            size = rec.get("size")
            entry: Dict[str, Any] = {
                "taskkey": taskkey,
                "key": key,
                "sha256": rec.get("sha256"),
                "size": size,
                "at": rec.get("at"),
            }
            if isinstance(size, int) and 0 <= size <= max_value_bytes:
                # fetch the payload by the digest this record already
                # carries: an artifact_get here would re-list and re-parse
                # the run's whole artifact stream per entry (quadratic in
                # the stream), only to resolve the very record in hand.
                # A swept blob reads back as None and is skipped, exactly
                # like any other unreadable value.
                digest = rec.get("sha256")
                data = None
                if digest:
                    try:
                        data = await asyncio.wait_for(
                            backend.get_blob(str(digest)),
                            timeout=STATE_OP_TIMEOUT,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001 - unreadable; skip it
                        data = None
                if data is not None:
                    try:
                        entry["value"] = data.decode("utf-8")
                    except UnicodeDecodeError:
                        entry["binary"] = True
            else:
                entry["oversize"] = True
            result["entries"].append(entry)
        return result

    async def list_runs(
        self, dag_name: str, *, limit: int = 50
    ) -> Optional[List[Dict[str, Any]]]:
        """The newest ``limit`` runs of ``dag_name``, newest first.

        Served from the same per-key summary cache list_dags' rollup fills, so
        a drawer open (or an MCP run listing) re-reads only the runs that are
        still going: reading and parsing all ``retainRuns`` documents cost
        tens of milliseconds of CPU per request on a wide mapped fan-out, all
        of it re-deriving summaries of runs that finished long ago.
        """
        backend = self._backend()
        if backend is None or dag_name not in self._dags():
            return None
        summaries = await self._run_summaries(backend, dag_name)
        if summaries is None:
            # the keys-only listing or a body read failed: take the plain
            # full-listing path, which surfaces a store failure to the caller
            # exactly as this method always has.
            docs = await asyncio.wait_for(
                backend.list_documents(self._ns(dag_name)),
                timeout=STATE_OP_TIMEOUT,
            )
            docs.sort(
                key=lambda b: float(b.get("createdAt") or 0), reverse=True
            )
            return [_listed_run(self._summarize_run(b)) for b in docs[:limit]]
        summaries.sort(
            key=lambda s: float(s.get("createdAt") or 0), reverse=True
        )
        return [_listed_run(s) for s in summaries[:limit]]

    # =====================================================================
    # Retention GC (DAG-owned; dag_run documents live outside the record GC)
    # =====================================================================

    async def _gc_runs(self) -> None:
        backend = self._backend()
        if backend is None:
            return
        for name, dagcfg in list(self._dags().items()):
            try:
                await self._gc_one_dag(backend, name, dagcfg)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("dag %s: run GC failed", name)

    async def _gc_one_dag(
        self, backend: StateBackend, name: str, dagcfg: Any
    ) -> None:
        docs = await asyncio.wait_for(
            backend.list_documents(self._ns(name)),
            timeout=STATE_OP_TIMEOUT,
        )
        terminal = [b for b in docs if dag.is_terminal_run(b)]
        # this pass parsed every body anyway: rebuild the adopt scan's
        # terminal-key cache from truth (its periodic self-heal).
        self._terminal_run_keys[name] = {
            b["runKey"] for b in terminal if isinstance(b.get("runKey"), str)
        }
        terminal.sort(key=lambda b: float(b.get("createdAt") or 0))
        excess = len(terminal) - dagcfg.retain_runs
        if excess <= 0:
            return
        for body in terminal[:excess]:
            run_key = body.get("runKey")
            if not run_key:
                continue
            await self._delete_run(backend, name, run_key, body.get("runId"))

    async def _delete_run(
        self,
        backend: StateBackend,
        name: str,
        run_key: str,
        run_id: Any,
    ) -> None:
        """Delete one run document and prune its XCom record stream.

        The stream's blobs become unreferenced once the records are gone;
        the state GC's orphan-blob sweep (cron._collect_state_garbage /
        `cronstable state gc`) reclaims them on its next pass.  Record order
        matters: the document goes FIRST, so a crash between the two leaves
        a doc-less stream the stream GC ages out, never a live run whose
        XCom vanished.
        """
        await asyncio.wait_for(
            backend.delete_document(self._ns(name), run_key),
            timeout=STATE_OP_TIMEOUT,
        )
        # the other local write shape (_mutate covers the rest): the memo
        # must not keep serving a run the GC just deleted.
        self._summaries_memo.pop(name, None)
        known = self._terminal_run_keys.get(name)
        if known is not None:
            # the key may legitimately come back (an operator backfill of the
            # same logical date re-creates it): a deleted key must not linger
            # as "known terminal".
            known.discard(run_key)
        if run_id:
            from cronstable.jobstate import ARTIFACT_STREAM_PREFIX

            scope = dag.xcom_scope(name, str(run_id))
            try:
                await asyncio.wait_for(
                    backend.prune_records(
                        ARTIFACT_STREAM_PREFIX + scope, keep=0
                    ),
                    timeout=STATE_OP_TIMEOUT,
                )
            except Exception:  # noqa: BLE001 - best effort
                pass

    async def gc_removed_dags(
        self,
        backend: StateBackend,
        dag_names: "set[str]",
        grace: float,
    ) -> None:
        """Collect run documents (and XCom) of dags gone from every config.

        Called from the daemon's state GC pass (cron._collect_state_garbage)
        with the dag names that exist in the store's ``dagrun/`` namespaces
        but are in NEITHER this node's config NOR any recent manifest -- the
        same absence anchor job streams use, so a dag briefly removed during
        a config edit keeps its whole run history for a full gcGraceSeconds.
        Belt and braces on top of that anchor: only a TERMINAL run whose
        last update is itself older than ``grace`` is deleted; an active,
        owned, or undatable run is never touched, so a re-added dag resumes
        it exactly where it stopped.
        """
        now = _now()
        for name in sorted(dag_names):
            if name in self._dags():
                continue  # re-added since the caller built the live set
            try:
                docs = await asyncio.wait_for(
                    backend.list_documents(self._ns(name)),
                    timeout=STATE_OP_TIMEOUT,
                )
                for body in docs:
                    if not dag.is_terminal_run(body):
                        continue
                    run_key = body.get("runKey")
                    if not isinstance(run_key, str) or not run_key:
                        continue
                    if (name, run_key) in self._owned:
                        continue
                    updated = body.get("updatedAt") or body.get("createdAt")
                    if (
                        not isinstance(updated, (int, float))
                        or now - float(updated) < grace
                    ):
                        continue  # too recent, or undatable: keep
                    await self._delete_run(
                        backend, name, run_key, body.get("runId")
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one dag must not stop the pass
                logger.exception("dag %s: removed-dag run GC failed", name)

    # =====================================================================
    # Shutdown
    # =====================================================================

    async def shutdown(self) -> None:
        """Release every held advance lease and stop the renewers."""
        if self._service_task is not None and not self._service_task.done():
            self._service_task.cancel()
        for task in list(self._catchup_tasks):
            if not task.done():
                task.cancel()
        self._catchup_tasks.clear()
        for ref in list(self._owned):
            await self._release(ref)

    def forget(self) -> None:
        """Drop all in-memory run ownership after a backend swap.

        The old store's advance leases lapse by TTL (their renewers are
        cancelled here, since renewing them against a dead store is pointless);
        the new store's active runs are re-adopted from scratch by
        :meth:`reconcile_on_boot`, which reruns because the backend swap
        cleared ``_state_rehydrated``.  No store ops here -- the old backend is
        already gone.
        """
        if self._service_task is not None and not self._service_task.done():
            self._service_task.cancel()
        self._service_task = None
        # a deferred catch-up replay sleeping out its jitter targeted the
        # old store; the new store's own seed pass owes it nothing.
        for task in list(self._catchup_tasks):
            if not task.done():
                task.cancel()
        self._catchup_tasks.clear()
        for renewer in list(self._renewers.values()):
            if not renewer.done():
                renewer.cancel()
        self._renewers.clear()
        self._owned.clear()
        self._wake.clear()
        self._advance_pending.clear()
        self._locks.clear()
        self._next_logical.clear()
        self._seeded.clear()
        self._seed_failed.clear()
        # queued completions targeted the old store; the new store's runs are
        # reconciled from scratch (a still-RUNNING entry is recovered there).
        self._pending_completions.clear()
        self._completion_buffer.clear()
        # Run keys are deterministic (dag name + logical date), so the new
        # store's runs collide with whatever the old store left cached here.
        # _dag_summary_cache is the load-bearing one: _dag_run_rollup skips
        # re-reading any key whose cached summary is terminal, and unlike
        # _terminal_run_keys nothing rebuilds it on a timer -- so without this
        # clear, /dags serves the OLD store's finished state for the NEW
        # store's live run until an unrelated eviction happens to knock the
        # key out. _terminal_run_keys would self-heal at the next full adopt
        # pass, but until then it suppresses adoption of the new store's runs,
        # so drop it here and bring that pass forward to now.
        self._dag_summary_cache.clear()
        self._summaries_memo.clear()
        self._terminal_run_keys.clear()
        self._advance_again.clear()
        self._next_sched_check = 0.0
        self._next_adopt = 0.0
        self._next_full_adopt = 0.0
        self._next_gc = 0.0


# --------------------------------------------------------------------------
# module helpers
# --------------------------------------------------------------------------


def _parse_iso(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _listed_run(summary: Dict[str, Any]) -> Dict[str, Any]:
    """One run summary as the run-list payload.

    Rebuilt field by field rather than returned as-is: a summary carries the
    rollup's private ``terminal`` flag, and callers must never see it (nor be
    able to mutate the cached dict it may have come from).  The single
    definition of that payload, so the cached and full-listing paths of
    :meth:`DagScheduler.list_runs` cannot drift apart.
    """
    return {
        "runKey": summary.get("runKey"),
        "runId": summary.get("runId"),
        "state": summary.get("state"),
        "kind": summary.get("kind"),
        "logicalDate": summary.get("logicalDate"),
        "createdAt": summary.get("createdAt"),
        "updatedAt": summary.get("updatedAt"),
        "taskStates": dict(summary.get("taskStates") or {}),
    }
