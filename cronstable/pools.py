"""Durable admission queues shared by jobs and DAG tasks."""

import asyncio
import copy
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from cronstable.fingerprint import job_digest_cached
from cronstable.state import DOC_KEEP

logger = logging.getLogger(__name__)
NAMESPACE = "scheduler-pools"
OP_TIMEOUT = 5.0
LEASE_SECONDS = 30.0
TERMINAL = frozenset({"finished", "expired", "cancelled"})


class PoolError(Exception):
    def __init__(self, message: str, *, pool=None, key=None) -> None:
        super().__init__(message)
        self.pool = pool
        self.key = key


def _unobserved_task(entry) -> bool:
    return (
        entry["state"] in ("cancelled", "expired")
        and entry["payload"].get("kind") == "task"
        and not entry.get("observed")
    )


@dataclass
class Ticket:
    pool: str
    key: str
    owner: str
    backend: Any
    deadline: float
    running: Any = None
    completion: Optional[tuple[str, Optional[str]]] = None
    payload: dict[str, Any] = field(default_factory=dict)
    valid: bool = True


@dataclass
class RetrySettlement:
    generation: str
    reason: str
    previous: Optional[str] = None


def _maintain(body: dict[str, Any], now: float) -> None:
    entries = body["entries"]
    for entry in entries.values():
        if entry["state"] == "running" and entry["leaseUntil"] <= now:
            entry["state"] = "queued"
            entry["owner"] = None
        if entry["state"] == "queued" and entry["expiresAt"] <= now:
            entry.update(
                state="expired", reason="queue timeout", finishedAt=now
            )
    finished = sorted(
        (
            e
            for e in entries.values()
            if e["state"] in TERMINAL and not _unobserved_task(e)
        ),
        key=lambda e: (e.get("finishedAt", 0), e["id"]),
        reverse=True,
    )
    for entry in finished[100:]:
        entries.pop(entry["id"], None)


def _waiting(body):
    return sorted(
        (e for e in body["entries"].values() if e["state"] == "queued"),
        key=lambda e: (-e["priority"], e["queuedAt"], e["id"]),
    )


class PoolScheduler:
    def __init__(self, cron: Any) -> None:
        self.cron = cron
        self.held: dict[tuple[str, str], Ticket] = {}
        self._task: Optional[asyncio.Task] = None
        self._heartbeat: Optional[asyncio.Task] = None
        self._wake = asyncio.Event()
        self._retry_settlements: dict[tuple[str, str], RetrySettlement] = {}

    def service(self) -> None:
        if self._task is None or self._task.done():
            if self.cron.pool_config or self.held:
                self._task = asyncio.create_task(self._run())
        if (self.cron.pool_config or self.held) and (
            self._heartbeat is None or self._heartbeat.done()
        ):
            self._heartbeat = asyncio.create_task(self._renew_loop())
        self._wake.set()

    async def close(self) -> None:
        tasks = [t for t in (self._task, self._heartbeat) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._task = self._heartbeat = None

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.gather(
                *(self._renew(t) for t in list(self.held.values()))
            )
            await asyncio.sleep(5)

    @staticmethod
    def check_ticket(ticket: Ticket) -> None:
        if not ticket.valid or time.monotonic() >= ticket.deadline:
            raise PoolError("pool lease was lost before launch")

    async def _run(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("pool queue service failed; retrying")
            try:
                await asyncio.wait_for(self._wake.wait(), 1.0)
            except asyncio.TimeoutError:
                pass

    async def _change(self, pool, action, *, backend=None, strict=False):
        backend = backend or self.cron.state_backend
        if backend is None:
            raise PoolError("pool state is unavailable")
        conf = self.cron.pool_config.get(pool)
        if strict and conf is None:
            raise PoolError("unknown pool {!r}".format(pool))
        if strict and await self.cron._slot_fidelity_reason():
            raise PoolError("pool state requires reliable exclusive locks")
        now = time.time()

        def transform(current):
            body = (
                copy.deepcopy(current)
                if current is not None
                else {
                    "slots": conf["slots"] if conf is not None else 0,
                    "entries": {},
                }
            )
            _maintain(body, now)
            if strict and conf is not None and body["slots"] != conf["slots"]:
                if not any(
                    e["state"] not in TERMINAL
                    for e in body["entries"].values()
                ):
                    body["slots"] = conf["slots"]
            result = action(body, now)
            return (DOC_KEEP if body == current else body), result

        _, result = await asyncio.wait_for(
            backend.mutate_document(NAMESPACE, pool, transform), OP_TIMEOUT
        )
        return result

    async def enqueue(
        self, job, *, key=None, payload=None, retry_state=None, resume=False
    ):
        key = key or uuid.uuid4().hex
        conf = self.cron.pool_config.get(job.pool)
        if conf is None:
            raise PoolError("unknown pool {!r}".format(job.pool))

        def add(body, now):
            old = body["entries"].get(key)
            if old is not None and not (
                resume and old["state"] in ("cancelled", "expired")
            ):
                return old
            if body["slots"] != conf["slots"]:
                raise PoolError("pool is draining before a capacity change")
            if (
                len(_waiting(body))
                + sum(_unobserved_task(e) for e in body["entries"].values())
                >= conf["maxQueued"]
            ):
                raise PoolError("pool queue is full")
            context = dict(payload or {})
            if retry_state is not None:
                scope = self._retry_scope(job.name, context.get("targetHost"))
                guard = retry_state.pool_retry or {
                    "pool": job.pool,
                    "scope": scope,
                    "generation": body.get("retryGenerations", {}).get(
                        scope, ""
                    ),
                }
                context["retryGuard"] = guard
                context["retryCancelled"] = retry_state.cancelled
                context["retry"] = {
                    "count": retry_state.count,
                    "delay": retry_state.delay,
                }
            entry = {
                "id": key,
                "job": job.name,
                "state": "queued",
                "slots": job.poolSlots,
                "priority": job.queuePriority,
                "queuedAt": now,
                "expiresAt": now + job.queueTimeout,
                "digest": job_digest_cached(job),
                "owner": None,
                "payload": context,
            }
            if payload and payload.get("kind") == "task":
                entry.update(
                    {k: payload[k] for k in ("dag", "runKey", "task")}
                )
            body["entries"][key] = entry
            return entry

        await self._flush_retry_settlements(job.pool)
        entry = await self._change(job.pool, add, strict=True)
        if retry_state is not None:
            retry_state.pool_retry = entry["payload"].get("retryGuard")
        self.service()
        return entry

    async def enqueue_job(
        self, job, *, with_retries=True, manual=False, key=None, resume=False
    ):
        slot = self.cron._last_run_slot.get(job.name)
        retry_state = (
            self.cron.retry_state.get(job.name)
            if with_retries and not manual
            else None
        )
        if (
            key is None
            and retry_state is not None
            and retry_state.count > 0
            and retry_state.pool_retry
        ):
            # Rehydration or a timed-out admission must find the same attempt.
            key = self._key(
                ("retry", retry_state.pool_retry, retry_state.count)
            )
        return await self.enqueue(
            job,
            key=key,
            payload={
                "kind": "job",
                "withRetries": with_retries,
                "manual": manual,
                "targetHost": self.cron._state_host
                if manual or job.clusterPolicy == "EveryNode"
                else None,
                "scheduledAt": slot.isoformat()
                if slot is not None and not manual
                else None,
            },
            retry_state=retry_state,
            resume=resume,
        )

    @staticmethod
    def _key(parts):
        return hashlib.sha256(
            json.dumps(parts, sort_keys=True).encode()
        ).hexdigest()

    def catchup_key(self, job, watermark, index):
        host = (
            self.cron._state_host if job.clusterPolicy == "EveryNode" else None
        )
        return self._key(
            (
                "catchup",
                job.name,
                job_digest_cached(job),
                host,
                watermark,
                index,
            )
        )

    @staticmethod
    def _retry_scope(name, host):
        return json.dumps([name, host], separators=(",", ":"))

    @staticmethod
    def restore_retry(state, record):
        guard = record.get("poolRetry")
        if isinstance(guard, dict) and all(
            isinstance(guard.get(key), str)
            for key in ("pool", "scope", "generation")
        ):
            state.pool_retry = guard

    @staticmethod
    def _retry_current(body, payload):
        if payload.get("retryCancelled"):
            return False
        guard = payload.get("retryGuard")
        return (
            guard is None
            or body.get("retryGenerations", {}).get(guard["scope"], "")
            == guard["generation"]
        )

    async def retry_current(self, guard):
        if guard is None:
            return True
        await self._flush_retry_settlements(guard["pool"])
        return await self._change(
            guard["pool"],
            lambda body, now: self._retry_current(body, {"retryGuard": guard}),
        )

    async def settle_retries(self, name, reason):
        job = self.cron.cron_jobs.get(name)
        # EveryNode ladders and explicit starts belong to their node.
        scopes = {self._retry_scope(name, self.cron._state_host)}
        if job is None or job.clusterPolicy != "EveryNode":
            scopes.add(self._retry_scope(name, None))
        for pool in self.cron.pool_config:
            for scope in scopes:
                self._retry_settlements.setdefault(
                    (pool, scope), RetrySettlement(uuid.uuid4().hex, reason)
                )
            try:
                await self._flush_retry_settlements(pool)
            except (PoolError, OSError, asyncio.TimeoutError):
                logger.warning("pool %s: retry settlement deferred", pool)
        self.service()

    async def _flush_retry_settlements(self, pool):
        for (name, scope), settlement in list(self._retry_settlements.items()):
            if name != pool:
                continue

            def settle(body, now, scope=scope, settlement=settlement):
                generations = body.setdefault("retryGenerations", {})
                current = generations.get(scope, "")
                if settlement.previous is None:
                    settlement.previous = current
                if current != settlement.previous:
                    # A timed-out mutation can land after its retry. Never
                    # repeat cancellation or overwrite a newer generation.
                    return
                generations[scope] = settlement.generation
                for entry in body["entries"].values():
                    payload = entry["payload"]
                    retry = payload.get("retry")
                    if (
                        retry is None
                        or self._retry_scope(
                            entry["job"], payload.get("targetHost")
                        )
                        != scope
                    ):
                        continue
                    payload["retryCancelled"] = True
                    if entry["state"] == "queued" and retry["count"] > 0:
                        entry.update(
                            state="cancelled",
                            reason=settlement.reason,
                            finishedAt=now,
                        )

            await self._change(pool, settle)
            if self._retry_settlements.get((pool, scope)) == settlement:
                del self._retry_settlements[(pool, scope)]

    async def acquire(self, pool: str, key: str) -> Optional[Ticket]:
        if (pool, key) in self.held:
            return None
        owner = self.cron._proc_token + ":" + uuid.uuid4().hex
        backend = self.cron.state_backend

        def claim(body, now):
            entry = body["entries"].get(key)
            if entry is None or entry["state"] != "queued":
                return False
            target = entry["payload"].get("targetHost")
            if target is not None and target != self.cron._state_host:
                return False
            retry = entry["payload"].get("retry")
            if (
                retry
                and retry["count"] > 0
                and not self._retry_current(body, entry["payload"])
            ):
                entry.update(
                    state="cancelled",
                    reason="retry superseded",
                    finishedAt=now,
                )
                return False
            used = sum(
                e["slots"]
                for e in body["entries"].values()
                if e["state"] == "running"
            )
            waiting = _waiting(body)
            if (
                not waiting
                or waiting[0]["id"] != key
                or used + entry["slots"] > body["slots"]
            ):
                return False
            entry.update(
                state="running", owner=owner, leaseUntil=now + LEASE_SECONDS
            )
            return copy.deepcopy(entry)

        started = time.monotonic()
        entry = await self._change(pool, claim, backend=backend, strict=True)
        if not entry:
            return None
        ticket = Ticket(pool, key, owner, backend, started + LEASE_SECONDS)
        ticket.payload = entry["payload"]
        self.held[(pool, key)] = ticket
        return ticket

    async def finish(
        self, ticket: Optional[Ticket], state="finished", reason=None
    ):
        if ticket is None:
            return

        def finish(body, now):
            entry = body["entries"].get(ticket.key)
            if entry is not None and entry.get("owner") == ticket.owner:
                entry.update(
                    state=state, reason=reason, finishedAt=now, owner=None
                )
            return None

        try:
            await self._change(ticket.pool, finish, backend=ticket.backend)
        except Exception:
            logger.exception("pool completion could not be recorded")
            # Retry the completion on the next heartbeat.
            ticket.running = None
            ticket.completion = (state, reason)
            return
        self.held.pop((ticket.pool, ticket.key), None)
        self._wake.set()

    async def cancel(self, pool, key, reason="cancelled by operator"):
        def cancel(body, now):
            entry = body["entries"].get(key)
            if entry is None:
                raise PoolError("queue entry not found")
            if entry["state"] == "cancelled":
                return entry
            if entry["state"] != "queued":
                raise PoolError("only waiting entries can be cancelled")
            entry.update(
                state="cancelled",
                reason=reason,
                finishedAt=now,
            )
            return entry

        return await self._change(pool, cancel)

    async def snapshot(self):
        result = []
        for name, conf in self.cron.pool_config.items():
            body = await self._change(
                name, lambda body, now: copy.deepcopy(body)
            )
            entries = list(body["entries"].values())
            waiting = _waiting(body)
            positions = {e["id"]: i + 1 for i, e in enumerate(waiting)}
            free = body["slots"] - sum(
                e["slots"] for e in entries if e["state"] == "running"
            )
            visible = []
            for entry in entries:
                item = {
                    k: v
                    for k, v in entry.items()
                    if k not in ("owner", "payload")
                }
                if entry["state"] == "queued":
                    item["position"] = positions[entry["id"]]
                    item["waitingReason"] = (
                        "Waiting behind higher-priority or earlier work"
                        if item["position"] > 1
                        else f"Needs {entry['slots']} slots; {free} available"
                        if entry["slots"] > free
                        else entry.get("reason") or "Waiting for admission"
                    )
                visible.append(item)
            result.append(
                {
                    "name": name,
                    **conf,
                    "slots": body["slots"],
                    "configuredSlots": conf["slots"],
                    "used": sum(
                        e["slots"] for e in entries if e["state"] == "running"
                    ),
                    "queued": sum(e["state"] == "queued" for e in entries),
                    "entries": sorted(
                        visible,
                        key=lambda e: (
                            0
                            if e["state"] == "running"
                            else 1
                            if e["state"] == "queued"
                            else 2,
                            e.get("position", e["queuedAt"]),
                            e["id"],
                        ),
                    ),
                }
            )
        return result

    async def _renew(self, ticket):
        completion = ticket.completion
        if completion is not None:
            await self.finish(ticket, *completion)
            return
        if not ticket.valid:
            return

        def renew(body, now):
            entry = body["entries"].get(ticket.key)
            if (
                entry is None
                or entry["state"] != "running"
                or entry.get("owner") != ticket.owner
            ):
                return False
            entry["leaseUntil"] = now + LEASE_SECONDS
            return True

        try:
            started = time.monotonic()
            renewed = await self._change(
                ticket.pool, renew, backend=ticket.backend
            )
        except Exception:
            renewed = False
        if renewed:
            ticket.deadline = started + LEASE_SECONDS
        else:
            ticket.valid = False
            if ticket.running is not None:
                logger.error(
                    "pool lease lost for %s; cancelling its process",
                    ticket.key,
                )
                await ticket.running.cancel()

    async def tick(self):
        await asyncio.gather(
            *(self._renew(t) for t in list(self.held.values()))
        )
        if self.cron._stop_event.is_set() or self.cron.state_backend is None:
            return
        for pool in self.cron.pool_config:
            try:
                await self._tick_pool(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("pool %s could not dispatch; retrying", pool)

    async def _tick_pool(self, pool):
        await self._flush_retry_settlements(pool)
        body = await self._change(pool, lambda body, now: copy.deepcopy(body))
        if await self._retire_tasks(pool, body):
            body = await self._change(
                pool, lambda body, now: copy.deepcopy(body)
            )
        for entry in _waiting(body)[:32]:
            payload = entry["payload"]
            if payload.get("kind") != "job":
                continue
            target = payload.get("targetHost")
            if target is not None and target != self.cron._state_host:
                continue
            job = self.cron.cron_jobs.get(entry["job"])
            if (
                job is None
                or not job.enabled
                or job.pool != pool
                or job_digest_cached(job) != entry["digest"]
            ):
                await self.cancel(
                    pool,
                    entry["id"],
                    "job removed, disabled, or configuration changed",
                )
                continue
            if not payload.get("manual") and not self.cron._cluster_allows(
                job
            ):
                continue
            if not payload.get("manual") and self.cron._pause_active(job.name):
                continue
            if (
                job.concurrencyPolicy == "Forbid"
                and self.cron.running_jobs.get(job.name)
            ):
                continue
            ticket = await self.acquire(pool, entry["id"])
            if ticket is None:
                continue
            try:
                launched = await self.cron.maybe_launch_job(
                    job,
                    with_retries=payload.get("withRetries", True),
                    pool_ticket=ticket,
                )
                if not launched:
                    await self.finish(
                        ticket, "queued", "waiting for concurrency admission"
                    )
            except BaseException:
                await self.finish(ticket, "queued", "launch interrupted")
                raise

    async def _retire_tasks(self, pool, body):
        """Retire orphaned claims, including failures nobody can observe.

        Read each run once, with a bounded scan. Never infer absence from
        an unavailable store. Terminal task states cannot become live again
        under the same run ID and attempt.
        """
        from cronstable import dag

        candidates = [
            e for e in _waiting(body) if e["payload"].get("kind") == "task"
        ][:32] + [e for e in body["entries"].values() if _unobserved_task(e)][
            :32
        ]
        runs = {}
        retired = set()
        for entry in candidates:
            payload = entry["payload"]
            if payload.get("kind") != "task":
                continue
            ref = (payload["dag"], payload["runKey"])
            if ref not in runs:
                runs[ref] = await self.cron._dag._read(*ref)
            run = runs[ref]
            task = (run or {}).get("tasks", {}).get(payload["task"])
            if (
                run is None
                or task is None
                or dag.is_terminal_run(run)
                or task["state"] in dag.TERMINAL_STATES
                or (
                    payload.get("runId") is not None
                    and payload["runId"] != run["runId"]
                )
                or (
                    payload.get("attempt") is not None
                    and payload["attempt"] != task.get("attempt", 0)
                )
                or (
                    payload.get("poke") is not None
                    and payload["poke"] != task.get("pokeCount", 0)
                )
            ):
                retired.add(entry["id"])
        if not retired:
            return False

        def retire(body, now):
            for key in retired:
                entry = body["entries"].get(key)
                if entry is None or entry["state"] == "running":
                    continue
                if entry["state"] == "queued":
                    entry.update(
                        state="cancelled",
                        reason="task is no longer waiting",
                        finishedAt=now,
                    )
                entry["observed"] = True
            return True

        return await self._change(pool, retire)

    async def wait_finished(self, pool, key):
        """Wait for this receipt, not for an instant of local idleness."""
        while not self.cron._stop_event.is_set():
            entry = await self._change(
                pool, lambda body, now: body["entries"].get(key)
            )
            if entry is None or entry["state"] in TERMINAL:
                return entry is not None and entry["state"] == "finished"
            try:
                await asyncio.wait_for(self.cron._stop_event.wait(), 0.25)
            except asyncio.TimeoutError:
                pass
        return False

    async def admit_task(self, job, ref, run_id, intent):
        raw = repr(
            (ref, run_id, intent.taskkey, intent.attempt, intent.poke_number)
        )
        key = "dag-" + hashlib.sha256(raw.encode()).hexdigest()
        entry = await self.enqueue(
            job,
            key=key,
            payload={
                "kind": "task",
                "dag": ref[0],
                "runKey": ref[1],
                "task": intent.taskkey,
                "runId": run_id,
                "attempt": intent.attempt,
                "poke": intent.poke_number,
            },
        )
        if entry["state"] in TERMINAL:
            raise PoolError(
                entry.get("reason") or "queue entry is closed",
                pool=job.pool,
                key=key,
            )
        return await self.acquire(job.pool, key), entry

    async def acknowledge_task(self, error: PoolError) -> None:
        if error.pool is None or error.key is None:
            return

        def acknowledge(body, now):
            entry = body["entries"].get(error.key)
            if entry is not None:
                entry["observed"] = True
            return None

        await self._change(error.pool, acknowledge)
