"""Durable admission queues shared by jobs and DAG tasks."""

import asyncio
import copy
import hashlib
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

    async def _change(
        self, pool, action, *, backend=None, strict=False, admission=False
    ):
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
                if any(
                    e["state"] not in TERMINAL
                    for e in body["entries"].values()
                ):
                    if admission:
                        raise PoolError(
                            "pool is draining before a capacity change"
                        )
                else:
                    body["slots"] = conf["slots"]
            result = action(body, now)
            return (DOC_KEEP if body == current else body), result

        _, result = await asyncio.wait_for(
            backend.mutate_document(NAMESPACE, pool, transform), OP_TIMEOUT
        )
        return result

    async def enqueue(self, job, *, key=None, payload=None):
        key = key or uuid.uuid4().hex
        conf = self.cron.pool_config.get(job.pool)
        if conf is None:
            raise PoolError("unknown pool {!r}".format(job.pool))

        def add(body, now):
            old = body["entries"].get(key)
            if old is not None:
                return old
            if (
                len(_waiting(body))
                + sum(_unobserved_task(e) for e in body["entries"].values())
                >= conf["maxQueued"]
            ):
                raise PoolError("pool queue is full")
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
                "payload": payload or {},
            }
            if payload and payload.get("kind") == "task":
                entry.update(
                    {k: payload[k] for k in ("dag", "runKey", "task")}
                )
            body["entries"][key] = entry
            return entry

        entry = await self._change(job.pool, add, strict=True, admission=True)
        self.service()
        return entry

    async def acquire(self, pool: str, key: str) -> Optional[Ticket]:
        if (pool, key) in self.held:
            return None
        owner = self.cron._proc_token + ":" + uuid.uuid4().hex
        backend = self.cron.state_backend

        def claim(body, now):
            entry = body["entries"].get(key)
            if entry is None or entry["state"] != "queued":
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
        body = await self._change(pool, lambda body, now: copy.deepcopy(body))
        for entry in _waiting(body)[:32]:
            payload = entry["payload"]
            if payload.get("kind") != "job":
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
