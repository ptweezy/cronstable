"""Run one worker process for ``tests/test_state_contention.py``.

Usage: ``python -m tests._contention_worker MODE STORE OUT ...``.

Each worker opens ``STORE`` through the package API and waits for the ``go``
file beside ``OUT``. The parent creates that file to start the workers
together. Each worker completes its task and writes a JSON result to
``OUT``. The parent test checks the results.
"""

import asyncio
import json
import os
import sys
import time
from types import SimpleNamespace

from cronstable import pools
from cronstable.config import StateConfig, parse_config_string
from cronstable.state import FilesystemStateBackend

#: Generous: the parent holds the barrier until every worker has imported
#: the package, which dominates on a slow runner.
DEADLINE = 240.0

POOL_CONFIG = """
pools:
  shared:
    slots: {slots}
    maxQueued: 512
jobs:
  - name: contender
    command: ignored
    schedule: "@reboot"
    pool: shared
"""


def _backend(store):
    return FilesystemStateBackend(
        StateConfig(
            {"path": store, "topology": "single-node", "deploymentId": None}
        ),
        lambda: "contention",
    )


def _await_barrier(out):
    open(out + ".ready", "w").close()
    go = os.path.join(os.path.dirname(out), "go")
    deadline = time.monotonic() + DEADLINE
    while not os.path.exists(go):
        if time.monotonic() > deadline:
            raise SystemExit("barrier never opened")
        time.sleep(0.005)


def _excl(path):
    """Create ``path`` exclusively; False when it already exists."""
    try:
        os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
    except FileExistsError:
        return False
    return True


async def _documents(backend, worker, count):
    """``count`` read-modify-write increments of one shared counter."""
    wrote = []

    def bump(current):
        value = (current or {"n": 0})["n"] + 1
        return {"n": value, "by": worker}, value

    for _ in range(count):
        _body, value = await backend.mutate_document(
            "contention", "counter", bump
        )
        wrote.append(value)
    return {"wrote": wrote}


async def _leases(backend, worker, count, scratch):
    """Acquire the lease ``count`` times, creating a witness file each time.

    Create the witness with ``O_EXCL`` so concurrent holders cause creation
    to fail. Append to ``order`` only while holding the lease to record
    fencing tokens in acquisition order.
    """
    witness = os.path.join(scratch, "holder.witness")
    order = os.path.join(scratch, "order.log")
    fences = []
    violations = []
    deadline = time.monotonic() + DEADLINE
    while len(fences) < count and time.monotonic() < deadline:
        lease = await backend.acquire_lease("contended", worker, 30.0)
        if lease is None:
            await asyncio.sleep(0.001)
            continue
        if not _excl(witness):
            violations.append("fence {}: a holder exists".format(lease.fence))
        else:
            with open(order, "a", encoding="utf-8") as fobj:
                fobj.write("{} {}\n".format(lease.fence, worker))
            # still ours after the work: the renew path contends too.
            renewed = await backend.renew_lease(lease, 30.0)
            if renewed is None or renewed.fence != lease.fence:
                violations.append(
                    "fence {}: lost mid-hold".format(lease.fence)
                )
            os.unlink(witness)
        fences.append(lease.fence)
        await backend.release_lease(lease)
    return {"fences": fences, "violations": violations}


async def _tickets(backend, worker, count, scratch, slots):
    """Queue ``count`` pool entries and run each under its ticket.

    Each running ticket has a file in ``running/``. The number of files must
    never exceed the pool's capacity.
    """
    config = parse_config_string(POOL_CONFIG.format(slots=slots), "")
    (job,) = config.jobs

    async def fidelity():
        return None

    cron = SimpleNamespace(
        state_backend=backend,
        pool_config=config.pools,
        _proc_token=worker,
        _state_host="host-" + worker,
        _slot_fidelity_reason=fidelity,
    )
    scheduler = pools.PoolScheduler(cron)
    # the scheduler's own service tasks launch jobs through a real Cron;
    # here the worker is the launcher.
    scheduler.service = lambda: None
    running = os.path.join(scratch, "running")
    os.makedirs(running, exist_ok=True)
    keys = []
    for index in range(count):
        entry = await scheduler.enqueue(
            job, key="{}-{:03d}".format(worker, index)
        )
        keys.append(entry["id"])
    peak = 0
    violations = []
    done = 0
    deadline = time.monotonic() + DEADLINE
    while keys and time.monotonic() < deadline:
        ticket = await scheduler.acquire("shared", keys[0])
        if ticket is None:
            await asyncio.sleep(0.001)
            continue
        mark = os.path.join(running, keys[0])
        open(mark, "w").close()
        seen = len(os.listdir(running))
        peak = max(peak, seen)
        if seen > slots:
            violations.append("{} tickets running".format(seen))
        os.unlink(mark)
        await scheduler.finish(ticket)
        keys.pop(0)
        done += 1
    return {"done": done, "peak": peak, "violations": violations}


async def _hold_lock(backend, name, out):
    """Hold a lease's file lock until the process is terminated."""
    lock_path, _lease_path = backend._lease_paths(name)
    with backend._locked(lock_path):
        open(out, "w").close()
        while True:
            time.sleep(3600)


async def main(argv):
    mode, store, out = argv[:3]
    backend = _backend(store)
    await backend.start()
    if mode == "hold-lock":
        await _hold_lock(backend, argv[3], out)
    worker = os.path.basename(out).split(".")[0]
    count = int(argv[3])
    scratch = os.path.dirname(out)
    _await_barrier(out)
    if mode == "documents":
        result = await _documents(backend, worker, count)
    elif mode == "leases":
        result = await _leases(backend, worker, count, scratch)
    elif mode == "tickets":
        # This process stress-tests mutual exclusion, not operation latency.
        # Six writers plus coverage can starve one Windows file-lock waiter
        # beyond the daemon's five-second limit. Bound the WHOLE workload
        # instead, without weakening capacity assertions or production limits.
        pools.OP_TIMEOUT = DEADLINE
        result = await asyncio.wait_for(
            _tickets(backend, worker, count, scratch, int(argv[4])), DEADLINE
        )
    else:
        raise SystemExit("unknown mode " + mode)
    with open(out + ".tmp", "w", encoding="utf-8") as fobj:
        json.dump(result, fobj)
    os.replace(out + ".tmp", out)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
