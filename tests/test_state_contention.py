"""Test concurrent processes sharing a filesystem state store.

Each worker runs ``tests/_contention_worker.py`` in its own interpreter
and opens the same store through the package API. A barrier file starts
the workers together. The store's file locks coordinate their operations.
The parent checks worker reports and the resulting files.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from cronstable import platform, pools
from cronstable.backends.filesystem import FilesystemBackend
from cronstable.config import parse_config_string
from tests._crash_helpers import (
    ROOT,
    WAIT,
    open_store,
    source_env,
    wait_for,
)

WORKERS = 6


def _run_workers(tmp_path, mode, *args, workers=WORKERS):
    """Run ``workers`` contenders to completion; return their reports."""
    store = tmp_path / "store"
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    outs = [scratch / "w{}.json".format(i) for i in range(workers)]
    procs = []
    try:
        for out in outs:
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "tests._contention_worker",
                        mode,
                        str(store),
                        str(out),
                        *map(str, args),
                    ],
                    cwd=ROOT,
                    env=source_env(),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
            )

        def all_ready():
            for proc in procs:
                assert proc.poll() is None, proc.communicate()[0].decode()
            return all(os.path.exists(str(out) + ".ready") for out in outs)

        # the barrier opens only once every worker is past its imports
        # and parked on it, so they really do start together.
        wait_for("every worker reaching the barrier", all_ready)
        (scratch / "go").touch()
        for proc in procs:
            output = proc.communicate(timeout=WAIT * 3)[0].decode()
            assert proc.returncode == 0, output
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=30)
            proc.stdout.close()
    return [json.loads(out.read_text(encoding="utf-8")) for out in outs]


def _read(store, fn):
    async def go():
        backend = open_store(store)
        await backend.start()
        return await fn(backend)

    return asyncio.run(go())


def test_mutate_document_loses_no_update_across_processes(tmp_path):
    per_worker = 150
    reports = _run_workers(tmp_path, "documents", per_worker)
    total = WORKERS * per_worker
    body = _read(
        tmp_path / "store",
        lambda b: b.read_document("contention", "counter"),
    )
    assert body["n"] == total
    # stronger than the final count: every value was handed to exactly
    # one writer, so no two transforms ever ran against the same read.
    wrote = sorted(v for r in reports for v in r["wrote"])
    assert wrote == list(range(1, total + 1))
    for report in reports:
        assert report["wrote"] == sorted(report["wrote"])
    # and the contention left no temporary file behind.
    tmp_dir = tmp_path / "store" / "default" / "tmp"
    assert list(tmp_dir.iterdir()) == []


def test_lease_has_one_holder_and_monotonic_fences(tmp_path):
    per_worker = 25
    reports = _run_workers(tmp_path, "leases", per_worker)
    for report in reports:
        assert report["violations"] == []
        assert len(report["fences"]) == per_worker
        # each worker sees its own fences strictly grow.
        assert report["fences"] == sorted(set(report["fences"]))
    total = WORKERS * per_worker
    # every hold released before the next began, so each acquire was a
    # takeover and bumped the fence by exactly one: no value reissued,
    # none skipped.
    fences = sorted(f for r in reports for f in r["fences"])
    assert fences == list(range(1, total + 1))
    # the order log was appended to only while holding: it lists the
    # fences in the order the holds happened, strictly increasing.
    order = [
        int(line.split()[0])
        for line in (tmp_path / "scratch" / "order.log")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert order == list(range(1, total + 1))
    # a released lease reads as "nobody holds it", yet the file stays:
    # it is the fence counter's only home, so the next taker continues
    # the sequence instead of restarting it.
    store = tmp_path / "store"
    assert _read(store, lambda b: b.read_lease("contended")) is None
    lease = _read(store, lambda b: b.acquire_lease("contended", "late", 5.0))
    assert lease.fence == total + 1


def test_pool_tickets_never_exceed_capacity(tmp_path):
    slots, per_worker = 2, 12
    reports = _run_workers(tmp_path, "tickets", per_worker, slots)
    for report in reports:
        assert report["violations"] == []
        assert report["done"] == per_worker
        assert 1 <= report["peak"] <= slots
    body = _read(
        tmp_path / "store",
        lambda b: b.read_document(pools.NAMESPACE, "shared"),
    )
    assert body["slots"] == slots
    states = [e["state"] for e in body["entries"].values()]
    # the document keeps the newest 100 finished entries.
    assert len(states) == min(100, WORKERS * per_worker)
    assert set(states) == {"finished"}
    assert list((tmp_path / "scratch" / "running").iterdir()) == []


# --- the wedged-renew fence -------------------------------------------------

ELECTION = (
    "cluster:\n  backend: filesystem\n  nodeName: {}\n"
    "  filesystem:\n    path: {}\n    ttl: 3\n    topology: shared\n"
)


def _election(store, node):
    cfg = parse_config_string(
        ELECTION.format(node, json.dumps(str(store))), ""
    ).cluster_config
    return FilesystemBackend(cfg, lambda: "v1:jobs")


async def _until(description, predicate, *, invariant=None, timeout=WAIT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if invariant is not None:
            invariant()
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out " + description)


@pytest.mark.skipif(
    platform.IS_WINDOWS, reason="SIGSTOP and flock are POSIX-only"
)
async def test_wedged_lock_holder_demotes_the_leader_without_a_second(
    tmp_path,
):
    # A peer holding the file lock blocks acquisition and renewal while
    # unlocked reads continue. Leadership expires based on the last renewal;
    # reads cannot extend the deadline. Another node must acquire the lock
    # before it can take over the lease.
    store = tmp_path / "store"
    alpha = _election(store, "alpha")
    beta = _election(store, "beta")
    ready = tmp_path / "locked"
    holder = None
    try:
        await alpha.start()
        await _until("alpha winning", alpha.is_leader)
        await beta.start()
        assert not beta.is_leader() and beta.leader_name() == "alpha"
        fence = alpha.lease_detail()["fence"]

        holder = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests._contention_worker",
                "hold-lock",
                str(store),
                str(ready),
                alpha.election_name,
            ],
            cwd=ROOT,
            env=source_env(),
            stdin=subprocess.DEVNULL,
        )
        await _until(
            "the peer taking the flock",
            lambda: holder.poll() is not None or ready.exists(),
        )
        assert holder.poll() is None
        os.kill(holder.pid, signal.SIGSTOP)
        wedged_at = time.monotonic()

        def never_two():
            assert not (alpha.is_leader() and beta.is_leader())

        await _until(
            "alpha lapsing on its own deadline",
            lambda: not alpha.is_leader(),
            invariant=never_two,
        )
        # the deadline is anchored before the last successful renew was
        # sent and nothing since has pushed it out, so it sits inside one
        # ttl of the wedge. Judged on the deadline itself, which is exact,
        # instead of on when this loop got to look.
        assert alpha._lease_deadline_mono <= wedged_at + alpha.ttl
        # the unlocked read still answers with alpha's own token, which
        # is exactly what must not re-grant leadership.
        on_disk = await alpha._store.read_lease(alpha.election_name)
        assert on_disk.holder == alpha._holder_token
        assert on_disk.fence == fence
        frozen_expiry = on_disk.expires_at

        # hold the wedge until the on-disk lease is well past expiry by
        # every margin: beta wants it and still cannot have it.
        await _until(
            "the on-disk lease expiring under the wedge",
            lambda: time.time() > frozen_expiry + 3.0,
            invariant=lambda: (
                never_two(),
                _assert(not alpha.is_leader()),
                _assert(not beta.is_leader()),
            ),
        )
        on_disk = await alpha._store.read_lease(alpha.election_name)
        assert on_disk.expires_at == frozen_expiry
        assert on_disk.fence == fence

        # the peer exits, the kernel releases its lock, and exactly one node
        # comes back as leader through a locked write.
        holder.kill()
        holder.wait(timeout=30)
        await _until(
            "one node regaining leadership",
            lambda: alpha.is_leader() != beta.is_leader(),
            invariant=never_two,
        )
        leader = alpha if alpha.is_leader() else beta
        on_disk = await leader._store.read_lease(leader.election_name)
        assert on_disk.holder == leader._holder_token
        assert on_disk.expires_at > time.time()
        if leader is beta:
            assert on_disk.fence == fence + 1
    finally:
        if holder is not None and holder.poll() is None:
            holder.kill()
            holder.wait(timeout=30)
        await beta.stop()
        await alpha.stop()


def _assert(condition):
    assert condition
