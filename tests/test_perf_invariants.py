"""Count and ordering invariants extracted from the benchmark-suite reviews.

Five separate benchmark candidates across the 2026-07 investigations turned
out to be COUNT or ORDERING invariants, not timings: the fsync barrier
protocol, the once-per-batch process-table walk, the 413-before-fetch
artifact contract, the per-run durable-write count, and artifact prune
residency.  A perf gate is the wrong tool for those -- it measures a proxy
(elapsed time) that is platform-dependent, noisy, and blind in one
direction -- while a test gates the invariant itself in BOTH directions, on
every platform, on every matrix Python, in milliseconds.  This file began
as those five tests and takes later carve-outs of the same shape (the
lease-write durability split); benchmarks/README.md's waiver section
points here.
"""

import asyncio
import datetime
import functools
import glob
import os
import random
import re
import subprocess
import sys
import textwrap
import time
import types

import pytest

import cronstable.pools as pools_mod
import cronstable.state as state_mod
from cronstable import dag, dagrun, jobstate, qr, tui
from cronstable.cron import Cron
from cronstable.jobstate import JobStateError
from tests._commands import cmd_print, yaml_command
from tests._helpers import (
    _backend,
    _drain_pending,
    _drain_state_writes,
    _reap_running,
    _state_cfg,
    start_state,
)
from tests.conftest import Req


def test_idle_mirror_releases_completed_output(monkeypatch):
    import gc
    import weakref

    from cronstable.job import StreamReader, _MirrorWriter

    class Text(str):
        pass

    monkeypatch.setattr(StreamReader, "_emit", lambda *args: None)
    writer = _MirrorWriter()
    payload = Text("x" * 1048576)
    ref = weakref.ref(payload)
    writer.submit("job", "stdout", payload)
    del payload
    assert writer.drain(5.0)
    gc.collect()
    assert ref() is None, "idle mirror retained a completed output batch"


def test_idle_eventlog_writer_releases_completed_record(monkeypatch):
    import gc
    import weakref

    from cronstable.job import _EventLogWriter

    class Text(str):
        pass

    monkeypatch.setattr(_EventLogWriter, "_write", lambda *args: None)
    writer = _EventLogWriter("memory-probe")
    try:
        payload = Text("x" * 32768)
        ref = weakref.ref(payload)
        assert writer.submit((1, 0, 1, [payload]))
        del payload
        writer._queue.join()
        gc.collect()
        assert ref() is None, "idle Event Log writer retained its last record"
    finally:
        writer.stop()
        writer.join(5.0)


def test_dashboard_caches_wire_bytes_without_decoding(monkeypatch):
    import hashlib
    import zlib

    from cronstable import cron

    raw = "<!doctype html><p>cronstable \U0001f550</p>".encode()
    reads = []

    class Page:
        def joinpath(self, name):
            assert name == "index.html"
            return self

        def read_bytes(self):
            reads.append(1)
            return raw

    cron._index_document.cache_clear()
    cron._index_gzip.cache_clear()
    monkeypatch.setattr(cron.importlib.resources, "files", lambda _: Page())
    try:
        body, etag = cron._index_document()
        assert body == raw
        assert etag == '"' + hashlib.sha256(raw).hexdigest()[:32] + '"'
        assert zlib.decompress(cron._index_gzip(), wbits=31) == raw
        assert cron.load_index_html() == raw.decode()
        assert cron._index_document()[0] is body
        assert reads == [1]
    finally:
        cron._index_document.cache_clear()
        cron._index_gzip.cache_clear()


async def test_job_queues_scan_each_pool_once():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    class Entries(list):
        scans = 0

        def __iter__(self):
            self.scans += 1
            return super().__iter__()

    def entry(key, job="job0", state="queued", **extra):
        return {"id": key, "job": job, "state": state, **extra}

    shared = Entries(
        [
            entry("first"),
            entry("running", state="running"),
            entry("task", task="extract"),
            entry("second", task=None),
            entry("other-job", job="job1"),
            entry("done", state="finished"),
        ]
    )
    other = Entries([entry("other-pool"), entry("third", job="job2")])
    cron = SimpleNamespace(
        pool_config={"shared": {}, "other": {}},
        _pools=SimpleNamespace(
            snapshot=AsyncMock(
                return_value=[
                    {"name": "shared", "entries": shared},
                    {"name": "other", "entries": other},
                ]
            )
        ),
    )
    jobs = [
        {"name": f"job{i}", "pool": {"name": "shared"}} for i in range(25)
    ] + [
        {"name": "job2", "pool": {"name": "other"}},
        {"name": "missing", "pool": {"name": "missing"}},
        {"name": "unpooled"},
    ]
    await Cron._attach_job_queues(cron, jobs)
    assert [e["id"] for e in jobs[0]["pool"]["queued"]] == ["first", "second"]
    assert [e["id"] for e in jobs[1]["pool"]["queued"]] == ["other-job"]
    assert jobs[2]["pool"]["queued"] == []
    assert [e["id"] for e in jobs[-3]["pool"]["queued"]] == ["third"]
    assert jobs[-2]["pool"]["queued"] == []
    assert "pool" not in jobs[-1]
    assert shared.scans == other.scans == 1
    cron._pools.snapshot.assert_awaited_once()


# --- 1. the fsync barrier protocol ----------------------------------------
#
# Each appended record must be made durable by exactly one file fsync AND
# exactly one directory barrier (the rename's directory entry needs its own
# flush, or a power loss can drop a perfectly-fsynced file).  Counting the
# BARRIER CALL rather than its platform implementation makes the test equal
# on POSIX (where the barrier is an os.fsync of the directory) and Windows
# (FlushFileBuffers via ctypes, where an os.fsync count is blind to a lost
# directory barrier -- the dangerous direction).  Exact equality gates both
# ways: a dropped barrier AND a superlinear re-sync regression.


async def test_append_pays_one_file_fsync_and_one_dir_barrier(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        # create the stream first: the first append also pays the durable
        # mkdir of the stream directory, which is its own (once-only) cost,
        # not part of the steady-state protocol under test.
        await backend.append_record("runs", {"seq": -1})

        file_fsyncs = []
        dir_barriers = []
        real_fsync = os.fsync

        def counting_fsync(fd):
            file_fsyncs.append(fd)
            return real_fsync(fd)

        def counting_barrier(path):
            dir_barriers.append(path)
            # deliberately not called through: the count IS the contract,
            # and skipping the real flush keeps the test fast on slow disks.

        monkeypatch.setattr(os, "fsync", counting_fsync)
        monkeypatch.setattr(state_mod, "fsync_directory", counting_barrier)

        n = 25
        for i in range(n):
            await backend.append_record("runs", {"seq": i})

        assert len(file_fsyncs) == n, (
            "each append must fsync its record file exactly once"
        )
        assert len(dir_barriers) == n, (
            "each append must flush its directory entry exactly once; a "
            "lost barrier means a crash can silently drop durable records"
        )
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- 2. one process-table walk per sample batch ---------------------------
#
# K concurrently monitored runs must cost ONE table snapshot per due tick,
# not K: the shared ticker indexes psutil's _ppid_map once and every due
# monitor folds its own tree from that index.


def test_sample_batch_walks_the_process_table_once(monkeypatch):
    psutil = pytest.importorskip("psutil")
    from cronstable import resources

    walks = []
    real_map = psutil._ppid_map

    def counting_map():
        walks.append(1)
        return real_map()

    monkeypatch.setattr(psutil, "_ppid_map", counting_map)

    class _StubMonitor:
        def __init__(self):
            self.samples = []

        def _sample(self, index):
            self.samples.append(index)

    for k in (1, 5, 12):
        walks.clear()
        monitors = [_StubMonitor() for _ in range(k)]
        resources._SharedSampleTicker._sample_batch(monitors)
        assert len(walks) == 1, (
            "a batch of %d monitors walked the table %d times; the shared "
            "ticker must snapshot once per due tick regardless of K"
            % (k, len(walks))
        )
        for monitor in monitors:
            assert len(monitor.samples) == 1


# --- 3. 413 before fetch --------------------------------------------------
#
# An artifact over the caller's byte budget must be refused from its RECORD
# metadata, before the payload blob is ever fetched, so an oversized
# artifact can never enter daemon memory on the read path.


async def test_oversized_artifact_413s_before_the_blob_is_fetched(tmp_path):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        payload = b"x" * 4096
        await jobstate.artifact_put(backend, "scope", "big", payload)

        fetches = []
        real_get_blob = backend.get_blob

        async def spying_get_blob(digest):
            fetches.append(digest)
            return await real_get_blob(digest)

        backend.get_blob = spying_get_blob  # type: ignore[method-assign]

        with pytest.raises(JobStateError) as err:
            await jobstate.artifact_get(
                backend, "scope", "big", max_bytes=1024
            )
        assert err.value.status == 413
        assert fetches == [], (
            "the oversized artifact's blob was fetched before the 413; the "
            "cap must be enforced from the record's stored size"
        )

        # the healthy direction: under budget, exactly one blob fetch.
        result = await jobstate.artifact_get(
            backend, "scope", "big", max_bytes=65536
        )
        assert result is not None and result[1] == payload
        assert len(fetches) == 1
    finally:
        await backend.stop()


# --- 4. the per-run durable-write count -----------------------------------
#
# One completed scheduled run writes a FIXED set of durable records: the
# inflight open, the finished-run ledger record, the inflight close, plus
# (for the FIRST persist in any COUNTER_SNAPSHOT_INTERVAL window) one
# durable counter snapshot.  A regression that adds per-run writes (they
# compound per job per fire, forever) or drops one (a close left open reads
# as a phantom interrupted run on the next boot) changes the count.  The
# open must precede the close within the inflight stream; cross-stream
# ordering rides worker-lane scheduling and is deliberately not pinned.


# This is the one test in the file that actually LAUNCHES its job and
# asserts the outcome, so the command has to succeed on every platform the
# suite runs on.  A bare `ls` does not: cmd.exe has no such binary, and a
# Windows shell without Git's usr\bin on PATH ran it to 'failure' and broke
# the ledger assertion below.  tests._commands runs the test interpreter
# instead (see its module docstring).
_RUN_JOB = (
    "jobs:\n  - name: j\n"
    + yaml_command(cmd_print())
    + '\n    schedule: "0 0 * * *"\n'
)


async def test_one_run_writes_open_record_close_and_nothing_else(tmp_path):
    cron = Cron(None, config_yaml=_RUN_JOB)
    cfg = _state_cfg(
        "state:\n  path: %s\n  jobApi:\n    enabled: false\n" % tmp_path
    )
    await start_state(cron, cfg)
    assert cron.state_backend is not None
    try:
        backend = cron.state_backend
        events = []
        real_append = backend.append_record

        async def spying_append(stream, data, **kwargs):
            events.append((stream, data.get("kind") or data.get("outcome")))
            return await real_append(stream, data, **kwargs)

        backend.append_record = spying_append  # type: ignore[method-assign]

        # the reaper's own two steps, driven directly (the run loop's
        # _wait_for_running_jobs does exactly this per finished job)
        await cron.launch_scheduled_job(cron.cron_jobs["j"])
        running = cron.running_jobs["j"][0]
        await running.wait()
        await cron._handle_finished_job(running)
        await _drain_state_writes(cron)

        inflight = [kind for stream, kind in events if stream == "inflight/j"]
        runs = [kind for stream, kind in events if stream == "runs/j"]
        counters = [
            stream
            for stream, _kind in events
            if stream.startswith("counters/")
        ]
        assert inflight == ["open", "closed"], (
            "the inflight stream must see exactly open then closed; got %r"
            % (events,)
        )
        assert runs == ["success"], (
            "exactly one ledger record per run; got %r" % (events,)
        )
        assert len(counters) == 1, (
            "the first persist in a snapshot window carries exactly one "
            "durable counter snapshot; got %r" % (events,)
        )
        assert len(events) == 4, (
            "a completed run wrote %d durable records, expected exactly 4 "
            "(open, ledger, close, counter snapshot); every extra write "
            "here compounds per job per fire forever: %r"
            % (len(events), events)
        )
    finally:
        await _drain_state_writes(cron)
        await cron.state_backend.stop()
        cron.state_backend = None


# --- 5. artifact prune residency ------------------------------------------
#
# Publishing under a name supersedes the previous version, and the stream
# must stay bounded by the DISTINCT-NAME count (plus the documented
# amortisation slack), never by the publish count -- while the newest
# version of every name must survive pruning intact.


async def test_artifact_stream_residency_is_bounded_by_distinct_names(
    tmp_path,
):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        names = 4
        puts = 60
        for i in range(puts):
            await jobstate.artifact_put(
                backend,
                "scope",
                "report-%d" % (i % names),
                ("payload-%d" % i).encode(),
            )
        stream = jobstate.ARTIFACT_STREAM_PREFIX + "scope"
        records = await backend.list_records(stream)
        slack = getattr(state_mod, "_PRUNE_EVERY_APPENDS", 8) - 1
        assert len(records) <= names + slack, (
            "%d publishes over %d names left %d records resident; the "
            "prune-by-name bound broke and the store grows with publish "
            "count" % (puts, names, len(records))
        )
        # over-pruning is the other direction: every name's NEWEST version
        # must read back intact.
        listing = await jobstate.artifact_list(backend, "scope")
        assert sorted(rec["name"] for rec in listing) == [
            "report-%d" % i for i in range(names)
        ]
        for i in range(names):
            result = await jobstate.artifact_get(
                backend, "scope", "report-%d" % i
            )
            assert result is not None
            last_version = puts - names + i
            assert result[1] == ("payload-%d" % last_version).encode()
    finally:
        await backend.stop()


# --- 6. the lease-write durability split -----------------------------------
#
# Every lease write used to pay the full append barrier (temp fsync + rename
# + directory flush) although elections renew every ttl/3 and every held
# cluster slot / DAG advance lease renews every ~10s: tens of thousands of
# barriers a day on an idle HA pair, buying durability for a value whose
# loss is harmless.  The split is a count invariant with a safety edge in
# each direction: a same-fence write (renew, release, same-holder valid
# re-acquire) must NOT pay the directory barrier, and a fence-CHANGING
# write (first issue, takeover) must ALWAYS pay it, or a crash could
# re-issue an acknowledged fence and defeat stale-writer detection.  The
# temp-file fsync stays on every write either way: a lease file that reads
# back truncated after a crash fails every later acquire closed.


async def test_lease_write_barrier_follows_the_fence(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        # warm up: the first lease op pays the leases directory's durable
        # mkdir, a once-only cost outside the protocol under test.
        warm = await backend.acquire_lease("warm", "holder-a", ttl=30.0)
        assert warm is not None

        file_fsyncs = []
        dir_barriers = []
        real_fsync = os.fsync

        def counting_fsync(fd):
            file_fsyncs.append(fd)
            return real_fsync(fd)

        def counting_barrier(path):
            dir_barriers.append(path)
            # deliberately not called through, same as the append test: the
            # count IS the contract.

        monkeypatch.setattr(os, "fsync", counting_fsync)
        monkeypatch.setattr(state_mod, "fsync_directory", counting_barrier)

        # first issue of a new lease name: fence 1 is born, barrier required
        lease = await backend.acquire_lease("slot", "holder-a", ttl=30.0)
        assert lease is not None and lease.fence == 1
        assert len(file_fsyncs) == 1
        assert len(dir_barriers) == 1, (
            "a fence-issuing acquire must flush the rename; losing it to a "
            "crash would re-issue the fence to the next acquirer"
        )

        # steady state: renews keep the fence and must skip the barrier
        # (the file fsync stays: no write may leave a truncatable lease).
        for i in range(2, 12):
            lease = await backend.renew_lease(lease, ttl=30.0)
            assert lease is not None
            assert len(file_fsyncs) == i
        assert len(dir_barriers) == 1, (
            "a same-fence renew must not pay the directory barrier; this "
            "is the ~10s heartbeat write of every election, cluster slot "
            "and DAG advance lease"
        )

        # a same-holder still-valid acquire is a renew in acquire clothing
        again = await backend.acquire_lease("slot", "holder-a", ttl=30.0)
        assert again is not None and again.fence == lease.fence
        assert len(dir_barriers) == 1

        # release keeps the fence (expiry-in-place): no barrier either
        await backend.release_lease(again)
        assert len(dir_barriers) == 1

        # takeover of the released lease bumps the fence: barrier required
        taken = await backend.acquire_lease("slot", "holder-b", ttl=30.0)
        assert taken is not None and taken.fence == again.fence + 1
        assert len(dir_barriers) == 2, (
            "a fence-bumping takeover must flush the rename, exactly like "
            "first issue"
        )
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- 7. the strictyaml Seq attribute-copy count ----------------------------
#
# strictyaml validates a sequence by deep-copying the ruamel document, and
# its vendored CommentedSeq.__deepcopy__ calls copy_attributes from INSIDE
# the element loop, so an N-element sequence re-copies the sequence's
# whole attribute set N times and config parsing comes out quadratic in the
# job count.  cronstable rebinds the method to hoist that call out of the
# loop (config._patch_strictyaml_seq_deepcopy).  The invariant is a COUNT,
# not a timing: one copy_attributes call per deepcopy no matter how long
# the sequence is.  Counting it gates both directions (a dropped shim and
# a future re-quadratic regression) in microseconds, on every platform,
# where a wall-clock assertion would be noisy and one-directional.


def _counting_seq_class():
    """A CommentedSeq subclass that tallies its own copy_attributes calls."""
    from strictyaml.ruamel.comments import CommentedSeq

    class Counting(CommentedSeq):
        calls = 0

        def copy_attributes(self, t, memo=None):
            Counting.calls += 1
            super().copy_attributes(t, memo=memo)

    return Counting


@pytest.mark.parametrize("length", [1, 2, 8, 64])
def test_seq_deepcopy_copies_attributes_once_per_copy(length):
    import copy as copy_mod

    counting = _counting_seq_class()
    copy_mod.deepcopy(counting(list(range(length))))
    assert counting.calls == 1, (
        "CommentedSeq.__deepcopy__ must copy the sequence's attribute set "
        "once per copy, not once per element: at %d elements it ran %d "
        "times, which is the quadratic config parse "
        "config._patch_strictyaml_seq_deepcopy exists to remove"
        % (length, counting.calls)
    )


def test_seq_deepcopy_leaves_an_empty_sequence_alone():
    # Deliberate carve-out: upstream never reaches the in-loop call for an
    # empty sequence, so the hoisted version must not start copying
    # attributes that the stock implementation left unset.  Keeping this
    # asymmetry is what makes the rebind a pure cost change.
    import copy as copy_mod

    counting = _counting_seq_class()
    copy_mod.deepcopy(counting([]))
    assert counting.calls == 0


def _deep_repr(obj, depth=0):
    """Structural, address-free rendering of a parsed config."""
    if depth > 12:
        return "..."
    if isinstance(obj, (str, int, float, bool, type(None))):
        return repr(obj)
    if isinstance(obj, dict):
        return "{%s}" % ",".join(
            "%s:%s" % (_deep_repr(k, depth + 1), _deep_repr(v, depth + 1))
            for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))
        )
    if isinstance(obj, (list, tuple)):
        return "[%s]" % ",".join(_deep_repr(x, depth + 1) for x in obj)
    slots = getattr(type(obj), "__slots__", None)
    if slots:
        return "%s(%s)" % (
            type(obj).__name__,
            ",".join(
                "%s=%s" % (s, _deep_repr(getattr(obj, s, None), depth + 1))
                for s in sorted(slots)
            ),
        )
    if hasattr(obj, "__dict__"):
        return "%s(%s)" % (
            type(obj).__name__,
            ",".join(
                "%s=%s" % (k, _deep_repr(v, depth + 1))
                for k, v in sorted(obj.__dict__.items())
            ),
        )
    return repr(obj)


def test_hoisted_seq_deepcopy_parses_identically_to_the_stock_one():
    # The count invariants above gate the cost; this one gates the meaning.
    # Parsing the same text under the stock (in-loop) implementation and the
    # hoisted one must produce indistinguishable configs: the rebind is a
    # pure cost change, so nothing a caller can observe may move.
    import copy as copy_mod
    import dataclasses

    from strictyaml.ruamel.comments import CommentedSeq

    from cronstable.config import parse_config_string

    def stock(self, memo):  # verbatim upstream: the call sits in the loop
        res = self.__class__()
        memo[id(self)] = res
        for k in self:
            res.append(copy_mod.deepcopy(k, memo))
            self.copy_attributes(res, memo=memo)
        return res

    text = textwrap.dedent(
        """\
        defaults:
          captureStderr: true
        jobs:
          # a comment inside the sequence
          - name: alpha
            command: echo alpha
            schedule: '*/5 * * * *'
            environment:
              - key: A
                value: '1'
          - name: beta
            command: echo beta
            schedule: '0 1 * * *'
            captureStdout: true
        """
    )

    patched = CommentedSeq.__deepcopy__
    try:
        CommentedSeq.__deepcopy__ = stock
        expected = parse_config_string(text, "test")
        CommentedSeq.__deepcopy__ = patched
        actual = parse_config_string(text, "test")
    finally:
        CommentedSeq.__deepcopy__ = patched

    for field in dataclasses.fields(expected):
        assert _deep_repr(getattr(actual, field.name)) == _deep_repr(
            getattr(expected, field.name)
        ), field.name


def _stock_pointer_methods():
    """strictyaml's YAMLPointer navigation: upstream's bodies without their
    type assertions, so each fork goes through the deepcopy the config shim
    removes."""
    import copy as copy_mod

    def stock_val(self, regularkey, strictkey):
        new_location = copy_mod.deepcopy(self)
        new_location._indices.append(("val", (regularkey, strictkey)))
        return new_location

    def stock_key(self, regularkey, strictkey):
        new_location = copy_mod.deepcopy(self)
        new_location._indices.append(("key", (regularkey, strictkey)))
        return new_location

    def stock_index(self, index):
        new_location = copy_mod.deepcopy(self)
        new_location._indices.append(("index", index))
        return new_location

    def stock_textslice(self, start, end):
        new_location = copy_mod.deepcopy(self)
        new_location._indices.append(("textslice", (start, end)))
        return new_location

    def stock_parent(self):
        new_location = copy_mod.deepcopy(self)
        new_location._indices = new_location._indices[:-1]
        return new_location

    return {
        "val": stock_val,
        "key": stock_key,
        "index": stock_index,
        "textslice": stock_textslice,
        "parent": stock_parent,
    }


def test_forked_pointer_parses_identically_to_the_stock_one():
    # The twin of the test above for config._patch_strictyaml_pointer_copy.
    # A document parsed under upstream's pointer methods and under the
    # forked ones must give indistinguishable configs, and an invalid
    # document the same error: the error path slices the offending chunk
    # out through the very pointers being forked, so a wrong pointer shows
    # up there first, as a mislocated or blank snippet.
    import dataclasses

    from strictyaml.yamlpointer import YAMLPointer

    from cronstable.config import ConfigError, parse_config_string

    stock = _stock_pointer_methods()
    forked = {name: getattr(YAMLPointer, name) for name in stock}
    # the shim must actually be installed, or this compares stock to stock
    assert all(
        "deepcopy" not in fn.__code__.co_names for fn in forked.values()
    ), "config._patch_strictyaml_pointer_copy did not rebind YAMLPointer"

    good = textwrap.dedent(
        """\
        defaults:
          captureStderr: true
        jobs:
          # a comment inside the sequence
          - name: alpha
            command: echo alpha
            schedule: '*/5 * * * *'
            environment:
              - key: A
                value: '1'
          - name: beta
            command: echo beta
            schedule: '0 1 * * *'
            captureStdout: true
        """
    )
    bad = [
        # a scalar the schema rejects, deep inside the jobs sequence
        textwrap.dedent(
            """\
            jobs:
              - name: alpha
                command: echo alpha
                schedule: '*/5 * * * *'
              - name: beta
                command: echo beta
                schedule: '0 1 * * *'
                captureStdout: sometimes
            """
        ),
        # a key the schema does not know
        textwrap.dedent(
            """\
            jobs:
              - name: alpha
                command: echo alpha
                schedule: '*/5 * * * *'
                bogus: 1
            """
        ),
    ]

    def parse_under(methods):
        for name, method in methods.items():
            setattr(YAMLPointer, name, method)
        parsed = parse_config_string(good, "test")
        errors = []
        for text in bad:
            with pytest.raises(ConfigError) as err:
                parse_config_string(text, "test")
            errors.append(str(err.value))
        return parsed, errors

    try:
        expected, expected_errors = parse_under(stock)
        actual, actual_errors = parse_under(forked)
    finally:
        for name, method in forked.items():
            setattr(YAMLPointer, name, method)

    assert actual_errors == expected_errors
    for field in dataclasses.fields(expected):
        assert _deep_repr(getattr(actual, field.name)) == _deep_repr(
            getattr(expected, field.name)
        ), field.name

    # Pointer by pointer, for all five methods: a config parse never
    # reaches textslice (strictyaml's only caller is CommaSeparated, which
    # no cronstable schema uses), so the parse above cannot vouch for it.
    calls = {
        "val": ("jobs", "jobs"),
        "key": ("name", "name"),
        "index": (2,),
        "textslice": (1, 3),
        "parent": (),
    }
    predicates = ("is_val", "is_key", "is_index", "is_textslice")
    for name, args in calls.items():
        source = YAMLPointer()
        source._indices = [("val", ("jobs", "jobs")), ("index", 0)]
        before = list(source._indices)
        expected_ptr = stock[name](source, *args)
        actual_ptr = forked[name](source, *args)
        assert actual_ptr._indices == expected_ptr._indices, name
        assert source._indices == before, name
        assert actual_ptr._indices is not source._indices, name
        for predicate in predicates:
            assert getattr(actual_ptr, predicate)() == getattr(
                expected_ptr, predicate
            )(), (name, predicate)


def test_pointer_shim_stands_down_on_unknown_pointer_state():
    # config._patch_strictyaml_pointer_copy installs its fork only while a
    # pointer's state is exactly `_indices`: a strictyaml whose pointers
    # carry more state would lose it to the fork. Once installed, a second call
    # sees no deepcopy to remove and leaves the methods as they are.
    from strictyaml.yamlpointer import YAMLPointer

    from cronstable import config

    stock = _stock_pointer_methods()
    forked = {name: getattr(YAMLPointer, name) for name in stock}
    stock_init = YAMLPointer.__init__

    def init_with_extra_state(self):
        stock_init(self)
        self._extra = None

    try:
        for name, method in stock.items():
            setattr(YAMLPointer, name, method)
        YAMLPointer.__init__ = init_with_extra_state
        config._patch_strictyaml_pointer_copy()
        for name, method in stock.items():
            assert getattr(YAMLPointer, name) is method, name

        YAMLPointer.__init__ = stock_init
        config._patch_strictyaml_pointer_copy()
        installed = {name: getattr(YAMLPointer, name) for name in stock}
        for name, method in installed.items():
            assert method is not stock[name], name
            assert "deepcopy" not in method.__code__.co_names, name
        config._patch_strictyaml_pointer_copy()
        for name, method in installed.items():
            assert getattr(YAMLPointer, name) is method, name
    finally:
        YAMLPointer.__init__ = stock_init
        for name, method in forked.items():
            setattr(YAMLPointer, name, method)


# --- 8. the lazy import doors stay shut ------------------------------------
#
# cron.py binds `web` and `aiohttp` to a _AiohttpDoor proxy that imports the
# real modules on first attribute access, because every consumer of them (the
# web listener, cluster gossip, the push relay) is optional while the module
# itself is imported by state_admin, the CLIs and the MCP surface.  Opening
# that door costs 144 ms and 14 MB of RSS (measured in CI, run 31170258121),
# so the invariant is a COUNT: importing cronstable.cron loads ZERO aiohttp
# modules.  A test rather than a timing because the failure is binary and
# platform-independent, and because the only two metrics that saw it
# (startup.import_daemon and mem.rss_daemon_import) live in the subprocess
# tier of a Linux-only perf job, which means a branch can carry the
# regression for days.
#
# The regression this gates shipped exactly once: a module-scope
# `@web.middleware` decorator, which reads an attribute off the proxy while
# the module body is still running.  Anything evaluated at import time does
# it: a decorator, a base class, a default argument, a module constant.
#
# discovery.py has the same shape for zeroconf (~24 ms and ~3.6 MB, its own
# docstring) and cron.py imports discovery unconditionally, so it rides the
# same probe for one extra string rather than waiting for its own incident.
#
# Gated in both directions.  The door must still OPEN on first touch, and
# opening it must REBIND the module globals: the proxy's whole reason to
# rebind is that `web.Response` sits on every request path and must not pay a
# __getattr__ per call, and an import-only door would pass a naive
# did-aiohttp-load assertion while quietly costing that forever.


@functools.lru_cache(maxsize=1)
def _import_door_probe():
    """Probe the doors in a child process, once for every test below.

    A child is required because of the SUITE, not this module: importing
    tests/test_perf_invariants.py leaves both doors shut, but test_cron,
    test_ui_endpoints and test_web_scopes all start web apps, so under a full
    run the parent reaches this test with aiohttp long since imported and the
    AFTER-IMPORT half would be vacuous.  Inlining the assertions passes when
    this file is run alone and fails (or worse, silently proves nothing) in
    the full suite.

    Cached because the child costs ~0.3s, most of it importing the aiohttp
    the zeroconf assertion never looks at, and because two tests reading two
    DIFFERENT children could disagree about a door without either failing.

    Deliberately NOT isolated (``-I``/``-E``): tox.ini puts the package on
    PYTHONPATH, so an isolated child cannot import cronstable at all.  The
    parse below carries the weight instead.  It requires every key and calls
    ``int()`` without a fallback, so a sitecustomize or shim printing to
    stdout splices into a key name and fails loudly here, rather than
    degrading a count to a string and reporting a door state nobody measured.
    """
    code = textwrap.dedent(
        """
        import sys

        import cronstable.cron as cron

        def loaded(root):
            return sum(
                1 for m in sys.modules
                if m == root or m.startswith(root + ".")
            )

        print("AIOHTTP-AFTER-IMPORT", loaded("aiohttp"))
        print("ZEROCONF-AFTER-IMPORT", loaded("zeroconf"))
        print("ZEROCONF-INSTALLED", int(_zeroconf_installed()))
        print("ISAL-AFTER-IMPORT", loaded("isal"))
        print("ISAL-INSTALLED", int(_isal_installed()))
        cron.web.Response  # first touch: this is what opens the door
        print("AIOHTTP-AFTER-TOUCH", loaded("aiohttp"))
        # the NAME, not type(...).__name__: both globals rebind to modules, so
        # a door that bound `aiohttp` to aiohttp.web (or either to the wrong
        # one) reads as "module" on both and slips through.
        print("WEB-NAME-AFTER-TOUCH", getattr(cron.web, "__name__", "?"))
        print(
            "AIOHTTP-NAME-AFTER-TOUCH",
            getattr(cron.aiohttp, "__name__", "?"),
        )
        """
    )
    preamble = textwrap.dedent(
        """
        import importlib.util

        def _zeroconf_installed():
            return importlib.util.find_spec("zeroconf") is not None

        def _isal_installed():
            return importlib.util.find_spec("isal") is not None
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", preamble + code],
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr
    out = {}
    for line in done.stdout.split("\n"):
        key, _, value = line.partition(" ")
        if key.endswith("-NAME-AFTER-TOUCH"):
            out[key] = value.strip()
        elif key.endswith(("-AFTER-IMPORT", "-AFTER-TOUCH", "-INSTALLED")):
            # int(), never a silent fallback: a garbled count must raise here
            # rather than reach an assertion and be reported as a door that
            # is open when nothing was measured at all.
            out[key] = int(value)
    missing = {
        "AIOHTTP-AFTER-IMPORT",
        "ZEROCONF-AFTER-IMPORT",
        "ZEROCONF-INSTALLED",
        "ISAL-AFTER-IMPORT",
        "ISAL-INSTALLED",
        "AIOHTTP-AFTER-TOUCH",
        "WEB-NAME-AFTER-TOUCH",
        "AIOHTTP-NAME-AFTER-TOUCH",
    } - set(out)
    assert not missing, "probe child printed no %s\nstdout:\n%s" % (
        sorted(missing),
        done.stdout,
    )
    return out


def test_importing_the_daemon_loads_no_aiohttp_and_first_touch_loads_it():
    probe = _import_door_probe()
    assert probe["AIOHTTP-AFTER-IMPORT"] == 0, (
        "importing cronstable.cron pulled in aiohttp, so the lazy door is "
        "open at import time: something in the module body reads an "
        "attribute off the `web`/`aiohttp` proxy (a decorator, a base "
        "class, a default argument, a module-level constant). Move it onto "
        "a runtime path. This costs every offline caller 144 ms and 14 MB "
        "of RSS, and gates startup.import_daemon / mem.rss_daemon_import."
    )
    assert probe["AIOHTTP-AFTER-TOUCH"] > 0, (
        "touching cron.web did not import aiohttp: the door no longer "
        "resolves the real module, which breaks every web/cluster/push "
        "path at runtime."
    )
    # the import alone is not the contract: __getattr__ must also rebind both
    # globals, to the RIGHT modules, or every later web.Response(...) on the
    # request path pays a proxy hop plus a sys.modules lookup forever. Assert
    # the module names: both rebind to modules, so `type(...).__name__` reads
    # "module" either way and cannot see a door that bound `aiohttp` to
    # aiohttp.web. That mutation is one word in cron.py and it breaks the
    # `except (..., aiohttp.ClientError, ...)` tuples, which is an
    # AttributeError raised while already handling a failure.
    assert probe["WEB-NAME-AFTER-TOUCH"] == "aiohttp.web", (
        "cron.web resolved to %r after the first touch, not aiohttp.web: the "
        "door imported aiohttp but rebound the global to the wrong object "
        "(or not at all, leaving the proxy on the request path)."
        % probe["WEB-NAME-AFTER-TOUCH"]
    )
    assert probe["AIOHTTP-NAME-AFTER-TOUCH"] == "aiohttp", (
        "cron.aiohttp resolved to %r after touching cron.web, not aiohttp: "
        "the door rebinds only one of the two globals it promises, or binds "
        "them to the same module."
        % probe["AIOHTTP-NAME-AFTER-TOUCH"]
    )


def test_importing_the_daemon_loads_no_zeroconf():
    # discovery.py's own door, same shape and the same blind spot: cron.py
    # imports discovery unconditionally while web.bonjour is off by default,
    # so an eager zeroconf import would tax every daemon start,
    # --validate-config and --job-set-id. Nothing else gates it.
    probe = _import_door_probe()
    # zeroconf is an optional extra (pyproject's `discovery`), so a zero here
    # proves a shut door only when the package is actually installed. Without
    # this the gate goes permanently green the day a dev-dep prune or a
    # platform marker drops zeroconf from the row, with the regression live:
    # discovery.py catches `except Exception` around its import, so hoisting
    # those imports to module scope stays silent on a machine without it.
    assert probe["ZEROCONF-INSTALLED"] == 1, (
        "zeroconf is not installed in this environment, so the door check "
        "below would pass vacuously. The dev extra requires it; "
        "install the `discovery` extra or fix the environment."
    )
    assert probe["ZEROCONF-AFTER-IMPORT"] == 0, (
        "importing cronstable.cron pulled in zeroconf: discovery.py's "
        "_probe_zeroconf deferral was defeated, costing ~24 ms and ~3.6 MB "
        "of RSS on every start that never advertises."
    )


def test_importing_the_daemon_loads_no_isal():
    # cronstable._gzip resolves its backend on first use: isal imports the
    # stdlib gzip module, and a daemon with no web or cluster listener
    # never compresses anything.
    probe = _import_door_probe()
    if not probe["ISAL-INSTALLED"]:
        pytest.skip("isal has no wheel for this platform")
    assert probe["ISAL-AFTER-IMPORT"] == 0, (
        "importing cronstable.cron pulled in isal: cronstable._gzip's "
        "deferred backend was defeated, costing ~5 ms on every start."
    )


# --- the idle reaper holds no finished run ---------------------------------
#
# The reaper parks between batches for as long as nothing launches or
# finishes.  A finished RunningJob carries its captured output and its
# process handles, so the parked coroutine must hold none of the batch it
# just handled.

_SCHEDCORE_RUN_JOB = (
    "jobs:\n  - name: j\n"
    + yaml_command(cmd_print())
    + '\n    schedule: "0 0 * * *"\n'
)


async def test_idle_reaper_releases_its_finished_batch():
    import asyncio
    import gc
    import weakref

    cron = Cron(None, config_yaml=_SCHEDCORE_RUN_JOB)
    reaper = asyncio.create_task(cron._wait_for_running_jobs())
    try:
        await cron.launch_scheduled_job(cron.cron_jobs["j"])
        ref = weakref.ref(cron.running_jobs["j"][0])
        for _ in range(3000):
            if not cron.running_jobs:
                break
            await asyncio.sleep(0.01)
        assert not cron.running_jobs, "the job never finished"
        await cron._drain_completions()
        # let the reaper reach its wait and the done callbacks run
        for _ in range(5):
            await asyncio.sleep(0)
        assert cron.last_run["j"].outcome == "success"
        gc.collect()
        assert ref() is None, "idle reaper retained its last finished batch"
    finally:
        cron._stop_event.set()
        cron._jobs_running.set()
        await asyncio.wait_for(reaper, 10.0)


# --- the durable-write count of a retry ladder ------------------------------
#
# A run that fails and retries adds a FIXED set of records to the one-run
# count above: one "pending" when the retry is armed and one "settled" when
# it is consumed, per retry, plus one "settled" when the ladder ends.  Two
# retries that both fail therefore write 14 records for the job: three runs
# of open, ledger record, close (9), two pending, three settled.  Each of
# them is a durable write a fleet-wide outage multiplies by every failing
# job, and a missing settle re-arms a consumed retry on the next boot.

_SCHEDCORE_RETRY_JOB = (
    "jobs:\n  - name: j\n"
    + yaml_command(cmd_print(code=1))
    + '\n    schedule: "0 0 * * *"\n'
    "    onFailure:\n      retry:\n        maximumRetries: 2\n"
    "        initialDelay: 0\n        maximumDelay: 1\n"
    "        backoffMultiplier: 1\n"
)


async def test_exhausted_two_retry_ladder_writes_fourteen_records(tmp_path):
    cron = Cron(None, config_yaml=_SCHEDCORE_RETRY_JOB)
    cfg = _state_cfg(
        "state:\n  path: %s\n  jobApi:\n    enabled: false\n" % tmp_path
    )
    await start_state(cron, cfg)
    assert cron.state_backend is not None
    try:
        backend = cron.state_backend
        events = []
        real_append = backend.append_record

        async def spying_append(stream, data, **kwargs):
            events.append(
                (
                    stream,
                    data.get("kind") or data.get("outcome"),
                    data.get("reason"),
                )
            )
            return await real_append(stream, data, **kwargs)

        backend.append_record = spying_append  # type: ignore[method-assign]

        await cron.launch_scheduled_job(cron.cron_jobs["j"])
        for attempt in range(3):
            (running,) = cron.running_jobs["j"]
            await running.wait()
            await cron._handle_finished_job(running)
            await cron._drain_completions()
            if attempt < 2:
                # the armed retry sleeps its (zero) delay, settles its
                # pending record and launches the next attempt
                await cron.retry_state["j"].task
        assert "j" not in cron.retry_state and not cron.running_jobs
        await _drain_state_writes(cron)

        ladder = [e for e in events if not e[0].startswith("counters/")]
        inflight = [
            kind for stream, kind, _ in ladder if stream == "inflight/j"
        ]
        runs = [kind for stream, kind, _ in ladder if stream == "runs/j"]
        retries = [
            (kind, reason)
            for stream, kind, reason in ladder
            if stream == "retries/j"
        ]
        assert inflight == ["open", "closed"] * 3, events
        assert runs == ["failure"] * 3, events
        assert retries == [
            ("pending", None),
            ("settled", "launched"),
            ("pending", None),
            ("settled", "launched"),
            ("settled", "exhausted"),
        ], events
        assert len(ladder) == 14, (
            "an exhausted two-retry ladder wrote %d durable records, "
            "expected exactly 14: %r" % (len(ladder), ladder)
        )
        assert len(events) > len(ladder), (
            "the first persist carries a counter snapshot; got %r" % (events,)
        )
    finally:
        await _drain_state_writes(cron)
        await cron.state_backend.stop()
        cron.state_backend = None


# --- a pool tick reads its document once -----------------------------------
#
# A pool is one durable document, so every admission attempt reads and
# compares the whole queue.  A tick over a full pool must cost ONE read
# however long the queue is: no attempt for an entry that cannot fit, and
# no write.  When a slot frees, the tick that follows pays one read and one
# claim.  A tick that tries every waiting entry multiplies the read by 32,
# once a second, for as long as the pool stays full.

_SCHEDCORE_POOL_JOBS = (
    "pools:\n  shared:\n    slots: 2\n    maxQueued: 64\n"
    "jobs:\n  - name: j\n    command: 'true'\n"
    '    schedule: "0 0 * * *"\n    pool: shared\n'
)


async def test_saturated_pool_tick_costs_one_read(tmp_path):
    cfg = "state:\n  path: %s\n  jobApi:\n    enabled: false\n" % tmp_path
    cron = Cron(None, config_yaml=cfg + _SCHEDCORE_POOL_JOBS)
    await start_state(cron, _state_cfg(cfg + _SCHEDCORE_POOL_JOBS))
    assert cron.state_backend is not None
    pools = cron._pools
    pools.service = lambda: None  # the test drives every tick itself
    try:
        backend = cron.state_backend
        job = cron.cron_jobs["j"]
        tickets = []
        for _ in range(2):
            entry = await pools.enqueue(job, payload={"kind": "job"})
            tickets.append(await pools.acquire("shared", entry["id"]))
        assert all(tickets)
        waiting = 40  # more than one tick's 32-entry window
        for _ in range(waiting):
            await pools.enqueue(job, payload={"kind": "job"})

        mutations = []
        real_mutate = backend.mutate_document

        async def spying_mutate(namespace, key, transform):
            def observed(current):
                body, result = transform(current)
                mutations.append(
                    "read" if body is state_mod.DOC_KEEP else "write"
                )
                return body, result

            return await real_mutate(namespace, key, observed)

        backend.mutate_document = spying_mutate  # type: ignore[method-assign]
        attempts = []
        real_claim = pools._claim

        async def spying_claim(pool, key):
            attempts.append(key)
            return await real_claim(pool, key)

        pools._claim = spying_claim
        launched = []

        async def launch(job, **kwargs):
            launched.append(kwargs["pool_ticket"])
            return True

        cron.maybe_launch_job = launch

        await pools._tick_pool("shared")
        assert (mutations, attempts, launched) == (["read"], [], []), (
            "a tick over a full pool must read the document once, try no "
            "admission and write nothing; got %r with %d attempt(s)"
            % (mutations, len(attempts))
        )

        del mutations[:]
        await pools.finish(tickets.pop())
        assert mutations == ["write"]
        del mutations[:]
        await pools._tick_pool("shared")
        assert (mutations, len(attempts), len(launched)) == (
            ["read", "write"],
            1,
            1,
        ), (
            "one freed slot must cost the next tick one read and one "
            "claim; got %r with %d attempt(s)" % (mutations, len(attempts))
        )
        tickets.extend(launched)
        body = await backend.read_document("scheduler-pools", "shared")
        states = [e["state"] for e in body["entries"].values()]
        assert states.count("queued") == waiting - 1

        # The head is cancelled between the tick's read and its claim.
        # The refused claim reports that the entry left the queue, so the
        # tick goes on to the next head without reading the pool again.
        await pools.finish(tickets.pop(0))
        retire = pools._retire_tasks

        async def cancel_the_head(pool, body):
            head = pools_mod._waiting(body, limit=1)[0]
            await pools.cancel(pool, head["id"])
            return await retire(pool, body)

        pools._retire_tasks = cancel_the_head
        del mutations[:], attempts[:], launched[:]
        await pools._tick_pool("shared")
        assert (mutations, len(attempts), len(launched)) == (
            ["read", "write", "read", "write"],
            2,
            1,
        ), (
            "a head cancelled mid-tick must cost its cancel, one refused "
            "claim and one admitting claim; got %r with %d attempt(s)"
            % (mutations, len(attempts))
        )
        tickets.extend(launched)
    finally:
        for ticket in tickets:
            await pools.finish(ticket)
        await pools.close()
        await _drain_state_writes(cron)
        await cron.state_backend.stop()
        cron.state_backend = None


# --- the store calls of a catch-up evaluation -------------------------------
#
# After downtime every job that asks for catch-up is evaluated against the
# store, one job at a time.  Each owed job costs exactly four store calls:
# the last-run watermark, the open-checkpoint read, the pause-window read,
# and the checkpoint that records the backfill.  A fleet that leaves
# onMissed at its default pays ONE stream listing in total, never a read
# per job.  A call added per job here is a store round trip per job per
# boot.


def _schedcore_catchup_jobs(policy):
    lines = ["jobs:"]
    for i in range(3):
        lines.append("  - name: j%d" % i)
        lines.append("    command: 'true'")
        lines.append('    schedule: "*/5 * * * *"')
        if policy is not None:
            lines.append("    onMissed: %s" % policy)
    return "\n".join(lines) + "\n"


async def _schedcore_catchup_calls(tmp_path, policy):
    """Evaluate catch-up for three jobs down for two days; return the
    store calls it made and the backfills it scheduled."""
    import asyncio

    cfg = "state:\n  path: %s\n  jobApi:\n    enabled: false\n" % tmp_path
    text = cfg + _schedcore_catchup_jobs(policy)
    cron = Cron(None, config_yaml=text)
    await start_state(cron, _state_cfg(text))
    assert cron.state_backend is not None
    try:
        backend = cron.state_backend
        now = datetime.datetime(
            2026, 3, 15, 12, 30, 5, tzinfo=datetime.timezone.utc
        )
        last = (now - datetime.timedelta(hours=48)).isoformat()
        for name in cron.cron_jobs:
            await backend.append_record(
                cron._run_stream(name),
                {
                    "outcome": "success",
                    "exit_code": 0,
                    "started_at": last,
                    "finished_at": last,
                    "ranAt": last,
                },
            )
        calls = []
        for method in (
            "derive_max",
            "list_records",
            "append_record",
            "list_stream_names",
            "list_stream_names_audit",
        ):

            def spying(real, method):
                async def spy(*args, **kwargs):
                    calls.append((method, args[0]))
                    return await real(*args, **kwargs)

                return spy

            setattr(backend, method, spying(getattr(backend, method), method))
        owed = []

        async def backfill(job, count, offset, at, **kwargs):
            owed.append((job.name, count))

        cron._run_catch_up = backfill
        unresolved = await cron._evaluate_catch_up(now)
        await asyncio.gather(*list(cron._catchup_tasks))
        assert not unresolved
        return calls, owed
    finally:
        await _drain_state_writes(cron)
        await cron.state_backend.stop()
        cron.state_backend = None


async def test_catch_up_evaluation_makes_four_store_calls_per_owed_job(
    tmp_path,
):
    calls, owed = await _schedcore_catchup_calls(tmp_path, "run-once")
    assert sorted(owed) == [("j0", 1), ("j1", 1), ("j2", 1)]
    for name in ("j0", "j1", "j2"):
        mine = [
            (method, stream)
            for method, stream in calls
            if stream.endswith("/" + name)
        ]
        assert mine == [
            ("derive_max", "runs/" + name),
            ("list_records", "catchup/" + name),
            ("list_records", "paused/" + name),
            ("append_record", "catchup/" + name),
        ], "catch-up evaluation of %s made %r" % (name, mine)
    assert len(calls) == 12, calls


async def test_catch_up_evaluation_lists_once_for_a_skip_fleet(tmp_path):
    calls, owed = await _schedcore_catchup_calls(tmp_path, None)
    assert owed == []
    assert calls == [("list_stream_names_audit", "catchup/")], (
        "jobs that skip missed runs must cost one stream listing in total; "
        "got %r" % (calls,)
    )


_DAGSTATE_NO_JOB_API = "  jobApi:\n    enabled: false\n"


def _dagstate_chain_yaml(name, count, *, reverse=False, retain=50):
    """A ``count``-task chain ``t0 <- t1 <- ...``, optionally listed last
    task first."""
    order = range(count - 1, -1, -1) if reverse else range(count)
    lines = ["dags:", "  - name: %s" % name, "    retainRuns: %d" % retain]
    lines.append("    tasks:")
    for i in order:
        lines += ["      - id: t%d" % i, "        command: 'x'"]
        if i:
            lines += ["        dependsOn:", "          - t%d" % (i - 1)]
    return "\n".join(lines) + "\n"


def _dagstate_stub_launches(monkeypatch, cron):
    """Launch DAG tasks without a subprocess; return the launched list.

    The stub is the real RunningJob with start() replaced, so everything
    else the launch and completion paths read from a run is the real thing.
    Launched instances land in the returned list, where the reaper would
    have picked them up.
    """

    class _Proc:
        pid = 4242

    class _Started(dagrun.RunningJob):
        async def start(self):
            self.proc = _Proc()
            self.retcode = 0

    launched = []
    monkeypatch.setattr(dagrun, "RunningJob", _Started)
    monkeypatch.setattr(cron, "_add_running_instance", launched.append)
    return launched


def _dagstate_count_writes(monkeypatch, backend):
    """Count document mutations, document file rewrites and lease takes."""
    counts = {"mutations": 0, "rewrites": 0, "leases": 0}
    real_mutate = backend.mutate_document
    real_write = backend._atomic_write
    real_acquire = backend.acquire_lease

    async def counting_mutate(namespace, key, transform):
        counts["mutations"] += 1
        return await real_mutate(namespace, key, transform)

    def counting_write(dest, payload, **kwargs):
        if dest.endswith(".doc"):
            counts["rewrites"] += 1
        return real_write(dest, payload, **kwargs)

    async def counting_acquire(name, holder, ttl):
        counts["leases"] += 1
        return await real_acquire(name, holder, ttl)

    monkeypatch.setattr(backend, "mutate_document", counting_mutate)
    monkeypatch.setattr(backend, "_atomic_write", counting_write)
    monkeypatch.setattr(backend, "acquire_lease", counting_acquire)
    return counts


async def _dagstate_finish(cron, running, *, exit_code=0):
    """Hand one launched instance back the way the reaper does."""
    running.retcode = exit_code
    await cron._dag.on_task_finished(running)
    await cron._dag.flush_completions()
    await _drain_pending(cron)


# --- 1. document writes per task ------------------------------------------
#
# A run document is read and rewritten whole, so the number of rewrites per
# task is what a run costs the store.  Starting a run takes four document
# mutations under one lease: create, reconcile (which keeps the document),
# claim, and the pid stamp.  Every later task of a chain takes exactly
# three rewrites: its upstream's completion, its own claim, and its pid
# stamp.  The last completion is followed by the advance that ends the run.
# A fourth write per task compounds per task per run forever; a missing one
# means a completion, a claim or a pid was not recorded.


async def test_chain_task_costs_three_document_rewrites(dag_cron, monkeypatch):
    tasks = 6
    cron = await dag_cron(
        _dagstate_chain_yaml("chain", tasks), extra_state=_DAGSTATE_NO_JOB_API
    )
    sched = cron._dag
    launched = _dagstate_stub_launches(monkeypatch, cron)
    counts = _dagstate_count_writes(monkeypatch, cron.state_backend)

    run_key = await sched.trigger_run("chain")
    await _drain_pending(cron)
    assert counts == {"mutations": 4, "rewrites": 3, "leases": 1}, (
        "starting a run must cost four document mutations (three of them "
        "rewrites) and one lease; got %r" % (counts,)
    )
    for step in range(tasks):
        (running,) = launched
        del launched[:]
        assert running.dag_ref.task_id == "t%d" % step
        before = dict(counts)
        await _dagstate_finish(cron, running)
        # the last task has no successor to claim and stamp
        expected = 3 if step < tasks - 1 else 2
        assert counts["mutations"] - before["mutations"] == expected
        assert counts["rewrites"] - before["rewrites"] == expected, (
            "finishing task %d rewrote the run document %d times, expected "
            "%d" % (step, counts["rewrites"] - before["rewrites"], expected)
        )
    assert counts["leases"] == 1
    assert launched == []
    body = await sched.get_run("chain", run_key)
    assert body["state"] == dag.SUCCESS
    assert ("chain", run_key) not in sched._owned


# --- 2. full listings per GC pass -----------------------------------------
#
# The retention pass runs inside the scheduler's single-flight service
# task.  It lists the run namespace once, then deletes the excess runs a
# batch at a time, under the lease of every run in the batch.  The check
# it repeats there reads the recovery runs in one call and the recovery
# batches in another, so the full listings of the run namespace must not
# depend on how many runs the pass deletes, and its keyed listings follow
# the number of batches.


async def test_gc_pass_full_listings_do_not_grow_with_deleted_runs(
    dag_cron, monkeypatch
):
    monkeypatch.setattr(dagrun, "GC_LEASE_BATCH", 4)
    cron = await dag_cron(_dagstate_chain_yaml("gc", 1, retain=2))
    backend = cron.state_backend
    listings = []
    keyed = []
    real_list = backend.list_documents
    real_keyed = backend.list_documents_keyed

    async def counting_list(namespace, **kwargs):
        listings.append(namespace)
        return await real_list(namespace, **kwargs)

    async def counting_keyed(namespace, key_prefix, **kwargs):
        keyed.append(namespace)
        return await real_keyed(namespace, key_prefix, **kwargs)

    monkeypatch.setattr(backend, "list_documents", counting_list)
    monkeypatch.setattr(backend, "list_documents_keyed", counting_keyed)
    seen = {}
    first = 0
    for excess in (3, 12):
        # two runs are retained: the first round seeds them as well
        seeded = excess + (0 if first else 2)
        for i in range(first, first + seeded):
            body = {
                "dag": "gc",
                "runKey": "r%03d" % i,
                "runId": "id%03d" % i,
                "state": dag.SUCCESS,
                "createdAt": 1000.0 + i,
                "tasks": {},
                "mapped": {},
            }
            await backend.mutate_document(
                "dagrun/gc", body["runKey"], lambda _cur, b=body: (b, None)
            )
        first += seeded
        before = len(await backend.list_document_keys("dagrun/gc"))
        del listings[:], keyed[:]
        await cron._dag._gc_one_dag(backend, "gc", cron.cron_dags["gc"])
        after = len(await backend.list_document_keys("dagrun/gc"))
        assert (before - after, after) == (excess, 2)
        seen[excess] = listings.count("dagrun/gc")
        assert listings.count("recoverybatch/gc") == 1, (
            "a GC pass lists the recovery batches once; got %r" % (listings,)
        )
        batches = -(-excess // dagrun.GC_LEASE_BATCH)
        assert (
            keyed.count("dagrun/gc"),
            keyed.count("recoverybatch/gc"),
        ) == (batches, batches), (
            "under the leases of each batch of deleted runs a GC pass "
            "reads the recovery runs in one call and the recovery batches "
            "in another; got %r" % (keyed,)
        )
    assert seen[3] == seen[12] == 1, (
        "a GC pass must list the run namespace once however many runs it "
        "deletes; got %r" % (seen,)
    )


# --- 3. advances per failure cascade --------------------------------------
#
# When a task fails, everything downstream of it ends upstream_failed and
# the run ends.  That takes one advance whatever order the configuration
# lists the tasks in: an advance that settled one level at a time would
# leave the rest to the idle wake, a minute apart per level.


async def test_failure_cascade_ends_a_reversed_chain_in_one_advance(
    dag_cron, monkeypatch
):
    tasks = 8
    cron = await dag_cron(
        _dagstate_chain_yaml("reversed", tasks, reverse=True),
        extra_state=_DAGSTATE_NO_JOB_API,
    )
    sched = cron._dag
    launched = _dagstate_stub_launches(monkeypatch, cron)
    counts = _dagstate_count_writes(monkeypatch, cron.state_backend)
    run_key = await sched.trigger_run("reversed")
    (running,) = launched
    del launched[:]
    assert running.dag_ref.task_id == "t0"

    before = dict(counts)
    await _dagstate_finish(cron, running, exit_code=1)
    body = await sched.get_run("reversed", run_key)
    assert body["state"] == dag.FAILED
    assert body["tasks"]["t0"]["state"] == dag.FAILED
    assert [body["tasks"]["t%d" % i]["state"] for i in range(1, tasks)] == [
        dag.UPSTREAM_FAILED
    ] * (tasks - 1)
    assert counts["rewrites"] - before["rewrites"] == 2, (
        "the completion and ONE advance must end the run; the document was "
        "rewritten %d times" % (counts["rewrites"] - before["rewrites"])
    )
    assert launched == []
    assert ("reversed", run_key) not in sched._owned


# --- 4. directory listings per pruned append -------------------------------
#
# Every finished run appends its ledger record with prune_keep, and the
# prune that rides it lists the whole stream.  It is amortised: one pass,
# so one listing, per _PRUNE_EVERY_APPENDS appends.  A pass per append
# would list a full stream for every run of every job.


async def test_prune_keep_appends_list_the_stream_once_per_cadence(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        # The first append seeds the stream's name floor and carries the
        # pass a backend runs on its first append to a stream; both list
        # the directory, once each, outside the cadence under test.
        await backend.append_record("runs", {"seq": -1}, prune_keep=1000)
        stream_dir = os.path.normpath(backend._stream_dir("runs"))
        listings = []
        real_listdir = os.listdir

        def counting_listdir(path="."):
            if os.path.normpath(str(path)) == stream_dir:
                listings.append(path)
            return real_listdir(path)

        monkeypatch.setattr(os, "listdir", counting_listdir)
        cadence = state_mod._PRUNE_EVERY_APPENDS
        passes = 5
        for i in range(passes * cadence):
            await backend.append_record("runs", {"seq": i}, prune_keep=1000)
        assert len(listings) == passes, (
            "%d appends listed the stream %d times, expected one listing "
            "per %d appends" % (passes * cadence, len(listings), cadence)
        )
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- 5. record reads per artifact put --------------------------------------
#
# XCom publishes one name per task instance, so a run's artifact stream
# holds as many distinct names as the run has instances.  The name-keyed
# prune that rides each put reads every record of the stream, and it waits
# for the stream to double between passes, so a put costs a constant
# number of record reads on average at any stream size.  Every name's
# record must also survive: a put never prunes a live version.


async def test_artifact_put_record_reads_do_not_grow_with_distinct_names(
    tmp_path, monkeypatch
):
    # the flushes are skipped for speed: this counts reads, and the append
    # test above pins the barrier protocol
    monkeypatch.setattr(os, "fsync", lambda fd: None)
    monkeypatch.setattr(state_mod, "fsync_directory", lambda path: None)

    async def reads_per_put(names):
        backend = _backend(tmp_path / ("names-%d" % names))
        await backend.start()
        try:
            reads = []
            real_read = backend._read_record

            def counting_read(stream_dir, name, **kwargs):
                reads.append(name)
                return real_read(stream_dir, name, **kwargs)

            backend._read_record = counting_read  # type: ignore[method-assign]
            for i in range(names):
                await jobstate.artifact_put(
                    backend, "scope", "task%d/return_value" % i, b"x"
                )
            backend._read_record = real_read  # type: ignore[method-assign]
            listing = await jobstate.artifact_list(backend, "scope")
            assert len(listing) == names
            return len(reads) / names
        finally:
            await backend.stop()

    per_put = {names: await reads_per_put(names) for names in (48, 384)}
    assert max(per_put.values()) <= 3.0, (
        "record reads per artifact put grew with the stream: %r (reads per "
        "put by distinct names)" % (per_put,)
    )


# --- 5b. what a republish costs between prune passes -----------------------
#
# Publishing a name again keeps the version that it replaces and unlinks the
# one before that, which the backend remembers.  Between prune passes a
# republish lists no directory and reads no record, and the stream holds two
# records per name.  Each unlink follows one lstat, of the remembered record
# that it rests on.


async def test_artifact_republish_lists_and_reads_nothing_between_passes(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(os, "fsync", lambda fd: None)
    monkeypatch.setattr(state_mod, "fsync_directory", lambda path: None)
    backend = _backend(tmp_path)
    await backend.start()
    try:
        names = ["report-a", "report-b"]
        # every publish before the stream's second prune pass
        rounds = state_mod._PRUNE_EVERY_APPENDS // len(names)
        stream = jobstate.ARTIFACT_STREAM_PREFIX + "scope"
        stream_dir = os.path.normpath(backend._stream_dir(stream))

        async def publish(version):
            for name in names:
                await jobstate.artifact_put(
                    backend, "scope", name, b"v%d" % version
                )

        await publish(0)
        listed = []
        read = []
        unlinked = []
        confirmed = []
        real_listdir = os.listdir
        real_unlink = os.unlink
        real_lstat = os.lstat
        real_read = backend._read_record

        def spying_listdir(path="."):
            if os.path.normpath(str(path)) == stream_dir:
                listed.append(path)
            return real_listdir(path)

        def spying_unlink(path, *args, **kwargs):
            if os.path.dirname(os.path.normpath(str(path))) == stream_dir:
                unlinked.append(path)
            return real_unlink(path, *args, **kwargs)

        def spying_lstat(path, *args, **kwargs):
            if os.path.dirname(os.path.normpath(str(path))) == stream_dir:
                confirmed.append(path)
            return real_lstat(path, *args, **kwargs)

        def counting_read(stream_dir, name, **kwargs):
            read.append(name)
            return real_read(stream_dir, name, **kwargs)

        monkeypatch.setattr(os, "listdir", spying_listdir)
        monkeypatch.setattr(os, "unlink", spying_unlink)
        monkeypatch.setattr(os, "lstat", spying_lstat)
        backend._read_record = counting_read  # type: ignore[method-assign]
        for version in range(1, rounds):
            await publish(version)
        monkeypatch.undo()
        backend._read_record = real_read  # type: ignore[method-assign]

        assert listed == [] and read == [], (
            "a republish between prune passes listed the stream %d times "
            "and read %d records" % (len(listed), len(read))
        )
        assert len(unlinked) == (rounds - 2) * len(names), (
            "%d republishes of %d names unlinked %d records; each one "
            "after a name's first unlinks the version two publishes back"
            % ((rounds - 1) * len(names), len(names), len(unlinked))
        )
        assert len(confirmed) == len(unlinked), (
            "%d unlinks followed %d lstats of the stream's records, "
            "expected one each" % (len(unlinked), len(confirmed))
        )
        records = await backend.list_records(stream)
        assert len(records) == 2 * len(names)
        for name in names:
            result = await jobstate.artifact_get(backend, "scope", name)
            assert result is not None
            assert result[1] == b"v%d" % (rounds - 1)
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- 5c. what an artifact read costs beside the name memory -----------------
#
# A lookup and a listing compare their answer with the newest record that
# the backend remembers for each name.  When the answer is that record, the
# read has listed the stream once and looks for nothing else in the store.


async def test_artifact_reads_list_once_and_confirm_nothing_when_untorn(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(os, "fsync", lambda fd: None)
    monkeypatch.setattr(state_mod, "fsync_directory", lambda path: None)
    backend = _backend(tmp_path)
    await backend.start()
    try:
        names = ["report-%d" % i for i in range(6)]
        stream = jobstate.ARTIFACT_STREAM_PREFIX + "scope"
        stream_dir = os.path.normpath(backend._stream_dir(stream))
        for version in range(2):
            for name in names:
                await jobstate.artifact_put(
                    backend, "scope", name, b"v%d" % version
                )
        listed = []
        confirmed = []
        real_listdir = os.listdir
        real_lstat = os.lstat

        def spying_listdir(path="."):
            if os.path.normpath(str(path)) == stream_dir:
                listed.append(path)
            return real_listdir(path)

        def spying_lstat(path, *args, **kwargs):
            if os.path.dirname(os.path.normpath(str(path))) == stream_dir:
                confirmed.append(path)
            return real_lstat(path, *args, **kwargs)

        monkeypatch.setattr(os, "listdir", spying_listdir)
        monkeypatch.setattr(os, "lstat", spying_lstat)
        reads = 0
        for strict in (False, True):
            for name in names:
                record = await jobstate.artifact_get_record(
                    backend, "scope", name, strict=strict
                )
                assert record is not None and record["name"] == name
                reads += 1
            listing = await jobstate.artifact_list(backend, "scope")
            assert [record["name"] for record in listing] == names
            reads += 1
        monkeypatch.undo()

        assert len(listed) == reads, (
            "%d artifact reads listed the stream %d times, expected one "
            "listing each" % (reads, len(listed))
        )
        assert confirmed == [], (
            "%d artifact reads of an untorn stream looked for %d records "
            "in the store" % (reads, len(confirmed))
        )
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- 6. file opens per inventory walk ---------------------------------------
#
# GET /state and MCP cron_inspect_state walk the store on every poll.  The
# walk is metadata only: it counts each stream's records and each
# namespace's documents from directory listings, and reads the lease files.
# Opening a record or a document would turn a poll into a read of the
# whole store.


async def test_inventory_opens_no_record_or_document_file(
    tmp_path, monkeypatch
):
    backend = _backend(tmp_path)
    await backend.start()
    try:
        for stream in range(6):
            for seq in range(4):
                await backend.append_record(
                    "runs/job%d" % stream, {"seq": seq}
                )
        for key in range(5):
            await backend.mutate_document(
                "kv/scope", "k%d" % key, lambda _cur: ({"v": 1}, None)
            )
        assert await backend.acquire_lease("held", "holder-a", ttl=30.0)

        opened = []
        real_open = open
        real_os_open = os.open

        def spying_open(path, *args, **kwargs):
            opened.append(str(path))
            return real_open(path, *args, **kwargs)

        def spying_os_open(path, *args, **kwargs):
            opened.append(str(path))
            return real_os_open(path, *args, **kwargs)

        monkeypatch.setattr(state_mod, "open", spying_open, raising=False)
        monkeypatch.setattr(os, "open", spying_os_open)
        inventory = await backend.inventory()
        monkeypatch.undo()

        assert inventory["records"]["runs"]["streams"] == 6
        assert inventory["records"]["runs"]["count"] == 24
        assert inventory["documents"]["kv"]["count"] == 5
        assert [lease["name"] for lease in inventory["leases"]] == ["held"]
        # the spy does see the walk: the held lease is read
        assert any(path.endswith(".lease") for path in opened)
        touched = [p for p in opened if p.endswith((".json", ".doc"))]
        assert touched == [], (
            "inventory opened record or document files: %r" % (touched,)
        )
    finally:
        monkeypatch.undo()
        await backend.stop()


# --- the strictyaml mapping-hop lookup ---------------------------------------
#
# strictyaml resolves a mapping hop of a pointer by scanning the mapping's
# items for the first key that matches, so every chunk access under a
# mapping of K keys costs K/2 comparisons and a parse is quadratic in the
# keys of each mapping.  config._patch_strictyaml_key_lookup answers the hop
# from the mapping's hash table while parse_config_string loads
# CONFIG_SCHEMA.  The cost invariant is a COUNT: a load iterates each
# mapping's items once, to build its data, and never to find a key.  The
# meaning is gated separately, by loading the same documents under
# upstream's scan and comparing everything a caller can observe.


def _configcli_upstream_individual_get():
    """strictyaml's ``YAMLPointer._individual_get``, verbatim."""

    def _individual_get(self, segment, index_type, index, strictdoc):
        if index_type == "val":
            for key, value in segment.items():
                if key == index[0]:
                    return value
                if hasattr(key, "text"):
                    if key.text == index[0]:
                        return value
            raise Exception("Invalid state")
        elif index_type == "index":
            return segment[index]
        elif index_type == "textslice":
            return segment[index[0] : index[1]]
        elif index_type == "key":
            return index[1] if strictdoc else index[0]
        else:
            raise Exception("Invalid state")

    return _individual_get


def _configcli_installed_lookup():
    """The lookup config.py installed, which must not be upstream's scan."""
    from strictyaml.yamlpointer import YAMLPointer

    import cronstable.config  # noqa: F401  (installs the shims)

    installed = YAMLPointer._individual_get
    assert "items" not in installed.__code__.co_names, (
        "config._patch_strictyaml_key_lookup did not rebind "
        "YAMLPointer._individual_get, so every comparison below would "
        "pit upstream's scan against itself"
    )
    return installed


def _configcli_describe(node):
    """Everything a caller can read off a loaded strictyaml tree."""
    kind = type(node.validator).__name__
    if node.is_mapping():
        return (
            kind,
            [
                (_configcli_describe(key), _configcli_describe(value))
                for key, value in node.value.items()
            ],
        )
    if node.is_sequence():
        return (kind, [_configcli_describe(item) for item in node.value])
    return (kind, node.text, repr(node.data))


def _configcli_load_outcome(text, lookup, direct):
    """Load ``text`` over CONFIG_SCHEMA with ``lookup`` installed.

    Returns the whole observable result: the described tree and its dump,
    or the exception's type and message.
    """
    import strictyaml
    from strictyaml.yamlpointer import YAMLPointer

    from cronstable import config

    installed = YAMLPointer._individual_get
    state = config._STRICTYAML_KEY_LOOKUP
    was = getattr(state, "direct", False)
    YAMLPointer._individual_get = lookup
    state.direct = direct
    try:
        try:
            loaded = strictyaml.load(text, config.CONFIG_SCHEMA, label="doc")
        except Exception as ex:  # noqa: BLE001  (the outcome IS the error)
            return ("error", type(ex).__name__, str(ex))
        return ("ok", _configcli_describe(loaded), loaded.as_yaml())
    finally:
        state.direct = was
        YAMLPointer._individual_get = installed


def _configcli_both_outcomes(text):
    upstream = _configcli_upstream_individual_get()
    installed = _configcli_installed_lookup()
    return (
        _configcli_load_outcome(text, upstream, False),
        _configcli_load_outcome(text, installed, True),
    )


def _configcli_mapping_entries(data):
    """How many key/value pairs the mappings of plain parsed data hold."""
    if isinstance(data, dict):
        return len(data) + sum(
            _configcli_mapping_entries(value) for value in data.values()
        )
    if isinstance(data, list):
        return sum(_configcli_mapping_entries(item) for item in data)
    return 0


_CONFIGCLI_WIDE_MAPPINGS = {
    "pools": "pools:\n"
    + "".join("  pool%03d:\n    slots: 4\n" % i for i in range(48)),
    "headers": "web:\n  listen:\n    - http://127.0.0.1:8080\n  headers:\n"
    + "".join("    X-Header-%03d: v%d\n" % (i, i) for i in range(48)),
    "job": "jobs:\n"
    "  - name: wide\n"
    "    command: echo wide\n"
    "    schedule: '0 * * * *'\n"
    "    captureStdout: true\n"
    "    captureStderr: true\n"
    "    killTimeout: 5\n"
    "    saveLimit: 10\n"
    "    timezone: UTC\n"
    "    enabled: true\n"
    "    utc: true\n"
    "    environment:\n"
    + "".join(
        "      - key: K%02d\n        value: v%d\n" % (i, i) for i in range(12)
    ),
}


@pytest.mark.parametrize("shape", sorted(_CONFIGCLI_WIDE_MAPPINGS))
def test_mapping_hops_resolve_without_scanning(monkeypatch, shape):
    import strictyaml
    from strictyaml.ruamel.comments import CommentedMapItemsView

    from cronstable import config

    _configcli_installed_lookup()
    text = _CONFIGCLI_WIDE_MAPPINGS[shape]
    steps = [0]
    stock_iter = CommentedMapItemsView.__iter__

    def counting(self):
        for item in stock_iter(self):
            steps[0] += 1
            yield item

    monkeypatch.setattr(CommentedMapItemsView, "__iter__", counting)
    config.parse_config_string(text, "test")
    counted = steps[0]

    # the same document, loaded untimed for its shape alone
    monkeypatch.undo()
    entries = _configcli_mapping_entries(
        strictyaml.load(text, config.CONFIG_SCHEMA).data
    )
    assert entries >= 20, entries
    assert counted == entries, (
        "loading the %r document walked %d mapping items for %d mapping "
        "entries. One walk per mapping builds its data; anything more is "
        "a pointer hop scanning for its key, the quadratic parse "
        "config._patch_strictyaml_key_lookup removes"
        % (shape, counted, entries)
    )


def test_direct_key_lookup_loads_the_example_configs_identically():
    # Every example configuration in the repository, loaded under
    # upstream's scan and under the direct lookup.  Between them they use
    # each section of the schema, which is what a hand-written sample
    # cannot promise.
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = sorted(
        glob.glob(
            os.path.join(root, "example", "**", "*.yaml"), recursive=True
        )
    ) + [
        os.path.join(root, "tests", "testconfig.yaml"),
        os.path.join(root, "tests", "test_include_parent.yaml"),
    ]
    assert len(paths) >= 20, paths
    valid = 0
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        expected, actual = _configcli_both_outcomes(text)
        assert actual == expected, path
        valid += expected[0] == "ok"
    # a Kubernetes manifest among the examples is not a cronstable config;
    # the rest must really have loaded
    assert valid >= 20, valid


_CONFIGCLI_PARITY_DOC = textwrap.dedent(
    """\
    defaults:
      captureStderr: true
      environment:
        - key: TZ
          value: UTC
    pools:
      db:
        slots: 8
      "1":
        slots: 2
      api pool:
        slots: 4
        maxQueued: 50
    web:
      listen:
        - http://127.0.0.1:8080
      headers:
        X-Frame-Options: DENY
        1: one
        true: yes
        Content-Type: text/plain
    jobs:
      # a comment inside the sequence
      - name: alpha
        command: echo alpha
        schedule: '*/5 * * * *'
        timezone: America/New_York
        environment:
          - key: A
            value: '1'
        onFailure:
          retry:
            maximumRetries: 3
            initialDelay: 5
            maximumDelay: 300
            backoffMultiplier: 2
          report:
            sentry:
              extra:
                team: platform
                2: two
            webhook:
              url:
                fromEnvVar: HOOK
              headers:
                Authorization: token
                X-Team: platform
      - name: beta
        command:
          - echo
          - beta
        schedule:
          minute: "0"
          hour: "1"
        captureStdout: true
    dags:
      - name: etl
        schedule: '0 2 * * *'
        tasks:
          - id: extract
            command: echo extract
          - id: load
            command: echo load
            dependsOn:
              - extract
    logging:
      version: 1
      formatters:
        plain:
          format: '%(message)s'
          1: one
      root:
        level: INFO
    """
)


def test_direct_key_lookup_agrees_with_the_scan_on_mutated_documents():
    # One document that reaches every kind of mapping the schema has (fixed
    # keys, pattern keys, keys that read as numbers or booleans, the
    # free-form logging section), then seeded edits of it.  Most edits
    # break the document, so this is chiefly a comparison of ERRORS: the
    # exception type and the full message with its line, column and
    # snippet.
    keys = (
        "name", "command", "schedule", "environment", "key", "value",
        "slots", "headers", "report", "retry", "tasks", "id", "bogus",
        "1", "01", "1.0", "true", "null", "~", "x y", '"quoted"', "''",
        "<<", "? complex", "a:b",
    )  # fmt: skip
    values = (
        "", "x", "1", "1.5", "true", ".nan", "x y", "'q'", "|", "- a",
        "{a: b}", "[a]", "&anchor v", "*alias", "!!int 3", "~", "a: b",
    )  # fmt: skip
    rng = random.Random(20261006)

    def mutate(text):
        lines = text.split("\n")
        for _ in range(rng.choice((1, 1, 2))):
            i = rng.randrange(len(lines))
            line = lines[i]
            indent = " " * (len(line) - len(line.lstrip(" ")))
            op = rng.randrange(8)
            if op == 0:
                del lines[i]
            elif op == 1:
                lines.insert(i, line)  # a duplicate key or element
            elif op == 2:
                j = rng.randrange(len(lines))
                lines[i], lines[j] = lines[j], lines[i]
            elif op == 3:
                lines[i] = "  " + line
            elif op == 4 and ":" in line:
                rest = line.split(":", 1)[1]
                lines[i] = indent + rng.choice(keys) + ":" + rest
            elif op == 5 and ":" in line:
                lines[i] = line.split(":", 1)[0] + ": " + rng.choice(values)
            elif op == 6:
                lines.insert(
                    i, indent + rng.choice(keys) + ": " + rng.choice(values)
                )
            else:
                lines.insert(i, indent + rng.choice(keys) + ":")
        return "\n".join(lines)

    expected, actual = _configcli_both_outcomes(_CONFIGCLI_PARITY_DOC)
    assert expected[0] == "ok", expected
    assert actual == expected

    kinds = {}
    for _ in range(60):
        text = mutate(_CONFIGCLI_PARITY_DOC)
        expected, actual = _configcli_both_outcomes(text)
        assert actual == expected, text
        kind = expected[1] if expected[0] == "error" else "ok"
        kinds[kind] = kinds.get(kind, 0) + 1
    # the sweep has to reach the schema's own errors and some documents
    # that still load, or it compared nothing but YAML syntax errors
    assert kinds.get("YAMLValidationError", 0) >= 10, kinds
    assert kinds.get("DuplicateKeysDisallowed", 0) >= 1, kinds
    assert kinds.get("ok", 0) >= 5, kinds


_CONFIGCLI_BAD_DOCUMENTS = {
    "unknown key in a job": """\
        jobs:
          - name: alpha
            command: echo alpha
            schedule: '*/5 * * * *'
            bogus: 1
        """,
    "wrong scalar deep in a report": """\
        jobs:
          - name: alpha
            command: echo alpha
            schedule: '*/5 * * * *'
            onFailure:
              report:
                mail:
                  from: a@example.com
                  to: b@example.com
                  smtpPort: not-a-port
        """,
    "missing required key": """\
        jobs:
          - name: alpha
            schedule: '*/5 * * * *'
        """,
    "duplicate key": """\
        jobs:
          - name: alpha
            command: echo alpha
            command: echo again
            schedule: '*/5 * * * *'
        """,
    "duplicate key in a pattern mapping": """\
        pools:
          db:
            slots: 1
          db:
            slots: 2
        """,
    "number as a fixed key": """\
        jobs:
          - name: alpha
            command: echo alpha
            schedule: '*/5 * * * *'
            1: one
        """,
    "boolean and null keys in a pattern mapping": """\
        pools:
          true:
            slots: 1
          null:
            slots: 2
          3:
            slots: three
        """,
    "error under the last of many pattern keys": "pools:\n"
    + "".join("  pool%03d:\n    slots: 4\n" % i for i in range(40))
    + "  last:\n    slots: 4\n    burst: 1\n",
    "complex key": """\
        pools:
          ? - a
            - b
          : slots: 1
        """,
    "mapping where a sequence belongs": """\
        jobs:
          name: alpha
          command: echo alpha
        """,
    "scalar where a mapping belongs": """\
        web:
          listen:
            - http://127.0.0.1:8080
          headers: plain
        """,
    "unknown key in the free-form logging section": """\
        logging:
          version: 1
          handlers:
            console:
              class: logging.StreamHandler
          bogus: 1
        """,
}


@pytest.mark.parametrize("case", sorted(_CONFIGCLI_BAD_DOCUMENTS))
def test_direct_key_lookup_reports_the_same_error_as_the_scan(case):
    # The message a user reads, through parse_config_string: identical
    # under upstream's scan and under the direct lookup, line, column and
    # quoted snippet included.
    from strictyaml.yamlpointer import YAMLPointer

    from cronstable.config import parse_config_string

    text = textwrap.dedent(_CONFIGCLI_BAD_DOCUMENTS[case])
    installed = _configcli_installed_lookup()

    def failure():
        with pytest.raises(Exception) as err:  # noqa: B017, PT011
            parse_config_string(text, "test.yaml")
        return type(err.value).__name__, str(err.value)

    try:
        YAMLPointer._individual_get = _configcli_upstream_individual_get()
        expected = failure()
    finally:
        YAMLPointer._individual_get = installed
    assert failure() == expected
    # A complex key trips strictyaml's own assertion that a key is a
    # string before any lookup runs; every other case is a ConfigError
    # with a message.
    if case == "complex key":
        assert expected[0] == "AssertionError"
    else:
        assert expected[0] == "ConfigError"
        assert expected[1].strip(), case
    # and the raw strictyaml outcome, exception type included
    upstream_outcome, direct_outcome = _configcli_both_outcomes(text)
    assert upstream_outcome[0] == "error", case
    assert direct_outcome == upstream_outcome


def test_key_lookup_shim_stands_down_on_an_unknown_lookup_shape():
    # config._patch_strictyaml_key_lookup rebinds only a method that still
    # has upstream's scanning shape: five arguments, and a body that walks
    # `items` and reads `text`.  A strictyaml that resolves the hop some
    # other way is left alone, and so is a second call.
    from strictyaml.yamlpointer import YAMLPointer

    from cronstable import config

    installed = _configcli_installed_lookup()
    upstream = _configcli_upstream_individual_get()

    def reworked(self, segment, index_type, index, strictdoc):
        if index_type == "val":
            return segment[index[0]]
        return upstream(self, segment, index_type, index, strictdoc)

    def rearranged(self, segment, index_type, index):
        for key, value in segment.items():
            if key == index[0] or getattr(key, "text", None) == index[0]:
                return value
        raise Exception("Invalid state")

    try:
        for foreign in (reworked, rearranged):
            YAMLPointer._individual_get = foreign
            config._patch_strictyaml_key_lookup()
            assert YAMLPointer._individual_get is foreign, foreign.__name__

        YAMLPointer._individual_get = upstream
        config._patch_strictyaml_key_lookup()
        rebound = YAMLPointer._individual_get
        assert rebound is not upstream
        assert "items" not in rebound.__code__.co_names
        config._patch_strictyaml_key_lookup()
        assert YAMLPointer._individual_get is rebound
    finally:
        YAMLPointer._individual_get = installed


def test_a_second_import_of_config_drives_the_installed_key_lookup():
    # Importing the module runs
    # `_STRICTYAML_KEY_LOOKUP = _patch_strictyaml_key_lookup()`.  The
    # installed lookup carries the flag it reads, so that call hands a
    # second import (importlib.reload, a purged sys.modules) the same flag,
    # and its parse_config_string switches the lookup on.
    from cronstable import config

    installed = _configcli_installed_lookup()
    assert installed.load is config._STRICTYAML_KEY_LOOKUP
    assert (
        config._patch_strictyaml_key_lookup() is config._STRICTYAML_KEY_LOOKUP
    )


def test_direct_key_lookup_is_scoped_to_the_config_schema_load(monkeypatch):
    # The direct lookup is sound because CONFIG_SCHEMA validates keys with
    # Str() alone, so it is on only while parse_config_string runs
    # strictyaml, on the calling thread, and off again when that load
    # returns or raises.  Any other strictyaml load gets upstream's scan.
    import threading

    import strictyaml
    from strictyaml.ruamel.comments import CommentedMapItemsView

    from cronstable import config

    _configcli_installed_lookup()
    state = config._STRICTYAML_KEY_LOOKUP
    text = _CONFIGCLI_WIDE_MAPPINGS["pools"]

    def direct():
        return getattr(state, "direct", False)

    during = []
    elsewhere = []
    real_load = strictyaml.load

    def recording(*args, **kwargs):
        during.append(direct())
        thread = threading.Thread(target=lambda: elsewhere.append(direct()))
        thread.start()
        thread.join()
        return real_load(*args, **kwargs)

    monkeypatch.setattr(strictyaml, "load", recording)
    assert direct() is False
    config.parse_config_string(text, "test")
    with pytest.raises(config.ConfigError):
        config.parse_config_string("jobs:\n  - bogus: 1\n", "test")
    assert during == [True, True]
    assert elsewhere == [False, False]
    assert direct() is False
    monkeypatch.undo()

    # outside parse_config_string the same document is scanned
    steps = [0]
    stock_iter = CommentedMapItemsView.__iter__

    def counting(self):
        for item in stock_iter(self):
            steps[0] += 1
            yield item

    monkeypatch.setattr(CommentedMapItemsView, "__iter__", counting)
    data = strictyaml.load(text, config.CONFIG_SCHEMA).data
    assert steps[0] > 10 * _configcli_mapping_entries(data)


def _configcli_schema_validators():
    """Every validator reachable from CONFIG_SCHEMA, each one once."""
    from cronstable.config import CONFIG_SCHEMA

    seen = set()
    found = []
    stack = [CONFIG_SCHEMA]
    while stack:
        validator = stack.pop()
        if id(validator) in seen:
            continue
        seen.add(id(validator))
        found.append(validator)
        for attr in (
            "_validator_a",
            "_validator_b",
            "_key_validator",
            "_value_validator",
            "_item_validator",
        ):
            child = getattr(validator, attr, None)
            if child is not None:
                stack.append(child)
        inner = getattr(validator, "_validator", None)
        if isinstance(inner, dict):
            stack.extend(inner.values())
        elif inner is not None:
            stack.append(inner)
    return found


def test_config_schema_validates_every_mapping_key_with_str():
    # What makes the direct key lookup exact: a Str()-validated key keeps
    # its source text, so one key of a mapping, and only one, can match a
    # pointer.  A key validator that rewrites the key (or a typed one such
    # as Int()) would need the lookup rethought before it joins the schema.
    from strictyaml import Map, MapPattern, Str

    mappings = [
        validator
        for validator in _configcli_schema_validators()
        if isinstance(validator, (Map, MapPattern))
    ]
    assert len(mappings) >= 40, len(mappings)
    assert any(isinstance(v, MapPattern) for v in mappings)
    offenders = [
        repr(validator)[:80]
        for validator in mappings
        if type(validator.key_validator) is not Str
    ]
    assert offenders == [], (
        "CONFIG_SCHEMA validates the keys of these mappings with something "
        "other than Str(): %s" % offenders
    )


def test_config_schema_has_no_defaulted_optional():
    # strictyaml fills an Optional(..., default=...) by forking the chunk,
    # which deep-copies the WHOLE document once per mapping that omits the
    # key: one defaulted key in the job schema took 0.26s at 100 jobs and
    # 5.35s at 400.  cronstable applies its defaults after the load
    # (mergedicts over DEFAULT_CONFIG), so the schema needs none.
    from strictyaml import Map, Optional

    mappings = [
        validator
        for validator in _configcli_schema_validators()
        if isinstance(validator, Map)
    ]
    assert len(mappings) >= 40, len(mappings)
    optional = 0
    for validator in mappings:
        assert validator._defaults == {}, repr(validator)[:80]
        for key in validator._validator:
            if isinstance(key, Optional):
                optional += 1
                assert key.default is None, key.key
    assert optional >= 100, optional


# --- one env_file read per parsed document -----------------------------------
#
# An env_file commonly reaches every job and DAG task of a document through
# `defaults:`.  The parse reads it once and hands each JobConfig a copy of
# the result, so the read COUNT is one per document whatever the job and
# task counts are.  A count because the cost of a read is the filesystem's
# (a config on a network mount pays milliseconds per open), and a timing
# on a local disk would never see it.


def test_env_file_is_opened_once_per_document(tmp_path, monkeypatch):
    import builtins

    from cronstable.config import parse_config_string

    env = tmp_path / "shared.env"
    env.write_text("SHARED=1\nOTHER=2\n", encoding="utf-8")
    lines = ["defaults:", "  env_file: %s" % env, "jobs:"]
    for i in range(40):
        lines += [
            "  - name: job%02d" % i,
            "    command: echo %d" % i,
            "    schedule: '0 * * * *'",
        ]
    lines.append("dags:")
    for d in range(2):
        lines += ["  - name: dag%d" % d, "    tasks:"]
        for i in range(20):
            lines += ["      - id: t%02d" % i, "        command: echo %d" % i]
            if i % 2:
                lines.append("        env_file: %s" % env)
    opens = []
    real_open = builtins.open

    def counting(file, *args, **kwargs):
        if isinstance(file, str) and os.path.basename(file) == "shared.env":
            opens.append(file)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", counting)
    conf = parse_config_string("\n".join(lines) + "\n", "test")
    monkeypatch.undo()

    holders = list(conf.jobs)
    for dag in conf.dags:
        holders.extend(dag.task_templates.values())
    assert len(holders) == 80
    assert all(
        {e["key"] for e in holder.environment} == {"SHARED", "OTHER"}
        for holder in holders
    )
    assert len(opens) == 1, (
        "one document with 40 jobs and 40 DAG tasks sharing an env_file "
        "opened it %d times; the per-document cache in _config_from_doc "
        "must reach every JobConfig the document builds" % len(opens)
    )


# --- a warm load reparses nothing --------------------------------------------
#
# The per-file parse cache holds every file of the configuration being
# loaded, so a second load of an unchanged directory runs the parser zero
# times at any size.  The invariant is that COUNT.  It is checked on both
# sides of _DIR_FILE_CACHE_MAX because a cache that evicts by a fixed size
# during a sorted scan drops each entry just before the scan reaches it,
# which reparses EVERY file once the directory outgrows the bound.


@pytest.mark.parametrize("files", [1000, 1100])
def test_unchanged_warm_load_reparses_no_file(tmp_path, monkeypatch, files):
    from collections import OrderedDict

    from cronstable import config

    assert config._DIR_FILE_CACHE_MAX == 1024
    cache = OrderedDict()
    monkeypatch.setattr(config, "_DIR_FILE_CACHE", cache)
    parsed = []

    def stand_in(path, _seen=None, _sources=None):
        # parse_config_file's bookkeeping without the YAML parse, which is
        # 0.5ms a file and not what this counts
        abspath = os.path.abspath(path)
        if _sources is not None:
            _sources.add(abspath)
        if _seen is not None:
            _seen.add(abspath)
        parsed.append(abspath)
        return config.CronstableConfig(
            jobs=[],
            web_config=None,
            job_defaults=config.JobDefaults({}),
            logging_config=None,
        )

    monkeypatch.setattr(config, "parse_config_file", stand_in)
    for i in range(files):
        with open(str(tmp_path / ("job-%04d.yaml" % i)), "w") as handle:
            handle.write("jobs:\n")

    config.parse_config(str(tmp_path))
    assert len(parsed) == files
    assert len(cache) == files

    del parsed[:]
    config.parse_config(str(tmp_path))
    assert parsed == [], (
        "a second load of an unchanged %d-file directory reparsed %d "
        "files" % (files, len(parsed))
    )
    assert len(cache) == files

    # one edit costs one parse
    with open(str(tmp_path / "job-0500.yaml"), "w") as handle:
        handle.write("jobs:\n# edited\n")
    config.parse_config(str(tmp_path))
    assert [os.path.basename(path) for path in parsed] == ["job-0500.yaml"]


# --- one read per source per load --------------------------------------------
#
# A cache entry is signed with the bytes its parse read, so a cold load
# opens each file once however deep the include tree is.  A warm load opens
# each file once to validate it.  A count, for the reason the env_file one
# above is.


def test_a_load_opens_each_config_file_once(tmp_path, monkeypatch):
    import builtins
    from collections import Counter, OrderedDict

    from cronstable import config

    monkeypatch.setattr(config, "_DIR_FILE_CACHE", OrderedDict())
    job = (
        "jobs:\n  - name: job-%d-%d\n    command: echo x\n"
        "    schedule: '0 3 * * *'\n"
    )
    confdir = tmp_path / "conf"
    leaves = confdir / "leaves"
    leaves.mkdir(parents=True)
    names = []
    for m in range(3):
        for i in range(4):
            name = "leaf-%d-%d.yaml" % (m, i)
            (leaves / name).write_text(job % (m, i))
            names.append(name)
        mid = "mid-%d.yaml" % m
        (leaves / mid).write_text(
            "include:\n"
            + "".join("  - leaf-%d-%d.yaml\n" % (m, i) for i in range(4))
        )
        names.append(mid)
        top = "top-%d.yaml" % m
        (confdir / top).write_text("include:\n  - leaves/%s\n" % mid)
        names.append(top)
    opens = Counter()
    real_open = builtins.open

    def counting(file, *args, **kwargs):
        if isinstance(file, str) and file.endswith(".yaml"):
            opens[os.path.basename(file)] += 1
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", counting)
    cold = config.parse_config(str(confdir))
    cold_opens = dict(opens)
    opens.clear()
    warm = config.parse_config(str(confdir))
    warm_opens = dict(opens)
    monkeypatch.undo()

    assert len(cold.jobs) == len(warm.jobs) == 12
    once = dict.fromkeys(names, 1)
    assert cold_opens == once, (
        "a cold load opened a file more than once; a cache entry must be "
        "signed from the read its parse made: %r"
        % sorted(n for n, c in cold_opens.items() if c != 1)
    )
    assert warm_opens == once


# --- the thin clients stay thin ----------------------------------------------
#
# `cronstable state get` and its siblings run inside jobs, once per call,
# so their cold start is paid by every script that uses them.  What keeps
# it small is the import graph: the job client and its dispatch path load
# the web client and the argument leaf and nothing else of cronstable, and
# never asyncio, aiohttp or the YAML parser.  A count of modules, taken in
# a child because this process has long since imported everything.


def _configcli_child(code, env=None):
    done = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env
    )
    assert done.returncode == 0, done.stderr
    return done.stdout.strip().splitlines()[-1]


_CONFIGCLI_LOADED = (
    "print(sorted(m for m in sys.modules if m.startswith('cronstable.') "
    "or m in ('asyncio', 'aiohttp', 'strictyaml')))"
)


def test_importing_jobcli_loads_only_the_client_modules():
    out = _configcli_child(
        "import sys, cronstable.jobcli; " + _CONFIGCLI_LOADED
    )
    assert out == str(
        ["cronstable._cliargs", "cronstable.jobcli", "cronstable.webclient"]
    )


def test_state_get_dispatch_loads_only_the_client_modules():
    # The whole path a job-side call walks: the entry module, the parser
    # build, the dispatch branch and the verb, up to the point where it
    # finds no endpoint in the environment and exits 1.
    code = textwrap.dedent(
        """
        import sys

        sys.argv = ["cronstable", "state", "get", "k"]
        import cronstable.__main__ as main

        try:
            main.main_loop()
        except SystemExit as stop:
            print("EXIT", stop.code)
        """
    )
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("CRONSTABLE_STATE_")
    }
    done = subprocess.run(
        [sys.executable, "-c", code + _CONFIGCLI_LOADED],
        capture_output=True,
        text=True,
        env=env,
    )
    assert done.returncode == 0, done.stderr
    lines = done.stdout.strip().splitlines()
    assert lines[-2] == "EXIT 1", done.stdout + done.stderr
    assert lines[-1] == str(
        [
            "cronstable.__main__",
            "cronstable._cliargs",
            "cronstable.jobcli",
            "cronstable.platform",
            "cronstable.version",
            "cronstable.webclient",
        ]
    )


def test_importing_taskxml_loads_the_standard_library_only():
    # `cronstable import-taskscheduler` converts an estate on any machine,
    # so the converter depends on nothing a bare interpreter lacks.
    code = (
        "import sys; before = set(sys.modules); import cronstable.taskxml; "
        "print(sorted(m for m in set(sys.modules) - before "
        "if m.split('.')[0] not in sys.stdlib_module_names))"
    )
    assert _configcli_child(code) == str(["cronstable", "cronstable.taskxml"])


# --- one store read per pool per poll, and no write ------------------------
#
# The web page polls GET /pools on every cycle and each GET /jobs build
# attaches the pool queues, so both take a pool snapshot per poll.  A
# snapshot of a pool nothing has changed must cost one document read per
# configured pool and leave the store alone: a write per poll would fsync
# on a timer for every open dashboard.

_HTTPAPI_POOLS_YAML = """
pools:
  database:
    slots: 2
  network:
    slots: 4
jobs:
  - name: one
    command: ignored
    schedule: "@reboot"
    pool: database
  - name: two
    command: ignored
    schedule: "@reboot"
    pool: network
"""


async def test_pool_poll_reads_each_pool_once_and_writes_nothing(
    dag_cron, monkeypatch
):
    from cronstable.pools import NAMESPACE
    from cronstable.state import DOC_KEEP

    cron = await dag_cron(
        _HTTPAPI_POOLS_YAML,
        extra_state="  jobApi:\n    enabled: false\n",
        web=True,
    )
    # the queue service would admit and launch the entry enqueued below
    monkeypatch.setattr(cron._pools, "service", lambda: None)
    await cron._pools.enqueue_job(cron.cron_jobs["one"], manual=True)
    # the first snapshot of a pool nothing has used creates its document
    await cron._pools.snapshot()

    backend = cron.state_backend
    reads = []
    kept = []
    real_mutate = backend.mutate_document

    async def spying_mutate(namespace, key, transform):
        def spying_transform(current):
            new_body, result = transform(current)
            kept.append(new_body is DOC_KEEP)
            return new_body, result

        reads.append((namespace, key))
        return await real_mutate(namespace, key, spying_transform)

    fsyncs = []
    real_fsync = os.fsync

    def counting_fsync(fd):
        fsyncs.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(backend, "mutate_document", spying_mutate)
    monkeypatch.setattr(os, "fsync", counting_fsync)
    pools = [(NAMESPACE, "database"), (NAMESPACE, "network")]
    for poll, path in (
        (cron._web_pools, "/pools"),
        (cron._web_list_jobs, "/jobs"),
    ):
        del reads[:], kept[:]
        resp = await poll(Req())
        assert resp.status == 200
        assert reads == pools, (
            "GET %s read the pool documents %r; one read per configured "
            "pool is the whole budget" % (path, reads)
        )
        assert kept == [True, True], (
            "GET %s rewrote an unchanged pool document" % path
        )
    assert fsyncs == [], "a dashboard poll synced a file to disk"


# --- the dashboard page is compressed once per process ---------------------
#
# The page is static package data compressed with zlib at level 9, which
# costs milliseconds of CPU, so a worker thread does it off the scheduler's
# loop.  The first gzip-capable request pays it, and the requests that
# arrive with it wait for its result; every later request, and every
# request that does not take the compressed body, pays nothing.  The count
# covers both compressors the module can open: its own zlib and the
# response backend.


async def test_dashboard_page_is_compressed_once_per_process(stand_in_isal):
    from cronstable import cron as cron_mod

    backend, stdlib = stand_in_isal
    cron = Cron(
        None,
        config_yaml=(
            'jobs:\n  - name: j\n    command: x\n    schedule: "0 0 * * *"\n'
        ),
    )
    cron.web_config = {}
    cron_mod._index_gzip.cache_clear()
    try:
        etag = cron_mod._index_document()[1]
        plain = await cron._web_index(Req())
        revalidated = await cron._web_index(
            Req(headers={"If-None-Match": etag, "Accept-Encoding": "gzip"})
        )
        assert plain.status == 200 and revalidated.status == 304
        assert backend.levels == stdlib.levels == [], (
            "the page was compressed for a request that takes no "
            "compressed body"
        )

        def load():
            return cron._web_index(Req(headers={"Accept-Encoding": "gzip"}))

        # four loads that arrive together, then one more
        loads = await asyncio.gather(*(load() for _ in range(4)))
        loads.append(await load())
        for resp in loads:
            assert resp.headers["Content-Encoding"] == "gzip"
        assert (backend.levels, stdlib.levels) == ([], [9]), (
            "five page loads opened the compressors at levels %r and %r; "
            "the page is static and must be compressed once by zlib"
            % (backend.levels, stdlib.levels)
        )
        assert len({bytes(resp.body) for resp in loads}) == 1
    finally:
        cron_mod._index_gzip.cache_clear()


# --- runs register with the job API; they never start it -------------------
#
# The loopback state API binds one listener when the state backend starts.
# A launch mints a token and registers it with that listener.  A listener
# per run would cost a socket, an aiohttp application and a bind for every
# fire of every job.

_HTTPAPI_RUN_YAML = """
jobs:
  - name: j
    command: ignored
    schedule: "@reboot"
"""


async def test_launching_runs_starts_no_listener(dag_cron, monkeypatch):
    from cronstable.jobapi import JobStateAPI

    starts = []
    real_start = JobStateAPI.start

    async def counting_start(self):
        starts.append(self)
        return await real_start(self)

    monkeypatch.setattr(JobStateAPI, "start", counting_start)
    cron = await dag_cron(_HTTPAPI_RUN_YAML)
    assert len(starts) == 1, "the state backend start binds the listener"
    api = cron._job_api
    assert api is not None and api.base_url

    job = cron.cron_jobs["j"]
    job.command = [sys.executable, "-c", "pass"]
    tokens = set()
    for _ in range(3):
        await cron.maybe_launch_job(job)
        (running,) = cron.running_jobs[job.name]
        # the run reached the API: without a token the count below would
        # hold for a launch path that skipped the API altogether
        assert running.state_token in api._runs
        tokens.add(running.state_token)
        await _reap_running(cron)
    assert len(tokens) == 3
    assert api._runs == {}, "a finished run left its token registered"
    assert len(starts) == 1, (
        "launching %d runs started the job API %d more time(s)"
        % (len(tokens), len(starts) - 1)
    )
    assert cron._job_api is api


# --- a denied semaphore acquire probes each permit once --------------------
#
# A semaphore permit is one lease.  An acquire tries the permits in order
# and stops at the first one it wins, so a saturated semaphore costs exactly
# ``permits`` lease attempts per pass.  A waiting acquire repeats the pass
# on a timer, so an extra attempt per permit multiplies by every waiter.


async def test_denied_semaphore_acquire_probes_each_permit_once(fs_backend):
    from cronstable.jobapi import JobStateAPI, RunContext

    api = JobStateAPI(
        lambda: fs_backend, base_holder="h#proc", config={"lockTtlSeconds": 30}
    )

    def register(token):
        ctx = RunContext(
            token=token,
            run_id="rid-" + token,
            job_name="job-" + token,
            attempt=0,
            scheduled_at=None,
            host="h",
            default_scope="job-" + token,
        )
        api.register_run(ctx)
        return ctx

    holder = register("holder")
    waiter = register("waiter")
    permits = 5
    try:
        holds = []
        for _ in range(permits):
            got = await api.locks.acquire(
                holder.token, "global", "sem", permits=permits
            )
            assert got["acquired"]
            holds.append(got)

        attempts = []
        real_acquire = fs_backend.acquire_lease

        async def counting_acquire(name, lease_holder, ttl):
            attempts.append(name)
            return await real_acquire(name, lease_holder, ttl)

        fs_backend.acquire_lease = counting_acquire  # type: ignore[method-assign]

        denied = await api.locks.acquire(
            waiter.token, "global", "sem", permits=permits
        )
        assert denied == {"acquired": False}
        assert len(attempts) == permits, (
            "a denied acquire on a %d-permit semaphore made %d lease "
            "attempts" % (permits, len(attempts))
        )
        assert len(set(attempts)) == permits  # each permit once

        # the healthy direction: the pass stops at the first free permit
        freed = holds[2]
        assert await api.locks.release(holder.token, freed["token"])
        del attempts[:]
        won = await api.locks.acquire(
            waiter.token, "global", "sem", permits=permits
        )
        assert won["acquired"] and won["slot"] == freed["slot"]
        assert len(attempts) == freed["slot"] + 1
    finally:
        await api.stop()


async def test_mcp_list_jobs_builds_one_row_per_returned_job(monkeypatch):
    # cron_list_jobs chooses its page from each job's name, enabled flag and
    # live-run list, then builds a full row for every job on that page. The
    # row is the expensive part and the page is a small share of a large
    # job set, so the count of rows built must equal the count returned,
    # whatever the filters and wherever the page starts.
    from cronstable.config import _build_mcp_config
    from cronstable.cron import Cron
    from cronstable.mcp import MCPHandler

    jobs = 120
    lines = ["jobs:"]
    for i in range(jobs):
        lines.append("  - name: job%03d" % i)
        lines.append("    command: echo %d" % i)
        lines.append('    schedule: "*/5 * * * *"')
        if i % 3 == 0:
            lines.append("    enabled: false")
    cron = Cron(None, config_yaml="\n".join(lines) + "\n")
    cron.web_config = {}
    handler = MCPHandler(
        cron, _build_mcp_config({"enabled": True, "maxRows": 50})
    )
    built = []
    real_row = cron._job_to_dict

    def counting_row(name, job, now=None):
        built.append(name)
        return real_row(name, job, now)

    monkeypatch.setattr(cron, "_job_to_dict", counting_row)

    async def listing(arguments):
        built.clear()
        reply = await handler.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "cron_list_jobs", "arguments": arguments},
            }
        )
        body = reply["result"]["structuredContent"]
        assert [row["name"] for row in body["jobs"]] == built
        return body["page"]["total"], list(built)

    def names(*indexes):
        return ["job%03d" % i for i in indexes]

    assert await listing({"limit": 5}) == (jobs, names(0, 1, 2, 3, 4))
    assert await listing({"offset": 100, "limit": 3}) == (
        jobs,
        names(100, 101, 102),
    )
    assert await listing({"state": "disabled", "limit": 4}) == (
        40,
        names(0, 3, 6, 9),
    )
    assert await listing({"filter": "JOB11", "state": "scheduled"}) == (
        7,
        names(110, 112, 113, 115, 116, 118, 119),
    )
    assert await listing({"filter": "no-such-job"}) == (0, [])
    # with no limit the page is maxRows rows, and that is all it builds
    total, page = await listing({})
    assert (total, len(page)) == (jobs, 50)


async def test_all_304_gossip_round_parses_no_body(monkeypatch):
    # A peer whose /peer content is unchanged answers the echoed ETag with a
    # bodyless 304, and the poller replays the observation it already holds.
    # The round then costs one GET per peer and no decode: nothing reads a
    # body, parses JSON or validates a summary block. A live field leaking
    # into the ETag turns every round into a full one whose results are
    # identical, so a behavior test cannot see it.
    import json

    from cronstable import cluster as cluster_mod
    from cronstable.config import DEFAULT_CLUSTER

    monkeypatch.setattr(
        cluster_mod, "build_client_ssl_context", lambda tls: None
    )
    monkeypatch.setattr(
        cluster_mod, "build_server_ssl_context", lambda tls: None
    )
    names = ["node-%d" % i for i in range(5)]
    hosts = ["%s.test:1" % name for name in names[1:]]
    config = dict(DEFAULT_CLUSTER)
    config.update(
        {
            "nodeName": names[0],
            "listen": "127.0.0.1:1",
            "tls": {"ca": "ca", "cert": "cert", "key": "key"},
            "peers": [{"host": host} for host in hosts],
            "interval": 3600,
            "connectTimeout": 5,
        }
    )
    mgr = cluster_mod.ClusterManager(config, lambda: "v1:jobs")

    def body_of(index):
        return json.dumps(
            {
                "node_name": names[index],
                "job_set_id": "v1:jobs",
                "scheme_version": cluster_mod.SCHEME_VERSION,
                "instance_id": "instance-%d" % index,
                "cluster_size": len(names),
                "members": [
                    {
                        "node_name": names[0],
                        "instance_id": mgr.instance_id,
                        "agreed": True,
                    }
                ],
                "mutual_agreeing": [names[0]],
                "quorate_vouched": [],
                "ran_reboot_jobs": [],
                "job_summaries": {
                    "job-%d" % j: {
                        "running": False,
                        "enabled": True,
                        "scheduled_in": 60.0,
                        "last": None,
                    }
                    for j in range(20)
                },
                "job_summaries_truncated": False,
            }
        ).encode("utf-8")

    bodies = {host: body_of(i) for i, host in enumerate(hosts, start=1)}

    class Response:
        def __init__(self, status, body, etag):
            self.status = status
            self.headers = {"ETag": etag}
            self.content = self
            self._body = body

        def raise_for_status(self):
            pass

        async def iter_chunked(self, size):
            yield self._body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

    class Session:
        def __init__(self):
            self.answered = []

        def get(self, url, headers=None, **_kwargs):
            host = url.split("//", 1)[-1].split("/", 1)[0]
            etag = '"tag-%s"' % host
            if (headers or {}).get("If-None-Match") == etag:
                self.answered.append((host, 304))
                return Response(304, b"", etag)
            self.answered.append((host, 200))
            return Response(200, bodies[host], etag)

    session = Session()
    mgr._session = session
    counts = {"read": 0, "decode": 0, "summaries": 0, "members": 0}

    def counted(key, real):
        def wrapper(*args, **kwargs):
            counts[key] += 1
            return real(*args, **kwargs)

        return wrapper

    monkeypatch.setattr(
        cluster_mod, "_read_capped", counted("read", cluster_mod._read_capped)
    )
    monkeypatch.setattr(
        cluster_mod._json, "loads", counted("decode", cluster_mod._json.loads)
    )
    monkeypatch.setattr(
        cluster_mod,
        "_parse_job_summaries",
        counted("summaries", cluster_mod._parse_job_summaries),
    )
    monkeypatch.setattr(
        cluster_mod,
        "_parse_members",
        counted("members", cluster_mod._parse_members),
    )

    # the first round has no tag to echo: every peer sends its full body
    await mgr._poll_all()
    assert sorted(session.answered) == sorted((host, 200) for host in hosts)
    peers = len(hosts)
    assert counts == {
        "read": peers,
        "decode": peers,
        "summaries": peers,
        "members": peers,
    }
    held = {host: mgr.view.peers[host].job_summaries for host in hosts}
    seen = {host: mgr.view.peers[host].last_seen for host in hosts}
    assert all(len(block) == 20 for block in held.values())

    session.answered.clear()
    for key in counts:
        counts[key] = 0
    await mgr._poll_all()
    assert sorted(session.answered) == sorted((host, 304) for host in hosts)
    assert counts == {"read": 0, "decode": 0, "summaries": 0, "members": 0}
    # the 304 still counts as a fresh observation of the same content
    for host in hosts:
        peer = mgr.view.peers[host]
        assert peer.status == cluster_mod.STATUS_AGREED
        assert peer.job_summaries is held[host]
        assert peer.last_seen >= seen[host]


async def test_push_alert_burst_reads_the_registry_once(monkeypatch):
    # The reporting path trusts the in-memory device mirror for
    # REGISTRY_REFRESH_SECONDS, so a burst of alerts inside that window
    # costs the store nothing after the read that filled the mirror. Each
    # alert opens one client session for all of its devices, seals once per
    # device and posts once per device. A per-alert registry read would
    # serialize an incident's alerts behind store I/O, and a per-device
    # session would repeat the TLS handshake for every phone.
    from types import SimpleNamespace

    import aiohttp

    from cronstable import push

    devices = [
        {
            "id": "device-%d" % i,
            "name": "phone %d" % i,
            "platform": "ios",
            "pushToken": "token-%d" % i,
            "publicKey": "key-%d" % i,
            "suite": "x25519",
        }
        for i in range(3)
    ]

    class Store:
        loads = 0

        def describe(self):
            return "memory"

        async def load(self):
            self.loads += 1
            return [dict(device) for device in devices]

        async def ensure_salt(self):
            return "salt"

    sealed = []
    monkeypatch.setattr(
        push,
        "seal_to_device",
        lambda key, plaintext, suite=push.DEFAULT_SUITE: (
            sealed.append(key) or "ciphertext"
        ),
    )
    sessions = []
    real_session = aiohttp.ClientSession

    def counting_session(*args, **kwargs):
        sessions.append(1)
        return real_session(*args, **kwargs)

    monkeypatch.setattr(aiohttp, "ClientSession", counting_session)
    store = Store()
    service = push.PushService(
        relay_url="http://127.0.0.1:1/unused",
        relay_timeout=5.0,
        store=store,
        host="node-a",
    )
    posted = []

    async def post(session, body, outcome):
        outcome["error"] = None
        outcome["status"] = 200
        posted.append((session, body["device"]))

    monkeypatch.setattr(service, "_post_envelope", post)
    # the read that fills the mirror, as PushService.start does at boot
    await service.refresh(force=True)
    assert store.loads == 1

    ctx = SimpleNamespace(
        template_vars={
            "name": "nightly",
            "host": "node-a",
            "exit_code": 1,
            "fail_reason": "exited with status 1",
            "stderr": "boom",
        }
    )
    alerts = 25
    for _ in range(alerts):
        await service.send_report(ctx, False, {"enabled": True})
    assert store.loads == 1
    assert len(sessions) == alerts
    assert len(sealed) == alerts * len(devices)
    assert len(posted) == alerts * len(devices)
    # every device of one alert rode that alert's single session
    tokens = sorted(device["pushToken"] for device in devices)
    for first in range(0, len(posted), len(devices)):
        alert = posted[first : first + len(devices)]
        assert len({id(session) for session, _token in alert}) == 1
        assert sorted(token for _session, token in alert) == tokens
    assert len({id(session) for session, _token in posted}) == alerts


async def test_mcp_tools_list_serves_the_prebuilt_listings(monkeypatch):
    # A client asks for tools/list at the start of every session. The
    # handler builds each tool's listing (description, input schema, output
    # schema) once, when it is constructed, and tools/list hands those same
    # objects back, so a call assembles no schema.
    from cronstable import mcp as mcp_mod
    from cronstable.config import _build_mcp_config
    from cronstable.cron import Cron

    cron = Cron(
        None,
        config_yaml=(
            "jobs:\n"
            "  - name: hello\n"
            "    command: echo hi\n"
            '    schedule: "* * * * *"\n'
        ),
    )
    handler = mcp_mod.MCPHandler(
        cron,
        _build_mcp_config(
            {
                "enabled": True,
                "readOnly": False,
                "toolsets": ["observe", "dags", "state", "act"],
            }
        ),
    )
    prebuilt = {tool["name"]: tool["listing"] for tool in handler._tools}

    def no_build(*args, **kwargs):
        raise AssertionError("tools/list assembled a schema per call")

    for factory in ("_tool", "_obj_schema", "_enum", "_nullable"):
        monkeypatch.setattr(mcp_mod, factory, no_build)
    for _ in range(2):
        reply = await handler.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        )
        tools = reply["result"]["tools"]
        assert len(tools) == len(prebuilt)
        for tool in tools:
            assert tool is prebuilt[tool["name"]]


def _tui_jobs(n):
    """``n`` enabled, idle jobs in the shape of a ``GET /jobs`` row."""
    return [
        {
            "name": "job-%05d" % i,
            "enabled": True,
            "schedule": "*/5 * * * *",
            "command": "run --shard %d" % i,
            "running": False,
            "scheduled_in": 30.0 + i,
            "last_run": None,
            "history": [],
            "paused": None,
        }
        for i in range(n)
    ]


class _TuiWire:
    """A stream that keeps what ``Term`` writes, one entry per frame."""

    def __init__(self):
        self.frames = []

    def write(self, data):
        self.frames.append(data)

    def flush(self):
        pass


def _tui_app(tmp_path, jobs, cols=120, lines=40, api=None):
    """A dashboard over ``jobs`` with no daemon and no running loops, which
    paints through the real ``Term`` at a fixed size into a ``_TuiWire``."""

    class FixedTerm(tui.Term):
        def size(self):
            return (cols, lines)

    wire = _TuiWire()
    app = tui.TuiApp(
        api or tui.Api("http://127.0.0.1:1", None),
        FixedTerm(stream=wire),
        tui.ScriptedKeys(),
        dict(tui.PREF_DEFAULTS),
        boot=False,
        prefs_file=str(tmp_path / "prefs.json"),
    )
    app.jobs = jobs
    app.by_name = {job["name"]: job for job in jobs}
    app.connected = True
    app.fetched_mono = time.monotonic()
    app.recompute_view()
    return app, wire


# --- a burst of filter keystrokes rebuilds the view once -------------------
#
# The input loop takes every key that is already queued in one pass without
# yielding, and a paste queues its characters together.  The filter edits in
# a pass share ONE recompute_view, run before any other key acts and before
# the pass ends.  One rebuild per character is 600 ms of blocked loop for a
# 200-character paste over 5,000 jobs.  The rebuild replays the edits (the
# selection depends on each intermediate filter), and a run of typed
# characters narrows the previous view, so it costs at most one
# compute_view as well.


async def test_queued_filter_keystrokes_cost_one_view_rebuild(
    tmp_path, monkeypatch
):
    jobs = _tui_jobs(500)
    app, _ = _tui_app(tmp_path, jobs)
    rebuilds = []
    sorts = []
    real_rebuild = app.recompute_view
    real_compute = tui.compute_view

    def counting_rebuild():
        rebuilds.append(app.filter_text)
        real_rebuild()

    def counting_compute(*args):
        sorts.append(args[1])
        return real_compute(*args)

    app.recompute_view = counting_rebuild
    monkeypatch.setattr(tui, "compute_view", counting_compute)
    loop = asyncio.get_running_loop().create_task(app._input_loop())
    await asyncio.sleep(0)

    async def queued(*keys):
        # one turn of the event loop: the pass takes the whole queue
        app.keys.send(*keys)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert app.keys.queue.empty() and not loop.done()

    try:
        paste = "job-0001 and then two hundred characters of prose " * 4
        await queued("/", *paste)
        assert rebuilds == [paste], (
            "a %d-character paste rebuilt the view %d times; the filter "
            "edits of one queue pass must share one recompute_view"
            % (len(paste), len(rebuilds))
        )
        assert len(sorts) <= 1, (
            "the paste ran compute_view %d times; typed characters only "
            "narrow the view the pass started from" % len(sorts)
        )
        assert app.view == [] and app.filter_text == paste

        # deletions while a row stays selected: one rebuild, one sort
        await queued("ctrl+u", *"job-0001")
        assert [job["name"] for job in app.view] == [
            "job-0001%d" % i for i in range(10)
        ]
        del rebuilds[:], sorts[:]
        await queued(*["backspace"] * 7)
        assert rebuilds == ["j"] and len(sorts) == 1, (rebuilds, sorts)
        assert len(app.view) == len(jobs)
        assert app.selected_job()["name"] == "job-00010"

        # the healthy direction: keys that arrive one per pass each rebuild
        del rebuilds[:]
        for key in "ob-":
            await queued(key)
        assert rebuilds == ["jo", "job", "job-"]

        # any other key ends the run: it acts on the rebuilt view
        del rebuilds[:]
        await queued(*"0004", "enter", "j", "/", *"2")
        assert rebuilds == ["job-0004", "job-00042"], rebuilds
        assert [job["name"] for job in app.view] == ["job-00042"]
    finally:
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)


# --- a frame styles only the rows it shows ----------------------------------
#
# The jobs table is O(visible rows): one _job_row per row of the window,
# whatever the fleet size, and the toolbar's counts come from the per-poll
# fold.  A frame that walks every job (to count them, or to size a column)
# costs milliseconds per frame at fleet scale, at least once a second for
# as long as the dashboard is open; tui.table_paint_5k times the frame, and
# this pins the count behind it.


def test_a_frame_styles_only_the_visible_rows(tmp_path, monkeypatch):
    lines = 40
    body_rows = lines - 4  # header, toolbar, column titles, footer
    app, wire = _tui_app(tmp_path, _tui_jobs(5000), lines=lines)
    styled = []
    walked = []
    real_row = app._job_row
    real_health = tui.health

    def counting_row(paint, job, *rest):
        styled.append(job["name"])
        return real_row(paint, job, *rest)

    def counting_health(job):
        walked.append(job["name"])
        return real_health(job)

    app._job_row = counting_row
    monkeypatch.setattr(tui, "health", counting_health)

    app.paint()
    assert len(app.view) == 5000
    assert len(styled) == body_rows, (
        "a frame over 5,000 jobs styled %d rows on a %d-row window"
        % (len(styled), body_rows)
    )
    assert walked == styled, (
        "a frame read the health of %d jobs to draw %d rows; the toolbar "
        "and the table read the per-poll fold" % (len(walked), len(styled))
    )

    # scrolled to the far end: still one window of rows
    del styled[:], walked[:]
    app.sel = 4999
    app.paint()
    assert styled == ["job-%05d" % i for i in range(5000 - body_rows, 5000)]
    assert walked == styled

    # fewer jobs than rows: one row per job
    app.filter_text = "job-0000"
    app.recompute_view()
    del styled[:], walked[:]
    app.paint()
    assert len(app.view) == 10 and len(styled) == 10
    assert len(wire.frames) == 3


# --- the differ writes only the rows that changed ---------------------------
#
# Term.paint compares each row with the row of the previous frame and writes
# the ones that differ, inside one synchronized-output bracket.  An idle
# dashboard repaints every second, so this is what keeps an unchanged board
# from resending its whole screen down an SSH link: 16 bytes for a frame
# with no change, two rows for a selection move.  The clock is frozen here
# because the header carries it.


def test_an_unchanged_frame_writes_no_row_and_a_move_writes_two(
    tmp_path, monkeypatch
):
    frozen = types.SimpleNamespace(
        time=lambda: 1_800_000_000.0,
        monotonic=lambda: 5_000.0,
        time_ns=lambda: 1_800_000_000_000_000_000,
    )
    monkeypatch.setattr(tui, "time", frozen)
    lines = 30
    app, wire = _tui_app(tmp_path, _tui_jobs(200), lines=lines)
    app.fetched_mono = frozen.monotonic()
    address = re.compile(r"\x1b\[(\d+);1H")

    app.paint()
    first = [int(row) for row in address.findall(wire.frames[0])]
    assert first == list(range(1, lines + 1)), (
        "the first frame on a fresh terminal repaints every row"
    )

    app.paint()
    assert wire.frames[1] == tui.SYNC_ON + tui.SYNC_OFF, (
        "an unchanged frame wrote %d characters; it must write the two "
        "synchronized-output markers and no row" % len(wire.frames[1])
    )

    app.sel = 1
    app.paint()
    moved = [int(row) for row in address.findall(wire.frames[2])]
    # rows 1 to 3 are the header, the toolbar and the column titles
    assert moved == [4, 5], (
        "moving the selection one row rewrote terminal rows %r; only the "
        "row that lost the highlight and the row that gained it change" % moved
    )
    assert "job-00000" in wire.frames[2] and "job-00001" in wire.frames[2]
    assert "job-00002" not in wire.frames[2]


# --- one QR encode per pairing link ------------------------------------------
#
# The pairing panel's symbol comes from a pure-Python encoder that scores
# eight masked candidates: 6 to 14 ms for a real link, on the event loop.
# The panel therefore encodes ONCE per built link, when the link is built,
# and a frame only lays out and styles the stored matrix.  An encode on the
# paint path would cost that much per frame, at least once a second.


async def test_the_pairing_code_is_encoded_once_per_link(
    tmp_path, monkeypatch
):
    encodes = []
    real_encode = qr.encode

    def counting_encode(data, *args, **kwargs):
        encodes.append(data)
        return real_encode(data, *args, **kwargs)

    monkeypatch.setattr(qr, "encode", counting_encode)

    async def get_json(path, **kwargs):
        assert path == "/whoami"
        return {
            "authenticated": True,
            "allScopes": False,
            "scopes": ["control", "view"],
            "pairLinkBase": "https://relay.example.test/pair",
        }

    # an address a phone can dial, so the build asks nothing of the network
    api = tui.Api("http://192.168.1.50:8080", "token")
    api.get_json = get_json
    app, wire = _tui_app(tmp_path, _tui_jobs(20), lines=60, api=api)

    async def built():
        for _ in range(500):
            if app.pair is not None:
                return app.pair
            await asyncio.sleep(0.01)
        raise AssertionError("the pairing code was never built")

    app._open_pair()
    app.paint()  # the panel before its code arrives
    assert encodes == []
    pair = await built()
    assert "matrix" in pair, pair
    assert len(encodes) == 1 and encodes[0] == pair["link"].encode("utf-8")

    for _ in range(25):
        app.paint()
    app.term.invalidate()
    app.paint()
    app.theme = tui.Theme(tui.THEME_HUES[-1], True)
    app.paint()
    assert qr.INK_ON_PAPER in wire.frames[-1]
    assert len(encodes) == 1, (
        "%d frames of the open panel ran qr.encode %d more times; a frame "
        "draws the matrix the build stored"
        % (len(wire.frames) - 1, len(encodes) - 1)
    )

    # the healthy direction: a new build is a new link, and one more encode
    app._build_pair()
    await built()
    app.paint()
    assert len(encodes) == 2
    await asyncio.gather(*app._background_tasks, return_exceptions=True)


# --- the paint loop's cadence -----------------------------------------------
#
# A frame is painted only for a mark(), and the tick marks once a second for
# the clock and the countdowns: an idle dashboard paints ONE frame per tick.
# Marks that arrive faster (a log flood, a held key) coalesce: the paint
# loop keeps 33 ms between frames, so a burst costs one frame, and sustained
# marking costs about 30 frames a second however many marks arrive.


class _TuiFrameLog(tui.Term):
    """A terminal that records when each frame is painted."""

    def __init__(self):
        super().__init__(stream=_TuiWire())
        self.painted = []

    def size(self):
        return (100, 30)

    def paint(self, rows, bg):
        self.painted.append(time.monotonic())
        super().paint(rows, bg)


def _tui_cadence_app(tmp_path):
    term = _TuiFrameLog()
    app = tui.TuiApp(
        tui.Api("http://127.0.0.1:1", None),
        term,
        tui.ScriptedKeys(),
        dict(tui.PREF_DEFAULTS),
        boot=False,
        prefs_file=str(tmp_path / "prefs.json"),
    )
    return app, term


async def test_an_idle_second_paints_one_frame_per_tick(tmp_path, monkeypatch):
    app, term = _tui_cadence_app(tmp_path)
    ticks = []
    real_ambient = app._mark_ambient

    def counting_ambient():
        ticks.append(time.monotonic())
        real_ambient()

    app._mark_ambient = counting_ambient  # the tick calls it once per pass
    # The tick's one-second sleep waits for the test, so the frames are
    # counted per tick on a runner of any speed.  Every wait below is a
    # minimum: a slow runner makes it longer and changes no count.
    seconds = asyncio.Semaphore(0)
    real_sleep = asyncio.sleep

    async def stepped_sleep(delay, *args, **kwargs):
        if delay == 1:
            await seconds.acquire()
        else:
            await real_sleep(delay, *args, **kwargs)

    monkeypatch.setattr(asyncio, "sleep", stepped_sleep)
    past_the_gate = 0.1  # the paint loop holds a frame back 33 ms at most
    loops = [
        asyncio.get_running_loop().create_task(coro)
        for coro in (app._tick_loop(), app._paint_loop())
    ]
    try:
        await real_sleep(past_the_gate)
        assert term.painted == [], (
            "an idle dashboard painted %d frames before its first tick; "
            "nothing had marked it dirty" % len(term.painted)
        )
        for second in (1, 2, 3):
            seconds.release()
            for _ in range(3000):
                if len(ticks) == second and len(term.painted) == second:
                    break
                await real_sleep(0.01)
            await real_sleep(past_the_gate)
            assert len(ticks) == second, "the tick loop never ran"
            assert len(term.painted) == second, (
                "%d tick(s) painted %d frames" % (second, len(term.painted))
            )
    finally:
        app.quit = True
        for task in loops:
            task.cancel()
        await asyncio.gather(*loops, return_exceptions=True)


async def test_a_burst_of_marks_paints_at_most_once_per_33_ms(tmp_path):
    app, term = _tui_cadence_app(tmp_path)
    # a timer may fire one clock tick early (asyncio counts a timer due
    # once it is within the clock's resolution), which is 15.6 ms on
    # Windows before Python 3.13
    slack = time.get_clock_info("monotonic").resolution + 0.001
    paint_loop = asyncio.get_running_loop().create_task(app._paint_loop())
    try:
        # marks within one turn of the loop: one frame
        for _ in range(1000):
            app.mark()
        await asyncio.sleep(0.15)
        assert len(term.painted) == 1, (
            "1,000 marks in one loop turn painted %d frames"
            % len(term.painted)
        )

        # sustained marking, a mark per loop turn, for as long as four
        # frames take.  Each frame is timed from the last mark before the
        # frame ahead of it: that frame's gate opens 33 ms past the mark
        # or later, so a slow runner only widens the gap.
        del term.painted[:]
        marked = []
        deadline = time.monotonic() + 60.0
        while len(term.painted) < 4:
            assert time.monotonic() < deadline, (
                "sustained marking painted %d frames in a minute"
                % len(term.painted)
            )
            at = time.monotonic()
            app.mark()
            await asyncio.sleep(0)
            # a frame slower than the gate is followed by the next in
            # the same turn
            marked += [at] * (len(term.painted) - len(marked))
        for at, painted in zip(marked, term.painted[1:], strict=False):
            assert painted - at >= 0.033 - slack, (
                "a frame was painted %.1f ms after the last mark before "
                "the frame ahead of it; the paint loop keeps 33 ms "
                "between frames" % ((painted - at) * 1e3)
            )
    finally:
        app.quit = True
        paint_loop.cancel()
        await asyncio.gather(paint_loop, return_exceptions=True)


# --- importing the dashboard stays off the daemon graph ----------------------
#
# `cronstable tui` is a client: the only daemon code its module imports is
# the cron engine, for the schedule previews.  aiohttp loads on the first
# request, inside the running session, and the YAML parser and the
# scheduler never load at all.  An eager import of any of them adds 100 to
# 200 ms to every launch.  The probe runs in a child process, because the
# suite's own imports leave all four in sys.modules.


def test_importing_the_dashboard_loads_no_daemon_modules():
    code = textwrap.dedent(
        """
        import sys

        import cronstable.tui

        def loaded(root):
            return sum(
                1 for m in sys.modules
                if m == root or m.startswith(root + ".")
            )

        print("TUI", loaded("cronstable.tui"))
        print("CRONEXPR", loaded("cronstable.cronexpr"))
        print("AIOHTTP", loaded("aiohttp"))
        print("STRICTYAML", loaded("strictyaml"))
        print("CRON", loaded("cronstable.cron"))
        print("CONFIG", loaded("cronstable.config"))
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    counts = {}
    for line in done.stdout.split("\n"):
        key, _, value = line.partition(" ")
        if key:
            counts[key] = int(value)
    assert set(counts) == {
        "TUI",
        "CRONEXPR",
        "AIOHTTP",
        "STRICTYAML",
        "CRON",
        "CONFIG",
    }, done.stdout
    # the probe really imported the dashboard and the engine it previews with
    assert counts["TUI"] == 1 and counts["CRONEXPR"] == 1, counts
    for name in ("AIOHTTP", "STRICTYAML", "CRON", "CONFIG"):
        assert counts[name] == 0, (
            "importing cronstable.tui loaded %d %s module(s): the dashboard "
            "must import them at the point of use, or not at all"
            % (counts[name], name.lower())
        )
