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

import functools
import os
import subprocess
import sys
import textwrap

import pytest

import cronstable.state as state_mod
from cronstable import jobstate
from cronstable.cron import Cron
from cronstable.jobstate import JobStateError
from tests._commands import cmd_print, yaml_command
from tests._helpers import _backend, _drain_state_writes, _state_cfg

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
    await cron.start_stop_state(cfg)
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


def test_forked_pointer_parses_identically_to_the_stock_one():
    # The twin of the test above for config._patch_strictyaml_pointer_copy,
    # which forks strictyaml's YAMLPointer with a list copy instead of a
    # deepcopy per navigation step.  A document parsed under upstream's
    # methods and under the forked ones must produce indistinguishable
    # configs, and an INVALID document must render the same error: the
    # error path slices the offending chunk out of the document through the
    # very pointers being forked, so a pointer that went wrong would show
    # up as a mislocated or blank snippet there before anywhere else.
    import copy as copy_mod
    import dataclasses

    from strictyaml.yamlpointer import YAMLPointer

    from cronstable.config import ConfigError, parse_config_string

    def stock_val(self, regularkey, strictkey):  # verbatim upstream
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

    stock = {
        "val": stock_val,
        "key": stock_key,
        "index": stock_index,
        "textslice": stock_textslice,
        "parent": stock_parent,
    }
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
        "them to the same module." % probe["AIOHTTP-NAME-AFTER-TOUCH"]
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
        "below would pass vacuously. It is required by requirements_dev.txt; "
        "install the `discovery` extra or fix the environment."
    )
    assert probe["ZEROCONF-AFTER-IMPORT"] == 0, (
        "importing cronstable.cron pulled in zeroconf: discovery.py's "
        "_probe_zeroconf deferral was defeated, costing ~24 ms and ~3.6 MB "
        "of RSS on every start that never advertises."
    )
