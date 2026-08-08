"""The optional durable state backend: config, records, cursor, lease, probe.

Exercises :mod:`cronstable.state` and its config/lifecycle wiring end to end
against a real temp directory (the "local filesystem == Amazon S3 Files, one
backend" path), with the topology probe and clock stubbed where a test needs to
drive them deterministically.
"""

import asyncio
import contextlib
import datetime
import json
import logging
import os
import shutil
import threading
import time

import pytest

from cronstable import state
from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import Cron, JobRunInfo, _job_run_info_from_dict
from cronstable.dag import DAG_LEASE_PREFIX
from cronstable.job import JobOutputStream
from cronstable.platform import exclusive_file_lock
from cronstable.state import (
    DOCS_DIR,
    RECORDS_DIR,
    FilesystemStateBackend,
    Lease,
    _fs_safe,
    _record_epoch,
    _unescape_mount,
    detect_topology,
    make_state_backend,
)
from tests._configs import _DEP_JOB, _ONE_JOB
from tests._helpers import (
    _UTC,
    _backend,
    _drain_state_writes,
    _state_cfg,
    _wait_until,
    _write_raw_record,
)


def _info(second=0, outcome="success", exit_code=0, fail_reason=None):
    dt = datetime.datetime(2026, 7, 1, 0, 0, second, tzinfo=_UTC)
    return JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=dt,
        finished_at=dt,
        fail_reason=fail_reason,
        output=JobOutputStream(),
    )


# --- config parsing -------------------------------------------------------


def test_no_state_section_is_none():
    # stateless by default: no `state:` -> no config, nothing constructed.
    assert _state_cfg("") is None


def test_state_defaults_filled():
    cfg = _state_cfg("state:\n  path: /var/lib/cronstable\n")
    assert cfg is not None
    assert cfg["path"] == "/var/lib/cronstable"
    assert cfg["topology"] == "auto"
    assert cfg["deploymentId"] is None


def test_state_all_fields():
    cfg = _state_cfg(
        "state:\n"
        "  path: /mnt/s3files/cronstable\n"
        "  topology: shared\n"
        "  deploymentId: my-app\n"
    )
    assert cfg["topology"] == "shared"
    assert cfg["deploymentId"] == "my-app"


def test_state_path_required_by_schema():
    # `path` is a required key in the schema.
    with pytest.raises(ConfigError):
        _state_cfg("state:\n  topology: shared\n")


def test_state_path_rejects_blank():
    with pytest.raises(ConfigError, match="state.path is required"):
        _state_cfg("state:\n  path: '   '\n")


def test_state_topology_enum_validated():
    with pytest.raises(ConfigError):
        _state_cfg("state:\n  path: /x\n  topology: bogus\n")


def test_multiple_state_sections_via_include_rejected(tmp_path):
    child = tmp_path / "child.yaml"
    child.write_text("state:\n  path: /b\n")
    parent = tmp_path / "parent.yaml"
    parent.write_text("state:\n  path: /a\ninclude:\n  - child.yaml\n")
    from cronstable.config import parse_config_file

    with pytest.raises(ConfigError, match="multiple state configs"):
        parse_config_file(str(parent))


def test_state_section_from_config_dir(tmp_path):
    # a config directory: the `state` section is picked up from whichever file
    # carries it (the multi-file merge path in _parse_config_dir).
    (tmp_path / "10-jobs.yaml").write_text(
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    )
    (tmp_path / "20-state.yaml").write_text("state:\n  path: /srv/state\n")
    from cronstable.config import parse_config

    cfg = parse_config(str(tmp_path))
    assert cfg.state_config is not None
    assert cfg.state_config["path"] == "/srv/state"


def test_multiple_state_sections_in_dir_rejected(tmp_path):
    (tmp_path / "a.yaml").write_text("state:\n  path: /a\n")
    (tmp_path / "b.yaml").write_text("state:\n  path: /b\n")
    from cronstable.config import parse_config

    with pytest.raises(ConfigError, match="Multiple 'state' configurations"):
        parse_config(str(tmp_path))


# --- backend construction / factory --------------------------------------


def test_factory_builds_filesystem(tmp_path):
    cfg = _state_cfg("state:\n  path: " + str(tmp_path) + "\n")
    backend = make_state_backend(cfg, lambda: "js")
    assert isinstance(backend, FilesystemStateBackend)


def test_factory_unknown_backend_raises(tmp_path):
    cfg = {"path": str(tmp_path), "backend": "nope"}
    with pytest.raises(ConfigError, match="unknown state.backend"):
        make_state_backend(cfg, lambda: "js")  # type: ignore[arg-type]


async def test_start_creates_layout(fs_backend_factory, tmp_path):
    await fs_backend_factory(deploymentId="app1")
    base = os.path.join(str(tmp_path), "app1")
    for sub in ("records", "leases", "quarantine", "tmp"):
        assert os.path.isdir(os.path.join(base, sub))
    # the write-probe file is cleaned up after start.
    assert not any(
        n.endswith(".probe")
        for n in os.listdir(os.path.join(base, "tmp"))
    )


async def test_default_namespace(fs_backend, tmp_path):
    backend = fs_backend  # deploymentId None
    assert backend.namespace == "default"
    assert os.path.isdir(os.path.join(str(tmp_path), "default", "records"))


async def test_start_failure_on_unwritable_path(tmp_path):
    # point the store at an existing *file*: makedirs under it raises OSError.
    afile = tmp_path / "not-a-dir"
    afile.write_text("x")
    backend = _backend(tmp_path, path=str(afile))
    with pytest.raises(OSError):
        await backend.start()


# --- records + derived cursor --------------------------------------------


async def test_append_and_list_roundtrip(fs_backend):
    backend = fs_backend
    rid = await backend.append_record("runs", {"outcome": "success", "n": 1})
    assert isinstance(rid, str) and rid
    await backend.append_record("runs", {"outcome": "failure", "n": 2})
    got = await backend.list_records("runs")
    assert [r["n"] for r in got] == [1, 2]
    assert got[0]["outcome"] == "success"


async def test_list_missing_stream_is_empty(fs_backend):
    backend = fs_backend
    assert await backend.list_records("never-written") == []


async def test_list_newest_first_and_limit(fs_backend):
    backend = fs_backend
    for i in range(5):
        await backend.append_record("s", {"i": i})
    newest = await backend.list_records("s", newest_first=True, limit=2)
    assert [r["i"] for r in newest] == [4, 3]
    oldest = await backend.list_records("s", limit=3)
    assert [r["i"] for r in oldest] == [0, 1, 2]


async def test_list_records_predicate_and_max_matches(fs_backend):
    backend = fs_backend
    for i in range(10):
        await backend.append_record(
            "s", {"i": i, "kind": "even" if i % 2 == 0 else "odd"}
        )
    # predicate + max_matches=1: the single newest matching record, and the
    # scan stops at it (the early-stop the artifact-name lookup relies on).
    newest_odd = await backend.list_records(
        "s",
        newest_first=True,
        predicate=lambda r: r.get("kind") == "odd",
        max_matches=1,
    )
    assert [r["i"] for r in newest_odd] == [9]
    # predicate with no max_matches: the whole filtered window, newest first.
    evens = await backend.list_records(
        "s", newest_first=True, predicate=lambda r: r.get("kind") == "even"
    )
    assert [r["i"] for r in evens] == [8, 6, 4, 2, 0]
    # `limit` still bounds the WINDOW of records considered, so a predicate
    # only ever matches within the newest `limit`: no "even" in {9, 8} beyond
    # index 8, so limiting to 2 yields just index 8.
    limited = await backend.list_records(
        "s",
        newest_first=True,
        limit=2,
        predicate=lambda r: r.get("kind") == "even",
    )
    assert [r["i"] for r in limited] == [8]
    # no predicate/max_matches => unchanged behaviour.
    assert [
        r["i"] for r in await backend.list_records("s", limit=3)
    ] == [0, 1, 2]


async def test_records_are_immutable_and_versioned(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"k": "v"})
    stream_dir = backend._stream_dir("s")
    files = [n for n in os.listdir(stream_dir) if n.endswith(".json")]
    assert len(files) == 1
    on_disk = json.loads((open(os.path.join(stream_dir, files[0])).read()))
    assert on_disk["schemaVersion"] == state.SCHEMA_VERSION
    assert on_disk["data"] == {"k": "v"}


def test_the_old_scheme_version_spelling_still_resolves():
    # SCHEMA_VERSION was renamed off its collision with the unrelated
    # fingerprint.SCHEME_VERSION. The old spelling is a module-level name a
    # downstream script may import, so it stays as an alias.
    assert state.SCHEME_VERSION == state.SCHEMA_VERSION


async def test_derive_max_is_order_independent(fs_backend):
    backend = fs_backend
    for ts in (5, 3, 8, 1, 4):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 8


async def test_derive_max_empty_and_missing_field(fs_backend):
    backend = fs_backend
    assert await backend.derive_max("s", "ts") is None
    await backend.append_record("s", {"other": 1})
    assert await backend.derive_max("s", "ts") is None


async def test_derive_max_ignores_incomparable(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"v": "abc"})
    await backend.append_record("s", {"v": 5})  # int vs str: incomparable
    # does not raise; keeps the first-seen value.
    assert await backend.derive_max("s", "v") == "abc"


async def test_derive_max_fails_closed_on_read_error(fs_backend):
    # H1: an ENVIRONMENTAL read error (NFS blip, AV hold, cross-user EACCES)
    # on one record must fail the whole derive (raise), never silently return
    # the max over the SURVIVORS -- a value below the true max, which regresses
    # the "last fired" watermark and makes catch-up replay an already-run slot.
    backend = fs_backend
    for ts in (1, 2, 3):
        await backend.append_record("s", {"ts": ts})
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    # make the NEWEST record raise OSError on open: swap the file for a
    # directory of the same name (open() on a dir raises OSError everywhere).
    victim = os.path.join(stream_dir, names[-1])
    os.remove(victim)
    os.mkdir(victim)
    with pytest.raises(OSError):
        await backend.derive_max("s", "ts")
    # list_records stays BEST-EFFORT (non-strict): it still swallows the
    # unreadable record and returns the survivors, so the fix is localized to
    # the watermark path.
    survived = await backend.list_records("s")
    assert {r["ts"] for r in survived} == {1, 2}


async def test_derive_max_still_skips_poison_record(fs_backend):
    # the strict path fails closed only for ENVIRONMENTAL errors; a genuinely
    # corrupt (content-bad) record is unrecoverable and is still quarantined
    # and skipped, so one poison object can never wedge the watermark forever.
    backend = fs_backend
    await backend.append_record("s", {"ts": 5})
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    assert await backend.derive_max("s", "ts") == 5


def _spy_reads(backend):
    """Wrap ``_read_record`` to record which record files a derive parses."""
    read = []
    real = FilesystemStateBackend._read_record

    def spying(stream_dir, name, **kwargs):
        read.append(name)
        return real(backend, stream_dir, name, **kwargs)

    backend._read_record = spying
    return read


async def test_derive_max_incremental_fold_reads_only_new_records(fs_backend):
    # the watermark memo: a repeat derive over an unchanged stream is one
    # listdir and ZERO record parses, and after appends it parses only the
    # records that landed since the previous call (the performance fix: the
    # catch-up path derives per job per service pass over a ledger of up to
    # ~1000 multi-KB records, which used to be fully re-parsed every time).
    backend = fs_backend
    for ts in (1, 2, 3):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 3  # full scan primes memo
    read = _spy_reads(backend)
    assert await backend.derive_max("s", "ts") == 3
    assert read == []  # unchanged stream: nothing re-parsed
    del backend._read_record
    await backend.append_record("s", {"ts": 9})
    await backend.append_record("s", {"ts": 4})
    read = _spy_reads(backend)
    assert await backend.derive_max("s", "ts") == 9
    assert len(read) == 2  # only the two new records, never the old three


async def test_derive_max_survives_prune_of_the_max_record(fs_backend):
    # derived cursors are MONOTONIC maxima (see StateBackend.append_record):
    # a bounded prune deletes only old records, and deleting a record whose
    # value is already folded into the memo must never lower the cursor.
    backend = fs_backend
    for ts in (9, 1, 2, 3):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 9
    assert await backend.prune_records("s", keep=2) == 2  # 9 and 1 deleted
    assert await backend.derive_max("s", "ts") == 9


async def test_derive_max_watermark_record_pruned_after_newer_appends(
    fs_backend,
):
    # a later prune may delete the memo's watermark record itself once newer
    # records exist (prune keeps the newest ``keep``): that must read as a
    # prune (fold only the newer names), not as a stream wipe.
    backend = fs_backend
    await backend.append_record("s", {"ts": 5})
    assert await backend.derive_max("s", "ts") == 5  # watermark = the 5
    await backend.append_record("s", {"ts": 1})
    await backend.append_record("s", {"ts": 2})
    await backend.prune_records("s", keep=2)  # deletes the watermark record
    assert await backend.derive_max("s", "ts") == 5


async def test_derive_max_wiped_stream_resets_to_none(fs_backend):
    # prune keep<=0 deletes the WHOLE stream: the cursor must read as empty
    # again (and then track only the recreated records), never echo the
    # pre-wipe max out of the memo.
    backend = fs_backend
    for ts in (5, 8):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 8
    await backend.prune_records("s", keep=0)
    assert await backend.derive_max("s", "ts") is None
    await backend.append_record("s", {"ts": 3})
    assert await backend.derive_max("s", "ts") == 3


async def test_derive_max_wipe_then_append_before_next_derive(fs_backend):
    # a keep<=0 wipe followed by appends BEFORE the next derive looks exactly
    # like a prune from the listing alone (the new filenames sort above the
    # old watermark): only the explicit memo invalidation on the wipe path
    # keeps the deleted records' values out of the recreated stream's cursor.
    backend = fs_backend
    await backend.append_record("s", {"ts": 8})
    assert await backend.derive_max("s", "ts") == 8
    await backend.prune_records("s", keep=0)
    await backend.append_record("s", {"ts": 3})
    assert await backend.derive_max("s", "ts") == 3


async def test_derive_max_invalidated_by_gc_stream_removal(
    fs_backend, monkeypatch
):
    # collect_garbage removes whole streams without going through
    # prune_records: it must invalidate the memo the same way, or a stream
    # recreated after gc (again: new filenames above the old watermark)
    # would inherit the collected records' max.
    backend = fs_backend
    old = state._now() - 7200.0
    monkeypatch.setattr(state, "_now", lambda: old)
    await backend.append_record("runs/gone", {"ts": 8})
    monkeypatch.undo()
    assert await backend.derive_max("runs/gone", "ts") == 8
    result = await backend.collect_garbage(keep={"runs/": set()}, grace=3600.0)
    assert "runs%2Fgone" in result["removed"]
    await backend.append_record("runs/gone", {"ts": 3})
    assert await backend.derive_max("runs/gone", "ts") == 3


async def test_derive_max_externally_wiped_stream_yields_none(fs_backend):
    # an out-of-band wipe (an operator's rm) never crosses the backend's own
    # delete paths, so it is detected structurally: the watermark filename is
    # gone AND nothing newer exists, a shape a bounded prune can never
    # produce.  The memo is discarded and the empty stream reads None.
    backend = fs_backend
    for ts in (5, 8):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 8
    stream_dir = backend._stream_dir("s")
    for name in os.listdir(stream_dir):
        os.unlink(os.path.join(stream_dir, name))
    assert await backend.derive_max("s", "ts") is None


async def test_derive_max_newest_record_deleted_out_of_band_rescans(
    fs_backend,
):
    # deleting just the NEWEST record out of band drops the watermark with
    # survivors below it: the memo is discarded and a full rescan recomputes
    # the max from what actually remains.
    backend = fs_backend
    for ts in (2, 9):
        await backend.append_record("s", {"ts": ts})
    assert await backend.derive_max("s", "ts") == 9
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    os.unlink(os.path.join(stream_dir, names[-1]))
    assert await backend.derive_max("s", "ts") == 2


async def test_derive_max_strict_error_on_new_record_after_memo(fs_backend):
    # the incremental path parses only new records but with the SAME strict
    # semantics: an environmental error on a new record fails the whole
    # derive AND leaves the memo unadvanced, so the next derive retries the
    # same record instead of skipping past it (a skipped record would let
    # the cursor settle below the true max, the exact bug strict=True is
    # there to prevent).
    backend = fs_backend
    await backend.append_record("s", {"ts": 1})
    assert await backend.derive_max("s", "ts") == 1
    await backend.append_record("s", {"ts": 9})
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    # make the NEW record raise OSError on open: swap the file for a
    # directory of the same name (open() on a dir raises OSError everywhere).
    victim = os.path.join(stream_dir, names[-1])
    os.remove(victim)
    os.mkdir(victim)
    with pytest.raises(OSError):
        await backend.derive_max("s", "ts")
    with pytest.raises(OSError):  # memo did not advance past the bad record
        await backend.derive_max("s", "ts")


async def test_derive_max_new_poison_record_skipped_incrementally(fs_backend):
    # a content-bad NEW record keeps the first-scan behaviour on the
    # incremental path too: quarantined and skipped, never wedging or
    # regressing the cursor.  (The name sorts above any real record id.)
    backend = fs_backend
    await backend.append_record("s", {"ts": 5})
    assert await backend.derive_max("s", "ts") == 5
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "99999-bad.json"), "w") as fobj:
        fobj.write("{not json")
    assert await backend.derive_max("s", "ts") == 5
    assert "99999-bad.json" not in os.listdir(stream_dir)  # quarantined


async def test_derive_max_wipe_racing_a_scan_is_not_cached(fs_backend):
    # a keep<=0 wipe that lands while a derive is mid-scan on another worker
    # thread must fence that scan's memo write-back (the wipe-generation
    # gate), or the finished scan would resurrect a fold of records that no
    # longer exist and the recreated stream would inherit it.
    backend = fs_backend
    await backend.append_record("s", {"ts": 8})
    real = FilesystemStateBackend._read_record

    def wiping_read(stream_dir, name, **kwargs):
        data = real(backend, stream_dir, name, **kwargs)
        # the concurrent wipe, landing after the record was read but before
        # the scan finishes and writes the memo.
        backend._prune_sync("s", 0)
        return data

    backend._read_record = wiping_read
    assert await backend.derive_max("s", "ts") == 8  # this scan's own answer
    del backend._read_record
    await backend.append_record("s", {"ts": 3})
    # without the fence the raced scan would have cached best=8 and the new
    # record (a newer filename) would merely fold onto it.
    assert await backend.derive_max("s", "ts") == 3


# --- worker lanes: lease isolation + wedge observability -----------------


async def test_lease_lane_isolated_from_saturated_bulk_pool(fs_backend):
    # H2/M27: lease/coordination ops run in a DEDICATED worker lane, so a bulk
    # pool fully saturated by slow or wedged record writes cannot starve a
    # lease renew below its TTL -- which would expire a live holder's lease and
    # hand its fenced work to a standby (split-brain / double-fire).  Saturate
    # every bulk slot with a wedged op and prove a lease op still gets through
    # while a further bulk op does not.
    backend = fs_backend
    gate = threading.Event()

    def _wedge():
        gate.wait(timeout=30.0)  # hold the worker (and its bulk slot) hostage

    bulk = [
        asyncio.create_task(backend._call("bulk-wedge", _wedge))
        for _ in range(state.BULK_CALL_SLOTS)
    ]
    try:
        # every wedged op acquires its bulk slot: the bulk lane is now full.
        assert await _wait_until(
            lambda: backend.stats()["workers"]["bulk_inflight"]
            == state.BULK_CALL_SLOTS,
            raise_on_timeout=False,
        )
        assert backend.stats()["workers"]["lease_inflight"] == 0
        # a LEASE op still completes promptly on its own lane...
        got = await asyncio.wait_for(
            backend._call("lease-probe", lambda: "ok"), timeout=2.0
        )
        assert got == "ok"
        # ...while a further BULK op is blocked (the bulk lane is exhausted).
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                backend._call("bulk-extra", lambda: "nope"), timeout=0.3
            )
    finally:
        gate.set()
        await asyncio.gather(*bulk)


async def test_stats_reports_worker_lane_occupancy(fs_backend):
    # M27: a wedged store must be VISIBLE.  The op counters only tick when an
    # op FINISHES, so a hung mount reads as idle; the worker gauge shows the
    # live occupancy, so a lane pinned at capacity is the "wedged" signal.
    backend = fs_backend
    w = backend.stats()["workers"]
    assert w["bulk_capacity"] == state.BULK_CALL_SLOTS
    assert w["lease_capacity"] == state.LEASE_CALL_SLOTS
    assert w["bulk_inflight"] == 0 and w["lease_inflight"] == 0
    assert w["bulk_peak"] >= 1  # start() itself ran a bulk op

    gate = threading.Event()
    task = asyncio.create_task(
        backend._call("bulk-wedge", lambda: gate.wait(30.0))
    )
    try:
        # the wedged op is observable as one in-flight bulk worker.
        assert await _wait_until(
            lambda: backend.stats()["workers"]["bulk_inflight"] == 1,
            raise_on_timeout=False,
        )
    finally:
        gate.set()
        await task
    # and it returns to zero once the op drains.
    assert await _wait_until(
        lambda: backend.stats()["workers"]["bulk_inflight"] == 0,
        raise_on_timeout=False,
    )


# --- directory-entry durability -------------------------------------------


async def test_append_to_fresh_stream_fsyncs_new_directory_chain(
    fs_backend_factory, monkeypatch
):
    # the very first append into a stream that never existed before must
    # makedirs it durably: without flushing each newly-created level's
    # PARENT, a power loss right after could drop the whole new subtree
    # (parent and all), silently taking the just-fsynced record with it.
    flushed = []
    monkeypatch.setattr(
        state, "fsync_directory", lambda p: flushed.append(p)
    )
    backend = await fs_backend_factory()
    flushed.clear()
    await backend.append_record("brand-new-stream", {"x": 1})
    stream_dir = backend._stream_dir("brand-new-stream")
    # the stream dir's own PARENT (records/) must have been flushed, so the
    # newly-created stream directory's entry is durable.
    assert os.path.dirname(stream_dir) in flushed
    # and a second append into the now-existing stream does not re-walk/
    # re-flush the directory chain (it already exists).
    flushed.clear()
    await backend.append_record("brand-new-stream", {"x": 2})
    assert os.path.dirname(stream_dir) not in flushed


async def test_document_delete_fsyncs_namespace_directory(
    fs_backend, monkeypatch
):
    # without this, an unlinked idempotency/KV document can RESURRECT after
    # a power loss (the unlink itself was never made durable), silently
    # undoing the delete and letting guarded once-only work run again.
    backend = fs_backend
    await backend.mutate_document("ns", "k", lambda c: ({"v": 1}, None))
    flushed = []
    monkeypatch.setattr(
        state, "fsync_directory", lambda p: flushed.append(p)
    )
    await backend.mutate_document(
        "ns", "k", lambda c: (state.DOC_DELETE, None)
    )
    _lock_path, doc_path = backend._doc_paths("ns", "k")
    assert os.path.dirname(doc_path) in flushed
    assert await backend.read_document("ns", "k") is None


# --- corrupt-record quarantine -------------------------------------------


async def test_corrupt_json_is_quarantined(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"good": True})
    stream_dir = backend._stream_dir("s")
    # a truncated/garbage record (as a crash mid-write on a non-atomic store
    # could leave) lands under the stream's final name.
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    got = await backend.list_records("s")
    assert got == [{"good": True}]  # bad one skipped
    # and moved out of the stream into quarantine.
    assert "00000-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-bad.json") for n in os.listdir(quarantine))


async def test_unrecognised_schema_version_is_left_in_place(fs_backend):
    # A well-formed record with a schemaVersion this build doesn't recognise
    # is most likely written by a newer peer mid rolling-upgrade: it must be
    # skipped, never destructively quarantined (deleting it would erase that
    # peer's record fleet-wide the moment an old node reads the shared store).
    backend = fs_backend
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-new.json"), "w") as fobj:
        json.dump({"schemaVersion": "v99", "data": {"x": 1}}, fobj)
    assert await backend.list_records("s") == []
    assert "00001-new.json" in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert not os.path.isdir(quarantine) or os.listdir(quarantine) == []


async def test_unrecognised_schema_version_fails_closed_when_strict(
    fs_backend,
):
    from cronstable.state import _DocumentUnreadable

    backend = fs_backend
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-new.json"), "w") as fobj:
        json.dump({"schemaVersion": "v99", "data": {"finished_at": "x"}}, fobj)
    with pytest.raises(_DocumentUnreadable):
        await backend.derive_max("s", "finished_at")


async def test_malformed_record_shape_is_still_quarantined(fs_backend):
    # Distinct from an unrecognised-but-well-formed schemaVersion: a record
    # whose "data" isn't even a dict is genuinely unreadable content, not a
    # forward-compat gap, so destructive quarantine is still correct here.
    backend = fs_backend
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-bad.json"), "w") as fobj:
        json.dump(
            {"schemaVersion": state.SCHEMA_VERSION, "data": "not-a-dict"}, fobj
        )
    assert await backend.list_records("s") == []
    assert "00001-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00001-bad.json") for n in os.listdir(quarantine))


# --- record content cache -------------------------------------------------


async def test_record_content_cache_serves_repeat_reads(
    fs_backend, monkeypatch
):
    # A record file is immutable and its name unique forever, so a second
    # read of the same record must not open the file again.  Each read still
    # hands back its OWN parsed body: callers mutate what they get back, and
    # a shared dict would let one caller's edit rewrite history for the next.
    backend = fs_backend
    await backend.append_record("s", {"i": 1})
    first = await backend.list_records("s")
    assert first == [{"i": 1}]

    opened = []
    real_open = open

    def counting_open(path, *args, **kwargs):
        opened.append(str(path))
        return real_open(path, *args, **kwargs)

    # shadow the module's open, the seam the other read tests patch too.
    monkeypatch.setattr(state, "open", counting_open, raising=False)
    second = await backend.list_records("s")
    monkeypatch.undo()
    assert second == [{"i": 1}]
    assert opened == []  # served from memory, no second open
    assert second[0] is not first[0]
    second[0]["i"] = 99
    assert (await backend.list_records("s")) == [{"i": 1}]


async def test_record_content_cache_is_bounded(
    fs_backend_factory, monkeypatch
):
    # Both bounds hold: an oversized body is never cached at all (one
    # archived-output record must not evict a thousand useful small ones),
    # and the cache evicts least-recently-read once it is full.
    monkeypatch.setattr(state, "_RECORD_CACHE_MAX_ITEM_BYTES", 200)
    monkeypatch.setattr(state, "_RECORD_CACHE_MAX_ENTRIES", 3)
    backend = await fs_backend_factory()
    await backend.append_record("big", {"blob": "x" * 400})
    await backend.list_records("big")
    assert backend._record_cache == {}

    for i in range(5):
        await backend.append_record("small", {"i": i})
    await backend.list_records("small")
    assert len(backend._record_cache) == 3
    assert backend._record_cache_bytes == sum(
        len(v) for v in backend._record_cache.values()
    )
    # the surviving entries are the newest read (the scan runs oldest first)
    assert await backend.list_records("small") == [
        {"i": i} for i in range(5)
    ]


async def test_record_content_cache_is_bypassed_by_strict_reads(
    fs_backend, monkeypatch
):
    # strict is the fail-closed reader (derived watermarks, the mapped
    # fan-out): a record it cannot read RIGHT NOW must fail the caller
    # closed, never be resolved from a body this process happens to
    # remember, so it reads the store in both directions.
    backend = fs_backend
    await backend.append_record("s", {"ts": 5})
    assert await backend.derive_max("s", "ts") == 5
    assert backend._record_cache == {}  # a strict read caches nothing

    assert await backend.list_records("s") == [{"ts": 5}]  # this one does
    assert backend._record_cache

    def boom_open(path, *args, **kwargs):
        raise PermissionError(13, "transient hold", str(path))

    monkeypatch.setattr(state, "open", boom_open, raising=False)
    assert await backend.list_records("s") == [{"ts": 5}]  # cache, no I/O
    with pytest.raises(PermissionError):
        await backend.list_records("s", strict=True)


async def test_record_content_cache_never_holds_an_invalid_record(fs_backend):
    # Only a body that read back VALID is cached, so a record this build
    # cannot interpret keeps failing closed on every later read instead of
    # being remembered as readable.
    from cronstable.state import _DocumentUnreadable

    backend = fs_backend
    stream_dir = backend._stream_dir("s")
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, "00001-new.json"), "w") as fobj:
        json.dump({"schemaVersion": "v99", "data": {"finished_at": "x"}}, fobj)
    assert await backend.list_records("s") == []
    assert backend._record_cache == {}
    with pytest.raises(_DocumentUnreadable):
        await backend.derive_max("s", "finished_at")


# --- lease primitive ------------------------------------------------------


async def test_lease_acquire_read_release(fs_backend):
    backend = fs_backend
    lease = await backend.acquire_lease("job-x", "node-a", ttl=30)
    assert lease is not None
    assert lease.holder == "node-a"
    assert lease.fence == 1
    observed = await backend.read_lease("job-x")
    assert observed is not None and observed.holder == "node-a"
    await backend.release_lease(lease)
    assert await backend.read_lease("job-x") is None


async def test_lease_denies_second_holder_while_valid(fs_backend):
    backend = fs_backend
    a = await backend.acquire_lease("L", "A", ttl=30)
    assert a is not None
    # B cannot take a validly-held lease.
    assert await backend.acquire_lease("L", "B", ttl=30) is None


async def test_lease_same_holder_renews_keeps_fence(fs_backend):
    backend = fs_backend
    a1 = await backend.acquire_lease("L", "A", ttl=30)
    a2 = await backend.acquire_lease("L", "A", ttl=30)
    assert a1 is not None and a2 is not None
    assert a2.fence == a1.fence == 1


async def test_lease_takeover_after_expiry_bumps_fence(
    fs_backend, monkeypatch
):
    backend = fs_backend
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    a = await backend.acquire_lease("L", "A", ttl=10)  # expires at 1010
    assert a is not None and a.fence == 1
    clock["t"] = 1020.0  # past expiry
    b = await backend.acquire_lease("L", "B", ttl=10)
    assert b is not None
    assert b.holder == "B"
    assert b.fence == 2  # takeover bumps the fence

    # A's stale renew is fenced off (someone took over).
    assert await backend.renew_lease(a, ttl=10) is None
    # B can still renew.
    b2 = await backend.renew_lease(b, ttl=10)
    assert b2 is not None and b2.fence == 2


async def test_lease_release_only_by_current_holder(fs_backend, monkeypatch):
    backend = fs_backend
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    a = await backend.acquire_lease("L", "A", ttl=10)
    assert a is not None
    clock["t"] = 1020.0
    b = await backend.acquire_lease("L", "B", ttl=10)
    assert b is not None
    # A trying to release does not remove B's lease.
    await backend.release_lease(a)
    still = await backend.read_lease("L")
    assert still is not None and still.holder == "B"


async def test_renew_missing_lease_returns_none(fs_backend):
    backend = fs_backend
    ghost = Lease(name="L", holder="A", fence=1, expires_at=0.0)
    assert await backend.renew_lease(ghost, ttl=10) is None


async def test_release_missing_lease_is_noop(fs_backend):
    backend = fs_backend
    ghost = Lease(name="L", holder="A", fence=1, expires_at=0.0)
    await backend.release_lease(ghost)  # must not raise


async def test_read_corrupt_lease_is_none(fs_backend):
    backend = fs_backend
    lock, lease_path = backend._lease_paths("L")
    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
    with open(lease_path, "w") as fobj:
        fobj.write("garbage")
    assert await backend.read_lease("L") is None


# --- topology probe -------------------------------------------------------


async def test_topology_explicit_shared(fs_backend_factory, monkeypatch):
    # explicit shared overrides even when the probe disagrees (and warns).
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "ext4")
    backend = await fs_backend_factory(topology="shared")
    assert backend.topology == "shared"
    assert backend.supports_shared_locking() is True


async def test_topology_explicit_single_node(fs_backend_factory):
    backend = await fs_backend_factory(topology="single-node")
    assert backend.topology == "single-node"
    assert backend.supports_shared_locking() is False


async def test_topology_auto_detects_shared(fs_backend_factory, monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "nfs4")
    backend = await fs_backend_factory(topology="auto")
    assert backend.topology == "shared"


async def test_topology_auto_detects_single_node(
    fs_backend_factory, monkeypatch
):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "xfs")
    backend = await fs_backend_factory(topology="auto")
    assert backend.topology == "single-node"


async def test_topology_auto_unknown_falls_back(
    fs_backend_factory, monkeypatch
):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: None)
    backend = await fs_backend_factory(topology="auto")
    assert backend.topology == "single-node"


def test_detect_topology_uses_fstype(monkeypatch):
    monkeypatch.setattr(state, "IS_WINDOWS", False)
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "nfs")
    assert detect_topology("/x") == "shared"
    monkeypatch.setattr(state, "_mount_fstype", lambda p: "btrfs")
    assert detect_topology("/x") == "single-node"
    monkeypatch.setattr(state, "_mount_fstype", lambda p: None)
    assert detect_topology("/x") is None


def test_detect_topology_windows_returns_none(monkeypatch):
    # on Windows there is no cross-host lock story and no /proc to probe.
    monkeypatch.setattr(state, "IS_WINDOWS", True)
    assert detect_topology("/x") is None


def test_mount_fstype_longest_prefix(tmp_path, monkeypatch):
    mounts = (
        "rootfs / rootfs rw 0 0\n"
        "srv /srv ext4 rw 0 0\n"
        "efs /srv/data\\040dir nfs4 rw 0 0\n"
    )
    real = os.path.realpath

    def fake_open(path, *a, **k):
        assert path == "/proc/mounts"
        import io

        return io.StringIO(mounts)

    monkeypatch.setattr(state, "open", fake_open, raising=False)
    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    # deepest matching mountpoint wins; the escaped space is decoded.
    assert state._mount_fstype("/srv/data dir/x") == "nfs4"
    assert state._mount_fstype("/srv/other") == "ext4"
    assert state._mount_fstype("/elsewhere") == "rootfs"
    monkeypatch.setattr(os.path, "realpath", real)


def test_mount_fstype_no_proc(monkeypatch):
    def boom(path, *a, **k):
        raise OSError("no /proc")

    monkeypatch.setattr(state, "open", boom, raising=False)
    assert state._mount_fstype("/x") is None


# --- small helpers --------------------------------------------------------


def test_fs_safe_is_injective_and_portable():
    assert _fs_safe("plain-name_1") == "plain-name_1"
    # path separators / spaces / unicode are percent-encoded, not dropped.
    assert "/" not in _fs_safe("a/b c")
    assert _fs_safe("a/b") != _fs_safe("a-b")  # no collision
    assert _fs_safe("") == "_"


def test_fs_safe_case_insensitive_injectivity():
    # NTFS/APFS resolve names case-insensitively: two jobs differing only by
    # case must not share one on-disk stream (merged ledgers -> wrong
    # watermark/gate decisions), so uppercase is encoded, not passed through.
    assert _fs_safe("Backup").lower() != _fs_safe("backup").lower()
    assert _fs_safe("JOB").lower() != _fs_safe("job").lower()


def test_fs_safe_neutralizes_traversal_and_windows_hazards():
    # "." is encoded, so no name can yield a "." / ".." path component (a
    # deploymentId of ".." would otherwise escape the state root) or the
    # trailing-dot aliases Windows strips.
    assert _fs_safe("..") != ".."
    assert "." not in _fs_safe("..")
    assert not _fs_safe("job.").endswith(".")
    # reserved Windows device names are re-encoded to something openable.
    for name in ("con", "nul", "com1", "lpt9"):
        assert _fs_safe(name) not in {name, name.upper()}
    # ...without breaking injectivity against a literal "%63on"-style name.
    assert _fs_safe("con") != _fs_safe("%63on")


def test_fs_safe_caps_component_length():
    # percent-encoding expands 3x per non-safe byte (9x per CJK char):
    # without a cap a long job name exceeds NAME_MAX=255 and every append
    # for its stream fails with ENAMETOOLONG forever. Long names truncate to
    # a digest-uniqued token that still tells inputs apart.
    long_a = "測試" * 60
    long_b = "測試" * 60 + "x"
    token_a, token_b = _fs_safe(long_a), _fs_safe(long_b)
    assert len(token_a.encode()) <= 150
    assert len(token_b.encode()) <= 150
    assert token_a != token_b


def test_unescape_mount():
    assert _unescape_mount("/plain/path") == "/plain/path"
    assert _unescape_mount("/a\\040b") == "/a b"  # \040 == space
    assert _unescape_mount("/a\\011b") == "/a\tb"  # \011 == tab


def test_view_dict(tmp_path):
    backend = _backend(tmp_path, deploymentId="dep")
    backend._topology = "shared"
    view = backend.view_dict()
    assert view["backend"] == "filesystem"
    assert view["namespace"] == "dep"
    assert view["topology"] == "shared"
    assert view["shared_locking"] is True
    assert view["job_set_id"] == "jobset-abc"


# --- Cron lifecycle wiring ------------------------------------------------


async def test_cron_start_stop_state(tmp_path):
    cron = Cron(None)
    assert cron.state_backend is None
    cfg = _state_cfg("state:\n  path: " + str(tmp_path) + "\n")
    await cron.start_stop_state(cfg)
    assert cron.state_backend is not None
    # unchanged config -> the same backend instance is kept (no churn).
    same = cron.state_backend
    await cron.start_stop_state(cfg)
    assert cron.state_backend is same
    # removing the section tears it down.
    await cron.start_stop_state(None)
    assert cron.state_backend is None


async def test_cron_state_rebuilds_on_change(tmp_path):
    cron = Cron(None)
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(a) + "\n"))
    first = cron.state_backend
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(b) + "\n"))
    assert cron.state_backend is not None
    assert cron.state_backend is not first


async def test_cron_state_start_failure_is_swallowed(tmp_path, caplog):
    cron = Cron(None)
    afile = tmp_path / "afile"
    afile.write_text("x")
    cfg = _state_cfg("state:\n  path: " + str(afile) + "\n")
    await cron.start_stop_state(cfg)
    # a bad path is logged and swallowed; jobs keep running in memory.
    assert cron.state_backend is None
    assert any(
        "state: failed to start" in r.getMessage() for r in caplog.records
    )


# --- retention / pruning ----------------------------------------


async def test_prune_keeps_newest(fs_backend):
    backend = fs_backend
    for i in range(5):
        await backend.append_record("s", {"i": i})
    removed = await backend.prune_records("s", keep=2)
    assert removed == 3
    assert [r["i"] for r in await backend.list_records("s")] == [3, 4]


async def test_prune_keep_zero_deletes_all(fs_backend):
    backend = fs_backend
    for i in range(3):
        await backend.append_record("s", {"i": i})
    assert await backend.prune_records("s", keep=0) == 3
    assert await backend.list_records("s") == []


async def test_prune_fewer_than_keep_is_noop(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"i": 0})
    assert await backend.prune_records("s", keep=10) == 0
    assert len(await backend.list_records("s")) == 1


async def test_prune_missing_stream(fs_backend):
    backend = fs_backend
    assert await backend.prune_records("nope", keep=5) == 0


# --- config retention knob --------------------------------------


def test_state_max_runs_default():
    assert _state_cfg("state:\n  path: /x\n")["maxRunsPerJob"] == 1000


def test_state_max_runs_custom():
    cfg = _state_cfg("state:\n  path: /x\n  maxRunsPerJob: 5\n")
    assert cfg["maxRunsPerJob"] == 5


# --- JobRunInfo reconstruction ----------------------------------


def test_job_run_info_from_dict_roundtrip():
    dt = datetime.datetime(2026, 7, 3, 12, 0, 0, tzinfo=_UTC)
    orig = JobRunInfo(
        outcome="failure",
        exit_code=2,
        started_at=dt,
        finished_at=dt,
        fail_reason="boom",
        output=JobOutputStream(),
    )
    restored = _job_run_info_from_dict(orig.to_dict())
    assert restored is not None
    assert restored.outcome == "failure"
    assert restored.exit_code == 2
    assert restored.fail_reason == "boom"
    assert restored.finished_at == dt
    assert restored.started_at == dt
    # output is not persisted: a rehydrated run gets an empty, closed stream.
    assert restored.output.closed is True


def test_job_run_info_from_dict_no_started_at():
    dt = datetime.datetime(2026, 7, 3, 12, 0, 0, tzinfo=_UTC)
    rec = {
        "outcome": "success",
        "exit_code": None,
        "started_at": None,
        "finished_at": dt.isoformat(),
        "fail_reason": None,
    }
    restored = _job_run_info_from_dict(rec)
    assert restored is not None
    assert restored.started_at is None
    assert restored.duration is None


def test_job_run_info_from_dict_bad_record_returns_none():
    assert _job_run_info_from_dict({}) is None
    assert _job_run_info_from_dict({"finished_at": "not-a-date"}) is None
    assert _job_run_info_from_dict({"finished_at": 123}) is None


# --- Cron durable run ledger ------------------------------------


async def test_record_run_persists_to_ledger(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info(1, outcome="success"))
    await _drain_state_writes(cron)
    recs = await cron.state_backend.list_records(cron._run_stream("j"))
    assert len(recs) == 1
    assert recs[0]["outcome"] == "success"


async def test_record_run_noop_without_backend():
    cron = Cron(None, config_yaml=_ONE_JOB)
    cron._record_run("j", _info(0))
    # no backend -> no durable write scheduled, classic in-memory path only.
    assert not cron._pending_state_writes
    assert len(cron.run_history["j"]) == 1


async def test_prune_on_append_bounds_ledger(tmp_path):
    from cronstable.state import _PRUNE_EVERY_APPENDS

    cron = Cron(None, config_yaml=_ONE_JOB)
    cfg = _state_cfg(
        "state:\n  path: " + str(tmp_path) + "\n  maxRunsPerJob: 3\n"
    )
    await cron.start_stop_state(cfg)
    # the prune folded into the append is amortised: it actually runs on the
    # first append of a stream and then every K-th, so the stream may briefly
    # exceed the bound by up to K-1 records but never more.
    for i in range(_PRUNE_EVERY_APPENDS + 1):
        cron._record_run("j", _info(i))
        await _drain_state_writes(cron)  # sequential -> deterministic bound
        recs = await cron.state_backend.list_records(cron._run_stream("j"))
        assert len(recs) <= 3 + _PRUNE_EVERY_APPENDS - 1
    # the last append crossed the amortisation cadence: the bound is enforced
    recs = await cron.state_backend.list_records(cron._run_stream("j"))
    assert len(recs) == 3


async def test_persist_error_is_swallowed(tmp_path, caplog):
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))

    async def boom(*a, **k):
        raise OSError("disk full")

    cron.state_backend.append_record = boom  # type: ignore[method-assign]
    cron._record_run("j", _info(0))
    await _drain_state_writes(cron)
    assert any(
        "failed to persist run record" in r.getMessage()
        for r in caplog.records
    )


# --- warm-dashboard rehydration + watermark ---------------------


async def _prepopulate_ledger(backend, finished_isos):
    # seed a run ledger through a caller-provided started backend (the
    # fs_backend fixture); the Cron under test then opens its own backend
    # over the same store.
    for iso in finished_isos:
        await backend.append_record(
            "runs/j",
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": iso,
                "duration": None,
                "fail_reason": None,
            },
        )


async def test_cron_rehydrates_history_on_restart(fs_backend, tmp_path):
    await _prepopulate_ledger(
        fs_backend,
        [
            "2026-07-01T00:00:00+00:00",
            "2026-07-02T00:00:00+00:00",
            "2026-07-03T00:00:00+00:00",
        ],
    )
    # a fresh process: same store, same job -> history warmed from the ledger.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert len(cron.run_history["j"]) == 3
    assert cron.last_run["j"].outcome == "success"
    assert (
        cron.last_run["j"].finished_at.isoformat()
        == "2026-07-03T00:00:00+00:00"
    )


async def test_rehydration_runs_once(fs_backend, tmp_path):
    await _prepopulate_ledger(fs_backend, ["2026-07-01T00:00:00+00:00"])
    cron = Cron(None, config_yaml=_ONE_JOB)
    cfg = _state_cfg("state:\n  path: " + str(tmp_path))
    await cron.start_stop_state(cfg)
    assert cron._state_rehydrated is True
    before = len(cron.run_history["j"])
    # a second housekeeping pass with the unchanged config keeps the same
    # backend and must NOT rehydrate again (which would duplicate history).
    await cron.start_stop_state(cfg)
    assert len(cron.run_history["j"]) == before


async def test_durable_last_run_at_watermark(fs_backend, tmp_path):
    await _prepopulate_ledger(
        fs_backend,
        ["2026-07-01T00:00:00+00:00", "2026-07-03T00:00:00+00:00"],
    )
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert await cron.durable_last_run_at("j") == "2026-07-03T00:00:00+00:00"


async def test_durable_last_run_at_no_backend():
    cron = Cron(None, config_yaml=_ONE_JOB)
    assert await cron.durable_last_run_at("j") is None


# --- missed-run catch-up ----------------------------------------

_NOW = datetime.datetime(2026, 7, 1, 10, 10, 30, tzinfo=_UTC)


def _catchup_yaml(
    onmissed="run-all", deadline=None, jitter=0, sched="* * * * *"
):
    lines = [
        "jobs:",
        "  - name: j",
        "    command: 'true'",
        "    schedule: '" + sched + "'",
        "    onMissed: " + onmissed,
        "    catchupJitterSeconds: " + str(jitter),
    ]
    if deadline is not None:
        lines.append("    startingDeadlineSeconds: " + str(deadline))
    return "\n".join(lines) + "\n"


async def _cron_with_watermark(tmp_path, watermark_iso, **jobkw):
    cron = Cron(None, config_yaml=_catchup_yaml(**jobkw))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    if watermark_iso is not None:
        await cron.state_backend.append_record(
            cron._run_stream("j"),
            {
                "outcome": "success",
                "exit_code": 0,
                "started_at": None,
                "finished_at": watermark_iso,
                "duration": None,
                "fail_reason": None,
            },
        )
    return cron


def _count_launcher():
    calls = []

    async def fake(job, *, with_retries=True):
        calls.append(job.name)
        return True

    return calls, fake


# --- config surface ---


def test_onmissed_defaults():
    cfg = parse_config_string(_ONE_JOB, "")
    j = cfg.jobs[0]
    assert j.onMissed == "skip"
    assert j.startingDeadlineSeconds is None
    assert j.catchupJitterSeconds == 0


def test_onmissed_custom_fields():
    cfg = parse_config_string(
        _catchup_yaml(onmissed="run-all", deadline=60, jitter=5), ""
    )
    j = cfg.jobs[0]
    assert j.onMissed == "run-all"
    assert j.startingDeadlineSeconds == 60
    assert j.catchupJitterSeconds == 5


def test_onmissed_invalid_rejected():
    with pytest.raises(ConfigError):
        parse_config_string(_catchup_yaml(onmissed="bogus"), "")


def test_starting_deadline_must_be_positive():
    with pytest.raises(ConfigError, match="startingDeadlineSeconds"):
        parse_config_string(_catchup_yaml(deadline=0), "")


def test_catchup_jitter_must_be_nonnegative():
    with pytest.raises(ConfigError, match="catchupJitterSeconds"):
        parse_config_string(_catchup_yaml(jitter=-1), "")


# --- _catchup_offset (pure) ---


def test_catchup_offset_zero_when_disabled():
    assert Cron._catchup_offset("j", 0) == 0.0


def test_catchup_offset_deterministic_and_in_range():
    off = Cron._catchup_offset("job-name", 10)
    assert 0.0 <= off < 10.0
    assert Cron._catchup_offset("job-name", 10) == off  # stable across calls


# --- _missed_occurrences ---


async def test_missed_zero_when_never_ran(tmp_path):
    cron = await _cron_with_watermark(tmp_path, None, onmissed="run-all")
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert (count, watermark) == (0, None)


async def test_missed_zero_when_current(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:10:00+00:00", onmissed="run-all"
    )
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert count == 0
    assert watermark == "2026-07-01T10:10:00+00:00"


async def test_missed_run_once_coalesces_to_one(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    count, watermark = await cron._missed_occurrences(
        cron.cron_jobs["j"], _NOW
    )
    assert count == 1
    assert watermark == "2026-07-01T10:00:00+00:00"


async def test_missed_run_all_counts_each(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    # per-minute job, 10:01..10:10 missed by 10:10:30 -> 10 occurrences.
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 10


async def test_missed_deadline_bounds_window(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all", deadline=180
    )
    # only the last 180s (10:08, 10:09, 10:10) count.
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 3


async def test_missed_hard_capped(tmp_path, caplog):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    far = datetime.datetime(2026, 7, 1, 12, 30, 0, tzinfo=_UTC)  # 150 min
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], far)
    assert count == 100
    assert any("dropping the rest" in r.getMessage() for r in caplog.records)


async def test_missed_naive_watermark_is_pinned_to_utc(tmp_path):
    # a foreign/hand-written record with a NAIVE finished_at must not raise
    # TypeError out of the schedule arithmetic (which would crash the
    # scheduler at every boot): it is pinned to UTC instead.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00", onmissed="run-once"
    )
    count, _ = await cron._missed_occurrences(cron.cron_jobs["j"], _NOW)
    assert count == 1


# --- _catch_up orchestration ---


async def test_catch_up_run_once_launches_once(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j"]
    assert cron._caught_up is True


async def test_catch_up_run_all_launches_each(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    now = datetime.datetime(2026, 7, 1, 10, 3, 30, tzinfo=_UTC)  # 3 missed
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(now)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == ["j", "j", "j"]


async def test_catch_up_runs_only_once(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    await cron._catch_up(_NOW)  # second call is a no-op
    assert calls == ["j"]


async def test_catch_up_skip_schedules_nothing(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="skip"
    )
    await cron._catch_up(_NOW)
    assert cron._catchup_tasks == set()


async def test_catch_up_without_backend_warns(tmp_path, caplog):
    cron = Cron(None, config_yaml=_catchup_yaml(onmissed="run-all"))
    await cron._catch_up(_NOW)  # no state backend configured
    assert cron._catchup_tasks == set()
    assert any(
        "needs a" in r.getMessage() and "state" in r.getMessage()
        for r in caplog.records
    )


async def test_catch_up_respects_cluster_gate(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron._catch_up(_NOW)
    await asyncio.gather(*list(cron._catchup_tasks))
    assert calls == []  # non-owner leaves the backfill to the owner


async def test_run_catch_up_bails_when_stopping(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-all"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron._stop_event.set()
    await cron._run_catch_up(cron.cron_jobs["j"], 5, 0.0, _NOW)
    assert calls == []


async def test_run_catch_up_waits_out_jitter(tmp_path):
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    # a small non-zero offset exercises the interruptible jitter sleep before
    # the launch (the cross-job stagger path).
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.02, _NOW)
    assert calls == ["j"]


async def test_run_catch_up_revalidates_after_jitter(tmp_path):
    # ownership moving (or the job being disabled/removed) during the jitter
    # sleep must abort the backfill: launching anyway would double-run it
    # alongside the new owner's own catch-up.
    cron = await _cron_with_watermark(
        tmp_path, "2026-07-01T10:00:00+00:00", onmissed="run-once"
    )
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    cron._cluster_allows = lambda job: False  # type: ignore[method-assign]
    await cron._run_catch_up(cron.cron_jobs["j"], 1, 0.0, _NOW)
    assert calls == []
    del cron.cron_jobs["j"]  # removed by a reload during the jitter
    cron._cluster_allows = lambda job: True  # type: ignore[method-assign]
    await cron._run_catch_up(
        parse_config_string(_catchup_yaml(onmissed="run-once"), "").jobs[0],
        1,
        0.0,
        _NOW,
    )
    assert calls == []


# --- depends_on_past (onlyIfLastSucceeded) -----------------------


async def _dep_cron(tmp_path):
    cron = Cron(None, config_yaml=_DEP_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    return cron


async def _put_outcome(cron, outcome, ts):
    await cron.state_backend.append_record(
        cron._run_stream("j"), {"outcome": outcome, "finished_at": ts}
    )


def test_phase3_config_defaults():
    j = parse_config_string(_ONE_JOB, "").jobs[0]
    assert j.onlyIfLastSucceeded is False
    assert j.archiveOutput is False
    assert j.redactArchivedSecrets is True


def test_phase3_config_custom():
    j = parse_config_string(
        _archive_yaml(archive=True, redact=False), ""
    ).jobs[0]
    assert j.archiveOutput is True
    assert j.redactArchivedSecrets is False
    assert parse_config_string(_DEP_JOB, "").jobs[0].onlyIfLastSucceeded


async def test_depends_on_past_allows_without_flag(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)  # flag defaults off
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_allows_without_backend():
    cron = Cron(None, config_yaml=_DEP_JOB)  # flag on, no state backend
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_allows_first_run(tmp_path):
    cron = await _dep_cron(tmp_path)  # empty ledger
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_blocks_after_failure(tmp_path):
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_depends_on_past_allows_after_success(tmp_path):
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    await _put_outcome(cron, "success", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is True


async def test_depends_on_past_ignores_cancelled(tmp_path):
    # a cancelled run after a failure does not itself re-open the gate.
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    await _put_outcome(cron, "cancelled", "2026-07-01T10:05:00+00:00")
    assert await cron._depends_on_past_ok(cron.cron_jobs["j"]) is False


async def test_launch_scheduled_job_skips_on_depends_on_past(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="cronstable")
    cron = await _dep_cron(tmp_path)
    await _put_outcome(cron, "failure", "2026-07-01T10:00:00+00:00")
    calls, cron.maybe_launch_job = _count_launcher()  # type: ignore[method-assign]
    await cron.launch_scheduled_job(cron.cron_jobs["j"])
    assert calls == []
    assert any(
        "skipped: onlyIfLastSucceeded" in r.getMessage()
        for r in caplog.records
    )


# --- output/log archival with redaction -------------------------


def _archive_yaml(archive=True, redact=True):
    return (
        "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
        "    archiveOutput: " + ("true" if archive else "false") + "\n"
        "    redactArchivedSecrets: " + ("true" if redact else "false") + "\n"
    )


def _info_with_output(pairs):
    out = JobOutputStream()
    for stream_name, line in pairs:
        out.publish(stream_name, line)
    dt = datetime.datetime(2026, 7, 1, 10, 0, 0, tzinfo=_UTC)
    return JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=dt,
        finished_at=dt,
        fail_reason=None,
        output=out,
    )


async def test_archive_output_writes_redacted(tmp_path):
    cron = Cron(None, config_yaml=_archive_yaml(archive=True, redact=True))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run(
        "j",
        _info_with_output(
            [("stdout", "password=hunter2"), ("stderr", "normal line")]
        ),
    )
    await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert len(logs) == 1
    assert logs[0]["redacted"] is True
    lines = logs[0]["lines"]
    assert lines[0]["line"] == "password=***REDACTED***"
    assert lines[1]["line"] == "normal line"


async def test_archive_output_without_redaction(tmp_path):
    cron = Cron(None, config_yaml=_archive_yaml(archive=True, redact=False))
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info_with_output([("stdout", "password=hunter2")]))
    await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert logs[0]["redacted"] is False
    assert logs[0]["lines"][0]["line"] == "password=hunter2"


async def test_no_archive_without_flag(tmp_path):
    cron = Cron(None, config_yaml=_ONE_JOB)  # archiveOutput defaults off
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    cron._record_run("j", _info_with_output([("stdout", "x")]))
    await _drain_state_writes(cron)
    assert await cron.state_backend.list_records(cron._log_stream("j")) == []


async def test_archive_output_pruned_to_max(tmp_path):
    from cronstable.state import _PRUNE_EVERY_APPENDS

    cron = Cron(None, config_yaml=_archive_yaml(archive=True))
    cfg = _state_cfg(
        "state:\n  path: " + str(tmp_path) + "\n  maxRunsPerJob: 2\n"
    )
    await cron.start_stop_state(cfg)
    # one past the amortisation cadence, so the folded prune has run again
    # on the archive stream (see test_prune_on_append_bounds_ledger).
    for i in range(_PRUNE_EVERY_APPENDS + 1):
        cron._record_run("j", _info_with_output([("stdout", "run %d" % i)]))
        await _drain_state_writes(cron)
    logs = await cron.state_backend.list_records(cron._log_stream("j"))
    assert len(logs) == 2


# =====================================================================
# document / stream / blob / lease sync helpers
# =====================================================================
# Drives the sync helpers of FilesystemStateBackend straight against a
# real temp directory: the document/stream/blob stores and the
# lease-lifecycle read paths, plus the small durability/quarantine/rename
# helpers they rest on.

# a name long enough that _fs_safe truncates it to a digest-uniqued token
# (the token then carries _FS_TRUNCATION_MARKER == "%.").
_LONG = "x" * 400


# --- _record_epoch (pure) -------------------------------------------------


def test_record_epoch_parses_numeric_prefix():
    # a real record filename sorts by its zero-padded write epoch.
    assert _record_epoch("00000000000001.500000-inst-000000000001") == 1.5


def test_record_epoch_unclassifiable_names_are_infinite():
    # foreign/non-numeric names and the NaN/Inf spellings all map to +inf so
    # an age sweep treats them as brand new and never collects them.
    assert _record_epoch("not-a-number-xyz") == float("inf")
    assert _record_epoch("nan-inst-seq") == float("inf")
    assert _record_epoch("inf-inst-seq") == float("inf")


# --- stream-name sidecar + truncated-token enumeration --------------------


async def test_truncated_stream_roundtrips_via_sidecar(fs_backend):
    backend = fs_backend
    # the first append into a truncated-token stream lands the name sidecar
    # (_ensure_stream_name_sidecar), and a second append early-returns because
    # the sidecar already records the exact logical name.
    await backend.append_record(_LONG, {"n": 1})
    await backend.append_record(_LONG, {"n": 2})
    stream_dir = backend._stream_dir(_LONG)
    assert os.path.isfile(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))
    # enumeration recovers the exact name from the sidecar (audit complete).
    names, complete = await backend.list_stream_names_audit("")
    assert _LONG in names
    assert complete is True


async def test_stream_sidecar_rejects_non_roundtripping_name(fs_backend):
    backend = fs_backend
    await backend.append_record(_LONG, {"n": 1})
    stream_dir = backend._stream_dir(_LONG)
    token = os.path.basename(stream_dir)
    # a sidecar whose contents do NOT re-encode to this token is corrupt/
    # foreign: it must read back as None rather than a name that would
    # protect the wrong token in a GC keep-set.
    with open(
        os.path.join(stream_dir, state._STREAM_NAME_SIDECAR), "wb"
    ) as fobj:
        fobj.write(b"totally-different-name")
    assert backend._read_stream_name_sidecar(stream_dir, token) is None
    # and enumeration then reports the listing incomplete (the stream is
    # hidden rather than returned garbled).
    _names, complete = backend._list_stream_names_audit_sync("")
    assert complete is False


async def test_list_stream_names_skips_prefix_and_stray_files(fs_backend):
    backend = fs_backend
    await backend.append_record("runs/job-a", {"n": 1})
    await backend.append_record("logs/job-b", {"n": 2})
    records_root = os.path.join(backend.base, RECORDS_DIR)
    # a stray non-directory entry in the records root is skipped, not listed.
    with open(os.path.join(records_root, "stray-file"), "wb") as fobj:
        fobj.write(b"junk")
    names, complete = await backend.list_stream_names_audit("runs/")
    assert names == ["runs/job-a"]  # the logs/ prefix is filtered out
    assert complete is True
    # with the empty prefix every token matches, so the stray file reaches --
    # and is dropped by -- the "is this token a directory?" guard.
    names_all, complete_all = await backend.list_stream_names_audit("")
    assert {"logs/job-b", "runs/job-a"} <= set(names_all)
    assert "stray-file" not in names_all  # the non-directory entry was skipped
    assert complete_all is True


async def test_list_stream_names_missing_root_is_empty_not_unreadable(
    fs_backend,
):
    backend = fs_backend
    records_root = os.path.join(backend.base, RECORDS_DIR)
    shutil.rmtree(records_root)
    # no records root at all: exhaustively empty and COMPLETE (a never-written
    # store, not an unreadable one).
    names, complete = backend._list_stream_names_audit_sync("")
    assert names == []
    assert complete is True


async def test_list_stream_names_unreadable_root_reports_incomplete(
    fs_backend,
):
    backend = fs_backend
    records_root = os.path.join(backend.base, RECORDS_DIR)
    shutil.rmtree(records_root)
    # replace the records dir with a plain file: listdir raises a non-
    # FileNotFound OSError, which reads as "unreadable right now" (incomplete).
    with open(records_root, "wb") as fobj:
        fobj.write(b"not a directory")
    names, complete = backend._list_stream_names_audit_sync("")
    assert names == []
    assert complete is False


# --- prune-latest-by (name-keyed supersession) ----------------------------


async def test_prune_latest_by_keeps_newest_per_value(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"name": "a", "v": 1})
    await backend.append_record("s", {"name": "a", "v": 2})
    await backend.append_record("s", {"name": "b", "v": 3})
    # a record whose keyed field is NOT a string is unclassifiable and always
    # kept -- the prune cannot judge whether it supersedes anything.
    await backend.append_record("s", {"name": 999, "v": 4})
    await backend.append_record("s", {"name": "a", "v": 5})
    # a corrupt record reads back as None during the prune: never superseded
    # on its account, and left in place (it is quarantined on read instead).
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    deleted = backend._prune_latest_by_sync("s", "name")
    assert deleted == 2  # the two superseded "a" records (v1, v2)
    survivors = await backend.list_records("s")
    by_name = {}
    for rec in survivors:
        by_name[rec["name"]] = rec["v"]
    assert by_name == {"a": 5, "b": 3, 999: 4}  # newest "a" and non-str kept


async def test_prune_latest_by_missing_stream_is_zero(fs_backend):
    backend = fs_backend
    assert backend._prune_latest_by_sync("never-written", "name") == 0


async def test_prune_latest_by_swallows_unlink_race(fs_backend, monkeypatch):
    backend = fs_backend
    await backend.append_record("s", {"name": "a", "v": 1})
    await backend.append_record("s", {"name": "a", "v": 2})

    real_unlink = os.unlink

    def _raced_unlink(path):
        if str(path).endswith(".json"):
            raise OSError("already gone (raced another prune/node)")
        return real_unlink(path)

    # a superseded record that vanishes out from under the prune (another
    # pass/node deleted it) must not raise: it just is not counted.
    monkeypatch.setattr(state.os, "unlink", _raced_unlink)
    assert backend._prune_latest_by_sync("s", "name") == 0


# --- append-side prune failure is swallowed -------------------------------


async def test_append_swallows_prune_failure(fs_backend, caplog):
    backend = fs_backend

    def _boom(stream, keep):
        raise OSError("prune blew up")

    backend._prune_sync = _boom  # type: ignore[method-assign]
    # the first append of a stream is prune-due, so the folded prune runs and
    # raises: the append has already landed, so the failure is logged and the
    # record id is still returned.
    rid = await backend.append_record("s", {"n": 1}, prune_keep=1)
    assert rid  # append succeeded despite the prune error
    assert (await backend.list_records("s")) == [{"n": 1}]
    assert any(
        "could not prune stream" in r.getMessage() for r in caplog.records
    )


# --- quarantine helper ----------------------------------------------------


async def test_quarantine_moves_corrupt_record_out_of_stream(fs_backend):
    backend = fs_backend
    await backend.append_record("s", {"good": True})
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    # the corrupt record is skipped on read AND relocated to quarantine.
    assert await backend.list_records("s") == [{"good": True}]
    assert "00000-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-bad.json") for n in os.listdir(quarantine))


async def test_quarantine_of_absent_path_is_swallowed(fs_backend):
    backend = fs_backend
    ghost = os.path.join(backend._stream_dir("s"), "does-not-exist.json")
    # relocating a record that already raced away must never raise into a read.
    backend._quarantine(ghost, "does-not-exist.json", "gone")


# --- meta stamp: unreadable stamp is skipped ------------------------------


async def test_stamp_meta_skips_unreadable_stamp(fs_backend):
    backend = fs_backend  # its start() wrote the real meta stamp
    meta_dir = backend._stream_dir("meta")
    # a garbage stamp that sorts newest: _stamp_meta_sync reads it first, fails
    # to parse it, and keeps looking (never raises) -- the real stamp behind it
    # still satisfies the version check.
    with open(os.path.join(meta_dir, "99999-bad.json"), "w") as fobj:
        fobj.write("{not json")
    backend._stamp_meta_sync()  # must not raise


# --- mutable documents ----------------------------------------------------


async def test_mutate_document_rejects_non_dict_body(fs_backend):
    backend = fs_backend
    # a transform returning something that is neither a dict body nor a
    # DOC_KEEP/DOC_DELETE sentinel is a programming error, refused loudly.
    with pytest.raises(TypeError, match="must return a dict body"):
        await backend.mutate_document("ns", "k", lambda c: (12345, None))


async def test_read_doc_strict_raises_on_unknown_schema(fs_backend):
    backend = fs_backend
    _lock, doc_path = backend._doc_paths("ns", "k")
    os.makedirs(os.path.dirname(doc_path), exist_ok=True)
    # a well-formed JSON object with a foreign schemaVersion: best-effort read
    # returns None, but the strict locked RMW fails closed.
    with open(doc_path, "wb") as fobj:
        fobj.write(b'{"schemaVersion": "v99", "data": {"x": 1}}')
    assert backend._read_doc_file(doc_path) is None  # non-strict
    with pytest.raises(state._DocumentUnreadable):
        backend._read_doc_file(doc_path, strict=True)


async def test_list_document_keys_returns_sorted_keys(fs_backend):
    backend = fs_backend
    for key in ("charlie", "alpha", "bravo"):
        await backend.mutate_document(
            "ns", key, lambda c, k=key: ({"key": k}, None)
        )
    assert await backend.list_document_keys("ns") == [
        "alpha",
        "bravo",
        "charlie",
    ]
    # a namespace with no document ever written is exhaustively empty.
    assert await backend.list_document_keys("empty-ns") == []


async def test_list_document_keys_unreadable_namespace_dir(fs_backend):
    backend = fs_backend
    ns_dir = backend._doc_dir("ns")
    os.makedirs(os.path.dirname(ns_dir), exist_ok=True)
    # the namespace path is a file, not a directory: listdir raises a non-
    # FileNotFound OSError, which reads as "unreadable right now" (None).
    with open(ns_dir, "wb") as fobj:
        fobj.write(b"not a directory")
    assert await backend.list_document_keys("ns") is None


async def test_list_document_keys_truncated_key_reports_unable(fs_backend):
    backend = fs_backend
    # a key long enough to truncate has no name sidecar to decode it back, so
    # the WHOLE listing reports unable (None) rather than hiding this key.
    await backend.mutate_document(_LONG, "k", lambda c: ({"v": 1}, None))
    assert await backend.mutate_document(
        "docns", _LONG, lambda c: ({"v": 1}, None)
    )
    assert await backend.list_document_keys("docns") is None


async def test_list_document_keys_foreign_filename_reports_unable(fs_backend):
    backend = fs_backend
    ns_dir = backend._doc_dir("ns")
    os.makedirs(ns_dir, exist_ok=True)
    # a .doc filename our encoder could never emit (an invalid percent-escape
    # decoding to a non-utf8 byte): the listing falls back to None rather than
    # returning a key that cannot address the document.
    with open(os.path.join(ns_dir, "%FF.doc"), "wb") as fobj:
        fobj.write(b"{}")
    assert await backend.list_document_keys("ns") is None


async def test_list_document_keys_round_trip_guard(fs_backend):
    backend = fs_backend
    await backend.mutate_document("ns", "real", lambda c: ({"v": 1}, None))
    ns_dir = backend._doc_dir("ns")
    # tokens that DECODE cleanly but that the encoder can never have
    # produced: uppercase letters (which _fs_safe escapes) and a
    # lowercase-hex alias of an escape.  A bare unquote once returned "KEY"
    # and "/" for these, keys that address a different (or no) document;
    # the _decode_fs_token round-trip check reports the listing unable
    # instead, the same contract as the truncated-token branch.
    for foreign in ("KEY.doc", "%2f.doc"):
        with open(os.path.join(ns_dir, foreign), "wb") as fobj:
            fobj.write(b"{}")
        assert await backend.list_document_keys("ns") is None
        os.unlink(os.path.join(ns_dir, foreign))
    assert await backend.list_document_keys("ns") == ["real"]


async def test_list_document_keys_surrogate_key_round_trips(fs_backend):
    backend = fs_backend
    # a key carrying a lone surrogate (names come from os.fsdecode'd
    # sources elsewhere in the store) encodes via surrogatepass; the
    # listing must hand back the SAME key, not report unable (the old
    # strict-utf-8 unquote could not decode it) and never a U+FFFD-mangled
    # spelling that addresses a nonexistent document.
    key = "job-\udcff"
    assert await backend.mutate_document(
        "ns", key, lambda c: ({"v": 1}, None)
    )
    assert await backend.list_document_keys("ns") == [key]


async def test_list_document_namespaces_lists_and_filters(fs_backend):
    backend = fs_backend
    await backend.mutate_document("runs/a", "k", lambda c: ({"v": 1}, None))
    await backend.mutate_document("runs/b", "k", lambda c: ({"v": 2}, None))
    await backend.mutate_document("other/c", "k", lambda c: ({"v": 3}, None))
    docs_root = os.path.join(backend.base, DOCS_DIR)
    # a stray non-directory entry in the docs root is skipped, not listed.
    with open(os.path.join(docs_root, "stray"), "wb") as fobj:
        fobj.write(b"junk")
    names, complete = await backend.list_document_namespaces("runs/")
    assert names == ["runs/a", "runs/b"]  # the other/ prefix is filtered out
    assert complete is True
    # the empty prefix matches every token, so the stray file reaches -- and
    # is dropped by -- the "is this token a directory?" guard.
    names_all, complete_all = await backend.list_document_namespaces("")
    assert sorted(names_all) == ["other/c", "runs/a", "runs/b"]
    assert complete_all is True


async def test_list_document_namespaces_missing_root_is_empty(fs_backend):
    backend = fs_backend
    # remove the docs root entirely (a never-written store): the listing is
    # exhaustively empty and complete.
    shutil.rmtree(os.path.join(backend.base, DOCS_DIR))
    names, complete = await backend.list_document_namespaces("")
    assert names == []
    assert complete is True


async def test_list_document_namespaces_unreadable_root_incomplete(fs_backend):
    backend = fs_backend
    docs_root = os.path.join(backend.base, DOCS_DIR)
    shutil.rmtree(docs_root)
    # a file where the docs root should be: listdir raises a non-FileNotFound
    # OSError, which reads as unreadable (incomplete).
    with open(docs_root, "wb") as fobj:
        fobj.write(b"not a directory")
    names, complete = await backend.list_document_namespaces("")
    assert names == []
    assert complete is False


# --- content-addressed blobs ----------------------------------------------


async def test_get_blob_transient_read_error_is_raised(fs_backend):
    backend = fs_backend
    digest = await backend.put_blob(b"payload")
    blob_path = backend._blob_path(digest)
    # swap the blob file for a directory of the same name: open() on a dir
    # raises a non-FileNotFound OSError, which is the ENVIRONMENT failing, so
    # get_blob surfaces it (the awaiter can retry) rather than reporting
    # absence.
    os.remove(blob_path)
    os.mkdir(blob_path)
    with pytest.raises(OSError):
        await backend.get_blob(digest)


# --- lease lifecycle: unreadable lease fails closed -----------------------


async def test_renew_denied_when_lease_file_unreadable(fs_backend):
    backend = fs_backend
    lease = await backend.acquire_lease("L", "A", ttl=30)
    assert lease is not None
    _lock, lease_path = backend._lease_paths("L")
    # corrupt the lease file: the strict locked read cannot prove we still
    # hold it, so renew fails closed (deny) rather than resurrecting it.
    with open(lease_path, "wb") as fobj:
        fobj.write(b"garbage-not-json")
    assert await backend.renew_lease(lease, ttl=30) is None


async def test_release_of_unreadable_lease_is_noop(fs_backend):
    backend = fs_backend
    lease = await backend.acquire_lease("L", "A", ttl=30)
    assert lease is not None
    _lock, lease_path = backend._lease_paths("L")
    with open(lease_path, "wb") as fobj:
        fobj.write(b"garbage-not-json")
    # cannot prove ownership: release leaves the file to expire by TTL and
    # must not raise.
    await backend.release_lease(lease)


# --- lock-fidelity probe --------------------------------------------------


async def test_verify_locking_passes_on_a_real_lock(fs_backend):
    backend = fs_backend
    # a real filesystem's advisory locks genuinely exclude, so the probe
    # returns None (no reason to distrust them).
    assert await backend.verify_locking() is None


async def test_verify_locking_inconclusive_on_io_error(
    fs_backend, monkeypatch
):
    backend = fs_backend

    def _boom(*a, **k):
        raise OSError("probe I/O error")

    # a probe that cannot even take its first lock is inconclusive, not a
    # failure: it reports None rather than condemning a healthy store on a blip.
    monkeypatch.setattr(state, "exclusive_file_lock", _boom)
    assert backend._verify_locking_sync() is None


# --- rename/unlink retry helpers ------------------------------------------


def test_replace_retries_then_raises_on_persistent_sharing_violation(
    monkeypatch,
):
    if not state.IS_WINDOWS:
        pytest.skip("the retry loop only runs on Windows")
    calls = []

    def _always_denied(src, dest):
        calls.append((src, dest))
        raise PermissionError("sharing violation")

    monkeypatch.setattr(state.os, "replace", _always_denied)
    monkeypatch.setattr(state.time, "sleep", lambda _s: None)
    with pytest.raises(PermissionError):
        FilesystemStateBackend._replace("a-src", "a-dest")
    assert len(calls) == 5  # all five attempts were made before giving up


def test_unlink_removes_a_plain_file(tmp_path):
    victim = tmp_path / "gone.txt"
    victim.write_text("bye")
    FilesystemStateBackend._unlink(str(victim))
    assert not victim.exists()


def test_unlink_retries_then_raises_on_persistent_sharing_violation(
    monkeypatch,
):
    if not state.IS_WINDOWS:
        pytest.skip("the retry loop only runs on Windows")
    calls = []

    def _always_denied(path):
        calls.append(path)
        raise PermissionError("sharing violation")

    monkeypatch.setattr(state.os, "unlink", _always_denied)
    monkeypatch.setattr(state.time, "sleep", lambda _s: None)
    with pytest.raises(PermissionError):
        FilesystemStateBackend._unlink("a-path")
    assert len(calls) == 5


# --- durable makedirs walk-to-root guard ----------------------------------


def test_makedirs_durable_walk_stops_at_self_referential_root(tmp_path):
    backend = _backend(tmp_path)
    if not state.IS_WINDOWS:
        pytest.skip("nonexistent-drive-root probe is Windows-specific")
    free = None
    for letter in "QRSTUVWXYZ":
        if not os.path.exists(letter + ":\\"):
            free = letter
            break
    if free is None:
        pytest.skip("no free drive letter to exercise the root-walk guard")
    # a path whose every ancestor -- up to and including a self-referential,
    # nonexistent drive root -- does not exist forces the walk-up loop to
    # break at ``parent == cur`` before makedirs raises for the missing drive.
    with pytest.raises(OSError):
        backend._makedirs_durable(free + ":\\cronstable_nope\\x\\y")


# =====================================================================
# garbage-collection / migration / inventory / lock-reclaim mechanics
# =====================================================================
# Exercises the sync GC halves through their public async wrappers
# (collect_garbage / migrate_schema / inventory / sweep_orphan_blobs)
# against a real temp directory, seeding streams, leases, locks, blobs,
# documents and crash debris on disk and asserting the on-disk effects.
# Time is driven by monkeypatching cronstable.state._now (the one clock
# source) plus os.utime on the seeded files.


# --- empty-store fast paths: every root missing -> the OSError branches -----


async def test_maintenance_tolerates_missing_roots(fs_backend, monkeypatch):
    # ``start()`` pre-creates the directory skeleton, so to exercise the
    # "root absent" os.listdir(...) -> OSError branches (an operator-wiped or
    # not-yet-populated store) we remove the roots and drive the sync halves
    # directly.  Each op must return its empty-result shape, not raise.
    backend = fs_backend
    # No clock patch here on purpose. Every op under test returns from its
    # root-missing OSError branch before any aging arithmetic runs, so
    # patching state._now cannot influence a single assertion below; the
    # original `lambda: time.time()` patch was _now's own body, and pinning it
    # to a constant instead was no less inert.
    for sub in (
        state.RECORDS_DIR,
        state.LEASES_DIR,
        state.QUARANTINE_DIR,
        state.TMP_DIR,
        state.DOCS_DIR,
        state.BLOBS_DIR,
    ):
        shutil.rmtree(os.path.join(backend.base, sub), ignore_errors=True)

    # _gc_sync: records_root gone -> entries=[]; its nested sweeps see the
    # leases/docs/tmp/quarantine roots gone too and each returns 0.
    gc = backend._gc_sync(
        {"runs/": set()}, 3600.0, (DAG_LEASE_PREFIX,), False
    )
    assert gc["streams_removed"] == 0
    assert gc["streams_kept"] == 0
    assert gc["leases_removed"] == 0
    assert gc["locks_removed"] == 0
    assert gc["tmp_removed"] == 0
    assert gc["quarantine_removed"] == 0

    # _migrate_sync: records_root gone.
    mig = backend._migrate_sync(False)
    assert mig["converted"] == 0 and mig["current"] == 0

    # _sweep_orphan_blobs_sync: blobs_root gone.
    assert backend._sweep_orphan_blobs_sync(set(), 3600.0, False) == 0

    # _inventory_sync: records/docs/leases/quarantine roots all gone.
    inv = backend._inventory_sync()
    assert inv == {
        "records": {},
        "documents": {},
        "leases": [],
        "quarantine": 0,
    }


# --- _gc_sync stream reclamation + classification --------------------------


async def test_gc_removes_aged_stream_and_keeps_unmatched(
    fs_backend, monkeypatch
):
    # A managed, unreferenced, aged stream is deleted (records unlinked, dir
    # rmdir'd); a stream whose prefix nothing in `keep` names is unclassified
    # and kept; a stray non-dir entry under records/ is skipped.
    backend = fs_backend
    clock = {"t": time.time()}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    t0 = clock["t"]
    await backend.append_record("runs/dead", {"x": 1})
    await backend.append_record("custom/live", {"y": 2})
    dead_dir = backend._stream_dir("runs/dead")
    kept_dir = backend._stream_dir("custom/live")
    assert os.path.isdir(dead_dir)

    # a young managed stream (matches "runs/" but written far later than the
    # aged one) is kept by the age guard, not the keep-set.  The record epoch
    # comes from the write clock, so appending it after the jump dates it new.
    clock["t"] = t0 + 30 * 86400.0
    await backend.append_record("runs/fresh", {"z": 3})
    young_dir = backend._stream_dir("runs/fresh")

    # a stray file directly under records/ must be skipped, not crash.
    records_root = os.path.join(backend.base, state.RECORDS_DIR)
    with open(os.path.join(records_root, "stray-file"), "wb") as fobj:
        fobj.write(b"junk")

    grace = 7 * 86400.0
    # GC "now" is t0 + 30d, so cutoff is t0 + 23d: "runs/dead" (epoch ~t0)
    # is past it, "runs/fresh" (epoch ~t0+30d) is not.
    # a dry run reports the removal but leaves the stream on disk.
    dry = await backend.collect_garbage(
        keep={"runs/": set()}, grace=grace, dry_run=True
    )
    assert dry["streams_removed"] == 1
    assert os.path.isdir(dead_dir)

    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=grace
    )
    assert result["streams_removed"] == 1
    assert result["removed"] == [state._fs_safe("runs/dead")]
    assert result["records_removed"] == 1
    assert not os.path.isdir(dead_dir)
    # "custom/" is not a managed prefix in keep: unclassifiable -> kept.
    assert os.path.isdir(kept_dir)
    # "runs/fresh" is managed but too young: kept by the age guard.
    assert os.path.isdir(young_dir)
    assert result["streams_kept"] >= 2


async def test_gc_keeps_referenced_and_empty_dir_ages_against_grace(
    fs_backend, monkeypatch
):
    # A stream whose suffix IS in the keep set survives however old; an empty
    # managed dir (a writer's half-born stream) is aged against the grace via
    # its own mtime and reclaimed only once idle past it.
    backend = fs_backend
    clock = {"t": time.time()}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    await backend.append_record("runs/keepme", {"x": 1})
    kept_dir = backend._stream_dir("runs/keepme")

    # an empty managed dir with an ancient mtime: deletable debris.
    empty_dir = backend._stream_dir("runs/empty")
    os.makedirs(empty_dir, exist_ok=True)
    old = clock["t"] - 30 * 86400.0
    os.utime(empty_dir, (old, old))

    grace = 7 * 86400.0
    clock["t"] = clock["t"] + 30 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": {"keepme"}}, grace=grace
    )
    # keepme is explicitly kept; empty is aged-out debris.
    assert os.path.isdir(kept_dir)
    assert not os.path.isdir(empty_dir)
    assert state._fs_safe("runs/empty") in result["removed"]


async def test_gc_keeps_truncated_stream_without_name_sidecar(
    fs_backend, monkeypatch
):
    # A length-truncated stream directory with no verifiable name sidecar was
    # invisible to the keep-set builder, so its absence from `keep` proves
    # nothing: it must be KEPT no matter how aged/unreferenced.
    backend = fs_backend
    clock = {"t": time.time()}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    long_stream = "runs/" + "Z" * 220
    await backend.append_record(long_stream, {"x": 1})
    stream_dir = backend._stream_dir(long_stream)
    assert state._FS_TRUNCATION_MARKER in os.path.basename(stream_dir)
    # strip the sidecar to simulate a legacy pre-sidecar directory.
    os.unlink(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))

    grace = 7 * 86400.0
    clock["t"] = clock["t"] + 30 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=grace
    )
    assert result["streams_removed"] == 0
    assert os.path.isdir(stream_dir)


# --- lease reclamation + the dead-past-grace judge -------------------------


async def test_gc_reclaims_ephemeral_lease_dead_past_grace(
    fs_backend, monkeypatch
):
    # The happy path of _gc_leases_sync / _lease_dead_past_grace: an ephemeral
    # dagadvance lease provably dead for the whole grace window is reclaimed
    # (dry-run counts but keeps; the real pass unlinks the .lease).
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=10.0)
    assert lease is not None
    _lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    # a non-ephemeral lease (its token does not start with the dagadvance
    # prefix) must be skipped by the reclaim loop whatever its age.
    slot = await backend.acquire_lease("slots/j", "A", ttl=10.0)
    assert slot is not None
    _sl_lock, slot_lease = backend._lease_paths("slots/j")
    grace = 3600.0

    # within grace: never touched.
    clock["t"] = t0 + 60.0
    r = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert r["leases_removed"] == 0
    assert os.path.exists(lease_path)

    # dead past the whole window: dry run counts, deletes nothing.
    clock["t"] = t0 + grace + 120.0
    dry = await backend.collect_garbage(
        keep={},
        grace=grace,
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
        dry_run=True,
    )
    assert dry["leases_removed"] == 1
    assert os.path.exists(lease_path)

    # real pass reclaims it.
    r = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert r["leases_removed"] == 1
    assert not os.path.exists(lease_path)
    # the non-ephemeral slot lease was never eligible: still on disk.
    assert os.path.exists(slot_lease)


async def test_gc_empty_ephemeral_prefixes_reclaims_nothing(
    fs_backend, monkeypatch
):
    # ephemeral_lease_prefixes that collapse to nothing after the blank-string
    # filter (`("",)`) short-circuit to a no-op, as does the default `()`.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=10.0)
    assert lease is not None
    _lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    clock["t"] = t0 + 3600.0 + 120.0

    # a prefix tuple that filters to empty -> no reclaim.
    r = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=("",)
    )
    assert r["leases_removed"] == 0
    # the default (no ephemeral classes) -> no reclaim.
    r = await backend.collect_garbage(keep={}, grace=3600.0)
    assert r["leases_removed"] == 0
    assert os.path.exists(lease_path)


async def test_gc_never_reclaims_unreadable_ephemeral_lease(
    fs_backend, monkeypatch
):
    # An unreadable/corrupt lease under an ephemeral prefix must fail the
    # dead-past-grace judge (strict read raises _LeaseUnreadable) and survive:
    # never delete what cannot be classified.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    lease_root = os.path.join(backend.base, state.LEASES_DIR)
    os.makedirs(lease_root, exist_ok=True)
    old = t0 - 10 * 86400.0
    # invalid JSON bytes: the strict read raises on the outer parse.
    corrupt = os.path.join(lease_root, "dagadvance%2Fd%2Fbad.lease")
    with open(corrupt, "wb") as fobj:
        fobj.write(b"{ this is not json")
    os.utime(corrupt, (old, old))
    # valid JSON object but missing the lease fields: the strict read raises
    # on the inner field-decode path instead.
    badfields = os.path.join(lease_root, "dagadvance%2Fd%2Fpartial.lease")
    with open(badfields, "wb") as fobj:
        fobj.write(b'{"name": "x"}')
    os.utime(badfields, (old, old))

    clock["t"] = t0 + 3600.0 + 120.0
    r = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert r["leases_removed"] == 0
    assert os.path.exists(corrupt)
    assert os.path.exists(badfields)


# --- orphan .lock sweeps ---------------------------------------------------


async def test_gc_sweeps_orphan_document_lock_only_when_doc_absent(fs_backend):
    # A document .lock is reclaimed only when BOTH the .doc is absent AND the
    # lock sat idle past the grace; a present doc keeps its lock forever, and
    # a young orphan lock survives.
    backend = fs_backend
    grace = 3600.0
    old = time.time() - grace - 300.0

    # present doc, ancient lock -> kept (sibling exists).
    await backend.mutate_document("kv/a", "k", lambda _c: ({"v": 1}, None))
    kept_lock, _kept_doc = backend._doc_paths("kv/a", "k")
    os.utime(kept_lock, (old, old))

    # deleted doc, young lock -> kept (idle clock not past grace).
    await backend.mutate_document("kv/b", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/b", "k")
    young_lock, _yb = backend._doc_paths("kv/b", "k")

    # deleted doc, ancient lock -> the reclaimable orphan.
    await backend.mutate_document("kv/c", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/c", "k")
    dead_lock, _dc = backend._doc_paths("kv/c", "k")
    os.utime(dead_lock, (old, old))

    # a stray non-dir entry directly under docs/ must be skipped by the sweep.
    docs_root = os.path.join(backend.base, state.DOCS_DIR)
    with open(os.path.join(docs_root, "stray"), "wb") as fobj:
        fobj.write(b"junk")

    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["locks_removed"] == 1
    assert os.path.exists(dead_lock)

    r = await backend.collect_garbage(keep={}, grace=grace)
    assert r["locks_removed"] == 1
    assert os.path.exists(kept_lock)
    assert os.path.exists(young_lock)
    assert not os.path.exists(dead_lock)


async def test_gc_sweeps_expired_idempotency_docs(fs_backend, monkeypatch):
    # A ttl>0 idempotency claim used to leave its .doc behind forever after
    # expiry (the documented per-event key pattern mints one per event, so
    # a busy dedupe scope grew its flat namespace without bound). The GC
    # must sweep exactly the claims whose expiry lapsed a whole grace ago:
    # permanent claims (no expiresAt), still-live TTLs, and every other
    # docs/ namespace stay untouched.
    from cronstable import jobstate

    backend = fs_backend
    try:
        await jobstate.idempotency_claim(backend, "scope", "permanent")
        await jobstate.idempotency_claim(backend, "scope", "lapsed", ttl=5.0)
        await jobstate.idempotency_claim(
            backend, "scope", "alive", ttl=999999.0
        )
        # a KV doc in a non-idem namespace, as the never-swept control
        await jobstate.kv_set(backend, "scope", "keep", {"v": 1})

        grace = 3600.0
        future = time.time() + 4000.0  # "lapsed" expired ~4000s ago > grace
        monkeypatch.setattr(state, "_now", lambda: future)

        dry = backend._gc_sync({"runs/": set()}, grace, (), True)
        assert dry["idem_docs_removed"] == 1
        ns_dir = backend._doc_dir("idem/scope")
        assert (
            len([n for n in os.listdir(ns_dir) if n.endswith(".doc")]) == 3
        )  # dry run deleted nothing

        gc = backend._gc_sync({"runs/": set()}, grace, (), False)
        assert gc["idem_docs_removed"] == 1
        docs = sorted(
            n for n in os.listdir(ns_dir) if n.endswith(".doc")
        )
        assert docs == ["alive.doc", "permanent.doc"]
        # the survivors still dedupe, and the KV control is untouched
        again = await jobstate.idempotency_claim(
            backend, "scope", "permanent"
        )
        assert again["fresh"] is False
        kept = await jobstate.kv_get(backend, "scope", "keep")
        assert kept is not None and kept["value"] == {"v": 1}
        # a second pass finds nothing left to sweep
        assert (
            backend._gc_sync({"runs/": set()}, grace, (), False)[
                "idem_docs_removed"
            ]
            == 0
        )
    finally:
        await backend.stop()


async def test_gc_idem_sweep_skips_doc_whose_lock_is_held(fs_backend):
    # REGRESSION: the idem-doc sweep used to take each candidate's flock
    # with the blocking _locked, so on a shared store one peer wedged
    # mid-claim stalled the entire gc() call (and its worker slot) behind
    # that single doc. The sweep now try-locks: a held lock skips the doc
    # for this pass and a later pass collects it once released. Both
    # platforms' lock primitives conflict across two descriptors of one
    # file inside a single process (POSIX flock is per-open-file-
    # description, Windows byte-range locks are per-handle; pinned by
    # test_platform.test_nonblocking_lock_raises_on_contention), so a
    # plain second fd stands in for the wedged peer and no subprocess or
    # platform gate is needed.
    from cronstable import jobstate
    from cronstable.platform import exclusive_file_lock

    backend = fs_backend
    try:
        await jobstate.idempotency_claim(backend, "scope", "held", ttl=5.0)
        await jobstate.idempotency_claim(backend, "scope", "free", ttl=5.0)
        cutoff = time.time() + 4000.0  # both claims lapsed vs this cutoff

        held_lock, held_doc = backend._doc_paths("idem/scope", "held")
        _free_lock, free_doc = backend._doc_paths("idem/scope", "free")

        result = {}

        def _sweep():
            result["removed"] = backend._gc_idem_docs_sync(cutoff, False)

        fd = os.open(held_lock, os.O_RDWR)
        try:
            with exclusive_file_lock(fd, blocking=False):
                # daemon so a regression (the sweep blocking forever on
                # the held flock) fails the join assert instead of
                # hanging the whole run.
                worker = threading.Thread(target=_sweep, daemon=True)
                worker.start()
                worker.join(timeout=30.0)
                assert not worker.is_alive(), (
                    "gc stalled behind a held idempotency-doc lock"
                )
        finally:
            os.close(fd)

        assert result["removed"] == 1  # the uncontended doc only
        assert os.path.exists(held_doc)
        assert not os.path.exists(free_doc)

        # lock released: the next pass collects the skipped doc.
        assert backend._gc_idem_docs_sync(cutoff, False) == 1
        assert not os.path.exists(held_doc)
    finally:
        await backend.stop()


def test_idem_prefix_mirror_stays_in_sync():
    # state cannot import jobstate (layering), so the GC recognises the
    # idempotency namespaces by a mirrored prefix constant; this pin is
    # what makes the mirror safe.
    from cronstable import jobstate

    assert state._IDEM_DOC_NS_PREFIX == jobstate.IDEM_NS_PREFIX


async def test_gc_sweeps_bare_lease_lock_idle_past_grace(fs_backend):
    # A bare lease .lock with no .lease sibling (a lost post-release unlink)
    # is reclaimed once idle past the grace; a fresh bare lock and a lock with
    # a live .lease sibling both survive.
    backend = fs_backend
    grace = 3600.0
    old = time.time() - grace - 300.0
    lease_root = os.path.join(backend.base, state.LEASES_DIR)

    lease = await backend.acquire_lease("slots/j", "A", ttl=30.0)
    assert lease is not None
    live_lock, live_lease = backend._lease_paths("slots/j")
    os.utime(live_lock, (old, old))  # ancient, but .lease present -> kept

    dead_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fgone.lock")
    with open(dead_lock, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(dead_lock, (old, old))

    young_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fnew.lock")
    with open(young_lock, "wb") as fobj:
        fobj.write(b"\0")

    r = await backend.collect_garbage(keep={}, grace=grace)
    assert r["locks_removed"] == 1
    assert not os.path.exists(dead_lock)
    assert os.path.exists(live_lock) and os.path.exists(live_lease)
    assert os.path.exists(young_lock)


# --- tmp / quarantine crash-debris sweeps ----------------------------------


async def test_gc_sweeps_tmp_and_quarantine_debris(fs_backend, monkeypatch):
    # _sweep_dir_sync: aged write-temp files (> TMP_MAX_AGE) and aged
    # quarantined records (> grace) are unlinked; a young file, and a
    # sub-directory (not a plain file), are left alone.
    backend = fs_backend
    now = time.time()
    clock = {"t": now}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])

    tmp_dir = os.path.join(backend.base, state.TMP_DIR)
    quar_dir = os.path.join(backend.base, state.QUARANTINE_DIR)
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(quar_dir, exist_ok=True)

    old = now - state.TMP_MAX_AGE - 3600.0
    old_tmp = os.path.join(tmp_dir, "abandoned.tmp")
    with open(old_tmp, "wb") as fobj:
        fobj.write(b"partial")
    os.utime(old_tmp, (old, old))

    young_tmp = os.path.join(tmp_dir, "recent.tmp")
    with open(young_tmp, "wb") as fobj:
        fobj.write(b"fresh")

    # a directory under tmp/ must be skipped (isfile guard).
    os.makedirs(os.path.join(tmp_dir, "a-subdir"), exist_ok=True)

    old_quar = os.path.join(quar_dir, "poison.json")
    with open(old_quar, "wb") as fobj:
        fobj.write(b"{bad}")
    quar_old = now - 30 * 86400.0
    os.utime(old_quar, (quar_old, quar_old))

    grace = 7 * 86400.0
    # a dry run counts the aged debris but unlinks nothing.
    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["tmp_removed"] == 1
    assert dry["quarantine_removed"] == 1
    assert os.path.exists(old_tmp)
    assert os.path.exists(old_quar)

    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["tmp_removed"] == 1
    assert result["quarantine_removed"] == 1
    assert not os.path.exists(old_tmp)
    assert os.path.exists(young_tmp)
    assert os.path.isdir(os.path.join(tmp_dir, "a-subdir"))
    assert not os.path.exists(old_quar)


# --- migrate_schema converter walk -----------------------------------------


async def test_migrate_schema_counts_every_class(fs_backend, monkeypatch):
    # Drive _migrate_sync through all its record classes: current, converted,
    # unknown (no converter / converter returns None / non-dict data),
    # unreadable (corrupt bytes), failed (converter raises), plus non-.json
    # and non-dir entries under records/ that the walk must skip.
    backend = fs_backend
    await backend.append_record("runs/j", {"outcome": "ok"})  # current v1

    monkeypatch.setitem(
        state.RECORD_MIGRATIONS,
        "v0",
        lambda data: {"outcome": data.get("result")},
    )
    monkeypatch.setitem(state.RECORD_MIGRATIONS, "vNull", lambda data: None)

    def _bad(_data):
        raise ValueError("converter bug")

    monkeypatch.setitem(state.RECORD_MIGRATIONS, "vBad", _bad)

    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000001-old-000000000001.json",
        {"schemaVersion": "v0", "data": {"result": "converted!"}},
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000002-old-000000000002.json",
        {"schemaVersion": "vX", "data": {"a": 1}},  # no converter -> unknown
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000003-old-000000000003.json",
        {"schemaVersion": "vNull", "data": {"a": 1}},  # None -> unknown
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000004-old-000000000004.json",
        {"schemaVersion": "vBad", "data": {"a": 1}},  # raises -> failed
    )
    # corrupt bytes -> unreadable
    stream_dir = backend._stream_dir("runs/j")
    with open(
        os.path.join(stream_dir, "00000000000000000005-x-000000000005.json"),
        "wb",
    ) as fobj:
        fobj.write(b"{ not valid json")
    # a non-.json file in the stream dir is ignored by the walk.
    with open(os.path.join(stream_dir, "notes.txt"), "wb") as fobj:
        fobj.write(b"ignore me")
    # a stray non-dir entry directly under records/ must be skipped.
    records_root = os.path.join(backend.base, state.RECORDS_DIR)
    with open(os.path.join(records_root, "loose"), "wb") as fobj:
        fobj.write(b"x")

    dry = await backend.migrate_schema(dry_run=True)
    assert dry["converted"] == 1
    assert dry["unknown"] == 2
    assert dry["failed"] == 1
    assert dry["unreadable"] == 1
    assert dry["current"] >= 1  # the fresh v1 record (+ meta stamp)

    result = await backend.migrate_schema()
    assert result["converted"] == 1
    # the v0 record is now rewritten to v1 and reads back converted.
    recs = await backend.list_records("runs/j")
    assert {"outcome": "converted!"} in recs
    # a second migrate finds nothing left to convert.
    again = await backend.migrate_schema(dry_run=True)
    assert again["converted"] == 0


async def test_migrate_schema_rolls_back_a_failed_atomic_write(
    fs_backend, monkeypatch
):
    # If the in-place re-encode write itself fails (a disk error, a
    # sharing hold), the record is NOT counted as converted: the tentative
    # ``converted`` is rolled back and the record is booked as failed.
    backend = fs_backend
    monkeypatch.setitem(
        state.RECORD_MIGRATIONS, "v0", lambda data: {"outcome": "x"}
    )
    _write_raw_record(
        backend,
        "runs/j",
        "00000000000000000001-old-000000000001.json",
        {"schemaVersion": "v0", "data": {"result": "ok"}},
    )

    def _boom(_path, _payload):
        raise OSError("disk full")

    # simulate the write failing only for the real (non-dry) pass; the
    # convert itself succeeds, so the failure is purely the durable write.
    monkeypatch.setattr(backend, "_atomic_write", _boom)
    result = await backend.migrate_schema()
    assert result["converted"] == 0
    assert result["failed"] == 1


# --- orphan blob sweep -----------------------------------------------------


async def test_sweep_orphan_blobs_removes_aged_unreferenced(
    fs_backend, monkeypatch
):
    # _sweep_orphan_blobs_sync: an aged, unreferenced blob is unlinked (and
    # its now-empty shard rmdir'd); a referenced blob and a young blob both
    # survive; a stray file directly under blobs/ is skipped.
    backend = fs_backend
    now = time.time()
    monkeypatch.setattr(state, "_now", lambda: now)

    referenced = await backend.put_blob(b"still-referenced")
    orphan = await backend.put_blob(b"nobody-points-here")
    young = await backend.put_blob(b"just-landed-orphan")
    assert referenced != orphan != young

    old = now - 7200.0
    os.utime(backend._blob_path(orphan), (old, old))
    # `referenced` also aged, but its digest is in the keep set -> survives.
    os.utime(backend._blob_path(referenced), (old, old))

    # a stray file directly under blobs/ (not a shard dir) is skipped.
    blobs_root = os.path.join(backend.base, state.BLOBS_DIR)
    with open(os.path.join(blobs_root, "stray"), "wb") as fobj:
        fobj.write(b"junk")
    # a non-.blob file inside the orphan's shard dir is skipped by the sweep.
    orphan_shard = os.path.dirname(backend._blob_path(orphan))
    with open(os.path.join(orphan_shard, "README"), "wb") as fobj:
        fobj.write(b"not a blob")

    grace = 3600.0
    # dry run counts the one aged orphan but deletes nothing.
    assert (
        await backend.sweep_orphan_blobs(
            {referenced}, grace, dry_run=True
        )
        == 1
    )
    assert await backend.get_blob(orphan) is not None

    removed = await backend.sweep_orphan_blobs({referenced}, grace)
    assert removed == 1
    assert await backend.get_blob(orphan) is None
    assert await backend.get_blob(referenced) == b"still-referenced"
    assert await backend.get_blob(young) == b"just-landed-orphan"


# --- inventory snapshot ----------------------------------------------------


async def test_inventory_reports_counts_leases_and_quarantine(
    fs_backend, monkeypatch
):
    # _inventory_sync walks records + documents into per-prefix groups, lists
    # live leases (skipping released/absent and corrupt ones), and counts the
    # quarantine dir.  A non-dir entry under records/ is skipped by the walk.
    backend = fs_backend
    t0 = time.time()
    monkeypatch.setattr(state, "_now", lambda: t0)

    await backend.append_record("runs/j1", {"x": 1})
    await backend.append_record("runs/j1", {"x": 2})
    await backend.append_record("runs/j2", {"x": 3})
    await backend.mutate_document("kv/ns", "k", lambda _c: ({"v": 1}, None))

    # a live lease shows up; a released one (expiry 0) does not.
    live = await backend.acquire_lease("leader", "node-a", ttl=30.0)
    assert live is not None
    gone = await backend.acquire_lease("gone", "node-b", ttl=30.0)
    assert gone is not None
    await backend.release_lease(gone)

    # corrupt lease files are observed best-effort (None) and skipped: one
    # valid-JSON-but-not-an-object, one with invalid JSON bytes.
    lease_root = os.path.join(backend.base, state.LEASES_DIR)
    with open(os.path.join(lease_root, "broken.lease"), "wb") as fobj:
        fobj.write(b"[1, 2, 3]")  # JSON, but not a lease object
    with open(os.path.join(lease_root, "garbled.lease"), "wb") as fobj:
        fobj.write(b"{not: valid json")  # unparseable

    # a stray file under records/ that the walk must skip.
    records_root = os.path.join(backend.base, state.RECORDS_DIR)
    with open(os.path.join(records_root, "loose"), "wb") as fobj:
        fobj.write(b"x")

    # seed the quarantine dir so its count is non-zero.
    quar_dir = os.path.join(backend.base, state.QUARANTINE_DIR)
    os.makedirs(quar_dir, exist_ok=True)
    with open(os.path.join(quar_dir, "poison.json"), "wb") as fobj:
        fobj.write(b"{bad}")

    inv = await backend.inventory()

    assert inv["enumerable"] is True
    assert "view" in inv and "stats" in inv
    runs = inv["records"]["runs"]
    assert runs["streams"] == 2
    assert runs["count"] == 3
    scopes = {s["scope"]: s["count"] for s in runs["scopes"]}
    assert scopes == {"j1": 2, "j2": 1}
    assert inv["documents"]["kv"]["streams"] == 1

    lease_names = {ent["name"] for ent in inv["leases"]}
    assert "leader" in lease_names
    assert "gone" not in lease_names  # released
    assert "broken" not in lease_names  # corrupt (not an object)
    assert "garbled" not in lease_names  # corrupt (bad JSON)
    leader = next(e for e in inv["leases"] if e["name"] == "leader")
    assert leader["holder"] == "node-a"
    assert leader["fence"] == live.fence
    assert leader["expired"] is False

    assert inv["quarantine"] == 1


# =====================================================================
# Base-backend defaults, platform-gated retry
# loops, and the best-effort OSError branches of the maintenance sweeps
# =====================================================================


def _os_raiser(monkeypatch, attr, target_paths, exc=OSError):
    # Make ``os.<attr>`` raise for a fixed set of exact paths and delegate
    # to the real function for every other path, so a single sweep step can
    # be pushed down its OSError branch without disturbing the rest.
    real = getattr(os, attr)
    wanted = {os.path.abspath(str(p)) for p in target_paths}

    def _w(path, *a, **k):
        if os.path.abspath(str(path)) in wanted:
            raise exc("injected failure")
        return real(path, *a, **k)

    monkeypatch.setattr(os, attr, _w)


def _os_one_shot(monkeypatch, attr, target, exc=OSError):
    # Like _os_raiser but only for the FIRST call against ``target``; every
    # later call (including the retry that the caller makes) delegates.
    real = getattr(os, attr)
    tgt = os.path.abspath(str(target))
    seen = {"fired": False}

    def _w(path, *a, **k):
        if not seen["fired"] and os.path.abspath(str(path)) == tgt:
            seen["fired"] = True
            raise exc("one-shot failure")
        return real(path, *a, **k)

    monkeypatch.setattr(os, attr, _w)
    return seen


class _DummyLock:
    # A stand-in for FilesystemStateBackend._locked that runs a mutation on
    # the lock path at acquire time, so a test can simulate a concurrent
    # actor winning the race between the pre-check and the re-judge.
    def __init__(self, path, mutation):
        self._path = path
        self._mutation = mutation

    def __enter__(self):
        self._mutation(self._path)
        return None

    def __exit__(self, *exc):
        return False


class _MinimalBackend(state.StateBackend):
    # The smallest concrete StateBackend: every abstract op is a trivial
    # stub, so the base class's non-abstract DEFAULTS (the ones a future
    # native-S3 backend inherits unchanged) can be exercised directly.
    async def start(self):
        pass

    async def stop(self):
        pass

    async def append_record(
        self, stream, data, *, prune_keep=None, prune_latest_by=None
    ):
        return "r"

    async def list_records(
        self, stream, *, limit=None, newest_first=False, strict=False
    ):
        return []

    async def list_stream_names(self, prefix):
        return []

    async def derive_max(self, stream, field):
        return None

    async def prune_records(self, stream, *, keep):
        return 0

    async def read_document(self, namespace, key):
        return None

    async def mutate_document(self, namespace, key, transform):
        return (None, None)

    async def delete_document(self, namespace, key):
        return False

    async def list_documents(self, namespace):
        return []

    async def put_blob(self, data):
        return ""

    async def get_blob(self, digest):
        return None

    async def acquire_lease(self, name, holder, ttl):
        return None

    async def renew_lease(self, lease, ttl):
        return None

    async def release_lease(self, lease):
        return None

    async def read_lease(self, name):
        return None

    @property
    def topology(self):
        return "unknown"


async def test_base_backend_defaults_are_inert():
    # The base StateBackend cannot enumerate or coordinate, so every
    # optional surface reports its empty/incomplete shape rather than
    # pretending to have done work.
    backend = _MinimalBackend()
    assert await backend.list_stream_names_audit("x") == ([], False)
    assert await backend.list_document_keys("ns") is None
    assert await backend.list_document_namespaces("p") == ([], False)
    assert await backend.collect_garbage(keep={}, grace=1.0) == {}
    assert await backend.migrate_schema() == {}
    assert await backend.sweep_orphan_blobs(set(), 1.0) == 0
    assert await backend.verify_locking() is None
    assert backend.stats() == {}
    assert backend.view_dict() == {
        "backend": "state",
        "topology": "unknown",
    }
    assert backend.supports_shared_locking() is False
    inv = await backend.inventory()
    assert inv["enumerable"] is False
    assert inv["records"] == {}
    assert inv["documents"] == {}
    assert inv["leases"] == []
    assert inv["quarantine"] == 0


def test_mount_entry_skips_malformed_short_lines(monkeypatch):
    # A /proc/mounts line with fewer than four space-separated fields (a
    # truncated/garbled entry) is skipped, not indexed out of bounds.
    mounts = (
        "short-junk-line\n"  # only one field: skipped by the len guard
        "rootfs / rootfs rw 0 0\n"
    )

    def fake_open(path, *a, **k):
        assert path == "/proc/mounts"
        import io

        return io.StringIO(mounts)

    monkeypatch.setattr(state, "open", fake_open, raising=False)
    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    # the malformed line is ignored; the valid rootfs line still resolves.
    assert state._mount_entry("/anything") == ("rootfs", "rw")


async def test_call_never_reuses_a_worker_still_stuck_in_an_op(fs_backend):
    # What thread-per-call used to buy, the pool must keep: a worker wedged
    # in an uninterruptible syscall on a dead mount is never handed another
    # op.  A worker rejoins the idle pool only once its op RETURNS, so the
    # next op runs on a different thread instead of queueing behind the
    # wedged one forever.
    backend = fs_backend
    gate = threading.Event()
    idents = []

    def wedge():
        idents.append(threading.get_ident())
        gate.wait(timeout=30.0)

    wedged = asyncio.create_task(backend._call("wedge", wedge))
    try:
        assert await _wait_until(
            lambda: len(idents) == 1, raise_on_timeout=False
        )
        got = await asyncio.wait_for(
            backend._call("after", threading.get_ident), timeout=5.0
        )
        assert got != idents[0]
    finally:
        gate.set()
        await wedged


async def test_call_releases_slot_when_thread_spawn_fails(fs_backend):
    # If the per-call worker thread cannot even be started, the worker slot
    # it reserved is released (never leaked) and the failure propagates.
    backend = fs_backend

    class _BoomThread:
        def __init__(self, *a, **k):
            pass

        def start(self):
            raise RuntimeError("cannot spawn a thread")

    original = state.threading.Thread
    state.threading.Thread = _BoomThread
    # Empty the idle worker pool for the duration: with a parked worker
    # available _call hands the op straight to it and never reaches a thread
    # spawn at all, which is the whole point of the pool -- the failure this
    # test is about is the one on the path that DOES have to spawn.  The
    # parked workers are put back afterwards (they are still parked, waiting
    # on their queue, so nothing else can be holding them).
    with state._IDLE_WORKERS_LOCK:
        parked = list(state._IDLE_WORKERS)
        state._IDLE_WORKERS.clear()
    try:
        with pytest.raises(RuntimeError, match="cannot spawn"):
            await backend.derive_max("runs/j", "ts")
    finally:
        state.threading.Thread = original
        with state._IDLE_WORKERS_LOCK:
            state._IDLE_WORKERS.extend(parked)

    # the slot was returned: a normal op still completes afterwards.
    assert await backend.derive_max("runs/j", "ts") is None


def test_replace_windows_retry_loop_forced(tmp_path, monkeypatch):
    # Drive the Windows sharing-violation retry loop of _replace on a
    # non-Windows host by forcing IS_WINDOWS: a transient PermissionError is
    # retried and then succeeds; a persistent one exhausts and re-raises.
    monkeypatch.setattr(state, "IS_WINDOWS", True)
    monkeypatch.setattr(state.time, "sleep", lambda _s: None)

    src = tmp_path / "src"
    src.write_text("payload")
    dest = tmp_path / "dest"
    real_replace = os.replace
    calls = {"n": 0}

    def flaky(a, b):
        calls["n"] += 1
        if calls["n"] < 3:
            raise PermissionError("sharing violation")
        return real_replace(a, b)

    monkeypatch.setattr(state.os, "replace", flaky)
    FilesystemStateBackend._replace(str(src), str(dest))
    assert dest.read_text() == "payload"
    assert calls["n"] == 3

    def always(a, b):
        raise PermissionError("held forever")

    monkeypatch.setattr(state.os, "replace", always)
    with pytest.raises(PermissionError):
        FilesystemStateBackend._replace("a", "b")


def test_unlink_windows_retry_loop_forced(tmp_path, monkeypatch):
    # The delete-side twin: _unlink's Windows retry loop, forced on Linux.
    monkeypatch.setattr(state, "IS_WINDOWS", True)
    monkeypatch.setattr(state.time, "sleep", lambda _s: None)

    victim = tmp_path / "victim"
    victim.write_text("bye")
    real_unlink = os.unlink
    calls = {"n": 0}

    def flaky(p):
        calls["n"] += 1
        if calls["n"] < 2:
            raise PermissionError("sharing violation")
        return real_unlink(p)

    monkeypatch.setattr(state.os, "unlink", flaky)
    FilesystemStateBackend._unlink(str(victim))
    assert not victim.exists()

    def always(p):
        raise PermissionError("held forever")

    monkeypatch.setattr(state.os, "unlink", always)
    with pytest.raises(PermissionError):
        FilesystemStateBackend._unlink("nope")


def test_makedirs_durable_walk_breaks_at_self_referential_root(
    tmp_path, monkeypatch
):
    # Force the walk-up loop to reach a self-referential root (dirname(cur) ==
    # cur) so its ``break`` fires before makedirs raises for the unreachable
    # ancestor.  The whole ancestor chain, up to and including the filesystem
    # root the walk converges on, is reported absent; makedirs then raises.
    # The root is derived from the path itself, so this resolves identically
    # on POSIX ("/") and Windows (a drive root such as "C:\\").
    backend = _backend(tmp_path)
    target = os.path.join(str(tmp_path), "no_such_root", "a", "b")

    chain = set()
    cur = os.path.abspath(target)
    while True:
        chain.add(cur)
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent

    real_isdir = os.path.isdir

    def isdir(p):
        return False if os.path.abspath(str(p)) in chain else real_isdir(p)

    def boom(*a, **k):
        raise OSError("unreachable ancestor")

    monkeypatch.setattr(os.path, "isdir", isdir)
    monkeypatch.setattr(os, "makedirs", boom)
    with pytest.raises(OSError):
        backend._makedirs_durable(target)


async def test_prune_tolerates_unlink_oserror(fs_backend, monkeypatch):
    # A prune whose individual unlink races another node (or a Windows
    # sharing hold) swallows the OSError and simply reports fewer deletes.
    backend = fs_backend
    for i in range(3):
        await backend.append_record("runs/p", {"n": i})
    stream_dir = backend._stream_dir("runs/p")
    names = [
        os.path.join(stream_dir, n)
        for n in os.listdir(stream_dir)
        if n.endswith(".json")
    ]
    _os_raiser(monkeypatch, "unlink", names)
    # keep=1 would delete two records, but every unlink raises: none counted.
    assert await backend.prune_records("runs/p", keep=1) == 0


async def test_locked_reopens_on_ghost_inode(fs_backend, monkeypatch):
    # After winning the flock, _locked re-verifies the path still names the
    # locked inode; when the identity stat cannot be taken (a ghost inode
    # reclaimed underneath a waiter) it re-opens and contends afresh.
    backend = fs_backend
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "ghost.lock")
    _os_one_shot(monkeypatch, "stat", lock_path)
    # the first samestat's os.stat raises -> loop retries and then acquires.
    with backend._locked(lock_path):
        pass
    assert os.path.exists(lock_path)


async def test_locked_touch_without_fd_utime_support(fs_backend, monkeypatch):
    # On a platform whose os.utime cannot take a file descriptor, the
    # touch=True path refreshes the lock's mtime by PATH instead.
    backend = fs_backend
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "touch.lock")
    monkeypatch.setattr(os, "supports_fd", set())
    with backend._locked(lock_path, touch=True):
        pass
    assert os.path.exists(lock_path)


async def test_locked_bootstrap_write_loss_is_not_an_error(
    fs_backend, monkeypatch
):
    # Two contenders can both open a FRESH lock file and observe size 0;
    # the first writes the bootstrap byte and takes the byte-range lock,
    # and on Windows the loser's own write then lands on the locked range
    # and fails with EACCES (PermissionError).  The loser must fall
    # through and contend on the lock like any other second-comer, not
    # blow PermissionError out of the state op (it escaped acquire_lease
    # and killed a whole retry-claim pass).  Simulated on every platform:
    # the fake write installs the rival's byte and raises what the
    # Windows kernel would.
    backend = fs_backend
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "race.lock")
    real_write = os.write

    def _rival_won(fdesc, data):
        try:
            ours = os.path.samestat(os.fstat(fdesc), os.stat(lock_path))
        except OSError:
            ours = False
        if not ours:
            return real_write(fdesc, data)
        real_write(fdesc, b"\0")  # the rival's byte, already on disk
        raise PermissionError(13, "byte range held by the rival")

    monkeypatch.setattr(os, "write", _rival_won)
    entered = False
    with backend._locked(lock_path):
        entered = True
    assert entered


async def test_locked_bootstrap_write_race_real_byte_lock(
    fs_backend_factory, monkeypatch
):
    if not state.IS_WINDOWS:
        pytest.skip("msvcrt byte-range lock semantics are Windows-only")
    import msvcrt

    # The same race against the real kernel: a rival holds the locked
    # bootstrap byte while our first fstat still reports the empty file
    # the loser saw before the rival won (the stale observation that
    # makes it attempt the write).  The write fails with the genuine
    # EACCES; _locked must wait the rival out and acquire, not raise.
    backend = await fs_backend_factory()
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "kernel.lock")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    rival = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    os.write(rival, b"\0")
    os.lseek(rival, 0, os.SEEK_SET)
    msvcrt.locking(rival, msvcrt.LK_NBLCK, 1)
    real_fstat = os.fstat
    lied = {"done": False}

    def _stale_fstat(fdesc):
        result = real_fstat(fdesc)
        if not lied["done"]:
            try:
                ours = os.path.samestat(result, os.stat(lock_path))
            except OSError:
                ours = False
            if ours:
                lied["done"] = True
                values = list(result)
                values[6] = 0  # st_size as the loser saw it pre-race
                return os.stat_result(tuple(values))
        return result

    monkeypatch.setattr(os, "fstat", _stale_fstat)

    def _release():
        time.sleep(0.3)
        os.lseek(rival, 0, os.SEEK_SET)
        msvcrt.locking(rival, msvcrt.LK_UNLCK, 1)
        os.close(rival)

    releaser = threading.Thread(target=_release)
    releaser.start()
    entered = False
    try:
        with backend._locked(lock_path):
            entered = True
    finally:
        releaser.join()
    assert entered


async def test_gc_keeps_stream_when_listdir_fails(fs_backend, monkeypatch):
    # A managed candidate stream whose directory cannot be listed (a
    # transient I/O error mid-sweep) is kept, never partially collected.
    backend = fs_backend
    now = time.time()
    monkeypatch.setattr(state, "_now", lambda: now + 30 * 86400.0)
    await backend.append_record("runs/unreadable", {"x": 1})
    stream_dir = backend._stream_dir("runs/unreadable")
    _os_raiser(monkeypatch, "listdir", [stream_dir])
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 0
    assert os.path.isdir(stream_dir)


async def test_gc_empty_stream_dir_vanishing_mid_scan_is_kept(
    fs_backend, monkeypatch
):
    # An empty managed dir whose stat fails (it was rmdir'd between the
    # listing and the age check) is treated as brand new (newest == +inf)
    # and kept, not deleted on sight.
    backend = fs_backend
    now = time.time()
    monkeypatch.setattr(state, "_now", lambda: now + 30 * 86400.0)
    stream_dir = backend._stream_dir("runs/ghost")
    os.makedirs(stream_dir, exist_ok=True)
    target = os.path.abspath(stream_dir)
    real_listdir = os.listdir

    def vanishing_listdir(path, *a, **k):
        if os.path.abspath(str(path)) == target:
            result = real_listdir(path, *a, **k)
            # empty listing, then the dir disappears before its stat.
            try:
                os.rmdir(path)
            except OSError:
                pass
            return result
        return real_listdir(path, *a, **k)

    monkeypatch.setattr(os, "listdir", vanishing_listdir)
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 0


async def test_lease_dead_past_grace_false_when_read_returns_none(
    fs_backend, monkeypatch
):
    # The dead-past-grace judge returns False (never reclaim) when the lease
    # stats fine but reads back positively absent -- an ambiguous state that
    # must not be classified as safely dead.
    backend = fs_backend
    lease_path = os.path.join(backend.base, state.LEASES_DIR, "x.lease")
    with open(lease_path, "wb") as fobj:
        fobj.write(b"{}")
    monkeypatch.setattr(backend, "_read_lease_file", lambda *a, **k: None)
    assert backend._lease_dead_past_grace(lease_path, time.time() + 100) is False


async def test_gc_leases_rejudge_keeps_revived_lease(fs_backend, monkeypatch):
    # An ephemeral lease that passes the cheap pre-check but is re-judged
    # NOT-dead under the lock (a concurrent re-acquire revived it) is left
    # untouched.
    backend = fs_backend
    lease = await backend.acquire_lease("dagadvance/d/r", "A", ttl=10.0)
    assert lease is not None
    _lock, lease_path = backend._lease_paths("dagadvance/d/r")

    verdicts = iter([True, False])
    monkeypatch.setattr(
        backend,
        "_lease_dead_past_grace",
        lambda *a, **k: next(verdicts, False),
    )
    r = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert r["leases_removed"] == 0
    assert os.path.exists(lease_path)


async def test_gc_leases_windows_post_release_unlink(fs_backend, monkeypatch):
    # On Windows the dead ephemeral lease's .lock sibling is unlinked AFTER
    # releasing the flock (own handle closed), best-effort.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    lease = await backend.acquire_lease("dagadvance/d/w", "A", ttl=10.0)
    assert lease is not None
    lock_path, lease_path = backend._lease_paths("dagadvance/d/w")

    monkeypatch.setattr(state, "IS_WINDOWS", True)
    clock["t"] = t0 + 3600.0 + 120.0
    r = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert r["leases_removed"] == 1
    assert not os.path.exists(lease_path)
    assert not os.path.exists(lock_path)


async def test_gc_orphan_locks_skips_unreadable_namespace(
    fs_backend, monkeypatch
):
    # The orphan-lock sweep skips a document namespace directory it cannot
    # list rather than crashing the whole pass.
    backend = fs_backend
    await backend.mutate_document("kv/ns", "k", lambda _c: ({"v": 1}, None))
    ns_dir = os.path.dirname(backend._doc_paths("kv/ns", "k")[0])
    _os_raiser(monkeypatch, "listdir", [ns_dir])
    # must not raise; the (unlistable) namespace simply contributes nothing.
    r = await backend.collect_garbage(keep={}, grace=3600.0)
    assert r["locks_removed"] == 0


async def test_reclaim_idle_lock_absent_lock_is_noop(fs_backend):
    # A .lock whose stat fails (already gone / unreadable) cannot be
    # classified, so it is not reclaimed.
    backend = fs_backend
    missing = os.path.join(backend.base, state.LEASES_DIR, "vanished.lock")
    sibling = os.path.join(backend.base, state.LEASES_DIR, "vanished.lease")
    assert (
        backend._reclaim_idle_lock_sync(
            missing, sibling, time.time(), False
        )
        is False
    )


async def test_reclaim_idle_lock_rejudge_touched_under_lock(
    fs_backend, monkeypatch
):
    # Under the flock the lock's mtime is re-read; a concurrent acquire that
    # just touched it (mtime now past the cutoff) aborts the reclaim.
    backend = fs_backend
    now = time.time()
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "touched.lock")
    sibling = os.path.join(backend.base, state.LEASES_DIR, "touched.lease")
    with open(lock_path, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(lock_path, (now - 1000.0, now - 1000.0))

    def touch(path):
        os.utime(path, (now + 1000.0, now + 1000.0))

    monkeypatch.setattr(
        backend, "_locked", lambda p, **k: _DummyLock(p, touch)
    )
    assert (
        backend._reclaim_idle_lock_sync(lock_path, sibling, now, False)
        is False
    )
    assert os.path.exists(lock_path)


async def test_reclaim_idle_lock_vanishes_under_lock(fs_backend, monkeypatch):
    # Under the flock the lock's re-stat can fail (it vanished): abort.
    backend = fs_backend
    now = time.time()
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "gone.lock")
    sibling = os.path.join(backend.base, state.LEASES_DIR, "gone.lease")
    with open(lock_path, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(lock_path, (now - 1000.0, now - 1000.0))

    def remove(path):
        os.unlink(path)

    monkeypatch.setattr(
        backend, "_locked", lambda p, **k: _DummyLock(p, remove)
    )
    assert (
        backend._reclaim_idle_lock_sync(lock_path, sibling, now, False)
        is False
    )


async def test_reclaim_idle_lock_sibling_reappears_under_lock(
    fs_backend, monkeypatch
):
    # Under the flock the sibling data file is re-checked; if a concurrent
    # actor re-created it, the lock is no longer orphaned: abort.
    backend = fs_backend
    now = time.time()
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "resurrect.lock")
    sibling = os.path.join(backend.base, state.LEASES_DIR, "resurrect.lease")
    with open(lock_path, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(lock_path, (now - 1000.0, now - 1000.0))

    def recreate_sibling(_path):
        with open(sibling, "wb") as fobj:
            fobj.write(b"\0")

    monkeypatch.setattr(
        backend, "_locked", lambda p, **k: _DummyLock(p, recreate_sibling)
    )
    assert (
        backend._reclaim_idle_lock_sync(lock_path, sibling, now, False)
        is False
    )
    assert os.path.exists(lock_path)


async def test_reclaim_idle_lock_windows_post_release_unlink(
    fs_backend, monkeypatch
):
    # On Windows the reclaim's unlink happens AFTER the flock is released,
    # best-effort, and still reports the lock reclaimed.
    backend = fs_backend
    now = time.time()
    lock_path = os.path.join(backend.base, state.LEASES_DIR, "winlock.lock")
    sibling = os.path.join(backend.base, state.LEASES_DIR, "winlock.lease")
    with open(lock_path, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(lock_path, (now - 1000.0, now - 1000.0))
    monkeypatch.setattr(state, "IS_WINDOWS", True)
    assert (
        backend._reclaim_idle_lock_sync(lock_path, sibling, now, False)
        is True
    )
    assert not os.path.exists(lock_path)


async def test_sweep_dir_tolerates_unlink_oserror(fs_backend, monkeypatch):
    # _sweep_dir_sync swallows a per-file OSError (a racing delete / hold)
    # and moves on rather than aborting the whole sweep.
    backend = fs_backend
    sweep_dir = os.path.join(backend.base, state.TMP_DIR)
    os.makedirs(sweep_dir, exist_ok=True)
    aged = os.path.join(sweep_dir, "aged.tmp")
    with open(aged, "wb") as fobj:
        fobj.write(b"x")
    old = time.time() - 10000.0
    os.utime(aged, (old, old))
    _os_raiser(monkeypatch, "unlink", [aged])
    # the unlink raises and is swallowed: nothing counted, file survives.
    assert backend._sweep_dir_sync(sweep_dir, time.time(), False) == 0
    assert os.path.exists(aged)


async def test_migrate_skips_unreadable_stream_dir(fs_backend, monkeypatch):
    # migrate_schema skips a stream directory it cannot list rather than
    # failing the whole migration pass.  A convertible legacy record sits in
    # the unlistable stream: because the stream is skipped it is never read,
    # so nothing is converted (proving the skip, not a silent read).
    backend = fs_backend
    monkeypatch.setitem(
        state.RECORD_MIGRATIONS, "v0", lambda data: {"outcome": "x"}
    )
    _write_raw_record(
        backend,
        "runs/m",
        "00000000000000000001-old-000000000001.json",
        {"schemaVersion": "v0", "data": {"result": "ok"}},
    )
    stream_dir = backend._stream_dir("runs/m")
    _os_raiser(monkeypatch, "listdir", [stream_dir])
    result = await backend.migrate_schema()
    # the unlistable stream is skipped: its convertible record is untouched.
    assert result["converted"] == 0
    assert result["unreadable"] == 0


async def test_blob_sweep_tolerates_unlink_oserror(fs_backend, monkeypatch):
    # An aged, unreferenced blob whose unlink raises is left in place and
    # not counted; the sweep does not abort.
    backend = fs_backend
    digest = await backend.put_blob(b"orphaned-artifact")
    blob_path = backend._blob_path(digest)
    old = time.time() - 10000.0
    os.utime(blob_path, (old, old))
    _os_raiser(monkeypatch, "unlink", [blob_path])
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 0
    assert await backend.get_blob(digest) == b"orphaned-artifact"


async def test_inventory_skips_unreadable_stream_node(fs_backend, monkeypatch):
    # The inventory walk skips a stream directory it cannot list rather than
    # failing the whole snapshot.
    backend = fs_backend
    await backend.append_record("runs/inv", {"x": 1})
    stream_dir = backend._stream_dir("runs/inv")
    _os_raiser(monkeypatch, "listdir", [stream_dir])
    inv = await backend.inventory()
    # the unreadable node drops out of the grouping; the snapshot returns.
    assert inv["enumerable"] is True
    assert "runs" not in inv["records"]


async def test_inventory_tolerates_lease_read_exception(
    fs_backend, monkeypatch
):
    # A lease file that raises on read during the inventory walk is observed
    # best-effort (None) and skipped, never crashing the snapshot.
    backend = fs_backend
    lease = await backend.acquire_lease("leader", "node-a", ttl=30.0)
    assert lease is not None

    def _boom(*_a, **_k):
        raise RuntimeError("lease read blew up")

    monkeypatch.setattr(backend, "_read_lease_file", _boom)
    inv = await backend.inventory()
    assert inv["leases"] == []


# =====================================================================
# Adversarial-review hardening of cronstable.state
# (formerly tests/test_state_hardening.py; merged per finding B18,
# rationale below kept verbatim from that module's docstring)
# =====================================================================
# Regression tests for the adversarial-review hardening of cronstable.state.
#
# Each test pins a CONFIRMED bug fixed in the hardening pass; if a fix
# regresses, the matching test fails.  Covered invariants:
#
# * record ids stay unique when several worker threads append at once (the
#   ``_seq`` counter is now behind a lock);
# * lease fences are monotonic for the life of the store, across release and
#   expiry (release marks the lease expired in place, never deletes it);
# * an unreadable lease file fails CLOSED on the locked paths and stays
#   best-effort on the unlocked observer;
# * poison record CONTENT is quarantined while transient I/O errors are not;
# * a failed atomic write never leaks its temp file;
# * two backend instances over one mount (the shared-mount deployment) keep a
#   coherent, non-colliding ledger;
# * ``~`` in ``state.path`` means the home directory, not a literal ``~``
#   directory under the daemon's CWD;
# * length-truncated stream tokens round-trip through ``list_stream_names``
#   via the logical-name sidecar, and a legacy truncated dir without one is
#   skipped/kept -- never returned garbled or collected;
# * garbage collection reclaims ONLY the ephemeral per-run lease classes
#   (``dagadvance/``) once provably dead past the whole grace window --
#   never slot/retry-claim/election leases, whose fences persist in durable
#   slot cancel records -- and sweeps orphaned ``.lock`` side-files (a
#   deleted document's, a bare lease lock's) only once idle past the grace,
#   while ``delete_document`` itself never unlinks the ``.lock`` (an eager
#   unlink split the document mutex across nodes on NFS);
# * stream/document-namespace enumeration reports hidden (unnameable)
#   entries, the orphan-blob sweep's age guard keeps young payloads, and a
#   dedupe re-put re-arms that guard -- the KEEP biases the blob sweep
#   builds on;
# * the document delete path rides out Windows sharing violations like the
#   replace-write path does;
# * stream/document-namespace enumeration decodes a lone-surrogate name
#   exactly (the surrogatepass inverse of ``_fs_safe``), and a token that
#   cannot round-trip is reported through ``complete``, never returned
#   garbled;
# * record filenames stay monotonic per stream across a backward wall-clock
#   step, so the amortised prune never deletes the just-written record and
#   the derive_max watermark never hides it.
#
# No tight wall-clock timing anywhere: lease expiry is driven by
# monkeypatching ``cronstable.state._now`` (the one time source), so
# nothing here depends on the coarse Windows clock.


# --- 1: record-id uniqueness under thread concurrency ----------------------


async def test_append_sync_ids_unique_under_thread_contention(
    fs_backend, monkeypatch
):
    # The sync halves run on daemon worker threads, several of which can be
    # in flight at once (two jobs finishing together each schedule an
    # append).  Before the fix `self._seq += 1` was an unlocked read-modify-
    # write; two threads could interleave it, and a duplicated seq plus the
    # coarse Windows clock meant a duplicated record id -- one record
    # silently clobbering another via the atomic rename.
    backend = fs_backend
    # Freeze the clock: the record id is "<epoch>-<instance>-<seq>", so with
    # a frozen epoch (and a single instance) uniqueness rests ENTIRELY on
    # the locked counter, making a seq race a deterministic id collision
    # instead of a needs-the-right-millisecond one.
    monkeypatch.setattr(state, "_now", lambda: 1000.0)
    n_threads, per_thread = 8, 50
    buckets = [[] for _ in range(n_threads)]
    errors = []

    def worker(bucket):
        try:
            for i in range(per_thread):
                bucket.append(backend._append_sync("s", {"i": i}))
        except BaseException as ex:  # noqa: BLE001 - relayed to the test
            errors.append(ex)

    threads = [
        threading.Thread(target=worker, args=(buckets[t],))
        for t in range(n_threads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    ids = [rid for bucket in buckets for rid in bucket]
    assert len(ids) == n_threads * per_thread
    # every id unique: a duplicate means one record overwrote another.
    assert len(set(ids)) == n_threads * per_thread
    got = await backend.list_records("s")
    assert len(got) == n_threads * per_thread


# --- 2: lease fence monotonicity ------------------------------------------


async def test_fence_monotonic_across_release(fs_backend):
    # Release used to unlink the lease file -- the fence counter's only home
    # -- so the next acquire restarted at fence=1, re-issuing a fence value
    # already handed out and defeating stale-writer detection.  Release now
    # marks the lease expired IN PLACE, so the counter survives.
    backend = fs_backend
    a = await backend.acquire_lease("L", "A", ttl=30)
    assert a is not None and a.fence == 1
    await backend.release_lease(a)
    b = await backend.acquire_lease("L", "B", ttl=30)
    assert b is not None and b.holder == "B"
    assert b.fence > a.fence  # regression: reset to 1 after release
    assert b.fence == 2


async def test_fence_bumps_on_expiry_and_stale_renew_denied(
    fs_backend, monkeypatch
):
    backend = fs_backend
    # drive expiry through the module's one time source instead of real
    # sleeps: deterministic on the coarse Windows clock.
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    a = await backend.acquire_lease("L", "A", ttl=5)  # expires at 1005
    assert a is not None and a.fence == 1
    clock["t"] = 1010.0  # past expiry
    b = await backend.acquire_lease("L", "B", ttl=5)
    assert b is not None and b.holder == "B"
    assert b.fence == a.fence + 1  # takeover must bump the fence
    # A's renew with its stale lease is fenced off, not honoured.
    assert await backend.renew_lease(a, ttl=5) is None


# --- 3: unreadable lease fails closed --------------------------------------


async def test_corrupt_lease_fails_closed_on_acquire(fs_backend):
    # An unreadable lease is not a FREE lease.  Before the fix a corrupt (or
    # transiently unreadable) lease file read as "no lease", letting an
    # acquirer steal a possibly-valid lease from its live holder with a
    # reset fence=1.  The locked paths must deny instead.
    backend = fs_backend
    _lock_path, lease_path = backend._lease_paths("L")
    os.makedirs(os.path.dirname(lease_path), exist_ok=True)
    with open(lease_path, "w") as fobj:
        fobj.write("{definitely not json")
    assert await backend.acquire_lease("L", "H", ttl=30) is None
    # the unlocked observer stays best-effort: None, never an exception.
    assert await backend.read_lease("L") is None


# --- 4: release keeps the file, observers see it as free -------------------


async def test_release_marks_expired_in_place_keeps_file(fs_backend):
    backend = fs_backend
    lease = await backend.acquire_lease("L", "A", ttl=30)
    assert lease is not None
    await backend.release_lease(lease)
    # observers must see "nobody holds it" after a release...
    assert await backend.read_lease("L") is None
    # ...but the file must still exist: it is what preserves the fence
    # counter across release/re-acquire cycles (see the monotonicity test).
    _lock_path, lease_path = backend._lease_paths("L")
    assert os.path.exists(lease_path)
    with open(lease_path, "rb") as fobj:
        on_disk = json.loads(fobj.read())
    assert on_disk["expiresAt"] == 0.0


# --- 5: _read_record error taxonomy ----------------------------------------


async def test_invalid_json_record_is_quarantined(fs_backend):
    # Bad CONTENT (truncated JSON from a crash mid-write on a store without
    # atomic rename) is the record's fault: quarantine it so one poison
    # object can never brick every later read of the stream.
    backend = fs_backend
    await backend.append_record("s", {"good": True})
    stream_dir = backend._stream_dir("s")
    with open(os.path.join(stream_dir, "00000-bad.json"), "w") as fobj:
        fobj.write("{not json")
    assert await backend.list_records("s") == [{"good": True}]
    assert "00000-bad.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-bad.json") for n in os.listdir(quarantine))


async def test_deeply_nested_json_record_is_quarantined(fs_backend):
    # A hostile >1000-deep nesting makes json.loads raise RecursionError,
    # which is NOT an OSError or ValueError: before the fix it escaped the
    # except clauses and crashed whichever caller was reading the stream.
    # Content poison must be caught and quarantined like any bad record.
    backend = fs_backend
    await backend.append_record("s", {"ok": 1})
    stream_dir = backend._stream_dir("s")
    bomb = "[" * 1300 + "]" * 1300
    with open(os.path.join(stream_dir, "00000-deep.json"), "w") as fobj:
        fobj.write(bomb)
    got = await backend.list_records("s")  # must not raise
    assert got == [{"ok": 1}]
    assert "00000-deep.json" not in os.listdir(stream_dir)
    quarantine = os.path.join(backend.base, "quarantine")
    assert any(n.startswith("00000-deep.json") for n in os.listdir(quarantine))


async def test_transient_read_error_skips_but_never_quarantines(
    fs_backend, monkeypatch
):
    # A transient I/O error (an NFS blip, an AV scanner's momentary hold) is
    # the ENVIRONMENT's fault, not the record's.  Quarantining on it would
    # eject perfectly valid history -- and regress the derived watermark --
    # on every store hiccup.  The read must skip the record for this pass
    # and leave the file in place.
    backend = fs_backend
    await backend.append_record("s", {"i": 0})
    await backend.append_record("s", {"i": 1})
    stream_dir = backend._stream_dir("s")
    names = sorted(n for n in os.listdir(stream_dir) if n.endswith(".json"))
    victim = os.path.normpath(os.path.join(stream_dir, names[0]))
    real_open = open
    failing = {"on": True}

    def flaky_open(path, *args, **kwargs):
        if failing["on"] and os.path.normpath(str(path)) == victim:
            raise PermissionError(13, "transient hold", str(path))
        return real_open(path, *args, **kwargs)

    # shadow the module's open, the same seam tests/test_state.py patches.
    monkeypatch.setattr(state, "open", flaky_open, raising=False)
    got = await backend.list_records("s")
    assert [r["i"] for r in got] == [1]  # unreadable one skipped
    # still in the stream (NOT quarantined), and quarantine stays empty.
    assert names[0] in os.listdir(stream_dir)
    assert os.listdir(os.path.join(backend.base, "quarantine")) == []
    # once the blip clears the record is readable again, proving the file
    # was left fully intact.
    failing["on"] = False
    assert [r["i"] for r in await backend.list_records("s")] == [0, 1]


# --- 6: atomic-write temp-file hygiene --------------------------------------


async def test_failed_replace_leaves_no_tmp_files(fs_backend, monkeypatch):
    # A failed rename must not strand its temp file: on a long-lived store
    # leaked w-*.tmp files accumulate forever (and on a shared mount every
    # node's leaks pile into the same directory).
    backend = fs_backend

    def boom(*args):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(
        state.FilesystemStateBackend, "_replace", staticmethod(boom)
    )
    with pytest.raises(OSError, match="simulated rename failure"):
        await backend.append_record("s", {"i": 0})
    tmp_dir = os.path.join(backend.base, "tmp")
    assert [n for n in os.listdir(tmp_dir) if n.endswith(".tmp")] == []
    # and the half-written record never became visible either.
    monkeypatch.undo()
    assert await backend.list_records("s") == []


# --- 7: two instances over one mount (shared-mount simulation) --------------


async def test_two_instances_share_one_ledger(fs_backend_factory):
    # The shared-mount deployment: two processes (here: two backend
    # instances) over the SAME store and namespace.  Ids must never collide
    # (each instance mixes its own random `_instance` token into every
    # filename), reads must see the union, and the derived cursor must be
    # the true max regardless of which node wrote it.
    b1 = await fs_backend_factory()
    b2 = await fs_backend_factory()
    assert b1._instance != b2._instance
    stream = "runs/shared-job"
    ids = []
    for i in range(5):  # interleaved appends, as two live nodes produce
        ids.append(
            await b1.append_record(stream, {"finished_at": 2 * i, "n": 1})
        )
        ids.append(
            await b2.append_record(stream, {"finished_at": 2 * i + 1, "n": 2})
        )
    assert len(set(ids)) == 10  # no cross-instance id collision
    seen1 = await b1.list_records(stream)
    seen2 = await b2.list_records(stream)
    assert len(seen1) == len(seen2) == 10
    union = {(r["n"], r["finished_at"]) for r in seen1}
    assert union == {(r["n"], r["finished_at"]) for r in seen2}
    assert {r["n"] for r in seen1} == {1, 2}  # both writers visible
    # the max (9) was written by node 2 but must be derived identically
    # from either node -- order-independent, never last-writer-wins.
    assert await b1.derive_max(stream, "finished_at") == 9
    assert await b2.derive_max(stream, "finished_at") == 9
    # one node prunes while the other lists: neither may raise.  The racing
    # list may observe any prefix of the deletes; only convergence matters.
    removed, racing = await asyncio.gather(
        b1.prune_records(stream, keep=3),
        b2.list_records(stream),
    )
    assert 3 <= len(racing) <= 10
    # a Windows lister can transiently hold a record open, making prune's
    # unlink of that one file fail (silently skipped by design); a second
    # pass after the reader finished sweeps any such straggler.
    swept = await b1.prune_records(stream, keep=3)
    assert removed + swept == 7
    assert len(await b1.list_records(stream)) == 3
    assert len(await b2.list_records(stream)) == 3


# --- 8: ~ expansion ----------------------------------------------------------


def test_tilde_path_expands_to_home():
    # `path: ~/state` must mean the home directory: before the fix the raw
    # "~" went through abspath, creating a literal "~" directory under
    # whatever CWD the daemon started in.  Construction only -- no start(),
    # so nothing is created under the real home.
    backend = _backend("~/some-cronstable-test-path")
    home = os.path.expanduser("~")
    expected = os.path.abspath(os.path.join(home, "some-cronstable-test-path"))
    assert backend.root == expected
    assert backend.root.startswith(home)


# --- 9: renew cannot resurrect a released lease -----------------------------


async def test_renew_after_release_is_denied(fs_backend):
    # Release marks the lease expired in place with the SAME holder+fence
    # (that is what keeps the fence monotonic), so a renew that checks only
    # holder+fence would still match and silently un-release it -- an
    # in-flight renew loop racing shutdown would resurrect the lease and
    # block other nodes for a full TTL after a clean release.
    backend = fs_backend
    lease = await backend.acquire_lease("leader", "node-a", 30.0)
    assert lease is not None
    await backend.release_lease(lease)
    assert await backend.read_lease("leader") is None
    assert await backend.renew_lease(lease, 30.0) is None
    # ...and the lease is genuinely still free for the next holder, with a
    # bumped fence.
    lease2 = await backend.acquire_lease("leader", "node-b", 30.0)
    assert lease2 is not None
    assert lease2.fence > lease.fence


# --- 10: a failed lease write denies instead of raising ----------------------


async def test_lease_write_failure_denies_instead_of_raising(
    fs_backend, monkeypatch
):
    # On Windows a reader/AV holding the .lease file open past the replace
    # retries surfaces PermissionError from the write; the lease API must
    # translate that into a clean denial (fail closed), never an exception
    # escaping acquire/renew to its caller.
    backend = fs_backend
    lease = await backend.acquire_lease("leader", "node-a", 30.0)
    assert lease is not None

    def boom(path, obj, *, durable=True):
        raise PermissionError(5, "sharing violation", path)

    monkeypatch.setattr(backend, "_write_lease_file", boom)
    assert await backend.renew_lease(lease, 30.0) is None
    monkeypatch.setattr(
        state, "_now", lambda: lease.expires_at + 1.0
    )  # expire it
    assert await backend.acquire_lease("leader", "node-b", 30.0) is None


# --- 11: truncated stream tokens round-trip through list_stream_names -------


async def test_list_stream_names_roundtrips_truncated_tokens(fs_backend):
    # _fs_safe truncates a >_FS_SAFE_MAX token to head + "%." + digest, and
    # unquote()ing that token back produced a GARBLED name that re-encoded
    # to a DIFFERENT token: a host with a long (or multibyte -- 9 encoded
    # chars per CJK char) hostname had its manifest stream invisible to
    # every GC keep-set builder, and its jobs' durable state was collected
    # as garbage.  The name sidecar written on append must make every
    # returned name round-trip exactly to its on-disk token.
    backend = fs_backend
    long_ascii = "manifests/" + "H" * 140  # uppercase: 3 encoded chars each
    multibyte = "manifests/" + "主机" * 20  # a CJK hostname
    plain = "manifests/plain-host"
    for stream in (long_ascii, multibyte, plain):
        await backend.append_record(stream, {"x": 1})
    names = await backend.list_stream_names("manifests/")
    assert set(names) == {long_ascii, multibyte, plain}
    # the property under test: _fs_safe over every returned name reproduces
    # exactly the set of on-disk stream tokens (nothing garbled, nothing
    # skipped), so keep-sets built from these names protect the real dirs.
    records_root = os.path.join(backend.base, "records")
    prefix_token = state._fs_safe_fragment("manifests/")
    on_disk = {
        t for t in os.listdir(records_root) if t.startswith(prefix_token)
    }
    assert {state._fs_safe(n) for n in names} == on_disk
    # and each name feeds straight back into a record read, the exact call
    # the GC keep-set builders make.
    for name in names:
        assert await backend.list_records(name) == [{"x": 1}]


async def test_gc_keeps_legacy_truncated_stream_and_skips_its_name(
    fs_backend, monkeypatch
):
    # A store written BEFORE the sidecar existed: a truncated dir's logical
    # name is unrecoverable, so enumeration must SKIP it (never return a
    # garbled, non-round-tripping name) and GC must treat it as
    # unclassifiable (keep) -- until an append lands the sidecar and makes
    # it classifiable again.
    backend = fs_backend
    clock = {"t": 1000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    long_job = "runs/" + "J" * 200
    await backend.append_record(long_job, {"x": 1})
    stream_dir = backend._stream_dir(long_job)
    # simulate the legacy dir by removing the sidecar the append wrote.
    os.unlink(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))
    assert await backend.list_stream_names("runs/") == []
    # aged far past the grace and unreferenced: still KEPT, because its
    # absence from the keep map proves nothing (the builder never saw it).
    clock["t"] = 1000.0 + 8 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 0
    assert await backend.list_records(long_job) == [{"x": 1}]
    # the next append self-heals the sidecar: enumerable again...
    await backend.append_record(long_job, {"x": 2})
    assert await backend.list_stream_names("runs/") == [long_job]
    # ...and therefore classifiable: aged and unreferenced now really goes.
    clock["t"] = 1000.0 + 16 * 86400.0
    result = await backend.collect_garbage(
        keep={"runs/": set()}, grace=7 * 86400.0
    )
    assert result["streams_removed"] == 1
    assert not os.path.isdir(stream_dir)


async def test_list_stream_names_audit_reports_hidden_streams(fs_backend):
    # The orphan-blob sweep builds its referenced-digest set from the stream
    # listing, so it must be able to tell "these are ALL the artifact
    # streams" from "a legacy truncated dir is hiding one": a hidden
    # stream's records still reference blobs, and a sweep that could not
    # see them would delete live payloads.
    backend = fs_backend
    long_scope = "artifacts/" + "S" * 200
    await backend.append_record("artifacts/plain", {"sha256": "a" * 64})
    await backend.append_record(long_scope, {"sha256": "b" * 64})
    names, complete = await backend.list_stream_names_audit("artifacts/")
    assert complete is True
    assert set(names) == {"artifacts/plain", long_scope}
    # a legacy dir (pre-sidecar store): the stream becomes unnameable and
    # the audit must say so instead of silently shrinking the listing.
    stream_dir = backend._stream_dir(long_scope)
    os.unlink(os.path.join(stream_dir, state._STREAM_NAME_SIDECAR))
    names, complete = await backend.list_stream_names_audit("artifacts/")
    assert complete is False
    assert names == ["artifacts/plain"]
    # the plain (non-audit) listing keeps its established best-effort shape.
    assert await backend.list_stream_names("artifacts/") == [
        "artifacts/plain"
    ]
    # a prefix with nothing hidden under it stays complete.
    names, complete = await backend.list_stream_names_audit("manifests/")
    assert (names, complete) == ([], True)


async def test_list_document_namespaces_skips_truncated_namespace(fs_backend):
    # The GC discovers dag-run namespaces (dagrun/<dag>) through this
    # listing; a length-truncated namespace has no name sidecar to recover
    # its logical name from, so it must be reported as an INCOMPLETE
    # listing -- never returned garbled (a garbled name would be treated as
    # a removed dag and its runs' XCom scopes left unprotected).
    backend = fs_backend

    def put(body):
        return lambda _cur: (body, None)

    await backend.mutate_document("dagrun/a", "r1", put({"runId": "1"}))
    await backend.mutate_document("dagrun/b", "r1", put({"runId": "2"}))
    await backend.mutate_document("kv/x", "k", put({"value": 1}))
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert (names, complete) == (["dagrun/a", "dagrun/b"], True)
    await backend.mutate_document(
        "dagrun/" + "D" * 200, "r1", put({"runId": "3"})
    )
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert names == ["dagrun/a", "dagrun/b"]
    assert complete is False
    # unrelated prefixes are unaffected by the truncated dagrun namespace.
    assert await backend.list_document_namespaces("kv/") == (["kv/x"], True)


async def test_sweep_orphan_blobs_keeps_young_unreferenced_blobs(fs_backend):
    # The put-blob-then-append-record window: a payload that has just
    # landed has no record yet, so an unreferenced-but-young blob must
    # survive the sweep (the grace is the age guard) and only a blob both
    # unreferenced AND older than the grace may go.
    backend = fs_backend
    digest = await backend.put_blob(b"just-landed")
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 0
    assert await backend.get_blob(digest) is not None
    old = time.time() - 7200.0
    os.utime(backend._blob_path(digest), (old, old))
    # dry run counts it but must not delete...
    assert (
        await backend.sweep_orphan_blobs(set(), 3600.0, dry_run=True) == 1
    )
    assert await backend.get_blob(digest) is not None
    # ...the real pass reclaims it.
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 1
    assert await backend.get_blob(digest) is None


async def test_put_blob_dedupe_rearms_the_sweep_age_guard(fs_backend):
    # a re-put of identical content dedupes to the EXISTING blob file; if
    # that file kept its old mtime, a sweep racing the re-putter's
    # record append would read the blob as an aged orphan (its previous
    # references may be mid-deletion) and delete a payload about to be
    # referenced again.  The dedupe hit must refresh the mtime, re-arming
    # the age guard.
    backend = fs_backend
    digest = await backend.put_blob(b"republished-content")
    old = time.time() - 7200.0
    os.utime(backend._blob_path(digest), (old, old))
    assert await backend.put_blob(b"republished-content") == digest
    assert await backend.sweep_orphan_blobs(set(), 3600.0) == 0
    assert await backend.get_blob(digest) == b"republished-content"


def test_blob_path_rejects_a_non_sha256_digest(tmp_path):
    # A digest is a content-addressed lowercase sha256 hex string.  A crafted
    # value (e.g. a "sha256" field from a malicious restore archive) must be
    # rejected before it builds a path, so it can never traverse out of the
    # blob directory via ".." or a path separator.
    backend = _backend(tmp_path)
    for bad in ("../../etc/passwd", "0" * 63, "0" * 65, "g" * 64, "AB" * 32):
        with pytest.raises(ValueError, match="invalid blob digest"):
            backend._blob_path(bad)
    # a well-formed digest still yields a normal sharded path.
    good = "a" * 64
    assert backend._blob_path(good).endswith(good + ".blob")


# --- 12: GC reclaims ONLY ephemeral leases, only once dead past grace -------


async def test_gc_reclaims_ephemeral_leases_dead_past_grace_only(
    fs_backend, monkeypatch
):
    # dagrun takes one uniquely-named advance lease per DAG run, and nothing
    # ever deleted the files: ~210k permanent files/year for a 5-minute DAG.
    # GC must reclaim a lease (and its .lock sibling) matching an EPHEMERAL
    # prefix once dead past the whole grace window -- and never sooner.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=10.0)
    assert lease is not None
    lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    grace = 3600.0
    # expired, but within the grace window: never touched (the fence home).
    clock["t"] = t0 + 60.0
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 0
    assert os.path.exists(lease_path)
    # dead past the whole window: dry run counts it but deletes nothing...
    clock["t"] = t0 + grace + 60.0
    dry = await backend.collect_garbage(
        keep={},
        grace=grace,
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
        dry_run=True,
    )
    assert dry["leases_removed"] == 1
    assert os.path.exists(lease_path) and os.path.exists(lock_path)
    # ...and the real pass reclaims BOTH files.
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 1
    assert not os.path.exists(lease_path)
    assert not os.path.exists(lock_path)
    # without the prefix (a caller that names no ephemeral classes) the
    # same dead-past-grace lease would never have been touched.
    revived = await backend.acquire_lease("dagadvance/d/r2", "A", ttl=10.0)
    assert revived is not None
    clock["t"] = t0 + 2 * (grace + 60.0)
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["leases_removed"] == 0
    assert os.path.exists(backend._lease_paths("dagadvance/d/r2")[1])


async def test_gc_never_reclaims_non_ephemeral_leases(fs_backend, monkeypatch):
    # REGRESSION of the previous fix round: reclaiming ANY dead lease reset
    # slot fences that persisted Replace-cancel records still reference
    # ({kind: cancel, fence: N} in slots/<job> stays newest until the next
    # cancel), so a reborn fence could re-collide and a healthy future run
    # be silently cancelled.  A slot lease dead past ANY grace window must
    # survive GC and the next acquire must CONTINUE its fence line.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    first = await backend.acquire_lease("slots/j", "A", ttl=10.0)
    assert first is not None and first.fence == 1
    # the durable cancel record a Replace takeover leaves behind: it names
    # fence 1 and nothing will ever prune it until another cancel lands.
    await backend.append_record("slots/j", {"kind": "cancel", "fence": 1})
    await backend.release_lease(first)
    grace = 3600.0
    clock["t"] = t0 + grace + 60.0  # dead past the whole grace window
    result = await backend.collect_garbage(
        keep={"slots/": {"j"}},
        grace=grace,
        ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,),
    )
    assert result["leases_removed"] == 0
    lock_path, lease_path = backend._lease_paths("slots/j")
    assert os.path.exists(lease_path)  # the fence counter's only home
    assert os.path.exists(lock_path)
    # the fence line CONTINUES: the reborn holder is at fence 2, so the
    # stale fence-1 cancel record can never match it again.
    nxt = await backend.acquire_lease("slots/j", "B", ttl=10.0)
    assert nxt is not None and nxt.fence == first.fence + 1
    records = await backend.list_records("slots/j")
    assert records == [{"kind": "cancel", "fence": 1}]
    assert all(rec["fence"] != nxt.fence for rec in records)


async def test_gc_dates_released_lease_by_mtime_not_expiry(
    fs_backend, monkeypatch
):
    # release marks expiresAt 0.0 IN PLACE -- ancient by expiry alone.  An
    # ephemeral lease released moments ago must survive the full grace
    # window (a stale fence from just before the release could still be
    # live), so the sweep dates a release by the file's mtime and the
    # fence counter survives.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    lease = await backend.acquire_lease("dagadvance/d/r1", "A", ttl=30.0)
    assert lease is not None
    await backend.release_lease(lease)
    clock["t"] = t0 + 120.0  # well within the grace window
    result = await backend.collect_garbage(
        keep={}, grace=3600.0, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 0
    _lock_path, lease_path = backend._lease_paths("dagadvance/d/r1")
    assert os.path.exists(lease_path)  # the fence counter's only home
    nxt = await backend.acquire_lease("dagadvance/d/r1", "B", ttl=30.0)
    assert nxt is not None and nxt.fence == lease.fence + 1


async def test_lease_gc_fence_reset_cannot_enable_stale_ops(
    fs_backend, monkeypatch
):
    # The safety argument for deleting an ephemeral lease file at all, as a
    # test: a lease GC reclaims has every fence it ever issued expired >=
    # grace ago, so the post-reclaim fence reset to 1 must be unobservable
    # -- a stale Lease from before the reclaim (same holder, higher fence)
    # can neither renew nor release the reborn lease.
    backend = fs_backend
    t0 = time.time()
    clock = {"t": t0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    name = "dagadvance/d/rX"
    first = await backend.acquire_lease(name, "A", ttl=10.0)
    assert first is not None and first.fence == 1
    clock["t"] = t0 + 60.0
    stale = await backend.acquire_lease(name, "A", ttl=10.0)  # takeover
    assert stale is not None and stale.fence == 2
    grace = 3600.0
    clock["t"] = t0 + 60.0 + grace + 60.0  # both fences dead past grace
    result = await backend.collect_garbage(
        keep={}, grace=grace, ephemeral_lease_prefixes=(DAG_LEASE_PREFIX,)
    )
    assert result["leases_removed"] == 1
    lock_path, lease_path = backend._lease_paths(name)
    assert not os.path.exists(lease_path)
    assert not os.path.exists(lock_path)
    reborn = await backend.acquire_lease(name, "A", ttl=30.0)
    assert reborn is not None and reborn.fence == 1
    # fence EQUALITY (never >=) is what keeps the reset safe: the stale
    # renew and the stale release must both miss the reborn lease.
    assert await backend.renew_lease(stale, ttl=30.0) is None
    await backend.release_lease(stale)
    current = await backend.read_lease(name)
    assert current is not None
    assert current.holder == "A" and current.fence == 1  # untouched


# --- 13: delete_document leaves the .lock for the GC orphan sweep -----------


async def test_delete_document_leaves_lock_sibling_alone(fs_backend):
    # REGRESSION of the previous fix round: delete_document unlinked the
    # .lock side-file eagerly, which splits the document mutex across nodes
    # on a shared NFS/EFS store (a waiter that wins the flock on the ghost
    # inode can pass the post-acquire stat re-verify through a stale
    # dentry/attribute cache while another node locks a fresh file).  The
    # hot path must never unlink a doc .lock; reclamation belongs to the
    # GC orphan sweep, gated on idle-past-grace.
    backend = fs_backend

    def put(value):
        def transform(_current):
            return {"v": value}, None

        return transform

    await backend.mutate_document("dagrun/d", "r1", put(1))
    lock_path, doc_path = backend._doc_paths("dagrun/d", "r1")
    assert os.path.exists(doc_path) and os.path.exists(lock_path)
    assert await backend.delete_document("dagrun/d", "r1") is True
    assert not os.path.exists(doc_path)
    assert os.path.exists(lock_path)  # left for the GC sweep, never eager
    # the key stays fully usable: a fresh mutation recreates the doc under
    # the SAME lock file and reads back.
    body, _ = await backend.mutate_document("dagrun/d", "r1", put(2))
    assert body == {"v": 2}
    assert await backend.read_document("dagrun/d", "r1") == {"v": 2}
    assert os.path.exists(lock_path)


# --- 13b: the GC orphan-lock sweep ------------------------------------------


async def test_gc_sweeps_orphaned_doc_lock_only_when_doc_absent_and_idle(
    fs_backend,
):
    # a doc .lock is swept only when BOTH hold: the document is absent AND
    # the lock sat idle (mtime) past the whole grace window.  A present
    # document keeps its lock whatever the mtime says.
    backend = fs_backend
    grace = 3600.0
    old = time.time() - grace - 120.0

    await backend.mutate_document("kv/a", "k", lambda _c: ({"v": 1}, None))
    kept_lock, kept_doc = backend._doc_paths("kv/a", "k")
    os.utime(kept_lock, (old, old))  # ancient, but the doc is PRESENT

    await backend.mutate_document("kv/b", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/b", "k")
    young_lock, _young_doc = backend._doc_paths("kv/b", "k")
    # doc absent but the lock was just touched: still within the grace.

    await backend.mutate_document("kv/c", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/c", "k")
    dead_lock, dead_doc = backend._doc_paths("kv/c", "k")
    os.utime(dead_lock, (old, old))  # doc absent AND idle past grace

    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["locks_removed"] == 1
    assert os.path.exists(dead_lock)  # dry run deletes nothing
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 1
    assert os.path.exists(kept_lock)  # doc present: never swept
    assert os.path.exists(young_lock)  # idle clock not yet past grace
    assert not os.path.exists(dead_lock)
    assert not os.path.exists(dead_doc)


async def test_mutate_touch_refreshes_the_doc_lock_idle_clock(fs_backend):
    # a flock never updates mtime, so every mutate must utime the lock:
    # a doc-absent .lock that was CONTENDED recently (an idempotency key
    # between claims) must read as active and survive the sweep.
    backend = fs_backend
    grace = 3600.0
    old = time.time() - grace - 120.0
    await backend.mutate_document("kv/t", "k", lambda _c: ({"v": 1}, None))
    await backend.delete_document("kv/t", "k")
    lock_path, _doc_path = backend._doc_paths("kv/t", "k")
    os.utime(lock_path, (old, old))
    # a doc-keeping probe of the absent key: acquires (and touches) the
    # lock without recreating the document.
    body, _ = await backend.mutate_document(
        "kv/t", "k", lambda _c: (state.DOC_KEEP, None)
    )
    assert body is None
    assert os.stat(lock_path).st_mtime > old  # the touch really landed
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 0
    assert os.path.exists(lock_path)


async def test_gc_sweeps_bare_lease_lock_only_once_idle_past_grace(fs_backend):
    # REGRESSION of the previous fix round: _gc_leases_sync keys off .lease
    # names, so a .lock orphaned by a lost post-release unlink (Windows AV
    # scanner handle) was never revisited -- it leaked forever.  The orphan
    # sweep must reclaim a BARE .lock (no .lease sibling) once idle past
    # the grace, and keep both a fresh bare lock and a lock whose .lease
    # still exists.
    backend = fs_backend
    grace = 3600.0
    old = time.time() - grace - 120.0
    lease_root = os.path.join(backend.base, state.LEASES_DIR)

    # a live lease: .lock has its .lease sibling, whatever the mtime.
    lease = await backend.acquire_lease("slots/j", "A", ttl=30.0)
    assert lease is not None
    live_lock, live_lease = backend._lease_paths("slots/j")
    os.utime(live_lock, (old, old))

    # the orphan: a bare .lock aged past the grace window.
    dead_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fgone.lock")
    with open(dead_lock, "wb") as fobj:
        fobj.write(b"\0")
    os.utime(dead_lock, (old, old))

    # a fresh bare .lock (an acquire between open and lease write).
    young_lock = os.path.join(lease_root, "dagadvance%2Fd%2Fnew.lock")
    with open(young_lock, "wb") as fobj:
        fobj.write(b"\0")

    dry = await backend.collect_garbage(keep={}, grace=grace, dry_run=True)
    assert dry["locks_removed"] == 1
    assert os.path.exists(dead_lock)
    result = await backend.collect_garbage(keep={}, grace=grace)
    assert result["locks_removed"] == 1
    assert not os.path.exists(dead_lock)
    assert os.path.exists(live_lock) and os.path.exists(live_lease)
    assert os.path.exists(young_lock)


# --- 14: document delete rides out Windows sharing violations ---------------


async def test_document_delete_retries_sharing_violation(
    fs_backend, monkeypatch
):
    # On Windows a concurrent reader/AV scan holding a .doc open makes
    # os.unlink raise PermissionError; the write path always retried this
    # (see _replace) but the delete path surfaced it as a spurious error
    # from a healthy store.  The unlink must retry the transient hold away.
    backend = fs_backend
    await backend.mutate_document("ns", "k", lambda _c: ({"v": 1}, None))
    _lock_path, doc_path = backend._doc_paths("ns", "k")
    monkeypatch.setattr(state, "IS_WINDOWS", True)  # force the retry path
    calls = {"n": 0}
    real_unlink = os.unlink

    def flaky_unlink(path, *args, **kwargs):
        if (
            os.path.normpath(str(path)) == os.path.normpath(doc_path)
            and calls["n"] < 2
        ):
            calls["n"] += 1
            raise PermissionError(5, "sharing violation", str(path))
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(state.os, "unlink", flaky_unlink)
    assert await backend.delete_document("ns", "k") is True
    assert calls["n"] == 2  # the transient hold really was retried away
    assert not os.path.exists(doc_path)
    assert await backend.read_document("ns", "k") is None


# --- 15: lone-surrogate names round-trip through enumeration -----------------


async def test_stream_audit_roundtrips_lone_surrogate_names(fs_backend):
    # Job names come from os.fsdecode'd crontab filenames, so a stream name
    # can carry a lone surrogate; _fs_safe encodes it via surrogatepass.
    # Decoding the token back with errors="replace" returned a DIFFERENT
    # name (U+FFFD in place of the surrogate) while still reporting the
    # listing complete: the orphan-blob sweep re-encoded the garbled name,
    # read a stream that does not exist (silently empty), dropped the real
    # stream's digests from its referenced set, and deleted still-referenced
    # payload blobs.
    backend = fs_backend
    surrogate = "artifacts/caf\udce9"
    plain = "artifacts/plain"
    for stream in (surrogate, plain):
        await backend.append_record(stream, {"sha256": "a" * 64})
    names, complete = await backend.list_stream_names_audit("artifacts/")
    assert complete is True
    assert set(names) == {surrogate, plain}
    # _fs_safe over every returned name reproduces exactly the on-disk
    # tokens, so keep-sets built from these names protect the real dirs.
    records_root = os.path.join(backend.base, "records")
    prefix_token = state._fs_safe_fragment("artifacts/")
    on_disk = {
        t for t in os.listdir(records_root) if t.startswith(prefix_token)
    }
    assert {state._fs_safe(n) for n in names} == on_disk
    # each returned name feeds straight back into a record read, the exact
    # call the sweep's referenced-digest builder makes.
    for name in names:
        assert await backend.list_records(name) == [{"sha256": "a" * 64}]


async def test_stream_audit_never_returns_foreign_tokens_garbled(fs_backend):
    # A records-root entry _fs_safe cannot have produced (undecodable bytes,
    # or an alias that decodes but re-encodes to a DIFFERENT token) must be
    # reported through ``complete``, never returned as a mangled name a
    # destructive caller could act on.
    backend = fs_backend
    await backend.append_record("artifacts/plain", {"n": 1})
    records_root = os.path.join(backend.base, "records")
    os.mkdir(os.path.join(records_root, "artifacts%2Fbad%FF"))
    os.mkdir(os.path.join(records_root, "artifacts%2fx"))
    names, complete = await backend.list_stream_names_audit("")
    assert complete is False
    assert "artifacts/plain" in names
    assert "artifacts/bad\ufffd" not in names
    assert "artifacts/x" not in names  # the %2f alias must not decode
    # every returned name still round-trips to a real on-disk token.
    for name in names:
        assert os.path.isdir(
            os.path.join(records_root, state._fs_safe(name))
        )


async def test_document_namespaces_roundtrip_lone_surrogate_names(fs_backend):
    # The docs-side twin: a dag-run namespace named after an os.fsdecode'd
    # dag name can carry a lone surrogate.  A replace-decoded (garbled)
    # namespace reads as a REMOVED dag to the GC, whose runs' XCom scopes
    # then lose their keep-set protection.
    backend = fs_backend

    def put(body):
        return lambda _cur: (body, None)

    surrogate_ns = "dagrun/caf\udce9"
    await backend.mutate_document(surrogate_ns, "r1", put({"runId": "1"}))
    await backend.mutate_document("dagrun/plain", "r1", put({"runId": "2"}))
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert complete is True
    assert set(names) == {surrogate_ns, "dagrun/plain"}
    # the returned namespace addresses the real documents.
    assert await backend.list_documents(surrogate_ns) == [{"runId": "1"}]
    # a foreign token that cannot round-trip is reported, never garbled.
    docs_root = os.path.join(backend.base, "docs")
    os.mkdir(os.path.join(docs_root, "dagrun%2Fbad%FF"))
    names, complete = await backend.list_document_namespaces("dagrun/")
    assert complete is False
    assert "dagrun/bad\ufffd" not in names


# --- 16: record names stay monotonic across a backward clock step ------------


async def test_backward_clock_step_never_prunes_the_fresh_record(
    fs_backend, monkeypatch
):
    # Record filenames embed the wall clock, and both the amortised prune
    # (keep the lexicographic tail) and the derive_max memo (scan only names
    # above the watermark) assume they only ever grow.  A backward step (NTP
    # correcting a fast host clock) minted names BELOW retained history: at
    # the prune cap, the very next amortised prune deleted the just-written,
    # acknowledged record while keeping stale future-dated ones, and
    # derive_max kept answering from the pre-step fold.
    backend = fs_backend
    clock = {"t": 2000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    keep = 3
    total = state._PRUNE_EVERY_APPENDS
    ids = []
    for i in range(total):
        clock["t"] += 1.0
        ids.append(
            await backend.append_record(
                "runs/j", {"seq": i}, prune_keep=keep
            )
        )
    # arm the derive_max memo: its watermark is the newest pre-step name.
    assert await backend.derive_max("runs/j", "seq") == total - 1
    clock["t"] = 1000.0  # the wall clock steps backwards
    # append number _PRUNE_EVERY_APPENDS + 1 carries the due prune itself.
    ids.append(
        await backend.append_record("runs/j", {"seq": total}, prune_keep=keep)
    )
    recs = await backend.list_records("runs/j")
    assert {"seq": total} in recs  # survived the prune it carried
    assert len(recs) == keep  # and the prune still bounded the stream
    assert recs[-1] == {"seq": total}  # newest by name, like by ack order
    assert await backend.derive_max("runs/j", "seq") == total
    # ids stay strictly increasing and parseable by the epoch reader.
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)
    assert state._record_epoch(ids[-1]) != float("inf")


async def test_backward_clock_step_survives_a_process_restart(
    fs_backend, fs_backend_factory, monkeypatch
):
    # The name floor is in-process state, so a restart must re-learn it from
    # the stream directory itself: the first append of a fresh backend
    # (which always carries an immediately-due prune) must land above the
    # retained future-dated history, not below it.
    backend = fs_backend
    clock = {"t": 2000.0}
    monkeypatch.setattr(state, "_now", lambda: clock["t"])
    keep = 3
    for i in range(keep):
        clock["t"] += 1.0
        await backend.append_record("runs/j", {"seq": i}, prune_keep=keep)
    clock["t"] = 1000.0  # steps back, and then the daemon restarts
    backend2 = await fs_backend_factory()
    await backend2.append_record("runs/j", {"seq": keep}, prune_keep=keep)
    recs = await backend2.list_records("runs/j")
    assert {"seq": keep} in recs
    assert recs[-1] == {"seq": keep}
    assert len(recs) == keep
    assert await backend2.derive_max("runs/j", "seq") == keep


# =====================================================================
# Lifecycle hardening of the durable state layer
# (formerly tests/test_state_lifecycle_hardening.py; merged per finding
# B18, rationale below kept verbatim from that module's docstring; its
# local _info helper collapsed onto this module's _info, which has the
# same body with a wider signature)
# =====================================================================
# Lifecycle hardening of the durable state layer.
#
# Adversarial-review follow-ups not covered elsewhere: the platform file
# lock must BLOCK (never raise) under contention, the backend's
# daemon-thread ``_call`` helper must keep a hung store abandonable, and
# the shutdown flush in :meth:`cronstable.cron.Cron.run` must persist
# in-flight run records without ever letting a wedged store hang exit.
#
# Timing discipline (Windows CI has coarse ~15.6ms timers and slow
# spawns): every wait here is an event or a generous bound, and nothing
# asserts a duration or a tight window -- only ordering and completion.


# --- exclusive_file_lock: block-then-succeed under contention -------------


def test_exclusive_lock_blocks_then_succeeds_under_contention(tmp_path):
    # The semantic guarantee both platforms must give: a second locker
    # BLOCKS while the first holds, then SUCCEEDS once it releases --
    # it never raises.  (msvcrt's LK_LOCK raised OSError after ~10
    # one-second retries; the Windows path is now a non-blocking retry
    # loop, and this is its contract test.  On POSIX flock blocks
    # natively.)  Ordering is asserted with events only, never times.
    lock_file = tmp_path / "contended.lock"
    lock_file.write_bytes(b"\0")  # msvcrt needs a byte present to lock

    a_holding = threading.Event()  # A is inside the locked section
    a_release = threading.Event()  # main tells A to let go
    b_attempting = threading.Event()  # B is about to block on the lock
    b_acquired = threading.Event()  # B made it inside
    b_saw_release_order = []  # was A told to release before B got in?
    errors = []

    def hold_a():
        fd = os.open(str(lock_file), os.O_RDWR)
        try:
            with exclusive_file_lock(fd):
                a_holding.set()
                a_release.wait(timeout=30)
        except OSError as ex:  # pragma: no cover - the failure under test
            errors.append(ex)
        finally:
            os.close(fd)

    def contend_b():
        fd = os.open(str(lock_file), os.O_RDWR)
        try:
            b_attempting.set()
            with exclusive_file_lock(fd):
                # a_release is set by the main thread strictly before A
                # can exit its locked section, so if mutual exclusion
                # holds B can only ever observe it already set.
                b_saw_release_order.append(a_release.is_set())
                b_acquired.set()
        except OSError as ex:  # pragma: no cover - the failure under test
            errors.append(ex)
        finally:
            os.close(fd)

    thread_a = threading.Thread(target=hold_a, daemon=True)
    thread_a.start()
    assert a_holding.wait(timeout=10)
    thread_b = threading.Thread(target=contend_b, daemon=True)
    thread_b.start()
    assert b_attempting.wait(timeout=10)
    # hold the lock a while with B contending.  B staying out is a
    # mutual-exclusion check, not a timing one: b_acquired can only be
    # set by actually acquiring, impossible while A holds the lock.
    time.sleep(0.5)
    assert not b_acquired.is_set()
    assert errors == []  # above all, B must not have RAISED while blocked
    a_release.set()
    assert b_acquired.wait(timeout=10)  # B succeeds once A releases
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)
    assert not thread_a.is_alive()
    assert not thread_b.is_alive()
    assert errors == []
    assert b_saw_release_order == [True]


# --- FilesystemStateBackend._call: the daemon-thread seam -----------------


async def test_call_runs_sync_half_on_daemon_thread(fs_backend):
    # _call must run the blocking half on a DAEMON thread named
    # "cronstable-state", never the default executor: non-daemonic workers
    # are joined at interpreter exit, so one wedged in a dead NFS hard
    # mount would hang process shutdown forever.
    backend = fs_backend
    seen = {}
    real_append = backend._append_sync

    def spy(stream, data, prune_keep=None, prune_latest_by=None):
        thread = threading.current_thread()
        seen["daemon"] = thread.daemon
        seen["name"] = thread.name
        return real_append(stream, data, prune_keep)

    backend._append_sync = spy  # type: ignore[method-assign]
    rec_id = await backend.append_record("s", {"i": 1})
    assert rec_id
    assert seen["daemon"] is True
    assert seen["name"].startswith("cronstable-state")


class _StoreBoom(Exception):
    pass


async def test_call_propagates_sync_half_exception(fs_backend):
    # an exception raised on the worker thread must surface, as itself,
    # to the awaiter -- not vanish and not wedge the await.
    backend = fs_backend

    def boom(stream, data, prune_keep=None, prune_latest_by=None):
        raise _StoreBoom("sync half exploded")

    backend._append_sync = boom  # type: ignore[method-assign]
    with pytest.raises(_StoreBoom, match="sync half exploded"):
        await backend.append_record("s", {"i": 1})


async def test_call_survives_abandoned_await(fs_backend):
    # An awaiter that times out (asyncio.wait_for) abandons _call's
    # future; when the daemon thread later finishes, _resolve must see
    # the cancelled future and drop the result silently -- no
    # InvalidStateError thrown into the loop, no unhandled-exception
    # noise.  This is the "hung store, caller moved on" path shutdown
    # relies on.
    backend = fs_backend
    entered = threading.Event()
    release = threading.Event()

    def blocked(stream, data, prune_keep=None, prune_latest_by=None):
        entered.set()
        release.wait(timeout=30)  # released below; bound is a safety net
        return "late-result"

    backend._append_sync = blocked  # type: ignore[method-assign]

    loop = asyncio.get_running_loop()
    captured = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda lp, ctx: captured.append(ctx))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                backend.append_record("s", {"i": 1}), timeout=0.1
            )
        # the worker really is in-flight (tries=1000 keeps this file's
        # original ~10s bound for slow CI)
        await _wait_until(entered.is_set, tries=1000)
        release.set()  # let the daemon thread finish into the dead future
        await asyncio.sleep(0.3)  # a generous beat for _resolve to run
    finally:
        loop.set_exception_handler(previous)
    assert captured == []


async def test_inventory_runs_via_call_daemon_thread(fs_backend):
    # inventory() used to submit its full listdir walk to the DEFAULT
    # executor (loop.run_in_executor(None, ...)), bypassing _call's
    # abandonable daemon threads, lane cap and throttle: a dashboard
    # polling GET /state against a hung mount wedged the non-daemon
    # default workers one by one, after which config reload -- and the
    # interpreter-exit join of those workers -- hung behind them.  The
    # walk must ride the same "cronstable-state" daemon lane as every other
    # op, and be accounted in the per-op stats like one.
    backend = fs_backend
    seen = {}
    real_inventory = backend._inventory_sync

    def spy():
        thread = threading.current_thread()
        seen["daemon"] = thread.daemon
        seen["name"] = thread.name
        return real_inventory()

    backend._inventory_sync = spy  # type: ignore[method-assign]
    inv = await backend.inventory()
    assert inv["enumerable"] is True
    assert seen["daemon"] is True
    assert seen["name"].startswith("cronstable-state")
    assert "inventory" in backend.stats()["ops"]


# --- Cron.run(): the shutdown flush ----------------------------------------


_FLUSH_CFG = """\
jobs:
  - name: j
    command: echo hi
    schedule: "0 0 29 2 *"
state:
  path: {path}
"""


async def test_shutdown_flushes_pending_run_record(tmp_path):
    # The exact data loss the flush exists to prevent: a run record
    # scheduled fire-and-forget moments before shutdown must still be
    # durable after run() returns.  Drives the REAL run() loop (the
    # test_cron.py pattern): start -> backend up -> record ->
    # signal_shutdown -> clean exit -> read the store back cold.
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(_FLUSH_CFG.format(path=state_dir))

    cron = Cron(str(cfg))
    task = asyncio.create_task(cron.run())
    try:
        await _wait_until(
            lambda: cron.state_backend is not None, tries=1000
        )
        # schedule the persist and shut down in the SAME loop step: the
        # write task has not run even once yet, so without the shutdown
        # flush it would be abandoned mid-air.
        cron._record_run("j", _info(second=1))
        # at least the run-record write is pending and un-run (the backend
        # start may also have queued chore writes, e.g. the manifest)
        assert len(cron._pending_state_writes) >= 1
    finally:
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=30)

    assert cron.state_backend is None  # run() tore the backend down
    reader = _backend(state_dir)
    recs = await reader.list_records("runs/j")
    assert any(
        r["finished_at"] == "2026-07-01T00:00:01+00:00"
        and r["outcome"] == "success"
        for r in recs
    )


async def test_shutdown_completes_despite_hung_state_write(
    tmp_path, monkeypatch
):
    # A wedged store (the classic dead-NFS-server hard mount) must not
    # hang exit: the flush is BOUNDED, the hung write is abandoned, and
    # run() still returns.  Never asserts how fast -- only that it
    # completes at all, within a x10-generous outer bound.
    cron = Cron(None, config_yaml=_ONE_JOB)
    await cron.start_stop_state(_state_cfg("state:\n  path: " + str(tmp_path)))
    assert cron.state_backend is not None

    hang = asyncio.get_running_loop().create_future()

    async def hung_append(stream, data, *, prune_keep=None):
        await hang  # never resolves

    monkeypatch.setattr(cron.state_backend, "append_record", hung_append)

    # Shrink run()'s hardcoded flush bound (asyncio.wait(..., timeout=5))
    # so the test proves "bounded" without idling out the real 5s; every
    # other asyncio.wait call in flight passes through untouched.
    real_wait = asyncio.wait

    async def fast_wait(fs, timeout=None, **kwargs):
        if timeout == 5:
            timeout = 0.2
        return await real_wait(fs, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "wait", fast_wait)

    cron._record_run("j", _info())
    assert len(cron._pending_state_writes) == 1
    pending = next(iter(cron._pending_state_writes))

    # Straight to the shutdown sequence (the pattern of test_cron.py's
    # test_shutdown_stops_cluster_manager_before_job_drain): the loop
    # body never runs, so housekeeping cannot replace the rigged
    # backend, and run() goes directly to the flush block.
    cron.signal_shutdown()
    await asyncio.wait_for(cron.run(), timeout=30)

    assert cron.state_backend is None  # shutdown ran to completion
    assert not pending.done()  # the hung write was abandoned, not awaited
    pending.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending
