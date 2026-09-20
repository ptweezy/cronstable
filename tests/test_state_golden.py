"""Check compatibility with the saved v1 state store.

``tests/gen_state_golden.py`` writes ``tests/fixtures/state_v1`` through the
package APIs. These tests check compatibility in both directions:

* Reading: Parse saved records, documents, leases, and blobs. Start a
  scheduler using the saved store and verify the restored state.
* Writing: Regenerate the store and compare its files with the saved files
  to detect changes to filenames, fields, defaults, or encoding.

After an intentional format change, run
``PYTHONPATH=. python tests/gen_state_golden.py`` and commit the changes.
Comparisons exclude empty directories, file permissions, and modification
times.

The saved store uses orjson, which writes non-ASCII text as UTF-8. The
standard library encoder escapes that text; both decoders accept either
form. If orjson is unavailable, compare JSON files containing non-ASCII text
by parsed value. Compare every other file byte for byte. See
``cronstable/_json.py``.
"""

import datetime
import json
import os
import shutil

import pytest

from cronstable import _json, dag, jobstate, pools, state
from cronstable import config as config_mod
from cronstable import cron as cron_mod
from cronstable.config import parse_config_string
from tests import gen_state_golden as gen
from tests._helpers import _backend, start_state

GOLDEN = gen.OUT
UTC = datetime.timezone.utc


@pytest.fixture(params=["/bin/sh", ""], ids=["posix-shell", "windows-shell"])
def host_shell_default(request, monkeypatch):
    """Exercise both platforms' defaults on every runner.

    Job and report shells enter the saved digests, so leaving either implicit
    would invalidate retries and DAGs when reading the tree on another OS.
    """
    monkeypatch.setitem(config_mod.DEFAULT_CONFIG, "shell", request.param)
    for action in ("onFailure", "onPermanentFailure", "onSuccess", "onLate"):
        monkeypatch.setitem(
            config_mod.DEFAULT_CONFIG[action]["report"]["shell"],
            "shell",
            request.param,
        )


@pytest.fixture
def golden_copy(tmp_path):
    """A scratch copy: loading a store appends to it (meta, reconcile)."""
    root = tmp_path / "state_v1"
    shutil.copytree(GOLDEN, root)
    return root


@pytest.fixture
async def golden_backend(golden_copy):
    backend = _backend(golden_copy)
    await backend.start()
    yield backend
    await backend.stop()


def test_committed_tree_is_a_v1_store():
    files = gen.tree(GOLDEN)
    assert files, "the golden tree is missing: run tests/gen_state_golden.py"
    tops = {path.split("/")[1] for path in files}
    assert tops == {
        state.RECORDS_DIR,
        state.DOCS_DIR,
        state.LEASES_DIR,
        state.BLOBS_DIR,
    }
    for path, raw in files.items():
        if path.endswith(".lock"):
            assert raw == b"\0", path
        elif path.endswith((".json", ".doc")):
            obj = json.loads(raw)
            assert set(obj) == {"schemaVersion", "data"}, path
            assert obj["schemaVersion"] == "v1" == state.SCHEMA_VERSION
        elif path.endswith(".lease"):
            assert set(json.loads(raw)) == {
                "name",
                "holder",
                "fence",
                "expiresAt",
            }, path
        else:
            assert path.endswith(".blob"), path


async def test_generator_reproduces_the_committed_tree(
    tmp_path, host_shell_default
):
    out = tmp_path / "regenerated"
    await gen.close(await gen.generate(out))
    fresh = gen.tree(out)
    committed = gen.tree(GOLDEN)
    assert sorted(fresh) == sorted(committed)
    for path in sorted(committed):
        if fresh[path] == committed[path]:
            continue
        # the one documented normalization; see the module docstring.
        assert _json.orjson is None and not committed[path].isascii(), path
        assert json.loads(fresh[path]) == json.loads(committed[path]), path


async def test_every_stream_lists_and_nothing_is_quarantined(
    golden_backend, golden_copy
):
    backend = golden_backend
    streams = await backend.list_stream_names("")
    assert streams == [
        "artifacts/nightly",
        "catchup/nightly",
        "counters/golden-host",
        "inflight/crashed",
        "inflight/held",
        "logs/nightly",
        "manifests/golden-host",
        "meta",
        "paused/held",
        "paused/nightly",
        "reboot/boot",
        "retries/flaky",
        "runs/flaky",
        "runs/held",
        "runs/nightly",
    ]
    counts = {s: len(await backend.list_records(s)) for s in streams}
    on_disk = {
        s: len(
            os.listdir(golden_copy / "default" / "records" / state._fs_safe(s))
        )
        for s in streams
    }
    # every committed record file came back as a parsed record.
    assert counts == on_disk
    assert list((golden_copy / "default" / "quarantine").iterdir()) == []
    inventory = await backend.inventory()
    assert not inventory["quarantine"]


async def test_records_read_back_with_their_values(golden_backend):
    backend = golden_backend
    runs = await backend.list_records("runs/nightly")
    assert [r["outcome"] for r in runs] == ["success", "success"]
    assert runs[0]["finished_at"] == "2026-01-02T00:00:01+00:00"
    assert runs[0]["duration"] == 12.5
    (failed,) = await backend.list_records("runs/flaky")
    assert (failed["outcome"], failed["exit_code"]) == ("failure", 23)
    assert failed["fail_reason"] == "command exited with code 23"
    assert await backend.derive_max("runs/nightly", "ranAt") == (
        "2026-01-03T00:00:01+00:00"
    )

    (log, _later) = await backend.list_records("logs/nightly")
    assert log["redacted"] is True
    assert log["lines"][0] == {"stream": "stdout", "line": "first night"}
    assert "hunter2" not in json.dumps(log)

    (opened,) = await backend.list_records("inflight/crashed")
    assert opened["kind"] == "open"
    assert (opened["host"], opened["proc"]) == (gen.HOST, gen.PROC)
    assert opened["pid"] == gen.DEAD_PID
    assert [
        r["kind"] for r in await backend.list_records("inflight/held")
    ] == [
        "open",
        "closed",
    ]

    (pending,) = await backend.list_records("retries/flaky")
    assert pending["kind"] == "pending" and pending["attempt"] == 1
    assert pending["notBefore"] == gen.RETRY_NOT_BEFORE.isoformat()

    (marker,) = await backend.list_records("reboot/boot")
    assert marker["bootId"] == gen.BOOT_ID
    assert marker["bootTime"] == gen.BOOT_TIME

    assert [
        r["kind"] for r in await backend.list_records("catchup/nightly")
    ] == ["open", "close"]
    (manifest,) = await backend.list_records("manifests/golden-host")
    assert manifest["jobs"] == sorted(
        ["nightly", "flaky", "crashed", "held", "boot", "pooled"]
    )
    assert manifest["dags"] == ["release"]
    (counters,) = await backend.list_records("counters/golden-host")
    assert "buckets" in counters and "jobs" in counters


async def test_documents_leases_and_blobs_read_back(golden_backend):
    backend = golden_backend
    namespaces, complete = await backend.list_document_namespaces("")
    assert complete and namespaces == [
        "cursor/nightly",
        "dagrun/release",
        "idem/nightly",
        "kv/nightly",
        "scheduler-pools",
    ]
    assert (await jobstate.kv_get(backend, "nightly", "last-batch"))[
        "value"
    ] == {"id": 41}
    assert (await jobstate.kv_get(backend, "nightly", "unicode"))[
        "value"
    ] == "café ☃"
    assert (await jobstate.cursor_get(backend, "nightly", "offset"))[
        "value"
    ] == 1200
    # the claim in the tree still excludes a second claimant.
    again = await jobstate.idempotency_claim(backend, "nightly", "send-report")
    assert again["fresh"] is False
    payload = await jobstate.artifact_get(backend, "nightly", "report.txt")
    assert payload is not None
    record, data = payload
    assert data == b"golden artifact payload"
    assert record["meta"] == {"contentType": "text/plain"}
    assert await backend.blob_exists(record["sha256"], record["size"])

    pool = await backend.read_document(pools.NAMESPACE, "database")
    assert pool["slots"] == 2
    assert {k: e["state"] for k, e in pool["entries"].items()} == {
        "ticket-running": "running",
        "ticket-queued": "queued",
    }

    gated = await backend.read_document("dagrun/release", "manual-gated")
    assert gated["state"] == "running" and not dag.is_terminal_run(gated)
    assert gated["tasks"]["build"]["state"] == dag.SUCCESS
    assert gated["tasks"]["gate"]["awaitingApproval"] is True
    assert gated["tasks"]["publish"]["state"] == "pending"
    done = await backend.read_document("dagrun/release", "manual-done")
    assert dag.is_terminal_run(done) and done["state"] == "success"
    assert done["tasks"]["gate"]["approval"]["by"] == "golden"

    # the held lease is live on the golden clock and long expired on the
    # real one; the fence counter is what has to survive.
    held = await backend.read_lease("slots/held")
    assert (held.holder, held.fence) == (gen.HOST + "#held", 1)
    assert await backend.read_lease("slots/nightly") is None
    for name in ("slots/held", "slots/nightly"):
        taken = await backend.acquire_lease(name, "successor", 30.0)
        assert taken is not None and taken.fence == 2


@pytest.fixture
async def golden_cron(golden_copy, monkeypatch, host_shell_default):
    """A scheduler booted on the golden store, as the golden host.

    Its clock reads a day after the tree was written: a pause cannot span
    more than 30 days, so the standing one is only live on that calendar.
    """
    monkeypatch.setattr(
        cron_mod, "get_now", lambda tz: gen.REHYDRATE_AT.astimezone(tz)
    )
    yaml = (
        "state:\n  path: {}\n  topology: single-node\n"
        "  jobApi:\n    enabled: false\n".format(json.dumps(str(golden_copy)))
        + gen.CONFIG
    )
    with gen.fixed_shell_defaults():
        cron = cron_mod.Cron(None, config_yaml=yaml)
        state_config = parse_config_string(yaml, "").state_config
    cron._state_host = gen.HOST
    cron._pools.service = lambda: None
    cron._dag._spawn_advance = lambda ref: None
    await start_state(cron, state_config)
    assert cron.state_backend is not None
    try:
        yield cron
    finally:
        for retry in list(cron.retry_state.values()):
            if retry.task is not None:
                retry.task.cancel()
        await gen.close(cron)


async def test_scheduler_rehydrates_the_golden_store(golden_cron):
    cron = golden_cron
    await gen._drain(cron)

    # the run ledger warmed the history.
    nightly = cron.last_run["nightly"]
    assert nightly.outcome == "success" and nightly.exit_code == 0
    assert nightly.finished_at == datetime.datetime(
        2026, 1, 3, 0, 0, 1, tzinfo=UTC
    )
    assert nightly.duration == 12.5
    assert len(cron.run_history["nightly"]) == 2
    assert cron.last_run["flaky"].exit_code == 23
    assert cron.last_run["held"].outcome == "success"

    # the open in-flight record of a dead process became one interrupted
    # row; the cleanly closed one did not.
    crashed = cron.last_run["crashed"]
    assert crashed.outcome == "unknown"
    assert crashed.fail_reason.startswith("run interrupted")
    backend = cron.state_backend
    (row,) = await backend.list_records("runs/crashed")
    assert row["outcome"] == "unknown"
    assert [r["kind"] for r in await backend.list_records("inflight/crashed")][
        -1
    ] == "closed"
    assert len(await backend.list_records("runs/held")) == 1

    # pause state: the standing pause holds, the resumed one is gone.
    held = cron._pause_active("held")
    assert held is not None
    assert (held.note, held.by) == ("golden pause", "golden")
    assert cron._pause_active("nightly") is None

    # the pending ladder re-armed at its saved attempt and deadline, which
    # also proves the saved job digest still matches this build's.
    retry = cron.retry_state["flaky"]
    assert retry.count == 1
    assert retry.task is not None and not retry.task.done()
    # re-arming goes through the ordinary retry scheduler, which records
    # the ladder again: same attempt, same absolute deadline.
    ladder = await backend.list_records("retries/flaky")
    assert {(r["kind"], r["attempt"], r["notBefore"]) for r in ladder} == {
        ("pending", 1, gen.RETRY_NOT_BEFORE.isoformat())
    }

    # the gated DAG run is still parked on its gate, and approving it
    # through the scheduler moves the stored document on.
    gated = await cron._dag._read("release", "manual-gated")
    assert gated["tasks"]["gate"]["awaitingApproval"] is True
    result = await cron._dag.approve(
        "release", "manual-gated", "gate", approved=True, by="pytest"
    )
    assert result["ok"]
    gated = await cron._dag._read("release", "manual-gated")
    assert gated["tasks"]["gate"]["state"] == dag.SUCCESS

    # the pool queue came back with its capacity and both tickets.
    (pool,) = await cron._pools.snapshot()
    assert pool["name"] == "database" and pool["slots"] == 2
    assert {e["id"] for e in pool["entries"]} == {
        "ticket-running",
        "ticket-queued",
    }
