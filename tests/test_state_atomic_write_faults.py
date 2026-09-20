"""Inject faults into each stage of an atomic state write.

``FilesystemStateBackend._atomic_write`` creates a temporary file, writes
the data, syncs the file, renames it, and syncs the directory. Each test
replaces the module's ``os`` reference with a wrapper that fails one call
and delegates the rest.

Read the store through its public API after each failure. Check that no
incomplete record is visible, temporary files are removed when possible,
callers receive the expected error, and writes resume after the fault ends.
"""

import errno
import logging
import os
import stat
import time

import pytest

from cronstable import platform, state
from tests._helpers import _backend, start_state

# every shape that goes through _atomic_write, as (label, write, read):
# write() attempts one write of ``value``; read() returns what a reader
# sees afterwards (None when nothing is visible).


async def _append(backend, value):
    await backend.append_record("runs/job", {"value": value})


async def _read_records(backend):
    recs = await backend.list_records("runs/job")
    return [r["value"] for r in recs] or None


async def _mutate(backend, value):
    await backend.mutate_document(
        "docs", "key", lambda _cur: ({"value": value}, None)
    )


async def _read_doc(backend):
    body = await backend.read_document("docs", "key")
    return None if body is None else [body["value"]]


async def _blob(backend, value):
    await backend.put_blob(value.encode())


async def _read_blobs(backend):
    found = []
    for root, _dirs, names in os.walk(backend._blobs_root):
        for name in names:
            with open(os.path.join(root, name), "rb") as fobj:
                found.append(fobj.read().decode())
    return sorted(found) or None


SHAPES = [
    pytest.param(_append, _read_records, id="record"),
    pytest.param(_mutate, _read_doc, id="document"),
    pytest.param(_blob, _read_blobs, id="blob"),
]


class _FaultyOS:
    """``os`` for cronstable.state alone, with chosen calls overridden."""

    def __init__(self, **overrides):
        self._overrides = overrides
        self.calls = dict.fromkeys(overrides, 0)

    def __getattr__(self, name):
        override = self._overrides.get(name)
        if override is None:
            return getattr(os, name)

        def call(*args, **kwargs):
            self.calls[name] += 1
            return override(*args, **kwargs)

        return call


def _enospc(*_args, **_kwargs):
    raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))


def _eio(*_args, **_kwargs):
    raise OSError(errno.EIO, os.strerror(errno.EIO))


def _partial_then_enospc():
    """Write half of the first chunk for real, then the disk is full."""
    wrote = []

    def write(fdesc, view):
        if wrote:
            _enospc()
        wrote.append(os.write(fdesc, bytes(view[: max(1, len(view) // 2)])))
        return wrote[0]

    return write


def _tmp_files(backend):
    return sorted(os.listdir(backend._tmp_root))


def _visible_files(backend):
    """Every file a reader could reach: all of the store except tmp."""
    found = []
    for root, _dirs, names in os.walk(backend.base):
        if os.path.abspath(root) == os.path.abspath(backend._tmp_root):
            continue
        found += [
            os.path.join(root, n) for n in names if not n.endswith(".lock")
        ]
    return sorted(found)


FAULTS = [
    pytest.param(lambda: {"write": _enospc}, errno.ENOSPC, id="enospc"),
    pytest.param(
        lambda: {"write": _partial_then_enospc()},
        errno.ENOSPC,
        id="enospc-mid-write",
    ),
    pytest.param(lambda: {"fsync": _eio}, errno.EIO, id="fsync"),
    pytest.param(lambda: {"replace": _eio}, errno.EIO, id="rename"),
]


@pytest.mark.parametrize("write, read", SHAPES)
@pytest.mark.parametrize("fault, code", FAULTS)
async def test_failed_write_is_invisible_and_the_store_recovers(
    fs_backend, monkeypatch, write, read, fault, code
):
    backend = fs_backend
    await write(backend, "before")
    before = _visible_files(backend)
    assert await read(backend) == ["before"]

    shim = _FaultyOS(**fault())
    monkeypatch.setattr(state, "os", shim)
    with pytest.raises(OSError) as raised:
        await write(backend, "during")
    monkeypatch.undo()
    assert raised.value.errno == code
    assert all(shim.calls.values()), "the fault never fired"

    # nothing of the failed write is visible: same files, same content,
    # and the temporary file it was building is gone.
    assert _visible_files(backend) == before
    assert await read(backend) == ["before"]
    assert _tmp_files(backend) == []
    # a second reader (a peer, or the next daemon) agrees.
    other = _backend(os.path.dirname(backend.base))
    await other.start()
    assert await read(other) == ["before"]
    await other.stop()

    # the fault cleared: the very next write lands.
    await write(backend, "after")
    landed = await read(backend)
    assert "after" in landed and "during" not in landed
    assert _tmp_files(backend) == []


@pytest.mark.parametrize("write, read", SHAPES)
async def test_short_writes_still_land_the_whole_payload(
    fs_backend, monkeypatch, write, read
):
    # os.write may write short (a signal, an odd mount); the write loop
    # keeps going until the payload is out, so the record is complete.
    def three_bytes(fdesc, view):
        return os.write(fdesc, bytes(view[:3]))

    shim = _FaultyOS(write=three_bytes)
    monkeypatch.setattr(state, "os", shim)
    await write(fs_backend, "x" * 200)
    monkeypatch.undo()
    assert shim.calls["write"] > 60
    assert await read(fs_backend) == ["x" * 200]
    assert _tmp_files(fs_backend) == []


async def test_failed_write_counts_as_an_op_error(fs_backend, monkeypatch):
    await _append(fs_backend, "ok")
    monkeypatch.setattr(state, "os", _FaultyOS(fsync=_eio))
    with pytest.raises(OSError):
        await _append(fs_backend, "lost")
    monkeypatch.undo()
    ops = fs_backend.stats()["ops"]
    assert ops["append"]["count"] == 2
    assert ops["append"]["errors"] == 1


async def test_lease_write_fault_denies_instead_of_raising(
    fs_backend, monkeypatch, caplog
):
    # The lease API fails closed: a write that cannot land is a denied
    # acquire or renew, logged, never an exception out of the API.
    lease = await fs_backend.acquire_lease("slot", "holder", 60.0)
    assert lease is not None and lease.fence == 1
    monkeypatch.setattr(state, "os", _FaultyOS(replace=_eio))
    with caplog.at_level(logging.WARNING, logger="cronstable.state"):
        assert await fs_backend.renew_lease(lease, 60.0) is None
        assert await fs_backend.acquire_lease("other", "holder", 60.0) is None
    monkeypatch.undo()
    assert "lease slot write failed" in caplog.text
    assert "denying renew" in caplog.text
    assert "lease other write failed" in caplog.text
    assert "denying acquire" in caplog.text
    # the lease on disk is the last complete one, the refused lease left
    # no file at all, and no temporary file lingers.
    on_disk = await fs_backend.read_lease("slot")
    assert on_disk == lease
    assert await fs_backend.read_lease("other") is None
    assert _tmp_files(fs_backend) == []
    # the fault cleared: both calls work, and the fence did not move.
    renewed = await fs_backend.renew_lease(lease, 60.0)
    assert renewed is not None and renewed.fence == 1
    other = await fs_backend.acquire_lease("other", "holder", 60.0)
    assert other is not None and other.fence == 1


async def test_stranded_temp_file_is_invisible_and_swept_by_gc(
    fs_backend, monkeypatch
):
    # A temporary file remains if both rename and cleanup fail, or if the
    # process exits between those operations.
    await _append(fs_backend, "before")
    shim = _FaultyOS(replace=_eio, unlink=_eio)
    monkeypatch.setattr(state, "os", shim)
    with pytest.raises(OSError):
        await _append(fs_backend, "stranded")
    monkeypatch.undo()
    (stranded,) = _tmp_files(fs_backend)
    assert stranded.startswith("w-") and stranded.endswith(".tmp")
    # complete bytes, but in tmp: no reader, inventory or stream listing
    # ever looks there.
    assert await _read_records(fs_backend) == ["before"]
    assert await fs_backend.list_stream_names("") == ["meta", "runs/job"]

    # GC leaves a young temporary file alone (its writer may be mid-rename)...
    swept = await fs_backend.collect_garbage(keep={}, grace=0.0)
    assert swept["tmp_removed"] == 0
    assert _tmp_files(fs_backend) == [stranded]
    # ...and removes it once it is older than TMP_MAX_AGE.
    old = time.time() - state.TMP_MAX_AGE - 60
    os.utime(os.path.join(fs_backend._tmp_root, stranded), (old, old))
    dry = await fs_backend.collect_garbage(keep={}, grace=0.0, dry_run=True)
    assert dry["tmp_removed"] == 1
    assert _tmp_files(fs_backend) == [stranded]
    swept = await fs_backend.collect_garbage(keep={}, grace=0.0)
    assert swept["tmp_removed"] == 1
    assert _tmp_files(fs_backend) == []
    assert await _read_records(fs_backend) == ["before"]


async def test_cron_logs_and_counts_a_dropped_write_then_recovers(
    stateful_cron, monkeypatch, caplog
):
    # The scheduler's durable writes are fire-and-forget: a store fault
    # costs the record, is logged and counted, and never reaches the job
    # path as an exception.
    cron = await stateful_cron(
        "jobs:\n  - name: job\n    command: 'true'\n"
        '    schedule: "* * * * *"\n',
        extra_state="  jobApi:\n    enabled: false\n",
    )
    monkeypatch.setattr(state, "os", _FaultyOS(write=_enospc))
    with caplog.at_level(logging.WARNING, logger="cronstable"):
        await cron._persist_inflight_closed("job")
    monkeypatch.undo()
    assert "failed to close the in-flight record of job" in caplog.text
    assert os.strerror(errno.ENOSPC) in caplog.text
    assert cron.metrics._state_dropped == {"inflight": 1}
    backend = cron.state_backend
    assert await backend.list_records("inflight/job") == []

    await cron._persist_inflight_closed("job")
    (rec,) = await backend.list_records("inflight/job")
    assert rec["kind"] == "closed"
    assert cron.metrics._state_dropped == {"inflight": 1}


# --- a read-only state store --------------------------------------

_needs_posix_permissions = pytest.mark.skipif(
    platform.IS_WINDOWS or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="chmod 0555 stops neither Windows nor root from writing",
)


def _chmod_tree(root, dir_mode):
    for path, _dirs, _names in os.walk(root):
        os.chmod(path, dir_mode)


@_needs_posix_permissions
async def test_read_only_store_fails_writes_serves_reads_and_recovers(
    fs_backend,
):
    await _append(fs_backend, "before")
    await _mutate(fs_backend, "before")
    lease = await fs_backend.acquire_lease("slot", "holder", 60.0)
    try:
        _chmod_tree(fs_backend.base, 0o555)
        with pytest.raises(PermissionError):
            await _append(fs_backend, "during")
        with pytest.raises(PermissionError):
            await _mutate(fs_backend, "during")
        # the lease API fails closed here too.
        assert await fs_backend.renew_lease(lease, 60.0) is None
        # reads keep working off the last complete state.
        assert await _read_records(fs_backend) == ["before"]
        assert await _read_doc(fs_backend) == ["before"]
        assert await fs_backend.read_lease("slot") == lease
    finally:
        _chmod_tree(fs_backend.base, 0o700)
    assert stat.S_IMODE(os.stat(fs_backend.base).st_mode) == 0o700
    await _append(fs_backend, "after")
    assert await _read_records(fs_backend) == ["before", "after"]
    assert (await fs_backend.renew_lease(lease, 60.0)).fence == lease.fence
    assert _tmp_files(fs_backend) == []


@_needs_posix_permissions
async def test_read_only_state_dir_degrades_the_daemon_at_start(
    tmp_path, caplog
):
    from cronstable.cron import Cron
    from tests._helpers import _state_cfg

    root = tmp_path / "ro"
    root.mkdir()
    root.chmod(0o555)
    try:
        cron = Cron(
            None,
            config_yaml="jobs:\n  - name: job\n    command: 'true'\n"
            '    schedule: "* * * * *"\n',
        )
        cfg = _state_cfg(
            "state:\n  path: {}\n  jobApi:\n    enabled: false\n".format(root)
        )
        with caplog.at_level(logging.ERROR, logger="cronstable"):
            await start_state(cron, cfg)
        # the startup check reports the inaccessible store and the scheduler
        # carries on with the in-memory path.
        assert cron.state_backend is None
        assert "state: failed to start" in caplog.text
        assert list(root.iterdir()) == []

        # once the directory is writable the next pass brings it up.
        root.chmod(0o700)
        await start_state(cron, cfg)
        assert cron.state_backend is not None
        await cron.state_backend.stop()
    finally:
        root.chmod(0o700)
