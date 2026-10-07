"""cronstable._gzip: the response gzip backend and its aiohttp hookup."""

import gzip
import importlib.util
import sys
import zlib

import pytest

from cronstable import _gzip

_SAMPLE = b'{"name":"backup","status":"ok"}' * 400 + "café ☃".encode()

requires_isal = pytest.mark.skipif(
    importlib.util.find_spec("isal") is None, reason="isal not installed"
)


@pytest.fixture
def fresh_backend():
    """Resolve the backend again in the test, and again after it."""
    _gzip.backend.cache_clear()
    yield
    _gzip.backend.cache_clear()


@pytest.fixture
def without_isal(monkeypatch, fresh_backend):
    # a None entry makes `from isal import ...` raise ImportError
    monkeypatch.setitem(sys.modules, "isal", None)


@pytest.fixture
def aiohttp_backend(monkeypatch):
    """Restore aiohttp's process-wide backend after the test."""
    from aiohttp import compression_utils

    monkeypatch.setattr(
        compression_utils.ZLibBackend,
        "_zlib_backend",
        compression_utils.ZLibBackend._zlib_backend,
    )
    return compression_utils.ZLibBackend


def test_gzip_body_is_a_standard_gzip_stream():
    assert gzip.decompress(_gzip.gzip_body(_SAMPLE)) == _SAMPLE


def _zlib_level_nine(body):
    packer = zlib.compressobj(9, zlib.DEFLATED, 31)
    return packer.compress(body) + packer.flush()


def test_gzip_static_is_the_zlib_level_nine_stream():
    packed = _gzip.gzip_static(_SAMPLE)
    assert gzip.decompress(packed) == _SAMPLE
    assert packed == _zlib_level_nine(_SAMPLE)


class _RecordingZlib:
    """A zlib-compatible stand-in that records each compressor's level."""

    DEFLATED = zlib.DEFLATED

    def __init__(self, best):
        self.Z_BEST_COMPRESSION = best
        self.levels = []

    def compressobj(self, level, *args):
        self.levels.append(level)
        return zlib.compressobj(level, *args)


@pytest.fixture
def stand_in_isal(monkeypatch):
    """(response backend, the module's zlib), both recording.

    The backend's strongest level is 3, as ISA-L's is.
    """
    isal = _RecordingZlib(3)
    stdlib = _RecordingZlib(zlib.Z_BEST_COMPRESSION)
    monkeypatch.setattr(_gzip, "backend", lambda: isal)
    monkeypatch.setattr(_gzip, "zlib", stdlib)
    return isal, stdlib


def test_responses_take_the_backend_and_static_takes_zlib_level_nine(
    stand_in_isal,
):
    # a per-response body pays the CPU on every build, so it takes the
    # backend at its fastest level; a document compressed once pays it
    # once, so it takes zlib's best ratio whatever the backend is
    isal, stdlib = stand_in_isal
    fast = _gzip.gzip_body(_SAMPLE)
    strong = _gzip.gzip_static(_SAMPLE)
    assert isal.levels == [1]
    assert stdlib.levels == [9]
    assert strong == _zlib_level_nine(_SAMPLE)
    assert len(strong) <= len(fast)


def test_falls_back_to_stdlib_zlib_without_isal(without_isal):
    assert _gzip.backend() is zlib
    assert gzip.decompress(_gzip.gzip_body(_SAMPLE)) == _SAMPLE
    assert _gzip.gzip_static(_SAMPLE) == _zlib_level_nine(_SAMPLE)


@requires_isal
def test_gzip_static_takes_zlib_with_isal_installed(fresh_backend):
    from isal import isal_zlib

    assert _gzip.backend() is isal_zlib
    assert _gzip.gzip_static(_SAMPLE) == _zlib_level_nine(_SAMPLE)


@requires_isal
def test_prefers_isal_when_installed(fresh_backend):
    from isal import isal_zlib

    assert _gzip.backend() is isal_zlib


def test_use_for_aiohttp_installs_the_same_backend(aiohttp_backend):
    _gzip.use_for_aiohttp()
    assert aiohttp_backend._zlib_backend is _gzip.backend()


@pytest.mark.parametrize("isal", [True, False], ids=["isal", "zlib"])
async def test_aiohttp_gzip_round_trips_through_the_backend(
    request, aiohttp_backend, fresh_backend, isal
):
    # The cluster's exchange end to end: a /peer-style response gzipped by
    # enable_compression, read back raw and through the client's inflate.
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    if isal:
        pytest.importorskip("isal")
    else:
        request.getfixturevalue("without_isal")
    _gzip.use_for_aiohttp()

    async def handler(_request):
        resp = web.Response(body=_SAMPLE, content_type="application/json")
        resp.enable_compression(web.ContentCoding.gzip)
        return resp

    app = web.Application()
    app.router.add_get("/peer", handler)
    async with TestClient(TestServer(app)) as client:
        inflated = await client.get("/peer")
        assert inflated.headers["Content-Encoding"] == "gzip"
        assert await inflated.read() == _SAMPLE
        raw = await client.get("/peer", auto_decompress=False)
        assert gzip.decompress(await raw.read()) == _SAMPLE
