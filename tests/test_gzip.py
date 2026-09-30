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


def test_falls_back_to_stdlib_zlib_without_isal(without_isal):
    assert _gzip.backend() is zlib
    assert gzip.decompress(_gzip.gzip_body(_SAMPLE)) == _SAMPLE


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
