"""gzip for HTTP response bodies, with optional ISA-L acceleration.

The ``isal`` package wraps Intel's ISA-L, whose SIMD deflate compresses at
level 1 about three times as fast as the stock zlib that most Linux
Pythons link, with output within a few percent of zlib's size.  It is part
of the ``speedups`` extra because wheels exist only for x86-64 and ARM64.
Without it, this module uses the standard library's ``zlib``.  Both produce
standard gzip streams, and every client decodes either one.

The backend loads on first use. Importing ``isal`` also imports the
standard library's ``gzip`` module, which costs about 5 ms, and a daemon
without a web or cluster listener never compresses anything.
"""

import functools
import zlib
from types import ModuleType


@functools.cache
def backend() -> ModuleType:
    """The zlib-compatible module: ``isal.isal_zlib`` when installed."""
    try:
        from isal import isal_zlib
    except ImportError:
        return zlib
    lib: ModuleType = isal_zlib
    return lib


def _pack(lib: ModuleType, body: bytes, level: int) -> bytes:
    """``body`` as one gzip stream at ``level``.

    ``wbits=31`` wraps the deflate stream in a gzip container.
    """
    packer = lib.compressobj(level, lib.DEFLATED, 31)
    packed: bytes = packer.compress(body) + packer.flush()
    return packed


def gzip_body(body: bytes) -> bytes:
    """``body`` as a gzip stream, level 1.

    Level 1 on purpose: the payloads are highly repetitive JSON, and
    higher levels cost multiples of the CPU for little gain.
    """
    return _pack(backend(), body, 1)


def gzip_static(body: bytes) -> bytes:
    """``body`` as a gzip stream from the standard library's ``zlib``,
    level 9.

    For a document compressed once and served for the life of the
    process: the CPU is paid one time and the size on every transfer.
    It uses ``zlib`` whatever :func:`backend` returns, because ISA-L
    trades ratio for speed and its levels stop at 3.
    """
    return _pack(zlib, body, zlib.Z_BEST_COMPRESSION)


def use_for_aiohttp() -> None:
    """Point aiohttp's compression at :func:`backend` too.

    aiohttp compresses the cluster's ``/peer`` responses and decompresses
    every compressed response its clients read.  The setting is
    process-wide and aiohttp reads it each time it builds a compressor, so
    the listeners call this before they start.  Calling it imports
    aiohttp, which ``cronstable.cron`` defers until a listener starts.
    """
    import aiohttp

    aiohttp.set_zlib_backend(backend())
