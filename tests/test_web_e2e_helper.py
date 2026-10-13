"""Test the port choice of the daemon helper in ``tests/_web_e2e.py``.

The tests start no browser and no scheduler.
"""

import asyncio
import threading

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


class _Listener:
    """A stand-in for ``asyncio.Server`` with one socket on ``port``."""

    def __init__(self, port):
        self.port = port
        self.closed = False
        self.sockets = (self,)

    def getsockname(self):
        return ("127.0.0.1", self.port)

    def close(self):
        self.closed = True


def _assigning(ports, binds):
    """A ``create_server`` that gives port 0 the first free one of ``ports``.

    A call past ``binds`` fails, so a wrapper that never settles on a port
    fails its test.
    """
    made = []

    async def create_server(protocol_factory, host=None, port=None, **kwargs):
        assert len(made) < binds, "bound more often than the ports allow"
        if port == 0:
            taken = {other.port for other in made if not other.closed}
            port = next(p for p in ports if p not in taken)
        made.append(_Listener(port))
        return made[-1]

    return create_server, made


async def test_port_zero_bind_goes_on_past_ports_that_chromium_restricts():
    create_server, made = _assigning([1720, 1723, 50123], binds=3)
    bind = e2e._bind_past_unsafe_ports(create_server)
    server = await bind(asyncio.Protocol, "127.0.0.1", 0, backlog=8)
    assert [(listener.port, listener.closed) for listener in made] == [
        (1720, True),
        (1723, True),
        (50123, False),
    ]
    assert server is made[2]


async def test_named_port_is_bound_as_asked():
    create_server, made = _assigning([], binds=1)
    bind = e2e._bind_past_unsafe_ports(create_server)
    server = await bind(asyncio.Protocol, "127.0.0.1", port=6000)
    assert [(listener.port, listener.closed) for listener in made] == [
        (6000, False)
    ]
    assert server is made[0]


class _FirstAsked:
    """A restricted set that holds only the first port asked about."""

    def __init__(self):
        self.asked = []

    def __contains__(self, port):
        self.asked.append(port)
        return len(self.asked) == 1


def test_daemon_thread_binds_past_a_restricted_port(tmp_path, monkeypatch):
    # The operating system assigns these ports, so the restricted set is
    # one that holds whichever port comes first.
    restricted = _FirstAsked()
    monkeypatch.setattr(e2e, "_UNSAFE_PORTS", restricted)
    daemon = e2e.Daemon(tmp_path)
    bound = []

    async def listen():
        loop = asyncio.get_running_loop()
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        bound.extend(sock.getsockname()[1] for sock in server.sockets)
        server.close()
        await server.wait_closed()

    monkeypatch.setattr(daemon, "_main", listen)
    thread = threading.Thread(target=daemon._run, daemon=True)
    thread.start()
    thread.join(30)
    assert not thread.is_alive()
    assert daemon._error is None
    refused, kept = restricted.asked
    assert bound == [kept]
    assert kept != refused
