"""Tests for the host address helpers (cronstable.netutil)."""

import pytest

from cronstable import netutil


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8080",
        "http://127.8.9.10",
        "http://localhost:8080",
        "http://cron.localhost",
        "http://[::1]:8080",
        "http://0.0.0.0:8080",
        "http://[::]:8080",
        "http://[::ffff:127.0.0.1]:8080",
        # other spellings the socket layer reads as the same addresses
        "http://localhost.:8080",
        "http://LOCALHOST:8080",
        "http://127.1:8080",
        "http://0x7f.0.0.1:8080",
        "http://2130706433:8080",
        "http://127.000.000.001:8080",
        "http://0:8080",
    ],
)
def test_is_loopback_for_addresses_no_other_host_can_reach(url):
    assert netutil.is_loopback(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://192.168.1.50:8080",
        "http://10.0.0.5",
        "http://[fd00::5]:8080",
        "https://cron.example.net",
        "http://nas.local:8080",
        "http://localhost.example.net",
        "http://192.168.1:8080",
        "http://0xc0.0xa8.1.50:8080",
        "http://127.example.net",
        "http://",
    ],
)
def test_is_loopback_false_for_reachable_addresses(url):
    assert not netutil.is_loopback(url)


class _FakeSocket:
    def __init__(self, address=None, error=None):
        self._address, self._error = address, error
        self.connected = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self, target):
        if self._error is not None:
            raise self._error
        self.connected = target

    def getsockname(self):
        return (self._address, 54321)


def test_lan_address_reads_the_default_routes_source(monkeypatch):
    made = []

    def fake_socket(family, kind):
        made.append(_FakeSocket("192.168.1.50"))
        assert (family, kind) == (
            netutil.socket.AF_INET,
            netutil.socket.SOCK_DGRAM,
        )
        return made[-1]

    monkeypatch.setattr(netutil.socket, "socket", fake_socket)
    assert netutil.lan_address() == "192.168.1.50"
    # a documentation address: a UDP connect sends nothing to it
    assert made[0].connected == ("192.0.2.1", 9)


def _no_route(*args):
    return _FakeSocket(error=OSError("network is unreachable"))


def _unresolved(name):
    raise OSError("name or service not known")


@pytest.mark.parametrize(
    "probe", [_no_route, lambda *a: _FakeSocket("127.0.0.1")]
)
def test_lan_address_falls_back_to_the_hostnames_address(monkeypatch, probe):
    # a host with a LAN interface and no default route
    monkeypatch.setattr(netutil.socket, "socket", probe)
    monkeypatch.setattr(netutil.socket, "gethostname", lambda: "nas")
    monkeypatch.setattr(
        netutil.socket,
        "gethostbyname",
        lambda name: {"nas": "192.168.1.50"}[name],
    )
    assert netutil.lan_address() == "192.168.1.50"


def test_lan_address_none_without_a_route_or_a_hostname_address(monkeypatch):
    monkeypatch.setattr(netutil.socket, "socket", _no_route)
    monkeypatch.setattr(netutil.socket, "gethostbyname", _unresolved)
    assert netutil.lan_address() is None


@pytest.mark.parametrize("resolved", ["127.0.1.1", "0.0.0.0"])
def test_lan_address_none_when_every_answer_is_loopback(monkeypatch, resolved):
    monkeypatch.setattr(
        netutil.socket, "socket", lambda *a: _FakeSocket("127.0.0.1")
    )
    monkeypatch.setattr(netutil.socket, "gethostbyname", lambda name: resolved)
    assert netutil.lan_address() is None


def test_lan_address_on_this_host_is_reachable_or_absent():
    address = netutil.lan_address()
    assert address is None or not netutil.is_loopback("http://" + address)


@pytest.mark.parametrize(
    "host, expected",
    [
        ("127.0.0.1", "127.0.0.1"),
        ("[::1]", "::1"),
        ("::1", "::1"),
        ("0.0.0.0", "0.0.0.0"),
        # forms the socket layer reads as the same addresses
        ("127.1", "127.0.0.1"),
        ("0x7f.0.0.1", "127.0.0.1"),
        ("2130706433", "127.0.0.1"),
        ("0", "0.0.0.0"),
    ],
)
def test_ip_literal_reads_an_address_in_any_spelling(host, expected):
    assert str(netutil.ip_literal(host)) == expected


@pytest.mark.parametrize(
    "host",
    [
        "",
        "localhost",
        "nas.local",
        "127.example.net",
        "[::1",
        # text after an address, which some C libraries read past
        "127.0.0.1 x",
        "127.0.0.1\n",
    ],
)
def test_ip_literal_none_for_a_name(host):
    assert netutil.ip_literal(host) is None


def test_netloc_brackets_an_ipv6_address():
    assert netutil.netloc("192.168.1.50", 8080) == "192.168.1.50:8080"
    assert netutil.netloc("fd00::5", 8080) == "[fd00::5]:8080"
    assert netutil.netloc("nas.local") == "nas.local"
    assert netutil.netloc("localhost") == "localhost"
    assert netutil.netloc("::") == "[::]"
    assert netutil.netloc("fe80::1%eth0") == "[fe80::1%eth0]"
    # a host that has its brackets keeps them, and a port can be text
    assert netutil.netloc("[fd00::1]", "443") == "[fd00::1]:443"
