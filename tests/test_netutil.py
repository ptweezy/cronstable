"""Tests for the host address helpers (cronstable.netutil)."""

import ipaddress
import itertools
import socket

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
        # other spellings of the same name
        "http://localhost.:8080",
        "http://LOCALHOST:8080",
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


@pytest.mark.parametrize(
    "hostname",
    [
        # a label of 64 characters, which Linux allows
        "n" * 64,
        "host..example",
        ".leading",
    ],
)
def test_lan_address_none_for_a_hostname_that_idna_refuses(
    monkeypatch, hostname
):
    # the real lookup: the codec raises UnicodeError ahead of any resolver
    monkeypatch.setattr(netutil.socket, "socket", _no_route)
    monkeypatch.setattr(netutil.socket, "gethostname", lambda: hostname)
    assert netutil.lan_address() is None


def test_lan_address_none_when_the_lookup_raises_unicode_error(monkeypatch):
    def refused(name):
        raise UnicodeError("label too long")

    monkeypatch.setattr(
        netutil.socket, "socket", lambda *a: _FakeSocket("127.0.0.1")
    )
    monkeypatch.setattr(netutil.socket, "gethostbyname", refused)
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


# 0127 and 0177 are 127 and 177 in decimal, and 87 and 127 in octal.
_SPELLED = ["127.0.0.1", "177.20.30.40", "0.0.0.0", "10.0.0.8", "192.168.1.50"]


def _spellings():
    """Each address of ``_SPELLED`` in every form of one to four parts,
    with each part in decimal, with a leading zero, in octal, and in
    hexadecimal."""
    for dotted in _SPELLED:
        packed = ipaddress.ip_address(dotted).packed
        for split in range(4):
            # the last part holds the bytes that the parts ahead of it leave
            values = [*packed[:split], int.from_bytes(packed[split:], "big")]
            forms = [
                (str(v), "0{}".format(v), "0{:o}".format(v), hex(v))
                for v in values
            ]
            for parts in itertools.product(*forms):
                yield ".".join(parts)


def _socket_layer(host):
    """The addresses that this host's socket layer reads ``host`` as.

    A bind or a connect asks ``inet_pton`` and then ``getaddrinfo``. The C
    libraries differ in how each call reads a leading zero, and in whether
    ``getaddrinfo`` reads a short or hexadecimal form or looks it up as a
    name.
    """
    found = set()
    try:
        found.add(socket.inet_ntoa(socket.inet_pton(socket.AF_INET, host)))
    except OSError:
        pass
    try:
        infos = socket.getaddrinfo(
            host, None, socket.AF_INET, flags=socket.AI_NUMERICHOST
        )
    except (OSError, UnicodeError):
        infos = []
    return found | {info[4][0] for info in infos}


def _strict_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """``getaddrinfo`` on a host that reads an IPv4 address where
    ``inet_pton`` does and looks up any other text as a name."""
    # without the flag, the call is a name lookup
    assert flags & socket.AI_NUMERICHOST
    try:
        packed = socket.inet_pton(socket.AF_INET, host)
    except OSError:
        raise socket.gaierror(socket.EAI_NONAME, "no such name") from None
    return [(socket.AF_INET, type, proto, "", (socket.inet_ntoa(packed), 0))]


def _lenient_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """``getaddrinfo`` on a host that reads every form that ``inet_aton``
    reads."""
    assert flags & socket.AI_NUMERICHOST
    try:
        packed = socket.inet_aton(host)
    except OSError:
        raise socket.gaierror(socket.EAI_NONAME, "no such name") from None
    return [(socket.AF_INET, type, proto, "", (socket.inet_ntoa(packed), 0))]


@pytest.mark.parametrize(
    "getaddrinfo",
    [None, _strict_getaddrinfo, _lenient_getaddrinfo],
    ids=["host", "strict", "lenient"],
)
def test_ip_literal_reads_an_address_as_this_hosts_socket_layer_does(
    monkeypatch, getaddrinfo
):
    if getaddrinfo is not None:
        monkeypatch.setattr(netutil.socket, "getaddrinfo", getaddrinfo)
    for dotted in _SPELLED:
        assert str(netutil.ip_literal(dotted)) == dotted
        assert _socket_layer(dotted) == {dotted}
    # a check that calls a host loopback holds only when the listener then
    # binds the address that the check read
    misread = {}
    for host in _spellings():
        address, bound = netutil.ip_literal(host), _socket_layer(host)
        if address is not None and bound != {str(address)}:
            misread[host] = (str(address), sorted(bound))
    assert misread == {}


# Forms that inet_aton reads and the ipaddress module refuses. A bind reads
# each one as its address on a host whose socket layer reads the form, and
# looks it up as a name on any other host.
_FORMS = [
    ("127.1", "127.0.0.1"),
    ("0x7f.0.0.1", "127.0.0.1"),
    ("2130706433", "127.0.0.1"),
    ("127.000.000.001", "127.0.0.1"),
    ("0", "0.0.0.0"),
]


@pytest.mark.parametrize("host, expected", _FORMS)
def test_ip_literal_reads_a_form_that_this_hosts_socket_layer_reads(
    host, expected
):
    reads = _socket_layer(host) == {expected}
    address = netutil.ip_literal(host)
    assert address == (ipaddress.ip_address(expected) if reads else None)
    # each of these addresses is loopback or unspecified
    assert netutil.is_loopback("http://{}:8080".format(host)) is reads


@pytest.mark.parametrize("host, expected", _FORMS)
def test_ip_literal_reads_a_form_where_getaddrinfo_reads_it(
    monkeypatch, host, expected
):
    monkeypatch.setattr(netutil.socket, "getaddrinfo", _lenient_getaddrinfo)
    assert str(netutil.ip_literal(host)) == expected
    assert netutil.is_loopback("http://{}:8080".format(host))


@pytest.mark.parametrize("host", ["127.1", "0x7f.0.0.1", "2130706433", "0"])
def test_ip_literal_none_where_getaddrinfo_looks_the_form_up(
    monkeypatch, host
):
    # a bind on this host resolves the text as a hostname
    monkeypatch.setattr(netutil.socket, "getaddrinfo", _strict_getaddrinfo)
    assert netutil.ip_literal(host) is None
    assert not netutil.is_loopback("http://{}:8080".format(host))
    # the dotted quad is an address to every C library
    assert str(netutil.ip_literal("127.0.0.1")) == "127.0.0.1"
    assert netutil.is_loopback("http://127.0.0.1:8080")
    assert netutil.is_loopback("http://0.0.0.0:8080")


def test_ip_literal_takes_the_reading_of_inet_pton_ahead_of_getaddrinfo(
    monkeypatch,
):
    def decimal(family, host):
        # inet_pton on a host that reads a leading zero as decimal
        return bytes(int(part) for part in host.split("."))

    def unasked(*args, **kwargs):
        raise AssertionError("a bind asks inet_pton first")

    monkeypatch.setattr(netutil.socket, "inet_pton", decimal)
    monkeypatch.setattr(netutil.socket, "getaddrinfo", unasked)
    assert str(netutil.ip_literal("127.000.000.001")) == "127.0.0.1"
    # 0177 is 127 in octal to inet_aton
    assert netutil.ip_literal("0177.0.0.1") is None


@pytest.mark.parametrize(
    "found, expected",
    [
        (["127.0.0.1"], "127.0.0.1"),
        # a host that answers once for each socket type
        (["127.0.0.1", "127.0.0.1"], "127.0.0.1"),
        # a bind takes the address that getaddrinfo reads
        (["127.1.0.0"], None),
        (["127.0.0.1", "127.1.0.0"], None),
        ([], None),
    ],
)
def test_ip_literal_needs_getaddrinfo_to_read_the_one_address(
    monkeypatch, found, expected
):
    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        # the call carries the one flag, so it looks up no name
        assert (family, flags) == (socket.AF_INET, socket.AI_NUMERICHOST)
        return [(family, type, proto, "", (address, 0)) for address in found]

    monkeypatch.setattr(netutil.socket, "getaddrinfo", getaddrinfo)
    address = netutil.ip_literal("127.1")
    assert address == (ipaddress.ip_address(expected) if expected else None)


@pytest.mark.parametrize(
    "error",
    [
        socket.gaierror(socket.EAI_NONAME, "no such name"),
        OSError("getaddrinfo failed"),
        UnicodeError("label too long"),
    ],
)
def test_ip_literal_none_when_getaddrinfo_raises(monkeypatch, error):
    def refused(*args, **kwargs):
        raise error

    monkeypatch.setattr(netutil.socket, "getaddrinfo", refused)
    assert netutil.ip_literal("127.1") is None


def test_ip_literal_none_for_a_form_that_idna_refuses():
    # the real call: the codec raises UnicodeError for a label of 64
    # characters, ahead of the C library
    assert netutil.ip_literal("0" * 64) is None
    assert netutil.ip_literal("0x" + "0" * 62) is None
    assert not netutil.is_loopback("http://{}:8080".format("0" * 64))


def test_netloc_brackets_an_ipv6_address():
    assert netutil.netloc("192.168.1.50", 8080) == "192.168.1.50:8080"
    assert netutil.netloc("fd00::5", 8080) == "[fd00::5]:8080"
    assert netutil.netloc("nas.local") == "nas.local"
    assert netutil.netloc("localhost") == "localhost"
    assert netutil.netloc("::") == "[::]"
    assert netutil.netloc("fe80::1%eth0") == "[fe80::1%eth0]"
    # a host that has its brackets keeps them, and a port can be text
    assert netutil.netloc("[fd00::1]", "443") == "[fd00::1]:443"
