"""Tests for the terminal clients' pairing link (cronstable.pairlink).

``cronstable pair`` and the terminal dashboard must write the bytes the web
dashboard's Pair a device panel writes, so some expectations here are read
out of ``cronstable/web/index.html``.
"""

import base64
import json
import os
import re

import pytest

from cronstable import pairlink

PAGE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "cronstable",
    "web",
    "index.html",
)

# GET /whoami replies in the four shapes the daemon produces.
OPEN = {
    "authenticated": False,
    "label": None,
    "scopes": ["approve", "control", "view"],
    "allScopes": True,
}
ANONYMOUS = {
    "authenticated": False,
    "label": "anonymous",
    "scopes": ["view"],
    "allScopes": False,
}
ADMIN = {
    "authenticated": True,
    "label": "admin",
    "scopes": ["approve", "control", "view"],
    "allScopes": True,
}
PHONE = {
    "authenticated": True,
    "label": "phone",
    "scopes": ["control", "view"],
    "allScopes": False,
}
VIEWER = {
    "authenticated": True,
    "label": "viewer",
    "scopes": ["view"],
    "allScopes": False,
}


def _page():
    with open(PAGE, encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# text shared with the page
# ---------------------------------------------------------------------------


def test_full_access_note_is_the_pages_warning():
    found = re.search(r'id="pairWarn"[^>]*>(.*?)</div>', _page())
    warning = re.sub(r"<[^>]+>", "", found.group(1))
    assert ("full-access token", warning) in pairlink.notes(ADMIN)


# ---------------------------------------------------------------------------
# addresses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "given, expected",
    [
        ("http://127.0.0.1:8080", "http://127.0.0.1:8080"),
        ("http://nas.local:8080/", "http://nas.local:8080"),
        ("  https://cron.example.net  ", "https://cron.example.net"),
        ("https://cron.example.net/ops/", "https://cron.example.net/ops"),
        (
            "https://user:pw@cron.example.net:8443",
            "https://cron.example.net:8443",
        ),
        ("http://[fd00::5]:8080/?x=1#frag", "http://[fd00::5]:8080"),
        # a path is percent-encoded, as a browser's address bar has it
        ("http://nas.local:8080/x y/", "http://nas.local:8080/x%20y"),
        ("http://nas.local/café", "http://nas.local/caf%C3%A9"),
        ("http://nas.local/a%20b/~c;d=1", "http://nas.local/a%20b/~c;d=1"),
        ("http://nas.local/a\x7fb", "http://nas.local/a%7Fb"),
    ],
)
def test_base_url_keeps_what_the_app_dials(given, expected):
    assert pairlink.base_url(given) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "nas.local:8080",
        "ftp://nas.local",
        "unix:///run/cronstable.sock",
        "http://",
        "http://nas.local:99999",
        "http://nas.local:0",
        "http://[fd00::5",
        # a host that no request can name
        "http://nas local:8080",
        "http://nas\x00.local",
        "http://nas\x7f.local:8080",
    ],
)
def test_base_url_rejects_what_the_app_cannot_dial(bad):
    with pytest.raises(ValueError, match="not an http:// or https:// URL"):
        pairlink.base_url(bad)


def test_with_host_keeps_scheme_port_and_path():
    assert (
        pairlink.with_host("http://127.0.0.1:8080", "192.168.1.50")
        == "http://192.168.1.50:8080"
    )
    assert (
        pairlink.with_host("https://localhost/ops", "10.0.0.5")
        == "https://10.0.0.5/ops"
    )
    assert (
        pairlink.with_host("http://127.0.0.1:8080", "fd00::5")
        == "http://[fd00::5]:8080"
    )


# ---------------------------------------------------------------------------
# what a reply says about the listener
# ---------------------------------------------------------------------------


def test_instance_reads_the_daemons_id():
    assert pairlink.instance(dict(PHONE, instance="k3Jx")) == "k3Jx"


@pytest.mark.parametrize(
    "whoami", [PHONE, dict(PHONE, instance=""), dict(PHONE, instance=7)]
)
def test_instance_none_for_a_reply_that_names_none(whoami):
    assert pairlink.instance(whoami) is None
    for other in (None, [], "x"):
        assert pairlink.instance(other) is None


LAN_IP = "192.168.1.50"
LOCAL = "http://127.0.0.1:8080"


def _dial(listeners, base=LOCAL, lan=LAN_IP):
    return pairlink.dial_urls(dict(PHONE, listeners=listeners), base, lan)


@pytest.mark.parametrize(
    "listeners, expected",
    [
        # a listener on every IPv4 address serves the LAN address
        (["http://0.0.0.0:8080"], ["http://192.168.1.50:8080"]),
        # a listener on the LAN address itself
        (
            ["http://127.0.0.1:9090", "http://192.168.1.50:8080"],
            ["http://192.168.1.50:8080"],
        ),
        # a listener on another interface, such as a VPN's, is dialed at
        # its own address
        (
            ["http://127.0.0.1:8080", "http://10.8.0.1:8080"],
            ["http://10.8.0.1:8080"],
        ),
        (
            ["http://127.0.0.1:8080", "http://[fd00::5]:8080"],
            ["http://[fd00::5]:8080"],
        ),
        # the addresses on the LAN come first, whatever the reply's order
        (
            ["http://10.8.0.1:8080", "http://0.0.0.0:8080"],
            ["http://192.168.1.50:8080", "http://10.8.0.1:8080"],
        ),
        # the port of the base, then each listener on its own port
        (
            ["http://0.0.0.0:9090", "http://192.168.1.50:9091"],
            [
                "http://192.168.1.50:8080",
                "http://192.168.1.50:9090",
                "http://192.168.1.50:9091",
            ],
        ),
        # a port bound on loopback alone is left out on the LAN address
        (
            ["http://127.0.0.1:8080", "http://0.0.0.0:9090"],
            ["http://192.168.1.50:9090"],
        ),
        # a published port in front of a listener on one address, such as
        # a container's own: the port of the base on the LAN address first
        (
            ["http://172.17.0.2:9090"],
            ["http://192.168.1.50:8080", "http://172.17.0.2:9090"],
        ),
        # a link-local listener is left out beside one that a phone can dial
        (
            ["http://[fe80::1]:8080", "http://10.8.0.1:8080"],
            ["http://10.8.0.1:8080"],
        ),
    ],
)
def test_dial_urls_names_the_addresses_a_phone_might_reach(
    listeners, expected
):
    assert _dial(listeners) == expected


@pytest.mark.parametrize(
    "listeners",
    [
        [],
        # loopback only
        ["http://127.0.0.1:8080", "http://[::1]:8080"],
        # a link-local address, which a phone cannot dial
        ["http://127.0.0.1:8080", "http://[fe80::1]:8080"],
        ["http://127.0.0.1:8080", "http://[fe80::1%eth0]:8080"],
        ["http://127.0.0.1:8080", "http://169.254.7.9:8080"],
        # the other scheme
        ["http://127.0.0.1:8080", "https://0.0.0.0:8443"],
        # every IPv6 address: that socket takes no IPv4 connection, and
        # the LAN address is an IPv4 one
        ["http://127.0.0.1:8080", "http://[::]:8080"],
        # entries that are no listener address
        [7, None, "nas.local:8080", "http://[fd00::5", "http://nas.local:1"],
        # a port that no request can name
        ["http://0.0.0.0:99999", "http://0.0.0.0:0"],
        # a scheme that the command does not speak
        ["ftp://0.0.0.0:8080", "unix:///run/cronstable.sock"],
    ],
)
def test_dial_urls_empty_without_a_listener_a_phone_can_reach(listeners):
    assert _dial(listeners) == []


@pytest.mark.parametrize(
    "whoami", [PHONE, dict(PHONE, listeners="http://0.0.0.0:8080"), None, []]
)
def test_dial_urls_empty_for_a_reply_that_lists_no_listeners(whoami):
    assert pairlink.dial_urls(whoami, LOCAL, LAN_IP) == []
    assert not pairlink.lists_listeners(whoami)


def test_lists_listeners_for_a_reply_that_names_them():
    assert pairlink.lists_listeners(dict(PHONE, listeners=[]))
    assert pairlink.lists_listeners(dict(PHONE, listeners=[LOCAL]))


def test_dial_urls_without_a_lan_address_names_only_bound_addresses():
    # a host with no default route: a listener on every address has no
    # address to name, and one on a VPN address has its own
    assert _dial(["http://0.0.0.0:8080"], lan=None) == []
    both = ["http://0.0.0.0:8080", "http://10.8.0.1:9090"]
    assert _dial(both, lan=None) == ["http://10.8.0.1:9090"]


def test_dial_urls_keeps_the_scheme_and_the_path_of_the_base():
    listeners = ["http://0.0.0.0:8080", "https://0.0.0.0:8443"]
    assert _dial(listeners, base="https://localhost:9443/ops") == [
        # a published port in front of the daemon, under the base's path
        "https://192.168.1.50:9443/ops",
        "https://192.168.1.50:8443",
    ]


def test_dial_urls_takes_only_the_address_of_a_listener():
    # the reply is the server's text: its path, query, and credentials
    # reach no address
    hostile = "http://user:pw@0.0.0.0:9090/\x1b[2J?x=\x07#frag"
    assert _dial(["http://127.0.0.1:8080", hostile]) == [
        "http://192.168.1.50:9090"
    ]


@pytest.mark.parametrize(
    "listeners, url",
    [
        # loopback holds the port, and the address is another host
        (["http://127.0.0.1:8080"], "http://192.168.1.50:8080"),
        (
            ["http://127.0.0.1:8080", "http://0.0.0.0:9090"],
            "http://192.168.1.50:8080",
        ),
        (["http://[::1]:8080"], "http://192.168.1.50:8080"),
        # another interface, such as a VPN's
        (["http://10.8.0.1:8080"], "http://192.168.1.50:8080"),
        # the other scheme holds the port all the same
        (["https://127.0.0.1:8080"], "http://192.168.1.50:8080"),
        # a URL that names no port has its scheme's
        (["http://127.0.0.1:80"], "http://192.168.1.50"),
        (["https://127.0.0.1"], "https://192.168.1.50:443"),
        # every IPv6 address is no IPv4 address
        (
            ["http://127.0.0.1:8080", "http://[::]:8080"],
            "http://192.168.1.50:8080",
        ),
    ],
)
def test_bound_elsewhere_for_a_port_the_daemon_binds_on_other_hosts(
    listeners, url
):
    assert pairlink.bound_elsewhere(dict(PHONE, listeners=listeners), url)


@pytest.mark.parametrize(
    "listeners",
    [
        # a listener serves the address on the port
        ["http://0.0.0.0:8080"],
        ["http://127.0.0.1:8080", "http://192.168.1.50:8080"],
        # no listener binds the port: a published container port
        ["http://0.0.0.0:9090"],
        ["http://127.0.0.1:9090"],
        [],
        # entries that are no listener address
        [7, None, "nas.local:8080", "http://127.0.0.1:0"],
    ],
)
def test_bound_elsewhere_false_for_a_port_that_may_serve_the_address(
    listeners,
):
    url = "http://192.168.1.50:8080"
    assert not pairlink.bound_elsewhere(dict(PHONE, listeners=listeners), url)
    for whoami in (PHONE, None, []):
        assert not pairlink.bound_elsewhere(whoami, url)


def test_lan_note_says_which_address_stands_in():
    loopback, lan = "http://127.0.0.1:8080", "http://192.168.1.50:8080"
    assert pairlink.lan_note(loopback, lan) == (
        "The code names this host's address {} in place of {}.".format(
            lan, loopback
        )
    )
    assert pairlink.lan_note(lan, lan) is None


def test_default_name_prefers_the_cluster_node():
    url = "http://192.168.1.50:8080"
    clustered = {"enabled": True, "node_name": "node-a"}
    assert pairlink.default_name(url, clustered) == "node-a"
    for cluster in (
        None,
        {},
        {"enabled": False, "node_name": "node-a"},
        {"enabled": True},
        {"enabled": True, "node_name": ""},
        {"enabled": True, "node_name": 7},
        ["enabled"],
    ):
        assert pairlink.default_name(url, cluster) == "192.168.1.50:8080"


def test_link_base_follows_the_daemon_and_falls_back():
    base = "https://relay.example.test/pair"
    assert pairlink.link_base(dict(PHONE, pairLinkBase=base)) == base
    for whoami in (PHONE, dict(PHONE, pairLinkBase=""), None, [], "x"):
        assert pairlink.link_base(whoami) == pairlink.PAIR_LINK_FALLBACK
    assert (
        pairlink.link_base(dict(PHONE, pairLinkBase=7))
        == pairlink.PAIR_LINK_FALLBACK
    )


@pytest.mark.parametrize(
    "base",
    [
        # terminal escapes, which the link would carry to stdout
        "https://relay.example.test/pair\x1b]0;x\x07",
        "https://relay.example.test/pair\x9b2J",
        "https://relay.example.test/pair\n",
        "javascript:alert(1)",
        "relay.example.test/pair",
        "http://[bad/pair",
    ],
)
def test_link_base_refuses_what_is_no_printable_http_url(base):
    with pytest.raises(ValueError) as raised:
        pairlink.link_base(dict(PHONE, pairLinkBase=base))
    message = str(raised.value)
    assert message == (
        "the server's pairing link base {!a} is not a printable http:// or "
        "https:// URL".format(base)
    )
    assert message.isprintable()
    with pytest.raises(ValueError, match="pairing link base"):
        pairlink.pairing(dict(PHONE, pairLinkBase=base), LOCAL, "t")


# ---------------------------------------------------------------------------
# the payload and the link
# ---------------------------------------------------------------------------


def test_payload_is_the_pages_compact_json():
    assert (
        pairlink.payload("nas", "http://192.168.1.50:8080", "s3cr3t")
        == '{"v":1,"name":"nas","url":"http://192.168.1.50:8080",'
        '"token":"s3cr3t"}'
    )


def test_payload_keeps_non_ascii_names_and_writes_an_empty_token():
    name = "büro-節点-🚀/north+east?"
    text = pairlink.payload(name, "https://cron.example.net", None)
    # JSON.stringify leaves non-ASCII as is; the shorter form keeps the
    # code small
    assert name in text
    assert json.loads(text) == {
        "v": 1,
        "name": name,
        "url": "https://cron.example.net",
        "token": "",
    }


def test_payload_escapes_what_a_terminal_does_not_print():
    # DEL, a C1 control (CSI), a bidi override, a zero-width space, and a
    # language tag from beyond the BMP, in a name that the daemon sent
    name = "na\x7f\x9b2J\u202es\u200b\U000e0001 büro"
    url = "https://cron.example.net"
    text = pairlink.payload(name, url, "t")
    assert text == (
        r'{"v":1,"name":"na\u007f\u009b2J\u202es\u200b\udb40\udc01 büro",'
        '"url":"https://cron.example.net","token":"t"}'
    )
    assert text.isprintable()
    # the app reads the name that the page's own JSON carries
    assert json.loads(text) == {"v": 1, "name": name, "url": url, "token": "t"}
    link = pairlink.pairing(PHONE, url, "t", name=name).link
    fragment = link.split("#")[1]
    decoded = base64.urlsafe_b64decode(fragment + "=" * (-len(fragment) % 4))
    assert json.loads(decoded)["name"] == name


@pytest.mark.parametrize(
    "name, url, named",
    [
        ("na\udcffs", "http://10.0.0.5:8080", "the server name 'na\\udcffs'"),
        (
            "nas",
            "http://na\ud800s.local:8080",
            "the address 'http://na\\ud800s.local:8080'",
        ),
    ],
)
def test_payload_refuses_what_utf8_cannot_encode(name, url, named):
    # a lone surrogate, which neither a link nor a pipe can carry
    with pytest.raises(ValueError) as raised:
        pairlink.payload(name, url, "t")
    assert str(raised.value) == (
        named + " holds a character that UTF-8 cannot encode"
    )
    with pytest.raises(ValueError, match="UTF-8 cannot encode"):
        pairlink.pairing(PHONE, url, "t", name=name)


def test_accepted_token_is_the_one_the_daemon_authenticated():
    assert pairlink.accepted_token(PHONE, "s3cr3t") == "s3cr3t"
    assert pairlink.accepted_token(ADMIN, "s3cr3t") == "s3cr3t"


@pytest.mark.parametrize("whoami", [OPEN, ANONYMOUS, None, [], {}])
def test_accepted_token_drops_one_the_daemon_ignored(whoami):
    # an open daemon reads no token, so one meant for another server
    # stays out of the code
    assert pairlink.accepted_token(whoami, "for-another-server") is None
    assert pairlink.accepted_token(whoami, None) is None


def test_link_carries_the_payload_as_unpadded_base64url():
    text = pairlink.payload("büro?>", "http://10.0.0.5:8080", "t/+k=")
    link = pairlink.link(text, "https://relay.example.test/pair")
    base, fragment = link.split("#")
    assert base == "https://relay.example.test/pair"
    assert not set("=+/") & set(fragment)
    padded = fragment + "=" * (-len(fragment) % 4)
    assert base64.urlsafe_b64decode(padded).decode("utf-8") == text


def test_label_names_the_server_once():
    url = "http://192.168.1.50:8080"
    assert pairlink.label("192.168.1.50:8080", url) == url
    assert pairlink.label("nas", url) == "nas ({})".format(url)


def test_label_drops_control_characters_from_the_daemons_name():
    url = "http://192.168.1.50:8080"
    hostile = "na\x1b[2Js\r\n\x07\x9b31m büro"
    assert pairlink.label(hostile, url) == "na[2Js31m büro ({})".format(url)


# ---------------------------------------------------------------------------
# notes about the credential
# ---------------------------------------------------------------------------


def test_notes_are_empty_for_a_scoped_phone_token():
    assert pairlink.notes(PHONE) == []


def test_notes_flag_an_all_scopes_token():
    assert [short for short, _ in pairlink.notes(ADMIN)] == [
        "full-access token"
    ]


def test_notes_flag_an_unauthenticated_connection():
    [(short, sentence)] = pairlink.notes(OPEN)
    assert short == "no access token"
    assert "the code carries none" in sentence


def test_notes_flag_a_connection_that_cannot_register_a_device():
    [(short, sentence)] = pairlink.notes(VIEWER)
    assert short == "no control scope"
    assert "POST /push/devices" in sentence
    assert [short for short, _ in pairlink.notes(ANONYMOUS)] == [
        "no access token",
        "no control scope",
    ]


def test_notes_tolerate_a_reply_that_is_not_the_expected_document():
    for whoami in (None, [], "x", {}):
        assert [short for short, _ in pairlink.notes(whoami)] == [
            "no access token"
        ]


# ---------------------------------------------------------------------------
# the pairing both terminal clients show
# ---------------------------------------------------------------------------


def test_pairing_assembles_the_payload_the_link_and_the_notes():
    url = "http://192.168.1.50:8080"
    whoami = dict(ADMIN, pairLinkBase="https://relay.example.test/pair")
    built = pairlink.pairing(whoami, url, "s3cr3t", name="nas")
    text = pairlink.payload("nas", url, "s3cr3t")
    assert built == pairlink.Pairing(
        "nas",
        url,
        text,
        pairlink.link(text, "https://relay.example.test/pair"),
        pairlink.hint("s3cr3t"),
        pairlink.notes(whoami),
    )


def test_pairing_names_the_server_from_the_cluster_reply():
    url = "http://192.168.1.50:8080"
    clustered = {"enabled": True, "node_name": "node-a"}
    assert (
        pairlink.pairing(PHONE, url, "t", cluster=clustered).name == "node-a"
    )
    assert pairlink.pairing(PHONE, url, "t").name == "192.168.1.50:8080"
    # a given name wins
    named = pairlink.pairing(PHONE, url, "t", name="nas", cluster=clustered)
    assert named.name == "nas"


def test_pairing_leaves_out_a_token_the_daemon_ignored():
    built = pairlink.pairing(OPEN, "http://192.168.1.50:8080", "stray")
    assert json.loads(built.payload)["token"] == ""
    assert built.link.startswith(pairlink.PAIR_LINK_FALLBACK + "#")
    assert [short for short, _ in built.notes] == ["no access token"]
    # and the caption does not claim a token that the code lacks
    assert built.hint == pairlink.hint(None)


def test_hint_warns_that_the_code_carries_the_token():
    scan, warning = pairlink.hint("s3cr3t")
    assert "Scan QR" in scan
    assert "access token" in warning and "HTTPS" in warning


def test_hint_names_the_apps_control_by_its_label():
    # "Scan QR code" labels the app's button and its Add a server menu item
    [scan] = pairlink.hint(None)
    assert scan == (
        "Scan the code with the phone's camera, or tap Scan QR code in the "
        "app."
    )


@pytest.mark.parametrize("token", [None, ""])
def test_hint_for_a_code_without_a_token_says_only_how_to_scan(token):
    [scan] = pairlink.hint(token)
    assert scan == pairlink.hint("s3cr3t")[0]
