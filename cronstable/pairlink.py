"""The mobile app's pairing link, for the clients that draw it in a terminal.

``cronstable pair`` and the terminal dashboard encode the link that the web
dashboard's Pair a device panel encodes: the relay's ``/pair`` route with
``{v, name, url, token}`` as base64url JSON in the fragment (see "Pairing
links" in ``docs/relay-protocol.md``). The app reads the payload and then
calls the web API at ``url``, so ``url`` must be an address the phone can
reach.

This module imports the standard library and :mod:`cronstable.netutil`.
"""

import base64
import json
import re
from collections.abc import Iterator
from typing import Any, NamedTuple
from urllib.parse import SplitResult, quote, urlsplit, urlunsplit

from cronstable import netutil

#: The hosted relay's landing route: the link's base while the daemon has
#: no push section, and when ``GET /whoami`` names no ``pairLinkBase``.
PAIR_LINK_FALLBACK = "https://relay.cronstable.com/pair"

#: The header that carries the daemon's instance ID on every reply that the
#: web API serves, with or without a token. ``GET /whoami`` reports the same
#: ID as ``instance``.
INSTANCE_HEADER = "Cronstable-Instance"

# The characters that http.client refuses in a host.
_UNSAFE_HOST = re.compile(r"[\x00-\x20\x7f]")
# Path characters that RFC 3986 allows as written, and "%" so that an
# escape stays one.
_PATH_SAFE = "/%:@!$&'()*+,;=-._~"
# The port of a URL that names none.
_DEFAULT_PORTS = {"http": 80, "https": 443}


def base_url(url: str) -> str:
    """``url`` reduced to what the app dials: scheme, host, port, and path.

    Credentials, query, and fragment are dropped, and the path is
    percent-encoded. Raises :exc:`ValueError` unless the URL is ``http://``
    or ``https://`` with a host that a request can name.
    """
    try:
        parts = urlsplit(url.strip())
        netloc = parts.netloc.rsplit("@", 1)[-1]
        valid = parts.scheme in ("http", "https") and bool(parts.hostname)
        # Reading the port is what range-checks it.
        valid = valid and (parts.port is None or parts.port > 0)
        valid = valid and not _UNSAFE_HOST.search(netloc)
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("{!r} is not an http:// or https:// URL".format(url))
    path = quote(parts.path.rstrip("/"), safe=_PATH_SAFE)
    return urlunsplit((parts.scheme, netloc, path, "", ""))


def with_host(url: str, host: str) -> str:
    """``url`` with its host replaced, keeping scheme, port, and path."""
    parts = urlsplit(url)
    return urlunsplit(
        (parts.scheme, netutil.netloc(host, parts.port), parts.path, "", "")
    )


def instance(whoami: Any) -> str | None:
    """The daemon's instance ID from a ``GET /whoami`` reply, or ``None``."""
    found = whoami.get("instance") if isinstance(whoami, dict) else None
    return found if isinstance(found, str) and found else None


def _port(parts: SplitResult) -> int | None:
    """The port of a parsed URL: the one it names, or its scheme's."""
    return parts.port or _DEFAULT_PORTS.get(parts.scheme)


def lists_listeners(whoami: Any) -> bool:
    """Whether a ``GET /whoami`` reply names the daemon's listeners.

    The reply to a connection that the daemon serves without a token under
    ``web.anonymousScopes`` leaves them out.
    """
    return isinstance(whoami, dict) and isinstance(
        whoami.get("listeners"), list
    )


def _listeners(whoami: Any) -> Iterator[tuple[str, netutil.Address, int]]:
    """The listeners in a ``GET /whoami`` reply that a request can name.

    ``listeners`` holds the addresses of the daemon's bound TCP sockets.
    Each one comes back as its scheme, the address that it binds, and its
    port. An address to dial is built from those parts, so no other text
    of the reply reaches a request or a message.
    """
    if not lists_listeners(whoami):
        return
    for listener in whoami["listeners"]:
        try:
            parts = urlsplit(listener)
            bound = netutil.ip_literal(parts.hostname or "")
            # Reading the port is what range-checks it.
            port = _port(parts) if parts.port != 0 else None
        except (AttributeError, TypeError, ValueError):
            continue
        if parts.scheme in _DEFAULT_PORTS and bound is not None and port:
            yield parts.scheme, bound, port


def _serves(bound: netutil.Address, host: netutil.Address | None) -> bool:
    """Whether a socket bound on ``bound`` serves the address ``host``.

    A socket on the unspecified address serves every address of its own
    family. The daemon's IPv6 sockets take no IPv4 connection. ``host`` is
    ``None`` for a name, which can resolve in either family.
    """
    if bound.is_unspecified:
        return host is None or bound.version == host.version
    return bound == host


def bound_elsewhere(whoami: Any, url: str) -> bool:
    """Whether the daemon binds ``url``'s port on other hosts alone.

    The daemon then serves no request at ``url``, so whatever answers
    there is another process. A port that no listener binds is a forwarded
    one, such as a published container port, and the result is false.
    """
    target = urlsplit(url)
    host, port = netutil.ip_literal(target.hostname or ""), _port(target)
    serving = [
        _serves(bound, host)
        for _scheme, bound, bound_port in _listeners(whoami)
        if bound_port == port
    ]
    return bool(serving) and not any(serving)


def dial_urls(whoami: Any, base: str, lan: str | None) -> list[str]:
    """The addresses to check for a daemon reached at a loopback ``base``.

    ``lan`` is :func:`cronstable.netutil.lan_address`. Each listener in the
    ``GET /whoami`` reply with the scheme of ``base`` gives one address:
    ``lan`` for a listener that serves it, and the listener's own address
    for one bound to an address that another host can dial. A loopback or
    link-local address gives none. The addresses on ``lan`` come first.

    When a listener gives an address, the port of ``base`` on ``lan`` goes
    ahead of them all, because a published container port differs from the
    one the daemon binds. It is left out when the daemon binds that port on
    other addresses alone, so a process that holds it on ``lan`` is asked
    nothing.
    """
    scheme = urlsplit(base).scheme
    on_lan: list[str] = []
    elsewhere: list[str] = []
    for found, bound, port in _listeners(whoami):
        if found != scheme:
            continue
        if lan is not None and _serves(bound, netutil.ip_literal(lan)):
            on_lan.append("{}://{}".format(scheme, netutil.netloc(lan, port)))
        elif not (
            bound.is_unspecified or bound.is_loopback or bound.is_link_local
        ):
            own = netutil.netloc(str(bound), port)
            elsewhere.append("{}://{}".format(scheme, own))
    if lan is not None and (on_lan or elsewhere):
        named = with_host(base, lan)
        if not bound_elsewhere(whoami, named):
            on_lan.insert(0, named)
    return list(dict.fromkeys(on_lan + elsewhere))


def lan_note(base: str, url: str) -> str | None:
    """What to tell the operator when ``url`` stands in for a loopback
    ``base``, or ``None`` when the code names ``base`` itself."""
    if url == base:
        return None
    return "The code names this host's address {} in place of {}.".format(
        url, base
    )


def default_name(url: str, cluster: Any) -> str:
    """The server name the app shows: the cluster node name, or the
    address's host and port."""
    if isinstance(cluster, dict) and cluster.get("enabled"):
        node = cluster.get("node_name")
        if isinstance(node, str) and node:
            return node
    return urlsplit(url).netloc


def link_base(whoami: Any) -> str:
    """The pairing link's base from a ``GET /whoami`` reply.

    The base is the server's text and the link goes to a terminal, so a
    base that is not a printable ``http://`` or ``https://`` URL raises
    :exc:`ValueError`.
    """
    base = whoami.get("pairLinkBase") if isinstance(whoami, dict) else None
    if not isinstance(base, str) or not base:
        return PAIR_LINK_FALLBACK
    try:
        valid = urlsplit(base).scheme in _DEFAULT_PORTS and base.isprintable()
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(
            "the server's pairing link base {!a} is not a printable http:// "
            "or https:// URL".format(base)
        )
    return base


def accepted_token(whoami: Any, token: str | None) -> str | None:
    """``token`` when the daemon authenticated this connection with it.

    A daemon that requires no token ignores one, so a token meant for
    another server stays out of the code.
    """
    if isinstance(whoami, dict) and whoami.get("authenticated") is True:
        return token
    return None


def payload(name: str, url: str, token: str | None) -> str:
    """The pairing JSON: the text that the dashboard writes, with each
    character that :meth:`str.isprintable` rejects as its JSON escape.

    The text goes to a terminal and the name comes from the daemon. The
    escape parses to the same character, so the app reads the same
    pairing. Raises :exc:`ValueError` for a name or an address that UTF-8
    cannot encode, such as a command-line argument with a byte outside
    UTF-8.
    """
    for what, value in (("server name", name), ("address", url)):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError(
                "the {} {!a} holds a character that UTF-8 cannot "
                "encode".format(what, value)
            ) from None
    text = json.dumps(
        {"v": 1, "name": name, "url": url, "token": token or ""},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    # The call above leaves every character from U+007F up as it is. With
    # its defaults, json.dumps writes one as its escape inside quotes, and
    # as a surrogate pair above the BMP.
    return "".join(
        ch if ch.isprintable() else json.dumps(ch)[1:-1] for ch in text
    )


def link(pairing_json: str, base: str) -> str:
    """The pairing link: ``base`` with the JSON in its fragment."""
    encoded = base64.urlsafe_b64encode(pairing_json.encode("utf-8"))
    return base + "#" + encoded.decode("ascii").rstrip("=")


def hint(token: str | None) -> list[str]:
    """What the terminal clients print beside a code that carries
    ``token``."""
    lines = [
        "Scan the code with the phone's camera, or tap Scan QR code in the "
        "app."
    ]
    if token:
        lines.append(
            "The code contains the access token, so pair over HTTPS or a "
            "trusted network."
        )
    return lines


def label(name: str, url: str) -> str:
    """One line naming the server a code pairs with.

    The name comes from the daemon, so control characters are dropped
    before it reaches a terminal.
    """
    if name == urlsplit(url).netloc:
        return url
    shown = "".join(ch for ch in name if ch.isprintable())
    return "{} ({})".format(shown, url)


def notes(whoami: Any) -> list[tuple[str, str]]:
    """What to tell the operator about the credential in the code.

    Each note is a short label and the full sentence, from a
    ``GET /whoami`` reply.
    """
    who = whoami if isinstance(whoami, dict) else {}
    out = []
    if who.get("authenticated") is not True:
        out.append(
            (
                "no access token",
                "The server authenticated no access token, so the code "
                "carries none and the app connects without one.",
            )
        )
    elif who.get("allScopes") is True:
        out.append(
            (
                "full-access token",
                "This token grants full access. To limit phone access, "
                "configure a token with fewer permissions in "
                "web.authTokens.",
            )
        )
    scopes = who.get("scopes")
    if isinstance(scopes, list) and "control" not in scopes:
        out.append(
            (
                "no control scope",
                "Registering a device (POST /push/devices) requires the "
                "control scope, which this connection lacks. The app can "
                "read this server but can't register for push alerts.",
            )
        )
    return out


class Pairing(NamedTuple):
    """What a terminal client shows for one server."""

    name: str
    url: str
    payload: str
    link: str
    hint: list[str]
    notes: list[tuple[str, str]]


def pairing(
    whoami: Any,
    url: str,
    token: str | None,
    name: str | None = None,
    cluster: Any = None,
) -> Pairing:
    """The pairing for a daemon that the phone dials at ``url``.

    ``whoami`` is the daemon's ``GET /whoami`` reply to ``token``. ``name``
    defaults to :func:`default_name` for the ``GET /cluster`` reply. Raises
    :exc:`ValueError` as :func:`payload` and :func:`link_base` do.
    """
    name = name or default_name(url, cluster)
    carried = accepted_token(whoami, token)
    text = payload(name, url, carried)
    return Pairing(
        name,
        url,
        text,
        link(text, link_base(whoami)),
        hint(carried),
        notes(whoami),
    )
