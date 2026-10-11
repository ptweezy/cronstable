"""Host addresses, as the daemon and its clients read and write them.

Configuration checks, the web listeners, the Bonjour advert, and the pairing
clients read a literal host through :func:`ip_literal` and write a URL's
host and port through :func:`netloc`.

This module imports only the standard library.
"""

import ipaddress
import re
import socket
from urllib.parse import urlsplit

Address = ipaddress.IPv4Address | ipaddress.IPv6Address

# The characters of an IPv4 address in any form the socket layer reads.
_IPV4_FORM = re.compile(r"[0-9A-Fa-fXx.]+")


def ip_literal(host: str) -> Address | None:
    """``host`` as an IP address, or ``None`` when it is a name.

    The one reader of a literal host for the daemon and its clients. It
    reads an address with or without a URL's brackets, and the short and
    hexadecimal IPv4 forms that this host's socket layer reads, such as
    ``127.1`` and ``0x7f.0.0.1``. It looks up no name.

    A form that the host's C library reads as two addresses is a name: in
    ``0177.0.0.1``, a leading zero is octal to one call and decimal to
    another. A form that the host's ``getaddrinfo`` looks up is a name
    too: some C libraries read an address there only in the dotted quad.
    """
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        pass
    if not _IPV4_FORM.fullmatch(host):
        # Some C libraries read an address with other text after it.
        return None
    try:
        address = ipaddress.ip_address(socket.inet_aton(host))
    except (OSError, ValueError):
        return None
    try:
        # A bind or a connect asks inet_pton first. Some C libraries read
        # a leading zero as decimal there and as octal in inet_aton.
        packed = socket.inet_pton(socket.AF_INET, host)
    except OSError:
        packed = None
    if packed is not None:
        return address if packed == address.packed else None
    try:
        # A bind or a connect asks getaddrinfo next. Some C libraries
        # read an address there, and others look the text up as a name.
        # With AI_NUMERICHOST, the call raises in place of a lookup.
        infos = socket.getaddrinfo(
            host,
            None,
            socket.AF_INET,
            socket.SOCK_STREAM,
            flags=socket.AI_NUMERICHOST,
        )
    except (OSError, UnicodeError):
        # The idna codec refuses a label of more than 63 characters.
        return None
    read = {info[4][0] for info in infos}
    return address if read == {str(address)} else None


def netloc(host: str, port: int | str | None = None) -> str:
    """``host`` and ``port`` as a URL writes them, an IPv6 address in
    brackets. A host that already has its brackets keeps them."""
    bare = ":" in host and not host.startswith("[")
    text = "[{}]".format(host) if bare else host
    return text if port is None else "{}:{}".format(text, port)


def is_loopback(url: str) -> bool:
    """Whether ``url`` names only this machine, which no other host can
    dial.

    That is ``localhost``, a name under it, or a loopback or unspecified
    address in any form the socket layer reads. Any other name resolves on
    the host that dials it, so it counts as reachable.
    """
    host = (urlsplit(url).hostname or "").rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    address = ip_literal(host)
    if address is None:
        return False
    address = getattr(address, "ipv4_mapped", None) or address
    return address.is_loopback or address.is_unspecified


def lan_address() -> str | None:
    """This host's IPv4 address on its default route, or ``None``.

    Connecting a UDP socket picks the route's source address and sends
    nothing. The target is a documentation address (RFC 5737). A host
    with no default route gets the address of its hostname, from a
    resolver call that can block.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.0.2.1", 9))
            address = str(sock.getsockname()[0])
    except OSError:
        address = None
    if address is None or is_loopback("http://{}".format(address)):
        try:
            address = socket.gethostbyname(socket.gethostname())
        except (OSError, UnicodeError):
            # The idna codec refuses some hostnames before any lookup.
            return None
    if is_loopback("http://{}".format(address)):
        return None
    return address
