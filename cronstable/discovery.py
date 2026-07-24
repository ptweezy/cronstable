"""Zero-config LAN discovery: the opt-in Bonjour/mDNS advert.

With ``web.bonjour`` enabled, the daemon advertises its web control API
as a ``_cronstable._tcp`` service on the local network, so a companion
app (or ``dns-sd -B _cronstable._tcp``) finds it without a typed URL.
The advert carries no secrets: instance name, port, scheme and version
only; a client still needs a bearer token to read anything.

python-zeroconf is an optional extra (``pip install
"cronstable[discovery]"``); the import is guarded, and config validation
refuses ``web.bonjour`` when the library is absent.  Unlike push (an
alerting channel that must fail closed), a *runtime* advert failure is
logged and swallowed: discovery is a convenience, and an mDNS hiccup
must never take down a scheduler.
"""

import asyncio
import logging
import re
import socket
from typing import Any, Dict, Optional

try:
    from zeroconf import ServiceInfo
    from zeroconf.asyncio import AsyncZeroconf

    HAVE_ZEROCONF = True
except ImportError:  # pragma: no cover - exercised on the bare baseline
    HAVE_ZEROCONF = False

logger = logging.getLogger("cronstable")

SERVICE_TYPE = "_cronstable._tcp.local."

#: Bound the register/unregister round-trips: mDNS involves the network,
#: and the callers (config apply, shutdown) must never hang on it.
_MDNS_OP_TIMEOUT = 10.0

#: Bound the address probe: the gethostbyname fallback can hang on a
#: broken resolver, and it runs every housekeeping pass.
_ADDR_TIMEOUT = 5.0


def primary_address() -> Optional[str]:
    """This host's primary outbound IPv4 address, or ``None``.

    The connected-UDP trick: connecting a datagram socket selects the
    route (and thus the source address) without sending a packet.  The
    target is TEST-NET-1, never actually reached.  Falls back to the
    hostname's A record, then gives up (the caller skips the advert
    with a warning rather than advertising loopback).
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("192.0.2.1", 9))
        address = str(probe.getsockname()[0])
    except OSError:
        address = None
    finally:
        probe.close()
    if address and not address.startswith("127."):
        return address
    try:
        address = socket.gethostbyname(socket.gethostname())
    except OSError:
        return None
    if address.startswith("127."):
        return None
    return address


def _instance_name(name: str) -> str:
    """A safe mDNS instance label: dots would split the service name.

    DNS labels cap at 63 BYTES, not characters; a multibyte (e.g.
    Japanese) hostname truncated by characters can still exceed the
    limit and make zeroconf raise ``BadTypeInNameException``.  Truncate
    the UTF-8 encoding and drop any codepoint the cut tore in half.
    """
    cleaned = name.replace(".", "-").strip("-")
    cleaned = cleaned.encode("utf-8")[:63].decode("utf-8", "ignore")
    return cleaned or "cronstable"


#: Appended to the advert's SRV target hostname so it can never equal the
#: machine's own ``<hostname>.local.``; see :func:`_server_name`.
_SERVER_SUFFIX = "-cronstable"


def _server_name(name: str) -> str:
    """The advert's SRV target hostname label, distinct and LDH-safe.

    Deliberately NOT the host's own ``<hostname>.local.``: that name
    already has an owner on most machines (avahi on Linux desktops and
    servers, mDNSResponder on macOS), defended as a unique record set
    with the host's full address list.  A second responder claiming the
    same name with one route-derived IPv4 is the RFC 6762 section 10.2
    conflict between non-cooperating responders, and avahi resolves it
    by renaming the whole machine (``hostname-2.local``), breaking every
    unrelated ``.local`` consumer.  A dedicated label keeps this
    daemon's A record in a namespace nothing else defends.

    Unlike the instance label (a user-visible display name, where
    spaces are fine and common), a hostname must survive strict
    letters-digits-hyphen resolvers, so everything else becomes a
    hyphen.  The sanitized result is pure ASCII, so the 63-byte label
    cap is a plain character slice here.
    """
    cleaned = re.sub(r"[^A-Za-z0-9-]+", "-", name).strip("-")
    cleaned = cleaned[: 63 - len(_SERVER_SUFFIX)].rstrip("-")
    return (cleaned + _SERVER_SUFFIX).lstrip("-")


class BonjourAdvertiser:
    """Owns the registered mDNS service across config reloads.

    ``start_stop`` is the whole lifecycle, mirroring the daemon's other
    ``start_stop_*`` edges: call it with the desired advert (or ``None``
    to stop) on every config apply; it re-registers only when something
    the advert carries actually changed.
    """

    def __init__(self) -> None:
        self._zeroconf: Optional[Any] = None
        self._info: Optional[Any] = None
        self._signature: Optional[Dict[str, Any]] = None

    @property
    def active(self) -> bool:
        return self._info is not None

    async def start_stop(self, advert: Optional[Dict[str, Any]]) -> None:
        """Converge the running advert onto ``advert``.

        ``advert`` is ``{"name", "port", "properties"}`` plus an
        optional ``"address"`` (built by the caller from the web config
        and its bound listeners) or ``None`` for off.  Never raises: a
        network/mDNS failure logs and leaves the advert off until the
        next config apply retries it.

        The convergence signature includes the resolved address, so a
        DHCP lease change or a Wi-Fi hop re-registers the advert on the
        next housekeeping pass instead of advertising a dead IP until
        the daemon restarts.
        """
        if advert is None:
            self._signature = None
            await self._unregister()
            return
        if not HAVE_ZEROCONF:  # pragma: no cover - config validation gates
            self._signature = None
            logger.error(
                "bonjour: python-zeroconf is not installed; not advertising"
            )
            return
        # An advert may carry the address to publish (the caller knows it
        # when the advertised listener is bound to one specific IP; the
        # outbound-route probe could name a different interface).  Only a
        # wildcard-bound listener leaves it to the probe.
        address = advert.get("address") or await self._resolve_address()
        if address is None:
            self._signature = None
            await self._unregister()
            logger.warning(
                "bonjour: could not determine a non-loopback address to "
                "advertise; skipping the advert until the next reload"
            )
            return
        signature = dict(advert, address=address)
        if signature == self._signature and self._info is not None:
            return
        await self._unregister()
        self._signature = signature
        zeroconf = None
        try:
            instance = _instance_name(advert["name"])
            properties = {
                key: str(value)
                for key, value in (advert.get("properties") or {}).items()
            }
            # ServiceInfo construction stays inside the try: zeroconf
            # validates the name eagerly (BadTypeInNameException), and
            # this method's never-raises contract covers that too.
            info = ServiceInfo(
                SERVICE_TYPE,
                "{}.{}".format(instance, SERVICE_TYPE),
                addresses=[socket.inet_aton(address)],
                port=int(advert["port"]),
                properties=properties,
                server="{}.local.".format(_server_name(advert["name"])),
            )
            zeroconf = AsyncZeroconf()
            await asyncio.wait_for(
                zeroconf.async_register_service(info),
                timeout=_MDNS_OP_TIMEOUT,
            )
        except Exception as exc:
            logger.error(
                "bonjour: failed to register the %s advert: %s",
                SERVICE_TYPE,
                exc,
            )
            self._signature = None
            if zeroconf is not None:
                # Close the half-built instance: leaking one per
                # housekeeping pass exhausts fds within a day.
                try:
                    await asyncio.wait_for(
                        zeroconf.async_close(), timeout=_MDNS_OP_TIMEOUT
                    )
                except Exception as close_exc:
                    logger.warning(
                        "bonjour: close after failed register failed: %s",
                        close_exc,
                    )
            return
        self._zeroconf = zeroconf
        self._info = info
        logger.info(
            "bonjour: advertising %r on %s:%d",
            instance,
            address,
            int(advert["port"]),
        )

    async def _resolve_address(self) -> Optional[str]:
        """:func:`primary_address` off-loop, bounded.

        The gethostbyname fallback inside it is a blocking resolver
        call; on a broken DNS setup it can stall for seconds, and this
        runs every housekeeping pass, so it must never run on the event
        loop.  A timeout counts as "no address" for this pass.
        """
        loop = asyncio.get_running_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, primary_address),
                timeout=_ADDR_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None

    async def _unregister(self) -> None:
        zeroconf, info = self._zeroconf, self._info
        self._zeroconf = None
        self._info = None
        if zeroconf is None:
            return
        try:
            if info is not None:
                await asyncio.wait_for(
                    zeroconf.async_unregister_service(info),
                    timeout=_MDNS_OP_TIMEOUT,
                )
        except Exception as exc:
            logger.warning("bonjour: unregister failed: %s", exc)
        try:
            await asyncio.wait_for(
                zeroconf.async_close(), timeout=_MDNS_OP_TIMEOUT
            )
        except Exception as exc:
            logger.warning("bonjour: close failed: %s", exc)

    async def stop(self) -> None:
        """Tear the advert down (shutdown path)."""
        self._signature = None
        await self._unregister()
