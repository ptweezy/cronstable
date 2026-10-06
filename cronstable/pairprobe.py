"""The address that a pairing code names for a daemon reached on loopback.

``cronstable pair`` and the terminal dashboard's Pair a device panel reach
the daemon at the operator's ``--url``. A phone cannot dial a loopback
address, so :func:`phone_url` finds another address of this host where the
same daemon answers. :exc:`Unreachable` says why there is none, and what
each client tells the operator to do about it.

This module imports the standard library, :mod:`cronstable._cliargs`,
:mod:`cronstable.netutil`, :mod:`cronstable.pairlink`, and
:mod:`cronstable.webclient`.
"""

import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from typing import TYPE_CHECKING, Any, Literal, TypeVar
from urllib.parse import urlsplit

from cronstable import _cliargs, netutil, pairlink, webclient

if TYPE_CHECKING:  # pragma: no cover - annotations only
    import ssl

_T = TypeVar("_T")

# Seconds allowed for each step in checking whether the daemon also answers
# on another address of this host: the lookup of the LAN address, and then
# the requests to the candidate addresses.
_PROBE_TIMEOUT = 3.0

#: What the operator has to supply before a phone can reach the daemon.
Needs = Literal["address", "certificate", "token"]
#: The clients that show an :exc:`Unreachable`: ``cronstable pair`` and the
#: terminal dashboard.
Client = Literal["pair", "tui"]

# What each client tells the operator to do for each ``Needs``.
_ADVICE: dict[Needs, dict[Client, str]] = {
    "address": {
        "pair": "Add a LAN or VPN address to web.listen, or pass "
        "--public-url with the address the phone uses.",
        "tui": "Start the terminal dashboard with --url set to the server's "
        "LAN, VPN, or public address, or run cronstable pair --public-url "
        "URL.",
    },
    "certificate": {
        "pair": "Pass --public-url with an address that the listener's "
        "certificate names.",
        "tui": "Start the terminal dashboard with --url set to an address "
        "that the listener's certificate names, or run cronstable pair "
        "--public-url URL.",
    },
    "token": {
        "pair": "Pass --token-env VAR or set {} to present one, or pass "
        "--public-url with the address the phone uses.".format(
            _cliargs.WEB_ENV_TOKEN
        ),
        "tui": "Select Set access token in the command palette, or run "
        "cronstable pair --public-url URL.",
    },
}


class Unreachable(Exception):
    """The daemon has no address that a phone can dial.

    The message says why, and :meth:`advice` says what to do about it.
    ``needs`` names what the operator has to supply: ``"address"``, an
    address that the phone can reach; ``"certificate"``, an address that
    the listener's certificate names; or ``"token"``, an access token,
    which the server asks for before it names its listeners.
    """

    def __init__(self, message: str, needs: Needs = "address") -> None:
        super().__init__(message)
        self.needs: Needs = needs

    def advice(self, client: Client) -> str:
        """What ``client`` tells the operator to do."""
        return _ADVICE[self.needs][client]


def phone_url(
    base: str,
    whoami: Any,
    context: "ssl.SSLContext | None" = None,
) -> str:
    """The address to put in the code for a daemon reached at ``base``.

    A loopback ``base`` is replaced with another address of this host when
    the daemon serves one: this host's LAN address, or the address of a
    listener bound to another address that a phone can dial. ``whoami``,
    the reply that ``base`` gave, must report a listener there, and a
    request to the address must come back with the instance ID that
    ``whoami`` reports. That request carries no token, so the token reaches
    no address but ``base``. :func:`cronstable.pairlink.dial_urls` gives
    the addresses in the order of preference.

    ``context`` is the connection's TLS context. Raises :exc:`Unreachable`
    when a phone can reach none of the addresses.
    """
    if not netutil.is_loopback(base):
        return base
    loopback = "{} is a loopback address, which a phone cannot reach".format(
        base
    )
    if not pairlink.lists_listeners(whoami):
        if isinstance(whoami, dict) and whoami.get("label") == "anonymous":
            raise Unreachable(
                "{}, and the server names its listeners only to a "
                "connection that presents an access token.".format(loopback),
                needs="token",
            )
        raise Unreachable(
            "{}, and the server's reply names no listeners.".format(loopback)
        )
    address = _lan_address()
    candidates = pairlink.dial_urls(whoami, base, address)
    scheme = urlsplit(base).scheme
    if not candidates:
        if address is None:
            raise Unreachable(loopback + ".")
        # Whatever answers at the LAN address is then some other server,
        # so nothing is asked of it.
        raise Unreachable(
            "{}, and the server reports no {} listener on this host's LAN "
            "address ({}) or on another address that a phone can "
            "reach.".format(loopback, scheme, address)
        )
    if scheme == "https" and not webclient.verifies(context):
        # A phone verifies the certificate, so an unverified answer shows
        # nothing about an address.
        raise Unreachable(
            "{}, and this connection skips certificate verification, which "
            "leaves the listener's certificate unchecked for any other "
            "address.".format(loopback),
            needs="certificate",
        )
    return _answering(
        candidates, whoami, webclient.build_opener(context), loopback, address
    )


def _start(call: Callable[..., _T], *args: Any) -> "Future[_T]":
    """Run ``call`` on a thread that exit leaves behind.

    The future takes what the call returns or raises. The caller reads it
    with a time limit, so a call that never returns delays neither the
    result nor the exit.
    """
    future: Future[_T] = Future()

    def work() -> None:
        try:
            future.set_result(call(*args))
        except BaseException as ex:  # noqa: BLE001 - raised by the caller
            future.set_exception(ex)

    threading.Thread(target=work, daemon=True).start()
    return future


def _lan_address() -> str | None:
    """:func:`cronstable.netutil.lan_address`, or ``None`` when the lookup
    takes longer than ``_PROBE_TIMEOUT``.

    A host with no default route asks its resolver, which can stall.
    """
    try:
        return _start(netutil.lan_address).result(_PROBE_TIMEOUT)
    except FutureTimeout:
        return None


def _where(candidate: str, lan: str | None) -> str:
    """What a message calls the address that ``candidate`` names."""
    if urlsplit(candidate).hostname == lan:
        return "this host's LAN address"
    return "this host's address"


def _answering(
    candidates: list[str],
    whoami: Any,
    opener: Any,
    loopback: str,
    lan: str | None,
) -> str:
    """The first of ``candidates`` where the daemon that sent ``whoami``
    answers.

    Every candidate is checked at once, and the wait for all of them ends
    after ``_PROBE_TIMEOUT``. Raises the :exc:`Unreachable` of the first
    candidate when none answers.
    """
    checks = [
        _start(
            _check, candidate, whoami, opener, loopback, _where(candidate, lan)
        )
        for candidate in candidates
    ]
    deadline = time.monotonic() + _PROBE_TIMEOUT
    failures = []
    for candidate, check in zip(candidates, checks, strict=True):
        try:
            check.result(max(0.0, deadline - time.monotonic()))
        except FutureTimeout:
            failures.append(
                Unreachable(
                    "{}, and the check of {} failed: no reply from {} within "
                    "{:g} seconds.".format(
                        loopback,
                        _where(candidate, lan),
                        candidate,
                        _PROBE_TIMEOUT,
                    )
                )
            )
        except Unreachable as failure:
            failures.append(failure)
        else:
            return candidate
    # The failure at the first address asked.
    raise failures[0]


def _check(
    candidate: str, whoami: Any, opener: Any, loopback: str, where: str
) -> None:
    """Check that the daemon that sent ``whoami`` answers at ``candidate``.

    The request carries no token, and the check reads the reply's headers
    alone. Raises :exc:`Unreachable` with a message that opens with
    ``loopback`` and calls the address ``where``.
    """
    try:
        status, headers, _body = webclient.send(
            candidate + "/whoami",
            opener,
            _PROBE_TIMEOUT,
            "the cronstable server at {}".format(candidate),
            "/whoami",
            headers={"Accept": "application/json"},
            limit=0,
        )
    except webclient.ClientError as ex:
        # The detail leaves out the advice about --cacert and --insecure,
        # which changes nothing for the phone.
        raise Unreachable(
            "{}, and the check of {} failed: {}.".format(
                loopback, where, ex.detail
            ),
            needs="certificate"
            if isinstance(ex, webclient.TLSError)
            else "address",
        ) from None
    # Only a cronstable daemon sends the header.
    answered = headers.get(pairlink.INSTANCE_HEADER)
    if not answered:
        raise Unreachable(
            "{}, and another server answers at {}: {}/whoami returned HTTP "
            "{}.".format(loopback, where, candidate, status)
        )
    if answered != pairlink.instance(whoami):
        raise Unreachable(
            "{}, and a different cronstable server answers at {} ({}).".format(
                loopback, where, candidate
            )
        )
