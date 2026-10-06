"""Tests for the pairing address check (cronstable.pairprobe).

``tests/test_paircli.py`` and ``tests/test_tui_pair.py`` cover the check
through each client. The tests here cover what the two clients share.
"""

import threading
from typing import get_args

import pytest

from cronstable import netutil, pairprobe, webclient
from tests.test_paircli import LOOPBACK, PHONE, VPN, _Daemon, _guarded


@pytest.mark.parametrize("needs", get_args(pairprobe.Needs))
@pytest.mark.parametrize("client", get_args(pairprobe.Client))
def test_every_need_has_advice_for_every_client(needs, client):
    advice = pairprobe.Unreachable("no address.", needs).advice(client)
    assert advice.endswith(".")
    assert "--public-url" in advice
    # the terminal dashboard has no such flag, so it names the command
    assert ("cronstable pair --public-url URL" in advice) == (client == "tui")


def test_unreachable_defaults_to_needing_an_address():
    failure = pairprobe.Unreachable("no address.")
    assert (str(failure), failure.needs) == ("no address.", "address")
    assert failure.advice("pair").startswith("Add a LAN or VPN address")


@pytest.fixture
def stalled_lookup(monkeypatch):
    """A host with no default route, whose resolver never answers."""
    answered = threading.Event()
    monkeypatch.setattr(netutil, "lan_address", lambda: answered.wait(30))
    yield
    answered.set()


def test_lookup_that_stalls_counts_as_no_lan_address(
    monkeypatch, stalled_lookup
):
    monkeypatch.setattr(pairprobe, "_PROBE_TIMEOUT", 0.05)
    with pytest.raises(pairprobe.Unreachable) as caught:
        pairprobe.phone_url(LOOPBACK, PHONE)
    assert str(caught.value) == (
        "{} is a loopback address, which a phone cannot reach.".format(
            LOOPBACK
        )
    )


def test_lookup_that_stalls_takes_no_time_from_the_address_check(
    monkeypatch, stalled_lookup
):
    # a listener on a VPN address needs no LAN address to be dialed
    whoami = dict(PHONE, listeners=["http://127.0.0.1:8080", VPN])
    daemon = _Daemon({VPN + "/whoami": _guarded(whoami)})
    monkeypatch.setattr(webclient, "OPENER", daemon)
    monkeypatch.setattr(pairprobe, "_PROBE_TIMEOUT", 0.5)
    assert pairprobe.phone_url(LOOPBACK, whoami) == VPN


def test_lookup_that_fails_is_raised_to_the_caller(monkeypatch):
    def lookup():
        raise UnicodeError("label too long")

    monkeypatch.setattr(netutil, "lan_address", lookup)
    with pytest.raises(UnicodeError, match="label too long"):
        pairprobe.phone_url(LOOPBACK, PHONE)
