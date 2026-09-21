"""Test the etcd backend's HTTP transport against a fake gateway.

These tests exercise ``_post`` over a socket to ``tests/_fake_etcd.py``.
They cover endpoint failover and rotation, per-endpoint timeouts,
reauthentication after a 401 response, plaintext credential protection,
redirects, and invalid response bodies. Election scenarios also run through
the transport.

``tests/test_backend_etcd.py`` tests the lifecycle with ``_post`` replaced.
``tests/test_backend_live.py`` runs election scenarios against a real etcd
server when one is configured.
"""

import asyncio
import contextlib

import aiohttp
import pytest

from cronstable.backends.etcd import EtcdBackend
from cronstable.config import parse_config_string
from cronstable.leadership import decode_reboot_ran
from tests._fake_conformance import check_etcd_gateway
from tests._fake_etcd import EtcdStore, FakeEtcd, b64, unb64
from tests._fake_http import DEAD_ENDPOINT, FakeClock, server_ssl_context
from tests._helpers import _wait_until, _write_tls

ELECTION = "cronstable/leader"
ELECTION_KEY = ELECTION.encode()
REBOOT_KEY = (ELECTION + "/reboot-ran").encode()


def _backend(endpoints, *, node="node-a", ttl=15, extra="", timeout=4):
    yaml = (
        "cluster:\n"
        "  backend: etcd\n"
        "  nodeName: " + node + "\n"
        "  connectTimeout: " + str(timeout) + "\n"
        "  etcd:\n"
        "    endpoints:\n"
        + "".join("      - " + e + "\n" for e in endpoints)
        + "    electionName: "
        + ELECTION
        + "\n    ttl: "
        + str(ttl)
        + "\n"
        + extra
    )
    cfg = parse_config_string(yaml, "").cluster_config
    return EtcdBackend(cfg, lambda: "v1:job")


def _tls_extra(material, *, client_cert=True, auth=False):
    extra = "    tls:\n      ca: " + material["ca"].replace("\\", "/") + "\n"
    if client_cert:
        extra += (
            "      cert: " + material["cert"].replace("\\", "/") + "\n"
            "      key: " + material["key"].replace("\\", "/") + "\n"
        )
    if auth:
        extra += "    username: root\n    password:\n      value: s3cret\n"
    return extra


@contextlib.asynccontextmanager
async def _session(backend):
    """Open the backend's session and TLS context without a renew loop."""
    backend._ssl = backend._build_ssl()
    backend._session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=backend.connect_timeout)
    )
    try:
        yield backend
    finally:
        await backend._session.close()
        backend._session = None


RANGE_BODY = {"key": b64(ELECTION_KEY)}


# --- _post: the request it sends ------------------------------------------


async def test_post_sends_json_and_returns_the_object():
    async with FakeEtcd() as etcd:
        etcd.store.put(ELECTION_KEY, b"node-z")
        async with _session(_backend([etcd.endpoint + "/"])) as b:
            resp = await b._post("/v3/kv/range", RANGE_BODY)
    assert unb64(resp["kvs"][0]["value"]) == b"node-z"
    # every int64 crosses the wire as a string
    assert resp["kvs"][0]["mod_revision"] == "2"
    assert resp["count"] == "1"
    [req] = etcd.requests
    # the endpoint's trailing slash does not double up in the path
    assert (req.method, req.path) == ("POST", "/v3/kv/range")
    assert req.json == RANGE_BODY
    assert req.headers["Content-Type"] == "application/json"
    # no credentials configured: no Authorization header
    assert "Authorization" not in req.headers


# --- _post: failover and rotation -----------------------------------------


async def test_post_fails_over_from_a_refused_endpoint():
    async with FakeEtcd() as etcd:
        b = _backend([DEAD_ENDPOINT, etcd.endpoint])
        async with _session(b):
            resp = await b._post("/v3/kv/range", RANGE_BODY)
    assert "header" in resp
    assert etcd.paths() == ["/v3/kv/range"]


async def test_post_fails_over_from_an_endpoint_that_hangs():
    # A half-open member accepts the request and never answers: the
    # per-endpoint timeout (not the wider session default) abandons it and
    # the next member serves the call.
    store = EtcdStore()
    async with FakeEtcd(store) as stuck, FakeEtcd(store) as healthy:
        stuck.inject(hang=True)
        b = _backend([stuck.endpoint, healthy.endpoint])
        assert b.request_timeout < b.connect_timeout
        loop = asyncio.get_running_loop()
        async with _session(b):
            began = loop.time()
            resp = await b._post("/v3/kv/range", RANGE_BODY)
            # abandoned at request_timeout; the session-wide connectTimeout
            # would have held the call for at least connect_timeout
            assert loop.time() - began < b.connect_timeout
    assert "header" in resp
    assert stuck.paths() == ["/v3/kv/range"]
    assert healthy.paths() == ["/v3/kv/range"]


async def test_post_times_out_when_the_only_endpoint_hangs():
    async with FakeEtcd() as stuck:
        stuck.inject(hang=True)
        b = _backend([stuck.endpoint], ttl=3, timeout=1)
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="all etcd") as err:
                await b._post("/v3/kv/range", RANGE_BODY)
    assert stuck.endpoint in str(err.value)
    assert "TimeoutError" in str(err.value)


async def test_post_rotates_the_probe_order_by_round():
    store = EtcdStore()
    async with FakeEtcd(store) as first, FakeEtcd(store) as second:
        b = _backend([first.endpoint, second.endpoint])
        async with _session(b):
            await b._post("/v3/kv/range", RANGE_BODY)
            assert (len(first.requests), len(second.requests)) == (1, 0)
            b._endpoint_offset = 1
            await b._post("/v3/kv/range", RANGE_BODY)
            assert (len(first.requests), len(second.requests)) == (1, 1)
            # the offset wraps: round 2 of a two-member list probes the
            # first member again
            b._endpoint_offset = 2
            await b._post("/v3/kv/range", RANGE_BODY)
            assert (len(first.requests), len(second.requests)) == (2, 1)


async def test_post_raises_when_every_endpoint_is_down():
    b = _backend([DEAD_ENDPOINT, DEAD_ENDPOINT + "/"])
    async with _session(b):
        with pytest.raises(aiohttp.ClientError) as err:
            await b._post("/v3/kv/range", RANGE_BODY)
    # the message names the last endpoint's failure
    assert "all etcd endpoints failed" in str(err.value)
    assert "127.0.0.1" in str(err.value)


# --- _post: statuses and bodies that are not etcd's answer ----------------


@pytest.mark.parametrize(
    "fault",
    [
        pytest.param({"status": 500, "body": b"{}"}, id="500"),
        pytest.param({"status": 503, "body": b"busy"}, id="503"),
        pytest.param({"status": 404, "body": b"{}"}, id="404"),
        pytest.param({"status": 204}, id="204-no-body"),
        pytest.param({"body": b"[1, 2]"}, id="json-list"),
        pytest.param({"body": b"null"}, id="json-null"),
        pytest.param({"body": b'"text"'}, id="json-scalar"),
        pytest.param({"body": b"{not json"}, id="invalid-json"),
        pytest.param({"body": b""}, id="empty-body"),
        pytest.param({"body": b"\xff\xfe\xfd"}, id="invalid-utf8"),
        pytest.param(
            {"body": b"<html></html>", "content_type": "text/html"},
            id="html",
        ),
    ],
)
async def test_post_treats_a_bad_answer_as_a_failed_endpoint(fault):
    # Each of these fails over to the healthy member, and on its own surfaces
    # as the ClientError logged for a failed round (never an
    # AttributeError / ValueError escaping the network catch tuples).
    store = EtcdStore()
    async with FakeEtcd(store) as bad, FakeEtcd(store) as healthy:
        bad.inject(times=2, **fault)
        b = _backend([bad.endpoint, healthy.endpoint])
        async with _session(b):
            resp = await b._post("/v3/kv/range", RANGE_BODY)
        assert "header" in resp
        assert len(healthy.requests) == 1
        alone = _backend([bad.endpoint])
        async with _session(alone):
            with pytest.raises(aiohttp.ClientError, match="all etcd"):
                await alone._post("/v3/kv/range", RANGE_BODY)


@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_post_never_follows_a_redirect(status):
    # The redirect target is a working gateway, and the redirect even carries
    # a well-formed JSON object: the target is never contacted and the body
    # is never taken for etcd's answer.
    async with FakeEtcd() as target, FakeEtcd() as redirector:
        redirector.inject(
            status=status,
            body=b'{"kvs": []}',
            headers={"Location": target.endpoint + "/v3/kv/range"},
        )
        b = _backend([redirector.endpoint])
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="unexpected etcd"):
                await b._post("/v3/kv/range", RANGE_BODY)
    assert target.requests == []
    assert len(redirector.requests) == 1


# --- _post: credentials ---------------------------------------------------


async def test_post_refuses_plaintext_endpoints_when_credentials_are_set():
    # Config validation rejects auth over http://, so reach the defensive
    # filter the way a bug upstream would: credentials on a plaintext list.
    async with FakeEtcd() as etcd:
        b = _backend([etcd.endpoint])
        b.username, b.password = "root", "s3cret"
        b._auth_token = "would-leak"
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="all etcd"):
                await b._post("/v3/kv/range", RANGE_BODY)
            # a password alone is a credential too
            b.username = None
            with pytest.raises(aiohttp.ClientError, match="all etcd"):
                await b._authenticate()
    assert etcd.requests == []


async def test_post_without_credentials_does_not_reauthenticate_on_401():
    async with FakeEtcd() as etcd:
        etcd.inject(status=401, body=b"{}")
        b = _backend([etcd.endpoint])
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="401"):
                await b._post("/v3/kv/range", RANGE_BODY)
    assert etcd.paths() == ["/v3/kv/range"]


@contextlib.asynccontextmanager
async def _auth_cluster(tmp_path, members=1):
    """``members`` TLS gateways over one auth-enabled store."""
    material = _write_tls(tmp_path, cn="etcd-ca", suffix="member")
    store = EtcdStore()
    store.enable_auth("root", "s3cret")
    async with contextlib.AsyncExitStack() as stack:
        servers = [
            await stack.enter_async_context(
                FakeEtcd(store, ssl_context=server_ssl_context(material))
            )
            for _ in range(members)
        ]
        yield material, store, servers


def _https(server):
    # the leaf's SANs cover localhost and the loopback IPs
    return "https://localhost:{}".format(server.port)


async def test_post_reauthenticates_exactly_once_on_401(tmp_path):
    async with _auth_cluster(tmp_path) as (material, store, [etcd]):
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        async with _session(b):
            b._auth_token = await b._authenticate()
            first_token = b._auth_token
            await b._post("/v3/kv/range", RANGE_BODY)
            # the token TTL lapses server-side
            store.expire_tokens()
            resp = await b._post("/v3/kv/range", RANGE_BODY)
    assert "header" in resp
    assert b._auth_token != first_token
    assert etcd.paths() == [
        "/v3/auth/authenticate",
        "/v3/kv/range",
        "/v3/kv/range",  # 401: stale token
        "/v3/auth/authenticate",
        "/v3/kv/range",  # retried once with the fresh token
    ]
    assert etcd.requests[2].headers["Authorization"] == first_token
    assert etcd.requests[4].headers["Authorization"] == b._auth_token
    # the password travels only to the authenticate call
    assert etcd.requests[3].json == {"name": "root", "password": "s3cret"}
    assert "Authorization" not in etcd.requests[0].headers


async def test_post_persistent_401_surfaces_after_one_refresh(tmp_path):
    # Two members, both answering 401 whatever the token: one refresh, one
    # retry across the members, then an ordinary failed round. The stale
    # token is not retried at another member to trigger a second refresh.
    async with _auth_cluster(tmp_path, members=2) as (material, _store, srv):
        for server in srv:
            server.inject(path="/v3/kv/range", status=401, times=10)
        b = _backend(
            [_https(s) for s in srv],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="401"):
                await b._post("/v3/kv/range", RANGE_BODY)
    paths = srv[0].paths() + srv[1].paths()
    assert paths.count("/v3/auth/authenticate") == 1
    assert paths.count("/v3/kv/range") == 3


async def test_authenticate_is_never_reauthenticated(tmp_path):
    async with _auth_cluster(tmp_path) as (material, _store, [etcd]):
        etcd.inject(path="/v3/auth/authenticate", status=401, times=10)
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="401"):
                await b._authenticate()
    assert etcd.paths() == ["/v3/auth/authenticate"]


async def test_reauthentication_failure_fails_the_call(tmp_path):
    async with _auth_cluster(tmp_path) as (material, store, [etcd]):
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        async with _session(b):
            b._auth_token = await b._authenticate()
            store.expire_tokens()
            store.users["root"] = "rotated"
            with pytest.raises(aiohttp.ClientError, match="400"):
                await b._post("/v3/kv/range", RANGE_BODY)
    assert etcd.paths()[-1] == "/v3/auth/authenticate"


async def test_credentials_only_reach_https_members_of_a_mixed_list(tmp_path):
    async with _auth_cluster(tmp_path) as (material, _store, [secure]):
        async with FakeEtcd() as plaintext:
            b = _backend(
                [_https(secure)],
                extra=_tls_extra(material, client_cert=False, auth=True),
            )
            b.endpoints = [plaintext.endpoint, _https(secure)]
            async with _session(b):
                b._auth_token = await b._authenticate()
                await b._post("/v3/kv/range", RANGE_BODY)
    assert plaintext.requests == []
    assert len(secure.requests) == 2


# --- end to end: one node -------------------------------------------------


async def test_start_wins_the_election_and_stop_revokes_the_lease():
    async with FakeEtcd() as etcd:
        b = _backend([etcd.endpoint])
        await b.start()
        try:
            assert b.is_leader() and b.is_quorate()
            assert b.leader_name() == "node-a"
            kv = etcd.store.kvs[ELECTION_KEY]
            assert kv.value == b"node-a"
            # the key is bound to the lease the backend was granted
            assert str(kv.lease) == b.lease_detail()["leaseId"]
            assert etcd.paths() == [
                "/v3/lease/grant",
                "/v3/kv/txn",
                "/v3/kv/range",  # the @reboot-ran read-back
            ]
        finally:
            await b.stop()
        # revoking the lease deleted the key at once
        assert ELECTION_KEY not in etcd.store.kvs
        assert etcd.store.leases == {}
        assert etcd.paths()[-1] == "/v3/lease/revoke"
        assert not b.is_leader()


async def test_steady_state_renews_with_a_keepalive_and_a_read():
    async with FakeEtcd() as etcd:
        b = _backend([etcd.endpoint])
        async with _session(b):
            await b._renew_once()
            revision = etcd.store.revision
            del etcd.requests[:]
            await b._renew_once()
            # a keepalive and a plain range: no txn, so no raft write
            assert etcd.paths() == ["/v3/lease/keepalive", "/v3/kv/range"]
            assert etcd.store.revision == revision
            assert b.is_leader()


async def test_a_hand_deleted_key_is_won_back_through_the_txn():
    async with FakeEtcd() as etcd:
        b = _backend([etcd.endpoint])
        async with _session(b):
            await b._renew_once()
            first_create = etcd.store.kvs[ELECTION_KEY].create_revision
            etcd.store.delete_range(ELECTION_KEY)
            del etcd.requests[:]
            await b._renew_once()
            assert etcd.paths()[:3] == [
                "/v3/lease/keepalive",
                "/v3/kv/range",
                "/v3/kv/txn",
            ]
            assert b.is_leader()
            # revisions only move forward
            kv = etcd.store.kvs[ELECTION_KEY]
            assert kv.create_revision > first_create


async def test_the_renew_loop_recovers_a_deleted_key_on_its_own():
    async with FakeEtcd() as etcd:
        b = _backend([etcd.endpoint], ttl=3)
        await b.start()
        try:
            etcd.store.delete_range(ELECTION_KEY)
            await _wait_until(
                lambda: ELECTION_KEY in etcd.store.kvs, tries=1000
            )
        finally:
            await b.stop()


async def test_the_election_survives_a_dead_member():
    async with FakeEtcd() as etcd:
        b = _backend([DEAD_ENDPOINT, etcd.endpoint])
        await b.start()
        try:
            assert b.is_leader()
            # the next round rotates to probe the live member first
            await b._renew_once()
            assert b.is_leader()
        finally:
            await b.stop()


async def test_an_unreachable_cluster_leaves_the_node_not_quorate():
    b = _backend([DEAD_ENDPOINT])
    await b.start()
    try:
        assert not b.is_quorate()
        assert not b.is_leader()
        assert b.leader_name() is None
    finally:
        await b.stop()


async def test_a_server_shortened_ttl_narrows_the_fence():
    async with FakeEtcd() as etcd:
        etcd.store.grant_ttl_cap = 5
        b = _backend([etcd.endpoint])
        async with _session(b):
            await b._renew_once()
            assert b._effective_ttl == 5
            etcd.store.grant_ttl_cap = 4
            await b._renew_once()
            assert b._effective_ttl == 4
            assert b.is_leader()


@pytest.mark.parametrize("camel_case", [False, True])
async def test_the_election_reads_both_gateway_spellings(camel_case):
    async with FakeEtcd(camel_case=camel_case) as etcd:
        leader = _backend([etcd.endpoint])
        follower = _backend([etcd.endpoint], node="node-b")
        async with _session(leader), _session(follower):
            await leader._renew_once()
            await follower._renew_once()
            assert leader.is_leader() and not follower.is_leader()
            assert follower.leader_name() == "node-a"
            # the CAS against an EXISTING @reboot-ran key needs the key's
            # mod revision, whichever way the gateway spells the field
            await leader.mark_reboot_ran("first")
            await leader.mark_reboot_ran("second")
    _jsid, jobs = decode_reboot_ran(etcd.store.kvs[REBOOT_KEY].value.decode())
    assert jobs == {"first", "second"}


# --- end to end: two nodes ------------------------------------------------


async def test_two_nodes_elect_one_leader_and_hand_over_on_stop():
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint], node="node-a")
        b = _backend([etcd.endpoint], node="node-b")
        await a.start()
        await b.start()
        try:
            assert a.is_leader() and not b.is_leader()
            assert b.is_quorate()
            assert a.leader_name() == b.leader_name() == "node-a"
            # a follower's lease exists but backs no key
            assert len(etcd.store.leases) == 2
            first_term = etcd.store.kvs[ELECTION_KEY].create_revision
            for _ in range(3):
                await b._renew_once()
                await a._renew_once()
                assert [a.is_leader(), b.is_leader()] == [True, False]
            await a.stop()
            await b._renew_once()
            assert b.is_leader() and not a.is_leader()
            assert b.leader_name() == "node-b"
            # each term's key is created at a strictly later revision
            second_term = etcd.store.kvs[ELECTION_KEY].create_revision
            assert second_term > first_term
        finally:
            await a.stop()
            await b.stop()


async def test_concurrent_campaigns_elect_exactly_one_leader():
    async with FakeEtcd() as etcd:
        nodes = [
            _backend([etcd.endpoint], node="node-{}".format(i))
            for i in range(5)
        ]
        async with contextlib.AsyncExitStack() as stack:
            for node in nodes:
                await stack.enter_async_context(_session(node))
            await asyncio.gather(*(n._renew_once() for n in nodes))
            leaders = [n for n in nodes if n.is_leader()]
            assert len(leaders) == 1
            assert {n.leader_name() for n in nodes} == {leaders[0].identity}


async def test_duplicate_node_names_still_elect_one_leader():
    # The fence is the lease id bound to the key, never the identity string.
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint], node="twin")
        b = _backend([etcd.endpoint], node="twin")
        async with _session(a), _session(b):
            await a._renew_once()
            for _ in range(2):
                await b._renew_once()
            assert a.is_leader() and not b.is_leader()


async def test_leadership_moves_when_the_holders_lease_expires():
    clock = FakeClock()
    async with FakeEtcd(EtcdStore(clock)) as etcd:
        a = _backend([etcd.endpoint], node="node-a")
        b = _backend([etcd.endpoint], node="node-b")
        async with _session(a), _session(b):
            await a._renew_once()
            await b._renew_once()
            assert a.is_leader()
            first_term = etcd.store.kvs[ELECTION_KEY].create_revision
            # within the ttl a keepalive holds the lease
            clock.advance(10)
            await a._renew_once()
            clock.advance(10)
            await b._renew_once()
            assert a.is_leader() and not b.is_leader()
            # the holder stalls past its ttl: etcd expires the lease and
            # deletes the key, and the follower (whose own keepalive kept
            # its lease alive at +10s) wins the freed key
            clock.advance(10)
            await b._renew_once()
            assert b.is_leader()
            assert etcd.store.kvs[ELECTION_KEY].value == b"node-b"
            assert etcd.store.kvs[ELECTION_KEY].create_revision > first_term
            # the old holder's keepalive finds its lease gone; it re-grants,
            # loses the campaign and stands down
            old_lease = a._lease_id
            await a._renew_once()
            assert not a.is_leader()
            assert a._lease_id != old_lease
            assert a.leader_name() == "node-b"


async def test_a_known_lease_loss_fences_the_holder_closed_mid_round():
    clock = FakeClock()
    async with FakeEtcd(EtcdStore(clock)) as etcd:
        a = _backend([etcd.endpoint])
        async with _session(a):
            await a._renew_once()
            clock.advance(16)
            # the keepalive reports the loss; the re-grant then fails
            etcd.inject(path="/v3/lease/grant", status=503)
            with pytest.raises(aiohttp.ClientError):
                await a._renew_once()
            assert not a.is_leader()
            # the next healthy round wins the key back on a fresh lease
            await a._renew_once()
            assert a.is_leader()


# --- end to end: the @reboot-ran document ---------------------------------


async def test_reboot_ran_marks_survive_a_failover():
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint], node="node-a")
        b = _backend([etcd.endpoint], node="node-b")
        await a.start()
        await b.start()
        try:
            assert a.reboot_ran("oneshot") is False
            await a.mark_reboot_ran("oneshot")
            # the mark is in the store before the job would launch, on a key
            # that is bound to no lease
            assert etcd.store.kvs[REBOOT_KEY].lease == 0
            await a.stop()
            assert REBOOT_KEY in etcd.store.kvs
            await b._renew_once()
            assert b.is_leader()
            assert b.reboot_ran("oneshot") is True
            assert b.reboot_ran("another") is False
        finally:
            await a.stop()
            await b.stop()


async def test_concurrent_reboot_ran_writers_union_their_marks():
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint], node="node-a")
        b = _backend([etcd.endpoint], node="node-b")
        async with _session(a), _session(b):
            await asyncio.gather(
                a.mark_reboot_ran("from-a"), b.mark_reboot_ran("from-b")
            )
            await a._cas_write_reboot_ran()
    _jsid, jobs = decode_reboot_ran(etcd.store.kvs[REBOOT_KEY].value.decode())
    assert jobs == {"from-a", "from-b"}
    assert a.reboot_ran("from-b")


async def test_a_lost_cas_race_rereads_and_merges():
    # Another writer moves the key between this node's read and its write:
    # the guarded txn fails and the retry folds the other mark in.
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint])
        rival = _backend([etcd.endpoint], node="node-b")
        async with _session(a), _session(rival):
            await rival.mark_reboot_ran("rival")
            real_post = a._post
            raced = []

            async def post_with_a_race(path, body, **kwargs):
                if path == "/v3/kv/txn" and not raced:
                    raced.append(True)
                    await rival.mark_reboot_ran("rival-again")
                return await real_post(path, body, **kwargs)

            a._post = post_with_a_race
            await a.mark_reboot_ran("mine")
    _jsid, jobs = decode_reboot_ran(etcd.store.kvs[REBOOT_KEY].value.decode())
    assert jobs == {"rival", "rival-again", "mine"}


async def test_an_etcd_outage_defers_the_mark_and_a_later_round_persists():
    async with FakeEtcd() as etcd:
        a = _backend([etcd.endpoint])
        async with _session(a):
            await a._renew_once()
            etcd.inject(status=503, times=10)
            await a.mark_reboot_ran("oneshot")
            assert REBOOT_KEY not in etcd.store.kvs
            # the local mark still answers for this node
            assert a.reboot_ran("oneshot") is True
            del etcd._faults[:]
            await a._renew_once()
    _jsid, jobs = decode_reboot_ran(etcd.store.kvs[REBOOT_KEY].value.decode())
    assert jobs == {"oneshot"}


# --- end to end: TLS and auth ---------------------------------------------


async def test_the_election_runs_over_mutual_tls(tmp_path):
    material = _write_tls(tmp_path, cn="etcd-ca", suffix="member")
    ctx = server_ssl_context(material, require_client_cert=True)
    async with FakeEtcd(ssl_context=ctx) as etcd:
        b = _backend([_https(etcd)], extra=_tls_extra(material))
        await b.start()
        try:
            assert b.is_leader()
            assert b.tls_files_changed() is False
        finally:
            await b.stop()
        assert ELECTION_KEY not in etcd.store.kvs


async def test_a_member_demanding_a_client_cert_rejects_a_bare_client(
    tmp_path,
):
    material = _write_tls(tmp_path, cn="etcd-ca", suffix="member")
    ctx = server_ssl_context(material, require_client_cert=True)
    async with FakeEtcd(ssl_context=ctx) as etcd:
        b = _backend(
            [_https(etcd)], extra=_tls_extra(material, client_cert=False)
        )
        await b.start()
        try:
            assert not b.is_quorate()
        finally:
            await b.stop()
        assert etcd.store.kvs == {}


async def test_a_member_with_an_untrusted_certificate_is_never_spoken_to(
    tmp_path,
):
    served = _write_tls(tmp_path, cn="rogue-ca", suffix="member")
    trusted = _write_tls(tmp_path, cn="etcd-ca", suffix="member")
    async with FakeEtcd(ssl_context=server_ssl_context(served)) as etcd:
        b = _backend(
            [_https(etcd)], extra=_tls_extra(trusted, client_cert=False)
        )
        await b.start()
        try:
            assert not b.is_quorate()
            assert not b.is_leader()
        finally:
            await b.stop()
        # the handshake failed, so no request (and no lease) reached it
        assert etcd.requests == []


async def test_the_election_runs_with_auth_and_outlives_the_token(tmp_path):
    async with _auth_cluster(tmp_path) as (material, store, [etcd]):
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        await b.start()
        try:
            assert b.is_leader()
            assert etcd.paths()[0] == "/v3/auth/authenticate"
            # every later call carries the token, never the password
            for req in etcd.requests[1:]:
                assert req.headers["Authorization"] == b._auth_token
                assert b"s3cret" not in req.body
            store.expire_tokens()
            await b._renew_once()
            assert b.is_leader()
            # This renewal refreshes the token once. The concurrent loop can
            # also refresh it after receiving an expired-token response.
            assert etcd.paths().count("/v3/auth/authenticate") in (2, 3)
        finally:
            await b.stop()
        # the revoke carried the refreshed token too
        assert ELECTION_KEY not in store.kvs


async def test_a_wrong_password_fails_start_and_leaks_no_session(tmp_path):
    async with _auth_cluster(tmp_path) as (material, store, [etcd]):
        store.users["root"] = "something-else"
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        with pytest.raises(aiohttp.ClientError, match="400"):
            await b.start()
        assert b._session is None
        assert b._task is None
    assert store.leases == {}


async def test_a_request_without_a_token_is_rejected_when_auth_is_on(
    tmp_path,
):
    # etcd answers a token-less request with 400 (user name is empty), which
    # is not the 401 that triggers a refresh: the call fails plainly.
    async with _auth_cluster(tmp_path) as (material, _store, [etcd]):
        b = _backend(
            [_https(etcd)],
            extra=_tls_extra(material, client_cert=False, auth=True),
        )
        async with _session(b):
            with pytest.raises(aiohttp.ClientError, match="400"):
                await b._post("/v3/kv/range", RANGE_BODY)
    assert etcd.paths() == ["/v3/kv/range"]


# --- the fake itself ------------------------------------------------------


@pytest.mark.parametrize("camel_case", [False, True])
async def test_fake_gateway_passes_the_shared_conformance_checks(camel_case):
    # Run the same conformance checks as tests/test_backend_live.py.
    async with FakeEtcd(camel_case=camel_case) as etcd:
        async with aiohttp.ClientSession() as http:

            async def call(path, body):
                async with http.post(etcd.endpoint + path, json=body) as resp:
                    return resp.status, await resp.json()

            await check_etcd_gateway(call, "cronstable-conformance")
        assert etcd.store.kvs == {} and etcd.store.leases == {}


async def test_fake_gateway_semantics():
    # Test the store's HTTP behavior directly so an incorrect fake produces
    # a specific failure instead of an unrelated election failure.
    clock = FakeClock()
    async with FakeEtcd(EtcdStore(clock)) as etcd:
        async with aiohttp.ClientSession() as http:

            async def call(path, body, method="POST"):
                async with http.request(
                    method, etcd.endpoint + path, json=body
                ) as resp:
                    return resp.status, await resp.json()

            _s, grant = await call("/v3/lease/grant", {"TTL": 10})
            lease = grant["ID"]
            assert grant["TTL"] == "10" and int(lease) > 2**53
            put = {"key": b64(b"a/1"), "value": b64(b"v"), "lease": lease}
            assert (await call("/v3/kv/put", put))[0] == 200
            # re-putting a leased key with no lease detaches it
            await call("/v3/kv/put", {**put, "key": b64(b"b/1")})
            await call("/v3/kv/put", {"key": b64(b"b/1"), "value": b64(b"")})
            await call("/v3/kv/put", {"key": b64(b"a/2"), "value": b64(b"w")})
            await call("/v3/kv/put", {"key": b64(b"a/2"), "value": b64(b"x")})
            _s, got = await call(
                "/v3/kv/range", {"key": b64(b"a/"), "range_end": b64(b"a0")}
            )
            assert got["count"] == "2"
            assert got["kvs"][0]["lease"] == lease
            assert "lease" not in got["kvs"][1]
            assert got["kvs"][1]["version"] == "2"
            _s, everything = await call(
                "/v3/kv/range", {"key": b64(b"a"), "range_end": b64(b"\0")}
            )
            assert everything["count"] == "3"
            # a put bound to an unknown lease is refused
            bad = {"key": b64(b"z"), "value": b64(b""), "lease": "12345"}
            status, err = await call("/v3/kv/put", bad)
            assert (status, err["code"]) == (404, 5)
            # every compare target and result
            for cmp, expected in [
                ({"target": "VERSION", "version": "2"}, True),
                ({"target": "VALUE", "value": b64(b"x")}, True),
                ({"target": "LEASE", "lease": "0"}, True),
                ({"target": "MOD", "result": "GREATER", "modRevision": 1}, 1),
                (
                    {
                        "target": "CREATE",
                        "result": "LESS",
                        "create_revision": 1,
                    },
                    0,
                ),
                ({"target": "VERSION", "result": "NOT_EQUAL"}, True),
            ]:
                txn = {
                    "compare": [{"key": b64(b"a/2"), **cmp}],
                    "success": [{"request_range": {"key": b64(b"a/2")}}],
                    "failure": [
                        {"request_delete_range": {"key": b64(b"nope")}}
                    ],
                }
                _s, out = await call("/v3/kv/txn", txn)
                assert bool(out.get("succeeded")) is bool(expected), cmp
            status, _ = await call(
                "/v3/kv/txn",
                {"compare": [{"key": b64(b"a/2"), "result": "SIDEWAYS"}]},
            )
            assert status == 400
            status, _ = await call("/v3/kv/txn", {"success": [{"bogus": {}}]})
            assert status == 400
            # a keepalive for a live lease refreshes it; expiry deletes the
            # bound key and advances the revision
            clock.advance(8)
            _s, alive = await call("/v3/lease/keepalive", {"ID": lease})
            assert alive["result"]["TTL"] == "10"
            clock.advance(8)
            assert b"a/1" in etcd.store.kvs
            before = etcd.store.revision
            clock.advance(3)
            _s, gone = await call("/v3/lease/keepalive", {"ID": lease})
            assert "TTL" not in gone["result"]
            assert b"a/1" not in etcd.store.kvs
            assert b"b/1" in etcd.store.kvs
            assert etcd.store.revision == before + 1
            status, err = await call("/v3/lease/revoke", {"ID": lease})
            assert (status, err["code"]) == (404, 5)
            _s, deleted = await call(
                "/v3/kv/deleterange", {"key": b64(b"a/2")}
            )
            assert deleted["deleted"] == "1"
            _s, none = await call("/v3/kv/deleterange", {"key": b64(b"a/2")})
            assert "deleted" not in none
            assert (await call("/v3/nope", {}))[0] == 404
            assert (await call("/v3/kv/range", None, "GET"))[0] == 405
            status, err = await call(
                "/v3/auth/authenticate", {"name": "root", "password": "x"}
            )
            assert status == 400 and "not enabled" in err["error"]
