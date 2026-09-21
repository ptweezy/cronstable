"""Test the Kubernetes library transport with a fake client.

``_K8sLibraryTransport`` imports ``kubernetes`` lazily. Install the fake from
``tests/_fake_kubernetes_client.py`` in ``sys.modules`` to exercise the
transport with compatible model objects, ``ApiException``, and
``ConfigException``.

The fake client shares the fake API server's Lease store. The final test
elects one leader between nodes using the library and HTTP transports.
``tests/test_backend_live.py`` tests the real client against a real API
server when both are available.
"""

import asyncio
import datetime
import json
import logging
import os

import pytest

from cronstable.backends import kubernetes as kubernetes_backend
from cronstable.backends.kubernetes import (
    KubernetesBackend,
    _K8sHttpTransport,
    _K8sLibraryTransport,
    parse_lease,
)
from cronstable.config import ConfigError, parse_config_string
from tests import _fake_kubernetes_client as fake_client
from tests._fake_kube_apiserver import FakeKubeApiserver, LeaseStore


def _backend(extra="", *, node="node-a", namespace="ns", library="library"):
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: " + node + "\n"
        "  connectTimeout: 4\n"
        "  kubernetes:\n"
        "    leaseName: yl\n"
        "    clientLibrary: " + library + "\n"
        "    leaseDurationSeconds: 15\n"
        "    renewDeadlineSeconds: 10\n"
        "    retryPeriodSeconds: 2\n"
    )
    if namespace:
        yaml += "    leaseNamespace: " + namespace + "\n"
    cfg = parse_config_string(yaml + extra, "").cluster_config
    return KubernetesBackend(cfg, lambda: "v1:job")


def _lease_body(holder="node-a#1", resource_version=None):
    metadata = {"name": "yl", "namespace": "ns"}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": metadata,
        "spec": {
            "holderIdentity": holder,
            "leaseDurationSeconds": 15,
            "renewTime": "2026-01-01T12:00:00.250000Z",
            "leaseTransitions": 0,
        },
    }


# --- choosing a transport -------------------------------------------------


def test_the_import_probe_follows_the_package(monkeypatch):
    fake_client.install(monkeypatch)
    assert _backend()._native_available() is True
    fake_client.uninstall(monkeypatch)
    assert _backend()._native_available() is False


@pytest.mark.parametrize(
    "setting, installed, expected",
    [
        ("auto", True, _K8sLibraryTransport),
        ("auto", False, _K8sHttpTransport),
        ("library", True, _K8sLibraryTransport),
        ("http", True, _K8sHttpTransport),
        ("http", False, _K8sHttpTransport),
    ],
)
async def test_start_selects_the_transport(
    monkeypatch, tmp_path, setting, installed, expected
):
    if installed:
        fake_client.install(monkeypatch)
    else:
        fake_client.uninstall(monkeypatch)
    async with FakeKubeApiserver() as api:
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [
                        {
                            "name": "ctx",
                            "context": {"cluster": "c", "user": "u"},
                        }
                    ],
                    "clusters": [
                        {"name": "c", "cluster": {"server": api.url}}
                    ],
                }
            )
        )
        b = _backend(
            "    kubeconfig: '{}'\n".format(
                str(kubeconfig).replace("\\", "/")
            ),
            library=setting,
        )
        await b.start()
        try:
            assert type(b._transport) is expected
            assert b.is_leader()
        finally:
            await b.stop()


async def test_requiring_an_absent_library_is_a_config_error(monkeypatch):
    fake_client.uninstall(monkeypatch)
    b = _backend()
    with pytest.raises(ConfigError, match="clientLibrary is 'library'"):
        await b.start()
    assert b._transport is None


# --- loading client configuration -----------------------------------------


async def test_setup_in_cluster(monkeypatch, tmp_path):
    fake = fake_client.install(monkeypatch)
    (tmp_path / "namespace").write_text("pod-ns\n")
    monkeypatch.setattr(kubernetes_backend, "_SA_DIR", str(tmp_path))
    b = _backend(namespace=None)
    t = _K8sLibraryTransport(b)
    await t.setup()
    try:
        assert fake.names() == ["load_incluster_config"]
        assert b.namespace == "pod-ns"
        assert t._api_client.configuration.host == fake.loaded_host
        assert t._api.api_client is t._api_client
        # the projected CA is tracked for rotation
        assert list(b._tls_signature) == [os.path.join(tmp_path, "ca.crt")]
        # the blocking loader ran on the transport's own pool
        assert all(n.startswith("cronstable-k8s-lease") for n in fake.threads)
    finally:
        await t.close()


@pytest.mark.parametrize(
    "configured, active, expected",
    [
        (None, {"context": {"namespace": "team-b"}}, "team-b"),
        ("ns", {"context": {"namespace": "team-b"}}, "ns"),
        (None, {"context": {}}, "default"),
        (None, {"context": None}, "default"),
        (None, None, "default"),
    ],
)
async def test_setup_from_a_kubeconfig(
    monkeypatch, tmp_path, configured, active, expected
):
    fake = fake_client.install(monkeypatch)
    fake.active_context = active
    # a pod-mounted namespace file is never consulted on the kubeconfig path
    (tmp_path / "namespace").write_text("from-the-pod")
    monkeypatch.setattr(kubernetes_backend, "_SA_DIR", str(tmp_path))
    kubeconfig = tmp_path / "kubeconfig"
    client_key = tmp_path / "client.key"
    kubeconfig.write_text(
        json.dumps(
            {
                "current-context": "ctx",
                "contexts": [
                    {"name": "ctx", "context": {"cluster": "c", "user": "u"}}
                ],
                "clusters": [
                    {
                        "name": "c",
                        "cluster": {
                            "server": "https://h",
                            "certificate-authority": "pki/ca.crt",
                        },
                    }
                ],
                "users": [
                    {
                        "name": "u",
                        "user": {"client-key": client_key.as_posix()},
                    }
                ],
            }
        )
    )
    b = _backend(
        "    kubeconfig: '{}'\n".format(str(kubeconfig).replace("\\", "/")),
        namespace=configured,
    )
    t = _K8sLibraryTransport(b)
    await t.setup()
    try:
        assert fake.calls[:2] == [
            ("load_kube_config", (), {"config_file": b.kubeconfig}),
            ("list_kube_config_contexts", (), {"config_file": b.kubeconfig}),
        ]
        assert "load_incluster_config" not in fake.names()
        assert b.namespace == expected
        # the kubeconfig and the files it references, resolved the way the
        # client resolves them, are tracked for rotation
        assert list(b._tls_signature) == [
            b.kubeconfig,
            os.path.normpath(
                os.path.join(os.path.dirname(b.kubeconfig), "pki/ca.crt")
            ),
            str(client_key),
        ]
    finally:
        await t.close()


@pytest.mark.parametrize("source", ["kubeconfig", "incluster"])
async def test_a_loader_failure_is_a_config_error(
    monkeypatch, tmp_path, source
):
    fake = fake_client.install(monkeypatch)
    error = fake_client.ConfigException("Invalid kube-config file.")
    extra = ""
    if source == "kubeconfig":
        fake.kube_config_error = error
        extra = "    kubeconfig: /nonexistent/kubeconfig\n"
    else:
        fake.incluster_error = error
    b = _backend(extra)
    with pytest.raises(ConfigError, match="could not load client config"):
        await b.start()
    # the half-started transport (and its thread pool) was torn down
    assert b._transport is None
    assert fake.api_clients == []


async def test_a_loader_bug_is_not_disguised_as_a_config_error(monkeypatch):
    fake = fake_client.install(monkeypatch)
    fake.incluster_error = RuntimeError("boom")
    t = _K8sLibraryTransport(_backend())
    with pytest.raises(RuntimeError):
        await t.setup()
    await t.close()


async def test_the_api_server_override_applies_to_the_client(monkeypatch):
    fake_client.install(monkeypatch)
    b = _backend("    apiServer: https://vip.example:6443/\n")
    t = _K8sLibraryTransport(b)
    await t.setup()
    try:
        assert t._api_client.configuration.host == "https://vip.example:6443"
    finally:
        await t.close()


@pytest.mark.parametrize("verify_ssl", [True, False])
async def test_disabled_tls_verification_is_warned_about(
    monkeypatch, caplog, verify_ssl
):
    caplog.set_level(logging.WARNING, "cronstable.backends.kubernetes")
    fake = fake_client.install(monkeypatch)
    fake.loaded_verify_ssl = verify_ssl
    t = _K8sLibraryTransport(_backend())
    await t.setup()
    await t.close()
    assert ("NOT verified" in caplog.text) is (not verify_ssl)


# --- observe / write ------------------------------------------------------


async def _ready(monkeypatch, store=None, **kwargs):
    fake = fake_client.install(monkeypatch, store)
    transport = _K8sLibraryTransport(_backend(**kwargs))
    await transport.setup()
    return fake, transport


async def test_observe_turns_the_model_into_the_wire_dict(monkeypatch):
    fake, t = await _ready(monkeypatch)
    try:
        assert await t.observe() is None
        body = _lease_body()
        body["metadata"]["annotations"] = {"k": "v"}
        assert await t.write(body, create=True) is True
        seen = await t.observe()
    finally:
        await t.close()
    # camelCase keys, as the REST transport returns them
    assert seen["metadata"]["resourceVersion"] == "1001"
    assert seen["metadata"]["annotations"] == {"k": "v"}
    assert seen["spec"]["holderIdentity"] == "node-a#1"
    assert seen["spec"]["leaseDurationSeconds"] == 15
    # None fields are dropped rather than rendered as null
    assert "acquireTime" not in seen["spec"]
    assert "labels" not in seen["metadata"]
    # a datetime renders with isoformat(): an explicit offset, where the
    # apiserver itself writes Z. The shared parser reads both.
    assert seen["spec"]["renewTime"] == "2026-01-01T12:00:00.250000+00:00"
    state = parse_lease(seen)
    assert state.renew_time == datetime.datetime(
        2026, 1, 1, 12, 0, 0, 250000, tzinfo=datetime.timezone.utc
    )
    assert state.acquire_time is None
    assert state.resource_version == "1001"
    assert state.annotations == {"k": "v"}
    # every call is bounded by the renew deadline, off the event loop
    for name, _args, kwargs in fake.calls:
        if name.endswith("_namespaced_lease"):
            assert kwargs == {"_request_timeout": 10}
    assert fake.names().count("read_namespaced_lease") == 2


async def test_write_arguments_and_lost_races(monkeypatch):
    fake, t = await _ready(monkeypatch)
    try:
        body = _lease_body()
        assert await t.write(body, create=True) is True
        assert fake.calls[-1] == (
            "create_namespaced_lease",
            ("ns", body),
            {"_request_timeout": 10},
        )
        # AlreadyExists
        assert await t.write(_lease_body("node-b#2"), create=True) is False
        current = (await t.observe())["metadata"]["resourceVersion"]
        renew = _lease_body("node-a#1", current)
        assert await t.write(renew, create=False) is True
        assert fake.calls[-1] == (
            "replace_namespaced_lease",
            ("yl", "ns", renew),
            {"_request_timeout": 10},
        )
        # Conflict on the stale resourceVersion
        assert await t.write(renew, create=False) is False
    finally:
        await t.close()


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500, 503])
async def test_write_raises_every_status_but_409(monkeypatch, status):
    fake, t = await _ready(monkeypatch)
    try:
        for create in (True, False):
            fake.fail_with = [status]
            with pytest.raises(fake_client.ApiException) as err:
                await t.write(_lease_body(), create=create)
            assert err.value.status == status
        fake.fail_with = [409, 409]
        assert await t.write(_lease_body(), create=True) is False
        assert await t.write(_lease_body(), create=False) is False
    finally:
        await t.close()


@pytest.mark.parametrize("status", [400, 401, 403, 409, 500, 503])
async def test_observe_raises_every_status_but_404(monkeypatch, status):
    fake, t = await _ready(monkeypatch)
    try:
        fake.fail_with = [status]
        with pytest.raises(fake_client.ApiException) as err:
            await t.observe()
        assert err.value.status == status
        fake.fail_with = [404]
        assert await t.observe() is None
    finally:
        await t.close()


async def test_replacing_a_deleted_lease_recreates_it(monkeypatch):
    # Replacing a deleted Lease creates it again. The live backend tests
    # verify this behavior against a real API server. A write with a stale
    # resourceVersion then returns 409 Conflict.
    _fake, t = await _ready(monkeypatch)
    try:
        assert await t.write(_lease_body("node-a#1", "1"), create=False)
        seen = await t.observe()
        assert seen["spec"]["holderIdentity"] == "node-a#1"
        assert not await t.write(_lease_body("node-b#1", "1"), create=False)
    finally:
        await t.close()


async def test_a_non_api_error_propagates(monkeypatch):
    _fake, t = await _ready(monkeypatch)

    def boom(*args, **kwargs):
        raise OSError("connection reset")

    t._api.read_namespaced_lease = boom
    t._api.create_namespaced_lease = boom
    try:
        with pytest.raises(OSError):
            await t.observe()
        with pytest.raises(OSError):
            await t.write(_lease_body(), create=True)
    finally:
        await t.close()


# --- close ----------------------------------------------------------------


async def test_close_releases_the_client_and_the_pool(monkeypatch):
    fake, t = await _ready(monkeypatch)
    [api_client] = fake.api_clients
    pool = t._pool
    await t.close()
    assert api_client.closed
    assert t._api_client is None and t._pool is None
    assert pool._shutdown
    # closing again, and closing a transport that never set up, is harmless
    await t.close()
    await _K8sLibraryTransport(_backend()).close()


async def test_close_releases_the_pool_even_when_the_client_close_fails(
    monkeypatch,
):
    _fake, t = await _ready(monkeypatch)

    def boom():
        raise RuntimeError("close failed")

    t._api_client.close = boom
    pool = t._pool
    with pytest.raises(RuntimeError):
        await t.close()
    assert t._pool is None
    assert pool._shutdown


async def test_a_wedged_call_does_not_block_the_next_round(monkeypatch):
    # Two workers: a read wedged in the client occupies one, and the next
    # read still runs on the other.
    import threading

    fake, t = await _ready(monkeypatch)
    wedged = threading.Event()
    release = threading.Event()
    real_read = t._api.read_namespaced_lease
    calls = []

    def read(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            wedged.set()
            release.wait(30)
        return real_read(*args, **kwargs)

    t._api.read_namespaced_lease = read
    try:
        await t.write(_lease_body(), create=True)
        stuck = asyncio.ensure_future(t.observe())
        await asyncio.get_running_loop().run_in_executor(None, wedged.wait, 30)
        seen = await asyncio.wait_for(t.observe(), 30)
        assert seen["spec"]["holderIdentity"] == "node-a#1"
        assert not stuck.done()
    finally:
        release.set()
        await stuck
        await t.close()


# --- end to end -----------------------------------------------------------


async def test_election_through_the_library_transport(monkeypatch):
    fake = fake_client.install(monkeypatch)
    a = _backend(node="node-a")
    b = _backend(node="node-b")
    await a.start()
    await b.start()
    try:
        assert type(a._transport) is _K8sLibraryTransport
        assert a.is_leader() and not b.is_leader()
        assert b.leader_name() == "node-a"
        for _ in range(2):
            await b._renew_once()
            await a._renew_once()
            assert [a.is_leader(), b.is_leader()] == [True, False]
        await a.mark_reboot_ran("oneshot")
        await a.stop()
        lease = fake.store.objects[("ns", "yl")]
        assert lease["spec"].get("holderIdentity") is None
        await b._renew_once()
        assert b.is_leader()
        assert b.reboot_ran("oneshot") is True
        assert lease is not fake.store.objects[("ns", "yl")]
        assert (
            fake.store.objects[("ns", "yl")]["spec"]["leaseTransitions"] == 1
        )
    finally:
        await a.stop()
        await b.stop()
    assert all(client.closed for client in fake.api_clients)


async def test_mixed_transports_share_one_lease(monkeypatch, tmp_path):
    # One node on the official client, one on the HTTP transport, one store:
    # they read each other's writes (Z and +00:00 timestamps alike) and
    # still elect a single leader.
    store = LeaseStore()
    fake_client.install(monkeypatch, store)
    async with FakeKubeApiserver(store) as api:
        kubeconfig = tmp_path / "kubeconfig"
        kubeconfig.write_text(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [
                        {
                            "name": "ctx",
                            "context": {"cluster": "c", "user": "u"},
                        }
                    ],
                    "clusters": [
                        {"name": "c", "cluster": {"server": api.url}}
                    ],
                }
            )
        )
        extra = "    kubeconfig: '{}'\n".format(
            str(kubeconfig).replace("\\", "/")
        )
        native = _backend(extra, node="native", library="library")
        rest = _backend(extra, node="rest", library="http")
        await native.start()
        await rest.start()
        try:
            assert native.is_leader() and not rest.is_leader()
            assert rest.leader_name() == "native"
            await rest._renew_once()
            await native._renew_once()
            assert native.is_leader() and not rest.is_leader()
            await native.stop()
            await rest._renew_once()
            assert rest.is_leader()
            # and back again
            await native.start()
            assert not native.is_leader()
            assert native.leader_name() == "rest"
        finally:
            await native.stop()
            await rest.stop()
