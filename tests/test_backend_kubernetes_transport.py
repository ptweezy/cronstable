"""Test the built-in Kubernetes HTTP transport against a fake API server.

Test kubeconfig parsing and in-cluster credential loading with parameterized
inputs. ``observe`` and ``write`` call ``tests/_fake_kube_apiserver.py`` over
a socket. The fake server stores Lease objects and checks concurrency with
``resourceVersion``. Election scenarios also run through this transport.

``tests/test_backend_kubernetes.py`` tests an in-memory transport.
``tests/test_backend_kubernetes_library.py`` tests the library transport.
``tests/test_backend_live.py`` runs election scenarios against a real API
server when one is configured.
"""

import asyncio
import base64
import contextlib
import json
import logging
import os
import ssl
from unittest.mock import AsyncMock

import aiohttp
import pytest

from cronstable.backends import kubernetes as kubernetes_backend
from cronstable.backends.kubernetes import (
    KubernetesBackend,
    _K8sHttpTransport,
    _kubeconfig_cert_files,
    _kubeconfig_file,
)
from cronstable.config import ConfigError, parse_config_string
from cronstable.leadership import REBOOT_RAN_KEY
from tests._fake_conformance import check_lease_api
from tests._fake_http import DEAD_ENDPOINT, FakeClock, server_ssl_context
from tests._fake_kube_apiserver import FakeKubeApiserver
from tests._helpers import _wait_until, _write_tls

LEASES = "/apis/coordination.k8s.io/v1/namespaces/ns/leases"


def _backend(extra="", *, node="node-a", namespace="ns", timeout=4):
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: " + node + "\n"
        "  connectTimeout: " + str(timeout) + "\n"
        "  kubernetes:\n"
        "    leaseName: yl\n"
        "    clientLibrary: http\n"
        "    leaseDurationSeconds: 15\n"
        "    renewDeadlineSeconds: 10\n"
        "    retryPeriodSeconds: 2\n"
    )
    if namespace:
        yaml += "    leaseNamespace: " + namespace + "\n"
    cfg = parse_config_string(yaml + extra, "").cluster_config
    return KubernetesBackend(cfg, lambda: "v1:job")


def _write_kubeconfig(
    path,
    *,
    server="https://10.0.0.5:6443",
    cluster=None,
    user=None,
    context=None,
):
    """A one-context kubeconfig (JSON, which every YAML parser reads)."""
    doc = {
        "apiVersion": "v1",
        "kind": "Config",
        "current-context": "ctx",
        "contexts": [
            {
                "name": "ctx",
                "context": {"cluster": "c", "user": "u", **(context or {})},
            }
        ],
        "clusters": [
            {"name": "c", "cluster": {"server": server, **(cluster or {})}}
        ],
        "users": [{"name": "u", "user": user if user is not None else {}}],
    }
    path.write_text(json.dumps(doc))
    return str(path)


def _kubeconfig_backend(kubeconfig, **kwargs):
    extra = "    kubeconfig: '{}'\n".format(kubeconfig.replace("\\", "/"))
    return _backend(extra, **kwargs)


def _loaded(tmp_path, **kubeconfig):
    """A transport whose ``_load_connection`` read this kubeconfig."""
    namespace = kubeconfig.pop("namespace", "ns")
    path = _write_kubeconfig(tmp_path / "kubeconfig", **kubeconfig)
    transport = _K8sHttpTransport(
        _kubeconfig_backend(path, namespace=namespace)
    )
    transport._load_connection()
    return transport


def _b64_file(path):
    with open(path, "rb") as handle:
        return base64.b64encode(handle.read()).decode("ascii")


# --- _load_kubeconfig: which context, cluster and user --------------------


def test_kubeconfig_current_context_selects_among_several(tmp_path):
    doc = {
        "current-context": "second",
        "contexts": [
            {"name": "first", "context": {"cluster": "c1", "user": "u1"}},
            {
                "name": "second",
                "context": {
                    "cluster": "c2",
                    "user": "u2",
                    "namespace": "team-b",
                },
            },
        ],
        "clusters": [
            {"name": "c1", "cluster": {"server": "https://one:6443"}},
            {"name": "c2", "cluster": {"server": "https://two:6443///"}},
        ],
        "users": [
            {"name": "u1", "user": {"token": "tok-1"}},
            {"name": "u2", "user": {"token": "tok-2"}},
        ],
    }
    path = tmp_path / "kubeconfig"
    path.write_text(json.dumps(doc))
    b = _kubeconfig_backend(str(path), namespace=None)
    t = _K8sHttpTransport(b)
    t._load_connection()
    assert t._base_url == "https://two:6443"
    assert t._auth_token == "tok-2"
    # a static token is never reread from disk
    assert t._token_path is None
    assert t._auth_headers() == {"Authorization": "Bearer tok-2"}
    # the context's namespace applies when none is configured
    assert b.namespace == "team-b"


@pytest.mark.parametrize(
    "configured, context, expected",
    [
        ("ns", "team-b", "ns"),
        (None, "team-b", "team-b"),
        (None, None, "default"),
    ],
)
def test_kubeconfig_namespace_precedence(
    tmp_path, monkeypatch, configured, context, expected
):
    # A pod-mounted namespace file is never consulted on the kubeconfig path.
    (tmp_path / "namespace").write_text("from-the-pod")
    monkeypatch.setattr(kubernetes_backend, "_SA_DIR", str(tmp_path))
    t = _loaded(
        tmp_path,
        namespace=configured,
        context={"namespace": context} if context else None,
        user={"token": "tok"},
    )
    assert t.b.namespace == expected


@pytest.mark.parametrize(
    "content",
    [
        pytest.param("", id="empty-file"),
        pytest.param("- a\n- list\n", id="not-a-mapping"),
        pytest.param("just a string\n", id="scalar"),
        pytest.param(json.dumps({"contexts": []}), id="no-current-context"),
        pytest.param(
            json.dumps({"current-context": "gone", "contexts": []}),
            id="unknown-context",
        ),
        pytest.param(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [{"name": "ctx", "context": {"user": "u"}}],
                }
            ),
            id="context-without-cluster",
        ),
        pytest.param(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [{"name": "ctx", "context": {"cluster": "c"}}],
                    "clusters": [
                        {"name": "c", "cluster": {"server": "https://h"}}
                    ],
                }
            ),
            id="context-without-user",
        ),
        pytest.param(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [
                        {
                            "name": "ctx",
                            "context": {"cluster": "gone", "user": "u"},
                        }
                    ],
                    "clusters": [],
                }
            ),
            id="unknown-cluster",
        ),
        pytest.param(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [
                        {
                            "name": "ctx",
                            "context": {"cluster": "c", "user": "u"},
                        }
                    ],
                    "clusters": [{"name": "c", "cluster": {}}],
                }
            ),
            id="cluster-without-server",
        ),
        pytest.param(
            json.dumps(
                {
                    "current-context": "ctx",
                    "contexts": [
                        {
                            "name": "ctx",
                            "context": {"cluster": "c", "user": "u"},
                        }
                    ],
                    "clusters": [{"name": "c", "cluster": {"server": 6443}}],
                }
            ),
            id="server-not-a-string",
        ),
        pytest.param(
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
                        {"name": "c", "cluster": {"server": "https://h"}}
                    ],
                    "users": [{"name": "u", "user": "not-a-mapping"}],
                }
            ),
            id="user-not-a-mapping",
        ),
        pytest.param("contexts: [unclosed\n", id="yaml-syntax"),
    ],
)
def test_malformed_kubeconfig_is_a_config_error(tmp_path, content):
    path = tmp_path / "kubeconfig"
    path.write_text(content)
    t = _K8sHttpTransport(_kubeconfig_backend(str(path)))
    with pytest.raises(ConfigError, match="malformed kubeconfig"):
        t._load_connection()
    # the TLS-tracking parse of the same file degrades to "nothing tracked"
    assert not any(_kubeconfig_cert_files(str(path)))


def test_missing_kubeconfig_file_is_an_os_error(tmp_path):
    t = _K8sHttpTransport(_kubeconfig_backend(str(tmp_path / "absent")))
    with pytest.raises(OSError):
        t._load_connection()
    assert _kubeconfig_cert_files(str(tmp_path / "absent")) == []


@pytest.mark.parametrize(
    "users",
    [
        pytest.param([], id="no-users-section"),
        pytest.param([{"name": "other", "user": {}}], id="user-not-listed"),
        pytest.param([{"name": "u", "user": None}], id="null-user"),
    ],
)
def test_a_context_whose_user_has_no_credentials_loads(tmp_path, users):
    doc = {
        "current-context": "ctx",
        "contexts": [
            {"name": "ctx", "context": {"cluster": "c", "user": "u"}}
        ],
        "clusters": [{"name": "c", "cluster": {"server": "http://h:8001"}}],
        "users": users,
    }
    path = tmp_path / "kubeconfig"
    path.write_text(json.dumps(doc))
    t = _K8sHttpTransport(_kubeconfig_backend(str(path)))
    t._load_connection()
    assert t._base_url == "http://h:8001"
    assert t._auth_headers() == {}


# --- _load_kubeconfig: bearer tokens --------------------------------------


@pytest.mark.parametrize("field", ["token", "tokenFile"])
def test_a_bearer_token_is_never_sent_over_cleartext_http(tmp_path, field):
    (tmp_path / "tok").write_text("tok")
    value = "tok" if field == "token" else str(tmp_path / "tok")
    with pytest.raises(ConfigError, match="refusing to send it in cleartext"):
        _loaded(tmp_path, server="HTTP://10.0.0.5:8080", user={field: value})


def test_a_token_file_is_read_relative_to_the_kubeconfig_and_reread(
    tmp_path, monkeypatch
):
    (tmp_path / "creds").mkdir()
    token = tmp_path / "creds" / "token"
    token.write_text("first\n")
    # a relative path is relative to the kubeconfig, whatever the cwd is
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    t = _loaded(tmp_path, user={"tokenFile": "creds/token"})
    assert t._token_path == str(token)
    assert t._auth_headers() == {"Authorization": "Bearer first"}
    token.write_text("rotated\n")
    assert t._auth_headers() == {"Authorization": "Bearer rotated"}
    # an unreadable file keeps the last good token
    token.unlink()
    assert t._auth_headers() == {"Authorization": "Bearer rotated"}


def test_an_inline_token_wins_over_a_token_file(tmp_path):
    (tmp_path / "tok").write_text("from-file")
    t = _loaded(
        tmp_path, user={"token": "inline", "tokenFile": str(tmp_path / "tok")}
    )
    assert t._auth_token == "inline"
    assert t._token_path is None


def test_a_missing_token_file_is_an_os_error(tmp_path):
    with pytest.raises(OSError):
        _loaded(tmp_path, user={"tokenFile": "absent"})


# --- _load_kubeconfig: exec and auth-provider users -----------------------


@pytest.mark.parametrize(
    "user",
    [
        pytest.param({"exec": {"command": "aws"}}, id="exec"),
        pytest.param({"auth-provider": {"name": "gcp"}}, id="auth-provider"),
    ],
)
def test_a_credential_plugin_user_is_rejected_loudly(tmp_path, user):
    with pytest.raises(ConfigError, match="exec-credential plugin") as err:
        _loaded(tmp_path, user=user)
    assert "'u'" in str(err.value)


def test_a_credential_plugin_next_to_a_static_token_is_accepted(tmp_path):
    t = _loaded(tmp_path, user={"exec": {"command": "aws"}, "token": "tok"})
    assert t._auth_token == "tok"


# --- _load_kubeconfig: TLS material ---------------------------------------


def test_insecure_skip_tls_verify_disables_verification_loudly(
    tmp_path, caplog
):
    caplog.set_level(logging.WARNING, "cronstable.backends.kubernetes")
    t = _loaded(
        tmp_path,
        cluster={
            "insecure-skip-tls-verify": True,
            # ignored once verification is off
            "certificate-authority": "absent.crt",
        },
        user={"token": "tok"},
    )
    assert t._ssl.verify_mode == ssl.CERT_NONE
    assert t._ssl.check_hostname is False
    assert "NOT verified" in caplog.text


def test_no_ca_means_the_system_trust_store(tmp_path):
    t = _loaded(tmp_path, user={"token": "tok"})
    assert t._ssl.verify_mode == ssl.CERT_REQUIRED
    assert t._ssl.check_hostname is True
    # only the kubeconfig itself is on disk to rotate
    assert list(t.b._tls_signature) == [t.b.kubeconfig]


@pytest.mark.parametrize(
    "cluster, error",
    [
        pytest.param(
            {"certificate-authority-data": "!!not base64!!"},
            ValueError,
            id="ca-data-not-base64",
        ),
        pytest.param(
            {
                "certificate-authority-data": base64.b64encode(
                    b"not a pem"
                ).decode()
            },
            ssl.SSLError,
            id="ca-data-not-pem",
        ),
        pytest.param(
            {"certificate-authority": "absent.crt"},
            OSError,
            id="ca-file-missing",
        ),
    ],
)
def test_unloadable_ca_material_raises_a_start_error(tmp_path, cluster, error):
    # Each is in cron's tolerated backend-start tuple (OSError, ssl.SSLError,
    # ValueError), so the daemon logs "cluster: failed to start" and continues.
    with pytest.raises(error):
        _loaded(tmp_path, cluster=cluster, user={"token": "tok"})


def test_client_key_data_that_is_not_base64_raises_a_start_error(tmp_path):
    with pytest.raises(ValueError):
        _loaded(
            tmp_path,
            user={
                "client-certificate-data": "!!not base64!!",
                "client-key-data": "!!not base64!!",
            },
        )


def test_ca_and_client_cert_files_resolve_against_the_kubeconfig_dir(
    tmp_path, monkeypatch
):
    pki = tmp_path / "pki"
    pki.mkdir()
    material = _write_tls(pki, cn="kube-ca", suffix="client")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    t = _loaded(
        tmp_path,
        cluster={"certificate-authority": "pki/kube-ca-ca.pem"},
        user={
            "client-certificate": "pki/kube-ca-client.pem",
            # an absolute path stays as written
            "client-key": material["key"],
        },
    )
    # only the configured CA is trusted; public roots are excluded
    assert t._ssl.cert_store_stats()["x509_ca"] == 1
    # files are used in place: nothing is copied to a temporary file
    assert t._tempfiles == []
    # every referenced file is tracked for rotation, by its resolved path
    assert list(t.b._tls_signature) == [
        t.b.kubeconfig,
        material["ca"],
        material["cert"],
        material["key"],
    ]
    assert all(sig is not None for sig in t.b._tls_signature.values())
    assert _kubeconfig_cert_files(t.b.kubeconfig) == [
        material["ca"],
        material["cert"],
        material["key"],
    ]
    # a rotation of a referenced file is noticed
    assert t.b.tls_files_changed() is False
    with open(material["cert"], "a") as handle:
        handle.write("\n")
    assert t.b.tls_files_changed() is True


async def test_embedded_data_material_uses_temp_files_removed_on_close(
    tmp_path,
):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="client")
    t = _loaded(
        tmp_path,
        cluster={"certificate-authority-data": _b64_file(material["ca"])},
        user={
            "client-certificate-data": _b64_file(material["cert"]),
            "client-key-data": _b64_file(material["key"]),
        },
    )
    assert t._ssl.cert_store_stats()["x509_ca"] == 1
    cert_tmp, key_tmp = t._tempfiles
    with open(key_tmp, "rb") as handle, open(material["key"], "rb") as real:
        assert handle.read() == real.read()
    # embedded material leaves only the kubeconfig to track
    assert list(t.b._tls_signature) == [t.b.kubeconfig]
    assert _kubeconfig_cert_files(t.b.kubeconfig) == [None, None, None]
    # one temporary file already gone: close still removes the other
    os.unlink(cert_tmp)
    await t.close()
    assert not os.path.exists(key_tmp)
    assert t._tempfiles == []


def test_a_client_cert_without_its_key_is_not_loaded(tmp_path):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="client")
    t = _loaded(
        tmp_path,
        user={"client-certificate": material["cert"], "token": "tok"},
    )
    assert t._auth_token == "tok"
    # a credential plugin is still refused when the cert pair is incomplete
    with pytest.raises(ConfigError, match="exec-credential plugin"):
        _loaded(
            tmp_path,
            user={
                "client-certificate": material["cert"],
                "exec": {"command": "aws"},
            },
        )


def test_a_credential_plugin_next_to_a_client_cert_is_accepted(tmp_path):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="client")
    t = _loaded(
        tmp_path,
        user={
            "client-certificate": material["cert"],
            "client-key": material["key"],
            "exec": {"command": "aws"},
        },
    )
    assert t._auth_headers() == {}


def test_kubeconfig_file_helper():
    # a parent reference is collapsed, so one file has one tracked spelling
    assert _kubeconfig_file("/etc/kube/config", "../pki/ca.crt") == (
        os.path.abspath("/etc/pki/ca.crt")
    )
    assert _kubeconfig_file("/etc/kube/config", None) is None
    assert _kubeconfig_file("/etc/kube/config", "") is None
    assert _kubeconfig_file("/etc/kube/config", "ca.crt") == os.path.join(
        os.path.abspath("/etc/kube"), "ca.crt"
    )
    absolute = os.path.abspath("/pki/ca.crt")
    assert _kubeconfig_file("/etc/kube/config", absolute) == absolute


# --- in-cluster loading ---------------------------------------------------


def _service_account(tmp_path, monkeypatch, *, host, port="443", ca=None):
    sa_dir = tmp_path / "serviceaccount"
    sa_dir.mkdir()
    (sa_dir / "token").write_text("sa-token-1\n")
    (sa_dir / "namespace").write_text("pod-ns\n")
    if ca is not None:
        with open(ca, "rb") as handle:
            (sa_dir / "ca.crt").write_bytes(handle.read())
    monkeypatch.setattr(kubernetes_backend, "_SA_DIR", str(sa_dir))
    if host is None:
        monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    else:
        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", host)
    if port is None:
        monkeypatch.delenv("KUBERNETES_SERVICE_PORT", raising=False)
    else:
        monkeypatch.setenv("KUBERNETES_SERVICE_PORT", port)
    return sa_dir


def test_outside_a_cluster_with_nothing_configured_is_a_config_error(
    tmp_path, monkeypatch
):
    _service_account(tmp_path, monkeypatch, host=None)
    t = _K8sHttpTransport(_backend())
    with pytest.raises(ConfigError, match="not running in a cluster"):
        t._load_connection()


def test_missing_service_account_files_are_a_config_error(
    tmp_path, monkeypatch
):
    sa_dir = _service_account(tmp_path, monkeypatch, host="10.96.0.1")
    t = _K8sHttpTransport(_backend())
    # no ca.crt
    with pytest.raises(ConfigError, match="service account credentials"):
        t._load_connection()
    # a ca.crt that is not a certificate
    (sa_dir / "ca.crt").write_text("garbage")
    with pytest.raises(ConfigError, match="service account credentials"):
        t._load_connection()
    # no token
    (sa_dir / "token").unlink()
    with pytest.raises(ConfigError, match="service account credentials"):
        t._load_connection()


def test_the_service_account_token_never_goes_to_a_cleartext_apiserver(
    tmp_path, monkeypatch
):
    # Config validation rejects an http:// apiServer, so reach the defensive
    # check the way a bug upstream would.
    _service_account(tmp_path, monkeypatch, host="10.96.0.1")
    b = _backend()
    b.api_server_override = "http://proxy.local:8001/"
    t = _K8sHttpTransport(b)
    with pytest.raises(ConfigError, match="non-https apiserver"):
        t._load_connection()
    assert t._auth_token is None


@pytest.mark.parametrize(
    "host, port, override, expected",
    [
        ("10.96.0.1", "6443", None, "https://10.96.0.1:6443"),
        ("10.96.0.1", None, None, "https://10.96.0.1:443"),
        ("fd00:10:96::1", "443", None, "https://[fd00:10:96::1]:443"),
        (None, None, "https://rbac-proxy:8443/", "https://rbac-proxy:8443"),
        ("10.96.0.1", "443", "https://vip:6443", "https://vip:6443"),
    ],
)
def test_in_cluster_connection(
    tmp_path, monkeypatch, host, port, override, expected
):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="apiserver")
    sa_dir = _service_account(
        tmp_path, monkeypatch, host=host, port=port, ca=material["ca"]
    )
    extra = "    apiServer: " + override + "\n" if override else ""
    b = _backend(extra, namespace=None)
    t = _K8sHttpTransport(b)
    t._load_connection()
    assert t._base_url == expected
    assert t._token_path == str(sa_dir / "token")
    assert t._auth_headers() == {"Authorization": "Bearer sa-token-1"}
    assert t._ssl.cert_store_stats()["x509_ca"] == 1
    # the pod's namespace applies when none is configured
    assert b.namespace == "pod-ns"
    # the projected CA is tracked for rotation
    assert list(b._tls_signature) == [str(sa_dir / "ca.crt")]


def test_in_cluster_namespace_falls_back_to_default(tmp_path, monkeypatch):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="apiserver")
    sa_dir = _service_account(
        tmp_path, monkeypatch, host="10.96.0.1", ca=material["ca"]
    )
    (sa_dir / "namespace").unlink()
    b = _backend(namespace=None)
    _K8sHttpTransport(b)._load_connection()
    assert b.namespace == "default"
    configured = _backend(namespace="ns")
    _K8sHttpTransport(configured)._load_connection()
    assert configured.namespace == "ns"


# --- observe / write against the fake apiserver ---------------------------


@contextlib.asynccontextmanager
async def _connected(tmp_path, apiserver, **kwargs):
    """A set-up transport pointed at ``apiserver`` over plain HTTP."""
    path = _write_kubeconfig(tmp_path / "kubeconfig", server=apiserver.url)
    backend = _kubeconfig_backend(path, **kwargs)
    transport = _K8sHttpTransport(backend)
    await transport.setup()
    try:
        yield transport
    finally:
        await transport.close()


def _lease_body(holder="node-a#1", resource_version=None):
    metadata = {"name": "yl", "namespace": "ns"}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": metadata,
        "spec": {"holderIdentity": holder, "leaseDurationSeconds": 15},
    }


async def test_observe_and_write_round_trip(tmp_path):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        assert await t.observe() is None
        assert await t.write(_lease_body(), create=True) is True
        seen = await t.observe()
        assert seen["spec"]["holderIdentity"] == "node-a#1"
        first_rv = seen["metadata"]["resourceVersion"]
        assert isinstance(first_rv, str)
        body = _lease_body("node-a#1", first_rv)
        assert await t.write(body, create=False) is True
        again = await t.observe()
        assert int(again["metadata"]["resourceVersion"]) > int(first_rv)
        assert again["metadata"]["uid"] == seen["metadata"]["uid"]
    assert [(r.method, r.path) for r in api.requests] == [
        ("GET", LEASES + "/yl"),
        ("POST", LEASES),
        ("GET", LEASES + "/yl"),
        ("PUT", LEASES + "/yl"),
        ("GET", LEASES + "/yl"),
    ]
    post = api.requests[1]
    assert post.json == _lease_body()
    assert post.headers["Content-Type"] == "application/json"
    assert post.headers["Accept"] == "application/json"
    assert "Authorization" not in post.headers


async def test_a_lost_race_is_false_never_an_error(tmp_path):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        assert await t.write(_lease_body("node-a#1"), create=True) is True
        # AlreadyExists: another node created it first
        assert await t.write(_lease_body("node-b#2"), create=True) is False
        # Conflict: the resourceVersion moved on since this node observed it
        stale = (await t.observe())["metadata"]["resourceVersion"]
        assert await t.write(_lease_body("x", stale), create=False) is True
        assert await t.write(_lease_body("y", stale), create=False) is False
        assert (await t.observe())["spec"]["holderIdentity"] == "x"


async def test_replacing_a_deleted_lease_recreates_it(tmp_path):
    # Replacing a deleted Lease creates it again and returns 201. The live
    # backend tests verify this behavior against a real API server. A write
    # with a stale resourceVersion then returns 409 Conflict.
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        assert await t.write(_lease_body("node-a#1", "1"), create=False)
        seen = await t.observe()
        assert seen["spec"]["holderIdentity"] == "node-a#1"
        assert not await t.write(_lease_body("node-b#1", "1"), create=False)


async def test_a_404_on_a_write_raises(tmp_path):
    # Only 409 indicates a conflicting write. A 404 response, such as one
    # for a deleted namespace, fails the round.
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        api.inject(status=404, body=b"{}", times=1)
        with pytest.raises(aiohttp.ClientResponseError) as err:
            await t.write(_lease_body("node-a#1", "1"), create=False)
        assert err.value.status == 404


@pytest.mark.parametrize("status", [400, 401, 403, 500, 503])
async def test_an_error_status_raises_from_observe_and_write(tmp_path, status):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        api.inject(status=status, body=b"{}", times=3)
        with pytest.raises(aiohttp.ClientResponseError) as err:
            await t.observe()
        assert err.value.status == status
        for create in (True, False):
            with pytest.raises(aiohttp.ClientResponseError) as err:
                await t.write(_lease_body(), create=create)
            assert err.value.status == status
        assert api.store.objects == {}


@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_a_redirect_is_never_followed_and_never_a_win(tmp_path, status):
    # The redirect points at a working apiserver and carries a Lease-shaped
    # JSON body. Following it would be SSRF; taking the 3xx for the
    # apiserver's answer would let write() report a lease nobody stored.
    async with FakeKubeApiserver() as target, FakeKubeApiserver() as proxy:
        async with _connected(tmp_path, proxy) as t:
            proxy.inject(
                status=status,
                body=json.dumps(_lease_body()).encode(),
                headers={"Location": target.url + LEASES + "/yl"},
                times=3,
            )
            with pytest.raises(aiohttp.ClientResponseError) as err:
                await t.observe()
            assert err.value.status == status
            for create in (True, False):
                with pytest.raises(aiohttp.ClientResponseError):
                    await t.write(_lease_body(), create=create)
        assert target.requests == []


async def test_a_bodiless_success_status_is_not_a_lease(tmp_path):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        api.inject(status=204, times=2)
        with pytest.raises(aiohttp.ClientResponseError):
            await t.observe()
        with pytest.raises(aiohttp.ClientResponseError):
            await t.write(_lease_body(), create=True)


@pytest.mark.parametrize(
    "fault",
    [
        pytest.param({"body": b"{not json"}, id="invalid-json"),
        pytest.param({"body": b"\xff\xfe\xfd"}, id="invalid-utf8"),
        pytest.param(
            {"body": b"<html></html>", "content_type": "text/html"},
            id="html",
        ),
    ],
)
async def test_observe_rejects_a_body_that_is_not_json(tmp_path, fault):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        api.inject(**fault)
        with pytest.raises((aiohttp.ClientError, ValueError)):
            await t.observe()


async def test_a_round_survives_a_body_that_is_not_json(tmp_path):
    # Through the backend: the round fails (not quorate), nothing escapes.
    async with FakeKubeApiserver() as api:
        api.inject(body=b"[1, 2]")
        path = _write_kubeconfig(tmp_path / "kubeconfig", server=api.url)
        b = _kubeconfig_backend(path)
        await b.start()
        try:
            assert not b.is_quorate()
            await b._renew_once()
            assert b.is_leader()
        finally:
            await b.stop()


async def test_an_apiserver_that_hangs_times_out(tmp_path):
    async with FakeKubeApiserver() as api:
        async with _connected(tmp_path, api, timeout=1) as t:
            api.inject(hang=True, times=2)
            with pytest.raises(asyncio.TimeoutError):
                await t.observe()
            with pytest.raises(asyncio.TimeoutError):
                await t.write(_lease_body(), create=True)


async def test_a_refused_connection_raises(tmp_path):
    path = _write_kubeconfig(tmp_path / "kubeconfig", server=DEAD_ENDPOINT)
    t = _K8sHttpTransport(_kubeconfig_backend(path))
    await t.setup()
    try:
        with pytest.raises(aiohttp.ClientConnectionError):
            await t.observe()
        with pytest.raises(aiohttp.ClientConnectionError):
            await t.write(_lease_body(), create=True)
    finally:
        await t.close()
    # closing twice is harmless
    await t.close()


async def test_the_lease_url_percent_encodes_namespace_and_name(tmp_path):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        t.b.namespace = "we/ird ns?x#y"
        t.b.lease_name = "a/b"
        assert await t.observe() is None
        t.b.namespace = None
        assert t._lease_url(collection=True).endswith("/namespaces//leases")
    assert api.requests[0].raw_path == (
        "/apis/coordination.k8s.io/v1/namespaces/we%2Fird%20ns%3Fx%23y"
        "/leases/a%2Fb"
    )


async def test_bearer_token_checking(tmp_path):
    async with FakeKubeApiserver() as api, _connected(tmp_path, api) as t:
        api.tokens = {"good", "no-rbac"}
        api.allowed_tokens = {"good"}
        # no token at all, then an unknown one
        for token in (None, "bad"):
            t._auth_token = token
            with pytest.raises(aiohttp.ClientResponseError) as err:
                await t.observe()
            assert err.value.status == 401
        # authenticated but not authorized
        t._auth_token = "no-rbac"
        with pytest.raises(aiohttp.ClientResponseError) as err:
            await t.write(_lease_body(), create=True)
        assert err.value.status == 403
        t._auth_token = "good"
        assert await t.write(_lease_body(), create=True) is True
        assert api.requests[-1].headers["Authorization"] == "Bearer good"


# --- over TLS -------------------------------------------------------------


async def test_in_cluster_election_over_tls_with_a_rotating_token(
    tmp_path, monkeypatch
):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="apiserver")
    async with FakeKubeApiserver(
        ssl_context=server_ssl_context(material)
    ) as api:
        api.tokens = {"sa-token-1"}
        sa_dir = _service_account(
            tmp_path,
            monkeypatch,
            host="127.0.0.1",
            port=str(api.port),
            ca=material["ca"],
        )
        b = _backend(namespace=None)
        await b.start()
        try:
            assert b.is_leader()
            assert b.namespace == "pod-ns"
            assert ("pod-ns", "yl") in api.store.objects
            # the kubelet rotates the projected token; the old one dies
            (sa_dir / "token").write_text("sa-token-2\n")
            api.tokens = {"sa-token-2"}
            await b._renew_once()
            assert b.is_leader()
            assert (
                api.requests[-1].headers["Authorization"]
                == "Bearer sa-token-2"
            )
        finally:
            await b.stop()
        # the graceful stop handed the lease back
        lease = api.store.objects[("pod-ns", "yl")]
        assert lease["spec"].get("holderIdentity") is None


async def test_kubeconfig_election_over_mutual_tls(tmp_path):
    material = _write_tls(tmp_path, cn="kube-ca", suffix="client")
    ctx = server_ssl_context(material, require_client_cert=True)
    async with FakeKubeApiserver(ssl_context=ctx) as api:
        server = "https://localhost:{}".format(api.port)
        with_cert = _write_kubeconfig(
            tmp_path / "with-cert",
            server=server,
            cluster={"certificate-authority-data": _b64_file(material["ca"])},
            user={
                "client-certificate-data": _b64_file(material["cert"]),
                "client-key-data": _b64_file(material["key"]),
            },
        )
        b = _kubeconfig_backend(with_cert)
        await b.start()
        tempfiles = list(b._transport._tempfiles)
        try:
            assert b.is_leader()
        finally:
            await b.stop()
        assert not any(os.path.exists(tmp) for tmp in tempfiles)
        # without the client certificate the handshake is refused
        bare = _write_kubeconfig(
            tmp_path / "bare",
            server=server,
            cluster={"certificate-authority": material["ca"]},
        )
        refused = _kubeconfig_backend(bare, node="node-b")
        await refused.start()
        try:
            assert not refused.is_quorate()
        finally:
            await refused.stop()


async def test_an_apiserver_with_an_untrusted_certificate_is_never_spoken_to(
    tmp_path,
):
    served = _write_tls(tmp_path, cn="rogue-ca", suffix="apiserver")
    trusted = _write_tls(tmp_path, cn="kube-ca", suffix="apiserver")
    async with FakeKubeApiserver(
        ssl_context=server_ssl_context(served)
    ) as api:
        server = "https://localhost:{}".format(api.port)
        path = _write_kubeconfig(
            tmp_path / "kubeconfig",
            server=server,
            cluster={"certificate-authority": trusted["ca"]},
            user={"token": "secret-token"},
        )
        b = _kubeconfig_backend(path)
        await b.start()
        try:
            assert not b.is_quorate()
        finally:
            await b.stop()
        assert api.requests == []
        # insecure-skip-tls-verify logs a warning when verification is disabled
        insecure = _write_kubeconfig(
            tmp_path / "insecure",
            server=server,
            cluster={"insecure-skip-tls-verify": True},
            user={"token": "secret-token"},
        )
        b2 = _kubeconfig_backend(insecure)
        await b2.start()
        try:
            assert b2.is_leader()
        finally:
            await b2.stop()


# --- end to end: leader election through the HTTP transport ---------------


@contextlib.asynccontextmanager
async def _cluster(tmp_path, names=("node-a", "node-b")):
    async with FakeKubeApiserver() as api:
        path = _write_kubeconfig(tmp_path / "kubeconfig", server=api.url)
        nodes = [_kubeconfig_backend(path, node=name) for name in names]
        # These scenarios advance elections explicitly through _renew_once.
        # A background renewal can change the lease after a follower observes
        # it, invalidating a simulated expiry. The automatic lease-recovery
        # test below exercises the real renewal loop separately.
        for node in nodes:
            node._renew_loop = AsyncMock()
        try:
            yield api, nodes
        finally:
            for node in nodes:
                await node.stop()


def _stored(api):
    return api.store.objects[("ns", "yl")]


async def test_two_nodes_elect_one_leader_and_hand_over_on_stop(tmp_path):
    async with _cluster(tmp_path) as (api, [a, b]):
        await a.start()
        await b.start()
        assert a.is_leader() and not b.is_leader()
        assert b.is_quorate()
        assert a.leader_name() == b.leader_name() == "node-a"
        assert _stored(api)["spec"]["holderIdentity"] == a.identity
        versions = [int(_stored(api)["metadata"]["resourceVersion"])]
        for _ in range(3):
            await b._renew_once()
            await a._renew_once()
            assert [a.is_leader(), b.is_leader()] == [True, False]
            versions.append(int(_stored(api)["metadata"]["resourceVersion"]))
        # every renew is a write the apiserver versioned, strictly forward
        assert versions == sorted(set(versions))
        await a.stop()
        assert _stored(api)["spec"].get("holderIdentity") is None
        await b._renew_once()
        assert b.is_leader()
        assert _stored(api)["spec"]["holderIdentity"] == b.identity
        assert _stored(api)["spec"]["leaseTransitions"] == 1
        assert int(_stored(api)["metadata"]["resourceVersion"]) > versions[-1]


async def test_concurrent_cold_start_elects_exactly_one_leader(tmp_path):
    names = tuple("node-{}".format(i) for i in range(5))
    async with _cluster(tmp_path, names) as (api, nodes):
        await asyncio.gather(*(node.start() for node in nodes))
        leaders = [node for node in nodes if node.is_leader()]
        assert len(leaders) == 1
        assert _stored(api)["spec"]["holderIdentity"] == leaders[0].identity
        # a loser of the create race never reports an unknown (None) holder
        assert all(node.leader_name() is not None for node in nodes)
        await asyncio.gather(*(node._renew_once() for node in nodes))
        assert {node.leader_name() for node in nodes} == {
            leaders[0].display_identity
        }


async def test_leadership_moves_when_the_holder_stops_renewing(
    tmp_path, monkeypatch
):
    clock = FakeClock()
    monkeypatch.setattr(kubernetes_backend, "_monotonic", clock)
    async with _cluster(tmp_path) as (api, [a, b]):
        await a.start()
        await b.start()
        assert a.is_leader() and not b.is_leader()
        # inside the lease duration the follower keeps waiting
        clock.advance(10)
        await b._renew_once()
        assert not b.is_leader()
        # the holder never renewed: its own fence has closed locally, and
        # past the duration the follower takes the lease over
        clock.advance(6)
        assert not a.is_leader()
        await b._renew_once()
        assert b.is_leader()
        assert _stored(api)["spec"]["holderIdentity"] == b.identity
        assert _stored(api)["spec"]["leaseTransitions"] == 1
        # the old holder comes back, sees the new record and stands down
        await a._renew_once()
        assert not a.is_leader()
        assert a.leader_name() == "node-b"


async def test_a_stale_holder_loses_the_resource_version_race(tmp_path):
    # The holder observes, then another writer moves the Lease on before the
    # holder's PUT lands: a real 409 over HTTP, and the holder self-demotes.
    async with _cluster(tmp_path) as (api, [a, _b]):
        await a.start()
        assert a.is_leader()
        real_write = a._transport.write

        async def write_after_a_rival(body, *, create):
            api.store.patch("ns", "yl", {"metadata": {"labels": {"x": "y"}}})
            return await real_write(body, create=create)

        a._transport.write = write_after_a_rival
        await a._renew_once()
        assert not a.is_leader()
        assert a.is_quorate()
        a._transport.write = real_write
        await a._renew_once()
        assert a.is_leader()


async def test_a_lease_deleted_under_the_holder_is_recreated(tmp_path):
    async with _cluster(tmp_path) as (api, [a, _b]):
        await a.start()
        api.store.delete("ns", "yl")
        await a._renew_once()
        assert a.is_leader()
        assert _stored(api)["spec"]["holderIdentity"] == a.identity


async def test_reboot_ran_marks_survive_a_failover(tmp_path):
    async with _cluster(tmp_path) as (api, [a, b]):
        await a.start()
        await b.start()
        await a.mark_reboot_ran("oneshot")
        # persisted eagerly, into the Lease annotations
        assert REBOOT_RAN_KEY in _stored(api)["metadata"]["annotations"]
        await a.stop()
        # the hand-back keeps the annotation
        assert REBOOT_RAN_KEY in _stored(api)["metadata"]["annotations"]
        await b._renew_once()
        assert b.is_leader()
        assert b.reboot_ran("oneshot") is True
        assert b.reboot_ran("another") is False


async def test_an_apiserver_outage_costs_quorum_then_recovers(
    tmp_path, monkeypatch
):
    clock = FakeClock()
    monkeypatch.setattr(kubernetes_backend, "_monotonic", clock)
    async with _cluster(tmp_path) as (api, [a, _b]):
        await a.start()
        assert a.is_leader() and a.is_quorate()
        api.inject(status=503, times=100)
        with pytest.raises(aiohttp.ClientResponseError):
            await a._renew_once()
        clock.advance(16)
        assert not a.is_leader()
        assert not a.is_quorate()
        del api._faults[:]
        await a._renew_once()
        assert a.is_leader() and a.is_quorate()


async def test_the_renew_loop_recovers_a_deleted_lease_on_its_own(tmp_path):
    async with FakeKubeApiserver() as api:
        path = _write_kubeconfig(tmp_path / "kubeconfig", server=api.url)
        b = _kubeconfig_backend(path)
        b.retry_period = 0.05
        await b.start()
        try:
            api.store.delete("ns", "yl")
            await _wait_until(
                lambda: ("ns", "yl") in api.store.objects, tries=1000
            )
        finally:
            await b.stop()


# --- the fake itself ------------------------------------------------------


async def test_fake_apiserver_semantics():
    async with FakeKubeApiserver() as api, aiohttp.ClientSession() as http:

        async def call(method, path, body=None):
            async with http.request(method, api.url + path, json=body) as r:
                return r.status, await r.json()

        one = LEASES + "/yl"
        status, err = await call("GET", one)
        assert (status, err["kind"], err["reason"]) == (
            404,
            "Status",
            "NotFound",
        )
        assert err["details"]["name"] == "yl" and err["code"] == 404
        status, made = await call("POST", LEASES, _lease_body())
        assert status == 201 and made["metadata"]["uid"]
        status, err = await call("POST", LEASES, _lease_body())
        assert (status, err["reason"]) == (409, "AlreadyExists")
        status, _ = await call("POST", LEASES, {"metadata": {}})
        assert status == 400
        wrong_ns = _lease_body()
        wrong_ns["metadata"]["namespace"] = "other"
        assert (await call("POST", LEASES, wrong_ns))[0] == 400
        wrong_name = _lease_body()
        wrong_name["metadata"]["name"] = "other"
        assert (await call("PUT", one, wrong_name))[0] == 400
        # a replace with no resourceVersion is an unconditional update
        status, replaced = await call("PUT", one, _lease_body("node-b#2"))
        assert status == 200
        assert replaced["metadata"]["uid"] == made["metadata"]["uid"]
        status, err = await call(
            "PUT", one, _lease_body("c", made["metadata"]["resourceVersion"])
        )
        assert (status, err["reason"]) == (409, "Conflict")
        patch = {"spec": {"holderIdentity": None, "leaseTransitions": 3}}
        status, patched = await call("PATCH", one, patch)
        assert status == 200
        assert "holderIdentity" not in patched["spec"]
        assert patched["spec"]["leaseTransitions"] == 3
        status, listed = await call("GET", LEASES)
        assert [i["metadata"]["name"] for i in listed["items"]] == ["yl"]
        status, gone = await call("DELETE", one)
        assert (status, gone["status"]) == (200, "Success")
        assert (await call("DELETE", one))[0] == 404
        assert (await call("PATCH", one, patch))[0] == 404
        assert (await call("DELETE", LEASES))[0] == 405
        assert (await call("GET", "/api/v1/pods"))[0] == 404
        assert (await call("GET", LEASES + "/yl/status"))[0] == 404
        # a fault can target one method; a body that is not JSON reads as {}
        api.inject(method="PUT", status=500, body=b"{}")
        assert (await call("GET", one))[0] == 404
        assert (await call("PUT", one, _lease_body()))[0] == 500
        async with http.post(api.url + LEASES, data=b"{not json") as resp:
            assert resp.status == 400
        assert api.requests[-1].json is None


async def test_fake_apiserver_passes_the_shared_conformance_checks():
    # The same checks tests/test_backend_live.py runs on a real apiserver.
    async with FakeKubeApiserver() as api, aiohttp.ClientSession() as http:

        async def call(method, path, body):
            async with http.request(method, api.url + path, json=body) as r:
                return r.status, await r.json()

        await check_lease_api(call, "ns", "conformance")
    assert api.store.objects == {}
