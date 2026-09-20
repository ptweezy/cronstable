"""Provide a fake of the official Kubernetes Python client in ``sys.modules``.

``install`` registers the modules that the library transport imports:
``kubernetes``, ``kubernetes.client``, ``kubernetes.client.exceptions``,
``kubernetes.config``, and ``kubernetes.config.config_exception``. It returns
a ``FakeKubernetes`` instance that controls the fake client.

``CoordinationV1Api`` shares ``tests._fake_kube_apiserver.LeaseStore`` with
the fake API server. Both transports therefore use the same create, replace,
and conflict behavior.

Match the real client in these ways:

* Reads return ``V1Lease`` model objects with snake_case attributes. The
  ``renew_time``, ``acquire_time``, and ``creation_timestamp`` attributes
  contain datetime values.
* ``ApiClient.sanitize_for_serialization`` uses each model's ``attribute_map``
  to produce an API dictionary, omitting attributes set to ``None``. It formats
  datetimes with ``isoformat()``, which uses ``+00:00`` where the API server
  uses ``Z``. Plain dictionaries pass through unchanged.
* Writes accept the dictionaries built by the backend.
* HTTP errors raise ``ApiException`` with ``status``, ``reason``, and a JSON
  ``Status`` object in ``body``.
* Configuration loading errors raise ``ConfigException``.
"""

import datetime
import json
import sys
import threading
import types
from typing import Any, Optional

from tests._fake_kube_apiserver import ApiError, LeaseStore


class ApiException(Exception):
    def __init__(
        self, status: int = 0, reason: str = "", body: Optional[str] = None
    ) -> None:
        super().__init__("({})\nReason: {}\n".format(status, reason))
        self.status = status
        self.reason = reason
        self.body = body


class ConfigException(Exception):
    pass


class _Model:
    attribute_map: dict[str, str] = {}

    def __init__(self, **kwargs: Any) -> None:
        for attr in self.attribute_map:
            setattr(self, attr, kwargs.get(attr))


class V1ObjectMeta(_Model):
    attribute_map = {
        "name": "name",
        "namespace": "namespace",
        "uid": "uid",
        "resource_version": "resourceVersion",
        "creation_timestamp": "creationTimestamp",
        "annotations": "annotations",
        "labels": "labels",
    }


class V1LeaseSpec(_Model):
    attribute_map = {
        "holder_identity": "holderIdentity",
        "lease_duration_seconds": "leaseDurationSeconds",
        "acquire_time": "acquireTime",
        "renew_time": "renewTime",
        "lease_transitions": "leaseTransitions",
    }


class V1Lease(_Model):
    attribute_map = {
        "api_version": "apiVersion",
        "kind": "kind",
        "metadata": "metadata",
        "spec": "spec",
    }


_DATETIME_ATTRS = {"creation_timestamp", "acquire_time", "renew_time"}


def _parse_time(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_model(cls: type[_Model], wire: Optional[dict[str, Any]]) -> Any:
    if wire is None:
        return None
    kwargs: dict[str, Any] = {}
    for attr, key in cls.attribute_map.items():
        value = wire.get(key)
        if attr in _DATETIME_ATTRS:
            value = _parse_time(value)
        kwargs[attr] = value
    return cls(**kwargs)


def lease_model(wire: dict[str, Any]) -> V1Lease:
    lease = _to_model(V1Lease, wire)
    lease.metadata = _to_model(V1ObjectMeta, wire.get("metadata"))
    lease.spec = _to_model(V1LeaseSpec, wire.get("spec"))
    result: V1Lease = lease
    return result


class Configuration:
    _default: Optional["Configuration"] = None

    def __init__(self) -> None:
        self.host = "http://localhost"
        self.verify_ssl = True
        self.api_key: dict[str, str] = {}

    @classmethod
    def set_default(cls, config: "Configuration") -> None:
        cls._default = config

    @classmethod
    def get_default_copy(cls) -> "Configuration":
        source = cls._default or Configuration()
        copy = Configuration()
        copy.host = source.host
        copy.verify_ssl = source.verify_ssl
        copy.api_key = dict(source.api_key)
        return copy


class FakeKubernetes:
    """Configure the fake client and record its calls."""

    def __init__(self, store: Optional[LeaseStore] = None) -> None:
        self.store = store if store is not None else LeaseStore()
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        # loader behavior
        self.kube_config_error: Optional[Exception] = None
        self.incluster_error: Optional[Exception] = None
        self.loaded_host = "https://kube.example:6443"
        self.loaded_verify_ssl = True
        self.active_context: Optional[dict[str, Any]] = {
            "name": "ctx",
            "context": {"cluster": "c", "user": "u"},
        }
        # statuses raised (as ApiException) by the next API calls, in order
        self.fail_with: list[int] = []
        self.api_clients: list[ApiClient] = []
        self.threads: set[str] = set()

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))
        self.threads.add(threading.current_thread().name)

    def names(self) -> list[str]:
        return [name for name, _a, _k in self.calls]

    def _load(self) -> None:
        loaded = Configuration()
        loaded.host = self.loaded_host
        loaded.verify_ssl = self.loaded_verify_ssl
        Configuration.set_default(loaded)


_active: Optional[FakeKubernetes] = None


def _fake() -> FakeKubernetes:
    assert _active is not None, "the fake kubernetes package is not installed"
    return _active


class ApiClient:
    def __init__(self, configuration: Optional[Configuration] = None) -> None:
        self.configuration = configuration or Configuration.get_default_copy()
        self.closed = False
        _fake().api_clients.append(self)

    def close(self) -> None:
        _fake()._record("ApiClient.close")
        self.closed = True

    def sanitize_for_serialization(self, obj: Any) -> Any:
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, (list, tuple)):
            return [self.sanitize_for_serialization(item) for item in obj]
        if isinstance(obj, dict):
            return {
                key: self.sanitize_for_serialization(value)
                for key, value in obj.items()
            }
        return {
            obj.attribute_map[attr]: self.sanitize_for_serialization(
                getattr(obj, attr)
            )
            for attr in obj.attribute_map
            if getattr(obj, attr) is not None
        }


class CoordinationV1Api:
    def __init__(self, api_client: Optional[ApiClient] = None) -> None:
        self.api_client = api_client or ApiClient()

    def _call(self, name: str, *args: Any, **kwargs: Any) -> None:
        fake = _fake()
        fake._record(name, *args, **kwargs)
        if fake.fail_with:
            status = fake.fail_with.pop(0)
            raise ApiException(status=status, reason="injected")

    @staticmethod
    def _translate(ex: ApiError) -> ApiException:
        return ApiException(
            status=ex.code,
            reason=ex.reason,
            body=json.dumps(ex.status_object()),
        )

    def read_namespaced_lease(
        self, name: str, namespace: str, **kwargs: Any
    ) -> V1Lease:
        self._call("read_namespaced_lease", name, namespace, **kwargs)
        try:
            return lease_model(_fake().store.get(namespace, name))
        except ApiError as ex:
            raise self._translate(ex) from ex

    def create_namespaced_lease(
        self, namespace: str, body: Any, **kwargs: Any
    ) -> V1Lease:
        self._call("create_namespaced_lease", namespace, body, **kwargs)
        wire = self.api_client.sanitize_for_serialization(body)
        try:
            return lease_model(_fake().store.create(namespace, wire))
        except ApiError as ex:
            raise self._translate(ex) from ex

    def replace_namespaced_lease(
        self, name: str, namespace: str, body: Any, **kwargs: Any
    ) -> V1Lease:
        self._call("replace_namespaced_lease", name, namespace, body, **kwargs)
        wire = self.api_client.sanitize_for_serialization(body)
        try:
            return lease_model(_fake().store.replace(namespace, name, wire))
        except ApiError as ex:
            raise self._translate(ex) from ex


def _load_kube_config(config_file: Optional[str] = None, **kw: Any) -> None:
    fake = _fake()
    fake._record("load_kube_config", config_file=config_file, **kw)
    if fake.kube_config_error is not None:
        raise fake.kube_config_error
    fake._load()


def _list_kube_config_contexts(
    config_file: Optional[str] = None,
) -> tuple[list[dict[str, Any]], Optional[dict[str, Any]]]:
    fake = _fake()
    fake._record("list_kube_config_contexts", config_file=config_file)
    contexts = [fake.active_context] if fake.active_context else []
    return contexts, fake.active_context


def _load_incluster_config() -> None:
    fake = _fake()
    fake._record("load_incluster_config")
    if fake.incluster_error is not None:
        raise fake.incluster_error
    fake._load()


def install(
    monkeypatch: Any, store: Optional[LeaseStore] = None
) -> FakeKubernetes:
    """Install the fake package in ``sys.modules`` for one test."""
    fake = FakeKubernetes(store)
    monkeypatch.setattr(sys.modules[__name__], "_active", fake)
    monkeypatch.setattr(Configuration, "_default", None)

    root = types.ModuleType("kubernetes")
    client = types.ModuleType("kubernetes.client")
    exceptions = types.ModuleType("kubernetes.client.exceptions")
    config = types.ModuleType("kubernetes.config")
    config_exception = types.ModuleType("kubernetes.config.config_exception")

    exceptions.ApiException = ApiException  # type: ignore[attr-defined]
    config_exception.ConfigException = (  # type: ignore[attr-defined]
        ConfigException
    )
    for name, value in {
        "ApiClient": ApiClient,
        "Configuration": Configuration,
        "CoordinationV1Api": CoordinationV1Api,
        "V1Lease": V1Lease,
        "V1LeaseSpec": V1LeaseSpec,
        "V1ObjectMeta": V1ObjectMeta,
        "ApiException": ApiException,
        "exceptions": exceptions,
    }.items():
        setattr(client, name, value)
    for name, value in {
        "load_kube_config": _load_kube_config,
        "list_kube_config_contexts": _list_kube_config_contexts,
        "load_incluster_config": _load_incluster_config,
        "ConfigException": ConfigException,
        "config_exception": config_exception,
    }.items():
        setattr(config, name, value)
    root.client = client  # type: ignore[attr-defined]
    root.config = config  # type: ignore[attr-defined]
    for module in (root, client, exceptions, config, config_exception):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    return fake


def uninstall(monkeypatch: Any) -> None:
    """Make ``import kubernetes`` raise ImportError for one test."""
    for name in [m for m in sys.modules if m.split(".")[0] == "kubernetes"]:
        monkeypatch.delitem(sys.modules, name)
    # a None entry is the import system's "known missing" marker
    monkeypatch.setitem(sys.modules, "kubernetes", None)
