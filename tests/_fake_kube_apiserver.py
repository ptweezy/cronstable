"""Simulate the Kubernetes API for ``coordination.k8s.io/v1`` Leases.

``FakeKubeApiserver`` uses the aiohttp server in ``tests/_fake_http.py`` and
stores leases in ``LeaseStore``. The fake client in
``tests/_fake_kubernetes_client.py`` shares the store without using HTTP.

Match the real API server in these ways:

* Each write assigns a new ``resourceVersion`` from a store-wide counter.
  Versions are opaque strings.
* Creating a Lease returns ``201``. Creating one with an existing name
  returns ``409 AlreadyExists``.
* Replacing a Lease with a stale ``resourceVersion`` returns ``409 Conflict``.
  Omitting the version performs an unconditional update. Replacing a missing
  Lease creates it and returns ``201``, even if the request includes a version.
* Patching or deleting a missing Lease returns ``404 NotFound``.
* Creating a Lease in a different namespace or replacing one with a different
  object name returns ``400 BadRequest``.
* Errors return a ``v1`` ``Status`` object with ``status: Failure``,
  ``reason``, ``code``, and ``message``. Include ``details`` when an object
  name is available.
* If tokens are configured, a missing or unknown token returns ``401
  Unauthorized``. A known token outside ``allowed_tokens`` returns ``403
  Forbidden``, matching a role-based access control (RBAC) denial.
* Creation assigns ``metadata.uid`` and ``creationTimestamp``. Replacing the
  object preserves both values.

PATCH supports only JSON merge patch (RFC 7386).
"""

import copy
import datetime
import ssl
import uuid
from typing import Any, Optional

from aiohttp import web

from tests._fake_http import FakeHttpServer, RecordedRequest

API_VERSION = "coordination.k8s.io/v1"
_PREFIX = "/apis/coordination.k8s.io/v1/namespaces/"


class ApiError(Exception):
    def __init__(
        self, code: int, reason: str, message: str, name: str = ""
    ) -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason
        self.message = message
        self.name = name

    def status_object(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": self.message,
            "reason": self.reason,
            "code": self.code,
        }
        if self.name:
            out["details"] = {
                "name": self.name,
                "group": "coordination.k8s.io",
                "kind": "leases",
            }
        return out


def _not_found(name: str) -> ApiError:
    return ApiError(
        404,
        "NotFound",
        'leases.coordination.k8s.io "{}" not found'.format(name),
        name,
    )


def merge_patch(target: Any, patch: Any) -> Any:
    """RFC 7386 JSON merge patch."""
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    out = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = merge_patch(out.get(key), value)
    return out


class LeaseStore:
    """Lease objects with the apiserver's optimistic concurrency."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self._rv = 1000

    def _stamp(self, obj: dict[str, Any]) -> None:
        self._rv += 1
        obj["metadata"]["resourceVersion"] = str(self._rv)

    def get(self, namespace: str, name: str) -> dict[str, Any]:
        obj = self.objects.get((namespace, name))
        if obj is None:
            raise _not_found(name)
        return copy.deepcopy(obj)

    def list(self, namespace: str) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(obj)
            for (ns, _name), obj in sorted(self.objects.items())
            if ns == namespace
        ]

    def create(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        obj = copy.deepcopy(body)
        meta = obj.setdefault("metadata", {})
        name = meta.get("name")
        if not name:
            raise ApiError(400, "BadRequest", "metadata.name is required")
        if meta.setdefault("namespace", namespace) != namespace:
            raise ApiError(
                400,
                "BadRequest",
                "the namespace of the provided object does not match the "
                "namespace sent on the request",
            )
        if (namespace, name) in self.objects:
            raise ApiError(
                409,
                "AlreadyExists",
                'leases.coordination.k8s.io "{}" already exists'.format(name),
                name,
            )
        meta["uid"] = str(uuid.uuid4())
        meta["creationTimestamp"] = datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        obj.setdefault("apiVersion", API_VERSION)
        obj.setdefault("kind", "Lease")
        self._stamp(obj)
        self.objects[(namespace, name)] = obj
        return copy.deepcopy(obj)

    def replace(
        self, namespace: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        current = self.objects.get((namespace, name))
        obj = copy.deepcopy(body)
        meta = obj.setdefault("metadata", {})
        if meta.get("name", name) != name:
            raise ApiError(
                400,
                "BadRequest",
                "the name of the object does not match the name on the URL",
            )
        if current is None:
            # Replacing a missing Lease creates it and returns 201,
            # regardless of the resourceVersion in the request.
            meta["name"] = name
            meta.pop("resourceVersion", None)
            return self.create(namespace, obj)
        sent = meta.get("resourceVersion")
        if sent and sent != current["metadata"]["resourceVersion"]:
            raise ApiError(
                409,
                "Conflict",
                'Operation cannot be fulfilled on leases.coordination.k8s.io "'
                '{}": the object has been modified; please apply your changes '
                "to the latest version and try again".format(name),
                name,
            )
        meta["name"] = name
        meta["namespace"] = namespace
        meta["uid"] = current["metadata"]["uid"]
        meta["creationTimestamp"] = current["metadata"]["creationTimestamp"]
        obj.setdefault("apiVersion", API_VERSION)
        obj.setdefault("kind", "Lease")
        self._stamp(obj)
        self.objects[(namespace, name)] = obj
        return copy.deepcopy(obj)

    def patch(
        self, namespace: str, name: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        merged = merge_patch(self.get(namespace, name), patch)
        return self.replace(namespace, name, merged)

    def delete(self, namespace: str, name: str) -> None:
        if self.objects.pop((namespace, name), None) is None:
            raise _not_found(name)
        self._rv += 1


class FakeKubeApiserver(FakeHttpServer):
    """The Lease endpoints of an apiserver; see the module doc."""

    def __init__(
        self,
        store: Optional[LeaseStore] = None,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__(ssl_context)
        self.store = store if store is not None else LeaseStore()
        # every token the authenticator knows; empty means anonymous access
        self.tokens: set[str] = set()
        # the known tokens RBAC lets touch leases; None means all of them
        self.allowed_tokens: Optional[set[str]] = None

    def _authorize(self, request: web.Request) -> None:
        if not self.tokens:
            return
        header = request.headers.get("Authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme != "Bearer" or token not in self.tokens:
            raise ApiError(401, "Unauthorized", "Unauthorized")
        if self.allowed_tokens is not None and (
            token not in self.allowed_tokens
        ):
            raise ApiError(
                403,
                "Forbidden",
                "leases.coordination.k8s.io is forbidden: User cannot access "
                'resource "leases" in API group "coordination.k8s.io"',
            )

    def _route(
        self, request: web.Request, body: Any
    ) -> tuple[int, dict[str, Any]]:
        path = request.path
        if not path.startswith(_PREFIX):
            raise ApiError(404, "NotFound", "the server could not find it")
        parts = path[len(_PREFIX) :].split("/")
        if len(parts) not in (2, 3) or parts[1] != "leases":
            raise ApiError(404, "NotFound", "the server could not find it")
        namespace = parts[0]
        method = request.method
        if len(parts) == 2:
            if method == "POST":
                return 201, self.store.create(namespace, body or {})
            if method == "GET":
                return 200, {
                    "kind": "LeaseList",
                    "apiVersion": API_VERSION,
                    "metadata": {"resourceVersion": str(self.store._rv)},
                    "items": self.store.list(namespace),
                }
        else:
            name = parts[2]
            if method == "GET":
                return 200, self.store.get(namespace, name)
            if method == "PUT":
                existed = (namespace, name) in self.store.objects
                stored = self.store.replace(namespace, name, body or {})
                return (200 if existed else 201), stored
            if method == "PATCH":
                return 200, self.store.patch(namespace, name, body or {})
            if method == "DELETE":
                self.store.delete(namespace, name)
                return 200, {
                    "kind": "Status",
                    "apiVersion": "v1",
                    "metadata": {},
                    "status": "Success",
                    "details": {"name": name, "kind": "leases"},
                }
        raise ApiError(405, "MethodNotAllowed", "method not allowed")

    async def respond(
        self, request: web.Request, recorded: RecordedRequest
    ) -> web.StreamResponse:
        try:
            self._authorize(request)
            status, out = self._route(request, recorded.json)
        except ApiError as ex:
            return web.json_response(ex.status_object(), status=ex.code)
        return web.json_response(out, status=status)
