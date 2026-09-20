"""Simulate the etcd v3 JSON gateway over HTTP with an in-memory store.

``FakeEtcd`` represents one cluster member. It uses the aiohttp server in
``tests/_fake_http.py`` to implement the JSON gateway that the backend calls.
Members can share an ``EtcdStore`` so failover tests use consistent data.

Match the real gateway in these ways:

* Encode keys and values as base64. Represent int64 values, including
  revisions, lease IDs, TTLs, and counts, as JSON strings.
* Omit proto3 zero values: failed transactions omit ``succeeded``, empty
  ranges omit ``kvs`` and ``count``, keys without leases omit ``lease``,
  and keepalive responses for missing leases omit ``TTL``.
* Use snake_case field names such as ``response_range`` and ``mod_revision``.
  With ``camel_case=True``, use the camelCase names produced by some gateways.
* Return error objects with ``error``, ``code``, and ``message`` fields.
  Map gRPC codes to HTTP statuses: invalid or expired tokens return ``401``;
  requests without a required token return ``400`` ("user name is empty");
  failed authentication returns ``400``; unknown leases return ``404``.

The store tracks a global revision, each key's creation and modification
revisions and version, and leases that expire using an injectable clock.
Expiring or revoking a lease deletes its keys and advances the revision for
each deletion.

The test-only setting ``EtcdStore.grant_ttl_cap`` limits granted and renewed
leases to a shorter TTL than requested. This checks that the backend reduces
its leadership deadline accordingly. Real etcd can increase a requested TTL
to its minimum but does not reduce it this way.
"""

import base64
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional

from aiohttp import web

from tests._fake_http import FakeClock, FakeHttpServer, RecordedRequest

_FIRST_LEASE_ID = 7587862072907364000

_GRPC_INVALID_ARGUMENT = 3
_GRPC_NOT_FOUND = 5
_GRPC_UNAUTHENTICATED = 16
_GRPC_UNIMPLEMENTED = 12

_CAMEL = {
    "create_revision": "createRevision",
    "mod_revision": "modRevision",
    "response_range": "responseRange",
    "response_put": "responsePut",
    "response_delete_range": "responseDeleteRange",
    "cluster_id": "clusterId",
    "member_id": "memberId",
    "raft_term": "raftTerm",
    "prev_kvs": "prevKvs",
}


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def unb64(text: Any) -> bytes:
    return base64.b64decode(text or "")


class EtcdError(Exception):
    def __init__(self, status: int, code: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


@dataclass
class _KeyValue:
    create_revision: int
    mod_revision: int
    version: int
    value: bytes
    lease: int


@dataclass
class _Lease:
    ttl: int
    deadline: float
    keys: set[bytes] = field(default_factory=set)


class EtcdStore:
    """The data one or more :class:`FakeEtcd` members serve."""

    def __init__(self, clock: Optional[Callable[[], float]] = None) -> None:
        self.clock: Callable[[], float] = clock or FakeClock()
        self.revision = 1
        self.kvs: dict[bytes, _KeyValue] = {}
        self.leases: dict[int, _Lease] = {}
        self._next_lease = _FIRST_LEASE_ID
        self.auth_enabled = False
        self.users: dict[str, str] = {}
        self.tokens: dict[str, str] = {}
        self._next_token = 0
        self.grant_ttl_cap: Optional[int] = None

    # --- auth -------------------------------------------------------------

    def enable_auth(self, name: str, password: str) -> None:
        self.users[name] = password
        self.auth_enabled = True

    def expire_tokens(self) -> None:
        """Invalidate every issued token, as the token TTL lapsing does."""
        self.tokens.clear()

    def authenticate(self, name: Any, password: Any) -> str:
        if not self.auth_enabled:
            raise EtcdError(
                400,
                _GRPC_INVALID_ARGUMENT,
                "etcdserver: authentication is not enabled",
            )
        if name not in self.users or self.users[name] != password:
            raise EtcdError(
                400,
                _GRPC_INVALID_ARGUMENT,
                "etcdserver: authentication failed, invalid user ID or "
                "password",
            )
        self._next_token += 1
        token = "fake-token-{}.{}".format(name, self._next_token)
        self.tokens[token] = name
        return token

    def check_token(self, token: Optional[str]) -> None:
        if not self.auth_enabled:
            return
        if not token:
            raise EtcdError(
                400, _GRPC_INVALID_ARGUMENT, "etcdserver: user name is empty"
            )
        if token not in self.tokens:
            raise EtcdError(
                401, _GRPC_UNAUTHENTICATED, "etcdserver: invalid auth token"
            )

    # --- leases -----------------------------------------------------------

    def expire_leases(self) -> None:
        now = self.clock()
        for lease_id in [
            i for i, lease in self.leases.items() if lease.deadline <= now
        ]:
            self._drop_lease(lease_id)

    def _drop_lease(self, lease_id: int) -> None:
        lease = self.leases.pop(lease_id)
        for key in sorted(lease.keys):
            self._delete(key)

    def _served_ttl(self, requested: int) -> int:
        if self.grant_ttl_cap is not None:
            return min(requested, self.grant_ttl_cap)
        return requested

    def lease_grant(self, ttl: int) -> tuple[int, int]:
        self._next_lease += 1
        granted = self._served_ttl(ttl)
        self.leases[self._next_lease] = _Lease(
            ttl=granted, deadline=self.clock() + granted
        )
        return self._next_lease, granted

    def lease_keepalive(self, lease_id: int) -> Optional[int]:
        lease = self.leases.get(lease_id)
        if lease is None:
            return None
        lease.ttl = self._served_ttl(lease.ttl)
        lease.deadline = self.clock() + lease.ttl
        return lease.ttl

    def lease_revoke(self, lease_id: int) -> None:
        if lease_id not in self.leases:
            raise EtcdError(
                404,
                _GRPC_NOT_FOUND,
                "etcdserver: requested lease not found",
            )
        self._drop_lease(lease_id)

    # --- kv ---------------------------------------------------------------

    def _select(self, key: bytes, range_end: bytes) -> list[bytes]:
        if not range_end:
            return [key] if key in self.kvs else []
        if range_end == b"\0":
            return sorted(k for k in self.kvs if k >= key)
        return sorted(k for k in self.kvs if key <= k < range_end)

    def range(self, key: bytes, range_end: bytes = b"") -> list[bytes]:
        return self._select(key, range_end)

    def put(self, key: bytes, value: bytes, lease_id: int = 0) -> None:
        if lease_id and lease_id not in self.leases:
            raise EtcdError(
                404,
                _GRPC_NOT_FOUND,
                "etcdserver: requested lease not found",
            )
        self.revision += 1
        existing = self.kvs.get(key)
        if existing is not None and existing.lease in self.leases:
            self.leases[existing.lease].keys.discard(key)
        self.kvs[key] = _KeyValue(
            create_revision=(
                existing.create_revision if existing else self.revision
            ),
            mod_revision=self.revision,
            version=(existing.version + 1) if existing else 1,
            value=value,
            lease=lease_id,
        )
        if lease_id:
            self.leases[lease_id].keys.add(key)

    def _delete(self, key: bytes) -> None:
        existing = self.kvs.pop(key)
        if existing.lease in self.leases:
            self.leases[existing.lease].keys.discard(key)
        self.revision += 1

    def delete_range(self, key: bytes, range_end: bytes = b"") -> int:
        doomed = self._select(key, range_end)
        for k in doomed:
            self._delete(k)
        return len(doomed)

    def compare(self, cmp: dict[str, Any]) -> bool:
        key = unb64(cmp.get("key"))
        kv = self.kvs.get(key)
        target = str(cmp.get("target", "VERSION")).upper()
        actual: Any
        expected: Any
        if target == "VALUE":
            actual = kv.value if kv else b""
            expected = unb64(cmp.get("value"))
        else:
            field_name, attr = {
                "CREATE": ("create_revision", "create_revision"),
                "MOD": ("mod_revision", "mod_revision"),
                "VERSION": ("version", "version"),
                "LEASE": ("lease", "lease"),
            }[target]
            camel = _CAMEL.get(field_name, field_name)
            expected = int(cmp.get(field_name, cmp.get(camel, 0)) or 0)
            actual = getattr(kv, attr) if kv else 0
        result = str(cmp.get("result", "EQUAL")).upper()
        if result == "EQUAL":
            return bool(actual == expected)
        if result == "NOT_EQUAL":
            return bool(actual != expected)
        if result == "GREATER":
            return bool(actual > expected)
        if result == "LESS":
            return bool(actual < expected)
        raise EtcdError(
            400, _GRPC_INVALID_ARGUMENT, "unknown compare result " + result
        )


class FakeEtcd(FakeHttpServer):
    """One etcd member's JSON gateway; see the module doc."""

    def __init__(
        self,
        store: Optional[EtcdStore] = None,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        camel_case: bool = False,
    ) -> None:
        super().__init__(ssl_context)
        self.store = store if store is not None else EtcdStore()
        self.camel_case = camel_case

    @property
    def endpoint(self) -> str:
        return self.url

    # --- wire shaping -----------------------------------------------------

    def _header(self) -> dict[str, str]:
        return {
            "cluster_id": "14841639068965178418",
            "member_id": "10276657743932975437",
            "revision": str(self.store.revision),
            "raft_term": "2",
        }

    def _kv_json(self, key: bytes) -> dict[str, str]:
        kv = self.store.kvs[key]
        out = {
            "key": b64(key),
            "create_revision": str(kv.create_revision),
            "mod_revision": str(kv.mod_revision),
            "version": str(kv.version),
            "value": b64(kv.value),
        }
        if kv.lease:
            out["lease"] = str(kv.lease)
        return out

    def _range_json(self, req: dict[str, Any]) -> dict[str, Any]:
        keys = self.store.range(
            unb64(req.get("key")),
            unb64(req.get("range_end", req.get("rangeEnd"))),
        )
        out: dict[str, Any] = {"header": self._header()}
        if keys:
            out["kvs"] = [self._kv_json(k) for k in keys]
            out["count"] = str(len(keys))
        return out

    def _put_json(self, req: dict[str, Any]) -> dict[str, Any]:
        self.store.put(
            unb64(req.get("key")),
            unb64(req.get("value")),
            int(req.get("lease") or 0),
        )
        return {"header": self._header()}

    def _delete_json(self, req: dict[str, Any]) -> dict[str, Any]:
        deleted = self.store.delete_range(
            unb64(req.get("key")),
            unb64(req.get("range_end", req.get("rangeEnd"))),
        )
        out: dict[str, Any] = {"header": self._header()}
        if deleted:
            out["deleted"] = str(deleted)
        return out

    def _txn_json(self, req: dict[str, Any]) -> dict[str, Any]:
        succeeded = all(
            self.store.compare(c) for c in req.get("compare") or []
        )
        responses: list[dict[str, Any]] = []
        for op in req.get("success" if succeeded else "failure") or []:
            if "requestPut" in op or "request_put" in op:
                body = op.get("requestPut") or op.get("request_put")
                responses.append({"response_put": self._put_json(body)})
            elif "requestRange" in op or "request_range" in op:
                body = op.get("requestRange") or op.get("request_range")
                responses.append({"response_range": self._range_json(body)})
            elif "requestDeleteRange" in op or "request_delete_range" in op:
                body = op.get("requestDeleteRange") or op.get(
                    "request_delete_range"
                )
                responses.append(
                    {"response_delete_range": self._delete_json(body)}
                )
            else:
                raise EtcdError(
                    400, _GRPC_INVALID_ARGUMENT, "unknown txn request op"
                )
        out: dict[str, Any] = {"header": self._header()}
        if succeeded:
            out["succeeded"] = True
        if responses:
            out["responses"] = responses
        return out

    def _camelize(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                _CAMEL.get(k, k): self._camelize(v) for k, v in value.items()
            }
        if isinstance(value, list):
            return [self._camelize(v) for v in value]
        return value

    # --- dispatch ---------------------------------------------------------

    def _dispatch(self, path: str, req: dict[str, Any]) -> dict[str, Any]:
        store = self.store
        if path == "/v3/kv/range":
            return self._range_json(req)
        if path == "/v3/kv/put":
            return self._put_json(req)
        if path == "/v3/kv/deleterange":
            return self._delete_json(req)
        if path == "/v3/kv/txn":
            return self._txn_json(req)
        if path == "/v3/lease/grant":
            lease_id, ttl = store.lease_grant(int(req.get("TTL") or 0))
            return {
                "header": self._header(),
                "ID": str(lease_id),
                "TTL": str(ttl),
            }
        if path == "/v3/lease/keepalive":
            lease_id = int(req.get("ID") or 0)
            ttl_left = store.lease_keepalive(lease_id)
            result = {"header": self._header(), "ID": str(lease_id)}
            if ttl_left:
                result["TTL"] = str(ttl_left)
            return {"result": result}
        if path in ("/v3/lease/revoke", "/v3/kv/lease/revoke"):
            store.lease_revoke(int(req.get("ID") or 0))
            return {"header": self._header()}
        raise EtcdError(404, _GRPC_UNIMPLEMENTED, "Not Found")

    async def respond(
        self, request: web.Request, recorded: RecordedRequest
    ) -> web.StreamResponse:
        req = recorded.json if isinstance(recorded.json, dict) else {}
        try:
            if request.method != "POST":
                raise EtcdError(405, _GRPC_UNIMPLEMENTED, "Method Not Allowed")
            self.store.expire_leases()
            if request.path == "/v3/auth/authenticate":
                token = self.store.authenticate(
                    req.get("name"), req.get("password")
                )
                out: dict[str, Any] = {
                    "header": self._header(),
                    "token": token,
                }
            else:
                self.store.check_token(request.headers.get("Authorization"))
                out = self._dispatch(request.path, req)
        except EtcdError as ex:
            return web.json_response(
                {"error": ex.message, "code": ex.code, "message": ex.message},
                status=ex.status,
            )
        if self.camel_case:
            out = self._camelize(out)
        return web.json_response(out)
