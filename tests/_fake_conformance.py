"""Check HTTP behavior shared by fake and real backend servers.

Each function accepts an HTTP request function and checks behavior that
the backends depend on. The transport suites run these checks against
``tests/_fake_etcd.py`` and ``tests/_fake_kube_apiserver.py``.
``tests/test_backend_live.py`` runs the same checks against real etcd and
Kubernetes API servers. A mismatch fails the live backend tests.
"""

import base64
from collections.abc import Awaitable, Callable
from typing import Any

#: ``await call(path, body) -> (status, parsed JSON)`` against etcd's gateway.
EtcdCall = Callable[[str, dict[str, Any]], Awaitable[tuple[int, Any]]]
#: ``await call(method, path, body) -> (status, parsed JSON)``, apiserver.
KubeCall = Callable[[str, str, Any], Awaitable[tuple[int, Any]]]


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


async def check_etcd_gateway(call: EtcdCall, prefix: str) -> None:
    """The JSON-gateway shapes the etcd backend parses.

    Works under ``prefix`` only and deletes what it wrote.
    """
    key = _b64((prefix + "/conformance").encode())
    try:
        # an absent key: no kvs, no count (proto3 zero values are omitted)
        status, empty = await call("/v3/kv/range", {"key": key})
        assert status == 200
        assert not empty.get("kvs") and "count" not in empty
        assert isinstance(empty["header"]["revision"], str)

        status, grant = await call("/v3/lease/grant", {"TTL": "30"})
        assert status == 200
        lease = grant["ID"]
        # int64s are JSON strings, and the TTL is never below the request
        assert isinstance(lease, str) and int(lease) > 0
        assert isinstance(grant["TTL"], str) and int(grant["TTL"]) >= 30

        campaign = {
            "compare": [
                {
                    "key": key,
                    "result": "EQUAL",
                    "target": "CREATE",
                    "create_revision": "0",
                }
            ],
            "success": [
                {
                    "requestPut": {
                        "key": key,
                        "value": _b64(b"first"),
                        "lease": lease,
                    }
                }
            ],
            "failure": [{"requestRange": {"key": key}}],
        }
        status, won = await call("/v3/kv/txn", campaign)
        assert status == 200 and won["succeeded"] is True

        # the same txn again fails its compare: "succeeded" is omitted, and
        # the failure branch's range names the holder and its lease
        status, lost = await call("/v3/kv/txn", campaign)
        assert status == 200 and not lost.get("succeeded")
        [entry] = lost["responses"]
        kv = (entry.get("response_range") or entry["responseRange"])["kvs"][0]
        assert base64.b64decode(kv["value"]) == b"first"
        assert kv["lease"] == lease
        mod_revision = kv.get("mod_revision") or kv["modRevision"]
        assert isinstance(mod_revision, str)

        # a MOD-revision compare-and-swap: stale loses, current wins
        def cas(revision: str) -> dict[str, Any]:
            return {
                "compare": [
                    {
                        "key": key,
                        "result": "EQUAL",
                        "target": "MOD",
                        "mod_revision": revision,
                    }
                ],
                "success": [
                    {"requestPut": {"key": key, "value": _b64(b"second")}}
                ],
                "failure": [],
            }

        status, stale = await call("/v3/kv/txn", cas("1"))
        assert status == 200 and not stale.get("succeeded")
        status, fresh = await call("/v3/kv/txn", cas(mod_revision))
        assert status == 200 and fresh["succeeded"] is True
        # that put carried no lease, so the key is detached from it
        status, now = await call("/v3/kv/range", {"key": key})
        [kv_now] = now["kvs"]
        assert "lease" not in kv_now
        moved = kv_now.get("mod_revision") or kv_now["modRevision"]
        assert int(moved) > int(mod_revision)

        status, alive = await call("/v3/lease/keepalive", {"ID": lease})
        assert status == 200 and int(alive["result"]["TTL"]) > 0
        status, _ = await call("/v3/lease/revoke", {"ID": lease})
        assert status == 200
        # a keepalive for a lease that is gone: 200, with no (or a zero) TTL
        status, gone = await call("/v3/lease/keepalive", {"ID": lease})
        assert status == 200
        assert int((gone.get("result") or {}).get("TTL") or 0) == 0
        # revoking it again is a 404
        status, _ = await call("/v3/lease/revoke", {"ID": lease})
        assert status == 404
    finally:
        await call("/v3/kv/deleterange", {"key": key})


async def check_lease_api(call: KubeCall, namespace: str, name: str) -> None:
    """The Lease REST shapes the HTTP transport maps to results.

    Creates and deletes the Lease ``namespace``/``name``.
    """
    collection = "/apis/coordination.k8s.io/v1/namespaces/{}/leases".format(
        namespace
    )
    one = collection + "/" + name

    def body(holder: str, resource_version: str = "") -> dict[str, Any]:
        metadata = {"name": name, "namespace": namespace}
        if resource_version:
            metadata["resourceVersion"] = resource_version
        return {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": metadata,
            "spec": {"holderIdentity": holder, "leaseDurationSeconds": 15},
        }

    try:
        status, missing = await call("GET", one, None)
        assert status == 404
        assert (missing["kind"], missing["reason"]) == ("Status", "NotFound")
        assert missing["code"] == 404 and missing["status"] == "Failure"
    # Replacing a missing Lease creates it and returns 201, even when the
    # request specifies a resourceVersion. This is create-on-update behavior.
        status, made = await call("PUT", one, body("nobody", "1"))
        assert status == 201
        assert made["spec"]["holderIdentity"] == "nobody"
        assert made["metadata"]["resourceVersion"] != "1"
        status, _ = await call("DELETE", one, None)
        assert status == 200

        status, created = await call("POST", collection, body("first"))
        assert status == 201
        version = created["metadata"]["resourceVersion"]
        assert isinstance(version, str) and version
        assert created["metadata"]["uid"]

        status, exists = await call("POST", collection, body("second"))
        assert (status, exists["reason"]) == (409, "AlreadyExists")

        status, replaced = await call("PUT", one, body("second", version))
        assert status == 200
        assert replaced["metadata"]["resourceVersion"] != version
        assert replaced["metadata"]["uid"] == created["metadata"]["uid"]

        status, conflict = await call("PUT", one, body("third", version))
        assert (status, conflict["reason"]) == (409, "Conflict")

        status, seen = await call("GET", one, None)
        assert status == 200 and seen["spec"]["holderIdentity"] == "second"
    finally:
        await call("DELETE", one, None)
