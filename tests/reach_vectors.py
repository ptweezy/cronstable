"""The shared Reach vectors (``tests/fixtures/reach/vectors.json``).

``build()`` derives every vector from fixed seeds and nonces through the
codec in :mod:`cronstable.reach`, so the fixture is a function of the
protocol alone.  ``tests/test_reach_codec.py`` asserts the committed file
equals what ``build()`` produces; the relay and the companion app read the
same file, which is how three implementations in three languages agree on
every byte.  Regenerate with ``python -m tests.reach_vectors``.

Vectors are authored here, in the MIT repository, and ported outward:
never the other direction.
"""

import hashlib
import json
import pathlib
import sys

from cronstable import reach

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "reach" / "vectors.json"

RELAY_HOST = "relay.cronstable.com"
BEARERS = [
    "cronstable-public-demo-view",
    "",
    "x" * 200,
    "tök€n ünïcode",
]
REQUEST_HEAD = (
    '{"v":1,"m":"GET","u":"/jobs?limit=5","a":"nas.local:8080",'
    '"h":[["authorization","Bearer cronstable-public-demo-view"],'
    '["accept","application/json"],["accept-encoding","identity"]]}'
)
POST_HEAD = (
    '{"v":1,"m":"POST","u":"/jobs/backup/pause","a":"nas.local:8080",'
    '"h":[["authorization","Bearer cronstable-public-demo-view"],'
    '["content-type","application/json"],'
    '["accept-encoding","identity"]]}'
)
POST_BODY = b'{"by":"vector","durationSeconds":60}'
RESPONSE_HEAD = (
    '{"s":200,"h":[["content-type","application/json"],["etag","\\"abc\\""]]}'
)
RESPONSE_BODY = b'{"jobs":[]}'
ERROR_DATA = b'{"error":"session busy","status":429}'


def _seed(tag: str) -> bytes:
    return hashlib.sha256(("reach-vector-" + tag).encode("ascii")).digest()


def _b64(raw: bytes) -> str:
    return reach.b64_encode(raw)


def build() -> dict:
    identity = reach.Identity(
        _seed("sign-seed"),
        _seed("dh-seed"),
        _seed("salt")[:16],
        "2026-09-17T00:00:00+00:00",
    )
    tid = identity.tunnel_id_bytes
    nonce = _seed("nonce")
    signature = identity.sign(reach.signature_message(RELAY_HOST, nonce))

    admission = []
    for bearer in BEARERS:
        token = reach.reach_token(bearer, identity.salt, tid)
        admission.append(
            {
                "bearer": bearer,
                "reachToken": reach.b64url_encode(token),
                "authorization": reach.credential_header(token),
                "admitId": _b64(reach.admit_id(token)),
            }
        )

    app = reach.AppSession(tid, identity.dh_public)
    hello = app.begin(ephemeral_seed=_seed("e"), nonce=_seed("n1")[:12])
    session, reply = reach.answer_hello(
        identity,
        hello,
        1_000.0,
        ephemeral_seed=_seed("f"),
        nonce=_seed("n2")[:12],
        sid=_seed("sid")[:16],
    )
    app.finish(reply)
    assert app.k_c2d == session.k_c2d and app.k_d2c == session.k_d2c

    req1 = app.seal_request_raw(REQUEST_HEAD.encode("utf-8"), b"", ctr=1)
    req2 = app.seal_request_raw(POST_HEAD.encode("utf-8"), POST_BODY, ctr=2)
    for body in (req1, req2):
        ctr, head, inner = reach.open_request(session, body)
        assert head["v"] == 1 and ctr in (1, 2)

    head_data = reach.encode_head_record_raw(RESPONSE_HEAD.encode("utf-8"))
    records = [
        (1, 0, reach.KIND_HEAD, head_data),
        (1, 1, reach.KIND_BODY, RESPONSE_BODY),
        (1, 2, reach.KIND_END, b""),
    ]
    sealed_records = [
        {
            "ctr": ctr,
            "idx": idx,
            "kind": kind,
            "flags": 0,
            "data": _b64(data),
            "chunk": _b64(reach.seal_record(session, ctr, idx, kind, data)),
        }
        for ctr, idx, kind, data in records
    ]
    error_chunk = reach.seal_record(
        session, 2, 0, reach.KIND_ERROR, ERROR_DATA
    )
    stream = b"".join(
        reach.pack_chunk(reach.b64_decode(r["chunk"])) for r in sealed_records
    )

    frames = [
        {
            "name": "open",
            "type": reach.FRAME_OPEN,
            "reqId": 7,
            "payload": _b64(hello),
        },
        {
            "name": "data",
            "type": reach.FRAME_DATA,
            "reqId": 7,
            "payload": _b64(
                reach.pack_chunk(reach.b64_decode(sealed_records[0]["chunk"]))
            ),
        },
        {"name": "end", "type": reach.FRAME_END, "reqId": 7, "payload": ""},
        {
            "name": "cancel",
            "type": reach.FRAME_CANCEL,
            "reqId": 7,
            "payload": _b64(b"\x00\x02"),
        },
        {
            "name": "reject",
            "type": reach.FRAME_REJECT,
            "reqId": 7,
            "payload": _b64(b"\x00\x02" + b"replay"),
        },
        {
            "name": "window",
            "type": reach.FRAME_WINDOW,
            "reqId": 7,
            "payload": _b64(b"\x00\x04\x00\x00"),
        },
    ]
    for frame in frames:
        frame["bytes"] = _b64(
            reach.pack_frame(
                frame["type"],
                frame["reqId"],
                reach.b64_decode(frame["payload"]),
            )
        )

    return {
        "v": 1,
        "label": reach.LABEL.decode("ascii"),
        "identity": {
            "signSeed": _b64(identity.sign_seed),
            "dhSeed": _b64(identity.dh_seed),
            "salt": _b64(identity.salt),
            "signPublic": _b64(identity.sign_public),
            "tunnelId": identity.tunnel_id,
            "fingerprint": identity.fingerprint,
            "dhPublic": _b64(identity.dh_public),
        },
        "challenge": {
            "relay": RELAY_HOST,
            "nonce": _b64(nonce),
            "message": _b64(reach.signature_message(RELAY_HOST, nonce)),
            "signature": _b64(signature),
        },
        "admission": admission,
        "handshake": {
            "appEphemeralSeed": _b64(_seed("e")),
            "appEphemeralPublic": _b64(app.e_pk),
            "daemonEphemeralSeed": _b64(_seed("f")),
            "n1": _b64(_seed("n1")[:12]),
            "n2": _b64(_seed("n2")[:12]),
            "sid": _b64(_seed("sid")[:16]),
            "ttl": 1800,
            "hello": _b64(hello),
            "helloPlaintext": _b64(b'{"v":1}'),
            "helloReply": _b64(reply),
            "helloReplyPlaintext": _b64(
                json.dumps(
                    {"v": 1, "sid": _b64(_seed("sid")[:16]), "ttl": 1800},
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
            "tx1": app.tx1.hex(),
            "rx1": app.rx1.hex(),
            "tx2": app.tx2.hex(),
            "rx2": app.rx2.hex(),
            "kC2d": app.k_c2d.hex(),
            "kD2c": app.k_d2c.hex(),
        },
        "requests": [
            {
                "ctr": 1,
                "head": REQUEST_HEAD,
                "body": "",
                "plaintext": _b64(
                    reach.encode_request_plaintext_raw(
                        REQUEST_HEAD.encode(), b""
                    )
                ),
                "sealed": _b64(req1),
            },
            {
                "ctr": 2,
                "head": POST_HEAD,
                "body": _b64(POST_BODY),
                "plaintext": _b64(
                    reach.encode_request_plaintext_raw(
                        POST_HEAD.encode(), POST_BODY
                    )
                ),
                "sealed": _b64(req2),
            },
        ],
        "response": {
            "head": RESPONSE_HEAD,
            "headRecordData": _b64(head_data),
            "records": sealed_records,
            "stream": _b64(stream),
            "errorRecord": {
                "ctr": 2,
                "idx": 0,
                "kind": reach.KIND_ERROR,
                "data": _b64(ERROR_DATA),
                "chunk": _b64(error_chunk),
            },
            "nosession": _b64(reach.NOSESSION_CHUNK),
        },
        "frames": frames,
    }


def main() -> int:
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(build(), indent=2, sort_keys=True) + "\n")
    print("wrote", FIXTURE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
