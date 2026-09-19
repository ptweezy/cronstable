"""The Reach codec (cronstable.reach): identity, admission, handshake,
sealed requests and records, frames, the replay window, and the shared
vectors every implementation is checked against."""

import json
import os
import stat

import pytest

from cronstable import reach
from tests import reach_vectors

pytest.importorskip("nacl", reason="pynacl (the push extra) is not installed")


def _b(text: str) -> bytes:
    return reach.b64_decode(text)


@pytest.fixture(scope="module")
def vectors() -> dict:
    return reach_vectors.build()


def test_committed_fixture_matches_the_generator(vectors):
    committed = json.loads(reach_vectors.FIXTURE.read_text())
    assert committed == vectors, (
        "tests/fixtures/reach/vectors.json is stale; run "
        "`python -m tests.reach_vectors`"
    )


def test_identity_round_trips_and_names_itself(vectors):
    ident = reach.Identity.from_json(
        {
            "v": 1,
            "signSeed": vectors["identity"]["signSeed"],
            "dhSeed": vectors["identity"]["dhSeed"],
            "salt": vectors["identity"]["salt"],
            "createdAt": "2026-09-17T00:00:00+00:00",
        }
    )
    assert ident.tunnel_id == vectors["identity"]["tunnelId"]
    assert len(ident.tunnel_id) == 43
    assert reach.tunnel_id_bytes(ident.tunnel_id) == ident.sign_public
    assert ident.fingerprint == vectors["identity"]["fingerprint"]
    assert (
        reach.Identity.from_json(ident.to_json()).sign_public
        == ident.sign_public
    )
    with pytest.raises(reach.ReachError):
        reach.tunnel_id_bytes("short")


def test_identity_file_is_private_and_exclusive(tmp_path):
    path = str(tmp_path / "reach.json")
    first = reach.ensure_identity(path)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert reach.ensure_identity(path).tunnel_id == first.tunnel_id
    with pytest.raises(reach.ReachError):
        reach.write_identity(path, reach.Identity.generate(), replace=False)
    rotated = reach.Identity.generate()
    reach.write_identity(path, rotated, replace=True)
    assert reach.load_identity(path).tunnel_id == rotated.tunnel_id
    assert not [n for n in os.listdir(tmp_path) if n.endswith(".tmp")]


def test_challenge_signature(vectors):
    ident = reach.Identity(
        _b(vectors["identity"]["signSeed"]),
        _b(vectors["identity"]["dhSeed"]),
        _b(vectors["identity"]["salt"]),
        "",
    )
    nonce = _b(vectors["challenge"]["nonce"])
    message = reach.signature_message(vectors["challenge"]["relay"], nonce)
    assert message == _b(vectors["challenge"]["message"])
    signature = ident.sign(message)
    assert signature == _b(vectors["challenge"]["signature"])
    assert reach.verify_challenge(
        ident.sign_public, "relay.cronstable.com", nonce, signature
    )
    assert not reach.verify_challenge(
        ident.sign_public, "other.example", nonce, signature
    )
    assert not reach.verify_challenge(
        ident.sign_public, "relay.cronstable.com", nonce, b"\x00" * 64
    )


def test_admission_credentials(vectors):
    salt = _b(vectors["identity"]["salt"])
    tid = _b(vectors["identity"]["signPublic"])
    for row in vectors["admission"]:
        token = reach.reach_token(row["bearer"], salt, tid)
        assert reach.b64url_encode(token) == row["reachToken"]
        assert reach.credential_header(token) == row["authorization"]
        assert reach.b64_encode(reach.admit_id(token)) == row["admitId"]
        assert reach.parse_credential(row["authorization"]) == token
    assert reach.parse_credential("Bearer abc") is None
    assert reach.parse_credential("Reach !!!") is None
    ids = reach.admit_ids(
        [r["bearer"] for r in vectors["admission"]], salt, tid
    )
    assert ids == sorted(r["admitId"] for r in vectors["admission"])


def test_handshake_matches_the_vectors(vectors):
    ident = reach.Identity(
        _b(vectors["identity"]["signSeed"]),
        _b(vectors["identity"]["dhSeed"]),
        _b(vectors["identity"]["salt"]),
        "",
    )
    hs = vectors["handshake"]
    app = reach.AppSession(ident.tunnel_id_bytes, ident.dh_public)
    hello = app.begin(
        ephemeral_seed=_b(hs["appEphemeralSeed"]), nonce=_b(hs["n1"])
    )
    assert hello == _b(hs["hello"])
    session, reply = reach.answer_hello(
        ident,
        hello,
        0.0,
        ephemeral_seed=_b(hs["daemonEphemeralSeed"]),
        nonce=_b(hs["n2"]),
        sid=_b(hs["sid"]),
    )
    assert reply == _b(hs["helloReply"])
    app.finish(reply)
    assert app.k_c2d.hex() == hs["kC2d"] == session.k_c2d.hex()
    assert app.k_d2c.hex() == hs["kD2c"] == session.k_d2c.hex()
    assert app.sid == session.sid == _b(hs["sid"])
    assert app.ttl == 1800


def test_hello_to_the_wrong_daemon_fails_closed(vectors):
    ident = reach.Identity.generate()
    other = reach.Identity.generate()
    app = reach.AppSession(ident.tunnel_id_bytes, ident.dh_public)
    hello = app.begin()
    with pytest.raises(reach.DecryptError):
        reach.answer_hello(other, hello, 0.0)
    with pytest.raises(reach.ReachError):
        reach.answer_hello(ident, b"\x02" + hello[1:], 0.0)


def test_requests_and_records_match_the_vectors(vectors):
    ident = reach.Identity(
        _b(vectors["identity"]["signSeed"]),
        _b(vectors["identity"]["dhSeed"]),
        _b(vectors["identity"]["salt"]),
        "",
    )
    hs = vectors["handshake"]
    app = reach.AppSession(ident.tunnel_id_bytes, ident.dh_public)
    hello = app.begin(
        ephemeral_seed=_b(hs["appEphemeralSeed"]), nonce=_b(hs["n1"])
    )
    session, reply = reach.answer_hello(
        ident,
        hello,
        0.0,
        ephemeral_seed=_b(hs["daemonEphemeralSeed"]),
        nonce=_b(hs["n2"]),
        sid=_b(hs["sid"]),
    )
    app.finish(reply)
    for row in vectors["requests"]:
        sealed = app.seal_request_raw(
            row["head"].encode(), _b(row["body"]), ctr=row["ctr"]
        )
        assert sealed == _b(row["sealed"])
        assert reach.encode_request_plaintext_raw(
            row["head"].encode(), _b(row["body"])
        ) == _b(row["plaintext"])
        ctr, head, body = reach.open_request(session, sealed)
        assert ctr == row["ctr"]
        assert head == json.loads(row["head"])
        assert body == _b(row["body"])
    with pytest.raises(reach.ReplayError):
        reach.open_request(session, _b(vectors["requests"][0]["sealed"]))

    res = vectors["response"]
    assert reach.encode_head_record_raw(res["head"].encode()) == _b(
        res["headRecordData"]
    )
    assert len(_b(res["headRecordData"])) % 256 == 0
    status, headers = reach.decode_head_record(_b(res["headRecordData"]))
    assert status == 200 and headers == [
        ("content-type", "application/json"),
        ("etag", '"abc"'),
    ]
    for row in res["records"]:
        chunk = reach.seal_record(
            session, row["ctr"], row["idx"], row["kind"], _b(row["data"])
        )
        assert chunk == _b(row["chunk"])
        ctr, idx, kind, flags, data = app.open_record(chunk)
        assert (ctr, idx, kind, flags, data) == (
            row["ctr"],
            row["idx"],
            row["kind"],
            0,
            _b(row["data"]),
        )
    err = res["errorRecord"]
    assert reach.seal_record(
        session, err["ctr"], err["idx"], err["kind"], _b(err["data"])
    ) == _b(err["chunk"])
    assert reach.NOSESSION_CHUNK == _b(res["nosession"])

    reader = reach.ChunkReader()
    stream = _b(res["stream"])
    chunks = []
    for i in range(0, len(stream), 7):  # deliberately awkward splits
        chunks.extend(reader.feed(stream[i : i + 7]))
    assert chunks == [_b(r["chunk"]) for r in res["records"]]
    assert reader.pending == 0


def test_record_tampering_is_refused(vectors):
    ident = reach.Identity.generate()
    app = reach.AppSession(ident.tunnel_id_bytes, ident.dh_public)
    session, reply = reach.answer_hello(ident, app.begin(), 0.0)
    app.finish(reply)
    chunk = bytearray(reach.seal_record(session, 1, 0, reach.KIND_END))
    chunk[-1] ^= 1
    with pytest.raises(reach.DecryptError):
        app.open_record(bytes(chunk))
    # a record replayed under another index does not open either
    good = reach.seal_record(session, 1, 0, reach.KIND_END)
    moved = good[:9] + b"\x00\x00\x00\x01" + good[13:]
    with pytest.raises(reach.DecryptError):
        app.open_record(moved)


def test_frames_match_the_vectors(vectors):
    for row in vectors["frames"]:
        packed = reach.pack_frame(
            row["type"], row["reqId"], _b(row["payload"])
        )
        assert packed == _b(row["bytes"]), row["name"]
        assert reach.unpack_frame(packed) == (
            row["type"],
            row["reqId"],
            _b(row["payload"]),
        )
    assert reach.pack_cancel(7, reach.CANCEL_STREAM_CAP) == _b(
        vectors["frames"][3]["bytes"]
    )
    assert reach.pack_reject(7, reach.REJECT_REPLAY, "replay") == _b(
        vectors["frames"][4]["bytes"]
    )
    assert reach.pack_window(7, 262144) == _b(vectors["frames"][5]["bytes"])
    with pytest.raises(reach.ReachError):
        reach.unpack_frame(b"\x01\x00")
    with pytest.raises(reach.ReachError):
        reach.pack_frame(
            reach.FRAME_DATA, 1, bytes(reach.MAX_FRAME_PAYLOAD + 1)
        )


def test_replay_window_semantics():
    window = reach.ReplayWindow(width=8)
    window.accept(5)
    for used in (5,):
        with pytest.raises(reach.ReplayError):
            window.accept(used)
    window.accept(3)  # inside the window, unseen
    with pytest.raises(reach.ReplayError):
        window.accept(3)
    window.accept(12)  # jumps ahead; 5 and 3 now age out
    with pytest.raises(reach.ReplayError):
        window.accept(4)  # 12 - 4 = 8 >= width
    window.accept(11)
    with pytest.raises(reach.ReplayError):
        window.accept(0)
    far = reach.ReplayWindow(width=8)
    far.accept(1)
    far.accept(1_000_000)
    far.accept(999_999)
    with pytest.raises(reach.ReplayError):
        far.accept(999_999)


def test_session_table_evicts_and_expires():
    table = reach.SessionTable(capacity=2)
    a = reach.DaemonSession(b"a" * 16, b"k" * 32, b"k" * 32, now=0.0)
    b = reach.DaemonSession(b"b" * 16, b"k" * 32, b"k" * 32, now=0.0)
    c = reach.DaemonSession(b"c" * 16, b"k" * 32, b"k" * 32, now=0.0)
    table.add(a)
    table.add(b)
    assert table.get(a.sid, 1.0) is a  # touched: most recent now
    table.add(c)  # evicts b, the least recently used
    assert table.get(b.sid, 1.0) is None
    assert table.get(a.sid, 1.0) is a
    assert table.get(c.sid, reach.SESSION_IDLE_SECONDS + 2.0) is None
    assert len(table) == 1
    old = reach.DaemonSession(b"o" * 16, b"k" * 32, b"k" * 32, now=0.0)
    table.add(old)
    table.sweep(reach.SESSION_ABSOLUTE_SECONDS + 1.0)
    assert len(table) == 0
