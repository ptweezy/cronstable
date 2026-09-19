# Reach protocol (v1)

Reach is a private control channel between the companion app and a
cronstable daemon that runs through the push relay. The daemon dials out
to the relay and holds one WebSocket open. The app sends ordinary HTTPS
requests to the relay. The relay matches the two and copies bytes between
them. Every request and every response is sealed between the app and the
daemon, so the relay carries ciphertext and routing metadata only: the
same posture the push path in [`relay-protocol.md`](relay-protocol.md)
holds for alerts. The push envelope is unchanged by this document.

This document is the wire contract. The daemon (`cronstable/reach.py`),
the hosted relay ([ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay)),
and the companion app implement it. Anything that implements it can serve
as the relay a daemon's `web.reach.relay` points at. The cryptographic
rationale lives in [`reach-crypto-review.md`](reach-crypto-review.md).

## Roles

- The daemon generates one identity per node, connects to the relay,
  proves the identity with a signature over a relay nonce, publishes which
  bearer-derived credentials the relay may admit, and serves every relayed
  request through its own web app, so authentication, scopes, the error
  envelope, and access logging apply unchanged.
- The relay keeps one object per tunnel. It admits or refuses an app
  request, forwards the sealed body to the daemon as one frame, and streams
  the daemon's sealed reply frames back as the HTTP response body. It never
  parses the inside of a frame.
- The app seals each HTTP request, posts it to the relay, and unseals the
  streamed reply. The app talks to the daemon directly whenever it can and
  uses Reach when the daemon's own address is unreachable.

## Constants and encodings

| Name | Value |
| --- | --- |
| `L` | The ASCII bytes `cronstable-reach-v1` (19 bytes). Every key derivation and every associated-data string starts with it. |
| Subprotocol | `cronstable-reach-v1` |
| base64url | RFC 4648 URL-safe alphabet, padding stripped. Used for the tunnel id and the `Authorization` credential. |
| base64 | RFC 4648 standard alphabet with padding. Used inside JSON messages. |
| `u16`, `u32`, `u64` | Unsigned big-endian integers of that width. |

Strings inside `‖` concatenations are UTF-8 with no length prefix and no
terminator unless the layout says otherwise.

## Daemon identity

The daemon keeps one identity file per node (`web.reach.keyFile`, mode
`0600`, created exclusively):

```json
{"v": 1, "signSeed": "<base64 32 bytes>", "dhSeed": "<base64 32 bytes>", "salt": "<base64 16 bytes>", "createdAt": "2026-09-17T00:00:00+00:00"}
```

- `signSeed` seeds an Ed25519 keypair. The **tunnel id** is the base64url
  form of the 32-byte public key: 43 characters, path-safe. The id is the
  key, so the relay verifies possession without storing anything.
- `dhSeed` seeds an X25519 keypair through libsodium's
  `crypto_kx_seed_keypair`. The public half is the daemon's static channel
  key `D`, which the app seals sessions to. Two keys rather than one
  converted key keep the signing and key-agreement domains separate.
- `salt` is 16 random bytes that key the app's relay credential. It travels
  only in the pairing payload and in this file.

The **fingerprint** is the first 12 hex characters of SHA-256 over the raw
Ed25519 public key, grouped `xxxx-xxxx-xxxx`, the same derivation the push
registry uses for device keys. The dashboard's pairing panel and the app's
remote-access screen show it.

The secret half never leaves the node and is never written to the state
store. Rotation (`cronstable reach rotate`) writes a new file; every paired
phone then rescans, because the id, the channel key, and the salt all
change.

## Daemon to relay

### Connecting

The daemon opens `GET wss://<relay origin>/v1/tunnel/<tunnelId>` with:

- `Sec-WebSocket-Protocol: cronstable-reach-v1`, which the relay echoes.
- `User-Agent: cronstable/<version> reach`.

The relay origin is `web.reach.relay`, or the origin of `push.relay.url`
when `web.reach.relay` is absent. Userinfo in the URL becomes Basic
authentication on the upgrade request, for self-hosted relays that want
it; the daemon redacts it from every log line.

### Control messages

Text frames carry JSON control messages. Every message has `"v": 1` and a
`"type"`. Unknown fields are ignored. An unknown type is a protocol error.

| Direction | `type` | Fields |
| --- | --- | --- |
| relay to daemon | `challenge` | `nonce` (base64, 32 random bytes), `relay` (the host the relay serves, `host` or `host:port`) |
| daemon to relay | `hello` | `sig` (base64, 64 bytes), `agent` (string), `node` (the daemon's node name), `admit` (array of base64 32-byte admit ids, at most 64), `limits` (`{"maxBody": <bytes>}`, the daemon's inner request body cap) |
| relay to daemon | `welcome` | `limits` (`maxBody`, `maxChunk`, `maxStreamS`, `maxInflight`, `initialWindow`, all integers) |
| daemon to relay | `admit` | `admit` (a full replacement of the admit set, sent when the daemon's tokens change) |
| daemon to relay | `bye` | none; sent before a graceful stop |

The signature is Ed25519 over `L ‖ 0x00 ‖ relay ‖ 0x00 ‖ nonce`, where
`relay` is the UTF-8 host from the challenge and `nonce` is the 32 raw
bytes. The daemon refuses a challenge whose `relay` differs from the host
it dialed, so a signature made for one relay is useless at another.

Rules:

- A bad signature closes the socket with code `4403`.
- A socket that sends no `hello` within 15 seconds closes with `4408`.
- When a verified `hello` arrives on a second socket for the same id, the
  relay closes the older socket with `4409`. The newest socket wins. A
  daemon that receives `4409` three times in ten minutes waits five minutes
  before reconnecting.
- A malformed message, a missing field, or a frame over the size cap closes
  the socket with `4400`. A relay may truncate `node` and `agent` to 200
  characters.
- A relay deploy or restart drops every daemon socket without a close
  frame. The daemon treats any close as a reason to reconnect.
- A relay may forget everything it holds about a tunnel after a long
  absence of its daemon; the hosted relay keeps a tunnel's records for 60
  days after the daemon's last connection.

### Liveness and reconnection

The daemon sends RFC 6455 ping frames every `web.reach.heartbeat` seconds
(30 by default). The relay's edge answers them without waking the tunnel
object. There are no application-level pings.

Reconnection uses jittered exponential backoff from 1 second to 60
seconds, reset after 60 seconds of a healthy connection. A `404` on the
upgrade means the relay has no Reach; the daemon parks with hourly retries
and one log line. A `403` or `503` that carries a `cf-mitigated` header
means zone policy refused the upgrade; same treatment.

### Binary frames

Binary frames carry requests. Layout: `u8 type ‖ u32 reqId ‖ payload`. A
frame payload is at most 1,114,112 bytes (1 MiB plus 64 KiB): room for the
largest sealed request in an `OPEN` and for the largest chunk in a `DATA`.

| Type | Direction | Payload |
| --- | --- | --- |
| `0x01 OPEN` | relay to daemon | The app's sealed request body, verbatim. |
| `0x02 DATA` | daemon to relay | Bytes of the sealed response stream, copied verbatim into the app's HTTP response body. |
| `0x03 END` | daemon to relay | The response is complete. No payload. |
| `0x04 CANCEL` | relay to daemon | `u16` reason: `1` client gone, `2` a stream cap or the day's allowance reached, `3` reserved for a relay that can announce its own shutdown (the hosted relay cannot, see above). |
| `0x05 REJECT` | daemon to relay | `u16` code followed by a UTF-8 reason. Only before any `DATA`. Codes: `1` cannot decrypt, `2` replay, `3` busy, `4` too large, `5` internal. |
| `0x06 WINDOW` | relay to daemon | `u32` bytes of credit for this request (see [Flow control](#limits-and-flow-control)). |

A frame naming a `reqId` the receiver does not know is dropped. The relay
answers such a `DATA` with `CANCEL 1` so the daemon stops streaming a
request nobody is waiting for.

## Relay admission

The relay learns nothing secret and keeps only hashes.

- The app derives `reachToken = HMAC-SHA-256(key = bearer, message = L ‖ 0x00 ‖ salt ‖ tunnelIdBytes)`, where `bearer` is the UTF-8 bearer token it holds for the daemon, `salt` is the 16 raw salt bytes, and `tunnelIdBytes` is the raw 32-byte Ed25519 public key. HMAC accepts any key length, so a 200-byte token and an empty token both derive.
- The daemon computes `admitId = SHA-256(reachToken)` for every configured bearer (`web.authToken` and every `web.authTokens` entry, however sourced) and sends the set in `hello` and again in `admit` whenever the set changes.
- The app sends `Authorization: Reach <base64url(reachToken)>` on every request. The relay admits when `SHA-256(credential)` is in the current set.

Holding a valid bearer is exactly what grants relay access, which is the
daemon's existing trust boundary. Rotating a token revokes relay access on
the daemon's next `admit`. The salt keeps a weak operator token safe from
a dictionary attack by the relay. The relay can see a stable pseudonym per
bearer; rotating it by UTC day is planned for a later revision.

## App to daemon session

Everything in this section is opaque to the relay.

### Primitives

| Purpose | Primitive |
| --- | --- |
| Key agreement | libsodium `crypto_kx` (X25519 with BLAKE2b-512 session-key derivation): `crypto_kx_client_session_keys` on the app, `crypto_kx_server_session_keys` on the daemon. |
| Key derivation | Keyed BLAKE2b with a 32-byte digest. |
| Authenticated encryption | ChaCha20-Poly1305 as specified in RFC 8439: a 32-byte key, a 12-byte nonce, and a 16-byte tag appended to the ciphertext. |

### Handshake

The app generates an ephemeral keypair `E` and computes
`(rx1, tx1) = crypto_kx_client_session_keys(E_pk, E_sk, D)` against the
daemon's static key `D`.

**HELLO** is the request body of the first request in a session:

```text
0x01 ‖ E_pk (32) ‖ n1 (12) ‖ ct
```

`ct` seals the UTF-8 JSON `{"v":1}` with key
`K_hello = BLAKE2b-256(key = L ‖ "hello", message = tx1)`, nonce `n1` (12
random bytes), and associated data `L ‖ "hello" ‖ tunnelIdBytes ‖ E_pk`.
Unknown JSON keys are ignored.

The daemon runs `crypto_kx_server_session_keys(D_pk, D_sk, E_pk)`, whose
receive key equals the app's `tx1` and whose transmit key equals the app's
`rx1`. Only the holder of `D`'s secret half derives them, which
authenticates the daemon. It opens HELLO, generates its own ephemeral pair
`F`, and answers with one **HELLO-REPLY** chunk:

```text
0x11 ‖ F_pk (32) ‖ n2 (12) ‖ ct
```

`ct` seals the UTF-8 JSON `{"v":1,"sid":"<base64 16 bytes>","ttl":1800}`
with key `K_reply = BLAKE2b-256(key = L ‖ "hello-reply", message = rx1)`,
nonce `n2` (12 random bytes), and associated data
`L ‖ "hello-reply" ‖ tunnelIdBytes ‖ E_pk ‖ F_pk`. `sid` is the session
id. `ttl` is the idle lifetime in seconds.

Both sides then compute the session keys from a second exchange between
the two ephemeral keys: `(rx2, tx2) = crypto_kx_client_session_keys(E_pk, E_sk, F_pk)`
on the app and `crypto_kx_server_session_keys(F_pk, F_sk, E_pk)` on the
daemon. In the app's naming:

```text
K_c2d = BLAKE2b-256(key = L ‖ "c2d", message = tx1 ‖ tx2)
K_d2c = BLAKE2b-256(key = L ‖ "d2c", message = rx1 ‖ rx2)
```

The daemon mirrors this with its own receive and transmit keys, which are
the same bytes. The static exchange authenticates the daemon; the
ephemeral exchange gives forward secrecy against a later leak of the
identity file. No clocks or timestamps take part.

### Requests

**REQ** is the request body of every request after HELLO:

```text
0x02 ‖ sid (16) ‖ ctr (u64) ‖ ct
```

`ct` seals the plaintext below with key `K_c2d`, nonce
`ctr (8) ‖ 0x00 0x00 0x00 0x01`, and associated data
`L ‖ "req" ‖ sid ‖ ctr (8)`. `ctr` starts at 1 and is unique per request
within a session. Concurrent requests are fine.

Plaintext:

```text
u32 headLen ‖ head ‖ u32 padLen ‖ pad ‖ body
```

`head` is UTF-8 JSON:

```json
{"v":1,"m":"GET","u":"/jobs?limit=5","a":"nas.local:8080","h":[["authorization","Bearer …"],["accept","application/json"],["if-none-match","\"…\""]]}
```

- `m` is the method, `u` the path and query, `h` the request headers as
  `[name, value]` pairs with lowercase names.
- `a` is the authority the app would have dialed directly. The daemon sets
  it as the `Host` of the inner request, so its origin gate sees the same
  host it sees on a direct connection.
- `pad` is zero bytes. `padLen` is the smallest value that makes
  `headLen + padLen` a multiple of 256, so the relay cannot tell a `304`
  probe from a small `POST` by size.
- The app sets `accept-encoding: identity` in every head. The body is at
  most `maxBody` bytes.

### Responses

The HTTP response body from the relay is a sequence of length-prefixed
chunks:

```text
(u32 len ‖ chunk)*
```

Chunk boundaries are independent of WebSocket frame boundaries and of
transport reads. The first byte of every chunk names its type: `0x11`
HELLO-REPLY, `0x12` RES, `0x13` NOSESSION.

**RES** chunks carry one sealed record each:

```text
0x12 ‖ ctr (u64) ‖ idx (u32) ‖ ct
```

`ct` seals `u8 kind ‖ u8 flags ‖ data` with key `K_d2c`, nonce
`ctr (8) ‖ idx (4)`, and associated data
`L ‖ "res" ‖ sid ‖ ctr (8) ‖ idx (4)`. `idx` starts at 0 for the first
record of a response and increases by one per record. `flags` is reserved
and zero.

| `kind` | `data` |
| --- | --- |
| `0x01 HEAD` | `u32 jsonLen ‖ json ‖ pad`, where `json` is `{"s":304,"h":[["etag","\"…\""],["content-type","…"]]}` (the inner status and the response headers as lowercase pairs) and `pad` is zero bytes bringing `4 + jsonLen + padLen` to a multiple of 256. |
| `0x02 BODY` | Raw response bytes, at most `maxChunk` per record. On a streaming response, one record per daemon write, so the daemon's `: ping` keepalive comments pass through unbatched. |
| `0x03 END` | Nothing. Required: a response whose stream ends without END or ERROR is a transport error, which defeats truncation by the relay. |
| `0x04 ERROR` | UTF-8 JSON `{"error":"…","status":429}`. A failure of the daemon's Reach adapter itself, never the inner HTTP status, which always arrives as a HEAD. `status` is optional and advisory. The app maps ERROR to a transport error. |

A response is HEAD, then zero or more BODY records, then END; or ERROR at
any point, which ends it.

**NOSESSION** is the single-byte chunk `0x13`, sent unsealed when the
daemon does not know `sid`. The app runs HELLO again and retries the
request once. The retry is safe because the daemon never dispatched a
request it could not open.

### Replay and session lifetime

The daemon keeps, per session, the highest counter seen and a 256-wide
bitmap of counters below it. A repeated counter, or one more than 256
below the highest, is answered with `REJECT 2`. Sessions are held in an
LRU of 256 per daemon, expire after 30 minutes idle (`ttl` in
HELLO-REPLY), and after 24 hours regardless. The app holds one session per
(server, process); widgets and intents pay one HELLO round trip each.

The daemon applies its own per-session limits regardless of what the relay
enforces: at most 8 requests in flight and a request-rate bucket, refused
with an `ERROR` record shaped like the daemon's 429 envelope.

## Relay HTTP mapping

`POST /v1/reach/<tunnelId>` with `Authorization: Reach <credential>`,
`Content-Type: application/octet-stream`, and a `Content-Length` of at most
1,052,672 bytes (the daemon's 1 MiB body cap plus framing). The body is a
HELLO or a REQ.

| Status | Body | Meaning |
| --- | --- | --- |
| 200 | `application/octet-stream`, `Cache-Control: no-store`, streamed | The sealed response. The relay answers the moment the first `DATA` frame arrives. |
| 400 | `{"v":1,"error":"…"}` | Malformed id, missing or malformed `Authorization`, wrong content type, or a missing `Content-Length`. |
| 403 | `{"v":1,"error":"reach token not admitted"}` | The credential is not in the daemon's admit set: the token was rotated or removed. |
| 404 | `{"v":1,"error":"unknown tunnel"}` | No daemon has connected under that id, or the relay has Reach switched off. The relay writes nothing for an unknown id. |
| 409 | `{"v":1,"error":"key mismatch"}` or `{"v":1,"error":"replay"}` | `REJECT 1` or `REJECT 2` from the daemon. |
| 413 | `{"v":1,"error":"…"}` | The body is over the cap (refused before any daemon traffic), or `REJECT 4`. |
| 429 | `{"v":1,"error":"…"}` with `Retry-After` | A bucket or cap in the table below, or `REJECT 3` (with `Retry-After: 1`). |
| 502 | `{"v":1,"error":"daemon aborted"}` | The daemon socket dropped mid-request, or `REJECT 5`. |
| 503 | `{"v":1,"error":"daemon offline","lastSeenAt":"…"}` with `Retry-After: 5` | The daemon socket is absent. `lastSeenAt` is null when it never connected while the relay kept records. |
| 504 | `{"v":1,"error":"no response"}` | No first frame within the first-byte window. |

`GET /v1/reach/<tunnelId>` with the same `Authorization` answers
`{"v":1,"online":true,"lastSeenAt":"…","node":"…"}` for the app's status
display. It is credential-gated so the id alone is never a presence oracle;
an unadmitted credential gets `403`, an unknown id `404`.

The inner HTTP status is never visible to the relay. The app maps 502,
503, and 504 to "unreachable through the relay", 403 to "the token changed
on the daemon, pair again", and a 409 key mismatch to "the daemon's
remote-access key changed, pair again".

## Limits and flow control

All limits are the same for every tunnel and are tunable through the
relay's environment.

| Limit | Default |
| --- | --- |
| Requests in flight per tunnel | 32 |
| Concurrent streams per tunnel | 4, a stream being a request open longer than 15 seconds; the newest request to cross that threshold while every slot is taken is cut with `CANCEL 2` |
| Stream lifetime | 30 minutes, then `CANCEL 2`; the response body ends without `END`, which the app treats as a transport error, and its tail loop reattaches |
| Open-stream seconds per tunnel per UTC day | 3 hours; once spent, new requests get `429` with a `Retry-After` reaching 00:00 UTC and open streams are cut with `CANCEL 2` |
| Requests per admit id | Burst 120, refilling 2 per second |
| Frames per daemon socket | 30 per second sustained, burst 200 |
| Unadmitted credentials per tunnel | 30 per minute, then `429` with `Retry-After` for the caller; never charged to the daemon socket |
| First-byte wait | 8 seconds |
| Stale-request sweep | A request with no daemon frame for 120 seconds fails with `502` |
| Frame payload | 1,114,112 bytes |

Flow control works on credit. Every request starts with `initialWindow`
bytes of credit (from `welcome`). The daemon never has more `DATA` payload bytes in flight
for a request than its remaining credit. As the app drains the response,
the relay sends `WINDOW` frames that add credit for that `reqId`. A slow
cellular link therefore cannot grow the relay's outbound buffer, and a
1,000-line tail replay waits for the phone rather than for the relay's
memory.

## Pairing payload and `/whoami`

The dashboard's pairing payload (`relay-protocol.md`, "Pairing links")
gains an additive `tunnel` object while, and only while, the daemon's
relay socket is connected, so a QR never advertises a dead route:

```json
{"v":1,"name":"nas","url":"http://nas.local:8080","token":"…","tunnel":{"relay":"https://relay.cronstable.com","id":"<tunnelId>","key":"<base64 32 bytes>","salt":"<base64 16 bytes>","node":"nas"}}
```

- `relay` is the relay origin (`https` scheme, no path, no userinfo).
- `id` is the tunnel id, `key` is `D` (the daemon's static X25519 public
  key), `salt` is the admission salt, and `node` is the daemon's node
  name.

An app that does not understand `tunnel` ignores it. An app that finds an
invalid `tunnel` (a wrong key length, a non-`https` relay) drops the
object, keeps the rest of the payload, and says why.

`GET /whoami` gains a `reach` object. For an authenticated caller:

```json
{"reach":{"state":"connected","relay":"https://relay.cronstable.com","id":"<tunnelId>","key":"<base64>","salt":"<base64>","node":"nas","fingerprint":"a1b2-c3d4-e5f6"}}
```

`state` is `connected`, `connecting`, or `off`. A caller served through
`web.anonymousScopes` sees `state` only. A daemon without a `web.reach`
section reports `{"state":"off"}`.

## What the relay sees

The relay learns, and a relay operator can log: the tunnel id, the daemon's
public IP address and connection times, the daemon's node name and agent
string, the admit-id hashes, the app's IP address, the credential
pseudonym, and the timing and sizes of requests and responses. It can
correlate a daemon's push posts with its tunnel by source address. It never
sees a bearer token, a URL path, a header, a job name, or a log line. The
hosted relay's console lines record the first 8 hex characters of
`SHA-256(tunnelId)`, a status, byte counts, and latency, and its
invocation logs, which would record request URLs, are switched off.

## Versioning

Every control message, every pairing object, and every sealed JSON carries
`"v": 1`. Unknown fields are ignored so the protocol can grow additively.
A change to any byte layout, key derivation, or associated-data string is
a new subprotocol name and a new `L`.
