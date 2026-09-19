# Reach cryptographic review

This document records the security reasoning behind
[`reach-protocol.md`](reach-protocol.md): what each party can learn, which
primitives the protocol uses and why, and the residual risks an operator
accepts. Read the protocol document first; this one explains it.

## Threat model

Parties:

- The app holds a bearer token for the daemon and the pairing payload
  from the dashboard QR (the tunnel id, the daemon's channel key, and the
  admission salt).
- The daemon holds its identity file (two seeds and the salt) and its
  configured bearer tokens.
- The relay holds nothing secret. It sees both TLS connections in the
  clear, because Cloudflare terminates TLS before the Worker runs.

Adversaries the protocol defends against:

- A curious or hostile relay operator who reads, reorders, replays, delays,
  truncates, or forges traffic between the app and the daemon.
- Any party who obtains a pairing payload after the fact and tries to use
  the relay without the bearer.
- A dictionary attack by the relay against a weak bearer token, using the
  credential the app presents.
- A later leak of the daemon's identity file, against traffic recorded
  earlier.

Out of scope: an attacker holding both the pairing payload and a valid
bearer (that is an operator), a compromised phone, a compromised daemon
host, and traffic analysis beyond what the "What the relay sees" section
of the protocol document lists.

## Primitives

| Purpose | Primitive | Why this one |
| --- | --- | --- |
| Daemon identity | Ed25519 | Deterministic signatures, 32-byte public keys that double as the tunnel id, WebCrypto verification on the relay, PyNaCl signing on the daemon. |
| Relay credential | HMAC-SHA-256 keyed by the bearer | Standard, accepts any key length, available on every platform. |
| Key agreement | libsodium `crypto_kx` (X25519, BLAKE2b-512 session keys) | Both ends link libsodium; the construction binds both public keys into the session keys. |
| Key derivation | Keyed BLAKE2b-256 | Fast, keyed, and labeled; every derived key carries its purpose in the key material. |
| Authenticated encryption | ChaCha20-Poly1305 (RFC 8439) | Available with an explicit-nonce API on the app (CryptoKit) and the daemon (PyNaCl); nonces are structured counters, so the 96-bit nonce is ample. |

Two alternatives are worth naming. XChaCha20-Poly1305 would offer a
192-bit nonce, but the app's Swift libsodium binding has no encrypt call
that takes a caller-supplied nonce, and every nonce in this protocol is
either a counter or a single-use random value under a single-use key, so
the 96-bit nonce of ChaCha20-Poly1305 loses nothing. Keyed BLAKE2b could
derive the credential, but it caps its key at 64 bytes, which would force
a pre-hash of long bearers; HMAC accepts a bearer of any length directly.

## Key separation

- The identity file holds two seeds. `signSeed` only ever signs; `dhSeed`
  only ever agrees keys. A signature can never be confused with a key
  agreement input.
- No raw `crypto_kx` output is used as an AEAD key. `K_hello` and
  `K_reply` derive from `tx1` and `rx1` through labeled BLAKE2b, and the
  session keys `K_c2d` and `K_d2c` derive from the concatenation of both
  exchanges with distinct labels, so the two directions never share a
  key and a record can never be reflected.
- Every associated-data string starts with `L` and a purpose word, and
  every derivation key starts with `L`, so a ciphertext or a derived key
  from one context cannot be replayed in another.

## The handshake

`crypto_kx_client_session_keys(E, D)` yields keys that only the holder of
`D`'s secret half can also compute. The daemon proves that by opening
HELLO and sealing HELLO-REPLY under keys derived from that exchange. That
is what authenticates the daemon to the app: an impostor without the
identity file cannot produce a HELLO-REPLY the app will open.

The second exchange between the two ephemeral keys gives forward secrecy:
a later leak of the identity file reveals `D`'s secret half, which lets
an attacker who recorded old traffic compute `rx1` and `tx1`, but not
`rx2` and `tx2`, and the session keys need both.

The app authenticates itself with the bearer inside the sealed head of
every REQ, and the daemon's own auth middleware judges it exactly as on a
direct connection. The channel itself carries no app identity. At the crypto layer the daemon
serves any party who can seal to `D`, which is anyone holding the pairing
payload; the relay's admission check (a bearer-derived credential) and the
daemon's bearer check together mean that holding the payload without the
bearer buys nothing.

Compromise of the identity file lets an attacker impersonate the daemon to
apps paired with it. The remedy is rotation, which the pairing fingerprint
makes visible: every phone sees a new fingerprint and must rescan.

## Replay, reordering, and truncation

- Each REQ names a session id and a counter. The daemon keeps the highest
  counter and a 256-wide bitmap below it, and rejects any counter it has
  seen or that fell out of the window before it touches a key. A relay
  that replays a request gets `REJECT 2` and causes no second execution.
  This is what makes an `Approve` or a `Run now` safe to send through an
  untrusted party.
- A replayed HELLO creates a fresh session whose keys the replayer cannot
  derive, because they depend on the app's ephemeral secret. The session
  table's LRU bound and idle expiry keep that from costing more than a
  slot.
- Every response record binds the session id, the request counter, and
  the record index into its nonce and associated data. A record moved to
  another index, another request, or another session fails to open. The
  app requires indexes to increase by one from zero.
- Every response ends with an `END` or an `ERROR` record. A relay that
  truncates a stream produces a transport error on the app instead of a
  shorter answer. The daemon's own `content-length`, when present, arrives
  inside the sealed HEAD as a second check.

## The relay credential

`reachToken = HMAC-SHA-256(bearer, L ‖ 0x00 ‖ salt ‖ tunnelId)`. The relay
stores and compares `SHA-256(reachToken)`, so a relay database leak
reveals nothing usable. A dictionary attack on a weak bearer needs the
salt, which reaches only the phone and the identity file, so the relay
cannot mount one from what it sees. The credential is stable per bearer
and tunnel, so the relay can recognize the same phone across requests
under a pseudonym; a daily-rotating pseudonym is planned for a later
revision and needs no change to the sealed layer.

## The challenge signature

The daemon signs `L ‖ 0x00 ‖ relayHost ‖ 0x00 ‖ nonce`. The 32-byte random
nonce prevents replay of a captured signature at the same relay, and the
host binds it to one relay, so a signature collected by one relay is
useless at another. No timestamp takes part, so a daemon with a wrong
clock still connects. The daemon refuses a challenge that names a host it
did not dial, which stops a relay from harvesting signatures for a peer.

## What the relay can and cannot do

The relay can refuse service, delay traffic, and observe the metadata the
protocol document lists: addresses, timing, sizes, the tunnel id, the
daemon's node name, and credential hashes. It can correlate a daemon's
push posts with its tunnel by source address.

The relay cannot read a request or a response, forge either, replay a
request into a session, truncate a response without detection, substitute
one daemon for another (the tunnel id is the daemon's public key and the
app checks the fingerprint at pairing), or reuse a credential once the
daemon rotates the token it derives from.

## Residual risks

- Request heads and response heads pad to 256 bytes, which hides a `304`
  probe among small `POST`s. Bodies are unpadded, so the relay can see
  that a job list grew or that a log tail is busy.
- The pairing payload is a worldwide credential. Off the LAN, a
  photographed QR is worth as much as the bearer inside it, which was
  already true of the bearer. The dashboard shows the fingerprint and the
  all-scopes warning; short-lived pairing tokens minted by the dashboard
  are the planned mitigation.
- The key agreement is classical X25519, while the push path seals under
  X-Wing where the daemon can. A recorded session could be opened by a
  future quantum computer that also recovers `D`'s secret and the
  ephemeral secrets from their public halves. Adding an ML-KEM
  encapsulation to the ephemeral exchange is the planned follow-up and
  changes only the handshake and the session-key derivation.
- The identity file is protected by file permissions only. Anyone who
  reads it can impersonate the daemon until rotation.

## Test vectors

`tests/fixtures/reach/vectors.json` is generated by `tests/reach_vectors.py`
from fixed seeds and nonces, and `tests/test_reach_codec.py` asserts the
committed file matches. The relay and the companion app test against the
same file, so all three implementations agree byte for byte on the
identity, the challenge signature, the credentials, the handshake, sealed
requests, sealed records, the response stream, and the frame layout.
Vectors are authored here and ported outward, and nothing flows back into
this repository from a port.
