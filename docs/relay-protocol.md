# Push relay protocol (v1)

This document is the wire contract between a cronstable daemon and a push
relay: the hosted service that accepts sealed alert ciphertexts from daemons
and forwards them to the platform push service (APNs). The reference
implementation (the source of the hosted relay at
`https://relay.cronstable.com/`) is published at
[ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay).
Anything that implements this contract can serve as the relay a daemon's
`push.relay.url` points at.

The design goal is that the relay is not a trusted party. Every alert is
sealed end to end (by default a libsodium sealed box: X25519 +
XSalsa20-Poly1305, see [Suites](#suites)) to each paired device's public key
before it leaves the daemon. The relay
handles ciphertext and routing metadata only. The paired device's app
decrypts the payload on the phone, inside its Notification Service
Extension.

All fields described here are versioned under `"v": 1`. Both the relay
envelope and the sealed plaintext carry their own `v`, so either side of the
protocol can evolve independently.

## Pairing links

Separate from the alert path, the dashboard's "Pair a device" QR encodes a
pairing link rather than the bare pairing JSON, so a phone's system camera
deep-links straight into the mobile app:

```text
https://relay.cronstable.com/pair#<base64url({"v":1,"name":…,"url":…,"token":…})>
```

The payload (the same `{v, name, url, token}` JSON the panel shows as a
copyable string) rides in the URL fragment, base64url-encoded with padding
stripped. The link's base is the origin of the daemon's `push.relay.url`
plus `/pair`, embedded credentials dropped. The dashboard reads that base
from `GET /whoami` as `pairLinkBase`, and the hosted relay's landing is
the fallback while no `push:` section is applied.

Fragments are never sent with an HTTP request, so the payload never
reaches the landing host's server or its logs. The page that host serves
is another matter. When it loads at all (a scan with the app installed
opens the app directly), its script reads the fragment to build the
fallback link described later, so the app-absent camera flow trusts the
landing host's content. The in-app scanner and the copyable raw JSON
involve no third party.

The hosted relay serves that landing page at `GET /pair`: install pointers
plus a `cronstable://pair#<fragment>` fallback link carrying the identical
fragment. The hosted relay also serves the app-association file at
`GET /.well-known/apple-app-site-association`. A client app accepts the
payload as raw JSON or inside either link form.

A self-hosted relay that serves `GET /pair` receives its deployments'
camera scans on its own domain. Instant app-open through the association
file works only for the domains listed in the app's entitlements, so a
self-hosted landing relies on the `cronstable://` fallback. A relay that
skips both routes still satisfies the daemon wire contract. Its
deployments pair through the in-app scanner or the copyable JSON instead.

## Inbound request

The daemon sends one HTTP POST per (alert, device) to `push.relay.url`, with
a JSON body. It issues the POSTs for one alert concurrently, so a relay sees
a burst of up to one request per paired device (at most 100 connections per
alert, the client's connector limit). Size admission control for that shape
rather than for a serial stream.

```json
{
  "v": 1,
  "device": "8f3a1b…",
  "ciphertext": "TmV2ZXIgcGxhaW50ZXh0…",
  "collapseId": "93bc5d02dc2a24b5365347573b6f5115",
  "priority": "time-sensitive",
  "event": false,
  "suite": "x25519"
}
```

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Protocol version. Always `1`. |
| `device` | string | The platform push token (APNs device token), exactly as the device registered it at pairing time. Opaque to the daemon. The relay uses it to address the notification. |
| `ciphertext` | string | The sealed alert, base64, sealed to the target device's public key under `suite`. At most 3800 characters (see [Size budget](#size-budget)). |
| `suite` | string | The sealing suite the ciphertext was produced under, so neither relay nor app has to infer the algorithm from a length. Optional: an absent value means `x25519`. See [Suites](#suites). |
| `collapseId` | string | An opaque coalescing key: 32 lowercase hex characters (a truncated SHA-256 over the alert's identity fields, keyed with a per-installation secret salt the relay never sees). The same alert, reported again or by any node sharing the installation's device registry, produces the same id. Without the salt the id is not invertible even for guessable job names. Test alerts are the exception: each carries a fresh random id so a coalescing relay never drops one. |
| `priority` | string | `time-sensitive` or `passive`. The relay maps it to the APNs interruption level: `time-sensitive` breaks through scheduled summaries, `passive` does not. |
| `event` | bool | `true` when the alert is a daemon event (the `notify:` fan-out: DAG failures, approval gates, leadership and quorum changes), `false` for job, SLA, and test alerts. Routing metadata only. The event's content is inside the ciphertext. |

The request carries no authentication from the daemon in v1. Relay
deployments own their admission policy (network controls, per-device rate
limits keyed on `device`).

## Suites

`suite` names the public-key algorithm a device's registered key belongs
to. It rides on the pairing record, this envelope, and the APNs payload
the relay builds, so the algorithm is always stated rather than inferred
from a key or ciphertext length.

| Suite | Algorithm | Public key | Sealing overhead |
| --- | --- | ---: | ---: |
| `x25519` | libsodium sealed box (X25519 + XSalsa20-Poly1305) | 32 B | 48 B |
| `xwing` | X-Wing (ML-KEM-768 + X25519) | 1216 B | 1136 B |

`x25519` is the default suite; `xwing` is the post-quantum hybrid. A
daemon seals each alert under the suite the target device registered at
pairing, and a daemon that cannot seal to a suite (its install lacks the
suite's library) refuses the pairing rather than storing a record whose
every alert would fail. `GET /whoami` lists the suites a daemon can seal
to as `sealableSuites`, and the companion app pairs under `xwing` when
that list advertises it.

Relays MUST treat `suite` as opaque routing metadata: the ciphertext is
sealed to the device either way, and a relay that does not recognize a
suite should still forward it. Relays MUST NOT reject an envelope for
carrying an unknown suite.

A suite identifier is 1 to 16 characters from `[a-z0-9-]` and starts with a
letter or digit. A relay answers 400 to an envelope whose `suite` is outside
that grammar, the same way it answers a malformed `collapseId`. The token
lands in the APNs payload, so its length is part of the size budget below.
The reserve absorbs the widest token the grammar allows, which costs 10
bytes more than the measured `x25519`.

### `x25519` construction

The ciphertext is a libsodium sealed box (`crypto_box_seal`): a fresh
ephemeral X25519 sender key per message, XSalsa20-Poly1305 over the
plaintext, wire layout ephemeral public key (32 bytes) || box (plaintext
length plus the 16-byte tag), standard base64. Sealing overhead is exactly
48 bytes for every plaintext.

### `xwing` construction

This block is normative. Daemon and companion app implement it
independently and must match byte for byte.

The device public key on the wire is 1216 bytes, the ML-KEM-768
encapsulation key (1184 bytes) followed by the X25519 public key
(32 bytes), standard base64 in the pairing body.

Sealing is HPKE ([RFC 9180](https://www.rfc-editor.org/rfc/rfc9180)) in
base mode, single-shot, under this ciphersuite:

| Role | Algorithm | HPKE id |
| --- | --- | ---: |
| KEM | X-Wing: ML-KEM-768 + X25519 ([draft-connolly-cfrg-xwing-kem-10](https://datatracker.ietf.org/doc/draft-connolly-cfrg-xwing-kem/10/)) | `0x647A` |
| KDF | HKDF-SHA256 | `0x0001` |
| AEAD | AES-256-GCM | `0x0002` |

`info` is the ASCII bytes `cronstable-push-xwing`, exact, on both sides.
There is no AAD: both sides pass none to their single-shot APIs.

The wire ciphertext is the HPKE `enc` value (1120 bytes) followed by the
single-shot ciphertext (plaintext length plus the 16-byte GCM tag),
standard base64. Sealing overhead is exactly 1136 bytes for every
plaintext.

Every message carries a fresh encapsulation (HPKE base mode); the daemon
holds no long-lived sending secret. That keeps sealing anonymous-sender
under this suite (see [Privacy guarantees](#privacy-guarantees)).

## Size budget

APNs rejects notifications whose final JSON exceeds **4096 bytes**. That
frame is divided as:

| Part | Bytes |
| --- | ---: |
| The relay's APNs envelope (alert stub, `mutable-content`, `v`, `suite`) | 189 |
| `ciphertext`, base64 | ≤ 3800 |
| Reserve for future protocol fields | 107 |

The 189 is measured, not estimated: the relay's `tests/apns-size.spec.ts`
serializes the real envelope at a max-length ciphertext and asserts the
total stays under the cap. The daemon derives its own cap from the same
three numbers.

What that leaves for plaintext, after each suite's sealing overhead:

| Suite | Plaintext budget |
| --- | ---: |
| `x25519` | 2802 B |
| `xwing` | 1714 B |

A daemon fits each device's payload to **that device's** suite budget, so
a device paired under a wider-ciphertext suite is trimmed harder without
costing the devices beside it any log lines.

### Relay and daemon versions

A relay enforces the cap it was built with, and a daemon fits alerts to the
cap it was built with. Deploy the relay before you upgrade the daemons that
post to it. A daemon that fits to 3800 in front of a relay that enforces a
smaller cap gets a 400 for its largest alerts, which are the ones that carry
a full log tail.

The daemon recovers from that 400 instead of dropping the page. **3000
characters is the floor**: the smallest cap a conforming relay enforces. A
relay MUST accept every ciphertext up to the floor. When a daemon's
ciphertext exceeds the floor and the relay answers 400 with an error that
names `ciphertext`, the daemon re-fits that device's payload to the floor,
posts the envelope again, and logs once per process that the relay is
behind. The alert reaches the phone with fewer log lines.

## Responses

| Status | Meaning | Daemon behavior |
| --- | --- | --- |
| 2xx | Accepted. The relay has taken responsibility for forwarding, or has folded the alert into a digest (see [Delivery quota](#delivery-quota)). | Success. |
| 429 | Rate limited. | Logged per device (up to 512 bytes of the response body); no retry. |
| Other 4xx | Rejected (malformed body, unknown or unroutable device token). | Logged per device; no retry. |
| 5xx | Relay-side failure. | Logged per device; no retry. |

The daemon never retries a relay POST: delivery semantics past acceptance
(retries toward APNs, dedup, suppression) are the relay's responsibility.
A failed POST is logged and never propagates into the daemon's reporting
path.

## Relay responsibilities

- **Deduplication and coalescing** on (`device`, `collapseId`): several
  nodes of a cluster may report the same alert. The relay collapses them
  without learning what the alert is about.
- **Rate limiting** per device token.
- **Delivery quota** per device per month, lifted by a verified
  entitlement, with a digest in place of the per-alert pushes past the
  bound (both optional for a self-hosted relay).
- **Flap suppression**: a job failing and recovering in a tight loop should
  not produce an unbounded notification stream. The relay owns the
  suppression policy, keyed on `collapseId`.
- **APNs forwarding** with `mutable-content` set, so the receiving app's
  Notification Service Extension runs, decrypts the ciphertext, and renders
  the notification locally. The `priority` field maps to the APNs
  interruption level.

## Delivery quota

A relay may bound how many alerts it forwards to one device per UTC
calendar month, and lift that bound for devices holding a paid
entitlement. The hosted relay does: `RELAY_FREE_MONTHLY_FORWARDS`
(500) forwards per device per month on the free plan, unlimited with a
Cronstable Pro entitlement. Only alerts that reach APNs count; coalesced,
suppressed, and rate-limited envelopes do not, and neither do digests.
The month rolls over at 00:00 UTC on the first.

Past the bound the relay stops forwarding individual alerts and answers
each envelope with:

```json
{ "v": 1, "outcome": "digested" }
```

as a 2xx, so the daemon treats it as accepted. At most once per
`RELAY_DIGEST_INTERVAL_S` (3600) while alerts keep arriving, the relay
sends the device one fixed **digest** notification: a passive-priority
push with no ciphertext, under its own collapse id `digest`, whose body
tells the operator that alerts are waiting and to open the app. It
carries a top-level `"kind": "digest"` in place of `ciphertext` and
`suite`, so the app renders and routes it without decrypting anything.
The app still polls its servers directly; the digest only replaces the
per-alert pushes until the month resets or the device becomes entitled.

## Entitlement proof

A device proves a paid entitlement to the relay with the App Store's own
signed transaction (a StoreKit 2 `jwsRepresentation`, ES256 JWS with an
`x5c` chain to Apple Root CA G3). The relay verifies it offline: the
chain (pinned root, validity windows, Apple's in-app purchase marker
extension `1.2.840.113635.100.6.11.1` on the leaf), the signature, the
`bundleId` against the relay's APNs topic, a `productId` in the relay's
allowlist (`RELAY_PRO_PRODUCT_IDS`, default `com.cronstable.app.pro.monthly`
and `com.cronstable.app.pro.yearly`), no `revocationDate`, and
`expiresDate` (when present) in the future. The relay accepts `environment: "Sandbox"`
transactions while `RELAY_ACCEPT_SANDBOX_ENTITLEMENTS` is `true` (the
hosted default until App Store launch).

```http
POST /entitlement
Content-Type: application/json

{ "v": 1, "device": "8f3a1b…", "jws": "eyJhbGciOiJFUzI1NiIsIng1YyI6…" }
```

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Always `1`. |
| `device` | string | The platform push token, the same value the daemon's envelopes carry. |
| `jws` | string | Optional. The signed transaction. Without it, the request only reads the device's current plan and quota. |

The body is at most 16384 bytes. The response, 200 on success:

```json
{
  "v": 1,
  "plan": "pro",
  "expiresAt": "2027-08-01T00:00:00Z",
  "environment": "Production",
  "quota": { "used": 12, "limit": null, "resetsAt": "2026-10-01T00:00:00Z" }
}
```

`plan` is `free` or `pro`. `expiresAt` and `environment` appear only
on `pro`; `expiresAt` is null for an entitlement without an expiry. `quota.limit`
is the month's bound, or null when unlimited; `used` is this month's
forwards so far; `resetsAt` is the next rollover.

Errors, all with `{"v": 1, "error": …}` and a `reason` where one applies:

| Status | Meaning |
| --- | --- |
| 400 | Malformed body, device, or JWS. |
| 401 | The transaction does not verify: bad chain or signature, wrong bundle, unknown product, revoked, expired, or a Sandbox transaction while the relay refuses those. |
| 409 | The transaction already lifts its maximum number of devices (`RELAY_PRO_DEVICES_PER_TRANSACTION`, 5). The body carries `limit`. |
| 413 | The body is over 16384 bytes. |

One transaction (keyed by `originalTransactionId`) lifts at most that
many devices. A device holds its slot while it keeps re-posting the proof,
and the slot lapses after `RELAY_PRO_DEVICE_SLOT_TTL_S` (60 days) of silence, so
a replaced phone frees its slot on its own. Devices re-post on every
foreground and after a push-token rotation, and a newer, still-valid proof
replaces the stored one; the relay keeps only the transaction id,
product, expiry, environment, and verification time per device, never
the JWS itself.

The app learns from the daemon which relay to post to: the origin of
`pairLinkBase` in `GET /whoami`. A self-hosted relay that does not
implement this route answers 404, which the app treats as "no quota".

## Privacy guarantees

- The relay never sees plaintext. Job names, hostnames, schedules, log
  lines, and event details exist only inside the sealed payload, which only
  the target device's private key (generated on the phone and never leaving
  it) can open.
- `collapseId` is a truncated hash of identity fields, not the fields
  themselves. The hash is keyed with a per-installation salt stored beside
  the device registry and never sent to the relay. Identity fields are low
  entropy (on a stateless install they reduce to alert kind plus job name),
  so an unkeyed hash would let a relay recover job names from a precomputed
  wordlist. The salt closes that.
- Sealing is anonymous-sender under every suite: a fresh ephemeral sender
  key per message under `x25519`, a fresh encapsulation per message under
  `xwing`, so the daemon holds no long-lived sending secret worth stealing.

## Replay protection

The relay (or anyone who can reach APNs with a captured request) can
re-deliver an old ciphertext. Every suite seals anonymous-sender, so the
payload itself is the only place freshness can live.

Every sealed plaintext carries `ts`, the UTC instant the daemon built the
alert. A receiving app MUST treat a payload whose `ts` is older than
**10 minutes** as stale: render it as an outdated alert or drop it, never
as a live page. That window absorbs clock skew plus APNs delivery latency.
Future protocol versions may tighten it. The window is part of this contract
so daemon, relay, and app implementations age payloads identically.

## Sealed plaintext

What the app decrypts is a compact JSON document. Fields other than the
common set are present only when they apply (empty and null values are
omitted).

Common to every alert:

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Payload version. Always `1`. |
| `kind` | string | `success`, `failure`, `sla`, `event`, or `test`. |
| `name` | string | The job name. The DAG or node name for events. `test` for test alerts. |
| `success` | bool | Whether the reported outcome is a success (`false` for SLA breaches and events). |
| `host` | string | The reporting daemon's hostname. |
| `ts` | string | ISO-8601 UTC instant the alert was built. |

Job run context, on any alert where the value is known:

| Field | Type | Description |
| --- | --- | --- |
| `run_id` | string | The run's durable-ledger id. |
| `schedule` | string | The job's schedule as a crontab line. |
| `started_at` | string | ISO-8601 instant the run started. |
| `exit_code` | int | The process exit code. |
| `fail_reason` | string | Why the run counts as failed. |

`kind: success` / `kind: failure` only, when the report's `includeLogTail`
is on and output was captured:

| Field | Type | Description |
| --- | --- | --- |
| `log_tail` | array of string | The last captured output lines (stderr when captured, else stdout), at most 40 lines before size trimming. |

`kind: event` only:

| Field | Type | Description |
| --- | --- | --- |
| `event` | string | The event name (`dag_failure`, `approval_waiting`, `leader_change`, `quorum_loss`). |
| `subject` | string | One-line headline. |
| `message` | string | Body detail. |
| `dag`, `run_key`, `taskkey`, `role`, `leader` | string | Event extras, present when the event carries them. |

`kind: sla` only:

| Field | Type | Description |
| --- | --- | --- |
| `sla_check` | string | The breached check (`maxTimeSinceSuccess`, `lateAfter`, or `maxRuntime`). |
| `threshold_seconds` | number | The configured threshold. |
| `observed_seconds` | number | The measured value that breached it. |
| `last_success_at` | string | ISO-8601 instant of the last known success. |

`kind: test` alerts (from `POST /push/devices/{id}/test`) carry the common
fields plus a fixed `message`.

### Size fitting

The daemon guarantees the sealed, base64-encoded ciphertext never exceeds
3800 characters, and fits each target device's payload to that device's own
suite budget (see [Size budget](#size-budget)). When a payload is too large,
the daemon shrinks it in this order, re-checking after each step:

1. `log_tail` lines are dropped oldest-first (the newest lines carry the
   failure).
2. Long free-text fields (`message`, `fail_reason`, `subject`) are halved,
   never below 64 characters.
3. Optional context fields (`schedule`, `started_at`, `run_id`) are dropped.

The alert's identity (`name`, `kind`, `host`) is never trimmed.
