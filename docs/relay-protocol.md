# Push relay protocol (v1)

This document is the wire contract between a cronstable daemon and a push
relay: the hosted service that accepts sealed alert ciphertexts from daemons
and forwards them to the platform push service (APNs). The reference
implementation (the source of the hosted relay at
`https://relay.cronstable.com/`) is published at
[ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay);
anything that implements this contract can serve as the relay a daemon's
`push.relay.url` points at.

The design goal is that the relay is not a trusted party. Every alert is
sealed end to end (by default a libsodium sealed box: X25519 +
XSalsa20-Poly1305; see [Suites](#suites)) to
each paired device's public key before it leaves the daemon. The relay
handles ciphertext and routing metadata only; the paired device's app
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
plus `/pair`, embedded credentials dropped; the dashboard reads it from
`GET /whoami` as `pairLinkBase`, and the hosted relay's landing is the
fallback while no `push:` section is applied. Fragments are never sent
with an HTTP request, so the payload never reaches the landing host's
server or its logs. The page that host serves is another matter: when it
loads at all (a scan with the app installed opens the app directly), its
script reads the fragment to build the fallback link below, so the
app-absent camera flow trusts the landing host's content. The in-app
scanner and the copyable raw JSON involve no third party.

The hosted relay serves that landing page at `GET /pair` (install pointers
plus a `cronstable://pair#<fragment>` fallback link carrying the identical
fragment) and the app-association file at
`GET /.well-known/apple-app-site-association`; a client app accepts the
payload as raw JSON or inside either link form. A self-hosted relay that
serves `GET /pair` receives its deployments' camera scans on its own
domain; instant app-open through the association file works only for the
domains baked into the app's entitlements, so a self-hosted landing leans
on the `cronstable://` fallback. A relay that skips both routes still
satisfies the daemon wire contract; its deployments pair through the
in-app scanner or the copyable JSON.

## Inbound request

The daemon sends one HTTP POST per (alert, device) to `push.relay.url`, with
a JSON body. The POSTs for one alert are issued concurrently, so a relay
sees a burst of up to one request per paired device (at most 100 connections
per alert, the client's connector limit); size admission control for that
shape rather than for a serial stream.

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
| `device` | string | The platform push token (APNs device token), exactly as the device registered it at pairing time. Opaque to the daemon; the relay uses it to address the notification. |
| `ciphertext` | string | The sealed alert, base64, sealed to the target device's public key under `suite`. At most 3800 characters (see [Size budget](#size-budget)). |
| `suite` | string | The sealing suite the ciphertext was produced under, so neither relay nor app has to infer the algorithm from a length. Optional: an absent value means `x25519`. See [Suites](#suites). |
| `collapseId` | string | An opaque coalescing key: 32 lowercase hex characters (a truncated SHA-256 over the alert's identity fields, keyed with a per-installation secret salt the relay never sees). The same alert, reported again or by any node sharing the installation's device registry, produces the same id; without the salt the id is not invertible even for guessable job names. Test alerts are the exception: each carries a fresh random id so a coalescing relay never swallows one. |
| `priority` | string | `time-sensitive` or `passive`. The relay maps it to the APNs interruption level: `time-sensitive` breaks through scheduled summaries, `passive` does not. |
| `event` | bool | `true` when the alert is a daemon event (the `notify:` fan-out: DAG failures, approval gates, leadership and quorum changes), `false` for job, SLA, and test alerts. Routing metadata only; the event's content is inside the ciphertext. |

The request carries no authentication from the daemon in v1; relay
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
| `xwing` | X-Wing (ML-KEM-768 + X25519), reserved | 1216 B | 1136 B |

`x25519` is the default and the only suite a daemon seals under today.
`xwing` is registered so that the wire format, the size fitting and the
pairing validation are already suite-driven; a daemon that cannot seal to
a suite refuses the pairing rather than storing a record whose every alert
would fail. Post-quantum sealing lands when PyNaCl exposes libsodium
1.0.22's `crypto_kem_*` functions.

Relays MUST treat `suite` as opaque routing metadata: the ciphertext is
sealed to the device either way, and a relay that does not recognize a
suite should still forward it. Relays MUST NOT reject an envelope for
carrying an unknown suite.

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

## Responses

| Status | Meaning | Daemon behavior |
| --- | --- | --- |
| 2xx | Accepted; the relay has taken responsibility for forwarding. | Success. |
| 429 | Rate limited. | Logged per device (up to 512 bytes of the response body); no retry. |
| Other 4xx | Rejected (malformed body, unknown or unroutable device token). | Logged per device; no retry. |
| 5xx | Relay-side failure. | Logged per device; no retry. |

The daemon never retries a relay POST: delivery semantics past acceptance
(retries toward APNs, dedup, suppression) are the relay's responsibility.
A failed POST is logged and never propagates into the daemon's reporting
path.

## Relay responsibilities

- **Deduplication and coalescing** on (`device`, `collapseId`): several
  nodes of a cluster may report the same alert; the relay collapses them
  without learning what the alert is about.
- **Rate limiting** per device token.
- **Flap suppression**: a job failing and recovering in a tight loop should
  not produce an unbounded notification stream; the relay owns the
  suppression policy, keyed on `collapseId`.
- **APNs forwarding** with `mutable-content` set, so the receiving app's
  Notification Service Extension runs, decrypts the ciphertext, and renders
  the notification locally. The `priority` field maps to the APNs
  interruption level.

## Privacy guarantees

- The relay never sees plaintext. Job names, hostnames, schedules, log
  lines, and event details exist only inside the sealed box, which only the
  target device's private key (generated on the phone and never leaving it)
  can open.
- `collapseId` is a truncated hash of identity fields, not the fields
  themselves, and the hash is keyed with a per-installation salt stored
  beside the device registry and never sent to the relay. Identity fields
  are low entropy (on a stateless install they reduce to alert kind plus
  job name), so an unkeyed hash would let a relay recover job names from a
  precomputed wordlist; the salt closes that.
- Sealing uses an ephemeral sender key per message (anonymous-sender sealed
  box), so the daemon holds no long-lived sending secret worth stealing.

## Replay protection

The relay (or anyone who can reach APNs with a captured request) can
re-deliver an old ciphertext; sealed boxes are anonymous-sender, so the
payload itself is the only place freshness can live. Every sealed
plaintext carries `ts`, the UTC instant the daemon built the alert. A
receiving app MUST treat a payload whose `ts` is older than **10 minutes**
(a window that absorbs clock skew plus APNs delivery latency) as stale:
render it as an outdated alert or drop it, never as a live page. Future
protocol versions may tighten the window; it is part of this contract so
daemon, relay, and app implementations age payloads identically.

## Sealed plaintext

What the app decrypts is a compact JSON document. Fields other than the
common set are present only when they apply (empty and null values are
omitted).

Common to every alert:

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Payload version. Always `1`. |
| `kind` | string | `success`, `failure`, `sla`, `event`, or `test`. |
| `name` | string | The job name; the DAG or node name for events; `test` for test alerts. |
| `success` | bool | Whether the reported outcome is a success (`false` for SLA breaches and events). |
| `host` | string | The reporting daemon's host name. |
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
3800 characters, and fits each target device's payload to that device's
own suite budget (see [Size budget](#size-budget)). When a payload is too
large it is shrunk in this order, re-checking after each step:

1. `log_tail` lines are dropped oldest-first (the newest lines carry the
   failure).
2. Long free-text fields (`message`, `fail_reason`, `subject`) are halved,
   never below 64 characters.
3. Optional context fields (`schedule`, `started_at`, `run_id`) are dropped.

The alert's identity (`name`, `kind`, `host`) is never trimmed.
