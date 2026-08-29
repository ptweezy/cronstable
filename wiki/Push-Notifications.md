# Push notifications

cronstable can deliver job and daemon alerts as end-to-end encrypted push
notifications to paired devices, through a hosted relay, using a fifth
reporter named `push` beside the mail, Sentry, shell, and webhook reporters
(see [reporting](Reporting)). The point is a page on a phone with none of
the usual trust cost: no third-party service ever reads the alert.

The encryption model: the daemon seals each alert to every paired device's
X25519 public key, in a libsodium sealed box (X25519 + XSalsa20-Poly1305),
so only that device's private key can open the payload. That key is
generated on the phone and never leaves it. The relay forwards alerts to the
platform push service (APNs), and sees only a device token, a ciphertext, an
opaque coalescing hash (keyed with a per-installation salt the relay never
sees), a priority, and an event flag, never job names, hostnames, or log
lines.

## Enabling push

Push is an optional extra. Install it alongside cronstable:

```shell
pip install "cronstable[push]"
```

(The extra is PyNaCl, which bundles libsodium. The release binaries bundle
it per architecture. A lane that cannot build it ships without the extra,
and the daemon then refuses to start with a config that asks for push,
reporting an error that says so.)

Then configure the daemon-global `push:` section, which says where alerts
go and where device pairings are stored:

```yaml
push:
  relay:
    url: https://relay.cronstable.com/
    timeout: 10
  devicesFile: /var/lib/cronstable/devices.json
```

`https://relay.cronstable.com/` is the hosted relay. Its full source is
published at
[ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay)
(MIT), including its delivery policy (per-device coalescing, flap
suppression, rate limits) and self-hosting instructions. Because alerts are
sealed before they leave the daemon, pointing at the hosted relay trusts it
with routing metadata only, never content.

`devicesFile` is needed only on a stateless install. With a
[`state:`](Durable-State) section the registry rides the durable store
instead (see [where pairings are stored](#where-pairings-are-stored)).

The section alone sends nothing. Each job hook (or the daemon-level
`notify:` block) opts in through the report schema, exactly like the other
reporters. A typical setup pushes on every failure through the file's
`defaults:`:

```yaml
defaults:
  onFailure:
    report:
      push:
        enabled: true
        priority: time-sensitive
        includeLogTail: true
```

The same block under [`notify.report`](Reporting#daemon-event-notifications-notify)
pushes daemon and orchestration events too: DAG failures, approval gates
awaiting a decision, and leadership and quorum changes:

```yaml
notify:
  report:
    push:
      enabled: true
```

### `report.push` options

Available on `onFailure`, `onPermanentFailure`, `onSuccess`, and `onLate`,
and under `notify.report`:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool (Opt) | `false` | Opt this hook into the push channel. Enabling it anywhere requires the daemon-global `push:` section (a `ConfigError` otherwise). |
| `priority` | `time-sensitive` or `passive` (Opt) | `time-sensitive` | Relayed to APNs as the interruption level. `time-sensitive` breaks through scheduled summaries; `passive` does not. |
| `includeLogTail` | bool (Opt) | `true` | Carry the last captured output lines (stderr when captured, otherwise stdout, up to 40 lines) inside the sealed payload, trimmed oldest-first to fit the size cap. |

### The `push:` section

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `relay.url` | str (required) | none | The relay endpoint the daemon POSTs alerts to. Must be an http(s) URL. The daemon never posts ciphertext anywhere the config did not spell out. |
| `relay.timeout` | float (Opt) | `10` | Total timeout in seconds for each relay request. Must be greater than 0. Device POSTs go out together, so this is also the worst case a whole fan-out adds to a report, whatever the fleet size. |
| `devicesFile` | str (Opt) | unset | Path of a local JSON file holding the paired-device registry. Required when no `state:` section is configured. When set, the daemon uses it even with a `state:` section. |
| `allowUnauthenticated` | bool (Opt) | `false` | Serve the `/push/devices` pairing endpoints on a routable (non-loopback, non-socket) listener even with no `web.authToken`. The default fails closed: with no token the web app has no auth middleware at all, so a `push:` section alongside a routable listener raises a `ConfigError` at load. An `https://` listener with `web.tls.clientCa` set is exempt without this flag (mutual TLS authenticates the caller). Plain `https://` is not exempt. Set true only when the endpoints are protected by other means (an mTLS-terminating proxy, a network policy). |

## Delivery quota on the hosted relay

The hosted relay forwards up to 500 alerts per device per UTC calendar
month for devices on the app's free plan, and without bound for devices
holding a Cronstable Pro entitlement. Only alerts that reach APNs count;
coalesced, suppressed, and rate-limited envelopes do not. Past the bound
the relay answers the daemon with `{"outcome": "digested"}` (a 2xx, so
the daemon logs no failure) and sends the device at most one fixed
"alerts are waiting" digest per hour; the app keeps polling its servers
directly. The device proves its plan to the relay itself, with the App
Store's signed transaction; the daemon takes no part beyond naming the
relay in `pairLinkBase`. A self-hosted relay applies whatever policy it
runs, and the reference relay's bound is the `RELAY_FREE_MONTHLY_FORWARDS`
knob. The wire details are in the
[relay protocol](https://github.com/ptweezy/cronstable/blob/main/docs/relay-protocol.md#delivery-quota).

## Pairing devices

Pairing registers a device's public key and platform push token with the
daemon. The dashboard has a **Pair a device** panel, in the command palette
and in settings. Its QR encodes a pairing link: the
`{v: 1, name, url, token}` payload, base64url-encoded into the fragment of
the relay origin's `/pair` route, so a phone-camera scan deep-links into the
companion app, or lands on install pointers when the app is missing.

The panel shows the same payload as a copyable JSON string, and the app's
in-app scanner accepts both forms. Either way the app completes the pairing
by calling `POST /push/devices` with the scanned token. The relay protocol
specifies the link format in its
["Pairing links" section](https://github.com/ptweezy/cronstable/blob/main/docs/relay-protocol.md#pairing-links).
The panel warns when the token it would embed is the all-scopes one. Give a
phone a scoped [`web.authTokens`](HTTP-API#scoped-tokens-webauthtokens)
entry instead.

[![The Pair a device panel: a QR code deep-linking the connection payload into the companion app, the payload as a copyable JSON string, and the all-scopes token warning](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/dashboard-pair.png)](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/dashboard-pair.png)

Pairing is also one API call (`control` scope):

```shell
$ curl -X POST http://127.0.0.1:8080/push/devices \
    -H "Authorization: Bearer s3cr3t" \
    -H "Content-Type: application/json" \
    -d '{"name": "parker-iphone", "platform": "ios",
         "publicKey": "jSNlDu28No2itHnvrs6ajHHuNAxvqgOjmGxHJrMo8yg=",
         "pushToken": "8f3a1bc2…d94af1c9"}'
{
    "device": {
        "id": "f1e2d3c4b5a69788",
        "name": "parker-iphone",
        "platform": "ios",
        "publicKey": "jSNlDu28No2itHnvrs6ajHHuNAxvqgOjmGxHJrMo8yg=",
        "fingerprint": "8c2f-91ab-04de",
        "pushToken": "…4af1c9",
        "createdAt": "2026-07-23T14:00:00+00:00",
        "createdBy": "parker-iphone"
    },
    "created": true
}
```

`publicKey` must be base64 decoding to the length its `suite` requires (32
bytes for the default `x25519`) and be a usable key for that suite. At
pairing, rather than on the first alert, the daemon rejects an all-zero or
low-order X25519 key that libsodium refuses to seal to. `suite` is optional
and defaults to `x25519`.
The daemon refuses a pairing that names a suite it cannot seal to, rather
than storing a record whose every alert would fail. `name`, `platform`, and
`pushToken` are bounded strings. Validation failures are a `400` naming the
field.

Re-pairing the same public key (push tokens rotate; phones get renamed)
answers `200` with `created: false` and updates `name`/`platform`/`pushToken`
in place, keeping the record's `id` and `createdAt` so revocation references
stay stable. `createdBy` records the label of the bearer token that
performed the pairing.

List pairings (`view` scope; push tokens are redacted to their trailing six
characters, and public keys are returned whole):

```shell
$ curl -H "Authorization: Bearer s3cr3t" http://127.0.0.1:8080/push/devices
```

Send a test alert through the relay to one device (`control` scope; `200`
with the relay outcome, `502` when sealing or the relay failed, so a silent
phone is debuggable from the dashboard):

```shell
$ curl -X POST -H "Authorization: Bearer s3cr3t" \
    http://127.0.0.1:8080/push/devices/f1e2d3c4b5a69788/test
{"device": "f1e2d3c4b5a69788", "status": 200, "error": null}
```

Revoke a device (`control` scope):

```shell
$ curl -X DELETE -H "Authorization: Bearer s3cr3t" \
    http://127.0.0.1:8080/push/devices/f1e2d3c4b5a69788
{"revoked": "f1e2d3c4b5a69788"}
```

Revoking the pairing stops future alerts to that device. To also revoke its
API access, drop its `web.authTokens` entry and reload. Endpoint details are
in the [HTTP control API](HTTP-API#get-pushdevices).

### Pair over a trusted transport, then compare fingerprints

Pairing endpoints sit behind the web API's bearer token whenever one is
configured, and the daemon refuses to start a `push:` section on a routable
listener that has none (see [failure behavior](#failure-behavior)). An
ungated pairing endpoint can therefore exist only on loopback or a unix
socket.

Three things still travel during pairing that are worth protecting:

- The QR payload embeds a bearer token.
- An app-absent camera scan loads the relay host's landing page with that
  payload in the URL fragment, readable by the page's script. The in-app
  scanner and the copyable JSON involve no third party.
- The pairing POST carries the device public key that every future alert is
  sealed to.

On a transport an attacker can read or rewrite (plaintext HTTP across a
shared network), the token can be stolen and, worse, the key can be
substituted: alerts would then seal to the attacker's key. So pair over
HTTPS ([`web.tls`](Listener-TLS#the-webtls-block) or a TLS-terminating
proxy), over loopback (an SSH tunnel to the daemon host), or on a network
you trust.

With [`web.bonjour`](LAN-Discovery) on, the daemon also advertises its URL
to the local network. The advert carries no secrets, but it does make the
daemon easier to find, which is one more reason pairing belongs on a trusted
network.

Key substitution is detectable after the fact, and the check takes 10
seconds. The pairing response and the device listing carry a `fingerprint`
(the first 12 hex characters of SHA-256 over the raw key bytes, grouped for
reading aloud), and the companion app displays the same fingerprint for the
key it generated. Compare them. If they differ, the daemon stored a key that
is not your phone's: revoke the device and pair again over a trusted
transport.

## Where pairings are stored

The registry has two homes, and exactly one is in effect:

- **The durable state store** (when a [`state:`](Durable-State) section is
  configured and `devicesFile` is not set): one document per device, so
  pairing and revocation are per-key atomic operations, and every node
  sharing the store sees the same registry. Whichever node fires a report
  pushes to the same device set. State garbage collection never sweeps the
  documents: a pairing lives until it is explicitly revoked.
- **`push.devicesFile`** (required on stateless installs): a single local
  JSON file, written atomically (a uniquely named temp file plus a rename)
  and created with owner-only permissions where the platform honors them.
  - If the file fails to parse, the daemon refuses writes rather than
    overwriting possibly recoverable pairings, and a later write sweeps a
    temp file that an unfinished write left beside it.
  - Reads and writes run on a private worker thread, are bounded at 10
    seconds, and are serialized process-wide per file path. A registry on an
    unresponsive mount therefore degrades pairing (the endpoints answer
    `503`) without stalling the scheduler or process shutdown.

Both homes also hold the installation's collapse salt: the `collapseSalt`
key in `devicesFile`, or a `pushmeta` document in the state store. It is the
secret that keys the coalescing hash sent to the relay, created on first
use. Every node sharing a registry derives the same hash for the same alert,
and the relay never sees the salt itself.

The reporting path reads an in-memory mirror of the registry, refreshed at
most every 60 seconds, so a slow or briefly unavailable store can never
stall a report fan-out. A pairing made on another node becomes effective
within that window. The daemon remembers a failed read for about 5 seconds,
so a burst of alerts against an unreachable store costs one attempt between
them rather than one each. The pairing endpoints always read and write the
store directly.

## Size limits and what an alert contains

The sealed plaintext is a compact JSON document: `v`, `kind` (`success`,
`failure`, `sla`, `event`, or `test`), `name`, `success`, `host`, and `ts`.
It also carries the run context that is known (`run_id`, `schedule`,
`started_at`, `exit_code`, `fail_reason`), the log tail on job alerts (when
`includeLogTail` is on and output was captured), the `event` / `subject` /
`message` fields on daemon events, and the breach fields on SLA alerts. The
full field-by-field schema is in the
[relay protocol document](https://github.com/ptweezy/cronstable/blob/main/docs/relay-protocol.md).

APNs rejects notifications over 4096 bytes, so the daemon caps the sealed,
base64-encoded ciphertext at 3800 characters: 4096 minus the 189 bytes of the
relay's own APNs envelope, minus a 107-byte reserve for future protocol
fields. The daemon fits each device's payload to that device's own suite
budget (2802 bytes of plaintext under `x25519`), so a device paired under a
wider-ciphertext suite is trimmed harder without costing the devices beside
it any log lines. It trims an oversized payload in order: log-tail lines
oldest-first (the newest lines carry the failure), then long free-text
fields halved (never below 64 characters), then the optional
context fields dropped. The alert's identity (`name`, `kind`, `host`) is
never trimmed. A relay that enforces the 3000-character floor (the smallest
cap the relay protocol allows) answers a larger ciphertext with a 400. The
daemon then re-fits that alert to the floor, posts it again, and logs once
that the relay is behind, so the page lands with fewer log lines instead of
bouncing. There is no per-job template: the companion app renders the
decrypted fields itself.

## Failure behavior

Push is an alerting channel, so its configuration fails closed: a channel
that silently self-disables is a missed page. All of the following are
`ConfigError`s at parse time (so `--validate-config` catches them), never
runtime degradations:

- a `push:` section on an install without PyNaCl (install the `push` extra
  or remove the section);
- `report.push.enabled: true` anywhere (a job, a DAG task, `notify:`)
  without a `push:` section; the error names the offenders;
- a `push:` section with neither a `state:` section nor `devicesFile`
  (the registry needs somewhere durable to live);
- a `push.relay.url` that is not http(s), or a non-positive
  `push.relay.timeout`;
- a `push:` section on a daemon whose web API listens on a routable
  address with no `web.authToken`/`web.authTokens`. Pairing is a mutating
  endpoint, and a token is what installs the auth middleware at all, so
  without one anything that can reach the listener can pair its own key and
  keep receiving alerts. Give the web API a token, keep `web.listen` on
  loopback or a unix socket, set `web.tls.clientCa`, or set
  `push.allowUnauthenticated: true`.

At runtime, delivery is deliberately non-fatal. The daemon logs a sealing
failure or relay outage per device and never raises it into the reporting
path, so a relay outage can never look like a reporting crash or affect the
other reporters.

The daemon POSTs to the devices concurrently, so an unreachable relay delays
a report by one `relay.timeout` rather than one per device, and therefore
does not hold up retry arming or a graceful shutdown by the sum. An alert
fired with no device paired is dropped with a warning naming the pairing
endpoint.

When the registry store is unavailable, whether unreachable or holding a
document this build cannot read, report fan-outs fall back to the last-known
device set and say so, and the pairing endpoints answer `503` per request.

## Relay protocol

The daemon-to-relay wire contract is documented in
[`docs/relay-protocol.md`](https://github.com/ptweezy/cronstable/blob/main/docs/relay-protocol.md).
The relay implementation lives in a separate repository and its source will
be published; that file is the contract any implementation must satisfy.

The trust model: the relay is not a trusted party. It receives ciphertext
and routing metadata only (device token, coalescing hash, priority, event
flag), and owns deduplication, rate limiting, and flap suppression,
coalescing on the hash without learning what it hashes. The per-installation
salt keys that hash, so the hash stays opaque even for guessable job names.
It forwards to APNs with `mutable-content` set so the app decrypts and
renders the notification on the device.

## Related pages

- [Reporting](Reporting): the mail, Sentry, shell, and webhook reporters and the shared report schema
- [HTTP Control API](HTTP-API): the `/push/devices` endpoints and `GET /whoami`
- [Durable State](Durable-State): the store the device registry rides when configured
- [LAN Discovery](LAN-Discovery): how a companion app finds the daemon on the local network
- [Late-Run Detection](Late-Run-Detection): the `onLate` hook push alerts can ride
- [Web Dashboard](Web-Dashboard): the dashboard hosting the pairing panel
