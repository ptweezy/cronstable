# Remote access

cronstable talks to your phone in two directions. [Push
notifications](Push-Notifications) carry alerts out to the companion app.
Reach carries the app's requests back in: a relay-brokered control channel
that serves the dashboard's data, log tails, and approval gates from
anywhere, with no inbound port on the daemon's host. This page covers Reach,
then the alternatives that expose the web listener itself: Tailscale,
cloudflared, and SSH.

## Reach

### What it does

The daemon dials out to the push relay and holds one WebSocket open. The app
sends ordinary HTTPS requests to the relay, which matches them to the
daemon's socket and copies bytes between the two. Every request and every
response is sealed end to end between the app and the daemon, so the relay
carries ciphertext and routing metadata only. The daemon serves each relayed
request through its own web app, so bearer tokens, scopes, the error
envelope, the origin gate, and the access log apply unchanged. The wire
contract is
[`docs/reach-protocol.md`](https://github.com/ptweezy/cronstable/blob/main/docs/reach-protocol.md).

The app uses the daemon's own address whenever it can reach it, on the LAN
or over a VPN, and falls back to Reach when it cannot.

### What the relay can and cannot see

The relay learns the tunnel id (the daemon's public signing key), the
daemon's public IP address and connection times, its node name and agent
string, hashes of the admitted credentials, the app's IP address, a stable
per-token pseudonym, and the timing and sizes of requests and responses. It
never sees a bearer token, a URL path, a header, a job name, or a log line.
The hosted relay's logs record a hash prefix of the tunnel id, a status,
byte counts, and latency.

### Configuration

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromFile: /etc/cronstable/token
  reach:
    keyFile: /var/lib/cronstable/reach.json
    # relay: https://relay.example    # default: the origin of push.relay.url
    # heartbeat: 30
```

| Key | Default | Description |
| --- | --- | --- |
| `keyFile` | required | The node's identity file: two key seeds and the admission salt, created with mode `0600` on first start. Keep it out of shared backups and out of version control. |
| `relay` | the origin of `push.relay.url` | The relay origin. `https` only, plus `http` for a loopback host so a relay under `wrangler dev` works. Scheme and host only, with no path, query, or fragment. Userinfo becomes Basic authentication on the WebSocket upgrade; the daemon redacts it from every log line. |
| `heartbeat` | `30` | Seconds between WebSocket pings, 5 to 300. |

The block fails closed at config load:

- It requires at least one bearer token (`web.authToken` or a
  `web.authTokens` entry). The relay admits an app by a credential derived
  from its bearer token, and every relayed request is authenticated by the
  web app inside the tunnel. `web.tls.clientCa` cannot stand in, because a
  relayed request presents no client certificate, and
  `push.allowUnauthenticated` does not apply.
- It requires PyNaCl, which the `push` extra carries
  (`pip install "cronstable[push]"`).
- It cannot share a configuration with a `cluster:` section. Reach serves
  one node's identity and pairing payload. To reach the nodes of a cluster,
  give each one its own address through one of the alternatives later on
  this page.
- Without `relay`, it requires a `push:` section to derive the relay origin
  from.

The daemon starts the service right after the web app comes up and
restarts it whenever the web app is rebuilt, so a token rotation reaches
the relay's admit set on the next housekeeping pass. The startup log names
the relay and the identity fingerprint:

```text
reach: remote access through relay.cronstable.com as a1b2-c3d4-e5f6 (identity /var/lib/cronstable/reach.json)
reach: connected to relay relay.cronstable.com as a1b2-c3d4-e5f6 (node nas)
```

On Windows the daemon serves relayed requests through an ephemeral loopback
TCP port instead of a unix socket, because asyncio's Proactor loop has no
unix servers. That port is a listener like every other one: the same bearer
tokens gate it.

### Pairing

Open **Pair a device** in the dashboard. While the daemon's relay socket is
connected, the pairing payload gains a `tunnel` object (the relay origin,
the tunnel id, the daemon's channel key, the admission salt, and the node
name), and the panel shows a line such as `Remote access key a1b2-c3d4-e5f6
through relay.cronstable.com`. The app stores the tunnel beside the
daemon's address and switches to it when the address is unreachable. A QR
scanned while the socket is down carries no tunnel, so it never advertises
a dead route: wait for the panel to show the key line, or pair again later.

The pairing token admits the phone at the relay. A token with
narrow scopes (`web.authTokens`) narrows what the phone can do through the
tunnel too, and dropping the token from the config revokes its relay access
on the daemon's next reload.

### Check the fingerprint

The daemon derives the fingerprint from its public signing key. It appears
in the dashboard's pairing panel, in `GET /whoami` as `reach.fingerprint`,
in `cronstable reach show`, and on the app's remote-access screen. Compare
the app's value with one of the daemon's after pairing. A mismatch means
the app paired with a different key than the one your daemon holds.

### Limits and cost

An idle tunnel costs the daemon one outbound WebSocket and one ping per
heartbeat. Each relayed request costs one HTTP request against the daemon's
own web app over a private loopback socket.

The daemon caps each app session at 8 requests in flight and a rate of 2
requests per second with a burst of 60. A request past either limit gets a
429-shaped error inside the tunnel, and the app retries later. The daemon
caps request bodies at 1 MiB. The relay adds its own limits per tunnel,
including a 30 minute cap per log stream (the app's tail loop reattaches),
a daily budget of open-stream time, and per-credential request rates; the
protocol document carries the full table. Flow control means a slow phone
link slows the daemon's writes rather than growing the relay's buffers.

### Rotation

`cronstable reach rotate -c /etc/cronstable.d` writes a fresh identity. The
tunnel id, the channel key, and the salt all change, so every paired phone
must scan the dashboard's pairing QR again. A running daemon picks the new
file up on its next housekeeping pass, within a minute, and reconnects
under the new id. Rotate after a key file leak, or when you move to
another relay.

`cronstable reach show` prints the tunnel id, the fingerprint, the relay
origin, the key file path, and the creation time. When the file is absent
it generates the identity first, exactly as the daemon does on its first
start. See the [CLI reference](CLI-Reference#the-reach-subcommand).

### Troubleshooting

| Symptom | Meaning | What to do |
| --- | --- | --- |
| `reach: relay … has no Reach (404 on the tunnel upgrade); retrying every 3600 seconds` | The relay you dialed serves push only. | Point `web.reach.relay` at a relay that implements the Reach protocol, or update your self-hosted relay. The daemon retries hourly and logs the line once per outage. |
| `reach: relay … refused the upgrade by zone policy (cf-mitigated)` | A WAF or bot rule in front of the relay blocked the WebSocket upgrade. | Allow WebSocket upgrades to `/v1/tunnel/` for the daemon's user agent, `cronstable/<version> reach`. The daemon retries hourly. |
| `reach: superseded by another daemon on this tunnel id 3 times in ten minutes` | Two daemons share one identity file, and the relay keeps only the newest socket (close code 4409). | Give each daemon its own `keyFile`. The daemon waits five minutes before it reconnects. |
| The pairing panel shows `Remote access is configured but not connected` | The daemon has no live relay socket right now: it is dialing, backing off, or parked. | Read the last `reach:` line in the daemon log. The QR carries no tunnel until the socket is up. |
| `reach: relay … presented itself as '…', not the host this daemon dialed` | The challenge named a different host than the one in `web.reach.relay`: a proxy rewrites the relay, or the relay's configured host is wrong. | Dial the relay by the host it serves, or fix the relay's host setting. |
| The app says the token changed on the daemon | The bearer token the app holds is no longer in the daemon's admit set. | Pair the phone again with a current token. |
| The app says the daemon's remote-access key changed | The identity was rotated after the phone paired. | Pair the phone again. |

## Alternatives

Each of these exposes the web listener itself, so the app dials the daemon
directly and Reach is unused. Keep `web.authToken` or `web.authTokens` set
on every one of them: transport encryption is not caller authentication.

### Tailscale Serve and Funnel

Tailscale gives every device a stable address on a private WireGuard
network, and the phone joins the same tailnet. Point the app at the node's
tailnet address and the daemon's port, or let `tailscale serve` front the
listener with HTTPS and a certificate Tailscale issues:

```shell
tailscale serve --bg 8080
```

`tailscale funnel --bg 8080` publishes the same listener to the internet
through Tailscale's relays. TLS ends on your node in both cases, so the
relays forward encrypted bytes only. Tailscale's own access controls decide
who can reach the node; the bearer token decides what they can do.

### cloudflared

A Cloudflare Tunnel (`cloudflared`) dials out to Cloudflare's edge and
publishes the listener on a hostname in your zone, with no inbound port.
TLS ends at Cloudflare's edge, so the edge handles every request in
plaintext, bearer token included. Put Cloudflare Access in front of the
hostname and treat the token as shared with Cloudflare.

`example/demo-instance/` in the repository is the worked example: the
public demo runs a Cloudflare Tunnel in front of a small gateway that
forwards an allowlisted set of routes to a standard daemon. Its README
covers the compose file, the tunnel, and the gateway's limits.

### SSH port forwarding

From a laptop, forward a local port to the daemon's loopback listener:

```shell
ssh -N -L 8080:127.0.0.1:8080 user@host
```

The dashboard and the app on that laptop then dial `http://127.0.0.1:8080`.
The forward lives as long as the SSH session, which suits an operator's
workstation better than a phone; for the phone, use Reach or Tailscale.

## Related pages

- [Push Notifications](Push-Notifications): the other direction, alerts
  sealed to the phone
- [HTTP Control API](HTTP-API): `GET /whoami` and its `reach` object
- [Configuration Reference](Configuration-Reference): the `web.reach` rows
- [CLI Reference](CLI-Reference#the-reach-subcommand): `reach show` and
  `reach rotate`
- [Listener TLS](Listener-TLS): native TLS for a directly exposed listener
- [Web Dashboard](Web-Dashboard): the dashboard hosting the pairing panel
