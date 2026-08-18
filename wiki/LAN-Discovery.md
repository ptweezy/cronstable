# LAN discovery (Bonjour/mDNS)

`web.bonjour` is opt-in, zero-config LAN discovery. When you enable it, the
daemon advertises its [web control API](HTTP-API) as a `_cronstable._tcp`
service over mDNS/Bonjour, so a companion app (or a service browser) on the
same network finds it without a typed URL.

## What is advertised

The advert describes one listener a LAN peer can dial: the first bound
`https://` listen entry reachable from another machine, or the first such
`http://` one when no https listener qualifies. Loopback and Unix listeners
are never advertised (their ports are unreachable from any other machine),
and a listener bound to one specific IPv6 address cannot be advertised (the
advert's address record is IPv4-only). The advert carries:

- The **instance name**: the node's hostname by default, or the map form's
  `name:` override (dots are replaced with hyphens; the label is truncated
  to 63 bytes).
- The **port**: the actually bound TCP port of the advertised listener, so
  it is correct even for an ephemeral `listen: http://0.0.0.0:0`.
- The **address**: the listener's own IP when it is bound to one specific
  IPv4 address, otherwise the host's primary outbound IPv4.
- TXT records `v` (the daemon version) and `scheme` (the advertised
  listener's own `http` or `https`).

The advert's SRV target is a dedicated `<name>-cronstable.local.` hostname,
so the daemon never contends with the machine's own `<hostname>.local.`
record (which Avahi or mDNSResponder may already answer for).

The advert carries no secrets: name, port, scheme, and version only. A
client that discovers the daemon still needs a bearer token to read
anything (see [authentication](HTTP-API#authentication)).

## Configuration

Bonjour requires the `discovery` extra:

```shell
pip install "cronstable[discovery]"
```

The extra is python-zeroconf, which is LGPL-2.1-licensed. The release
binaries and Docker images bundle it together with its license notice.
Any binary prints the notice with `cronstable --third-party-licenses`,
and each GitHub Release attaches the library's source archive. The full
compliance story is in
[LICENSING.md](https://github.com/ptweezy/cronstable/blob/main/LICENSING.md).

The boolean form advertises under the hostname:

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  bonjour: true
```

The map form overrides the instance name (and `enabled: false` turns the
advert off while keeping the block):

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  bonjour:
    enabled: true
    name: cron-prod-1
```

## Validation

Both checks fail at parse time (`ConfigError`), so `--validate-config`
catches them:

- `web.bonjour` enabled without python-zeroconf installed: install the
  `discovery` extra or disable `web.bonjour`.
- `web.bonjour` enabled when every `web.listen` entry is a Unix socket:
  the advert needs a TCP listener to point at.

## Runtime behavior

The advert follows the web app's lifecycle. It is registered while (and
only while) an advertisable TCP listener is actually bound, is updated on
a config reload when anything it carries changed, and is withdrawn on
shutdown.

A runtime mDNS failure (a registration error, no LAN-reachable listener, no
non-loopback address to advertise) is logged, and the advert is skipped
until the next config apply. Discovery is a convenience and never takes
down the scheduler.

## Browsing

macOS:

```shell
dns-sd -B _cronstable._tcp
```

Linux (Avahi):

```shell
avahi-browse -r _cronstable._tcp
```

## Related pages

- [HTTP Control API](HTTP-API): the interface the advert points at
- [Push Notifications](Push-Notifications): pairing the companion app the advert helps find
- [Web Dashboard](Web-Dashboard)
- [Configuration Reference](Configuration-Reference)
