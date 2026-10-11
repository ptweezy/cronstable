# Pairing from the terminal

`cronstable pair` prints the QR code that pairs the
[iOS app](https://apps.apple.com/app/cronstable/id6801933039) with a server.
It's the code that the web dashboard's
[Pair a device](Web-Dashboard#pair-a-device) panel shows, drawn in a terminal.
Use it when the daemon serves the [HTTP API](HTTP-API) without the dashboard
page (`web.ui: false`), or when you have a shell and no browser, such as an
SSH session. The [terminal dashboard](Terminal-Dashboard) has the same panel.

```shell
export CRONSTABLE_WEB_TOKEN=phone-token-value        # the token the phone gets
cronstable pair                                      # local daemon on :8080
cronstable pair --url http://nas.local:8080          # a remote daemon
cronstable pair --public-url https://cron.example.net  # behind a proxy
```

Scan the code with the phone's camera, or tap **Scan QR code** in the app.

The command is a client of the running daemon, like `cronstable tui` and
`cronstable mcp`. It reads no configuration file, and it works on Linux,
macOS, and Windows.

## Before you pair

The phone calls the daemon's HTTP API, so the daemon needs the following:

- A `web.listen` address that the phone can reach, such as a LAN, VPN, or
  HTTPS address. A phone can't reach `127.0.0.1`.
- A token for the phone, when the daemon requires one. A scoped
  [`web.authTokens`](HTTP-API#scoped-tokens-webauthtokens) entry limits what
  the phone can do.
- A [`push:` section](Push-Notifications#enabling-push), for lock-screen
  alerts. Without one, the app connects and polls the server.

This configuration serves the API without the dashboard page and gives the
phone its own token:

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  ui: false
  authTokens:
    - fromEnvVar: PHONE_TOKEN
      scopes:
        - view
        - control
      label: phone
```

## What the code contains

The code is a pairing link: the relay's `/pair` route with the payload
`{v: 1, name, url, token}` as base64url JSON in the fragment. A camera scan
opens the app, or a page that explains how to get the app. The
[relay protocol](https://github.com/ptweezy/cronstable/blob/main/docs/relay-protocol.md#pairing-links)
specifies the link. The command reads the link's base from
[`GET /whoami`](HTTP-API#get-whoami), so a self-hosted relay keeps pairing on
its own domain.

The token in the code is the token that the command itself presents, from
`--token`, `--token-env`, or `CRONSTABLE_WEB_TOKEN`. A daemon that requires no
token ignores the one presented, and the command leaves it out of the code.
The phone receives everything that token grants: `view` reads the server,
`control` registers the phone for push alerts and controls jobs, and `approve`
decides approval gates. The command prints a warning to stderr in these cases:

| Warning | Meaning |
| --- | --- |
| The token grants full access | The token holds `view`, `control`, and `approve`, so it passes every route's check, with or without `params`. Pair with a scoped token to limit the phone. |
| The server authenticated no access token | The daemon requires no token, or it allows anonymous access and the command presented none. The code carries no token, and the app connects without one. |
| The connection lacks the `control` scope | The app can read the server, and `POST /push/devices` refuses to register the phone for alerts. |

Because the code contains the token, pair over HTTPS or a trusted network.
The code stays in the terminal's scrollback, so clear the scrollback after
you pair. To confirm that the daemon registered your phone's key, compare
fingerprints as described in
[pair over a trusted transport](Push-Notifications#pair-over-a-trusted-transport-then-compare-fingerprints).

## The address in the code

The app dials the address in the code, so the address must be the one that
the phone uses. The command chooses it in this order:

1. `--public-url`, when you pass it.
2. `--url`, when it names an address other than loopback.
3. Another address of this host, when `--url` is a loopback address such as
   the default `http://127.0.0.1:8080`. The command keeps the scheme of
   `--url`, checks that the same daemon serves the address, and prints a
   line to stderr that names both addresses.

In the third case, the [`GET /whoami`](HTTP-API#get-whoami) reply at `--url`
lists the daemon's listeners. Each listener with the scheme of `--url` gives
one address to check:

| Listener | Address to check |
| --- | --- |
| On every IPv4 address, such as `http://0.0.0.0:8080` | This host's LAN address, on the listener's port |
| On one address that another host can dial, such as a VPN address | The listener's own address and port |
| On loopback, on a link-local address (`fe80::/10` or `169.254.0.0/16`), or on every IPv6 address (`http://[::]:8080`) | None |

This host's LAN address is the IPv4 address of the default route, or the
address of the hostname on a host with no default route. The command waits
up to three seconds for the hostname's address, and then goes on with no LAN
address. A listener on `[::]` accepts IPv6 connections only, so it doesn't
serve the LAN address. A link-local address works on one network link only,
and its IPv6 form needs the name of the phone's own network interface, which
the code can't supply.

The command prefers the LAN address to a listener's own address. That
address belongs to the network of the listener's interface, such as a VPN or
a container bridge, and only a phone on that network can dial it.

The command sends the token only to `--url`. It sends each address a request
without the token, and the reply must carry the instance ID that the daemon
reported at `--url`. A daemon puts its instance ID in the
`Cronstable-Instance` header of every reply that its web API serves,
including a `401`. A second daemon on the address has another ID, even when
it shares the first daemon's configuration and tokens. The command reads the
headers of the reply and leaves the body unread.

When no listener gives an address, the command sends no request. A process
that holds the LAN address while the daemon listens only on loopback
receives nothing.

On the LAN address, the command also checks the port of `--url`, ahead of
every listener's address, because a published container port differs from
the port that the daemon binds. It skips the port of `--url` when the daemon
binds that port on other addresses only, such as loopback. The daemon
doesn't serve the LAN address on that port, so a process that holds the port
there receives nothing. A daemon that listens on port 8080 for loopback and
on port 9090 for every address gets port 9090 in the code.

The command checks the addresses at the same time and waits up to three
seconds for the replies. The first address that passes, in the order of
preference, goes into the code.

The instance ID is public, so on an `http://` listener it can't prove that a
reply comes from the daemon. On an `https://` listener, the check also
verifies the listener's certificate for the address, as the phone does.
With `--insecure`, the command can't verify the certificate, so it sends no
request. Pass `--public-url` in that case.

When no address passes the check, the command prints the reason for the
first address and exits with status 1. Add a LAN or VPN address to
`web.listen`, or pass `--public-url`.

A daemon leaves its listeners out of the reply to a connection that it
serves without a token under `web.anonymousScopes`. The command then has no
address to check. Present a token, or pass `--public-url`.

A loopback address is `localhost`, a name under it, or an address such as
`127.0.0.1`, `::1`, or `0.0.0.0` in any form that this host's socket layer
reads as an address, for example `127.1` where this host reads it as
`127.0.0.1`. A form that this host looks up as a name counts as a hostname.
So does a form with a leading zero that this host's C library reads as octal
in one call and as decimal in another, such as `0177.0.0.1` on macOS. Any
other hostname goes into the code as given, because the phone resolves it. A
path in the address goes into the code percent-encoded.

The command doesn't follow redirects, because its request carries the token.
When `--url` answers with a redirect, the command reports the redirect's
target and exits with status 1. When the redirect leads to the same API at
another address, such as an `https://` listener, the message names the `--url`
value to pass.

Pass `--public-url` whenever the phone's address differs from the one this
host sees: behind a reverse proxy, with a published container port, or with a
DNS name. A daemon that listens on every address also serves a VPN address,
and the code names the LAN address, so pass `--public-url` for a phone on the
VPN. Inside a container or WSL, the default route's address is internal to
the host machine, so pass `--public-url` there too.

```shell
cronstable pair --public-url https://cron.example.net
```

The line on stderr and the caption above the code both show the address, so
you can check it before you scan.

## Terminal size

The command draws the code with half-block characters, two rows of the code
on each line of text, in black on white on every terminal theme. The size of
the code grows with the length of the address, the server name, and the
token. A typical code has 49 to 57 modules on a side and needs a window of
about 60 columns by 30 lines.

The command fits the code to the window. It uses the standard four-module
margin when the window has room, and a narrower margin when it doesn't. When
the window is too small for the code, the command draws the code with a
one-module margin and then prints a warning with the size that the code
needs. The warning is the last line on the screen. Enlarge the window or
reduce the font size, and then run the command again.

On Windows, the command turns on the console's ANSI processing, so Command
Prompt, PowerShell, and Windows Terminal all work.

## Other formats

`--format` selects what the command prints:

| Format | Output |
| --- | --- |
| `qr` (default) | The caption and the QR code. |
| `link` | The pairing link as one line of text, for another QR tool. |
| `json` | The pairing JSON as one line of text. Paste it into the **Paste pairing details from the dashboard** field of the app's add-server form. |

In the pairing JSON, the command writes a character that isn't printable,
such as a control character or a bidirectional override in the server name,
as a JSON escape. The app reads the escape as the same character. The link
and the QR code carry the JSON in this form.

Each format contains the token, so handle the output as a secret.

## The terminal dashboard panel

In [`cronstable tui`](Terminal-Dashboard), open the command palette with
`Ctrl-K` and select **Pair a device (QR)**. The panel shows the same code for
the session's server and token. Press `c` to copy the pairing JSON, and `Esc`
to close the panel. A drawer, another panel, the settings, the keyboard
shortcuts, and the access token prompt open over the panel, and `Esc` closes
whichever one is on top.

The panel applies the same [address rules](#the-address-in-the-code) to the
session's `--url`. When the code names another address of this host, the
caption says so. When the session reaches the daemon only on loopback, the
panel says why. Start the terminal dashboard with `--url` set to the address
the phone uses, or run `cronstable pair --public-url`. When the daemon serves
the session without a token under `web.anonymousScopes`, select **Set access
token** in the command palette.

When the daemon rejects the session's token, the panel opens the access token
prompt and builds the code after you set a token.

In a short window, the panel gives its caption's lines to the code and moves
any warning to the hint row, before the key hints. The panel widens to fit the
warnings when the window has room, and shortens them when it doesn't.

## Options

```text
cronstable pair [--url URL] [--token TOKEN] [--token-env VAR]
                [--cacert PATH] [--client-cert PATH --client-key PATH]
                [--insecure] [--public-url URL] [--name NAME]
                [--format qr|link|json]
```

| Option | Default | Description |
| --- | --- | --- |
| `--url URL` | `http://127.0.0.1:8080` | The address where the command reaches the daemon. |
| `--token TOKEN` | unset | The bearer token to present and to put in the code. Prefer `--token-env`, which keeps the token out of the process list. |
| `--token-env VAR` | `CRONSTABLE_WEB_TOKEN` | The environment variable that holds the token. |
| `--public-url URL` | see [the address in the code](#the-address-in-the-code) | The address that the phone uses to reach the daemon. An empty value is an error. |
| `--name NAME` | the cluster node name, or the address's host and port | The server name that the app shows. |
| `--format FORMAT` | `qr` | `qr`, `link`, or `json`. See [other formats](#other-formats). |
| `--cacert`, `--client-cert`, `--client-key`, `--insecure` | unset | TLS options for an `https://` listener. See [client configuration](Listener-TLS#client-configuration). |

The command exits with status 0 after it prints, with status 1 when it can't
build or write the code, and with status 2 for a usage error.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `is a loopback address, which a phone cannot reach` | The command reached the daemon on loopback, and the rest of the message says why no other address of this host passed the check. Add a LAN or VPN address to `web.listen`, or pass `--public-url`. When the message reports a TLS verification failure, the listener's certificate lacks the address, so pass `--public-url` with a name that the certificate carries. |
| `reports no http listener on this host's LAN address`, or `no https listener` | The daemon at `--url` listens only on loopback, on a link-local address, on every IPv6 address (`[::]`), or with the other scheme. Add a LAN or VPN address to `web.listen`, or pass `--public-url`. |
| `names its listeners only to a connection that presents an access token` | The daemon serves the command without a token under `web.anonymousScopes`, and that reply has no listeners. Set `CRONSTABLE_WEB_TOKEN`, pass `--token-env`, or pass `--public-url`. |
| `the server's reply names no listeners` | The daemon runs a release that doesn't report its listeners. After an upgrade, restart the daemon so that it and the command run the same release, or pass `--public-url`. |
| `a different cronstable server answers at this host's LAN address`, or `at this host's address` | Another daemon listens on the address at that port. Pass `--public-url` with the address where the phone reaches the daemon at `--url`. |
| `no reply from` an address `within 3 seconds` | The address sent no reply in time. The usual cause is a firewall rule, or a server other than the daemon on that port. Pass `--public-url` with the address the phone uses. |
| `skips certificate verification` | With `--insecure`, the command can't check the listener's certificate for another address, and the phone rejects a certificate that doesn't name the address. Pass `--public-url` with a name that the certificate carries. |
| `pairing link base` and `is not a printable http:// or https:// URL` | The daemon's [`GET /whoami`](HTTP-API#get-whoami) reply carries a `pairLinkBase` that the command can't put in a link. Check `push.relay.url` in the daemon's configuration. |
| `redirects to` | The address answers with a redirect, which the command doesn't follow. When the redirect leads to the same API at another address, the message ends with the `--url` value to pass. Any other redirect, such as one to a sign-in page, means that the address doesn't serve the API itself, so pass an address that does. |
| `no HTTP reply from` | Something other than an HTTP server answers at the address, or the reply ended early. Check the port in `--url`. |
| `is not an http:// or https:// URL` | The value of `--url` or `--public-url` is empty, or it isn't a whole `http://` or `https://` address. The usual cause of an empty value is a shell variable that isn't set. Pass the address with its scheme, such as `https://cron.example.net`. |
| `requires an access token` | The daemon has a token configured. Set `CRONSTABLE_WEB_TOKEN`, or pass `--token-env`. |
| `rejected the access token` | The token matches no `web.authToken` or `web.authTokens` entry. |
| `holds a line break or another character that an HTTP header cannot carry` | The token from `--token` or the environment variable contains a control character, which a request can't send. The usual cause is the line ending of a file that the variable was read from. Set the token again without it. The command sends every other character as UTF-8, which is the form that the daemon compares. |
| `cannot write the output` | The reader of the command's output closed the pipe, as `head` does after its last line. The command exits with status 1. |
| The app scans the code and can't connect | The phone can't reach the address in the caption. Put the phone on the same network or VPN, or pass `--public-url`. |
| The app connects and no alert arrives | Check that the daemon has a `push:` section, that the token holds the `control` scope, and that the job enables the [`push` reporter](Push-Notifications). |
| The camera doesn't read the code | Enlarge the window until the whole code is visible. If the terminal draws gaps between lines, reduce its line spacing, or print `--format link` and use another QR tool. |

## Related pages

- [Push Notifications](Push-Notifications): the alerts that pairing enables,
  and where pairings are stored.
- [Web Dashboard](Web-Dashboard#pair-a-device): the same panel in a browser.
- [Terminal Dashboard](Terminal-Dashboard): the keyboard-driven dashboard that
  hosts the terminal panel.
- [HTTP Control API](HTTP-API#scoped-tokens-webauthtokens): scoped tokens, and
  the `/whoami` and `/push/devices` routes.
- [LAN Discovery](LAN-Discovery): how the app finds a server without a code.
- [Command-Line Reference](CLI-Reference#the-pair-subcommand): every
  subcommand.
