# Public read-only demo instance

The always-on demo daemon behind the companion app's **"Try the demo"**
button (and App Store review): a plausible homelab board with something
on every screen. `cronstable.yaml` here is the whole personality:
healthy pulses, a live log streamer, one failure with a log tail worth
triaging (`backup-verify`), a retrying flaky job, a long runner, an
SLA-monitored job, and a small DAG.

## Access model (deliberately public)

The instance serves **one credential: a `view`-scoped bearer token**,
and it is public *by design*; it ships inside the app and sits in plain
sight in this directory (default `cronstable-public-demo-view`). View
scope reads jobs, runs, logs and `/summary`; every mutating call
(run/cancel/pause, approvals, device pairing) answers 403 by scope.
There is no `push:` block: nothing can pair with the demo, and the
app's demo mode renders its own sample alerts locally.

Treat the token like the public URL it effectively is. Rotating it is
an env-var change plus an app-config update:

```sh
CRONSTABLE_DEMO_VIEW_TOKEN=new-value docker compose up -d    # containers
CRONSTABLE_DEMO_VIEW_TOKEN=new-value ./launchd/install.sh    # native
```

## Standing it up

Any always-on machine works, including one at home: the daemon runs
beside a **Cloudflare Tunnel** that publishes it, so the host needs no
port forwarding, no static IP and no inbound firewall hole, and TLS
terminates at Cloudflare (nothing to renew here).

Two supported shapes. Containers are the default and give the tighter
sandbox. The native path exists because a Mac with no container runtime
is still a perfectly good demo host, and it is what serves
`demo.cronstable.com` today.

### With Docker Compose

1. Create the tunnel: Cloudflare **Zero Trust** dashboard → **Networks**
   → **Tunnels** → *Create a tunnel* → **Cloudflared** → name it, pick
   **Docker**, and copy the token out of the command it shows you.
2. Add a **public hostname** to that tunnel:
   - Subdomain `demo`, domain `cronstable.com`
   - Type **HTTP**, URL **`cronstable-demo:8080`**, the daemon's name on
     the compose network, not `localhost`; the tunnel runs in its own
     container. Cloudflare creates the DNS record for you.
3. Put the token in place:

   ```sh
   cp .env.example .env    # then paste the token into CLOUDFLARE_TUNNEL_TOKEN
   ```

4. `docker compose up -d --build`
5. Verify from anywhere:

   ```sh
   curl -H "Authorization: Bearer cronstable-public-demo-view" \
        https://demo.cronstable.com/summary
   ```

The daemon keeps run history in the `demo-state` volume (so the board
survives restarts populated), drops all capabilities, and is memory and
CPU capped; the demo jobs are pure shell noise with no network egress
worth speaking of. Nothing is published to the host except a loopback
port for local `curl`.

### Natively, under launchd (macOS)

What serves `demo.cronstable.com` today. Worth choosing when the host
has no container runtime, and on a Mac it also removes the weakest link
in the container path: `restart: unless-stopped` only helps once Docker
Desktop is running, so that path leans on automatic login *and* on
Docker's own start-at-login setting. A LaunchAgent needs only the first.

```sh
brew install cronstable cloudflared
cloudflared tunnel login                     # authorize the zone in a browser
cloudflared tunnel create cronstable-demo
cloudflared tunnel route dns cronstable-demo demo.cronstable.com

./launchd/install.sh                         # safe to re-run
```

No dashboard steps and no tunnel token: `cloudflared tunnel login`
writes a certificate that authorizes creating the tunnel and its DNS
record from the CLI, so `.env` stays a container-path concern.

`install.sh` derives a host-local copy of `cronstable.yaml` into
`$(brew --prefix)/etc/cronstable-demo/`, changing exactly two lines and
refusing to run if either stops matching what is in this directory:

| Retargeted | Why |
| --- | --- |
| `state.path` | `/var/lib` needs root; the Homebrew prefix does not |
| `listen` → `127.0.0.1:8080` | the container published only a loopback port, so binding the wildcard natively would newly expose the daemon to the LAN |

It then loads two agents, both `RunAtLoad` + `KeepAlive`:
`com.cronstable.demo` (the daemon) and `com.cronstable.tunnel`
(`cloudflared`, ingress in `~/.cloudflared/config.yml`), logging to
`$(brew --prefix)/var/log/cronstable-{demo,tunnel}.*.log`.

Two things this path gives up, both fine for shell-noise demo jobs but
worth saying plainly: the container's `cap_drop: [ALL]`,
`no-new-privileges` and memory/CPU caps have no equivalent here, and
jobs run as your login user. And because the deployed config is a
*copy*, editing `cronstable.yaml` in this directory does not reach the
running demo until you re-run `install.sh`.

## Keeping it up on a Mac

A Mac left alone will quietly take the demo down whichever path you
picked, so these matter more than anything else in this directory:

```sh
sudo pmset -a sleep 0 disksleep 0   # never doze
sudo pmset -a autorestart 1         # come back after a power cut
```

Enable automatic login as well (System Settings → Users & Groups →
Automatic login). It is what brings either path back unattended, since
LaunchAgents load at login and Docker Desktop additionally wants *Start
Docker Desktop when you sign in*. Without it a power blip leaves the
machine sitting at the login window with nothing running. Note that
FileVault and automatic login are mutually exclusive, so an encrypted
demo host cannot come back by itself.

Uptime is then only as good as your house. That is fine for a demo, but
App Store review can arrive at any hour and a dead demo backend is a
plausible rejection, so it is worth monitoring. A `maxTimeSinceSuccess`
SLA on another cronstable install, pointed at this one, is the
self-hosted way to find out before Apple does.

## What the app points at

- Base URL: `https://demo.cronstable.com`
- Token: the view token above (bake into the "Try the demo" action).
- `GET /whoami` answers `{label: "public-demo-viewer", scopes: ["view"]}`,
  exactly the shape the app uses to hide mutating affordances in demo
  mode.
