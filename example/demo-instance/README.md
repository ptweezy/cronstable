# Public read-only demo instance

The always-on demo daemon behind the companion app's **"Try the demo"**
button (and App Store review): a plausible homelab board with something
on every screen. `cronstable.yaml` here is the whole personality:
healthy pulses, a live log streamer, one failure with a log tail worth
triaging (`backup-verify`), a retrying flaky job, a long runner, an
SLA-monitored job, and a small DAG.

## Access model (deliberately public)

The instance serves **one credential: a `view`-scoped bearer token**,
and it is public *by design*; it ships inside the app and sits right
here in the compose file (default `cronstable-public-demo-view`). View
scope reads jobs, runs, logs and `/summary`; every mutating call
(run/cancel/pause, approvals, device pairing) answers 403 by scope.
There is no `push:` block: nothing can pair with the demo, and the
app's demo mode renders its own sample alerts locally.

Treat the token like the public URL it effectively is. Rotating it is
an env-var change plus an app-config update:

```sh
CRONSTABLE_DEMO_VIEW_TOKEN=new-value docker compose up -d
```

## Standing it up

Any always-on machine works, including one at home: the compose file
brings up the daemon plus a **Cloudflare Tunnel** that publishes it, so
the host needs no port forwarding, no static IP and no inbound firewall
hole, and TLS terminates at Cloudflare (nothing to renew here).

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

### If the host is a Mac

A Mac left alone will quietly take the demo down, so three settings
matter more than anything in this directory:

```sh
sudo pmset -a sleep 0 disksleep 0   # never doze
sudo pmset -a autorestart 1         # come back after a power cut
```

Also enable automatic login (System Settings → Users & Groups →
Automatic login) and Docker Desktop's *Start Docker Desktop when you
sign in*. Without those, a power blip leaves the machine sitting at the
login window with nothing running, and `restart: unless-stopped` never
gets a chance to help.

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
