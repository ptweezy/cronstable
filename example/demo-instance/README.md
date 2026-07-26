# Public read-only demo instance

The always-on demo daemon behind the companion app's **"Try the demo"**
button (and App Store review): a plausible homelab board with something
on every screen. `cronstable.yaml` here is the whole personality —
healthy pulses, a live log streamer, one failure with a log tail worth
triaging (`backup-verify`), a retrying flaky job, a long runner, an
SLA-monitored job, and a small DAG.

## Access model (deliberately public)

The instance serves **one credential: a `view`-scoped bearer token**,
and it is public *by design* — it ships inside the app and sits right
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

Any small always-on box works (a $4–6/mo VPS is the reliable choice
while App Store review depends on this being up; a homelab host behind
a tunnel works too, with your uptime as the risk).

1. DNS: point `demo.cronstable.com` at the host.
2. `docker compose -f docker-compose.yml up -d --build`
   — the daemon listens on loopback only (`127.0.0.1:8080`).
3. TLS: `caddy run --config Caddyfile` (or adapt to your proxy).
   Caddy obtains and renews the certificate on its own.
4. Verify from outside:

   ```sh
   curl -H "Authorization: Bearer cronstable-public-demo-view" \
        https://demo.cronstable.com/summary
   ```

The container keeps run history in the `demo-state` volume (so the
board survives restarts populated), drops all capabilities, and is
memory/CPU-capped; the demo jobs are pure shell noise with no network
egress worth speaking of.

## What the app points at

- Base URL: `https://demo.cronstable.com`
- Token: the view token above (bake into the "Try the demo" action).
- `GET /whoami` answers `{label: "public-demo-viewer", scopes: ["view"]}` —
  exactly the shape the app uses to hide mutating affordances in demo
  mode.
