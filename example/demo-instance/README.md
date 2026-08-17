# Public read-only demo instance

The always-on demo daemon behind <https://demo.cronstable.com>, the
companion app's **"Try the demo"** button, and App Store review.
`cronstable.yaml` here is the whole personality: 30 jobs and 3 DAGs
curated as a **feature tour**, where every entry exists to light up one
capability and says so in its name, its comment, and its log lines, so a
visitor who has never heard of cronstable can answer "what can this
thing do?" by scrolling.

What is on the board: healthy pulses, a per-second schedule, a live log
streamer, and two long runners you can catch mid-flight. The failure
school covers a log tail worth triaging (`backup-verify`), a flaky job
that retries its way out, a job that fails on *stderr* alone, and one
that gets killed by its timeout. Durable state shows up as cursor
watermarks, exactly-once claims, and a depends-on-past gate the operator
has to clear. On the schedule side there are hashed `H/10` slots,
business-day forms (`LW`, `sun#1`, `L-3`), two timezones, and three
deliberate lint findings. Three DAGs cover fan-out with XCom, retries with
`all_done`, and a sensor feeding an approval gate. Two classic crontab
lines ride along through `include:` to show the migration path.

## Access model (deliberately public)

Anyone may read this board with **no credential at all**.
`web.anonymousScopes` grants the `view` scope to unauthenticated
requests, so the dashboard, the log tails, `/summary` and the calendar
feeds all open for a visitor who never sees a token prompt:

```sh
curl https://demo.cronstable.com/summary        # no Authorization header
open https://demo.cronstable.com/               # no token modal
```

Reads are all it grants. Every call needing `control` or `approve`
(run/cancel/pause, DAG trigger and backfill, approval decisions, device
pairing, `/shutdown`, `/mcp`) answers a reasoned 403, and the config
schema refuses to grant those scopes anonymously in the first place.
`GET /push/devices` is excluded from anonymous access too, since the
registry names paired devices. There is no `push:` block here anyway, so
nothing can pair with the demo, and the app's demo mode renders its own
sample alerts locally.

Two tokens still exist:

| Token | Scopes | Public? | Why it exists |
| --- | --- | --- | --- |
| `public-demo-viewer` | `view` | Yes, by design | The companion app authenticates with it. It grants anonymous view plus `GET /push/devices` (empty here: no `push:` block), so publishing it costs nothing on this instance. |
| `demo-operator` | `control`, `approve` | **No** | The `demo-operator` job's own credential. Visitors cannot act, so this scripted operator performs the mutating actions the board shows off: it decides the `firmware-rollout` approval gate, opens maintenance-window pauses on `feed-sync`, and clears `meter-export`'s depends-on-past gate with a manual start. |

The operator token is generated per host, never printed by any job, and
never checked in. On the native path it lives in two `0600` files, the
token file (`$(brew --prefix)/etc/cronstable-demo/operator-token`) and
the LaunchAgent plist that hands it to the daemon; on the container path
it lives in `.env`, which is gitignored. It reaches the job as a
run-scoped secret over loopback, so it never appears in a command line
or in any view-scoped API response.

Either token can be rotated on either deployment path by setting its
variable and re-running the bring-up command:

```sh
# containers: persist the value in .env, which compose reads on every up
echo 'CRONSTABLE_DEMO_VIEW_TOKEN=new-value' >> .env && docker compose up -d
CRONSTABLE_DEMO_VIEW_TOKEN=new-value ./launchd/install.sh    # native
```

Rotating the *view* token additionally means updating the app config.
Rotating the operator token affects only this host.

> **Daemon version.** This board needs a daemon that understands
> `web.anonymousScopes`; an older daemon rejects the config outright and
> names the key as unexpected. Build cronstable from `main`, or install a
> release that carries the key, and deploy the config and the daemon
> together.

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
3. Put both required secrets in place. `.env` needs the tunnel token you
   just copied and an operator token you generate; the daemon refuses to
   start without either:

   ```sh
   cp .env.example .env    # then paste the token into CLOUDFLARE_TUNNEL_TOKEN
   printf 'CRONSTABLE_DEMO_OPERATOR_TOKEN=%s\n' \
     "$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')" >> .env
   ```

4. `docker compose up -d --build`
5. Verify from anywhere, the way a visitor arrives:

   ```sh
   curl https://demo.cronstable.com/summary          # 200, no credential
   curl -X POST https://demo.cronstable.com/jobs/heartbeat/start   # 403
   ```

The daemon keeps run history in the `demo-state` volume (so the board
survives restarts populated), drops all capabilities, and is memory and
CPU capped. The jobs are bash and `python3` only: no `curl`, no
coreutils-only binaries, and no outbound network. The only sockets any
job opens are to the daemon's own loopback API. Nothing is published to
the host except a loopback port for local `curl`.

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

The daemon-version note above applies here: a brew-installed release
without `web.anonymousScopes` stops `install.sh` at its config-validation
step, and a failed validation leaves any previous install untouched.

The daemon it deploys is the `cronstable` on PATH unless `CRONSTABLE_BIN`
points at another build. The agent runs that binary, and its directory
leads the jobs' `PATH`, so every `cronstable` a job shells out to is the
same build. `install.sh` also refuses a PyInstaller build older than
1.2.43: on those, jobs cannot invoke the cronstable CLI at all (the
daemon's inherited bootloader variables kill it before the subcommand
runs), so every state, cursor, lock, XCom and secret feature on this
board fails silently and the board looks fine while doing nothing. If the
brew build is one of those, install from source and point `CRONSTABLE_BIN`
at it:

```sh
python3 -m venv "$(brew --prefix)/var/cronstable-demo/venv"
"$(brew --prefix)/var/cronstable-demo/venv/bin/pip" install cronstable
CRONSTABLE_BIN="$(brew --prefix)/var/cronstable-demo/venv/bin/cronstable" \
  ./launchd/install.sh
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

It copies `legacy.crontab` alongside it (the config's `include:`
resolves next to the deployed file, not next to this one), generates the
operator token on first run, and finishes by proving both halves of the
access model against the live daemon: an anonymous `GET /summary` must
answer 200, and an anonymous `POST /jobs/heartbeat/start` must answer
403. A regression in either one fails the install rather than quietly
publishing a board that acts on strangers' requests.

It then loads two agents, both `RunAtLoad` + `KeepAlive`:
`com.cronstable.demo` (the daemon) and `com.cronstable.tunnel`
(`cloudflared`, ingress in `~/.cloudflared/config.yml`; the script backs
up a pre-existing hand-written file beside it before overwriting),
logging to
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
- Token: the view token above (baked into the "Try the demo" action).
  Keeping it is the simpler path; the app could also drop it entirely,
  because the instance serves anonymous view.
- `GET /whoami` with that token answers
  `{authenticated: true, label: "public-demo-viewer", scopes: ["view"], allScopes: false, pairLinkBase: "https://relay.cronstable.com/pair"}`,
  exactly the shape the app uses to hide mutating affordances in demo
  mode. With **no** token the same endpoint answers
  `{authenticated: false, label: "anonymous", scopes: ["view"], allScopes: false, pairLinkBase: "https://relay.cronstable.com/pair"}`,
  so a client that keys on `allScopes` (not on `authenticated`) degrades
  identically either way.
