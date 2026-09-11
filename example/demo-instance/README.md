# Public interactive demo instance

The deployment behind <https://demo.cronstable.com> and the companion app's
**Try the demo** button. Short local commands simulate homelab backups, media
processing, telemetry, failures, retries and workflow approvals. No real
backup, firmware, media or AI service is called.

The deployment has three processes:

```text
visitor → Cloudflare Tunnel → demo gateway :8081 → standard daemon :8080
```

`gateway.py`, `gateway.json` and `gateway-overlay.html` belong to this example
only. They are not installed in the Python wheel or standard runtime image,
imported by the daemon, or bundled into release binaries. The source archive
includes them with the other examples. The gateway calls the existing HTTP
API; the daemon keeps its usual configuration schema and bearer scopes.

## Try it

No registration or token entry is required in the dashboard or iOS demo:

1. Open **restore-drill → Run now** for a two-second simulated restore.
2. Run **sensor-sweep**, watch its live logs, and try **Cancel**.
3. Run **backup-verify** to inspect a deliberate failure and its error log.
4. Trigger **firmware-rollout**, open its newest run, and **Approve** or
   **Reject** the gate. If a run is already active, open that one. Private
   housekeeping resolves abandoned gates after at least five minutes.
5. Pause/resume a sample job. Visitor pauses expire after **one minute**.

Everyone shares this board: starts have a ten-second cooldown, and all
mutations have a one-second cooldown. Busy requests return 429 with
`Retry-After`. The gateway's allowlist names 25 jobs and all three workflows.
The operator, secret-handling example (`cert-check`), disabled legacy job and
imported crontab entries remain read-only for visitors.

The demo has no real push pairing or APNs delivery; the iOS alert cards are
local samples. Real notifications need a separate configured server.

## Access and resource bounds

The daemon grants anonymous `view` access and gives the public compatibility
token, `cronstable-public-demo-view`, that same scope. Direct anonymous or
view-token mutations at port 8080 return 403.

The gateway accepts anonymous visitors and that one public token. It forwards
reads with the view token and selected mutations with the private
`CRONSTABLE_DEMO_OPERATOR_TOKEN` (`control` + `approve`, which imply `view`).
It never forwards visitor credentials, cookies or forwarding headers. Unknown
credentials, including the private token presented at the public gateway,
return 401. Use the internal daemon API for operator access.

Only explicit read routes and sample start/cancel/pause/resume, workflow
trigger and approval routes are forwarded. Pairing, MCP, backfill, shutdown
and new/unlisted endpoints are unavailable. Mutation bodies are limited to
2 KiB and five seconds; notes and attribution are fixed, and pause duration
is always 60 seconds. Public browser mutations must have an allowed Origin.
The gateway retains live log streaming and conditional/gzip read responses.

Run **one gateway per board**: budgets and admission locks are process-local.
The workflow start check reads the daemon's full run-state histogram, so an
old waiting gate still blocks starts after a gateway restart. If that state
cannot be established, the gateway refuses the trigger. Scheduled and private
operator runs remain independent of visitor admission.

At startup the gateway validates its separate allowlist against the standard
parsed daemon config. Listed commands must have no run-scoped secrets and
execution timeouts of at most 60 seconds; jobs need Forbid/Replace concurrency,
and clustered configs are refused. The supplied commands use 30-second
budgets, small fixed fan-outs and lightweight work. These checks are not a
command sandbox: deploy the reviewed samples with disposable data.

Changing the policy or any parsed config file disables visitor mutations
until the gateway restarts. Restart both services when changing the board.
The Docker limits bound CPU/memory; native jobs run as the login user.

The gateway adapts `/whoami` to report effective visitor `control`/`approve`
capabilities with `allScopes: false`, keeping existing clients compatible.
Only its dashboard response receives the demo notice and hides bulk/backfill
buttons. The packaged dashboard and daemon `/whoami` remain standard.

## Docker Compose

1. Create a named tunnel in Cloudflare **Zero Trust → Networks → Tunnels**.
   Select **Docker** and copy its tunnel token.
2. Map the public hostname `demo.cronstable.com` to the HTTP service
   **`http://gateway:8081`**. Existing demo tunnels must update their old
   `cronstable-demo:8080` target to this gateway address.
3. From this directory, configure the two tokens:

   ```sh
   cp .env.example .env
   # Fill CLOUDFLARE_TUNNEL_TOKEN in .env.
   # Replace the empty CRONSTABLE_DEMO_OPERATOR_TOKEN value with:
   python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
   ```

4. Run `docker compose up -d --build`.
5. Verify both sides of the boundary:

   ```sh
   curl http://127.0.0.1:8081/whoami
   curl -X POST http://127.0.0.1:8081/jobs/restore-drill/start  # 200; 429 during cooldown
   curl -X POST http://127.0.0.1:8080/jobs/restore-drill/start  # 403
   curl https://demo.cronstable.com/summary
   ```

The gateway reuses the locally built standard image with its entrypoint
changed and demo files mounted read-only. The daemon stores history in the
`demo-state` volume. Ports 8080 and 8081 are published only on loopback;
the tunnel reaches the gateway over the Compose network. If using another
hostname, set `CRONSTABLE_DEMO_HOSTNAME` in `.env` as well as the tunnel mapping.
After editing files, run `docker compose restart cronstable-demo gateway`.

## Native launchd (macOS)

From this directory, install the standard daemon library in a venv. Both the
daemon and gateway can use that interpreter; no special daemon build is needed.

```sh
brew install python cloudflared
python3 -m venv "$(brew --prefix)/var/cronstable-demo/venv"
"$(brew --prefix)/var/cronstable-demo/venv/bin/pip" install --upgrade ../..
cloudflared tunnel login
cloudflared tunnel create cronstable-demo
cloudflared tunnel route dns cronstable-demo demo.cronstable.com
CRONSTABLE_BIN="$(brew --prefix)/var/cronstable-demo/venv/bin/cronstable" \
  ./launchd/install.sh
```

To run an existing Homebrew/frozen daemon, leave `CRONSTABLE_BIN` unset and
set `CRONSTABLE_DEMO_PYTHON` to the venv's `bin/python`. The gateway needs
`aiohttp` and the standard `cronstable` library for config validation; use the
same version as the daemon. Frozen daemons older than 1.2.43 are refused
because their jobs cannot reliably invoke the CLI.

The installer copies the config, crontab and three gateway files to
`$(brew --prefix)/etc/cronstable-demo/`. It retargets the state path to the
Homebrew prefix and binds the daemon to `127.0.0.1:8080`. The gateway binds to
`127.0.0.1:8081`, and the generated tunnel ingress points there. A pre-existing
hand-written `~/.cloudflared/config.yml` is backed up before replacement.
The operator token is generated once and retained in a 0600 file; both plists
that contain it also get mode 0600.

Three agents use `RunAtLoad` and `KeepAlive`: `com.cronstable.demo`,
`com.cronstable.demo-gateway` and `com.cronstable.tunnel`. Logs are under
`$(brew --prefix)/var/log/cronstable-{demo,demo-gateway,tunnel}.*.log`.
The installer verifies public reads and sample starts, refusal of private
housekeeping through the gateway, and refusal of anonymous direct daemon
mutations. Re-run it after editing this directory's deployment files.

For unattended operation, arrange for the Mac to stay awake and the user
session to return after reboot: LaunchAgents start at login. The Docker path
also requires its container runtime to start. Native jobs have no container
CPU/memory caps and run with the login user's filesystem access.

## Companion app

The base URL remains `https://demo.cronstable.com`, and the public token
remains `cronstable-public-demo-view`. Gateway `/whoami` reports scopes
`approve`, `control`, `view`, and `allScopes: false`; the label is
`public-demo-viewer` with the token and `anonymous` without it. These effective
capabilities remain subject to the gateway allowlist and shared budgets.

App builds also need intent resolution for the temporary demo session;
server deployment alone cannot fix a client that searches only saved servers.
Verify the deployed walkthrough on the app build being distributed.
