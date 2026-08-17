# Inbound Heartbeats

Every other monitoring feature in cronstable watches jobs it runs itself, from the inside: it launched the process, so it knows when it started, what it exited with, and how long it took. A **heartbeat** is the mirror image. Something else does the work — a classic crontab line on a NAS, a GitHub Actions workflow, a Kubernetes CronJob, a backup appliance whose only integration point is a "notify URL" field — and calls an unguessable URL when it finishes. cronstable alerts when that call does **not** arrive.

The signal is an absence, which is what makes it a dead man's switch: it catches the backup that quietly stopped running four months ago. No amount of watching the jobs you *do* run will ever surface that one.

Nothing has to be migrated for this to work, which is the practical point. The far end keeps its own scheduler, its own credentials, its own operational story; you append one `curl` to a command line it already runs. That makes cronstable useful for scheduled work living somewhere you cannot install a daemon — which is often exactly where the jobs you are most afraid of losing are.

The monitor lives in `cronstable/cron.py` (`Cron._heartbeat_periodic`), and the state machine it reads is `cronstable/heartbeat.py`, a pure module with no I/O in it. Like [SLA monitoring](Late-Run-Detection) it evaluates once per wall-clock minute and needs no [state store](Durable-State), though one makes the last ping durable and fleet-wide.

## Configuring it

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  pingSecret:
    fromEnvVar: CRONSTABLE_PING_SECRET

heartbeats:
  - name: nas-backup
    description: restic to Backblaze B2, from the Synology
    periodSeconds: 86400        # expect a ping at least daily
    graceSeconds: 7200          # ...with two hours of slack
    onLate:
      report:
        webhook:
          url:
            fromEnvVar: SLACK_WEBHOOK_URL

  - name: nightly-etl
    schedule: "0 2 * * *"       # expect a ping after each 02:00 fire
    timezone: America/New_York
    graceSeconds: 1800
    maxRuntimeSeconds: 5400     # for a job that also pings /start
```

`heartbeats:` is a list, like `jobs:` and `dags:`, so [included files and config directories](Includes-and-Defaults) extend it the same way. It deliberately does **not** inherit the `defaults:` block: that block's keys are job-launch keys, and silently applying a fleet-wide `onFailure` reporter to every heartbeat would be a surprise.

A complete, runnable example is in [`example/heartbeats/`](https://github.com/ptweezy/cronstable/tree/main/example/heartbeats).

### Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Unique across heartbeats, and may not collide with a job name: alerts, metric labels and the incident list all key on it. |
| `description` | string | `null` | Free text, shown on the dashboards and in alerts. |
| `periodSeconds` | int or null | `null` | Interval mode: expect a ping at least this often, measured from the last one. Exactly one of this or `schedule` is required. |
| `schedule` | cron expression | `null` | Schedule mode: expect a ping after each fire. The full dialect a job gets, [business-day expressions](Business-Day-Schedules) included. `@reboot` is refused: it names an event, not a cadence. |
| `timezone` / `utc` | string / bool | daemon local | The zone `schedule` is resolved in. Refused beside `periodSeconds`, which has no wall clock to be in. |
| `graceSeconds` | int | `300` | Slack past the due instant before the heartbeat is called down. Must be `>= 0`. |
| `maxRuntimeSeconds` | int or null | `null` | For a job that pings `/start`: how long its run may take before the silence is called an overrun. Does nothing without a `/start` ping. Must be `> 0` when set. |
| `token` | secret block | `null` | An explicit ping token instead of one derived from `web.pingSecret`. Same `value` / `fromFile` / `fromEnvVar` sources as every other secret. |
| `enabled` | bool | `true` | A disabled heartbeat is excused from every check but stays visible. |
| `onLate.report` | report block | reporter defaults | Fired once when it goes down. |
| `onFailure.report` | report block | reporter defaults | Fired when a `/fail` ping (or a nonzero exit code) says so. |
| `onRecovery.report` | report block | reporter defaults | Fired when the next good ping arrives. |

A heartbeat that names no report destination at all is legal and logs one line at load. It is still counted in `/summary`, shown on the dashboards and exported to Prometheus — a legitimate, if quiet, way to run one when Alertmanager is doing the routing.

## The two ways to say when a ping is due

| | anchored on | use when |
| --- | --- | --- |
| `periodSeconds` | the last ping | you care about the *gap* — "at least daily", however it drifts |
| `schedule` | the first fire strictly after the last ping | you know the far end's wall clock — "after each 02:00 run" |

Interval mode judges a job against its own last success rather than a fixed grid, so a drifting job is not punished for drifting. Schedule mode notices the morning the 02:00 run does not happen, rather than 24 hours later.

Schedule mode expects the ping to land **after** the fire, which is what makes a ping satisfy exactly the fire it followed. A job that reliably pings a little *before* its fire (it starts early and finishes early) is judged against the fire it has not yet reached, and wants `periodSeconds` instead.

## States

```
NEW ──first ping──▶ UP ──past due──▶ LATE ──past grace──▶ DOWN
                    ▲                                      │
                    └──────────── a ping arrives ───────────┘
```

| State | Meaning |
| --- | --- |
| `new` | Configured, never pinged, still inside its first window. |
| `up` | Heard from within the expected window. |
| `late` | Past due, inside `graceSeconds`. Visible on every surface and **deliberately silent**. |
| `down` | Past due + grace, or an explicit failure. This is what reports. |
| `paused` | An operator is holding it (see below). Excused. |
| `disabled` | `enabled: false`. Excused, but still listed. |

`new` ages exactly like the others, anchored on when this daemon first loaded the heartbeat. A fresh boot (or a reload that adds one) therefore grants a full window before anything can report — a restart is not evidence of a missed run — and a backup that never ran *once* still eventually pages. That is the whole point of the feature, so it is deliberately not special-cased into silence.

Silence is detected on the housekeeping cadence, about once a minute, so a window shorter than that is meaningless. A ping, by contrast, is not an absence: it is processed the instant it lands, so a recovery reports immediately rather than up to a pass later.

## Why it went down

| Reason | Meaning |
| --- | --- |
| `missed` | No ping arrived before the due instant plus grace. |
| `failed` | A `/fail` ping, or a nonzero exit code, said so outright. This short-circuits the clock: the job spoke, and waiting out a grace window to believe it would only delay the page. |
| `overrun` | `/start` arrived, `maxRuntimeSeconds` elapsed, and no finish ever landed. This is what distinguishes "began and hung" from "never began at all", which no amount of waiting for a finish ping can tell apart. |

Down **latches**: one report per outage, not one per minute. If the reason changes mid-outage (a job that went quiet finally comes back with a failure: `missed` → `failed`) it reports again, because that is a new fact — but the outage's start instant is unchanged, so the recovery report still quotes the true duration.

A pause or a disable clears the latch without claiming a recovery. Nothing came back; the operator just stopped asking.

## Pinging

```sh
# 1. bare: "it finished, and it worked"
0 3 * * * /usr/local/bin/backup.sh && curl -fsS -m 10 "$URL"

# 2. with the outcome, in one unbranching line
0 3 * * * /usr/local/bin/backup.sh; curl -fsS -m 10 "$URL/$?"

# 3. bracketed, arming maxRuntimeSeconds; the body is kept and shown in alerts
curl -fsS "$URL/start"
./etl.sh
curl -fsS --data-raw "$(tail -c 400 etl.log)" "$URL/$?"
```

| Suffix | Kind |
| --- | --- |
| *(none)*, `0`, `ok`, `success` | success |
| `fail`, `failure`, any nonzero integer | failure (the integer is kept as the exit code) |
| `start` | opens a run, arming `maxRuntimeSeconds` |

`GET` and `POST` both work, because a great many things that can call a URL cannot choose the method. A `POST` body is read up to 8 KiB and the first 1000 characters are kept, shown in alerts and on the detail view. An optional `?rid=` (max 64 characters, truncated rather than refused) correlates a `start` with its finish and is echoed in alerts.

Always give `curl` a timeout (`-m 10`) and `-fsS`: a ping that hangs must not hold up the job it is reporting on, and one that fails should say so on stderr rather than silently.

Each heartbeat's ingest is rate-limited to a burst of 20 pings, then one per second. A pinger faster than that is a loop that got loose, and the limit is what keeps it costing a dict lookup rather than a store write.

## The ping URL is a credential

`POST|GET /ping/{token}` is the one route in the [HTTP API](HTTP-API) that no bearer token gates. It has to be: the URL goes into a crontab line on a machine that must never hold the dashboard's token, and handing those the dashboard's token would grant far more than this endpoint does. The unguessable token in the path *is* the credential, and it authorises exactly one thing — saying that one heartbeat is alive.

Tokens are 26 lowercase base32 characters (130 bits), derived as an HMAC of the heartbeat's name under `web.pingSecret`. That derivation is deterministic, so the URL in a hundred crontabs stays valid across restarts, reloads and cluster nodes with nothing stored anywhere — and rotating `web.pingSecret` rotates every derived URL at once, which is the right move if one leaks. Pin `token:` on any heartbeat whose URL is deployed somewhere you cannot easily edit; a rotation then leaves it alone.

Because a URL that can silence a monitor is a *write* credential, and the `view` scope is handed out freely (a wallboard, a phone, an [anonymous grant](HTTP-API#authentication)), **no API response contains a ping URL at any scope**. Operators read them from the CLI, on the daemon's own host, where the secret already lives:

```console
$ cronstable heartbeats --urls
nas-backup    every 86400s   grace 7200s
    https://cron.example/ping/53vgfgiod3dwmmrtkr5spih4l2
nightly-etl   0 2 * * *      grace 1800s
    https://cron.example/ping/s5spg6lqy7f332xsodpon5zlnq
```

`--json` emits the same data machine-readably, and `--base URL` supplies the host when `web.listen` binds a wildcard address (which is not an address anything can ping, so the printed host is a placeholder until you say otherwise).

Unknown tokens, malformed tokens, tokens for a heartbeat a reload just removed, and unrecognised signals all answer the same 404 with the same body: probing the URL space learns nothing either way.

Set `web.pingSecret`, or give every heartbeat its own `token:`. A heartbeat with neither is a load-time `ConfigError`: there would be no address for the job to ping, so it could only ever report down. Heartbeats also require a `web` section at all, for the same reason.

## Reporting

The three hooks take the same `report` block a job's hooks take, through the same six [reporters](Reporting): mail, Sentry, shell, webhook, Windows Event Log (events 1020 down / 1021 up), and the end-to-end encrypted [push](Push-Notifications) reporter (alert kind `heartbeat`).

Templates render the heartbeat's own variables — `{{name}}`, `{{description}}`, `{{state}}`, `{{reason}}`, `{{expected_at}}`, `{{last_ping_at}}`, `{{overdue_seconds}}`, `{{down_since}}`, `{{down_seconds}}`, `{{exit_code}}`, `{{ping_body}}` — plus the standard set, with `{{schedule}}` carrying the heartbeat's expectation (its cron expression, or `every <n>s`). Shell reporters additionally get `CRONSTABLE_HEARTBEAT`, `CRONSTABLE_HEARTBEAT_STATE`, `CRONSTABLE_HEARTBEAT_REASON`, `CRONSTABLE_HEARTBEAT_LAST_PING_AT`, `CRONSTABLE_HEARTBEAT_EXPECTED_AT` and `CRONSTABLE_HEARTBEAT_OVERDUE_SECONDS` in their environment.

## Holding one for maintenance

```console
$ curl -X POST -H 'Content-Type: application/json' \
    -d '{"durationSeconds": 7200, "note": "NAS firmware", "by": "parker"}' \
    https://cron.example/heartbeats/nas-backup/pause
```

A held heartbeat is excused from every check; pings are still accepted and recorded. The window is bounded exactly like a [job pause](Pausing-Jobs), and for the same reason: an unbounded hold on a monitor is how a fleet ends up with a backup nobody has watched since March. `POST /heartbeats/{name}/resume` lifts it, and is idempotent.

With a [state store](Durable-State) the hold rides the heartbeat's own ping document, so it is durable and cluster-visible; without one it is node-local and lost on restart.

## Durability and clustering

Without a `state:` section everything is in memory: the daemon monitors heartbeats perfectly well, but a restart re-anchors every one of them on boot.

With a `state:` section the last ping and any hold are stored as one document per heartbeat (namespace `heartbeat`), read back at startup so a restart is not read as a fleet-wide silence, and written through the store's own read-modify-write so two nodes accepting pings for one heartbeat merge instead of clobbering each other. Out-of-order delivery is clamped rather than rejected: a replayed or racing ping still counts, but can never move the record backwards and hand the monitor a stale due instant.

A ping may land on **any** node — there is one URL per heartbeat, and whatever resolves it decides. With a shared store every node therefore sees the same record, and only the leader reports, so a five-node cluster pages once for one silent backup. Without a shared store each node keeps its own record and judges what it personally heard, which is correct for a single-node install and duplicative for a clustered one.

The leader gate deliberately fails **open**, unlike the job `Leader` gate: a heartbeat report is an alert about something already wrong, and reporting twice is far kinder than a cluster that has lost its leader reporting not at all.

## Surfaces

- **[HTTP API](HTTP-API)**: `GET /heartbeats` (the set plus per-state counts), `GET /heartbeats/{name}` (with the newest ping's exit code, body, origin and the schedule lint), `POST /heartbeats/{name}/pause` and `/resume`.
- **`GET /summary`**: a `heartbeats` object with per-state counts, present only when heartbeats are configured — so a client can use the key as a feature probe.
- **[Prometheus](Metrics-with-Prometheus)**: `cronstable_heartbeat_state{heartbeat,state}` (a 0/1 gauge per state, so alerting on `state="down"` is a label match and a future state cannot renumber an existing rule), `cronstable_heartbeat_last_ping_timestamp_seconds`, `cronstable_heartbeat_due_timestamp_seconds`, `cronstable_heartbeat_overdue_seconds`, `cronstable_heartbeat_pings_total{heartbeat,kind}`, `cronstable_heartbeat_pings_rejected_total{reason}`, `cronstable_heartbeat_downs_total{heartbeat,reason}`. Emitted only when heartbeats are configured.
- **[MCP](MCP)**: `cron_list_heartbeats` and `cron_get_heartbeat` in the `observe` toolset.
- **[CLI](CLI-Reference)**: `cronstable heartbeats [--urls] [--json] [--base URL]`.

Like the SLA monitor, this one runs inside the daemon and cannot report its own death, so pair it with an external Prometheus staleness alert.

## See also

- [Late-Run Detection (SLA)](Late-Run-Detection) — the same question asked about jobs cronstable *does* run
- [Reporting](Reporting) — the six reporters the three hooks share
- [Pausing Jobs](Pausing-Jobs) — the job-side twin of a hold
- [HTTP API](HTTP-API) · [Metrics with Prometheus](Metrics-with-Prometheus) · [Durable State](Durable-State)
