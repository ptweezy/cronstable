# Inbound heartbeats — watching what cronstable does *not* run

Every other example here is about jobs cronstable launches. This one is
about the ones it does not.

A **heartbeat** is watched from the outside. Something else does the work —
a crontab line on a NAS, a GitHub Actions workflow, a Kubernetes CronJob, a
backup appliance whose only integration point is a "notify URL" field — and
calls an unguessable URL when it finishes. cronstable alerts when that call
does **not** arrive.

The signal is an absence, which is the whole point: it catches the backup
that quietly stopped running four months ago. No amount of watching the jobs
you *do* run will ever surface that one.

## Why this matters more than it sounds

Nothing has to be migrated. The far end keeps its own scheduler, its own
credentials, its own on-call story; you append one `curl` to a command line
it already runs. That makes cronstable useful for the ninety percent of your
scheduled work that lives somewhere you cannot install a daemon — which is
usually where the scariest jobs are.

## Run it

```console
# what are the URLs?
cronstable -c example/heartbeats/cronstable.yaml heartbeats --urls

# start the daemon and open http://localhost:8080/
cronstable -c example/heartbeats/cronstable.yaml
```

Everything starts in `new`: configured, never pinged, still inside its first
window. Give `lab-sensor-rollup` (period 1h, grace 10m) 70 minutes and it
ages to `late`, then to `down`. Or ping one and watch it flip to `up`:

```console
curl -fsS "$(cronstable -c example/heartbeats/cronstable.yaml heartbeats --json \
  | python -c 'import json,sys; print(json.load(sys.stdin)[0]["pingUrl"])')"
```

## The three ways to ping

```sh
# 1. bare: "it finished, and it worked"
0 3 * * * /usr/local/bin/backup.sh && curl -fsS -m 10 "$URL"

# 2. with the outcome, in one unbranching line — 0 succeeds, anything else fails
0 3 * * * /usr/local/bin/backup.sh; curl -fsS -m 10 "$URL/$?"

# 3. bracketed, so a run that BEGINS and hangs is caught by maxRuntimeSeconds
#    rather than by tomorrow's missed ping. The body is kept and shown in alerts.
curl -fsS "$URL/start"
./etl.sh
curl -fsS --data-raw "$(tail -c 400 etl.log)" "$URL/$?"
```

Always give `curl` a timeout (`-m 10`) and `-fsS`: a ping that hangs must
not hold up the job it is reporting on, and a ping that fails should say so
on stderr rather than silently.

## The two ways to say when a ping is due

| | anchored on | use when |
|---|---|---|
| `periodSeconds` | the last ping | you care about the *gap* — "at least daily", however it drifts |
| `schedule` | the last ping's next fire | you know the far end's wall clock — "after each 02:00 run" |

`graceSeconds` is the slack past due before it is called down; it absorbs
the ordinary jitter between "the job fired" and "the ping landed". Past due
but inside grace is `late`, which is visible on every surface and
deliberately reports nothing.

`schedule` expects the ping to land *after* the fire. A job that reliably
pings a little early wants `periodSeconds` instead.

## The URL is a credential

It is the one route in the API that no bearer token gates — it has to be,
because it goes into a crontab line on a machine that must never hold the
dashboard's token. The token authorises exactly one thing: saying that one
heartbeat is alive.

Because a URL that can silence a monitor is a write credential, **no API
response contains it, at any scope**. `cronstable heartbeats --urls`, run on
the daemon's own host where the secret already lives, is where you read
them.

Rotating `web.pingSecret` rotates every derived URL at once — which is the
right move if one leaks, and the reason to pin `token:` on any heartbeat
whose URL is deployed somewhere you cannot easily edit.

## See also

- [Inbound Heartbeats](https://github.com/ptweezy/cronstable/wiki/Inbound-Heartbeats)
  — the full reference
- [Late-Run Detection](https://github.com/ptweezy/cronstable/wiki/Late-Run-Detection)
  — the same question asked about jobs cronstable *does* run
