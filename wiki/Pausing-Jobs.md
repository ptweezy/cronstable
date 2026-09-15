# Pausing jobs

Pause a job at runtime to skip scheduled runs until the pause expires or you resume it. Pauses last one hour by default and at most thirty days. To stop a job indefinitely, set `enabled: false` in its configuration.

Pause and resume are available through the [HTTP API](HTTP-API#post-jobsnamepause), the [web dashboard](Web-Dashboard) and [terminal dashboard](Terminal-Dashboard) (`p` or the drawer button), and the `cron_pause_job` and `cron_resume_job` [MCP tools](MCP).

## Pausing and resuming

`POST /jobs/{name}/pause` takes an optional JSON body:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `durationSeconds` | int | `3600` (one hour) | How long the pause lasts, `1` to `2592000` (thirty days). Exclusive with `until`. |
| `until` | ISO-8601 string | (none) | Absolute expiry instant. Must be in the future and at most thirty days away. A timestamp without a UTC offset is read as UTC. Exclusive with `durationSeconds`. |
| `note` | string | `""` | Free-text audit note, up to 500 characters, shown wherever the pause shows. |
| `by` | string | `"api"` | Who paused it, up to 100 characters. |

An empty or absent body pauses for the default hour. The response is `200` with the pause record, where `channel` records which surface acted (`api`, `mcp`):

```shell
$ http post http://127.0.0.1:8080/jobs/nightly-etl/pause durationSeconds:=7200 note="upstream DB migration" by=parker
{
    "paused": {
        "since": "2026-07-19T14:00:00+00:00",
        "until": "2026-07-19T16:00:00+00:00",
        "note": "upstream DB migration",
        "by": "parker",
        "channel": "api"
    }
}
```

To extend or change a pause, pause the job again. An unknown job returns `404`. Invalid inputs return `400`:

- both `durationSeconds` and `until`
- an `until` value in the past or more than thirty days away
- an out-of-range duration
- a wrong type
- an oversized `note`/`by`

`POST /jobs/{name}/resume` (optional body: `by`) ends the pause immediately and returns `{"paused": null}`. Resuming a job that is not paused is a no-op with the same response. Both routes are mutating: they sit behind [`web.authToken`](HTTP-API#authentication) and the [cross-site request defense](HTTP-API#cross-site-request-defense) like `start` and `cancel`.

## What a pause does

- **Scheduled fires are skipped, visibly.** Each due slot gets a synthetic row in the run ledger with outcome `skipped` and `skip_reason: "paused"` (no `started_at`, no `exit_code`), so the history records a due fire that was deliberately not run, instead of a silent gap. The dashboards show these rows neutrally, and they stamp no success or failure state.
- **Pending retries defer.** A pending [retry](Failure-Detection-and-Retries) waits until the job resumes, preserving its attempt number and retry budget.
- **Catch-up skips the pause window.** [Missed-run catch-up](Durable-State) excludes slots within recorded pause windows, including daemon downtime, and covers slots after expiry. If the job is paused during boot evaluation, catch-up waits with its pre-pause watermark saved in an open checkpoint. After resume, it replays the backlog from before the pause or closes the checkpoint if nothing is owed. Disabling the job or setting `onMissed: skip` also closes the checkpoint; later catch-up starts from the run ledger.
- **Manual start still works.** `POST /jobs/{name}/start` can launch a paused job; a disabled job returns `409`. Cancellation also remains available.
- **Running instances are unaffected.** Pausing stops future fires. It never touches a run already in progress.
- **The pause sticks to the name.** Config reloads and edits to the job leave an active pause in place. Only removing the job from the config drops it.

While paused, the job's [service level agreement (SLA) checks](Late-Run-Detection) are suppressed. Slots skipped during a pause do not count as late.

## Expiry

A pause stops applying when its `until` deadline passes. The daemon removes the expired entry and logs the automatic resume during housekeeping, once per wall-clock minute. A restart after the deadline ignores the expired pause.

## Durability and clusters

Without a [`state:` store](Durable-State), a pause is in-memory: a daemon restart forgets it and the schedule resumes. With one, each pause and resume appends a record to the job's durable `paused/<job>` stream (newest record wins), so:

- a pause **survives restarts**: boot rehydrates the active windows before the first fire;
- a pause is **fleet-wide**: every node sharing the store honors it, whichever node accepted the request. The record's `host` field is audit information only. Peers pick up a pause or resume on their housekeeping pass, so cross-node propagation takes up to about a minute. The node that handled the request applies it immediately.

Scheduling never reads the store. Fire-time checks are memory-only. If the store cannot be read, each node keeps its last known in-memory pause state and logs a warning, under either `onStoreUnavailable` policy. A pause is an operator convenience, not a correctness fence, so an unreadable store neither resurrects nor drops pauses, and never blocks firing.

## Where a pause shows

| Surface | What appears |
| --- | --- |
| [HTTP API](HTTP-API#get-jobs) | `GET /jobs` always carries a `paused` field: `null`, or `{since, until, note, by, channel}`. Skipped slots appear in `GET /jobs/{name}/runs` as `outcome: "skipped"` rows with `skip_reason`. |
| [`GET /schedule/why`](Why-No-Run) | The daemon answers a probe against a paused job with a `paused` note naming the expiry, the actor, and the note. The answer explains why the schedule matched but nothing ran. |
| [Web dashboard](Web-Dashboard) | A **Paused** status with a `⏸` chip showing the expiry and note, a paused summary pill and wallboard tile, and one-click pause/resume (row button, drawer button, palette, the `p` key). |
| [Terminal dashboard](Terminal-Dashboard) | The same status, `⏸ til HH:MM` in the next-fire column, and the same `p` toggle. |
| [Prometheus](Metrics-with-Prometheus#per-job) | `cronstable_job_paused{job_name}` is `1` while the job is paused. `cronstable_job_runs_total` counts the skipped slots under `status="skipped"`. |
| [MCP](MCP) | The observe tools report the same `paused` object. `cron_pause_job` and `cron_resume_job` act on it. |

## See also

- [Late-Run Detection](Late-Run-Detection): the SLA checks a pause suppresses.
- [HTTP Control API](HTTP-API): the endpoint reference, authentication, and error shapes.
- [Durable State](Durable-State): the store behind restart survival and fleet-wide pauses.
- [Failure Detection and Retries](Failure-Detection-and-Retries): the retry ladder that defers across a pause.
- [Why Didn't It Run?](Why-No-Run): probing one timestamp, pause note included.
