# Calendar export (iCal) and the week calendar

For every schedule it runs, cronstable owns the fire-instant enumeration, so it can hand that enumeration to anything that reads a calendar. Two surfaces do:

- **`GET /calendar.ics`** and **`GET /jobs/{name}/calendar.ics`**: standard iCalendar (RFC 5545) feeds of upcoming fires, fleet-wide or per job. Subscribe a calendar app and the overnight maintenance jobs appear on your week, updating as the feed refreshes.
- The dashboard's **week calendar** (the `◫ week` toolbar button): the same data drawn as a seven-day grid inside the [web dashboard](Web-Dashboard).

Both enumerate through the scheduler's own engine in each job's resolved time zone, so what the calendar shows is exactly what the daemon will do, daylight saving time shifts included.

## The feed endpoints

| Endpoint | Contents |
|----------|----------|
| `GET /calendar.ics` | every enabled, cron-scheduled job (directed acyclic graph, or DAG, schedules appear as their `dag:<name>` job) |
| `GET /jobs/{name}/calendar.ics` | one job (or one DAG schedule); `404` for an unknown name |

Query parameters, both clamped rather than erroring:

| Parameter | Default | Range | Meaning |
|-----------|---------|-------|---------|
| `days` | 14 | 1 to 60 | the window: every fire in `[now, now+days)` becomes an event |
| `per_job` | 100 | 1 to 1000 | event cap per job; a capped job is flagged with an `X-CRONSTABLE-TRUNCATED` line in the feed |

```console
curl http://localhost:8080/calendar.ics
curl "http://localhost:8080/calendar.ics?days=30&per_job=20"
curl http://localhost:8080/jobs/nightly-backup/calendar.ics
```

Disabled jobs and `@reboot` jobs never become events (neither has upcoming scheduled fires). A job with no timetable renders as a valid, empty calendar rather than an error.

## What an event carries

- **`DTSTART` in UTC** (`...Z` form). The fire instants are real instants. The calendar client localizes them, and no `VTIMEZONE` blocks need shipping.
- **A stable `UID`** (hashed job name plus the fire instant), so a subscribed client updates events in place across refreshes instead of duplicating them.
- **A duration from run history**: the job's typical runtime rounded up to a whole minute, never under 5 minutes, because a zero-length event renders as an invisible sliver in week views, and never over 24 hours. The description states the real average.
- **`TRANSP:TRANSPARENT`**: a maintenance window on your calendar does not mark you busy.
- **`SUMMARY`** is the job name. **`DESCRIPTION`** is the schedule expression, its plain-English description, the job's time zone, and the typical runtime when known.
- **Refresh hints** (`REFRESH-INTERVAL` / `X-PUBLISHED-TTL`, one hour) for subscription clients that honor them.

Deliberately absent: command lines, environment, and output. Calendar feeds end up on phones and third-party calendar services, far outside the daemon's [redaction](Output-Capturing) reach, so the feed carries scheduling facts only.

## The feed key

With [`web.authToken`](HTTP-API) unset, the feeds are as open as the rest of the read API. With a token set, calendar apps are a special case: they cannot attach an `Authorization` header, so the credential has to travel in the URL, where a third-party calendar service stores it, syncs it to every device on the account, and every proxy on the path logs it.

The credential for that job is the feed key: an HMAC of a fixed context string under your token, which authorizes the `.ics` routes and nothing else. Read it from [`GET /whoami`](HTTP-API#get-whoami) as `calendarFeed`, or let the dashboard's `.ics` links carry it for you.

```console
curl -H "Authorization: Bearer s3cret" http://localhost:8080/whoami
{"authenticated": true, ..., "calendarFeed": "6f1c0a94d2b7e35810ff4c2a9d6b7e01"}

curl "http://localhost:8080/calendar.ics?feed=6f1c0a94d2b7e35810ff4c2a9d6b7e01"
```

Subscribe with that second URL. Whoever holds it reads the schedule feeds and can do nothing else: not start a job, not pause one, not reach any other route. The derivation is one-way, so a leaked subscribe URL also reveals nothing about the token behind it. A wrong or missing feed key is a `401` like any other auth failure.

Nothing about the key needs storing or rotating on its own. The daemon derives it per request from the tokens you already configured, each token has its own, and rotating a token invalidates every subscribe URL minted from that one while leaving the rest subscribed.

Every other API path still requires the bearer header, which keeps credentials out of URLs, logs, and referrers where a header suffices. The daemon's own access log writes `feed=***` rather than the live value, since calendar apps poll hourly or faster.

A `token` query parameter also opens the `.ics` paths, for a token holding `view` and nothing more:

```yaml
web:
  authTokens:
    - value: s3cret-readonly
      scopes:
        - view
      label: calendar
```

Anything more privileged, the all-scopes `web.authToken` included, answers `403` there and names the feed key as the way through.

## Subscribing

Any calendar app that adds a calendar "from URL" works: paste the feed URL (with `?feed=` when auth is on). Google Calendar, Apple Calendar, Outlook, and Thunderbird all poll subscribed feeds on their own cadence (typically hours; the feed's refresh hints suggest one hour). The feed regenerates on every request, so a fetched copy is always current as of the fetch.

## The week calendar in the dashboard

The `◫ week` toolbar button opens a seven-day grid of the same enumeration, starting today:

- Fires are computed in each job's own frame and **placed by your browser's local time** (the grid needs one display frame, and you think in yours). The dashed line is now.
- Each chip is one fire, hue-keyed to its job. Chips sharing a quarter-hour split the column instead of stacking. Today's already-fired chips render dimmed. Clicking a chip opens the job's drawer on its **Schedule** tab.
- **High-frequency jobs stay out of the grid.** A job firing more than about eight times a day summarizes into the **background hum** strip below the grid, where its cadence reads better than hundreds of sliver chips would. The strip chips open the same drawer.
- The card header links the fleet `.ics` feed, and each job's drawer **Schedule** tab links its per-job feed. Both carry the feed key, so the URL you copy out of the dashboard is safe to hand to a calendar service.

The view is a persisted preference like the other dashboard panels, and appears in the command palette as "Toggle week calendar". The [terminal dashboard](Terminal-Dashboard) carries the same panel under the same palette command: a day-by-hour fire grid, the agenda, and the hum strip, rendered in UTC.

See also: [Web Dashboard](Web-Dashboard), [HTTP Control API](HTTP-API), [Business-Day Schedules](Business-Day-Schedules) for the month-shaped day forms that make these calendars worth watching, and [Schedule Pressure](Schedule-Pressure) for the collision view of the same enumeration.
