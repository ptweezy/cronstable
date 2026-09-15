# Calendar export (iCal) and the week calendar

cronstable shows upcoming scheduled runs in two calendar views:

- **`GET /calendar.ics`** and **`GET /jobs/{name}/calendar.ics`**: standard iCalendar (RFC 5545) feeds, fleet-wide or per job. Subscribe in a calendar app to see scheduled jobs alongside your other events.
- The dashboard's **week calendar** (the `◫ week` toolbar button): the same data drawn as a seven-day grid inside the [web dashboard](Web-Dashboard).

Both use the scheduler's engine and each job's resolved time zone, including daylight saving time shifts.

## The feed endpoints

| Endpoint | Contents |
|----------|----------|
| `GET /calendar.ics` | every enabled, cron-scheduled job (directed acyclic graph, or DAG, schedules appear as their `dag:<name>` job) |
| `GET /jobs/{name}/calendar.ics` | one job (or one DAG schedule); `404` for an unknown name |

Query parameters (values outside the range are clamped):

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

- **`DTSTART` in UTC** (`...Z` form). The calendar client converts event times to its display time zone; no `VTIMEZONE` blocks are needed.
- **A stable `UID`** (hashed job name plus the fire instant), so a subscribed client updates events in place across refreshes instead of duplicating them.
- **A duration from run history**: the job's typical runtime rounded up to a whole minute, with a minimum of 5 minutes for visibility and a maximum of 24 hours. The description states the actual average.
- **`TRANSP:TRANSPARENT`**: a maintenance window on your calendar does not mark you busy.
- **`SUMMARY`** is the job name. **`DESCRIPTION`** is the schedule expression, its plain-English description, the job's time zone, and the typical runtime when known.
- **Refresh hints** (`REFRESH-INTERVAL` / `X-PUBLISHED-TTL`, one hour) for subscription clients that honor them.

Feeds contain scheduling information only. They exclude command lines, environment variables, and output because subscribers may store the data on phones or third-party calendar services.

## Authentication for calendar clients

With [`web.authToken`](HTTP-API) unset, the feeds are as open as the rest of the read API. For calendar clients that cannot send an `Authorization` header, the `.ics` endpoints also accept a `token` query parameter:

```console
curl "http://localhost:8080/calendar.ics?token=s3cret"
```

Subscribe with the full URL. Other API paths require the bearer header. Invalid or missing credentials return `401`. Keep the subscription URL private: anyone holding it can read the fleet's schedule until the token rotates.

## Subscribing

In a calendar app that supports subscriptions "from URL", paste the feed URL (with `?token=` when auth is on). Google Calendar, Apple Calendar, Outlook, and Thunderbird poll on their own schedules, typically every few hours. The feed suggests a one-hour refresh interval and regenerates on every request.

## The week calendar in the dashboard

The `◫ week` toolbar button opens a seven-day grid of scheduled runs, starting today:

- Runs follow each job's time zone and **appear in your browser's local time**. The dashed line marks the current time.
- Each chip represents a scheduled run, colored by job. Chips sharing a quarter-hour split the column. Past times today appear dimmed. Clicking a chip opens the job's **Schedule** tab.
- **High-frequency jobs** (more than about eight runs a day) appear in the **background hum** summary strip below the grid. Clicking a strip chip opens the same job drawer.
- The card header links to the fleet `.ics` feed; each job's **Schedule** tab links to its feed. Both links include the token when authentication is enabled.

The view is a persisted preference like the other dashboard panels, and appears in the command palette as "Toggle week calendar". The [terminal dashboard](Terminal-Dashboard) carries the same panel under the same palette command: a day-by-hour fire grid, the agenda, and the hum strip, rendered in UTC.

See also: [Web Dashboard](Web-Dashboard), [HTTP Control API](HTTP-API), [Business-Day Schedules](Business-Day-Schedules), and [Schedule Pressure](Schedule-Pressure).
