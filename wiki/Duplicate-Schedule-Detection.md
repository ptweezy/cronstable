# Duplicate schedule detection

Find jobs with identical schedules so you can spread their runs out.
Cronstable groups jobs by their parsed schedule and timezone, rather
than the spelling of the cron expression. For example, `*/5 * * * *`
matches `0-59/5 * * * *`, and `@hourly` matches `0 * * * *`.

An [`H` schedule](Hashed-Schedules) joins a group when its resolved
schedule matches. Disabled jobs and `@reboot` jobs are excluded.
Jobs without identical schedules can still have individual run times
in common; use [schedule load](Schedule-Pressure) to see those overlaps.

## The endpoint

```
GET /schedule/duplicates
```

```json
{
  "jobs": 41,
  "groups": [
    {
      "expression": "0 0 * * *",
      "description": "At 00:00, every day",
      "timezone": "UTC",
      "count": 14,
      "jobs": ["billing-export", "cleanup-tmp", "..."]
    }
  ]
}
```

Groups are sorted largest first. `expression` is the most common source spelling among the members, and `description` is the shared schedule in plain English. The same data backs the `cron_schedule_duplicates` [Model Context Protocol (MCP) tool](MCP).

## In the dashboards

The [web dashboard](Web-Dashboard)'s schedule load card lists the groups as clickable job chips. A chip opens that job's schedule tab. The [terminal dashboard](Terminal-Dashboard)'s schedule load overlay shows the top groups inline.

## What to do with a group

A duplicate group is not automatically a problem: four probes that must all fire each minute are supposed to coincide. The group becomes actionable when the members are independent batch work that happened to copy the same expression. Then either spread them with [`H` hashed schedules](Hashed-Schedules) (one edit per job, no coordination) or give each a concrete minute from [suggest a slot](Suggest-a-Slot).
