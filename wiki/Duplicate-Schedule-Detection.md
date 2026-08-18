# Duplicate schedule detection

Fourteen jobs sharing `0 0 * * *` is a fact worth surfacing before midnight proves it. Using the engine's own semantic equality, cronstable groups the fleet's schedules and reports every group of two or more jobs that fire on the identical set of instants.

Semantic, not textual: `*/5 * * * *`, `0-59/5 * * * *`, and `@hourly` versus `0 * * * *` group together because the parsed field sets are equal. The scheduler itself uses that same equality to keep an unchanged job's next-fire instant across reloads.

An [`H` schedule](Hashed-Schedules) joins a group only if its resolved slot coincides with the others, which is exactly when it collides. The grouping also includes each job's resolved time zone, so two `0 0 * * *` jobs in different zones, which never fire together, are not called duplicates.

Disabled and `@reboot` jobs are excluded for the same reason they are excluded from [schedule pressure](Schedule-Pressure): they cannot collide with anything.

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

The [web dashboard](Web-Dashboard)'s schedule-pressure card lists the groups as clickable job chips. A chip opens that job's schedule tab. The [terminal dashboard](Terminal-Dashboard)'s pressure overlay shows the top groups inline.

## What to do with a group

A duplicate group is not automatically a problem: four probes that must all fire each minute are supposed to coincide. The group becomes actionable when the members are independent batch work that happened to copy the same expression. Then either spread them with [`H` hashed schedules](Hashed-Schedules) (one edit per job, no coordination) or give each a concrete minute from [suggest a slot](Suggest-a-Slot).
