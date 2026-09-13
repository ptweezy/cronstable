# Suggest a slot

Find a quieter time for a new job. Cronstable compares upcoming runs over
the next 24 hours and suggests a time with the fewest scheduled runs,
using the same forecast as [schedule load](Schedule-Pressure).

```
GET /schedule/suggest?period=hourly
GET /schedule/suggest?period=daily&tz=Europe/London
```

`period=hourly` picks a minute of the hour (a `<m> * * * *` schedule); `period=daily` picks a minute and hour (`<m> <h> * * *`). `tz` sets the timezone for the daily suggestion (default UTC).

```json
{
  "period": "hourly",
  "minute": 29,
  "expression": "29 * * * *",
  "fires_in_window": 0,
  "busiest": { "minute": 0, "fires_in_window": 851 },
  "alternatives": [
    { "minute": 31, "expression": "31 * * * *", "fires_in_window": 0 },
    { "minute": 28, "expression": "28 * * * *", "fires_in_window": 0 }
  ],
  "based_on": { "jobs": 41, "start": "2026-07-18T16:20:00+00:00", "hours": 24 },
  "hash_hint": "H * * * *"
}
```

The choice is deterministic, so the same fleet always gets the same answer: fewest scheduled runs first. Ties break toward the slot circularly farthest from the busiest one, then toward the earliest slot. That tie-break is why an idle fleet gets `:30`, not `:00`: the outside world crowds the top of the hour even when your fleet does not.

`busiest` is included for contrast. `alternatives` are the two runners-up. `hash_hint` names the [`H` spelling](Hashed-Schedules) that would keep future jobs spreading themselves without anyone consulting this endpoint again.

The same analyzer backs the `cron_suggest_slot` [Model Context Protocol (MCP) tool](MCP), so an agent asked to "add a cleanup job" can pick a schedule that does not add to a crowded slot.

## In the dashboards

The [web dashboard](Web-Dashboard)'s schedule load card has **suggest an hourly slot** and **suggest a daily slot** buttons. The suggested expression is a chip you click to copy. The [terminal dashboard](Terminal-Dashboard)'s schedule load overlay shows both suggestions inline, computed locally from the same shared analyzer.

## Suggest versus H

Both solve the same collision problem from different ends. A suggested slot is explicit: the schedule reads as a concrete minute. The cost is a point-in-time answer that no one re-balances later. An [`H` hashed slot](Hashed-Schedules) is self-maintaining: every job spreads itself. The cost is the minute living in the hash rather than the configuration file.

New fleets tend to standardize on `H`. Established fleets use suggest to place jobs that must keep an explicit, reviewable schedule.
