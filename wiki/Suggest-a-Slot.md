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

The choice is deterministic for a given forecast: fewest scheduled runs first, then the slot farthest around the clock from the busiest one, then the earliest slot. For an idle fleet, this rule selects `:30`.

`busiest` identifies the most crowded slot, `alternatives` gives two runners-up, and `hash_hint` suggests an [`H` expression](Hashed-Schedules) for automatic distribution.

The same analyzer backs the `cron_suggest_slot` [Model Context Protocol (MCP) tool](MCP), so an agent asked to "add a cleanup job" can pick a schedule that does not add to a crowded slot.

## In the dashboards

The [web dashboard](Web-Dashboard)'s schedule load card has **suggest an hourly slot** and **suggest a daily slot** buttons. The suggested expression is a chip you click to copy. The [terminal dashboard](Terminal-Dashboard)'s schedule load overlay shows both suggestions inline, computed locally from the same shared analyzer.

## Suggest versus H

A suggested slot gives you an explicit minute based on the current forecast; it does not rebalance later. An [`H` schedule](Hashed-Schedules) derives a stable slot from each job's name. Use a suggestion when the time must be explicit in the configuration, or `H` to assign slots automatically.
