# Schedule load

Schedule load shows when jobs are scheduled to run over the next 24 hours.
Use the hour-by-minute grid to find crowded times, such as the start of
each hour, before choosing a schedule for another job.

The forecast uses the scheduler’s engine, including each job’s timezone
and daylight saving time rules. For schedules with seconds, it counts
every scheduled run within each minute.

Disabled jobs and `@reboot` jobs are excluded because they do not have
upcoming scheduled runs. [Workflows](Orchestration-and-DAGs) are included
under their schedule names, `dag:<name>`.

## The endpoint

```
GET /schedule/pressure?hours=24&tz=Europe/London
```

Both parameters are optional: `hours` is 1 to 168 (default 24), and `tz` sets the grid’s display timezone (default UTC). Each job still follows its own configured timezone. The payload has these fields:

| Field | Meaning |
|-------|---------|
| `grid` | 24 rows (hour of day) of 60 scheduled-run counts (minute of hour). |
| `by_minute_fires` / `by_minute_jobs` | The 60-bin histogram: scheduled runs and distinct jobs at each minute of the hour, across the whole window. |
| `by_hour` | Scheduled runs per hour row. |
| `busiest_minute` | `{minute, jobs, fires}`: the "37 jobs are scheduled at :00" headline. |
| `empty_minutes` | The minutes of the hour with no scheduled runs. |
| `top_cells` | The busiest times, each naming up to ten of its jobs. |
| `jobs`, `total_fires`, `excluded` | Fleet totals, plus how many jobs were excluded as disabled or `@reboot`. |

See [HTTP API](HTTP-API) for the route table. The same analyzer backs the `cron_schedule_pressure` [MCP tool](MCP), so an AI agent can check schedule load directly.

## In the dashboards

- The [web dashboard](Web-Dashboard) has a **schedule load** card (the `▥ schedule load` toolbar toggle): the 24x60 grid with hot cells highlighted, the minute-of-hour histogram, the [duplicate-schedule groups](Duplicate-Schedule-Detection), and the [suggest-a-slot](Suggest-a-Slot) buttons, with a UTC/local display-zone switch. Whenever the panel is enabled, the wallboard (TV mode) shows a compact pressure strip above the tile grid, to highlight times when many jobs are scheduled together.
- The [terminal dashboard](Terminal-Dashboard) has the same panel as an overlay (command palette: "Toggle schedule load"), computed locally from its `/jobs` snapshot with the identical shared analyzer, so it works against older daemons too.

Both refresh about once a minute.

## Reading it

A more even grid means runs are spread across the available times. Look for these patterns:

- **A hot `:00` column** is the classic collision: cron's default minute. Spread it with [`H` hashed schedules](Hashed-Schedules), or move individual jobs to minutes [suggested from real load](Suggest-a-Slot).
- **Hot columns at `:00/:15/:30/:45`** mean everyone picked the same round step phases; `H/15` keeps the cadence and spreads the phase.
- **A solid row** is an hourly window where many daily jobs pile up (backup hour, report hour). Check the row's cells before adding another job there.
- **Many identical rows** usually mean [duplicate schedules](Duplicate-Schedule-Detection): the same expression pasted across jobs.
