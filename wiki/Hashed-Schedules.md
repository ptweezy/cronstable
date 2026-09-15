# Hashed schedules (H)

`H * * * *` runs a job once an hour, at a stable minute chosen by hashing its name. This spreads hourly jobs across the hour, reducing the number scheduled at `:00`. The Jenkins-style syntax works in any field except the year.

```yaml
jobs:
  - name: refresh-cache
    command: ./refresh
    schedule: "H * * * *"     # this job's own minute, every hour
  - name: nightly-report
    command: ./report
    schedule: "H H * * *"     # a stable minute AND hour, once a day
```

## Why a hash and not jitter

Hashing spreads load while keeping each job's expected run time fixed, which supports lateness checks and schedule previews.

For a given expression and job name, a hashed slot stays the same across restarts, config reloads, redeploys, and replicas. [Late-run detection](Late-Run-Detection), `scheduled_in` countdowns, and dashboard previews use that fixed schedule.

That predictability has a cost: **renaming a job re-hashes its slots**, because the name is the seed. In a [classic crontab file](Classic-Crontabs), names embed the line number, so inserting a line above an `H` entry moves the slots the same way.

## Forms

| Form | Meaning |
|------|---------|
| `H` | One hashed value from the field's whole range. In day-of-month, every rangeless `H` form (bare `H` and `H/n` alike) hashes over 1 to 28, so a short month is never silently skipped. To opt back in to the full range, write `H(1-31)`. |
| `H(a-b)` | One hashed value from the numeric range `a` to `b` (`H(0-29)` picks a first-half-hour minute). |
| `H/n` | Every `n`, starting at a hashed offset. A minute `H/15` fires four times an hour at `p`, `p+15`, `p+30`, `p+45` for this job's own phase `p`. In day-of-month, the steps stay within 1 to 28, like bare `H`. |
| `H(a-b)/n` | Every `n` within `a` to `b`, phase hashed. The step must not exceed the range's span. |

Resolution rules:

- The hash is a SHA-256 of the job name, salted per field, so `H H * * *` picks an uncorrelated minute and hour rather than the same residue twice. It does not depend on Python's `hash()`, and it is identical across processes, hosts, and versions. Tests pin the concrete slots.
- Because the moduli divide each other, a job's bare `H` minute and its `H/15` phase agree (`43` and `13,28,43,58` for the same name), so tightening or loosening a job's cadence keeps it on familiar minutes.
- `H` resolves at config load, before scheduling. Everything downstream (matching, next-fire search, and [semantic schedule equality](Schedules-and-Timezones)) sees plain values, and `H * * * *` compares equal to the `43 * * * *` it resolved to.
- A minute `H/7` gets the same [`uneven-step` lint warning](Schedule-Linting) as `*/7`: seven does not divide sixty, so one interval at the wrap is short.

## Where the resolution shows

The API and dashboards display the original `H` expression alongside its resolved values:

- The [schedule linter](Schedule-Linting) attaches a `hashed-slot` note to every `H` job. The note names the exact expression the schedule resolved to.
- For `H` jobs, `GET /jobs` adds `schedule_resolved` next to `schedule`. `GET /schedule/preview` takes a `seed` parameter (a job name, real or prospective) so sandboxes can resolve `H` expressions. See the [HTTP API](HTTP-API).
- The [web dashboard](Web-Dashboard)'s job drawer shows "`H * * * *` (H resolves to `9 * * * *`)". Its previews and collision analysis compute from the resolved form.
- The job-set [fingerprint](Job-Set-ID) hashes the schedule as written, so an `H` schedule fingerprints as `H`. Identical configs still agree across replicas because the resolution is deterministic.

## Pairs with schedule load

Use [schedule load](Schedule-Pressure) to find crowded times, then apply `H` to spread jobs automatically. Use [suggest a slot](Suggest-a-Slot) to choose an explicit minute.
