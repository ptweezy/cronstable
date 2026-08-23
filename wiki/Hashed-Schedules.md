# Hashed schedules (H)

`H * * * *` runs a job once an hour, at a minute cronstable picks by hashing the job's name. Every job that uses `H` lands on its own stable slot, so a fleet of hourly jobs spreads across the hour instead of firing together at `:00`. The syntax is Jenkins-style and works in any field except the year.

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

Random jitter also spreads load, but it destroys the question a monitoring product must answer: "was this run late?" A randomly jittered job has no fixed expected time, so lateness is undefined.

A hashed slot is a pure function of the job name. It survives restarts, config reloads, and redeploys, and it is identical on every replica of the same config. The job fires at the same minute today, tomorrow, and on the standby node, so [late-run detection](Late-Run-Detection), `scheduled_in` countdowns, and the dashboards' next-fire previews all keep working.

That predictability has a cost: **renaming a job re-hashes its slots**, because the name is the seed. In a [classic crontab file](Classic-Crontabs), names embed the line number, so inserting a line above an `H` entry moves the slots the same way.

## Forms

| Form | Meaning |
|------|---------|
| `H` | One hashed value from the field's whole range. In day-of-month, every rangeless `H` form (bare `H` and `H/n` alike) hashes over 1 to 28, so a short month is never silently skipped. To opt back in to the full range, write `H(1-31)`. |
| `H(a-b)` | One hashed value from the numeric range `a` to `b` (`H(0-29)` picks a first-half-hour minute). |
| `H/n` | Every `n`, starting at a hashed offset. A minute `H/15` fires four times an hour at `p`, `p+15`, `p+30`, `p+45` for this job's own phase `p`. In day-of-month, the steps stay within 1 to 28, like bare `H`. |
| `H(a-b)/n` | Every `n` within `a` to `b`, phase hashed. The step must not exceed the range's span. |

Details that keep the behavior boring and predictable:

- The hash is a SHA-256 of the job name, salted per field, so `H H * * *` picks an uncorrelated minute and hour rather than the same residue twice. It does not depend on Python's `hash()`, and it is identical across processes, hosts, and versions. Tests pin the concrete slots.
- Because the moduli divide each other, a job's bare `H` minute and its `H/15` phase agree (`43` and `13,28,43,58` for the same name), so tightening or loosening a job's cadence keeps it on familiar minutes.
- `H` resolves at config load, before scheduling. Everything downstream (matching, next-fire search, and [semantic schedule equality](Schedules-and-Timezones)) sees plain values, and `H * * * *` compares equal to the `43 * * * *` it resolved to.
- A minute `H/7` gets the same [`uneven-step` lint warning](Schedule-Linting) as `*/7`: seven does not divide sixty, so one interval at the wrap is short.

## Where the resolution shows

The original `H` spelling is what you configured, so it is what the surfaces display; the resolved values ride along everywhere they matter:

- The [schedule linter](Schedule-Linting) attaches a `hashed-slot` note to every `H` job. The note names the exact expression the schedule resolved to.
- For `H` jobs, `GET /jobs` adds `schedule_resolved` next to `schedule`. `GET /schedule/preview` takes a `seed` parameter (a job name, real or prospective) so sandboxes can resolve `H` expressions. See the [HTTP API](HTTP-API).
- The [web dashboard](Web-Dashboard)'s job drawer shows "`H * * * *` (H resolves to `9 * * * *`)". Its previews and collision analysis compute from the resolved form.
- The job-set [fingerprint](Job-Set-ID) hashes the schedule as written, so an `H` schedule fingerprints as `H`. Identical configs still agree across replicas because the resolution is deterministic.

## Pairs with schedule pressure

[Schedule pressure](Schedule-Pressure) reports the problem (`37 jobs fire at :00`). `H` is the remedy you can apply per job without anyone coordinating slot assignments by hand. When you want a concrete, explicit minute instead, [suggest a slot](Suggest-a-Slot) is the middle path.
