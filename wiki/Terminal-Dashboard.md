# Terminal dashboard

`cronstable tui` is the [web dashboard](Web-Dashboard)'s terminal
sibling: the same board, keyboard-first, rendered in your terminal: an
SSH session, a tmux pane, a serial console, or a machine where running
a browser is impractical. It is a client of the same
[HTTP control API](HTTP-API) the web page uses, so there is nothing
extra to enable on the daemon: if the dashboard works, so does the
terminal user interface (TUI).

[![The cronstable TUI against a live 9-node fleet: 59 jobs with status glyphs, next-fire countdowns, run sparklines, live CPU/memory chips, the cluster owner column, and the verdict bar](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/tui-overview.png)](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/tui-overview.png)

*(This screenshot, like the larger gallery in the README's Terminal
dashboard section, is the real TUI driven against the running
[grand tour](https://github.com/ptweezy/cronstable/tree/main/example/grand-tour)
fleet, the same one the web dashboard's screenshots use.)*

```shell
cronstable tui                              # local daemon on :8080
cronstable tui --url http://prod-node:8080  # a remote daemon
cronstable tui --tv                         # straight to the wallboard
cronstable tui --job nightly-backup         # deep-link a job's drawer
```

It is hand-rolled on the standard library plus the core `aiohttp`
dependency, following the same zero-new-dependency rule as the
[Model Context Protocol (MCP) server](MCP). It works on Linux, macOS,
and Windows, and ships in the same package and binaries as the daemon.

It does need a real terminal. If stdin or stdout is not a tty (a pipe, a
redirect, a CI runner), it refuses to start, printing `cronstable tui
needs an interactive terminal (stdin/stdout are not a tty)` to stderr
and exiting with code 2. On Windows it turns on the console's ANSI/VT
processing itself, so a stock Command Prompt or PowerShell window works.
For fonts missing the status glyphs, use `--ascii`.

## Options

| Flag | Meaning |
| --- | --- |
| `--url URL` | Daemon web listener (default `http://127.0.0.1:8080`). |
| `--token TOKEN` | Bearer token for `web.authToken`-protected daemons. |
| `--token-env VAR` | Environment variable to read the token from when `--token` is absent (default `CRONSTABLE_WEB_TOKEN`). |
| `--theme NAME` | Start on a theme (`standard`, `carolina`, `amber`, `green`, `modern`, each also as `NAME-light`). The choice persists (default: remembered, else `standard`). |
| `--tv` | Start on the wallboard, like opening the page at `#tv`. |
| `--job NAME` | Open a job's drawer at startup, like `#job/NAME`. |
| `--poll SECONDS` | Refresh interval. `0` pauses (default: remembered, else 3). |
| `--boot` / `--no-boot` | Force or skip the boot self-test. |
| `--ascii` | Plain-ASCII status glyphs for limited fonts and terminals. |

## Keyboard shortcuts

The web page's shortcut table applies verbatim to both frontends. Press
`?` at any time for the overlay.

| Key | Action |
| --- | --- |
| `Ctrl-K` / `Ctrl-P` | Open the command palette |
| `/` | Focus the filter |
| `j` / `↓`, `k` / `↑` | Select the next / previous job |
| `Enter` | Open the selected job |
| `r` / `x` | Run / cancel the selected job |
| `p` | Pause or resume the selected job |
| `c` | Copy the selected job's command |
| `g` | Refresh now |
| `t` / `T` | Cycle theme / flip dark ↔ paper |
| `i` | Incident timeline |
| `w` | Wallboard (TV) mode |
| `a` | Acknowledge the failure alarm |
| `?` | The shortcut overlay |
| `Esc` | Close the open panel or drawer |

Terminal-only extras are grouped separately in the `?` overlay. `q`
quits, `s`/`S` cycle the sort key/direction, `f` cycles the status
filter, `m` opens the multi-tail console, `←`/`→` (or `Tab`) switch
drawer tabs, and `PgUp`/`PgDn` scroll.

Inside the **Logs** tab, `f`/`t`/`w` toggle follow/timestamps/wrap;
`/` searches, with `n`/`N` for next/previous match; and `d` saves the
log to your home directory as
`cronstable-<job>-<YYYYmmdd-HHMMSS>.log` (a toast confirms the exact
path).

## What made the trip

Everything an operator drives from the web page:

- The **jobs board**: status glyphs, next-fire countdowns, last-run
  ages, duration sparklines, live CPU/memory chips for monitored jobs,
  the owner column under a spread cluster, filtering, sorting, and the
  status segments. A [paused](Pausing-Jobs) job shows the `⏸` glyph
  (`p` in `--ascii` mode) with `⏸ til HH:MM` in the next-fire column,
  and `p` toggles pause/resume (also in the palette). A job
  [late on a service level agreement (SLA) check](Late-Run-Detection)
  carries an **OVERDUE** suffix in its status cell and wallboard tile.
- The **job drawer**: the live **Server-Sent-Events (SSE) log tail**
  (ANSI colors re-inked per theme, search, follow, wrap, timestamps,
  save-to-file), **run history** with success rate and per-run bars,
  **resources** for monitored jobs, and the **schedule tab**, whose
  plain-English text and next-fire preview come from the daemon's own
  cron engine.
- The **fuzzy command palette**, with the same global, per-job, and
  per-DAG (directed acyclic graph) actions.
- The **verdict bar** and **incident timeline**, with the same
  failure-correlation logic ("×4 share exit=69 — likely one cause").
  The **mitigate console** has staggered bulk start/cancel, **abort**,
  and a Markdown **incident writeup**, copied to the clipboard and saved
  to `~/cronstable-incident-<YYYYmmdd-HHMMSS>.md`. If the file write
  fails, the clipboard copy still stands.
- The **multi-tail console**: up to four jobs' live logs merged with
  identity-colored prefixes.
- The **DAG drawer**: runs, an ASCII task graph, per-task states and
  attempts, **approval gates** (`a` approve / `R` reject), XCom
  (cross-communication) values, task logs, trigger and backfill.
- The **cluster panel**, **fleet matrix** (jobs × nodes, failing-only
  filter), **node resources**, **activity heatmap** punchcard, and
  **next-fire radar**.
- The **schedule pressure** overlay (`Ctrl-K` → "Toggle schedule
  pressure"): the next 24 hours of fires as an hour-by-minute collision
  grid with a minute histogram, the fleet's
  [duplicate-schedule groups](Duplicate-Schedule-Detection), and the
  [least-loaded-slot suggestions](Suggest-a-Slot), computed locally from
  the `/jobs` snapshot with the daemon's own shared analyzers (see
  [schedule pressure](Schedule-Pressure)), so it works against older
  daemons too.
- The **week calendar** overlay (`Ctrl-K` → "Toggle week calendar"): the
  web dashboard's seven-day view, terminal-shaped: a day-by-hour shaded
  fire grid, a chronological agenda of the calendar-worthy fires (all
  labels UTC, the TUI's frame everywhere), and the same background-hum
  rule, so a minutely job summarizes to one name-and-count line instead
  of flooding the agenda. It is computed locally like pressure, and is
  the same data the daemon serves as an iCal feed (see
  [calendar export](Calendar-Export)).
- The **state inspector** for the durable store (inventory, document
  namespaces, record streams).
- The **cron sandbox** (`Ctrl-K` → "Cron sandbox"), evaluating
  expressions live against the daemon's own engine, with the
  [schedule linter's](Schedule-Linting) advisory findings inline. A
  job's schedule drawer shows the same findings in the job's own time
  zone, so DST notes carry real dates.
- The **wallboard** (`w`) with worst-first tiles, the tally foot, a
  `NO SIGNAL` banner when data goes stale, and the zen screensaver on an
  idle board (nothing failing or running, data fresh).
- The **BIOS-style boot self-test**, probing the daemon for real, once
  per 12 hours. Skip it with any key, `--no-boot`, or a settings toggle.

Everything painted into the terminal is sanitized first. Raw log lines
get log-viewer carriage-return semantics: only the last non-empty `\r`
segment of a line is kept, so progress bars and cmd.exe's CRLF output
collapse cleanly. Tabs expand, and other control characters are dropped.

Every escape sequence except SGR styling is scrubbed from job output and
from API-derived strings such as job and node names, which under
clustering arrive from other machines over gossip. Untrusted output can
color a line, but it can never move the cursor, retitle your window, or
write your clipboard with OSC 52.

Some features stay web-only: desktop notifications (the TUI rings the
terminal bell instead, off by default), the run ledger, and the
pendulum wordmark.

## Themes and accessibility

The same five hues as the web page: **standard** (default, flat
neutral), **carolina**, **amber** and **green**, and flat **modern**.
Each has a dark and a light (paper) variant. `t` cycles hues and `T`
flips the variant, exactly as in the browser.

The **color-vision** remaps (red-green and blue-yellow) re-ink the
status colors with the same shape-differs-too guarantee, and `--ascii`
swaps the status glyphs for plain ASCII.

Preferences (theme, refresh, toggles) persist in a small JSON file, the
TUI's analogue of the page's `localStorage`:
`%APPDATA%\cronstable\tui.json` on Windows,
`$XDG_CONFIG_HOME/cronstable/tui.json` (or `~/.config/...`) elsewhere.

## Settings

Open the settings panel from the command palette (`Ctrl-K` → "Open
settings"); it has no dedicated key. `j`/`k` (or the arrow keys) select
a row. `Enter`, `Space`, `←` or `→` cycle or toggle the selected value.
Every change saves immediately to the prefs file, whose path the panel's
footer shows.

The panel has twelve rows. **Theme**, **Light / dark**, **Color
vision**, and **ASCII glyphs** are the
[themes and accessibility](#themes-and-accessibility) knobs described
earlier. **Refresh interval** is the `--poll` cadence, 1s–10s or paused.
**Wrap log lines** and **Log timestamps** are the **Logs** tab's `w`/`t`
toggles. **Audible cues (bell)** is off by default. The panel also has
**Boot self-test**. Two rows live only here:

- **Compact density** drops the schedule and sparkline columns from
  the jobs board so the rest fits a narrower terminal (also in the
  palette as "Toggle compact density").
- **Zen screensaver** and **Zen idle** govern the wallboard's
  screensaver. On a board (nothing failing or running, data fresh), it
  engages after the keyboard has been idle for the **Zen idle**
  interval: 30, 60, 90, 120, or 300 seconds, default 90. Any key wakes
  it without acting.

## Clipboard

The copy actions are `c` on a job, the palette's "Copy version" and
"Copy job set id", and the incident writeup. Each takes two paths at
once: an OSC 52 escape asking the terminal emulator itself to set the
system clipboard, plus the platform's copy tool (`clip.exe` on Windows,
`pbcopy` on macOS, `wl-copy` or `xclip` on Linux, whichever is
installed).

Over SSH the platform tool runs on the remote machine, so OSC 52 is the
only path to your local clipboard. It needs an emulator that supports
OSC 52; most modern ones do, and tmux passes it through only when its
`set-clipboard` option allows.

A failed copy is silent: the TUI cannot see whether the OSC 52 escape
landed, so it reports success either way. If pastes come up empty in a
remote session, check the emulator's OSC 52 support. The incident
writeup is the one copy that also lands in a file.

## Authentication

With [`web.authToken`](HTTP-API#authentication) enabled, pass the token
with `--token`, or export it and let the default `--token-env
CRONSTABLE_WEB_TOKEN` pick it up. Exactly like the page, an
unauthenticated start is fine: the first `401` opens the token prompt,
and the token is kept for the session only, never written to the prefs
file.

Mutating keys (`r`, `x`, DAG trigger/backfill/decision) go through the
same `POST` endpoints. The daemon's cross-site `Origin` gate does not
apply to a native client, so no extra configuration is needed.

## What it polls

The endpoints and cadence model match the web page's: `GET /jobs` on
the refresh interval (1s–10s or paused, default 3s), with `/cluster`
and `/node` riding each successful poll. `/fleet`, `/state`,
and the heatmap's one-request `/activity` batch are polled only while
their panels are open, with the per-job `/jobs/{name}/runs` fan-out kept
as the fallback against a daemon without that batch.
`GET /jobs/{name}/logs` is an SSE stream while a **Logs** tab or
multi-tail pane is attached (replay-then-follow, with the page's same
reconnect throttle).

Run `cronstable tui` against any daemon you can `curl`.

## See also

- [Web Dashboard](Web-Dashboard): the browser original, every surface
  the TUI mirrors, annotated with screenshots.
- [HTTP Control API](HTTP-API): the endpoints and authentication both
  frontends are built on.
- [Pausing Jobs](Pausing-Jobs): the runtime pause behind the `⏸` glyph
  and the `p` key.
- [Late-Run Detection](Late-Run-Detection): the `sla:` monitor behind
  the **OVERDUE** suffix.
- [MCP](MCP): the third frontend, the same daemon, for artificial
  intelligence agents.
