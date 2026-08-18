# Logging configuration

This page documents how cronstable produces its own diagnostic log output: the
default behavior driven by `-l/--log-level`, and the optional `logging:`
configuration section that applies a full Python `logging.config` dictionary
schema. It does not cover capturing a job's stdout/stderr (see
[output capturing](Output-Capturing)) or sending notifications on job
success/failure (see [reporting](Reporting)).

## Default logging (no `logging:` section)

When the configuration contains no `logging:` section, cronstable's log output is
governed entirely by the CLI. At startup, `__main__.py` calls:

```python
logging.basicConfig(level=log_level)
```

The level comes from `-l/--log-level` (default `INFO`). `logging.basicConfig`
installs a single `StreamHandler` on the root logger that writes to **stderr**
with the standard library default format
(`LEVEL:logger_name:message`). There is no timestamp in this default format.

`-l/--log-level` is upper-cased and resolved against the standard level names
(`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`, plus the `logging` module's
aliases such as `WARN`). An unknown value exits `2` as a usage error. See
[command-line reference](CLI-Reference) for the full CLI.

This default applies whether or not the run later loads a `logging:` section.
`basicConfig` always runs first. If a `logging:` section is present, it is
applied afterwards during the scheduler loop, overriding the default.

## The `logging:` section

The `logging:` section is a Python `logging.config` *dictionary schema*
(the same structure accepted by `logging.config.dictConfig`). cronstable
validates its top-level shape with strictyaml and then hands the whole
dictionary to `logging.config.dictConfig`.

```yaml
logging:
  version: 1
  disable_existing_loggers: false
  formatters:
    simple:
      format: '%(asctime)s [%(processName)s/%(threadName)s] %(levelname)s (%(name)s): %(message)s'
      datefmt: '%Y-%m-%d %H:%M:%S'
  handlers:
    console:
      class: logging.StreamHandler
      level: DEBUG
      formatter: simple
      stream: ext://sys.stdout
  root:
    level: INFO
    handlers:
      - console
```

The preceding example, from `README.md`, displays each log line with an
embedded timestamp and routes all root-logger output to stdout with a `simple`
formatter.

> The ability to configure yacron's own logging was added in yacron 0.19.0
> (upstream issues #81/#82/#83). The `datefmt` line in the README example was a
> later fix.

### Top-level keys

The strictyaml schema validates only the *top-level* keys of the `logging:`
map, and the types of `version`, `incremental`, and `disable_existing_loggers`.
It accepts the contents of `formatters`, `filters`, `handlers`, `loggers`, and
`root` as arbitrary YAML (strictyaml `Any`), and `dictConfig` validates them
only later. An error inside one of those nested mappings is therefore not
caught at config-parse time. It surfaces when `dictConfig` runs (see
[reload and error handling](#reload-and-error-handling)).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `version` | int | (required) | dictConfig schema version. Must be present. `logging.config.dictConfig` currently accepts only `1`. |
| `incremental` | bool | optional (dictConfig default `false`) | If `true`, `dictConfig` interprets the configuration incrementally: it keeps existing loggers/handlers, adjusts only their *levels* and `propagate` flags, and ignores `formatters`/`filters` and handler creation. See the dictConfig docs. |
| `disable_existing_loggers` | bool | optional (dictConfig default `true`) | If `true` (the dictConfig default), `dictConfig` disables loggers that exist when it runs but are not named in this configuration. The README example sets this to `false` so previously created loggers, such as `cronstable`, keep working. Ignored when `incremental` is `true`. |
| `formatters` | mapping | optional | Named formatter definitions (`format`, `datefmt`, and other keys), as in dictConfig. Contents unvalidated by strictyaml. |
| `filters` | mapping | optional | Named filter definitions, as in dictConfig. Contents unvalidated by strictyaml. |
| `handlers` | mapping | optional | Named handler definitions (`class`, `level`, `formatter`, `stream`, and other keys). Contents unvalidated by strictyaml. |
| `loggers` | mapping | optional | Per-logger configuration (`level`, `handlers`, `propagate`). Contents unvalidated by strictyaml. |
| `root` | mapping | optional | Configuration of the root logger (`level`, `handlers`). Contents unvalidated by strictyaml. |

The defaults shown for `incremental` and `disable_existing_loggers` are the
defaults of `logging.config.dictConfig` itself. They are *not* defined in
cronstable's `DEFAULT_CONFIG`. cronstable supplies no values for any logging
key; what you write passes through verbatim. The schema requires only
`version`; all other keys are optional (strictyaml `Opt(...)`).

### Logger names used by cronstable

cronstable emits log records under these logger names. Target them in `loggers:`
to tune their levels independently, or rely on `root:` to catch them all:

| Logger | Source module | Emits |
| --- | --- | --- |
| `cronstable` | `cron.py`, `job.py` | Scheduler lifecycle, job start/spawn/exit, retries, web server start/stop, shutdown, and most operational messages. |
| `cronstable.config` | `config.py` | Configuration parsing diagnostics, such as the converted schedule string at `DEBUG`. |
| `statsd` | `statsd.py` | statsd metric-writer diagnostics. See [metrics with statsd](Metrics-with-Statsd). |
| `prometheus` | `prometheus.py` | Prometheus `/metrics` endpoint diagnostics, such as a cluster-backend read failing during a scrape. See [metrics with Prometheus](Metrics-with-Prometheus). |

Because `cronstable.config` is a child of `cronstable`, configuring the `cronstable`
logger affects it too (subject to `propagate`). The `statsd` logger is a
separate top-level logger.

## Reload and error handling

The daemon re-reads its configuration on every scheduler tick (roughly once per
minute; see [architecture and internals](Architecture-and-Internals)). The
`logging:` section participates in this reload with specific rules, implemented
in `cron.py`:

- The logging configuration is applied with `logging.config.dictConfig`.
- It is **only re-applied when it changes.** The scheduler keeps the
  last successfully applied logging dictionary and compares the freshly loaded
  one against it. If they are equal, the scheduler does not call `dictConfig`
  again.
- It is **only marked as applied on success.** If `dictConfig` raises, the
  scheduler logs an error (`Error while configuring logging: ...`, which points
  at the dictConfig schema documentation and includes the offending
  configuration) and does **not** record it as applied.
- Consequently, a `logging:` section that was **broken and then fixed** is
  picked up on the next reload **without restarting cronstable**. The broken
  version was never marked applied, so the corrected version still counts as
  "changed" and is retried.

This behavior (re-apply on change, mark applied only on success) was
introduced as a fix so a logging section fixed after an error, or changed at
runtime, is picked up without a restart.

If the loaded configuration has no `logging:` section, the scheduler never calls
`dictConfig`, and whatever logging configuration is currently in effect (the
startup `basicConfig`, or a previously-applied `logging:` section) remains
active.

## One logging block per configuration

At most **one** `logging:` block may exist across an entire configuration:

- Within a single file plus its `include:`s, a second `logging:` block raises
  `ConfigError("multiple logging configs")`.
- Across a configuration *directory* (multiple `.yml`/`.yaml` files), a second
  file containing a `logging:` block raises
  `ConfigError("Multiple 'logging' configurations found: first in <file>, now
  in <file>")`.

See [includes, defaults, and multi-file config](Includes-and-Defaults) for how
files in a directory are aggregated and for the matching rule that applies to the
`web:` block.

## Notes

- The `logging:` section configures **cronstable's own** logging only. It has no
  effect on how a job's captured output is stored or reported.
- Validating the configuration with `-v/--validate-config` checks the top-level
  schema of the `logging:` section but does **not** call `dictConfig`. A nested
  error, such as an unknown handler class, is therefore not detected by
  validation. It surfaces only when the daemon actually applies the
  configuration.
- For the complete configuration schema, see
  [configuration reference](Configuration-Reference).
