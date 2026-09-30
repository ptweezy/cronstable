# Output capturing

This page documents how cronstable handles a job's standard output and standard
error: which streams are captured, how captured output is prefixed and
re-emitted, how much is retained for reports, and the line-length limit applied
to the underlying reader.

## Overview

For each job, cronstable decides per stream whether to *capture* it. The
decision is made independently for stdout (`captureStdout`, default `false`)
and stderr (`captureStderr`, default `true`).

- A **captured** stream is read line-by-line by cronstable. Each line is decoded
  as UTF-8, re-emitted to cronstable's own stdout/stderr with a configurable
  prefix, and retained in memory subject to `saveLimit`, so it can be included
  in [reports](Reporting) and exposed to
  [failure detection](Failure-Detection-and-Retries).
- An **uncaptured** stream is not piped through cronstable. The child process
  inherits cronstable's own stdout/stderr file descriptors, so its output passes
  through directly. Such output is *not* retained, *not* prefixed, and *not*
  available to reporters or to the `producesStdout`/`producesStderr` failure
  checks.

Whether a captured stream is re-emitted to cronstable's stdout or stderr depends
on the original stream, not on which stream was captured. Captured stdout lines
are written to cronstable's stdout, and captured stderr lines to cronstable's
stderr.

## Options

These options are per-job, and you may also set them in a `defaults` block (see
[includes, defaults, and multi-file config](Includes-and-Defaults)). All are
optional (`Opt(...)` in the schema). Types and defaults are taken from the
strictyaml schema and `DEFAULT_CONFIG`.

| Option | Type | Default | Description |
|---|---|---|---|
| `captureStdout` | boolean | `false` | Capture the job's standard output: read, prefix, re-emit to cronstable's stdout, and retain for reports/failure checks. |
| `captureStderr` | boolean | `true` | Capture the job's standard error: read, prefix, re-emit to cronstable's stderr, and retain for reports/failure checks. |
| `streamPrefix` | string | `"[{job_name} {stream_name}] "` | Format string prepended to each re-emitted captured line. Supports `{job_name}` and `{stream_name}`. Set to `""` to disable. |
| `saveLimit` | integer | `4096` | Maximum lines retained per captured stream for reporting. Must be `>= 0`. `0` retains nothing but still counts discarded lines. |
| `maxLineLength` | integer | `16777216` (16 MiB) | Maximum length of one captured line, in bytes. Must be `> 0`. Lines exceeding it are skipped with a warning. |

`saveLimit` and `maxLineLength` are validated at config load time. A non-integer
fails the strictyaml schema, and `saveLimit < 0` or `maxLineLength <= 0` raises
a `ConfigError`.

## What "capture" means

When a stream is captured, cronstable launches the subprocess with that stream
connected to a pipe (`asyncio.subprocess.PIPE`) and starts a `StreamReader` task
that reads the pipe in 64 KiB chunks (`stream.read(65536)`) and splits them on
newlines. For each line:

1. The raw bytes are decoded as strict UTF-8. On Windows, a line that is not
   valid UTF-8 is retried with the console's OEM code page. Anything still
   undecodable is decoded as UTF-8 with `errors="replace"`, so a job that
   emits invalid bytes does not crash the reader. Invalid sequences become
   the Unicode replacement character.
2. The decoded line, with `streamPrefix` formatted and prepended, is queued
   for cronstable's own stdout (for stdout lines) or stderr (for stderr
   lines). The queued lines go in batches to one mirror writer thread, so a
   stalled consumer of cronstable's output cannot block the scheduler; while
   the consumer stays stalled, the oldest batches are dropped. Job stderr is written to
   cronstable's stderr, never to stdout.
3. The (unprefixed) line is retained according to `saveLimit`.

If a stream is not captured, no pipe is created for it and no `StreamReader` is
started. The child inherits cronstable's corresponding file descriptor.

### Encoding of re-emitted lines

Re-emitted lines are written as encoded bytes to the underlying buffer, in the
stream's own declared encoding with `errors="replace"`, so a character that
encoding cannot represent becomes `?`. cronstable falls back to ASCII with
replacement only when the stream's encoding is unknown or broken. Retained
output (used in reports) is the decoded string and is unaffected by this
encoding step.

## streamPrefix

`streamPrefix` is a Python `str.format` template applied once per
`StreamReader`. Two placeholders are substituted:

- `{job_name}`: the job's `name`.
- `{stream_name}`: `"stdout"` or `"stderr"`.

With the default `"[{job_name} {stream_name}] "`, a job named `test-01` emits
lines such as `[test-01 stdout] hello`.

To change the prefix:

```yaml
jobs:
  - name: test-01
    command: echo "hello world"
    schedule:
      minute: "*/2"
    captureStdout: true
    streamPrefix: "[{job_name} job] "
```

To remove the prefix entirely (for example when the job emits structured JSON
log lines that should pass through unmodified), set it to the empty string:

```yaml
jobs:
  - name: test-01
    command: echo '{"msg":"hello world"}'
    schedule:
      minute: "*/2"
    captureStdout: true
    streamPrefix: ""
```

The default prefix ends with a space. If you want a separator, include it in
the prefix; a custom prefix is concatenated directly with the line.

## saveLimit and discarded-line accounting

`saveLimit` bounds how many lines per captured stream are retained for reporting.
The `StreamReader` does not keep the most recent `N` lines. It keeps the **first
half and the last half**, so both the beginning and the end of long output
survive although the middle is dropped:

- The first `saveLimit // 2` lines are stored in a top buffer.
- After the top buffer is full, subsequent lines go into a bottom buffer holding
  at most `saveLimit - saveLimit // 2` lines. When that bottom buffer is full,
  the oldest line in it is evicted and a discard counter is incremented.

When the retained output is assembled, if any lines were discarded a marker line
is inserted between the top and bottom halves:

```
   [.... N lines discarded ...]
```

where `N` is the number of discarded lines. The marker is only present when
discards occurred and when the bottom buffer is nonempty.

### saveLimit = 0

`saveLimit` may be set to `0`. With `saveLimit: 0`, no
lines are retained at all: every line is counted as discarded. The lines are
still decoded and re-emitted with their prefix as usual. Only the in-memory
retention for reports is suppressed. The discard count is preserved, which
matters for failure detection (described later).

## maxLineLength

`maxLineLength` (default 16 MiB) caps one line's length in bytes, measured
before decoding. The `StreamReader` enforces it with its own length check. The
asyncio `limit` set at spawn is the 64 KiB read chunk size, which only sets how
much unread output asyncio buffers per pipe. When a line, or a run of output
with no newline yet, grows past the cap, the reader drops it and logs a warning

```
job <name>: ignored a very long line
```

and keeps reading; whatever follows the dropped bytes is read as an ordinary
line. The oversized line is neither retained nor re-emitted, and is **not**
counted as a discarded line.

## Interaction with failure detection

The `producesStdout` and `producesStderr` checks in `failsWhen` (see
[failure detection and retries](Failure-Detection-and-Retries)) consider a
stream nonempty if it has **either** retained output **or** a nonzero discard
count.

Consequently, output that was produced but discarded (including all output when
`saveLimit: 0`) still triggers these failure conditions. Lines skipped because
they exceeded `maxLineLength` are not counted as discards and therefore do not,
on their own, satisfy these checks.

Because these checks operate only on captured streams, `producesStdout` has no
effect unless `captureStdout` is enabled, and `producesStderr` has no effect
unless `captureStderr` is enabled.

## Examples

Capture both streams with the default prefix and retain up to 1000 lines each:

```yaml
jobs:
  - name: report-builder
    command: ./build-report.sh
    schedule:
      minute: "0"
      hour: "6"
    captureStdout: true
    captureStderr: true
    saveLimit: 1000
```

Let stdout pass through to cronstable's stdout unmodified while capturing stderr for
failure reports:

```yaml
jobs:
  - name: importer
    command: ./import.sh
    schedule:
      minute: "*/15"
    captureStdout: false
    captureStderr: true
```

## See also

- [Reporting (Mail, Sentry, Shell, Webhook, Push, Event Log)](Reporting): how captured `stdout`/`stderr`
  appear in report templates and shell-reporter environment variables.
- [Failure Detection and Retries](Failure-Detection-and-Retries):
  `failsWhen.producesStdout` / `producesStderr`.
- [Configuration Reference](Configuration-Reference): full option list.
- [Logging Configuration](Logging-Configuration): cronstable's own logging, which
  is separate from job output capturing.
