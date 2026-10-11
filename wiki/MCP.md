# MCP server (Model Context Protocol)

cronstable ships an optional [Model Context Protocol](https://modelcontextprotocol.io)
server, so an AI agent (Claude Desktop / Code, Cursor, VS Code Copilot,
ChatGPT connectors) can drive cronstable the way an operator drives the
[dashboard](Web-Dashboard): **observe** every job, DAG (directed acyclic
graph), the cluster/fleet, metrics, and the durable state store, and, when you
opt in, **act** (run, cancel, [pause or resume](Pausing-Jobs) a job, trigger,
backfill, or approve a DAG).

It is served two ways from the same code:

- **`POST /mcp`** on the existing [`web.listen`](HTTP-API) addresses, a
  stateless [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
  JSON-RPC endpoint that inherits the web API's `authToken` / unix-socket auth.
- **`cronstable mcp`**, a small **stdio bridge** that desktop clients launch as
  a subprocess. It forwards frames to a running daemon's `/mcp`.

It is hand-written in pure Python with **no new dependencies** (the same
minimal-dependency stance as the rest of cronstable), and exposes **tools**,
**resources**, **prompts**, and argument **completion**.

## Protocol revisions

The endpoint serves both eras of the protocol, so clients that speak either
one connect without configuration:

- **`2026-07-28`**, the stateless revision. A request whose `_meta` carries
  `io.modelcontextprotocol/protocolVersion` is served on its own:
  `server/discover` reports the supported versions, capabilities, and server
  identity, and every result carries `resultType`. The list results and
  `resources/read` carry the cache hints `ttlMs` and `cacheScope: "private"`.
  Over HTTP, the server checks that the `MCP-Protocol-Version`, `Mcp-Method`,
  and `Mcp-Name` headers match the request body, and answers a mismatch with
  error `-32020` and an unsupported version with `-32022`.
- **`2025-11-25`**, **`2025-06-18`**, and **`2025-03-26`**, the revisions a
  client negotiates with `initialize`.

Neither era keeps sessions: the server issues no `Mcp-Session-Id` and opens no
`GET` stream.

## Enabling it

The server is **off by default**. Add an [`mcp`](Configuration-Reference#mcp)
section (it uses the [`web`](HTTP-API) listeners, so a `web` section is
required):

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN   # also gates /mcp

mcp:
  enabled: true
```

That serves the **read-only** `observe` toolset. An agent can read but not
write. To let it act, opt into more toolsets and turn off `readOnly`:

```yaml
mcp:
  enabled: true
  readOnly: false        # expose mutating tools
  toolsets:
    - observe
    - dags
    - act
    - state
```

See the [`mcp` configuration reference](Configuration-Reference#mcp) for every
field.

## What the agent can do

### Tools (model-controlled actions)

Grouped into **toolsets** you enable with `toolsets:`. `readOnly: true` (the
default) strips every mutating tool regardless of toolset.

| Toolset | Tools |
| --- | --- |
| `observe` (read) | `cron_get_status`, `cron_list_jobs`, `cron_get_job`, `cron_list_runs`, `cron_get_job_trends`, `cron_get_job_resources`, `cron_get_cluster`, `cron_get_fleet`, `cron_get_node`, `cron_query_metrics`, `cron_get_version`, `cron_tail_job_logs`, `cron_schedule_pressure`, `cron_schedule_duplicates`, `cron_suggest_slot`, `cron_validate_schedule`, `cron_explain_schedule`, `cron_why_no_run`, `cron_list_pools` |
| `dags` (read) | `cron_list_dags`, `cron_list_dag_runs`, `cron_get_dag_run`, `cron_get_dag_xcom`, `cron_tail_dag_task_logs`, `cron_preview_recovery` |
| `state` (read) | `cron_inspect_state` (store overview / a namespace's documents / a stream's records; KV values and secrets redacted) |
| `act` (**mutating**) | `cron_run_job`, `cron_cancel_job`, `cron_pause_job`, `cron_resume_job`, `cron_cancel_queued` |
| `dags` (**mutating**) | `cron_trigger_dag`, `cron_backfill_dag`, `cron_recover_dag`, `cron_decide_gate` |

A tool result has two text blocks, a one-line summary followed by the result
as JSON, plus the same object in `structuredContent`. Clients that pass only
the text to the model still see the data. `cron_get_status`,
`cron_list_jobs`, and the three schedule-authoring tools declare an
`outputSchema` for their structured results. A call with an argument the tool
does not accept returns an error result that names the argument and lists the
accepted ones, so the model can correct the call.

`cron_get_status` and `cron_list_jobs` return one page per call. `limit` sets
the page size, up to [`mcp.maxRows`](Configuration-Reference#mcp), and
`offset` sets where the page starts. Without `limit`, a page holds
`mcp.maxRows` rows. The result's `page` object reports `offset`, `limit`,
`total`, `returned`, and `nextOffset`, which is `null` on the last page.
`cron_list_jobs` also takes `filter`, a case-insensitive substring of the job
name, and `state`: `running`, `disabled`, or `scheduled` (enabled and not
running). The tool applies both before it pages, so `total` counts the
matching jobs, and it builds full job rows for the returned page. On a large
job set, a small `limit` or a `filter` keeps the result small.

Mutating tools require an explicit `confirm: true` argument and re-check the
same authorization as the REST API. The tools that launch configured commands
(`cron_run_job`, `cron_trigger_dag`, `cron_backfill_dag`, and
`cron_recover_dag`) and `cron_decide_gate`, which releases the tasks waiting
on a gate, report `destructiveHint: true` and `openWorldHint: true`, so a
client that asks before risky calls asks before these. `cron_backfill_dag`
defaults to `dry_run: true`. The default dry run confirms the workflow exists
and echoes the range; the range itself is validated only when `dry_run: false`
and `confirm: true` execute it.

`cron_pause_job` takes `name` plus an optional `durationSeconds` and `note`,
and holds the job's scheduled fires for the window (one hour when
`durationSeconds` is omitted). `cron_resume_job` takes `name` and ends the
pause. Both call the daemon's own pause path. The audit field `by` records the
label of the presented token (`mcp` on a listener without tokens), and the
channel records `mcp`, so a pause taken by an agent reads as such. The
observe tools report the resulting `paused` and `sla` state on every job
payload. Semantics: [pausing jobs](Pausing-Jobs) and
[late-run detection](Late-Run-Detection).

`cron_list_pools` inspects shared capacity and waiting work.
`cron_cancel_queued` cancels a waiting entry using its `pool` and `id`.
`cron_preview_recovery` previews selected tasks or failed dates;
`cron_recover_dag` executes that preview with its `plan_token`. Cancellation
and recovery execution require `confirm: true` and a writable MCP setup.
Recovery also requires `allow_config_change: true` when the preview reports
a configuration change. A recovery that the REST routes answer with `409` or
`503`, such as `source run is busy; retry shortly` or
`recovery state is unavailable`, comes back as a tool error with the same
message. `cron_list_pools`, `cron_cancel_queued`, and `cron_run_job` for a
pooled job return the tool error `pool state is unavailable` when pool state
cannot be read or written. A refusal by a pool that answers comes back as a
tool error with the refusal's message, such as `queue entry not found` or
`pool queue is full`. On a daemon whose configuration has no `state`
section, the recovery tools return `workflow run not found` for a run and
`workflow not found` for a date range, and `cron_cancel_queued` returns
`unknown pool '<name>'`. The [HTTP API](HTTP-API#enabling-the-api) lists the
cases behind each answer. See [resource pools](Resource-Pools) and
[workflow recovery](Workflow-Recovery) for behavior and limits.

The three schedule-authoring tools make an agent a competent schedule
**author**, not only a reader, with the daemon's own engine as the authority:

- `cron_validate_schedule` parses and lints any expression before it becomes a
  job: the engine's exact error with its dialect hints,
  [lint findings](Schedule-Linting), the first upcoming fire, and prospective
  [`H` slot](Hashed-Schedules) resolution with `seed`. The dialect includes the
  [business-day forms](Business-Day-Schedules) `L-n`, `nW`, `LW` and `d#n`.
- `cron_explain_schedule` adds the next N fires in a chosen zone, so the agent
  can round-trip a plain-English description of a proposed schedule to you
  before it ships.
- `cron_why_no_run` explains field by field why a job's schedule did or did not
  fire at a timestamp (see [why a job didn't run](Why-No-Run)).

### Resources (read-only context)

Enabled by default (`resources: true`). URI-addressable snapshots that clients
can attach as context, scoped by the same toolsets:

- Fixed: `cronstable://status`, `cronstable://cluster`, `cronstable://fleet`, `cronstable://version`
- Templates: `cronstable://jobs/{name}`, `cronstable://jobs/{name}/runs`, `cronstable://dags/{name}`, `cronstable://dags/{name}/runs/{run_key}`, `cronstable://state/{ns}`

Template values are percent-encoded, as RFC 6570 expands them: a job named
`nightly backup` is `cronstable://jobs/nightly%20backup`.

Every critical read is *also* a tool, because client support for resources is
uneven. Resources are an optimization, never the only path.

### Prompts (canned triage playbooks)

Enabled by default (`prompts: true`). Slash-command workflows that chain the
read tools. A prompt is served only when every tool it calls is available to
the caller:

- `triage_job_failure(job)`: root-cause a failing job
- `why_did_dag_run_fail(dag, run_key)`: walk a failed DAG run (needs the
  `dags` toolset)
- `blast_radius(target)`: scope what else is at risk, adding the workflow and
  shared-state checks when the `dags` and `state` toolsets are on
- `fleet_health_summary()`: a wallboard-style summary
- `backfill_plan(dag, from, to)`: reason about a backfill before running it
  (needs the `dags` toolset, `readOnly: false`, and a token that can call
  `cron_backfill_dag`)

With the default `toolsets: [observe]`, only the three `observe` prompts are
served. Every prompt argument is required, and `prompts/get` without one
returns an invalid-params error.

### Argument completion

Clients that support completion suggest values while you fill in a prompt
argument or a resource template variable:

- `job` and a job template's `{name}`: job names
- `target`: job and workflow names
- `dag` and a workflow template's `{name}`: workflow names
- `run_key`: the recent runs of the workflow already chosen for `dag` or
  `{name}`

Matching is a case-insensitive prefix, and a reply lists at most 100 values
with the total number of matches.

## Wiring a client to it

### Claude Code

```shell
# remote (Streamable HTTP):
claude mcp add --transport http cronstable https://your-host/mcp \
  --header "Authorization: Bearer $CRONSTABLE_WEB_TOKEN"

# local (stdio bridge to a daemon on this host):
claude mcp add --transport stdio cronstable -- \
  cronstable mcp --url http://127.0.0.1:8080 --token-env CRONSTABLE_WEB_TOKEN
```

### Claude Desktop / Cursor / VS Code

Point the client's MCP config at the stdio bridge. For Claude Desktop
(`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "cronstable": {
      "command": "cronstable",
      "args": ["mcp", "--url", "http://127.0.0.1:8080",
               "--token-env", "CRONSTABLE_WEB_TOKEN"]
    }
  }
}
```

Cursor uses `~/.cursor/mcp.json` (`mcpServers` wrapper). VS Code uses
`.vscode/mcp.json` (`servers` wrapper, with an explicit `type`). Remote
Streamable-HTTP entries take a `url` and the `Authorization` header shown
earlier.

### The stdio bridge

`cronstable mcp` reads newline-delimited JSON-RPC on stdin and forwards each
frame to `<url>/mcp`, writing the reply to stdout (only frames go to stdout;
logs go to stderr). It needs a **reachable running daemon**, the right design
for an operations tool.

For a frame whose `_meta` names a protocol version, the bridge copies that
version, the method, and the tool, prompt, or resource name into the
`MCP-Protocol-Version`, `Mcp-Method`, and `Mcp-Name` headers, and
Base64-encodes a name that is not plain ASCII. Other frames carry the version
`initialize` negotiated. The daemon's JSON-RPC errors reach the client as the
daemon sent them, so a client can pick a version from a `-32022` error. When
the bridge cannot reach the daemon, it replies with its own error, code
`-31000`. The bridge does not follow redirects, because each request carries
the bearer token, so a redirect gets the same error, which names the
redirect's target. When the redirect leads to the `/mcp` endpoint at another
address, the error also names the `--url` value to pass. A `--url` that is
not a valid URL gets the same error for each request. The bridge exits with
status 1 before it reads a request when the token contains a line break or
another character that an HTTP header cannot carry. The bridge drops
JSON-RPC responses a client writes, because the daemon sends no requests.
Flags:

- `--url` (default `http://127.0.0.1:8080`): the daemon's web base URL
- `--token` / `--token-env`: the bearer token (defaults to the
  `CRONSTABLE_WEB_TOKEN` env var if set)
- `--cacert PATH`: verify the daemon's certificate against this CA file (a
  private CA) instead of the system trust store (defaults to
  `CRONSTABLE_WEB_CACERT` if set)
- `--client-cert PATH` / `--client-key PATH`: the client certificate and key
  for a listener that requires one through `web.tls.clientCa` (default to
  `CRONSTABLE_WEB_CLIENT_CERT` and `CRONSTABLE_WEB_CLIENT_KEY` if set)
- `--insecure`: skip TLS certificate verification, which can expose the bearer
  token to an untrusted server (equivalent to `CRONSTABLE_WEB_INSECURE=1`)
- `--protocol-version`: pin the `MCP-Protocol-Version` header of the frames
  sent before `initialize` completes (default `2025-11-25`); after
  `initialize` returns, the bridge adopts the server's negotiated version
- `--timeout` (default `30.0`): per-request deadline, in seconds, for each
  forwarded frame
- `--check`: probe the endpoint with `server/discover` (or `initialize`,
  for a daemon that does not answer it), count its tools, print the protocol
  and era, and exit

With the default configuration:

```shell
$ cronstable mcp --url http://127.0.0.1:8080 --token-env CRONSTABLE_WEB_TOKEN --check
mcp check: ok - protocol 2026-07-28 (modern; the daemon serves 2026-07-28, 2025-11-25, 2025-06-18, 2025-03-26), 19 tool(s) at http://127.0.0.1:8080/mcp
```

## Security

The MCP surface matches cronstable's hardening and is safe by default:

- **Read-only by default.** `readOnly: true` strips every mutating tool. Until
  you opt in, an agent can read but not act.

- **Inherits the web auth.** `/mcp` sits behind `web.authToken` exactly like
  the data routes. It is never public. If you enable `mcp` with **no** token
  on a routable (non-loopback, non-socket) listener, cronstable **fails
  closed**: it refuses to start (with no token there is no auth middleware at
  all, so `/mcp` would be unauthenticated). Instead:

  - Restrict `web.listen` to loopback or unix sockets.
  - Set `web.authToken`.
  - Set `web.tls.clientCa` so an `https://` listener authenticates its callers
    by certificate (see [listener TLS](Listener-TLS)).
  - Only if the endpoint is protected by other means (an mTLS (mutual TLS)
    terminating proxy), set `mcp.allowUnauthenticated: true`.

  A plain `https://` listener does not lift the gate: encryption is not caller
  authentication.

- **Per-tool scopes.** With [scoped tokens](HTTP-API#scoped-tokens-webauthtokens), `/mcp`
  accepts any token with the `view` scope, so a `view` token opens a read-only
  session and cannot reach the REST control routes. Each tool then requires
  the scope of its REST route: `control` for the mutating tools and
  `cron_preview_recovery`, `approve` for `cron_decide_gate`, and `view` for
  the rest. `tools/list` shows only the tools the presented token can call,
  and the prompts follow. Anonymous access (`web.anonymousScopes`) never
  reaches `/mcp`.

- **Origin + body defenses.** A present, non-allow-listed `Origin` is refused
  `403` (a DNS-rebinding defense; browser clients go on `mcp.allowedOrigins`).
  An oversized request body is refused `413`.

- **Human-in-the-loop for writes.** Mutating tools require `confirm: true`.
  Backfills default to a dry-run preview. Tool annotations are hints for the
  client. The real guards are the read-only default, the confirm gate, and
  server-side authorization.

- **Attribution.** Pause, resume, and gate decisions record the presented
  token's label as `by`.
  `cron_decide_gate` appends its optional `by` argument, up to 100
  characters, to the label as display text.

- **Redaction.** `cron_inspect_state` mirrors the dashboard's metadata-only
  stance: KV values become a size/type summary and secret **names** are shown
  without values.

## Trying it

The [`example/mcp`](https://github.com/ptweezy/cronstable/tree/main/example/mcp)
project boots a node with the MCP server enabled:

```shell
docker compose -f example/mcp/docker-compose.yml up
# then, in another shell, point a client (or the bridge) at it:
CRONSTABLE_WEB_TOKEN=dev-token \
  cronstable mcp --url http://127.0.0.1:8080 --check
```

## See also

- [`mcp` configuration reference](Configuration-Reference#mcp): every field.
- [HTTP Control API](HTTP-API): the REST endpoints the tools project, and the
  `POST /mcp` entry.
- [MCP Server Design](MCP-Server-Design): the design document this server was
  built from, with notes on where the implementation diverged.
- The [MCP specification](https://modelcontextprotocol.io/specification/2026-07-28)
  and the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector)
  for debugging a server.
