# Design document: a Model Context Protocol (MCP) server for cronstable

**Status:** Implemented · **Author:** Principal Engineering · **Date:** 2026-07-08 · **Target spec:** MCP `2025-11-25`

This page summarizes the MCP server's original design and how the
implementation differs. The server is in `cronstable/mcp.py`; its stdio
bridge is in `cronstable/mcpcli.py`. The full design dated 2026-07-08 is
available in this page's history.

The authoritative sources are:

- [MCP](MCP): the user-facing documentation (enabling the server, the
  tool/resource/prompt catalog, client wiring, the stdio bridge, security).
- [`mcp` configuration reference](Configuration-Reference#mcp): every
  shipped configuration field.
- `cronstable/mcp.py` and `cronstable/mcpcli.py` in the source tree: the
  implementation itself.
- [HTTP Control API](HTTP-API): the REST surface the tools project,
  including the `POST /mcp` endpoint.

## What the design decided

The document's headline decisions, all of which shipped (details that moved
are in the divergence list):

- Implement a small pure-Python MCP layer on the existing aiohttp server
  to avoid adding the official `mcp` SDK's dependencies to multi-architecture
  builds: `pydantic-core`, `cryptography`, `rpds-py`, `starlette`, `uvicorn`,
  `anyio`, and `httpx`.
- Two transports, one core: a stateless Streamable HTTP `POST /mcp` route
  embedded in the existing web server (same listeners, auth, and reload
  lifecycle), plus the `cronstable mcp` stdio bridge, a urllib frame-proxy
  with no daemon imports, for local desktop clients.
- Safe by default: `readOnly: true` strips every mutating tool. The default
  toolset is `observe`. Mutating tools require an explicit `confirm` (and
  `dry_run` where a preview exists). Annotations are treated as UX hints,
  never a security boundary.
- Authentication reuses what cronstable already has: `web.authToken` bearer
  tokens, filesystem-gated Unix sockets, and mutual TLS (mTLS). It fails
  closed on tokenless routable listeners. No OAuth for the self-hosted case.
- Target spec revision `2025-11-25` with a stateless, session-free server.

## Shipped divergences

Section references (§) point into the original design text, in this page's
history.

- `--validate` shipped as `--check` (§9): the stdio bridge's self-check
  flag is `cronstable mcp --check`.
- No per-run job resource template (§5.3): the proposed
  `cronstable://jobs/{name}/runs/{run_id}` shipped as
  `cronstable://jobs/{name}/runs` (the whole retained history, no
  `run_id`). The directed acyclic graph template kept `{run_key}`.
- Three additional configuration keys (§7): the shipped `mcp:` block also
  takes `allowUnauthenticated`, `resources`, and `prompts`.
- Stricter fail-closed rule (§6/§7): no bind-safe-listeners-and-warn mode
  exists for a mixed listen set. Startup raises a `ConfigError` whenever any
  routable listener lacks a token. `mcp.allowUnauthenticated: true` is the
  explicit override.
- Offset paging, not opaque cursors (§5.1): list tools take
  `offset`/`limit` and return a `nextOffset`. Only the two log-tail tools
  take a `cursor`, an integer position for polling newly appended lines.
- Resources and prompts are toolset-scoped (§5.3/§5.4): the `dags` resource
  templates and the `why_did_dag_run_fail` / `backfill_plan` prompts
  require the `dags` toolset.
- TLS is served by the daemon, not only by a reverse proxy (§6): where the
  design text says to terminate TLS/mTLS in a reverse proxy and cites the
  [HTTP control API](HTTP-API) page for it, `web.listen` now accepts
  `https://` addresses served from a `web.tls` block, and
  `web.tls.clientCa` makes those listeners require a client certificate.
  That mTLS listener satisfies the fail-closed token gate on its own,
  exactly as a proxy-terminated one does. Plain `https://` does not. See
  [listener TLS](Listener-TLS).
