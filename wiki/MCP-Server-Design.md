# Design Document: A Model Context Protocol (MCP) Server for cronstable

**Status:** Implemented · **Author:** Principal Engineering · **Date:** 2026-07-08 · **Target spec:** MCP `2025-11-25`

This page was the design document for cronstable's MCP server. The design
shipped as `cronstable/mcp.py` (protocol core and tool registry) and
`cronstable/mcpcli.py` (the `cronstable mcp` stdio bridge), and the
implementation has since diverged from the text in the points listed below.
Where they differ, the implementation wins, so the design body is no longer
kept on this page; the full text as written on 2026-07-08 is available in
this page's history.

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

- Hand-roll a small pure-Python MCP layer over the existing aiohttp
  apiserver rather than vendor the official `mcp` SDK, whose transitive
  tree (Rust-compiled `pydantic-core`, `cryptography`, and `rpds-py`, plus
  `starlette`/`uvicorn`/`anyio`/`httpx`) breaks cronstable's
  zero-new-dependency, multi-arch packaging story.
- Two transports, one core: a stateless Streamable HTTP `POST /mcp` route
  embedded in the existing web server (same listeners, auth, and reload
  lifecycle), plus the `cronstable mcp` stdio bridge, a urllib frame-proxy
  with no daemon imports, for local desktop clients.
- Safe by default: `readOnly: true` strips every mutating tool, the default
  toolset is `observe`, mutating tools require an explicit `confirm` (and
  `dry_run` where a preview exists), and annotations are treated as UX
  hints, never a security boundary.
- Auth reuses what cronstable already has (`web.authToken` bearer tokens,
  filesystem-gated unix sockets, mTLS) and fails closed on tokenless
  routable listeners; no OAuth for the self-hosted case.
- Build to spec revision `2025-11-25` and stay stateless (no sessions) so
  later spec revisions land small.

## Shipped divergences

Section references (§) point into the original design text, in this page's
history.

- `--validate` shipped as `--check` (§9): the stdio bridge's self-check
  flag is `cronstable mcp --check`.
- No per-run job resource template (§5.3): the proposed
  `cronstable://jobs/{name}/runs/{run_id}` shipped as
  `cronstable://jobs/{name}/runs` (the whole retained history, no
  `run_id`). The DAG template kept `{run_key}`.
- Three additional config keys (§7): the shipped `mcp:` block also takes
  `allowUnauthenticated`, `resources`, and `prompts`.
- Stricter fail-closed rule (§6/§7): there is no
  bind-safe-listeners-and-warn mode for a mixed listen set. Startup raises
  a `ConfigError` whenever any routable listener lacks a token;
  `mcp.allowUnauthenticated: true` is the explicit override.
- Offset paging, not opaque cursors (§5.1): list tools take
  `offset`/`limit` and return a `nextOffset`; only the two log-tail tools
  take a `cursor`, an integer position for polling newly appended lines.
- Resources and prompts are toolset-scoped (§5.3/§5.4): the `dags`
  resource templates and the `why_did_dag_run_fail` / `backfill_plan`
  prompts require the `dags` toolset.
- TLS is served natively, not only by a reverse proxy (§6): where the
  design text says to terminate TLS/mTLS in a reverse proxy (and cites the
  [HTTP Control API](HTTP-API) page for it), `web.listen` now accepts
  `https://` addresses served from a `web.tls` block, and
  `web.tls.clientCa` makes those listeners require a client certificate.
  That mTLS listener satisfies the fail-closed token gate on its own,
  exactly as a proxy-terminated one does; plain `https://` does not. See
  [Listener TLS](Listener-TLS).
