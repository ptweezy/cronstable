"""A hand-rolled Model Context Protocol (MCP) server for cronstable.

MCP (https://modelcontextprotocol.io) lets an AI agent -- Claude Desktop /
Code, Cursor, VS Code Copilot, and other MCP clients -- drive cronstable the
way an operator drives the dashboard: list and inspect jobs, DAGs, the
cluster/fleet and the durable state store (observe), and, when the operator
opts in, run/cancel a job or trigger/backfill/approve a DAG (act).

The protocol is JSON-RPC 2.0 over the "Streamable HTTP" transport
(https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) --
a single ``POST /mcp`` endpoint.  cronstable already owns every building block
(a JSON dispatcher, aiohttp, a bearer-token middleware, the ``_json`` fast
path), so this server is a few hundred lines of pure Python with NO new
dependencies -- deliberately, rather than vendoring the official ``mcp`` SDK
and its Rust-compiled transitive tree (pydantic-core / cryptography / rpds-py),
which would break cronstable's multi-architecture, distroless packaging story.

The endpoint is served on the existing ``web.listen`` addresses and rides the
same bearer/mTLS/unix-socket auth (see :meth:`cronstable.cron.Cron.\
start_stop_web_app`).  The tools call the same in-process payload builders the
``_web_*`` REST handlers use, so there is one source of truth.  Local desktop
clients reach the server through the featherweight ``cronstable mcp`` stdio
bridge (:mod:`cronstable.mcpcli`), which forwards frames here over urllib.

The server offers tools, resources, prompts and argument completion, and
keeps no sessions (no ``Mcp-Session-Id``, no GET SSE stream). It serves both
protocol eras on one endpoint. A request whose ``_meta`` names a protocol
version, or whose MCP-Protocol-Version header names a modern revision, is
served statelessly under ``2026-07-28``: ``server/discover``, header
validation, ``resultType`` and cache hints. Every other request takes the
legacy path, where ``initialize`` negotiates ``2025-11-25`` or an earlier
revision.
"""

import asyncio
import base64
import binascii
import json as _stdlib_json
import logging
import re
from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    cast,
)
from urllib.parse import unquote

from aiohttp import web

from cronstable import _json
from cronstable import version as _version
from cronstable.cron import (
    PAUSE_BY_MAX,
    WEB_ANON_REQUEST_KEY,
    WEB_TOKEN_REQUEST_KEY,
    ApiActionError,
    _load_index_bytes,
)

if TYPE_CHECKING:  # pragma: no cover - typing only, no import cost / no cycle
    from cronstable.cron import Cron

logger = logging.getLogger("cronstable.mcp")

# Legacy revisions, negotiated by initialize. PROTOCOL_VERSION is the newest,
# offered to a client that asks for one this server cannot speak.
PROTOCOL_VERSION = "2025-11-25"
SUPPORTED_PROTOCOL_VERSIONS = frozenset(
    {"2025-11-25", "2025-06-18", "2025-03-26"}
)
# Modern revisions: stateless, with the version in every request's _meta.
MODERN_PROTOCOL_VERSIONS = frozenset({"2026-07-28"})
# Every revision served, newest first (server/discover and -32022 data).
ALL_PROTOCOL_VERSIONS = tuple(
    sorted(
        MODERN_PROTOCOL_VERSIONS | SUPPORTED_PROTOCOL_VERSIONS, reverse=True
    )
)

# JSON-RPC 2.0 error codes (protocol-level faults). Tool *execution* and
# input-validation failures do NOT use these: they return a normal result
# with isError:true so the model can read and self-correct (MCP SEP-1303).
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
# Legacy only: a resources/read for a URI that does not resolve. The modern
# path reports INVALID_PARAMS instead.
RESOURCE_NOT_FOUND = -32002
# Modern only. A client reads these as proof of a modern server and stops
# falling back to initialize, so no legacy reply may carry them.
HEADER_MISMATCH = -32020
UNSUPPORTED_PROTOCOL_VERSION = -32022

# Per-request and per-result _meta keys of the modern revisions.
META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
META_SERVER_INFO = "io.modelcontextprotocol/serverInfo"

# Modern cache hints. The lists depend on the config and the caller's token,
# and resources carry operator data, so nothing is public. Lists change only
# on a config reload; resources/read returns live data.
CACHE_SCOPE = "private"
LIST_TTL_MS = 60000
READ_TTL_MS = 0
_CACHE_TTL_MS = {
    "server/discover": LIST_TTL_MS,
    "tools/list": LIST_TTL_MS,
    "resources/list": LIST_TTL_MS,
    "resources/templates/list": LIST_TTL_MS,
    "prompts/list": LIST_TTL_MS,
    "resources/read": READ_TTL_MS,
}

# Methods that exist in only one era: 2026-07-28 removed the handshake and
# ping, and added server/discover.
_LEGACY_ONLY_METHODS = frozenset(
    {"initialize", "notifications/initialized", "ping"}
)
_MODERN_ONLY_METHODS = frozenset({"server/discover"})

# The request field each method mirrors into the Mcp-Name header.
_MCP_NAME_SOURCE = {
    "tools/call": "name",
    "prompts/get": "name",
    "resources/read": "uri",
}
# The Base64 sentinel form of a header value that is not plain ASCII.
_B64_PREFIX = "=?base64?"
_B64_SUFFIX = "?="

# completion/complete returns at most this many values.
COMPLETION_MAX = 100

SERVER_NAME = "cronstable"
SERVER_DESCRIPTION = (
    "A cron replacement with retries, alerts, saved run history, "
    "workflows, and dashboards."
)
SERVER_WEBSITE = "https://github.com/ptweezy/cronstable"

_DEFAULT_INSTRUCTIONS = (
    "cronstable's MCP server. Read-only 'observe' tools describe "
    "jobs, workflows, cluster health, metrics and saved state. "
    "Mutating tools (run/cancel/pause/resume a job, "
    "run/backfill/approve a workflow) require confirm=true and "
    "appear only when the operator disabled readOnly and the "
    "presented token allows them. Start with "
    "cron_get_status or cron_list_jobs. When authoring a schedule, "
    "verify it with cron_validate_schedule / cron_explain_schedule "
    "(the server's scheduling engine) before proposing it; "
    "cron_why_no_run "
    "explains why a job's schedule did or did not match a given "
    "timestamp."
)

RESOURCE_MIME = "application/json"

ToolHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
_MethodHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]]
# completion candidates, given the arguments already resolved
_CompletionSource = Callable[[dict[str, Any]], Awaitable[list[str]]]

# Tools whose REST twin demands a scope other than this server's default
# (`control` for a mutating tool, `view` for the rest): the gate decision
# route is promoted to `approve`, and the recovery preview is a POST. A
# token can therefore take through tools/call only what REST grants it.
_TOOL_SCOPE_OVERRIDES = {
    "cron_decide_gate": "approve",
    "cron_preview_recovery": "control",
}


class _Caller(NamedTuple):
    """Who sent the current request: a token's label and granted scopes."""

    label: str
    scopes: "frozenset[str]"


#: The current request's caller, set by handle_http from what the web auth
#: middleware matched. None when no token auth applies (auth off, an
#: mTLS-only listener, or direct handle_message use), where REST grants every
#: action too. A ContextVar: one handler serves concurrent requests.
_caller: "ContextVar[_Caller | None]" = ContextVar(
    "cronstable_mcp_caller", default=None
)


class MCPError(Exception):
    """A JSON-RPC protocol-level fault (mapped to an ``error`` response).

    ``http_status`` applies on the modern path only; a legacy error always
    rides a 200.
    """

    def __init__(
        self,
        code: int,
        message: str,
        *,
        data: Any = None,
        http_status: int = 200,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data
        self.http_status = http_status


class _ToolInputError(Exception):
    """A bad tool argument -> an ``isError`` result the model can correct."""


def _dumps(obj: Any) -> bytes:
    """Serialize a response body to JSON bytes.

    Prefers the orjson-accelerated :func:`cronstable._json.dumps_bytes`, but
    an MCP payload is a transient response (never a durable, cross-fleet
    record), so a non-finite float or other non-"portable" value should not
    500 the endpoint: fall back to the stdlib, which encodes it.
    """
    try:
        return _json.dumps_bytes(obj)
    except _json.UnsupportedValue:
        return _stdlib_json.dumps(obj, default=str).encode("utf-8")


# Prometheus exposition line: metric name, optional {labels}, then a value.
_METRIC_LINE_RE = re.compile(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+(\S+)")


def _parse_prometheus(
    text: str, match: str | None, limit: int
) -> tuple[list[dict[str, Any]], int]:
    """Reduce a Prometheus exposition to a compact, filtered sample list.

    Returns ``(samples, total_matched)``; ``samples`` is capped at ``limit``.
    HELP/TYPE comment lines are skipped and values are kept as strings so a
    ``NaN`` / ``+Inf`` gauge round-trips untouched.
    """
    needle = match.lower() if match else None
    samples: list[dict[str, Any]] = []
    total = 0
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if m is None:
            continue
        name = m.group(1)
        if needle is not None and needle not in name.lower():
            continue
        total += 1
        if len(samples) < limit:
            samples.append(
                {
                    "name": name,
                    "labels": m.group(2) or "",
                    "value": m.group(3),
                }
            )
    return samples, total


def _filter_metric_samples(
    samples: Iterator[tuple[str, str, str]],
    match: str | None,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Filter structured ``(name, label_block, value)`` samples by a
    case-insensitive name substring, capping the returned list at ``limit``
    while still counting every match.

    The model-level twin of :func:`_parse_prometheus` (same filter, same
    output shape), but fed the metric families directly via
    :meth:`prometheus...iter_samples`, so the metrics query skips rendering
    the whole exposition text only to regex it back apart.
    """
    needle = match.lower() if match else None
    out: list[dict[str, Any]] = []
    total = 0
    for name, labels, value in samples:
        if needle is not None and needle not in name.lower():
            continue
        total += 1
        if len(out) < limit:
            out.append({"name": name, "labels": labels, "value": value})
    return out, total


class MCPHandler:
    """Serves MCP over Streamable HTTP for one running :class:`Cron`.

    Built inside ``start_stop_web_app`` so it always reflects the current
    config; a config reload discards and rebuilds it.
    """

    def __init__(self, cron: "Cron", config: dict[str, Any]) -> None:
        self._cron = cron
        self._read_only: bool = config["readOnly"]
        self._toolsets = set(config["toolsets"])
        self._max_rows: int = config["maxRows"]
        self._max_body: int = config["maxBodyBytes"]
        self._allowed_origins = set(config["allowedOrigins"])
        self._resources_enabled: bool = config.get("resources", True)
        self._prompts_enabled: bool = config.get("prompts", True)
        self._instructions: str = (
            config.get("instructions") or _DEFAULT_INSTRUCTIONS
        )
        # the lean identity every modern result carries, and the full one
        # initialize and server/discover return
        self._identity = {
            "name": SERVER_NAME,
            "title": SERVER_NAME,
            "version": _version.version,
        }
        self._server_info: dict[str, Any] = {
            **self._identity,
            "description": SERVER_DESCRIPTION,
            "websiteUrl": SERVER_WEBSITE,
        }
        icons = _server_icons()
        if icons:
            self._server_info["icons"] = [dict(icon) for icon in icons]
        self._methods: dict[str, _MethodHandler] = {
            "initialize": self._m_initialize,
            "notifications/initialized": self._m_noop,
            "notifications/cancelled": self._m_noop,
            "ping": self._m_ping,
            "server/discover": self._m_discover,
            "tools/list": self._m_tools_list,
            "tools/call": self._m_tools_call,
        }
        self._tools = self._build_registry()
        self._tool_by_name = {t["name"]: t for t in self._tools}
        self._resources, self._templates = self._build_resources()
        self._resource_by_uri = {r["uri"]: r for r in self._resources}
        self._template_by_uri = {t["uriTemplate"]: t for t in self._templates}
        self._prompts = self._build_prompts()
        self._prompt_by_name = {p["name"]: p for p in self._prompts}
        if self._resources_enabled:
            self._methods["resources/list"] = self._m_resources_list
            self._methods["resources/templates/list"] = (
                self._m_resource_templates_list
            )
            self._methods["resources/read"] = self._m_resources_read
        if self._prompts_enabled:
            self._methods["prompts/list"] = self._m_prompts_list
            self._methods["prompts/get"] = self._m_prompts_get
        if self._resources_enabled or self._prompts_enabled:
            self._methods["completion/complete"] = self._m_complete

    # -- capabilities / visibility ----------------------------------------

    def _capabilities(self) -> dict[str, Any]:
        """Advertise ONLY what the current caller can use.

        A server MUST NOT advertise a capability it does not implement (a
        conformant client would then call the method and get -32601). Tools
        are always present; resources/prompts appear only when enabled AND
        something is visible, and completions whenever either appears.
        """
        caps: dict[str, Any] = {"tools": {"listChanged": False}}
        if self._resources_enabled and (
            any(self._resource_visible(r) for r in self._resources)
            or any(self._resource_visible(t) for t in self._templates)
        ):
            caps["resources"] = {"listChanged": False}
        if self._prompts_enabled and any(
            self._prompt_visible(p) for p in self._prompts
        ):
            caps["prompts"] = {"listChanged": False}
        if "resources" in caps or "prompts" in caps:
            caps["completions"] = {}
        return caps

    def _resource_visible(self, entry: dict[str, Any]) -> bool:
        return entry["toolset"] in self._toolsets

    def _prompt_visible(self, entry: dict[str, Any]) -> bool:
        """A prompt is served only when every tool it requires is."""
        return all(self._tool_visible(name) for name in entry["requires"])

    def _configured(self, tool: dict[str, Any]) -> bool:
        """Whether the config serves ``tool``.

        Its toolset must be enabled, and a mutating tool is stripped entirely
        while ``readOnly`` is on (readOnly wins over toolsets, GitHub-style).
        """
        if tool["toolset"] not in self._toolsets:
            return False
        if tool["mutating"] and self._read_only:
            return False
        return True

    @staticmethod
    def _permitted(tool: dict[str, Any]) -> bool:
        """Whether the current caller's token grants ``tool``'s scope."""
        caller = _caller.get()
        return caller is None or tool["scope"] in caller.scopes

    def _tool_visible(self, name: str) -> bool:
        tool = self._tool_by_name.get(name)
        return (
            tool is not None
            and self._configured(tool)
            and self._permitted(tool)
        )

    # -- JSON-RPC method handlers -----------------------------------------

    async def _m_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        requested = params.get("protocolVersion")
        # echo the client's version when we can speak it, else offer ours.
        negotiated = (
            requested
            if isinstance(requested, str)
            and requested in SUPPORTED_PROTOCOL_VERSIONS
            else PROTOCOL_VERSION
        )
        return {
            "protocolVersion": negotiated,
            "capabilities": self._capabilities(),
            "serverInfo": self._server_info,
            "instructions": self._instructions,
        }

    async def _m_discover(self, params: dict[str, Any]) -> dict[str, Any]:
        return {
            "supportedVersions": list(ALL_PROTOCOL_VERSIONS),
            "capabilities": self._capabilities(),
            "instructions": self._instructions,
            "_meta": {META_SERVER_INFO: self._server_info},
        }

    async def _m_noop(self, params: dict[str, Any]) -> dict[str, Any] | None:
        return None

    async def _m_ping(self, params: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def _m_tools_list(self, params: dict[str, Any]) -> dict[str, Any]:
        tools = [
            t["listing"]
            for t in self._tools
            if self._configured(t) and self._permitted(t)
        ]
        return {"tools": tools}

    async def _m_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str):
            raise MCPError(INVALID_PARAMS, "tools/call requires a 'name'")
        tool = self._tool_by_name.get(name)
        if tool is None or not self._configured(tool):
            raise MCPError(INVALID_PARAMS, "unknown tool: {}".format(name))
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise MCPError(INVALID_PARAMS, "'arguments' must be an object")
        if not self._permitted(tool):
            return _tool_error(
                "the presented web token lacks the {!r} scope this tool "
                "requires (the REST route for the same action is gated "
                "identically)".format(tool["scope"])
            )
        allowed = tool["inputSchema"]["properties"]
        unknown = [key for key in arguments if key not in allowed]
        if unknown:
            return _tool_error(
                "unknown argument(s) {}; {} accepts {}".format(
                    ", ".join(map(repr, unknown)),
                    name,
                    ", ".join(allowed) or "no arguments",
                )
            )
        handler = cast(ToolHandler, tool["handler"])
        try:
            return await handler(arguments)
        except _ToolInputError as ex:
            return _tool_error(str(ex))
        except ApiActionError as ex:
            # a client-facing action failure (unknown/disabled/not-running
            # job, bad dag): an isError result the model can act on, not a
            # transport fault.
            return _tool_error(ex.message)

    # -- top-level dispatch (transport-independent, unit-testable) --------

    async def handle_message(self, msg: Any) -> dict[str, Any] | None:
        """Dispatch one JSON-RPC message.

        Returns the response object for a request, or ``None`` for a
        notification or a stray response (the caller then emits a 202).
        The single seam tests drive directly.
        """
        return (await self._dispatch(msg))[0]

    async def _dispatch(self, msg: Any) -> tuple[dict[str, Any] | None, int]:
        """:meth:`handle_message`, plus the HTTP status for its reply.

        The status differs from 200 only on the modern path, where a missing
        ``_meta`` field or an unsupported version is 400 and an unknown
        method is 404.
        """
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            return (
                _error_envelope(
                    _id_of(msg),
                    INVALID_REQUEST,
                    "invalid JSON-RPC 2.0 message",
                ),
                200,
            )
        if _is_response(msg):
            # this server sends no requests, and nothing answers a reply.
            return None, 202
        is_notification = "id" not in msg
        msg_id = msg.get("id")
        if not is_notification and not _valid_id(msg_id):
            return (
                _error_envelope(
                    None,
                    INVALID_REQUEST,
                    "request id must be a string or an integer",
                ),
                200,
            )
        method = msg.get("method")
        modern = _declares_modern(msg)
        try:
            if not isinstance(method, str):
                raise MCPError(INVALID_REQUEST, "missing method")
            params = msg.get("params") or {}
            if not isinstance(params, dict):
                raise MCPError(INVALID_PARAMS, "invalid params")
            if modern:
                problem = _meta_error(msg) or _version_error(msg)
                if problem is not None:
                    raise problem
            excluded = _LEGACY_ONLY_METHODS if modern else _MODERN_ONLY_METHODS
            handler = None if method in excluded else self._methods.get(method)
            if handler is None:
                raise MCPError(
                    METHOD_NOT_FOUND,
                    "unknown method: {}".format(method),
                    http_status=404,
                )
            result = await handler(params)
        except MCPError as ex:
            if is_notification:
                return None, 202
            code = ex.code
            if modern and code == RESOURCE_NOT_FOUND:
                code = INVALID_PARAMS
            envelope = _error_envelope(msg_id, code, ex.message, ex.data)
            return envelope, ex.http_status if modern else 200
        except Exception:  # noqa: BLE001 - never leak a traceback to a client
            logger.exception("mcp: internal error handling %s", method)
            if is_notification:
                return None, 202
            envelope = _error_envelope(
                msg_id, INTERNAL_ERROR, "internal error"
            )
            return envelope, 200
        if is_notification:
            return None, 202
        if modern:
            result = self._modern_result(cast(str, method), result or {})
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}, 200

    def _modern_result(
        self, method: str, result: dict[str, Any]
    ) -> dict[str, Any]:
        """Add the fields every modern result carries."""
        out = dict(result)
        out["resultType"] = "complete"
        meta = dict(out.get("_meta") or {})
        meta.setdefault(META_SERVER_INFO, self._identity)
        out["_meta"] = meta
        ttl = _CACHE_TTL_MS.get(method)
        if ttl is not None:
            out["ttlMs"] = ttl
            out["cacheScope"] = CACHE_SCOPE
        return out

    # -- HTTP (Streamable HTTP, stateless profile) ------------------------

    async def handle_http(self, request: web.Request) -> web.StreamResponse:
        headers = request.headers
        origin = headers.get("Origin")
        pv = headers.get("MCP-Protocol-Version")
        version = _echo_version(pv)
        # DNS-rebinding defense: a present Origin must be allow-listed. With
        # an empty allowedOrigins (non-browser clients only) any Origin is
        # refused: a real MCP client over stdio/CLI sends none.
        if origin is not None and origin not in self._allowed_origins:
            return self._http_error(403, "Origin not allowed", origin, version)
        accept = headers.get("Accept")
        # stateless mode only ever emits application/json; be lenient on a
        # missing Accept, but honor a present, incompatible one.
        if accept and "application/json" not in accept and "*/*" not in accept:
            return self._http_error(
                406, "Accept application/json", origin, version
            )
        if (
            request.content_length is not None
            and request.content_length > self._max_body
        ):
            return self._http_error(
                413, "request body too large", origin, version
            )
        # File the caller where tools/call can consult it (per-request task
        # context, so concurrent requests cannot bleed).
        _caller.set(_caller_of(request))
        raw = await request.read()
        if len(raw) > self._max_body:
            return self._http_error(
                413, "request body too large", origin, version
            )
        if not raw:
            return self._http_error(400, "empty request body", origin, version)
        try:
            msg = _json.loads(raw)
        except Exception:  # noqa: BLE001 - malformed JSON -> 400
            return self._http_error(400, "malformed JSON", origin, version)
        if isinstance(msg, list):
            # JSON-RPC batching was removed in MCP 2025-06-18.
            return self._http_error(
                400, "batching unsupported", origin, version
            )
        if isinstance(msg, dict):
            if _is_response(msg):
                return self._http_error(
                    400,
                    "JSON-RPC responses are not accepted: this server "
                    "sends no requests",
                    origin,
                    version,
                )
        modern = isinstance(msg, dict) and (
            pv in MODERN_PROTOCOL_VERSIONS or _declares_modern(msg)
        )
        if modern and "id" in msg:
            # notifications have no header rules in 2026-07-28
            problem = _meta_error(msg) or _header_error(msg, headers)
            if problem is not None:
                envelope = _error_envelope(
                    _id_of(msg), problem.code, problem.message
                )
                return self._json_response(
                    envelope, origin=origin, status=400, version=version
                )
        elif not modern and (
            pv is not None and pv not in SUPPORTED_PROTOCOL_VERSIONS
        ):
            return self._http_error(
                400, "unsupported MCP-Protocol-Version", origin, version
            )
        response, status = await self._dispatch(msg)
        if response is None:
            # a notification carries no reply.
            return self._plain(202, origin, version)
        if isinstance(msg, dict) and msg.get("method") == "initialize":
            # the reply names the revision the rest of the session uses.
            version = (response.get("result") or {}).get(
                "protocolVersion", version
            )
        return self._json_response(
            response, origin=origin, status=status, version=version
        )

    async def handle_http_get(
        self, request: web.Request
    ) -> web.StreamResponse:
        # stateless: no server->client SSE stream to open.
        origin = request.headers.get("Origin")
        version = _echo_version(request.headers.get("MCP-Protocol-Version"))
        resp = self._http_error(405, "method not allowed", origin, version)
        resp.headers["Allow"] = "POST, OPTIONS"
        return resp

    async def handle_options(self, request: web.Request) -> web.StreamResponse:
        origin = request.headers.get("Origin")
        if origin and origin in self._allowed_origins:
            headers = self._cors_headers(origin)
            headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
            return web.Response(status=204, headers=headers)
        return self._http_error(403 if origin else 405, "preflight", origin)

    # -- HTTP response helpers --------------------------------------------

    def _cors_headers(self, origin: str | None) -> dict[str, str]:
        if origin and origin in self._allowed_origins:
            # credentialed CORS may not use a wildcard; echo the exact origin.
            return {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Headers": (
                    "Authorization, Content-Type, MCP-Protocol-Version, "
                    "Mcp-Method, Mcp-Name"
                ),
                "Access-Control-Expose-Headers": "MCP-Protocol-Version",
                "Vary": "Origin",
            }
        return {}

    def _json_response(
        self,
        obj: dict[str, Any],
        *,
        origin: str | None,
        status: int = 200,
        version: str = PROTOCOL_VERSION,
    ) -> web.Response:
        headers = {"MCP-Protocol-Version": version}
        headers.update(self._cors_headers(origin))
        return web.Response(
            body=_dumps(obj),
            status=status,
            content_type="application/json",
            charset="utf-8",
            headers=headers,
        )

    def _plain(
        self, status: int, origin: str | None, version: str
    ) -> web.Response:
        headers = {"MCP-Protocol-Version": version}
        headers.update(self._cors_headers(origin))
        return web.Response(status=status, headers=headers)

    def _http_error(
        self,
        status: int,
        message: str,
        origin: str | None,
        version: str = PROTOCOL_VERSION,
    ) -> web.Response:
        return self._json_response(
            {"error": message}, origin=origin, status=status, version=version
        )

    # -- pagination / argument helpers ------------------------------------

    def _clamp_limit(self, requested: Any) -> int:
        n = _opt_int(requested)
        if n is None:
            return self._max_rows
        return max(1, min(n, self._max_rows))

    def _page(
        self, items: list[Any], offset: Any, limit: Any
    ) -> tuple[list[Any], dict[str, Any]]:
        total = len(items)
        off = max(0, _opt_int(offset) or 0)
        lim = self._clamp_limit(limit)
        page = items[off : off + lim]
        nxt = off + len(page)
        return page, {
            "offset": off,
            "limit": lim,
            "total": total,
            "returned": len(page),
            "nextOffset": nxt if nxt < total else None,
        }

    # -- tool registry -----------------------------------------------------

    def _build_registry(self) -> list[dict[str, Any]]:
        obj = _obj_schema
        # every entry is a _tool(...) spec; see that factory for the defaults
        return [
            # ---- observe (read-only) ----
            _tool(
                "observe",
                "cron_get_status",
                "Job status",
                "Show each job's status: running, disabled, or scheduled.",
                obj({"offset": _INT, "limit": _INT}),
                self._t_get_status,
                output=_STATUS_OUTPUT,
            ),
            _tool(
                "observe",
                "cron_list_jobs",
                "List jobs",
                "List jobs with schedule, enabled/running state, next run, "
                "and last outcome. Optional name substring `filter` "
                "and `state` "
                "(running/disabled/scheduled).",
                obj(
                    {
                        "filter": _STR,
                        "state": _enum(["running", "disabled", "scheduled"]),
                        "offset": _INT,
                        "limit": _INT,
                    }
                ),
                self._t_list_jobs,
                output=_JOBS_OUTPUT,
            ),
            _tool(
                "observe",
                "cron_get_job",
                "Get one job",
                "Show details for one job (schedule, command, last run, live "
                "resources, retry/slot state).",
                obj({"name": _STR}, ["name"]),
                self._t_get_job,
            ),
            _tool(
                "observe",
                "cron_list_runs",
                "Run history",
                "Show saved run history, success rate, and duration "
                "statistics for one job "
                "(most recent `limit` runs).",
                obj({"name": _STR, "limit": _INT}, ["name"]),
                self._t_list_runs,
            ),
            _tool(
                "observe",
                "cron_get_job_trends",
                "SLA trends",
                "Per-window (1h/24h/7d/30d/all) success-rate and duration "
                "statistics from the saved run history for one job.",
                obj({"name": _STR}, ["name"]),
                self._t_get_job_trends,
            ),
            _tool(
                "observe",
                "cron_get_job_resources",
                "Resource usage",
                "CPU/RSS time series for a job's live and recent runs "
                "(monitorResources jobs).",
                obj({"name": _STR, "runs": _INT}, ["name"]),
                self._t_get_job_resources,
            ),
            _tool(
                "observe",
                "cron_get_cluster",
                "Cluster view",
                "This node's cluster/leadership view (peers, quorum, role, "
                "live load). Returns enabled:false if no cluster is "
                "configured.",
                obj({}),
                self._t_get_cluster,
            ),
            _tool(
                "observe",
                "cron_get_fleet",
                "Fleet view",
                "Run status for each job on each node. Returns enabled:false "
                "when cluster peer communication is unavailable.",
                obj({}),
                self._t_get_fleet,
            ),
            _tool(
                "observe",
                "cron_get_node",
                "Node resources",
                "This node's live whole-host CPU/memory (optionally with the "
                "recent history).",
                obj({"history": _BOOL}),
                self._t_get_node,
            ),
            _tool(
                "observe",
                "cron_query_metrics",
                "Query metrics",
                "Parsed samples from the Prometheus /metrics exposition, "
                "optionally filtered by a metric-name substring `match`.",
                obj({"match": _STR, "limit": _INT}),
                self._t_query_metrics,
            ),
            _tool(
                "observe",
                "cron_get_version",
                "Version",
                "Show the server version, job-set ID, and job count.",
                obj({}),
                self._t_get_version,
            ),
            _tool(
                "observe",
                "cron_tail_job_logs",
                "Tail job logs",
                "Last retained stdout/stderr lines of a job, with a `cursor` "
                "to poll for newly appended lines (the poll form of the live "
                "log stream).",
                obj({"name": _STR, "tail": _INT, "cursor": _INT}, ["name"]),
                self._t_tail_job_logs,
            ),
            _tool(
                "observe",
                "cron_schedule_pressure",
                "Schedule load",
                "Upcoming runs for enabled schedules over the next `hours` "
                "(default 24, max 168), grouped by hour and minute in `tz` "
                "(default UTC). Shows when jobs are scheduled together "
                "and which minute positions have no scheduled runs.",
                obj({"hours": _INT, "tz": _STR}),
                self._t_schedule_pressure,
            ),
            _tool(
                "observe",
                "cron_schedule_duplicates",
                "Duplicate schedules",
                "Jobs with identical run times (for example, */5 and 0-59/5 "
                "in the same time zone). Use this to find schedules "
                "that could be spread out.",
                obj({}),
                self._t_schedule_duplicates,
            ),
            _tool(
                "observe",
                "cron_suggest_slot",
                "Suggest a quieter run time",
                "Suggest a time with fewer scheduled runs in the next "
                "24 hours. "
                "For `period`, 'hourly' picks a minute, "
                "'daily' a minute and hour; returns the cron expression, two "
                "alternatives, and the busiest time.",
                obj(
                    {
                        "period": _enum(["hourly", "daily"]),
                        "tz": _STR,
                    }
                ),
                self._t_suggest_slot,
            ),
            _tool(
                "observe",
                "cron_validate_schedule",
                "Validate a schedule",
                "Check a cron expression before saving it as a job: "
                "valid true/false with the engine's exact error (including "
                "wrong-field hints for Quartz-style forms), the "
                "plain-English description, the normalized form, advisory "
                "warnings, and the first scheduled run, all from the "
                "server's scheduling engine. The dialect includes L "
                "(last day), L-n (n days before it), nW / LW (nearest / "
                "last weekday) and Ln / d#n (last / nth weekday: L5 = last "
                "Friday, 5#3 = third Friday). `tz` (IANA zone the job will "
                "run in) enables the DST checks; `seed` (the prospective "
                "job name) resolves Jenkins-style H slots.",
                obj(
                    {"expression": _STR, "tz": _STR, "seed": _STR},
                    ["expression"],
                ),
                self._t_validate_schedule,
                output=_PREVIEW_OUTPUT,
            ),
            _tool(
                "observe",
                "cron_explain_schedule",
                "Explain a schedule",
                "Decode a cron expression into a plain-English description, "
                "its next `count` run times (default 5, max 60) as ISO "
                "timestamps "
                "in `tz` (default UTC; pass the job's zone), and advisory "
                "warnings from the server's scheduling engine. "
                "`seed` (a job name) resolves "
                "Jenkins-style H slots.",
                obj(
                    {
                        "expression": _STR,
                        "count": _INT,
                        "tz": _STR,
                        "seed": _STR,
                    },
                    ["expression"],
                ),
                self._t_explain_schedule,
                output=_PREVIEW_OUTPUT,
            ),
            _tool(
                "observe",
                "cron_why_no_run",
                "Why no run?",
                "Explain field-by-field why a job's schedule did or did not "
                "select a timestamp ('day-of-week Tuesday is not in Monday "
                "and Friday'), with the previous and next scheduled runs and "
                "notes on this dialect's day-field AND rule and DST "
                "effects. `at` is ISO 8601; timestamps without an offset use "
                "the job's time zone. If the schedule does "
                "match, the answer refers to execution history "
                "(cron_list_runs) instead.",
                obj({"name": _STR, "at": _STR}, ["name", "at"]),
                self._t_why_no_run,
                output=_WHY_OUTPUT,
            ),
            _tool(
                "observe",
                "cron_list_pools",
                "List resource pools",
                "Show pool capacity, queued work, and recent queue outcomes.",
                obj({}),
                self._t_list_pools,
            ),
            # ---- dags ----
            _tool(
                "dags",
                "cron_list_dags",
                "List workflows",
                "Configured workflows (DAGs) with their tasks and "
                "dependencies.",
                obj({}),
                self._t_list_dags,
            ),
            _tool(
                "dags",
                "cron_list_dag_runs",
                "List workflow runs",
                "Recent workflow runs with task counts for each status.",
                obj({"dag": _STR, "limit": _INT}, ["dag"]),
                self._t_list_dag_runs,
            ),
            _tool(
                "dags",
                "cron_get_dag_run",
                "Get workflow run",
                "Workflow run details: task status, timing, and decisions.",
                obj({"dag": _STR, "run_key": _STR}, ["dag", "run_key"]),
                self._t_get_dag_run,
            ),
            _tool(
                "dags",
                "cron_get_dag_xcom",
                "Workflow task outputs (XCom)",
                "Output values (XCom) shared by tasks in a workflow run.",
                obj({"dag": _STR, "run_key": _STR}, ["dag", "run_key"]),
                self._t_get_dag_xcom,
            ),
            _tool(
                "dags",
                "cron_tail_dag_task_logs",
                "Read workflow task logs",
                "Latest saved log lines for a running workflow task "
                "instance, with a `cursor` to poll for more.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "taskkey": _STR,
                        "tail": _INT,
                        "cursor": _INT,
                    },
                    ["dag", "run_key", "taskkey"],
                ),
                self._t_tail_dag_task_logs,
            ),
            _tool(
                "dags",
                "cron_preview_recovery",
                "Preview workflow recovery",
                "Preview failed tasks or a selected task and its downstream "
                "tasks. Supply run_key, or from and to for failed dates.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "from": _STR,
                        "to": _STR,
                        "mode": _STR,
                        "tasks": {"type": "array", "items": _STR},
                    },
                    ["dag"],
                ),
                self._t_preview_recovery,
            ),
            # ---- state (read-only inspector) ----
            _tool(
                "state",
                "cron_inspect_state",
                "Inspect state store",
                "Inspect saved state without revealing values: overview "
                "(default), one namespace's documents (`ns` "
                "kv/|cursor/|idem/) or a stream's newest records (`stream`). "
                "KV values and "
                "secrets are redacted.",
                obj({"ns": _STR, "stream": _STR, "limit": _INT}),
                self._t_inspect_state,
            ),
            # ---- act (mutating job control; readOnly:false to expose) ----
            _tool(
                "act",
                "cron_cancel_queued",
                "Cancel queued work",
                "Cancel a waiting pool entry. Requires confirm=true.",
                obj(
                    {"pool": _STR, "id": _STR, "confirm": _BOOL},
                    ["pool", "id"],
                ),
                self._t_cancel_queued,
                mutating=True,
                destructive=True,
                idempotent=True,
            ),
            _tool(
                "act",
                "cron_run_job",
                "Run job now",
                "Launch a job immediately, following its concurrencyPolicy. "
                "Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_run_job,
                mutating=True,
                destructive=True,
                open_world=True,
            ),
            _tool(
                "act",
                "cron_cancel_job",
                "Cancel job",
                "Terminate a job's running instances (graceful, then kill). "
                "Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_cancel_job,
                mutating=True,
                destructive=True,
                idempotent=True,
            ),
            _tool(
                "act",
                "cron_pause_job",
                "Pause job",
                "Pause scheduled runs until the pause expires "
                "(durationSeconds, default 3600) or the job is resumed; "
                "manual runs stay allowed. Requires confirm=true.",
                obj(
                    {
                        "name": _STR,
                        "durationSeconds": _INT,
                        "note": _STR,
                        "confirm": _BOOL,
                    },
                    ["name"],
                ),
                self._t_pause_job,
                mutating=True,
                idempotent=True,
            ),
            _tool(
                "act",
                "cron_resume_job",
                "Resume job",
                "Resume scheduled runs; has no effect when "
                "the job is not paused. Requires confirm=true.",
                obj({"name": _STR, "confirm": _BOOL}, ["name"]),
                self._t_resume_job,
                mutating=True,
                idempotent=True,
            ),
            # ---- dag control (mutating; toolset dags + readOnly:false) ----
            _tool(
                "dags",
                "cron_trigger_dag",
                "Run workflow now",
                "Start a workflow run now. Requires confirm=true.",
                obj({"dag": _STR, "confirm": _BOOL}, ["dag"]),
                self._t_trigger_dag,
                mutating=True,
                destructive=True,
                open_world=True,
            ),
            _tool(
                "dags",
                "cron_backfill_dag",
                "Run workflow for past dates",
                "Run a scheduled workflow for an ISO date range. dry_run "
                "(default true) previews the runs. To start them, set "
                "dry_run=false and confirm=true.",
                obj(
                    {
                        "dag": _STR,
                        "from": _STR,
                        "to": _STR,
                        "dry_run": _BOOL,
                        "confirm": _BOOL,
                    },
                    ["dag", "from", "to"],
                ),
                self._t_backfill_dag,
                mutating=True,
                destructive=True,
                open_world=True,
            ),
            _tool(
                "dags",
                "cron_recover_dag",
                "Recover workflow tasks",
                "Execute a reviewed recovery plan. Requires plan_token and "
                "confirm=true. Creates a new run preserving successful work.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "from": _STR,
                        "to": _STR,
                        "mode": _STR,
                        "tasks": {"type": "array", "items": _STR},
                        "plan_token": _STR,
                        "allow_config_change": _BOOL,
                        "confirm": _BOOL,
                    },
                    ["dag", "plan_token"],
                ),
                self._t_recover_dag,
                mutating=True,
                destructive=True,
                idempotent=True,
                open_world=True,
            ),
            _tool(
                "dags",
                "cron_decide_gate",
                "Decide approval gate",
                "Approve or reject a workflow approval step. "
                "Requires confirm=true; with scoped web tokens, the "
                "presented token must hold the approve scope.",
                obj(
                    {
                        "dag": _STR,
                        "run_key": _STR,
                        "taskkey": _STR,
                        "decision": _enum(["approve", "reject"]),
                        "by": _STR,
                        "confirm": _BOOL,
                    },
                    ["dag", "run_key", "taskkey", "decision"],
                ),
                self._t_decide_gate,
                mutating=True,
                # approving releases the downstream tasks
                destructive=True,
                open_world=True,
            ),
        ]

    # -- observe tool handlers --------------------------------------------

    async def _t_get_status(self, args: dict[str, Any]) -> dict[str, Any]:
        rows = self._cron.status_payload()
        page, meta = self._page(rows, args.get("offset"), args.get("limit"))
        running = sum(1 for r in page if r.get("status") == "running")
        summary = "{} job(s); {} running in this page".format(
            meta["total"], running
        )
        return _result({"status": page, "page": meta}, summary)

    async def _t_list_jobs(self, args: dict[str, Any]) -> dict[str, Any]:
        rows = self._cron.jobs_payload()
        flt = args.get("filter")
        if isinstance(flt, str) and flt:
            low = flt.lower()
            rows = [r for r in rows if low in r["name"].lower()]
        state = args.get("state")
        if state == "running":
            rows = [r for r in rows if r.get("running")]
        elif state == "disabled":
            rows = [r for r in rows if not r.get("enabled")]
        elif state == "scheduled":
            rows = [
                r for r in rows if r.get("enabled") and not r.get("running")
            ]
        page, meta = self._page(rows, args.get("offset"), args.get("limit"))
        return _result(
            {"jobs": page, "page": meta},
            "{} matching job(s); {} returned".format(
                meta["total"], meta["returned"]
            ),
        )

    async def _t_get_job(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        payload = self._cron.job_detail_payload(name)
        if payload is None:
            return _tool_error(
                "job not found: {!r}. Use cron_list_jobs to find "
                "available jobs.".format(name)
            )
        return _result(payload, "job {!r}".format(name))

    async def _t_list_runs(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        payload = self._cron.job_runs_payload(name)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        limit = self._clamp_limit(args.get("limit"))
        all_runs = payload["runs"]
        payload["runs"] = all_runs[-limit:]
        payload["totalRuns"] = len(all_runs)
        payload["returnedRuns"] = len(payload["runs"])
        return _result(
            payload,
            "job {!r}: {} run(s) retained, {} returned".format(
                name, len(all_runs), len(payload["runs"])
            ),
        )

    async def _t_get_job_trends(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        payload = await self._cron.job_trends_payload(name)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(payload, "trends for job {!r}".format(name))

    async def _t_get_job_resources(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        name = _req_str(args, "name")
        max_runs = self._clamp_limit(args.get("runs"))
        payload = self._cron.job_resources_payload(name, max_runs)
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(payload, "resource series for job {!r}".format(name))

    async def _t_get_cluster(self, args: dict[str, Any]) -> dict[str, Any]:
        payload = self._cron.cluster_payload()
        return _result(
            payload,
            "cluster enabled={}".format(payload.get("enabled")),
        )

    async def _t_get_fleet(self, args: dict[str, Any]) -> dict[str, Any]:
        payload = self._cron.fleet_payload()
        return _result(
            payload, "fleet enabled={}".format(payload.get("enabled"))
        )

    async def _t_get_node(self, args: dict[str, Any]) -> dict[str, Any]:
        payload = self._cron.node_payload(history=bool(args.get("history")))
        return _result(payload, "node {}".format(payload.get("node_name")))

    async def _t_query_metrics(self, args: dict[str, Any]) -> dict[str, Any]:
        match = args.get("match")
        if match is not None and not isinstance(match, str):
            raise _ToolInputError("`match` must be a string")
        limit = self._clamp_limit(args.get("limit"))
        cron = self._cron
        # Split in two phases exactly the way GET /metrics splits a scrape
        # (see Cron._web_metrics and Metrics.families), and offload at the
        # same job count, for the same reason.  A metrics query is asked for
        # a handful of samples but has to visit the WHOLE metric universe to
        # know which ones match, and the visit is the expensive half: it
        # assembles a label block and formats a value for every sample of
        # every family.  Doing that inline stalled job dispatch and every
        # other handler for the duration, and at fleet scale that is a
        # measurable freeze on a tool an agent may poll.
        #
        # The universe walk (the expensive half: it assembles a label
        # block and formats a value per sample) is shared across the
        # callers that arrive within a second and offloaded past the same
        # job-count gate GET /metrics uses, so an agent polling this tool
        # neither stalls dispatch nor rebuilds what a concurrent caller is
        # already building. The filter stays per call: it is substring
        # matching over prebuilt strings, far cheaper than the walk.
        samples = await cron.metric_samples_snapshot()
        rows, total = _filter_metric_samples(iter(samples), match, limit)
        return _result(
            {
                "samples": rows,
                "totalMatched": total,
                "returned": len(rows),
                "match": match,
            },
            "{} metric sample(s) matched, {} returned".format(
                total, len(rows)
            ),
        )

    async def _t_schedule_pressure(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        # no clamp here: croninfo.schedule_pressure clamps hours to
        # [1, 168] authoritatively and echoes the clamped value back in
        # the payload; only the default is applied at this layer.
        hours = _opt_int(args.get("hours"))
        hours = 24 if hours is None else hours
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        try:
            # offloaded to the default executor (see the _async wrapper in
            # cron.py): the up-to-168h occurrence walk is pure CPU and must
            # not stall job dispatch on the scheduler's event loop.
            payload = await self._cron.schedule_pressure_payload_async(
                hours, tz or None
            )
        except ValueError as err:
            return _tool_error(str(err))
        busiest = payload["busiest_minute"]
        return _result(
            payload,
            "{} scheduled runs from {} jobs in the next {}h; "
            "busiest minute :{:02d} ({} jobs); "
            "{} of 60 minute positions have no scheduled runs".format(
                payload["total_fires"],
                payload["jobs"],
                payload["hours"],
                busiest["minute"],
                busiest["jobs"],
                len(payload["empty_minutes"]),
            ),
        )

    async def _t_schedule_duplicates(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        # offloaded (see cron.py): the fleet walk must not block the loop.
        payload = await self._cron.schedule_duplicates_payload_async()
        groups = payload["groups"]
        biggest = (
            "; biggest: {} job(s) sharing '{}'".format(
                groups[0]["count"], groups[0]["expression"]
            )
            if groups
            else ""
        )
        return _result(
            payload,
            "{} duplicate group(s) across {} scheduled job(s){}".format(
                len(groups), payload["jobs"], biggest
            ),
        )

    async def _t_suggest_slot(self, args: dict[str, Any]) -> dict[str, Any]:
        period = args.get("period") or "hourly"
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        try:
            # offloaded (see cron.py): the 24h fleet walk must not block
            # the loop; the bad-period/timezone ValueError still surfaces
            # at the await.
            payload = await self._cron.schedule_suggest_payload_async(
                period, tz or None
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(
            payload,
            "suggested {} schedule: '{}' ({} runs already scheduled at "
            "this time "
            "in 24h)".format(
                payload["period"],
                payload["expression"],
                payload["fires_in_window"],
            ),
        )

    @staticmethod
    def _preview_args(
        args: dict[str, Any],
    ) -> tuple[str | None, str | None]:
        """The shared `tz`/`seed` arguments of the schedule sandboxes."""
        tz = args.get("tz")
        if tz is not None and not isinstance(tz, str):
            raise _ToolInputError("`tz` must be an IANA timezone string")
        seed = args.get("seed")
        if seed is not None and not isinstance(seed, str):
            raise _ToolInputError("`seed` must be a string (a job name)")
        return (tz or None), (seed or None)

    async def _t_validate_schedule(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        expr = _req_str(args, "expression")
        tz, seed = self._preview_args(args)
        try:
            # count=1: the gate needs validity, lint and never_fires, not a
            # fire list; the single fire keeps never_fires truthful and
            # doubles as a confirmation of the first launch instant.
            payload = self._cron.schedule_preview_payload(
                expr, tz, count=1, seed=seed
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(payload, _preview_summary(payload))

    async def _t_explain_schedule(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        expr = _req_str(args, "expression")
        tz, seed = self._preview_args(args)
        count = _opt_int(args.get("count"))
        count = 5 if count is None else max(1, min(count, 60))
        try:
            payload = self._cron.schedule_preview_payload(
                expr, tz, count=count, seed=seed
            )
        except ValueError as err:
            return _tool_error(str(err))
        return _result(payload, _preview_summary(payload))

    async def _t_why_no_run(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        at = _req_str(args, "at")
        try:
            payload = self._cron.schedule_why_payload(name, at)
        except ValueError as err:
            return _tool_error(str(err))
        if payload is None:
            # the same lookup GET /schedule/why performs
            # (Cron._job_or_dag_schedule), so a DAG's synthetic dag:<name>
            # schedule job answers here too and the reason must not claim a
            # job was the only thing searched, nor point at a tool that
            # cannot list one. The HTTP twin says the same sentence.
            return _tool_error(
                "no job or workflow schedule named {!r}. Use "
                "cron_list_jobs or "
                "cron_list_dags to find available schedules.".format(name)
            )
        return _result(payload, _why_summary(payload))

    async def _t_get_version(self, args: dict[str, Any]) -> dict[str, Any]:
        data = {
            "version": _version.version,
            "job_set_id": self._cron.job_set_id(),
            "jobs": len(self._cron.cron_jobs),
        }
        return _result(
            data,
            "cronstable {} - {} job(s)".format(data["version"], data["jobs"]),
        )

    async def _t_tail_job_logs(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        tail = self._clamp_limit(args.get("tail"))
        payload = self._cron.job_logs_tail_payload(
            name, tail=tail, cursor=_opt_int(args.get("cursor"))
        )
        if payload is None:
            return _tool_error("job not found: {!r}".format(name))
        return _result(
            payload,
            "job {!r}: {} line(s) (cursor {})".format(
                name, len(payload["lines"]), payload["cursor"]
            ),
        )

    # -- dags tool handlers -----------------------------------------------

    async def _t_list_dags(self, args: dict[str, Any]) -> dict[str, Any]:
        dags = await self._cron.dags_payload()
        return _result({"dags": dags}, "{} workflow(s)".format(len(dags)))

    async def _t_list_dag_runs(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        limit = self._clamp_limit(args.get("limit"))
        runs = await self._cron._dag.list_runs(dag, limit=limit)
        if runs is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            {"dag": dag, "runs": runs},
            "dag {!r}: {} run(s)".format(dag, len(runs)),
        )

    async def _t_get_dag_run(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        body = await self._cron._dag.get_run(dag, run_key)
        if body is None:
            return _tool_error(
                "dag run not found: {!r}/{!r}".format(dag, run_key)
            )
        return _result(body, "dag {!r} run {!r}".format(dag, run_key))

    async def _t_get_dag_xcom(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        result = await self._cron._dag.xcom_for_run(dag, run_key)
        if result is None:
            return _tool_error(
                "dag run not found: {!r}/{!r}".format(dag, run_key)
            )
        return _result(
            result, "xcom for dag {!r} run {!r}".format(dag, run_key)
        )

    async def _t_tail_dag_task_logs(
        self, args: dict[str, Any]
    ) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        taskkey = _req_str(args, "taskkey")
        tail = self._clamp_limit(args.get("tail"))
        payload = self._cron.dag_task_logs_tail_payload(
            dag,
            run_key,
            taskkey,
            tail=tail,
            cursor=_opt_int(args.get("cursor")),
        )
        if payload is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            payload,
            "dag {!r} task {!r}: {} line(s)".format(
                dag, taskkey, len(payload["lines"])
            ),
        )

    # -- state tool handler -----------------------------------------------

    async def _t_inspect_state(self, args: dict[str, Any]) -> dict[str, Any]:
        ns = args.get("ns")
        stream = args.get("stream")
        if ns is not None and stream is not None:
            raise _ToolInputError("pass at most one of `ns` or `stream`")
        if ns is not None:
            payload = await self._cron.state_documents_payload(str(ns))
            return _result(payload, "state documents in {!r}".format(ns))
        if stream is not None:
            limit = self._clamp_limit(args.get("limit"))
            payload = await self._cron.state_records_payload(
                str(stream), limit=limit
            )
            return _result(payload, "state records in {!r}".format(stream))
        overview = await self._cron.state_payload()
        return _result(
            overview,
            "state store enabled={}".format(overview.get("enabled")),
        )

    # -- act (mutating) tool handlers -------------------------------------

    async def _t_run_job(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "running")
        queued = await self._cron.start_job_by_name(name)
        if queued is not None:
            return _result(
                {"queued": name, "queueId": queued},
                "queued job {!r}".format(name),
            )
        return _result({"started": name}, "started job {!r}".format(name))

    async def _t_cancel_job(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "cancelling")
        count = await self._cron.cancel_job_by_name(name)
        return _result(
            {"cancelled": name, "instances": count},
            "cancelled {} instance(s) of job {!r}".format(count, name),
        )

    async def _t_pause_job(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "pausing")
        duration: int | None = None
        if args.get("durationSeconds") is not None:
            duration = _opt_int(args["durationSeconds"])
            if duration is None:
                raise _ToolInputError("durationSeconds must be an integer")
        note = args.get("note")
        if note is not None and not isinstance(note, str):
            raise _ToolInputError("note must be a string")
        record = await self._cron.pause_job_by_name(
            name,
            duration=duration,
            note=note or "",
            by=_attribution(None),
            channel="mcp",
        )
        return _result(
            {"paused": name, "until": record["until"]},
            "paused job {!r} until {}".format(name, record["until"]),
        )

    async def _t_resume_job(self, args: dict[str, Any]) -> dict[str, Any]:
        name = _req_str(args, "name")
        _require_confirm(args, "resuming")
        await self._cron.resume_job_by_name(
            name, by=_attribution(None), channel="mcp"
        )
        return _result({"resumed": name}, "resumed job {!r}".format(name))

    async def _t_list_pools(self, args):
        from cronstable.pools import PoolError

        try:
            pools = await self._cron._pools.snapshot()
        except PoolError as ex:
            return _tool_error(str(ex))
        except (OSError, asyncio.TimeoutError):
            return _tool_error("pool state is unavailable")
        return _result({"pools": pools}, "resource pools")

    async def _t_cancel_queued(self, args):
        from cronstable.pools import PoolError

        _require_confirm(args, "cancelling queued work")
        try:
            entry = await self._cron._pools.cancel(
                _req_str(args, "pool"), _req_str(args, "id")
            )
        except PoolError as ex:
            return _tool_error(str(ex))
        except (OSError, asyncio.TimeoutError):
            return _tool_error("pool state is unavailable")
        return _result(
            {"id": entry["id"], "state": entry["state"]},
            "queued work cancelled",
        )

    async def _t_preview_recovery(self, args):
        return await self._recovery_tool(args, execute=False)

    async def _t_recover_dag(self, args):
        _require_confirm(args, "recovering workflow tasks")
        return await self._recovery_tool(args, execute=True)

    async def _recovery_tool(self, args, *, execute):
        from cronstable.recovery import RecoveryError

        name = _req_str(args, "dag")
        # the checks POST .../recover applies (Cron._web_dag_recover)
        token = None
        if execute:
            token = _req_str(args, "plan_token")
            if len(token) != 64:
                raise _ToolInputError(
                    "plan_token must be the 64-character token "
                    "cron_preview_recovery returned"
                )
        mode = args.get("mode", "failed")
        if not isinstance(mode, str):
            raise _ToolInputError("mode must be a string")
        allow_change = args.get("allow_config_change", False)
        if not isinstance(allow_change, bool):
            raise _ToolInputError("allow_config_change must be a boolean")
        tasks = args.get("tasks", [])
        if (
            not isinstance(tasks, list)
            or len(tasks) > 2000
            or any(not isinstance(t, str) for t in tasks)
        ):
            raise _ToolInputError(
                "tasks must be a list of task instance names"
            )
        try:
            if args.get("run_key"):
                data = await self._cron._dag.recover(
                    name,
                    _req_str(args, "run_key"),
                    mode=mode,
                    tasks=tasks,
                    plan_token=token,
                    allow_config_change=allow_change,
                )
            else:
                if tasks or mode != "failed":
                    raise _ToolInputError(
                        "date ranges recover failed tasks; "
                        "select run_key for mode 'from'"
                    )
                data = await self._cron._dag.recover_range(
                    name,
                    _req_str(args, "from"),
                    _req_str(args, "to"),
                    plan_token=token,
                    allow_config_change=allow_change,
                )
        except RecoveryError as ex:
            return _tool_error(str(ex))
        except (OSError, asyncio.TimeoutError):
            return _tool_error("recovery state is unavailable")
        return _result(
            data, "recovery started" if execute else "recovery preview"
        )

    async def _t_trigger_dag(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        _require_confirm(args, "triggering")
        run_key = await self._cron._dag.trigger_run(dag)
        if run_key is None:
            return _tool_error("dag not found: {!r}".format(dag))
        return _result(
            {"dag": dag, "runKey": run_key},
            "triggered dag {!r} (run {})".format(dag, run_key),
        )

    async def _t_backfill_dag(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        start = _req_str(args, "from")
        end = _req_str(args, "to")
        if dag not in self._cron.cron_dags:
            return _tool_error("dag not found: {!r}".format(dag))
        # dry_run defaults TRUE, tested by IDENTITY like _require_confirm:
        # only the literal boolean false may take the destructive branch.
        # ``args.get("dry_run", True)`` applied the default only when the
        # key was ABSENT, so a present-but-falsy value -- null (exactly how
        # an MCP client or LLM encodes "unspecified"), [], {}, "", 0 --
        # fell through the preview gate into a real backfill.
        if args.get("dry_run") is not False:
            return _result(
                {
                    "dag": dag,
                    "from": start,
                    "to": end,
                    "dryRun": True,
                    "wouldExecute": False,
                },
                "DRY RUN: would backfill dag {!r} from {} to {}. Call again "
                "with dry_run=false and confirm=true to execute.".format(
                    dag, start, end
                ),
            )
        _require_confirm(args, "backfilling")
        result = await self._cron._dag.backfill(dag, start, end)
        if not result.get("ok"):
            return _tool_error(str(result.get("reason")))
        return _result(
            result, "backfilled dag {!r} from {} to {}".format(dag, start, end)
        )

    async def _t_decide_gate(self, args: dict[str, Any]) -> dict[str, Any]:
        dag = _req_str(args, "dag")
        run_key = _req_str(args, "run_key")
        taskkey = _req_str(args, "taskkey")
        decision = args.get("decision")
        if decision not in ("approve", "reject"):
            raise _ToolInputError("decision must be 'approve' or 'reject'")
        _require_confirm(args, "deciding an approval gate")
        by = _attribution(args.get("by"))
        result = await self._cron._dag.approve(
            dag, run_key, taskkey, approved=(decision == "approve"), by=by
        )
        if not result.get("ok"):
            return _tool_error(str(result.get("reason")))
        return _result(
            result, "{}d gate {!r} on dag {!r}".format(decision, taskkey, dag)
        )

    # -- resources (URI-addressable read-only context) --------------------

    async def _m_resources_list(
        self, params: dict[str, Any]
    ) -> dict[str, Any]:
        resources = [
            {
                "uri": r["uri"],
                "name": r["name"],
                "title": r["title"],
                "description": r["description"],
                "mimeType": RESOURCE_MIME,
            }
            for r in self._resources
            if self._resource_visible(r)
        ]
        return {"resources": resources}

    async def _m_resource_templates_list(
        self, params: dict[str, Any]
    ) -> dict[str, Any]:
        templates = [
            {
                "uriTemplate": t["uriTemplate"],
                "name": t["name"],
                "title": t["title"],
                "description": t["description"],
                "mimeType": RESOURCE_MIME,
            }
            for t in self._templates
            if self._resource_visible(t)
        ]
        return {"resourceTemplates": templates}

    async def _m_resources_read(
        self, params: dict[str, Any]
    ) -> dict[str, Any]:
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise MCPError(INVALID_PARAMS, "resources/read requires a 'uri'")
        loader, args = self._match_resource(uri)
        if loader is None:
            raise MCPError(
                RESOURCE_NOT_FOUND, "resource not found: {}".format(uri)
            )
        try:
            data = await loader(*args)
        except ApiActionError as ex:
            raise MCPError(RESOURCE_NOT_FOUND, ex.message) from ex
        if data is None:
            raise MCPError(
                RESOURCE_NOT_FOUND, "resource not found: {}".format(uri)
            )
        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": RESOURCE_MIME,
                    "text": _dumps(data).decode("utf-8"),
                }
            ]
        }

    def _match_resource(
        self, uri: str
    ) -> tuple[Callable[..., Any] | None, tuple[str, ...]]:
        """Resolve a URI to a loader + captured args, or ``(None, ())``.

        Clients expand the templates under RFC 6570, which percent-encodes
        a name's reserved and non-ASCII characters, so each captured value
        is decoded; an encoded ``/`` (``%2F``) still matches ``[^/]+``.
        """
        fixed = self._resource_by_uri.get(uri)
        if fixed is not None and self._resource_visible(fixed):
            return fixed["loader"], ()
        for tmpl in self._templates:
            if not self._resource_visible(tmpl):
                continue
            m = tmpl["regex"].match(uri)
            if m is not None:
                return tmpl["loader"], tuple(unquote(g) for g in m.groups())
        return None, ()

    def _build_resources(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        cron = self._cron

        async def version_data() -> dict[str, Any]:
            return {
                "version": _version.version,
                "job_set_id": cron.job_set_id(),
                "jobs": len(cron.cron_jobs),
            }

        async def status_data() -> dict[str, Any]:
            return {"status": cron.status_payload()}

        async def dag_detail(name: str) -> dict[str, Any] | None:
            for entry in await cron.dags_payload():
                if entry.get("name") == name:
                    return entry
            return None

        # fixed resources
        fixed = [
            (
                "cronstable://status",
                "status",
                "Job status",
                "Live status of every job.",
                "observe",
                status_data,
            ),
            (
                "cronstable://cluster",
                "cluster",
                "Cluster view",
                "This node's cluster/leadership view.",
                "observe",
                _async(cron.cluster_payload),
            ),
            (
                "cronstable://fleet",
                "fleet",
                "Fleet view",
                "The cluster-wide jobs x nodes matrix.",
                "observe",
                _async(cron.fleet_payload),
            ),
            (
                "cronstable://version",
                "version",
                "Version",
                "Show the server version, job-set ID, and job count.",
                "observe",
                version_data,
            ),
        ]
        resources = [
            {
                "uri": uri,
                "name": name,
                "title": title,
                "description": desc,
                "toolset": toolset,
                "loader": loader,
            }
            for uri, name, title, desc, toolset, loader in fixed
        ]
        # resource templates: (uriTemplate, regex, name, title, desc, toolset,
        #  loader(*groups), completion source per variable)
        templates_spec = [
            (
                "cronstable://jobs/{name}",
                r"^cronstable://jobs/([^/]+)$",
                "job",
                "Job detail",
                "Full detail for one job.",
                "observe",
                _async1(cron.job_detail_payload),
                {"name": self._complete_jobs},
            ),
            (
                "cronstable://jobs/{name}/runs",
                r"^cronstable://jobs/([^/]+)/runs$",
                "job-runs",
                "Job run history",
                "Retained run history + stats for one job.",
                "observe",
                _async1(cron.job_runs_payload),
                {"name": self._complete_jobs},
            ),
            (
                "cronstable://dags/{name}",
                r"^cronstable://dags/([^/]+)$",
                "dag",
                "Workflow details",
                "A workflow's tasks and dependencies.",
                "dags",
                dag_detail,
                {"name": self._complete_dags},
            ),
            (
                "cronstable://dags/{name}/runs/{run_key}",
                r"^cronstable://dags/([^/]+)/runs/([^/]+)$",
                "dag-run",
                "workflow run",
                "One workflow run's full document.",
                "dags",
                cron._dag.get_run,
                {
                    "name": self._complete_dags,
                    "run_key": self._complete_runs("name"),
                },
            ),
            (
                "cronstable://state/{ns}",
                r"^cronstable://state/(.+)$",
                "state-ns",
                "State namespace",
                "Redacted documents of a kv/|cursor/|idem/ namespace.",
                "state",
                cron.state_documents_payload,
                {},
            ),
        ]
        templates = [
            {
                "uriTemplate": tmpl,
                "regex": re.compile(rx),
                "name": name,
                "title": title,
                "description": desc,
                "toolset": toolset,
                "loader": loader,
                "complete": complete,
            }
            for (
                tmpl,
                rx,
                name,
                title,
                desc,
                toolset,
                loader,
                complete,
            ) in templates_spec
        ]
        return resources, templates

    # -- prompts (canned triage playbooks) --------------------------------

    async def _m_prompts_list(self, params: dict[str, Any]) -> dict[str, Any]:
        prompts = [
            {
                "name": p["name"],
                "title": p["title"],
                "description": p["description"],
                "arguments": p["arguments"],
            }
            for p in self._prompts
            if self._prompt_visible(p)
        ]
        return {"prompts": prompts}

    async def _m_prompts_get(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str):
            raise MCPError(INVALID_PARAMS, "prompts/get requires a 'name'")
        prompt = self._prompt_by_name.get(name)
        if prompt is None or not self._prompt_visible(prompt):
            raise MCPError(INVALID_PARAMS, "unknown prompt: {}".format(name))
        args = params.get("arguments")
        if args is None:
            args = {}
        if not isinstance(args, dict):
            raise MCPError(INVALID_PARAMS, "'arguments' must be an object")
        for key, value in args.items():
            if not isinstance(value, str):
                raise MCPError(
                    INVALID_PARAMS,
                    "prompt argument {!r} must be a string".format(key),
                )
        missing = [
            a["name"]
            for a in prompt["arguments"]
            if a["required"] and not args.get(a["name"])
        ]
        if missing:
            raise MCPError(
                INVALID_PARAMS,
                "missing required prompt argument(s): {}".format(
                    ", ".join(missing)
                ),
            )
        present = frozenset(
            t for t in prompt["optional"] if self._tool_visible(t)
        )
        return {
            "description": prompt["description"],
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": prompt["render"](args, present),
                    },
                }
            ],
        }

    def _build_prompts(self) -> list[dict[str, Any]]:
        """The prompt catalog.

        Each prompt names the tools its text calls: ``requires`` gates the
        prompt itself, and an ``optional`` tool's step is rendered only when
        that tool is served, so a prompt never tells the model to call a
        tool it cannot see.
        """

        def arg(name: str, desc: str, required: bool = True) -> dict[str, Any]:
            return {"name": name, "description": desc, "required": required}

        def triage(a: dict[str, str], present: frozenset) -> str:
            return (
                "Investigate why the cronstable job '{0}' is failing. Steps:\n"
                "1. cron_get_job(name='{0}') and cron_list_runs(name='{0}') "
                "for the recent outcomes.\n"
                "2. cron_get_job_trends(name='{0}') to see if this is new or "
                "chronic.\n"
                "3. cron_tail_job_logs(name='{0}') for the failing output.\n"
                "4. cron_get_node() / cron_get_cluster() to rule out host or "
                "quorum problems.\n"
                "Then give a root-cause hypothesis, the blast radius, and the "
                "safest next action (do NOT run or cancel anything without "
                "asking)."
            ).format(a["job"])

        def dag_fail(a: dict[str, str], present: frozenset) -> str:
            return (
                "Diagnose the failed workflow run '{1}' of dag '{0}'. Use "
                "cron_get_dag_run(dag='{0}', run_key='{1}') to find the "
                "failed task(s), cron_tail_dag_task_logs(...) for their "
                "output, and cron_get_dag_xcom(dag='{0}', run_key='{1}') for "
                "the data they passed. Explain which task failed, why, and "
                "what downstream tasks were blocked."
            ).format(a["dag"], a["run_key"])

        def blast(a: dict[str, str], present: frozenset) -> str:
            steps = [
                "cron_get_status and cron_get_fleet to find other affected "
                "jobs"
            ]
            if "cron_list_dags" in present:
                steps.append(
                    "cron_list_dags to see which workflows depend on it"
                )
            if "cron_inspect_state" in present:
                steps.append(
                    "cron_inspect_state to check for shared locks/cursors it "
                    "holds"
                )
            return (
                "Assess the blast radius of an incident involving '{0}'. Use "
                "{1}. Summarize what else is at risk if it stays broken."
            ).format(a["target"], _join(steps))

        def fleet(a: dict[str, str], present: frozenset) -> str:
            return (
                "Summarize overall cronstable health for a status update. Use "
                "cron_get_fleet, cron_get_cluster and cron_get_status to "
                "report: how many jobs are failing vs healthy, the cluster "
                "quorum/leadership state, and any node under resource "
                "pressure (cron_get_node). Lead with the single most "
                "important thing."
            )

        def backfill_plan(a: dict[str, str], present: frozenset) -> str:
            return (
                "Plan a backfill of dag '{0}' from {1} to {2}. First run "
                "cron_backfill_dag(dag='{0}', from='{1}', to='{2}') with its "
                "default dry_run to preview the range, confirm the "
                "workflow exists "
                "and review the date range, then explain what a real backfill "
                "would do. Only propose the real run (dry_run=false, "
                "confirm=true) after the operator agrees."
            ).format(a["dag"], a["from"], a["to"])

        return [
            {
                "name": "triage_job_failure",
                "title": "Triage a job failure",
                "description": "Find why a job failed using its runs, "
                "trends, logs, and host health.",
                "arguments": [arg("job", "the failing job's name")],
                "requires": (
                    "cron_get_job",
                    "cron_list_runs",
                    "cron_get_job_trends",
                    "cron_tail_job_logs",
                    "cron_get_node",
                    "cron_get_cluster",
                ),
                "optional": (),
                "complete": {"job": self._complete_jobs},
                "render": triage,
            },
            {
                "name": "blast_radius",
                "title": "Assess affected jobs and workflows",
                "description": "Identify jobs and workflows affected by a "
                "failing job or workflow.",
                "arguments": [arg("target", "a job or workflow name")],
                "requires": ("cron_get_status", "cron_get_fleet"),
                "optional": ("cron_list_dags", "cron_inspect_state"),
                "complete": {"target": self._complete_targets},
                "render": blast,
            },
            {
                "name": "fleet_health_summary",
                "title": "Fleet health summary",
                "description": "A summary of cluster, node, and job health.",
                "arguments": [],
                "requires": (
                    "cron_get_fleet",
                    "cron_get_cluster",
                    "cron_get_status",
                    "cron_get_node",
                ),
                "optional": (),
                "complete": {},
                "render": fleet,
            },
            {
                "name": "why_did_dag_run_fail",
                "title": "Diagnose a failed workflow run",
                "description": "Review tasks, logs, and outputs from a failed "
                "workflow run to find the cause.",
                "arguments": [
                    arg("dag", "the workflow name"),
                    arg("run_key", "the failed run key"),
                ],
                "requires": (
                    "cron_get_dag_run",
                    "cron_tail_dag_task_logs",
                    "cron_get_dag_xcom",
                ),
                "optional": (),
                "complete": {
                    "dag": self._complete_dags,
                    "run_key": self._complete_runs("dag"),
                },
                "render": dag_fail,
            },
            {
                "name": "backfill_plan",
                "title": "Plan runs for past dates",
                "description": "Preview workflow runs for a date range "
                "before proposing a real run.",
                "arguments": [
                    arg("dag", "the workflow name"),
                    arg("from", "ISO start date"),
                    arg("to", "ISO end date"),
                ],
                "requires": ("cron_backfill_dag",),
                "optional": (),
                "complete": {"dag": self._complete_dags},
                "render": backfill_plan,
            },
        ]

    # -- argument completion ----------------------------------------------

    async def _m_complete(self, params: dict[str, Any]) -> dict[str, Any]:
        ref = params.get("ref")
        argument = params.get("argument")
        if not isinstance(ref, dict) or not isinstance(argument, dict):
            raise MCPError(
                INVALID_PARAMS,
                "completion/complete requires 'ref' and 'argument' objects",
            )
        name = argument.get("name")
        value = argument.get("value", "")
        if not isinstance(name, str) or not isinstance(value, str):
            raise MCPError(
                INVALID_PARAMS,
                "'argument' requires a string 'name' and 'value'",
            )
        context = params.get("context")
        known = context.get("arguments") if isinstance(context, dict) else None
        source = self._completion_source(ref, name)
        candidates = (
            await source(known if isinstance(known, dict) else {})
            if source is not None
            else []
        )
        prefix = value.lower()
        matches = [c for c in candidates if c.lower().startswith(prefix)]
        return {
            "completion": {
                "values": matches[:COMPLETION_MAX],
                "total": len(matches),
                "hasMore": len(matches) > COMPLETION_MAX,
            }
        }

    def _completion_source(
        self, ref: dict[str, Any], argument: str
    ) -> _CompletionSource | None:
        """The candidates for ``argument`` of ``ref``; None offers nothing.

        A reference the current config or caller cannot see offers nothing,
        exactly as an argument without a source does.
        """
        kind = ref.get("type")
        if kind == "ref/prompt":
            name = ref.get("name")
            prompt = (
                self._prompt_by_name.get(name)
                if isinstance(name, str)
                else None
            )
            if prompt is None:
                raise MCPError(
                    INVALID_PARAMS, "unknown prompt: {}".format(name)
                )
            if not self._prompts_enabled or not self._prompt_visible(prompt):
                return None
            return cast(
                _CompletionSource | None, prompt["complete"].get(argument)
            )
        if kind == "ref/resource":
            uri = ref.get("uri")
            template = (
                self._template_by_uri.get(uri)
                if isinstance(uri, str)
                else None
            )
            if template is None:
                raise MCPError(
                    INVALID_PARAMS,
                    "unknown resource template: {}".format(uri),
                )
            if not self._resources_enabled or not self._resource_visible(
                template
            ):
                return None
            return cast(
                _CompletionSource | None,
                template["complete"].get(argument),
            )
        raise MCPError(
            INVALID_PARAMS, "'ref.type' must be 'ref/prompt' or 'ref/resource'"
        )

    async def _complete_jobs(self, known: dict[str, Any]) -> list[str]:
        return list(self._cron.cron_jobs)

    async def _complete_dags(self, known: dict[str, Any]) -> list[str]:
        return list(self._cron.cron_dags)

    async def _complete_targets(self, known: dict[str, Any]) -> list[str]:
        jobs = list(self._cron.cron_jobs)
        return jobs + [d for d in self._cron.cron_dags if d not in jobs]

    def _complete_runs(self, dag_argument: str) -> _CompletionSource:
        """Recent run keys of the DAG named by the ``dag_argument`` value."""

        async def complete(known: dict[str, Any]) -> list[str]:
            dag = known.get(dag_argument)
            if not isinstance(dag, str):
                return []
            try:
                runs = await self._cron._dag.list_runs(
                    dag, limit=self._max_rows
                )
            except ApiActionError:
                return []
            return [r["runKey"] for r in runs or () if "runKey" in r]

        return complete


# -- module-level helpers -------------------------------------------------


def _async(fn: Callable[[], Any]) -> Callable[[], Awaitable[Any]]:
    """Wrap a sync payload builder as a zero-arg coroutine for a resource."""

    async def loader() -> Any:
        return fn()

    return loader


def _async1(fn: Callable[[str], Any]) -> Callable[[str], Awaitable[Any]]:
    """Wrap a sync one-arg payload builder as a coroutine for a template."""

    async def loader(arg: str) -> Any:
        return fn(arg)

    return loader


# JSON Schema fragments (draft 2020-12, the MCP default dialect).
_STR = {"type": "string"}
_INT = {"type": "integer"}
_BOOL = {"type": "boolean"}
_OBJ = {"type": "object"}


def _enum(values: list[str]) -> dict[str, Any]:
    return {"type": "string", "enum": values}


def _obj_schema(
    properties: dict[str, Any], required: list[str] | None = None
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    # anyOf rather than a type list: clients that map tool schemas onto a
    # single-type dialect reject the list form
    return {"anyOf": [schema, {"type": "null"}]}


# outputSchema fragments. Each declares only what its payload always
# carries (optional fields are typed but not required) and leaves
# additionalProperties open, so the payloads can grow.
_PAGE_OUTPUT = {
    "type": "object",
    "properties": {
        "offset": _INT,
        "limit": _INT,
        "total": _INT,
        "returned": _INT,
        "nextOffset": _nullable(_INT),
    },
    "required": ["offset", "limit", "total", "returned", "nextOffset"],
}
_STATUS_OUTPUT = {
    "type": "object",
    "properties": {
        "status": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "job": _STR,
                    "status": _enum(["running", "disabled", "scheduled"]),
                    "never_fires": _BOOL,
                },
                "required": ["job", "status"],
            },
        },
        "page": _PAGE_OUTPUT,
    },
    "required": ["status", "page"],
}
_JOBS_OUTPUT = {
    "type": "object",
    "properties": {
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": _STR,
                    "enabled": _BOOL,
                    "schedule": _STR,
                    "command": _STR,
                    "running": _BOOL,
                    "timezone": _nullable(_STR),
                    "last_run": _nullable(_OBJ),
                    "paused": _nullable(_OBJ),
                },
                "required": ["name", "enabled", "running", "paused"],
            },
        },
        "page": _PAGE_OUTPUT,
    },
    "required": ["jobs", "page"],
}
_FINDINGS = {"type": "array", "items": _OBJ}
_PREVIEW_OUTPUT = {
    "type": "object",
    "properties": {
        "expression": _STR,
        "timezone": _STR,
        "valid": _BOOL,
        "error": _STR,
        "reboot": _BOOL,
        "normalized": _STR,
        "resolved": _STR,
        "seed": _STR,
        "description": _STR,
        "fires": {"type": "array", "items": _STR},
        "never_fires": _BOOL,
        "lint": _FINDINGS,
    },
    "required": ["expression", "timezone", "valid"],
}
_WHY_OUTPUT = {
    "type": "object",
    "properties": {
        "job": _STR,
        "enabled": _BOOL,
        "timezone": _STR,
        "at": _STR,
        "at_in_zone": _STR,
        "expression": _STR,
        "resolved": _STR,
        "reboot": _BOOL,
        "description": _STR,
        "matches": _BOOL,
        "checks": _FINDINGS,
        "failed": {"type": "array", "items": _STR},
        "notes": _FINDINGS,
        "previous_fire": _nullable(_STR),
        "next_fire": _nullable(_STR),
    },
    "required": [
        "job",
        "enabled",
        "timezone",
        "at",
        "expression",
        "reboot",
        "description",
        "matches",
        "checks",
        "failed",
        "notes",
        "previous_fire",
        "next_fire",
    ],
}


def _tool(
    toolset: str,
    name: str,
    title: str,
    description: str,
    schema: dict[str, Any],
    handler: Any,
    *,
    mutating: bool = False,
    destructive: bool = False,
    idempotent: bool | None = None,
    open_world: bool = False,
    output: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One registry entry, defaults tuned to the common case.

    Read-only tools take every default; mutating tools default to
    non-idempotent (a re-run acts again) unless declared otherwise, e.g.
    pause/resume, whose repeat is a no-op. ``open_world`` marks a tool that
    runs whatever command a job or task configures.
    """
    if idempotent is None:
        idempotent = not mutating
    listing: dict[str, Any] = {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": schema,
        "annotations": {
            "title": title,
            "readOnlyHint": not mutating,
            "destructiveHint": destructive,
            "idempotentHint": idempotent,
            "openWorldHint": open_world,
        },
    }
    if output is not None:
        listing["outputSchema"] = output
    return {
        "toolset": toolset,
        "mutating": mutating,
        "name": name,
        "scope": _TOOL_SCOPE_OVERRIDES.get(
            name, "control" if mutating else "view"
        ),
        "inputSchema": schema,
        "handler": handler,
        "listing": listing,
    }


def _result(structured: dict[str, Any], summary: str) -> dict[str, Any]:
    """A successful tool result: a one-line summary, then the same data.

    The second text block is ``structuredContent`` as JSON, for clients
    that pass only ``content`` to the model.
    """
    return {
        "content": [
            {"type": "text", "text": summary},
            {"type": "text", "text": _dumps(structured).decode("utf-8")},
        ],
        "structuredContent": structured,
    }


def _tool_error(message: str) -> dict[str, Any]:
    """A tool-execution failure (isError:true), readable by the model."""
    return {"content": [{"type": "text", "text": message}], "isError": True}


def _preview_summary(payload: dict[str, Any]) -> str:
    """The one-line verdict of a validate/explain schedule payload."""
    if not payload.get("valid"):
        return "INVALID: {}".format(payload.get("error"))
    if payload.get("reboot"):
        return (
            "valid: @reboot runs when cronstable starts, without a timetable"
        )
    parts = ["valid: {}".format(payload["description"])]
    if payload.get("never_fires"):
        parts.append("WARNING: this schedule has no future runs")
    else:
        warnings = sum(
            1 for f in payload["lint"] if f.get("level") == "warning"
        )
        notes = len(payload["lint"]) - warnings
        if warnings or notes:
            parts.append(
                "{} lint warning(s), {} note(s)".format(warnings, notes)
            )
        fires = payload.get("fires") or []
        if fires:
            parts.append("first scheduled run {}".format(fires[0]))
    return "; ".join(parts)


def _why_summary(payload: dict[str, Any]) -> str:
    """The one-line verdict of a cron_why_no_run payload."""
    name = payload["job"]
    if payload.get("reboot"):
        return (
            "job {!r} is @reboot: it runs when cronstable starts and "
            "has no scheduled run time".format(name)
        )
    if payload["matches"]:
        text = "YES: the schedule of job {!r} selects {}".format(
            name, payload["at_in_zone"]
        )
        # a DST note rewrites the story ("fired at the shifted wall time"),
        # so it belongs in the one-line verdict, not only in the notes.
        for note in payload["notes"]:
            if note["code"].startswith("dst-"):
                text += "; " + note["message"]
        if not payload["enabled"]:
            return text + ", but the job is disabled, so it did not launch"
        return text + (
            "; if no run is recorded (cron_list_runs), check for "
            "server downtime, concurrencyPolicy limits, or "
            "cluster leadership"
        )
    matched = [c["field"] for c in payload["checks"] if c["matched"]]
    failed = [
        "{} {} is not in {}".format(c["field"], c["label"], c["allowed"])
        for c in payload["checks"]
        if not c["matched"]
    ]
    text = "NO"
    if matched:
        text += ": {} matched".format(", ".join(matched))
    text += "; " + "; ".join(failed)
    if not payload["enabled"]:
        text += " (the job is also disabled)"
    return text


def _req_str(args: dict[str, Any], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not value:
        raise _ToolInputError(
            "missing or empty required string argument: {!r}".format(key)
        )
    return value


def _opt_int(value: Any) -> int | None:
    # bool is an int subclass; `true` must not read as 1.
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    # OverflowError: int(float("inf")) raises it, and stdlib json parses
    # the legal literal 1e999 to inf. Fall back to the default like any
    # other unusable value instead of surfacing a -32603 internal error.
    except (TypeError, ValueError, OverflowError):
        return None


def _require_confirm(args: dict[str, Any], gerund: str) -> None:
    if args.get("confirm") is not True:
        raise _ToolInputError(
            "{} changes state; call again with confirm=true to proceed".format(
                gerund
            )
        )


def _attribution(supplied: Any) -> str:
    """The audit ``by`` of an action: the caller's token label, or ``mcp``
    without one, with a model-supplied ``by`` appended as display text."""
    caller = _caller.get()
    label = caller.label if caller is not None else "mcp"
    if supplied is None or supplied == "":
        return label
    if not isinstance(supplied, str):
        raise _ToolInputError("by must be a string")
    if len(supplied) > PAUSE_BY_MAX:
        raise _ToolInputError(
            "by is longer than {} characters".format(PAUSE_BY_MAX)
        )
    return "{} ({})".format(label, supplied)


def _join(parts: list[str]) -> str:
    """``a``, ``a and b``, ``a, b, and c``."""
    if len(parts) < 3:
        return " and ".join(parts)
    return ", ".join(parts[:-1]) + ", and " + parts[-1]


def _valid_id(value: Any) -> bool:
    """MCP request IDs are strings or integers, never null."""
    return isinstance(value, str) or (
        isinstance(value, int) and not isinstance(value, bool)
    )


def _id_of(msg: Any) -> Any:
    msg_id = msg.get("id") if isinstance(msg, dict) else None
    return msg_id if _valid_id(msg_id) else None


def _is_response(msg: dict[str, Any]) -> bool:
    """A JSON-RPC response: a reply with no method."""
    return "method" not in msg and ("result" in msg or "error" in msg)


def _request_meta(msg: dict[str, Any]) -> dict[str, Any] | None:
    params = msg.get("params")
    meta = params.get("_meta") if isinstance(params, dict) else None
    return meta if isinstance(meta, dict) else None


def _declares_modern(msg: dict[str, Any]) -> bool:
    """Whether the body names a protocol version, as only modern clients do."""
    meta = _request_meta(msg)
    return meta is not None and META_PROTOCOL_VERSION in meta


def _meta_error(msg: dict[str, Any]) -> MCPError | None:
    """A modern request's missing or malformed required ``_meta`` fields."""
    meta = _request_meta(msg) or {}
    if isinstance(meta.get(META_PROTOCOL_VERSION), str) and isinstance(
        meta.get(META_CLIENT_CAPABILITIES), dict
    ):
        return None
    return MCPError(
        INVALID_PARAMS,
        "params._meta requires a string {!r} and an object {!r}".format(
            META_PROTOCOL_VERSION, META_CLIENT_CAPABILITIES
        ),
        http_status=400,
    )


def _version_error(msg: dict[str, Any]) -> MCPError | None:
    requested = (_request_meta(msg) or {}).get(META_PROTOCOL_VERSION)
    if requested in MODERN_PROTOCOL_VERSIONS:
        return None
    return MCPError(
        UNSUPPORTED_PROTOCOL_VERSION,
        "Unsupported protocol version",
        data={
            "supported": list(ALL_PROTOCOL_VERSIONS),
            "requested": requested,
        },
        http_status=400,
    )


def _header_error(msg: dict[str, Any], headers: Any) -> MCPError | None:
    """A modern request whose mirrored headers disagree with its body.

    Called after :func:`_meta_error`, so the body's version is a string.
    """
    method = msg.get("method")
    expected = [
        (
            "MCP-Protocol-Version",
            cast(dict, _request_meta(msg))[META_PROTOCOL_VERSION],
        ),
        ("Mcp-Method", method),
    ]
    source = _MCP_NAME_SOURCE.get(method) if isinstance(method, str) else None
    if source is not None:
        params = cast(dict, msg["params"])
        expected.append(("Mcp-Name", params.get(source)))
    for header, body_value in expected:
        value = headers.get(header)
        if value is None:
            return _mismatch("missing {} header".format(header))
        if header == "Mcp-Name":
            try:
                value = _decode_header_value(value)
            except ValueError:
                return _mismatch("Mcp-Name header is malformed Base64")
        if value != body_value:
            return _mismatch(
                "{} header value {!r} does not match body value {!r}".format(
                    header, value, body_value
                )
            )
    return None


def _mismatch(detail: str) -> MCPError:
    return MCPError(
        HEADER_MISMATCH, "Header mismatch: " + detail, http_status=400
    )


def _decode_header_value(value: str) -> str:
    """Undo the ``=?base64?...?=`` form; plain values pass through."""
    if not (
        len(value) >= len(_B64_PREFIX) + len(_B64_SUFFIX)
        and value.startswith(_B64_PREFIX)
        and value.endswith(_B64_SUFFIX)
    ):
        return value
    encoded = value[len(_B64_PREFIX) : -len(_B64_SUFFIX)]
    try:
        return base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as ex:
        raise ValueError("malformed Base64 header value") from ex


def _echo_version(requested: str | None) -> str:
    """The MCP-Protocol-Version a response carries: the request's, when this
    server speaks it, else PROTOCOL_VERSION."""
    if requested in SUPPORTED_PROTOCOL_VERSIONS or (
        requested in MODERN_PROTOCOL_VERSIONS
    ):
        return cast(str, requested)
    return PROTOCOL_VERSION


def _caller_of(request: Any) -> _Caller | None:
    token = request.get(WEB_TOKEN_REQUEST_KEY)
    if token is not None:
        return _Caller(token.label, token.scopes)
    anonymous = request.get(WEB_ANON_REQUEST_KEY)
    if anonymous is not None:
        return _Caller("anonymous", frozenset(anonymous))
    return None


# The dashboard's favicon link: the one source of the server icon.
_FAVICON_RE = re.compile(
    rb'<link rel="icon" sizes="32x32" href="(data:image/png;base64,'
    rb'[A-Za-z0-9+/]+=*)"'
)


@lru_cache(maxsize=1)
def _server_icons() -> "tuple[dict[str, Any], ...]":
    """The dashboard's 32x32 PNG favicon as MCP icons, or none if absent."""
    try:
        match = _FAVICON_RE.search(_load_index_bytes())
    except OSError:
        return ()
    if match is None:
        return ()
    return (
        {
            "src": match.group(1).decode("ascii"),
            "mimeType": "image/png",
            "sizes": ["32x32"],
        },
    )


def _error_envelope(
    msg_id: Any, code: int, message: str, data: Any = None
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": error}
