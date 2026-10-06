"""The ``cronstable mcp`` stdio-to-HTTP bridge for local MCP clients.

Desktop MCP clients launch a subprocess that exchanges newline-delimited
JSON-RPC messages over stdin and stdout. This bridge forwards each request
to a running daemon's ``POST /mcp`` endpoint through ``urllib`` and writes
the reply to stdout. :mod:`cronstable.mcp` implements the tools in the
daemon, so the bridge requires a reachable daemon.

The bridge uses the standard library, :mod:`cronstable._cliargs`, and
:mod:`cronstable.webclient`, the transport it shares with ``cronstable pair``.
It avoids importing aiohttp, strictyaml, or the scheduler at startup.

For HTTPS listeners with publicly trusted certificates, no extra flags are
needed. Use ``--cacert`` for a private CA and ``--client-cert`` with
``--client-key`` when the listener requires a client certificate through
``web.tls.clientCa``. ``--insecure`` disables certificate verification.
:func:`cronstable.webclient.resolve_tls` builds the contexts.

Stdout contains only JSON-RPC messages; diagnostics go to stderr. The
bridge forwards each request and writes the one reply it gets back.
Notifications have no ``id`` and receive no reply, and a response from the
client is dropped, because the daemon sends no requests.

A frame whose ``_meta`` names a protocol version is a modern (2026-07-28)
request: the bridge copies that version, the method, and the tool, prompt,
or resource name into the headers the transport requires. Every other frame
carries the version the ``initialize`` reply negotiated.
"""

import argparse
import base64
import json
import sys
from typing import Any

from cronstable import _cliargs, webclient

# Owned by cronstable._cliargs (which registers the `mcp` subcommand for
# __main__ without importing this module); re-exported here under their
# original names.  DEFAULT_PROTOCOL_VERSION is only the wire default sent
# before initialize completes; the real negotiated version is learned from
# the initialize reply and used thereafter.
DEFAULT_PROTOCOL_VERSION = _cliargs.MCP_DEFAULT_PROTOCOL_VERSION
DEFAULT_URL = _cliargs.WEB_DEFAULT_URL
DEFAULT_TIMEOUT = _cliargs.MCP_DEFAULT_TIMEOUT

# JSON-RPC codes used when the bridge itself must synthesize an error reply.
# MCP asks for errors local to an implementation to use codes outside the
# reserved -32768..-32000 range, so a client cannot mistake the transport
# error for the daemon's.
_PARSE_ERROR = -32700
_TRANSPORT_ERROR = -31000

# The modern revision --check probes with server/discover.
MODERN_PROTOCOL_VERSION = "2026-07-28"
_META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
# The request field each method mirrors into the Mcp-Name header.
_MCP_NAME_SOURCE = {
    "tools/call": "name",
    "prompts/get": "name",
    "resources/read": "uri",
}
_B64_PREFIX = "=?base64?"
_B64_SUFFIX = "?="


def _post(
    url: str,
    frame: bytes,
    token: str | None,
    protocol_version: str,
    timeout: float,
    opener: Any = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    """POST one JSON-RPC frame to ``<url>/mcp``; return ``(status, body)``.

    ``opener`` defaults to ``None``, which sends through
    ``webclient.OPENER``.  That global is read here at call time, so a test
    can replace it by name.

    ``headers`` are a modern frame's request headers (:func:`_modern_headers`),
    which replace ``protocol_version``.
    """
    endpoint = url.rstrip("/") + "/mcp"
    request_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if headers is None:
        request_headers["MCP-Protocol-Version"] = protocol_version
    else:
        request_headers.update(headers)
    if token:
        request_headers["Authorization"] = "Bearer " + token
    status, _headers, body = webclient.send(
        endpoint,
        opener or webclient.OPENER,
        timeout,
        "the cronstable MCP endpoint at {}".format(endpoint),
        "/mcp",
        headers=request_headers,
        data=frame,
    )
    return status, body


def _bridge_stdio() -> tuple[Any, Any]:
    """The bridge's (stdin, stdout) as UTF-8 text streams.

    Resolved once at bridge start and used only by the frame loop, so help
    text and error messages elsewhere keep the console's own encoding.
    """
    return (
        webclient.utf8_stream(sys.stdin),
        webclient.utf8_stream(sys.stdout),
    )


def _emit(out: Any, obj: Any) -> None:
    """Write one JSON frame to ``out`` (stdout carries only JSON-RPC)."""
    out.write(json.dumps(obj) + "\n")
    out.flush()


def _write_reply(out: Any, body: bytes) -> None:
    """Write a daemon reply body to ``out`` as one newline-terminated frame.

    The daemon's body is raw UTF-8 JSON; decoding here and writing through
    the UTF-8 stream from :func:`_bridge_stdio` re-emits those bytes exactly,
    with a single trailing LF as the frame delimiter.
    """
    out.write(body.decode("utf-8").rstrip("\n") + "\n")
    out.flush()


def _error_frame(out: Any, msg_id: Any, code: int, message: str) -> None:
    _emit(
        out,
        {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": code, "message": message},
        },
    )


def _run_bridge(args: argparse.Namespace) -> int:
    try:
        # Built once, before the read loop rather than per frame: an SSL
        # context parses the CA and the client key off disk, which has no
        # business on a path walked once per JSON-RPC message. Unusable
        # material is fatal here instead of an error frame per request,
        # because nothing about a bad path improves mid-session. The same
        # goes for a token that no header can carry.
        token = webclient.resolve_token(args)
        opener = webclient.build_opener(webclient.resolve_tls(args))
    except webclient.ClientError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    protocol_version = args.protocol_version or DEFAULT_PROTOCOL_VERSION
    # UTF-8 stdio for the frame loop only (see _bridge_stdio), resolved here,
    # after the fatal-error path above, so a bridge that never starts its
    # loop leaves the process streams untouched.
    stdin, stdout = _bridge_stdio()
    try:
        for line in stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                _error_frame(stdout, None, _PARSE_ERROR, "parse error")
                continue
            if _is_response(msg):
                continue  # the daemon sends no requests to answer
            is_request = isinstance(msg, dict) and "id" in msg
            msg_id = msg.get("id") if isinstance(msg, dict) else None
            method = msg.get("method") if isinstance(msg, dict) else None
            modern = _modern_headers(msg)
            try:
                status, body = _post(
                    args.url,
                    line.encode("utf-8"),
                    token,
                    protocol_version,
                    args.timeout,
                    opener=opener,
                    headers=modern,
                )
            except webclient.ClientError as ex:
                if is_request:
                    _error_frame(stdout, msg_id, _TRANSPORT_ERROR, str(ex))
                else:
                    print(str(ex), file=sys.stderr)
                continue
            # learn the negotiated protocol version from the initialize reply
            # and stamp it on every subsequent legacy request (how a
            # forwarding proxy discovers the value it must send).
            if (
                modern is None
                and method == "initialize"
                and status == 200
                and body
            ):
                sniffed = _sniff_protocol_version(body)
                if sniffed is not None:
                    protocol_version = sniffed
            if not is_request:
                continue  # a notification gets no reply frame
            if status == 200 and body:
                _write_reply(stdout, body)
                continue
            # a modern 400/404 carries the JSON-RPC error a client needs to
            # pick a version or fall back, so it is passed through as is.
            reply = _jsonrpc_error(body, msg_id)
            if reply is not None:
                _emit(stdout, reply)
            else:
                _error_frame(
                    stdout,
                    msg_id,
                    _TRANSPORT_ERROR,
                    _http_error_message(status, body),
                )
    finally:
        webclient.release_stream(stdin, sys.stdin)
        webclient.release_stream(stdout, sys.stdout)
    return 0


def _is_response(msg: Any) -> bool:
    """A JSON-RPC response: a reply with no method."""
    return (
        isinstance(msg, dict)
        and "method" not in msg
        and ("result" in msg or "error" in msg)
    )


def _plain_header_value(value: str) -> bool:
    """Visible ASCII, spaces and tabs, with no surrounding whitespace."""
    return value == value.strip() and all(
        " " <= ch <= "~" or ch == "\t" for ch in value
    )


def _encode_header_value(value: str) -> str:
    """``value``, or its ``=?base64?...?=`` form when it is not plain ASCII
    or would read as that form itself."""
    if _plain_header_value(value) and not (
        value.startswith(_B64_PREFIX) and value.endswith(_B64_SUFFIX)
    ):
        return value
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return _B64_PREFIX + encoded + _B64_SUFFIX


def _modern_headers(msg: Any) -> dict[str, str] | None:
    """The headers a modern frame's POST carries, or None for a legacy frame.

    A value that cannot travel as a header is left out, and the daemon then
    names the missing header in its HeaderMismatch error.
    """
    if not isinstance(msg, dict):
        return None
    params = msg.get("params")
    if not isinstance(params, dict):
        return None
    meta = params.get("_meta")
    if not isinstance(meta, dict) or _META_PROTOCOL_VERSION not in meta:
        return None
    method = msg.get("method")
    headers = {}
    for header, value in (
        ("MCP-Protocol-Version", meta[_META_PROTOCOL_VERSION]),
        ("Mcp-Method", method),
    ):
        if isinstance(value, str) and _plain_header_value(value):
            headers[header] = value
    source = _MCP_NAME_SOURCE.get(method) if isinstance(method, str) else None
    if source is not None and isinstance(params.get(source), str):
        headers["Mcp-Name"] = _encode_header_value(params[source])
    return headers


def _jsonrpc_error(body: bytes, msg_id: Any) -> dict[str, Any] | None:
    """A daemon error body that is a JSON-RPC error, given the frame's id."""
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    error = parsed.get("error")
    if not isinstance(error, dict) or not isinstance(error.get("code"), int):
        return None
    parsed.setdefault("jsonrpc", "2.0")
    if parsed.get("id") is None:
        parsed["id"] = msg_id
    return parsed


def _sniff_protocol_version(body: bytes) -> str | None:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    result = parsed.get("result") if isinstance(parsed, dict) else None
    pv = result.get("protocolVersion") if isinstance(result, dict) else None
    return pv if isinstance(pv, str) else None


def _http_error_message(status: int, body: bytes) -> str:
    message = "MCP endpoint returned HTTP {}".format(status)
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and parsed.get("error"):
            message = "{}: {}".format(message, parsed["error"])
    except ValueError:
        pass
    return message


_CLIENT_INFO = {"name": "cronstable-mcp-check", "version": "0"}


def _check(args: argparse.Namespace) -> int:
    """Handshake self-test, reported on stderr.

    Probes with ``server/discover`` first and falls back to ``initialize``
    when the daemon does not answer it, then counts ``tools/list``.
    """
    try:
        # Same one-shot build as the bridge, and the same reasoning: a --check
        # that cannot even assemble its token or TLS material has failed, so
        # say so once here rather than twice through the later round-trips.
        token = webclient.resolve_token(args)
        opener = webclient.build_opener(webclient.resolve_tls(args))
    except webclient.ClientError as ex:
        print("mcp check: {}".format(ex), file=sys.stderr)
        return 1

    def post(frame: dict[str, Any], version: str) -> tuple[int, bytes]:
        return _post(
            args.url,
            json.dumps(frame).encode(),
            token,
            version,
            args.timeout,
            opener=opener,
            headers=_modern_headers(frame),
        )

    meta = {
        _META_PROTOCOL_VERSION: MODERN_PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientInfo": _CLIENT_INFO,
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    probe = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "server/discover",
        "params": {"_meta": meta},
    }
    pv = args.protocol_version or DEFAULT_PROTOCOL_VERSION
    try:
        status, body = post(probe, MODERN_PROTOCOL_VERSION)
        supported = _discovered_versions(body) if status == 200 else None
        if supported is not None:
            negotiated = MODERN_PROTOCOL_VERSION
            era = "modern; the daemon serves {}".format(", ".join(supported))
            tools_params: dict[str, Any] = {"_meta": meta}
        else:
            init = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": pv,
                    "capabilities": {},
                    "clientInfo": _CLIENT_INFO,
                },
            }
            status, body = post(init, pv)
            if status != 200:
                print(
                    "mcp check: initialize failed ({})".format(
                        _http_error_message(status, body)
                    ),
                    file=sys.stderr,
                )
                return 1
            negotiated = _sniff_protocol_version(body) or pv
            era = "legacy"
            tools_params = {}
    except webclient.ClientError as ex:
        print("mcp check: {}".format(ex), file=sys.stderr)
        return 1
    listing = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    if tools_params:
        listing["params"] = tools_params
    try:
        _s, body2 = post(listing, negotiated)
        tools = json.loads(body2).get("result", {}).get("tools", [])
    except (webclient.ClientError, ValueError, AttributeError):
        tools = []
    print(
        "mcp check: ok - protocol {} ({}), {} tool(s) at {}".format(
            negotiated, era, len(tools), args.url.rstrip("/") + "/mcp"
        ),
        file=sys.stderr,
    )
    return 0


def _discovered_versions(body: bytes) -> list[str] | None:
    """The versions a ``server/discover`` reply lists, when it lists ours."""
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    result = parsed.get("result") if isinstance(parsed, dict) else None
    versions = (
        result.get("supportedVersions") if isinstance(result, dict) else None
    )
    if (
        not isinstance(versions, list)
        or MODERN_PROTOCOL_VERSION not in versions
    ):
        return None
    return [str(v) for v in versions]


# The `cronstable mcp` parser definition lives in cronstable._cliargs so
# __main__ registers the subcommand without importing this module until the
# bridge is actually dispatched; re-exported under its original name.
add_mcp_command = _cliargs.add_mcp_command


def dispatch(args: argparse.Namespace) -> int:
    if getattr(args, "mcp_check", False):
        return _check(args)
    return _run_bridge(args)
