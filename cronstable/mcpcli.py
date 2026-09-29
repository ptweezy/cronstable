"""The ``cronstable mcp`` stdio-to-HTTP bridge for local MCP clients.

Desktop MCP clients launch a subprocess that exchanges newline-delimited
JSON-RPC messages over stdin and stdout. This bridge forwards each request
to a running daemon's ``POST /mcp`` endpoint through ``urllib`` and writes
the reply to stdout. :mod:`cronstable.mcp` implements the tools in the
daemon, so the bridge requires a reachable daemon.

The bridge uses the standard library and :mod:`cronstable._cliargs`.
It avoids importing aiohttp, strictyaml, or the scheduler at startup.

For HTTPS listeners with publicly trusted certificates, no extra flags are
needed. Use ``--cacert`` for a private CA and ``--client-cert`` with
``--client-key`` when the listener requires a client certificate through
``web.tls.clientCa``. ``--insecure`` disables certificate verification.
:func:`_resolve_tls` imports :mod:`cronstable.tlsutil` to build the contexts.

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
import io
import json
import os
import sys
import urllib.error
import urllib.request
from typing import TYPE_CHECKING, Any, Optional

from cronstable import _cliargs

if TYPE_CHECKING:  # pragma: no cover - annotations only
    # ssl is imported inside the two functions that name it at runtime, so the
    # module-level import block stays exactly what the header promises.
    import ssl

# Owned by cronstable._cliargs (which registers the `mcp` subcommand for
# __main__ without importing this module); re-exported here under their
# original names.  DEFAULT_PROTOCOL_VERSION is only the wire default sent
# before initialize completes; the real negotiated version is learned from
# the initialize reply and used thereafter.  ENV_TOKEN is the env var
# cronstable's own docs use for the web bearer token, consulted as a
# convenience when neither --token nor --token-env is given, and the other
# ENV_* names are the env fallbacks for the client TLS flags.
DEFAULT_PROTOCOL_VERSION = _cliargs.MCP_DEFAULT_PROTOCOL_VERSION
DEFAULT_URL = _cliargs.WEB_DEFAULT_URL
DEFAULT_TIMEOUT = _cliargs.MCP_DEFAULT_TIMEOUT
ENV_TOKEN = _cliargs.WEB_ENV_TOKEN
ENV_CACERT = _cliargs.WEB_ENV_CACERT
ENV_CLIENT_CERT = _cliargs.WEB_ENV_CLIENT_CERT
ENV_CLIENT_KEY = _cliargs.WEB_ENV_CLIENT_KEY
ENV_INSECURE = _cliargs.WEB_ENV_INSECURE

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

# Loopback/control traffic must never be proxied (the daemon's endpoint is
# usually 127.0.0.1, which an external proxy cannot reach), matching jobcli.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _build_opener(
    ctx: Optional["ssl.SSLContext"],
) -> urllib.request.OpenerDirector:
    """The opener this invocation posts through: the shared one, or a TLS one.

    A ``None`` context (no TLS options given, the overwhelmingly common case)
    returns the module-level ``_OPENER`` UNCHANGED rather than an equivalent
    copy: that global is the no-TLS default and the seam the tests monkeypatch
    by name, and handing back a copy would silently detach both.

    With a context, the same proxy-free handler is paired with an HTTPSHandler
    bound to it, so only the HTTPS transport changes; ``build_opener`` fills in
    the rest of its stock handlers exactly as it does above.
    """
    if ctx is None:
        return _OPENER
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=ctx),
    )


class _BridgeError(Exception):
    """A transport failure reaching the daemon's ``/mcp`` endpoint."""


def _resolve_token(args: argparse.Namespace) -> Optional[str]:
    if args.token:
        return str(args.token)
    env_name = args.token_env or ENV_TOKEN
    value = os.environ.get(env_name)
    return value or None


def _resolve_tls(args: argparse.Namespace) -> Optional["ssl.SSLContext"]:
    """The client TLS posture for this invocation, or ``None`` for the default.

    Flag then env, the same precedence as :func:`_resolve_token`, so a shell
    that already exports the bearer token can export its trust material beside
    it.  ``None`` comes back when nothing is set, which leaves the opener (and
    therefore every plaintext ``http://`` bridge that existed before TLS) on
    exactly the transport it had.
    """
    # Imported at the point of use, not at module load: this bridge's header
    # promises a standard-library-only import block, and tlsutil is the one
    # cronstable module it needs. tlsutil is itself a stdlib-only leaf, so this
    # pulls in nothing further.
    from cronstable import tlsutil

    ca = args.cacert or os.environ.get(ENV_CACERT) or None
    cert = args.client_cert or os.environ.get(ENV_CLIENT_CERT) or None
    key = args.client_key or os.environ.get(ENV_CLIENT_KEY) or None
    insecure = bool(args.insecure) or (
        os.environ.get(ENV_INSECURE, "").lower() in ("1", "true", "yes")
    )
    if insecure:
        # Deliberately never silent. Verification is off but the Authorization
        # header is still sent, so the token goes to whoever answers the
        # connection, which is precisely what an interception would want.
        print(
            "warning: --insecure disables TLS certificate verification; "
            "this can expose the bearer token to an untrusted server",
            file=sys.stderr,
        )
    try:
        return tlsutil.build_verifying_client_ssl_context(
            ca=ca, cert=cert, key=key, insecure=insecure
        )
    except (OSError, ValueError) as ex:
        # OSError is a missing/unreadable file or malformed PEM (ssl.SSLError
        # subclasses it); ValueError is --client-key with no --client-cert,
        # which tlsutil refuses rather than ignore. Without this arm an
        # operator's typo in a path exits with a traceback instead of the
        # clean error every other failure in this bridge produces.
        #
        # The paths are echoed because ssl does NOT name them: a missing file
        # surfaces as a bare "[Errno 2] No such file or directory", which
        # leaves an operator who fat-fingered one of three paths with nothing
        # to look at.
        given = ", ".join(
            "{}={}".format(flag, path)
            for flag, path in (
                ("--cacert", ca),
                ("--client-cert", cert),
                ("--client-key", key),
            )
            if path
        )
        raise _BridgeError(
            "cannot use the given TLS material{}: {}".format(
                " ({})".format(given) if given else "", ex
            )
        ) from ex


def _post(
    url: str,
    frame: bytes,
    token: Optional[str],
    protocol_version: str,
    timeout: float,
    opener: Any = None,
    headers: Optional[dict[str, str]] = None,
) -> tuple[int, bytes]:
    """POST one JSON-RPC frame to ``<url>/mcp``; return ``(status, body)``.

    ``opener`` trails the original signature and defaults to ``None`` so a
    five-argument call still goes through the module-level ``_OPENER``.  That
    global is read here at call time rather than captured as the parameter
    default, which is what keeps it monkeypatchable by name.

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
    req = urllib.request.Request(
        endpoint, data=frame, method="POST", headers=request_headers
    )
    via = opener or _OPENER
    try:
        with via.open(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, resp.read()
    except urllib.error.HTTPError as ex:
        # HTTPError holds the response. Close it to release the connection.
        with ex:
            return ex.code, ex.read()
    except urllib.error.URLError as ex:
        # urllib wraps a failed handshake as URLError(reason=ssl.SSLError),
        # which the generic arm below would report as "cannot reach": that
        # sends the operator hunting a firewall or a wrong port when the
        # socket connected fine and only verification failed. Imported here
        # for the same reason as the tlsutil import in _resolve_tls.
        import ssl

        if isinstance(ex.reason, ssl.SSLError):
            raise _BridgeError(
                "TLS verification failed for the cronstable MCP endpoint at "
                "{}: {} (pass --cacert with the CA that signed the "
                "listener's certificate, or --insecure to skip verification "
                "entirely)".format(endpoint, ex.reason)
            ) from ex
        raise _BridgeError(
            "cannot reach the cronstable MCP endpoint at {}: {}".format(
                endpoint, ex.reason
            )
        ) from ex
    except (TimeoutError, OSError) as ex:
        raise _BridgeError(
            "cannot reach the cronstable MCP endpoint at {}: {}".format(
                endpoint, ex
            )
        ) from ex


def _utf8_stream(stream: Any) -> Any:
    """``stream`` as UTF-8 text, wrapping its binary buffer when it has one.

    MCP mandates UTF-8 JSON-RPC and the daemon replies in raw UTF-8, but a
    piped stdio pair on Windows defaults to the ANSI codepage (cp1252): an
    emoji or box-drawing char in a tool result then raises
    UnicodeEncodeError on write and kills the bridge mid-session, and
    inbound non-ASCII arrives mojibake.  Wrapping the underlying binary
    buffer pins the bridge to UTF-8 on every platform.  ``newline=""``
    keeps frames byte-exact in both directions: no CRLF translation on
    write (a frame ends with a bare LF) and untranslated line endings on
    read (the loop strips them anyway).

    A stream without a binary buffer (a test's StringIO, a captured or
    already-detached stream) is reconfigured in place when it supports
    that, and otherwise handed back as-is, so the bridge still runs over
    whatever the harness supplied.
    """
    buffer = getattr(stream, "buffer", None)
    if buffer is not None:
        try:
            return io.TextIOWrapper(
                buffer, encoding="utf-8", newline="", write_through=True
            )
        except (OSError, ValueError):
            pass
    try:
        stream.reconfigure(encoding="utf-8")
    except (AttributeError, OSError, ValueError):
        pass
    return stream


def _bridge_stdio() -> tuple[Any, Any]:
    """The bridge's (stdin, stdout) as UTF-8 text streams.

    Resolved once at bridge start and used only by the frame loop, so help
    text and error messages elsewhere keep the console's own encoding.
    """
    return _utf8_stream(sys.stdin), _utf8_stream(sys.stdout)


def _release_stream(wrapped: Any, original: Any) -> None:
    """Detach a wrapper made by :func:`_utf8_stream`, leaving ``original``
    usable.

    A dropped TextIOWrapper CLOSES its buffer, and that buffer belongs to
    the real stdin/stdout, which whatever runs after the bridge (the
    interpreter's own shutdown, a test harness) still owns.  Flush what the
    wrapper holds, then detach it so nothing is closed underneath the
    original stream.  A stream that was passed through as-is (the
    no-buffer fallback) has nothing to release.
    """
    if wrapped is original:
        return
    try:
        wrapped.flush()
    except (OSError, ValueError):
        pass
    try:
        wrapped.detach()
    except (OSError, ValueError):
        pass


def _emit(out: Any, obj: Any) -> None:
    """Write one JSON frame to ``out`` (stdout carries only JSON-RPC)."""
    out.write(json.dumps(obj) + "\n")
    out.flush()


def _write_reply(out: Any, body: bytes) -> None:
    """Write a daemon reply body to ``out`` as one newline-terminated frame.

    The daemon's body is raw UTF-8 JSON; decoding here and writing through
    the UTF-8 stream from :func:`_utf8_stream` re-emits those bytes exactly,
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
    token = _resolve_token(args)
    try:
        # Built once, before the read loop rather than per frame: an SSL
        # context parses the CA and the client key off disk, which has no
        # business on a path walked once per JSON-RPC message. Unusable
        # material is fatal here instead of an error frame per request,
        # because nothing about a bad path improves mid-session.
        opener = _build_opener(_resolve_tls(args))
    except _BridgeError as ex:
        print(str(ex), file=sys.stderr)
        return 1
    protocol_version = args.protocol_version or DEFAULT_PROTOCOL_VERSION
    # UTF-8 stdio for the frame loop only (see _utf8_stream): resolved here,
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
            except _BridgeError as ex:
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
        _release_stream(stdin, sys.stdin)
        _release_stream(stdout, sys.stdout)
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


def _modern_headers(msg: Any) -> Optional[dict[str, str]]:
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


def _jsonrpc_error(body: bytes, msg_id: Any) -> Optional[dict[str, Any]]:
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


def _sniff_protocol_version(body: bytes) -> Optional[str]:
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
    token = _resolve_token(args)
    try:
        # Same one-shot build as the bridge, and the same reasoning: a --check
        # that cannot even assemble its TLS material has failed, so say so
        # once here rather than twice through the round-trips below.
        opener = _build_opener(_resolve_tls(args))
    except _BridgeError as ex:
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
    except _BridgeError as ex:
        print("mcp check: {}".format(ex), file=sys.stderr)
        return 1
    listing = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    if tools_params:
        listing["params"] = tools_params
    try:
        _s, body2 = post(listing, negotiated)
        tools = json.loads(body2).get("result", {}).get("tools", [])
    except (_BridgeError, ValueError, AttributeError):
        tools = []
    print(
        "mcp check: ok - protocol {} ({}), {} tool(s) at {}".format(
            negotiated, era, len(tools), args.url.rstrip("/") + "/mcp"
        ),
        file=sys.stderr,
    )
    return 0


def _discovered_versions(body: bytes) -> Optional[list[str]]:
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
