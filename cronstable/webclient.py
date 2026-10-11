"""The standard-library HTTP client that the daemon's terminal clients share.

``cronstable mcp``, ``cronstable pair``, the terminal dashboard's Pair a
device panel, and the job state commands reach a running daemon through this
module. It holds the bearer token and TLS flags from
:mod:`cronstable._cliargs`, an opener that uses no proxy and follows no
redirect, a request function whose failures name the server, and UTF-8
wrappers for the standard streams.

This module imports the standard library and :mod:`cronstable._cliargs`.
:func:`resolve_tls` imports :mod:`cronstable.tlsutil` to build the context.
"""

import argparse
import http.client
import io
import os
import re
import sys
import urllib.error
import urllib.request
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from cronstable import _cliargs

if TYPE_CHECKING:  # pragma: no cover - annotations only
    # ssl is imported inside the functions that name it at runtime, so the
    # module-level import block stays what the header promises.
    import ssl

# What no header value can carry: a control character other than a tab, or
# a lone surrogate, which UTF-8 cannot encode.
_UNSENDABLE = re.compile(r"[\x00-\x08\x0a-\x1f\x7f\ud800-\udfff]")

# The longest timeout, in seconds, that a socket takes on every platform:
# about 24.9 days. Windows counts a timeout in milliseconds in a C int, and
# a socket there raises OverflowError for a longer one.
_LONGEST_TIMEOUT = 2147483.0


class ClientError(Exception):
    """A request that got no usable reply from the daemon.

    ``detail`` is the message without ``advice``, which names this client's
    command-line flags. A caller that gives its own advice reads ``detail``.
    """

    def __init__(self, detail: str, advice: str = "") -> None:
        super().__init__(
            "{} ({})".format(detail, advice) if advice else detail
        )
        self.detail = detail


class TLSError(ClientError):
    """A TLS handshake that failed, most often on certificate verification."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Leave a redirect unfollowed, so the caller gets the 3xx reply.

    urllib resends every request header to a redirect's target, which
    would hand the bearer token to an address the operator did not name.
    The handler takes each redirect status before urllib parses the
    target, because that parse raises ``ValueError`` on a malformed one.
    """

    def http_error_302(self, *args: Any, **kwargs: Any) -> None:
        return None

    http_error_301 = http_error_303 = http_error_302
    http_error_307 = http_error_308 = http_error_302


# Loopback/control traffic must never be proxied: the daemon's endpoint is
# usually 127.0.0.1, which an external proxy cannot reach, and urllib's
# default opener would send the bearer token to the proxy that
# http_proxy/HTTP_PROXY names.
OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}), NoRedirect()
)


def build_opener(
    ctx: "ssl.SSLContext | None",
) -> urllib.request.OpenerDirector:
    """The opener one invocation sends through: the shared one, or a TLS one.

    A ``None`` context returns the module-level ``OPENER`` itself, read at
    call time, because that name is the seam the tests replace.

    With a context, the same proxy-free and redirect-free handlers are paired
    with an HTTPSHandler bound to it, so only the HTTPS transport changes.
    """
    if ctx is None:
        return OPENER
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        NoRedirect(),
        urllib.request.HTTPSHandler(context=ctx),
    )


def verifies(ctx: "ssl.SSLContext | None") -> bool:
    """Whether an ``https://`` request through ``ctx`` checks the server's
    certificate. ``None`` is the library default, which does."""
    if ctx is None:
        return True
    import ssl

    return ctx.verify_mode != ssl.CERT_NONE


def resolve_token(args: argparse.Namespace) -> str | None:
    """The bearer token: ``--token``, then the ``--token-env`` variable.

    Raises :exc:`ClientError` for a token that no header can carry, over
    this module's transport or aiohttp's. The message names where the
    token came from and leaves the token out.
    """
    if args.token:
        token, source = str(args.token), "--token"
    else:
        env_name = args.token_env or _cliargs.WEB_ENV_TOKEN
        token = os.environ.get(env_name) or ""
        source = "the {} environment variable".format(env_name)
    if _UNSENDABLE.search(token):
        raise ClientError(
            "the access token from {} holds a line break or another "
            "character that an HTTP header cannot carry".format(source),
            "check it for a trailing newline",
        )
    return token or None


def resolve_tls(args: argparse.Namespace) -> "ssl.SSLContext | None":
    """The client TLS posture for this invocation, or ``None`` for the default.

    Flag then env, the same precedence as :func:`resolve_token`, so a shell
    that already exports the bearer token can export its trust material beside
    it. ``None`` comes back when nothing is set, which leaves a plaintext
    ``http://`` client on the shared opener.
    """
    # Imported at the point of use, so the module-level import block stays
    # what the header promises. tlsutil is itself a stdlib-only leaf, so this
    # pulls in nothing further.
    from cronstable import tlsutil

    ca = args.cacert or os.environ.get(_cliargs.WEB_ENV_CACERT) or None
    cert = (
        args.client_cert
        or os.environ.get(_cliargs.WEB_ENV_CLIENT_CERT)
        or None
    )
    key = (
        args.client_key or os.environ.get(_cliargs.WEB_ENV_CLIENT_KEY) or None
    )
    insecure = bool(args.insecure) or (
        os.environ.get(_cliargs.WEB_ENV_INSECURE, "").lower()
        in ("1", "true", "yes")
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
        # clean error every other failure in this client produces.
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
        raise ClientError(
            "cannot use the given TLS material{}: {}".format(
                " ({})".format(given) if given else "", ex
            )
        ) from ex


def _moved_base(url: str, target: str, path: str) -> str | None:
    """The base URL that serves ``path`` at ``target``, or ``None``.

    ``url`` is the request's own address, which ends with ``path``. A
    target on the same base, or one that leads somewhere other than
    ``path``, gives no base to use instead.
    """
    try:
        parts = urlsplit(target)
    except ValueError:
        return None
    if not path or not parts.path.endswith(path):
        return None
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None
    base = urlunsplit(
        (parts.scheme, parts.netloc, parts.path[: -len(path)], "", "")
    )
    return None if base == url[: -len(path)] else base


def send(
    url: str,
    opener: Any,
    timeout: float,
    what: str,
    path: str = "",
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    method: str | None = None,
    limit: int | None = None,
) -> tuple[int, Any, bytes]:
    """Request ``url``; return the reply's ``(status, headers, body)``.

    Without ``method``, ``data`` makes the request a POST. A redirect, an
    address that is not ``http://`` or ``https://``, a request that cannot
    be built, and every transport failure raise :exc:`ClientError`, whose
    message names the server as ``what``. Any other status is the caller's
    to read.

    ``path`` is the endpoint that the caller appended to the operator's
    ``--url``. A redirect to that endpoint on another base names the base
    to pass instead.

    ``timeout`` is the number of seconds to wait on the socket. One that
    is not greater than 0 raises :exc:`ClientError`, and one longer than
    ``_LONGEST_TIMEOUT`` waits that long.

    ``limit`` is the most bytes of the body to read. A caller that reads
    only the headers passes 0.
    """
    sent = {}
    for name, value in (headers or {}).items():
        if _UNSENDABLE.search(value):
            # The value stays out of the message: it can be the token.
            raise ClientError(
                "cannot send a request to {}: the {} header holds a "
                "character that HTTP does not allow".format(what, name)
            )
        # http.client writes a header as Latin-1, and the daemon reads it
        # as UTF-8, so each UTF-8 byte goes in as one Latin-1 character.
        sent[name] = value.encode("utf-8").decode("latin-1")

    def read(resp: Any) -> bytes:
        body: bytes = resp.read() if limit is None else resp.read(limit)
        return body

    try:
        req = urllib.request.Request(
            url, data=data, headers=sent, method=method
        )
    except ValueError as ex:
        raise ClientError(
            "cannot send a request to {}: the address is not a valid URL: "
            "{}".format(what, ex)
        ) from ex
    if req.type not in ("http", "https"):
        # urllib also opens file:, ftp:, and data: addresses, and their
        # replies have no HTTP status.
        raise ClientError(
            "cannot send a request to {}: the address is not an http:// or "
            "https:// URL".format(what)
        )
    if not timeout > 0:
        # A socket raises ValueError for a negative timeout and for one
        # that is not a number, and a timeout of 0 makes it non-blocking.
        # NaN fails the comparison, so this branch refuses it too.
        raise ClientError(
            "cannot send a request to {}: the timeout of {:g} seconds is "
            "not greater than 0".format(what, timeout)
        )
    timeout = min(timeout, _LONGEST_TIMEOUT)
    try:
        try:
            with opener.open(req, timeout=timeout) as resp:
                status, reply, body = resp.status, resp.headers, read(resp)
        except urllib.error.HTTPError as ex:
            # HTTPError holds the response. Close it to release the
            # connection.
            with ex:
                status, reply, body = ex.code, ex.headers or {}, read(ex)
    except urllib.error.URLError as ex:
        # urllib wraps a failed handshake as URLError(reason=ssl.SSLError),
        # which the later generic arm would report as "cannot reach": that
        # sends the operator hunting a firewall or a wrong port when the
        # socket connected fine and only verification failed. Imported here
        # for the same reason as the tlsutil import in resolve_tls.
        import ssl

        if isinstance(ex.reason, ssl.SSLError):
            raise TLSError(
                "TLS verification failed for {}: {}".format(what, ex.reason),
                "pass --cacert with the CA that signed the listener's "
                "certificate, or --insecure to skip verification entirely",
            ) from ex
        raise ClientError(
            "cannot reach {}: {}".format(what, ex.reason)
        ) from ex
    except (TimeoutError, OSError) as ex:
        raise ClientError("cannot reach {}: {}".format(what, ex)) from ex
    except (http.client.InvalidURL, UnicodeError) as ex:
        # http.client refuses the address before it sends anything. The
        # headers passed their own check, so the fault is the address's.
        raise ClientError(
            "cannot send a request to {}: the address is not a valid URL: "
            "{}".format(what, ex)
        ) from ex
    except http.client.HTTPException as ex:
        # A reply that is not HTTP, or one cut short. The repr escapes the
        # peer's bytes, which the message can quote.
        raise ClientError(
            "no HTTP reply from {}: {!r}".format(what, ex)
        ) from ex
    location = reply.get("Location")
    if 300 <= status < 400 and location:
        try:
            target = urljoin(req.full_url, location)
        except ValueError:
            target = location
        base = _moved_base(req.full_url, target, path)
        # The repr escapes the target and the base, which are the server's
        # text.
        raise ClientError(
            "{} redirects to {!r}, which this client does not follow".format(
                what, target
            ),
            "pass --url {!r} instead".format(base)
            if base
            else "point --url at an address that answers without a redirect",
        )
    return status, reply, body


def utf8_stream(stream: Any) -> Any:
    """``stream`` as UTF-8 text, wrapping its binary buffer when it has one.

    A piped stdio pair on Windows defaults to the ANSI code page (cp1252)
    whenever UTF-8 Mode is off, the default on Python 3.14 and earlier. That
    code page has no emoji, box-drawing, or block characters: writing one
    raises UnicodeEncodeError, and inbound non-ASCII arrives as mojibake.
    Wrapping the underlying binary buffer pins the stream to UTF-8 on every
    platform and Python version.
    ``newline=""`` keeps the text byte-exact in both directions: no CRLF
    translation on write and untranslated line endings on read.

    A stream without a binary buffer (a test's StringIO, a captured or
    already-detached stream) is reconfigured in place when it supports
    that, and otherwise handed back as-is.
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


def release_stream(wrapped: Any, original: Any) -> None:
    """Detach a wrapper made by :func:`utf8_stream`, leaving ``original``
    usable.

    A dropped TextIOWrapper CLOSES its buffer, and that buffer belongs to
    the real stdin/stdout, which whatever runs after the client (the
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
