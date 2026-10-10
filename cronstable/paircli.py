"""The ``cronstable pair`` command: the app's pairing QR code in a terminal.

The web dashboard's Pair a device panel draws this code in a browser. This
command draws it for a daemon that serves the HTTP API without the
dashboard page (``web.ui: false``) and for a shell with no browser. It
asks the running daemon's ``GET /whoami`` for the pairing link's base and
for what the access token grants, then prints the link as a QR code, as
text, or as the pairing JSON.

Like the MCP bridge, this is a standard-library client: it takes that
bridge's token flags, TLS flags, and transport from
:mod:`cronstable.webclient` and never imports aiohttp or the scheduler.
:mod:`cronstable.pairprobe` finds the address that the code names, for
this command and for the terminal dashboard.
"""

import argparse
import json
import shutil
import sys
from typing import Any

from cronstable import _cliargs, pairlink, pairprobe, platform, qr, webclient

# Seconds allowed for each request to the daemon.
_TIMEOUT = 10.0
# The most bytes of a reply's body that the command reads.
_BODY_LIMIT = 1 << 20


class _PairError(Exception):
    """A failure worth one line on stderr and exit status 1."""


def _get(
    base: str,
    path: str,
    token: str | None,
    opener: Any,
    timeout: float,
    limit: int = _BODY_LIMIT,
) -> tuple[int, Any]:
    """GET ``base + path``; return ``(status, parsed JSON)``.

    The JSON is ``None`` when the body is not JSON, when it nests deeper
    than the parser recurses, or when it is longer than ``limit`` bytes.
    """
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    status, _headers, body = webclient.send(
        base + path,
        opener,
        timeout,
        "the cronstable server at {}".format(base),
        path,
        headers=headers,
        limit=limit,
    )
    try:
        parsed = json.loads(body)
    except (ValueError, RecursionError):
        parsed = None
    return status, parsed


def _whoami(base: str, token: str | None, opener: Any) -> dict[str, Any]:
    """The daemon's description of this connection's credential."""
    status, body = _get(base, "/whoami", token, opener, _TIMEOUT)
    if status == 401:
        if token:
            raise _PairError(
                "the server at {} rejected the access token".format(base)
            )
        raise _PairError(
            "the server at {} requires an access token; pass --token-env "
            "VAR or set {}".format(base, _cliargs.WEB_ENV_TOKEN)
        )
    if status != 200 or not isinstance(body, dict):
        raise _PairError(
            "{}/whoami answered HTTP {} without the expected JSON; check "
            "that --url names a cronstable server".format(base, status)
        )
    return body


def _cluster(base: str, token: str | None, opener: Any) -> Any:
    """The ``GET /cluster`` reply, or ``None`` when it is unavailable."""
    try:
        status, body = _get(base, "/cluster", token, opener, _TIMEOUT)
    except webclient.ClientError:
        return None
    return body if status == 200 else None


def _emit(text: str) -> None:
    """Write ``text`` to stdout as UTF-8.

    A pipe on Windows defaults to the ANSI code page, which has no block
    characters.
    """
    out = sys.stdout
    # The text written so far stays ahead of the code.
    out.flush()
    utf8 = webclient.utf8_stream(out)
    try:
        utf8.write(text)
    finally:
        webclient.release_stream(utf8, out)
    # A warning on stderr then follows the output.
    out.flush()


def _code(pairing: pairlink.Pairing) -> tuple[str, str | None]:
    """The caption and the QR code for ``pairing``, ready to print, and a
    warning to print after them when the terminal is too small.

    The code follows the caption, so it is what stays on screen in a short
    window. In a terminal too small for it, the code takes its narrowest
    margin, which is the size that the warning names.
    """
    try:
        matrix = qr.encode_for_screen(pairing.link)
    except ValueError as ex:
        raise _PairError(
            "the pairing link is too long for a QR code ({}); use "
            "--format link".format(ex)
        ) from None
    warning = None
    if sys.stdout.isatty():
        platform.enable_console_vt()
        size = shutil.get_terminal_size()
        # One line stays free for the shell prompt that follows the code.
        rows = qr.fit_half_blocks(matrix, size.columns, size.lines - 1)
        if rows is None:
            warning = "{} Then run the command again.".format(
                qr.too_small(matrix, size.columns, size.lines, frame=(0, 1))
            )
            rows = qr.half_block_rows(matrix, qr.QUIET_ZONES[-1])
    else:
        rows = qr.half_block_rows(matrix)
    caption = [
        "Pairing code for {}".format(
            pairlink.label(pairing.name, pairing.url)
        ),
        *pairing.hint,
        "",
    ]
    code = [qr.INK_ON_PAPER + row + qr.SGR_RESET for row in rows]
    return "\n".join(caption + code) + "\n", warning


def _pair(args: argparse.Namespace) -> int:
    try:
        base = pairlink.base_url(args.url)
        public = (
            pairlink.base_url(args.public_url)
            if args.public_url is not None
            else None
        )
    except ValueError as ex:
        raise _PairError(str(ex)) from None
    token = webclient.resolve_token(args)
    context = webclient.resolve_tls(args)
    opener = webclient.build_opener(context)

    whoami = _whoami(base, token, opener)
    lan_note = None
    if public is None:
        try:
            public = pairprobe.phone_url(base, whoami, context)
        except pairprobe.Unreachable as ex:
            raise _PairError("{} {}".format(ex, ex.advice("pair"))) from None
        lan_note = pairlink.lan_note(base, public)
    cluster = None if args.name else _cluster(base, token, opener)
    try:
        pairing = pairlink.pairing(whoami, public, token, args.name, cluster)
    except ValueError as ex:
        raise _PairError(str(ex)) from None

    if lan_note:
        print(
            "{} Pass --public-url when the phone reaches the server at "
            "another address.".format(lan_note),
            file=sys.stderr,
        )
    for _short, note in pairing.notes:
        print("warning: {}".format(note), file=sys.stderr)
    if args.format == "json":
        _emit(pairing.payload + "\n")
    elif args.format == "link":
        _emit(pairing.link + "\n")
    else:
        text, warning = _code(pairing)
        _emit(text)
        if warning:
            # After the code, so it is the last thing on screen.
            print("warning: {}".format(warning), file=sys.stderr)
    return 0


def dispatch(args: argparse.Namespace) -> int:
    """Run ``cronstable pair``; returns a process exit code."""
    try:
        return _pair(args)
    except (_PairError, webclient.ClientError) as ex:
        print("cronstable pair: {}".format(ex), file=sys.stderr)
        return 1
