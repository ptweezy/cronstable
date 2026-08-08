"""argparse wiring for the subcommands whose implementations must stay lazy.

One `cronstable` entry point fronts several heavyweight surfaces: the
job-facing state verbs (:mod:`cronstable.jobcli`), the MCP stdio bridge
(:mod:`cronstable.mcpcli`) and the terminal dashboard (:mod:`cronstable.tui`).
Every invocation builds the full parser first (the daemon, ``--version``, and
each job-spawned thin client such as ``state get`` or ``lock``), so the
parser definitions cannot live in those modules: importing tui alone runs its
~7000-line module body and pulls unicodedata's C table plus dozens of other
modules (~50ms), and jobcli drags urllib.request/ssl/email in for ~27ms.
The definitions live here instead; the real modules are imported only inside
their dispatch branches (see ``cronstable.__main__.main_loop``), and
tests/test_cli_stubs.py pins that lazy-import property.

Like :mod:`cronstable.tlsutil`, this module is a deliberate leaf: it imports
nothing from ``cronstable`` and nothing outside the standard library, so
``cronstable.__main__`` and each surface module can import it at module
level for free.  Each surface re-exports its registration function and
constants under their original public names, so ``from cronstable.tui
import add_tui_command`` (and every test that reaches these through the
surface modules) keeps working.
"""

import argparse
from typing import Any

# Every job-facing `state` action name, so __main__ can tell a `state get`
# (cronstable.jobcli) from a `state backup` (cronstable.state_admin) without
# importing either.
STATE_JOB_ACTIONS = frozenset({"get", "set", "delete", "keys"})

# Client-side conventions shared with the web dashboard: the default listener
# URL, the bearer-token env var cronstable's own docs use, and the env
# fallbacks for the client TLS flags. Every cronstable client that speaks to
# a web listener uses these same names, so one exported set of variables
# serves the TUI, the MCP bridge and the thin CLIs at once; they are the
# web-listener counterparts of the CRONSTABLE_STATE_* variables the daemon
# injects into a job.
WEB_DEFAULT_URL = "http://127.0.0.1:8080"
WEB_ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"
WEB_ENV_CACERT = "CRONSTABLE_WEB_CACERT"
WEB_ENV_CLIENT_CERT = "CRONSTABLE_WEB_CLIENT_CERT"
WEB_ENV_CLIENT_KEY = "CRONSTABLE_WEB_CLIENT_KEY"
WEB_ENV_INSECURE = "CRONSTABLE_WEB_INSECURE"

# Hardcoded, NOT imported from cronstable.mcp: importing that module would
# pull aiohttp and the daemon graph into the featherweight bridge CLI. This
# is only the wire default sent before initialize completes; the real
# negotiated version is learned from the initialize reply and used
# thereafter.
MCP_DEFAULT_PROTOCOL_VERSION = "2025-11-25"
MCP_DEFAULT_TIMEOUT = 30.0

# The web dashboard's five theme hues, mirrored by the TUI (same t / T
# cycling); each also has a -light variant, appended below.
THEME_HUES = ["carolina", "amber", "green", "modern", "standard"]


def _add_scope_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--scope",
        metavar="NAME",
        help="the namespace to act in (default: this job's own name)",
    )
    group.add_argument(
        "--global",
        dest="use_global",
        action="store_true",
        help="act in the shared `global` scope (cross-job coordination)",
    )


def add_state_job_actions(actions: Any) -> None:
    """Add the job-facing KV actions to the existing `state` subparser.

    Coexists with cronstable.state_admin's backup/restore/gc/... actions under
    the same ``cronstable state`` command; the action name disambiguates.
    """
    get = actions.add_parser("get", help="print a durable KV value")
    get.add_argument("key")
    _add_scope_flags(get)

    setp = actions.add_parser("set", help="set a durable KV value")
    setp.add_argument("key")
    setp.add_argument("value")
    setp.add_argument(
        "--json",
        action="store_true",
        help="parse VALUE as JSON instead of storing it as a string",
    )
    _add_scope_flags(setp)

    delete = actions.add_parser("delete", help="delete a durable KV value")
    delete.add_argument("key")
    _add_scope_flags(delete)

    keys = actions.add_parser("keys", help="list the keys in a scope")
    _add_scope_flags(keys)


def add_job_commands(sub: Any) -> None:
    """Add the top-level `cursor|lock|artifact|idempotent|secret` commands."""
    # cursor
    cursor = sub.add_parser(
        "cursor", help="read or advance a monotonic ETL cursor/watermark"
    )
    cursor_actions = cursor.add_subparsers(
        dest="cursor_command", metavar="ACTION"
    )
    cget = cursor_actions.add_parser("get", help="print a cursor's value")
    cget.add_argument("name")
    _add_scope_flags(cget)
    cadv = cursor_actions.add_parser(
        "advance", help="advance a cursor (monotonic unless --force)"
    )
    cadv.add_argument("name")
    cadv.add_argument("value")
    cadv.add_argument(
        "--force",
        action="store_true",
        help="set the value even if it moves the cursor backwards",
    )
    _add_scope_flags(cadv)

    # lock
    lock = sub.add_parser(
        "lock", help="a fleet-wide distributed mutex or semaphore"
    )
    lock_actions = lock.add_subparsers(dest="lock_command", metavar="ACTION")
    for verb, help_text in (
        ("acquire", "take the lock; print its hold token"),
        ("run", "hold the lock while running a command"),
    ):
        p = lock_actions.add_parser(verb, help=help_text)
        p.add_argument("name")
        p.add_argument(
            "--permits",
            type=int,
            default=1,
            help="semaphore capacity (default 1 = a mutex)",
        )
        p.add_argument(
            "--wait",
            action="store_true",
            help="block until the lock is free (up to --timeout)",
        )
        p.add_argument(
            "--timeout",
            type=float,
            default=0.0,
            metavar="SECONDS",
            help="how long --wait blocks before giving up",
        )
        p.add_argument(
            "--ttl",
            type=float,
            default=None,
            metavar="SECONDS",
            help="lease TTL (default: state.jobApi.lockTtlSeconds)",
        )
        _add_scope_flags(p)
        if verb == "run":
            # NOT dest "command": the root subparsers already store the
            # command name (state/lock/...) under args.command, and a same-
            # named REMAINDER here would clobber it and misroute the whole
            # invocation.
            p.add_argument(
                "run_command",
                # The command after "--" is split off BEFORE argparse, in
                # __main__.main_loop (portable across Python versions; see
                # the note there -- argparse's own "--"/trailing handling is
                # inconsistent before 3.13, and REMAINDER would swallow our
                # own --wait/--timeout/--ttl). This positional only holds the
                # default [] and a command given WITHOUT a "--" separator.
                nargs="*",
                metavar="command",
                help="the command to run while holding the lock (after --)",
            )
    lrel = lock_actions.add_parser("release", help="release a held lock")
    lrel.add_argument("token")

    # artifact
    artifact = sub.add_parser(
        "artifact", help="publish or fetch a named artifact blob"
    )
    art_actions = artifact.add_subparsers(
        dest="artifact_command", metavar="ACTION"
    )
    aput = art_actions.add_parser(
        "put", help="publish an artifact (from FILE or stdin)"
    )
    aput.add_argument("name")
    aput.add_argument("file", nargs="?", default=None)
    _add_scope_flags(aput)
    aget = art_actions.add_parser(
        "get", help="fetch an artifact (to -o FILE or stdout)"
    )
    aget.add_argument("name")
    aget.add_argument("-o", "--output", default=None, metavar="FILE")
    _add_scope_flags(aget)
    alist = art_actions.add_parser("list", help="list artifact names")
    _add_scope_flags(alist)

    # idempotent
    idem = sub.add_parser(
        "idempotent",
        help="claim a key once fleet-wide (exit 0 fresh, 5 duplicate, "
        "1 error)",
    )
    idem.add_argument("key")
    idem.add_argument(
        "--ttl",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="expire the claim after N seconds (0 = permanent)",
    )
    idem.add_argument(
        "--release",
        action="store_true",
        help="drop the claim instead of making it",
    )
    _add_scope_flags(idem)

    # xcom: cross-task data hand-off within a dag_run
    xcom = sub.add_parser(
        "xcom",
        help="publish or read a DAG task output (XCom) within a dag_run",
    )
    xcom_actions = xcom.add_subparsers(dest="xcom_command", metavar="ACTION")
    xpush = xcom_actions.add_parser(
        "push", help="publish this task's output under a key (FILE or stdin)"
    )
    xpush.add_argument("--key", required=True, help="the XCom key to publish")
    xpush.add_argument("file", nargs="?", default=None)
    xpull = xcom_actions.add_parser(
        "pull", help="read an upstream task's output by key"
    )
    xpull.add_argument(
        "--task", required=True, metavar="TASK", help="the upstream task id"
    )
    xpull.add_argument("--key", required=True, help="the XCom key to read")
    xpull.add_argument(
        "--map-index",
        type=int,
        default=None,
        metavar="I",
        help="read a specific mapped instance of the upstream task",
    )
    xpull.add_argument("-o", "--output", default=None, metavar="FILE")
    xcom_actions.add_parser("list", help="list XCom keys in this run")

    # secret
    secret = sub.add_parser(
        "secret", help="read a run-scoped secret staged for this run"
    )
    secret_actions = secret.add_subparsers(
        dest="secret_command", metavar="ACTION"
    )
    sget = secret_actions.add_parser("get", help="print a secret's value")
    sget.add_argument("name")
    secret_actions.add_parser("list", help="list staged secret names")


def add_mcp_command(sub: Any) -> None:
    """Register the ``cronstable mcp`` subcommand on the subparsers."""
    parser = sub.add_parser(
        "mcp",
        help="run the MCP stdio bridge to a running daemon's /mcp endpoint "
        "(for desktop MCP clients)",
    )
    parser.add_argument(
        "--url",
        default=WEB_DEFAULT_URL,
        metavar="URL",
        help="daemon web base URL serving /mcp (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        metavar="TOKEN",
        help="web.authToken bearer value (prefer --token-env to keep it out "
        "of the process table)",
    )
    parser.add_argument(
        "--token-env",
        default=None,
        metavar="VAR",
        help="env var holding the bearer token (default: {} if set)".format(
            WEB_ENV_TOKEN
        ),
    )
    parser.add_argument(
        "--cacert",
        default=None,
        metavar="PATH",
        help="verify the listener against this CA file instead of the system "
        "trust store, for an internally-issued or self-signed certificate "
        "(default: {} if set)".format(WEB_ENV_CACERT),
    )
    parser.add_argument(
        "--client-cert",
        default=None,
        metavar="PATH",
        help="client certificate to present to a listener configured with "
        "web.tls.clientCa, which requires one (default: {} if set)".format(
            WEB_ENV_CLIENT_CERT
        ),
    )
    parser.add_argument(
        "--client-key",
        default=None,
        metavar="PATH",
        help="private key for --client-cert (default: {} if set)".format(
            WEB_ENV_CLIENT_KEY
        ),
    )
    parser.add_argument(
        "--insecure",
        default=False,
        action="store_true",
        help="skip TLS verification entirely; the bearer token is still sent, "
        "so it goes to whoever answers (set {}=1 for the same)".format(
            WEB_ENV_INSECURE
        ),
    )
    parser.add_argument(
        "--protocol-version",
        default=None,
        metavar="REV",
        help="pin the MCP-Protocol-Version sent before initialize "
        "(default: {})".format(MCP_DEFAULT_PROTOCOL_VERSION),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=MCP_DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help="per-request deadline (default: %(default)s)",
    )
    parser.add_argument(
        "--check",
        dest="mcp_check",
        default=False,
        action="store_true",
        help="handshake the endpoint (initialize + tools/list) and exit, "
        "instead of proxying stdin",
    )


def add_tui_command(sub: Any) -> None:
    """Attach the ``tui`` subcommand to the root parser's subparsers."""
    parser = sub.add_parser(
        "tui",
        help=(
            "open the terminal dashboard (the web dashboard's TUI "
            "sibling) against a running daemon's web listener"
        ),
        description=(
            "A keyboard-driven terminal rendition of the cronstable web "
            "dashboard, speaking the same HTTP control API. The web "
            "page's shortcuts apply: j/k move, Enter opens a job, r "
            "runs it, x cancels, / filters, Ctrl-K opens the command "
            "palette, ? lists every key."
        ),
    )
    parser.add_argument(
        "--url",
        default=WEB_DEFAULT_URL,
        help="daemon web listener (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="bearer token for web.authToken-protected daemons",
    )
    parser.add_argument(
        "--token-env",
        default=WEB_ENV_TOKEN,
        metavar="VAR",
        help=(
            "environment variable to read the token from when --token "
            "is not given (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--cacert",
        default=None,
        metavar="PATH",
        help=(
            "verify an https:// listener against this CA file instead "
            "of the system trust store (env: %s)" % WEB_ENV_CACERT
        ),
    )
    parser.add_argument(
        "--client-cert",
        default=None,
        metavar="PATH",
        help=(
            "certificate to present to a listener that requires one "
            "(web.tls.clientCa is set) (env: %s)" % WEB_ENV_CLIENT_CERT
        ),
    )
    parser.add_argument(
        "--client-key",
        default=None,
        metavar="PATH",
        help="private key for --client-cert (env: %s)" % WEB_ENV_CLIENT_KEY,
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "skip certificate verification; the token is still sent, so "
            "it reaches whoever answers (env: %s)" % WEB_ENV_INSECURE
        ),
    )
    parser.add_argument(
        "--theme",
        default=None,
        choices=list(THEME_HUES) + [h + "-light" for h in THEME_HUES],
        help="start on a specific theme (persisted for next time)",
    )
    parser.add_argument(
        "--tv",
        action="store_true",
        help="start straight on the wallboard (the page's #tv)",
    )
    parser.add_argument(
        "--job",
        default=None,
        metavar="NAME",
        help="open a job's drawer at startup (the page's #job/NAME)",
    )
    parser.add_argument(
        "--boot",
        action="store_true",
        help="force the boot self-test even if one ran recently",
    )
    parser.add_argument(
        "--no-boot",
        action="store_true",
        help="skip the boot self-test",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="plain-ASCII status glyphs (limited fonts/terminals)",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=None,
        metavar="SECONDS",
        help="refresh interval; 0 pauses (default: remembered, else 3)",
    )
