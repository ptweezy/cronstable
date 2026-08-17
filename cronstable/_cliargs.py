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
level for free.  mcpcli and tui re-export their registration function and
constants under the original public names, so ``from cronstable.tui
import add_tui_command`` (and every test that reaches these through the
surface modules) keeps working; jobcli imports nothing from here, its
parsers are registered directly by ``cronstable.__main__``.
"""

import argparse
from typing import Any

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


def _add_get_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("key")
    _add_scope_flags(parser)


def _add_set_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("key")
    parser.add_argument("value")
    parser.add_argument(
        "--json",
        action="store_true",
        help="parse VALUE as JSON instead of storing it as a string",
    )
    _add_scope_flags(parser)


def _add_delete_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("key")
    _add_scope_flags(parser)


def _add_keys_args(parser: argparse.ArgumentParser) -> None:
    _add_scope_flags(parser)


# One row per job-facing `state` verb: name, subcommand help, and the
# function that declares the verb's arguments. add_state_job_actions
# registers the rows in order, and STATE_JOB_ACTIONS is derived from the
# same rows, so a verb added here is routed to jobcli by __main__
# automatically; there is no second list to keep in step.
_STATE_JOB_ACTION_TABLE = (
    ("get", "print a durable KV value", _add_get_args),
    ("set", "set a durable KV value", _add_set_args),
    ("delete", "delete a durable KV value", _add_delete_args),
    ("keys", "list the keys in a scope", _add_keys_args),
)

# Every job-facing `state` action name, so __main__ can tell a `state get`
# (cronstable.jobcli) from a `state backup` (cronstable.state_admin) without
# importing either.
STATE_JOB_ACTIONS = frozenset(name for name, _, _ in _STATE_JOB_ACTION_TABLE)


def add_state_job_actions(actions: Any) -> None:
    """Add the job-facing KV actions to the existing `state` subparser.

    Coexists with cronstable.state_admin's backup/restore/gc/... actions under
    the same ``cronstable state`` command; the action name disambiguates.
    """
    for name, help_text, add_args in _STATE_JOB_ACTION_TABLE:
        add_args(actions.add_parser(name, help=help_text))


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


def _add_web_client_flags(
    parser: argparse.ArgumentParser,
    *,
    url_help: str,
    token_env_default: str | None = None,
) -> None:
    """Declare the connection flags every web-listener client takes.

    The `mcp` and `tui` subcommands accept the same seven flags with the
    same dests, actions and defaults, so both surfaces' _resolve_token /
    _resolve_tls plumbing sees one shape. Only the --url help (each
    surface names its own endpoint) and the --token-env declaration
    default vary per caller; the latter is cosmetic, both surfaces fall
    back to WEB_ENV_TOKEN at runtime either way.
    """
    parser.add_argument(
        "--url",
        default=WEB_DEFAULT_URL,
        metavar="URL",
        help=url_help,
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
        default=token_env_default,
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
        action="store_true",
        help="skip TLS verification entirely; the bearer token is still sent, "
        "so it goes to whoever answers (set {}=1 for the same)".format(
            WEB_ENV_INSECURE
        ),
    )


def add_mcp_command(sub: Any) -> None:
    """Register the ``cronstable mcp`` subcommand on the subparsers."""
    parser = sub.add_parser(
        "mcp",
        help="run the MCP stdio bridge to a running daemon's /mcp endpoint "
        "(for desktop MCP clients)",
    )
    _add_web_client_flags(
        parser,
        url_help="daemon web base URL serving /mcp (default: %(default)s)",
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


#: The service the SCM knows cronstable by when nothing else is said.  Also
#: the display name's stem and the key `sc query cronstable` answers to.
SERVICE_NAME_DEFAULT = "cronstable"


def _add_service_config_flag(parser: argparse.ArgumentParser) -> None:
    """Give a `service` action its own -c/--config.

    ``SUPPRESS``, not a real default, for the reason written at
    ``cronstable.__main__._add_state_subcommands``: argparse applies a
    subparser's defaults AFTER the root parse, so a concrete default here
    would overwrite a root-level ``cronstable -c X service install``.  The
    root parser already supplies the default.
    """
    parser.add_argument(
        "-c",
        "--config",
        default=argparse.SUPPRESS,
        metavar="FILE-OR-DIR",
        help="configuration the installed service will read",
    )


def _add_service_log_flags(parser: argparse.ArgumentParser) -> None:
    """The bootstrap-log flags `run` needs and `install` bakes in."""
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="bootstrap log file (default: a logs/ directory beside the "
        "configuration). A service has no console, so without this there "
        "is nowhere for a startup failure to be reported",
    )
    parser.add_argument(
        "--no-log-file",
        default=False,
        action="store_true",
        help="do not open a bootstrap log; use when the configuration's "
        "own `logging:` section is the only log you want",
    )
    parser.add_argument(
        "--console",
        default=False,
        action="store_true",
        help="allocate a console for the service, so a job kill can send "
        "the trappable CTRL_BREAK step that killTimeout bounds "
        "(off by default; see the Windows Service documentation)",
    )


def add_import_taskscheduler_command(sub: Any) -> None:
    """Register ``cronstable import-taskscheduler``.

    A flat verb rather than ``import <format>``: a nested subparser would
    buy a second dest, another --help level and a "no format given" error
    path today, against a compatibility break that only exists if a second
    format ever ships. A future importer is a sibling verb, which costs
    nothing either way.

    Declared in this leaf rather than beside `init` in __main__ because,
    unlike `init`, its implementation is a separate module that pulls in
    the XML parser, and that is exactly the distinction this module exists
    to draw.
    """
    parser = sub.add_parser(
        "import-taskscheduler",
        help="convert Windows Task Scheduler XML exports into cronstable "
        "jobs and exit",
        description=(
            "Convert one or more Task Scheduler exports (schtasks /query "
            "/XML ONE, or Export-ScheduledTask) into cronstable YAML. The "
            "converted configuration goes to stdout or -o; a report of "
            "everything that could not be carried across goes to stderr. "
            "Review the result before loading it: exporting a task does "
            "not unregister it."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help="export files, directories of *.xml, or - for stdin",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        metavar="FILE",
        help="write the configuration here instead of stdout",
    )
    parser.add_argument(
        "--timezone",
        default=None,
        metavar="NAME",
        help="evaluate every converted schedule in this IANA timezone "
        "(default: keep each task's own clock, which for a task with no "
        "stored offset is the daemon host's local time)",
    )


def add_service_command(sub: Any) -> None:
    """Register ``cronstable service <action>`` on the root subparsers.

    Declared here, in the stdlib-only leaf, for the reason this module
    exists at all: ``cronstable.winservice`` pulls in ctypes, the Win32
    surface and (in the ``run`` branch) the entire scheduler graph, and
    every invocation of the program builds this parser first.  The real
    module is imported only inside its dispatch branch.
    """
    parser = sub.add_parser(
        "service",
        help="install, remove or control cronstable as a Windows service "
        "(Windows only)",
        description=(
            "Run cronstable as a Windows service, so it starts at boot and "
            "keeps running whether or not anyone is logged on. `install` "
            "registers it with the Service Control Manager and needs an "
            "elevated prompt; `run` is what the SCM itself invokes and is "
            "not meant to be typed."
        ),
    )
    actions = parser.add_subparsers(dest="service_command", metavar="ACTION")

    def _named(action_parser):
        action_parser.add_argument(
            "--name",
            default=SERVICE_NAME_DEFAULT,
            metavar="NAME",
            help="service name, for running more than one instance on a "
            "host (default: %(default)s)",
        )
        return action_parser

    install = _named(
        actions.add_parser(
            "install", help="register the service (needs an elevated prompt)"
        )
    )
    _add_service_config_flag(install)
    _add_service_log_flags(install)
    install.add_argument(
        "--start-type",
        default="auto",
        choices=["auto", "delayed", "demand"],
        help="when Windows starts it: at boot, at boot after the other "
        "auto services, or only on request (default: %(default)s)",
    )
    install.add_argument(
        "--log-level",
        default="INFO",
        metavar="LEVEL",
        help="log level baked into the service's command line "
        "(default: %(default)s)",
    )
    install.add_argument(
        "--restart-delay",
        type=float,
        default=60.0,
        metavar="SECONDS",
        help="how long Windows waits before restarting the service after "
        "it fails (default: %(default)s)",
    )
    install.add_argument(
        "--no-restart",
        default=False,
        action="store_true",
        help="do not configure recovery actions, so a failed service "
        "stays stopped",
    )
    _named(actions.add_parser("remove", help="stop and unregister"))
    started = _named(actions.add_parser("start", help="start the service"))
    started.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="how long to wait for it to report running "
        "(default: %(default)s)",
    )
    stopped = _named(
        actions.add_parser(
            "stop",
            help="stop the service, waiting for running jobs to finish",
        )
    )
    stopped.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="how long to wait for the drain; 0 (the default) waits as "
        "long as the running jobs take",
    )
    _named(
        actions.add_parser(
            "reload",
            help="make the running service reload its configuration now, "
            "reparsing even when file stats are unchanged (what SIGHUP "
            "does on POSIX)",
        )
    )
    _named(
        actions.add_parser("status", help="print the service's state and pid")
    )
    run = _named(
        actions.add_parser(
            "run",
            help="the entry point the Service Control Manager invokes; "
            "not meant to be run by hand",
        )
    )
    _add_service_config_flag(run)
    _add_service_log_flags(run)


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
    _add_web_client_flags(
        parser,
        url_help="daemon web listener (default: %(default)s)",
        token_env_default=WEB_ENV_TOKEN,
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
