"""Argument parsers for subcommands whose implementations load on demand.

Every cronstable invocation builds the full parser, including ``--version``
and commands launched by jobs, such as ``state get``. Keeping parser
registration here avoids importing the terminal dashboard, MCP bridge, and
job client until their commands run. ``cronstable.__main__.main_loop``
imports each implementation in its dispatch branch.
``tests/test_cli_stubs.py`` verifies this behavior.

This module imports only the standard library and has no dependencies on
other cronstable modules. The ``mcpcli`` and ``tui`` modules re-export their
registration functions and constants under their existing public names.
``cronstable.__main__`` registers the job client parsers directly.
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
# cycling); each also has a -light variant, appended below. The default
# hue leads the list, and both cyclers walk it in this order.
DEFAULT_THEME_HUE = "standard"
THEME_HUES = ["standard", "carolina", "amber", "green", "modern"]


def _add_scope_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--scope",
        metavar="NAME",
        help="namespace for this operation (default: this job's name)",
    )
    group.add_argument(
        "--global",
        dest="use_global",
        action="store_true",
        help="use the shared global scope to coordinate across jobs",
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
    ("get", "print a saved value by key", _add_get_args),
    ("set", "save a value by key", _add_set_args),
    ("delete", "delete a saved value by key", _add_delete_args),
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
        "cursor", help="read or advance a saved processing position (cursor)"
    )
    cursor_actions = cursor.add_subparsers(
        dest="cursor_command", metavar="ACTION"
    )
    cget = cursor_actions.add_parser("get", help="print a cursor's value")
    cget.add_argument("name")
    _add_scope_flags(cget)
    cadv = cursor_actions.add_parser(
        "advance", help="move a cursor forward (use --force to move backward)"
    )
    cadv.add_argument("name")
    cadv.add_argument("value")
    cadv.add_argument(
        "--force",
        action="store_true",
        help="set the value even if it moves the cursor backward",
    )
    _add_scope_flags(cadv)

    # lock
    lock = sub.add_parser(
        "lock",
        help="coordinate concurrent work across nodes with a shared lock",
    )
    lock_actions = lock.add_subparsers(dest="lock_command", metavar="ACTION")
    for verb, help_text in (
        ("acquire", "acquire the lock and print its token"),
        ("run", "hold the lock while running a command"),
    ):
        p = lock_actions.add_parser(verb, help=help_text)
        p.add_argument("name")
        p.add_argument(
            "--permits",
            type=int,
            default=1,
            help="maximum simultaneous lock holders (default: 1)",
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
            help="maximum wait time with --wait, in seconds",
        )
        p.add_argument(
            "--ttl",
            type=float,
            default=None,
            metavar="SECONDS",
            help="lease duration in seconds "
            "(default: state.jobApi.lockTtlSeconds)",
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
        "artifact", help="save or retrieve a named artifact"
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
        "get", help="write an artifact to stdout or the file specified by -o"
    )
    aget.add_argument("name")
    aget.add_argument("-o", "--output", default=None, metavar="FILE")
    _add_scope_flags(aget)
    alist = art_actions.add_parser("list", help="list artifact names")
    _add_scope_flags(alist)

    # idempotent
    idem = sub.add_parser(
        "idempotent",
        help="claim a key once across nodes (exit 0 for a new claim, "
        "5 for an existing claim, or 1 on error)",
    )
    idem.add_argument("key")
    idem.add_argument(
        "--ttl",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="expire the claim after SECONDS (0 = permanent)",
    )
    idem.add_argument(
        "--release",
        action="store_true",
        help="release the claim instead of creating it",
    )
    _add_scope_flags(idem)

    # xcom: cross-task data hand-off within a dag_run
    xcom = sub.add_parser(
        "xcom",
        help="share task output (XCom) within a workflow run",
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
        "--task", required=True, metavar="TASK", help="the upstream task ID"
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
        "secret", help="read a secret available to the current run"
    )
    secret_actions = secret.add_subparsers(
        dest="secret_command", metavar="ACTION"
    )
    sget = secret_actions.add_parser("get", help="print a secret's value")
    sget.add_argument("name")
    secret_actions.add_parser(
        "list", help="list secrets available to the current run"
    )


def _add_web_client_flags(
    parser: argparse.ArgumentParser,
    *,
    url_help: str,
    token_env_default: str | None = None,
) -> None:
    """Add shared connection flags for the MCP and TUI clients.

    Both clients use the same destinations, actions, and runtime defaults.
    Only the URL help and the displayed ``--token-env`` default vary.
    Both clients fall back to ``WEB_ENV_TOKEN`` at runtime.
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
        help="bearer token for the web API (use --token-env to keep it out "
        "of the process list)",
    )
    parser.add_argument(
        "--token-env",
        default=token_env_default,
        metavar="VAR",
        help="environment variable containing the bearer token "
        "(default: {} if set)".format(WEB_ENV_TOKEN),
    )
    parser.add_argument(
        "--cacert",
        default=None,
        metavar="PATH",
        help="verify the server certificate with this CA file instead of "
        "the system trust store "
        "(default: {} if set)".format(WEB_ENV_CACERT),
    )
    parser.add_argument(
        "--client-cert",
        default=None,
        metavar="PATH",
        help="client certificate for a listener that requires one through "
        "web.tls.clientCa (default: {} if set)".format(WEB_ENV_CLIENT_CERT),
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
        help="disable TLS certificate verification; this can expose the "
        "bearer token to an untrusted server (equivalent to {}=1)".format(
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
        url_help="cronstable server URL serving /mcp (default: %(default)s)",
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
        help="check the connection with initialize and tools/list, then exit",
    )


#: Default Service Control Manager name and base display name.
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
    """Add startup logging flags for service installation and execution."""
    parser.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="file for startup logs (default: in a logs/ directory beside "
        "the configuration); records startup failures when the service "
        "has no console",
    )
    parser.add_argument(
        "--no-log-file",
        default=False,
        action="store_true",
        help="disable the startup log file and use only the logging section "
        "in the configuration",
    )
    parser.add_argument(
        "--console",
        default=False,
        action="store_true",
        help="allocate a console so jobs can handle CTRL_BREAK before "
        "killTimeout expires (off by default; see the Windows service "
        "documentation)",
    )


def add_import_taskscheduler_command(sub: Any) -> None:
    """Register ``cronstable import-taskscheduler``.

    Keep registration here so unrelated commands do not import the XML
    parser. A separate top-level command avoids an extra subparser for
    the single supported import format.
    """
    parser = sub.add_parser(
        "import-taskscheduler",
        help="convert Windows Task Scheduler XML exports into cronstable "
        "jobs and exit",
        description=(
            "Convert one or more Task Scheduler exports (schtasks /query "
            "/XML ONE, or Export-ScheduledTask) into cronstable YAML. The "
            "converted configuration goes to stdout or the file specified "
            "by -o. Unsupported features are reported on stderr. "
            "Review the result before loading it: exported tasks remain "
            "registered in Task Scheduler."
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
        help="write the configuration to FILE instead of stdout",
    )
    parser.add_argument(
        "--timezone",
        default=None,
        metavar="NAME",
        help="evaluate every converted schedule in this IANA time zone "
        "(default: preserve each task's offset, or use the daemon host's "
        "local time if the task has no stored offset)",
    )


def add_service_command(sub: Any) -> None:
    """Register ``cronstable service <action>`` on the root subparsers.

    Keep registration here so unrelated commands do not import ctypes,
    the Windows APIs, or the scheduler through ``cronstable.winservice``.
    """
    parser = sub.add_parser(
        "service",
        help="install, remove, or control cronstable as a Windows service "
        "(Windows only)",
        description=(
            "Run cronstable as a Windows service. By default, it starts at "
            "boot and runs even when no user is signed in. Run install "
            "from an elevated command prompt to register the service. "
            "The Service Control Manager invokes run; do not invoke it "
            "manually."
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
        help="log level saved in the service's command line "
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
        help="time to wait for running jobs to finish, in seconds; "
        "0 (the default) waits without a timeout",
    )
    _named(
        actions.add_parser(
            "reload",
            help="reload the configuration even if file metadata is unchanged "
            "(equivalent to SIGHUP on POSIX)",
        )
    )
    _named(
        actions.add_parser("status", help="print the service's state and PID")
    )
    run = _named(
        actions.add_parser(
            "run",
            help="the entry point the Service Control Manager invokes; "
            "do not invoke manually",
        )
    )
    _add_service_config_flag(run)
    _add_service_log_flags(run)


def add_tui_command(sub: Any) -> None:
    """Attach the ``tui`` subcommand to the root parser's subparsers."""
    parser = sub.add_parser(
        "tui",
        help=("open the terminal dashboard for a running cronstable server"),
        description=(
            "Manage cronstable from your terminal. Use the same shortcuts "
            "as the web dashboard: j/k move, Enter opens a job, r "
            "runs it, x cancels, / filters, Ctrl-K opens the command "
            "palette, ? lists every key."
        ),
    )
    _add_web_client_flags(
        parser,
        url_help="cronstable server URL (default: %(default)s)",
        token_env_default=WEB_ENV_TOKEN,
    )
    parser.add_argument(
        "--theme",
        default=None,
        choices=list(THEME_HUES) + [h + "-light" for h in THEME_HUES],
        help="select a theme and save it for future sessions",
    )
    parser.add_argument(
        "--tv",
        action="store_true",
        help="open in wallboard mode",
    )
    parser.add_argument(
        "--job",
        default=None,
        metavar="NAME",
        help="open a job's details at startup",
    )
    parser.add_argument(
        "--boot",
        action="store_true",
        help="show startup checks even if they ran recently",
    )
    parser.add_argument(
        "--no-boot",
        action="store_true",
        help="skip startup checks",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="use ASCII status symbols for terminals with limited fonts",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=None,
        metavar="SECONDS",
        help="refresh interval in seconds; 0 pauses (default: saved value, "
        "or 3 if none is saved)",
    )
