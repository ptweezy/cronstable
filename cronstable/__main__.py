import argparse
import logging
import os
import sys
from typing import Any

import cronstable.version
from cronstable import platform

# Where -c looks when not given: /etc/cronstable.d on POSIX,
# %APPDATA%\cronstable on
# Windows (see cronstable.platform).
CONFIG_DEFAULT = platform.DEFAULT_CONFIG_PATH


def _add_state_subcommands(parser: argparse.ArgumentParser) -> None:
    """Wire the `cronstable state <action>` administration subcommands.

    Bare `cronstable` (no subcommand) stays the daemon.  Each action accepts
    its own -c/--config (same dest and default as the daemon flag) so both
    `cronstable -c X state gc` and `cronstable state gc -c X` work.
    """
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    state = sub.add_parser(
        "state",
        help="administer the durable state store (backup/restore/migrate/"
        "gc/check/migrate-schema)",
    )
    actions = state.add_subparsers(dest="state_command", metavar="ACTION")

    def _with_config(sub_parser):
        # SUPPRESS, not CONFIG_DEFAULT: a subparser default would otherwise
        # OVERWRITE a root-level `cronstable -c X state ...` value (argparse
        # applies subparser defaults after the root parse). The root parser
        # already supplies the default.
        sub_parser.add_argument(
            "-c",
            "--config",
            default=argparse.SUPPRESS,
            metavar="FILE-OR-DIR",
            help="configuration with the `state:` section to administer",
        )
        return sub_parser

    backup = _with_config(
        actions.add_parser(
            "backup", help="write a .tar.gz backup of the store"
        )
    )
    backup.add_argument("-o", "--output", required=True, metavar="FILE.tar.gz")
    restore = _with_config(
        actions.add_parser("restore", help="restore a backup into the store")
    )
    restore.add_argument("archive", metavar="FILE.tar.gz")
    restore.add_argument(
        "--force",
        default=False,
        action="store_true",
        help="merge into a non-empty store (NOT safe while a daemon uses it)",
    )
    migrate = _with_config(
        actions.add_parser(
            "migrate",
            help="copy the store to another path or mount "
            "(local disk <-> S3 Files / EFS)",
        )
    )
    migrate.add_argument("--dest", required=True, metavar="PATH")
    migrate.add_argument(
        "--dest-deployment-id",
        default=None,
        metavar="ID",
        help="namespace at the destination (default: keep the current one)",
    )
    migrate.add_argument(
        "--force",
        default=False,
        action="store_true",
        help="overwrite a non-empty destination store",
    )
    gc = _with_config(
        actions.add_parser(
            "gc", help="garbage-collect state of unreferenced jobs"
        )
    )
    gc.add_argument("--dry-run", default=False, action="store_true")
    _with_config(
        actions.add_parser(
            "check", help="verify the store is usable and print an inventory"
        )
    )
    migrate_schema = _with_config(
        actions.add_parser(
            "migrate-schema",
            help="rewrite records of older known record schemes",
        )
    )
    migrate_schema.add_argument(
        "--dry-run", default=False, action="store_true"
    )

    # The job-facing state commands. The KV actions (get/set/delete/
    # keys) hang off the SAME `state` subparser as the admin actions above and
    # coexist with them (the action name routes); the other verbs (cursor/
    # lock/artifact/idempotent/secret) are their own top-level commands. Both
    # are thin clients of the daemon's loopback endpoint.
    #
    # `cronstable jobcli`, `mcp` and `tui` all register as bare stubs below,
    # NOT by importing cronstable.jobcli / cronstable.mcpcli / cronstable.tui.
    # Importing tui alone runs its ~7000-line module body and pulls
    # unicodedata's C table plus dozens of other modules (~50ms), and jobcli
    # drags urllib.request/ssl/email in for ~27ms; every job-spawned thin
    # client (`state get`, `lock`, `xcom pull`) builds this parser first, and
    # so does the daemon and `--version`, so an eager import taxed every
    # invocation for commands almost never the one invoked. The real modules
    # are imported only inside their dispatch branches (see main_loop).
    # A parity test (tests/test_cli_stubs.py) keeps the stub flags in lockstep
    # with the real add_state_job_actions / add_job_commands /
    # add_mcp_command / add_tui_command definitions.
    _add_jobcli_stub(actions, sub)
    _add_mcp_stub(sub)
    _add_tui_stub(sub)


def _add_scope_flags_stub(parser: Any) -> None:
    """Mirror of cronstable.jobcli._add_scope_flags."""
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


def _add_jobcli_stub(actions: Any, sub: Any) -> None:
    """Register the job-facing verbs without importing cronstable.jobcli.

    ``actions`` is the `cronstable state` action subparser (the KV verbs share
    it with the offline admin actions); ``sub`` is the root command subparser
    (cursor / lock / artifact / idempotent / xcom / secret).  Must stay
    flag-for-flag identical to jobcli.add_state_job_actions and
    jobcli.add_job_commands; the parity test enforces it.
    """
    _add_jobcli_state_stub(actions)
    _add_jobcli_cursor_stub(sub)
    _add_jobcli_lock_stub(sub)
    _add_jobcli_artifact_stub(sub)
    _add_jobcli_idempotent_stub(sub)
    _add_jobcli_xcom_stub(sub)
    _add_jobcli_secret_stub(sub)


def _add_jobcli_state_stub(actions: Any) -> None:
    """Mirror of cronstable.jobcli.add_state_job_actions."""
    get = actions.add_parser("get", help="print a durable KV value")
    get.add_argument("key")
    _add_scope_flags_stub(get)

    setp = actions.add_parser("set", help="set a durable KV value")
    setp.add_argument("key")
    setp.add_argument("value")
    setp.add_argument(
        "--json",
        action="store_true",
        help="parse VALUE as JSON instead of storing it as a string",
    )
    _add_scope_flags_stub(setp)

    delete = actions.add_parser("delete", help="delete a durable KV value")
    delete.add_argument("key")
    _add_scope_flags_stub(delete)

    keys = actions.add_parser("keys", help="list the keys in a scope")
    _add_scope_flags_stub(keys)


def _add_jobcli_cursor_stub(sub: Any) -> None:
    cursor = sub.add_parser(
        "cursor", help="read or advance a monotonic ETL cursor/watermark"
    )
    cursor_actions = cursor.add_subparsers(
        dest="cursor_command", metavar="ACTION"
    )
    cget = cursor_actions.add_parser("get", help="print a cursor's value")
    cget.add_argument("name")
    _add_scope_flags_stub(cget)
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
    _add_scope_flags_stub(cadv)


def _add_jobcli_lock_stub(sub: Any) -> None:
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
        _add_scope_flags_stub(p)
        if verb == "run":
            p.add_argument(
                "run_command",
                nargs="*",
                metavar="command",
                help="the command to run while holding the lock (after --)",
            )
    lrel = lock_actions.add_parser("release", help="release a held lock")
    lrel.add_argument("token")


def _add_jobcli_artifact_stub(sub: Any) -> None:
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
    _add_scope_flags_stub(aput)
    aget = art_actions.add_parser(
        "get", help="fetch an artifact (to -o FILE or stdout)"
    )
    aget.add_argument("name")
    aget.add_argument("-o", "--output", default=None, metavar="FILE")
    _add_scope_flags_stub(aget)
    alist = art_actions.add_parser("list", help="list artifact names")
    _add_scope_flags_stub(alist)


def _add_jobcli_idempotent_stub(sub: Any) -> None:
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
    _add_scope_flags_stub(idem)


def _add_jobcli_xcom_stub(sub: Any) -> None:
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


def _add_jobcli_secret_stub(sub: Any) -> None:
    secret = sub.add_parser(
        "secret", help="read a run-scoped secret staged for this run"
    )
    secret_actions = secret.add_subparsers(
        dest="secret_command", metavar="ACTION"
    )
    sget = secret_actions.add_parser("get", help="print a secret's value")
    sget.add_argument("name")
    secret_actions.add_parser("list", help="list staged secret names")


# Mirrors of cronstable.jobcli / cronstable.mcpcli / cronstable.tui module
# constants used only to register their subparsers or route to them. Kept here
# so building the CLI (and routing a `state` ADMIN action, which reaches
# state_admin, not jobcli) never imports those modules; the parity test asserts
# these match the originals.
_JOBCLI_STATE_ACTIONS = frozenset({"get", "set", "delete", "keys"})
_MCP_DEFAULT_URL = "http://127.0.0.1:8080"
_MCP_ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"
_MCP_ENV_CACERT = "CRONSTABLE_WEB_CACERT"
_MCP_ENV_CLIENT_CERT = "CRONSTABLE_WEB_CLIENT_CERT"
_MCP_ENV_CLIENT_KEY = "CRONSTABLE_WEB_CLIENT_KEY"
_MCP_ENV_INSECURE = "CRONSTABLE_WEB_INSECURE"
_MCP_DEFAULT_PROTOCOL_VERSION = "2025-11-25"
_MCP_DEFAULT_TIMEOUT = 30.0
_TUI_DEFAULT_URL = "http://127.0.0.1:8080"
_TUI_ENV_TOKEN = "CRONSTABLE_WEB_TOKEN"
_TUI_ENV_CACERT = "CRONSTABLE_WEB_CACERT"
_TUI_ENV_CLIENT_CERT = "CRONSTABLE_WEB_CLIENT_CERT"
_TUI_ENV_CLIENT_KEY = "CRONSTABLE_WEB_CLIENT_KEY"
_TUI_ENV_INSECURE = "CRONSTABLE_WEB_INSECURE"
_TUI_THEME_HUES = ["carolina", "amber", "green", "modern", "standard"]
#: Mirrors cronstable.tui.THEME_NAMES: every hue's tiers, hue-grouped and
#: darkest first.  Only carolina has a third (deep phosphor) tier.
_TUI_THEME_NAMES = [
    "carolina-dark",
    "carolina",
    "carolina-light",
    "amber",
    "amber-light",
    "green",
    "green-light",
    "modern",
    "modern-light",
    "standard",
    "standard-light",
]


def _add_mcp_stub(sub: Any) -> None:
    """Register `cronstable mcp` without importing cronstable.mcpcli.

    Must stay flag-for-flag identical to mcpcli.add_mcp_command; the parity
    test enforces it.
    """
    parser = sub.add_parser(
        "mcp",
        help="run the MCP stdio bridge to a running daemon's /mcp endpoint "
        "(for desktop MCP clients)",
    )
    parser.add_argument(
        "--url",
        default=_MCP_DEFAULT_URL,
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
            _MCP_ENV_TOKEN
        ),
    )
    parser.add_argument(
        "--cacert",
        default=None,
        metavar="PATH",
        help="verify the listener against this CA file instead of the system "
        "trust store, for an internally-issued or self-signed certificate "
        "(default: {} if set)".format(_MCP_ENV_CACERT),
    )
    parser.add_argument(
        "--client-cert",
        default=None,
        metavar="PATH",
        help="client certificate to present to a listener configured with "
        "web.tls.clientCa, which requires one (default: {} if set)".format(
            _MCP_ENV_CLIENT_CERT
        ),
    )
    parser.add_argument(
        "--client-key",
        default=None,
        metavar="PATH",
        help="private key for --client-cert (default: {} if set)".format(
            _MCP_ENV_CLIENT_KEY
        ),
    )
    parser.add_argument(
        "--insecure",
        default=False,
        action="store_true",
        help="skip TLS verification entirely; the bearer token is still sent, "
        "so it goes to whoever answers (set {}=1 for the same)".format(
            _MCP_ENV_INSECURE
        ),
    )
    parser.add_argument(
        "--protocol-version",
        default=None,
        metavar="REV",
        help="pin the MCP-Protocol-Version sent before initialize "
        "(default: {})".format(_MCP_DEFAULT_PROTOCOL_VERSION),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_MCP_DEFAULT_TIMEOUT,
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


def _add_tui_stub(sub: Any) -> None:
    """Register `cronstable tui` without importing cronstable.tui.

    Must stay flag-for-flag identical to tui.add_tui_command; the parity test
    enforces it.
    """
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
        default=_TUI_DEFAULT_URL,
        help="daemon web listener (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="bearer token for web.authToken-protected daemons",
    )
    parser.add_argument(
        "--token-env",
        default=_TUI_ENV_TOKEN,
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
            "of the system trust store (env: %s)" % _TUI_ENV_CACERT
        ),
    )
    parser.add_argument(
        "--client-cert",
        default=None,
        metavar="PATH",
        help=(
            "certificate to present to a listener that requires one "
            "(web.tls.clientCa is set) (env: %s)" % _TUI_ENV_CLIENT_CERT
        ),
    )
    parser.add_argument(
        "--client-key",
        default=None,
        metavar="PATH",
        help="private key for --client-cert (env: %s)" % _TUI_ENV_CLIENT_KEY,
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "skip certificate verification; the token is still sent, so "
            "it reaches whoever answers (env: %s)" % _TUI_ENV_INSECURE
        ),
    )
    parser.add_argument(
        "--theme",
        default=None,
        choices=list(_TUI_THEME_NAMES),
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


def main_loop(loop=None):
    """Parse argv, dispatch, and (for the daemon) run the scheduler.

    ``loop`` is optional: passing one keeps it the caller's to close, while
    omitting it defers building a loop -- and importing asyncio at all -- to
    :func:`_run_daemon`, the only branch that needs either.
    """
    parser = argparse.ArgumentParser(prog="cronstable")
    parser.add_argument(
        "-c",
        "--config",
        default=CONFIG_DEFAULT,
        metavar="FILE-OR-DIR",
        help="configuration file, or directory containing configuration files",
    )
    parser.add_argument("-l", "--log-level", default="INFO")
    parser.add_argument(
        "-v", "--validate-config", default=False, action="store_true"
    )
    parser.add_argument(
        "--job-set-id",
        default=False,
        action="store_true",
        help="print the job-set id (an order-independent hash of every job's "
        "effective configuration) and exit; identical across instances "
        "running the same set of jobs",
    )
    parser.add_argument("--version", default=False, action="store_true")
    parser.add_argument(
        "--third-party-licenses",
        default=False,
        action="store_true",
        help="print the bundled third-party license notices (the LGPL "
        "notice for python-zeroconf) and exit",
    )
    _add_state_subcommands(parser)
    # `lock run NAME [flags] -- CMD...` carries an arbitrary trailing command.
    # argparse cannot capture it portably: nargs=REMAINDER swallows our own
    # flags into the command list, while nargs="*" only picks up the tokens
    # after "--" on Python >= 3.13 (older argparse reports them as
    # "unrecognized arguments"). So split the command off at the first "--"
    # ourselves -- identical on every supported Python -- and hand argparse
    # only the head, where our flags and NAME parse cleanly everywhere.
    argv = sys.argv[1:]
    trailing_command = None
    if "--" in argv:
        cut = argv.index("--")
        argv, trailing_command = argv[:cut], argv[cut + 1 :]
    args = parser.parse_args(argv)
    if trailing_command is not None:
        if (
            getattr(args, "command", None) == "lock"
            and getattr(args, "lock_command", None) == "run"
        ):
            args.run_command = trailing_command
        else:
            parser.error("`--` is only valid before a `lock run` command")

    logging.basicConfig(level=getattr(logging, args.log_level))
    # logging.getLogger("asyncio").setLevel(logging.WARNING)
    logger = logging.getLogger("cronstable")

    if args.version:
        print(cronstable.version.version)
        sys.exit(0)

    if args.third_party_licenses:
        # Package data, so the notice travels inside every artifact the
        # code does (wheel, Docker, the one-file frozen binaries): the
        # LGPL notice for bundled python-zeroconf must accompany the
        # binary itself, not just the repository. See LICENSING.md.
        from importlib.resources import files

        print(
            files("cronstable")
            .joinpath("licenses/THIRD-PARTY-NOTICES.txt")
            .read_text(encoding="utf-8")
        )
        sys.exit(0)

    command = getattr(args, "command", None)
    if command == "state":
        # `state get/set/delete/keys` are job-facing (they reach the running
        # daemon's loopback endpoint); everything else under `state` is offline
        # store administration. Route by action name so the two coexist --
        # off the mirrored set, not jobcli's own, so an admin action does not
        # import jobcli's urllib/ssl/email graph to read a frozenset.
        if getattr(args, "state_command", None) in _JOBCLI_STATE_ACTIONS:
            from cronstable import jobcli

            sys.exit(jobcli.dispatch(args))
        # lazy import: the admin module (tarfile etc.) costs the daemon and
        # the stateless install nothing.
        from cronstable import state_admin

        sys.exit(state_admin.dispatch(args))

    if command in (
        "cursor",
        "lock",
        "artifact",
        "idempotent",
        "secret",
        "xcom",
    ):
        from cronstable import jobcli

        sys.exit(jobcli.dispatch(args))

    if command == "mcp":
        # the MCP stdio bridge: a thin stdlib client of the running daemon,
        # like the job-facing subcommands, so it never imports Cron/aiohttp.
        from cronstable import mcpcli

        sys.exit(mcpcli.dispatch(args))

    if command == "tui":
        # the terminal dashboard: a client of the running daemon's web
        # listener, dispatched before the Cron import below so its startup
        # pays for aiohttp only (inside cronstable.tui), never the daemon
        # graph.
        from cronstable import tui

        sys.exit(tui.dispatch(args))

    if args.config == CONFIG_DEFAULT and not os.path.exists(args.config):
        print(
            "cronstable error: configuration file not found, please provide "
            "one with the --config option",
            file=sys.stderr,
        )
        parser.print_help(sys.stderr)
        sys.exit(1)

    # --job-set-id and --validate-config are pure config questions: they parse,
    # answer and exit without ever running a scheduler. Answering them through
    # the config/fingerprint modules directly skips constructing a Cron, which
    # would import the whole daemon graph (aiohttp and friends) and build a
    # PrometheusMetrics, a NodeResourceSampler and a BonjourAdvertiser that
    # this process is about to throw away. The answers are identical:
    # Cron.update_config delegates all validation to parse_config_with_sources
    # (so the same ConfigError surfaces from the same place), and
    # Cron.job_set_id is job_set_id() over cron_jobs, which _apply_reload
    # builds from config.jobs keyed by name without filtering any of them.
    from cronstable.config import ConfigError, parse_config_with_sources

    if args.job_set_id or args.validate_config:
        try:
            config, _sources = parse_config_with_sources(args.config)
        except ConfigError as err:
            logger.error("Configuration error: %s", str(err))
            sys.exit(1)
        if args.job_set_id:
            from collections import OrderedDict

            from cronstable.fingerprint import job_set_id

            # Key by name exactly as _apply_reload does, so a config that
            # somehow carries a repeated job name fingerprints identically
            # here and in the daemon.
            jobs = OrderedDict((job.name, job) for job in config.jobs)
            print(job_set_id(jobs.values()))
        else:
            logger.info("Configuration is valid.")
        sys.exit(0)

    # Imported here, not at module top: this pulls in aiohttp, strictyaml,
    # sentry_sdk and the rest of the daemon graph (~300ms of import). The
    # branches that exit before this point -- --version, --job-set-id,
    # --validate-config and the state / xcom / lock / cursor / artifact /
    # idempotent / secret subcommands (thin urllib clients of the running
    # daemon, routinely spawned from inside jobs) -- never touch Cron, so a
    # job-facing CLI call no longer pays that cost. Only the daemon does.
    from cronstable.cron import Cron

    try:
        cron = Cron(args.config)
    except ConfigError as err:
        logger.error("Configuration error: %s", str(err))
        sys.exit(1)

    _run_daemon(cron, loop)


#: Floor on the shared default executor's worker count.  CPython sizes it
#: ``min(32, cpu_count + 4)``, i.e. 5-6 slots on the 1-2 vCPU containers
#: cronstable is routinely deployed in -- and that one pool carries the config
#: reparse, the leadership lease read/write, the per-completion archive redact
#: and ResourceMonitor.stop(), the /jobs serialize and every getaddrinfo.  Its
#: queue is unbounded, so a minute boundary that finishes hundreds of jobs can
#: park a lease renewal behind them until its renewDeadlineSeconds expires and
#: the node drops leadership.  A floor of 8 does not remove that ordering
#: hazard (only a dedicated lease executor does), but it stops the smallest
#: deployments from being the tightest.
_MIN_EXECUTOR_WORKERS = 8

#: Ceiling, kept at CPython's own so a many-core host does not grow an
#: unbounded thread set for what is an I/O-wait pool.
_MAX_EXECUTOR_WORKERS = 32

#: Thread-name prefix, so a thread dump or a `py-spy dump` attributes the pool
#: instead of showing anonymous ThreadPoolExecutor-N-M workers.
EXECUTOR_THREAD_PREFIX = "cronstable-exec"


def executor_workers() -> int:
    """How many workers the shared default thread pool gets."""
    return max(
        _MIN_EXECUTOR_WORKERS,
        min(_MAX_EXECUTOR_WORKERS, (os.cpu_count() or 1) + 4),
    )


def _install_default_executor(loop) -> None:
    """Give ``loop`` an explicitly sized, named default thread pool.

    Sized with a floor because the interpreter's own default is derived from
    cpu_count alone and takes no account of how many distinct subsystems share
    this one pool.
    """
    from concurrent.futures import ThreadPoolExecutor

    loop.set_default_executor(
        ThreadPoolExecutor(
            max_workers=executor_workers(),
            thread_name_prefix=EXECUTOR_THREAD_PREFIX,
        )
    )


def _run_daemon(cron, loop=None) -> None:
    """Run the scheduler to completion, with shutdown signalling wired up.

    The event loop is built HERE rather than in :func:`main` because asyncio
    is a ~50ms import (and several MB of RSS) that only this branch needs:
    ``--version``, ``--third-party-licenses`` and every job-spawned thin
    client (`state get`, `lock`, `xcom pull`) have already exited by now.  A
    loop the caller supplied stays the caller's to close; one built here is
    closed here.

    Wiring Ctrl-C / termination to a graceful shutdown differs per platform
    (loop signal handlers on POSIX, signal.signal on Windows), so it lives
    behind platform.install_shutdown_handlers.
    """
    owned = loop is None
    if owned:
        loop = _new_event_loop()
    try:
        _install_default_executor(loop)
        remove_shutdown_handlers = platform.install_shutdown_handlers(
            loop, cron.signal_shutdown
        )
        try:
            loop.run_until_complete(cron.run())
        finally:
            remove_shutdown_handlers()
    finally:
        if owned:
            loop.close()


def _new_event_loop():  # pragma: no cover
    """The event loop to run on: uvloop's faster libuv loop when available,
    otherwise stock asyncio.

    uvloop is a drop-in, libuv-based replacement for asyncio's selector loop
    that runs cronstable's I/O paths -- cluster gossip/lease HTTP, the web
    dashboard, the Prometheus scrape -- markedly faster. It is strictly
    optional (install the ``speedups`` extra to pull it in): it has no Windows
    build (where cronstable also needs the Proactor loop for subprocess
    support)
    and ships no wheels for some of the leaner architectures we target, so a
    missing or unimportable uvloop silently falls back to stock asyncio with
    identical behavior. Selecting the loop directly (rather than via
    ``asyncio.set_event_loop_policy``) sidesteps the event-loop-policy API that
    Python 3.14 deprecates.

    ``asyncio.new_event_loop()`` yields the right stock loop per platform: a
    subprocess-capable Proactor loop on Windows (the default since 3.8) and a
    selector loop on POSIX.

    ``asyncio`` is imported here, not at module scope: it is the single
    largest import on the entry point's graph and only the daemon branch
    reaches this function.
    """
    import asyncio

    if sys.platform != "win32":
        try:
            import uvloop
        except ImportError:
            pass
        else:
            return uvloop.new_event_loop()
    return asyncio.new_event_loop()


def main():  # pragma: no cover
    main_loop()


if __name__ == "__main__":  # pragma: no cover
    main()
