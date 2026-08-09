import argparse
import logging
import os
import sys
from typing import Any

import cronstable.version
from cronstable import _cliargs, platform

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
    # These (and `mcp` / `tui`) all register from cronstable._cliargs, a
    # stdlib-only leaf, NOT by importing cronstable.jobcli / cronstable.mcpcli
    # / cronstable.tui. Importing tui alone runs its ~7000-line module body
    # and pulls unicodedata's C table plus dozens of other modules (~50ms),
    # and jobcli drags urllib.request/ssl/email in for ~27ms; every
    # job-spawned thin client (`state get`, `lock`, `xcom pull`) builds this
    # parser first, and so does the daemon and `--version`, so an eager
    # import would tax every invocation for commands almost never the one
    # invoked. The real modules are imported only inside their dispatch
    # branches (see main_loop); tests/test_cli_stubs.py pins the lazy-import
    # property.
    _cliargs.add_state_job_actions(actions)
    _cliargs.add_job_commands(sub)
    _cliargs.add_mcp_command(sub)
    _cliargs.add_tui_command(sub)
    _add_init_command(sub)


def _add_init_command(sub: Any) -> None:
    """Register `cronstable init`: write a starter configuration and exit.

    The answer to the very first wall a new install hits, the
    "configuration file not found" exit: one command creates the config
    directory and a commented starter file at the platform default (or a
    directory of the caller's choosing, e.g. a machine-wide
    ``%ProgramData%\\cronstable`` on Windows).
    """
    init = sub.add_parser(
        "init",
        help="write a commented starter configuration into DIRECTORY "
        "(default: the platform config directory) and exit",
    )
    init.add_argument(
        "directory",
        nargs="?",
        default=None,
        metavar="DIRECTORY",
        help="target configuration directory (default: {})".format(
            CONFIG_DEFAULT
        ),
    )


#: The starter file `cronstable init` writes: one working job in the
#: platform's own shell, and the most commonly wanted next step (a local
#: dashboard with an API token) left commented out.
_INIT_STARTER = """\
# cronstable starter configuration (written by `cronstable init`).
# Every *.yaml file in this directory is loaded; classic crontab files
# (*.crontab, *.cron, or a file named `crontab`) are accepted here too.
# Reference: https://github.com/ptweezy/cronstable/wiki/Configuration-Reference
#
# Windows note: quote paths with single quotes ('C:\\scripts\\nightly.bat').
# In double-quoted YAML strings a backslash starts an escape sequence.

jobs:
  - name: hello
    command: {hello}
    schedule: "*/5 * * * *"

# Uncomment for the web dashboard and HTTP control API on localhost. The
# bearer token also unlocks POST /shutdown, the graceful stop for service
# wrappers and supervisors:
#
# web:
#   listen:
#     - http://127.0.0.1:8080
#   authToken:
#     fromEnvVar: CRONSTABLE_TOKEN
"""


def _run_init(args: Any) -> int:
    """Write a commented starter configuration (`cronstable init`).

    Creates the target directory when needed and writes ``cronstable.yaml``
    into it. Never touches an existing setup: a target that is a file, that
    already holds config files, or that already has a ``cronstable.yaml``
    is refused with the reason, so re-running init is always safe.
    """
    # dispatch-time import, like the other subcommand branches: building the
    # parser must stay import-light for every thin-client invocation.
    from cronstable.crontabs import is_crontab_path

    target = args.directory or CONFIG_DEFAULT
    if os.path.isfile(target):
        print(
            "cronstable init: {} is a file; init writes a starter file "
            "into a configuration DIRECTORY. Name a directory, or edit "
            "the existing file directly.".format(target),
            file=sys.stderr,
        )
        return 1
    existing = []
    if os.path.isdir(target):
        for name in sorted(os.listdir(target)):
            base, ext = os.path.splitext(name)
            if not base or base[0] in {"_", "."}:
                continue
            if ext in {".yml", ".yaml"} or is_crontab_path(name):
                existing.append(name)
    if existing:
        print(
            "cronstable init: {} already holds configuration ({}); "
            "refusing to add a starter to a live setup".format(
                target, ", ".join(existing[:5])
            ),
            file=sys.stderr,
        )
        return 1
    hello = (
        "echo hello from cronstable on %COMPUTERNAME%"
        if platform.IS_WINDOWS
        else 'echo "hello from cronstable on $(hostname)"'
    )
    path = os.path.join(target, "cronstable.yaml")
    try:
        os.makedirs(target, exist_ok=True)
        # "x": exclusive create, so a config racing into place between the
        # scan above and this write still cannot be clobbered.
        with open(path, "x", encoding="utf-8") as handle:
            handle.write(_INIT_STARTER.format(hello=hello))
    except OSError as ex:
        print(
            "cronstable init: could not write {}: {}".format(path, ex),
            file=sys.stderr,
        )
        return 1
    print("wrote {}".format(path))
    if args.directory and os.path.abspath(target) != os.path.abspath(
        CONFIG_DEFAULT
    ):
        print("start the scheduler with: cronstable -c {}".format(target))
    else:
        print("start the scheduler with: cronstable")
    return 0


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
        help="configuration file, or directory containing configuration "
        "files (default: %(default)s)",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        metavar="LEVEL",
        help="logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL "
        "(default: INFO)",
    )
    parser.add_argument(
        "-v",
        "--validate-config",
        default=False,
        action="store_true",
        help="validate the configuration and exit (-v is NOT verbose)",
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

    # resolve the level name leniently (any case, plus aliases like WARN)
    # but fail a typo as a clean usage error, not an AttributeError
    # traceback out of a bare getattr(logging, ...).
    log_level = logging.getLevelName(str(args.log_level).upper())
    if not isinstance(log_level, int):
        parser.error(
            "invalid --log-level {!r} (use DEBUG, INFO, WARNING, ERROR "
            "or CRITICAL)".format(args.log_level)
        )
    logging.basicConfig(level=log_level)
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
        # off the leaf's set, not jobcli's re-export of it, so an admin action
        # does not import jobcli's urllib/ssl/email graph to read a frozenset.
        if getattr(args, "state_command", None) in _cliargs.STATE_JOB_ACTIONS:
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

    if command == "init":
        sys.exit(_run_init(args))

    if args.config == CONFIG_DEFAULT and not os.path.exists(args.config):
        print(
            "cronstable error: configuration file not found at the default "
            "location ({}). Run `cronstable init` to create a starter "
            "configuration there, or point -c/--config at an existing file "
            "or directory.".format(CONFIG_DEFAULT),
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
