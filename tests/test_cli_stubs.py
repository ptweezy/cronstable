"""The `cronstable mcp` / `cronstable tui` / job-facing subparsers are
registered from lightweight stubs in cronstable.__main__ so building the CLI
never imports the heavy tui / mcpcli / jobcli modules (a cost every job-spawned
thin client, the daemon and `--version` would all pay).

These tests pin the stubs flag-for-flag to the real add_mcp_command /
add_tui_command / add_state_job_actions / add_job_commands, so any drift in the
originals fails here instead of silently diverging the two code paths.
"""

import argparse

import pytest

import cronstable.__main__ as main


def _register(func):
    parser = argparse.ArgumentParser(prog="cronstable")
    sub = parser.add_subparsers(dest="command")
    func(sub)
    return sub.choices


def _action_summary(parser):
    """A comparable, order-independent view of a subparser's options.

    Keyed by the sorted option strings (positionals, which have none, by their
    dest, so two of them cannot collide on the same empty key); the auto-added
    -h/--help is dropped since both parsers get it for free.
    """
    summary = {}
    for action in parser._actions:
        opts = tuple(sorted(action.option_strings))
        if opts == ("--help", "-h"):
            continue
        summary[opts or ("<positional {}>".format(action.dest),)] = {
            "dest": action.dest,
            "default": action.default,
            "choices": (
                list(action.choices) if action.choices is not None else None
            ),
            "nargs": action.nargs,
            "const": action.const,
            "type": getattr(action.type, "__name__", action.type),
            "metavar": action.metavar,
            "help": action.help,
            "cls": type(action).__name__,
            "required": action.required,
        }
    return summary


def _tree_summary(parser):
    """_action_summary, recursively through every nested subparser.

    The job-facing verbs nest two levels deep (`cursor advance --force`), and
    the mutually-exclusive grouping of --scope/--global is itself part of the
    contract, so both are captured here rather than compared flag-by-flag.
    """
    subs = {}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, child in action.choices.items():
                subs[name] = _tree_summary(child)
    return {
        "options": _action_summary(parser),
        "mutex": sorted(
            tuple(sorted(a.dest for a in group._group_actions))
            for group in parser._mutually_exclusive_groups
        ),
        "sub": subs,
    }


def test_mcp_stub_matches_real_registration():
    from cronstable import mcpcli

    real = _register(mcpcli.add_mcp_command)["mcp"]
    stub = _register(main._add_mcp_stub)["mcp"]
    assert _action_summary(stub) == _action_summary(real)


def test_tui_stub_matches_real_registration():
    from cronstable import tui

    real = _register(tui.add_tui_command)["tui"]
    stub = _register(main._add_tui_stub)["tui"]
    assert _action_summary(stub) == _action_summary(real)


def _register_jobcli(register):
    """Build the subparser tree the job-facing verbs register into.

    The KV actions hang off `cronstable state` (sharing it with the offline
    admin actions) while the rest are top-level commands, so ``register`` is
    handed the same two parsers __main__ hands its stub.
    """
    parser = argparse.ArgumentParser(prog="cronstable")
    sub = parser.add_subparsers(dest="command")
    state = sub.add_parser("state")
    actions = state.add_subparsers(dest="state_command")
    register(actions, sub)
    return {name: _tree_summary(p) for name, p in sub.choices.items()}


def test_jobcli_stub_matches_real_registration():
    from cronstable import jobcli

    def real(actions, sub):
        jobcli.add_state_job_actions(actions)
        jobcli.add_job_commands(sub)

    assert _register_jobcli(main._add_jobcli_stub) == _register_jobcli(real)


@pytest.mark.parametrize(
    "const_name, module_name, attr",
    [
        ("_JOBCLI_STATE_ACTIONS", "cronstable.jobcli", "STATE_JOB_ACTIONS"),
        ("_MCP_DEFAULT_URL", "cronstable.mcpcli", "DEFAULT_URL"),
        ("_MCP_ENV_TOKEN", "cronstable.mcpcli", "ENV_TOKEN"),
        ("_MCP_ENV_CACERT", "cronstable.mcpcli", "ENV_CACERT"),
        (
            "_MCP_ENV_CLIENT_CERT",
            "cronstable.mcpcli",
            "ENV_CLIENT_CERT",
        ),
        ("_MCP_ENV_CLIENT_KEY", "cronstable.mcpcli", "ENV_CLIENT_KEY"),
        ("_MCP_ENV_INSECURE", "cronstable.mcpcli", "ENV_INSECURE"),
        (
            "_MCP_DEFAULT_PROTOCOL_VERSION",
            "cronstable.mcpcli",
            "DEFAULT_PROTOCOL_VERSION",
        ),
        ("_MCP_DEFAULT_TIMEOUT", "cronstable.mcpcli", "DEFAULT_TIMEOUT"),
        ("_TUI_DEFAULT_URL", "cronstable.tui", "DEFAULT_URL"),
        ("_TUI_ENV_TOKEN", "cronstable.tui", "ENV_TOKEN"),
        ("_TUI_ENV_CACERT", "cronstable.tui", "ENV_CACERT"),
        ("_TUI_ENV_CLIENT_CERT", "cronstable.tui", "ENV_CLIENT_CERT"),
        ("_TUI_ENV_CLIENT_KEY", "cronstable.tui", "ENV_CLIENT_KEY"),
        ("_TUI_ENV_INSECURE", "cronstable.tui", "ENV_INSECURE"),
        ("_TUI_THEME_HUES", "cronstable.tui", "THEME_HUES"),
        ("_TUI_THEME_NAMES", "cronstable.tui", "THEME_NAMES"),
    ],
)
def test_stub_constants_match_source(const_name, module_name, attr):
    import importlib

    module = importlib.import_module(module_name)
    assert getattr(main, const_name) == getattr(module, attr)


def test_building_cli_does_not_import_tui(monkeypatch):
    """The whole point: constructing the parser must not pull in cronstable.tui
    (its module body + unicodedata table cost ~50ms). mcpcli is likewise heavy
    enough to keep off the parser-build path every thin client walks, and
    jobcli drags urllib.request/ssl/email in for ~27ms more.
    """
    import sys

    # Evict any copy imported by an earlier test; if building the parser
    # re-imports one it reappears in sys.modules and the check fails.
    heavy = ("cronstable.tui", "cronstable.mcpcli", "cronstable.jobcli")
    for name in heavy:
        monkeypatch.delitem(sys.modules, name, raising=False)

    parser = argparse.ArgumentParser(prog="cronstable")
    main._add_state_subcommands(parser)

    assert [name for name in heavy if name in sys.modules] == []


def test_state_admin_action_does_not_import_jobcli(monkeypatch, capsys):
    """Routing `cronstable state check` to state_admin reads the mirrored
    action set, so an offline admin action never imports jobcli either.
    """
    import sys

    monkeypatch.delitem(sys.modules, "cronstable.jobcli", raising=False)
    monkeypatch.setattr(
        "cronstable.state_admin.dispatch", lambda args: 0, raising=True
    )
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "check"])
    with pytest.raises(SystemExit):
        main.main_loop()
    assert "cronstable.jobcli" not in sys.modules


def test_entry_point_module_does_not_import_asyncio():
    """asyncio is the entry point's single largest import (~50ms, several MB)
    and only the daemon branch needs it, so neither __main__ nor the
    cronstable.platform module it pulls in for DEFAULT_CONFIG_PATH may import
    it at module scope.
    """
    import subprocess
    import sys

    probe = (
        "import sys, cronstable.__main__, cronstable.platform; "
        "print('asyncio' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True
    )
    assert out.stdout.strip() == "False", out.stderr
