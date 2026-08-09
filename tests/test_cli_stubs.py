"""The `cronstable mcp` / `cronstable tui` / job-facing subparsers are
registered from cronstable._cliargs, a stdlib-only leaf, so building the CLI
never imports the heavy tui / mcpcli / jobcli modules (a cost every
job-spawned thin client, the daemon and `--version` would all pay).

These tests pin that import-cost property.  The parser definitions
themselves need no parity test: __main__ and the surface modules share the
single copy in the leaf.
"""

import argparse
import subprocess
import sys

import pytest

import cronstable.__main__ as main


def test_building_cli_does_not_import_tui(monkeypatch):
    """The whole point: constructing the parser must not pull in cronstable.tui
    (its module body + unicodedata table cost ~50ms). mcpcli is likewise heavy
    enough to keep off the parser-build path every thin client walks, and
    jobcli drags urllib.request/ssl/email in for ~27ms more.
    """
    # Evict any copy imported by an earlier test; if building the parser
    # re-imports one it reappears in sys.modules and the check fails.
    heavy = ("cronstable.tui", "cronstable.mcpcli", "cronstable.jobcli")
    for name in heavy:
        monkeypatch.delitem(sys.modules, name, raising=False)

    parser = argparse.ArgumentParser(prog="cronstable")
    main._add_state_subcommands(parser)

    assert [name for name in heavy if name in sys.modules] == []


def test_state_admin_action_does_not_import_jobcli(monkeypatch, capsys):
    """Routing `cronstable state check` to state_admin reads the action set
    off the cronstable._cliargs leaf, so an offline admin action never
    imports jobcli either.
    """
    monkeypatch.delitem(sys.modules, "cronstable.jobcli", raising=False)
    monkeypatch.setattr(
        "cronstable.state_admin.dispatch", lambda args: 0, raising=True
    )
    monkeypatch.setattr(sys, "argv", ["cronstable", "state", "check"])
    with pytest.raises(SystemExit):
        main.main_loop()
    assert "cronstable.jobcli" not in sys.modules


def _probe(code: str) -> str:
    """Run ``code`` in a fresh interpreter and return its stdout.

    A subprocess, not this process: the suite has long since imported
    everything, so only a clean interpreter can show what an import alone
    pulls in.
    """
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_entry_point_module_does_not_import_asyncio():
    """asyncio is the entry point's single largest import (~50ms, several MB)
    and only the daemon branch needs it, so neither __main__ nor the
    cronstable.platform module it pulls in for DEFAULT_CONFIG_PATH may import
    it at module scope.
    """
    probe = (
        "import sys, cronstable.__main__, cronstable.platform; "
        "print('asyncio' in sys.modules)"
    )
    assert _probe(probe) == "False"


def test_entry_point_module_does_not_import_heavy_surfaces():
    """Importing cronstable.__main__ registers every subcommand (through the
    cronstable._cliargs leaf), and that alone must not pull in tui / mcpcli /
    jobcli; only dispatching one of their commands may.
    """
    probe = (
        "import sys, cronstable.__main__; "
        "print(sorted(m for m in sys.modules if m in ("
        "'cronstable.tui', 'cronstable.mcpcli', 'cronstable.jobcli')))"
    )
    assert _probe(probe) == "[]"


def test_cliargs_is_a_leaf():
    """cronstable._cliargs is imported at module scope by __main__,
    mcpcli AND tui, so any import it grows beyond the stdlib is paid by all
    of them at once; asyncio is checked by name as the costliest stdlib
    module it could sprout.
    """
    probe = (
        "import sys, cronstable._cliargs; "
        "print(sorted(m for m in sys.modules "
        "if (m.startswith('cronstable') "
        "and m not in ('cronstable', 'cronstable._cliargs')) "
        "or m == 'asyncio'))"
    )
    assert _probe(probe) == "[]"
