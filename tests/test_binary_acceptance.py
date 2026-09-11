"""The external test driver fails closed instead of silently testing source."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_windows_driver_uses_normalized_environment_keys(
    monkeypatch, tmp_path
):
    from acceptance import harness

    system = tmp_path / "Windows"
    binary = tmp_path / "bin" / "cronstable.exe"
    # os.environ itself is case-insensitive on Windows; its copy() is a
    # plain dict with uppercase keys. Exercise that behavior on every host
    # without changing the real os.name (which would also change pytest).
    monkeypatch.setattr(
        harness,
        "os",
        SimpleNamespace(
            name="nt",
            pathsep=";",
            environ={"SYSTEMROOT": str(system), "PATH": "source-venv"},
        ),
    )
    with patch.object(harness.socket, "socket") as socket_type:
        sock = socket_type.return_value.__enter__.return_value
        sock.getsockname.return_value = ("127.0.0.1", 12345)
        app = harness.Daemon(binary, tmp_path, tmp_path)
    assert app.env["SYSTEMROOT"] == str(system)
    assert app.env["PATH"].split(";") == [
        str(binary.parent),
        str(system / "System32"),
        str(system),
    ]


def run_driver(suite, *args, cwd=ROOT):
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env["PYTEST_ADDOPTS"] = ""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-c",
            str(suite / "pytest.ini"),
            str(suite),
            "-q",
            *map(str, args),
        ],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize("source_script", [False, True])
def test_driver_requires_a_native_artifact(tmp_path, source_script):
    args = ["-k", "startup", "--acceptance-output", tmp_path / "evidence"]
    expected = "--cronstable-bin is required"
    if source_script:
        script = tmp_path / "cronstable"
        script.write_text("#!/usr/bin/env python3\nraise SystemExit(0)\n")
        args += ["--cronstable-bin", script]
        expected = "must be a native executable"
    result = run_driver(ROOT / "acceptance", *args)
    assert result.returncode != 0
    assert expected in result.stdout + result.stderr
    assert not (tmp_path / "evidence").exists()


def test_driver_fails_when_nothing_is_selected():
    result = run_driver(ROOT / "acceptance", "-k", "no_such_acceptance_case")
    assert result.returncode != 0


def test_driver_fails_when_a_required_behavior_skips(tmp_path):
    suite = tmp_path / "acceptance"
    suite.mkdir()
    for filename in ("__init__.py", "conftest.py", "harness.py", "pytest.ini"):
        shutil.copy2(ROOT / "acceptance" / filename, suite / filename)
    (suite / "test_missing_capability.py").write_text(
        "import pytest\ndef test_required():\n"
        "    pytest.skip('missing runtime')\n"
    )
    result = run_driver(suite, cwd=tmp_path)
    assert result.returncode != 0
    assert "1 skipped" in result.stdout
