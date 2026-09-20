"""Build definitions propagate changes without handwritten copies."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def generator():
    pytest.importorskip("tomllib")  # generation/CI use Python 3.11+
    spec = importlib.util.spec_from_file_location(
        "generate_build_files", ROOT / "scripts/generate_build_files.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checked_in_build_files_are_current(generator):
    stale = [
        name
        for name, text in generator.generated_files().items()
        if not (ROOT / name).exists()
        or (ROOT / name).read_bytes() != text.encode("utf-8")
    ]
    assert not stale, (
        f"Run python scripts/generate_build_files.py; stale files: {stale}"
    )


def test_free_threaded_dependencies_omit_only_orjson(generator):
    files = generator.generated_files()

    def requirements(name):
        return {
            line
            for line in files[name].splitlines()
            if line and not line.startswith("#")
        }

    regular = requirements("requirements_dev.txt")
    free_threaded = requirements("requirements_dev_freethreaded.txt")
    assert regular - free_threaded == {
        line for line in regular if line.startswith("orjson>=")
    }
    assert not free_threaded - regular
    assert any(line.startswith("orjson>=") for line in regular)


@pytest.mark.parametrize("extra", ["push", "dev"])
@pytest.mark.parametrize(
    "system,machine,has_crypto",
    [
        ("linux", "x86_64", True),
        ("linux", "aarch64", True),
        ("linux", "armv7l", False),
        ("darwin", "arm64", True),
        ("darwin", "x86_64", False),
        ("win32", "AMD64", True),
        ("win32", "x86", False),
        ("win32", "ARM64", False),
    ],
)
def test_push_dependencies_resolve_without_vulnerable_legacy_crypto(
    generator, extra, system, machine, has_crypto
):
    project = generator.tomllib.loads((ROOT / "pyproject.toml").read_text())
    requirements = [
        Requirement(line)
        for line in project["project"]["optional-dependencies"][extra]
    ]
    environment = {"sys_platform": system, "platform_machine": machine}
    active = {
        req.name: req
        for req in requirements
        if req.marker is None or req.marker.evaluate(environment)
    }
    # X25519 remains available even where patched cryptography has no wheel.
    assert "pynacl" in active
    assert ("cryptography" in active) == has_crypto
    if has_crypto:
        spec = active["cryptography"].specifier
        assert "50.0.1" in spec
        for vulnerable in ("44.0.0", "48.0.0", "48.0.1", "49.0.0"):
            assert vulnerable not in spec


def test_dependency_bump_reaches_dev_binary_and_every_image(
    generator, tmp_path
):
    for name in (
        "pyproject.toml",
        "docker/images.toml",
        "docker/templates/Dockerfile",
        ".github/docker-matrix.json",
    ):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    project = tmp_path / "pyproject.toml"
    data = generator.tomllib.loads(project.read_text())["project"]
    original = generator.extra_floors(data)["orjson"]
    project.write_text(project.read_text().replace(original, "orjson>=99.0"))
    files = generator.generated_files(tmp_path)
    assert "orjson>=99.0;" in files["requirements_dev.txt"]
    assert files["pyinstaller/requirements/orjson.txt"] == "orjson>=99.0\n"
    matrix = json.loads((tmp_path / ".github/docker-matrix.json").read_text())
    for row in matrix:
        assert 'install_orjson.sh "orjson>=99.0"' in files[row["dockerfile"]]


def test_check_reports_stale_files_without_writing(
    generator, tmp_path, monkeypatch
):
    path = tmp_path / "requirements_dev.txt"
    path.write_text("old\n")
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    monkeypatch.setattr(
        generator, "generated_files", lambda root: {path.name: "new\n"}
    )
    assert generator.main(["--check"]) == 1
    assert path.read_text() == "old\n"
    assert generator.main([]) == 0
    assert path.read_bytes() == b"new\n"
    assert generator.main(["--check"]) == 0


@pytest.fixture
def installer(tmp_path, monkeypatch):
    if os.name == "nt" or shutil.which("sh") is None:
        pytest.skip("POSIX build installer")
    for script in ("install_extra.sh", "install_orjson.sh"):
        shutil.copyfile(ROOT / "pyinstaller" / script, tmp_path / script)
    requirements = tmp_path / "requirements"
    requirements.mkdir()
    # A distinctive floor proves the shell reads the supplied file.
    for name in ("cryptography", "orjson"):
        (requirements / f"{name}.txt").write_text(f"{name}>=42.0\n")
    log = tmp_path / "calls.jsonl"
    stub = (
        f"#!{sys.executable}\n"
        + """import json, os, sys
from pathlib import Path
kind = Path(sys.argv[0]).name
with open(os.environ["CRONSTABLE_INSTALL_LOG"], "a") as out:
    out.write(json.dumps([kind, *sys.argv[1:]]) + "\\n")
if kind == "probe-stub":
    raise SystemExit(int(os.environ.get("PROBE_STATUS", "0")))
if sys.argv[1] == "install":
    raise SystemExit(int(os.environ.get("INSTALL_STATUS", "0")))
"""
    )
    for name in ("pip-stub", "probe-stub"):
        path = tmp_path / name
        path.write_text(stub)
        path.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("PIP", "pip-stub")
    monkeypatch.setenv("PY", "probe-stub")
    monkeypatch.delenv("PIPUNINST", raising=False)
    monkeypatch.delenv("RUST_SETUP", raising=False)
    monkeypatch.setenv("CRONSTABLE_INSTALL_LOG", str(log))

    def run(script, *args):
        result = subprocess.run(
            ["sh", str(tmp_path / script), *args],
            capture_output=True,
            text=True,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, calls

    return run


@pytest.mark.parametrize(
    "policy,install_status,probe_status,exit_code,uninstall",
    [
        ("hard", 0, 0, 0, False),
        ("hard", 1, 0, 1, False),
        ("hard", 0, 1, 1, False),
        ("soft", 1, 0, 0, False),
        ("soft", 0, 1, 0, True),
        ("soft", 0, 2, 2, False),
    ],
)
def test_generated_floor_preserves_installer_policy(
    installer,
    monkeypatch,
    policy,
    install_status,
    probe_status,
    exit_code,
    uninstall,
):
    monkeypatch.setenv("INSTALL_STATUS", str(install_status))
    monkeypatch.setenv("PROBE_STATUS", str(probe_status))
    result, calls = installer(
        "install_extra.sh", "cryptography", "@,<49", policy
    )
    assert result.returncode == exit_code, result.stderr
    assert calls[0] == ["pip-stub", "install", "cryptography>=42.0,<49"]
    assert any(call[1] == "uninstall" for call in calls) == uninstall


def test_binary_orjson_uses_generated_floor(installer):
    result, calls = installer("install_orjson.sh")
    assert result.returncode == 0, result.stderr
    assert calls[0] == ["pip-stub", "install", "orjson>=42.0"]
    assert calls[1][-1] == "orjson"
