"""Build definitions propagate changes without handwritten copies."""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]
DIGEST = "sha256:" + "a" * 64


def copy_build_inputs(root):
    """Copy what the generator reads, with each Dockerfile's pinned digests."""
    matrix = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    for name in (
        "pyproject.toml",
        "docker/images.toml",
        "docker/templates/Dockerfile",
        ".github/docker-matrix.json",
        *(row["dockerfile"] for row in matrix),
    ):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    return matrix


def from_lines(dockerfile):
    return [
        line for line in dockerfile.splitlines() if line.startswith("FROM ")
    ]


@pytest.fixture
def generator():
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
    project = generator.tomllib.loads((ROOT / "pyproject.toml").read_text())
    regular = set(project["project"]["optional-dependencies"]["dev"])
    free_threaded = {
        line
        for line in files["requirements/dev-freethreaded.txt"].splitlines()
        if line and not line.startswith("#")
    }
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
        assert "50.0.2" in spec
        for vulnerable in ("44.0.0", "48.0.0", "48.0.1", "49.0.0"):
            assert vulnerable not in spec


def test_dependency_bump_reaches_min_pins_binary_and_every_image(
    generator, tmp_path
):
    matrix = copy_build_inputs(tmp_path)
    project = tmp_path / "pyproject.toml"
    data = generator.tomllib.loads(project.read_text())["project"]
    original = generator.extra_floors(data)["orjson"]
    project.write_text(project.read_text().replace(original, "orjson>=99.0"))
    files = generator.generated_files(tmp_path)
    assert "orjson==99.0;" in files["requirements/min.txt"]
    assert not any(
        line.startswith("orjson")
        for line in files["requirements/dev-freethreaded.txt"].splitlines()
    )
    assert files["pyinstaller/requirements/orjson.txt"] == "orjson>=99.0\n"
    for row in matrix:
        assert 'install_orjson.sh "orjson>=99.0"' in files[row["dockerfile"]]


def test_check_reports_stale_files_without_writing(
    generator, tmp_path, monkeypatch
):
    path = tmp_path / "generated.txt"
    path.write_text("old\n")
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    monkeypatch.setattr(
        generator,
        "generated_files",
        lambda root, pins=None: {path.name: "new\n"},
    )
    assert generator.main(["--check"]) == 1
    assert path.read_text() == "old\n"
    assert generator.main([]) == 0
    assert path.read_bytes() == b"new\n"
    assert generator.main(["--check"]) == 0


def test_every_generated_dockerfile_pins_its_base_images(generator):
    matrix = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    for row in matrix:
        references = generator.FROM_REFERENCE.findall(
            (ROOT / row["dockerfile"]).read_text()
        )
        assert len(references) == 2, row["dockerfile"]
        for reference in references:
            assert generator.PINNED_REFERENCE.fullmatch(reference), reference


def test_the_image_definitions_carry_no_digest(generator, tmp_path):
    # A digest in docker/images.toml would be a second copy that Dependabot
    # cannot update.
    images = generator.tomllib.loads((ROOT / "docker/images.toml").read_text())
    for image in images.values():
        assert "@" not in image["builder"] + image.get("runtime", "")
    copy_build_inputs(tmp_path)
    definitions = tmp_path / "docker/images.toml"
    declared = images["fedora"]["builder"]
    definitions.write_text(
        definitions.read_text().replace(
            f'"{declared}"', f'"{declared}@{DIGEST}"'
        )
    )
    with pytest.raises(generator.PinError, match="names a digest"):
        generator.generated_files(tmp_path)


def test_a_digest_bump_in_a_dockerfile_leaves_it_current(generator, tmp_path):
    # Dependabot changes only the digests on a generated Dockerfile's FROM
    # lines. The generator renders the same file from them.
    matrix = copy_build_inputs(tmp_path)
    for row in matrix:
        path = tmp_path / row["dockerfile"]
        bumped, count = re.subn(
            r"@sha256:[0-9a-f]{64}", "@" + DIGEST, path.read_text()
        )
        assert count == 2, row["dockerfile"]
        path.write_text(bumped)
    files = generator.generated_files(tmp_path)
    for row in matrix:
        current = (tmp_path / row["dockerfile"]).read_text()
        assert files[row["dockerfile"]] == current
        assert all(DIGEST in line for line in from_lines(current))


def test_a_tag_bump_in_a_dockerfile_waits_for_the_image_definition(
    generator, tmp_path
):
    copy_build_inputs(tmp_path)
    dockerfile = tmp_path / "docker/Dockerfile.fedora"
    definitions = tmp_path / "docker/images.toml"
    declared = generator.tomllib.loads(definitions.read_text())["fedora"][
        "builder"
    ]
    bumped = declared + "-next"
    dockerfile.write_text(
        dockerfile.read_text().replace(declared + "@", bumped + "@")
    )
    with pytest.raises(generator.PinError) as error:
        generator.generated_files(tmp_path)
    assert f"docker/Dockerfile.fedora pins {bumped}" in str(error.value)
    assert f"docker/images.toml declares {declared}" in str(error.value)
    definitions.write_text(
        definitions.read_text().replace(f'"{declared}"', f'"{bumped}"')
    )
    files = generator.generated_files(tmp_path)
    assert files["docker/Dockerfile.fedora"] == dockerfile.read_text()


def test_a_requested_pin_sets_the_digest_on_every_from_line(
    generator, tmp_path
):
    copy_build_inputs(tmp_path)
    definitions = tmp_path / "docker/images.toml"
    declared = generator.tomllib.loads(definitions.read_text())["ubuntu"][
        "builder"
    ]
    moved = generator.repository(declared) + ":next"
    definitions.write_text(
        definitions.read_text().replace(f'"{declared}"', f'"{moved}"')
    )
    # A tag that no Dockerfile pins yet needs a digest from the caller.
    with pytest.raises(generator.PinError, match=f"--pin {moved}@sha256:"):
        generator.generated_files(tmp_path)
    files = generator.generated_files(tmp_path, {moved: DIGEST})
    assert from_lines(files["docker/Dockerfile.ubuntu"]) == [
        f"FROM {moved}@{DIGEST} AS builder",
        f"FROM {moved}@{DIGEST}",
    ]
    # A requested pin also replaces a digest that a Dockerfile holds.
    debian = generator.tomllib.loads(definitions.read_text())["debian"][
        "builder"
    ]
    files = generator.generated_files(
        tmp_path, {moved: DIGEST, debian: DIGEST}
    )
    assert from_lines(files["Dockerfile"]) == [
        f"FROM {debian}@{DIGEST} AS builder",
        f"FROM {debian}@{DIGEST}",
    ]


def test_a_pin_for_an_undeclared_image_is_refused(generator, tmp_path):
    copy_build_inputs(tmp_path)
    with pytest.raises(
        generator.PinError,
        match="declares no base image named example.invalid/base:1",
    ):
        generator.generated_files(tmp_path, {"example.invalid/base:1": DIGEST})


def test_one_dockerfile_cannot_pin_an_image_to_two_digests(
    generator, tmp_path
):
    copy_build_inputs(tmp_path)
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        re.sub(
            r"@sha256:[0-9a-f]{64}",
            "@" + DIGEST,
            dockerfile.read_text(),
            count=1,
        )
    )
    with pytest.raises(generator.PinError, match="Dockerfile pins .* to two"):
        generator.generated_files(tmp_path)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("fedora:45", "fedora"),
        ("opensuse/leap:16.0", "opensuse/leap"),
        ("registry.example:5000/base:1", "registry.example:5000/base"),
        ("registry.example:5000/base", "registry.example:5000/base"),
    ],
)
def test_repository_drops_only_the_tag(generator, name, expected):
    assert generator.repository(name) == expected


def test_main_reports_a_missing_pin_then_accepts_one(
    generator, tmp_path, monkeypatch, capsys
):
    copy_build_inputs(tmp_path)
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    assert generator.main([]) == 0
    assert generator.main(["--check"]) == 0
    definitions = tmp_path / "docker/images.toml"
    declared = generator.tomllib.loads(definitions.read_text())["alpine"][
        "builder"
    ]
    moved = generator.repository(declared) + ":next"
    definitions.write_text(
        definitions.read_text().replace(f'"{declared}"', f'"{moved}"')
    )
    before = (tmp_path / "docker/Dockerfile.alpine").read_text()
    capsys.readouterr()
    assert generator.main(["--check"]) == 1
    assert f"--pin {moved}@sha256:DIGEST" in capsys.readouterr().out
    assert generator.main([]) == 1
    assert (tmp_path / "docker/Dockerfile.alpine").read_text() == before
    assert generator.main(["--pin", f"{moved}@{DIGEST}"]) == 0
    assert generator.main(["--check"]) == 0
    assert f"FROM {moved}@{DIGEST}" in (
        tmp_path / "docker/Dockerfile.alpine"
    ).read_text()


@pytest.mark.parametrize(
    "arguments",
    [
        ["--pin", "fedora:45"],
        ["--pin", "fedora:45@sha256:abc"],
        ["--check", "--pin", "fedora:45@" + DIGEST],
    ],
)
def test_main_rejects_an_unusable_pin_request(generator, arguments):
    with pytest.raises(SystemExit) as exit_info:
        generator.main(arguments)
    assert exit_info.value.code == 2


@pytest.fixture
def installer(tmp_path, monkeypatch):
    if os.name == "nt" or shutil.which("sh") is None:
        pytest.skip("POSIX build installer")
    for script in ("install_extra.sh", "install_orjson.sh"):
        shutil.copyfile(ROOT / "pyinstaller" / script, tmp_path / script)
    requirements = tmp_path / "requirements"
    requirements.mkdir()
    # A distinctive floor proves the shell reads the supplied file.
    for name in ("cryptography", "orjson", "isal"):
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


@pytest.mark.parametrize(
    "install_status,probe_status,uninstall",
    [(0, 0, False), (1, 0, False), (0, 1, True)],
)
def test_wheel_policy_is_soft_and_never_builds_the_sdist(
    installer, monkeypatch, install_status, probe_status, uninstall
):
    # isal's sdist needs nasm on x86, so the lanes install it from a wheel
    # or not at all, with soft's failure handling either way.
    monkeypatch.setenv("INSTALL_STATUS", str(install_status))
    monkeypatch.setenv("PROBE_STATUS", str(probe_status))
    result, calls = installer("install_extra.sh", "isal", "@", "wheel")
    assert result.returncode == 0, result.stderr
    assert calls[0] == [
        "pip-stub",
        "install",
        "--only-binary",
        "isal",
        "isal>=42.0",
    ]
    assert any(call[1] == "uninstall" for call in calls) == uninstall


def test_binary_orjson_uses_generated_floor(installer):
    result, calls = installer("install_orjson.sh")
    assert result.returncode == 0, result.stderr
    assert calls[0] == ["pip-stub", "install", "orjson>=42.0"]
    assert calls[1][-1] == "orjson"
