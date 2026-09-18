"""Check package updates and cleanup in the final container filesystem."""

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runtime():
    spec = importlib.util.spec_from_file_location(
        "refresh_runtime", ROOT / ".github/scripts/refresh_runtime.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write(root, relative):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("fixture", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "prefix", ["opt/venv", "opt/python-runtime", "usr/local"]
)
@pytest.mark.parametrize("library", ["lib", "lib64"])
def test_cleanup_removes_packaging_and_bundled_metadata(
    runtime, tmp_path, prefix, library
):
    python = f"{prefix}/{library}/python3.14"
    removed = [
        write(tmp_path, f"{python}/site-packages/{relative}")
        for relative in (
            "pip/_vendor/bom.cdx.json",
            "pip-26.2.1.dist-info/METADATA",
            "setuptools/package_index.py",
            "setuptools-65.5.1.dist-info/METADATA",
            "pkg_resources/__init__.py",
            "_distutils_hack/__init__.py",
            "distutils-precedence.pth",
            "wheel/__init__.py",
            "wheel-0.45.1.dist-info/METADATA",
        )
    ]
    removed += [
        write(tmp_path, f"{python}/ensurepip/_bundled/pip-26.2.1.whl"),
        write(tmp_path, f"{prefix}/bin/pip3.14"),
        write(tmp_path, f"{prefix}/bin/easy_install-3.11"),
        write(tmp_path, f"{prefix}/bin/wheel"),
    ]
    kept = [
        write(tmp_path, f"{python}/site-packages/cronstable/__init__.py"),
        write(tmp_path, f"{python}/site-packages/packaging/__init__.py"),
        write(tmp_path, f"{python}/ssl.py"),
        write(tmp_path, f"{prefix}/bin/python3.14"),
        write(tmp_path, "usr/lib/python3.14/site-packages/pip/__init__.py"),
        write(tmp_path, "usr/lib/python3.14/site-packages/setuptools/a.py"),
    ]
    with pytest.raises(RuntimeError, match="Runtime contains build tools"):
        runtime.check(tmp_path, "debian")
    runtime.clean(tmp_path, "debian")
    runtime.clean(tmp_path, "debian")
    runtime.check(tmp_path, "debian")
    assert all(not path.exists() for path in removed)
    assert all(path.read_text() == "fixture" for path in kept)


def test_cleanup_covers_dist_packages(runtime, tmp_path):
    metadata = write(
        tmp_path,
        "usr/local/lib/python3.11/dist-packages/setuptools-65.5.1.dist-info/METADATA",
    )
    runtime.clean(tmp_path, "amazonlinux")
    assert not metadata.exists()


def test_ubuntu_cleanup_removes_pebble_and_package_lists(runtime, tmp_path):
    pebble = write(tmp_path, "usr/bin/pebble")
    lists = write(tmp_path, "var/lib/apt/lists/partial/package-list")
    cronstable = write(tmp_path, "opt/venv/bin/cronstable")
    runtime.clean(tmp_path, "ubuntu")
    runtime.check(tmp_path, "ubuntu")
    assert not pebble.exists()
    assert not lists.exists()
    assert cronstable.exists()


def test_cleanup_rejects_paths_outside_its_root(runtime, tmp_path):
    outside = write(tmp_path, "outside/file")
    root = tmp_path / "image"
    root.mkdir()
    with pytest.raises(ValueError, match="escapes the image root"):
        runtime.remove(root, outside.parent)
    assert outside.exists()


@pytest.mark.parametrize(
    "command,codes,success",
    [
        ("zypper --non-interactive update --no-recommends", [103, 0], True),
        ("zypper --non-interactive update --no-recommends", [102], True),
        ("zypper --non-interactive refresh", [106] * 5, False),
        ("apt-get update", [100, 0], True),
        ("apt-get upgrade -y", [102] * 5, False),
    ],
)
def test_package_updates_retry_and_propagate_failures(
    runtime, monkeypatch, command, codes, success
):
    attempts = []
    results = iter(codes)

    def run(args, **kwargs):
        attempts.append(args)
        assert kwargs["env"]["DEBIAN_FRONTEND"] == "noninteractive"
        return SimpleNamespace(returncode=next(results))

    monkeypatch.setattr(runtime.subprocess, "run", run)
    monkeypatch.setattr(runtime.time, "sleep", lambda delay: None)
    if success:
        runtime.run(command)
    else:
        with pytest.raises(subprocess.CalledProcessError):
            runtime.run(command)
    assert len(attempts) == len(codes)


def test_amazon_updates_escape_the_base_repository_snapshot(runtime):
    assert "--releasever=latest upgrade" in runtime.UPDATES["amazonlinux"][0]


def test_distroless_cleanup_needs_no_package_manager(runtime, tmp_path):
    assert runtime.UPDATES["distroless"] == ()
    pip = write(
        tmp_path, "opt/python-runtime/lib/python3.14/site-packages/pip/a.py"
    )
    runtime.clean(tmp_path, "distroless")
    runtime.check(tmp_path, "distroless")
    assert not pip.exists()


def test_refresh_refuses_to_modify_the_host(runtime, monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["refresh_runtime.py", "refresh", "--distro", "debian"]
    )
    monkeypatch.setattr(sys, "prefix", "/usr")
    with pytest.raises(SystemExit) as error:
        runtime.main()
    assert error.value.code == 2
