"""Check cache isolation, platform scheduling, and browser test enforcement."""

import importlib.util
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from strictyaml.ruamel import YAML

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / ".github/scripts" / (name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workflow(name):
    return YAML(typ="safe").load(
        (ROOT / f".github/workflows/{name}.yml").read_text()
    )


def test_wheel_groups_cover_each_platform_once():
    matrix = load("docker_matrix")
    distros = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    wheels = json.loads((ROOT / ".github/pq-wheel-matrix.json").read_text())
    groups = matrix.wheel_groups(distros, wheels)
    images = []
    for libc, rows in groups.items():
        assert len(rows) == len(wheels[libc])
        for row in rows:
            assert row["docker"]
            for image in row["docker"]:
                assert image["wheel"] == f"pq-wheel-{libc}-{row['arch']}"
                assert image["platforms"] == row["platform"]
                images.append(image)
    expected = [row for row in matrix.platforms(distros) if row["wheel"]]

    def identity(row):
        return row["distro"], row["platforms"]

    assert sorted(map(identity, images)) == sorted(map(identity, expected))


@pytest.mark.parametrize(
    "name,prepare", [("release", "version"), ("rehydrate-docker", "prepare")]
)
def test_each_architecture_waits_only_for_its_wheel(name, prepare):
    jobs = workflow(name)["jobs"]
    for libc in ("glibc", "musl"):
        job = jobs[f"docker-{libc}"]
        assert "pq-wheels" not in str(job.get("needs"))
        assert job["uses"].endswith("/build-docker-with-wheel.yml")
        assert (
            f"needs.{prepare}.outputs.pq-{libc}"
            in job["strategy"]["matrix"]["include"]
        )
        assert job["with"]["wheel"] == "${{ toJSON(matrix) }}"
        assert job["with"]["matrix"] == "${{ toJSON(matrix.docker) }}"
        assert job["permissions"]["packages"] == "write"
    chained = workflow("build-docker-with-wheel")["jobs"]
    assert chained["images"]["needs"] == "wheel"
    assert (
        chained["wheel"]["with"]["matrix"]
        == "${{ format('[{0}]', inputs.wheel) }}"
    )
    assert not chained["images"].get("continue-on-error")


def test_cache_trimming_preserves_crates_and_openssl(tmp_path):
    cache = load("cache_size")
    files = {
        "cargo/registry/src/crate/source.rs": 40,
        "cargo/registry/cache/crate.crate": 10,
        "target/old/debug/object.o": 40,
        "target/old/release/library.rlib": 40,
        "openssl/lib/libcrypto.a": 10,
    }
    for name, length in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * length)
    assert cache.trim(tmp_path, limit=20) == 20
    assert (tmp_path / "openssl/lib/libcrypto.a").read_bytes() == b"x" * 10
    assert (tmp_path / "cargo/registry/cache/crate.crate").exists()


def test_pip_trimming_preserves_built_wheels(tmp_path):
    cache = load("cache_size")
    wheels = tmp_path / "toolchain/wheels"
    http = tmp_path / "toolchain/http-v2"
    wheels.mkdir(parents=True)
    http.mkdir()
    (wheels / "maturin.whl").write_bytes(b"wheel")
    (http / "download").write_bytes(b"x" * 100)
    assert cache.trim(tmp_path, limit=10) == 5
    assert (wheels / "maturin.whl").read_bytes() == b"wheel"


def test_small_cache_is_unchanged(tmp_path):
    cache = load("cache_size")
    target = tmp_path / "target/toolchain/debug"
    target.mkdir(parents=True)
    (target / "object").write_bytes(b"keep")
    assert cache.trim(tmp_path, limit=10) == 4
    assert (target / "object").read_bytes() == b"keep"


def test_cache_trimming_does_not_follow_symlinks(tmp_path):
    cache = load("cache_size")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep")
    root = tmp_path / "cache"
    root.mkdir()
    try:
        (root / "target").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Creating symlinks requires permission on this host")
    cache.trim(root, limit=0)
    assert (outside / "keep").read_text() == "keep"


def test_browser_enforcement_ignores_unrelated_skips_and_allows_xfail(
    tmp_path,
):
    report = tmp_path / "junit.xml"
    report.write_text("""<testsuites><testsuite>
      <testcase classname="tests.test_web_one.TestBrowser" name="works"/>
      <testcase classname="tests.test_web_two" name="known">
        <skipped type="pytest.xfail"/>
      </testcase>
      <testcase classname="tests.test_other" name="platform">
        <skipped type="pytest.skip"/>
      </testcase>
    </testsuite></testsuites>""")
    assert (
        load("check_browser_tests").check(
            report, ["tests/test_web_one.py", "tests/test_web_two.py"]
        )
        == 2
    )


@pytest.mark.parametrize(
    "problem", ["missing", "skip", "error", "failure", "empty"]
)
def test_browser_enforcement_rejects_missing_or_unsuccessful_tests(
    tmp_path, problem
):
    suite = ET.Element("testsuite")
    if problem != "empty":
        case = ET.SubElement(
            suite, "testcase", classname="tests.test_web_one", name="probe"
        )
        if problem in ("skip", "error", "failure"):
            ET.SubElement(case, "skipped" if problem == "skip" else problem)
    report = tmp_path / "junit.xml"
    ET.ElementTree(suite).write(report)
    modules = ["tests/test_web_one.py"]
    if problem == "missing":
        modules.append("tests/test_web_two.py")
    with pytest.raises(ValueError):
        load("check_browser_tests").check(report, modules)


@pytest.mark.parametrize("browser_skip", [False, True])
def test_browser_enforcement_reads_real_pytest_report(tmp_path, browser_skip):
    browser = tmp_path / "test_web_browser.py"
    browser.write_text(
        "import pytest\n"
        f"@pytest.mark.skipif({browser_skip}, reason='browser missing')\n"
        "def test_browser():\n"
        "    pass\n"
        "class TestBrowser:\n"
        "    @pytest.mark.xfail(reason='known issue')\n"
        "    def test_expected_failure(self):\n"
        "        assert False\n"
    )
    unrelated = tmp_path / "test_other.py"
    unrelated.write_text(
        "import pytest\n"
        "@pytest.mark.skip(reason='another platform')\n"
        "def test_platform():\n"
        "    pass\n"
    )
    report = tmp_path / "junit.xml"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-c",
            str(ROOT / ".github/tests/pytest.ini"),
            f"--rootdir={tmp_path}",
            f"--junitxml={report}",
            str(browser),
            str(unrelated),
        ],
        cwd=tmp_path,
        env=dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1"),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / ".github/scripts/check_browser_tests.py"),
            str(report),
            str(browser),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if browser_skip:
        assert result.returncode != 0
        assert "test_web_browser::test_browser: skipped" in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert "Verified 2 browser test results" in result.stdout


def test_freebsd_cache_uses_installed_packages_and_flags(monkeypatch):
    from types import SimpleNamespace

    compiler = load("compiler_fingerprint")
    monkeypatch.setattr(
        compiler.shutil, "which", lambda name: name in {"pkg", "rustc"}
    )
    packages = {"value": b"openssl35-3.5.7\npython314-3.14.0"}
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(
            stdout=packages["value"] if command[0] == "pkg" else b"compiler"
        )

    monkeypatch.setattr(compiler.subprocess, "run", run)
    monkeypatch.delenv("OPENSSL_DIR", raising=False)
    first = compiler.fingerprint()
    assert ["pkg", "query", "-a", "%n-%v"] in commands
    packages["value"] = b"openssl35-3.5.8\npython314-3.14.0"
    second = compiler.fingerprint()
    assert first != second
    monkeypatch.setenv("SODIUM_INSTALL", "system")
    assert compiler.fingerprint() != second


def test_freebsd_cache_is_copied_back_and_saved_after_the_build():
    steps = workflow("release")["jobs"]["binaries-freebsd"]["steps"]
    vm = next(step for step in steps if "freebsd-vm" in step.get("uses", ""))
    assert vm["with"]["copyback"] is True
    run = vm["with"]["run"]
    assert run.index("compiler_fingerprint.py") < run.index(
        "venv/bin/python get-pip.py"
    )
    assert 'PIP_CACHE_DIR="$PWD/.freebsd-pip-cache/$toolchain"' in run
    saves = [
        step for step in steps if "actions/cache/save" in step.get("uses", "")
    ]
    assert len(saves) == 1 and steps.index(saves[0]) > steps.index(vm)
    assert saves[0]["with"]["path"] == ".freebsd-pip-cache"
    assert "refs/heads/main" in saves[0]["if"]
