"""v3 must change the runtime and reach every existing x64 delivery format."""

import hashlib
import importlib.util
import io
import json
import re
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from strictyaml.ruamel import YAML

ROOT = Path(__file__).resolve().parents[1]


def load(relative):
    spec = importlib.util.spec_from_file_location(
        Path(relative).stem, ROOT / relative
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workflow():
    return YAML(typ="safe").load(
        (ROOT / ".github/workflows/release.yml").read_text()
    )


def test_every_shipped_amd64_format_has_a_v3_counterpart():
    steps = workflow()["jobs"]["release"]["steps"]
    files = next(
        s["with"]["files"]
        for s in steps
        if s.get("name") == "Create GitHub Release"
    )
    assets = set(files.split())
    baseline = {p for p in assets if re.search(r"-amd64(?:$|[.-])", p)}
    assert (
        len(baseline) == 14
    )  # Both Linux libcs, native binaries and packages.
    assert {p.replace("-amd64", "-amd64v3") for p in baseline} <= assets
    assert not any("macos26" in p for p in assets)


def test_native_jobs_build_v3_and_select_the_new_interpreter():
    jobs = workflow()["jobs"]
    for name in (
        "macos",
        "windows",
        "freebsd",
        "openbsd",
        "netbsd",
        "illumos",
    ):
        job = jobs["binaries-" + name]
        matrix = job["strategy"]["matrix"]
        arches = set(matrix.get("arch", [])) | {
            r["arch"] for r in matrix.get("include", [])
        }
        assert {"amd64", "amd64v3"} <= arches, name
        steps = str(job["steps"])
        assert "docker/python_runtime.py --variant amd64v3" in steps, name
        if name in ("macos", "windows"):
            build = next(
                s for s in job["steps"] if s.get("name") == "Build binary"
            )
            assert "steps.runtime.outputs.python-path" in build["run"]
        else:
            assert "/tmp/cronstable-python-amd64v3/bin/python3" in steps


def test_glibc_pin_and_cpu_validation_match_the_shared_runtime():
    runtime = load("docker/python_runtime.py")
    job = workflow()["jobs"]["binaries"]
    row = next(
        r
        for r in job["strategy"]["matrix"]["include"]
        if r["arch"] == "amd64v3"
    )
    assert row["pysha"] == runtime.PBS_SHA["gnu"]
    assert (
        f"cpython-{runtime.VERSION}+{runtime.PBS_RELEASE}-x86_64_v3"
        in row["pyurl"]
    )
    assert row["platform"] == "linux/amd64"
    assert row["archspec"] == "elf:2:1:62"
    assert row["floor"] == "2.17"
    assert "docker/python_runtime.py --check" in str(job["steps"])


def test_v3_packages_keep_native_architecture_metadata():
    script = (ROOT / ".github/scripts/build_packages.sh").read_text()
    rows = re.search(r'ROWS="\n(.*?)\n"', script, re.S)[1]
    mappings = {r.split()[0]: r.split()[1:] for r in rows.splitlines()}
    assert (
        mappings["amd64v3"] == mappings["amd64"] == ["amd64", "amd64", "2.17"]
    )
    apk = re.search(r'APK_ROWS="\n(.*?)\n"', script, re.S)[1]
    assert dict(r.split() for r in apk.splitlines())["amd64v3"] == "x86_64"
    msi = (ROOT / ".github/scripts/build_msi.sh").read_text()
    assert "amd64|amd64v3) wixarch=x64" in msi


def test_docker_variants_have_separate_tags_and_caches():
    matrix = load(".github/scripts/docker_matrix.py")
    source = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    rows = matrix.expand(source)
    assert len(rows) == 2 * len(source)
    assert len({r["suffix"] for r in rows}) == len(rows)
    assert len({r["distro"] for r in rows}) == len(rows)
    for original in source:
        baseline = next(r for r in rows if r["distro"] == original["distro"])
        v3 = next(
            r for r in rows if r["distro"] == original["distro"] + "-amd64v3"
        )
        assert baseline == dict(original, python_variant="baseline")
        assert v3["dockerfile"] == baseline["dockerfile"]
        assert v3["platforms"] == "linux/amd64"
        assert v3["python_variant"] == "amd64v3"
    builds = YAML(typ="safe").load(
        (ROOT / ".github/workflows/build-docker.yml").read_text()
    )
    build = next(
        s
        for s in builds["jobs"]["build"]["steps"]
        if s.get("uses", "").startswith("docker/build-push-action@")
    )
    assert (
        "PYTHON_VARIANT=${{ matrix.python_variant }}"
        in build["with"]["build-args"]
    )
    assert "matrix.distro" in build["with"]["cache-from"]
    assert "matrix.platform_id" in build["with"]["cache-from"]
    push = workflow()["jobs"]["docker-push"]
    assert "docker-release-${{ matrix.distro }}" in str(push["steps"])
    assert "docker/build-push-action" not in str(push["steps"])


def test_docker_runtime_survives_the_final_stage():
    rows = json.loads((ROOT / ".github/docker-matrix.json").read_text())
    for row in rows:
        body = (ROOT / row["dockerfile"]).read_text()
        assert body.index("python_runtime.py --variant") < body.index(
            "-m venv /opt/venv"
        )
        assert (
            body.count('"$(cat /opt/python-runtime/python-path)" -m venv') == 2
        )
        final = body.rsplit("\nFROM ", 1)[1]
        assert (
            "COPY --from=builder /opt/python-runtime /opt/python-runtime"
            in final
        )
        assert (
            'RUN ["/opt/venv/bin/python", "-m", "cronstable", "--version"]'
            in final
        )


def test_baseline_runtime_does_not_download_or_replace_python(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    prefix = tmp_path / "runtime"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "docker/python_runtime.py"),
            "--prefix",
            str(prefix),
        ],
        check=True,
    )
    assert (prefix / "python-path").read_text().strip() == sys.executable
    assert {p.name for p in prefix.iterdir()} == {"python-path"}


def test_runtime_rejects_corrupt_download_without_retrying(
    monkeypatch, tmp_path
):
    runtime = load("docker/python_runtime.py")
    calls = []

    def fetch(*args, **kwargs):
        calls.append(args)
        return io.BytesIO(b"wrong bytes")

    monkeypatch.setattr(runtime.urllib.request, "urlopen", fetch)
    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        runtime.download(
            "https://example.invalid/python", "0" * 64, tmp_path / "archive"
        )
    assert len(calls) == 1


def test_runtime_retries_transport_then_verifies_bytes(monkeypatch, tmp_path):
    runtime = load("docker/python_runtime.py")
    calls = []
    payload = b"verified archive"

    def fetch(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise runtime.urllib.error.URLError("transient failure")
        return io.BytesIO(payload)

    monkeypatch.setattr(runtime.urllib.request, "urlopen", fetch)
    monkeypatch.setattr(runtime.time, "sleep", lambda seconds: None)
    archive = tmp_path / "archive"
    runtime.download(
        "https://example.invalid/python",
        hashlib.sha256(payload).hexdigest(),
        archive,
    )
    assert archive.read_bytes() == payload
    assert len(calls) == 2


def test_illumos_i86pc_uses_a_source_runtime(monkeypatch, tmp_path):
    runtime = load("docker/python_runtime.py")
    prefix = tmp_path / "runtime"
    python = prefix / "bin/python3"
    checked = []
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(runtime.platform, "machine", lambda: "i86pc")
    monkeypatch.setattr(runtime.platform, "system", lambda: "SunOS")
    monkeypatch.setattr(runtime, "build_source", lambda *args: python)
    monkeypatch.setattr(
        runtime, "verify", lambda p, **kwargs: checked.append(p)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["python_runtime.py", "--variant", "amd64v3", "--prefix", str(prefix)],
    )
    runtime.main()
    assert checked == [python]
    assert (prefix / "python-path").read_text().strip() == str(python)


def test_failed_runtime_verification_exposes_no_interpreter(
    monkeypatch, tmp_path
):
    runtime = load("docker/python_runtime.py")
    prefix = tmp_path / "runtime"
    monkeypatch.setattr(runtime.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(runtime.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        runtime, "install_pbs", lambda *args: prefix / "bin/python3"
    )

    def fail(*args, **kwargs):
        raise RuntimeError("runtime verification failed")

    monkeypatch.setattr(runtime, "verify", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        ["python_runtime.py", "--variant", "amd64v3", "--prefix", str(prefix)],
    )
    with pytest.raises(RuntimeError, match="verification failed"):
        runtime.main()
    assert not (prefix / "python-path").exists()


@pytest.fixture
def source_runtime(monkeypatch, tmp_path):
    runtime = load("docker/python_runtime.py")

    # Python 3.10.11 lacks tar extraction filters. Stub extraction so these
    # tests reach the build and install commands on every test interpreter.
    def extractall(path, *, filter):
        assert filter == "data"
        source = Path(path) / f"Python-{runtime.VERSION}"
        source.mkdir()
        (source / "configure").touch()

    monkeypatch.setattr(runtime, "download", lambda *args: None)
    monkeypatch.setattr(
        runtime,
        "tarfile",
        SimpleNamespace(
            open=lambda path: nullcontext(
                SimpleNamespace(extractall=extractall)
            )
        ),
    )
    monkeypatch.setattr(
        runtime,
        "os",
        SimpleNamespace(
            name="posix",
            environ={"GITHUB_OUTPUT": str(tmp_path / "github-output")},
            cpu_count=lambda: 8,
        ),
    )
    monkeypatch.setattr(runtime.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(runtime.shutil, "which", lambda name: "gmake")
    monkeypatch.setattr(runtime.sysconfig, "get_config_var", lambda name: "cc")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "python_runtime.py",
            "--variant",
            "amd64v3",
            "--prefix",
            str(tmp_path),
        ],
    )
    return runtime


@pytest.mark.parametrize("system", ["FreeBSD", "NetBSD"])
def test_source_install_selects_bytecode_workers_and_verifies_runtime(
    source_runtime, system, monkeypatch, tmp_path
):
    runtime = source_runtime
    commands = []
    checked = []
    monkeypatch.setattr(runtime.platform, "system", lambda: system)
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda cmd, **kwargs: commands.append((cmd, kwargs)),
    )
    monkeypatch.setattr(
        runtime, "verify", lambda python, **kwargs: checked.append(python)
    )

    runtime.main()

    configure, compile_, install = [cmd for cmd, _ in commands]
    assert configure[0] == "./configure"
    assert compile_ == ["gmake", "-j4"]
    if system == "FreeBSD":
        assert install == [
            "timeout",
            "-v",
            "-k",
            "30s",
            "15m",
            "gmake",
            "install",
            "COMPILEALL_OPTS=-j1",
        ]
    else:
        assert install == ["gmake", "install"]
    assert all(options["check"] for _, options in commands)
    assert all(
        options["cwd"] == tmp_path / "source" for _, options in commands
    )
    assert all(
        runtime.MARCH in options["env"]["CFLAGS"] for _, options in commands
    )
    python = tmp_path / "bin/python3"
    assert checked == [python]
    assert (tmp_path / "python-path").read_text().strip() == str(python)
    assert (
        tmp_path / "github-output"
    ).read_text() == f"python-path={python}\n"
    assert not (tmp_path / "source").exists()


@pytest.mark.parametrize("exit_code", [2, 124, 137])
def test_freebsd_install_failure_exposes_no_interpreter(
    source_runtime, exit_code, monkeypatch, tmp_path
):
    runtime = source_runtime
    checked = []
    monkeypatch.setattr(runtime.platform, "system", lambda: "FreeBSD")

    def run(cmd, **kwargs):
        if "install" in cmd:
            raise subprocess.CalledProcessError(exit_code, cmd)

    monkeypatch.setattr(runtime.subprocess, "run", run)
    monkeypatch.setattr(
        runtime, "verify", lambda python, **kwargs: checked.append(python)
    )

    with pytest.raises(subprocess.CalledProcessError) as error:
        runtime.main()

    assert error.value.returncode == exit_code
    assert checked == []
    assert not (tmp_path / "python-path").exists()
    assert not (tmp_path / "github-output").exists()
    assert (tmp_path / "source/configure").exists()
