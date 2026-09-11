import hashlib
import json
import platform
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

from acceptance.harness import Daemon


def pytest_addoption(parser):
    group = parser.getgroup("binary acceptance")
    group.addoption("--cronstable-bin", help="The frozen executable to test")
    group.addoption("--expected-version", help="Require this exact version")
    group.addoption(
        "--acceptance-output",
        default="acceptance-results",
        help="Retained logs, configurations, state, and artifact identity",
    )


@pytest.fixture(scope="session")
def artifact(request):
    argument = request.config.getoption("--cronstable-bin")
    if not argument:
        raise pytest.UsageError(
            "--cronstable-bin is required; no source fallback"
        )
    source = Path(argument).resolve(strict=True)
    # Reject a Python console script accidentally supplied from a venv. The
    # existing architecture/ABI gates remain responsible for the target ABI.
    with source.open("rb") as stream:
        magic = stream.read(4)
    if not (
        magic == b"\x7fELF"
        or magic[:2] == b"MZ"
        or magic
        in {
            b"\xfe\xed\xfa\xce",
            b"\xce\xfa\xed\xfe",
            b"\xfe\xed\xfa\xcf",
            b"\xcf\xfa\xed\xfe",
            b"\xca\xfe\xba\xbe",
            b"\xbe\xba\xfe\xca",
            b"\xca\xfe\xba\xbf",
            b"\xbf\xba\xfe\xca",
        }
    ):
        raise pytest.UsageError("--cronstable-bin must be a native executable")
    output = Path(request.config.getoption("--acceptance-output")).resolve()
    output = output / ("run-" + uuid.uuid4().hex[:12])
    output.mkdir(parents=True)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    (output / "artifact.json").write_text(
        json.dumps(
            {
                "binary": str(source),
                "sha256": digest,
                "platform": platform.platform(),
                "machine": platform.machine(),
                "expected_version": request.config.getoption(
                    "--expected-version"
                ),
                "layout": "onedir"
                if (source.parent / "_internal").is_dir()
                else "onefile",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nAcceptance evidence: {output}")
    # Jobs resolve the same executable as their parent via the bare CLI name.
    # Stage the standard onedir layout too; never copy source or a venv.
    with tempfile.TemporaryDirectory(prefix="cronstable acceptance ") as tmp:
        root = Path(tmp)
        bindir = root / "bin"
        bindir.mkdir()
        binary = bindir / (
            "cronstable.exe"
            if platform.system() == "Windows"
            else "cronstable"
        )
        shutil.copy2(source, binary)
        binary.chmod(binary.stat().st_mode | 0o111)
        if (source.parent / "_internal").is_dir():
            shutil.copytree(
                source.parent / "_internal",
                bindir / "_internal",
                symlinks=True,
            )
        yield binary, root, output


@pytest.fixture
def daemon(artifact, request):
    binary, root, output = artifact
    name = request.node.name
    work = root / (name + " café")
    work.mkdir()
    evidence = output / name
    evidence.mkdir()
    app = Daemon(binary, work, evidence)
    try:
        yield app
    finally:
        try:
            app.close()
        finally:
            shutil.copytree(work, evidence / "work")
            # Linux builders run as root in Docker; the host artifact uploader
            # must be able to read these synthetic state documents as well.
            for path in evidence.rglob("*"):
                path.chmod(0o755 if path.is_dir() else 0o644)


def pytest_sessionfinish(session, exitstatus):
    # An acceptance lane cannot go green by collecting nothing or skipping a
    # required behavior (including an accidental platform skip added later).
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if not session.testscollected or (
        reporter and reporter.stats.get("skipped")
    ):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
