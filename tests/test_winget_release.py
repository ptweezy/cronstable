"""Verify that published MSIs and setup bundles match the scanned files."""

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "verify_winget_release", ROOT / ".github/scripts/verify_winget_release.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows profile paths")
def test_windows_first_run_smoke_checks_real_cli(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location(
        "smoke_windows_first_run",
        ROOT / ".github/scripts/smoke_windows_first_run.py",
    )
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
    monkeypatch.setattr(smoke.tempfile, "tempdir", str(tmp_path))
    smoke.check(
        [
            sys.executable,
            "-c",
            f"import sys, runpy; sys.path.insert(0, {str(ROOT)!r}); "
            "runpy.run_module('cronstable', run_name='__main__')",
        ]
    )


@pytest.fixture
def published(tmp_path):
    metadata = {}
    sums = []
    for arch in ("amd64", "arm64"):
        metadata[arch] = {"ProductVersion": "1.2.50"}
        for suffix, field in (
            (".msi", "Sha256"),
            ("-setup.exe", "BundleSha256"),
        ):
            name = f"cronstable-windows-{arch}{suffix}"
            content = f"signed {name}".encode()
            (tmp_path / name).write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()
            metadata[arch][field] = digest.upper()
            sums.append(f"{digest}  {name}\n")
    (tmp_path / "SHA256SUMS").write_text("".join(sums), encoding="utf-8")
    return metadata, tmp_path


def test_published_files_match_scan(published):
    metadata, assets = published
    verifier.verify("1.2.50", metadata, assets)


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
@pytest.mark.parametrize(
    "suffix,field", [(".msi", "Sha256"), ("-setup.exe", "BundleSha256")]
)
def test_replaced_asset_blocks_submission_even_with_updated_sums(
    published, arch, suffix, field
):
    metadata, assets = published
    name = f"cronstable-windows-{arch}{suffix}"
    content = b"rebuilt installer with a different signature"
    (assets / name).write_bytes(content)
    sums = assets / "SHA256SUMS"
    sums.write_text(
        sums.read_text("utf-8").replace(
            metadata[arch][field].lower(),
            hashlib.sha256(content).hexdigest(),
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="differ from scanned SHA256"):
        verifier.verify("1.2.50", metadata, assets)


def test_published_sums_must_match_scan(published):
    metadata, assets = published
    (assets / "SHA256SUMS").write_text(
        "0" * 64 + "  cronstable-windows-amd64.msi\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="differ from scanned SHA256"):
        verifier.verify("1.2.50", metadata, assets)


@pytest.mark.parametrize("suffix", [".msi", "-setup.exe"])
def test_missing_asset_blocks_submission(published, suffix):
    metadata, assets = published
    (assets / f"cronstable-windows-arm64{suffix}").unlink()
    with pytest.raises(FileNotFoundError):
        verifier.verify("1.2.50", metadata, assets)


def test_scan_from_another_version_blocks_submission(published):
    metadata, assets = published
    with pytest.raises(ValueError, match="scanned version differs"):
        verifier.verify("1.2.51", metadata, assets)


def test_missing_scan_architecture_blocks_submission(published):
    metadata, assets = published
    del metadata["arm64"]
    with pytest.raises(ValueError, match="Expected scanned metadata"):
        verifier.verify("1.2.50", metadata, assets)


def test_invalid_release_version_blocks_submission(published):
    metadata, assets = published
    with pytest.raises(ValueError, match="Invalid release version"):
        verifier.verify("../1.2.50", metadata, assets)
