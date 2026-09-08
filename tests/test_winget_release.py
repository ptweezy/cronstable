"""Verify the link between scanned MSIs and published release assets."""

import hashlib
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "verify_winget_release", ROOT / ".github/scripts/verify_winget_release.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.fixture
def published(tmp_path):
    metadata = {}
    sums = []
    for arch in ("amd64", "arm64"):
        name = f"cronstable-windows-{arch}.msi"
        content = f"signed MSI for {arch}".encode()
        (tmp_path / name).write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        metadata[arch] = {"ProductVersion": "1.2.50", "Sha256": digest.upper()}
        sums.append(f"{digest}  {name}\n")
    (tmp_path / "SHA256SUMS").write_text("".join(sums), encoding="utf-8")
    return metadata, tmp_path


def test_published_files_match_scan(published):
    metadata, assets = published
    verifier.verify("1.2.50", metadata, assets)


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_replaced_asset_blocks_submission_even_with_updated_sums(
    published, arch
):
    metadata, assets = published
    name = f"cronstable-windows-{arch}.msi"
    content = b"rebuilt MSI with a different signature"
    (assets / name).write_bytes(content)
    sums = assets / "SHA256SUMS"
    sums.write_text(
        sums.read_text("utf-8").replace(
            metadata[arch]["Sha256"].lower(),
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


def test_missing_asset_blocks_submission(published):
    metadata, assets = published
    (assets / "cronstable-windows-arm64.msi").unlink()
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
