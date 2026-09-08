"""Check that published WinGet installers match the scanned release files."""

import hashlib
import json
import re
import sys
from pathlib import Path


def verify(version, metadata, assets):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError(f"Invalid release version: {version!r}")
    if set(metadata) != {"amd64", "arm64"}:
        raise ValueError("Expected scanned metadata for amd64 and arm64")
    assets = Path(assets)
    sums = {}
    for line in (assets / "SHA256SUMS").read_text("utf-8").splitlines():
        match = re.fullmatch(r"([a-fA-F0-9]{64})\s+\*?(\S+)", line)
        if match:
            sums[match[2]] = match[1].lower()
    for arch, item in metadata.items():
        name = f"cronstable-windows-{arch}.msi"
        if item["ProductVersion"] != version:
            raise ValueError(f"{name}: scanned version differs from release")
        expected = item["Sha256"].lower()
        digest = hashlib.sha256()
        with (assets / name).open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected or sums.get(name) != expected:
            raise ValueError(
                f"{name}: published bytes differ from scanned SHA256"
            )
        print(f"{name}: published SHA256 matches scanned file ({expected})")


if __name__ == "__main__":
    version, metadata_path, assets = sys.argv[1:]
    verify(
        version, json.loads(Path(metadata_path).read_text("utf-8-sig")), assets
    )
