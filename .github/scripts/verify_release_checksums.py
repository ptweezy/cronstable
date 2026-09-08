"""Verify the prepared binary/distribution handoff before publication."""

import hashlib
import sys
from pathlib import Path


def verify(root):
    seen = set()
    for line in (root / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        name = name.lstrip("*")
        if name in seen or Path(name).name != name:
            raise ValueError("Duplicate or unsafe checksum filename")
        seen.add(name)
        candidates = [
            root / directory / name for directory in ("dist", "binaries")
        ]
        candidates = [p for p in candidates if p.is_file()]
        if len(candidates) != 1:
            raise ValueError(f"Missing or ambiguous prepared artifact: {name}")
        with candidates[0].open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != digest:
            raise ValueError(f"Prepared artifact checksum mismatch: {name}")
    if not seen:
        raise ValueError("Empty SHA256SUMS")


if __name__ == "__main__":
    verify(Path(sys.argv[1] if len(sys.argv) > 1 else "."))
