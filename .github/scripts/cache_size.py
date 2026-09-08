"""Keep each new compiler/source cache below 512 MiB before uploading."""

import os
import sys
from pathlib import Path


def size(root):
    return sum(
        p.stat().st_size
        for p in root.rglob("*")
        if p.is_file() and not p.is_symlink()
    )


if __name__ == "__main__":
    total = size(Path(sys.argv[1]))
    fits = total <= 512 * 1024**2
    verdict = "saving" if fits else "over budget, skipping upload"
    print(f"Cache: {total} bytes; {verdict}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"fits={str(fits).lower()}\n")
