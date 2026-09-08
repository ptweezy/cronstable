"""Require every declared artifact pattern to contain a nonempty file."""

import glob
import sys
from pathlib import Path


def check(patterns):
    for pattern in patterns:
        paths = [
            Path(p)
            for p in glob.glob(pattern, recursive=True)
            if Path(p).is_file()
        ]
        if not paths or any(p.stat().st_size == 0 for p in paths):
            raise ValueError(f"Missing or empty artifact: {pattern}")


if __name__ == "__main__":
    check(sys.argv[1:])
