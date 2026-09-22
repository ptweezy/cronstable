"""Bound cache uploads and discard rebuildable data when requested."""

import argparse
import os
import shutil
from pathlib import Path

LIMIT = 512 * 1024**2


def size(root):
    return sum(
        p.stat().st_size
        for p in root.rglob("*")
        if p.is_file() and not p.is_symlink()
    )


def trim(root, limit=LIMIT):
    root = root.absolute()
    if root.is_symlink():
        raise ValueError("Cache root must be a directory, not a symlink")
    # Pip wheels survive HTTP cache removal. Cargo can extract crate sources
    # and rebuild each toolchain's target directory from its saved archives.
    candidates = [root / "http-v2", root / "http"]
    candidates += sorted(root.glob("*/http-v2"))
    candidates += sorted(root.glob("*/http"))
    candidates += [
        root / "cargo/registry/src",
        root / "cargo/git/checkouts",
    ]
    candidates += sorted(root.glob("target/*/debug"))
    candidates += sorted(
        root.glob("target/*"), key=lambda path: path.lstat().st_mtime
    )
    for path in candidates:
        if size(root) <= limit:
            break
        if not path.exists() or path.is_symlink():
            continue
        if any(p.is_symlink() for p in path.parents if p != root.parent):
            continue
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"Cache path escapes its root: {path}")
        if path.is_dir():
            shutil.rmtree(path)
            print(f"Removed rebuildable cache data: {path.relative_to(root)}")
    return size(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--trim", action="store_true")
    args = parser.parse_args()
    total = trim(args.root) if args.trim else size(args.root)
    fits = total <= LIMIT
    verdict = "saving" if fits else "over budget, skipping upload"
    print(f"Cache: {total} bytes; {verdict}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"fits={str(fits).lower()}\n")
