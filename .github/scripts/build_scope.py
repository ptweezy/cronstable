"""Decide whether a CI run needs the build fan-out.

The binaries, cryptography wheels, Docker images, perf and nix jobs run
unless every path a branch or pull request changes is documentation. The
tests run either way. A release, a dispatch and a push to main always
build, and so does a run whose changed paths git can't list.

Reads EVENT, REF, RELEASE and HEAD_SHA from the environment and writes
build=true or build=false to $GITHUB_OUTPUT.
"""

import os
import subprocess

DOC_DIRS = ("wiki/", "docs/")
# flake.nix reads the version from HISTORY.md.
BUILD_INPUTS = {"HISTORY.md"}


def is_doc(path):
    if path in BUILD_INPUTS:
        return False
    return path.startswith(DOC_DIRS) or path.endswith(".md")


def git(*args):
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True
    ).stdout


def changed_paths(event, head):
    """Paths the run changes against its base, or None when unknown.

    A pull request runs on GitHub's merge commit, whose first parent is the
    base branch tip. A branch push compares with its merge base on main, so
    a docs-only push still builds the code changes of earlier pushes.
    --no-renames lists both sides of a rename.
    """
    try:
        if event == "pull_request":
            base = f"{head}^1"
        elif event == "push":
            base = git("merge-base", "origin/main", head).strip()
        else:
            return None
        out = git("diff", "--name-only", "--no-renames", base, head)
    except (OSError, subprocess.CalledProcessError):
        return None
    return [line for line in out.splitlines() if line]


def always_builds(event, ref, release):
    return (
        release
        or event == "workflow_dispatch"
        or (event == "push" and ref == "refs/heads/main")
    )


def needs_build(event, ref, release, paths):
    if always_builds(event, ref, release) or paths is None:
        return True
    return not all(is_doc(path) for path in paths)


def main():
    event = os.environ["EVENT"]
    ref = os.environ["REF"]
    release = os.environ["RELEASE"] == "true"
    paths = None
    if not always_builds(event, ref, release):
        paths = changed_paths(event, os.environ["HEAD_SHA"])
    build = needs_build(event, ref, release, paths)
    if paths is not None and not build:
        print(
            f"::notice::Skipping the build fan-out: all {len(paths)} "
            "changed paths are documentation."
        )
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write(f"build={str(build).lower()}\n")


if __name__ == "__main__":
    main()
