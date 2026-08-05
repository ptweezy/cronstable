"""All eight Dockerfiles keep the dependency layers off the source tree.

The eight images (the root Dockerfile plus the seven distro variants under
docker/) are hand-mirrored: the same builder stage rewritten per package
manager.  They all once did ``WORKDIR /src`` then ``COPY . .`` and only
afterwards installed the toolchain and every third-party dependency.  A COPY
layer's cache key is the digest of the copied content, so any change anywhere
in the build context invalidated that COPY and every RUN beneath it, and all
36 emulated platform builds redid the whole dependency compile on a commit
that touched only a test file or a screenshot.

The split that fixed it is an ordering invariant, and an ordering invariant in
eight parallel files is exactly the shape this repo has been bitten by before.
So this pins the order rather than the wording: the dependency layers may only
read ``pyproject.toml``, and the per-commit inputs (the source tree and the
``VERSION`` arg) may only appear below them.
"""

import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCKERFILES = [
    "Dockerfile",
    "docker/Dockerfile.alpine",
    "docker/Dockerfile.amazonlinux",
    "docker/Dockerfile.distroless",
    "docker/Dockerfile.fedora",
    "docker/Dockerfile.opensuse",
    "docker/Dockerfile.rhel",
    "docker/Dockerfile.ubuntu",
]


def _lines(relpath):
    with open(os.path.join(ROOT, relpath), encoding="utf-8") as fobj:
        return [line.rstrip("\n") for line in fobj]


def _index(lines, predicate, what, relpath):
    for i, line in enumerate(lines):
        if predicate(line):
            return i
    raise AssertionError("{}: no {}".format(relpath, what))


def test_every_dockerfile_is_covered():
    """The list above is the whole set, so a ninth image cannot slip past."""
    found = {"Dockerfile"} | {
        "docker/" + name
        for name in os.listdir(os.path.join(ROOT, "docker"))
        if name.startswith("Dockerfile")
    }
    assert found == set(DOCKERFILES)


@pytest.mark.parametrize("relpath", DOCKERFILES)
def test_dependency_layers_read_only_pyproject(relpath):
    lines = _lines(relpath)
    deps_copy = _index(
        lines,
        lambda ln: ln.startswith("COPY pyproject.toml /tmp/deps/"),
        "pyproject-only COPY for the dependency layer",
        relpath,
    )
    source_copy = _index(
        lines,
        lambda ln: ln.startswith("COPY . ."),
        "source COPY",
        relpath,
    )
    # the whole point: dependencies resolve from pyproject.toml alone, above
    # the layer that carries the rest of the tree.
    assert deps_copy < source_copy, (
        "{}: the source COPY must sit BELOW the dependency layers, else "
        "every dependency rebuilds on every commit".format(relpath)
    )
    # and the source tree is the LAST thing copied in the builder stage, so
    # nothing expensive can drift back underneath it.
    later = [
        ln
        for ln in lines[source_copy + 1 :]
        if ln.startswith("COPY ") and not ln.startswith("COPY --from=")
    ]
    assert later == [], "{}: unexpected COPY after the source copy: {}".format(
        relpath, later
    )


@pytest.mark.parametrize("relpath", DOCKERFILES)
def test_per_commit_version_arg_stays_below_the_dependency_layers(relpath):
    # An ARG referenced in a layer joins that layer's cache key. VERSION
    # changes on every release (and is empty otherwise), so declaring it above
    # the dependency install would defeat the split as surely as copying the
    # source would.
    lines = _lines(relpath)
    version_arg = _index(
        lines,
        lambda ln: ln.startswith("ARG VERSION"),
        "ARG VERSION",
        relpath,
    )
    deps_copy = _index(
        lines,
        lambda ln: ln.startswith("COPY pyproject.toml /tmp/deps/"),
        "pyproject-only COPY for the dependency layer",
        relpath,
    )
    assert version_arg > deps_copy, (
        "{}: ARG VERSION must be declared below the dependency layers".format(
            relpath
        )
    )


@pytest.mark.parametrize("relpath", DOCKERFILES)
def test_release_cache_bust_arg_is_present(relpath):
    # DEPS_REFRESH is how a release deliberately MISSES the cached dependency
    # layers and re-resolves everything fresh from the index; without it in
    # every file, a release would ship whatever the cache last held.
    lines = _lines(relpath)
    deps_arg = _index(
        lines,
        lambda ln: ln.startswith("ARG DEPS_REFRESH"),
        "ARG DEPS_REFRESH",
        relpath,
    )
    # referenced above the dependency install, or declaring it does nothing
    assert any(
        "DEPS_REFRESH" in ln for ln in lines[deps_arg + 1 :]
    ), "{}: ARG DEPS_REFRESH is declared but never referenced".format(relpath)


def test_dockerignore_excludes_the_heavy_untouched_trees():
    # docs/ alone is tens of MB of screenshots. None of it reaches the wheel
    # (pyproject's `packages` lists three directories, all under cronstable/),
    # so it is pure build-context upload and pure cache-key churn.
    with open(os.path.join(ROOT, ".dockerignore"), encoding="utf-8") as fobj:
        entries = {
            line.strip()
            for line in fobj
            if line.strip() and not line.startswith("#")
        }
    for name in ("docs", "wiki", "benchmarks", "packaging", "pro", "tests"):
        assert name in entries, ".dockerignore no longer excludes " + name
    # ... and the two that MUST stay, because pyproject.toml reads them at
    # build time and setuptools_scm reads .git.
    for name in ("README.md", "LICENSE", ".git"):
        assert name not in entries, ".dockerignore must not exclude " + name
