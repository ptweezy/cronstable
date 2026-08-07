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
read ``pyproject.toml`` and the shared ``docker/extract_deps.py`` helper, and
the per-commit inputs (the source tree and the ``VERSION`` arg) may only
appear below them.
"""

import os
import shutil
import subprocess
import sys

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
SCRIPT_COPY = "COPY docker/extract_deps.py /tmp/deps/extract_deps.py"
SCRIPT_INVOKE = (
    "/opt/venv/bin/python /tmp/deps/extract_deps.py /tmp/deps/pyproject.toml"
)


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
def test_dependency_extraction_goes_through_the_shared_script(relpath):
    # Sixteen pasted tomllib one-liners were a drift class of their own, so
    # extraction lives once in docker/extract_deps.py: every file COPYs it
    # next to pyproject.toml and invokes it, and inline tomllib is banned.
    lines = _lines(relpath)
    deps_copy = _index(
        lines,
        lambda ln: ln.startswith("COPY pyproject.toml /tmp/deps/"),
        "pyproject-only COPY for the dependency layer",
        relpath,
    )
    script_copy = _index(
        lines,
        lambda ln: ln == SCRIPT_COPY,
        "COPY of the shared extraction script",
        relpath,
    )
    invoke = _index(
        lines,
        lambda ln: SCRIPT_INVOKE in ln,
        "invocation of the shared extraction script",
        relpath,
    )
    assert deps_copy < script_copy < invoke, (
        "{}: expected the pyproject COPY, then the script COPY, then its "
        "invocation, in that order".format(relpath)
    )
    assert not any("import tomllib" in ln for ln in lines), (
        "{}: inline tomllib extraction is back; edit docker/extract_deps.py "
        "instead".format(relpath)
    )


def test_retry_helper_is_identical_in_every_dockerfile():
    # retry() stays inline: its first use runs before any COPYd file's
    # interpreter exists, five runtime stages use it too (where a COPY would
    # ship a stray file and break the no-COPY-after-source pin above), and
    # inlining costs nothing as long as the copies cannot drift, which this
    # pins.
    variants = set()
    for relpath in DOCKERFILES:
        defs = [
            ln for ln in _lines(relpath) if ln.lstrip().startswith("retry()")
        ]
        assert defs, "{}: no retry() definition".format(relpath)
        variants.update(defs)
    assert len(variants) == 1, (
        "the retry() copies have drifted apart:\n"
        + "\n".join(sorted(variants))
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
    # the docker/ exclusion carves out exactly one file: the extraction
    # helper every dependency layer COPYs.
    assert "!docker/extract_deps.py" in entries, (
        ".dockerignore no longer re-includes docker/extract_deps.py; "
        "every deps layer COPY would fail"
    )


def test_extract_deps_emits_what_the_extras_pair_resolves(tmp_path):
    # The push+discovery extras pair now lives in exactly one place, so pin
    # what the script writes against a straight read of pyproject.toml, and
    # that everything it writes is echoed to stdout (the build-log
    # visibility the old `cat` provided). tomllib is 3.11+; every image venv
    # has it, only the 3.10 tox rows skip here.
    tomllib = pytest.importorskip("tomllib")
    shutil.copy(
        os.path.join(ROOT, "pyproject.toml"), tmp_path / "pyproject.toml"
    )
    proc = subprocess.run(
        [
            sys.executable,
            os.path.join(ROOT, "docker", "extract_deps.py"),
            str(tmp_path / "pyproject.toml"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as fobj:
        data = tomllib.load(fobj)
    project = data["project"]
    expected = (
        project["dependencies"]
        + project["optional-dependencies"]["push"]
        + project["optional-dependencies"]["discovery"]
    )
    written = tmp_path / "requirements.txt"
    assert written.read_text(encoding="utf-8").splitlines() == expected
    build_requires = tmp_path / "build-requires.txt"
    assert (
        build_requires.read_text(encoding="utf-8").splitlines()
        == data["build-system"]["requires"]
    )
    for line in expected + data["build-system"]["requires"]:
        assert line in proc.stdout
