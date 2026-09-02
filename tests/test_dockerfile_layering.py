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
appear below them.  The shared ``docker/install_orjson.sh`` sits between the
two: COPYd below the dependency layers (so editing it cannot invalidate their
cache) and invoked above the per-commit section.
"""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The canonical Dockerfile set is .github/docker-matrix.json: the `version`
# job emits it and both docker jobs (the build-only gate and docker-push)
# consume it via fromJSON, so an image exists in CI only if it has a row
# there. DOCKERFILES comes from the matrix so every check below runs over
# exactly what CI builds, and test_every_dockerfile_is_covered proves the
# matrix matches the disk.
with open(
    os.path.join(ROOT, ".github", "docker-matrix.json"), encoding="utf-8"
) as _fobj:
    DOCKERFILES = [row["dockerfile"] for row in json.load(_fobj)]
SCRIPT_COPY = "COPY docker/extract_deps.py /tmp/deps/extract_deps.py"
SCRIPT_INVOKE = (
    "/opt/venv/bin/python /tmp/deps/extract_deps.py /tmp/deps/pyproject.toml"
)
ORJSON_COPY = "COPY docker/install_orjson.sh /tmp/deps/install_orjson.sh"
ORJSON_INVOKE = "sh /tmp/deps/install_orjson.sh"


def _lines(relpath):
    with open(os.path.join(ROOT, relpath), encoding="utf-8") as fobj:
        return [line.rstrip("\n") for line in fobj]


def _index(lines, predicate, what, relpath):
    for i, line in enumerate(lines):
        if predicate(line):
            return i
    raise AssertionError("{}: no {}".format(relpath, what))


def test_every_dockerfile_is_covered():
    """The matrix set equals the on-disk set, in both directions.

    A Dockerfile added on disk without a .github/docker-matrix.json row
    would pass every layering check yet never build or publish (both
    docker jobs build only what the matrix lists); a matrix row whose
    file is gone would fail only mid-release. Either drift fails here.
    """
    found = {"Dockerfile"} | {
        "docker/" + name
        for name in os.listdir(os.path.join(ROOT, "docker"))
        if name.startswith("Dockerfile")
    }
    # a duplicated matrix row would hide inside the set comparison
    assert len(DOCKERFILES) == len(set(DOCKERFILES)), DOCKERFILES
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


@pytest.mark.parametrize("relpath", DOCKERFILES)
def test_orjson_install_goes_through_the_shared_script(relpath):
    # Eight pasted ~25-line orjson blocks were the same drift class the
    # extraction script killed, so the block lives once in
    # docker/install_orjson.sh; per-distro variance stays in each file as the
    # RUST_SETUP env string and any trailing package-manager cleanup. Two
    # placement rules matter: the script COPY sits BELOW the dependency
    # install (editing the script must never invalidate the expensive cached
    # dependency layers) and the invocation sits ABOVE the source COPY
    # (per-commit inputs stay at the bottom). The invocation must also spell
    # the "orjson>=X.Y" floor itself, because test_extra_pins_parity.py scans
    # the Dockerfiles, not the script, for hand-spelled pins to check against
    # pyproject.
    lines = _lines(relpath)
    deps_invoke = _index(
        lines,
        lambda ln: SCRIPT_INVOKE in ln,
        "invocation of the shared extraction script",
        relpath,
    )
    orjson_copy = _index(
        lines,
        lambda ln: ln == ORJSON_COPY,
        "COPY of the shared orjson install script",
        relpath,
    )
    orjson_invoke = _index(
        lines,
        lambda ln: (
            ORJSON_INVOKE in ln and re.search(r'"orjson>=[0-9][^"]*"', ln)
        ),
        "pin-carrying invocation of the shared orjson install script",
        relpath,
    )
    source_copy = _index(
        lines,
        lambda ln: ln.startswith("COPY . ."),
        "source COPY",
        relpath,
    )
    assert deps_invoke < orjson_copy < orjson_invoke < source_copy, (
        "{}: expected the dependency install, then the orjson script COPY, "
        "then its invocation, all above the source COPY".format(relpath)
    )
    assert not any("orjson_ok()" in ln for ln in lines), (
        "{}: the inline orjson block is back; edit docker/install_orjson.sh "
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
    assert any("DEPS_REFRESH" in ln for ln in lines[deps_arg + 1 :]), (
        "{}: ARG DEPS_REFRESH is declared but never referenced".format(relpath)
    )


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
    # the docker/ exclusion carves out exactly two files: the extraction
    # helper every dependency layer COPYs and the shared orjson installer.
    for carved in ("!docker/extract_deps.py", "!docker/install_orjson.sh"):
        assert carved in entries, (
            ".dockerignore no longer re-includes {}; every Dockerfile's "
            "COPY of it would fail".format(carved.lstrip("!"))
        )


def _extract_deps_module():
    """docker/extract_deps.py imported by path (it ships no package)."""
    pytest.importorskip("tomllib")  # 3.11+, as the script itself needs
    path = os.path.join(ROOT, "docker", "extract_deps.py")
    spec = importlib.util.spec_from_file_location("_extract_deps", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_extract_deps_emits_what_the_extras_pair_resolves(tmp_path):
    # The push-pq+discovery extras pair lives in exactly one place, so pin
    # what the script writes against a straight read of pyproject.toml, and
    # that everything it writes is echoed to stdout (the build-log
    # visibility the old `cat` provided). tomllib is 3.11+; every image venv
    # has it, only the 3.10 tox rows skip here.
    tomllib = pytest.importorskip("tomllib")
    module = _extract_deps_module()
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
    declared = (
        project["dependencies"]
        + project["optional-dependencies"]["push-pq"]
        + project["optional-dependencies"]["discovery"]
    )
    written = (
        (tmp_path / "requirements.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    # Every requirement but cryptography rides through verbatim, marker and
    # all, in pyproject's own order.
    others = [
        line
        for line in declared
        if module.requirement_name(line) != "cryptography"
    ]
    assert [
        line
        for line in written
        if module.requirement_name(line) != "cryptography"
    ] == others
    build_requires = tmp_path / "build-requires.txt"
    assert (
        build_requires.read_text(encoding="utf-8").splitlines()
        == data["build-system"]["requires"]
    )
    for line in written + data["build-system"]["requires"]:
        assert line in proc.stdout


@pytest.mark.parametrize(
    "target,keeps",
    [
        (("x86_64", 8, "glibc"), True),  # linux/amd64
        (("x86_64", 4, "glibc"), False),  # linux/386 on an amd64 host
        (("armv7l", 4, "musl"), False),  # the Alpine linux/arm/v7 row
        (("ppc64le", 8, "glibc"), True),  # manylinux_2_28 wheel
    ],
)
def test_extract_deps_writes_the_file_the_target_can_install(
    tmp_path, monkeypatch, target, keeps
):
    # The end of the same story, on the written file rather than the
    # decision: an image whose target has no wheel must get a
    # requirements.txt with no cryptography line at all, because pip reading
    # one there is exactly what turns the linux/386 rows red. Driven through
    # a patched target so every row is reachable from any dev machine.
    module = _extract_deps_module()
    monkeypatch.setattr(module, "detect_target", lambda: target)
    shutil.copy(
        os.path.join(ROOT, "pyproject.toml"), tmp_path / "pyproject.toml"
    )
    module.main(str(tmp_path / "pyproject.toml"))
    written = (
        (tmp_path / "requirements.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    crypto = [
        line
        for line in written
        if module.requirement_name(line) == "cryptography"
    ]
    assert crypto == (["cryptography>=48"] if keeps else [])
    # PyNaCl rides through untouched either way: it has wheels or a plain C
    # source build everywhere, so push keeps sealing x25519 on the rows that
    # lose the post-quantum suite.
    assert any(module.requirement_name(line) == "pynacl" for line in written)


def test_cryptography_line_keeps_its_floor_and_loses_its_marker():
    # Where the script says yes, the marker comes off (it is narrower than
    # the decision just made and would veto the armv7l/ppc64le images), and
    # the floor survives untouched: pyproject stays the one place it is
    # spelled, which is what tests/test_extra_pins_parity.py depends on.
    module = _extract_deps_module()
    line = "cryptography>=48; sys_platform == 'linux'"
    assert module.resolve_cryptography(line, ("x86_64", 8, "glibc")) == (
        "cryptography>=48"
    )
    assert module.resolve_cryptography(line, ("x86_64", 4, "glibc")) is None
    # Off Linux there is no image to resolve for, so pip and the marker keep
    # the decision.
    assert module.resolve_cryptography(line, None) == line


def test_a_wheelhouse_wheel_keeps_cryptography_where_pypi_has_none(
    tmp_path, monkeypatch
):
    # The pq-wheels job hands the image builds a cryptography wheel for the
    # platforms PyPI publishes none for. With one in the wheelhouse the
    # script keeps the line (marker stripped, as for a PyPI wheel); with an
    # empty wheelhouse, or one holding another arch's wheel, it drops it as
    # before. Only the machine is matched: the libc is decided by which
    # per-libc directory the Dockerfile COPYd.
    module = _extract_deps_module()
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    target = ("s390x", 8, "glibc")
    line = "cryptography>=48; sys_platform == 'linux'"
    assert module.resolve_cryptography(line, target, str(wheelhouse)) is None
    (wheelhouse / "cryptography-50.0.1-cp311-abi3-linux_riscv64.whl").touch()
    assert module.resolve_cryptography(line, target, str(wheelhouse)) is None
    (wheelhouse / "cryptography-50.0.1-cp311-abi3-linux_s390x.whl").touch()
    assert module.resolve_cryptography(line, target, str(wheelhouse)) == (
        "cryptography>=48"
    )
    # A 32-bit userland matches on the machine pip resolves under.
    (wheelhouse / "cryptography-50.0.1-cp311-abi3-linux_i686.whl").touch()
    assert module.resolve_cryptography(
        line, ("x86_64", 4, "glibc"), str(wheelhouse)
    ) == "cryptography>=48"
    # And main() threads the directory through from its second argument.
    monkeypatch.setattr(module, "detect_target", lambda: target)
    shutil.copy(
        os.path.join(ROOT, "pyproject.toml"), tmp_path / "pyproject.toml"
    )
    module.main(str(tmp_path / "pyproject.toml"), str(wheelhouse))
    written = (tmp_path / "requirements.txt").read_text(encoding="utf-8")
    assert "cryptography>=48" in written.splitlines()


def test_another_platforms_cryptography_line_never_reaches_an_image():
    # pyproject carries a second, capped cryptography line for Intel macOS
    # and 32-bit Windows. Stripping its marker on a Linux image would pin
    # the image below 49 for no reason, so the script must recognize it as
    # another OS's line and drop it, and keep the Linux line as before.
    module = _extract_deps_module()
    capped = (
        "cryptography>=48,<49; (sys_platform == 'darwin' and "
        "platform_machine == 'x86_64') or (sys_platform == 'win32' and "
        "platform_machine == 'x86')"
    )
    linux = (
        "cryptography>=48; (sys_platform == 'linux' and "
        "platform_machine == 'x86_64') or (sys_platform == 'darwin' and "
        "platform_machine == 'arm64')"
    )
    assert not module.marker_can_hold_on_linux(capped)
    assert module.marker_can_hold_on_linux(linux)
    # A line with no marker is everyone's.
    assert module.marker_can_hold_on_linux("cryptography>=48")
