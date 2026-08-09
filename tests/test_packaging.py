"""What pyproject says the wheel ships matches what is on disk.

Nothing in the repo or in CI ever imports the BUILT artifact: the suite runs
against the checkout, so ``[tool.setuptools].packages`` and
``[tool.setuptools.package-data]`` in pyproject.toml are hand-maintained lists
that no gate reads.  Add ``cronstable/foo/__init__.py``, or a new runtime data
file beside ``cronstable/web/index.html``, forget the corresponding pyproject
line, and every test still passes: the omission first shows up as an
ImportError or a missing dashboard in an installed user's terminal.

So this module rebuilds both lists from what is on disk (every directory
under ``cronstable/`` holding an ``__init__.py``; every non-Python file
under it) and diffs them against what pyproject declares.  That catches the
real failure mode without building anything.

Building a wheel here and reading its manifest would be the stronger check,
and is deliberately not done: setuptools_scm's ``version_file`` rewrites
``cronstable/version.py`` as a side effect of every build, so an in-test build
mutates the working tree it is measuring (and races a parallel test run).  The
CI ``dist`` job builds and ``twine check``s the real artifact; this module
covers the input those lists come from.

The last tests fence MANIFEST.in's sdist carve-out instead.  The sdist is
seeded by setuptools_scm's git file finder, so ``prune docs`` plus a few
explicit add-backs are all that keeps ~116 MB of website screenshots out of
it (an sdist that once blew past PyPI's 100 MiB per-file limit) while the
docs/ files the SHIPPED tests read still arrive.  Rename or forget one of
those add-backs and the sdist silently loses the file; the shipped suite
then fails only for whoever installed from it.  So beyond checking the
add-backs exist, the suite scrapes its own source for repo docs/ reads and
diffs that set against the add-back list.
"""

import fnmatch
import os
import posixpath
import re

import pytest

tomllib = pytest.importorskip("tomllib")  # py3.11+; the other cells enforce

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PKG_ROOT = os.path.join(ROOT, "cronstable")

#: files under cronstable/ that are code or build droppings, never shipped
#: data.  Everything else in the tree has to be claimed by a package-data
#: glob, which is the point of the walk.
_NOT_DATA = (".py", ".pyc", ".pyo", ".pyd", ".so")


def _pyproject():
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
        return tomllib.load(f)


def _source_packages():
    """{dotted name: absolute dir} for every real package under cronstable/.

    A "real package" is exactly what setuptools would import: a directory
    holding ``__init__.py``.  Namespace packages are not used here, so a
    directory without one (``cronstable/licenses``) is plain data belonging
    to its nearest packaged ancestor.
    """
    found = {}
    for dirpath, dirnames, filenames in os.walk(PKG_ROOT):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        if "__init__.py" not in filenames:
            continue
        rel = os.path.relpath(dirpath, ROOT)
        found[rel.replace(os.sep, ".")] = dirpath
    return found


def _owning_package(path, packages):
    """The package a data file belongs to: its deepest packaged ancestor."""
    best_name, best_dir = None, ""
    for name, directory in packages.items():
        prefix = directory + os.sep
        if path.startswith(prefix) and len(directory) > len(best_dir):
            best_name, best_dir = name, directory
    return best_name, best_dir


def _source_data_files():
    """[(package, path relative to it, absolute path)] for shipped data."""
    packages = _source_packages()
    out = []
    for dirpath, dirnames, filenames in os.walk(PKG_ROOT):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for filename in sorted(filenames):
            if filename.endswith(_NOT_DATA) or filename.startswith("."):
                continue
            full = os.path.join(dirpath, filename)
            package, directory = _owning_package(full, packages)
            assert package is not None, full  # cronstable/ is itself a package
            rel = os.path.relpath(full, directory).replace(os.sep, "/")
            out.append((package, rel, full))
    return out


def _glob_matches(relpath, pattern):
    """setuptools package-data glob semantics: ``*`` never crosses a ``/``."""
    parts = relpath.split("/")
    pattern_parts = pattern.replace(os.sep, "/").split("/")
    if len(parts) != len(pattern_parts):
        return False
    return all(
        fnmatch.fnmatchcase(part, glob)
        for part, glob in zip(parts, pattern_parts, strict=True)
    )


def test_declared_packages_match_the_source_tree():
    declared = set(_pyproject()["tool"]["setuptools"]["packages"])
    actual = set(_source_packages())
    assert declared == actual, (
        "pyproject [tool.setuptools].packages has drifted from the source "
        "tree. Declared but not on disk (the wheel build will fail): %r. "
        "On disk but undeclared (the wheel ships without them and importing "
        "cronstable breaks for installed users): %r"
        % (sorted(declared - actual), sorted(actual - declared))
    )


def test_every_data_file_under_the_package_is_declared_package_data():
    package_data = _pyproject()["tool"]["setuptools"]["package-data"]
    missing = []
    for package, rel, _full in _source_data_files():
        # the "" key, if it ever appears, applies to every package
        globs = list(package_data.get(package, ())) + list(
            package_data.get("", ())
        )
        if not any(_glob_matches(rel, glob) for glob in globs):
            missing.append("{}:{}".format(package, rel))
    assert not missing, (
        "these files live under cronstable/ but no [tool.setuptools."
        "package-data] glob claims them, so the wheel ships without them "
        "and the code that opens them raises FileNotFoundError once "
        "installed: %r" % (missing,)
    )


def test_no_package_data_glob_is_dead():
    # The mirror of the test above, and the half that catches a rename: a
    # glob that matches nothing is a line that used to ship a file and now
    # ships nothing.
    package_data = _pyproject()["tool"]["setuptools"]["package-data"]
    files = _source_data_files()
    dead = []
    for package, globs in package_data.items():
        for glob in globs:
            if not any(
                (package == "" or owner == package)
                and _glob_matches(rel, glob)
                for owner, rel, _full in files
            ):
                dead.append("{}: {}".format(package, glob))
    assert not dead, (
        "these [tool.setuptools.package-data] globs match nothing on disk, "
        "so whatever they used to ship is gone (a rename, or a file that "
        "moved out of its package): %r" % (dead,)
    )


def test_package_data_keys_are_declared_packages():
    # setuptools silently ignores package-data for a package it is not told
    # to build, so a key that outlives its packages entry ships nothing and
    # says nothing about it.
    config = _pyproject()["tool"]["setuptools"]
    declared = set(config["packages"])
    keys = {key for key in config["package-data"] if key != ""}
    assert keys <= declared, (
        "package-data names package(s) that [tool.setuptools].packages does "
        "not build, so their data is silently dropped from the wheel: %r"
        % (sorted(keys - declared),)
    )


def _manifest_add_backs(text):
    """The paths MANIFEST.in adds back INSIDE a pruned tree.

    An ``include`` of a path under a ``prune``d directory is load-bearing in
    a way an ordinary include is not: the prune is what makes the file's
    absence silent, so a typo or a rename there costs the sdist a file that
    the shipped test suite then cannot find.
    """
    pruned = []
    add_backs = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("prune "):
            pruned.append(line[len("prune ") :].strip().rstrip("/"))
        elif line.startswith("include "):
            path = line[len("include ") :].strip()
            top = posixpath.normpath(path).split("/")[0]
            if top in pruned:
                add_backs.append(path)
    return add_backs


def test_manifest_add_backs_inside_pruned_trees_still_exist():
    with open(os.path.join(ROOT, "MANIFEST.in"), encoding="utf-8") as f:
        add_backs = _manifest_add_backs(f.read())
    # The add-backs all live under the pruned docs/ tree; the docs-read scan
    # below keeps the list complete, this only checks the paths still exist.
    assert add_backs, (
        "MANIFEST.in no longer adds anything back inside a pruned tree; if "
        "the prune is still there, the shipped tests lost their fixtures."
    )
    gone = [
        path
        for path in add_backs
        if not os.path.exists(os.path.join(ROOT, path.replace("/", os.sep)))
    ]
    assert not gone, (
        "MANIFEST.in adds these paths back into the sdist but they do not "
        "exist, so the sdist ships without them (the prune swallows the "
        "rename silently) and the shipped test suite fails only for someone "
        "who installed from the sdist: %r" % (gone,)
    )


def test_manifest_add_back_scan_only_looks_inside_pruned_trees():
    # Fences the helper above, since the real MANIFEST.in is data and cannot
    # demonstrate its own failure: an include OUTSIDE a pruned tree (LICENSE,
    # README.md) is not the fragile case and must not be reported, while one
    # inside a pruned tree must be, whatever the tree is called.
    manifest = (
        "include LICENSE\n"
        "prune docs\n"
        "include docs/openapi.yaml\n"
        "prune private/\n"
        "include private/notice.txt\n"
    )
    assert _manifest_add_backs(manifest) == [
        "docs/openapi.yaml",
        "private/notice.txt",
    ]


def _docs_reads_in(source):
    """Repo-relative ``docs/...`` paths one test module opens, from source.

    Two idioms cover every real read today: ``os.path.join(ROOT, "docs",
    ...)`` and a pathlib ``... / "docs" / ...`` chain.  Both anchor on the
    repo root, which is what separates a read of the website tree from the
    state-store tests that happen to name a namespace directory "docs".  A
    test that reaches repo docs/ some third way must adopt one of these or
    teach this scan the new idiom; the self-check in the test below fails
    loudly if the scrape ever goes blind.
    """
    join_style = re.compile(
        r'os\.path\.join\(\s*ROOT,\s*"docs"((?:,\s*"[^"]+")+)\s*\)'
    )
    slash_style = re.compile(r'/\s*"docs"((?:\s*/\s*"[^"]+")+)')
    found = set()
    for pattern in (join_style, slash_style):
        for match in pattern.finditer(source):
            segments = re.findall(r'"([^"]+)"', match.group(1))
            found.add("/".join(["docs"] + segments))
    return found


def _docs_paths_read_by_tests():
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    found = set()
    for name in sorted(os.listdir(tests_dir)):
        if not name.endswith(".py"):
            continue
        with open(os.path.join(tests_dir, name), encoding="utf-8") as fh:
            found |= _docs_reads_in(fh.read())
    return found


def test_every_docs_file_the_suite_reads_is_added_back_into_the_sdist():
    # The failure this exists for: a test grows a new read under docs/ (or a
    # docs file is renamed), MANIFEST.in is not updated, and the shipped
    # suite starts raising FileNotFoundError, but only from an sdist install,
    # which no local run or CI lane ever is.
    with open(os.path.join(ROOT, "MANIFEST.in"), encoding="utf-8") as f:
        add_backs = set(_manifest_add_backs(f.read()))
    read_paths = _docs_paths_read_by_tests()
    # self-check: the two longest-standing reads must be visible to the
    # scrape, or the reading idiom changed and _docs_reads_in needs updating
    assert {"docs/openapi.yaml", "docs/demo/index.html"} <= read_paths, (
        "the docs-read scrape no longer sees the reads it was built on: %r"
        % (sorted(read_paths),)
    )
    missing = sorted(read_paths - add_backs)
    assert not missing, (
        "the shipped test suite reads these docs/ paths but MANIFEST.in does "
        "not add them back inside the pruned docs/ tree, so an sdist install "
        "ships a test suite that cannot find its fixtures: %r" % (missing,)
    )


def test_docs_read_scan_catches_both_idioms_and_skips_lookalikes():
    # Fences _docs_reads_in the same way the manifest scan is fenced above.
    # The positive examples are assembled with join() so the idiom never
    # appears contiguously in THIS file's source; the scan reads every test
    # module including this one, and a literal fixture here would leak into
    # the real scrape (it did, on first writing).
    source = "\n".join(
        (
            ", ".join(('SPEC = os.path.join(ROOT', '"docs"', '"o.yaml")')),
            " / ".join(('DEMO = P(__file__).parent.parent', '"docs"',
                        '"demo"', '"index.html"')),
            # a state-store namespace directory, not the website tree
            'ns_dir = os.path.join(backend.base, "docs")',
            # prune lists name the directory without reading anything from it
            'for name in ("docs", "wiki", "benchmarks"):',
        )
    )
    assert _docs_reads_in(source) == {
        "docs/o.yaml",
        "docs/demo/index.html",
    }
