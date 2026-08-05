"""The ``dev`` extra and requirements_dev.txt stay the same list.

CONTRIBUTING and the wiki document the two as equivalent installs
(``pip install -e ".[dev]"`` or ``pip install -r requirements_dev.txt``):
tox and CI install from the file while contributors typically install the
extra.  They drifted once: the extra lacked orjson/pynacl/zeroconf, so an
extra-based checkout ran the suite inside the exact importorskip blind
spot requirements_dev.txt's own comments warn about (optional-dep code
paths silently skipping).  This pins the two lists equal, name for name,
specifier for specifier, marker for marker.
"""

import os

import pytest

tomllib = pytest.importorskip("tomllib")  # py3.11+; the other cells enforce
requirements = pytest.importorskip("packaging.requirements")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _normalize(lines):
    """Requirement lines -> comparable (name, extras, specifier, marker)."""
    out = set()
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        req = requirements.Requirement(line)
        out.add(
            (
                req.name.lower(),
                tuple(sorted(req.extras)),
                str(req.specifier),
                str(req.marker) if req.marker else "",
            )
        )
    return out


def test_dev_extra_matches_requirements_dev():
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
        pyproject = tomllib.load(f)
    extra = pyproject["project"]["optional-dependencies"]["dev"]
    path = os.path.join(ROOT, "requirements_dev.txt")
    with open(path, encoding="utf-8") as f:
        file_lines = f.readlines()
    from_extra = _normalize(extra)
    from_file = _normalize(file_lines)
    assert from_extra == from_file, (
        "pyproject [project.optional-dependencies].dev and "
        "requirements_dev.txt have drifted apart:\n"
        "only in the extra: %r\nonly in the file: %r"
        % (
            sorted(from_extra - from_file),
            sorted(from_file - from_extra),
        )
    )
