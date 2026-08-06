"""The ``dev`` extra and requirements_dev.txt stay the same list.

CONTRIBUTING and the wiki document the two as equivalent installs
(``pip install -e ".[dev]"`` or ``pip install -r requirements_dev.txt``):
tox and CI install from the file while contributors typically install the
extra.  They drifted once: the extra lacked orjson/pynacl/zeroconf, so an
extra-based checkout ran the suite inside the exact importorskip blind
spot requirements_dev.txt's own comments warn about (optional-dep code
paths silently skipping).  This pins the two lists equal, name for name,
specifier for specifier, marker for marker.

The second test here is the other half of that contract: two equal lists
are still worthless if the environment running the suite does not have the
package installed.  It asserts the playwright line took effect wherever a
wheel exists, so the browser-backed differential cannot quietly go back to
skipping everywhere.
"""

import importlib.util
import os
import platform
import sys

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


def test_playwright_is_installed_where_a_wheel_exists():
    # tests/test_web_engine_parity.py replays the whole cron golden corpus
    # through the dashboard's client-side schedule engine in a real Chromium
    # and through the daemon's, and fails on any disagreement.  It is
    # importorskip-guarded, and playwright was in neither dependency list, so
    # it skipped everywhere and the dashboard's second implementation of the
    # schedule dialect shipped with nothing comparing it to the first.
    # requirements_dev.txt now installs playwright wherever a wheel exists;
    # this fails loudly if that line is dropped or its marker stops matching,
    # instead of degrading back to a silent skip (the same way the orjson
    # guard in tests/test_json_portability.py works).
    #
    # It deliberately says nothing about the browser: `playwright install
    # chromium` is a separate step pip cannot perform, and the differential
    # skips (never errors) without it, so this test only claims the half the
    # dependency lists control.
    if sys.platform == "win32" and platform.machine().upper() == "ARM64":
        pytest.skip("no playwright wheel for win-arm64, and no sdist either")
    assert importlib.util.find_spec("playwright") is not None, (
        "playwright is missing from this environment, so the client/daemon "
        "cron-engine differential silently skips. Check the playwright line "
        "in requirements_dev.txt."
    )
