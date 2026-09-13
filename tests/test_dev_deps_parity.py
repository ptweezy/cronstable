"""Optional test dependencies must actually be installed on supported hosts."""

import importlib.util
import platform
import sys

import pytest


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
        "in pyproject.toml (then regenerate build files)."
    )
