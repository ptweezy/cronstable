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


#: Required development dependencies and the checks that need them.
#: Missing packages can silently skip tests or disable pytest checks.
#: Each package supports every platform where the suite runs.
_ALWAYS_INSTALLED = {
    "hypothesis": "the property-based tests (tests/test_properties_*.py)",
    "crontab": "the legacy-library differential in tests/test_cronexpr.py",
    "nacl": "the push sealing tests (requires_pynacl)",
    "zeroconf": "the mDNS discovery tests",
    "pytest_randomly": "the random test order that exposes order dependence",
    "pytest_timeout": "the per-test timeout",
}


@pytest.mark.parametrize("module", sorted(_ALWAYS_INSTALLED))
def test_skip_guarded_dev_dependencies_are_installed(module):
    assert importlib.util.find_spec(module) is not None, (
        "{} is missing from this environment, which silently disables {}. "
        "Check its line in pyproject.toml's dev extra (then regenerate "
        "build files).".format(module, _ALWAYS_INSTALLED[module])
    )


def _has_cryptography_wheel():
    # mirrors the marker on the cryptography lines of the dev extra
    machine = platform.machine()
    if sys.platform == "linux":
        return machine in ("x86_64", "aarch64")
    if sys.platform == "darwin":
        return machine == "arm64"
    if sys.platform == "win32":
        return machine == "AMD64"
    return False


def test_cryptography_is_installed_where_a_wheel_exists():
    # The mTLS, certificate-rotation and X-Wing tests skip without
    # cryptography (about 70 call sites). The dev extra installs it behind
    # a platform marker. If it stops matching, these tests could all skip
    # without failing CI.
    if not _has_cryptography_wheel():
        pytest.skip("no cryptography wheel for this platform")
    assert importlib.util.find_spec("cryptography") is not None, (
        "cryptography is missing from this environment, so the TLS and "
        "X-Wing tests silently skip. Check the cryptography lines in "
        "pyproject.toml's dev extra (then regenerate build files)."
    )
