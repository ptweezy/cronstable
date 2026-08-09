#!/bin/sh
# Install ONE optional extra into the current build env and prove it works,
# with the failure policy the calling lane chose. The release binary lanes
# bundle uvloop, pynacl and zeroconf in per-lane subsets and policies; this
# replaces their hand-spelled install+verify blocks so the choreography lives
# once. orjson stays on install_orjson.sh (it carries its own Rust
# source-build ladder and stays best-effort everywhere).
#
# Usage: install_extra.sh NAME SPEC hard|soft [FALLBACK]
#   NAME      verify_extra.py probe name, also the pip name to uninstall
#   SPEC      the full pip requirement, e.g. "uvloop>=X.Y". The version
#             floors stay spelled at the release.yml call sites, where
#             tests/test_extra_pins_parity.py checks them against
#             pyproject's extras: bump them together, never here.
#   hard      install and verify must both succeed, else exit nonzero. On
#             the lanes that pick this, a wheel always exists, so absence
#             means a broken build (a soft-fail would let a transient index
#             blip silently ship the artifact without the advertised extra).
#   soft      best-effort: a failed install just logs; a package that
#             installs but fails verify_extra.py (a source build, notably
#             under QEMU, can import yet be miscompiled) is uninstalled so a
#             broken extra is never frozen in. Always exits 0.
#   FALLBACK  what the binary uses instead, for the soft-path log lines
#             (default: "not bundled").
#
# Optional env knobs (the same contract as install_orjson.sh):
#   PIP        install command    (default: pip)              e.g. "uv pip"
#   PIPUNINST  uninstall command  (default: "$PIP uninstall -y")
#   PY         python command     (default: python)           e.g. "uv run python"
#
# Deliberately NOT `set -e`: every outcome is policy-handled inline. $PIP is
# left unquoted on purpose so "uv pip" word-splits into two tokens.
set -u

PIP="${PIP:-pip}"
PY="${PY:-python}"
PIPUNINST="${PIPUNINST:-$PIP uninstall -y}"
here=$(dirname "$0")

usage="usage: install_extra.sh NAME SPEC hard|soft [FALLBACK]"
name="${1:?$usage}"
spec="${2:?$usage}"
policy="${3:?$usage}"
note="${4:-not bundled}"

case "$policy" in
    hard|soft) ;;
    *) echo "install_extra.sh: unknown policy \"$policy\" ($usage)" >&2; exit 2 ;;
esac

if [ "$policy" = "hard" ]; then
    $PIP install "$spec" || exit 1
    $PY "$here/verify_extra.py" "$name" || exit 1
    exit 0
fi

# soft: install wherever a wheel exists or a source build succeeds; a failed
# install is not an error, the binary uses the fallback.
if $PIP install "$spec"; then
    :
else
    echo "$name unavailable; $note"
fi

# Prove the installed result actually works before it is frozen in, and drop
# it if not, so only a known-good build of the extra is ever shipped.
if $PY "$here/verify_extra.py" "$name"; then
    :
else
    # exit 2 is verify_extra.py's USAGE code (NAME is not one of its probe
    # names), not a probe failure. Folding the two together uninstalled a
    # perfectly healthy package over a typo and still exited 0, so the
    # binary shipped without the extra and nothing said so. A bad name is
    # a build bug: fail loudly on both policies.
    status=$?
    if [ "$status" -eq 2 ]; then
        echo "install_extra.sh: \"$name\" is not a verify_extra.py probe name" >&2
        exit 2
    fi
    echo "$name: verification failed; uninstalling, $note"
    $PIPUNINST "$name" || true
fi
exit 0
