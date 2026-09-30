#!/bin/sh
# Smoke-test a frozen binary for the optional extras its build env kept, so
# the POSIX release lanes share one gate. Windows runs the same checks in
# pwsh (binaries-windows).
#
# The push-plus-bonjour config validates only when PyNaCl and zeroconf both
# import inside the bundle, and a bundle whose build env kept cryptography
# must seal xwing (see install_extra.sh header). A soft row that dropped a
# broken source build has nothing to gate, and the install log said so.
#
# Usage: smoke_frozen.sh BINARY [hard|soft]
#   BINARY  the frozen executable, e.g. dist/cronstable
#   hard    always validate the push config: PyNaCl was a hard install
#   soft    validate it only when the build env kept PyNaCl (default)
#
# Optional env knob (the same contract as install_extra.sh):
#   PY  the build env's python command (default: python), e.g. "uv run python"
#
# $PY is left unquoted on purpose so "uv run python" word-splits.
set -eu

bin=$1
policy=${2:-soft}
PY="${PY:-python}"

if [ "$policy" = hard ] || $PY -c "import nacl" 2>/dev/null; then
    "$bin" --validate-config -c pyinstaller/smoke-push.yaml
fi
if $PY -c "import cryptography" 2>/dev/null; then
    suites=$("$bin" --sealable-suites)
    echo "sealable suites: $suites"
    echo "$suites" | grep -qx xwing || {
        echo "FAIL: the frozen binary cannot seal xwing" >&2
        exit 1
    }
fi
