#!/bin/sh
# Fail when a frozen binary lacks a module that cronstable imports only
# inside a function. The analysis follows an import statement there and
# misses a computed name, and a missing module fails nothing until a
# daemon reaches it at run time. The build env's PyInstaller lists the
# bundle's modules without running the binary, so the check works the same
# on every row, emulated ones included, and on hosts where cronstable never
# loads a cap backend (macOS, the BSDs).
#
# Usage: check_bundle.sh BINARY
#   BINARY  the frozen executable, e.g. dist/cronstable
#
# Optional env knob (the same contract as smoke_frozen.sh):
#   PY  the build env's python command (default: python), e.g. "uv run python"
#
# $PY is left unquoted on purpose so "uv run python" word-splits.
set -eu

bin=$1
PY="${PY:-python}"

# The cap backends limits._select_backend loads. A bundle without one
# leaves every resourceLimits cap on its platform unenforced.
modules="cronstable._cgroups cronstable._jobobjects"

listing=$($PY -m PyInstaller.utils.cliutils.archive_viewer -r -b "$bin")
# One name per line, indented; Windows Python ends the lines with CRLF.
listing=$(printf '%s\n' "$listing" | tr -d '\r' | sed 's/^[[:space:]]*//')
for module in $modules; do
    printf '%s\n' "$listing" | grep -Fqx "$module" || {
        echo "FAIL: the frozen binary does not bundle $module" >&2
        exit 1
    }
done
echo "bundled cap backends: $modules"
