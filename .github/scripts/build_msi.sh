# The ONE recipe that builds a cronstable MSI in CI, run by both
# release.yml jobs: binaries-windows' gate build (whose msiexec smoke
# proves the .wxs semantics) and sign-windows' rebuild from the signed
# payload. One shared recipe keeps the signed rebuild identical to the
# MSI the gate proved; tests/test_ci_fences.py pins both call sites and
# the pins below.
#
# Executed (git bash on the Windows runners), not sourced:
#
#     sh .github/scripts/build_msi.sh <arch> <version> <payload> <out> [bump-patch]
#
#     arch      amd64 | arm64 | i686 (the release asset spelling)
#     version   full version; a setuptools_scm dev/local suffix is
#               stripped here (ProductVersion must be numeric X.Y.Z)
#     payload   the PyInstaller one-directory build (dist/cronstable)
#     out       the .msi to write
#     bump-patch  optional: build with the patch version + 1, so the
#               upgrade smoke gets an upgrade-eligible twin without a
#               second copy of the version normalization.
set -euo pipefail

# WiX is not preinstalled on windows-latest or windows-11-arm; the .NET
# tool install is the one path identical on both runners. The Util
# extension major must match the tool major. (WiX v6's Open Source
# Maintenance Fee does not affect open-source CI use.) MSI assembly is
# not compilation, so -arch is metadata and one runner shape serves
# both.
WIX_TOOL_VERSION=6.0.2
WIX_UTIL_VERSION=6.0.2

arch="$1"
version="$2"
payload="$3"
out="$4"

export PATH="$PATH:$(cygpath -u "$USERPROFILE")/.dotnet/tools"
# install fails when the tool is already present (a second call in the
# same job, e.g. the upgrade twin); update is idempotent. Same for the
# extension: an already-added one must not fail the build, while a
# missing one still does (wix build errors on the util namespace).
dotnet tool install --global wix --version "$WIX_TOOL_VERSION" 2>/dev/null \
  || dotnet tool update --global wix --version "$WIX_TOOL_VERSION"
wix extension add --global "WixToolset.Util.wixext/$WIX_UTIL_VERSION" \
  || wix extension list --global | grep -q "WixToolset.Util.wixext"

# ProductVersion must be numeric X.Y.Z (MSI caps 255.255.65535); strip
# the setuptools_scm dev suffix on non-release builds, whose MSIs are
# 7-day CI artifacts and never published.
msiver="$(printf '%s' "$version" | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+')"
if [ "${5:-}" = "bump-patch" ]; then
  msiver="${msiver%.*}.$((${msiver##*.} + 1))"
fi

# i686 builds an x86 package. On a 64-bit host Windows Installer then
# redirects it to "Program Files (x86)" and the WOW6432Node registry view;
# on a 32-bit host, the only place this artifact is needed, both resolve to
# the plain paths. The .wxs needs no arch conditionals for that: it uses
# ProgramFiles6432Folder, and its RegistrySearch reads back through the same
# view its RegistryValue wrote.
case "$arch" in
  amd64) wixarch=x64 ;;
  arm64) wixarch=arm64 ;;
  i686)  wixarch=x86 ;;
  *) echo "build_msi.sh: unknown arch '$arch'" >&2; exit 2 ;;
esac

# -sw1149: WiX warns on every native ServiceConfig element; the rationale
# for keeping it lives at the element in the .wxs.
# -pdbtype none: no .wixpdb beside the .msi. sign-windows verifies a
# signature on everything in its output folder, and a debug pdb is not
# a signable file type.
wix build packaging/msi/cronstable.wxs \
  -arch "$wixarch" \
  -d Version="$msiver" \
  -d Payload="$(cygpath -w "$(realpath "$payload")")" \
  -ext WixToolset.Util.wixext \
  -sw1149 \
  -pdbtype none \
  -o "$out"
# ICE validation is a separate command in WiX v4+ (build does not run
# it). Errors fail the job.
wix msi validate "$out"
