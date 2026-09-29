# Build the WinGet installer around an MSI built by build_msi.sh.
# Usage: sh .github/scripts/build_setup.sh <amd64|amd64v3|arm64> <msi> <out>
set -euo pipefail

WIX_BOOTSTRAPPER_VERSION=6.0.2

arch="$1"
msi="$2"
out="$3"

export PATH="$PATH:$(cygpath -u "$USERPROFILE")/.dotnet/tools"
wix extension add --global "WixToolset.BootstrapperApplications.wixext/$WIX_BOOTSTRAPPER_VERSION"

case "$arch" in
  amd64|amd64v3) wixarch=x64 ;;
  arm64) wixarch=arm64 ;;
  *) echo "build_setup.sh: unknown arch '$arch'" >&2; exit 2 ;;
esac

wix build packaging/msi/setup.wxs \
  -arch "$wixarch" \
  -d Msi="$(cygpath -w "$(realpath "$msi")")" \
  -ext WixToolset.BootstrapperApplications.wixext \
  -pdbtype none \
  -o "$out"
