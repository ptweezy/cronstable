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

build() {
  wix build packaging/msi/setup.wxs \
    -arch "$wixarch" \
    -d Msi="$(cygpath -w "$(realpath "$msi")")" \
    -ext WixToolset.BootstrapperApplications.wixext \
    -pdbtype none \
    "$@"
}

# The setup window takes its icon only from the theme's Window/@IconFile:
# WixStdBA 6 runs in its own process, so its fallback of loading the
# engine's icon resource finds nothing and Windows draws the generic icon.
# The stock theme lives inside the extension, so build once to extract it,
# add the attribute, and build again with the result. setup.wxs carries
# the icon as a payload beside the theme. WiX 7 loads the bundle icon
# itself.
work="$(cygpath -m "$(mktemp -d)")"
trap 'rm -rf "$work"' EXIT
build -o "$work/stock.exe"
wix burn extract "$work/stock.exe" -oba "$work/ux"
sed -b 's/<Window /<Window IconFile="cronstable-pendulum.ico" /' "$work/ux/thm.xml" > "$work/thm.xml"
grep -q '<Window IconFile="cronstable-pendulum.ico" ' "$work/thm.xml"
build -bindvariable WixStdbaThemeXml="$work/thm.xml" -o "$out"
