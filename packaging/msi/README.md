# Building the cronstable MSI

**`cronstable-windows-amd64v3.msi` is recommended for compatible x64 CPUs.**
It uses an optimized embedded Python runtime and requires the full x86-64-v3
feature set. **`cronstable-windows-amd64.msi` is the compatibility build**
for CPUs or VMs without v3, or when support is uncertain. Both use the same
service, install paths and upgrade identity.

The MSI carries a PyInstaller one-directory build and registers the Windows
service. CI builds it in the `binaries-windows` job of
`.github/workflows/release.yml`; to build one locally:

```shell
# 1. A one-directory payload at dist/cronstable (see the spec's knob).
uv venv && uv pip install pyinstaller==6.22.1 .
CRONSTABLE_BUNDLE=onedir uv run pyinstaller --noconfirm pyinstaller/cronstable.spec

# 2. Build and validate with the same script CI uses (pinned WiX v6
#    tool + Util extension, version normalization, wix build + wix msi
#    validate). The recipe lives only there: the gate build and the
#    signed rebuild must be the same code path.
sh .github/scripts/build_msi.sh amd64 0.0.1 dist/cronstable dist/cronstable-test.msi
```

The service values in `cronstable.wxs` mirror `cronstable service install`
and are fenced by `tests/test_msi_parity.py`; change either side only in
lockstep. User-facing behavior is documented in `wiki/Windows-MSI.md`.

WinGet uses the WiX Burn bundle in `setup.wxs` so that its administrator prompt
shows cronstable's icon. Build the setup executable after the MSI:

```shell
sh .github/scripts/build_setup.sh amd64 dist/cronstable-test.msi dist/cronstable-windows-amd64-setup.exe
```

The bundle takes its version from the embedded MSI. Its upgrade identity is
separate from the MSI's `UpgradeCode`; keep both stable across releases.
Setup executables support `amd64`, `amd64v3`, and `arm64`; WinGet uses the
baseline `amd64` build and the `arm64` build.
The bundle handles MSI and setup upgrades and provides the installed application
entry. Use the MSI directly for deployment properties such as `CONFIGDIR`.

The Windows artwork uses the full wordmark from the first frame of
`docs/img/logo-balance.webp`, with the pendulum upright.
`packaging/windows/cronstable-logo.png` preserves its proportions on a square
transparent canvas for the setup window. `packaging/windows/cronstable.ico`
contains the same image at 16, 20, 24, 32, 40, 48, 64, 128, and 256 pixels.
PyInstaller embeds the icon in both Windows executable layouts, the MSI uses
it for `ARPPRODUCTICON`, and the bundle uses it for its executable and elevation
helper. The artwork follows the
[brand asset policy](../../LICENSING.md#brand-assets).

The signing job builds each bundle from a signed MSI, detaches and signs the
Burn engine, reattaches it, and signs the complete bundle. Both signatures need
timestamps because the engine also handles repair and uninstall elevation.

The `sign-windows` job runs `.github/scripts/prepare_winget.ps1` on the signed
amd64 and arm64 MSIs and setup bundles while the other platform builds run. The
script verifies hashes and signatures, reads product metadata, and scans each
installer and its extracted payload with current Microsoft Defender signatures.
It checks that the embedded MSI matches the standalone MSI. The script also
verifies timestamped signatures and scans the signed engines supplied to
`wix burn reattach`, using the retained files in `EngineDirectory`. This follows
the [WiX bundle signing flow](https://docs.firegiant.com/wix/tools/signing/).
`render_winget.py` generates manifests from that metadata, and `winget validate`
checks them before publication.
The renderer sets the installer type to `burn` and the scope to `machine`. It
includes the bundle and MSI identities so WinGet can match either installed
package type.

The `winget` job uses `verify_winget_release.py` to compare the published installers
and `SHA256SUMS` with the scanned hashes before submitting the saved manifests.
See `wiki/Contributing-and-Releasing.md` for steps to investigate validation
failures and resubmit a package.
