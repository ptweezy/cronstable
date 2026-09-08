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

The `sign-windows` job runs `.github/scripts/prepare_winget.ps1` on the signed
amd64 and arm64 MSIs while the other platform builds run. The script verifies
hashes and signatures, reads MSI product metadata, and scans each installer
and its extracted payload with current Microsoft Defender signatures.
`render_winget.py` generates manifests from that metadata, including each MSI's
`ProductCode`, and `winget validate` checks them before publication. The renderer
sets the installer type to `wix` and the scope to `machine` independently of the
manifest in winget-pkgs.

The `winget` job uses `verify_winget_release.py` to compare the published MSIs
and `SHA256SUMS` with the scanned hashes before submitting the saved manifests.
See `wiki/Contributing-and-Releasing.md` for steps to investigate validation
failures and resubmit a package.
