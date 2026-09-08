# Building the cronstable MSI

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

The WinGet release job submits the published signed amd64 and arm64 MSIs.
`.github/scripts/prepare_winget.ps1` verifies release hashes and signatures,
reads MSI product metadata, and scans each installer and its extracted payload
with current Microsoft Defender signatures. `render_winget.py` generates the
manifests from that metadata, including each MSI's `ProductCode`. The renderer
sets the installer type to `wix` and the scope to `machine` independently of the
manifest in winget-pkgs. See `wiki/Contributing-and-Releasing.md` for steps to
investigate validation failures and resubmit a package.
