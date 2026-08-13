# Building the cronstable MSI

The MSI carries a PyInstaller one-directory build and registers the Windows
service. CI builds it in the `binaries-windows` job of
`.github/workflows/release.yml`; to build one locally:

```shell
# 1. A one-directory payload at dist/cronstable (see the spec's knob).
uv venv && uv pip install pyinstaller==6.21.0 .
CRONSTABLE_BUNDLE=onedir uv run pyinstaller --workpath build-onedir --noconfirm pyinstaller/cronstable.spec

# 2. WiX v6 as a .NET tool, plus the Util extension at the same version.
dotnet tool install --global wix --version 6.0.1
wix extension add --global WixToolset.Util.wixext/6.0.1

# 3. Build and validate. Payload must be an absolute path (it is a
#    compile-time preprocessor variable; a bind path resolves too late for
#    the Files harvest). -sw1149 is explained at the ServiceConfig element.
wix build packaging/msi/cronstable.wxs -arch x64 -d Version=0.0.1 \
  -d Payload="$(cygpath -w "$PWD/dist/cronstable")" \
  -ext WixToolset.Util.wixext -sw1149 -o dist/cronstable-test.msi
wix msi validate dist/cronstable-test.msi
```

The service values in `cronstable.wxs` mirror `cronstable service install`
and are fenced by `tests/test_msi_parity.py`; change either side only in
lockstep. User-facing behavior is documented in `wiki/Windows-MSI.md`.
