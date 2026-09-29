"""Check setup registration against incomplete Windows uninstall entries."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PWSH = shutil.which("pwsh")
pytestmark = pytest.mark.skipif(PWSH is None, reason="PowerShell required")

HARNESS = r"""
using namespace System.Management.Automation.Language

param($Script, $Entries)
$ErrorActionPreference = 'Stop'
$ErrorView = 'NormalView'
Set-StrictMode -Version Latest
$global:entries = Get-Content -Raw $Entries | ConvertFrom-Json
function Get-ItemProperty {
    param($Path)
    $expected = 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\' +
        'Uninstall\*'
    if ($Path -ne $expected) {
        throw "Unexpected registry path: $Path"
    }
    $global:entries
}
$tokens = $null
$errors = $null
$ast = [Parser]::ParseFile(
    $Script, [ref]$tokens, [ref]$errors
)
if ($errors.Count) { throw "PowerShell parse errors: $errors" }
$definition = $ast.Find({
    param($node)
    $node -is [FunctionDefinitionAst] -and
        $node.Name -eq 'Assert-BundleRegistration'
}, $true)
if ($null -eq $definition) { throw 'Missing registration assertion' }
. ([scriptblock]::Create($definition.Extent.Text))
Assert-BundleRegistration
"""

BUNDLE = {"DisplayName": "cronstable", "DisplayIcon": "cronstable.ico"}


@pytest.mark.parametrize(
    "entries,success",
    [
        pytest.param(
            [{}, {"DisplayName": "Other app"}, BUNDLE],
            True,
            id="missing-optional-properties",
        ),
        pytest.param(
            [{**BUNDLE, "SystemComponent": 0}], True, id="explicitly-visible"
        ),
        pytest.param(
            [BUNDLE, {"DisplayName": "cronstable", "SystemComponent": 1}],
            True,
            id="hidden-msi",
        ),
        pytest.param([BUNDLE, BUNDLE], False, id="duplicate-visible-entries"),
        pytest.param([], False, id="empty-registry"),
        pytest.param(
            [{"DisplayName": "Other app"}], False, id="missing-bundle"
        ),
        pytest.param(
            [{**BUNDLE, "SystemComponent": 1}], False, id="only-hidden"
        ),
        pytest.param(
            [{"DisplayName": "cronstable"}], False, id="missing-icon"
        ),
        pytest.param([{**BUNDLE, "DisplayIcon": ""}], False, id="empty-icon"),
    ],
)
def test_bundle_registration(tmp_path, entries, success):
    harness = tmp_path / "harness.ps1"
    harness.write_text(HARNESS, encoding="utf-8")
    data = tmp_path / "entries.json"
    data.write_text(json.dumps(entries), encoding="utf-8")
    result = subprocess.run(
        [
            PWSH,
            "-NoProfile",
            "-File",
            str(harness),
            str(ROOT / ".github/scripts/smoke_windows_setup.ps1"),
            str(data),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    output = result.stdout + result.stderr
    assert (result.returncode == 0) == success, output
    if not success:
        assert (
            "Expected one installed application entry with an icon" in output
        )
