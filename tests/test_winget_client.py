"""Run the CI validation step with simulated WinGet installations."""

import shutil
import subprocess
from pathlib import Path

import pytest
from strictyaml.ruamel import YAML

ROOT = Path(__file__).resolve().parents[1]
PWSH = shutil.which("pwsh")
pytestmark = pytest.mark.skipif(PWSH is None, reason="PowerShell required")

HARNESS = r"""
param($Script, $Case)
$ErrorActionPreference = 'Stop'
$global:case = $Case
$global:clientVersion = 'v1.11.430'
$global:installed = $Case -ne 'missing'
$global:repaired = $false
function Install-Module {
    param($Name, [switch]$Force, $Repository, $Scope)
    if ($Name -ne 'Microsoft.WinGet.Client' -or
        $Repository -ne 'PSGallery' -or $Scope -ne 'CurrentUser') {
        throw 'Unexpected module installation'
    }
    if ($global:case -eq 'install-error') { throw 'Module install failed' }
}
function Repair-WinGetPackageManager {
    param($Version, [switch]$Force)
    if ($Version -ne '1.29.290' -or -not $Force) {
        throw 'Expected pinned WinGet repair'
    }
    if ($global:case -eq 'repair-error') { throw 'Client repair failed' }
    $global:repaired = $true
    $global:installed = $true
    if ($global:case -ne 'stale') { $global:clientVersion = 'v' + $Version }
    Write-Host "REPAIRED $Version"
}
function Get-Command {
    param($Name, $ErrorAction)
    if ($Name -ne 'winget.exe') { throw "Unexpected command: $Name" }
    if ($global:installed -and $global:case -notin @('appx', 'unavailable')) {
        return [pscustomobject]@{ Source = 'Invoke-FakeWinGet' }
    }
}
function Get-AppxPackage {
    param($Name)
    if ($Name -ne 'Microsoft.DesktopAppInstaller') {
        throw 'Unexpected package lookup'
    }
    if ($global:case -ne 'unavailable') {
        return [pscustomobject]@{ InstallLocation = 'mock-package' }
    }
}
function Join-Path {
    param($Path, $ChildPath)
    if ($Path -ne 'mock-package' -or $ChildPath -ne 'winget.exe') {
        throw 'Unexpected client path'
    }
    return 'Invoke-FakeWinGet'
}
function Invoke-FakeWinGet {
    $global:LASTEXITCODE = 0
    if ($args[0] -eq '--version') {
        if ($global:case -eq 'version-error') { $global:LASTEXITCODE = 1 }
        return $global:clientVersion
    }
    if (($args -join ' ') -ne
        'validate --manifest winget-manifests --disable-interactivity') {
        throw 'Unexpected validation arguments'
    }
    if (-not $global:repaired -or $global:clientVersion -ne 'v1.29.290') {
        throw 'Validation used an unprovisioned client'
    }
    Write-Host 'VALIDATE'
    if ($global:case -eq 'invalid') { $global:LASTEXITCODE = -1978335192 }
}
& $Script
"""


@pytest.mark.parametrize(
    "case,error",
    [
        ("old", None),
        ("missing", None),
        ("appx", None),
        ("unavailable", "WinGet executable unavailable"),
        ("stale", "Expected WinGet v1.29.290, found v1.11.430"),
        ("version-error", "winget --version failed"),
        ("invalid", "winget validate failed (-1978335192)"),
        ("install-error", "Module install failed"),
        ("repair-error", "Client repair failed"),
    ],
)
def test_ci_provisions_client_before_validation(tmp_path, case, error):
    workflow = YAML(typ="safe").load(
        (ROOT / ".github/workflows/release.yml").read_text("utf-8")
    )
    step = next(
        s
        for s in workflow["jobs"]["sign-windows"]["steps"]
        if s.get("name") == "Validate with the winget client"
    )
    script = tmp_path / "validate.ps1"
    script.write_text(step["run"], encoding="utf-8")
    harness = tmp_path / "harness.ps1"
    harness.write_text(HARNESS, encoding="utf-8")
    result = subprocess.run(
        [PWSH, "-NoProfile", "-File", str(harness), str(script), case],
        capture_output=True,
        text=True,
        timeout=30,
    )
    output = result.stdout + result.stderr
    assert (result.returncode == 0) == (error is None), output
    if error:
        assert error in output
    else:
        assert "WinGet client: v1.29.290" in output
    if error is None or case == "invalid":
        assert output.index("REPAIRED 1.29.290") < output.index("VALIDATE")
    else:
        assert "VALIDATE" not in result.stdout
