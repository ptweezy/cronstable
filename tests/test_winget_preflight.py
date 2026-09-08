"""Exercise the preflight's failure paths with mocked Windows services."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PWSH = shutil.which("pwsh")
pytestmark = pytest.mark.skipif(PWSH is None, reason="PowerShell required")

# The harness mocks Windows and Defender entry points. It runs the preflight
# script with real filesystem operations, PowerShell control flow, and JSON.
HARNESS = r"""
param($Script, $Assets, $Output, $Case)
$ErrorActionPreference = 'Stop'
$global:ErrorView = 'NormalView'
$global:case = $Case
$global:scans = 0
$global:updates = 0
$global:delays = @()
$env:ProgramData = '/mock'
function Get-MpComputerStatus {
    if ($global:case -eq 'readiness-rpc' -and $global:updates -eq 1) {
        throw 'The remote procedure call failed.'
    }
    @{ AMServiceEnabled = ($global:case -ne 'disabled')
       AntivirusEnabled = $true }
}
function Update-MpSignature {
    throw 'Unexpected PowerShell update cmdlet call'
}
function Start-Sleep {
    param($Seconds)
    $global:delays += $Seconds
}
function Invoke-FakeScan {
    if ('-SignatureUpdate' -in $args) {
        $global:updates++
        Write-Host "UPDATE $global:updates"
        if ('-MMPC' -notin $args) { throw 'expected direct update source' }
        $global:LASTEXITCODE = 0
        if ($global:case -eq 'update' -or
            ($global:case -eq 'update-retry' -and $global:updates -lt 3)) {
            $global:LASTEXITCODE = 1726
        }
        if ($global:case -eq 'update-rpc' -and $global:updates -eq 1) {
            throw 'The remote procedure call failed.'
        }
        return
    }
    if ($global:updates -eq 0) { throw 'scan before signature update' }
    $global:scans++
    Write-Host "SCAN $global:scans"
    if ('-DisableRemediation' -notin $args) { throw 'remediation enabled' }
    if ('-ScanType' -notin $args -or 3 -notin $args) {
        throw 'not custom scan'
    }
    $global:LASTEXITCODE = 0
    if ($global:case -eq 'detected') { $global:LASTEXITCODE = 2 }
    if ($global:case -eq 'scan-error') { $global:LASTEXITCODE = 5 }
    if ($global:case -eq 'payload-detected' -and $global:scans -eq 2) {
        $global:LASTEXITCODE = 2
    }
}
function Get-ChildItem {
    param($Path, [switch]$Recurse, [switch]$File, $Filter)
    if ($Path -like '/mock*') {
        return [pscustomobject]@{
            FullName = 'Invoke-FakeScan'
            Directory = @{ Name = '4.18.26070.3006-0' }
        }
    }
    Microsoft.PowerShell.Management\Get-ChildItem @PSBoundParameters
}
function Get-AuthenticodeSignature {
    param($Path)
    $valid = $global:case -ne 'unsigned'
    if ($global:case -eq 'payload-unsigned' -and $Path.EndsWith('.exe')) {
        $valid = $false
    }
    [pscustomobject]@{
        Status = $(if ($valid) { 'Valid' } else { 'NotSigned' })
        TimeStamperCertificate = ($global:case -ne 'untimestamped')
        SignerCertificate = @{ Subject = 'Test publisher' }
    }
}
function New-Object {
    param($ComObject)
    if ($ComObject -ne 'WindowsInstaller.Installer') { throw 'unexpected COM' }
    $instance = [pscustomobject]@{}
    $instance | Add-Member ScriptMethod OpenDatabase {
        param($Path, $Mode)
        if ($Mode -ne 0) { throw 'database must be read only' }
        $global:arch = $(if ($Path -match 'amd64') { 'x64' } else { 'Arm64' })
        $database = [pscustomobject]@{}
        $database | Add-Member ScriptMethod OpenView {
            param($Query)
            $global:key = ($Query -split "'")[1]
            $view = [pscustomobject]@{}
            $view | Add-Member ScriptMethod Execute {}
            $view | Add-Member ScriptMethod Close {}
            $view | Add-Member ScriptMethod Fetch {
                $record = [pscustomobject]@{}
                $record | Add-Member ScriptMethod StringData {
                    param($Index)
                    if ($Index -ne 1) { throw 'wrong field' }
                    return @{
                        ProductCode = '{12345678-1234-1234-1234-123456789012}'
                        UpgradeCode = '{B995CBA8-16CD-48F1-A13B-C4C4B927E7BE}'
                        ProductVersion = '1.2.50'
                        ProductName = 'cronstable'
                        Manufacturer = 'cronstable'
                    }[$global:key]
                }
                return $record
            }
            return $view
        }
        $database | Add-Member ScriptMethod SummaryInformation {
            param($Count)
            $summary = [pscustomobject]@{}
            $summary | Add-Member ScriptMethod Property {
                param($Index)
                if ($Index -ne 7) { throw 'wrong summary field' }
                return "$global:arch;1033"
            }
            return $summary
        }
        return $database
    }
    return $instance
}
function Invoke-FakeWix {
    if ($args[0] -ne 'msi' -or $args[1] -ne 'decompile') {
        throw 'must decompile'
    }
    $target = $args[4]
    $authoring = $args[6]
    New-Item -ItemType Directory -Force $target | Out-Null
    $file = ''
    if ($global:case -ne 'missing-payload') {
        $payload = Join-Path $target 'cronstable.exe'
        Set-Content $payload 'test payload'
        $escaped = [System.Security.SecurityElement]::Escape($payload)
        $file = "<File Id='CronstableExe' Source='$escaped' />"
    }
    Set-Content $authoring "<Wix>$file</Wix>"
    $global:LASTEXITCODE = $(if ($global:case -eq 'extract') { 1 } else { 0 })
}
try {
    & $Script -AssetDirectory $Assets -OutputDirectory $Output `
        -WixCommand Invoke-FakeWix
} finally {
    Write-Host "DELAYS $($global:delays -join ',')"
}
if ($global:scans -ne 4) { throw 'must scan both MSIs and both payloads' }
"""


@pytest.mark.parametrize(
    "case,success",
    [
        ("clean", True),
        ("update-retry", True),
        ("update-rpc", True),
        ("readiness-rpc", True),
        ("disabled", False),
        ("update", False),
        ("hash", False),
        ("unsigned", False),
        ("untimestamped", False),
        ("detected", False),
        ("scan-error", False),
        ("payload-detected", False),
        ("payload-unsigned", False),
        ("extract", False),
        ("missing-payload", False),
    ],
)
def test_preflight(tmp_path, case, success):
    import hashlib

    assets = tmp_path / "assets"
    assets.mkdir()
    sums = []
    for arch in ("amd64", "arm64"):
        name = f"cronstable-windows-{arch}.msi"
        (assets / name).write_bytes(b"test MSI")
        digest = hashlib.sha256(b"test MSI").hexdigest()
        sums.append(f"{digest if case != 'hash' else '0' * 64}  {name}")
    (assets / "SHA256SUMS").write_text("\n".join(sums), encoding="utf-8")
    harness = tmp_path / "harness.ps1"
    harness.write_text(HARNESS, encoding="utf-8")
    output = tmp_path / "output"
    result = subprocess.run(
        [
            PWSH,
            "-NoProfile",
            "-File",
            str(harness),
            str(ROOT / ".github/scripts/prepare_winget.ps1"),
            str(assets),
            str(output),
            case,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (result.returncode == 0) == success, result.stdout + result.stderr
    assert (output / "defender.log").exists()
    assert (output / "metadata.json").exists() == success
    if not success:
        expected = {
            "disabled": "Microsoft Defender is unavailable",
            "update": "signature update failed after 3 attempts",
            "hash": "SHA256SUMS mismatch",
            "unsigned": "Authenticode signature",
            "untimestamped": "Authenticode signature",
            "detected": "Defender rejected",
            "scan-error": "Defender rejected",
            "payload-detected": "Defender rejected",
            "payload-unsigned": "invalid payload executable",
            "extract": "MSI extraction failed",
            "missing-payload": "expected one payload executable",
        }
        assert expected[case] in result.stdout + result.stderr
    if case in {"update", "update-retry"}:
        assert result.stdout.count("UPDATE ") == 3
        assert "DELAYS 5,10" in result.stdout
    elif case in {"update-rpc", "readiness-rpc"}:
        assert result.stdout.count("UPDATE ") == 2
        assert "DELAYS 5" in result.stdout
    if case == "update":
        assert "SCAN " not in result.stdout
    if case in {"detected", "scan-error"}:
        assert result.stdout.count("SCAN ") == 1
    if success:
        data = json.loads((output / "metadata.json").read_text("utf-8-sig"))
        assert data["amd64"]["Architecture"] == "x64"
        assert data["arm64"]["Architecture"] == "arm64"
        assert data["amd64"]["Sha256"].lower() == digest
