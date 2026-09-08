# Read and scan the exact release MSIs. Run on an elevated Windows runner.
param(
    [Parameter(Mandatory)][string]$AssetDirectory,
    [Parameter(Mandatory)][string]$OutputDirectory,
    [string]$WixCommand = 'wix'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$assets = (Resolve-Path $AssetDirectory).Path
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
$output = (Resolve-Path $OutputDirectory).Path
Start-Transcript -Path (Join-Path $output 'defender.log')
try {
    $status = Get-MpComputerStatus
    if (-not $status.AMServiceEnabled -or -not $status.AntivirusEnabled) {
        throw 'Microsoft Defender is unavailable; refusing an unscanned submission.'
    }
    function Get-DefenderScanner {
        $command = Get-ChildItem "$env:ProgramData\Microsoft\Windows Defender\Platform\*\MpCmdRun.exe" -ErrorAction SilentlyContinue |
            Sort-Object { [version]$_.Directory.Name.Split('-')[0] } -Descending |
            Select-Object -First 1
        if (-not $command) {
            $command = Get-Item "$env:ProgramFiles\Windows Defender\MpCmdRun.exe"
        }
        return $command
    }
    # Use Defender's native updater with Microsoft's direct update source.
    # Each attempt resolves the executable and checks service readiness.
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $scanner = Get-DefenderScanner
            Write-Host "Updating Defender signatures (attempt $attempt/3): $($scanner.FullName)"
            & $scanner.FullName -SignatureUpdate -MMPC
            if ($LASTEXITCODE -ne 0) {
                throw "MpCmdRun signature update exited with $LASTEXITCODE"
            }
            $status = Get-MpComputerStatus
            if (-not $status.AMServiceEnabled -or -not $status.AntivirusEnabled) {
                throw 'Microsoft Defender is unavailable after signature update'
            }
            break
        } catch {
            if ($attempt -eq 3) {
                throw "Defender signature update failed after 3 attempts; scanning is blocked. $($_.Exception.Message)"
            }
            Write-Warning "Defender update attempt $attempt failed: $($_.Exception.Message). Retrying."
            Start-Sleep -Seconds (5 * $attempt)
        }
    }
    $status | Select-Object AMProductVersion, AMEngineVersion,
        AntivirusSignatureVersion, AntivirusSignatureLastUpdated | Format-List
    $scanner = Get-DefenderScanner
    function Scan-Path([string]$Path) {
        # A scan that remediates malware can return 0. DisableRemediation
        # retains detected files and makes detections fail the scan.
        # It also scans archives and ignores file exclusions.
        & $scanner.FullName -Scan -ScanType 3 -File $Path -DisableRemediation
        if ($LASTEXITCODE -ne 0) {
            throw "Defender rejected $Path (exit $LASTEXITCODE). See defender.log; submit suspected false positives to https://www.microsoft.com/en-us/wdsi/filesubmission."
        }
    }
    $sums = @{}
    foreach ($line in Get-Content (Join-Path $assets 'SHA256SUMS')) {
        if ($line -match '^([a-fA-F0-9]{64})\s+\*?(\S+)$') {
            $sums[$Matches[2]] = $Matches[1]
        }
    }
    $installer = New-Object -ComObject WindowsInstaller.Installer
    $metadata = @{}
    foreach ($arch in @('amd64', 'arm64')) {
        $name = "cronstable-windows-$arch.msi"
        $path = Join-Path $assets $name
        $hash = (Get-FileHash $path -Algorithm SHA256).Hash
        if (-not $sums.ContainsKey($name) -or $sums[$name] -ne $hash) {
            throw "SHA256SUMS mismatch: $name"
        }
        $signature = Get-AuthenticodeSignature $path
        if ($signature.Status -ne 'Valid' -or -not $signature.TimeStamperCertificate) {
            throw "Missing valid timestamped Authenticode signature: $name"
        }
        Write-Host "$name SHA256=$hash Publisher=$($signature.SignerCertificate.Subject)"
        Scan-Path $path

        $database = $installer.OpenDatabase($path, 0)
        $properties = @{ Sha256 = $hash }
        foreach ($key in @('ProductCode', 'UpgradeCode', 'ProductVersion', 'ProductName', 'Manufacturer')) {
            $view = $database.OpenView("SELECT ``Value`` FROM ``Property`` WHERE ``Property``='$key'")
            $view.Execute()
            $record = $view.Fetch()
            if (-not $record) { throw "$name missing MSI property $key" }
            $properties[$key] = $record.StringData(1)
            $view.Close()
        }
        $platform = $database.SummaryInformation(0).Property(7).Split(';')[0]
        $architectures = @{ x64 = 'x64'; Arm64 = 'arm64' }
        if (-not $architectures.ContainsKey($platform)) {
            throw "$name unsupported MSI platform $platform"
        }
        $properties.Architecture = $architectures[$platform]
        $metadata[$arch] = $properties

        # Use WiX to extract both architectures as data on this x64 runner.
        $target = Join-Path $output $arch
        $log = Join-Path $output "$arch-extract.log"
        $authoring = Join-Path $output "$arch.wxs"
        & $WixCommand msi decompile $path -x $target -o $authoring 2>&1 | Tee-Object -FilePath $log
        if ($LASTEXITCODE -ne 0) {
            throw "MSI extraction failed: $name ($LASTEXITCODE); see $log"
        }
        [xml]$xml = Get-Content $authoring -Raw
        $executables = @($xml.SelectNodes("//*[local-name()='File' and @Id='CronstableExe']"))
        if ($executables.Count -ne 1) { throw "${name}: expected one payload executable" }
        # WiX writes SourceDir\File\CronstableExe into the authoring, but
        # extracts cabinet entries by File id beneath the -x folder's File
        # directory. SourceDir is a placeholder, not a filesystem path.
        $payloadPath = Join-Path $target 'File/CronstableExe'
        if (-not (Test-Path $payloadPath -PathType Leaf)) {
            throw "$name missing extracted payload: $payloadPath"
        }
        $payloadSignature = Get-AuthenticodeSignature $payloadPath
        if ($payloadSignature.Status -ne 'Valid' -or -not $payloadSignature.TimeStamperCertificate) {
            throw "$name has an unsigned or invalid payload executable"
        }
        Scan-Path $target
        if ((Get-FileHash $path -Algorithm SHA256).Hash -ne $hash) {
            throw "$name changed during validation"
        }
    }
    $metadata | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $output 'metadata.json') -Encoding utf8
} finally {
    Stop-Transcript
}
