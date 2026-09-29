param(
    [Parameter(Mandatory)][string]$Setup,
    [string]$PreviousMsi,
    [string]$UpgradeSetup
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-Installer([string]$Path, [string[]]$Arguments) {
    $process = Start-Process -FilePath $Path -ArgumentList $Arguments `
        -Wait -PassThru -WindowStyle Hidden
    if ($process.ExitCode -notin @(0, 3010)) {
        throw "Installer exited with $($process.ExitCode): $Path"
    }
}
function Invoke-Setup([string]$Path, [string]$Action, [string]$Log) {
    Invoke-Installer (Resolve-Path $Path).Path @(
        $Action, '/quiet', '/norestart', '/log', "`"$Log`""
    )
}
function Assert-Installed {
    $exe = Join-Path $env:ProgramFiles 'cronstable\cronstable.exe'
    if (-not (Test-Path $exe)) { throw 'Missing installed executable' }
    & $exe --version
    if ($LASTEXITCODE -ne 0) { throw 'Installed executable failed' }
    $service = Get-Service cronstable
    if ($service.Status -ne 'Stopped') { throw 'Expected a stopped service' }
    Assert-BundleRegistration
}
function Assert-BundleRegistration {
    $entries = @(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*' |
        Where-Object DisplayName -eq 'cronstable' |
        Where-Object SystemComponent -ne 1)
    if ($entries.Count -ne 1 -or -not $entries[0].DisplayIcon) {
        throw 'Expected one installed application entry with an icon'
    }
}
function Assert-Uninstalled {
    if (Get-Service cronstable -ErrorAction SilentlyContinue) {
        throw 'Service remains after uninstall'
    }
    if (Test-Path (Join-Path $env:ProgramFiles 'cronstable\cronstable.exe')) {
        throw 'Executable remains after uninstall'
    }
}

try {
    Invoke-Setup $Setup '/install' 'setup-install.log'
    Assert-Installed
    if ($UpgradeSetup) {
        Invoke-Setup $UpgradeSetup '/install' 'setup-upgrade.log'
        & (Join-Path $env:ProgramFiles 'cronstable\cronstable.exe') --version
        if ($LASTEXITCODE -ne 0) { throw 'Upgraded executable failed' }
        Assert-BundleRegistration
        Invoke-Setup $UpgradeSetup '/uninstall' 'setup-upgrade-uninstall.log'
    } else {
        Invoke-Setup $Setup '/uninstall' 'setup-uninstall.log'
    }
    Assert-Uninstalled

    if ($PreviousMsi) {
        if (-not $UpgradeSetup) { throw 'Migration requires UpgradeSetup' }
        $config = Join-Path $env:RUNNER_TEMP 'cronstable-setup-config-absent'
        if (Test-Path $config) { throw "Migration config must be absent: $config" }
        Invoke-Installer 'msiexec.exe' @(
            '/i', "`"$((Resolve-Path $PreviousMsi).Path)`"",
            '/qn', '/norestart', '/l*v', 'setup-msi-install.log',
            "CONFIGDIR=`"$config`"", 'ADDPATH=0'
        )
        Invoke-Setup $UpgradeSetup '/install' 'setup-msi-upgrade.log'
        Assert-Installed
        $properties = Get-ItemProperty 'HKLM:\SOFTWARE\cronstable'
        if ($properties.ConfigDir -ne $config -or $properties.AddPath -ne '0') {
            throw 'Setup upgrade lost remembered MSI properties'
        }
        $service = Get-CimInstance Win32_Service -Filter "Name='cronstable'"
        if (-not $service.PathName.Contains($config)) {
            throw 'Setup upgrade lost the service configuration directory'
        }
        Invoke-Setup $UpgradeSetup '/uninstall' 'setup-msi-uninstall.log'
        Assert-Uninstalled
    }
} finally {
    if ($UpgradeSetup) {
        Invoke-Setup $UpgradeSetup '/uninstall' 'setup-cleanup-upgrade.log'
    }
    Invoke-Setup $Setup '/uninstall' 'setup-cleanup.log'
}
