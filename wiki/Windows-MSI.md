# Windows MSI

Every release attaches `cronstable-windows-amd64.msi` and
`cronstable-windows-arm64.msi`: per-machine Windows Installer packages for
managed deployment through GPO, Intune, SCCM, or a plain elevated
`msiexec`. The MSI carries the same one-directory build the zip asset
holds, so nothing self-extracts at startup and Python is not required on
the target.

See [Windows Service](Windows-Service) for the service the MSI registers,
[Running on Windows](Running-on-Windows) for the platform behavior, and
[Installation](Installation) for every other install method.

## What the MSI installs

* The program in `C:\Program Files\cronstable` (`cronstable.exe` beside its
  `_internal` directory).
* The `cronstable` Windows service, registered with exactly the settings
  `cronstable service install` writes: the same command line, LocalSystem,
  automatic start, the same description, and the same recovery actions
  (restart after 60 seconds, twice, then give up, resetting the count
  daily), applied to orderly failure exits as well as crashes. A test in
  the repository holds the two spellings equal.
* The install directory on the system `PATH`, so `cronstable` works in any
  new shell. Shells that were already open do not see the change until
  they are restarted.

Uninstalling removes all three. Configuration and logs under
`C:\ProgramData\cronstable` are never touched by install, upgrade or
uninstall.

## Quick start

From an elevated prompt:

```shell
msiexec /i cronstable-windows-amd64.msi /qn
cronstable init C:\ProgramData\cronstable
cronstable service start
```

The service is registered for automatic start but the MSI does not start
it on a first install: there is no configuration yet, and a service with
nothing to read would stop with an error and burn its recovery restarts.
`cronstable init` writes a commented starter configuration (and tightens
the directory's permissions where the platform default leaves them loose);
after that, `cronstable service start` or the next boot brings it up.
Until then `cronstable service status` reports the service as stopped,
which is the expected state.

## Properties

Pass public properties on the `msiexec` command line
(`msiexec /i ... PROPERTY=value`):

| Property | Default | Effect |
| --- | --- | --- |
| `CONFIGDIR` | `C:\ProgramData\cronstable` | The configuration directory baked into the service's command line. |
| `ADDPATH` | `1` | `0` skips adding the install directory to the system `PATH`. |
| `STARTSERVICE` | unset | `1` starts the service at the end of the install, including a first install. Pass it when the configuration is deployed ahead of the package. |
| `INSTALLFOLDER` | `C:\Program Files\cronstable` | The install directory. |

For a managed rollout that ships configuration with the package, deploy
the configuration files first (or in the same policy) and install with
`STARTSERVICE=1`:

```shell
msiexec /i cronstable-windows-amd64.msi /qn STARTSERVICE=1
```

Log an install with `/l*v install.log` when a deployment misbehaves; the
log names the exact action that failed.

## Upgrades

Installing a newer MSI over an older one upgrades in place: the old
version is removed first, and a running service is stopped for the
switch. The service is started again at the end of the upgrade when the
machine-wide configuration directory exists, which is the working
deployment's normal case; a machine that never got configuration stays
stopped. Windows Installer waits for the service's stop, and the stop
drains running jobs first, so an upgrade during a very long job can time
out and roll back; for maintenance windows, stop the service yourself
(`cronstable service stop`) before pushing the upgrade.

Downgrades are refused with a message rather than silently replacing a
newer install.

## Signing

Like the other Windows assets, the MSI is not Authenticode-signed. GPO,
Intune and SCCM deployments do not involve SmartScreen, but a
browser-downloaded MSI's first run trips it (choose "More info", then
"Run anyway"), and AppLocker/WDAC publisher rules cannot admit an
unsigned package; use a hash rule there. Verify a download against the
release's `SHA256SUMS` when your policy calls for it.
