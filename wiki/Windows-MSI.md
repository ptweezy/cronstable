# Windows MSI

**`cronstable-windows-amd64v3.msi` is recommended for compatible x64 CPUs.**
It uses an optimized embedded Python runtime and requires the full x86-64-v3
feature set. **`cronstable-windows-amd64.msi` is the compatibility build**
for CPUs or VMs without v3, or when support is uncertain. Both use the same
service, install paths and upgrade identity.

Every release attaches `cronstable-windows-amd64v3.msi`,
`cronstable-windows-amd64.msi`, `cronstable-windows-arm64.msi` and
`cronstable-windows-i686.msi`: per-machine Windows Installer packages for
managed deployment through GPO, Intune, SCCM, or a plain elevated
`msiexec`. The MSI carries the same one-directory build the zip asset
holds, so nothing self-extracts at startup and Python is not required on
the target.

See [Windows service](Windows-Service) for the service the MSI registers,
[running on Windows](Running-on-Windows) for the platform behavior, and
[installation](Installation) for every other install method.

WinGet uses `cronstable-windows-amd64-setup.exe` and
`cronstable-windows-arm64-setup.exe`. These signed setup executables embed the
corresponding MSI and display cronstable's icon in the administrator prompt.
The release also includes `cronstable-windows-amd64v3-setup.exe` for
compatible CPUs.
They upgrade existing MSI and setup installations. Direct MSI installations
use Windows Installer's icon in that prompt; the installed application entry
and `cronstable.exe` use cronstable's icon. See
[the installation instructions](Installation#install-using-winget) for package
availability and steps to switch from a portable install.

## What the MSI installs

* The program in `C:\Program Files\cronstable` (`cronstable.exe` beside its
  `_internal` directory).
* The `cronstable` Windows service, using the same settings as
  `cronstable service install`: LocalSystem, automatic start, and recovery
  after failure exits or crashes. Recovery restarts the service after
  60 seconds, up to twice, with the count reset daily.
* The install directory on the system `PATH`, so `cronstable` works in any
  new shell. Shells that were already open do not see the change until
  they are restarted.
* The configuration directory `C:\ProgramData\cronstable`, when it does not
  exist yet. See [the configuration directory](#the-configuration-directory).

Uninstalling removes the program, the service, and the `PATH` entry. It
keeps `C:\ProgramData\cronstable` and everything in it.

## The configuration directory

The service runs as LocalSystem, so whoever can add a file to its
configuration directory can run commands as SYSTEM, and `%ProgramData%` lets
any local account create a directory and become its owner. The MSI therefore
creates `C:\ProgramData\cronstable` with the same ownership and permissions
as `cronstable init`: ownership by the Administrators group, full control for
SYSTEM and Administrators, read access for everyone else, and no permissions
inherited from `%ProgramData%`. It sets these permissions only on a directory
it creates and preserves existing directories and their permissions.

Before it reads anything, the service checks its configuration directory. It
refuses to start when the directory is missing or is a junction or symbolic
link, when Everyone, Users, Authenticated Users, or another any-user group
can add files to it, or when an account other than SYSTEM, Administrators, or
TrustedInstaller owns it (unless an OWNER RIGHTS entry holds that owner to
read). It repeats the check before every reload. The check ignores a write
grant to a single named account. [Where a service
logs](Windows-Service#where-a-service-logs) describes how the service reports
a refusal, and [who may write the config
directory](Running-on-Windows#who-may-write-the-config-directory) has the fix.

## Quick start

The examples use amd64v3 for a compatible x64 CPU; substitute `amd64` in
the filenames for the compatibility build. Check the
[CPU requirements](Installation#amd64v3-cpu-requirements), then use an elevated
prompt:

```shell
msiexec /i cronstable-windows-amd64v3.msi /qn
"C:\Program Files\cronstable\cronstable.exe" init C:\ProgramData\cronstable
"C:\Program Files\cronstable\cronstable.exe" service start
```

The full paths matter: the shell that ran `msiexec` predates the `PATH`
change the installer made, so a bare `cronstable` only works in shells
opened later.

A first install leaves the service stopped until the next boot. While the
directory holds no configuration, the service runs with no jobs, and it loads
new files within a minute. To start it now, run `cronstable init` to write a
commented starter configuration into the directory the MSI created, then run
`cronstable service start`.

## Properties

Pass public properties on the `msiexec` command line
(`msiexec /i ... PROPERTY=value`):

| Property | Default | Effect |
| --- | --- | --- |
| `CONFIGDIR` | `C:\ProgramData\cronstable` | The configuration directory in the service's command line. The MSI creates only the default directory. Before starting the service with a custom path, create a directory writable only by SYSTEM and Administrators, for example, with `cronstable init`. |
| `ADDPATH` | `1` | `0` skips adding the install directory to the system `PATH`. |
| `STARTSERVICE` | unset | `1` starts the service at the end of the install, including a first install. Pass it when the configuration is deployed ahead of the package. |
| `INSTALLFOLDER` | `C:\Program Files\cronstable` (`C:\Program Files (x86)\cronstable` for the i686 package on 64-bit Windows) | The install directory. |

`CONFIGDIR` and `ADDPATH` are remembered: an upgrade installed without
them keeps the existing install's values, so a fleet push never has to
repeat them. Passing one on an upgrade command line changes it.

For a managed rollout that ships configuration with the package, deploy
the configuration files first (or in the same policy) and install with
`STARTSERVICE=1`:

```shell
msiexec /i cronstable-windows-amd64v3.msi /qn STARTSERVICE=1
```

A directory that your deployment tool creates under `%ProgramData%`
inherits permission for every local account to add files, and the service
refuses it. Restrict the directory before the service starts, using the
`icacls` commands in [who may write the config
directory](Running-on-Windows#who-may-write-the-config-directory), or install
the MSI first and deploy the files into the directory it creates.

When a deployment misbehaves, log the install with `/l*v install.log`. The
log names the exact action that failed.

## Upgrades

Installing a newer MSI upgrades in place, stopping the service and
removing the old version first. The upgrade preserves install properties,
including the configuration directory. It restarts the service if that
directory exists; otherwise, the service remains stopped.

Windows Installer waits for the service's stop, and the stop drains
running jobs first. An upgrade during a very long job can therefore time
out and roll back. For maintenance windows, stop the service yourself
(`cronstable service stop`) before pushing the upgrade.

Downgrades are refused with a message rather than silently replacing a
newer install.

## Signing

The MSI, like every Windows release asset, is Authenticode-signed with
Azure Artifact Signing. Each signature carries an RFC 3161 timestamp, so
it outlives the short-lived signing certificates. UAC elevation shows the
verified publisher, and AppLocker/WDAC deployments can admit the package
with a publisher rule.

GPO, Intune, and SCCM deployments do not involve SmartScreen. It can still
appear on a browser-downloaded MSI's first run while the signing identity's
reputation accrues (choose **More info**, then **Run anyway**).
When your policy calls for it, verify a download against the release's
`SHA256SUMS`.
