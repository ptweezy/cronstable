# Installation

This page covers every way to install cronstable: the published container image,
`pip`, `pipx`, Homebrew, winget, and the self-contained PyInstaller binaries. It
documents the Python and platform requirements, the runtime dependencies, the
exact binary release assets, and the writable-and-executable temp-directory
requirement that applies to the standalone binary only.

Besides Linux and macOS, cronstable runs natively on Windows. For the
Windows-specific details, see [running on Windows](Running-on-Windows).

## Requirements

| Requirement | Value |
| --- | --- |
| Python (pip/pipx) | `>= 3.10` (`requires-python = ">=3.10"`). Versions 3.10, 3.11, 3.12, 3.13, and 3.14 are supported and tested. For an older Python, use the standalone binary instead. |
| Operating system | Linux, macOS, and Windows. `cronstable/platform.py` isolates OS-specific behavior. `grp` and `pwd` are imported only on POSIX. A few features differ on Windows; see [running on Windows](Running-on-Windows). |
| CPU architectures | Linux: `amd64` (x86_64), `arm64`, `i686` (32-bit x86), `armv7` (32-bit ARM), `ppc64le` (POWER) and `s390x` (IBM Z), for both the container image and the prebuilt binaries. The prebuilt binaries also cover `riscv64` (glibc and musl) and `armv6` (musl-only). macOS: `amd64` and `arm64` (prebuilt binaries). Windows: `amd64` (x64) and `arm64` (ARM64) (prebuilt binaries). |

Python is required only for the `pip`/`pipx` installs. The container image
bundles its own interpreter, and the standalone binaries embed Python, so
neither needs Python on the target host.

### Runtime dependencies (pip/pipx)

Installing the `cronstable` distribution pulls in the following, taken from
`pyproject.toml`:

| Dependency | Version constraint |
| --- | --- |
| `strictyaml` | `>=1.7,<2` |
| `aiohttp` | `>=3.10,<4` |
| `sentry-sdk` | `>=2,<3` |
| `aiosmtplib` | `>=3,<6` |
| `jinja2` | `>=3,<4` |
| `tzdata` | `>=2024.1` |

`tzdata` ships the IANA time-zone database so `zoneinfo` resolves time zones on
minimal/slim images that do not include the system tz data. See
[schedules and time zones](Schedules-and-Timezones).

## Install methods at a glance

| Method | Source | Embeds Python? | Self-extracts at startup? |
| --- | --- | --- | --- |
| Container image | `ghcr.io/ptweezy/cronstable` | Yes (in-image interpreter) | No |
| pip | PyPI (`cronstable`) | No (uses your interpreter) | No |
| pipx | PyPI (`cronstable`) | No (uses your interpreter) | No |
| Standalone binary | GitHub Releases | Yes (embedded) | **Yes** |
| Windows zip (one-directory) | GitHub Releases | Yes (embedded) | No |
| Windows MSI | GitHub Releases | Yes (embedded) | No |
| Homebrew | cronstable tap (release binary) | Yes (embedded) | **Yes** |
| winget | winget-pkgs (release binary) | Yes (embedded) | **Yes** |

Only the standalone binary, including the copies Homebrew and winget install,
self-extracts at startup and therefore needs a writable and executable temp
directory (see
[standalone binary temp-directory requirement](#standalone-binary-temp-directory-requirement)).
The image, the `pip`/`pipx` installs, and the Windows zip and MSI (whose
files sit on disk beside `cronstable.exe`) run without self-extracting.

## Run with Docker

Prebuilt, multi-architecture (`linux/amd64`, `linux/arm64`, `linux/386`,
`linux/arm/v7`, `linux/ppc64le`, `linux/s390x` and `linux/riscv64`) images are
published on every release to two registries: the GitHub Container Registry
(`ghcr.io/ptweezy/cronstable`) and Docker Hub (`docker.io/ptweezy/cronstable`).
The images are identical, so pull from whichever you prefer. Mount your crontab
and run:

```shell
docker run --rm \
  -v "$PWD/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" \
  ghcr.io/ptweezy/cronstable:latest
```

The image runs as the non-root user `65534:65534`. Its entrypoint is
`cronstable` with default arguments `-c /etc/cronstable.d`, so it reads
configuration from `/etc/cronstable.d` unless you override the arguments. For
production, pin a specific version instead of `latest` (for example,
`ghcr.io/ptweezy/cronstable:1.0.4`).

To include your configuration in your own image, base it on the published
image:

```dockerfile
FROM ghcr.io/ptweezy/cronstable:latest

# The base image already runs as the non-root user 65534.
COPY cronstable.yaml /etc/cronstable.d/cronstable.yaml
```

The image is built from `python:3.14-slim` (a multi-stage build that copies a
self-contained venv into the runtime stage) and sets `PYTHONUNBUFFERED=1` and
`PYTHONDONTWRITEBYTECODE=1`. It requires no writable paths at runtime. For the
hardened Kubernetes/Docker setup (read-only root filesystem, dropped
capabilities, `fsGroup`), see
[production and container deployment](Production-Deployment).

### Distro variants

The default `latest` (and `<version>`) image is built on **Debian** (slim). The
same release is also published on several other bases, so you can match a
specific one to your environment: a familiar userland, an image-provenance
policy that mandates a particular vendor, or the smallest possible image. Each
variant adds a `-<distro>` suffix to the tag, and the default Debian image is
also available explicitly as `-debian`:

| Tag suffix | Base image | Python | Notes |
| --- | --- | --- | --- |
| *(none)* / `-debian` | `python:3.14-slim` | 3.14 | Default. Widest architecture coverage. |
| `-alpine` | `python:3.14-alpine` | 3.14 | musl libc; smallest image. |
| `-ubuntu` | `ubuntu:26.04` | 3.14 | Ubuntu LTS userland. |
| `-rhel` | UBI 10 (`ubi-minimal`) | 3.12 | Red Hat base for RHEL / OpenShift. |
| `-fedora` | `fedora:44` | 3.14 | Leading-edge RPM userland. |
| `-opensuse` | `opensuse/leap:16.0` | 3.13 | SUSE / SLES family. |
| `-amazonlinux` | `amazonlinux:2023` | 3.11 | AWS-centric deployments. |
| `-distroless` | `gcr.io/distroless/python3-debian13` | 3.13 | No shell or package manager; minimal attack surface. |

```shell
# e.g. the Alpine variant, pinned to a version:
docker run --rm \
  -v "$PWD/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" \
  ghcr.io/ptweezy/cronstable:1.0.14-alpine
```

Because cronstable is a pure-Python app that supports any Python >= 3.10,
behavior is identical across variants. Pick the base, not the interpreter
version.

The Debian default covers the most architectures. Each variant covers the
architectures its base image publishes:

* Alpine matches Debian's full set.
* RHEL, Fedora, openSUSE, and distroless cover `amd64`, `arm64`, `ppc64le` and
  `s390x`.
* Amazon Linux covers `amd64` and `arm64`.

All variants share the same non-root, read-only-friendly hardening as the
default image.

## Install using pip

Python >= 3.10 is required. Install cronstable in a virtual environment:

```shell
python3 -m venv cronstableenv
. cronstableenv/bin/activate
pip install cronstable
```

This installs the `cronstable` console script (entry point
`cronstable.__main__:main`). For systems with an older Python, use the standalone
binary instead.

If you plan to use the Kubernetes leadership backend with the optional native
client library (`cluster.kubernetes.clientLibrary: native`), install the extra:
`pip install "cronstable[kubernetes]"`. The default HTTP transport needs no extra
dependency. See
[clustering and leader election](Clustering-and-Leader-Election).

## Install using pipx

[pipx](https://github.com/pipxproject/pipx) creates the virtualenv and installs
the program into it:

```shell
pipx install cronstable
```

pipx still requires a supported Python (3.10 or newer) available to build the
isolated environment.

## Install using Homebrew

On macOS or Linux, install from the cronstable
[Homebrew tap](https://github.com/ptweezy/homebrew-tap):

```shell
brew install ptweezy/tap/cronstable
```

This installs the self-contained release binary for your platform (signed and
notarized on macOS; glibc `amd64`/`arm64` from Homebrew on Linux), so no Python
is required. Upgrade later with `brew upgrade cronstable`.

## Install using winget

On Windows, install the
[winget package](https://github.com/microsoft/winget-pkgs/tree/master/manifests/p/ptweezy/cronstable):

```shell
winget install ptweezy.cronstable
```

This installs the self-contained release binary (`amd64` or `arm64`, matching
your system), so no Python is required. Upgrade later with
`winget upgrade ptweezy.cronstable`.

## Install using a binary

A self-contained binary can be downloaded from
<https://github.com/ptweezy/cronstable/releases>. Python is not required on the
target system: the executable embeds it. Every release attaches the following
assets, each built for its own platform and architecture:

| Asset | Platform | libc / arch | Notes |
| --- | --- | --- | --- |
| `cronstable-linux-amd64` | Linux | glibc, x86_64 | Runs on any Linux with glibc 2.39 or newer, such as Ubuntu 24.04. |
| `cronstable-linux-arm64` | Linux | glibc, arm64 | Runs on any Linux with glibc 2.39 or newer on arm64. |
| `cronstable-linux-i686` | Linux | glibc, 32-bit x86 | 32-bit x86 (i686) for glibc hosts. |
| `cronstable-linux-armv7` | Linux | glibc, 32-bit ARM | 32-bit ARM (armv7, such as an older Raspberry Pi) for glibc hosts. |
| `cronstable-linux-ppc64le` | Linux | glibc, ppc64le | 64-bit little-endian POWER (IBM POWER) for glibc hosts. |
| `cronstable-linux-s390x` | Linux | glibc, s390x | IBM Z (s390x, big-endian) for glibc hosts. |
| `cronstable-linux-riscv64` | Linux | glibc, riscv64 | 64-bit RISC-V for glibc hosts. |
| `cronstable-linux-amd64-musl` | Linux | musl, x86_64 | For Alpine and other musl hosts. |
| `cronstable-linux-arm64-musl` | Linux | musl, arm64 | For Alpine and other musl hosts. |
| `cronstable-linux-i686-musl` | Linux | musl, 32-bit x86 | 32-bit x86 (i686) for Alpine and other musl hosts. |
| `cronstable-linux-armv7-musl` | Linux | musl, 32-bit ARM | 32-bit ARM (armv7) for Alpine and other musl hosts. |
| `cronstable-linux-ppc64le-musl` | Linux | musl, ppc64le | 64-bit little-endian POWER for Alpine and other musl hosts. |
| `cronstable-linux-s390x-musl` | Linux | musl, s390x | IBM Z (s390x) for Alpine and other musl hosts. |
| `cronstable-linux-riscv64-musl` | Linux | musl, riscv64 | 64-bit RISC-V for Alpine and other musl hosts. |
| `cronstable-linux-armv6-musl` | Linux | musl, 32-bit ARM | 32-bit ARM (armv6, such as Raspberry Pi 1/Zero); musl-only, no glibc build. |
| `cronstable-macos-arm64` | macOS | Apple Silicon (arm64) | Developer ID signed and notarized. |
| `cronstable-macos-amd64` | macOS | Intel (x86_64) | Developer ID signed and notarized. |
| `cronstable-freebsd-amd64` | FreeBSD | x86_64 | For FreeBSD 14 and 15 hosts, including TrueNAS, pfSense and OPNsense. |
| `cronstable-freebsd-arm64` | FreeBSD | arm64 | For FreeBSD 14 and 15 hosts on arm64. |
| `cronstable-windows-amd64.exe` | Windows | x64 (amd64) | Self-contained `.exe`. The target needs no Python. |
| `cronstable-windows-arm64.exe` | Windows | ARM64 | Self-contained `.exe`. The target needs no Python. |
| `cronstable-windows-i686.exe` | Windows | 32-bit x86 | Self-contained `.exe` for 32-bit Windows. |
| `cronstable-windows-amd64.zip` | Windows | x64 (amd64) | One-directory build: a `cronstable\` folder with `cronstable.exe` and `_internal\`. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-arm64.zip` | Windows | ARM64 | One-directory build. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-i686.zip` | Windows | 32-bit x86 | One-directory build. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-amd64.msi` | Windows | x64 (amd64) | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |
| `cronstable-windows-arm64.msi` | Windows | ARM64 | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |
| `cronstable-windows-i686.msi` | Windows | 32-bit x86 | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |

The glibc Linux builds target glibc 2.39, the Ubuntu 24.04 runner's libc, and
work on any Linux host with glibc 2.39 or newer on the matching CPU. The musl
builds are built inside an Alpine container for musl/Alpine hosts.

The `i686`, `armv7`, `ppc64le` and `s390x` builds, both glibc and musl, extend
the 64-bit `amd64`/`arm64` binaries to 32-bit x86, 32-bit ARM, POWER, and IBM Z
hosts. They build inside a container: `i686` natively on the x86-64 runner, the
rest under QEMU emulation. The `riscv64` builds cover 64-bit RISC-V for both
glibc and musl. The musl-only `armv6` build extends to older 32-bit ARM, such as
a Raspberry Pi 1 or Zero. There is no glibc `armv6` build.

macOS builds cover both Apple Silicon and Intel.

The FreeBSD builds cover amd64 and arm64 and run on FreeBSD 14 and 15
hosts, including appliance systems such as TrueNAS, pfSense and OPNsense.
They are built in a FreeBSD 14 virtual machine. PyPI publishes no FreeBSD
wheels, so the optional speedups are compiled from source; the arm64 build
ships without `orjson` and uses the standard library JSON encoder.

The Windows binaries are self-contained `.exe` files for x64 (`amd64`),
ARM64, and 32-bit x86 (`i686`). Like the other binaries they embed Python,
so Python is not required on the target. Use `i686` only on a 32-bit
Windows install; on 64-bit Windows, choose `amd64` or `arm64`.

Download and run (glibc amd64 Linux shown; append `-musl` on Alpine, or use
`cronstable-macos-<arch>` on a Mac or `cronstable-freebsd-<arch>` on FreeBSD):

```shell
curl -fsSL -o cronstable \
  https://github.com/ptweezy/cronstable/releases/latest/download/cronstable-linux-amd64
chmod +x cronstable
./cronstable --version
```

On Windows, download `cronstable-windows-amd64.exe` (or
`cronstable-windows-arm64.exe` on ARM64, or `cronstable-windows-i686.exe` on
32-bit Windows) and run it directly; no `chmod` is needed:

```powershell
.\cronstable-windows-amd64.exe --version
```

The Windows binaries carry a version resource and are Authenticode-signed
with Azure Artifact Signing. While the signing identity's reputation accrues,
the first run of a browser-downloaded copy can still be blocked by SmartScreen.
Choose **More info**, then **Run anyway**. If your policy calls for it,
verify the download against the release's `SHA256SUMS`.

`winget install ptweezy.cronstable` installs the same binary through the Windows
Package Manager. For the full Windows install and deployment details, see
[running on Windows](Running-on-Windows).

Windows releases also attach `cronstable-windows-amd64.zip`,
`cronstable-windows-arm64.zip` and `cronstable-windows-i686.zip`,
one-directory builds of the same program.
Each extracts to a single `cronstable\` folder and is the download that can
host the [Windows service](Windows-Service), which the one-file `.exe`
cannot.

Clear the Mark of the Web from the zip before extracting, so the extracted
files do not each carry it. Writing into `C:\Program Files` needs an elevated
PowerShell, so use one:

```powershell
Unblock-File .\cronstable-windows-amd64.zip
Expand-Archive .\cronstable-windows-amd64.zip -DestinationPath 'C:\Program Files'
& 'C:\Program Files\cronstable\cronstable.exe' --version
```

For managed machine-wide deployment (GPO, Intune, SCCM) there is also an
MSI per architecture. See [Windows MSI](Windows-MSI).

### macOS signing and notarization

The macOS binaries are Developer ID code-signed (hardened runtime)
and notarized by Apple, so Gatekeeper accepts them and they run without first
clearing the quarantine attribute. No `xattr -d com.apple.quarantine` step is
needed before first run.

### Standalone binary temp-directory requirement

The standalone binary is a self-extracting PyInstaller executable: on each start
it unpacks its embedded Python runtime into a temporary directory and loads
shared libraries from there. It therefore needs a temporary directory that is
both **writable and executable**. On an ordinary system the default `/tmp`
already satisfies this, so no extra setup is required.

This matters only when you run the binary under a **read-only root filesystem**
(for example, a hardened container). With the root filesystem read-only, `/tmp`
is read-only too, and the binary stops at startup: `Could not create temporary
directory`, or `Error loading shared library …: Operation not permitted`. Give
it a small writable *and executable* temp mount and it runs:

```shell
# Note `exec`: Docker's --tmpfs defaults to `noexec`, but the binary must be
# able to execute the libraries it unpacks.
docker run --rm --read-only \
  --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
  -v "$PWD/cronstable.yaml:/etc/cronstable.d/cronstable.yaml:ro" \
  your-image-with-the-binary -c /etc/cronstable.d
```

Remedies:

* **Docker**: mount an `rw,exec` tmpfs at `/tmp`. `--tmpfs` defaults to
  `noexec`, which fails; pass `exec` explicitly, as shown earlier.
* **Kubernetes**: mount an `emptyDir` at `/tmp` (writable and executable by
  default; use `medium: Memory` for a tmpfs).
* **Any host**: point the binary at another writable, executable directory
  with `TMPDIR=/path`.

This requirement is unique to the standalone binary. The published container
image and the `pip`/`pipx` installs run cronstable as a normal Python package with
the interpreter on disk, so they never self-extract and need no writable temp
directory. See [production and container deployment](Production-Deployment).

On Windows the self-extracting `.exe` uses the standard Windows temp directory
(`%TEMP%`), which is writable and executable by default. The preceding
read-only-rootfs and `noexec` caveats are Linux-container concerns only. The
Windows zip and MSI installs keep their files on disk beside `cronstable.exe`
and never unpack at startup, so this requirement does not apply to them at all.

## After installation

Start cronstable by giving it a configuration file or directory with `-c`. It
always runs in the foreground:

```shell
cronstable -c /etc/cronstable.d
```

The `-c` default is platform-specific. On POSIX it is `/etc/cronstable.d`. On
Windows it is the machine-wide `%ProgramData%\cronstable` when that directory
holds configuration, and otherwise `%APPDATA%\cronstable` (for example,
`C:\Users\<you>\AppData\Roaming\cronstable`, falling back to the user profile
`~` if `APPDATA` is unset). `cronstable init` writes a commented starter
configuration into the default location.

The default `shell` also differs: `/bin/sh` on POSIX, and on Windows an empty
default that runs a string `command` through `%ComSpec%` (`cmd.exe`). On
Windows, press Ctrl+C to stop cronstable gracefully. It finishes running jobs
first, as `SIGTERM` does on POSIX.

Per-job `user`/`group` switching and `unix://` web listeners are not available
on Windows. For the full details, see [running on Windows](Running-on-Windows).

See the [command-line reference](CLI-Reference) for all flags, and the
[configuration reference](Configuration-Reference) for the configuration schema.
For Windows-specific behavior, see [running on Windows](Running-on-Windows).
