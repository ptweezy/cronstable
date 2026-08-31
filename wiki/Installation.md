# Installation

This page covers every way to install cronstable: the published container image,
`pip`, `pipx`, Homebrew, winget, Scoop, `.deb`, `.rpm`, `.apk` and FreeBSD
packages, Nix, `ubi` and `mise`, and the self-contained PyInstaller binaries. It
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
| CPU architectures | Linux: `amd64` (x86_64), `arm64`, `i686` (32-bit x86), `armv7` (32-bit ARM), `ppc64le` (POWER) and `s390x` (IBM Z), for both the container image and the prebuilt binaries. The prebuilt binaries also cover `armv6` and `riscv64` and `loong64` (LoongArch) in both libcs, plus `mips64le` and `armel` (glibc). macOS: `amd64` and `arm64`. Windows: `amd64` (x64), `arm64` (ARM64) and `i686`. Also FreeBSD (`amd64`, `arm64`), OpenBSD, NetBSD and illumos (`amd64`). |

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
| Scoop | ScoopInstaller/Extras (release binary) | Yes (embedded) | **Yes** |
| `.deb` / `.rpm` | GitHub Releases | Yes (embedded) | **Yes** |
| `.apk` (Alpine) | GitHub Releases | Yes (embedded) | **Yes** |
| `.pkg` (FreeBSD) | GitHub Releases | Yes (embedded) | **Yes** |
| `ubi` / `mise` | GitHub Releases | Yes (embedded) | **Yes** |
| Nix | this flake | No (uses nixpkgs Python) | No |

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

On a 32-bit userland with a 64-bit kernel, install the post-quantum push
extra as `linux32 pip install "cronstable[push-pq]"`. Under `linux32`,
`uname` reports the 32-bit machine, so pip skips `cryptography`, which has
no 32-bit wheel, rather than failing to build it from source.

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

## Install using Scoop

On Windows, through [Scoop](https://scoop.sh):

```shell
scoop bucket add extras
scoop install cronstable
```

This installs the same self-contained `.exe` the release publishes, for
whichever architecture you are on. Upgrade later with `scoop update cronstable`.

## Install using a .deb or .rpm package

Releases attach a Debian package and an RPM for `amd64`, `arm64`, `i686`,
`armv7`, `ppc64le`, `s390x` and `riscv64`. Each installs the same self-contained
binary as `/usr/bin/cronstable`, plus a systemd unit and a starter
configuration:

| Path | Contents |
| --- | --- |
| `/usr/bin/cronstable` | The binary. |
| `/usr/lib/systemd/system/cronstable.service` | The service unit, running as the `cronstable` user. |
| `/etc/cronstable.d/cronstable.yaml` | Starter configuration, marked as a config file, so your edits survive an upgrade. |
| `/var/lib/cronstable` | The durable store, created by systemd on first start. |

```shell
# Debian, Ubuntu and derivatives
curl -fsSLO https://github.com/ptweezy/cronstable/releases/latest/download/cronstable-linux-amd64.deb
sudo apt install ./cronstable-linux-amd64.deb

# RHEL, Alma, Rocky, Fedora, SLES and derivatives
curl -fsSLO https://github.com/ptweezy/cronstable/releases/latest/download/cronstable-linux-amd64.rpm
sudo dnf install ./cronstable-linux-amd64.rpm
```

Installing creates the `cronstable` service account and leaves the service
stopped and disabled, because the shipped configuration schedules no jobs. Edit
`/etc/cronstable.d/cronstable.yaml`, then:

```shell
sudo systemctl enable --now cronstable
systemctl status cronstable
```

The packages declare the glibc version their binary needs, so a host too old for
a given architecture refuses the install rather than failing at first run.
Removing the package leaves `/etc/cronstable.d` and `/var/lib/cronstable` in
place; delete them by hand when you mean to drop the schedule and its history.

Both formats are vendor packages built from the release binary. They are not in
Debian or Fedora proper and carry no distribution changelog or copyright file.

## Install using an Alpine package

Releases attach an `.apk` for every architecture the musl builds cover. It
installs the binary, an OpenRC service and a starter configuration:

| Path | Contents |
| --- | --- |
| `/usr/bin/cronstable` | The binary, linked against musl. |
| `/etc/init.d/cronstable` | The OpenRC service, running as the `cronstable` user. |
| `/etc/conf.d/cronstable` | Service settings: config directory, user, state directory, stop timeout. |
| `/etc/cronstable.d/cronstable.yaml` | Starter configuration. |
| `/var/lib/cronstable` | The durable store, created by the service on first start. |

```shell
curl -fsSLO https://github.com/ptweezy/cronstable/releases/latest/download/cronstable-linux-amd64.apk
apk add --allow-untrusted ./cronstable-linux-amd64.apk
```

`--allow-untrusted` is required: these packages are not signed with a key in
`/etc/apk/keys`, so apk declines them otherwise. Verify the download against the
release's `SHA256SUMS` if your policy calls for it.

Installing creates the `cronstable` service account and leaves the service
stopped, because the shipped configuration schedules no jobs and no Alpine
package enables its own service. Edit the configuration, then:

```shell
rc-update add cronstable default
rc-service cronstable start
rc-service cronstable status
```

`rc-service cronstable reload` reparses the configuration in place without
interrupting running jobs. The service runs under `supervise-daemon`, so a crash
is restarted, and stopping it gives cronstable two minutes to drain its running
jobs before they are killed (`cronstable_retry` in `/etc/conf.d/cronstable`).

Most Alpine users run the [container image](#run-with-docker) instead, which is
built on the same musl base.

## Install using a FreeBSD package

Releases attach a `.pkg` for amd64 and arm64, built by FreeBSD's own `pkg` in a
FreeBSD 14 virtual machine and installed there as part of the build:

| Path | Contents |
| --- | --- |
| `/usr/local/bin/cronstable` | The binary. |
| `/usr/local/etc/rc.d/cronstable` | The `rc.d` script, running the daemon under `daemon(8)` as the `cronstable` user. |
| `/usr/local/etc/cronstable.d/cronstable.yaml` | Starter configuration, installed from a `.sample` so your edits survive an upgrade. |
| `/var/db/cronstable` | The durable store, created by the `rc.d` script on first start. |

```shell
fetch https://github.com/ptweezy/cronstable/releases/latest/download/cronstable-freebsd-amd64.pkg
pkg add cronstable-freebsd-amd64.pkg
```

`pkg add` of a local file needs no repository. Installing creates the
`cronstable` user and leaves the service disabled:

```shell
sysrc cronstable_enable=YES
service cronstable start
service cronstable status
```

`service cronstable reload` sends `SIGHUP` to reparse the configuration in
place. Settings live in `/etc/rc.conf`: `cronstable_config`, `cronstable_user`,
`cronstable_statedir` and `cronstable_flags`.

## Install using ubi or mise

[`ubi`](https://github.com/houseabsolute/ubi) installs release binaries straight
from GitHub, and [`mise`](https://mise.jdx.dev) drives it as a backend:

```shell
# ubi
ubi --project ptweezy/cronstable --in /usr/local/bin

# mise
mise use -g ubi:ptweezy/cronstable
```

Both pick the asset for your OS, architecture and libc from the release, so
there is nothing to configure. Pass `--matching` (or `[matching=...]` in mise)
to name a specific asset when you want one other than the default.

## Install using Nix

The repository is a flake, so it installs straight from the source tree:

```shell
nix run github:ptweezy/cronstable -- --version
nix profile install github:ptweezy/cronstable
```

This builds cronstable against nixpkgs' Python and dependency set rather than
unpacking a release binary, so it needs no writable temp directory at startup.
The flake covers Linux and macOS on x86_64 and aarch64.

## Install using a binary

A self-contained binary can be downloaded from
<https://github.com/ptweezy/cronstable/releases>. Python is not required on the
target system: the executable embeds it. Every release attaches the following
assets, each built for its own platform and architecture:

| Asset | Platform | libc / arch | Notes |
| --- | --- | --- | --- |
| `cronstable-linux-amd64` | Linux | glibc, x86_64 | glibc 2.17 or newer: RHEL, Alma and Rocky 7 onward, Debian 8 onward, Ubuntu 14.04 onward, Amazon Linux 2 and 2023, SLES 12 onward. |
| `cronstable-linux-arm64` | Linux | glibc, arm64 | glibc 2.17 or newer on arm64. |
| `cronstable-linux-i686` | Linux | glibc, 32-bit x86 | 32-bit x86 (i686), glibc 2.36 or newer: Debian 12 i386 onward. |
| `cronstable-linux-armv7` | Linux | glibc, 32-bit ARM | 32-bit ARM (armv7), glibc 2.31 or newer: Raspberry Pi OS bullseye, Debian 11, Ubuntu 20.04 onward. Raspberry Pi 2 and newer. |
| `cronstable-linux-armv6` | Linux | glibc, 32-bit ARM | ARMv6 hard-float, glibc 2.36 or newer: the Raspberry Pi 1, Zero and Zero W running Raspberry Pi OS. |
| `cronstable-linux-armel` | Linux | glibc, 32-bit ARM | ARMv5 soft-float (Debian armel), glibc 2.36 or newer: Kirkwood devices such as the SheevaPlug, QNAP TS-x1x, D-Link DNS-320, Zyxel NSA325 and Pogoplug. |
| `cronstable-linux-ppc64le` | Linux | glibc, ppc64le | 64-bit little-endian POWER (IBM POWER), glibc 2.17 or newer. |
| `cronstable-linux-s390x` | Linux | glibc, s390x | IBM Z (s390x, big-endian), glibc 2.17 or newer. |
| `cronstable-linux-riscv64` | Linux | glibc, riscv64 | 64-bit RISC-V, glibc 2.41 or newer: Debian 13 onward. |
| `cronstable-linux-loong64` | Linux | glibc, loongarch64 | LoongArch, glibc 2.41 or newer. New-world ABI only; the older Loongnix, Kylin and UOS fleets run a different, incompatible ABI. |
| `cronstable-linux-mips64le` | Linux | glibc, mips64el | 64-bit little-endian MIPS, glibc 2.36 or newer, such as Loongson and Cavium Octeon hardware. |
| `cronstable-linux-amd64-musl` | Linux | musl, x86_64 | For Alpine and other musl hosts. |
| `cronstable-linux-arm64-musl` | Linux | musl, arm64 | For Alpine and other musl hosts. |
| `cronstable-linux-i686-musl` | Linux | musl, 32-bit x86 | 32-bit x86 (i686) for Alpine and other musl hosts. |
| `cronstable-linux-armv7-musl` | Linux | musl, 32-bit ARM | 32-bit ARM (armv7) for Alpine and other musl hosts. |
| `cronstable-linux-ppc64le-musl` | Linux | musl, ppc64le | 64-bit little-endian POWER for Alpine and other musl hosts. |
| `cronstable-linux-s390x-musl` | Linux | musl, s390x | IBM Z (s390x) for Alpine and other musl hosts. |
| `cronstable-linux-riscv64-musl` | Linux | musl, riscv64 | 64-bit RISC-V for Alpine and other musl hosts. |
| `cronstable-linux-armv6-musl` | Linux | musl, 32-bit ARM | ARMv6 hard-float for Alpine on that hardware. On Raspberry Pi OS use the glibc `armv6` build instead. |
| `cronstable-linux-loong64-musl` | Linux | musl, loongarch64 | LoongArch for Alpine and other musl hosts. New-world ABI only. |
| `cronstable-linux-<arch>.deb` | Linux | glibc | Debian package for `amd64`, `arm64`, `i686`, `armv7`, `ppc64le`, `s390x` and `riscv64`. Installs the binary, a systemd unit and `/etc/cronstable.d`. |
| `cronstable-linux-<arch>.rpm` | Linux | glibc | RPM package for the same seven architectures, with the same contents. |
| `cronstable-linux-<arch>.apk` | Linux | musl | Alpine package for `amd64`, `arm64`, `i686`, `armv7`, `armv6`, `ppc64le`, `s390x`, `riscv64` and `loong64`. Installs the binary, an OpenRC service and `/etc/cronstable.d`. |
| `cronstable-macos-arm64` | macOS | Apple Silicon (arm64) | Developer ID signed and notarized. |
| `cronstable-macos-amd64` | macOS | Intel (x86_64) | Developer ID signed and notarized. |
| `cronstable-freebsd-amd64` | FreeBSD | x86_64 | For FreeBSD 14 and 15 hosts, including TrueNAS, pfSense and OPNsense. |
| `cronstable-freebsd-arm64` | FreeBSD | arm64 | For FreeBSD 14 and 15 hosts on arm64. |
| `cronstable-freebsd-<arch>.pkg` | FreeBSD | amd64, arm64 | FreeBSD package. Installs the binary, an `rc.d` script and `/usr/local/etc/cronstable.d`. |
| `cronstable-openbsd-amd64` | OpenBSD | x86_64 | For OpenBSD 7.9. OpenBSD gives no cross-release ABI guarantee, so this asset tracks one release. |
| `cronstable-netbsd-amd64` | NetBSD | x86_64 | For NetBSD 11.0. |
| `cronstable-illumos-amd64` | illumos | x86_64 | Built on OmniOS r151054 LTS; the illumos ABI is shared, so it also runs on OpenIndiana and in SmartOS zones. |
| `cronstable-windows-amd64.exe` | Windows | x64 (amd64) | Self-contained `.exe`. The target needs no Python. |
| `cronstable-windows-arm64.exe` | Windows | ARM64 | Self-contained `.exe`. The target needs no Python. |
| `cronstable-windows-i686.exe` | Windows | 32-bit x86 | Self-contained `.exe` for 32-bit Windows. |
| `cronstable-windows-amd64.zip` | Windows | x64 (amd64) | One-directory build: a `cronstable\` folder with `cronstable.exe` and `_internal\`. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-arm64.zip` | Windows | ARM64 | One-directory build. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-i686.zip` | Windows | 32-bit x86 | One-directory build. Runs in place and can host the [Windows service](Windows-Service). |
| `cronstable-windows-amd64.msi` | Windows | x64 (amd64) | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |
| `cronstable-windows-arm64.msi` | Windows | ARM64 | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |
| `cronstable-windows-i686.msi` | Windows | 32-bit x86 | Machine-wide installer. Registers the [Windows service](Windows-Service). See [Windows MSI](Windows-MSI). |

### Which libc version each build needs

A binary embeds Python but not the C library, so the one number that decides
whether it starts is the oldest glibc or musl it accepts. Each build declares
that number, and CI re-derives it from the frozen bytes on every release, so the
table above is measured rather than estimated.

`amd64`, `arm64`, `ppc64le` and `s390x` need glibc 2.17, which reaches every
glibc distribution still in production, including the RHEL family from 7 onward
and Amazon Linux 2. They are built inside manylinux2014 containers against a
[python-build-standalone](https://github.com/astral-sh/python-build-standalone)
interpreter, which is where that number comes from. `armv7` needs glibc 2.31;
`i686`, `armv6`, `armel` and `mips64le` need 2.36; `riscv64` and `loong64` need
2.41. Each is set by the oldest base image carrying a working toolchain for that
architecture.

On 32-bit ARM the glibc version is only half the requirement, because there are
three incompatible ABIs. `armv7` is hard-float ARMv7 and covers the Raspberry Pi
2 and newer plus Debian's own armhf port. `armv6` is hard-float ARMv6, which is
what the Raspberry Pi 1, Zero and Zero W need: their ARM1176 core has neither
ARMv7 instructions nor a VFPv3 unit, so an armv7 binary faults on them. `armel`
is soft-float ARMv5 for Kirkwood hardware, which has no floating-point unit at
all. CI asserts all three from the frozen bytes, per bundled object, so a binary
cannot ship under the wrong one.

The musl builds need musl 1.2.5 or newer, which is Alpine 3.20 and later. They
are built inside an Alpine container pinned to one Alpine release, so the
requirement moves only when that pin does.

Raspberry Pi OS is glibc, so the binary for a Pi is always one of the glibc
builds: `cronstable-linux-armv6` on a Pi 1, Zero or Zero W, `armv7` on a 32-bit
install of anything newer, and `arm64` on a 64-bit install, which is the default
on a Pi 3 and later. The `-musl` ARM builds are for Alpine on that hardware.

**The `-musl` builds are not a fallback for an older glibc host.** They are
dynamically linked against musl and name `/lib/ld-musl-<arch>.so.1` as their
interpreter, so on a glibc machine the kernel finds no interpreter and the shell
prints `not found`, which reads like a corrupt download rather than the
wrong-libc error it is. When your glibc is older than a build needs, install
from PyPI, use the container image, or use the `.deb` or `.rpm`, which declines
the install outright instead of failing at first run.

### How each architecture is built

The `i686`, `armv7`, `ppc64le` and `s390x` builds, both glibc and musl, extend
the 64-bit `amd64`/`arm64` binaries to 32-bit x86, 32-bit ARM, POWER, and IBM Z
hosts. They build inside a container: `amd64`, `arm64` and `i686` natively on
their runners, the rest under QEMU emulation. The `riscv64` builds cover 64-bit
RISC-V for both glibc and musl. The musl-only `armv6` build extends to older
32-bit ARM, such as a Raspberry Pi 1 or Zero. There is no glibc `armv6` build.

Each build bundles the optional extras its build lane can take a wheel for.
`cryptography`, which carries post-quantum push sealing, has the shortest
reach: `linux-amd64`, `linux-arm64`, `linux-armv7`, `linux-amd64-musl`,
`linux-arm64-musl`, `macos-arm64`, and `windows-amd64` seal the `xwing` suite,
and every other build seals `x25519` only. See
[Push notifications](Push-Notifications).

The `mips64le` build covers 64-bit little-endian MIPS on glibc hosts. It is
built in an emulated Debian bookworm container, the last Debian suite that
carries this port, pinned to a snapshot of that archive because bookworm's LTS
phase publishes no further mips64el updates. Everything in it is compiled from
source, because no MIPS wheels are published. `orjson` is the one piece that
does not build there, so that binary uses the standard library JSON encoder.
There is no musl MIPS build, because Alpine has no MIPS port.

The `loong64` builds cover LoongArch for both libcs, compiled from source under
emulation. They target the new-world ABI that upstream Debian and Alpine use.
Hardware running an old-world distribution (Loongnix, Kylin, UOS) is a different
ABI, and these binaries do not run there.

The glibc `armv6` and `armel` builds compile from source too, on Raspbian
bookworm and Debian bookworm armel respectively, against the Python 3.11 those
suites package. Both run under an emulator pinned to the real target core, an
ARM1176 for `armv6` and an ARM926 for `armel`, so an instruction the hardware
lacks faults during the build rather than on a user's machine.

macOS builds cover both Apple Silicon and Intel.

The FreeBSD builds cover amd64 and arm64 and run on FreeBSD 14 and 15
hosts, including appliance systems such as TrueNAS, pfSense and OPNsense.
They are built in a FreeBSD 14 virtual machine. PyPI publishes no FreeBSD
wheels, so the optional speedups are compiled from source; the arm64 build
ships without `orjson` and uses the standard library JSON encoder.

The OpenBSD, NetBSD and illumos builds are amd64, each built in a virtual
machine of that system, since no runner offers one and PyInstaller cannot
cross-compile. The OpenBSD binary is tied to OpenBSD 7.9: that system offers no
cross-release ABI guarantee, so a new asset follows each release. It ships
without `orjson`, because OpenBSD's Rust is older than that package's minimum,
and uses the standard library JSON encoder. The illumos binary is built on
OmniOS r151054 LTS and runs anywhere the illumos ABI does, including
OpenIndiana and SmartOS zones; nothing is prebuilt for illumos, so it ships
without `orjson` and without the push extra.

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
