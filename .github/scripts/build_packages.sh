#!/usr/bin/env bash
# Build the .deb, .rpm and .apk packages from the Linux binaries the gate built.
#
#     bash .github/scripts/build_packages.sh <version> <binaries-dir>
#
# Packaging is metadata plus a payload nfpm never executes, so every
# architecture builds here on the amd64 runner with no emulation, in about a
# second for the whole set. The recipes are packaging/nfpm/nfpm.yaml (deb, rpm)
# and packaging/nfpm/apk.yaml (Alpine); this script only supplies the
# per-architecture variables and checks the result.
#
# Four things that fail OPEN and are therefore asserted rather than assumed:
#
#  1. The architecture NAME. nfpm's translation tables have no `i686` or `armv7`
#     key and pass unknown values straight through, so `arch: i686` yields
#     `Architecture: i686`, which is not a Debian architecture. The package
#     builds, uploads and installs nowhere. Every package is read back and its
#     recorded architecture compared against what this table expects.
#
#  2. The libc the payload was built against. nfpm never inspects the binary, so
#     an .apk built from a glibc binary installs cleanly on Alpine and then
#     fails at exec. The two tables below name different source files for that
#     reason: the deb/rpm rows take `cronstable-linux-<arch>` and the apk rows
#     take `cronstable-linux-<arch>-musl`.
#
#  3. The glibc floor. It is a property of the frozen bytes, not of the base
#     image the binary was built in, and the two disagree: five rows build on a
#     glibc far newer than the binary ends up needing. These floors are the ones
#     the build jobs declare and elf_floor.py enforces against the bytes;
#     tests/test_ci_fences.py holds the two lists to each other.
#
#  4. The output NAME. tests/test_ci_fences.py asserts the SHA256SUMS cp list
#     and the release files: list name the same set, and a version-bearing
#     package name would be a different literal token in each. Version-less
#     names also keep the download URLs stable across releases.
set -euo pipefail

VERSION="${1:?usage: build_packages.sh <version> <binaries-dir>}"
BINARIES="${2:?usage: build_packages.sh <version> <binaries-dir>}"

NFPM_VERSION=2.47.0
NFPM_SHA256=0660ca602b2d2d2ae4781a06c692b3eeb9d437ffea05b831d76e41f4a3188783

# arch | nfpm arch | expected Debian architecture | glibc floor
ROWS="
amd64   amd64   amd64   2.17
arm64   arm64   arm64   2.17
i686    386     i386    2.36
armv7   arm7    armhf   2.31
ppc64le ppc64le ppc64el 2.17
s390x   s390x   s390x   2.17
riscv64 riscv64 riscv64 2.41
"

# arch | Alpine architecture, which is also what is passed to nfpm as its
# `arch`. Alpine's names are given directly rather than through nfpm's Go-arch
# keys: unknown values pass through untranslated, so this states the answer
# instead of relying on a lookup that has no key for armhf and would emit
# `armv6`, which is not an Alpine architecture at all.
#
# There is no armel row: Alpine has no armv5 port at all, so that .apk would be
# a well-formed package for an architecture that does not exist. Every other musl
# binary the gate builds gets one, loong64 included, since Alpine has shipped a
# loongarch64 port since 3.21.
APK_ROWS="
amd64   x86_64
arm64   aarch64
i686    x86
armv7   armv7
armv6   armhf
ppc64le ppc64le
s390x   s390x
riscv64 riscv64
loong64 loongarch64
"

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
. "$here/retry.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

tarball="$work/nfpm.tar.gz"
retry 5 curl --proto =https --tlsv1.2 -fsSL --retry 5 --retry-connrefused \
    --retry-delay 5 -o "$tarball" \
    "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${NFPM_VERSION}_Linux_x86_64.tar.gz"
echo "${NFPM_SHA256}  ${tarball}" | sha256sum -c -
tar -xzf "$tarball" -C "$work" nfpm
NFPM="$work/nfpm"
"$NFPM" --version

# Read the architecture back out of an .apk. There is no dpkg-deb equivalent, so
# this reads .PKGINFO out of the control segment directly: an apk is
# concatenated gzip members, and the control tar (which holds .PKGINFO) comes
# first, so the first archive in the stream is the one to look in.
apk_arch() {
    python3 - "$1" <<'PY'
import sys
import tarfile

with tarfile.open(sys.argv[1], "r:gz") as archive:
    for member in archive:
        if member.name != ".PKGINFO":
            continue
        body = archive.extractfile(member).read().decode("utf-8", "replace")
        for line in body.splitlines():
            if line.startswith("arch = "):
                print(line.split("=", 1)[1].strip())
                raise SystemExit(0)
raise SystemExit("no arch in .PKGINFO")
PY
}

while read -r arch nfpm_arch deb_arch floor; do
    [ -n "$arch" ] || continue
    binary="$BINARIES/cronstable-linux-$arch"
    if [ ! -f "$binary" ]; then
        echo "::error::build_packages: $binary is missing" >&2
        exit 1
    fi
    export PKG_VERSION="$VERSION"
    export PKG_ARCH="$nfpm_arch"
    export PKG_GLIBC="$floor"
    export PKG_BINARY="$binary"

    deb="$BINARIES/cronstable-linux-$arch.deb"
    rpm="$BINARIES/cronstable-linux-$arch.rpm"
    "$NFPM" package -f packaging/nfpm/nfpm.yaml -p deb -t "$deb"
    "$NFPM" package -f packaging/nfpm/nfpm.yaml -p rpm -t "$rpm"

    got="$(dpkg-deb -f "$deb" Architecture)"
    if [ "$got" != "$deb_arch" ]; then
        echo "::error::build_packages: $deb declares Architecture $got, expected $deb_arch" >&2
        exit 1
    fi
    depends="$(dpkg-deb -f "$deb" Depends)"
    case "$depends" in
        *"libc6 (>= $floor)"*) ;;
        *)
            echo "::error::build_packages: $deb depends on '$depends', expected the libc6 >= $floor floor" >&2
            exit 1
            ;;
    esac
    echo "packaged $arch: $deb ($got, $depends) and $rpm"
# Fed by redirection rather than a pipe: a pipeline would run the loop in a
# subshell, where a failed assertion cannot take the script down reliably.
done <<< "$ROWS"

while read -r arch alpine_arch; do
    [ -n "$arch" ] || continue
    binary="$BINARIES/cronstable-linux-$arch-musl"
    if [ ! -f "$binary" ]; then
        echo "::error::build_packages: $binary is missing" >&2
        exit 1
    fi
    export PKG_VERSION="$VERSION"
    export PKG_ARCH="$alpine_arch"
    export PKG_BINARY="$binary"

    apk="$BINARIES/cronstable-linux-$arch.apk"
    "$NFPM" package -f packaging/nfpm/apk.yaml -p apk -t "$apk"

    got="$(apk_arch "$apk")"
    if [ "$got" != "$alpine_arch" ]; then
        echo "::error::build_packages: $apk records arch $got, expected $alpine_arch" >&2
        exit 1
    fi
    echo "packaged $arch: $apk ($got, musl)"
done <<< "$APK_ROWS"
