#!/bin/sh
# Build a static OpenSSL into PREFIX for a cryptography source build.
#
# cryptography compiles its ML-KEM (the `xwing` push suite) only against
# OpenSSL 3.5 or newer. The build lanes whose base image predates that
# (manylinux2014 is CentOS 7 with 1.0.2, manylinux_2_28 is AlmaLinux 8
# with 1.1.1, Debian bookworm has 3.0) and whose arch has no cryptography
# wheel get one this way, then point the sdist build at it with
# OPENSSL_DIR=PREFIX and OPENSSL_STATIC=1, so the extension links the
# library in and needs no libssl at run time. Under QEMU the build takes
# about an hour, which is why callers keep PREFIX somewhere their cache
# step persists: the script treats an existing libcrypto.a as a finished
# build and returns immediately.
#
# Pinned by version and checksum, fetched from the project's GitHub
# releases (the same bytes openssl.org serves). Bump VERSION and SHA256
# together; the workflow cache keys hash this file, so a bump rebuilds.
#
# Usage: build_openssl.sh PREFIX
#
# BUILD_OPENSSL_CONFIG_EXTRA, when set, is appended to the Configure line.
# The ARMv6 wheel build passes `no-asm`: the ARM assembly paths carry code
# for newer cores behind run-time dispatch, and the assembler tags every
# object that holds them above the ARMv6 ceiling the lane enforces, so the
# C paths alone go in there.
#
# Needs perl with IPC::Cmd, Time::Piece, and bigint, plus make, a C
# compiler, and curl or wget. Configure, the Makefile it generates, and the
# s390x assembler generators import those three modules; RHEL-family
# distributions package them apart from perl, as perl-IPC-Cmd,
# perl-Time-Piece, and perl-bignum, and the manylinux images leave them
# out. No `set -e` around the fetch: retry.sh handles the network hop when
# it is on hand.
set -u

VERSION=3.5.8
SHA256=a8f84a39918ec6415ce765d9b429d313ba97b8143169c172e734b9514464f5b2

prefix="${1:?usage: build_openssl.sh PREFIX}"

if [ -f "$prefix/lib/libcrypto.a" ]; then
    echo "build_openssl.sh: $prefix already holds a build; skipping"
    exit 0
fi

# The check runs before the fetch so a failure names the modules instead
# of surfacing as a perl compilation error inside Configure.
if ! command -v perl >/dev/null 2>&1; then
    echo "build_openssl.sh: perl is not installed" >&2
    exit 1
fi
if ! perl -MIPC::Cmd -MTime::Piece -Mbigint -e 1 2>/dev/null; then
    echo "build_openssl.sh: perl lacks IPC::Cmd, Time::Piece, or bigint" \
        "(perl-IPC-Cmd, perl-Time-Piece, and perl-bignum on RHEL-family" \
        "distributions)" >&2
    exit 1
fi

tarball="openssl-$VERSION.tar.gz"
url="https://github.com/openssl/openssl/releases/download/openssl-$VERSION/$tarball"
work=$(mktemp -d)
cd "$work" || exit 1

fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl --proto '=https' --tlsv1.2 -sSfLo "$tarball" "$url"
    else
        wget -qO "$tarball" "$url"
    fi
}
if [ -f "$(dirname "$0")/../.github/scripts/retry.sh" ]; then
    . "$(dirname "$0")/../.github/scripts/retry.sh"
    retry 3 fetch || exit 1
else
    fetch || exit 1
fi

# The checksum is the whole trust story for the fetch: a wrong file is a
# hard stop, never a build against unknown bytes.
actual=$(sha256sum "$tarball" | cut -d' ' -f1)
if [ "$actual" != "$SHA256" ]; then
    echo "build_openssl.sh: checksum mismatch for $tarball" >&2
    echo "  expected $SHA256" >&2
    echo "  actual   $actual" >&2
    exit 1
fi

tar xzf "$tarball" || exit 1
cd "openssl-$VERSION" || exit 1

# `config` picks the target from uname, which reports the kernel: under
# docker --platform an i686 userland on an x86_64 host still says x86_64,
# and config, which finds a 32-bit compiler under that name, selects the
# x32 ABI, which the i686 toolchain cannot build. The compiler reports the
# ABI it targets, so where the two disagree this script names the target
# itself, the one config selects natively on i686; everywhere else config's
# own guess stands.
target=
if [ "$(uname -m)" = x86_64 ] &&
    ${CC:-cc} -dM -E -x c /dev/null 2>/dev/null | grep -q __i386__; then
    target=linux-x86
fi

# Static only, position independent (the archive is linked into a shared
# extension), and with --libdir=lib so the result lands in lib/ on every
# distro rather than lib64/ on some. Docs and tests are skipped: nothing
# here reads them and under emulation they cost real time.
set -e
if [ -n "$target" ]; then
    ./Configure "$target" --prefix="$prefix" --libdir=lib no-shared no-docs no-tests -fPIC ${BUILD_OPENSSL_CONFIG_EXTRA:-}
else
    ./config --prefix="$prefix" --libdir=lib no-shared no-docs no-tests -fPIC ${BUILD_OPENSSL_CONFIG_EXTRA:-}
fi
jobs=$(nproc 2>/dev/null || echo 2)
make -j"$jobs" build_libs
make install_dev
set +e

cd / && rm -rf "$work"
echo "build_openssl.sh: OpenSSL $VERSION installed into $prefix"
