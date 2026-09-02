#!/bin/sh
# Build a static OpenSSL into PREFIX for a cryptography source build.
#
# cryptography compiles its ML-KEM (the `xwing` push suite) only against
# OpenSSL 3.5 or newer. The build lanes whose base image predates that
# (manylinux2014 is CentOS 7 with 1.0.2; Debian bookworm has 3.0) and whose
# arch has no cryptography wheel get one this way, then point the sdist
# build at it with OPENSSL_DIR=PREFIX and OPENSSL_STATIC=1, so the frozen
# bundle links the library in and carries no libssl of its own. Under QEMU
# the build takes on the order of an hour, which is why callers keep PREFIX
# somewhere their cache step persists: an existing libcrypto.a is trusted
# as a finished build and the script returns at once.
#
# Pinned by version and checksum, fetched from the project's GitHub
# releases (the same bytes openssl.org serves). Bump VERSION and SHA256
# together; the workflow cache keys hash this file, so a bump rebuilds.
#
# Usage: build_openssl.sh PREFIX
#
# Needs perl, make, a C compiler, and curl or wget. No `set -e` around the
# fetch: retry.sh handles the network hop when it is on hand.
set -u

VERSION=3.5.8
SHA256=a8f84a39918ec6415ce765d9b429d313ba97b8143169c172e734b9514464f5b2

prefix="${1:?usage: build_openssl.sh PREFIX}"

if [ -f "$prefix/lib/libcrypto.a" ]; then
    echo "build_openssl.sh: $prefix already holds a build; skipping"
    exit 0
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

# `config` picks the target from the machine it runs on, which under
# docker --platform or QEMU is the one the binary is for. Static only,
# position independent (the archive is linked into a shared extension),
# and with --libdir=lib so the result lands in lib/ on every distro rather
# than lib64/ on some. Docs and tests are skipped: nothing here reads them
# and under emulation they cost real time.
set -e
./config --prefix="$prefix" --libdir=lib no-shared no-docs no-tests -fPIC
jobs=$(nproc 2>/dev/null || echo 2)
make -j"$jobs" build_libs
make install_dev
set +e

cd / && rm -rf "$work"
echo "build_openssl.sh: OpenSSL $VERSION installed into $prefix"
