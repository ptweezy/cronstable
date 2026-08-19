#!/bin/sh
# Install a python-build-standalone interpreter into the build container.
#
#     sh install_pbs.sh <url> <sha256> <prefix>
#
# The glibc binary lanes build inside manylinux images, whose own
# /opt/python interpreters are configured --disable-shared (see
# manylinux's docker/build_scripts/build-cpython.sh).  PyInstaller
# hard-requires a shared libpython and raises PythonLibraryNotFoundError
# with no fallback, so those interpreters cannot freeze anything and the
# lane brings its own: python-build-standalone publishes shared-libpython
# builds whose whole ELF set tops out at GLIBC_2.17, which is what puts the
# floor where it is.
#
# The release and its digest are pinned by the caller, one per architecture.
# A moving pin is what shipped an executable-stack libpython in the upstream
# 20260320 build, which then died at runtime on exactly the SELinux-hardened
# hosts a low floor exists to reach, so the digest is checked here and the
# resulting bundle is re-checked by elf_floor.py --no-exec-stack.
#
# POSIX sh: this runs under CentOS 7 bash and Debian dash alike.
set -eu

usage="usage: install_pbs.sh <url> <sha256> <prefix>"
url="${1:?$usage}"
sha="${2:?$usage}"
prefix="${3:?$usage}"

here=$(dirname "$0")
. "$here/retry.sh"

tarball=/tmp/pbs-python.tar.gz
# Only the network hop is retried; an unpack or a digest mismatch is
# deterministic and retrying it just multiplies the failure. retry.sh is the
# whole retry story here: manylinux2014 is CentOS 7, whose curl 7.29 rejects
# --retry-connrefused outright, and a flag it does not know is a hard usage
# error rather than a download attempt.
retry 5 curl --proto =https --tlsv1.2 -fsSL -o "$tarball" "$url"
echo "$sha  $tarball" | sha256sum -c -

# The archive roots at python/, so strip that component and land bin/, lib/
# and include/ directly under the prefix.
mkdir -p "$prefix"
tar -xzf "$tarball" -C "$prefix" --strip-components=1
rm -f "$tarball"

"$prefix/bin/python3" -VV
# Prove the two properties the lane depends on before anything is built
# against this interpreter: a shared libpython (else PyInstaller cannot
# freeze) and a working ssl module (else the whole HTTP stack is dead in a
# way the --version smoke would not catch).
"$prefix/bin/python3" - <<'PY'
import ssl
import sysconfig

if not sysconfig.get_config_var("Py_ENABLE_SHARED"):
    raise SystemExit("install_pbs.sh: interpreter has no shared libpython")
print("install_pbs.sh: shared libpython, " + ssl.OPENSSL_VERSION)
PY
