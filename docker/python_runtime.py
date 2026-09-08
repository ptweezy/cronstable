"""Select a baseline interpreter or install a real amd64v3 runtime.

Shared by the release binaries and all Dockerfiles. Linux uses digest-pinned
python-build-standalone; other systems compile the same CPython release. The
prefix's python-path file is written only after the runtime passes its probes.
Baseline mode records the calling interpreter and downloads nothing.

Pins: https://github.com/astral-sh/python-build-standalone/releases/tag/20260814
      https://www.python.org/downloads/release/python-3147/
"""

import argparse
import hashlib
import os
import platform
import shlex
import shutil
import struct
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

VERSION = "3.14.7"
SOURCE_SHA = "3b48dac8fb59f62eaa67ac83c1eb12bda1b7a08406dd286e252c11a66be27f81"
PBS_RELEASE = "20260814"
PBS_SHA = {
    "gnu": "8fc4a6bd7422364d815844616de31bfb5c38743815810fd9a8a4bc278684f6fc",
    "musl": "12656d3adb74e2dfbbc7f411707cad30e6fd89e19ffd3a6579dd0770f054bd42",
}
MARCH = "-march=x86-64-v3"


def download(url, sha, target):
    """Retry transport failures only; a digest mismatch is a hard failure."""
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                with target.open("wb") as output:
                    shutil.copyfileobj(response, output)
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    if digest != sha:
        raise RuntimeError(f"SHA-256 mismatch for {url}: {digest} != {sha}")


def run(command, **kwargs):
    print("+ " + shlex.join(map(str, command)), flush=True)
    subprocess.run(list(map(str, command)), check=True, **kwargs)


def verify(python, *, source_windows=False):
    # --version alone misses absent SSL, sqlite and libffi modules.
    # Windows has no CFLAGS in sysconfig; its compilation is asserted below.
    probe = """
import asyncio, ctypes, os, socket, sqlite3, ssl, struct, sys, sysconfig, zlib
assert struct.calcsize('P') == 8, 'amd64v3 requires a 64-bit interpreter'
assert sysconfig.get_config_var('Py_ENABLE_SHARED') or os.name == 'nt'
flags = sysconfig.get_config_var('CFLAGS') or ''
if sys.argv[1] != 'windows':
    assert '-march=x86-64-v3' in flags.split(), flags
assert sqlite3.connect(':memory:').execute('select 1').fetchone() == (1,)
ssl.create_default_context()
asyncio.run(asyncio.sleep(0))
print('amd64v3 runtime verified:', ssl.OPENSSL_VERSION, flags)
"""
    run([python, "-c", probe, "windows" if source_windows else "posix"])


def install_pbs(prefix, libc):
    name = (
        f"cpython-{VERSION}+{PBS_RELEASE}-x86_64_v3-unknown-linux-{libc}"
        "-install_only.tar.gz"
    )
    url = f"https://github.com/astral-sh/python-build-standalone/releases/download/{PBS_RELEASE}/{name}"
    with tempfile.TemporaryDirectory(prefix="cronstable-pbs-") as tmp:
        archive = Path(tmp) / "python.tar.gz"
        download(url, PBS_SHA[libc], archive)
        with tarfile.open(archive) as tar:
            tar.extractall(tmp, filter="data")
        shutil.copytree(
            Path(tmp) / "python", prefix, dirs_exist_ok=True, symlinks=True
        )
    # PBS's build-time clang/musl-clang paths do not exist in distro builders.
    # Retain its v3 CFLAGS, but use the container's native C/C++ toolchain for
    # dependencies built from source. Wheels remain compatible x86_64.
    for config in prefix.glob("lib/python*/_sysconfigdata_*.py"):
        with config.open("a", encoding="utf-8") as output:
            output.write(
                "\nbuild_time_vars.update(CC='cc', CXX='c++', AR='ar', "
                "LDSHARED='cc -shared')\n"
            )
    return prefix / "bin/python3"


def build_source(prefix, openssl=None):
    # Keep the source outside the checkout so VM copyback and Docker contexts
    # never acquire it. Windows runs from PCbuild and needs the source Lib/.
    source = prefix / "source"
    with tempfile.TemporaryDirectory(prefix="cronstable-cpython-") as tmp:
        archive = Path(tmp) / "python.tar.xz"
        download(
            f"https://www.python.org/ftp/python/{VERSION}/Python-{VERSION}.tar.xz",
            SOURCE_SHA,
            archive,
        )
        with tarfile.open(archive) as tar:
            tar.extractall(tmp, filter="data")
        shutil.move(str(Path(tmp) / f"Python-{VERSION}"), source)
    env = os.environ.copy()
    if os.name == "nt":
        # Put the target in CPython's common C/C++ property sheet so MSBuild
        # records it in the compiler command log. MSVC AVX2 is within v3.
        props = source / "PCbuild/pyproject.props"
        body = props.read_text(encoding="utf-8")
        old = (
            "<AdditionalOptions>/utf-8 "
            "%(AdditionalOptions)</AdditionalOptions>"
        )
        if body.count(old) != 1:
            raise RuntimeError("CPython's common compiler options changed")
        props.write_text(
            body.replace(old, old.replace("/utf-8", "/utf-8 /arch:AVX2")),
            encoding="utf-8",
        )
        env["PYTHON"] = sys.executable
        run(
            [
                source / "PCbuild/build.bat",
                "-p",
                "x64",
                "-c",
                "Release",
                "-e",
                "--no-tkinter",
            ],
            env=env,
        )
        python = source / "PCbuild/amd64/python.exe"
        # Confirm the actual core compile consumed the flag, not just the
        # driver environment. MSBuild's CL command log is UTF-16.
        logs = list(
            source.glob("PCbuild/obj/**/pythoncore*/**/CL.command.1.tlog")
        )
        if not logs or not any(
            "/arch:avx2" in p.read_text(encoding="utf-16").lower()
            for p in logs
        ):
            raise RuntimeError("pythoncore's compiler log has no /arch:AVX2")
        return python

    system = platform.system()
    cc = env.get("CC", sysconfig.get_config_var("CC") or "cc")
    env["CFLAGS"] = f"{env.get('CFLAGS', '')} -O3 {MARCH}"
    # Use the same CPU target for source-built Rust extensions in the callers.
    # No -march=native: hosted runners can expose instructions newer than v3.
    configure = [
        "./configure",
        f"--prefix={prefix}",
        "--enable-shared",
        "--with-ensurepip=install",
    ]
    if system == "Darwin":
        env.setdefault("MACOSX_DEPLOYMENT_TARGET", "15.0")
    else:
        env["LDFLAGS"] = f"{env.get('LDFLAGS', '')} -Wl,-rpath,{prefix}/lib"
    if system == "SunOS":
        # illumos names even its 64-bit x86 host i86pc and keeps 64-bit
        # libraries/pkg-config metadata below amd64, beside the 32-bit ABI.
        env["CFLAGS"] += " -m64"
        env["LDFLAGS"] += " -m64"
        env["PKG_CONFIG_PATH"] = "/usr/lib/amd64/pkgconfig:" + env.get(
            "PKG_CONFIG_PATH", ""
        )
    if openssl:
        configure.extend(
            [f"--with-openssl={openssl}", "--with-openssl-rpath=auto"]
        )
    elif system == "OpenBSD":
        # The alternate OpenSSL port has split include/lib paths rather than
        # a conventional prefix. Give configure a private conventional view.
        ssl_prefix = prefix / "openssl"
        ssl_prefix.mkdir()
        (ssl_prefix / "include").symlink_to("/usr/local/include/eopenssl35")
        (ssl_prefix / "lib").symlink_to("/usr/local/lib/eopenssl35")
        configure.extend(
            [
                f"--with-openssl={ssl_prefix}",
                "--with-openssl-rpath=/usr/local/lib/eopenssl35",
            ]
        )
    elif system in ("FreeBSD", "NetBSD"):
        ssl_prefix = "/usr/pkg" if system == "NetBSD" else "/usr/local"
        configure.extend(
            [f"--with-openssl={ssl_prefix}", "--with-openssl-rpath=auto"]
        )
    env["CC"] = cc
    make = shutil.which("gmake") or "make"
    run(configure, cwd=source, env=env)
    run([make, f"-j{min(os.cpu_count() or 2, 4)}"], cwd=source, env=env)
    run([make, "install"], cwd=source, env=env)
    shutil.rmtree(source)
    return prefix / "bin/python3"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--variant", choices=("baseline", "amd64v3"), default="baseline"
    )
    parser.add_argument("--prefix", type=Path)
    parser.add_argument("--libc", choices=("gnu", "musl"), default="gnu")
    parser.add_argument("--openssl")
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the calling v3 interpreter only",
    )
    args = parser.parse_args()
    if args.check:
        verify(sys.executable, source_windows=os.name == "nt")
        return
    if args.prefix is None:
        parser.error("--prefix is required when installing")
    prefix = args.prefix.resolve()
    prefix.mkdir(parents=True, exist_ok=True)
    python = Path(sys.executable)
    if args.variant == "amd64v3":
        machine = platform.machine().lower()
        x64 = machine in ("amd64", "x86_64") or (
            platform.system() == "SunOS" and machine == "i86pc"
        )
        if not x64 or struct.calcsize("P") != 8:
            raise RuntimeError("amd64v3 requires a native x86_64 build host")
        if platform.system() == "Linux":
            python = install_pbs(prefix, args.libc)
        else:
            python = build_source(prefix, args.openssl)
        verify(python, source_windows=os.name == "nt")
    (prefix / "python-path").write_text(str(python) + "\n", encoding="utf-8")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(
            os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8"
        ) as output:
            output.write(f"python-path={python}\n")


if __name__ == "__main__":
    main()
