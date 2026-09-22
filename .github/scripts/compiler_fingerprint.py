"""Fingerprint the installed target toolchain before reusing compiler work."""

import hashlib
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def fingerprint():
    digest = hashlib.sha256(sys.version.encode())
    commands = [shlex.split(os.environ.get("CC", "cc")) + ["--version"]]
    if shutil.which("rustc"):
        commands.append(["rustc", "-vV"])
    for tool, args in (
        ("apk", ["info", "-v"]),
        ("dpkg-query", ["-W"]),
        ("rpm", ["-qa"]),
    ):
        if shutil.which(tool):
            commands.append([tool, *args])
            break
    for command in commands:
        result = subprocess.run(command, check=True, capture_output=True)
        digest.update(b"\n".join(sorted(result.stdout.splitlines())))
    for key in (
        "CFLAGS",
        "LDFLAGS",
        "RUSTFLAGS",
        "OPENSSL_DIR",
        "OPENSSL_LIB_DIR",
        "OPENSSL_STATIC",
    ):
        digest.update((key + "=" + os.environ.get(key, "")).encode())
    # Source-built OpenSSL isn't in the system package database.
    if prefix := os.environ.get("OPENSSL_DIR"):
        if prefix != "/usr":
            for archive in sorted(Path(prefix).glob("lib*/lib*.a")):
                with archive.open("rb") as source:
                    digest.update(
                        hashlib.file_digest(source, "sha256").digest()
                    )
    return digest.hexdigest()


if __name__ == "__main__":
    print(fingerprint())
