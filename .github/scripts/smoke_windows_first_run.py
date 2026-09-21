"""Check the installed Windows executable with an unconfigured profile."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def check(command):
    with tempfile.TemporaryDirectory(prefix="cronstable-first-run-") as tmp:
        root = Path(tmp)
        env = os.environ.copy()
        for key in ("APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "USERPROFILE"):
            directory = root / key
            directory.mkdir()
            env[key] = str(directory)
        env["PYTHONUTF8"] = "1"
        for arguments, expected, message in (
            ([], 0, "Run `cronstable init`"),
            (["--validate-config"], 1, "configuration file not found"),
        ):
            result = subprocess.run(
                [*command, *arguments],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
            )
            label = " ".join(arguments) or "no arguments"
            print(f"First launch ({label}): exit {result.returncode}")
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
            if result.returncode != expected:
                raise RuntimeError(f"{label}: expected exit {expected}")
            stream = result.stderr if arguments else result.stdout
            if message not in stream or "usage: cronstable" not in stream:
                raise RuntimeError(f"{label}: missing setup guidance or help")
            if not arguments and result.stderr:
                raise RuntimeError("Bare first launch wrote to stderr")
            if any(root.glob("*/cronstable")):
                raise RuntimeError("First launch created configuration")


if __name__ == "__main__":
    if sys.platform != "win32" or len(sys.argv) != 2:
        sys.exit("Usage on Windows: smoke_windows_first_run.py EXECUTABLE")
    check([str(Path(sys.argv[1]).resolve(strict=True))])
