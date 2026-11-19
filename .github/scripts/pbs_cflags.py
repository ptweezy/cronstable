"""Remove frame-pointer flags that the image compiler rejects from sysconfig.

    /opt/pbs/bin/python3 .github/scripts/pbs_cflags.py

Run it with the python-build-standalone interpreter that install_pbs.sh
unpacked. CPython's configure tests each frame-pointer flag against the
compiler that builds CPython, and records the ones it accepts in BASECFLAGS.
sysconfig then hands that list to every extension build.
python-build-standalone builds CPython with clang, and the manylinux images
build extensions with gcc, which rejects some of clang's flags. On armv7,
-mno-thumb is clang's spelling of gcc's -marm, and gcc has no
-mno-omit-leaf-frame-pointer for 32-bit ARM. While either flag stays
recorded, every extension source build fails.

The script tests each recorded frame-pointer flag the way configure does: it
compiles an empty file with the flag and -Werror, using the compiler in $CC
(gcc when unset). It removes each rejected flag from the sysconfig data
module, its JSON copy, the config Makefile and python-config, then checks
that a fresh interpreter no longer reports any of them. Only the standard
library is used.
"""

import glob
import importlib
import os
import re
import shlex
import subprocess
import sys
import sysconfig

# Every flag that CPython's configure can add to BASECFLAGS for frame
# pointers, across all architectures.
FRAME_POINTER_FLAGS = (
    "-fno-omit-frame-pointer",
    "-mno-omit-leaf-frame-pointer",
    "-marm",
    "-mno-thumb",
    "-mbackchain",
)


def accepts(compiler, flag):
    """Return whether the compiler builds an empty C file with this flag."""
    result = subprocess.run(
        [*compiler, "-Werror", flag, "-x", "c", "-c", "-", "-o", os.devnull],
        input="",
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def strip_flags(text, flags):
    """Remove each flag wherever it appears in text as a whole token.

    A token starts after whitespace, a quote or "=", and ends before
    whitespace, a quote or a backslash. One space or tab after the token
    goes with it.
    """
    for flag in flags:
        pattern = r"(?<![^\s'\"=])" + re.escape(flag) + r"(?![^\s'\"\\])[ \t]?"
        text = re.sub(pattern, "", text)
    return text


def recorded_flags():
    """Return the frame-pointer flags that this sysconfig records."""
    tokens = set()
    for value in sysconfig.get_config_vars().values():
        if isinstance(value, str):
            tokens.update(value.split())
    return [flag for flag in FRAME_POINTER_FLAGS if flag in tokens]


def config_files():
    """Return the sysconfig module and the files that copy its build flags."""
    module = importlib.import_module(sysconfig._get_sysconfigdata_name())
    paths = [module.__file__]
    here = os.path.dirname(module.__file__)
    paths += sorted(glob.glob(os.path.join(here, "_sysconfig_vars_*.json")))
    paths.append(sysconfig.get_makefile_filename())
    paths.append(
        os.path.join(
            sysconfig.get_path("scripts"),
            "python{}-config".format(sysconfig.get_python_version()),
        )
    )
    return module, [path for path in paths if os.path.isfile(path)]


def main():
    compiler = shlex.split(os.environ.get("CC") or "gcc")
    try:
        rejected = [
            flag for flag in recorded_flags() if not accepts(compiler, flag)
        ]
    except FileNotFoundError:
        raise SystemExit(
            "pbs_cflags.py: no C compiler {!r}".format(compiler[0])
        ) from None
    if not rejected:
        print(
            "pbs_cflags.py: {} accepts every recorded frame-pointer "
            "flag".format(compiler[0])
        )
        return

    module, paths = config_files()
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        stripped = strip_flags(text, rejected)
        if stripped != text:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(stripped)
            print(
                "pbs_cflags.py: removed {} from {}".format(
                    " ".join(rejected), path
                )
            )
    # Bytecode compiled in unchecked-hash mode would keep the old flags.
    cache = os.path.join(os.path.dirname(module.__file__), "__pycache__")
    for pyc in glob.glob(os.path.join(cache, module.__name__ + ".*.pyc")):
        os.remove(pyc)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('CFLAGS'))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    left = [flag for flag in rejected if flag in result.stdout.split()]
    if left:
        raise SystemExit(
            "pbs_cflags.py: sysconfig still records {}".format(" ".join(left))
        )


if __name__ == "__main__":
    main()
