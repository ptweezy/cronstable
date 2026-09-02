"""Write the Docker dependency layer's requirement lists from pyproject.toml.

All eight Dockerfiles (the root Dockerfile plus the docker/ variants) COPY
this file into /tmp/deps/ beside pyproject.toml and run it inside the
dependency layer, so the extraction logic lives once instead of being
hand-mirrored per image. Given the path to pyproject.toml, it writes two
files next to it:

- requirements.txt: the core dependencies plus the push-pq and discovery
  extras, so the images and pyproject.toml can never drift and a renamed
  extra fails the build loudly (KeyError) instead of silently shipping
  without it.  push-pq rather than push so the images seal post-quantum
  `xwing` push as well as `x25519`; it carries push's PyNaCl too.
- build-requires.txt: build-system.requires, for the throwaway buildenv
  the project install builds its wheel with.

Every requirement rides through verbatim, marker and all, except
cryptography, which this script resolves itself.  pyproject guards that one
with a PEP 508 marker listing the machines that have a wheel, and a marker
has two blind spots here:

- `platform_machine` is the raw `uname -m`, which reports the KERNEL rather
  than the userland.  The linux/386 rows run i686 binaries natively on an
  x86_64 host, so uname says `x86_64`, the marker passes, and pip then
  correctly looks for an i686 wheel, finds none, and falls back to building
  the sdist, which needs a Rust toolchain the image does not carry.
- A marker cannot see the libc at all, and the two reach different
  distances: cryptography publishes armv7l and ppc64le wheels for manylinux
  and nothing for musllinux.

This script runs on the target container's own interpreter, so it can
answer both questions directly: `struct.calcsize("P")` gives the userland
bitness, and the musl loader gives the libc.  It keeps the requirement only
where a wheel really exists and drops it otherwise.  An image that loses it
still has PyNaCl, so the daemon advertises `x25519` and refuses `xwing`
pairings, which is the designed fail-closed degradation.

Both lists are echoed to stdout so the build log shows what was resolved.
Runs on the venv interpreter, which is Python 3.11+ (tomllib) in every
image.
"""

import glob
import os
import platform
import re
import struct
import sys
import tomllib

EXTRAS = ("push-pq", "discovery")

#: The (machine, libc) pairs cryptography publishes a Linux wheel for, keyed
#: on the machine name pip resolves wheels UNDER rather than the one uname
#: reports.  x86_64 and aarch64 have both manylinux and musllinux wheels;
#: armv7l (manylinux_2_31) and ppc64le (manylinux_2_28) have manylinux only,
#: and every base image here is far above both floors.  s390x, riscv64 and
#: i686 have no wheel at any tag, so no image can give them one.
CRYPTOGRAPHY_WHEELS = frozenset(
    {
        ("x86_64", "glibc"),
        ("x86_64", "musl"),
        ("aarch64", "glibc"),
        ("aarch64", "musl"),
        ("armv7l", "glibc"),
        ("ppc64le", "glibc"),
    }
)


def wheel_machine(machine, pointer_size):
    """The machine name pip resolves wheels under, from uname and bitness.

    packaging.tags applies exactly this correction before it builds the
    platform tags: a 32-bit interpreter on an x86_64 kernel gets i686 tags,
    and one on an aarch64 kernel gets armv8l tags, which carry armv7l as
    their compatible fallback.  armv7l is the name spelled here because it
    is the one cryptography actually publishes under.
    """
    if pointer_size == 4:
        return {"x86_64": "i686", "aarch64": "armv7l"}.get(machine, machine)
    return machine


def cryptography_has_wheel(machine, pointer_size, libc):
    """Whether cryptography publishes a wheel for this Linux target.

    Pure on purpose: `machine` is the raw `uname -m`, `pointer_size` is
    `struct.calcsize("P")` for the running interpreter, and `libc` is
    `glibc` or `musl`. The platform sniffing all lives in detect_target(),
    which keeps this a table lookup the tests can walk case by case.
    """
    return (wheel_machine(machine, pointer_size), libc) in CRYPTOGRAPHY_WHEELS


def detect_libc():
    """`glibc` or `musl` for the interpreter running this script.

    musl is the side that can be proved: it names its loader in a fixed
    place, so Alpine, the one musl image here, answers for itself. Anything
    else on Linux is glibc.
    """
    return "musl" if glob.glob("/lib/ld-musl-*.so.1") else "glibc"


def detect_target():
    """(machine, pointer_size, libc) here, or None where the marker rules.

    The eight images are all Linux, which is the whole matrix this script
    resolves for. Anywhere else it defers: the requirement goes out as
    pyproject spells it and pip evaluates the marker as usual.
    """
    if not sys.platform.startswith("linux"):
        return None
    return platform.machine(), struct.calcsize("P"), detect_libc()


def resolve_cryptography(line, target):
    """The cryptography requirement to emit, or None to drop it.

    Where a wheel exists the marker comes off: this script has already made
    the platform decision, with more to go on than a marker has, and leaving
    a narrower marker in place would veto the armv7l and ppc64le images it
    just approved.
    """
    if target is None:
        return line
    if not cryptography_has_wheel(*target):
        return None
    # The requirement lines here carry no other semicolon, so the marker is
    # everything past the first one.
    return line.split(";", 1)[0].strip()


def marker_can_hold_on_linux(line):
    """Whether a requirement's marker admits some Linux target at all.

    pyproject splits cryptography across marker lines: one for Linux and
    the other platforms that take the newest release, one capped for Intel
    macOS and 32-bit Windows.  Only a line that can be true on Linux is
    this script's to resolve; the other is another OS's and must not reach
    an image with its marker stripped.  Probed as Linux x86_64 because the
    split is by OS, and which Linux machines get the wheel is the table's
    decision rather than the marker's.
    """
    if ";" not in line:
        return True
    try:
        from packaging.markers import Marker
    except ImportError:  # the image venv carries pip's vendored copy
        from pip._vendor.packaging.markers import Marker
    return Marker(line.split(";", 1)[1].strip()).evaluate(
        {
            "sys_platform": "linux",
            "platform_system": "Linux",
            "os_name": "posix",
            "platform_machine": "x86_64",
        }
    )


def requirement_name(line):
    """The distribution name at the head of a requirement line."""
    return re.split(r"[\s\[(<>=!~;]", line.strip(), maxsplit=1)[0].lower()


def main(pyproject_path):
    with open(pyproject_path, "rb") as fobj:
        data = tomllib.load(fobj)
    project = data["project"]
    requirements = list(project["dependencies"])
    for extra in EXTRAS:
        requirements += project["optional-dependencies"][extra]
    target = detect_target()
    resolved = []
    for line in requirements:
        if requirement_name(line) == "cryptography":
            if target is not None and not marker_can_hold_on_linux(line):
                continue  # another OS's line; no image is built for it
            line = resolve_cryptography(line, target)
            if line is None:
                sys.stdout.write(
                    "cryptography: no wheel for this image "
                    "(%r); x25519 push only\n" % (target,)
                )
                continue
        resolved.append(line)
    out_dir = os.path.dirname(os.path.abspath(pyproject_path))
    for name, lines in (
        ("requirements.txt", resolved),
        ("build-requires.txt", data["build-system"]["requires"]),
    ):
        body = "".join(line + "\n" for line in lines)
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fobj:
            fobj.write(body)
        sys.stdout.write(body)


if __name__ == "__main__":
    main(sys.argv[1])
