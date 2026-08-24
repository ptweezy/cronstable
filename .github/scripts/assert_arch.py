"""Assert that a freshly built binary is the architecture its lane claims.

Run by every release lane that can silently produce the wrong architecture:
a Windows row whose 32-bit interpreter did not take, a VM or emulated
container that came up on the wrong image.  Each of those passes every
functional smoke test and still ships a mislabelled asset, so the check reads
the object header rather than trusting the build environment.

    python .github/scripts/assert_arch.py <path> <spec>

    pe:<machine>              COFF machine, hex.  8664 amd64, aa64 arm64,
                              014c i386.
    elf:<class>:<data>:<machine>
                              EI_CLASS (1 = 32-bit, 2 = 64-bit), EI_DATA
                              (1 = little-endian, 2 = big-endian) and
                              e_machine, decimal.  62 x86-64, 183 aarch64,
                              8 MIPS.

All three ELF fields are checked together because e_machine alone is not an
architecture: EM_MIPS covers 32- and 64-bit and both endiannesses.

`file` is deliberately not used.  Its output is prose that differs between
versions and platforms, which is how the first version of this check passed
locally and failed every Windows row in CI.

Exits nonzero with the mismatch spelled out, so a failure names both what was
built and what was expected.
"""

import struct
import sys

USAGE = "usage: assert_arch.py <path> pe:<machine-hex>|elf:<class>:<data>:<machine>"


def _read(path, count):
    with open(path, "rb") as fobj:
        return fobj.read(count)


def check_pe(path, want):
    blob = _read(path, 4096)
    if blob[:2] != b"MZ":
        return "{}: not a PE image (no MZ header)".format(path)
    # e_lfanew at 0x3c points at the PE signature; the COFF machine field is
    # the first thing after it.
    off = struct.unpack_from("<I", blob, 0x3C)[0]
    if blob[off:off + 4] != b"PE\0\0":
        return "{}: not a PE image (no PE signature)".format(path)
    machine = struct.unpack_from("<H", blob, off + 4)[0]
    if machine != want:
        return "{}: COFF machine 0x{:04x}, expected 0x{:04x}".format(
            path, machine, want
        )
    print("{}: COFF machine 0x{:04x}".format(path, machine))
    return None


def check_elf(path, want_class, want_data, want_machine):
    head = _read(path, 20)
    if head[:4] != b"\x7fELF":
        return "{}: not an ELF image".format(path)
    cls, data = head[4], head[5]
    if data not in (1, 2):
        return "{}: invalid EI_DATA {}".format(path, data)
    machine = struct.unpack_from("<H" if data == 1 else ">H", head, 0x12)[0]
    got, want = (cls, data, machine), (want_class, want_data, want_machine)
    if got != want:
        return "{}: ELF class/data/machine {}/{}/{}, expected {}/{}/{}".format(
            path, *(got + want)
        )
    print("{}: ELF class {} data {} machine {}".format(path, cls, data, machine))
    return None


def main(argv):
    if len(argv) != 3:
        return USAGE
    path, spec = argv[1], argv[2]
    kind, _, rest = spec.partition(":")
    try:
        if kind == "pe":
            return check_pe(path, int(rest, 16))
        if kind == "elf":
            fields = [int(part) for part in rest.split(":")]
            if len(fields) != 3:
                return USAGE
            return check_elf(path, *fields)
    except OSError as exc:
        # An unreadable path is the likeliest failure here (a build step that
        # renamed or never produced the artifact), and a bare traceback buries
        # that in CI output.
        return "{}: cannot open: {}".format(path, exc)
    except (ValueError, IndexError, struct.error) as exc:
        return "{}: cannot read as {}: {}".format(path, kind or "?", exc)
    return USAGE


if __name__ == "__main__":
    sys.exit(main(sys.argv))
