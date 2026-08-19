"""Assert the glibc floor of a frozen PyInstaller binary, from the bytes.

The floor a binary actually carries is an emergent property of the build
environment: it is the highest ``GLIBC_x.y`` symbol version any member of the
bundle asks for, contributed by the interpreter, OpenSSL, libstdc++ and every
wheel that was installed.  Nothing in the build declares it, and a dependency
that starts publishing a higher-tagged wheel raises it with CI green
throughout, because the smoke test runs on a runner whose glibc is newer than
anything the binary needs.

    python .github/scripts/elf_floor.py dist/cronstable --max-glibc 2.17
    python .github/scripts/elf_floor.py dist/cronstable --max-glibc 2.31 --no-exec-stack

The binary is a one-file build, so the bundled shared libraries live inside its
appended CArchive rather than on disk.  This reads the archive directly: locate
the cookie at the tail, walk the table of contents, inflate each member, and
parse ``.gnu.version_r`` on everything that is an ELF image, plus on the
bootloader (the outer file) itself.  Only the standard library is used, so it
runs inside every build container without a toolchain, and no ``readelf`` or
``objdump`` has to exist on the lane.

``--no-exec-stack`` additionally fails on any member whose ``PT_GNU_STACK``
segment is executable.  A libpython built with an executable stack ships fine
and then dies at runtime on an SELinux-hardened host, which is the audience a
low floor exists to reach (python-build-standalone shipped exactly that in its
20260320 release).

Exits nonzero with the offending member and version named, so a failure says
which dependency raised the floor rather than only that it moved.
"""

import argparse
import struct
import sys
import zlib

# PyInstaller's CArchive: an 88-byte cookie at the tail of the executable,
# then a table of contents of variable-length entries.  Both are big-endian.
# (PyInstaller/archive/readers.py, CArchiveReader.)
COOKIE_MAGIC = b"MEI\014\013\012\013\016"
COOKIE_FORMAT = "!8sIIii64s"
COOKIE_LENGTH = struct.calcsize(COOKIE_FORMAT)
TOC_ENTRY_FORMAT = "!IIIIBc"
TOC_ENTRY_LENGTH = struct.calcsize(TOC_ENTRY_FORMAT)

SHT_GNU_VERNEED = 0x6FFFFFFE
PT_GNU_STACK = 0x6474E551
PF_X = 0x1


def carchive_members(blob):
    """Yield (name, data) for every member of the appended CArchive.

    Yields nothing when the file carries no archive (a one-directory build, or
    a plain executable), which the caller reports as an error: a silently empty
    member list would pass every floor.
    """
    cookie_pos = blob.rfind(COOKIE_MAGIC)
    if cookie_pos < 0:
        return
    fields = struct.unpack_from(COOKIE_FORMAT, blob, cookie_pos)
    _, pkg_length, toc_offset, toc_length = fields[:4]
    # Offsets inside the archive are relative to where it begins, which is its
    # own length back from the end of the cookie.
    pkg_start = cookie_pos + COOKIE_LENGTH - pkg_length
    pos = pkg_start + toc_offset
    end = pos + toc_length
    while pos < end:
        entry_length, entry_offset, data_length, _ulen, compressed, _typecode = (
            struct.unpack_from(TOC_ENTRY_FORMAT, blob, pos)
        )
        if entry_length < TOC_ENTRY_LENGTH:
            raise ValueError("corrupt TOC entry at offset {}".format(pos))
        name = blob[pos + TOC_ENTRY_LENGTH:pos + entry_length]
        name = name.split(b"\0", 1)[0].decode("utf-8", "replace")
        start = pkg_start + entry_offset
        data = blob[start:start + data_length]
        if compressed:
            try:
                data = zlib.decompress(data)
            except zlib.error:
                data = b""
        yield name, data
        pos += entry_length


class Elf:
    """The slice of ELF this check needs: program headers and sections."""

    def __init__(self, blob):
        if blob[:4] != b"\x7fELF":
            raise ValueError("not an ELF image")
        self.blob = blob
        self.is64 = blob[4] == 2
        self.end = "<" if blob[5] == 1 else ">"
        if self.is64:
            self.phoff, self.shoff = struct.unpack_from(self.end + "QQ", blob, 0x20)
            self.phentsize, self.phnum, self.shentsize, self.shnum = (
                struct.unpack_from(self.end + "HHHH", blob, 0x36)
            )
        else:
            self.phoff, self.shoff = struct.unpack_from(self.end + "II", blob, 0x1C)
            self.phentsize, self.phnum, self.shentsize, self.shnum = (
                struct.unpack_from(self.end + "HHHH", blob, 0x2A)
            )

    def program_headers(self):
        for i in range(self.phnum):
            off = self.phoff + i * self.phentsize
            if self.is64:
                p_type, p_flags = struct.unpack_from(self.end + "II", self.blob, off)
            else:
                # 32-bit puts p_flags last in the header, not second.
                p_type = struct.unpack_from(self.end + "I", self.blob, off)[0]
                p_flags = struct.unpack_from(self.end + "I", self.blob, off + 24)[0]
            yield p_type, p_flags

    def sections(self):
        for i in range(self.shnum):
            off = self.shoff + i * self.shentsize
            sh_type = struct.unpack_from(self.end + "I", self.blob, off + 4)[0]
            if self.is64:
                sh_offset = struct.unpack_from(self.end + "Q", self.blob, off + 0x18)[0]
                sh_link, sh_info = struct.unpack_from(
                    self.end + "II", self.blob, off + 0x28
                )
            else:
                sh_offset = struct.unpack_from(self.end + "I", self.blob, off + 0x10)[0]
                sh_link, sh_info = struct.unpack_from(
                    self.end + "II", self.blob, off + 0x18
                )
            yield i, sh_type, sh_offset, sh_link, sh_info

    def section_offset(self, index):
        off = self.shoff + index * self.shentsize
        if self.is64:
            return struct.unpack_from(self.end + "Q", self.blob, off + 0x18)[0]
        return struct.unpack_from(self.end + "I", self.blob, off + 0x10)[0]

    def exec_stack(self):
        """True when the image declares an executable stack."""
        for p_type, p_flags in self.program_headers():
            if p_type == PT_GNU_STACK:
                return bool(p_flags & PF_X)
        return False

    def needed_versions(self):
        """Every symbol version this image imports, as plain strings.

        Reads .gnu.version_r, whose entries name a library and carry a list of
        the versions wanted from it.  Absent (a fully static image, or one that
        imports nothing versioned) the result is empty.
        """
        out = []
        for _i, sh_type, sh_offset, sh_link, sh_info in self.sections():
            if sh_type != SHT_GNU_VERNEED:
                continue
            strtab = self.section_offset(sh_link)
            pos = sh_offset
            for _ in range(sh_info):
                _ver, cnt, _file, aux, nxt = struct.unpack_from(
                    self.end + "HHIII", self.blob, pos
                )
                apos = pos + aux
                for _a in range(cnt):
                    _hash, _flags, _other, name, anext = struct.unpack_from(
                        self.end + "IHHII", self.blob, apos
                    )
                    out.append(self._string(strtab + name))
                    if not anext:
                        break
                    apos += anext
                if not nxt:
                    break
                pos += nxt
        return out

    def _string(self, offset):
        end = self.blob.index(b"\0", offset)
        return self.blob[offset:end].decode("utf-8", "replace")


def glibc_version(symbol):
    """(2, 17) for "GLIBC_2.17"; None for anything else.

    GLIBC_PRIVATE and the non-glibc versions (OPENSSL_3.0.0 and friends) have
    no bearing on which hosts the binary starts on, so they drop out here.
    """
    if not symbol.startswith("GLIBC_"):
        return None
    parts = symbol[len("GLIBC_"):].split(".")
    if not all(part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def parse_floor(text):
    parts = text.split(".")
    if not parts or not all(part.isdigit() for part in parts):
        raise argparse.ArgumentTypeError("expected a version like 2.17, got " + text)
    return tuple(int(part) for part in parts)


def show(version):
    return ".".join(str(part) for part in version)


def main(argv=None):
    parser = argparse.ArgumentParser(description="assert a binary's glibc floor")
    parser.add_argument("binary", help="the one-file PyInstaller build")
    parser.add_argument(
        "--max-glibc",
        type=parse_floor,
        required=True,
        help="the floor this lane declares, e.g. 2.17",
    )
    parser.add_argument(
        "--no-exec-stack",
        action="store_true",
        help="also fail when any bundled image declares an executable stack",
    )
    args = parser.parse_args(argv)

    with open(args.binary, "rb") as handle:
        blob = handle.read()

    images = [("<bootloader>", blob)]
    images.extend(
        (name, data)
        for name, data in carchive_members(blob)
        if data[:4] == b"\x7fELF"
    )
    if len(images) == 1:
        print(
            "{}: no CArchive members; not a one-file build?".format(args.binary),
            file=sys.stderr,
        )
        return 1

    worst = (0,)
    contributors = []
    exec_stack = []
    for name, data in images:
        image = Elf(data)
        if args.no_exec_stack and image.exec_stack():
            exec_stack.append(name)
        highest = (0,)
        for symbol in image.needed_versions():
            version = glibc_version(symbol)
            if version and version > highest:
                highest = version
        if highest == (0,):
            continue
        if highest > worst:
            worst, contributors = highest, [name]
        elif highest == worst:
            contributors.append(name)

    print(
        "{}: {} ELF images, max GLIBC_{} from {}".format(
            args.binary,
            len(images),
            show(worst),
            ", ".join(sorted(contributors)[:8]) or "nothing",
        )
    )

    failed = False
    if worst > args.max_glibc:
        print(
            "::error::{}: requires GLIBC_{}, above the declared floor GLIBC_{}; "
            "raised by {}".format(
                args.binary,
                show(worst),
                show(args.max_glibc),
                ", ".join(sorted(contributors)),
            ),
            file=sys.stderr,
        )
        failed = True
    if exec_stack:
        print(
            "::error::{}: executable stack declared by {}; this fails at runtime "
            "on SELinux-hardened hosts".format(
                args.binary, ", ".join(sorted(exec_stack))
            ),
            file=sys.stderr,
        )
        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
