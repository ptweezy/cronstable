"""Assert the ABI floor of a frozen PyInstaller binary, from the bytes.

The floor a binary actually carries is an emergent property of the build
environment: it is the highest ``GLIBC_x.y`` symbol version any member of the
bundle asks for, contributed by the interpreter, OpenSSL, libstdc++ and every
wheel that was installed.  Nothing in the build declares it, and a dependency
that starts publishing a higher-tagged wheel raises it with CI green
throughout, because the smoke test runs on a runner whose glibc is newer than
anything the binary needs.

    python .github/scripts/elf_floor.py dist/cronstable --max-glibc 2.17
    python .github/scripts/elf_floor.py dist/cronstable --no-exec-stack \
        --max-glibc 2.31

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

    python .github/scripts/elf_floor.py dist/cronstable --max-glibc 2.36 \
        --arm-float hard --max-arm-arch v6

On 32-bit ARM the glibc version is only half the story, and the other half is
invisible to every functional test:

``--arm-float`` compares each member's ``EF_ARM_ABI_FLOAT_HARD`` /
``_SOFT`` flag against the ABI the lane claims.  A hard-float binary shipped
under a soft-float name (or the reverse) names an interpreter the host does not
have, and the failure reads like a corrupt download.

``--max-arm-arch`` reads ``Tag_CPU_arch`` out of each member's
``.ARM.attributes`` and fails when anything needs a newer instruction set than
the lane targets.  This is the ARMv6 trap: under QEMU an ARMv6 container
reports ``armv7l`` from ``uname``, so pip installs ``musllinux`` and
``manylinux2014`` ``armv7l`` wheels whose object code uses ARMv7 instructions.
Every smoke test passes under emulation, and the binary then dies with SIGILL
on the Raspberry Pi 1 it was built for.  Nothing else in the pipeline can see
that.

``--arm-arch-except`` exempts a named member from that ceiling.  It covers one
specific false positive: a library that selects an implementation through an
indirect function (IFUNC) resolver.

The linker MAX-merges ``Tag_CPU_arch`` over every input object.  The tag
therefore records the newest core that any code in the file was built for, not
the core that the file requires at run time.  For most libraries, these two
values match.  For an IFUNC library, they differ by design, because the library
contains several implementations of each function and selects one at load time
based on the processor that is present.

Debian armel ``libatomic1`` is such a library.  It declares v7, but each of its
resolvers contains only ARMv5TE instructions, and its ``ldrex`` and ``dmb``
code belongs to the variants that a v5 core never selects.  On a v5 core, the
library uses the ``__kuser_cmpxchg`` kernel helper instead.  ``libcrypto``
links against ``libatomic1``, which is how the library enters the armel bundle.

The exemption applies only to a member that contains an IFUNC resolver.  If you
name a member that contains none, the check reports an error.  The flag
therefore cannot suppress a failure for a library that is built for the wrong
core.

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
SHT_ARM_ATTRIBUTES = 0x70000003
SHT_DYNSYM = 11
# STT_GNU_IFUNC marks a symbol that resolves through a function that the
# loader runs at load time.  That function selects an implementation for the
# processor that is present.
STT_GNU_IFUNC = 10
PT_GNU_STACK = 0x6474E551
PF_X = 0x1

EM_ARM = 40
EF_ARM_ABI_FLOAT_SOFT = 0x200
EF_ARM_ABI_FLOAT_HARD = 0x400

# Tag_CPU_arch, the .ARM.attributes tag that says which instruction set an
# object needs (ARM ABI addenda, section 2.3.3).  The numbering is not a
# ladder: 10 is ARMv7 while 11 and 12 are the ARMv6 microcontroller profiles
# that come after it, so a "<= n" comparison is the wrong shape and this is a
# named set per target instead.
ARM_CPU_ARCH_TAG = 6
# Tag_FP_arch, the floating-point unit an object needs.  A separate failure
# mode from the integer ISA and a monotonic ladder, unlike Tag_CPU_arch: an
# object can declare Tag_CPU_arch v6 and still contain VFPv3 code, which faults
# on the VFPv2 unit in an ARM1176.  Measured on the shipped 1.2.41 binaries: the
# healthy ARMv6 members are (CPU_arch v6KZ, FP_arch VFPv2) and every
# contaminated one is (v7, VFPv3-D16).
ARM_FP_ARCH_TAG = 10
ARM_FP_ARCH_NAMES = {
    0: "none", 1: "VFPv1", 2: "VFPv2", 3: "VFPv3", 4: "VFPv3-D16",
    5: "VFPv4", 6: "VFPv4-D16", 7: "FP-ARMv8-D16", 8: "FP-ARMv8",
}
# Tags whose value is a NUL-terminated string rather than a ULEB128, which the
# attribute walk has to know in order to stay in sync.  Tag_compatibility (32)
# is a ULEB128 followed by a string, so it needs both.
ARM_STRING_TAGS = frozenset({4, 5, 65, 67})
ARM_ULEB_THEN_STRING_TAGS = frozenset({32})
ARM_CPU_ARCH_NAMES = {
    0: "pre-v4", 1: "v4", 2: "v4T", 3: "v5T", 4: "v5TE", 5: "v5TEJ",
    6: "v6", 7: "v6KZ", 8: "v6T2", 9: "v6K", 10: "v7", 11: "v6-M",
    12: "v6S-M", 13: "v7E-M", 14: "v8-A", 15: "v8-R", 16: "v8-M.baseline",
    17: "v8-M.mainline", 18: "v8.1-A", 19: "v8.2-A", 20: "v8.3-A",
    21: "v8.1-M.mainline", 22: "v9-A",
}
# What each lane accepts.  v6 covers the Raspberry Pi 1/Zero (ARM1176JZF-S,
# an ARMv6KZ part), so v6/v6KZ/v6K pass and v6T2 does not: Thumb-2 is absent
# from ARM1176.  v5 is the armel target (soft-float Kirkwood hardware).
ARM_ARCH_ALLOWED = {
    "v5": {0, 1, 2, 3, 4, 5},
    "v6": {0, 1, 2, 3, 4, 5, 6, 7, 9},
    "v7": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
}


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

    def machine(self):
        return struct.unpack_from(self.end + "H", self.blob, 0x12)[0]

    def flags(self):
        """e_flags, which on ARM carries the float ABI of the whole object."""
        return struct.unpack_from(self.end + "I", self.blob, 0x30 if self.is64 else 0x24)[0]

    def float_abi(self):
        """'hard', 'soft', or None when the object declares neither.

        Only meaningful on EM_ARM.  Objects with no floating point at all
        (a handful of small extension modules) set neither bit, and those are
        loadable either way, so they report None rather than a mismatch.
        """
        if self.machine() != EM_ARM:
            return None
        flags = self.flags()
        if flags & EF_ARM_ABI_FLOAT_HARD:
            return "hard"
        if flags & EF_ARM_ABI_FLOAT_SOFT:
            return "soft"
        return None

    def arm_attributes(self):
        """The aeabi build attributes as {tag: value}, or {} when absent.

        Layout (ARM ABI addenda 2.2): a format byte 'A', then subsections of
        uint32 length plus a NUL-terminated vendor name; inside the "aeabi"
        vendor, sub-subsections of a ULEB128 tag plus a uint32 length, and
        inside Tag_File (1), a stream of ULEB128 tag/value pairs.  Most values
        are ULEB128 and a few are strings, so every tag has to be decoded in
        order: skipping one with the wrong width desynchronizes the rest of the
        stream and yields plausible nonsense.

        The linker MAX-merges these over all input objects, which is what makes
        them usable as a gate: one vendored blob compiled for a newer core
        raises the whole library's declaration.
        """
        found = {}
        for index, sh_type, sh_offset, _link, _info in self.sections():
            if sh_type != SHT_ARM_ATTRIBUTES:
                continue
            size = self._section_size(index)
            blob = self.blob[sh_offset:sh_offset + size]
            if blob[:1] != b"A":
                continue
            pos = 1
            while pos + 4 <= len(blob):
                (length,) = struct.unpack_from(self.end + "I", blob, pos)
                if length < 4 or pos + length > len(blob):
                    break
                vendor_end = blob.index(b"\0", pos + 4)
                if blob[pos + 4:vendor_end] == b"aeabi":
                    found.update(self._aeabi_tags(blob[vendor_end + 1:pos + length]))
                pos += length
        return found

    def _aeabi_tags(self, blob):
        out = {}
        pos = 0
        while pos + 4 < len(blob):
            tag, pos = _uleb(blob, pos)
            (length,) = struct.unpack_from(self.end + "I", blob, pos)
            body_end = pos - _uleb_len(tag) + length
            pos += 4
            if tag != 1:
                # Tag_Section and Tag_Symbol carry per-section overrides that
                # nothing here needs.
                pos = body_end
                continue
            while pos < min(body_end, len(blob)):
                attr, pos = _uleb(blob, pos)
                if attr in ARM_ULEB_THEN_STRING_TAGS:
                    _flag, pos = _uleb(blob, pos)
                    pos = blob.index(b"\0", pos) + 1
                elif attr in ARM_STRING_TAGS:
                    pos = blob.index(b"\0", pos) + 1
                else:
                    out[attr], pos = _uleb(blob, pos)
            pos = body_end
        return out

    def _section_size(self, index):
        off = self.shoff + index * self.shentsize
        if self.is64:
            return struct.unpack_from(self.end + "Q", self.blob, off + 0x20)[0]
        return struct.unpack_from(self.end + "I", self.blob, off + 0x14)[0]

    def has_ifunc(self):
        """Reports whether any dynamic symbol uses an IFUNC resolver.

        An IFUNC symbol does not name an implementation.  It names a resolver
        function.  The loader calls that resolver, and the resolver returns the
        address of the implementation to use on the processor that is present.
        A library of this kind contains code for cores that it never executes.
        This design is the one case in which a build attribute legitimately
        differs from what the library requires at run time.

        This method reports only that IFUNC dispatch is present.  It does not
        verify that every instruction for a newer core belongs to a dispatched
        variant.  The result therefore qualifies an exemption that you name
        explicitly, rather than granting one on its own.
        """
        entry = 24 if self.is64 else 16
        # st_info, whose low nibble is the symbol type, moves between the
        # 32- and 64-bit layouts.
        info_at = 4 if self.is64 else 12
        for index, sh_type, sh_offset, _sh_link, _sh_info in self.sections():
            if sh_type != SHT_DYNSYM:
                continue
            # Bound the walk to the bytes that are present, so that a section
            # header that points past the end of the image yields no symbols
            # instead of raising an error.  This bound is not general
            # protection against truncation: an image short enough to truncate
            # the section header table already raises an error in sections(),
            # as it does for every other check in this file.
            end = min(sh_offset + self._section_size(index), len(self.blob))
            for pos in range(sh_offset, end - entry + 1, entry):
                if self.blob[pos + info_at] & 0xF == STT_GNU_IFUNC:
                    return True
        return False

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


def _uleb(blob, pos):
    """(value, new_pos) for the ULEB128 at pos."""
    value = 0
    shift = 0
    while pos < len(blob):
        byte = blob[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return value, pos


def _uleb_len(value):
    """How many bytes the ULEB128 encoding of value occupies."""
    count = 1
    while value >= 0x80:
        value >>= 7
        count += 1
    return count


def _names(items, limit=12):
    """A sorted, capped join, so one bad wheel does not print 90 file names."""
    items = sorted(items)
    if len(items) <= limit:
        return ", ".join(items)
    return "{} and {} more".format(", ".join(items[:limit]), len(items) - limit)


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
        help="the floor this lane declares, e.g. 2.17. Optional: the musl lanes "
             "carry no GLIBC_ symbol versions at all, and pass the ARM checks "
             "alone",
    )
    parser.add_argument(
        "--no-exec-stack",
        action="store_true",
        help="also fail when any bundled image declares an executable stack",
    )
    parser.add_argument(
        "--arm-float",
        choices=("hard", "soft"),
        help="the ARM float ABI this lane claims; fails on any member that "
             "declares the other one",
    )
    parser.add_argument(
        "--max-arm-arch",
        choices=sorted(ARM_ARCH_ALLOWED),
        help="the newest ARM instruction set this lane targets; fails on any "
             "member whose Tag_CPU_arch is outside it",
    )
    parser.add_argument(
        "--arm-arch-except",
        action="append",
        default=[],
        metavar="MEMBER",
        help="exempt this bundled member from --max-arm-arch, named as it "
             "appears in the archive; repeatable. Applies only to a member "
             "that dispatches through an IFUNC resolver, and reports an error "
             "for a member that does not",
    )
    parser.add_argument(
        "--max-arm-fp",
        type=int,
        help="the newest floating-point unit this lane targets, as a "
             "Tag_FP_arch value (0 none, 2 VFPv2, 4 VFPv3-D16); fails on any "
             "member that needs a newer one",
    )
    args = parser.parse_args(argv)
    if not (
        args.max_glibc
        or args.arm_float
        or args.max_arm_arch
        or args.max_arm_fp is not None
    ):
        parser.error(
            "nothing to check: pass --max-glibc, --arm-float, --max-arm-arch "
            "or --max-arm-fp"
        )

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
    wrong_float = []
    too_new = []
    exempted = []
    exempt_denied = []
    too_much_fp = []
    arm_seen = {}
    fp_seen = {}
    for name, data in images:
        image = Elf(data)
        if args.no_exec_stack and image.exec_stack():
            exec_stack.append(name)
        if args.arm_float:
            abi = image.float_abi()
            if abi is not None and abi != args.arm_float:
                wrong_float.append("{} ({})".format(name, abi))
        if args.max_arm_arch or args.max_arm_fp is not None:
            attrs = image.arm_attributes()
            cpu = attrs.get(ARM_CPU_ARCH_TAG)
            if args.max_arm_arch and cpu is not None:
                arm_seen[name] = cpu
                if cpu not in ARM_ARCH_ALLOWED[args.max_arm_arch]:
                    shown = "{} ({})".format(
                        name, ARM_CPU_ARCH_NAMES.get(cpu, cpu)
                    )
                    if name not in args.arm_arch_except:
                        too_new.append(shown)
                    elif image.has_ifunc():
                        exempted.append(shown)
                    else:
                        # The member is named, but it dispatches nothing, so
                        # the attribute describes a real requirement and the
                        # exemption does not apply.
                        exempt_denied.append(shown)
            fp = attrs.get(ARM_FP_ARCH_TAG)
            if args.max_arm_fp is not None and fp is not None:
                fp_seen[name] = fp
                if fp > args.max_arm_fp:
                    too_much_fp.append(
                        "{} ({})".format(name, ARM_FP_ARCH_NAMES.get(fp, fp))
                    )
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

    if worst == (0,):
        # A musl bundle imports no versioned glibc symbols at all, so there is
        # no floor to report and "max GLIBC_0 from nothing" is only noise.
        print("{}: {} ELF images, no glibc symbol versions".format(
            args.binary, len(images)))
    else:
        print(
            "{}: {} ELF images, max GLIBC_{} from {}".format(
                args.binary, len(images), show(worst), _names(contributors, 8)
            )
        )

    if args.max_arm_arch:
        highest = max(arm_seen.values()) if arm_seen else None
        print(
            "{}: {} of {} images declare Tag_CPU_arch, highest {}".format(
                args.binary,
                len(arm_seen),
                len(images),
                ARM_CPU_ARCH_NAMES.get(highest, highest),
            )
        )
    if exempted:
        # Print this line on every run, not only on failure.  An exemption
        # that the log never mentions is one that nobody reviews when the
        # member is next rebuilt.
        print(
            "{}: these members exceed the {} ceiling but dispatch through "
            "IFUNC, so --arm-arch-except exempts them: {}".format(
                args.binary, args.max_arm_arch, _names(exempted)
            )
        )
    if args.max_arm_fp is not None:
        highest_fp = max(fp_seen.values()) if fp_seen else None
        print(
            "{}: {} of {} images declare Tag_FP_arch, highest {}".format(
                args.binary,
                len(fp_seen),
                len(images),
                ARM_FP_ARCH_NAMES.get(highest_fp, highest_fp),
            )
        )

    failed = False
    if args.max_glibc and worst > args.max_glibc:
        print(
            "::error::{}: requires GLIBC_{}, above the declared floor GLIBC_{}; "
            "raised by {}".format(
                args.binary,
                show(worst),
                show(args.max_glibc),
                _names(contributors),
            ),
            file=sys.stderr,
        )
        failed = True
    if exec_stack:
        print(
            "::error::{}: executable stack declared by {}; this fails at runtime "
            "on SELinux-hardened hosts".format(
                args.binary, _names(exec_stack)
            ),
            file=sys.stderr,
        )
        failed = True
    if wrong_float:
        print(
            "::error::{}: this lane ships a {}-float binary, but these members "
            "declare the other ABI: {}".format(
                args.binary, args.arm_float, _names(wrong_float)
            ),
            file=sys.stderr,
        )
        failed = True
    if too_new:
        print(
            "::error::{}: these members need a newer instruction set than {}, so "
            "the binary would SIGILL on the hardware this lane targets: {}".format(
                args.binary, args.max_arm_arch, _names(too_new)
            ),
            file=sys.stderr,
        )
        failed = True
    if exempt_denied:
        print(
            "::error::{}: --arm-arch-except names these members, but they "
            "contain no IFUNC resolver. Nothing selects an implementation at "
            "run time, so the {} ceiling applies to them: {}".format(
                args.binary, args.max_arm_arch, _names(exempt_denied)
            ),
            file=sys.stderr,
        )
        failed = True
    if too_much_fp:
        print(
            "::error::{}: these members need a newer FPU than {}, which faults on "
            "the hardware this lane targets: {}".format(
                args.binary,
                ARM_FP_ARCH_NAMES.get(args.max_arm_fp, args.max_arm_fp),
                _names(too_much_fp),
            ),
            file=sys.stderr,
        )
        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
