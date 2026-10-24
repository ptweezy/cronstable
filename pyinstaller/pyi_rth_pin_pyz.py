# PyInstaller runtime hook. A package upgrade renames a new binary over the
# running one, and the stock PYZ reader reopens that path to load each
# module. This reader keeps a descriptor to the original file.
#
# extract() is ZlibArchiveReader.extract from PyInstaller 6.22.3, reading
# with os.pread. Without os.pread (Windows), with an unexpected loader, or
# with no PYZ header at the archive offset, the stock reader stays.


def _pyi_rthook():
    import marshal
    import os
    import sys
    import zlib

    if not hasattr(os, "pread"):
        return

    def pin(path, start):
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        held = [fd]
        try:
            # Above stdio, which a child's dup2 replaces before preexec_fn.
            while fd < 3:
                fd = os.dup(fd)
                held.append(fd)
            if os.pread(fd, 4, start) == b"PYZ\0":
                return held.pop()
        finally:
            for low in held:
                os.close(low)
        return None

    try:
        names = sys.modules["pyimod01_archive"]
        archive = sys.modules["pyimod02_importers"].pyz_archive
        nspkg = names.PYZ_ITEM_NSPKG
        code = (names.PYZ_ITEM_MODULE, names.PYZ_ITEM_PKG)
        lookup = archive.toc.get
        start = archive._start_offset
        fd = pin(archive._filename, start)
    except Exception:  # noqa: BLE001 - the stock reader stays
        return
    if fd is None:
        return

    def read(length, offset):
        # As fp.read(length): short only at end of file.
        data = b""
        while len(data) < length:
            more = os.pread(fd, length - len(data), offset + len(data))
            if not more:
                break
            data += more
        return data

    def extract(name, raw=False):
        entry = lookup(name)
        if entry is None:
            raise KeyError(f"No entry named {name!r} found in the archive!")
        typecode, entry_offset, entry_length = entry
        if typecode == nspkg:
            return None
        obj = read(entry_length, start + entry_offset)
        try:
            obj = zlib.decompress(obj)
            if typecode in code and not raw:
                obj = marshal.loads(obj)
        except EOFError as exc:
            raise ImportError(
                f"Failed to unmarshal PYZ entry {name!r}!"
            ) from exc
        return obj

    archive.extract = extract


_pyi_rthook()
del _pyi_rthook
