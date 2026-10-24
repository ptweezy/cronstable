"""The PyInstaller runtime hook that pins the PYZ archive.

A release binary reads its pure-Python modules from the PYZ archive inside
the executable, and PyInstaller's reader reopens the executable by path to
load each module. A package upgrade renames a new binary over that path
while the old daemon finishes its jobs. These tests run the hook against
stand-ins for PyInstaller 6.22.3's ``pyimod01_archive`` and
``pyimod02_importers``, so they need no PyInstaller.
"""

import ast
import importlib.util
import marshal
import os
import re
import runpy
import struct
import sys
import types
import zlib

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYINSTALLER = os.path.join(ROOT, "pyinstaller")
HOOK = os.path.join(PYINSTALLER, "pyi_rth_pin_pyz.py")

needs_pread = pytest.mark.skipif(
    not hasattr(os, "pread"), reason="os.pread is POSIX-only"
)

# The PYZ entry typecodes pyimod01_archive defines.
MODULE, PKG, DATA, NSPKG = 0, 1, 2, 3
NAMES = ("mod", "pkg", "data", "broken", "ns", "missing")


class StockReader:
    """ZlibArchiveReader as PyInstaller 6.22.3 behaves."""

    _PYZ_MAGIC_PATTERN = b"PYZ\0"

    def __init__(self, filename, start_offset):
        self._filename = filename
        self._start_offset = start_offset
        with open(filename, "rb") as fobj:
            fobj.seek(start_offset)
            assert fobj.read(4) == self._PYZ_MAGIC_PATTERN
            fobj.read(4)  # the bytecode magic number
            (toc_offset,) = struct.unpack("!i", fobj.read(4))
            fobj.seek(start_offset + toc_offset)
            self.toc = dict(marshal.load(fobj))

    def extract(self, name, raw=False):
        entry = self.toc.get(name)
        if entry is None:
            raise KeyError(f"No entry named {name!r} found in the archive!")
        typecode, offset, length = entry
        if typecode == NSPKG:
            return None
        try:
            with open(self._filename, "rb") as fobj:
                fobj.seek(self._start_offset + offset)
                obj = fobj.read(length)
        except FileNotFoundError:
            raise SystemExit(f"ERROR: {self._filename} moved") from None
        try:
            obj = zlib.decompress(obj)
            if typecode in (MODULE, PKG) and not raw:
                obj = marshal.loads(obj)
        except EOFError as exc:
            raise ImportError(
                f"Failed to unmarshal PYZ entry {name!r}!"
            ) from exc
        return obj


def _code(value):
    return compile(f"VALUE = {value}", "mod", "exec")


def _write_archive(path, prefix, value):
    """Write ``prefix``, then a PYZ archive; return the archive offset."""
    payloads = {
        "mod": (MODULE, marshal.dumps(_code(value))),
        "pkg": (PKG, marshal.dumps(compile("", "pkg", "exec"))),
        "data": (DATA, b"data %d" % value),
        # unmarshals to EOFError, which the reader turns into ImportError
        "broken": (MODULE, b""),
    }
    header = 12
    blob = b""
    toc = [("ns", (NSPKG, 0, 0))]
    for name, (typecode, payload) in payloads.items():
        packed = zlib.compress(payload)
        toc.append((name, (typecode, header + len(blob), len(packed))))
        blob += packed
    with open(path, "wb") as fobj:
        fobj.write(prefix + b"PYZ\0" + importlib.util.MAGIC_NUMBER)
        fobj.write(struct.pack("!i", header + len(blob)) + blob)
        fobj.write(marshal.dumps(toc))
    return len(prefix)


def _upgrade(path):
    """Rename a different binary over ``path``, as a package manager does."""
    staged = path + ".pkgnew"
    _write_archive(staged, b"\xff" * 4096, 2)
    os.replace(staged, path)


def _outcome(extract, name, raw):
    try:
        return "returned", extract(name, raw=raw)
    except Exception as exc:  # noqa: BLE001 - compared, not handled
        cause = type(exc.__cause__)
        return "raised", type(exc), str(exc), cause


def _outcomes(extract):
    return {
        (name, raw): _outcome(extract, name, raw)
        for name in NAMES
        for raw in (False, True)
    }


def _is_open(fd):
    try:
        os.fstat(fd)
    except OSError:
        return False
    return True


def _stock(archive):
    return "extract" not in vars(archive)


@pytest.fixture
def loader(tmp_path, monkeypatch):
    """The stand-in loader modules, holding a reader over an executable."""
    exe = str(tmp_path / "cronstable")
    start = _write_archive(exe, b"\x7fELF" + b"\0" * 60, 1)
    archive_mod = types.ModuleType("pyimod01_archive")
    archive_mod.PYZ_ITEM_MODULE = MODULE
    archive_mod.PYZ_ITEM_PKG = PKG
    archive_mod.PYZ_ITEM_DATA = DATA
    archive_mod.PYZ_ITEM_NSPKG = NSPKG
    archive_mod.ZlibArchiveReader = StockReader
    importers = types.ModuleType("pyimod02_importers")
    importers.pyz_archive = StockReader(exe, start)
    monkeypatch.setitem(sys.modules, "pyimod01_archive", archive_mod)
    monkeypatch.setitem(sys.modules, "pyimod02_importers", importers)
    return archive_mod, importers


@pytest.fixture
def archive(loader):
    return loader[1].pyz_archive


@pytest.fixture
def run_hook(monkeypatch):
    """Run the hook; return the descriptors it left open."""
    kept = []

    def run():
        opened = []

        def recording(func):
            def wrapper(*args):
                fd = func(*args)
                opened.append(fd)
                return fd

            return wrapper

        with monkeypatch.context() as patch:
            for name in ("open", "dup"):
                patch.setattr(os, name, recording(getattr(os, name)))
            runpy.run_path(HOOK)
        left = [fd for fd in opened if _is_open(fd)]
        kept.extend(left)
        return left

    yield run
    for fd in kept:
        os.close(fd)


def test_the_stock_reader_reads_the_new_binary_after_an_upgrade(archive):
    # Without the hook, the reader reopens the path and reads the new file
    # at the old offsets.
    _upgrade(archive._filename)
    with pytest.raises(zlib.error, match="incorrect header check"):
        archive.extract("mod")


@needs_pread
def test_the_hooked_reader_returns_what_the_stock_one_does(archive, run_hook):
    stock = _outcomes(archive.extract)
    assert len(run_hook()) == 1
    assert not _stock(archive)
    assert _outcomes(archive.extract) == stock
    assert stock[("mod", False)] == ("returned", _code(1))
    assert stock[("mod", True)] == ("returned", marshal.dumps(_code(1)))
    assert stock[("data", False)] == ("returned", b"data 1")
    assert stock[("ns", False)] == ("returned", None)
    assert stock[("broken", False)][1:] == (
        ImportError,
        "Failed to unmarshal PYZ entry 'broken'!",
        EOFError,
    )
    assert stock[("missing", True)][1] is KeyError


@needs_pread
def test_the_hooked_reader_keeps_reading_the_running_binary(archive, run_hook):
    stock = _outcomes(archive.extract)
    run_hook()
    _upgrade(archive._filename)
    assert _outcomes(archive.extract) == stock
    module = types.ModuleType("mod")
    exec(archive.extract("mod"), module.__dict__)
    assert module.VALUE == 1


@needs_pread
def test_the_hooked_reader_resumes_a_short_read(
    archive, run_hook, monkeypatch
):
    run_hook()
    pread = os.pread
    monkeypatch.setattr(
        os, "pread", lambda fd, size, at: pread(fd, min(size, 3), at)
    )
    assert archive.extract("data") == b"data 1"
    assert archive.extract("mod") == _code(1)


@needs_pread
def test_the_pinned_descriptor_sits_above_stdio(archive, run_hook):
    # A daemon started with stdin closed would get fd 0, which a child's
    # dup2 replaces before its preexec_fn runs.
    saved = os.dup(0)
    os.close(0)
    try:
        kept = run_hook()
    finally:
        os.dup2(saved, 0)
        os.close(saved)
    assert len(kept) == 1 and kept[0] > 2
    assert archive.extract("data") == b"data 1"


def test_without_pread_the_stock_reader_stays(archive, run_hook, monkeypatch):
    # Windows has no os.pread.
    monkeypatch.delattr(os, "pread", raising=False)
    assert run_hook() == []
    assert _stock(archive)
    assert archive.extract("data") == b"data 1"


def _drop(loader, name, monkeypatch):
    archive_mod, importers = loader
    if name in ("pyimod01_archive", "pyimod02_importers"):
        monkeypatch.delitem(sys.modules, name)
    elif name == "pyz_archive":
        importers.pyz_archive = None
    elif name.startswith("PYZ_ITEM_"):
        delattr(archive_mod, name)
    else:
        delattr(importers.pyz_archive, name)


@needs_pread
@pytest.mark.parametrize(
    "name",
    [
        "pyimod01_archive",
        "pyimod02_importers",
        "pyz_archive",
        "PYZ_ITEM_MODULE",
        "PYZ_ITEM_PKG",
        "PYZ_ITEM_NSPKG",
        "_filename",
        "_start_offset",
        "toc",
    ],
)
def test_a_missing_loader_name_keeps_the_stock_reader(
    loader, archive, run_hook, monkeypatch, name
):
    _drop(loader, name, monkeypatch)
    assert run_hook() == []
    assert _stock(archive)


@needs_pread
def test_an_unopenable_archive_keeps_the_stock_reader(
    archive, run_hook, tmp_path
):
    archive._filename = str(tmp_path / "gone")
    assert run_hook() == []
    assert _stock(archive)


@needs_pread
def test_a_misplaced_archive_keeps_the_stock_reader(archive, run_hook):
    # The bytes at the offset are not a PYZ header.
    archive._start_offset -= 1
    assert run_hook() == []
    assert _stock(archive)


def _spec_runtime_hooks():
    path = os.path.join(PYINSTALLER, "cronstable.spec")
    with open(path, encoding="utf-8") as fobj:
        tree = ast.parse(fobj.read())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Analysis"
        ):
            for keyword in node.keywords:
                if keyword.arg == "runtime_hooks":
                    code = compile(ast.Expression(keyword.value), path, "eval")
                    scope = {"__builtins__": {}, "os": os}
                    scope["SPECPATH"] = PYINSTALLER
                    return eval(code, scope)
    raise AssertionError("cronstable.spec calls no Analysis(runtime_hooks=)")


def test_the_spec_bundles_the_hook():
    hooks = [os.path.normpath(path) for path in _spec_runtime_hooks()]
    assert HOOK in hooks
    assert all(os.path.isfile(path) for path in hooks), hooks


def test_the_hook_names_the_pinned_pyinstaller():
    # The hook falls back to the stock reader when a loader name it reads
    # is gone, so a PyInstaller bump fails here until the hook and these
    # stand-ins name the new release.
    path = os.path.join(PYINSTALLER, "build-requirements.txt")
    with open(path, encoding="utf-8") as fobj:
        pins = re.findall(r"^pyinstaller==([^\s;\\]+)", fobj.read(), re.M)
    assert len(pins) == 1, pins
    for source in (HOOK, __file__):
        with open(source, encoding="utf-8") as fobj:
            named = re.findall(r"PyInstaller (\d+(?:\.\d+)+)", fobj.read())
        assert named and set(named) == set(pins), (source, named)
