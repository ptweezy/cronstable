# -*- mode: python ; coding: utf-8 -*-

import os
import re
import sys

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# CRONSTABLE_BUNDLE selects the output layout: "onefile" (the default),
# "onedir", or "both". The default must stay one-file because six knob-less
# consumers read the single dist/cronstable(.exe): four release.yml lanes
# plus pyinstaller/Dockerfile and pyinstaller/Makefile. The one-dir layout
# exists because a one-file build cannot host a Windows service: its
# bootloader unpacks itself and runs the program in a child process the
# Service Control Manager never sees (see cronstable/winservice.py). "both" emits the two
# layouts from one Analysis (the Windows release lane; a second invocation
# would repeat the dependency scan). `or "onefile"` rather than a get()
# default, so an empty-string export cannot flip the layout.
BUNDLE = os.environ.get("CRONSTABLE_BUNDLE") or "onefile"
if BUNDLE not in ("onefile", "onedir", "both"):
    raise SystemExit(
        "CRONSTABLE_BUNDLE must be 'onefile', 'onedir' or 'both', "
        "got {!r}".format(BUNDLE)
    )

# strip is a Unix concept (ELF/Mach-O). On Windows the GNU `strip` that ships
# with git bash WILL corrupt the bundled PE DLLs (notably pythonXY.dll) if
# PyInstaller is allowed to run it -- the resulting .exe then fails to load the
# Python DLL ("Invalid access to memory location"). So strip only off Windows.
STRIP = sys.platform != "win32"

# bundle the single-page web UI (cronstable/web/index.html) so the binary
# serves it without needing any files on disk, plus the third-party license
# notices (cronstable/licenses/*.txt) that `--third-party-licenses` prints:
# the LGPL notice for bundled zeroconf must travel inside the binary itself.
datas = collect_data_files("cronstable")

# uvloop and orjson are optional runtime accelerators, each imported behind a
# try/except with a stdlib fallback: uvloop by cronstable/__main__._new_event_loop
# (else asyncio) and orjson by cronstable/_json (else stdlib json). A frozen binary
# only contains what is importable in the build environment, so bundle each (as
# a hidden import, since the guarded import is easy for the analysis to miss)
# exactly when the build env actually has it -- the binary CI jobs best-effort
# install a wheel, or source-build it, before freezing (see install_orjson.sh /
# the uvloop steps). Absent (an arch with no wheel and no working source build)
# the list stays empty and the binary simply runs on the stdlib equivalents.
hiddenimports = []
try:
    import uvloop  # noqa: F401

    hiddenimports.append("uvloop")
except ImportError:
    pass
try:
    import orjson  # noqa: F401

    hiddenimports.append("orjson")
except ImportError:
    pass
# pynacl (the `push` extra): cronstable/push guards `from nacl.public
# import ...` in a try/except, the pattern the analysis is most likely
# to drop. Name the exact module we import, AND cffi's `_cffi_backend`
# extension: nacl's compiled `_sodium` module imports it from generated
# code the static analysis cannot see, so without the explicit entry the
# frozen import dies with "No module named '_cffi_backend'" while the
# libsodium .so sits uselessly in the bundle. The binary lanes verify a
# real sealed-box round-trip before freezing and a push-enabled
# --validate-config after, so a miss here fails CI.
try:
    import nacl.public  # noqa: F401

    hiddenimports.extend(["nacl.public", "_cffi_backend"])
except ImportError:
    pass
# cryptography (the `push-pq` extra): cronstable/push imports the hpke
# module and the mlkem and x25519 asymmetric modules inside _xwing_suite,
# guarded call-site imports like nacl's above. All three are named because
# nothing else names them: cryptography ships no PyInstaller hooks of its
# own (no __pyinstaller package, no entry point), and the hook that
# PyInstaller's contrib set carries covers the backends and bindings
# rather than these. The analysis does follow the import statement itself
# today, so this is the insurance the nacl entry is: it holds the bundle
# together if that import is ever made dynamic.
try:
    import cryptography.hazmat.primitives.hpke  # noqa: F401

    hiddenimports.extend(
        [
            "cryptography.hazmat.primitives.hpke",
            "cryptography.hazmat.primitives.asymmetric.mlkem",
            "cryptography.hazmat.primitives.asymmetric.x25519",
        ]
    )
except ImportError:
    pass
# zeroconf (the `discovery` extra, behind web.bonjour): same guarded-import
# pattern as pynacl above. It is LGPL-2.1; bundling is deliberate and paired
# with the compliance kit (the in-binary notice behind --third-party-licenses,
# the source archive attached to every GitHub Release, and the public build
# recipe as the relink path). See LICENSING.md before changing anything here.
try:
    import zeroconf  # noqa: F401
    import zeroconf.asyncio  # noqa: F401

    hiddenimports.extend(["zeroconf", "zeroconf.asyncio"])
except ImportError:
    pass


# Modules that are never reachable at runtime but that the analysis (or a
# dependency's optional `try: import ...` probe) could otherwise rake into the
# bundle. cronstable is a headless daemon with an ANSI TUI (raw termios/tty on
# POSIX, msvcrt on Windows) and an HTML/JSON web UI, so no GUI toolkit is ever
# imported; the TUI reads raw keypresses itself and never uses readline; durable
# state is JSON (orjson / stdlib json), never sqlite. Excluding a module that was
# never going to be collected is a harmless no-op, so this list is insurance
# against dead weight sneaking in. Verified against the tree and the runtime deps
# (aiohttp / jinja2 / strictyaml / sentry-sdk / aiosmtplib / psutil / tzdata);
# the per-arch `--version` smoke test is the build-time backstop.
excludes = [
    # GUI toolkits: never imported by a headless daemon.
    "tkinter",
    "_tkinter",
    "turtle",
    "turtledemo",
    "idlelib",
    # curses: the TUI drives the terminal through termios/tty directly.
    "curses",
    "_curses",
    "_curses_panel",
    # the TUI reads raw keypresses; nothing uses readline's line editor.
    "readline",
    # no sqlite anywhere in cronstable or its runtime deps.
    "sqlite3",
    "_sqlite3",
    # dev/tooling stdlib that never runs inside the frozen daemon.
    "test",
    "lib2to3",
    "ensurepip",
    "pydoc_data",
]


# optimize=2 compiles every bundled module at -OO: it strips assert statements
# AND docstrings from the frozen bytecode. cronstable's modules are deliberately
# docstring-dense (the rationale lives next to the code), and those strings
# otherwise ship in the archive and sit in resident memory for the life of the
# daemon; dropping them shrinks the binary and lowers RSS. Every assert in the
# tree is a type-narrowing / internal-invariant check (`x is not None`,
# `isinstance`, `not in`) with no side effects and no untrusted-input
# validation, so removing them does not change behavior on the correct path.
# The source-run test suite does not exercise the frozen -OO build; CI's
# per-arch `--version` smoke test is the backstop for a dependency that might
# misbehave without its docstrings/asserts.
# A Windows VERSIONINFO resource, so the shipped .exe identifies itself in
# Explorer's Properties > Details (product name, version, copyright) the way
# every in-box Windows tool does, instead of shipping with a blank details
# tab like an anonymous binary. Built as a VSVersionInfo object (PyInstaller
# accepts that directly for EXE's `version`); non-Windows builds drop the
# resource themselves, so the guard here just skips the work. The version
# module exists because the binary lanes `pip install .` before freezing.
version_resource = None
if sys.platform == "win32":
    from PyInstaller.utils.win32.versioninfo import (
        FixedFileInfo,
        StringFileInfo,
        StringStruct,
        StringTable,
        VarFileInfo,
        VarStruct,
        VSVersionInfo,
    )

    from cronstable.version import version as cronstable_version

    # A PE FILEVERSION is four 16-bit integers. Take the leading numeric
    # dotted components (a dev/local suffix contributes nothing) and pad:
    # "1.2.37" -> (1, 2, 37, 0), "1.2.38.dev5+g94d6fad" -> (1, 2, 38, 0).
    numbers = []
    for part in cronstable_version.split("."):
        match = re.match(r"\d+", part)
        if match is None:
            break
        numbers.append(int(match.group()) & 0xFFFF)
        if len(numbers) == 4:
            break
    while len(numbers) < 4:
        numbers.append(0)
    filevers = tuple(numbers)
    version_resource = VSVersionInfo(
        ffi=FixedFileInfo(filevers=filevers, prodvers=filevers),
        kids=[
            StringFileInfo(
                [
                    StringTable(
                        "040904B0",  # en-US, Unicode
                        [
                            StringStruct("CompanyName", "cronstable"),
                            StringStruct(
                                "FileDescription",
                                "cronstable job scheduler",
                            ),
                            StringStruct(
                                "FileVersion", cronstable_version
                            ),
                            StringStruct("InternalName", "cronstable"),
                            StringStruct(
                                "LegalCopyright",
                                "MIT License. (c) the cronstable authors.",
                            ),
                            StringStruct(
                                "OriginalFilename", "cronstable.exe"
                            ),
                            StringStruct("ProductName", "cronstable"),
                            StringStruct(
                                "ProductVersion", cronstable_version
                            ),
                        ],
                    )
                ]
            ),
            VarFileInfo([VarStruct("Translation", [0x0409, 1200])]),
        ],
    )

a = Analysis(
    ["cronstable"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    optimize=2,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ONE kwargs dict for both EXE calls, so the two shipped layouts cannot
# drift in metadata or stripping (the onedir exe is the one inside the
# zip AND the MSI). The real differences stay at the call sites: what
# the exe embeds, and runtime_tmpdir, a one-file-only option.
exe_kwargs = dict(
    name="cronstable",
    debug=False,
    bootloader_ignore_signals=False,
    strip=STRIP,
    upx=False,
    upx_exclude=[],
    console=True,
    version=version_resource,
)
if BUNDLE in ("onedir", "both"):
    # dist/cronstable/cronstable.exe plus _internal/. The exe keeps the
    # version resource so Properties > Details works on the shipped file.
    # Under "both" this EXE lands in the workpath (COLLECT assembles
    # dist/cronstable/), so it coexists with dist/cronstable.exe below.
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        **exe_kwargs,
    )
    coll = COLLECT(
        exe,
        a.binaries,
        a.zipfiles,
        a.datas,
        strip=STRIP,
        upx=False,
        upx_exclude=[],
        name="cronstable",
    )
if BUNDLE in ("onefile", "both"):
    exe_onefile = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.zipfiles,
        a.datas,
        [],
        runtime_tmpdir=None,
        **exe_kwargs,
    )
