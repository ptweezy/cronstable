"""Verify a just-installed optional extra actually works; exit nonzero if not.

The release binary jobs and the container image builds let the optional
extras compile from sdist on arches with no prebuilt wheel. A source build,
especially under QEMU emulation, can succeed yet be subtly miscompiled, and
cronstable prefers each of these packages whenever it is merely importable:

- uvloop: the frozen binary swaps in its event loop at start-up
  (cronstable.__main__._new_event_loop); a broken build crashes the daemon
  at boot instead of falling back to stock asyncio.
- orjson: every durable-state and cluster-gossip read/write routes through
  it when importable (cronstable._json); a broken build would corrupt the
  state store instead of falling back to the stdlib json.
- pynacl: a broken bundled libsodium corrupts or crashes every push alert;
  with it absent, the daemon's fail-closed config check reports push as
  unavailable instead of sealing garbage.
- cryptography: importable is not sealable. The HPKE module can be present
  while the OpenSSL underneath has no ML-KEM, which no import can see, so the
  probe seals X-Wing for real. Absent or demoted here, the daemon advertises
  `x25519` only and refuses `xwing` pairings, which costs post-quantum
  sealing and pages nobody less.
- zeroconf: pure Python (no miscompile risk); its probe only proves the
  install produced an importable package, async surface included, before
  the bundle is frozen.

Each probe exercises the real code path the daemon depends on. The caller
uninstalls the package on a nonzero exit, so a broken build is never frozen
or shipped and the runtime fallback (where one exists) engages.

Exit 0 (nothing to verify) when the package is not installed at all: the
arch had no wheel and the optional source build was skipped or failed;
that artifact simply ships without the extra.

Usage: python pyinstaller/verify_extra.py
       {uvloop|orjson|pynacl|cryptography|zeroconf}
"""

import importlib.util
import sys


def _verify_uvloop():
    import asyncio

    import uvloop

    loop = uvloop.new_event_loop()
    try:
        loop.run_until_complete(asyncio.sleep(0))
    finally:
        loop.close()
    return "imports and runs a loop"


def _verify_orjson():
    import orjson

    # Exercise exactly what cronstable._json.dumps_bytes/loads depend on:
    # compact bytes output, the OPT_SORT_KEYS path, and a lossless
    # round-trip including non-ASCII (orjson emits raw UTF-8). A
    # miscompiled build typically fails here.
    sample = {
        "schemaVersion": "v1",
        "z": 1,
        "a": "café ☃ 日本",
        "n": [1, 2.5, True, None],
    }
    blob = orjson.dumps(sample, option=orjson.OPT_SORT_KEYS)
    if not isinstance(blob, bytes) or orjson.loads(blob) != sample:
        raise AssertionError("round-trip mismatch (miscompiled?)")
    return "imports and round-trips"


def _verify_pynacl():
    from nacl.public import PrivateKey, SealedBox

    message = b"cronstable push self-test \xf0\x9f\x94\x94"
    device = PrivateKey.generate()
    sealed = SealedBox(device.public_key).encrypt(message)
    if SealedBox(device).decrypt(sealed) != message:
        raise AssertionError("sealed-box round-trip mismatch")
    return "sealed-box round-trip ok"


def _verify_cryptography():
    from cronstable import push

    # The daemon's own probe, not a reimplementation: it seals a message to a
    # throwaway X-Wing key through the same suite the alert path uses, so a
    # pass here means this build really seals `xwing`. It logs the underlying
    # reason on failure, as it does on the daemon's first /whoami.
    if not push._xwing_probe():
        raise AssertionError(
            "X-Wing probe seal failed (see the logged reason above); the "
            "HPKE module is present but this build cannot seal ML-KEM"
        )
    return "X-Wing probe seal ok"


def _verify_zeroconf():
    import zeroconf
    import zeroconf.asyncio  # noqa: F401  (the surface cronstable.discovery uses)

    return "imports (zeroconf %s)" % getattr(zeroconf, "__version__", "?")


#: extra name -> (module probed for presence, probe)
_PROBES = {
    "uvloop": ("uvloop", _verify_uvloop),
    "orjson": ("orjson", _verify_orjson),
    "pynacl": ("nacl", _verify_pynacl),
    # `cryptography`, not its HPKE submodule: a cryptography too old for the
    # module is installed-but-useless, which the probe must fail (so the
    # caller uninstalls it) rather than skip as "nothing to verify".
    "cryptography": ("cryptography", _verify_cryptography),
    "zeroconf": ("zeroconf", _verify_zeroconf),
}


def main(argv):
    if len(argv) != 2 or argv[1] not in _PROBES:
        print("usage: verify_extra.py {%s}" % "|".join(sorted(_PROBES)))
        return 2
    name = argv[1]
    module, probe = _PROBES[name]
    if importlib.util.find_spec(module) is None:
        print("%s not installed; nothing to verify" % name)
        return 0
    try:
        detail = probe()
    except Exception as exc:  # noqa: BLE001  (any breakage means: do not ship)
        print("%s verification FAILED: %s" % (name, exc), file=sys.stderr)
        return 1
    print("%s verified: %s" % (name, detail))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
