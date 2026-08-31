"""Regenerate the cross-implementation X-Wing push vector.

The daemon seals alerts and the app opens them, and each repo's own suite
proves only that a side can open what that side sealed, so the two can
drift apart while both stay green.  One frozen fixture closes that gap: a
32-byte X-Wing seed, the 1216-byte public key both libraries derive from it,
and one alert plaintext sealed to that key by the daemon's own seal path.
Install the post-quantum push extra (``pip install -e ".[push-pq]"``) and run
this script to print the constant blocks for ``tests/test_push_vectors.py``
and the app's ``XWingVectorTests.swift``.  The keypair is a pure function of
``--seed``, so re-running reproduces the committed key exactly; the sealed
blob is fresh every run because HPKE encapsulates per message, and any blob
the frozen key opens serves as the vector.
"""

import argparse
import base64
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from cryptography.hazmat.primitives.asymmetric import mlkem, x25519
except ImportError:
    sys.exit(
        "cryptography is required (install the post-quantum push extra: "
        'pip install -e ".[push-pq]")'
    )

import cronstable.push as push

#: The frozen seed.  Any 32 bytes are a valid X-Wing seed (SHAKE256
#: expands them), so this one is arbitrary; it is frozen so the script
#: reproduces the committed keypair rather than a new one each run.
DEFAULT_SEED_HEX = (
    "b4c1e70d5f2a9384c6ee1027bd4f83a51c9de6027f3b48a0d5127ec9a3641f8e"
)

#: The frozen plaintext: one failure alert in the shape
#: :func:`cronstable.push.build_payload` produces, encoded the way
#: :func:`cronstable.push._encode` encodes it.
PLAINTEXT_DOC = {
    "v": 1,
    "kind": "failure",
    "host": "daemon.local",
    "name": "nightly-backup",
    "success": False,
    "exit_code": 1,
    "ts": "2026-08-31T04:00:00+00:00",
}


def expand_seed(seed: bytes) -> tuple[bytes, bytes]:
    """Split an X-Wing seed into its two private halves.

    draft-connolly-cfrg-xwing-kem's ``expandDecapsulationKey``: 96 bytes
    of SHAKE256 over the seed, read as the ML-KEM-768 d||z seed followed
    by the X25519 private key.  CryptoKit does this internally; nothing
    in ``cryptography`` exposes it, so the vector spells it out.
    """
    expanded = hashlib.shake_256(seed).digest(96)
    return expanded[0:64], expanded[64:96]


def keypair(seed: bytes):
    """The X-Wing keypair for ``seed``: (private key, 1216-byte wire key).

    The wire key is the pairing form: the ML-KEM-768 encapsulation key
    (1184 bytes) followed by the X25519 public key (32).
    """
    from cryptography.hazmat.primitives import hpke

    mlkem_seed, x25519_seed = expand_seed(seed)
    mlkem_private = mlkem.MLKEM768PrivateKey.from_seed_bytes(mlkem_seed)
    x_private = x25519.X25519PrivateKey.from_private_bytes(x25519_seed)
    wire = (
        mlkem_private.public_key().public_bytes_raw()
        + x_private.public_key().public_bytes_raw()
    )
    return hpke.MLKEM768X25519PrivateKey(mlkem_private, x_private), wire


def _chunks(text: str, width: int) -> list[str]:
    return [text[i : i + width] for i in range(0, len(text), width)]


def _python_block(name: str, value: str, quote: str = '"') -> str:
    """A wrapped module constant, 64 chars a line (ruff caps 79).

    ``quote`` picks the literal's delimiter, so text carrying double
    quotes of its own goes out single-quoted and stays readable.
    """
    parts = _chunks(value, 64)
    if len(parts) == 1:
        return "{} = {}{}{}".format(name, quote, value, quote)
    lines = ["{} = (".format(name)]
    lines += ["    {}{}{}".format(quote, part, quote) for part in parts]
    lines.append(")")
    return "\n".join(lines)


def _swift_block(name: str, value: str) -> str:
    """The same constant for the Swift suite, as joined chunks."""
    parts = [_swift_escape(part) for part in _chunks(value, 64)]
    if len(parts) == 1:
        return '    static let {} = "{}"'.format(name, parts[0])
    lines = ["    static let {} = [".format(name)]
    lines += ['        "{}",'.format(part) for part in parts]
    lines.append("    ].joined()")
    return "\n".join(lines)


def _swift_escape(text: str) -> str:
    """Escape a chunk for a Swift string literal.

    Per character, so chunking never splits an escape sequence.
    """
    return text.replace("\\", "\\\\").replace('"', '\\"')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--seed",
        default=DEFAULT_SEED_HEX,
        help="32-byte X-Wing seed, hex (default: the frozen one)",
    )
    args = parser.parse_args()

    seed = bytes.fromhex(args.seed)
    if len(seed) != 32:
        sys.exit("seed must be exactly 32 bytes ({} hex chars)".format(64))

    _, wire = keypair(seed)
    public_b64 = base64.b64encode(wire).decode("ascii")
    plaintext = json.dumps(
        PLAINTEXT_DOC, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")

    # Seal through the daemon's own path, so the fixture is what a real
    # alert looks like on the wire rather than a hand-rolled lookalike.
    ciphertext_b64 = push.seal_to_device(public_b64, plaintext, "xwing")
    sealed = base64.b64decode(ciphertext_b64)

    assert len(wire) == 1216, len(wire)
    assert len(sealed) == len(plaintext) + 1136, len(sealed)

    import cryptography

    seed_b64 = base64.b64encode(seed).decode("ascii")
    plaintext_text = plaintext.decode("utf-8")
    # Neither block escapes backslashes, so one in the fixture would
    # paste in as a constant that parses and holds the wrong bytes.
    assert "\\" not in plaintext_text

    print("# cryptography {}".format(cryptography.__version__))
    print("# seed          {} bytes".format(len(seed)))
    print("# public key    {} bytes".format(len(wire)))
    print("# plaintext     {} bytes".format(len(plaintext)))
    print("# ciphertext    {} bytes".format(len(sealed)))
    print()
    print("--- tests/test_push_vectors.py ---")
    print(_python_block("SEED_B64", seed_b64))
    print(_python_block("PUBLIC_KEY_B64", public_b64))
    print(_python_block("CIPHERTEXT_B64", ciphertext_b64))
    print(_python_block("PLAINTEXT_JSON", plaintext_text, quote="'"))
    print()
    print("--- XWingVectorTests.swift ---")
    print(_swift_block("seedBase64", seed_b64))
    print(_swift_block("publicKeyBase64", public_b64))
    print(_swift_block("ciphertextBase64", ciphertext_b64))
    print(_swift_block("plaintextJSON", plaintext_text))


if __name__ == "__main__":
    main()
