"""Canonical copies of helpers duplicated across test files (finding B7).

A leaf module like tests/_commands.py: it imports only the stdlib, pytest,
and the cronstable package, never a tests/test_*.py module, so importing it
can never create the circular in-function imports it exists to kill.

Consumers switch over file by file; until a file converts, its local copy
keeps working side by side with the one here.  Where two source files had
diverging variants of the same helper, the version here is a superset and
the parameter differences are documented on the helper itself.
"""

import asyncio
import datetime
import json
import os
import socket

import pytest

from cronstable.config import parse_config_string
from cronstable.state import FilesystemStateBackend

_UTC = datetime.timezone.utc

# Cert validity anchor. Both source files (tests/test_cluster.py and
# tests/test_web_tls.py) pin the same NOW = 2026-01-01 UTC for cert
# validity windows (not_valid_before NOW-1d, not_valid_after NOW+3650d).
TLS_NOW = datetime.datetime(2026, 1, 1, tzinfo=_UTC)


# --- config parsing shims ---------------------------------------------------
# canonical: tests/test_state.py (same 2-line body in test_state_dag_run.py
# and others; test_ui_endpoints.py imports this one)


def _state_cfg(yaml):
    return parse_config_string(yaml, "").state_config


# --- the filesystem state-backend factory -----------------------------------
# canonical: tests/test_state.py (test_perf_invariants.py keeps the same
# construction; test_backend_filesystem.py's variant with node/jsid
# parameters stays local).


def _backend(tmp_path, **over):
    cfg = {
        "path": str(tmp_path),
        "topology": "single-node",
        "deploymentId": None,
    }
    cfg.update(over)
    return FilesystemStateBackend(cfg, lambda: "jobset-abc")  # type: ignore[arg-type]


async def _drain_state_writes(cron):
    # canonical: tests/test_state.py
    await asyncio.gather(*list(cron._pending_state_writes))


# --- polling ----------------------------------------------------------------


async def _wait_until(
    pred, tries=300, interval=0.01, *, raise_on_timeout=True
):
    """Poll a predicate instead of sleeping a fixed time.

    Superset of the six per-file variants, so the tests stay fast and do not
    flake under CI load, and a never-true predicate fails cleanly instead of
    hanging.  Returns True as soon as ``pred()`` is true.  On timeout it
    raises AssertionError by default, or (``raise_on_timeout=False``) returns
    one final ``pred()`` so a boundary-true predicate still reads True.

    Source variants and how to reproduce them:
    - test_cluster.py / test_prometheus.py: the defaults (tries=300,
      interval=0.01, raising).  tests/_cron_helpers.py re-exports this
      helper for the test_cron_* split files.
    - test_state.py's merged lifecycle-hardening tests: tries=1000
      (raising).
    - test_state.py (``timeout=3.0`` = 300 x 0.01): defaults with
      raise_on_timeout=False (it returned a final predicate() check).
    - test_state_fleet_ha.py: interval=0.05, raise_on_timeout=False (it
      returned False flat; the final re-check here is strictly more
      forgiving, never stricter).
    """
    for _ in range(tries):
        if pred():
            return True
        await asyncio.sleep(interval)
    if raise_on_timeout:
        raise AssertionError(
            "condition not met within {} tries".format(tries)
        )
    return pred()


def _instant_sleep(monkeypatch):
    # canonical: identical copies in tests/test_state_dag_run.py and
    # tests/test_state_job_api.py; collapse every awaited delay to a yield.
    real_sleep = asyncio.sleep

    async def fast(_delay):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast)


# --- state-store record plumbing ----------------------------------------


def _write_raw_record(backend, stream, name, payload):
    # canonical: tests/test_state.py (byte-identical body in
    # tests/test_state_admin.py): drop a record wrapper straight onto disk so
    # the migrate walk sees a chosen schemaVersion verbatim.
    stream_dir = backend._stream_dir(stream)
    os.makedirs(stream_dir, exist_ok=True)
    with open(os.path.join(stream_dir, name), "wb") as fobj:
        fobj.write(json.dumps(payload).encode())


async def _newest(cron, stream):
    # canonical: identical copies in tests/test_state_fleet_ha.py and
    # tests/test_state_scheduler_durability.py
    recs = await cron.state_backend.list_records(
        stream, limit=1, newest_first=True
    )
    return recs[0] if recs else None


def _utc_now_plus(seconds):
    # canonical: identical copies in tests/test_backend_etcd.py and
    # tests/test_backend_kubernetes.py
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=seconds
    )


# --- CLI exit capture ---------------------------------------------------
# canonical: tests/test_state_admin.py; test_main.py's base class is the same
# RuntimeError, test_state_job_cli.py subclassed Exception (RuntimeError IS an
# Exception, so this one satisfies every existing except/raises site).


class ExitError(RuntimeError):
    pass


def _exit(code=0):
    # the default mirrors test_state_job_cli.py (a bare sys.exit());
    # test_state_admin.py and test_main.py always pass the code explicitly.
    raise ExitError(code)


# --- TLS cert cluster -----------------------------------------------------
# Reconciled from the two 145-line copies in tests/test_cluster.py:460-595
# and tests/test_web_tls.py:42-192.  _gen_ca and _free_port were
# byte-identical; _gen_leaf and _write_tls carry the differences as keyword
# parameters, documented on each.


def _free_port():
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _gen_ca(cn):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(TLS_NOW - datetime.timedelta(days=1))
        .not_valid_after(TLS_NOW + datetime.timedelta(days=3650))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True
        )
        .add_extension(
            # OpenSSL 3.x rejects a CA with no key-usage extension ("CA cert
            # does not include key usage extension"), so keyCertSign has to be
            # spelled out.
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _gen_leaf(ca_key, ca_cert, hostname, *, ip_sans=False):
    """A leaf for ``hostname``, optionally also covering the loopback IPs.

    Parameter difference between the two source copies: test_cluster.py's
    leaf carries only a ``DNSName(hostname)`` SAN, which is enough for a peer
    dialled by name (``ip_sans=False``, the default here).  test_web_tls.py's
    leaf adds ``127.0.0.1`` and ``::1`` IP SANs because a listener test dials
    ``https://127.0.0.1:PORT`` and hostname verification (on by default, and
    what an operator actually gets) needs an IP SAN to match that
    (``ip_sans=True``).

    One further reconciliation: test_cluster.py's leaf carries a critical
    KeyUsage(digitalSignature) extension that test_web_tls.py's leaf omitted.
    It is kept here unconditionally; digitalSignature is exactly what an
    ECDSA handshake uses, so the web-listener handshakes are unaffected.
    """
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    sans = [x509.DNSName(hostname)]
    if ip_sans:
        sans += [
            x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            x509.IPAddress(ipaddress.ip_address("::1")),
        ]
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
        )
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(TLS_NOW - datetime.timedelta(days=1))
        .not_valid_after(TLS_NOW + datetime.timedelta(days=3650))
        .add_extension(
            x509.SubjectAlternativeName(sans),
            critical=False,
        )
        .add_extension(
            # OpenSSL 3.x refuses a chain whose leaf carries no authority key
            # identifier ("Missing Authority Key Identifier"), so this is not
            # decoration.
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                ca_key.public_key()
            ),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def _write_tls(dirpath, cn="web-ca", *, suffix="leaf", ip_sans=True):
    """Mint a CA plus one localhost leaf under ``dirpath``; return the paths.

    The ``importorskip`` lives here so every caller self-skips: cryptography
    has no win-arm64 wheel and cannot build from source on that CI runner, so
    it is not installed there; the crypto-free tiers of both source files
    still cover the pure logic on that platform.

    ``cn`` prefixes the filenames, so a second, untrusted CA can be minted
    into the same ``tmp_path`` for the rejection tests.

    Parameter differences between the two source copies:
    - test_web_tls.py: the defaults (cn="web-ca", files ``<cn>-leaf.pem`` /
      ``<cn>-leaf.key``, leaf with loopback IP SANs).
    - test_cluster.py: ``_write_tls(d, cn="cluster-ca", suffix="node",
      ip_sans=False)`` (files ``<cn>-node.pem`` / ``<cn>-node.key``, leaf
      with the DNS SAN only).
    """
    pytest.importorskip(
        "cryptography",
        reason="cryptography unavailable on this platform (e.g. win-arm64)",
    )
    from cryptography.hazmat.primitives import serialization

    ca_key, ca_cert = _gen_ca(cn)
    leaf_key, leaf_cert = _gen_leaf(
        ca_key, ca_cert, "localhost", ip_sans=ip_sans
    )
    ca_path = dirpath / (cn + "-ca.pem")
    cert_path = dirpath / (cn + "-" + suffix + ".pem")
    key_path = dirpath / (cn + "-" + suffix + ".key")
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return {
        "ca": str(ca_path),
        "cert": str(cert_path),
        "key": str(key_path),
    }
