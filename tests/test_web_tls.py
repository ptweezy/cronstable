"""Native TLS on the web listeners, and the shared helper behind it.

Two tiers, deliberately:

* the crypto-free tier drives the pure logic (scheme handling in
  ``web_site_from_url``, the on-disk signature, the loadability probe, the
  restart triage in ``start_stop_web_app``) with no certificates at all, so
  it still runs where ``cryptography`` has no wheel (win-arm64);
* the crypto-gated tier mints a real CA and does real handshakes against a
  real listener, covering what only a handshake can prove: that https serves,
  that an untrusting client is refused, that a mixed http+https listen list
  works on one runner, and that ``clientCa`` actually requires a client
  certificate.

Style follows tests/test_state_job_api.py: bare ``async def``, the server in
a ``try`` with ``cleanup()`` in ``finally``.
"""

import ssl

import aiohttp
import pytest

from cronstable import tlsutil
from cronstable.config import (
    ConfigError,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.cron import Cron, web_site_from_url
from tests._helpers import _backend, _free_port, _state_cfg, _write_tls

# The TLS cert cluster (_gen_ca/_gen_leaf/_write_tls/_free_port) lives in
# tests/_helpers.py; this file uses its defaults (cn="web-ca", a localhost
# leaf with loopback IP SANs, files <cn>-leaf.pem / <cn>-leaf.key), which
# reproduce the copy that used to live here.

# A syntactically-PEM-looking file that no TLS stack can parse: the shape a
# half-written rotation leaves behind, and what the loadability probes must
# treat as "not yet".
GARBAGE_PEM = b"-----BEGIN CERTIFICATE-----\nnot-valid-base64\n"


class _StubRunner:
    """Enough of an AppRunner for ``BaseSite.__init__``, which only checks
    that ``setup()`` has run (``runner.server is not None``). Lets the
    scheme/context wiring be asserted without binding a socket."""

    server = object()


def _cfg(yaml):
    return parse_config_string(yaml, "test.yaml")


def _cross(yaml):
    """Parse AND run the cross-section checks, which is where the MCP
    fail-closed gate lives (it needs the web and mcp sections together)."""
    cfg = _cfg(yaml)
    _validate_cross_sections(cfg)
    return cfg


def _client_ctx(ca_path):
    return ssl.create_default_context(cafile=ca_path)


def _copy_bytes(src, dst):
    """Overwrite ``dst`` with ``src``'s bytes: an in-place rotation."""
    with open(src, "rb") as fh:
        payload = fh.read()
    with open(dst, "wb") as fh:
        fh.write(payload)


# ==========================================================================
# crypto-free tier: no certificates minted, runs everywhere
# ==========================================================================


def test_web_site_from_url_https_without_context_is_skipped(caplog):
    # config validation normally refuses this shape, so reaching the bind loop
    # means the context failed to BUILD. Serving the port in cleartext would
    # be worse than not serving it, so the entry is skipped.
    with pytest.raises(ValueError):
        web_site_from_url(_StubRunner(), "https://127.0.0.1:8443")
    assert "no usable web.tls material" in caplog.text


def test_web_site_from_url_https_needs_host_and_port():
    # aiohttp would silently default a TLS site to 8443; an operator who typed
    # a bare host did not ask for that.
    with pytest.raises(ValueError):
        web_site_from_url(_StubRunner(), "https://127.0.0.1", object())


def test_web_site_from_url_http_never_gets_the_context():
    # a listen list may mix schemes on one runner; the plaintext entries must
    # not accidentally inherit the https context.
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    site = web_site_from_url(_StubRunner(), "http://127.0.0.1:8080", ctx)
    assert site._ssl_context is None


def test_web_site_from_url_https_gets_the_context():
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    site = web_site_from_url(_StubRunner(), "https://127.0.0.1:8443", ctx)
    assert site._ssl_context is ctx


def test_listener_tls_loadable_absent_block_is_true():
    # a plaintext listener has nothing on disk to pre-validate, so it must
    # never defer a restart on this probe.
    assert tlsutil.listener_tls_loadable(None) is True
    assert tlsutil.listener_tls_loadable({}) is True


def test_listener_tls_loadable_missing_files_is_false(tmp_path):
    assert (
        tlsutil.listener_tls_loadable(
            {"cert": str(tmp_path / "nope.pem"), "key": str(tmp_path / "n.key")}
        )
        is False
    )


def test_listener_tls_loadable_half_configured_is_false(tmp_path):
    # config validation rejects cert-without-key, so reaching the probe with
    # one means treat it as unloadable rather than raise KeyError out of a
    # "can we restart?" question.
    assert (
        tlsutil.listener_tls_loadable({"cert": str(tmp_path / "c.pem")})
        is False
    )


def test_listener_tls_loadable_garbage_pem_is_false(tmp_path):
    cert = tmp_path / "c.pem"
    key = tmp_path / "c.key"
    cert.write_bytes(GARBAGE_PEM)
    key.write_bytes(GARBAGE_PEM)
    assert (
        tlsutil.listener_tls_loadable({"cert": str(cert), "key": str(key)})
        is False
    )


def test_tls_file_signature_tolerates_an_absent_optional_key(tmp_path):
    # clientCa is optional; the cluster's old stat loop would KeyError here.
    cert = tmp_path / "c.pem"
    cert.write_bytes(b"x")
    sig = tlsutil.tls_file_signature(
        {"cert": str(cert), "key": None}, tlsutil.LISTENER_TLS_KEYS
    )
    assert sig["key"] is None
    assert sig["clientCa"] is None
    assert sig["cert"] is not None


def test_tls_file_signature_changes_on_in_place_rotation(tmp_path):
    # the whole point: same path, new bytes, and the config is byte-identical,
    # so nothing else would notice.
    cert = tmp_path / "c.pem"
    cert.write_bytes(b"one")
    before = tlsutil.tls_file_signature({"cert": str(cert)}, ("cert",))
    cert.write_bytes(b"one-and-a-bit")
    assert tlsutil.tls_file_signature({"cert": str(cert)}, ("cert",)) != before


def test_build_listener_ssl_context_requires_clients_only_with_a_ca(tmp_path):
    tls = _write_tls(tmp_path)
    plain = tlsutil.build_listener_ssl_context(tls["cert"], tls["key"])
    assert plain.verify_mode is ssl.CERT_NONE
    mutual = tlsutil.build_listener_ssl_context(
        tls["cert"], tls["key"], client_ca=tls["ca"]
    )
    # CERT_REQUIRED, never CERT_OPTIONAL: "optional client CA" means the CA is
    # optional, not the client certificate. CERT_OPTIONAL would complete the
    # handshake for a client presenting nothing, silently making clientCa a
    # no-op for anyone who set it expecting caller authentication.
    assert mutual.verify_mode is ssl.CERT_REQUIRED


def test_verifying_client_context_is_none_without_options():
    # so a client with no TLS flags keeps its existing default transport
    # rather than paying for a context that changes nothing.
    assert tlsutil.build_verifying_client_ssl_context() is None


def test_verifying_client_context_refuses_a_key_without_a_cert():
    # a key alone cannot present an identity, so the alternative to failing
    # is accepting --client-key and silently ignoring it, leaving the caller
    # believing it authenticated to an mTLS listener that refused it.
    with pytest.raises(ValueError, match="needs its certificate"):
        tlsutil.build_verifying_client_ssl_context(key="/k")


def test_listener_tls_configured_rejects_a_blank_block():
    # strictyaml maps a blank scalar (`cert:` with nothing after it, what a
    # template renders for an unset variable) to None, giving a TRUTHY dict
    # of falsy values. Every path that reaches for tls["cert"] gates on this
    # rather than on the dict's own truthiness.
    assert tlsutil.listener_tls_configured({"cert": None, "key": None}) is False
    assert tlsutil.listener_tls_configured({"clientCa": None}) is False
    assert tlsutil.listener_tls_configured(None) is False
    assert tlsutil.listener_tls_configured({"cert": "/c", "key": "/k"}) is True


async def test_blank_web_tls_block_still_serves_the_plaintext_listeners():
    # A present-but-blank tls block cannot coexist with an https:// listener
    # (config validation refuses that pair), so it means no TLS is wanted.
    # Building a context from it would raise TypeError, which is not what the
    # failure path catches, and would take the whole web app down.
    port = _free_port()
    cron = Cron(None)
    try:
        await cron.start_stop_web_app(
            {
                "listen": ["http://127.0.0.1:{}".format(port)],
                "tls": {"cert": None, "key": None},
                "ui": True,
            }
        )
        assert cron.web_runner is not None
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://127.0.0.1:{}/version".format(port)
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


def test_verifying_client_context_insecure_disables_verification():
    ctx = tlsutil.build_verifying_client_ssl_context(insecure=True)
    assert ctx is not None
    assert ctx.check_hostname is False
    assert ctx.verify_mode is ssl.CERT_NONE


async def test_start_stop_web_app_skips_start_when_tls_is_unloadable(
    tmp_path, caplog
):
    # the config points at files that are not there: no listener, and
    # crucially web_config is NOT latched, so the next reload retries instead
    # of concluding "nothing changed" forever.
    cron = Cron(None)
    await cron.start_stop_web_app(
        {
            "listen": ["https://127.0.0.1:{}".format(_free_port())],
            "tls": {
                "cert": str(tmp_path / "absent.pem"),
                "key": str(tmp_path / "absent.key"),
            },
        }
    )
    assert cron.web_runner is None
    assert cron.web_config is None
    assert "not loadable" in caplog.text


# ==========================================================================
# config validation
# ==========================================================================


def _web_yaml(listen, tls=""):
    body = "web:\n  listen:\n"
    body += "".join("    - {}\n".format(a) for a in listen)
    return body + tls


@pytest.mark.parametrize(
    ("listen", "tls", "match"),
    [
        pytest.param(
            ["https://0.0.0.0:8443"],
            "  tls:\n    cert: /c\n",
            "must be set together",
            id="cert-without-key",
        ),
        pytest.param(
            ["https://0.0.0.0:8443"],
            "  tls:\n    clientCa: /ca\n",
            "cannot require client certificates",
            id="client-ca-without-cert",
        ),
        pytest.param(
            ["http://0.0.0.0:8080"],
            "  tls:\n    cert: /c\n    key: /k\n",
            "no web.listen address uses https",
            id="tls-without-an-https-listener",
        ),
        pytest.param(
            ["https://0.0.0.0:8443"],
            "",
            "no web.tls.cert",
            id="https-listener-without-tls",
        ),
        # guards the early returns in _validate_web_config: a TLS check
        # appended after them would be dead code for every config with no
        # metrics section, which is the common case.
        pytest.param(
            ["https://0.0.0.0:8443"],
            "  tls:\n    key: /k\n",
            "must be set together",
            id="validated-without-a-metrics-block",
        ),
    ],
)
def test_web_tls_misconfiguration_is_refused(listen, tls, match):
    with pytest.raises(ConfigError, match=match):
        _cfg(_web_yaml(listen, tls))


def test_web_tls_accepted_with_an_https_listener():
    cfg = _cfg(
        _web_yaml(
            ["http://127.0.0.1:8080", "https://0.0.0.0:8443"],
            "  tls:\n    cert: /c\n    key: /k\n    clientCa: /ca\n",
        )
    )
    assert cfg.web_config["tls"]["clientCa"] == "/ca"


def test_mcp_on_a_routable_https_listener_still_needs_a_token():
    # transport encryption is not caller authentication.
    with pytest.raises(ConfigError, match="without authentication"):
        _cross(
            _web_yaml(
                ["https://0.0.0.0:8443"],
                "  tls:\n    cert: /c\n    key: /k\n",
            )
            + "mcp:\n  enabled: true\n"
        )


def test_mcp_on_a_routable_mtls_listener_is_allowed():
    # clientCa DOES authenticate callers (CERT_REQUIRED against that CA),
    # which is the same guarantee the gate already accepts from an
    # mTLS-terminating proxy.
    cfg = _cross(
        _web_yaml(
            ["https://0.0.0.0:8443"],
            "  tls:\n    cert: /c\n    key: /k\n    clientCa: /ca\n",
        )
        + "mcp:\n  enabled: true\n"
    )
    assert cfg.mcp_config["enabled"] is True


def _state_yaml(listen=None, tls="", extra=""):
    body = "state:\n  path: /x\n  jobApi:\n"
    if listen:
        body += "    listen: {}\n".format(listen)
    return body + extra + tls


def test_jobapi_https_listen_is_accepted():
    cfg = _cfg(
        _state_yaml(
            "https://10.0.0.5:9000",
            "    tls:\n      cert: /c\n      key: /k\n      ca: /ca\n",
            "    allowNonLoopbackBind: true\n",
        )
    ).state_config
    assert cfg["jobApi"]["tls"]["ca"] == "/ca"


@pytest.mark.parametrize(
    ("listen", "tls", "extra", "match"),
    [
        pytest.param(
            "127.0.0.1:9000",
            "    tls:\n      cert: /c\n      key: /k\n",
            "",
            "would be ignored",
            id="tls-without-an-https-listen",
        ),
        pytest.param(
            "https://10.0.0.5:9000",
            "",
            "    allowNonLoopbackBind: true\n",
            "no certificate to serve",
            id="https-without-tls",
        ),
        # `ca` is injected into every job as CRONSTABLE_STATE_CACERT, so left
        # set against a plaintext endpoint it is inert and misleading, and a
        # bad path in it fails the job CLI on a channel it was never used on.
        pytest.param(
            "127.0.0.1:9000",
            "    tls:\n      ca: /ca\n",
            "",
            "would be ignored",
            id="ca-alone-against-a-plaintext-listen",
        ),
    ],
)
def test_jobapi_tls_misconfiguration_is_refused(listen, tls, extra, match):
    with pytest.raises(ConfigError, match=match):
        _cfg(_state_yaml(listen, tls, extra))


@pytest.mark.parametrize(
    "host",
    [
        "0.0.0.0",
        "[::]",
        "[::0]",
        "[0:0:0:0:0:0:0:0]",
    ],
)
def test_jobapi_https_wildcard_bind_is_refused(host):
    # jobs dial the address they are handed, and no certificate covers
    # "every interface". The unspecified address has several spellings, so
    # the check parses rather than matching strings.
    with pytest.raises(ConfigError, match="wildcard host over"):
        _cfg(
            _state_yaml(
                "https://{}:9000".format(host),
                "    tls:\n      cert: /c\n      key: /k\n",
                "    allowNonLoopbackBind: true\n",
            )
        )


def test_jobapi_https_named_host_is_not_treated_as_a_wildcard():
    cfg = _cfg(
        _state_yaml(
            "https://jobs.internal:9000",
            "    tls:\n      cert: /c\n      key: /k\n",
            "    allowNonLoopbackBind: true\n",
        )
    ).state_config
    assert cfg["jobApi"]["listen"] == "https://jobs.internal:9000"


def test_jobapi_tls_defaults_are_filled():
    # third-level merge: a bare state block still gets the tls keys, so no
    # consumer has to guard for their absence.
    cfg = _cfg("state:\n  path: /x\n").state_config
    assert cfg["jobApi"]["tls"] == {"cert": None, "key": None, "ca": None}


def test_jobapi_partial_tls_block_keeps_the_other_defaults():
    cfg = _cfg(
        _state_yaml(
            "https://10.0.0.5:9000",
            "    tls:\n      cert: /c\n      key: /k\n",
            "    allowNonLoopbackBind: true\n",
        )
    ).state_config
    assert cfg["jobApi"]["tls"]["ca"] is None


def test_jobapi_plaintext_off_host_bind_warns(caplog):
    _cfg(
        _state_yaml(
            "10.0.0.5:9000", extra="    allowNonLoopbackBind: true\n"
        )
    )
    assert "cleartext" in caplog.text


# ==========================================================================
# crypto-gated tier: real handshakes against a real listener
# ==========================================================================


async def _serve(cron, listen, tls):
    await cron.start_stop_web_app({"listen": listen, "tls": tls, "ui": True})
    assert cron.web_runner is not None


async def test_https_listener_serves_the_dashboard(tmp_path):
    tls = _write_tls(tmp_path)
    port = _free_port()
    cron = Cron(None)
    try:
        await _serve(
            cron,
            ["https://127.0.0.1:{}".format(port)],
            {"cert": tls["cert"], "key": tls["key"]},
        )
        ctx = _client_ctx(tls["ca"])
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://127.0.0.1:{}/version".format(port), ssl=ctx
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


async def test_https_listener_refuses_a_client_that_distrusts_the_ca(
    tmp_path,
):
    tls = _write_tls(tmp_path)
    port = _free_port()
    cron = Cron(None)
    try:
        await _serve(
            cron,
            ["https://127.0.0.1:{}".format(port)],
            {"cert": tls["cert"], "key": tls["key"]},
        )
        async with aiohttp.ClientSession() as session:
            with pytest.raises((aiohttp.ClientError, ssl.SSLError, OSError)):
                # default trust store: nothing signed by our throwaway CA
                async with session.get(
                    "https://127.0.0.1:{}/version".format(port)
                ):
                    pass
    finally:
        await cron.start_stop_web_app(None)


async def test_mixed_http_and_https_listeners_on_one_runner(tmp_path):
    # aiohttp's AppRunner owns the app; each SITE owns its transport and its
    # SSL, so one app can be served plaintext on one port and over TLS on
    # another. This is the arrangement an operator gets by listing both.
    tls = _write_tls(tmp_path)
    plain_port, tls_port = _free_port(), _free_port()
    cron = Cron(None)
    try:
        await _serve(
            cron,
            [
                "http://127.0.0.1:{}".format(plain_port),
                "https://127.0.0.1:{}".format(tls_port),
            ],
            {"cert": tls["cert"], "key": tls["key"]},
        )
        ctx = _client_ctx(tls["ca"])
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://127.0.0.1:{}/version".format(plain_port)
            ) as resp:
                assert resp.status == 200
            async with session.get(
                "https://127.0.0.1:{}/version".format(tls_port), ssl=ctx
            ) as resp:
                assert resp.status == 200
            # and the TLS port is genuinely TLS: plaintext to it fails
            with pytest.raises((aiohttp.ClientError, OSError)):
                async with session.get(
                    "http://127.0.0.1:{}/version".format(tls_port)
                ):
                    pass
    finally:
        await cron.start_stop_web_app(None)


async def test_client_ca_requires_a_client_certificate(tmp_path):
    tls = _write_tls(tmp_path)
    port = _free_port()
    cron = Cron(None)
    try:
        await _serve(
            cron,
            ["https://127.0.0.1:{}".format(port)],
            {
                "cert": tls["cert"],
                "key": tls["key"],
                "clientCa": tls["ca"],
            },
        )
        url = "https://127.0.0.1:{}/version".format(port)
        # trusting the CA is not enough: the listener wants a certificate back
        async with aiohttp.ClientSession() as session:
            with pytest.raises((aiohttp.ClientError, ssl.SSLError, OSError)):
                async with session.get(url, ssl=_client_ctx(tls["ca"])):
                    pass
        # the same CA's leaf doubles as a client certificate (it carries
        # CLIENT_AUTH), so presenting it satisfies the gate
        ctx = _client_ctx(tls["ca"])
        ctx.load_cert_chain(tls["cert"], tls["key"])
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=ctx) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


async def test_client_ca_rejects_a_certificate_from_another_ca(tmp_path):
    # the CA file IS the allowlist, so a certificate from a different CA is
    # exactly the case it has to refuse.
    tls = _write_tls(tmp_path)
    rogue = _write_tls(tmp_path, cn="rogue")
    port = _free_port()
    cron = Cron(None)
    try:
        await _serve(
            cron,
            ["https://127.0.0.1:{}".format(port)],
            {
                "cert": tls["cert"],
                "key": tls["key"],
                "clientCa": tls["ca"],
            },
        )
        ctx = _client_ctx(tls["ca"])
        ctx.load_cert_chain(rogue["cert"], rogue["key"])
        async with aiohttp.ClientSession() as session:
            with pytest.raises((aiohttp.ClientError, ssl.SSLError, OSError)):
                async with session.get(
                    "https://127.0.0.1:{}/version".format(port), ssl=ctx
                ):
                    pass
    finally:
        await cron.start_stop_web_app(None)


async def test_in_place_rotation_restarts_the_listener(tmp_path):
    # the config bytes are identical across a rotation, so only the file
    # signature can notice it. Without this the daemon serves the old
    # certificate until it expires.
    tls = _write_tls(tmp_path)
    port = _free_port()
    config = {
        "listen": ["https://127.0.0.1:{}".format(port)],
        "tls": {"cert": tls["cert"], "key": tls["key"]},
        "ui": True,
    }
    cron = Cron(None)
    try:
        await cron.start_stop_web_app(config)
        first = cron.web_runner
        assert first is not None
        # an unchanged config with unchanged files is a no-op
        await cron.start_stop_web_app(config)
        assert cron.web_runner is first

        # rotate IN PLACE: mint fresh material elsewhere, then overwrite the
        # configured paths with it, which is what a secret refresh does.
        rotated = _write_tls(tmp_path, cn="rotated")
        for field in ("cert", "key"):
            _copy_bytes(rotated[field], config["tls"][field])
        await cron.start_stop_web_app(config)
        assert cron.web_runner is not None
        assert cron.web_runner is not first
        # and the NEW certificate is what is served now
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://127.0.0.1:{}/version".format(port),
                ssl=_client_ctx(rotated["ca"]),
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


async def test_half_written_rotation_keeps_the_old_listener(
    tmp_path, caplog
):
    # cert-manager / Vault / a Kubernetes secret refresh is not atomic across
    # the files, so a reload can observe one half-written. Tearing the working
    # listener down and then failing to rebuild would leave nothing serving;
    # keeping it and retrying leaves the valid old certificate up.
    tls = _write_tls(tmp_path)
    port = _free_port()
    config = {
        "listen": ["https://127.0.0.1:{}".format(port)],
        "tls": {"cert": tls["cert"], "key": tls["key"]},
        "ui": True,
    }
    cron = Cron(None)
    try:
        await cron.start_stop_web_app(config)
        first = cron.web_runner
        assert first is not None
        (tmp_path / "web-ca-leaf.pem").write_bytes(GARBAGE_PEM)
        await cron.start_stop_web_app(config)
        assert cron.web_runner is first
        assert "not yet loadable" in caplog.text
    finally:
        await cron.start_stop_web_app(None)


async def test_job_state_api_serves_over_tls(tmp_path):
    # The whole jobApi https path end to end: the scheme carried through
    # _bind_target, the context built in start(), the https _base_url handed
    # to jobs, and a real request completing over it.
    from cronstable.jobapi import JobStateAPI

    tls = _write_tls(tmp_path)
    port = _free_port()
    backend = _backend(tmp_path)
    await backend.start()
    api = JobStateAPI(
        lambda: backend,
        base_holder="h#proc",
        config={
            "listen": "https://localhost:{}".format(port),
            "maxValueBytes": 0,
            "maxArtifactBytes": 0,
            "lockTtlSeconds": 5,
            "tls": {"cert": tls["cert"], "key": tls["key"], "ca": tls["ca"]},
        },
    )
    try:
        await api.start()
        # advertised with the https scheme and the CONFIGURED host, which is
        # what a job dials and what the certificate has to cover
        assert api.base_url == "https://localhost:{}".format(port)
        # and the CA path jobs need to verify it is exposed for injection
        assert api.cacert == tls["ca"]
        ctx = _client_ctx(tls["ca"])
        async with aiohttp.ClientSession() as session:
            async with session.get(
                api.base_url + "/v1/run",
                ssl=ctx,
                headers={"Authorization": "Bearer nope"},
            ) as resp:
                # 401 is the point: the request completed over TLS and the
                # endpoint answered on its own terms.
                assert resp.status == 401
    finally:
        await api.stop()
        await backend.stop()


async def test_job_state_api_plaintext_base_url_is_unchanged(tmp_path):
    # the ephemeral loopback default keeps the http scheme and the BOUND
    # address, which is the only thing that knows the OS-assigned port
    from cronstable.jobapi import JobStateAPI

    backend = _backend(tmp_path)
    await backend.start()
    api = JobStateAPI(
        lambda: backend,
        base_holder="h#proc",
        config={"maxValueBytes": 0, "maxArtifactBytes": 0},
    )
    try:
        await api.start()
        assert api.base_url.startswith("http://127.0.0.1:")
        assert api.cacert is None
    finally:
        await api.stop()
        await backend.stop()


def test_run_environment_injects_the_cacert_only_when_set():
    from cronstable.jobapi import ENV_CACERT, RunContext, run_environment

    ctx = RunContext(
        token="t",
        run_id="r",
        job_name="j",
        attempt=0,
        scheduled_at=None,
        host="h",
        default_scope="j",
    )
    assert ENV_CACERT not in run_environment(ctx, "http://127.0.0.1:1")
    env = run_environment(ctx, "https://h:1", "/etc/pki/ca.pem")
    assert env[ENV_CACERT] == "/etc/pki/ca.pem"


async def test_bearer_token_still_applies_over_https(tmp_path):
    tls = _write_tls(tmp_path)
    port = _free_port()
    cron = Cron(None)
    try:
        await cron.start_stop_web_app(
            {
                "listen": ["https://127.0.0.1:{}".format(port)],
                "tls": {"cert": tls["cert"], "key": tls["key"]},
                "authToken": {"value": "s3cret"},
                "ui": True,
            }
        )
        ctx = _client_ctx(tls["ca"])
        url = "https://127.0.0.1:{}/jobs".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=ctx) as resp:
                assert resp.status == 401
            async with session.get(
                url,
                ssl=ctx,
                headers={"Authorization": "Bearer s3cret"},
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


def _job_api_state_yaml(store, port, tls):
    """A ``state`` block whose loopback job API serves TLS over ``https``."""
    return (
        "state:\n"
        "  path: " + str(store) + "\n"
        "  jobApi:\n"
        "    listen: https://localhost:" + str(port) + "\n"
        "    tls:\n"
        "      cert: " + tls["cert"] + "\n"
        "      key: " + tls["key"] + "\n"
        "      ca: " + tls["ca"] + "\n"
    )


async def test_plaintext_job_api_has_no_tls_signature(tmp_path):
    # The loopback plaintext default (crypto-free, so this runs everywhere)
    # watches no files: its signature is None, so the rotation check
    # short-circuits and a no-op reload never disturbs the endpoint.
    cfg = _state_cfg("state:\n  path: " + str(tmp_path))
    cron = Cron(None)
    try:
        await cron.start_stop_state(cfg)
        first = cron._job_api
        assert first is not None
        assert cron._job_api_tls_signature is None
        # an unchanged reload reaches the rotation arm but does nothing
        await cron.start_stop_state(cfg)
        assert cron._job_api is first
    finally:
        await cron.start_stop_state(None)


async def test_job_api_in_place_rotation_restarts_only_the_listener(tmp_path):
    # The jobApi https listener's analogue of
    # test_in_place_rotation_restarts_the_listener: the state config is
    # byte-identical across a rotation, so only the file signature notices it.
    # The restart rebuilds ONLY the listener: the store backend, and the
    # object identity that proves it, are untouched.
    tls = _write_tls(tmp_path)
    port = _free_port()
    cfg = _state_cfg(_job_api_state_yaml(tmp_path / "store", port, tls))
    cron = Cron(None)
    try:
        await cron.start_stop_state(cfg)
        first = cron._job_api
        backend = cron.state_backend
        assert first is not None
        assert cron._job_api_tls_signature is not None
        # an unchanged config with unchanged files leaves both in place
        await cron.start_stop_state(cfg)
        assert cron._job_api is first
        assert cron.state_backend is backend

        # rotate IN PLACE: overwrite the configured cert/key paths with fresh
        # material signed by a different CA, as a secret refresh would
        rotated = _write_tls(tmp_path, cn="rotated")
        for field in ("cert", "key"):
            _copy_bytes(rotated[field], tls[field])
        await cron.start_stop_state(cfg)
        # the listener was rebuilt; the backend was NOT
        assert cron._job_api is not None
        assert cron._job_api is not first
        assert cron.state_backend is backend
        # and the NEW certificate is what the endpoint serves now: a client
        # pinning the rotated CA completes the handshake (401 is the endpoint
        # answering on its own terms, over TLS)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                cron._job_api.base_url + "/v1/run",
                ssl=_client_ctx(rotated["ca"]),
                headers={"Authorization": "Bearer nope"},
            ) as resp:
                assert resp.status == 401
    finally:
        await cron.start_stop_state(None)


async def test_job_api_half_written_rotation_keeps_the_old_listener(
    tmp_path, caplog
):
    # A half-written rotation (cert-manager / Vault / a Kubernetes secret
    # refresh is not atomic across the files) must keep the working endpoint up
    # and retry, not tear it down and then fail to rebuild.
    tls = _write_tls(tmp_path)
    port = _free_port()
    cfg = _state_cfg(_job_api_state_yaml(tmp_path / "store", port, tls))
    cron = Cron(None)
    try:
        await cron.start_stop_state(cfg)
        first = cron._job_api
        assert first is not None
        # overwrite the cert with garbage: the signature changes (a rotation is
        # detected) but the new material will not build a context
        with open(tls["cert"], "wb") as fh:
            fh.write(GARBAGE_PEM)
        await cron.start_stop_state(cfg)
        assert cron._job_api is first
        assert "not yet loadable" in caplog.text
    finally:
        await cron.start_stop_state(None)
