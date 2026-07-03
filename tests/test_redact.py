"""Secret scrubbing for archived job output (yacron2.redact)."""

from yacron2.redact import REDACTED, redact_secrets


def test_key_value_secrets_redacted():
    assert redact_secrets("password=hunter2") == "password=" + REDACTED
    assert redact_secrets("API_KEY: abc123xyz") == "API_KEY: " + REDACTED
    assert redact_secrets('secret="s3cr3t"') == 'secret="' + REDACTED + '"'


def test_non_secret_keys_untouched():
    assert redact_secrets("count=5") == "count=5"
    assert redact_secrets("status: running") == "status: running"


def test_url_credentials_redacted():
    out = redact_secrets("postgres://user:s3cret@db:5432/app")
    assert "s3cret" not in out
    assert "user:" + REDACTED + "@db" in out


def test_bearer_token_redacted():
    out = redact_secrets("Authorization: Bearer abcdef123456")
    assert "abcdef123456" not in out
    assert REDACTED in out


def test_cloud_and_service_tokens_redacted():
    assert redact_secrets("AKIAIOSFODNN7EXAMPLE") == REDACTED
    assert REDACTED in redact_secrets("xoxb-123456789012-abcdefghij")
    assert REDACTED in redact_secrets("ghp_" + "a" * 36)


def test_jwt_redacted():
    jwt = (
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSM12345"
    )
    assert REDACTED in redact_secrets(jwt)


def test_private_key_marker_redacted():
    out = redact_secrets("-----BEGIN RSA PRIVATE KEY-----MIIabc")
    assert REDACTED in out
    assert "MIIabc" not in out


def test_plain_text_unchanged():
    assert redact_secrets("just a normal log line") == "just a normal log line"
    assert redact_secrets("") == ""


def test_never_raises_on_odd_input():
    # must be safe on anything a job might print.
    redact_secrets("%%%$$$### \x00 \t weird")
    redact_secrets("=:=:=:")
