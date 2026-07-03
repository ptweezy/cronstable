"""Best-effort secret scrubbing for archived job output.

Captured stdout/stderr routinely carries credentials -- a connection string, a
bearer token, an API key echoed by a misbehaving script -- so before yacron2
writes a run's output to a durable store (see
:meth:`yacron2.cron.Cron._archive_output`) it runs each line through
:func:`redact_secrets`.

This is a *defence in depth* pass, deliberately conservative: it errs toward
redacting a bit too much rather than leaking, and it is not a guarantee that no
secret survives.  It replaces only the sensitive span (keeping the surrounding
key/label for context), so an archived log stays readable.  Redaction is on by
default and can be turned off per job with ``redactArchivedSecrets: false``.
"""

import re
from typing import Callable, List, Tuple, Union

#: What a redacted span is replaced with.
REDACTED = "***REDACTED***"

_Repl = Union[str, Callable[[re.Match], str]]


def _redact_kv(m: "re.Match") -> str:
    """Keep a secret key and its separator, redact only the value."""
    return f"{m.group(1)}{m.group(2)}{m.group(3)}{REDACTED}{m.group(5)}"


def _redact_url_pass(m: "re.Match") -> str:
    """Redact only the password in a ``scheme://user:pass@host`` URL."""
    return f"{m.group(1)}{REDACTED}{m.group(3)}"


# (compiled pattern, replacement) applied in order.  Replacements that need to
# keep surrounding context (a key name, a URL host) use a callable; the rest
# replace the whole match, which is itself the secret.
_PATTERNS: List[Tuple[re.Pattern, _Repl]] = [
    # key = value / key: value where the key names a secret. Keeps the key and
    # separator, redacts the value (quoted or bare).
    (
        re.compile(
            r"(?i)\b("
            r"password|passwd|pwd|secret|token|api[_-]?key|apikey|"
            r"access[_-]?key|secret[_-]?key|auth[_-]?token|credential"
            r")s?\b(\s*[=:]\s*)(\"?)([^\"\s]+)(\"?)"
        ),
        _redact_kv,
    ),
    # credentials embedded in a URL: scheme://user:PASSWORD@host (redact pass).
    (
        re.compile(r"([a-zA-Z][a-zA-Z0-9+.\-]*://[^:/\s@]+:)([^@/\s]+)(@)"),
        _redact_url_pass,
    ),
    # Authorization: Bearer <token>
    (
        re.compile(r"(?i)(bearer\s+)([A-Za-z0-9._\-]{8,})"),
        lambda m: m.group(1) + REDACTED,
    ),
    # Recognisable cloud/service token formats (the whole match is the secret).
    (re.compile(r"AKIA[0-9A-Z]{16}"), REDACTED),
    (re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{8,}"), REDACTED),
    (re.compile(r"\bgh[posur]_[0-9A-Za-z]{20,}"), REDACTED),
    # JWTs (three base64url segments joined by dots).
    (
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
        ),
        REDACTED,
    ),
    # A PEM private-key header line and anything after it on that line.
    (re.compile(r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*"), REDACTED),
]


def redact_secrets(text: str) -> str:
    """Return ``text`` with recognisable secrets replaced by :data:`REDACTED`.

    Conservative and best-effort (see the module docstring): applies each known
    pattern in turn.  Safe on any input and never raises.
    """
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    return text
