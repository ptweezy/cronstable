"""Best-effort secret scrubbing for archived job output.

Captured stdout/stderr routinely carries credentials -- a connection string, a
bearer token, an API key echoed by a misbehaving script -- so before cronstable
writes a run's output to a durable store (see
:meth:`cronstable.cron.Cron._archive_output`) it runs each line through
:func:`redact_secrets` (or, for a whole run's output, :func:`redact_lines`,
which additionally tracks multi-line PEM blocks across lines).

This is a *defence in depth* pass, deliberately conservative: it errs toward
redacting a bit too much rather than leaking, and it is not a guarantee that no
secret survives.  It replaces only the sensitive span (keeping the surrounding
key/label for context), so an archived log stays readable.  Redaction is on by
default and can be turned off per job with ``redactArchivedSecrets: false``.
"""

import re
from collections.abc import Callable, Iterable
from typing import Optional

#: What a redacted span is replaced with.
REDACTED = "***REDACTED***"

_Repl = str | Callable[[re.Match], str]

#: The keyword alternatives inside the key=value pattern's key group, in
#: match order.  Held as data rather than spelled inline in the pattern
#: source so the casefold gate below can be checked against them at import:
#: a keyword added here that no gate word covers would silently stop being
#: redacted, which in this module is a leak, not a slowdown.
_KEY_KEYWORDS: tuple[str, ...] = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api[_-]?key",
    "apikey",
    "access[_-]?key",
    "secret[_-]?key",
    "auth[_-]?token",
    "credential",
    "private[_-]?key",
    # known vendor keys whose credential suffix is not on the generic list
    # ("auth" alone would swallow e.g. "oauth: on").
    "rediscli[_-]?auth",
)


# (compiled pattern, replacement) applied in order.  Replacements that need to
# keep surrounding context (a key name, a URL host) use a callable; the rest
# replace the whole match, which is itself the secret.
_PATTERNS: list[tuple[re.Pattern, _Repl]] = [
    # key = value / key: value where the key names a secret.  Keeps the key
    # and separator, redacts the value.  Deliberately loose around the key:
    #
    # * the key may be a SUFFIX of a compound name, with or without a
    #   separator.  There is NO left anchor at all: the keyword matches
    #   wherever it sits inside the token, and any compound prefix simply
    #   falls OUTSIDE the match, so ``MY_PASSWORD=`` and
    #   ``AWS_SECRET_ACCESS_KEY=`` (the separator forms) AND ``PGPASSWORD=``
    #   / ``MYSQLPWD=`` (libpq's and friends' UNseparated vendor forms) all
    #   redact.  The prefix is left in place because the replacement echoes
    #   the key verbatim and the prefix was never consumed, so output is
    #   character-identical either way.  Anchoring here is what the earlier
    #   ``(?<![a-z0-9])`` got wrong (it leaked the unseparated vendor forms),
    #   and the ``(?<![a-z0-9_\-])`` + ``[a-z0-9_\-]*`` pair that replaced it
    #   redacted correctly but cost ~150%: a variable-length run in front of
    #   the alternation defeats sre's literal-prefix prescan and backtracks
    #   per token.  The ``(?=[pstacr])`` lookahead is a pure prescan aid --
    #   every branch below starts with one of those letters, so it can never
    #   narrow what matches;
    # * the key may be quoted (JSON bodies): an optional closing quote is
    #   allowed between the key and the ``=``/``:`` separator;
    # * the value may be quoted: a quoted value is redacted to its closing
    #   quote (preserving any trailing structure, e.g. the rest of a JSON
    #   object); a BARE value is redacted to end of line, because a secret
    #   containing spaces ("correct horse battery staple") has no reliable
    #   delimiter and a first-word-only redaction leaks the tail while the
    #   archive is stamped redacted -- over-redaction is this module's
    #   documented bias.
    # The quoted alternatives admit backslash escapes: a JSON-encoded value
    # inside a JSON log line ("password": "{\"inner\":\"s3cret\"}") would
    # otherwise terminate the match at the first embedded \" and leak the
    # tail of the secret while the archive is stamped redacted.
    # Bare values: a value ending at a JSON/`key=value`-list delimiter
    # (comma, closing brace/bracket) is redacted only up to it, preserving
    # the surrounding structure; anything else redacts to end of line (a
    # multi-word passphrase has no reliable delimiter).
    (
        re.compile(
            r"(?i)(?=[pstacr])("
            + "|".join(_KEY_KEYWORDS)
            + r")(s?)([\"']?\s*[=:]\s*)"
            r"(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'"
            r"|[^\s,}\]]+(?=\s*[,}\]])|[^\r\n]+)"
        ),
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}{REDACTED}",
    ),
    # credentials embedded in a URL: scheme://user:PASSWORD@host (redact pass).
    # The username is OPTIONAL (``*`` not ``+``): the credential-only form
    # ``scheme://:PASSWORD@host`` -- how redis/mongodb/amqp connection strings
    # carry a password with no user -- has an empty username, and requiring a
    # username here leaked those passwords verbatim.  The ``@`` anchor keeps a
    # plain ``host:port`` URL (no userinfo) from matching.
    # The scheme is ANCHORED (the lookbehind) and BOUNDED ({0,31}): without
    # the anchor, a long run of scheme characters followed by ``://`` and a
    # long ``@``-less tail restarts the failing tail scan at every offset in
    # the run -- O(run) x O(tail) backtracking, a job-controlled ReDoS that
    # measured a clean 4x per input doubling and blocks the event loop for
    # the whole archive pass.  With one viable start per run the scan is
    # amortised linear; the bound is belt-and-braces (real schemes are
    # < 12 chars) so a pathological run also fails in O(32).
    (
        re.compile(
            r"(?<![a-zA-Z0-9+.\-])"
            r"([a-zA-Z][a-zA-Z0-9+.\-]{0,31}://[^:/\s@]*:)([^@/\s]+)(@)"
        ),
        lambda m: f"{m.group(1)}{REDACTED}{m.group(3)}",
    ),
    # Authorization: Bearer <token> / Basic <base64 user:pass>.  The Basic
    # form is anchored to the header name: "basic" is an ordinary English
    # word, and an unanchored pattern redacted innocent text like
    # "basic understanding" wholesale.
    # Both charsets are RFC 7235 token68 (letters, digits, "-._~+/", then
    # optional trailing "="), the grammar of the credentials slot in an
    # Authorization header (RFC 6750 for Bearer).  The earlier Bearer charset
    # stopped at [A-Za-z0-9._-]: a standard-base64 token with a "+" or "/" in
    # its first 8 chars escaped whole (no matchable run), and one further in
    # matched only up to it, persisting the tail of a live credential while
    # the archive was stamped redacted (the partial redaction the key=value
    # comment above rules out).  Termination is unchanged: whitespace, quotes
    # and JSON delimiters sit outside the charset, so surrounding prose and
    # structure still end the match exactly where they used to.
    (
        re.compile(r"(?i)(bearer\s+)([A-Za-z0-9._~+/\-]{8,}=*)"),
        lambda m: m.group(1) + REDACTED,
    ),
    # The separator is "a colon (spaces optional on either side) OR at least
    # one space": HTTP's optional whitespace means "Authorization:Basic x"
    # (no space after the colon) is just as valid -- and as leaky -- as the
    # spaced form, while still requiring SOME separator so an unbroken
    # "authorizationbasic" in random text cannot match.
    # Same token68 charset as Bearer above (base64url emitters put "-" and
    # "_" in the payload, and the old [A-Za-z0-9+/=] leaked the tail past the
    # first one); "=" stays inside the run here, not only trailing, so every
    # sloppily-padded value the old charset caught is still caught.
    (
        re.compile(
            r"(?i)(authorization(?:\s*:\s*|\s+)basic\s+)"
            r"([A-Za-z0-9._~+/=\-]{8,})"
        ),
        lambda m: m.group(1) + REDACTED,
    ),
    # Recognisable cloud/service token formats (the whole match is the secret).
    (re.compile(r"AKIA[0-9A-Z]{16}"), REDACTED),
    (re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{8,}"), REDACTED),
    (re.compile(r"\bgh[posur]_[0-9A-Za-z]{20,}"), REDACTED),
    # GitHub fine-grained personal access tokens.
    (re.compile(r"\bgithub_pat_[0-9A-Za-z_]{20,}"), REDACTED),
    # OpenAI/Anthropic-style bare keys and Stripe live/test keys.
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"), REDACTED),
    (re.compile(r"\b[sr]k_(?:live|test)_[0-9A-Za-z]{16,}"), REDACTED),
    # JWTs (three base64url segments joined by dots).
    (
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
        ),
        REDACTED,
    ),
    # A PEM private-key header line and anything after it on that line.  The
    # BODY of a multi-line PEM block is handled statefully by redact_lines --
    # per-line patterns cannot see that the base64 lines following the header
    # ARE the key material.
    (re.compile(r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*"), REDACTED),
]

#: Gate words for the key=value pattern, one covering substring per keyword
#: group.  Deliberately i-FREE: U+0131 (dotless i) is matched by ``(?i)i`` but
#: casefolds to itself, so a gate word spelling "credential" or
#: "authorization" would be invisible to "credentıal", i.e. a redaction FALSE
#: negative in a module whose bias is meant to run the other way.  Every
#: keyword is checked against these at import (see below).
_GATE_KEY: tuple[str, ...] = (
    "passw",  # password, passwd
    "pwd",
    "secret",  # secret, secret_key
    "token",  # token, auth_token
    "key",  # api_key, apikey, access_key, secret_key, private_key
    "auth",  # auth_token, rediscli_auth
    "credent",  # credential
)

# Cheap prefilter, parallel to _PATTERNS (same order), of two kinds:
#
# * a plain literal that MUST be present in a line for the corresponding
#   pattern to have any chance of matching, tested against the line as-is;
# * a tuple of CASEFOLDED substrings, ANY of which must be present in one
#   casefolded copy of the line, for the three case-insensitive patterns
#   (key=value, Bearer, Authorization: Basic).  None of those three has a
#   single required literal, so before the fold gate they ran on every line
#   and cost roughly 70% of a clean line's redaction.
#
# redact_secrets skips any pattern whose gate is closed: since that sub()
# could only be a no-op, the elision keeps the output byte-identical while
# dropping a typical no-secret line from 12 regex passes to none.
# "***REDACTED***" contains none of these literals, so an earlier redaction
# can never spuriously trip a later gate. Kept in lockstep with _PATTERNS by
# the checks below: plain `if`s, deliberately not `assert`s, because the
# release binary runs under -OO, which strips asserts.
_PATTERN_GATES: tuple[str | tuple[str, ...], ...] = (
    _GATE_KEY,  # 1. key = value / key: value (case-insensitive keywords)
    "://",  # 2. scheme://user:PASSWORD@host
    ("bearer",),  # 3. Bearer <token> (case-insensitive)
    ("author",),  # 4. Authorization: Basic <base64> (case-insensitive)
    "AKIA",  # 5. AWS access key id
    "xox",  # 6. Slack tokens
    "gh",  # 7. GitHub ghp_/gho_/ghs_/ghu_/ghr_ tokens
    "github_pat_",  # 8. GitHub fine-grained PAT
    "sk-",  # 9. OpenAI/Anthropic-style keys
    "k_",  # 10. Stripe [sr]k_live_/_test_ keys
    "eyJ",  # 11. JWT (base64url of the opening '{"')
    "-----",  # 12. PEM -----BEGIN ... PRIVATE KEY----- header
)


def _key_gate_covers(keyword: str) -> bool:
    """Whether every literal spelling of ``keyword`` trips :data:`_GATE_KEY`.

    Both renderings of an optional separator are checked, because the gate
    runs on the line and the line may carry either: a gate word of "passw"
    would cover "password" but not a hypothetical "pass[_-]?word".
    """
    for sep in ("", "_", "-"):
        literal = keyword.replace("[_-]?", sep)
        if not any(word in literal for word in _GATE_KEY):
            return False
    return True


if len(_PATTERN_GATES) != len(_PATTERNS):  # pragma: no cover - dev invariant
    raise RuntimeError("redact: _PATTERN_GATES is out of step with _PATTERNS")
if not all(  # pragma: no cover - dev invariant
    _key_gate_covers(keyword) for keyword in _KEY_KEYWORDS
):
    raise RuntimeError(
        "redact: a _KEY_KEYWORDS entry is not covered by _GATE_KEY, so it "
        "would be gated out and never redacted"
    )
if any(  # pragma: no cover - dev invariant
    keyword[0] not in "pstacr" for keyword in _KEY_KEYWORDS
):
    raise RuntimeError(
        "redact: a _KEY_KEYWORDS entry starts outside the (?=[pstacr]) "
        "prescan lookahead, which would narrow what the pattern matches"
    )
if any(  # pragma: no cover - dev invariant
    "i" in word
    for gate in _PATTERN_GATES
    if not isinstance(gate, str)
    for word in gate
):
    raise RuntimeError(
        "redact: a casefold gate word contains 'i', which U+0131 evades"
    )

#: The gates and patterns flattened into one tuple of
#: ``(literal gate, casefold gate, bound sub, replacement)``, built once at
#: import.  The literal gate is ``""`` (never consulted) on a fold-gated
#: entry.  Per call this replaces a fresh ``zip(..., strict=True)`` and a
#: nested tuple unpack, which together cost more than the gate tests they fed.
_STEPS: tuple[
    tuple[str, Optional[tuple[str, ...]], Callable[..., str], _Repl], ...
] = tuple(
    (gate, None, pattern.sub, repl)
    if isinstance(gate, str)
    else ("", gate, pattern.sub, repl)
    for (pattern, repl), gate in zip(_PATTERNS, _PATTERN_GATES, strict=True)
)

_PEM_BEGIN = re.compile(r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
_PEM_END = re.compile(r"(?i)-----END [A-Z0-9 ]*PRIVATE KEY-----")


def redact_secrets(text: str) -> str:
    """Return ``text`` with recognisable secrets replaced by :data:`REDACTED`.

    Conservative and best-effort (see the module docstring): applies each known
    pattern in turn.  Safe on any input and never raises.  Stateless: for a
    *sequence* of lines that may contain a multi-line PEM block, use
    :func:`redact_lines`, which redacts the block's body, not just its header.
    """
    # One casefolded copy for the three case-insensitive gates, taken BEFORE
    # any substitution and reused for the whole pass.  That stays sound
    # because a replacement only ever echoes back text it just matched or
    # inserts REDACTED, whose '*' fences cannot join with neighbouring text
    # into a gate word: the set of gate words present can shrink over the
    # pass but never grow, and a gate that opens for a word a previous
    # pattern has since removed merely runs a sub() that today runs anyway.
    # casefold(), not lower(): U+017F folds to 's' and so matches (?i)s.
    low = text.casefold()
    for required, folded, sub, repl in _STEPS:
        # Skip a pattern whose gate is closed: its sub() would be a
        # guaranteed no-op, so eliding it leaves the output byte-identical
        # while sparing a clean line every regex pass.  Gates are consumed
        # in PATTERN order, never hoisted: running the Bearer or Basic
        # pattern ahead of the "://" one changes the output of a URL
        # carrying an embedded token.
        if folded is None:
            if required not in text:
                continue
        else:
            for word in folded:
                if word in low:
                    break
            else:
                continue
        text = sub(repl, text)
    return text


def _pem_state_after(line: str, in_pem: bool) -> bool:
    """Whether a PEM block is still open after ``line``.

    Walks the BEGIN/END markers in POSITION order, so a line carrying both
    (two PEM files concatenated without a trailing newline, a log line
    quoting ``END`` before ``BEGIN``) transitions correctly.  Judging by mere
    marker *presence* mis-ordered exactly those lines and leaked the second
    key's whole base64 body.
    """
    if "-----" not in line:  # cheap gate, parallel to redact_secrets
        return in_pem
    pos = 0
    while True:
        marker = _PEM_END if in_pem else _PEM_BEGIN
        match = marker.search(line, pos)
        if match is None:
            return in_pem
        in_pem = not in_pem
        pos = match.end()


def _starts_mid_pem(lines: list[str]) -> bool:
    """Whether ``lines`` begins INSIDE a PEM block whose BEGIN was truncated.

    ``True`` iff the first PEM marker in the batch (in position order) is an
    ``END`` with no ``BEGIN`` before it -- the fingerprint of a private key
    whose header line was evicted from the bounded live-log ring before
    archiving, leaving only its base64 body and the ``END``.  Seeding
    :func:`redact_lines` with this scrubs the leading body instead of leaking
    it.  In untruncated output a ``BEGIN`` always precedes its ``END``, so this
    is ``False`` and the output is byte-identical to the unseeded walk.
    """
    for line in lines:
        if "-----" not in line:  # cheap gate, parallel to redact_secrets
            continue
        begin = _PEM_BEGIN.search(line)
        end = _PEM_END.search(line)
        if begin is None and end is None:
            continue
        return end is not None and (
            begin is None or end.start() < begin.start()
        )
    return False


def redact_lines(lines: Iterable[str]) -> list[str]:
    """Redact an ordered sequence of output lines, tracking PEM blocks.

    Applies :func:`redact_secrets` to each line, and additionally replaces
    every line inside a ``-----BEGIN ... PRIVATE KEY----- / -----END ...-----``
    block (inclusive) with :data:`REDACTED`: the base64 body lines *are* the
    key material, and no per-line pattern can recognise them in isolation.  A
    block left unterminated (truncated output) stays redacted to the end --
    erring toward over-redaction, per the module contract.

    The batch may also start MID-block: when a private key is printed early and
    the run then emits enough further output that the bounded live-log ring
    evicts the ``BEGIN`` header before archiving, the archived tail opens with
    the key's base64 body.  :func:`_starts_mid_pem` detects that (a leading
    ``END`` with no preceding ``BEGIN``) and seeds the walk ``in_pem`` so the
    orphaned body is redacted rather than passed through -- the symmetric case
    to a truncated trailing ``END``.
    """
    materialised = list(lines)
    out: list[str] = []
    in_pem = _starts_mid_pem(materialised)
    for line in materialised:
        if in_pem:
            out.append(REDACTED)
        else:
            # a line that OPENS a block still gets the per-line pass (the
            # header pattern redacts from the marker to end of line).
            out.append(redact_secrets(line))
        in_pem = _pem_state_after(line, in_pem)
    return out
