"""Compute a deterministic, order-independent fingerprint of a job set.

The *job-set ID* is a hash over the effective configuration of every job a
cronstable instance is running.  Two instances produce the **same** ID if and
only if they are running the same set of jobs, regardless of:

* the order the jobs appear in the configuration;
* whether a setting was written inline on each job or hoisted into a
  ``defaults`` block (the fingerprint is taken over the *merged*, effective
  :class:`~cronstable.config.JobConfig`, not the raw YAML text);
* equivalent spellings of the same schedule (the ``minute:``/``hour:`` object
  form normalizes to the same five-field crontab line as the string form).

This is intended for coordinating replicas: several cronstable instances
deployed
from the same configuration can confirm they hold an identical job set (e.g.
for leader election, to avoid double-running jobs) by comparing IDs.

Design notes that matter for that "byte-identical across hosts" guarantee:

* **user/group are fingerprinted as configured, not as resolved.**  A job's
  ``user: www-data`` resolves to a numeric uid from the *local* passwd
  database, and that uid can differ host to host.  We hash the configured
  value so the intent matches across hosts (see ``JobConfig.user``/``group``).
* **secret/value material is never embedded.**  The ID is logged at startup
  and served on a (possibly unauthenticated) HTTP endpoint, so it must never
  embed secret material.  Inline reporting secrets (Sentry DSN, mail password,
  webhook URL and header values) are redacted, and only the *names* of
  ``environment`` variables are hashed,
  never their values (env is a common place to carry secrets, and a per-host
  value, e.g. from ``env_file``, would otherwise make identical configs
  fingerprint differently across hosts).  We hash *whether* and *how* a secret
  is configured, never its literal value.
* **the scheme is versioned** (the ``v1:`` prefix).  IDs are only comparable
  within the same scheme version; bumping :data:`SCHEME_VERSION` lets the
  canonicalization evolve without silently making old and new IDs "disagree".

Because the fingerprint is over *effective* config, it also reflects
platform-dependent defaults (e.g. the default ``shell`` is ``/bin/sh`` on POSIX
and ``cmd.exe`` on Windows).  Compare instances running on the same platform,
which HA replicas are.
"""

import hashlib
import json
from collections.abc import Iterable
from typing import (
    Any,
    NamedTuple,
    Optional,
)

from cronstable.config import (
    DEFAULT_EVENTLOG_REPORT,
    DEFAULT_PUSH_REPORT,
    DEFAULT_REPORT_SHELL_TIMEOUT,
    JobConfig,
    schedule_object_to_crontab,
)
from cronstable.platform import DEFAULT_PRIORITY

# Canonicalization scheme version.  Prefixes the emitted ID and is folded into
# the hash input, so a future change to what/how we canonicalize can bump this
# and old/new IDs will compare unequal instead of silently colliding.
SCHEME_VERSION = "v1"

# Placeholder substituted for any inline secret *value* so the fingerprint
# never embeds secret material.  The surrounding structure (whether a secret
# is set, and via value/fromFile/fromEnvVar) is still part of the identity.
_SECRET_PLACEHOLDER = "<redacted>"

# The node types :func:`_normalize_numbers` can rewrite: a float becomes an
# int, and a dict or list may contain one.  Hoisted out of the walk so the
# tuple is built once at import rather than once per child.
_NORMALIZABLE = (float, dict, list)

# The one serializer every digest goes through: sort_keys for
# order-independence, ensure_ascii so the byte output is pure-ASCII and
# identical regardless of locale/encoding, and compact separators so there is
# exactly one serialization.
#
# It is held as a bound method because json.dumps() builds a fresh JSONEncoder
# whenever it is passed non-default arguments, and every call here passes the
# same ones.  Sharing the one instance is safe: encode() builds its
# circular-reference markers per call, so it is re-entrant.
_canonical_json = json.JSONEncoder(
    sort_keys=True, ensure_ascii=True, separators=(",", ":")
).encode

# The canonical-dict keys holding the hook blocks jobs share by identity (see
# :class:`SharedNodeMemo`).  They are also ~90% of the payload's bytes, which
# is why :func:`_spliced_bytes` caches their serialized text.
_HOOK_KEYS = frozenset(("onFailure", "onPermanentFailure", "onSuccess"))

# ``"<key>":`` as the canonical encoder writes it, so a spliced payload is
# assembled only out of bytes json itself produced.
_HOOK_PREFIXES = {
    key: (_canonical_json(key) + ":").encode("ascii") for key in _HOOK_KEYS
}

# A memo entry keeps its source node alive alongside the derived value: the
# key is id(source), and a freed source would let CPython hand the same id to
# an unrelated object.
_Hook = dict[str, Any]
_RedactTable = dict[int, tuple[_Hook, _Hook]]
_NormalizeTable = dict[int, tuple[Any, Any]]
_FragmentTable = dict[int, tuple[Any, Optional[bytes]]]


class SharedNodeMemo(NamedTuple):
    """Per-call memo over config subtrees many jobs share by identity.

    ``config.mergedicts`` merges copy-on-write, so a job that does not
    override a hook block ends up pointing at the very same dict every other
    non-overriding job (and ``DEFAULT_CONFIG``) points at.  Those three hook
    blocks are ~90% of a job's canonical payload, and without a memo
    :func:`_redact_action` hands :func:`_normalize_numbers` a fresh copy per
    job, so the shared subtree is re-walked once per job.

    The first two tables are needed together: a fresh redaction defeats the
    normalize memo, and memoizing only the redaction still re-walks its
    result, so either one alone measures as a regression.  ``fragments``
    carries the same sharing one stage further, to the JSON text a block
    serializes to (:func:`_hook_fragment`), which is the ~90% of every payload
    that comes out byte-identical from job to job.

    Only :func:`_redact_action` ever *writes* ``normalized``, and only for a
    block it just shared.  :func:`_normalize_numbers` reads it and stores
    nothing, so a job's own one-off containers (its identity dict, its
    command, its environment list) are walked and dropped exactly as before
    rather than being pinned for the length of the call.  That keeps both
    tables bounded by the number of *distinct* hook blocks in the fleet,
    which is normally three, instead of by the number of jobs.  ``fragments``
    holds one entry per distinct block as well, but it only stores the ~5 KB
    of serialized text for a block it has seen twice, so a fleet whose jobs
    each override their own hooks costs a pointer per job.
    """

    redacted: _RedactTable
    normalized: _NormalizeTable
    fragments: _FragmentTable


def _new_memo() -> SharedNodeMemo:
    return SharedNodeMemo({}, {}, {})


def _schedule_repr(job: JobConfig) -> str:
    """Normalize a job's schedule to a canonical string.

    The object form (``minute:``/``hour:``/...) collapses to the same crontab
    line as the equivalent string form, so the two spellings fingerprint
    identically -- a plain 5-field line when neither ``second`` nor ``year`` is
    used (unchanged from before), and the matching 7-/6-field line when they
    are.  Bare strings (a crontab line, or ``@reboot``) are used verbatim.
    """
    unparsed = job.schedule_unparsed
    if isinstance(unparsed, str):
        # Collapse runs of whitespace (and trim) so a trivially reformatted
        # crontab line, or an '@'-macro with stray spacing, fingerprints the
        # same. Cron syntax (and the '@reboot'/'@daily'/... sentinels) is
        # whitespace-delimited single tokens, so this is lossless.
        return " ".join(unparsed.split())
    # Shared object->crontab builder, so the fingerprint, the parsed schedule
    # and the web UI label can never disagree on how the object form maps to a
    # crontab line (including the second/year columns).
    return schedule_object_to_crontab(unparsed)


def _command_repr(command: str | list[str]) -> dict[str, Any]:
    """Structural representation of a job's command.

    Keeps the shell-string vs argv-list distinction (they behave differently),
    rather than joining a list into a string, which would be lossy:
    ``["echo", "a b"]`` and ``["echo", "a", "b"]`` must not collide.
    """
    if isinstance(command, list):
        return {"argv": command}
    return {"shell_command": command}


def _redact_report(report: dict[str, Any]) -> dict[str, Any]:
    """Copy a report block, replacing inline secret values with a marker.

    Only the literal ``value`` of a sentry DSN / mail password / webhook URL
    (plus webhook header values) is redacted; the ``fromFile`` /
    ``fromEnvVar`` references are paths and env-var names (not secrets) and
    are kept, since they are part of the job's identity.

    Copy-on-write, not ``deepcopy``: the report carries long immutable template
    strings (sentry body, the ~300-char webhook body) that we only ever read,
    so we shallow-copy just the dicts on the path to each redacted leaf and
    share every untouched subtree by reference. The input ``report`` (a live
    JobConfig dict) is never mutated -- see test_canonical_job_is_json_safe_
    and_pure -- and downstream (``_normalize_numbers``, ``json.dumps``) only
    reads, so reference-sharing is safe and the output stays byte-identical.
    """
    out = dict(report)
    sentry = out.get("sentry")
    if isinstance(sentry, dict):
        dsn = sentry.get("dsn")
        if isinstance(dsn, dict) and dsn.get("value") is not None:
            out["sentry"] = {
                **sentry,
                "dsn": {**dsn, "value": _SECRET_PLACEHOLDER},
            }
    mail = out.get("mail")
    if isinstance(mail, dict):
        password = mail.get("password")
        if isinstance(password, dict) and password.get("value") is not None:
            out["mail"] = {
                **mail,
                "password": {**password, "value": _SECRET_PLACEHOLDER},
            }
    webhook = out.get("webhook")
    if isinstance(webhook, dict):
        new_webhook = None
        url = webhook.get("url")
        if isinstance(url, dict) and url.get("value") is not None:
            new_webhook = dict(webhook)
            new_webhook["url"] = {**url, "value": _SECRET_PLACEHOLDER}
        headers = webhook.get("headers")
        if isinstance(headers, dict):
            # header *values* commonly carry credentials (e.g. an
            # Authorization token); keep the names, which are identity.
            if new_webhook is None:
                new_webhook = dict(webhook)
            new_webhook["headers"] = dict.fromkeys(
                headers, _SECRET_PLACEHOLDER
            )
        if new_webhook is not None:
            out["webhook"] = new_webhook
    return out


def _omit_default_report_fields(report: dict[str, Any]) -> dict[str, Any]:
    """Drop report fields that post-date the v1 scheme while still at default.

    The omit-when-default rule (see :func:`canonical_job`) applies to nested
    report keys too, and is easier to get wrong here: a new entry in
    ``config._REPORT_DEFAULTS`` merges into every job's report block and lands
    in identity without anything in this module changing, silently repointing
    every job's digest.

    ``report`` must be a dict the caller owns -- it is: ``_redact_report``
    always returns a fresh top-level copy. Untouched subtrees are still shared
    by reference with the live JobConfig, so ``shell`` is replaced, never
    mutated in place (see test_canonical_job_is_json_safe_and_pure).

    An inline ``timeout: 60`` parses to ``60.0`` via ``Float()`` while the
    default inherited from ``_REPORT_DEFAULTS`` stays the int ``60``; ``==``
    spans both, so the two spell the same identity -- consistent with what
    ``_normalize_numbers`` guarantees for every other numeric field.
    """
    shell = report.get("shell")
    if (
        isinstance(shell, dict)
        and shell.get("timeout") == DEFAULT_REPORT_SHELL_TIMEOUT
    ):
        report["shell"] = {k: v for k, v in shell.items() if k != "timeout"}
    # The whole push block post-dates v1: at its defaults it vanishes from
    # identity; a job that actually enables (or tunes) push gets a new
    # digest, correctly, since what fires on failure changed. pop() is
    # safe: `report` is the fresh top-level copy _redact_report returned.
    if report.get("push") == DEFAULT_PUSH_REPORT:
        report.pop("push")
    # The eventlog block post-dates v1 exactly as push does, and takes the
    # same rule: at its defaults it leaves identity entirely, so no existing
    # job's digest moves on upgrade, while a job that enables it gets a new
    # one, correctly, because what happens on failure changed. Nothing here
    # is redacted on the way past: `source` is a machine-local name and the
    # block holds no secret, which is why it has no entry in
    # _redact_report.
    if report.get("eventlog") == DEFAULT_EVENTLOG_REPORT:
        report.pop("eventlog")
    return report


def _redact_action(
    action: dict[str, Any], memo: Optional[SharedNodeMemo] = None
) -> dict[str, Any]:
    """Copy an on{Failure,PermanentFailure,Success} block, redacting secrets.

    Preserves everything else (e.g. the ``retry`` policy under ``onFailure``).

    With a ``memo`` (see :class:`SharedNodeMemo`) the copy is made once per
    distinct source block rather than once per job, and its normalization is
    recorded at the same time so every job that inherits the block shares
    both.  Returning a shared result is safe for the same reason the
    copy-on-write redaction is: everything downstream only reads.
    """
    if memo is not None:
        hit = memo.redacted.get(id(action))
        if hit is not None:
            return hit[1]
    out = dict(action)
    report = out.get("report")
    if isinstance(report, dict):
        out["report"] = _omit_default_report_fields(_redact_report(report))
    if memo is not None:
        memo.redacted[id(action)] = (action, out)
        memo.normalized[id(out)] = (out, _normalize_numbers(out))
    return out


def canonical_job(
    job: JobConfig, memo: Optional[SharedNodeMemo] = None
) -> dict[str, Any]:
    """Build the canonical, host-independent identity dict for one job.

    Includes every behavior-affecting field of the effective config.  The
    resolved uid/gid (host-specific) are deliberately excluded in favor of the
    configured ``user``/``group``; inline secret values are redacted.

    Fields added AFTER the v1 scheme shipped are included only when they
    differ from their default (see ``out`` below): serialization is
    ``sort_keys`` over the whole dict, so an unconditional new key would
    change every existing job's digest on upgrade -- mass-invalidating the
    persisted retry ladders and ``@reboot`` markers keyed by
    :func:`job_digest`.  Omit-when-default keeps every pre-existing config's
    digest byte-identical without a scheme bump; a job that actually sets
    the new field gets (correctly) a new identity.

    ``memo`` is an optional per-call :class:`SharedNodeMemo`.  Passing one
    changes nothing about the identity that comes out, only how much of the
    work is repeated across the jobs of one set.
    """
    timezone = job.timezone
    out = {
        "name": job.name,
        "command": _command_repr(job.command),
        "schedule": _schedule_repr(job),
        "shell": job.shell,
        "concurrencyPolicy": job.concurrencyPolicy,
        # where the job runs under leader election: a behaviour-affecting,
        # host-independent field, so replicas disagreeing on it should show as
        # drift rather than silently coordinate differently.
        "clusterPolicy": job.clusterPolicy,
        "captureStderr": job.captureStderr,
        "captureStdout": job.captureStdout,
        "streamPrefix": job.streamPrefix,
        "saveLimit": job.saveLimit,
        "maxLineLength": job.maxLineLength,
        # The resolved scheduling frame fully captures firing behavior, so the
        # raw ``utc`` flag is NOT hashed separately: it would be redundant and
        # would split behaviorally-identical configs. job.timezone is "UTC"
        # when utc=true (or unset), the IANA name when a timezone is set (the
        # raw utc flag is then inert), and None for local time (utc=false, no
        # timezone).
        "timezone": (str(timezone) if timezone is not None else None),
        "enabled": job.enabled,
        # gates every scheduled fire (like `enabled`), so replicas that
        # disagree on it must show as drift.  The catch-up trio
        # (onMissed/startingDeadlineSeconds/catchupJitterSeconds), the
        # archival pair and the sla/onLate pair stay excluded: restart-time,
        # observability-only or alerting-only behaviour that never changes
        # what runs or when (see cronstable.config.JobConfig.__init__).
        "onlyIfLastSucceeded": job.onlyIfLastSucceeded,
        "failsWhen": job.failsWhen,
        "onFailure": _redact_action(job.onFailure, memo),
        "onPermanentFailure": _redact_action(job.onPermanentFailure, memo),
        "onSuccess": _redact_action(job.onSuccess, memo),
        # Only the SET of variable NAMES is identity, never the values: env
        # values are a common place to carry secrets (and the id is logged and
        # served), and a per-host value (e.g. from env_file, read at parse
        # time) would otherwise make byte-identical configs fingerprint
        # differently across hosts, defeating HA coordination. The name set
        # DOES include names contributed by env_file, so replicas must ship an
        # env_file with the same variable names (only the values may differ per
        # host). Sorted so it is independent of declaration order, over a list
        # rather than a generator: sorted() drains it either way, and the
        # generator only adds a frame to resume per name.
        "environment": sorted([e["key"] for e in job.environment]),
        # `workingDirectory` is deliberately absent, for the reason directly
        # above: it is a per-host path, and a fleet legitimately runs the
        # same logical job from D:\jobs on a Windows replica and /srv/jobs
        # on a Linux one.  Folding it into identity would read as permanent
        # drift and split the job-set id the state and cluster backends
        # namespace on.  Do not "fix" this by adding it.
        "executionTimeout": job.executionTimeout,
        "killTimeout": job.killTimeout,
        "statsd": job.statsd,
        # configured values, NOT the resolved uid/gid (which are host-specific)
        "user": job.user,
        "group": job.group,
    }
    if job.concurrencyScope != "node":
        # cluster-wide concurrency gates every fire fleet-wide (replicas
        # disagreeing on it must show as drift), so it is identity -- but
        # only when set, per the omit-when-default rule above.
        out["concurrencyScope"] = job.concurrencyScope
    if job.priority != DEFAULT_PRIORITY:
        # A launch-shaping field, like executionTimeout and killTimeout, and
        # host-independent: the LEVEL is identity, never the nice number or
        # priority class it resolves to on one platform.  Only when set, per
        # the same rule, so no existing digest moves.
        out["priority"] = job.priority
    return out


def _normalize_numbers(
    obj: Any, memo: Optional[_NormalizeTable] = None
) -> Any:
    """Collapse the int/float distinction by value, recursively.

    A whole-number float (``30.0``) canonicalizes to the same int (``30``) it
    would be if inherited from a ``DEFAULT_CONFIG`` int literal.  Without this,
    a ``Float()``-typed field (``killTimeout``, the retry delays, ...) written
    *inline*, where strictyaml coerces it to a float, would hash differently
    from the *same value inherited from defaults* (which stays a Python int),
    breaking the inline-vs-defaults guarantee.  ``bool`` is left untouched (it
    is an ``int`` subclass but must stay ``true``/``false`` in JSON), and a
    fractional float (``0.5``) is preserved.

    A ``memo`` (the ``normalized`` table of a :class:`SharedNodeMemo`)
    short-circuits a subtree that has already been normalized under a
    different job.  The result is a pure function of the node, so serving a
    remembered one is byte-identical; the table is keyed by identity, never
    equality, so nothing that merely *looks* alike is ever collapsed.

    Children are recursed into only when they *can* come back different: a
    float (the whole-number collapse) or a container that may hold one.
    ``str``/``int``/``bool``/``None`` are already canonical and a call on one
    returns it unchanged, so :data:`_NORMALIZABLE` is the same predicate the
    call would have applied to itself (an ``isinstance`` tuple rather than a
    ``type()`` test, so subclasses stay covered).  A job's canonical dict is
    ~30 scalars around 3 shared hook blocks, so the filter settles almost
    every node the walk meets without a call.  Containers are tested first for
    the same reason, and a float is rare enough to test for last.
    """
    if isinstance(obj, dict):
        if memo is not None:
            hit = memo.get(id(obj))
            if hit is not None:
                return hit[1]
        return {
            k: (
                _normalize_numbers(v, memo)
                if isinstance(v, _NORMALIZABLE)
                else v
            )
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [
            _normalize_numbers(v, memo) if isinstance(v, _NORMALIZABLE) else v
            for v in obj
        ]
    # A bool cannot reach int() here: it subclasses int, NOT float, so the
    # test below rejects True/False and they stay true/false in JSON.
    if isinstance(obj, float) and obj.is_integer():
        return int(obj)
    return obj


def _hook_fragment(block: Any, table: _FragmentTable) -> bytes:
    """Serialize one already-normalized hook block, cached across jobs.

    Caching starts at a block's *second* sighting.  A block seen only once
    (the fleet where every job overrides its own hooks) is serialized once
    either way, so storing its ~5 KB of text would cost that much per job for
    nothing; remembering it as ``None`` keeps the entry down to a pointer.
    The block itself is stored in both cases, which is what makes the ``id()``
    key sound: a freed block would let CPython hand its id to an unrelated
    object.
    """
    entry = table.get(id(block))
    if entry is None:
        table[id(block)] = (block, None)
        return _canonical_json(block).encode("ascii")
    fragment = entry[1]
    if fragment is None:
        fragment = _canonical_json(block).encode("ascii")
        table[id(block)] = (block, fragment)
    return fragment


def _spliced_bytes(doc: dict[str, Any], memo: SharedNodeMemo) -> bytes:
    """:data:`_canonical_json` over ``doc``, reusing cached hook fragments.

    Byte-identical to serializing ``doc`` whole, by construction.  With
    ``sort_keys`` and ``separators=(",", ":")``, a JSON object is

        ``{`` + ``,``.join(``<key>`` ``:`` ``<value>``) + ``}``

    over its items in sorted key order, and JSON encodes each value with no
    reference to the keys around it.  So a contiguous *run* of sorted keys,
    handed to the same encoder as an object of its own, comes back as exactly
    that run's pairs wrapped in its own braces (sorting a subset preserves the
    relative order of the whole).  Stripping those braces and re-joining the
    runs and the hook pairs with commas therefore rebuilds the whole object
    byte for byte, and the ``ensure_ascii`` output is pure ASCII so slicing
    bytes off the ends can never cut a character in half.

    The three hook blocks are ~90% of the payload and are usually the *same*
    three objects across a job set, so the encoder ends up running over a few
    hundred bytes of per-job identity rather than ~5 KB that is mostly text it
    produced for the previous job.

    ``doc`` must already be normalized: this splices serialization only, and
    :func:`_hook_fragment` caches what it is given.
    """
    parts: list[bytes] = []
    run: dict[str, Any] = {}
    for key in sorted(doc):
        if key not in _HOOK_KEYS:
            run[key] = doc[key]
            continue
        if run:
            # [1:-1] drops this run's own braces; it is non-empty, so there
            # are always braces to drop.
            parts.append(_canonical_json(run).encode("ascii")[1:-1])
            run = {}
        parts.append(
            _HOOK_PREFIXES[key] + _hook_fragment(doc[key], memo.fragments)
        )
    if run:
        parts.append(_canonical_json(run).encode("ascii")[1:-1])
    return b"{" + b",".join(parts) + b"}"


def _canonical_bytes(obj: Any, memo: Optional[SharedNodeMemo] = None) -> bytes:
    # _normalize_numbers so int and float spellings of the same value cannot
    # diverge; _canonical_json (see above) for the settings that make the
    # serialization the single canonical one.
    #
    # Without a memo the serialization stops there, and that plain path is
    # what job_digest() answers with and what the splice is tested against.
    # With a memo it goes through _spliced_bytes instead, which reuses the
    # hook fragments the other jobs of the set already serialized.
    if memo is None:
        return _canonical_json(_normalize_numbers(obj)).encode("ascii")
    return _spliced_bytes(_normalize_numbers(obj, memo.normalized), memo)


def job_digest(job: JobConfig, memo: Optional[SharedNodeMemo] = None) -> str:
    """Hex SHA-256 of a single job's canonical identity.

    ``memo``, when given, is a per-call :class:`SharedNodeMemo` shared with
    the other jobs of the same set; it never changes the digest.
    """
    return hashlib.sha256(
        _canonical_bytes(canonical_job(job, memo), memo)
    ).hexdigest()


def job_digest_cached(job: JobConfig) -> str:
    """:func:`job_digest` memoized on the JobConfig itself.

    The daemon asks for the same job's digest several times per run (the run
    record, the retry ladder, the @reboot marker) and the answer is a pure
    function of a config the parser already froze: nothing outside
    :mod:`cronstable.config` assigns JobConfig attributes, and a reload
    rebuilds every JobConfig rather than editing the live ones, so the first
    answer stays the right one for as long as the instance exists.

    Use this only for parser-built jobs.  :func:`job_digest` stays the live
    function of a job's current attributes and is what a caller that edits a
    JobConfig in place (tests do) must keep using, since the memo here would
    hand such a caller the pre-edit digest.
    """
    digest = job._digest
    if digest is None:
        digest = job_digest(job)
        job._digest = digest
    return digest


def job_set_id(jobs: Iterable[JobConfig]) -> str:
    """Compute the order-independent fingerprint of a set of jobs.

    Returns a string of the form ``"v1:<64 hex chars>"``.  Per-job digests are
    sorted (neutralizing order) and hashed together; an empty job set yields a
    stable, well-defined ID.
    """
    # One memo for the whole set: the hook blocks are ~90% of a job's payload
    # and most jobs inherit them from the same DEFAULT_CONFIG objects, so this
    # is where the sharing pays.  It also pins every source node it keys on
    # for its own lifetime, which is what makes the id() keys sound even if a
    # caller feeds a generator that owns the only reference to each job.
    memo = _new_memo()
    digests = sorted([job_digest(job, memo) for job in jobs])
    combined = SCHEME_VERSION + "\n" + "\n".join(digests)
    final = hashlib.sha256(combined.encode("ascii")).hexdigest()
    return "{}:{}".format(SCHEME_VERSION, final)
