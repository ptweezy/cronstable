"""The job-facing state CLI: `cronstable state|cursor|lock|artifact|...`.

These are the commands a job command line reaches for -- durable KV, an ETL
cursor, a distributed lock, the artifact store, idempotency keys, run-scoped
secrets.  Each is a thin client of the loopback endpoint the daemon injected
into the job's environment (:mod:`cronstable.jobapi`): it reads the injected
``CRONSTABLE_STATE_URL`` / ``CRONSTABLE_STATE_TOKEN`` and speaks HTTP over
stdlib ``urllib`` (no aiohttp, no event loop, so the command starts instantly).

They coexist with the ``cronstable state`` *admin* commands (backup / restore /
gc / check ...): the admin actions operate on the store file tree offline via
``-c``, while these job-facing actions (get / set / delete / keys, and the
other verbs) act through the running daemon.  ``cronstable.__main__`` routes by
action name, so ``cronstable state check`` and ``cronstable state get KEY``
reach the right handler.

The default *scope* every KV / cursor / artifact / lock call lands in is the
calling job's own name (the daemon fills it from the run's identity), so one
job cannot read another's keys by accident; ``--global`` (or ``--scope NAME``)
opts into a shared namespace for deliberate cross-job coordination.
"""

import argparse
import json
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

# The env vars the daemon injects (see cronstable.jobapi); the job CLI is the
# consumer.  Hardcoded here rather than imported so the CLI never pulls aiohttp
# into its import graph -- these three names are a stable wire contract.
ENV_URL = "CRONSTABLE_STATE_URL"
ENV_TOKEN = "CRONSTABLE_STATE_TOKEN"
# Injected too, but only when state.jobApi.tls.ca is set: the trust anchor to
# verify the endpoint against, because an internally-issued certificate is
# signed by nothing the job's Python already trusts.  It rides in on the
# environment rather than a flag for two reasons: it is the same kind of fact
# as the URL and the token (where the endpoint is, how to speak to it), and a
# per-subcommand --cacert would have to be threaded through every job-facing
# verb.  There is deliberately no way to switch verification OFF from in here:
# nothing running inside a job's environment should be able to downgrade the
# channel that carries that job's own secrets.
ENV_CACERT = "CRONSTABLE_STATE_CACERT"

# Injected into every DAG task so `cronstable xcom` knows this task's own
# key and the run's shared XCom scope (an artifact scope; XCom is a thin,
# task-keyed convention over the durable artifact store).  Hardcoded for the
# same reason as above -- a stable wire contract (see cronstable.dag).
ENV_DAG_XCOM_SCOPE = "CRONSTABLE_DAG_XCOM_SCOPE"
ENV_DAG_TASKKEY = "CRONSTABLE_DAG_TASKKEY"

# Exit codes: 0 success, 1 a real error, 2 usage, 3 lock not acquired, 4 the
# looked-up thing does not exist (so a script can branch on "missing"), 5 an
# idempotency key already claimed.  "Duplicate" gets its own code rather than
# sharing 1 with errors: a store outage must never masquerade as "a prior
# caller already did the work" to a guard script.
EXIT_ERROR = 1
EXIT_NOT_ACQUIRED = 3
EXIT_NOT_FOUND = 4
EXIT_DUPLICATE = 5


class _CliError(Exception):
    """A user-facing failure: printed to stderr, exits non-zero."""


# Loopback control traffic must never be proxied: the default urllib opener
# honors http_proxy/HTTP_PROXY, which would route every state call -- bearer
# run token included -- to an external proxy that cannot reach the daemon's
# 127.0.0.1 endpoint anyway (CPython's bypass logic does not exempt loopback).
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# Every request gets a deadline so a wedged daemon (state store on a dead
# mount) cannot hang the calling job forever.  Every verb but a blocking lock
# acquire is answered in milliseconds, so this is generous; the long poll
# passes its own deadline (blockSeconds plus this margin).
_DEFAULT_TIMEOUT = 30.0


# --------------------------------------------------------------------------
# HTTP transport (stdlib only, monkeypatchable in tests via _http)
# --------------------------------------------------------------------------


def _endpoint() -> tuple[str, str]:
    url = os.environ.get(ENV_URL)
    token = os.environ.get(ENV_TOKEN)
    if not url or not token:
        raise _CliError(
            "not running inside a cronstable job: {} is not set (these "
            "commands reach the daemon's loopback state endpoint, which is "
            "injected into a job's environment; is a `state` section with "
            "jobApi enabled configured?)".format(ENV_URL)
        )
    return url, token


# The verifying openers, keyed by the CA path that produced them.  Built once
# each for the same reason _OPENER is a module global: `lock run` makes two
# requests (the acquire, then the release in its finally), and re-reading and
# re-parsing the CA PEM for the second one buys nothing, since the injected
# environment cannot change under a running CLI process.  Keyed by path rather
# than held in a single slot so the cache can never hand back an opener built
# for a DIFFERENT CA than the one the environment currently names.
_TLS_OPENERS: dict[str, urllib.request.OpenerDirector] = {}


def _opener() -> urllib.request.OpenerDirector:
    """The opener this request goes through: verifying, or the plain one.

    With ENV_CACERT unset this is _OPENER itself, so the plaintext loopback
    path is exactly what it was before the endpoint could speak TLS.  With it
    set, the same no-proxy posture is paired with an HTTPSHandler pinned to
    that CA.

    ``ssl`` is imported at module scope rather than deferred into here
    because deferring it would buy nothing: ``urllib.request`` already pulls
    it in transitively (through ``http.client``), so it is resident before
    this module's first line runs, and the TLS diagnostic arm in _http needs
    the name as well.
    """
    cacert = os.environ.get(ENV_CACERT)
    if not cacert:
        return _OPENER
    cached = _TLS_OPENERS.get(cacert)
    if cached is not None:
        return cached
    try:
        ctx = ssl.create_default_context(cafile=cacert)
    except (OSError, ssl.SSLError) as ex:
        # A missing or malformed CA file is the job environment's problem and
        # must read as one.  Allowed to propagate it would be caught by the
        # OSError arm in _http and reported as "cannot reach the state
        # endpoint", blaming a daemon that is answering perfectly well.
        raise _CliError(
            "cannot load the CA bundle {} points at ({}): {}".format(
                ENV_CACERT, cacert, ex
            )
        ) from ex
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=ctx),
    )
    _TLS_OPENERS[cacert] = opener
    return opener


def _http(
    method: str,
    path: str,
    *,
    query: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
    data: Optional[bytes] = None,
    timeout: Optional[float] = None,
) -> tuple[int, dict[str, str], bytes]:
    """One request to the endpoint; return ``(status, headers, body)``.

    The single seam the whole CLI goes through, so a test monkeypatches this
    to drive every verb without a live server.  ``timeout`` overrides the
    default deadline (the blocking lock acquire needs its long poll to be
    ended by the server, not the socket).
    """
    url, token = _endpoint()
    full = url.rstrip("/") + path
    if query:
        pairs = {k: v for k, v in query.items() if v is not None}
        if pairs:
            # POSIX argv is decoded with surrogateescape, so a key sourced
            # from a filename or an upstream tool's output can hold lone
            # surrogates; urlencode re-encodes with STRICT utf-8 and raises
            # UnicodeEncodeError, which no arm below catches -- a traceback
            # out of a CLI that promises a clean error instead.
            try:
                full += "?" + urllib.parse.urlencode(pairs)
            except UnicodeEncodeError as ex:
                raise _CliError(
                    # repr, so the escaped surrogates cannot in turn blow up
                    # writing this very message to the terminal.
                    "{!r} is not valid UTF-8, so it cannot be sent to the "
                    "state endpoint".format(ex.object)
                ) from ex
    headers = {"Authorization": "Bearer " + token}
    body = None
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif data is not None:
        body = data
        headers["Content-Type"] = "application/octet-stream"
    req = urllib.request.Request(
        full, data=body, method=method, headers=headers
    )
    if timeout is None:
        timeout = _DEFAULT_TIMEOUT
    try:
        with _opener().open(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as ex:
        return ex.code, dict(ex.headers or {}), ex.read()
    except urllib.error.URLError as ex:
        # A handshake failure arrives here WRAPPED: urllib turns the OSError
        # the TLS layer raised into a URLError, so this cannot be a separate
        # `except ssl.SSLError` clause ahead of this one.  It has to be told
        # apart before the generic message below claims the endpoint is
        # unreachable, which sends the reader after a daemon that is
        # listening and answering; what is wrong is the trust between them.
        if isinstance(ex.reason, ssl.SSLError):
            raise _CliError(
                "TLS handshake with the cronstable state endpoint at {} "
                "failed: {} (does {} point at the CA that signed the "
                "daemon's certificate?)".format(url, ex.reason, ENV_CACERT)
            ) from ex
        raise _CliError(
            "cannot reach the cronstable state endpoint at {}: {}".format(
                url, ex.reason
            )
        ) from ex
    except (TimeoutError, OSError) as ex:
        # urllib wraps only errors from SENDING the request in URLError; a
        # deadline that fires while waiting for or reading the response
        # escapes as a raw TimeoutError (an OSError that is NOT a URLError
        # subclass).  Same transport failure, same clean error.  Ordered
        # after HTTPError/URLError on purpose: both subclass OSError, and
        # an HTTP error response must keep its status semantics.
        raise _CliError(
            "cannot reach the cronstable state endpoint at {}: {}".format(
                url, ex
            )
        ) from ex


def _parse_body(body: bytes) -> dict[str, Any]:
    """A response body as a dict, tolerating non-JSON.

    Error bodies are not always JSON.  This daemon's endpoint now wraps
    every error it serves in the ``{"error": ...}`` envelope, but the CLI
    may be talking to one older than that arm, and a reverse proxy or a
    transport-level failure aiohttp answers itself still renders as
    plaintext (``401: Unauthorized``).  A parse failure must degrade to
    ``{}`` so the caller's ``_ok`` raises the endpoint's HTTP status as a
    clean ``_CliError``, never a ``JSONDecodeError`` traceback.
    """
    try:
        parsed = json.loads(body) if body else {}
    except ValueError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {"value": parsed}
    return parsed


def _json(
    method: str,
    path: str,
    *,
    query: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> tuple[int, dict[str, Any]]:
    # timeout is forwarded only when explicitly set: _http applies the
    # default itself, and test fakes of the _http seam predate the kwarg.
    kwargs: dict[str, Any] = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    status, _headers, body = _http(
        method, path, query=query, json_body=json_body, **kwargs
    )
    return status, _parse_body(body)


def _ok(status: int, data: dict[str, Any]) -> dict[str, Any]:
    """Return the body, or raise the endpoint's error for a 4xx/5xx."""
    if status >= 400:
        raise _CliError(
            data.get("error")
            or "the state endpoint returned HTTP {}".format(status)
        )
    return data


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------


def _scope_of(args: argparse.Namespace) -> Optional[str]:
    """The scope to send, or ``None`` to let the daemon default to the job."""
    if getattr(args, "use_global", False):
        return "global"
    return getattr(args, "scope", None)


def _emit(value: Any) -> None:
    """Print a value: strings verbatim, everything else as compact JSON."""
    if isinstance(value, str):
        sys.stdout.write(value + "\n")
    else:
        sys.stdout.write(json.dumps(value) + "\n")


def _typed_value(raw: str) -> Any:
    """Parse a cursor value: int, then float, else the raw string.

    So a numeric watermark compares numerically (``9 < 10``) and an ISO
    timestamp compares as the string it is (``2026-06 < 2026-07``).
    """
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


# --------------------------------------------------------------------------
# verb handlers
# --------------------------------------------------------------------------


def _cmd_state(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    action = args.state_command
    if action == "get":
        status, data = _json(
            "GET", "/v1/kv/get", query={"scope": scope, "key": args.key}
        )
        if status == 404:
            print("key not found: {}".format(args.key), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if action == "set":
        if args.json:
            try:
                value = json.loads(args.value)
            except ValueError as ex:
                raise _CliError(
                    "--json was given but VALUE is not valid JSON: {}".format(
                        ex
                    )
                ) from ex
        else:
            value = args.value
        status, data = _json(
            "POST",
            "/v1/kv/set",
            json_body={"scope": scope, "key": args.key, "value": value},
        )
        _ok(status, data)
        return 0
    if action == "delete":
        status, data = _json(
            "POST",
            "/v1/kv/delete",
            json_body={"scope": scope, "key": args.key},
        )
        return 0 if _ok(status, data).get("existed") else EXIT_NOT_FOUND
    if action == "keys":
        status, data = _json("GET", "/v1/kv/list", query={"scope": scope})
        for entry in _ok(status, data).get("keys", []):
            sys.stdout.write(str(entry.get("key")) + "\n")
        return 0
    raise _CliError("unknown state action {!r}".format(action))


def _cmd_cursor(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.cursor_command == "get":
        status, data = _json(
            "GET", "/v1/cursor/get", query={"scope": scope, "name": args.name}
        )
        if status == 404:
            print("cursor not set: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if args.cursor_command == "advance":
        status, data = _json(
            "POST",
            "/v1/cursor/advance",
            json_body={
                "scope": scope,
                "name": args.name,
                "value": _typed_value(args.value),
                "force": args.force,
            },
        )
        _emit(_ok(status, data).get("value"))
        return 0
    raise _CliError("unknown cursor action {!r}".format(args.cursor_command))


def _cmd_idempotent(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.release:
        status, data = _json(
            "POST",
            "/v1/idempotency/release",
            json_body={"scope": scope, "key": args.key},
        )
        _ok(status, data)
        return 0
    status, data = _json(
        "POST",
        "/v1/idempotency/claim",
        json_body={"scope": scope, "key": args.key, "ttl": args.ttl},
    )
    # exit 0 when this caller won the claim (fresh: do the work), 5 when a
    # prior caller already claimed it (a duplicate: skip). Made for a shell
    # guard: `cronstable idempotent "$KEY" && do-the-side-effect`.  Distinct
    # from EXIT_ERROR (1, used for transport/store failures) so an outage
    # is detectable instead of reading as "already done".
    return 0 if _ok(status, data).get("fresh") else EXIT_DUPLICATE


def _cmd_secret(args: argparse.Namespace) -> int:
    if args.secret_command == "get":
        status, data = _json(
            "GET", "/v1/secret/get", query={"name": args.name}
        )
        if status == 404:
            print("secret not staged: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        _emit(_ok(status, data).get("value"))
        return 0
    if args.secret_command == "list":
        status, data = _json("GET", "/v1/secret/list")
        for name in _ok(status, data).get("names", []):
            sys.stdout.write(str(name) + "\n")
        return 0
    raise _CliError("unknown secret action {!r}".format(args.secret_command))


def _cmd_artifact(args: argparse.Namespace) -> int:
    scope = _scope_of(args)
    if args.artifact_command == "put":
        if args.file in (None, "-"):
            payload = sys.stdin.buffer.read()
        else:
            try:
                with open(args.file, "rb") as fobj:
                    payload = fobj.read()
            except OSError as ex:
                raise _CliError(
                    "cannot read {}: {}".format(args.file, ex)
                ) from ex
        status, _headers, body = _http(
            "POST",
            "/v1/artifact/put",
            query={"scope": scope, "name": args.name},
            data=payload,
        )
        data = _ok(status, _parse_body(body))
        sys.stdout.write(str(data.get("sha256", "")) + "\n")
        return 0
    if args.artifact_command == "get":
        status, _headers, body = _http(
            "GET",
            "/v1/artifact/get",
            query={"scope": scope, "name": args.name},
        )
        if status == 404:
            print("artifact not found: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_FOUND
        if status >= 400:
            _ok(status, _parse_body(body))
        if args.output in (None, "-"):
            sys.stdout.buffer.write(body)
        else:
            try:
                with open(args.output, "wb") as fobj:
                    fobj.write(body)
            except OSError as ex:
                raise _CliError(
                    "cannot write {}: {}".format(args.output, ex)
                ) from ex
        return 0
    if args.artifact_command == "list":
        status, data = _json(
            "GET", "/v1/artifact/list", query={"scope": scope}
        )
        for entry in _ok(status, data).get("artifacts", []):
            sys.stdout.write(str(entry.get("name")) + "\n")
        return 0
    raise _CliError(
        "unknown artifact action {!r}".format(args.artifact_command)
    )


def _xcom_scope() -> str:
    scope = os.environ.get(ENV_DAG_XCOM_SCOPE)
    if not scope:
        raise _CliError(
            "not running inside a cronstable DAG task: {} is not set (xcom "
            "publishes/reads task outputs within a dag_run; it only works "
            "for a task the DAG scheduler launched)".format(ENV_DAG_XCOM_SCOPE)
        )
    return scope


def _cmd_xcom(args: argparse.Namespace) -> int:
    scope = _xcom_scope()
    if args.xcom_command == "push":
        my = os.environ.get(ENV_DAG_TASKKEY)
        if not my:
            raise _CliError(
                "cannot determine this task's id ({} unset)".format(
                    ENV_DAG_TASKKEY
                )
            )
        payload = _read_input(args.file)
        status, _headers, body = _http(
            "POST",
            "/v1/artifact/put",
            query={"scope": scope, "name": my + "/" + args.key},
            data=payload,
        )
        _ok(status, _parse_body(body))
        return 0
    if args.xcom_command == "pull":
        upstream = args.task
        if args.map_index is not None:
            upstream = "{}#{}".format(upstream, args.map_index)
        status, _headers, body = _http(
            "GET",
            "/v1/artifact/get",
            query={"scope": scope, "name": upstream + "/" + args.key},
        )
        if status == 404:
            print(
                "no xcom {!r} from task {!r}".format(args.key, upstream),
                file=sys.stderr,
            )
            return EXIT_NOT_FOUND
        if status >= 400:
            _ok(status, _parse_body(body))
        _write_output(args.output, body)
        return 0
    if args.xcom_command == "list":
        status, data = _json(
            "GET", "/v1/artifact/list", query={"scope": scope}
        )
        for entry in _ok(status, data).get("artifacts", []):
            sys.stdout.write(str(entry.get("name")) + "\n")
        return 0
    raise _CliError("unknown xcom action {!r}".format(args.xcom_command))


def _read_input(path: Optional[str]) -> bytes:
    if not path or path == "-":
        return sys.stdin.buffer.read()
    try:
        with open(path, "rb") as fobj:
            return fobj.read()
    except OSError as ex:
        raise _CliError("cannot read {}: {}".format(path, ex)) from ex


def _write_output(path: Optional[str], data: bytes) -> None:
    if not path or path == "-":
        sys.stdout.buffer.write(data)
        return
    try:
        with open(path, "wb") as fobj:
            fobj.write(data)
    except OSError as ex:
        raise _CliError("cannot write {}: {}".format(path, ex)) from ex


def _lock_acquire(args: argparse.Namespace) -> tuple[bool, Optional[str]]:
    scope = _scope_of(args)
    # a --wait long poll is server-bounded by blockSeconds: the client
    # deadline is that plus margin, so the server (not the socket) ends it.
    deadline = args.timeout + _DEFAULT_TIMEOUT if args.wait else None
    status, data = _json(
        "POST",
        "/v1/lock/acquire",
        json_body={
            "scope": scope,
            "name": args.name,
            "permits": args.permits,
            "wait": args.wait,
            "blockSeconds": args.timeout,
            "ttl": args.ttl,
        },
        timeout=deadline,
    )
    data = _ok(status, data)
    return bool(data.get("acquired")), data.get("token")


def _lock_release(token: str) -> None:
    status, data = _json(
        "POST", "/v1/lock/release", json_body={"token": token}
    )
    _ok(status, data)


def _cmd_lock(args: argparse.Namespace) -> int:
    if args.lock_command == "acquire":
        acquired, token = _lock_acquire(args)
        if not acquired:
            print("lock not acquired: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_ACQUIRED
        # print the hold token so a later `cronstable lock release TOKEN` (or a
        # wrapper script) can free it.
        sys.stdout.write(str(token) + "\n")
        return 0
    if args.lock_command == "release":
        _lock_release(args.token)
        return 0
    if args.lock_command == "run":
        # reject a missing command before taking the lock, so a usage mistake
        # does not needlessly acquire (and immediately release) it.
        if not args.run_command:
            raise _CliError(
                "lock run needs a command to run (put it after `--`)"
            )
        acquired, token = _lock_acquire(args)
        if not acquired:
            print("lock not acquired: {}".format(args.name), file=sys.stderr)
            return EXIT_NOT_ACQUIRED
        try:
            completed = subprocess.run(args.run_command)  # noqa: S603
            return completed.returncode
        except OSError as ex:
            # a bad argv (command not found, not executable): report it
            # cleanly rather than leaking the raw OSError traceback. The
            # finally below still releases the lock.
            raise _CliError(
                "cannot run {!r}: {}".format(args.run_command[0], ex)
            ) from ex
        finally:
            # release even if the wrapped command raised or was signalled;
            # the daemon would also free the lease when the run ends, but
            # prompt release lets a peer proceed at once.
            if token is not None:
                try:
                    _lock_release(token)
                except _CliError:
                    pass
    raise _CliError("unknown lock action {!r}".format(args.lock_command))


_DISPATCH = {
    "state": _cmd_state,
    "cursor": _cmd_cursor,
    "lock": _cmd_lock,
    "artifact": _cmd_artifact,
    "idempotent": _cmd_idempotent,
    "secret": _cmd_secret,
    "xcom": _cmd_xcom,
}


def dispatch(args: argparse.Namespace) -> int:
    """Run a parsed job-facing command; return its exit code."""
    handler = _DISPATCH.get(args.command)
    if handler is None:  # pragma: no cover - routed by __main__
        print("cronstable: unknown command", file=sys.stderr)
        return 2
    # a bare `cronstable cursor` (no action) prints help via a missing
    # sub-action
    action_attr = {
        "cursor": "cursor_command",
        "lock": "lock_command",
        "artifact": "artifact_command",
        "secret": "secret_command",
        "xcom": "xcom_command",
    }.get(args.command)
    if action_attr is not None and getattr(args, action_attr, None) is None:
        print(
            "cronstable {}: no action given (see --help)".format(args.command),
            file=sys.stderr,
        )
        return 2
    try:
        return handler(args)
    except _CliError as ex:
        print("cronstable {}: {}".format(args.command, ex), file=sys.stderr)
        return EXIT_ERROR
