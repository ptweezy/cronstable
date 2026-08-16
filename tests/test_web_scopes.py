"""Scoped web bearer tokens (web.authTokens).

Covers the four layers of the feature:
  * ``_effective_web_scopes`` / ``_required_web_scope``: the scope model.
  * ``Cron._resolve_web_tokens``: config -> token table (fail-closed).
  * ``Cron._make_auth_middleware``: 401 (unrecognised token) vs 403
    (recognised token, insufficient scope), plus the .ics carve-out and the
    backward-compatible scalar path, as fast fake-request unit tests and one
    real-aiohttp end-to-end boot.
  * ``_redact_query_token`` / ``_access_log_class``: the carve-out's fallout,
    keeping the URL-borne token out of the aiohttp access log.

Mirrors the bearer-auth unit tests in tests/test_ui_endpoints.py (fake request
+ sentinel-return / pytest.raises) and the app-boot test in tests/test_cron.py.
"""

import asyncio
import logging
from types import SimpleNamespace

import pytest
from aiohttp import web

import cronstable.cron
from cronstable.config import ConfigError
from cronstable.cron import (
    _WEB_ALL_SCOPES,
    WEB_ANON_REQUEST_KEY,
    WEB_TOKEN_REQUEST_KEY,
    Cron,
    _effective_web_scopes,
    _required_web_scope,
    _WebToken,
)
from tests._configs import DISABLED_JOB as _DISABLED_JOB

# --------------------------------------------------------------------------
# fake request plumbing for the middleware unit tests
# --------------------------------------------------------------------------


class _FakeResource:
    def __init__(self, canonical):
        self.canonical = canonical


class _FakeRoute:
    def __init__(self, canonical):
        self.resource = _FakeResource(canonical) if canonical else None


class _FakeMatchInfo:
    def __init__(self, canonical):
        self.route = _FakeRoute(canonical)


class _ScopedReq:
    """A minimal stand-in for aiohttp's Request carrying just what the auth
    middleware and _required_web_scope read: path, method, headers, query,
    a match_info whose route resource has a canonical path, and the
    mapping surface the middleware files the matched token into."""

    def __init__(
        self,
        path,
        method="GET",
        canonical="__self__",
        headers=None,
        query=None,
    ):
        self.path = path
        self.method = method
        self.headers = headers or {}
        self.query = query or {}
        self.match_info = _FakeMatchInfo(
            path if canonical == "__self__" else canonical
        )
        self.store = {}

    def __setitem__(self, key, value):
        self.store[key] = value

    def get(self, key, default=None):
        return self.store.get(key, default)


async def _run(middleware, request):
    async def handler(_request):
        return "ok"

    return await middleware(request, handler)


def _bearer(tok):
    return {"Authorization": "Bearer " + tok}


def _table(*entries):
    """Build a token table from (secret, scopes, label) triples, expanding
    implied scopes exactly as _resolve_web_tokens does."""
    return [
        _WebToken(secret.encode("utf-8"), _effective_web_scopes(scopes), label)
        for secret, scopes, label in entries
    ]


# --------------------------------------------------------------------------
# the scope model: _effective_web_scopes / _required_web_scope
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("scopes", "expected"),
    [
        pytest.param(["view"], frozenset({"view"}), id="view-only"),
        pytest.param(
            ["control"],
            frozenset({"control", "view"}),
            id="control-implies-view",
        ),
        pytest.param(
            ["approve"],
            frozenset({"approve", "view"}),
            id="approve-implies-view",
        ),
    ],
)
def test_effective_scopes(scopes, expected):
    assert _effective_web_scopes(scopes) == expected


def test_effective_scopes_approve_does_not_imply_control():
    assert "control" not in _effective_web_scopes(["approve"])


def test_required_scope_get_is_view():
    assert _required_web_scope(_ScopedReq("/status")) == "view"


@pytest.mark.parametrize(
    ("path", "canonical", "expected"),
    [
        pytest.param(
            "/jobs/x/start",
            "/jobs/{name}/start",
            "control",
            id="start-post-is-control",
        ),
        pytest.param(
            "/dags/d/runs/r/tasks/t/decision",
            "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
            "approve",
            id="decision-post-is-approve",
        ),
        pytest.param("/mcp", "/mcp", "control", id="mcp-post-is-control"),
    ],
)
def test_required_scope_post_routes(path, canonical, expected):
    req = _ScopedReq(path, method="POST", canonical=canonical)
    assert _required_web_scope(req) == expected


def test_required_scope_unmatched_route_falls_back_to_method():
    # a 404-bound request has no resource; still classified by method.
    assert _required_web_scope(_ScopedReq("/nope", canonical=None)) == "view"


# --------------------------------------------------------------------------
# middleware: 401 vs 403 and the scope matrix
# --------------------------------------------------------------------------


async def test_unknown_token_is_401():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/status", headers=_bearer("nope")))


async def test_view_token_allowed_on_get():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    assert (
        await _run(mw, _ScopedReq("/status", headers=_bearer("viewtok")))
        == "ok"
    )


async def test_control_token_allowed_on_get_via_implied_view():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ci")))
    assert (
        await _run(mw, _ScopedReq("/status", headers=_bearer("ctl"))) == "ok"
    )


@pytest.mark.parametrize(
    ("token", "scopes", "label", "path", "canonical"),
    [
        pytest.param(
            "viewtok",
            ["view"],
            "phone",
            "/jobs/x/start",
            "/jobs/{name}/start",
            id="view-token-on-control-post",
        ),
        pytest.param(
            "ctl",
            ["control"],
            "ci",
            "/dags/d/runs/r/tasks/t/decision",
            "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
            id="control-token-on-approve-route",
        ),
        # approve implies view but NOT control.
        pytest.param(
            "apr",
            ["approve"],
            "oncall",
            "/jobs/x/start",
            "/jobs/{name}/start",
            id="approve-token-on-control-post",
        ),
        pytest.param(
            "viewtok",
            ["view"],
            "phone",
            "/mcp",
            "/mcp",
            id="view-token-on-mcp",
        ),
    ],
)
async def test_insufficient_scope_post_is_403(
    token, scopes, label, path, canonical
):
    mw = Cron._make_auth_middleware(_table((token, scopes, label)))
    req = _ScopedReq(
        path, method="POST", canonical=canonical, headers=_bearer(token)
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, req)


@pytest.mark.parametrize(
    ("token", "scopes", "label", "path", "canonical"),
    [
        pytest.param(
            "ctl",
            ["control"],
            "ci",
            "/jobs/x/start",
            "/jobs/{name}/start",
            id="control-token-on-control-post",
        ),
        pytest.param(
            "apr",
            ["approve"],
            "oncall",
            "/dags/d/runs/r/tasks/t/decision",
            "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
            id="approve-token-on-approve-route",
        ),
        pytest.param(
            "ctl", ["control"], "ci", "/mcp", "/mcp", id="control-token-on-mcp"
        ),
    ],
)
async def test_matching_scope_post_reaches_the_handler(
    token, scopes, label, path, canonical
):
    mw = Cron._make_auth_middleware(_table((token, scopes, label)))
    req = _ScopedReq(
        path, method="POST", canonical=canonical, headers=_bearer(token)
    )
    assert await _run(mw, req) == "ok"


async def test_scoped_view_token_on_ics_query():
    # a view (or control, via implication) token may ride ?token= on .ics.
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    assert (
        await _run(mw, _ScopedReq("/calendar.ics", query={"token": "viewtok"}))
        == "ok"
    )


async def test_scoped_wrong_token_on_ics_query_is_401():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/calendar.ics", query={"token": "wrong"}))


async def test_multiple_tokens_each_match_own_scope():
    mw = Cron._make_auth_middleware(
        _table(("viewtok", ["view"], "phone"), ("ctl", ["control"], "ci"))
    )
    # the view token still cannot control...
    start = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("viewtok"),
    )
    with pytest.raises(web.HTTPForbidden):
        await _run(mw, start)
    # ...but the control token can, on the very same middleware.
    start_ctl = _ScopedReq(
        "/jobs/x/start",
        method="POST",
        canonical="/jobs/{name}/start",
        headers=_bearer("ctl"),
    )
    assert await _run(mw, start_ctl) == "ok"


async def test_scalar_string_token_is_full_scope():
    # backward-compat: a bare string is an all-scopes token and clears every
    # route without a per-route scope lookup.
    mw = Cron._make_auth_middleware("god")
    req = _ScopedReq(
        "/dags/d/runs/r/tasks/t/decision",
        method="POST",
        canonical="/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
        headers=_bearer("god"),
    )
    assert await _run(mw, req) == "ok"


# --------------------------------------------------------------------------
# _resolve_web_tokens: config -> table, fail-closed
# --------------------------------------------------------------------------


def test_resolve_tokens_none_when_unconfigured():
    assert Cron._resolve_web_tokens({"listen": []}) is None


def test_resolve_tokens_scalar_is_full_scope():
    table = Cron._resolve_web_tokens({"authToken": {"value": "god"}})
    assert table is not None and len(table) == 1
    assert table[0].scopes == _WEB_ALL_SCOPES
    assert table[0].label == "authToken"
    assert table[0].token_bytes == b"god"


def test_resolve_tokens_scoped_entry():
    table = Cron._resolve_web_tokens(
        {"authTokens": [{"value": "t", "scopes": ["control"], "label": "ci"}]}
    )
    assert table is not None and len(table) == 1
    assert table[0].scopes == frozenset({"control", "view"})
    assert table[0].label == "ci"


def test_resolve_tokens_default_label():
    table = Cron._resolve_web_tokens(
        {"authTokens": [{"value": "t", "scopes": ["view"]}]}
    )
    assert table is not None
    assert table[0].label == "web.authTokens[0]"


def test_resolve_tokens_scalar_and_scoped_combine():
    table = Cron._resolve_web_tokens(
        {
            "authToken": {"value": "god"},
            "authTokens": [{"value": "t", "scopes": ["view"], "label": "ph"}],
        }
    )
    assert table is not None and len(table) == 2
    assert table[0].label == "authToken"
    assert table[1].label == "ph"


def test_resolve_tokens_empty_source_fails_closed():
    # a scoped entry that names no resolvable source must refuse to start,
    # never silently mint an empty (match-anything-empty) token.
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {"authTokens": [{"scopes": ["view"], "label": "broken"}]}
        )


def test_resolve_tokens_explicit_empty_value_fails_closed():
    # `value: ""` rides the same fail-closed branch as a missing source.
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {"authTokens": [{"value": "", "scopes": ["view"], "label": "e"}]}
        )


def test_resolve_tokens_scoped_duplicate_of_scalar_refused():
    # matching is by secret: a scoped entry repeating the scalar authToken
    # would silently downgrade the all-scopes token to the entry's scopes
    # (the middleware's no-early-return loop keeps the LAST match). Refused
    # at resolve time instead; the error names labels, never the secret.
    with pytest.raises(ConfigError) as exc:
        Cron._resolve_web_tokens(
            {
                "authToken": {"value": "sekrit-dup-9x"},
                "authTokens": [
                    {
                        "value": "sekrit-dup-9x",
                        "scopes": ["view"],
                        "label": "wall",
                    }
                ],
            }
        )
    assert "'wall'" in str(exc.value)
    assert "'authToken'" in str(exc.value)
    assert "sekrit-dup-9x" not in str(exc.value)


def test_resolve_tokens_two_scoped_duplicates_refused():
    with pytest.raises(ConfigError):
        Cron._resolve_web_tokens(
            {
                "authTokens": [
                    {"value": "s", "scopes": ["view"], "label": "a"},
                    {"value": "s", "scopes": ["control"], "label": "b"},
                ]
            }
        )


# --------------------------------------------------------------------------
# web.anonymousScopes: credential-less requests hold `view` and nothing more
# --------------------------------------------------------------------------

_ANON_VIEW = frozenset({"view"})


def _anon_mw(scopes=_ANON_VIEW):
    return Cron._make_auth_middleware(
        _table(("viewtok", ["view"], "phone"), ("ctltok", ["control"], "ci")),
        anonymous_scopes=scopes,
    )


async def test_anonymous_view_serves_a_credential_less_get():
    req = _ScopedReq("/jobs")
    assert await _run(_anon_mw(), req) == "ok"
    # the granted scopes are filed under their own key: handlers that key on
    # the token key's absence (the /shutdown refusal, the pairing audit)
    # must keep seeing an anonymous caller as unauthenticated.
    assert req.get(WEB_ANON_REQUEST_KEY) == _ANON_VIEW
    assert req.get(WEB_TOKEN_REQUEST_KEY) is None


@pytest.mark.parametrize(
    ("path", "canonical", "method", "required"),
    [
        pytest.param(
            "/jobs/x/start",
            "/jobs/{name}/start",
            "POST",
            "control",
            id="start-needs-control",
        ),
        pytest.param(
            "/dags/d/runs/r/tasks/t/decision",
            "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
            "POST",
            "approve",
            id="decision-needs-approve",
        ),
        pytest.param("/mcp", "/mcp", "POST", "control", id="mcp-post"),
        pytest.param("/mcp", "/mcp", "GET", "control", id="mcp-get"),
    ],
)
async def test_anonymous_view_refuses_mutating_routes(
    path, canonical, method, required
):
    with pytest.raises(web.HTTPForbidden) as exc:
        await _run(
            _anon_mw(), _ScopedReq(path, method=method, canonical=canonical)
        )
    # reasoned, unlike the 401: the caller presented nothing, so naming the
    # missing scope leaks nothing about any configured token.
    assert required in str(exc.value.text)


async def test_anonymous_view_excludes_the_device_registry():
    # the registry names every paired phone; a token-holding viewer may read
    # it, a stranger may not.
    with pytest.raises(web.HTTPForbidden) as exc:
        await _run(
            _anon_mw(), _ScopedReq("/push/devices", canonical="/push/devices")
        )
    assert "device registry" in str(exc.value.text)


async def test_anonymous_view_does_not_rescue_a_wrong_token():
    # a typo'd or revoked token must 401 rather than silently degrading to
    # the anonymous grant: 401 keeps meaning "you presented credentials and
    # they are wrong".
    with pytest.raises(web.HTTPUnauthorized):
        await _run(_anon_mw(), _ScopedReq("/jobs", headers=_bearer("nope")))


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({"Authorization": "Basic Zm9vOmJhcg=="}, id="basic"),
        pytest.param({"Authorization": "Bearer"}, id="bearer-no-value"),
        pytest.param({"Authorization": "Bearer "}, id="bearer-empty"),
    ],
)
async def test_anonymous_view_does_not_rescue_a_malformed_header(headers):
    with pytest.raises(web.HTTPUnauthorized):
        await _run(_anon_mw(), _ScopedReq("/jobs", headers=headers))


async def test_anonymous_view_leaves_the_token_path_intact():
    # the companion app's baked view token keeps authenticating normally.
    req = _ScopedReq("/jobs", headers=_bearer("viewtok"))
    assert await _run(_anon_mw(), req) == "ok"
    assert req.get(WEB_TOKEN_REQUEST_KEY).label == "phone"
    assert req.get(WEB_ANON_REQUEST_KEY) is None
    # and a control token still controls
    assert (
        await _run(
            _anon_mw(),
            _ScopedReq(
                "/jobs/x/start",
                method="POST",
                canonical="/jobs/{name}/start",
                headers=_bearer("ctltok"),
            ),
        )
        == "ok"
    )


async def test_anonymous_view_serves_a_bare_calendar_feed():
    assert await _run(_anon_mw(), _ScopedReq("/calendar.ics")) == "ok"


async def test_anonymous_view_still_401s_a_wrong_calendar_query_token():
    # the ?token= carve-out is a presented credential like any other.
    with pytest.raises(web.HTTPUnauthorized):
        await _run(
            _anon_mw(), _ScopedReq("/calendar.ics", query={"token": "nope"})
        )


async def test_anonymous_view_treats_a_query_token_as_presented_anywhere():
    # `?token=` authenticates only the calendar feeds, but it counts as a
    # presented credential on every path: a caller whose stale token is
    # refused elsewhere must not find this request quietly served instead.
    with pytest.raises(web.HTTPUnauthorized):
        await _run(_anon_mw(), _ScopedReq("/jobs", query={"token": "nope"}))
    # ...including one that would otherwise match a real token, since the
    # carve-out is scoped to the calendar routes by path.
    with pytest.raises(web.HTTPUnauthorized):
        await _run(_anon_mw(), _ScopedReq("/jobs", query={"token": "viewtok"}))


async def test_anonymous_view_unmatched_route_reaches_routing():
    # a 404-bound GET is view by method default, so it reaches the router
    # and 404s instead of 401ing.
    assert await _run(_anon_mw(), _ScopedReq("/nope", canonical=None)) == "ok"


async def test_no_anonymous_scopes_keeps_todays_401():
    mw = Cron._make_auth_middleware(_table(("viewtok", ["view"], "phone")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/jobs"))


# --------------------------------------------------------------------------
# end-to-end over a real aiohttp app
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scoped_tokens_end_to_end():
    import aiohttp

    from cronstable.config import _build_mcp_config

    cron = cronstable.cron.Cron(None, config_yaml=_DISABLED_JOB)
    web_config = {
        "listen": ["http://127.0.0.1:0"],
        "authTokens": [
            {"value": "viewtok", "scopes": ["view"], "label": "phone"},
            {"value": "ctltok", "scopes": ["control"], "label": "ci"},
            {"value": "apprtok", "scopes": ["approve"], "label": "lead"},
        ],
    }
    await cron.start_stop_web_app(
        web_config, _build_mcp_config({"enabled": True})
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # no token -> 401
            async with session.get(base + "/status") as resp:
                assert resp.status == 401
            # view token reads
            async with session.get(
                base + "/status", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 200
            # view token cannot control -> 403 (Origin-less POST passes the
            # CSRF gate, so this isolates the scope check)
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 403
            # control token controls -> reaches the handler (409: disabled)
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 409
            # control implies view
            async with session.get(
                base + "/status", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 200
            # the /mcp override binds on the real registered route, on every
            # method: view is refused, control reaches the handler (whose GET
            # answer is 405: the stateless transport has no SSE stream).
            async with session.get(
                base + "/mcp", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 403
            async with session.get(
                base + "/mcp", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 405
            # the decision route's `approve` override, on the real route:
            # approve passes the scope gate and reaches the handler (409, no
            # such run); control and view are 403 despite outranking approve
            # elsewhere; approve does not leak into control routes but does
            # imply view.
            decision = base + "/dags/d/runs/r/tasks/t/decision"
            body = {"decision": "approve"}
            async with session.post(
                decision, json=body, headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 409
            for tok in ("ctltok", "viewtok"):
                async with session.post(
                    decision, json=body, headers=_bearer(tok)
                ) as resp:
                    assert resp.status == 403, tok
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 403
            async with session.get(
                base + "/status", headers=_bearer("apprtok")
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_anonymous_view_end_to_end(caplog):
    """A public-view instance over real HTTP: strangers read, only tokens
    mutate, and the startup log says so."""
    import aiohttp

    from cronstable.config import _build_mcp_config

    cron = cronstable.cron.Cron(None, config_yaml=_DISABLED_JOB)
    web_config = {
        "listen": ["http://127.0.0.1:0"],
        "authTokens": [
            {"value": "viewtok", "scopes": ["view"], "label": "phone"},
            {"value": "ctltok", "scopes": ["control"], "label": "ci"},
        ],
        "anonymousScopes": ["view"],
    }
    with caplog.at_level(logging.INFO, logger="cronstable"):
        await cron.start_stop_web_app(
            web_config, _build_mcp_config({"enabled": True})
        )
    assert "anonymous requests granted scopes: view" in caplog.text
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # a stranger reads
            async with session.get(base + "/status") as resp:
                assert resp.status == 200
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 200
            # ...and is told exactly what it holds
            async with session.get(base + "/whoami") as resp:
                assert resp.status == 200
                assert await resp.json() == {
                    "authenticated": False,
                    "label": "anonymous",
                    "scopes": ["view"],
                    "allScopes": False,
                }
            # ...but cannot act, on any of the three mutating gates
            async with session.post(base + "/jobs/test/start") as resp:
                assert resp.status == 403
            async with session.post(
                base + "/dags/d/runs/r/tasks/t/decision",
                json={"decision": "approve"},
            ) as resp:
                assert resp.status == 403
            async with session.get(base + "/mcp") as resp:
                assert resp.status == 403
            # ...nor read the device registry
            async with session.get(base + "/push/devices") as resp:
                assert resp.status == 403
            # ...nor stop the daemon (control-gated first, and the handler
            # refuses an unauthenticated caller besides)
            async with session.post(base + "/shutdown") as resp:
                assert resp.status == 403
            # a wrong token is still wrong
            async with session.get(
                base + "/status", headers=_bearer("bogus")
            ) as resp:
                assert resp.status == 401
            # tokens behave exactly as they do without the anonymous grant
            async with session.get(
                base + "/whoami", headers=_bearer("viewtok")
            ) as resp:
                body = await resp.json()
                assert body["authenticated"] is True
                assert body["label"] == "phone"
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("ctltok")
            ) as resp:
                assert resp.status == 409
            async with session.post(
                base + "/jobs/test/start", headers=_bearer("viewtok")
            ) as resp:
                assert resp.status == 403
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)


# --------------------------------------------------------------------------
# the .ics carve-out puts the token in the URL, so it must stay out of the
# access log (cron._access_log_class)
# --------------------------------------------------------------------------


def test_redact_query_token():
    redact = cronstable.cron._redact_query_token
    assert redact("/calendar.ics?token=s3cr3t") == "/calendar.ics?token=***"
    assert redact("/jobs/j/calendar.ics?token=s3cr3t&alarm=10") == (
        "/jobs/j/calendar.ics?token=***&alarm=10"
    )
    assert redact("/calendar.ics?alarm=10&token=s3cr3t") == (
        "/calendar.ics?alarm=10&token=***"
    )
    # nothing to redact: left byte for byte alone
    assert redact("/status") == "/status"
    assert redact("/status?x=1") == "/status?x=1"
    # a bare `token` with no '=' is not a value, and a key that merely ends
    # in "token" is a different parameter
    assert redact("/calendar.ics?token") == "/calendar.ics?token"
    assert redact("/x?pushToken=abc") == "/x?pushToken=abc"


class _LoggedRequest:
    """The slice of aiohttp's Request its default access log renders."""

    def __init__(self, path_qs):
        self.method = "GET"
        self.path_qs = path_qs
        self.version = _Version()
        self.remote = "127.0.0.1"
        self.headers = {"Referer": "-", "User-Agent": "cal/1.0"}


class _Version:
    major = 1
    minor = 1


def test_web_access_log_redacts_calendar_token(caplog):
    # aiohttp renders request.path_qs into the access line at INFO, and the
    # dashboard mints /calendar.ics?token=<web bearer token> for operators
    # to subscribe with, so an unredacted line writes a live all-scopes
    # credential to the log on every poll.  Asserts on the EMITTED line
    # rather than on the override, so an aiohttp upgrade that renames the
    # %r hook fails here instead of silently restoring the leak.
    logger_cls = cronstable.cron._access_log_class()
    logger = logging.getLogger("test.access")
    access = logger_cls(logger, logger_cls.LOG_FORMAT)
    response = SimpleNamespace(status=200, body_length=183, headers={})
    with caplog.at_level(logging.INFO, logger="test.access"):
        access.log(
            _LoggedRequest("/calendar.ics?token=s3cr3tTOKEN"), response, 0.01
        )
        access.log(_LoggedRequest("/status"), response, 0.01)
    assert "s3cr3tTOKEN" not in caplog.text
    assert "GET /calendar.ics?token=*** HTTP/1.1" in caplog.text
    # an ordinary request line is untouched
    assert "GET /status HTTP/1.1" in caplog.text


@pytest.mark.asyncio
async def test_web_access_log_redaction_is_installed_on_the_runner(caplog):
    # Pins the one line that installs _access_log_class on the runner
    # (Cron.start_stop_web_app).  The unit test above proves the class
    # redacts; only a real request through the real runner proves the
    # daemon uses it, and deleting the access_log_class argument leaves
    # every other test in the suite green.  This also exercises
    # aiohttp's own RequestHandler.log_access rather than calling log()
    # by hand.
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_DISABLED_JOB)
    web_config = {
        "listen": ["http://127.0.0.1:0"],
        "authTokens": [
            {"value": "s3cr3tTOKEN", "scopes": ["view"], "label": "cal"},
        ],
    }
    await cron.start_stop_web_app(web_config)
    try:
        port = cron.web_runner.addresses[0][1]
        url = "http://127.0.0.1:{}/jobs/test/calendar.ics?token={}".format(
            port, "s3cr3tTOKEN"
        )
        with caplog.at_level(logging.INFO, logger="aiohttp.access"):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    assert resp.status == 200
                    await resp.read()
            # aiohttp emits the access line after the body is written;
            # the same settle the teardown above uses.
            await asyncio.sleep(0.25)
        lines = [
            r.getMessage()
            for r in caplog.records
            if r.name == "aiohttp.access"
        ]
        assert lines, "no access log line captured"
        assert "s3cr3tTOKEN" not in "\n".join(lines), lines
        assert any("calendar.ics?token=***" in ln for ln in lines), lines
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)


# --------------------------------------------------------------------------
# the CORS-preflight carve-out: a credential-less preflight passes the gate
# (the Fetch standard strips credentials from preflights, and the /mcp
# OPTIONS route enforces mcp.allowedOrigins itself); a bare OPTIONS without
# the defining header stays 401.
# --------------------------------------------------------------------------


async def test_cors_preflight_passes_the_bearer_gate():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ctl")))
    req = _ScopedReq(
        "/mcp",
        method="OPTIONS",
        headers={
            "Origin": "https://inspector.example",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert await _run(mw, req) == "ok"


async def test_bare_options_without_preflight_header_is_401():
    mw = Cron._make_auth_middleware(_table(("ctl", ["control"], "ctl")))
    with pytest.raises(web.HTTPUnauthorized):
        await _run(mw, _ScopedReq("/mcp", method="OPTIONS"))
