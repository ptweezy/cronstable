import asyncio
import datetime
import inspect
import threading
from pathlib import Path

import pytest

import cronstable.cron
from cronstable.job import JobOutputStream
from tests._configs import DISABLED_JOB
from tests._cron_helpers import (
    _SUBMINUTE_NOFIRE,
    _WEB_ONE_JOB,
    CONCURRENT_JOB,
    DT,
    JOB_THAT_SUCCEEDS,
    TWO_JOBS,
    UTC,
    _FakeMesh,
    fixed_current_time,  # noqa: F401
)
from tests._helpers import (
    _drain_state_writes,
    _state_cfg,
    _wait_until,
    bare_http_raises,
)
from tests.conftest import Req, _cron


@pytest.fixture
async def start_web_app():
    """Start a cron's real web app with teardown guaranteed (finding B1:
    the start_stop_web_app try/finally idiom).

    ``await start_web_app(cron, config, *mcp)`` forwards to
    cron.start_stop_web_app and registers the cron.  Teardown clears every
    registered app (idempotent, so a test that already cleared its own is
    unaffected), then lets the Proactor's connection transports finish
    closing before the loop is torn down (the aiohttp-documented Windows
    grace period); otherwise their GC-time repr can raise "I/O operation
    on closed pipe" as a PytestUnraisableExceptionWarning.
    """
    crons = []

    async def start(cron, config, *mcp):
        crons.append(cron)
        await cron.start_stop_web_app(config, *mcp)

    yield start
    for cron in reversed(crons):
        await cron.start_stop_web_app(None)
    await asyncio.sleep(0.25)


@pytest.fixture
async def run_cron():
    """Drive cron.run() as a background task with a guaranteed graceful
    stop (finding B1: the signal_shutdown try/finally idiom).  Teardown
    signals shutdown and drains the task with the same 5s bound the
    try/finally sites used; a test that already stopped its own cron is
    unaffected (signal_shutdown is idempotent and the task already done).
    """
    running = []

    def start(cron):
        task = asyncio.create_task(cron.run())
        running.append((cron, task))
        return task

    yield start
    for cron, task in reversed(running):
        cron.signal_shutdown()
        await asyncio.wait_for(task, timeout=5)


def test_resolve_web_token_value():
    auth = {"value": "secret", "fromFile": None, "fromEnvVar": None}
    token = cronstable.cron.Cron._resolve_web_token({"authToken": auth})
    assert token == "secret"


def test_resolve_web_token_envvar(monkeypatch):
    monkeypatch.setenv("CRONSTABLE_TEST_WEB_TOKEN", "envsecret")
    token = cronstable.cron.Cron._resolve_web_token(
        {
            "authToken": {
                "value": None,
                "fromFile": None,
                "fromEnvVar": "CRONSTABLE_TEST_WEB_TOKEN",
            }
        }
    )
    assert token == "envsecret"


def test_resolve_web_token_absent():
    assert cronstable.cron.Cron._resolve_web_token({"listen": []}) is None


def test_resolve_web_token_missing_envvar_fails_closed(monkeypatch):
    # authToken configured but the env var is unset: must raise rather than
    # silently leaving the web API unauthenticated.
    monkeypatch.delenv("CRONSTABLE_TEST_WEB_TOKEN", raising=False)
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": None,
                    "fromEnvVar": "CRONSTABLE_TEST_WEB_TOKEN",
                }
            }
        )


def test_resolve_web_token_empty_value_fails_closed():
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": None,
                    "fromEnvVar": None,
                }
            }
        )


def test_resolve_web_token_empty_file_fails_closed(tmp_path):
    empty = tmp_path / "token"
    empty.write_text("   \n")
    with pytest.raises(cronstable.config.ConfigError):
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": str(empty),
                    "fromEnvVar": None,
                }
            }
        )


def test_resolve_web_token_file_tolerates_a_bom(tmp_path):
    # The failure this closes is silent at both ends.  A BOM is not
    # whitespace, so .strip() leaves it on the front of the token; the
    # non-empty check above therefore passes, the daemon starts
    # authenticated, and every request carrying the token the operator
    # actually wrote comes back 401 with nothing in the log to say why.
    # Notepad's "UTF-8 with BOM" and a PowerShell redirect both write one,
    # and wiki/Running-on-Windows.md documents exactly this file as the
    # unattended POST /shutdown stop path.
    token = tmp_path / "token"
    token.write_bytes("\ufeffhunter2\n".encode("utf-8"))
    assert (
        cronstable.cron.Cron._resolve_web_token(
            {
                "authToken": {
                    "value": None,
                    "fromFile": str(token),
                    "fromEnvVar": None,
                }
            }
        )
        == "hunter2"
    )


@pytest.mark.asyncio
async def test_auth_middleware():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_auth_middleware("secret")

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, auth):
            self.headers = {"Authorization": auth} if auth else {}
            # the middleware consults these on non-bearer requests (the
            # .ics query-token carve-out, the preflight carve-out's method
            # dispatch); a real request always has them
            self.method = "GET"
            self.path = "/jobs"
            self.query = {}
            # the middleware files the matched token on the request
            self.store = {}

        def __setitem__(self, key, value):
            self.store[key] = value

    resp = await middleware(FakeRequest("Bearer secret"), handler)
    assert resp.text == "ok"

    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer wrong"), handler)
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest(None), handler)
    # a non-ASCII token must be a clean 401: compare_digest raises TypeError
    # (-> 500 + traceback) on any non-ASCII str operand.
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer café"), handler)
    # surrogates (raw header bytes that never decoded) cannot even encode --
    # and can never match a real token; still a 401, not a 500.
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("Bearer t\udc80k"), handler)


def test_origin_matches_host():
    m = cronstable.cron._origin_matches_host
    assert m("http://localhost:8021", "localhost:8021")
    # default ports: a browser omits them in BOTH headers of a same-origin
    # request, so both sides normalize from the Origin's scheme
    assert m("https://cron.example.com", "cron.example.com")
    assert m("http://cron.example.com", "cron.example.com")
    # hostname case-insensitivity (urlparse lowercases both sides)
    assert m("HTTP://LOCALHOST:8021", "LocalHost:8021")
    # bracketed IPv6 authority
    assert m("http://[::1]:8021", "[::1]:8021")
    # scheme deliberately ignored: a TLS-terminating proxy shows the daemon
    # plain http while the browser's Origin says https
    assert m("https://localhost:8021", "localhost:8021")
    assert not m("http://localhost:9999", "localhost:8021")
    assert not m("http://evil.example", "localhost:8021")
    # "null" (sandboxed iframe / redirect chain) and garbage fail closed
    assert not m("null", "localhost:8021")
    assert not m("garbage", "localhost:8021")
    assert not m("http://localhost:8021", None)
    assert not m("http://localhost:8021", "")
    # a malformed port on either side can never match (urlparse defers the
    # ValueError to .port; the helper must swallow it, not 500)
    assert not m("http://localhost:notaport", "localhost:8021")
    assert not m("http://localhost:8021", "localhost:notaport")


@pytest.mark.asyncio
async def test_origin_middleware_blocks_cross_site_mutations():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_origin_middleware(
        frozenset({"https://dash.example"})
    )

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(
            self,
            method="POST",
            origin=None,
            host="localhost:8021",
            path="/jobs/x/start",
        ):
            self.method = method
            self.headers = {} if origin is None else {"Origin": origin}
            self.host = host
            self.path = path

    # non-browser clients (curl, monitoring) send no Origin: unaffected
    resp = await middleware(FakeRequest(), handler)
    assert resp.text == "ok"
    # the served dashboard: same-origin POST passes
    resp = await middleware(
        FakeRequest(origin="http://localhost:8021"), handler
    )
    assert resp.text == "ok"
    # operator-trusted extra origin (web.allowedOrigins) passes
    resp = await middleware(
        FakeRequest(origin="https://dash.example"), handler
    )
    assert resp.text == "ok"
    # any other page the operator happens to visit: refused before the
    # handler runs -- the CSRF this middleware exists to stop
    with pytest.raises(web.HTTPForbidden):
        await middleware(FakeRequest(origin="https://evil.example"), handler)
    # "null" Origin fails closed
    with pytest.raises(web.HTTPForbidden):
        await middleware(FakeRequest(origin="null"), handler)
    # safe methods pass untouched (reads mutate nothing; the browser's
    # same-origin policy already hides their responses cross-site)
    resp = await middleware(
        FakeRequest(method="GET", origin="https://evil.example"), handler
    )
    assert resp.text == "ok"
    # /mcp enforces its own allow-list (mcp.allowedOrigins) and must stay
    # reachable for origins allow-listed THERE: exempt from this gate
    resp = await middleware(
        FakeRequest(origin="https://evil.example", path="/mcp"), handler
    )
    assert resp.text == "ok"


@pytest.mark.asyncio
async def test_web_app_origin_gate_end_to_end(start_web_app):
    # the gate is wired into the real app even with NO authToken configured
    # (the default posture the CSRF gate exists for): a cross-site POST is
    # refused before the handler, while same-origin and Origin-less requests
    # reach it (409 -- the job is disabled -- proves the handler answered).
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=DISABLED_JOB)
    await start_web_app(cron, {"listen": ["http://127.0.0.1:0"]})
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    async with aiohttp.ClientSession() as session:
        async with session.post(
            base + "/jobs/test/start",
            headers={"Origin": "https://evil.example"},
        ) as resp:
            assert resp.status == 403
        async with session.post(
            base + "/jobs/test/start", headers={"Origin": base}
        ) as resp:
            assert resp.status == 409
        async with session.post(base + "/jobs/test/start") as resp:
            assert resp.status == 409


def test_web_site_from_url_bad_scheme():
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "ftp://localhost:1234")


def test_web_site_from_url_malformed_http():
    # missing host/port must raise ValueError (a skippable bad entry), not
    # AssertionError (which would be reported as an internal cronstable bug).
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "http://")


@pytest.mark.asyncio
async def test_start_web_app_ignores_bad_listen_urls(start_web_app):
    # an unusable listen url is skipped, not surfaced as an exception
    cron = cronstable.cron.Cron(None)
    bad_config = {"listen": ["ftp://localhost:1234", "http://"]}
    await start_web_app(cron, bad_config)  # must not raise


@pytest.mark.asyncio
async def test_web_start_disabled_job_refused():
    from aiohttp import web

    cron = _cron(DISABLED_JOB)
    with pytest.raises(web.HTTPConflict):
        await cron._web_start_job(Req(match={"name": "test"}))
    # the disabled job must not have been launched
    assert not cron.running_jobs


@pytest.mark.asyncio
async def test_web_status_reports_disabled():
    import json

    cron = _cron(DISABLED_JOB)
    resp = await cron._web_get_status(
        Req(headers={"Accept": "application/json"})
    )
    data = json.loads(resp.text)
    assert data[0]["status"] == "disabled"


@pytest.mark.asyncio
async def test_web_status_and_job_set_id_accept_negotiation():
    # A generated client sends compound Accept headers ("application/json,
    # */*", ";q=" parameters); any explicit application/json range selects
    # JSON. Wildcards deliberately do NOT: curl's default `*/*` must keep
    # the classic text form every existing script parses.
    import json

    cron = _cron(DISABLED_JOB)

    def req(accept):
        return Req(headers={} if accept is None else {"Accept": accept})

    for accept in (
        "application/json",
        "application/json, */*",
        "text/html, application/json;q=0.9",
        "APPLICATION/JSON; charset=utf-8",
    ):
        resp = await cron._web_get_status(req(accept))
        assert json.loads(resp.text)[0]["status"] == "disabled", accept
    for accept in (None, "*/*", "application/*", "text/plain"):
        resp = await cron._web_get_status(req(accept))
        assert resp.content_type == "text/plain", accept

    jresp = await cron._web_job_set_id(req("application/json, */*"))
    assert "job_set_id" in json.loads(jresp.text)
    tresp = await cron._web_job_set_id(req("*/*"))
    assert tresp.content_type == "text/plain"


@pytest.mark.asyncio
async def test_web_list_jobs():
    import json

    cron = _cron(TWO_JOBS)
    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    assert [j["name"] for j in data] == ["alpha", "beta"]
    assert resp.headers.get("ETag")  # content ETag present for caches

    alpha = data[0]
    assert alpha["enabled"] is True
    assert alpha["schedule"] == "*/5 * * * *"
    assert alpha["command"] == "echo alpha"
    assert alpha["captureStdout"] is True
    assert alpha["running"] is False
    assert alpha["scheduled_in"] is not None  # next run computed
    assert alpha["last_run"] is None  # never run yet

    beta = data[1]
    assert beta["enabled"] is False
    assert beta["command"] == "echo beta"  # argv list joined for display
    assert beta["scheduled_in"] is None  # disabled -> no next run


@pytest.mark.asyncio
async def test_web_list_jobs_etag_304_and_invalidation():
    """GET /jobs serves a content ETag, 304s a matching conditional poll,
    keeps the tag stable while only the countdown moves, and moves it when
    job state changes."""
    cron = _cron(TWO_JOBS)

    def req(inm=None):
        return Req(headers={} if inm is None else {"If-None-Match": inm})

    first = await cron._web_list_jobs(req())
    etag = first.headers["ETag"]
    assert first.status == 200 and etag

    # a second poll with no change re-serves 200 with the SAME tag: the
    # relative countdown is not part of it (it is derived from the absolute
    # next-fire), so an idle poll is byte-identical.
    again = await cron._web_list_jobs(req())
    assert again.status == 200
    assert again.headers["ETag"] == etag

    # a conditional poll carrying that tag is told nothing changed.
    not_modified = await cron._web_list_jobs(req(etag))
    assert not_modified.status == 304
    assert not_modified.body in (None, b"")
    assert not_modified.headers["ETag"] == etag

    # a real state change (advancing a job's next fire) moves the tag, so
    # the same conditional poll now gets a fresh body instead of a 304.
    # The live fire path pairs the advance with a launch, whose bust drops
    # the shared response memo; poking the index directly skips that, so
    # bust it the same way here (a next-fire-only change would otherwise
    # simply age out of the memo within its one-second TTL).
    when = cron._next_fire.get("alpha")
    cron._next_fire["alpha"] = (
        when or DT(2000, 1, 1, tzinfo=UTC)
    ) + datetime.timedelta(hours=1)
    cron._bust_response_memos()
    changed = await cron._web_list_jobs(req(etag))
    assert changed.status == 200
    assert changed.headers["ETag"] != etag


@pytest.mark.asyncio
async def test_web_list_jobs_memo_shares_one_build(monkeypatch):
    # N pollers inside the memo TTL must share ONE payload build (the
    # whole point: a wallboard plus tabs used to cost N identical builds
    # per cycle), and a locally recorded run must bust the memo so the
    # next poll sees it immediately.
    # The TTL is widened so the exact build counts below cannot be broken
    # by a stall between awaits (CPU steal on a loaded runner under
    # --cov inserts an extra build past the real 1.0s TTL).
    monkeypatch.setattr(cronstable.cron, "_JOBS_RESPONSE_TTL", 3600.0)
    cron = _cron(TWO_JOBS)
    builds = []
    real_payload = cron.jobs_payload

    def counting_payload():
        builds.append(1)
        return real_payload()

    monkeypatch.setattr(cron, "jobs_payload", counting_payload)

    first = await cron._web_list_jobs(Req())
    second = await cron._web_list_jobs(Req())
    third = await cron._web_list_jobs(Req())
    assert len(builds) == 1  # one build served all three pollers
    assert second.headers["ETag"] == first.headers["ETag"]
    assert third.body == first.body

    # a recorded run changes the payload: the memo is busted and the next
    # poll rebuilds rather than serving the stale product out to the TTL.
    cron._record_run("alpha", _mk_run("failure"))
    fresh = await cron._web_list_jobs(Req())
    assert len(builds) == 2
    assert fresh.headers["ETag"] != first.headers["ETag"]


@pytest.mark.asyncio
async def test_web_list_jobs_single_flight_shares_one_build(monkeypatch):
    # Concurrent pollers must JOIN one build while it is in flight;
    # sharing a product that already landed in the memo proves nothing.
    # Above the offload threshold the build spans an executor hop, and a
    # plain check-then-store memo let every poller landing inside that
    # await miss too and run its own full build.
    # The build is parked on an Event while the followers arrive (the
    # bust-mid-build sibling's pattern): fired free-running, this tiny
    # payload finishes in the executor before the leader even suspends,
    # so the followers find a warm memo and pass whether or not the
    # join exists.
    monkeypatch.setattr(cronstable.cron, "_JOBS_RESPONSE_TTL", 3600.0)
    monkeypatch.setattr(cronstable.cron, "_JOBS_SERIALIZE_OFFLOAD_MIN", 0)
    cron = _cron(TWO_JOBS)
    builds = []
    release = threading.Event()
    real_product = cronstable.cron._jobs_response_product

    def parked_product(payload, next_fire):
        builds.append(1)
        release.wait(5)
        return real_product(payload, next_fire)

    # the executor call resolves the module global at call time
    monkeypatch.setattr(
        cronstable.cron, "_jobs_response_product", parked_product
    )

    tasks = [asyncio.create_task(cron._web_list_jobs(Req()))]
    try:
        await _wait_until(lambda: builds)
        tasks += [
            asyncio.create_task(cron._web_list_jobs(Req()))
            for _ in range(7)
        ]
        # the state that proves the join: every task suspended (the
        # leader in its executor hop, the seven followers on the inflight
        # future) with still exactly one build recorded. A follower
        # running a build of its own would record it, or complete, and
        # this wait would fail.
        await _wait_until(
            lambda: (
                len(builds) == 1
                and all(
                    inspect.getcoroutinestate(t.get_coro())
                    == inspect.CORO_SUSPENDED
                    for t in tasks
                )
            )
        )
        release.set()
        responses = await asyncio.gather(*tasks)
        assert len(builds) == 1
        first = responses[0]
        assert all(r.status == 200 for r in responses)
        assert all(
            r.headers["ETag"] == first.headers["ETag"] for r in responses
        )
        assert all(r.body == first.body for r in responses)
        assert cron._jobs_response_memo.inflight is None
    finally:
        release.set()
        for t in tasks:
            if not t.done():
                t.cancel()


@pytest.mark.asyncio
async def test_web_list_jobs_bust_mid_build_is_not_stored(monkeypatch):
    # A bust landing while the build is on the executor must not be undone
    # by that pre-bust product being stored on the way out: the leader
    # still serves its own product to its own caller, but the next poll
    # rebuilds instead of inheriting the stale product.
    monkeypatch.setattr(cronstable.cron, "_JOBS_RESPONSE_TTL", 3600.0)
    monkeypatch.setattr(cronstable.cron, "_JOBS_SERIALIZE_OFFLOAD_MIN", 0)
    cron = _cron(TWO_JOBS)
    calls = []
    release = threading.Event()
    real_product = cronstable.cron._jobs_response_product

    def gated_product(payload, next_fire):
        calls.append(1)
        release.wait(5)
        return real_product(payload, next_fire)

    monkeypatch.setattr(
        cronstable.cron, "_jobs_response_product", gated_product
    )

    task = asyncio.create_task(cron._web_list_jobs(Req()))
    try:
        await _wait_until(lambda: calls)
        cron._bust_response_memos()
        release.set()
        resp = await task
        assert resp.status == 200
        assert cron._jobs_response_memo.cached is None
        follow_up = await cron._web_list_jobs(Req())
        assert follow_up.status == 200
        assert len(calls) == 2
    finally:
        release.set()
        if not task.done():
            task.cancel()


# enough tasks that the serialized graph clears the gzip minimum, so the
# compression arm of the /dags conditional response is exercised for real
_FAT_DAG = "dags:\n  - name: d\n    tasks:\n" + "".join(
    "      - id: step-number-%02d\n        command: 'run step %02d'\n"
    % (i, i)
    for i in range(16)
)


def _hdr_req(inm=None, ae=None):
    headers = {}
    if inm is not None:
        headers["If-None-Match"] = inm
    if ae is not None:
        headers["Accept-Encoding"] = ae
    return Req(headers=headers)


@pytest.mark.asyncio
async def test_web_list_dags_etag_304_and_gzip():
    """GET /dags is the third leg of the dashboard's per-poll fan-out: it
    must serve a content ETag, 304 an unchanged conditional poll instead of
    re-shipping every task graph, move the tag when the payload changes, and
    gzip a large body for a client that accepts it."""
    import gzip as gzip_mod
    import json

    cron = _cron(_FAT_DAG)
    first = await cron._web_list_dags(_hdr_req())
    etag = first.headers["ETag"]
    assert first.status == 200 and etag
    assert first.headers["Vary"] == "Accept-Encoding"
    payload = json.loads(first.text)
    assert [d["name"] for d in payload] == ["d"]
    assert len(payload[0]["tasks"]) == 16

    # an unchanged conditional poll costs a 304, no body
    not_modified = await cron._web_list_dags(_hdr_req(inm=etag))
    assert not_modified.status == 304
    assert not_modified.body in (None, b"")
    assert not_modified.headers["ETag"] == etag

    # a gzip-capable client gets the compressed representation of the same
    # bytes (the graph is comfortably past the minimum with 16 tasks)
    packed = await cron._web_list_dags(_hdr_req(ae="gzip"))
    assert packed.status == 200
    assert packed.headers.get("Content-Encoding") == "gzip"
    assert packed.headers["ETag"] == etag
    assert json.loads(gzip_mod.decompress(packed.body)) == payload

    # a payload change moves the tag and the same conditional poll gets 200
    cron.cron_dags["d"].enabled = False
    changed = await cron._web_list_dags(_hdr_req(inm=etag))
    assert changed.status == 200
    assert changed.headers["ETag"] != etag


@pytest.mark.asyncio
async def test_single_caller_response_builds_only_what_it_serves(monkeypatch):
    """The unmemoized path serves ONE caller, so gzipping a body that
    caller will not take is pure waste on the event loop. A 304 carries no
    body, and a client that did not advertise gzip gets none, so neither
    may pay for compression; /cluster never tags, so it must not hash
    either. (The memoized product still builds both eagerly on purpose:
    it is shared across pollers with mixed Accept-Encoding and
    If-None-Match.)"""
    import cronstable.cron

    zipped = []
    real_gzip = cronstable.cron._gzip_body

    def counting_gzip(body):
        zipped.append(len(body))
        return real_gzip(body)

    monkeypatch.setattr(cronstable.cron, "_gzip_body", counting_gzip)

    cron = _cron(_FAT_DAG)
    etag = (await cron._web_list_dags(_hdr_req())).headers["ETag"]
    assert zipped == []  # no Accept-Encoding: nothing to compress

    nm = await cron._web_list_dags(_hdr_req(inm=etag, ae="gzip"))
    assert nm.status == 304
    assert zipped == []  # a 304 has no body to compress

    packed = await cron._web_list_dags(_hdr_req(ae="gzip"))
    assert packed.headers["Content-Encoding"] == "gzip"
    assert len(zipped) == 1  # only the representation actually served

    hashed = []
    real_sha = cronstable.cron.hashlib.sha256

    def counting_sha(data=b""):
        hashed.append(len(data))
        return real_sha(data)

    monkeypatch.setattr(cronstable.cron.hashlib, "sha256", counting_sha)
    await cron._web_get_cluster(_hdr_req(ae="gzip"))
    assert hashed == []  # use_etag=False: the digest was never taken


@pytest.mark.asyncio
async def test_web_get_cluster_negotiates_gzip_but_never_etags():
    """GET /cluster embeds freshly sampled node gauges, so a content tag
    would churn per poll and never match: the handler must not emit one.
    Gzip stays negotiated (with the size floor) and Vary rides along."""
    import json

    cron = _cron(TWO_JOBS)
    resp = await cron._web_get_cluster(_hdr_req(ae="gzip"))
    assert resp.status == 200
    assert resp.headers.get("ETag") is None
    assert resp.headers["Vary"] == "Accept-Encoding"
    # the no-cluster payload is far below the gzip minimum: shipped plain
    assert resp.headers.get("Content-Encoding") is None
    assert json.loads(resp.text) == {"enabled": False, "peers": []}


@pytest.mark.asyncio
async def test_web_job_set_id():
    import json

    cron = _cron(TWO_JOBS)
    resp = await cron._web_job_set_id(Req())
    assert resp.text == cron.job_set_id()
    # the id always carries the live scheme label (see cronstable.fingerprint;
    # the golden-value tests pin the actual version)
    from cronstable.fingerprint import SCHEME_VERSION

    assert resp.text.startswith(SCHEME_VERSION + ":")

    resp = await cron._web_job_set_id(
        Req(headers={"Accept": "application/json"})
    )
    data = json.loads(resp.text)
    assert data["job_set_id"] == cron.job_set_id()
    assert data["jobs"] == 2


def test_job_set_id_logged_only_on_change(caplog):
    import logging

    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    with caplog.at_level(logging.INFO, logger="cronstable"):
        cron._log_job_set_id()
        cron._log_job_set_id()  # unchanged: must not log again
    logged = [r.message for r in caplog.records if "Job set id" in r.message]
    assert len(logged) == 1
    assert cron.job_set_id() in logged[0]


@pytest.mark.asyncio
async def test_web_list_jobs_includes_last_run():
    import json

    cron = _cron(TWO_JOBS)
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="failure",
        exit_code=2,
        started_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        finished_at=DT(1999, 12, 31, 12, 0, 5, tzinfo=UTC),
        fail_reason="failsWhen=nonzeroReturn and retcode=2",
        output=JobOutputStream(),
    )
    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    last = data[0]["last_run"]
    assert last["outcome"] == "failure"
    assert last["exit_code"] == 2
    assert last["duration"] == 5.0
    assert last["fail_reason"].startswith("failsWhen")


def _mk_run(outcome, exit_code=0, dur=1.0):
    start = DT(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    return cronstable.cron.JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=start,
        finished_at=start + datetime.timedelta(seconds=dur),
        fail_reason=None if outcome == "success" else "boom",
        output=JobOutputStream(),
    )


def test_job_run_info_resources_round_trip():
    from cronstable.resources import ResourceUsage

    run = _mk_run("success")
    run.resource_usage = ResourceUsage(1.0, 0.5, 9000, 4)
    d = run.to_dict()
    assert d["resources"]["cpu_total_seconds"] == 1.5
    assert d["resources"]["max_rss_bytes"] == 9000
    # rehydrate from the ledger record
    restored = cronstable.cron._job_run_info_from_dict(d)
    assert restored is not None
    assert restored.resource_usage == run.resource_usage


def test_job_run_info_round_trip_without_resources():
    d = _mk_run("success").to_dict()
    assert d["resources"] is None
    restored = cronstable.cron._job_run_info_from_dict(d)
    assert restored is not None
    assert restored.resource_usage is None


def test_run_stats_cpu_and_memory_aggregates():
    from cronstable.resources import ResourceUsage

    runs = []
    for cpu, rss in ((1.0, 1000), (3.0, 5000)):
        r = _mk_run("success")
        r.resource_usage = ResourceUsage(cpu, 0.0, rss, 1)
        runs.append(r)
    # one unmonitored run in the window: it must not skew the averages
    runs.append(_mk_run("success"))
    stats = cronstable.cron._run_stats(runs)
    assert stats["avg_cpu_seconds"] == 2.0
    assert stats["max_cpu_seconds"] == 3.0
    assert stats["max_rss_bytes"] == 5000
    # the last run was unmonitored -> last_* are None
    assert stats["last_cpu_seconds"] is None
    assert stats["last_rss_bytes"] is None


def test_run_stats_last_fields_take_the_newest_by_finished_at():
    # run records are appended unserialized and a peer node sharing the mount
    # appends through its own process, so the last row in the list can be an
    # older run.  Inverted on purpose: the newer run is FIRST.
    from cronstable.resources import ResourceUsage

    newer = _mk_run("success", dur=5.0)
    newer.resource_usage = ResourceUsage(3.0, 0.0, 7000, 1)
    older = _mk_run("success", dur=1.0)
    older.resource_usage = ResourceUsage(1.0, 0.0, 1000, 1)
    stats = cronstable.cron._run_stats([newer, older])
    assert stats["last_duration"] == 5.0
    assert stats["last_cpu_seconds"] == 3.0
    assert stats["last_rss_bytes"] == 7000
    # the aggregates are untouched: only the "last" selection moved
    assert stats["avg_duration"] == 3.0
    assert stats["max_cpu_seconds"] == 3.0


def test_run_stats_equal_finish_instants_resolve_to_the_later_position():
    # the documented tiebreak, and the guard for a plain max(): max returns
    # the FIRST maximum it walks, so folding forwards over rows sharing an
    # instant would report the OLDEST position's run as "last" and quietly
    # invert test_run_stats_cpu_and_memory_aggregates above.
    from cronstable.resources import ResourceUsage

    first = _mk_run("success")
    first.resource_usage = ResourceUsage(1.0, 0.0, 1000, 1)
    second = _mk_run("success")
    second.resource_usage = ResourceUsage(9.0, 0.0, 9000, 1)
    stats = cronstable.cron._run_stats([first, second])
    assert stats["last_cpu_seconds"] == 9.0
    assert stats["last_rss_bytes"] == 9000


def test_run_stats_row_without_a_finish_instant_never_wins_the_fold():
    # the key function is total, so no caller has to pre-filter its rows: a
    # JobRunInfo without a finish instant sorts oldest instead of making the
    # fold raise TypeError.  The ledger cannot produce one (a record with
    # neither finished_at nor interruptedAt is rejected outright by
    # _job_run_info_from_dict, and a crash-reconciled row carrying only
    # interruptedAt is rebuilt with the interruption instant standing in), so
    # this row is hand-constructed.
    from cronstable.resources import ResourceUsage

    orphan = cronstable.cron.JobRunInfo(
        outcome="unknown",
        exit_code=None,
        started_at=None,
        finished_at=None,
        fail_reason="no finish instant",
        output=JobOutputStream(),
    )
    real = _mk_run("success", dur=2.0)
    real.resource_usage = ResourceUsage(4.0, 0.0, 4000, 1)
    assert cronstable.cron._run_finish_key(
        orphan
    ) < cronstable.cron._run_finish_key(real)
    stats = cronstable.cron._run_stats([real, orphan])
    assert stats["last_duration"] == 2.0
    assert stats["last_cpu_seconds"] == 4.0
    assert stats["total"] == 2


def test_run_stats_no_monitored_runs_leaves_resource_fields_none():
    stats = cronstable.cron._run_stats(
        [_mk_run("success"), _mk_run("failure")]
    )
    assert stats["avg_cpu_seconds"] is None
    assert stats["max_rss_bytes"] is None
    assert stats["last_cpu_seconds"] is None


def test_record_run_caps_history():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    limit = cronstable.cron.RUN_HISTORY_LIMIT
    for i in range(limit + 10):
        cron._record_run("alpha", _mk_run("success", exit_code=i))
    hist = cron.run_history["alpha"]
    assert len(hist) == limit  # bounded ring buffer
    # oldest entries evicted; newest retained and ordered oldest-first
    assert hist[0].exit_code == 10
    assert hist[-1].exit_code == limit + 9
    # last_run mirrors the most recent recorded run
    assert cron.last_run["alpha"].exit_code == limit + 9


def test_record_run_releases_superseded_ring():
    # Only the newest finished run's output is replayable, so a superseded
    # record must not keep its ring alive for the whole history window:
    # before the release, 50 retained records x a 1000-line ring per job
    # could pin gigabytes of unservable lines fleet-wide.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    first = _mk_run("success")
    first.output.publish("stdout", "kept while newest")
    first.output.publish("stderr", "and this one")
    first.output.close()
    cron._record_run("alpha", first)
    # while newest, the ring is intact (this is what the log replay serves)
    assert len(cron.last_run["alpha"].output.lines) == 2

    second = _mk_run("failure")
    second.output.publish("stdout", "the new newest")
    second.output.close()
    cron._record_run("alpha", second)

    # the superseded record stays in history as a summary, but its ring is
    # gone; the counters survive (history rows still show published totals).
    assert cron.run_history["alpha"][0] is first
    assert list(first.output.lines) == []
    assert first.output.published == 2
    # the newest record's ring is untouched
    assert [line for _s, line in cron.last_run["alpha"].output.lines] == [
        "the new newest"
    ]


def test_record_run_releases_the_ring_of_whichever_row_is_not_newest():
    # the other direction of the same invariant, and a two-directional trap.
    # A completion landing older than one already recorded (a peer node's
    # append on a shared mount, or a backward wall-clock step) does not take
    # over last_run, so the release has to follow the promotion rather than
    # the call order: freeing the previous record unconditionally would empty
    # the drawer the log replay still serves, and freeing nothing would leak
    # the un-promoted row's 1000-line ring for the whole history window.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    newer = _mk_run("success", dur=5.0)
    newer.output.publish("stdout", "kept while newest")
    newer.output.close()
    cron._record_run("alpha", newer)

    older = _mk_run("failure", exit_code=1, dur=1.0)
    older.output.publish("stdout", "landed late")
    older.output.close()
    cron._record_run("alpha", older)

    assert cron.last_run["alpha"] is newer
    assert [line for _s, line in newer.output.lines] == ["kept while newest"]
    # the un-promoted row is the superseded one, so ITS ring is released
    assert list(older.output.lines) == []
    assert older.output.published == 1
    # both rows stay in history as summaries
    assert list(cron.run_history["alpha"]) == [newer, older]
    # and the watermarks did not rewind with it
    assert cron._sla_last_success["alpha"] == newer.finished_at
    assert cron._last_completed_at["alpha"] == newer.finished_at
    assert cron._last_real_outcome["alpha"] == (newer.finished_at, "success")


async def test_reconcile_open_record_releases_the_superseded_ring():
    # the third writer of last_run, and the one that installs a row it did
    # not run: on a runtime slot takeover the outgoing last_run is a real
    # local run holding a full ring, which would otherwise sit in
    # run_history until it fell out of RUN_HISTORY_LIMIT.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    ran = _mk_run("success", dur=1.0)
    ran.output.publish("stdout", "replayable while newest")
    ran.output.close()
    cron._record_run("alpha", ran)

    # this host's own crash, so the synthetic row is promoted whatever its
    # instant, and the run it supersedes gives up its ring
    cron._reconcile_open_record(
        "alpha",
        cron.cron_jobs["alpha"],
        {"startedAt": "2020-01-01T00:00:00+00:00", "host": cron._state_host},
        "reconciled-crash",
    )
    assert cron.last_run["alpha"].outcome == "unknown"
    assert list(ran.output.lines) == []
    assert ran.output.published == 1
    await asyncio.gather(*list(cron._pending_state_writes))


_ARCHIVE_JOB = """
jobs:
  - name: a
    command: echo hi
    schedule: "*/5 * * * *"
    captureStdout: true
    archiveOutput: true
"""


class _GatedAppendBackend:
    """A ledger stub whose appends wait for the test to open a gate.

    Models a slow store: the fire-and-forget persist task is still parked on
    its first append while later completions of the same job land.
    """

    def __init__(self):
        self.appends = []
        self.gate = asyncio.Event()

    async def append_record(self, stream, record, prune_keep=None):
        await self.gate.wait()
        self.appends.append((stream, record))


@pytest.mark.asyncio
async def test_archive_snapshots_lines_at_record_time():
    # The archive must write the lines the run had when it was RECORDED,
    # not whatever the ring holds when the persist task finally runs: a
    # back-to-back completion releases the superseded ring, and reading it
    # late would archive nothing.
    cron = cronstable.cron.Cron(None, config_yaml=_ARCHIVE_JOB)
    backend = _GatedAppendBackend()
    cron.state_backend = backend

    first = _mk_run("success")
    first.output.publish("stdout", "one")
    first.output.publish("stdout", "two")
    first.output.close()
    cron._record_run("a", first)

    # the store has not accepted the first run's append yet when the next
    # completion supersedes it and releases its ring
    second = _mk_run("success")
    second.output.publish("stdout", "three")
    second.output.close()
    cron._record_run("a", second)
    assert list(first.output.lines) == []

    backend.gate.set()
    await _drain_state_writes(cron)

    log_stream = cron._log_stream("a")
    archived = [
        rec for stream, rec in backend.appends if stream == log_stream
    ]
    # Order is not pinned. Both persist tasks suspend on the executor hop in
    # _archive_output (redact_lines) before their append, so which one lands
    # first rides thread scheduling, and delaying the first redact_lines call
    # inverts the pair. Each record must still hold its own record-time
    # snapshot, not the ring the newer completion already released.
    assert sorted(
        [entry["line"] for entry in rec["lines"]] for rec in archived
    ) == [["one", "two"], ["three"]]
    # nothing was double-counted as evicted: the snapshot held every line
    assert [rec["dropped_lines"] for rec in archived] == [0, 0]


def test_fleet_backend_prefers_observability_mesh():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.cluster_manager = object()
    assert cron._fleet_backend() is cron.cluster_manager
    mesh = object()
    cron.observability_mesh = mesh
    assert cron._fleet_backend() is mesh


@pytest.mark.asyncio
async def test_start_stop_observability_builds_mesh_and_installs_providers(
    monkeypatch,
):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    built = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: built.append(_FakeMesh(cfg)) or built[-1],
    )
    cluster_config = {
        "observabilityMesh": {"backend": "gossip", "marker": 1},
        "shareNodeStats": True,
    }
    await cron.start_stop_observability(cluster_config)
    assert cron.observability_mesh is built[0]
    assert built[0].started is True
    # both fleet providers installed on the overlay mesh, node stats SHARED
    assert built[0].job_summaries_provider == cron.fleet_job_summaries
    assert built[0].node_stats_provider == cron.node_resource_snapshot
    assert built[0].node_stats_share is True
    # a reload dropping the observability section tears the mesh down
    await cron.start_stop_observability({"observabilityMesh": None})
    assert cron.observability_mesh is None
    assert built[0].stopped is True


@pytest.mark.asyncio
async def test_start_stop_observability_respects_share_opt_out(monkeypatch):
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: made.append(_FakeMesh(cfg)) or made[-1],
    )
    # mesh configured (for job summaries) but shareNodeStats off
    await cron.start_stop_observability(
        {"observabilityMesh": {"backend": "gossip"}, "shareNodeStats": False}
    )
    assert made[0].job_summaries_provider == cron.fleet_job_summaries
    # the provider is still installed (for the overlay's own self readout) but
    # NOT gossiped to peers
    assert made[0].node_stats_provider == cron.node_resource_snapshot
    assert made[0].node_stats_share is False


@pytest.mark.asyncio
async def test_start_stop_observability_reconciles_share_on_kept_mesh(
    monkeypatch,
):
    # shareNodeStats lives on the CLUSTER config, not on the resolved mesh
    # config the keep/rebuild comparison sees, so a toggle keeps the running
    # mesh: the latched share flag must be re-reconciled every reload, or
    # toggling off would keep gossiping CPU/memory until an unrelated restart
    # and toggling on would never start.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    made = []
    monkeypatch.setattr(
        cronstable.cron,
        "make_backend",
        lambda cfg, jsid: made.append(_FakeMesh(cfg)) or made[-1],
    )
    mesh_cfg = {"backend": "gossip", "marker": 1}
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
    )
    assert made[0].node_stats_share is True
    # toggle OFF: the mesh config is unchanged, so the mesh is KEPT...
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": False}
    )
    assert cron.observability_mesh is made[0]
    assert made[0].stopped is False
    # ...and the running mesh sees the new share value
    assert made[0].node_stats_share is False
    # toggling back ON reaches the kept mesh too
    await cron.start_stop_observability(
        {"observabilityMesh": mesh_cfg, "shareNodeStats": True}
    )
    assert cron.observability_mesh is made[0]
    assert made[0].node_stats_share is True


@pytest.mark.asyncio
async def test_start_stop_observability_none_is_noop():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    await cron.start_stop_observability(None)
    assert cron.observability_mesh is None
    await cron.start_stop_observability({"observabilityMesh": None})
    assert cron.observability_mesh is None


@pytest.mark.asyncio
async def test_web_get_cluster_injects_local_node_stats():
    import json

    cron = _cron(TWO_JOBS)

    class FakeMgr:
        def view_dict(self):
            return {"backend": "gossip", "peers": []}

    cron.cluster_manager = FakeMgr()
    resp = await cron._web_get_cluster(Req())
    data = json.loads(resp.text)
    # this node's own live load is always injected (local, free)
    assert data["node_stats"] is not None
    assert "cpu_percent" in data["node_stats"]


@pytest.mark.asyncio
async def test_web_get_node_returns_resources():
    import json

    cron = _cron(TWO_JOBS)
    resp = await cron._web_get_node(Req())
    data = json.loads(resp.text)
    assert data["node_name"]
    # psutil is a core dep, so the node snapshot is populated in tests
    assert data["resources"] is not None
    assert "cpu_percent" in data["resources"]
    assert "mem_percent" in data["resources"]


@pytest.mark.asyncio
async def test_job_to_dict_includes_live_running_resources():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    job = cron.cron_jobs["alpha"]

    class FakeRunning:
        proc = None

        def live_resources(self):
            return {"cpu_percent": 40.0, "cpu_seconds": 2.0, "rss_bytes": 1000}

    cron.running_jobs["alpha"] = [FakeRunning(), FakeRunning()]
    d = cron._job_to_dict("alpha", job)
    # summed across the two running instances
    assert d["running_resources"] == {
        "cpu_percent": 80.0,
        "cpu_seconds": 4.0,
        "rss_bytes": 2000,
        "instances": 2,
    }


@pytest.mark.asyncio
async def test_job_to_dict_omits_running_resources_when_unmonitored():
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    job = cron.cron_jobs["alpha"]

    class FakeRunning:
        proc = None

        def live_resources(self):
            return None  # unmonitored / no sample yet

    cron.running_jobs["alpha"] = [FakeRunning()]
    d = cron._job_to_dict("alpha", job)
    assert "running_resources" not in d


@pytest.mark.asyncio
async def test_web_list_jobs_includes_history_and_timezone():
    import json

    cron = _cron(TWO_JOBS)
    for outcome in ("success", "failure", "success"):
        cron._record_run("alpha", _mk_run(outcome))
    resp = await cron._web_list_jobs(Req())
    data = json.loads(resp.text)
    alpha = data[0]
    # inline compact history (oldest first) for the table sparkline
    assert [h["outcome"] for h in alpha["history"]] == [
        "success",
        "failure",
        "success",
    ]
    # schedule reference frame exposed for client-side next-run computation;
    # a utc:true job (the default) resolves to the "UTC" zone
    assert alpha["utc"] is True
    assert alpha["timezone"] == "UTC"
    # a job that never ran reports an empty (not missing) history
    assert data[1]["history"] == []


@pytest.mark.asyncio
async def test_web_job_runs_endpoint_returns_runs_and_stats():
    import json

    cron = _cron(TWO_JOBS)
    cron._record_run("alpha", _mk_run("success", dur=2.0))
    cron._record_run("alpha", _mk_run("failure", dur=4.0))
    cron._record_run("alpha", _mk_run("success", dur=6.0))
    cron._record_run("alpha", _mk_run("cancelled", dur=1.0))

    # the runs listing reads Req's (empty) query for its `limit` param
    resp = await cron._web_job_runs(Req(match={"name": "alpha"}))
    body = json.loads(resp.text)
    assert body["name"] == "alpha"
    assert [r["outcome"] for r in body["runs"]] == [
        "success",
        "failure",
        "success",
        "cancelled",
    ]
    stats = body["stats"]
    assert stats["total"] == 4
    assert stats["success"] == 2
    assert stats["failure"] == 1
    assert stats["cancelled"] == 1
    # success rate excludes cancellations: 2 success / (2 success + 1 failure)
    assert stats["success_rate"] == pytest.approx(2 / 3)
    assert stats["avg_duration"] == pytest.approx((2 + 4 + 6 + 1) / 4)
    assert stats["min_duration"] == 1.0
    assert stats["max_duration"] == 6.0
    # "last" is the newest run by finished_at, not the last one appended:
    # _mk_run shares one start instant across the four rows and varies the
    # duration, so the dur=6.0 run is the one that finished last even
    # though the dur=1.0 cancellation was recorded after it.
    assert stats["last_duration"] == 6.0
    # ...and last_run follows the same rule, so the older cancellation
    # recorded last did not become the job's latest run.
    assert cron.last_run["alpha"].outcome == "success"


@pytest.mark.asyncio
async def test_web_job_runs_unknown_job_404():
    from aiohttp import web

    cron = _cron(TWO_JOBS)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_job_runs(Req(match={"name": "nope"}))


@pytest.mark.asyncio
async def test_web_job_runs_empty_history():
    import json

    cron = _cron(TWO_JOBS)
    # the runs listing reads Req's (empty) query for its `limit` param
    resp = await cron._web_job_runs(Req(match={"name": "alpha"}))
    body = json.loads(resp.text)
    assert body["runs"] == []
    assert body["stats"]["total"] == 0
    assert body["stats"]["success_rate"] is None
    assert body["stats"]["avg_duration"] is None


@pytest.mark.asyncio
async def test_web_job_runs_honours_limit_param():
    # the one run-listing surface without a cap gained the same clamped
    # `limit` its DAG and MCP twins always had; the default serves the
    # whole retained history, exactly the old behavior
    import json

    cron = _cron(TWO_JOBS)
    for n in range(5):
        cron._record_run("alpha", _mk_run("success", dur=float(n + 1)))
    resp = await cron._web_job_runs(
        Req(match={"name": "alpha"}, query={"limit": "2"})
    )
    body = json.loads(resp.text)
    assert [r["duration"] for r in body["runs"]] == [4.0, 5.0]  # newest kept
    assert body["stats"]["total"] == 5  # stats keep the whole window


def test_web_int_query_reads_limit_before_its_legacy_alias():
    # count/per_job/runs had grown endpoint by endpoint; every capped
    # listing reads `limit` first and its original spelling still works
    query_only_alias = Req(query={"count": "7"})
    assert (
        cronstable.cron.Cron._web_int_query(
            query_only_alias, "limit", default=12, lo=1, hi=60, alias="count"
        )
        == 7
    )
    both = Req(query={"count": "7", "limit": "9"})
    assert (
        cronstable.cron.Cron._web_int_query(
            both, "limit", default=12, lo=1, hi=60, alias="count"
        )
        == 9  # the canonical name wins when both are present
    )


def test_strip_headers_drops_names_in_any_spelling():
    # header names are case-insensitive on the wire but these dicts are
    # not; the helpers are the one home of the endpoint-header-wins rule
    strip = cronstable.cron._strip_headers
    assert strip(None, "content-type") == {}
    assert strip({"Content-TYPE": "x", "X-Custom": "y"}, "content-type") == {
        "X-Custom": "y"
    }
    assert strip(
        {"content-length": "3", "Content-Type": "x", "Allow": "GET"},
        "content-type",
        "content-length",
    ) == {"Allow": "GET"}
    assert cronstable.cron._strip_content_type({"CONTENT-type": "x"}) == {}


@pytest.mark.asyncio
async def test_handler_errors_carry_the_json_envelope():
    # every 4xx body on this origin is the ONE envelope {"error": msg}
    # (matching jobapi and /mcp) instead of per-handler text/plain. The
    # exception a handler RAISES carries it, not just the response the
    # middleware eventually writes: a direct call never touches the
    # middleware, and neither do the two middleware-less test apps below.
    import json

    from aiohttp import web

    cron = _cron(TWO_JOBS)
    for handler, request, reason in (
        (
            cron._web_job_runs,
            Req(match={"name": "nope"}),
            "job 'nope' not found",
        ),
        (
            cron._web_get_job,
            Req(match={"name": "nope"}),
            "job 'nope' not found",
        ),
        (
            cron._web_job_trends,
            Req(match={"name": "nope"}),
            "job 'nope' not found",
        ),
        (
            cron._web_schedule_why,
            Req(query={"job": "nope", "at": "2026-01-01T00:00:00Z"}),
            "no job or DAG schedule named 'nope'",
        ),
        (
            cron._web_job_calendar,
            Req(match={"name": "nope"}),
            "no job or DAG schedule named 'nope'",
        ),
    ):
        with pytest.raises(web.HTTPNotFound) as raised:
            await handler(request)
        assert raised.value.content_type == "application/json"
        assert json.loads(raised.value.text or "") == {"error": reason}

    with pytest.raises(web.HTTPBadRequest) as raised:
        await cron._web_pause_job(
            Req(match={"name": "alpha"}, body={"durationSeconds": "soon"})
        )
    assert raised.value.content_type == "application/json"
    assert json.loads(raised.value.text or "") == {
        "error": "durationSeconds must be an integer"
    }


@pytest.mark.asyncio
async def test_web_cancel_unknown_job_404():
    from aiohttp import web

    cron = _cron(TWO_JOBS)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_cancel_job(Req(match={"name": "nope"}))


@pytest.mark.asyncio
async def test_web_cancel_not_running_409():
    from aiohttp import web

    cron = _cron(TWO_JOBS)
    with pytest.raises(web.HTTPConflict):
        await cron._web_cancel_job(Req(match={"name": "alpha"}))


@pytest.mark.asyncio
async def test_handle_finished_job_records_cancelled(monkeypatch):
    # a run cancelled by the user is recorded as "cancelled" but, like a
    # replacement, must not be reported as success/failure or retried.
    from types import SimpleNamespace

    cron = cronstable.cron.Cron(None)
    calls = []

    async def fake_failure(job):
        calls.append(("failure", job))

    async def fake_success(job):
        calls.append(("success", job))

    monkeypatch.setattr(cron, "handle_job_failure", fake_failure)
    monkeypatch.setattr(cron, "handle_job_success", fake_success)

    job = SimpleNamespace(
        config=SimpleNamespace(name="test", concurrencyScope="node"),
        replaced=False,
        cancelled=True,
        fail_reason=None,
        retcode=-15,
        stdout=None,
        stderr=None,
        started_at=None,
        output=JobOutputStream(),
    )
    cron.running_jobs["test"].append(job)
    await cron._handle_finished_job(job)

    assert calls == []  # neither reported
    assert "test" not in cron.running_jobs  # cleaned up
    assert cron.last_run["test"].outcome == "cancelled"
    assert cron.last_run["test"].exit_code == -15
    assert [r.outcome for r in cron.run_history["test"]] == ["cancelled"]


@pytest.mark.asyncio
async def test_web_cancel_running_job_terminates_and_records():
    # end-to-end: launch a real long-running job, cancel it via the endpoint,
    # and confirm it is actually terminated and recorded as "cancelled".
    cron = _cron(CONCURRENT_JOB.format(policy="Allow"))
    job = cron.cron_jobs["test"]
    await cron.maybe_launch_job(job)
    rj = cron.running_jobs["test"][0]
    assert rj.proc.returncode is None

    resp = await cron._web_cancel_job(Req(match={"name": "test"}))
    assert resp.status == 200
    # the MCP cron_cancel_job ack shape (this route once returned an
    # empty 200 while every sibling action returned JSON)
    import json as _json_mod

    assert _json_mod.loads(resp.text) == {
        "cancelled": "test",
        "instances": 1,
    }
    assert rj.cancelled is True
    assert rj.proc.returncode is not None  # process actually terminated

    # the reaper would normally do this once the process exits; drive it here
    await cron._handle_finished_job(rj)
    assert "test" not in cron.running_jobs
    assert cron.last_run["test"].outcome == "cancelled"
    assert [r.outcome for r in cron.run_history["test"]] == ["cancelled"]


@pytest.mark.asyncio
async def test_web_index_served():
    cron = _cron(TWO_JOBS)
    # a real web.Request always carries headers; _web_index reads
    # If-None-Match and Accept-Encoding off them (Req provides both).
    resp = await cron._web_index(Req())
    assert resp.content_type == "text/html"
    assert "cronstable" in resp.text
    assert "<html" in resp.text.lower()


@pytest.mark.asyncio
async def test_web_index_sets_security_headers():
    cron = _cron(TWO_JOBS)
    resp = await cron._web_index(Req())
    csp = resp.headers["Content-Security-Policy"]
    # self-contained app: no external connections, so the CSP confines any
    # injected script to this origin and blocks framing of the action controls
    assert "connect-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "object-src 'none'" in csp
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"


@pytest.mark.asyncio
async def test_web_index_security_headers_overridable():
    # an operator-configured web.headers value wins over the secure default,
    # while defaults the operator didn't set are still applied.
    cron = cronstable.cron.Cron(None, config_yaml=TWO_JOBS)
    cron.web_config = {"headers": {"X-Frame-Options": "SAMEORIGIN"}}
    resp = await cron._web_index(Req())
    assert resp.headers["X-Frame-Options"] == "SAMEORIGIN"  # operator override
    assert resp.headers["X-Content-Type-Options"] == "nosniff"  # default kept


@pytest.mark.asyncio
async def test_web_index_revalidates_with_304():
    # the dashboard is static package data, so a client that echoes the ETag
    # gets an empty 304 instead of another ~573 KB body.
    cron = _cron(TWO_JOBS)
    _raw, etag = cronstable.cron._index_document()

    resp = await cron._web_index(Req(headers={"If-None-Match": etag}))
    assert resp.status == 304
    assert resp.headers["ETag"] == etag
    assert not resp.body

    # a stale/absent validator still gets the full document
    full = await cron._web_index(Req(headers={"If-None-Match": '"stale"'}))
    assert full.status == 200
    assert full.body


@pytest.mark.asyncio
async def test_web_index_serves_gzip_when_accepted():
    # precompressed once for the life of the process; the compressed body must
    # decode back to exactly the identity body.
    import gzip as _gzip

    cron = _cron(TWO_JOBS)
    raw, _etag = cronstable.cron._index_document()

    resp = await cron._web_index(
        Req(headers={"Accept-Encoding": "gzip, deflate"})
    )
    assert resp.status == 200
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert _gzip.decompress(resp.body) == raw
    assert len(resp.body) < len(raw)

    # a client that does not accept gzip still gets the identity body, and the
    # response still advertises that it varies on the header.
    plain = await cron._web_index(Req())
    assert "Content-Encoding" not in plain.headers
    assert plain.headers["Vary"] == "Accept-Encoding"
    assert plain.body == raw


@pytest.mark.asyncio
async def test_auth_middleware_public_path():
    from aiohttp import web

    middleware = cronstable.cron.Cron._make_auth_middleware(
        "secret", cronstable.cron.WEB_PUBLIC_PATHS
    )

    async def handler(request):
        return web.Response(text="ok")

    class FakeRequest:
        def __init__(self, path, auth=None):
            self.path = path
            self.method = "GET"
            self.headers = {"Authorization": auth} if auth else {}

    # the UI page is reachable without a token...
    resp = await middleware(FakeRequest("/"), handler)
    assert resp.text == "ok"
    # ...but data endpoints still require it
    with pytest.raises(web.HTTPUnauthorized):
        await middleware(FakeRequest("/jobs"), handler)


@pytest.mark.asyncio
async def test_web_job_logs_streams_last_run():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(TWO_JOBS)
    out = JobOutputStream()
    out.publish("stdout", "hello world\n")
    out.publish("stderr", "uh oh\n")
    out.close()
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=None,
        finished_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        fail_reason=None,
        output=out,
    )
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")
        assert resp.status == 200
        assert resp.content_type == "text/event-stream"
        body = await resp.text()
    # buffered lines of the last run are replayed, then the stream ends
    assert "event: line" in body
    assert "hello world" in body
    assert "uh oh" in body
    assert "event: end" in body


@pytest.mark.asyncio
async def test_web_job_logs_batches_live_bursts(monkeypatch):
    """A burst of published lines must reach the SSE client in a handful of
    transport writes, not one write (plus one fresh wait_for timer task) per
    line: per-line delivery on the scheduler's loop was ~2 coroutine steps
    per line per subscriber for a chatty job."""
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(TWO_JOBS)
    out = JobOutputStream()
    out.publish("stdout", "replayed\n")
    cron.last_run["alpha"] = cronstable.cron.JobRunInfo(
        outcome="success",
        exit_code=0,
        started_at=None,
        finished_at=DT(1999, 12, 31, 12, 0, 0, tzinfo=UTC),
        fail_reason=None,
        output=out,  # NOT closed: the handler stays in its live-tail loop
    )
    writes = []
    real_write = web.StreamResponse.write

    async def counting_write(self, data):
        writes.append(bytes(data))
        return await real_write(self, data)

    monkeypatch.setattr(web.StreamResponse, "write", counting_write)
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp_task = asyncio.create_task(client.get("/jobs/alpha/logs"))
        # wait for the handler to attach its subscriber queue
        for _ in range(500):
            if out._subscribers:
                break
            await asyncio.sleep(0.01)
        assert out._subscribers, "tail never attached"
        # one synchronous burst: no await between publishes, so the whole
        # burst is queued before the handler can wake once
        for i in range(60):
            out.publish("stdout", "line-%d\n" % i)
        out.close()
        resp = await resp_task
        body = await resp.text()
    assert "line-0" in body and "line-59" in body and "event: end" in body
    burst_writes = [w for w in writes if b"line-" in w]
    # the whole 60-line burst went out in a handful of joined writes (the
    # old per-line loop needed 60); the exact count depends on how often
    # the handler woke mid-burst, so bound it rather than pin it.
    assert len(burst_writes) <= 6, len(burst_writes)
    assert sum(w.count(b"event: line") for w in burst_writes) == 60


@pytest.mark.asyncio
async def test_web_job_logs_no_output():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(TWO_JOBS)
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")  # never run
        assert resp.status == 200
        body = await resp.text()
    assert "no-output" in body


@pytest.mark.asyncio
async def test_web_job_logs_unknown_job():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(TWO_JOBS)
    # a bare Application with NO middleware, deliberately: this is where
    # the handler's own response reaches the wire unrewrapped, so it shows
    # the handler is a conforming object rather than one the envelope
    # middleware happens to rescue. Adding the middleware here for tidiness
    # would silently retire that half of the assertion.
    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/nope/logs")
        assert resp.status == 404
        assert resp.content_type == "application/json"
        assert (await resp.json())["error"] == "job 'nope' not found"


# ---------------------------------------------------------------------------
# Web server integration.
#
# Every other web test calls a handler coroutine directly with a hand-rolled
# fake request, so routing, the auth middleware, and the bind/serve path are
# never exercised together. These drive the real server start_stop_web_app
# stands up, over real HTTP, so a dropped route or an inverted ui/auth gate (a
# data endpoint served unauthenticated) is caught.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_app_enforces_auth_when_token_configured(start_web_app):
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": False,
        },
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    async with aiohttp.ClientSession() as session:
        # no credentials -> rejected
        async with session.get(base + "/jobs") as resp:
            assert resp.status == 401
        # wrong token -> rejected
        async with session.get(
            base + "/jobs", headers={"Authorization": "Bearer nope"}
        ) as resp:
            assert resp.status == 401
        # correct token -> the real jobs payload is served
        async with session.get(
            base + "/jobs", headers={"Authorization": "Bearer secret"}
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert [j["name"] for j in data] == ["alpha"]
    # clearing the config fully stops the server
    await cron.start_stop_web_app(None)
    assert cron.web_runner is None


@pytest.mark.asyncio
async def test_shutdown_route_is_gated_by_the_real_app(start_web_app):
    """POST /shutdown through the real router and middleware stack.

    The handler's own refusal is unit-tested in test_cron_lifecycle.py,
    but that test drives the coroutine directly and so proves nothing
    about the wiring the refusal depends on: that the route is registered
    at all, that it never lands in WEB_PUBLIC_PATHS, and that its scope
    resolves to `control` rather than the `view` every read endpoint
    takes.  Each of those is a one-line edit elsewhere, and any of them
    would turn the stop switch loose without failing a single test.
    """
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            # `view` alone must not carry it, so the scoped table is what
            # exercises the gate; `admin` is the full-scope control token.
            "authTokens": [
                {"label": "readonly", "scopes": ["view"], "value": "ro-tok"},
                {
                    "label": "admin",
                    "scopes": ["view", "control"],
                    "value": "rw-tok",
                },
            ],
            # the UI page is the one public path; /shutdown must not join it
            "ui": True,
        },
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    async with aiohttp.ClientSession() as session:
        # public UI, to prove the listener really is the ui-enabled shape
        # that widens WEB_PUBLIC_PATHS
        async with session.get(base + "/") as resp:
            assert resp.status == 200
        # no credentials: the auth middleware refuses before the handler
        async with session.post(base + "/shutdown") as resp:
            assert resp.status == 401
        assert not cron._stop_event.is_set()
        # authenticated but read-only: 403 from the SCOPE check, which is
        # the assertion that pins /shutdown to `control`
        async with session.post(
            base + "/shutdown", headers={"Authorization": "Bearer ro-tok"}
        ) as resp:
            assert resp.status == 403
            assert "control" in (await resp.json())["error"]
        assert not cron._stop_event.is_set()
        # the control token drives the same drain Ctrl-C/SIGTERM trigger
        async with session.post(
            base + "/shutdown", headers={"Authorization": "Bearer rw-tok"}
        ) as resp:
            assert resp.status == 200
            assert await resp.json() == {"shuttingDown": True}
    assert cron._stop_event.is_set()


@pytest.mark.asyncio
async def test_shutdown_route_refuses_on_a_listener_with_no_tokens():
    """The open-listener case, end to end.

    With no token configured there is no auth middleware, so every other
    route is reachable by anyone who can connect.  /shutdown is the one
    that must still refuse: reachable (not a 404, so the route really is
    registered on this shape too) and refused with the remedy named.
    """
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    try:
        await cron.start_stop_web_app({"listen": ["http://127.0.0.1:0"]})
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # the neighbouring control route is wide open on this listener
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 200
            async with session.post(base + "/shutdown") as resp:
                assert resp.status == 403
                assert "web.authToken" in (await resp.json())["error"]
        assert not cron._stop_event.is_set()
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_web_app_ui_path_public_but_data_paths_require_auth(
    start_web_app,
):
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": True,
        },
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    async with aiohttp.ClientSession() as session:
        # the UI page holds no data, so it is reachable without a token
        async with session.get(base + "/") as resp:
            assert resp.status == 200
            assert "text/html" in resp.headers["Content-Type"]
        # a data endpoint still requires the token even with the UI enabled
        async with session.get(base + "/jobs") as resp:
            assert resp.status == 401


@pytest.mark.asyncio
async def test_web_json_endpoints_tolerate_operator_content_type(
    start_web_app,
):
    # aiohttp refuses content_type= when the headers mapping already
    # carries a Content-Type, so an operator-configured web.headers
    # Content-Type used to 500 every route built by _json_response and
    # the conditional-serve tail.  The endpoint's own Content-Type wins,
    # in any spelling: this polices _json_response, _strip_content_type
    # and _conditional_response end to end.
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            # a sentinel no endpoint serves, so every row below (including
            # the text/plain routes) discriminates operator-vs-own
            "headers": {"content-type": "application/x-operator"},
            "ui": True,
        },
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    expected = {
        "/jobs": "application/json",
        "/fleet": "application/json",
        "/cluster": "application/json",
        "/dags": "application/json",
        "/": "text/html",
        "/calendar.ics": "text/calendar",
        # the plain-text trio (no Accept: application/json sent, so
        # /job-set-id and /status take their text branches)
        "/version": "text/plain",
        "/job-set-id": "text/plain",
        "/status": "text/plain",
    }
    async with aiohttp.ClientSession() as session:
        for path, ctype in expected.items():
            async with session.get(base + path) as resp:
                assert resp.status == 200, path
                assert resp.content_type == ctype, path


def test_error_envelope_middleware_carries_the_new_style_marker():
    # An UNMARKED middleware is not refused: aiohttp reads it as a pre-3.0
    # middleware FACTORY, calls it as m(app, handler), and every request 500s
    # behind nothing louder than a DeprecationWarning. cron.py sets the marker
    # by assignment at module scope rather than with @web.middleware, because
    # the decorator would read an attribute off the lazy aiohttp door and
    # import the whole web stack at import time. This pins the assignment to
    # whatever aiohttp's own decorator does, so an aiohttp release that moves
    # the marker cannot silently demote the envelope to a factory.
    from aiohttp import web

    async def probe(request, handler):  # pragma: no cover - never called
        raise AssertionError("probe middleware must not run")

    marked = web.middleware(probe)
    assert (
        cronstable.cron._error_envelope_middleware.__middleware_version__
        == marked.__middleware_version__
    )


@pytest.mark.asyncio
async def test_web_errors_carry_the_json_envelope(start_web_app):
    # every error body is one JSON envelope, including the three families
    # that used to escape as aiohttp's text/plain defaults: the auth
    # middleware's 401, the router's 404 on an unmatched path, and the
    # router's 405 on a wrong method (whose Allow header must survive the
    # rewrap).
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": False,
        },
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    auth = {"Authorization": "Bearer secret"}
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/jobs") as resp:
            assert resp.status == 401
            assert resp.content_type == "application/json"
            assert "error" in await resp.json()
        async with session.get(
            base + "/no-such-route", headers=auth
        ) as resp:
            assert resp.status == 404
            assert resp.content_type == "application/json"
            assert "error" in await resp.json()
        async with session.delete(base + "/jobs", headers=auth) as resp:
            assert resp.status == 405
            assert resp.content_type == "application/json"
            assert "error" in await resp.json()
            assert "GET" in resp.headers.get("Allow", "")


@pytest.mark.asyncio
async def test_chunked_oversized_mcp_body_413s_with_the_envelope(
    start_web_app,
):
    # The fourth family the envelope middleware catches, and the only one
    # that starts below the application: a transport 413. POST /mcp refuses
    # an oversized body from request.content_length BEFORE reading it, but a
    # chunked request carries no length for that check to read, so the
    # handler goes on to `await request.read()` and aiohttp's own
    # client_max_size failure escapes it as an HTTPException nothing in the
    # handler built. Without the middleware the caller gets that as
    # text/plain, which is the shape /mcp clients never otherwise see.
    import aiohttp

    from cronstable.config import _build_mcp_config

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {"listen": ["http://127.0.0.1:0"], "ui": False},
        _build_mcp_config({"enabled": True}),
    )
    port = cron.web_runner.addresses[0][1]

    async def chunks():
        # 1.25 MiB, over BOTH caps: mcp.maxBodyBytes (1 MiB by default) and
        # aiohttp's client_max_size (1 MiB, the Application default this
        # app takes). Over both is what makes the body of the reply tell
        # the two refusals apart below. An async iterable is what makes the
        # request chunked: the client cannot size one, so it sends no
        # Content-Length and frames the body with Transfer-Encoding.
        for _ in range(20):
            yield b"x" * (64 * 1024)

    async with aiohttp.ClientSession() as session:
        url = "http://127.0.0.1:{}/mcp".format(port)
        async with session.post(url, data=chunks()) as resp:
            # The request really went out chunked. Asserted because the
            # test degrades into a second spelling of test_mcp.py's
            # test_http_oversized_body_is_413 (which drives the pre-check,
            # its FakeReq carrying a content_length) the moment the body
            # stops being an async iterable, and it degrades silently: the
            # status, the content type and the envelope are identical down
            # either path.
            sent = resp.request_info.headers
            assert sent.get("Transfer-Encoding") == "chunked"
            assert "Content-Length" not in sent
            assert resp.status == 413
            assert resp.content_type == "application/json"
            body = await resp.json()
            assert set(body) == {"error"}
            # ...and the server agrees it saw no length: the pre-check
            # answers with its own sentence (mcp.py:_http_error), which
            # never reaches the middleware because it is a RETURNED
            # Response. Any other reason means the read is what failed.
            assert body["error"] != "request body too large"


# Every route that used to answer with a bare `raise web.HTTPNotFound()`,
# with the reason its own lookup justifies. The two calendar/schedule rows
# resolve through _job_or_dag_schedule, so a DAG's synthetic dag:<name>
# schedule job is a legitimate 200 on them and the reason must not claim a
# job was the only thing searched; the rest really do look only in
# cron_jobs (or cron_dags). Parametrized rather than kept as a table keyed
# on WEB_ROUTES: that lockstep contract already has two owners
# (tests/test_openapi.py, tests/test_mcp_tools.py), and a third would mean
# one added route fails three tests in three files. The general rule (no
# handler raises a bare web.HTTP*) is carried by the AST guard below.
_CONVERTED_404_ROUTES = [
    (
        "/schedule/why?job=nope&at=2026-01-01T00:00:00Z",
        "no job or DAG schedule named 'nope'",
    ),
    ("/jobs/nope/calendar.ics", "no job or DAG schedule named 'nope'"),
    ("/jobs/nope", "job 'nope' not found"),
    ("/jobs/nope/trends", "job 'nope' not found"),
    ("/jobs/nope/logs", "job 'nope' not found"),
    ("/dags/nope/runs/rk/tasks/tk/logs", "dag 'nope' not found"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("path,reason", _CONVERTED_404_ROUTES)
async def test_converted_404s_name_what_was_not_found(
    start_web_app, path, reason
):
    # These six answered with aiohttp's own `404: Not Found`, which names
    # neither the subject nor what was searched for it. The envelope
    # middleware made the BODY json, so the defect survived a content-type
    # check; assert the reason itself.
    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(cron, {"listen": ["http://127.0.0.1:0"], "ui": False})
    port = cron.web_runner.addresses[0][1]
    async with aiohttp.ClientSession() as session:
        url = "http://127.0.0.1:{}{}".format(port, path)
        async with session.get(url) as resp:
            assert resp.status == 404
            assert resp.content_type == "application/json"
            assert await resp.json() == {"error": reason}


@pytest.mark.asyncio
async def test_dag_run_404_splits_unknown_dag_from_no_state_store():
    # DagRunStore answers a plain None for two different causes (dagrun.py:
    # `backend is None or dag_name not in self._dags()`), so the old single
    # reason told an operator with no `state:` section that every DAG they
    # configured did not exist. docs/openapi.yaml describes both causes on
    # these routes; the body now distinguishes them.
    import json

    from aiohttp import web

    cron = _cron(_DAG_LOGS_YAML)  # dags: lin, and no `state:` section
    assert cron.state_backend is None
    no_store = (
        "no `state:` store is configured; DAG run documents only exist "
        "in a durable store"
    )
    for handler, request in (
        (cron._web_dag_runs, Req(match={"name": "lin"})),
        (cron._web_dag_run, Req(match={"name": "lin", "run_key": "rk"})),
        (cron._web_dag_xcom, Req(match={"name": "lin", "run_key": "rk"})),
    ):
        with pytest.raises(web.HTTPNotFound) as raised:
            await handler(request)
        assert json.loads(raised.value.text or "") == {"error": no_store}

    # with a store configured the two DAG-side causes read as they did
    cron.state_backend = object()
    assert cron._dag_run_lookup_reason("ghost") == "dag 'ghost' not found"
    assert (
        cron._dag_run_lookup_reason("ghost", "rk") == "dag 'ghost' not found"
    )
    assert (
        cron._dag_run_lookup_reason("lin", "rk") == "dag 'lin' has no run 'rk'"
    )


def test_no_web_handler_raises_a_bare_http_error():
    # The pin that keeps the next handler from shipping a reasonless 404:
    # an error response is built through one of the named envelope helpers
    # (_api_error, _action_http_error, _http_for_action_error,
    # _push_store_unavailable), never by naming a web.HTTP* class in a
    # raise OR a return. The return half matters most: the envelope
    # middleware rescues a RAISED HTTPException only, so a returned one
    # reaches the wire as aiohttp's own text/plain. HTTPUnauthorized is
    # allowlisted for an ARGUMENTLESS raise, which is what makes its body
    # reasonless, and the rationale is written at its four sites here.
    # jobapi.py carries the same rule, pinned in
    # tests/test_state_job_api.py (its own test home).
    path = Path(cronstable.cron.__file__)
    assert bare_http_raises(path) == []


@pytest.mark.asyncio
async def test_web_500_and_504_carry_the_json_envelope(
    start_web_app, monkeypatch, caplog
):
    # The 500 was the one status a client could NOT parse uniformly: an
    # unhandled error escaped to aiohttp's own handler, which answers
    # text/plain. A store call that outran its budget is answered 504
    # rather than folded into the 500, since that is the accurate status.
    # Neither body may echo the exception: the reason goes to the log.
    import logging

    import aiohttp

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(cron, {"listen": ["http://127.0.0.1:0"], "ui": False})
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)

    # patch what the handler CALLS, not the handler: the routes bind their
    # bound methods when the app is built, so replacing the attribute
    # afterwards would never be seen.
    def boom():
        raise RuntimeError("secret detail: /var/lib/cronstable/state")

    def slow():
        raise asyncio.TimeoutError

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        async with aiohttp.ClientSession() as session:
            monkeypatch.setattr(cron, "status_payload", boom)
            async with session.get(base + "/status") as resp:
                assert resp.status == 500
                assert resp.content_type == "application/json"
                body = await resp.json()
            assert "error" in body
            assert "secret detail" not in body["error"]
            assert "/var/lib" not in body["error"]

            monkeypatch.setattr(cron, "status_payload", slow)
            async with session.get(base + "/status") as resp:
                assert resp.status == 504
                assert resp.content_type == "application/json"
                assert "error" in await resp.json()
    # the traceback aiohttp would have logged is not lost to the rewrap:
    # it rides exc_info, not the message, so the reason is recoverable
    # from the log even though the response withholds it
    logged = [
        r
        for r in caplog.records
        if r.exc_info and "secret detail" in str(r.exc_info[1])
    ]
    assert logged, [r.getMessage() for r in caplog.records]
    assert "/status" in logged[0].getMessage()


@pytest.mark.asyncio
async def test_web_app_restarts_on_config_change(monkeypatch):
    # changing the web config replaces the running server with a new one;
    # clearing it stops the server entirely. web_site_from_url is faked so no
    # real socket is bound and the transition logic is tested in isolation.
    started = []

    class FakeSite:
        def __init__(self, url):
            self.url = url

        async def start(self):
            started.append(self.url)

    monkeypatch.setattr(
        cronstable.cron,
        "web_site_from_url",
        lambda runner, url, ssl_context=None: FakeSite(url),
    )

    cron = cronstable.cron.Cron(None)
    await cron.start_stop_web_app({"listen": ["http://host-a:8000"]})
    runner1 = cron.web_runner
    assert runner1 is not None
    assert started == ["http://host-a:8000"]

    # a different config: the old runner is replaced and the new site started
    await cron.start_stop_web_app({"listen": ["http://host-b:9000"]})
    assert cron.web_runner is not None
    assert cron.web_runner is not runner1
    assert started == ["http://host-a:8000", "http://host-b:9000"]

    # clearing the config stops the server
    await cron.start_stop_web_app(None)
    assert cron.web_runner is None


# ---------------------------------------------------------------------------
# Web endpoints, the run loop, config-signature, and schedule helpers.
# ---------------------------------------------------------------------------


def test_webloop_origin_matches_host_no_hostname():
    # a Host header that parses to no hostname (a bare ":port") can never be a
    # same-origin match; fail closed.
    assert (
        cronstable.cron._origin_matches_host("http://example.com", ":8080")
        is False
    )
    # a real same-origin pair still matches, for contrast.
    assert (
        cronstable.cron._origin_matches_host(
            "http://a.example:80", "a.example"
        )
        is True
    )


def test_webloop_http_for_action_error_with_headers():
    from aiohttp import web

    ex = cronstable.cron.ApiActionError("nope", status=409)
    resp = cronstable.cron._http_for_action_error(ex, headers={"X-Test": "1"})
    assert isinstance(resp, web.HTTPConflict)
    assert resp.headers.get("X-Test") == "1"
    # and the headerless path maps the status to the matching exception type.
    assert isinstance(
        cronstable.cron._http_for_action_error(
            cronstable.cron.ApiActionError("x", status=404)
        ),
        web.HTTPNotFound,
    )


def test_webloop_fold_manifest_ignores_mistyped_fields():
    names, hosts, scopes, dags = set(), set(), set(), set()
    # every mistyped field contributes nothing (an older node's record simply
    # advertises less).
    cronstable.cron._fold_manifest(
        {"jobs": "a", "host": 123, "scopes": "s", "dags": None},
        names,
        hosts,
        scopes,
        dags,
    )
    assert (names, hosts, scopes, dags) == (set(), set(), set(), set())
    # a well-formed record still folds in.
    cronstable.cron._fold_manifest(
        {"jobs": ["j"], "host": "h", "scopes": ["sc"], "dags": ["d"]},
        names,
        hosts,
        scopes,
        dags,
    )
    assert names == {"j"}
    assert hosts == {"h"}
    assert scopes == {"sc"}
    assert dags == {"d"}


def test_webloop_load_index_html_disk_fallback(monkeypatch):
    import importlib

    # force the importlib.resources lookup to fail so the on-disk fallback path
    # is exercised; clear the lru_cache on the way in and out so neither this
    # test nor its neighbours see a stale cached value.
    cronstable.cron.load_index_html.cache_clear()

    def boom(*a, **k):
        raise ModuleNotFoundError("no package data")

    monkeypatch.setattr(importlib.resources, "files", boom)
    try:
        html = cronstable.cron.load_index_html()
    finally:
        cronstable.cron.load_index_html.cache_clear()
    assert "<" in html and len(html) > 0


def test_webloop_schedule_str_object_form():
    yaml = """
jobs:
  - name: obj
    command: echo hi
    schedule:
      minute: "*/5"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    job = cron.cron_jobs["obj"]
    # an object-form schedule is rebuilt into a crontab line via the shared
    # builder.
    s = cronstable.cron.schedule_str(job)
    assert isinstance(s, str) and s


def test_webloop_web_site_from_url_unix_unsupported(monkeypatch):
    monkeypatch.setattr(
        cronstable.cron.platform, "supports_unix_sockets", lambda: False
    )
    # a unix-socket listener on a platform that cannot serve one is a skippable
    # bad-config entry (ValueError), not a crash.
    with pytest.raises(ValueError):
        cronstable.cron.web_site_from_url(None, "unix:///tmp/whatever.sock")


def test_webloop_config_signature_missing_file_and_dir(tmp_path):
    cron = cronstable.cron.Cron(None)
    # a vanished file collapses to the (path, None, None) sentinel so a deletion
    # still registers as a change.
    missing = str(tmp_path / "gone.yaml")
    sig = cron._config_signature(frozenset([missing]))
    assert sig == ((missing, None, None),)
    # a directory config source folds its own mtime in as well.
    cron.config_arg = str(tmp_path)
    sig2 = cron._config_signature(frozenset())
    assert any(part[0] == "\0dir" for part in sig2)


def test_webloop_config_signature_dir_stat_error(monkeypatch):
    # a directory config source whose own stat fails still records a sentinel
    # (the dir-vanished-mid-scan branch).
    cron = cronstable.cron.Cron(None)
    cron.config_arg = "some-dir"
    monkeypatch.setattr(cronstable.cron.os.path, "isdir", lambda p: True)

    def raising_stat(path, *a, **k):
        raise OSError("boom")

    monkeypatch.setattr(cronstable.cron.os, "stat", raising_stat)
    sig = cron._config_signature(frozenset())
    assert ("\0dir", None) in sig


def test_webloop_update_config_no_source_returns_empty():
    cron = cronstable.cron.Cron(None)
    cfg = cron.update_config()
    assert cfg.jobs == []
    assert cfg.web_config is None


@pytest.mark.asyncio
async def test_webloop_run_skips_subminute_housekeeping(
    monkeypatch, run_cron
):
    # a second-level job forces per-second ticking, so after the first pass the
    # once-a-minute housekeeping is SKIPPED on subsequent same-minute ticks (the
    # frozen clock keeps now_minute constant). The seconds (5/15) never include
    # the frozen :00, so the job itself never actually spawns.
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.001
    )
    cron = cronstable.cron.Cron(None, config_yaml=_SUBMINUTE_NOFIRE)
    assert cron._needs_subminute() is True
    task = run_cron(cron)
    await _wait_until(lambda: cron._last_housekeeping_minute is not None)
    # let it iterate several more times within the same frozen minute
    await asyncio.sleep(0.05)
    assert not task.done()


@pytest.mark.asyncio
async def test_webloop_run_shutdown_teardown(tmp_path, monkeypatch, run_cron):
    # drive run()'s graceful-shutdown teardown across the observability overlay,
    # the slot renewers / catch-up / slot-pursuit task pools, the state-backend
    # block (with an empty pending-write set) and the web runner cleanup.
    monkeypatch.setattr("cronstable.cron.next_sleep_interval", lambda *a: 30)
    cron = cronstable.cron.Cron(None)
    task = run_cron(cron)

    stopped = {"mesh": False}

    # let the loop finish its first housekeeping pass and park on the long
    # sleep, then inject the teardown-path fixtures.
    await asyncio.sleep(0.2)

    await cron.start_stop_state(
        _state_cfg("state:\n  path: {}\n".format(tmp_path))
    )
    assert cron.state_backend is not None

    # drain any writes the backend startup queued, then neutralise
    # _track_state_write so the final counter snapshot does not repopulate
    # the pending set: the shutdown flush must see it EMPTY.
    pend = list(cron._pending_state_writes)
    if pend:
        await asyncio.gather(*pend, return_exceptions=True)
    monkeypatch.setattr(
        cron, "_track_state_write", lambda coro: coro.close()
    )

    class Mesh:
        async def stop(self):
            stopped["mesh"] = True

    cron.observability_mesh = Mesh()

    class Runner:
        def __init__(self):
            self.cleaned = False

        async def cleanup(self):
            self.cleaned = True

    cron.web_runner = Runner()

    renewer = asyncio.create_task(asyncio.sleep(100))
    cron._slot_renewers["s"] = renewer
    catchup = asyncio.create_task(asyncio.sleep(100))
    cron._catchup_tasks.add(catchup)
    pursuit = asyncio.create_task(asyncio.sleep(100))
    cron._slot_pursuits["p"] = pursuit
    # a Replace cancel still in its tail is joined, never cancelled
    replace_cancel = asyncio.create_task(asyncio.sleep(0.05))
    cron._replace_cancel_tasks.add(replace_cancel)

    cron.signal_shutdown()
    await asyncio.wait_for(task, timeout=5)

    await asyncio.gather(
        renewer, catchup, pursuit, return_exceptions=True
    )
    assert replace_cancel.done() and not replace_cancel.cancelled()
    assert stopped["mesh"] is True
    assert cron.observability_mesh is None
    assert cron.web_runner.cleaned is True
    assert renewer.cancelled()
    assert catchup.cancelled()
    assert pursuit.cancelled()
    assert cron._slot_renewers == {}


_LOGGING_CFG = """
jobs:
  - name: a
    command: echo hi
    schedule: "0 0 * * *"
logging:
    version: 1
"""


@pytest.mark.asyncio
async def test_webloop_run_applies_logging_config(
    tmp_path, monkeypatch, run_cron
):
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.01
    )
    applied = []
    monkeypatch.setattr(
        "cronstable.cron.logging.config.dictConfig",
        lambda cfg: applied.append(cfg),
    )
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LOGGING_CFG)
    cron = cronstable.cron.Cron(str(cfg))
    run_cron(cron)
    await _wait_until(lambda: bool(applied))
    assert applied[0] == {"version": 1}


@pytest.mark.asyncio
async def test_webloop_run_survives_logging_config_error(
    tmp_path, monkeypatch, run_cron
):
    monkeypatch.setattr(
        "cronstable.cron.next_sleep_interval", lambda *a: 0.01
    )
    attempts = []

    def boom(cfg):
        attempts.append(cfg)
        raise ValueError("bad logging config")

    monkeypatch.setattr("cronstable.cron.logging.config.dictConfig", boom)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_LOGGING_CFG)
    cron = cronstable.cron.Cron(str(cfg))
    task = run_cron(cron)
    await _wait_until(lambda: len(attempts) >= 1)
    # a broken logging section is logged and the daemon keeps running.
    assert not task.done()


@pytest.mark.asyncio
async def test_webloop_web_get_version():
    import cronstable.version

    cron = _cron(TWO_JOBS)
    resp = await cron._web_get_version(Req())
    assert resp.text == cronstable.version.version


@pytest.mark.asyncio
async def test_webloop_web_status_text_running_and_disabled():
    from types import SimpleNamespace

    cron = _cron(TWO_JOBS)
    cron.running_jobs["alpha"] = [
        SimpleNamespace(proc=SimpleNamespace(pid=4321))
    ]

    # no Accept header -> plain-text renderer
    resp = await cron._web_get_status(Req())
    assert "alpha: running (pid: 4321)" in resp.text
    assert "beta: disabled" in resp.text


@pytest.mark.asyncio
async def test_webloop_schedule_why_reboot_with_pause(monkeypatch):
    from types import SimpleNamespace

    yaml = """
jobs:
  - name: boot
    command: echo hi
    schedule: "@reboot"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    pause = SimpleNamespace(
        until=DT(2020, 1, 1, tzinfo=UTC), by="op", note=None
    )
    monkeypatch.setattr(cron, "_pause_active", lambda name, now=None: pause)
    payload = cron.schedule_why_payload("boot", "2020-01-01T00:00:00")
    assert payload is not None
    assert payload["reboot"] is True
    # the active pause is surfaced as a note on the @reboot payload.
    assert any(n["code"] == "paused" for n in payload["notes"])


@pytest.mark.asyncio
async def test_webloop_schedule_why_no_previous_fire():
    yaml = """
jobs:
  - name: yr
    command: echo hi
    schedule: "0 0 1 1 * * 2035"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    payload = cron.schedule_why_payload("yr", "2020-06-15T12:00:00")
    assert payload is not None
    # a future-year schedule has no previous fire before the probe.
    assert payload["previous_fire"] is None
    assert payload["next_fire"] is not None


@pytest.mark.asyncio
async def test_webloop_schedule_why_previous_fire():
    yaml = """
jobs:
  - name: m
    command: echo hi
    schedule: "* * * * *"
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    payload = cron.schedule_why_payload("m", "2020-06-15T12:00:30")
    assert payload is not None
    assert payload["previous_fire"] is not None


def test_webloop_schedule_entries_includes_dag():
    yaml = """
dags:
  - name: sch
    schedule: "*/5 * * * *"
    tasks:
      - id: a
        command: x
  - name: nosched
    tasks:
      - id: a
        command: x
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    entries = cron._schedule_entries()
    # a DAG's schedule rides along as its synthetic dag:<name> entry; a DAG with
    # no schedule (nosched) contributes nothing.
    names = [e.name for e in entries]
    assert "dag:sch" in names
    assert "dag:nosched" not in names


@pytest.mark.asyncio
async def test_webloop_web_dag_run_and_xcom(monkeypatch):
    import json as _json

    from aiohttp import web

    cron = _cron(TWO_JOBS)
    req = Req(match={"name": "d", "run_key": "rk"})

    async def none_run(name, run_key):
        return None

    monkeypatch.setattr(cron._dag, "get_run", none_run)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_dag_run(req)

    async def some_run(name, run_key):
        return {"state": "ok"}

    monkeypatch.setattr(cron._dag, "get_run", some_run)
    resp = await cron._web_dag_run(req)
    assert _json.loads(resp.text)["state"] == "ok"

    async def none_xcom(name, run_key):
        return None

    monkeypatch.setattr(cron._dag, "xcom_for_run", none_xcom)
    with pytest.raises(web.HTTPNotFound):
        await cron._web_dag_xcom(req)

    async def some_xcom(name, run_key):
        return {"a": 1}

    monkeypatch.setattr(cron._dag, "xcom_for_run", some_xcom)
    resp = await cron._web_dag_xcom(req)
    assert _json.loads(resp.text)["a"] == 1


@pytest.mark.asyncio
async def test_webloop_web_dag_backfill_errors(monkeypatch):
    import json as _json

    from aiohttp import web

    cron = _cron(TWO_JOBS)

    # non-string from/to -> 400
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_dag_backfill(
            Req(match={"name": "d"}, body={"from": 1, "to": 2})
        )

    async def bad_backfill(name, start, end):
        return {"ok": False, "reason": "nope"}

    monkeypatch.setattr(cron._dag, "backfill", bad_backfill)
    with pytest.raises(web.HTTPBadRequest):
        await cron._web_dag_backfill(
            Req(
                match={"name": "d"},
                body={"from": "2020-01-01", "to": "2020-01-02"},
            )
        )

    async def ok_backfill(name, start, end):
        return {"ok": True, "runs": 2}

    monkeypatch.setattr(cron._dag, "backfill", ok_backfill)
    resp = await cron._web_dag_backfill(
        Req(
            match={"name": "d"},
            body={"from": "2020-01-01", "to": "2020-01-02"},
        )
    )
    assert _json.loads(resp.text)["runs"] == 2


@pytest.mark.asyncio
async def test_webloop_state_payloads_propagate_cancel():
    cron = cronstable.cron.Cron(None)

    class CancelBackend:
        async def inventory(self):
            raise asyncio.CancelledError()

        def view_dict(self):
            return {}

        def stats(self):
            return {}

        async def list_documents(self, ns):
            raise asyncio.CancelledError()

        async def list_records(self, stream, limit, newest_first):
            raise asyncio.CancelledError()

    cron.state_backend = CancelBackend()
    # a cancellation must propagate, never be swallowed by the degrade-to-empty
    # guard.
    with pytest.raises(asyncio.CancelledError):
        await cron.state_payload()
    with pytest.raises(asyncio.CancelledError):
        await cron.state_documents_payload("kv/x")
    with pytest.raises(asyncio.CancelledError):
        await cron.state_records_payload("s")


def test_webloop_tail_payload_with_cursor():
    out = JobOutputStream()
    for i in range(5):
        out.publish("stdout", "line{}\n".format(i))
    payload = cronstable.cron.Cron._tail_payload(out, 10, 2)
    # with a cursor, the lines AFTER that offset are returned (not the tail).
    assert payload["cursor"] == 5
    assert payload["truncated"] is False
    assert len(payload["lines"]) == 3


@pytest.mark.asyncio
async def test_webloop_pump_output_handles_disconnect():
    cron = _cron(TWO_JOBS)
    out = JobOutputStream()
    out.publish("stdout", "x\n")
    out.close()

    class FakeResp:
        async def write(self, data):
            raise ConnectionResetError()

    # a client that vanishes mid-write is swallowed; nothing escapes.
    await cron._pump_output(FakeResp(), out)


@pytest.mark.asyncio
async def test_webloop_web_job_logs_live_running():
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(TWO_JOBS)
    out = JobOutputStream()
    # a currently-running instance exposes its live output buffer.
    cron.running_jobs["alpha"] = [SimpleNamespace(output=out)]

    app = web.Application()
    app.router.add_get("/jobs/{name}/logs", cron._web_job_logs)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/jobs/alpha/logs")
        assert resp.status == 200
        # let the handler subscribe and park, then publish a live line so it is
        # delivered over the queue (not just the replay buffer), and end it.
        await asyncio.sleep(0.05)
        out.publish("stdout", "live line\n")
        out.close()
        body = await resp.text()
    assert "live line" in body
    assert "event: end" in body


_DAG_LOGS_YAML = """
dags:
  - name: lin
    tasks:
      - id: a
        command: x
"""


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_unknown_dag():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(_DAG_LOGS_YAML)
    # no middleware here on purpose, as in test_web_job_logs_unknown_job:
    # the handler's own response is what reaches the wire.
    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/dags/nope/runs/rk/tasks/a/logs")
        assert resp.status == 404
        assert resp.content_type == "application/json"
        assert (await resp.json())["error"] == "dag 'nope' not found"


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_no_output():
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(_DAG_LOGS_YAML)
    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        # no running instance -> no reachable buffer.
        resp = await client.get("/dags/lin/runs/rk/tasks/a/logs")
        assert resp.status == 200
        body = await resp.text()
    assert "no-output" in body


@pytest.mark.asyncio
async def test_webloop_web_dag_task_logs_live_running():
    from types import SimpleNamespace

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    cron = _cron(_DAG_LOGS_YAML)
    out = JobOutputStream()
    dref = SimpleNamespace(run_key="rk", taskkey="a")
    # a running instance under the template name "<dag>.<task_id>" whose dag_ref
    # matches this run + instance key exposes its live buffer; a non-matching
    # sibling instance is skipped first (the loop-continue branch).
    cron.running_jobs["lin.a"] = [
        SimpleNamespace(
            output=JobOutputStream(),
            dag_ref=SimpleNamespace(run_key="other", taskkey="a"),
        ),
        SimpleNamespace(output=out, dag_ref=dref),
    ]

    app = web.Application()
    app.router.add_get(
        "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
        cron._web_dag_task_logs,
    )
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/dags/lin/runs/rk/tasks/a/logs")
        assert resp.status == 200
        await asyncio.sleep(0.05)
        out.publish("stdout", "task line\n")
        out.close()
        body = await resp.text()
    assert "task line" in body
    assert "event: end" in body


# ---------------------------------------------------------------------------
# Web-app lifecycle hardening from the 2026-08-02 review: an all-binds-failed
# start must retry on the next housekeeping pass, a teardown must end open
# SSE tails promptly, and a credential-less CORS preflight must reach the
# /mcp OPTIONS route through the bearer gate.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_app_retries_bind_after_all_listens_fail(start_web_app):
    # A predecessor (here: a plain socket) still holds the only listen port,
    # so every bind fails. web_config must NOT latch: the unchanged latch is
    # what makes the next housekeeping pass retry, and latching it left the
    # web API down for the daemon's life once the port freed.
    import socket

    blocker = socket.socket()
    try:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        port = blocker.getsockname()[1]
        listen = {"listen": ["http://127.0.0.1:{}".format(port)]}
        cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
        await start_web_app(cron, listen)
        assert cron.web_runner is None  # torn down, nothing half-bound
        assert cron.web_config is None  # not latched: a retry will run
        blocker.close()  # the old holder exits; the port frees
        await cron.start_stop_web_app(listen)  # the next pass
        assert cron.web_runner is not None
        assert cron.web_runner.addresses
    finally:
        blocker.close()


@pytest.mark.asyncio
async def test_web_teardown_ends_open_sse_tails_promptly(
    monkeypatch, start_web_app
):
    # An SSE log tail never finishes on its own: site.stop() only closes the
    # listening socket and the 15s keep-alive pings keep succeeding on the
    # established connection. Without the on_shutdown drain, a teardown
    # blocked on aiohttp's 60s in-flight-handler wait, freezing the
    # housekeeping loop (and with it every job launch) for the duration.
    import aiohttp

    from cronstable.job import JobOutputStream

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    live = JobOutputStream()
    live.publish("stdout", "hello")  # replay content the client can sync on
    monkeypatch.setattr(cron, "_job_output", lambda name: live)
    await start_web_app(cron, {"listen": ["http://127.0.0.1:0"]})
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    async with aiohttp.ClientSession() as session:
        resp = await session.get(base + "/jobs/alpha/logs")
        # the replayed frame proves the tail handler is up and
        # subscribed (frame shape: "event: <name>" then "data: <line>")
        event_line = await resp.content.readline()
        data_line = await resp.content.readline()
        assert event_line.startswith(b"event:")
        assert b"hello" in data_line
        # teardown with the tail still open must complete promptly, not
        # after aiohttp's 60s shutdown timeout.
        await asyncio.wait_for(cron.start_stop_web_app(None), timeout=5)
        # the handler ended the stream via the end-of-output path
        rest = await resp.content.read()
        assert b"event: end" in rest


@pytest.mark.asyncio
async def test_cors_preflight_reaches_mcp_options_through_auth(start_web_app):
    # A browser preflight carries no Authorization by the Fetch standard, so
    # the bearer gate must pass it through to the /mcp OPTIONS route, which
    # enforces mcp.allowedOrigins itself; the POST that follows still
    # authenticates normally.
    import aiohttp

    from cronstable.config import _build_mcp_config

    cron = cronstable.cron.Cron(None, config_yaml=_WEB_ONE_JOB)
    await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
        },
        _build_mcp_config(
            {
                "enabled": True,
                "allowedOrigins": ["https://inspector.example"],
            }
        ),
    )
    port = cron.web_runner.addresses[0][1]
    base = "http://127.0.0.1:{}".format(port)
    preflight = {
        "Origin": "https://inspector.example",
        "Access-Control-Request-Method": "POST",
    }
    async with aiohttp.ClientSession() as session:
        async with session.options(base + "/mcp", headers=preflight) as r:
            assert r.status == 204
            assert (
                r.headers["Access-Control-Allow-Origin"]
                == "https://inspector.example"
            )
            assert (
                "Authorization" in (r.headers["Access-Control-Allow-Headers"])
            )
        # a foreign origin's preflight is refused by the route itself
        async with session.options(
            base + "/mcp",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "POST",
            },
        ) as r:
            assert r.status == 403
        # the carve-out is preflights only: a bare OPTIONS keeps the 401
        async with session.options(base + "/mcp") as r:
            assert r.status == 401
        # and the actual POST still requires the bearer
        async with session.post(base + "/mcp", json={}) as r:
            assert r.status == 401


# --- the memo-busting mutator funnels and the bounded boot scan -------------


def test_payload_mutations_funnel_through_memo_busting_helpers():
    # The pin that makes the next direct mutation unshippable without a
    # bust: every write to the payload-feeding structures must go through
    # the funnel helpers, which bust the response memos. The scan keys on
    # attribute names so it also catches self._cron.running_jobs; it is a
    # tripwire for the easy regression (a direct write at a new site), not
    # a proof, since aliasing through a local escapes it and only the
    # allowlisted funnels use that shape.
    import ast

    tracked = {"_paused", "running_jobs", "run_history", "last_run"}
    mutators = {
        "append",
        "appendleft",
        "pop",
        "popitem",
        "clear",
        "update",
        "setdefault",
        "remove",
        "extend",
        "insert",
    }
    allowed = {
        "cron.py": {
            "__init__",
            "_apply_reload",
            "_set_pause",
            "_clear_pause",
            "_add_running_instance",
            "_remove_running_instance",
            "_install_run_info",
        },
        "dagrun.py": set(),
    }

    def is_tracked(node):
        # the structure itself (obj._paused) or a subscript over it
        # (obj.running_jobs[name])
        if isinstance(node, ast.Subscript):
            node = node.value
        return isinstance(node, ast.Attribute) and node.attr in tracked

    offenders = []
    src_dir = Path(cronstable.cron.__file__).parent
    for fname in ("cron.py", "dagrun.py"):
        tree = ast.parse((src_dir / fname).read_text(encoding="utf-8"))

        def walk(node, stack, fname=fname):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                stack = stack + [node.name]
            hit = False
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                targets = (
                    node.targets
                    if isinstance(node, ast.Assign)
                    else [node.target]
                )
                hit = any(is_tracked(t) for t in targets)
            elif isinstance(node, ast.Delete):
                hit = any(
                    isinstance(t, ast.Subscript) and is_tracked(t)
                    for t in node.targets
                )
            elif isinstance(node, ast.Call):
                func = node.func
                hit = (
                    isinstance(func, ast.Attribute)
                    and func.attr in mutators
                    and is_tracked(func.value)
                )
            if hit:
                enclosing = stack[-1] if stack else "<module>"
                if enclosing not in allowed[fname]:
                    offenders.append((fname, node.lineno, enclosing))
            for child in ast.iter_child_nodes(node):
                walk(child, stack, fname)

        walk(tree, [])

    assert offenders == []


@pytest.mark.asyncio
async def test_pause_set_and_resume_bust_via_funnel():
    # both halves of the pause lifecycle render immediately (each op busts
    # at least once), while a resume of a not-paused job changes no
    # payload and adds no bust (the _clear_pause no-change rule).
    cron = cronstable.cron.Cron(None, config_yaml=JOB_THAT_SUCCEEDS)
    busts = []
    real_bust = cron._bust_response_memos

    def counting_bust():
        busts.append(1)
        real_bust()

    cron._bust_response_memos = counting_bust

    await cron.pause_job_by_name("test", duration=60)
    assert busts
    after_pause = len(busts)

    await cron.resume_job_by_name("test")
    assert len(busts) > after_pause
    after_resume = len(busts)

    await cron.resume_job_by_name("test")  # not paused: nothing dropped
    assert len(busts) == after_resume


@pytest.mark.asyncio
async def test_bounded_boot_scan_partitions_and_tallies():
    # the pool contract: worker count is min(pool bound, items), the
    # shared iterator hands each item to exactly one worker, and the tally
    # counts exactly the "counted" outcomes.
    cron = cronstable.cron.Cron(None)
    items = [("j%02d" % i, None) for i in range(20)]
    pool = cronstable.cron._REHYDRATE_CONCURRENCY
    calls = []
    parked = asyncio.Event()
    release = asyncio.Event()

    async def step(name, _job):
        calls.append(name)
        if len(calls) >= pool:
            parked.set()
        await release.wait()
        return "counted" if int(name[1:]) % 2 == 0 else None

    scan = asyncio.ensure_future(
        cron._bounded_boot_scan(items, step, "unused %s")
    )
    try:
        # every worker draws one item then parks, so the plateau below is
        # exactly the pool size: no more steps can start until the release.
        await asyncio.wait_for(parked.wait(), timeout=5)
        assert len(calls) == min(pool, len(items))
    finally:
        release.set()
    counted = await scan
    assert sorted(calls) == [name for name, _ in items]  # each drawn once
    assert counted == 10  # the even indices


@pytest.mark.asyncio
async def test_bounded_boot_scan_timeout_warns_once_and_aborts(caplog):
    import logging

    cron = cronstable.cron.Cron(None)
    items = [("j%02d" % i, None) for i in range(20)]
    calls = []

    async def step(name, _job):
        # no await before the return: the first worker aborts the pass
        # synchronously, before any other worker draws an item, so the
        # single-call assert below is deterministic
        calls.append(name)
        return "timeout"

    with caplog.at_level(logging.WARNING, logger="cronstable"):
        counted = await cron._bounded_boot_scan(
            items, step, "state: boot scan timed out reading %s"
        )
    assert calls == ["j00"]  # the abort left the other 19 items undrawn
    warned = [
        r
        for r in caplog.records
        if "boot scan timed out reading" in r.message
    ]
    assert len(warned) == 1
    assert counted == 0


@pytest.mark.asyncio
async def test_bounded_boot_scan_skips_a_job_whose_step_raises(caplog):
    # asyncio.gather without return_exceptions hands the FIRST exception
    # to the awaiter and leaves the siblings running, so an unexpected
    # error out of one step used to abort the boot tail (dag reconcile,
    # retry re-arm, the job API start) while the other workers went on
    # mutating scheduler state behind it. One bad job is now a logged
    # skip: every other item is still drawn, and the pass still returns
    # its tally.
    import logging

    cron = cronstable.cron.Cron(None)
    items = [("j%02d" % i, None) for i in range(20)]
    calls = []

    async def step(name, _job):
        calls.append(name)
        if name == "j03":
            raise ValueError("a corrupt index entry")
        return "counted"

    with caplog.at_level(logging.ERROR, logger="cronstable"):
        counted = await cron._bounded_boot_scan(items, step, "unused %s")
    assert sorted(calls) == [name for name, _ in items]  # nothing skipped
    assert counted == 19  # every item but the raiser
    logged = [r for r in caplog.records if "boot scan: job" in r.message]
    assert len(logged) == 1
    assert "j03" in logged[0].getMessage()


def test_webloop_schedule_why_local_clock_gap(monkeypatch):
    # a utc: false job is explained in LOCAL_ZONE: the probe carries the
    # host's offset, a spring-gap label gets the DST note, and the next
    # fire is the following day's, on the new offset
    from tests._helpers import _pin_host_zone

    _pin_host_zone(monkeypatch, "America/New_York")
    yaml = """
jobs:
  - name: loc
    command: echo hi
    schedule: "30 2 * * *"
    utc: false
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)
    payload = cron.schedule_why_payload("loc", "2027-03-14T02:30:00")
    assert payload is not None
    assert payload["timezone"] == "local"
    assert payload["matches"] is True
    assert payload["at_in_zone"] == "2027-03-14T02:30:00-05:00"
    assert [n["code"] for n in payload["notes"]] == ["dst-skipped-time"]
    assert "in local" in payload["notes"][0]["message"]
    assert payload["next_fire"] == "2027-03-15T02:30:00-04:00"
    assert payload["previous_fire"] == "2027-03-13T02:30:00-05:00"


def test_webloop_schedule_why_local_clock_outside_the_c_library_range(
    monkeypatch,
):
    # with the host clock pinned to the MS CRT's shape, a probe before
    # 1970 and a previous fire on the engine's 1970 floor still answer
    # the payload the America/New_York twin gives
    from tests._helpers import _pin_host_zone

    _pin_host_zone(monkeypatch, "America/New_York", years=(1971, 2999))
    yaml = """
jobs:
  - name: loc
    command: echo hi
    schedule: "30 2 * * *"
    utc: false
  - name: ny
    command: echo hi
    schedule: "30 2 * * *"
    timezone: America/New_York
  - name: seventy
    command: echo hi
    schedule: "0 0 1 1 * 1970"
    utc: false
  - name: seventy_ny
    command: echo hi
    schedule: "0 0 1 1 * 1970"
    timezone: America/New_York
"""
    cron = cronstable.cron.Cron(None, config_yaml=yaml)

    def twins(local, zoned, at):
        mine = cron.schedule_why_payload(local, at)
        twin = cron.schedule_why_payload(zoned, at)
        assert mine is not None and twin is not None
        assert mine["timezone"] == "local"
        for key in ("job", "timezone"):
            del mine[key], twin[key]
        assert mine == twin
        return mine

    early = twins("loc", "ny", "1969-06-01T00:00:00")
    assert early["at_in_zone"] == "1969-06-01T00:00:00-04:00"
    assert early["previous_fire"] is None
    assert early["next_fire"] == "1969-06-01T02:30:00-04:00"
    epoch = twins("loc", "ny", "1970-01-01T00:00:00Z")
    assert epoch["at_in_zone"] == "1969-12-31T19:00:00-05:00"
    assert epoch["next_fire"] == "1970-01-01T02:30:00-05:00"
    floor = twins("seventy", "seventy_ny", "2027-01-01T00:00:00")
    assert floor["previous_fire"] == "1970-01-01T00:00:00-05:00"
    assert floor["next_fire"] is None
