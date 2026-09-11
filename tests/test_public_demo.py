"""The demo gateway grants sample actions without changing daemon scopes."""

import asyncio
import gzip
import importlib.util
import json
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from yarl import URL

from cronstable.config import ConfigError, parse_config_string
from cronstable.cron import WEB_ROUTES, Cron, _error_envelope_middleware
from tests._configs import DISABLED_JOB

DEMO = Path(__file__).parents[1] / "example/demo-instance"
spec = importlib.util.spec_from_file_location(
    "demo_gateway", DEMO / "gateway.py"
)
gateway_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gateway_module)
DemoGateway = gateway_module.DemoGateway


@pytest.fixture
async def demo_http():
    now = [100.0]
    seen = []
    state = {
        "rows": [
            {
                "name": "pipeline",
                "runCounts": {"success": 600},
                "totalRuns": 600,
            }
        ],
        "runs": [],
        "unchanged": True,
        "stream_release": asyncio.Event(),
    }
    tokens = Cron._resolve_web_tokens(
        {
            "authTokens": [
                {"value": "public", "scopes": ["view"]},
                {"value": "private", "scopes": ["control", "approve"]},
            ]
        }
    )
    cron = Cron(None, config_yaml=DISABLED_JOB)
    cron.web_config = {}

    async def handler(request):
        seen.append(
            (
                request.method,
                request.path,
                dict(request.headers),
                dict(request.query),
            )
        )
        if request.path == "/whoami":
            return await cron._web_whoami(request)
        if request.path == "/dags":
            return web.json_response(state["rows"])
        if request.path == "/pools":
            return web.json_response([])
        if request.path == "/dags/pipeline/runs":
            return web.json_response({"runs": state["runs"]})
        if request.path == "/":
            return web.Response(
                text="<html><body>dashboard</body></html>",
                content_type="text/html",
                headers={"ETag": '"ui"'},
            )
        if request.path.endswith("/logs"):
            response = web.StreamResponse(
                headers={"Content-Type": "text/event-stream"}
            )
            await response.prepare(request)
            await response.write(b"data: first\n\n")
            await state["stream_release"].wait()
            await response.write(b"data: last\n\n")
            return response
        if request.path == "/version":
            return web.Response(
                status=302, headers={"Location": "http://untrusted.example"}
            )
        if request.path == "/summary":
            if request.headers.get("If-None-Match") == '"summary"':
                return web.Response(status=304, headers={"ETag": '"summary"'})
            return web.Response(
                body=gzip.compress(b'{"ok":true}'),
                headers={
                    "Content-Encoding": "gzip",
                    "Content-Type": "application/json",
                    "ETag": '"summary"',
                    "Set-Cookie": "internal=private",
                    "Connection": "keep-alive",
                },
            )
        body = await request.json() if request.can_read_body else {}
        return web.json_response({"body": body})

    upstream = web.Application(
        middlewares=[
            _error_envelope_middleware,
            Cron._make_auth_middleware(
                tokens, anonymous_scopes=frozenset({"view"})
            ),
        ]
    )
    for method, route, _, _ in WEB_ROUTES:
        upstream.router.add_route(method, route, handler)
    async with TestClient(TestServer(upstream)) as daemon:
        gateway = DemoGateway(
            str(daemon.make_url("/")),
            {"jobs": ["sample"], "dags": ["pipeline"]},
            "public",
            "private",
            origins={"https://demo.example"},
            clock=lambda: now[0],
            unchanged=lambda: state["unchanged"],
        )
        async with TestClient(TestServer(gateway.application())) as client:
            yield client, gateway, now, state, seen, daemon
        state["stream_release"].set()


async def test_dashboard_can_poll_resource_pools(demo_http):
    client, gateway, now, state, seen, daemon = demo_http
    response = await client.get("/pools")
    assert response.status == 200
    assert await response.json() == []
    assert seen[-1][1] == "/pools"
    assert seen[-1][2]["Authorization"] == "Bearer public"


@pytest.mark.parametrize("token", [None, "public"])
async def test_gateway_actions_leave_direct_daemon_viewers_read_only(
    demo_http, token
):
    client, _, _, _, seen, daemon = demo_http
    headers = {"Authorization": "Bearer " + token} if token else {}
    direct = await daemon.post("/jobs/sample/start", headers=headers)
    assert direct.status == 403
    response = await client.get("/whoami", headers=headers)
    body = await response.json()
    assert body["scopes"] == ["approve", "control", "view"]
    assert body["allScopes"] is False
    assert body["authenticated"] is bool(token)
    assert body["label"] == ("public-demo-viewer" if token else "anonymous")
    response = await client.post("/jobs/sample/start", headers=headers)
    assert response.status == 200
    assert "private" not in await response.text()
    assert seen[-1][2]["Authorization"] == "Bearer private"
    direct = await daemon.get("/whoami", headers=headers)
    assert (await direct.json())["scopes"] == ["view"]


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/shutdown"),
        ("POST", "/mcp"),
        ("GET", "/mcp"),
        ("GET", "/push/devices"),
        ("POST", "/push/devices"),
        ("DELETE", "/push/devices/device"),
        ("POST", "/push/devices/device/test"),
        ("POST", "/dags/pipeline/backfill"),
        ("POST", "/jobs/operator/start"),
        ("POST", "/jobs/operator/cancel"),
        ("POST", "/jobs/operator/pause"),
        ("POST", "/dags/private/trigger"),
        ("POST", "/dags/private/runs/r/tasks/gate/decision"),
        ("GET", "/future-endpoint"),
        ("PUT", "/jobs/sample/start"),
    ],
)
@pytest.mark.parametrize("token", [None, "public"])
async def test_private_and_unlisted_routes_never_reach_daemon(
    demo_http, method, path, token
):
    client, _, _, _, seen, _ = demo_http
    headers = {"Authorization": "Bearer " + token} if token else {}
    response = await client.request(method, path, headers=headers)
    assert response.status in {403, 404, 405}
    assert "error" in await response.json()
    assert not seen


@pytest.mark.parametrize(
    "headers,query",
    [
        ({"Authorization": "Bearer wrong"}, ""),
        ({"Authorization": "Bearer private"}, ""),
        ({"Authorization": "Basic public"}, ""),
        ({"Authorization": ""}, ""),
        ({}, "?token="),
        ({}, "?token=wrong"),
        ({"Authorization": "Bearer public"}, "?token=wrong"),
        ({}, "?token=public&token=wrong"),
    ],
)
async def test_presented_bad_credentials_never_get_public_grant(
    demo_http, headers, query
):
    client, _, _, _, seen, _ = demo_http
    response = await client.post("/jobs/sample/start" + query, headers=headers)
    assert response.status == 401
    assert not seen


async def test_origin_check_ignores_forged_forwarded_headers(demo_http):
    client, _, _, _, seen, _ = demo_http
    response = await client.post(
        "/jobs/sample/start",
        headers={
            "Origin": "https://unrelated.example",
            "Host": "unrelated.example",
            "X-Forwarded-Host": "demo.example",
            "X-Forwarded-Proto": "https",
        },
    )
    assert response.status == 403
    assert not seen
    response = await client.post(
        "/jobs/sample/start", headers={"Origin": "https://demo.example"}
    )
    assert response.status == 200
    assert "Origin" not in seen[-1][2]
    assert "X-Forwarded-Host" not in seen[-1][2]


async def test_query_credentials_are_stripped_and_mutation_queries_discarded(
    demo_http,
):
    client, _, _, _, seen, _ = demo_http
    response = await client.get("/jobs/sample/runs?token=public&limit=2")
    assert response.status == 200
    assert seen[-1][3] == {"limit": "2"}
    assert seen[-1][2]["Authorization"] == "Bearer public"
    response = await client.post("/jobs/sample/start?by=attacker&token=public")
    assert response.status == 200
    assert seen[-1][3] == {}


@pytest.mark.parametrize(
    "path",
    [
        "/jobs/sample%2F..%2Foperator/start",
        "/jobs/%252e%252e/start",
        "/dags/pipeline/runs/%2e%2e/tasks/gate/decision",
    ],
)
async def test_encoded_segments_cannot_escape_the_resource_allowlist(
    demo_http, path
):
    client, _, _, _, seen, _ = demo_http
    response = await client.post(URL(path, encoded=True))
    assert response.status in {403, 404}
    assert not seen


async def test_shared_start_budget_keeps_cancellation_available(demo_http):
    client, _, now, _, _, _ = demo_http
    assert (await client.post("/jobs/sample/start")).status == 200
    now[0] += 1
    response = await client.post(
        "/dags/pipeline/trigger",
        headers={
            "Authorization": "Bearer public",
            "X-Forwarded-For": "203.0.113.9",
        },
    )
    assert response.status == 429
    assert response.headers["Retry-After"] == "9"
    assert (await client.post("/jobs/sample/cancel")).status == 200
    now[0] += 9
    assert (await client.post("/dags/pipeline/trigger")).status == 200


async def test_parallel_starts_admit_only_one(demo_http):
    client, _, _, _, seen, _ = demo_http
    responses = await asyncio.gather(
        *(client.post("/jobs/sample/start") for _ in range(8))
    )
    assert sorted(r.status for r in responses) == [200] + [429] * 7
    assert len(seen) == 1


async def test_full_histogram_blocks_old_active_runs_after_gateway_restart(
    demo_http,
):
    client, _, _, state, seen, _ = demo_http
    state["rows"][0].update(
        runCounts={"success": 600, "running": 1}, totalRuns=601
    )
    state["runs"] = [{"state": "success"}]  # newest window misses the old gate
    response = await client.post("/dags/pipeline/trigger")
    assert response.status == 409
    assert [s[1] for s in seen] == ["/dags"]


@pytest.mark.parametrize(
    "runs,status", [([], 200), ([{"state": "success"}], 503)]
)
async def test_missing_rollup_needs_authoritative_empty_history(
    demo_http, runs, status
):
    client, _, _, state, seen, _ = demo_http
    state["rows"] = [{"name": "pipeline"}]
    state["runs"] = runs
    response = await client.post("/dags/pipeline/trigger")
    assert response.status == status
    assert seen[1][1] == "/dags/pipeline/runs"
    assert seen[1][3] == {"limit": "1"}


async def test_config_change_stops_mutation_until_restart(demo_http):
    client, _, _, state, seen, _ = demo_http
    state["unchanged"] = False
    response = await client.post("/jobs/sample/start")
    assert response.status == 503
    assert not seen


async def test_public_actions_cannot_publish_arbitrary_notes(demo_http):
    client, _, now, _, _, _ = demo_http
    response = await client.post(
        "/jobs/sample/pause",
        json={
            "durationSeconds": 999999,
            "until": "2099-01-01T00:00:00Z",
            "note": "untrusted note",
            "by": "untrusted name",
        },
    )
    body = (await response.json())["body"]
    assert body["durationSeconds"] == 60
    assert body["by"] == "demo visitor"
    assert "untrusted" not in str(body)
    now[0] += 1
    response = await client.post(
        "/dags/pipeline/runs/r/tasks/gate/decision",
        json={"decision": "reject", "by": "untrusted name"},
    )
    assert (await response.json())["body"] == {
        "decision": "reject",
        "by": "demo visitor",
    }


@pytest.mark.parametrize(
    "data,status",
    [
        (b"x" * 2049, 413),
        (b"[]", 400),
        (b"not JSON", 400),
    ],
)
async def test_malformed_or_large_bodies_never_reach_actions(
    demo_http, data, status
):
    client, _, _, _, seen, _ = demo_http
    response = await client.post("/jobs/sample/start", data=data)
    assert response.status == status
    assert "error" in await response.json()
    assert not seen


async def test_chunked_body_is_bounded(demo_http):
    client, _, _, _, seen, _ = demo_http

    async def chunks():
        for _ in range(3):
            yield b"x" * 1024

    response = await client.post("/jobs/sample/start", data=chunks())
    assert response.status == 413
    assert not seen


async def test_stalled_body_releases_admission_slot(demo_http):
    client, gateway, now, _, seen, _ = demo_http
    release = asyncio.Event()

    async def chunks():
        yield b"{"
        await release.wait()
        yield b"}"

    response = await client.post("/jobs/sample/start", data=chunks())
    assert response.status == 408
    assert not gateway.lock.locked()
    assert not seen
    release.set()
    now[0] += 10
    assert (await client.post("/jobs/sample/start")).status == 200


async def test_dashboard_overlay_only_exists_at_gateway(demo_http):
    client, _, _, _, _, daemon = demo_http
    direct = await daemon.get("/")
    assert "demoNotice" not in await direct.text()
    response = await client.get("/", headers={"If-None-Match": '"ui"'})
    assert response.status == 200
    assert "demoNotice" in await response.text()
    assert "ETag" not in response.headers
    assert response.headers["Cache-Control"] == "no-store"


async def test_reads_preserve_gzip_and_conditional_cache_without_cookies(
    demo_http,
):
    client, _, _, _, _, _ = demo_http
    response = await client.get("/summary")
    assert await response.json() == {"ok": True}
    assert response.headers["ETag"] == '"summary"'
    assert "Set-Cookie" not in response.headers
    response = await client.get(
        "/summary", headers={"If-None-Match": '"summary"'}
    )
    assert response.status == 304
    assert await response.read() == b""


async def test_logs_stream_before_upstream_finishes(demo_http):
    client, _, _, state, seen, _ = demo_http
    response = await client.get(
        "/jobs/sample/logs?token=public", headers={"Last-Event-ID": "42"}
    )
    assert response.headers["Content-Type"] == "text/event-stream"
    assert (
        await asyncio.wait_for(response.content.readuntil(b"\n\n"), 1)
        == b"data: first\n\n"
    )
    assert seen[-1][3] == {}
    assert seen[-1][2]["Last-Event-ID"] == "42"
    state["stream_release"].set()
    assert await response.read() == b"data: last\n\n"


async def test_upstream_redirect_is_not_followed(demo_http):
    client, _, _, _, seen, _ = demo_http
    response = await client.get("/version")
    assert response.status == 502
    assert len(seen) == 1
    assert "Location" not in response.headers


CONFIG = """
defaults:
  concurrencyPolicy: Forbid
  executionTimeout: 10
jobs:
  - name: sample
    command: echo simulated
    schedule: "0 0 1 1 * 2020"
web:
  listen:
    - http://127.0.0.1:0
  authTokens:
    - value: public
      scopes:
        - view
    - value: private
      scopes:
        - control
        - approve
  anonymousScopes:
    - view
"""


@pytest.mark.parametrize(
    "old,new",
    [
        ("concurrencyPolicy: Forbid", "concurrencyPolicy: Allow"),
        ("executionTimeout: 10", "executionTimeout: 600"),
        ("name: sample", "name: missing"),
        ("  anonymousScopes:\n    - view\n", ""),
        (
            "    command: echo simulated",
            "    command: echo simulated\n    secrets:\n"
            "      - name: private\n        value: secret",
        ),
    ],
)
def test_gateway_validates_sample_config(tmp_path, old, new):
    cfg = tmp_path / "cronstable.yaml"
    cfg.write_text(CONFIG.replace(old, new))
    policy = tmp_path / "gateway.json"
    policy.write_text(json.dumps({"jobs": ["sample"], "dags": []}))
    with pytest.raises((ValueError, ConfigError)):
        gateway_module.load_policy(policy, cfg)


def test_example_is_standard_daemon_config_with_separate_bounded_policy():
    policy, unchanged = gateway_module.load_policy(
        DEMO / "gateway.json", DEMO / "cronstable.yaml"
    )
    assert "restore-drill" in policy["jobs"]
    assert "demo-operator" not in policy["jobs"]
    assert "cert-check" not in policy["jobs"]
    assert "firmware-rollout" in policy["dags"]
    assert unchanged()


def test_policy_edit_invalidates_gateway(tmp_path):
    cfg = tmp_path / "cronstable.yaml"
    cfg.write_text(CONFIG)
    policy = tmp_path / "gateway.json"
    policy.write_text(json.dumps({"jobs": ["sample"], "dags": []}))
    _, unchanged = gateway_module.load_policy(policy, cfg)
    assert unchanged()
    policy.write_text("{}\n")
    assert not unchanged()


async def test_gateway_uses_real_unmodified_daemon_listener(monkeypatch):
    cron = Cron(None, config_yaml=CONFIG)
    started = []

    async def start(name):
        started.append(name)

    monkeypatch.setattr(cron, "start_job_by_name", start)
    await cron.start_stop_web_app(parse_config_string(CONFIG, "").web_config)
    try:
        port = cron.web_runner.addresses[0][1]
        gateway = DemoGateway(
            f"http://127.0.0.1:{port}",
            {"jobs": ["sample"], "dags": []},
            "public",
            "private",
            origins={"https://demo.example"},
        )
        async with TestClient(TestServer(gateway.application())) as client:
            response = await client.post("/jobs/sample/start")
            assert response.status == 200
            assert started == ["sample"]
            response = await client.post("/shutdown")
            assert response.status == 404
            response = await client.get("/whoami")
            assert (await response.json())["scopes"] == ["control", "view"]
    finally:
        await cron.start_stop_web_app(None)
        await asyncio.sleep(0.25)
