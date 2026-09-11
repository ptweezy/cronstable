#!/usr/bin/env python3
"""Interactive access for this demo deployment, outside the shipped daemon.

Run one gateway per board. The upstream is a standard cronstable listener;
only this process holds the private credential used for visitor actions.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import math
import os
import time
from pathlib import Path
from urllib.parse import quote

import aiohttp
from aiohttp import web
from yarl import URL

# Explicit routes: a new daemon endpoint does not become public implicitly.
READ_ROUTES = (
    "/",
    "/version",
    "/job-set-id",
    "/cluster",
    "/fleet",
    "/node",
    "/node/history",
    "/status",
    "/summary",
    "/schedule/preview",
    "/schedule/pressure",
    "/schedule/duplicates",
    "/schedule/suggest",
    "/schedule/why",
    "/calendar.ics",
    "/jobs",
    "/activity",
    "/jobs/{name}",
    "/jobs/{name}/runs",
    "/jobs/{name}/calendar.ics",
    "/jobs/{name}/resources",
    "/jobs/{name}/trends",
    "/jobs/{name}/logs",
    "/dags",
    "/dags/{name}/runs",
    "/dags/{name}/runs/{run_key}",
    "/dags/{name}/runs/{run_key}/xcom",
    "/dags/{name}/runs/{run_key}/tasks/{taskkey}/logs",
    "/state",
    "/state/documents",
    "/state/records",
    "/whoami",
    "/metrics",
)
JOB_ACTIONS = tuple(
    "/jobs/{name}/" + x
    for x in (
        "start",
        "cancel",
        "pause",
        "resume",
    )
)
DAG_ACTIONS = (
    "/dags/{name}/trigger",
    "/dags/{name}/runs/{run_key}/tasks/{taskkey}/decision",
)
REQUEST_HEADERS = {
    "accept",
    "accept-encoding",
    "if-none-match",
    "if-modified-since",
    "last-event-id",
}
RESPONSE_HEADERS = {
    "content-type",
    "content-encoding",
    "cache-control",
    "etag",
    "last-modified",
    "vary",
    "retry-after",
    "content-security-policy",
    "x-content-type-options",
    "x-frame-options",
    "referrer-policy",
    "permissions-policy",
    "x-accel-buffering",
}


def error(kind, message, **kwargs):
    return kind(
        text=json.dumps({"error": message}),
        content_type="application/json",
        **kwargs,
    )


def file_stamp(path):
    stat = Path(path).stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns


def load_policy(policy_path, config_path):
    # Reuse the normal config parser as a library; no demo hooks are installed
    # into it. The gateway's own policy is a separate deployment file.
    from cronstable.config import parse_config_with_sources

    policy = json.loads(Path(policy_path).read_text())
    if not isinstance(policy, dict) or set(policy) != {"jobs", "dags"}:
        raise ValueError("gateway policy needs jobs and dags lists")
    for names in policy.values():
        if not isinstance(names, list) or any(
            not isinstance(n, str)
            or not n
            or n in {".", ".."}
            or any(c in n for c in "/\\%")
            for n in names
        ):
            raise ValueError("gateway resource names must be path segments")
    if not policy["jobs"] and not policy["dags"]:
        raise ValueError("gateway needs sample jobs or dags")
    config, sources = parse_config_with_sources(str(config_path))
    if config.cluster_config is not None:
        raise ValueError("the demo gateway requires a single-node board")
    webconf = config.web_config or {}
    if webconf.get("anonymousScopes") != ["view"] or not webconf.get(
        "authTokens"
    ):
        raise ValueError("the demo daemon must keep token authentication")
    jobs = {job.name: job for job in config.jobs}
    dags = {dag.name: dag for dag in config.dags}

    def bounded(job):
        if (
            job.secrets
            or job.executionTimeout is None
            or not (0 < job.executionTimeout <= 60)
        ):
            raise ValueError(
                f"sample {job.name!r} needs no secrets and a timeout <= 60s"
            )

    for name in policy["jobs"]:
        if name not in jobs:
            raise ValueError(f"unknown sample job {name!r}")
        job = jobs[name]
        if job.concurrencyPolicy not in {"Forbid", "Replace"}:
            raise ValueError(
                f"sample {name!r} needs Forbid/Replace concurrency"
            )
        bounded(job)
    for name in policy["dags"]:
        if name not in dags:
            raise ValueError(f"unknown sample workflow {name!r}")
        for task in dags[name].tasks:
            bounded(task.job_template)
    stamps = {p: file_stamp(p) for p in (*sources, str(policy_path))}

    def unchanged():
        try:
            return all(file_stamp(p) == stamp for p, stamp in stamps.items())
        except OSError:
            return False

    return policy, unchanged


class DemoGateway:
    def __init__(
        self,
        upstream,
        policy,
        view_token,
        operator_token,
        *,
        origins,
        unchanged=lambda: True,
        clock=time.monotonic,
    ):
        upstream = URL(upstream)
        if (
            upstream.scheme not in {"http", "https"}
            or not upstream.host
            or upstream.user is not None
            or upstream.path != "/"
            or upstream.query_string
            or upstream.fragment
        ):
            raise ValueError("upstream must be an HTTP(S) origin")
        if (
            not view_token
            or not operator_token
            or view_token == operator_token
            or any(c in view_token + operator_token for c in "\r\n")
        ):
            raise ValueError(
                "distinct, nonempty view and operator tokens required"
            )
        self.upstream = str(upstream).rstrip("/")
        self.jobs = frozenset(policy["jobs"])
        self.dags = frozenset(policy["dags"])
        self.view_token = view_token
        self.operator_token = operator_token
        self.origins = frozenset(origins)
        self.unchanged = unchanged
        self.clock = clock
        self.next_action = self.next_start = 0.0
        self.lock = asyncio.Lock()
        self.overlay = (
            Path(__file__).with_name("gateway-overlay.html").read_text()
        )

    def application(self):
        app = web.Application(middlewares=[self.guard], client_max_size=2048)
        app.cleanup_ctx.append(self.session_context)
        for route in READ_ROUTES:
            app.router.add_get(route, self.read)
        for route in JOB_ACTIONS + DAG_ACTIONS:
            app.router.add_post(route, self.act)
        return app

    async def session_context(self, app):
        async with aiohttp.ClientSession(
            auto_decompress=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            timeout=aiohttp.ClientTimeout(total=20, sock_connect=5),
        ) as self.session:
            yield

    @web.middleware
    async def guard(self, request, handler):
        try:
            # Never pass through an operator credential, even if supplied by
            # a visitor. Query credentials support EventSource and calendars;
            # strip them before forwarding and disable URL access logging.
            supplied = request.query.getall("token", [])
            if "Authorization" in request.headers:
                headers = request.headers.getall("Authorization")
                if len(headers) != 1 or not headers[0].startswith("Bearer "):
                    raise error(web.HTTPUnauthorized, "Invalid demo token.")
                supplied.append(headers[0][7:])
            if any(
                not hmac.compare_digest(t.encode(), self.view_token.encode())
                for t in supplied
            ):
                raise error(web.HTTPUnauthorized, "Invalid demo token.")
            request["demo_authenticated"] = bool(supplied)
            if request.method not in {"GET", "HEAD"}:
                origin = request.headers.get("Origin")
                if origin is not None and origin not in self.origins:
                    raise error(web.HTTPForbidden, "Origin not allowed.")
            for value in request.match_info.values():
                if value in {".", ".."} or any(c in value for c in "/\\%"):
                    raise error(web.HTTPForbidden, "Invalid resource path.")
            return await handler(request)
        except web.HTTPException as ex:
            if ex.content_type == "application/json":
                raise
            return web.json_response(
                {"error": "Not available in the public demo."},
                status=ex.status,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise error(
                web.HTTPBadGateway, "The demo daemon is unavailable."
            ) from None

    def path(self, request):
        route = request.match_info.route.resource.canonical
        return route.format(
            **{k: quote(v, safe="") for k, v in request.match_info.items()}
        )

    async def read(self, request):
        return await self.proxy(request)

    async def dag_busy(self, name):
        # /dags reports a full-history histogram, unlike the capped run list.
        # A missing rollup can also mean an unavailable store, so only an
        # authoritative empty run list proves a new board is clear to start.
        headers = {
            "Authorization": "Bearer " + self.view_token,
            "Accept-Encoding": "identity",
        }
        async with self.session.get(
            self.upstream + "/dags", headers=headers, allow_redirects=False
        ) as response:
            if response.status != 200:
                raise error(
                    web.HTTPServiceUnavailable, "Cannot check workflow state."
                )
            rows = await response.json()
        row = next((d for d in rows if d.get("name") == name), None)
        if row is None:
            raise error(
                web.HTTPServiceUnavailable, "Cannot check workflow state."
            )
        counts = row.get("runCounts")
        if (
            isinstance(counts, dict)
            and counts
            and all(type(n) is int and n >= 0 for n in counts.values())
            and sum(counts.values()) == row.get("totalRuns")
        ):
            return any(
                n
                for state, n in counts.items()
                if state not in {"success", "failed"}
            )
        path = "/dags/" + quote(name, safe="") + "/runs?limit=1"
        async with self.session.get(
            self.upstream + path, headers=headers, allow_redirects=False
        ) as response:
            if (
                response.status == 200
                and (await response.json()).get("runs") == []
            ):
                return False
        raise error(web.HTTPServiceUnavailable, "Cannot check workflow state.")

    async def act(self, request):
        route = request.match_info.route.resource.canonical
        name = request.match_info["name"]
        if not (
            (route in JOB_ACTIONS and name in self.jobs)
            or (route in DAG_ACTIONS and name in self.dags)
        ):
            raise error(
                web.HTTPForbidden, "Only selected samples can be controlled."
            )
        if not self.unchanged():
            raise error(
                web.HTTPServiceUnavailable,
                "Demo configuration changed; restart the gateway.",
            )
        starting = route.endswith(("/start", "/trigger"))
        now = self.clock()
        wait = max(self.next_action, self.next_start if starting else 0) - now
        if self.lock.locked() or wait > 0:
            raise error(
                web.HTTPTooManyRequests,
                "The shared demo is busy. Try again shortly.",
                headers={"Retry-After": str(max(1, math.ceil(wait)))},
            )
        # No await before reserving admission; stalled clients cannot queue
        # actions. Keep cancellation/approval separate from the start budget.
        self.next_action = now + 1
        async with self.lock:
            try:
                raw = await asyncio.wait_for(request.read(), timeout=5)
            except asyncio.TimeoutError:
                raise error(
                    web.HTTPRequestTimeout, "Demo request timed out."
                ) from None
            try:
                body = json.loads(raw) if raw else {}
            except (ValueError, UnicodeError):
                raise error(
                    web.HTTPBadRequest, "Request body is not valid JSON."
                ) from None
            if not isinstance(body, dict):
                raise error(
                    web.HTTPBadRequest, "Request body must be an object."
                )
            safe_body = {"by": "demo visitor"}
            if route.endswith("/decision"):
                if body.get("decision") not in ("approve", "reject"):
                    raise error(
                        web.HTTPBadRequest, "Choose approve or reject."
                    )
                safe_body["decision"] = body["decision"]
            elif route.endswith("/pause"):
                safe_body.update(
                    durationSeconds=60,
                    note="Demo pause; automatically resumes after one minute.",
                )
            if route.endswith("/trigger") and await self.dag_busy(name):
                raise error(
                    web.HTTPConflict,
                    "This demo workflow already has an active run.",
                )
            if starting:
                self.next_start = self.clock() + 10
            return await self.proxy(request, body=safe_body)

    async def proxy(self, request, body=None):
        path = self.path(request)
        special = path in {"/", "/whoami"}
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() in REQUEST_HEADERS and not special
        }
        headers["Authorization"] = "Bearer " + (
            self.operator_token if body is not None else self.view_token
        )
        if special:
            headers["Accept-Encoding"] = "identity"
        query = [(k, v) for k, v in request.query.items() if k != "token"]
        url = URL(self.upstream + path, encoded=True)
        if body is None:
            url = url.with_query(query)
        streaming = path.endswith("/logs")
        timeout = aiohttp.ClientTimeout(
            total=None if streaming else 20, sock_connect=5, sock_read=75
        )
        async with self.session.request(
            "GET" if special else request.method,
            url,
            headers=headers,
            json=body,
            allow_redirects=False,
            timeout=timeout,
        ) as upstream:
            if 300 <= upstream.status < 400 and upstream.status != 304:
                raise error(web.HTTPBadGateway, "Unexpected daemon redirect.")
            response_headers = {
                k: v
                for k, v in upstream.headers.items()
                if k.lower() in RESPONSE_HEADERS
            }
            if special and upstream.status == 200:
                for key in list(response_headers):
                    if key.lower() in {
                        "etag",
                        "content-encoding",
                        "last-modified",
                    }:
                        del response_headers[key]
                response_headers["Cache-Control"] = "no-store"
                if path == "/whoami":
                    payload = await upstream.json()
                    payload.update(
                        authenticated=request["demo_authenticated"],
                        label=(
                            "public-demo-viewer"
                            if request["demo_authenticated"]
                            else "anonymous"
                        ),
                        scopes=(
                            ["approve", "control", "view"]
                            if self.dags
                            else ["control", "view"]
                        ),
                        allScopes=False,
                    )
                    data = json.dumps(payload).encode()
                else:
                    data = (await upstream.read()).replace(
                        b"</body>", self.overlay.encode() + b"\n</body>"
                    )
                return web.Response(body=data, headers=response_headers)
            if not streaming:
                return web.Response(
                    status=upstream.status,
                    body=await upstream.read(),
                    headers=response_headers,
                )
            # Copy chunks as they arrive, including SSE heartbeats. A closed
            # downstream closes the upstream connection instead of buffering.
            response = web.StreamResponse(
                status=upstream.status, headers=response_headers
            )
            await response.prepare(request)
            try:
                async for chunk in upstream.content.iter_any():
                    await response.write(chunk)
                await response.write_eof()
            except (
                ConnectionError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ):
                response.force_close()
            return response


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    here = Path(__file__).resolve().parent
    parser.add_argument("--config", default=str(here / "cronstable.yaml"))
    parser.add_argument("--policy", default=str(here / "gateway.json"))
    parser.add_argument("--upstream", default="http://127.0.0.1:8080")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--origin", action="append", required=True)
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    policy, unchanged = load_policy(args.policy, args.config)
    gateway = DemoGateway(
        args.upstream,
        policy,
        os.environ.get(
            "CRONSTABLE_DEMO_VIEW_TOKEN", "cronstable-public-demo-view"
        ),
        os.environ.get("CRONSTABLE_DEMO_OPERATOR_TOKEN", ""),
        origins=args.origin,
        unchanged=unchanged,
    )
    if not args.validate:
        web.run_app(
            gateway.application(),
            host=args.host,
            port=args.port,
            access_log=None,
        )


if __name__ == "__main__":
    main()
