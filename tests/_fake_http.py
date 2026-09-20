"""Provide a shared HTTP server for the fake etcd and Kubernetes servers.

``FakeHttpServer`` runs aiohttp on a loopback socket bound to port 0. It
reads the assigned port, records requests, and responds through
:meth:`FakeHttpServer.respond`. Tests can queue faults with
:meth:`FakeHttpServer.inject`. Each fault overrides the next matching
request to simulate error statuses, redirects, invalid JSON, or a server
that accepts a connection but never responds.

``FakeClock`` controls server-side expiration in both fake servers, so
expiration tests do not need to wait for real time to pass.
"""

import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Any, Optional

from aiohttp import web

#: An address nothing listens on: port 1 is privileged and unassigned, so a
#: connect is refused at once. The "this member is down" endpoint.
DEAD_ENDPOINT = "http://127.0.0.1:1"

# Upper bound on how long a hanging fault holds a request open when the test
# forgets to stop the server; stop() releases it at once.
_HANG_CAP_SECONDS = 30.0


class FakeClock:
    """A manually advanced clock (seconds)."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass
class RecordedRequest:
    method: str
    path: str
    raw_path: str
    headers: dict[str, str]
    body: bytes
    json: Any = None


@dataclass
class _Fault:
    path: Optional[str]
    method: Optional[str]
    status: int
    body: bytes
    content_type: str
    headers: dict[str, str] = field(default_factory=dict)
    hang: bool = False
    remaining: int = 1


class FakeHttpServer:
    """A recording aiohttp server with a fault queue; see the module doc."""

    def __init__(self, ssl_context: Optional[ssl.SSLContext] = None) -> None:
        self._ssl_context = ssl_context
        self._runner: Optional[web.AppRunner] = None
        self._release_hangs = asyncio.Event()
        self._faults: list[_Fault] = []
        self.requests: list[RecordedRequest] = []
        self.port = 0

    # --- lifecycle --------------------------------------------------------

    async def start(self) -> "FakeHttpServer":
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        self._runner = web.AppRunner(app, shutdown_timeout=1.0)
        await self._runner.setup()
        site = web.TCPSite(
            self._runner, "127.0.0.1", 0, ssl_context=self._ssl_context
        )
        await site.start()
        self.port = self._runner.addresses[0][1]
        return self

    async def stop(self) -> None:
        self._release_hangs.set()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> "FakeHttpServer":
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    @property
    def url(self) -> str:
        scheme = "https" if self._ssl_context is not None else "http"
        return "{}://127.0.0.1:{}".format(scheme, self.port)

    # --- faults -----------------------------------------------------------

    def inject(
        self,
        *,
        path: Optional[str] = None,
        method: Optional[str] = None,
        status: int = 200,
        body: bytes = b"",
        content_type: str = "application/json",
        headers: Optional[dict[str, str]] = None,
        hang: bool = False,
        times: int = 1,
    ) -> None:
        """Respond to the next ``times`` matching requests with this fault.

        A ``None`` value for ``path`` or ``method`` matches any value. ``hang``
        keeps the request open until the server stops to simulate an
        unresponsive endpoint.
        """
        self._faults.append(
            _Fault(
                path=path,
                method=method,
                status=status,
                body=body,
                content_type=content_type,
                headers=dict(headers or {}),
                hang=hang,
                remaining=times,
            )
        )

    def _take_fault(self, method: str, path: str) -> Optional[_Fault]:
        for fault in self._faults:
            if fault.path is not None and fault.path != path:
                continue
            if fault.method is not None and fault.method != method:
                continue
            fault.remaining -= 1
            if fault.remaining <= 0:
                self._faults.remove(fault)
            return fault
        return None

    # --- request handling -------------------------------------------------

    def paths(self) -> list[str]:
        return [r.path for r in self.requests]

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        raw = await request.read()
        try:
            parsed = await request.json() if raw else None
        except ValueError:
            parsed = None
        recorded = RecordedRequest(
            method=request.method,
            path=request.path,
            raw_path=request.raw_path,
            headers=dict(request.headers),
            body=raw,
            json=parsed,
        )
        self.requests.append(recorded)
        fault = self._take_fault(request.method, request.path)
        if fault is not None:
            if fault.hang:
                try:
                    await asyncio.wait_for(
                        self._release_hangs.wait(), _HANG_CAP_SECONDS
                    )
                except asyncio.TimeoutError:
                    pass
                return web.Response(status=503)
            return web.Response(
                status=fault.status,
                body=fault.body,
                headers={"Content-Type": fault.content_type, **fault.headers},
            )
        return await self.respond(request, recorded)

    async def respond(
        self, request: web.Request, recorded: RecordedRequest
    ) -> web.StreamResponse:
        raise NotImplementedError


def server_ssl_context(
    material: dict[str, str], *, require_client_cert: bool = False
) -> ssl.SSLContext:
    """A server-side TLS context from ``tests._helpers._write_tls`` output.

    ``require_client_cert`` makes it mutual TLS: the client must present a
    certificate the same CA signed.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(material["cert"], material["key"])
    if require_client_cert:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=material["ca"])
    return ctx
