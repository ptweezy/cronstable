import asyncio
import logging
import time
import weakref
from typing import Any, Dict, Tuple

logger = logging.getLogger("statsd")

#: How long a pooled endpoint is reused before it is rebuilt.  UDP is
#: connectionless, so nothing about the socket expires, but the remote
#: address is resolved ONCE, when the endpoint is created, so an endpoint
#: kept forever would pin a rolling ``statsd.internal`` to whatever address
#: it had at daemon start.  A minute means at most one resolution per
#: minute per target, against one per datagram before pooling.
ENDPOINT_MAX_AGE_SECONDS = 60.0

#: Datagrams are dropped once this much unflushed data is queued on an
#: endpoint.  A pooled endpoint outlives the emit, so a send path that
#: stops draining no longer has its backlog reclaimed by the close that
#: used to follow every datagram; statsd is best-effort telemetry, so
#: shedding beats growing a queue nobody will drain.  Silently, because a
#: dropped datagram is the normal UDP outcome and a log line per shed
#: would be the very flood being shed.
MAX_QUEUED_BYTES = 1 << 20


class StatsdClientProtocol(asyncio.DatagramProtocol):
    """The datagram protocol behind one pooled statsd endpoint.

    ``message`` is the datagram to put on the wire as soon as the endpoint
    is up, so the send that had to open the endpoint costs no extra write;
    every later send goes straight through the transport.

    A real :class:`asyncio.DatagramProtocol`, not a duck type: a long-lived
    transport reaches its write-buffer high-water mark under a burst and
    calls ``pause_writing`` on the protocol, which a bare object does not
    have.  Per-datagram endpoints never lived long enough to hit that.
    """

    def __init__(self, message, loop):
        self.message = message
        self.loop = loop
        self.transport = None
        # Set when this endpoint can no longer be trusted: an ICMP error
        # came back for a previous datagram, or the transport went away.
        # The pool checks it before handing the endpoint out again, so a
        # statsd server that moved or died costs one lost datagram rather
        # than a permanently deaf socket.
        self.broken = False

    def connection_made(self, transport):
        self.transport = transport
        if self.message is not None:
            self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        pass

    def error_received(self, exc):
        # the format string needs a placeholder, otherwise logging raises a
        # TypeError and the actual exception detail is lost.
        logger.error("UDP error received: %s", exc)
        self.broken = True

    def connection_lost(self, exc):
        self.broken = True


#: One endpoint per (host, port), per event loop.  Weak keys so a torn-down
#: loop (tests build one per test) drops its endpoints instead of pinning
#: dead transports.  The values are the CREATION TASKS, not their results,
#: so concurrent emits to a cold target share a single
#: create_datagram_endpoint instead of racing to open several sockets and
#: leaking all but the last.
_ENDPOINTS: "weakref.WeakKeyDictionary[Any, Dict[Tuple[str, int], Any]]" = (
    weakref.WeakKeyDictionary()
)


async def _open_endpoint(loop, host, port, message):
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: StatsdClientProtocol(message, loop), remote_addr=(host, port)
    )
    return transport, protocol, loop.time()


def _unusable(task, now) -> bool:
    """Whether a finished endpoint task must be thrown away and reopened."""
    if task.cancelled() or task.exception() is not None:
        return True
    transport, protocol, created_at = task.result()
    return bool(
        protocol.broken
        or transport.is_closing()
        or now - created_at >= ENDPOINT_MAX_AGE_SECONDS
    )


def _discard(endpoints, key, task) -> None:
    del endpoints[key]
    if not task.cancelled() and task.exception() is None:
        task.result()[0].close()


async def _endpoint(host, port, message):
    """The pooled transport for ``(host, port)``, and whether ``message``
    already went out on it.

    Opening one endpoint per datagram cost a socket, a connect, a
    protocol/transport object graph and a close for every metric, and,
    for a non-numeric host, one ``getaddrinfo`` per datagram, dispatched to
    the shared default executor that the config reload and every other
    ``to_thread`` caller share.
    """
    loop = asyncio.get_running_loop()
    endpoints = _ENDPOINTS.get(loop)
    if endpoints is None:
        endpoints = {}
        _ENDPOINTS[loop] = endpoints
    key = (host, port)
    task = endpoints.get(key)
    if task is not None and task.done() and _unusable(task, loop.time()):
        _discard(endpoints, key, task)
        task = None
    mine = task is None
    if mine:
        task = loop.create_task(_open_endpoint(loop, host, port, message))
        endpoints[key] = task
    try:
        # shield: a caller cancelled while waiting must not tear down the
        # open that its siblings are waiting on.
        transport, _protocol, _created_at = await asyncio.shield(task)
    except BaseException:
        # A failed open (unresolvable host, no route) must not be cached,
        # or every later emit would replay the same failure without ever
        # retrying.
        if endpoints.get(key) is task:
            del endpoints[key]
        raise
    return transport, mine


async def send_to_statsd(host, port, message):
    transport, already_sent = await _endpoint(host, port, message)
    # Only the emit that OPENED the endpoint gets its datagram sent from
    # connection_made; siblings sharing that open, and every later emit,
    # write through the transport.
    if already_sent:
        return
    if transport.get_write_buffer_size() > MAX_QUEUED_BYTES:
        return
    transport.sendto(message.encode())


def close_endpoints() -> None:
    """Close every pooled endpoint of the running loop (daemon shutdown).

    Safe to call more than once and outside a loop; the next emit reopens
    whatever it needs.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    for task in list(_ENDPOINTS.pop(loop, {}).values()):
        if not task.done():
            task.cancel()
        elif not task.cancelled() and task.exception() is None:
            task.result()[0].close()


class StatsdJobMetricWriter:
    def __init__(self, host, port, prefix, job):
        self.host = host
        self.port = port
        # Strip statsd wire-format metacharacters (CR, LF, ':', '|') from the
        # prefix so a configured prefix cannot forge or inject additional
        # samples into the datagram. None of these are legal in a statsd
        # metric name, so a prefix that works today is left unchanged.
        self.prefix = prefix.translate({ord(c): None for c in "\r\n:|"})
        self.start_time = None
        self.job = job

    async def job_started(self) -> None:
        self.start_time = time.perf_counter()
        await send_to_statsd(
            self.host,
            self.port,
            "{prefix}.start:1|g\n".format(prefix=self.prefix),
        )

    async def job_stopped(self) -> None:
        if self.start_time is None:
            return
        duration_seconds = time.perf_counter() - self.start_time
        duration = int(round(duration_seconds * 1000))
        message = (
            "{prefix}.stop:1|g\n"
            "{prefix}.success:{success}|g\n"
            "{prefix}.duration:{duration}|ms|@0.1\n"
        ).format(
            prefix=self.prefix,
            success=0 if self.job.failed else 1,
            duration=duration,
        )
        # When the run was resource-monitored (see cronstable.resources) ship
        # the CPU time and peak RSS alongside the duration. resource_usage is
        # finalized in RunningJob._on_stop before this hook runs, so it is
        # already populated here; None when monitoring was off or unavailable.
        usage = getattr(self.job, "resource_usage", None)
        if usage is not None:
            message += (
                "{prefix}.cpu:{cpu}|ms|@0.1\n{prefix}.max_rss:{rss}|g\n"
            ).format(
                prefix=self.prefix,
                cpu=int(round(usage.cpu_total_seconds * 1000)),
                rss=usage.max_rss_bytes,
            )
        await send_to_statsd(self.host, self.port, message)
