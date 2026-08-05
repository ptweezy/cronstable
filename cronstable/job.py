import asyncio
import asyncio.subprocess
import atexit
import html
import logging
import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime
from functools import lru_cache
from socket import gethostname
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
)
from urllib.parse import urlsplit, urlunsplit

from cronstable import platform, push
from cronstable.config import (
    ConfigError,
    JobConfig,
    _resolve_secret,
    schedule_object_to_crontab,
)
from cronstable.resources import ResourceMonitor, ResourceUsage
from cronstable.statsd import StatsdJobMetricWriter

if TYPE_CHECKING:
    # jinja2/sentry_sdk/aiosmtplib are imported lazily inside the reporters
    # that use them (_compiled_template / SentryReporter / MailReporter): they
    # cost ~40-170ms to import and pull a lot into RSS, and a job that never
    # reports through those channels should pay for none of it. This block
    # runs only under the type checker (to resolve the jinja2.Template
    # annotation); at runtime TYPE_CHECKING is False and it is skipped.
    import jinja2

logger = logging.getLogger("cronstable")


@lru_cache(maxsize=256)
def _compiled_template(source: str) -> "jinja2.Template":
    # Template source strings come from config. They are NOT constant for the
    # life of the process: a config reload can edit a template, and each
    # distinct source text then becomes a new entry. The cache is therefore
    # bounded rather than unbounded, so a daemon that runs for months across
    # many reloads cannot accumulate compiled templates without limit; 256 is
    # far above any realistic live template count, so the steady state is
    # still one compile per distinct template. jinja2 is
    # imported here (not at module top) so a daemon whose jobs never render a
    # report template never pays its import cost; the lru_cache means the
    # import statement is only reached on the first distinct template anyway.
    import jinja2

    return jinja2.Template(source)


if "HOSTNAME" not in os.environ:
    os.environ["HOSTNAME"] = gethostname()


def report_hostname() -> str:
    """The host name to stamp on report payloads.

    ``os.environ["HOSTNAME"]`` is forced to :func:`gethostname` at import (see
    above), so this is the daemon's host regardless of whether the environment
    named it.  Shared by ``template_vars`` and the shell reporter so every
    notification channel agrees on which node ran the job.
    """
    return os.environ.get("HOSTNAME", "")


def schedule_string(config: "JobConfig") -> str:
    """A job's schedule as a crontab line, object schedules rendered.

    ``config.schedule_unparsed`` is ``Union[str, dict]``; the object form is
    rendered the same way the status payload, prometheus, and the shell
    reporter's ``CRONSTABLE_JOB_SCHEDULE`` do, so every report payload carries
    the identical string no matter which spelling the config used.
    """
    unparsed = config.schedule_unparsed
    if isinstance(unparsed, str):
        return unparsed
    return schedule_object_to_crontab(unparsed)


def fixup_pyinstaller_env(env: Dict[str, str]) -> None:
    # check for pyinstaller env, fix clobbered env vars
    # https://github.com/gjcarneiro/yacron/issues/68
    # These are the dynamic-loader paths PyInstaller rewrites on POSIX; the
    # Windows bootloader doesn't touch them, so there's nothing to restore.
    if getattr(sys, "frozen", False) and not platform.IS_WINDOWS:
        for env_var in "LD_LIBRARY_PATH", "LIBPATH":
            env[env_var] = env.get(f"{env_var}_ORIG", "")


def loggable_spawn_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``kwargs`` with the child environment reduced to a summary.

    The spawn kwargs carry ``env``: a full copy of the daemon's own
    :data:`os.environ` (whatever the operator exported to cronstable, such as
    cloud keys, database URLs, or a systemd ``EnvironmentFile``) plus the
    job's configured variables plus the ``CRONSTABLE_*`` control-channel vars,
    whose token is a live bearer credential for the loopback state API.
    Formatting that dict into a log record publishes all of it to
    journald/syslog and any shipper behind them, at whatever level the record
    was emitted.

    :func:`cronstable.redact.redact_secrets` deliberately does not help here:
    it is scoped to archived job output and is pattern-based, so it would miss
    any variable whose name it doesn't recognise.  Names alone are also not
    safe to log (a variable can be named after the secret it holds), so the
    value is replaced wholesale by a count, which is what the surviving
    diagnostics (a bad ``argv[0]``, a bad encoding, a resource-exhaustion
    errno) actually need: whether a custom environment was in play, not what
    was in it.  ``preexec_fn`` and the stream/limit entries are left alone;
    none of them carries user data.
    """
    if "env" not in kwargs:
        return kwargs
    redacted = dict(kwargs)
    redacted["env"] = "<{} vars, values omitted>".format(len(kwargs["env"]))
    return redacted


# How many of the most recent output lines a JobOutputStream retains for the
# live web log tail. Independent of saveLimit (which bounds the text kept for
# failure reports); this only bounds the in-memory buffer the UI streams from.
LIVE_LOG_LIMIT = 1000

# Hard cap on the lines held in one subscriber's delivery queue. A live tail
# that keeps up drains this to near-empty each loop; the cap only bites when a
# subscriber stalls (a backgrounded tab, a full/slow TCP window) while its job
# is a firehose. Without it the queue grows to the run's ENTIRE output per
# stalled subscriber (the LIVE_LOG_LIMIT ring bounds the shared buffer, not the
# per-subscriber queue), so one paused tab on a chatty job could pin hundreds
# of MB. On overflow the OLDEST queued line is dropped so the viewer keeps
# receiving the newest output; the live tail is best-effort, and a reconnect
# re-snapshots the ring buffer. Generous headroom over the 1000-line ring so a
# briefly-slow client loses nothing.
LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT = 8192

# How long a forcibly-terminated run waits for its stdout/stderr to reach EOF
# before the readers are cancelled and whatever they captured is kept (see
# RunningJob._read_job_streams). Only ever reached when a descendant escaped
# the process-group kill, so it costs nothing on a healthy run; a fixed bound
# rather than killTimeout, which is legitimately configured to 0 (kill at
# once) by jobs that would then lose output they had already produced.
KILLED_STREAM_DRAIN_TIMEOUT = 30.0

# Overall bound on one mail report's SMTP conversation (connect, STARTTLS,
# login, send). aiosmtplib's own default is 60 seconds PER OPERATION, so a
# black-holed or tar-pitting SMTP server could hold a report for several
# minutes with no explicit bound; the report runs inside the job's completion
# sequence, so that would also hold up the same job's retry arming. Generous
# for any healthy server; on expiry the report is logged as failed and the
# socket released.
MAIL_REPORT_TIMEOUT = 60.0


class _MirrorWriter:
    """The stdout/stderr passthrough's single daemon-wide writer thread.

    Job output mirrored to the daemon's own stdout/stderr used to be
    written (and flushed) on the event-loop thread.  Batching had already
    cut it to one write per drained read, but the write itself remained a
    blocking syscall: with the daemon's pipe full (a stopped
    ``docker logs``, a Ctrl+S'd console, a dead journald) it parked the
    loop indefinitely and the whole daemon, scheduling included, froze
    behind one wedged log consumer.  Now batches queue here and one
    daemon thread writes them; a wedged consumer wedges only this thread,
    and the bounded queue sheds the OLDEST batch (counted, warned once)
    so memory stays flat however long the consumer sleeps.

    One thread for both streams on purpose: it preserves the enqueue
    order across stdout and stderr, exactly what the inline writes gave.
    The thread starts lazily on the first mirrored batch, so a daemon
    with no capturing jobs never creates it, and registers a bounded
    atexit drain so an orderly shutdown flushes the tail without letting
    a wedged pipe hold the exit hostage.
    """

    #: Retained batches (one per drained read) while the consumer stalls.
    #: At the reader's chunk size a batch is a few KB, so the cap bounds a
    #: fully wedged consumer to a few MB, not the run's whole output.
    MAX_PENDING_BATCHES = 512

    def __init__(self) -> None:
        self._batches: Deque[Tuple[str, str, str]] = deque()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._idle = threading.Event()
        self._idle.set()
        self._thread: Optional[threading.Thread] = None
        self.dropped_batches = 0
        self._drop_logged = False

    def submit(self, job_name: str, stream_name: str, text: str) -> None:
        """Queue one passthrough batch; never blocks, sheds when full."""
        start = False
        with self._lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="cronstable-mirror",
                    daemon=True,
                )
                start = True
            if len(self._batches) >= self.MAX_PENDING_BATCHES:
                self._batches.popleft()
                self.dropped_batches += 1
                if not self._drop_logged:
                    self._drop_logged = True
                    logger.warning(
                        "passthrough mirror is backed up (its consumer is "
                        "not reading the daemon's output); shedding oldest "
                        "batches until it drains"
                    )
            self._batches.append((job_name, stream_name, text))
            self._idle.clear()
            self._wake.set()
        if start:
            self._thread.start()
            # bounded: an orderly exit flushes the tail, a wedged consumer
            # cannot hold the process open past the timeout.
            atexit.register(self._idle.wait, 1.0)

    def drain(self, timeout: float) -> bool:
        """Wait until every queued batch was written (tests, atexit)."""
        return self._idle.wait(timeout)

    def _run(self) -> None:
        while True:
            self._wake.wait()
            with self._lock:
                batch = list(self._batches)
                self._batches.clear()
                self._wake.clear()
            for job_name, stream_name, text in batch:
                out = sys.stdout if stream_name == "stdout" else sys.stderr
                try:
                    StreamReader._emit(out, text)
                except (OSError, ValueError):
                    # The daemon's own stdout/stderr is broken or closed (a
                    # dead pipe consumer). The passthrough copy is
                    # best-effort; the capture buffers and live-tail publish
                    # are unaffected, so log per batch and keep going.
                    logger.warning(
                        "job %s: could not mirror %s to the daemon's own "
                        "stream",
                        job_name,
                        stream_name,
                        exc_info=True,
                    )
            with self._lock:
                if not self._batches:
                    self._idle.set()


#: The one mirror writer for the process; see :class:`_MirrorWriter`.
_MIRROR = _MirrorWriter()


class JobOutputStream:
    """In-memory, broadcastable view of a job run's captured output.

    Lines are appended as the job produces them (see ``StreamReader``) and
    pushed to any live subscribers — the web UI's log tail. A bounded ring
    buffer of the most recent lines is retained so a viewer that connects
    mid-run, or just after the run finished, still sees recent context.

    Nothing is ever written to disk, preserving cronstable's
    read-only-filesystem deployment story. The ring itself lives only while
    this run is its job's newest (or still running): once a newer run's
    record supersedes it the scheduler calls :meth:`release_lines`, because
    nothing can replay a superseded ring and the bounded run history would
    otherwise pin one full ring per retained record.
    """

    def __init__(self, limit: int = LIVE_LOG_LIMIT) -> None:
        # each item is (stream_name, line) with stream_name "stdout"/"stderr"
        self.lines: Deque[Tuple[str, str]] = deque(maxlen=limit)
        self._subscribers: List["asyncio.Queue"] = []
        self.closed = False
        # total lines ever published: `published - len(lines)` is how many
        # the ring evicted, so a consumer archiving the buffer (see
        # Cron._archive_output) can record the truncation instead of
        # presenting the tail as the whole output.
        self.published = 0
        # lines a stalled subscriber's bounded queue overflowed and dropped;
        # observability only (the live tail is best-effort).
        self.dropped = 0

    @staticmethod
    def _offer(queue: "asyncio.Queue", item: Any) -> bool:
        """Enqueue for one subscriber, dropping its oldest line if full.

        Returns True when an existing item had to be evicted to make room.
        publish() runs synchronously on the event-loop thread, so no consumer
        coroutine interleaves here and the get_nowait/put_nowait pair is race
        free. Dropping the OLDEST keeps the newest output flowing to a viewer
        that has fallen behind, and guarantees room for the end-of-stream
        sentinel even when the queue is saturated.
        """
        try:
            queue.put_nowait(item)
            return False
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
            except (
                asyncio.QueueEmpty
            ):  # pragma: no cover - full implies non-empty
                pass
            queue.put_nowait(item)
            return True

    def publish(self, stream_name: str, line: str) -> None:
        item = (stream_name, line)
        self.published += 1
        self.lines.append(item)
        for queue in self._subscribers:
            if self._offer(queue, item):
                self.dropped += 1

    def subscribe(self) -> "asyncio.Queue":
        queue: asyncio.Queue = asyncio.Queue(
            maxsize=LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT
        )
        self._subscribers.append(queue)
        if self.closed:
            # the run already finished: deliver the end sentinel immediately so
            # a late subscriber's read loop terminates after the buffered
            # snapshot instead of blocking on a stream that will never produce
            # another line.
            queue.put_nowait(None)
        return queue

    def unsubscribe(self, queue: "asyncio.Queue") -> None:
        try:
            self._subscribers.remove(queue)
        except ValueError:
            pass

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        # None is the end-of-stream sentinel for subscriber read loops. Route
        # it through _offer so a saturated queue still receives it (dropping an
        # oldest line to make room) and the reader loop terminates.
        for queue in self._subscribers:
            self._offer(queue, None)

    def release_lines(self) -> None:
        """Drop the retained ring buffer; counters and subscribers stay.

        Called when this run's record stops being its job's newest finished
        run: the log endpoints replay only the newest finished run (or a
        live one), so a superseded record's ring is unreachable payload,
        yet each one held up to its full ring for as long as the record sat
        in the bounded run history. That made steady-state memory scale
        with history depth times ring size per job instead of one ring per
        job. ``published``/``dropped`` are kept (they are plain counters,
        still shown in history rows), and any still-attached subscriber
        already received the end sentinel via :meth:`close`, so nothing
        observes the lines vanishing.
        """
        self.lines.clear()


#: Bytes pulled from a job's pipe per read.  ``StreamReader.read`` returns
#: as soon as ANY data is buffered, so a bigger chunk never delays a live
#: tail; it only lets a chatty job's output be split in C, dozens of lines
#: at a time, instead of running asyncio's Python-level ``readuntil``
#: machinery (a find, a slice, a delete, a resume check and a coroutine
#: frame) once per line.
_READ_CHUNK = 65536

#: Fallback line cap for a StreamReader built without one, matching the
#: ``maxLineLength`` default in cronstable.config.
DEFAULT_MAX_LINE_LENGTH = 16 * 1024 * 1024


class StreamReader:
    def __init__(
        self,
        job_name: str,
        stream_name: str,
        stream: asyncio.StreamReader,
        stream_prefix: str,
        save_limit: int,
        on_line: Optional[Callable[[str, str], None]] = None,
        max_line_length: Optional[int] = None,
    ) -> None:
        self.save_top: List[str] = []
        self.save_bottom: Deque[str] = deque()
        self.job_name = job_name
        self.save_limit = save_limit
        self.stream_name = stream_name
        self.stream_prefix = stream_prefix
        # Longest line kept, in BYTES before decoding.  Reading in chunks
        # means asyncio's own StreamReader limit no longer bounds a line
        # (that bound came from readuntil, which _read no longer calls), so
        # the cap is enforced by hand below.  Defaulting to the stream's
        # own limit leaves the cap exactly where it was for a caller that
        # passes none; the daemon passes maxLineLength explicitly, which is
        # the same number it hands the subprocess pipe.
        if max_line_length is None:
            max_line_length = getattr(
                stream, "_limit", DEFAULT_MAX_LINE_LENGTH
            )
        self.max_line_length = max_line_length
        # called with (stream_name, line) for each line read, so a live viewer
        # (the web UI) can tail output as the job produces it.
        self.on_line = on_line
        # lines awaiting one batched passthrough write to the daemon's own
        # stdout/stderr; flushed once per drained read (see _queue_emit).
        self._emit_buffer: List[str] = []
        self._emit_scheduled = False
        self._reader = asyncio.create_task(self._read(stream))
        self.discarded_lines = 0

    @staticmethod
    def _emit(out_stream, out_line: str) -> None:
        # Write bytes so we control the encoding; fall back to ASCII with
        # replacement when the console encoding can't represent the text.
        try:
            out_stream.buffer.write(out_line.encode())
        except UnicodeEncodeError:
            safe = out_line.encode("ascii", "replace").decode("ascii")
            out_stream.write(safe)
        out_stream.flush()

    def _flush_emit_buffer(self) -> None:
        self._emit_scheduled = False
        if not self._emit_buffer:
            return
        text = "".join(self._emit_buffer)
        self._emit_buffer.clear()
        # Hand the batch to the mirror's writer thread rather than writing
        # here: this method runs on the EVENT LOOP thread, and a write to a
        # full pipe (a stopped `docker logs`, a Ctrl+S'd console, a dead
        # journald) blocks until the consumer drains it, which used to
        # freeze the entire daemon, scheduling included, behind one wedged
        # log reader.
        _MIRROR.submit(self.job_name, self.stream_name, text)

    def _queue_emit(self, out_line: str) -> None:
        # One write+flush per DRAINED READ, not per line: readline() completes
        # without suspending while earlier reads left complete lines buffered,
        # so a flush scheduled with call_soon runs only once the read loop
        # actually blocks for new data, by which point every line of the
        # burst is in the buffer and goes out as a single write. Per line the
        # old inline emit cost two blocking syscalls ON THE EVENT LOOP THREAD,
        # and with the daemon's stdout pipe full it stalled the entire loop
        # once per line.
        self._emit_buffer.append(out_line)
        if not self._emit_scheduled:
            self._emit_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush_emit_buffer)

    async def _read(self, stream):
        """Drain ``stream`` to EOF, splitting it into lines.

        Reads in chunks and splits in C rather than awaiting
        ``StreamReader.readline`` per line: readline is a Python wrapper
        around ``readuntil``, whose find/slice/delete/resume bookkeeping and
        coroutine frame cost several times the decode they surround, once
        per output line, on the event-loop thread.

        Two things readuntil supplied for free are re-implemented here,
        because this loop reads job-controlled bytes:

        * the ``maxLineLength`` cap.  A complete line longer than the cap
          is dropped, and an unterminated run past the cap is dropped as it
          accumulates, both with the warning the ``ValueError`` branch used
          to log.  Whatever follows a drop is then read as an ordinary
          line, exactly as the cleared readuntil buffer left it; how much
          of an over-long line that surviving remainder holds depends on
          where the read boundary fell, as it always did (readuntil
          measured against pipe delivery, this measures against the chunk),
          and it is capped either way.
        * the unterminated tail at EOF.  A stream whose last line has no
          newline still yields that line, as readline's final non-empty
          return did.

        Splitting on ``b"\\n"`` cannot cut a UTF-8 code point in half (no
        continuation byte is 0x0A) and only complete lines are decoded, so
        a multi-byte character straddling a chunk boundary rides in the
        carry-over tail and decodes intact.
        """
        prefix = self.stream_prefix.format(
            job_name=self.job_name, stream_name=self.stream_name
        )
        limit_top = self.save_limit // 2
        limit_bottom = self.save_limit - limit_top
        passthrough = self.stream_name in ("stdout", "stderr")
        cap = self.max_line_length
        on_line = self.on_line
        save_limit = self.save_limit
        save_top = self.save_top
        save_bottom = self.save_bottom
        discarded = self.discarded_lines
        # Bytes after the last newline seen: not a line until the next
        # chunk (or EOF) terminates it.
        tail = b""
        while True:
            chunk = await stream.read(_READ_CHUNK)
            if chunk:
                if tail:
                    chunk = tail + chunk
                parts = chunk.split(b"\n")
                tail = parts.pop()
                if len(chunk) > cap:
                    # A segment can never be longer than the buffer it was
                    # cut from, so the per-line cap check is only reachable
                    # once the buffer itself has passed the cap: one
                    # comparison per chunk instead of one per line.
                    parts = [p for p in parts if not self._too_long(p, cap)]
                # errors="replace" so a job emitting non-UTF-8 bytes does
                # not crash the reader task with UnicodeDecodeError.
                lines = [
                    raw.decode("utf-8", errors="replace") + "\n"
                    for raw in parts
                ]
            elif tail and not self._too_long(tail, cap):
                lines = [tail.decode("utf-8", errors="replace")]
            else:
                lines = []
            for line in lines:
                if on_line is not None:
                    on_line(self.stream_name, line)
                if passthrough:
                    self._queue_emit(prefix + line)
                if save_limit > 0:
                    if len(save_top) < limit_top:
                        save_top.append(line)
                    else:
                        # deque(maxlen) would evict silently; track discards
                        # explicitly to preserve the "N lines discarded"
                        # count.
                        if len(save_bottom) == limit_bottom:
                            save_bottom.popleft()
                            discarded += 1
                        save_bottom.append(line)
                else:
                    discarded += 1
            # Published before the next await, so a reader cancelled by
            # join()'s timeout still reports the count it had reached.
            self.discarded_lines = discarded
            if not chunk:
                # EOF: push out whatever the last drain accumulated (the
                # already-scheduled callback then finds an empty buffer).
                self._flush_emit_buffer()
                return
            if self._too_long(tail, cap):
                # An unterminated run past the cap. Drop what has piled up
                # and keep reading: the readuntil limit cleared its buffer
                # and carried on in exactly the same way.
                tail = b""

    def _too_long(self, raw: bytes, cap: int) -> bool:
        """Whether ``raw`` breaks the line cap, warning once when it does."""
        if len(raw) <= cap:
            return False
        logger.warning("job %s: ignored a very long line", self.job_name)
        return True

    async def join(self, timeout: Optional[float] = None) -> Tuple[str, int]:
        """Drain to end-of-file; return ``(output, discarded_lines)``.

        ``timeout`` bounds the wait. The read loop only ends on EOF, which
        arrives when *every* write-end of the pipe is closed -- including any
        a descendant of the job inherited -- so a caller that has just killed
        the job passes a bound rather than trusting the pipe to close (see
        RunningJob._read_job_streams). On expiry the read loop is cancelled
        and the output captured so far is returned: the lines already read are
        held here, not in the pipe, so nothing collected is lost.
        """
        if timeout is None:
            await self._reader
        else:
            try:
                await asyncio.wait_for(self._reader, timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "job %s: %s did not reach end-of-file within %.1f seconds "
                    "of the job being killed -- a descendant that outlived it "
                    "still holds the pipe open; keeping the output captured "
                    "so far",
                    self.job_name,
                    self.stream_name,
                    timeout,
                )
        if self.save_bottom:
            middle = (
                [
                    "   [.... {} lines discarded ...]\n".format(
                        self.discarded_lines
                    )
                ]
                if self.discarded_lines
                else []
            )
            output = "".join(self.save_top + middle + list(self.save_bottom))
        else:
            output = "".join(self.save_top)
        return output, self.discarded_lines


class Reporter:
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        raise NotImplementedError  # pragma: no cover


class SentryReporter(Reporter):
    def __init__(self) -> None:
        # Remember the last (dsn, environment) we initialized the global
        # Sentry client with, so we don't rebuild the client/transport on
        # every single report.
        self._inited_key: Optional[Tuple[str, Optional[str]]] = None

    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        config = config["sentry"]
        try:
            # One resolver for every value/fromFile/fromEnvVar triple
            # (config._resolve_secret, shared with the cluster/push/job-API
            # secrets): an unreadable fromFile or an unset env var is a
            # clean skip, never a traceback out of the completion path.
            # Its messages name the config key, not the env var name; the
            # name is config-derived and tied to a secret, so it stays out
            # of the logs (the rule MailReporter always had, now shared by
            # all three reporters).
            dsn = _resolve_secret(config["dsn"], "sentry.dsn")
        except ConfigError as ex:
            logger.error("sentry: %s; not reporting", ex)
            return
        if dsn is None:
            return  # sentry disabled: early return

        # Imported here, past the disabled/no-DSN early returns, so the ~130ms
        # sentry_sdk import (and its RSS) is paid only when a job actually
        # reports to Sentry, not by every daemon at startup.
        import sentry_sdk
        import sentry_sdk.utils

        template = _compiled_template(config["body"])
        body = template.render(job.template_vars)

        fingerprint = []
        for line in config["fingerprint"]:
            fingerprint.append(
                _compiled_template(line).render(job.template_vars)
            )

        kwargs = {}
        if config.get("maxStringLength"):
            sentry_sdk.utils.MAX_STRING_LENGTH = (  # type:ignore
                config["maxStringLength"]
            )
        if config.get("environment"):
            kwargs["environment"] = config["environment"]
        init_key = (dsn, kwargs.get("environment"))
        if init_key != self._inited_key:
            sentry_sdk.init(dsn=dsn, **kwargs)
            self._inited_key = init_key
        extra = {
            "job": job.config.name,
            "exit_code": job.retcode,
            "command": job.config.command,
            "shell": job.config.shell,
            "success": success,
        }
        extra.update(config.get("extra", {}))
        logger.debug(
            "sentry: fingerprint=%r; extra=%r' body:\n%s",
            fingerprint,
            extra,
            body,
        )
        with sentry_sdk.new_scope() as scope:
            for key, val in extra.items():
                scope.set_extra(key, val)
            scope.fingerprint = fingerprint
            sentry_sdk.capture_message(
                body, level=config.get("level", "error")
            )


class MailReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        mail = config["mail"]
        if not (mail["to"] and mail["from"]):
            return  # email reporting disabled
        smtp_host = mail["smtpHost"]
        smtp_port = mail["smtpPort"]

        try:
            # Shared secret resolver; see SentryReporter for the rationale
            # (clean skip on a bad source, env var names stay out of logs).
            # None (no source configured) means unauthenticated SMTP.
            password = _resolve_secret(mail["password"], "mail.password")
        except ConfigError as ex:
            logger.error("mail: %s; not sending email", ex)
            return
        username = mail.get("username")

        tmpl_vars = job.template_vars
        body_tmpl = _compiled_template(mail["body"])
        body = body_tmpl.render(tmpl_vars)
        if success and not body.strip():
            logger.debug("body is empty, not sending email")
            return
        subject_tmpl = _compiled_template(mail["subject"])
        subject = subject_tmpl.render(tmpl_vars)

        logger.debug("smtp: host=%r, port=%r", smtp_host, smtp_port)
        message = EmailMessage()
        message["From"] = mail["from"]
        message["To"] = mail["to"].strip()
        message["Subject"] = subject.strip()
        # RFC 5322 date, e.g. "Wed, 18 Jun 2026 12:34:56 +0000" (not ISO-8601).
        message["Date"] = format_datetime(datetime.now(timezone.utc))
        if mail["html"]:
            # set_content handles charset + transfer-encoding so non-ASCII
            # HTML bodies are sent correctly (set_payload would not).
            message.set_content(body, subtype="html")
        else:
            message.set_content(body)
        # Imported here, past the reporting-disabled early returns, so a daemon
        # that never sends a mail report never pays the aiosmtplib import cost.
        import aiosmtplib

        smtp = aiosmtplib.SMTP(
            hostname=smtp_host,
            port=smtp_port,
            use_tls=mail["tls"],
            validate_certs=mail["validate_certs"],
        )
        # One overall bound on the whole conversation: aiosmtplib only bounds
        # each individual operation (60s default), so without this a
        # black-holed server could hold the report (and the job's completion
        # sequence behind it) for several minutes.
        try:
            await asyncio.wait_for(
                self._converse(smtp, mail, username, password, message),
                MAIL_REPORT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error(
                "mail: report for job %s did not complete within %.0f "
                "seconds; giving up on it",
                job.config.name,
                MAIL_REPORT_TIMEOUT,
            )

    @staticmethod
    async def _converse(
        smtp: Any,
        mail: Dict[str, Any],
        username: Optional[str],
        password: Optional[str],
        message: EmailMessage,
    ) -> None:
        await smtp.connect()
        # close() (sync, idempotent) guarantees the socket is released even if
        # starttls/login/send raises (including the CancelledError a
        # wait_for timeout injects), so a failing SMTP server can't leak a
        # connection per report.
        try:
            if mail["starttls"]:
                await smtp.starttls()
            if username and password:
                # aiosmtplib >=2 takes username/password as positional args.
                await smtp.login(username, password)
            await smtp.send_message(message)
        finally:
            smtp.close()


class ShellReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        shell_config = config["shell"]

        if shell_config["command"] is None:
            return

        if isinstance(shell_config["command"], list):
            create = asyncio.create_subprocess_exec  # type: Any
            cmd = shell_config["command"]
        else:
            if shell_config["shell"]:
                create = asyncio.create_subprocess_exec
                cmd = [shell_config["shell"], "-c", shell_config["command"]]
            else:
                create = asyncio.create_subprocess_shell
                cmd = [shell_config["command"]]

        # pass the necessary information as env variables

        # We have to be a bit careful because job.stderr and job.stdout
        # can potentially be very large. On Linux there are limits
        # both on the individual as well as combined length of the arguments.
        std_err_str = job.stderr if job.stderr is not None else ""
        std_out_str = job.stdout if job.stdout is not None else ""
        # this is an arbitrary safe lower limit
        max_length_arg = 1024 * 16
        args_too_long = (
            len(std_err_str) > max_length_arg
            or len(std_out_str) > max_length_arg
            or len(std_err_str) + len(std_out_str) > max_length_arg
        )
        std_err_str_safe = (
            std_err_str if not args_too_long else std_err_str[:max_length_arg]
        )
        std_out_str_safe = (
            std_out_str if not args_too_long else std_out_str[:max_length_arg]
        )

        env = {
            **os.environ,
            "CRONSTABLE_FAIL_REASON": (
                job.fail_reason if job.fail_reason is not None else ""
            ),
            "CRONSTABLE_JOB_NAME": job.config.name,
            "CRONSTABLE_JOB_COMMAND": (
                job.config.command
                if not isinstance(job.config.command, list)
                else " ".join(job.config.command)
            ),
            # Rendered to the crontab line for the OBJECT form (like
            # cron.schedule_str, the status payload and prometheus do):
            # ``schedule_unparsed`` is Union[str, dict], and a dict here
            # dies in os.fsencode at spawn -- silently disabling the shell
            # reporter for every object-schedule job.  README declares this
            # variable (str), and the two schedule spellings are documented
            # as equivalent.
            "CRONSTABLE_JOB_SCHEDULE": schedule_string(job.config),
            "CRONSTABLE_FAILED": "1" if job.failed else "0",
            "CRONSTABLE_RETCODE": str(job.retcode),
            "CRONSTABLE_STDERR": std_err_str_safe,
            "CRONSTABLE_STDOUT": std_out_str_safe,
            "CRONSTABLE_STDERR_TRUNCATED": (
                "1" if len(std_err_str_safe) != len(std_err_str) else "0"
            ),
            "CRONSTABLE_STDOUT_TRUNCATED": (
                "1" if len(std_out_str_safe) != len(std_out_str) else "0"
            ),
        }
        # resource accounting, when the run was monitored; empty otherwise so
        # the reporter command can test for presence.
        usage = job.resource_usage
        env["CRONSTABLE_CPU_SECONDS"] = (
            repr(usage.cpu_total_seconds) if usage is not None else ""
        )
        env["CRONSTABLE_MAX_RSS_BYTES"] = (
            str(usage.max_rss_bytes) if usage is not None else ""
        )
        # run context: identity and timing of the run being reported. A
        # SlaBreachContext (onLate) carries no run, so run_id/started_at are
        # absent there and export empty. host is the daemon's, always set.
        env["CRONSTABLE_HOST"] = report_hostname()
        run_id = getattr(job, "run_id", None)
        env["CRONSTABLE_RUN_ID"] = run_id if run_id else ""
        started_at = getattr(job, "started_at", None)
        env["CRONSTABLE_STARTED_AT"] = (
            started_at.isoformat() if started_at is not None else ""
        )
        # SLA breach detail, when this report is an onLate dispatch (only
        # SlaBreachContext carries sla_vars; a finished run has none).
        # Always exported, empty when N/A, matching the CPU vars above.
        # The dict check also shields against non-dict stand-ins.
        sla_vars = getattr(job, "sla_vars", None)
        if not isinstance(sla_vars, dict):
            sla_vars = {}
        for env_name, key in (
            ("CRONSTABLE_SLA_CHECK", "sla_check"),
            ("CRONSTABLE_SLA_THRESHOLD_SECONDS", "threshold_seconds"),
            ("CRONSTABLE_SLA_OBSERVED_SECONDS", "observed_seconds"),
            ("CRONSTABLE_LAST_SUCCESS_AT", "last_success_at"),
        ):
            value = sla_vars.get(key)
            env[env_name] = str(value) if value is not None else ""

        logger.debug("Executing shell report cmd: %s", cmd)
        # Same process-group isolation as the job itself, so the timeout kill
        # below reaches the reporter's descendants as a unit (see
        # platform.new_process_group_kwargs).
        kwargs = platform.new_process_group_kwargs()
        try:
            proc = await create(*cmd, env=env, **kwargs)
        # OSError for the same reason RunningJob.start catches it: a missing
        # reporter binary (FileNotFoundError) or a spawn-time resource failure
        # (EMFILE/ENOMEM/EAGAIN) is not a SubprocessError subclass, and a
        # reporting problem must be logged, never propagated.  TypeError and
        # ValueError likewise: a non-string env value (fsencode raises
        # TypeError) or an embedded NUL in argv/env (ValueError) must land
        # here, not escape to _report_common's gather.
        except (subprocess.SubprocessError, OSError, TypeError, ValueError):
            logger.exception(
                "Error executing shell reporter of job %s", job.config.name
            )
            return

        # Bounded: report() runs INLINE on the reaper, the daemon's single
        # job-completion loop, so a reporter that never exits (curl with no
        # --max-time, a script reading stdin) would otherwise freeze
        # completion handling for EVERY job daemon-wide -- Forbid jobs stop
        # firing, shutdown never finishes. On expiry the reporter's whole
        # process group is killed and the run's handling proceeds.
        timeout = shell_config.get("timeout") or 60
        try:
            retcode = await asyncio.wait_for(proc.wait(), timeout)
        except asyncio.TimeoutError:
            logger.error(
                "Shell reporter of job %s did not finish within %.1f "
                "seconds; killing it",
                job.config.name,
                timeout,
            )
            if not await platform.kill_process_group(proc.pid, force=True):
                # group already gone or unsignallable: fall back to the
                # direct child, guarded like RunningJob.cancel.
                if proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
            # reap the killed child so it does not linger as a zombie. The
            # direct child is dead after the kill above, so this returns at
            # once; the extra bound only guarantees the reaper can never be
            # wedged here no matter what.
            try:
                await asyncio.wait_for(proc.wait(), 10)
            except asyncio.TimeoutError:  # pragma: no cover - defensive
                logger.error(
                    "Shell reporter of job %s could not be reaped after "
                    "being killed",
                    job.config.name,
                )
            return
        if retcode != 0:
            # not in an except block: a nonzero exit is not an exception, so
            # logger.exception would log a bogus "NoneType: None" traceback.
            logger.error(
                "Error executing shell reporter of job %s with return code %s",
                job.config.name,
                retcode,
            )


def _scrub_url_in(text: str, url: str) -> str:
    """``text`` with the webhook URL and its request target removed.

    The response body is third-party text, and a receiver can quote the
    request target straight back: finalhandler's ``Cannot POST /<path>``
    (Express's default for an unrouted path), Apache 2.2-era 404s, and
    some gateway error pages all do.  ``webhook.url`` is a secret whose
    secret part IS the path or query (see config.py's ``webhook.url``
    docs), so the body cannot be logged raw.  The HTML-escaped spelling
    is scrubbed too, because a server that echoes usually escapes what
    it echoes.
    """
    parts = urlsplit(url)
    target = urlunsplit(("", "", parts.path, parts.query, ""))
    out = text
    for needle in (url, target, parts.path):
        # len > 1 guards a root path: replacing "/" would redact every
        # slash in an otherwise innocent body.
        if len(needle) > 1:
            out = out.replace(needle, "<redacted>")
            escaped = html.escape(needle, quote=False)
            if escaped != needle:
                out = out.replace(escaped, "<redacted>")
    return out


class WebhookReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        webhook = config["webhook"]

        try:
            # Shared secret resolver; see SentryReporter for the rationale
            # (clean skip on a bad source, env var names stay out of logs;
            # webhook.url's secret part IS the URL, so that rule matters
            # here most of all).
            url = _resolve_secret(webhook["url"], "webhook.url")
        except ConfigError as ex:
            logger.error("webhook: %s; not reporting", ex)
            return
        if url is None:
            return  # webhook disabled: early return

        template = _compiled_template(webhook["body"])
        body = template.render(job.template_vars)

        headers = {"Content-Type": webhook["contentType"]}
        headers.update(webhook["headers"])

        # aiohttp is imported here, not at module top: this module is on the
        # daemon's unconditional import graph (cron -> dagrun -> job), and
        # aiohttp is ~155 ms and ~21 MB of RSS. The webhook reporter is the
        # only thing in this file that wants it, so a daemon whose jobs never
        # report over HTTP pays none of it, and neither does an offline path
        # that merely imports the module. By the time control reaches here the
        # reporter is already committed to making the call.
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=webhook["timeout"])
        # Encoded OUTSIDE the try below: a rendered body carrying a lone
        # surrogate (a job environment variable arriving through
        # os.environ's surrogateescape, interpolated by the template)
        # raises UnicodeEncodeError here, and that is a template bug
        # worth _report_common's traceback, not a "check webhook.url and
        # the network" line about a request that was never attempted.
        data = body.encode("utf-8")
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.request(
                    webhook["method"],
                    url,
                    data=data,
                    headers=headers,
                ) as resp:
                    if resp.status >= 400:
                        # never log the URL: Slack/Discord-style webhook
                        # URLs embed a secret token.  The response body
                        # is scrubbed for it too, and BEFORE the slice:
                        # the body is third-party text and a receiver
                        # that echoes the request target back (Express
                        # answers an unrouted path with "Cannot POST
                        # /<path>") would otherwise write the token to
                        # the log on every failing report.  Slicing
                        # first could also cut a needle in half and
                        # leave a prefix of the token behind.
                        #
                        # errors="replace" rather than aiohttp's strict
                        # default: the body is third-party bytes and a
                        # receiver whose error page does not match its
                        # own declared charset (a latin-1 gateway page
                        # labelled utf-8) would otherwise raise
                        # UnicodeDecodeError out of a request that
                        # COMPLETED, costing this line its status code
                        # and reporting a served 500 as a failure to
                        # reach the server at all.
                        logger.error(
                            "webhook reporter of job %s: server returned"
                            " HTTP %s: %s",
                            job.config.name,
                            resp.status,
                            _scrub_url_in(
                                await resp.text(errors="replace"), url
                            )[:1024],
                        )
                    else:
                        logger.debug(
                            "webhook reporter of job %s: HTTP %s",
                            job.config.name,
                            resp.status,
                        )
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                UnicodeError,
            ) as exc:
                # Caught HERE rather than left to _report_common's
                # catch-all, which logs the exception and its traceback:
                # the URL is the credential in this model (config.py
                # documents webhook.url as a secret, since a Slack or
                # Discord URL embeds its token) and it is unvalidated, so
                # a spelling yarl rejects (scheme omitted, typo'd port,
                # empty host) raises aiohttp's InvalidUrlClientError,
                # whose str() IS the URL.  Report the failure kind only,
                # matching the HTTP-error branch above, which keeps the
                # URL out of the log the same way.
                #
                # UnicodeError is in the tuple for the one failure here
                # that is not a ClientError subclass: a host yarl accepts
                # but idna rejects at connect time (a doubled dot, a
                # label over 63 characters) raises UnicodeEncodeError out
                # of getaddrinfo, which would otherwise reach the
                # catch-all's traceback.  It is a connect failure like
                # the rest of this arm, so "request failed" is honest.
                # The two Unicode failures that are NOT connect failures
                # are kept out of here on purpose: the body decode above
                # is errors="replace", and the request body's encode
                # happens before the try.
                logger.error(
                    "webhook reporter of job %s: request failed (%s);"
                    " check webhook.url and the network",
                    job.config.name,
                    type(exc).__name__,
                )


class PushReporter(Reporter):
    """End-to-end encrypted push alerts to paired devices.

    The thin edge only: this reads the per-job/per-event ``push`` block
    (enabled/priority/includeLogTail) and hands the context to the
    daemon-global :class:`cronstable.push.PushService`, which owns the
    device registry, the sealing and the relay client.  Config
    validation guarantees a ``push:`` section exists whenever this is
    enabled, so a missing service here is a real wiring bug worth an
    error line, not a silent drop.
    """

    async def report(
        self, success: bool, job: "RunningJob", config: Dict[str, Any]
    ) -> None:
        push_config = config.get("push") or {}
        if not push_config.get("enabled"):
            return  # push disabled: early return
        service = push.get_service()
        if service is None:
            logger.error(
                "push: report.push.enabled is set for %s but the push "
                "service is not running (no push: section applied); "
                "alert dropped",
                job.config.name,
            )
            return
        await service.send_report(job, success, push_config)


def report_config_enabled(report_config: Dict[str, Any]) -> bool:
    """Whether any of the five reporters would actually fire for this config.

    Mirrors each reporter's own disabled early-return exactly (sentry: no DSN
    source; mail: ``to`` and ``from`` unset; shell: no command; webhook: no URL
    source; push: not enabled), so a caller can skip scheduling a report
    fan-out that every reporter would drop on arrival. Used by the DAG-task
    reaper path, where a mapped fan-out can finish hundreds of instances at
    once and the common case (no reporter configured) must cost dict probes,
    not task spawns.
    """
    dsn = report_config["sentry"]["dsn"]
    if dsn["value"] or dsn["fromFile"] or dsn["fromEnvVar"]:
        return True
    mail = report_config["mail"]
    if mail["to"] and mail["from"]:
        return True
    if report_config["shell"]["command"] is not None:
        return True
    url = report_config["webhook"]["url"]
    if url["value"] or url["fromFile"] or url["fromEnvVar"]:
        return True
    # .get, not [], so report dicts predating the push block (older
    # persisted shapes, hand-built test configs) keep working.
    return bool((report_config.get("push") or {}).get("enabled"))


class JobRetryState:
    def __init__(
        self, initial_delay: float, multiplier: float, max_delay: float
    ) -> None:
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.delay = initial_delay
        self.count = 0  # number of times retried
        self.task = None  # type: Optional[asyncio.Task]
        self.cancelled = False
        # the absolute instant the currently-armed retry will fire, and the
        # delay it is sleeping out. Set by the scheduler when a retry is armed
        # (Cron.schedule_retry_job) so the dashboard can render a live
        # "attempt N/M · next retry in Xs" countdown from GET /jobs; None while
        # no retry is pending.
        self.next_retry_at = None  # type: Optional[datetime]
        self.scheduled_delay = None  # type: Optional[float]
        # the instant this ladder's current attempt was ARMED (its pending
        # first written). Copied into a cross-node HANDOFF record's
        # ``armedAt`` so the new owner's superseded-by-run guard anchors on
        # the original arm time, not the hand-off instant -- otherwise a run
        # the new owner already completed BETWEEN arming and hand-off would
        # look "older" than the record and be re-run (a double-fire).
        self.armed_at = None  # type: Optional[datetime]

    def next_delay(self) -> float:
        delay = self.delay
        self.delay = min(delay * self.multiplier, self.max_delay)
        self.count += 1
        return delay


class RunningJob:
    REPORTERS = [
        SentryReporter(),
        MailReporter(),
        ShellReporter(),
        WebhookReporter(),
        PushReporter(),
    ]  # type: List[Reporter]

    def __init__(
        self,
        config: JobConfig,
        retry_state: Optional[JobRetryState],
        *,
        extra_env: Optional[Dict[str, str]] = None,
        state_token: Optional[str] = None,
        run_id: Optional[str] = None,
        dag_ref: Optional[Any] = None,
    ) -> None:
        self.config = config
        # when set, this RunningJob is one DAG task instance rather
        # than a scheduled job; the reaper routes its completion to the DAG
        # scheduler (cronstable.dagrun) instead of the normal
        # record/retry path.
        # An opaque marker carrying (dag, run_key, taskkey, ...) the scheduler
        # needs to move the graph forward.
        self.dag_ref = dag_ref
        # environment the daemon injects on top of the job's own
        # (the loopback state-API URL + a per-run bearer token + run context).
        # Applied unconditionally in start(), after config.environment, so the
        # control-channel vars are present on every job and win over a same-
        # named user override. state_token is the loopback token the daemon
        # revokes when this run finishes (see Cron._handle_finished_job); it is
        # also carried in extra_env, but kept here for a direct, unambiguous
        # cleanup handle. run_id identifies this run in the durable ledger.
        self.extra_env = extra_env or {}
        self.state_token = state_token
        self.run_id = run_id
        self.proc = None  # type: Optional[asyncio.subprocess.Process]
        self.retcode = None  # type: Optional[int]
        # wall-clock instant this run started, for the web UI's run history;
        # set in start() so even a failed launch carries a timestamp.
        self.started_at = None  # type: Optional[datetime]
        # live, broadcastable view of this run's captured output (web UI tail)
        self.output = JobOutputStream()
        self._stderr_reader = None  # type: Optional[StreamReader]
        self._stdout_reader = None  # type: Optional[StreamReader]
        self.stderr = None  # type: Optional[str]
        self.stdout = None  # type: Optional[str]
        self.stderr_discarded = 0
        self.stdout_discarded = 0
        self.execution_deadline = None  # type: Optional[float]
        self.retry_state = retry_state
        self.env = None  # type: Optional[Dict[str, str]]
        # per-run CPU/memory accounting (opt-in via config.monitorResources).
        # _resource_monitor samples the process tree while the job runs;
        # resource_usage holds the finished result (None when monitoring is
        # off, unavailable, or the run was too short to sample). Finalized in
        # _on_stop, before the statsd emission that reports it.
        self._resource_monitor: Optional[ResourceMonitor] = None
        self.resource_usage: Optional[ResourceUsage] = None
        # set when the subprocess could not be launched at all (e.g. the
        # command does not exist). Lets wait() treat it as a normal job
        # failure instead of raising RuntimeError("process is not running").
        self.start_failed = False
        # guards against _on_stop running twice (cancel() racing wait())
        self._stopped = False
        # set by cancel(): this run was forcibly terminated (executionTimeout,
        # Replace, a user cancel) rather than left to exit on its own. Read by
        # _read_job_streams, which then bounds its wait for pipe EOF instead of
        # trusting a killed process tree to close its output.
        self._terminated = False
        # set by the scheduler when this run is deliberately cancelled to make
        # way for a newer instance (concurrencyPolicy=Replace). Such a forced
        # termination is not a job failure and must not be reported or retried.
        self.replaced = False
        # set when a user explicitly cancels this run from the web UI. Like
        # `replaced` it is not reported or retried, but unlike `replaced` it is
        # recorded in the run history (shown as "cancelled" in the dashboard).
        self.cancelled = False

        statsd_config = self.config.statsd
        if statsd_config is not None:
            self.statsd_writer = StatsdJobMetricWriter(
                host=statsd_config["host"],
                port=statsd_config["port"],
                prefix=statsd_config["prefix"],
                job=self,
            )  # type: Optional[StatsdJobMetricWriter]
        else:
            self.statsd_writer = None

    async def _on_start(self) -> None:
        if self.statsd_writer:
            # statsd is best-effort telemetry; a send failure (e.g. an
            # unresolvable host) must never propagate out of job launch and
            # crash the scheduler loop.
            try:
                await self.statsd_writer.job_started()
            except OSError:
                logger.warning(
                    "Job %s: failed to send statsd job_started metric",
                    self.config.name,
                    exc_info=True,
                )

    async def _on_stop(self) -> None:
        # idempotent: cancel() and the wait() task can both reach here for a
        # single run (e.g. concurrencyPolicy=Replace), but stop metrics must
        # only be emitted once. Safe without locking because asyncio is
        # single-threaded and there is no await before the flag is set.
        if self._stopped:
            return
        self._stopped = True
        # Finalize resource accounting before statsd reports it. _on_stop is
        # the single choke point every completion path funnels through (normal
        # exit, executionTimeout, cancel/replace), and it is idempotent, so
        # stopping the monitor here captures usage exactly once no matter how
        # the run ended. Errors are swallowed inside stop(); guard anyway so a
        # monitor bug can never break job completion.
        if self._resource_monitor is not None:
            try:
                self.resource_usage = await self._resource_monitor.stop()
            except Exception:  # noqa: BLE001 - accounting must never be fatal
                logger.warning(
                    "Job %s: failed to finalize resource monitoring",
                    self.config.name,
                    exc_info=True,
                )
            finally:
                self._resource_monitor = None
        if self.statsd_writer:
            try:
                await self.statsd_writer.job_stopped()
            except OSError:
                logger.warning(
                    "Job %s: failed to send statsd job_stopped metric",
                    self.config.name,
                    exc_info=True,
                )

    async def start(self) -> None:
        if self.proc is not None:
            raise RuntimeError("process already running")
        self.started_at = datetime.now(timezone.utc)
        # Isolate the job in its own process group, so cancel() can take its
        # whole descendant tree down as a unit rather than only the process we
        # spawned -- see cronstable.platform.new_process_group_kwargs.
        kwargs = platform.new_process_group_kwargs()  # type: Dict[str, Any]
        if isinstance(self.config.command, list):
            create = asyncio.create_subprocess_exec  # type: Any
            cmd = self.config.command
        else:
            if self.config.shell:
                create = asyncio.create_subprocess_exec
                cmd = [self.config.shell, "-c", self.config.command]
            else:
                create = asyncio.create_subprocess_shell
                cmd = [self.config.command]
        if self.config.environment or self.extra_env:
            env = dict(os.environ)
            fixup_pyinstaller_env(env)
            for envvar in self.config.environment:
                env[envvar["key"]] = envvar["value"]
            # The daemon-injected control-channel vars go last, so a job's own
            # environment cannot shadow the loopback URL/token it needs to
            # reach the state API (CRONSTABLE_* is reserved for cronstable's
            # use).
            env.update(self.extra_env)
            self.env = env
            kwargs["env"] = env
        if self.config.uid is not None or self.config.gid is not None:
            # POSIX only: uid/gid are always None on Windows (the config layer
            # rejects user/group there), so preexec_fn is never wired up on a
            # platform that doesn't support it.
            kwargs["preexec_fn"] = self._demote
        logger.debug("%s: will execute argv %r", self.config.name, cmd)
        if self.config.captureStderr:
            kwargs["stderr"] = asyncio.subprocess.PIPE
        if self.config.captureStdout:
            kwargs["stdout"] = asyncio.subprocess.PIPE
        if self.config.executionTimeout:
            self.execution_deadline = (
                time.perf_counter() + self.config.executionTimeout
            )
        if self.config.captureStderr or self.config.captureStdout:
            kwargs["limit"] = self.config.maxLineLength

        try:
            # POSIX wants UTF-8 bytes argv (locale-independent); Windows wants
            # str (CreateProcessW rejects bytes). See platform.encode_argv.
            args = platform.encode_argv(cmd)
            logger.debug(
                "subprocess: args=%r, kwargs=%r",
                args,
                loggable_spawn_kwargs(kwargs),
            )
            self.proc = await create(*args, **kwargs)
        except (
            subprocess.SubprocessError,
            # ValueError covers UnicodeEncodeError (unencodable argv) and,
            # critically, the 'embedded null byte' create_subprocess_exec
            # raises for a NUL in an argument or environment value.  The
            # crontab front end now refuses NULs at parse time, but any
            # unspawnable argv that still reaches here must be recorded as
            # start_failed for the reaper -- not kill the whole scheduler.
            ValueError,
            # OSError covers FileNotFoundError (bad argv[0]) AND the resource-
            # exhaustion / permission cases create_subprocess_exec can raise --
            # EMFILE/ENFILE (fd exhaustion), ENOMEM, EPERM/EACCES, EAGAIN (fork
            # limit). These are NOT SubprocessError subclasses, so without
            # OSError they propagate out of launch_scheduled_job through the
            # unguarded spawn_jobs / _process_pending_reboots path and kill the
            # whole scheduler. Catching here sets start_failed so the reaper
            # retries, instead of bringing the daemon down on a transient
            # spawn-time resource spike.
            OSError,
        ):
            logger.exception(
                "Error launching subprocess of job %s, cmd=%r, kwargs=%s "
                "(system encoding: %s)",
                self.config.name,
                cmd,
                loggable_spawn_kwargs(kwargs),
                sys.getdefaultencoding(),
            )
            self.start_failed = True
            return

        await self._on_start()

        if self.config.monitorResources and self.proc.pid is not None:
            # Begin sampling the child's process tree. Best-effort: if psutil
            # cannot attach (already exited, permission denied) the monitor
            # stays inert and resource_usage ends up None. Started here, right
            # after launch, so a long run is sampled from as early as possible.
            self._resource_monitor = ResourceMonitor(
                self.proc.pid,
                interval=self.config.monitorResourcesInterval,
                history=self.config.monitorResourcesHistory,
            )
            self._resource_monitor.start()

        if self.config.captureStderr:
            assert self.proc.stderr is not None
            self._stderr_reader = StreamReader(
                self.config.name,
                "stderr",
                self.proc.stderr,
                self.config.streamPrefix,
                self.config.saveLimit,
                on_line=self.output.publish,
                max_line_length=self.config.maxLineLength,
            )
        if self.config.captureStdout:
            assert self.proc.stdout is not None
            self._stdout_reader = StreamReader(
                self.config.name,
                "stdout",
                self.proc.stdout,
                self.config.streamPrefix,
                self.config.saveLimit,
                on_line=self.output.publish,
                max_line_length=self.config.maxLineLength,
            )

    def live_resources(self) -> Optional[Dict[str, Any]]:
        """Current live CPU/memory of this running instance, or ``None``.

        Read by the scheduler while the job is still running (the dashboard's
        live per-job readout). ``None`` when the run is not monitored, the
        monitor could not attach, or no sample has landed yet.
        """
        if self._resource_monitor is None:
            return None
        return self._resource_monitor.snapshot()

    def live_resource_series(self) -> Optional[List[List[float]]]:
        """The run-so-far CPU/RSS chart series, or ``None``.

        Kept separate from :meth:`live_resources` so the polled /jobs payload
        stays lean; only the dedicated resources endpoint asks for the series.
        """
        if self._resource_monitor is None:
            return None
        return self._resource_monitor.series()

    def _demote(self):
        # Runs in the child (preexec_fn) while still privileged. Order matters:
        # set/clear supplementary groups, then the primary gid, then the uid.
        # Dropping supplementary groups BEFORE setuid is essential — otherwise
        # the child keeps root's supplementary group memberships (the classic
        # "forgot setgroups() before setuid()" privilege-escalation bug).
        gid = self.config.gid
        uid = self.config.uid
        username = self.config.username
        try:
            if username is not None and gid is not None:
                # gives the target user exactly their own supplementary groups
                os.initgroups(username, gid)
            else:
                # unknown user/gid: drop all supplementary groups
                os.setgroups([])
        except OSError as ex:
            raise RuntimeError("setgroups/initgroups: {}".format(ex)) from ex
        if gid is not None:
            logger.debug("Changing to gid %r ...", gid)
            try:
                os.setgid(gid)
            except OSError as ex:
                raise RuntimeError("setgid: {}".format(ex)) from ex
        if uid is not None:
            logger.debug("Changing to uid %r ...", uid)
            try:
                os.setuid(uid)
            except OSError as ex:
                raise RuntimeError("setuid: {}".format(ex)) from ex

    async def wait(self) -> None:
        if self.proc is None:
            if self.start_failed:
                # The command never launched (e.g. it does not exist). Report
                # it as a normal failure (conventional "command not found"
                # exit code 127) rather than raising RuntimeError, which the
                # reaper would log as "please report this as a bug".
                self.retcode = 127
                await self._read_job_streams()
                return
            raise RuntimeError("process is not running")
        if self.execution_deadline is None:
            self.retcode = await self.proc.wait()
            await self._on_stop()
        else:
            timeout = self.execution_deadline - time.perf_counter()
            try:
                if timeout > 0:
                    self.retcode = await asyncio.wait_for(
                        self.proc.wait(), timeout
                    )
                    await self._on_stop()
                else:
                    raise asyncio.TimeoutError
            except asyncio.TimeoutError:
                logger.info(
                    "Job %s exceeded its executionTimeout of "
                    "%.1f seconds, cancelling it...",
                    self.config.name,
                    self.config.executionTimeout,
                )
                self.retcode = -100
                await self.cancel()
        await self._read_job_streams()

    async def _read_job_streams(self):
        # The readers end on pipe EOF, which needs EVERY write-end closed --
        # including any a descendant of the job inherited. cancel() takes the
        # job's whole process group down, so on a killed run EOF normally
        # follows at once; but a descendant that escaped the group (it called
        # setsid itself, or Windows could not reach it once orphaned) would
        # hold the pipe open indefinitely. This await is what the reaper is
        # parked on, and it has no outer bound, so that would strand the run in
        # running_jobs forever. Bound the drain on a killed run: the slot is
        # then always released, at the cost of the output we never saw anyway.
        # An untouched run is left unbounded -- it owns its own lifetime, and
        # its output is not ours to cut short.
        timeout = KILLED_STREAM_DRAIN_TIMEOUT if self._terminated else None
        if self._stderr_reader:
            (
                self.stderr,
                self.stderr_discarded,
            ) = await self._stderr_reader.join(timeout)
        if self._stdout_reader:
            (
                self.stdout,
                self.stdout_discarded,
            ) = await self._stdout_reader.join(timeout)
        # signal end-of-output to any live web log subscribers; their read
        # loops terminate on the sentinel this delivers.
        self.output.close()
        # Close our end of the subprocess pipes now that both readers have been
        # joined above. A run that reached EOF normally already had its
        # transport closed by asyncio, so this is a no-op; but a KILLED run
        # whose descendant escaped the group (see the bounded drain above)
        # never reaches EOF, so without this its stdout/stderr pipe transport
        # lingers unclosed until garbage collection -- leaking the read-end fd
        # in a long-lived daemon, and, under the test harness, surfacing as a
        # ProactorEventLoop "unclosed transport" finalizer error ("Event loop
        # is closed") once the per-test loop is torn down. Closing here runs
        # the transport's connection-lost on the live loop instead. close() is
        # idempotent and, after the joins above, can lose no captured output.
        transport = getattr(self.proc, "_transport", None)
        if transport is not None:
            transport.close()

    @property
    def failed(self) -> bool:
        return self.fail_reason is not None

    @property
    def fail_reason(self) -> Optional[str]:
        if self.config.failsWhen["always"]:
            return "failsWhen=always"
        if self.config.failsWhen["nonzeroReturn"] and self.retcode != 0:
            return "failsWhen=nonzeroReturn and retcode={}".format(
                self.retcode
            )
        if self.config.failsWhen["producesStdout"] and (
            self.stdout or self.stdout_discarded
        ):
            return "failsWhen=producesStdout and stdout is not empty"
        if self.config.failsWhen["producesStderr"] and (
            self.stderr or self.stderr_discarded
        ):
            return "failsWhen=producesStderr and stderr is not empty"
        return None

    async def cancel(self) -> None:
        """Terminate this run and everything it spawned.

        Signals the job's whole process group, not just the process we
        spawned: a job's descendants (``sh -c 'helper & main'``) inherit its
        stdout/stderr write-ends, so a helper that outlives a killed shell
        holds the pipe open forever -- the run never finishes draining, never
        leaves ``running_jobs``, and under ``concurrencyPolicy: Forbid`` the
        job never runs again. Killing the group also makes ``executionTimeout``
        mean what it says: a bound on the run's work, not on its root process.

        A run with no process (the spawn failed, so it registered with
        ``proc=None`` and ``start_failed``) is a NO-OP, not an error: callers
        cancel whatever ``running_jobs`` holds (the Replace branch of
        ``maybe_launch_job``, the cluster slot-renewer), and several of those
        paths run outside ``run()``'s try/except -- a raise here would escape
        them and take down the whole scheduler over a job that never even
        launched. The reaper still completes such a run through ``wait()``'s
        ``start_failed`` path, so nothing is left stranded.
        """
        if self.proc is None:
            logger.info(
                "Job %s: cancel is a no-op, no process was ever spawned "
                "(start_failed=%s)",
                self.config.name,
                self.start_failed,
            )
            return
        self._terminated = True
        # Graceful first: SIGTERM the group. This reaches the descendants even
        # once the leader itself has exited, which is exactly the case that
        # wedges the run. On Windows this step IS the taskkill tree kill:
        # there is no graceful signal, and the tree walk must run while the
        # root is still alive to anchor it (killing the root first, as the
        # fallback below does, would orphan every descendant for good). The
        # fallback to the direct child remains for a group/tree that could
        # not be signalled at all.
        if not await platform.kill_process_group(self.proc.pid, force=False):
            if self.proc.returncode is None:
                try:
                    self.proc.terminate()
                except ProcessLookupError:
                    pass
        try:
            await asyncio.wait_for(self.proc.wait(), self.config.killTimeout)
        except asyncio.TimeoutError:
            logger.warning(
                "Job %s did not gracefully terminate after "
                "%.1f seconds, killing it...",
                self.config.name,
                self.config.killTimeout,
            )
        # Unconditionally, whether or not the leader made its killTimeout: it
        # exiting says nothing about the descendants sharing its group, and
        # those are what hold the pipes open. A group that is already empty
        # reports back as "not signalled" and this is a no-op.
        if not await platform.kill_process_group(self.proc.pid, force=True):
            # The process may already be gone: on Python <=3.11
            # asyncio.wait_for can spuriously time out even though
            # proc.wait() completed (the timeout race fixed in 3.12),
            # leaving the transport closed with the returncode already
            # set. kill() would then raise ProcessLookupError on the
            # dead transport, so re-check and guard it like terminate().
            if self.proc.returncode is None:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
        await self._on_stop()

    # The three completion hooks below probe report_config_enabled before
    # doing anything, the same guard the DAG-task reaper already uses (see
    # Cron._maybe_report_dag_task). The default config configures no
    # reporter at all, so without it every completed run paid five
    # coroutine frames, five Tasks and at least two loop iterations before
    # each reporter reached its own disabled early-return, plus an INFO
    # record announcing reporting that was never going to happen. The log
    # line sits behind the probe for that reason: it is only true when a
    # reporter will actually fire.

    async def report_failure(self):
        report_config = self.config.onFailure["report"]
        if not report_config_enabled(report_config):
            return
        logger.info("Cron job %s: reporting failure", self.config.name)
        await self._report_common(report_config, False)

    async def report_permanent_failure(self):
        report_config = self.config.onPermanentFailure["report"]
        if not report_config_enabled(report_config):
            return
        logger.info(
            "Cron job %s: reporting permanent failure", self.config.name
        )
        await self._report_common(report_config, False)

    async def report_success(self):
        report_config = self.config.onSuccess["report"]
        if not report_config_enabled(report_config):
            return
        logger.info("Cron job %s: reporting success", self.config.name)
        await self._report_common(report_config, True)

    async def _report_common(self, report_config: dict, success: bool) -> None:
        results = await asyncio.gather(
            *[
                reporter.report(success, self, report_config)
                for reporter in self.REPORTERS
            ],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    "Problem reporting job %s failure: %s",
                    self.config.name,
                    result,
                    exc_info=result,
                )

    @property
    def template_vars(self) -> dict:
        fail_reason = self.fail_reason
        usage = self.resource_usage
        return {
            "name": self.config.name,
            "success": fail_reason is None,
            "fail_reason": fail_reason,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.retcode,
            "command": self.config.command,
            "shell": self.config.shell,
            "environment": self.env,
            # run context: which node ran it, its schedule, when it started,
            # and the durable-ledger id, so a webhook/ntfy payload can identify
            # the run without the template digging into ``environment``.
            # started_at is ISO-8601 (None before start / on a failed launch);
            # run_id is None without a durable state store.
            "host": report_hostname(),
            "schedule": schedule_string(self.config),
            "started_at": (
                self.started_at.isoformat()
                if self.started_at is not None
                else None
            ),
            "run_id": self.run_id,
            # resource accounting for report templates; all None when the run
            # was not monitored (monitorResources off / unavailable).
            "cpu_seconds": usage.cpu_total_seconds if usage else None,
            "cpu_user_seconds": usage.cpu_user_seconds if usage else None,
            "cpu_system_seconds": usage.cpu_system_seconds if usage else None,
            "max_rss_bytes": usage.max_rss_bytes if usage else None,
        }


class SlaBreachContext:
    """Reporting context for one SLA breach: a job that did NOT (yet) run.

    Quacks like a :class:`RunningJob` exactly as far as the reporters
    read one (``config``, ``template_vars``, and the attributes
    :class:`ShellReporter` exports), with every run-shaped field
    explicitly empty. Deliberately NOT a bare ``RunningJob``: with no
    process, the default ``failsWhen.nonzeroReturn`` would synthesize the
    bogus fail_reason "failsWhen=nonzeroReturn and retcode=None" (and
    ``__init__`` would build a pointless statsd writer); here ``failed``
    and ``fail_reason`` state exactly what happened.

    ``template_vars`` carries the full standard key set with None/False
    fills, so operator templates written for onFailure render unchanged
    on onLate, plus the breach detail (``sla_check``,
    ``threshold_seconds``, ``observed_seconds``, ``last_success_at``).
    ``env`` carries HOSTNAME so the default sentry fingerprint's
    ``{{ environment.HOSTNAME }}`` line keeps its host dimension.
    """

    def __init__(
        self,
        config: JobConfig,
        *,
        check: str,
        threshold_seconds: float,
        observed_seconds: float,
        last_success_at: Optional[str] = None,
    ) -> None:
        self.config = config
        self.sla_check = check
        self.threshold_seconds = threshold_seconds
        self.observed_seconds = observed_seconds
        self.last_success_at = last_success_at
        self.fail_reason = "sla: {} breached".format(check)
        self.failed = True
        self.retcode = None  # type: Optional[int]
        self.stdout = None  # type: Optional[str]
        self.stderr = None  # type: Optional[str]
        self.stdout_discarded = 0
        self.stderr_discarded = 0
        self.resource_usage = None  # type: Optional[ResourceUsage]
        self.env = {"HOSTNAME": os.environ.get("HOSTNAME", "")}
        # read by ShellReporter for the CRONSTABLE_SLA_* exports.
        self.sla_vars = {
            "sla_check": check,
            "threshold_seconds": threshold_seconds,
            "observed_seconds": observed_seconds,
            "last_success_at": last_success_at,
        }

    @property
    def template_vars(self) -> dict:
        return {
            "name": self.config.name,
            "success": False,
            "fail_reason": self.fail_reason,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.retcode,
            "command": self.config.command,
            "shell": self.config.shell,
            "environment": self.env,
            # run context: an SLA breach describes a job that did NOT run, so
            # started_at/run_id are None; host and schedule still describe it.
            "host": report_hostname(),
            "schedule": schedule_string(self.config),
            "started_at": None,
            "run_id": None,
            "cpu_seconds": None,
            "cpu_user_seconds": None,
            "cpu_system_seconds": None,
            "max_rss_bytes": None,
            "sla_check": self.sla_check,
            "threshold_seconds": self.threshold_seconds,
            "observed_seconds": self.observed_seconds,
            "last_success_at": self.last_success_at,
        }


async def report_sla_breach(
    ctx: SlaBreachContext, report_config: dict
) -> None:
    """Fan one SLA breach out to all four reporters (the onLate hook).

    The ``_report_common`` gather idiom with ``success=False``
    throughout: an overdue job is bad news, so MailReporter's empty-body
    suppression (success-only) can never eat the alert and Sentry
    defaults to level "error".
    """
    logger.info(
        "Cron job %s: reporting SLA breach (%s)",
        ctx.config.name,
        ctx.sla_check,
    )
    results = await asyncio.gather(
        *[
            # duck-typed on purpose: the context quacks like the
            # RunningJob slice each reporter actually reads.
            r.report(False, ctx, report_config)  # type: ignore[arg-type]
            for r in RunningJob.REPORTERS
        ],
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            logger.error(
                "Problem reporting job %s SLA breach: %s",
                ctx.config.name,
                result,
                exc_info=result,
            )


class _NotifyJobShim:
    """The tiny ``JobConfig`` slice the reporters read for a daemon event.

    A daemon/orchestration event (a DAG failure, an approval gate, a
    leadership change) has no job, but :class:`SentryReporter` and
    :class:`ShellReporter` reach into ``job.config`` for ``name`` / ``command``
    / ``shell`` / ``schedule_unparsed``.  This supplies exactly those, with the
    non-name launch fields empty so the shell reporter's env encodes to strings
    rather than ``None`` (which would die in ``os.fsencode`` at spawn).
    """

    __slots__ = ("name", "command", "shell", "schedule_unparsed")

    def __init__(self, name: str) -> None:
        self.name = name
        self.command = ""
        self.shell = platform.DEFAULT_SHELL
        self.schedule_unparsed = ""


class NotifyEventContext:
    """Reporting context for a daemon/orchestration event (the ``notify:``
    block): a DAG run failure, an approval gate awaiting a decision, or a
    leadership / quorum change; none of which is a job run.

    Quacks like a :class:`RunningJob` exactly as far as the four reporters read
    one (a minimal :class:`_NotifyJobShim` ``config``, the run-shaped fields
    empty, and a ``template_vars`` carrying the standard key set so operator
    templates written for a job render unchanged), plus the event detail:
    ``event`` (the :data:`~cronstable.config.NOTIFY_EVENTS` name), ``subject``
    (a one-line headline), ``message`` (the body), and any event-specific
    ``fields`` (dag, run_key, taskkey, role, leader, ...).  The notify report
    defaults key their templates on ``event`` / ``subject`` / ``message``.
    """

    def __init__(
        self,
        *,
        event: str,
        success: bool,
        name: str,
        subject: str,
        message: str,
        fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.event = event
        self.config = _NotifyJobShim(name)
        self._success = success
        self._subject = subject
        self._message = message
        self._fields = fields or {}
        # run-shaped fields the reporters read, all empty: no process ran.
        self.fail_reason = None if success else message
        self.failed = not success
        self.retcode = None  # type: Optional[int]
        self.stdout = None  # type: Optional[str]
        self.stderr = None  # type: Optional[str]
        self.stdout_discarded = 0
        self.stderr_discarded = 0
        self.resource_usage = None  # type: Optional[ResourceUsage]
        self.env = {"HOSTNAME": report_hostname()}

    @property
    def template_vars(self) -> dict:
        base = {
            "name": self.config.name,
            "success": self._success,
            "fail_reason": self.fail_reason,
            "stdout": None,
            "stderr": None,
            "exit_code": None,
            "command": self.config.command,
            "shell": self.config.shell,
            "environment": self.env,
            "host": report_hostname(),
            "schedule": "",
            "started_at": None,
            "run_id": None,
            "cpu_seconds": None,
            "cpu_user_seconds": None,
            "cpu_system_seconds": None,
            "max_rss_bytes": None,
            # the event detail the notify templates render.
            "event": self.event,
            "subject": self._subject,
            "message": self._message,
        }
        # event-specific extras (dag, run_key, taskkey, role, leader, ...);
        # last so an event can override a standard key if it must.
        base.update(self._fields)
        return base


async def report_event(ctx: NotifyEventContext, report_config: dict) -> None:
    """Fan one daemon/orchestration event out to every reporter.

    The ``_report_common`` gather idiom, reused for the ``notify:`` block: the
    context is a :class:`NotifyEventContext` rather than a job.  ``success`` is
    threaded from the event (an alert-worthy event passes ``success=False`` so
    MailReporter's empty-body suppression cannot eat it).  Every reporter error
    is caught and logged; a notification failure never propagates to the
    scheduler or cluster loop that raised the event.
    """
    logger.info("Reporting %s event: %s", ctx.event, ctx._subject)
    results = await asyncio.gather(
        *[
            # duck-typed on purpose: the context quacks like the RunningJob
            # slice each reporter actually reads.
            r.report(not ctx.failed, ctx, report_config)  # type: ignore[arg-type]
            for r in RunningJob.REPORTERS
        ],
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            logger.error(
                "Problem reporting %s event: %s",
                ctx.event,
                result,
                exc_info=result,
            )
