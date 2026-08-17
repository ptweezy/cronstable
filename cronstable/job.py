import asyncio
import asyncio.subprocess
import atexit
import html
import itertools
import logging
import ntpath
import os
import subprocess
import sys
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime
from functools import lru_cache

# The names, not the module: `queue` is already a local variable in this
# file (the per-subscriber output queues in JobOutputStream), so importing
# the module under its own name shadows them and ruff refuses it.
from queue import Full, Queue
from socket import gethostname
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
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
    # that use them; a daemon that never reports through those channels pays
    # none of their import cost. This block only satisfies the type checker.
    import jinja2

logger = logging.getLogger("cronstable")


@lru_cache(maxsize=256)
def _compiled_template(source: str) -> "jinja2.Template":
    # Sources come from config and change across reloads, so the cache is
    # bounded (256 is far above any realistic live template count). jinja2
    # is imported here so a daemon that never renders a report template
    # never pays its import cost.
    import jinja2

    # typed local because jinja2.Template.__new__ is typed Any-returning
    # and warn_return_any would flag returning the call directly.
    template: "jinja2.Template" = jinja2.Template(source)
    return template


if "HOSTNAME" not in os.environ:
    os.environ["HOSTNAME"] = gethostname()


def report_hostname() -> str:
    """The host name to stamp on report payloads.

    ``HOSTNAME`` is forced to :func:`gethostname` at import (see above);
    shared so every notification channel agrees on which node ran the job.
    """
    return os.environ.get("HOSTNAME", "")


def schedule_string(config: "JobConfig") -> str:
    """A job's schedule as a crontab line, object schedules rendered.

    Shared by the status payload, prometheus, and the reporters so every
    surface carries the identical string whichever spelling the config used.
    """
    unparsed = config.schedule_unparsed
    if isinstance(unparsed, str):
        return unparsed
    return schedule_object_to_crontab(unparsed)


def fixup_pyinstaller_env(env: dict[str, str]) -> None:
    # check for pyinstaller env, fix clobbered env vars
    # https://github.com/gjcarneiro/yacron/issues/68
    # These are the dynamic-loader paths PyInstaller rewrites on POSIX; the
    # Windows bootloader doesn't touch them, so there's nothing to restore.
    if getattr(sys, "frozen", False) and not platform.IS_WINDOWS:
        for env_var in "LD_LIBRARY_PATH", "LIBPATH":
            env[env_var] = env.get(f"{env_var}_ORIG", "")


def loggable_spawn_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Return ``kwargs`` with the child environment reduced to a summary.

    The spawn kwargs carry ``env``: the daemon's full :data:`os.environ`
    (cloud keys, database URLs, whatever the operator exported) plus the
    job's variables plus the ``CRONSTABLE_*`` control-channel vars, whose
    token is a live bearer credential for the loopback state API.  Logging
    that dict would publish all of it to journald/syslog and any shipper
    behind them.  :func:`cronstable.redact.redact_secrets` cannot help
    (pattern-based, scoped to archived output) and names alone are not safe
    to log either, so the value is replaced wholesale by a count: the
    surviving diagnostics only need to know whether a custom environment
    was in play.
    """
    if "env" not in kwargs:
        return kwargs
    redacted = dict(kwargs)
    redacted["env"] = "<{} vars, values omitted>".format(len(kwargs["env"]))
    return redacted


def is_cmd_shell(shell: str) -> bool:
    """Whether ``shell`` names the Windows command processor.

    Matched on the basename, so ``cmd``, ``cmd.exe`` and a full
    ``C:\\Windows\\System32\\cmd.exe`` all resolve alike.
    """
    name = os.path.basename(shell.replace("\\", "/")).lower()
    return name in ("cmd", "cmd.exe")


def shell_spawn(
    shell: str, command: str, windows: Optional[bool] = None
) -> tuple[Any, list[str], dict[str, Any]]:
    """The spawn call, argv and extra kwargs for ``command`` under ``shell``.

    Every shell but cmd.exe is spawned directly, as ``<shell> -c
    <command>`` (PowerShell reads ``-c`` as an abbreviation of
    ``-Command``).  Windows has no argv, so that list is rendered into one
    command line by the MSVC runtime's quoting rules, and those are the
    rules ``CommandLineToArgvW`` reverses, which is how every one of those
    shells recovers the command it was handed.

    cmd.exe is the exception, for two reasons.  It wants ``/c``, not
    ``-c``: handed ``-c`` it starts an interactive shell, prints its
    version banner, reads EOF on stdin and exits 0, so a ``shell: cmd`` job
    records a clean success without ever running its command.  It also
    parses its own command line by its own rules rather than
    ``CommandLineToArgvW``'s, so the ``\\"`` the renderer emits for an
    embedded double quote survives into the command verbatim and ``echo
    "hello world"`` prints ``\\"hello world\\"``.  Handing the command
    string to :func:`asyncio.create_subprocess_shell` instead skips the
    rendering entirely: the string goes to ``CreateProcess`` as
    ``%ComSpec% /c "<command>"``, which is both the flag cmd.exe wants and
    the shape its ``/c`` quote rules are written for.  An empty ``shell:``
    takes that same path, so the Windows default and an explicit
    ``shell: cmd`` land on one spawn rather than two.

    A ``shell:`` spelled out as a path is passed as ``executable`` so a
    deliberately chosen cmd.exe still beats ``%ComSpec%``; a bare ``cmd``
    resolves through ComSpec, which is also what keeps an unqualified name
    from being searched for in the current directory first.

    All of which is Windows' business alone, so ``windows`` (defaulting to
    :data:`~cronstable.platform.IS_WINDOWS`, and injectable so both
    branches are testable from either OS) gates it: a POSIX box running a
    shell that happens to be named ``cmd`` gets the ordinary treatment,
    and a ``shell: cmd`` typo there still fails to spawn rather than
    quietly running under /bin/sh.
    """
    if windows is None:
        windows = platform.IS_WINDOWS
    if windows and is_cmd_shell(shell):
        # ntpath, not the host's os.path: `windows=True` drives this
        # branch from POSIX too, where posixpath reads all of
        # `C:\Windows\System32\cmd.exe` as one long bare name and
        # silently drops the operator's chosen shell. The test is
        # against the basename rather than ntpath.isabs(), which
        # stopped counting a rooted `\Windows\System32\cmd.exe` as
        # absolute in 3.13: which cmd.exe a job runs must not depend
        # on the interpreter that scheduled it. Anything carrying a
        # directory or a drive is a path the operator spelled out;
        # only a bare name goes to ComSpec.
        spelled_out = ntpath.basename(shell) != shell
        kwargs = {"executable": shell} if spelled_out else {}
        return asyncio.create_subprocess_shell, [command], kwargs
    return asyncio.create_subprocess_exec, [shell, "-c", command], {}


def _decode_output_line(raw: bytes) -> str:
    """Decode one captured line (or unterminated tail) of job output.

    UTF-8 first, strictly: the overwhelmingly common case on every
    platform, and what POSIX tools and PowerShell 7 emit.  On Windows the
    native console tools (cmd.exe builtins, ``dir``, OS error messages,
    Windows PowerShell 5) emit the console's OEM code page instead, so a
    line that is not valid UTF-8 is retried through the Windows-only
    ``"oem"`` codec, keeping accented output from a non-English install
    intact rather than collapsing it to U+FFFD.  Anything still undecodable
    falls back to UTF-8 with replacement (the old unconditional behavior),
    so the reader task can never die on job-controlled bytes.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        if platform.IS_WINDOWS:
            try:
                # East Asian OEM code pages are multi-byte and can also
                # fail to decode; LookupError guards a Python built
                # without the codec.
                return raw.decode("oem")
            except (UnicodeDecodeError, LookupError):
                pass
        return raw.decode("utf-8", errors="replace")


# How many of the most recent output lines a JobOutputStream retains for the
# live web log tail. Independent of saveLimit (which bounds the text kept for
# failure reports); this only bounds the in-memory buffer the UI streams from.
LIVE_LOG_LIMIT = 1000

# Hard cap on the lines held in one subscriber's delivery queue. Only bites
# when a subscriber stalls on a chatty job; without it the queue grows to the
# run's ENTIRE output per stalled subscriber (the ring bounds the shared
# buffer, not this queue). On overflow the OLDEST line is dropped: the live
# tail is best-effort and a reconnect re-snapshots the ring.
LIVE_LOG_SUBSCRIBER_QUEUE_LIMIT = 8192

# How long a forcibly-terminated run waits for stdout/stderr EOF before its
# readers are cancelled and the captured output kept (see
# RunningJob._read_job_streams). Only reached when a descendant escaped the
# process-group kill. A fixed bound rather than killTimeout, which is
# legitimately 0 for jobs that would then lose output already produced.
KILLED_STREAM_DRAIN_TIMEOUT = 30.0

# Overall bound on one mail report's SMTP conversation. aiosmtplib only
# bounds each operation (60s default), and the report runs inside the job's
# completion sequence, so an unbounded stall would hold up retry arming. On
# expiry the report is logged as failed and the socket released.
MAIL_REPORT_TIMEOUT = 60.0

# How long _on_stop waits for the spawned job_started emission (spawned, not
# awaited, in start(): a stalled statsd send must not hold the daemon-wide
# spawn gate). A host that misses this window loses the start/stop pair.
STATSD_START_FLUSH_TIMEOUT = 2.0


class _MirrorWriter:
    """The stdout/stderr passthrough's single daemon-wide writer thread.

    Mirrored writes are blocking syscalls: done on the event-loop thread,
    a full pipe (a stopped ``docker logs``, a Ctrl+S'd console, a dead
    journald) would park the loop and freeze the whole daemon behind one
    wedged log consumer.  Batches queue here instead and one daemon
    thread writes them; a wedged consumer wedges only this thread, and
    the queue is bounded by batch count AND retained bytes, shedding the
    OLDEST batches so memory stays flat.

    The submit path NEVER logs, especially not under the lock: in the
    shed scenario stderr IS the wedged fd, so a synchronous handler write
    would park the loop with the lock held.  Submit only flags the shed;
    the writer thread logs it AFTER a successful write, once the consumer
    is provably draining again.

    One thread for both streams on purpose: it preserves enqueue order
    across stdout and stderr.  The thread starts lazily on the first
    mirrored batch and registers a bounded atexit drain, so an orderly
    shutdown flushes the tail without a wedged pipe holding the exit
    hostage.
    """

    #: Retained batches (one per drained read) while the consumer stalls.
    #: At the reader's chunk size a batch is a few KB, so the cap bounds a
    #: fully wedged consumer to a few MB, not the run's whole output.
    MAX_PENDING_BATCHES = 512

    #: Byte ceiling over the same queue: the batch count alone bounds
    #: nothing when batches are large (maxLineLength can be 16 MiB).  A
    #: single over-ceiling batch is still queued alone (newest output
    #: wins), so the true bound is this plus one batch.
    MAX_PENDING_BYTES = 8 * 1024 * 1024

    def __init__(self) -> None:
        self._batches: deque[tuple[str, str, str]] = deque()
        self._pending_bytes = 0
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._idle = threading.Event()
        self._idle.set()
        self._thread: Optional[threading.Thread] = None
        self.dropped_batches = 0
        self._drop_logged = False
        self._drop_warn_pending = False
        self._no_stream_logged = False

    def submit(self, job_name: str, stream_name: str, text: str) -> None:
        """Queue one passthrough batch; never blocks, sheds when full."""
        start = False
        size = len(text)
        with self._lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="cronstable-mirror",
                    daemon=True,
                )
                start = True
            while self._batches and (
                len(self._batches) >= self.MAX_PENDING_BATCHES
                or self._pending_bytes + size > self.MAX_PENDING_BYTES
            ):
                shed = self._batches.popleft()
                self._pending_bytes -= len(shed[2])
                self.dropped_batches += 1
                if not self._drop_logged:
                    # flag only; the writer thread logs it outside the lock
                    # (see the class docstring).
                    self._drop_logged = True
                    self._drop_warn_pending = True
            self._batches.append((job_name, stream_name, text))
            self._pending_bytes += size
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

    def _log_no_stream(self) -> None:
        # This latch stays set for the life of the process, where
        # _drop_logged re-arms: a backed-up consumer recovers, a missing
        # stream stays missing.  Only the writer thread reaches this, so
        # the latch needs no lock.
        if self._no_stream_logged:
            return
        self._no_stream_logged = True
        logger.warning(
            "the daemon has no stdout/stderr, so job output is not being "
            "mirrored to it (a Windows service has no standard streams); "
            "captured output still reaches the API, the reporters and "
            "archiveOutput"
        )

    def _run(self) -> None:
        while True:
            self._wake.wait()
            with self._lock:
                batch = self._batches
                self._batches = deque()
                self._pending_bytes = 0
                self._wake.clear()
            wrote = False
            for job_name, stream_name, text in batch:
                out = sys.stdout if stream_name == "stdout" else sys.stderr
                if out is None:
                    # A Windows service has no standard streams at all, so
                    # there is nothing to mirror TO.  Without this guard the
                    # write raises AttributeError on `out.buffer`, and the
                    # arm below turns that into a WARNING plus a traceback
                    # for every batch of every job's output, which is the
                    # loudest log the daemon can produce for a condition
                    # that holds for the whole run.  Log it once instead.
                    # The saved lines still hold everything the job wrote:
                    # capture, the log tail, the archive and every reporter
                    # read those.
                    self._log_no_stream()
                    continue
                try:
                    StreamReader._emit(out, text)
                    wrote = True
                except Exception:  # noqa: BLE001 - this thread must survive
                    # The daemon's own stream is broken or rejecting the
                    # payload; an escaping exception would silently kill
                    # the process's ONE mirror thread and end the
                    # passthrough for the daemon's life, so log and keep
                    # going, whatever the type.
                    logger.warning(
                        "job %s: could not mirror %s to the daemon's own "
                        "stream",
                        job_name,
                        stream_name,
                        exc_info=True,
                    )
            if wrote:
                # a write just succeeded, so the consumer is draining and
                # this cannot block behind a wedged fd (see class docstring)
                with self._lock:
                    warn = self._drop_warn_pending
                    self._drop_warn_pending = False
                    # re-arm: this episode is over (the consumer is
                    # provably draining again), so the NEXT backup gets
                    # its own warning. Without the reset the latch was
                    # per-process and every later episode shed job output
                    # silently.
                    self._drop_logged = False
                if warn:
                    logger.warning(
                        "passthrough mirror is backed up (its consumer is "
                        "not reading the daemon's output); shedding oldest "
                        "batches until it drains"
                    )
            with self._lock:
                if not self._batches:
                    self._idle.set()


#: The one mirror writer for the process; see :class:`_MirrorWriter`.
_MIRROR = _MirrorWriter()


class JobOutputStream:
    """In-memory, broadcastable view of a job run's captured output.

    Lines are pushed to live subscribers (the web UI's log tail) and kept
    in a bounded ring so a viewer connecting mid-run still sees recent
    context. Nothing is ever written to disk (the read-only-filesystem
    deployment story). Once a newer run's record supersedes this one the
    scheduler calls :meth:`release_lines`: a superseded ring is
    unreplayable and would otherwise pin one full ring per retained
    history record.
    """

    def __init__(self, limit: int = LIVE_LOG_LIMIT) -> None:
        # each item is (stream_name, line) with stream_name "stdout"/"stderr"
        self.lines: deque[tuple[str, str]] = deque(maxlen=limit)
        self._subscribers: list["asyncio.Queue"] = []
        self.closed = False
        # total lines ever published: `published - len(lines)` is the
        # ring's eviction count, so an archiver (Cron._archive_output) can
        # record the truncation.
        self.published = 0
        # lines a stalled subscriber's bounded queue overflowed and dropped;
        # observability only (the live tail is best-effort).
        self.dropped = 0

    @staticmethod
    def _offer(queue: "asyncio.Queue", item: Any) -> bool:
        """Enqueue for one subscriber, dropping its oldest line if full.

        Returns True when an item had to be evicted. Runs synchronously on
        the event-loop thread, so the get_nowait/put_nowait pair is race
        free; dropping the OLDEST keeps the newest output flowing and
        guarantees room for the end-of-stream sentinel.
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
            # run already finished: deliver the end sentinel now so a late
            # subscriber's read loop terminates after the buffered snapshot.
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

        Called when this record stops being its job's newest finished run:
        the log endpoints replay only the newest run, so a superseded ring
        is unreachable payload that would otherwise scale memory with
        history depth. ``published``/``dropped`` stay (shown in history
        rows), and subscribers already got the end sentinel via
        :meth:`close`, so nothing observes the lines vanishing.
        """
        self.lines.clear()


#: Bytes pulled from a job's pipe per read.  ``read`` returns as soon as
#: ANY data is buffered, so a bigger chunk never delays a live tail; it
#: only lets a chatty job's output be split in C instead of per line in
#: Python.
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
        self.save_top: list[str] = []
        self.save_bottom: deque[str] = deque()
        self.job_name = job_name
        self.save_limit = save_limit
        self.stream_name = stream_name
        self.stream_prefix = stream_prefix
        # Longest line kept, in BYTES before decoding.  _read reads in
        # chunks, not via readuntil, so asyncio's own StreamReader limit
        # does not bound a line; the cap is enforced by hand in _read.  The
        # daemon always passes maxLineLength; the fallback covers callers
        # (tests, benchmarks) that pass none.
        if max_line_length is None:
            max_line_length = getattr(
                stream, "_limit", DEFAULT_MAX_LINE_LENGTH
            )
        self.max_line_length = max_line_length
        # called with (stream_name, line) for each line read, so a live viewer
        # (the web UI) can tail output as the job produces it.
        self.on_line = on_line
        # lines awaiting one batched passthrough write to the daemon's own
        # stdout/stderr; flushed once per drained read (see _read).
        self._emit_buffer: list[str] = []
        self._emit_scheduled = False
        self._reader = asyncio.create_task(self._read(stream))
        self.discarded_lines = 0

    @staticmethod
    def _emit(out_stream, out_line: str) -> None:
        # Write bytes so we control the encoding, and use the STREAM'S OWN
        # encoding, not a hardcoded UTF-8: a Windows daemon whose output is
        # redirected to a pipe or log file declares the ANSI code page, and
        # UTF-8 bytes in that file read back as mojibake. Replacement (not
        # raising) for what the target encoding cannot carry; ASCII with
        # replacement remains the last-ditch fallback for a stream with a
        # broken or unknown encoding.
        #
        # A real console and almost every POSIX process declare UTF-8 (PEP
        # 538 coerces the C locale, PEP 540 turns UTF-8 mode on by default
        # from 3.15), so those streams get the same bytes either way. Under
        # an explicit non-UTF-8 locale they do not: non-ASCII job output
        # reaches the mirror as `?` rather than as UTF-8 the stream never
        # claimed it could carry. That side of the trade is deliberate,
        # since the mirror is read by whatever the operator pointed the
        # daemon's stdout at.
        try:
            encoding = getattr(out_stream, "encoding", None) or "utf-8"
            payload = out_line.encode(encoding, errors="replace")
            out_stream.buffer.write(payload)
        except (LookupError, UnicodeEncodeError):
            safe = out_line.encode("ascii", "replace").decode("ascii")
            out_stream.write(safe)
        out_stream.flush()

    def _flush_emit_buffer(self) -> None:
        self._emit_scheduled = False
        if not self._emit_buffer:
            return
        text = "".join(self._emit_buffer)
        self._emit_buffer.clear()
        # Hand the batch to the mirror's writer thread: this runs on the
        # EVENT LOOP thread, and a write to a full pipe would block the
        # whole daemon behind one wedged log consumer.
        _MIRROR.submit(self.job_name, self.stream_name, text)

    async def _read(self, stream):
        """Drain ``stream`` to EOF, splitting it into lines.

        Reads in chunks and splits in C rather than awaiting ``readline``
        per line, whose per-line bookkeeping costs several times the
        decode it surrounds.  Two things readuntil supplied for free are
        re-implemented here:

        * the ``maxLineLength`` cap: an over-cap complete line is
          dropped, and an unterminated run past the cap is dropped as it
          accumulates, both with a warning; whatever follows a drop is
          read as an ordinary line.
        * the unterminated tail at EOF still yields its line.

        Splitting on ``b"\\n"`` cannot cut a UTF-8 code point in half (no
        continuation byte is 0x0A) and only complete lines are decoded,
        so a multi-byte character straddling a chunk boundary decodes
        intact.
        """
        prefix = self.stream_prefix.format(
            job_name=self.job_name, stream_name=self.stream_name
        )
        limit_top = self.save_limit // 2
        limit_bottom = self.save_limit - limit_top
        stream_name = self.stream_name
        passthrough = stream_name in ("stdout", "stderr")
        cap = self.max_line_length
        on_line = self.on_line
        saving = self.save_limit > 0
        save_bottom = self.save_bottom
        discarded = self.discarded_lines
        save_top_append = self.save_top.append
        save_bottom_append = save_bottom.append
        save_bottom_popleft = save_bottom.popleft
        top_room = limit_top
        bottom_room = limit_bottom
        emit_buffer = self._emit_buffer
        emit_buffer_append = emit_buffer.append
        loop = asyncio.get_running_loop()
        # Bytes after the last newline seen: not a line until the next
        # chunk (or EOF) terminates it.  Held as a LIST of chunks plus a
        # running length and joined exactly once, so an unterminated run
        # stays linear instead of quadratic on the event-loop thread.
        tail_parts: list[bytes] = []
        tail_len = 0
        while True:
            chunk = await stream.read(_READ_CHUNK)
            if chunk:
                buffered = tail_len + len(chunk)
                parts = chunk.split(b"\n")
                rest = parts.pop()
                if parts:
                    # a newline in this chunk terminates the carried tail:
                    # join it onto the first segment, once.
                    if tail_parts:
                        parts[0] = b"".join(tail_parts) + parts[0]
                    tail_parts = [rest]
                    tail_len = len(rest)
                else:
                    # no newline at all: carry the chunk without copying
                    # anything that came before it.
                    tail_parts.append(rest)
                    tail_len = buffered
                if buffered > cap:
                    # a segment cannot outgrow the buffer it was cut from,
                    # so the per-line cap check only runs once the buffer
                    # itself has passed the cap.
                    parts = [p for p in parts if not self._too_long(p, cap)]
                # decoded per line: strict UTF-8 with an OEM-code-page
                # retry on Windows, never an exception (see
                # _decode_output_line).
                lines = [_decode_output_line(raw) + "\n" for raw in parts]
            elif tail_len and not self._over_cap(tail_len, cap):
                lines = [_decode_output_line(b"".join(tail_parts))]
            else:
                lines = []
            for line in lines:
                if on_line is not None:
                    on_line(stream_name, line)
                if passthrough:
                    emit_buffer_append(prefix + line)
                if saving:
                    if top_room:
                        top_room -= 1
                        save_top_append(line)
                    elif bottom_room:
                        bottom_room -= 1
                        save_bottom_append(line)
                    else:
                        # deque(maxlen) would evict silently; track discards
                        # explicitly to preserve the "N lines discarded"
                        # count.
                        save_bottom_popleft()
                        discarded += 1
                        save_bottom_append(line)
                else:
                    discarded += 1
            # Published before the next await, so a reader cancelled by
            # join()'s timeout still reports the count it had reached.
            self.discarded_lines = discarded
            if not chunk:
                # EOF: push out whatever the last drain accumulated (an
                # already-scheduled callback then finds an empty buffer).
                self._flush_emit_buffer()
                return
            if emit_buffer and not self._emit_scheduled:
                self._emit_scheduled = True
                loop.call_soon(self._flush_emit_buffer)
            if self._over_cap(tail_len, cap):
                # unterminated run past the cap: drop what has piled up and
                # keep reading. Measured on the running length, so it is
                # never joined into one buffer.
                tail_parts = []
                tail_len = 0

    def _too_long(self, raw: bytes, cap: int) -> bool:
        """Whether ``raw`` breaks the line cap, warning once when it does."""
        return self._over_cap(len(raw), cap)

    def _over_cap(self, size: int, cap: int) -> bool:
        """:meth:`_too_long` on a length alone, for the unjoined tail."""
        if size <= cap:
            return False
        logger.warning("job %s: ignored a very long line", self.job_name)
        return True

    async def join(self, timeout: Optional[float] = None) -> tuple[str, int]:
        """Drain to end-of-file; return ``(output, discarded_lines)``.

        EOF needs every write-end of the pipe closed, including any a
        descendant inherited, so a caller that just killed the job passes
        a bound (see RunningJob._read_job_streams). On expiry the read
        loop is cancelled and the output captured so far is returned;
        nothing already collected is lost.
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
            # chain feeds join without concatenating the three parts first
            output = "".join(
                itertools.chain(self.save_top, middle, self.save_bottom)
            )
        else:
            output = "".join(self.save_top)
        return output, self.discarded_lines


async def _resolve_secret_async(
    spec: Optional[dict[str, Any]], what: str
) -> Optional[str]:
    """:func:`config._resolve_secret`, off the event loop for a file source.

    The reporters run from the completion path, so a ``fromFile`` secret on
    a slow or hung mount (a Kubernetes secret volume, NFS) would block the
    whole scheduler on an ordinary open+read.  Same offload rule
    :func:`cronstable.jobapi.stage_secrets` applies at launch: only a file
    source pays the thread hop, since value and env sources cost less than
    the hop itself.
    """
    if spec and spec.get("fromFile"):
        return await asyncio.get_running_loop().run_in_executor(
            None, _resolve_secret, spec, what
        )
    return _resolve_secret(spec, what)


class Reporter:
    async def report(
        self, success: bool, job: "RunningJob", config: dict[str, Any]
    ) -> None:
        raise NotImplementedError  # pragma: no cover


class SentryReporter(Reporter):
    def __init__(self) -> None:
        # Remember the last (dsn, environment) we initialized the global
        # Sentry client with, so we don't rebuild the client/transport on
        # every single report.
        self._inited_key: Optional[tuple[str, Optional[str]]] = None

    async def report(
        self, success: bool, job: "RunningJob", config: dict[str, Any]
    ) -> None:
        config = config["sentry"]
        try:
            # Shared secret resolver: an unreadable fromFile or unset env
            # var is a clean skip, never a traceback out of the completion
            # path, and its messages name the config key so env var names
            # stay out of the logs.
            dsn = await _resolve_secret_async(config["dsn"], "sentry.dsn")
        except ConfigError as ex:
            logger.error("sentry: %s; not reporting", ex)
            return
        if dsn is None:
            return  # sentry disabled: early return

        # Imported past the early returns so the sentry_sdk import cost is
        # paid only when a job actually reports to Sentry.
        import sentry_sdk
        import sentry_sdk.utils

        # template_vars is rebuilt on every property access, so one read
        # serves the body render and every fingerprint line
        tvars = job.template_vars
        template = _compiled_template(config["body"])
        body = template.render(tvars)

        fingerprint = []
        for line in config["fingerprint"]:
            fingerprint.append(_compiled_template(line).render(tvars))

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
        self, success: bool, job: "RunningJob", config: dict[str, Any]
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
            password = await _resolve_secret_async(
                mail["password"], "mail.password"
            )
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
        # One overall bound on the whole conversation; see
        # MAIL_REPORT_TIMEOUT.
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
        mail: dict[str, Any],
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
        self, success: bool, job: "RunningJob", config: dict[str, Any]
    ) -> None:
        shell_config = config["shell"]

        if shell_config["command"] is None:
            return

        shell_kwargs: dict[str, Any] = {}
        if isinstance(shell_config["command"], list):
            create: Any = asyncio.create_subprocess_exec
            cmd = shell_config["command"]
        else:
            if shell_config["shell"]:
                create, cmd, shell_kwargs = shell_spawn(
                    shell_config["shell"], shell_config["command"]
                )
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
            # Rendered to the crontab line for the OBJECT form:
            # schedule_unparsed is Union[str, dict], and a dict here dies
            # in os.fsencode at spawn, silently disabling the shell
            # reporter for every object-schedule job.
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
        # Heartbeat detail, for an onLate/onFailure/onRecovery dispatch of
        # an inbound heartbeat (only HeartbeatContext carries these).
        # Exported on the same terms as the SLA block above: always
        # present, empty when this report is not a heartbeat's.
        hb_vars = getattr(job, "heartbeat_vars", None)
        if not isinstance(hb_vars, dict):
            hb_vars = {}
        for env_name, key in (
            ("CRONSTABLE_HEARTBEAT", "heartbeat"),
            ("CRONSTABLE_HEARTBEAT_STATE", "state"),
            ("CRONSTABLE_HEARTBEAT_REASON", "reason"),
            ("CRONSTABLE_HEARTBEAT_LAST_PING_AT", "last_ping_at"),
            ("CRONSTABLE_HEARTBEAT_EXPECTED_AT", "expected_at"),
            ("CRONSTABLE_HEARTBEAT_OVERDUE_SECONDS", "overdue_seconds"),
        ):
            value = hb_vars.get(key)
            env[env_name] = str(value) if value is not None else ""

        logger.debug("Executing shell report cmd: %s", cmd)
        # Same process-group isolation as the job itself, so the timeout kill
        # below reaches the reporter's descendants as a unit (see
        # platform.new_process_group_kwargs).
        kwargs = platform.new_process_group_kwargs()
        kwargs.update(shell_kwargs)
        try:
            proc = await create(*cmd, env=env, **kwargs)
        # OSError: a missing reporter binary or a spawn-time resource
        # failure is not a SubprocessError subclass (see RunningJob.start).
        # TypeError/ValueError: a non-string env value or an embedded NUL
        # must land here, not escape to _report_common's gather.
        except (subprocess.SubprocessError, OSError, TypeError, ValueError):
            logger.exception(
                "Error executing shell reporter of job %s", job.config.name
            )
            return

        # Bounded: report() runs INLINE on the reaper, so a reporter that
        # never exits would freeze completion handling for EVERY job
        # daemon-wide. On expiry the reporter's whole process group is
        # killed and the run's handling proceeds.
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
            # reap the killed child so it does not linger as a zombie; the
            # extra bound guarantees the reaper can never be wedged here.
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

    A receiver can quote the request target back (Express's "Cannot POST
    /<path>", gateway error pages), and ``webhook.url`` is a secret whose
    secret part IS the path or query, so the body cannot be logged raw.
    The HTML-escaped spelling is scrubbed too, since a server that echoes
    usually escapes what it echoes.
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


#: How long an idle webhook connection is kept pooled.  aiohttp's default
#: (15s) is shorter than a minutely job's gap, so every report would pay a
#: fresh connect and TLS handshake; 90 covers minutely reporting with
#: margin while idle sockets still go away promptly.
WEBHOOK_KEEPALIVE_SECONDS = 90

#: One connection pool per event loop for :class:`WebhookReporter`, the same
#: shape as the pooled statsd endpoints in cronstable.statsd.
#:
#: Weak keys reclaim nothing: aiohttp's connector stores the loop it was
#: built on, so the value holds its own key alive and an entry never expires
#: by itself.  The daemon releases its pool through
#: :func:`close_webhook_pool` on shutdown; any other loop's is swept by
#: :func:`_drop_dead_webhook_pools` on the next report.
_WEBHOOK_CONNECTORS: "weakref.WeakKeyDictionary[Any, Any]" = (
    weakref.WeakKeyDictionary()
)


def _drop_dead_webhook_pools() -> None:
    """Release the pools of loops that have gone away.

    Entries cannot expire on their own (the value holds its own key alive,
    see above), so this sweep covers processes that build a loop per unit
    of work, like the test suite.  The last loop's pool has no later
    report to sweep it: :func:`close_webhook_pool` handles the daemon's,
    and :func:`_close_webhook_pools_atexit` sweeps whatever is left, so
    pooling never prints aiohttp's "Unclosed connector" warning.
    """
    for dead in [lp for lp in list(_WEBHOOK_CONNECTORS) if lp.is_closed()]:
        _sync_close_webhook_pool(_WEBHOOK_CONNECTORS.pop(dead, None))


def _sync_close_webhook_pool(stale: Any) -> None:
    """Tear a connector's transports down without a running loop.

    ``close()`` is a coroutine and needs a loop; the private synchronous
    half does the part that matters (closing transports, returning
    handshake waiters).  An aiohttp version without it degrades to merely
    forgetting the connector.
    """
    closer = getattr(stale, "_close", None)
    if closer is None:
        return
    try:
        closer()
    except Exception as ex:  # noqa: BLE001 - degrade, never crash
        logger.debug("cannot close a stale webhook pool: %s", ex)


@atexit.register
def _close_webhook_pools_atexit() -> None:
    """Release every webhook pool still held at interpreter exit.

    The backstop for the last loop's pool in a process that never shuts
    the daemon down gracefully; releasing before GC-time teardown is what
    keeps aiohttp's "Unclosed connector" warning from firing.
    """
    while _WEBHOOK_CONNECTORS:
        try:
            _, stale = _WEBHOOK_CONNECTORS.popitem()
        except KeyError:  # pragma: no cover - emptied under us
            return
        _sync_close_webhook_pool(stale)


def _webhook_connector() -> Any:
    """The pooled connector for this loop's webhook reports."""
    # Re-imported per call: past the first report this is a sys.modules
    # hit, and it keeps this helper usable without a module-scope aiohttp
    # (WebhookReporter.report says why that import is deferred).
    import aiohttp

    loop = asyncio.get_running_loop()
    _drop_dead_webhook_pools()
    connector = _WEBHOOK_CONNECTORS.get(loop)
    # `closed` covers the shutdown-then-report order: close_webhook_pool
    # drops the entry, but a connector someone else closed must be replaced
    # too rather than handed out dead, which would fail every later report.
    if connector is None or connector.closed:
        # limit=0 (unlimited): aiohttp's default of 100 would cap the
        # whole daemon's in-flight webhook reports, and the wait for a
        # slot counts against each report's own ClientTimeout, so a
        # fleet-wide burst at :00 could time reports out on connection
        # acquisition alone.
        connector = aiohttp.TCPConnector(
            keepalive_timeout=WEBHOOK_KEEPALIVE_SECONDS, limit=0
        )
        _WEBHOOK_CONNECTORS[loop] = connector
    return connector


async def close_webhook_pool() -> None:
    """Close this loop's pooled webhook connections (daemon shutdown).

    Safe to call more than once and outside a loop; the next report opens
    a fresh pool.  Without it the idle sockets live until loop GC, which
    logs aiohttp's "Unclosed connector" on teardown.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    connector = _WEBHOOK_CONNECTORS.pop(loop, None)
    if connector is not None:
        # awaited: close() returns a waiter for the TLS shutdown
        # handshakes, and dropping it unawaited earns a DeprecationWarning
        # from aiohttp.
        await connector.close()


class WebhookReporter(Reporter):
    async def report(
        self, success: bool, job: "RunningJob", config: dict[str, Any]
    ) -> None:
        webhook = config["webhook"]

        try:
            # Shared secret resolver; see SentryReporter (clean skip on a
            # bad source; the URL itself is the secret here).
            url = await _resolve_secret_async(webhook["url"], "webhook.url")
        except ConfigError as ex:
            logger.error("webhook: %s; not reporting", ex)
            return
        if url is None:
            return  # webhook disabled: early return

        template = _compiled_template(webhook["body"])
        body = template.render(job.template_vars)

        headers = {"Content-Type": webhook["contentType"]}
        headers.update(webhook["headers"])

        # aiohttp is imported here, not at module top: this module is on
        # the daemon's unconditional import graph and the webhook reporter
        # is the only thing here that wants aiohttp, so a daemon whose
        # jobs never report over HTTP pays none of its import cost.
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=webhook["timeout"])
        # Encoded OUTSIDE the try below: a rendered body carrying a lone
        # surrogate (os.environ's surrogateescape via the template) raises
        # UnicodeEncodeError here, a template bug worth _report_common's
        # traceback rather than a "check the network" line.
        data = body.encode("utf-8")
        # A fresh session per report over a SHARED connector: sharing the
        # session would share its cookie jar across jobs, while the
        # connector owns the sockets, the pool and the SSL context.
        # connector_owner=False so leaving this `async with` closes the
        # session but leaves the pool open for the next report.
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=_webhook_connector(),
            connector_owner=False,
        ) as session:
            try:
                async with session.request(
                    webhook["method"],
                    url,
                    data=data,
                    headers=headers,
                ) as resp:
                    if resp.status >= 400:
                        # never log the URL: webhook URLs embed a secret
                        # token.  The body is scrubbed for it too, and
                        # BEFORE the slice, which could cut a needle in
                        # half and leave a prefix of the token behind.
                        # errors="replace": a body that does not match
                        # its declared charset must not raise out of a
                        # request that COMPLETED and cost this line its
                        # status code.
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
                # catch-all: the URL is the credential, and a spelling
                # yarl rejects raises InvalidUrlClientError, whose str()
                # IS the URL.  Report the failure kind only, keeping the
                # URL out of the log.  UnicodeError covers the one
                # non-ClientError failure: a host idna rejects at connect
                # time raises UnicodeEncodeError out of getaddrinfo.  The
                # non-connect Unicode failures stay out on purpose: the
                # body decode is errors="replace" and the request body's
                # encode happens before the try.
                logger.error(
                    "webhook reporter of job %s: request failed (%s);"
                    " check webhook.url and the network",
                    job.config.name,
                    type(exc).__name__,
                )


class PushReporter(Reporter):
    """End-to-end encrypted push alerts to paired devices.

    The thin edge only: reads the per-job ``push`` block and hands the
    context to the daemon-global :class:`cronstable.push.PushService`.
    Config validation guarantees a ``push:`` section exists whenever this
    is enabled, so a missing service is a wiring bug worth an error line,
    not a silent drop.
    """

    async def report(
        self, success: bool, job: "RunningJob", config: dict[str, Any]
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


#: The Windows Event Log contract, and a PUBLIC one: an Event Viewer custom
#: view, a Windows Event Forwarding subscription and every SIEM rule key on
#: these numbers, so a shipped row keeps its meaning forever.
#:
#: outcome -> (event id, wType, wCategory).
#:
#: One band per subject, so a single rule can express "anything that happened
#: to a job": 1000 to 1003 is contiguous, and daemon/orchestration events
#: start a fresh decade at 1010.  The band starts at 1000 rather than 1
#: because an unregistered source writes into the Application log, where
#: single- and double-digit ids collide with half of Windows.
#:
#: Plain small positive integers, with no severity or customer bits folded
#: in: Event Viewer and the modern EventLog API report an id masked to its
#: low 16 bits, so a number carrying 0x2000_0000 would be documented as one
#: value and displayed as another.  Severity travels in wType, which is
#: where every consumer already reads it.
EVENTLOG_EVENTS: dict[str, tuple[int, int, int]] = {
    "success": (1000, platform.EVENTLOG_INFORMATION_TYPE, 1),
    "failure": (1001, platform.EVENTLOG_ERROR_TYPE, 1),
    "permanent-failure": (1002, platform.EVENTLOG_ERROR_TYPE, 1),
    "late": (1003, platform.EVENTLOG_WARNING_TYPE, 1),
    "event": (1010, platform.EVENTLOG_INFORMATION_TYPE, 2),
    "event-alert": (1011, platform.EVENTLOG_ERROR_TYPE, 2),
    # inbound heartbeats: the thing cronstable watches but does not run
    "heartbeat-down": (1020, platform.EVENTLOG_ERROR_TYPE, 3),
    "heartbeat-up": (1021, platform.EVENTLOG_INFORMATION_TYPE, 3),
}

#: What each insertion string means, BY POSITION, which is the other half of
#: the contract.  An unregistered source has no message table to name its
#: fields, and a forwarder ships them as ``<Data>`` elements in order, so the
#: arity is fixed for every outcome and an unused field is ``""`` rather than
#: absent.  Appending a twelfth field is additive and safe; reordering or
#: removing one of these is not.
EVENTLOG_STRING_FIELDS = (
    "summary",
    "name",
    "outcome",
    "host",
    "exitCode",
    "failReason",
    "runId",
    "startedAt",
    "schedule",
    "detail",
    "output",
)

#: Per-field ceiling for everything except ``output``.
EVENTLOG_MAX_FIELD_CHARS = 1024

#: Ceiling on the captured-output tail.
EVENTLOG_MAX_OUTPUT_CHARS = 8000

#: Events queued for one writer thread before further ones are dropped.
#: Bounded rather than unbounded because the failure this exists to survive
#: is a wedged EventLog service, and an unbounded queue would turn that into
#: unbounded memory in the daemon.
EVENTLOG_QUEUE_LIMIT = 1000

#: How long shutdown waits for queued events to reach the service.
EVENTLOG_FLUSH_TIMEOUT = 5.0

#: Hard cap on distinct live writers.  See :data:`_EVENTLOG_WRITERS` for why
#: a dict keyed on a config string is not self-limiting.
EVENTLOG_MAX_WRITERS = 8

#: What a cut field ends with, so a truncated tail is never read as the whole
#: story.  Named because :func:`_eventlog_safe` sizes the kept prefix off its
#: length, which is what keeps a capped field exactly at its ceiling.
_EVENTLOG_TRUNCATED = "...[truncated]"


def _eventlog_safe(value: Any, limit: int) -> str:
    """One insertion string: never None, never NUL, always encodable.

    ``None`` becomes ``""``, because a NULL in the ``LPCWSTR`` array renders
    as nothing and shifts every later position an operator (or a parser
    reading ``EventData/Data`` by index) is counting on.

    An embedded NUL becomes a space: ctypes accepts it into the wide buffer
    and the API then truncates the field there, silently.

    Lone surrogates are folded out through a utf-16 round trip.  They reach
    ``template_vars`` from ``os.environ`` via surrogateescape, the same
    hazard the webhook reporter documents, and they would otherwise raise
    UnicodeEncodeError inside the ctypes conversion, which happens on the
    writer thread where the fan-out's gather cannot see it.
    """
    if value is None:
        text = ""
    elif isinstance(value, str):
        text = value
    else:
        text = str(value)
    text = text.replace("\x00", " ")
    if not text.isascii():
        text = text.encode("utf-16-le", "replace").decode(
            "utf-16-le", "replace"
        )
    if len(text) > limit:
        # Marked, so nobody reads a cut tail as the whole story, and sized
        # off the marker so the result is exactly `limit` and the ceiling
        # arithmetic in the caller stays exact.
        keep = max(0, limit - len(_EVENTLOG_TRUNCATED))
        text = text[:keep] + _EVENTLOG_TRUNCATED
    return text


def _eventlog_summary(outcome: str, tvars: dict[str, Any]) -> str:
    """Field 0: the one line a human reads in the Event Viewer list."""
    name = tvars.get("name")
    host = tvars.get("host")
    if outcome in ("event", "event-alert"):
        return "cronstable event {} on {} concerning {}".format(
            tvars.get("event"), host, name
        )
    if outcome == "late":
        return "Cron job {!r} is overdue on {} ({})".format(
            name, host, tvars.get("sla_check")
        )
    if outcome in ("heartbeat-down", "heartbeat-up"):
        if outcome == "heartbeat-up":
            return "Heartbeat {!r} recovered on {}".format(name, host)
        return "Heartbeat {!r} is down on {} ({})".format(
            name, host, tvars.get("reason")
        )
    if outcome == "success":
        return "Cron job {!r} succeeded on {}".format(name, host)
    what = "failed permanently" if outcome == "permanent-failure" else "failed"
    return "Cron job {!r} {} on {} (exit {}): {}".format(
        name, what, host, tvars.get("exit_code"), tvars.get("fail_reason")
    )


def _eventlog_detail(outcome: str, tvars: dict[str, Any]) -> str:
    """Field 9: the per-outcome extras that have no column of their own."""
    if outcome == "late":
        return "check={} threshold={}s observed={}s lastSuccess={}".format(
            tvars.get("sla_check"),
            tvars.get("threshold_seconds"),
            tvars.get("observed_seconds"),
            tvars.get("last_success_at"),
        )
    if outcome in ("event", "event-alert"):
        return "event={}".format(tvars.get("event"))
    if outcome in ("heartbeat-down", "heartbeat-up"):
        return "state={} reason={} lastPing={} overdue={}s".format(
            tvars.get("state"),
            tvars.get("reason"),
            tvars.get("last_ping_at"),
            tvars.get("overdue_seconds"),
        )
    usage = []
    for key in ("cpu_seconds", "max_rss_bytes"):
        value = tvars.get(key)
        if value is not None:
            usage.append("{}={}".format(key, value))
    return " ".join(usage)


def _eventlog_outcome(ctx: Any, config: dict[str, Any], success: bool) -> str:
    """Which :data:`EVENTLOG_EVENTS` row one report belongs to.

    A reporter is handed ``(success, ctx, config)`` and nothing else, so
    which hook is firing has to be recovered from those three.  All three
    are enough, and none of it needs a new attribute on the hot path:

    * a notify event is the only context carrying ``event``;
    * an SLA breach is the only one carrying ``sla_vars``;
    * a heartbeat transition is the only one carrying ``heartbeat_vars``,
      and its ``success`` says which direction it moved;
    * onFailure and onPermanentFailure are told apart by the IDENTITY of the
      report dict.  Each hook's block is an independent ``copy.deepcopy`` of
      ``_REPORT_DEFAULTS`` and ``mergedicts`` is copy-on-write, so the test
      is exact both for a job that configured the hooks and for one that
      wrote neither, which still points at a distinct per-hook default
      object.  ``test_eventlog_hook_report_blocks_never_alias`` pins the
      invariant that rests on, because it is an implementation detail of
      config.py that a well-meant deduplication there could quietly break.

    ``getattr`` throughout: the notify context's job shim carries
    ``__slots__`` with four names and no ``onPermanentFailure``, and the
    fan-out's ``return_exceptions`` gather would turn an AttributeError
    raised here into a log line rather than a test failure.
    """
    if getattr(ctx, "event", None) is not None:
        return "event" if success else "event-alert"
    if getattr(ctx, "sla_vars", None) is not None:
        return "late"
    if getattr(ctx, "heartbeat_vars", None) is not None:
        return "heartbeat-up" if success else "heartbeat-down"
    if not success:
        job_config = getattr(ctx, "config", None)
        permanent = getattr(job_config, "onPermanentFailure", None)
        if isinstance(permanent, dict) and config is permanent.get("report"):
            return "permanent-failure"
        return "failure"
    return "success"


def eventlog_event_strings(
    ctx: Any, outcome: str, *, include_output: bool
) -> list[str]:
    """The :data:`EVENTLOG_STRING_FIELDS` vector for one report.

    Built off ``ctx.template_vars`` rather than by reaching into the
    context's attributes: that dict is the documented cross-context
    contract, all three reporting contexts fill it through the same
    builder, and the notify context's job shim carries ``__slots__``, so
    attribute probing is the brittle way to ask the same question.
    """
    # every template_vars implementation builds a fresh dict per access,
    # and this function only reads it, so the dict is used as handed over
    tvars = ctx.template_vars
    output = ""
    if include_output:
        output = _eventlog_safe(
            tvars.get("stderr") or tvars.get("stdout") or "",
            EVENTLOG_MAX_OUTPUT_CHARS,
        )
    field = EVENTLOG_MAX_FIELD_CHARS
    return [
        _eventlog_safe(_eventlog_summary(outcome, tvars), field),
        _eventlog_safe(tvars.get("name"), field),
        _eventlog_safe(outcome, field),
        _eventlog_safe(tvars.get("host"), field),
        _eventlog_safe(tvars.get("exit_code"), field),
        _eventlog_safe(tvars.get("fail_reason"), field),
        _eventlog_safe(tvars.get("run_id"), field),
        _eventlog_safe(tvars.get("started_at"), field),
        _eventlog_safe(tvars.get("schedule"), field),
        _eventlog_safe(_eventlog_detail(outcome, tvars), field),
        output,
    ]


class _EventLogWriter:
    """One daemon thread owning one source handle and its write queue.

    ``ReportEventW`` is a synchronous RPC to the EventLog service and can
    block on a stalled disk or a busy service, and so can
    ``RegisterEventSourceW``, which is why the handle is opened on this
    thread rather than on the reporter's first call.  Reports run INLINE on
    the reaper (the reason the shell reporter carries a timeout), so neither
    call may be reached from the event loop.

    A dedicated thread rather than ``run_in_executor(None, ...)``: the
    default pool is shared with the durable-state writes and the fromFile
    secret resolver, and a wedged event write would hold one of its handful
    of slots for every report.  A dedicated ThreadPoolExecutor was rejected
    for a sharper reason: concurrent.futures registers an atexit hook that
    JOINS its worker threads, so one stuck ReportEventW would hang
    interpreter exit, which is exactly the shutdown behavior this platform
    work spent several fixes on.  A daemon thread cannot; the OS reclaims
    it.

    Fire and forget, therefore: :meth:`submit` does a bounded
    ``put_nowait`` and returns, so the reporter never awaits anything, and a
    failed write is logged from the thread rather than surfaced to the
    fan-out.  The alternative is delaying every job's completion on an OS
    service.
    """

    def __init__(self, source: str) -> None:
        self.source = source
        self._queue: Queue = Queue(EVENTLOG_QUEUE_LIMIT)
        self._handle: Optional[int] = None
        self._dropped = 0
        self._logged_codes: set[int] = set()
        self._thread = threading.Thread(
            target=self._run,
            name="cronstable-eventlog-{}".format(source),
            daemon=True,
        )
        self._thread.start()

    def submit(self, record: tuple[int, int, int, list[str]]) -> bool:
        """Queue one record.  False when the queue is full."""
        try:
            self._queue.put_nowait(record)
        except Full:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 100 == 0:
                logger.error(
                    "eventlog: the writer for source %r is not keeping up; "
                    "%d record(s) dropped so far",
                    self.source,
                    self._dropped,
                )
            return False
        return True

    def _write(self, record: tuple[int, int, int, list[str]]) -> None:
        event_type, category, event_id, strings = record
        if self._handle is None:
            self._handle = platform.open_event_log(self.source)
        if self._handle is None:
            self._log_once(
                0,
                "eventlog: could not open the event source %r; "
                "the record was dropped",
                self.source,
            )
            return
        code = platform.write_event_log(
            self._handle,
            event_type=event_type,
            category=category,
            event_id=event_id,
            strings=strings,
        )
        if code == platform.EVENTLOG_ERROR_INVALID_HANDLE:
            # The EventLog service restarted under us; re-registering the
            # source is the whole repair, so do it and retry this record
            # exactly once rather than dropping the alert.
            platform.close_event_log(self._handle)
            self._handle = platform.open_event_log(self.source)
            if self._handle is not None:
                code = platform.write_event_log(
                    self._handle,
                    event_type=event_type,
                    category=category,
                    event_id=event_id,
                    strings=strings,
                )
        if code:
            self._log_once(
                code,
                "eventlog: writing to source %r failed with code %s; "
                "the record was dropped",
                self.source,
                code,
            )

    def _log_once(self, code: int, message: str, *args: Any) -> None:
        # One line per distinct failure code, so a sink that is permanently
        # broken cannot become the log.
        if code in self._logged_codes:
            return
        self._logged_codes.add(code)
        logger.error(message, *args)

    def _run(self) -> None:
        try:
            while True:
                record = self._queue.get()
                try:
                    if record is None:
                        return
                    self._write(record)
                except Exception:  # noqa: BLE001 - a writer thread never dies
                    logger.exception("eventlog: unexpected writer failure")
                finally:
                    self._queue.task_done()
        finally:
            # The thread owns the handle, so it can release it here with
            # no caller waiting.  It has to be here rather than in join(),
            # because retire_event_log_writers drops a renamed writer
            # WITHOUT joining it (a reload runs on the scheduler's own
            # loop iteration, and the bounded drain belongs to
            # shutdown).  A close that lived only in join() therefore
            # leaked one source handle per reload that renamed the source:
            # unbounded, and invisible to EVENTLOG_MAX_WRITERS, which caps
            # the live registry rather than what has already left it.
            handle, self._handle = self._handle, None
            if handle is not None:
                platform.close_event_log(handle)

    def stop(self) -> None:
        """Ask the thread to finish once it has drained what is queued."""
        try:
            self._queue.put_nowait(None)
        except Full:
            pass

    def join(self, timeout: float) -> None:
        """Wait out the drain.  The writer thread closes the handle."""
        self._thread.join(timeout)


#: One writer per distinct ``source`` name.
#:
#: NOT the shape ``_WEBHOOK_CONNECTORS`` uses.  That is a WeakKeyDictionary
#: keyed on the event loop, swept when a loop dies and backstopped by an
#: atexit, so its entries retire themselves.  This is keyed on a config
#: STRING, and a config reload can change that string any number of times in
#: one process, so nothing here retires an entry by itself.  A leaked entry
#: is a leaked OS thread and a leaked source handle, so there are two
#: guards: :func:`retire_event_log_writers` drops writers the live config no
#: longer names, and :data:`EVENTLOG_MAX_WRITERS` refuses to mint past a
#: hard cap, so even a pathological reload loop degrades to "no new events"
#: rather than exhausting threads.
_EVENTLOG_WRITERS: dict[str, _EventLogWriter] = {}

#: Whether the writer cap has already been logged, so hitting it repeatedly
#: costs one line rather than one per report.
_EVENTLOG_CAP_LOGGED = False


def _eventlog_writer(source: str) -> Optional[_EventLogWriter]:
    """The writer for ``source``, minting one if the cap allows."""
    global _EVENTLOG_CAP_LOGGED
    writer = _EVENTLOG_WRITERS.get(source)
    if writer is not None:
        return writer
    if len(_EVENTLOG_WRITERS) >= EVENTLOG_MAX_WRITERS:
        if not _EVENTLOG_CAP_LOGGED:
            _EVENTLOG_CAP_LOGGED = True
            logger.error(
                "eventlog: refusing to open more than %d event sources "
                "(wanted %r); reports to it are dropped",
                EVENTLOG_MAX_WRITERS,
                source,
            )
        return None
    writer = _EventLogWriter(source)
    _EVENTLOG_WRITERS[source] = writer
    return writer


def retire_event_log_writers(live_sources: set[str]) -> None:
    """Stop and drop writers the running config no longer names.

    Called from the reload path.  Non-blocking per writer: the thread is
    asked to finish once it has drained, and is not waited for.  The bounded
    drain belongs to shutdown, not to a config reload, which runs on the
    scheduler's own loop iteration.

    Because it does not wait, the thread releases the source handle itself
    as it exits (see :meth:`_EventLogWriter._run`).  Once a writer is
    popped from the registry nothing else can reach it, so a release that
    needed a join would never run on this path.
    """
    for source in [s for s in _EVENTLOG_WRITERS if s not in live_sources]:
        _EVENTLOG_WRITERS.pop(source).stop()


async def close_event_log_writers() -> None:
    """Drain and stop every writer.  The sibling of close_webhook_pool.

    Bounded by :data:`EVENTLOG_FLUSH_TIMEOUT`, so a wedged EventLog service
    delays shutdown by that much and no more.  The joins run on the default
    executor, which is safe at the point this is called specifically because
    the state backend has already stopped by then, so the pool it briefly
    occupies is otherwise idle.  Safe to call twice, and with no writer ever
    created.
    """
    writers = list(_EVENTLOG_WRITERS.values())
    _EVENTLOG_WRITERS.clear()
    if not writers:
        return
    for writer in writers:
        writer.stop()

    def _join() -> None:
        # one deadline shared by every writer: all stop sentinels are
        # already queued, so the drains overlap and N wedged threads still
        # cost EVENTLOG_FLUSH_TIMEOUT total, not N times it
        deadline = time.monotonic() + EVENTLOG_FLUSH_TIMEOUT
        for writer in writers:
            writer.join(max(0.0, deadline - time.monotonic()))

    await asyncio.get_running_loop().run_in_executor(None, _join)


@atexit.register
def _close_event_log_writers_atexit() -> None:
    """Best-effort drain for a process that never shuts down gracefully.

    The threads are daemon threads, so the OS reclaims them either way; this
    exists so a short-lived process flushes what it queued instead of
    dropping it.  A hard kill still drops the queue, which the Event Log
    documentation states outright rather than implying a durability this
    design does not have.
    """
    writers = [_EVENTLOG_WRITERS.pop(s) for s in list(_EVENTLOG_WRITERS)]
    for writer in writers:
        writer.stop()  # all sentinels first, so the drains overlap
    deadline = time.monotonic() + EVENTLOG_FLUSH_TIMEOUT
    for writer in writers:
        writer.join(max(0.0, deadline - time.monotonic()))


class EventLogReporter(Reporter):
    """Job outcomes as Windows Event Log records (Windows only).

    Writes where a Windows shop's monitoring already looks: Event Viewer, a
    Windows Event Forwarding subscription, SCOM, every SIEM connector.  What
    it writes is a stable event id plus a fixed-arity insertion-string
    vector, never a rendered template, because the id and the field
    positions ARE the contract and a free-text override of either is a
    contract this reporter cannot keep.  An operator who wants prose has the
    shell and webhook reporters.

    On POSIX it is a no-op, and the config load has already said so once,
    naming every hook that enabled it.
    """

    async def report(
        self, success: bool, job: "RunningJob", config: dict[str, Any]
    ) -> None:
        conf = config.get("eventlog") or {}
        if not conf.get("enabled"):
            return  # event log reporting disabled: early return
        if not platform.IS_WINDOWS:
            return  # no Event Log here; warned once at config load
        outcome = _eventlog_outcome(job, config, success)
        event_id, event_type, category = EVENTLOG_EVENTS[outcome]
        strings = eventlog_event_strings(
            job, outcome, include_output=bool(conf.get("includeOutput"))
        )
        writer = _eventlog_writer(conf.get("source") or "cronstable")
        if writer is not None:
            writer.submit((event_type, category, event_id, strings))


def report_config_enabled(report_config: dict[str, Any]) -> bool:
    """Whether any of the six reporters would actually fire for this config.

    Mirrors each reporter's own disabled early-return EXACTLY, so a caller
    can skip scheduling a fan-out every reporter would drop on arrival;
    the common case (no reporter configured) must cost dict probes, not
    task spawns.
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
    if (report_config.get("push") or {}).get("enabled"):
        return True
    # eventlog last, with the platform tested before the dict: on POSIX
    # the reporter is a no-op and the attribute read is cheaper than the
    # dict probes (dict first measured +15.6% on job.report_noop_100k at
    # the 1.2.40 perf gate). platform.IS_WINDOWS is read at call time
    # because tests monkeypatch it. The platform test is part of the
    # mirror: the reporter's own second early return IS the platform, so
    # an eventlog-only config on Linux reads as disabled here too.
    # Otherwise every completion would schedule a fan-out for six
    # reporters that all drop it on arrival.
    if not platform.IS_WINDOWS:
        return False
    return bool((report_config.get("eventlog") or {}).get("enabled"))


#: The key set every reporting context's ``template_vars`` exposes.
#:
#: A user-facing contract (wiki/Reporting.md documents the names).
#: :func:`_base_template_vars` builds exactly these keys; the three
#: contexts (:meth:`RunningJob.template_vars`,
#: :meth:`SlaBreachContext.template_vars`,
#: :meth:`NotifyEventContext.template_vars`) merge their extras on top.
#: This tuple is the single source of truth they are checked against in
#: tests/test_job.py; the drift is invisible at runtime, since a
#: template referencing a missing name renders empty rather than
#: raising.
STANDARD_TEMPLATE_VARS = (
    "name",
    "success",
    "fail_reason",
    "stdout",
    "stderr",
    "exit_code",
    "command",
    "shell",
    "environment",
    "host",
    "schedule",
    "started_at",
    "run_id",
    "cpu_seconds",
    "cpu_user_seconds",
    "cpu_system_seconds",
    "max_rss_bytes",
)


def _base_template_vars(
    ctx: "RunningJob | SlaBreachContext | NotifyEventContext"
    " | HeartbeatContext",
    *,
    success: bool,
    schedule: str,
    started_at: str | None = None,
    run_id: str | None = None,
    usage: ResourceUsage | None = None,
) -> dict:
    """The STANDARD_TEMPLATE_VARS keys, read off one reporting context.

    Callers pass the run-shaped values their context actually has; the
    defaults are the "nothing ran" shape the SLA and notify contexts
    share (started_at/run_id/resource keys all None).
    """
    return {
        "name": ctx.config.name,
        "success": success,
        "fail_reason": ctx.fail_reason,
        "stdout": ctx.stdout,
        "stderr": ctx.stderr,
        "exit_code": ctx.retcode,
        "command": ctx.config.command,
        "shell": ctx.config.shell,
        "environment": ctx.env,
        "host": report_hostname(),
        "schedule": schedule,
        "started_at": started_at,
        "run_id": run_id,
        # resource accounting for report templates; all None when the run
        # was not monitored (monitorResources off / unavailable).
        "cpu_seconds": usage.cpu_total_seconds if usage else None,
        "cpu_user_seconds": usage.cpu_user_seconds if usage else None,
        "cpu_system_seconds": usage.cpu_system_seconds if usage else None,
        "max_rss_bytes": usage.max_rss_bytes if usage else None,
    }


async def _fan_out_reports(
    ctx: "RunningJob | SlaBreachContext | NotifyEventContext"
    " | HeartbeatContext",
    success: bool,
    report_config: dict,
    error_fmt: str,
    error_arg: str,
) -> None:
    """Fan one report out to every reporter.

    One reporter failing never blocks the rest: exceptions land in the
    log (as ``error_fmt % (error_arg, exc)``), not in the caller.
    """
    # Instance lookup, not RunningJob.REPORTERS: tests shadow a job's
    # reporter list per instance. The non-job contexts fall back to the
    # class list; they are duck-typed on purpose, quacking like the
    # RunningJob slice each reporter actually reads.
    reporters = getattr(ctx, "REPORTERS", RunningJob.REPORTERS)
    results = await asyncio.gather(
        *[
            reporter.report(success, ctx, report_config)  # type: ignore[arg-type]
            for reporter in reporters
        ],
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            logger.error(error_fmt, error_arg, result, exc_info=result)


class JobRetryState:
    def __init__(
        self, initial_delay: float, multiplier: float, max_delay: float
    ) -> None:
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.delay = initial_delay
        self.count = 0  # number of times retried
        self.task: asyncio.Task | None = None
        self.cancelled = False
        # the instant the currently-armed retry will fire and the delay it
        # is sleeping out; set by Cron.schedule_retry_job for the
        # dashboard's countdown, None while no retry is pending.
        self.next_retry_at: datetime | None = None
        self.scheduled_delay: float | None = None
        # the instant this ladder's current attempt was ARMED. Copied into
        # a cross-node HANDOFF record's ``armedAt`` so the new owner's
        # superseded-by-run guard anchors on the original arm time, not
        # the hand-off instant; anchoring on the latter could re-run a run
        # completed between arming and hand-off (a double-fire).
        self.armed_at: datetime | None = None

    def next_delay(self) -> float:
        delay = self.delay
        self.delay = min(delay * self.multiplier, self.max_delay)
        self.count += 1
        return delay


class RunningJob:
    REPORTERS: list[Reporter] = [
        SentryReporter(),
        MailReporter(),
        ShellReporter(),
        WebhookReporter(),
        PushReporter(),
        EventLogReporter(),
    ]

    def __init__(
        self,
        config: JobConfig,
        retry_state: Optional[JobRetryState],
        *,
        extra_env: Optional[dict[str, str]] = None,
        state_token: Optional[str] = None,
        run_id: Optional[str] = None,
        dag_ref: Optional[Any] = None,
    ) -> None:
        self.config = config
        # when set, this run is one DAG task instance: the reaper routes
        # its completion to cronstable.dagrun instead of the record/retry
        # path. An opaque marker carrying (dag, run_key, taskkey, ...).
        self.dag_ref = dag_ref
        # environment the daemon injects on top of the job's own (loopback
        # state-API URL, per-run bearer token, run context); applied after
        # config.environment so it wins over a same-named user override.
        # state_token is the daemon's cleanup handle for revoking the
        # token (see Cron._handle_finished_job); run_id identifies this
        # run in the durable ledger.
        self.extra_env = extra_env or {}
        self.state_token = state_token
        self.run_id = run_id
        self.proc: asyncio.subprocess.Process | None = None
        self.retcode: int | None = None
        # wall-clock instant this run started, for the web UI's run history;
        # set in start() so even a failed launch carries a timestamp.
        self.started_at: datetime | None = None
        # live, broadcastable view of this run's captured output (web UI tail)
        self.output = JobOutputStream()
        self._stderr_reader: StreamReader | None = None
        self._stdout_reader: StreamReader | None = None
        self.stderr: str | None = None
        self.stdout: str | None = None
        self.stderr_discarded = 0
        self.stdout_discarded = 0
        self.execution_deadline: float | None = None
        self.retry_state = retry_state
        self.env: dict[str, str] | None = None
        # per-run CPU/memory accounting (opt-in via monitorResources):
        # _resource_monitor samples the process tree; resource_usage holds
        # the finished result (None when off/unavailable), finalized in
        # _on_stop before the statsd emission that reports it.
        self._resource_monitor: Optional[ResourceMonitor] = None
        self.resource_usage: Optional[ResourceUsage] = None
        # set when the subprocess could not be launched at all (e.g. the
        # command does not exist). Lets wait() treat it as a normal job
        # failure instead of raising RuntimeError("process is not running").
        self.start_failed = False
        # guards against _on_stop running twice (cancel() racing wait())
        self._stopped = False
        # set by cancel(): this run was forcibly terminated. Read by
        # _read_job_streams, which then bounds its wait for pipe EOF
        # instead of trusting a killed process tree to close its output.
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
            self.statsd_writer: StatsdJobMetricWriter | None = (
                StatsdJobMetricWriter(
                    host=statsd_config["host"],
                    port=statsd_config["port"],
                    prefix=statsd_config["prefix"],
                    job=self,
                )
            )
        else:
            self.statsd_writer = None
        # the spawned job_started emission (see start()); _on_stop joins it,
        # bounded, so job_started still precedes job_stopped on the wire.
        self._start_telemetry: asyncio.Task | None = None

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
        # Finalize resource accounting before statsd reports it. _on_stop
        # is the single idempotent choke point every completion path
        # funnels through, so usage is captured exactly once. Guarded so a
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
        task = self._start_telemetry
        self._start_telemetry = None
        if task is not None and task.done():
            # retrieve the outcome anyway, or an exception that escaped
            # _on_start's OSError net surfaces at GC time as "Task
            # exception was never retrieved". Cancelled tasks carry no
            # outcome to retrieve.
            if not task.cancelled() and task.exception() is not None:
                logger.warning(
                    "Job %s: failed to send statsd job_started metric",
                    self.config.name,
                    exc_info=task.exception(),
                )
        elif task is not None:
            # bounded join so the start datagram still precedes the stop
            # one; see STATSD_START_FLUSH_TIMEOUT.
            try:
                await asyncio.wait_for(task, STATSD_START_FLUSH_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                # the documented "host that misses this window" case: the
                # open is still in flight and stays pooled, nothing to say
                pass
            except Exception as ex:  # noqa: BLE001 - best-effort telemetry
                # same failure as the done branch above, so log it the same
                # way: which arm runs is pure timing, and swallowing here
                # made an identical fault diagnosable or invisible by race.
                logger.warning(
                    "Job %s: failed to send statsd job_started metric",
                    self.config.name,
                    exc_info=ex,
                )
        if self.statsd_writer:
            # best-effort: the statsd machinery can raise beyond OSError,
            # and a telemetry failure must never break the completion
            # accounting every run funnels through here.
            try:
                await self.statsd_writer.job_stopped()
            except Exception:  # noqa: BLE001 - telemetry is best-effort
                logger.warning(
                    "Job %s: failed to send statsd job_stopped metric",
                    self.config.name,
                    exc_info=True,
                )

    async def start(self) -> None:
        if self.proc is not None:
            raise RuntimeError("process already running")
        config = self.config
        self.started_at = datetime.now(timezone.utc)
        # Isolate the job in its own process group, so cancel() can take its
        # whole descendant tree down as a unit rather than only the process we
        # spawned -- see cronstable.platform.new_process_group_kwargs.
        # The job's scheduling priority rides along on Windows, where the
        # creation flags are the only race-free place to set one; POSIX is
        # served by the renice below, once there is a group to renice.
        kwargs: dict[str, Any] = platform.new_process_group_kwargs(
            config.priority
        )
        if isinstance(config.command, list):
            create: Any = asyncio.create_subprocess_exec
            cmd = config.command
        else:
            if config.shell:
                create, cmd, shell_kwargs = shell_spawn(
                    config.shell, config.command
                )
                kwargs.update(shell_kwargs)
            else:
                create = asyncio.create_subprocess_shell
                cmd = [config.command]
        if config.environment or self.extra_env:
            env = dict(os.environ)
            fixup_pyinstaller_env(env)
            for envvar in config.environment:
                env[envvar["key"]] = envvar["value"]
            # The daemon-injected control-channel vars go last, so a job's own
            # environment cannot shadow the loopback URL/token it needs to
            # reach the state API (CRONSTABLE_* is reserved for cronstable's
            # use).
            env.update(self.extra_env)
            self.env = env
            kwargs["env"] = env
        if config.workingDirectory is not None:
            # The directory the child starts in.  Omitted rather than passed
            # as None when unset, so a job that does not ask for one keeps
            # inheriting the daemon's CWD byte for byte as it always has.
            # The value was normalized at config load; whether the directory
            # exists is the OS's call, and a bad one raises OSError into the
            # start_failed net below.  Note the child chdirs BEFORE
            # preexec_fn runs, so on a job that also demotes, the chdir uses
            # the daemon's privileges and the demoted child can land in a
            # directory it cannot itself read.
            kwargs["cwd"] = config.workingDirectory
        if config.uid is not None or config.gid is not None:
            # POSIX only: uid/gid are always None on Windows (the config layer
            # rejects user/group there), so preexec_fn is never wired up on a
            # platform that doesn't support it.
            kwargs["preexec_fn"] = self._demote
        logger.debug("%s: will execute argv %r", config.name, cmd)
        capture_stderr = config.captureStderr
        capture_stdout = config.captureStdout
        if capture_stderr:
            kwargs["stderr"] = asyncio.subprocess.PIPE
        if capture_stdout:
            kwargs["stdout"] = asyncio.subprocess.PIPE
        if config.executionTimeout:
            self.execution_deadline = (
                time.perf_counter() + config.executionTimeout
            )
        if capture_stderr or capture_stdout:
            # The pipe's flow-control watermark, NOT the line cap (the
            # reader enforces maxLineLength by hand, see
            # StreamReader._read). This only decides how much unread
            # output asyncio buffers per pipe before pausing the child;
            # pinned to the reader's chunk size so a chatty job is
            # backpressured instead of held in the daemon's memory.
            kwargs["limit"] = _READ_CHUNK

        try:
            # POSIX wants UTF-8 bytes argv (locale-independent); Windows wants
            # str (CreateProcessW rejects bytes). See platform.encode_argv.
            args = platform.encode_argv(cmd)
            # loggable_spawn_kwargs copies and summarises the whole kwargs
            # dict, so the guard keeps the launch path free of it whenever
            # DEBUG is off.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "subprocess: args=%r, kwargs=%r",
                    args,
                    loggable_spawn_kwargs(kwargs),
                )
            self.proc = await create(*args, **kwargs)
        except (
            subprocess.SubprocessError,
            # ValueError covers UnicodeEncodeError (unencodable argv) and
            # the 'embedded null byte' raised for a NUL in argv/env; any
            # unspawnable argv must be recorded as start_failed for the
            # reaper, not kill the whole scheduler.
            ValueError,
            # OSError covers FileNotFoundError (bad argv[0]) AND the
            # resource-exhaustion/permission cases (EMFILE, ENOMEM,
            # EACCES, EAGAIN). These are NOT SubprocessError subclasses;
            # without this they would propagate through the unguarded
            # spawn path and kill the whole scheduler.
            OSError,
        ):
            logger.exception(
                "Error launching subprocess of job %s, cmd=%r, kwargs=%s "
                "(system encoding: %s)",
                config.name,
                cmd,
                loggable_spawn_kwargs(kwargs),
                sys.getdefaultencoding(),
            )
            self.start_failed = True
            return

        if self.proc.pid is not None:
            # POSIX: renice the group now that it exists (on Windows the
            # class already rode in on the creation flags).  First thing
            # after the spawn, so the window in which the job runs at the
            # inherited priority is as short as it can be, and called for
            # its effect alone: a refusal is best-effort by design and
            # apply_priority logs it, so there is nothing to decide here and
            # nothing to say twice.
            platform.apply_priority(self.proc.pid, config.priority)

        # Spawned, not awaited: every launch path holds the daemon-wide
        # spawn gate around start(), and a stalled statsd send would hold
        # a gate permit for the whole stall.  _on_stop joins the task
        # (bounded) so the start/stop pair still orders on the wire.
        if self.statsd_writer:
            self._start_telemetry = asyncio.create_task(self._on_start())

        if config.monitorResources and self.proc.pid is not None:
            # Best-effort: if psutil cannot attach the monitor stays inert
            # and resource_usage ends up None. Started right after launch.
            self._resource_monitor = ResourceMonitor(
                self.proc.pid,
                interval=config.monitorResourcesInterval,
                history=config.monitorResourcesHistory,
            )
            self._resource_monitor.start()

        if capture_stderr:
            assert self.proc.stderr is not None
            self._stderr_reader = StreamReader(
                config.name,
                "stderr",
                self.proc.stderr,
                config.streamPrefix,
                config.saveLimit,
                on_line=self.output.publish,
                max_line_length=config.maxLineLength,
            )
        if capture_stdout:
            assert self.proc.stdout is not None
            self._stdout_reader = StreamReader(
                config.name,
                "stdout",
                self.proc.stdout,
                config.streamPrefix,
                config.saveLimit,
                on_line=self.output.publish,
                max_line_length=config.maxLineLength,
            )

    def live_resources(self) -> Optional[dict[str, Any]]:
        """Current live CPU/memory of this running instance, or ``None``.

        Read by the scheduler while the job is still running (the dashboard's
        live per-job readout). ``None`` when the run is not monitored, the
        monitor could not attach, or no sample has landed yet.
        """
        if self._resource_monitor is None:
            return None
        return self._resource_monitor.snapshot()

    def live_resource_series(self) -> Optional[list[list[float]]]:
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
                # The command never launched: report a normal failure
                # (exit code 127, "command not found") rather than raising
                # RuntimeError, which the reaper logs as a bug.
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
        # Pipe EOF needs EVERY write-end closed, including any a
        # descendant inherited; one that escaped the group kill would hold
        # the pipe open and strand the run in running_jobs forever (the
        # reaper parks on this await, which has no outer bound). So bound
        # the drain on a killed run only: an untouched run owns its own
        # lifetime and its output is not ours to cut short.
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
        # Close our end of the subprocess pipes now both readers are
        # joined. A no-op after a normal EOF, but a KILLED run whose
        # descendant escaped the group never reaches EOF, and its pipe
        # transport would linger until GC: a leaked read-end fd, and a
        # ProactorEventLoop "unclosed transport" finalizer error under the
        # test harness. close() is idempotent and, after the joins, can
        # lose no captured output.
        transport = getattr(self.proc, "_transport", None)
        if transport is not None:
            transport.close()

    @property
    def failed(self) -> bool:
        return self.fail_reason is not None

    @property
    def fail_reason(self) -> Optional[str]:
        fails_when = self.config.failsWhen
        if fails_when["always"]:
            return "failsWhen=always"
        if fails_when["nonzeroReturn"] and self.retcode != 0:
            return "failsWhen=nonzeroReturn and retcode={}".format(
                self.retcode
            )
        if fails_when["producesStdout"] and (
            self.stdout or self.stdout_discarded
        ):
            return "failsWhen=producesStdout and stdout is not empty"
        if fails_when["producesStderr"] and (
            self.stderr or self.stderr_discarded
        ):
            return "failsWhen=producesStderr and stderr is not empty"
        return None

    async def cancel(self) -> None:
        """Terminate this run and everything it spawned.

        Signals the job's whole process group: descendants inherit the
        stdout/stderr write-ends, and a helper outliving a killed shell
        would hold the pipe open forever (the run never leaves
        ``running_jobs``, and under ``concurrencyPolicy: Forbid`` the job
        never runs again).

        A run with no process (``proc=None``, ``start_failed``) is a
        NO-OP, not an error: several callers run outside ``run()``'s
        try/except, so a raise here would take down the whole scheduler.
        The reaper still completes such a run through ``wait()``'s
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
        # Graceful first: SIGTERM the group on POSIX, CTRL_BREAK_EVENT to the
        # group on Windows (both trappable, so the job gets killTimeout
        # seconds to flush and exit). On POSIX this reaches the descendants
        # even once the leader itself has exited, which is exactly the case
        # that wedges the run. On Windows, where the break cannot be
        # delivered (no shared console, as in a service context), this step
        # degrades to the immediate taskkill tree kill, which must run while
        # the root is still alive to anchor its walk (killing the root
        # first, as the fallback below does, would orphan every descendant
        # for good). The fallback to the direct child remains for a
        # group/tree that could not be signalled at all.
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
            # On Python <=3.11 wait_for can spuriously time out even
            # though proc.wait() completed, leaving the returncode already
            # set; kill() would then raise ProcessLookupError, so re-check
            # and guard it like terminate().
            if self.proc.returncode is None:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
        await self._on_stop()

    # The three completion hooks probe report_config_enabled first (the
    # guard the DAG-task reaper also uses, see Cron._maybe_report_dag_task)
    # so the no-reporter default costs dict probes, not task spawns. The
    # INFO line sits behind the probe: it is only true when a reporter
    # will actually fire. The probe stays inlined in each hook:
    # deduplicating the three through a shared awaited helper puts a
    # coroutine hop in front of the probe on every completion, and that
    # hop alone costs 42% on job.report_noop_100k.

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
        await _fan_out_reports(
            self,
            success,
            report_config,
            "Problem reporting job %s failure: %s",
            self.config.name,
        )

    @property
    def template_vars(self) -> dict:
        # The reporting contract in full (STANDARD_TEMPLATE_VARS);
        # SlaBreachContext and NotifyEventContext below merge the same
        # base, with the run-shaped values empty.
        return _base_template_vars(
            self,
            success=self.fail_reason is None,
            schedule=schedule_string(self.config),
            # run context so a payload can identify the run: started_at is
            # ISO-8601 (None on a failed launch); run_id is None without a
            # durable state store.
            started_at=(
                self.started_at.isoformat()
                if self.started_at is not None
                else None
            ),
            run_id=self.run_id,
            usage=self.resource_usage,
        )


class SlaBreachContext:
    """Reporting context for one SLA breach: a job that did NOT (yet) run.

    Quacks like a :class:`RunningJob` exactly as far as the reporters read
    one, every run-shaped field explicitly empty. Deliberately NOT a bare
    ``RunningJob``: with no process, ``failsWhen.nonzeroReturn`` would
    synthesize a bogus fail_reason. ``template_vars`` carries the full
    standard key set plus the breach detail, so onFailure templates
    render unchanged on onLate; ``env`` carries HOSTNAME so the default
    sentry fingerprint keeps its host dimension.
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
        self.retcode: int | None = None
        self.stdout: str | None = None
        self.stderr: str | None = None
        self.stdout_discarded = 0
        self.stderr_discarded = 0
        self.resource_usage: ResourceUsage | None = None
        self.env = {"HOSTNAME": report_hostname()}
        # read by ShellReporter for the CRONSTABLE_SLA_* exports.
        self.sla_vars = {
            "sla_check": check,
            "threshold_seconds": threshold_seconds,
            "observed_seconds": observed_seconds,
            "last_success_at": last_success_at,
        }

    @property
    def template_vars(self) -> dict:
        # STANDARD_TEMPLATE_VARS with the run-shaped keys emptied (a breach
        # describes a job that did NOT run; host and schedule still describe
        # it), plus the four breach-detail keys sla_vars already carries
        # under the same names.
        return {
            **_base_template_vars(
                self, success=False, schedule=schedule_string(self.config)
            ),
            **self.sla_vars,
        }


async def report_sla_breach(
    ctx: SlaBreachContext, report_config: dict
) -> None:
    """Fan one SLA breach out to every reporter (the onLate hook).

    ``success=False`` throughout, so MailReporter's empty-body
    suppression can never eat the alert and Sentry defaults to level
    "error".
    """
    logger.info(
        "Cron job %s: reporting SLA breach (%s)",
        ctx.config.name,
        ctx.sla_check,
    )
    await _fan_out_reports(
        ctx,
        False,
        report_config,
        "Problem reporting job %s SLA breach: %s",
        ctx.config.name,
    )


class _HeartbeatJobShim:
    """The ``JobConfig`` slice the reporters read for a heartbeat.

    A heartbeat has no command, shell or schedule string to launch, but
    the reporters reach into ``job.config`` for all three (the shell
    reporter encodes them into its child's environment, where a ``None``
    dies in ``os.fsencode``).  ``schedule_unparsed`` carries the
    heartbeat's own expectation instead -- the cron expression it is
    watched against, or ``every <n>s`` for an interval -- so an alert
    still says what was expected of it.
    """

    __slots__ = ("name", "command", "shell", "schedule_unparsed")

    def __init__(self, name: str, expectation: str) -> None:
        self.name = name
        self.command = ""
        self.shell = platform.DEFAULT_SHELL
        self.schedule_unparsed = expectation


class HeartbeatContext:
    """Reporting context for one inbound heartbeat transition.

    The third sibling of :class:`SlaBreachContext` and
    :class:`NotifyEventContext`, and quacks like a :class:`RunningJob`
    exactly as far as the reporters read one: no process ever ran here
    either, so every run-shaped field is explicitly empty.  What it adds
    is the heartbeat detail the templates in
    :mod:`cronstable.config` render -- ``state``, ``reason``,
    ``last_ping_at``, ``overdue_seconds`` and the ping's own words.

    ``success`` is threaded from the transition rather than derived: a
    recovery is a success (and must render the recovery templates), a
    down is not, and MailReporter's empty-body suppression keys on it.
    """

    def __init__(
        self,
        *,
        name: str,
        expectation: str,
        success: bool,
        state: str,
        reason: Optional[str] = None,
        description: Optional[str] = None,
        last_ping_at: Optional[str] = None,
        expected_at: Optional[str] = None,
        overdue_seconds: Optional[float] = None,
        down_since: Optional[str] = None,
        down_seconds: Optional[float] = None,
        exit_code: Optional[int] = None,
        run_id: Optional[str] = None,
        ping_body: Optional[str] = None,
    ) -> None:
        self.config = _HeartbeatJobShim(name, expectation)
        self.heartbeat = name
        self._success = success
        self.fail_reason = (
            None if success else "heartbeat: {}".format(reason or state)
        )
        self.failed = not success
        self.retcode: int | None = None
        self.stdout: str | None = None
        self.stderr: str | None = None
        self.stdout_discarded = 0
        self.stderr_discarded = 0
        self.resource_usage: ResourceUsage | None = None
        self.env = {"HOSTNAME": report_hostname()}
        self.run_id = run_id
        # Read by ShellReporter for its CRONSTABLE_HEARTBEAT_* exports and
        # merged into template_vars below, so the two can never drift.
        self.heartbeat_vars: dict[str, Any] = {
            "heartbeat": name,
            "state": state,
            "reason": reason,
            "description": description,
            "last_ping_at": last_ping_at,
            "expected_at": expected_at,
            "overdue_seconds": (
                None if overdue_seconds is None else round(overdue_seconds)
            ),
            "down_since": down_since,
            "down_seconds": (
                None if down_seconds is None else round(down_seconds)
            ),
            "exit_code": exit_code,
            "ping_body": ping_body,
        }

    @property
    def template_vars(self) -> dict:
        # STANDARD_TEMPLATE_VARS with the run-shaped keys emptied, plus
        # the heartbeat detail.  `schedule` comes from the shim, so
        # {{schedule}} renders the expectation the heartbeat is judged
        # against exactly as it renders a job's cron line.
        return {
            **_base_template_vars(
                self,
                success=self._success,
                schedule=self.config.schedule_unparsed,
                run_id=self.run_id,
            ),
            **self.heartbeat_vars,
        }


async def report_heartbeat(ctx: HeartbeatContext, report_config: dict) -> None:
    """Fan one heartbeat transition out to every reporter.

    Used by all three hooks (``onLate``, ``onFailure``, ``onRecovery``);
    which one fired is already baked into ``report_config`` and into the
    context's ``success``, so there is nothing left here to branch on.
    """
    logger.info(
        "Heartbeat %s: reporting %s",
        ctx.heartbeat,
        ctx.heartbeat_vars["state"],
    )
    await _fan_out_reports(
        ctx,
        ctx._success,
        report_config,
        "Problem reporting heartbeat %s: %s",
        ctx.heartbeat,
    )


class _NotifyJobShim:
    """The tiny ``JobConfig`` slice the reporters read for a daemon event.

    Supplies exactly the fields the reporters reach into ``job.config``
    for, the non-name launch fields empty so the shell reporter's env
    encodes to strings, not ``None`` (which dies in ``os.fsencode``).
    """

    __slots__ = ("name", "command", "shell", "schedule_unparsed")

    def __init__(self, name: str) -> None:
        self.name = name
        self.command = ""
        self.shell = platform.DEFAULT_SHELL
        self.schedule_unparsed = ""


class NotifyEventContext:
    """Reporting context for a daemon/orchestration event (the ``notify:``
    block): a DAG run failure, an approval gate, a leadership change.

    Quacks like a :class:`RunningJob` exactly as far as the reporters read
    one (a :class:`_NotifyJobShim` ``config``, run-shaped fields empty,
    the standard ``template_vars`` key set), plus the event detail:
    ``event``, ``subject``, ``message`` and any event-specific ``fields``.
    """

    def __init__(
        self,
        *,
        event: str,
        success: bool,
        name: str,
        subject: str,
        message: str,
        fields: Optional[dict[str, Any]] = None,
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
        self.retcode: int | None = None
        self.stdout: str | None = None
        self.stderr: str | None = None
        self.stdout_discarded = 0
        self.stderr_discarded = 0
        self.resource_usage: ResourceUsage | None = None
        self.env = {"HOSTNAME": report_hostname()}

    @property
    def template_vars(self) -> dict:
        # STANDARD_TEMPLATE_VARS with the run-shaped keys emptied, plus the
        # event detail (and whatever `fields` the event carries).
        base = _base_template_vars(self, success=self._success, schedule="")
        # the event detail the notify templates render.
        base["event"] = self.event
        base["subject"] = self._subject
        base["message"] = self._message
        # event-specific extras (dag, run_key, taskkey, role, leader, ...);
        # last so an event can override a standard key if it must.
        base.update(self._fields)
        return base


async def report_event(ctx: NotifyEventContext, report_config: dict) -> None:
    """Fan one daemon/orchestration event out to every reporter.

    ``success`` is threaded from the event (an alert-worthy event passes
    ``success=False`` so MailReporter's empty-body suppression cannot eat
    it); a notification failure never propagates to the loop that raised
    the event.
    """
    logger.info("Reporting %s event: %s", ctx.event, ctx._subject)
    await _fan_out_reports(
        ctx,
        not ctx.failed,
        report_config,
        "Problem reporting %s event: %s",
        ctx.event,
    )
