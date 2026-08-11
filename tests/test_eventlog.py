"""The Windows Event Log reporter: contract, routing, safety and lifetime.

The event ids and the insertion-string positions are a PUBLIC contract (an
Event Viewer custom view, a Windows Event Forwarding subscription and a SIEM
rule all key on them), so the two tables are pinned verbatim here.  The rest
of the file covers the three things that are easy to get wrong and quiet
when you do: which hook a report belongs to, that a report never waits on
the OS, and that a writer thread and its source handle are retired when the
config stops naming them.

Everything runs on every OS.  The reporter's Windows arm is plain Python
driven by monkeypatching ``platform.IS_WINDOWS``, and the three ctypes leaf
calls are faked, so nothing here writes to a real Event Log even when the
suite runs on Windows.  ``tests/test_platform.py`` owns the real calls.
"""

import asyncio
import time
import types

import pytest

import cronstable.job
from cronstable import platform
from cronstable.config import parse_config_string
from cronstable.job import (
    EVENTLOG_EVENTS,
    EVENTLOG_MAX_FIELD_CHARS,
    EVENTLOG_MAX_OUTPUT_CHARS,
    EVENTLOG_STRING_FIELDS,
    EventLogReporter,
    _eventlog_outcome,
    close_event_log_writers,
    eventlog_event_strings,
    retire_event_log_writers,
)


class _FakeLog:
    """Stand-in for the three cronstable.platform Event Log leaf calls."""

    def __init__(self):
        self.opened = []
        self.closed = []
        self.written = []
        self.handles = iter(range(100, 200))
        self.open_returns_none = False
        self.codes = []
        self.write_delay = 0.0

    def open(self, source):
        self.opened.append(source)
        if self.open_returns_none:
            return None
        return next(self.handles)

    def write(self, handle, *, event_type, category, event_id, strings):
        if self.write_delay:
            time.sleep(self.write_delay)
        self.written.append(
            {
                "handle": handle,
                "event_type": event_type,
                "category": category,
                "event_id": event_id,
                "strings": strings,
            }
        )
        return self.codes.pop(0) if self.codes else 0

    def close(self, handle):
        self.closed.append(handle)


@pytest.fixture(autouse=True)
def fake_event_log(monkeypatch):
    """Fake the leaf calls and guarantee no writer outlives a test.

    _EVENTLOG_WRITERS is a process global keyed only on the source name, and
    every entry owns a live daemon thread, so without the teardown one
    test's writer would still be running during the next one.
    """
    fake = _FakeLog()
    monkeypatch.setattr(platform, "open_event_log", fake.open)
    monkeypatch.setattr(platform, "write_event_log", fake.write)
    monkeypatch.setattr(platform, "close_event_log", fake.close)
    monkeypatch.setattr(platform, "IS_WINDOWS", True)
    monkeypatch.setattr(cronstable.job, "_EVENTLOG_CAP_LOGGED", False)
    yield fake
    retire_event_log_writers(set())
    for writer in list(cronstable.job._EVENTLOG_WRITERS.values()):
        writer.join(2.0)
    cronstable.job._EVENTLOG_WRITERS.clear()


_JOB = """
jobs:
  - name: backup
    command: do-backup
    schedule: "* * * * *"
    onFailure:
      report:
        eventlog:
          enabled: true
    onPermanentFailure:
      report:
        eventlog:
          enabled: true
    onSuccess:
      report:
        eventlog:
          enabled: true
"""


def _job_config(yaml=_JOB):
    return parse_config_string(yaml, "").jobs[0]


def _ctx(job_config, **extra):
    """A reporting context shaped like the slice a reporter reads.

    SimpleNamespace, never Mock: a Mock invents every attribute it is
    asked for, so ``getattr(ctx, "event", None)`` would return a truthy
    Mock and route every report to the notify branch.
    """
    tvars = {
        "name": job_config.name,
        "success": True,
        "fail_reason": None,
        "stdout": None,
        "stderr": None,
        "exit_code": 0,
        "command": job_config.command,
        "shell": job_config.shell,
        "host": "WS-01",
        "schedule": "* * * * *",
        "started_at": "2026-08-10T03:00:00+00:00",
        "run_id": "r-1",
        "cpu_seconds": None,
        "max_rss_bytes": None,
    }
    tvars.update(extra.pop("template_vars", {}))
    return types.SimpleNamespace(
        config=job_config, template_vars=tvars, **extra
    )


async def _drain():
    await close_event_log_writers()


# ---------------------------------------------------------------------------
# The published contract
# ---------------------------------------------------------------------------


def test_eventlog_event_ids_are_the_documented_table():
    # A shipped id keeps its meaning forever: a SIEM rule keys on it. This
    # is the whole table, verbatim, so changing one is a deliberate act.
    assert EVENTLOG_EVENTS == {
        "success": (1000, platform.EVENTLOG_INFORMATION_TYPE, 1),
        "failure": (1001, platform.EVENTLOG_ERROR_TYPE, 1),
        "permanent-failure": (1002, platform.EVENTLOG_ERROR_TYPE, 1),
        "late": (1003, platform.EVENTLOG_WARNING_TYPE, 1),
        "event": (1010, platform.EVENTLOG_INFORMATION_TYPE, 2),
        "event-alert": (1011, platform.EVENTLOG_ERROR_TYPE, 2),
    }


def test_eventlog_string_fields_arity_and_order():
    # position is the contract: an unregistered source has no message table
    # to name fields, and a forwarder ships them as <Data> in this order.
    assert EVENTLOG_STRING_FIELDS == (
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


def test_eventlog_field_caps_fit_the_api_ceiling():
    # ReportEventW writes NO record when the combined vector is too long, so
    # the caps are sized by arithmetic rather than trimmed at write time.
    largest = (
        len(EVENTLOG_STRING_FIELDS) - 1
    ) * EVENTLOG_MAX_FIELD_CHARS + EVENTLOG_MAX_OUTPUT_CHARS
    assert largest < platform.EVENTLOG_MAX_TOTAL_CHARS


# ---------------------------------------------------------------------------
# Which hook a report belongs to
# ---------------------------------------------------------------------------


def test_eventlog_hook_report_blocks_never_alias():
    # _eventlog_outcome tells onFailure from onPermanentFailure by the
    # IDENTITY of the report dict, which is only sound while each hook's
    # block is an independent object. That is an implementation detail of
    # config.py (four separate deepcopies), so it is pinned here.
    for job_config in (_job_config(), _job_config(_BARE_JOB)):
        blocks = [
            id(job_config.onFailure["report"]),
            id(job_config.onPermanentFailure["report"]),
            id(job_config.onSuccess["report"]),
            id(job_config.onLate["report"]),
        ]
        assert len(set(blocks)) == 4


_BARE_JOB = """
jobs:
  - name: bare
    command: x
    schedule: "* * * * *"
"""


def test_eventlog_outcome_maps_every_hook():
    job_config = _job_config()
    ctx = _ctx(job_config)
    assert (
        _eventlog_outcome(ctx, job_config.onSuccess["report"], True)
        == "success"
    )
    assert (
        _eventlog_outcome(ctx, job_config.onFailure["report"], False)
        == "failure"
    )
    assert (
        _eventlog_outcome(
            ctx, job_config.onPermanentFailure["report"], False
        )
        == "permanent-failure"
    )
    late = _ctx(job_config, sla_vars={"sla_check": "lateAfter"})
    assert _eventlog_outcome(late, job_config.onLate["report"], False) == (
        "late"
    )
    notify = _ctx(job_config, event="dag_failure")
    assert _eventlog_outcome(notify, {}, True) == "event"
    assert _eventlog_outcome(notify, {}, False) == "event-alert"


def test_eventlog_outcome_survives_a_context_without_the_hook():
    # the notify context's job shim carries __slots__ and has no
    # onPermanentFailure; a raise here would be swallowed by the fan-out's
    # return_exceptions gather and surface only as a log line.
    shim = types.SimpleNamespace(config=object(), template_vars={})
    assert _eventlog_outcome(shim, {}, False) == "failure"


# ---------------------------------------------------------------------------
# The reporter's two early returns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eventlog_reporter_is_a_noop_when_disabled(fake_event_log):
    job_config = _job_config(_BARE_JOB)
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    await _drain()
    assert fake_event_log.written == []
    assert fake_event_log.opened == []


@pytest.mark.asyncio
async def test_eventlog_reporter_is_a_noop_on_posix(
    fake_event_log, monkeypatch
):
    monkeypatch.setattr(platform, "IS_WINDOWS", False)
    job_config = _job_config()
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    await _drain()
    assert fake_event_log.written == []
    assert fake_event_log.opened == []


# ---------------------------------------------------------------------------
# What it writes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eventlog_reporter_writes_the_success_event(fake_event_log):
    job_config = _job_config()
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    await _drain()
    assert len(fake_event_log.written) == 1
    record = fake_event_log.written[0]
    assert record["event_id"] == 1000
    assert record["event_type"] == platform.EVENTLOG_INFORMATION_TYPE
    assert record["category"] == 1
    assert len(record["strings"]) == len(EVENTLOG_STRING_FIELDS)
    assert all(isinstance(s, str) for s in record["strings"])
    assert record["strings"][1] == "backup"
    assert record["strings"][2] == "success"
    assert record["strings"][3] == "WS-01"
    assert fake_event_log.opened == ["cronstable"]


@pytest.mark.asyncio
async def test_eventlog_reporter_separates_failure_from_permanent_failure(
    fake_event_log,
):
    # red against any implementation that folds the two hooks together.
    job_config = _job_config()
    ctx = _ctx(job_config)
    reporter = EventLogReporter()
    await reporter.report(False, ctx, job_config.onFailure["report"])
    await reporter.report(
        False, ctx, job_config.onPermanentFailure["report"]
    )
    await _drain()
    assert [r["event_id"] for r in fake_event_log.written] == [1001, 1002]


@pytest.mark.asyncio
async def test_eventlog_reporter_reports_an_sla_breach_as_late(
    fake_event_log,
):
    job_config = _job_config(_SLA_JOB)
    ctx = _ctx(
        job_config,
        sla_vars={"sla_check": "lateAfter"},
        template_vars={
            "sla_check": "lateAfter",
            "threshold_seconds": 120,
            "observed_seconds": 300.5,
            "last_success_at": "2026-08-10T02:00:00+00:00",
        },
    )
    await EventLogReporter().report(
        False, ctx, job_config.onLate["report"]
    )
    await _drain()
    record = fake_event_log.written[0]
    assert record["event_id"] == 1003
    assert record["event_type"] == platform.EVENTLOG_WARNING_TYPE
    detail = record["strings"][EVENTLOG_STRING_FIELDS.index("detail")]
    assert "check=lateAfter" in detail
    assert "threshold=120s" in detail
    assert "observed=300.5s" in detail


_SLA_JOB = """
jobs:
  - name: backup
    command: do-backup
    schedule: "* * * * *"
    sla:
      lateAfterSeconds: 120
    onLate:
      report:
        eventlog:
          enabled: true
"""


@pytest.mark.asyncio
async def test_eventlog_reporter_reports_a_notify_event_by_severity(
    fake_event_log,
):
    job_config = _job_config()
    report = job_config.onSuccess["report"]
    reporter = EventLogReporter()
    ok = _ctx(job_config, event="leader_acquired")
    bad = _ctx(job_config, event="dag_failure")
    await reporter.report(True, ok, report)
    await reporter.report(False, bad, report)
    await _drain()
    ids = [(r["event_id"], r["event_type"]) for r in fake_event_log.written]
    assert ids == [
        (1010, platform.EVENTLOG_INFORMATION_TYPE),
        (1011, platform.EVENTLOG_ERROR_TYPE),
    ]


# ---------------------------------------------------------------------------
# String safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "outcome", ["success", "failure", "permanent-failure", "late", "event"]
)
def test_eventlog_strings_are_never_none_and_always_eleven(outcome):
    job_config = _job_config()
    ctx = _ctx(
        job_config,
        template_vars={
            "exit_code": None,
            "fail_reason": None,
            "run_id": None,
            "started_at": None,
        },
    )
    strings = eventlog_event_strings(ctx, outcome, include_output=False)
    assert len(strings) == len(EVENTLOG_STRING_FIELDS)
    assert all(isinstance(s, str) for s in strings)


def test_eventlog_output_is_omitted_unless_opted_in():
    job_config = _job_config()
    ctx = _ctx(job_config, template_vars={"stderr": "boom" * 100})
    without = eventlog_event_strings(ctx, "failure", include_output=False)
    assert without[-1] == ""
    with_output = eventlog_event_strings(ctx, "failure", include_output=True)
    assert with_output[-1].startswith("boom")


def test_eventlog_fields_are_capped():
    job_config = _job_config()
    ctx = _ctx(
        job_config,
        template_vars={
            "fail_reason": "x" * 5000,
            "stderr": "y" * 50000,
        },
    )
    strings = eventlog_event_strings(ctx, "failure", include_output=True)
    reason = strings[EVENTLOG_STRING_FIELDS.index("failReason")]
    assert len(reason) == EVENTLOG_MAX_FIELD_CHARS
    assert reason.endswith("...[truncated]")
    assert len(strings[-1]) == EVENTLOG_MAX_OUTPUT_CHARS
    assert strings[-1].endswith("...[truncated]")


def test_eventlog_strings_drop_nul_and_lone_surrogates():
    # a NUL truncates the field inside the API, and a lone surrogate (which
    # reaches template_vars from os.environ via surrogateescape) would raise
    # inside the ctypes conversion, on the writer thread where the fan-out
    # cannot see it.
    job_config = _job_config()
    ctx = _ctx(
        job_config, template_vars={"fail_reason": "a\x00b\udcffc"}
    )
    strings = eventlog_event_strings(ctx, "failure", include_output=False)
    reason = strings[EVENTLOG_STRING_FIELDS.index("failReason")]
    assert "\x00" not in reason
    reason.encode("utf-16-le")  # would raise on a surviving surrogate


# ---------------------------------------------------------------------------
# Lifetime: the loop, the queue, the registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eventlog_report_never_awaits_the_writer(fake_event_log):
    # report() runs INLINE on the reaper, so a slow EventLog service must
    # not delay a job's completion handling.
    fake_event_log.write_delay = 1.0
    job_config = _job_config()
    started = time.monotonic()
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    assert time.monotonic() - started < 0.5
    await _drain()
    assert len(fake_event_log.written) == 1


@pytest.mark.asyncio
async def test_eventlog_writer_reopens_after_an_invalid_handle(
    fake_event_log,
):
    # the EventLog service restarting invalidates the source handle;
    # re-registering and retrying once is the whole repair.
    fake_event_log.codes = [platform.EVENTLOG_ERROR_INVALID_HANDLE]
    job_config = _job_config()
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    await _drain()
    assert len(fake_event_log.written) == 2
    assert fake_event_log.written[0]["handle"] != (
        fake_event_log.written[1]["handle"]
    )
    assert fake_event_log.opened == ["cronstable", "cronstable"]
    assert len(fake_event_log.closed) >= 1


@pytest.mark.asyncio
async def test_eventlog_writer_continues_when_the_source_will_not_open(
    fake_event_log, caplog
):
    fake_event_log.open_returns_none = True
    job_config = _job_config()
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    await _drain()
    assert fake_event_log.written == []
    assert "could not open the event source" in caplog.text


@pytest.mark.asyncio
async def test_eventlog_writer_drops_and_logs_when_the_queue_is_full(
    fake_event_log, caplog
):
    writer = cronstable.job._EventLogWriter("full-probe")
    try:
        # block the thread so the queue can actually fill
        writer._queue.put((platform.EVENTLOG_INFORMATION_TYPE, 1, 1000, []))
        accepted = 0
        for _ in range(cronstable.job.EVENTLOG_QUEUE_LIMIT + 50):
            if writer.submit(
                (platform.EVENTLOG_INFORMATION_TYPE, 1, 1000, [])
            ):
                accepted += 1
        assert accepted < cronstable.job.EVENTLOG_QUEUE_LIMIT + 50
        assert "is not keeping up" in caplog.text
    finally:
        writer.stop()
        writer.join(2.0)


@pytest.mark.asyncio
async def test_retire_event_log_writers_drops_a_renamed_source(
    fake_event_log,
):
    job_config = _job_config(_SOURCE_JOB)
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    assert "alpha" in cronstable.job._EVENTLOG_WRITERS
    retire_event_log_writers({"beta"})
    assert "alpha" not in cronstable.job._EVENTLOG_WRITERS


@pytest.mark.asyncio
async def test_retire_event_log_writers_releases_the_source_handle(
    fake_event_log,
):
    # Dropping the registry entry is only half the retirement.  The reload
    # path pops a writer and deliberately does NOT join it, so a release
    # that lived in join() never ran on this path: every reload that
    # renamed `source` leaked one source handle, unbounded, and invisible
    # to EVENTLOG_MAX_WRITERS, which caps the LIVE registry rather than
    # what has already left it.  The writer's own thread releases it now,
    # so the reload never has to wait for it.
    job_config = _job_config(_SOURCE_JOB)
    await EventLogReporter().report(
        True, _ctx(job_config), job_config.onSuccess["report"]
    )
    writer = cronstable.job._EVENTLOG_WRITERS["alpha"]
    # the handle opens lazily on the writer thread, so wait for the record
    deadline = time.monotonic() + 5.0
    while not fake_event_log.written and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert fake_event_log.written
    handle = fake_event_log.written[0]["handle"]
    assert fake_event_log.closed == []

    retire_event_log_writers({"beta"})

    # the raw thread, NOT writer.join(): joining is exactly what the reload
    # path does not do, so a test that called it would pass either way.
    writer._thread.join(5.0)
    assert not writer._thread.is_alive()
    assert fake_event_log.closed == [handle]


_SOURCE_JOB = """
jobs:
  - name: backup
    command: do-backup
    schedule: "* * * * *"
    onSuccess:
      report:
        eventlog:
          enabled: true
          source: alpha
"""


def test_eventlog_writers_are_capped(caplog):
    made = []
    for index in range(cronstable.job.EVENTLOG_MAX_WRITERS + 3):
        writer = cronstable.job._eventlog_writer("cap-{}".format(index))
        if writer is not None:
            made.append(writer)
    assert len(made) == cronstable.job.EVENTLOG_MAX_WRITERS
    assert "refusing to open more than" in caplog.text


@pytest.mark.asyncio
async def test_close_event_log_writers_without_a_writer():
    # idempotent, and safe when nothing was ever created
    await close_event_log_writers()
    await close_event_log_writers()


def test_reporter_list_has_the_six_reporters():
    assert [type(r).__name__ for r in cronstable.job.RunningJob.REPORTERS] == [
        "SentryReporter",
        "MailReporter",
        "ShellReporter",
        "WebhookReporter",
        "PushReporter",
        "EventLogReporter",
    ]


@pytest.mark.asyncio
async def test_eventlog_writer_thread_is_not_joined_at_interpreter_exit():
    # the property, not the flag: a wedged ReportEventW must not be able to
    # hold interpreter exit open, which is what a ThreadPoolExecutor's own
    # atexit join would have done.
    writer = cronstable.job._EventLogWriter("exit-probe")
    try:
        assert writer._thread.daemon is True
        assert not any(
            t is writer._thread
            for t in getattr(asyncio, "_nonexistent", []) or []
        )
    finally:
        writer.stop()
        writer.join(2.0)
