r"""Convert Windows Task Scheduler XML exports into cronstable jobs.

A Windows estate's schedules already exist, as Task Scheduler tasks, and
Task Scheduler exports its whole task set as a documented, stable XML schema
(``schtasks /query /XML``, ``Export-ScheduledTask``, namespace
``http://schemas.microsoft.com/windows/2004/02/mit/task``).  Without this,
migrating a few hundred tasks means retyping a few hundred tasks.

This module is a ONE-SHOT CONVERTER, not a config-directory loader, and that
is the one place it departs from what the roadmap entry asked for.  Four
reasons, in the order that decides it:

* exporting a task does not unregister it, so an export describes tasks Task
  Scheduler is still firing.  A loader would silently double-run an estate
  on first start.  ``crontabs.py`` has no equivalent hazard, because a
  crontab file is handed over deliberately and cron may not even be running;
* ``.xml`` is a name half the tooling on a Windows box writes, while
  ``.crontab`` and ``.cron`` are names nothing else uses.  Teaching the
  config directory to read ``.xml`` would also let one stray file decide
  which directory becomes the Windows default config location;
* measured on a real Windows 11 estate, most of a whole-machine export is
  not convertible at all (111 of 195 tasks act through a COM handler and 57
  have no trigger).  A loader would have to either fail the whole directory
  or drop those silently, and dropping silently is what the roadmap entry
  explicitly forbids;
* a converter can be reviewed.  Its output is YAML an operator reads, edits
  and commits, which is where the judgement calls below belong.

So the contract is one-directional and narrow: the SCHEDULE is converted,
and the surrounding Task Scheduler semantics are reported rather than
emulated.  Everything that could not be carried across is listed, with a
reason and, where one exists, a remedy.

Two things about exports trip people up before any of this matters, so they
are handled here rather than left to the operator:

* ``schtasks /query /XML`` without ``ONE`` emits one XML declaration per
  task INSIDE a single ``<Tasks>`` root, which is not well-formed;
  :func:`strip_xml_declarations` removes them.
* the declaration lies about the encoding whenever the export passed
  through a redirect.  ``Export-ScheduledTask`` returns a string stamped
  ``encoding="UTF-16"`` that PowerShell then writes as UTF-8, and expat
  honours that declaration only for byte input.  :func:`decode_task_xml`
  therefore decides the encoding from the bytes and hands the parser text.
"""

from __future__ import annotations

import datetime
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from typing import Any, NamedTuple, Optional

#: The schema every ``<Task>`` element lives in.
TASK_NS = "http://schemas.microsoft.com/windows/2004/02/mit/task"
_NS = "{" + TASK_NS + "}"

#: Largest export accepted, enforced on the READ rather than checked after
#: it.  ``-`` reads standard input, and an unbounded read of a hostile
#: stream is already resident by the time any length check could look at
#: it.  A whole-machine export measured on a real Windows 11 box is 314 KB,
#: so this is three orders of magnitude of headroom and exists only to stop
#: a multi-gigabyte file, which no entity rule addresses.
MAX_DOCUMENT_BYTES = 32 * 1024 * 1024

#: The year the cron engine stops searching at.  A one-shot beyond it would
#: be refused at config load, so it is refused here instead, where the
#: message can name the task.
_YEAR_HORIZON = 2099


class TaskXmlError(ValueError):
    """An export could not be read (the message names the file).

    Deliberately not ``config.ConfigError``, for the reason
    :class:`cronstable.crontabs.CrontabError` is not either: importing the
    config module here would be circular.  Unlike CrontabError, nothing
    re-raises this as a ConfigError, because this module never runs inside
    a config parse.  That difference is the converter-not-a-loader decision
    showing up in the type.
    """


class Note(NamedTuple):
    """One thing the conversion could not carry, or carried with a caveat.

    Every field is operator-facing prose.  ``blocking`` decides whether the
    owning task's YAML is emitted live or commented out: a blocking note
    means what would be emitted does not faithfully do what the task did.
    """

    task: str
    element: str
    reason: str
    remedy: str
    blocking: bool


class ConvertedTask(NamedTuple):
    """One ``<Task>`` lowered.

    ``jobs`` holds plain dicts shaped exactly like one ``jobs:`` entry, the
    same contract :func:`cronstable.crontabs.parse_crontab` documents, so
    the emitter and the tests can treat them as ordinary configuration.

    ``commented`` is separate from ``jobs`` being empty.  A task with two
    actions produces two real jobs that must still be emitted commented
    out, because Task Scheduler runs a task's actions in sequence inside one
    instance while two cronstable jobs on one schedule run at once.
    """

    label: str
    jobs: list[dict[str, Any]]
    notes: list[Note]
    commented: bool


# --- Reading and hardening -------------------------------------------------
def read_source(path: str) -> tuple[bytes, str]:
    """Read one export, bounded.

    Returns its bytes plus the label used in messages.
    """
    if path == "-":
        data = sys.stdin.buffer.read(MAX_DOCUMENT_BYTES + 1)
        label = "<stdin>"
    else:
        label = path
        try:
            with open(path, "rb") as handle:
                data = handle.read(MAX_DOCUMENT_BYTES + 1)
        except OSError as ex:
            raise TaskXmlError("{}: {}".format(path, ex)) from ex
    if len(data) > MAX_DOCUMENT_BYTES:
        raise TaskXmlError(
            "{}: larger than {} bytes, which is far past any real Task "
            "Scheduler export; refusing to read it".format(
                label, MAX_DOCUMENT_BYTES
            )
        )
    return data, label


def decode_task_xml(data: bytes, source: str) -> str:
    """Decode an export to text, deciding the encoding from the BYTES.

    Never hand these bytes to the parser directly.  expat honours an XML
    declaration only for byte input, and the declaration is routinely wrong:
    ``Export-ScheduledTask`` returns a string stamped ``UTF-16`` which
    PowerShell then writes as UTF-8, so parsing those bytes fails with
    "encoding specified in XML declaration is incorrect" while parsing the
    same content as text succeeds.  Deciding here, from a BOM or by trial,
    and passing ``str`` onward makes the declaration irrelevant.
    """
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        encoding = "utf-16"
    elif data[:3] == b"\xef\xbb\xbf":
        encoding = "utf-8-sig"
    else:
        encoding = ""
    if encoding:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError as ex:
            raise TaskXmlError(
                "{}: begins with a {} byte-order mark but does not decode "
                "as {}".format(source, encoding, encoding)
            ) from ex
    for candidate in ("utf-8", "utf-16-le"):
        try:
            text = data.decode(candidate)
        except UnicodeDecodeError:
            continue
        # A UTF-16 trial decode succeeds on almost any even-length byte
        # string, so "it decoded" is not evidence that it was that
        # encoding. The result has to look like a document, or a
        # mis-decode reaches the parser as mojibake and is then reported
        # as malformed XML rather than as an unreadable encoding.
        if "<" in text:
            return text
    raise TaskXmlError(
        "{}: could not be decoded as UTF-8 or UTF-16. Re-export it with "
        "`schtasks /query /TN <task> /XML ONE > task.xml`.".format(source)
    )


def strip_xml_declarations(text: str) -> str:
    """Remove every ``<?xml ... ?>`` run from ``text``.

    Required to read the very command the migration path names.  Measured,
    ``schtasks /query /XML`` without ``ONE`` puts one declaration per task
    inside a single ``<Tasks>`` root, and a parser stops at the second one
    with "XML or text declaration not at start of entity".

    Sound rather than a hack: ``<`` cannot appear literally in character
    data, so any ``<?xml`` in the text really is a processing instruction.
    """
    return re.sub(r"<\?xml[^>]*\?>", "", text)


class _NoDoctype(ET.TreeBuilder):
    """A build target that refuses any DOCTYPE outright.

    This is the whole XML hardening, and it is deliberately not the usual
    list of incantations.  What is actually true, measured on this
    interpreter rather than assumed:

    * external entities need no disabling.  ElementTree installs no expat
      external-entity handler at all, so an external entity is simply
      undefined and the parse fails;
    * entity expansion (the billion-laughs class) IS the real risk, and
      expat's own amplification cap is not ours to rely on: it belongs to
      expat, not CPython, and a Linux build links the system expat, so
      nothing lets us assert it across the supported interpreters;
    * so the mitigation is refusing the DOCTYPE.  Every custom entity has
      to be declared in the internal subset, and expat reports the start of
      the doctype before parsing that subset, so raising here means no
      entity is ever declared.  That is defusedxml's ``forbid_dtd`` without
      the dependency.

    Do not reach for defusedxml's spelling of it: ``ET.XMLParser`` has no
    ``.parser`` attribute on current CPython, so setting a handler through
    it is an AttributeError at run time.  The documented hook is a
    ``TreeBuilder`` target with a ``doctype`` method, which is this.
    """

    def doctype(self, name: str, pubid: str, system: str) -> None:
        raise TaskXmlError(
            "the document declares a DOCTYPE. Task Scheduler never writes "
            "one, and cronstable refuses it because a document type can "
            "declare entities that expand without bound. Remove the "
            "DOCTYPE, or re-export the task."
        )


def parse_task_documents(text: str, source: str) -> list[ET.Element]:
    """Every ``<Task>`` element in one export, whatever shape produced it."""
    parser = ET.XMLParser(target=_NoDoctype())  # nosec B314
    # B314 flags stdlib XML parsing as a whole. The specific risk it names
    # (entity expansion) is removed by the target above, which refuses the
    # DOCTYPE that every custom entity must be declared in, so what is left
    # is a parser with no way to expand anything. This is the only
    # annotation of its kind in the tree and is scoped to this one call
    # rather than waived in pyproject.toml, so the next XML parse added
    # anywhere has to make its own argument. B405 (the import) is Low and
    # below the gate's severity floor, so it needs no annotation.
    try:
        parser.feed(text)
        root = parser.close()
    except ET.ParseError as ex:
        raise TaskXmlError(
            "{}: not well-formed XML ({}). If this came from `schtasks "
            "/query /XML`, re-run it with the ONE argument.".format(source, ex)
        ) from ex
    if root.tag == _NS + "Task":
        return [root]
    # The container element carries NO namespace even though every Task
    # under it does (measured on a real export), so it is matched by local
    # name rather than by a namespaced tag.
    tasks = [child for child in root if child.tag == _NS + "Task"]
    if tasks:
        return tasks
    raise TaskXmlError(
        "{}: this is XML but not a Task Scheduler export (its root element "
        "is {!r}). Export tasks with `schtasks /query /TN <task> /XML ONE` "
        "or `Export-ScheduledTask`.".format(source, root.tag)
    )


# --- Small parsers ---------------------------------------------------------
_DURATION = re.compile(
    r"^P(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def duration_seconds(text: str, where: str) -> float:
    """An ISO 8601 duration as seconds.

    The halves are split on ``T`` inside the pattern itself, so the classic
    ``M`` ambiguity cannot be got wrong: before ``T`` it means months and
    after it minutes.  Months and years are refused rather than approximated,
    because neither has a fixed length and every consumer here wants a real
    number of seconds.
    """
    match = _DURATION.match((text or "").strip())
    if match is None:
        raise TaskXmlError(
            "{}: {!r} is not a duration this converter can read (months "
            "and years have no fixed length)".format(where, text)
        )
    parts = match.groupdict()
    return (
        int(parts["weeks"] or 0) * 604800.0
        + int(parts["days"] or 0) * 86400.0
        + int(parts["hours"] or 0) * 3600.0
        + int(parts["minutes"] or 0) * 60.0
        + float(parts["seconds"] or 0)
    )


_BOUNDARY = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[T ](?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.\d+)?(?P<zone>Z|[+-]\d{2}:?\d{2})?$"
)


class Boundary(NamedTuple):
    """A parsed ``StartBoundary``: when, and whether it named a zone."""

    when: datetime.datetime
    zone: str


def parse_boundary(text: str, where: str) -> Boundary:
    """Parse a ``StartBoundary``.

    Hand-rolled rather than ``datetime.fromisoformat``, which does not
    accept a trailing ``Z`` on the oldest interpreter this project supports,
    and that is a live matrix row rather than a theoretical one.
    """
    match = _BOUNDARY.match((text or "").strip())
    if match is None:
        raise TaskXmlError(
            "{}: {!r} is not a timestamp this converter can read".format(
                where, text
            )
        )
    when = datetime.datetime.strptime(
        match.group("date") + " " + match.group("time"), "%Y-%m-%d %H:%M:%S"
    )
    return Boundary(when, match.group("zone") or "")


def _text(parent: Optional[ET.Element], tag: str) -> Optional[str]:
    if parent is None:
        return None
    found = parent.find(_NS + tag)
    if found is None or found.text is None:
        return None
    return found.text.strip()


def _flag(parent: Optional[ET.Element], tag: str) -> Optional[bool]:
    value = _text(parent, tag)
    if value is None:
        return None
    return value.lower() == "true"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


# --- Names -----------------------------------------------------------------
_NAME_REPLACED = re.compile(r"[/#?%\x00-\x1f\x7f-\x9f]")
_NAME_SPACES = re.compile(r"\s+")


def job_name(uri: Optional[str], fallback: str) -> str:
    """A job name derived from a task's ``RegistrationInfo/URI``.

    ``\\`` becomes ``.`` so a Task Scheduler folder reads as a namespace,
    whitespace collapses to ``-`` so the name is usable unquoted on a
    command line, and the four characters that would break the
    ``/jobs/{name}`` route are replaced.  ``:``, braces and parentheses are
    deliberately left alone: they load, they survive the durable store's
    filename mapping, and classic crontab jobs already carry a ``:`` in
    their names.
    """
    text = (uri or "").strip()
    if not text:
        return fallback
    text = text.lstrip("\\").replace("\\", ".")
    text = _NAME_REPLACED.sub("-", text)
    text = _NAME_SPACES.sub("-", text)
    text = text.strip("-.")
    return text or fallback


# --- Trigger lowering ------------------------------------------------------
#: Triggers with no cron equivalent, and one plain sentence for each.
#: ``WnfStateChangeTrigger`` is in none of the documented lists and is the
#: most common trigger on a real Windows 11 box (97 of 195 tasks measured),
#: so it is named for what it is rather than left to a generic message.
_UNCONVERTIBLE_TRIGGERS = {
    "LogonTrigger": "fires when a user logs on, which is not a schedule",
    "IdleTrigger": "fires when the machine goes idle, which is not a schedule",
    "EventTrigger": "fires on a Windows event-log event, which is not a "
    "schedule",
    "SessionStateChangeTrigger": "fires on a session connect, disconnect "
    "or lock, which is not a schedule",
    "RegistrationTrigger": "fires once when the task is registered",
    "WnfStateChangeTrigger": "fires on an internal Windows notification "
    "with no public schema",
}

_WEEKDAYS = (
    ("Monday", "mon"),
    ("Tuesday", "tue"),
    ("Wednesday", "wed"),
    ("Thursday", "thu"),
    ("Friday", "fri"),
    ("Saturday", "sat"),
    ("Sunday", "sun"),
)

_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def repetition_fields(
    start: datetime.datetime, interval_s: float, duration_s: Optional[float]
) -> Optional[tuple[str, str]]:
    """The (minute, hour) cron fields a repetition tiles a day with.

    ``None`` when it cannot be expressed as one cron expression, which is
    most of the interesting cases and is why this is a function rather than
    a division.

    A cron minute field and hour field MULTIPLY: the expression fires at
    every combination of the two.  So a repetition only converts when its
    occurrences over a day are exactly that cross product.  Dividing 1440
    is necessary and not sufficient: ``PT90M`` from midnight divides the
    day, but its occurrences are 00:00, 01:30, 03:00 and so on, whose
    minute-by-hour cross product would also contain 00:30, which is not an
    occurrence.  Widening it silently would double the job's firing rate,
    so it is refused instead.

    A repetition with a bounded ``Duration`` is refused for the same
    reason: a window inside a day is not a cross product either.
    """
    if duration_s is not None and duration_s not in (86400.0,):
        return None
    if interval_s <= 0 or interval_s % 60 or 1440 % (interval_s / 60):
        return None
    if start.second:
        return None
    step = int(interval_s // 60)
    occurrences = set()
    minute_of_day = start.hour * 60 + start.minute
    for index in range(1440 // step):
        moment = (minute_of_day + index * step) % 1440
        occurrences.add((moment // 60, moment % 60))
    hours = sorted({hour for hour, _ in occurrences})
    minutes = sorted({minute for _, minute in occurrences})
    if len(hours) * len(minutes) != len(occurrences):
        return None
    # A field that covers its whole range is written `*`. Exactly the same
    # schedule either way, but an hourly repetition reads as `0 * * * *`
    # rather than as an explicit list of all twenty-four hours.
    return (
        "*" if len(minutes) == 60 else ",".join(str(m) for m in minutes),
        "*" if len(hours) == 24 else ",".join(str(h) for h in hours),
    )


def _repetition(
    trigger: ET.Element, where: str
) -> tuple[Optional[float], Optional[float]]:
    block = trigger.find(_NS + "Repetition")
    if block is None:
        return None, None
    interval = _text(block, "Interval")
    duration = _text(block, "Duration")
    return (
        duration_seconds(interval, where) if interval else None,
        duration_seconds(duration, where) if duration else None,
    )


def _lower_time_trigger(
    trigger: ET.Element, where: str, task: str
) -> tuple[Optional[str], list[Note]]:
    notes: list[Note] = []
    boundary_text = _text(trigger, "StartBoundary")
    if not boundary_text:
        return None, [
            Note(
                task,
                "TimeTrigger",
                "it has no StartBoundary, so there is no instant to convert",
                "",
                True,
            )
        ]
    boundary = parse_boundary(boundary_text, where)
    interval_s, duration_s = _repetition(trigger, where)
    if interval_s is not None:
        fields = repetition_fields(boundary.when, interval_s, duration_s)
        if fields is None:
            return None, [
                Note(
                    task,
                    "TimeTrigger/Repetition",
                    "its repetition does not describe one cron expression "
                    "(a cron minute and hour field multiply, so only a "
                    "repetition that tiles a whole day evenly converts)",
                    "run it at the nearest expressible interval, or "
                    "schedule it more often and gate on a durable cursor",
                    True,
                )
            ]
        minute, hour = fields
        notes.append(
            Note(
                task,
                "TimeTrigger/StartBoundary",
                "the start date became the repetition's phase rather than "
                "a start date, so cronstable begins firing at once instead "
                "of waiting for {}".format(boundary.when.date()),
                "",
                False,
            )
        )
        # The date columns MUST become `*`. Keeping the one-shot's day,
        # month and year turns an hourly repetition on a 2010 trigger into
        # a job that can never fire.
        return "{} {} * * *".format(minute, hour), notes
    if boundary.when.year > _YEAR_HORIZON:
        return None, [
            Note(
                task,
                "TimeTrigger",
                "its start is in {}, past the last year the cron engine "
                "searches".format(boundary.when.year),
                "",
                True,
            )
        ]
    notes.append(
        Note(
            task,
            "TimeTrigger",
            "it runs once, at {}. cronstable loads a one-shot whose "
            "instant has passed and reports it as never firing".format(
                boundary.when.isoformat(sep=" ")
            ),
            "delete the job once it has run, or give it a repeating schedule",
            False,
        )
    )
    return (
        "{} {} {} {} * {}".format(
            boundary.when.minute,
            boundary.when.hour,
            boundary.when.day,
            boundary.when.month,
            boundary.when.year,
        ),
        notes,
    )


def _weekday_list(parent: ET.Element) -> list[str]:
    names = []
    for element, short in _WEEKDAYS:
        if parent.find(_NS + element) is not None:
            names.append(short)
    return names


def _month_list(parent: Optional[ET.Element]) -> str:
    if parent is None:
        return "*"
    numbers = [
        str(index)
        for index, name in enumerate(_MONTHS, start=1)
        if parent.find(_NS + name) is not None
    ]
    return ",".join(numbers) if numbers else "*"


def _lower_schedule_by_day(
    block: ET.Element, boundary: Boundary, task: str
) -> tuple[Optional[str], list[Note]]:
    interval = int(_text(block, "DaysInterval") or "1")
    minute, hour = boundary.when.minute, boundary.when.hour
    if interval <= 1:
        return "{} {} * * *".format(minute, hour), []
    if interval == 7:
        # Exact, because seven divides the week: the same weekday forever.
        weekday = _WEEKDAYS[boundary.when.weekday()][1]
        return "{} {} * * {}".format(minute, hour, weekday), []
    return None, [
        Note(
            task,
            "CalendarTrigger/ScheduleByDay",
            "it runs every {} days, which cron cannot express: a "
            "day-of-month step restarts each month, so it would fire on "
            "the 1st, then every {} days, then the 1st again".format(
                interval, interval
            ),
            "run it daily and gate it on a durable `cronstable cursor`",
            True,
        )
    ]


def _lower_schedule_by_week(
    block: ET.Element, boundary: Boundary, task: str
) -> tuple[Optional[str], list[Note]]:
    interval = int(_text(block, "WeeksInterval") or "1")
    days = block.find(_NS + "DaysOfWeek")
    names = _weekday_list(days) if days is not None else []
    if interval != 1:
        return None, [
            Note(
                task,
                "CalendarTrigger/ScheduleByWeek",
                "it runs every {} weeks, and cron has no week-of-year "
                "phase to express that".format(interval),
                "run it weekly and gate it on a durable `cronstable cursor`",
                True,
            )
        ]
    if not names:
        return None, [
            Note(
                task,
                "CalendarTrigger/ScheduleByWeek",
                "it names no days of the week",
                "",
                True,
            )
        ]
    return (
        "{} {} * * {}".format(
            boundary.when.minute, boundary.when.hour, ",".join(names)
        ),
        [],
    )


def _lower_schedule_by_month(
    block: ET.Element, boundary: Boundary, task: str
) -> tuple[Optional[str], list[Note]]:
    days_element = block.find(_NS + "DaysOfMonth")
    days: list[str] = []
    if days_element is not None:
        for day in days_element.findall(_NS + "Day"):
            value = (day.text or "").strip()
            # "Last" is the schema's own spelling of the month's last day,
            # and the cron dialect spells it L.
            days.append("L" if value.lower() == "last" else value)
    if not days:
        return None, [
            Note(
                task,
                "CalendarTrigger/ScheduleByMonth",
                "it names no days of the month",
                "",
                True,
            )
        ]
    return (
        "{} {} {} {} *".format(
            boundary.when.minute,
            boundary.when.hour,
            ",".join(days),
            _month_list(block.find(_NS + "Months")),
        ),
        [],
    )


def _lower_schedule_by_month_dow(
    block: ET.Element, boundary: Boundary, task: str
) -> tuple[Optional[str], list[Note]]:
    weeks_element = block.find(_NS + "Weeks")
    days_element = block.find(_NS + "DaysOfWeek")
    weeks: list[str] = []
    if weeks_element is not None:
        for element in weeks_element.findall(_NS + "Week"):
            weeks.append((element.text or "").strip())
    names = _weekday_list(days_element) if days_element is not None else []
    if not weeks or not names:
        return None, [
            Note(
                task,
                "CalendarTrigger/ScheduleByMonthDayOfWeek",
                "it names no weeks or no days of the week",
                "",
                True,
            )
        ]
    items = []
    for week in weeks:
        for index, (_name, short) in enumerate(_WEEKDAYS):
            if short not in names:
                continue
            if week.lower() == "last":
                # The dialect spells the month's last such weekday L<n>,
                # counting Sunday as 0.
                items.append("L{}".format((index + 1) % 7))
            else:
                items.append("{}#{}".format(short, week))
    return (
        "{} {} * {} {}".format(
            boundary.when.minute,
            boundary.when.hour,
            _month_list(block.find(_NS + "Months")),
            ",".join(items),
        ),
        [],
    )


def _lower_calendar_trigger(
    trigger: ET.Element, where: str, task: str
) -> tuple[Optional[str], list[Note]]:
    boundary_text = _text(trigger, "StartBoundary")
    if not boundary_text:
        return None, [
            Note(
                task,
                "CalendarTrigger",
                "it has no StartBoundary, so there is no time of day to "
                "convert",
                "",
                True,
            )
        ]
    boundary = parse_boundary(boundary_text, where)
    handlers = (
        ("ScheduleByDay", _lower_schedule_by_day),
        ("ScheduleByWeek", _lower_schedule_by_week),
        ("ScheduleByMonth", _lower_schedule_by_month),
        ("ScheduleByMonthDayOfWeek", _lower_schedule_by_month_dow),
    )
    for tag, handler in handlers:
        block = trigger.find(_NS + tag)
        if block is not None:
            schedule, notes = handler(block, boundary, task)
            if schedule is None:
                return None, notes
            interval_s, duration_s = _repetition(trigger, where)
            if interval_s is not None:
                fields = repetition_fields(
                    boundary.when, interval_s, duration_s
                )
                if fields is None:
                    notes.append(
                        Note(
                            task,
                            "CalendarTrigger/Repetition",
                            "its repetition does not describe one cron "
                            "expression, so only the base schedule was "
                            "converted",
                            "",
                            False,
                        )
                    )
                else:
                    columns = schedule.split(None, 2)
                    schedule = "{} {} {}".format(
                        fields[0], fields[1], columns[2]
                    )
            return schedule, notes
    return None, [
        Note(
            task,
            "CalendarTrigger",
            "it uses a calendar schedule this converter does not read",
            "",
            True,
        )
    ]


def _lower_boot_trigger(
    trigger: ET.Element, where: str, task: str
) -> tuple[Optional[str], list[Note]]:
    notes = []
    if _text(trigger, "Delay"):
        notes.append(
            Note(
                task,
                "BootTrigger/Delay",
                "cronstable runs an @reboot job when the daemon starts, "
                "with no delay after boot",
                "",
                False,
            )
        )
    interval_s, _duration = _repetition(trigger, where)
    if interval_s is not None:
        notes.append(
            Note(
                task,
                "BootTrigger/Repetition",
                "the repetition was not merged into @reboot, which fires "
                "once per boot",
                "add a second job on a repeating schedule if the repeat "
                "matters",
                False,
            )
        )
    return "@reboot", notes


def lower_trigger(
    trigger: ET.Element, where: str, task: str
) -> tuple[Optional[str], list[Note]]:
    """One trigger element as a cron schedule, or ``None`` plus the reason."""
    kind = _local(trigger.tag)
    if kind == "TimeTrigger":
        return _lower_time_trigger(trigger, where, task)
    if kind == "CalendarTrigger":
        return _lower_calendar_trigger(trigger, where, task)
    if kind == "BootTrigger":
        return _lower_boot_trigger(trigger, where, task)
    reason = _UNCONVERTIBLE_TRIGGERS.get(kind)
    if reason is None:
        reason = "cronstable has no equivalent trigger"
    return None, [Note(task, kind, reason, "", True)]


def _trigger_notes(trigger: ET.Element, task: str) -> list[Note]:
    """Caveats that apply to a trigger whatever its type."""
    notes = []
    if _text(trigger, "EndBoundary"):
        notes.append(
            Note(
                task,
                "EndBoundary",
                "cronstable schedules have no end date",
                "delete the job when it should stop, or pause it",
                False,
            )
        )
    if _text(trigger, "RandomDelay"):
        notes.append(
            Note(
                task,
                "RandomDelay",
                "cronstable does not delay a fire by a random amount",
                "a hashed schedule (H) spreads jobs deterministically, "
                "which is a different thing but usually the intent",
                False,
            )
        )
    return notes


# --- Actions ---------------------------------------------------------------
def windows_argv_split(text: str) -> list[str]:
    """Split an ``Arguments`` string by ``CommandLineToArgvW`` rules.

    Unconditionally the Windows rules, with no platform test: the target of
    the conversion is a Windows host no matter where the conversion runs.
    ``shlex.split(posix=False)`` is a different grammar that mishandles a
    backslash before a quote, which is every Windows path.
    """
    args: list[str] = []
    current: list[str] = []
    backslashes = 0
    in_quotes = False
    started = False
    for char in text:
        if char == "\\":
            backslashes += 1
            continue
        if char == '"':
            current.append("\\" * (backslashes // 2))
            if backslashes % 2:
                current.append('"')
            else:
                in_quotes = not in_quotes
                started = True
            backslashes = 0
            continue
        current.append("\\" * backslashes)
        backslashes = 0
        if char.isspace() and not in_quotes:
            if current or started:
                args.append("".join(current))
            current = []
            started = False
            continue
        current.append(char)
        started = True
    current.append("\\" * backslashes)
    if current or started:
        args.append("".join(current))
    return [arg for arg in args if arg != ""] or ([""] if started else [])


def lower_exec(
    action: ET.Element, task: str
) -> tuple[Optional[list[str]], Optional[str], list[Note]]:
    """One ``Exec`` action as an argv plus a working directory."""
    command = _text(action, "Command")
    if not command:
        return (
            None,
            None,
            [Note(task, "Exec", "it names no command", "", True)],
        )
    # Task Scheduler stores a spaced path quoted, because Command and
    # Arguments are concatenated into one command line. cronstable's list
    # form is an argv, where a quote is a literal character in the file
    # name, so the pair has to come off or the program cannot be found.
    if len(command) >= 2 and command[0] == command[-1] == '"':
        command = command[1:-1]
    arguments = _text(action, "Arguments") or ""
    argv = [command] + windows_argv_split(arguments)
    notes = []
    if arguments:
        # The split gets a self check for free: list2cmdline implements the
        # same C runtime rules in reverse, so a round trip that does not
        # come back is a command line whose child parses its own raw text
        # (cmd.exe /c and wmic both do) rather than one this got wrong.
        rebuilt = subprocess.list2cmdline(argv[1:])
        if rebuilt.split() != arguments.split():
            notes.append(
                Note(
                    task,
                    "Exec/Arguments",
                    "the arguments do not survive being split and rebuilt, "
                    "so this program probably parses its own command line",
                    "check the `command` list against the original: {}".format(
                        arguments
                    ),
                    False,
                )
            )
    return argv, _text(action, "WorkingDirectory"), notes


# --- Settings --------------------------------------------------------------
_INSTANCES = {
    "IgnoreNew": "Forbid",
    "Parallel": "Allow",
    "StopExisting": "Replace",
}

_PRIORITY = {
    0: "high",
    1: "high",
    2: "above-normal",
    3: "above-normal",
    7: "below-normal",
    8: "below-normal",
    9: "idle",
    10: "idle",
}


def lower_settings(
    settings: Optional[ET.Element], where: str, task: str
) -> tuple[dict[str, Any], list[Note]]:
    """The ``Settings`` block as job keys, plus what could not be carried."""
    keys: dict[str, Any] = {}
    notes: list[Note] = []
    if settings is None:
        keys["concurrencyPolicy"] = "Forbid"
        return keys, notes
    if _flag(settings, "Enabled") is False:
        keys["enabled"] = False
    limit = _text(settings, "ExecutionTimeLimit")
    if limit:
        seconds = duration_seconds(limit, where)
        # PT0S means "no time limit" in Task Scheduler, and cronstable
        # refuses an executionTimeout that is not greater than zero, so the
        # literal mapping would make a config-load failure out of what the
        # source called unlimited. Measured, 34 of 195 real tasks say PT0S.
        if seconds > 0:
            keys["executionTimeout"] = seconds
    policy = _text(settings, "MultipleInstancesPolicy")
    if policy == "Queue":
        notes.append(
            Note(
                task,
                "MultipleInstancesPolicy",
                "Queue has no cronstable equivalent: an overlapping fire "
                "is either skipped or replaces the running one, never "
                "queued behind it",
                "choose concurrencyPolicy Forbid to skip it or Replace to "
                "cancel the running one",
                False,
            )
        )
        keys["concurrencyPolicy"] = "Forbid"
    else:
        # Absent maps to Forbid too, because IgnoreNew is Task Scheduler's
        # documented default. Getting overlap wrong runs a job twice.
        keys["concurrencyPolicy"] = _INSTANCES.get(policy or "", "Forbid")
    priority = _text(settings, "Priority")
    if priority is not None:
        level = _PRIORITY.get(int(priority))
        if level is not None:
            keys["priority"] = level
        if priority == "0":
            notes.append(
                Note(
                    task,
                    "Priority",
                    "priority 0 is REALTIME, which cronstable does not "
                    "offer at any level; it was lowered to high",
                    "",
                    False,
                )
            )
    for tag, sentence in (
        (
            "RunOnlyIfIdle",
            "cronstable schedules on time and does not wait "
            "for the machine to go idle",
        ),
        (
            "DisallowStartIfOnBatteries",
            "cronstable does not test the power source",
        ),
        (
            "StopIfGoingOnBatteries",
            "cronstable does not stop a run when the machine goes on battery",
        ),
        (
            "RunOnlyIfNetworkAvailable",
            "cronstable does not test for a network before running",
        ),
        ("WakeToRun", "cronstable cannot wake a sleeping machine"),
        (
            "StartWhenAvailable",
            "cronstable catches up missed runs only with "
            "a durable state store and onMissed",
        ),
    ):
        if _flag(settings, tag) is True:
            notes.append(Note(task, tag, sentence, "", False))
    restart = settings.find(_NS + "RestartOnFailure")
    if restart is not None:
        notes.append(
            Note(
                task,
                "RestartOnFailure",
                "cronstable retries a failed run through its own retry "
                "ladder rather than by restarting the task",
                "set onFailure.retry.maximumRetries and initialDelay",
                False,
            )
        )
    return keys, notes


def _principal_notes(task_element: ET.Element, task: str) -> list[Note]:
    principals = task_element.find(_NS + "Principals")
    if principals is None:
        return []
    notes = []
    for principal in principals.findall(_NS + "Principal"):
        for tag in ("UserId", "GroupId", "RunLevel"):
            value = _text(principal, tag)
            if not value:
                continue
            notes.append(
                Note(
                    task,
                    "Principals/" + tag,
                    "the task runs as {} {!r}; cronstable runs every job as "
                    "the account the daemon runs as".format(tag, value),
                    "run the daemon as that account, or split those jobs "
                    "into their own daemon",
                    False,
                )
            )
    return notes


# --- One task --------------------------------------------------------------
def convert_task(
    task_element: ET.Element,
    source: str,
    fallback: str,
    *,
    timezone: Optional[str] = None,
) -> ConvertedTask:
    """Lower one ``<Task>`` into jobs plus everything that did not carry."""
    registration = task_element.find(_NS + "RegistrationInfo")
    label = (_text(registration, "URI") or fallback).strip()
    name = job_name(_text(registration, "URI"), fallback)
    where = "{}: {}".format(source, label)
    notes: list[Note] = list(_principal_notes(task_element, label))

    settings = task_element.find(_NS + "Settings")
    setting_keys, setting_notes = lower_settings(settings, where, label)
    notes += setting_notes

    actions_element = task_element.find(_NS + "Actions")
    execs = (
        actions_element.findall(_NS + "Exec")
        if actions_element is not None
        else []
    )
    other_actions = [
        _local(child.tag)
        for child in (actions_element if actions_element is not None else [])
        if _local(child.tag) != "Exec"
    ]
    for kind in sorted(set(other_actions)):
        notes.append(
            Note(
                label,
                "Actions/" + kind,
                "cronstable runs a command line; a {} action has none".format(
                    kind
                ),
                "",
                True,
            )
        )
    if not execs:
        return ConvertedTask(label, [], notes, True)

    triggers_element = task_element.find(_NS + "Triggers")
    triggers = list(triggers_element) if triggers_element is not None else []
    if not triggers:
        notes.append(
            Note(
                label,
                "Triggers",
                "the task has no trigger, so it is registered but launched "
                "on demand or by another task",
                "give it a schedule, or drive it from a cronstable DAG",
                True,
            )
        )

    zone_keys, zone_notes = _timezone_keys(triggers, label, timezone)

    schedules: list[tuple[str, bool]] = []
    for trigger in triggers:
        notes += _trigger_notes(trigger, label)
        schedule, trigger_notes = lower_trigger(trigger, where, label)
        notes += trigger_notes
        if schedule is not None:
            schedules.append(
                (schedule, _flag(trigger, "Enabled") is not False)
            )

    if not schedules:
        return ConvertedTask(label, [], notes, True)

    commented = len(execs) > 1
    if commented:
        notes.append(
            Note(
                label,
                "Actions",
                "the task runs {} actions in sequence inside one instance, "
                "while separate cronstable jobs on one schedule run at "
                "once".format(len(execs)),
                "chain them as a DAG, or leave one job per action only if "
                "their order does not matter",
                True,
            )
        )

    jobs: list[dict[str, Any]] = []
    for schedule_index, (schedule, trigger_enabled) in enumerate(schedules):
        for action_index, action in enumerate(execs):
            argv, workdir, action_notes = lower_exec(action, label)
            notes += action_notes
            if argv is None:
                continue
            suffix = ""
            if schedule_index:
                suffix += "-t{}".format(schedule_index + 1)
            if action_index:
                suffix += "-a{}".format(action_index + 1)
            job: dict[str, Any] = {
                "name": name + suffix,
                "command": argv,
                "schedule": schedule,
            }
            if workdir:
                job["workingDirectory"] = workdir
            job.update(setting_keys)
            if schedule != "@reboot":
                job.update(zone_keys)
            if not trigger_enabled:
                job["enabled"] = False
            jobs.append(job)
    return ConvertedTask(label, jobs, notes + zone_notes, commented)


def _timezone_keys(
    triggers: list[ET.Element], task: str, timezone: Optional[str]
) -> tuple[dict[str, Any], list[Note]]:
    """How a converted job should read its schedule's clock.

    cronstable evaluates a schedule in UTC by default; Task Scheduler means
    the machine's local time unless the boundary says otherwise.  So a naive
    boundary becomes ``utc: false``, which is exact and guesses no IANA
    name, and an explicit UTC boundary emits nothing because that is already
    the default.

    A stored numeric offset is reported rather than converted.  ``-04:00``
    is not an IANA zone name, so it is not even loadable as ``timezone:``,
    and claiming to reproduce the source's daylight-saving behavior from a
    fixed offset would assert something this conversion cannot know.
    """
    if timezone:
        return {"timezone": timezone}, []
    zones = set()
    for trigger in triggers:
        boundary = _text(trigger, "StartBoundary")
        if boundary:
            match = _BOUNDARY.match(boundary.strip())
            if match is not None:
                zones.add(match.group("zone") or "")
    if not zones or zones == {"Z"} or zones == {"+00:00"}:
        return {}, []
    notes = []
    offsets = sorted(z for z in zones if z and z != "Z")
    if offsets:
        notes.append(
            Note(
                task,
                "StartBoundary",
                "the start time carries the offset {}, which is not a zone "
                "name; the job reads the daemon host's local time "
                "instead".format(", ".join(offsets)),
                "set `timezone:` to the IANA name for that offset if the "
                "daemon runs elsewhere",
                False,
            )
        )
    return {"utc": False}, notes


# --- Emitting --------------------------------------------------------------
_PLAIN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]*$")

#: Keys whose value the config loader does NOT interpolate, so a ``$`` in
#: them needs no escaping.  Everything else does: the loader expands
#: ``${VAR}`` in every other scalar, and ``$$`` is its documented escape.
_UNINTERPOLATED = frozenset({"command", "shell"})


def _scalar(value: Any, *, interpolated: bool = True) -> str:
    """One YAML scalar, quoted the way a Windows path survives.

    Single quotes rather than double, matching what the starter config
    already tells users: in a double-quoted YAML string a backslash starts
    an escape sequence, and every path here is full of them.

    Hand-rolled rather than handed to strictyaml, which cannot interleave
    the comments this emitter depends on.  What makes hand-rolling safe is
    that every emitted document is fed back through the real config parser
    in the tests, so a quoting mistake is a test failure and not a field
    report.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    if interpolated:
        text = text.replace("$", "$$")
    if _PLAIN.match(text) and not text.endswith("."):
        return text
    return "'" + text.replace("'", "''") + "'"


def _render_job(job: dict[str, Any], *, commented: bool) -> list[str]:
    """One ``jobs:`` list item as lines, live or commented out.

    A commented line is the live line with ``# `` inserted right after the
    two-space list indent, so deleting that prefix from each line of a block
    restores it exactly.
    """
    lines: list[str] = []
    for key, value in job.items():
        prefix = "  - " if not lines else "    "
        if key == "command" and isinstance(value, list):
            lines.append("{}{}:".format(prefix, key))
            for item in value:
                lines.append(
                    "      - {}".format(_scalar(item, interpolated=False))
                )
            continue
        if key == "schedule":
            lines.append('{}{}: "{}"'.format(prefix, key, str(value)))
            continue
        lines.append(
            "{}{}: {}".format(
                prefix,
                key,
                _scalar(value, interpolated=key not in _UNINTERPOLATED),
            )
        )
    if not commented:
        return lines
    return [
        "  # " + line[2:] if line.startswith("  ") else line for line in lines
    ]


def render_yaml(tasks: list[ConvertedTask], *, sources: list[str]) -> str:
    """The converted configuration, or the empty string.

    The return value is either empty or a document with exactly one
    top-level ``jobs:`` key and at least one live item.  That is not a style
    choice, it is forced: two ``jobs:`` keys are a duplicate-key error, a
    document of only comments is a parse error, and any unloadable file in a
    config directory fails the load of every other file in that directory.
    So when nothing converted, nothing is written, and the whole inventory
    reaches the operator through the report instead.

    The header names the source files and nothing else.  No timestamp and no
    version, so converting the same export twice gives the same bytes.
    """
    live = [task for task in tasks if task.jobs and not task.commented]
    if not live:
        return ""
    names = ", ".join(sorted({os.path.basename(s) for s in sources}))
    out = [
        "# Converted from {} by `cronstable import-taskscheduler`.".format(
            names
        ),
        "# Review every job before loading it: exporting a task does not",
        "# unregister it, so Task Scheduler is still running these.",
        "jobs:",
    ]
    for task in tasks:
        if not task.jobs:
            continue
        out.append("  # {}".format(task.label))
        if task.commented:
            out.append("  # NOT CONVERTED, see the report; left here to edit:")
        for job in task.jobs:
            out.extend(_render_job(job, commented=task.commented))
    return "\n".join(out) + "\n"


def render_report(tasks: list[ConvertedTask]) -> str:
    """What was converted and, for everything else, why it was not."""
    converted = sum(1 for t in tasks if t.jobs and not t.commented)
    jobs = sum(len(t.jobs) for t in tasks if not t.commented)
    lines = [
        "Read {} task(s): {} converted into {} job(s), {} not "
        "converted.".format(
            len(tasks), converted, jobs, len(tasks) - converted
        )
    ]
    for task in tasks:
        if not task.notes:
            continue
        blocking = [note for note in task.notes if note.blocking]
        state = "NOT CONVERTED" if (blocking or task.commented) else "note"
        lines.append("")
        lines.append("{}: {}".format(state, task.label))
        for note in task.notes:
            lines.append("  {}: {}".format(note.element, note.reason))
            if note.remedy:
                lines.append("    try: {}".format(note.remedy))
    return "\n".join(lines) + "\n"


# --- The command -----------------------------------------------------------
def convert_source(
    path: str, *, timezone: Optional[str], strict: bool
) -> tuple[list[ConvertedTask], list[Note]]:
    """Every task in one file (or standard input).

    ``strict`` separates a file the operator named from one a directory scan
    found.  A named file that is not an export is a mistake worth stopping
    for; a stray ``.xml`` sitting beside real ones is a report row and a
    skip, because ``.xml`` is a name half the tooling on a Windows box
    writes.
    """
    data, label = read_source(path)
    text = strip_xml_declarations(decode_task_xml(data, label))
    try:
        documents = parse_task_documents(text, label)
    except TaskXmlError:
        if strict:
            raise
        return [], [
            Note(
                label,
                "file",
                "skipped: it is not a Task Scheduler export",
                "",
                False,
            )
        ]
    stem = (
        "stdin" if path == "-" else os.path.splitext(os.path.basename(path))[0]
    )
    return [
        convert_task(
            document,
            label,
            "{}-{}".format(stem, index),
            timezone=timezone,
        )
        for index, document in enumerate(documents, start=1)
    ], []


def _expand(paths: list[str]) -> list[tuple[str, bool]]:
    """Each input as ``(path, strict)``.  A directory expands to its xml.

    Directory input exists because neither cmd.exe nor a native executable
    launched from PowerShell expands a wildcard, and Python does not glob
    argv, so on the target platform naming a directory is the only way to
    convert an estate without typing every filename.
    """
    out: list[tuple[str, bool]] = []
    for path in paths:
        if path != "-" and os.path.isdir(path):
            for name in sorted(os.listdir(path)):
                if name.lower().endswith(".xml"):
                    out.append((os.path.join(path, name), False))
            continue
        out.append((path, True))
    return out


def _unique_names(tasks: list[ConvertedTask]) -> None:
    """Make every job name unique across the conversion, in place.

    The config loader's duplicate-name refusal is the backstop, so a
    collision missed here fails the converted file's load loudly rather than
    dropping a job.  Deduplicating anyway keeps the ordinary case from
    needing a hand edit.
    """
    seen: dict[str, int] = {}
    for task in tasks:
        for job in task.jobs:
            name = job["name"]
            if name not in seen:
                seen[name] = 1
                continue
            seen[name] += 1
            job["name"] = "{}-{}".format(name, seen[name])


def dispatch(args: Any) -> int:
    """Run ``cronstable import-taskscheduler``.

    Exit codes are the binary's own three: 0 when something was converted,
    1 when the input could not be read or produced nothing usable, 2 for a
    usage error.  There is deliberately no separate code for a partial
    conversion, because on a whole-machine export a partial conversion is
    the normal outcome, and a distinct code would make every successful
    first run look like a failure to a shell script.
    """
    tasks: list[ConvertedTask] = []
    skipped: list[Note] = []
    sources = []
    try:
        for path, strict in _expand(list(args.paths)):
            sources.append(path)
            converted, notes = convert_source(
                path,
                timezone=getattr(args, "timezone", None),
                strict=strict,
            )
            tasks.extend(converted)
            skipped.extend(notes)
    except TaskXmlError as ex:
        print(
            "cronstable import-taskscheduler: {}".format(ex),
            file=sys.stderr,
        )
        return 1
    _unique_names(tasks)
    document = render_yaml(tasks, sources=sources)
    report = render_report(tasks)
    for note in skipped:
        report += "skipped: {}\n".format(note.task)
    print(report, file=sys.stderr, end="")
    if not document:
        print(
            "cronstable import-taskscheduler: nothing was converted, so no "
            "configuration was written. The report above says why, task by "
            "task.",
            file=sys.stderr,
        )
        return 1
    output = getattr(args, "output", None)
    if not output:
        sys.stdout.write(document)
        return 0
    try:
        # newline="\n": the repository is LF only and the release build
        # fails on a CRLF blob, while Python's default text mode would
        # write CRLF on the Windows rows alone.
        with open(output, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(document)
    except OSError as ex:
        print(
            "cronstable import-taskscheduler: could not write {}: {}".format(
                output, ex
            ),
            file=sys.stderr,
        )
        return 1
    print("wrote {}".format(output), file=sys.stderr)
    print(
        "review it, then check it with: cronstable -v -c {}".format(output),
        file=sys.stderr,
    )
    return 0
