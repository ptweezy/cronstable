"""Human-facing schedule intelligence, shared by every surface.

Plain-English descriptions (:func:`describe_cron`), fire previews
(:func:`next_fires`) and the advisory schedule linter
(:func:`lint_schedule`) in one importable module, so the TUI, the daemon's
``GET /schedule/preview`` endpoint and any future MCP tool all agree with
the engine that actually schedules (:mod:`cronstable.cronexpr`) instead of
re-implementing the arithmetic.  The TUI re-exports the names it always
had, so ``cronstable.tui.describe_cron`` keeps working.

The describers are deliberately tolerant: text the engine rejects degrades
to a "Custom schedule" phrase rather than raising, because the TUI renders
them while the user is still typing.  The linter is the opposite: it
assumes the expression already parses and reports advisory
:class:`Finding` rows for legal schedules that probably do not mean what
they say (level ``"warning"``) or behave in a way worth knowing about
(level ``"note"``).  Config loading logs the findings per job and the
status payloads carry them to the dashboards.
"""

import datetime
import itertools
import re
from typing import Dict, List, NamedTuple, Optional, Sequence, Set

from cronstable.cronexpr import CronTab

__all__ = [
    "Finding",
    "describe_cron",
    "lint_schedule",
    "next_fires",
    "pad2",
]


def pad2(n: int) -> str:
    return "%02d" % n


# ===================================================================
#  plain-English descriptions (ports of the web page's describeCron)
# ===================================================================
_MONTHS = [
    "",
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
]
_DOWN = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]
_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MACRO_TEXT = {
    "@yearly": "Once a year, at midnight on 1 January",
    "@annually": "Once a year, at midnight on 1 January",
    "@monthly": "At midnight on the 1st of every month",
    "@weekly": "At midnight every Sunday",
    "@daily": "Every day at midnight",
    "@midnight": "Every day at midnight",
    "@hourly": "Every hour, on the hour",
}


def _ordinal(n: int) -> str:
    suffix = ["th", "st", "nd", "rd"]
    v = n % 100
    if 20 <= v or v < 10:
        return "%d%s" % (n, suffix[v % 10] if v % 10 < 4 else "th")
    return "%dth" % n


def _list_join(items: Sequence[str]) -> str:
    parts = list(items)
    if len(parts) <= 1:
        return "".join(parts)
    if len(parts) == 2:
        return "%s and %s" % (parts[0], parts[1])
    return "%s and %s" % (", ".join(parts[:-1]), parts[-1])


def _field_values(
    spec: str, lo: int, hi: int, names: Optional[Dict[str, int]] = None
) -> Optional[List[int]]:
    """Enumerate a cron field, or ``None`` for an unrestricted ``*``/``?``.

    A tolerant re-implementation of the web page's ``parseField`` (kept
    here rather than reaching into :class:`CronTab` internals so malformed
    input degrades to prose instead of raising).
    """
    spec = spec.strip().lower()
    if spec in ("*", "?"):
        return None
    out: Set[int] = set()
    for part in spec.split(","):
        body, step = part, 1
        if "/" in part:
            body, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValueError("bad step: %s" % part)
            step = int(step_text)

        def resolve(token: str) -> Optional[int]:
            token = token.strip().lower()
            if names and token in names:
                return names[token]
            return int(token) if token.isdigit() else None

        if body == "*":
            start, end = lo, hi
        elif "-" in body:
            a, b = body.split("-", 1)
            start_v, end_v = resolve(a), resolve(b)
            if start_v is None or end_v is None:
                raise ValueError("bad field: %s" % part)
            start, end = start_v, end_v
        else:
            v = resolve(body)
            if v is None:
                raise ValueError("bad field: %s" % part)
            start, end = v, (hi if "/" in part else v)
        values: List[int]
        if start <= end:
            values = list(range(start, end + 1, step))
        else:  # wrap-around range, e.g. fri-mon
            values = list(range(start, hi + 1, step)) + list(
                range(lo, end + 1, step)
            )
        for v in values:
            v = 0 if (hi == 6 and v == 7) else v
            if v < lo or v > hi:
                # out-of-range values (month 13, dow 8, minute 60) would
                # index past the name tables below; degrade to prose.
                raise ValueError("out of range: %s" % part)
            out.add(v)
    return sorted(out)


_MON_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_DOW_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def describe_cron(expr: str) -> str:
    """Plain-English schedule text, a port of the web ``describeCron``.

    Handles the 5-field core plus the 6-/7-field (year / second) forms the
    daemon accepts; anything it cannot phrase degrades to ``Custom
    schedule: <expr>`` rather than raising.
    """
    low = (expr or "").strip().lower()
    if low == "@reboot":
        return "Once, when cronstable starts (@reboot)"
    if low in _MACRO_TEXT:
        return _MACRO_TEXT[low]
    fields = _MACROS.get(low, expr).split()
    try:
        sec_spec, year_spec = "0", "*"
        if len(fields) == 5:
            core = fields
        elif len(fields) == 6:
            core, year_spec = fields[:5], fields[5]
        elif len(fields) == 7:
            sec_spec, core, year_spec = fields[0], fields[1:6], fields[6]
        else:
            return "Custom schedule: %s" % expr
        mi, hr, dom, mon, dow = core
        minutes = _field_values(mi, 0, 59)
        hours = _field_values(hr, 0, 23)
        doms = _field_values(dom, 1, 31)
        months = _field_values(mon, 1, 12, _MON_NAMES)
        dows = _field_values(dow, 0, 6, _DOW_NAMES)
        seconds = _field_values(sec_spec, 0, 59)
        years = (
            _field_values(year_spec, 1970, 2099) if year_spec != "*" else None
        )
    except (ValueError, KeyError):
        return "Custom schedule: %s" % expr

    time_part = _describe_time(mi, hr, minutes, hours)

    day_clauses = []
    if dows is not None:
        day_clauses.append("on " + _list_join([_DOWN[d] for d in dows]))
    if doms is not None:
        day_clauses.append(
            "on the "
            + _list_join([_ordinal(d) for d in doms])
            + (" of the month" if dows is None else "")
        )
    clauses = []
    if len(day_clauses) == 2:
        # dom and dow must BOTH match when both are restricted: the
        # daemon's engine (cronexpr._day_matches) deliberately keeps
        # parse-crontab's AND rule ("0 0 13 * 5" is Friday the 13th),
        # unlike std cron's OR, so the prose must say so too.
        clauses.append("%s, and only %s" % (day_clauses[1], day_clauses[0]))
    elif day_clauses:
        clauses.append(day_clauses[0])
    if months is not None:
        clauses.append("in " + _list_join([_MONTHS[m] for m in months]))
    elif doms is None and dows is None:
        clauses.append("every day")
    if years is not None:
        clauses.append("in " + _list_join([str(y) for y in years]))
    base = ", ".join([time_part] + clauses)

    if seconds != [0] and len(fields) == 7:
        top_free = (
            minutes is None
            and hours is None
            and doms is None
            and months is None
            and dows is None
            and years is None
        )
        return _describe_seconds(sec_spec, seconds, base, top_free)
    return base


def _describe_time(
    mi: str,
    hr: str,
    minutes: Optional[List[int]],
    hours: Optional[List[int]],
) -> str:
    """The leading time-of-day phrase of :func:`describe_cron`."""
    step_m = re.match(r"^\*/(\d+)$", mi)
    step_h = re.match(r"^\*/(\d+)$", hr)
    # "*/n" only reads as a true fixed interval when n divides the span;
    # otherwise the pre-boundary gap is shorter, so enumerate instead.
    step_m_ok = step_m is not None and 60 % int(step_m.group(1)) == 0
    step_h_ok = step_h is not None and 24 % int(step_h.group(1)) == 0
    if minutes is None and hours is None:
        return "Every minute"
    if step_m_ok and hours is None:
        assert step_m is not None
        return "Every %s minutes" % step_m.group(1)
    if minutes is None and step_h_ok:
        assert step_h is not None
        return "Every minute, every %s hours" % step_h.group(1)
    if minutes is not None and hours is None and mi.isdigit():
        return "Every hour at :%s" % pad2(int(mi))
    if step_h_ok and mi.isdigit():
        assert step_h is not None
        return "At :%s every %s hours" % (pad2(int(mi)), step_h.group(1))
    if mi.isdigit() and hr.isdigit():
        return "At %s:%s" % (pad2(int(hr)), pad2(int(mi)))
    mp = (
        "every minute"
        if minutes is None
        else "minute%s %s"
        % (
            "s" if len(minutes) > 1 else "",
            ", ".join(pad2(x) for x in minutes),
        )
    )
    hp = (
        "every hour"
        if hours is None
        else "hour%s %s"
        % ("s" if len(hours) > 1 else "", ", ".join(pad2(x) for x in hours))
    )
    return "At %s past %s" % (mp, hp)


def _describe_seconds(
    sec_spec: str,
    seconds: Optional[List[int]],
    base: str,
    top_free: bool,
) -> str:
    """The seconds clause of :func:`describe_cron` (7-field forms).

    A standalone cadence phrase ("Every N seconds") is only true when
    nothing above the seconds column is restricted; otherwise the seconds
    merely sub-select within the matched minutes, so they append as a
    qualifying clause instead of overstating the frequency.
    """
    step_s = re.match(r"^\*/(\d+)$", sec_spec)
    step_s_ok = step_s is not None and 60 % int(step_s.group(1)) == 0
    if top_free:
        if seconds is None:
            return "Every second"
        if step_s_ok:
            assert step_s is not None
            return "Every %s seconds" % step_s.group(1)
        return "At second%s %s" % (
            "s" if len(seconds) > 1 else "",
            ", ".join(pad2(x) for x in seconds),
        )
    if seconds is None:
        return base + ", every second"
    return base + ", at second%s %s" % (
        "s" if len(seconds) > 1 else "",
        ", ".join(pad2(x) for x in seconds),
    )


# ===================================================================
#  fire previews
# ===================================================================
def next_fires(
    schedule: str,
    count: int,
    tz: Optional[datetime.tzinfo] = None,
    start: Optional[datetime.datetime] = None,
) -> List[datetime.datetime]:
    """The next ``count`` fire times of a schedule, straight from the
    daemon's own engine (:meth:`CronTab.occurrences`), so the preview
    always agrees with what the scheduler will actually do.  Returns
    ``[]`` for @reboot, for an expression the engine rejects, and for a
    schedule with no remaining occurrence.  ``tz`` picks the frame when
    ``start`` is omitted (UTC by default); with an aware start the
    returned datetimes are aware in that frame.
    """
    text = (schedule or "").strip()
    if text.lower() == "@reboot":
        return []
    try:
        tab = CronTab(text)
    except (ValueError, KeyError):
        return []
    zone = tz or datetime.timezone.utc
    current = start if start is not None else datetime.datetime.now(zone)
    return list(itertools.islice(tab.occurrences(current), count))


# ===================================================================
#  the advisory schedule linter
# ===================================================================
class Finding(NamedTuple):
    """One advisory lint result for a schedule."""

    #: stable machine identifier, kebab-case (dashboards key styling on it)
    code: str
    #: ``"warning"`` (probable mistake) or ``"note"`` (behaviour worth
    #: knowing about)
    level: str
    #: one line of plain text, self-contained enough for a log line
    message: str


LEVEL_WARNING = "warning"
LEVEL_NOTE = "note"

_FULL_DOM = frozenset(range(1, 32))
_FULL_DOW = frozenset(range(7))
#: spans for the uneven-step rule: values wrap modulo the span, so a star
#: step that does not divide it leaves one short interval at the wrap.
#: Day-of-month is handled separately (month lengths vary) and the year
#: column does not wrap at all.
_STEP_SPANS = {
    "second": (60, "seconds"),
    "minute": (60, "minutes"),
    "hour": (24, "hours"),
    "month": (12, "months"),
    "day-of-week": (7, "days"),
}
#: the longest length each month can have (February counts its leap 29th)
_MONTH_MAX = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


def lint_schedule(
    expression: str,
    timezone: Optional[datetime.tzinfo] = None,
    now: Optional[datetime.datetime] = None,
) -> List[Finding]:
    """Advisory findings for a schedule the engine accepts.

    Returns ``[]`` for ``@reboot`` and for text that does not parse:
    rejecting bad syntax is the parser's job, the linter only flags legal
    schedules that probably do not mean what the author thinks.
    ``timezone`` is the job's resolved zone and enables the DST checks
    (skipped when ``None``, since the daemon's local zone rules are not
    knowable here, and for fixed-offset zones, which never transition).
    ``now`` fixes the reference instant for determinism in tests; it
    defaults to the current time in ``timezone`` (or UTC).
    """
    text = (expression or "").strip()
    if text.lower() == "@reboot":
        return []
    try:
        tab = CronTab(text)
    except (ValueError, KeyError):
        return []
    if now is None:
        now = datetime.datetime.now(timezone or datetime.timezone.utc)
    findings: List[Finding] = []
    dead = tab.next(now=now, default_utc=True) is None
    if dead:
        findings.append(
            Finding("never-fires", LEVEL_WARNING, _never_fires_message(tab))
        )
    findings.extend(_lint_day_fields(tab))
    findings.extend(_lint_steps(text))
    if not dead:
        # pointless refinements of "it never fires at all"
        findings.extend(_lint_month_lengths(tab))
        if timezone is not None:
            findings.extend(_lint_dst(tab, timezone, now))
    return findings


def _never_fires_message(tab: CronTab) -> str:
    years = tab.years
    if years is not None:
        return (
            "no future occurrence: the year column ends at {}, so this "
            "schedule will never fire again".format(max(years))
        )
    return (
        "no future occurrence: the day, month and weekday fields never all "
        "line up on a real date, so this schedule will never fire"
    )


def _lint_day_fields(tab: CronTab) -> List[Finding]:
    """Both day fields restricted: the AND-semantics footgun.

    This dialect requires a day to satisfy BOTH fields (deliberately, see
    cronexpr), while classic Vixie cron fires when EITHER matches, so a
    schedule imported from a system crontab fires less often than it did
    there.  Say so whenever the combination appears.
    """
    # a field whose plain values already cover the whole range matches
    # every day whatever else (an L form) rides along, so only the
    # subset test decides restriction; a bare L leaves the plain set
    # empty, which the same test correctly reads as restricted.
    dom_restricted = not (_FULL_DOM <= tab.days_of_month)
    dow_restricted = not (_FULL_DOW <= tab.days_of_week)
    if dom_restricted and dow_restricted:
        return [
            Finding(
                "day-fields-both-restricted",
                LEVEL_WARNING,
                "day-of-month and day-of-week are both restricted, and a "
                "day must satisfy BOTH here ('0 0 13 * 5' is Friday the "
                "13th); classic Vixie cron fires when either field "
                "matches, so a schedule imported from a system crontab "
                "fires less often than it did there",
            )
        ]
    return []


def _lint_steps(expression: str) -> List[Finding]:
    """Star steps that do not divide their field's span run unevenly.

    ``*/7`` in the minute field fires at :56 and then :00 four minutes
    later, because the values restart at the wrap.  Only ``*/n`` items are
    flagged: an explicit range with a step reads as deliberate.
    Day-of-month gets its own note (steps restart at day 1 each month and
    month lengths differ), and the year column never wraps.
    """
    low = expression.strip().lower()
    fields = _MACROS.get(low, low).split()
    if len(fields) == 7:
        labels = (
            "second",
            "minute",
            "hour",
            "day-of-month",
            "month",
            "day-of-week",
        )
    else:
        # 5 fields, or 6 where the extra trailing column is the year;
        # zip() drops it either way
        labels = ("minute", "hour", "day-of-month", "month", "day-of-week")
    findings: List[Finding] = []
    # 6-field forms have one more field (the year) than labels; the year
    # column never wraps, so non-strict zip dropping it is the point
    for label, field in zip(labels, fields, strict=False):
        for item in field.split(","):
            head, slash, step_text = item.partition("/")
            if not slash or head != "*" or not step_text.isdigit():
                continue
            step = int(step_text)
            if step <= 1:
                continue
            if label == "day-of-month":
                findings.append(
                    Finding(
                        "uneven-step",
                        LEVEL_NOTE,
                        "'{}' in the day-of-month field restarts at day 1 "
                        "every month, and month lengths differ, so the "
                        "interval between runs varies at month "
                        "boundaries".format(item),
                    )
                )
                continue
            span, unit = _STEP_SPANS[label]
            if span % step:
                gap = span - ((span - 1) // step) * step
                findings.append(
                    Finding(
                        "uneven-step",
                        LEVEL_WARNING,
                        "'{}' in the {} field: {} does not divide the "
                        "field's span of {}, so one interval at the wrap "
                        "is only {} {}".format(
                            item,
                            label,
                            step,
                            span,
                            gap,
                            unit if gap != 1 else unit[:-1],
                        ),
                    )
                )
    return findings


def _lint_month_lengths(tab: CronTab) -> List[Finding]:
    """Selected days that no selected month is long enough to reach."""
    dom = tab.days_of_month
    if tab.last_day_of_month or not dom or _FULL_DOM <= dom:
        return []
    findings: List[Finding] = []
    dmin = min(dom)
    skipped = [m for m in sorted(tab.months) if dmin > _MONTH_MAX[m]]
    if skipped:
        findings.append(
            Finding(
                "skipped-months",
                LEVEL_WARNING,
                "the smallest selected day of month is {}, which never "
                "occurs in {}; {} skipped entirely".format(
                    dmin,
                    _list_join([_MONTHS[m] for m in skipped]),
                    "that month is"
                    if len(skipped) == 1
                    else "those months are",
                ),
            )
        )
    if 2 in tab.months and 2 not in skipped:
        feb_days = [d for d in dom if d <= _MONTH_MAX[2]]
        if feb_days and min(feb_days) == 29:
            findings.append(
                Finding(
                    "leap-day-only",
                    LEVEL_NOTE,
                    "in February only day 29 can match, so February runs "
                    "occur only in leap years",
                )
            )
    return findings


def _lint_dst(
    tab: CronTab,
    timezone: datetime.tzinfo,
    now: datetime.datetime,
) -> List[Finding]:
    """DST transition notes for schedules with restricted hours.

    Scans the coming year for utcoffset changes in the zone; for each
    transition, reports the first scheduled wall time that falls in the
    skipped (nonexistent) or repeated (ambiguous) window.  Schedules with
    unrestricted hours are skipped: they fire right through a transition
    and have no single anomalous wall time worth calling out.
    """
    if len(tab.hours) >= 24:
        return []
    if isinstance(timezone, datetime.timezone):
        return []  # fixed-offset zones (UTC included) never transition
    if now.tzinfo is not None:
        day0 = now.astimezone(timezone).date()
    else:
        day0 = now.date()
    findings: List[Finding] = []
    prev_offset = _offset_at(timezone, day0)
    for i in range(1, 367):
        day = day0 + datetime.timedelta(days=i)
        offset = _offset_at(timezone, day)
        if offset != prev_offset:
            # the offset changed somewhere in the 24h before `day` 00:00;
            # scan both civil dates the window can touch
            finding = _dst_finding(
                tab, timezone, day - datetime.timedelta(days=1)
            )
            if finding is not None:
                findings.append(finding)
                if len(findings) >= 2:
                    break
        prev_offset = offset
    return findings


def _offset_at(
    timezone: datetime.tzinfo, day: datetime.date
) -> Optional[datetime.timedelta]:
    return (
        datetime.datetime.combine(day, datetime.time(0))
        .replace(tzinfo=timezone)
        .utcoffset()
    )


def _dst_finding(
    tab: CronTab, timezone: datetime.tzinfo, first_day: datetime.date
) -> Optional[Finding]:
    """The first scheduled wall time a transition around ``first_day``
    skips or repeats, as a Finding, or ``None`` when the schedule misses
    the anomalous window (or the day fields exclude the date)."""
    second = min(tab.seconds)
    zone_name = str(timezone)
    for offset in (0, 1):
        day = first_day + datetime.timedelta(days=offset)
        # cheap pre-pass: which hours are anomalous at all on this date
        # (probed on the half hour too, for zones with :30 transitions)
        affected: Set[int] = set()
        for hour in range(24):
            for minute in (0, 30):
                civil = datetime.datetime.combine(
                    day, datetime.time(hour, minute)
                )
                if _classify(timezone, civil) is not None:
                    affected.add(hour)
        for hour in sorted(affected & tab.hours):
            for minute in sorted(tab.minutes):
                civil = datetime.datetime.combine(
                    day, datetime.time(hour, minute, second)
                )
                if not tab.test(civil):
                    continue  # day fields or year exclude this date
                kind = _classify(timezone, civil)
                if kind == "gap":
                    return Finding(
                        "dst-skipped-time",
                        LEVEL_NOTE,
                        "on {} the wall time {:02d}:{:02d} does not exist "
                        "in {} (clocks jump forward); that run fires at "
                        "the shifted wall time instead of being "
                        "skipped".format(
                            day.isoformat(), hour, minute, zone_name
                        ),
                    )
                if kind == "fold":
                    return Finding(
                        "dst-repeated-time",
                        LEVEL_NOTE,
                        "on {} the wall time {:02d}:{:02d} occurs twice in "
                        "{} (clocks fall back); the run fires on the "
                        "first occurrence only".format(
                            day.isoformat(), hour, minute, zone_name
                        ),
                    )
    return None


def _classify(
    timezone: datetime.tzinfo, civil: datetime.datetime
) -> Optional[str]:
    """``"gap"`` (nonexistent), ``"fold"`` (ambiguous) or ``None``."""
    off0 = civil.replace(tzinfo=timezone, fold=0).utcoffset()
    off1 = civil.replace(tzinfo=timezone, fold=1).utcoffset()
    if off0 == off1:
        return None
    aware = civil.replace(tzinfo=timezone)
    roundtrip = aware.astimezone(datetime.timezone.utc).astimezone(timezone)
    if roundtrip.replace(tzinfo=None, fold=0) == civil:
        return "fold"
    return "gap"
