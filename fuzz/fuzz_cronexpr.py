"""Fuzz the cron expression parser and the schedule tools built on it.

Each input is one schedule string. CronTab must reject bad text with
ValueError alone. Accepted text must reparse to an equal schedule, and its
next and previous fires must match it. describe_cron and lint_schedule must
answer any text without raising.
"""

import datetime
import itertools
import logging
import sys
import zoneinfo

import atheris

with atheris.instrument_imports(include=["cronstable"]):
    from cronstable.cronexpr import CronTab
    from cronstable.croninfo import describe_cron, lint_schedule

HASH_KEY = "fuzz-job"
UTC = datetime.timezone.utc
# A leap day, and both ends of the engine's 1970 to 2099 search window.
NAIVE_NOWS = (
    datetime.datetime(2024, 2, 29, 12, 0, 30),
    datetime.datetime(1970, 1, 1, 0, 0, 1),
    datetime.datetime(2099, 12, 31, 23, 59, 59),
)
# Just before a spring-forward gap and a fall-back overlap.
ZONE = zoneinfo.ZoneInfo("America/New_York")
AWARE_NOWS = (
    datetime.datetime(2026, 3, 8, 1, 30, tzinfo=ZONE),
    datetime.datetime(2026, 11, 1, 0, 30, tzinfo=ZONE),
)


def check_naive(tab: CronTab, now: datetime.datetime) -> bool:
    """Check the fires on either side of ``now``; False if it has none."""
    ahead = tab.next(now)
    if ahead is not None:
        assert ahead > 0
        assert tab.test(now + datetime.timedelta(seconds=ahead))
    behind = tab.prev(now)
    if behind is not None:
        assert behind > 0
        assert tab.test(now - datetime.timedelta(seconds=behind))
    return ahead is not None or behind is not None


def check_aware(tab: CronTab, start: datetime.datetime) -> None:
    # occurrences() is next() iterated over real instants
    previous = start.astimezone(UTC)
    cursor = start
    for fire in itertools.islice(tab.occurrences(start), 3):
        fire_utc = fire.astimezone(UTC)
        assert fire_utc > previous
        # the label is the one the wall clock shows at that instant
        assert fire_utc.astimezone(ZONE).replace(tzinfo=None) == (
            fire.replace(tzinfo=None)
        )
        delay = tab.next(cursor)
        assert delay is not None
        elapsed = datetime.timedelta(seconds=delay)
        assert cursor.astimezone(UTC) + elapsed == fire_utc
        previous = fire_utc
        cursor = fire
    behind = tab.prev(start)
    assert behind is None or behind > 0


def TestOneInput(data: bytes) -> None:
    text = data.decode("utf-8", "replace")
    describe_cron(text, hash_key=HASH_KEY)
    lint_schedule(text, timezone=ZONE, now=AWARE_NOWS[0], hash_key=HASH_KEY)
    try:
        tab = CronTab(text, hash_key=HASH_KEY)
    except ValueError:
        return
    assert CronTab(str(tab), hash_key=HASH_KEY) == tab
    # the resolved source is plain dialect and needs no key
    assert CronTab(tab.resolved_source) == tab
    if not check_naive(tab, NAIVE_NOWS[0]):
        # It never fires, so every further search would scan the whole
        # window, which takes seconds under instrumentation.
        return
    for now in NAIVE_NOWS[1:]:
        check_naive(tab, now)
    for now in AWARE_NOWS:
        check_aware(tab, now)


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
