"""Test cron engine properties across generated schedules and times.

``tests/data/cron_golden.json`` covers fixed expressions and instants.
These tests generate expressions from the supported grammar and select
time zones with varied clock transitions. They check these properties:

* Timezone-naive searches agree with a brute-force scan using ``test()``.
* ``next``, ``prev``, and ``occurrences`` agree.
* Timezone-aware searches match an independent implementation of the DST
  policy: resolve local matches with ``fold=0`` and yield each instant once.
* Parsed expressions round-trip, and invalid input raises ``ValueError``.
"""

import datetime
import itertools

import pytest
from hypothesis import HealthCheck, assume, example, given, settings
from hypothesis import strategies as st

from cronstable.cronexpr import CronTab
from tests._strategies import (
    aware_datetimes,
    cron_expressions,
    junk_text,
    naive_datetimes,
    zone,
)

UTC = datetime.timezone.utc
MINUTE = datetime.timedelta(minutes=1)
SECOND = datetime.timedelta(seconds=1)
#: how far the brute-force scans look; wide enough that most generated
#: schedules fire inside it, small enough to scan minute by minute.
SCAN = datetime.timedelta(days=3)

slow = settings(
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)


def _brute_next(tab, now):
    """First minute-granular match strictly after ``now`` within SCAN."""
    cursor = now.replace(second=0, microsecond=0) + MINUTE
    end = now + SCAN
    while cursor <= end:
        if tab.test(cursor):
            return cursor
        cursor += MINUTE
    return None


def _brute_prev(tab, now):
    cursor = now.replace(second=0, microsecond=0)
    if cursor >= now:
        cursor -= MINUTE
    end = now - SCAN
    while cursor >= end:
        if tab.test(cursor):
            return cursor
        cursor -= MINUTE
    return None


# --- parsing ---------------------------------------------------------------


@given(cron_expressions())
def test_generated_expressions_parse_and_round_trip(expr):
    tab = CronTab(expr)
    # str() is the whitespace-normalized source and reparses to an equal
    # schedule; equality is semantic, so the round trip is exact.
    assert str(tab) == " ".join(expr.split())
    assert CronTab(str(tab)) == tab
    assert CronTab(expr.upper()) == tab
    assert not tab.resolved_differs
    assert tab.resolved_source == str(tab)


@given(cron_expressions(hashed=True), st.text(min_size=1, max_size=30))
def test_hashed_expressions_resolve_stably(expr, key):
    tab = CronTab(expr, hash_key=key)
    again = CronTab(expr, hash_key=key)
    assert tab == again
    assert tab.resolved_source == again.resolved_source
    # the resolved source is plain dialect: it needs no key and means the
    # same schedule
    assert "h" not in tab.resolved_source.lower().replace("thu", "")
    assert CronTab(tab.resolved_source) == tab
    if tab.resolved_differs:
        with pytest.raises(ValueError):
            CronTab(expr)


@given(junk_text)
@example("* * * * * * * *")
@example("@reboot")
@example("60 * * * *")
@example("* * 0 * *")
@example("* * * 13 *")
@example("*/0 * * * *")
@example("5-1 * * * *")
@example("* * L-31 * *")
@example("* * 32W * *")
@example("* * * * 1#6")
@example("H * * * *")
@example("\x00 * * * *")
def test_junk_raises_only_value_error(text):
    try:
        tab = CronTab(text)
    except ValueError:
        return
    # whatever parsed must behave like a schedule
    assert CronTab(str(tab)) == tab
    tab.next(datetime.datetime(2024, 1, 1))


@given(junk_text, st.text(max_size=10))
def test_junk_with_a_hash_key_raises_only_value_error(text, key):
    try:
        CronTab(text, hash_key=key)
    except ValueError:
        pass


# --- the naive search against a brute-force scan ----------------------------


@slow
@given(cron_expressions(seconds=False), naive_datetimes())
def test_naive_next_equals_a_brute_force_scan(expr, now):
    tab = CronTab(expr)
    delay = tab.next(now)
    expected = _brute_next(tab, now)
    if delay is None:
        assert expected is None
        return
    assert delay > 0
    target = now + datetime.timedelta(seconds=delay)
    assert tab.test(target)
    assert target.second == 0 and target.microsecond == 0
    if target <= now + SCAN:
        assert expected == target
    else:
        assert expected is None


@slow
@given(cron_expressions(seconds=False), naive_datetimes())
def test_naive_prev_equals_a_brute_force_scan(expr, now):
    tab = CronTab(expr)
    age = tab.prev(now)
    expected = _brute_prev(tab, now)
    if age is None:
        assert expected is None
        return
    assert age > 0
    target = now - datetime.timedelta(seconds=age)
    assert tab.test(target)
    if target >= now - SCAN:
        assert expected == target
    else:
        assert expected is None


@slow
@given(cron_expressions(seconds=True), naive_datetimes())
def test_second_granular_next_lands_on_the_first_match(expr, now):
    tab = CronTab(expr)
    delay = tab.next(now)
    assume(delay is not None)
    target = now + datetime.timedelta(seconds=delay)
    assert delay > 0 and tab.test(target)
    # scan the (bounded) stretch before the target second by second
    cursor = max(now + SECOND, target - datetime.timedelta(minutes=30))
    while cursor < target:
        assert not tab.test(cursor), cursor
        cursor += SECOND


# --- next / prev / occurrences agree ----------------------------------------


@slow
@given(cron_expressions(), naive_datetimes())
def test_occurrences_are_iterated_next(expr, start):
    tab = CronTab(expr)
    cursor = start
    for fire in itertools.islice(tab.occurrences(start), 12):
        delay = tab.next(cursor)
        assert delay is not None and delay > 0
        assert fire == cursor + datetime.timedelta(seconds=delay)
        assert tab.test(fire)
        cursor = fire
    if cursor == start:
        assert tab.next(start) is None


@slow
@given(cron_expressions(), naive_datetimes())
def test_prev_mirrors_next(expr, now):
    tab = CronTab(expr)
    delay = tab.next(now)
    assume(delay is not None)
    fire = now + datetime.timedelta(seconds=delay)
    # standing one second either side of a fire, it is one second away
    assert tab.prev(fire + SECOND) == 1.0
    assert tab.next(fire - SECOND) == 1.0
    # a fire is never its own predecessor or successor
    assert tab.next(fire) != 0 and tab.prev(fire) != 0
    age = tab.prev(fire)
    if age is not None:
        before = fire - datetime.timedelta(seconds=age)
        assert before <= now
        assert tab.next(before) == age


@slow
@given(
    cron_expressions(),
    naive_datetimes(),
    st.integers(-14 * 60, 14 * 60),
)
def test_a_fixed_offset_behaves_like_the_naive_clock(expr, now, minutes):
    tab = CronTab(expr)
    tz = datetime.timezone(datetime.timedelta(minutes=minutes))
    aware = now.replace(tzinfo=tz)
    assert tab.next(aware) == tab.next(now)
    assert tab.prev(aware) == tab.prev(now)
    naive = list(itertools.islice(tab.occurrences(now), 5))
    fixed = list(itertools.islice(tab.occurrences(aware), 5))
    assert [fire.replace(tzinfo=None) for fire in fixed] == naive
    assert all(fire.tzinfo is tz for fire in fixed)


def test_a_schedule_that_never_fires_reports_none():
    tab = CronTab("0 0 30 2 *")
    now = datetime.datetime(2024, 1, 1)
    assert tab.next(now) is None
    assert tab.prev(now) is None
    assert list(tab.occurrences(now)) == []
    assert tab.next(now.replace(tzinfo=zone("Pacific/Apia"))) is None


# --- the aware search against an independent DST oracle ---------------------


def _oracle(tab, start, horizon):
    """Real fire instants in (start, horizon], from the documented policy.

    Every civil match is resolved in the zone with ``fold=0``: a wall time
    inside a spring-forward gap lands at its shifted instant, a repeated
    fall-back wall time on its first occurrence. One real instant fires
    once, however many civil labels resolve to it.
    """
    tz = start.tzinfo
    start_utc = start.astimezone(UTC)
    # civil labels up to 27h behind the start can still resolve after it
    # (a date-line hop), and labels up to 27h past the horizon before it
    civil = start.replace(tzinfo=None) - datetime.timedelta(hours=27)
    civil_end = horizon.astimezone(tz).replace(
        tzinfo=None
    ) + datetime.timedelta(hours=27)
    instants = set()
    for label in tab.occurrences(civil):
        if label > civil_end:
            break
        resolved = label.replace(tzinfo=tz).astimezone(UTC)
        if start_utc < resolved <= horizon:
            instants.add(resolved)
    return sorted(instants)


@slow
@given(cron_expressions(seconds=False), aware_datetimes())
def test_aware_occurrences_are_real_strictly_increasing_instants(expr, start):
    tab = CronTab(expr)
    start_utc = start.astimezone(UTC)
    previous = start_utc
    cursor = start
    for fire in itertools.islice(tab.occurrences(start), 10):
        fire_utc = fire.astimezone(UTC)
        assert fire_utc > previous, "an instant fired twice or out of order"
        assert fire.tzinfo is start.tzinfo
        # the rendered label is the one the wall clock shows at that
        # instant (never a nonexistent gap label)
        assert fire_utc.astimezone(fire.tzinfo).replace(
            tzinfo=None
        ) == fire.replace(tzinfo=None)
        # the iterator is next() iterated, in true elapsed seconds
        delay = tab.next(cursor)
        assert delay is not None
        assert (
            cursor.astimezone(UTC) + datetime.timedelta(seconds=delay)
            == fire_utc
        )
        previous = fire_utc
        cursor = fire


@slow
@given(cron_expressions(seconds=False), aware_datetimes())
def test_aware_fires_match_the_dst_policy_oracle(expr, start):
    tab = CronTab(expr)
    horizon = start.astimezone(UTC) + datetime.timedelta(hours=50)
    expected = _oracle(tab, start, horizon)
    got = []
    for fire in tab.occurrences(start):
        fire_utc = fire.astimezone(UTC)
        if fire_utc > horizon:
            break
        got.append(fire_utc)
    assert got == expected


@slow
@given(cron_expressions(seconds=False), aware_datetimes())
def test_aware_prev_mirrors_aware_next(expr, now):
    tab = CronTab(expr)
    delay = tab.next(now)
    assume(delay is not None and delay < 40 * 86400)
    fire = (
        now.astimezone(UTC) + datetime.timedelta(seconds=delay)
    ).astimezone(now.tzinfo)
    assert tab.prev(fire + SECOND) == 1.0
    age = tab.prev(fire)
    if age is not None:
        before = fire.astimezone(UTC) - datetime.timedelta(seconds=age)
        assert before <= now.astimezone(UTC)
        assert tab.next(before.astimezone(now.tzinfo)) == age


# --- the counterexamples the oracle property found, pinned ------------------


def _fires(tab, start, count):
    return [
        fire.strftime("%d %H:%M")
        for fire in itertools.islice(tab.occurrences(start), count)
    ]


def _iterated_next(tab, start, count):
    out, cursor = [], start
    for _ in range(count):
        delay = tab.next(cursor)
        cursor = (
            cursor.astimezone(UTC) + datetime.timedelta(seconds=delay)
        ).astimezone(start.tzinfo)
        out.append(cursor.strftime("%d %H:%M"))
    return out


def test_every_label_inside_a_gap_fires_at_its_shifted_time():
    # New York, 2024-03-10: 02:00-02:59 does not exist. All four labels
    # of `*/15 2` fire, each an hour late, from next() and occurrences()
    # alike (the dashboard, calendar feed and pressure map read the
    # latter; the scheduler arms from the former).
    tab = CronTab("*/15 2 * * *")
    start = datetime.datetime(
        2024, 3, 10, 1, 50, tzinfo=zone("America/New_York")
    )
    want = ["10 03:00", "10 03:15", "10 03:30", "10 03:45", "11 02:00"]
    assert _fires(tab, start, 5) == want
    assert _iterated_next(tab, start, 5) == want


def test_a_real_label_between_two_shifted_ones_fires_in_real_order():
    # Lord Howe, 2024-10-06: 02:00-02:29 does not exist (a half-hour
    # shift). `0,20,40 2` fires 02:00 at 02:30 and 02:20 at 02:50, and
    # the real 02:40 falls between them.
    tab = CronTab("0,20,40 2 * * *")
    start = datetime.datetime(
        2024, 10, 6, 1, 0, tzinfo=zone("Australia/Lord_Howe")
    )
    want = ["06 02:30", "06 02:40", "06 02:50", "07 02:00"]
    assert _fires(tab, start, 4) == want
    assert _iterated_next(tab, start, 4) == want
    # standing between the shifted fires, the real one is next
    between = datetime.datetime(
        2024, 10, 6, 2, 35, tzinfo=zone("Australia/Lord_Howe")
    )
    assert tab.next(between) == 300.0
    assert tab.prev(between) == 300.0


def test_a_now_labelled_inside_the_gap_never_gets_a_past_fire():
    # 02:10 does not exist on 2026-03-08 in New York; read with fold=0 it
    # is the instant the wall clock shows 03:10.
    tab = CronTab("*/15 * * * *")
    now = datetime.datetime(2026, 3, 8, 2, 10, tzinfo=zone("America/New_York"))
    assert tab.next(now) == 300.0
