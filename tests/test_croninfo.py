"""The shared schedule-intelligence module: previews and the linter.

``describe_cron``/``next_fires`` moved here from the TUI (their behaviour
is pinned by ``test_tui.py`` through the re-exports, which this file also
asserts); the advisory linter is new and covered rule by rule.  ``now`` is
always pinned so the DST findings land on known transition dates.
"""

import datetime
from zoneinfo import ZoneInfo

from cronstable.croninfo import (
    Finding,
    describe_cron,
    lint_schedule,
    next_fires,
)

_UTC = datetime.timezone.utc
_NY = ZoneInfo("America/New_York")
#: a fixed reference instant: July 2026, between the US transitions
_NOW = datetime.datetime(2026, 7, 18, 12, 0, tzinfo=_UTC)


def _codes(expr, tz=None):
    return [f.code for f in lint_schedule(expr, timezone=tz, now=_NOW)]


def _by_code(expr, code, tz=None):
    for finding in lint_schedule(expr, timezone=tz, now=_NOW):
        if finding.code == code:
            return finding
    raise AssertionError("no {} finding for {!r}".format(code, expr))


# ---------------------------------------------------------------------------
# module plumbing
# ---------------------------------------------------------------------------


def test_tui_still_reexports_the_moved_names():
    from cronstable import tui

    assert tui.describe_cron is describe_cron
    assert tui.next_fires is next_fires


def test_findings_are_json_shaped():
    finding = _by_code("0 0 30 2 *", "never-fires")
    assert finding._asdict() == {
        "code": "never-fires",
        "level": "warning",
        "message": finding.message,
    }
    assert isinstance(finding, Finding)


# ---------------------------------------------------------------------------
# never-fires
# ---------------------------------------------------------------------------


def test_never_fires_impossible_date():
    finding = _by_code("0 0 30 2 *", "never-fires")
    assert finding.level == "warning"
    assert "never fire" in finding.message


def test_never_fires_past_year_names_the_year():
    finding = _by_code("0 0 1 1 * 2020", "never-fires")
    assert "2020" in finding.message


def test_never_fires_suppresses_month_refinements():
    # "it never fires at all" beats "it skips February"
    assert _codes("0 0 30 2 *") == ["never-fires"]


def test_live_schedules_do_not_warn():
    assert _codes("*/15 * * * *") == []
    assert _codes("@daily") == []
    assert _codes("@reboot", tz=_NY) == []
    assert _codes("not a schedule") == []


# ---------------------------------------------------------------------------
# both day fields restricted (AND semantics)
# ---------------------------------------------------------------------------


def test_both_day_fields_restricted_warns():
    finding = _by_code("0 0 13 * 5", "day-fields-both-restricted")
    assert finding.level == "warning"
    assert "Vixie" in finding.message


def test_one_day_field_alone_is_fine():
    assert "day-fields-both-restricted" not in _codes("0 0 13 * *")
    assert "day-fields-both-restricted" not in _codes("0 0 * * 5")
    # L forms count as restrictions too
    assert "day-fields-both-restricted" in _codes("0 0 l * 5")
    assert "day-fields-both-restricted" in _codes("0 0 13 * l5")


# ---------------------------------------------------------------------------
# uneven steps
# ---------------------------------------------------------------------------


def test_uneven_minute_step_names_the_wrap_gap():
    finding = _by_code("*/7 * * * *", "uneven-step")
    assert finding.level == "warning"
    assert "4 minutes" in finding.message


def test_uneven_hour_month_dow_and_second_steps():
    assert "uneven-step" in _codes("0 */5 * * *")
    assert "uneven-step" in _codes("0 0 1 */5 *")
    assert "uneven-step" in _codes("0 0 * * */2")
    assert "uneven-step" in _codes("*/7 * * * * * *")
    # the singular gap reads grammatically
    finding = _by_code("0 0 * * */2", "uneven-step")
    assert "only 1 day" in finding.message
    assert "1 days" not in finding.message


def test_dividing_steps_are_clean():
    assert _codes("*/15 * * * *") == []
    assert _codes("0 */6 * * *") == []
    assert _codes("0 0 * * * 2030/5") == []  # year steps never flagged


def test_day_of_month_step_is_a_note():
    finding = _by_code("0 0 */2 * *", "uneven-step")
    assert finding.level == "note"
    assert "month lengths differ" in finding.message


def test_explicit_range_steps_read_as_deliberate():
    assert _codes("10-40/7 * * * *") == []


# ---------------------------------------------------------------------------
# month lengths
# ---------------------------------------------------------------------------


def test_day_31_in_a_30_day_month_warns():
    finding = _by_code("0 0 31 1,4 *", "skipped-months")
    assert finding.level == "warning"
    assert "April" in finding.message and "January" not in finding.message


def test_day_31_every_month_lists_all_short_months():
    message = _by_code("0 0 31 * *", "skipped-months").message
    for name in ("February", "April", "June", "September", "November"):
        assert name in message


def test_a_reachable_smaller_day_keeps_months_alive():
    # the 1st fires in April; only unreachable-everywhere months warn
    assert "skipped-months" not in _codes("0 0 1,31 * *")


def test_leap_day_only_note():
    finding = _by_code("0 0 29 2 *", "leap-day-only")
    assert finding.level == "note"
    assert "leap years" in finding.message
    # day 29 in ALL months: February still only matches in leap years
    assert "leap-day-only" in _codes("0 0 29 * *")


def test_last_day_covers_every_month():
    assert _codes("0 0 l 2 *") == []


# ---------------------------------------------------------------------------
# DST notes (job timezone required)
# ---------------------------------------------------------------------------


def test_dst_gap_note_names_the_transition_date():
    finding = _by_code("30 2 * * *", "dst-skipped-time", tz=_NY)
    assert finding.level == "note"
    assert "2027-03-14" in finding.message
    assert "02:30" in finding.message
    assert "America/New_York" in finding.message


def test_dst_fold_note_names_the_transition_date():
    finding = _by_code("30 1 * * *", "dst-repeated-time", tz=_NY)
    assert "2026-11-01" in finding.message
    assert "first occurrence" in finding.message


def test_dst_notes_only_with_restricted_hours_and_a_real_zone():
    # every-hour schedules fire through transitions; nothing to call out
    assert _codes("30 * * * *", tz=_NY) == []
    # no zone, or a fixed-offset zone: the scan cannot / need not run
    assert _codes("30 2 * * *") == []
    assert _codes("30 2 * * *", tz=_UTC) == []


def test_dst_notes_respect_the_day_fields():
    # 02:30 only on the 1st of the month: the 2027-03-14 gap never hits it,
    # and the 2026-11-01 fold is not a gap, so no skipped-time note appears
    codes = _codes("30 2 1 * *", tz=_NY)
    assert "dst-skipped-time" not in codes


# ---------------------------------------------------------------------------
# next_fires on the occurrences iterator
# ---------------------------------------------------------------------------


def test_next_fires_series_and_degenerate_inputs():
    start = datetime.datetime(2026, 7, 18, 11, 50, tzinfo=_UTC)
    fires = next_fires("*/15 * * * *", 3, start=start)
    assert [f.minute for f in fires] == [0, 15, 30]
    assert all(f.tzinfo is _UTC for f in fires)
    assert next_fires("@reboot", 3) == []
    assert next_fires("garbage", 3) == []
    assert next_fires("0 0 30 2 *", 3) == []


def test_next_fires_dst_gap_keeps_the_wall_clock_label():
    start = datetime.datetime(2026, 3, 7, 12, 0, tzinfo=_NY)
    fires = next_fires("30 2 * * *", 2, tz=_NY, start=start)
    assert fires[0].astimezone(_UTC) == datetime.datetime(
        2026, 3, 8, 7, 30, tzinfo=_UTC
    )
    # the label is the clock that actually exists at that instant
    assert (fires[0].hour, fires[0].minute) == (3, 30)


def test_describe_cron_question_mark_matches_star():
    assert describe_cron("0 12 ? * ?") == describe_cron("0 12 * * *")
