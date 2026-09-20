"""Share Hypothesis strategies for ``tests/test_properties_*.py``.

These helpers generate values without fixtures; see ``tests/conftest.py``.
Cron strategies build valid expressions from the supported grammar.
``junk_text`` generates arbitrary input to check that parsers raise only
their documented error types.
"""

import datetime
import zoneinfo
from functools import lru_cache

from hypothesis import strategies as st

MONTH_NAMES = "jan feb mar apr may jun jul aug sep oct nov dec".split()
DOW_NAMES = "sun mon tue wed thu fri sat".split()

#: Zones picked for the shape of their transitions, beyond the northern
#: one-hour zones the hand-written tests use: southern-hemisphere DST, a
#: 30-minute shift (Lord Howe), gaps that swallow midnight (Santiago,
#: Beirut, Cairo, Havana, Sao Paulo), a skipped calendar day (Apia,
#: Kwajalein), 45- and 30-minute base offsets (Kathmandu, Chatham,
#: St Johns, Kolkata), a two-hour shift (Troll), rule changes without
#: DST (Caracas, Pyongyang), and DST rules that come and go (Casablanca,
#: Windhoek, Fiji).
ZONES = (
    "UTC",
    "America/New_York",
    "America/Los_Angeles",
    "America/St_Johns",
    "America/Santiago",
    "America/Sao_Paulo",
    "America/Havana",
    "America/Caracas",
    "America/Asuncion",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Dublin",
    "Europe/Moscow",
    "Africa/Cairo",
    "Africa/Casablanca",
    "Africa/Windhoek",
    "Asia/Beirut",
    "Asia/Tehran",
    "Asia/Kathmandu",
    "Asia/Kolkata",
    "Asia/Pyongyang",
    "Asia/Gaza",
    "Australia/Lord_Howe",
    "Australia/Sydney",
    "Australia/Adelaide",
    "Pacific/Apia",
    "Pacific/Auckland",
    "Pacific/Chatham",
    "Pacific/Fiji",
    "Pacific/Kwajalein",
    "Antarctica/Troll",
)


def _values(low, high, names=None):
    numbers = st.integers(low, high).map(str)
    if names is None:
        return numbers
    # names are case-insensitive in the dialect, so mix the cases
    named = st.sampled_from(names).flatmap(
        lambda name: st.sampled_from((name, name.upper(), name.capitalize()))
    )
    return st.one_of(numbers, named)


def _step_from(start, high):
    # the dialect's step rule: positive, and ``start + step`` lands back
    # inside the field (so ``*/12`` in the month field is an error)
    return st.integers(1, max(1, high - start))


@st.composite
def _range_item(draw, low, high):
    first = draw(st.integers(low, high))
    last = draw(st.integers(first, high))
    text = "{}-{}".format(first, last)
    if first < high and draw(st.booleans()):
        text += "/{}".format(draw(_step_from(first, high)))
    return text


@st.composite
def _open_step_item(draw, low, high, step_end):
    # ``a/n``: from a to the field's end
    first = draw(st.integers(low, min(step_end, high - 1)))
    return "{}/{}".format(first, draw(_step_from(first, high)))


def _plain_item(low, high, names=None, step_end=None):
    step_end = high if step_end is None else step_end
    return st.one_of(
        _values(low, high, names),
        _values(low, high, names),
        _range_item(low, high),
        _step_from(low, high).map("*/{}".format),
        _open_step_item(low, high, step_end),
    )


def _field(item, star_weight=2):
    listed = st.lists(item, min_size=1, max_size=3).map(",".join)
    return st.one_of(*([st.just("*")] * star_weight), listed)


_DOM_ITEM = st.one_of(
    _plain_item(1, 31),
    _plain_item(1, 31),
    _plain_item(1, 28),
    st.just("L"),
    st.integers(1, 30).map("L-{}".format),
    st.integers(1, 31).map("{}W".format),
    st.just("LW"),
)

_DOW_ITEM = st.one_of(
    _plain_item(0, 7, DOW_NAMES, step_end=6),
    _plain_item(0, 6, DOW_NAMES),
    st.tuples(st.integers(0, 7), st.integers(1, 5)).map(
        lambda pair: "{}#{}".format(*pair)
    ),
    st.integers(0, 7).map("L{}".format),
)


@st.composite
def _hash_item(draw, low, high, rangeless_end=None):
    end = high if rangeless_end is None else rangeless_end
    shape = draw(st.integers(0, 2))
    if shape == 0:
        return "H"
    if shape == 1:
        return "H/{}".format(draw(st.integers(1, end - low + 1)))
    first = draw(st.integers(low, high))
    last = draw(st.integers(first, high))
    text = "H({}-{})".format(first, last)
    if draw(st.booleans()):
        text += "/{}".format(draw(st.integers(1, last - first + 1)))
    return text


@st.composite
def cron_expressions(draw, *, seconds=None, years=None, hashed=False):
    """Generate a valid expression in the cronstable dialect.

    ``seconds`` and ``years`` control the second and year fields. ``True``
    requires the field, ``False`` excludes it, and ``None`` selects randomly.
    ``hashed`` permits Jenkins-style ``H`` items, which require a ``hash_key``.
    """

    def column(low, high, item):
        if hashed and draw(st.integers(0, 3)) == 0:
            return draw(_hash_item(low, high))
        return draw(_field(item))

    minute = column(0, 59, _plain_item(0, 59))
    hour = column(0, 23, _plain_item(0, 23))
    dom = draw(st.one_of(st.just("?"), st.just("*"), _field(_DOM_ITEM, 3)))
    if hashed and draw(st.integers(0, 5)) == 0:
        dom = draw(_hash_item(1, 31, rangeless_end=28))
    month = column(1, 12, _plain_item(1, 12, MONTH_NAMES))
    dow = draw(st.one_of(st.just("?"), st.just("*"), _field(_DOW_ITEM, 3)))
    if dom == "?" and dow == "?":
        dow = "*"
    fields = [minute, hour, dom, month, dow]
    with_seconds = draw(st.booleans()) if seconds is None else seconds
    with_years = draw(st.booleans()) if years is None else years
    if with_seconds or with_years:
        year_item = _plain_item(1970, 2099)
        fields.append(draw(_field(year_item)) if with_years else "*")
    if with_seconds:
        fields.insert(0, column(0, 59, _plain_item(0, 59)))
    gap = draw(st.sampled_from((" ", "  ", "\t")))
    return gap.join(fields)


def naive_datetimes(min_year=1971, max_year=2098):
    return st.datetimes(
        min_value=datetime.datetime(min_year, 1, 1),
        max_value=datetime.datetime(max_year, 12, 31, 23, 59, 59),
    ).map(lambda dt: dt.replace(microsecond=0))


@lru_cache(maxsize=None)
def zone(name):
    return zoneinfo.ZoneInfo(name)


@lru_cache(maxsize=None)
def transitions(name, year):
    """UTC instants (hour resolution, refined to the second) at which the
    zone's offset changes during ``year``."""
    tz = zone(name)
    utc = datetime.timezone.utc
    found = []
    cursor = datetime.datetime(year, 1, 1, tzinfo=utc)
    end = datetime.datetime(year + 1, 1, 1, tzinfo=utc)
    hour = datetime.timedelta(hours=1)
    offset = cursor.astimezone(tz).utcoffset()
    while cursor < end:
        following = cursor + hour
        after = following.astimezone(tz).utcoffset()
        if after != offset:
            low, high = cursor, following
            while high - low > datetime.timedelta(seconds=1):
                mid = low + (high - low) / 2
                mid = mid.replace(microsecond=0)
                if mid <= low:
                    break
                if mid.astimezone(tz).utcoffset() == offset:
                    low = mid
                else:
                    high = mid
            found.append(high)
            offset = after
        cursor = following
    return tuple(found)


@st.composite
def aware_datetimes(draw, near_transition=None):
    """Generate an aware datetime in one of ``ZONES``.

    Most values fall within 36 hours of an offset change to exercise scheduling
    across transitions. The remaining values can fall anywhere in the range.
    """
    name = draw(st.sampled_from(ZONES))
    tz = zone(name)
    year = draw(st.integers(1995, 2037))
    hops = transitions(name, year)
    close = (
        draw(st.integers(0, 4)) > 0
        if near_transition is None
        else near_transition
    )
    if close and hops:
        edge = draw(st.sampled_from(hops))
        skew = draw(st.integers(-36 * 3600, 36 * 3600))
        instant = edge + datetime.timedelta(seconds=skew)
    else:
        instant = draw(naive_datetimes(1995, 2037)).replace(
            tzinfo=datetime.timezone.utc
        )
    return instant.astimezone(tz)


#: Text a user or a hostile peer could feed a parser: printable noise,
#: cron-ish punctuation, control characters, surrogates excluded (they
#: cannot arrive through a UTF-8 decode).
junk_text = st.one_of(
    st.text(max_size=80),
    st.text(
        alphabet="*/,-?LWH#()0123456789 \t@abcdefijmnorstuvwy", max_size=60
    ),
    st.text(
        alphabet=st.characters(
            codec="utf-8", categories=("Cc", "Cf", "Zs", "Nd", "Po")
        ),
        max_size=40,
    ),
)
