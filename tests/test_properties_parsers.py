"""Test parsers with generated input from users and peers.

Robustness tests check that arbitrary input raises only the parser's
expected error type. An unexpected exception during configuration loading,
crontab import, or Task Scheduler conversion would stop the operation.

Round-trip tests check that converted Task XML loads as configuration,
JSON retains its values through the storage API, and folded calendar lines
unfold unchanged. Redaction tests check that inserted secrets are removed.
"""

import datetime
import json
import re
from xml.sax.saxutils import escape as xml_escape

import pytest
from hypothesis import HealthCheck, assume, example, given, settings
from hypothesis import strategies as st

from cronstable import _json, ical, taskxml
from cronstable.config import ConfigError, parse_config_string
from cronstable.cronexpr import CronTab
from cronstable.crontabs import CrontabError, parse_crontab
from cronstable.redact import REDACTED, redact_lines, redact_secrets
from tests._strategies import cron_expressions, junk_text

slow = settings(
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)

# --- crontab import ---------------------------------------------------------

_COMMAND = st.text(
    alphabet=st.characters(
        codec="utf-8",
        categories=("L", "N", "P", "S", "Zs"),
        exclude_characters="%\\",
    ),
    min_size=1,
    max_size=60,
).filter(lambda text: text.strip() and not text.lstrip().startswith("#"))


@given(st.one_of(junk_text, st.lists(junk_text, max_size=6).map("\n".join)))
@example("* * * * *")
@example("* * * * * echo 100%")
@example("CRON_TZ=Nowhere/City\n* * * * * x")
@example("@reboot x")
@example("\x00")
def test_crontab_junk_raises_only_crontab_error(data):
    try:
        jobs = parse_crontab(data, "crontab")
    except CrontabError as err:
        assert str(err).startswith("crontab")
        return
    assert isinstance(jobs, list)
    for job in jobs:
        assert job["name"] and job["command"] is not None


@slow
@given(
    st.lists(
        st.tuples(cron_expressions(seconds=False, years=False), _COMMAND),
        min_size=1,
        max_size=5,
    )
)
def test_generated_crontabs_import_one_job_per_line(entries):
    lines = [
        "{} {}".format(" ".join(expr.split()), command)
        for expr, command in entries
    ]
    jobs = parse_crontab("\n".join(lines) + "\n", "crontab")
    assert len(jobs) == len(entries)
    assert len({job["name"] for job in jobs}) == len(jobs)
    for job, (expr, command) in zip(jobs, entries, strict=True):
        assert CronTab(job["schedule"], hash_key=job["name"]) == CronTab(expr)
        assert job["command"] == command.strip()


# --- YAML config ------------------------------------------------------------

_VALID_CONFIG = """\
defaults:
  shell: /bin/sh
  utc: true
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
    captureStdout: true
    onFailure:
      retry:
        maximumRetries: 2
        initialDelay: 1
        maximumDelay: 30
        backoffMultiplier: 2
  - name: beta
    command:
      - echo
      - beta
    schedule:
      minute: "0"
      hour: "3"
    timezone: Europe/Berlin
"""


def test_the_mutation_seed_config_is_valid():
    assert len(parse_config_string(_VALID_CONFIG, "seed.yaml").jobs) == 2


@st.composite
def _mutated_config(draw):
    text = _VALID_CONFIG
    for _ in range(draw(st.integers(1, 4))):
        at = draw(st.integers(0, len(text)))
        kind = draw(st.integers(0, 3))
        if kind == 0:
            text = text[:at] + text[at + draw(st.integers(1, 8)) :]
        elif kind == 1:
            text = text[:at] + draw(junk_text) + text[at:]
        elif kind == 2:
            lines = text.split("\n")
            row = draw(st.integers(0, len(lines) - 1))
            lines[row] = draw(st.sampled_from((" ", "  ", "\t"))) + lines[row]
            text = "\n".join(lines)
        else:
            text = (
                text[:at]
                + draw(
                    st.sampled_from(
                        (
                            ": ",
                            "- ",
                            "\n",
                            "&a ",
                            "*a ",
                            "!!python/object ",
                            "|\n",
                        )
                    )
                )
                + text[at:]
            )
    return text


@slow
@given(st.one_of(junk_text, _mutated_config()))
@example("jobs: []")
@example("jobs:\n  - name: x\n    command: y\n    schedule: '* * * * * * * *'")
@example("jobs:\n  - name: x\n    command: y\n    schedule: {minute: ''}")
@example("\ud800".encode("utf-8", "surrogatepass").decode("utf-8", "replace"))
def test_config_junk_raises_only_config_error(data):
    try:
        parse_config_string(data, "junk.yaml")
    except ConfigError:
        pass


# --- Task Scheduler XML import ----------------------------------------------

_XML_TEXT = st.text(
    alphabet=st.characters(
        codec="utf-8",
        categories=("L", "N", "P", "S", "Zs"),
    ),
    min_size=1,
    max_size=40,
)
_DAYS = "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split()
_MONTHS = (
    "January February March April May June July August September "
    "October November December".split()
)


def _flags(names, min_size=1):
    return st.lists(
        st.sampled_from(names), min_size=min_size, unique=True
    ).map(lambda picked: "".join("<{}/>".format(name) for name in picked))


_BOUNDARY = st.datetimes(
    min_value=datetime.datetime(2000, 1, 1),
    max_value=datetime.datetime(2037, 12, 31),
).map(lambda dt: dt.replace(microsecond=0).isoformat())

_INTERVAL = st.sampled_from(
    ("PT1M", "PT5M", "PT7M", "PT15M", "PT1H", "PT90M", "PT2H", "PT12H", "P1D")
)


@st.composite
def _repetition(draw):
    if draw(st.booleans()):
        return ""
    text = "<Interval>{}</Interval>".format(draw(_INTERVAL))
    if draw(st.booleans()):
        text += "<Duration>{}</Duration>".format(
            draw(st.sampled_from(("PT30M", "PT4H", "P1D", "PT10H")))
        )
    return "<Repetition>{}</Repetition>".format(text)


@st.composite
def _schedule(draw):
    kind = draw(st.integers(0, 3))
    if kind == 0:
        return (
            "<ScheduleByDay><DaysInterval>{}</DaysInterval></ScheduleByDay>"
        ).format(draw(st.integers(1, 40)))
    if kind == 1:
        return (
            "<ScheduleByWeek><WeeksInterval>{}</WeeksInterval>"
            "<DaysOfWeek>{}</DaysOfWeek></ScheduleByWeek>"
        ).format(draw(st.integers(1, 4)), draw(_flags(_DAYS)))
    if kind == 2:
        days = draw(
            st.lists(
                st.one_of(st.integers(1, 31).map(str), st.just("Last")),
                min_size=1,
                max_size=4,
                unique=True,
            )
        )
        return (
            "<ScheduleByMonth><DaysOfMonth>{}</DaysOfMonth>"
            "<Months>{}</Months></ScheduleByMonth>"
        ).format(
            "".join("<Day>{}</Day>".format(day) for day in days),
            draw(_flags(_MONTHS)),
        )
    weeks = draw(
        st.lists(
            st.sampled_from(("1", "2", "3", "4", "Last")),
            min_size=1,
            unique=True,
        )
    )
    return (
        "<ScheduleByMonthDayOfWeek><Weeks>{}</Weeks>"
        "<DaysOfWeek>{}</DaysOfWeek><Months>{}</Months>"
        "</ScheduleByMonthDayOfWeek>"
    ).format(
        "".join("<Week>{}</Week>".format(week) for week in weeks),
        draw(_flags(_DAYS)),
        draw(_flags(_MONTHS)),
    )


@st.composite
def _trigger(draw):
    common = "<StartBoundary>{}</StartBoundary>".format(draw(_BOUNDARY))
    if draw(st.booleans()):
        common += "<EndBoundary>{}</EndBoundary>".format(draw(_BOUNDARY))
    if draw(st.integers(0, 4)) == 0:
        common += "<Enabled>false</Enabled>"
    common += draw(_repetition())
    kind = draw(st.integers(0, 4))
    if kind == 0:
        return "<TimeTrigger>{}</TimeTrigger>".format(common)
    if kind == 1:
        return "<BootTrigger>{}</BootTrigger>".format(draw(_repetition()))
    if kind == 2:
        return "<LogonTrigger/>"
    return "<CalendarTrigger>{}{}</CalendarTrigger>".format(
        common, draw(_schedule())
    )


@st.composite
def _task_xml(draw):
    triggers = "".join(draw(st.lists(_trigger(), max_size=3)))
    command = xml_escape(draw(_XML_TEXT))
    arguments = xml_escape(draw(st.one_of(st.just(""), _XML_TEXT)))
    uri = xml_escape("\\" + draw(_XML_TEXT))
    settings_xml = ""
    if draw(st.booleans()):
        settings_xml += "<ExecutionTimeLimit>{}</ExecutionTimeLimit>".format(
            draw(st.sampled_from(("PT0S", "PT1H", "P3D", "PT72H")))
        )
    if draw(st.booleans()):
        settings_xml += (
            "<MultipleInstancesPolicy>{}</MultipleInstancesPolicy>"
        ).format(
            draw(
                st.sampled_from(
                    ("IgnoreNew", "Parallel", "Queue", "StopExisting")
                )
            )
        )
    return (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<Task version="1.4" xmlns="{ns}">'
        "<RegistrationInfo><URI>{uri}</URI></RegistrationInfo>"
        "<Triggers>{triggers}</Triggers>"
        "<Settings>{settings}</Settings>"
        "<Actions><Exec><Command>{command}</Command>"
        "<Arguments>{arguments}</Arguments></Exec></Actions>"
        "</Task>"
    ).format(
        ns=taskxml.TASK_NS,
        uri=uri,
        triggers=triggers,
        settings=settings_xml,
        command=command,
        arguments=arguments,
    )


@slow
@given(_task_xml())
def test_converted_task_xml_always_loads_as_config(xml):
    documents = taskxml.parse_task_documents(
        taskxml.strip_xml_declarations(xml), "t.xml"
    )
    converted = taskxml.convert_task(documents[0], "t.xml", "task-1")
    for job in converted.jobs:
        # every lowered schedule is one the engine accepts (a boot
        # trigger lowers to the loader's @reboot, which never reaches it)
        if job["schedule"] != "@reboot":
            CronTab(job["schedule"], hash_key=job["name"])
    text = taskxml.render_yaml([converted], sources=["t.xml"])
    taskxml.render_report([converted])
    if not text:
        assert converted.commented or not converted.jobs
        return
    config = parse_config_string(text, "converted.yaml")
    # the YAML scalar quoting carries every job through unchanged
    assert [job.name for job in config.jobs] == [
        job["name"] for job in converted.jobs
    ]
    # converting the same export twice gives the same bytes
    again = taskxml.convert_task(documents[0], "t.xml", "task-1")
    assert taskxml.render_yaml([again], sources=["t.xml"]) == text


@given(st.one_of(st.binary(max_size=200), junk_text.map(str.encode)))
@example(b"\xff\xfe<\x00T\x00")
@example(b"<!DOCTYPE x [<!ENTITY a 'b'>]><Task/>")
@example(b"<Task xmlns='urn:other'/>")
@example(b"")
def test_task_xml_junk_raises_only_task_xml_error(data):
    try:
        text = taskxml.strip_xml_declarations(
            taskxml.decode_task_xml(data, "junk.xml")
        )
        documents = taskxml.parse_task_documents(text, "junk.xml")
        for index, document in enumerate(documents):
            taskxml.convert_task(document, "junk.xml", "t-{}".format(index))
    except taskxml.TaskXmlError:
        pass


@given(junk_text)
def test_task_xml_scalar_helpers_raise_only_task_xml_error(text):
    for helper in (
        lambda: taskxml.duration_seconds(text, "here"),
        lambda: taskxml.parse_boundary(text, "here"),
        lambda: taskxml.windows_argv_split(text),
        lambda: taskxml.job_name(text, "fallback"),
    ):
        try:
            helper()
        except taskxml.TaskXmlError:
            pass


@given(
    st.lists(st.text(alphabet='ab \\"', min_size=1, max_size=8), max_size=5)
)
def test_windows_argv_split_inverts_list2cmdline(argv):
    import subprocess

    assume(all(arg.strip() == arg and arg for arg in argv))
    assert taskxml.windows_argv_split(subprocess.list2cmdline(argv)) == argv


# --- secret redaction -------------------------------------------------------

_SECRET = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    min_size=12,
    max_size=40,
)
_KEY = st.sampled_from(
    (
        "password",
        "PASSWORD",
        "db_password",
        "MY_SECRET",
        "api_key",
        "apikey",
        "API-KEY",
        "access_key",
        "auth_token",
        "token",
        "private_key",
        "REDISCLI_AUTH",
    )
)


@given(st.one_of(junk_text, st.text(max_size=300)))
def test_redaction_is_total_and_stable(text):
    once = redact_secrets(text)
    assert isinstance(once, str)
    # a second pass finds nothing the first one left behind
    assert redact_secrets(once) == once
    lines = text.split("\n")
    assert len(redact_lines(lines)) == len(lines)


@given(
    junk_text,
    _KEY,
    st.sampled_from(("=", ": ", " = ", ":")),
    _SECRET,
    junk_text,
)
def test_a_planted_key_value_secret_never_survives(
    before, key, sep, secret, after
):
    assume("\n" not in before and "\n" not in after)
    assume(secret not in before and secret not in after)
    line = "{} {}{}{} {}".format(before, key, sep, secret, after)
    out = redact_secrets(line)
    assert secret not in out
    assert REDACTED in out
    assert secret not in "\n".join(redact_lines([before, line, after]))


@given(
    _SECRET, _SECRET, st.sampled_from(("https", "postgres", "redis", "amqp"))
)
def test_a_planted_url_password_never_survives(user, secret, scheme):
    assume(secret != user and secret not in user)
    out = redact_secrets(
        "{}://{}:{}@db.example/x".format(scheme, user, secret)
    )
    assert secret not in out
    assert "db.example" in out


@given(
    st.lists(
        st.text(alphabet="ABCDEFabcdef0123456789+/=", max_size=70), max_size=6
    )
)
def test_a_pem_body_never_survives_line_redaction(body):
    lines = (
        ["ok before", "-----BEGIN PRIVATE KEY-----"]
        + body
        + ["-----END PRIVATE KEY-----", "ok after"]
    )
    out = redact_lines(lines)
    assert len(out) == len(lines)
    assert out[0] == "ok before" and out[-1] == "ok after"
    for original, redacted in zip(body, out[2:-2], strict=True):
        if len(original) >= 8:
            assert original not in redacted


# --- the JSON facade --------------------------------------------------------

_PORTABLE = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(-(2**63), 2**64 - 1),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(alphabet=st.characters(codec="utf-8"), max_size=20),
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(
            st.text(alphabet=st.characters(codec="utf-8"), max_size=8),
            children,
            max_size=5,
        ),
    ),
    max_leaves=25,
)


@given(_PORTABLE)
def test_portable_json_round_trips_and_matches_the_stdlib(value):
    data = _json.dumps_bytes(value)
    assert _json.loads(data) == value
    assert json.loads(data) == value
    assert _json.loads(data.decode("utf-8")) == value
    assert _json.deepcopy_json(value) == value
    _json.ensure_portable(value)


@given(st.dictionaries(st.text(max_size=6), st.integers(-5, 5), max_size=8))
def test_sorted_dumps_ignore_insertion_order(mapping):
    reverse = dict(reversed(list(mapping.items())))
    assert _json.dumps_bytes(mapping, sort_keys=True) == _json.dumps_bytes(
        reverse, sort_keys=True
    )


@given(
    _PORTABLE,
    st.one_of(
        st.just(float("nan")),
        st.just(float("inf")),
        st.just(-float("inf")),
        st.integers(min_value=2**64),
        st.integers(max_value=-(2**63) - 1),
    ),
)
def test_a_non_portable_leaf_is_refused_wherever_it_hides(value, poison):
    for holder in ([value, poison], {"a": value, "b": {"c": [poison]}}):
        with pytest.raises(_json.UnsupportedValue):
            _json.dumps_bytes(holder)
        with pytest.raises(_json.UnsupportedValue):
            _json.ensure_portable(holder)


@given(st.one_of(st.binary(max_size=60), junk_text))
def test_json_junk_raises_only_value_error(data):
    try:
        _json.loads(data)
    except ValueError:
        pass


# --- the calendar feed ------------------------------------------------------


@given(
    st.text(
        alphabet=st.characters(codec="utf-8", exclude_categories=("Cc",)),
        max_size=300,
    )
)
def test_folded_lines_fit_75_octets_and_unfold_exactly(line):
    folded = ical._fold(line)
    physical = folded.split("\r\n")
    for index, part in enumerate(physical):
        assert len(part.encode("utf-8")) <= 75
        if index:
            assert part.startswith(" ")
    assert physical[0] + "".join(part[1:] for part in physical[1:]) == line


@given(st.text(max_size=80))
def test_escaped_text_carries_no_raw_line_break_and_unescapes(text):
    escaped = ical._escape(text)
    assert "\n" not in escaped and "\r" not in escaped
    restored = re.sub(
        r"\\(.)", lambda m: "\n" if m.group(1) == "n" else m.group(1), escaped
    )
    assert restored == text.replace("\r\n", "\n").replace("\r", "\n")


@slow
@given(
    st.lists(
        st.tuples(
            st.text(min_size=1, max_size=40),
            cron_expressions(seconds=False, years=False),
        ),
        max_size=4,
    ),
    st.integers(1, 5),
)
def test_a_rendered_calendar_is_well_formed_for_any_job_names(jobs, days):
    entries = [
        ical.CalendarEntry._make(_entry_fields(name, CronTab(expr)))
        for name, expr in jobs
    ]
    start = datetime.datetime(2026, 3, 7, tzinfo=datetime.timezone.utc)
    text = ical.render_calendar(
        entries, start, days, per_job_cap=20, now=start
    )
    assert text.endswith("\r\n")
    lines = text[:-2].split("\r\n")
    assert lines[0] == "BEGIN:VCALENDAR" and lines[-1] == "END:VCALENDAR"
    assert lines.count("BEGIN:VEVENT") == lines.count("END:VEVENT")
    for line in lines:
        assert "\n" not in line and "\r" not in line
        assert len(line.encode("utf-8")) <= 75


def _entry_fields(name, tab):
    values = {
        "name": name,
        "tab": tab,
        "crontab": tab,
        "schedule": str(tab),
        "timezone": datetime.timezone.utc,
        "tz": datetime.timezone.utc,
        "avg_duration": None,
        "enabled": True,
        "paused": False,
        "command": "true",
        "description": name,
    }
    missing = [f for f in ical.CalendarEntry._fields if f not in values]
    assert not missing, missing
    return [values[field] for field in ical.CalendarEntry._fields]
