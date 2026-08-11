"""The Task Scheduler XML importer.

Two things carry most of the weight here. First, every document the emitter
produces is fed back through the real config parser, because the emitter is
hand-rolled YAML and that is what makes hand-rolling safe: a quoting mistake
is a test failure rather than a field report. Second, the export shapes are
built from the real thing, including the two that trip an operator up before
any conversion happens (one XML declaration per task inside one root, and a
declaration that lies about the encoding).

Nothing here needs Windows: the converter is deliberately OS-independent so
an estate can be converted on any machine.
"""

import os
import re
import types

import pytest

from cronstable import taskxml
from cronstable.config import parse_config_string
from cronstable.taskxml import (
    TaskXmlError,
    convert_task,
    decode_task_xml,
    duration_seconds,
    job_name,
    parse_boundary,
    parse_task_documents,
    render_yaml,
    repetition_fields,
    strip_xml_declarations,
    windows_argv_split,
)

NS = taskxml.TASK_NS


def _task(
    *,
    uri="\\Contoso\\Nightly Backup",
    triggers="",
    actions=None,
    settings="",
):
    if actions is None:
        actions = (
            "<Exec><Command>C:\\scripts\\backup.exe</Command>"
            "<Arguments>--incremental</Arguments></Exec>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<Task version="1.4" xmlns="{ns}">'
        "<RegistrationInfo><URI>{uri}</URI></RegistrationInfo>"
        "<Triggers>{triggers}</Triggers>"
        "<Settings>{settings}</Settings>"
        "<Actions>{actions}</Actions>"
        "</Task>"
    ).format(
        ns=NS, uri=uri, triggers=triggers, settings=settings, actions=actions
    )


def _convert(xml, fallback="task-1"):
    documents = parse_task_documents(strip_xml_declarations(xml), "t.xml")
    return convert_task(documents[0], "t.xml", fallback)


def _schedules(converted):
    return [job["schedule"] for job in converted.jobs]


def _reasons(converted):
    return " ".join(note.reason for note in converted.notes)


# ---------------------------------------------------------------------------
# Reading real export shapes
# ---------------------------------------------------------------------------


def test_decode_prefers_the_bom_over_the_declaration():
    # Export-ScheduledTask stamps UTF-16 and PowerShell writes UTF-8, so the
    # declaration is routinely a lie; the bytes are the truth.
    text = _task()
    assert decode_task_xml(text.encode("utf-8"), "t.xml").endswith("</Task>")
    assert decode_task_xml(text.encode("utf-16"), "t.xml").endswith("</Task>")


def test_decode_refuses_bytes_that_are_neither():
    with pytest.raises(TaskXmlError, match="could not be decoded"):
        decode_task_xml(b"\xc3\x28\xa0\xa1" * 3, "t.xml")


def test_a_mislabelled_export_still_parses():
    # the exact failure an operator hits: UTF-8 bytes stamped UTF-16. Parsing
    # the bytes fails; going through decode first is what makes it work.
    data = _task().encode("utf-8")
    text = decode_task_xml(data, "t.xml")
    assert parse_task_documents(strip_xml_declarations(text), "t.xml")


def test_strip_declarations_reads_the_multi_task_export():
    # `schtasks /query /XML` without ONE emits one declaration per task
    # INSIDE one <Tasks> root, which no parser accepts as it stands.
    body = "".join(_task(uri="\\T{}".format(i)) for i in range(3))
    export = "<Tasks>" + body + "</Tasks>"
    with pytest.raises(TaskXmlError, match="not well-formed"):
        parse_task_documents(export, "t.xml")
    assert (
        len(parse_task_documents(strip_xml_declarations(export), "t.xml")) == 3
    )


def test_container_root_carries_no_namespace():
    # measured on a real export: <Tasks> is unnamespaced while every <Task>
    # under it is namespaced, so a namespaced findall on the root finds none.
    export = "<Tasks>" + _task() + "</Tasks>"
    assert len(parse_task_documents(strip_xml_declarations(export), "x")) == 1


def test_a_doctype_is_refused():
    # the whole entity-expansion class, removed by refusing the thing every
    # custom entity must be declared inside.
    doc = '<!DOCTYPE Task [<!ENTITY a "hi">]><Task xmlns="{}"></Task>'.format(
        NS
    )
    with pytest.raises(TaskXmlError, match="DOCTYPE"):
        parse_task_documents(doc, "t.xml")


def test_xml_that_is_not_a_task_export_names_its_root():
    with pytest.raises(TaskXmlError, match="not a Task Scheduler export"):
        parse_task_documents("<configuration><x/></configuration>", "app.xml")


def test_an_oversized_document_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(taskxml, "MAX_DOCUMENT_BYTES", 16)
    path = tmp_path / "big.xml"
    path.write_text("x" * 200, encoding="utf-8")
    with pytest.raises(TaskXmlError, match="larger than"):
        taskxml.read_source(str(path))


# ---------------------------------------------------------------------------
# Small parsers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, seconds",
    [
        ("PT0S", 0.0),
        ("PT5M", 300.0),
        ("PT1H", 3600.0),
        ("P1D", 86400.0),
        ("P1DT2H47M", 96420.0),
        ("PT12H5M", 43500.0),
        ("P1W", 604800.0),
    ],
)
def test_duration_seconds(text, seconds):
    assert duration_seconds(text, "where") == seconds


@pytest.mark.parametrize("text", ["P1M", "P1Y", "", "5 minutes"])
def test_duration_refuses_what_has_no_fixed_length(text):
    # a month before T is months, not minutes, and neither months nor years
    # convert to a number of seconds.
    with pytest.raises(TaskXmlError):
        duration_seconds(text, "where")


@pytest.mark.parametrize(
    "text, zone",
    [
        ("2026-08-10T03:30:00", ""),
        ("2026-08-10T03:30:00Z", "Z"),
        ("2026-08-10T03:30:00-04:00", "-04:00"),
        ("2026-08-10T03:30:00.123456", ""),
    ],
)
def test_parse_boundary(text, zone):
    # fromisoformat is not used: it does not take a trailing Z on the oldest
    # interpreter this project supports, which is a live matrix row.
    assert parse_boundary(text, "w").zone == zone
    assert parse_boundary(text, "w").when.hour == 3


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("\\Contoso\\Nightly Backup", "Contoso.Nightly-Backup"),
        (
            "\\Microsoft\\Windows\\UPnP\\UPnPHostConfig",
            "Microsoft.Windows.UPnP.UPnPHostConfig",
        ),
        ("\\a/b", "a-b"),
        ("\\job%20name", "job-20name"),
        ("\\  ", "fallback"),
        ("", "fallback"),
    ],
)
def test_job_name(uri, expected):
    assert job_name(uri, "fallback") == expected


def test_job_name_keeps_the_characters_that_load():
    # braces and colons load, survive the durable store's filename mapping,
    # and classic crontab jobs already carry a colon.
    name = job_name("\\NVIDIA App SelfUpdate_{B2FE-46C3}", "x")
    assert name == "NVIDIA-App-SelfUpdate_{B2FE-46C3}"
    parse_config_string(
        "jobs:\n  - name: '{}'\n    command: x\n"
        '    schedule: "* * * * *"\n'.format(name),
        "",
    )


@pytest.mark.parametrize(
    "text, argv",
    [
        ("--incremental", ["--incremental"]),
        ('-p "C:\\Program Files\\x"', ["-p", "C:\\Program Files\\x"]),
        ('a "b c" d', ["a", "b c", "d"]),
        (r'"C:\dir\\" next', ["C:\\dir\\", "next"]),
        ("", []),
    ],
)
def test_windows_argv_split(text, argv):
    # CommandLineToArgvW rules, not shlex(posix=False), which has a different
    # backslash grammar and every Windows path is full of backslashes.
    assert windows_argv_split(text) == argv


# ---------------------------------------------------------------------------
# Repetition
# ---------------------------------------------------------------------------


def _boundary(text="2026-08-10T00:00:00"):
    return parse_boundary(text, "w").when


@pytest.mark.parametrize(
    "start, interval, expected",
    [
        ("2026-08-10T00:00:00", 3600.0, ("0", "*")),
        ("2026-08-10T00:12:00", 10800.0, ("12", "0,3,6,9,12,15,18,21")),
        ("2026-08-10T00:00:00", 900.0, ("0,15,30,45", "*")),
        ("2026-08-10T02:30:00", 86400.0, ("30", "2")),
    ],
)
def test_repetition_that_tiles_the_day(start, interval, expected):
    assert repetition_fields(_boundary(start), interval, None) == expected


def test_repetition_that_divides_the_day_but_is_not_a_cross_product():
    # PT90M divides 1440, but its occurrences are 00:00, 01:30, 03:00 ...
    # whose minute-by-hour cross product would also contain 00:30. Widening
    # it would double the firing rate, so it is refused instead.
    assert repetition_fields(_boundary(), 5400.0, None) is None


@pytest.mark.parametrize("interval", [25200.0, 11220.0, 96420.0])
def test_repetition_that_does_not_divide_the_day(interval):
    assert repetition_fields(_boundary(), interval, None) is None


def test_repetition_with_a_bounded_duration_is_refused():
    # a window inside a day is not a cross product either
    assert repetition_fields(_boundary(), 3600.0, 14400.0) is None


def test_repetition_with_a_nonzero_second_is_refused():
    assert (
        repetition_fields(_boundary("2026-08-10T00:00:38"), 3600.0, None)
        is None
    )


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


def test_time_trigger_becomes_a_one_shot_and_says_so():
    converted = _convert(
        _task(
            triggers="<TimeTrigger><StartBoundary>2026-08-10T03:30:00"
            "</StartBoundary></TimeTrigger>"
        )
    )
    assert _schedules(converted) == ["30 3 10 8 * 2026"]
    assert "runs once" in _reasons(converted)


def test_time_trigger_with_repetition_clears_the_date_columns():
    # keeping them turns an hourly repetition on a 2010 trigger into a job
    # that can never fire.
    converted = _convert(
        _task(
            triggers="<TimeTrigger><StartBoundary>2010-10-14T20:00:00"
            "</StartBoundary><Repetition><Interval>PT1H</Interval>"
            "</Repetition></TimeTrigger>"
        )
    )
    assert _schedules(converted) == ["0 * * * *"]
    assert "phase" in _reasons(converted)


def test_boot_trigger_becomes_reboot():
    converted = _convert(_task(triggers="<BootTrigger/>"))
    assert _schedules(converted) == ["@reboot"]


@pytest.mark.parametrize(
    "element",
    [
        "LogonTrigger",
        "IdleTrigger",
        "EventTrigger",
        "SessionStateChangeTrigger",
        "RegistrationTrigger",
        "WnfStateChangeTrigger",
    ],
)
def test_unconvertible_triggers_are_reported_not_dropped(element):
    converted = _convert(_task(triggers="<{0}/>".format(element)))
    assert converted.jobs == []
    assert any(note.element == element for note in converted.notes)
    assert any(note.blocking for note in converted.notes)


def test_a_task_with_no_trigger_is_reported_and_commented():
    # 57 of 195 tasks on a real box; in none of the documented lists.
    converted = _convert(_task(triggers=""))
    assert converted.commented
    assert "no trigger" in _reasons(converted)


def test_calendar_daily():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByDay><DaysInterval>1</DaysInterval>"
            "</ScheduleByDay></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 * * *"]


def test_calendar_every_seven_days_is_exact():
    # seven divides the week, so it is the same weekday forever
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByDay><DaysInterval>7</DaysInterval>"
            "</ScheduleByDay></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 * * mon"]


@pytest.mark.parametrize("interval", ["3", "8", "14", "30"])
def test_calendar_other_day_intervals_are_refused_with_the_reason(interval):
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByDay><DaysInterval>{}</DaysInterval>"
            "</ScheduleByDay></CalendarTrigger>".format(interval)
        )
    )
    assert converted.jobs == []
    assert "restarts each month" in _reasons(converted)


def test_calendar_weekly():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByWeek><WeeksInterval>1"
            "</WeeksInterval><DaysOfWeek><Monday/><Friday/></DaysOfWeek>"
            "</ScheduleByWeek></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 * * mon,fri"]


def test_calendar_monthly_with_last_day():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByMonth><DaysOfMonth><Day>1</Day>"
            "<Day>Last</Day></DaysOfMonth><Months><January/><July/>"
            "</Months></ScheduleByMonth></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 1,L 1,7 *"]


def test_calendar_nth_weekday_of_the_month():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByMonthDayOfWeek><Weeks><Week>2</Week>"
            "</Weeks><DaysOfWeek><Tuesday/></DaysOfWeek>"
            "</ScheduleByMonthDayOfWeek></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 * * tue#2"]


def test_calendar_last_weekday_of_the_month():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00"
            "</StartBoundary><ScheduleByMonthDayOfWeek><Weeks><Week>Last"
            "</Week></Weeks><DaysOfWeek><Tuesday/></DaysOfWeek>"
            "</ScheduleByMonthDayOfWeek></CalendarTrigger>"
        )
    )
    assert _schedules(converted) == ["30 2 * * L2"]


def test_a_disabled_trigger_disables_its_job():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><Enabled>false</Enabled>"
            "<StartBoundary>2026-08-10T02:30:00</StartBoundary>"
            "<ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>"
            "</CalendarTrigger>"
        )
    )
    assert converted.jobs[0]["enabled"] is False


def test_two_triggers_become_two_jobs_with_distinct_names():
    converted = _convert(
        _task(
            triggers="<BootTrigger/><CalendarTrigger><StartBoundary>"
            "2026-08-10T02:30:00</StartBoundary><ScheduleByDay/>"
            "</CalendarTrigger>"
        )
    )
    names = [job["name"] for job in converted.jobs]
    assert names == [
        "Contoso.Nightly-Backup",
        "Contoso.Nightly-Backup-t2",
    ]


# ---------------------------------------------------------------------------
# Actions and settings
# ---------------------------------------------------------------------------


_DAILY = (
    "<CalendarTrigger><StartBoundary>2026-08-10T02:30:00</StartBoundary>"
    "<ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>"
    "</CalendarTrigger>"
)


def test_a_percent_variable_makes_the_command_one_string():
    # Task Scheduler expands %VAR% before CreateProcess and cronstable
    # expands nothing, so an argv whose argv[0] is '%windir%\\...' reaches
    # create_subprocess_exec verbatim and fails with WinError 2 on every
    # run. A string command with no shell: goes to create_subprocess_shell,
    # which is %ComSpec% /c on Windows, so cmd.exe does the expansion.
    # Measured on this project's own dev box: 18 of the 31 jobs a
    # whole-machine export converted had one in argv[0].
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions=(
                "<Exec><Command>%windir%\\system32\\rundll32.exe</Command>"
                "<Arguments>acproxy.dll,Run</Arguments></Exec>"
            ),
        )
    )
    command = converted.jobs[0]["command"]
    assert isinstance(command, str)
    # the program is quoted even though the literal text has no space in
    # it: Task Scheduler decides quoting on what it stored and calls
    # CreateProcess, so a variable that expands to a spaced path is stored
    # bare, and cmd.exe handed that bare runs the wrong program.
    assert command == r'"%windir%\system32\rundll32.exe" acproxy.dll,Run'
    assert "%VAR% environment variables" in _reasons(converted)
    # advisory, not blocking: the job works, it just runs under a shell
    assert not any(note.blocking for note in converted.notes)
    # and it survives the emitter and the real config parser
    conf = parse_config_string(render_yaml([converted], sources=["t.xml"]), "")
    assert conf.jobs[0].command == command
    # no `shell:` key is emitted, which is what routes a string command
    # through %ComSpec% /c on Windows. Asserted on the emitted job rather
    # than on the parsed config, whose `shell` defaults to the platform's
    # own ("" on Windows, /bin/sh on POSIX) and would make this pass or
    # fail depending on where the suite runs.
    assert "shell" not in converted.jobs[0]


def test_a_quoted_percent_command_keeps_its_quotes_for_the_shell():
    # The argv path strips the quote pair around a spaced path because a
    # quote is a literal character in an argv. The string path must NOT:
    # Task Scheduler quotes it precisely because Command and Arguments are
    # concatenated into one command line, so the quotes are what make the
    # concatenation parse.
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions=(
                '<Exec><Command>"%ProgramFiles(x86)%\\App\\a b.exe"</Command>'
                "<Arguments>--now</Arguments></Exec>"
            ),
        )
    )
    assert converted.jobs[0]["command"] == (
        '"%ProgramFiles(x86)%\\App\\a b.exe" --now'
    )


def test_no_percent_variable_still_becomes_an_argv_list():
    # The bias check: the string form is for command lines that need a
    # shell, and everything else keeps the argv it always had.
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions=(
                '<Exec><Command>"C:\\Program Files\\a b.exe"</Command>'
                "<Arguments>--now</Arguments></Exec>"
            ),
        )
    )
    assert converted.jobs[0]["command"] == [
        "C:\\Program Files\\a b.exe",
        "--now",
    ]


def test_exec_becomes_an_argv_list_and_a_working_directory():
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions="<Exec><Command>C:\\s\\b.exe</Command>"
            "<Arguments>--x 1</Arguments>"
            "<WorkingDirectory>C:\\s</WorkingDirectory></Exec>",
        )
    )
    job = converted.jobs[0]
    assert job["command"] == ["C:\\s\\b.exe", "--x", "1"]
    assert job["workingDirectory"] == "C:\\s"


def test_a_quoted_command_loses_its_quotes():
    # Task Scheduler stores a spaced path quoted because Command and
    # Arguments are concatenated; in an argv a quote is part of the name.
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions='<Exec><Command>"C:\\Program Files\\a.exe"</Command>'
            "</Exec>",
        )
    )
    assert converted.jobs[0]["command"] == ["C:\\Program Files\\a.exe"]


def test_a_com_handler_action_is_reported_not_dropped():
    # the majority action type on a real box: 111 of 195 tasks.
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions="<ComHandler><ClassId>{x}</ClassId></ComHandler>",
        )
    )
    assert converted.jobs == []
    assert "ComHandler" in " ".join(n.element for n in converted.notes)


def test_two_exec_actions_are_emitted_commented_out():
    # Task Scheduler runs a task's actions in sequence inside one instance;
    # two cronstable jobs on one schedule race, so this must not go live.
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions="<Exec><Command>a.exe</Command></Exec>"
            "<Exec><Command>b.exe</Command></Exec>",
        )
    )
    assert len(converted.jobs) == 2
    assert converted.commented
    assert "in sequence" in _reasons(converted)


def test_execution_time_limit_of_zero_emits_nothing():
    # PT0S means no limit in Task Scheduler, while cronstable refuses an
    # executionTimeout that is not > 0. Measured, 34 of 195 tasks say PT0S,
    # so the literal mapping makes 17% of an estate fail to load.
    converted = _convert(
        _task(
            triggers=_DAILY,
            settings="<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
        )
    )
    assert "executionTimeout" not in converted.jobs[0]


def test_execution_time_limit_converts_to_seconds():
    converted = _convert(
        _task(
            triggers=_DAILY,
            settings="<ExecutionTimeLimit>PT1H</ExecutionTimeLimit>",
        )
    )
    assert converted.jobs[0]["executionTimeout"] == 3600.0


@pytest.mark.parametrize(
    "policy, expected",
    [
        ("IgnoreNew", "Forbid"),
        ("Parallel", "Allow"),
        ("StopExisting", "Replace"),
        ("Queue", "Forbid"),
        (None, "Forbid"),
    ],
)
def test_multiple_instances_policy(policy, expected):
    settings = (
        "<MultipleInstancesPolicy>{}</MultipleInstancesPolicy>".format(policy)
        if policy
        else ""
    )
    converted = _convert(_task(triggers=_DAILY, settings=settings))
    assert converted.jobs[0]["concurrencyPolicy"] == expected


def test_queue_says_what_it_could_not_do():
    converted = _convert(
        _task(
            triggers=_DAILY,
            settings="<MultipleInstancesPolicy>Queue"
            "</MultipleInstancesPolicy>",
        )
    )
    assert "queued behind" in _reasons(converted)


@pytest.mark.parametrize(
    "priority, level",
    [
        ("1", "high"),
        ("2", "above-normal"),
        ("7", "below-normal"),
        ("10", "idle"),
        ("0", "high"),
    ],
)
def test_priority_mapping(priority, level):
    converted = _convert(
        _task(
            triggers=_DAILY,
            settings="<Priority>{}</Priority>".format(priority),
        )
    )
    assert converted.jobs[0]["priority"] == level


def test_priority_in_the_middle_of_the_range_emits_nothing():
    converted = _convert(
        _task(triggers=_DAILY, settings="<Priority>5</Priority>")
    )
    assert "priority" not in converted.jobs[0]


def test_an_absent_priority_emits_nothing():
    # normal is the level cronstable never applies, so synthesising one onto
    # 163 of 195 tasks would change every spawn on the box.
    converted = _convert(_task(triggers=_DAILY))
    assert "priority" not in converted.jobs[0]


def test_a_disabled_task_disables_its_jobs():
    converted = _convert(
        _task(triggers=_DAILY, settings="<Enabled>false</Enabled>")
    )
    assert converted.jobs[0]["enabled"] is False


def test_a_principal_is_reported_and_never_emitted_as_user():
    # user: and group: are a fatal config-load error on Windows, so emitting
    # one would produce a file that cannot load on the platform it targets.
    xml = _task(triggers=_DAILY).replace(
        "<Settings>",
        "<Principals><Principal><UserId>SYSTEM</UserId>"
        "<RunLevel>HighestAvailable</RunLevel></Principal></Principals>"
        "<Settings>",
    )
    converted = _convert(xml)
    assert "user" not in converted.jobs[0]
    assert "group" not in converted.jobs[0]
    assert "SYSTEM" in _reasons(converted)


# ---------------------------------------------------------------------------
# The emitter, and the invariant that keeps it loadable
# ---------------------------------------------------------------------------


def _emit(*xmls):
    tasks = [_convert(xml, "t-{}".format(i)) for i, xml in enumerate(xmls)]
    return render_yaml(tasks, sources=["tasks.xml"])


def test_emitted_yaml_loads():
    document = _emit(_task(triggers=_DAILY))
    conf = parse_config_string(document, "")
    assert [job.name for job in conf.jobs] == ["Contoso.Nightly-Backup"]


def test_emitted_yaml_has_exactly_one_jobs_key():
    # two jobs: keys are a duplicate-key error, which is what a per-task
    # block would produce the moment an export held two tasks.
    document = _emit(
        _task(uri="\\A", triggers=_DAILY),
        _task(uri="\\B", triggers=_DAILY),
    )
    assert document.count("\njobs:") == 1
    assert len(parse_config_string(document, "").jobs) == 2


def test_nothing_convertible_emits_nothing_at_all():
    # a document of only comments is a parse error, and an unloadable file
    # in a config directory fails the load of every other file there.
    assert _emit(_task(triggers="<LogonTrigger/>")) == ""


def test_a_commented_task_never_makes_the_file_unloadable():
    document = _emit(
        _task(uri="\\Live", triggers=_DAILY),
        _task(
            uri="\\Twin",
            triggers=_DAILY,
            actions="<Exec><Command>a.exe</Command></Exec>"
            "<Exec><Command>b.exe</Command></Exec>",
        ),
    )
    conf = parse_config_string(document, "")
    assert [job.name for job in conf.jobs] == ["Live"]


def test_uncommenting_a_block_restores_a_valid_job():
    # the commented form is the live form with "# " after the list indent,
    # so deleting that prefix is the whole edit an operator makes.
    document = _emit(
        _task(uri="\\Live", triggers=_DAILY),
        _task(
            uri="\\Twin",
            triggers=_DAILY,
            actions="<Exec><Command>a.exe</Command></Exec>"
            "<Exec><Command>b.exe</Command></Exec>",
        ),
    )
    # what an operator actually does: delete the "# " from the job lines,
    # leaving the label and marker comments where they are.
    restored = "\n".join(
        line.replace("  # ", "  ", 1)
        if re.match(r"^  # (- |\s)", line)
        else line
        for line in document.splitlines()
    )
    assert len(parse_config_string(restored, "").jobs) == 3


def test_windows_paths_and_dollars_survive_the_round_trip():
    converted = _convert(
        _task(
            triggers=_DAILY,
            actions="<Exec><Command>C:\\a b\\c.exe</Command>"
            "<WorkingDirectory>C:\\x${HOME}y</WorkingDirectory></Exec>",
        )
    )
    document = render_yaml([converted], sources=["t.xml"])
    job = parse_config_string(document, "").jobs[0]
    assert job.command == ["C:\\a b\\c.exe"]
    # $ is escaped as $$ because the loader interpolates every scalar but
    # command and shell; without it this would expand or fail at load.
    assert "${HOME}" in job.workingDirectory


def test_the_header_is_deterministic():
    # no timestamp and no version, so the same export gives the same bytes
    # and a checked-in example does not move on an unrelated commit.
    first = _emit(_task(triggers=_DAILY))
    second = _emit(_task(triggers=_DAILY))
    assert first == second
    assert "cronstable import-taskscheduler" in first


def test_duplicate_names_are_made_unique():
    tasks = [_convert(_task(uri="\\Same"), "a") for _ in range(2)]
    for task in tasks:
        task.jobs.append(
            {"name": "Same", "command": ["x"], "schedule": "* * * * *"}
        )
    taskxml._unique_names(tasks)
    names = [job["name"] for task in tasks for job in task.jobs]
    assert names == ["Same", "Same-2"]


# ---------------------------------------------------------------------------
# The command
# ---------------------------------------------------------------------------


def _args(paths, **over):
    values = {"paths": paths, "output": None, "timezone": None}
    values.update(over)
    return types.SimpleNamespace(**values)


def test_dispatch_writes_and_reports(tmp_path, capsys):
    source = tmp_path / "t.xml"
    source.write_text(_task(triggers=_DAILY), encoding="utf-8")
    out = tmp_path / "jobs.yaml"
    assert taskxml.dispatch(_args([str(source)], output=str(out))) == 0
    captured = capsys.readouterr()
    assert "1 converted into 1 job" in captured.err
    assert parse_config_string(out.read_text(encoding="utf-8"), "").jobs


def test_dispatch_writes_lf_line_endings(tmp_path):
    # the repository is LF only and the release build fails on a CRLF blob,
    # while Python's default text mode writes CRLF on Windows.
    source = tmp_path / "t.xml"
    source.write_text(_task(triggers=_DAILY), encoding="utf-8")
    out = tmp_path / "jobs.yaml"
    taskxml.dispatch(_args([str(source)], output=str(out)))
    assert b"\r\n" not in out.read_bytes()


def test_dispatch_reports_failure_when_nothing_converted(tmp_path, capsys):
    source = tmp_path / "t.xml"
    source.write_text(_task(triggers="<LogonTrigger/>"), encoding="utf-8")
    assert taskxml.dispatch(_args([str(source)])) == 1
    assert "nothing was converted" in capsys.readouterr().err


def test_dispatch_stops_on_a_named_file_that_is_not_an_export(
    tmp_path, capsys
):
    source = tmp_path / "app.xml"
    source.write_text("<configuration/>", encoding="utf-8")
    assert taskxml.dispatch(_args([str(source)])) == 1
    assert "not a Task Scheduler export" in capsys.readouterr().err


def test_a_directory_skips_a_stray_xml_instead_of_failing(tmp_path, capsys):
    # .xml is a name half the tooling on a Windows box writes, so a scan
    # skips what it did not expect; a file the operator NAMED does not.
    (tmp_path / "task.xml").write_text(
        _task(triggers=_DAILY), encoding="utf-8"
    )
    (tmp_path / "app.xml").write_text("<configuration/>", encoding="utf-8")
    out = tmp_path / "jobs.yaml"
    assert taskxml.dispatch(_args([str(tmp_path)], output=str(out))) == 0
    assert "skipped" in capsys.readouterr().err


def test_timezone_flag_stamps_every_job(tmp_path):
    source = tmp_path / "t.xml"
    source.write_text(_task(triggers=_DAILY), encoding="utf-8")
    out = tmp_path / "jobs.yaml"
    taskxml.dispatch(
        _args([str(source)], output=str(out), timezone="Europe/Berlin")
    )
    job = parse_config_string(out.read_text(encoding="utf-8"), "").jobs[0]
    assert str(job.timezone) == "Europe/Berlin"


def test_a_naive_boundary_becomes_local_time():
    # Task Scheduler means machine local time; cronstable defaults to UTC.
    converted = _convert(_task(triggers=_DAILY))
    document = render_yaml([converted], sources=["t.xml"])
    assert "utc: false" in document


def test_a_utc_boundary_emits_no_clock_key():
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>2026-08-10T02:30:00Z"
            "</StartBoundary><ScheduleByDay/></CalendarTrigger>"
        )
    )
    document = render_yaml([converted], sources=["t.xml"])
    assert "utc:" not in document


def test_a_stored_offset_is_reported_rather_than_invented():
    # -04:00 is not an IANA name, so it is not loadable as timezone:, and
    # claiming to reproduce its DST behaviour would assert what we cannot
    # know.
    converted = _convert(
        _task(
            triggers="<CalendarTrigger><StartBoundary>"
            "2026-08-10T02:30:00-04:00</StartBoundary><ScheduleByDay/>"
            "</CalendarTrigger>"
        )
    )
    document = render_yaml([converted], sources=["t.xml"])
    assert "utc: false" in document
    assert "-04:00" in _reasons(converted)


def test_no_seconds_column_is_ever_emitted():
    # one imported :38 would set has_seconds, and the daemon's subminute
    # decision is any() over its WHOLE job set, so every other job on the
    # box would start ticking once a second.
    converted = _convert(
        _task(
            triggers="<TimeTrigger><StartBoundary>2026-08-10T03:30:38"
            "</StartBoundary></TimeTrigger>"
        )
    )
    document = render_yaml([converted], sources=["t.xml"])
    conf = parse_config_string(document, "")
    assert all(not job.has_seconds for job in conf.jobs)
    assert len(converted.jobs[0]["schedule"].split()) == 6


def test_a_partly_converted_task_is_not_reported_as_not_converted():
    # A blocking note is per TRIGGER; `commented` is per task. A task with
    # an unconvertible trigger beside a daily one emits a live job AND
    # carries a blocking note, and the report used to call that "NOT
    # CONVERTED" while the job sat live in the same file. On a real
    # 195-task export that was 11 of the 24 converted tasks, and the
    # report is the only place an operator learns which tasks Task
    # Scheduler is still firing too.
    converted = _convert(
        _task(
            triggers=(
                "<LogonTrigger><Enabled>true</Enabled></LogonTrigger>" + _DAILY
            )
        )
    )
    assert converted.jobs and not converted.commented
    assert any(note.blocking for note in converted.notes)
    assert taskxml.report_state(converted) == "PARTIAL"
    report = taskxml.render_report([converted])
    assert "PARTIAL: " in report
    assert "NOT CONVERTED" not in report
    # the headline has to agree with the label under it
    assert "1 converted into 1 job(s), 0 not converted." in report
    assert "1 of the converted task(s)" in report


def test_a_task_with_nothing_emitted_is_still_not_converted():
    # The other side of the same rule: a state read off the YAML, so a
    # task that produced no job keeps the words render_yaml uses.
    converted = _convert(_task(triggers=""))
    assert not converted.jobs
    assert taskxml.report_state(converted) == "NOT CONVERTED"
    assert "NOT CONVERTED: " in taskxml.render_report([converted])


def test_one_unreadable_task_no_longer_costs_the_whole_export(
    tmp_path, capsys
):
    # `P1M` is one MONTH (the M is before the T), which has no fixed
    # length, so duration_seconds raises. The raise used to reach
    # dispatch, which printed one line and wrote NO yaml, so one bad task
    # in the 34th of 195 cost the other 194 and hand-editing the export
    # was the only recourse. Every other unconvertible thing here is a
    # Note, and now so is this.
    good = _task(uri="\\Good", triggers=_DAILY)
    bad = _task(
        uri="\\Bad",
        triggers=_DAILY,
        settings="<ExecutionTimeLimit>P1M</ExecutionTimeLimit>",
    )
    source = tmp_path / "t.xml"
    source.write_text("<Tasks>" + good + bad + "</Tasks>", encoding="utf-8")
    out = tmp_path / "jobs.yaml"

    assert taskxml.dispatch(_args([str(source)], output=str(out))) == 0

    document = out.read_text(encoding="utf-8")
    conf = parse_config_string(document, "")
    assert [job.name for job in conf.jobs] == ["Good"]
    err = capsys.readouterr().err
    assert "NOT CONVERTED: \\Bad" in err
    assert "not a duration this converter can read" in err
    assert "1 converted into 1 job(s), 1 not converted." in err


def test_a_bare_value_error_is_isolated_too(tmp_path, capsys):
    # TaskXmlError subclasses ValueError, so catching only the module's own
    # error leaves the plain int() calls uncovered: Priority here, and
    # DaysInterval and WeeksInterval on the calendar triggers. Those would
    # abort the whole export with a traceback, which is the thing the
    # isolation exists to stop.
    good = _task(uri="\\Good", triggers=_DAILY)
    bad = _task(
        uri="\\Bad",
        triggers=_DAILY,
        settings="<Priority>not-a-number</Priority>",
    )
    source = tmp_path / "t.xml"
    source.write_text("<Tasks>" + good + bad + "</Tasks>", encoding="utf-8")
    out = tmp_path / "jobs.yaml"

    assert taskxml.dispatch(_args([str(source)], output=str(out))) == 0

    conf = parse_config_string(out.read_text(encoding="utf-8"), "")
    assert [job.name for job in conf.jobs] == ["Good"]
    err = capsys.readouterr().err
    assert "NOT CONVERTED: \\Bad" in err
    assert "could not be read" in err


@pytest.mark.skipif(
    not os.environ.get("CRONSTABLE_TASKXML_LIVE"),
    reason="reads this machine's real Task Scheduler export",
)
def test_a_real_export_converts_and_loads(tmp_path):
    import subprocess

    export = tmp_path / "all.xml"
    subprocess.run(
        ["schtasks", "/query", "/XML", "ONE"],
        stdout=export.open("wb"),
        check=True,
    )
    out = tmp_path / "jobs.yaml"
    assert taskxml.dispatch(_args([str(export)], output=str(out))) == 0
    parse_config_string(out.read_text(encoding="utf-8"), "")
