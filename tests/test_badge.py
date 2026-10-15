"""Status badges: the SVG renderer, the verdicts, and the badge routes.

The renderer and verdict tables in ``cronstable/badge.py`` are pure, so
they are driven directly.  The handlers are called with the small mock
request the other web tests use, and the auth cases (private by default,
public by opt-in, off entirely) run against a real listener, because the
public exemption matches the route the router picked.
"""

import asyncio
import dataclasses
import datetime
import json
import math
import os
import re
import xml.etree.ElementTree as ET

import pytest
from aiohttp import web

import cronstable.cron
from cronstable import badge
from cronstable.config import (
    ConfigError,
    parse_config_string,
    resolve_badges_config,
)
from cronstable.cron import (
    RUN_HISTORY_LIMIT,
    SLA_CHECK_LATE,
    SLA_CHECK_STALE,
    WEB_ANON_REQUEST_KEY,
    WEB_BADGE_ROUTES,
    WEB_TOKEN_REQUEST_KEY,
    Cron,
    JobRunInfo,
    PauseInfo,
    _route_canonical,
    _web_public_paths,
    _web_public_routes,
    _WebToken,
)
from cronstable.job import JobOutputStream
from tests.conftest import Req, _cron

_UTC = datetime.timezone.utc
_SVG = "{http://www.w3.org/2000/svg}"

_JOBS = """
jobs:
  - name: backup
    command: "true"
    schedule: "0 3 * * *"
  - name: report
    command: "true"
    schedule: "0 6 * * *"
    sla:
      lateAfterSeconds: 600
  - name: parked
    command: "true"
    schedule: "0 0 * * *"
    enabled: false
"""


def _run(outcome, ago=0):
    now = datetime.datetime.now(_UTC) - datetime.timedelta(seconds=ago)
    return JobRunInfo(
        outcome=outcome,
        exit_code=0 if outcome == "success" else 1,
        started_at=now - datetime.timedelta(seconds=2),
        finished_at=now,
        fail_reason=None if outcome == "success" else "exit 1",
        output=JobOutputStream(),
    )


def _skip(ago=0):
    """The synthetic row of a slot that a pause held back."""
    output = JobOutputStream()
    output.closed = True
    return JobRunInfo(
        outcome="skipped",
        exit_code=None,
        started_at=None,
        finished_at=datetime.datetime.now(_UTC)
        - datetime.timedelta(seconds=ago),
        fail_reason=None,
        output=output,
        skip_reason="paused",
    )


@pytest.fixture
def fresh_fleet_badge(monkeypatch):
    """Rebuild the fleet badge on every request, so a test that writes the
    scheduler state directly sees its change at once."""
    monkeypatch.setattr(cronstable.cron, "_FLEET_BADGE_TTL", 0.0)


def _texts(svg):
    """The visible text runs of a rendered badge, in document order."""
    root = ET.fromstring(svg)
    return [
        el.text
        for el in root.iter(_SVG + "text")
        if el.get("aria-hidden") != "true"
    ]


@pytest.fixture
async def start_web_app():
    """Start a cron's real web app with teardown guaranteed, like the
    fixture of the same name in tests/test_cron_web.py."""
    crons = []

    async def start(cron, config):
        crons.append(cron)
        await cron.start_stop_web_app(config)
        return "http://127.0.0.1:{}".format(cron.web_runner.addresses[0][1])

    yield start
    for cron in reversed(crons):
        await cron.start_stop_web_app(None)
    # the Windows Proactor's transports finish closing (see test_cron_web)
    await asyncio.sleep(0.25)


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(None, {"public": False}, id="absent-is-on"),
        pytest.param(True, {"public": False}, id="true"),
        pytest.param(False, None, id="false"),
        pytest.param({}, {"public": False}, id="empty-map"),
        pytest.param({"enabled": False}, None, id="map-disabled"),
        pytest.param({"public": True}, {"public": True}, id="public"),
        pytest.param(
            {"enabled": True, "public": False}, {"public": False}, id="map"
        ),
    ],
)
def test_resolve_badges_config(raw, expected):
    web_config = {} if raw is None else {"badges": raw}
    assert resolve_badges_config(web_config) == expected


_WEB = "web:\n  listen:\n    - http://127.0.0.1:8080\n"


@pytest.mark.parametrize(
    "block",
    [
        "  badges: false\n",
        "  badges: true\n",
        "  badges:\n    public: true\n",
        "  badges:\n    enabled: false\n    public: false\n",
    ],
)
def test_web_badges_config_forms_parse(block):
    conf = parse_config_string(_WEB + block, "")
    assert "badges" in conf.web_config


@pytest.mark.parametrize(
    "block",
    [
        pytest.param("  badges:\n    public: maybe\n", id="not-a-bool"),
        pytest.param("  badges:\n    colour: red\n", id="unknown-key"),
        pytest.param("  badges: sometimes\n", id="not-bool-or-map"),
    ],
)
def test_web_badges_config_rejects_bad_values(block):
    with pytest.raises(ConfigError):
        parse_config_string(_WEB + block, "")


# ---------------------------------------------------------------------------
# verdicts
# ---------------------------------------------------------------------------


def _verdict(**over):
    state = {
        "enabled": True,
        "paused": False,
        "outcome": None,
        "overdue": False,
        "running": False,
    }
    state.update(over)
    return badge.job_verdict(**state)


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({}, "pending"),
        ({"running": True}, "running"),
        ({"outcome": "success"}, "passing"),
        # a later run in progress keeps the verdict
        ({"outcome": "success", "running": True}, "passing"),
        ({"outcome": "failure", "running": True}, "failing"),
        ({"outcome": "failure"}, "failing"),
        ({"outcome": "cancelled"}, "cancelled"),
        ({"outcome": "unknown"}, "unknown"),
        ({"outcome": "something-new"}, "unknown"),
        # overdue outranks a good or neutral result, not a failure
        ({"outcome": "success", "overdue": True}, "overdue"),
        ({"overdue": True}, "overdue"),
        ({"outcome": "failure", "overdue": True}, "failing"),
        # paused and disabled outrank every result
        ({"outcome": "failure", "paused": True}, "paused"),
        ({"outcome": "failure", "enabled": False}, "disabled"),
        ({"paused": True, "enabled": False}, "disabled"),
    ],
)
def test_job_verdict_precedence(state, expected):
    assert _verdict(**state) == expected


def test_every_verdict_has_a_color():
    seen = set()
    for enabled in (True, False):
        for paused in (True, False):
            for outcome in (
                None,
                "success",
                "failure",
                "cancelled",
                "unknown",
            ):
                for overdue in (True, False):
                    for running in (True, False):
                        seen.add(
                            badge.job_verdict(
                                enabled=enabled,
                                paused=paused,
                                outcome=outcome,
                                overdue=overdue,
                                running=running,
                            )
                        )
    assert seen == set(badge.VERDICT_COLORS)


@pytest.mark.parametrize(
    ("verdicts", "expected"),
    [
        ([], ("no jobs", "#9f9f9f")),
        (["passing"] * 12, ("12 passing", "#4c1")),
        (
            ["passing", "failing", "failing", "overdue"],
            ("2 failing", "#e05d44"),
        ),
        (["passing", "overdue", "paused"], ("1 overdue", "#fe7d37")),
        (["passing", "pending", "disabled"], ("1 passing", "#4c1")),
        (["pending", "paused", "disabled"], ("no status", "#9f9f9f")),
    ],
)
def test_fleet_message(verdicts, expected):
    assert badge.fleet_message(iter(verdicts)) == expected


# ---------------------------------------------------------------------------
# the renderer
# ---------------------------------------------------------------------------


def test_text_width_uses_the_table_and_the_fallbacks():
    # p a s s i n g in DejaVu Sans at 11px
    assert badge.text_width("passing") == pytest.approx(42.2)
    assert badge.text_width("") == 0.0
    # a combining mark adds nothing, a wide CJK character a full em
    assert badge.text_width("e\u0301") == pytest.approx(6.8)
    assert badge.text_width("\u65e5") == 11.0
    # any other character outside the table gets the fallback width
    assert badge.text_width("\u00e9") == 7.0


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("zero\u200bwidth", id="zero-width-space"),
        pytest.param("lab\ufe0f", id="variation-selector"),
        pytest.param("lab\u034f", id="grapheme-joiner"),
        pytest.param("lab\u20dd", id="enclosing-mark"),
    ],
)
def test_text_width_skips_characters_that_take_no_space(text):
    visible = "".join(c for c in text if c.isascii())
    assert badge.text_width(text) == badge.text_width(visible)


def test_text_width_counts_an_emoji_sequence_once():
    scientist = "\U0001f469\u200d\U0001f52c"
    assert badge.text_width(scientist) == badge.text_width("\U0001f469")
    # a skin tone merges into the emoji before it, and alone draws a swatch
    assert badge.text_width("\U0001f44d\U0001f3fd") == 11.0
    assert badge.text_width("\U0001f3fd") == 11.0
    # an emoji selector draws a text-style emoji, such as a warning sign,
    # a full em wide
    warning = "\N{WARNING SIGN}\N{VARIATION SELECTOR-16}"
    assert badge.text_width(warning) == 11.0
    couple = "\U0001f469\N{ZERO WIDTH JOINER}\N{HEAVY BLACK HEART}" + (
        "\N{VARIATION SELECTOR-16}\N{ZERO WIDTH JOINER}\U0001f468"
    )
    assert badge.text_width(couple) == 11.0
    # a joiner between letters merges nothing
    zwj = "\N{ZERO WIDTH JOINER}"
    assert badge.text_width("a" + zwj + "b") == badge.text_width("ab")
    e = "\N{LATIN SMALL LETTER E WITH ACUTE}"
    n = "\N{LATIN SMALL LETTER N WITH TILDE}"
    assert badge.text_width(e + zwj + n) == badge.text_width(e + n) == 14.0
    # a format character that draws a glyph keeps its width
    assert badge.text_width("\N{ARABIC NUMBER SIGN}") == 7.0


@pytest.mark.parametrize("style", badge.BADGE_STYLES)
def test_render_badge_is_well_formed_and_labelled(style):
    svg = badge.render_badge("nightly-backup", "passing", "#4c1", style)
    root = ET.fromstring(svg)
    assert root.tag == _SVG + "svg"
    assert root.get("role") == "img"
    assert root.get("aria-label") == "nightly-backup: passing"
    assert root.find(_SVG + "title").text == "nightly-backup: passing"
    assert _texts(svg) == ["nightly-backup", "passing"]
    # the two halves: 5px of padding around each measured text run
    label_width = math.ceil(badge.text_width("nightly-backup")) + 10
    message_width = math.ceil(badge.text_width("passing")) + 10
    assert root.get("width") == str(label_width + message_width)
    assert root.get("height") == "20"
    rects = [r for r in root.iter(_SVG + "rect") if r.get("fill")]
    fills = [r.get("fill") for r in rects]
    assert "#555" in fills and "#4c1" in fills


def test_flat_square_has_no_gradient_shadow_or_rounding():
    flat = badge.render_badge("a", "b", "#4c1", "flat")
    square = badge.render_badge("a", "b", "#4c1", "flat-square")
    assert "linearGradient" in flat and 'rx="3"' in flat
    assert 'aria-hidden="true"' in flat
    assert "linearGradient" not in square and "rx=" not in square
    assert 'aria-hidden="true"' not in square
    assert 'shape-rendering="crispEdges"' in square


def test_badges_inlined_together_keep_their_own_ids():
    # one HTML page can inline several badges; each must resolve its own
    # gradient and clip path, not the first badge's narrower one
    def ids(svg):
        return re.findall(r'id="([^"]+)"', svg)

    one = badge.render_badge("a", "passing", "#4c1")
    two = badge.render_badge("a much longer label", "failing", "#e05d44")
    assert ids(one) and set(ids(one)).isdisjoint(ids(two))
    for svg in (one, two):
        refs = re.findall(r"url\(#([^)]+)\)", svg)
        assert refs and set(refs) <= set(ids(svg))
    # the same badge renders the same bytes, so its ETag holds
    assert badge.render_badge("a", "passing", "#4c1") == one


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.mark.parametrize(
    ("path", "label", "message", "verdict", "style"),
    [
        pytest.param(
            os.path.join(ROOT, "docs", "img", "badge-passing.svg"),
            "nightly-backup",
            "passing",
            "passing",
            "flat",
            id="passing",
        ),
        pytest.param(
            os.path.join(ROOT, "docs", "img", "badge-failing.svg"),
            "db-vacuum",
            "failing",
            "failing",
            "flat",
            id="failing",
        ),
        pytest.param(
            os.path.join(ROOT, "docs", "img", "badge-overdue.svg"),
            "weekly-report",
            "overdue",
            "overdue",
            "flat",
            id="overdue",
        ),
        pytest.param(
            os.path.join(ROOT, "docs", "img", "badge-fleet.svg"),
            "cronstable",
            "12 passing",
            "passing",
            "flat",
            id="fleet",
        ),
        pytest.param(
            os.path.join(ROOT, "docs", "img", "badge-flat-square.svg"),
            "nightly backup",
            "passing",
            "passing",
            "flat-square",
            id="flat-square",
        ),
    ],
)
def test_documentation_images_match_the_renderer(
    path, label, message, verdict, style
):
    # the README and the wiki show these files as what the daemon serves
    with open(path, "rb") as fh:
        stored = fh.read()
    color = badge.VERDICT_COLORS[verdict]
    rendered = badge.render_badge(label, message, color, style)
    assert stored == rendered.encode("utf-8") + b"\n"


def test_render_badge_refuses_an_unknown_style():
    with pytest.raises(ValueError, match="unknown badge style"):
        badge.render_badge("a", "b", "#4c1", "plastic")


def test_render_badge_escapes_markup():
    label = "<script>alert(1)</script> & \"q\" 's'"
    svg = badge.render_badge(label, "passing", "#4c1")
    assert "<script>" not in svg
    # the parsed document carries the text back verbatim
    assert _texts(svg)[0] == label
    assert ET.fromstring(svg).get("aria-label") == label + ": passing"


def test_render_badge_replaces_characters_xml_forbids():
    # a job name is any YAML string, control characters included; the
    # image must stay a well-formed document
    svg = badge.render_badge("bell\x07name", "pass\x00ing", "#4c1")
    assert _texts(svg) == ["bell\ufffdname", "pass\ufffding"]
    # tab, newline, and characters past the BMP are legal and kept
    svg = badge.render_badge("tab\there", "\U0001f600", "#4c1")
    assert _texts(svg) == ["tab\there", "\U0001f600"]


_CONTROL = "label must not contain control characters or line breaks"


@pytest.mark.parametrize(
    ("label", "error"),
    [
        ("", "label must not be blank"),
        ("   ", "label must not be blank"),
        # spaces and invisible format characters only
        (
            "\N{NO-BREAK SPACE}\N{ZERO WIDTH SPACE}"
            "\N{ZERO WIDTH NO-BREAK SPACE}",
            "label must not be blank",
        ),
        ("x" * 65, "label must be at most 64 characters"),
        ("tab\there", _CONTROL),
        ("nul\x00", _CONTROL),
        ("two\N{LINE SEPARATOR}lines", _CONTROL),
        ("two\N{PARAGRAPH SEPARATOR}paragraphs", _CONTROL),
        ("lone\ud800surrogate", _CONTROL),
        # characters that take no space count as blank too
        (
            "\N{COMBINING ACUTE ACCENT}\N{VARIATION SELECTOR-16}",
            "label must not be blank",
        ),
        (" \N{COMBINING ENCLOSING CIRCLE}", "label must not be blank"),
        # a skin tone or an Arabic number sign on its own draws a glyph
        ("\N{EMOJI MODIFIER FITZPATRICK TYPE-4}", None),
        ("\N{ARABIC NUMBER SIGN}", None),
        ("x" * 64, None),
        ("nightly backup (prod) ✓", None),
        ("日本語", None),
        # any space separator, and the joiners emoji sequences use
        ("nightly\N{NO-BREAK SPACE}backup", None),
        ("nightly\N{THIN SPACE}backup", None),
        ("\U0001f469\N{ZERO WIDTH JOINER}\U0001f52c lab", None),
    ],
)
def test_label_error(label, error):
    assert badge.label_error(label) == error


@pytest.mark.parametrize(
    ("base", "name", "expected"),
    [
        (
            "https://cron.example.com",
            None,
            "[![cronstable](https://cron.example.com/badge.svg)]"
            "(https://cron.example.com/)",
        ),
        (
            "https://cron.example.com/",
            "nightly-backup",
            "[![nightly-backup]"
            "(https://cron.example.com/jobs/nightly-backup/badge.svg)]"
            "(https://cron.example.com/#job/nightly-backup)",
        ),
        (
            "http://h:8080",
            "odd [name] (x)\\y/z",
            "[![odd \\[name\\] (x)\\\\y/z]"
            "(http://h:8080/jobs/odd%20%5Bname%5D%20%28x%29%5Cy%2Fz/badge.svg)]"
            "(http://h:8080/#job/odd%20%5Bname%5D%20%28x%29%5Cy%2Fz)",
        ),
        # Markdown syntax in a name stays text: no code span, emphasis,
        # table cell break, HTML, entity, or GitHub footnote
        (
            "http://h",
            "^a`b|c*d<f&g~h",
            "[![\\^a\\`b\\|c\\*d\\<f\\&g\\~h]"
            "(http://h/jobs/%5Ea%60b%7Cc%2Ad%3Cf%26g~h/badge.svg)]"
            "(http://h/#job/%5Ea%60b%7Cc%2Ad%3Cf%26g~h)",
        ),
        # an underscore inside a word opens no emphasis; one at a word's
        # edge can
        (
            "http://h",
            "nightly_backup _x_",
            "[![nightly_backup \\_x\\_]"
            "(http://h/jobs/nightly_backup%20_x_/badge.svg)]"
            "(http://h/#job/nightly_backup%20_x_)",
        ),
        # a line break becomes a space, so the paragraph cannot end in
        # the alt text
        (
            "http://h",
            "a\n# b",
            "[![a # b](http://h/jobs/a%0A%23%20b/badge.svg)]"
            "(http://h/#job/a%0A%23%20b)",
        ),
        (
            "http://h",
            "a\r\n\r\nb",
            "[![a  b](http://h/jobs/a%0D%0A%0D%0Ab/badge.svg)]"
            "(http://h/#job/a%0D%0A%0D%0Ab)",
        ),
        (
            "http://h",
            "a\N{LINE SEPARATOR}b",
            "[![a b](http://h/jobs/a%E2%80%A8b/badge.svg)]"
            "(http://h/#job/a%E2%80%A8b)",
        ),
        (
            "http://h",
            "nightly\n",
            "[![nightly ](http://h/jobs/nightly%0A/badge.svg)]"
            "(http://h/#job/nightly%0A)",
        ),
        (
            "http://h",
            "\nx",
            "[![ x](http://h/jobs/%0Ax/badge.svg)](http://h/#job/%0Ax)",
        ),
        (
            "http://h",
            "[x]\\\ny",
            "[![\\[x\\]\\\\ y](http://h/jobs/%5Bx%5D%5C%0Ay/badge.svg)]"
            "(http://h/#job/%5Bx%5D%5C%0Ay)",
        ),
        # an unbalanced parenthesis in a --url path prefix would end the
        # link destination early
        (
            "https://h/cron(prod",
            None,
            "[![cronstable](https://h/cron%28prod/badge.svg)]"
            "(https://h/cron%28prod/)",
        ),
        (
            "https://h/ops)/",
            "x",
            "[![x](https://h/ops%29/jobs/x/badge.svg)]"
            "(https://h/ops%29/#job/x)",
        ),
    ],
)
def test_badge_markdown(base, name, expected):
    assert badge.badge_markdown(base, name) == expected


def test_badge_markdown_turns_each_line_break_into_one_space():
    # the boundaries of str.splitlines, which the page's badgeMarkdown
    # matches too
    for code in range(0x10000):
        if 0xD800 <= code <= 0xDFFF:
            continue  # quote() cannot encode a lone surrogate
        char = chr(code)
        breaks = len(("a" + char + "b").splitlines()) == 2
        embed = badge.badge_markdown("h", "a" + char + "b", link=False)
        assert embed.startswith("![a b]") == (breaks or char == " "), hex(code)
    # CRLF is one boundary
    embed = badge.badge_markdown("h", "a\r\nb", link=False)
    assert embed.startswith("![a b]")


@pytest.mark.parametrize(
    ("name", "addressable"),
    [
        ("nightly-backup", True),
        ("team/backup (prod)", True),
        ("...", True),
        (".a", True),
        ("%2e%2e", True),
        ("../x", True),
        # a browser resolves the dot segments before it sends the request
        (".", False),
        ("..", False),
        # an empty segment matches no route
        ("", False),
        # the route's {name} segment matches no brace
        ("a{b}", False),
        ("{", False),
        ("x}", False),
    ],
)
def test_badge_addressable(name, addressable):
    assert badge.badge_addressable(name) is addressable


def test_badge_markdown_without_a_link():
    # a daemon with web.ui: false serves no dashboard to link to
    assert badge.badge_markdown("http://h:8080", "a b", link=False) == (
        "![a b](http://h:8080/jobs/a%20b/badge.svg)"
    )
    assert badge.badge_markdown("http://h:8080/", link=False) == (
        "![cronstable](http://h:8080/badge.svg)"
    )


# ---------------------------------------------------------------------------
# the handlers
# ---------------------------------------------------------------------------


async def _job_badge(cron, name, **req):
    return await cron._web_job_badge(Req(match={"name": name}, **req))


async def test_job_badge_reads_the_scheduler_state():
    cron = _cron(_JOBS)
    now = datetime.datetime.now(_UTC)

    async def message(name):
        return _texts((await _job_badge(cron, name)).text)[1]

    assert await message("backup") == "pending"
    cron.running_jobs["backup"] = [object()]
    assert await message("backup") == "running"
    cron._record_run("backup", _run("success"))
    assert await message("backup") == "passing"
    cron._record_run("backup", _run("failure"))
    assert await message("backup") == "failing"
    del cron.running_jobs["backup"]
    cron._paused["backup"] = PauseInfo(
        since=now,
        until=now + datetime.timedelta(hours=1),
        note="",
        by="t",
        channel="api",
    )
    assert await message("backup") == "paused"
    # an expired pause no longer counts
    cron._paused["backup"] = PauseInfo(
        since=now - datetime.timedelta(hours=2),
        until=now - datetime.timedelta(hours=1),
        note="",
        by="t",
        channel="api",
    )
    assert await message("backup") == "failing"
    assert await message("parked") == "disabled"
    # an SLA latch makes a passing job overdue
    cron._record_run("report", _run("success"))
    assert await message("report") == "passing"
    cron._sla_state[("report", SLA_CHECK_LATE)] = now
    assert await message("report") == "overdue"
    # a latch on a check the job no longer configures, as after a reload
    # that dropped it, does not count; the dashboard's sla.state agrees
    del cron._sla_state[("report", SLA_CHECK_LATE)]
    cron._sla_state[("report", SLA_CHECK_STALE)] = now
    assert await message("report") == "passing"


async def test_a_pause_skip_keeps_the_last_run_verdict():
    cron = _cron(_JOBS)

    async def message(name):
        return _texts((await _job_badge(cron, name)).text)[1]

    async def fleet():
        return _texts((await cron._web_fleet_badge(Req())).text)[1]

    cron._record_run("backup", _run("failure", ago=60))
    cron._record_run("report", _run("success", ago=60))
    assert await fleet() == "1 failing"
    # the slot a pause held back ran nothing, so the failure still shows
    cron._record_run("backup", _skip(ago=30))
    assert cron.last_run["backup"].outcome == "skipped"
    assert await message("backup") == "failing"
    assert await fleet() == "1 failing"
    # a cancelled run is a run: a skip after it leaves "cancelled"
    cron._record_run("report", _run("cancelled", ago=40))
    cron._record_run("report", _skip(ago=20))
    assert await message("report") == "cancelled"
    # the next real run sets the verdict
    cron._record_run("backup", _run("success"))
    assert await message("backup") == "passing"
    # a job that never ran stays pending through a skip
    fresh = _cron(_JOBS)
    fresh._record_run("backup", _skip())
    assert _texts((await _job_badge(fresh, "backup")).text)[1] == "pending"


async def test_a_long_pause_keeps_the_last_run_verdict():
    # a long pause fills the history ring with skipped rows; the badge
    # still reads the last real run, whatever its outcome
    cron = _cron(_JOBS)
    cron._record_run("backup", _run("failure", ago=7200))
    cron._record_run("report", _run("success", ago=7200))
    cron._record_run("report", _run("cancelled", ago=3600))
    cron._record_run("parked", _run("success", ago=7200))
    # this host's crash-reconciled run, installed as the latest news
    cron._install_run_info("parked", _run("unknown", ago=9000), promote=True)
    for name in ("backup", "report", "parked"):
        for ago in range(RUN_HISTORY_LIMIT, 0, -1):
            cron._record_run(name, _skip(ago=ago))
        assert all(r.outcome == "skipped" for r in cron.run_history[name])
    assert cron._last_run_outcome("backup") == "failure"
    assert cron._last_run_outcome("report") == "cancelled"
    assert cron._last_run_outcome("parked") == "unknown"
    assert _texts((await _job_badge(cron, "report")).text)[1] == "cancelled"


def test_the_last_run_installed_wins_a_tie():
    # two runs that finish at the same instant: the later row is the last
    # run, for the badge as for last_run
    cron = _cron(_JOBS)
    first = _run("success")
    cron._record_run("backup", first)
    cron._record_run("backup", dataclasses.replace(first, outcome="failure"))
    assert cron.last_run["backup"].outcome == "failure"
    assert cron._last_run_outcome("backup") == "failure"


class _GatedRuns:
    """A state backend whose run listing waits for the test to release it."""

    def __init__(self, records):
        self.records = records
        self.release = asyncio.Event()
        self.calls = []

    async def list_records(self, stream, **kwargs):
        self.calls.append(kwargs)
        await self.release.wait()
        if isinstance(self.records, BaseException):
            raise self.records
        return [r for r in self.records if kwargs["predicate"](r)]


# newest first, as listed, except that the newest real run landed after an
# older one: appends can land out of finish order
_RECORDS = [
    {"outcome": "skipped", "finished_at": "2026-07-01T10:00:00+00:00"},
    {"outcome": "success", "finished_at": "2026-07-01T08:00:00+00:00"},
    {"outcome": "failure", "finished_at": "2026-07-01T09:00:00+00:00"},
]


async def test_the_deeper_read_finds_the_last_real_run():
    cron = _cron(_JOBS)
    cron.state_backend = backend = _GatedRuns(_RECORDS)
    backend.release.set()
    assert await cron._warm_last_real_run_beyond_history("backup")
    assert cron._last_run_outcome("backup") == "failure"
    # the scan stops at the first real runs instead of the whole stream
    assert backend.calls[0]["max_matches"] == RUN_HISTORY_LIMIT
    # and, with unbounded retention, at a fixed depth
    assert cron._state_max_runs == 0
    assert backend.calls[0]["limit"] == cronstable.cron.LAST_RUN_SCAN_LIMIT


@pytest.mark.parametrize("meanwhile", ["live run", "job removed"])
async def test_the_deeper_read_yields_to_what_happens_meanwhile(meanwhile):
    cron = _cron(_JOBS)
    cron.state_backend = backend = _GatedRuns(_RECORDS)
    read = asyncio.ensure_future(
        cron._warm_last_real_run_beyond_history("backup")
    )
    await asyncio.sleep(0)
    if meanwhile == "live run":
        cron._install_run_info("backup", _run("cancelled"))
    else:
        del cron.cron_jobs["backup"]
    backend.release.set()
    await read
    expected = {"live run": "cancelled", "job removed": None}[meanwhile]
    assert cron._last_run_outcome("backup") == expected


@pytest.mark.parametrize(
    "error, timed_out",
    [
        (asyncio.TimeoutError(), True),
        (TimeoutError(), True),
        (OSError("store unavailable"), False),
    ],
    ids=["asyncio-timeout", "builtin-timeout", "store-error"],
)
async def test_a_deeper_read_failure_leaves_the_badge_pending(
    error, timed_out, caplog
):
    # the badge reads what it had; a timeout is reported, so the
    # post-boot pass skips the scans still to run
    cron = _cron(_JOBS)
    cron.state_backend = backend = _GatedRuns(error)
    backend.release.set()
    ok = await cron._warm_last_real_run_beyond_history("backup")
    assert ok is not timed_out
    assert cron._last_run_outcome("backup") is None
    assert "cannot widen the last-run scan for backup" in caplog.text


async def test_the_deeper_read_lets_a_cancellation_through():
    cron = _cron(_JOBS)
    cron.state_backend = backend = _GatedRuns(asyncio.CancelledError())
    backend.release.set()
    with pytest.raises(asyncio.CancelledError):
        await cron._warm_last_real_run_beyond_history("backup")


async def test_job_badge_response_headers():
    cron = _cron(_JOBS)
    cron.web_config = {
        "headers": {
            "X-Operator": "kept",
            "content-type": "text/plain",
            "Content-Security-Policy": "default-src 'self'",
        }
    }
    resp = await _job_badge(cron, "backup")
    assert resp.status == 200
    assert resp.content_type == "image/svg+xml"
    assert resp.charset == "utf-8"
    assert resp.headers["Cache-Control"] == "no-cache"
    assert re.fullmatch(r'"[0-9a-f]{32}"', resp.headers["ETag"])
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Operator"] == "kept"
    # an operator's header wins over the image's default, as on the page
    assert resp.headers["Content-Security-Policy"] == "default-src 'self'"
    assert _texts(resp.text) == ["backup", "pending"]


async def test_operator_headers_in_any_spelling_never_double_up():
    # an operator header in another case replaces the default of the same
    # name and never doubles the validator or the cache policy; the
    # dashboard page follows the same rule
    cron = _cron(_JOBS)
    cron.web_config = {
        "headers": {
            "cache-control": "max-age=86400",
            "etag": '"operator"',
            "vary": "Cookie",
            "content-security-policy": "default-src 'self'",
            "content-encoding": "gzip",
        }
    }
    csp = ["default-src 'self'"]
    resp = await _job_badge(cron, "backup")
    assert resp.headers.getall("Cache-Control") == ["no-cache"]
    assert resp.headers.getall("ETag") != ['"operator"']
    assert len(resp.headers.getall("ETag")) == 1
    assert resp.headers.getall("Content-Security-Policy") == csp
    page = await cron._web_index(Req())
    assert page.headers.getall("Cache-Control") == ["no-cache"]
    assert len(page.headers.getall("ETag")) == 1
    assert page.headers.getall("Vary") == ["Accept-Encoding"]
    assert page.headers.getall("Content-Security-Policy") == csp
    # the page is sent uncompressed to a client that accepts no gzip
    assert page.headers.getall("Content-Encoding", []) == []


_PRIVATE_BADGE_HEADERS = {
    "Content-Security-Policy": ["default-src 'none'; frame-ancestors 'self'"],
    "X-Content-Type-Options": ["nosniff"],
    "X-Frame-Options": ["SAMEORIGIN"],
    "Cross-Origin-Resource-Policy": ["same-origin"],
}
_PUBLIC_BADGE_HEADERS = {
    "Content-Security-Policy": ["default-src 'none'"],
    "X-Content-Type-Options": ["nosniff"],
    "Cross-Origin-Resource-Policy": ["cross-origin"],
}


def _security_headers(resp):
    """Every copy of each security header a badge response carries."""
    return {
        name: resp.headers.getall(name)
        for name in _PRIVATE_BADGE_HEADERS
        if name in resp.headers
    }


async def _badge_and_revalidation(get):
    """A badge response and the ``304`` that revalidates it."""
    first = await get()
    again = await get(headers={"If-None-Match": first.headers["ETag"]})
    assert (first.status, again.status) == (200, 304)
    return first, again


def _both_badges(cron):
    return (
        lambda **req: _job_badge(cron, "backup", **req),
        lambda **req: cron._web_fleet_badge(Req(**req)),
    )


@pytest.mark.parametrize(
    ("badges", "expected"),
    [
        pytest.param(None, _PRIVATE_BADGE_HEADERS, id="default"),
        pytest.param({"public": False}, _PRIVATE_BADGE_HEADERS, id="private"),
        pytest.param({"public": True}, _PUBLIC_BADGE_HEADERS, id="public"),
    ],
)
async def test_badges_load_and_frame_cross_origin_only_when_public(
    badges, expected
):
    # a page on another origin could load a badge as an image, or frame it
    # in an <object> or <embed>, and read its width, which tells one
    # verdict from another: CORP refuses the image, frame-ancestors and
    # X-Frame-Options the frame
    cron = _cron(_JOBS)
    if badges is not None:
        cron.web_config = {"badges": badges}
    for get in _both_badges(cron):
        for resp in await _badge_and_revalidation(get):
            assert _security_headers(resp) == expected


@pytest.mark.parametrize("public", [False, True])
async def test_operator_headers_replace_the_badge_defaults(public):
    # in any letter case, public or not, and whether or not the default
    # set has the header
    cron = _cron(_JOBS)
    csp = "default-src 'none'; frame-ancestors https://wiki.example"
    cron.web_config = {
        "badges": {"public": public},
        "headers": {
            "cross-origin-resource-policy": "same-site",
            "CONTENT-SECURITY-POLICY": csp,
            "x-frame-options": "DENY",
        },
    }
    for get in _both_badges(cron):
        for resp in await _badge_and_revalidation(get):
            assert _security_headers(resp) == {
                "Content-Security-Policy": [csp],
                "X-Content-Type-Options": ["nosniff"],
                "X-Frame-Options": ["DENY"],
                "Cross-Origin-Resource-Policy": ["same-site"],
            }


async def test_job_badge_revalidates_with_the_etag():
    cron = _cron(_JOBS)
    first = await _job_badge(cron, "backup")
    etag = first.headers["ETag"]
    for header in (etag, "W/" + etag, '"other", ' + etag):
        again = await _job_badge(
            cron, "backup", headers={"If-None-Match": header}
        )
        assert again.status == 304
        assert again.body is None
        assert again.headers["ETag"] == etag
    # a new verdict is a new image, so the old tag no longer matches
    cron._record_run("backup", _run("success"))
    changed = await _job_badge(cron, "backup", headers={"If-None-Match": etag})
    assert changed.status == 200
    assert changed.headers["ETag"] != etag


async def test_job_badge_label_and_style_overrides():
    cron = _cron(_JOBS)
    resp = await _job_badge(
        cron, "backup", query={"label": "nightly backup", "style": ""}
    )
    assert _texts(resp.text) == ["nightly backup", "pending"]
    assert "linearGradient" in resp.text
    square = await _job_badge(cron, "backup", query={"style": "flat-square"})
    assert "linearGradient" not in square.text


@pytest.mark.parametrize(
    ("query", "error"),
    [
        ({"label": ""}, "label must not be blank"),
        ({"label": "x" * 65}, "label must be at most 64 characters"),
        ({"label": "a\nb"}, _CONTROL),
        ({"style": "plastic"}, "style must be one of: flat, flat-square"),
    ],
)
async def test_badge_bad_query_is_a_400(query, error):
    cron = _cron(_JOBS)
    for resp in (
        await _job_badge(cron, "backup", query=query),
        await cron._web_fleet_badge(Req(query=query)),
    ):
        assert resp.status == 400
        assert json.loads(resp.text) == {"error": error}


async def test_job_badge_unknown_job_is_a_404():
    cron = _cron(_JOBS)
    with pytest.raises(web.HTTPNotFound) as raised:
        await _job_badge(cron, "nope")
    assert json.loads(raised.value.text or "") == {
        "error": "job 'nope' not found"
    }


async def test_fleet_badge_counts_the_job_verdicts(fresh_fleet_badge):
    cron = _cron(_JOBS)

    async def texts(**req):
        return _texts((await cron._web_fleet_badge(Req(**req))).text)

    # nothing has run, and the disabled job shows no result either
    assert await texts() == ["cronstable", "no status"]
    cron._record_run("backup", _run("success"))
    cron._record_run("report", _run("success"))
    assert await texts() == ["cronstable", "2 passing"]
    cron._sla_state[("report", SLA_CHECK_LATE)] = datetime.datetime.now(_UTC)
    assert await texts() == ["cronstable", "1 overdue"]
    cron._record_run("backup", _run("failure"))
    assert await texts() == ["cronstable", "1 failing"]
    # a failed run of a disabled job does not color the fleet
    cron._record_run("parked", _run("failure"))
    assert await texts() == ["cronstable", "1 failing"]
    assert await texts(query={"label": "prod"}) == ["prod", "1 failing"]


async def test_fleet_badge_with_no_jobs():
    cron = _cron("")
    resp = await cron._web_fleet_badge(Req())
    assert _texts(resp.text) == ["cronstable", "no jobs"]


async def test_fleet_badge_shares_one_pass_until_a_bust(monkeypatch):
    # a public badge in a busy README must not walk every job per request.
    # The TTL is widened so a stall on a loaded runner cannot add a pass.
    monkeypatch.setattr(cronstable.cron, "_FLEET_BADGE_TTL", 3600.0)
    cron = _cron(_JOBS)
    passes = []
    build = cron._build_fleet_badge_verdict

    async def counting():
        passes.append(1)
        return await build()

    monkeypatch.setattr(cron, "_build_fleet_badge_verdict", counting)
    for query in ({}, {"label": "prod"}, {"style": "flat-square"}):
        await cron._web_fleet_badge(Req(query=query))
    assert len(passes) == 1
    # a recorded run busts the memo, as it does for /jobs
    cron._record_run("backup", _run("failure"))
    resp = await cron._web_fleet_badge(Req())
    assert _texts(resp.text)[1] == "1 failing"
    assert len(passes) == 2


# ---------------------------------------------------------------------------
# auth: private by default, public by opt-in
# ---------------------------------------------------------------------------


def test_web_public_paths_and_routes_composition():
    on = {"public": True}
    off = {"public": False}
    assert _web_public_paths(True, None) == {"/"}
    assert _web_public_paths(False, off) == frozenset()
    assert _web_public_paths(False, on) == {"/metrics"}
    assert _web_public_routes(None) == frozenset()
    assert _web_public_routes(off) == frozenset()
    assert _web_public_routes(on) == WEB_BADGE_ROUTES
    # the badge routes come from the route table itself
    assert WEB_BADGE_ROUTES == {
        path
        for _method, path, _handler, gate in cronstable.cron.WEB_ROUTES
        if gate == "badges"
    }
    assert "/jobs/{name}/badge.svg" in WEB_BADGE_ROUTES


async def test_auth_reads_the_route_only_for_public_routes(monkeypatch):
    # every request passes the public check first, so without a public
    # route it must not resolve the matched route at all
    def no_route(request):
        raise AssertionError("route read")

    monkeypatch.setattr(cronstable.cron, "_route_canonical", no_route)

    async def handler(request):
        return web.Response(text="ok")

    class Request(dict):
        path = "/jobs"
        method = "GET"
        headers = {"Authorization": "Bearer secret"}
        query: dict = {}

    middleware = Cron._make_auth_middleware("secret", frozenset({"/"}))
    assert (await middleware(Request(), handler)).text == "ok"


def test_route_canonical_tolerates_a_bare_request():
    class Bare:
        path = "/jobs/x/badge.svg"

    assert _route_canonical(Bare()) is None


_TOKEN = {"value": "s3cret", "fromFile": None, "fromEnvVar": None}


async def _status(session, url, token=None):
    headers = {"Authorization": "Bearer " + token} if token else None
    async with session.get(url, headers=headers) as resp:
        await resp.read()
        return resp.status


async def test_badges_require_the_token_by_default(start_web_app):
    import aiohttp

    cron = Cron(None, config_yaml=_JOBS)
    base = await start_web_app(
        cron, {"listen": ["http://127.0.0.1:0"], "authToken": _TOKEN}
    )
    async with aiohttp.ClientSession() as session:
        assert await _status(session, base + "/badge.svg") == 401
        assert await _status(session, base + "/jobs/backup/badge.svg") == 401
        assert await _status(session, base + "/badge.svg", "s3cret") == 200
        assert (
            await _status(session, base + "/jobs/backup/badge.svg", "s3cret")
            == 200
        )


async def test_public_badges_open_only_the_badge_routes(start_web_app):
    import aiohttp

    cron = Cron(None, config_yaml=_JOBS)
    base = await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": _TOKEN,
            "badges": {"public": True},
        },
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/jobs/backup/badge.svg") as resp:
            assert resp.status == 200
            assert resp.content_type == "image/svg+xml"
            assert _texts(await resp.text()) == ["backup", "pending"]
        assert await _status(session, base + "/badge.svg") == 200
        # HEAD rides along with GET, as for every read route
        async with session.head(base + "/badge.svg") as resp:
            assert resp.status == 200
        # an unknown job is still an honest 404, not a 401
        assert await _status(session, base + "/jobs/nope/badge.svg") == 404
        # every other route keeps the token gate
        for path in ("/jobs", "/jobs/backup", "/summary", "/metrics"):
            assert await _status(session, base + path) == 401


async def test_disabled_badges_are_not_routed(start_web_app):
    import aiohttp

    cron = Cron(None, config_yaml=_JOBS)
    base = await start_web_app(
        cron, {"listen": ["http://127.0.0.1:0"], "badges": False}
    )
    async with aiohttp.ClientSession() as session:
        assert await _status(session, base + "/badge.svg") == 404
        assert await _status(session, base + "/jobs/backup/badge.svg") == 404
        assert await _status(session, base + "/jobs/backup") == 200


async def test_anonymous_view_serves_badges(start_web_app):
    import aiohttp

    cron = Cron(None, config_yaml=_JOBS)
    base = await start_web_app(
        cron,
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": _TOKEN,
            "anonymousScopes": ["view"],
        },
    )
    async with aiohttp.ClientSession() as session:
        assert await _status(session, base + "/jobs/backup/badge.svg") == 200


async def test_whoami_says_whether_badges_are_served_and_public():
    class Request(Req):
        """A request the auth middleware has already filed."""

        def __init__(self, token=None, anon=None):
            super().__init__()
            self._store = {}
            if token is not None:
                self._store[WEB_TOKEN_REQUEST_KEY] = token
            if anon is not None:
                self._store[WEB_ANON_REQUEST_KEY] = anon

        def get(self, key, default=None):
            return self._store.get(key, default)

    token = _WebToken(b"t", frozenset({"view"}), "wallboard")

    async def badges(web_config, request):
        cron = _cron(_JOBS)
        cron.web_config = web_config
        return json.loads((await cron._web_whoami(request)).body)["badges"]

    # off: the dashboards hide their copy-badge commands
    assert await badges({"badges": False}, Request(token=token)) is None
    # served, but a page on another origin shows them only when public,
    # with a token, an anonymous grant, or no token configured
    gated = {"public": False, "dashboard": True}
    assert await badges({}, Request(token=token)) == gated
    grant = {"anonymousScopes": ["view"]}
    assert await badges(grant, Request(token=token)) == gated
    assert await badges(grant, Request(anon=frozenset({"view"}))) == gated
    assert await badges({}, Request()) == gated
    # public by opt-in only
    public = {"public": True, "dashboard": True}
    opt_in = {"badges": {"public": True}}
    assert await badges(opt_in, Request(token=token)) == public
    assert await badges(opt_in, Request()) == public
    # web.ui: false serves no page for the copied Markdown to link to
    headless = {"ui": False}
    assert await badges(headless, Request(token=token)) == {
        "public": False,
        "dashboard": False,
    }


@pytest.mark.parametrize(
    ("old", "new", "headers", "answer"),
    [
        pytest.param(
            {"public": True},
            {"public": False},
            _PRIVATE_BADGE_HEADERS,
            {"public": False, "dashboard": True},
            id="public-to-private",
        ),
        pytest.param(
            {"public": False},
            {"public": True},
            _PUBLIC_BADGE_HEADERS,
            {"public": True, "dashboard": True},
            id="private-to-public",
        ),
        pytest.param(True, False, None, None, id="on-to-off"),
    ],
)
async def test_a_restart_answers_with_the_new_apps_badge_settings(
    start_web_app, monkeypatch, old, new, headers, answer
):
    # A reload hands new connections to the new app while the old one
    # drains, and latches web_config only after that. The badge headers
    # and /whoami must match the routes and gate of the app that serves.
    import aiohttp

    cron = Cron(None, config_yaml=_JOBS)
    config = {"listen": ["http://127.0.0.1:0"], "badges": old}
    base = await start_web_app(cron, config)
    retiring = cron.web_runner
    seen = {}
    cleanup = web.AppRunner.cleanup

    async def read_while_draining(runner):
        if runner is retiring:
            assert cron.web_config["badges"] == old
            async with aiohttp.ClientSession() as session:
                url = base + "/jobs/backup/badge.svg"
                async with session.get(url) as resp:
                    seen["status"] = resp.status
                    seen["headers"] = _security_headers(resp)
                async with session.get(base + "/whoami") as resp:
                    seen["answer"] = (await resp.json())["badges"]
        await cleanup(runner)

    monkeypatch.setattr(web.AppRunner, "cleanup", read_while_draining)
    await cron.start_stop_web_app(dict(config, badges=new))
    assert cron.web_runner is not retiring
    if headers is None:
        assert seen["status"] == 404
    else:
        assert (seen["status"], seen["headers"]) == (200, headers)
    assert seen["answer"] == answer


_ODD_NAME = """
jobs:
  - name: team/backup (prod)
    command: "true"
    schedule: "0 3 * * *"
"""


async def test_copied_markdown_image_url_reaches_the_badge(start_web_app):
    # the dashboards percent-encode the name, "/" and ")" included, and
    # the router decodes it back into one path segment
    import aiohttp

    cron = Cron(None, config_yaml=_ODD_NAME)
    base = await start_web_app(cron, {"listen": ["http://127.0.0.1:0"]})
    markdown = badge.badge_markdown(base, "team/backup (prod)")
    image = re.search(r"\]\((\S+?/badge\.svg)\)\]", markdown).group(1)
    async with aiohttp.ClientSession() as session:
        async with session.get(image) as resp:
            assert resp.status == 200
            assert _texts(await resp.text()) == [
                "team/backup (prod)",
                "pending",
            ]


# ---------------------------------------------------------------------------
# the dashboards' copy actions
# ---------------------------------------------------------------------------

_PAGE = os.path.join(os.path.dirname(badge.__file__), "web", "index.html")


def test_page_palette_offers_badge_markdown():
    with open(_PAGE, encoding="utf-8") as fh:
        page = fh.read()
    assert 'lbl: "Copy badge Markdown: " + j.name' in page
    # the fleet entry sits after "Copy job-set ID", as in the terminal
    labels = re.findall(r'lbl: "([^"]+)"', page)
    at = labels.index("Copy fleet badge Markdown")
    assert labels[at - 1] == "Copy job-set ID"


def test_tui_palette_copies_badge_markdown(tmp_path):
    from tests.test_tui import _bare_app, _job

    app = _bare_app(tmp_path)
    app.jobs = [_job("nightly backup")]
    app.by_name = {j["name"]: j for j in app.jobs}

    def labels():
        return [label for _icon, label, _run in app.palette_commands()]

    # nothing to copy until the daemon says it serves badges
    assert not [label for label in labels() if "badge" in label]
    app.badges = {"public": True}
    at = labels().index("Copy fleet badge Markdown")
    assert labels()[at - 1] == "Copy job-set ID"
    run = {label: action for _icon, label, action in app.palette_commands()}
    run["Copy badge Markdown: nightly backup"]()
    run["Copy fleet badge Markdown"]()
    assert app.term.copied == [
        badge.badge_markdown(app.api.url, "nightly backup"),
        badge.badge_markdown(app.api.url),
    ]
    assert app.toasts[-1][1] == "❏ copied badge Markdown"
    # a badge that isn't public says what embedding it takes
    app.badges = {"public": False}
    run["Copy fleet badge Markdown"]()
    assert app.toasts[-1][1] == "❏ copied; embeds need web.badges.public"


def test_tui_badge_toast_names_the_setting_at_80_columns(tmp_path):
    from cronstable import tui
    from tests.test_tui import _bare_app, _screen

    app = _bare_app(tmp_path)
    # a toast gets half the width
    app.term = tui.HeadlessTerm(80, 24)
    app.badges = {"public": False, "dashboard": True}
    app._copy_badge_markdown("backup")
    assert "❏ copied; embeds need web.badges.public" in _screen(app)


def test_tui_palette_leaves_out_badges_no_url_reaches(tmp_path):
    from tests.test_tui import _bare_app, _job

    app = _bare_app(tmp_path)
    app.badges = {"public": True, "dashboard": True}
    names = ["ok", ".", "..", "", "{x}", "a}b", "..."]
    app.jobs = [_job(name) for name in names]
    app.by_name = {j["name"]: j for j in app.jobs}
    labels = [label for _icon, label, _run in app.palette_commands()]
    rows = [
        label[len("Copy badge Markdown: ") :]
        for label in labels
        if label.startswith("Copy badge Markdown: ")
    ]
    assert rows == ["ok", "..."]
    # the jobs keep their other rows
    assert "Copy command: .." in labels
    assert "Copy fleet badge Markdown" in labels


def test_tui_badge_markdown_leaves_out_the_url_credentials(tmp_path):
    from tests.test_tui import _bare_app

    app = _bare_app(tmp_path)
    app.badges = {"public": True, "dashboard": True}
    # the Markdown goes into a README: the --url credentials, query, and
    # fragment stay out, and a path prefix stays in
    app.api.url = "https://admin:hunter2@cron.example.com/ops?x=1#f"
    app._copy_badge_markdown("backup")
    assert app.term.copied == [
        badge.badge_markdown("https://cron.example.com/ops", "backup")
    ]
    # a daemon with web.ui: false serves no dashboard to link to
    app.badges = {"public": True, "dashboard": False}
    app._copy_badge_markdown("backup")
    assert app.term.copied[-1] == (
        "![backup](https://cron.example.com/ops/jobs/backup/badge.svg)"
    )
    # an address that names no http(s) host copies nothing
    app.api.url = "unix:///run/cronstable.sock"
    app._copy_badge_markdown(None)
    assert len(app.term.copied) == 2
    assert app.toasts[-1][0] == "fail"


def test_tui_palette_rows_follow_the_badges_answer(tmp_path):
    from tests.test_tui import _bare_app, _job

    app = _bare_app(tmp_path)
    app.jobs = [_job("backup")]
    app.by_name = {j["name"]: j for j in app.jobs}
    app.inputs["palette"] = "badge"

    def labels():
        return [label for _icon, label, _run in app.palette_matches()]

    # the memo must not hold on to rows built before /whoami answered
    assert not labels()
    app.badges = {"public": True, "dashboard": True}
    assert "Copy badge Markdown: backup" in labels()
    app.badges = None
    assert not labels()


async def test_tui_reads_the_badges_from_whoami(tmp_path):
    from cronstable import tui
    from tests.test_tui import _bare_app

    app = _bare_app(tmp_path)
    answers = []

    async def get_json(path):
        assert path == "/whoami"
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    app.api.get_json = get_json
    answers.append({"badges": {"public": False}})
    assert await app._load_badges()
    assert app.badges == {"public": False}
    # web.badges: false, and an older daemon, send no badges object
    for whoami in ({"badges": None}, {"authenticated": False}):
        answers.append(whoami)
        assert await app._load_badges()
        assert app.badges is None
    # a failed fetch, such as a 401 before the token is set, keeps the
    # last answer and says so
    app.badges = {"public": True}
    answers.append(RuntimeError("unreachable"))
    assert not await app._load_badges()
    assert app.badges == {"public": True}
    answers.append(tui.ApiError(503))
    assert not await app._load_badges()
    assert app.badges == {"public": True}
    # a daemon without /whoami answers for good: it serves no badges
    answers.append(tui.ApiError(404))
    assert await app._load_badges()
    assert app.badges is None


async def test_tui_reads_the_badges_again_after_a_failed_read(tmp_path):
    from tests.test_tui import Harness, _wait_for

    h = Harness()
    answer = {"public": True, "dashboard": True}
    h.daemon.whoami["badges"] = answer
    served = h.daemon._whoami
    calls = []

    async def flaky_whoami(request):
        calls.append(request.path)
        if len(calls) == 1:
            return web.Response(status=503)
        return await served(request)

    h.daemon._whoami = flaky_whoami
    app = await h.start(tmp_path)
    try:
        # the first good poll's read fails, and a later poll reads again
        await _wait_for(lambda: app.badges == answer)
        assert len(calls) == 2
        # once it has the answer, polling stops asking
        await asyncio.sleep(0.6)
        assert len(calls) == 2
    finally:
        await h.stop()


@pytest.mark.parametrize("failure", ["unauthorized", "unreachable"])
async def test_tui_reads_the_badges_again_after_a_failed_poll(
    tmp_path, failure
):
    # the daemon may come back with another web.badges setting, or answer
    # /whoami only once a new token is set
    from cronstable import tui
    from tests.test_tui import _bare_app

    app = _bare_app(tmp_path)
    app._badges_due = False

    async def get_json(path):
        if failure == "unauthorized":
            raise tui.Unauthorized()
        raise OSError("connection refused")

    app.api.get_json = get_json
    await app._poll_once()
    assert app._badges_due


async def _poll(app):
    # the /whoami read runs beside the poll: wait for it too
    await app._poll_once()
    await asyncio.gather(*app._background_tasks)


async def test_tui_reads_the_badges_again_a_minute_on(tmp_path):
    # polls keep succeeding across a reload that changes web.badges
    from tests.test_tui import _bare_app, _job

    app = _bare_app(tmp_path)
    whoami = {"badges": {"public": False, "dashboard": True}}
    reads = []

    async def get_json(path):
        if path == "/jobs":
            return [_job("backup")]
        if path == "/whoami":
            reads.append(path)
            return whoami
        return {}

    app.api.get_json = get_json
    app.inputs["palette"] = "badge"

    def labels():
        return [label for _icon, label, _run in app.palette_matches()]

    await _poll(app)
    assert len(reads) == 1
    assert "Copy badge Markdown: backup" in labels()
    # the reload turns badges off; a poll within the minute skips /whoami
    whoami["badges"] = None
    await _poll(app)
    assert len(reads) == 1
    assert "Copy badge Markdown: backup" in labels()
    # a minute on, the next good poll reads /whoami again
    app._badges_read -= 61
    await _poll(app)
    assert len(reads) == 2
    assert not labels()
    # a reload that makes the badges public changes the toast
    whoami["badges"] = {"public": True, "dashboard": True}
    app._badges_read -= 61
    await _poll(app)
    assert len(reads) == 3
    app._copy_badge_markdown("backup")
    assert app.toasts[-1][1] == "❏ copied badge Markdown"
    await _poll(app)
    assert len(reads) == 3


async def test_tui_first_load_lands_while_whoami_stalls(tmp_path):
    # the --job drawer, the first cluster view, and the verdict
    from tests.test_tui import _bare_app, _job, _stub_api, _wait_for

    app = _bare_app(tmp_path, start_job="backup")
    _stub_api(app)
    stub = app.api.get_json
    release = asyncio.Event()
    reads = []

    async def get_json(path, **k):
        if path == "/jobs":
            return [_job("backup", outcome="failure")]
        if path == "/whoami":
            reads.append(path)
            await release.wait()
            raise asyncio.TimeoutError()
        return await stub(path, **k)

    app.api.get_json = get_json
    await asyncio.wait_for(app._startup(), 2)
    assert app.open_overlays == ["drawer"]
    assert app.cluster is not None and app.verdict is not None
    assert reads == ["/whoami"] and app._badges_busy
    release.set()
    await _wait_for(lambda: not app._badges_busy)
    app.close("drawer")
    await asyncio.sleep(0.05)


async def test_tui_polls_land_while_whoami_stalls(tmp_path):
    from tests.test_tui import _bare_app, _job, _wait_for

    app = _bare_app(tmp_path)
    release = asyncio.Event()
    reads = []

    async def get_json(path):
        if path == "/jobs":
            return [_job("backup", outcome="failure")]
        if path == "/whoami":
            reads.append(path)
            await release.wait()
            raise asyncio.TimeoutError()
        return {"from": path}

    app.api.get_json = get_json
    await asyncio.wait_for(app._poll_once(), 2)
    assert app.cluster == {"from": "/cluster"}
    assert app.verdict is not None
    # a poll during the read neither waits on it nor starts another
    app.cluster = None
    await asyncio.wait_for(app._poll_once(), 2)
    assert app.cluster == {"from": "/cluster"}
    assert reads == ["/whoami"]
    # the read times out, and the next poll asks again
    release.set()
    await _wait_for(lambda: not app._badges_busy)
    assert app._badges_due
    await _poll(app)
    assert reads == ["/whoami", "/whoami"]


async def test_tui_reads_the_badges_again_after_a_poll_fails_mid_read(
    tmp_path,
):
    from tests.test_tui import _bare_app, _job, _wait_for

    app = _bare_app(tmp_path)
    release = asyncio.Event()
    down = False
    reads = []

    async def get_json(path):
        if down:
            raise OSError("connection refused")
        if path == "/whoami":
            reads.append(path)
            await release.wait()
            return {"badges": {"public": True, "dashboard": True}}
        return [_job("backup")] if path == "/jobs" else {}

    app.api.get_json = get_json
    await asyncio.wait_for(app._poll_once(), 2)
    assert app._badges_busy
    down = True
    await app._poll_once()
    down = False
    # the daemon is back before the read answers; no second read starts
    await asyncio.wait_for(app._poll_once(), 2)
    assert reads == ["/whoami"]
    release.set()
    await _wait_for(lambda: not app._badges_busy)
    assert app.badges == {"public": True, "dashboard": True}
    # its answer may predate the outage, so the next poll asks again
    assert app._badges_due
    await _poll(app)
    assert reads == ["/whoami", "/whoami"]
    assert not app._badges_due


async def test_tui_reads_the_badges_once_it_has_the_token(tmp_path):
    from tests.test_tui import Harness, _wait_for

    h = Harness()
    h.daemon.token = "s3cret"
    h.daemon.whoami["badges"] = {"public": False}
    app = await h.start(tmp_path)
    try:
        await _wait_for(lambda: app.conn_error == "unauthorized")
        assert app.badges is None
        app.inputs["token"] = "s3cret"
        await app._input_commit("token")
        # the first poll the token passes reads /whoami
        await _wait_for(lambda: app.badges == {"public": False})
    finally:
        await h.stop()
