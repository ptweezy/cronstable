"""Check the dashboard's ``resourceLimits`` views in a real browser.

A real daemon serves jobs that set ``resourceLimits``. A fake limits
manager stands in for cgroups and Job Objects, and the tests install run
records directly, so no test creates a cgroup or a Job Object. The checks
cover the caps chip in the drawer, the History tab's per-run markers
(including a verify check's memory kill), the cap lines on the Resources
charts (recorded and live runs), the caps-only Resources view of a capped
job without ``monitorResources``, and the Resources view of a monitored
job that has no runs yet.
"""

import datetime
import random
import re

import pytest

pytest.importorskip("playwright.sync_api")

from cronstable import limits, tui  # noqa: E402
from cronstable.config import format_size  # noqa: E402
from cronstable.cron import JobRunInfo  # noqa: E402
from cronstable.job import JobOutputStream  # noqa: E402
from cronstable.limits import Capability, LimitsRecord  # noqa: E402
from cronstable.resources import ResourceUsage  # noqa: E402
from tests import _web_e2e as e2e  # noqa: E402

MIB = 1024 * 1024
GIB = 1024 * MIB
MONITORED = "capped-monitored"
UNMONITORED = "capped-unmonitored"
FRACTIONAL = "capped-fractional"
SPARSE = "capped-sparse"
REFUSED = "capped-refused"
CHECKED = "capped-checked"
EMPTY = "monitored-empty"
CAPPED_EMPTY = "capped-empty"
UNCAPPED = "uncapped"
NO_SAMPLES = (
    "No resource samples yet. A run is charted here once it has been sampled."
)
CPU_REASON = "could not set cpu.max: Invalid argument"
DELEGATE_REASON = "cgroup /sys/fs/cgroup/x is not delegated"

# rcRender's chart geometry (H 92, PT 8, PB 16)
PT, BASE_Y, PLOT_H = 8, 76, 68


class _FakeManager:
    """The calls the scheduler makes when no capped job launches."""

    def __init__(self):
        self.converged = []

    def converge(self, capped):
        self.converged.append(list(capped))

    def capability(self):
        return Capability(
            available=True,
            mechanism="cgroup-v2",
            resources=frozenset({"cpu", "memory", "processes"}),
            reason=None,
            hint=None,
        )


def _jobs():
    return [
        e2e.job(
            MONITORED,
            "echo never",
            monitorResources=True,
            resourceLimits={"cpu": 1.5, "memory": "512M", "processes": 64},
        ),
        e2e.job(
            UNMONITORED,
            "echo never",
            resourceLimits={"memory": "256M", "required": True},
        ),
        e2e.job(
            FRACTIONAL,
            "echo never",
            monitorResources=True,
            resourceLimits={"cpu": 2.2, "memory": "12.5G"},
        ),
        e2e.job(
            SPARSE,
            "echo never",
            monitorResources=True,
            resourceLimits={"memory": "256M", "required": True},
        ),
        e2e.job(
            REFUSED,
            "echo never",
            monitorResources=True,
            resourceLimits={"memory": "256M", "required": True},
        ),
        e2e.job(CHECKED, "echo never", resourceLimits={"memory": "96M"}),
        e2e.job(EMPTY, "echo never", monitorResources=True),
        e2e.job(
            CAPPED_EMPTY,
            "echo never",
            monitorResources=True,
            resourceLimits={
                "cpu": 0.0125,
                "memory": "512M",
                "processes": 1,
                "required": True,
            },
        ),
        e2e.job(UNCAPPED, "echo never"),
    ]


def _record(**over):
    fields = {
        "cpu": None,
        "memory_bytes": None,
        "processes": None,
        "required": False,
        "enforced": True,
        "mechanism": "cgroup-v2",
        "unenforced": (),
        "reason": None,
    }
    fields.update(over)
    return LimitsRecord(**fields)


_START = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
    hours=1
)


def _run(minute, *, outcome="success", exit_code=0, fail_reason=None, **kw):
    start = _START + datetime.timedelta(minutes=minute)
    return JobRunInfo(
        outcome=outcome,
        exit_code=exit_code,
        started_at=start,
        finished_at=start + datetime.timedelta(seconds=20),
        fail_reason=fail_reason,
        output=JobOutputStream(),
        **kw,
    )


def _usage(minute, cpu_peak, rss_peak):
    """Ten samples climbing to the given CPU% and RSS peaks."""
    t0 = (_START + datetime.timedelta(minutes=minute)).timestamp()
    series = [
        [t0 + 2 * k, cpu_peak * (k + 1) / 10, int(rss_peak * (k + 1) / 10)]
        for k in range(10)
    ]
    return ResourceUsage(15.0, 2.0, rss_peak, len(series), series=series)


_MONITORED_CAPS = {"cpu": 1.5, "memory_bytes": 512 * MIB, "processes": 64}


def _monitored_runs():
    return [
        _run(
            1,
            resource_usage=_usage(1, 60.0, 200 * MIB),
            limits=_record(
                **_MONITORED_CAPS,
                process_limit_hits=0,
                peak_memory_bytes=260 * MIB,
                cpu_throttled_seconds=0.5,
            ),
        ),
        _run(
            2,
            resource_usage=_usage(2, 90.0, 240 * MIB),
            limits=_record(
                **_MONITORED_CAPS,
                enforced=False,
                unenforced=("cpu",),
                reason=CPU_REASON,
                process_limit_hits=0,
                peak_memory_bytes=250 * MIB,
            ),
        ),
        _run(
            3,
            outcome="failure",
            exit_code=-9,
            fail_reason="memory limit exceeded (512 MiB)",
            resource_usage=_usage(3, 140.0, 480 * MIB),
            limits=_record(
                **_MONITORED_CAPS,
                memory_exceeded=True,
                process_limit_hits=2,
                peak_memory_bytes=512 * MIB,
                cpu_throttled_seconds=2.5,
            ),
        ),
    ]


_REQUIRED_CAPS = {"memory_bytes": 256 * MIB, "required": True}


def _refused(minute):
    """A required run the host could not cap: it never spawned."""
    record = _record(
        **_REQUIRED_CAPS,
        enforced=False,
        mechanism=None,
        unenforced=("memory",),
        reason=DELEGATE_REASON,
        refused=True,
    )
    return _run(
        minute,
        outcome="failure",
        exit_code=127,
        fail_reason=record.failure,
        limits=record,
    )


# 12.5G, a fractional cap of 10 or more units
_FRACTIONAL_CAPS = {"cpu": 2.2, "memory_bytes": 12 * GIB + 512 * MIB}


def _fractional_record():
    return _record(
        **_FRACTIONAL_CAPS,
        memory_exceeded=True,
        peak_memory_bytes=_FRACTIONAL_CAPS["memory_bytes"],
    )


def _fractional_runs():
    record = _fractional_record()
    return [
        _run(
            1,
            outcome="failure",
            exit_code=-9,
            fail_reason=record.failure,
            resource_usage=_usage(1, 100.0, 12 * GIB),
            limits=record,
        )
    ]


def _sparse_runs():
    """Three sampled runs, then a refused one with no samples."""
    caps = _record(**_REQUIRED_CAPS, peak_memory_bytes=120 * MIB)
    return [
        _run(1, resource_usage=_usage(1, 40.0, 100 * MIB), limits=caps),
        _run(2, resource_usage=_usage(2, 50.0, 110 * MIB), limits=caps),
        _run(3, resource_usage=_usage(3, 60.0, 120 * MIB), limits=caps),
        _refused(4),
    ]


def _unmonitored_runs():
    caps = _REQUIRED_CAPS
    return [
        _refused(0),
        _run(1, limits=_record(**caps, peak_memory_bytes=100 * MIB)),
        _run(
            2,
            outcome="failure",
            exit_code=-9,
            fail_reason="memory limit exceeded (256 MiB)",
            limits=_record(
                **caps, memory_exceeded=True, peak_memory_bytes=256 * MIB
            ),
        ),
    ]


_CHECK_CAPS = {"memory_bytes": 96 * MIB}


def _checked_runs():
    """A run whose verify check was killed at its memory cap, then one whose
    check passed. Each check carries its own limits record; the run's own
    record stays within the cap."""
    killed = _record(
        **_CHECK_CAPS, memory_exceeded=True, peak_memory_bytes=96 * MIB
    )
    passed = _record(**_CHECK_CAPS, peak_memory_bytes=20 * MIB)
    run_record = _record(**_CHECK_CAPS, peak_memory_bytes=8 * MIB)
    return [
        _run(
            1,
            outcome="failure",
            fail_reason="verification failed: " + killed.failure,
            limits=run_record,
            verification={
                "outcome": "failure",
                "exit_code": -9,
                "fail_reason": killed.failure,
                "limits": killed.to_dict(),
            },
        ),
        _run(
            2,
            limits=run_record,
            verification={
                "outcome": "success",
                "exit_code": 0,
                "fail_reason": None,
                "limits": passed.to_dict(),
            },
        ),
    ]


async def _record_runs(cron, name, runs):
    for info in runs:
        cron._record_run(name, info)


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


@pytest.fixture(scope="module")
def daemon(tmp_path_factory):
    previous = limits._MANAGER
    fake = _FakeManager()
    limits.set_manager(fake)
    d = e2e.Daemon(tmp_path_factory.mktemp("limits"), jobs=_jobs())
    try:
        d.start()
        e2e.wait_until(lambda: fake.converged)
        d.call(_record_runs(d.cron, MONITORED, _monitored_runs()))
        d.call(_record_runs(d.cron, UNMONITORED, _unmonitored_runs()))
        d.call(_record_runs(d.cron, FRACTIONAL, _fractional_runs()))
        d.call(_record_runs(d.cron, SPARSE, _sparse_runs()))
        d.call(_record_runs(d.cron, REFUSED, [_refused(1)]))
        d.call(_record_runs(d.cron, CHECKED, _checked_runs()))
        yield d
    finally:
        d.stop()
        limits.set_manager(previous)


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _open_drawer(page, name, tab=None):
    if page.evaluate(
        "document.getElementById('drawer').getAttribute('aria-hidden')"
        " === 'false'"
    ):
        page.keyboard.press("Escape")
        page.wait_for_selector('#drawer[aria-hidden="true"]')
    _click(page, '#rows [data-logs="{}"]'.format(name))
    page.wait_for_selector('#drawer[aria-hidden="false"]')
    page.wait_for_function(
        "(n) => document.getElementById('dName').textContent === n", arg=name
    )
    # the drawer slides in; pointer positions need it at rest
    page.wait_for_function(
        "Math.abs(document.getElementById('drawer').getBoundingClientRect()"
        ".right - document.documentElement.clientWidth) < 1"
    )
    if tab is not None:
        _show_tab(page, tab)


def _show_tab(page, tab):
    _click(page, '#dTabs button[data-tab="{}"]'.format(tab))
    page.wait_for_selector('.pane.active[data-pane="{}"]'.format(tab))


def _caps_chip(page):
    return page.evaluate(
        """() => {
          const c = document.querySelector('#dMeta .ha-chip.caps');
          return c ? { text: c.textContent, title: c.title } : null;
        }"""
    )


def _nice_ceil(v):
    """rcNiceCeil: the smallest 1/1.25/1.5/2/2.5/4/5/7.5/10 step >= v."""
    import math

    p = 10 ** math.floor(math.log10(v))
    for m in (1, 1.25, 1.5, 2, 2.5, 4, 5, 7.5, 10):
        if m * p >= v:
            return m * p
    return 10 * p


def _cap_y(cap, data_peak):
    vmax = _nice_ceil(max(cap, data_peak) * 1.12)
    return BASE_Y - cap / vmax * PLOT_H


_CHARTS = """() => [...document.querySelectorAll('#resCharts .rchart')]
  .map((c) => {
    const line = c.querySelector('line.rc-cap');
    const lab = c.querySelector('text.rc-caplab');
    return {
      line: line ? { y1: +line.getAttribute('y1'),
                     y2: +line.getAttribute('y2'),
                     cls: line.getAttribute('class') } : null,
      label: lab ? lab.textContent : null,
      labelY: lab ? +lab.getAttribute('y') : null,
      aria: c.querySelector('svg').getAttribute('aria-label'),
    };
  })"""

# Hovers the middle of chart ``ci`` and reads the tip in the same task, so
# a live view's refresh cannot redraw the charts in between.
_HOVER_TIP = """(ci) => {
  const svg = document.querySelector(
    "#resCharts .rchart[data-ci='" + ci + "'] svg");
  const r = svg.getBoundingClientRect();
  svg.dispatchEvent(new MouseEvent('mousemove',
    { clientX: r.left + r.width / 2, clientY: r.top + 30 }));
  return document.querySelector('#resCharts .rc-tip').textContent;
}"""

_TREND = """() => [...document.querySelectorAll(
    '#resourcesPane .res-trend .barchart')].map((strip) =>
  [...strip.querySelectorAll('i')].map((bar) => ({
    run: +bar.getAttribute('data-runsel'),
    title: bar.title,
    sel: bar.classList.contains('sel'),
  })))"""


def _active_selection(page):
    return page.evaluate(
        "document.querySelector('#resourcesPane .res-chip.active')"
        ".getAttribute('data-sel')"
    )


def _select_run(page, index):
    _click(page, '#resourcesPane [data-sel="run:{}"]'.format(index))
    page.wait_for_selector(
        '#resourcesPane [data-sel="run:{}"].active'.format(index)
    )
    page.wait_for_selector("#resCharts svg")


def test_drawer_caps_chip_names_the_set_caps(browser, daemon):
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, MONITORED)
        chip = _caps_chip(page)
        assert chip["text"] == "caps 1.5 CPU · 512 MiB · 64 procs"
        assert "goes ahead uncapped" in chip["title"]

        _open_drawer(page, UNMONITORED)
        chip = _caps_chip(page)
        assert chip["text"] == "caps 256 MiB"
        assert chip["title"].startswith("resourceLimits.required")

        # CPUs to six decimals and a singular process, as the TUI reads them
        _open_drawer(page, CAPPED_EMPTY)
        chip = _caps_chip(page)
        assert chip["text"] == "caps 0.0125 CPU · 512 MiB · 1 proc"
        assert chip["text"] == "caps " + tui.caps_text(
            {"cpu": 0.0125, "memory_bytes": 512 * MIB, "processes": 1}
        )

        _open_drawer(page, UNCAPPED)
        assert _caps_chip(page) is None


_REASONS = """() => [...document.querySelectorAll(
    '#historyPane .runtable tbody tr')].map((tr) => {
  const cell = tr.querySelector('td.reason');
  return {
    text: cell.textContent,
    chips: [...cell.querySelectorAll('.ha-chip')].map((c) => ({
      cls: c.className, text: c.textContent, title: c.title,
    })),
  };
})"""


def test_history_reason_cell_marks_memory_kills_and_unenforced_caps(
    browser, daemon
):
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, MONITORED, "history")
        page.wait_for_selector("#historyPane .runtable tbody tr")
        rows = page.evaluate(_REASONS)
        # newest first: the memory kill, the unenforced run, the clean run
        assert len(rows) == 3
        hit, off, clean = rows
        assert hit["chips"] == [
            {
                "cls": "ha-chip cap-hit",
                "text": "memory cap",
                "title": "killed at its memory cap (512 MiB)",
            }
        ]
        assert "memory limit exceeded (512 MiB)" in hit["text"]
        assert off["chips"] == [
            {
                "cls": "ha-chip cap-off",
                "text": "caps not enforced",
                "title": "caps not enforced (cpu): " + CPU_REASON,
            }
        ]
        assert clean["chips"] == []
        assert clean["text"] == ""


def test_history_reason_cell_marks_a_verify_checks_memory_kill(
    browser, daemon
):
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, CHECKED, "history")
        page.wait_for_selector("#historyPane .runtable tbody tr")
        # newest first: the passing check, then the check killed at its cap
        passed, killed = page.evaluate(_REASONS)
        assert passed["chips"] == []
        # the run's own record stayed within the cap; the check's did not
        assert killed["chips"] == [
            {
                "cls": "ha-chip cap-hit",
                "text": "check memory cap",
                "title": "verify check killed at its memory cap (96 MiB)",
            }
        ]
        assert (
            "verification failed: memory limit exceeded (96 MiB)"
            in killed["text"]
        )


def test_resource_charts_draw_cap_lines_inside_the_frame(browser, daemon):
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, MONITORED, "resources")
        # a horizontal line has no height, so it never counts as visible
        page.wait_for_selector("#resCharts line.rc-cap", state="attached")
        # the newest run is selected: the memory kill
        cpu, mem = page.evaluate(_CHARTS)
        assert cpu["label"] == "cap 150%"
        assert mem["label"] == "cap 512 MiB · hit"
        assert mem["line"]["cls"] == "rc-cap hit"
        assert cpu["line"]["cls"] == "rc-cap"
        # each cap folds into its chart's y ceiling, so the line sits
        # inside the frame at cap / vmax of the plot height
        assert cpu["line"]["y1"] == cpu["line"]["y2"]
        assert cpu["line"]["y1"] == pytest.approx(_cap_y(150, 140), abs=0.06)
        assert mem["line"]["y1"] == pytest.approx(
            _cap_y(512 * MIB, 480 * MIB), abs=0.06
        )
        for chart in (cpu, mem):
            assert PT < chart["line"]["y1"] < BASE_Y
        assert mem["aria"] == "memory (rss) over time, cap 512 MiB · hit"

        note = page.evaluate(
            "document.querySelector('#resourcesPane .res-note').textContent"
        )
        for part in (
            "memory cap hit",
            "kernel peak 512 MiB",
            "cpu throttled 2.50s",
            "process cap hit 2×",
        ):
            assert part in note, (part, note)
        assert "not enforced" not in note

        # the hover row names each cap; the memory one says what it counts
        box = page.locator(
            "#resCharts .rchart[data-ci='1'] svg"
        ).bounding_box()
        page.mouse.move(box["x"] + box["width"] / 2, box["y"] + 30)
        page.wait_for_function(
            "getComputedStyle(document.querySelector('#resCharts .rc-tip'))"
            ".visibility === 'visible'"
        )
        tip = page.evaluate(
            "document.querySelector('#resCharts .rc-tip').textContent"
        )
        assert "cap 150%" in tip
        assert "cap 512 MiB (kernel-counted, not RSS)" in tip

        # the run whose CPU cap did not apply says so on that line and in
        # the hover row
        _select_run(page, 1)
        cpu, mem = page.evaluate(_CHARTS)
        assert cpu["label"] == "cap 150% (not enforced)"
        assert mem["label"] == "cap 512 MiB"
        assert mem["line"]["cls"] == "rc-cap"
        tip = page.evaluate(_HOVER_TIP, 0)
        assert "cap 150% (not enforced)" in tip
        assert "cap 512 MiB (kernel-counted, not RSS)" in tip
        note = page.evaluate(
            "document.querySelector('#resourcesPane .res-note').textContent"
        )
        assert "caps not enforced (cpu): " + CPU_REASON in note
        assert "memory cap hit" not in note

        # the clean run: plain caps, its own data peaks set the ceiling
        _select_run(page, 0)
        cpu, mem = page.evaluate(_CHARTS)
        assert (cpu["label"], mem["label"]) == ("cap 150%", "cap 512 MiB")
        assert mem["line"]["y1"] == pytest.approx(
            _cap_y(512 * MIB, 200 * MIB), abs=0.06
        )
        # the selector chip of the memory kill says so in its title
        title = page.evaluate(
            "document.querySelector('#resourcesPane [data-sel=\"run:2\"]')"
            ".title"
        )
        assert title.endswith("· memory cap hit")


def test_capped_job_without_monitoring_lists_its_runs_limits(browser, daemon):
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, UNMONITORED, "resources")
        page.wait_for_selector("#resourcesPane .res-limits")
        view = page.evaluate(
            """() => {
              const pane = document.getElementById('resourcesPane');
              const table = pane.querySelector('.res-limits');
              return {
                caps: pane.querySelector('.res-caps').textContent,
                head: [...table.querySelectorAll('th')]
                  .map((th) => th.textContent),
                rows: [...table.querySelectorAll('tbody tr')].map((tr) =>
                  [...tr.querySelectorAll('td')].map((td) => ({
                    text: td.textContent.trim(),
                    title: (td.querySelector('[title]') || {}).title || '',
                  }))),
                charts: !!pane.querySelector('#resCharts'),
                text: pane.textContent,
              };
            }"""
        )
        assert view["caps"] == "caps 256 MiB · required"
        assert view["head"] == [
            "Started",
            "Outcome",
            "Enforced",
            "Kernel peak",
            "CPU throttled",
            "Cap hits",
        ]
        assert not view["charts"]
        assert "monitorResources: true" in view["text"]
        assert "Resource monitoring is off" not in view["text"]
        # newest first: the memory kill, the clean run, the refused run
        hit, clean, refused = view["rows"]
        assert [c["text"] for c in hit[1:]] == [
            "✕ failure",
            "yes",
            "256 MiB",
            "—",
            "memory",
        ]
        assert [c["text"] for c in clean[1:]] == [
            "● success",
            "yes",
            "100 MiB",
            "—",
            "—",
        ]
        assert refused[2]["text"] == "refused"
        assert refused[2]["title"] == (
            "caps not enforced (memory): " + DELEGATE_REASON
        )
        assert refused[3]["text"] == "—"

        # an uncapped job without monitoring keeps the opt-in screen
        _open_drawer(page, UNCAPPED, "resources")
        page.wait_for_function(
            "document.getElementById('resourcesPane').textContent"
            ".includes('Resource monitoring is off')"
        )


def _cap_sizes():
    """Byte counts that probe format_size: unit edges, exact ties at the
    first decimal, the fractional caps 12.5G, 10.5M, 100.5M and 1.25G,
    and a seeded sample below 2**53."""
    sizes = [0, 1, 1023, 1024, 1025, 1048535, 2**53 - 1]
    for unit in range(1, 5):
        step = 1024**unit
        for twentieths in (1, 3, 5, 25, 249, 251, 20479, 20480, 20481):
            exact = twentieths * step // 20
            sizes += [exact - 1, exact, exact + 1]
    sizes += [
        12 * GIB + 512 * MIB,
        10 * MIB + 512 * 1024,
        100 * MIB + 512 * 1024,
        GIB + 256 * MIB,
    ]
    rng = random.Random(1234)
    sizes += [
        rng.randrange(2**bits) for bits in range(11, 54) for _ in range(20)
    ]
    return sizes


def test_configured_sizes_read_as_format_size_writes_them(browser):
    """fmtCap, the page's copy of config.format_size, agrees with it."""
    html = e2e.INDEX.read_text(encoding="utf-8")
    script = html[
        html.index("  function fmtCap(") : html.index("  function capsText(")
    ]
    sizes = _cap_sizes()
    page = browser.new_page()
    try:
        page.set_content("<p></p>")
        page.add_script_tag(content=script)
        got = page.evaluate("(sizes) => sizes.map((n) => fmtCap(n))", sizes)
        junk = page.evaluate(
            "[null, undefined, 'x', NaN, Infinity].map((n) => fmtCap(n))"
        )
    finally:
        page.close()
    want = [format_size(n) for n in sizes]
    assert len(got) == len(sizes)
    assert [
        (n, g, w) for n, g, w in zip(sizes, got, want, strict=True) if g != w
    ] == []
    assert junk == ["—"] * 5


def test_fractional_caps_read_as_the_daemon_writes_them(browser, daemon):
    reason = _fractional_record().failure
    assert reason == "memory limit exceeded (12.5 GiB)"
    with e2e.open_page(browser, daemon.url) as page:
        _open_drawer(page, FRACTIONAL)
        assert _caps_chip(page)["text"] == "caps 2.2 CPU · 12.5 GiB"

        _show_tab(page, "history")
        page.wait_for_selector("#historyPane td.reason .ha-chip.cap-hit")
        cell = page.evaluate(
            """() => {
              const td = document.querySelector('#historyPane td.reason');
              return { text: td.textContent,
                       title: td.querySelector('.ha-chip.cap-hit').title };
            }"""
        )
        assert cell["text"].endswith(reason)
        assert cell["title"] == "killed at its memory cap (12.5 GiB)"
        # the chip's tooltip and the failure reason name the same size
        size = re.compile(r"\([^()]*\)$")
        assert (
            size.search(cell["title"]).group() == size.search(reason).group()
        )

        _show_tab(page, "resources")
        page.wait_for_selector("#resCharts line.rc-cap", state="attached")
        cpu, mem = page.evaluate(_CHARTS)
        assert cpu["label"] == "cap 220%"
        assert mem["label"] == "cap 12.5 GiB · hit"
        assert mem["aria"] == "memory (rss) over time, cap 12.5 GiB · hit"
        # a cap line near the top of the frame takes its label below it
        assert cpu["line"]["y1"] == pytest.approx(_cap_y(220, 100), abs=0.06)
        assert cpu["line"]["y1"] < PT + 12
        assert cpu["labelY"] == pytest.approx(cpu["line"]["y1"] + 11)
        assert mem["line"]["y1"] == pytest.approx(
            _cap_y(_FRACTIONAL_CAPS["memory_bytes"], 12 * GIB), abs=0.06
        )
        assert mem["line"]["y1"] >= PT + 12
        assert mem["labelY"] == pytest.approx(mem["line"]["y1"] - 3)
        tip = page.evaluate(_HOVER_TIP, 1)
        assert "cap 220%" in tip
        assert "cap 12.5 GiB (kernel-counted, not RSS)" in tip
        # the kernel peak equals the cap and reads as the cap does
        note = page.evaluate(
            "document.querySelector('#resourcesPane .res-note').textContent"
        )
        assert "kernel peak 12.5 GiB" in note
        assert "13 GiB" not in note


def _cpu_counts():
    """CPU caps that probe "%f": six-decimal ties (odd multiples of 1/128),
    values past six decimals, the range ends, and a seeded sample."""
    cpus = [0.01, 0.0125, 0.1, 1, 1.5, 2.2, 1 / 3, 1_000_000]
    cpus += [1.0000004, 1.0000005, 1.0000006, 2.9999995, 0.0199999]
    cpus += [m / 128 for m in range(1, 2048, 2)]
    rng = random.Random(4321)
    cpus += [
        round(rng.uniform(0.01, 64), rng.randrange(1, 9)) for _ in range(400)
    ]
    cpus += [rng.uniform(0.01, 1_000_000) for _ in range(200)]
    return cpus


def test_caps_text_reads_as_the_tui_writes_it(browser):
    """capsText, the drawer chip's text, agrees with tui.caps_text."""
    html = e2e.INDEX.read_text(encoding="utf-8")
    script = html[
        html.index("  function fmtCpus(") : html.index("  function capsHead(")
    ]
    caps = [{"cpu": c} for c in _cpu_counts()]
    caps += [{"processes": p} for p in (1, 2, 64, 4194304)]
    caps += [
        {"cpu": 1.5, "memory_bytes": 512 * MIB, "processes": 64},
        {"cpu": 0.0125, "memory_bytes": 12 * GIB + 512 * MIB, "processes": 1},
        {"memory_bytes": GIB + 256 * MIB},
    ]
    page = browser.new_page()
    try:
        page.set_content("<p></p>")
        page.add_script_tag(content=script)
        got = page.evaluate("(caps) => caps.map((c) => capsText(c))", caps)
    finally:
        page.close()
    want = [tui.caps_text(c) for c in caps]
    assert len(got) == len(caps)
    assert [
        (c, g, w) for c, g, w in zip(caps, got, want, strict=True) if g != w
    ] == []
    # the tie cases differ from toFixed, which rounds them up
    assert got[caps.index({"cpu": 1 / 128})] == "0.007812 CPU"
    assert got[caps.index({"processes": 1})] == "1 proc"


def test_resources_tab_leaves_unsampled_runs_out_of_the_charts(
    browser, daemon
):
    with e2e.open_page(browser, daemon.url) as page:
        # the newest run was refused; the tab opens on the newest sampled one
        _open_drawer(page, SPARSE, "resources")
        page.wait_for_selector("#resCharts line.rc-cap", state="attached")
        assert _active_selection(page) == "run:2"
        cpu, mem = page.evaluate(_CHARTS)
        assert cpu["line"] is None
        assert mem["label"] == "cap 256 MiB"
        # the trend strips hold the sampled runs only
        cpu_bars, mem_bars = page.evaluate(_TREND)
        for strip in (cpu_bars, mem_bars):
            assert [bar["run"] for bar in strip] == [0, 1, 2]
            assert [bar["sel"] for bar in strip] == [False, False, True]
        assert [bar["title"].split(" · ")[1] for bar in cpu_bars] == [
            "17.0s"
        ] * 3
        assert [bar["title"].split(" · ")[1] for bar in mem_bars] == [
            "100 MiB",
            "110 MiB",
            "120 MiB",
        ]

        # the refused run's chip still opens its limits record
        _click(page, '#resourcesPane [data-sel="run:3"]')
        page.wait_for_selector('#resourcesPane [data-sel="run:3"].active')
        pane = page.evaluate(
            """() => ({
              charts: document.getElementById('resCharts').textContent,
              note: document.querySelector('#resourcesPane .res-note')
                .textContent,
            })"""
        )
        assert pane["charts"] == "no samples yet"
        assert "caps not enforced (memory): " + DELEGATE_REASON in pane["note"]
        for strip in page.evaluate(_TREND):
            assert [bar["sel"] for bar in strip] == [False] * 3

        # with no sampled run at all, the tab opens on the newest run
        _open_drawer(page, REFUSED, "resources")
        page.wait_for_selector("#resourcesPane .res-chip.active")
        assert _active_selection(page) == "run:0"
        assert page.evaluate(_TREND) == []
        assert (
            page.evaluate("document.getElementById('resCharts').textContent")
            == "no samples yet"
        )


def _with_live(body):
    """The daemon's /resources answer plus one live instance."""
    t0 = datetime.datetime.now(datetime.timezone.utc).timestamp() - 20
    body["live"] = [
        {
            "started_at": "2026-01-01T00:00:00+00:00",
            "pid": 4242,
            "current": {
                "cpu_seconds": 3.0,
                "cpu_percent": 80.0,
                "rss_bytes": 300 * MIB,
            },
            "series": [
                [t0 + 2 * k, 8.0 * (k + 1), 30 * MIB * (k + 1)]
                for k in range(10)
            ],
        }
    ]
    return body


def _capability(available, resources):
    return {
        "available": available,
        "mechanism": "cgroup-v2" if available else None,
        "resources": resources,
        "reason": None if available else "no cgroup v2 here",
        "hint": None,
    }


def _wait_cap_labels(page, want):
    page.wait_for_function(
        "(want) => JSON.stringify([...document.querySelectorAll("
        "'#resCharts text.rc-caplab')].map((t) => t.textContent))"
        " === JSON.stringify(want)",
        arg=want,
    )


def test_live_cap_lines_tag_the_caps_this_host_cannot_apply(browser, daemon):
    node = {"node_name": "n1", "resources": None}

    def routes(page):
        page.faults.json(r"/node", lambda request: node)
        page.faults.rewrite(
            r"/jobs/{}/resources".format(MONITORED), _with_live
        )

    with e2e.open_page(
        browser,
        daemon.url,
        prefs={"pollMs": 1000, "resMs": 1000},
        before_goto=routes,
    ) as page:
        _open_drawer(page, MONITORED, "resources")
        page.wait_for_selector('#resourcesPane [data-sel="live:0"].active')
        # a /node answer without a capability tags nothing
        _wait_cap_labels(page, ["cap 150%", "cap 512 MiB"])

        # no cpu controller: the cpu cap goes unapplied
        node["limits"] = _capability(True, ["memory", "processes"])
        _wait_cap_labels(page, ["cap 150% (not enforced)", "cap 512 MiB"])
        tip = page.evaluate(_HOVER_TIP, 0)
        assert "cap 150% (not enforced)" in tip
        assert "cap 512 MiB (kernel-counted, not RSS)" in tip

        # a host that caps nothing: every cap goes unapplied
        node["limits"] = _capability(False, [])
        _wait_cap_labels(
            page, ["cap 150% (not enforced)", "cap 512 MiB (not enforced)"]
        )
        tip = page.evaluate(_HOVER_TIP, 1)
        assert "cap 512 MiB (not enforced)" in tip
        assert "kernel-counted" not in tip

        node["limits"] = _capability(True, ["cpu", "memory", "processes"])
        _wait_cap_labels(page, ["cap 150%", "cap 512 MiB"])


_EMPTY_VIEW = """() => {
  const pane = document.getElementById('resourcesPane');
  const caps = pane.querySelector('.res-caps');
  return {
    caps: caps ? caps.textContent : null,
    empty: [...pane.querySelectorAll('.term-empty')]
      .map((e) => e.textContent),
    charts: !!pane.querySelector('#resCharts'),
  };
}"""


def _wait_no_samples(page):
    page.wait_for_function(
        "(text) => document.getElementById('resourcesPane').textContent"
        ".includes(text)",
        arg=NO_SAMPLES,
    )


def test_resources_tab_of_a_monitored_job_with_no_runs(browser, daemon):
    live = {"on": False}

    def routes(page):
        page.faults.rewrite(
            r"/jobs/{}/resources".format(EMPTY),
            lambda body: _with_live(body) if live["on"] else body,
        )

    with e2e.open_page(
        browser, daemon.url, prefs={"resMs": 1000}, before_goto=routes
    ) as page:
        # a capped job keeps its caps line over the empty state
        _open_drawer(page, CAPPED_EMPTY, "resources")
        _wait_no_samples(page)
        assert page.evaluate(_EMPTY_VIEW) == {
            "caps": "caps 0.0125 CPU · 512 MiB · 1 proc · required",
            "empty": [NO_SAMPLES],
            "charts": False,
        }

        _open_drawer(page, EMPTY, "resources")
        _wait_no_samples(page)
        assert page.evaluate(_EMPTY_VIEW) == {
            "caps": None,
            "empty": [NO_SAMPLES],
            "charts": False,
        }
        # the pane keeps refetching: the first live instance replaces it
        live["on"] = True
        page.wait_for_selector('#resourcesPane [data-sel="live:0"].active')
        page.wait_for_selector("#resCharts svg")
        assert page.log.page_errors == []
