"""Behavior and work counts for dashboard sorting and chart formatting."""

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as instance:
        yield instance


def test_rate_sort_reads_each_history_once_and_refreshes_keys(browser):
    html = e2e.INDEX.read_text(encoding="utf-8")
    rates = html[
        html.index("  function rateOf(") : html.index("  function rateCell(")
    ]
    sort = html[
        html.index("  function computeView(") : html.index(
            "  function lastTs("
        )
    ]
    page = browser.new_page()
    try:
        page.add_script_tag(
            content="""
          const state = {filter:'', statusFilter:'all', sort:'rate', sortDir:1};
          const _cmp = new Intl.Collator().compare;
        """
            + rates
            + sort
        )
        result = page.evaluate("""() => {
          const counts = {}, histories = {
            z: [], b: ['success'], a: ['success'],
            c: ['success', 'cancelled'], d: ['failure'],
          };
          state.jobs = Object.keys(histories).map(name => ({
            name,
            get history() {
              counts[name] = (counts[name] || 0) + 1;
              return histories[name].map(outcome => ({outcome}));
            },
          }));
          const orders = [], reads = [];
          for (const dir of [1, -1]) {
            for (const name in counts) counts[name] = 0;
            state.sortDir = dir;
            orders.push(computeView().map(j => j.name));
            reads.push({...counts});
          }
          histories.d.push('success', 'success', 'success');
          state.sortDir = 1;
          orders.push(computeView().map(j => j.name));
          return {orders, reads};
        }""")
        assert result["orders"] == [
            list("dcabz"),
            list("zbacd"),
            list("cdabz"),
        ]
        assert result["reads"] == [dict.fromkeys("zbacd", 1)] * 2
    finally:
        page.close()


@pytest.mark.parametrize(
    "locale,zone",
    [
        ("en-US", "America/New_York"),
        ("en-GB", "Europe/London"),
        ("ar-EG", "Africa/Cairo"),
        ("ja-JP", "Asia/Tokyo"),
    ],
)
def test_chart_axes_and_hover_share_one_local_time_formatter(
    browser, locale, zone
):
    html = e2e.INDEX.read_text(encoding="utf-8")
    escape = html[
        html.index("  const _ESC =") : html.index("  // One collator")
    ]
    charts = html[
        html.index("  function rcNiceCeil(") : html.index(
            "  // active chart groups"
        )
    ]
    page = browser.new_page(locale=locale, timezone_id=zone)
    try:
        page.set_content('<div id="charts" style="width:500px"></div>')
        page.add_script_tag(content=escape + charts)
        result = page.evaluate("""() => {
          const native = Intl.DateTimeFormat;
          let formats = 0;
          Intl.DateTimeFormat = new Proxy(native, {
            construct(target, args) { formats++; return Reflect.construct(target, args); },
          });
          try {
            const host = document.getElementById('charts');
            const points = [
              [Date.parse('2026-01-01T05:00:00Z') / 1000, 1],
              [Date.parse('2026-07-01T16:34:56Z') / 1000, 2],
            ];
            const series = [{pts:points, fmt:String, label:'cpu', color:'blue'}];
            rcRender(host, series);
            const axes = [...host.querySelectorAll('.xlab')].map(e => e.textContent);
            const expected = points.map(p => new Date(p[0] * 1000)
              .toLocaleTimeString([], {hour12:false}));
            const svg = host.querySelector('svg'), rect = svg.getBoundingClientRect();
            const hover = [];
            for (const clientX of [rect.left, rect.right, rect.left]) {
              svg.dispatchEvent(new MouseEvent('mousemove', {clientX, clientY:rect.top}));
              hover.push(host.querySelector('.rc-tip .t').textContent);
            }
            const afterHover = formats;
            rcRender(host, series);
            return {axes, expected, hover, afterHover, afterRedraw:formats};
          } finally { Intl.DateTimeFormat = native; }
        }""")
        assert result["axes"] == result["expected"]
        assert result["hover"] == [
            result["expected"][0],
            result["expected"][1],
            result["expected"][0],
        ]
        assert result["afterHover"] == 1
        assert result["afterRedraw"] == 2
    finally:
        page.close()
