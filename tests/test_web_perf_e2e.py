"""Behavior and work counts for the dashboard's sorting, chart formatting,
polls, per-second tick, and panels.

The count tests open the page with ``?perf=1`` and drive it through
``window.__perf`` and its own controls. Each one pins the work a steady
state does, such as the rows a poll rebuilds or the DOM writes a tick makes.
"""

import datetime

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as instance:
        yield instance


@pytest.fixture(scope="module")
def daemon(tmp_path_factory):
    with e2e.Daemon(tmp_path_factory.mktemp("perf")) as instance:
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


def _refresh(page):
    page.evaluate("document.getElementById('refreshBtn').click()")
    page.wait_for_function(
        "!document.getElementById('refreshBtn').classList.contains('spin')"
    )


_ANCHOR = """() => {
  const state = window.__perf.state();
  return {
    anchor: state.fetchedAt,
    // the age of the held jobs on the monotonic and on the wall clock
    ages: [performance.now() - state.fetchedAt,
      Date.now() - state.fetchedWallAt],
    targets: state.jobs.map(
      (j) => state.fetchedWallAt + j.scheduled_in * 1000),
    kept: [...document.querySelectorAll('#rows tr[data-job]')]
      .filter((tr) => tr.__kept).length,
    conn: document.getElementById('conn').title,
  };
}"""


def test_a_revalidated_poll_keeps_the_countdown_anchor(browser, tmp_path):
    """The daemon answers an unchanged ``/jobs`` with 304. Each countdown
    belongs to the response that carried it, so the fire instants and the
    rows stay as they were, and the connection readout still counts the
    poll as a response. When the two clocks disagree on the age of that
    response, as they do after a suspend that stops the monotonic clock,
    the page asks for the body again and takes its anchors from it. Every
    poll bypasses the browser's HTTP cache."""
    jobs = [e2e.job("job%d" % i, schedule="%d 3 1 1 *" % i) for i in range(6)]
    polls = []

    def before_goto(page):
        # The page's clock is installed before the first poll, so both
        # anchors of that poll are read from it. It starts at midday in
        # the page's zone, so the test stays inside one local day.
        page.clock.install(time="2026-01-01T12:00:00Z")
        page.on(
            "request",
            lambda request: (
                request.url.endswith("/jobs") and polls.append(request)
            ),
        )

    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            prefs={"pollMs": 0},
            before_goto=before_goto,
            timezone_id="UTC",
        ) as page:
            statuses = []
            page.on(
                "response",
                lambda response: (
                    response.url.endswith("/jobs")
                    and statuses.append(response.status)
                ),
            )
            page.evaluate(
                "document.querySelectorAll('#rows tr[data-job]')"
                ".forEach((tr) => { tr.__kept = true; })"
            )
            before = page.evaluate(_ANCHOR)
            assert before["kept"] == len(jobs)
            # A minute on the page's clock: a countdown stamped again at
            # this instant would name the next minute in every row.
            page.clock.fast_forward(61000)
            _refresh(page)
            after = page.evaluate(_ANCHOR)
            assert statuses == [304]
            assert after["targets"] == before["targets"]
            assert after["ages"][1] >= 61000
            assert abs(after["ages"][0] - after["ages"][1]) < 5
            assert after["kept"] == len(jobs)
            assert "minute" not in after["conn"]

            # An hour passes on the wall clock alone, as it does while a
            # suspended machine holds the monotonic clock still, and as a
            # wall clock set an hour ahead reads.  Neither anchor can say
            # which, so the poll leaves the validator out.
            page.evaluate(
                """() => {
                  const monotonic = performance.now.bind(performance);
                  performance.now = () => monotonic() - 3600000;
                }"""
            )
            page.clock.fast_forward(3600000)
            assert page.evaluate(_ANCHOR)["ages"][0] < 3600000
            _refresh(page)
            woken = page.evaluate(_ANCHOR)
            assert statuses == [304, 200]
            assert 0 <= woken["ages"][0] < 60000
            assert abs(woken["ages"][0] - woken["ages"][1]) < 5
            # the monotonic anchor is a body's arrival, so the code that
            # reads a positive one as "a poll has landed" still does
            assert woken["anchor"] > 0

            # the anchors agree again, and so the next poll revalidates
            _refresh(page)
            assert statuses == [304, 200, 304]
            assert page.evaluate(_ANCHOR)["targets"] == woken["targets"]

            # all_headers() includes the headers the browser adds on the
            # wire. A fetch that bypasses the HTTP cache carries no-cache,
            # and the page-load and wake polls name no validator.
            sent = [request.all_headers() for request in polls]
            assert [h.get("cache-control") for h in sent] == ["no-cache"] * 4
            assert ["if-none-match" in h for h in sent] == [
                False,
                True,
                False,
                True,
            ]


def _daily_job():
    """A job whose next fire is about twelve hours away."""
    fire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        hours=12
    )
    return e2e.job("daily", schedule="%d %d * * *" % (fire.minute, fire.hour))


# Reads the "Next at" label and the rows kept since the last read.
_NEXT_AT = """() => {
  const state = window.__perf.state();
  const at = new Date(
    state.fetchedWallAt + state.jobs[0].scheduled_in * 1000);
  const rows = [...document.querySelectorAll('#rows tr[data-job]')];
  const kept = rows.filter((tr) => tr.__kept).length;
  rows.forEach((tr) => { tr.__kept = true; });
  return {
    kept,
    label: rows[0].querySelector('.col-nextat span').textContent,
    // the fire's hour and minute in the page's zone
    time: [at.getHours(), at.getMinutes()]
      .map((n) => String(n).padStart(2, '0')).join(':'),
  };
}"""


def test_next_at_names_its_day_from_the_browsers_local_date(browser, tmp_path):
    """The "Next at" label names a fire's day relative to the browser's
    local date. The first poll after local midnight rebuilds the rows,
    even on a 304, and the polls after it keep them."""

    def before_goto(page):
        # stopped one minute before midnight in the page's zone
        page.clock.install(time="2026-03-10T14:00:00Z")
        page.clock.pause_at("2026-03-10T14:59:00Z")

    with e2e.Daemon(tmp_path, jobs=[_daily_job()]) as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            prefs={"pollMs": 0, "cols": {"nextat": True}},
            before_goto=before_goto,
            timezone_id="Asia/Tokyo",
        ) as page:
            statuses = []
            page.on(
                "response",
                lambda response: (
                    response.url.endswith("/jobs")
                    and statuses.append(response.status)
                ),
            )
            before = page.evaluate(_NEXT_AT)
            assert before["label"] == "tom " + before["time"]
            _refresh(page)
            assert page.evaluate(_NEXT_AT)["kept"] == 1

            page.clock.fast_forward(61000)
            _refresh(page)
            after = page.evaluate(_NEXT_AT)
            assert statuses == [304, 304]
            assert after["kept"] == 0
            assert after["label"] == before["time"]
            _refresh(page)
            assert page.evaluate(_NEXT_AT)["kept"] == 1


def test_next_at_follows_a_change_of_the_browsers_time_zone(browser, tmp_path):
    """The "Next at" label gives a fire's day and time in the browser's
    time zone, so the first poll after a zone change rebuilds the rows."""
    sessions = []

    def before_goto(page):
        # Playwright's timezone_id holds the zone for the life of the
        # context, so the test sets the zone through its own CDP session.
        session = page.context.new_cdp_session(page)
        session.send("Emulation.setTimezoneOverride", {"timezoneId": "UTC"})
        sessions.append(session)
        page.clock.install(time="2026-03-10T05:00:00Z")
        page.clock.pause_at("2026-03-10T06:00:00Z")

    with e2e.Daemon(tmp_path, jobs=[_daily_job()]) as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            prefs={"pollMs": 0, "cols": {"nextat": True}},
            before_goto=before_goto,
        ) as page:
            statuses = []
            page.on(
                "response",
                lambda response: (
                    response.url.endswith("/jobs")
                    and statuses.append(response.status)
                ),
            )
            offset = "new Date().getTimezoneOffset()"
            assert page.evaluate(offset) == 0
            before = page.evaluate(_NEXT_AT)
            assert before["label"] == before["time"]

            # nine hours later on the same local date: the fire falls on
            # the next one
            sessions[0].send(
                "Emulation.setTimezoneOverride", {"timezoneId": "Asia/Tokyo"}
            )
            assert page.evaluate(offset) == -540
            _refresh(page)
            after = page.evaluate(_NEXT_AT)
            assert statuses == [304]
            assert after["kept"] == 0
            assert after["time"] != before["time"]
            assert after["label"] == "tom " + after["time"]


def test_a_revalidated_poll_carries_the_token(browser, tmp_path):
    """The conditional request authenticates like any other. A token the
    daemon refuses opens the token dialog, and the next accepted poll
    revalidates the rows the page still holds."""
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            token=e2e.FULL_TOKEN,
            prefs={"pollMs": 0},
            # midday in the page's zone, so the polls share one local day
            before_goto=lambda page: page.clock.install(
                time="2026-01-01T12:00:00Z"
            ),
            timezone_id="UTC",
        ) as page:
            page.faults.record()
            statuses = []
            page.on(
                "response",
                lambda response: (
                    response.url.endswith("/jobs")
                    and statuses.append(response.status)
                ),
            )
            names = e2e.row_names(page)
            page.evaluate(
                "document.querySelectorAll('#rows tr[data-job]')"
                ".forEach((tr) => { tr.__kept = true; })"
            )
            _refresh(page)
            sent = page.faults.sent("GET", "/jobs")[-1]["headers"]
            assert sent["authorization"] == "Bearer " + e2e.FULL_TOKEN
            assert sent["if-none-match"]

            page.evaluate("sessionStorage.setItem('cronstable_token', 'no')")
            page.evaluate("document.getElementById('refreshBtn').click()")
            page.wait_for_selector("#modalWrap.open")
            assert e2e.row_names(page) == names

            page.fill("#tokenInput", e2e.FULL_TOKEN)
            page.click("#tokenSave")
            page.wait_for_function(
                "!document.getElementById('modalWrap').classList"
                ".contains('open')"
            )
            page.wait_for_function(
                "!document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )
            assert statuses == [304, 401, 304]
            assert page.evaluate(_ANCHOR)["kept"] == len(names)


def test_a_poll_that_is_not_a_job_list_leaves_the_held_jobs(
    browser, tmp_path
):
    """The page refuses a ``/jobs`` answer that is not a list of jobs. It
    keeps the jobs it holds and the validator that names them, so the next
    poll revalidates them and the connection readout recovers."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
        ) as page:
            statuses = []
            page.on(
                "response",
                lambda response: (
                    response.url.endswith("/jobs")
                    and statuses.append(response.status)
                ),
            )
            names = e2e.row_names(page)
            held = page.evaluate("window.__perf.state().jobsEtag")
            assert held
            page.faults.status("/jobs", 200, body={"oops": 1}, times=1)
            _refresh(page)
            assert "no signal" in page.inner_text("#conn")
            assert page.evaluate("window.__perf.state().jobsEtag") == held
            assert e2e.row_names(page) == names
            _refresh(page)
            assert statuses == [200, 304]
            assert "live" in page.inner_text("#conn")
            assert e2e.row_names(page) == names


def _freeze(page):
    """Install the page clock and stop it, before navigation."""
    page.clock.install(time="2026-01-01T12:00:00Z")
    page.clock.pause_at("2026-01-01T12:01:00Z")


def test_a_tick_with_a_frozen_clock_writes_nothing(browser, daemon):
    """Every readout ``tick`` keeps current is written when its text
    changes, so a tick at an unchanged instant leaves the document alone."""
    with e2e.open_page(
        browser,
        daemon.url + "?perf=1",
        prefs={"pollMs": 0, "radar": True, "week": True},
        before_goto=_freeze,
    ) as page:
        records = page.evaluate(
            """() => {
              const perf = window.__perf;
              perf.seedJobs(40);
              perf.varySchedules();
              perf.renderRows();
              perf.seedFleet(3, 40);
              perf.renderFleet();
              perf.computeRadar();
              perf.computeWeek();
              // the first tick paints the seeded panels
              perf.tick();
              const seen = [];
              const observer = new MutationObserver(
                (list) => seen.push(...list));
              observer.observe(document, {
                subtree: true, childList: true, characterData: true,
                attributes: true,
              });
              perf.tick();
              perf.tick();
              seen.push(...observer.takeRecords());
              observer.disconnect();
              return {
                swept: document.querySelectorAll(
                  '[data-ago], [data-ago-short], [data-next]').length,
                marks: document.querySelectorAll('#radarTrack .rt-mk').length,
                seen: seen.map((m) => [
                  m.type, m.target.id || m.target.nodeName,
                  m.attributeName]),
              };
            }"""
        )
        assert records["swept"] > 100
        assert records["marks"] > 0
        assert records["seen"] == []


_SWEPT = (
    "document.querySelectorAll('[data-ago], [data-ago-short],"
    " [data-ago-epoch], [data-until-epoch], [data-until-iso],"
    " [data-next]').length"
)


def test_closed_panels_leave_nothing_for_the_tick_to_sweep(browser, daemon):
    """``tick`` sweeps every relative-time cell in the document, so a
    closed fleet panel and a closed timeline hold none, and each renders
    again in full when it reopens."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
    ) as page:
        # The start-up /cluster reply clears the fleet, so the seed below
        # waits for it.
        page.wait_for_load_state("networkidle")
        page.evaluate(
            "() => { window.__perf.seedJobs(40); window.__perf.renderRows(); }"
        )
        table = page.evaluate(_SWEPT)
        assert table > 40

        page.evaluate(
            "() => { window.__perf.seedFleet(3, 40);"
            " window.__perf.renderFleet(); }"
        )
        opened = page.evaluate(_SWEPT)
        assert opened > table + 40
        page.evaluate("document.getElementById('fleetBtn').click()")
        assert page.evaluate(_SWEPT) == table
        assert (
            page.evaluate(
                "document.getElementById('fleetPanel').childNodes.length"
            )
            == 0
        )
        page.evaluate("document.getElementById('fleetBtn').click()")
        assert page.evaluate(_SWEPT) == opened
        page.evaluate("document.getElementById('fleetBtn').click()")

        page.keyboard.press("i")
        page.wait_for_selector("#timelineWrap.open #tlBody .tlrow")
        assert page.evaluate(_SWEPT) == table + 40
        page.click("#tlClose")
        page.wait_for_function("!document.getElementById('tlBody').firstChild")
        assert page.evaluate(_SWEPT) == table
        page.keyboard.press("i")
        page.wait_for_selector("#timelineWrap.open #tlBody .tlrow")
        assert page.evaluate(_SWEPT) == table + 40


_CLOSE_REOPEN_CLOSE = """async () => {
  const body = document.getElementById('tlBody');
  const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
  const close = () => document.getElementById('tlClose').click();
  close();
  await sleep(150);
  document.body.dispatchEvent(
    new KeyboardEvent('keydown', {key: 'i', bubbles: true}));
  const reopened = body.childNodes.length;
  close();
  // past the first close's wait, and short of the second's
  await sleep(125);
  const fading = body.childNodes.length;
  await sleep(150);
  return [reopened, fading, body.childNodes.length];
}"""


def test_timeline_closed_twice_keeps_its_rows_through_the_fade(
    browser, daemon
):
    """A close empties the timeline after its fade.  A timeline that is
    reopened and closed again inside that wait keeps its rows until the
    second close's fade is over."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
    ) as page:
        page.evaluate(
            "() => { window.__perf.seedJobs(40); window.__perf.renderRows(); }"
        )
        page.keyboard.press("i")
        page.wait_for_selector("#timelineWrap.open #tlBody .tlrow")
        reopened, fading, emptied = page.evaluate(_CLOSE_REOPEN_CLOSE)
        assert reopened > 0
        assert fading == reopened
        assert emptied == 0


_POLLS = """(count) => {
  const perf = window.__perf, state = perf.state();
  let touch = window.__touch || 1;
  for (let poll = 0; poll < count; poll++) {
    for (let k = 0; k < 10; k++) perf.touchJob(touch++ * 53);
    // a poll delivers new objects for every job, changed or not
    const jobs = JSON.parse(JSON.stringify(state.jobs));
    state.jobs = jobs;
    state.byName = {};
    jobs.forEach((j) => { state.byName[j.name] = j; });
    perf.renderRowsDiff();
    perf.tick();
  }
  window.__touch = touch;
}"""


def _live_counts(page):
    session = page.context.new_cdp_session(page)
    try:
        session.send("Performance.enable")
        session.send("HeapProfiler.collectGarbage")
        metrics = {
            entry["name"]: entry["value"]
            for entry in session.send("Performance.getMetrics")["metrics"]
        }
    finally:
        session.detach()
    return metrics["Nodes"], metrics["JSEventListeners"]


def test_polls_leave_node_and_listener_counts_flat(browser, daemon):
    """A thousand polls that each move ten rows end with the node and
    listener counts they started with."""
    with e2e.open_page(
        browser,
        daemon.url + "?perf=1",
        prefs={"pollMs": 0, "motion": True},
        before_goto=_freeze,
    ) as page:
        # The counts are compared exactly, so only the polls may write:
        # the clock is stopped and the start-up requests are answered.
        page.wait_for_load_state("networkidle")
        page.evaluate(
            "() => { window.__perf.seedJobs(200);"
            " window.__perf.renderRows(); }"
        )
        # the first polls settle one-time growth such as cached row nodes
        page.evaluate(_POLLS, 20)
        before = _live_counts(page)
        page.evaluate(_POLLS, 1000)
        assert _live_counts(page) == before


def test_a_steady_wallboard_poll_writes_nothing_to_the_grid(browser, daemon):
    """The wallboard rebuilds its tiles when the fleet signature moves. A
    poll that brings the same jobs leaves every tile node alone."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
    ) as page:
        page.evaluate("document.getElementById('tvBtn').click()")
        page.wait_for_selector("#wbGrid .wb-tile")
        page.evaluate(
            """() => {
              window.__gridWrites = [];
              new MutationObserver(
                (list) => window.__gridWrites.push(...list)
              ).observe(document.getElementById('wbGrid'), {
                subtree: true, childList: true, characterData: true,
                attributes: true,
              });
            }"""
        )
        _refresh(page)
        _refresh(page)
        assert (
            page.evaluate(
                "window.__gridWrites.map((m) => [m.type, m.attributeName])"
            )
            == []
        )
        tiles = page.evaluate(
            "document.querySelectorAll('#wbGrid .wb-tile').length"
        )
        assert tiles == len(daemon.config["jobs"])


def test_ledger_analysis_reads_only_the_named_jobs(browser, daemon):
    """A poll hands ``ledgerAnalyze`` the jobs that finished a run. It
    reads the windows of those jobs and replaces only their statistics."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
    ) as page:
        result = page.evaluate(
            """() => {
              const perf = window.__perf, ledger = perf.state().ledger;
              perf.seedLedger(12, 30);
              perf.ledgerAnalyze();
              const before = Object.assign({}, ledger.stats);
              const runs = ledger.runs, read = new Set();
              ledger.runs = new Proxy(runs, {
                get(target, key, receiver) {
                  if (typeof key === 'string') read.add(key);
                  return Reflect.get(target, key, receiver);
                },
              });
              try {
                perf.ledgerAnalyze(['job3', 'job7']);
              } finally {
                ledger.runs = runs;
              }
              return {
                jobs: Object.keys(before).length,
                read: [...read].sort(),
                replaced: Object.keys(ledger.stats)
                  .filter((name) => ledger.stats[name] !== before[name])
                  .sort(),
              };
            }"""
        )
        assert result["jobs"] == 12
        assert result["read"] == ["job3", "job7"]
        assert result["replaced"] == ["job3", "job7"]


_LOGO_WRITES = """async (frames) => {
  const svg = document.querySelector('#mark svg');
  let writes = 0;
  const observer = new MutationObserver((list) => { writes += list.length; });
  observer.observe(svg, {
    subtree: true, childList: true, attributes: true,
  });
  for (let i = 0; i < frames; i++) {
    await new Promise((done) => requestAnimationFrame(done));
  }
  writes += observer.takeRecords().length;
  observer.disconnect();
  return writes;
}"""


def test_a_parked_logo_writes_nothing(browser, daemon):
    """The balanced mark sways on every frame. Reduce motion parks it: the
    animation loop stops, and no frame writes to its drawing."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"motion": True}
    ) as page:
        assert page.evaluate(_LOGO_WRITES, 30) == 0
        page.evaluate(
            """() => {
              const motion = document.getElementById('setMotion');
              motion.checked = false;
              motion.dispatchEvent(new Event('change'));
            }"""
        )
        assert page.evaluate(_LOGO_WRITES, 30) > 0


def test_the_large_tables_keep_their_own_paint_record(browser, daemon):
    """The jobs table and the fleet matrix are isolated stacking contexts,
    so a frame that repaints the swaying logo skips their rows."""
    with e2e.open_page(
        browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
    ) as page:
        isolated = page.evaluate(
            """() => {
              window.__perf.seedFleet(2, 5);
              window.__perf.renderFleet();
              const read = (selector) => getComputedStyle(
                document.querySelector(selector)).isolation;
              return [read('.twrap'), read('#fleetPanel .fleetwrap')];
            }"""
        )
        assert isolated == ["isolate", "isolate"]
