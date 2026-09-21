"""Test job filtering, sorting, columns, and incremental table updates.

The dashboard uses the daemon in ``tests/_web_e2e.py``. Filter tests use jobs
that succeeded, failed, are running, are disabled, or have never run. Sort
and update tests replace ``/jobs`` responses with controlled job objects.
This lets tests insert, remove, reorder, and update rows between polls.

``diffRows`` updates individual rows in the live ``<tbody>``. Compare each
update with a full render of the same state through ``window.__perf``,
which the page exposes with ``?perf=1``. Verify that unchanged jobs retain
their ``<tr>`` nodes, changed jobs receive new nodes, selection and drawer
highlights follow the corresponding jobs, and removed jobs leave no rows.
"""

import copy

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _wait_names(page, names):
    try:
        _wait_names_(page, names)
    except Exception:
        raise AssertionError(
            "rows are {}, expected {}".format(e2e.row_names(page), names)
        ) from None


def _wait_names_(page, names):
    page.wait_for_function(
        """(names) => {
          const have = [...document.querySelectorAll('#rows tr[data-job]')]
            .map((tr) => tr.getAttribute('data-job'));
          return JSON.stringify(have) === JSON.stringify(names);
        }""",
        arg=names,
    )


# --------------------------------------------------------------------------
# scripted /jobs payloads
# --------------------------------------------------------------------------

T1 = "2026-01-01T00:00:00+00:00"
T2 = "2026-01-02T00:00:00+00:00"
T3 = "2026-01-03T00:00:00+00:00"


def _run(outcome, finished, duration):
    return {
        "outcome": outcome,
        "exit_code": 0 if outcome == "success" else 1,
        "started_at": finished,
        "finished_at": finished,
        "duration": duration,
        "fail_reason": None if outcome == "success" else "exit 1",
        "skip_reason": None,
        "resources": None,
    }


def _fleet(base):
    """Five jobs whose order under every sort key is distinct."""

    def make(name, **over):
        j = copy.deepcopy(base)
        j.update(
            name=name,
            command="cmd-" + name,
            enabled=True,
            running=False,
            paused=None,
            last_run=None,
            history=[],
            scheduled_in=None,
            pids=[],
        )
        j.update(over)
        return j

    ok, bad = _run("success", T1, 5), _run("failure", T3, 1)
    return [
        make(
            "a",
            last_run=ok,
            history=[ok, ok],
            scheduled_in=50000,
            clusterPolicy="Leader",
            clusterOwner="n2",
        ),
        make(
            "b",
            last_run=bad,
            history=[ok, bad],
            scheduled_in=10000,
            clusterPolicy="PreferLeader",
            clusterOwner="n1",
        ),
        make(
            "c",
            running=True,
            last_run=_run("failure", T2, 3),
            history=[bad, bad],
            scheduled_in=30000,
            clusterPolicy="Leader",
            clusterOwner=None,
        ),
        make("d", enabled=False, clusterPolicy="EveryNode"),
        make("e", scheduled_in=20000),
    ]


class _Jobs:
    """Serves ``self.jobs`` as the ``/jobs`` answer."""

    def __init__(self, daemon):
        self.base = daemon.jobs()["alpha-ok"]
        self.jobs = _fleet(self.base)

    def install(self, page):
        page.faults.json(r"/jobs", lambda request: self.jobs)

    def get(self, name):
        return next(j for j in self.jobs if j["name"] == name)

    def add(self, name, **over):
        j = copy.deepcopy(self.get("e"))
        j.update(name=name, command="cmd-" + name)
        j.update(over)
        self.jobs.append(j)
        return j

    def remove(self, *names):
        self.jobs = [j for j in self.jobs if j["name"] not in names]


def _refresh(page):
    _click(page, "#refreshBtn")
    # Names may be unchanged while the request is still applying new state.
    page.wait_for_function(
        "!document.querySelector('#refreshBtn').classList.contains('spin')"
    )


# --------------------------------------------------------------------------
# filters (real job states)
# --------------------------------------------------------------------------


def _real_states(daemon):
    daemon.run_and_wait("alpha-ok", outcome="success")
    daemon.run_and_wait("beta-fail", outcome="failure")
    status, _ = daemon.api("POST", "/jobs/gamma-slow/start")
    assert status == 200


def test_search_filters_by_name_and_command(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        _real_states(daemon)
        with e2e.open_page(browser, daemon.url) as page:
            _wait_names(
                page,
                [
                    "alpha-ok",
                    "beta-fail",
                    "delta-off",
                    "epsilon-quiet",
                    "gamma-slow",
                ],
            )
            assert page.text_content("#countLabel") == "5 jobs"
            # "/" focuses the filter
            page.keyboard.press("/")
            assert page.evaluate("document.activeElement.id") == "search"
            page.keyboard.type("ALPHA")
            _wait_names(page, ["alpha-ok"])
            assert page.text_content("#countLabel") == "1 job (of 5)"
            # the command is searched as well as the name
            page.fill("#search", "beta-err")
            _wait_names(page, ["beta-fail"])
            page.fill("#search", "ta-")
            _wait_names(page, ["beta-fail", "delta-off"])
            assert page.text_content("#countLabel") == "2 jobs (of 5)"
            # nothing matches: the filter's own empty state
            page.fill("#search", "no such job")
            _wait_names(page, [])
            assert page.is_visible("#emptyState")
            assert page.inner_text("#emptyMsg") == (
                "No jobs match your filter."
            )
            assert page.text_content("#countLabel") == "0 jobs (of 5)"
            page.fill("#search", "")
            page.wait_for_function(
                "document.querySelectorAll('#rows tr').length === 5"
            )
            assert not page.is_visible("#emptyState")
            # a regex metacharacter is plain text here
            page.fill("#search", ".*")
            _wait_names(page, [])


def test_status_filter_selects_by_real_state(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        _real_states(daemon)
        with e2e.open_page(browser, daemon.url) as page:
            e2e.wait_row_status(page, "gamma-slow", "Running")
            expect = {
                "ok": ["alpha-ok"],
                "fail": ["beta-fail"],
                "run": ["gamma-slow"],
                "disabled": ["delta-off"],
                "queued": [],
                "verifying": [],
                "all": [
                    "alpha-ok",
                    "beta-fail",
                    "delta-off",
                    "epsilon-quiet",
                    "gamma-slow",
                ],
            }
            for key, names in expect.items():
                _click(page, '#statusFilter button[data-f="{}"]'.format(key))
                _wait_names(page, names)
                assert page.evaluate(
                    "[...document.querySelectorAll("
                    "'#statusFilter button.active')]"
                    ".map((b) => b.getAttribute('data-f'))"
                ) == [key]
                if not names:
                    assert page.inner_text("#emptyMsg") == (
                        "No jobs match your filter."
                    )
            # status and text filters combine
            _click(page, '#statusFilter button[data-f="fail"]')
            page.fill("#search", "alpha")
            _wait_names(page, [])
            page.fill("#search", "beta")
            _wait_names(page, ["beta-fail"])
            # the summary pills count the fleet, not the filtered view
            assert page.inner_text("#summary").split() == (
                "5 jobs 1 running 1 failing 1 ok".split()
            )
            # the running job leaves the "run" view when it is canceled
            page.fill("#search", "")
            _click(page, '#statusFilter button[data-f="run"]')
            _wait_names(page, ["gamma-slow"])
            daemon.api("POST", "/jobs/gamma-slow/cancel")
            _wait_names(page, [])


def test_no_jobs_configured_empty_state(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=[]) as daemon:
        with e2e.open_page(browser, daemon.url, wait_rows=False) as page:
            page.wait_for_function(
                "document.getElementById('emptyState').style.display === "
                "'block'"
            )
            assert page.inner_text("#emptyMsg") == "No jobs configured."
            assert page.text_content("#countLabel") == "0 jobs"
            assert page.inner_text("#summary").split()[:2] == ["0", "jobs"]
            page.wait_for_function("document.title === 'no jobs · cronstable'")


# --------------------------------------------------------------------------
# sorting
# --------------------------------------------------------------------------

# ascending order per sort key for _fleet(); descending is the exact
# reverse, because the name tie-break flips with the direction
_ORDERS = {
    "name": list("abcde"),
    "status": list("cbead"),
    "last": list("deacb"),
    "next": list("becad"),
    "nextat": list("becad"),
    "duration": list("debca"),
    "owner": list("bacde"),
    "policy": list("dacbe"),
    "rate": list("cbade"),
}

_ALL_COLUMNS = {
    "cols": {"policy": True, "tz": True, "nextat": True, "rate": True}
}


def _arrows(page):
    return page.evaluate(
        "Object.fromEntries([...document.querySelectorAll("
        "'thead th[data-sort]')].filter((th) => th.querySelector('.arrow'))"
        ".map((th) => [th.getAttribute('data-sort'), "
        "th.querySelector('.arrow').textContent]))"
    )


def test_every_sort_key_in_both_directions(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        jobs = _Jobs(daemon)
        with e2e.open_page(
            browser,
            daemon.url,
            prefs=_ALL_COLUMNS,
            before_goto=jobs.install,
        ) as page:
            _wait_names(page, _ORDERS["name"])
            assert _arrows(page) == {"name": "↑"}
            # the owner column appears because the payload carries owners
            assert page.is_visible('thead th[data-sort="owner"]')
            for key, order in _ORDERS.items():
                header = 'thead th[data-sort="{}"]'.format(key)
                if key == "name":
                    # already the active ascending key: a click flips it
                    page.click(header)
                    _wait_names(page, order[::-1])
                    assert _arrows(page) == {"name": "↓"}
                    page.click(header)
                    _wait_names(page, order)
                    continue
                page.click(header)
                _wait_names(page, order)
                assert _arrows(page) == {key: "↑"}, key
                page.click(header)
                _wait_names(page, order[::-1])
                assert _arrows(page) == {key: "↓"}, key
                # the header labels survive the arrow rewrites
                assert page.inner_text(header).rstrip(" ↓↑") != ""
            labels = page.evaluate(
                "[...document.querySelectorAll('thead th[data-sort]')]"
                ".map((th) => th.textContent.replace(/[ ↑↓]+$/, ''))"
            )
            assert labels == [
                "Status",
                "Job",
                "Owner",
                "Policy",
                "Last run",
                "Took",
                "Next",
                "Next at",
                "Rate",
            ]


def test_sort_select_follows_and_drives_the_sort(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        jobs = _Jobs(daemon)
        with e2e.open_page(
            browser, daemon.url, before_goto=jobs.install
        ) as page:
            _wait_names(page, _ORDERS["name"])
            for key in ("status", "last", "next", "duration", "name"):
                page.select_option("#sortSel", key)
                _wait_names(page, _ORDERS[key])
            # a header click moves the select along with it
            page.click('thead th[data-sort="status"]')
            assert page.input_value("#sortSel") == "status"
            # the select keeps the direction a header click set
            page.click('thead th[data-sort="status"]')
            _wait_names(page, _ORDERS["status"][::-1])
            page.select_option("#sortSel", "duration")
            _wait_names(page, _ORDERS["duration"][::-1])
            # sorting applies on top of a filter
            page.fill("#search", "cmd-")
            _click(page, '#statusFilter button[data-f="fail"]')
            _wait_names(page, ["b"])


# --------------------------------------------------------------------------
# columns menu
# --------------------------------------------------------------------------


def _table_classes(page):
    return set(
        page.evaluate("[...document.getElementById('jobsTable').classList]")
    )


def test_columns_menu_toggles_and_persists(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            classic = {
                "show-sched",
                "show-last",
                "show-dur",
                "show-next",
                "show-trend",
            }
            assert _table_classes(page) == classic
            assert not page.evaluate(
                "document.querySelector('main').classList.contains('wide')"
            )
            assert not page.is_visible("#rows td.col-tz")
            page.click("#colsBtn")
            page.wait_for_selector("#colsMenu.open")
            boxes = page.evaluate(
                "Object.fromEntries([...document.querySelectorAll("
                "'#colsMenu input')].map((i) => "
                "[i.getAttribute('data-col'), i.checked]))"
            )
            assert boxes == {
                "policy": False,
                "sched": True,
                "tz": False,
                "last": True,
                "dur": True,
                "next": True,
                "nextat": False,
                "rate": False,
                "trend": True,
            }
            page.check('#colsMenu input[data-col="tz"]')
            page.uncheck('#colsMenu input[data-col="sched"]')
            assert _table_classes(page) == (classic - {"show-sched"}) | {
                "show-tz"
            }
            assert page.is_visible("#rows td.col-tz")
            assert not page.is_visible("#rows td.col-sched")
            assert page.inner_text("#rows td.col-tz") == "UTC"
            # an extra column flips the page to the fluid layout
            assert page.evaluate(
                "document.querySelector('main').classList.contains('wide')"
            )
            # a click inside the menu keeps it open; outside closes it
            assert page.is_visible("#colsMenu")
            page.click("#countLabel")
            page.wait_for_function(
                "!document.getElementById('colsMenu').classList"
                ".contains('open')"
            )
            # Escape closes it too
            page.click("#colsBtn")
            page.wait_for_selector("#colsMenu.open")
            page.keyboard.press("Escape")
            page.wait_for_function(
                "!document.getElementById('colsMenu').classList"
                ".contains('open')"
            )
            assert page.evaluate(
                "JSON.parse(localStorage.getItem('cronstable.cols'))"
            ) == {"tz": True, "sched": False}

            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert _table_classes(page) == (classic - {"show-sched"}) | {
                "show-tz"
            }
            page.click("#colsBtn")
            page.wait_for_selector("#colsMenu.open")
            assert page.is_checked('#colsMenu input[data-col="tz"]')
            assert not page.is_checked('#colsMenu input[data-col="sched"]')
            # back to the classic set: the centered layout returns
            page.uncheck('#colsMenu input[data-col="tz"]')
            assert not page.evaluate(
                "document.querySelector('main').classList.contains('wide')"
            )


# --------------------------------------------------------------------------
# keyed reconcile vs wholesale rebuild
# --------------------------------------------------------------------------

_STAMP = """
() => {
  window.__stampSeq = (window.__stampSeq || 0) + 1;
  for (const tr of document.querySelectorAll('#rows tr[data-job]'))
    if (tr.__stamp === undefined) tr.__stamp = window.__stampSeq;
}
"""

_STAMPS = """
() => Object.fromEntries(
  [...document.querySelectorAll('#rows tr[data-job]')]
    .map((tr) => [tr.getAttribute('data-job'), tr.__stamp ?? null]))
"""

# The live tbody against the markup a wholesale rebuild produces from the
# same state, plus the bookkeeping the reconcile keeps beside the DOM.
_VERSUS_FRESH = """
() => {
  const rows = document.getElementById('rows');
  const st = window.__perf.state();
  const live = rows.innerHTML;
  const liveNames = [...rows.children].map((tr) =>
    tr.getAttribute('data-job'));
  const mapNames = [...st.rowNodes.keys()].sort();
  const mapped = [...st.rowNodes.entries()].every(([name, tr]) =>
    tr.parentNode === rows && tr.getAttribute('data-job') === name);
  const stamps = new Map([...rows.children].map((tr) =>
    [tr.getAttribute('data-job'), tr.__stamp]));
  window.__perf.renderRows();
  const fresh = rows.innerHTML;
  // the rebuild made new nodes: carry the stamps over so identity checks
  // in later steps still compare against the pre-rebuild generation
  for (const tr of rows.children)
    tr.__stamp = stamps.get(tr.getAttribute('data-job'));
  return { same: live === fresh, live, fresh, liveNames, mapNames, mapped,
           sigNames: [...st.rowSigs.keys()].sort() };
}
"""


def _check(page, names):
    """The patched table equals a fresh render and tracks exactly ``names``."""
    _wait_names(page, names)
    got = page.evaluate(_VERSUS_FRESH)
    assert got["liveNames"] == names
    assert got["same"], (
        "patched DOM differs from a fresh render:\n{}\n{}".format(
            got["live"], got["fresh"]
        )
    )
    assert got["mapNames"] == sorted(names)
    assert got["sigNames"] == sorted(names)
    assert got["mapped"]
    assert page.evaluate(
        "document.querySelectorAll('#rows tr').length"
    ) == len(names)


def _step(page, names, kept=(), rebuilt=()):
    """Refresh, then verify node identity and the fresh-render equality.

    ``kept`` jobs keep the ``<tr>`` they had; ``rebuilt`` jobs get a new
    one (their stamp is from this step).
    """
    before = page.evaluate(_STAMPS)
    seq = page.evaluate("window.__stampSeq")
    _refresh(page)
    _wait_names(page, names)
    page.evaluate(_STAMP)
    after = page.evaluate(_STAMPS)
    for name in kept:
        assert after[name] == before[name], name + " was rebuilt"
    for name in rebuilt:
        assert after[name] == seq + 1, name + " was not rebuilt"
    _check(page, names)


def test_next_at_reconciles_across_a_minute_boundary(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url + "?perf=1", prefs={"pollMs": 0}
        ) as page:
            result = page.evaluate(
                """() => {
                  const now = Date.now, monotonic = performance.now;
                  try {
                    Date.now = () => Date.UTC(2026, 2, 7, 12);
                    performance.now = () => 1000;
                    const perf = window.__perf, state = perf.state();
                    perf.seedJobs(1);
                    state.fetchedAt = 1000;
                    const job = state.jobs[0];
                    job.running = false;
                    job.scheduled_in = 3599.99;
                    perf.renderRows();
                    const before = document.querySelector('.col-nextat span')
                      .getAttribute('title');
                    job.scheduled_in = 3600.01;
                    perf.renderRowsDiff();
                    const after = document.querySelector('.col-nextat span')
                      .getAttribute('title');
                    const live = document.getElementById('rows').innerHTML;
                    perf.renderRows();
                    const fresh = document.getElementById('rows').innerHTML;
                    // A wall-clock correction must not move this response's
                    // absolute target back into the preceding minute.
                    Date.now = () => state.fetchedWallAt + 59999;
                    performance.now = () => 61000;
                    perf.renderRows();
                    const later = document.querySelector('.col-nextat span')
                      .getAttribute('title');
                    return {before, after, later, same: live === fresh};
                  } finally {
                    Date.now = now;
                    performance.now = monotonic;
                  }
                }"""
            )
            assert result["before"] != result["after"]
            assert result["same"]
            assert result["later"] == result["after"]


def test_reconcile_matches_a_fresh_render_through_every_change(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path) as daemon:
        jobs = _Jobs(daemon)

        def install(page):
            jobs.install(page)
            # These scripted payloads hold scheduled_in constant. Hold browser
            # time constant too, so only payload changes can rebuild a row.
            page.clock.install(time="2026-01-01T12:00:00Z")
            page.clock.pause_at("2026-01-01T12:01:00Z")

        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            # a manual refresh drives every step
            prefs={"pollMs": 0},
            before_goto=install,
        ) as page:
            _wait_names(page, list("abcde"))
            page.evaluate(_STAMP)
            _check(page, list("abcde"))

            # an unchanged payload touches nothing
            _step(page, list("abcde"), kept="abcde")

            # insert at the head, in the middle and at the tail
            jobs.add("0first")
            jobs.add("bb")
            jobs.add("zlast")
            _step(
                page,
                ["0first", "a", "b", "bb", "c", "d", "e", "zlast"],
                kept="abcde",
                rebuilt=["0first", "bb", "zlast"],
            )

            # remove from the head and the middle
            jobs.remove("0first", "b")
            _step(
                page,
                ["a", "bb", "c", "d", "e", "zlast"],
                kept=["a", "bb", "c", "d", "e", "zlast"],
            )

            # a status change rebuilds exactly that row
            c = jobs.get("c")
            c.update(running=False, last_run=_run("success", T3, 2))
            _step(
                page,
                ["a", "bb", "c", "d", "e", "zlast"],
                kept=["a", "bb", "d", "e", "zlast"],
                rebuilt=["c"],
            )
            assert e2e.row_status(page, "c") == "OK"

            # a payload in another order renders in sort order regardless
            jobs.jobs.reverse()
            _step(
                page,
                ["a", "bb", "c", "d", "e", "zlast"],
                kept=["a", "bb", "c", "d", "e", "zlast"],
            )

            # reorder under a sort: rows move, unchanged ones keep their node
            page.click('thead th[data-sort="last"]')
            _check(page, ["bb", "d", "e", "zlast", "a", "c"])
            jobs.get("e").update(last_run=_run("success", T2, 1))
            jobs.get("a").update(
                last_run=_run("failure", "2026-02-01T00:00:00+00:00", 9)
            )
            _step(
                page,
                ["bb", "d", "zlast", "e", "c", "a"],
                kept=["bb", "d", "zlast", "c"],
                rebuilt=["e", "a"],
            )
            page.click('thead th[data-sort="last"]')
            _check(page, ["a", "c", "e", "zlast", "d", "bb"])
            page.click('thead th[data-sort="name"]')
            _check(page, ["a", "bb", "c", "d", "e", "zlast"])

            # a rename is a removal plus an insertion
            jobs.get("bb").update(name="renamed")
            _step(
                page,
                ["a", "c", "d", "e", "renamed", "zlast"],
                kept=["a", "c", "d", "e", "zlast"],
                rebuilt=["renamed"],
            )

            # under a filter, a change moves a job out of and into the view
            _click(page, '#statusFilter button[data-f="fail"]')
            _check(page, ["a"])
            jobs.get("a").update(last_run=_run("success", T3, 1))
            jobs.get("d").update(enabled=True, last_run=_run("failure", T3, 1))
            _step(page, ["d"], rebuilt=["d"])
            _click(page, '#statusFilter button[data-f="all"]')
            _check(page, ["a", "c", "d", "e", "renamed", "zlast"])

            # every job gone, then back
            saved = jobs.jobs
            jobs.jobs = []
            _refresh(page)
            _wait_names(page, [])
            assert page.inner_text("#emptyMsg") == "No jobs configured."
            got = page.evaluate(_VERSUS_FRESH)
            assert got["same"] and got["mapNames"] == []
            jobs.jobs = saved
            _step(page, ["a", "c", "d", "e", "renamed", "zlast"])


def test_selection_and_drawer_highlight_follow_their_job(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        jobs = _Jobs(daemon)
        with e2e.open_page(
            browser,
            daemon.url + "?perf=1",
            prefs={"pollMs": 0},
            before_goto=jobs.install,
        ) as page:
            _wait_names(page, list("abcde"))

            def marked(cls):
                return page.evaluate(
                    "(c) => [...document.querySelectorAll('#rows tr.' + c)]"
                    ".map((tr) => tr.getAttribute('data-job'))",
                    cls,
                )

            page.keyboard.press("j")
            page.keyboard.press("j")
            page.keyboard.press("j")
            assert marked("sel") == ["c"]
            # rows arrive before and after it, and its own state changes
            jobs.add("0first")
            jobs.add("zlast")
            jobs.get("c").update(running=False)
            _refresh(page)
            _wait_names(page, ["0first", "a", "b", "c", "d", "e", "zlast"])
            assert marked("sel") == ["c"]
            page.keyboard.press("j")
            assert marked("sel") == ["d"]
            page.keyboard.press("k")
            page.keyboard.press("k")
            assert marked("sel") == ["b"]
            # selection follows the job through a re-sort
            page.click('thead th[data-sort="status"]')
            assert marked("sel") == ["b"]
            page.click('thead th[data-sort="name"]')
            # the cursor clamps at both ends
            for _ in range(12):
                page.keyboard.press("k")
            assert marked("sel") == ["0first"]
            for _ in range(12):
                page.keyboard.press("j")
            assert marked("sel") == ["zlast"]
            # a filter that hides the selected job drops the selection
            page.fill("#search", "cmd-a")
            _wait_names(page, ["a"])
            assert marked("sel") == []
            page.fill("#search", "")
            page.evaluate("document.activeElement.blur()")
            page.keyboard.press("j")
            assert marked("sel") == ["0first"]

            # the open drawer's row is highlighted, and survives a poll
            page.keyboard.press("j")
            page.keyboard.press("Enter")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert marked("active") == ["a"]
            jobs.add("aa")
            _refresh(page)
            page.wait_for_function(
                "document.querySelectorAll('#rows tr').length === 8"
            )
            assert marked("active") == ["a"]
            assert page.evaluate(_VERSUS_FRESH)["same"]
            # the open job vanishing from the payload closes its drawer
            jobs.remove("a")
            _refresh(page)
            page.wait_for_selector('#drawer[aria-hidden="true"]')
            assert marked("active") == []
            assert marked("sel") == []
            assert page.evaluate("location.hash") == ""
            assert page.evaluate(_VERSUS_FRESH)["same"]


def test_reconcile_with_real_state_changes(browser, tmp_path):
    """The same equality against the real daemon: a run starts, streams,
    is canceled; a job is paused and resumed; each poll patches rows."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url + "?perf=1", prefs={"pollMs": 1000}
        ) as page:
            names = [
                "alpha-ok",
                "beta-fail",
                "delta-off",
                "epsilon-quiet",
                "gamma-slow",
            ]
            _check(page, names)
            daemon.api("POST", "/jobs/gamma-slow/start")
            e2e.wait_row_status(page, "gamma-slow", "Running")
            _check(page, names)
            daemon.api(
                "POST", "/jobs/alpha-ok/pause", body={"note": "maintenance"}
            )
            e2e.wait_row_status(page, "alpha-ok", "Paused")
            _check(page, names)
            daemon.api("POST", "/jobs/gamma-slow/cancel")
            e2e.wait_row_status(page, "gamma-slow", "Cancelled")
            _check(page, names)
            daemon.api("POST", "/jobs/alpha-ok/resume")
            e2e.wait_row_status(page, "alpha-ok", "Pending")
            daemon.run_and_wait("beta-fail", outcome="failure")
            e2e.wait_row_status(page, "beta-fail", "Failed")
            _check(page, names)
