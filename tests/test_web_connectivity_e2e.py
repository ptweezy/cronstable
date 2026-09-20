"""Test connection loss, recovery, and polling against a running daemon.

The dashboard uses ``tests/_web_e2e.py``. ``page.route`` simulates network
faults, and Playwright's clock controls request timeouts and polling delays.
Tests check these behaviors:

* Failed polls update the connection header, tab title, and pendulum logo
  while retaining the last table rows. A successful poll restores them.
* The 20-second request timeout clears ``refreshing`` so polling can resume.
* Hidden tabs poll less often and refresh immediately when visible again.
* Poll interval settings update the timer and persist across reloads.
* Polls reuse a pending request instead of starting overlapping requests.
"""

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


_CONN = "document.getElementById('conn').textContent.trim()"

# Flips what the page reads as its visibility, then announces it, the way
# a browser does when the tab is backgrounded or restored.
_SET_HIDDEN = """
(hidden) => {
  Object.defineProperty(document, "hidden",
    { configurable: true, get: () => hidden });
  Object.defineProperty(document, "visibilityState",
    { configurable: true, get: () => (hidden ? "hidden" : "visible") });
  document.dispatchEvent(new Event("visibilitychange"));
}
"""


def _count(page, path):
    return len(page.faults.sent("GET", path))


def _wait_conn(page, text):
    page.wait_for_function(
        "(t) => document.getElementById('conn').textContent.trim() === t",
        arg=text,
    )


@pytest.mark.parametrize("fault", ["abort", "500", "garbage"])
def test_poll_failure_shows_no_signal_and_recovery_restores(
    browser, tmp_path, fault
):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok", outcome="success")
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            allow=(r"Unexpected token", r"is not valid JSON"),
        ) as page:
            _wait_conn(page, "live")
            page.wait_for_function("document.title.includes('ok')")
            page.wait_for_selector("#mark svg[data-mode='balance']")
            assert "Connected to the cronstable server" in (
                page.get_attribute("#conn", "title")
            )
            rows_before = e2e.row_names(page)

            if fault == "abort":
                page.faults.abort(r"/jobs")
            elif fault == "500":
                page.faults.status(r"/jobs", 500)
            else:
                page.route(
                    e2e.Faults._glob(r"/jobs"),
                    lambda route: route.fulfill(
                        status=200,
                        content_type="application/json",
                        body="<html>proxy error</html>",
                    ),
                )
            _wait_conn(page, "no signal")
            assert page.evaluate("!!document.querySelector('#conn .dot.dead')")
            page.wait_for_function(
                "document.title === 'no signal · cronstable'"
            )
            # the logo's motor cuts out: it leaves the balanced mode
            page.wait_for_function(
                "document.querySelector('#mark svg')"
                ".getAttribute('data-mode') !== 'balance'"
            )
            assert "No response from the cronstable server" in (
                page.get_attribute("#conn", "title")
            )
            # the last good rows stay on screen, and the refresh button's
            # spinner is released after each failed attempt
            assert e2e.row_names(page) == rows_before
            page.wait_for_function(
                "!document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )

            page.faults.clear(r"/jobs")
            _wait_conn(page, "live")
            assert page.evaluate("!!document.querySelector('#conn .dot.live')")
            page.wait_for_function("!document.title.includes('no signal')")
            # the motor is back: the logo swings up toward its balance
            page.wait_for_function(
                "document.querySelector('#mark svg')"
                ".getAttribute('data-mode') !== 'limp'"
            )


def test_first_poll_failure_then_recovery(browser, tmp_path):
    """A daemon unreachable from the first request: the static title holds
    nothing stale, and the board fills in when the daemon answers."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.faults.abort(r"/jobs"),
            wait_rows=False,
        ) as page:
            _wait_conn(page, "no signal")
            assert e2e.row_names(page) == []
            assert "Waiting for the first response" in (
                page.get_attribute("#conn", "title")
            )
            page.faults.clear(r"/jobs")
            page.wait_for_selector("#rows tr[data-job]")
            _wait_conn(page, "live")


def test_secondary_endpoint_failures_do_not_drop_the_signal(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:

        def routes(page):
            for path in ("/cluster", "/node", "/dags", "/state", "/pools"):
                page.faults.status(path, 503)

        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=routes,
            allow=(),
        ) as page:
            requests = page.faults.record()
            page.wait_for_function(
                "document.getElementById('poolMeta').textContent"
                ".includes('unavailable')"
            )
            assert page.evaluate(_CONN) == "live"
            assert (
                page.evaluate("document.getElementById('nodeMeter').innerHTML")
                == ""
            )
            # the main poll continues while related requests fail
            before = len([r for r in requests if r["path"] == "/jobs"])
            page.wait_for_function(
                "(n) => performance.getEntriesByName("
                "location.origin + '/jobs').length > n",
                arg=before + 2,
            )
            assert page.evaluate(_CONN) == "live"


def test_request_bound_aborts_a_hung_poll_and_unlatches(browser, tmp_path):
    """Cancel a pending request after 20 seconds and resume polling.

    While the request is pending, ``refreshing`` blocks additional polls.
    Use Playwright's clock to advance time without waiting."""
    with e2e.Daemon(tmp_path) as daemon:

        def before(page):
            page.clock.install()

        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=before,
        ) as page:
            page.faults.record()
            _wait_conn(page, "live")
            parked = page.faults.hang(r"/jobs")
            page.clock.fast_forward(1100)
            e2e.wait_until(lambda: len(parked) == 1, page)
            page.wait_for_function(
                "document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )
            # ten more poll intervals: each one early-returns on the latch
            for _ in range(10):
                page.clock.fast_forward(1000)
            assert len(parked) == 1
            assert page.evaluate(_CONN) == "live"
            # manual refresh is turned away by the same latch
            page.keyboard.press("g")
            assert len(parked) == 1
            # past the bound the request aborts and the latch releases
            page.clock.fast_forward(9500)
            _wait_conn(page, "no signal")
            page.wait_for_function(
                "!document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )
            page.clock.fast_forward(1100)
            e2e.wait_until(lambda: len(parked) >= 2, page)
            page.faults.clear(r"/jobs")
            page.clock.fast_forward(21000)
            _wait_conn(page, "live")


def test_hidden_tab_polls_slowly_and_resyncs_on_return(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.clock.install(),
        ) as page:
            page.faults.record()
            _wait_conn(page, "live")

            def settle():
                # let in-flight requests land before counting
                page.wait_for_function(
                    "!document.getElementById('refreshBtn').classList"
                    ".contains('spin')"
                )
                return _count(page, "/jobs")

            page.evaluate(_SET_HIDDEN, True)
            base = settle()
            # 29 s hidden: the foreground cadence would have polled 29 times
            for _ in range(29):
                page.clock.fast_forward(1000)
            assert settle() == base
            page.clock.fast_forward(1500)
            e2e.wait_until(lambda: _count(page, "/jobs") == base + 1, page)
            # the clock cell is a foreground-only sweep: it holds still
            clock_text = page.inner_text("#clock")
            page.clock.fast_forward(5000)
            assert page.inner_text("#clock") == clock_text

            base = settle()
            page.evaluate(_SET_HIDDEN, False)
            # an immediate resync, without waiting out an interval
            e2e.wait_until(lambda: _count(page, "/jobs") == base + 1, page)
            page.wait_for_function(
                "(t) => document.getElementById('clock').textContent !== t",
                arg=clock_text,
            )
            # and the foreground cadence is back
            for extra in (1, 2, 3):
                page.clock.fast_forward(1000)
                want = base + 1 + extra
                e2e.wait_until(
                    lambda want=want: _count(page, "/jobs") >= want, page
                )
                settle()


def test_poll_interval_setting_rearms_the_timer(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            before_goto=lambda page: page.clock.install(),
        ) as page:
            page.faults.record()
            _wait_conn(page, "live")

            def settle():
                page.wait_for_function(
                    "!document.getElementById('refreshBtn').classList"
                    ".contains('spin')"
                )
                return _count(page, "/jobs")

            def polls_in(ms):
                base = settle()
                # one interval at a time, so each poll lands before the
                # next is due (a pending one would turn the next away)
                step = 500
                for _ in range(ms // step):
                    page.clock.fast_forward(step)
                    settle()
                return settle() - base

            # the default cadence is 3 s
            assert page.input_value("#setPoll") == "3000"
            assert polls_in(9000) == 3
            page.evaluate("document.getElementById('settingsBtn').click()")
            page.select_option("#setPoll", "1000")
            assert polls_in(5000) == 5
            assert (
                page.evaluate("localStorage.getItem('cronstable.pollMs')")
                == "1000"
            )
            page.select_option("#setPoll", "0")
            assert polls_in(15000) == 0
            # paused is a choice, not an outage
            assert page.evaluate(_CONN) == "live"
            # manual refresh still works while paused
            page.keyboard.press("Escape")
            base = settle()
            page.evaluate("document.getElementById('refreshBtn').click()")
            e2e.wait_until(lambda: _count(page, "/jobs") == base + 1, page)
            page.evaluate("document.getElementById('settingsBtn').click()")
            page.select_option("#setPoll", "10000")
            assert polls_in(20000) == 2
            # the stored interval is what the next load arms
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert page.input_value("#setPoll") == "10000"


def test_slow_secondary_polls_are_joined_not_overlapped(browser, tmp_path):
    """/cluster answers slower than the poll interval. Every cycle that
    finds the previous request pending joins it, so the daemon sees one
    request at a time however many cycles pass."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _wait_conn(page, "live")
            cluster_before = _count(page, "/cluster")
            parked = page.faults.hang(r"/cluster")
            e2e.wait_until(lambda: len(parked) == 1, page)
            jobs_before = _count(page, "/jobs")
            # four more main polls go by while /cluster stays pending
            page.wait_for_function(
                "(n) => performance.getEntriesByName("
                "location.origin + '/jobs').length >= n",
                arg=page.evaluate(
                    "performance.getEntriesByName("
                    "location.origin + '/jobs').length"
                )
                + 4,
            )
            assert _count(page, "/jobs") >= jobs_before + 3
            assert len(parked) == 1
            assert _count(page, "/cluster") == cluster_before + 1
            # releasing it frees the slot: the next cycle asks again
            parked[0].fallback()
            e2e.wait_until(lambda: len(parked) >= 2, page)
            for route in parked[1:]:
                route.fallback()
            page.faults.clear(r"/cluster")


def test_slow_main_poll_is_never_overlapped(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.clock.install(),
        ) as page:
            _wait_conn(page, "live")
            parked = page.faults.hang(r"/jobs")
            page.clock.fast_forward(1100)
            e2e.wait_until(lambda: len(parked) == 1, page)
            page.clock.fast_forward(8000)
            # visibility resync and manual refresh join the same latch
            page.evaluate(_SET_HIDDEN, False)
            page.evaluate("document.getElementById('refreshBtn').click()")
            assert len(parked) == 1
            parked[0].fallback()
            page.wait_for_function(
                "!document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )
            page.faults.clear(r"/jobs")
            assert page.evaluate(_CONN) == "live"


def test_wallboard_reports_no_signal_and_stale_data(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "#tv",
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.clock.install(),
            wait_rows=False,
        ) as page:
            page.wait_for_selector("#wbGrid .wb-tile")
            assert not page.evaluate(
                "document.getElementById('wallboard').classList"
                ".contains('stale')"
            )
            page.faults.abort(r"/jobs")
            page.clock.fast_forward(1200)
            page.wait_for_function(
                "document.getElementById('wbNoSignalMsg').textContent"
                ".includes('NO SIGNAL')"
            )
            assert page.evaluate(
                "document.getElementById('wallboard').classList"
                ".contains('stale')"
            )
            # countdown tiles freeze at an explicit marker
            page.clock.fast_forward(1100)
            page.wait_for_function(
                "[...document.querySelectorAll('#wbGrid [data-next]')]"
                ".every((e) => e.textContent === 'stale')"
            )
            assert "last update" in page.inner_text("#wbNoSignalAge")
            page.faults.clear(r"/jobs")
            page.clock.fast_forward(1200)
            page.wait_for_function(
                "!document.getElementById('wallboard').classList"
                ".contains('stale')"
            )
            # polling paused with the daemon up: data ages into STALE DATA
            page.evaluate(_SET_HIDDEN, False)
            parked = page.faults.hang(r"/jobs")
            page.clock.fast_forward(17000)
            page.wait_for_function(
                "document.getElementById('wbNoSignalMsg').textContent"
                ".includes('STALE DATA')"
            )
            for route in parked:
                route.fallback()
            page.faults.clear(r"/jobs")
