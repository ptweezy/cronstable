"""Test job action requests and their visible results.

The dashboard uses the daemon in ``tests/_web_e2e.py``. Actions reach its
handlers unless ``page.route`` injects a failure, such as a 500 response or
a dropped connection. Tests check the request method, path, body, and bearer
header, along with notifications, row state, and daemon run records.

Cases cover starting jobs, pool queuing, conflicts, cancellation, pause
notes and authors, and resuming jobs. Drawer buttons, keyboard shortcuts,
and command palette entries must produce the same requests.

Bulk action tests cover running failed jobs, sequential requests with a
delay between them, partial failures, stopping requests, empty job sets,
a 401 response during an operation, copied incident summaries, and queued
pool entry cancellation.
"""

import json

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _palette(page, query):
    """Open the palette, type ``query`` once the input holds focus."""
    page.keyboard.press("Control+k")
    page.wait_for_function(
        "document.activeElement === document.getElementById('paletteInput')"
    )
    page.keyboard.type(query)


def _posts(page):
    return [
        (r["method"], r["path"])
        for r in page.faults.requests
        if r["method"] != "GET"
    ]


def _failing_jobs(n=3):
    return [
        e2e.job("fail-{}".format(i), "echo boom-{} >&2; exit 4".format(i))
        for i in range(1, n + 1)
    ]


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


def test_run_button_starts_the_job(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.FULL_TOKEN,
            prefs={"pollMs": 1000},
        ) as page:
            page.faults.record()
            assert e2e.row_status(page, "alpha-ok") == "Pending"
            _click(page, '#rows [data-run="alpha-ok"]')
            assert "ok" in e2e.wait_toast(page, "▶ started alpha-ok")
            sent = page.faults.sent("POST", "/jobs/alpha-ok/start")
            assert len(sent) == 1
            assert sent[0]["body"] is None
            assert sent[0]["headers"]["authorization"] == (
                "Bearer " + e2e.FULL_TOKEN
            )
            e2e.wait_row_status(page, "alpha-ok", "OK")
            assert (
                page.inner_text('#rows tr[data-job="alpha-ok"] .exit')
                == "exit 0"
            )
            # the toast leaves on its own
            page.wait_for_function(
                "document.querySelectorAll('#toasts .toast').length === 0"
            )
        last = daemon.jobs(e2e.FULL_TOKEN)["alpha-ok"]["last_run"]
        assert last["outcome"] == "success"


def test_run_failure_shows_in_row_verdict_and_title(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            _click(page, '#rows [data-run="beta-fail"]')
            e2e.wait_toast(page, "started beta-fail")
            e2e.wait_row_status(page, "beta-fail", "Failed")
            assert (
                page.inner_text('#rows tr[data-job="beta-fail"] .exit')
                == "exit 3"
            )
            page.wait_for_function(
                "document.getElementById('vHead').textContent === "
                "'JOB FAILING — beta-fail'"
            )
            assert "exit 3 · command exited with code 3" in page.inner_text(
                "#vSub"
            )
            page.wait_for_function(
                "document.title === 'beta-fail failing · cronstable'"
            )
            assert page.inner_text("#summary").split()[4:6] == [
                "1",
                "failing",
            ]


def test_run_queued_behind_a_pool_reports_202(browser, tmp_path):
    jobs = [
        e2e.job("pool-a", "sleep 60", pool="db"),
        e2e.job("pool-b", "echo b", pool="db"),
    ]
    with e2e.Daemon(tmp_path, jobs=jobs, pools={"db": {"slots": 1}}) as daemon:
        status, _ = daemon.api("POST", "/jobs/pool-a/start")
        assert status == 202
        e2e.wait_until(lambda: daemon.jobs()["pool-a"]["running"])
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _click(page, '#rows [data-run="pool-b"]')
            assert "ok" in e2e.wait_toast(page, "Queued pool-b")
            e2e.wait_row_status(page, "pool-b", "Queued")
            # the row offers to queue another, and the chip counts the wait
            page.wait_for_function(
                "document.querySelector('#rows [data-run=\"pool-b\"]')"
                ".textContent === 'Queue another'"
            )
            assert "db · 1 queued" in page.text_content(
                '#rows tr[data-job="pool-b"] .poolchip'
            )
            page.wait_for_function(
                "document.getElementById('poolMeta').textContent === "
                "'1 waiting'"
            )
            # canceling the queued entry from the pool card
            _click(page, '#rows tr[data-job="pool-b"] .poolchip')
            page.wait_for_selector("#poolBody details[open]")
            page.click("#poolBody [data-cancel-queue]")
            assert "ok" in e2e.wait_toast(page, "Queued work cancelled")
            cancels = [
                p
                for m, p in _posts(page)
                if p.startswith("/pools/db/queue/") and p.endswith("/cancel")
            ]
            assert len(cancels) == 1
            page.wait_for_function(
                "document.getElementById('poolMeta').textContent === "
                "'0 waiting'"
            )
            e2e.wait_row_status(page, "pool-b", "Pending")


def test_run_409_shows_the_daemons_reason(browser, tmp_path):
    """The job is disabled by a config reload after the page last polled;
    the stale Run button receives a 409 response from the daemon."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.faults.record()
            jobs = [dict(j) for j in daemon.config["jobs"]]
            next(j for j in jobs if j["name"] == "alpha-ok")["enabled"] = False
            daemon.reload(jobs=jobs)
            e2e.wait_until(
                lambda: daemon.jobs()["alpha-ok"]["enabled"] is False
            )
            _click(page, '#rows [data-run="alpha-ok"]')
            assert "err" in e2e.wait_toast(page, "job 'alpha-ok' is disabled")
            assert _posts(page) == [("POST", "/jobs/alpha-ok/start")]
            # the refresh catches the row up: Run renders disabled
            _click(page, "#refreshBtn")
            e2e.wait_row_status(page, "alpha-ok", "Disabled")
            assert page.evaluate(
                "document.querySelector('#rows [data-run=\"alpha-ok\"]')"
                ".disabled"
            )
            # a disabled button sends nothing
            _click(page, '#rows [data-run="alpha-ok"]')
            page.keyboard.press("j")
            page.keyboard.press("r")
            assert _posts(page) == [("POST", "/jobs/alpha-ok/start")]


@pytest.mark.parametrize(
    "fault,toast",
    [
        ("500", "could not start (HTTP 500)"),
        ("404", "could not start (HTTP 404)"),
        ("abort", "could not start alpha-ok"),
        ("409-empty", "Job could not be queued"),
    ],
)
def test_run_failures_are_reported(browser, tmp_path, fault, toast):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            path = r"/jobs/alpha-ok/start"
            if fault == "abort":
                page.faults.abort(path)
            elif fault == "409-empty":
                page.faults.status(path, 409, {})
            else:
                page.faults.status(path, int(fault))
            _click(page, '#rows [data-run="alpha-ok"]')
            assert "err" in e2e.wait_toast(page, toast)
            assert e2e.row_status(page, "alpha-ok") == "Pending"
        assert daemon.jobs()["alpha-ok"]["last_run"] is None


# --------------------------------------------------------------------------
# cancel
# --------------------------------------------------------------------------


def test_cancel_stops_a_real_run(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.api("POST", "/jobs/gamma-slow/start")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            e2e.wait_row_status(page, "gamma-slow", "Running")
            # a running row swaps Run for Stop
            assert not page.query_selector('#rows [data-run="gamma-slow"]')
            _click(page, '#rows [data-cancel="gamma-slow"]')
            assert "ok" in e2e.wait_toast(page, "■ cancelled gamma-slow")
            sent = page.faults.sent("POST", "/jobs/gamma-slow/cancel")
            assert len(sent) == 1 and sent[0]["body"] is None
            e2e.wait_row_status(page, "gamma-slow", "Cancelled")
            page.wait_for_selector('#rows [data-run="gamma-slow"]')
        last = daemon.jobs()["gamma-slow"]["last_run"]
        assert last["outcome"] == "cancelled"


def test_cancel_409_when_the_run_already_ended(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.api("POST", "/jobs/gamma-slow/start")
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.wait_for_selector('#rows [data-cancel="gamma-slow"]')
            daemon.api("POST", "/jobs/gamma-slow/cancel")
            e2e.wait_until(lambda: not daemon.jobs()["gamma-slow"]["running"])
            _click(page, '#rows [data-cancel="gamma-slow"]')
            assert "err" in e2e.wait_toast(page, "gamma-slow is not running")


@pytest.mark.parametrize(
    "fault,toast",
    [
        ("500", "could not cancel (HTTP 500)"),
        ("abort", "could not cancel gamma-slow"),
    ],
)
def test_cancel_failures_are_reported(browser, tmp_path, fault, toast):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.api("POST", "/jobs/gamma-slow/start")
        with e2e.open_page(browser, daemon.url) as page:
            page.wait_for_selector('#rows [data-cancel="gamma-slow"]')
            path = r"/jobs/gamma-slow/cancel"
            if fault == "abort":
                page.faults.abort(path)
            else:
                page.faults.status(path, 500)
            _click(page, '#rows [data-cancel="gamma-slow"]')
            assert "err" in e2e.wait_toast(page, toast)
        assert daemon.jobs()["gamma-slow"]["running"]


# --------------------------------------------------------------------------
# pause / resume
# --------------------------------------------------------------------------


def test_pause_and_resume_round_trip(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _click(page, '#rows [data-pause="alpha-ok"]')
            assert "ok" in e2e.wait_toast(page, "⏸ paused alpha-ok")
            e2e.wait_row_status(page, "alpha-ok", "Paused")
            paused = daemon.jobs()["alpha-ok"]["paused"]
            assert paused and paused["by"] == "api"
            # the row: a countdown chip, a Resume button, no next-run cell
            row = '#rows tr[data-job="alpha-ok"]'
            assert page.inner_text(row + " .ha-chip.paused").startswith("⏸")
            assert (
                page.get_attribute(
                    row + " .ha-chip.paused [data-until-iso]", "data-until-iso"
                )
                == paused["until"]
            )
            assert page.inner_text(row + " .col-next") == "—"
            assert page.inner_text("#summary").endswith("paused")
            # a manual run stays available while paused
            assert page.query_selector('#rows [data-run="alpha-ok"]')
            _click(page, '#rows [data-resume="alpha-ok"]')
            assert "ok" in e2e.wait_toast(page, "▶ resumed alpha-ok")
            e2e.wait_row_status(page, "alpha-ok", "Pending")
            assert daemon.jobs()["alpha-ok"]["paused"] is None
            assert _posts(page) == [
                ("POST", "/jobs/alpha-ok/pause"),
                ("POST", "/jobs/alpha-ok/resume"),
            ]
            assert all(
                r["body"] is None
                for r in page.faults.requests
                if r["method"] == "POST"
            )


def test_pause_note_and_author_are_displayed(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        status, _ = daemon.api(
            "POST",
            "/jobs/alpha-ok/pause",
            body={
                "note": "db maintenance window",
                "by": "parker",
                "durationSeconds": 3600,
            },
        )
        assert status == 200
        with e2e.open_page(browser, daemon.url) as page:
            e2e.wait_row_status(page, "alpha-ok", "Paused")
            chip = '#rows tr[data-job="alpha-ok"] .ha-chip.paused'
            assert page.get_attribute(chip, "title") == (
                "paused by parker: db maintenance window"
            )
            assert page.inner_text(chip + " [data-until-iso]") in (
                "in 59m",
                "in 60m",
            )
            _click(page, '#rows [data-logs="alpha-ok"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            meta = page.inner_text("#dMeta")
            assert "paused until" in meta
            assert "by parker" in meta
            assert "db maintenance window" in meta
            # the drawer's pause button doubles as resume
            page.faults.record()
            page.wait_for_function(
                "document.getElementById('dPause').textContent.trim() === "
                "'Resume'"
            )
            page.click("#dPause")
            e2e.wait_toast(page, "resumed alpha-ok")
            page.wait_for_function(
                "document.getElementById('dPause').textContent.trim() === "
                "'Pause'"
            )
            assert "paused until" not in page.inner_text("#dMeta")
            assert _posts(page) == [("POST", "/jobs/alpha-ok/resume")]


@pytest.mark.parametrize("verb", ["pause", "resume"])
def test_pause_resume_failures_are_reported(browser, tmp_path, verb):
    with e2e.Daemon(tmp_path) as daemon:
        if verb == "resume":
            daemon.api("POST", "/jobs/alpha-ok/pause")
        with e2e.open_page(browser, daemon.url) as page:
            path = "/jobs/alpha-ok/" + verb
            page.faults.status(path, 500, times=1)
            button = '#rows [data-{}="alpha-ok"]'.format(verb)
            page.wait_for_selector(button)
            _click(page, button)
            assert "err" in e2e.wait_toast(
                page, "could not {} (HTTP 500)".format(verb)
            )
            page.faults.clear(path)
            page.faults.abort(path)
            _click(page, button)
            assert "err" in e2e.wait_toast(
                page, "could not {} alpha-ok".format(verb)
            )


# --------------------------------------------------------------------------
# parallel routes: drawer buttons, keys, palette
# --------------------------------------------------------------------------


def test_drawer_buttons_send_the_same_requests(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _click(page, '#rows [data-logs="gamma-slow"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.is_enabled("#dRun")
            assert page.is_disabled("#dCancel")
            page.click("#dRun")
            e2e.wait_toast(page, "started gamma-slow")
            # the drawer reattaches its tail to the new run
            page.wait_for_function(
                "document.querySelectorAll('#term .ln.stdout').length > 3"
            )
            page.wait_for_function(
                "document.getElementById('dRun').disabled && "
                "!document.getElementById('dCancel').disabled"
            )
            assert "pid" in page.inner_text("#dMeta")
            page.click("#dCancel")
            e2e.wait_toast(page, "cancelled gamma-slow")
            page.wait_for_function(
                "document.getElementById('dCancel').disabled"
            )
            page.click("#dPause")
            e2e.wait_toast(page, "paused gamma-slow")
            assert _posts(page) == [
                ("POST", "/jobs/gamma-slow/start"),
                ("POST", "/jobs/gamma-slow/cancel"),
                ("POST", "/jobs/gamma-slow/pause"),
            ]
            # the disabled job's drawer cannot run it
            page.keyboard.press("Escape")
            _click(page, '#rows [data-logs="delta-off"]')
            page.wait_for_function(
                "document.getElementById('dName').textContent === 'delta-off'"
            )
            assert page.is_disabled("#dRun")


def test_keys_and_palette_send_the_same_requests(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            page.faults.record()
            # select gamma-slow (fifth row) and drive it from the keyboard
            for _ in range(5):
                page.keyboard.press("j")
            assert page.get_attribute("#rows tr.sel", "data-job") == (
                "gamma-slow"
            )
            page.keyboard.press("x")  # not running: inert
            page.keyboard.press("r")
            e2e.wait_toast(page, "started gamma-slow")
            e2e.wait_row_status(page, "gamma-slow", "Running")
            page.keyboard.press("r")  # already running: inert
            page.keyboard.press("x")
            e2e.wait_toast(page, "cancelled gamma-slow")
            e2e.wait_row_status(page, "gamma-slow", "Cancelled")
            page.keyboard.press("p")
            e2e.wait_toast(page, "paused gamma-slow")
            e2e.wait_row_status(page, "gamma-slow", "Paused")
            page.keyboard.press("p")
            e2e.wait_toast(page, "resumed gamma-slow")
            page.keyboard.press("c")
            e2e.wait_toast(page, "copied command")
            assert "time.sleep" in page.evaluate(
                "navigator.clipboard.readText()"
            )
            assert _posts(page) == [
                ("POST", "/jobs/gamma-slow/start"),
                ("POST", "/jobs/gamma-slow/cancel"),
                ("POST", "/jobs/gamma-slow/pause"),
                ("POST", "/jobs/gamma-slow/resume"),
            ]
            # the palette: a fuzzy query, Enter runs the top hit
            _palette(page, "run: alpha")
            page.wait_for_function(
                "document.querySelector('#paletteList .item.cur .lbl')"
                ".textContent === 'Run: alpha-ok'"
            )
            page.keyboard.press("Enter")
            e2e.wait_toast(page, "started alpha-ok")
            assert _posts(page)[-1] == ("POST", "/jobs/alpha-ok/start")


# --------------------------------------------------------------------------
# run failing
# --------------------------------------------------------------------------


def test_run_failing_restarts_each_failing_enabled_job(browser, tmp_path):
    jobs = _failing_jobs(2) + [e2e.job("fine", "echo fine")]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _click(page, "#runFailingBtn")
            assert "info" in e2e.wait_toast(page, "no failing jobs")
            assert _posts(page) == []
            for name in ("fail-1", "fail-2", "fine"):
                daemon.run_and_wait(name)
            e2e.wait_row_status(page, "fail-2", "Failed")
            e2e.wait_row_status(page, "fail-1", "Failed")
            before = {
                n: j["last_run"]["finished_at"]
                for n, j in daemon.jobs().items()
            }
            _click(page, "#runFailingBtn")
            e2e.wait_toast(page, "restarting 2 failing jobs")
            e2e.wait_toast(page, "started fail-1")
            e2e.wait_toast(page, "started fail-2")
            assert sorted(_posts(page)) == [
                ("POST", "/jobs/fail-1/start"),
                ("POST", "/jobs/fail-2/start"),
            ]

            def reran():
                now = daemon.jobs()
                return all(
                    now[n]["last_run"]["finished_at"] != before[n]
                    for n in ("fail-1", "fail-2")
                )

            e2e.wait_until(reran)
            assert (
                daemon.jobs()["fine"]["last_run"]["finished_at"]
                == (before["fine"])
            )


# --------------------------------------------------------------------------
# mitigate console
# --------------------------------------------------------------------------


def _open_mitigate(page, daemon, names):
    for name in names:
        daemon.run_and_wait(name, outcome="failure")
    page.wait_for_function(
        "(n) => document.getElementById('vHead').textContent"
        ".includes(n + ' jobs failing')",
        arg=len(names),
    )
    _click(page, "#vMitigate")
    page.wait_for_selector("#mitigateWrap.open")


def test_mitigate_sends_one_request_at_a_time(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_failing_jobs(3)) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _open_mitigate(page, daemon, ["fail-1", "fail-2", "fail-3"])
            # three failures with one exit code correlate into the headline
            assert "×3 share exit code 4" in page.inner_text("#vSub")
            assert page.inner_text("#mitTitle") == (
                "Job actions — failing jobs"
            )
            assert "these 3 jobs" in page.inner_text("#mitDesc")
            listed = page.evaluate(
                "[...document.querySelectorAll('#mitList .mj')].map((m) => "
                "[m.querySelector('.mn').textContent, "
                "m.querySelector('.ms').textContent])"
            )
            assert listed == [
                ["fail-{}".format(i), "exit 4 · command exited with code 4"]
                for i in (1, 2, 3)
            ]
            # nothing is running, so there is nothing to cancel
            page.click("#mitCancelAll")
            page.wait_for_function(
                "document.getElementById('mitLog').textContent.includes("
                "'nothing to cancel (no eligible jobs)')"
            )
            assert _posts(page) == []

            page.click("#mitStartAll")
            page.wait_for_function(
                "document.getElementById('mitStartAll').disabled && "
                "document.getElementById('mitCancelAll').disabled"
            )
            assert page.is_visible("#mitAbort")
            assert "ok" in e2e.wait_toast(page, "start: 3 ok")
            log = page.inner_text("#mitLog")
            assert "— starting 3 jobs —" in log
            for i in (1, 2, 3):
                assert "✓ start fail-{}".format(i) in log
            assert "done: 3 ok" in log
            starts = [
                r
                for r in page.faults.requests
                if r["method"] == "POST" and r["path"].endswith("/start")
            ]
            assert [r["path"] for r in starts] == [
                "/jobs/fail-{}/start".format(i) for i in (1, 2, 3)
            ]
            # staggered: each request waits out the pause after the last
            gaps = [
                later["at"] - earlier["at"]
                for earlier, later in zip(starts[:-1], starts[1:], strict=True)
            ]
            assert all(gap >= 0.28 for gap in gaps), gaps
            page.wait_for_function(
                "!document.getElementById('mitStartAll').disabled"
            )
            assert not page.is_visible("#mitAbort")


def test_mitigate_partial_failure_and_abort(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_failing_jobs(3)) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _open_mitigate(page, daemon, ["fail-1", "fail-2", "fail-3"])
            page.faults.status(r"/jobs/fail-2/start", 500)
            page.faults.abort(r"/jobs/fail-3/start")
            page.click("#mitStartAll")
            assert "err" in e2e.wait_toast(page, "start: 1 ok, 2 failed")
            log = page.inner_text("#mitLog")
            assert "✓ start fail-1" in log
            assert "✕ fail-2 (HTTP 500)" in log
            assert "✕ fail-3 (error)" in log
            assert "done: 1 ok, 2 failed" in log
            page.faults.clear(r"/jobs/fail-2/start")
            page.faults.clear(r"/jobs/fail-3/start")

            # abort after the first request leaves the rest untouched
            page.wait_for_function(
                "!document.getElementById('mitStartAll').disabled"
            )
            before = len(_posts(page))
            parked = page.faults.hang(r"/jobs/fail-1/start")
            page.click("#mitStartAll")
            e2e.wait_until(lambda: len(parked) == 1, page)
            page.click("#mitAbort")
            parked[0].fallback()
            page.wait_for_function(
                "document.getElementById('mitLog').textContent"
                ".includes('aborted (1/3 sent)')"
            )
            assert len(_posts(page)) == before + 1
            e2e.wait_toast(page, "start: 1 ok")


def test_mitigate_stops_on_a_401(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full", jobs=_failing_jobs(2)) as daemon:
        for name in ("fail-1", "fail-2"):
            daemon.run_and_wait(name, e2e.FULL_TOKEN)
        with e2e.open_page(browser, daemon.url, token=e2e.FULL_TOKEN) as page:
            page.faults.record()
            page.wait_for_function(
                "document.getElementById('vHead').textContent"
                ".includes('2 jobs failing')"
            )
            _click(page, "#vMitigate")
            page.wait_for_selector("#mitigateWrap.open")
            # the token is withdrawn mid-session
            page.evaluate("sessionStorage.removeItem('cronstable_token')")
            page.click("#mitStartAll")
            page.wait_for_function(
                "document.getElementById('mitLog').textContent"
                ".includes('unauthorized — set a token')"
            )
            assert _posts(page) == [("POST", "/jobs/fail-1/start")]
            page.wait_for_selector("#modalWrap.open")
            assert page.inner_text("#modalTitle") == "Access token required"


def test_mitigate_cancel_all_and_live_logs_handoff(browser, tmp_path):
    jobs = _failing_jobs(1) + [
        e2e.job("slow-fail", "echo start; sleep 60; exit 4"),
    ]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        daemon.run_and_wait("fail-1")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            page.wait_for_function(
                "document.getElementById('vHead').textContent"
                ".includes('fail-1')"
            )
            # the palette route lists every failing job
            _palette(page, "Review actions")
            page.keyboard.press("Enter")
            page.wait_for_selector("#mitigateWrap.open")
            assert page.inner_text("#mitDesc").startswith("Choose an action for this job. ")
            page.click("#mitStartAll")
            e2e.wait_toast(page, "start: 1 ok")
            page.click("#mitTail")
            page.wait_for_selector("#tailWrap.open")
            assert not page.evaluate(
                "document.getElementById('mitigateWrap').classList"
                ".contains('open')"
            )
            assert page.evaluate(
                "[...document.querySelectorAll('#tailChips .tnm')]"
                ".map((c) => c.textContent)"
            ) == ["fail-1"]


def test_copy_incident_summary(browser, tmp_path):
    jobs = _failing_jobs(2) + [e2e.job("pipe|name", "echo 'a|b' >&2; exit 4")]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            _open_mitigate(page, daemon, ["fail-1", "fail-2", "pipe|name"])
            page.wait_for_function(
                "document.getElementById('ver').textContent.startsWith('v')"
            )
            page.click("#mitCopy")
            assert "ok" in e2e.wait_toast(page, "copied incident summary")
            text = page.evaluate("navigator.clipboard.readText()")
            lines = text.split("\n")
            assert lines[0] == "# cronstable incident"
            assert lines[2].startswith("- when: 20")
            assert lines[3] == "- host: 127.0.0.1:{}".format(daemon.port)
            assert lines[4] == "- version: " + page.inner_text("#ver")
            assert "| job | status | exit | reason | when |" in lines
            rows = [ln for ln in lines if ln.startswith("| fail-")]
            assert len(rows) == 2
            assert rows[0].startswith(
                "| fail-1 | Failed | 4 | command exited with code 4 | "
            )
            # The Markdown table preserves the quoted text.
            json.dumps(text)
            assert any(ln.startswith("| pipe|name | Failed") for ln in lines)
