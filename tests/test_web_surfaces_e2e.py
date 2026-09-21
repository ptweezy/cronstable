"""Test dashboard panels, notifications, and pairing.

Coverage includes the wallboard, screensaver, alarms, notifications,
schedule radar, week calendar, schedule load, peer timeline, cluster card,
state inspector, run ledger, cron sandbox, and pairing QR code.

The dashboard uses ``tests/_web_e2e.py`` for jobs, runs, state storage, and
schedule forecasts. Tests supply controlled ``/cluster`` responses to
simulate gossip and lease clusters. Playwright's clock fixes the current
time for countdowns, calendar entries, and daylight saving time notices,
and advances idle timers without waiting.
"""

import base64
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


def _palette(page, label):
    page.keyboard.press("Control+k")
    page.wait_for_function("document.activeElement.id === 'paletteInput'")
    page.keyboard.type(label)
    page.keyboard.press("Enter")


# March 7, 2026, at 12:00:30 UTC: the day before New York's DST transition.
PINNED = "2026-03-07T12:00:30Z"


def _pin(page):
    page.clock.install(time=PINNED)


def _scheduled_jobs():
    return [
        e2e.job("every-five", "true", schedule="*/5 * * * *"),
        e2e.job("hourly-a", "true", schedule="0 * * * *"),
        e2e.job("hourly-b", "true", schedule="0 * * * *"),
        e2e.job(
            "nightly-ny",
            "true",
            schedule="30 2 * * *",
            timezone="America/New_York",
        ),
        e2e.job("weekly", "true", schedule="0 9 * * 1"),
        e2e.job("off", "true", schedule="* * * * *", enabled=False),
    ]


# --------------------------------------------------------------------------
# wallboard
# --------------------------------------------------------------------------


def test_wallboard_tiles_order_footer_and_hash(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        daemon.run_and_wait("beta-fail")
        daemon.api("POST", "/jobs/gamma-slow/start")
        with e2e.open_page(
            browser,
            daemon.url + "#tv",
            prefs={"pollMs": 1000},
            wait_rows=False,
        ) as page:
            page.wait_for_selector("#wbGrid .wb-tile.st-run")
            assert page.evaluate("document.body.classList.contains('tv')")
            tiles = page.evaluate(
                "[...document.querySelectorAll('#wbGrid .wb-tile')].map("
                "(t) => [t.getAttribute('data-job'), [...t.classList].find("
                "(c) => c.startsWith('st-'))])"
            )
            # Order tiles by status: failing, running, pending, OK, disabled.
            assert tiles == [
                ["beta-fail", "st-fail"],
                ["gamma-slow", "st-run"],
                ["epsilon-quiet", "st-pending"],
                ["alpha-ok", "st-ok"],
                ["delta-off", "st-disabled"],
            ]
            fail = '#wbGrid .wb-tile[data-job="beta-fail"]'
            assert page.inner_text(fail + " .wb-exit") == "· exit 3"
            assert page.query_selector(fail + " [data-ago]")
            # The running job tile updates elapsed time as the clock advances.
            run = '#wbGrid .wb-tile[data-job="gamma-slow"] [data-run-since]'
            first = page.inner_text(run)
            assert first.startswith("▶")
            page.wait_for_function(
                "([s, t]) => document.querySelector(s).textContent !== t",
                arg=[run, first],
            )
            foot = page.inner_text("#wbFoot")
            for part in ("5\njobs", "1\nfail", "1\nrun", "1\nok", "1\noff"):
                assert part.replace("\n", "") in foot.replace("\n", "")
            assert page.inner_text("#wbVerdictHead") == (
                "▲ JOB FAILING — beta-fail"
            )
            # Every tile fits on screen without clipping.
            fits = page.evaluate(
                """() => { const g = document.getElementById('wbGrid')
                  .getBoundingClientRect(); return [...document
                  .querySelectorAll('#wbGrid .wb-tile')].every((t) => {
                    const b = t.getBoundingClientRect();
                    return b.width > 0 && b.bottom <= g.bottom + 1 &&
                      b.right <= g.right + 1; }); }"""
            )
            assert fits
            assert not page.is_visible("#wbMore")
            # a tile opens that job's drawer and leaves the wallboard
            page.click(fail)
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert not page.evaluate("document.body.classList.contains('tv')")
            assert page.evaluate("location.hash") == "#job/beta-fail"
            # navigating back into #tv re-enters it and closes the drawer
            page.evaluate("location.hash = '#tv'")
            page.wait_for_function("document.body.classList.contains('tv')")
            assert page.get_attribute("#drawer", "aria-hidden") == "true"
            page.click("#wbExit")
            page.wait_for_function("!document.body.classList.contains('tv')")
            assert page.evaluate("location.hash") == ""
            daemon.api("POST", "/jobs/gamma-slow/cancel")


def test_wallboard_grid_fits_a_large_fleet(browser, tmp_path):
    jobs = [e2e.job("job-{:03d}".format(i), "true") for i in range(400)]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        daemon.run_and_wait("job-399")
        with e2e.open_page(
            browser,
            daemon.url + "#tv",
            wait_rows=False,
            viewport={"width": 1280, "height": 720},
        ) as page:
            page.wait_for_selector("#wbGrid .wb-tile")
            page.wait_for_function(
                "document.getElementById('wallboard').classList"
                ".contains('wb-compact')"
            )
            state = page.evaluate(
                """() => { const tiles = [...document.querySelectorAll(
                  '#wbGrid .wb-tile')]; const g = document.getElementById(
                  'wbGrid').getBoundingClientRect();
                  const shown = tiles.filter((t) => t.style.display !==
                    'none');
                  return { total: tiles.length, shown: shown.length,
                    clipped: shown.filter((t) => t.getBoundingClientRect()
                      .bottom > g.bottom + 1).length,
                    chip: document.getElementById('wbMore').textContent,
                    chipShown: document.getElementById('wbMore').style
                      .display !== 'none' }; }"""
            )
            assert state["total"] == 400
            assert 0 < state["shown"] < 400
            assert state["clipped"] == 0
            assert state["chipShown"]
            hidden = state["total"] - state["shown"]
            assert state["chip"] == "+{}offscreen · none failing".format(
                hidden
            )
            # a bigger screen shows more, recomputed on resize
            page.set_viewport_size({"width": 2560, "height": 1440})
            page.wait_for_function(
                "(n) => [...document.querySelectorAll('#wbGrid .wb-tile')]"
                ".filter((t) => t.style.display !== 'none').length > n",
                arg=state["shown"],
            )
            # the UI scale changes the effective viewport as well
            before = page.evaluate(
                "[...document.querySelectorAll('#wbGrid .wb-tile')]"
                ".filter((t) => t.style.display !== 'none').length"
            )
            _palette(page, "Cycle UI scale")
            page.wait_for_function(
                "(n) => [...document.querySelectorAll('#wbGrid .wb-tile')]"
                ".filter((t) => t.style.display !== 'none').length < n",
                arg=before,
            )


def test_zen_screensaver_engages_when_idle_and_healthy(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_scheduled_jobs()) as daemon:
        with e2e.open_page(
            browser,
            daemon.url + "#tv",
            prefs={"pollMs": 1000, "zenIdle": 10000},
            before_goto=_pin,
            wait_rows=False,
            reduced_motion="no-preference",
        ) as page:
            page.wait_for_selector("#wbGrid .wb-tile")
            assert page.get_attribute("#zen", "aria-hidden") == "true"
            for _ in range(12):
                page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.getElementById('zen').style.display === 'flex'"
            )
            assert page.get_attribute("#zen", "aria-hidden") == "false"
            # one dot per enabled job, and the readout names the next fire
            assert (
                page.evaluate(
                    "document.querySelectorAll('#zenField .zen-dot').length"
                )
                == 5
            )
            page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.getElementById('zenSub').textContent"
                ".includes('next every-five')"
            )
            assert "5 enabled jobs" in page.inner_text("#zenSub")
            # A keypress dismisses the screensaver and retains its action.
            page.keyboard.press("Shift")
            page.wait_for_function(
                "document.getElementById('zen').style.display === 'none'"
            )
            assert page.evaluate("document.body.classList.contains('tv')")
            # pointer movement wakes it too
            for _ in range(12):
                page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.getElementById('zen').style.display === 'flex'"
            )
            page.mouse.move(200, 200)
            page.mouse.move(260, 240)
            page.wait_for_function(
                "document.getElementById('zen').style.display === 'none'"
            )
            # switched off in Settings, it stays off
            _palette(page, "Open settings")
            page.wait_for_selector("#settingsWrap.open")
            page.evaluate("document.getElementById('setZen').click()")
            page.keyboard.press("Escape")
            for _ in range(15):
                page.clock.fast_forward(1000)
            assert (
                page.evaluate("document.getElementById('zen').style.display")
                == "none"
            )


def test_zen_never_covers_a_failure_or_a_lost_signal(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(
            browser,
            daemon.url + "#tv",
            prefs={"pollMs": 1000, "zenIdle": 5000},
            before_goto=lambda page: page.clock.install(),
            wait_rows=False,
        ) as page:
            page.wait_for_selector("#wbGrid .wb-tile.st-fail")
            for _ in range(10):
                page.clock.fast_forward(1000)
            assert (
                page.evaluate("document.getElementById('zen').style.display")
                == "none"
            )
            # The incident strip updates the duration while the job is failing.
            page.wait_for_function(
                "document.getElementById('wbIncident').textContent"
                ".startsWith('◉ INCIDENT +')"
            )
            assert page.evaluate(
                "document.querySelector('#wbGrid .wb-tile.st-fail')"
                ".classList.contains('escalate')"
            )
            level = float(
                page.evaluate(
                    "document.getElementById('wbAlarm').style.opacity"
                )
            )
            assert level > 0.1
            # Pressing a acknowledges the alarm and updates the tile and strip.
            page.keyboard.press("a")
            e2e.wait_toast(page, "alarm acknowledged")
            page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.getElementById('wbIncident').textContent"
                ".endsWith('· ack')"
            )
            assert not page.evaluate(
                "document.querySelector('#wbGrid .wb-tile.st-fail')"
                ".classList.contains('escalate')"
            )
            assert (
                float(
                    page.evaluate(
                        "document.getElementById('wbAlarm').style.opacity"
                    )
                )
                <= level
            )
            # Pressing a again has no effect when there are no new alarms.
            page.keyboard.press("a")
            assert len(e2e.toasts(page)) == 1


# --------------------------------------------------------------------------
# sound and notifications
# --------------------------------------------------------------------------

# Records every oscillator the page starts instead of making sound.
_AUDIO_STUB = """
window.__tones = [];
class FakeParam {
  setValueAtTime(v) { this.v = v; }
  linearRampToValueAtTime(v) { this.v = v; }
  exponentialRampToValueAtTime(v) { this.v = v; }
}
class FakeNode { connect(next) { return next; } }
class FakeOsc extends FakeNode {
  constructor() { super(); this.frequency = new FakeParam(); }
  start() { window.__tones.push({ type: this.type,
    f: this.frequency.v, at: Date.now() }); }
  stop() {}
}
class FakeGain extends FakeNode {
  constructor() { super(); this.gain = new FakeParam(); }
}
window.AudioContext = class {
  constructor() { this.state = "running"; this.currentTime = 0;
    this.destination = new FakeNode(); window.__audioMade =
      (window.__audioMade || 0) + 1; }
  createOscillator() { return new FakeOsc(); }
  createGain() { return new FakeGain(); }
  resume() { return Promise.resolve(); }
  suspend() { this.state = "suspended"; return Promise.resolve(); }
};
"""


def _tones(page, kind):
    return page.evaluate(
        "(k) => window.__tones.filter((t) => t.type === k).length", kind
    )


def _wait_tones(page, kind, n):
    page.wait_for_function(
        "([k, n]) => window.__tones.filter((t) => t.type === k).length >= n",
        arg=[kind, n],
    )


def test_audible_cues_and_the_standing_alarm(browser, tmp_path):
    """Check the waveforms used for success, failure, and active alarms.

    Success uses one sine wave, failure uses two sawtooth pulses, and an
    active alarm uses pairs of square pulses.
    """
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            init_scripts=[_AUDIO_STUB],
            before_goto=lambda page: page.clock.install(),
        ) as page:
            # off by default: no audio context is even created
            daemon.run_and_wait("beta-fail")
            daemon.run_and_wait("alpha-ok")
            page.clock.fast_forward(1100)
            e2e.wait_row_status(page, "beta-fail", "Failed")
            assert page.evaluate("window.__audioMade || 0") == 0
            _click(page, "#settingsBtn")
            page.evaluate("document.getElementById('setSound').click()")
            # Enabling sound plays one confirmation tone.
            _wait_tones(page, "sine", 1)
            assert page.evaluate("window.__audioMade") == 1
            page.keyboard.press("Escape")

            # The alarm repeats until the failure is acknowledged.
            for _ in range(8):
                page.clock.fast_forward(1000)
            _wait_tones(page, "square", 4)
            page.evaluate("document.activeElement.blur()")
            page.keyboard.press("a")
            e2e.wait_toast(page, "alarm acknowledged")
            silenced = _tones(page, "square")
            for _ in range(15):
                page.clock.fast_forward(1000)
            assert _tones(page, "square") == silenced

            # Success plays one sine wave without restarting the alarm.
            daemon.run_and_wait("alpha-ok")
            page.clock.fast_forward(1100)
            _wait_tones(page, "sine", 2)
            assert _tones(page, "sawtooth") == 0
            assert _tones(page, "square") == silenced

            # a fresh failure: the two-step cue, and the alarm is back
            daemon.run_and_wait("beta-fail")
            page.clock.fast_forward(1100)
            _wait_tones(page, "sawtooth", 2)
            for _ in range(8):
                page.clock.fast_forward(1000)
            _wait_tones(page, "square", silenced + 2)

            # Changing the volume plays a tone and saves the setting.
            _click(page, "#settingsBtn")
            sines = _tones(page, "sine")
            page.select_option("#setVol", "25")
            _wait_tones(page, "sine", sines + 1)
            assert (
                page.evaluate("localStorage.getItem('cronstable.volume')")
                == "25"
            )
            # switching cues off stops the alarm with everything else
            page.evaluate("document.getElementById('setSound').click()")
            assert (
                page.evaluate("localStorage.getItem('cronstable.sound')")
                == "false"
            )
            quiet = page.evaluate("window.__tones.length")
            for _ in range(10):
                page.clock.fast_forward(1000)
            assert page.evaluate("window.__tones.length") == quiet


@pytest.mark.xfail(
    strict=True,
    reason=(
        "fleetSound requires a previous completed run (prev !== undefined). "
        "A job's first failure after daemon startup therefore produces no "
        "failure sound or wallboard transition; later failures do."
    ),
)
def test_first_failure_of_a_never_run_job_plays_the_fail_cue(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000, "sound": True},
            init_scripts=[_AUDIO_STUB],
        ) as page:
            page.evaluate("document.body.click()")
            daemon.run_and_wait("beta-fail")
            e2e.wait_row_status(page, "beta-fail", "Failed")
            assert _tones(page, "sawtooth") == 2


_NOTIFY_STUB = """
window.__notes = [];
const RealNotification = window.Notification;
window.Notification = class {
  constructor(title, opts) { window.__notes.push({ title, body:
    (opts || {}).body }); }
  static get permission() { return window.__perm || "default"; }
  static requestPermission() { window.__asked = (window.__asked || 0) + 1;
    window.__perm = window.__grant || "denied";
    return Promise.resolve(window.__perm); }
};
"""


def test_failure_notifications(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            init_scripts=[_NOTIFY_STUB],
        ) as page:
            _click(page, "#settingsBtn")
            # Denying notification permission leaves notifications disabled.
            page.evaluate("document.getElementById('setNotify').click()")
            assert "err" in e2e.wait_toast(page, "permission denied")
            assert not page.is_checked("#setNotify")
            page.evaluate("window.__grant = 'granted'; window.__perm = null")
            page.evaluate("document.getElementById('setNotify').click()")
            assert "ok" in e2e.wait_toast(page, "notifications on")
            assert page.is_checked("#setNotify")
            assert page.evaluate("window.__asked") == 2
            page.keyboard.press("Escape")
            # An existing failure does not generate a new notification.
            assert page.evaluate("window.__notes") == []
            daemon.run_and_wait("alpha-ok")
            daemon.run_and_wait("beta-fail")
            page.wait_for_function("window.__notes.length === 1")
            note = page.evaluate("window.__notes[0]")
            assert note["title"] == "cronstable: job failed"
            assert note["body"] == (
                "beta-fail (exit 3)\ncommand exited with code 3"
            )
            # Later polls do not repeat the notification for the same failure.
            page.wait_for_function(
                "(n) => performance.getEntriesByName(location.origin + "
                "'/jobs').length >= n",
                arg=page.evaluate(
                    "performance.getEntriesByName(location.origin + "
                    "'/jobs').length"
                )
                + 2,
            )
            assert len(page.evaluate("window.__notes")) == 1


# --------------------------------------------------------------------------
# radar, week calendar, schedule load
# --------------------------------------------------------------------------


def test_radar_lists_the_next_fires_in_order(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_scheduled_jobs()) as daemon:
        with e2e.open_page(
            browser, daemon.url, before_goto=_pin, prefs={"pollMs": 0}
        ) as page:
            _click(page, "#radarBtn")
            page.wait_for_selector("#radarFeed li[data-job]")
            feed = page.evaluate(
                "[...document.querySelectorAll('#radarFeed li')].map((li) "
                "=> [li.querySelector('.nm').textContent, "
                "li.querySelector('.cd').textContent])"
            )
            # At 12:00:30, */5 runs in 4 min 30 sec; hourly jobs run in
            # 59 min 30 sec.
            assert feed[0] == ["every-five", "04:30"]
            assert [name for name, _ in feed[1:3]] == ["hourly-a", "hourly-b"]
            assert feed[1][1] == "59:30"
            assert "off" not in [name for name, _ in feed]
            assert page.inner_text("#radarMeta") == "5 UPCOMING"
            # only fires inside ten minutes get a mark on the track
            assert page.evaluate(
                "[...document.querySelectorAll('#radarTrack .rt-mk')]"
                ".map((m) => m.getAttribute('data-job'))"
            ) == ["every-five"]
            left = page.evaluate(
                "parseFloat(document.querySelector('#radarTrack .rt-mk')"
                ".style.left)"
            )
            assert 44 < left < 46  # 4.5 of 10 minutes
            # the tick counts the feed down in place
            page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.querySelector('#radarFeed .cd').textContent === "
                "'04:29'"
            )
            # past the fire, the feed rolls to the next one
            page.clock.fast_forward(270000)
            page.clock.fast_forward(1000)
            page.wait_for_function(
                "document.querySelector('#radarFeed .cd').textContent"
                ".startsWith('04:')"
            )
            # a feed entry opens the job
            page.click("#radarFeed li[data-job='hourly-a']")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.inner_text("#dName") == "hourly-a"


def test_week_calendar_and_ics_link(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full", jobs=_scheduled_jobs()) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.FULL_TOKEN,
            before_goto=_pin,
            prefs={"pollMs": 0},
            timezone_id="UTC",
        ) as page:
            _click(page, "#weekBtn")
            page.wait_for_selector("#weekBody .wk-day")
            days = page.evaluate(
                "[...document.querySelectorAll('#weekBody .wk-day')].map("
                "(d) => [d.querySelector('.wk-head b').textContent, "
                "d.classList.contains('today'), [...d.querySelectorAll("
                "'.wk-ev')].map((e) => e.getAttribute('data-job'))])"
            )
            assert len(days) == 7
            assert days[0][1] is True and not any(d[1] for d in days[1:])
            assert days[0][0] == "today"
            assert days[1][0].upper().startswith("SUN")
            # the weekly Monday job appears on exactly one day
            weekly_days = [d[0] for d in days if "weekly" in d[2]]
            assert len(weekly_days) == 1
            assert weekly_days[0].upper().startswith("MON")
            # the nightly New York job shows once per day from tomorrow
            nightly = [d[2].count("nightly-ny") for d in days]
            assert sum(nightly) >= 6 and max(nightly) == 1
            # Frequent jobs appear in the calendar's frequency summary.
            hum = page.evaluate(
                "[...document.querySelectorAll('#weekBody .wk-fq')]"
                ".map((f) => f.getAttribute('data-job'))"
            )
            assert "every-five" in hum
            assert page.query_selector("#weekNow")
            assert "browser time" in page.inner_text("#weekMeta").lower()
            # a chip opens the job on its schedule tab
            page.click("#weekBody .wk-ev[data-job='weekly']")
            page.wait_for_selector('.pane.active[data-pane="schedule"]')
            assert page.inner_text("#dName") == "weekly"
            page.keyboard.press("Escape")

            # Include the token in the feed URL for clients that cannot
            # send bearer authentication headers.
            href = page.get_attribute("#icsLink", "href")
            assert href == "/calendar.ics?token=" + e2e.FULL_TOKEN
        status, body = daemon.api("GET", href)
        assert status == 200
        assert body.startswith("BEGIN:VCALENDAR")
        assert "SUMMARY:weekly" in body
        assert daemon.api("GET", "/calendar.ics")[0] == 401


def test_ics_link_without_a_token_has_no_query(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"week": True}) as page:
            page.wait_for_function(
                "document.getElementById('icsLink').getAttribute('href') "
                "=== '/calendar.ics'"
            )


def test_schedule_load_duplicates_and_slot_suggestions(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_scheduled_jobs()) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            page.faults.record()
            _click(page, "#pressBtn")
            page.wait_for_selector("#pressBody .press-grid")
            summary = page.inner_text("#pressBody .press-summary")
            assert "scheduled runs in the next 24h" in summary
            assert "not counting 1 disabled" in summary
            assert (
                page.evaluate(
                    "document.querySelectorAll('#pressBody .press-cell')"
                    ".length"
                )
                == 24 * 60
            )
            # Hourly jobs and */5 jobs overlap at :00, so its cell uses
            # a stronger color than :05 in the same row.
            opacity = page.evaluate(
                "(() => { const row = document.querySelectorAll('#pressBody "
                ".press-cells')[3].children; return [0, 5, 7].map((m) => "
                "parseFloat(row[m].style.opacity || '0')); })()"
            )
            assert opacity[0] > opacity[1] > opacity[2] == 0
            # the two identical hourly schedules are called out
            dup = page.inner_text("#pressBody .press-dup")
            assert "0 * * * *" in dup and "× 2" in dup
            assert "hourly-a" in dup and "hourly-b" in dup
            assert page.inner_text("#pressMeta") == "NEXT 24H · UTC"

            page.click('#pressBody [data-suggest="hourly"]')
            page.wait_for_selector("#pressSuggestOut .chip")
            sent = page.faults.sent("GET", "/schedule/suggest")
            assert sent[-1]["query"] == "period=hourly"
            text = page.inner_text("#pressSuggestOut")
            assert text.startswith("suggested hourly:")
            expr = page.get_attribute("#pressSuggestOut .chip", "data-expr")
            minute = int(expr.split()[0])
            # the suggestion avoids the crowded minutes
            assert minute % 5 != 0
            page.click("#pressSuggestOut .chip")
            e2e.wait_toast(page, "copied schedule")
            assert page.evaluate("navigator.clipboard.readText()") == expr
            page.click('#pressBody [data-suggest="daily"]')
            page.wait_for_function(
                "document.getElementById('pressSuggestOut').textContent"
                ".startsWith('suggested daily:')"
            )
            # Changing the time zone updates the request and clears
            # suggestions.
            page.select_option("#pressTz", "browser")
            page.wait_for_function(
                "document.getElementById('pressSuggestOut').textContent === ''"
            )
            assert (
                "tz="
                in page.faults.sent("GET", "/schedule/pressure")[-1]["query"]
            )
            # a duplicate's job chip opens its schedule tab
            page.click("#pressBody .press-dup [data-job='hourly-b']")
            page.wait_for_selector('.pane.active[data-pane="schedule"]')
            assert page.inner_text("#dName") == "hourly-b"


def test_activity_heatmap_from_real_runs(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        daemon.run_and_wait("beta-fail")
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _click(page, "#heatBtn")
            page.wait_for_selector("#heatBody [data-job]")
            assert len(page.faults.sent("GET", "/activity")) == 1
            rows = page.evaluate(
                "[...document.querySelectorAll('#heatBody [data-job]')]"
                ".map((r) => r.getAttribute('data-job'))"
            )
            assert "alpha-ok" in rows and "beta-fail" in rows
            page.click("#heatBody [data-job='beta-fail']")
            page.wait_for_selector('.pane.active[data-pane="history"]')
            page.wait_for_selector("#historyPane .runtable")
            assert (
                page.evaluate(
                    "document.querySelectorAll("
                    "'#historyPane .runtable tbody tr').length"
                )
                == 2
            )
            stats = page.inner_text("#historyPane .stats")
            assert "0 / 2" in stats.replace("\n", " ")


# --------------------------------------------------------------------------
# cluster card, swimlane, lease view
# --------------------------------------------------------------------------


def _gossip(status="agreed", **over):
    body = {
        "enabled": True,
        "backend": "gossip",
        "node_name": "node-a",
        "distribution": "single-leader",
        "elect_leader": True,
        "quorate": True,
        "quorum": 2,
        "is_leader": True,
        "leader": "node-a",
        "conflict": False,
        "interval": 5,
        "peers": [
            {
                "host": "node-b:8443",
                "node_name": "node-b",
                "status": status,
                "job_set_id": "v1:0123456789abcdef",
            },
            {
                "host": "node-c:8443",
                "node_name": "node-c",
                "status": "agreed",
                "job_set_id": "v1:0123456789abcdef",
            },
        ],
    }
    body.update(over)
    return body


def test_cluster_card_summary_alert_and_stale_state(browser, tmp_path):
    current = {"body": _gossip()}
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.faults.json(
                r"/cluster", lambda request: current["body"]
            ),
        ) as page:
            page.wait_for_selector("#clusterRows tr")
            assert page.text_content("#clusterSummary") == (
                "node-a · 2/2 agreed · quorum ok (need 2) · leader"
            )
            rows = page.evaluate(
                "[...document.querySelectorAll('#clusterRows tr')].map("
                "(tr) => [...tr.cells].map((c) => c.textContent))"
            )
            assert [r[0] for r in rows] == ["node-b:8443", "node-c:8443"]
            assert rows[0][-1] == "0123456789ab"
            # The tab title includes the cluster node's name.
            page.wait_for_function("document.title.endsWith('· node-a')")
            assert not page.is_visible("#verdictBar")

            # Loss of quorum updates the verdict bar and tab title.
            current["body"] = _gossip(
                "unreachable", quorate=False, is_leader=False, leader=None
            )
            page.wait_for_function(
                "document.getElementById('clusterSummary').textContent"
                ".includes('quorum lost')"
            )
            assert "no quorum" in page.text_content("#clusterSummary")
            page.wait_for_function(
                "document.getElementById('vHead').textContent.startsWith("
                "'CLUSTER ALERT')"
            )
            assert "NO QUORUM" in page.inner_text("#vHead")
            assert "this node: node-a" in page.inner_text("#vSub")
            page.wait_for_function(
                "document.title.startsWith('cluster alert') || "
                "document.title.startsWith('no quorum')"
            )
            assert page.evaluate(
                "document.querySelector('#clusterRows .cdot')"
                ".classList.contains('unreachable')"
            )

            # The summary identifies duplicate node names.
            current["body"] = _gossip(conflict=True, conflict_names=["node-b"])
            page.wait_for_function(
                "document.getElementById('clusterSummary').textContent"
                ".includes('duplicate nodeName (node-b)')"
            )
            assert "standing down (conflict)" in page.text_content(
                "#clusterSummary"
            )

            # A failed /cluster request marks the card as stale.
            page.faults.clear(r"/cluster")
            page.faults.status(r"/cluster", 503)
            page.wait_for_function(
                "document.getElementById('clusterCard').classList"
                ".contains('stale')"
            )
            assert page.text_content("#clusterSummary") == (
                "cluster status unavailable (HTTP 503)"
            )
            page.faults.clear(r"/cluster")
            # The cluster card disappears for a standalone daemon.
            page.wait_for_function(
                "document.getElementById('clusterCard').style.display === "
                "'none'"
            )
            page.wait_for_function("document.title.endsWith('cronstable')")


def test_peer_swimlane_accumulates_snapshots(browser, tmp_path):
    current = {"body": _gossip()}
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.faults.json(
                r"/cluster", lambda request: current["body"]
            ),
        ) as page:
            page.wait_for_function(
                "document.getElementById('swimBtn').style.display === ''"
            )
            _click(page, "#swimBtn")
            page.wait_for_selector("#swimPanel svg")
            assert page.get_attribute("#swimPanel svg", "aria-label") == (
                "peer status timeline"
            )
            first = page.evaluate(
                "document.querySelectorAll('#swimPanel svg rect').length"
            )
            current["body"] = _gossip("drifted")
            page.wait_for_function(
                "(n) => document.querySelectorAll('#swimPanel svg rect')"
                ".length > n",
                arg=first,
            )
            text = page.inner_text("#swimPanel")
            assert "node-b" in text and "node-c" in text
            # the panel choice and the recorded timeline are stored under
            # separate keys, and both survive a reload
            assert (
                page.evaluate("localStorage.getItem('cronstable.swim')")
                == "true"
            )
            stored = json.loads(
                page.evaluate("localStorage.getItem('cronstable.swimBuf')")
            )
            assert len(stored["buf"]) >= 2
            assert stored["buf"][-1]["peers"][0] == {
                "h": "node-b:8443",
                "s": "drifted",
                "e": None,
            }
            page.reload()
            page.wait_for_selector("#swimPanel svg")
            assert (
                page.evaluate(
                    "document.querySelectorAll('#swimPanel svg rect').length"
                )
                >= first
            )
            # closing the panel keeps it closed across the next reload
            _click(page, "#swimBtn")
            page.wait_for_function(
                "getComputedStyle(document.getElementById('swimPanel'))"
                ".display === 'none'"
            )
            page.reload()
            page.wait_for_selector("#clusterRows tr")
            assert not page.is_visible("#swimPanel")


def test_lease_backend_renders_the_lease_block(browser, tmp_path):
    lease = {
        "enabled": True,
        "backend": "kubernetes",
        "node_name": "pod-1",
        "elect_leader": True,
        "quorate": True,
        "is_leader": False,
        "leader": "pod-0",
        "fleet": False,
        "lease": {
            "holder": "pod-0",
            "expiry": "2099-01-01T00:00:00.123+00:00",
            "fence": 7,
            "electionName": "cronstable",
            "identity": "pod-1",
        },
    }
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            before_goto=lambda page: page.faults.json(r"/cluster", lease),
        ) as page:
            page.wait_for_selector("#clusterLeaseRows tr")
            assert page.text_content("#clusterSummary") == (
                "pod-1 · kubernetes · follower (leader: pod-0)"
            )
            rows = dict(
                page.evaluate(
                    "[...document.querySelectorAll('#clusterLeaseRows tr')]"
                    ".map((tr) => [tr.cells[0].textContent, "
                    "tr.cells[1].textContent])"
                )
            )
            assert rows["held by"] == "pod-0"
            assert rows["fence"] == "7"
            assert rows["election"] == "cronstable"
            assert rows["node identity"] == "pod-1"
            assert rows["expires"].endswith("· 2099-01-01 00:00:00")
            # no peer set to chart on a lease backend
            assert not page.is_visible("#swimBtn")
            assert not page.is_visible("#clusterTable")


# --------------------------------------------------------------------------
# state inspector
# --------------------------------------------------------------------------


def test_state_inspector_reads_the_real_store(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        daemon.run_and_wait("alpha-ok")
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            page.wait_for_function(
                "document.getElementById('stateBtn').style.display === ''"
            )
            assert not page.is_visible("#stateCard")
            _click(page, "#stateBtn")
            page.wait_for_selector("#stateTabs button")
            assert "filesystem" in page.text_content("#stateTopo")
            assert str(tmp_path) in page.text_content("#stateMeta")
            tabs = page.evaluate(
                "[...document.querySelectorAll('#stateTabs button')]"
                ".map((b) => b.getAttribute('data-sttab'))"
            )
            assert tabs[0] == "overview" and "runs" in tabs
            assert "records/runs" in page.inner_text("#stateBody")
            # drill into the run records of one job
            _click(page, '#stateTabs button[data-sttab="runs"]')
            page.wait_for_selector("#stateBody [data-stscope]")
            assert "metadata only" in page.inner_text("#stateBody")
            scopes = page.evaluate(
                "[...document.querySelectorAll('#stateBody [data-stscope]')]"
                ".map((b) => b.getAttribute('data-stscope'))"
            )
            target = next(s for s in scopes if s.endswith("alpha-ok"))
            page.click('#stateBody [data-stscope="{}"]'.format(target))
            page.wait_for_selector("#stateDetail pre")
            records = [
                json.loads(line)
                for line in page.inner_text("#stateDetail pre").splitlines()
            ]
            assert records and all(
                r.get("outcome") == "success" for r in records
            )
            sent = page.faults.sent("GET", "/state/records")
            assert sent and "alpha-ok" in sent[-1]["query"]
            # the closed inspector stops polling /state
            _click(page, "#stateBtn")
            assert not page.is_visible("#stateCard")
            count = len(page.faults.sent("GET", "/state"))
            _click(page, "#refreshBtn")
            page.wait_for_function(
                "!document.getElementById('refreshBtn').classList"
                ".contains('spin')"
            )
            assert len(page.faults.sent("GET", "/state")) == count


def test_state_inspector_stays_hidden_without_a_store(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.wait_for_function(
                "performance.getEntriesByName(location.origin + '/state')"
                ".length >= 1"
            )
            assert not page.is_visible("#stateBtn")
            assert not page.is_visible("#stateCard")


# --------------------------------------------------------------------------
# run ledger (IndexedDB)
# --------------------------------------------------------------------------

_LEDGER_ROWS = """
() => new Promise((resolve) => {
  const open = indexedDB.open('cronstable-ledger', 1);
  open.onsuccess = () => {
    const db = open.result;
    if (!db.objectStoreNames.contains('runs')) { resolve([]); return; }
    const all = db.transaction('runs').objectStore('runs').getAll();
    all.onsuccess = () => { db.close(); resolve(all.result); };
  };
  open.onerror = () => resolve(null);
})
"""


def test_run_ledger_records_analyzes_exports_and_purges(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            _click(page, "#settingsBtn")
            assert not page.is_visible("#ledgerStatRow")
            page.evaluate("document.getElementById('setLedger').click()")
            page.wait_for_function(
                "document.getElementById('ledgerStat').textContent"
                ".startsWith('1 runs · 1 jobs')"
            )
            # the ledger samples each job's latest run once per poll, so
            # let every run be seen before the next one replaces it
            expect = 1
            for name in ("alpha-ok", "beta-fail", "alpha-ok", "beta-fail"):
                daemon.run_and_wait(name)
                expect += 1
                page.wait_for_function(
                    "(n) => document.getElementById('ledgerStat')"
                    ".textContent.startsWith(n + ' runs · ')",
                    arg=expect,
                )
            assert page.text_content("#ledgerStat").startswith(
                "5 runs · 2 jobs"
            )
            rows = page.evaluate(_LEDGER_ROWS)
            assert len(rows) == 5
            fails = [r for r in rows if r["job"] == "beta-fail"]
            assert [r["o"] for r in fails] == ["failure", "failure"]
            assert all(r["x"] == 3 for r in fails)
            assert all(
                r["id"] == r["job"] + "|" + r["iso"] and r["d"] > 0
                for r in rows
            )
            # the history pane adds the browser-side totals
            page.keyboard.press("Escape")
            _click(page, '#rows [data-logs="beta-fail"]')
            _click(page, '#dTabs button[data-tab="history"]')
            page.wait_for_selector("#historyPane .tlnote")
            assert "2 runs · 0 ok / 2 fail" in page.inner_text(
                "#historyPane .tlnote"
            )
            page.keyboard.press("Escape")

            # Reloading restores IndexedDB data without duplicate records.
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            _click(page, "#settingsBtn")
            page.wait_for_function(
                "document.getElementById('ledgerStat').textContent"
                ".startsWith('5 runs · 2 jobs')"
            )
            assert len(page.evaluate(_LEDGER_ROWS)) == 5

            with page.expect_download() as info:
                page.click("#ledgerExport")
            e2e.wait_toast(page, "exported 5 runs")
            assert info.value.suggested_filename == (
                "cronstable-run-ledger.json"
            )
            target = tmp_path / "ledger.json"
            info.value.save_as(str(target))
            exported = json.loads(target.read_text())
            assert exported["schema"] == "cronstable-ledger/1"
            assert sorted(exported["runs"]) == ["alpha-ok", "beta-fail"]
            assert len(exported["runs"]["alpha-ok"]) == 3
            assert exported["runs"]["beta-fail"][0] == {
                "finished_at": fails[0]["iso"],
                "outcome": "failure",
                "exit_code": 3,
                "duration": fails[0]["d"],
                "fail_reason": "command exited with code 3",
            }

            page.click("#ledgerPurge")
            e2e.wait_toast(page, "ledger purged")
            page.wait_for_function(
                "document.getElementById('ledgerStat').textContent"
                ".startsWith('0 runs · 0 jobs')"
            )
            e2e.wait_until(lambda: page.evaluate(_LEDGER_ROWS) == [], page)


def test_ledger_flags_a_slow_run_against_its_own_history(browser, tmp_path):
    """Flag a slow run using durations recorded by the browser.

    After ten short successful runs, a much longer run adds the slow indicator
    to the job row.
    """
    seed = """
    (() => {
      const open = indexedDB.open('cronstable-ledger', 1);
      open.onupgradeneeded = () => {
        const os = open.result.createObjectStore('runs', { keyPath: 'id' });
        os.createIndex('job', 'job', { unique: false });
      };
      open.onsuccess = () => {
        const os = open.result.transaction('runs', 'readwrite')
          .objectStore('runs');
        for (let i = 0; i < 10; i++) {
          const iso = new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString();
          os.put({ id: 'alpha-ok|' + iso, job: 'alpha-ok', iso,
            fin: Date.parse(iso), o: 'success', x: 0,
            d: 0.001 + i * 0.00001, r: null });
        }
      };
    })();
    """
    slow = e2e.job(
        "alpha-ok", e2e.py_cmd("import time; time.sleep(0.05)")
    )
    with e2e.Daemon(tmp_path, jobs=[slow]) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            init_scripts=[seed],
        ) as page:
            # Commit the seed before the app reads it; IndexedDB opens and
            # transactions finish asynchronously during page startup.
            e2e.wait_until(
                lambda: len(page.evaluate(_LEDGER_ROWS) or []) == 10, page
            )
            _click(page, "#settingsBtn")
            page.evaluate("document.getElementById('setLedger').click()")
            page.wait_for_function(
                "document.getElementById('ledgerStat').textContent"
                ".startsWith('10 runs · 1 jobs')"
            )
            page.keyboard.press("Escape")
            assert not page.query_selector("#rows .slow-chip")
            daemon.run_and_wait("alpha-ok", outcome="success")
            page.wait_for_selector('#rows tr[data-job="alpha-ok"] .slow-chip')
            assert page.inner_text(
                '#rows tr[data-job="alpha-ok"] .slow-chip'
            ).startswith("◱ slow ×")
            # switching the ledger off removes the marker
            _click(page, "#settingsBtn")
            page.evaluate("document.getElementById('setLedger').click()")
            page.wait_for_function(
                "!document.querySelector('#rows .slow-chip')"
            )


# --------------------------------------------------------------------------
# cron sandbox
# --------------------------------------------------------------------------


def _sandbox(page, expr, frame=None):
    if frame:
        page.select_option("#sbxFrame", frame)
    page.fill("#sbxInput", expr)


def test_sandbox_describes_previews_and_finds_users(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_scheduled_jobs()) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            before_goto=_pin,
            prefs={"pollMs": 0},
            timezone_id="UTC",
        ) as page:
            _palette(page, "Schedule preview")
            page.wait_for_selector("#sandboxWrap.open")
            assert "Enter a cron expression" in page.inner_text("#sbxBody")
            frames = page.evaluate(
                "[...document.querySelectorAll('#sbxFrame option')]"
                ".map((o) => o.value)"
            )
            assert frames == ["UTC", "LOCAL", "America/New_York"]

            _sandbox(page, "*/15 9-17 * * 1-5")
            page.wait_for_selector("#sbxBody .nextruns li")
            fields = page.evaluate(
                "[...document.querySelectorAll('#sbxBody .sbx-fld')].map("
                "(f) => [f.querySelector('.fk').textContent, "
                "f.querySelector('.fv').textContent])"
            )
            assert fields == [
                ["min", "*/15"],
                ["hour", "9-17"],
                ["day-of-month", "*"],
                ["month", "*"],
                ["day-of-week", "1-5"],
            ]
            runs = page.evaluate(
                "[...document.querySelectorAll('#sbxBody .nextruns .when')]"
                ".map((w) => w.textContent)"
            )
            # pinned to a Saturday noon: the first fire is Monday 09:00
            assert runs[0] == "2026-03-09 09:00"
            assert runs[1] == "2026-03-09 09:15"
            assert len(runs) == 12

            # six and seven fields label the year and the second columns
            _sandbox(page, "0 0 1 1 * 2030")
            page.wait_for_function(
                "document.querySelectorAll('#sbxBody .sbx-fld').length === 6"
            )
            assert (
                page.inner_text("#sbxBody .sbx-fld:last-child .fk").lower()
                == "year"
            )
            _sandbox(page, "30 0 0 1 1 * 2030")
            page.wait_for_function(
                "document.querySelectorAll('#sbxBody .sbx-fld').length === 7"
            )
            assert (
                page.inner_text("#sbxBody .sbx-fld:first-child .fk").lower()
                == "second"
            )
            assert page.inner_text("#sbxBody .nextruns .when") == (
                "2030-01-01 00:00:30"
            )

            for expr, text in (
                ("@reboot", "Runs once, when cronstable starts"),
                ("not a cron", "Enter a valid cron expression"),
                ("* * *", "Enter a valid cron expression"),
                ("61 * * * *", "Enter a valid cron expression"),
                ("H * * * *", "chooses a consistent time"),
                ("H(10-5) * * * *", "Enter a valid cron expression"),
                ("0 0 31 2 *", "No future run times were found"),
            ):
                _sandbox(page, expr)
                page.wait_for_function(
                    "(t) => document.getElementById('sbxBody').textContent"
                    ".includes(t)",
                    arg=text,
                )
            # Expanding a macro shows its fields and the jobs that use it.
            _sandbox(page, "@hourly")
            page.wait_for_selector("#sbxBody .sbx-uses [data-job]")
            assert page.evaluate(
                "[...document.querySelectorAll('#sbxBody .sbx-uses "
                "[data-job]')].map((c) => c.textContent)"
            ) == ["hourly-a", "hourly-b"]
            # Enter remembers the expression; a recent chip restores it
            page.press("#sbxInput", "Enter")
            _sandbox(page, "")
            page.wait_for_selector("#sbxBody .sbx-recent .chip")
            page.click("#sbxBody .sbx-recent .chip")
            assert page.input_value("#sbxInput") == "@hourly"
            page.click("#sbxBody .sbx-uses [data-job='hourly-b']")
            page.wait_for_selector('.pane.active[data-pane="schedule"]')
            assert page.inner_text("#dName") == "hourly-b"
            assert not page.evaluate(
                "document.getElementById('sandboxWrap').classList"
                ".contains('open')"
            )


def test_schedule_tab_warnings_including_the_dst_advisory(browser, tmp_path):
    jobs = _scheduled_jobs() + [
        e2e.job("minutely-slow", "sleep 90", schedule="* * * * *"),
    ]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:

        def rewrite(body):
            for j in body:
                if j["name"] == "minutely-slow":
                    # a recorded run longer than the gap between fires
                    j["last_run"] = {
                        "outcome": "success",
                        "exit_code": 0,
                        "started_at": "2026-03-07T11:00:00+00:00",
                        "finished_at": "2026-03-07T11:01:30+00:00",
                        "duration": 90,
                        "fail_reason": None,
                    }
            return body

        def before(page):
            _pin(page)
            page.faults.rewrite(r"/jobs", rewrite)

        with e2e.open_page(
            browser,
            daemon.url,
            before_goto=before,
            prefs={"pollMs": 0},
            timezone_id="UTC",
        ) as page:

            def warnings(name):
                page.evaluate("location.hash = ''")
                _click(page, '#rows [data-logs="{}"]'.format(name))
                _click(page, '#dTabs button[data-tab="schedule"]')
                page.wait_for_function(
                    "(n) => document.getElementById('dName').textContent "
                    "=== n && !!document.querySelector('#schedulePane "
                    ".nextruns, #schedulePane .sched-desc')",
                    arg=name,
                )
                found = page.evaluate(
                    "[...document.querySelectorAll('#schedulePane "
                    ".sched-warn .sw')].map((w) => [w.classList.contains("
                    "'crit') ? 'crit' : 'warn', w.textContent])"
                )
                page.keyboard.press("Escape")
                page.wait_for_selector('#drawer[aria-hidden="true"]')
                return found

            slow = warnings("minutely-slow")
            assert slow[0][0] == "crit"
            assert "Runs may overlap" in slow[0][1]
            assert "every 1m 0s" in slow[0][1]
            assert "1m 30s" in slow[0][1]

            # the New York job at 02:30 local crosses the spring-forward
            nightly = warnings("nightly-ny")
            dst = [w for w in nightly if "Daylight saving" in w[1]]
            assert len(dst) == 1
            assert "America/New_York shifts clocks forward" in dst[0][1]
            assert "2026-03-08" in dst[0][1]

            # the two hourlies share their next fire; */5 fires sooner, so
            # its next fire is another minute and it is not counted
            hourly = warnings("hourly-a")
            together = [w for w in hourly if "scheduled together" in w[1]]
            assert len(together) == 1
            assert "13:00" in together[0][1]
            assert "1 other job (hourly-b)" in together[0][1]
            # UTC never shifts: no advisory there
            assert not [w for w in hourly if "Daylight" in w[1]]


# --------------------------------------------------------------------------
# pairing QR
# --------------------------------------------------------------------------

# The module matrix the page's own QR generator produces for ``text``, as
# the same path data renderPairQr builds.
_QR_PATH_FOR = """
(text) => {
  qrcode.stringToBytes = qrcode.stringToBytesFuncs["UTF-8"];
  const qr = qrcode(0, "M");
  qr.addData(text, "Byte");
  qr.make();
  const n = qr.getModuleCount(), quiet = 4;
  let d = "";
  for (let r = 0; r < n; r++)
    for (let c = 0; c < n; c++)
      if (qr.isDark(r, c)) d += "M" + (c + quiet) + " " + (r + quiet) +
        "h1v1h-1z";
  return d;
}
"""


def _b64url(text):
    raw = base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")
    return raw.rstrip("=")


def _unb64url(fragment):
    padded = fragment + "=" * (-len(fragment) % 4)
    return base64.urlsafe_b64decode(padded).decode("utf-8")


def _rendered_qr(page):
    return page.get_attribute("#pairQr svg path", "d")


def test_pair_qr_encodes_the_link_for_a_non_ascii_node(browser, tmp_path):
    node = "büro-節点-🚀/north+east?"
    with e2e.Daemon(tmp_path, auth="scoped") as daemon:

        def routes(page):
            page.faults.json(r"/cluster", _gossip(node_name=node))

        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.VIEW_TOKEN,
            before_goto=routes,
        ) as page:
            page.wait_for_function(
                "(n) => document.getElementById('clusterSummary')"
                ".textContent.startsWith(n)",
                arg=node,
            )
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            page.wait_for_selector("#pairQr svg path")
            payload = page.text_content("#pairPayload")
            assert json.loads(payload) == {
                "v": 1,
                "name": node,
                "url": daemon.url.rstrip("/"),
                "token": e2e.VIEW_TOKEN,
            }
            # the link: base + "#" + base64url(JSON), no padding, URL-safe
            fragment = _b64url(payload)
            assert "=" not in fragment
            assert "+" not in fragment and "/" not in fragment
            assert _unb64url(fragment) == payload
            link = "https://relay.cronstable.com/pair#" + fragment
            # the rendered code is exactly the code for that link
            assert _rendered_qr(page) == page.evaluate(_QR_PATH_FOR, link)
            assert _rendered_qr(page) != page.evaluate(
                _QR_PATH_FOR, link + "x"
            )
            svg = page.evaluate(
                "(() => { const s = document.querySelector('#pairQr svg');"
                " return [s.getAttribute('role'), s.getAttribute("
                "'aria-label'), +s.getAttribute('width')]; })()"
            )
            assert svg[:2] == ["img", "pairing QR code"]
            assert svg[2] >= 220
            # A scoped token produces no broad-permission warning or hint.
            assert not page.is_visible("#pairWarn")
            assert not page.is_visible("#pairHint")


def test_pair_qr_follows_the_daemons_link_base(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        base = "https://relay.example.test/pair"
        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.FULL_TOKEN,
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            # the first /whoami named the default base; the panel's own
            # probe names another one, and the code is redrawn for it
            page.faults.rewrite(
                r"/whoami", lambda body: dict(body, pairLinkBase=base)
            )
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            page.wait_for_selector("#pairQr svg path")
            payload = page.text_content("#pairPayload")
            expected = page.evaluate(
                _QR_PATH_FOR, base + "#" + _b64url(payload)
            )
            page.wait_for_function(
                "(d) => document.querySelector('#pairQr svg path')"
                ".getAttribute('d') === d",
                arg=expected,
            )
            # the all-scopes token draws the warning
            page.wait_for_function(
                "document.getElementById('pairWarn').style.display === ''"
            )
            page.click("#pairCopy")
            e2e.wait_toast(page, "copied pairing payload")
            assert page.evaluate("navigator.clipboard.readText()") == payload
            # "set a token" opens the token dialog.
            page.click("#pairClose")
            page.wait_for_function(
                "!document.getElementById('pairWrap').classList"
                ".contains('open')"
            )


def test_pair_panel_ignores_a_stale_whoami_answer(browser, tmp_path):
    """Ignore pairing responses from a previously closed panel."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            parked = page.faults.hang(r"/whoami")
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            page.wait_for_selector("#pairQr svg path")
            e2e.wait_until(lambda: len(parked) == 1, page)
            first = _rendered_qr(page)
            assert page.is_visible("#pairHint")
            assert "No token is saved" in page.inner_text("#pairHint")
            page.keyboard.press("Escape")
            page.wait_for_function(
                "!document.getElementById('pairWrap').classList"
                ".contains('open')"
            )
            # The delayed response reports all scopes and a different base URL.
            parked[0].fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "authenticated": True,
                        "allScopes": True,
                        "scopes": [],
                        "label": "late",
                        "pairLinkBase": "https://late.example.test/pair",
                    }
                ),
            )
            page.faults.clear(r"/whoami")
            _palette(page, "Pair a device")
            page.wait_for_selector("#pairWrap.open")
            page.wait_for_function(
                "performance.getEntriesByName(location.origin + '/whoami')"
                ".length >= 2"
            )
            assert _rendered_qr(page) == first
            assert not page.is_visible("#pairWarn")
            # the hint's button leads to the token modal
            page.click("#pairToken")
            page.wait_for_selector("#modalWrap.open")
            assert not page.evaluate(
                "document.getElementById('pairWrap').classList"
                ".contains('open')"
            )


# --------------------------------------------------------------------------
# header chips
# --------------------------------------------------------------------------


def test_version_and_job_set_chips_copy_their_values(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        status, version = daemon.api("GET", "/version")
        assert status == 200
        with e2e.open_page(
            browser,
            daemon.url,
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            page.wait_for_function(
                "document.getElementById('ver').textContent.startsWith('v')"
            )
            shown = page.text_content("#ver")
            assert shown == "v" + str(version).strip()
            page.click("#ver")
            e2e.wait_toast(page, "copied version")
            assert page.evaluate("navigator.clipboard.readText()") == shown
            full = page.evaluate(
                "document.getElementById('jobset').dataset.full"
            )
            status, body = daemon.api("GET", "/job-set-id")
            assert full == str(body).strip()
            # the chip shows a short prefix; the click copies all of it
            assert len(page.text_content("#jobset")) == 13
            page.click("#jobset")
            e2e.wait_toast(page, "copied job-set ID")
            assert page.evaluate("navigator.clipboard.readText()") == full
            # the node meter opens the node history card
            page.wait_for_selector("#nodeMeter .m")
            page.click("#nodeMeter")
            page.wait_for_function(
                "getComputedStyle(document.getElementById('nodeCard'))"
                ".display !== 'none'"
            )
