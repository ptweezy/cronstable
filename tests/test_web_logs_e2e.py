"""Test streamed logs, search, buffering, following, and multiple log tails.

The dashboard uses the daemon in ``tests/_web_e2e.py``. Tests use two types
of streams:

* Running jobs produce output that appears incrementally in the drawer.
  The stream ends with the run and reconnects when the next run starts.
  The combined log view follows multiple jobs across runs.
* ``Faults.sse`` supplies controlled byte sequences to test ``readSSE``.
  Cases include split frames, comments, unknown events, malformed data,
  error responses, incomplete frames, and end events with or without a
  reason. Larger inputs test the 5,000-line buffer, regular expression
  scan limit, and automatic scrolling.
"""

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


LOGS = r"^/jobs/[^/]+/logs$"
ESC = "\x1b"


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _open_logs(page, name="alpha-ok"):
    _click(page, '#rows [data-logs="{}"]'.format(name))
    page.wait_for_selector('#drawer[aria-hidden="false"]')


def _lines(page, term="term"):
    """``[class, text]`` per rendered line, gutter number excluded."""
    return page.evaluate(
        """(id) => [...document.querySelectorAll('#' + id + ' .ln')]
          .map((ln) => {
            const copy = ln.cloneNode(true);
            for (const g of copy.querySelectorAll('.gut, .ts, .tj'))
              g.remove();
            return [ln.className.replace('ln', '').trim(),
                    copy.textContent];
          })""",
        term,
    )


def _wait_line_count(page, n, term="term"):
    page.wait_for_function(
        "([id, n]) => document.querySelectorAll('#' + id + ' .ln').length "
        "=== n",
        arg=[term, n],
    )


def _count_label(page, text, label="logCount"):
    page.wait_for_function(
        "([id, t]) => document.getElementById(id).textContent === t",
        arg=[label, text],
    )


# --------------------------------------------------------------------------
# real streaming
# --------------------------------------------------------------------------


def test_real_run_streams_into_the_drawer_and_ends(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _open_logs(page, "gamma-slow")
            # never ran: the daemon ends the tail with nothing to replay
            page.wait_for_function(
                "document.getElementById('term').textContent"
                ".includes('(no output captured for the latest run)')"
            )
            page.click("#dRun")
            page.wait_for_function(
                "document.querySelectorAll('#term .ln.stdout').length >= 5"
            )
            first = page.evaluate(
                "document.querySelectorAll('#term .ln.stdout').length"
            )
            # it is live: more lines keep arriving while the run goes on
            page.wait_for_function(
                "(n) => document.querySelectorAll('#term .ln.stdout')"
                ".length > n + 10",
                arg=first,
            )
            texts = [t for cls, t in _lines(page) if cls == "stdout"]
            assert texts[:5] == ["0", "1", "2", "3", "4"]
            assert texts == [str(i) for i in range(len(texts))]
            # gutter numbers count lines from the start of this view
            assert page.evaluate(
                "[...document.querySelectorAll('#term .ln .gut')]"
                ".slice(0, 3).map((g) => g.textContent)"
            ) == ["1", "2", "3"]
            page.click("#dCancel")
            e2e.wait_toast(page, "cancelled gamma-slow")
            # the daemon ends the stream with the run; the line count holds
            e2e.wait_until(lambda: not daemon.jobs()["gamma-slow"]["running"])
            page.wait_for_function(
                "document.getElementById('dCancel').disabled"
            )
            settled = page.evaluate(
                "document.querySelectorAll('#term .ln.stdout').length"
            )
            assert settled >= first
            opened = len(page.faults.sent("GET", "/jobs/gamma-slow/logs"))

            # a new run started elsewhere: the next poll reattaches, and
            # the view restarts from that run's first line
            daemon.api("POST", "/jobs/gamma-slow/start")
            e2e.wait_until(
                lambda: (
                    len(page.faults.sent("GET", "/jobs/gamma-slow/logs"))
                    == opened + 1
                ),
                page,
            )
            page.wait_for_function(
                "(n) => { const l = document.querySelectorAll("
                "'#term .ln.stdout'); return l.length > 3 && l.length < n; }",
                arg=settled,
            )
            texts = [t for cls, t in _lines(page) if cls == "stdout"]
            assert texts[:3] == ["0", "1", "2"]
            # closing the drawer drops the connection
            page.keyboard.press("Escape")
            page.wait_for_selector('#drawer[aria-hidden="true"]')
            daemon.api("POST", "/jobs/gamma-slow/cancel")


def test_stderr_and_stdout_are_classed_and_tabs_stop_the_tail(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            _open_logs(page, "beta-fail")
            page.wait_for_selector("#term .ln.stderr")
            assert _lines(page) == [["stderr", "beta-err"]]
            # leaving the logs tab aborts the stream, returning reopens it
            _click(page, '#dTabs button[data-tab="history"]')
            page.wait_for_selector("#historyPane .runtable")
            assert "command exited with code 3" in page.inner_text(
                "#historyPane .reason"
            )
            _click(page, '#dTabs button[data-tab="logs"]')
            page.wait_for_function(
                "(n) => performance.getEntriesByName(location.origin + "
                "'/jobs/beta-fail/logs').length >= n",
                arg=2,
            )
            _wait_line_count(page, 1)


def test_job_without_capture_opens_no_stream(browser, tmp_path):
    jobs = [
        e2e.job("silent", "echo hi", captureStdout=False, captureStderr=False)
    ]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_logs(page, "silent")
            page.wait_for_selector("#term .ln.sys")
            assert "does not capture stdout or stderr" in page.inner_text(
                "#term"
            )
            assert "capture: stdout off, stderr off" in page.inner_text(
                "#dMeta"
            )
            assert not page.faults.sent("GET", "/jobs/silent/logs")


# --------------------------------------------------------------------------
# readSSE
# --------------------------------------------------------------------------


def test_frames_split_across_reads_and_noise_frames(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            whole = e2e.sse_line("whole frame")
            split = e2e.sse_line("split across three reads", "stderr")
            multibyte = e2e.sse_line("snowman ☃ and emoji 🚀")
            chunks = [
                whole,
                # cut inside the JSON, then inside the blank-line terminator
                split[:25],
                split[25:-1],
                split[-1:] + ": ping\n\n",
                # frames the reader skips: a comment, an unknown event, a
                # default-event message, a line frame with broken JSON, a
                # line frame with no data
                ": another comment\n\n",
                "event: other\ndata: {}\n\n",
                'data: {"stream":"stdout","line":"no event name"}\n\n',
                "event: line\ndata: {not json\n\n",
                "event: line\n\n",
                # two frames in one read
                e2e.sse_line("first of two") + e2e.sse_line("second of two"),
                multibyte,
            ]
            page.faults.sse(LOGS, chunks)
            _open_logs(page)
            _wait_line_count(page, 5)
            assert _lines(page) == [
                ["stdout", "whole frame"],
                ["stderr", "split across three reads"],
                ["stdout", "first of two"],
                ["stdout", "second of two"],
                ["stdout", "snowman ☃ and emoji 🚀"],
            ]
            # a multi-byte character cut between two reads still decodes
            raw = e2e.sse_line("cut ☃ here").encode()
            cut = raw.index("☃".encode()) + 1
            page.faults.sse_push(
                "/jobs/alpha-ok/logs", list(raw[:cut]), list(raw[cut:])
            )
            _wait_line_count(page, 6)
            assert _lines(page)[-1] == ["stdout", "cut ☃ here"]


@pytest.mark.parametrize(
    "status,text",
    [
        (500, "Could not open log stream (HTTP 500)."),
        (404, "Could not open log stream (HTTP 404)."),
        (403, "Could not open log stream (HTTP 403)."),
    ],
)
def test_non_ok_answer_is_reported_in_the_pane(
    browser, tmp_path, status, text
):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(LOGS, status=status)
            _open_logs(page)
            page.wait_for_selector("#term .ln.sys")
            assert _lines(page) == [["sys", text]]


def test_401_on_the_tail_opens_the_token_modal(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(LOGS, status=401)
            _open_logs(page)
            page.wait_for_selector("#modalWrap.open")
            # The dialog handles reauthentication without a log error line.
            assert _lines(page) == []


def test_stream_that_dies_mid_frame(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            half = e2e.sse_line("never completed")[:20]
            page.faults.sse(
                LOGS, [e2e.sse_line("before the drop"), half], fail=True
            )
            _open_logs(page)
            page.wait_for_selector("#term .ln.sys")
            lines = _lines(page)
            assert lines[0] == ["stdout", "before the drop"]
            assert lines[1][0] == "sys"
            assert lines[1][1].startswith("Log stream ended: ")
            assert len(lines) == 2


def test_stream_truncated_without_an_end_frame(browser, tmp_path):
    """The server closes mid-frame: the complete lines stay, the partial
    frame is dropped, and no end message is invented."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(
                LOGS,
                [e2e.sse_line("complete"), e2e.sse_line("partial")[:-5]],
                close=True,
            )
            _open_logs(page)
            _wait_line_count(page, 1)
            # the reader has finished: the refresh key reattaches nothing
            page.wait_for_function("(window.__sseOpens || []).length === 1")
            assert _lines(page) == [["stdout", "complete"]]


def test_end_frames(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            # end with no line: the drawer says nothing was captured
            page.faults.sse(
                LOGS,
                [e2e.sse_frame("end", {"reason": "no-output"})],
                close=True,
            )
            _open_logs(page)
            page.wait_for_selector("#term .ln.sys")
            assert _lines(page) == [
                ["sys", "(no output captured for the latest run)"]
            ]
            page.keyboard.press("Escape")
            # end after lines: the drawer adds nothing
            page.faults.sse(
                LOGS,
                [e2e.sse_line("one"), "event: end\ndata: {}\n\n"],
                close=True,
            )
            _open_logs(page, "beta-fail")
            _wait_line_count(page, 1)
            # a garbled end payload still ends cleanly
            page.keyboard.press("Escape")
            page.faults.sse(LOGS, ["event: end\ndata: {{{\n\n"], close=True)
            _open_logs(page, "epsilon-quiet")
            page.wait_for_selector("#term .ln.sys")
            assert len(_lines(page)) == 1


# --------------------------------------------------------------------------
# search, toggles, download
# --------------------------------------------------------------------------

_SEARCH_LINES = [
    "alpha needle one",
    ESC + "[31mred needle two" + ESC + "[0m tail",
    "no match here",
    "NEEDLE three and needle four",
    ESC + "[1;32mbold green" + ESC + "[0m plain",
]


def _search_page(page):
    page.faults.sse(LOGS, [e2e.sse_line(t) for t in _SEARCH_LINES])
    _open_logs(page)
    _wait_line_count(page, 5)


def _current_mark(page):
    return page.evaluate(
        "(() => { const marks = [...document.querySelectorAll("
        "'#term mark')]; return marks.findIndex((m) => "
        "m.classList.contains('cur')); })()"
    )


def test_enter_and_shift_enter_walk_the_matches(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            _search_page(page)
            page.fill("#logSearch", "needle")
            _count_label(page, "3 matches")
            page.wait_for_function(
                "document.querySelectorAll('#term mark').length === 4"
            )
            assert _current_mark(page) == -1
            seen = []
            for _ in range(5):
                page.press("#logSearch", "Enter")
                seen.append(_current_mark(page))
            # four marks: forward, wrapping to the first
            assert seen == [0, 1, 2, 3, 0]
            page.press("#logSearch", "Shift+Enter")
            assert _current_mark(page) == 3
            page.press("#logSearch", "Shift+Enter")
            assert _current_mark(page) == 2
            assert (
                page.evaluate(
                    "document.querySelectorAll('#term mark.cur').length"
                )
                == 1
            )
            # the case of the matched text is preserved in the mark
            assert page.evaluate(
                "[...document.querySelectorAll('#term mark')]"
                ".map((m) => m.textContent)"
            ) == ["needle", "needle", "NEEDLE", "needle"]
            # a new query resets the cursor
            page.fill("#logSearch", "plain")
            _count_label(page, "1 match")
            assert _current_mark(page) == -1
            # Enter with no matches does nothing
            page.fill("#logSearch", "absent")
            _count_label(page, "0 matches")
            page.press("#logSearch", "Enter")
            assert _current_mark(page) == -1
            # typing in the search box never fires list shortcuts
            page.fill("#logSearch", "")
            page.type("#logSearch", "wtig?")
            assert not page.evaluate("document.body.classList.contains('tv')")
            assert page.evaluate(
                "!document.getElementById('helpWrap').classList"
                ".contains('open')"
            )


def test_matches_only_ansi_timestamps_and_wrap_toggles(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            _search_page(page)
            # ansi on: styled spans, escape bytes gone from the text
            styles = page.evaluate(
                "[...document.querySelectorAll('#term .ln span[style]')]"
                ".map((s) => [s.textContent, s.getAttribute('style')])"
            )
            assert styles == [
                ["red needle two", "color:var(--ansi-31);"],
                ["bold green", "color:var(--ansi-32);font-weight:700;"],
            ]
            assert ESC not in page.inner_text("#term")
            page.uncheck("#optAnsi")
            page.wait_for_function(
                "!document.querySelector('#term .ln span[style]')"
            )
            assert [t for _, t in _lines(page)][1] == "red needle two tail"
            assert (
                page.evaluate("localStorage.getItem('cronstable.ansi')")
                == "false"
            )
            page.check("#optAnsi")

            page.fill("#logSearch", "needle")
            _count_label(page, "3 matches")
            page.check("#optOnly")
            page.wait_for_function(
                "[...document.querySelectorAll('#term .ln')]"
                ".filter((l) => l.style.display !== 'none').length === 3"
            )
            # a line arriving under the filter is hidden or shown by it
            page.faults.sse_push(
                "/jobs/alpha-ok/logs",
                e2e.sse_line("late needle"),
                e2e.sse_line("late miss"),
            )
            _count_label(page, "4 matches")
            visible = page.evaluate(
                "[...document.querySelectorAll('#term .ln')]"
                ".filter((l) => l.style.display !== 'none').length"
            )
            assert visible == 4
            page.uncheck("#optOnly")
            page.fill("#logSearch", "")
            _count_label(page, "")

            assert not page.query_selector("#term .ts")
            page.check("#optTs")
            page.wait_for_function(
                "document.querySelectorAll('#term .ts').length === 7"
            )
            assert page.evaluate(
                "[...document.querySelectorAll('#term .ts')].every((t) => "
                "/^\\d\\d:\\d\\d:\\d\\d$/.test(t.textContent))"
            )
            assert (
                page.evaluate("localStorage.getItem('cronstable.ts')")
                == "true"
            )
            page.uncheck("#optWrap")
            assert page.evaluate(
                "document.getElementById('term').classList.contains('nowrap')"
            )
            page.check("#optWrap")
            assert not page.evaluate(
                "document.getElementById('term').classList.contains('nowrap')"
            )


def test_regex_scan_cap_and_zero_width_matches(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            long_line = "x" * 5500 + "needle" + "y" * 100
            page.faults.sse(
                LOGS,
                [
                    e2e.sse_line("short needle"),
                    e2e.sse_line(long_line),
                    e2e.sse_line("abc"),
                ],
            )
            _open_logs(page)
            _wait_line_count(page, 3)
            # plain text scans the whole line
            page.fill("#logSearch", "needle")
            _count_label(page, "2 matches")
            # a regex stops at the 5000-character cap: the far needle is
            # neither counted nor marked, and the tail renders verbatim
            page.check("#optRegex")
            _count_label(page, "1 match")
            assert (
                page.evaluate("document.querySelectorAll('#term mark').length")
                == 1
            )
            assert [t for _, t in _lines(page)][1] == long_line
            # inside the cap a regex marks every hit
            page.fill("#logSearch", "x{10}")
            page.wait_for_function(
                "document.querySelectorAll('#term mark').length === 500"
            )
            # zero-width patterns terminate and drop no characters
            for pattern in ("x*", "^", "(?:)", "\\b", "(?=a)"):
                page.fill("#logSearch", pattern)
                page.wait_for_function(
                    "(p) => document.getElementById('logCount').textContent"
                    ".endsWith('matches') || document.getElementById("
                    "'logCount').textContent.endsWith('match')",
                    arg=pattern,
                )
                assert [t for _, t in _lines(page)] == [
                    "short needle",
                    long_line,
                    "abc",
                ], pattern
            # a pattern that does not compile
            page.fill("#logSearch", "(")
            _count_label(page, "bad regex")
            assert (
                page.evaluate("document.querySelectorAll('#term mark').length")
                == 0
            )
            # a catastrophic pattern over a capped line returns promptly
            page.fill("#logSearch", "(x+)+$")
            page.wait_for_function(
                "document.getElementById('logCount').textContent !== "
                "'bad regex'"
            )


def test_ring_keeps_the_newest_5000_lines(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(LOGS, [])
            _open_logs(page)
            page.wait_for_function("(window.__sseOpens || []).length === 1")
            page.fill("#logSearch", "needle")

            def push(start, stop):
                frames = "".join(
                    e2e.sse_line(
                        "line {}{}".format(
                            i, " needle" if i % 100 == 0 else ""
                        )
                    )
                    for i in range(start, stop)
                )
                page.faults.sse_push("/jobs/alpha-ok/logs", frames)

            push(1, 4001)
            _wait_line_count(page, 4000)
            _count_label(page, "40 matches")
            push(4001, 5251)
            page.wait_for_function(
                "document.querySelector('#term .ln:last-child').textContent"
                ".endsWith('line 5250')"
            )
            _wait_line_count(page, 5000)
            texts = [t for _, t in _lines(page)]
            assert texts[0] == "line 251"
            assert texts[-1] == "line 5250"
            # gutter numbers stay absolute across the trim
            assert page.evaluate(
                "[document.querySelector('#term .ln .gut').textContent, "
                "document.querySelector('#term .ln:last-child .gut')"
                ".textContent]"
            ) == ["251", "5250"]
            # the count tracks what the ring still holds: 300..5200
            _count_label(page, "50 matches")
            # a re-render of the ring agrees with the incremental count
            page.check("#optOnly")
            _count_label(page, "50 matches")
            assert (
                page.evaluate(
                    "[...document.querySelectorAll('#term .ln')]"
                    ".filter((l) => l.style.display !== 'none').length"
                )
                == 50
            )
            # clear empties the ring and restarts the numbering
            page.click("#dClear")
            _wait_line_count(page, 0)
            _count_label(page, "0 matches")
            push(1, 3)
            _wait_line_count(page, 2)
            assert page.inner_text("#term .ln .gut") == "1"


def test_follow_pins_to_the_bottom_until_the_reader_scrolls(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(LOGS, [])
            _open_logs(page)
            page.wait_for_function("(window.__sseOpens || []).length === 1")

            def push(start, n):
                page.faults.sse_push(
                    "/jobs/alpha-ok/logs",
                    "".join(
                        e2e.sse_line("row {}".format(i))
                        for i in range(start, start + n)
                    ),
                )

            at_bottom = (
                "(() => { const t = document.getElementById('term'); "
                "return t.scrollHeight - t.scrollTop - t.clientHeight < 2; "
                "})()"
            )
            push(0, 300)
            _wait_line_count(page, 300)
            page.wait_for_function(at_bottom)
            push(300, 50)
            _wait_line_count(page, 350)
            page.wait_for_function(at_bottom)
            # Wait for the automatic scroll from the last append to finish.
            # Otherwise, it would undo the manual scroll in this test.
            page.evaluate(
                "new Promise((r) => requestAnimationFrame(() => "
                "requestAnimationFrame(r)))"
            )
            # the reader scrolls up: new lines leave the position alone
            page.evaluate("document.getElementById('term').scrollTop = 100")
            page.wait_for_function(
                "document.getElementById('term').scrollTop === 100"
            )
            # The log view updates its scroll position on the next frame's
            # scroll event.
            page.evaluate(
                "new Promise((r) => requestAnimationFrame(() => "
                "requestAnimationFrame(r)))"
            )
            push(350, 50)
            _wait_line_count(page, 400)
            page.evaluate(
                "new Promise((r) => requestAnimationFrame(() => "
                "requestAnimationFrame(r)))"
            )
            assert (
                page.evaluate("document.getElementById('term').scrollTop")
                == 100
            )
            # back at the bottom, the pin returns
            page.evaluate(
                "(() => { const t = document.getElementById('term'); "
                "t.scrollTop = t.scrollHeight; })()"
            )
            page.wait_for_function(at_bottom)
            page.evaluate(
                "new Promise((r) => requestAnimationFrame(() => "
                "requestAnimationFrame(r)))"
            )
            push(400, 50)
            _wait_line_count(page, 450)
            page.wait_for_function(at_bottom)
            # follow off: nothing pins, even from the bottom
            page.uncheck("#optFollow")
            top = page.evaluate("document.getElementById('term').scrollTop")
            push(450, 50)
            _wait_line_count(page, 500)
            page.evaluate(
                "new Promise((r) => requestAnimationFrame(() => "
                "requestAnimationFrame(r)))"
            )
            assert (
                page.evaluate("document.getElementById('term').scrollTop")
                == top
            )
            assert (
                page.evaluate("localStorage.getItem('cronstable.follow')")
                == "false"
            )
            # checking it again jumps to the bottom at once
            page.check("#optFollow")
            page.wait_for_function(at_bottom)


def test_download_holds_the_plain_output_lines(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.sse(
                LOGS,
                [e2e.sse_line(t) for t in _SEARCH_LINES]
                + [e2e.sse_frame("end", {})],
                close=True,
            )
            _open_logs(page)
            _wait_line_count(page, 5)
            with page.expect_download() as info:
                page.click("#dDownload")
            download = info.value
            assert download.suggested_filename == (
                "cronstable-alpha-ok-logs.txt"
            )
            target = tmp_path / "logs.txt"
            download.save_as(str(target))
            assert target.read_text() == (
                "alpha needle one\n"
                "red needle two tail\n"
                "no match here\n"
                "NEEDLE three and needle four\n"
                "bold green plain\n"
            )
            # with timestamps on, each line leads with its clock time
            page.check("#optTs")
            with page.expect_download() as info:
                page.click("#dDownload")
            stamped = tmp_path / "stamped.txt"
            info.value.save_as(str(stamped))
            rows = stamped.read_text().splitlines()
            assert len(rows) == 5
            assert all(r[2] == ":" and r[8] == " " for r in rows)
            assert rows[0].endswith(" alpha needle one")


# --------------------------------------------------------------------------
# merged multi-tail
# --------------------------------------------------------------------------


def _tail_add(page, name):
    page.fill("#tailAddInput", name)
    page.press("#tailAddInput", "Enter")


def _tail_jobs(page):
    return page.evaluate(
        "[...document.querySelectorAll('#tailChips .tnm')]"
        ".map((c) => c.textContent)"
    )


def test_multi_tail_follows_real_jobs_across_runs(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            _click(page, "#tailBtn")
            page.wait_for_selector("#tailWrap.open")
            assert "No jobs selected" in page.inner_text("#tailTerm")
            assert page.inner_text("#tailMeta") == "0/4 jobs · 0 lines"
            _tail_add(page, "alpha-ok")
            # attaching replays the finished run, then marks its end
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('── end of run output ──')"
            )
            assert _lines(page, "tailTerm") == [
                ["stdout", "alpha-out"],
                ["sys", "── end of run output ──"],
            ]
            assert page.evaluate(
                "[...document.querySelectorAll('#tailTerm .tj')]"
                ".map((t) => t.textContent)"
            ) == ["alpha-ok", "alpha-ok"]
            # a job that never ran waits for its first output
            _tail_add(page, "epsilon-quiet")
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('no captured output yet')"
            )
            assert _tail_jobs(page) == ["alpha-ok", "epsilon-quiet"]
            # each job's label keeps one color, distinct from the other's
            colors = page.evaluate(
                "Object.fromEntries([...document.querySelectorAll("
                "'#tailTerm .tj')].map((t) => "
                "[t.textContent, t.style.color]))"
            )
            assert colors["alpha-ok"] != colors["epsilon-quiet"]

            # the governor reattaches for the next run, replay excluded
            daemon.run_and_wait("alpha-ok")
            page.wait_for_function(
                "[...document.querySelectorAll('#tailTerm .ln.stdout')]"
                ".length === 2"
            )
            daemon.run_and_wait("alpha-ok")
            page.wait_for_function(
                "[...document.querySelectorAll('#tailTerm .ln.stdout')]"
                ".length === 3"
            )
            ends = [
                t for cls, t in _lines(page, "tailTerm") if "end of run" in t
            ]
            assert len(ends) == 3
            # interleaving with a second real job
            _tail_add(page, "beta-fail")
            daemon.run_and_wait("beta-fail")
            page.wait_for_selector("#tailTerm .ln.stderr")
            assert ["stderr", "beta-err"] in _lines(page, "tailTerm")

            # search counts output lines, never the system lines
            page.fill("#tailSearch", "ALPHA-OUT")
            _count_label(page, "3 matches", "tailCount")
            assert (
                page.evaluate(
                    "document.querySelectorAll('#tailTerm mark').length"
                )
                == 3
            )
            page.fill("#tailSearch", "end of run")
            _count_label(page, "0 matches", "tailCount")
            page.fill("#tailSearch", "")

            # a label click opens that job's own drawer
            _click(page, "#tailTerm .ln.stderr .tj")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.inner_text("#dName") == "beta-fail"
            assert not page.evaluate(
                "document.getElementById('tailWrap').classList"
                ".contains('open')"
            )


def test_multi_tail_cap_presets_untail_clear_and_download(browser, tmp_path):
    jobs = [
        e2e.job("job-{}".format(i), "echo out-{}".format(i))
        for i in range(1, 6)
    ] + [e2e.job("bad", "echo bad-err >&2; exit 2")]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        for i in range(1, 6):
            daemon.run_and_wait("job-{}".format(i))
        daemon.run_and_wait("bad")
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            _click(page, "#tailBtn")
            page.wait_for_selector("#tailWrap.open")
            # presets
            _click(page, "#tailAddRunning")
            assert "info" in e2e.wait_toast(page, "no running jobs")
            _click(page, "#tailAddFailing")
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('bad-err')"
            )
            assert _tail_jobs(page) == ["bad"]
            _tail_add(page, "nope")
            assert "err" in e2e.wait_toast(page, "unknown job: nope")
            # the datalist offers only jobs that are not tailed yet
            assert page.evaluate(
                "[...document.querySelectorAll('#tailJobList option')]"
                ".map((o) => o.value)"
            ) == ["job-{}".format(i) for i in range(1, 6)]
            for i in (1, 2, 3):
                _tail_add(page, "job-{}".format(i))
            assert page.inner_text("#tailMeta").startswith("4/4 jobs")
            # the fifth stream is refused: the browser's connection budget
            _tail_add(page, "job-4")
            assert "info" in e2e.wait_toast(
                page, "stream cap 4 reached — 1 job not added"
            )
            assert _tail_jobs(page) == ["bad", "job-1", "job-2", "job-3"]
            # adding a tailed job again is a no-op
            _tail_add(page, "job-1")
            assert _tail_jobs(page) == ["bad", "job-1", "job-2", "job-3"]
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm .ln.stdout').length "
                "=== 3"
            )

            with page.expect_download() as info:
                page.click("#tailDownload")
            assert info.value.suggested_filename == "cronstable-multitail.txt"
            target = tmp_path / "tail.txt"
            info.value.save_as(str(target))
            assert sorted(target.read_text().splitlines()) == [
                "[bad] bad-err",
                "[job-1] out-1",
                "[job-2] out-2",
                "[job-3] out-3",
            ]

            # untail frees a slot
            _click(page, '#tailChips [data-untail="job-2"]')
            assert _tail_jobs(page) == ["bad", "job-1", "job-3"]
            _tail_add(page, "job-5")
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('out-5')"
            )
            page.click("#tailClearBtn")
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('waiting for output')"
            )
            assert page.inner_text("#tailMeta") == "4/4 jobs · 0 lines"
            # the tailed set survives closing and reopening the console,
            # and the reopened console replays each job's buffer once
            page.keyboard.press("Escape")
            page.wait_for_function(
                "!document.getElementById('tailWrap').classList"
                ".contains('open')"
            )
            _click(page, "#tailBtn")
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm .ln.stdout').length "
                "=== 3"
            )
            assert _tail_jobs(page) == ["bad", "job-1", "job-3", "job-5"]


def test_multi_tail_stream_errors_retry_with_one_notice(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            before_goto=lambda page: page.clock.install(),
        ) as page:
            page.faults.sse(LOGS, [e2e.sse_line("before the drop")], fail=True)
            _click(page, "#tailBtn")
            page.wait_for_selector("#tailWrap.open")
            _tail_add(page, "alpha-ok")
            page.wait_for_selector("#tailTerm .ln.sys")
            notice = "retrying (replay may repeat lines)"
            assert notice in _lines(page, "tailTerm")[1][1]
            # inside the retry throttle, polls leave the stream alone
            page.clock.fast_forward(1100)
            page.clock.fast_forward(1100)
            assert len(page.faults.sse_opens("/jobs/alpha-ok/logs")) == 1
            # past it, the governor reconnects; the same failure is not
            # announced a second time
            page.clock.fast_forward(4000)
            page.wait_for_function("(window.__sseOpens || []).length >= 2")
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm .ln.stdout').length "
                "=== 2"
            )
            sys_lines = [
                t for cls, t in _lines(page, "tailTerm") if cls == "sys"
            ]
            assert len(sys_lines) == 1
            # An HTTP error is a distinct failure and gets its own log line.
            page.faults.sse(LOGS, status=503)
            page.clock.fast_forward(6000)
            page.wait_for_function(
                "document.getElementById('tailTerm').textContent"
                ".includes('HTTP 503')"
            )
