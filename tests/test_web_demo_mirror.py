"""``docs/demo/index.html`` stays a faithful mirror of the shipped dashboard.

The demo page is the shipped dashboard plus a fake-backend layer, maintained
by hand: no build step generates it, so every dashboard edit has to be ported
across twice.  That discipline is invisible when it lapses, and a stale demo
silently misrepresents the product on the docs site, so this pins the mirror
structurally instead.

Only the deltas the mirror exists for are allowed: the ``<title>``, the
injected ``cronstable-demo-backend`` script block together with the
demo-only note that follows it about the inlined logo engine, and the blank
line at the injection point that the block consumes.  Anything else is drift.
Pure text comparison, so unlike ``test_web_engine_parity`` this runs
everywhere, including CI.
"""

import difflib
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "cronstable", "web", "index.html")
DEMO = os.path.join(ROOT, "docs", "demo", "index.html")

# The injected block, matched by its script id (kept stable for exactly this
# reason, cf. the banner comments test_web_engine_parity slices on), plus the
# demo-only note that trails it explaining why the logo engine is inlined.
# Both are part of the same insertion, so they are stripped together.
_DEMO_BLOCK = re.compile(
    r'[ \t]*<script id="cronstable-demo-backend">.*?</script>\n'
    r"(?:<!-- logo engine: copied verbatim.*?-->\n)?",
    re.DOTALL,
)
_TITLE = re.compile(r"<title>.*?</title>", re.DOTALL)


def _read(path):
    # newline="" so a stray CRLF shows up as drift rather than being
    # normalised away; the whole repo is LF.
    with open(path, encoding="utf-8", newline="") as fh:
        return fh.read()


def test_demo_is_the_dashboard_plus_only_its_fake_backend():
    web = _read(WEB)
    demo = _read(DEMO)

    stripped, count = _DEMO_BLOCK.subn("", demo)
    assert count == 1, (
        "expected exactly one <script id='cronstable-demo-backend'> block in "
        "%s, found %d. If the block was renamed, update this test; it is the "
        "anchor the mirror check relies on." % (DEMO, count)
    )
    # normalise the one intentional content delta
    stripped = _TITLE.sub(_TITLE.search(web).group(0), stripped, count=1)

    web_lines = web.splitlines(keepends=True)
    demo_lines = stripped.splitlines(keepends=True)
    if web_lines == demo_lines:
        return

    # The injection point absorbs a single blank line; tolerate that one
    # difference and nothing else.
    diff = [
        line
        for line in difflib.unified_diff(
            web_lines, demo_lines, "web", "demo", n=0
        )
        if line[:1] in "+-" and line[:3] not in ("+++", "---")
    ]
    assert diff == ["-\n"], (
        "cronstable/web/index.html and docs/demo/index.html have drifted "
        "apart. Port the change to both copies. Unexpected differences:\n"
        + "".join(diff[:40])
    )


def test_no_streamed_log_pane_measures_the_buffer_per_line():
    """No SSE line-append path reads scroll geometry or grows textContent.

    Reading ``scrollHeight``/``clientHeight`` in an append path forces a
    synchronous layout of the whole buffered block, and ``textContent +=``
    rebuilds it, so each costs O(buffer) per line: 2.5 ms per line at the
    5000-line cap, i.e. seconds of frozen main thread to fill a pane.  The
    job log pane was fixed for exactly this (see ``followBottom``), and the
    DAG task-log pane then reintroduced both, uncapped, by bypassing the
    shared infrastructure entirely, which is what makes this a guard on
    the SHAPE rather than on one call site.
    """
    web = _read(WEB)
    # the three streamed panes and the helper each one's `line` hook must
    # reach. Everything else in the page (a bulk-action progress box, a
    # status note) is bounded by a user action, not by a job's output rate.
    for opener, helper in (
        ("startDagTaskLog", "appendDagLogLine"),
        ("startStream", "appendLine"),
        ("tailConnect", "appendTailLine"),
    ):
        body = re.search(
            r"function %s\(.*?\n  \}\n" % opener, web, re.DOTALL
        )
        assert body, "%s() not found in %s" % (opener, WEB)
        body = body.group(0)
        assert helper in body, (
            "%s() no longer appends through %s(), the path that bounds the "
            "pane at MAX_LOG_LINES and follows without a forced layout"
            % (opener, helper)
        )
        for banned, why in (
            (
                "textContent +=",
                "append a node instead of rebuilding the whole buffer",
            ),
            (
                "clientHeight",
                "reading scroll geometry here forces a synchronous layout of "
                "the whole buffer; followBottom() keeps the follow bit from a "
                "passive scroll listener instead",
            ),
        ):
            assert banned not in body, (
                "%s() reintroduced `%s`: %s" % (opener, banned, why)
            )
    # and the DAG helper itself still applies the cap and the shared follow
    assert re.search(
        r"function appendDagLogLine\(.*?MAX_LOG_LINES.*?followBottom\(",
        web,
        re.DOTALL,
    ), "appendDagLogLine no longer bounds the pane / follows the shared way"


def test_demo_mirror_has_no_crlf():
    # A Windows editor or a Python open(..., "w") without newline="" rewrites
    # the whole file CRLF, which shows up as a several-thousand-line diff and
    # trips the repo's LF-only CI check.
    for path in (WEB, DEMO):
        assert "\r\n" not in _read(path), "%s picked up CRLF endings" % path
