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

The file also holds the page's other copy-and-shape guards, the ones that
want the same "read the HTML and assert on its text" machinery: the streamed
log panes, the in-flight guard every secondary poll must carry, and the logo
engine's two other verbatim copies out on the docs site.
"""

import difflib
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "cronstable", "web", "index.html")
DEMO = os.path.join(ROOT, "docs", "demo", "index.html")
# the two docs-site pages that inline the same logo engine (see
# test_logo_engine_is_identical_in_all_four_copies)
LAB = os.path.join(ROOT, "docs", "logo-lab.html")
COMPARISON = os.path.join(ROOT, "docs", "comparison.html")

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

# The logo engine block, from its banner comment to the close of the script
# tag that holds it.  Both ends are stable by construction: the banner is the
# first thing inside the tag on all four pages, and the engine always owns a
# script tag of its own (the page-specific mounting code lives in a later
# one), so the first `</script>` at column zero after the banner ends it.
_ENGINE = re.compile(r"/\* =+\n \*  logo engine.*?\n</script>", re.DOTALL)


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


def test_every_secondary_poll_loader_holds_an_in_flight_guard():
    """No secondary poll can launch a second request over its own first.

    Every one of these is fired without awaiting, from the jobs poll
    (loadJobs) or from the cluster poll, so a daemon slower than the poll
    interval used to add one more overlapping request per cycle against the
    browser's ~6-per-origin connection cap, which the held SSE tails already
    lean on.  The per-loader sequence numbers only order the APPLY; they never
    stopped the launch.  /fleet was the loader the earlier guard sweep missed,
    which is what makes this a guard on the whole set rather than on one call
    site.
    """
    web = _read(WEB)
    loaders = {
        "loadCluster": "cluster",
        "loadNode": "node",
        "loadDags": "dags",
        "loadState": "state",
        "loadFleet": "fleet",
    }
    decl = re.search(r"const secondaryInFlight = \{(.*?)\};", web)
    assert decl, "the secondaryInFlight registry is gone from %s" % WEB
    keys = set(re.findall(r"(\w+): false", decl.group(1)))
    assert keys == set(loaders.values()), (
        "secondaryInFlight declares %s but this test knows %s. A new "
        "secondary poll endpoint belongs in the loaders map above, with "
        "its guard."
        % (sorted(keys), sorted(loaders.values()))
    )
    for fn, key in loaders.items():
        body = re.search(
            r"(?:async )?function %s\(.*?\n  \}\n" % fn, web, re.DOTALL
        )
        assert body, "%s() not found in %s" % (fn, WEB)
        body = body.group(0)
        if key == "dags":
            # loadDags guards by handing back the PENDING promise, not a
            # bare return: the #dag deep link chains loadDags().then(open),
            # and at bootstrap the poll cycle has always already started
            # the fetch, so a bare return resolved the chain against a
            # list that had not arrived and deep links never opened the
            # drawer. The slot holds the promise itself (truthy), so the
            # skip-a-cycle behavior below still holds.
            assert (
                "if (secondaryInFlight.dags) return secondaryInFlight.dags;"
                in body
            ), (
                "loadDags() no longer returns its in-flight promise; the "
                "#dag deep link's loadDags().then(open) chain would resolve "
                "before the list arrives and never open the drawer"
            )
            assert "secondaryInFlight.dags = (async () => {" in body, (
                "loadDags() no longer claims its in-flight slot with the "
                "pending promise"
            )
        else:
            assert "if (secondaryInFlight.%s) return;" % key in body, (
                "%s() no longer skips the cycle while its predecessor is "
                "still pending; overlapping requests stack up against the "
                "browser's per-origin connection cap" % fn
            )
            assert "secondaryInFlight.%s = true;" % key in body, (
                "%s() no longer claims its in-flight slot" % fn
            )
        # released from a finally, never from the happy path alone: an early
        # return or a throw would otherwise wedge the slot true and the
        # endpoint would never be polled again for the life of the page.
        assert re.search(
            r"finally\s*\{[^}]*secondaryInFlight\.%s = false;" % key, body
        ), (
            "%s() must clear its in-flight slot in a finally block, or one "
            "failed request stops the endpoint from ever polling again" % fn
        )


def test_logo_engine_is_identical_in_all_four_copies():
    """The pendulum logo engine is one implementation living in four files.

    The dashboard owns it; the demo mirror, the logo lab and the comparison
    page each inline a verbatim copy because GitHub Pages serves them with no
    build step to share a file.  Only the web/demo pair had a drift guard, so
    a fix applied to three of the four could sit unnoticed on the docs site
    for as long as nobody happened to diff them.  The check is byte equality,
    since each copy is pasted in wholesale.
    """
    blocks = {}
    for path in (WEB, DEMO, LAB, COMPARISON):
        found = _ENGINE.findall(_read(path))
        assert len(found) == 1, (
            "expected exactly one logo engine block in %s, found %d. If the "
            "banner comment or the engine's own <script> tag changed, "
            "update _ENGINE; it is the anchor this check slices on."
            % (path, len(found))
        )
        blocks[path] = found[0]

    canonical = blocks[WEB]
    for path, block in blocks.items():
        if block == canonical:
            continue
        diff = [
            line
            for line in difflib.unified_diff(
                canonical.splitlines(keepends=True),
                block.splitlines(keepends=True),
                WEB,
                path,
                n=0,
            )
            if line[:1] in "+-" and line[:3] not in ("+++", "---")
        ]
        raise AssertionError(
            "the logo engine in %s has drifted from the dashboard's copy in "
            "%s. Re-copy the block wholesale into every page that inlines it "
            "(%s, %s, %s). Differences:\n%s"
            % (path, WEB, DEMO, LAB, COMPARISON, "".join(diff[:40]))
        )


def test_demo_mirror_has_no_crlf():
    # A Windows editor or a Python open(..., "w") without newline="" rewrites
    # the whole file CRLF, which shows up as a several-thousand-line diff and
    # trips the repo's LF-only CI check.
    for path in (WEB, DEMO):
        assert "\r\n" not in _read(path), "%s picked up CRLF endings" % path
