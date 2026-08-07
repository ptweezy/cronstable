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


def _fn_body(html, name, path):
    """Slice a top-level function body out of the page's app script."""
    m = re.search(r"(?:async )?function %s\(.*?\n  \}\n" % name, html, re.DOTALL)
    assert m, "%s() not found in %s" % (name, path)
    return m.group(0)


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


def test_every_secondary_poll_loader_is_single_flight():
    """Every secondary poll loader routes its fetch through singleFlight().

    Each of these is fired without awaiting, from the jobs poll (loadJobs)
    or from the cluster poll, so a daemon slower than the poll interval
    used to add one more overlapping request per cycle against the
    browser's ~6-per-origin connection cap, which the held SSE tails
    already lean on.  singleFlight() is now the one mechanism: a repeat
    caller joins the pending request and gets its promise back, and the
    slot clears when it settles, so overlap is impossible and responses
    apply in request order.  The loader list is derived from the poll
    fan-out rather than hardcoded, so a new loader fired from the poll
    path is picked up automatically and must arrive wrapped.

    The honest boundary of that derivation: direct calls inside
    loadJobs, loadCluster and loadClusterLease are derived, with or
    without arguments; a loader fired through an intermediate helper is
    not.  And the wrap demand applies to the derived callees that issue
    a request of their own; a load-named helper that only renders data
    the poll already fetched (loadCell, loadClusterLease itself) has
    nothing to serialise.
    """
    web = _read(WEB)

    # the helper itself: join, promise return, clear on settle
    h = _fn_body(web, "singleFlight", WEB)
    assert "if (secondaryInFlight[key]) return secondaryInFlight[key];" in h, (
        "singleFlight() no longer hands repeat callers the pending promise; "
        "the #dag deep link's loadDags().then(open) chain would resolve "
        "before the list arrives and never open the drawer"
    )
    assert re.search(
        r"\.finally\(\(\) => \{ secondaryInFlight\[key\] = null; \}\)", h
    ), (
        "singleFlight() no longer clears its slot when the request settles; "
        "one failed request would stop the endpoint from ever polling again"
    )

    # Derive the fan-out: every direct loadX( call, with or without
    # arguments, reachable from the jobs poll (loadJobs) or the cluster
    # poll's two render branches, so a loader that grows a parameter
    # cannot slip out of the derived set.  Comments are stripped first:
    # loadClusterLease's comment mentions loadFleet() in prose.
    fanout = set()
    for fn in ("loadJobs", "loadCluster", "loadClusterLease"):
        body = re.sub(r"(?m)^\s*//.*$", "", _fn_body(web, fn, WEB))
        fanout |= set(re.findall(r"\b(load[A-Z]\w*)\(", body))
    fanout.discard("loadJobs")
    # Only the callees that issue a request of their own can overlap
    # against the connection cap; a load-named helper that renders data
    # already fetched (loadCell's table cell, loadClusterLease's lease
    # branch) has nothing to serialise.
    fetchers = {
        fn
        for fn in fanout
        if re.search(
            r"\b(?:apiFetch|fetch)\(",
            re.sub(r"(?m)^\s*//.*$", "", _fn_body(web, fn, WEB)),
        )
    }
    # sanity floor: a broken derivation must fail loudly, not pass vacuously
    assert {"loadCluster", "loadNode", "loadDags", "loadState", "loadFleet"} <= fetchers, (
        "derived only %s as fetching poll loaders; the derivation no longer "
        "sees the known ones" % sorted(fetchers)
    )

    # every derived loader wraps its fetch, and under its own key
    keys = {}
    for fn in sorted(fetchers):
        m = re.search(
            r'return singleFlight\("(\w+)", async \(\) => \{',
            _fn_body(web, fn, WEB),
        )
        assert m, (
            "%s() is fired from the poll fan-out but does not wrap its "
            "fetch in singleFlight" % fn
        )
        keys[fn] = m.group(1)
    assert len(set(keys.values())) == len(keys), (
        "two loaders share a singleFlight key and would join each other's "
        "fetches: %s" % keys
    )

    # the retired seq counters stay retired: with overlap impossible,
    # responses apply in request order and nothing needs to order the APPLY
    for relic in ("ReqSeq", "AppliedSeq", "stateSeq", "stateApplied"):
        assert relic not in web, (
            "the per-loader seq counter relic %r is back in %s" % (relic, WEB)
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
