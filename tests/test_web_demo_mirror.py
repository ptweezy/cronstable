"""``docs/demo/index.html`` stays a faithful mirror of the shipped dashboard.

The demo page is the shipped dashboard plus a fake-backend layer.  It is
GENERATED: ``scripts/build_demo.py`` splices ``docs/demo/_backend.html`` into
``cronstable/web/index.html`` and swaps the ``<title>``.  A stale demo
silently misrepresents the product on the docs site, so the mirror test
rebuilds the page with the same code and demands byte equality with the
checked-in file.  Pure text comparison, so unlike ``test_web_engine_parity``
this runs everywhere, including CI.

The file also holds the page's other copy-and-shape guards, the ones that
want the same "read the HTML and assert on its text" machinery: the streamed
log panes, the in-flight guard every secondary poll must carry, and the logo
engine's extracted docs-site copy.
"""

import difflib
import importlib.util
import os
import re

from cronstable._cliargs import THEME_HUES

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "cronstable", "web", "index.html")
DEMO = os.path.join(ROOT, "docs", "demo", "index.html")
FRAGMENT = os.path.join(ROOT, "docs", "demo", "_backend.html")
# the engine copy the docs-site pages share via <script src> (see
# test_logo_engine_extract_matches_the_dashboards_inline_copy)
ENGINE_JS = os.path.join(ROOT, "docs", "logo-engine.js")
LAB = os.path.join(ROOT, "docs", "logo-lab.html")
COMPARISON = os.path.join(ROOT, "docs", "comparison.html")
# the theme palette those two pages share; the dashboard keeps its own
# inline copy (self-containment), so the values are pinned by parity below
PALETTE = os.path.join(ROOT, "docs", "palette.css")


def _build_demo():
    """The generator itself, loaded by path: scripts/ is not a package."""
    spec = importlib.util.spec_from_file_location(
        "build_demo", os.path.join(ROOT, "scripts", "build_demo.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def test_demo_is_generated_from_the_dashboard_plus_its_fake_backend():
    rebuilt = _build_demo().build(_read(WEB), _read(FRAGMENT))
    demo = _read(DEMO)
    if rebuilt == demo:
        return
    diff = [
        line
        for line in difflib.unified_diff(
            rebuilt.splitlines(keepends=True),
            demo.splitlines(keepends=True),
            "rebuilt",
            "checked in",
            n=0,
        )
        if line[:1] in "+-" and line[:3] not in ("+++", "---")
    ]
    raise AssertionError(
        "docs/demo/index.html is stale (it is generated; never edit it by "
        "hand). Run `python scripts/build_demo.py` and commit the result. "
        "Differences:\n" + "".join(diff[:40])
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
            assert banned not in body, "%s() reintroduced `%s`: %s" % (
                opener,
                banned,
                why,
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
    assert {
        "loadCluster",
        "loadNode",
        "loadDags",
        "loadState",
        "loadFleet",
    } <= fetchers, (
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


def test_logo_engine_extract_matches_the_dashboards_inline_copy():
    """The pendulum logo engine has two copies left and one is generated.

    The dashboard inlines it (that page must stay a self-contained single
    file); the logo lab and the comparison page share docs/logo-engine.js
    via ``<script src>``; the demo mirror's inline copy is generated from
    the dashboard's by scripts/build_demo.py.  So the only drift still
    possible is dashboard vs the extracted file, and that is what this pins,
    byte for byte.
    """
    found = _ENGINE.findall(_read(WEB))
    assert len(found) == 1, (
        "expected exactly one logo engine block in %s, found %d. If the "
        "banner comment or the engine's own <script> tag changed, update "
        "_ENGINE; it is the anchor this check slices on." % (WEB, len(found))
    )
    inline = found[0][: -len("</script>")]
    extracted = _read(ENGINE_JS)
    if inline != extracted:
        diff = [
            line
            for line in difflib.unified_diff(
                inline.splitlines(keepends=True),
                extracted.splitlines(keepends=True),
                WEB,
                ENGINE_JS,
                n=0,
            )
            if line[:1] in "+-" and line[:3] not in ("+++", "---")
        ]
        raise AssertionError(
            "docs/logo-engine.js has drifted from the dashboard's inline "
            "engine. It is generated; run `python scripts/build_demo.py`. "
            "Differences:\n" + "".join(diff[:40])
        )

    # and the docs pages actually consume the shared copy, exactly once,
    # instead of quietly regrowing an inline fork
    for path in (LAB, COMPARISON):
        page = _read(path)
        count = page.count('<script src="logo-engine.js"></script>')
        assert count == 1, (
            "%s should load the shared engine via exactly one "
            '<script src="logo-engine.js"> tag, found %d' % (path, count)
        )
        assert not _ENGINE.search(page), (
            "%s regrew an inline logo engine copy; it must consume "
            "docs/logo-engine.js instead" % path
        )
    # the demo's copy is spliced from the dashboard by build_demo, so it
    # cannot DRIFT; what it can do is arrive twice, since the injected
    # fragment lands verbatim right before the engine's own script tag.
    assert len(_ENGINE.findall(_read(DEMO))) == 1, (
        "%s carries more than one logo engine copy; the demo gets exactly "
        "one, spliced from the dashboard" % DEMO
    )
    assert not _ENGINE.search(_read(FRAGMENT)), (
        "%s regrew an inline logo engine copy; the demo inherits the "
        "dashboard's through the splice" % FRAGMENT
    )


_CSS_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}")
_CSS_DECL = re.compile(r"(--[\w-]+|color-scheme)\s*:\s*([^;}]+)")


def _declarations(css):
    """{selector: {property: whitespace-normalized value}} for flat CSS.

    Good enough for the palette work it serves: comments are stripped, a
    comma list fans out to one entry per selector, and a selector declared
    twice merges with the later declaration winning, matching the cascade
    for equal-specificity rules.
    """
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    out = {}
    for selectors, body in _CSS_RULE.findall(css):
        decls = {
            prop: re.sub(r"\s+", "", value)
            for prop, value in _CSS_DECL.findall(body)
        }
        if not decls:
            continue
        for selector in selectors.split(","):
            selector = re.sub(r"\s+", " ", selector).strip()
            if selector:
                out.setdefault(selector, {}).update(decls)
    return out


def test_docs_palette_matches_the_dashboards_theme_tokens():
    """Every declaration in docs/palette.css matches the dashboard's value.

    The dashboard cannot link a stylesheet (it must stay a self-contained
    single file), so the docs pages' shared palette is a hand-maintained
    re-ink of its theme blocks. The palette drifted once before it was
    pinned: logo-lab's paper inks sat three shades bright for weeks. The
    dashboard is canonical; edit it first, then re-ink palette.css.
    """
    style = re.search(r"<style>(.*?)</style>", _read(WEB), re.DOTALL)
    assert style, "no <style> block found in %s" % WEB
    web_rules = _declarations(style.group(1))
    palette_rules = _declarations(_read(PALETTE))

    # The lab's data-pal/data-mode selectors ride along in the same rules
    # (comma lists); the dashboard is matched through the data-theme
    # selector or :root alone.
    themed = {
        selector: decls
        for selector, decls in palette_rules.items()
        if selector == ":root" or selector.startswith("html[data-theme")
    }
    expected = {":root", 'html[data-theme$="-light"]'} | {
        'html[data-theme="%s"]' % name
        for hue in THEME_HUES
        for name in (hue, hue + "-light")
    }
    assert set(themed) == expected, (
        "docs/palette.css no longer declares the full theme set the docs "
        "pages rely on. Missing: %r, unexpected: %r"
        % (sorted(expected - set(themed)), sorted(set(themed) - expected))
    )

    mismatches = []
    for selector, decls in sorted(themed.items()):
        web_decls = web_rules.get(selector, {})
        for prop, value in sorted(decls.items()):
            if web_decls.get(prop) != value:
                mismatches.append(
                    "%s { %s: %s } (dashboard has %s)"
                    % (selector, prop, value, web_decls.get(prop))
                )
    assert not mismatches, (
        "docs/palette.css has drifted from the dashboard's theme tokens "
        "(the dashboard's inline copy is canonical):\n"
        + "\n".join(mismatches)
    )


def test_dashboard_theme_hues_match_the_cli_list():
    """The dashboard's JS THEME_HUES literal equals _cliargs.THEME_HUES.

    The hue list lives in two places by construction: the CLI's stdlib-only
    leaf (which drives --theme choices, the TUI cycler, and the screenshot
    matrix in docs/screenshots) and the dashboard's inline script (which
    drives its theme cycler and pref self-healing; the demo inherits it via
    the mirror). A hue added to only one side ships half-wired: either the
    dashboard can cycle to a theme the CLI and screenshots never heard of,
    or the reverse. Order is asserted too, so every cycler walks the hues
    the same way.
    """
    m = re.search(r"const THEME_HUES = \[(.*?)\];", _read(WEB))
    assert m, "const THEME_HUES literal not found in %s" % WEB
    web_hues = re.findall(r'"([^"]*)"', m.group(1))
    assert web_hues == list(THEME_HUES), (
        "the dashboard's THEME_HUES %r has drifted from "
        "cronstable._cliargs.THEME_HUES %r" % (web_hues, list(THEME_HUES))
    )


def test_demo_mirror_has_no_crlf():
    # A Windows editor or a Python open(..., "w") without newline="" rewrites
    # the whole file CRLF, which shows up as a several-thousand-line diff and
    # trips the repo's LF-only CI check.
    for path in (WEB, DEMO, FRAGMENT, ENGINE_JS):
        assert "\r\n" not in _read(path), "%s picked up CRLF endings" % path
