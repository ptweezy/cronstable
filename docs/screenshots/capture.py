"""Capture the cronstable docs/README imagery from one script.

Usage: python capture.py <target> [...]

Targets (each was once its own capture_*.py; flags, defaults, staging
order and output paths are unchanged):

  dashboard  web-dashboard stills off the running grand-tour fleet
  tui        terminal-dashboard stills off the same fleet
  showcase   still frames for the animated reel + theme row (same fleet)
  logo       the pendulum-logo loops (needs no daemon)
  logs       the single-job live-log-tail closeup (logs-demo daemon)

dashboard, tui and showcase accept shot/scene names after the target to
capture a subset; the other targets take no arguments. See each target's
--help for the full story, and README.md next door for the runbook
(daemon prerequisites, staging order, timing windows).
"""
import argparse
import asyncio
import html
import http.server
import json
import math
import random
import re
import sys
import threading
import time
import urllib.request
from functools import partial
from io import BytesIO
from pathlib import Path

# playwright, Pillow and cronstable.tui are imported inside the target
# runners: each target keeps exactly the dependency footprint its old
# script had, and every --help works without any of them installed.

BASE = "http://localhost:8080"       # the grand-tour fleet
HERE = Path(__file__).parent
ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "cronstable" / "web"

# The one module-scope cronstable import: a stdlib-only leaf resolved from
# the checkout itself, so --help keeps working with nothing installed and
# the theme matrix follows the CLI's canonical hue list (a new hue cannot
# ship without screenshots).
sys.path.insert(0, str(ROOT))
from cronstable._cliargs import THEME_HUES  # noqa: E402
SHOTS = HERE / "shots"               # every target's output dir but showcase's
REEL = HERE / "reel"                 # showcase frames for build_reel.py

# clean release-style version for the header (the local build carries a long
# setuptools-scm dev string; a release install shows a clean one like this)
VERSION = "1.2.31"

# what the pair-a-device shot's QR encodes as the bearer, shared by the
# dashboard and showcase pair shots. The grand-tour fleet runs
# unauthenticated, so the panel is staged: this obviously-fake token is
# seeded into sessionStorage and /whoami is fulfilled as an all-scopes
# match so the scoped-token warning line is in frame too.
DEMO_TOKEN = "demo-2f9c41d8a67e4b21"
WHOAMI_BODY = json.dumps(
    {
        "authenticated": True,
        "label": "authToken",
        "scopes": ["approve", "control", "view"],
        "allScopes": True,
    }
)

ONLY: set = set()    # shot/scene subset from the CLI, set by the target runner
results: dict = {}


# ===================================================================
#  shared scaffolding (fleet API, version stub, web-page helpers)
# ===================================================================
def api(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method)
    data = None
    if body is not None:
        req.add_header("Content-Type", "application/json")
        data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req, data=data, timeout=10) as r:
            raw = r.read().decode()
            try:
                return json.loads(raw)
            except Exception:
                return raw
    except Exception as e:
        print(f"    api {method} {path}: {e}")
        return None


def wants(name):
    return not ONLY or name in ONLY


def route_version(ctx):
    """Substitute the clean release version for the header chip."""
    ctx.route(
        "**/version",
        lambda route: route.fulfill(
            status=200,
            content_type="text/plain; charset=utf-8",
            body=VERSION,
        ),
    )


# the mark is a live pendulum sim (rAF-driven, so reduced-motion
# CSS can't freeze it); park it balanced at exact upright the
# moment it mounts so no shot catches it mid-sway. The hook wraps
# mountGlyph as window.CronstableLogo is assigned — page code runs
# unmodified, and a reload re-parks automatically. sync() is
# stubbed so nothing (kickMark, live-state flips) restarts it.
PARK_HOOK = (
    "(()=>{let CL;Object.defineProperty(window,'CronstableLogo',{"
    "configurable:true,get:()=>CL,set:(v)=>{const orig=v.mountGlyph;"
    "v.mountGlyph=function(slot,opts){const L=orig.call(v,slot,opts);"
    "L.sync=()=>{};if(L._raf)cancelAnimationFrame(L._raf);L._raf=0;"
    "L.sim.opts.breeze=false;L.sim.setConnected(true);"
    "L.sim.s=[0,0,0,0,0,0];L.sim.mode='balance';L.sim.a=0;"
    "L._render();window.__pendLogo=L;return L;};CL=v;}});})();"
)


def close_overlays(page):
    for _ in range(3):
        page.keyboard.press("Escape")
        page.wait_for_timeout(150)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(200)


def set_sort(page, order="status"):
    try:
        page.select_option("#sortSel", order)
        page.wait_for_timeout(300)
    except Exception as e:
        print(f"    set_sort: {e}")


def wait_ready(page, min_rows=40, timeout=60000):
    page.wait_for_function(
        f"document.querySelectorAll('#rows tr').length >= {min_rows}",
        timeout=timeout,
    )
    # under distribution: spread the Owner column arrives with cluster data and
    # flips the fluid (wide) layout; wait for it so no frame catches the narrow
    # centered layout mid-transition (harmless timeout on non-cluster daemons)
    try:
        page.wait_for_function(
            "document.querySelector('main').classList.contains('wide')",
            timeout=15000,
        )
    except Exception:
        pass
    page.wait_for_timeout(500)
    set_sort(page)


def open_job(page, name, tab=None):
    close_overlays(page)
    row = page.locator("#rows tr", has_text=name).first
    row.scroll_into_view_if_needed()
    row.click()
    page.wait_for_selector("#drawer.open", timeout=5000)
    if tab:
        page.click(f'#dTabs button[data-tab="{tab}"]')
    page.wait_for_timeout(1200)


def scroll_card(page, sel):
    page.wait_for_selector(sel, state="visible", timeout=15000)
    page.evaluate(
        """(sel) => {
            const el = document.querySelector(sel);
            const y = el.getBoundingClientRect().top + window.scrollY - 66;
            window.scrollTo(0, Math.max(0, y));
        }""",
        sel,
    )
    page.wait_for_timeout(400)


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


# ===================================================================
#  target: dashboard (web stills off the grand-tour fleet)
# ===================================================================
def shot(page, name):
    page.screenshot(path=str(SHOTS / f"{name}.png"))
    results[name] = "ok"
    print(f"  [shot] {name}")


def fresh(page, theme=None, extra_prefs=None):
    """Reload the page with a given theme/pref set via localStorage."""
    prefs = {"boot": "false", "zen": "false"}
    # always pin the theme: a prior themed reload leaves its choice in
    # localStorage, and the committed docs images are carolina, so "no
    # theme" here means carolina, not "keep"
    prefs["theme"] = json.dumps(theme or "carolina")
    if extra_prefs:
        prefs.update(extra_prefs)
    js = ";".join(
        f"localStorage.setItem('cronstable.{k}', {json.dumps(v)})"
        for k, v in prefs.items()
    )
    page.evaluate(js)
    page.reload()
    wait_ready(page)


def run_dashboard(shots_only):
    """Capture cronstable dashboard screenshots off the running grand-tour
    fleet.

    Usage: python capture.py dashboard [shot ...]
    With no args, captures every shot. Shots are saved to ./shots/ at 2880x1800
    (1440x900 viewport, deviceScaleFactor=2), matching the existing docs/img
    set.

    Order matters: clean-board shots come first; deliberately-staged failures
    (incident correlation) come last so they don't pollute earlier frames.
    """
    global ONLY
    ONLY = set(shots_only)
    from playwright.sync_api import sync_playwright

    SHOTS.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            # 16:10 like the original 1440x900 set, but wide enough that the
            # spread-mode board (Owner column + resource chips) and the 9-node
            # fleet matrix render without clipping under the fluid layout
            viewport={"width": 1680, "height": 1050},
            device_scale_factor=2,
            # page CSP has no unsafe-eval; needed for evaluate()
            bypass_csp=True,
            # headless default suppresses the boot POST
            reduced_motion="no-preference",
        )
        route_version(ctx)
        ctx.add_init_script(
            "try{if(!localStorage.getItem('cronstable.bootWant'))"
            "localStorage.setItem('cronstable.boot','false');"
            "localStorage.setItem('cronstable.zen','false');}catch(e){}"
            # park the pendulum mark at exact upright (see PARK_HOOK)
            + PARK_HOOK
        )
        page = ctx.new_page()
        page.goto(BASE)
        wait_ready(page)

        # ---- boot self-test (needs boot pref ON; capture mid-POST) ----
        if wants("dashboard-boot"):
            try:
                page.evaluate(
                    "localStorage.setItem('cronstable.bootWant','1');"
                    "localStorage.setItem('cronstable.boot','true');"
                    # 12h replay gate
                    "localStorage.removeItem('cronstable.bootShownAt')"
                )
                page.reload()
                page.wait_for_selector(
                    "#bootScreen", state="visible", timeout=8000
                )
                # shoot inside the READY hold (650 ms at full opacity, every
                # POST line printed) — a fixed sleep races the fade-out and
                # catches a washed-out overlay instead
                page.wait_for_selector("#bootLog .boot-ready", timeout=8000)
                # pin the READY cursor on (its 1s blink is 50/50 at shot time)
                page.add_style_tag(
                    content=".boot-cur{animation:none!important;"
                    "opacity:1!important}"
                )
                page.wait_for_timeout(150)
                shot(page, "dashboard-boot")
            except Exception as e:
                results["dashboard-boot"] = f"FAIL {e}"
                print(f"  boot shot failed: {e}")
            page.evaluate(
                "localStorage.removeItem('cronstable.bootWant');"
                "localStorage.setItem('cronstable.boot','false')"
            )
            page.reload()
            wait_ready(page)

        # ---- stage the hero: one deliberate red + a guaranteed cpu-burner
        # ----
        api("POST", "/jobs/alert-selftest/start")  # fails instantly, by design
        api(
            "POST", "/jobs/risk-model-recompute/start"
        )  # 30s CPU burn -> live cpu%
        page.wait_for_timeout(7000)  # let a poll land

        if wants("dashboard-overview"):
            try:
                close_overlays(page)
                set_sort(page)
                shot(page, "dashboard-overview")
            except Exception as e:
                results["dashboard-overview"] = f"FAIL {e}"

        # ---- theme variants on the same board ----
        for theme, fname in [
            ("amber", "dashboard-theme-amber"),
            ("green", "dashboard-theme-green"),
            ("modern", "dashboard-theme-modern"),
            ("carolina-light", "dashboard-theme-carolina-light"),
        ]:
            if not wants(fname):
                continue
            try:
                fresh(page, theme=theme)
                page.wait_for_timeout(1500)
                close_overlays(page)
                set_sort(page)
                shot(page, fname)
            except Exception as e:
                results[fname] = f"FAIL {e}"
        if not ONLY or any(
            wants(f"dashboard-theme-{t}")
            for t in ("amber", "green", "modern", "carolina-light")
        ):
            fresh(page)  # back to the pinned carolina baseline

        # ---- job drawer: live logs on the 5s heartbeat probe ----
        if wants("dashboard-logs"):
            try:
                open_job(page, "pulse-liveness", tab="logs")
                page.check("#optTs")
                page.wait_for_timeout(14000)  # accumulate a few probe runs
                page.fill("#logSearch", "UP")
                page.wait_for_timeout(800)
                shot(page, "dashboard-logs")
                close_overlays(page)
            except Exception as e:
                results["dashboard-logs"] = f"FAIL {e}"
                close_overlays(page)

        # ---- history + per-run cpu/peak-mem (monitorResources) ----
        if wants("dashboard-history"):
            try:
                open_job(page, "risk-model-recompute", tab="history")
                page.wait_for_timeout(1500)
                shot(page, "dashboard-history")
                close_overlays(page)
            except Exception as e:
                results["dashboard-history"] = f"FAIL {e}"
                close_overlays(page)

        # ---- schedule tab on a timezone job ----
        if wants("dashboard-schedule"):
            try:
                open_job(page, "finance-eod-close", tab="schedule")
                page.wait_for_timeout(800)
                shot(page, "dashboard-schedule")
                close_overlays(page)
            except Exception as e:
                results["dashboard-schedule"] = f"FAIL {e}"
                close_overlays(page)

        # ---- command palette ----
        if wants("dashboard-palette"):
            try:
                close_overlays(page)
                page.keyboard.press("Control+k")
                page.wait_for_selector(
                    "#paletteWrap.open, #paletteWrap.show", timeout=4000
                )
                page.fill("#paletteInput", "run")
                page.wait_for_timeout(600)
                shot(page, "dashboard-palette")
                close_overlays(page)
            except Exception as e:
                results["dashboard-palette"] = f"FAIL {e}"
                close_overlays(page)

        # ---- shortcut overlay ----
        if wants("dashboard-shortcuts"):
            try:
                close_overlays(page)
                page.keyboard.type("?")
                page.wait_for_selector(
                    "#helpWrap.open, #helpWrap.show", timeout=4000
                )
                page.wait_for_timeout(400)
                shot(page, "dashboard-shortcuts")
                close_overlays(page)
            except Exception as e:
                results["dashboard-shortcuts"] = f"FAIL {e}"
                close_overlays(page)

        # ---- settings ----
        if wants("dashboard-settings"):
            try:
                close_overlays(page)
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.wait_for_timeout(400)
                shot(page, "dashboard-settings")
                close_overlays(page)
            except Exception as e:
                results["dashboard-settings"] = f"FAIL {e}"
                close_overlays(page)

        # ---- pair-a-device panel (QR + the scoped-token warning) ----
        if wants("dashboard-pair"):
            try:
                close_overlays(page)
                page.route(
                    "**/whoami",
                    lambda route: route.fulfill(
                        status=200,
                        content_type="application/json",
                        body=WHOAMI_BODY,
                    ),
                )
                page.evaluate(
                    "sessionStorage.setItem('cronstable_token',"
                    f" {json.dumps(DEMO_TOKEN)})"
                )
                # the settings row closes the sheet before opening the
                # panel, so this path is deterministic (the palette's
                # fuzzy ranking is not)
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.click("#openPair")
                page.wait_for_selector("#pairWrap.open", timeout=4000)
                page.wait_for_selector("#pairQr svg", timeout=4000)
                page.wait_for_selector(
                    "#pairWarn", state="visible", timeout=4000
                )
                page.wait_for_timeout(400)
                shot(page, "dashboard-pair")
                close_overlays(page)
                page.unroute("**/whoami")
                page.evaluate(
                    "sessionStorage.removeItem('cronstable_token')"
                )
            except Exception as e:
                results["dashboard-pair"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG index card ----
        if wants("dashboard-dags"):
            try:
                close_overlays(page)
                scroll_card(page, "#dagCard")
                shot(page, "dashboard-dags")
            except Exception as e:
                results["dashboard-dags"] = f"FAIL {e}"

        # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
        if wants("dashboard-dag-graph"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/data-quality-gate/trigger")
                print(f"    data-quality-gate trigger -> {r}")
                page.wait_for_timeout(3500)
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="data-quality-gate"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1000)
                # open the newest run, then its graph
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="graph"]')
                page.wait_for_timeout(1200)
                shot(page, "dashboard-dag-graph")
                close_overlays(page)
            except Exception as e:
                results["dashboard-dag-graph"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG approval gate: release-train waits on a human ----
        if wants("dashboard-dag-approval"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/release-train/trigger")
                print(f"    release-train trigger -> {r}")
                run_key = r.get("runKey") if isinstance(r, dict) else None
                # poll the run document until the gate parks awaiting a
                # decision
                for _ in range(45):
                    page.wait_for_timeout(2000)
                    if not run_key:
                        break
                    doc = api("GET", f"/dags/release-train/runs/{run_key}")
                    tasks = (doc or {}).get("tasks") or {}
                    vals = tasks.values() if isinstance(tasks, dict) else tasks
                    if any(
                        isinstance(t, dict) and t.get("awaitingApproval")
                        for t in vals
                    ):
                        break
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="release-train"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1500)
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(1000)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="tasks"]')
                page.wait_for_selector("[data-approve]", timeout=20000)
                page.wait_for_timeout(500)
                shot(page, "dashboard-dag-approval")
                close_overlays(page)
            except Exception as e:
                results["dashboard-dag-approval"] = f"FAIL {e}"
                close_overlays(page)

        # ---- cluster panel (9 peers + per-node load) ----
        if wants("dashboard-cluster"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                shot(page, "dashboard-cluster")
            except Exception as e:
                results["dashboard-cluster"] = f"FAIL {e}"

        # ---- fleet view (jobs x nodes matrix) ----
        if wants("dashboard-fleet"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                page.click("#fleetBtn")
                page.wait_for_selector(
                    "#fleetPanel", state="visible", timeout=8000
                )
                page.wait_for_timeout(2500)
                scroll_card(page, "#fleetPanel")
                shot(page, "dashboard-fleet")
                page.click("#fleetBtn")  # toggle back off
            except Exception as e:
                results["dashboard-fleet"] = f"FAIL {e}"

        # ---- durable-state inspector ----
        if wants("dashboard-state"):
            try:
                fresh(page, extra_prefs={"stateInsp": "true"})
                scroll_card(page, "#stateCard")
                page.wait_for_timeout(2500)
                shot(page, "dashboard-state")
                fresh(page)  # back to defaults
            except Exception as e:
                results["dashboard-state"] = f"FAIL {e}"
                fresh(page)

        # ---- multi-tail console ----
        if wants("dashboard-multitail"):
            try:
                close_overlays(page)
                page.click("#tailBtn")
                page.wait_for_selector(
                    "#tailWrap.open, #tailWrap.show", timeout=4000
                )
                # the console caps at 4 streams: seed the two second-level
                # probes first (steady line flow), let +failing fill the rest
                for j in ("pulse-liveness", "pulse-latency"):
                    page.fill("#tailAddInput", j)
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(400)
                page.click("#tailAddFailing")
                page.wait_for_timeout(20000)  # accumulate merged lines
                shot(page, "dashboard-multitail")
                close_overlays(page)
            except Exception as e:
                results["dashboard-multitail"] = f"FAIL {e}"
                close_overlays(page)

        # ---- heatmap (may be sparse on a young fleet) ----
        if wants("dashboard-heatmap"):
            try:
                close_overlays(page)
                page.click("#heatBtn")
                page.wait_for_selector(
                    "#heatCard", state="visible", timeout=8000
                )
                page.wait_for_timeout(4000)
                scroll_card(page, "#heatCard")
                shot(page, "dashboard-heatmap")
                page.click("#heatBtn")
            except Exception as e:
                results["dashboard-heatmap"] = f"FAIL {e}"

        # ---- week calendar (business-day chips + the background-hum strip) ----
        if wants("dashboard-week"):
            try:
                close_overlays(page)
                page.click("#weekBtn")
                page.wait_for_selector(
                    "#weekCard", state="visible", timeout=8000
                )
                page.wait_for_selector("#weekBody .wk-ev", timeout=8000)
                scroll_card(page, "#weekCard")
                shot(page, "dashboard-week")
                page.click("#weekBtn")
            except Exception as e:
                results["dashboard-week"] = f"FAIL {e}"

        # ---- LAST: stage a correlated multi-job failure (incident tools) ----
        if (
            wants("dashboard-incident")
            or wants("dashboard-incident-timeline")
            or wants("dashboard-wallboard")
        ):
            try:
                for j in (
                    "db-health-orders",
                    "db-health-inventory",
                    "db-health-payments",
                    "db-health-warehouse",
                ):
                    api("POST", f"/jobs/{j}/start")
                page.wait_for_timeout(9000)
                close_overlays(page)
                set_sort(page)
                if wants("dashboard-incident"):
                    page.wait_for_selector(
                        "#verdictBar", state="visible", timeout=10000
                    )
                    shot(page, "dashboard-incident")
                if wants("dashboard-incident-timeline"):
                    page.keyboard.type("i")
                    page.wait_for_selector(
                        "#timelineWrap.open, #timelineWrap.show", timeout=4000
                    )
                    page.wait_for_timeout(600)
                    shot(page, "dashboard-incident-timeline")
                    close_overlays(page)
            except Exception as e:
                results["dashboard-incident"] = f"FAIL {e}"
                close_overlays(page)

        # ---- wallboard, worst-first with the incident set lit up ----
        if wants("dashboard-wallboard"):
            try:
                close_overlays(page)
                # the toolbar button is deterministic; the "w" hotkey is
                # swallowed if a closing overlay still holds focus
                page.click("#tvBtn")
                page.wait_for_selector(
                    "#wallboard", state="visible", timeout=4000
                )
                page.wait_for_timeout(1500)
                shot(page, "dashboard-wallboard")
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
            except Exception as e:
                results["dashboard-wallboard"] = f"FAIL {e}"
                close_overlays(page)

        browser.close()

    print("\n== capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


# ===================================================================
#  target: tui (terminal-dashboard stills off the same fleet)
# ===================================================================
COLS, LINES = 150, 38
TUI_FRAMES: dict = {}


# -------------------------------------------------------------------
#  driving the app
# -------------------------------------------------------------------
async def wait_for(pred, timeout=30.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        await asyncio.sleep(0.05)
    print(f"    wait timed out: {what}")
    return False


async def snap(app, term, name):
    """One clean frame: no toasts, freshly painted after our changes."""
    app.toasts = []
    app.version = VERSION
    app.mark()
    count = len(term.frames)
    await wait_for(lambda: len(term.frames) > count, 10, "paint " + name)
    TUI_FRAMES[name] = list(term.frames[-1])
    results[name] = "ok"
    print(f"  [shot] {name}")


def reset(app):
    """Back to a clean board between stages (Esc-all, filters off)."""
    while app.open_overlays:
        app.close(app.open_overlays[-1])
    app.wallboard = False
    app.zen_on = False
    app.filter_text = ""
    app.inputs["filter"] = ""
    app.inputs["logsearch"] = ""
    app.focus = None
    app.timestamps = False
    app.recompute_view()
    app.mark()


async def capture_boot(prefs_file):
    if not wants("tui-boot"):
        return
    prefs = dict(PREF_DEFAULTS)
    prefs["poll_ms"] = 1000
    keys = ScriptedKeys()
    term = HeadlessTerm(COLS, LINES)
    app = TuiApp(
        Api(BASE, None),
        term,
        keys,
        prefs,
        boot=True,
        prefs_file=prefs_file,
    )
    task = asyncio.get_running_loop().create_task(app.run())
    await wait_for(lambda: app.booting, 15, "boot starts")
    await wait_for(lambda: not app.booting, 45, "boot finishes")
    # the last frame still showing the POST is the finished self-test;
    # its verdict line varies with fleet health (ALL CHECKS PASSED /
    # N JOBS FAILING / DEGRADED), so accept any of them
    for frame in reversed(term.frames):
        text = "\n".join(strip_ansi(r) for r in frame)
        if "POWER-ON SELF-TEST" in text and any(
            marker in text for marker in ("CHECKS", "FAILING", "DEGRADED")
        ):
            TUI_FRAMES["tui-boot"] = list(frame)
            results["tui-boot"] = "ok"
            print("  [shot] tui-boot")
            break
    else:
        results["tui-boot"] = "FAIL no boot frame"
    app.quit = True
    keys.send("q")
    try:
        await asyncio.wait_for(task, 10)
    except asyncio.TimeoutError:
        task.cancel()


async def capture_all():  # noqa: C901 - one linear staging walk
    prefs_file = str(SHOTS / "tui-capture-prefs.json")
    await capture_boot(prefs_file)

    prefs = dict(PREF_DEFAULTS)
    prefs["poll_ms"] = 1000
    prefs["boot"] = False
    keys = ScriptedKeys()
    term = HeadlessTerm(COLS, LINES)
    app = TuiApp(
        Api(BASE, None),
        term,
        keys,
        prefs,
        boot=False,
        prefs_file=prefs_file,
    )
    task = asyncio.get_running_loop().create_task(app.run())
    await wait_for(lambda: len(app.jobs) >= 40, 60, "fleet jobs load")
    app.version = VERSION

    # ---- hero: one deliberate red + a guaranteed cpu-burner ----------
    api("POST", "/jobs/alert-selftest/start")
    api("POST", "/jobs/risk-model-recompute/start")
    await asyncio.sleep(7)  # let a poll land, like the web capture
    app.sort_key = "status"
    app.recompute_view()
    if wants("tui-overview"):
        reset(app)
        app.sort_key = "status"
        app.recompute_view()
        await snap(app, term, "tui-overview")

    # ---- theme variants on the same board ----------------------------
    for hue, light, fname in [
        ("amber", False, "tui-theme-amber"),
        ("green", False, "tui-theme-green"),
        ("modern", False, "tui-theme-modern"),
        ("carolina", True, "tui-theme-carolina-light"),
    ]:
        if not wants(fname):
            continue
        app.prefs["theme"], app.prefs["light"] = hue, light
        app._retheme()
        await snap(app, term, fname)
    app.prefs["theme"], app.prefs["light"] = "carolina", False
    app._retheme()

    # ---- job drawer: live logs on the 5s heartbeat probe -------------
    if wants("tui-logs"):
        reset(app)
        app.open_drawer("pulse-liveness", "logs")
        app.timestamps = True
        await wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) > 3,
            30,
            "probe log lines",
        )
        await asyncio.sleep(10)  # accumulate a few probe runs
        app.inputs["logsearch"] = "UP"
        app._log_search_recompute()
        await snap(app, term, "tui-logs")
        reset(app)

    # ---- history + per-run cpu/peak-mem (monitorResources) -----------
    if wants("tui-history"):
        app.open_drawer("risk-model-recompute", "history")
        await wait_for(lambda: app.drawer_runs is not None, 15, "history")
        await snap(app, term, "tui-history")
        reset(app)

    # ---- schedule tab on a timezone job ------------------------------
    if wants("tui-schedule"):
        app.open_drawer("finance-eod-close", "schedule")
        await asyncio.sleep(0.8)
        await snap(app, term, "tui-schedule")
        reset(app)

    # ---- command palette ---------------------------------------------
    if wants("tui-palette"):
        await app.handle_key("ctrl+k")
        for ch in "run":
            await app.handle_key(ch)
        await snap(app, term, "tui-palette")
        reset(app)

    # ---- shortcut overlay --------------------------------------------
    if wants("tui-shortcuts"):
        await app.handle_key("?")
        await snap(app, term, "tui-shortcuts")
        reset(app)

    # ---- settings ----------------------------------------------------
    if wants("tui-settings"):
        app.open("settings")
        await snap(app, term, "tui-settings")
        reset(app)

    # ---- DAG index panel ---------------------------------------------
    if wants("tui-dags"):
        app._toggle_dags()
        await wait_for(lambda: app.dags, 15, "dags load")
        await snap(app, term, "tui-dags")
        reset(app)

    # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
    if wants("tui-dag-graph"):
        r = api("POST", "/dags/data-quality-gate/trigger")
        print(f"    data-quality-gate trigger -> {r}")
        await asyncio.sleep(3.5)
        app.open_dag("data-quality-gate")
        await wait_for(lambda: app.dag_runs, 15, "dag runs")
        await app.handle_key("enter")  # newest run -> tasks tab
        await wait_for(lambda: app.dag_run is not None, 15, "run doc")
        app.dag_tab = "graph"
        await snap(app, term, "tui-dag-graph")
        reset(app)

    # ---- DAG approval gate: release-train waits on a human -----------
    if wants("tui-dag-approval"):
        r = api("POST", "/dags/release-train/trigger")
        print(f"    release-train trigger -> {r}")
        run_key = r.get("runKey") if isinstance(r, dict) else None
        for _ in range(45):
            await asyncio.sleep(2)
            if not run_key:
                break
            doc = api("GET", f"/dags/release-train/runs/{run_key}")
            tasks = (doc or {}).get("tasks") or {}
            vals = tasks.values() if isinstance(tasks, dict) else tasks
            if any(
                isinstance(t, dict) and t.get("awaitingApproval") for t in vals
            ):
                break
        app.open_dag("release-train")
        await wait_for(lambda: app.dag_runs, 15, "release runs")
        await app.handle_key("enter")
        await wait_for(lambda: app.dag_run is not None, 15, "release doc")
        app.dag_tab = "tasks"
        await snap(app, term, "tui-dag-approval")
        reset(app)

    # ---- cluster panel (9 peers) -------------------------------------
    if wants("tui-cluster"):
        app._toggle("cluster")
        await wait_for(
            lambda: (app.cluster or {}).get("enabled"), 15, "cluster"
        )
        await snap(app, term, "tui-cluster")
        reset(app)

    # ---- fleet matrix (jobs x nodes) ---------------------------------
    if wants("tui-fleet"):
        app._toggle("fleet")
        await wait_for(
            lambda: len((app.fleet or {}).get("nodes") or []) >= 9,
            30,
            "fleet nodes",
        )
        await snap(app, term, "tui-fleet")
        reset(app)

    # ---- durable-state inspector -------------------------------------
    if wants("tui-state"):
        app._toggle("state")
        await wait_for(
            lambda: (app.state_data or {}).get("enabled"), 20, "state"
        )
        await snap(app, term, "tui-state")
        reset(app)

    # ---- multi-tail console ------------------------------------------
    if wants("tui-multitail"):
        app.open_tail(["pulse-liveness", "pulse-latency"])
        failing = [j["name"] for j in app.jobs if health(j)[0] == "fail"]
        for name in failing[:2]:
            app.add_tail(name)
        await asyncio.sleep(20)  # accumulate merged lines
        app.timestamps = True
        await snap(app, term, "tui-multitail")
        reset(app)

    # ---- heatmap ------------------------------------------------------
    if wants("tui-heatmap"):
        app._toggle("heat")
        await wait_for(lambda: app.heat_data, 60, "heatmap batch")
        await snap(app, term, "tui-heatmap")
        reset(app)

    # ---- LAST: the correlated db-health incident ---------------------
    if (
        wants("tui-incident")
        or wants("tui-incident-timeline")
        or wants("tui-wallboard")
    ):
        # the staged db-health jobs only FAIL while the UTC minute is
        # 15-19 (a simulated outage window; see platform.yaml). Wait
        # for the window, nudge all four to run right away, then wait
        # for the ×4 CORRELATED verdict, not merely a crit one, which
        # the other staged failures already keep lit.
        now = time.time()
        minute = int((now // 60) % 60)
        if not (15 <= minute <= 19):
            wait_s = ((15 - minute) % 60) * 60 - (now % 60) + 5
            print(f"    waiting {wait_s:.0f}s for the :15-:19 window")
            await asyncio.sleep(wait_s)
        for j in (
            "db-health-orders",
            "db-health-inventory",
            "db-health-payments",
            "db-health-warehouse",
        ):
            api("POST", f"/jobs/{j}/start")
        await wait_for(
            lambda: (
                app.verdict is not None
                and "share exit=" in app.verdict.get("sub", "")
            ),
            40,
            "correlated verdict",
        )
        await asyncio.sleep(2)
        if wants("tui-incident"):
            reset(app)
            app.sort_key = "status"
            app.recompute_view()
            await snap(app, term, "tui-incident")
        if wants("tui-incident-timeline"):
            await app.handle_key("i")
            await snap(app, term, "tui-incident-timeline")
            reset(app)
        if wants("tui-wallboard"):
            app.set_wallboard(True)
            await asyncio.sleep(1.5)
            await snap(app, term, "tui-wallboard")
            app.set_wallboard(False)

    app.quit = True
    keys.send("q")
    try:
        await asyncio.wait_for(task, 10)
    except asyncio.TimeoutError:
        task.cancel()


# -------------------------------------------------------------------
#  rasterizing ANSI frames -> PNG
# -------------------------------------------------------------------
SGR = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_ANY = re.compile(
    r"\x1b(?:\[[0-9;:?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)?)"
)
DEF_FG = "#9ed3f5"

#: per-theme page/window chrome behind the frame (theme -> bg of frame)
_THEME_BG = {
    "tui-theme-amber": "#160d02",
    "tui-theme-green": "#03130a",
    "tui-theme-modern": "#101418",
    "tui-theme-carolina-light": "#eef4f9",
}
_THEME_FG = {
    "tui-theme-amber": "#f5c169",
    "tui-theme-green": "#7ee2a1",
    "tui-theme-modern": "#d7dde3",
    "tui-theme-carolina-light": "#173751",
}


def row_to_html(row, def_fg):
    out = []
    fg, bg, bold, dim, rev = def_fg, None, False, False, False
    pos = 0

    def emit(text):
        if not text:
            return
        f, b = (fg, bg)
        if rev:
            f, b = (b or "#000"), fg
        style = [f"color:{f}"]
        if b:
            style.append(f"background:{b}")
        if bold:
            style.append("font-weight:700")
        if dim:
            style.append("opacity:.62")
        out.append(
            f'<span style="{";".join(style)}">{html.escape(text)}</span>'
        )

    for match in ANSI_ANY.finditer(row):
        emit(row[pos : match.start()])
        pos = match.end()
        sgr = SGR.fullmatch(match.group(0))
        if not sgr:
            continue
        parts = (sgr.group(1) or "0").split(";")
        i = 0
        while i < len(parts):
            code = int(parts[i] or "0")
            if code == 0:
                fg, bg, bold, dim, rev = def_fg, None, False, False, False
            elif code == 1:
                bold = True
            elif code == 2:
                dim = True
            elif code == 7:
                rev = True
            elif code == 22:
                bold = dim = False
            elif code == 27:
                rev = False
            elif code == 38 and i + 1 < len(parts) and parts[i + 1] == "2":
                fg = "#%02x%02x%02x" % (
                    int(parts[i + 2]),
                    int(parts[i + 3]),
                    int(parts[i + 4]),
                )
                i += 4
            elif code == 48 and i + 1 < len(parts) and parts[i + 1] == "2":
                bg = "#%02x%02x%02x" % (
                    int(parts[i + 2]),
                    int(parts[i + 3]),
                    int(parts[i + 4]),
                )
                i += 4
            i += 1
    emit(row[pos:])
    return "".join(out)


def frame_html(name, rows):
    term_bg = _THEME_BG.get(name, "#06131d")
    def_fg = _THEME_FG.get(name, DEF_FG)
    body = "\n".join(row_to_html(r.rstrip(), def_fg) or "&nbsp;" for r in rows)
    bar_bg = "#0a1a28" if name not in _THEME_BG else term_bg
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
html,body {{ margin:0; padding:24px; background:#101418; }}
.term {{
  display:inline-block; background:{term_bg};
  border:1px solid rgba(128,148,170,.28); border-radius:10px;
  overflow:hidden; box-shadow:0 16px 44px rgba(0,0,0,.5);
}}
.bar {{
  display:flex; align-items:center; gap:8px; padding:9px 14px;
  background:{bar_bg}; border-bottom:1px solid rgba(128,148,170,.22);
}}
.dot {{ width:11px; height:11px; border-radius:50%;
  background:rgba(128,148,170,.35); }}
.title {{ margin-left:8px; font:500 12px/1 "Cascadia Mono", Consolas,
  monospace; color:rgba(128,148,170,.85); }}
pre {{
  margin:0; padding:10px 14px;
  font:13px/1.32 "Cascadia Mono", Consolas, monospace;
  font-variant-numeric: tabular-nums;
}}
</style></head><body>
<div class="term"><div class="bar"><span class="dot"></span>
<span class="dot"></span><span class="dot"></span>
<span class="title">cronstable tui</span></div>
<pre>{body}</pre></div>
</body></html>"""


def render_pngs():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(device_scale_factor=2).new_page()
        for name, rows in TUI_FRAMES.items():
            page.set_content(frame_html(name, rows))
            page.wait_for_timeout(120)
            card = page.locator(".term")
            card.screenshot(path=str(SHOTS / f"{name}.png"))
            print(f"  [png] {name}.png")
        browser.close()


def run_tui(shots_only):
    """Capture cronstable TUI screenshots off the running grand-tour fleet.

    Usage: python capture.py tui [shot ...]
    With no args, captures every shot. Shots land in ./shots/ as tui-*.png.

    The TUI is driven for real: a headless :class:`cronstable.tui.TuiApp`
    (HeadlessTerm + ScriptedKeys standing in for the tty) runs against
    meridian-a of the grand tour, the same fleet the web dashboard shots
    use, staged the same way (the deliberate red + CPU burner for the hero,
    DAG runs including parking release-train on its approval gate, and the
    correlated db-health incident saved for last).  Captured ANSI frames
    are then rasterized to PNG through Playwright's Chromium at
    deviceScaleFactor 2, in a terminal-window card styled after Windows
    Terminal, with Cascadia Mono.

    Like the web capture, the fleet should have 10-15 minutes of uptime
    first so sparklines and history have filled in.
    """
    # the TUI import is deferred into the runner so no other target needs
    # the package importable; the global statement points the capture
    # helpers above at the imported names
    global ONLY, PREF_DEFAULTS, Api, HeadlessTerm, ScriptedKeys, TuiApp
    global health, strip_ansi
    ONLY = set(shots_only)
    sys.path.insert(0, str(ROOT))
    from cronstable.tui import (  # noqa: E402
        PREF_DEFAULTS,
        Api,
        HeadlessTerm,
        ScriptedKeys,
        TuiApp,
        health,
        strip_ansi,
    )

    SHOTS.mkdir(exist_ok=True)
    asyncio.run(capture_all())
    if TUI_FRAMES:
        render_pngs()
    print("\n== capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


# ===================================================================
#  target: showcase (still frames for the reel + theme row)
# ===================================================================
# the full theme matrix (every hue x dark/paper); the overview is shot under
# all of them to drive the theme-row loop, other scenes take a tasteful subset
ALL_THEMES = list(THEME_HUES) + [h + "-light" for h in THEME_HUES]

# the hero reel stays in one theme throughout: the light carolina (paper)
# look. Marquee scenes and the a11y beat are shot here in both fonts so the
# reel can use either without another capture pass.
HERO_THEME = "carolina-light"

manifest = {}   # scene -> [theme, ...] actually captured


def set_theme_live(page, theme):
    """Flip the palette on the *current* frame, no reload. Drives the app's own
    settings <select id=setTheme> so the real applyTheme() runs and prefs
    persist. Falls back to setting the attribute by hand."""
    ok = page.evaluate(
        """(t) => {
          const s = document.querySelector('#setTheme');
          if (s) { s.value = t;
                   s.dispatchEvent(new Event('change', {bubbles: true}));
                   return document.documentElement.getAttribute('data-theme') === t; }
          return false;
        }""",
        theme,
    )
    if not ok:
        page.evaluate(
            "(t) => { document.documentElement.setAttribute('data-theme', t); }",
            theme,
        )
    page.wait_for_timeout(450)


def set_select(page, sel_id, value):
    """Drive any settings <select> (font/scale/cvd) the same way as the theme
    picker -- dispatch a real change so the app's applyA11y() runs."""
    page.evaluate(
        """([id, v]) => {
          const s = document.querySelector('#' + id);
          if (s) { s.value = v;
                   s.dispatchEvent(new Event('change', {bubbles: true})); }
        }""",
        [sel_id, str(value)],
    )
    page.wait_for_timeout(450)


def reel_shot(page, name):
    page.screenshot(path=str(REEL / f"{name}.png"))
    manifest.setdefault(name.split("@")[0], []).append(
        name.split("@")[1] if "@" in name else "carolina"
    )
    results[name.split("@")[0]] = "ok"
    print(f"  [shot] {name}")


def shoot_themes(page, scene, themes, clip=None):
    """Take one screenshot of the staged scene per theme."""
    got = []
    for theme in themes:
        try:
            set_theme_live(page, theme)
            page.screenshot(path=str(REEL / f"{scene}@{theme}.png"), clip=clip)
            got.append(theme)
            print(f"  [shot] {scene}@{theme}")
        except Exception as e:
            print(f"    {scene}@{theme}: {e}")
    if got:
        manifest[scene] = got
        results[scene] = "ok"
    else:
        results[scene] = "FAIL no frames"
    # leave the board on carolina for the next scene's staging
    set_theme_live(page, "carolina")


def shoot_combo(page, scene, combos, clip=None):
    """Shoot the staged scene under a list of (theme, font) pairs, driving both
    the theme picker and the font select live so the frame stays pixel-stable.
    File names encode both axes: `<scene>@<theme>` (mono) or
    `<scene>-sans@<theme>` (sans). Resets to carolina/mono afterwards."""
    got = []
    for theme, font in combos:
        try:
            set_theme_live(page, theme)
            set_select(page, "setFont", font)
            page.wait_for_timeout(400)
            stem = scene if font == "mono" else f"{scene}-sans"
            page.screenshot(path=str(REEL / f"{stem}@{theme}.png"), clip=clip)
            manifest.setdefault(stem, []).append(theme)
            got.append((theme, font))
            print(f"  [shot] {stem}@{theme} ({font})")
        except Exception as e:
            print(f"    {scene} {theme}/{font}: {e}")
    set_select(page, "setFont", "mono")
    set_theme_live(page, "carolina")
    results[scene] = "ok" if got else "FAIL no frames"


def run_showcase(scenes_only):
    """Capture the animated-showcase frames off the running grand-tour fleet.

    This is the still-frame source for the README's animated hero reel
    (`docs/img/dashboard-reel.webp`) and the animated theme row
    (`docs/img/dashboard-themes.webp`). It stages each marquee screen exactly
    once, then re-shoots that *identical* frame under a rotation of themes by
    calling the page's own `setTheme()` live (no reload, so the board, scroll
    position and any open overlay stay pixel-stable while only the palette
    changes). `build_reel.py` then stitches these stills into the loops.

    Usage: python capture.py showcase [scene ...]
    With no args, captures every scene. Raw frames land in ./reel/ as
    `<scene>@<theme>.png` alongside a `manifest.json` the builder reads.

    Order matters, exactly as in the dashboard target: the clean-board scenes
    come first and the deliberately-staged correlated failure (the four
    `db-health-*` reds that light up the incident tools) is saved for last so
    it does not bleed into earlier frames.
    """
    global ONLY
    ONLY = set(scenes_only)
    from playwright.sync_api import sync_playwright

    REEL.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            # 16:9 and >=1680 wide so the spread-mode board (Owner column +
            # resource chips) and the 9-node fleet matrix never clip; the
            # builder supersamples this down to the reel's 1280-wide frame
            viewport={"width": 1680, "height": 945},
            device_scale_factor=2,
            bypass_csp=True,
            reduced_motion="no-preference",
        )
        route_version(ctx)
        ctx.add_init_script(
            "try{if(!localStorage.getItem('cronstable.bootWant'))"
            "localStorage.setItem('cronstable.boot','false');"
            "localStorage.setItem('cronstable.zen','false');}catch(e){}"
            # park the pendulum mark at exact upright at mount so every frame
            # is pixel-identical across themes/fonts (see PARK_HOOK)
            + PARK_HOOK
        )
        page = ctx.new_page()
        page.goto(BASE)
        wait_ready(page)

        # ---- boot self-test (carolina only; capture mid-POST) ----
        if wants("boot"):
            try:
                page.evaluate(
                    "localStorage.setItem('cronstable.bootWant','1');"
                    "localStorage.setItem('cronstable.boot','true');"
                    "localStorage.removeItem('cronstable.bootShownAt')"
                )
                page.reload()
                page.wait_for_selector(
                    "#bootScreen", state="visible", timeout=8000
                )
                # shoot inside the READY hold (650 ms at full opacity, every
                # POST line printed) — a fixed sleep races the fade-out
                page.wait_for_selector("#bootLog .boot-ready", timeout=8000)
                # pin the READY cursor on (its 1s blink is 50/50 at shot time)
                page.add_style_tag(
                    content=".boot-cur{animation:none!important;"
                    "opacity:1!important}"
                )
                page.wait_for_timeout(150)
                page.screenshot(path=str(REEL / "boot@carolina.png"))
                manifest["boot"] = ["carolina"]
                results["boot"] = "ok"
                print("  [shot] boot@carolina")
            except Exception as e:
                results["boot"] = f"FAIL {e}"
                print(f"  boot shot failed: {e}")
            page.evaluate(
                "localStorage.removeItem('cronstable.bootWant');"
                "localStorage.setItem('cronstable.boot','false')"
            )
            page.reload()
            wait_ready(page)

        # ---- stage the hero board: one deliberate red + a live cpu-burner ----
        api("POST", "/jobs/alert-selftest/start")       # fails instantly
        api("POST", "/jobs/risk-model-recompute/start")  # 30s CPU burn
        page.wait_for_timeout(7000)

        # ---- overview: the marquee frame, shot under ALL ten themes ----
        if wants("overview"):
            close_overlays(page)
            set_sort(page)
            shoot_themes(page, "overview", ALL_THEMES)   # mono, every theme
            # ...and the same board in the readable sans font under every
            # theme too, so the theme row can show BOTH axes (theme x font)
            shoot_combo(page, "overview", [(t, "sans") for t in ALL_THEMES])

        # ---- command palette ----
        if wants("palette"):
            try:
                close_overlays(page)
                page.keyboard.press("Control+k")
                page.wait_for_selector(
                    "#paletteWrap.open, #paletteWrap.show", timeout=4000
                )
                page.fill("#paletteInput", "run")
                page.wait_for_timeout(600)
                shoot_combo(page, "palette",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["palette"] = f"FAIL {e}"
                close_overlays(page)

        # ---- pair-a-device panel (QR + the scoped-token warning) ----
        if wants("pair"):
            try:
                close_overlays(page)
                page.route(
                    "**/whoami",
                    lambda route: route.fulfill(
                        status=200,
                        content_type="application/json",
                        body=WHOAMI_BODY,
                    ),
                )
                page.evaluate(
                    "sessionStorage.setItem('cronstable_token',"
                    f" {json.dumps(DEMO_TOKEN)})"
                )
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.click("#openPair")
                page.wait_for_selector("#pairWrap.open", timeout=4000)
                page.wait_for_selector("#pairQr svg", timeout=4000)
                page.wait_for_selector(
                    "#pairWarn", state="visible", timeout=4000
                )
                page.wait_for_timeout(400)
                shoot_combo(page, "pair",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
                page.unroute("**/whoami")
                page.evaluate(
                    "sessionStorage.removeItem('cronstable_token')"
                )
            except Exception as e:
                results["pair"] = f"FAIL {e}"
                close_overlays(page)

        # ---- job drawer: live logs on the 5s heartbeat probe ----
        if wants("logs"):
            try:
                open_job(page, "pulse-liveness", tab="logs")
                page.check("#optTs")
                page.wait_for_timeout(14000)
                page.fill("#logSearch", "UP")
                page.wait_for_timeout(800)
                shoot_combo(page, "logs",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["logs"] = f"FAIL {e}"
                close_overlays(page)

        # ---- history + per-run cpu/peak-mem ----
        if wants("history"):
            try:
                open_job(page, "risk-model-recompute", tab="history")
                page.wait_for_timeout(1500)
                shoot_combo(page, "history",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["history"] = f"FAIL {e}"
                close_overlays(page)

        # ---- schedule tab on a timezone job ----
        if wants("schedule"):
            try:
                open_job(page, "finance-eod-close", tab="schedule")
                page.wait_for_timeout(800)
                shoot_combo(page, "schedule",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["schedule"] = f"FAIL {e}"
                close_overlays(page)

        # ---- DAG run: trigger the diamond, catch the graph mid-flight ----
        if wants("dag-graph"):
            try:
                close_overlays(page)
                r = api("POST", "/dags/data-quality-gate/trigger")
                print(f"    data-quality-gate trigger -> {r}")
                page.wait_for_timeout(3500)
                scroll_card(page, "#dagCard")
                page.click('[data-dagopen="data-quality-gate"]')
                page.wait_for_selector("#dagDrawer.open", timeout=5000)
                page.wait_for_timeout(1000)
                try:
                    page.locator("#dgRuns tr[data-runkey]").first.click(
                        timeout=3000
                    )
                    page.wait_for_timeout(500)
                except Exception:
                    pass
                page.click('#dagTabs button[data-dtab="graph"]')
                page.wait_for_timeout(1200)
                shoot_combo(page, "dag-graph",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["dag-graph"] = f"FAIL {e}"
                close_overlays(page)

        # ---- cluster panel (9 peers + per-node load) ----
        if wants("cluster"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                shoot_combo(page, "cluster",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
            except Exception as e:
                results["cluster"] = f"FAIL {e}"

        # ---- fleet view (jobs x nodes matrix) ----
        if wants("fleet"):
            try:
                close_overlays(page)
                scroll_card(page, "#clusterCard")
                page.click("#fleetBtn")
                page.wait_for_selector(
                    "#fleetPanel", state="visible", timeout=8000
                )
                page.wait_for_timeout(2500)
                scroll_card(page, "#fleetPanel")
                shoot_combo(page, "fleet",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                page.click("#fleetBtn")
            except Exception as e:
                results["fleet"] = f"FAIL {e}"

        # ---- accessibility beat, in the hero's carolina-light theme: the same
        # board made colour-blind safe (deuteranopia) and zoomed (125%), each
        # in both fonts so the reel can use either. ----
        if wants("a11y"):
            try:
                close_overlays(page)
                set_sort(page)
                set_theme_live(page, HERO_THEME)
                # 1) colour-vision-deficiency palette (deuteranopia)
                set_select(page, "setCvd", "deutan")
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(500)
                    stem = "a11y-cvd" + ("-sans" if font == "sans" else "")
                    reel_shot(page, f"{stem}@{HERO_THEME}")
                set_select(page, "setCvd", "none")
                # 2) larger UI scale
                set_select(page, "setScale", "125")
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(500)
                    stem = "a11y-scale" + ("-sans" if font == "sans" else "")
                    reel_shot(page, f"{stem}@{HERO_THEME}")
                # reset every a11y pref + theme for the scenes that follow
                set_select(page, "setScale", "100")
                set_select(page, "setFont", "mono")
                set_select(page, "setCvd", "none")
                set_theme_live(page, "carolina")
                page.wait_for_timeout(300)
            except Exception as e:
                results["a11y"] = f"FAIL {e}"
                set_select(page, "setScale", "100")
                set_select(page, "setFont", "mono")
                set_select(page, "setCvd", "none")
                set_theme_live(page, "carolina")

        # ---- settings panel (carolina-light), scrolled to the a11y controls ----
        if wants("settings-a11y"):
            try:
                close_overlays(page)
                set_theme_live(page, HERO_THEME)
                page.click("#settingsBtn")
                page.wait_for_selector(
                    "#settingsWrap.open, #settingsWrap.show", timeout=4000
                )
                page.wait_for_timeout(300)
                # bring the Interface font / UI scale / colour-vision selects
                # into view inside the settings panel
                try:
                    page.eval_on_selector(
                        "#setFont",
                        "el => el.scrollIntoView({block: 'center'})",
                    )
                except Exception:
                    pass
                for font in ("mono", "sans"):
                    set_select(page, "setFont", font)
                    page.wait_for_timeout(400)
                    stem = "settings-a11y" + ("-sans" if font == "sans" else "")
                    reel_shot(page, f"{stem}@{HERO_THEME}")
                set_select(page, "setFont", "mono")
                close_overlays(page)
                set_theme_live(page, "carolina")
            except Exception as e:
                results["settings-a11y"] = f"FAIL {e}"
                close_overlays(page)
                set_theme_live(page, "carolina")

        # ---- LAST: stage the correlated multi-job failure (incident tools) ----
        need_incident = (
            wants("wallboard") or wants("incident-timeline")
        )
        if need_incident:
            for j in (
                "db-health-orders", "db-health-inventory",
                "db-health-payments", "db-health-warehouse",
            ):
                api("POST", f"/jobs/{j}/start")
            page.wait_for_timeout(9000)
            close_overlays(page)
            set_sort(page)

        # ---- incident timeline overlay ----
        if wants("incident-timeline"):
            try:
                page.keyboard.type("i")
                page.wait_for_selector(
                    "#timelineWrap.open, #timelineWrap.show", timeout=4000
                )
                page.wait_for_timeout(600)
                shoot_combo(page, "incident-timeline",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                close_overlays(page)
            except Exception as e:
                results["incident-timeline"] = f"FAIL {e}"
                close_overlays(page)

        # ---- wallboard, worst-first with the incident set lit up ----
        if wants("wallboard"):
            try:
                close_overlays(page)
                # the toolbar button is deterministic; the "w" hotkey is
                # swallowed if a closing overlay still holds focus
                page.click("#tvBtn")
                page.wait_for_selector(
                    "#wallboard", state="visible", timeout=4000
                )
                page.wait_for_timeout(1500)
                shoot_combo(page, "wallboard",
                            [(HERO_THEME, "mono"), (HERO_THEME, "sans")])
                page.keyboard.press("Escape")
                page.wait_for_timeout(400)
            except Exception as e:
                results["wallboard"] = f"FAIL {e}"
                close_overlays(page)

        browser.close()

    # merge into any existing manifest so single-scene reruns don't wipe others
    mpath = REEL / "manifest.json"
    existing = {}
    if mpath.exists():
        try:
            existing = json.loads(mpath.read_text())
        except Exception:
            existing = {}
    existing.update(manifest)
    mpath.write_text(json.dumps(existing, indent=2))

    print("\n== showcase capture summary ==")
    for k, v in results.items():
        print(f"  {k}: {v}")
    fails = [k for k, v in results.items() if v != "ok"]
    sys.exit(1 if fails else 0)


# ===================================================================
#  target: logo (the pendulum-brand loops; serves the working tree)
# ===================================================================
LOGO_PORT = 8123

FRAME_MS = 20         # 50 fps: the ~20ms GIF decoder floor, and an exact sim dt
FRAMES = 9000         # 180.0 s loop (stillness is frame-free, see run_logo)
PAD = 9               # css px around the wordmark (glow tails + swing room)
SCALE = 3             # device pixels per css px. The README shows the loop at
                      # its intrinsic size, so this IS the zoom: 3 renders the
                      # wordmark half again larger than the dashboard PNGs' 2.

# WebP is the primary (24-bit colour -> no 256-palette banding on the glow or
# the glitch's saturated ghosts); the GIF is a 256-colour twin for clients that
# don't render animated WebP. Mirrors docs/screenshots/build_reel.py.
WEBP_LOSSLESS = True  # True = pixel-perfect (larger); False = high-q lossy
WEBP_QUALITY = 92     # lossy mode: fidelity. lossless mode: compression effort (->100)
WEBP_METHOD = 6       # libwebp effort (0-6): best ratio

# ---- choreography (frame numbers at 20 ms/frame) -----------------------------
# Ink hops (each one glitches AND knocks): base -> six glitched stops -> base.
# Deliberately off-beat spacing so the glitches don't read as a metronome; the
# first hop comes early so a fresh page load isn't 20 s of stillness.
HOP_FRAMES = [420, 1720, 2680, 4150, 6050, 7180, 8300]
# Every hop pokes the pendulum (rad/s on both joints). The sim's own seeded
# RNG picks the direction and the shoulder/elbow split; the magnitude is drawn
# per hop from the ranges below by events_map(seed). The glyph mount's track
# is sharply asymmetric (the cart has only ~0.5 m of run-out on the "e" side),
# so anything much past ~0.6 usually tips a BALANCED mark clean over (probed
# empirically; the seed search still verifies survival per seed). The
# signal-cut hop draws hotter: its motor is dead, toppling is the whole point,
# and the extra energy just varies the collapse.
POKE_SWAY = (0.28, 0.62)       # the six survivable stumbles
POKE_DROP = (0.40, 0.90)       # the knock that rides the disconnect
DISCONNECT_AT = HOP_FRAMES[3]  # fourth hop cuts the motor: collapse + offline swing
RECONNECT_AT = 4310            # 3.2 s later the signal returns: swing-up begins
CATCH_WINDOW = (4550, 5300)    # the verified catch must land in here (91-106 s)
CATCH_LOOSE = (RECONNECT_AT + 100, 5450)   # widened once before giving up
SIM_SEEDS = range(1, 97)       # candidate sim seeds for the search
SEARCH_CHUNK = 8               # seeds per in-page batch (progress + early exit)

# ---- stillness gate ----------------------------------------------------------
# A frame is only frozen (duration-extended, not screenshotted) once the sim
# is balanced, the swing trail has fully drained, and the state has stayed
# under these bounds for FREEZE_HOLD consecutive frames. At the mount's scale
# (~168 device px/m, ~0.6 m reach) the bounds keep any residual motion, and
# therefore the freeze cut itself and the loop seam, under ~0.5 device px.
FREEZE_CALM = 0.004   # |th1| + |th2| + 0.3*(|w1| + |w2|), wrapped rad
FREEZE_X = 0.0015     # cart offset from the l's cell (m)
FREEZE_XD = 0.008     # cart speed (m/s)
FREEZE_HOLD = 25      # consecutive calm frames (0.5 s) before freezing

# ---- glitch (tuned live in docs/screenshots/logo-glitch-tuner) --------------
# The ink SWITCHES clean to the target theme and holds (no dim); on top of it the
# glyph is torn into a chromatic misregistration — offset colour
# silhouettes that jitter per frame — plus a digital slice tear.
GLITCH_MS = 200       # length of each glitch/switch (clamped to whole frames, ~10f)
GLITCH_SPLIT = 4.5    # px of chromatic offset for the colour ghosts at peak
GLITCH_TEAR = 3       # horizontal slice shears at the peak (0 = none)
GLITCH_MOVE = 2.0     # css px the wordmark itself eases sideways & back (peak, *SCALE)
SEED = 20260708       # fixes the per-frame glitch jitter AND the poke schedule

# the saturated silhouette inks the glyph splits into during a glitch. Screened
# on over a dark theme (they glow) and multiplied under a paper theme (they read
# as CMYK misregistration), so each variant gets its own tuned set.
GHOSTS_DARK = [(255, 46, 104), (54, 214, 255), (60, 255, 150), (188, 108, 255)]
GHOSTS_PAPER = [(210, 24, 66), (0, 132, 194), (22, 150, 74), (124, 48, 196)]

# The colour JOURNEY each loop takes: the base theme, six glitched stops (the
# three-ink cast revisited, never the same ink twice in a row), and a final
# hop home so the base spans the loop seam. The fourth stop doubles as the
# signal-loss event (see the choreography block above): it wears "modern",
# the neutral beat, so the amber recovering bobs and the accent swing trail
# read against quiet ink.
# NB: "standard" is intentionally left out — for the logo it's a near-white,
# no-glow ink indistinguishable from "modern", so two adjacent white stops would
# look like a glitch that changes nothing. One neutral beat (modern) is plenty.
# (basename — each variant writes both <name>.webp and <name>.gif)
VARIANTS = [
    ("logo-balance", "carolina",
     ["amber", "green", "amber", "modern", "green", "amber"]),
    ("logo-balance-light", "carolina-light",
     ["amber-light", "green-light", "amber-light", "modern-light",
      "green-light", "amber-light"]),
]

# brand-box ink tokens, lifted verbatim from index.html. Setting these inline on
# <html> outranks the `html[data-theme=...]` rules, so the mark + wordmark +
# glow all flick to the target palette without disturbing the background.
# --pending inks the bobs while the mark is down/recovering, --border2 the rail:
# both are part of the pendulum's dress and must hop with the rest of the ink.
INK_KEYS = ("--fg", "--fg-dim", "--fg-faint", "--accent", "--glow",
            "--pending", "--border2")
THEME_INK = {
    "amber":          ("#ffb000", "#c98a2c", "#a37a38", "#ffd98a",
                       "rgba(255,176,0,.55)", "#ffbf47", "#523c1a"),
    "green":          ("#38ff7a", "#1fbf57", "#22a85c", "#b6ffce",
                       "rgba(56,255,122,.5)", "#ffbf47", "#1d6038"),
    "modern":         ("#d6dee8", "#9aa4b2", "#8b95a3", "#79c0ff",
                       "rgba(121,192,255,.0)", "#ffbf47", "#3a424f"),
    "standard":       ("#e6e9ed", "#a6aeb9", "#8f98a3", "#4c8dff",
                       "rgba(76,141,255,0)", "#ffbf47", "#48505a"),
    "amber-light":    ("#3f2d00", "#6d500b", "#755c20", "#855700",
                       "rgba(138,90,0,.28)", "#7d5300", "#a98f52"),
    "green-light":    ("#0b3a1e", "#1a6238", "#3a7050", "#0d7034",
                       "rgba(18,105,55,.25)", "#7d5300", "#7fb28c"),
    "modern-light":   ("#1f2328", "#57606a", "#656e79", "#0969da",
                       "rgba(9,105,218,0)", "#7d5300", "#a8b3bd"),
    "standard-light": ("#14181d", "#4b5563", "#5d6675", "#0b5ed7",
                       "rgba(11,94,215,0)", "#b45309", "#aab3bf"),
}

GLITCH_LEN = max(1, round(GLITCH_MS / FRAME_MS))


def events_map(seed):
    """frame -> sim events for one candidate seed, shared verbatim by the seed
    search and the capture (the two MUST consume the sim identically or the
    search lies). The poke magnitudes are the seed's own: drawn from a
    generator keyed on (SEED, seed) so every candidate performs a different
    set of stumbles and the search vets the schedule it will actually film."""
    rng = random.Random(SEED * 1_000_003 + seed)
    ev = {}
    for f in HOP_FRAMES:
        lo, hi = POKE_DROP if f == DISCONNECT_AT else POKE_SWAY
        ev[f] = {"poke": round(rng.uniform(lo, hi), 3)}
    ev[DISCONNECT_AT]["conn"] = False
    ev[RECONNECT_AT] = {"conn": True}
    return ev


def wrap_a(a):
    """Wrap an angle to (-pi, pi], mirroring the page's wrapA."""
    a = math.fmod(a, 2 * math.pi)
    if a > math.pi:
        a -= 2 * math.pi
    elif a <= -math.pi:
        a += 2 * math.pi
    return a


def is_calm(st):
    """True when the sim state JS_DRIVE returned is still enough to freeze:
    balanced, trail drained, and any residual motion sub-pixel at the mount's
    scale (see the stillness-gate block)."""
    s = st["s"]
    sway = abs(wrap_a(s[2])) + abs(wrap_a(s[4])) + 0.3 * (abs(s[3]) + abs(s[5]))
    return (st["mode"] == "balance" and st["trail"] == 0
            and sway < FREEZE_CALM
            and abs(s[0]) < FREEZE_X and abs(s[1]) < FREEZE_XD)


# Captures the header's mounted CronstableLogo instance the moment the page
# creates it: mountGlyph is wrapped as window.CronstableLogo is assigned, and
# the returned logo lands on window.__pendLogo. The dynamic right gate
# (railMax) is pinned OFF: the capture drives sim.step()+_render() directly
# (never _gateStep), the seed search is choreographed against the word-edge
# track, and a mid-page gate cap would balloon the '#mark svg' rect — and
# with it the clip box — to half the viewport. Otherwise noninvasive — the
# page's own code runs unmodified.
INIT_HOOK = (
    "(() => { let CL;"
    "Object.defineProperty(window, 'CronstableLogo', {"
    " configurable: true, get: () => CL,"
    " set: (v) => { const orig = v.mountGlyph;"
    "  v.mountGlyph = function (slot, opts) {"
    "   const logo = orig.call(v, slot, Object.assign({}, opts, { railMax: null }));"
    "   window.__pendLogo = logo; return logo; };"
    "  CL = v; } }); })();"
)

# Unhook the page's animation (its rAF loop + anything that could restart it)
# and rebuild the sim on the chosen seed: connected, balanced, exactly upright —
# frame 0 of the loop. breeze:false is the whole point of the choreography: the
# live page's ambient sway is off, so the only disturbances are the pokes.
JS_SETUP = """(seed) => {
  const L = window.__pendLogo;
  L.sync = () => {};                       // kickMark() etc. may not restart us
  if (L._raf) cancelAnimationFrame(L._raf);
  L._raf = 0;
  L.sim = new window.CronstableLogo.Sim(L.sim.p, { seed, breeze: false, planBudgetMs: 0 });
  // defensive: railMax is pinned off above, but if the dynamic gate is ever
  // re-enabled here, a pre-setup disconnect must not freeze it extended
  const gt = L._gate;
  if (gt) { gt.x = gt.rest; gt.settledAt = -1; gt.pending = null; L._gateDrawn = null; }
  L.trail.length = 0;
  L._render();
}"""

# Replay each candidate's event timeline through fresh sims (physics only) and
# report how the performance went. Runs inside the page so the physics is the
# page's own, byte for byte.
JS_SEARCH = """([frames, eventsBySeed, seeds, gate]) => {
  const params = window.__pendLogo.sim.p;
  const wrap = (a) => { const T = 2 * Math.PI;
    a = ((a % T) + T) % T; return a > Math.PI ? a - T : a; };
  const out = [];
  for (const seed of seeds) {
    const events = eventsBySeed[seed];
    const sim = new window.CronstableLogo.Sim(params, { seed, breeze: false, planBudgetMs: 0 });
    let catchAt = -1, fellEarly = false, clamped = false, lost = false;
    for (let k = 0; k < frames; k++) {
      const ev = events[k];
      if (ev) {
        if (ev.conn !== undefined) sim.setConnected(ev.conn);
        if (ev.poke) sim.poke(ev.poke);
      }
      sim.step(0.02);
      if (k < gate && sim.mode !== 'balance') fellEarly = true;
      if (sim.s[0] >= sim.xMax * 1.045 || sim.s[0] <= sim.xMin * 1.045) clamped = true;
      if (k > gate && catchAt < 0 && sim.mode === 'balance') catchAt = k;
      if (catchAt > 0 && k > catchAt && sim.mode !== 'balance') { lost = true; catchAt = -1; }
    }
    const s = sim.s;
    out.push({ seed, catchAt, fellEarly, clamped, lost,
      endCalm: Math.abs(wrap(s[2])) + Math.abs(wrap(s[4]))
             + 0.3 * (Math.abs(s[3]) + Math.abs(s[5])),
      endX: Math.abs(s[0]) });
  }
  return out;
}"""

# One captured frame: apply this frame's sim events and ink, step the sim by
# exactly one frame, render, let the compositor settle, and report the state
# so the capture can decide when the mark has gone still enough to freeze.
JS_DRIVE = """([ev, ink, dtMs]) => new Promise((res) => {
  const L = window.__pendLogo;
  if (ev) {
    if (ev.conn !== undefined) L.sim.setConnected(ev.conn);
    if (ev.poke) L.sim.poke(ev.poke);
  }
  const el = document.documentElement,
        keys = ['--fg','--fg-dim','--fg-faint','--accent','--glow',
                '--pending','--border2'];
  if (ink) keys.forEach((k, i) => el.style.setProperty(k, ink[k]));
  else keys.forEach((k) => el.style.removeProperty(k));
  L.sim.step(dtMs / 1000);
  L._render();
  requestAnimationFrame(() => requestAnimationFrame(() =>
    res({ mode: L.sim.mode, s: L.sim.s.slice(), trail: L.trail.length })));
})"""

# A frozen stretch: the screen holds the previous frame while the physics
# steps on underneath, render-free, in one round trip. No event ever lands
# inside one of these spans (the capture always stops at the next event
# frame), and a calm balanced sim with the breeze off cannot wake itself up.
JS_RUN = """([n, dtMs]) => {
  const L = window.__pendLogo, dt = dtMs / 1000;
  for (let i = 0; i < n; i++) L.sim.step(dt);
}"""


def glitch_env(i, span):
    """0..1 aberration strength across a glitch of `span` frames: one smooth
    rise-and-fall, ~0 at the ends so the base wordmark returns home cleanly. The
    colour *switch* itself is separate (held from the hop onward); this only
    shapes the chromatic-split / tear / base-slide intensity."""
    return math.sin(math.pi * (i + 0.5) / span)


def _silhouette(gray, color, paper):
    """A single-colour copy of the ink, neutral against its ground: bright on
    black (screen), white-backed on paper (multiply)."""
    if paper:
        return ImageOps.colorize(ImageOps.invert(gray), black=(255, 255, 255), white=color)
    return ImageOps.colorize(gray, black=(0, 0, 0), white=color)


def spiderverse(img, env, rng, paper):
    """Split the (already colour-switched) glyph into jittered, saturated
    silhouettes for a chromatic misregistration, then slice-tear. The whole
    wordmark also eases sideways (GLITCH_MOVE) so the foundation itself moves."""
    if env <= 0:
        return img
    img = ImageChops.offset(img, int(round(GLITCH_MOVE * SCALE * env)), 0)
    gray = img.convert("L")
    ghosts = GHOSTS_PAPER if paper else GHOSTS_DARK
    reach = GLITCH_SPLIT * SCALE * (0.55 + 0.75 * env)
    order = list(range(len(ghosts)))
    rng.shuffle(order)
    out = img
    for gi in order[:3]:                         # three colour ghosts per frame
        dx = int(round(rng.uniform(-reach, reach)))
        dy = int(round(rng.uniform(-reach, reach) * 0.55))
        layer = ImageChops.offset(_silhouette(gray, ghosts[gi], paper), dx, dy)
        if paper:
            out = ImageChops.darker(out, layer)   # colours print under the paper
        else:
            out = ImageChops.lighter(out, layer)  # colours glow over the black
    if GLITCH_TEAR:
        w, h = out.size
        torn = out.copy()
        max_shift = int(round((GLITCH_SPLIT + 3) * SCALE * env)) + 1
        for _ in range(GLITCH_TEAR):
            y = rng.randint(0, h - 1)
            band_h = rng.randint(max(2, h // 36), max(3, h // 10))
            band = out.crop((0, y, w, min(h, y + band_h)))
            torn.paste(ImageChops.offset(band, rng.randint(-max_shift, max_shift), 0), (0, y))
        out = torn
    return out


def new_page(browser, theme):
    ctx = browser.new_context(
        viewport={"width": 900, "height": 260},
        device_scale_factor=SCALE,
        bypass_csp=True,
        reduced_motion="no-preference",  # keep the logo animating (reduced motion parks it)
    )
    ctx.add_init_script(
        "try{localStorage.setItem('cronstable.boot','false');"
        "localStorage.setItem('cronstable.zen','false');"
        f"localStorage.setItem('cronstable.theme','\"{theme}\"');}}catch(e){{}}"
        + INIT_HOOK
    )
    page = ctx.new_page()
    page.goto(f"http://127.0.0.1:{LOGO_PORT}/index.html")
    page.wait_for_selector("#mark svg")
    page.evaluate("document.fonts.ready")  # settle glyph/wordmark metrics
    # The header's bottom border would cross the frame as a stray horizontal
    # line (the clip pads below the mark's swing box, past the header's edge),
    # so it is dropped and the header's own background is extended down over
    # the padding band.
    page.add_style_tag(content=
        "header { border-bottom: none !important;"
        f" padding-bottom: {2 * PAD}px !important; }}")
    return ctx, page


def pick_seed(page):
    """Replay each candidate's choreography through the page's own sim and
    pick the first acceptable performance. Chunked with an early exit: a
    seed's poke schedule is its own, so candidates keep arriving until one
    chunk yields survivors (widening the catch window once before giving up)."""
    seeds = list(SIM_SEEDS)
    all_events = {s: {str(k): v for k, v in events_map(s).items()} for s in seeds}

    def survivors(rs, lo, hi):
        return [r for r in rs
                if not r["fellEarly"] and not r["clamped"] and not r["lost"]
                and lo <= r["catchAt"] <= hi
                and r["endCalm"] < 0.003 and r["endX"] < 0.0012]

    results = []
    for i in range(0, len(seeds), SEARCH_CHUNK):
        chunk = seeds[i:i + SEARCH_CHUNK]
        results += page.evaluate(
            JS_SEARCH,
            [FRAMES, {str(s): all_events[s] for s in chunk}, chunk, DISCONNECT_AT])
        ok = survivors(results, *CATCH_WINDOW)
        print(f"[seed] searched {min(i + SEARCH_CHUNK, len(seeds))}/{len(seeds)}"
              f" ({len(ok)} acceptable)")
        if ok:
            break
    for lo, hi in (CATCH_WINDOW, CATCH_LOOSE):
        ok = survivors(results, lo, hi)
        if ok:
            mid = (CATCH_WINDOW[0] + CATCH_WINDOW[1]) / 2
            best = min(ok, key=lambda r: abs(r["catchAt"] - mid))
            pokes = ", ".join(f"{f * FRAME_MS / 1000:.0f}s:{e['poke']}"
                              for f, e in sorted(events_map(best["seed"]).items())
                              if "poke" in e)
            print(f"[seed] chose {best['seed']}: catch at frame {best['catchAt']} "
                  f"({best['catchAt'] * FRAME_MS / 1000:.1f}s), "
                  f"endCalm {best['endCalm']:.4f}, pokes [{pokes}]")
            return best["seed"]
    raise SystemExit(f"no candidate seed produced a clean loop: {results}")


def capture(browser, base, theme, stops, seed):
    ctx, page = new_page(browser, theme)
    page.evaluate(JS_SETUP, seed)
    # Horizontally the clip spans the wordmark AND the mark's svg (the rail's
    # run-out). Vertically it hugs the wordmark alone, padded the same on both
    # sides, so the resting lockup sits dead-centre in the frame: the svg box
    # reserves more below-rail swing room than the choreography ever uses
    # (scanning every captured frame puts the deepest swing ink ~6 css px
    # under the baseline, inside the wordmark's PAD band), and letting the
    # empty remainder into the frame reads as the logo riding high.
    box = page.evaluate("""() => {
      const rs = ['#mark svg', '#brandName']
        .map((s) => document.querySelector(s).getBoundingClientRect());
      const x = Math.min(...rs.map((r) => r.left));
      const b = rs[1];
      return { x, y: b.top,
               width: Math.max(...rs.map((r) => r.right)) - x,
               height: b.height };
    }""")
    clip = {
        "x": max(0, box["x"] - PAD),
        "y": max(0, box["y"] - PAD),
        "width": box["width"] + 2 * PAD,
        "height": box["height"] + 2 * PAD,
    }

    paper = theme.endswith("-light")   # the ground the colour ghosts blend on
    seq = [theme] + list(stops) + [theme]
    starts = list(HOP_FRAMES)
    inks = {
        c: (None if c == theme else dict(zip(INK_KEYS, THEME_INK[c])))
        for c in set(seq)
    }
    events = events_map(seed)
    stop_frames = sorted(events)       # a freeze may never skip an event

    # frames[] and durs[] run in parallel: a hot (screenshotted) frame arrives
    # as 20 ms and a frozen stretch extends the LAST hot frame's duration, so
    # sum(durs) is always exactly FRAMES * FRAME_MS of true-speed physics.
    frames, durs = [], []
    cap_of = {}                        # sim frame -> index in frames (hot only)
    seg_last = {}                      # ink segment -> its latest hot index
    glitch_reps = []
    calm_run = FREEZE_HOLD             # frame 0 is exact upright: pre-satisfied
    k = 0
    while k < FRAMES:
        in_glitch = any(s <= k < s + GLITCH_LEN for s in starts)
        if k and calm_run >= FREEZE_HOLD and not in_glitch and k not in events:
            nxt = min([f for f in stop_frames if f > k] or [FRAMES])
            page.evaluate(JS_RUN, [nxt - k, FRAME_MS])
            durs[-1] += (nxt - k) * FRAME_MS
            k = nxt
            continue
        seg = sum(1 for s in starts if k >= s)  # colour switches at each hop
        st = page.evaluate(JS_DRIVE, [events.get(k), inks[seq[seg]], FRAME_MS])
        img = Image.open(BytesIO(page.screenshot(clip=clip))).convert("RGB")
        for s in starts:
            if s <= k < s + GLITCH_LEN:
                img = spiderverse(img, glitch_env(k - s, GLITCH_LEN),
                                  random.Random(SEED + k), paper)
                if k == s + GLITCH_LEN // 2:
                    glitch_reps.append(len(frames))
                break
        frames.append(img)
        durs.append(FRAME_MS)
        cap_of[k] = seg_last[seg] = len(frames) - 1
        calm_run = calm_run + 1 if is_calm(st) else 0
        k += 1
    ctx.close()

    # one shared palette (no per-frame requantize -> no palette flicker), no
    # dither (a shifting dither pattern would shimmer between frames). The
    # palette source stacks each ink segment's settled still, the middle of
    # each glitch, AND a sweep through the collapse/swing-up (the amber
    # "recovering" bobs and the accent trail only exist there), so every ink
    # the loop wears gets real palette slots.
    w, h = frames[0].size
    swing_reps = [cap_of[f] for f in range(DISCONNECT_AT + 25, CATCH_WINDOW[1], 90)
                  if f in cap_of]      # post-catch frames may already be frozen
    picks = sorted(set(list(seg_last.values()) + glitch_reps + swing_reps)) or [0]
    src = Image.new("RGB", (w, h * len(picks)))
    for i, fk in enumerate(picks):
        src.paste(frames[fk], (0, i * h))
    pal = src.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    mapped = [f.quantize(palette=pal, dither=Image.Dither.NONE) for f in frames]

    SHOTS.mkdir(exist_ok=True)
    # WebP primary: full 24-bit RGB frames, no palette (the quality win). In
    # lossless mode `quality` is the compression *effort* (100 = smallest, still
    # pixel-perfect); in lossy mode it's fidelity. Per-frame durations carry
    # the frozen stretches.
    webp = SHOTS / f"{base}.webp"
    frames[0].save(
        webp, format="WEBP", save_all=True, append_images=frames[1:],
        duration=durs, loop=0, method=WEBP_METHOD,
        lossless=WEBP_LOSSLESS,
        quality=100 if WEBP_LOSSLESS else WEBP_QUALITY,
    )
    # GIF twin: the 256-colour fallback (centisecond timing: every duration is
    # a multiple of FRAME_MS = 20 ms, so nothing rounds)
    gif = SHOTS / f"{base}.gif"
    mapped[0].save(
        gif, format="GIF", save_all=True, append_images=mapped[1:],
        duration=durs, loop=0, optimize=True,
    )
    seam = max(hi for _, hi in ImageChops.difference(frames[0], frames[-1]).getextrema())
    tour = " -> ".join(seq)
    print(f"[img] {base}: {len(frames)} hot frames / {FRAMES} sim frames "
          f"({sum(durs) / 1000:.1f}s loop), seed {seed}, {len(starts)} hops [{tour}], "
          f"seam max-channel delta {seam} "
          f"-> webp {webp.stat().st_size // 1024} KB, gif {gif.stat().st_size // 1024} KB")


def run_logo():
    """Capture the header brand (self-balancing pendulum + wordmark) as
    seamless loops.

    Unlike the dashboard PNGs this needs NO running daemon: the page is served
    straight off the working tree, its own rAF loop is unhooked, and the mark's
    cart/double-pendulum simulation is stepped frame-by-frame at an exact 50 fps
    (sim dt = 20 ms, the GIF decoder floor), so the recording is deterministic
    and the replay is true-speed physics, not an approximation of it.

    The loop tells the product story with the real controller, not keyframes:

      * the l stands balanced and DEAD STILL: the ambient breeze of the live
        page is switched off, so between events the LQR pins the glyph at exact
        upright and nothing moves at all;
      * the only disturbances are the theme hops: each one lands as before (the
        ink SWITCHES clean to the next theme and holds while the glyph splits
        into a chromatic misregistration plus a digital slice tear), and each
        one physically KNOCKS the pendulum (sim.poke) with a random direction
        and a random magnitude (the direction and the shoulder/elbow split come
        from the sim's own seeded RNG; the magnitude from a per-seed schedule
        drawn here), so every stumble in the loop is different and the
        controller rides each one out;
      * the fourth hop is the big one: it cuts the signal
        (sim.setConnected(false)) and its knock topples the now-dead motor, so
        the l collapses out of the word and swings; seconds later the signal
        returns and the cross-entropy planner threads the swing-up into a
        verified catch; the word heals, exactly as the live dashboard does when
        the daemon drops and comes back;
      * the last hop returns to the base theme and the LQR settles the state to
        (sub-pixel) exact upright, which is also frame 0's state, so the loop
        seam is invisible.

    Stillness is what buys the loop its length: whenever the sim is balanced,
    calm, and the swing trail has drained, the capture stops screenshotting and
    simply EXTENDS the previous frame's on-screen duration while the physics
    steps on underneath (batched, render-free) until the next hop. A quiet
    stretch therefore costs zero frames and zero bytes, which is how a 3-minute
    loop stays the size of the old 40-second one.

    The pendulum is chaotic, so the choreography is SEED-SEARCHED: each
    candidate seed gets its own poke schedule (same generator, seeded by the
    sim seed), and the same event timeline is replayed headlessly (physics
    only, no screenshots) through the page's own `CronstableLogo.Sim`. The
    first seed whose performance survives every knock, keeps clear of the end
    stops, lands the recovery inside CATCH_WINDOW, holds the catch, and
    converges to a sub-pixel-calm final state is used for BOTH variants
    (physics is palette-independent, so the dark and light loops show the
    identical performance).

    Each variant is written twice: a 24-bit `<name>.webp` (the primary — no
    256-palette banding on the glow, the glitch's saturated ghosts, or the
    swing trail) and a 256-colour `<name>.gif` twin for clients that don't
    render animated WebP, the same webp-primary / gif-fallback convention as
    build_reel.py.

    Needs playwright (+ its Chromium) and Pillow. Files land in shots/.
    """
    global Image, ImageChops, ImageOps
    from PIL import Image, ImageChops, ImageOps
    from playwright.sync_api import sync_playwright

    handler = partial(Quiet, directory=str(WEB))
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", LOGO_PORT), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # the physics is palette-independent: search once, replay for both
        ctx, page = new_page(browser, VARIANTS[0][1])
        seed = pick_seed(page)
        ctx.close()
        for base, theme, stops in VARIANTS:
            capture(browser, base, theme, stops, seed)
        browser.close()
    srv.shutdown()


# ===================================================================
#  target: logs (the single-job live-log-tail closeup)
# ===================================================================
LOGS_BASE = "http://127.0.0.1:8899"   # the one-job logs-demo daemon


def run_logs():
    """Capture the single-job live-log-tail closeup from the logs-demo
    daemon."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            viewport={
                "width": 1680,
                "height": 1050,
            },  # match the main capture set
            device_scale_factor=2,
            bypass_csp=True,
            reduced_motion="no-preference",
        )
        route_version(ctx)
        ctx.add_init_script(
            "try{localStorage.setItem('cronstable.boot','false');"
            "localStorage.setItem('cronstable.zen','false');}catch(e){}"
            # park the pendulum mark at exact upright (see PARK_HOOK)
            + PARK_HOOK
        )
        page = ctx.new_page()
        page.goto(LOGS_BASE)
        page.wait_for_function(
            "document.querySelectorAll('#rows tr').length >= 5", timeout=30000
        )
        # start the chatty run, then open its drawer and let lines stream in
        urllib.request.urlopen(
            urllib.request.Request(
                LOGS_BASE + "/jobs/orders-ingest/start", method="POST"
            ),
            timeout=5,
        )
        page.wait_for_timeout(2500)
        row = page.locator("#rows tr", has_text="orders-ingest").first
        row.click()
        page.wait_for_selector("#drawer.open", timeout=5000)
        page.check("#optTs")
        page.wait_for_timeout(9000)  # ~15 colored lines by now, still running
        page.fill("#logSearch", "rows")
        page.wait_for_timeout(700)
        page.screenshot(path=str(SHOTS / "dashboard-logs.png"))
        print("[shot] dashboard-logs (local demo)")
        browser.close()


# ===================================================================
#  the CLI
# ===================================================================
def main(argv=None):
    top = argparse.ArgumentParser(
        prog="capture.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = top.add_subparsers(dest="target", required=True, metavar="target")

    def add(name, runner, help_line, subset=None):
        p = sub.add_parser(
            name,
            help=help_line,
            description=runner.__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        if subset:
            p.add_argument(
                subset,
                nargs="*",
                metavar=subset[:-1],
                help=f"capture only these {subset} (default: every one)",
            )
        return p

    add("dashboard", run_dashboard,
        "web-dashboard stills off the running grand-tour fleet", "shots")
    add("tui", run_tui,
        "terminal-dashboard stills off the same fleet", "shots")
    add("showcase", run_showcase,
        "still frames for the animated reel + theme row", "scenes")
    add("logo", run_logo, "the pendulum-logo loops (needs no daemon)")
    add("logs", run_logs, "the log-tail closeup off the logs-demo daemon")

    args = top.parse_args(argv)
    if args.target == "dashboard":
        run_dashboard(args.shots)
    elif args.target == "tui":
        run_tui(args.shots)
    elif args.target == "showcase":
        run_showcase(args.scenes)
    elif args.target == "logo":
        run_logo()
    else:
        run_logs()


if __name__ == "__main__":
    main()
