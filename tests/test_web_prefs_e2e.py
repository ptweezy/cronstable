"""Test preferences, themes, accessibility settings, and narrow layouts.

Use the dashboard served by ``tests/_web_e2e.py`` to check these behaviors:

* All ten themes use their stylesheet colors in rendered elements, and
  each palette is distinct. Read expected colors from the stylesheet to
  avoid maintaining a second copy.
* Text meets the configured contrast thresholds in every theme and color
  vision mode.
* ``prefers-reduced-motion`` and the app setting stop animations and skip
  the boot screen. The page also responds to operating system changes.
* UI scale, font, density, and color vision settings apply and persist.
* Preferences survive reloads. Invalid stored values fall back to defaults.
* Panels fit the viewport at widths of 390 and 820 pixels.
"""

import json
import re

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


HUES = ["standard", "carolina", "amber", "green", "modern"]
THEMES = HUES + [h + "-light" for h in HUES]
TOKENS = ["--bg", "--panel", "--fg", "--fg-dim", "--accent", "--border"]


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _stylesheet_tokens():
    """Return each theme's token values from its first stylesheet block."""
    css = e2e.INDEX.read_text(encoding="utf-8")
    out = {}
    for match in re.finditer(
        r'html\[data-theme="([a-z-]+)"\]\s*\{([^}]*)\}', css
    ):
        theme, body = match.group(1), match.group(2)
        if theme in out:
            continue
        out[theme] = {
            name: value.strip()
            for name, value in re.findall(r"(--[a-z0-9-]+):\s*([^;]+);", body)
        }
    return out


def _hex_to_rgb(value):
    value = value.lstrip("#")
    if len(value) == 3:
        value = "".join(ch * 2 for ch in value)
    return "rgb({}, {}, {})".format(
        int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)
    )


def _pref(page, key):
    raw = page.evaluate("(k) => localStorage.getItem('cronstable.' + k)", key)
    return None if raw is None else json.loads(raw)


def _open_settings(page):
    _click(page, "#settingsBtn")
    page.wait_for_selector("#settingsWrap.open")


# --------------------------------------------------------------------------
# themes
# --------------------------------------------------------------------------


def test_every_theme_applies_its_tokens_to_the_page(browser, tmp_path):
    sheet = _stylesheet_tokens()
    assert sorted(sheet) == sorted(THEMES)
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            _open_settings(page)
            assert sorted(
                page.evaluate(
                    "[...document.querySelectorAll('#setTheme option')]"
                    ".map((o) => o.value)"
                )
            ) == sorted(THEMES)
            seen = {}
            for theme in THEMES:
                page.select_option("#setTheme", theme)
                assert (
                    page.evaluate(
                        "document.documentElement.getAttribute('data-theme')"
                    )
                    == theme
                )
                got = page.evaluate(
                    "(names) => { const cs = getComputedStyle("
                    "document.documentElement); return Object.fromEntries("
                    "names.map((n) => [n, cs.getPropertyValue(n).trim()])); }",
                    TOKENS,
                )
                for token in TOKENS:
                    assert got[token].lower() == sheet[theme][token].lower(), (
                        theme,
                        token,
                    )
                # Rendered elements use the expected theme colors.
                painted = page.evaluate(
                    "() => ({ body: getComputedStyle(document.body)"
                    ".backgroundColor, ink: getComputedStyle(document.body)"
                    ".color, name: getComputedStyle(document.querySelector("
                    "'#rows .jobname')).color })"
                )
                assert painted["body"] == _hex_to_rgb(sheet[theme]["--bg"])
                assert painted["ink"] == _hex_to_rgb(sheet[theme]["--fg"])
                assert painted["name"] == _hex_to_rgb(sheet[theme]["--fg"])
                seen[theme] = (got["--bg"], got["--accent"], got["--fg"])
                assert _pref(page, "theme") == theme
            assert len(set(seen.values())) == len(THEMES)
            # Light themes use dark text; dark themes use light text.
            for theme, (bg, _, fg) in seen.items():
                light = theme.endswith("-light")
                bg_sum = sum(int(bg[i : i + 2], 16) for i in (1, 3, 5))
                fg_sum = sum(int(fg[i : i + 2], 16) for i in (1, 3, 5))
                assert (bg_sum > fg_sum) == light, theme
            # Reloading restores the saved theme.
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert (
                page.evaluate(
                    "document.documentElement.getAttribute('data-theme')"
                )
                == THEMES[-1]
            )


# WCAG relative luminance contrast of rendered text against the first
# opaque background behind it.
_CONTRAST = """
(selectors) => {
  const parse = (c) => {
    const m = c.match(/rgba?\\(([^)]+)\\)/)[1].split(",").map(parseFloat);
    return { r: m[0], g: m[1], b: m[2], a: m.length > 3 ? m[3] : 1 };
  };
  const lum = (c) => {
    const f = (v) => { v /= 255;
      return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b);
  };
  const backdrop = (el) => {
    for (let e = el; e; e = e.parentElement) {
      const c = parse(getComputedStyle(e).backgroundColor);
      if (c.a >= 0.99) return c;
    }
    return { r: 255, g: 255, b: 255, a: 1 };
  };
  const out = {};
  for (const sel of selectors) {
    const el = document.querySelector(sel);
    if (!el) { out[sel] = null; continue; }
    const ink = parse(getComputedStyle(el).color), bg = backdrop(el);
    const a = lum(ink), b = lum(bg);
    out[sel] = (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
  }
  return out;
}
"""

# Map selectors to the stylesheet's minimum contrast ratios: 7:1 for body
# text, 4.2:1 for secondary text and controls, and 3:1 for status indicators.
# Status indicators also use icons and labels so color is not the only cue.
_CONTRAST_FLOORS = {
    "#rows .jobname": 7.0,
    "#rows .jobcmd": 4.2,
    "#rows .col-last": 4.2,
    "#countLabel": 4.2,
    "#search": 4.2,
    "#rows .btn.run": 4.2,
    "#rows [data-logs]": 4.2,
    "#summary .pill": 4.2,
    '#rows tr[data-job="alpha-ok"] .st .label': 3.0,
    '#rows tr[data-job="beta-fail"] .st .label': 3.0,
    '#rows tr[data-job="epsilon-quiet"] .st .label': 3.0,
    '#rows tr[data-job="delta-off"] .st .label': 3.0,
    "#term .ln.stderr": 4.2,
    "#dMeta": 4.2,
}


def test_text_contrast_in_every_theme_and_vision_mode(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(browser, daemon.url) as page:
            _click(page, '#rows [data-logs="beta-fail"]')
            page.wait_for_selector("#term .ln.stderr")
            # Measure colors after palette transitions finish.
            page.add_style_tag(content="* { transition: none !important; }")
            failures = []
            for theme in THEMES:
                for cvd in ("none", "deutan", "tritan"):
                    page.evaluate(
                        """([theme, cvd]) => {
                          const d = document.documentElement;
                          d.setAttribute('data-theme', theme);
                          if (cvd === 'none') d.removeAttribute('data-cvd');
                          else d.setAttribute('data-cvd', cvd);
                        }""",
                        [theme, cvd],
                    )
                    ratios = page.evaluate(_CONTRAST, list(_CONTRAST_FLOORS))
                    for selector, floor in _CONTRAST_FLOORS.items():
                        ratio = ratios[selector]
                        assert ratio is not None, selector
                        if ratio < floor:
                            failures.append(
                                "{} {} {}: {:.2f} < {}".format(
                                    theme, cvd, selector, ratio, floor
                                )
                            )
            assert not failures, "\n".join(failures)


def test_color_vision_modes_remap_the_status_inks(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("alpha-ok")
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(browser, daemon.url) as page:

            def inks():
                # Success, failure, and pending (a job that has never run).
                return page.evaluate(
                    "() => ['alpha-ok', 'beta-fail', 'epsilon-quiet'].map("
                    "(n) => getComputedStyle(document.querySelector("
                    "'#rows tr[data-job=\"' + n + '\"] .st .glyph')).color)"
                )

            base = inks()
            assert len(set(base)) == 3
            _open_settings(page)
            seen = {"none": base}
            for mode in ("deutan", "tritan"):
                page.select_option("#setCvd", mode)
                assert (
                    page.evaluate(
                        "document.documentElement.getAttribute('data-cvd')"
                    )
                    == mode
                )
                seen[mode] = inks()
                # the three states stay distinguishable in every mode
                assert len(set(seen[mode])) == 3
                assert _pref(page, "cvd") == mode
            # Red-green mode changes the success and failure colors.
            # Blue-yellow mode changes the amber pending color.
            assert seen["deutan"][0] != base[0]
            assert seen["deutan"][1] != base[1]
            assert seen["tritan"][:2] == base[:2]
            assert seen["tritan"][2] != base[2]
            page.select_option("#setCvd", "none")
            assert (
                page.evaluate(
                    "document.documentElement.hasAttribute('data-cvd')"
                )
                is False
            )
            assert inks() == base
            # The palette cycles through the same modes and shows a toast.
            page.keyboard.press("Escape")
            for label in (
                "red-green friendly",
                "blue-yellow friendly",
                "default",
            ):
                page.keyboard.press("Control+k")
                page.wait_for_function(
                    "document.activeElement.id === 'paletteInput'"
                )
                page.keyboard.type("Cycle color vision")
                page.keyboard.press("Enter")
                e2e.wait_toast(page, "color vision: " + label)


# --------------------------------------------------------------------------
# motion
# --------------------------------------------------------------------------

_ANIMATIONS = """
() => ({
  live: getComputedStyle(document.querySelector('#conn .dot'))
    .animationName,
  reduced: document.body.classList.contains('reduce-motion'),
  logoRunning: !!document.querySelector('#mark svg') &&
    document.querySelector('#mark svg').getAttribute('data-mode'),
})
"""


def test_os_reduced_motion_stops_animations_and_skips_boot(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, reduced_motion="no-preference"
        ) as page:
            state = page.evaluate(_ANIMATIONS)
            assert state["reduced"] is False
            assert state["live"] != "none"
            # Change the OS setting while the page is open.
            page.emulate_media(reduced_motion="reduce")
            page.wait_for_function(
                "document.body.classList.contains('reduce-motion')"
            )
            assert page.evaluate(_ANIMATIONS)["live"] == "none"
            _click(page, "#refreshBtn")
            assert (
                page.evaluate(
                    "getComputedStyle(document.querySelector("
                    "'#refreshBtn svg')).animationName"
                )
                == "none"
            )
            page.evaluate("document.getElementById('runFailingBtn').click()")
            page.wait_for_selector("#toasts .toast")
            assert (
                page.evaluate(
                    "getComputedStyle(document.querySelector("
                    "'#toasts .toast')).animationName"
                )
                == "none"
            )
            page.emulate_media(reduced_motion="no-preference")
            page.wait_for_function(
                "!document.body.classList.contains('reduce-motion')"
            )
            assert page.evaluate(_ANIMATIONS)["live"] != "none"
        # Reduced motion skips the boot screen even when it is enabled.
        with e2e.open_page(
            browser,
            daemon.url,
            boot=True,
            reduced_motion="reduce",
            init_scripts=[
                "window.__bootShown = false;"
                "new MutationObserver(() => { const b = document"
                ".getElementById('bootScreen'); if (b && b.style.display "
                "=== 'flex') window.__bootShown = true; })"
                ".observe(document, { subtree: true, attributes: true, "
                "childList: true });"
            ],
        ) as page:
            assert page.evaluate("window.__bootShown") is False
            assert page.evaluate(_ANIMATIONS)["reduced"] is True


def test_reduce_motion_setting_matches_the_os_switch(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser, daemon.url, reduced_motion="no-preference"
        ) as page:
            assert page.evaluate(_ANIMATIONS)["live"] != "none"
            _open_settings(page)
            page.evaluate("document.getElementById('setMotion').click()")
            page.wait_for_function(
                "document.body.classList.contains('reduce-motion')"
            )
            assert page.evaluate(_ANIMATIONS)["live"] == "none"
            assert _pref(page, "motion") is True
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert page.evaluate(_ANIMATIONS)["reduced"] is True
            # Disabling the OS preference does not override the app setting.
            page.emulate_media(reduced_motion="no-preference")
            assert page.evaluate(_ANIMATIONS)["reduced"] is True
            _open_settings(page)
            assert page.is_checked("#setMotion")
            page.evaluate("document.getElementById('setMotion').click()")
            page.wait_for_function(
                "!document.body.classList.contains('reduce-motion')"
            )


def test_boot_screen_runs_once_per_cooldown_and_can_be_disabled(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            boot=True,
            wait_rows=False,
            reduced_motion="no-preference",
        ) as page:
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'flex'"
            )
            page.wait_for_function(
                "document.getElementById('bootLog').textContent"
                ".includes('READY.')"
            )
            log = page.inner_text("#bootLog")
            for label in (
                "CONNECTING TO SERVER",
                "FIRMWARE VERSION",
                "JOB SET",
                "CLUSTER",
                "SCANNING SCHEDULES",
            ):
                assert label in log
            assert "[5 jobs]" in log and "[5 scheduled]" in log
            assert "standalone (no cluster)" in log
            page.wait_for_selector("#rows tr[data-job]")
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'none'"
            )
            # inside the cooldown the next load starts the app directly
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert (
                page.evaluate(
                    "document.getElementById('bootScreen').style.display"
                )
                == "none"
            )
            # Enabling the setting again clears the cooldown timestamp.
            _open_settings(page)
            assert page.is_checked("#setBoot")
            page.evaluate("document.getElementById('setBoot').click()")
            assert _pref(page, "boot") is False
            page.evaluate("document.getElementById('setBoot').click()")
            assert (
                page.evaluate("localStorage.getItem('cronstable.bootShownAt')")
                is None
            )
            page.reload()
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'flex'"
            )
            page.mouse.click(400, 400)  # a click skips it
            page.wait_for_selector("#rows tr[data-job]")


@pytest.mark.parametrize(
    "fault,text",
    [
        ("abort", "FAIL — server unreachable"),
        ("500", "FAIL (HTTP 500)"),
    ],
)
def test_boot_screen_halts_on_failure_and_still_starts(
    browser, tmp_path, fault, text
):
    with e2e.Daemon(tmp_path) as daemon:

        def routes(page):
            if fault == "abort":
                page.faults.abort(r"/version", times=1)
            else:
                page.faults.status(r"/version", 500, times=1)

        with e2e.open_page(
            browser,
            daemon.url,
            boot=True,
            before_goto=routes,
            wait_rows=False,
            reduced_motion="no-preference",
        ) as page:
            page.wait_for_function(
                "document.getElementById('bootLog').textContent"
                ".includes('HALTED')"
            )
            log = page.inner_text("#bootLog")
            assert text in log
            assert "continuing without a server connection" in log
            assert "press any key to continue" in log
            # The boot screen error does not prevent the app from starting.
            page.wait_for_selector("#rows tr[data-job]")


def test_boot_screen_unauthorized_hands_over_to_the_token_modal(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            boot=True,
            wait_rows=False,
            reduced_motion="no-preference",
        ) as page:
            page.wait_for_function(
                "document.getElementById('bootLog').textContent"
                ".includes('UNAUTHORIZED')"
            )
            assert "enter an access token to continue" in page.inner_text(
                "#bootLog"
            )
            page.wait_for_selector("#modalWrap.open")
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'none'"
            )
            page.fill("#tokenInput", e2e.FULL_TOKEN)
            page.press("#tokenInput", "Enter")
            page.wait_for_selector("#rows tr[data-job]")


# --------------------------------------------------------------------------
# scale, font, density
# --------------------------------------------------------------------------


def test_scale_font_and_density_apply_and_persist(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            root = "document.documentElement"
            assert page.evaluate(root + ".style.zoom") == ""
            assert page.evaluate(root + ".getAttribute('data-font')") == (
                "mono"
            )
            mono = page.evaluate("getComputedStyle(document.body).fontFamily")
            row_height = page.evaluate(
                "document.querySelector('#rows tr').getBoundingClientRect()"
                ".height"
            )
            _open_settings(page)
            assert page.evaluate(
                "[...document.querySelectorAll('#setScale option')]"
                ".map((o) => o.value)"
            ) == ["100", "110", "125", "140"]
            for value in ("110", "125", "140"):
                page.select_option("#setScale", value)
                assert page.evaluate(root + ".style.zoom") == value + "%"
                assert _pref(page, "scale") == int(value)
            # Increasing the scale makes the rendered row taller.
            assert (
                page.evaluate(
                    "document.querySelector('#rows tr')"
                    ".getBoundingClientRect().height"
                )
                > row_height * 1.2
            )
            page.select_option("#setScale", "100")
            assert page.evaluate(root + ".style.zoom") == ""

            page.select_option("#setFont", "sans")
            assert page.evaluate(root + ".getAttribute('data-font')") == (
                "sans"
            )
            assert (
                page.evaluate("getComputedStyle(document.body).fontFamily")
                != mono
            )
            page.evaluate("document.getElementById('setDensity').click()")
            assert page.evaluate(
                "document.body.classList.contains('density-compact')"
            )
            assert (
                page.evaluate(
                    "document.querySelector('#rows tr')"
                    ".getBoundingClientRect().height"
                )
                < row_height
            )
            page.select_option("#setScale", "125")

            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert page.evaluate(root + ".style.zoom") == "125%"
            assert page.evaluate(root + ".getAttribute('data-font')") == (
                "sans"
            )
            assert page.evaluate(
                "document.body.classList.contains('density-compact')"
            )
            _open_settings(page)
            assert page.input_value("#setScale") == "125"
            assert page.input_value("#setFont") == "sans"
            assert page.is_checked("#setDensity")
            page.keyboard.press("Escape")
            # the palette cycles the scale and wraps
            for expect in ("140", "100", "110"):
                page.keyboard.press("Control+k")
                page.wait_for_function(
                    "document.activeElement.id === 'paletteInput'"
                )
                page.keyboard.type("Cycle UI scale")
                page.keyboard.press("Enter")
                e2e.wait_toast(page, "UI scale: {}%".format(expect))


# --------------------------------------------------------------------------
# persistence and corrupt storage
# --------------------------------------------------------------------------


def test_panel_and_log_preferences_persist(browser, tmp_path):
    with e2e.Daemon(tmp_path, state=True) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            for button, key in (
                ("#radarBtn", "radar"),
                ("#weekBtn", "week"),
                ("#heatBtn", "heat"),
                ("#pressBtn", "press"),
            ):
                _click(page, button)
                assert _pref(page, key) is True
            page.wait_for_function(
                "document.getElementById('stateBtn').style.display === ''"
            )
            _click(page, "#stateBtn")
            assert _pref(page, "stateInsp") is True
            page.select_option("#heatWindow", "7d")
            page.select_option("#pressTz", "browser")
            _click(page, '#rows [data-logs="alpha-ok"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            page.uncheck("#optWrap")
            page.check("#optTs")
            page.keyboard.press("Escape")

            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            for panel in (
                "radarPanel",
                "weekCard",
                "heatCard",
                "pressureCard",
            ):
                page.wait_for_function(
                    "(id) => getComputedStyle(document.getElementById(id))"
                    ".display !== 'none'",
                    arg=panel,
                )
            page.wait_for_function(
                "getComputedStyle(document.getElementById('stateCard'))"
                ".display !== 'none'"
            )
            for button in ("#radarBtn", "#weekBtn", "#heatBtn", "#pressBtn"):
                assert page.evaluate(
                    "(s) => document.querySelector(s).classList"
                    ".contains('on')",
                    button,
                )
            assert page.input_value("#heatWindow") == "7d"
            assert page.input_value("#pressTz") == "browser"
            assert not page.is_checked("#optWrap")
            assert page.is_checked("#optTs")
            # Store the token per tab, separately from persistent preferences.
            assert (
                page.evaluate(
                    "Object.keys(localStorage).filter((k) => /token/i.test(k))"
                )
                == []
            )


_CORRUPT = """
const bad = {
  theme: '{not json', density: '"yes"', font: '42', scale: '"huge"',
  motion: '{}', cvd: '[1,2]', pollMs: '"soon"', follow: 'null',
  wrap: '"no"', ts: '7', ansi: '[]', volume: '"loud"', heatWindow: '99',
  pressTz: 'false', resMs: '"fast"', zenIdle: 'null', cols: '"all"',
  radar: '"on"', heat: '1', week: '{}', press: '[]', ledger: '"x"',
  dags: '0', stateInsp: '"1"', boot: '"never"', notify: '"always"',
  sound: '3', sbxRecent: '{"not":"a list"}', bootShownAt: 'yesterday',
};
for (const k of Object.keys(bad))
  localStorage.setItem('cronstable.' + k, bad[k]);
"""


def test_corrupt_stored_preferences_fall_back_to_defaults(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            init_scripts=[_CORRUPT],
            reduced_motion="no-preference",
        ) as page:
            page.faults.record()
            root = "document.documentElement"
            assert page.evaluate(root + ".getAttribute('data-theme')") == (
                "standard"
            )
            assert page.evaluate(root + ".getAttribute('data-font')") == (
                "mono"
            )
            assert page.evaluate(root + ".style.zoom") == ""
            assert page.evaluate(root + ".hasAttribute('data-cvd')") is False
            assert not page.evaluate(
                "document.body.classList.contains('reduce-motion')"
            )
            assert not page.evaluate(
                "document.body.classList.contains('density-compact')"
            )
            # Default columns are visible, and polling remains active.
            assert page.evaluate(
                "document.getElementById('jobsTable').classList"
                ".contains('show-sched')"
            )
            before = len(page.faults.sent("GET", "/jobs"))
            e2e.wait_until(
                lambda: len(page.faults.sent("GET", "/jobs")) >= before + 2,
                page,
            )
            _open_settings(page)
            assert page.input_value("#setTheme") == "standard"
            assert page.input_value("#setPoll") == "3000"
            assert page.input_value("#setScale") == "100"
            assert not page.is_checked("#setDensity")
            page.keyboard.press("Escape")
            # every surface that reads a stored value still opens
            _click(page, '#rows [data-logs="alpha-ok"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.is_checked("#optAnsi")
            assert page.is_checked("#optWrap")
            page.keyboard.press("Escape")
            page.keyboard.press("Control+k")
            page.wait_for_function(
                "document.activeElement.id === 'paletteInput'"
            )
            page.keyboard.type("Schedule preview")
            page.keyboard.press("Enter")
            page.wait_for_selector("#sandboxWrap.open")
            page.fill("#sbxInput", "*/5 * * * *")
            page.wait_for_selector("#sbxBody .nextruns li")
            page.keyboard.press("Escape")
            _click(page, "#colsBtn")
            page.wait_for_selector("#colsMenu.open")
            page.check('#colsMenu input[data-col="tz"]')
            assert _pref(page, "cols") == {"tz": True}


def test_storage_that_refuses_writes_does_not_break_the_page(
    browser, tmp_path
):
    """Apply settings for the current tab even if localStorage writes fail."""
    script = (
        "Storage.prototype.setItem = function () { "
        "throw new DOMException('quota', 'QuotaExceededError'); };"
    )
    with e2e.Daemon(tmp_path) as daemon:
        context = browser.new_context()
        try:
            context.add_init_script(script)
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(daemon.url)
            # The boot screen's cooldown timestamp also cannot be stored.
            page.keyboard.press("Escape")
            page.wait_for_selector("#rows tr[data-job]")
            page.evaluate("document.activeElement.blur()")
            page.keyboard.press("t")
            assert (
                page.evaluate(
                    "document.documentElement.getAttribute('data-theme')"
                )
                == "carolina"
            )
            assert not errors, errors
        finally:
            context.close()


# --------------------------------------------------------------------------
# narrow viewports
# --------------------------------------------------------------------------

_OVERFLOW = """
(surface) => {
  const vw = document.documentElement.clientWidth;
  const out = { page: document.documentElement.scrollWidth - vw };
  if (surface) {
    const el = document.querySelector(surface);
    const box = el.getBoundingClientRect();
    out.left = box.left; out.right = box.right - vw;
    // Find elements that overflow horizontally without a scroll container.
    out.offenders = [...el.querySelectorAll('*')].filter((e) => {
      const b = e.getBoundingClientRect();
      if (!b.width || !b.height) return false;
      if (b.right <= vw + 1 && b.left >= -1) return false;
      for (let p = e.parentElement; p && p !== el.parentElement;
           p = p.parentElement) {
        const o = getComputedStyle(p).overflowX;
        if (o === 'auto' || o === 'scroll' || o === 'hidden') return false;
      }
      return true;
    }).slice(0, 5).map((e) => e.tagName + '#' + e.id + '.' + e.className);
  }
  return out;
}
"""


def _assert_fits(page, where, surface=None):
    got = page.evaluate(_OVERFLOW, surface)
    assert got["page"] <= 1, "{}: page scrolls sideways by {}px".format(
        where, got["page"]
    )
    if surface:
        assert got["left"] >= -1 and got["right"] <= 1, (where, got)
        assert not got["offenders"], (where, got["offenders"])


@pytest.mark.parametrize("width,height", [(390, 844), (820, 1180)])
def test_no_sideways_overflow_on_narrow_viewports(
    browser, tmp_path, width, height
):
    jobs = e2e.default_jobs() + [
        e2e.job(
            "a-very-long-job-name-that-keeps-going-" + "x" * 60,
            "echo " + "y" * 300,
        )
    ]
    with e2e.Daemon(
        tmp_path, jobs=jobs, dags=[e2e.diamond_dag("diamond")]
    ) as daemon:
        daemon.run_and_wait("beta-fail")
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={
                "radar": True,
                "week": True,
                "heat": True,
                "press": True,
                "stateInsp": True,
            },
            viewport={"width": width, "height": height},
            is_mobile=width < 500,
            has_touch=width < 500,
        ) as page:
            page.wait_for_selector("#dagRows tr[data-dag]")
            _assert_fits(page, "main")
            # the narrow table keeps its essential columns only
            assert not page.is_visible("#rows td.col-sched")
            assert page.is_visible("#rows .jobname")
            assert page.is_visible("#rows [data-logs]")

            _click(page, '#rows [data-logs="beta-fail"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            page.wait_for_function(
                "document.getElementById('drawer').getBoundingClientRect()"
                ".right <= document.documentElement.clientWidth + 1"
            )
            for tab in ("logs", "history", "resources", "schedule"):
                _click(page, '#dTabs button[data-tab="{}"]'.format(tab))
                page.wait_for_selector(
                    '.pane.active[data-pane="{}"]'.format(tab)
                )
                _assert_fits(page, "drawer " + tab, "#drawer")
            _click(page, "#dClose")
            page.wait_for_selector('#drawer[aria-hidden="true"]')

            _click(page, '#dagRows [data-dagopen="diamond"]')
            page.wait_for_selector("#dgRuns tr.dagrun")
            page.wait_for_function(
                "document.getElementById('dagDrawer')"
                ".getBoundingClientRect().right <= "
                "document.documentElement.clientWidth + 1"
            )
            for tab in ("runs", "tasks", "graph", "xcom", "logs"):
                _click(page, '#dagTabs button[data-dtab="{}"]'.format(tab))
                _assert_fits(page, "dag drawer " + tab, "#dagDrawer")
            _click(page, "#dgClose")
            page.wait_for_selector('#dagDrawer[aria-hidden="true"]')

            for opener, surface in (
                ("#authBtn", "#modalWrap .modal"),
                ("#paletteBtn", "#paletteWrap .palette"),
                ("#settingsBtn", "#settingsWrap .sheet"),
                ("#tailBtn", "#tailWrap .tailpanel"),
            ):
                _click(page, opener)
                page.wait_for_selector(surface.split(" ")[0] + ".open")
                _assert_fits(page, surface, surface)
                page.keyboard.press("Escape")
                page.wait_for_function(
                    "(s) => !document.querySelector(s).classList"
                    ".contains('open')",
                    arg=surface.split(" ")[0],
                )
            _click(page, "#verdictBar")
            page.wait_for_selector("#timelineWrap.open")
            _assert_fits(page, "timeline", "#timelineWrap .tlpanel")
            page.keyboard.press("Escape")

            _click(page, "#tvBtn")
            page.wait_for_selector("#wbGrid .wb-tile")
            _assert_fits(page, "wallboard", "#wallboard")
