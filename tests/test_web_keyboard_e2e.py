"""Test keyboard shortcuts, overlays, focus, and the command palette.

The dashboard uses the daemon in ``tests/_web_e2e.py`` and receives browser
key events. Tests check these behaviors:

* Each shortcut in the help dialog performs its documented action. Adding
  a shortcut to the dialog requires a corresponding test.
* Modifier combinations and typing in fields retain browser behavior.
* Escape closes stacked panels one at a time, starting with the top panel.
* Tab stays within the top panel. Closed overlays leave the tab order, and
  closing a panel restores the previous focus.
* The command palette filters, ranks, selects, and runs entries.
* Dialogs have the expected roles, and drawers expose ``aria-hidden``.
"""

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


OVERLAYS = [
    "tailWrap",
    "paletteWrap",
    "settingsWrap",
    "helpWrap",
    "pairWrap",
    "modalWrap",
    "timelineWrap",
    "mitigateWrap",
    "sandboxWrap",
]


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _open_ids(page):
    """Ids of every open overlay and drawer."""
    return page.evaluate(
        "[...document.querySelectorAll('.overlay.open, .drawer.open')]"
        ".map((e) => e.id)"
    )


def _wait_open(page, surface_id, is_open=True):
    page.wait_for_function(
        "([id, open]) => document.getElementById(id).classList"
        ".contains('open') === open",
        arg=[surface_id, is_open],
    )


def _active(page):
    return page.evaluate("document.activeElement.id")


def _wait_active(page, element_id):
    page.wait_for_function(
        "(id) => document.activeElement.id === id", arg=element_id
    )


def _palette_run(page, label):
    """Run one palette entry by its exact label."""
    page.keyboard.press("Control+k")
    _wait_active(page, "paletteInput")
    page.keyboard.type(label)
    page.wait_for_function(
        "(label) => (document.querySelector('#paletteList .item.cur .lbl')"
        " || {textContent: ''}).textContent.startsWith(label)",
        arg=label,
    )
    page.keyboard.press("Enter")
    _wait_open(page, "paletteWrap", False)


def _theme(page):
    return page.evaluate("document.documentElement.getAttribute('data-theme')")


def _selected(page):
    return page.evaluate(
        "(document.querySelector('#rows tr.sel') || {getAttribute: () => "
        "null}).getAttribute('data-job')"
    )


# --------------------------------------------------------------------------
# the help table
# --------------------------------------------------------------------------

_HELP_KEYS = [
    "⌘K / Ctrl+K",
    "/",
    "j / ↓",
    "k / ↑",
    "Enter",
    "r",
    "x",
    "p",
    "c",
    "g",
    "t",
    "T",
    "i",
    "w",
    "a",
    "?",
    "Esc",
]


def test_every_shortcut_in_the_help_sheet(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 0},
            permissions=["clipboard-read", "clipboard-write"],
        ) as page:
            page.faults.record()
            # ? opens the sheet; the sheet lists exactly the keys below
            page.keyboard.press("?")
            _wait_open(page, "helpWrap")
            listed = page.evaluate(
                "[...document.querySelectorAll('#helpGrid .keys')]"
                ".map((k) => k.textContent)"
            )
            assert listed == _HELP_KEYS
            page.keyboard.press("Escape")
            _wait_open(page, "helpWrap", False)

            # Ctrl+K, Cmd+K and Ctrl+P open the palette
            for chord in ("Control+k", "Meta+k", "Control+p", "Control+K"):
                page.keyboard.press(chord)
                _wait_open(page, "paletteWrap")
                page.keyboard.press("Escape")
                _wait_open(page, "paletteWrap", False)

            # / focuses the filter; Escape there closes nothing, and the
            # field keeps the keys
            page.keyboard.press("/")
            _wait_active(page, "search")
            assert page.input_value("#search") == ""
            page.evaluate("document.activeElement.blur()")

            # j/k and the arrows move the row cursor
            names = e2e.row_names(page)
            page.keyboard.press("j")
            assert _selected(page) == names[0]
            page.keyboard.press("ArrowDown")
            assert _selected(page) == names[1]
            page.keyboard.press("k")
            assert _selected(page) == names[0]
            page.keyboard.press("j")
            page.keyboard.press("j")
            page.keyboard.press("ArrowUp")
            assert _selected(page) == names[1]
            # with no cursor yet, k starts from the last row
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            page.faults.requests.clear()
            page.keyboard.press("k")
            assert _selected(page) == names[-1]

            # c copies the selected command
            page.keyboard.press("c")
            e2e.wait_toast(page, "copied command")
            assert "time.sleep" in page.evaluate(
                "navigator.clipboard.readText()"
            )

            # Enter opens the selected job, Escape closes it
            page.keyboard.press("Enter")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.inner_text("#dName") == names[-1]
            assert page.evaluate("location.hash") == "#job/" + names[-1]
            page.keyboard.press("Escape")
            page.wait_for_selector('#drawer[aria-hidden="true"]')
            assert page.evaluate("location.hash") == ""

            # r, p, p, (running) x on the selected job
            page.keyboard.press("r")
            e2e.wait_toast(page, "started gamma-slow")
            page.keyboard.press("g")
            e2e.wait_row_status(page, "gamma-slow", "Running")
            page.keyboard.press("x")
            e2e.wait_toast(page, "cancelled gamma-slow")
            page.keyboard.press("p")
            e2e.wait_toast(page, "paused gamma-slow")
            page.keyboard.press("g")
            e2e.wait_row_status(page, "gamma-slow", "Paused")
            page.keyboard.press("p")
            e2e.wait_toast(page, "resumed gamma-slow")

            # g refreshes now (polling is off, so each /jobs is a key press)
            before = len(page.faults.sent("GET", "/jobs"))
            page.keyboard.press("g")
            e2e.wait_until(
                lambda: len(page.faults.sent("GET", "/jobs")) == before + 1,
                page,
            )

            # t walks the five hues; T flips light and dark within a hue
            assert _theme(page) == "standard"
            seen = []
            for _ in range(5):
                page.keyboard.press("t")
                seen.append(_theme(page))
            assert seen == ["carolina", "amber", "green", "modern", "standard"]
            page.keyboard.press("Shift+T")
            assert _theme(page) == "standard-light"
            page.keyboard.press("t")
            assert _theme(page) == "carolina-light"
            page.keyboard.press("Shift+T")
            assert _theme(page) == "carolina"
            assert (
                page.evaluate("localStorage.getItem('cronstable.theme')")
                == '"carolina"'
            )

            # i opens the incident timeline
            page.keyboard.press("i")
            _wait_open(page, "timelineWrap")
            assert "beta-fail" in page.inner_text("#tlBody")
            page.keyboard.press("Escape")
            _wait_open(page, "timelineWrap", False)

            # a acknowledges the standing alarm (a job is failing)
            page.keyboard.press("a")
            e2e.wait_toast(page, "alarm acknowledged")

            # w toggles the wallboard; w and Escape both leave it
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")
            assert page.evaluate("location.hash") == "#tv"
            page.keyboard.press("w")
            page.wait_for_function("!document.body.classList.contains('tv')")
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")
            # the list shortcuts are inert on the wallboard
            page.keyboard.press("j")
            page.keyboard.press("Enter")
            assert page.get_attribute("#drawer", "aria-hidden") == "true"
            page.keyboard.press("Escape")
            page.wait_for_function("!document.body.classList.contains('tv')")
            assert page.evaluate("location.hash") == ""


def test_modifier_chords_are_left_to_the_browser(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.faults.record()
            page.keyboard.press("j")
            selected = _selected(page)
            theme = _theme(page)
            for modifier in ("Control", "Meta", "Alt"):
                for key in "jkrxpcgtiwa/":
                    if modifier in ("Control", "Meta") and key in "kp":
                        continue  # the palette chords
                    page.keyboard.press(modifier + "+" + key)
            page.keyboard.press("Control+Enter")
            assert _selected(page) == selected
            assert _theme(page) == theme
            assert _open_ids(page) == []
            assert not page.evaluate("document.body.classList.contains('tv')")
            assert e2e.toasts(page) == []
            assert [
                r for r in page.faults.requests if r["method"] != "GET"
            ] == []
            assert page.faults.sent("GET", "/jobs") == []
            assert _active(page) != "search"


@pytest.mark.parametrize(
    "field", ["#search", "#sortSel", "#logSearch", "#tokenInput"]
)
def test_keys_typed_into_a_field_are_not_shortcuts(browser, tmp_path, field):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.faults.record()
            if field == "#logSearch":
                _click(page, '#rows [data-logs="alpha-ok"]')
                page.wait_for_selector('#drawer[aria-hidden="false"]')
            if field == "#tokenInput":
                _click(page, "#authBtn")
                _wait_open(page, "modalWrap")
            page.focus(field)
            surfaces = _open_ids(page)
            theme = _theme(page)
            for key in "jkrxpcgtiwa?":
                page.keyboard.press(key)
            assert _theme(page) == theme
            assert _open_ids(page) == surfaces
            assert not page.evaluate("document.body.classList.contains('tv')")
            assert [
                r for r in page.faults.requests if r["method"] != "GET"
            ] == []
            if field in ("#search", "#logSearch", "#tokenInput"):
                assert page.input_value(field) == "jkrxpcgtiwa?"


def test_boot_screen_swallows_keys_until_it_is_skipped(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        parked = []

        def routes(page):
            parked.extend([page.faults.hang(r"/version")])

        with e2e.open_page(
            browser,
            daemon.url,
            boot=True,
            before_goto=routes,
            wait_rows=False,
        ) as page:
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'flex'"
            )
            assert page.get_attribute("#bootScreen", "aria-hidden") == "false"
            # the first key skips the self-test and is not a shortcut
            page.keyboard.press("w")
            page.wait_for_selector("#rows tr[data-job]")
            assert not page.evaluate("document.body.classList.contains('tv')")
            page.wait_for_function(
                "document.getElementById('bootScreen').style.display === "
                "'none'"
            )
            assert page.get_attribute("#bootScreen", "aria-hidden") == "true"
            # from here keys are shortcuts again
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")
            for route in parked[0]:
                route.fallback()
            page.faults.clear(r"/version")


# --------------------------------------------------------------------------
# Escape ordering
# --------------------------------------------------------------------------


def test_escape_closes_stacked_surfaces_topmost_first(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.run_and_wait("beta-fail")
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            _click(page, '#rows [data-logs="alpha-ok"]')
            _wait_open(page, "drawer")
            _palette_run(page, "Live logs")
            _wait_open(page, "tailWrap")
            _palette_run(page, "Incident timeline")
            _wait_open(page, "timelineWrap")
            _palette_run(page, "Schedule preview")
            _wait_open(page, "sandboxWrap")
            _palette_run(page, "Review actions for failing jobs")
            _wait_open(page, "mitigateWrap")
            _palette_run(page, "Pair a device")
            _wait_open(page, "pairWrap")
            _palette_run(page, "Keyboard shortcuts")
            _wait_open(page, "helpWrap")
            _palette_run(page, "Open settings")
            _wait_open(page, "settingsWrap")
            _palette_run(page, "Set access token")
            _wait_open(page, "modalWrap")
            page.keyboard.press("Control+k")
            _wait_open(page, "paletteWrap")
            expected = [
                "paletteWrap",
                "modalWrap",
                "settingsWrap",
                "helpWrap",
                "pairWrap",
                "mitigateWrap",
                "sandboxWrap",
                "timelineWrap",
                "tailWrap",
                "drawer",
            ]
            assert sorted(_open_ids(page)) == sorted(expected)
            for index, surface in enumerate(expected):
                page.keyboard.press("Escape")
                _wait_open(page, surface, False)
                assert sorted(_open_ids(page)) == sorted(
                    expected[index + 1 :]
                ), surface
            # one more Escape with nothing open is harmless
            page.keyboard.press("Escape")
            assert _open_ids(page) == []
            assert page.get_attribute("#drawer", "aria-hidden") == "true"


def test_escape_order_with_columns_menu_dag_drawer_and_wallboard(
    browser, tmp_path
):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.wait_for_selector("#dagRows tr[data-dag]")
            _click(page, '#dagRows [data-dagopen="diamond"]')
            _wait_open(page, "dagDrawer")
            assert page.get_attribute("#dagDrawer", "aria-hidden") == "false"
            assert page.evaluate("location.hash") == "#dag/diamond"
            # the two drawers exclude each other
            assert page.get_attribute("#drawer", "aria-hidden") == "true"
            page.evaluate("document.getElementById('colsBtn').click()")
            page.wait_for_selector("#colsMenu.open")
            page.keyboard.press("Escape")
            page.wait_for_function(
                "!document.getElementById('colsMenu').classList"
                ".contains('open')"
            )
            assert _open_ids(page) == ["dagDrawer"]
            page.keyboard.press("Escape")
            _wait_open(page, "dagDrawer", False)
            assert page.get_attribute("#dagDrawer", "aria-hidden") == "true"
            assert page.evaluate("location.hash") == ""

            # the palette opens over the wallboard; Escape closes it first
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")
            page.keyboard.press("Control+k")
            _wait_open(page, "paletteWrap")
            page.keyboard.press("Escape")
            _wait_open(page, "paletteWrap", False)
            assert page.evaluate("document.body.classList.contains('tv')")
            page.keyboard.press("Escape")
            page.wait_for_function("!document.body.classList.contains('tv')")

            # opening a job drawer closes the DAG drawer, and the reverse
            _click(page, '#dagRows [data-dagopen="diamond"]')
            _wait_open(page, "dagDrawer")
            page.evaluate("location.hash = '#job/alpha-ok'")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.get_attribute("#dagDrawer", "aria-hidden") == "true"
            _click(page, '#dagRows [data-dagopen="diamond"]')
            _wait_open(page, "dagDrawer")
            assert page.get_attribute("#drawer", "aria-hidden") == "true"


def test_backdrop_click_closes_only_its_own_surface(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            for opener, surface in (
                ("#settingsBtn", "settingsWrap"),
                ("#authBtn", "modalWrap"),
                ("#paletteBtn", "paletteWrap"),
                ("#tailBtn", "tailWrap"),
            ):
                _click(page, opener)
                _wait_open(page, surface)
                # a click on the panel itself keeps it open
                page.click(
                    "#{} [role=dialog]".format(surface),
                    position={"x": 5, "y": 5},
                )
                assert surface in _open_ids(page)
                page.mouse.click(3, 3)
                _wait_open(page, surface, False)
            _click(page, '#rows [data-logs="alpha-ok"]')
            _wait_open(page, "drawer")
            page.mouse.click(30, 500)  # the scrim beside the drawer
            _wait_open(page, "drawer", False)


# --------------------------------------------------------------------------
# focus: trap, restore, tab order
# --------------------------------------------------------------------------


def _wait_focus_inside(page, surface_id):
    page.wait_for_function(
        "(id) => document.getElementById(id)"
        ".contains(document.activeElement)",
        arg=surface_id,
    )


def _tab_walk(page, presses, shift=False):
    """Press Tab ``presses`` times; the container id of each stop."""
    stops = []
    for _ in range(presses):
        page.keyboard.press("Shift+Tab" if shift else "Tab")
        stops.append(
            page.evaluate(
                """() => {
                  const a = document.activeElement;
                  const host = a.closest('.overlay, .drawer');
                  return [host ? host.id : null, a.id || a.tagName];
                }"""
            )
        )
    return stops


@pytest.mark.parametrize(
    "opener,surface",
    [
        ("#authBtn", "modalWrap"),
        ("#settingsBtn", "settingsWrap"),
        ("#paletteBtn", "paletteWrap"),
        ("#tailBtn", "tailWrap"),
        ('#rows [data-logs="alpha-ok"]', "drawer"),
        ('#dagRows [data-dagopen="diamond"]', "dagDrawer"),
    ],
)
def test_tab_stays_inside_the_open_surface(browser, tmp_path, opener, surface):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        with e2e.open_page(
            browser, daemon.url, token="any", prefs={"pollMs": 0}
        ) as page:
            page.wait_for_selector("#dagRows tr[data-dag]")
            _click(page, opener)
            _wait_open(page, surface)
            page.wait_for_function(
                "(id) => document.getElementById(id)"
                ".contains(document.activeElement)",
                arg=surface,
            )
            forward = _tab_walk(page, 25)
            assert {host for host, _ in forward} == {surface}, forward
            backward = _tab_walk(page, 25, shift=True)
            assert {host for host, _ in backward} == {surface}, backward
            # more than one stop means Tab really cycles, not just sticks
            if surface != "paletteWrap":
                assert len({stop for _, stop in forward}) > 1
            # focus dragged outside comes back in on the next Tab
            page.evaluate("document.getElementById('search').focus()")
            assert _tab_walk(page, 1)[0][0] == surface


def test_tab_in_the_stack_belongs_to_the_topmost_surface(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            _click(page, '#rows [data-logs="alpha-ok"]')
            _wait_open(page, "drawer")
            _palette_run(page, "Open settings")
            _wait_open(page, "settingsWrap")
            # the panel takes focus a moment after it opens; a Tab pressed
            # before that starts from the page body
            _wait_focus_inside(page, "settingsWrap")
            assert {h for h, _ in _tab_walk(page, 20)} == {"settingsWrap"}
            page.keyboard.press("Escape")
            _wait_open(page, "settingsWrap", False)
            assert {h for h, _ in _tab_walk(page, 20)} == {"drawer"}
            # the help sheet has no controls: Tab holds focus on the sheet
            _palette_run(page, "Keyboard shortcuts")
            _wait_open(page, "helpWrap")
            page.wait_for_function(
                "document.activeElement === "
                "document.getElementById('helpWrap')"
            )
            assert _tab_walk(page, 3) == [["helpWrap", "helpWrap"]] * 3


def test_closed_surfaces_stay_out_of_the_tab_order(browser, tmp_path):
    """With nothing open, Tab walks the page's own controls only; every
    stop is a visible element outside the overlays and drawers."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            stops = []
            for _ in range(80):
                page.keyboard.press("Tab")
                stops.append(
                    page.evaluate(
                        """() => {
                          const a = document.activeElement;
                          const host = a.closest('.overlay, .drawer');
                          const style = getComputedStyle(a);
                          const box = a.getBoundingClientRect();
                          return {
                            id: a.id || a.tagName,
                            host: host ? host.id : null,
                            visible: style.visibility === 'visible' &&
                              box.width > 0 && box.height > 0,
                          };
                        }"""
                    )
                )
            inside = [s for s in stops if s["host"]]
            assert not inside, inside[:5]
            hidden = [s for s in stops if not s["visible"]]
            assert not hidden, hidden[:5]
            # the walk reached real controls, including the table's
            assert {"search", "refreshBtn", "settingsBtn"} <= {
                s["id"] for s in stops
            }


def test_closing_a_surface_returns_focus_in_lifo_order(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.focus("#settingsBtn")
            page.keyboard.press("Enter")
            _wait_open(page, "settingsWrap")
            page.wait_for_function(
                "document.getElementById('settingsWrap')"
                ".contains(document.activeElement)"
            )
            # a second surface on top, opened from inside the first
            page.focus("#setPoll")
            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")
            page.keyboard.press("Escape")
            _wait_active(page, "setPoll")
            page.keyboard.press("Escape")
            _wait_active(page, "settingsBtn")

            # the filter keeps its caret across a palette round trip
            page.focus("#search")
            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")
            page.keyboard.press("Escape")
            _wait_active(page, "search")

            # a drawer opened from a row button returns focus to it
            page.focus('#rows [data-logs="beta-fail"]')
            page.keyboard.press("Enter")
            _wait_open(page, "drawer")
            page.wait_for_function(
                "document.getElementById('drawer')"
                ".contains(document.activeElement)"
            )
            page.keyboard.press("Escape")
            page.wait_for_function(
                "document.activeElement.getAttribute('data-logs') === "
                "'beta-fail'"
            )


def test_surface_closed_at_once_does_not_steal_focus_later(browser, tmp_path):
    """Skip delayed focus changes after a panel closes.

    Focus inside a closed overlay would prevent keyboard shortcuts."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            for opener in ("#authBtn", "#paletteBtn", "#settingsBtn"):
                page.evaluate(
                    """(opener) => {
                      document.querySelector(opener).click();
                      document.dispatchEvent(new KeyboardEvent('keydown',
                        { key: 'Escape', bubbles: true }));
                      window.__closedAt = performance.now();
                    }""",
                    opener,
                )
                assert _open_ids(page) == []
                page.wait_for_function(
                    "performance.now() - window.__closedAt > 250"
                )
                assert page.evaluate(
                    "!document.activeElement.closest('.overlay')"
                ), opener
            page.evaluate("document.activeElement.blur()")
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")


def test_focus_placed_inside_an_opening_surface_is_kept(browser, tmp_path):
    """Preserve focus when the user selects a control as a panel opens.

    The delayed focus callback must not override the user's selection,
    such as clicking the log search field.
    """
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.evaluate(
                """() => {
                  document.querySelector('#rows [data-logs]').click();
                  document.getElementById('logSearch').focus();
                  window.__openedAt = performance.now();
                }"""
            )
            page.wait_for_function(
                "performance.now() - window.__openedAt > 300"
            )
            assert _active(page) == "logSearch"
            page.keyboard.type("twj")
            assert page.input_value("#logSearch") == "twj"
            assert _theme(page) == "standard"


# --------------------------------------------------------------------------
# command palette
# --------------------------------------------------------------------------


def _items(page):
    return page.evaluate(
        "[...document.querySelectorAll('#paletteList .item')].map((i) => "
        "i.querySelector('.lbl').firstChild.textContent.trim())"
    )


def _cursor(page):
    return page.evaluate(
        "[...document.querySelectorAll('#paletteList .item')]"
        ".findIndex((i) => i.classList.contains('cur'))"
    )


def test_palette_search_ranking_cursor_and_run(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs={"pollMs": 0}) as page:
            page.faults.record()
            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")
            everything = _items(page)
            assert everything[0] == "Refresh now"
            assert len(everything) == 60  # the list is capped
            assert _cursor(page) == 0

            # rank substrings before subsequences, and earlier matches first
            page.keyboard.type("log")
            ranked = _items(page)
            assert ranked[0].startswith("Logs: ")
            assert all("log" in lbl.lower() for lbl in ranked[:5])
            # a subsequence still matches
            page.fill("#paletteInput", "rfn")
            assert "Refresh now" in _items(page)
            # case is ignored
            page.fill("#paletteInput", "CYCLE THEME")
            assert _items(page) == ["Cycle theme"]
            page.fill("#paletteInput", "zzzz no such thing")
            assert page.inner_text("#paletteList") == "no matches"
            # Enter on an empty list runs nothing and keeps the palette
            page.keyboard.press("Enter")
            assert _open_ids(page) == ["paletteWrap"]
            page.keyboard.press("Escape")
            _wait_open(page, "paletteWrap", False)

            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")
            # the query resets on reopen
            assert page.input_value("#paletteInput") == ""
            page.keyboard.type("toggle")
            entries = _items(page)
            assert len(entries) > 5
            # arrows move the cursor and clamp at both ends
            page.keyboard.press("ArrowUp")
            assert _cursor(page) == 0
            page.keyboard.press("ArrowDown")
            page.keyboard.press("ArrowDown")
            assert _cursor(page) == 2
            for _ in range(len(entries) + 3):
                page.keyboard.press("ArrowDown")
            assert _cursor(page) == len(entries) - 1
            # typing resets the cursor to the top
            page.keyboard.type(" compact")
            assert _cursor(page) == 0
            assert _items(page) == ["Toggle compact density"]
            page.keyboard.press("Enter")
            _wait_open(page, "paletteWrap", False)
            assert page.evaluate(
                "document.body.classList.contains('density-compact')"
            )

            # a click runs an entry too
            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")
            page.keyboard.type("Logs: beta-fail")
            page.click("#paletteList .item")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            assert page.inner_text("#dName") == "beta-fail"
            page.keyboard.press("Escape")

            # "Focus filter" hands focus to the filter, not back to the
            # element that held it before the palette
            page.focus("#refreshBtn")
            _palette_run(page, "Focus filter")
            _wait_active(page, "search")


def test_palette_entries_follow_job_state(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        daemon.api("POST", "/jobs/gamma-slow/start")
        daemon.api("POST", "/jobs/alpha-ok/pause")
        with e2e.open_page(browser, daemon.url) as page:
            e2e.wait_row_status(page, "gamma-slow", "Running")
            page.keyboard.press("Control+k")
            _wait_active(page, "paletteInput")

            def verbs(name):
                page.fill("#paletteInput", name)
                return sorted(
                    lbl.split(":")[0]
                    for lbl in _items(page)
                    if lbl.endswith(": " + name)
                )

            base = ["Copy command", "Logs", "Schedule", "Tail"]
            assert verbs("gamma-slow") == sorted(base + ["Cancel", "Pause"])
            assert verbs("alpha-ok") == sorted(base + ["Resume", "Run"])
            assert verbs("delta-off") == sorted(base + ["Pause"])
            assert verbs("beta-fail") == sorted(base + ["Pause", "Run"])


# --------------------------------------------------------------------------
# ARIA
# --------------------------------------------------------------------------


def test_dialog_roles_and_hidden_state(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            for overlay in OVERLAYS:
                dialogs = page.evaluate(
                    "(id) => [...document.querySelectorAll('#' + id + "
                    "' [role=dialog]')].map((d) => !!(d.getAttribute("
                    "'aria-label') || d.getAttribute('aria-labelledby')))",
                    overlay,
                )
                assert dialogs == [True], overlay
            for drawer, label in (
                ("drawer", "dName"),
                ("dagDrawer", "dgName"),
            ):
                assert page.get_attribute("#" + drawer, "role") == "dialog"
                assert page.get_attribute("#" + drawer, "aria-modal") == (
                    "true"
                )
                assert (
                    page.get_attribute("#" + drawer, "aria-labelledby")
                    == label
                )
                assert page.get_attribute("#" + drawer, "aria-hidden") == (
                    "true"
                )
            # a closed surface is out of the accessibility tree as well
            hidden = page.evaluate(
                "(ids) => ids.filter((id) => getComputedStyle("
                "document.getElementById(id)).visibility !== 'hidden')",
                OVERLAYS + ["drawer", "dagDrawer"],
            )
            assert hidden == []
            assert page.get_attribute("#wallboard", "aria-hidden") == "true"
            page.keyboard.press("w")
            page.wait_for_function("document.body.classList.contains('tv')")
            assert page.get_attribute("#wallboard", "aria-hidden") == "false"
            page.keyboard.press("Escape")
            page.wait_for_function(
                "document.getElementById('wallboard')"
                ".getAttribute('aria-hidden') === 'true'"
            )
            _click(page, '#rows [data-logs="alpha-ok"]')
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            # the label the dialog points at names the job
            assert page.inner_text("#dName") == "alpha-ok"
            assert (
                page.evaluate(
                    "getComputedStyle(document.getElementById('drawer'))"
                    ".visibility"
                )
                == "visible"
            )
            assert page.get_attribute("#colsMenu", "role") == "menu"
