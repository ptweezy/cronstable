"""Test token entry, 401 responses, and controls for each authorization scope.

The dashboard runs against the daemon in ``tests/_web_e2e.py``. Its web
configuration includes a token with all scopes, one token per scope, and
anonymous viewing permissions. ``/whoami`` uses the daemon's authorization
middleware. Tests check these behaviors:

* Without a required token, the page opens the token dialog. Saving a token
  stores it in ``sessionStorage``, sends it in the bearer header, and loads
  the dashboard.
* Invalid or cleared tokens reopen the required token dialog.
* A ``view`` token hides controls that change state and disables the r, x,
  and p shortcuts.
* ``control`` and ``approve`` expose only their respective actions.
* Public viewing works without a token and is labeled accordingly.
* A 403 response produces a notification. If ``/whoami`` fails, the page
  retains its existing scopes.
"""

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _open(page, overlay_id):
    page.wait_for_function(
        "(id) => document.getElementById(id).classList.contains('open')",
        arg=overlay_id,
    )


def _closed(page, overlay_id):
    page.wait_for_function(
        "(id) => !document.getElementById(id).classList.contains('open')",
        arg=overlay_id,
    )


def _stored_token(page):
    return page.evaluate("sessionStorage.getItem('cronstable_token')")


def _shown(page, element_id):
    return page.evaluate(
        "(id) => getComputedStyle(document.getElementById(id)).display "
        "!== 'none'",
        element_id,
    )


def _row_buttons(page, name):
    """The data-* attributes of the buttons in one row's actions cell."""
    return page.evaluate(
        """(name) => {
          const tr = [...document.querySelectorAll('#rows tr[data-job]')]
            .find((r) => r.getAttribute('data-job') === name);
          return [...tr.querySelectorAll('.actions button')].map((b) =>
            [...b.attributes].map((a) => a.name)
              .find((n) => n.startsWith('data-')));
        }""",
        name,
    )


def _palette_labels(page, query):
    page.evaluate("document.activeElement.blur()")
    page.keyboard.press("Control+k")
    _open(page, "paletteWrap")
    page.fill("#paletteInput", query)
    labels = page.evaluate(
        "[...document.querySelectorAll('#paletteList .item .lbl')]"
        ".map((l) => l.textContent)"
    )
    page.keyboard.press("Escape")
    _closed(page, "paletteWrap")
    return labels


def _gate_daemon(tmp_path, auth):
    daemon = e2e.Daemon(
        tmp_path,
        auth=auth,
        jobs=e2e.default_jobs() + [e2e.job("pooled", "sleep 30", pool="db")],
        dags=[e2e.diamond_dag("diamond")],
        pools={"db": {"slots": 1}},
    )
    daemon.start()
    run_key = daemon.trigger_dag("diamond", e2e.FULL_TOKEN)
    daemon.wait_gate("diamond", run_key, token=e2e.FULL_TOKEN)
    return daemon, run_key


def _open_gate(page):
    _click(page, '#dagRows [data-dagopen="diamond"]')
    page.wait_for_selector("#dgRuns tr.dagrun")
    _click(page, '#dagTabs button[data-dtab="tasks"]')
    page.wait_for_function(
        "document.getElementById('dgTasks').textContent"
        ".includes('awaiting approval')"
    )


# --------------------------------------------------------------------------
# the token modal
# --------------------------------------------------------------------------


def test_no_token_lands_on_the_required_modal(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(browser, daemon.url, wait_rows=False) as page:
            requests = page.faults.record()
            _open(page, "modalWrap")
            assert page.inner_text("#modalTitle") == "Access token required"
            assert "requires a bearer token" in page.inner_text("#modalMsg")
            # nothing to clear yet, and no data reached the table
            assert not _shown(page, "tokenClear")
            assert e2e.row_names(page) == []
            assert page.inner_text("#authLabel") == "token"
            # the input takes focus for immediate typing
            page.wait_for_function(
                "document.activeElement === "
                "document.getElementById('tokenInput')"
            )
            # a 401 is not a lost connection: the header keeps "live"
            page.reload()
            _open(page, "modalWrap")
            assert "no signal" not in page.inner_text("#conn")
            assert all("authorization" not in r["headers"] for r in requests)


@pytest.mark.parametrize("how", ["button", "enter"])
def test_saving_a_token_loads_the_board(browser, tmp_path, how):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(browser, daemon.url, wait_rows=False) as page:
            _open(page, "modalWrap")
            requests = page.faults.record()
            # surrounding whitespace is trimmed before the token is stored
            page.fill("#tokenInput", "  " + e2e.FULL_TOKEN + " ")
            if how == "button":
                page.click("#tokenSave")
            else:
                page.press("#tokenInput", "Enter")
            page.wait_for_selector("#rows tr[data-job]")
            _closed(page, "modalWrap")
            assert _stored_token(page) == e2e.FULL_TOKEN
            assert page.inner_text("#authLabel") == "token ✓"
            assert page.evaluate(
                "document.getElementById('authBtn').classList.contains('on')"
            )
            # the token never reaches localStorage or the URL
            assert page.evaluate(
                "Object.keys(localStorage).every((k) => "
                "!localStorage.getItem(k).includes('full-token'))"
            )
            assert e2e.FULL_TOKEN not in page.url
            for path in ("/whoami", "/jobs"):
                sent = page.faults.sent("GET", path)
                assert sent, path
                assert sent[-1]["headers"]["authorization"] == (
                    "Bearer " + e2e.FULL_TOKEN
                )
            assert all("token=" not in r["query"] for r in requests)
            # full scope: every row carries its action buttons
            assert _row_buttons(page, "alpha-ok") == [
                "data-run",
                "data-pause",
                "data-logs",
            ]
            # the stored token survives a reload of the same tab
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            assert _stored_token(page) == e2e.FULL_TOKEN


def test_wrong_token_returns_to_the_required_modal(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(browser, daemon.url, wait_rows=False) as page:
            _open(page, "modalWrap")
            page.fill("#tokenInput", "not-the-token")
            page.click("#tokenSave")
            # the save closes the modal; the 401 that follows reopens it
            page.wait_for_function(
                "document.getElementById('modalWrap').classList"
                ".contains('open') && document.getElementById('modalTitle')"
                ".textContent === 'Access token required'"
            )
            assert e2e.row_names(page) == []
            # the rejected value is offered back for correction
            page.wait_for_function(
                "document.getElementById('tokenInput').value === "
                "'not-the-token'"
            )
            assert _shown(page, "tokenClear")
            page.fill("#tokenInput", e2e.FULL_TOKEN)
            page.press("#tokenInput", "Enter")
            page.wait_for_selector("#rows tr[data-job]")
            _closed(page, "modalWrap")


def test_clearing_the_token_returns_to_the_required_modal(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(browser, daemon.url, token=e2e.FULL_TOKEN) as page:
            _click(page, "#authBtn")
            _open(page, "modalWrap")
            assert page.inner_text("#modalTitle") == "Update access token"
            assert page.input_value("#tokenInput") == e2e.FULL_TOKEN
            assert _shown(page, "tokenClear")
            page.click("#tokenClear")
            page.wait_for_function(
                "document.getElementById('modalTitle').textContent === "
                "'Access token required'"
            )
            _open(page, "modalWrap")
            assert _stored_token(page) is None
            assert page.inner_text("#authLabel") == "token"
            # cancel leaves the modal without storing anything
            page.fill("#tokenInput", "typed-but-cancelled")
            page.click("#tokenCancel")
            _closed(page, "modalWrap")
            assert _stored_token(page) is None


def test_saving_an_empty_value_removes_the_token(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="public") as daemon:
        with e2e.open_page(browser, daemon.url, token=e2e.FULL_TOKEN) as page:
            assert "data-run" in _row_buttons(page, "alpha-ok")
            _click(page, "#authBtn")
            _open(page, "modalWrap")
            page.fill("#tokenInput", "   ")
            page.click("#tokenSave")
            page.wait_for_function(
                "document.getElementById('authLabel').textContent === "
                "'view only'"
            )
            assert _stored_token(page) is None


def test_token_revoked_by_the_daemon_reopens_the_modal(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="full") as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.FULL_TOKEN,
            prefs={"pollMs": 1000},
        ) as page:
            web = dict(daemon.config["web"])
            web["authToken"] = {"value": "rotated-token"}
            port = daemon.port
            web["listen"] = ["http://127.0.0.1:{}".format(port)]
            daemon.reload(web=web)
            _open(page, "modalWrap")
            assert page.inner_text("#modalTitle") == "Access token required"
            page.fill("#tokenInput", "rotated-token")
            page.click("#tokenSave")
            _closed(page, "modalWrap")
            page.wait_for_function(
                "document.getElementById('conn').textContent === 'live'"
            )


# --------------------------------------------------------------------------
# controls for each authorization scope
# --------------------------------------------------------------------------


def test_view_token_removes_every_mutating_affordance(browser, tmp_path):
    daemon, _ = _gate_daemon(tmp_path, "scoped")
    try:
        # a queued pool entry gives the pool card a cancel button to hide
        daemon.api("POST", "/jobs/pooled/start", e2e.FULL_TOKEN)
        daemon.api("POST", "/jobs/pooled/start", e2e.FULL_TOKEN)
        daemon.run_and_wait("beta-fail", e2e.FULL_TOKEN)
        with e2e.open_page(browser, daemon.url, token=e2e.VIEW_TOKEN) as page:
            requests = page.faults.record()
            page.wait_for_selector("#dagRows tr[data-dag]")
            assert page.inner_text("#authLabel") == "token ✓"
            for name in e2e.row_names(page):
                assert _row_buttons(page, name) == ["data-logs"], name
            for element_id in ("runFailingBtn", "vMitigate"):
                assert not _shown(page, element_id), element_id
            # the verdict bar itself still reports the failure
            assert _shown(page, "verdictBar")
            assert not page.query_selector("#dagRows [data-dagtrigger]")
            page.wait_for_selector("#poolBody details")
            page.evaluate(
                "document.querySelector('#poolBody details').open = true"
            )
            assert "queued" in page.inner_text("#poolBody")
            assert not page.query_selector("#poolBody [data-cancel-queue]")

            # the keyboard route to the row buttons is inert
            page.keyboard.press("j")
            page.wait_for_selector("#rows tr.sel")
            for key in ("r", "x", "p"):
                page.keyboard.press(key)
            # c (copy command) reads only, and still works
            page.keyboard.press("Enter")
            page.wait_for_selector('#drawer[aria-hidden="false"]')
            for element_id in ("dRun", "dCancel", "dPause"):
                assert not _shown(page, element_id), element_id
            # a poll re-renders the drawer meta; the buttons stay hidden
            page.keyboard.press("Escape")
            page.wait_for_selector('#drawer[aria-hidden="true"]')

            labels = _palette_labels(page, "alpha-ok")
            assert any(lbl.startswith("Logs: alpha-ok") for lbl in labels)
            assert not [
                lbl
                for lbl in labels
                if lbl.startswith(("Run:", "Pause:", "Cancel:", "Resume:"))
            ]
            failing = _palette_labels(page, "failing")
            assert any(lbl.startswith("Live logs: failing") for lbl in failing)
            assert not [lbl for lbl in failing if "Run all" in lbl]
            assert not [lbl for lbl in failing if "Review actions" in lbl]
            assert not [
                lbl
                for lbl in _palette_labels(page, "workflow")
                if lbl.startswith("Run workflow")
            ]

            # the gate shows as a fact, with no decision buttons
            _open_gate(page)
            assert not page.query_selector("#dgTasks [data-approve]")
            assert not page.query_selector("#dgTasks [data-reject]")
            assert not page.query_selector("#dgTasks [data-recover-task]")
            for element_id in ("dgTrigger", "dgBackfillBtn"):
                assert not _shown(page, element_id), element_id
            assert page.evaluate(
                "document.getElementById('dgRecoverBtn').disabled"
            )
            assert not [r for r in requests if r["method"] != "GET"]
    finally:
        daemon.stop()


def test_control_token_acts_but_cannot_decide_a_gate(browser, tmp_path):
    daemon, _ = _gate_daemon(tmp_path, "scoped")
    try:
        with e2e.open_page(
            browser, daemon.url, token=e2e.CONTROL_TOKEN
        ) as page:
            page.faults.record()
            assert _row_buttons(page, "alpha-ok") == [
                "data-run",
                "data-pause",
                "data-logs",
            ]
            assert _shown(page, "runFailingBtn")
            assert page.query_selector("#dagRows [data-dagtrigger]")
            _open_gate(page)
            assert not page.query_selector("#dgTasks [data-approve]")
            assert _shown(page, "dgTrigger")
            page.keyboard.press("Escape")
            page.wait_for_selector('#dagDrawer[aria-hidden="true"]')
            # the control scope is real: the daemon accepts the action
            _click(page, '#rows [data-run="alpha-ok"]')
            e2e.wait_toast(page, "started alpha-ok")
            sent = page.faults.sent("POST", "/jobs/alpha-ok/start")
            assert sent[0]["headers"]["authorization"] == (
                "Bearer " + e2e.CONTROL_TOKEN
            )
    finally:
        daemon.stop()


def test_approve_token_decides_a_gate_but_cannot_act(browser, tmp_path):
    daemon, run_key = _gate_daemon(tmp_path, "scoped")
    try:
        with e2e.open_page(
            browser, daemon.url, token=e2e.APPROVE_TOKEN
        ) as page:
            page.faults.record()
            assert _row_buttons(page, "alpha-ok") == ["data-logs"]
            assert not page.query_selector("#dagRows [data-dagtrigger]")
            _open_gate(page)
            assert not _shown(page, "dgTrigger")
            page.click("#dgTasks [data-approve='gate']")
            e2e.wait_toast(page, "approved gate")
            path = "/dags/diamond/runs/{}/tasks/gate/decision".format(run_key)
            sent = page.faults.sent("POST", path)
            assert len(sent) == 1
            assert sent[0]["headers"]["authorization"] == (
                "Bearer " + e2e.APPROVE_TOKEN
            )
        run = daemon.wait_dag_state(
            "diamond", run_key, "success", e2e.FULL_TOKEN
        )
        assert run["tasks"]["gate"]["approval"]["decision"] == "approved"
    finally:
        daemon.stop()


def test_full_token_shows_gate_decisions_and_pair_warning(browser, tmp_path):
    daemon, _ = _gate_daemon(tmp_path, "scoped")
    try:
        with e2e.open_page(browser, daemon.url, token=e2e.FULL_TOKEN) as page:
            _open_gate(page)
            assert page.query_selector("#dgTasks [data-approve='gate']")
            assert page.query_selector("#dgTasks [data-reject='gate']")
            page.keyboard.press("Escape")
            page.wait_for_selector('#dagDrawer[aria-hidden="true"]')
            # pairing with the all-scopes token warns; a scoped one is quiet
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            _open(page, "pairWrap")
            page.wait_for_function(
                "document.getElementById('pairWarn').style.display === ''"
            )
            assert not _shown(page, "pairHint")
        with e2e.open_page(browser, daemon.url, token=e2e.VIEW_TOKEN) as page:
            page.faults.record()
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            _open(page, "pairWrap")
            page.wait_for_function(
                "(n) => window.performance.getEntriesByName("
                "location.origin + '/whoami').length >= n",
                arg=2,
            )
            assert not _shown(page, "pairWarn")
    finally:
        daemon.stop()


def test_public_view_reads_without_a_token_and_says_so(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="public") as daemon:
        daemon.run_and_wait("beta-fail", e2e.FULL_TOKEN)
        with e2e.open_page(browser, daemon.url) as page:
            requests = page.faults.record()
            assert page.inner_text("#authLabel") == "view only"
            assert page.evaluate(
                "document.getElementById('authBtn').classList.contains('ro')"
            )
            assert "Read-only public view" in page.get_attribute(
                "#authBtn", "title"
            )
            assert not page.evaluate(
                "document.getElementById('modalWrap').classList"
                ".contains('open')"
            )
            for name in e2e.row_names(page):
                assert _row_buttons(page, name) == ["data-logs"], name
            assert not _shown(page, "runFailingBtn")
            # the logs tail and history are part of the view scope
            _click(page, '#rows [data-logs="beta-fail"]')
            page.wait_for_function(
                "document.getElementById('term').textContent"
                ".includes('beta-err')"
            )
            page.keyboard.press("Escape")
            page.wait_for_selector('#drawer[aria-hidden="true"]')
            _click(page, "#authBtn")
            _open(page, "modalWrap")
            assert "publicly viewable" in page.inner_text("#modalMsg")
            page.keyboard.press("Escape")
            _closed(page, "modalWrap")
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            _open(page, "pairWrap")
            assert "public view" in page.inner_text("#pairHint")
            page.keyboard.press("Escape")
            _closed(page, "pairWrap")
            assert all("authorization" not in r["headers"] for r in requests)
            # an operator token upgrades the same tab in place
            _click(page, "#authBtn")
            _open(page, "modalWrap")
            page.fill("#tokenInput", e2e.FULL_TOKEN)
            page.click("#tokenSave")
            page.wait_for_selector('#rows [data-run="alpha-ok"]')
            assert page.inner_text("#authLabel") == "token ✓"
            assert _shown(page, "runFailingBtn")
            assert not page.evaluate(
                "document.getElementById('authBtn').classList.contains('ro')"
            )


def test_public_view_boot_screen_announces_read_only(browser, tmp_path):
    with e2e.Daemon(tmp_path, auth="public") as daemon:
        with e2e.open_page(
            browser, daemon.url, boot=True, wait_rows=False
        ) as page:
            page.wait_for_function(
                "document.getElementById('bootLog').textContent"
                ".includes('PUBLIC VIEW')"
            )
            assert "[read-only]" in page.inner_text("#bootLog")
            page.wait_for_selector("#rows tr[data-job]")


def test_anonymous_viewer_with_a_wrong_token_gets_the_modal(browser, tmp_path):
    """Return 401 for an unknown token, even when anonymous viewing is allowed.

    An invalid token must not receive anonymous permissions."""
    with e2e.Daemon(tmp_path, auth="public") as daemon:
        with e2e.open_page(
            browser, daemon.url, token="stale-token", wait_rows=False
        ) as page:
            _open(page, "modalWrap")
            assert page.inner_text("#modalTitle") == "Access token required"
            page.click("#tokenClear")
            page.wait_for_selector("#rows tr[data-job]")
            assert page.inner_text("#authLabel") == "view only"


# --------------------------------------------------------------------------
# refusals and probe failures
# --------------------------------------------------------------------------


def test_a_403_from_the_daemon_reaches_the_operator(browser, tmp_path):
    """Enforce token scopes even if /whoami reports broader permissions.

    Show the daemon's refusal and leave the row unchanged."""
    with e2e.Daemon(tmp_path, auth="scoped") as daemon:

        def routes(page):
            page.faults.rewrite(
                r"/whoami",
                lambda body: dict(body, allScopes=True),
            )

        with e2e.open_page(
            browser, daemon.url, token=e2e.VIEW_TOKEN, before_goto=routes
        ) as page:
            page.faults.record()
            for attr, toast in (
                ("data-run", "could not start (HTTP 403)"),
                ("data-pause", "could not pause (HTTP 403)"),
            ):
                _click(page, '#rows [{}="alpha-ok"]'.format(attr))
                assert "err" in e2e.wait_toast(page, toast)
            assert len(page.faults.sent("POST", "/jobs/alpha-ok/start")) == 1
            assert e2e.row_status(page, "alpha-ok") == "Pending"
        assert daemon.jobs(e2e.FULL_TOKEN)["alpha-ok"]["last_run"] is None


@pytest.mark.parametrize("fault", ["status", "abort"])
def test_whoami_failure_keeps_the_scopes_already_held(
    browser, tmp_path, fault
):
    with e2e.Daemon(tmp_path, auth="scoped") as daemon:
        with e2e.open_page(browser, daemon.url, token=e2e.VIEW_TOKEN) as page:
            assert _row_buttons(page, "alpha-ok") == ["data-logs"]
            if fault == "status":
                page.faults.status(r"/whoami", 500)
            else:
                page.faults.abort(r"/whoami")
            page.faults.record()
            # re-saving the token reruns the probe, which fails this time
            _click(page, "#authBtn")
            _open(page, "modalWrap")
            page.click("#tokenSave")
            page.wait_for_function(
                "(n) => window.performance.getEntriesByName("
                "location.origin + '/whoami').length >= n",
                arg=2,
            )
            page.keyboard.press("g")
            page.wait_for_function(
                "document.getElementById('conn').textContent === 'live'"
            )
            assert _row_buttons(page, "alpha-ok") == ["data-logs"]
            assert not _shown(page, "runFailingBtn")


def test_first_whoami_failure_keeps_the_full_chrome(browser, tmp_path):
    """Keep controls available if /whoami is unreachable.

    The daemon still enforces authorization for each request."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            before_goto=lambda page: page.faults.status(r"/whoami", 503),
        ) as page:
            assert _row_buttons(page, "alpha-ok") == [
                "data-run",
                "data-pause",
                "data-logs",
            ]
            assert _shown(page, "runFailingBtn")
