"""Test the DAG card and run drawer against a running daemon.

Use ``tests/_web_e2e.py`` with durable storage and DAGs that cover parallel
tasks, XCom values, approval gates, failures, backfill, and streamed task
logs. The daemon executes task commands and stores the run documents that
the dashboard displays. Tests check these behaviors:

* The DAG card lists configured DAGs and sends trigger requests.
* The Runs, Tasks, Graph, XCom, and Logs tabs display run data. The graph
  arranges tasks by dependency rank.
* Approval decisions update the stored run. If another client has already
  decided a gate, the dashboard displays the daemon's 409 response.
* Backfill creates runs. Recovery previews and starts runs, and offers a
  retry when the daemon rejects a stale preview with a 409 response.
* ``#dag/<name>`` and ``#dag/<name>/<run>`` open the corresponding drawer.
"""

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


def _open_dag(page, name):
    page.wait_for_selector('#dagRows tr[data-dag="{}"]'.format(name))
    _click(page, '#dagRows [data-dagopen="{}"]'.format(name))
    page.wait_for_selector('#dagDrawer[aria-hidden="false"]')


def _tab(page, tab):
    _click(page, '#dagTabs button[data-dtab="{}"]'.format(tab))
    page.wait_for_selector(
        '#dagDrawer .dpane.active[data-dpane="{}"]'.format(tab)
    )


def _task_states(page):
    """Return a mapping from task names to state labels in the Tasks tab."""
    return page.evaluate(
        """() => Object.fromEntries(
          [...document.querySelectorAll('#dgTasks tbody tr')].map((tr) => [
            tr.cells[0].querySelector('b').textContent,
            // Read the state label without the preceding icon.
            tr.cells[1].textContent.slice(
              tr.cells[1].querySelector('.g').textContent.length),
          ]))"""
    )


def _wait_task_state(page, task, label):
    page.wait_for_function(
        """([task, label]) => [...document.querySelectorAll(
            '#dgTasks tbody tr')].some((tr) =>
              tr.cells[0].textContent.startsWith(task) &&
              tr.cells[1].textContent.includes(label))""",
        arg=[task, label],
    )


def _posts(page):
    return [
        (r["path"], json.loads(r["body"]) if r["body"] else None)
        for r in page.faults.requests
        if r["method"] == "POST"
    ]


def _slow_dag():
    code = (
        "import time\n"
        "for i in range(400):\n"
        '    print("tick", i, flush=True)\n'
        "    time.sleep(0.05)"
    )
    return {
        "name": "slowdag",
        "tasks": [
            {
                "id": "ticker",
                "command": e2e.py_cmd(code),
                # Enable output capture so task logs can be streamed.
                "captureStdout": True,
            },
            {"id": "after", "dependsOn": ["ticker"], "command": "echo done"},
        ],
    }


# --------------------------------------------------------------------------
# card, trigger, tabs
# --------------------------------------------------------------------------


def test_card_lists_dags_and_trigger_starts_a_real_run(browser, tmp_path):
    nightly = e2e.diamond_dag("nightly", gate=False)
    nightly["schedule"] = "0 0 1 1 *"
    dags = [e2e.diamond_dag("diamond"), nightly]
    with e2e.Daemon(tmp_path, dags=dags) as daemon:
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            page.faults.record()
            page.wait_for_selector('#dagRows tr[data-dag="diamond"]')
            assert page.text_content("#dagMeta") == "2 workflows"
            row = '#dagRows tr[data-dag="diamond"]'
            assert page.text_content(row + " .dagname") == "diamond"
            assert page.text_content(row + " .dagmeta") == (
                "5 tasks · 4 · 1 approval"
            )
            assert "no runs" in page.text_content(row)
            assert "manual" in page.text_content(row)
            # the scheduled DAG shows its cron chip with a description
            chip = '#dagRows tr[data-dag="nightly"] .chip'
            assert page.text_content(chip) == "0 0 1 1 *"
            assert page.get_attribute(chip, "title")

            _click(page, '#dagRows [data-dagtrigger="diamond"]')
            assert "ok" in e2e.wait_toast(page, "▶ triggered diamond")
            assert _posts(page) == [("/dags/diamond/trigger", None)]
            page.wait_for_selector(row + " .rpill.running")
            page.wait_for_function(
                "(sel) => document.querySelector(sel).textContent"
                ".includes('1 run')",
                arg=row,
            )
            # the card toggle hides it and the choice persists
            _click(page, "#dagsBtn")
            page.wait_for_function(
                "document.getElementById('dagCard').style.display === 'none'"
            )
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            page.wait_for_function(
                "document.getElementById('dagsBtn').style.display === ''"
            )
            assert not page.is_visible("#dagCard")
            _click(page, "#dagsBtn")
            page.wait_for_selector(row + " .rpill.running")
        runs = daemon.api("GET", "/dags/diamond/runs")[1]["runs"]
        assert len(runs) == 1 and runs[0]["kind"] == "manual"


def test_drawer_tabs_render_a_real_run(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            _open_dag(page, "diamond")
            # Runs: the one run, selected, with its state and kind
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            assert (
                page.get_attribute("#dgRuns tr.dagrun", "data-runkey")
                == run_key
            )
            assert page.text_content("#dgRuns .rpill").endswith("running")
            assert page.text_content("#dgRuns .kbadge") == "manual"
            assert page.evaluate("location.hash") == (
                "#dag/diamond/" + run_key
            )
            page.wait_for_function(
                "document.getElementById('dgState').textContent"
                ".includes('running')"
            )
            meta = page.inner_text("#dgMeta")
            assert "5 tasks" in meta and "manual" in meta
            assert run_key[:20] in meta

            _tab(page, "tasks")
            _wait_task_state(page, "gate", "running")
            assert _task_states(page) == {
                "extract": "success",
                "left": "success",
                "right": "success",
                "gate": "running",
                "publish": "pending",
            }
            assert (
                page.text_content("#dgTasks tbody tr:nth-child(4) .kbadge")
                == "approval"
            )
            assert "awaiting approval" in page.inner_text("#dgTasks")
            assert (
                page.inner_text("#dgTasks tbody tr:nth-child(1) .exit") == "0"
            )

            _tab(page, "xcom")
            page.wait_for_selector("#dgXcom .xcomval")
            cells = page.evaluate(
                "[...document.querySelectorAll('#dgXcom tbody td')]"
                ".map((td) => td.textContent)"
            )
            assert cells == ["extract", "rows", "10 B", '["a","b"]\n']

            # Logs: a finished task has no live buffer to tail
            _tab(page, "logs")
            options = page.evaluate(
                "[...document.querySelectorAll('#dgLogTask option')]"
                ".map((o) => o.textContent)"
            )
            assert options == [
                "— pick a task —",
                "extract · success",
                "gate · running",
                "left · success",
                "publish · pending",
                "right · success",
            ]
            page.select_option("#dgLogTask", "extract")
            page.wait_for_function(
                "document.getElementById('dgLogNote').textContent === "
                "'no live output (task is not running)'"
            )


def test_graph_lays_out_the_diamond_by_rank(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            _open_dag(page, "diamond")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "graph")
            page.wait_for_selector("#dgGraph svg g.node")
            nodes = page.evaluate(
                """() => Object.fromEntries(
                  [...document.querySelectorAll('#dgGraph g.node')].map(
                    (g) => { const r = g.querySelector('rect'); return [
                      g.getAttribute('data-gtask'),
                      { x: +r.getAttribute('x'), y: +r.getAttribute('y'),
                        w: +r.getAttribute('width'),
                        title: g.querySelector('title').textContent,
                        text: g.querySelector('text').textContent }]; }))"""
            )
            assert sorted(nodes) == [
                "extract",
                "gate",
                "left",
                "publish",
                "right",
            ]
            # one column per dependency rank, left to right
            assert nodes["extract"]["x"] < nodes["left"]["x"]
            assert nodes["left"]["x"] == nodes["right"]["x"]
            assert nodes["left"]["y"] != nodes["right"]["y"]
            assert nodes["left"]["x"] < nodes["gate"]["x"]
            assert nodes["gate"]["x"] < nodes["publish"]["x"]
            # no two nodes overlap
            boxes = [(n["x"], n["y"]) for n in nodes.values()]
            assert len(set(boxes)) == 5
            # titles carry the live state and the task type
            assert nodes["extract"]["title"] == "extract · success"
            assert nodes["gate"]["title"] == "gate · running (approval)"
            assert nodes["publish"]["title"] == "publish · pending"
            assert nodes["gate"]["text"].endswith("gate")

            edges = page.evaluate(
                "[...document.querySelectorAll('#dgGraph path.edge')]"
                ".map((p) => p.getAttribute('d'))"
            )
            assert len(edges) == 5
            # each edge leaves a node's right side and enters one's left
            ends = set()
            for d in edges:
                nums = [
                    float(v)
                    for v in d.replace("M", "").replace("C", "").split()
                ]
                ends.add(((nums[0], nums[1]), (nums[-2], nums[-1])))
            rights = {
                (n["x"] + n["w"], n["y"] + 15): k for k, n in nodes.items()
            }
            lefts = {(n["x"], n["y"] + 15): k for k, n in nodes.items()}
            assert sorted((rights[a], lefts[b]) for a, b in ends) == [
                ("extract", "left"),
                ("extract", "right"),
                ("gate", "publish"),
                ("left", "gate"),
                ("right", "gate"),
            ]
            # The SVG is large enough to contain every node.
            size = page.evaluate(
                "(() => { const s = document.querySelector('#dgGraph svg');"
                " return [+s.getAttribute('width'), "
                "+s.getAttribute('height')]; })()"
            )
            assert size[0] > nodes["publish"]["x"] + nodes["publish"]["w"]
            assert size[1] > max(n["y"] for n in nodes.values()) + 30
            # Clicking a graph node opens the Tasks tab.
            page.click('#dgGraph g.node[data-gtask="gate"]')
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="tasks"]'
            )


def test_task_log_tail_streams_a_running_task(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[_slow_dag()]) as daemon:
        run_key = daemon.trigger_dag("slowdag")
        daemon.wait_dag(
            "slowdag",
            run_key,
            lambda run: run["tasks"]["ticker"]["state"] == "running",
        )
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "slowdag")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "logs")
            page.wait_for_selector(
                '#dgLogTask option[value="ticker"]', state="attached"
            )
            page.select_option("#dgLogTask", "ticker")
            page.wait_for_function(
                "document.querySelectorAll('#dgTerm .ln.stdout').length > 5"
            )
            first = page.evaluate(
                "document.querySelectorAll('#dgTerm .ln').length"
            )
            page.wait_for_function(
                "(n) => document.querySelectorAll('#dgTerm .ln').length "
                "> n + 5",
                arg=first,
            )
            assert page.text_content("#dgTerm .ln") == "tick 0"
            path = "/dags/slowdag/runs/{}/tasks/ticker/logs".format(run_key)
            assert len(page.faults.sent("GET", path)) == 1
            # Polling updates the other tabs without closing the log stream.
            page.wait_for_function(
                "(n) => document.querySelectorAll('#dgTerm .ln').length "
                "> n + 40",
                arg=first,
            )
            assert len(page.faults.sent("GET", path)) == 1
            # leaving the tab drops the stream; returning opens a new one
            _tab(page, "tasks")
            _wait_task_state(page, "ticker", "running")
            _tab(page, "logs")
            e2e.wait_until(
                lambda: len(page.faults.sent("GET", path)) == 2, page
            )
            page.wait_for_function(
                "document.querySelectorAll('#dgTerm .ln.stdout').length > 5"
            )
            # the replay starts from the task's first line again
            assert page.text_content("#dgTerm .ln") == "tick 0"
            # Selecting "pick a task" stops the log stream.
            page.select_option("#dgLogTask", "")
            count = page.evaluate(
                "document.querySelectorAll('#dgTerm .ln').length"
            )
            page.wait_for_function(
                "(t0) => performance.now() - t0 > 400",
                arg=page.evaluate("performance.now()"),
            )
            assert (
                page.evaluate(
                    "document.querySelectorAll('#dgTerm .ln').length"
                )
                == count
            )


# --------------------------------------------------------------------------
# approval gate
# --------------------------------------------------------------------------


def test_approve_through_the_page_completes_the_run(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "diamond")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "tasks")
            page.click('#dgTasks [data-approve="gate"]')
            assert "ok" in e2e.wait_toast(page, "✓ approved gate")
            path = "/dags/diamond/runs/{}/tasks/gate/decision".format(run_key)
            assert _posts(page) == [
                (path, {"decision": "approve", "by": "dashboard"})
            ]
            sent = page.faults.sent("POST", path)[0]
            assert sent["headers"]["content-type"] == "application/json"
            # The drawer polls until the run completes.
            _wait_task_state(page, "publish", "success")
            page.wait_for_function(
                "document.getElementById('dgState').textContent"
                ".includes('success')"
            )
            assert "approved by dashboard" in page.inner_text("#dgTasks")
            assert not page.query_selector("#dgTasks [data-approve]")
            page.wait_for_selector(
                '#dagRows tr[data-dag="diamond"] .rpill.success'
            )
        run = daemon.dag_run("diamond", run_key)
        assert run["state"] == "success"
        assert run["tasks"]["gate"]["approval"]["decision"] == "approved"
        assert run["tasks"]["gate"]["approval"]["by"] == "dashboard"


def test_reject_through_the_page_fails_the_run(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "diamond")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "tasks")
            page.click('#dgTasks [data-reject="gate"]')
            assert "ok" in e2e.wait_toast(page, "✕ rejected gate")
            assert _posts(page)[0][1] == {
                "decision": "reject",
                "by": "dashboard",
            }
            _wait_task_state(page, "gate", "failed")
            _wait_task_state(page, "publish", "dependency failed")
            assert "rejected by dashboard" in page.inner_text("#dgTasks")
            page.wait_for_function(
                "document.getElementById('dgState').textContent"
                ".includes('failed')"
            )
            # a failed run enables recovery
            page.wait_for_function(
                "!document.getElementById('dgRecoverBtn').disabled"
            )
        run = daemon.dag_run("diamond", run_key)
        assert run["state"] == "failed"
        assert run["tasks"]["gate"]["approval"]["decision"] == "rejected"


def test_decision_on_a_gate_decided_elsewhere_shows_the_409(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            _open_dag(page, "diamond")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "tasks")
            page.wait_for_selector('#dgTasks [data-approve="gate"]')
            # Stop drawer updates, then submit a decision from another client.
            parked = page.faults.hang(
                r"/dags/diamond/runs/[^/]+", method="GET"
            )
            status, _ = daemon.api(
                "POST",
                "/dags/diamond/runs/{}/tasks/gate/decision".format(run_key),
                body={"decision": "approve", "by": "someone-else"},
            )
            assert status == 200
            daemon.wait_dag_state("diamond", run_key, "success")
            page.click('#dgTasks [data-approve="gate"]')
            assert "err" in e2e.wait_toast(
                page, "no longer waiting for approval"
            )
            page.faults.clear(r"/dags/diamond/runs/[^/]+")
            for route in parked:
                route.fallback()
            _click(page, '#dagTabs button[data-dtab="runs"]')
            _tab(page, "tasks")
            page.wait_for_function(
                "document.getElementById('dgTasks').textContent"
                ".includes('approved by someone-else')"
            )


@pytest.mark.parametrize(
    "fault,toast",
    [("500", "decision failed (HTTP 500)"), ("abort", "decision failed")],
)
def test_decision_failures_are_reported(browser, tmp_path, fault, toast):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        run_key = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", run_key)
        with e2e.open_page(browser, daemon.url) as page:
            _open_dag(page, "diamond")
            page.wait_for_selector("#dgRuns tr.dagrun.active")
            _tab(page, "tasks")
            path = r"/dags/diamond/runs/[^/]+/tasks/gate/decision"
            if fault == "abort":
                page.faults.abort(path)
            else:
                page.faults.status(path, 500)
            page.click('#dgTasks [data-approve="gate"]')
            assert "err" in e2e.wait_toast(page, toast)
        run = daemon.dag_run("diamond", run_key)
        assert run["tasks"]["gate"]["awaitingApproval"]


# --------------------------------------------------------------------------
# trigger from the drawer, backfill
# --------------------------------------------------------------------------


def test_trigger_from_the_drawer_selects_the_new_run(browser, tmp_path):
    dags = [e2e.diamond_dag("plain", gate=False)]
    with e2e.Daemon(tmp_path, dags=dags) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "plain")
            page.wait_for_function(
                "document.getElementById('dgRuns').textContent"
                ".includes('No runs yet')"
            )
            assert page.evaluate("location.hash") == "#dag/plain"
            page.click("#dgTrigger")
            e2e.wait_toast(page, "triggered plain")
            # the new run is selected and the view moves to its tasks
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="tasks"]'
            )
            _wait_task_state(page, "publish", "success")
            run_key = daemon.api("GET", "/dags/plain/runs")[1]["runs"][0][
                "runKey"
            ]
            assert page.evaluate("location.hash") == ("#dag/plain/" + run_key)
            # The Runs tab lists both runs, newest first.
            page.click("#dgTrigger")
            page.wait_for_function(
                "document.querySelectorAll('#dgRuns tr.dagrun').length === 2"
            )
            _tab(page, "runs")
            newest = page.get_attribute("#dgRuns tr.dagrun", "data-runkey")
            assert newest != run_key
            # selecting the older run switches the detail to it
            page.click('#dgRuns tr[data-runkey="{}"]'.format(run_key))
            page.wait_for_function(
                "(h) => location.hash === h", arg="#dag/plain/" + run_key
            )


def test_trigger_failures_are_reported(browser, tmp_path):
    dags = [e2e.diamond_dag("plain", gate=False)]
    with e2e.Daemon(tmp_path, dags=dags) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.wait_for_selector('#dagRows [data-dagtrigger="plain"]')
            for code, toast in (
                (404, "no such dag"),
                (500, "trigger failed (HTTP 500)"),
            ):
                page.faults.status(r"/dags/plain/trigger", code)
                _click(page, '#dagRows [data-dagtrigger="plain"]')
                assert "err" in e2e.wait_toast(page, toast)
                page.faults.clear(r"/dags/plain/trigger")
            page.faults.abort(r"/dags/plain/trigger")
            _click(page, '#dagRows [data-dagtrigger="plain"]')
            e2e.wait_toast(page, "trigger failed")
        assert daemon.api("GET", "/dags/plain/runs")[1]["runs"] == []


def test_backfill_creates_real_runs(browser, tmp_path):
    nightly = e2e.diamond_dag("nightly", gate=False)
    nightly["schedule"] = "0 0 1 1 *"
    with e2e.Daemon(tmp_path, dags=[nightly]) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "nightly")
            assert not page.is_visible("#dgBackfill")
            page.click("#dgBackfillBtn")
            page.wait_for_function("document.activeElement.id === 'dgBfFrom'")
            # a scheduled DAG can backfill or preview failed dates
            assert page.is_enabled("#dgBfFailed")
            assert page.inner_text("#dgBfGo") == "run backfill"
            page.click("#dgBfGo")
            assert "err" in e2e.wait_toast(
                page, "from and to ISO dates are required"
            )
            page.fill("#dgBfFrom", "bogus")
            page.fill("#dgBfTo", "2026-01-02")
            page.click("#dgBfGo")
            assert "err" in e2e.wait_toast(page, "backfill: bad date range")
            assert page.is_visible("#dgBackfill")
            page.fill("#dgBfFrom", "2025-12-30")
            page.click("#dgBfGo")
            assert "ok" in e2e.wait_toast(page, "▦ backfill queued (1 run)")
            assert _posts(page)[-1] == (
                "/dags/nightly/backfill",
                {"from": "2025-12-30", "to": "2026-01-02"},
            )
            page.wait_for_function(
                "document.getElementById('dgBackfill').style.display === "
                "'none'"
            )
            page.wait_for_selector("#dgRuns tr.dagrun")
            assert page.text_content("#dgRuns .kbadge") == "backfill"
            assert "2026-01-01 00:00:00" in page.inner_text("#dgRuns")
        runs = daemon.api("GET", "/dags/nightly/runs")[1]["runs"]
        assert [r["kind"] for r in runs] == ["backfill"]


# --------------------------------------------------------------------------
# recovery
# --------------------------------------------------------------------------


def _failed_daemon(tmp_path):
    daemon = e2e.Daemon(
        tmp_path, dags=[e2e.diamond_dag("broken", gate=False, fail=True)]
    )
    daemon.start()
    run_key = daemon.trigger_dag("broken")
    daemon.wait_dag_state("broken", run_key, "failed")
    return daemon, run_key


def test_recovery_preview_and_start(browser, tmp_path):
    daemon, run_key = _failed_daemon(tmp_path)
    try:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "broken")
            page.wait_for_selector("#dgRuns tr.dagrun .rpill.failed")
            _tab(page, "tasks")
            _wait_task_state(page, "right", "failed")
            assert _task_states(page)["publish"] == "dependency failed"
            assert "command exited with code 1" in page.inner_text("#dgTasks")
            # a manual DAG offers failed-date recovery only
            page.click("#dgBackfillBtn")
            assert page.is_checked("#dgBfFailed")
            assert page.is_disabled("#dgBfFailed")
            assert page.inner_text("#dgBfGo") == "Preview failed dates"
            page.click("#dgBackfillBtn")

            page.wait_for_function(
                "!document.getElementById('dgRecoverBtn').disabled"
            )
            page.click("#dgRecoverBtn")
            page.wait_for_selector("#dgRecoveryGo")
            text = page.inner_text("#dgRecovery")
            assert "2 tasks will run; 2 task results will be reused" in text
            assert "across 1 run." in text
            page.click("#dgRecovery summary")
            detail = page.inner_text("#dgRecovery details")
            assert "Run: publish, right" in detail
            assert "Reuse: extract, left" in detail
            assert "Artifacts: extract/rows" in detail
            recover = "/dags/broken/runs/{}/recover".format(run_key)
            assert _posts(page) == [
                (recover, {"mode": "failed", "dryRun": True})
            ]
            # Dismiss the preview, then preview recovery from one task.
            page.click("#dgRecoveryDismiss")
            assert not page.is_visible("#dgRecovery")
            page.click('#dgTasks [data-recover-task="left"]')
            page.wait_for_selector("#dgRecoveryGo")
            assert _posts(page)[-1] == (
                recover,
                {"mode": "from", "tasks": ["left"], "dryRun": True},
            )
            page.click("#dgRecoveryDismiss")

            page.click("#dgRecoverBtn")
            page.wait_for_selector("#dgRecoveryGo")
            page.click("#dgRecoveryGo")
            assert "ok" in e2e.wait_toast(page, "Recovery started · 1 run")
            body = _posts(page)[-1][1]
            assert body["dryRun"] is False
            assert len(body["planToken"]) == 64
            assert body["allowConfigChange"] is False
            # the new run is selected; its reused tasks link to the source
            page.wait_for_function(
                "location.hash.startsWith('#dag/broken/recovery-')"
            )
            page.wait_for_function(
                "document.getElementById('dgMeta').textContent"
                ".includes('2 tasks rerun · 2 results reused')"
            )
            page.wait_for_function(
                "document.getElementById('dgTasks').textContent"
                ".includes('Reused result')"
            )
            assert page.is_visible("#dgRecoveryResults")
            # "Source run" opens the failed run.
            page.click("#dgMeta [data-source-run]")
            page.wait_for_function(
                "(h) => location.hash === h",
                arg="#dag/broken/" + run_key,
            )
        runs = daemon.api("GET", "/dags/broken/runs")[1]["runs"]
        assert sorted(r["kind"] for r in runs) == ["manual", "recovery"]
    finally:
        daemon.stop()


def test_recovery_with_a_stale_plan_shows_the_409_and_retries(
    browser, tmp_path
):
    """Display a stale-plan error and keep the recovery review open.

    The daemon rejects a plan whose token no longer matches. The page offers
    a retry, and a new preview lets the operator start recovery.
    """
    daemon, run_key = _failed_daemon(tmp_path)
    try:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.record()
            _open_dag(page, "broken")
            page.wait_for_selector("#dgRuns tr.dagrun .rpill.failed")
            page.wait_for_function(
                "!document.getElementById('dgRecoverBtn').disabled"
            )
            # Replace the preview token in the request to simulate a plan
            # that the daemon has invalidated.
            handler = page.faults.rewrite(
                r"/dags/broken/runs/[^/]+/recover",
                lambda body: dict(body, planToken="0" * 64),
            )
            page.click("#dgRecoverBtn")
            page.wait_for_selector("#dgRecoveryGo")
            page.faults.clear(r"/dags/broken/runs/[^/]+/recover", handler)
            page.click("#dgRecoveryGo")
            assert "err" in e2e.wait_toast(
                page, "recovery preview is stale; preview again"
            )
            page.wait_for_selector("#dgRecoveryError")
            assert page.get_attribute("#dgRecoveryError", "role") == "alert"
            assert "preview is stale" in page.inner_text("#dgRecoveryError")
            assert page.inner_text("#dgRecoveryGo") == (
                "Retry reviewed recovery"
            )
            assert page.is_enabled("#dgRecoveryGo")
            # Retry sends the same reviewed request.
            page.click("#dgRecoveryGo")
            page.wait_for_function(
                "(n) => window.performance.getEntriesByType('resource')"
                ".filter((e) => e.name.endsWith('/recover')).length >= n",
                arg=3,
            )
            sent = [b for p, b in _posts(page) if not b["dryRun"]]
            assert len(sent) == 2 and sent[0] == sent[1]
            # A new preview provides a valid token, so recovery starts.
            page.click("#dgRecoveryDismiss")
            page.click("#dgRecoverBtn")
            page.wait_for_selector("#dgRecoveryGo")
            page.click("#dgRecoveryGo")
            assert "ok" in e2e.wait_toast(page, "Recovery started · 1 run")
        runs = daemon.api("GET", "/dags/broken/runs")[1]["runs"]
        assert sorted(r["kind"] for r in runs) == ["manual", "recovery"]
    finally:
        daemon.stop()


# --------------------------------------------------------------------------
# deep links
# --------------------------------------------------------------------------


def test_dag_deep_links(browser, tmp_path):
    with e2e.Daemon(tmp_path, dags=[e2e.diamond_dag("diamond")]) as daemon:
        first = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", first)
        second = daemon.trigger_dag("diamond")
        daemon.wait_gate("diamond", second)
        # a DAG link opens the runs tab on the newest run
        with e2e.open_page(browser, daemon.url + "#dag/diamond") as page:
            page.wait_for_selector('#dagDrawer[aria-hidden="false"]')
            assert page.inner_text("#dgName") == "diamond"
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="runs"]'
            )
            page.wait_for_function(
                "document.querySelectorAll('#dgRuns tr.dagrun').length === 2"
            )
        # a run link opens that run's tasks, not the newest run's
        url = daemon.url + "#dag/diamond/" + first
        with e2e.open_page(browser, url) as page:
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="tasks"]'
            )
            _wait_task_state(page, "gate", "running")
            assert (
                page.get_attribute("#dgRuns tr.dagrun.active", "data-runkey")
                == first
            )
            assert page.evaluate("location.hash") == "#dag/diamond/" + first
            # approving here decides the linked run, not the newest
            page.faults.record()
            page.click('#dgTasks [data-approve="gate"]')
            e2e.wait_toast(page, "approved gate")
            assert _posts(page)[0][0] == (
                "/dags/diamond/runs/{}/tasks/gate/decision".format(first)
            )
            # Changing the URL fragment opens the linked run when no job
            # drawer is open.
            page.evaluate("(h) => { location.hash = h; }", "#dag/diamond")
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="runs"]'
            )
        assert daemon.dag_run("diamond", second)["state"] == "running"
        # an unknown DAG opens nothing
        with e2e.open_page(browser, daemon.url + "#dag/nope") as page:
            page.wait_for_selector("#dagRows tr[data-dag]")
            assert page.get_attribute("#dagDrawer", "aria-hidden") == "true"
