"""Verify that untrusted data renders as text throughout the dashboard.

Run ``cronstable/web/index.html`` in Chromium against ``tests/_web_e2e.py``.
Because the page uses ``innerHTML``, values from servers, cluster peers,
job output, and URLs must not create unintended markup or execute scripts.
Tests supply untrusted data through two sources:

* Configuration and job output: Names, commands, and log lines include
  markup, attribute delimiters, ``javascript:`` URLs, bidirectional and
  zero-width characters, and names several kilobytes long.
* Modified API responses: ``page.route`` adds injection payloads to strings
  and selected numeric fields. This simulates data from a compromised
  server, a different server version, or a cluster peer that has not passed
  local configuration validation.

Each check verifies that ``window.__xss`` remains unset and that no payload
creates elements, event handler attributes, or script links. The original
payload must remain visible as text.
"""

import copy
import json
import re
import urllib.parse

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402

# Breaks out of element text, a "double-quoted" and a 'single-quoted'
# attribute value; the img fires onerror (the page's CSP allows inline
# handlers, and /x is a same-origin 404), the markers make a parsed
# payload visible even where no handler runs.
PAYLOAD = (
    'x"\'><img src=x data-xss=1 onerror="window.__xss=(window.__xss||0)+1">'
    "<b data-xss=2>"
)

IMG = "<img src=x onerror=window.__xss=1>"
SCRIPT = '"><script>window.__xss=1</script>'
ATTR = "' onmouseover='window.__xss=1"
JSURL = "javascript:window.__xss=1"
BIDI = "‮evil​<b data-xss=3>bidi</b>"
LONG = "A" * 3000 + "<i data-xss=4>long</i>"
ANSI = "\x1b[31m<img src=x onerror=window.__xss=1>\x1b[0m"

HOSTILE_NAMES = [IMG, SCRIPT, ATTR, JSURL, BIDI, LONG]

# every hostile line a job prints; python keeps the bytes exact
_PRINT_HOSTILE = (
    "import sys\n"
    "E = chr(27)\n"
    'print("<img src=x onerror=window.__xss=1>needle")\n'
    'print(E + "[31m<img src=x onerror=window.__xss=1>" + E + "[0mneedle")\n'
    'print(chr(34) + "><script>window.__xss=1</script>needle")\n'
    'print("needle<b data-xss=5>bold</b>", file=sys.stderr)\n'
)

_PROBE = """
() => {
  const bad = [];
  const where = (el) => {
    const host = el.closest("[id]");
    return (host ? "#" + host.id : "?") + " " +
      el.outerHTML.slice(0, 160);
  };
  for (const el of document.querySelectorAll("[data-xss],[data-xssattr]"))
    bad.push("marker " + where(el));
  for (const el of document.querySelectorAll(
      "img,iframe,object,embed,form,base,link[rel=import]"))
    bad.push("element " + where(el));
  for (const el of document.querySelectorAll("*")) {
    for (const a of el.attributes) {
      if (/^on/i.test(a.name)) bad.push("handler " + where(el));
      if ((a.name === "href" || a.name === "src") &&
          /^\\s*javascript:/i.test(a.value)) bad.push("jsurl " + where(el));
    }
  }
  const scripts = document.querySelectorAll("script").length;
  if (scripts !== 3) bad.push("script count " + scripts);
  return { xss: window.__xss === undefined ? null : window.__xss, bad };
}
"""


def assert_clean(page, where):
    """No payload executed and none parsed into the DOM."""
    found = page.evaluate(_PROBE)
    assert found["xss"] is None, "{}: a payload executed".format(where)
    assert not found["bad"], "{}: {}".format(where, found["bad"][:6])


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


def _hostile_jobs():
    jobs = [
        e2e.job(name, e2e.py_cmd(_PRINT_HOSTILE) + " # " + IMG)
        for name in HOSTILE_NAMES
    ]
    # a second failing pair sharing an exit code: the verdict bar's
    # correlation headline and the mitigate console both list them
    jobs.append(e2e.job("fail-" + IMG, "echo '" + IMG + "' >&2; exit 7"))
    jobs.append(e2e.job("fail2-" + IMG, "echo boom >&2; exit 7"))
    return jobs


# All add-on panels open from the first paint.
_ALL_PANELS = {
    "radar": True,
    "week": True,
    "heat": True,
    "press": True,
    "stateInsp": True,
    "fleet": True,
    "swim": True,
    "nodeCard": True,
    "ledger": True,
    "pollMs": 1000,
}


def _click(page, selector):
    page.evaluate("(s) => document.querySelector(s).click()", selector)


def _wait_overlay(page, overlay_id, is_open=True):
    page.wait_for_function(
        "([id, open]) => document.getElementById(id).classList"
        ".contains('open') === open",
        arg=[overlay_id, is_open],
    )


def _tour_job_drawer(page, name, where):
    """Open one job's drawer and walk its four tabs."""
    page.evaluate(
        """(name) => {
          const tr = [...document.querySelectorAll('#rows tr[data-job]')]
            .find((r) => r.getAttribute('data-job') === name);
          tr.querySelector('[data-logs]').click();
        }""",
        name,
    )
    page.wait_for_selector('#drawer[aria-hidden="false"]')
    assert page.evaluate("document.getElementById('dName').textContent") == (
        name
    )
    for tab in ("history", "resources", "schedule", "logs"):
        _click(page, '#dTabs button[data-tab="{}"]'.format(tab))
        page.wait_for_selector('.pane.active[data-pane="{}"]'.format(tab))
        if tab == "history":
            page.wait_for_function(
                "!document.getElementById('historyPane').textContent"
                ".includes('loading')"
            )
        assert_clean(page, "{} drawer {}".format(where, tab))


def _tour_overlays(page, where):
    """Palette, timeline, mitigate, sandbox, multi-tail, settings, help,
    pairing and the token modal, each opened, probed and closed."""
    # command palette: every job and DAG contributes entries
    page.keyboard.press("Control+k")
    _wait_overlay(page, "paletteWrap")
    page.wait_for_selector("#paletteList .item")
    assert_clean(page, where + " palette")
    page.fill("#paletteInput", "img")
    assert_clean(page, where + " palette filtered")
    # the list re-renders synchronously on input
    page.fill("#paletteInput", PAYLOAD)
    assert_clean(page, where + " palette hostile query")
    page.keyboard.press("Escape")
    _wait_overlay(page, "paletteWrap", False)

    # incident timeline; shortcuts are inert while a field has focus, and
    # a closed surface hands focus back to whatever held it before
    page.evaluate("document.activeElement.blur()")
    page.keyboard.press("i")
    _wait_overlay(page, "timelineWrap")
    assert_clean(page, where + " timeline")
    page.keyboard.press("Escape")
    _wait_overlay(page, "timelineWrap", False)

    # mitigate console, when a failing job gives it something to list (a
    # cluster alert alone shows the bar with an empty incident set)
    if page.evaluate(
        "document.getElementById('verdictBar').style.display !== 'none' && "
        "!!document.querySelector('#rows .st.fail')"
    ):
        assert_clean(page, where + " verdict bar")
        _click(page, "#vMitigate")
        _wait_overlay(page, "mitigateWrap")
        assert_clean(page, where + " mitigate")
        page.keyboard.press("Escape")
        _wait_overlay(page, "mitigateWrap", False)

    # multi-tail: every job added, chips and the datalist render names
    _click(page, "#tailBtn")
    _wait_overlay(page, "tailWrap")
    assert_clean(page, where + " tail empty")
    page.keyboard.press("Escape")
    _wait_overlay(page, "tailWrap", False)

    # settings, help, pairing, token modal
    _click(page, "#settingsBtn")
    _wait_overlay(page, "settingsWrap")
    assert_clean(page, where + " settings")
    _click(page, "#openPair")
    _wait_overlay(page, "pairWrap")
    page.wait_for_selector("#pairQr svg")
    assert_clean(page, where + " pair")
    page.keyboard.press("Escape")
    _wait_overlay(page, "pairWrap", False)
    page.evaluate("document.activeElement.blur()")
    page.keyboard.press("?")
    _wait_overlay(page, "helpWrap")
    assert_clean(page, where + " help")
    page.keyboard.press("Escape")
    _click(page, "#authBtn")
    _wait_overlay(page, "modalWrap")
    assert_clean(page, where + " modal")
    page.keyboard.press("Escape")
    _wait_overlay(page, "modalWrap", False)


def _tour_wallboard(page, where):
    page.evaluate("document.activeElement.blur()")
    page.keyboard.press("w")
    page.wait_for_function("document.body.classList.contains('tv')")
    page.wait_for_selector("#wbGrid > *")
    assert_clean(page, where + " wallboard")
    page.keyboard.press("Escape")
    page.wait_for_function("!document.body.classList.contains('tv')")


# --------------------------------------------------------------------------
# 1. hostile configuration and output, served by the real daemon
# --------------------------------------------------------------------------


def test_hostile_job_names_and_commands_render_literally(browser, tmp_path):
    with e2e.Daemon(tmp_path, jobs=_hostile_jobs()) as daemon:
        for name in ("fail-" + IMG, "fail2-" + IMG):
            daemon.api(
                "POST", "/jobs/" + urllib.parse.quote(name, safe="") + "/start"
            )
        e2e.wait_until(
            lambda: all(
                (j.get("last_run") or {}).get("outcome") == "failure"
                for n, j in daemon.jobs().items()
                if n.startswith("fail")
            )
        )
        with e2e.open_page(browser, daemon.url, prefs=_ALL_PANELS) as page:
            page.wait_for_function(
                "document.querySelectorAll('#rows tr[data-job]').length === 8"
            )
            assert sorted(e2e.row_names(page)) == sorted(
                HOSTILE_NAMES + ["fail-" + IMG, "fail2-" + IMG]
            )
            # the literal text is on screen: name and command cells
            cells = page.evaluate(
                "[...document.querySelectorAll('#rows .jobname')]"
                ".map((d) => d.textContent)"
            )
            assert IMG in cells and SCRIPT in cells and ATTR in cells
            assert LONG in cells
            assert page.evaluate(
                "[...document.querySelectorAll('#rows .jobcmd')]"
                ".every((d) => d.getAttribute('title') === d.textContent)"
            )
            assert_clean(page, "table")
            # the tab title rotates through the failing jobs' names, as text
            page.wait_for_function("document.title.includes('<img')")
            for name in (IMG, SCRIPT, ATTR, BIDI, "fail-" + IMG):
                _tour_job_drawer(page, name, "config")
                page.keyboard.press("Escape")
                page.wait_for_selector('#drawer[aria-hidden="true"]')
            _tour_overlays(page, "config")
            _tour_wallboard(page, "config")
            # keyboard selection builds a CSS selector from the name
            for _ in range(9):
                page.keyboard.press("j")
            assert_clean(page, "selection")
            assert (
                page.evaluate(
                    "document.querySelectorAll('#rows tr.sel').length"
                )
                == 1
            )


def test_hostile_log_lines_with_and_without_search(browser, tmp_path):
    jobs = [e2e.job("printer", e2e.py_cmd(_PRINT_HOSTILE))]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        # stderr output counts as a failure by default; the run still
        # leaves its four lines in the buffer the tail replays
        daemon.run_and_wait("printer")
        with e2e.open_page(browser, daemon.url) as page:
            _click(page, '#rows [data-logs="printer"]')
            page.wait_for_function(
                "document.querySelectorAll('#term .ln').length >= 4"
            )
            text = page.evaluate("document.getElementById('term').innerText")
            assert "<img src=x onerror=window.__xss=1>needle" in text
            assert '"><script>window.__xss=1</script>needle' in text
            assert "needle<b data-xss=5>bold</b>" in text
            assert_clean(page, "log plain")
            # the ANSI line keeps its color span and escapes its text
            assert page.evaluate(
                "[...document.querySelectorAll('#term .ln span[style]')]"
                ".some((s) => s.textContent.includes('<img'))"
            )
            # a plain-text search splices <mark> around the hit
            page.fill("#logSearch", "needle")
            page.wait_for_function(
                "document.getElementById('logCount').textContent === "
                "'4 matches'"
            )
            assert (
                page.evaluate("document.querySelectorAll('#term mark').length")
                == 4
            )
            assert_clean(page, "log text search")
            # a search for the markup itself marks the literal characters
            page.fill("#logSearch", "<img src=x")
            page.wait_for_function(
                "document.querySelectorAll('#term mark').length === 2"
            )
            assert page.evaluate(
                "[...document.querySelectorAll('#term mark')]"
                ".every((m) => m.textContent === '<img src=x')"
            )
            assert_clean(page, "log markup search")
            # regex mode, including a pattern matching inside the markup
            page.check("#optRegex")
            page.fill("#logSearch", "<[a-z]+|needle|>")
            page.wait_for_function(
                "document.querySelectorAll('#term mark').length > 8"
            )
            assert_clean(page, "log regex search")
            # zero-width matches interleave marks with every character
            page.fill("#logSearch", "x*")
            page.wait_for_function(
                "document.getElementById('logCount').textContent"
                ".endsWith('matches')"
            )
            assert_clean(page, "log zero-width regex")
            # the hostile pattern itself, as text and as a regex
            page.fill("#logSearch", PAYLOAD)
            assert_clean(page, "log hostile query")
            page.uncheck("#optRegex")
            page.fill("#logSearch", PAYLOAD)
            assert_clean(page, "log hostile text query")
            # ansi off strips the escape and still escapes the text
            page.fill("#logSearch", "")
            page.uncheck("#optAnsi")
            page.wait_for_function(
                "!document.querySelector('#term .ln span[style]')"
            )
            assert_clean(page, "log ansi off")
            page.check("#optTs")
            assert_clean(page, "log timestamps")
            page.keyboard.press("Escape")

            # the same lines through the merged multi-tail, with a search
            _click(page, "#tailBtn")
            _wait_overlay(page, "tailWrap")
            page.fill("#tailAddInput", "printer")
            page.press("#tailAddInput", "Enter")
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm .ln').length >= 4"
            )
            assert_clean(page, "tail plain")
            page.fill("#tailSearch", "<img")
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm mark').length === 2"
            )
            assert_clean(page, "tail search")
            page.fill("#tailSearch", PAYLOAD)
            page.wait_for_function(
                "document.getElementById('tailCount').textContent === "
                "'0 matches'"
            )
            assert_clean(page, "tail hostile search")


def test_hostile_stream_names_and_sse_payload_shapes(browser, tmp_path):
    """Frames a real daemon never sends: hostile stream names (they become
    a class attribute), non-string lines, and markup in the end reason."""
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            frames = [
                e2e.sse_line(PAYLOAD, stream=PAYLOAD),
                e2e.sse_line(PAYLOAD, stream="verify." + PAYLOAD),
                e2e.sse_frame("line", {"stream": "stdout", "line": 7}),
                e2e.sse_frame("line", {"stream": None, "line": [PAYLOAD]}),
                e2e.sse_line("tail " + PAYLOAD),
                e2e.sse_frame("end", {"reason": PAYLOAD}),
            ]
            page.faults.sse(r"^/jobs/[^/]+/logs$", frames, close=True)
            _click(page, '#rows [data-logs="alpha-ok"]')
            page.wait_for_function(
                "document.querySelectorAll('#term .ln').length >= 3"
            )
            assert_clean(page, "scripted stream")
            assert PAYLOAD in page.evaluate(
                "document.getElementById('term').innerText"
            )
            page.keyboard.press("Escape")
            _click(page, "#tailBtn")
            _wait_overlay(page, "tailWrap")
            page.fill("#tailAddInput", "alpha-ok")
            page.press("#tailAddInput", "Enter")
            page.wait_for_function(
                "document.querySelectorAll('#tailTerm .ln').length >= 3"
            )
            assert_clean(page, "scripted tail stream")


# --------------------------------------------------------------------------
# 2. poisoned API responses (a hostile server or peer)
# --------------------------------------------------------------------------

# Keys whose values select a render branch. The "branches" sweep leaves
# them intact so state-specific markup (failure rows, gates, leases) is
# built around poisoned neighbors; the "everything" sweep poisons them
# too, so a sink that trusts an enum is caught as well.
_ENUM_KEYS = {
    "outcome",
    "state",
    "status",
    "kind",
    "type",
    "backend",
    "distribution",
    "topology",
    "clusterPolicy",
    "concurrencyScope",
    "decision",
    "schedule",
    "timezone",
    "finished_at",
    "started_at",
    "ranAt",
    "until",
    "nextRetryAt",
    "as_of",
    "logicalDate",
    "expiry",
    "runKey",
    "sourceRunKey",
    "reusedFrom",
    "planToken",
    "id",
    "dependsOn",
    "dag",
}

# Keep arithmetic-only fields numeric so rendering reaches the string
# concatenation sites. A string here would cause an earlier exception;
# for example, NaN in a scheduled time prevents the jobs table from rendering.
_ARITHMETIC_KEYS = {"scheduled_in"}

_QUOTED = urllib.parse.quote(PAYLOAD, safe="")


def _strip_payload(url):
    """``url`` without the payload in its path and query values.

    The page sends poisoned identities (job, DAG and run names) back in
    request URLs; stripping them lets drill-downs reach the real daemon.
    """
    parts = urllib.parse.urlsplit(url)
    path = urllib.parse.unquote(parts.path).replace(PAYLOAD, "")
    query = urllib.parse.urlencode(
        [
            (k, v.replace(PAYLOAD, ""))
            for k, v in urllib.parse.parse_qsl(
                parts.query, keep_blank_values=True
            )
        ]
    )
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            urllib.parse.quote(path, safe="/"),
            query,
            "",
        )
    )


def _poison(value, *, numbers=False, keep=frozenset(), key=None):
    """``value`` with the payload appended to every string leaf.

    Leaves under a ``keep`` key stay intact; ``numbers`` turns numeric
    leaves into hostile strings as well.
    """
    if isinstance(value, dict):
        return {
            k: _poison(v, numbers=numbers, keep=keep, key=k)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_poison(v, numbers=numbers, keep=keep, key=key) for v in value]
    if key in keep or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value + PAYLOAD
    if numbers and isinstance(value, (int, float)):
        if key not in _ARITHMETIC_KEYS:
            return str(value) + PAYLOAD
    return value


def _zoo(jobs):
    """Append synthetic jobs covering every row chip and drawer branch."""
    base = copy.deepcopy(jobs[0])
    now = "2026-01-01T00:00:00+00:00"
    last = {
        "outcome": "failure",
        "exit_code": 9,
        "started_at": now,
        "finished_at": now,
        "duration": 1.5,
        "fail_reason": "reason",
        "skip_reason": None,
        "resources": {
            "cpu_total_seconds": 1,
            "cpu_user_seconds": 1,
            "cpu_system_seconds": 0,
            "max_rss_bytes": 1024,
        },
    }

    def variant(name, **over):
        j = copy.deepcopy(base)
        j.update(name=name, last_run=copy.deepcopy(last), history=[last])
        j.update(over)
        return j

    unknown = copy.deepcopy(last)
    unknown["outcome"] = "unknown"
    return jobs + [
        variant(
            "zoo-running",
            running=True,
            pids=[1, 2],
            running_resources={"cpu_percent": 12.5, "rss_bytes": 4096},
            verification={"configured": True, "running": True},
        ),
        variant(
            "zoo-paused",
            paused={"until": now, "note": "note", "by": "someone"},
        ),
        variant(
            "zoo-retry",
            retry={"attempt": 2, "maxAttempts": 5, "nextRetryAt": now},
        ),
        variant(
            "zoo-slot",
            concurrencyScope="cluster",
            slot={"held": True, "holder": "holder", "refs": 3},
        ),
        variant("zoo-reboot", rebootPending=True),
        variant("zoo-unknown", last_run=unknown),
        variant(
            "zoo-late",
            sla={"state": "late", "breaches": [{"check": "maxAge"}]},
        ),
        variant("zoo-owned", clusterPolicy="Leader", clusterOwner="node-b"),
        variant("zoo-every", clusterPolicy="EveryNode"),
        variant(
            "zoo-queued",
            pool={
                "name": "pool",
                "slots": 1,
                "priority": 0,
                "queued": [{"id": "q1", "job": "zoo-queued"}],
            },
        ),
        variant("zoo-tz", timezone="Europe/Berlin", utc=False),
    ]


_CLUSTER = {
    "enabled": True,
    "backend": "gossip",
    "node_name": "node-a",
    "distribution": "spread",
    "elect_leader": True,
    "quorate": False,
    "quorum": 2,
    "is_leader": False,
    "leader": "node-b",
    "conflict": True,
    "conflict_names": ["dup"],
    "size_conflict": True,
    "conflicting_sizes": ["3"],
    "cluster_size": 2,
    "policy_conflict": True,
    "conflicting_policies": ["spread/true"],
    "interval": 5,
    "node_stats": {"cpu_percent": 5, "mem_percent": 50},
    "peers": [
        {
            "host": "node-b:8443",
            "node_name": "node-b",
            "status": "agreed",
            "job_set_id": "v1:abcdef",
            "node_stats": {"cpu_percent": 5, "mem_percent": 50},
        },
        {
            "host": "node-c:8443",
            "node_name": "node-c",
            "status": "unreachable",
            "job_set_id": None,
        },
    ],
}

_LEASE_CLUSTER = {
    "enabled": True,
    "backend": "etcd",
    "node_name": "node-a",
    "elect_leader": True,
    "quorate": True,
    "is_leader": False,
    "leader": "node-b",
    "fleet": False,
    "lease": {
        "holder": "node-b",
        "expiry": "2030-01-01T00:00:00+00:00",
        "fence": 4,
        "electionName": "election",
        "identity": "node-a",
        "path": "/election",
        "extra": "extra",
    },
}


def _fleet(jobs):
    def cell(outcome):
        return {
            "running": outcome == "running",
            "enabled": True,
            "scheduled_in": 60,
            "last": {
                "outcome": outcome,
                "finished_at": "2026-01-01T00:00:00+00:00",
                "exit_code": 1,
                "duration": 1.5,
            },
        }

    names = [j["name"] for j in jobs][:4] + ["peer-only"]
    return {
        "enabled": True,
        "backend": "gossip",
        "node_name": "node-a",
        "distribution": "spread",
        "elect_leader": True,
        "interval": 5,
        "nodes": [
            {
                "node_name": "node-a",
                "host": None,
                "self": True,
                "status": "self",
                "as_of": "2026-01-01T00:00:00+00:00",
                "truncated": True,
                "jobs": {n: cell("failure") for n in names},
            },
            {
                "node_name": "node-b",
                "host": "node-b:8443",
                "self": False,
                "status": "agreed",
                "as_of": "2026-01-01T00:00:00+00:00",
                "truncated": False,
                "jobs": {n: cell("success") for n in names},
            },
        ],
    }


class _Poisoner:
    """Routes every API response through :func:`_poison`.

    ``mode`` is ``"everything"`` or ``"branches"`` (enum-like keys kept);
    ``numbers`` names the one endpoint prefix whose numbers are poisoned
    as well. Requests carrying a poisoned identity have the payload
    stripped before they reach the daemon, so drill-downs still answer.
    """

    def __init__(self, page, mode, numbers=None, cluster=None):
        self.page = page
        self.keep = _ENUM_KEYS if mode == "branches" else frozenset()
        self.numbers = numbers
        self.cluster = cluster
        self.jobs = []
        page.route(
            re.compile(r"^https?://[^/]+/.+"), e2e.quiet_route(self._handle)
        )

    def _handle(self, route):
        request = route.request
        url = _strip_payload(request.url)
        path = re.sub(r"^https?://[^/]+", "", url).split("?")[0]
        if path.endswith("/logs"):
            # log tails are scripted through the SSE shim instead
            return route.fallback()
        if path == "/cluster" and self.cluster is not None:
            return self._fulfill(route, path, copy.deepcopy(self.cluster))
        if path == "/fleet" and self.cluster is not None:
            return self._fulfill(route, path, _fleet(self.jobs))
        response = route.fetch(url=url)
        ctype = response.headers.get("content-type", "")
        if "json" not in ctype or request.method != "GET":
            return route.fulfill(response=response)
        try:
            body = json.loads(response.text())
        except ValueError:
            return route.fulfill(response=response)
        if path == "/jobs" and isinstance(body, list):
            body = _zoo(body)
            self.jobs = body
        return self._fulfill(route, path, body, response)

    def _fulfill(self, route, path, body, response=None):
        numbers = self.numbers is not None and path.startswith(self.numbers)
        poisoned = _poison(body, numbers=numbers, keep=self.keep)
        kwargs = {"response": response} if response is not None else {}
        return route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(poisoned),
            **kwargs,
        )


def _dag_daemon(tmp_path):
    daemon = e2e.Daemon(
        tmp_path,
        jobs=e2e.default_jobs()
        + [e2e.job("pooled", "echo pooled", pool="db")],
        dags=[
            e2e.diamond_dag("diamond"),
            e2e.diamond_dag("broken", gate=False, fail=True),
        ],
        pools={"db": {"slots": 1}},
    )
    daemon.start()
    daemon.run_and_wait("alpha-ok", outcome="success")
    daemon.run_and_wait("beta-fail", outcome="failure")
    gate_run = daemon.trigger_dag("diamond")
    daemon.wait_gate("diamond", gate_run)
    failed_run = daemon.trigger_dag("broken")
    daemon.wait_dag_state("broken", failed_run, "failed")
    return daemon


# The poisoned payloads break arithmetic and date parsing on purpose; the
# page may log the resulting errors. What it must never do is run or
# parse the payload, which assert_clean checks at every stop.
_POISON_NOISE = (
    r"Invalid time value",
    r"is not a function",
    r"is not iterable",
    r"Cannot read properties",
    r"is not valid JSON",
    r"Invalid (date|array length|count)",
    r"toFixed",
    r"RangeError",
    r"TypeError",
    r"<path> attribute d",
    r"Error: <\w+> attribute",
)


def _poison_tour(page, where):
    page.wait_for_selector("#rows tr[data-job]")
    page.wait_for_selector("#dagRows tr[data-dag]")
    assert_clean(page, where + " first paint")
    # job drawers: a real job and the synthetic states
    for index in (0, 1):
        page.evaluate(
            "(i) => document.querySelectorAll('#rows [data-logs]')[i].click()",
            index,
        )
        page.wait_for_selector('#drawer[aria-hidden="false"]')
        for tab in ("history", "resources", "schedule", "logs"):
            _click(page, '#dTabs button[data-tab="{}"]'.format(tab))
            page.wait_for_selector('.pane.active[data-pane="{}"]'.format(tab))
            assert_clean(page, "{} job drawer {}".format(where, tab))
        page.keyboard.press("Escape")
        page.wait_for_selector('#drawer[aria-hidden="true"]')
    # DAG drawers: every tab, for the gated and the failed run
    count = page.evaluate(
        "document.querySelectorAll('#dagRows [data-dagopen]').length"
    )
    for index in range(count):
        page.evaluate(
            "(i) => document.querySelectorAll('#dagRows [data-dagopen]')[i]"
            ".click()",
            index,
        )
        page.wait_for_selector('#dagDrawer[aria-hidden="false"]')
        page.wait_for_selector("#dgRuns tr.dagrun")
        assert_clean(page, where + " dag runs")
        for tab in ("tasks", "graph", "xcom", "logs", "runs"):
            _click(page, '#dagTabs button[data-dtab="{}"]'.format(tab))
            page.wait_for_selector(
                '#dagDrawer .dpane.active[data-dpane="{}"]'.format(tab)
            )
            assert_clean(page, "{} dag {}".format(where, tab))
        _click(page, "#dgBackfillBtn")
        assert_clean(page, where + " dag backfill")
        page.keyboard.press("Escape")
        page.wait_for_selector('#dagDrawer[aria-hidden="true"]')
    # state inspector: every tab, every scope drill-down
    tabs = page.evaluate(
        "[...document.querySelectorAll('#stateTabs button')]"
        ".map((b) => b.getAttribute('data-sttab'))"
    )
    for tab in tabs:
        page.evaluate(
            "(t) => [...document.querySelectorAll('#stateTabs button')]"
            ".find((b) => b.getAttribute('data-sttab') === t).click()",
            tab,
        )
        assert_clean(page, "{} state tab {}".format(where, tab[:20]))
        scopes = page.evaluate(
            "document.querySelectorAll('#stateBody [data-stscope]').length"
        )
        for index in range(min(scopes, 3)):
            page.evaluate(
                "(i) => document.querySelectorAll("
                "'#stateBody [data-stscope]')[i].click()",
                index,
            )
            page.wait_for_function(
                "!(document.getElementById('stateDetail') || {textContent:"
                " ''}).textContent.includes('Loading')"
            )
            assert_clean(page, "{} state detail {}".format(where, tab[:20]))
    # pools
    page.evaluate(
        "document.querySelectorAll('#poolBody details')"
        ".forEach((d) => { d.open = true; })"
    )
    assert_clean(page, where + " pools")
    _tour_overlays(page, where)
    _tour_wallboard(page, where)
    assert_clean(page, where + " end")


@pytest.mark.parametrize("mode", ["everything", "branches"])
def test_poisoned_strings_in_every_api_answer(browser, tmp_path, mode):
    daemon = _dag_daemon(tmp_path)
    try:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs=_ALL_PANELS,
            allow=_POISON_NOISE,
            before_goto=lambda page: _Poisoner(page, mode, cluster=_CLUSTER),
            wait_rows=False,
        ) as page:
            page.faults.sse(
                r"/logs$",
                [e2e.sse_line(PAYLOAD, stream="stderr")],
                close=True,
            )
            _poison_tour(page, mode)
            # the poisoned text is on screen, as text
            assert "data-xss" in page.evaluate("document.body.innerText")
    finally:
        daemon.stop()


@pytest.mark.parametrize(
    "prefix",
    ["/jobs", "/dags", "/state", "/pools", "/cluster", "/fleet", "/node"],
)
def test_poisoned_numbers_per_endpoint(browser, tmp_path, prefix):
    """Counters, sizes and timestamps arrive as hostile strings.

    Numeric fields are the ones most often concatenated unescaped, since
    the local daemon only ever sends numbers there.
    """
    daemon = _dag_daemon(tmp_path)
    try:
        with e2e.open_page(
            browser,
            daemon.url,
            prefs=_ALL_PANELS,
            allow=_POISON_NOISE,
            before_goto=lambda page: _Poisoner(
                page, "branches", numbers=prefix, cluster=_CLUSTER
            ),
            wait_rows=False,
        ) as page:
            page.faults.sse(r"/logs$", [e2e.sse_line(PAYLOAD)], close=True)
            _poison_tour(page, "numbers " + prefix)
    finally:
        daemon.stop()


def test_poisoned_lease_backend_cluster_card(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(
            browser,
            daemon.url,
            allow=_POISON_NOISE,
            before_goto=lambda page: _Poisoner(
                page, "branches", numbers="/cluster", cluster=_LEASE_CLUSTER
            ),
        ) as page:
            page.wait_for_selector("#clusterLeaseRows tr")
            assert_clean(page, "lease cluster card")
            assert PAYLOAD in page.evaluate(
                "document.getElementById('clusterLeaseRows').innerText"
            )


# --------------------------------------------------------------------------
# 3. attacker-controlled URLs, inputs and error strings
# --------------------------------------------------------------------------


def test_hash_deep_links_with_hostile_names(browser, tmp_path):
    jobs = [e2e.job(IMG), e2e.job(SCRIPT), e2e.job("plain")]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        for name in (IMG, SCRIPT):
            url = daemon.url + "#job/" + urllib.parse.quote(name, safe="")
            with e2e.open_page(browser, url) as page:
                page.wait_for_selector('#drawer[aria-hidden="false"]')
                assert (
                    page.evaluate(
                        "document.getElementById('dName').textContent"
                    )
                    == name
                )
                assert_clean(page, "job deep link")
        # names no job or DAG carries open nothing and inject nothing
        for fragment in (
            "#job/" + urllib.parse.quote(PAYLOAD, safe=""),
            "#dag/" + urllib.parse.quote(PAYLOAD, safe=""),
            "#dag/" + _QUOTED + "/" + _QUOTED,
            "#" + PAYLOAD,
        ):
            with e2e.open_page(browser, daemon.url + fragment) as page:
                page.wait_for_function(
                    "document.getElementById('conn').textContent === 'live'"
                )
                assert_clean(page, "unknown deep link " + fragment[:12])
                assert (
                    page.evaluate(
                        "document.getElementById('drawer')"
                        ".getAttribute('aria-hidden')"
                    )
                    == "true"
                )


def test_malformed_percent_escape_in_hash_is_ignored(browser, tmp_path):
    """``#job/%E0%A4%A`` cannot be URI-decoded; the page stays up."""
    with e2e.Daemon(tmp_path) as daemon:
        for fragment in ("#job/%E0%A4%A", "#dag/%E0%A4%A", "#dag/x/%"):
            with e2e.open_page(browser, daemon.url + fragment) as page:
                page.wait_for_function(
                    "document.getElementById('conn').textContent === 'live'"
                )
                assert e2e.row_names(page)
                # a later, well-formed hash still opens its drawer
                page.evaluate("location.hash = '#job/alpha-ok'")
                page.wait_for_selector('#drawer[aria-hidden="false"]')


def test_server_error_strings_reach_toasts_as_text(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.status(
                r"/jobs/alpha-ok/start", 409, {"error": PAYLOAD}
            )
            _click(page, '#rows [data-run="alpha-ok"]')
            e2e.wait_toast(page, "data-xss")
            assert_clean(page, "409 toast")
            assert PAYLOAD in e2e.toasts(page)[0]


def test_dag_action_error_strings_reach_toasts_as_text(browser, tmp_path):
    daemon = _dag_daemon(tmp_path)
    try:
        with e2e.open_page(browser, daemon.url) as page:
            page.faults.status(
                r"/dags/[^/]+/backfill", 400, {"error": PAYLOAD}
            )
            page.faults.status(
                r"/dags/[^/]+(/runs/[^/]+)?/recover", 409, {"error": PAYLOAD}
            )
            _click(page, '#dagRows [data-dagopen="broken"]')
            page.wait_for_selector("#dgRuns tr.dagrun")
            _click(page, "#dgBackfillBtn")
            page.fill("#dgBfFrom", "2026-01-01")
            page.fill("#dgBfTo", "2026-01-02")
            _click(page, "#dgBfGo")
            e2e.wait_toast(page, "data-xss")
            assert_clean(page, "dag error toast")
    finally:
        daemon.stop()


def test_cron_sandbox_input_and_recents(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url) as page:
            page.keyboard.press("Control+k")
            page.fill("#paletteInput", "Schedule preview")
            page.keyboard.press("Enter")
            _wait_overlay(page, "sandboxWrap")
            for expr in (PAYLOAD, "*/5 * * * " + IMG, IMG + " * * * *"):
                page.fill("#sbxInput", expr)
                page.press("#sbxInput", "Enter")
                page.wait_for_function(
                    "(e) => [...document.querySelectorAll("
                    "'#sbxBody .sbx-recent .chip')]"
                    ".some((c) => c.textContent === e.trim())",
                    arg=expr,
                )
                assert_clean(page, "sandbox " + expr[:10])
            # the recents survive a reload out of localStorage
            page.reload()
            page.wait_for_selector("#rows tr[data-job]")
            page.keyboard.press("Control+k")
            page.fill("#paletteInput", "Schedule preview")
            page.keyboard.press("Enter")
            _wait_overlay(page, "sandboxWrap")
            page.fill("#sbxInput", "")
            page.wait_for_selector("#sbxBody .sbx-recent .chip")
            assert_clean(page, "sandbox recents after reload")
            assert PAYLOAD in page.evaluate(
                "[...document.querySelectorAll('#sbxBody .sbx-recent .chip')]"
                ".map((c) => c.textContent)"
            )


def test_hostile_stored_preferences(browser, tmp_path):
    """Treat localStorage as untrusted data from pages on the same origin."""
    hostile = {
        "theme": PAYLOAD,
        "font": PAYLOAD,
        "cvd": PAYLOAD,
        "scale": PAYLOAD,
        "heatWindow": PAYLOAD,
        "pressTz": PAYLOAD,
        "cols": {PAYLOAD: True},
        "heat": True,
        "press": True,
    }
    with e2e.Daemon(tmp_path) as daemon:
        with e2e.open_page(browser, daemon.url, prefs=hostile) as page:
            _click(page, "#settingsBtn")
            _wait_overlay(page, "settingsWrap")
            assert_clean(page, "hostile prefs")
            assert (
                page.evaluate(
                    "document.documentElement.getAttribute('data-theme')"
                )
                == "standard"
            )
            page.keyboard.press("Escape")
            _click(page, "#colsBtn")
            page.wait_for_selector("#colsMenu.open")
            assert_clean(page, "hostile cols pref")


def test_pair_panel_fields(browser, tmp_path):
    """Node name, token and link base all land in the pairing sheet."""
    cluster = copy.deepcopy(_CLUSTER)
    cluster["node_name"] = "nöde-" + IMG
    with e2e.Daemon(tmp_path, auth="full") as daemon:

        def routes(page):
            page.faults.json(r"/cluster", cluster)
            page.faults.rewrite(
                r"/whoami",
                lambda body: dict(body, pairLinkBase=JSURL + "//" + PAYLOAD),
            )

        with e2e.open_page(
            browser,
            daemon.url,
            token=e2e.FULL_TOKEN,
            before_goto=routes,
        ) as page:
            page.wait_for_function(
                "document.getElementById('clusterSummary').textContent"
                ".includes('nöde')"
            )
            _click(page, "#settingsBtn")
            _click(page, "#openPair")
            _wait_overlay(page, "pairWrap")
            page.wait_for_selector("#pairQr svg")
            payload = json.loads(
                page.evaluate(
                    "document.getElementById('pairPayload').textContent"
                )
            )
            assert payload["name"] == "nöde-" + IMG
            assert payload["token"] == e2e.FULL_TOKEN
            assert_clean(page, "pair sheet")
            # the QR is path data only: nothing from the link base is markup
            assert (
                page.evaluate(
                    "document.querySelectorAll('#pairQr svg *').length"
                )
                == 2
            )
