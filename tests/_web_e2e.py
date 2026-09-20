"""Share helpers for the dashboard browser tests in ``test_web_*_e2e.py``.

* Browser setup: ``browser_session`` starts synchronous Playwright and
  Chromium, honors ``CRONSTABLE_TEST_BROWSER_CHANNEL``, and skips tests if
  either dependency is unavailable. Modules reuse it through a fixture.
* Error collection: ``open_page`` records page and console errors and checks
  them when the context manager exits. Tests declare expected errors with
  ``allow``.
* Daemon setup: ``Daemon`` runs ``Cron.run`` on a separate thread from a YAML
  configuration in ``tmp_path``. The listener binds to port 0 and reports its
  assigned port. It serves ``cronstable/web/index.html`` and uses configured
  jobs, tokens, scopes, pools, DAGs, and state storage.
* Fault injection: ``Faults`` uses ``page.route`` to simulate error statuses,
  connection failures, pending requests, modified JSON, and controlled SSE
  responses.
* JavaScript coverage: If ``CRONSTABLE_JS_COVERAGE`` specifies a directory,
  each page records V8 coverage through the Chrome DevTools Protocol (CDP)
  and saves the raw results for the dashboard script.

This module provides helpers and context managers. Test modules define their
own fixtures, so setup affects only tests that request it.
"""

import asyncio
import contextlib
import json
import os
import pathlib
import re
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")

ROOT = pathlib.Path(__file__).resolve().parent.parent
INDEX = ROOT / "cronstable" / "web" / "index.html"
DEMO = ROOT / "docs" / "demo" / "index.html"

#: Allow for slow CI runners. Override CRONSTABLE_E2E_TIMEOUT_MS to change
#: the wait timeout during local development.
TIMEOUT_MS = int(os.environ.get("CRONSTABLE_E2E_TIMEOUT_MS", "30000"))

COVERAGE_ENV = "CRONSTABLE_JS_COVERAGE"
CHANNEL_ENV = "CRONSTABLE_TEST_BROWSER_CHANNEL"

# A schedule that never fires during a test run: jobs start only when a
# test (or the page under test) asks.
NEVER = "0 0 29 2 *"

# Console noise every page may produce: Chromium logs one error line per
# failed or non-2xx subresource load, and the tests inject those on
# purpose. Everything else at error level fails the test.
_DEFAULT_ALLOW = (r"Failed to load resource",)


# --------------------------------------------------------------------------
# browser
# --------------------------------------------------------------------------


@contextlib.contextmanager
def browser_session():
    """Start synchronous Playwright and Chromium, or skip if unavailable.

    Playwright is a development dependency. Chromium requires a separate
    download; tests skip if the browser is not installed.
    """
    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel=os.environ.get(CHANNEL_ENV))
        except Exception as exc:  # no chromium provisioned
            pytest.skip("playwright chromium unavailable: {}".format(exc))
        try:
            yield browser
        finally:
            browser.close()


class PageLog:
    """The ``pageerror`` and console-error record of one page."""

    def __init__(self, allow=()):
        self.page_errors = []
        self.console_errors = []
        self._allow = [re.compile(p) for p in _DEFAULT_ALLOW]
        self.allow(*allow)

    def allow(self, *patterns):
        """Expect error text matching any of ``patterns`` (regex search)."""
        self._allow.extend(re.compile(p) for p in patterns)

    def attach(self, page):
        page.on("pageerror", lambda e: self.page_errors.append(str(e)))
        page.on("console", self._on_console)

    def _on_console(self, msg):
        if msg.type == "error":
            self.console_errors.append(msg.text)

    def unexpected(self):
        found = self.page_errors + self.console_errors
        return [
            text
            for text in found
            if not any(p.search(text) for p in self._allow)
        ]

    def check(self):
        bad = self.unexpected()
        assert not bad, "unexpected page errors: {}".format(bad)


def _init_script(token, prefs, boot, extra):
    """Initialize tokens, preferences, and the boot screen.

    Set only missing keys so UI preference changes survive reloads.
    """
    parts = []
    if token is not None:
        parts.append(
            "if (sessionStorage.getItem('cronstable_token') === null) "
            "sessionStorage.setItem('cronstable_token', {});".format(
                json.dumps(token)
            )
        )
    merged = {} if boot else {"boot": False}
    merged.update(prefs or {})
    for key, value in merged.items():
        parts.append(
            "if (localStorage.getItem({k}) === null) "
            "localStorage.setItem({k}, {v});".format(
                k=json.dumps("cronstable." + key),
                v=json.dumps(json.dumps(value)),
            )
        )
    parts.extend(extra)
    return "\n".join(parts)


@contextlib.contextmanager
def open_page(
    browser,
    url,
    *,
    token=None,
    prefs=None,
    boot=False,
    init_scripts=(),
    allow=(),
    before_goto=None,
    wait_rows=True,
    coverage_name=None,
    **context_kwargs,
):
    """Open a new browser context and page, and check errors on exit.

    ``token`` initializes the sessionStorage token. ``prefs`` initializes
    localStorage preferences. Set ``boot`` to keep the boot screen; otherwise,
    keyboard input reaches the dashboard immediately.

    ``before_goto(page)`` runs before navigation so routes can intercept the
    first request. ``wait_rows`` waits for the first job row to render.
    The page exposes ``.log`` as a ``PageLog`` and ``.faults`` as ``Faults``.
    """
    context_kwargs.setdefault("viewport", {"width": 1440, "height": 1000})
    context = browser.new_context(**context_kwargs)
    context.set_default_timeout(TIMEOUT_MS)
    try:
        context.add_init_script(
            _init_script(token, prefs, boot, list(init_scripts))
        )
        page = context.new_page()
        _wrap_wait_for_function(page)
        log = PageLog(allow)
        log.attach(page)
        page.log = log
        page.faults = Faults(page)
        cov = _Coverage.start(page, coverage_name)
        if before_goto is not None:
            before_goto(page)
        page.goto(url)
        if wait_rows:
            page.wait_for_selector("#rows tr[data-job]")
        yield page
        if cov is not None:
            cov.dump()
        log.check()
    finally:
        # Closing the context can make pending route handlers fail during
        # a later Playwright call, including in another test. Remove routes
        # first and suppress errors from pending handlers.
        try:
            page.unroute_all(behavior="ignoreErrors")
        except Exception:  # the page never opened, or is already gone
            pass
        context.close()


_FUNCTION_SOURCE = re.compile(
    r"^\s*(async\s+)?(function\b|(\([^()]*\)|[\w$]+)\s*=>)"
)


def _wrap_wait_for_function(page):
    """Wrap expressions for ``page.wait_for_function`` to comply with CSP.

    Playwright calls functions directly but uses ``eval`` for expressions.
    The dashboard's Content Security Policy (CSP) excludes ``unsafe-eval``.
    Wrap expressions in arrow functions so they work with that policy.
    """
    original = page.wait_for_function

    def wait_for_function(expression, **kwargs):
        if not _FUNCTION_SOURCE.match(expression):
            expression = "() => (" + expression + ")"
        return original(expression, **kwargs)

    page.wait_for_function = wait_for_function


def wait_until(predicate, page=None, timeout=None, interval=0.02):
    """Poll a Python predicate until it is truthy and return its value.

    Use this for conditions outside the page, such as daemon state or route
    records. Pass ``page`` when waiting on route handlers: synchronous
    Playwright dispatches events only during its own calls. A Playwright wait
    between polls lets those handlers run; a Python sleep would block them.
    """
    if timeout is None:
        timeout = TIMEOUT_MS / 1000.0
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError("condition not met within the timeout")
        if page is not None:
            page.wait_for_timeout(interval * 1000)
        else:
            time.sleep(interval)


# --------------------------------------------------------------------------
# JS coverage (opt-in)
# --------------------------------------------------------------------------


class _Coverage:
    """V8 precise coverage for one page, dumped as raw CDP JSON."""

    _seq = 0

    def __init__(self, page, session, name):
        self._page = page
        self._session = session
        self._name = name

    @classmethod
    def start(cls, page, name):
        out = os.environ.get(COVERAGE_ENV)
        if not out:
            return None
        session = page.context.new_cdp_session(page)
        session.send("Profiler.enable")
        session.send(
            "Profiler.startPreciseCoverage",
            {"callCount": True, "detailed": True},
        )
        if name is None:
            name = os.environ.get("PYTEST_CURRENT_TEST", "page")
        return cls(page, session, name)

    def dump(self):
        out = pathlib.Path(os.environ[COVERAGE_ENV])
        out.mkdir(parents=True, exist_ok=True)
        try:
            result = self._session.send("Profiler.takePreciseCoverage")
        except Exception:  # page already gone: nothing to record
            return
        keep = [
            entry
            for entry in result.get("result", [])
            if _is_dashboard_url(entry.get("url", ""))
        ]
        _Coverage._seq += 1
        slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", self._name)[:150]
        path = out / "{}-{}-{}.json".format(slug, os.getpid(), _Coverage._seq)
        path.write_text(json.dumps({"test": self._name, "result": keep}))


def _is_dashboard_url(url):
    """Whether a V8 script URL is the dashboard document.

    Inline scripts report the URL of their document: the daemon's ``/``
    or the demo mirror's ``index.html``.
    """
    if url.startswith("http://127.0.0.1"):
        tail = url.split("/", 3)[-1] if url.count("/") >= 3 else ""
        return tail.split("#")[0].split("?")[0] == ""
    return url.startswith("file:") and url.split("#")[0].endswith(
        "demo/index.html"
    )


# --------------------------------------------------------------------------
# YAML emitter (strictyaml accepts block style only)
# --------------------------------------------------------------------------


def _scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    # JSON string escapes are valid inside a YAML double-quoted scalar
    return json.dumps(str(value), ensure_ascii=True)


def to_yaml(value, indent=0):
    """Block-style YAML for dicts, lists and scalars."""
    pad = " " * indent
    lines = []
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, (dict, list)) and item:
                lines.append("{}{}:".format(pad, _scalar(key)))
                lines.append(to_yaml(item, indent + 2))
            elif isinstance(item, (dict, list)):
                continue
            else:
                lines.append(
                    "{}{}: {}".format(pad, _scalar(key), _scalar(item))
                )
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                body = to_yaml(item, indent + 2)
                lines.append("{}- {}".format(pad, body.lstrip()))
            else:
                lines.append("{}- {}".format(pad, _scalar(item)))
    return "\n".join(lines)


# --------------------------------------------------------------------------
# the real daemon
# --------------------------------------------------------------------------

FULL_TOKEN = "full-token-e2e"
CONTROL_TOKEN = "control-token-e2e"
VIEW_TOKEN = "view-token-e2e"
APPROVE_TOKEN = "approve-token-e2e"

#: named token sets for ``Daemon(auth=...)``
AUTH_PRESETS = {
    # no auth middleware: every request is allowed
    "none": {},
    # one all-scopes token (web.authToken)
    "full": {"authToken": {"value": FULL_TOKEN}},
    # the all-scopes token plus one token per scope
    "scoped": {
        "authToken": {"value": FULL_TOKEN},
        "authTokens": [
            {"value": VIEW_TOKEN, "scopes": ["view"], "label": "viewer"},
            {
                "value": CONTROL_TOKEN,
                "scopes": ["control"],
                "label": "operator",
            },
            {
                "value": APPROVE_TOKEN,
                "scopes": ["approve"],
                "label": "approver",
            },
        ],
    },
    # a public-view instance: credential-less requests read, tokens act
    "public": {
        "authToken": {"value": FULL_TOKEN},
        "authTokens": [
            {"value": VIEW_TOKEN, "scopes": ["view"], "label": "viewer"},
        ],
        "anonymousScopes": ["view"],
    },
}


def py_cmd(code):
    """A shell command running ``code`` under this interpreter, unbuffered.

    ``code`` must hold no single quote.
    """
    assert "'" not in code
    return "'{}' -u -c '{}'".format(sys.executable, code)


def job(name, command="echo ok", schedule=NEVER, **extra):
    """One job mapping with stdout and stderr captured for the log tail."""
    spec = {
        "name": name,
        "command": command,
        "schedule": schedule,
        "captureStdout": True,
        "captureStderr": True,
    }
    spec.update(extra)
    return spec


def xcom_push_cmd(key, value_json):
    """A shell command publishing ``value_json`` as XCom ``key``.

    Runs the CLI through this interpreter, so the task needs no
    ``cronstable`` entry point on PATH. ``value_json`` must hold no
    single quote.
    """
    assert "'" not in value_json
    return "echo '{}' | '{}' -m cronstable xcom push --key {}".format(
        value_json, sys.executable, key
    )


def diamond_dag(name="diamond", gate=True, fail=False):
    """extract -> (left, right) -> [gate ->] publish.

    ``extract`` publishes an XCom list; ``gate`` is an approval task;
    ``fail`` makes ``right`` exit 1 so the run fails and recovery applies.
    """
    join = ["left", "right"]
    tasks = [
        {
            "id": "extract",
            "command": xcom_push_cmd("rows", '["a","b"]')
            + "; echo extract-out",
        },
        {"id": "left", "dependsOn": ["extract"], "command": "echo left-out"},
        {
            "id": "right",
            "dependsOn": ["extract"],
            "command": "echo right-err >&2; exit 1"
            if fail
            else "echo right-out",
        },
    ]
    if gate:
        tasks.append({"id": "gate", "type": "approval", "dependsOn": join})
        join = ["gate"]
    tasks.append(
        {"id": "publish", "dependsOn": join, "command": "echo publish-out"}
    )
    return {"name": name, "tasks": tasks}


def default_jobs():
    """The standard fleet: one job per state the dashboard distinguishes."""
    return [
        job("alpha-ok", "echo alpha-out"),
        job("beta-fail", "echo beta-err >&2; exit 3"),
        job(
            "gamma-slow",
            py_cmd(
                "import time\n"
                "for i in range(600):\n"
                "    print(i, flush=True)\n"
                "    time.sleep(0.05)"
            ),
        ),
        job("delta-off", "echo never", enabled=False),
        job("epsilon-quiet", "true"),
    ]


class Daemon:
    """Run the scheduler and web app on a thread from a configuration file.

    ``auth`` selects an ``AUTH_PRESETS`` entry or supplies a web configuration
    mapping. ``jobs``, ``dags``, and ``pools`` are configuration mappings.
    ``state`` enables durable storage and the job API; DAGs and pools also
    enable them. ``extra`` adds top-level configuration.

    Use a context manager or call ``start()`` and ``stop()`` explicitly.
    """

    def __init__(
        self,
        tmp_path,
        *,
        auth="none",
        jobs=None,
        dags=None,
        pools=None,
        state=False,
        web=None,
        extra=None,
    ):
        self.tmp_path = pathlib.Path(tmp_path)
        web_cfg = {"listen": ["http://127.0.0.1:0"]}
        web_cfg.update(AUTH_PRESETS[auth] if isinstance(auth, str) else auth)
        web_cfg.update(web or {})
        config = {}
        # DAGs and pools both refuse to load without the durable store
        if state or dags or pools:
            config["state"] = {
                "path": str(self.tmp_path / "state"),
                "jobApi": {"enabled": True},
            }
        config["web"] = web_cfg
        if pools:
            config["pools"] = pools
        config["jobs"] = default_jobs() if jobs is None else jobs
        if dags:
            config["dags"] = dags
        config.update(extra or {})
        self.config = config
        self.config_path = self.tmp_path / "cronstable.yaml"
        self.cron = None
        self.loop = None
        self.port = None
        self._thread = None
        self._ready = threading.Event()
        self._error = None

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    def write_config(self):
        self.config_path.write_text(to_yaml(self.config) + "\n")

    def start(self):
        self.write_config()
        self._thread = threading.Thread(
            target=self._run, name="cronstable-e2e-daemon", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(60):
            raise AssertionError("daemon did not come up within 60s")
        if self._error is not None:
            raise AssertionError("daemon failed to start") from self._error
        return self

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop
        try:
            loop.run_until_complete(self._main())
        except BaseException as exc:  # surfaced through start()
            self._error = exc
        finally:
            self._ready.set()
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    async def _main(self):
        from cronstable.cron import Cron

        self.cron = Cron(str(self.config_path))
        runner = asyncio.ensure_future(self.cron.run())
        watcher = asyncio.ensure_future(self._announce(runner))
        try:
            await runner
        finally:
            watcher.cancel()

    async def _announce(self, runner):
        while not runner.done():
            web_runner = self.cron.web_runner
            if web_runner is not None and web_runner.addresses:
                self.port = web_runner.addresses[0][1]
                self._ready.set()
                return
            await asyncio.sleep(0.01)

    def stop(self):
        if self._thread is None:
            return
        if self.loop is not None and not self.loop.is_closed():
            try:
                self.call(self._shutdown(), timeout=30)
            except Exception:  # the loop is already gone
                pass
        self._thread.join(60)
        alive = self._thread.is_alive()
        self._thread = None
        assert not alive, "daemon thread did not stop within 60s"

    async def _shutdown(self):
        # Graceful shutdown waits for running work without a timeout.
        # Cancel all job and DAG task runs before requesting shutdown.
        running = [
            rj for rjs in list(self.cron.running_jobs.values()) for rj in rjs
        ]
        for rj in running:
            rj.cancelled = True
        await asyncio.gather(
            *(rj.cancel() for rj in running if rj.proc is not None),
            return_exceptions=True,
        )
        self.cron.signal_shutdown()

    # -- daemon-side access ------------------------------------------------

    def call(self, coro, timeout=30):
        """Run ``coro`` on the daemon's loop and return its result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout)

    def reload(self, **changes):
        """Rewrite the config file with ``changes`` and apply it."""
        self.config.update(changes)
        self.write_config()
        self.loop.call_soon_threadsafe(self.cron.signal_reload, "e2e test")

    @property
    def url(self):
        return "http://127.0.0.1:{}/".format(self.port)

    # -- plain HTTP, for arranging state and reading outcomes ---------------

    def api(self, method, path, token=None, body=None):
        """One API call outside the browser: ``(status, parsed body)``."""
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(
            self.url.rstrip("/") + path, data=data, method=method
        )
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status, raw = resp.status, resp.read()
        except urllib.error.HTTPError as err:
            # the error object owns the response socket: close it
            with err:
                status, raw = err.code, err.read()
        try:
            return status, json.loads(raw.decode() or "null")
        except ValueError:
            return status, raw.decode(errors="replace")

    def jobs(self, token=None):
        status, body = self.api("GET", "/jobs", token)
        assert status == 200, (status, body)
        return {j["name"]: j for j in body}

    # -- DAG helpers --------------------------------------------------------

    def trigger_dag(self, name, token=None):
        status, body = self.api("POST", "/dags/" + name + "/trigger", token)
        assert status == 200, (status, body)
        return body["runKey"]

    def dag_run(self, name, run_key, token=None):
        status, body = self.api(
            "GET", "/dags/{}/runs/{}".format(name, run_key), token
        )
        assert status == 200, (status, body)
        return body

    def wait_dag(self, name, run_key, predicate, token=None):
        """Wait until ``predicate(run document)`` holds; returns the run."""

        def check():
            run = self.dag_run(name, run_key, token)
            return run if predicate(run) else None

        return wait_until(check)

    def wait_gate(self, name, run_key, task="gate", token=None):
        return self.wait_dag(
            name,
            run_key,
            lambda run: (run["tasks"].get(task) or {}).get("awaitingApproval"),
            token,
        )

    def wait_dag_state(self, name, run_key, state, token=None):
        return self.wait_dag(
            name, run_key, lambda run: run["state"] == state, token
        )

    def run_and_wait(self, name, token=None, outcome=None):
        """Start ``name`` and wait for that run to finish."""
        before = (self.jobs(token)[name].get("last_run") or {}).get(
            "finished_at"
        )
        status, body = self.api("POST", "/jobs/" + name + "/start", token)
        assert status == 200, (status, body)

        def done():
            j = self.jobs(token)[name]
            last = j.get("last_run") or {}
            fresh = last.get("finished_at") not in (None, before)
            return last if fresh and not j["running"] else None

        last = wait_until(done)
        if outcome is not None:
            assert last["outcome"] == outcome, last
        return last


# --------------------------------------------------------------------------
# fault injection
# --------------------------------------------------------------------------


def sse_frame(event, data):
    """One SSE frame in the daemon's wire shape."""
    # non-ASCII stays literal, as the daemon sends it: multi-byte input
    # is part of what the reader has to cope with
    return "event: {}\ndata: {}\n\n".format(
        event, json.dumps(data, ensure_ascii=False)
    )


def sse_line(text, stream="stdout", **extra):
    payload = {"stream": stream, "line": text}
    payload.update(extra)
    return sse_frame("line", payload)


# Replaces window.fetch for paths matching window.__sse entries with a
# ReadableStream the test feeds chunk by chunk: Playwright's route.fulfill
# delivers a body in one piece, and the stream tests need frames split
# across reads, streams that stall mid-frame, and streams that error.
_SSE_SHIM = """
(() => {
  if (window.__sseInstalled) return;
  window.__sseInstalled = true;
  window.__sse = {};        // path regex source -> spec
  window.__sseCtl = {};     // path -> live controllers
  window.__sseOpens = [];   // every intercepted open, in order
  const orig = window.fetch;
  window.fetch = function (input, init) {
    const url = String(input && input.url ? input.url : input);
    const path = url.replace(/^https?:\\/\\/[^/]+/, "").split("?")[0];
    for (const src of Object.keys(window.__sse)) {
      if (!new RegExp(src).test(path)) continue;
      const spec = window.__sse[src];
      window.__sseOpens.push({ path, url, t: Date.now() });
      if (spec.status && spec.status !== 200) {
        return Promise.resolve(new Response(spec.body || "", {
          status: spec.status,
          headers: { "Content-Type": "application/json" },
        }));
      }
      const enc = new TextEncoder();
      const signal = init && init.signal;
      // Deliver one chunk per pull, then close or fail on the next pull.
      // A stream error discards queued data, so failing earlier would lose
      // bytes intended to arrive before the connection closes.
      const feed = {
        ctrl: null, queue: (spec.chunks || []).slice(),
        fail: !!spec.fail, close: !!spec.close, idle: false,
      };
      const step = (ctrl) => {
        if (feed.queue.length) {
          feed.idle = false;
          const chunk = feed.queue.shift();
          ctrl.enqueue(typeof chunk === "string" ? enc.encode(chunk)
            : new Uint8Array(chunk));
        } else if (feed.fail) ctrl.error(new TypeError("network error"));
        else if (feed.close) ctrl.close();
        else feed.idle = true;
      };
      feed.step = () => { if (feed.idle) step(feed.ctrl); };
      const body = new ReadableStream({
        start(ctrl) {
          feed.ctrl = ctrl;
          (window.__sseCtl[path] = window.__sseCtl[path] || []).push(feed);
          if (signal) signal.addEventListener("abort", () => {
            try { ctrl.error(new DOMException("aborted", "AbortError")); }
            catch (_) {}
          });
        },
        pull(ctrl) { step(ctrl); },
      });
      return Promise.resolve(new Response(body, {
        status: 200, headers: { "Content-Type": "text/event-stream" },
      }));
    }
    return orig.call(this, input, init);
  };
})();
"""


def quiet_route(handler):
    """Wrap a route handler to suppress Playwright errors.

    Closing a page while a route handler is fetching or fulfilling a request
    can raise an error on a later Playwright call, including in another test.
    Wrap each handler to avoid these teardown errors. This wrapper suppresses
    all Playwright errors from the handler, including errors unrelated to
    page closure.
    """

    def wrapped(route):
        try:
            return handler(route)
        except playwright_api.Error:
            return None

    return wrapped


class Faults:
    """``page.route`` fault injection layered over the real daemon."""

    def __init__(self, page):
        self.page = page
        self.requests = []

    # -- request recording -------------------------------------------------

    def record(self):
        """Record every request the page makes from here on."""

        def on_request(req):
            path = re.sub(r"^https?://[^/]+", "", req.url)
            self.requests.append(
                {
                    "method": req.method,
                    "path": path.split("?")[0],
                    "query": path.partition("?")[2],
                    "headers": dict(req.headers),
                    "body": req.post_data,
                    "at": time.monotonic(),
                }
            )

        self.page.on("request", on_request)
        return self.requests

    def sent(self, method, path):
        return [
            r
            for r in self.requests
            if r["method"] == method and r["path"] == path
        ]

    # -- HTTP faults -------------------------------------------------------

    @staticmethod
    def _glob(path):
        # match the path with or without a query string
        return re.compile(r"^https?://[^/]+" + path + r"(\?.*)?$")

    def status(self, path, code, body=None, times=None, method=None):
        """Answer ``path`` (a regex over the URL path) with ``code``."""
        payload = json.dumps(
            body if body is not None else {"error": "injected " + str(code)}
        )
        left = [times]

        def handler(route):
            if method and route.request.method != method:
                return route.fallback()
            if left[0] is not None:
                if left[0] <= 0:
                    return route.fallback()
                left[0] -= 1
            return route.fulfill(
                status=code, content_type="application/json", body=payload
            )

        installed = quiet_route(handler)
        self.page.route(self._glob(path), installed)
        return installed

    def abort(self, path, method=None, times=None):
        """Fail ``path`` at the network layer (connection refused)."""
        left = [times]

        def handler(route):
            if method and route.request.method != method:
                return route.fallback()
            if left[0] is not None:
                if left[0] <= 0:
                    return route.fallback()
                left[0] -= 1
            return route.abort("connectionrefused")

        installed = quiet_route(handler)
        self.page.route(self._glob(path), installed)
        return installed

    def hang(self, path, method=None):
        """Keep requests to ``path`` pending and return their routes.

        Call ``route.fallback()`` to release a pending request.
        """
        parked = []

        def handler(route):
            if method and route.request.method != method:
                return route.fallback()
            parked.append(route)

        self.page.route(self._glob(path), quiet_route(handler))
        return parked

    def json(self, path, payload, status=200, method=None):
        """Answer ``path`` with ``payload`` (or ``payload(request)``)."""

        def handler(route):
            if method and route.request.method != method:
                return route.fallback()
            body = payload(route.request) if callable(payload) else payload
            return route.fulfill(
                status=status,
                content_type="application/json",
                body=json.dumps(body),
            )

        installed = quiet_route(handler)
        self.page.route(self._glob(path), installed)
        return installed

    def rewrite(self, path, fn):
        """Pass the daemon's JSON response for ``path`` through ``fn``.

        Simulate a compromised server, a different server version, or a cluster
        peer supplying data that has not passed local configuration validation.
        """

        def handler(route):
            response = route.fetch()
            try:
                body = json.loads(response.text())
            except ValueError:
                return route.fulfill(response=response)
            return route.fulfill(
                response=response,
                body=json.dumps(fn(body)),
                headers={
                    **response.headers,
                    "content-type": "application/json",
                },
            )

        installed = quiet_route(handler)
        self.page.route(self._glob(path), installed)
        return installed

    def clear(self, path=None, handler=None):
        if path is None:
            self.page.unroute_all(behavior="ignoreErrors")
        elif handler is None:
            self.page.unroute(self._glob(path))
        else:
            self.page.unroute(self._glob(path), handler)

    # -- SSE ---------------------------------------------------------------

    def sse(self, path, chunks=(), close=False, fail=False, status=200):
        """Serve a controlled SSE stream for paths matching ``path``.

        Deliver ``chunks`` as separate reads so frames can span reads. Keep the
        stream open for ``sse_push`` unless ``close`` ends it or ``fail``
        injects a network error. For a status other than 200, return that
        HTTP status instead.
        """
        self.page.evaluate(_SSE_SHIM)
        self.page.evaluate(
            "([src, spec]) => { window.__sse[src] = spec; }",
            [
                path,
                {
                    "chunks": list(chunks),
                    "close": close,
                    "fail": fail,
                    "status": status,
                },
            ],
        )

    def sse_push(self, path, *chunks, close=False, fail=False):
        """Feed the newest open stream for the literal ``path``.

        A chunk is a string, or a list of byte values for input that is
        not valid UTF-8 on its own (a character cut between two reads).
        """
        self.page.evaluate(
            """([path, chunks, close, fail]) => {
              const list = window.__sseCtl[path] || [];
              const feed = list[list.length - 1];
              feed.queue.push(...chunks);
              feed.close = feed.close || close;
              feed.fail = feed.fail || fail;
              feed.step();
            }""",
            [path, list(chunks), close, fail],
        )

    def sse_opens(self, path=None):
        opens = self.page.evaluate("window.__sseOpens || []")
        return [o for o in opens if path is None or o["path"] == path]


#: init script form of the SSE shim, for streams that must be scripted
#: before the page's first request
SSE_SHIM_INIT = _SSE_SHIM


# --------------------------------------------------------------------------
# small page helpers
# --------------------------------------------------------------------------


def toasts(page):
    """Text of every toast on screen."""
    return page.evaluate(
        "[...document.querySelectorAll('#toasts .toast')]"
        ".map((t) => t.textContent)"
    )


def wait_toast(page, text):
    """Wait for a toast containing ``text``; returns its kind classes."""
    handle = page.wait_for_function(
        """(text) => {
          const hit = [...document.querySelectorAll('#toasts .toast')]
            .find((t) => t.textContent.includes(text));
          return hit ? hit.className : false;
        }""",
        arg=text,
    )
    return handle.json_value()


def row_names(page):
    return page.evaluate(
        "[...document.querySelectorAll('#rows tr[data-job]')]"
        ".map((tr) => tr.getAttribute('data-job'))"
    )


def row_status(page, name):
    """The status label of one job row, or None without such a row."""
    return page.evaluate(
        """(name) => {
          const tr = [...document.querySelectorAll('#rows tr[data-job]')]
            .find((r) => r.getAttribute('data-job') === name);
          return tr ? tr.querySelector('.st .label').textContent : null;
        }""",
        name,
    )


def wait_row_status(page, name, label):
    page.wait_for_function(
        """([name, label]) => {
          const tr = [...document.querySelectorAll('#rows tr[data-job]')]
            .find((r) => r.getAttribute('data-job') === name);
          return !!tr &&
            tr.querySelector('.st .label').textContent === label;
        }""",
        arg=[name, label],
    )
