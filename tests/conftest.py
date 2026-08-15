"""Shared fixtures for the cronstable test suite (findings B1/B2/B6/B12).

Ground rules for this file:

- NO autouse fixtures, ever.  Everything here is opt-in by parameter name,
  so nothing can change the behavior of a test that does not request it.
  In particular the frozen-clock fixture stays in tests/_cron_helpers.py,
  where only the files that import it get it.
- Pure helpers that are not fixtures live in tests/_helpers.py; shared YAML
  constants live in tests/_configs.py.  This module holds only what needs
  pytest wiring (fixtures) plus the shared ``Req``/``_cron`` pair, which
  test files import as ``from tests.conftest import Req, _cron``.
- Heavyweight imports (cronstable.cron, cronstable.jobapi, aiohttp) happen
  inside the fixtures/helpers that need them, so loading conftest costs the
  suite nothing.

Consumers switch over file by file; the local fixtures and try/finally
teardowns they replace keep working side by side until then.
"""

import asyncio
import sys
import threading
import time

import pytest

from tests._helpers import (
    ExitError,
    _backend,
    _drain_state_writes,
    _exit,
    _state_cfg,
)


# --- hung-test asyncio task dump (the chronic 3.12 teardown hang) ----------
#
# faulthandler_timeout (pyproject) dumps OS threads and exits at 300s, but
# the hang sits in a suspended coroutine faulthandler cannot see: the
# 2026-08-15 dump showed the main thread in Runner.close -> _cancel_all_tasks
# -> select() with every other thread a parked daemon.  This dump fires
# first (~240s) and prints every asyncio.Task with its await stack and
# _fut_waiter, which names what the unfinished task is stuck on.  It only
# observes (a gc walk from a side thread; no loop is touched, no callback
# scheduled), so it cannot perturb the hang it reports on.  Not a fixture,
# so the no-autouse rule above stands: no test's behavior can change.

_DUMP_TASKS_AFTER = 240.0
_current_test: "tuple[str, float] | None" = None
_watchdog_started = False


def _dump_asyncio_tasks(nodeid: str) -> None:
    import gc
    import traceback

    err = sys.__stderr__
    if err is None:  # pragma: no cover - pythonw
        return
    print(
        "\n=== asyncio task dump: %r still running after %.0fs ==="
        % (nodeid, _DUMP_TASKS_AFTER),
        file=err,
    )
    for obj in gc.get_objects():
        if not isinstance(obj, asyncio.Task):
            continue
        print("--- %r" % obj, file=err)
        waiter = getattr(obj, "_fut_waiter", None)
        if waiter is not None:
            print("    waiting on: %r" % waiter, file=err)
        try:
            obj.print_stack(file=err)
        except Exception:  # pragma: no cover - a task mid-teardown
            traceback.print_exc(file=err)
    print("=== end asyncio task dump ===", file=err, flush=True)


def _watch_for_hung_test() -> None:
    dumped_for = None
    while True:
        time.sleep(15)
        current = _current_test
        if current is None:
            continue
        nodeid, started = current
        if (
            nodeid != dumped_for
            and time.monotonic() - started > _DUMP_TASKS_AFTER
        ):
            dumped_for = nodeid
            _dump_asyncio_tasks(nodeid)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item):
    global _current_test, _watchdog_started
    if not _watchdog_started:
        _watchdog_started = True
        threading.Thread(
            target=_watch_for_hung_test,
            daemon=True,
            name="cronstable-test-watchdog",
        ).start()
    _current_test = (item.nodeid, time.monotonic())
    yield
    _current_test = None


class Req:
    """A minimal stand-in for an aiohttp request.

    The shape the former test_cron.py re-declared 42x (finding B6); the
    handler tests build one per direct handler call.  ``body`` (a
    JSON-ready object) arms ``can_read_body`` and ``json()`` for the
    POST-handler tests; without it the request carries no body.
    """

    def __init__(self, query=None, match=None, headers=None, body=None):
        self.query = query or {}
        self.match_info = match or {}
        self.headers = headers or {}
        self.can_read_body = body is not None
        self._body = body

    async def json(self):
        return self._body


def _cron(yaml):
    # a Cron parsed from YAML with the web surface enabled
    # (cron.web_config = {} is what the direct-handler-call tests need,
    # declared 44x in the former test_cron.py, finding B6).
    from cronstable.cron import Cron

    cron = Cron(None, config_yaml=yaml)
    cron.web_config = {}
    return cron


# --- filesystem state backend (finding B2) ---------------------------------


@pytest.fixture
async def fs_backend(fs_backend_factory):
    """A started FilesystemStateBackend in tmp_path, stopped on teardown.

    Replaces the ``backend = _backend(tmp_path); await backend.start()``
    prologue (229x across 6 files).  ``stop()`` is idempotent, so a test
    that stops the backend itself (e.g. to assert post-stop behavior) can
    still use this fixture.
    """
    return await fs_backend_factory()


@pytest.fixture
async def fs_backend_factory(tmp_path):
    """Factory for the ``_backend(tmp_path, **overrides)`` cases.

    ``make(**over)`` builds a backend in tmp_path (or ``path=`` elsewhere),
    starts it unless ``start=False`` (the construction-only tests, e.g. the
    rate-limit wiring checks), and registers every started backend for
    teardown in reverse creation order.
    """
    started = []

    async def make(path=None, *, start=True, **over):
        backend = _backend(tmp_path if path is None else path, **over)
        if start:
            await backend.start()
            started.append(backend)
        return backend

    yield make
    for backend in reversed(started):
        await backend.stop()


# --- state-backed Cron factories (finding B1) --------------------------------


@pytest.fixture
async def dag_cron(tmp_path):
    """Factory for a DAG-driving Cron on a real backend, torn down like
    tests/test_state_dag_run.py's ``_teardown`` (its 150 try/finally sites).

    ``make(yaml)`` prepends ``state:\\n  path: <tmp_path>\\n`` (plus
    ``extra_state`` lines, pre-indented, e.g.
    ``"  jobApi:\\n    enabled: true\\n"`` for test_ui_endpoints.py's
    variant), starts the state layer, and returns the cron.  ``web=True``
    additionally sets ``cron.web_config = {}`` for direct handler calls.
    Teardown per cron, in reverse creation order: dag shutdown, job-api
    stop, backend stop.
    """
    crons = []

    async def make(yaml, *, extra_state="", web=False):
        from cronstable.cron import Cron

        cfg = "state:\n  path: {}\n{}".format(tmp_path, extra_state) + yaml
        cron = Cron(None, config_yaml=cfg)
        if web:
            cron.web_config = {}
        await cron.start_stop_state(_state_cfg(cfg))
        crons.append(cron)
        return cron

    yield make
    for cron in reversed(crons):
        await cron._dag.shutdown()
        await cron._stop_job_api()
        if cron.state_backend is not None:
            await cron.state_backend.stop()


@pytest.fixture
async def stateful_cron(tmp_path):
    """Factory for a scheduler-side stateful Cron, torn down like
    tests/test_state_scheduler_durability.py's ``_stop_state`` (58 sites).

    Unlike ``dag_cron``, the job YAML here does NOT gain a state section:
    ``make(yaml)`` parses the jobs as given and starts the state layer from
    a separate ``state:  path: <tmp_path>`` config (plus ``extra_state``
    lines, pre-indented), mirroring the source file's ``_stateful_cron``.
    Teardown drains pending state writes, then stops and clears the backend.
    """
    crons = []

    async def make(yaml, *, extra_state=""):
        from cronstable.cron import Cron

        cron = Cron(None, config_yaml=yaml)
        cfg = _state_cfg(
            "state:\n  path: {}\n{}".format(tmp_path, extra_state)
        )
        await cron.start_stop_state(cfg)
        assert cron.state_backend is not None
        crons.append(cron)
        return cron

    yield make
    for cron in reversed(crons):
        await _drain_state_writes(cron)
        if cron.state_backend is not None:
            await cron.state_backend.stop()
            cron.state_backend = None


# --- the loopback job-state API (findings B1/B11) ----------------------------


class JobApiHarness:
    """What the 36 four-line prologues in tests/test_state_job_api.py build:
    a started backend, a started JobStateAPI with one registered default run
    (token "tok", job "job"), and an aiohttp session already carrying the
    matching Authorization header."""

    def __init__(self, api, backend, session, ctx):
        self.api = api
        self.backend = backend
        self.session = session
        self.ctx = ctx

    def url(self, path):
        return self.api.base_url + path


@pytest.fixture
async def job_api_factory(fs_backend_factory):
    """Factory for the ``job_api`` harness with config and run overrides.

    ``make(ctx=None, **cfg_over)`` builds the same harness on a fresh
    backend, with the default config (maxValueBytes/maxArtifactBytes 0,
    lockTtlSeconds 5) updated by ``cfg_over`` (the size-limit and
    lockTtlSeconds cases) and the default run replaced by ``ctx`` when
    given; the session's bearer token follows the registered run.
    Teardown per harness, in reverse creation order: session close, api
    stop (each backend is stopped by ``fs_backend_factory``'s own
    finalizer, after these).
    """
    harnesses = []

    async def make(ctx=None, **cfg_over):
        import aiohttp

        from cronstable.jobapi import JobStateAPI, RunContext

        backend = await fs_backend_factory()
        config = {
            "maxValueBytes": 0,
            "maxArtifactBytes": 0,
            "lockTtlSeconds": 5,
        }
        config.update(cfg_over)
        api = JobStateAPI(
            lambda: backend, base_holder="h#proc", config=config
        )
        await api.start()
        if ctx is None:
            # the source file's _ctx() defaults, verbatim
            ctx = RunContext(
                token="tok",
                run_id="rid-tok",
                job_name="job",
                attempt=0,
                scheduled_at=None,
                host="h",
                default_scope="job",
                allowed_scopes=set(),
                secrets={},
            )
        api.register_run(ctx)
        session = aiohttp.ClientSession(
            headers={"Authorization": "Bearer " + ctx.token}
        )
        harness = JobApiHarness(api, backend, session, ctx)
        harnesses.append(harness)
        return harness

    yield make
    for harness in reversed(harnesses):
        await harness.session.close()
        await harness.api.stop()


@pytest.fixture
async def job_api(job_api_factory):
    # the fixed default harness: job_api_factory's zero-argument make.
    return await job_api_factory()


# --- CLI runner (finding B12) ------------------------------------------------


class CliRunner:
    """The ExitError/_exit/_run/_cli scaffolding triplicated across
    tests/test_state_admin.py, tests/test_main.py and
    tests/test_state_job_cli.py.

    Calling the runner drives ``cronstable.__main__.main_loop`` on a fresh
    event loop with a fake ``sys.argv`` and ``sys.exit`` captured as
    :class:`tests._helpers.ExitError`, returning the exit code.  ``env=``
    sets environment variables first (test_state_job_cli.py's
    ``CRONSTABLE_STATE_URL``/``_TOKEN``); any extra monkeypatching (e.g.
    ``jobcli._http``) is the caller's, done before invoking.
    """

    def __init__(self, monkeypatch):
        self._monkeypatch = monkeypatch

    def __call__(self, argv, *, env=None):
        import cronstable.__main__

        loop = asyncio.new_event_loop()
        try:
            for key, value in (env or {}).items():
                self._monkeypatch.setenv(key, value)
            self._monkeypatch.setattr(sys, "argv", ["cronstable"] + list(argv))
            self._monkeypatch.setattr(sys, "exit", _exit)
            with pytest.raises(ExitError) as excinfo:
                cronstable.__main__.main_loop(loop)
            return excinfo.value.args[0]
        finally:
            loop.close()


@pytest.fixture
def cli_runner(monkeypatch):
    return CliRunner(monkeypatch)
