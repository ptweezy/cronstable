"""Run daemon subprocesses for the crash and contention tests.

This helper module imports only the standard library, the psutil runtime
dependency, and cronstable. It does not import test modules.

Each daemon runs from source with ``sys.executable -m cronstable`` and uses
a filesystem state store in the test's ``tmp_path``. ``Popen.kill`` stops
it with SIGKILL on POSIX or TerminateProcess on Windows. Waits poll an
observable condition until a deadline. ``CrashDaemon.close`` reaps the
daemon, its known descendants, and job processes that reported their PIDs,
including when a test fails.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psutil

from cronstable.config import StateConfig
from cronstable.state import FilesystemStateBackend

ROOT = Path(__file__).resolve().parents[1]

#: Deadline for one observable condition. CI runners are slow, Windows
#: runners slower, and a coverage run slows both again.
WAIT = 90.0

#: A schedule that never comes due inside a test: the job runs only when
#: the test starts it through the API.
NEVER = 'schedule:\n      year: "2099"\n'

#: The job body every scenario shares. It appends its pid to starts.log
#: (one line per launch to count duplicates), then waits for a release file.
#: It exits at its own deadline if the driver stops.
#: Avoid stdout and stderr writes after the daemon exits, because the
#: pipe readers are gone and a write could terminate the job.
HOLD_JOB = """\
import os, sys, time
tag = sys.argv[1]
code = int(sys.argv[2]) if len(sys.argv) > 2 else 0
with open(tag + ".starts.log", "a") as f:
    f.write("%d\\n" % os.getpid())
deadline = time.monotonic() + 240
while not os.path.exists(tag + ".release"):
    if time.monotonic() > deadline:
        sys.exit(24)
    time.sleep(0.05)
with open(tag + ".finished.log", "a") as f:
    f.write("%d\\n" % os.getpid())
sys.exit(code)
"""

#: A job that records the launch and exits at once with the given code.
QUICK_JOB = """\
import os, sys
tag = sys.argv[1]
code = int(sys.argv[2]) if len(sys.argv) > 2 else 0
with open(tag + ".starts.log", "a") as f:
    f.write("%d\\n" % os.getpid())
sys.exit(code)
"""


#: Run job scripts with the base interpreter on Windows. A virtual
#: environment's python.exe launches a child interpreter, so the daemon
#: would record a different PID from the one the script reports. The base
#: interpreter uses a single process. Jobs need only the standard library.
JOB_PYTHON = (
    getattr(sys, "_base_executable", None) if os.name == "nt" else None
) or sys.executable


def wait_for(description, predicate, *, timeout=WAIT, interval=0.05):
    """Poll ``predicate`` until it returns something truthy, or fail."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(
        "timed out {}; last result: {!r}".format(description, last)
    )


def source_env():
    """The environment a from-source child needs to import cronstable."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    return env


def command_yaml(script, *args, indent=6):
    """Return a YAML command list that runs ``script`` with this interpreter.

    The list executes the interpreter directly, without a shell, so the daemon
    records the job's PID. Each item is a JSON string, which is also a valid
    double-quoted YAML scalar and preserves backslashes in Windows paths.
    """
    pad = " " * indent
    items = [JOB_PYTHON, str(script), *map(str, args)]
    return "command:\n" + "".join(
        "{}- {}\n".format(pad, json.dumps(item)) for item in items
    )


def lines(path):
    path = Path(path)
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def open_store(path):
    """The package's own backend over the daemon's store directory."""
    return FilesystemStateBackend(
        StateConfig(
            {
                "path": str(path),
                "topology": "single-node",
                "deploymentId": None,
            }
        ),
        lambda: "crash-tests",
    )


def store_records(path, stream, **kwargs):
    """``stream``'s records, read through the package API."""

    async def read():
        backend = open_store(path)
        await backend.start()
        try:
            return await backend.list_records(stream, **kwargs)
        finally:
            await backend.stop()

    return asyncio.run(read())


def store_documents(path, namespace):
    async def read():
        backend = open_store(path)
        await backend.start()
        try:
            return await backend.list_documents(namespace)
        finally:
            await backend.stop()

    return asyncio.run(read())


def kill_pid(pid):
    try:
        psutil.Process(pid).kill()
    except psutil.NoSuchProcess:
        pass


def pid_gone(pid):
    """True once ``pid`` has exited (a zombie counts as gone)."""
    try:
        return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


class CrashDaemon:
    """One daemon-under-test: a config, a store, and a process to kill."""

    def __init__(self, work, *, state_dir=None):
        self.work = Path(work)
        self.work.mkdir(parents=True, exist_ok=True)
        self.state_dir = Path(state_dir or self.work / "state")
        self.config = self.work / "cronstable.yaml"
        self.token = "crash-test-token"
        self.process = None
        self.processes = []
        self.descendants = set()
        self.generation = 0
        self.port = None
        self._born = time.time()
        self._logs = []
        self._http = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )

    def configure(self, body, *, state_extra="", job_api=False, head=""):
        """Write state store and web API settings, then append ``body``.

        The web API binds to port 0 so the operating system assigns the port.
        ``start()`` reads the port from the running process. The job API,
        which DAGs require, uses the same approach.
        """
        self.config.write_text(
            "{}"
            "state:\n"
            "  path: {}\n"
            "  topology: single-node\n"
            "{}"
            "  jobApi:\n"
            "    enabled: {}\n"
            "web:\n"
            "  listen:\n"
            "    - http://127.0.0.1:0\n"
            "  authToken:\n"
            "    value: {}\n"
            "{}".format(
                head,
                json.dumps(str(self.state_dir)),
                state_extra,
                "true" if job_api else "false",
                self.token,
                body,
            ),
            encoding="utf-8",
        )

    def script(self, name, source):
        path = self.work / name
        path.write_text(source, encoding="utf-8")
        return path

    # --- process control ---------------------------------------------------

    def log_path(self, generation=None):
        return self.work / "daemon-{}.log".format(
            self.generation if generation is None else generation
        )

    def log_text(self, generation=None):
        return self.log_path(generation).read_text(
            encoding="utf-8", errors="replace"
        )

    def log_tail(self):
        """The newest log lines worth reading in a failure message."""
        kept = [
            line
            for line in self.log_text().splitlines()
            if "aiohttp.access" not in line
        ]
        return "\n".join(kept[-60:])

    def spawn(self):
        """Start the daemon process without waiting for readiness."""
        assert self.process is None or self.process.poll() is not None
        self.generation += 1
        self.port = None
        log = self.log_path().open("wb")
        self._logs.append(log)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "cronstable", "-c", str(self.config)],
            cwd=self.work,
            env=source_env(),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append(self.process)
        try:
            self.descendants.add(psutil.Process(self.process.pid))
        except psutil.NoSuchProcess:
            pass
        return self.process

    def start(self):
        self.spawn()
        self.wait("waiting for the web API", self._discover_port)

    def daemon_processes(self):
        """Identify the daemon processes, excluding its jobs.

        On Windows, a virtual environment's python.exe launches the interpreter
        as a child process. Include every process whose command line names this
        configuration file. Job command lines do not contain that path.
        """
        try:
            root = psutil.Process(self.process.pid)
            tree = [root, *root.children(recursive=True)]
        except psutil.NoSuchProcess:
            return []
        found = [root]
        for process in tree[1:]:
            try:
                if str(self.config) in process.cmdline():
                    found.append(process)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return found

    def _listening_ports(self):
        ports = []
        for process in self.daemon_processes():
            try:
                # psutil 5.9 uses connections(); 6.0 renamed it and
                # deprecated the old name. Use the current name when present
                # so warnings-as-errors also works with newer releases.
                connections = getattr(process, "net_connections", None)
                if connections is None:
                    connections = process.connections
                conns = connections("tcp")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            ports += [
                c.laddr.port for c in conns if c.status == psutil.CONN_LISTEN
            ]
        return ports

    def _discover_port(self):
        # with the job API up the daemon listens twice; the web API is
        # the listener that answers an authenticated GET /jobs.
        for port in self._listening_ports():
            try:
                self._open("GET", port, "/jobs")
            except urllib.error.HTTPError as ex:
                # the job API answering 404: an open response to close.
                ex.close()
                continue
            except (urllib.error.URLError, OSError, ValueError):
                continue
            self.port = port
            return True
        return False

    def remember_children(self):
        for process in tuple(self.descendants):
            try:
                self.descendants.update(process.children(recursive=True))
            except psutil.NoSuchProcess:
                pass

    def wait(self, description, predicate, *, timeout=WAIT, alive=True):
        def check():
            self.remember_children()
            if alive and self.process is not None:
                assert self.process.poll() is None, (
                    "daemon exited {} while {}:\n{}".format(
                        self.process.returncode,
                        description,
                        self.log_tail(),
                    )
                )
            return predicate()

        try:
            return wait_for(description, check, timeout=timeout)
        except AssertionError as ex:
            raise AssertionError(
                "{}\n--- daemon log ---\n{}".format(ex, self.log_tail())
            ) from None

    def kill(self):
        """Terminate the daemon without cleanup or data flushing.

        ``Process.kill`` uses SIGKILL on POSIX and TerminateProcess on Windows.
        Leave the daemon's jobs running.
        """
        self.remember_children()
        victims = self.daemon_processes()
        # Terminate the interpreter first. Otherwise, its launcher could
        # exit while termination is still in progress.
        for process in reversed(victims):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _gone, alive = psutil.wait_procs(victims, timeout=30)
        assert not alive, "the daemon survived a hard kill: {}".format(alive)
        self.process.wait(timeout=30)
        self.port = None

    def stop(self):
        """Graceful shutdown through the control API."""
        self.remember_children()
        assert self.request("/shutdown", method="POST")["shuttingDown"]
        self.process.wait(timeout=WAIT)
        assert self.process.returncode == 0, self.log_tail()
        self.port = None

    # --- HTTP ----------------------------------------------------------------

    def _open(self, method, port, path, body=None):
        headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + self.token,
        }
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            "http://127.0.0.1:{}{}".format(port, path),
            data=data,
            headers=headers,
            method=method,
        )
        with self._http.open(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def request(self, path, *, method="GET", body=None):
        assert self.port is not None, "daemon is not up"
        return self._open(method, self.port, path, body)

    def runs(self, job):
        return self.request("/jobs/{}/runs".format(job))["runs"]

    def start_job(self, job):
        return self.request("/jobs/{}/start".format(job), method="POST")

    # --- evidence ------------------------------------------------------------

    def job_pids(self, tag):
        return [int(p) for p in lines(self.work / (tag + ".starts.log"))]

    def release(self, tag):
        (self.work / (tag + ".release")).touch()

    def records(self, stream, **kwargs):
        return store_records(self.state_dir, stream, **kwargs)

    def documents(self, namespace):
        return store_documents(self.state_dir, namespace)

    # --- teardown ------------------------------------------------------------

    def close(self):
        self.remember_children()
        victims = set(self.descendants)
        for log in self.work.glob("*.starts.log"):
            for pid in lines(log):
                try:
                    process = psutil.Process(int(pid))
                    # Check the start time to avoid terminating an unrelated
                    # process that reused a job's PID.
                    if process.create_time() >= self._born - 1:
                        victims.add(process)
                except (psutil.NoSuchProcess, ValueError):
                    pass
        for process in victims:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        for proc in self.processes:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
        psutil.wait_procs(victims, timeout=30)
        for log in self._logs:
            log.close()
