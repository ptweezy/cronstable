"""Drive a real executable through files, subprocesses, and loopback HTTP."""

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psutil


def _reserve_ports(count):
    """``count`` distinct free loopback ports, named now and bound later.

    A reservation cannot be held open, since the daemon does the binding
    itself, so the only thief that matters is the daemon -- and an
    unpinned job API robs the web API: with no ``state.jobApi.listen`` it
    takes an OS-ASSIGNED port, which the kernel may draw from the pool
    this harness just released. The daemon then logs "address already in
    use", leaves the web API down for good (its bind retry can never win
    a port its own job API holds), and every request 404s against the job
    API instead. So ``configure`` pins BOTH listeners, and binding the
    whole set before reading any port back is what keeps them distinct.
    """
    socks = [socket.socket() for _ in range(count)]
    try:
        for sock in socks:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in socks]
    finally:
        for sock in socks:
            sock.close()


class Daemon:
    def __init__(self, binary, work, evidence):
        self.binary = binary
        self.work = work
        self.evidence = evidence
        self.token = uuid.uuid4().hex
        self.value = uuid.uuid4().hex
        self.config = work / "cronstable.yaml"
        self.process = None
        self.processes = []
        self.descendants = set()
        self.logs = []
        self.generation = 0
        self.env = os.environ.copy()
        for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
            self.env.pop(key, None)
        # The target cannot find an editable install or another cronstable on
        # the driver's PATH. Native scripts use only the OS shell/utilities.
        if os.name == "nt":
            # Windows os.environ normalizes keys to uppercase. Its plain
            # dict copy above no longer supports case-insensitive lookups.
            system = Path(self.env["SYSTEMROOT"])
            search = os.pathsep.join(map(str, [system / "System32", system]))
        else:
            search = os.defpath
        self.env["PATH"] = str(binary.parent) + os.pathsep + search
        self.env["ACCEPTANCE_VALUE"] = self.value
        self.http = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )
        # Every port the daemon listens on is one this harness chose.
        self.port, self.job_api_port = _reserve_ports(2)
        self.url = f"http://127.0.0.1:{self.port}"

    def configure(self, name, scenario, *, scheduled=False, retry=False):
        suffix = "cmd" if os.name == "nt" else "sh"
        script = Path(__file__).parent / "workloads" / f"{scenario}.{suffix}"
        shutil.copy2(script, self.work / script.name)
        command = script.name if os.name == "nt" else f"sh {script.name}"
        schedule = (
            '      second: "*/2"\n'
            if scheduled
            else (
                f'      year: "{datetime.now(timezone.utc).year + 1}"\n'
                '      month: "1"\n      dayOfMonth: "1"\n'
                '      hour: "0"\n      minute: "0"\n      second: "0"\n'
            )
        )
        body = (
            "state:\n  path: ./state\n  topology: single-node\n"
            "  jobApi:\n    enabled: true\n"
            f"    listen: http://127.0.0.1:{self.job_api_port}\n"
            f"web:\n  listen:\n    - {self.url}\n"
            f"  authToken:\n    value: {self.token}\n"
            f"jobs:\n  - name: {name}\n"
            + (
                '    schedule: "@reboot"\n'
                if retry
                else f"    schedule:\n{schedule}"
            )
            + f"    command: {json.dumps(command)}\n"
            "    captureStdout: true\n    captureStderr: true\n"
            "    concurrencyPolicy: Forbid\n    executionTimeout: 90\n"
        )
        if retry:
            body += (
                "    onFailure:\n      retry:\n        maximumRetries: 1\n"
                "        initialDelay: 30\n        maximumDelay: 30\n"
                "        backoffMultiplier: 1\n"
            )
        self.config.write_text(body, encoding="utf-8")
        (self.evidence / "cronstable.yaml").write_text(body, encoding="utf-8")

    def _spawn(self, args, label):
        log = (self.evidence / f"{label}.log").open("wb")
        self.logs.append(log)
        proc = subprocess.Popen(
            [str(self.binary), *args],
            cwd=self.work,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append(proc)
        try:
            self.descendants.add(psutil.Process(proc.pid))
        except psutil.NoSuchProcess:
            # A tiny CLI command can already have exited on Windows.
            pass
        return proc

    def _remember_children(self):
        for process in tuple(self.descendants):
            try:
                self.descendants.update(process.children(recursive=True))
            except psutil.NoSuchProcess:
                pass

    def wait(self, description, predicate, *, timeout=75, alive=True):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            self._remember_children()
            if alive and self.process is not None:
                assert self.process.poll() is None, (
                    f"Daemon exited {self.process.returncode} "
                    f"while {description}; "
                    f"logs: {self.evidence}"
                )
            last = predicate()
            if last:
                return last
            time.sleep(0.1)
        raise AssertionError(
            f"Timed out {description}; last result: {last!r}; "
            f"logs: {self.evidence}"
        )

    def cli(self, *args, expected=0):
        label = f"cli-{len(self.processes)}"
        proc = self._spawn(list(args), label)
        self.wait(
            f"running {args!r}",
            lambda: proc.poll() is not None,
            timeout=45,
            alive=False,
        )
        text = (self.evidence / f"{label}.log").read_text(
            encoding="utf-8", errors="replace"
        )
        assert proc.returncode == expected, text
        return text

    def request(self, path, *, method="GET", authenticated=True, raw=False):
        headers = {"Accept": "application/json"}
        if authenticated:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            self.url + path, headers=headers, method=method
        )
        with self.http.open(request, timeout=3) as response:
            body = response.read().decode("utf-8")
        if raw:
            return body
        data = json.loads(body)
        if method == "GET":
            filename = path.strip("/").replace("/", "-") + ".json"
            (self.evidence / filename).write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
        return data

    def start(self):
        assert self.process is None or self.process.poll() is not None
        self.generation += 1
        self.process = self._spawn(
            ["-c", str(self.config)], f"daemon-{self.generation}"
        )

        def ready():
            try:
                self.request("/jobs")
                return True
            except urllib.error.HTTPError:
                # An unrelated listener or a broken endpoint is a failure.
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                return False

        self.wait("waiting for authenticated readiness", ready)

    def runs(self, job):
        return self.request(f"/jobs/{job}/runs")["runs"]

    def lines(self, filename):
        path = self.work / filename
        return (
            path.read_text(encoding="utf-8").splitlines()
            if path.exists()
            else []
        )

    def log_text(self):
        """The current daemon generation's combined stdout and stderr."""
        path = self.evidence / f"daemon-{self.generation}.log"
        return path.read_text(encoding="utf-8", errors="replace")

    def _daemon_processes(self):
        """Identify the daemon processes, excluding its jobs.

        A single-file bundle runs the program as a child of its bootloader.
        Include every process in the tree that uses the daemon's executable.
        Jobs run as native shell scripts, so they don't match the executable.
        """
        try:
            root = psutil.Process(self.process.pid)
            image = root.exe()
            tree = [root, *root.children(recursive=True)]
        except psutil.NoSuchProcess:
            return []
        found = [root]
        for process in tree[1:]:
            try:
                if process.exe() == image:
                    found.append(process)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return found

    def kill(self):
        """Terminate the daemon with SIGKILL or, on Windows, TerminateProcess.

        No shutdown handler runs, and no buffered data is flushed. Leave jobs
        running and track them for ``wait_for_orphans`` and test teardown.
        """
        self._remember_children()
        victims = self._daemon_processes()
        # Terminate the program first. Otherwise, its bootloader could exit
        # after reaping it while termination is still in progress.
        for process in reversed(victims):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(victims, timeout=30)
        assert not alive, f"Daemon survived a hard kill: {alive}"
        self.process.wait(timeout=30)
        with socket.socket() as sock:
            sock.settimeout(1)
            assert sock.connect_ex(("127.0.0.1", self.port)) != 0, (
                "Listener survived a hard kill"
            )

    @staticmethod
    def _exited(process):
        # If PID 1 does not reap adopted children, an exited orphan remains
        # a zombie. Count it as exited.
        try:
            return (
                not process.is_running()
                or process.status() == psutil.STATUS_ZOMBIE
            )
        except psutil.NoSuchProcess:
            return True

    def wait_for_orphans(self):
        """Wait for every job from the terminated daemon to exit.

        The next daemon reconciles an in-flight record only after its process
        exits. The test scenario controls when these jobs finish.
        """
        assert self.process.poll() is not None

        def gone():
            self._remember_children()
            return all(self._exited(p) for p in self.descendants)

        self.wait("waiting for orphaned jobs to exit", gone, alive=False)

    def store_files(self):
        """Read every visible state file, parsing JSON where applicable.

        Files outside the quarantine and temporary directories must contain
        complete records. Raise for the first incomplete or invalid file.
        """
        checked = 0
        for path in sorted((self.work / "state").rglob("*")):
            parts = path.relative_to(self.work / "state").parts
            if not path.is_file() or {"quarantine", "tmp"} & set(parts):
                continue
            assert path.suffix != ".tmp", f"Temp file outside tmp: {path}"
            if path.suffix in (".json", ".doc", ".lease"):
                raw = path.read_bytes()
                assert raw, f"Empty visible record: {path}"
                try:
                    record = json.loads(raw)
                except ValueError as exc:
                    raise AssertionError(
                        f"Unparseable visible record: {path}"
                    ) from exc
                assert isinstance(record, dict), path
                checked += 1
        return checked

    def begin_shutdown(self):
        self._remember_children()
        assert self.request("/shutdown", method="POST")["shuttingDown"] is True

    def finish_shutdown(self):
        self.wait(
            "waiting for graceful shutdown",
            lambda: self.process.poll() is not None,
            timeout=60,
            alive=False,
        )
        assert self.process.returncode == 0
        with socket.socket() as sock:
            sock.settimeout(1)
            assert sock.connect_ex(("127.0.0.1", self.port)) != 0, (
                "Listener survived shutdown"
            )

    def stop(self):
        self.begin_shutdown()
        self.finish_shutdown()

    def close(self):
        # Remember descendants before killing their parent: jobs create their
        # own process groups, so killing only the daemon's group leaks jobs.
        self._remember_children()
        shutdown_error = None
        if self.process is not None and self.process.poll() is None:
            for barrier in ("drain.release", "retry.release"):
                (self.work / barrier).touch()
            try:
                self.stop()
            except (AssertionError, OSError, urllib.error.URLError) as exc:
                shutdown_error = exc
                (self.evidence / "cleanup.log").write_text(
                    str(exc), encoding="utf-8"
                )
        self._remember_children()
        for process in self.descendants:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        for proc in self.processes:
            proc.wait(timeout=10)
        _, alive = psutil.wait_procs(self.descendants, timeout=10)
        # A hard-killed daemon orphans its jobs. Under a PID 1 that never
        # reaps they linger as zombies: exited, holding nothing.
        alive = [p for p in alive if not self._exited(p)]
        for log in self.logs:
            log.close()
        assert not alive, f"Acceptance processes survived cleanup: {alive}"
        if shutdown_error is not None:
            raise AssertionError(
                "Graceful cleanup failed; forced teardown; "
                f"logs: {self.evidence}"
            ) from shutdown_error
