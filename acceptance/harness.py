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
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
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
        for log in self.logs:
            log.close()
        assert not alive, f"Acceptance processes survived cleanup: {alive}"
        if shutdown_error is not None:
            raise AssertionError(
                "Graceful cleanup failed; forced teardown; "
                f"logs: {self.evidence}"
            ) from shutdown_error
