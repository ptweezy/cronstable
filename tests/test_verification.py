import asyncio
import sys

import pytest

from cronstable.config import ConfigError, parse_config_string
from cronstable.job import RunningJob
from tests._helpers import _wait_until


def job(command, verify):
    config = parse_config_string(
        'jobs:\n  - name: check\n    command: ignored\n    schedule: "@reboot"\n',
        "",
    ).jobs[0]
    config.command = [sys.executable, "-c", command]
    config.verify = {"command": [sys.executable, "-c", verify], "timeout": 5}
    config.killTimeout = 0.1
    return RunningJob(config, None)


async def test_verification_failure_preserves_command_exit_and_output():
    run = job('print("created")', 'print("empty export"); exit(3)')
    await run.start()
    await run.wait()
    assert run.retcode == 0
    assert run.failed
    assert "verification failed" in run.fail_reason
    assert run.verification["exit_code"] == 3
    assert "empty export" in run.verification["stdout"]
    assert any(line[0] == "verify.stdout" for line in run.output.lines)


async def test_failed_command_skips_verification(tmp_path):
    output = tmp_path / "should-not-exist"
    run = job("exit(2)", f"open({str(output)!r}, 'w').close()")
    await run.start()
    await run.wait()
    assert run.verification["outcome"] == "skipped"
    assert not output.exists()


async def test_verifier_inherits_working_directory_and_environment(tmp_path):
    run = job(
        "open('result', 'w').write('done')",
        "import os; assert open('result').read() == os.environ['EXPECTED']",
    )
    run.config.workingDirectory = str(tmp_path)
    run.config.environment = [{"key": "EXPECTED", "value": "done"}]
    await run.start()
    await run.wait()
    assert not run.failed
    assert run.verification["outcome"] == "success"


async def test_verification_timeout():
    run = job("pass", "import time; time.sleep(30)")
    run.config.verify["timeout"] = 0.1
    await run.start()
    await asyncio.wait_for(run.wait(), 5)
    assert run.failed
    assert run.retcode == 0
    assert run.verification["exit_code"] == -100


async def test_cancelling_job_cancels_verifier(tmp_path):
    ready = tmp_path / "verifier-ready"
    run = job(
        "pass",
        f"import time; from pathlib import Path; Path({str(ready)!r}).touch(); time.sleep(30)",
    )
    await run.start()
    waiter = asyncio.create_task(run.wait())
    try:
        await _wait_until(ready.exists)
        verifier = run._verifier
        assert verifier is not None and verifier.proc is not None
    finally:
        run.cancelled = True
        await run.cancel()
        await asyncio.wait_for(waiter, 5)
    assert verifier.retcode not in (0xC0000142, -1073741502)
    assert run._verifier is None
    assert run._terminated


@pytest.mark.parametrize("timeout", ["0", "-1", ".inf", ".nan"])
def test_reject_invalid_verification_timeout(timeout):
    with pytest.raises(ConfigError):
        parse_config_string(
            'jobs:\n  - name: check\n    command: x\n    schedule: "@reboot"\n'
            '    verify:\n      command: x\n      timeout: ' + timeout + '\n', "",
        )


def test_verifier_command_expands_environment_at_execution(monkeypatch):
    monkeypatch.delenv("ONLY_IN_JOB", raising=False)
    config = parse_config_string(
        'jobs:\n  - name: check\n    command: x\n    schedule: "@reboot"\n'
        '    verify:\n      command: echo ${ONLY_IN_JOB}\n', "",
    )
    assert config.jobs[0].verify["command"] == "echo ${ONLY_IN_JOB}"


async def test_verification_failure_blocks_downstream_and_is_recorded(dag_cron):
    from tests.test_state_dag_run import _drive
    cron = await dag_cron('''
dags:
  - name: flow
    tasks:
      - id: export
        command: ignored
        verify:
          command: ignored
      - id: consume
        command: ignored
        dependsOn:
          - export
''')
    template = cron.cron_dags["flow"].task_templates["export"]
    template.command = [sys.executable, "-c", "pass"]
    template.verify["command"] = [sys.executable, "-c", "print('missing rows'); exit(1)"]
    key = await cron._dag.trigger_run("flow")
    result = await _drive(cron, "flow", key)
    assert result["tasks"]["export"]["exitCode"] == 0
    assert result["tasks"]["export"]["verification"]["outcome"] == "failure"
    assert result["tasks"]["consume"]["state"] == "upstream_failed"


async def test_verification_output_is_bounded():
    run = job("pass", "print('x' * 1000 + '\\n' * 2); " * 100)
    await run.start()
    await run.wait()
    assert len(run.verification["stdout"]) <= 16384
    assert run.verification["output_truncated"]


async def test_verification_deadline_includes_output_drain(monkeypatch):
    original = RunningJob.wait
    async def wait(run):
        if run._output_prefix == "verify.":
            await asyncio.Event().wait()
        await original(run)
    monkeypatch.setattr(RunningJob, "wait", wait)
    run = job("pass", "pass")
    run.config.verify["timeout"] = 0.1
    await run.start()
    await asyncio.wait_for(run.wait(), 5)
    assert run.verification["exit_code"] == -100
    assert run.failed
