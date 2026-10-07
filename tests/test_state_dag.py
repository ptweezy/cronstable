"""The durable DAG state machine (pure logic in cronstable.dag).

These tests drive :mod:`cronstable.dag` directly: the transforms are pure
``transform(body) -> (new_body, result)`` callables, so a tiny in-test executor
stands in for the cron driver (apply the claim transform, "launch" each intent,
mark it finished with a scripted outcome, repeat).  No backend, no clock, no
subprocess -- the whole graph engine is exercised against plain dicts.

Style matches the other state test files: bare ``def`` tests, module seams
driven with explicit values, no frozen wall clock (``now`` is an explicit
argument everywhere).  Backend + cron wiring lives in test_state_dag_run.py.
"""

import asyncio
import copy
import dataclasses
import json
import logging
import sys

import pytest

import cronstable.__main__
from cronstable import dag, jobcli
from cronstable.config import (
    ConfigError,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.dag import DagSpec, ExpandSpec, TaskSpec
from tests._configs import _STATE


def _dagcfg(dags_yaml, state=_STATE):
    return parse_config_string(state + dags_yaml, "")


def _xsect(dags_yaml, state=_STATE):
    _validate_cross_sections(_dagcfg(dags_yaml, state))


def _spec(*tasks):
    return DagSpec.build("d", list(tasks))


def _body(spec, now=0.0):
    return dag.new_run_body(
        dag="d",
        run_key="rk",
        run_id="rid",
        logical_date=None,
        kind="scheduled",
        now=now,
        spec=spec,
    )


def _apply(transform, body):
    new, result = transform(body)
    if dag.is_keep(new):
        return body, result
    return new, result


class _Executor:
    """Drives a spec+body to a fixed point like the real advance loop.

    ``outcomes`` maps a task id (or ``id#i``) to ``True`` (success) / ``False``
    (failure) / ``"skip"`` (the command exited with one of its skip codes);
    ``xcom`` maps a task id to the list it "published" so a mapped
    downstream can expand.  Approval gates and un-scripted tasks are left
    parked (the run stops making progress), which the test then inspects.
    """

    def __init__(self, spec, outcomes=None, xcom=None):
        self.spec = spec
        self.outcomes = outcomes or {}
        self.xcom = xcom or {}
        self.now = 100.0
        self.launched = []

    def _expansions(self, body):
        out = {}
        for tid, from_task, _key in dag.tasks_awaiting_expansion(
            self.spec, body
        ):
            out[tid] = self.xcom.get(from_task)
        return out

    def step(self, body):
        self.now += 1.0
        transform = dag.plan_and_claim(
            self.spec, self.now, "proc-A", "host-A", self._expansions(body)
        )
        body, result = _apply(transform, body)
        for intent in result.launches:
            self.launched.append(intent.taskkey)
            body = self._finish(body, intent)
        return body, result

    def _finish(self, body, intent):
        # simulate set_task_pid then completion
        body, _ = _apply(
            dag.set_task_pid(intent.taskkey, "proc-A", 4321, self.now), body
        )
        key = intent.taskkey
        success = self.outcomes.get(key, self.outcomes.get(intent.task_id))
        if success is None:
            return body  # unscripted: leave running (e.g. approval/sensor)
        task = self.spec.by_id[intent.task_id]
        skipped = success == "skip"
        body, _ = _apply(
            dag.mark_task_finished(
                key,
                success=bool(success),
                exit_code=99 if skipped else 0 if success else 1,
                fail_reason=None if success else "boom",
                now=self.now,
                task=task,
                skipped=skipped,
            ),
            body,
        )
        return body

    def run(self, body, max_steps=50):
        for _ in range(max_steps):
            body, result = self.step(body)
            if dag.is_terminal_run(body):
                return body
            if not result.changed and not result.launches:
                return body  # fixed point (parked on approval/sensor)
        raise AssertionError("did not converge")


def _state(body, key):
    return body["tasks"][key]["state"]


# The canonical two-task fan-out spec (finding B14): `work` expands over the
# "items" list its upstream `gen` publishes.  Specs are frozen dataclasses,
# so sharing one instance across tests is safe; only run bodies mutate.
_FANOUT_SPEC = _spec(
    TaskSpec("gen"),
    TaskSpec(
        "work",
        depends_on=("gen",),
        expand=ExpandSpec(from_task="gen", key="items"),
    ),
)


# --------------------------------------------------------------------------
# Graph validation
# --------------------------------------------------------------------------


def test_validate_ok_linear():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    dag.validate_graph(spec)  # no raise


@pytest.mark.parametrize(
    "tasks, match",
    [
        pytest.param(
            (TaskSpec("a", depends_on=("nope",)),),
            "unknown task 'nope'",
            id="unknown-dep",
        ),
        pytest.param(
            (
                TaskSpec("a", depends_on=("c",)),
                TaskSpec("b", depends_on=("a",)),
                TaskSpec("c", depends_on=("b",)),
            ),
            "cycle",
            id="cycle",
        ),
        pytest.param(
            (TaskSpec("a"), TaskSpec("a")),
            "duplicate",
            id="duplicate-id",
        ),
        pytest.param(
            (
                TaskSpec("a"),
                TaskSpec("b", depends_on=("a",)),
                TaskSpec(
                    "c",
                    depends_on=("b",),
                    expand=ExpandSpec(from_task="a", key="items"),
                ),
            ),
            "direct dependsOn",
            id="expand-needs-direct-dep",
        ),
        pytest.param(
            (
                TaskSpec("a"),
                TaskSpec(
                    "s",
                    type=dag.SENSOR,
                    depends_on=("a",),
                    expand=ExpandSpec(from_task="a", key="k"),
                ),
            ),
            "only a plain task",
            id="expand-of-sensor",
        ),
        pytest.param(
            (
                TaskSpec("a"),
                TaskSpec(
                    "b", depends_on=("a",),
                    expand=ExpandSpec(from_task="a", key="k"),
                ),
                TaskSpec(
                    "c", depends_on=("b",),
                    expand=ExpandSpec(from_task="b", key="k"),
                ),
            ),
            "itself mapped",
            id="chained-mapping",
        ),
        pytest.param((TaskSpec(""),), "non-empty", id="empty-task-id"),
        pytest.param(
            (TaskSpec("a", depends_on=("a",)),),
            "dependsOn itself",
            id="depends-on-self",
        ),
        # from_task is neither in depends_on nor a known task: the
        # expand-specific "is not a task" error fires (not the generic
        # unknown-dependsOn one).
        pytest.param(
            (
                TaskSpec("a"),
                TaskSpec(
                    "b",
                    depends_on=("a",),
                    expand=ExpandSpec(from_task="ghost", key="items"),
                ),
            ),
            "is not a task",
            id="expand-from-task-not-a-task",
        ),
        # a rule that counts upstream outcomes has none to count on a root
        pytest.param(
            (TaskSpec("alert", trigger_rule=dag.ALL_DONE_MIN_ONE_FAILED),),
            "task 'alert': triggerRule all_done_min_one_failed counts "
            "upstream outcomes, so the task needs a dependsOn entry",
            id="min-one-failed-on-a-root",
        ),
        pytest.param(
            (TaskSpec("j", trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS),),
            "triggerRule none_failed_min_one_success counts upstream",
            id="min-one-success-on-a-root",
        ),
    ],
)
def test_validate_graph_rejects(tasks, match):
    spec = _spec(*tasks)
    with pytest.raises(dag.DagValidationError, match=match):
        dag.validate_graph(spec)


# --------------------------------------------------------------------------
# Linear progression + terminal run
# --------------------------------------------------------------------------


def test_linear_all_success():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    ex = _Executor(spec, outcomes={"a": True, "b": True, "c": True})
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert ex.launched == ["a", "b", "c"]  # strict dependency order


def test_upstream_failure_propagates():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",)),
        TaskSpec("c", depends_on=("b",)),
    )
    ex = _Executor(spec, outcomes={"a": False})
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert _state(body, "a") == dag.FAILED
    assert _state(body, "b") == dag.UPSTREAM_FAILED
    assert _state(body, "c") == dag.UPSTREAM_FAILED
    assert "b" not in ex.launched  # never launched a doomed downstream


def test_all_done_runs_despite_failure():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",), trigger_rule=dag.ALL_DONE),
    )
    ex = _Executor(spec, outcomes={"a": False, "b": True})
    body = ex.run(_body(spec))
    assert _state(body, "a") == dag.FAILED
    assert _state(body, "b") == dag.SUCCESS
    # run is FAILED because a task failed, even though b ran and succeeded
    assert body["state"] == dag.FAILED


def test_diamond_fan_in():
    spec = _spec(
        TaskSpec("root"),
        TaskSpec("left", depends_on=("root",)),
        TaskSpec("right", depends_on=("root",)),
        TaskSpec("join", depends_on=("left", "right")),
    )
    ex = _Executor(
        spec,
        outcomes=dict.fromkeys(("root", "left", "right", "join"), True),
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert ex.launched[0] == "root"
    assert ex.launched[-1] == "join"
    assert set(ex.launched[1:3]) == {"left", "right"}


# --------------------------------------------------------------------------
# Retry
# --------------------------------------------------------------------------


def test_task_retries_then_succeeds():
    spec = _spec(TaskSpec("a", max_attempts=3, retry_delay=0.0))
    body = _body(spec)
    now = 10.0
    # first claim + fail -> up_for_retry
    body, res = _apply(
        dag.plan_and_claim(spec, now, "p", "h", {}), body
    )
    assert res.launches[0].task_id == "a"
    task = spec.by_id["a"]
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x",
            now=now, task=task,
        ),
        body,
    )
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1
    # next advance re-claims (retry delay elapsed)
    body, res = _apply(
        dag.plan_and_claim(spec, now + 1, "p", "h", {}), body
    )
    assert [i.task_id for i in res.launches] == ["a"]
    assert _state(body, "a") == dag.RUNNING
    # succeed the retry
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=now + 2, task=task,
        ),
        body,
    )
    assert _state(body, "a") == dag.SUCCESS


def test_task_exhausts_retries():
    spec = _spec(TaskSpec("a", max_attempts=2))
    ex = _Executor(spec, outcomes={"a": False})
    body = ex.run(_body(spec))
    assert _state(body, "a") == dag.FAILED
    assert body["tasks"]["a"]["attempt"] == 2
    assert ex.launched.count("a") == 2  # initial + one retry


def test_completion_is_fenced_to_the_claiming_proc_and_attempt():
    # H3/H4: a superseded attempt's late completion (a partitioned/evicted
    # former owner whose subprocess outlived its lease) must NOT terminalise
    # the instance another node has since reconciled and re-claimed.
    spec = _spec(TaskSpec("a", max_attempts=3, retry_delay=0.0))
    body = _body(spec)
    # node A claims attempt 0 under proc token "proc-A" and records its pid.
    body, res = _apply(
        dag.plan_and_claim(spec, 10.0, "proc-A", "host-A", {}), body
    )
    assert res.launches[0].task_id == "a"
    body, _ = _apply(dag.set_task_pid("a", "proc-A", 111, 10.0), body)
    # node A partitions; node B reconciles the crashed attempt (its proc is
    # foreign and its pid is not alive here) -> up_for_retry, attempt 1.
    body, _ = _apply(
        dag.reconcile_crashed(
            spec, 40.0, "proc-B", "host-B", lambda pid: False
        ),
        body,
    )
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1
    # node B re-claims attempt 1 under proc token "proc-B" and launches it.
    body, _ = _apply(
        dag.plan_and_claim(spec, 41.0, "proc-B", "host-B", {}), body
    )
    assert _state(body, "a") == dag.RUNNING
    assert body["tasks"]["a"]["proc"] == "proc-B"
    task = spec.by_id["a"]
    # node A's OLD attempt-0 subprocess now finishes; its completion carries
    # the stale (proc-A, attempt 0) identity -> it must be a NO-OP.
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None, now=45.0,
            task=task, expected_proc="proc-A", expected_attempt=0,
        ),
        body,
    )
    assert changed is False
    assert _state(body, "a") == dag.RUNNING  # live attempt-1 untouched
    assert body["tasks"]["a"]["proc"] == "proc-B"
    # node B's real completion (matching fence) DOES apply.
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None, now=46.0,
            task=task, expected_proc="proc-B", expected_attempt=1,
        ),
        body,
    )
    assert changed is True
    assert _state(body, "a") == dag.SUCCESS


def test_completion_without_fence_still_applies_backward_compat():
    # expected_proc/expected_attempt default to None -> no fence, so existing
    # callers/tests that omit them keep working.
    spec = _spec(TaskSpec("a", max_attempts=1))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, changed = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=2.0, task=spec.by_id["a"],
        ),
        body,
    )
    assert changed is True
    assert _state(body, "a") == dag.SUCCESS


def test_retry_delay_defers_reclaim():
    spec = _spec(TaskSpec("a", max_attempts=2, retry_delay=100.0))
    body = _body(spec)
    task = spec.by_id["a"]
    body, _ = _apply(dag.plan_and_claim(spec, 10.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x",
            now=10.0, task=task,
        ),
        body,
    )
    # before the delay elapses: no re-claim
    body, res = _apply(dag.plan_and_claim(spec, 50.0, "p", "h", {}), body)
    assert res.launches == []
    assert _state(body, "a") == dag.UP_FOR_RETRY
    # after: re-claim
    body, res = _apply(dag.plan_and_claim(spec, 200.0, "p", "h", {}), body)
    assert [i.task_id for i in res.launches] == ["a"]


# --------------------------------------------------------------------------
# Fan-out / dynamic mapping
# --------------------------------------------------------------------------


def test_fan_out_expands_and_joins():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={
            "gen": True, "collect": True,
            "work#0": True, "work#1": True, "work#2": True,
        },
        xcom={"gen": ["x", "y", "z"]},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert body["mapped"]["work"]["items"] == ["x", "y", "z"]
    assert {"work#0", "work#1", "work#2"}.issubset(set(ex.launched))
    # each instance carried its own item
    assert body["tasks"]["work#1"]["mapItem"] == "y"
    assert ex.launched[-1] == "collect"


def test_fan_out_empty_list_resolves_success():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={"gen": True, "collect": True},
        xcom={"gen": []},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert body["mapped"]["work"]["items"] == []
    # collect still ran (empty map counts as success upstream)
    assert "collect" in ex.launched


def test_fan_out_one_instance_fails_fails_join():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex = _Executor(
        spec,
        outcomes={"gen": True, "work#0": True, "work#1": False},
        xcom={"gen": ["a", "b"]},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert dag.effective_state(spec, body, "work") == dag.UPSTREAM_FAILED
    assert _state(body, "collect") == dag.UPSTREAM_FAILED


def test_mapped_all_done_source_fails_terminalises():
    # regression: a mapped task with trigger_rule=all_done whose expand source
    # FAILS must terminalise (it can never fan out), not hang the run forever.
    spec = _spec(
        TaskSpec("u"),
        TaskSpec(
            "m",
            depends_on=("u",),
            trigger_rule=dag.ALL_DONE,
            expand=ExpandSpec(from_task="u", key="items"),
        ),
    )
    ex = _Executor(spec, outcomes={"u": False})
    body = ex.run(_body(spec))
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.FAILED
    assert dag.effective_state(spec, body, "m") == dag.UPSTREAM_FAILED


def test_mapped_group_waits_for_all_instances():
    # regression: the fan-in barrier must hold -- the group is not terminal
    # (so a downstream cannot launch) until EVERY instance is terminal, even
    # if one has already failed.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w", depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["mapped"]["w"] = {"items": ["a", "b"], "expandedAt": 1.0}
    body["tasks"]["w#0"] = {"id": "w", "state": dag.FAILED}
    body["tasks"]["w#1"] = {"id": "w", "state": dag.RUNNING}
    assert dag._mapped_group_state(body, "w") == dag.RUNNING  # barrier
    body["tasks"]["w#1"]["state"] = dag.SUCCESS
    assert dag._mapped_group_state(body, "w") == dag.UPSTREAM_FAILED


def test_reconcile_protects_own_proc_without_pid():
    # regression: a task claimed by THIS process whose pid was never recorded
    # (set_pid failed / timed out) must NOT be reconciled by the same process
    # -- the proc token, set at claim time, protects the live task.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "me", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "me"
    assert body["tasks"]["a"]["pid"] is None
    body, n = _apply(
        dag.reconcile_crashed(spec, 2.0, "me", "h", lambda pid: False), body
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_added_task_does_not_block_terminalise():
    # regression: a reload that adds a task must not wedge an in-flight run
    # created under the older spec (the added task has no entry in this run).
    spec1 = _spec(TaskSpec("a"))
    body = _body(spec1)
    body, _ = _apply(dag.plan_and_claim(spec1, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=True, exit_code=0, fail_reason=None,
            now=1.0, task=spec1.by_id["a"],
        ),
        body,
    )
    spec2 = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a",)))
    body, _ = _apply(dag.plan_and_claim(spec2, 2.0, "p", "h", {}), body)
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS
    assert "b" not in body["tasks"]  # never materialised into this run


def test_fan_out_deterministic_on_replan():
    # the mapped item set is recorded once and never recomputed, even if the
    # upstream xcom "changes" underneath a later pass.
    spec = _FANOUT_SPEC
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "gen", success=True, exit_code=0, fail_reason=None,
            now=1.0, task=spec.by_id["gen"],
        ),
        body,
    )
    body, _ = _apply(
        dag.plan_and_claim(spec, 2.0, "p", "h", {"work": ["a", "b"]}), body
    )
    assert body["mapped"]["work"]["items"] == ["a", "b"]
    # a later pass offering a different list must NOT re-expand
    body, _ = _apply(
        dag.plan_and_claim(spec, 3.0, "p", "h", {"work": ["a", "b", "c"]}),
        body,
    )
    assert body["mapped"]["work"]["items"] == ["a", "b"]


# --------------------------------------------------------------------------
# Sensors
# --------------------------------------------------------------------------


def test_sensor_pokes_until_success():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    # first poke
    body, res = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    assert [i.is_sensor for i in res.launches] == [True]
    # poke returns "not yet" (nonzero) -> reschedule
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    assert _state(body, "s") == dag.RUNNING
    assert body["tasks"]["s"]["nextPokeAt"] == 110.0
    # not due yet
    body, res = _apply(dag.plan_and_claim(spec, 105.0, "p", "h", {}), body)
    assert res.launches == []
    # due: re-poke
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=True, exit_code=0, fail_reason=None,
            now=111.0, task=task,
        ),
        body,
    )
    assert _state(body, "s") == dag.SUCCESS


def test_sensor_times_out():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=25.0),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    # far past the timeout window (firstPokeAt=100, timeout=25 -> 125)
    body, res = _apply(dag.plan_and_claim(spec, 200.0, "p", "h", {}), body)
    assert _state(body, "s") == dag.FAILED
    assert res.launches == []


# --------------------------------------------------------------------------
# Approval gates
# --------------------------------------------------------------------------


def test_approval_blocks_then_approves():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("gate", type=dag.APPROVAL, depends_on=("a",)),
        TaskSpec("b", depends_on=("gate",)),
    )
    ex = _Executor(spec, outcomes={"a": True, "b": True})
    body = ex.run(_body(spec))
    # parked awaiting approval; b not launched
    assert _state(body, "gate") == dag.RUNNING
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    assert "b" not in ex.launched
    # approve
    body, result = _apply(
        dag.apply_approval(
            "gate", approved=True, by="alice", now=500.0,
            on_reject=dag.FAILED,
        ),
        body,
    )
    assert result["ok"] is True
    assert _state(body, "gate") == dag.SUCCESS
    # resume: b now runs to completion
    body = ex.run(body)
    assert body["state"] == dag.SUCCESS
    assert "b" in ex.launched


def test_approval_reject_skip_cascades():
    spec = _spec(
        TaskSpec("gate", type=dag.APPROVAL, on_reject=dag.SKIPPED),
        TaskSpec("b", depends_on=("gate",)),
    )
    body = _body(spec)
    # claim the gate (awaiting)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    body, result = _apply(
        dag.apply_approval(
            "gate", approved=False, by="bob", now=2.0, on_reject=dag.SKIPPED,
        ),
        body,
    )
    assert _state(body, "gate") == dag.SKIPPED
    # downstream cascades to skipped under all_success
    body, _ = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    assert _state(body, "b") == dag.SKIPPED
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS  # skipped is not a failure


def test_double_approval_is_noop():
    spec = _spec(TaskSpec("gate", type=dag.APPROVAL))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, r1 = _apply(
        dag.apply_approval(
            "gate", approved=True, by="a", now=2.0, on_reject=dag.FAILED
        ),
        body,
    )
    assert r1["ok"] is True
    body, r2 = _apply(
        dag.apply_approval(
            "gate", approved=False, by="b", now=3.0, on_reject=dag.FAILED
        ),
        body,
    )
    assert r2["ok"] is False  # already decided
    assert _state(body, "gate") == dag.SUCCESS


# --------------------------------------------------------------------------
# Crash reconciliation
# --------------------------------------------------------------------------


def test_reconcile_dead_task_retries():
    spec = _spec(TaskSpec("a", max_attempts=2))
    body = _body(spec)
    # claim + record a pid from a now-dead prior process
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "old-proc", 999, 1.0), body)
    assert _state(body, "a") == dag.RUNNING
    # a new process reconciles: pid 999 is dead
    body, n = _apply(
        dag.reconcile_crashed(
            spec, 10.0, "new-proc", "h", lambda pid: False
        ),
        body,
    )
    assert n == 1
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1


def test_reconcile_leaves_live_child():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "old-proc", 999, 1.0), body)
    # same host, pid still alive -> the child outlived the daemon; leave it
    body, n = _apply(
        dag.reconcile_crashed(spec, 10.0, "new-proc", "h", lambda pid: True),
        body,
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_reconcile_leaves_own_process():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-A", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "proc-A", 5, 1.0), body)
    # our own token: never reconcile (even if pid_alive says dead)
    body, n = _apply(
        dag.reconcile_crashed(spec, 2.0, "proc-A", "h", lambda pid: False),
        body,
    )
    assert n == 0
    assert _state(body, "a") == dag.RUNNING


def test_reconcile_claimed_but_never_launched():
    spec = _spec(TaskSpec("a", max_attempts=1))
    body = _body(spec)
    # a prior process claimed `a` (its proc token persisted at claim time) but
    # crashed before recording the pid; a fresh process must recover it.
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "old-proc"
    assert body["tasks"]["a"]["pid"] is None
    body, n = _apply(
        dag.reconcile_crashed(spec, 5.0, "new-proc", "h", lambda pid: True),
        body,
    )
    assert n == 1
    assert _state(body, "a") == dag.FAILED  # no attempts left -> terminal


def test_reconcile_leaves_sensor_between_pokes(monkeypatch):
    # a sensor idling between pokes has proc cleared;
    # reconciliation (which runs
    # at the top of every advance) must NOT touch it, or it would re-poke every
    # pass and defeat the poke schedule.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=30.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["proc"] is None
    assert body["tasks"]["s"]["nextPokeAt"] == 130.0
    # reconcile with a fresh proc: the idle sensor is left exactly as-is.
    body, n = _apply(
        dag.reconcile_crashed(spec, 105.0, "q", "h", lambda pid: False),
        body,
    )
    assert n == 0
    assert body["tasks"]["s"]["nextPokeAt"] == 130.0  # schedule preserved


def test_reconcile_recovers_crashed_sensor_poke():
    # a sensor whose poke crashed mid-flight (proc set, pid dead) IS recovered.
    spec = _spec(TaskSpec("s", type=dag.SENSOR, poke_timeout=1e9))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("s", "old", 999, 1.0), body)
    body, n = _apply(
        dag.reconcile_crashed(spec, 9.0, "new", "h", lambda pid: False),
        body,
    )
    assert n == 1
    assert _state(body, "s") == dag.RUNNING  # re-poke, not fail
    assert body["tasks"]["s"]["proc"] is None
    assert body["tasks"]["s"]["nextPokeAt"] == 9.0


def test_reconcile_skips_approval_gate():
    spec = _spec(TaskSpec("gate", type=dag.APPROVAL))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    body, n = _apply(
        dag.reconcile_crashed(spec, 5.0, "new-proc", "h", lambda pid: False),
        body,
    )
    assert n == 0  # a gate awaiting a human is not a crash victim
    assert body["tasks"]["gate"]["awaitingApproval"] is True


# --------------------------------------------------------------------------
# Key helpers
# --------------------------------------------------------------------------


def test_xcom_scheme():
    assert dag.xcom_scope("etl", "rid1") == "dagxcom/etl/rid1"
    assert dag.xcom_name("work#2", "out") == "work#2/out"
    assert dag.task_display_key("t", None) == "t"
    assert dag.task_display_key("t", 3) == "t#3"


def test_run_key_sanitised():
    key = dag.run_key_for_logical("2026-07-04T02:00:00+00:00")
    assert "/" not in key and " " not in key
    # deterministic
    assert key == dag.run_key_for_logical("2026-07-04T02:00:00+00:00")


# --------------------------------------------------------------------------
# `cronstable xcom` CLI (the HTTP seam monkeypatched, like the phase-5 CLI
# tests)
# --------------------------------------------------------------------------


class _ExitError(Exception):
    pass


class _FakeHTTP:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def __call__(self, method, path, *, query=None, json_body=None, data=None):
        self.calls.append(
            {"method": method, "path": path, "query": query, "data": data}
        )
        status, body = self.responses.get(path, (200, {}))
        payload = (
            body if isinstance(body, bytes) else json.dumps(body).encode()
        )
        return status, {}, payload


def _xcom_cli(monkeypatch, argv, http=None, stdin=b""):
    monkeypatch.setenv("CRONSTABLE_STATE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("CRONSTABLE_STATE_TOKEN", "tok")
    monkeypatch.setenv("CRONSTABLE_DAG_XCOM_SCOPE", "dagxcom/d/rid")
    monkeypatch.setenv("CRONSTABLE_DAG_TASKKEY", "gen")
    if http is not None:
        monkeypatch.setattr(jobcli, "_http", http)

    class _Buf:
        def __init__(self):
            self.buffer = self

        def read(self):
            return stdin

    monkeypatch.setattr(sys, "stdin", _Buf())
    loop = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(sys, "argv", ["cronstable"] + argv)
        monkeypatch.setattr(
            sys, "exit", lambda code=0: (_ for _ in ()).throw(_ExitError(code))
        )
        with pytest.raises(_ExitError) as ex:
            cronstable.__main__.main_loop(loop)
        return ex.value.args[0]
    finally:
        loop.close()


def test_xcom_push_targets_own_taskkey(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/put": (200, {"sha256": "ab", "size": 2})})
    code = _xcom_cli(
        monkeypatch, ["xcom", "push", "--key", "out"], http=http, stdin=b"hi"
    )
    assert code == 0
    call = http.calls[0]
    assert call["path"] == "/v1/artifact/put"
    assert call["query"] == {"scope": "dagxcom/d/rid", "name": "gen/out"}
    assert call["data"] == b"hi"


def test_xcom_pull_reads_upstream(monkeypatch, capsysbinary):
    http = _FakeHTTP({"/v1/artifact/get": (200, b"payload")})
    code = _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "out"],
        http=http,
    )
    assert code == 0
    assert http.calls[0]["query"]["name"] == "up/out"
    assert capsysbinary.readouterr().out == b"payload"


def test_xcom_pull_map_index(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/get": (200, b"x")})
    _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "out", "--map-index", "2"],
        http=http,
    )
    assert http.calls[0]["query"]["name"] == "up#2/out"


def test_xcom_pull_missing_is_exit_4(monkeypatch):
    http = _FakeHTTP({"/v1/artifact/get": (404, {})})
    code = _xcom_cli(
        monkeypatch,
        ["xcom", "pull", "--task", "up", "--key", "gone"],
        http=http,
    )
    assert code == jobcli.EXIT_NOT_FOUND


def test_xcom_outside_dag_errors(monkeypatch):
    # no CRONSTABLE_DAG_XCOM_SCOPE -> a clean error, not a traceback
    monkeypatch.delenv("CRONSTABLE_DAG_XCOM_SCOPE", raising=False)
    monkeypatch.setenv("CRONSTABLE_STATE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("CRONSTABLE_STATE_TOKEN", "tok")
    monkeypatch.setattr(sys, "argv", ["cronstable", "xcom", "list"])
    monkeypatch.setattr(
        sys, "exit", lambda code=0: (_ for _ in ()).throw(_ExitError(code))
    )
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(_ExitError) as ex:
            cronstable.__main__.main_loop(loop)
        assert ex.value.args[0] == jobcli.EXIT_ERROR
    finally:
        loop.close()


# --------------------------------------------------------------------------
# Config parsing + cross-section validation
# --------------------------------------------------------------------------


_ETL = """
dags:
  - name: etl
    schedule: '0 2 * * *'
    onMissed: run-all
    retainRuns: 7
    tasks:
      - id: extract
        command: 'echo x'
      - id: load
        command: 'echo y'
        dependsOn:
          - extract
        retries: 3
        retryDelaySeconds: 5
"""


def test_dag_parsed():
    cfg = _dagcfg(_ETL)
    (d,) = cfg.dags
    assert d.name == "etl"
    assert d.retain_runs == 7
    assert d.schedule_job is not None
    assert d.schedule_job.onMissed == "run-all"
    load = d.task_templates["load"]
    assert load.command == "echo y"
    spec = {t.id: t.spec for t in d.tasks}
    assert spec["load"].max_attempts == 4  # retries: 3 -> 4 attempts
    assert spec["load"].retry_delay == 5.0
    assert spec["load"].depends_on == ("extract",)


def test_dag_manual_only_no_schedule():
    cfg = _dagcfg(
        "dags:\n  - name: m\n    tasks:\n"
        "      - id: t\n        command: 'echo'\n"
    )
    assert cfg.dags[0].schedule_job is None


@pytest.mark.parametrize(
    "dags_yaml, state, match",
    [
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: t\n        command: 'echo'\n",
            "",
            "workflows require a `state` section",
            id="requires-state",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: t\n        command: 'echo'\n",
            "state:\n  path: /x\n  jobApi:\n    enabled: false\n",
            "workflows need the job API",
            id="requires-jobapi-enabled",
        ),
        pytest.param(
            "dags:\n"
            "  - name: d\n    tasks:\n      - id: a\n        command: 'e'\n"
            "  - name: d\n    tasks:\n      - id: b\n        command: 'e'\n",
            _STATE,
            "duplicate dag name",
            id="duplicate-name",
        ),
    ],
)
def test_dag_cross_section_rejected(dags_yaml, state, match):
    with pytest.raises(ConfigError, match=match):
        _xsect(dags_yaml, state=state)


@pytest.mark.parametrize(
    "dags_yaml, match",
    [
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n        dependsOn:\n"
            "          - b\n"
            "      - id: b\n        command: 'e'\n        dependsOn:\n"
            "          - a\n",
            "cycle",
            id="cycle",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n        dependsOn:\n"
            "          - ghost\n",
            "unknown task",
            id="unknown-dep",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n      - id: a\n",
            "needs a command",
            id="task-needs-command",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "      - id: b\n        command: 'e'\n        dependsOn:\n"
            "          - a\n"
            "      - id: c\n        command: 'e'\n        dependsOn:\n"
            "          - b\n        expand:\n"
            "          fromTask: a\n          key: items\n",
            "direct dependsOn",
            id="expand-not-direct-dep",
        ),
        pytest.param(
            "dags:\n  - name: d\n    retainRuns: 0\n    tasks:\n"
            "      - id: a\n        command: 'e'\n",
            "retainRuns must be >= 1",
            id="retain-runs-floor",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "        skipExitCodes:\n          - 99\n          - 0\n",
            "task 'a': skipExitCodes must be exit codes from 1 to 255, got 0",
            id="skip-code-zero",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "        skipExitCodes:\n          - 256\n          - -1\n",
            "from 1 to 255, got -1, 256",
            id="skip-code-out-of-range",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: gate\n        type: approval\n"
            "        skipExitCodes:\n          - 99\n",
            "an approval gate runs no command",
            id="skip-codes-on-a-gate",
        ),
        pytest.param(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "        triggerRule: one_failed\n",
            "all_done_min_one_failed",
            id="unknown-trigger-rule",
        ),
    ],
)
def test_dag_config_rejected(dags_yaml, match):
    with pytest.raises(ConfigError, match=match):
        _dagcfg(dags_yaml)


def test_dag_approval_needs_no_command():
    cfg = _dagcfg(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: gate\n        type: approval\n"
    )
    assert cfg.dags[0].tasks[0].type == "approval"


def test_dag_task_id_charset_rejected():
    # '#' / '/' would alias a mapped instance key or an XCom name
    with pytest.raises(ConfigError, match="may not contain"):
        _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: 'a/b'\n        command: 'e'\n"
        )
    with pytest.raises(dag.DagValidationError, match="may not contain"):
        dag.validate_graph(DagSpec.build("d", [TaskSpec("a#0")]))
    # a CR/LF (or other control char) in an id would forge daemon log lines
    for bad in ("a\nb", "a\rb", "a\tb"):
        with pytest.raises(
            dag.DagValidationError, match="control characters"
        ):
            dag.validate_graph(DagSpec.build("d", [TaskSpec(bad)]))
    # but only control chars are rejected: printable ids (space, ':', '.',
    # '-') are still accepted, so the fix narrows nothing operators may use.
    dag.validate_graph(DagSpec.build("d", [TaskSpec("a b.c-d:e")]))


# --------------------------------------------------------------------------
# Adversarial-review regressions
# --------------------------------------------------------------------------


def test_set_task_pid_fenced_to_claiming_proc():
    # A stale pid write from a superseded former owner must NOT clobber the
    # live claim's proc/pid -- doing so would fence out the real completion.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-B", "h", {}), body)
    assert body["tasks"]["a"]["proc"] == "proc-B"  # stamped at claim
    # a long-superseded former owner "proc-A" tries to record its pid
    body, changed = _apply(dag.set_task_pid("a", "proc-A", 999, 2.0), body)
    assert changed is False  # dropped: the entry is proc-B's claim now
    assert body["tasks"]["a"]["proc"] == "proc-B"  # unclobbered
    assert body["tasks"]["a"]["pid"] is None
    # the live owner's own pid write still applies
    body, changed = _apply(dag.set_task_pid("a", "proc-B", 4321, 3.0), body)
    assert changed is True
    assert body["tasks"]["a"]["pid"] == 4321


def test_set_task_pid_fenced_to_attempt():
    # A pid write stamped for a stale attempt is dropped even when the proc
    # token matches (a same-node reclaim after a retry bumps the attempt).
    spec = _spec(TaskSpec("a", max_attempts=3))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "proc-A", "h", {}), body)
    body["tasks"]["a"]["attempt"] = 1  # a newer attempt is now the live one
    body, changed = _apply(
        dag.set_task_pid("a", "proc-A", 7, 2.0, attempt=0), body
    )
    assert changed is False
    assert body["tasks"]["a"]["pid"] is None
    body, changed = _apply(
        dag.set_task_pid("a", "proc-A", 8, 3.0, attempt=1), body
    )
    assert changed is True
    assert body["tasks"]["a"]["pid"] == 8


def test_duplicate_depends_on_is_not_a_cycle():
    # regression: a repeated dependsOn entry is one edge; counting it twice
    # left a phantom indegree and a false 'cycle' rejection of an acyclic
    # graph.
    spec = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a", "a")))
    dag.validate_graph(spec)  # no raise
    ex = _Executor(spec, outcomes={"a": True, "b": True})
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS


def test_dag_duplicate_dependson_config_accepted():
    # the same graph through the YAML path: dependsOn: [a, a] must load.
    cfg = _dagcfg(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: a\n        command: 'e'\n"
        "      - id: b\n        command: 'e'\n        dependsOn:\n"
        "          - a\n          - a\n"
    )
    assert cfg.dags[0].name == "d"


def test_sensor_repoke_clears_stale_due_instant():
    # regression: poke N>=2 must clear nextPokeAt at claim time -- a stale
    # past due-instant on an in-flight poke reads as a due wake and busy-spun
    # the driver loop for the poke's whole duration.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=100.0,
            task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["nextPokeAt"] == 110.0
    # poke 2 claimed at its due instant: the in-flight poke owns the schedule
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    assert body["tasks"]["s"]["proc"] == "p"
    assert body["tasks"]["s"]["nextPokeAt"] is None
    # its completion re-sets the schedule
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=112.0,
            task=task,
        ),
        body,
    )
    assert body["tasks"]["s"]["nextPokeAt"] == 122.0


def test_sensor_completion_poke_fence():
    # regression: a delayed re-apply of poke N's completion (its mutate timed
    # out but actually landed) carries the SAME proc token and attempt as the
    # live poke N+1 -- a re-poke claim re-stamps proc and never bumps attempt,
    # so only the poke number tells them apart.  A stale poke fence must
    # no-op; the matching one must apply.
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    task = spec.by_id["s"]
    # poke 0 claimed, its completion lands (pokeCount -> 1)
    body, res = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    assert [i.poke_number for i in res.launches] == [0]
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=100.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=0,
        ),
        body,
    )
    assert applied is True
    assert body["tasks"]["s"]["pokeCount"] == 1
    # poke 1 claimed: same proc token, same attempt, only pokeCount differs
    body, res = _apply(dag.plan_and_claim(spec, 111.0, "p", "h", {}), body)
    assert [i.poke_number for i in res.launches] == [1]
    assert body["tasks"]["s"]["proc"] == "p"
    # a stale re-apply of poke 0's completion must NOT touch the live poke
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=False,
            exit_code=1,
            fail_reason=None,
            now=112.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=0,
        ),
        body,
    )
    assert applied is False
    entry = body["tasks"]["s"]
    assert entry["proc"] == "p"  # the live poke keeps its claim
    assert entry["pokeCount"] == 1
    assert entry["nextPokeAt"] is None  # in-flight poke owns the schedule
    # the live poke's own completion (matching poke fence) applies
    body, applied = _apply(
        dag.mark_task_finished(
            "s",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=113.0,
            task=task,
            expected_proc="p",
            expected_attempt=0,
            expected_poke=1,
        ),
        body,
    )
    assert applied is True
    assert _state(body, "s") == dag.SUCCESS


def test_mapped_fanout_item_cap_fails_task_cleanly():
    # regression: an oversized XCom list must FAIL the mapped task with a
    # clear reason instead of materialising thousands of instances into the
    # run document and stampeding the host.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    items = list(range(dag.MAX_MAPPED_ITEMS + 1))
    ex = _Executor(spec, outcomes={"gen": True}, xcom={"gen": items})
    body = ex.run(_body(spec))
    assert body["state"] == dag.FAILED
    assert _state(body, "work") == dag.FAILED
    assert "exceeds the cap" in body["tasks"]["work"]["failReason"]
    assert "work" not in body["mapped"]  # the flood was never materialised
    assert "work#0" not in body["tasks"]
    assert _state(body, "collect") == dag.UPSTREAM_FAILED


def test_claims_are_batched_per_pass(monkeypatch):
    # regression: one advance pass must not claim (and so launch) an
    # unbounded batch; the remainder stays claimable, the result is marked
    # deferred, and later passes drain it.
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 2)
    spec = _spec(*[TaskSpec("t{}".format(i)) for i in range(5)])
    body = _body(spec)
    body, res = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert len(res.launches) == 2
    assert res.deferred is True
    body, res = _apply(dag.plan_and_claim(spec, 2.0, "p", "h", {}), body)
    assert len(res.launches) == 2
    assert res.deferred is True
    body, res = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    assert res.deferred is False
    assert all(_state(body, "t{}".format(i)) == dag.RUNNING for i in range(5))


def _retry_after_pending(next_retry_at, pending):
    # ``pending`` claimable PENDING tasks, then (in spec order) task "r"
    # parked UP_FOR_RETRY with the given retry instant
    spec = _spec(
        *[TaskSpec("p{}".format(i)) for i in range(pending)],
        TaskSpec("r", max_attempts=3, retry_delay=0.0),
    )
    body = _body(spec)
    body["tasks"]["r"]["state"] = dag.UP_FOR_RETRY
    body["tasks"]["r"]["attempt"] = 1
    body["tasks"]["r"]["nextRetryAt"] = next_retry_at
    return spec, body


def _spy_claims(monkeypatch):
    # records the task ids _claim_task is called for, then delegates
    real_claim = dag._claim_task
    claimed = []

    def spy(task, *args, **kwargs):
        claimed.append(task.id)
        return real_claim(task, *args, **kwargs)

    monkeypatch.setattr(dag, "_claim_task", spy)
    return claimed


def test_retry_arm_skips_claim_once_quota_spent(monkeypatch):
    # _claims_full marks the result on the first over-quota claim (p1), so
    # the UP_FOR_RETRY arm sees deferred and returns without calling
    # _claim_task; the entry stays claimable for the next pass
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 1)
    claimed = _spy_claims(monkeypatch)
    spec, body = _retry_after_pending(0.0, pending=2)
    body, res = _apply(dag.plan_and_claim(spec, 10.0, "p", "h", {}), body)
    assert [i.task_id for i in res.launches] == ["p0"]
    assert res.deferred is True
    assert claimed == ["p0", "p1"]
    assert _state(body, "r") == dag.UP_FOR_RETRY
    assert body["tasks"]["r"].get("proc") is None
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 2)
    body, res = _apply(dag.plan_and_claim(spec, 11.0, "p", "h", {}), body)
    assert [i.task_id for i in res.launches] == ["p1", "r"]
    assert res.deferred is False
    assert _state(body, "r") == dag.RUNNING


def test_retry_arm_waits_for_future_retry_instant(monkeypatch):
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 1)
    claimed = _spy_claims(monkeypatch)
    spec, body = _retry_after_pending(1000.0, pending=1)
    body, res = _apply(dag.plan_and_claim(spec, 10.0, "p", "h", {}), body)
    assert [i.task_id for i in res.launches] == ["p0"]
    assert res.deferred is False
    assert claimed == ["p0"]
    assert _state(body, "r") == dag.UP_FOR_RETRY
    assert body["tasks"]["r"].get("proc") is None


def test_reload_added_dependency_does_not_wedge_run():
    # A run is created for A -> B (all_success). A config reload then adds task
    # C and repoints B at [A, C]. C is absent from the already-created run
    # document (creation materialises only the then-current tasks); it must not
    # gate B, or the run would wait on C forever and never terminalise.
    old = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a",)))
    body = _body(old)
    body["tasks"]["a"]["state"] = dag.SUCCESS  # A already ran this run
    body["tasks"]["a"]["finishedAt"] = 5.0
    reloaded = _spec(
        TaskSpec("a"),
        TaskSpec("c"),
        TaskSpec("b", depends_on=("a", "c")),
    )
    # B is ready despite C's absence, and the run drives to a terminal state.
    assert dag._deps_verdict(reloaded, body, reloaded.by_id["b"]) == "ready"
    ex = _Executor(reloaded, outcomes={"b": True})
    body = ex.run(body)
    assert "b" in ex.launched  # B actually ran
    assert _state(body, "b") == dag.SUCCESS
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS  # C's absence did not wedge it


# --------------------------------------------------------------------------
# Resource accounting on the task record (monitorResources)
# --------------------------------------------------------------------------


def test_finished_task_records_resources():
    # a monitored instance's sampled usage rides mark_task_finished into the
    # task record, and a later attempt's completion overwrites it.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a", max_attempts=2, retry_delay=0.0))
    body = _body(spec)
    task = spec.by_id["a"]
    now = 10.0
    usage1 = ResourceUsage(1.5, 0.5, 1024, 3).to_dict()
    body, res = _apply(dag.plan_and_claim(spec, now, "p", "h", {}), body)
    assert res.launches[0].task_id == "a"
    body, _ = _apply(
        dag.mark_task_finished(
            "a",
            success=False,
            exit_code=1,
            fail_reason="x",
            now=now,
            task=task,
            resources=usage1,
        ),
        body,
    )
    assert body["tasks"]["a"]["resources"] == usage1
    # retry succeeds with different usage: the record carries the latest
    body, _ = _apply(dag.plan_and_claim(spec, now + 1, "p", "h", {}), body)
    usage2 = ResourceUsage(9.0, 1.0, 4096, 8).to_dict()
    body, _ = _apply(
        dag.mark_task_finished(
            "a",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=now + 2,
            task=task,
            resources=usage2,
        ),
        body,
    )
    assert _state(body, "a") == dag.SUCCESS
    assert body["tasks"]["a"]["resources"] == usage2
    # the stored dict round-trips through the tolerant parser
    parsed = ResourceUsage.from_dict(body["tasks"]["a"]["resources"])
    assert parsed is not None and parsed.max_rss_bytes == 4096


def test_unmonitored_task_keeps_resources_none():
    # monitoring off (or nothing captured) -> resources stays None, and a
    # sensor's succeeding poke records its usage.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a"))
    ex = _Executor(spec, outcomes={"a": True})
    body = ex.run(_body(spec))
    assert body["tasks"]["a"]["resources"] is None
    spec = _spec(TaskSpec("s", type=dag.SENSOR))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    usage = ResourceUsage(0.2, 0.1, 512, 1).to_dict()
    body, _ = _apply(
        dag.mark_task_finished(
            "s",
            success=True,
            exit_code=0,
            fail_reason=None,
            now=2.0,
            task=spec.by_id["s"],
            resources=usage,
        ),
        body,
    )
    assert _state(body, "s") == dag.SUCCESS
    assert body["tasks"]["s"]["resources"] == usage


# --------------------------------------------------------------------------
# Batched pid stamping (set_task_pids)
# --------------------------------------------------------------------------


def test_set_task_pids_batch_equals_sequential():
    # applying the batch must be indistinguishable from applying the
    # per-entry set_task_pid transforms one by one at the same instant.
    spec = _spec(TaskSpec("a"), TaskSpec("b"), TaskSpec("c"))
    base = _body(spec)
    base, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), base)
    stamps = [("a", "p", 11, 0), ("b", "p", 12, 0), ("c", "p", 13, None)]
    seq = copy.deepcopy(base)
    for taskkey, proc, pid, attempt in stamps:
        seq, applied = _apply(
            dag.set_task_pid(taskkey, proc, pid, 5.0, attempt=attempt), seq
        )
        assert applied is True
    bat, applied = _apply(dag.set_task_pids(stamps, 5.0), copy.deepcopy(base))
    assert applied == 3
    assert bat == seq


def test_set_task_pids_fences_each_entry_independently():
    # one stale entry in a batch (a foreign re-claim, or a superseded
    # attempt) must be dropped on its own while the rest still applies:
    # batching removes RMWs, never a fence.
    spec = _spec(TaskSpec("a", max_attempts=3), TaskSpec("b"), TaskSpec("c"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body["tasks"]["b"]["proc"] = "other-proc"  # re-claimed elsewhere
    body["tasks"]["a"]["attempt"] = 1  # a newer attempt is the live one
    stamps = [
        ("a", "p", 11, 0),  # stale attempt: fenced out
        ("b", "p", 12, 0),  # foreign proc: fenced out
        ("c", "p", 13, 0),  # live: applies
        ("ghost", "p", 14, 0),  # no such entry: dropped
    ]
    body, applied = _apply(dag.set_task_pids(stamps, 5.0), body)
    assert applied == 1
    assert body["tasks"]["a"]["pid"] is None
    assert body["tasks"]["b"]["pid"] is None
    assert body["tasks"]["b"]["proc"] == "other-proc"  # unclobbered
    assert body["tasks"]["c"]["pid"] == 13


def test_set_task_pids_all_fenced_keeps_document():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    updated = body["updatedAt"]
    new, applied = dag.set_task_pids([("a", "other", 9, 0)], 5.0)(body)
    assert dag.is_keep(new) and applied == 0
    assert body["updatedAt"] == updated  # not even the timestamp moved
    new, applied = dag.set_task_pids([("a", "p", 9, 0)], 5.0)(None)
    assert dag.is_keep(new) and applied == 0


# --------------------------------------------------------------------------
# Releasing a claim whose launch intents the driver never received
# --------------------------------------------------------------------------


def test_release_lost_claims_resets_plain_task_and_sensor():
    # the plain task goes back to pending with its attempt intact and its
    # start cleared (the re-claim stamps the real launch), the sensor to
    # its idle shape due now with its poke-timeout clock intact, and the
    # next claim pass picks both up again as the SAME attempt / poke.
    spec = _spec(
        TaskSpec("a", max_attempts=3),
        TaskSpec("s", type=dag.SENSOR, poke_interval=30.0, poke_timeout=1e9),
    )
    body = _body(spec)
    body, res = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert sorted(li.taskkey for li in res.launches) == ["a", "s"]
    claims = [("a", "p", 0, 0), ("s", "p", 0, 0)]
    body, released = _apply(dag.release_lost_claims(spec, claims, 5.0), body)
    assert released == ["a", "s"]
    a = body["tasks"]["a"]
    assert (a["state"], a["proc"], a["pid"], a["attempt"]) == (
        dag.PENDING,
        None,
        None,
        0,
    )
    assert a["startedAt"] is None
    s = body["tasks"]["s"]
    assert (s["state"], s["proc"], s["pid"]) == (dag.RUNNING, None, None)
    assert s["nextPokeAt"] == 5.0
    assert s["pokeCount"] == 0
    assert (s["startedAt"], s["firstPokeAt"]) == (1.0, 1.0)
    assert body["updatedAt"] == 5.0
    body, res = _apply(dag.plan_and_claim(spec, 6.0, "p", "h", {}), body)
    got = {li.taskkey: (li.attempt, li.poke_number) for li in res.launches}
    assert got == {"a": (0, 0), "s": (0, 0)}
    assert body["tasks"]["a"]["startedAt"] == 6.0


def test_plain_claim_clears_a_stale_poke_count():
    # a task retyped from a sensor keeps its poke count on the entry; the
    # plain claim registers poke 0, so the entry must read 0 too, or the
    # driver's launch registry would mistake the live launch for a lost
    # claim and release it under its running subprocess
    spec = _spec(TaskSpec("a", max_attempts=3))
    body = _body(spec)
    body["tasks"]["a"]["pokeCount"] = 1
    body, res = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert [(li.taskkey, li.poke_number) for li in res.launches] == [("a", 0)]
    assert "pokeCount" not in body["tasks"]["a"]


def test_release_lost_claims_is_fenced():
    # only the exact claim (proc, attempt, poke) is undone; an entry that
    # moved since is left alone, and an all-fenced batch keeps the document.
    spec = _spec(
        TaskSpec("a", max_attempts=3),
        TaskSpec("s", type=dag.SENSOR, poke_interval=0.0, poke_timeout=1e9),
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    updated = body["updatedAt"]
    body["tasks"]["s"]["pokeCount"] = 1  # a later poke is the live one
    claims = [
        ("a", "other", 0, 0),  # foreign proc: re-claimed elsewhere
        ("a", "p", 1, 0),  # stale attempt
        ("s", "p", 0, 0),  # stale poke
        ("ghost", "p", 0, 0),  # no such entry
    ]
    new, released = dag.release_lost_claims(spec, claims, 5.0)(body)
    assert dag.is_keep(new) and released == []
    assert body["tasks"]["a"]["proc"] == "p"
    assert body["tasks"]["s"]["proc"] == "p"
    assert body["updatedAt"] == updated
    # a finished entry is not RUNNING: nothing to undo
    body["tasks"]["a"]["state"] = dag.SUCCESS
    claims = [("a", "p", 0, 0)]
    new, released = dag.release_lost_claims(spec, claims, 6.0)(body)
    assert dag.is_keep(new) and released == []
    # no document, or a terminal run
    new, released = dag.release_lost_claims(spec, claims, 6.0)(None)
    assert dag.is_keep(new) and released == []
    body["state"] = dag.SUCCESS
    claims = [("s", "p", 0, 1)]
    new, released = dag.release_lost_claims(spec, claims, 6.0)(body)
    assert dag.is_keep(new) and released == []


# --------------------------------------------------------------------------
# Combined reconcile+claim transform (reconcile_and_plan)
# --------------------------------------------------------------------------


def test_reconcile_and_plan_single_pass_reconciles_and_reclaims():
    # the common case: no expansions pending, so ONE transform application
    # both recovers the crashed claim and re-claims it for launch.
    spec = _spec(TaskSpec("a", max_attempts=2))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(dag.set_task_pid("a", "old-proc", 999, 1.0), body)
    tf = dag.reconcile_and_plan(
        spec, 10.0, "new-proc", "h", lambda pid: False
    )
    body, res = _apply(tf, body)
    assert res.reconciled == 1
    assert res.expansions_needed is False
    assert [i.taskkey for i in res.advance.launches] == ["a"]
    assert res.advance.launches[0].attempt == 1
    assert _state(body, "a") == dag.RUNNING
    assert body["tasks"]["a"]["proc"] == "new-proc"
    assert body["tasks"]["a"]["attempt"] == 1
    assert body["updatedAt"] == 10.0


def test_reconcile_and_plan_keeps_missing_or_terminal_body():
    spec = _spec(TaskSpec("a"))
    tf = dag.reconcile_and_plan(spec, 1.0, "p", "h", lambda pid: False)
    new, res = tf(None)
    assert dag.is_keep(new)
    assert res.reconciled == 0 and res.advance.launches == []
    body = _body(spec)
    body["state"] = dag.SUCCESS
    new, res = tf(body)
    assert dag.is_keep(new)


# --------------------------------------------------------------------------
# Run engine level: a build leaves a run above its own level untouched
# --------------------------------------------------------------------------


def test_new_run_body_records_engine_only_above_base():
    spec = _spec(TaskSpec("a"))
    assert spec.engine == dag.BASE_ENGINE_LEVEL
    assert "engine" not in _body(spec)
    needs_more = dataclasses.replace(spec, engine=dag.BASE_ENGINE_LEVEL + 1)
    assert _body(needs_more)["engine"] == dag.BASE_ENGINE_LEVEL + 1


@pytest.mark.parametrize(
    "body, supported",
    [
        ({}, True),  # no key: the base level every build supports
        ({"engine": dag.BASE_ENGINE_LEVEL}, True),
        ({"engine": dag.ENGINE_LEVEL}, True),
        ({"engine": dag.ENGINE_LEVEL + 1}, False),
        # anything but an integer fails closed
        ({"engine": None}, False),
        ({"engine": "1"}, False),
        ({"engine": 1.0}, False),
        ({"engine": True}, False),
    ],
)
def test_supports_run(body, supported):
    assert dag.supports_run(body) is supported


def test_transforms_keep_a_run_above_the_engine_level():
    spec = _spec(TaskSpec("a", max_attempts=2), TaskSpec("b"))
    body = _body(spec)
    # "a" is a crashed foreign claim the reconcile half would recover and
    # "b" is ready to claim: every transform has work to do on this body.
    body["tasks"]["a"].update(
        state=dag.RUNNING, proc="dead-proc", pid=999, host="h"
    )
    supported = copy.deepcopy(body)
    body["engine"] = dag.ENGINE_LEVEL + 1
    before = copy.deepcopy(body)

    def dead(pid):
        return False

    new, res = dag.reconcile_and_plan(spec, 5.0, "me", "h", dead)(body)
    assert dag.is_keep(new)
    assert res.unsupported is True
    assert res.reconciled == 0 and res.advance.launches == []
    new, claimed = dag.plan_and_claim(spec, 5.0, "me", "h", {})(body)
    assert dag.is_keep(new)
    assert claimed.launches == [] and claimed.changed is False
    new, changed = dag.reconcile_crashed(spec, 5.0, "me", "h", dead)(body)
    assert dag.is_keep(new) and changed == 0
    assert body == before  # nothing was mutated in place either

    # the same body at a supported level is reconciled and claimed
    new, res = dag.reconcile_and_plan(spec, 5.0, "me", "h", dead)(supported)
    assert res.unsupported is False
    assert res.reconciled == 1
    assert {i.taskkey for i in res.advance.launches} == {"a", "b"}


def test_reconcile_and_plan_flags_pending_expansion():
    # a mapped task awaiting its upstream list: the transform applies ONLY
    # the reconcile half and flags expansions_needed, so the driver runs the
    # classic pre-read + plan_and_claim RMW as the second step.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("x", max_attempts=2),
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "gen", success=True, exit_code=0, fail_reason=None,
            now=2.0, task=spec.by_id["gen"],
        ),
        body,
    )
    # gen succeeded (work now awaits expansion); x crashed under a dead
    # owner and must still be reconciled by the first half.
    tf = dag.reconcile_and_plan(
        spec, 10.0, "new-proc", "h", lambda pid: False
    )
    body, res = _apply(tf, body)
    assert res.expansions_needed is True
    assert res.advance is None  # the claim half did NOT run
    assert res.reconciled == 1
    assert _state(body, "x") == dag.UP_FOR_RETRY  # reconciled, not claimed
    assert "work" not in body["mapped"]
    assert _state(body, "work") == dag.PENDING
    # the driver's second step then expands and claims in one RMW as before
    body, res2 = _apply(
        dag.plan_and_claim(spec, 11.0, "new-proc", "h", {"work": ["i"]}),
        body,
    )
    assert body["mapped"]["work"]["items"] == ["i"]
    keys = {i.taskkey for i in res2.launches}
    assert keys == {"work#0", "x"}


def test_reconcile_and_plan_expansion_pending_nothing_to_reconcile_keeps():
    spec = _FANOUT_SPEC
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "gen", success=True, exit_code=0, fail_reason=None,
            now=2.0, task=spec.by_id["gen"],
        ),
        body,
    )
    tf = dag.reconcile_and_plan(spec, 3.0, "p", "h", lambda pid: False)
    new, res = tf(body)
    assert dag.is_keep(new)  # nothing recovered: no write for the flag
    assert res.expansions_needed is True
    assert res.advance is None
    assert res.reconciled == 0


def test_reconcile_and_plan_defeated_by_foreign_claim():
    # a foreign running claim always defeats the quiescence short-circuit:
    # its owner may be dead, and only the full pass (reconcile) can tell.
    spec = _spec(TaskSpec("a", max_attempts=1), TaskSpec("b", max_attempts=1))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "old-proc", "h", {}), body)
    tf = dag.reconcile_and_plan(
        spec, 5.0, "new-proc", "h", lambda pid: False
    )
    body, res = _apply(tf, body)
    assert res.reconciled == 2
    assert res.advance.run_terminal is True  # no attempts left: run over
    assert body["state"] == dag.FAILED


# --------------------------------------------------------------------------
# Quiescence short-circuit (the read-only pre-scan)
# --------------------------------------------------------------------------


def test_quiescent_all_running_returns_keep_and_never_mutates():
    # an all-in-flight document (every instance claimed under OUR proc
    # token, nothing due) is the canonical quiescent shape: both claim
    # transforms must keep it, and the pre-scan must be strictly read-only.
    spec = _spec(TaskSpec("a"), TaskSpec("b"))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.set_task_pids([("a", "p", 1, 0), ("b", "p", 2, 0)], 1.0), body
    )
    snapshot = copy.deepcopy(body)
    new, res = dag.plan_and_claim(spec, 2.0, "p", "h", {})(body)
    assert dag.is_keep(new)
    assert res.launches == [] and res.changed is False
    tf = dag.reconcile_and_plan(spec, 2.0, "p", "h", lambda pid: False)
    new, cres = tf(body)
    assert dag.is_keep(new)
    assert cres.reconciled == 0 and cres.advance.launches == []
    assert body == snapshot  # the pre-scan touched nothing


def test_quiescence_defeated_by_due_retry():
    spec = _spec(
        TaskSpec("a", max_attempts=2, retry_delay=50.0), TaskSpec("b")
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x",
            now=1.0, task=spec.by_id["a"],
        ),
        body,
    )
    # inside the backoff (due at 51): quiescent, kept without a copy
    new, res = dag.plan_and_claim(spec, 20.0, "p", "h", {})(body)
    assert dag.is_keep(new) and res.launches == []
    # at the SAME now the transform receives, the due retry defeats it
    body, res = _apply(dag.plan_and_claim(spec, 51.0, "p", "h", {}), body)
    assert [i.taskkey for i in res.launches] == ["a"]


def test_quiescence_defeated_by_due_poke():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9),
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 100.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason=None,
            now=100.0, task=spec.by_id["s"],
        ),
        body,
    )
    # idle until 110: quiescent
    new, res = dag.plan_and_claim(spec, 105.0, "p", "h", {})(body)
    assert dag.is_keep(new) and res.launches == []
    # due at the transform's own now: re-pokes
    body, res = _apply(dag.plan_and_claim(spec, 110.0, "p", "h", {}), body)
    assert len(res.launches) == 1
    # and with the poke now in flight (proc re-stamped) it is quiescent
    # again
    new, res = dag.plan_and_claim(spec, 120.0, "p", "h", {})(body)
    assert dag.is_keep(new)


def test_quiescence_never_blocks_terminalisation():
    # every task terminal but the run not yet marked: the pre-scan finds no
    # blocking entry and must fall through to the full pass, which
    # terminalises.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    body, res = _apply(dag.plan_and_claim(spec, 5.0, "p", "h", {}), body)
    assert res.run_terminal is True
    assert body["state"] == dag.SUCCESS


def test_quiescent_approval_gate_keeps_document():
    spec = _spec(TaskSpec("gate", type=dag.APPROVAL))
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert body["tasks"]["gate"]["awaitingApproval"] is True
    new, _res = dag.plan_and_claim(spec, 2.0, "p", "h", {})(body)
    assert dag.is_keep(new)
    # a parked gate blocks for a DIFFERENT proc token too (reconcile skips
    # gates, so the combined transform is just as inert)
    tf = dag.reconcile_and_plan(spec, 2.0, "q", "h", lambda pid: False)
    new, _res = tf(body)
    assert dag.is_keep(new)


def test_quiescent_mapped_fanout_in_flight():
    # the large-document case the short-circuit exists for: a fan-out whose
    # instances are all claimed under our token idles as a plain read.
    spec = _FANOUT_SPEC
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "gen", success=True, exit_code=0, fail_reason=None,
            now=2.0, task=spec.by_id["gen"],
        ),
        body,
    )
    body, _ = _apply(
        dag.plan_and_claim(spec, 3.0, "p", "h", {"work": ["a", "b"]}), body
    )
    assert _state(body, "work#0") == dag.RUNNING
    new, _res = dag.plan_and_claim(spec, 4.0, "p", "h", {})(body)
    assert dag.is_keep(new)
    tf = dag.reconcile_and_plan(spec, 4.0, "p", "h", lambda pid: False)
    new, _res = tf(body)
    assert dag.is_keep(new)
    # an instance that cannot be matched to a consulted slot (its recorded
    # mapIndex is out of the item range) is doubt: no short-circuit
    body["tasks"]["work#1"]["mapIndex"] = 5
    assert dag._is_quiescent(spec, body, 4.0, "p", None) is False


def test_quiescence_is_conservative_on_odd_entries():
    # every "in doubt" branch must resolve to NOT quiescent: the only cost
    # is running the full pass, never skipping real work.
    spec = _spec(TaskSpec("a"))
    # 1. a non-terminal entry of a task the spec no longer has must not
    # hold the short-circuit open: terminalisation ignores it, so keeping
    # the document on its account would wedge the run forever.
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    body["tasks"]["ghost"] = {"id": "ghost", "state": dag.RUNNING, "proc": "p"}
    assert dag._is_quiescent(spec, body, 5.0, "p", None) is False
    body, res = _apply(dag.plan_and_claim(spec, 5.0, "p", "h", {}), body)
    assert res.run_terminal is True  # the full pass finished the run
    # 2. an entry whose key does not match its recorded task id cannot be
    # proven consulted: doubt
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    body["tasks"]["weird"] = {"id": "a", "state": dag.RUNNING, "proc": "p"}
    assert dag._is_quiescent(spec, body, 5.0, "p", None) is False
    # 3. an unreadable retry instant: doubt
    spec2 = _spec(TaskSpec("a", max_attempts=2))
    body = _body(spec2)
    body["tasks"]["a"]["state"] = dag.UP_FOR_RETRY
    body["tasks"]["a"]["nextRetryAt"] = "soon"
    assert dag._is_quiescent(spec2, body, 5.0, "p", None) is False
    # 4. an unreadable (or missing) poke instant on an idle sensor: doubt
    spec3 = _spec(TaskSpec("s", type=dag.SENSOR))
    body = _body(spec3)
    body["tasks"]["s"]["state"] = dag.RUNNING
    body["tasks"]["s"]["nextPokeAt"] = "later"
    assert dag._is_quiescent(spec3, body, 5.0, "p", None) is False
    body["tasks"]["s"]["nextPokeAt"] = None
    assert dag._is_quiescent(spec3, body, 5.0, "p", None) is False
    # 5. a proc-less RUNNING plain task (a shape a claim never writes):
    # doubt
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.RUNNING
    assert dag._is_quiescent(spec, body, 5.0, "p", None) is False
    # 6. an expanded placeholder whose task the spec stopped mapping: doubt
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.EXPANDED
    assert dag._is_quiescent(spec, body, 5.0, "p", None) is False
    # 7. a usable pre-read expansion defeats an otherwise quiescent body;
    # an unreadable one (None) does not
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert dag._is_quiescent(spec, body, 2.0, "p", {"x": ["i"]}) is False
    assert dag._is_quiescent(spec, body, 2.0, "p", {"x": None}) is True
    # 8. no tasks at all: nothing blocks terminalisation, so no
    # short-circuit
    body = _body(spec)
    body["tasks"] = {}
    assert dag._is_quiescent(spec, body, 5.0, "p", None) is False


def test_task_record_without_resources_field_still_parses():
    # backward compat: a pre-feature dag_run document has no "resources" key
    # on its task entries; completing and reading it must not care.
    from cronstable.resources import ResourceUsage

    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    for entry in body["tasks"].values():
        entry.pop("resources", None)  # simulate an old document
    ex = _Executor(spec, outcomes={"a": True})
    body = ex.run(body)
    assert _state(body, "a") == dag.SUCCESS
    assert body["tasks"]["a"].get("resources") is None
    # a malformed stored value parses to None instead of raising
    assert ResourceUsage.from_dict(body["tasks"]["a"].get("resources")) is None
    assert ResourceUsage.from_dict("garbage") is None
    assert ResourceUsage.from_dict({"cpu_user_seconds": "nan?"}) is None


# --------------------------------------------------------------------------
# Internal helper edge cases (pure functions driven against hand-built bodies)
#
# These exercise the low-level state-machine helpers -- _apply_expansions,
# _propagate_placeholder, _advance_task, _is_quiescent and friends -- directly,
# hitting the defensive/no-op branches the higher-level executor tests above
# do not walk through.
# --------------------------------------------------------------------------


# validate_graph / _validate_expand edge cases (empty id, self-dep, phantom
# expand source) live as rows of test_validate_graph_rejects above.


# _mapped_group_state: the all-skipped reduction


def test_mapped_group_all_skipped_reduces_to_skipped():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["mapped"]["w"] = {"items": ["a", "b"], "expandedAt": 1.0}
    body["tasks"]["w#0"] = {"id": "w", "state": dag.SKIPPED}
    body["tasks"]["w#1"] = {"id": "w", "state": dag.SKIPPED}
    assert dag._mapped_group_state(body, "w") == dag.SKIPPED
    # a success sibling alongside a skipped one still reduces to skipped (no
    # failure present).
    body["tasks"]["w#0"]["state"] = dag.SUCCESS
    assert dag._mapped_group_state(body, "w") == dag.SKIPPED


# tasks_awaiting_expansion: terminal run + already-resolved placeholder


def test_awaiting_expansion_empty_on_terminal_run():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["state"] = dag.SUCCESS  # terminal run
    assert dag.tasks_awaiting_expansion(spec, body) == []


def test_awaiting_expansion_skips_resolved_placeholder():
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    # the placeholder already resolved (upstream_failed) without expanding:
    # it must not be re-offered for an XCom pre-read every pass.
    body["tasks"]["w"]["state"] = dag.UPSTREAM_FAILED
    assert dag.tasks_awaiting_expansion(spec, body) == []


# plan_and_claim transform: no-op on None / terminal body


def test_plan_and_claim_noop_on_none_and_terminal():
    spec = _spec(TaskSpec("a"))
    transform = dag.plan_and_claim(spec, 5.0, "p", "h", {})
    new, result = transform(None)
    assert dag.is_keep(new)
    assert result.launches == []
    body = _body(spec)
    body["state"] = dag.FAILED
    new, result = transform(body)
    assert dag.is_keep(new)
    assert result.launches == []


# _apply_expansions: the three skip branches


def test_apply_expansions_skip_branches():
    spec = _FANOUT_SPEC
    # (1) items is None -> unknown read, left for a later pass.
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    result = dag.AdvanceResult()
    dag._apply_expansions(spec, body, {"work": None}, 1.0, result)
    assert body["mapped"] == {}
    assert result.changed is False

    # (2) target has no expand (or is unknown): nothing to materialise.
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    result = dag.AdvanceResult()
    dag._apply_expansions(
        spec, body, {"gen": [1, 2], "ghost": [1]}, 1.0, result
    )
    assert body["mapped"] == {}
    assert result.changed is False

    # (3) upstream is not (yet) success under this fresh body: no fan-out.
    body = _body(spec)  # gen still pending
    result = dag.AdvanceResult()
    dag._apply_expansions(spec, body, {"work": [1, 2]}, 1.0, result)
    assert body["mapped"] == {}
    assert "work#0" not in body["tasks"]
    assert result.changed is False


# _instances_of: an un-expanded mapped task has no concrete instances


def test_instances_of_unexpanded_mapped_is_empty():
    spec = _FANOUT_SPEC
    body = _body(spec)  # no mapped entry yet
    assert dag._instances_of(spec, body, spec.by_id["work"]) == []
    # a plain task is always exactly one instance keyed by its id.
    assert dag._instances_of(spec, body, spec.by_id["gen"]) == [
        ("gen", None, None)
    ]


# _propagate_placeholder: source skipped, and a sibling-dep fail/skip verdict


def test_propagate_placeholder_source_skipped():
    spec = _FANOUT_SPEC
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SKIPPED
    result = dag.AdvanceResult()
    dag._propagate_placeholder(
        spec, body, spec.by_id["work"], 1.0, result
    )
    assert _state(body, "work") == dag.SKIPPED
    assert result.changed is True


def test_propagate_placeholder_sibling_dep_fail_and_skip():
    # the expand source succeeds, but a NON-expand dependency terminalises the
    # placeholder through the ordinary deps verdict.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec("other"),
        TaskSpec(
            "work",
            depends_on=("gen", "other"),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["other"]["state"] = dag.FAILED
    result = dag.AdvanceResult()
    dag._propagate_placeholder(spec, body, spec.by_id["work"], 1.0, result)
    assert _state(body, "work") == dag.UPSTREAM_FAILED

    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["other"]["state"] = dag.SKIPPED
    result = dag.AdvanceResult()
    dag._propagate_placeholder(spec, body, spec.by_id["work"], 1.0, result)
    assert _state(body, "work") == dag.SKIPPED


# _advance_task: unknown-state no-op + defensive verdict computation


def test_advance_task_unknown_state_is_noop():
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    entry = body["tasks"]["a"]
    entry["state"] = "not-a-real-state"
    result = dag.AdvanceResult()
    dag._advance_task(
        spec, body, spec.by_id["a"], "a", None, None, entry, 5.0,
        "p", "h", result,
    )
    assert entry["state"] == "not-a-real-state"
    assert result.changed is False
    assert result.launches == []


def test_advance_task_direct_call_computes_verdict():
    # a direct call passes verdict=None; the task-level verdict is computed
    # defensively and a ready pending task is claimed.
    spec = _spec(TaskSpec("a"))
    body = _body(spec)
    entry = body["tasks"]["a"]
    result = dag.AdvanceResult()
    dag._advance_task(
        spec, body, spec.by_id["a"], "a", None, None, entry, 5.0,
        "p", "h", result,
    )
    assert entry["state"] == dag.RUNNING
    assert [i.task_id for i in result.launches] == ["a"]


# _advance_running: poke-in-flight / not-due / launch-quota-spent


def test_advance_running_leaves_inflight_and_not_due_pokes():
    task = TaskSpec("s", type=dag.SENSOR, poke_interval=10.0)
    # a poke is in flight (proc set): leave it alone.
    entry = {"state": dag.RUNNING, "proc": "p", "pid": None, "pokeCount": 1}
    result = dag.AdvanceResult()
    dag._advance_running(task, "s", None, None, entry, 100.0, "p", "h", result)
    assert result.changed is False
    assert result.launches == []
    # idle but not yet due (nextPokeAt in the future): leave it alone.
    entry = {
        "state": dag.RUNNING,
        "proc": None,
        "pid": None,
        "nextPokeAt": 200.0,
        "pokeCount": 1,
    }
    result = dag.AdvanceResult()
    dag._advance_running(task, "s", None, None, entry, 100.0, "p", "h", result)
    assert result.changed is False
    assert result.launches == []


def test_advance_running_defers_when_quota_spent(monkeypatch):
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 0)
    task = TaskSpec("s", type=dag.SENSOR, poke_interval=10.0, poke_timeout=1e9)
    entry = {
        "state": dag.RUNNING,
        "proc": None,
        "pid": None,
        "nextPokeAt": None,
        "firstPokeAt": 99.0,
        "pokeCount": 1,
        "attempt": 0,
    }
    result = dag.AdvanceResult()
    dag._advance_running(task, "s", None, None, entry, 100.0, "p", "h", result)
    assert result.launches == []
    assert result.deferred is True
    assert entry["proc"] is None  # not claimed this pass


# _sensor_timed_out: no first poke recorded -> not timed out


def test_sensor_timed_out_without_first_poke():
    task = TaskSpec("s", type=dag.SENSOR, poke_timeout=25.0)
    assert dag._sensor_timed_out(task, {}, 1000.0) is False
    # once a first poke instant is present, the timeout window applies.
    assert dag._sensor_timed_out(task, {"firstPokeAt": 100.0}, 130.0) is True
    assert dag._sensor_timed_out(task, {"firstPokeAt": 100.0}, 110.0) is False


# _maybe_terminalise: a post-creation mapped placeholder never blocks the run


def test_maybe_terminalise_ignores_unmaterialised_mapped_task():
    spec = _FANOUT_SPEC
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    # `work` was added by a reload after the run doc was created: it has no
    # entry in this run, so it must not gate terminalisation.
    del body["tasks"]["work"]
    result = dag.AdvanceResult()
    dag._maybe_terminalise(spec, body, 5.0, result)
    assert body["state"] == dag.SUCCESS
    assert result.changed is True


# _fold_mapped_instances: the barrier and the terminaliser share one
# absent-entry rule


def test_absent_mapped_instance_holds_run_open_like_the_barrier():
    # regression: the terminaliser once SKIPPED an entry missing for a
    # run-recorded instance index while the fan-in barrier read the same
    # hole as pending, so a damaged run document could complete as a run
    # whose mapped group still read "running" to every downstream.  Both
    # consumers now share _fold_mapped_instances: the hole holds the run
    # open.  This calls the terminaliser directly, so it pins that rule in
    # isolation; a real advance pass runs _propagate_and_claim first, which
    # repairs the hole (see the test below) rather than leaving it open.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["mapped"]["w"] = {"items": ["a", "b"], "expandedAt": 1.0}
    body["tasks"]["w#0"] = {"id": "w", "state": dag.SUCCESS}
    body["tasks"].pop("w#1", None)  # the hole: a run-recorded index, absent
    assert dag._mapped_group_state(body, "w") == dag.RUNNING  # barrier holds
    result = dag.AdvanceResult()
    dag._maybe_terminalise(spec, body, 5.0, result)
    assert result.run_terminal is False  # ...and so must the run
    # both verdicts agree once the hole is filled terminally
    body["tasks"]["w#1"] = {"id": "w", "state": dag.SUCCESS}
    assert dag._mapped_group_state(body, "w") == dag.SUCCESS
    dag._maybe_terminalise(spec, body, 6.0, result)
    assert result.run_terminal is True


def test_claim_pass_repairs_a_missing_mapped_instance():
    # The other half of the rule above. Holding the run open is only safe
    # if something can fill the hole, and nothing else can: the reconcile
    # pass iterates the entries that EXIST and both GC paths only touch
    # already-terminal runs, so a hole used to wedge the run forever,
    # renewing its advance lease for the life of the daemon and never
    # becoming eligible for retention. The claim pass, which runs before
    # the terminaliser in the same transform, now materialises a
    # run-recorded index it finds missing and fails it with a reason.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "w",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["mapped"]["w"] = {"items": ["a", "b"], "expandedAt": 1.0}
    body["tasks"]["w#0"] = {"id": "w", "state": dag.SUCCESS}
    body["tasks"].pop("w#1", None)  # the hole
    result = dag.AdvanceResult()
    dag._propagate_and_claim(spec, body, 5.0, "proc", "host", result)
    entry = body["tasks"]["w#1"]
    assert entry["state"] == dag.FAILED
    assert entry["mapIndex"] == 1
    assert entry["mapItem"] == "b"  # the item the run recorded for it
    assert "holds no entry" in entry["failReason"]
    assert result.changed is True
    # and now the run can finish, so its lease releases and GC can prune
    dag._maybe_terminalise(spec, body, 6.0, result)
    assert result.run_terminal is True
    assert body["state"] == dag.FAILED


def test_claim_pass_leaves_a_plain_task_with_no_entry_alone():
    # The repair is scoped to MAPPED indices the run itself recorded. A
    # plain task absent from the document is the deliberate "added by a
    # config reload after the run was created" case, which both the claim
    # pass and the terminaliser skip; materialising a failure for it would
    # fail runs for work that was never part of their plan.
    spec = _spec(TaskSpec("a"), TaskSpec("b", depends_on=("a",)))
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    body["tasks"].pop("b")  # as a reload adding task "b" would leave it
    result = dag.AdvanceResult()
    dag._propagate_and_claim(spec, body, 5.0, "proc", "host", result)
    assert "b" not in body["tasks"]
    dag._maybe_terminalise(spec, body, 6.0, result)
    assert result.run_terminal is True
    assert body["state"] == dag.SUCCESS


def test_a_null_task_entry_is_refused_before_any_mutation():
    # tasks: {x: null} only appears in a damaged or foreign document, and
    # the readers disagree about it (absent to the dependency check,
    # pending to effective_state). Without the up-front refusal the
    # outcome depended on entry order: a null scanned first crashed the
    # pass, one scanned later silently un-gated its dependents.
    spec = _spec(TaskSpec("up"), TaskSpec("down", depends_on=("up",)))
    body = _body(spec)
    body["tasks"]["up"] = None
    transform = dag.plan_and_claim(spec, 1.0, "p", "h", {})
    with pytest.raises(ValueError, match="task entry 'up' is null"):
        transform(body)
    assert body["tasks"]["down"]["state"] == dag.PENDING  # untouched


def test_a_null_mapped_map_is_refused_before_any_mutation():
    # mapped: null is the same class of damage. The read paths all treat
    # it as "no fan-out", but the expansion write path cannot, so without
    # the refusal a run claimed real work first and wedged only when the
    # first expansion tried to record itself.
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec("w", expand=ExpandSpec(from_task="gen", key="items")),
    )
    body = _body(spec)
    body["mapped"] = None
    transform = dag.plan_and_claim(spec, 1.0, "p", "h", {})
    with pytest.raises(ValueError, match="'mapped' is null"):
        transform(body)
    assert body["tasks"]["gen"]["state"] == dag.PENDING  # never claimed


# set_task_pid: no-op on missing run / non-running entry


def test_set_task_pid_noop_on_none_and_non_running():
    spec = _spec(TaskSpec("a"))
    transform = dag.set_task_pid("a", "p", 1234, 1.0)
    new, changed = transform(None)
    assert dag.is_keep(new)
    assert changed is False
    # entry present but still pending (never claimed): nothing to stamp.
    body = _body(spec)
    new, changed = transform(body)
    assert dag.is_keep(new)
    assert changed is False
    assert body["tasks"]["a"]["pid"] is None


# mark_task_finished: no-op on None / duplicate / attempt fence


def test_mark_task_finished_noop_none_and_duplicate():
    spec = _spec(TaskSpec("a"))
    task = spec.by_id["a"]
    transform = dag.mark_task_finished(
        "a", success=True, exit_code=0, fail_reason=None, now=1.0, task=task
    )
    new, changed = transform(None)
    assert dag.is_keep(new)
    assert changed is False
    # already terminal (a duplicate completion): a no-op.
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    new, changed = transform(body)
    assert dag.is_keep(new)
    assert changed is False


def test_mark_task_finished_attempt_fence_with_matching_proc():
    # proc matches the live claim but the attempt does not: a stale attempt's
    # completion is dropped by the attempt fence (line reached only when the
    # proc fence passes first).
    spec = _spec(TaskSpec("a", max_attempts=3))
    task = spec.by_id["a"]
    body = _body(spec)
    entry = body["tasks"]["a"]
    entry["state"] = dag.RUNNING
    entry["proc"] = "proc-A"
    entry["attempt"] = 2
    transform = dag.mark_task_finished(
        "a",
        success=True,
        exit_code=0,
        fail_reason=None,
        now=1.0,
        task=task,
        expected_proc="proc-A",
        expected_attempt=0,
    )
    new, changed = transform(body)
    assert dag.is_keep(new)
    assert changed is False
    assert _state(body, "a") == dag.RUNNING  # the live attempt is untouched


# mark_tasks_finished: batch apply, per-entry fences, and the empty result


def test_mark_tasks_finished_batch_applies_and_fences():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("s", type=dag.SENSOR, poke_interval=10.0),
        TaskSpec("done"),
        TaskSpec("pf"),
        TaskSpec("af", max_attempts=3),
        TaskSpec("pk", type=dag.SENSOR, poke_interval=10.0),
    )
    body = _body(spec)
    for tid in ("a", "s", "pf", "af", "pk"):
        body["tasks"][tid]["state"] = dag.RUNNING
    body["tasks"]["a"]["proc"] = "p"
    body["tasks"]["s"]["proc"] = "p"
    body["tasks"]["pf"]["proc"] = "realproc"
    body["tasks"]["af"]["proc"] = "p"
    body["tasks"]["af"]["attempt"] = 0
    body["tasks"]["pk"]["proc"] = "p"
    body["tasks"]["pk"]["pokeCount"] = 0
    body["tasks"]["done"]["state"] = dag.SUCCESS  # already terminal

    marks = [
        {
            "taskkey": "a", "success": True, "exit_code": 0,
            "fail_reason": None, "task": spec.by_id["a"],
        },
        {"taskkey": "s", "success": False, "task": spec.by_id["s"]},
        {"taskkey": "done", "success": True, "task": spec.by_id["done"]},
        {
            "taskkey": "pf", "success": True, "task": spec.by_id["pf"],
            "expected_proc": "other",
        },
        {
            "taskkey": "af", "success": True, "task": spec.by_id["af"],
            "expected_attempt": 5,
        },
        {
            "taskkey": "pk", "success": True, "task": spec.by_id["pk"],
            "expected_poke": 9,
        },
    ]
    new, applied = dag.mark_tasks_finished(marks, 100.0)(body)
    assert set(applied) == {"a", "s"}
    assert _state(body, "a") == dag.SUCCESS
    # a failed sensor poke reschedules (still running), not fails.
    assert _state(body, "s") == dag.RUNNING
    assert body["tasks"]["s"]["nextPokeAt"] == 110.0
    # fenced / duplicate entries are all left untouched.
    assert _state(body, "done") == dag.SUCCESS
    assert _state(body, "pf") == dag.RUNNING
    assert _state(body, "af") == dag.RUNNING
    assert _state(body, "pk") == dag.RUNNING


def test_mark_tasks_finished_none_body_and_all_dropped():
    spec = _spec(TaskSpec("a"))
    # None body -> keep, empty applied list.
    new, applied = dag.mark_tasks_finished(
        [{"taskkey": "a", "success": True, "task": spec.by_id["a"]}], 1.0
    )(None)
    assert dag.is_keep(new)
    assert applied == []
    # every mark drops (task already terminal) -> document kept untouched.
    body = _body(spec)
    body["tasks"]["a"]["state"] = dag.SUCCESS
    new, applied = dag.mark_tasks_finished(
        [{"taskkey": "a", "success": True, "task": spec.by_id["a"]}], 1.0
    )(body)
    assert dag.is_keep(new)
    assert applied == []


# apply_approval: no such run


def test_apply_approval_no_such_run():
    transform = dag.apply_approval(
        "gate", approved=True, by="alice", now=1.0, on_reject=dag.FAILED
    )
    new, result = transform(None)
    assert dag.is_keep(new)
    assert result["ok"] is False
    assert result["reason"] == "no such run"


# reconcile_crashed: no-op on None / terminal run


def test_reconcile_crashed_noop_on_none_and_terminal():
    spec = _spec(TaskSpec("a"))
    transform = dag.reconcile_crashed(spec, 1.0, "p", "h", lambda pid: False)
    new, n = transform(None)
    assert dag.is_keep(new)
    assert n == 0
    body = _body(spec)
    body["state"] = dag.SUCCESS
    new, n = transform(body)
    assert dag.is_keep(new)
    assert n == 0


# _has_live_process: proc-less entry, own token, and a live child on this host


def test_has_live_process_variants():
    # no proc recorded -> owner is gone (never treated as live).
    assert (
        dag._has_live_process({"proc": None}, "p", "h", lambda pid: True)
        is False
    )
    # our own proc token -> trusted alive without a pid probe.
    assert (
        dag._has_live_process({"proc": "p"}, "p", "h", lambda pid: False)
        is True
    )
    # a foreign token but a live child on this host -> alive.
    entry = {"proc": "other", "host": "h", "pid": 4321}
    assert dag._has_live_process(entry, "p", "h", lambda pid: True) is True
    # a foreign token whose pid is dead -> not alive.
    assert dag._has_live_process(entry, "p", "h", lambda pid: False) is False


# --------------------------------------------------------------------------
# fuzzing finding: a task parked up_for_retry that a reload retypes to
# mapped (gains expand:) must not wedge its run forever
# --------------------------------------------------------------------------


def test_reload_retyped_to_mapped_while_up_for_retry_terminalises():
    # spec A: plain task w (with retries) downstream of src.  w's first
    # attempt fails and parks up_for_retry.  The operator then adds an
    # `expand:` block to w and reloads.  The mapped dispatch used to route
    # the entry exclusively through _propagate_placeholder (which returned
    # unless PENDING), so nothing could ever re-claim, expand or
    # terminalise it: the run stayed non-terminal forever, holding its
    # dagadvance lease and defeating the pruner, while every advance paid
    # a full deep copy to do nothing.
    old = _spec(
        TaskSpec("src"),
        TaskSpec("w", depends_on=("src",), max_attempts=2, retry_delay=10.0),
    )
    body = _body(old)
    ex = _Executor(old, outcomes={"src": True, "w": False})
    body = ex.run(body)
    # first failure parks the retrying task, run still open
    assert _state(body, "w") == dag.UP_FOR_RETRY
    assert not dag.is_terminal_run(body)

    reloaded = _spec(
        TaskSpec("src"),
        TaskSpec(
            "w",
            depends_on=("src",),
            max_attempts=2,
            retry_delay=10.0,
            expand=ExpandSpec(from_task="src", key="items"),
        ),
    )
    # never offered for expansion (not a fresh placeholder) ...
    assert dag.tasks_awaiting_expansion(reloaded, body) == []
    # ... and the advance resolves it instead of spinning forever
    ex2 = _Executor(reloaded)
    body = ex2.run(body)
    assert ex2.launched == []  # nothing was launched under the stale shape
    assert _state(body, "w") == dag.FAILED
    assert "expand" in body["tasks"]["w"]["failReason"]
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.FAILED


def test_reload_retyped_to_mapped_while_pending_still_expands():
    # control: the same reload while w is still PENDING keeps the normal
    # expansion path -- the stale-shape resolution must only catch entries
    # that already left PENDING under the old shape.
    old = _spec(TaskSpec("src"), TaskSpec("w", depends_on=("src",)))
    body = _body(old)
    body["tasks"]["src"]["state"] = dag.SUCCESS
    body["tasks"]["src"]["finishedAt"] = 5.0
    reloaded = _spec(
        TaskSpec("src"),
        TaskSpec(
            "w",
            depends_on=("src",),
            expand=ExpandSpec(from_task="src", key="items"),
        ),
    )
    assert dag.tasks_awaiting_expansion(reloaded, body) == [
        ("w", "src", "items")
    ]
    ex = _Executor(reloaded, outcomes={"w": True}, xcom={"src": ["a", "b"]})
    body = ex.run(body)
    assert _state(body, "w") == dag.EXPANDED
    assert _state(body, "w#0") == dag.SUCCESS
    assert _state(body, "w#1") == dag.SUCCESS
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.SUCCESS


def test_renamed_expand_source_does_not_wedge_an_inflight_run():
    # A reload renames the task a mapped task fans out from (expand.fromTask).
    # validate_graph accepts it (the NEW spec is internally consistent), but
    # the in-flight run document has an entry for the OLD name and none for the
    # new one. effective_state defaults a missing entry to PENDING, so the
    # mapped placeholder waited on a task that would never appear: the run
    # never reached a terminal state, its dagadvance lease was renewed for the
    # life of the daemon, retention GC could never collect it, and every
    # advance pass paid a full document deepcopy to change nothing.
    #
    # _deps_verdict and _maybe_terminalise both already implement the rule
    # ("no entry in the run document -> added after the run was created, so it
    # cannot gate anything"); only the mapped-expansion path was missing it.
    old = _FANOUT_SPEC
    body = _body(old)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["gen"]["finishedAt"] = 5.0

    renamed = _spec(
        TaskSpec("generate"),
        TaskSpec(
            "work",
            depends_on=("generate",),
            expand=ExpandSpec(from_task="generate", key="items"),
        ),
    )
    dag.validate_graph(renamed)  # the new spec is internally fine
    # the source has no entry in THIS run, so there is nothing to expand from
    assert dag.tasks_awaiting_expansion(renamed, body) == []

    ex = _Executor(renamed)
    body = ex.run(body)
    assert ex.launched == []  # the fan-out can never be built
    assert _state(body, "work") == dag.FAILED
    assert "expand source 'generate'" in body["tasks"]["work"]["failReason"]
    # and the run finishes, so the lease is released and the doc is prunable
    assert dag.is_terminal_run(body)
    assert body["state"] == dag.FAILED


def test_unmaterialised_expand_source_leaves_downstreams_resolvable():
    # the resolution must unblock the placeholder and whatever waits on the
    # mapped task: a downstream all_success task sees the group as
    # upstream_failed and terminalises rather than pending forever.
    renamed = _spec(
        TaskSpec("generate"),
        TaskSpec(
            "work",
            depends_on=("generate",),
            expand=ExpandSpec(from_task="generate", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    old = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    body = _body(old)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["gen"]["finishedAt"] = 5.0
    ex = _Executor(renamed)
    body = ex.run(body)
    assert _state(body, "work") == dag.FAILED
    assert _state(body, "collect") == dag.UPSTREAM_FAILED
    assert dag.is_terminal_run(body)


def test_expand_removed_by_reload_still_dispatches_and_terminalises():
    # A run that fanned out under an older spec must keep folding its
    # recorded instances after a reload removes the task's `expand:`. The
    # placeholder is parked in the non-terminal EXPANDED state and no path
    # can advance it, so keying dispatch on the spec instead of the run's
    # recorded fan-out wedged the run (its lease, its downstream tasks and
    # its GC) until the daemon was restarted with the old config.
    spec_v1 = _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("collect", depends_on=("work",)),
    )
    ex1 = _Executor(
        spec_v1,
        outcomes={"gen": True, "work#0": True, "work#1": True},
        xcom={"gen": ["x", "y"]},
    )
    body = _body(spec_v1)
    for _ in range(6):
        body, _ = ex1.step(body)
        entry = body["tasks"].get("work#1")
        if entry is not None and entry.get("state") == dag.SUCCESS:
            break
    else:
        raise AssertionError("fan-out did not complete under the old spec")
    assert body["tasks"]["work"]["state"] == dag.EXPANDED
    assert _state(body, "collect") == dag.PENDING

    # the reload: work is now a plain task, but this run's fan-out already
    # happened; the recorded instances (all SUCCESS) carry the state.
    spec_v2 = _spec(
        TaskSpec("gen"),
        TaskSpec("work"),
        TaskSpec("collect", depends_on=("work",)),
    )
    assert dag.effective_state(spec_v2, body, "work") == dag.SUCCESS
    # the stale placeholder must read INERT to the quiescence pre-scan (the
    # full pass never touches it; ACT would defeat quiescence for the rest
    # of the run's retention).
    assert (
        dag._entry_quiescence(
            spec_v2, body, "work", body["tasks"]["work"], ex1.now, "proc-A"
        )
        == dag._Q_INERT
    )
    ex2 = _Executor(spec_v2, outcomes={"collect": True})
    body = ex2.run(body)
    assert body["state"] == dag.SUCCESS
    assert _state(body, "collect") == dag.SUCCESS
    # the placeholder itself is left as the fan-out marked it
    assert body["tasks"]["work"]["state"] == dag.EXPANDED


# --------------------------------------------------------------------------
# Branching: trigger rules, skip exit codes, and skip reasons
# --------------------------------------------------------------------------

_S, _F, _K, _U = dag.SUCCESS, dag.FAILED, dag.SKIPPED, dag.UPSTREAM_FAILED

# rule -> the verdict for upstreams that (all succeeded, hold a skip and a
# success, are all skipped, hold a failure)
_RULE_VERDICTS = {
    dag.ALL_SUCCESS: ("ready", "skip", "skip", "fail"),
    dag.ALL_DONE: ("ready", "ready", "ready", "ready"),
    dag.NONE_FAILED: ("ready", "ready", "ready", "fail"),
    dag.NONE_FAILED_MIN_ONE_SUCCESS: ("ready", "ready", "skip", "fail"),
    dag.ALL_DONE_MIN_ONE_FAILED: ("skip", "skip", "skip", "ready"),
    # a rule string this build does not know reads as all_success
    "a_rule_from_a_newer_build": ("ready", "skip", "skip", "fail"),
}
# upstream states for (u1, u2); None is an upstream with no entry in the run
_UPSTREAM_COLUMNS = {
    0: [(_S, _S), (_S, None), (None, None)],
    1: [(_K, _S), (_S, _K)],
    2: [(_K, _K), (_K, None)],
    3: [(_F, _S), (_U, _S), (_F, _K), (_K, _U), (_F, None), (_F, _F)],
}


def _join_spec(rule, **kw):
    return _spec(
        TaskSpec("u1"),
        TaskSpec("u2"),
        TaskSpec("j", depends_on=("u1", "u2"), trigger_rule=rule, **kw),
    )


def _join_body(spec, states):
    body = _body(spec)
    for key, state in zip(("u1", "u2"), states):
        if state is None:
            del body["tasks"][key]
        else:
            body["tasks"][key]["state"] = state
    return body


@pytest.mark.parametrize("rule", list(_RULE_VERDICTS))
@pytest.mark.parametrize("column", list(_UPSTREAM_COLUMNS))
def test_trigger_rule_verdicts_over_plain_upstreams(rule, column):
    spec = _join_spec(rule)
    for states in _UPSTREAM_COLUMNS[column]:
        body = _join_body(spec, states)
        got = dag._deps_verdict(spec, body, spec.by_id["j"])
        assert got == _RULE_VERDICTS[rule][column], states


@pytest.mark.parametrize("rule", list(_RULE_VERDICTS))
@pytest.mark.parametrize("live", [dag.PENDING, dag.RUNNING, dag.UP_FOR_RETRY])
def test_every_trigger_rule_waits_for_a_live_upstream(rule, live):
    spec = _join_spec(rule)
    for other in (_S, _F, _K, _U):
        body = _join_body(spec, (other, live))
        assert dag._deps_verdict(spec, body, spec.by_id["j"]) == "wait"


def test_trigger_rule_constants_cover_the_config_values():
    # config.py spells the values out (it imports dag lazily), so the two
    # lists are checked against each other here.
    for rule in dag.TRIGGER_RULES:
        cfg = _dagcfg(
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: up\n        command: 'e'\n"
            "      - id: a\n        command: 'e'\n"
            "        dependsOn:\n          - up\n"
            "        triggerRule: {}\n".format(rule)
        )
        assert cfg.dags[0].spec.by_id["a"].trigger_rule == rule
        # the rules that only read upstream states also load on a root
        root = (
            "dags:\n  - name: d\n    tasks:\n"
            "      - id: a\n        command: 'e'\n"
            "        triggerRule: {}\n".format(rule)
        )
        if rule in dag._COUNTING_TRIGGER_RULES:
            with pytest.raises(ConfigError, match="needs a dependsOn entry"):
                _dagcfg(root)
        else:
            assert _dagcfg(root).dags[0].spec.by_id["a"].trigger_rule == rule
    assert set(dag.TRIGGER_RULES) == set(_RULE_VERDICTS) - {
        "a_rule_from_a_newer_build"
    }


def _fan_join(rule, instance_states, *, extra=None):
    """gen -> work (mapped) -> j(rule); returns the verdict for ``j``.

    ``instance_states`` is one state per ``work#i`` instance, or a single
    state string for an un-expanded placeholder.  ``extra`` adds a plain
    second upstream in that state.
    """
    deps = ("work",) if extra is None else ("work", "other")
    tasks = [
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
        TaskSpec("other"),
        TaskSpec("j", depends_on=deps, trigger_rule=rule),
    ]
    spec = _spec(*tasks)
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["other"]["state"] = extra or dag.SUCCESS
    if isinstance(instance_states, str):
        body["tasks"]["work"]["state"] = instance_states
    else:
        body["mapped"]["work"] = {
            "items": list(range(len(instance_states))),
            "expandedAt": 1.0,
        }
        body["tasks"]["work"]["state"] = dag.EXPANDED
        for i, state in enumerate(instance_states):
            entry = dag._new_task_entry(spec.by_id["work"], 1.0)
            entry.pop("mapped")
            entry.update(mapIndex=i, mapItem=i, state=state)
            body["tasks"]["work#{}".format(i)] = entry
    return dag._deps_verdict(spec, body, spec.by_id["j"])


@pytest.mark.parametrize(
    "instances, column",
    [
        pytest.param([_S, _S, _S], 0, id="all-instances-succeeded"),
        pytest.param([], 0, id="empty-expansion-reads-success"),
        pytest.param([_K, _K], 2, id="every-instance-skipped"),
        pytest.param(_K, 2, id="placeholder-skipped-before-expanding"),
        pytest.param([_S, _F, _K], 3, id="an-instance-failed"),
        pytest.param(_U, 3, id="placeholder-upstream-failed"),
    ],
)
@pytest.mark.parametrize("rule", list(_RULE_VERDICTS))
def test_trigger_rule_verdicts_over_a_mapped_upstream(rule, instances, column):
    assert _fan_join(rule, instances) == _RULE_VERDICTS[rule][column]


def test_mapped_group_with_skipped_and_successful_instances():
    # the group reads `skipped`, the "a skip and a success" column for every
    # rule: one instance that succeeded is the success
    # none_failed_min_one_success needs.
    for rule, verdicts in _RULE_VERDICTS.items():
        assert _fan_join(rule, [_S, _K, _K]) == verdicts[1], rule
    # ...and while an instance is still going, every rule waits
    for rule in _RULE_VERDICTS:
        assert _fan_join(rule, [_S, _K, dag.RUNNING]) == "wait"
    # a fully skipped group next to a plain upstream that succeeded
    assert (
        _fan_join(dag.NONE_FAILED_MIN_ONE_SUCCESS, [_K, _K], extra=_S)
        == "ready"
    )
    # ...or next to a skipped one: nothing succeeded
    assert (
        _fan_join(dag.NONE_FAILED_MIN_ONE_SUCCESS, [_K, _K], extra=_K)
        == "skip"
    )
    # a group that did not expand has no instance to count
    assert _fan_join(dag.NONE_FAILED_MIN_ONE_SUCCESS, _K, extra=_K) == "skip"
    assert _fan_join(dag.NONE_FAILED_MIN_ONE_SUCCESS, _K, extra=_S) == "ready"


def _diamond(join_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS):
    return _spec(
        TaskSpec("extract"),
        TaskSpec("full-load", depends_on=("extract",), skip_exit_codes=(99,)),
        TaskSpec(
            "incremental-load", depends_on=("extract",), skip_exit_codes=(99,)
        ),
        TaskSpec(
            "publish",
            depends_on=("full-load", "incremental-load"),
            trigger_rule=join_rule,
        ),
        TaskSpec(
            "alert",
            depends_on=("full-load", "incremental-load", "publish"),
            trigger_rule=dag.ALL_DONE_MIN_ONE_FAILED,
        ),
    )


def _run_diamond(spec, **outcomes):
    scripted = {"extract": True, "publish": True, "alert": True}
    scripted.update({k.replace("_", "-"): v for k, v in outcomes.items()})
    ex = _Executor(spec, outcomes=scripted)
    body = ex.run(_body(spec))
    states = {k: v["state"] for k, v in body["tasks"].items()}
    return body, states, ex.launched


@pytest.mark.parametrize(
    "outcomes, publish, all_success_publish, alert, run",
    [
        pytest.param(
            {"full_load": True, "incremental_load": "skip"},
            _S, _K, _K, _S,
            id="sunday",
        ),
        pytest.param(
            {"full_load": "skip", "incremental_load": True},
            _S, _K, _K, _S,
            id="weekday",
        ),
        pytest.param(
            {"full_load": False, "incremental_load": "skip"},
            _U, _U, _S, _F,
            id="the-full-load-fails",
        ),
        pytest.param(
            {"full_load": "skip", "incremental_load": "skip"},
            _K, _K, _K, _S,
            id="both-guards-skip",
        ),
    ],
)
def test_branching_diamond(outcomes, publish, all_success_publish, alert, run):
    body, states, launched = _run_diamond(_diamond(), **outcomes)
    assert states["publish"] == publish
    assert states["alert"] == alert
    assert body["state"] == run
    assert ("publish" in launched) == (publish == _S)
    assert ("alert" in launched) == (alert == _S)
    # the same graph with the default rule on the join: it skips whenever
    # either branch does
    body, states, launched = _run_diamond(_diamond(dag.ALL_SUCCESS), **outcomes)
    assert states["publish"] == all_success_publish
    assert "publish" not in launched
    assert body["state"] == run


def test_branching_diamond_publish_fails():
    body, states, launched = _run_diamond(
        _diamond(), full_load=True, incremental_load="skip", publish=False
    )
    assert states == {
        "extract": _S,
        "full-load": _S,
        "incremental-load": _K,
        "publish": _F,
        "alert": _S,
    }
    # the handler ran and succeeded; the run is failed because a task failed
    assert launched[-1] == "alert"
    assert body["state"] == dag.FAILED


def test_branching_diamond_records_why_each_task_skipped():
    body, _, _ = _run_diamond(
        _diamond(), full_load=True, incremental_load="skip"
    )
    tasks = body["tasks"]
    assert tasks["incremental-load"]["skipReason"] == {
        "kind": "exit_code",
        "detail": "exit code 99",
    }
    assert tasks["incremental-load"]["exitCode"] == 99
    assert tasks["alert"]["skipReason"] == {
        "kind": "trigger_rule",
        "detail": "all_done_min_one_failed: no upstream failed",
    }
    assert "skipReason" not in tasks["publish"]
    assert "skipReason" not in tasks["full-load"]
    body, _, _ = _run_diamond(
        _diamond(), full_load="skip", incremental_load="skip"
    )
    assert body["tasks"]["publish"]["skipReason"] == {
        "kind": "trigger_rule",
        "detail": "none_failed_min_one_success: no upstream succeeded",
    }
    body, _, _ = _run_diamond(
        _diamond(dag.ALL_SUCCESS), full_load="skip", incremental_load="skip"
    )
    assert body["tasks"]["publish"]["skipReason"] == {
        "kind": "upstream",
        "detail": "upstream skipped: full-load, incremental-load",
    }


def test_a_skipped_branch_head_cascades_down_to_the_join():
    # a branch longer than one task needs no extra keys: the head's skip
    # cascades through its all_success successors, and the join's rule ends
    # the cascade.
    spec = _spec(
        TaskSpec("guard-a", skip_exit_codes=(99,)),
        TaskSpec("a1", depends_on=("guard-a",)),
        TaskSpec("a2", depends_on=("a1",)),
        TaskSpec("guard-b", skip_exit_codes=(99,)),
        TaskSpec("b1", depends_on=("guard-b",)),
        TaskSpec(
            "join",
            depends_on=("a2", "b1"),
            trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS,
        ),
    )
    ex = _Executor(
        spec,
        outcomes={"guard-a": "skip", "guard-b": True, "b1": True, "join": True},
    )
    body = ex.run(_body(spec))
    assert body["state"] == dag.SUCCESS
    assert _state(body, "a1") == _state(body, "a2") == dag.SKIPPED
    assert body["tasks"]["a2"]["skipReason"] == {
        "kind": "upstream",
        "detail": "upstream skipped: a1",
    }
    assert _state(body, "join") == dag.SUCCESS
    assert "a1" not in ex.launched and "a2" not in ex.launched


def test_skip_exit_code_completion_is_terminal_and_uses_no_attempt():
    spec = _spec(TaskSpec("a", max_attempts=3, skip_exit_codes=(99,)))
    task = spec.by_id["a"]
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    # attempt 0 fails: parked for a retry
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=1, fail_reason="x", now=2.0,
            task=task,
        ),
        body,
    )
    assert _state(body, "a") == dag.UP_FOR_RETRY
    assert body["tasks"]["a"]["attempt"] == 1
    body, res = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    assert [i.attempt for i in res.launches] == [1]
    # attempt 1 exits with the skip code: terminal, with attempts to spare
    body, applied = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=99, fail_reason="ignored", now=4.0,
            task=task, skipped=True, resources={"cpu": 1},
        ),
        body,
    )
    assert applied is True
    entry = body["tasks"]["a"]
    assert entry["state"] == dag.SKIPPED
    assert entry["attempt"] == 1
    assert entry["exitCode"] == 99
    assert entry["failReason"] is None
    assert entry["finishedAt"] == 4.0
    assert entry["proc"] is None and entry["pid"] is None
    assert entry["resources"] == {"cpu": 1}
    assert entry["skipReason"] == {"kind": "exit_code", "detail": "exit code 99"}
    # nothing re-claims it, and the run ends successful
    body, res = _apply(dag.plan_and_claim(spec, 5.0, "p", "h", {}), body)
    assert res.launches == [] and res.run_terminal
    assert body["state"] == dag.SUCCESS


def test_mark_tasks_finished_applies_a_skip_beside_other_marks():
    spec = _spec(
        TaskSpec("a", skip_exit_codes=(99,)),
        TaskSpec("b"),
        TaskSpec("c", skip_exit_codes=(3,)),
    )
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)

    def mark(key, **kw):
        base = {
            "taskkey": key,
            "success": True,
            "exit_code": 0,
            "fail_reason": None,
            "task": spec.by_id[key],
            "expected_proc": "p",
            "expected_attempt": 0,
            "expected_poke": None,
        }
        base.update(kw)
        return base

    marks = [
        mark("a", success=False, exit_code=99, skipped=True),
        mark("b"),
        # a superseded claim's skip is fenced out like any stale completion
        mark("c", success=False, exit_code=3, skipped=True, expected_proc="x"),
    ]
    body, applied = _apply(dag.mark_tasks_finished(marks, 2.0), body)
    assert applied == ["a", "b"]
    assert _state(body, "a") == dag.SKIPPED
    assert body["tasks"]["a"]["skipReason"]["detail"] == "exit code 99"
    assert _state(body, "b") == dag.SUCCESS
    assert _state(body, "c") == dag.RUNNING
    assert "skipReason" not in body["tasks"]["c"]


def test_sensor_skips_on_a_skip_code_and_repokes_on_any_other():
    spec = _spec(
        TaskSpec("s", type=dag.SENSOR, poke_interval=5.0, skip_exit_codes=(99,)),
        TaskSpec("after", depends_on=("s",)),
    )
    task = spec.by_id["s"]
    body = _body(spec)
    body, res = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    assert res.launches[0].is_sensor
    # an unlisted nonzero exit: the condition is not met yet, poke again
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=1, fail_reason="x", now=2.0,
            task=task, expected_poke=0,
        ),
        body,
    )
    assert _state(body, "s") == dag.RUNNING
    assert body["tasks"]["s"]["nextPokeAt"] == 7.0
    body, res = _apply(dag.plan_and_claim(spec, 8.0, "p", "h", {}), body)
    assert res.launches[0].poke_number == 1
    # the next poke exits with the skip code: the sensor ends skipped
    body, _ = _apply(
        dag.mark_task_finished(
            "s", success=False, exit_code=99, fail_reason="x", now=9.0,
            task=task, expected_poke=1, skipped=True,
        ),
        body,
    )
    entry = body["tasks"]["s"]
    assert entry["state"] == dag.SKIPPED
    assert entry["pokeCount"] == 2
    assert entry["exitCode"] == 99
    assert entry["skipReason"] == {"kind": "exit_code", "detail": "exit code 99"}
    # ...and its all_success downstream is skipped with it
    body, res = _apply(dag.plan_and_claim(spec, 10.0, "p", "h", {}), body)
    assert _state(body, "after") == dag.SKIPPED
    assert body["state"] == dag.SUCCESS


def test_approval_rejection_records_a_skip_reason_only_when_it_skips():
    for on_reject, state in ((dag.SKIPPED, dag.SKIPPED), (dag.FAILED, dag.FAILED)):
        spec = _spec(TaskSpec("gate", type=dag.APPROVAL, on_reject=on_reject))
        body = _body(spec)
        body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
        body, res = _apply(
            dag.apply_approval(
                "gate", approved=False, by="carol", now=2.0,
                on_reject=on_reject,
            ),
            body,
        )
        assert res == {"ok": True, "state": state}
        if state == dag.SKIPPED:
            assert body["tasks"]["gate"]["skipReason"] == {
                "kind": "approval",
                "detail": "rejected by carol",
            }
        else:
            assert "skipReason" not in body["tasks"]["gate"]


def test_mapped_placeholder_and_instances_record_skip_reasons():
    # 1. the expand source is skipped: the placeholder can never fan out
    spec = _spec(
        TaskSpec("gen", skip_exit_codes=(99,)),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
            trigger_rule=dag.ALL_DONE,
        ),
    )
    body = _Executor(spec, outcomes={"gen": "skip"}).run(_body(spec))
    assert body["tasks"]["work"]["skipReason"] == {
        "kind": "upstream",
        "detail": "upstream skipped: gen",
    }
    assert body["state"] == dag.SUCCESS

    # 2. the placeholder's own rule skips it: a mapped failure handler with
    # nothing to handle
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec("side"),
        TaskSpec(
            "work",
            depends_on=("gen", "side"),
            expand=ExpandSpec(from_task="gen", key="items"),
            trigger_rule=dag.ALL_DONE_MIN_ONE_FAILED,
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["side"]["state"] = dag.SUCCESS
    # no expansion list is offered (the driver's read failed), so the
    # placeholder is resolved by its verdict alone
    body, _ = _apply(
        dag.plan_and_claim(spec, 1.0, "p", "h", {"work": None}), body
    )
    assert body["tasks"]["work"]["state"] == dag.SKIPPED
    assert body["tasks"]["work"]["skipReason"]["kind"] == "trigger_rule"

    # 3. instances that expanded before a second upstream skipped: each one
    # records its own copy of the reason
    spec = _spec(
        TaskSpec("gen"),
        TaskSpec("side", skip_exit_codes=(99,)),
        TaskSpec(
            "work",
            depends_on=("gen", "side"),
            expand=ExpandSpec(from_task="gen", key="items"),
        ),
    )
    body = _body(spec)
    body["tasks"]["gen"]["state"] = dag.SUCCESS
    body["tasks"]["side"]["state"] = dag.RUNNING
    body["tasks"]["side"]["proc"] = "p"
    body, _ = _apply(
        dag.plan_and_claim(spec, 1.0, "p", "h", {"work": ["x", "y"]}), body
    )
    assert _state(body, "work#0") == _state(body, "work#1") == dag.PENDING
    body, _ = _apply(
        dag.mark_task_finished(
            "side", success=False, exit_code=99, fail_reason=None, now=2.0,
            task=spec.by_id["side"], skipped=True,
        ),
        body,
    )
    body, _ = _apply(dag.plan_and_claim(spec, 3.0, "p", "h", {}), body)
    first, second = body["tasks"]["work#0"], body["tasks"]["work#1"]
    assert first["state"] == second["state"] == dag.SKIPPED
    assert first["skipReason"] == second["skipReason"] == {
        "kind": "upstream",
        "detail": "upstream skipped: side",
    }
    assert first["skipReason"] is not second["skipReason"]
    assert body["state"] == dag.SUCCESS


def test_reload_changes_the_rule_of_a_pending_join_only():
    # the live configuration decides a pending task, and a recorded outcome
    # stands: the mid-run reload twin of
    # test_reload_added_dependency_does_not_wedge_run.
    def spec_with(rule):
        return _spec(
            TaskSpec("a", skip_exit_codes=(99,)),
            TaskSpec("b"),
            TaskSpec("j", depends_on=("a", "b"), trigger_rule=rule),
            TaskSpec("k", depends_on=("a",), trigger_rule=rule),
        )

    old = spec_with(dag.ALL_SUCCESS)
    body = _body(old)
    body, _ = _apply(dag.plan_and_claim(old, 1.0, "p", "h", {}), body)
    body, _ = _apply(
        dag.mark_task_finished(
            "a", success=False, exit_code=99, fail_reason=None, now=2.0,
            task=old.by_id["a"], skipped=True,
        ),
        body,
    )
    # under the old rule `k` (below the skipped task only) is skipped now;
    # `j` still waits on `b`
    body, _ = _apply(dag.plan_and_claim(old, 3.0, "p", "h", {}), body)
    assert _state(body, "k") == dag.SKIPPED
    assert _state(body, "j") == dag.PENDING
    # the reload lands, then `b` finishes
    new = spec_with(dag.NONE_FAILED)
    body, _ = _apply(
        dag.mark_task_finished(
            "b", success=True, exit_code=0, fail_reason=None, now=4.0,
            task=new.by_id["b"],
        ),
        body,
    )
    body, res = _apply(dag.plan_and_claim(new, 5.0, "p", "h", {}), body)
    assert [i.taskkey for i in res.launches] == ["j"]  # the new rule decides
    assert _state(body, "k") == dag.SKIPPED  # the recorded outcome stands
    assert body["tasks"]["k"]["skipReason"]["kind"] == "upstream"


@pytest.mark.parametrize("rule", list(_RULE_VERDICTS))
def test_quiescence_is_the_same_under_every_trigger_rule(rule):
    spec = _join_spec(rule)
    body = _body(spec)
    body, _ = _apply(dag.plan_and_claim(spec, 1.0, "p", "h", {}), body)
    # both upstreams in flight under our token, the join waiting: nothing
    # a pass could change
    assert _state(body, "j") == dag.PENDING
    assert dag._is_quiescent(spec, body, 2.0, "p", None)
    new, res = dag.plan_and_claim(spec, 2.0, "p", "h", {})(body)
    assert dag.is_keep(new) and not res.changed
    # one upstream still in flight: still quiescent
    body["tasks"]["u1"].update(state=dag.SKIPPED, proc=None)
    assert dag._is_quiescent(spec, body, 2.0, "p", None)
    # every upstream terminal: the join resolves, so the pass must run
    body["tasks"]["u2"].update(state=dag.SUCCESS, proc=None)
    assert not dag._is_quiescent(spec, body, 2.0, "p", None)


@pytest.mark.parametrize(
    "task, level",
    [
        (TaskSpec("t"), dag.BASE_ENGINE_LEVEL),
        (TaskSpec("t", trigger_rule=dag.ALL_DONE), dag.BASE_ENGINE_LEVEL),
        (TaskSpec("t", trigger_rule=dag.NONE_FAILED), 2),
        (TaskSpec("t", trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS), 2),
        (TaskSpec("t", trigger_rule=dag.ALL_DONE_MIN_ONE_FAILED), 2),
        (TaskSpec("t", skip_exit_codes=(99,)), 2),
    ],
)
def test_a_branching_dag_needs_engine_level_two(task, level):
    spec = _spec(TaskSpec("root"), task)
    assert spec.engine == level
    body = _body(spec)
    assert body.get("engine", dag.BASE_ENGINE_LEVEL) == level
    assert ("engine" in body) == (level > dag.BASE_ENGINE_LEVEL)
    assert dag.supports_run(body)
    assert dag.BRANCHING_PARAMS_ENGINE_LEVEL == 2 <= dag.ENGINE_LEVEL


def test_skip_exit_codes_load_sorted_and_deduplicated():
    cfg = _dagcfg(
        "dags:\n  - name: d\n    tasks:\n"
        "      - id: a\n        command: 'e'\n"
        "        skipExitCodes:\n          - 99\n          - 3\n"
        "          - 99\n"
        "      - id: s\n        type: sensor\n        command: 'e'\n"
        "        skipExitCodes:\n          - 75\n"
        "      - id: b\n        command: 'e'\n"
    )
    spec = cfg.dags[0].spec
    assert spec.by_id["a"].skip_exit_codes == (3, 99)
    assert spec.by_id["s"].skip_exit_codes == (75,)
    assert spec.by_id["b"].skip_exit_codes == ()
    assert spec.engine == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    # a node key, so it never reaches the launch template
    assert not hasattr(cfg.dags[0].task_templates["a"], "skipExitCodes")


# --------------------------------------------------------------------------
# The join lint: an all_success task below two or more skippable upstreams
# --------------------------------------------------------------------------


def _gate(task_id, **kw):
    return TaskSpec(task_id, type=dag.APPROVAL, on_reject=dag.SKIPPED, **kw)


@pytest.mark.parametrize(
    "tasks, joins",
    [
        pytest.param(
            _diamond(dag.ALL_SUCCESS).tasks,
            [("publish", ["full-load", "incremental-load"])],
            id="the-diamond-with-the-default-rule",
        ),
        pytest.param(_diamond().tasks, [], id="the-diamond-with-a-join-rule"),
        pytest.param(
            (
                TaskSpec("a"),
                TaskSpec("b"),
                TaskSpec("j", depends_on=("a", "b")),
            ),
            [],
            id="nothing-can-skip",
        ),
        pytest.param(
            (
                TaskSpec("guard", skip_exit_codes=(99,)),
                TaskSpec("b"),
                TaskSpec("j", depends_on=("guard", "b")),
            ),
            [],
            id="one-guard-beside-a-plain-task",
        ),
        pytest.param(
            (
                TaskSpec("guard", skip_exit_codes=(99,)),
                TaskSpec("j", depends_on=("guard", "guard")),
            ),
            [],
            id="a-repeated-dependency-is-one-upstream",
        ),
        pytest.param(
            (
                # listed downstream-first: the result does not depend on
                # the order tasks appear in
                TaskSpec("j", depends_on=("a2", "b1")),
                TaskSpec("a2", depends_on=("a1",)),
                TaskSpec("a1", depends_on=("ga",)),
                TaskSpec("b1", depends_on=("gb",)),
                TaskSpec("ga", skip_exit_codes=(99,)),
                TaskSpec("gb", skip_exit_codes=(99,)),
            ),
            [("j", ["a2", "b1"])],
            id="a-skip-cascades-down-each-branch",
        ),
        pytest.param(
            (
                _gate("g1"),
                _gate("g2"),
                TaskSpec("j", depends_on=("g1", "g2")),
            ),
            [("j", ["g1", "g2"])],
            id="two-gates-that-skip-on-reject",
        ),
        pytest.param(
            (
                TaskSpec("work"),
                TaskSpec("guard", skip_exit_codes=(99,)),
                TaskSpec(
                    "handler",
                    depends_on=("work",),
                    trigger_rule=dag.ALL_DONE_MIN_ONE_FAILED,
                ),
                TaskSpec("j", depends_on=("guard", "handler")),
            ),
            [("j", ["guard", "handler"])],
            id="a-failure-handler-can-always-skip",
        ),
        pytest.param(
            (
                TaskSpec("ga", skip_exit_codes=(99,)),
                TaskSpec("gb", skip_exit_codes=(99,)),
                TaskSpec("a", depends_on=("ga",), trigger_rule=dag.NONE_FAILED),
                TaskSpec("b", depends_on=("gb",), trigger_rule=dag.ALL_DONE),
                TaskSpec("j", depends_on=("a", "b")),
            ),
            [],
            id="none-failed-and-all-done-stop-a-skip",
        ),
        pytest.param(
            (
                TaskSpec("ga", skip_exit_codes=(99,)),
                TaskSpec("gb", skip_exit_codes=(99,)),
                TaskSpec("plain"),
                TaskSpec(
                    "some",
                    depends_on=("ga", "plain"),
                    trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS,
                ),
                TaskSpec(
                    "every",
                    depends_on=("ga", "gb"),
                    trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS,
                ),
                TaskSpec("j1", depends_on=("some", "gb")),
                TaskSpec("j2", depends_on=("every", "gb")),
            ),
            [("j2", ["every", "gb"])],
            id="min-one-success-skips-once-every-upstream-can",
        ),
        pytest.param(
            (
                TaskSpec("gen", skip_exit_codes=(99,)),
                TaskSpec("guard", skip_exit_codes=(99,)),
                TaskSpec(
                    "work",
                    depends_on=("gen",),
                    expand=ExpandSpec(from_task="gen", key="items"),
                    trigger_rule=dag.ALL_DONE,
                ),
                TaskSpec("j", depends_on=("work", "guard")),
            ),
            [("j", ["work", "guard"])],
            id="a-mapped-task-skips-with-its-expand-source",
        ),
    ],
)
def test_skippable_joins(tasks, joins):
    assert dag.skippable_joins(_spec(*tasks)) == joins


def test_join_lint_warns_at_load_and_names_the_task(caplog):
    diamond = (
        "dags:\n  - name: nightly\n    tasks:\n"
        "      - id: full\n        command: 'e'\n"
        "        skipExitCodes:\n          - 99\n"
        "      - id: incremental\n        command: 'e'\n"
        "        skipExitCodes:\n          - 99\n"
        "      - id: publish\n        command: 'e'\n"
        "        dependsOn:\n          - full\n          - incremental\n"
    )
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        _xsect(diamond)
    (record,) = [r for r in caplog.records if "can end skipped" in r.message]
    text = record.getMessage()
    assert "dag 'nightly': task 'publish'" in text
    assert "(full, incremental)" in text
    assert "none_failed_min_one_success" in text
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        _xsect(
            diamond + "        triggerRule: none_failed_min_one_success\n"
        )
    assert not [r for r in caplog.records if "can end skipped" in r.message]


# --------------------------------------------------------------------------
# when: conditions over parameters and XCom
# --------------------------------------------------------------------------


def _p(name, op, *values):
    return dag.Condition(
        source=dag.WHEN_PARAM, name=name, op=op, values=values
    )


def _x(task, key, op, *values):
    return dag.Condition(
        source=dag.WHEN_XCOM, name=task, key=key, op=op, values=values
    )


def _when_body(spec, params=None):
    return dag.new_run_body(
        dag="d",
        run_key="rk",
        run_id="rid",
        logical_date=None,
        kind="manual",
        now=0.0,
        spec=spec,
        params=params,
    )


def _done(body, spec, key, success=True, now=5.0):
    body, _ = _apply(
        dag.mark_task_finished(
            key,
            success=success,
            exit_code=0 if success else 1,
            fail_reason=None if success else "boom",
            now=now,
            task=spec.by_id[key.partition("#")[0]],
        ),
        body,
    )
    return body


def _pass(spec, body, now, conditions=None, expansions=None):
    return _apply(
        dag.plan_and_claim(
            spec, now, "p", "h", expansions or {}, conditions
        ),
        body,
    )


def _combined(spec, body, now):
    return _apply(
        dag.reconcile_and_plan(spec, now, "p", "h", lambda pid: False), body
    )


@pytest.mark.parametrize(
    "stored, cond, holds",
    [
        ("full", _p("v", "equals", "full"), True),
        ("inc", _p("v", "equals", "full"), False),
        ("inc", _p("v", "notEquals", "full"), True),
        ("full", _p("v", "notEquals", "full"), False),
        ("full", _p("v", "in", "full", "backfill"), True),
        ("inc", _p("v", "in", "full", "backfill"), False),
        ("inc", _p("v", "notIn", "full", "backfill"), True),
        ("backfill", _p("v", "notIn", "full", "backfill"), False),
        (5, _p("v", "equals", 5), True),
        (5, _p("v", "notEquals", 5), False),
        (5, _p("v", "in", 1, 2), False),
        (2, _p("v", "notIn", 1, 2), False),
        # a number stored as a float equals the integer it is
        (5.0, _p("v", "equals", 5), True),
        (2.5, _p("v", "in", 2.5, 3), True),
        (True, _p("v", "equals", True), True),
        (False, _p("v", "notEquals", True), True),
        (False, _p("v", "in", True, False), True),
        (True, _p("v", "notIn", True), False),
        # a boolean equals only a boolean
        (1, _p("v", "equals", True), False),
        (True, _p("v", "equals", 1), False),
        (0, _p("v", "notEquals", False), True),
        (False, _p("v", "notIn", 0), True),
        # and text never equals a number
        ("5", _p("v", "equals", 5), False),
    ],
)
def test_when_operators_over_each_parameter_type(stored, cond, holds):
    spec = _spec(TaskSpec("t", when=(cond,)))
    body = _when_body(spec, {"v": stored})
    outcome = dag._when_outcome(spec, body, spec.by_id["t"], None)
    if holds:
        assert outcome is True
    else:
        assert isinstance(outcome, str)
        assert outcome.startswith("param v {} ".format(cond.op))
        assert outcome.endswith(
            ": the value is {}".format(dag.env_text(stored))
        )
    body, res = _pass(spec, body, 1.0)
    entry = body["tasks"]["t"]
    if holds:
        assert entry["state"] == dag.RUNNING and entry["whenMet"] is True
        assert [i.taskkey for i in res.launches] == ["t"]
    else:
        assert entry["state"] == dag.SKIPPED and "whenMet" not in entry
        assert entry["skipReason"] == {"kind": "condition", "detail": outcome}
        assert not res.launches
        assert entry["attempt"] == 0 and entry["startedAt"] is None


@pytest.mark.parametrize("params", [None, {}, {"other": "x"}])
def test_when_on_a_parameter_the_run_does_not_store(params):
    # a run created before the parameter was declared
    spec = _spec(
        TaskSpec("eq", when=(_p("mode", "equals", "full"),)),
        TaskSpec("isin", when=(_p("mode", "in", "full"),)),
        TaskSpec("ne", when=(_p("mode", "notEquals", "full"),)),
        TaskSpec("notin", when=(_p("mode", "notIn", "full"),)),
    )
    body, res = _pass(spec, _when_body(spec, params), 1.0)
    assert sorted(i.taskkey for i in res.launches) == ["ne", "notin"]
    for key in ("eq", "isin"):
        assert _state(body, key) == dag.SKIPPED
        assert body["tasks"][key]["skipReason"]["detail"].endswith(
            ": the run has no such parameter"
        )
    assert body["tasks"]["eq"]["skipReason"]["detail"] == (
        "param mode equals full: the run has no such parameter"
    )


def test_when_detail_names_the_first_failing_comparison_and_cuts_values():
    long = "x" * 200
    spec = _spec(
        TaskSpec(
            "t",
            when=(
                _p("a", "equals", "yes"),
                _p("b", "in", "one", "two"),
                _p("c", "equals", "never"),
            ),
        )
    )
    body, _ = _pass(spec, _when_body(spec, {"a": "yes", "b": long, "c": "z"}), 1.0)
    assert body["tasks"]["t"]["skipReason"]["detail"] == (
        "param b in one, two: the value is " + "x" * 80 + "..."
    )
    # every comparison has to hold
    body, res = _pass(
        spec, _when_body(spec, {"a": "yes", "b": "two", "c": "never"}), 1.0
    )
    assert [i.taskkey for i in res.launches] == ["t"]


def _xcom_spec(*conds, rule=dag.ALL_SUCCESS):
    return _spec(
        TaskSpec("pub"),
        TaskSpec("t", depends_on=("pub",), trigger_rule=rule, when=conds),
    )


def _published(spec, params=None):
    # pub claimed and finished, t ready with its comparison unread
    body, _ = _pass(spec, _when_body(spec, params), 1.0)
    return _done(body, spec, "pub")


@pytest.mark.parametrize(
    "read, cond, detail",
    [
        ("12", _x("pub", "rows", "notIn", "0"), None),
        ("0", _x("pub", "rows", "notIn", "0"), "the value is 0"),
        ("full", _x("pub", "rows", "equals", "full"), None),
        ("ful", _x("pub", "rows", "equals", "full"), "the value is ful"),
        ("", _x("pub", "rows", "equals", ""), None),
        (dag.XCOM_UNPUBLISHED, _x("pub", "rows", "equals", "x"),
         "the key is not published"),
        (dag.XCOM_UNPUBLISHED, _x("pub", "rows", "in", "x", "y"),
         "the key is not published"),
        (dag.XCOM_UNPUBLISHED, _x("pub", "rows", "notEquals", "x"), None),
        (dag.XCOM_UNPUBLISHED, _x("pub", "rows", "notIn", "x"), None),
        (dag.XCOM_TOO_LARGE, _x("pub", "rows", "equals", "x"),
         "the value is over 4096 bytes"),
        (dag.XCOM_TOO_LARGE, _x("pub", "rows", "notEquals", "x"), None),
        (dag.XCOM_NOT_TEXT, _x("pub", "rows", "in", "x"),
         "the value is not UTF-8 text"),
        (dag.XCOM_GONE, _x("pub", "rows", "equals", "x"),
         "the published value is no longer stored"),
    ],
)
def test_when_over_an_xcom_value(read, cond, detail):
    spec = _xcom_spec(cond)
    body = _published(spec)
    assert dag.tasks_awaiting_conditions(spec, body) == [("pub", "rows")]
    body, res = _pass(spec, body, 6.0, {("pub", "rows"): read})
    entry = body["tasks"]["t"]
    if detail is None:
        assert entry["state"] == dag.RUNNING and entry["whenMet"] is True
        assert [i.taskkey for i in res.launches] == ["t"]
    else:
        assert entry["state"] == dag.SKIPPED
        assert entry["skipReason"]["kind"] == dag.SKIP_CONDITION
        assert entry["skipReason"]["detail"] == "{}: {}".format(
            dag._condition_text(cond), detail
        )
        assert not res.launches
    assert not res.again
    assert dag.tasks_awaiting_conditions(spec, body) == []


def test_when_xcom_detail_text():
    cond = _x("extract", "row_count", "notIn", "0")
    assert dag._condition_text(cond) == "xcom extract/row_count notIn 0"
    assert dag._condition_text(_p("mode", "in", "a", True, 3)) == (
        "param mode in a, true, 3"
    )


def test_combined_transform_hands_an_xcom_comparison_to_the_driver():
    spec = _xcom_spec(_x("pub", "rows", "equals", "go"))
    body = _when_body(spec)
    body, res = _combined(spec, body, 1.0)
    # nothing to read while the publisher has not run: one RMW, as before
    assert not res.conditions_needed and res.advance is not None
    assert [i.taskkey for i in res.advance.launches] == ["pub"]
    body = _done(body, spec, "pub")
    before = copy.deepcopy(body)
    new, res = dag.reconcile_and_plan(
        spec, 6.0, "p", "h", lambda pid: False
    )(body)
    # the claim half did not run, and nothing was reconciled: the document
    # is kept and the driver reads the value
    assert dag.is_keep(new) and body == before
    assert res.conditions_needed and not res.expansions_needed
    assert res.advance is None
    body, res = _pass(spec, body, 7.0, {("pub", "rows"): "go"})
    assert [i.taskkey for i in res.launches] == ["t"]
    # decided: the next advance is one RMW again
    body, res = _combined(spec, body, 8.0)
    assert not res.conditions_needed


def test_an_unanswered_xcom_read_leaves_the_task_pending():
    spec = _xcom_spec(_x("pub", "rows", "equals", "go"))
    body = _published(spec)
    for conditions in (None, {}, {("pub", "rows"): None}):
        new, res = dag.plan_and_claim(
            spec, 6.0, "p", "h", {}, conditions
        )(body)
        # never guessed: nothing is written, and the pass asks for another
        assert dag.is_keep(new) and not res.changed and not res.launches
        assert res.again
        assert _state(body, "t") == dag.PENDING
        assert "whenMet" not in body["tasks"]["t"]
    # the next pass reads it
    body, res = _pass(spec, body, 7.0, {("pub", "rows"): "go"})
    assert [i.taskkey for i in res.launches] == ["t"] and not res.again


def test_a_parameter_comparison_that_fails_needs_no_xcom_read():
    spec = _xcom_spec(
        _x("pub", "rows", "equals", "go"), _p("mode", "equals", "full")
    )
    body = _published(spec, {"mode": "inc"})
    assert dag.tasks_awaiting_conditions(spec, body) == []
    body, res = _combined(spec, body, 6.0)
    assert not res.conditions_needed
    assert body["tasks"]["t"]["skipReason"]["detail"] == (
        "param mode equals full: the value is inc"
    )
    # with the parameter met, the XCom value is what is left to read
    body = _published(spec, {"mode": "full"})
    assert dag.tasks_awaiting_conditions(spec, body) == [("pub", "rows")]


def test_xcom_pairs_are_read_once_each():
    spec = _spec(
        TaskSpec("pub"),
        TaskSpec("other"),
        TaskSpec(
            "t1",
            depends_on=("pub", "other"),
            when=(
                _x("pub", "k", "equals", "a"),
                _x("other", "k", "notEquals", "b"),
                _x("pub", "k", "notEquals", "z"),
            ),
        ),
        TaskSpec("t2", depends_on=("pub",), when=(_x("pub", "k", "in", "a"),)),
    )
    body, _ = _pass(spec, _when_body(spec), 1.0)
    body = _done(body, spec, "pub")
    # t1 still waits for `other`, so only t2 contributes
    assert dag.tasks_awaiting_conditions(spec, body) == [("pub", "k")]
    body = _done(body, spec, "other")
    assert dag.tasks_awaiting_conditions(spec, body) == [
        ("pub", "k"),
        ("other", "k"),
    ]
    body, res = _pass(
        spec, body, 6.0, {("pub", "k"): "a", ("other", "k"): "c"}
    )
    assert sorted(i.taskkey for i in res.launches) == ["t1", "t2"]


def test_a_failed_upstream_wins_over_an_unmet_condition():
    spec = _spec(
        TaskSpec("a"),
        TaskSpec("b", depends_on=("a",), when=(_p("mode", "equals", "full"),)),
    )
    body, _ = _pass(spec, _when_body(spec, {"mode": "inc"}), 1.0)
    body = _done(body, spec, "a", success=False)
    body, _ = _pass(spec, body, 6.0)
    entry = body["tasks"]["b"]
    # the trigger rule is read first, so the condition is never consulted
    assert entry["state"] == dag.UPSTREAM_FAILED
    assert "skipReason" not in entry and "whenMet" not in entry
    assert body["state"] == dag.FAILED


def test_a_skipped_upstream_wins_over_a_met_condition():
    spec = _spec(
        TaskSpec("a", when=(_p("mode", "equals", "full"),)),
        TaskSpec("b", depends_on=("a",), when=(_p("mode", "equals", "inc"),)),
    )
    body, res = _pass(spec, _when_body(spec, {"mode": "inc"}), 1.0)
    assert not res.launches and body["state"] == dag.SUCCESS
    assert body["tasks"]["a"]["skipReason"]["kind"] == dag.SKIP_CONDITION
    assert body["tasks"]["b"]["skipReason"] == {
        "kind": "upstream",
        "detail": "upstream skipped: a",
    }


def test_when_met_is_recorded_once_and_survives_a_reload():
    old = _spec(
        TaskSpec(
            "a",
            max_attempts=2,
            when=(_p("mode", "equals", "full"),),
        ),
        TaskSpec("b", depends_on=("a",), when=(_p("mode", "equals", "full"),)),
    )
    body, res = _pass(old, _when_body(old, {"mode": "full"}), 1.0)
    assert [i.taskkey for i in res.launches] == ["a"]
    assert body["tasks"]["a"]["whenMet"] is True
    body = _done(body, old, "a", success=False)
    assert _state(body, "a") == dag.UP_FOR_RETRY
    # a reload changes both conditions to ones the run does not meet
    new = _spec(
        TaskSpec(
            "a",
            max_attempts=2,
            when=(_p("mode", "equals", "inc"),),
        ),
        TaskSpec("b", depends_on=("a",), when=(_p("mode", "equals", "inc"),)),
    )
    body, res = _pass(new, body, 10.0)
    # the retry keeps the recorded decision
    assert [i.taskkey for i in res.launches] == ["a"]
    assert _state(body, "a") == dag.RUNNING
    body = _done(body, new, "a", now=11.0)
    body, res = _pass(new, body, 12.0)
    # a task with no recorded result reads the condition as it is now
    assert not res.launches
    assert body["tasks"]["b"]["skipReason"] == {
        "kind": "condition",
        "detail": "param mode equals inc: the value is full",
    }
    assert body["state"] == dag.SUCCESS


def test_a_released_claim_keeps_when_met():
    spec = _spec(TaskSpec("a", when=(_p("mode", "equals", "full"),)))
    body, _ = _pass(spec, _when_body(spec, {"mode": "full"}), 1.0)
    body, released = _apply(
        dag.release_lost_claims(spec, [("a", "p", 0, 0)], 2.0), body
    )
    assert released == ["a"] and _state(body, "a") == dag.PENDING
    assert body["tasks"]["a"]["whenMet"] is True
    changed = _spec(TaskSpec("a", when=(_p("mode", "equals", "inc"),)))
    body, res = _pass(changed, body, 3.0)
    assert [i.taskkey for i in res.launches] == ["a"]


def test_a_deferred_claim_keeps_when_met_and_a_skip_never_waits(monkeypatch):
    monkeypatch.setattr(dag, "MAX_CLAIMS_PER_PASS", 1)
    met = (_p("mode", "equals", "full"),)
    unmet = (_p("mode", "equals", "inc"),)
    spec = _spec(
        TaskSpec("first", when=met),
        TaskSpec("second", when=met),
        TaskSpec("never", when=unmet),
        TaskSpec("gate", type=dag.APPROVAL, when=unmet),
    )
    body, res = _pass(spec, _when_body(spec, {"mode": "full"}), 1.0)
    assert [i.taskkey for i in res.launches] == ["first"] and res.deferred
    # the quota was spent before these three were reached: the skip and the
    # recorded result did not wait for it
    assert _state(body, "second") == dag.PENDING
    assert body["tasks"]["second"]["whenMet"] is True
    assert _state(body, "never") == dag.SKIPPED
    assert _state(body, "gate") == dag.SKIPPED
    # a reload now flips the condition: the deferred task still launches
    flipped = _spec(
        TaskSpec("first", when=met),
        TaskSpec("second", when=unmet),
        TaskSpec("never", when=unmet),
        TaskSpec("gate", type=dag.APPROVAL, when=unmet),
    )
    body, res = _pass(flipped, body, 2.0)
    assert [i.taskkey for i in res.launches] == ["second"]


def test_when_skips_a_gate_without_parking_it():
    spec = _spec(
        TaskSpec("gate", type=dag.APPROVAL, when=(_p("go", "equals", True),)),
        TaskSpec("after", depends_on=("gate",)),
    )
    body, res = _pass(spec, _when_body(spec, {"go": False}), 1.0)
    gate = body["tasks"]["gate"]
    assert gate["state"] == dag.SKIPPED and "awaitingApproval" not in gate
    assert gate["approval"] is None and gate["startedAt"] is None
    assert gate["skipReason"]["kind"] == dag.SKIP_CONDITION
    assert _state(body, "after") == dag.SKIPPED and body["state"] == dag.SUCCESS
    # a met condition parks the gate as usual
    body, res = _pass(spec, _when_body(spec, {"go": True}), 1.0)
    gate = body["tasks"]["gate"]
    assert gate["awaitingApproval"] is True and gate["whenMet"] is True
    assert not res.launches


def test_when_is_read_once_before_a_sensor_first_pokes():
    spec = _spec(
        TaskSpec(
            "wait",
            type=dag.SENSOR,
            poke_interval=10.0,
            when=(_p("go", "equals", True),),
        )
    )
    body, res = _pass(spec, _when_body(spec, {"go": False}), 1.0)
    entry = body["tasks"]["wait"]
    assert entry["state"] == dag.SKIPPED and not res.launches
    assert entry["pokeCount"] == 0 and "firstPokeAt" not in entry
    # met: the sensor pokes, and later pokes do not read the condition
    body, res = _pass(spec, _when_body(spec, {"go": True}), 1.0)
    assert [i.is_sensor for i in res.launches] == [True]
    body, _ = _apply(
        dag.mark_task_finished(
            "wait", success=False, exit_code=1, fail_reason=None, now=2.0,
            task=spec.by_id["wait"],
        ),
        body,
    )
    assert _state(body, "wait") == dag.RUNNING  # idle between pokes
    flipped = _spec(
        TaskSpec(
            "wait",
            type=dag.SENSOR,
            poke_interval=10.0,
            when=(_p("go", "equals", False),),
        )
    )
    body, res = _pass(flipped, body, 50.0)
    assert [i.poke_number for i in res.launches] == [1]


def _mapped_when(*conds):
    return _spec(
        TaskSpec("gen"),
        TaskSpec(
            "work",
            depends_on=("gen",),
            expand=ExpandSpec(from_task="gen", key="items"),
            when=conds,
        ),
        TaskSpec("collect", depends_on=("work",), trigger_rule=dag.NONE_FAILED),
    )


def test_when_skips_a_mapped_placeholder_before_it_expands():
    spec = _mapped_when(_p("mode", "equals", "full"))
    body, _ = _pass(spec, _when_body(spec, {"mode": "inc"}), 1.0)
    body = _done(body, spec, "gen")
    # no list is read for a fan-out a condition may still skip
    assert dag.tasks_awaiting_expansion(spec, body) == []
    # and a list read from a stale snapshot expands nothing
    body, res = _pass(spec, body, 6.0, expansions={"work": ["a", "b"]})
    work = body["tasks"]["work"]
    assert work["state"] == dag.SKIPPED
    assert work["skipReason"] == {
        "kind": "condition",
        "detail": "param mode equals full: the value is inc",
    }
    assert "work" not in body["mapped"]
    assert not [k for k in body["tasks"] if k.startswith("work#")]
    assert [i.taskkey for i in res.launches] == ["collect"]


def test_a_met_condition_lets_a_mapped_task_expand_on_the_next_pass():
    spec = _mapped_when(_p("mode", "equals", "full"))
    body, _ = _pass(spec, _when_body(spec, {"mode": "full"}), 1.0)
    body = _done(body, spec, "gen")
    body, res = _combined(spec, body, 6.0)
    # decided on the placeholder, in the one RMW; the driver advances again
    assert not res.expansions_needed and res.advance.again
    work = body["tasks"]["work"]
    assert work["state"] == dag.PENDING and work["whenMet"] is True
    assert dag.tasks_awaiting_expansion(spec, body) == [
        ("work", "gen", "items")
    ]
    new, res = dag.reconcile_and_plan(
        spec, 7.0, "p", "h", lambda pid: False
    )(body)
    assert res.expansions_needed and dag.is_keep(new)
    # a reload that flips the condition cannot undo the recorded result,
    # and an instance has no condition of its own to read
    flipped = _mapped_when(_p("mode", "equals", "inc"))
    body, res = _pass(flipped, body, 8.0, expansions={"work": ["a", "b"]})
    assert sorted(i.taskkey for i in res.launches) == ["work#0", "work#1"]
    assert _state(body, "work") == dag.EXPANDED
    assert "whenMet" not in body["tasks"]["work#0"]
    assert not res.again


def test_a_mapped_placeholder_reads_an_xcom_comparison():
    spec = _mapped_when(_x("gen", "go", "equals", "yes"))
    body, _ = _pass(spec, _when_body(spec), 1.0)
    body = _done(body, spec, "gen")
    assert dag.tasks_awaiting_conditions(spec, body) == [("gen", "go")]
    assert dag.tasks_awaiting_expansion(spec, body) == []
    met, res = _pass(spec, body, 6.0, {("gen", "go"): "yes"})
    assert met["tasks"]["work"]["whenMet"] is True and res.again
    unmet, res = _pass(spec, body, 6.0, {("gen", "go"): "no"})
    assert _state(unmet, "work") == dag.SKIPPED and not res.again
    assert unmet["tasks"]["work"]["skipReason"]["detail"] == (
        "xcom gen/go equals yes: the value is no"
    )


def _early_resolved():
    # `work` fans out over `src` and also depends on `pub`. When `src` fails,
    # the placeholder resolves at once, while `pub` still runs.
    return _spec(
        TaskSpec("src"),
        TaskSpec("pub"),
        TaskSpec(
            "work",
            depends_on=("src", "pub"),
            expand=ExpandSpec(from_task="src", key="items"),
        ),
        TaskSpec(
            "after",
            depends_on=("work",),
            trigger_rule=dag.ALL_DONE,
            when=(_x("pub", "go", "equals", "yes"),),
        ),
    )


def test_an_xcom_comparison_waits_for_its_publisher_to_finish():
    spec = _early_resolved()
    body, _ = _pass(spec, _when_body(spec), 1.0)
    body = _done(body, spec, "src", success=False)
    body, res = _pass(spec, body, 6.0)
    assert _state(body, "work") == dag.UPSTREAM_FAILED
    assert _state(body, "pub") == dag.RUNNING
    # `after` is ready by its rule, and its publisher is not final yet
    assert dag._deps_verdict(spec, body, spec.by_id["after"]) == "ready"
    assert _state(body, "after") == dag.PENDING
    assert dag.tasks_awaiting_conditions(spec, body) == []
    assert not res.again
    # even a value read too early is not used
    new, res = dag.plan_and_claim(
        spec, 7.0, "p", "h", {}, {("pub", "go"): "yes"}
    )(body)
    assert dag.is_keep(new) and not res.again
    body = _done(body, spec, "pub", now=8.0)
    assert dag.tasks_awaiting_conditions(spec, body) == [("pub", "go")]
    body, res = _pass(spec, body, 9.0, {("pub", "go"): "yes"})
    assert [i.taskkey for i in res.launches] == ["after"]


def test_quiescence_with_a_task_waiting_on_an_xcom_comparison():
    # a wrong "quiescent" answer would wedge the run, so each shape in which
    # a conditional task still has something to do reads as "act"
    spec = _xcom_spec(_x("pub", "rows", "equals", "go"))
    body, _ = _pass(spec, _when_body(spec), 1.0)
    # the publisher is in flight under our token and `t` waits on it
    assert dag._is_quiescent(spec, body, 2.0, "p", None)
    body = _done(body, spec, "pub")
    # ready with an unread comparison
    assert not dag._is_quiescent(spec, body, 6.0, "p", None)
    assert not dag._is_quiescent(spec, body, 6.0, "p", {})
    # ready by its rule while its publisher still runs
    spec = _early_resolved()
    body, _ = _pass(spec, _when_body(spec), 1.0)
    body = _done(body, spec, "src", success=False)
    body, _ = _pass(spec, body, 6.0)
    assert _state(body, "after") == dag.PENDING
    assert not dag._is_quiescent(spec, body, 7.0, "p", None)
    # a mapped placeholder whose condition is undecided
    spec = _mapped_when(_p("mode", "equals", "full"))
    body, _ = _pass(spec, _when_body(spec, {"mode": "full"}), 1.0)
    body = _done(body, spec, "gen")
    assert not dag._is_quiescent(spec, body, 6.0, "p", None)


def test_a_task_that_becomes_ready_inside_a_pass_asks_for_another():
    # `guard` is skipped by this pass, which is what makes `t` ready: the
    # driver read nothing for it, and nothing else would wake the run
    spec = _spec(
        TaskSpec("pub"),
        TaskSpec("guard", depends_on=("pub",), when=(_p("m", "equals", "x"),)),
        TaskSpec(
            "t",
            depends_on=("pub", "guard"),
            trigger_rule=dag.NONE_FAILED,
            when=(_x("pub", "k", "equals", "go"),),
        ),
    )
    body, _ = _pass(spec, _when_body(spec, {"m": "y"}), 1.0)
    body = _done(body, spec, "pub")
    assert dag.tasks_awaiting_conditions(spec, body) == []
    body, res = _combined(spec, body, 6.0)
    assert res.advance is not None and res.advance.again
    assert _state(body, "guard") == dag.SKIPPED
    assert _state(body, "t") == dag.PENDING
    body, res = _combined(spec, body, 7.0)
    assert res.conditions_needed
    body, res = _pass(spec, body, 8.0, {("pub", "k"): "go"})
    assert [i.taskkey for i in res.launches] == ["t"] and not res.again


def test_an_xcom_source_a_reload_added_reads_as_no_value():
    old = _spec(TaskSpec("a"), TaskSpec("t", depends_on=("a",)))
    body, _ = _pass(old, _when_body(old), 1.0)
    # the reload adds `late` and a comparison that reads it; this run has no
    # entry for `late`, so it never runs here and has published nothing
    def reloaded(op):
        return _spec(
            TaskSpec("a"),
            TaskSpec("late", depends_on=("a",)),
            TaskSpec(
                "t",
                depends_on=("a", "late"),
                when=(_x("late", "k", op, "go"),),
            ),
        )

    body = _done(body, old, "a")
    spec = reloaded("equals")
    assert dag.tasks_awaiting_conditions(spec, body) == []
    skipped, res = _combined(spec, body, 6.0)
    assert not res.conditions_needed
    assert skipped["tasks"]["t"]["skipReason"]["detail"] == (
        "xcom late/k equals go: the task is not in this run"
    )
    assert skipped["state"] == dag.SUCCESS
    ran, res = _combined(reloaded("notEquals"), body, 6.0)
    assert [i.taskkey for i in res.advance.launches] == ["t"]


@pytest.mark.parametrize(
    "tasks, level",
    [
        ((TaskSpec("t", when=(_p("m", "equals", "x"),)),), 2),
        ((TaskSpec("t"),), dag.BASE_ENGINE_LEVEL),
        (
            (
                TaskSpec("t", skip_exit_codes=(9,)),
                TaskSpec("u", when=(_p("m", "equals", "x"),)),
            ),
            2,
        ),
    ],
)
def test_a_dag_with_when_needs_engine_level_two(tasks, level):
    spec = _spec(*tasks)
    assert spec.engine == level
    body = _when_body(spec, {"m": "x"})
    assert body.get("engine", dag.BASE_ENGINE_LEVEL) == level
    assert dag.supports_run(body)
    # the level a skip code, a join rule, and a declaration need too
    assert dag.BRANCHING_PARAMS_ENGINE_LEVEL == 2 == dag.ENGINE_LEVEL
    # a build one level below leaves the run alone
    body["engine"] = dag.ENGINE_LEVEL + 1
    assert not dag.supports_run(body)


def test_conditional_tasks_are_the_ones_that_read_xcom():
    spec = _spec(
        TaskSpec("pub"),
        TaskSpec("p", when=(_p("m", "equals", "x"),)),
        TaskSpec("x", depends_on=("pub",), when=(_x("pub", "k", "in", "a"),)),
        TaskSpec("plain"),
    )
    assert [t.id for t in spec.conditional_tasks] == ["x"]
    assert _spec(TaskSpec("plain")).conditional_tasks == ()


@pytest.mark.parametrize(
    "tasks, joins",
    [
        pytest.param(
            (
                TaskSpec("full", when=(_p("m", "equals", "full"),)),
                TaskSpec("inc", when=(_p("m", "notEquals", "full"),)),
                TaskSpec("publish", depends_on=("full", "inc")),
            ),
            [("publish", ["full", "inc"])],
            id="two-conditional-branches",
        ),
        pytest.param(
            (
                TaskSpec("full", when=(_p("m", "equals", "full"),)),
                TaskSpec("inc"),
                TaskSpec("publish", depends_on=("full", "inc")),
            ),
            [],
            id="one-conditional-branch",
        ),
        pytest.param(
            (
                TaskSpec("full", when=(_p("m", "equals", "full"),)),
                TaskSpec("guard", skip_exit_codes=(99,)),
                TaskSpec(
                    "publish",
                    depends_on=("full", "guard"),
                    trigger_rule=dag.NONE_FAILED_MIN_ONE_SUCCESS,
                ),
            ),
            [],
            id="a-join-rule-keeps-the-lint-quiet",
        ),
    ],
)
def test_skippable_joins_count_a_conditional_upstream(tasks, joins):
    assert dag.skippable_joins(_spec(*tasks)) == joins


def test_validate_graph_checks_an_xcom_source():
    def graph(source, **kw):
        return _spec(
            TaskSpec("gen"),
            TaskSpec("mid", depends_on=("gen",)),
            TaskSpec(
                "fan",
                depends_on=("gen",),
                expand=ExpandSpec(from_task="gen", key="items"),
            ),
            TaskSpec("side"),
            TaskSpec(
                "t",
                depends_on=("mid", "fan"),
                when=(_x(source, "k", "equals", "v"),),
                **kw,
            ),
        )

    # a direct upstream and one reached through another task are both fine
    dag.validate_graph(graph("mid"))
    dag.validate_graph(graph("gen"))
    for source, text in (
        ("side", "is not upstream of this task"),
        ("t", "is not upstream of this task"),
        ("ghost", "is not a task"),
        ("fan", "is mapped"),
    ):
        with pytest.raises(dag.DagValidationError) as err:
            dag.validate_graph(graph(source))
        assert "task 't': when: xcom task {!r}".format(source) in str(err.value)
        assert text in str(err.value)


_WHEN_YAML = (
    "dags:\n  - name: d\n    params:\n"
    "      - name: mode\n        default: inc\n"
    "        allowed:\n          - inc\n          - full\n"
    "      - name: count\n        type: integer\n        default: 1\n"
    "        minimum: 0\n"
    "      - name: ratio\n        type: number\n        default: 0.5\n"
    "      - name: dry\n        type: boolean\n        default: false\n"
    "    tasks:\n"
    "      - id: a\n        command: 'e'\n"
    "      - id: fan\n        command: 'e'\n"
    "        dependsOn:\n          - a\n"
    "        expand:\n          fromTask: a\n          key: items\n"
    "      - id: side\n        command: 'e'\n"
    "      - id: b\n        command: 'e'\n"
    "        dependsOn:\n          - fan\n"
    "        when:\n"
)


def _when_entry(*lines):
    # one `when:` entry: the first line carries the dash
    out = "          - {}\n".format(lines[0])
    for line in lines[1:]:
        out += "            {}\n".format(line)
    return out


@pytest.mark.parametrize(
    "entries, message",
    [
        pytest.param(
            _when_entry("equals: full"),
            "when entry 1: names exactly one source",
            id="no-source",
        ),
        pytest.param(
            _when_entry(
                "param: mode", "xcom:", "  task: a", "  key: k", "equals: full"
            ),
            "when entry 1: names exactly one source",
            id="two-sources",
        ),
        pytest.param(
            _when_entry("param: mode"),
            "when entry 1: names exactly one operator",
            id="no-operator",
        ),
        pytest.param(
            _when_entry("param: mode", "equals: full", "notEquals: inc"),
            "when entry 1: names exactly one operator",
            id="two-operators",
        ),
        pytest.param(
            _when_entry("param: mode", "equals: full")
            + _when_entry("param: nope", "equals: x"),
            "when entry 2: param 'nope' is not a parameter this workflow "
            "declares",
            id="undeclared-param",
        ),
        pytest.param(
            _when_entry("param: mode", "equals: ful"),
            "param 'mode': value 'ful' must be one of: inc, full, so the "
            "parameter never holds it",
            id="a-value-outside-allowed",
        ),
        pytest.param(
            _when_entry("param: count", "in:", "  - '1'", "  - many"),
            "param 'count': value 'many' must be an integer",
            id="not-the-declared-type",
        ),
        pytest.param(
            _when_entry("param: count", "notEquals: '-1'"),
            "param 'count': value '-1' must be at least 0",
            id="a-value-below-the-minimum",
        ),
        pytest.param(
            _when_entry("param: dry", "equals: maybe"),
            "param 'dry': value 'maybe' must be true or false",
            id="not-a-boolean",
        ),
        pytest.param(
            _when_entry("xcom:", "  task: side", "  key: k", "equals: v"),
            "task 'b': when: xcom task 'side' is not upstream of this task",
            id="xcom-task-not-upstream",
        ),
        pytest.param(
            _when_entry("xcom:", "  task: ghost", "  key: k", "equals: v"),
            "task 'b': when: xcom task 'ghost' is not a task",
            id="xcom-task-unknown",
        ),
        pytest.param(
            _when_entry("xcom:", "  task: fan", "  key: k", "equals: v"),
            "task 'b': when: xcom task 'fan' is mapped",
            id="xcom-task-mapped",
        ),
        pytest.param(
            _when_entry(
                "xcom:", "  task: a", "  key: k", "equals: " + "v" * 4097
            ),
            "an XCom comparison value is at most 4096 bytes",
            id="xcom-value-too-large",
        ),
        pytest.param(
            _when_entry("xcom:", "  task: a", "  key: ''", "equals: v"),
            "`xcom` needs a `task` and a `key`",
            id="xcom-empty-key",
        ),
    ],
)
def test_when_load_rejections(entries, message):
    with pytest.raises(ConfigError) as err:
        _dagcfg(_WHEN_YAML + entries)
    text = str(err.value)
    assert text.startswith("dag 'd': ") and message in text


def test_when_refuses_an_empty_list_and_an_unknown_key():
    from cronstable.config import _dag_condition

    with pytest.raises(ValueError, match="`in` needs at least one value"):
        _dag_condition({"param": "mode", "in": []}, {})
    # strictyaml refuses a key the schema does not have
    with pytest.raises(ConfigError):
        _dagcfg(_WHEN_YAML + _when_entry("param: mode", "matches: f.*"))


def test_when_is_a_node_key_that_defaults_cannot_set():
    with pytest.raises(ConfigError):
        _dagcfg(
            "defaults:\n  when:\n    - param: mode\n      equals: full\n"
            + _WHEN_YAML
            + _when_entry("param: mode", "equals: full")
        )


def test_when_loads_typed_and_exports_in_the_configuration_shape():
    cfg = _dagcfg(
        _WHEN_YAML
        + _when_entry("param: mode", "equals: full")
        + _when_entry("param: count", "in:", "  - '2'", "  - '1'", "  - '2'")
        + _when_entry("param: ratio", "notEquals: '2'")
        + _when_entry("param: dry", "notIn:", "  - 'yes'")
        + _when_entry(
            "xcom:", "  task: a", "  key: rows", "notIn:", "  - '0'",
            "  - ''", "  - '0'",
        )
        + "        skipExitCodes:\n          - 99\n"
        + "      - id: gate\n        type: approval\n"
        + "        when:\n"
        + _when_entry("param: dry", "equals: 'false'")
    )
    spec = cfg.dags[0].spec
    when = spec.by_id["b"].when
    assert when == (
        _p("mode", "equals", "full"),
        _p("count", "in", 2, 1),
        _p("ratio", "notEquals", 2),
        _p("dry", "notIn", True),
        _x("a", "rows", "notIn", "0", ""),
    )
    assert [type(v) for v in when[1].values] == [int, int]
    assert when[3].values[0] is True
    assert spec.by_id["gate"].when == (_p("dry", "equals", False),)
    assert spec.engine == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    assert [t.id for t in spec.conditional_tasks] == ["b"]
    assert dag.when_export(when) == [
        {"param": "mode", "equals": "full"},
        {"param": "count", "in": [2, 1]},
        {"param": "ratio", "notEquals": 2},
        {"param": "dry", "notIn": [True]},
        {"xcom": {"task": "a", "key": "rows"}, "notIn": ["0", ""]},
    ]
    json.dumps(dag.when_export(when))  # serves as JSON
    # a node key, so it never reaches the launch template
    assert not hasattr(cfg.dags[0].task_templates["b"], "when")


def test_the_join_lint_names_a_join_below_two_conditional_branches(caplog):
    text = (
        "dags:\n  - name: nightly\n    params:\n"
        "      - name: mode\n        default: inc\n"
        "    tasks:\n"
        "      - id: full\n        command: 'e'\n"
        "        when:\n          - param: mode\n            equals: full\n"
        "      - id: incremental\n        command: 'e'\n"
        "        when:\n          - param: mode\n            notEquals: full\n"
        "      - id: publish\n        command: 'e'\n"
        "        dependsOn:\n          - full\n          - incremental\n"
    )
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        _xsect(text)
    (record,) = [r for r in caplog.records if "can end skipped" in r.message]
    assert "(full, incremental)" in record.getMessage()
