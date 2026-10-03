"""The DAG orchestration core: the state machine, as pure functions.

This module is the *logic* half of the durable DAG tier -- the analogue of
:mod:`cronstable.jobstate`.  It turns a static DAG definition plus the
current durable ``dag_run`` document into the decisions the scheduler acts on:
which tasks are ready, which to claim (``pending -> running``), how a finished
task moves the graph forward, how a dynamically-mapped task fans out, and when
the whole run is terminal.  It holds **no** I/O: every function is a pure
transform over plain dicts, so the whole state machine is unit-testable without
a backend, a clock, or a subprocess, and the cron wiring
(:mod:`cronstable.cron`) is a thin driver that persists the results through
:meth:`cronstable.state.StateBackend.mutate_document`.

A ``dag_run`` is stored as a single mutable *document* (see the layout in
:func:`new_run_body`), not a record stream: the core operation -- "flip this
task ``pending -> running`` only if it is still pending" -- is a
compare-and-set, exactly what ``mutate_document`` (a flock-guarded
read-modify-write) provides, and modelling the whole run as one document lets a
single atomic RMW claim every ready task at once.  The scheduler advances a run
only while holding that run's advance lease, so a fleet never double-advances;
the RMW claim is the correctness backstop underneath the lease (two would-be
advancers cannot both flip the same task, because the RMW serialises them).

Everything here is deterministic given ``(spec, body, now)``.  In particular a
dynamically-mapped task's expansion is recorded **once** in the body and never
recomputed, so a crash-resumed run reconstructs the identical mapped set rather
than re-deriving it from a possibly-changed upstream output.
"""

import re
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any

from cronstable import _json
from cronstable.params import ParamSpec, env_text

# --------------------------------------------------------------------------
# Durable namespaces (under the backend's docs/ and records/ trees)
# --------------------------------------------------------------------------

#: document namespace prefix for a dag's runs: ``dagrun/<dag_name>`` keyed by
#: the run key.  Documents live outside the record garbage collector,
#: so old terminal runs are reclaimed by the DAG-owned pruner, not the record
#: GC keep-set.
DAG_RUN_NS_PREFIX = "dagrun/"

#: lease-name prefix the scheduler advances a run under (the TTL lease trio);
#: one lease per run, distinct from every job/slot lease name.  The GC
#: callers pass this prefix as the one EPHEMERAL lease class the backend may
#: reclaim: a ``dagadvance/<dag>/<run_key>`` name recurs only if the same
#: run key is re-created after its run document was already GC'd, and no
#: fence for it is persisted outside the run document's own lifetime --
#: unlike slot/retry-claim leases, whose fences live on in durable slot
#: cancel records.
DAG_LEASE_PREFIX = "dagadvance/"

#: artifact-stream scope prefix for a run's XCom: the cross-task hand-off is
#: the durable artifact store scoped by ``dagxcom/<dag_name>/<run_id>``.
XCOM_SCOPE_PREFIX = "dagxcom/"

# Environment variables the daemon injects into every DAG task (on top of the
# durable ``CRONSTABLE_STATE_*`` control-channel vars), so the task -- and the
# ``cronstable xcom`` CLI it calls -- knows which run/task it is and where its
# XCom scope lives.  Defined here (a dependency-free module) so both the daemon
# (:mod:`cronstable.dagrun`) and the offline CLI (:mod:`cronstable.jobcli`)
# share the exact names without either pulling in the other's imports.
ENV_DAG_NAME = "CRONSTABLE_DAG_NAME"
ENV_DAG_RUN_ID = "CRONSTABLE_DAG_RUN_ID"
ENV_DAG_RUN_KEY = "CRONSTABLE_DAG_RUN_KEY"
ENV_DAG_TASK = "CRONSTABLE_DAG_TASK"  # the base task id
ENV_DAG_TASKKEY = "CRONSTABLE_DAG_TASKKEY"  # the instance key (id or id#index)
ENV_DAG_MAP_INDEX = "CRONSTABLE_DAG_MAP_INDEX"
ENV_DAG_MAP_ITEM = "CRONSTABLE_DAG_MAP_ITEM"
ENV_DAG_XCOM_SCOPE = "CRONSTABLE_DAG_XCOM_SCOPE"


# --------------------------------------------------------------------------
# Task / run states
# --------------------------------------------------------------------------

PENDING = "pending"
RUNNING = "running"
#: a plain task that failed but still has retry attempts left; non-terminal, so
#: the run stays alive and the next advance re-claims it once its retry delay
#: has elapsed.
UP_FOR_RETRY = "up_for_retry"
SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"
UPSTREAM_FAILED = "upstream_failed"
#: bookkeeping state of a mapped task's *group* placeholder once its item list
#: has been materialised into ``<id>#<i>`` instances; never a real task run.
EXPANDED = "expanded"

#: the terminal task states dependency resolution treats as "done".
TERMINAL_STATES = frozenset({SUCCESS, FAILED, SKIPPED, UPSTREAM_FAILED})
#: terminal states that count as a *success* for a downstream ``all_success``.
SUCCESS_STATES = frozenset({SUCCESS})
#: terminal states that make a downstream ``all_success`` dependency fail.
FAILURE_STATES = frozenset({FAILED, UPSTREAM_FAILED})
#: the states a claim pass leaves untouched: terminal, plus the EXPANDED
#: group placeholder whose instances carry the real work.  One membership
#: test for what was a membership test plus a comparison, on the first line
#: of the per-instance dispatch.
_INERT_TASK_STATES = TERMINAL_STATES | {EXPANDED}

TASK = "task"
SENSOR = "sensor"
APPROVAL = "approval"

ALL_SUCCESS = "all_success"
ALL_DONE = "all_done"
NONE_FAILED = "none_failed"
NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
ALL_DONE_MIN_ONE_FAILED = "all_done_min_one_failed"

#: Every ``triggerRule`` value, in documentation order. Each rule is evaluated
#: once every upstream is terminal (see :func:`_deps_verdict`).
TRIGGER_RULES = (
    ALL_SUCCESS,
    ALL_DONE,
    NONE_FAILED,
    NONE_FAILED_MIN_ONE_SUCCESS,
    ALL_DONE_MIN_ONE_FAILED,
)
#: The rules every build evaluates. A DAG that uses any other rule needs
#: :data:`BRANCHING_PARAMS_ENGINE_LEVEL`.
_BASE_TRIGGER_RULES = frozenset({ALL_SUCCESS, ALL_DONE})

#: ``skipReason.kind`` values: why a task entry is ``skipped``.
SKIP_EXIT_CODE = "exit_code"  # its command exited with a skipExitCodes code
SKIP_UPSTREAM = "upstream"  # an upstream was skipped (all_success)
SKIP_TRIGGER_RULE = "trigger_rule"  # its rule skips on these upstream states
SKIP_APPROVAL = "approval"  # its gate was rejected with onReject: skip
SKIP_CONDITION = "condition"  # a when: comparison did not hold

#: ``when:`` sources: where a comparison reads its value from.
WHEN_PARAM = "param"
WHEN_XCOM = "xcom"

#: ``when:`` operators, in documentation order. ``equals`` and ``in`` hold
#: when the source equals one of the values; the other two hold when it
#: equals none of them.
WHEN_EQUALS = "equals"
WHEN_NOT_EQUALS = "notEquals"
WHEN_IN = "in"
WHEN_NOT_IN = "notIn"
WHEN_OPERATORS = (WHEN_EQUALS, WHEN_NOT_EQUALS, WHEN_IN, WHEN_NOT_IN)
_WHEN_POSITIVE = frozenset({WHEN_EQUALS, WHEN_IN})

#: Byte ceiling on an XCom value a ``when:`` comparison reads, and on each
#: value it compares with. A larger published value reads as no value.
MAX_CONDITION_XCOM_BYTES = 4096

#: Hard cap on a mapped task's fan-out: a cron daemon shares its host, so an
#: unbounded XCom list must not become an unbounded instance set (run-document
#: bloat, subprocess stampede); past the cap the mapped task FAILS with an
#: explanatory reason instead of expanding.
MAX_MAPPED_ITEMS = 1000

#: Byte ceiling on a mapped fan-out's serialized XCom blob, enforced by the
#: consumer (:meth:`DagRunner._read_xcom_list`) BEFORE the blob is fetched or
#: decoded.  MAX_MAPPED_ITEMS bounds the item COUNT but only after the list is
#: in memory; a publisher that set ``maxArtifactBytes: 0`` (no publish-time
#: limit) could otherwise hand the fan-out an arbitrarily large blob that OOMs
#: the daemon during fetch/decode.  Sized generously above any legitimate
#: MAX_MAPPED_ITEMS fan-out of small pointer-sized items (~16 KiB/item).
MAX_MAPPED_XCOM_BYTES = 16 * 1024 * 1024

#: At most this many instances are claimed -- and therefore launched -- by one
#: advance pass; the rest stays claimable and the driver re-services promptly
#: (``AdvanceResult.deferred``), bounding any single pass's spawn burst.
MAX_CLAIMS_PER_PASS = 32

#: The engine level every build supports. A run document with no ``engine``
#: key is at this level, and so is a node whose manifest has no ``dagEngine``.
BASE_ENGINE_LEVEL = 1

#: The level a DAG needs once it branches or takes run parameters: a task
#: uses ``skipExitCodes``, ``when:``, or a trigger rule outside
#: ``all_success`` and ``all_done``, or the DAG declares ``params:``. The
#: features share one level because every build has all of them or none.
BRANCHING_PARAMS_ENGINE_LEVEL = 2

#: The highest run engine level this build advances. A run records the level
#: its DAG needs when it is created (``DagSpec.engine``), and a build leaves a
#: run above its own level untouched for a newer node (:func:`supports_run`).
ENGINE_LEVEL = BRANCHING_PARAMS_ENGINE_LEVEL


# --------------------------------------------------------------------------
# Static DAG specification (built by config.py, consumed here)
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExpandSpec:
    """A dynamic-mapping directive: fan out over an upstream's XCom list."""

    from_task: str
    key: str


@dataclass(frozen=True, slots=True)
class Condition:
    """One ``when:`` comparison: a source, an operator, and its values.

    ``source`` is :data:`WHEN_PARAM` or :data:`WHEN_XCOM`. ``name`` is the
    parameter name or the id of the task that published the XCom value, and
    ``key`` is the XCom key (empty for a parameter). ``values`` holds one
    value for ``equals`` and ``notEquals`` and the whole list for ``in`` and
    ``notIn``: text for XCom, and the declared type for a parameter.
    """

    source: str
    name: str
    op: str
    values: tuple[Any, ...]
    key: str = ""


@dataclass(frozen=True, slots=True)
class NoXcomValue:
    """Why an XCom source holds no value a ``when:`` comparison can read.

    The driver hands one of these to :func:`plan_and_claim` in place of the
    text. ``reason`` ends the ``skipReason`` detail.
    """

    reason: str


XCOM_UNPUBLISHED = NoXcomValue("the key is not published")
XCOM_TOO_LARGE = NoXcomValue(
    "the value is over {} bytes".format(MAX_CONDITION_XCOM_BYTES)
)
XCOM_NOT_TEXT = NoXcomValue("the value is not UTF-8 text")
XCOM_GONE = NoXcomValue("the published value is no longer stored")
_XCOM_NOT_IN_RUN = NoXcomValue("the task is not in this run")


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """One node of a DAG, normalised for the state machine.

    A ``TaskSpec`` is everything the pure logic needs; the *how to run it*
    (command, env, timeouts) lives in a sibling ``cronstable.config.JobConfig``
    launch template the cron driver holds, so this module stays I/O-free.
    """

    id: str
    type: str = TASK
    depends_on: tuple[str, ...] = ()
    trigger_rule: str = ALL_SUCCESS
    max_attempts: int = 1
    retry_delay: float = 0.0
    expand: ExpandSpec | None = None
    # sensor poke schedule (bounded, jittered, durable)
    poke_interval: float = 30.0
    poke_timeout: float = 3600.0
    poke_jitter: float = 0.0
    # approval gate: what a rejected gate does to the graph
    on_reject: str = FAILED  # FAILED or SKIPPED
    # exit codes with which the task's command skips the task
    skip_exit_codes: tuple[int, ...] = ()
    # comparisons that all have to hold for the task to run
    when: tuple[Condition, ...] = ()


@dataclass(frozen=True)
class DagSpec:
    """A whole DAG, normalised: ordered tasks, an id index, the mapped subset.

    ``mapped_tasks`` is the ``expand``-carrying subset, precomputed here
    because :func:`tasks_awaiting_expansion` runs on every advance and the
    overwhelmingly common DAG has none: the check becomes O(1) instead of a
    full spec walk per pass.

    ``conditional_tasks`` is the subset whose ``when:`` reads an XCom value,
    precomputed for the same reason: :func:`tasks_awaiting_conditions` runs
    on every advance.

    ``engine`` is the run engine level the DAG's features need. Each run
    created from the spec records it (see :func:`new_run_body`).

    ``params`` is the DAG's run parameter declaration, empty for a DAG that
    declares none.
    """

    name: str
    tasks: tuple[TaskSpec, ...]
    by_id: dict[str, TaskSpec] = field(default_factory=dict)
    mapped_tasks: tuple[TaskSpec, ...] = ()
    engine: int = BASE_ENGINE_LEVEL
    params: tuple[ParamSpec, ...] = ()
    conditional_tasks: tuple[TaskSpec, ...] = ()

    @staticmethod
    def build(
        name: str, tasks: list[TaskSpec], params: tuple[ParamSpec, ...] = ()
    ) -> "DagSpec":
        gated = [t for t in tasks if t.when]
        if (
            gated
            or params
            or any(
                t.skip_exit_codes or t.trigger_rule not in _BASE_TRIGGER_RULES
                for t in tasks
            )
        ):
            engine = BRANCHING_PARAMS_ENGINE_LEVEL
        else:
            engine = BASE_ENGINE_LEVEL
        return DagSpec(
            name=name,
            tasks=tuple(tasks),
            by_id={t.id: t for t in tasks},
            mapped_tasks=tuple(t for t in tasks if t.expand is not None),
            engine=engine,
            params=tuple(params),
            conditional_tasks=tuple(
                t for t in gated if any(c.source == WHEN_XCOM for c in t.when)
            ),
        )


class DagValidationError(Exception):
    """A malformed DAG graph (unknown dep, cycle, bad expand target)."""


#: The characters :func:`validate_graph` bars from a task id: the C0 control
#: range and DEL.  One precompiled scan per id, not a per-character generator:
#: config load validates every task of every DAG, and the ``any(ord(ch) ...)``
#: form paid a generator resume plus two ``ord`` calls for every character of
#: every id (two thirds of the validation walk on a 10k-task graph).
_ID_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def validate_graph(spec: DagSpec) -> None:
    """Raise :class:`DagValidationError` on an unusable graph.

    Checks unknown/duplicate ids, a safe id charset, that every ``dependsOn``
    resolves, that an ``expand.fromTask`` is a *direct*, non-mapped dependency,
    that mapped tasks are plain ``task`` nodes, that a ``when:`` comparison
    reads XCom from a non-mapped task upstream of its own, and that the
    dependency graph is acyclic (a cycle would never advance).  Called from
    config parsing so a bad DAG is a :class:`~cronstable.config.ConfigError`
    at load, not a runtime hang.
    """
    seen: dict[str, TaskSpec] = {}
    for task in spec.tasks:
        if not task.id:
            raise DagValidationError("a task id must be non-empty")
        # '#' and '/' are structural separators in a mapped instance key
        # (``id#index``) and an XCom name (``taskkey/key``); an id containing
        # one could alias another task's instance/XCom key and silently
        # overwrite its state, so reject them.
        if "#" in task.id or "/" in task.id:
            raise DagValidationError(
                "task id {!r} may not contain '#' or '/'".format(task.id)
            )
        # The id reaches %s log sinks and durable keys verbatim, so a control
        # character (CR/LF) could forge or split daemon log lines. Reject the
        # C0 range and DEL here (the docstring already promises a safe
        # charset) without narrowing the printable set configs may rely on.
        if _ID_CONTROL.search(task.id):
            raise DagValidationError(
                "task id {!r} may not contain control characters".format(
                    task.id
                )
            )
        if task.id in seen:
            raise DagValidationError("duplicate task id {!r}".format(task.id))
        seen[task.id] = task
    for task in spec.tasks:
        for dep in task.depends_on:
            if dep not in seen:
                raise DagValidationError(
                    "task {!r} dependsOn unknown task {!r}".format(
                        task.id, dep
                    )
                )
            if dep == task.id:
                raise DagValidationError(
                    "task {!r} dependsOn itself".format(task.id)
                )
        if task.expand is not None:
            _validate_expand(task, seen)
    _check_acyclic(spec)
    # after the cycle check, so the upstream walk below always ends
    for task in spec.conditional_tasks:
        _validate_when(task, seen)


def _validate_when(task: TaskSpec, seen: dict[str, TaskSpec]) -> None:
    """Check the XCom sources of ``task``'s ``when:`` comparisons.

    Each one names a task upstream of ``task`` through ``dependsOn`` edges,
    so the value is final by the time the comparison is read, and a task
    that is not mapped, because a mapped task publishes one value per
    instance.
    """
    upstream: set[str] = set()
    pending = list(task.depends_on)
    while pending:
        dep = pending.pop()
        if dep not in upstream:
            upstream.add(dep)
            pending.extend(seen[dep].depends_on)
    for cond in task.when:
        if cond.source != WHEN_XCOM:
            continue
        if cond.name not in seen:
            raise DagValidationError(
                "task {!r}: when: xcom task {!r} is not a task".format(
                    task.id, cond.name
                )
            )
        if cond.name not in upstream:
            raise DagValidationError(
                "task {!r}: when: xcom task {!r} is not upstream of this "
                "task; add it to dependsOn, or depend on a task that runs "
                "after it".format(task.id, cond.name)
            )
        if seen[cond.name].expand is not None:
            raise DagValidationError(
                "task {!r}: when: xcom task {!r} is mapped and publishes "
                "one value per instance, so a comparison cannot read "
                "it".format(task.id, cond.name)
            )


def skippable_joins(spec: DagSpec) -> list[tuple[str, list[str]]]:
    """``all_success`` tasks that join two or more upstreams that can skip.

    Such a join is skipped whenever one of those upstreams is, which is
    rarely what a join below alternative branches wants. Returns each join's
    id with the upstreams concerned, in spec order, for the load-time warning.

    An upstream can skip when its command can (``skip_exit_codes``), when it
    has a ``when:`` condition, when it is a gate with ``onReject: skip``,
    when its rule is ``all_done_min_one_failed``, or when its own rule
    passes a skip on: ``all_success`` from any upstream,
    ``none_failed_min_one_success`` once every upstream can skip, and a
    mapped task from its expand source.
    """
    can_skip = {
        t.id
        for t in spec.tasks
        if t.skip_exit_codes
        or t.when
        or t.trigger_rule == ALL_DONE_MIN_ONE_FAILED
        or (t.type == APPROVAL and t.on_reject == SKIPPED)
    }
    if not can_skip:
        return []
    dependents: dict[str, list[TaskSpec]] = {}
    for t in spec.tasks:
        for dep in t.depends_on:
            dependents.setdefault(dep, []).append(t)
    pending = list(can_skip)
    while pending:
        for t in dependents.get(pending.pop(), ()):
            if t.id in can_skip:
                continue
            if t.expand is not None and t.expand.from_task in can_skip:
                passes = True
            elif t.trigger_rule == NONE_FAILED_MIN_ONE_SUCCESS:
                passes = all(dep in can_skip for dep in t.depends_on)
            else:
                passes = t.trigger_rule not in (ALL_DONE, NONE_FAILED)
            if passes:
                can_skip.add(t.id)
                pending.append(t.id)
    joins = []
    for t in spec.tasks:
        if t.trigger_rule != ALL_SUCCESS:
            continue
        upstreams = [
            dep for dep in dict.fromkeys(t.depends_on) if dep in can_skip
        ]
        if len(upstreams) > 1:
            joins.append((t.id, upstreams))
    return joins


def _validate_expand(task: TaskSpec, seen: dict[str, TaskSpec]) -> None:
    exp = task.expand
    assert exp is not None
    if task.type != TASK:
        raise DagValidationError(
            "task {!r}: only a plain task can be mapped with expand, "
            "not a {}".format(task.id, task.type)
        )
    if exp.from_task not in seen:
        raise DagValidationError(
            "task {!r}: expand.fromTask {!r} is not a task".format(
                task.id, exp.from_task
            )
        )
    if exp.from_task not in task.depends_on:
        raise DagValidationError(
            "task {!r}: expand.fromTask {!r} must be a direct "
            "dependsOn".format(task.id, exp.from_task)
        )
    if seen[exp.from_task].expand is not None:
        raise DagValidationError(
            "task {!r}: expand.fromTask {!r} is itself mapped; chaining "
            "mapped tasks is not supported".format(task.id, exp.from_task)
        )


def _check_acyclic(spec: DagSpec) -> None:
    # Kahn's algorithm over the DEDUPED edge set: a repeated dependsOn entry
    # is one edge (counting it twice would leave a phantom indegree and a
    # false cycle verdict on an acyclic graph).
    #
    # Indegrees, the reverse adjacency and the initial ready set are built in
    # ONE walk over the tasks (they were four passes, three of them re-walking
    # the same tuple or re-reading the deduped map).  Popping a node then
    # rescanning every task for it made the walk O(V^2) set tests (a 10k-task
    # chain took seconds of the config load).  validate_graph has already
    # rejected unknown deps, self-deps and duplicate ids above, so every dep
    # here names a real node and the edge set is identical either way.
    #
    # Two allocations per node are dropped along the way: the dedupe set is
    # skipped for the 0- and 1-dep shapes, which cannot repeat an entry (the
    # common node in a chain or a wide DAG), and each dependents list is built
    # on its first edge rather than through ``setdefault(dep, [])``, which
    # constructed and discarded a list on EVERY edge.  ``ready`` keeps its old
    # contents and order: both it and ``indeg`` are filled in spec.tasks
    # order, exactly what iterating ``indeg`` afterwards yielded.
    indeg: dict[str, int] = {}
    dependents: dict[str, list[str]] = {}
    ready: list[str] = []
    for t in spec.tasks:
        deps: Collection[str] = t.depends_on
        if len(deps) > 1:
            deps = set(deps)
        indeg[t.id] = len(deps)
        if not deps:
            ready.append(t.id)
            continue
        for dep in deps:
            downs = dependents.get(dep)
            if downs is None:
                dependents[dep] = [t.id]
            else:
                downs.append(t.id)
    ordered = 0
    while ready:
        tid = ready.pop()
        ordered += 1
        for down in dependents.get(tid, ()):
            indeg[down] -= 1
            if indeg[down] == 0:
                ready.append(down)
    if ordered != len(spec.tasks):
        cyclic = sorted(tid for tid, d in indeg.items() if d > 0)
        raise DagValidationError(
            "the dependency graph has a cycle involving: {}".format(
                ", ".join(cyclic)
            )
        )


# --------------------------------------------------------------------------
# Run key / XCom key helpers
# --------------------------------------------------------------------------

_KEY_SAFE = re.compile(r"[^0-9A-Za-z_.:-]+")


def run_key_for_logical(logical_iso: str) -> str:
    """A filesystem-safe run key for a scheduled logical instant.

    Deterministic from the instant, so create-if-absent naturally dedupes a
    logical date to exactly one run (two nodes racing to schedule the same
    fire converge on the same document key).
    """
    return _KEY_SAFE.sub("_", logical_iso)


def xcom_scope(dag_name: str, run_id: str) -> str:
    """The artifact scope holding a run's XCom hand-offs."""
    return "{}{}/{}".format(XCOM_SCOPE_PREFIX, dag_name, run_id)


def xcom_name(taskkey: str, key: str) -> str:
    """The artifact name a task publishes an XCom ``key`` under."""
    return "{}/{}".format(taskkey, key)


def task_display_key(task_id: str, map_index: int | None) -> str:
    """The per-instance key: ``id`` or ``id#<map_index>`` for a mapped run."""
    if map_index is None:
        return task_id
    # concatenation, not format(): this is called once per instance of a
    # fan-out that can hold MAX_MAPPED_ITEMS entries, and the format machinery
    # is ~2x the cost for a two-field key with no format spec.
    return task_id + "#" + str(map_index)


# --------------------------------------------------------------------------
# The run document
# --------------------------------------------------------------------------


def new_run_body(
    *,
    dag: str,
    run_key: str,
    run_id: str,
    logical_date: str | None,
    kind: str,
    now: float,
    spec: DagSpec,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The initial ``dag_run`` document: every task pending, run running.

    Mapped tasks start as a single ``pending`` placeholder carrying
    ``mapped: true``; they materialise into ``<id>#<i>`` instances once their
    upstream produces the item list (see :func:`plan_and_claim`).

    The document carries ``engine`` only when the DAG needs more than
    :data:`BASE_ENGINE_LEVEL`, and ``params`` only when the caller passes
    the run's resolved parameters, which it does for a DAG that declares
    them. No transform changes ``params`` afterwards.
    """
    tasks = {task.id: _new_task_entry(task, now) for task in spec.tasks}
    body: dict[str, Any] = {
        "dag": dag,
        "runKey": run_key,
        "runId": run_id,
        "logicalDate": logical_date,
        "kind": kind,
        "state": RUNNING,
        "createdAt": now,
        "updatedAt": now,
        "tasks": tasks,
        "mapped": {},
    }
    if spec.engine > BASE_ENGINE_LEVEL:
        body["engine"] = spec.engine
    if params is not None:
        body["params"] = params
    return body


#: The constant shape of a fresh task entry, copied rather than rebuilt: one
#: run document holds one of these per task and up to MAX_MAPPED_ITEMS per
#: mapped fan-out, so ``dict.copy()`` beats re-executing the literal.  Every
#: value is an immutable scalar, so the copies share nothing mutable, and
#: copy-then-overwrite preserves the key order the literal produced.
_TASK_ENTRY: dict[str, Any] = {
    "id": None,
    "mapIndex": None,
    "state": PENDING,
    "attempt": 0,
    "proc": None,
    "pid": None,
    "host": None,
    "startedAt": None,
    "finishedAt": None,
    "exitCode": None,
    "failReason": None,
    # sampled CPU/peak-RSS of the finished instance (monitorResources);
    # absent from pre-feature documents, so read it with .get().
    "resources": None,
    "updatedAt": None,
}


def _new_task_entry(task: TaskSpec, now: float) -> dict[str, Any]:
    entry = _TASK_ENTRY.copy()
    entry["id"] = task.id
    entry["updatedAt"] = now
    if task.expand is not None:
        entry["mapped"] = True
    if task.type == SENSOR:
        entry["pokeCount"] = 0
        entry["nextPokeAt"] = None
    if task.type == APPROVAL:
        entry["approval"] = None
    return entry


def is_terminal_run(body: dict[str, Any]) -> bool:
    return body.get("state") in (SUCCESS, FAILED)


def supports_run(body: dict[str, Any]) -> bool:
    """Whether this build may adopt and advance the run in ``body``.

    False for a run above :data:`ENGINE_LEVEL` and for an ``engine`` value
    that is not an integer, so a build never applies rules it does not know.
    """
    level = body.get("engine", BASE_ENGINE_LEVEL)
    return type(level) is int and level <= ENGINE_LEVEL


# --------------------------------------------------------------------------
# Launch intents returned by the claim transform
# --------------------------------------------------------------------------


@dataclass(slots=True)
class LaunchIntent:
    """One task instance the driver should now start a subprocess for."""

    task_id: str
    taskkey: str
    map_index: int | None
    map_item: Any
    attempt: int
    is_sensor: bool
    poke_number: int  # 0-based poke count for a sensor; 0 for a plain task


@dataclass
class AdvanceResult:
    """What the claim transform decided (its ``mutate_document`` result)."""

    launches: list[LaunchIntent] = field(default_factory=list)
    changed: bool = False
    run_terminal: bool = False
    # claims hit MAX_CLAIMS_PER_PASS: more instances are claimable right now,
    # so the driver should re-service promptly rather than wait for a wake.
    deferred: bool = False
    # the pass left a step only the next pass can take: a comparison needs
    # an XCom value the driver has not read, or a mapped task's condition
    # held and its fan-out can now expand.  The driver advances again
    # promptly, as for ``deferred``, but claims in this pass go on.
    again: bool = False


@dataclass
class ReconcileAdvanceResult:
    """What :func:`reconcile_and_plan` decided (its RMW result).

    Carries the reconcile half's count next to the claim half's ordinary
    :class:`AdvanceResult`, so the driver's launch, deferred and wake logic
    consumes the exact shape :func:`plan_and_claim` already returns.
    """

    # how many crash-interrupted tasks the reconcile half recovered.
    reconciled: int = 0
    # mapped tasks are awaiting expansion: only the reconcile half was
    # applied (``advance`` is None then), and the driver must pre-read the
    # upstream XCom lists and run :func:`plan_and_claim` as a second RMW.
    expansions_needed: bool = False
    # ready tasks have when: comparisons over XCom values: the same split as
    # ``expansions_needed``, with the driver pre-reading those values.
    conditions_needed: bool = False
    # the claim half's result when it ran inside this same RMW.
    advance: AdvanceResult | None = None
    # the run is above this build's engine level (see supports_run): the
    # document was kept untouched and the driver must release the run.
    unsupported: bool = False


# --------------------------------------------------------------------------
# Dependency resolution over the run body
# --------------------------------------------------------------------------


#: Absent-key sentinel for the ``body["mapped"]`` lookups that ask both "is
#: this task recorded as expanded?" and "give me its record": one ``.get``
#: with the sentinel answers both, where a membership test followed by a
#: ``.get`` hashed the id twice on paths that run once per task per advance
#: pass.  Distinct from ``None``, which a damaged document can hold as a
#: recorded value and which those paths already treat as "no instances".
_UNSET = object()


def _fold_mapped_instances(
    tasks: dict[str, Any], prefix: str, count: int
) -> tuple[bool, bool, bool]:
    """Reduce an expanded task's instances to ``(holds, failed, skipped)``.

    THE one absent-entry rule (and the one WALK over it) shared by the
    fan-in barrier (:func:`_mapped_group_state`) and the run terminaliser
    (:func:`_maybe_terminalise`).  Expansion materialises every instance in
    the same RMW that records the item list, so an entry missing for a
    run-recorded index can only mean a foreign or damaged run document; it
    reads as ``pending`` (non-terminal) in BOTH consumers.  The two once
    disagreed (barrier held, terminaliser skipped), so the same document
    could complete as a run while its group still read ``running`` to every
    downstream; under the shared rule a hole holds the run open instead.
    Holding it open is safe because the claim pass repairs it: the same
    advance transform runs :func:`_propagate_and_claim` first, which
    materialises a run-recorded index it finds missing and fails it (see
    :func:`_resolve_missing_instance`), so the run terminalises on that
    very pass rather than wedging.

    ``holds`` is the barrier verdict: at least one instance is not terminal,
    which BOTH consumers act on before anything else, so the walk returns on
    the first one and never builds the remaining keys.  Folding here rather
    than per instance keeps the two consumers call-free over a fan-out that
    can hold MAX_MAPPED_ITEMS entries.
    """
    failed = False
    skipped = False
    tasks_get = tasks.get
    for i in range(count):
        entry = tasks_get(prefix + str(i))
        state = PENDING if entry is None else entry.get("state", PENDING)
        # An equality chain, not the TERMINAL_STATES/FAILURE_STATES
        # memberships: SUCCESS is the overwhelmingly common instance state of
        # a fan-out being folded, and settling it takes ONE comparison here
        # where the sets took a hash plus a lookup in each of two frozensets.
        # It also drops the str() coercion the memberships needed: a
        # damaged document's non-string (even unhashable) state compares
        # unequal to all four and falls to the same "not terminal" arm the
        # coercion produced, instead of hashing it inside a flock.
        if state == SUCCESS:
            continue
        if state == FAILED or state == UPSTREAM_FAILED:
            failed = True
        elif state == SKIPPED:
            skipped = True
        else:
            return True, failed, skipped
    return False, failed, skipped


def _mapped_group_state(body: dict[str, Any], task_id: str) -> str:
    """The aggregate state of a mapped task, for downstream dep checks.

    Un-expanded -> the placeholder's own state (``pending`` normally, or a
    terminal ``upstream_failed`` / ``skipped`` if its upstream failed before it
    could expand); expanded to an empty list -> ``success``; otherwise the
    reduction over its ``<id>#<i>`` instances -- but ONLY once every instance
    is terminal (the fan-in barrier): while any instance is still going the
    group reads ``running`` even if a sibling already failed, so a downstream
    (of either trigger rule) never starts against a half-finished fan-out.
    Once all are terminal: any failed/upstream_failed -> ``upstream_failed``;
    else any skipped -> ``skipped``; else ``success``.
    """
    mapped_all = body.get("mapped")
    mapped = mapped_all.get(task_id) if mapped_all else None
    if mapped is None:
        entry = body["tasks"].get(task_id)
        return PENDING if entry is None else str(entry.get("state", PENDING))
    items = mapped.get("items", [])
    if not items:
        return SUCCESS
    # One pass with an early exit, not a materialised states list plus three
    # scans: while a fan-out is in flight the FIRST instance is usually
    # non-terminal, so building the other MAX_MAPPED_ITEMS-1 keys and lookups
    # only to discard them is the bulk of this function's cost.
    holds, failed, skipped = _fold_mapped_instances(
        body["tasks"], task_id + "#", len(items)
    )
    if holds:
        return RUNNING  # fan-in barrier: not every instance is terminal
    if failed:
        return UPSTREAM_FAILED
    if skipped:
        return SKIPPED
    return SUCCESS


def effective_state(spec: DagSpec, body: dict[str, Any], task_id: str) -> str:
    """The state a dependency check should see for ``task_id``.

    Keyed on the RUN's recorded expansion as well as the spec: a run whose
    task fanned out under an older spec keeps folding its instances even if
    a reload since removed ``expand:`` from the task. Its placeholder is
    parked in the non-terminal EXPANDED state, so consulting it instead
    would leave every downstream (and the run) waiting forever.
    """
    task = spec.by_id[task_id]
    mapped_all = body.get("mapped")
    if task.expand is not None or (mapped_all and task_id in mapped_all):
        return _mapped_group_state(body, task_id)
    entry = body["tasks"].get(task_id)
    return PENDING if entry is None else str(entry.get("state", PENDING))


def _deps_verdict(spec: DagSpec, body: dict[str, Any], task: TaskSpec) -> str:
    """Resolve a task's upstreams into one of: ready / wait / fail / skip.

    ``ready`` -- launch it; ``wait`` -- upstreams still running; ``fail`` --
    the task ends ``upstream_failed``; ``skip`` -- the task ends ``skipped``.

    Every rule waits until each upstream is terminal. ``all_done`` is then
    ready. With no failed and no skipped upstream, ``all_done_min_one_failed``
    skips and every other rule is ready. Otherwise:

    * ``all_done_min_one_failed`` is ready on a failure and skips without one.
    * A failure fails every other rule.
    * On a skip without a failure, ``none_failed`` is ready,
      ``none_failed_min_one_success`` is ready when an upstream succeeded,
      and ``all_success`` skips.

    An upstream with no entry in the run is left out. A rule string this
    build does not know reads as ``all_success``.
    """
    if not task.depends_on:
        # a root task is ready under either trigger rule (no upstream can be
        # non-terminal, failed or skipped); this is the common shape in a wide
        # DAG and it skips the whole reduction below.
        return "ready"
    # ``or {}`` rather than a ``.get`` default: the default is built on every
    # call, key present or not, and this resolves once per task per pass.
    tasks = body.get("tasks") or {}
    by_id = spec.by_id
    mapped_all = body.get("mapped")
    # A dependency with NO entry in this run document was added to the DAG by a
    # config reload AFTER the run was created (creation materialises every
    # then-current task): it is not part of this run's plan, so it cannot gate
    # the dependent -- an ``effective_state`` of PENDING would leave the
    # dependent, and the whole run, waiting forever.  Mirrors the same
    # "not materialised -> skip" rule in :func:`_maybe_terminalise`.
    failed = False
    skipped = False
    for dep in task.depends_on:
        entry = tasks.get(dep)
        if entry is None:
            continue
        if (mapped_all and dep in mapped_all) or by_id[dep].expand is not None:
            state = _mapped_group_state(body, dep)
        else:
            # effective_state's plain branch, inlined: the fan-out map and
            # the dep's entry are both already in hand here, where the call
            # re-resolved them from the body for every dependency.
            state = entry.get("state", PENDING)
        # The same equality chain :func:`_fold_mapped_instances` folds with,
        # for the same reasons: SUCCESS is the state an upstream reduction
        # almost always lands on, and comparing rather than hashing keeps a
        # damaged document's non-string state out of a frozenset.
        if state == SUCCESS:
            continue
        if state == FAILED or state == UPSTREAM_FAILED:
            failed = True
        elif state == SKIPPED:
            skipped = True
        else:
            # one non-terminal upstream settles it: the remaining reductions
            # (each an O(instances) walk for a mapped upstream) are dead work.
            return "wait"
    rule = task.trigger_rule
    if rule == ALL_DONE:
        return "ready"
    if not failed and not skipped:
        return "skip" if rule == ALL_DONE_MIN_ONE_FAILED else "ready"
    if rule == ALL_DONE_MIN_ONE_FAILED:
        return "ready" if failed else "skip"
    if failed:
        return "fail"
    # an upstream was skipped and none failed
    if rule == NONE_FAILED:
        return "ready"
    if rule == NONE_FAILED_MIN_ONE_SUCCESS:
        return "ready" if _any_upstream_succeeded(body, task) else "skip"
    return "skip"


def _any_upstream_succeeded(body: dict[str, Any], task: TaskSpec) -> bool:
    """Whether one of ``task``'s upstreams counts as a success.

    The ``none_failed_min_one_success`` check, reached only once every
    upstream is terminal and at least one is skipped. A fan-out that reads
    ``skipped`` as a group still counts when one of its instances succeeded,
    so a join runs after a fan-out in which some items skipped themselves.
    """
    tasks = body["tasks"]
    mapped_all = body.get("mapped")
    for dep in task.depends_on:
        entry = tasks.get(dep)
        if entry is None:
            continue
        mapped = mapped_all.get(dep) if mapped_all else None
        if mapped is None:
            if entry.get("state") == SUCCESS:
                return True
            continue
        items = mapped.get("items", [])
        if not items:
            return True  # an empty expansion reads success
        prefix = dep + "#"
        for i in range(len(items)):
            instance = tasks.get(prefix + str(i))
            if instance is not None and instance.get("state") == SUCCESS:
                return True
    return False


def _verdict_skip_reason(
    spec: DagSpec, body: dict[str, Any], task: TaskSpec
) -> dict[str, str]:
    """The ``skipReason`` for a task whose deps verdict is ``skip``."""
    rule = task.trigger_rule
    if rule == ALL_DONE_MIN_ONE_FAILED:
        return {
            "kind": SKIP_TRIGGER_RULE,
            "detail": "{}: no upstream failed".format(rule),
        }
    if rule == NONE_FAILED_MIN_ONE_SUCCESS:
        return {
            "kind": SKIP_TRIGGER_RULE,
            "detail": "{}: no upstream succeeded".format(rule),
        }
    tasks = body["tasks"]
    skipped = [
        dep
        for dep in task.depends_on
        if dep in tasks and effective_state(spec, body, dep) == SKIPPED
    ]
    return {
        "kind": SKIP_UPSTREAM,
        "detail": "upstream skipped: {}".format(", ".join(skipped)),
    }


# --------------------------------------------------------------------------
# The claim transform (the core RMW body)
# --------------------------------------------------------------------------


def tasks_awaiting_expansion(
    spec: DagSpec, body: dict[str, Any]
) -> list[tuple[str, str, str]]:
    """Mapped tasks whose upstream is done but that are not yet expanded.

    Returns ``(task_id, from_task, key)`` triples so the driver can pre-read
    each upstream's XCom list before the claim RMW.  Derived from a plain
    (possibly stale) document read; the claim transform re-validates before
    applying, so a stale snapshot only costs a wasted read, never a wrong
    expansion.
    """
    out: list[tuple[str, str, str]] = []
    if not spec.mapped_tasks:
        return out  # nothing in this DAG can expand: no candidates to walk
    if is_terminal_run(body):
        return out
    mapped_all = body.get("mapped")
    tasks = body.get("tasks") or {}
    for task in spec.mapped_tasks:
        exp = task.expand
        if exp is None or (mapped_all and task.id in mapped_all):
            continue
        entry = tasks.get(task.id)
        if entry is not None and entry.get("state") != PENDING:
            # the placeholder already resolved without expanding (upstream
            # failed/skipped, or the fan-out failed the item cap): re-reading
            # its XCom every pass would be wasted work forever.
            continue
        if task.when and not (entry is not None and entry.get("whenMet")):
            # its condition is undecided and may still skip the whole
            # fan-out: no list is read until the placeholder records it met
            continue
        if effective_state(spec, body, exp.from_task) == SUCCESS:
            out.append((task.id, exp.from_task, exp.key))
    return out


def tasks_awaiting_conditions(
    spec: DagSpec, body: dict[str, Any]
) -> list[tuple[str, str]]:
    """The XCom values that ready tasks' ``when:`` comparisons need read.

    Returns ``(task_id, key)`` pairs, each the publishing task and its key,
    so the driver can pre-read the values before the claim RMW, the way
    :func:`tasks_awaiting_expansion` hands it the lists to read. A task
    contributes its pairs once it is pending with no recorded ``whenMet``,
    its trigger rule says ready, no comparison it can decide without a read
    already fails, and every task it reads from is terminal. Until then a
    read could not decide it. A mapped task is its placeholder here.

    Derived from a plain document read, like the expansion list: the claim
    transform evaluates against the fresh body, so a stale snapshot costs a
    wasted read and never a wrong decision.
    """
    out: list[tuple[str, str]] = []
    if not spec.conditional_tasks:
        return out  # no comparison in this DAG reads XCom
    if is_terminal_run(body):
        return out
    tasks = body.get("tasks") or {}
    for task in spec.conditional_tasks:
        entry = tasks.get(task.id)
        if (
            entry is None
            or entry.get("state") != PENDING
            or entry.get("whenMet")
        ):
            continue
        if _deps_verdict(spec, body, task) != "ready":
            continue
        if _when_outcome(spec, body, task, None) is not None:
            continue  # decided without a read
        pairs: list[tuple[str, str]] = []
        for cond in task.when:
            if cond.source != WHEN_XCOM or cond.name not in tasks:
                continue
            if effective_state(spec, body, cond.name) not in TERMINAL_STATES:
                break  # still running: no read decides the task yet
            pairs.append((cond.name, cond.key))
        else:
            out.extend(pair for pair in pairs if pair not in out)
    return out


def when_export(conditions: tuple[Condition, ...]) -> list[dict[str, Any]]:
    """A task's ``when:`` in the shape the configuration writes it.

    The form ``GET /dags`` serves. ``equals`` and ``notEquals`` carry one
    value, and ``in`` and ``notIn`` carry the list.
    """
    out = []
    for cond in conditions:
        entry: dict[str, Any] = {}
        if cond.source == WHEN_PARAM:
            entry["param"] = cond.name
        else:
            entry["xcom"] = {"task": cond.name, "key": cond.key}
        if cond.op in (WHEN_IN, WHEN_NOT_IN):
            entry[cond.op] = list(cond.values)
        else:
            entry[cond.op] = cond.values[0]
        out.append(entry)
    return out


def _same_value(left: Any, right: Any) -> bool:
    """Equality for a comparison: a boolean equals only a boolean, so
    ``true`` never equals ``1``."""
    return (type(left) is bool) == (type(right) is bool) and left == right


#: The characters a ``skipReason`` detail shows escaped: the C0 and C1 control
#: ranges, DEL, and the Unicode line and paragraph separators. An XCom value
#: can hold any of them, and a detail reaches terminals and logs as one line.
_DETAIL_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")


def _one_line(text: str) -> str:
    """``text`` with each control character as its backslash escape."""
    if text.isprintable():
        return text
    return _DETAIL_CONTROL.sub(
        lambda m: m.group().encode("unicode_escape").decode("ascii"), text
    )


def _clip(text: str, limit: int = 80) -> str:
    """``text`` on one line and cut to ``limit`` characters, for a detail."""
    text = _one_line(text)
    return text if len(text) <= limit else text[:limit] + "..."


def _condition_text(cond: Condition) -> str:
    """A comparison as a ``skipReason`` detail names it."""
    if cond.source == WHEN_PARAM:
        source = "param {}".format(cond.name)
    else:
        source = "xcom {}/{}".format(cond.name, _one_line(cond.key))
    values = ", ".join(_clip(env_text(value)) for value in cond.values)
    return "{} {} {}".format(source, cond.op, _clip(values, 240))


def _xcom_source(
    spec: DagSpec,
    body: dict[str, Any],
    cond: Condition,
    conditions: dict[tuple[str, str], Any] | None,
) -> Any:
    """The value an XCom comparison reads: the text, a :class:`NoXcomValue`,
    or ``None`` while the value is not known.

    Not known means the publishing task is not terminal yet, the driver has
    not read the value, or the store could not answer. A task with no entry
    in the run was added by a reload after the run was created and never
    runs in it, so it has published nothing.
    """
    if cond.name not in (body.get("tasks") or {}) or cond.name not in (
        spec.by_id
    ):
        return _XCOM_NOT_IN_RUN
    if effective_state(spec, body, cond.name) not in TERMINAL_STATES:
        return None
    if conditions is None:
        return None
    return conditions.get((cond.name, cond.key))


def _when_outcome(
    spec: DagSpec,
    body: dict[str, Any],
    task: TaskSpec,
    conditions: dict[tuple[str, str], Any] | None,
) -> bool | str | None:
    """Decide ``task``'s ``when:`` comparisons against the run.

    ``True`` when every comparison holds, the ``skipReason`` detail of the
    first one that does not, or ``None`` when none fails and an XCom value
    is not known yet. ``conditions`` maps ``(task, key)`` to what the driver
    read: the text, a :class:`NoXcomValue`, or ``None`` when the store could
    not answer.

    A source with no value fails ``equals`` and ``in`` and passes
    ``notEquals`` and ``notIn``.
    """
    params = body.get("params")
    unknown = False
    for cond in task.when:
        reason: str | None = None
        value: Any = None
        if cond.source == WHEN_PARAM:
            if isinstance(params, dict) and cond.name in params:
                value = params[cond.name]
            else:
                reason = "the run has no such parameter"
        else:
            value = _xcom_source(spec, body, cond, conditions)
            if value is None:
                unknown = True
                continue
            if isinstance(value, NoXcomValue):
                reason = value.reason
        found = reason is None and any(
            _same_value(value, wanted) for wanted in cond.values
        )
        if found != (cond.op in _WHEN_POSITIVE):
            if reason is None:
                reason = "the value is {}".format(_clip(env_text(value)))
            return "{}: {}".format(_condition_text(cond), reason)
    return None if unknown else True


def _decide_when(
    spec: DagSpec,
    body: dict[str, Any],
    task: TaskSpec,
    entry: dict[str, Any],
    now: float,
    result: AdvanceResult,
    conditions: dict[tuple[str, str], Any] | None,
) -> bool:
    """Apply ``task``'s ``when:`` to its pending entry; True when it held.

    A met condition is recorded as ``whenMet`` and never read again, so a
    claim that waits for a later pass, a retry, and a reload all keep the
    decision. An unmet one ends the entry ``skipped``. An unknown one leaves
    the entry pending for a later pass.
    """
    outcome = _when_outcome(spec, body, task, conditions)
    if outcome is None:
        return False
    if outcome is True:
        entry["whenMet"] = True
        entry["updatedAt"] = now
        result.changed = True
        return True
    entry["skipReason"] = {"kind": SKIP_CONDITION, "detail": outcome}
    _terminalise_task(entry, SKIPPED, now, result)
    return False


def _refuse_null_shapes(body: dict[str, Any]) -> None:
    """Refuse ``tasks: {x: null}`` and ``mapped: null`` before any mutation.

    Nothing in this module writes either shape, so both mean a damaged or
    foreign document.  The readers below disagree on what null means
    (absent, pending, or an empty fan-out), so one check up front replaces
    an order-dependent crash or a silent wedge.
    """
    if body.get("mapped", _UNSET) is None:
        raise ValueError("damaged run document: 'mapped' is null")
    tasks = body.get("tasks")
    if tasks:
        for taskkey, entry in tasks.items():
            if entry is None:
                raise ValueError(
                    "damaged run document: task entry {!r} is null".format(
                        taskkey
                    )
                )


def plan_and_claim(
    spec: DagSpec,
    now: float,
    proc: str,
    host: str,
    expansions: dict[str, list[Any] | None],
    conditions: dict[tuple[str, str], Any] | None = None,
):
    """Build the ``mutate_document`` transform that advances one run.

    The returned callable is a pure ``transform(body) -> (new_body, result)``
    for :meth:`StateBackend.mutate_document`.  In one atomic pass it:

    * applies any pre-read ``expansions`` (materialises ``<id>#<i>`` instances,
      or resolves an empty map straight to success);
    * propagates ``upstream_failed`` / ``skipped`` down the graph;
    * decides each ready task's ``when:`` comparisons, reading XCom values
      from the pre-read ``conditions`` (see :func:`_when_outcome`), and
      skips a task whose condition does not hold;
    * claims every ready plain/sensor task ``pending -> running`` (and
      re-claims a failed task whose retry delay has elapsed, and re-pokes a due
      sensor), recording the claim and a :class:`LaunchIntent` in the result;
    * parks a ready approval gate in ``running`` awaiting a decision;
    * terminalises the whole run once every task is terminal.

    ``expansions[task_id] is None`` means "the upstream list could not be read
    right now" -- the task is left for a later pass, never expanded to a guess.

    A read-only quiescence pre-scan (:func:`_is_quiescent`) runs before the
    deep copy: when it can prove nothing below would change the body, the
    transform keeps the document without copying it at all.
    """

    def transform(
        body: dict[str, Any] | None,
    ) -> tuple[Any, AdvanceResult]:
        result = AdvanceResult()
        if body is None or is_terminal_run(body):
            return _DOC_KEEP, result
        if not supports_run(body):
            # reconcile_and_plan reports this on the next pass.
            return _DOC_KEEP, result
        _refuse_null_shapes(body)
        if _is_quiescent(spec, body, now, proc, expansions):
            # the pre-scan proved nothing below can change this body: skip
            # the deep copy (and the rewrite) entirely.  On a large fan-out
            # idling in flight this turns the periodic advance from a full
            # in-lock copy of up to MAX_MAPPED_ITEMS task entries into a
            # plain read.
            return _DOC_KEEP, result
        # deep copy, so the transform stays pure and retryable.  This runs
        # inside the document flock on every advance; the orjson-backed
        # round trip keeps the in-lock copy cost of a large run document
        # (up to MAX_MAPPED_ITEMS task entries) low.
        working = _json.deepcopy_json(body)
        _apply_expansions(spec, working, expansions, now, result)
        _propagate_and_claim(
            spec, working, now, proc, host, result, conditions
        )
        _maybe_terminalise(spec, working, now, result)
        _flag_unread_conditions(spec, working, result)
        if not result.changed:
            return _DOC_KEEP, result
        working["updatedAt"] = now
        return working, result

    return transform


# a private mirror of state.DOC_KEEP so this module needs no state import; the
# driver maps it back.  ``mutate_document`` compares by identity to the real
# sentinel, so the driver substitutes state.DOC_KEEP for this in its wrapper.
class _DocKeep:
    pass


_DOC_KEEP = _DocKeep()


def is_keep(value: Any) -> bool:
    """Whether a transform asked to leave the document untouched."""
    return isinstance(value, _DocKeep)


def _apply_expansions(
    spec: DagSpec,
    body: dict[str, Any],
    expansions: dict[str, list[Any] | None],
    now: float,
    result: AdvanceResult,
) -> None:
    for task_id, items in expansions.items():
        if items is None:
            continue
        task = spec.by_id.get(task_id)
        if task is None or task.expand is None:
            continue
        mapped_all = body.get("mapped")
        if mapped_all and task_id in mapped_all:
            continue  # already expanded (stale pre-read); idempotent
        if task.when and not (body["tasks"].get(task_id) or {}).get("whenMet"):
            continue  # its condition is undecided under this fresh body
        if effective_state(spec, body, task.expand.from_task) != SUCCESS:
            continue  # upstream no longer success under this fresh body
        if len(items) > MAX_MAPPED_ITEMS:
            # an oversized fan-out is a per-task failure, never a run wedge:
            # the placeholder terminalises with a clear reason (downstreams
            # see upstream_failed) instead of materialising the flood.
            placeholder = body["tasks"].get(task_id)
            if placeholder is not None and (
                placeholder.get("state") == PENDING
            ):
                placeholder["failReason"] = (
                    "mapped fan-out of {} items exceeds the cap of {}".format(
                        len(items), MAX_MAPPED_ITEMS
                    )
                )
                _terminalise_task(placeholder, FAILED, now, result)
            continue
        body.setdefault("mapped", {})[task_id] = {
            "items": list(items),
            "expandedAt": now,
        }
        # the placeholder becomes a non-terminal group marker; instances carry
        # the real work.
        placeholder = body["tasks"].get(task_id)
        if placeholder is not None:
            placeholder["state"] = EXPANDED
            placeholder["updatedAt"] = now
        # Materialise the instances from ONE built entry: the rest are shallow
        # copies carrying their own mapIndex/mapItem.  A fan-out can hold
        # MAX_MAPPED_ITEMS instances, and this replaces that many entry builds
        # (each re-deciding the mapped/sensor/approval keys, then popping the
        # placeholder-only "mapped" flag straight back off) with that many
        # dict.copy() calls.  Every template value is an immutable scalar, so
        # the copies share nothing mutable, and copy-then-overwrite leaves the
        # key order the per-instance build produced.
        tasks = body["tasks"]
        prefix = task_id + "#"
        template = _new_task_entry(task, now)
        # task.expand is not None here (checked above), so the flag is always
        # present; an instance is not the group placeholder.
        del template["mapped"]
        for i, item in enumerate(items):
            entry = template.copy()
            entry["mapIndex"] = i
            entry["mapItem"] = item
            tasks[prefix + str(i)] = entry
        result.changed = True


def _instances_of(
    spec: DagSpec, body: dict[str, Any], task: TaskSpec
) -> list[tuple[str, int | None, Any]]:
    """The concrete (taskkey, map_index, item) instances of ``task``.

    A plain task is one instance keyed by its id; a mapped task is its
    materialised ``<id>#<i>`` instances (empty until expansion). The run
    body's recorded fan-out wins over the spec (the mirror of
    :func:`effective_state`): a task that expanded before a reload removed
    its ``expand:`` keeps dispatching its recorded instances, because its
    placeholder is parked EXPANDED and no path could ever advance it.
    """
    # ONE sentinel-defaulted lookup answers both questions the two
    # ``body["mapped"]`` reads used to ask separately ("is it recorded?", then
    # "give me the record"), on a path that runs once per spec task per
    # advance pass.  A recorded-but-null value still means "no instances",
    # exactly as the second read's ``is None`` arm did.
    mapped_all: Any = body.get("mapped")
    mapped: Any = mapped_all.get(task.id, _UNSET) if mapped_all else _UNSET
    if mapped is _UNSET:
        return [] if task.expand is not None else [(task.id, None, None)]
    if mapped is None:
        return []
    items = mapped.get("items", [])
    # task_display_key's mapped branch with its prefix hoisted out of the
    # comprehension: every index here has a real map_index, so the None arm
    # and the call itself are dead weight once per instance.
    prefix = task.id + "#"
    return [(prefix + str(i), i, item) for i, item in enumerate(items)]


def _propagate_and_claim(
    spec: DagSpec,
    body: dict[str, Any],
    now: float,
    proc: str,
    host: str,
    result: AdvanceResult,
    conditions: dict[tuple[str, str], Any] | None = None,
) -> None:
    # Hoisted out of the loops: both were re-resolved once per TASK, and
    # body["tasks"] again once per INSTANCE, so a wide DAG or a large fan-out
    # repeated the same two lookups thousands of times per pass.  Both are
    # safe to bind once: nothing below replaces either dict, it only mutates
    # them (_resolve_missing_instance adds a task entry, and no path here
    # records a fan-out), so the loop still sees every write.
    tasks = body["tasks"]
    mapped_all: Any = body.get("mapped")
    for task in spec.tasks:
        expanded = bool(mapped_all) and task.id in mapped_all
        if task.expand is not None and not expanded:
            # un-expanded mapped placeholder: only propagate an upstream
            # failure/skip to it (readiness -> expansion needs an out-of-band
            # XCom read, applied in _apply_expansions, so leave a ready one
            # pending here for the next pass).
            _propagate_placeholder(spec, body, task, now, result, conditions)
            continue
        if not expanded:
            # A plain task with no recorded fan-out is exactly one instance
            # keyed by its id: :func:`_instances_of`'s single-instance
            # branch, inlined because it is the node every non-mapped DAG is
            # made of and the call built a one-tuple inside a one-list for
            # each of them on every pass.  The mapped arm below still goes
            # through _instances_of, which owns the recorded-fan-out rule.
            entry = tasks.get(task.id)
            if entry is None:
                # deliberately skipped: a task a reload added after the run
                # was created, which the terminaliser skips as well
                continue
            verdict: str | None = None
            if entry.get("state") == PENDING:
                if (
                    result.deferred
                    and not task.depends_on
                    and task.type != APPROVAL
                    and not task.when
                ):
                    # quota spent: a root is always ready, so _advance_task
                    # would return at its deferred check untouched (a root
                    # with a condition still has that to decide)
                    continue
                verdict = _deps_verdict(spec, body, task)
            _advance_task(
                spec,
                body,
                task,
                task.id,
                None,
                None,
                entry,
                now,
                proc,
                host,
                result,
                verdict,
                None,
                conditions,
            )
            continue
        # The deps verdict is a function of the TASK (all map instances
        # share the same upstreams), and nothing this task's own instance
        # loop does can change it (a claim mutates only the instance's
        # entry, and a task cannot depend on itself).  Resolve it once per
        # task instead of once per instance: with N instances over a
        # mapped upstream of M instances that is the difference between
        # O(M) and O(N*M) state reductions per pass.  Computed lazily so
        # a task with no pending instance skips it entirely.
        verdict = None
        # resolved with a "skip" verdict, once per task; each skipped
        # instance records its own copy.
        skip_reason: dict[str, str] | None = None
        for taskkey, map_index, item in _instances_of(spec, body, task):
            entry = tasks.get(taskkey)
            if entry is None:
                if map_index is not None:
                    # A HOLE: the run's own mapped item list records this
                    # index, so the entry should exist. Materialise it as
                    # failed rather than skipping, or the terminaliser's
                    # absent-instance rule (which reads a hole as pending,
                    # holding the run open) would wedge the run forever:
                    # nothing else can create an entry that is not there.
                    _resolve_missing_instance(
                        task, taskkey, map_index, item, body, now, result
                    )
                    continue
                # a plain task with no entry is the deliberate
                # "added by a reload after the run was created" case,
                # skipped here and by the terminaliser alike
                continue
            if verdict is None and entry.get("state") == PENDING:
                verdict = _deps_verdict(spec, body, task)
                if verdict == "skip":
                    skip_reason = _verdict_skip_reason(spec, body, task)
            _advance_task(
                spec,
                body,
                task,
                taskkey,
                map_index,
                item,
                entry,
                now,
                proc,
                host,
                result,
                verdict,
                skip_reason,
                conditions,
            )


def _propagate_placeholder(
    spec, body, task, now, result, conditions=None
) -> None:
    entry = body["tasks"].get(task.id)
    if entry is None:
        return
    if entry.get("state") != PENDING:
        # Not a fresh placeholder.  Terminal (or expanded) is fine -- but a
        # NON-terminal, non-pending entry here is a task a config reload
        # retyped to mapped (gained ``expand:``) while it was mid-flight
        # under its OLD shape: no path can ever advance it again (the
        # mapped dispatch never reaches _advance_task, so an elapsed
        # up_for_retry backoff is never re-claimed; tasks_awaiting_expansion
        # only offers PENDING placeholders; and _maybe_terminalise demands a
        # terminal state) -- the run would hold its lease and defeat the
        # pruner forever.  Resolve it so the run can finish.
        _resolve_stale_placeholder(task, entry, now, result)
        return
    # A mapped task can only fan out once its expand source SUCCEEDS (that is
    # what produces the item list).  If the source is terminal-but-not-success
    # the fan-out can never be built, so resolve the placeholder rather than
    # leaving it pending forever -- this fires regardless of the trigger rule,
    # so an ``all_done`` mapped task (whose deps verdict is "ready", never
    # "fail"/"skip") does not wedge the run when its source fails/skips.
    if task.expand is not None:
        from_task = task.expand.from_task
        mapped_all = body.get("mapped")
        in_run = from_task in body["tasks"] or bool(
            mapped_all and from_task in mapped_all
        )
        if not in_run:
            # The expand source has NO entry in this run document, so it was
            # added (or renamed into existence) by a config reload after the
            # run was created (run creation materialises every then-current
            # task).  It is not part of this run's plan and will never produce
            # an item list, so the fan-out can never be built.  The same
            # "not materialised" rule that _deps_verdict and _maybe_terminalise
            # already apply, which this path was missing: effective_state
            # defaults an absent entry to PENDING, so without this arm the
            # placeholder waits on a task that will never appear.  Nothing
            # then reaches a terminal state, so the dagadvance lease is
            # renewed for the life of the daemon, retention GC can never
            # collect the run, and every advance pass pays a full document
            # deepcopy to change nothing.
            _resolve_unmaterialised_source(task, entry, now, result)
            return
        src = effective_state(spec, body, from_task)
        if src in (FAILED, UPSTREAM_FAILED):
            _terminalise_task(entry, UPSTREAM_FAILED, now, result)
            return
        if src == SKIPPED:
            entry["skipReason"] = {
                "kind": SKIP_UPSTREAM,
                "detail": "upstream skipped: {}".format(from_task),
            }
            _terminalise_task(entry, SKIPPED, now, result)
            return
    verdict = _deps_verdict(spec, body, task)
    if verdict == "fail":
        _terminalise_task(entry, UPSTREAM_FAILED, now, result)
    elif verdict == "skip":
        entry["skipReason"] = _verdict_skip_reason(spec, body, task)
        _terminalise_task(entry, SKIPPED, now, result)
    elif verdict == "ready" and task.when and not entry.get("whenMet"):
        # A mapped task's condition is decided once, here, before any
        # instance exists. Unmet ends the placeholder skipped. Met lets the
        # fan-out expand, which takes the driver's list read and one more
        # pass (see tasks_awaiting_expansion).
        if _decide_when(spec, body, task, entry, now, result, conditions):
            result.again = True


def _flag_unread_conditions(
    spec: DagSpec, body: dict[str, Any], result: AdvanceResult
) -> None:
    """Ask for another pass when a comparison still needs an XCom read.

    A task can become ready inside a pass, after the driver's pre-read: a
    skip or a failure that this pass propagated finished its last upstream.
    Its comparison then has no value to read in this pass, and nothing else
    would wake the run before its idle floor.
    """
    if (
        spec.conditional_tasks
        and not result.run_terminal
        and tasks_awaiting_conditions(spec, body)
    ):
        result.again = True


def _resolve_unmaterialised_source(task, entry, now, result) -> None:
    """Fail a mapped placeholder whose expand source is not in this run.

    Reached from :func:`_propagate_placeholder` when ``expand.fromTask`` names
    a task with no entry in the run document: a config reload renamed the
    source (or added it) after the run was created, and ``validate_graph``
    accepts that because the NEW spec is internally consistent.  The source
    will never run in THIS run, so its XCom item list will never exist and the
    placeholder can never fan out.

    Failed rather than skipped, and with a reason, for the same purpose
    :func:`_resolve_stale_placeholder` fails its case: the task genuinely
    cannot run under this run's plan, an operator wants to see why, and a
    silent skip would let the run report success for work that never
    happened.  Terminalising it lets the run finish, release its lease and be
    pruned; the next run, created wholly under the new spec, expands cleanly.
    """
    entry["failReason"] = (
        "expand source {!r} has no entry in this run: it was added or "
        "renamed by a config reload after the run was created, so its item "
        "list can never exist and this task cannot fan out (the next run "
        "expands normally)".format(task.expand.from_task)
    )
    _terminalise_task(entry, FAILED, now, result)


def _resolve_missing_instance(
    task, taskkey, map_index, item, body, now, result
) -> None:
    """Materialise and fail a mapped instance the run records but lacks.

    The run document's own ``mapped[<task>].items`` records this index, so
    :func:`_apply_expansions` wrote an entry for it and something later
    removed it: a partial backup restore, a hand edit, or a peer on a
    different build.  The entry cannot be recovered, and leaving the hole is
    worse than failing it: :func:`_fold_mapped_instances` reads an absent
    entry as PENDING (the fan-in barrier's rule), so the terminaliser would
    hold the run open forever, renewing its advance lease for the life of the
    daemon and never becoming eligible for retention.

    Failed with a reason, like :func:`_resolve_unmaterialised_source` and
    :func:`_resolve_stale_placeholder`: the instance genuinely cannot run,
    an operator wants to see why, and terminalising lets the run finish,
    release its lease and be pruned.
    """
    entry = _new_task_entry(task, now)
    entry["mapIndex"] = map_index
    entry.pop("mapped", None)
    entry["mapItem"] = item
    body["tasks"][taskkey] = entry
    entry["failReason"] = (
        "this run records mapped index {} for task {!r} but holds no entry "
        "for it: the run document was restored from a partial backup, hand "
        "edited, or written by a foreign build. Failed so the run can "
        "finish (the next run expands normally)".format(map_index, task.id)
    )
    _terminalise_task(entry, FAILED, now, result)
    result.changed = True


def _resolve_stale_placeholder(task, entry, now, result) -> None:
    """Fail an un-expanded mapped task's entry stranded in an OLD shape.

    Reached only from :func:`_propagate_placeholder` for an entry that is
    neither PENDING nor terminal: the task was retyped to mapped across a
    reload while parked ``up_for_retry`` (or similar).  Its recorded state
    belongs to the old shape and cannot be meaningfully resumed under the
    new one -- relaunching it as an unmapped instance would run the NEW
    spec's command without the map item it may now expect -- so it is
    terminalised as FAILED with an explanatory reason, letting the run
    reach a terminal state, release its lease and be pruned; the next run,
    created wholly under the new spec, expands cleanly.

    Two live sub-shapes are deliberately left alone: an entry with a
    ``proc`` token (a genuinely in-flight attempt -- its completion or the
    reconcile pass will move it to terminal or ``up_for_retry``, which the
    next advance resolves here) and a parked approval gate
    (``awaitingApproval`` -- an operator decision can still resolve it).
    """
    state = entry.get("state")
    if state in _INERT_TASK_STATES:
        return
    if state == RUNNING and (
        entry.get("proc") is not None or entry.get("awaitingApproval")
    ):
        return
    entry["failReason"] = (
        "task gained expand: across a config reload while parked "
        "{}; its pre-reload state cannot be resumed under the mapped "
        "shape, so it is failed to let the run finish (the next run "
        "expands normally)".format(state)
    )
    _terminalise_task(entry, FAILED, now, result)


def _advance_task(
    spec,
    body,
    task,
    taskkey,
    map_index,
    item,
    entry,
    now,
    proc,
    host,
    result,
    verdict=None,
    skip_reason=None,
    conditions=None,
) -> None:
    state = entry.get("state")
    if state in _INERT_TASK_STATES:
        return
    if state == RUNNING:
        _advance_running(
            task, taskkey, map_index, item, entry, now, proc, host, result
        )
        return
    if state == UP_FOR_RETRY:
        if float(entry.get("nextRetryAt") or 0.0) <= now and not (
            result.deferred and task.type != APPROVAL
        ):
            _claim_task(
                task, taskkey, map_index, item, entry, now, proc, host, result
            )
        return
    if state != PENDING:
        return
    if verdict is None:
        # defensive: _propagate_and_claim passes the task-level verdict in
        # for every pending instance, so this only fires for a direct call
        verdict = _deps_verdict(spec, body, task)
    if verdict == "wait":
        return
    if verdict == "fail":
        _terminalise_task(entry, UPSTREAM_FAILED, now, result)
        return
    if verdict == "skip":
        entry["skipReason"] = (
            _verdict_skip_reason(spec, body, task)
            if skip_reason is None
            else skip_reason.copy()
        )
        _terminalise_task(entry, SKIPPED, now, result)
        return
    if task.when and map_index is None and not entry.get("whenMet"):
        # Ahead of the quota check below, so a skip never waits for claim
        # quota and a met condition is recorded even when the claim is
        # deferred. An instance of a mapped task has none to decide: its
        # placeholder did (see _propagate_placeholder).
        if not _decide_when(spec, body, task, entry, now, result, conditions):
            return
    if result.deferred and task.type != APPROVAL:
        # Quota spent this pass (only _claims_full sets deferred, and
        # launches never shrink within a pass): _claim_task would return
        # untouched. A gate parks without a launch, so it is never quota-bound.
        return
    _claim_task(task, taskkey, map_index, item, entry, now, proc, host, result)


def _claims_full(result: AdvanceResult) -> bool:
    """Whether this pass used its claim quota (marks the result deferred)."""
    if len(result.launches) < MAX_CLAIMS_PER_PASS:
        return False
    result.deferred = True
    return True


def _advance_running(
    task, taskkey, map_index, item, entry, now, proc, host, result
) -> None:
    # A sensor sits in RUNNING across pokes; when a poke is due and no poke is
    # in flight (proc/pid cleared by its completion), claim the next poke.
    if task.type != SENSOR:
        # RUNNING with nothing in flight is a shape only a SENSOR reaches
        # legitimately.  If the CURRENT spec no longer types this task as a
        # sensor, a config reload retyped it while it idled between pokes and
        # every path now abandons the entry at once: this function returns,
        # _reconcile_entries skips a proc-less entry (a skip justified only
        # for sensors) and _maybe_terminalise sees a non-terminal state -- so
        # the run never terminalises, is never pruned, and holds its
        # dagadvance lease for the life of the daemon, paying a full document
        # deepcopy on every advance to change nothing.  Fail it, exactly as
        # _resolve_stale_placeholder does for the mapped-retype case, so the
        # run can finish; the next run is created wholly under the new spec.
        # An approval gate is left alone -- an operator can still resolve it.
        if (
            entry.get("proc") is None
            and entry.get("pid") is None
            and not entry.get("awaitingApproval")
        ):
            entry["failReason"] = (
                "task was retyped from sensor to {} across a config reload "
                "while idle between pokes; its pre-reload state cannot be "
                "resumed under the new shape, so it is failed to let the run "
                "finish (the next run starts cleanly)".format(task.type)
            )
            _terminalise_task(entry, FAILED, now, result)
        return
    if entry.get("pid") is not None or entry.get("proc") is not None:
        return  # a poke is in flight
    next_poke = entry.get("nextPokeAt")
    if next_poke is not None and next_poke > now:
        return  # not due yet
    if _sensor_timed_out(task, entry, now):
        entry["failReason"] = "sensor timed out"
        _terminalise_task(entry, FAILED, now, result)
        return
    if _claims_full(result):
        return  # this pass's launch quota is spent; re-poke next pass
    poke_number = int(entry.get("pokeCount", 0))
    result.launches.append(
        LaunchIntent(
            task_id=task.id,
            taskkey=taskkey,
            map_index=map_index,
            map_item=item,
            attempt=int(entry.get("attempt", 0)),
            is_sensor=True,
            poke_number=poke_number,
        )
    )
    # take ownership of this poke at claim time (not pid time): a store hiccup
    # setting the pid afterwards then cannot make reconciliation mistake this
    # live poke for a crash (proc == our token protects it).  host is refreshed
    # too so a poke after a cross-host lease handoff records its real host.
    entry["proc"] = proc
    entry["host"] = host
    entry["pid"] = None
    # the in-flight poke owns the schedule now: a stale past due-instant left
    # here would read as a due wake for the poke's whole duration (busy-spin);
    # completion re-sets it (not-yet) or terminalises (success).
    entry["nextPokeAt"] = None
    entry["updatedAt"] = now
    result.changed = True


def _sensor_timed_out(task, entry, now) -> bool:
    started = entry.get("firstPokeAt")
    if started is None:
        return False
    return bool((now - started) >= task.poke_timeout)


def _claim_task(
    task, taskkey, map_index, item, entry, now, proc, host, result
) -> None:
    # approval gates never run a subprocess; park them awaiting a decision.
    if task.type == APPROVAL:
        entry["state"] = RUNNING
        entry["startedAt"] = now
        entry["awaitingApproval"] = True
        entry["updatedAt"] = now
        result.changed = True
        return
    if _claims_full(result):
        return  # launch quota spent; stays claimable for the next pass
    is_sensor = task.type == SENSOR
    entry["state"] = RUNNING
    # take ownership at claim time (its pid is filled in after the subprocess
    # launches): reconciliation trusts a RUNNING task with proc == our token,
    # so a store hiccup on the pid write cannot make it fail a live task, and a
    # launch that never lands is failed explicitly by the driver, not left for
    # reconciliation to guess.  The one claim the driver cannot fail is one
    # whose intents never reached it; see release_lost_claims.
    entry["proc"] = proc
    entry["pid"] = None
    entry.pop("queued", None)
    entry["host"] = host
    entry["startedAt"] = entry.get("startedAt") or now
    entry["updatedAt"] = now
    poke_number = 0
    if is_sensor:
        entry["pokeCount"] = 0
        entry["firstPokeAt"] = now
        entry["nextPokeAt"] = None
    else:
        # a task retyped from a sensor may carry its poke count; the plain
        # claim is poke 0, and the launch registry reads the entry
        # (DagScheduler._repair_lost_claims)
        entry.pop("pokeCount", None)
    result.launches.append(
        LaunchIntent(
            task_id=task.id,
            taskkey=taskkey,
            map_index=map_index,
            map_item=item,
            attempt=int(entry.get("attempt", 0)),
            is_sensor=is_sensor,
            poke_number=poke_number,
        )
    )
    result.changed = True


def _terminalise_task(entry, state, now, result) -> None:
    entry["state"] = state
    entry["finishedAt"] = now
    entry["proc"] = None
    entry["pid"] = None
    entry["updatedAt"] = now
    result.changed = True


def _maybe_terminalise(spec, body, now, result) -> None:
    # the run is terminal once every task is terminal.  An un-expanded mapped
    # task contributes its placeholder state (terminal only if its upstream
    # failed/skipped before it could expand); an expanded mapped task
    # contributes its instances (an empty map contributes nothing and is
    # vacuously done).  A spec task with NO entry in this run document was
    # added to the DAG *after* this run was created (a config reload): it is
    # not part of this run, so it is skipped rather than blocking the run from
    # ever terminalising.
    # One non-terminal state is the whole answer, so return on it rather than
    # collecting every state first: the walk it cuts short is O(spec tasks +
    # mapped instances), and the very first entry of a run still in flight is
    # usually the one that proves it.  ``failed`` accumulates inline for the
    # run-state verdict, which is only consulted once every state is terminal.
    tasks = body["tasks"]
    mapped_all = body.get("mapped")
    failed = False
    for task in spec.tasks:
        # keyed on the RUN's recorded fan-out, not the spec: a task that
        # expanded before a reload removed its `expand:` still contributes
        # its instances, never its placeholder (parked non-terminally in
        # EXPANDED, which would hold the run open forever).
        mapped: Any = mapped_all.get(task.id, _UNSET) if mapped_all else _UNSET
        if mapped is _UNSET:
            entry = tasks.get(task.id)
            if entry is None:
                continue  # task added post-creation: not part of this run
            st = entry.get("state", PENDING)
            if st not in TERMINAL_STATES:
                return  # still pending / awaiting expansion
            if st in FAILURE_STATES:
                failed = True
            continue
        items = mapped.get("items", []) if mapped is not None else []
        # _fold_mapped_instances is the shared absent-entry rule: an entry
        # missing for a run-recorded index reads as pending, so a damaged
        # document holds the run open (matching the fan-in barrier's
        # verdict) instead of completing around the hole.
        # _propagate_and_claim, earlier in this same transform, has already
        # materialised and failed any such hole, so holding open here costs
        # at most the rest of this pass.
        holds, inst_failed, _skipped = _fold_mapped_instances(
            tasks, task.id + "#", len(items)
        )
        if holds:
            return
        if inst_failed:
            failed = True
    run_state = FAILED if failed else SUCCESS
    if body.get("state") != run_state:
        body["state"] = run_state
        result.changed = True
    result.run_terminal = True


# --------------------------------------------------------------------------
# Quiescence pre-scan (a read-only fast path for the claim transforms)
# --------------------------------------------------------------------------

# Per-entry verdicts of the pre-scan.  ACT: this pass could change the entry,
# or the scan cannot prove it will not (any doubt lands here; the only cost
# is running the full pass, which is exactly the pre-scan-less behavior).
# BLOCKED: the entry is provably inert this pass AND its non-terminal state
# is consulted by _maybe_terminalise, so the run provably cannot terminalise
# either.  INERT: the entry is inert but does not hold the run open (a
# terminal instance, or an expanded group placeholder whose materialised
# instances carry the real state).
_Q_ACT = "act"
_Q_BLOCKED = "blocked"
_Q_INERT = "inert"


def _is_quiescent(
    spec: DagSpec,
    body: dict[str, Any],
    now: float,
    proc: str,
    expansions: dict[str, list[Any] | None] | None,
) -> bool:
    """Whether an advance pass over ``body`` provably cannot change it.

    Called by :func:`plan_and_claim` and :func:`reconcile_and_plan` BEFORE
    the deep copy, so a quiescent run (typically: every instance in flight
    under this node's own proc token, nothing due) costs a read-only scan
    instead of a full copy and rewrite of a document that can hold up to
    ``MAX_MAPPED_ITEMS`` task entries.

    The predicate is deliberately one-sided: ``True`` must be airtight (a
    wrong ``True`` would silently skip real work and could wedge a run),
    while a wrong ``False`` merely runs the full pass and rediscovers there
    was nothing to do.  It is derived from what the transform halves can do,
    and returns ``False`` (not quiescent) whenever any of the following
    holds:

    * a pre-read expansion list is usable, or any mapped task is awaiting
      expansion (the driver must also learn ``expansions_needed``, which
      only the full combined pass reports);
    * any entry is pending (claimable now, or resolvable by propagation);
    * any retry's backoff is due at ``now`` (the same ``now`` the transform
      itself uses), or its due instant is unreadable;
    * any idle sensor's next poke is due at ``now``, unscheduled, or
      unreadable;
    * any running entry is claimed under a FOREIGN proc token (the
      reconcile half may recover it; an entry holding OUR token is left
      alone by reconcile and claim alike);
    * any entry is in a state this scan does not recognize, belongs to a
      task no longer in the spec, or cannot be positively matched to a slot
      :func:`_maybe_terminalise` consults;
    * no consulted non-terminal entry exists at all (the run could
      terminalise this pass).
    """
    if expansions and any(items is not None for items in expansions.values()):
        return False
    if tasks_awaiting_expansion(spec, body):
        return False
    blocked = False
    # resolved once for the whole scan rather than once (twice, inside
    # _q_blocked) per entry: this walks every entry of a document that can
    # hold MAX_MAPPED_ITEMS of them, on every advance.
    mapped_all: Any = body.get("mapped")
    for taskkey, entry in (body.get("tasks") or {}).items():
        verdict = _entry_quiescence(
            spec, body, taskkey, entry, now, proc, mapped_all
        )
        if verdict == _Q_ACT:
            return False
        if verdict == _Q_BLOCKED:
            blocked = True
    # With no BLOCKED entry every consulted task is terminal (or there are
    # no tasks at all), so _maybe_terminalise could finish the run: not
    # quiescent, let the full pass decide.
    return blocked


def _entry_quiescence(
    spec: DagSpec,
    body: dict[str, Any],
    taskkey: str,
    entry: dict[str, Any],
    now: float,
    proc: str,
    mapped_all: Any = _UNSET,
) -> str:
    """Classify one task entry for :func:`_is_quiescent` (see the verdicts).

    Mirrors the state dispatch of :func:`_advance_task`,
    :func:`_advance_running` and the reconcile loop, erring to ``_Q_ACT``
    for anything it cannot positively place.

    ``mapped_all`` is the body's recorded fan-out map, hoisted out of the
    scan by :func:`_is_quiescent` so the whole walk resolves it once; omitted
    (a direct caller), it is resolved here, after the terminal-entry fast
    path that never needs it.
    """
    state = entry.get("state")
    if state in TERMINAL_STATES:
        return _Q_INERT
    if mapped_all is _UNSET:
        mapped_all = body.get("mapped")
    task_id = entry.get("id")
    task = spec.by_id.get(task_id) if isinstance(task_id, str) else None
    if task is None:
        # a non-terminal entry of a task the spec no longer has: the claim
        # and terminalise passes ignore it entirely, so it must not hold
        # the short-circuit open (the run can terminalise around it, and a
        # quiescent verdict resting on it would keep the document forever).
        return _Q_ACT
    if state == EXPANDED:
        if task.expand is None and not (mapped_all and task_id in mapped_all):
            # marked expanded with no recorded fan-out to stand in for it:
            # a shape this scan does not recognize; the full pass owns it.
            return _Q_ACT
        # the recorded instances carry the real state (even if the spec
        # stopped mapping the task since: dispatch and terminalisation key
        # on the run's recorded fan-out, and neither consults this entry).
        return _Q_INERT
    if state == UP_FOR_RETRY:
        try:
            due_at = float(entry.get("nextRetryAt") or 0.0)
        except (TypeError, ValueError):
            return _Q_ACT
        if due_at <= now:
            return _Q_ACT  # the backoff has elapsed: claimable this pass
        return _q_blocked(mapped_all, task, taskkey, entry)
    if state == RUNNING:
        if entry.get("awaitingApproval"):
            # a parked approval gate: reconcile skips it, claims skip it,
            # and its non-terminal state pins the run open.
            return _q_blocked(mapped_all, task, taskkey, entry)
        entry_proc = entry.get("proc")
        if entry_proc is not None:
            if entry_proc != proc:
                # a foreign claim: the reconcile half may recover it (its
                # owner may be dead), so the full pass must look at it.
                return _Q_ACT
            # our own live claim: reconcile trusts the proc token, and the
            # claim/poke logic always skips an in-flight instance.
            return _q_blocked(mapped_all, task, taskkey, entry)
        if task.type != SENSOR or entry.get("pid") is not None:
            # a proc-less RUNNING entry is only ever a sensor idling
            # between pokes (see reconcile_crashed); anything else here is
            # a shape this scan does not recognize.
            return _Q_ACT
        next_poke = entry.get("nextPokeAt")
        if next_poke is None:
            return _Q_ACT  # the poke is due immediately
        try:
            if float(next_poke) <= now:
                return _Q_ACT  # the poke is due at this pass's now
        except (TypeError, ValueError):
            return _Q_ACT
        return _q_blocked(mapped_all, task, taskkey, entry)
    if state == PENDING and task.expand is None:
        # A pending PLAIN task whose upstreams are not all terminal is inert
        # this pass exactly like an in-flight one: _advance_task returns on a
        # "wait" verdict without touching it, reconcile skips non-RUNNING
        # entries, and terminalisation only reads it.  This is the shape a
        # chain (or a fan-in sink) spends its whole life in, so without it the
        # pre-scan never fires for any DAG that has a downstream task.  The
        # verdict cannot go stale mid-pass: a quiescent verdict requires NO
        # entry to be _Q_ACT, and every mutation path short-circuits on the
        # BLOCKED/INERT shapes, so no upstream state this reads can change.
        # A mapped placeholder is deliberately excluded: expansion and
        # _propagate_placeholder can both move it while its deps still "wait".
        if _deps_verdict(spec, body, task) == "wait":
            return _q_blocked(mapped_all, task, taskkey, entry)
    # PENDING and claimable/propagatable, or a state this build does not
    # recognize.
    return _Q_ACT


def _q_blocked(
    mapped_all: Any,
    task: TaskSpec,
    taskkey: str,
    entry: dict[str, Any],
) -> str:
    """``_Q_BLOCKED`` when :func:`_maybe_terminalise` provably consults this
    inert entry, else ``_Q_ACT``.

    Being consulted is what makes an inert non-terminal entry hold the run
    open: terminalisation walks the SPEC (a plain task's own key, a mapped
    task's ``id#i`` instances for its recorded item list), so an entry it
    never visits, however inert, cannot stop the run from finishing, and a
    quiescent verdict resting on such an entry could wedge the run.
    Anything that cannot be positively matched to a consulted slot falls
    back to the full pass.

    ``mapped_all`` is the body's recorded fan-out map, resolved once by
    :func:`_is_quiescent` for the whole scan: this ran on nearly every entry
    of the document and read ``body["mapped"]`` twice each time.
    """
    # one sentinel-defaulted lookup for the membership test and the record
    # alike; a non-dict (missing, or damaged) fails the isinstance below and
    # lands on _Q_ACT, exactly as the separate reads did.
    mapped = mapped_all.get(task.id, _UNSET) if mapped_all else _UNSET
    if task.expand is None and mapped is _UNSET:
        return _Q_BLOCKED if taskkey == task.id else _Q_ACT
    items = mapped.get("items") if isinstance(mapped, dict) else None
    map_index = entry.get("mapIndex")
    if (
        isinstance(items, list)
        and isinstance(map_index, int)
        and not isinstance(map_index, bool)
        and 0 <= map_index < len(items)
        and taskkey == task_display_key(task.id, map_index)
    ):
        return _Q_BLOCKED
    return _Q_ACT


# --------------------------------------------------------------------------
# Completion / pid / approval / reconcile transforms
# --------------------------------------------------------------------------


def set_task_pid(
    taskkey: str,
    proc: str,
    pid: int | None,
    now: float,
    *,
    attempt: int | None = None,
):
    """Transform recording the OS pid of a just-launched task instance.

    The instance's ``proc`` token is stamped at CLAIM time (see
    :func:`_claim_task`), so this only fills in the pid.  It FENCES that write
    to the exact claim that launched the subprocess -- the same proc-token /
    attempt identity :func:`mark_task_finished` and :func:`reconcile_crashed`
    fence on.  A superseded former owner (its lease lost, the task since
    reconciled -> re-claimed by another node with a fresh proc token / bumped
    attempt) whose launch loop only now reaches the pid write would otherwise
    overwrite the LIVE claim's proc/pid -- fencing out the real attempt's
    completion and dropping its result.  When the stamped identity no longer
    matches the entry, the write is dropped exactly like a duplicate.
    """

    def transform(body):
        if body is None:
            return _DOC_KEEP, False
        entry = body.get("tasks", {}).get(taskkey)
        if entry is None or entry.get("state") != RUNNING:
            return _DOC_KEEP, False
        if entry.get("proc") != proc:
            # the entry was re-claimed by another owner after we launched: not
            # our instance to stamp a pid on.
            return _DOC_KEEP, False
        if attempt is not None and int(entry.get("attempt", 0)) != attempt:
            # a newer attempt is the live one; this is a stale launch's pid.
            return _DOC_KEEP, False
        entry["pid"] = pid
        entry["updatedAt"] = now
        body["updatedAt"] = now
        return body, True

    return transform


def set_task_pids(
    entries: list[tuple[str, str, int | None, int | None]],
    now: float,
):
    """Transform recording a whole launch batch's OS pids in ONE RMW.

    The batched form of :func:`set_task_pid`: ``entries`` is a list of
    ``(taskkey, proc, pid, attempt)`` tuples, one per just-launched task
    instance, and the whole list is applied by a single read-modify-write.
    An advance pass launches up to ``MAX_CLAIMS_PER_PASS`` instances, and a
    mapped fan-out's document can hold up to ``MAX_MAPPED_ITEMS`` task
    entries, so stamping each pid through its own full-document RMW cost one
    full parse plus rewrite plus fsync PER LAUNCH; one batch write makes it
    one per pass.

    Batching is safe because it changes nothing about the fences: each entry
    is checked independently against the current body with EXACTLY the
    per-entry state, proc-token and attempt fences of :func:`set_task_pid`
    (see its docstring for why a superseded owner's late pid write must be
    dropped).  Every fence reads only its own task's entry and every apply
    writes only its own task's ``pid``/``updatedAt``, so no entry's outcome
    can depend on another's: applying the batch is equivalent to applying
    the single-entry transforms sequentially, and a stale entry (its
    instance re-claimed under a fresh proc token, or a newer attempt now
    live) is dropped on its own while the rest of the batch still lands.
    The result is the number of entries applied; zero keeps the document
    untouched, exactly like a lone fenced-out pid write.
    """

    def transform(body):
        if body is None:
            return _DOC_KEEP, 0
        applied = 0
        tasks = body.get("tasks", {})
        for taskkey, proc, pid, attempt in entries:
            entry = tasks.get(taskkey)
            if entry is None or entry.get("state") != RUNNING:
                continue  # finished/reconciled already: drop like a dupe
            if entry.get("proc") != proc:
                # re-claimed by another owner after this launch: not our
                # instance to stamp a pid on (the set_task_pid fence).
                continue
            if attempt is not None and int(entry.get("attempt", 0)) != (
                attempt
            ):
                # a newer attempt is the live one; this is a stale launch's
                # pid (the set_task_pid fence).
                continue
            entry["pid"] = pid
            entry["updatedAt"] = now
            applied += 1
        if not applied:
            return _DOC_KEEP, 0
        body["updatedAt"] = now
        return body, applied

    return transform


def release_lost_claims(
    spec: DagSpec,
    claims: list[tuple[str, str, int, int]],
    now: float,
):
    """Transform undoing claims whose launch intents the driver never got.

    A claim RMW whose write lands after the awaiting side timed out leaves
    the instance RUNNING under the owner's proc token with nothing launched
    (the intents rode the abandoned result).  Reconcile trusts that token,
    so only the driver, which records what it launched, can tell such an
    entry from a live one (see ``DagScheduler._repair_lost_claims``).
    ``claims`` holds ``(taskkey, proc, attempt, pokeCount)`` per entry, and
    each is undone only under that exact fence: a completion or re-claim
    that landed since is left alone.  A plain task returns to ``pending``
    with its attempt unchanged (nothing ran, so no attempt is spent) and
    ``startedAt`` cleared, so the re-claim stamps the real launch time; a
    sensor takes the idle-between-pokes shape, due now, keeping the
    ``firstPokeAt`` its poke timeout counts from.  The result lists the
    task keys released.
    """

    def transform(body):
        if body is None or is_terminal_run(body):
            return _DOC_KEEP, []
        released = []
        tasks = body.get("tasks", {})
        for taskkey, proc, attempt, poke in claims:
            entry = tasks.get(taskkey)
            if entry is None or entry.get("state") != RUNNING:
                continue
            if entry.get("proc") != proc:
                continue  # re-claimed by another owner: not ours to undo
            if int(entry.get("attempt", 0)) != attempt:
                continue  # a newer attempt is the live one
            if int(entry.get("pokeCount", 0)) != poke:
                continue  # a later poke is the live one
            entry["proc"] = None
            entry["pid"] = None
            task = spec.by_id.get(entry.get("id"))
            if task is not None and task.type == SENSOR:
                entry["nextPokeAt"] = now
            else:
                entry["state"] = PENDING
                entry["startedAt"] = None
            entry["updatedAt"] = now
            released.append(taskkey)
        if not released:
            return _DOC_KEEP, []
        body["updatedAt"] = now
        return body, released

    return transform


def mark_task_finished(
    taskkey: str,
    *,
    success: bool,
    exit_code: int | None,
    fail_reason: str | None,
    now: float,
    task: TaskSpec,
    jitter: float = 0.0,
    expected_proc: str | None = None,
    expected_attempt: int | None = None,
    expected_poke: int | None = None,
    resources: dict[str, Any] | None = None,
    skipped: bool = False,
):
    """Transform moving a finished instance to its terminal (or retry) state.

    A sensor whose poke exited non-zero is rescheduled (``nextPokeAt`` bumped)
    rather than failed, until it succeeds or times out.  A failed plain task
    with retries left is parked ``up_for_retry`` with ``nextRetryAt`` set; the
    next advance re-claims it.  Otherwise the instance is terminal.

    ``skipped`` says the command exited with one of the task's
    ``skipExitCodes`` (decided by ``RunningJob.skipped``). The instance ends
    ``skipped`` whatever ``success`` says, and a plain task uses no attempt.

    ``resources`` is the finished instance's sampled CPU/memory usage as an
    already-serialised dict (``ResourceUsage.to_dict()``), recorded verbatim
    on the entry -- this module stays a pure state machine and never touches
    the sampler.  ``None`` (monitoring off, or nothing captured) leaves the
    entry without stats; each plain-task attempt's completion overwrites the
    previous attempt's value, and a sensor records only its succeeding poke.

    ``expected_proc`` / ``expected_attempt`` FENCE the completion to the exact
    claim that produced it -- the same ``proc``-token identity
    :func:`reconcile_crashed` already fences reconciliation on.  A completion
    from a *superseded* attempt (a partitioned/evicted former owner whose
    subprocess outlived its lease and finished only after another node
    reconciled the task -> bumped its attempt -> re-claimed and re-launched it)
    carries the old proc token / attempt; applying it would terminalise the
    LIVE re-claimed instance with a dead attempt's exit code (double-advance /
    wrong outcome).  When the stamped identity no longer matches the entry, the
    completion is dropped exactly like a duplicate.  ``None`` (the default)
    disables the check, so pre-existing callers and tests are unaffected.

    ``expected_poke`` extends the fence to sensors, whose proc/attempt
    identity does NOT change between pokes: a re-poke claim
    (:func:`_advance_running`) re-stamps the SAME proc token and never bumps
    ``attempt``, only ``pokeCount`` -- so a delayed retry of poke N's
    completion (a mutate that timed out but actually landed) would otherwise
    pass the proc+attempt fence and clear the LIVE poke N+1's proc/pid under
    its running subprocess.  The completion carries the ``pokeCount`` observed
    at its claim; when the entry's current count differs, a later poke is the
    live one and the stale completion is dropped.  ``None`` (plain tasks, and
    pre-existing callers) disables the check.
    """

    def transform(body):
        if body is None:
            return _DOC_KEEP, False
        entry = body.get("tasks", {}).get(taskkey)
        if entry is None or entry.get("state") != RUNNING:
            # already reconciled/terminal: a duplicate completion is a no-op.
            return _DOC_KEEP, False
        if expected_proc is not None and entry.get("proc") != expected_proc:
            # superseded attempt: the entry was re-claimed by another node
            # (proc token bumped) after this instance's owner lost the run.
            return _DOC_KEEP, False
        if (
            expected_attempt is not None
            and int(entry.get("attempt", 0)) != expected_attempt
        ):
            # a newer attempt is the live one; this is a stale completion.
            return _DOC_KEEP, False
        if (
            expected_poke is not None
            and int(entry.get("pokeCount", 0)) != expected_poke
        ):
            # a later poke of the same sensor claim is the live one; this is
            # a stale poke's completion (see the docstring: proc/attempt do
            # not distinguish pokes).
            return _DOC_KEEP, False
        if skipped:
            _finish_skipped(entry, exit_code, now, task, resources)
        elif task.type == SENSOR:
            _finish_sensor(entry, success, now, task, jitter, resources)
        else:
            _finish_plain(
                entry, success, exit_code, fail_reason, now, task, resources
            )
        body["updatedAt"] = now
        return body, True

    return transform


def mark_tasks_finished(marks: list[dict[str, Any]], now: float):
    """Terminalise (or park-for-retry) a whole batch of finished instances in
    ONE RMW.  The batched form of :func:`mark_task_finished`.

    ``marks`` is a list of per-instance dicts, each carrying the same fields
    the single transform takes: ``taskkey``, ``success``, ``exit_code``,
    ``fail_reason``, ``task`` (the :class:`TaskSpec`), ``jitter``, and the
    ``expected_proc`` / ``expected_attempt`` / ``expected_poke`` fences, plus
    an optional serialised ``resources`` dict and an optional ``skipped``
    flag.

    Batching is safe for exactly the reason :func:`set_task_pids` is: every
    mark is fenced and applied against ONLY its own task's entry, so applying
    the batch is equivalent to applying the single-entry transforms in
    sequence.  A stale/duplicate/superseded completion (its instance
    re-claimed under a fresh proc token, a newer attempt or a later poke now
    live) is dropped on its own while the rest of the batch still lands.  A
    reaper flush of a mapped fan-out that used to pay one full-document parse +
    rewrite + fsync PER completion now pays one per run per flush.

    Returns the list of taskkeys actually applied (empty -> document left
    untouched, exactly like a lone fenced-out completion), so the caller can
    settle each applied entry's retry-queue copy and log the dropped ones.
    """

    def transform(body):
        if body is None:
            return _DOC_KEEP, []
        tasks = body.get("tasks", {})
        applied: list[str] = []
        for mark in marks:
            taskkey = mark["taskkey"]
            entry = tasks.get(taskkey)
            if entry is None or entry.get("state") != RUNNING:
                # already reconciled/terminal: a duplicate completion is a
                # no-op (the mark_task_finished state fence).
                continue
            expected_proc = mark.get("expected_proc")
            if (
                expected_proc is not None
                and entry.get("proc") != expected_proc
            ):
                continue  # superseded attempt (the proc-token fence)
            expected_attempt = mark.get("expected_attempt")
            if (
                expected_attempt is not None
                and int(entry.get("attempt", 0)) != expected_attempt
            ):
                continue  # a newer attempt is live (the attempt fence)
            expected_poke = mark.get("expected_poke")
            if (
                expected_poke is not None
                and int(entry.get("pokeCount", 0)) != expected_poke
            ):
                continue  # a later poke is live (the poke fence)
            task = mark["task"]
            if mark.get("skipped"):
                _finish_skipped(
                    entry,
                    mark.get("exit_code"),
                    now,
                    task,
                    mark.get("resources"),
                )
            elif task.type == SENSOR:
                _finish_sensor(
                    entry,
                    mark["success"],
                    now,
                    task,
                    mark.get("jitter", 0.0),
                    mark.get("resources"),
                )
            else:
                _finish_plain(
                    entry,
                    mark["success"],
                    mark.get("exit_code"),
                    mark.get("fail_reason"),
                    now,
                    task,
                    mark.get("resources"),
                )
            if mark.get("verification") is not None:
                entry["verification"] = mark["verification"]
            applied.append(taskkey)
        if not applied:
            return _DOC_KEEP, []
        body["updatedAt"] = now
        return body, applied

    return transform


def _finish_sensor(entry, success, now, task, jitter, resources=None) -> None:
    entry["proc"] = None
    entry["pid"] = None
    entry["pokeCount"] = int(entry.get("pokeCount", 0)) + 1
    if success:
        entry["state"] = SUCCESS
        entry["finishedAt"] = now
        entry["exitCode"] = 0
        if resources is not None:
            # only the succeeding poke's usage; a rescheduled poke keeps the
            # entry as-is (it is still logically one running sensor).
            entry["resources"] = resources
    else:
        # condition not met yet: schedule the next poke, bounded by the poke
        # timeout (enforced at claim time) and spread by the caller's jitter.
        entry["nextPokeAt"] = (
            now + max(0.0, task.poke_interval) + max(0.0, jitter)
        )
    entry["updatedAt"] = now


def _finish_skipped(entry, exit_code, now, task, resources=None) -> None:
    """End an instance ``skipped`` because its command exited with a skip code.

    Terminal for a plain task and a sensor alike. A plain task keeps its
    attempt count, so a skip after failed attempts uses none of the rest.
    """
    entry["proc"] = None
    entry["pid"] = None
    entry["exitCode"] = exit_code
    if resources is not None:
        entry["resources"] = resources
    if task.type == SENSOR:
        entry["pokeCount"] = int(entry.get("pokeCount", 0)) + 1
    entry["state"] = SKIPPED
    entry["finishedAt"] = now
    entry["failReason"] = None
    entry["skipReason"] = {
        "kind": SKIP_EXIT_CODE,
        "detail": "exit code {}".format(exit_code),
    }
    entry["updatedAt"] = now


def _finish_plain(
    entry, success, exit_code, fail_reason, now, task, resources=None
) -> None:
    entry["proc"] = None
    entry["pid"] = None
    entry["exitCode"] = exit_code
    if resources is not None:
        entry["resources"] = resources
    if success:
        entry["state"] = SUCCESS
        entry["finishedAt"] = now
        entry["failReason"] = None
        entry["updatedAt"] = now
        return
    attempt = int(entry.get("attempt", 0))
    if attempt + 1 < task.max_attempts:
        entry["attempt"] = attempt + 1
        entry["state"] = UP_FOR_RETRY
        entry["failReason"] = fail_reason
        entry["nextRetryAt"] = now + max(0.0, task.retry_delay)
    else:
        entry["attempt"] = attempt + 1
        entry["state"] = FAILED
        entry["failReason"] = fail_reason
        entry["finishedAt"] = now
        entry["nextRetryAt"] = None
    entry["updatedAt"] = now


def apply_approval(
    taskkey: str, *, approved: bool, by: str, now: float, on_reject: str
):
    """Transform recording an approval-gate decision from the API.

    Approve -> the gate succeeds and the graph proceeds; reject -> it fails
    (or, when the gate's ``onReject`` is ``skip``, it is skipped, cascading a
    ``skipped`` to its ``all_success`` downstream, with a ``skipReason``
    naming who rejected it).
    """

    def transform(body):
        if body is None:
            return _DOC_KEEP, {"ok": False, "reason": "no such run"}
        entry = body.get("tasks", {}).get(taskkey)
        if entry is None:
            return _DOC_KEEP, {"ok": False, "reason": "no such task"}
        if entry.get("state") != RUNNING or not entry.get("awaitingApproval"):
            return _DOC_KEEP, {
                "ok": False,
                "reason": "task is not awaiting approval",
            }
        entry.pop("awaitingApproval", None)
        entry["approval"] = {
            "decision": "approved" if approved else "rejected",
            "by": by,
            "at": now,
        }
        entry["finishedAt"] = now
        if approved:
            entry["state"] = SUCCESS
        elif on_reject == SKIPPED:
            entry["state"] = SKIPPED
            entry["skipReason"] = {
                "kind": SKIP_APPROVAL,
                "detail": "rejected by {}".format(by),
            }
        else:
            entry["state"] = FAILED
        entry["updatedAt"] = now
        body["updatedAt"] = now
        return body, {"ok": True, "state": entry["state"]}

    return transform


def reconcile_crashed(
    spec: DagSpec, now: float, proc: str, host: str, is_pid_alive
):
    """Transform that recovers tasks a crash left ``running`` with a dead proc.

    Called on rehydration, whenever a fresh dag_run lease is won, and at the
    top of every advance (the same seam the job reconciler uses).  A
    ``running`` instance is left alone when it belongs to *this* live process
    (``proc ==
    our token``) or to a live child on this host (``pid_alive``); otherwise its
    owner is gone.  A plain task is then treated as an interrupted attempt --
    retried if attempts remain, else failed.  A crashed sensor *poke* is
    cleared so the next advance re-pokes it.  Skipped, and thus untouched: an
    approval gate awaiting a decision, and a ``running`` task with no ``proc``
    -- which is only ever a sensor *between* pokes, legitimately idle until its
    ``nextPokeAt`` (a claim always persists the owning proc, so a proc-less
    RUNNING plain task cannot arise), so reconciling it would defeat the poke
    schedule.
    """

    def transform(body):
        if body is None or is_terminal_run(body):
            return _DOC_KEEP, 0
        if not supports_run(body):
            return _DOC_KEEP, 0
        changed = _reconcile_entries(spec, body, now, proc, host, is_pid_alive)
        if changed:
            body["updatedAt"] = now
            return body, changed
        return _DOC_KEEP, 0

    return transform


def _reconcile_entries(
    spec: DagSpec,
    body: dict[str, Any],
    now: float,
    proc: str,
    host: str,
    is_pid_alive,
) -> int:
    """The reconcile loop over ``body``'s task entries, mutating in place.

    Shared by :func:`reconcile_crashed` and :func:`reconcile_and_plan` so
    the two apply the identical recovery rules and fences; returns how many
    entries were recovered (zero means nothing was touched).
    """
    changed = 0
    for entry in body.get("tasks", {}).values():
        if entry.get("state") != RUNNING:
            continue
        if entry.get("awaitingApproval"):
            continue  # gate: nothing to reconcile
        if entry.get("proc") is None:
            continue  # a sensor idling between pokes: not a crash victim
        if _has_live_process(entry, proc, host, is_pid_alive):
            continue
        task = spec.by_id.get(entry.get("id"))
        _reconcile_one(entry, task, now)
        changed += 1
    return changed


def reconcile_and_plan(
    spec: DagSpec, now: float, proc: str, host: str, is_pid_alive
):
    """Build the combined reconcile+claim transform for one advance pass.

    The driver used to pay two full read-modify-writes per advance, one for
    :func:`reconcile_crashed` and one for :func:`plan_and_claim`, even on a
    completely quiescent run (and an owned run re-advances at least once a
    minute, plus after every task completion).  This transform composes the
    two into a single RMW for the common case.  The composition is safe
    because both halves are pure functions of the same ``(body, now)``:
    applying reconcile then claim inside one lock is observably identical to
    running the old two RMWs back-to-back with no interleaved writer, which
    is a schedule the two-RMW flow already had to be correct under (the
    per-run lease and lock make interleaving rare, never impossible, and
    every proc-token/attempt fence is evaluated here against the very body
    it mutates).

    The one thing the claim half cannot do inside a transform is read XCom:
    the expansion lists live outside the document, and a transform must stay
    pure and free of I/O.  So after reconciling, the transform checks the
    RECONCILED body for mapped tasks awaiting expansion.  When there are
    none (the overwhelmingly common case) it continues straight into the
    propagate/claim/terminalise logic with an empty expansion set, and the
    whole advance is one RMW.  When some are awaiting, it applies only the
    reconcile half, flags ``expansions_needed`` on the result, and the
    driver pre-reads the lists from the returned body and runs the existing
    :func:`plan_and_claim` RMW as a second step, exactly the old shape.

    The same read-only quiescence pre-scan as :func:`plan_and_claim` runs
    first; its foreign-proc-token rule covers the reconcile half (an entry
    holding OUR token is trusted alive and never reconciled, so a quiescent
    body is one the reconcile loop would not touch either).
    """

    def transform(
        body: dict[str, Any] | None,
    ) -> tuple[Any, ReconcileAdvanceResult]:
        advance = AdvanceResult()
        result = ReconcileAdvanceResult(advance=advance)
        if body is None or is_terminal_run(body):
            return _DOC_KEEP, result
        if not supports_run(body):
            result.unsupported = True
            return _DOC_KEEP, result
        _refuse_null_shapes(body)
        if _is_quiescent(spec, body, now, proc, None):
            return _DOC_KEEP, result
        # deep copy for the same reason plan_and_claim does: the transform
        # must stay pure and retryable.
        working = _json.deepcopy_json(body)
        result.reconciled = _reconcile_entries(
            spec, working, now, proc, host, is_pid_alive
        )
        result.expansions_needed = bool(
            tasks_awaiting_expansion(spec, working)
        )
        result.conditions_needed = bool(
            tasks_awaiting_conditions(spec, working)
        )
        if result.expansions_needed or result.conditions_needed:
            # expansion and an XCom comparison both need out-of-band XCom
            # reads the driver must do between the halves: persist only the
            # reconcile half and hand the decision back (advance=None says
            # the claim half did NOT run, so the driver never mistakes this
            # for an empty claim).
            result.advance = None
            if result.reconciled:
                working["updatedAt"] = now
                return working, result
            return _DOC_KEEP, result
        _propagate_and_claim(spec, working, now, proc, host, advance)
        _maybe_terminalise(spec, working, now, advance)
        _flag_unread_conditions(spec, working, advance)
        if not advance.changed and not result.reconciled:
            return _DOC_KEEP, result
        working["updatedAt"] = now
        return working, result

    return transform


def _has_live_process(entry, proc, host, is_pid_alive) -> bool:
    ep = entry.get("proc")
    if ep is None:
        # claimed but never recorded a pid (a crash between the claim RMW and
        # the pid RMW): its owner is gone unless it is THIS process still
        # mid-launch, which the caller's per-run lock rules out at reconcile
        # time -- so treat it as not-live.
        return False
    if ep == proc:
        return True
    pid = entry.get("pid")
    return bool(
        entry.get("host") == host
        and isinstance(pid, int)
        and not isinstance(pid, bool)
        and is_pid_alive(pid)
    )


def _reconcile_one(entry, task, now) -> None:
    entry["proc"] = None
    entry["pid"] = None
    if task is not None and task.type == SENSOR:
        # clear the interrupted poke and let the advance re-poke it now.
        entry["nextPokeAt"] = now
        entry["updatedAt"] = now
        return
    max_attempts = task.max_attempts if task is not None else 1
    attempt = int(entry.get("attempt", 0))
    if attempt + 1 < max_attempts:
        entry["attempt"] = attempt + 1
        entry["state"] = UP_FOR_RETRY
        entry["failReason"] = "reconciled-crash"
        entry["nextRetryAt"] = now
    else:
        entry["attempt"] = attempt + 1
        entry["state"] = FAILED
        entry["failReason"] = "reconciled-crash"
        entry["finishedAt"] = now
    entry["updatedAt"] = now
