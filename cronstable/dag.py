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
from typing import Any, Optional

from cronstable import _json

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


# --------------------------------------------------------------------------
# Static DAG specification (built by config.py, consumed here)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ExpandSpec:
    """A dynamic-mapping directive: fan out over an upstream's XCom list."""

    from_task: str
    key: str


@dataclass(frozen=True)
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
    expand: Optional[ExpandSpec] = None
    # sensor poke schedule (bounded, jittered, durable)
    poke_interval: float = 30.0
    poke_timeout: float = 3600.0
    poke_jitter: float = 0.0
    # approval gate: what a rejected gate does to the graph
    on_reject: str = FAILED  # FAILED or SKIPPED


@dataclass(frozen=True)
class DagSpec:
    """A whole DAG, normalised: ordered tasks, an id index, the mapped subset.

    ``mapped_tasks`` is the ``expand``-carrying subset, precomputed here
    because :func:`tasks_awaiting_expansion` runs on every advance and the
    overwhelmingly common DAG has none: the check becomes O(1) instead of a
    full spec walk per pass.
    """

    name: str
    tasks: tuple[TaskSpec, ...]
    by_id: dict[str, TaskSpec] = field(default_factory=dict)
    mapped_tasks: tuple[TaskSpec, ...] = ()

    @staticmethod
    def build(name: str, tasks: list[TaskSpec]) -> "DagSpec":
        return DagSpec(
            name=name,
            tasks=tuple(tasks),
            by_id={t.id: t for t in tasks},
            mapped_tasks=tuple(t for t in tasks if t.expand is not None),
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
    that mapped tasks are plain ``task`` nodes, and that the dependency graph
    is acyclic (a cycle would never advance).  Called from config parsing so a
    bad DAG is a :class:`~cronstable.config.ConfigError` at load, not a runtime
    hang.
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


def task_display_key(task_id: str, map_index: Optional[int]) -> str:
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
    logical_date: Optional[str],
    kind: str,
    now: float,
    spec: DagSpec,
) -> dict[str, Any]:
    """The initial ``dag_run`` document: every task pending, run running.

    Mapped tasks start as a single ``pending`` placeholder carrying
    ``mapped: true``; they materialise into ``<id>#<i>`` instances once their
    upstream produces the item list (see :func:`plan_and_claim`).
    """
    tasks = {task.id: _new_task_entry(task, now) for task in spec.tasks}
    return {
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


# --------------------------------------------------------------------------
# Launch intents returned by the claim transform
# --------------------------------------------------------------------------


@dataclass
class LaunchIntent:
    """One task instance the driver should now start a subprocess for."""

    task_id: str
    taskkey: str
    map_index: Optional[int]
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
    # the claim half's result when it ran inside this same RMW.
    advance: Optional[AdvanceResult] = None


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
    an upstream failed (``all_success``); ``skip`` -- an upstream was skipped
    (``all_success``).  ``all_done`` only ever returns ready or wait.
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
    if task.trigger_rule == ALL_DONE:
        return "ready"
    # all_success
    if failed:
        return "fail"
    if skipped:
        return "skip"
    return "ready"


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
        if effective_state(spec, body, exp.from_task) == SUCCESS:
            out.append((task.id, exp.from_task, exp.key))
    return out


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
                    "damaged run document: task entry {!r} is "
                    "null".format(taskkey)
                )


def plan_and_claim(
    spec: DagSpec,
    now: float,
    proc: str,
    host: str,
    expansions: dict[str, Optional[list[Any]]],
):
    """Build the ``mutate_document`` transform that advances one run.

    The returned callable is a pure ``transform(body) -> (new_body, result)``
    for :meth:`StateBackend.mutate_document`.  In one atomic pass it:

    * applies any pre-read ``expansions`` (materialises ``<id>#<i>`` instances,
      or resolves an empty map straight to success);
    * propagates ``upstream_failed`` / ``skipped`` down the graph;
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
        body: Optional[dict[str, Any]],
    ) -> tuple[Any, AdvanceResult]:
        result = AdvanceResult()
        if body is None or is_terminal_run(body):
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
        _propagate_and_claim(spec, working, now, proc, host, result)
        _maybe_terminalise(spec, working, now, result)
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
    expansions: dict[str, Optional[list[Any]]],
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
) -> list[tuple[str, Optional[int], Any]]:
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
            _propagate_placeholder(spec, body, task, now, result)
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
            verdict: Optional[str] = None
            if entry.get("state") == PENDING:
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
            )


def _propagate_placeholder(spec, body, task, now, result) -> None:
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
            _terminalise_task(entry, SKIPPED, now, result)
            return
    verdict = _deps_verdict(spec, body, task)
    if verdict == "fail":
        _terminalise_task(entry, UPSTREAM_FAILED, now, result)
    elif verdict == "skip":
        _terminalise_task(entry, SKIPPED, now, result)


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
        if float(entry.get("nextRetryAt") or 0.0) <= now:
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
        _terminalise_task(entry, SKIPPED, now, result)
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
    # reconciliation to guess.
    entry["proc"] = proc
    entry["pid"] = None
    entry["host"] = host
    entry["startedAt"] = entry.get("startedAt") or now
    entry["updatedAt"] = now
    poke_number = 0
    if is_sensor:
        entry["pokeCount"] = 0
        entry["firstPokeAt"] = now
        entry["nextPokeAt"] = None
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
# is running the full pass, which is exactly the pre-scan-less behaviour).
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
    expansions: Optional[dict[str, Optional[list[Any]]]],
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
    * any entry is in a state this scan does not recognise, belongs to a
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
            # a shape this scan does not recognise; the full pass owns it.
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
            # a shape this scan does not recognise.
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
    # recognise.
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
    pid: Optional[int],
    now: float,
    *,
    attempt: Optional[int] = None,
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
    entries: list[tuple[str, str, Optional[int], Optional[int]]],
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


def mark_task_finished(
    taskkey: str,
    *,
    success: bool,
    exit_code: Optional[int],
    fail_reason: Optional[str],
    now: float,
    task: TaskSpec,
    jitter: float = 0.0,
    expected_proc: Optional[str] = None,
    expected_attempt: Optional[int] = None,
    expected_poke: Optional[int] = None,
    resources: Optional[dict[str, Any]] = None,
):
    """Transform moving a finished instance to its terminal (or retry) state.

    A sensor whose poke exited non-zero is rescheduled (``nextPokeAt`` bumped)
    rather than failed, until it succeeds or times out.  A failed plain task
    with retries left is parked ``up_for_retry`` with ``nextRetryAt`` set; the
    next advance re-claims it.  Otherwise the instance is terminal.

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
        if task.type == SENSOR:
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
    an optional serialised ``resources`` dict.

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
            if task.type == SENSOR:
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
    ``skipped`` to its ``all_success`` downstream).
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
        else:
            entry["state"] = SKIPPED if on_reject == SKIPPED else FAILED
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
        body: Optional[dict[str, Any]],
    ) -> tuple[Any, ReconcileAdvanceResult]:
        advance = AdvanceResult()
        result = ReconcileAdvanceResult(advance=advance)
        if body is None or is_terminal_run(body):
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
        if tasks_awaiting_expansion(spec, working):
            # expansion needs out-of-band XCom reads the driver must do
            # between the halves: persist only the reconcile half and hand
            # the decision back (advance=None says the claim half did NOT
            # run, so the driver never mistakes this for an empty claim).
            result.expansions_needed = True
            result.advance = None
            if result.reconciled:
                working["updatedAt"] = now
                return working, result
            return _DOC_KEEP, result
        _propagate_and_claim(spec, working, now, proc, host, advance)
        _maybe_terminalise(spec, working, now, advance)
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
