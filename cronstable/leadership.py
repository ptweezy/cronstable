"""Pluggable leadership backends behind one interface.

The scheduler (:mod:`cronstable.cron`) only ever asks "am I allowed to
run this job?" through the methods on whatever object ``cluster.backend``
selected: the seam defined here as :class:`LeadershipBackend`, built by
:func:`make_backend`.

Four backends share the seam: **gossip** (default), the mTLS,
no-shared-state, best-effort quorum election in :mod:`cronstable.cluster`;
and three fenced leases in :mod:`cronstable.backends`: **kubernetes** (a
``coordination.k8s.io/v1`` ``Lease``), **etcd** (a lease-backed
key/election), and **filesystem** (a flock-guarded TTL lease on a shared
POSIX mount, fenced under NTP-bounded clock skew).  kubernetes/etcd talk
plain HTTP via the core ``aiohttp`` dependency, not the official client
libraries: zero new deps, no grpc/protobuf wheels to shrink the
architecture coverage.

The surface is split three ways so a new lease backend stays tiny:

* **core abstract**: ``start``, ``stop``, ``is_leader``, ``leader_name``,
  ``is_quorate``, ``view_dict``.  Every backend implements these.
* **defaulted**: bodies a single-holder lease backend inherits unchanged;
  gossip overrides every one.  :class:`LeaseBackend` replaces only the
  ``@reboot`` "already ran" pair, persisting the ran-set in the store
  (:data:`REBOOT_RAN_KEY`, job-set scoped) so a failover holder does not
  re-run a one-shot.
* **never-skip (PreferLeader)**: the ``available_*`` family, defaulted to
  the locked lease decision: a node that cannot reach the store runs a
  ``PreferLeader`` job anyway (may double-run); ``Leader`` fails closed.
"""

import abc
import json
from collections.abc import Callable
from typing import Any, Optional

from cronstable.config import ClusterConfig, ConfigError

#: annotation/key under which a lease backend persists the @reboot
#: one-shots already run, so a failover holder does not re-run them.
REBOOT_RAN_KEY = "cronstable.io/reboot-ran"


def encode_reboot_ran(job_set_id: str, jobs: set[str]) -> str:
    """Encode the @reboot-ran set + its job-set fingerprint for the store."""
    return json.dumps(
        {"jobSetId": job_set_id, "jobs": sorted(jobs)},
        separators=(",", ":"),
    )


def decode_reboot_ran(raw: Optional[str]) -> tuple[Optional[str], set[str]]:
    """Decode a stored @reboot-ran blob to ``(job_set_id, jobs)``.

    Tolerant of any malformed/absent value (returns ``(None, set())``): a
    backend must never crash its renew loop on junk written by others.
    """
    if not raw:
        return None, set()
    try:
        data = json.loads(raw)
    except (ValueError, TypeError, RecursionError):
        # CPython's json decoder raises RecursionError (NOT a ValueError)
        # on deeply nested input; mirrors cluster.py's guards.
        return None, set()
    if not isinstance(data, dict):
        return None, set()
    job_set_id = data.get("jobSetId")
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        jobs = []
    return (
        job_set_id if isinstance(job_set_id, str) else None,
        {j for j in jobs if isinstance(j, str)},
    )


class RebootRanUnknownError(RuntimeError):
    """The @reboot-ran answer is not yet safe to give.

    Raised by a lease backend's ``reboot_ran`` between GAINING leadership
    and the first re-read of the persisted ran-set: until then a ``False``
    may just mean "not read back yet", causing the failover double-fire
    the set exists to prevent.  The one consumer,
    ``cron._process_pending_reboots``, treats any raise as "not known to
    have run" and keeps the one-shot PENDING for the next wakeup: delaying
    an ``@reboot`` is acceptable, re-running one is not.  Used by the
    filesystem and etcd backends; kubernetes needs no gate, its ran-set
    rides the Lease read that wins leadership.
    """


class LeadershipBackend(abc.ABC):
    """The seam each leader-gating call in :mod:`cronstable.cron` goes through.

    Subclasses set the three attributes (``config``, ``node_name``,
    ``distribution``) and implement the core abstract methods; the rest
    default to single-holder lease behaviour, which gossip overrides.
    """

    #: the resolved cluster config block this backend was built from
    config: ClusterConfig
    #: a stable, human-readable identity for this node (defaults to hostname)
    node_name: str
    #: "single-leader" or "spread"; lease backends are always single-leader
    distribution: str

    # --- core: every backend implements these ----------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """Begin maintaining leadership (launch renew loops, listeners)."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop and release leadership, best-effort, for fast failover."""

    @abc.abstractmethod
    def is_leader(self) -> bool:
        """Whether this node currently holds leadership (quorum-gated)."""

    @abc.abstractmethod
    def leader_name(self) -> Optional[str]:
        """The current leader as this node sees it, or ``None`` if unknown."""

    @abc.abstractmethod
    def is_quorate(self) -> bool:
        """Whether this node currently has a trustworthy view of leadership.

        Gossip: the quorum gate; lease: a fresh successful read of the
        store.  When false, ``Leader`` jobs fail closed and the never-skip
        ``available_*`` defaults let ``PreferLeader`` jobs run anyway.
        """

    @abc.abstractmethod
    def view_dict(self) -> dict[str, Any]:
        """The cluster view for ``GET /cluster`` and the dashboard.

        Always carries a ``"backend"`` key naming the active backend.
        """

    # --- defaulted: a single-holder lease backend inherits these unchanged;
    #     gossip (ClusterManager) overrides every one ----------------------

    def is_job_owner(self, job_name: str) -> bool:
        """Per-job ownership collapses to leadership for a single holder."""
        return self.is_leader()

    def job_owner(self, job_name: str) -> Optional[str]:
        """Per-job owner collapses to the single leader."""
        return self.leader_name()

    def has_conflict(self) -> bool:
        """A lease store is authoritative: no split-identity gate applies."""
        return False

    def conflict_names(self) -> list[str]:
        return []

    def conflicting_sizes(self) -> list[int]:
        return []

    def conflicting_policies(self) -> list[str]:
        """Coordination-policy divergences among peers (gossip only)."""
        return []

    def cluster_size(self) -> int:
        """A lease backend is logically a single holder (size 1, quorum 1)."""
        return 1

    def quorum(self) -> int:
        return 1

    def reboot_ran(self, job_name: str) -> bool:
        """No cross-node ``@reboot`` gossip: the holder runs the one-shot."""
        return False

    async def mark_reboot_ran(self, job_name: str) -> None:  # noqa: B027
        """No-op: there is no peer set to gossip the run to."""

    def tls_files_changed(self) -> bool:
        """Lease backends are not restarted on an mTLS cert rotation."""
        return False

    def view_settled(self) -> bool:
        """Whether a ``False`` from the never-skip ``available_*`` gates
        positively identifies another node as the owner.

        Gossip holds those gates closed while a freshly built manager's
        view is still converging (see
        :meth:`cronstable.cluster.ClusterManager.view_settled`).  During
        that hold a ``False`` is a transient fail-closed denial, not an
        observed ownership move, so
        :meth:`cronstable.cron.Cron._cluster_owner_moved` must defer a
        pending retry, not abandon it (abandonment would end an ``@reboot``
        keep-alive sequence cluster-wide).  Lease backends have no
        convergence window: default ``True``; gossip overrides it.
        """
        return True

    def tls_files_loadable(self) -> bool:
        """Whether the current on-disk TLS material can be loaded right now.

        A bind-free dry-run used by
        :meth:`cronstable.cron.Cron.start_stop_cluster` BEFORE it tears the
        manager down for a cert rotation, so a half-written cert cannot
        leave the node with no manager (wedging ``Leader`` /
        ``PreferLeader`` jobs closed for up to one reload).  Lease backends
        have no per-node mTLS material: default ``True``; gossip overrides.
        """
        return True

    def set_job_summaries_provider(  # noqa: B027
        self, provider: Callable[[], dict[str, Any]]
    ) -> None:
        """Install the scheduler's per-job run-summary snapshot callable.

        The provider returns ``{job_name: summary}`` for this node's own
        jobs (see :meth:`cronstable.cron.Cron.fleet_job_summaries`).
        Gossip overrides this to piggyback the snapshot on its ``/peer``
        response; a lease backend has no node-to-node channel, so the
        default is a no-op.
        """

    def set_node_stats_provider(  # noqa: B027
        self,
        provider: Callable[[], Optional[dict[str, Any]]],
        share: bool = True,
    ) -> None:
        """Install the scheduler's whole-node CPU/memory snapshot callable.

        The provider returns this node's live load (see
        :meth:`cronstable.resources.NodeResourceSampler.snapshot`).  Gossip
        overrides this: the snapshot feeds its ``/cluster`` / ``/fleet``
        self readouts, ``share`` gates gossiping it to peers.  Default
        no-op: a lease backend has no node-to-node channel (a lease cluster
        shares node load via ``cluster.observability`` instead).
        """

    def fleet_view(self) -> Optional[dict[str, Any]]:
        """The merged per-node job-summary view for ``GET /fleet``.

        ``None`` means unavailable (a lease backend knows only the holder);
        the endpoint then reports the feature unavailable so the dashboard
        hides its fleet view.  Gossip overrides it.
        """
        return None

    # --- never-skip PreferLeader defaults (the locked lease semantics) ----

    def _is_self_demoted_holder(self) -> bool:
        """Whether this node is the lease holder in its self-demotion window.

        A holder stops calling itself :meth:`is_leader` a clock-skew margin
        BEFORE its lease expires server-side, and only learns of a takeover
        on its next renew round.  In that window it is still
        :meth:`is_quorate` and still observes ITSELF as holder, a state a
        genuine follower never reaches.  Returns ``True`` only then,
        decided from authoritative self-state (never a display-name
        compare), so :meth:`is_available_leader` keeps running
        ``PreferLeader`` work there.  Default ``False``; the lease backends
        override it (gossip has no such window).
        """
        return False

    def available_leader_name(self) -> str:
        """The ``PreferLeader`` owner's *display* name, never ``None``.

        Display only (``GET /cluster`` / the dashboard); the run/skip
        decision is :meth:`is_available_leader`.  Because this can coincide
        with another node's ``node_name`` (a duplicate ``nodeName``), it
        must **not** be string-compared against ``node_name`` to gate a
        run; see :meth:`is_available_leader`.
        """
        if not self.is_quorate():
            return self.node_name
        if self.is_leader():
            return self.node_name
        return self.leader_name() or self.node_name

    def is_available_leader(self) -> bool:
        """Whether this node should run a ``PreferLeader`` job this cycle.

        ``True`` exactly when: the store is unreachable (never-skip: run
        anyway, may double-run); this node holds the lease; or the holder
        is genuinely unknown (a lost ``create`` race), so the job still
        runs somewhere.  Otherwise this node defers.

        Decided from authoritative self-state, deliberately **not** a
        string-compare of :meth:`available_leader_name` against
        ``node_name``: display identities can collide (a duplicate
        ``nodeName`` is the default for a Kubernetes Deployment), which
        would make a quorate *follower* run every ``PreferLeader`` job,
        silently, on every replica.  The fence itself is per-process unique
        (a lease id / ``#<token>`` suffix), so :meth:`is_leader`
        self-recognises regardless of any display-name collision.
        """
        if not self.is_quorate():
            return True
        if self.is_leader():
            return True
        # The former holder in its self-demotion window is still quorate
        # and still names ITSELF holder; treat it as the never-skip owner
        # so the job is not dropped to at-most-zero on every node
        # (followers still defer to the old holder, so nobody else runs).
        if self._is_self_demoted_holder():
            return True
        return self.leader_name() is None

    def available_job_owner(self, job_name: str) -> str:
        """``available_leader_name`` for the per-job (spread) shape.

        Lease backends reject ``spread`` at config time, so this mirrors
        the single-leader path, recognising this node as owner via
        :meth:`is_job_owner` so an identity differing from ``node_name``
        cannot make the owner skip its own job.
        """
        if not self.is_quorate():
            return self.node_name
        if self.is_job_owner(job_name):
            return self.node_name
        return self.job_owner(job_name) or self.node_name

    def is_available_job_owner(self, job_name: str) -> bool:
        """Per-job (spread) analogue of :meth:`is_available_leader`.

        Same authoritative-self-state rule, for the same duplicate-identity
        reason; for lease backends per-job ownership collapses to
        leadership.
        """
        if not self.is_quorate():
            return True
        if self.is_job_owner(job_name):
            return True
        return self.job_owner(job_name) is None


class LeaseBackend(LeadershipBackend):
    """Shared base for the single-holder lease backends (kubernetes, etcd,
    filesystem).

    Pins ``distribution`` to ``"single-leader"`` (lease backends reject
    ``spread``) and provides the lease-shaped :meth:`view_dict`.
    Subclasses implement :meth:`start`, :meth:`stop`, the three live-state
    reads (:meth:`is_leader`, :meth:`leader_name`, :meth:`is_quorate`),
    and :meth:`lease_detail`.
    """

    #: backend name surfaced in view_dict (subclasses set this)
    backend_name: str = "lease"

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        self.node_name = config["nodeName"]
        # spread is rejected at config time, so per-job ownership never
        # diverges from leadership.
        self.distribution = "single-leader"
        # @reboot ran-set, persisted in the store scoped to the job-set id
        # so a FAILOVER holder does not re-run a deferred one-shot.
        # ``_reboot_ran`` is what we read back from the store;
        # ``_reboot_ran_local`` is what this node ran but has not confirmed
        # persisted; reboot_ran checks the union.  Unlike gossip, an
        # @reboot Leader job runs once per job CONFIG on a lease cluster
        # (persisted across restarts), not once per process boot.
        self._reboot_ran: set[str] = set()
        # job-set id ``_reboot_ran`` was last observed under; the READ path
        # gates on it so a reload that changes the job-set id WITHOUT
        # rebuilding this backend cannot let a stale store-read suppress
        # the genuinely-new one-shot.  None until the first observe.
        self._reboot_ran_job_set_id: Optional[str] = None
        self._reboot_ran_local: set[str] = set()
        # job-set id the ``_reboot_ran_local`` marks were recorded under
        # (see _reconcile_local_reboot_ran).  None until the first use.
        self._reboot_ran_local_job_set_id: Optional[str] = None

    def _reconcile_local_reboot_ran(self, current: str) -> None:
        """Drop our own @reboot-ran marks when the job set changed.

        A reload that redefines an @reboot job changes the job-set id, and
        that job must run again: a mark made under an older config must not
        survive, let alone be re-persisted stamped with the new id (which
        would suppress the new one-shot cluster-wide).  Mirrors gossip's
        :meth:`cronstable.cluster.ClusterManager._reconcile_job_set_id`;
        the store-read set is reconciled by :meth:`_observe_reboot_ran`.

        ``current``: the live job-set id.
        """
        if self._reboot_ran_local_job_set_id != current:
            self._reboot_ran_local = set()
            self._reboot_ran_local_job_set_id = current

    def reboot_ran(self, job_name: str) -> bool:
        current = self.get_job_set_id()
        self._reconcile_local_reboot_ran(current)
        self._reconcile_observed_reboot_ran(current)
        return (
            job_name in self._reboot_ran or job_name in self._reboot_ran_local
        )

    def _reconcile_observed_reboot_ran(self, current: str) -> None:
        """Drop the store-read @reboot-ran set when the live job set changed.

        A reload that redefines an @reboot job changes the job-set id but
        leaves the cluster section unchanged, so
        :meth:`cronstable.cron.Cron.start_stop_cluster` reuses this backend
        and the next renew round (which re-observes) may not have run yet;
        reading the stale set would make :meth:`reboot_ran` silently drop
        the redefined one-shot.  Gate on the live id here (mirrors gossip's
        ``advertised_ran_jobs`` read-path guard); the next observe
        re-populates under the new id.

        ``current``: the live job-set id.
        """
        if self._reboot_ran_job_set_id != current:
            self._reboot_ran = set()
            self._reboot_ran_job_set_id = None

    async def mark_reboot_ran(self, job_name: str) -> None:
        self._reconcile_local_reboot_ran(self.get_job_set_id())
        self._reboot_ran_local.add(job_name)
        await self._persist_reboot_ran()

    async def _persist_reboot_ran(self) -> None:  # pragma: no cover
        """Eagerly persist the local @reboot-ran set to the store.

        Both shipping lease backends override it to persist BEFORE the
        deferred @reboot job launches (cron records-then-spawns), so a
        failover holder does not re-run the one-shot.  The no-op default is
        for a backend with no eager-write path (it then persists on its
        next periodic round).
        """

    def _observe_reboot_ran(
        self, stored_job_set_id: Optional[str], stored: set[str]
    ) -> None:
        """Fold a store-read @reboot-ran set into the cache, job-set-scoped.

        A set tagged with a DIFFERENT job-set id belongs to an older config
        and is ignored (the @reboot job runs again), matching gossip's
        scoping of ``advertised_ran``.  Records the id observed under so
        :meth:`_reconcile_observed_reboot_ran` can drop it if the live job
        set changes before the next observe.
        """
        current = self.get_job_set_id()
        if stored_job_set_id == current:
            self._reboot_ran = set(stored)
        else:
            self._reboot_ran = set()
        self._reboot_ran_job_set_id = current

    def reboot_ran_annotation(
        self, existing: Optional[dict[str, str]] = None
    ) -> Optional[dict[str, str]]:
        """The annotations to write back: carry ``existing`` forward, set ours.

        Returns ``None`` when there is nothing to write and nothing to
        preserve, so a backend can skip an empty annotations block.  Used
        by the kubernetes backend's Lease write.
        """
        current = self.get_job_set_id()
        self._reconcile_local_reboot_ran(current)
        annotations = dict(existing) if existing else {}
        if self._reboot_ran or self._reboot_ran_local:
            annotations[REBOOT_RAN_KEY] = encode_reboot_ran(
                current, self._reboot_ran | self._reboot_ran_local
            )
        return annotations or None

    def lease_detail(self) -> dict[str, Any]:
        """Backend-specific ``"lease"`` block for :meth:`view_dict`.

        Default is empty; subclasses surface holder/expiry/name details.
        """
        return {}

    def view_dict(self) -> dict[str, Any]:
        leader = self.leader_name()
        return {
            "backend": self.backend_name,
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": True,
            "distribution": self.distribution,
            # a lease store is authoritative: no gossip-style conflicts.
            "conflict": False,
            "conflict_names": [],
            "size_conflict": False,
            "conflicting_sizes": [],
            "policy_conflict": False,
            "conflicting_policies": [],
            "quorate": self.is_quorate(),
            "leader": leader,
            "is_leader": self.is_leader(),
            # no static peer set; the lease store is the source of truth.
            "peers": [],
            "lease": self.lease_detail(),
        }


def make_backend(
    cluster_config: ClusterConfig,
    get_job_set_id: Callable[[], str],
) -> LeadershipBackend:
    """Build the leadership backend named by ``cluster.backend``.

    Imports are deferred so the lease backends never enter the import
    graph for the common gossip case.  The schema only admits the four
    names, so the final ``raise`` is a defensive backstop.
    """
    backend = cluster_config.get("backend", "gossip")
    if backend == "gossip":
        from cronstable.cluster import ClusterManager

        return ClusterManager(cluster_config, get_job_set_id)
    if backend == "kubernetes":
        from cronstable.backends.kubernetes import KubernetesBackend

        return KubernetesBackend(cluster_config, get_job_set_id)
    if backend == "etcd":
        from cronstable.backends.etcd import EtcdBackend

        return EtcdBackend(cluster_config, get_job_set_id)
    if backend == "filesystem":
        from cronstable.backends.filesystem import FilesystemBackend

        return FilesystemBackend(cluster_config, get_job_set_id)
    raise ConfigError(  # pragma: no cover - unreachable; schema-validated
        "unknown cluster.backend {!r}".format(backend)
    )
