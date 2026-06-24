"""Peer attestation: confirm a static set of peers run the same job set.

Each instance is configured with a list of peer ``host:port`` addresses and a
mutual-TLS identity (a cluster CA plus this node's certificate/key).  It serves
a tiny ``GET /peer`` endpoint on a dedicated mTLS listener, and periodically
polls every configured peer's ``/peer`` over mTLS to compare job-set ids (see
:mod:`yacron2.fingerprint`).

The trust model is deliberately simple and keeps no shared state:

* **mTLS is the membership boundary.**  A peer's certificate must chain to the
  configured cluster CA, and (client side) match the host we connected to, so
  only nodes the CA vouches for are ever attested.  Standard TLS hostname
  verification gives us that SAN pinning for free.
* **Each node keeps its own view.**  ``ClusterView`` is just this node's table
  of what it last observed per peer; two healthy nodes converge to the same
  picture, and any disagreement is itself the signal.  Nobody is authoritative.
* **Identities must be distinct.**  The election's safety rests on every node
  having a unique ``nodeName``; two nodes sharing one would *both* elect
  themselves (each is the ``min`` of its own live set) and double-run.  Each
  process therefore mints a random ``instance_id`` at startup and reports it
  alongside its name, so a node can tell a benign self-listing (same name *and*
  instance id) from a genuine duplicate (same name, *different* instance).
  Each /peer response also carries the responder's own observations, so a node
  detects a duplicate *transitively* -- even when it cannot reach both copies
  directly, two peers that each see one copy let it union the two instance ids
  for that name.  A detected duplicate is reported as ``conflict`` and makes
  the quorum-gated leader gate fail closed (see
  :meth:`ClusterManager.has_conflict`), so a misconfiguration pauses ``Leader``
  jobs rather than silently double-running them.
* **Drift is debounced.**  A reachable peer whose id differs is only reported
  as ``drifted`` after ``driftAfter`` consecutive rounds, so a rolling deploy
  (a transient, legitimate mismatch) does not raise a false alarm.

When ``electLeader`` is set, the same attestation drives a **quorum-gated
leader election** (see :func:`elect_leader`): each node independently elects,
as leader, the lowest ``nodeName`` among the *agreeing* members it can see, but
only if that set is a strict majority (a *quorum*) of the configured cluster.
Only the leader runs scheduled jobs, so replicas deployed from one config no
longer double-run.  Agreement is counted *mutually*: a peer joins the live set
only when both directions are confirmed -- we see it agreeing on the job-set id
*and* its /peer response shows it sees us agreeing too (matched by our unique
``instance_id``; see :meth:`ClusterManager._agreeing_peer_names`).  That, plus
the quorum gate, is what makes this safe with no shared state: two strict
majorities of N cannot be disjoint, and the mutual requirement means a one-way
link cannot let two nodes each count the other and both reach a majority, so at
most one leader exists -- under a clean partition *or* asymmetric reachability.
The trade-off is liveness: a minority partition (or a node reachable in only
one direction) deliberately goes idle rather than risk a second leader, mutual
agreement costs one extra poll round to converge, and because the view is only
as fresh as the last poll, the guarantee remains best-effort across membership
changes (a brief window after a leader dies may skip a firing).  It is *not* a
fenced, exactly-once guarantee; for that you would need a lease/consensus
store, which this design intentionally avoids.

When ``distribution`` is ``"spread"`` the single elected leader is replaced by
**per-job ownership** via rendezvous (highest-random-weight) hashing (see
:func:`elect_job_owner`): each job is independently assigned to one member
of the quorate set, so leader-gated work fans out roughly evenly across the
cluster instead of piling onto one node.  This is purely a load optimization:
it keeps
the same quorum gate and therefore the same safety guarantee (under a clean
partition all quorate nodes see the same member set and compute the same owner
for each job, so still at most one node runs it).
"""

import asyncio
import datetime
import hashlib
import json
import logging
import ssl
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import aiohttp
from aiohttp import web

from yacron2.config import ClusterConfig
from yacron2.fingerprint import SCHEME_VERSION

logger = logging.getLogger("yacron2.cluster")

# Per-peer status, as reported in the /cluster view.
STATUS_UNKNOWN = "unknown"  # not yet contacted
STATUS_SELF = "self"  # the peer reported our own node name AND instance id
STATUS_AGREED = "agreed"  # reachable, same job-set id
STATUS_SYNCING = "syncing"  # reachable, id differs but within driftAfter
STATUS_DRIFTED = "drifted"  # reachable, id has differed >= driftAfter rounds
STATUS_UNREACHABLE = "unreachable"  # connect/timeout failure
STATUS_UNTRUSTED = "untrusted"  # TLS/cert verification failed
# A *different* running instance is announcing our own nodeName: a duplicate
# nodeName, which breaks the election's distinct-identity assumption. Never
# counts toward agreement, and makes the leader gate fail closed (see
# ClusterManager.has_conflict / yacron2.cron._cluster_allows).
STATUS_CONFLICT = "conflict"

# Statuses for which we hold no fresh observation of the peer's identity this
# round, so the peer is ignored when detecting nodeName collisions.
_STALE_STATUSES = frozenset(
    {STATUS_UNKNOWN, STATUS_UNREACHABLE, STATUS_UNTRUSTED}
)

# Cap on the peer /peer response we will buffer per poll. The legitimate
# payload is a small JSON object (a fixed header plus one short member entry
# per node), so this is generous for clusters into the hundreds of nodes while
# bounding the memory a misbehaving-but-CA-trusted peer can force us to
# allocate each round (see _read_capped / _poll_peer).
MAX_PEER_RESPONSE_BYTES = 256 * 1024
_READ_CHUNK = 8192


def _parse_members(raw: Any) -> List["tuple[str, str, bool]"]:
    """Validate a peer's reported ``members`` list, dropping malformed entries.

    A peer is CA-vouched but otherwise untrusted input, so anything that is not
    a list of ``{node_name: str, instance_id: str, agreed: bool}`` objects is
    ignored: a malformed or hostile payload degrades to "no mutual/transitive
    information" rather than poisoning the election (see the type checks in
    :meth:`ClusterManager._poll_peer`).
    """
    members: List["tuple[str, str, bool]"] = []
    if not isinstance(raw, list):
        return members
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("node_name")
        instance = entry.get("instance_id")
        agreed = entry.get("agreed")
        if (
            isinstance(name, str)
            and isinstance(instance, str)
            and isinstance(agreed, bool)
        ):
            members.append((name, instance, agreed))
    return members


def _parse_str_list(raw: Any) -> "set[str]":
    """Validate an untrusted JSON value as a set of strings, dropping the rest.

    Used for the gossiped ``ran_reboot_jobs`` set; like _parse_members, hostile
    or malformed input degrades to an empty set rather than raising.
    """
    if not isinstance(raw, list):
        return set()
    return {item for item in raw if isinstance(item, str)}


def _peer_sees_me_agreed(
    peer_members: Optional[List["tuple[str, str, bool]"]],
    my_instance: str,
) -> bool:
    """Whether a peer's reported member list shows *us* — matched by our unique
    per-process ``instance_id`` — as one of the nodes it currently sees AGREED.

    This is the receiver half of the mutual-attestation gate: we count a peer
    toward quorum only when it confirms it sees us agreeing too (see
    :meth:`ClusterManager._agreeing_peer_names`).
    """
    if not peer_members:
        return False
    for _name, instance, agreed in peer_members:
        if agreed and instance == my_instance:
            return True
    return False


async def _read_capped(resp: Any, limit: int) -> "tuple[bytes, bool]":
    """Read a response body, refusing to buffer more than ``limit`` bytes.

    Returns ``(body, too_large)``.  Iterating (rather than ``resp.read()`` /
    ``resp.json()``, which buffer the whole body unconditionally) bounds memory
    even when the peer streams a huge or chunked response, and because aiohttp
    decompresses as we read, it also caps the *decompressed* size (a gzip-bomb
    guard).
    """
    chunks: List[bytes] = []
    total = 0
    async for chunk in resp.content.iter_chunked(_READ_CHUNK):
        total += len(chunk)
        if total > limit:
            return b"", True
        chunks.append(chunk)
    return b"".join(chunks), False


def quorum_size(cluster_size: int) -> int:
    """The strict majority of ``cluster_size`` nodes.

    A quorum requires *more than half* the cluster, so no two quorums can be
    disjoint; that is the property the leader gate relies on for safety.  Note
    this favours odd cluster sizes: N=3 and N=4 both need 3 and both tolerate
    only one failure, so the even node buys nothing.
    """
    return cluster_size // 2 + 1


def elect_leader(
    node_name: str,
    agreeing_peer_names: Iterable[str],
    cluster_size: int,
) -> Optional[str]:
    """Pure, deterministic leader election from one node's point of view.

    The *live set* is this node plus every peer it currently sees agreeing on
    the job-set id.  If that set is at least a quorum of ``cluster_size`` the
    leader is its lowest ``nodeName`` (so every node in one quorum elects the
    same single leader); otherwise there is no leader and ``None`` is returned,
    which is how a minority partition is made to stand down.
    """
    live = [node_name, *agreeing_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    return min(live)


def elect_available_leader(
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Leaderless election *without* the quorum gate (favours liveness).

    Returns the lowest ``nodeName`` among this node and the peers it sees
    agreeing — and, since this node is always in that set, it always returns a
    name (never ``None``).  Dropping the quorum requirement means a node
    isolated from the rest still elects itself and runs, so a job never skips
    while any node is up; the price is that two sides of a partition may each
    elect their own leader and double-run.  Used by the ``PreferLeader`` job
    policy; contrast :func:`elect_leader`.
    """
    return min([node_name, *agreeing_peer_names])


def _hrw_score(job_name: str, node_name: str) -> int:
    """Rendezvous (highest-random-weight) score for one (job, node) pair.

    A stable hash of ``job_name`` + ``node_name``: deterministic across nodes
    and processes (so every node computes the same scores), and well-mixed, so
    different jobs favour different nodes.  Only the *ordering* of scores
    matters, not their magnitude.
    """
    digest = hashlib.sha256(
        job_name.encode("utf-8") + b"\x00" + node_name.encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big")


def _hrw_owner(job_name: str, members: List[str]) -> str:
    """The rendezvous winner for ``job_name`` among ``members``.

    The member with the highest score owns the job; ties (astronomically
    unlikely with a 64-bit score) break on the node name so the choice stays
    deterministic.  This is what spreads jobs ~evenly and, crucially, only
    reassigns a leaving/joining node's *own* share on a membership change
    (the defining property of rendezvous hashing) rather than reshuffling
    everything the way ``hash % N`` would.
    """
    return max(members, key=lambda n: (_hrw_score(job_name, n), n))


def elect_job_owner(
    job_name: str,
    node_name: str,
    agreeing_peer_names: Iterable[str],
    cluster_size: int,
) -> Optional[str]:
    """Quorum-gated per-job owner (the ``distribution: spread`` analogue of
    :func:`elect_leader`).

    The live set is this node plus the peers it sees agreeing.  If that set is
    at least a quorum of ``cluster_size`` the owner is its rendezvous winner
    for ``job_name`` (so every node in one quorum picks the same owner); else
    ``None`` is returned, which is how a minority partition is made to stand
    down, exactly as in :func:`elect_leader`, just per job.
    """
    live = [node_name, *agreeing_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    return _hrw_owner(job_name, live)


def elect_available_job_owner(
    job_name: str,
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Per-job owner *without* the quorum gate (spread-mode ``PreferLeader``).

    The rendezvous winner among this node and the peers it sees agreeing.  As
    with :func:`elect_available_leader`, this node is always a candidate, so a
    value is always returned (never ``None``): an isolated node owns all its
    jobs and never skips, at the cost of a possible double-run on partition.
    """
    return _hrw_owner(job_name, [node_name, *agreeing_peer_names])


@dataclass
class PeerState:
    """This node's last observation of one configured peer."""

    host: str
    status: str = STATUS_UNKNOWN
    job_set_id: Optional[str] = None  # peer's last-reported id
    node_name: Optional[str] = None  # peer's last-reported node name
    # peer's last-reported per-process instance id, used to distinguish a
    # benign self-listing from a duplicate nodeName (see record_success).
    # Deliberately not surfaced in to_dict (it is an internal liveness token).
    instance_id: Optional[str] = None
    last_seen: Optional[datetime.datetime] = None  # last successful contact
    last_error: Optional[str] = None
    # consecutive reachable-but-mismatched rounds, for the drift hysteresis
    mismatch_streak: int = 0
    # the peer's own reported observations (node_name, instance_id, agreed)
    # from its last /peer response, feeding mutual-agreement and transitive
    # conflict detection (see ClusterManager._agreeing_peer_names /
    # conflict_names). None when we hold no fresh response. Internal, like
    # instance_id, so deliberately not surfaced in to_dict.
    members: Optional[List["tuple[str, str, bool]"]] = None
    # the @reboot job names the peer reports as already run in the cluster
    # (its own runs plus what it learned), used to retire our matching deferred
    # one-shots without re-running them (see ClusterManager.reboot_ran). Only
    # trusted from an AGREED peer (same job-set id). None when no fresh result.
    ran_reboot_jobs: Optional["set[str]"] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "status": self.status,
            "job_set_id": self.job_set_id,
            "node_name": self.node_name,
            "last_seen": (
                self.last_seen.isoformat()
                if self.last_seen is not None
                else None
            ),
            "last_error": self.last_error,
            "mismatch_streak": self.mismatch_streak,
        }


class ClusterView:
    """This node's peer table and the rules that update it.

    Pure (no I/O): the networking layer feeds it observations and reads back
    the table, which keeps the drift/state logic trivially testable.
    """

    def __init__(self, hosts: List[str], drift_after: int) -> None:
        self.drift_after = drift_after
        # preserve configured order for a stable view
        self.peers: "Dict[str, PeerState]" = {
            host: PeerState(host=host) for host in hosts
        }

    def record_success(
        self,
        host: str,
        peer_name: Optional[str],
        peer_id: Optional[str],
        peer_scheme: Optional[str],
        my_id: str,
        now: datetime.datetime,
        my_name: str,
        peer_instance: Optional[str] = None,
        my_instance: Optional[str] = None,
        peer_members: Optional[List["tuple[str, str, bool]"]] = None,
        peer_ran_reboot_jobs: Optional["set[str]"] = None,
    ) -> None:
        peer = self.peers[host]
        peer.last_seen = now
        peer.last_error = None
        peer.job_set_id = peer_id
        peer.node_name = peer_name
        peer.instance_id = peer_instance
        peer.members = peer_members
        peer.ran_reboot_jobs = peer_ran_reboot_jobs

        if peer_name is not None and peer_name == my_name:
            if peer_instance is not None and peer_instance != my_instance:
                # A *different* running instance is announcing our own
                # nodeName. That is a duplicate nodeName, which silently breaks
                # the election's core assumption (distinct identities -> a
                # single leader). Surface it as a hard conflict instead of
                # masking it as 'self'; the leader gate then fails closed.
                peer.status = STATUS_CONFLICT
                peer.mismatch_streak = 0
                peer.last_error = (
                    "duplicate nodeName {!r}: peer is a different "
                    "instance".format(peer_name)
                )
                return
            # Same name *and* same instance id (the operator listed this node's
            # own address), or a peer too old to report an instance id: the
            # benign self case. Never counts toward agreement.
            peer.status = STATUS_SELF
            peer.mismatch_streak = 0
            return

        if peer_scheme is not None and peer_scheme != SCHEME_VERSION:
            # different fingerprint scheme: the ids are not comparable, so this
            # is a (non-debounced) disagreement rather than transient skew.
            peer.status = STATUS_DRIFTED
            peer.last_error = (
                "fingerprint scheme mismatch: {!r} != {!r}".format(
                    peer_scheme, SCHEME_VERSION
                )
            )
            return

        if peer_id == my_id:
            peer.status = STATUS_AGREED
            peer.mismatch_streak = 0
        else:
            # debounce: a mismatch is "syncing" until it persists, so a rolling
            # deploy does not immediately read as drift.
            peer.mismatch_streak += 1
            peer.status = (
                STATUS_DRIFTED
                if peer.mismatch_streak >= self.drift_after
                else STATUS_SYNCING
            )

    def record_failure(
        self, host: str, error: str, *, untrusted: bool
    ) -> None:
        peer = self.peers[host]
        peer.last_error = error
        peer.status = STATUS_UNTRUSTED if untrusted else STATUS_UNREACHABLE
        # we could not observe the id this round, so the drift streak (which
        # only counts *reachable* mismatches) is reset and the peer's last
        # reported view is dropped as stale (no mutual/conflict info this time)
        peer.mismatch_streak = 0
        peer.members = None
        peer.ran_reboot_jobs = None

    def to_list(self) -> List[Dict[str, Any]]:
        return [peer.to_dict() for peer in self.peers.values()]

    def local_members(
        self, my_name: str, my_instance: str
    ) -> List[Dict[str, Any]]:
        """This node's current observations, for the /peer response body.

        Lists this node (always agreeing with itself) plus every peer we hold a
        fresh observation of, each tagged with whether we currently see it
        AGREED.  A polling peer uses this two ways: to confirm *mutual*
        agreement (does this list carry the poller, agreed?) and to detect a
        duplicate nodeName transitively (does any name appear with two distinct
        instance ids once everyone's lists are unioned?).
        """
        members: List[Dict[str, Any]] = [
            {
                "node_name": my_name,
                "instance_id": my_instance,
                "agreed": True,
            }
        ]
        for peer in self.peers.values():
            if peer.status in _STALE_STATUSES or peer.node_name is None:
                continue
            members.append(
                {
                    "node_name": peer.node_name,
                    "instance_id": peer.instance_id,
                    "agreed": peer.status == STATUS_AGREED,
                }
            )
        return members


def build_client_ssl_context(tls: Dict[str, str]) -> ssl.SSLContext:
    """Client context: verify peer certs vs the CA, pin the hostname."""
    ctx = ssl.create_default_context(cafile=tls["ca"])
    ctx.load_cert_chain(tls["cert"], tls["key"])
    # create_default_context already sets check_hostname=True and
    # verify_mode=CERT_REQUIRED for the client purpose; be explicit anyway.
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def build_server_ssl_context(tls: Dict[str, str]) -> ssl.SSLContext:
    """Server context: require and verify a CA-signed client cert (mTLS)."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=tls["ca"])
    ctx.load_cert_chain(tls["cert"], tls["key"])
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def _split_host_port(addr: str) -> "tuple[str, int]":
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError("expected host:port, got {!r}".format(addr))
    return host, int(port)


class ClusterManager:
    """Owns the mTLS ``/peer`` listener and the periodic peer-poll loop."""

    def __init__(
        self,
        config: ClusterConfig,
        get_job_set_id: Callable[[], str],
    ) -> None:
        self.config = config
        self.get_job_set_id = get_job_set_id
        self.node_name: str = config["nodeName"]
        # A random per-process identity, reported alongside node_name so peers
        # can tell a benign self-listing from a duplicate nodeName (a different
        # process claiming the same name); see ClusterView.record_success and
        # has_conflict. Changes every restart, which is fine: it only ever
        # distinguishes "is this the same running process as me".
        self.instance_id: str = uuid.uuid4().hex
        # "single-leader" (one leader runs all Leader jobs) or "spread"
        # (per-job ownership via rendezvous hashing); see _cluster_allows.
        self.distribution: str = config.get("distribution", "single-leader")
        self.view = ClusterView(
            [peer["host"] for peer in config["peers"]],
            config["driftAfter"],
        )
        self._client_ssl = build_client_ssl_context(config["tls"])
        self._server_ssl = build_server_ssl_context(config["tls"])
        self._runner: Optional[web.AppRunner] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # @reboot one-shots THIS node has run as the elected owner (plus any it
        # learned ran via push) -- gossiped so peers retire their matching
        # deferred jobs without re-running them on failover. Scoped to the
        # current job-set: cleared when our job_set_id changes (see _poll_all),
        # so a config change cannot carry a stale "already ran" across it.
        self._ran_reboot_jobs: Set[str] = set()
        self._ran_jobs_job_set_id: Optional[str] = None

    # --- the mTLS /peer server -------------------------------------------

    async def _handle_peer(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "node_name": self.node_name,
                "job_set_id": self.get_job_set_id(),
                "scheme_version": SCHEME_VERSION,
                "instance_id": self.instance_id,
                # our current observations, so a polling peer can confirm we
                # see it too (mutual agreement) and spot a duplicate nodeName
                # transitively; see ClusterView.local_members.
                "members": self.view.local_members(
                    self.node_name, self.instance_id
                ),
                # @reboot one-shots already run in the cluster (ours + learned
                # from agreed peers), so a poller can retire its matching
                # deferred job without re-running it; see advertised_ran_jobs.
                "ran_reboot_jobs": sorted(self.advertised_ran_jobs()),
            }
        )

    async def _handle_reboot_ran(self, request: web.Request) -> web.Response:
        """Receive an eager push of @reboot jobs a peer just ran.

        The pull-poll already carries this set, but a push shrinks the window
        in which an owner could run a one-shot and then die before any peer
        polled it (so a new leader would re-run it).  Best-effort: we accept it
        only when the sender's job_set_id matches ours (an agreed peer, same
        config), and any malformed body is ignored.

        Trust scope: the never-re-run guarantee holds against benign failures
        (crashes, partitions).  A CA-vouched but *hostile* peer could push a
        fabricated "ran X" to make others retire a job that never ran -- the
        same Byzantine class as a member lying about its job_set_id to skew the
        election, which this design already does not defend against.
        """
        raw, too_large = await _read_capped(request, MAX_PEER_RESPONSE_BYTES)
        if too_large:
            return web.Response(status=413)
        try:
            data = json.loads(raw)
        except ValueError:
            return web.Response(status=400)
        if (
            isinstance(data, dict)
            and data.get("job_set_id") == self.get_job_set_id()
        ):
            self._ran_reboot_jobs |= _parse_str_list(data.get("names"))
        return web.Response(status=204)

    async def start(self) -> None:
        app = web.Application()
        app.add_routes(
            [
                web.get("/peer", self._handle_peer),
                web.post("/reboot-ran", self._handle_reboot_ran),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            host, port = _split_host_port(self.config["listen"])
            site = web.TCPSite(
                runner, host, port, ssl_context=self._server_ssl
            )
            await site.start()
        except BaseException:
            # bad listen address (ValueError) or bind failure (OSError, e.g.
            # the port is already in use) after the runner was set up -- and
            # cancellation -- must not leak the half-started runner.
            await runner.cleanup()
            raise
        self._runner = runner
        logger.info(
            "cluster: node %r serving mTLS /peer on %s, polling %d peer(s) "
            "every %ds",
            self.node_name,
            self.config["listen"],
            len(self.config["peers"]),
            self.config["interval"],
        )
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    # --- the peer-poll loop ----------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception("cluster: unexpected error in poll loop")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), self.config["interval"]
                )
            except asyncio.TimeoutError:
                pass

    async def _poll_all(self) -> None:
        my_id = self.get_job_set_id()
        if (
            self._ran_jobs_job_set_id is not None
            and my_id != self._ran_jobs_job_set_id
        ):
            # our job set CHANGED (config reload): runs recorded under the old
            # set no longer apply to the new one, so forget them. A still-
            # deferred @reboot may then run again -- the safe direction; we
            # never silently skip a job whose definition changed. (The first
            # observation just establishes the id; it must not clear, or a push
            # that arrived before the first poll would be wiped.)
            self._ran_reboot_jobs.clear()
        self._ran_jobs_job_set_id = my_id
        timeout = aiohttp.ClientTimeout(total=self.config["connectTimeout"])
        peers = self.config["peers"]
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # return_exceptions so one peer raising an *unexpected* error (a
            # bug, not a network failure -- those are handled inside
            # _poll_peer) cannot abort the whole round and leave the other
            # peers' coroutines detached. Surface such errors, don't swallow.
            results = await asyncio.gather(
                *(
                    self._poll_peer(session, peer["host"], my_id)
                    for peer in peers
                ),
                return_exceptions=True,
            )
        # gather preserves order, so results[i] corresponds to peers[i].
        for index, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                logger.error(
                    "cluster: unexpected error polling %s: %r",
                    peers[index]["host"],
                    result,
                )

    async def _poll_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        url = "https://{}/peer".format(host)
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            async with session.get(url, ssl=self._client_ssl) as resp:
                resp.raise_for_status()
                raw, too_large = await _read_capped(
                    resp, MAX_PEER_RESPONSE_BYTES
                )
        except aiohttp.ClientSSLError as ex:
            # cert chain / hostname verification failure: the peer is not (or
            # not provably) a cluster member.
            self.view.record_failure(host, str(ex), untrusted=True)
            return
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
        ) as ex:
            self.view.record_failure(host, str(ex), untrusted=False)
            return
        if too_large:
            self.view.record_failure(
                host,
                "oversized /peer response (> {} bytes)".format(
                    MAX_PEER_RESPONSE_BYTES
                ),
                untrusted=False,
            )
            return
        try:
            data = json.loads(raw)
        except ValueError:
            # invalid/truncated JSON (JSONDecodeError and UnicodeDecodeError
            # both subclass ValueError): a CA-trusted peer can still be buggy
            # or hostile, so treat an unparseable body as a failed observation.
            self.view.record_failure(
                host, "invalid JSON in /peer response", untrusted=False
            )
            return
        if not isinstance(data, dict):
            self.view.record_failure(
                host,
                "malformed /peer response (not a JSON object)",
                untrusted=False,
            )
            return
        # Type-validate the scalar identity fields: a non-string node_name from
        # a CA-trusted-but-misbehaving peer would otherwise flow into
        # min()/sorted()/dict keys during election and crash the scheduler.
        fields: Dict[str, Optional[str]] = {}
        for key in (
            "node_name",
            "job_set_id",
            "scheme_version",
            "instance_id",
        ):
            value = data.get(key)
            if value is not None and not isinstance(value, str):
                self.view.record_failure(
                    host,
                    "malformed /peer response: {!r} is not a string".format(
                        key
                    ),
                    untrusted=False,
                )
                return
            fields[key] = value
        self.view.record_success(
            host,
            fields["node_name"],
            fields["job_set_id"],
            fields["scheme_version"],
            my_id,
            now,
            self.node_name,
            peer_instance=fields["instance_id"],
            my_instance=self.instance_id,
            peer_members=_parse_members(data.get("members")),
            peer_ran_reboot_jobs=_parse_str_list(data.get("ran_reboot_jobs")),
        )

    # --- deferred @reboot "already ran" gossip ---------------------------

    def advertised_ran_jobs(self) -> Set[str]:
        """@reboot one-shots known to have run under our *current* job set.

        Our own runs plus those reported by every peer we currently agree with
        (same job_set_id).  Re-advertising what we learned makes the fact
        survive the original owner's death (one-hop gossip), and trusting only
        AGREED peers scopes it to this config -- a peer on a different job set
        is not agreed, so its set is ignored.
        """
        jobs = set(self._ran_reboot_jobs)
        for peer in self.view.peers.values():
            if peer.status == STATUS_AGREED and peer.ran_reboot_jobs:
                jobs |= peer.ran_reboot_jobs
        return jobs

    def reboot_ran(self, job_name: str) -> bool:
        """Whether ``job_name`` already ran in the cluster (this config)."""
        return job_name in self.advertised_ran_jobs()

    async def mark_reboot_ran(self, job_name: str) -> None:
        """Record that we ran ``job_name`` as owner, and eagerly tell peers.

        The push is best-effort (the periodic pull carries the same set as a
        backstop); it just shrinks the window in which we could run the job and
        then die before any peer observed it.
        """
        # the poll loop is the sole authority for _ran_jobs_job_set_id (it
        # establishes it and clears the set on a change), so we only add here.
        self._ran_reboot_jobs.add(job_name)
        await self._push_reboot_ran()

    async def _push_reboot_ran(self) -> None:
        peers = self.config["peers"]
        names = sorted(self.advertised_ran_jobs())
        if not peers or not names:
            return
        payload = {"job_set_id": self.get_job_set_id(), "names": names}
        timeout = aiohttp.ClientTimeout(total=self.config["connectTimeout"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await asyncio.gather(
                *(
                    self._push_reboot_ran_one(session, peer["host"], payload)
                    for peer in peers
                ),
                return_exceptions=True,
            )

    async def _push_reboot_ran_one(
        self,
        session: aiohttp.ClientSession,
        host: str,
        payload: Dict[str, Any],
    ) -> None:
        url = "https://{}/reboot-ran".format(host)
        try:
            async with session.post(
                url, json=payload, ssl=self._client_ssl
            ) as resp:
                resp.raise_for_status()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            # best-effort: a delivery failure is fine, the periodic pull-poll
            # carries the same set; don't let it disturb the run loop.
            pass

    # --- leader election --------------------------------------------------

    def cluster_size(self) -> int:
        """Total number of cluster members.

        ``peers`` lists every *other* member, so the cluster is those plus this
        node.  (Listing this node in its own peer list is a misconfiguration;
        it is reported as ``self`` and never counts toward agreement, so it
        only makes quorum harder to reach, never easier.)
        """
        return len(self.config["peers"]) + 1

    def quorum(self) -> int:
        return quorum_size(self.cluster_size())

    def _agreeing_peer_names(self) -> List[str]:
        """Names of peers we *mutually* agree with on our job-set id.

        A peer counts only when both directions are confirmed: we see it AGREED
        *and* its last /peer response lists us (by our unique ``instance_id``)
        as a node it sees AGREED too.  The mutual requirement is what keeps the
        quorum gate sound under asymmetric reachability -- two nodes joined by
        a one-way link can no longer each count the other and both reach a
        bogus majority (which would let both self-elect and double-run a Leader
        job).  The price is one extra poll round to converge after a membership
        change, and that a purely one-way-reachable peer is treated as
        unreachable for quorum purposes.
        """
        return [
            peer.node_name
            for peer in self.view.peers.values()
            if peer.status == STATUS_AGREED
            and peer.node_name is not None
            and _peer_sees_me_agreed(peer.members, self.instance_id)
        ]

    # --- duplicate-nodeName detection ------------------------------------

    def conflict_names(self) -> List[str]:
        """nodeNames currently claimed by more than one distinct instance.

        Non-empty means a duplicate ``nodeName`` is present, which makes the
        quorum election unsafe (two nodes would each elect themselves), so the
        ``Leader`` gate treats it as fail-closed.

        The view is built by unioning *our own* fresh observations with every
        reachable peer's reported observations (the ``members`` list from its
        /peer response -- one-hop gossip).  That transitivity closes the gap
        where the duplicates are not both visible to us directly: two peers
        that each see only one copy of the duplicated name still let us spot
        the collision.  ``identity`` is the per-process ``instance_id``
        (falling back to a peer's host if it somehow reported none), and benign
        self-listing (same name *and* same instance id) is not a conflict.
        Stale peers (unreachable/untrusted/never-contacted) contribute nothing.
        """
        by_name: Dict[str, Set[str]] = defaultdict(set)
        by_name[self.node_name].add(self.instance_id)
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES:
                continue
            if peer.node_name is not None:
                # our own direct observation of this peer's identity
                by_name[peer.node_name].add(
                    peer.instance_id or "host:" + peer.host
                )
            # the peer's one-hop view of the cluster
            for name, instance, _agreed in peer.members or ():
                by_name[name].add(instance)
        return sorted(
            name for name, idents in by_name.items() if len(idents) > 1
        )

    def has_conflict(self) -> bool:
        """Whether any duplicate nodeName is currently visible to this node."""
        return bool(self.conflict_names())

    def leader_name(self) -> Optional[str]:
        """Elected leader as this node sees it, or ``None`` if not quorate."""
        return elect_leader(
            self.node_name, self._agreeing_peer_names(), self.cluster_size()
        )

    def is_leader(self) -> bool:
        """Whether this node is the elected leader (quorate, lowest name)."""
        return self.leader_name() == self.node_name

    def available_leader_name(self) -> str:
        """Elected leader ignoring quorum (for the ``PreferLeader`` policy)."""
        return elect_available_leader(
            self.node_name, self._agreeing_peer_names()
        )

    def is_available_leader(self) -> bool:
        """Whether this node leads its reachable set, quorum or not."""
        return self.available_leader_name() == self.node_name

    def is_quorate(self) -> bool:
        """Whether this node currently sees a quorum (so it may run jobs)."""
        return self.leader_name() is not None

    # --- per-job ownership (distribution: spread) -------------------------

    def job_owner(self, job_name: str) -> Optional[str]:
        """Quorum-gated owner of ``job_name`` (spread mode), else ``None``."""
        return elect_job_owner(
            job_name,
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
        )

    def is_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` (quorate, rendezvous winner)."""
        return self.job_owner(job_name) == self.node_name

    def available_job_owner(self, job_name: str) -> str:
        """Owner of ``job_name`` ignoring quorum (spread ``PreferLeader``)."""
        return elect_available_job_owner(
            job_name, self.node_name, self._agreeing_peer_names()
        )

    def is_available_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` in its reachable set."""
        return self.available_job_owner(job_name) == self.node_name

    def view_dict(self) -> Dict[str, Any]:
        leader = self.leader_name()
        spread = self.distribution == "spread"
        conflicts = self.conflict_names()
        return {
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": bool(self.config.get("electLeader")),
            "distribution": self.distribution,
            # a duplicate nodeName was detected: Leader jobs fail closed until
            # it clears (see has_conflict / cron._cluster_allows).
            "conflict": bool(conflicts),
            "conflict_names": conflicts,
            "quorate": leader is not None,
            # In spread mode there is no single leader: ownership is per job,
            # so leader/is_leader are not meaningful (reported null/false).
            "leader": None if spread else leader,
            "is_leader": (
                False
                if spread
                else (leader is not None and leader == self.node_name)
            ),
            "peers": self.view.to_list(),
        }
