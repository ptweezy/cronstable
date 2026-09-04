"""Peer attestation: confirm a static set of peers run the same job set.

Each node serves ``GET /peer`` on a dedicated mTLS listener and polls every
configured peer, comparing job-set ids (:mod:`cronstable.fingerprint`).
mTLS is the membership boundary; there is no shared state, each node keeps
only its own view. Duplicate ``nodeName``s and divergent declared cluster
sizes or coordination policies are ``conflict``s that fail the ``Leader``
gate closed; a same-N membership swap is NOT caught, so change membership
one node at a time. Drift is debounced over ``driftAfter`` rounds, and an
unchanged round is a bodyless 304 via content-derived ETags. With
``electLeader`` the lowest mutually-attested, confirmed-quorate ``nodeName``
leads under a strict-majority quorum; bridged asymmetry is closed via the
gossiped ``mutual_agreeing`` / ``quorate_vouched`` sets, and a hostile
CA-vouched peer (Byzantine) is out of scope. This is at-most-once for
``Leader`` jobs under one shared N, not a fenced exactly-once guarantee.
``distribution: spread`` replaces the leader with per-job rendezvous
ownership under the same quorum gate; ``PreferLeader`` drops the quorum
gate to favour liveness (an isolated node runs, partitions may double-run).
"""

import asyncio
import datetime
import functools
import hashlib
import json
import logging
import math
import ssl
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import islice
from typing import (
    Any,
    ClassVar,
    Optional,
    TypeGuard,
    TypeVar,
)

import aiohttp
from aiohttp import web

from cronstable import _json, tlsutil
from cronstable.config import ClusterConfig
from cronstable.fingerprint import SCHEME_VERSION
from cronstable.leadership import LeadershipBackend

logger = logging.getLogger("cronstable.cluster")

_UTC = datetime.timezone.utc

# Per-peer status, as reported in the /cluster view.
STATUS_UNKNOWN = "unknown"  # not yet contacted
STATUS_SELF = "self"  # the peer reported our own node name AND instance id
STATUS_AGREED = "agreed"  # reachable, same job-set id
STATUS_SYNCING = "syncing"  # reachable, id differs but within driftAfter
STATUS_DRIFTED = "drifted"  # reachable, id has differed >= driftAfter rounds
STATUS_UNREACHABLE = "unreachable"  # connect/timeout failure
STATUS_UNTRUSTED = "untrusted"  # TLS/cert verification failed
# A different running instance is announcing our own nodeName. Never counts
# toward agreement, and makes the leader gate fail closed (see
# ClusterManager.has_conflict / cronstable.cron._cluster_allows).
STATUS_CONFLICT = "conflict"

# Statuses for which we hold no fresh observation of the peer's identity this
# round, so the peer is ignored when detecting nodeName collisions.
_STALE_STATUSES = frozenset(
    {STATUS_UNKNOWN, STATUS_UNREACHABLE, STATUS_UNTRUSTED}
)

# Completed poll rounds after which the never-skip available_* gates stop
# holding for an AGREED peer that has not attested this instance back (see
# ClusterManager.view_settled): a peer still silent after 3 rounds is a
# genuinely one-way link, and the never-skip contract leans toward running.
_SETTLE_ROUNDS = 3

# Cap on the /peer response buffered per poll: bounds the memory a
# misbehaving-but-CA-trusted peer can force (see _read_capped / _poll_peer).
MAX_PEER_RESPONSE_BYTES = 256 * 1024
_READ_CHUNK = 8192

# Per-field bounds on a CA-vouched-but-untrusted peer's /peer payload. The
# @reboot-ran set is PERSISTENT and re-advertised, so without a per-set bound
# a peer could grow OUR /peer response past the byte cap and drop us from
# honest peers' quorum (a cluster-wide availability DoS). Also rejects over-
# long / control-character scalar identity fields (reflected into JSON/logs).
MAX_PEER_FIELD_LEN = 256  # node_name, job_set_id, instance_id, ...
MAX_MEMBER_ENTRIES = 4096  # members[] / mutual_agreeing[] cardinality
MAX_REBOOT_JOB_NAME_LEN = 128  # a single @reboot job name
MAX_ADVERTISED_REBOOT_JOBS = 512  # ran-set cardinality stored + re-advertised

# Bounds on the gossiped fleet-view job_summaries block. Absorbed peer
# summaries are never re-advertised, so these bound what we EMIT (our own
# /peer body must stay under MAX_PEER_RESPONSE_BYTES) and what we STORE from
# an untrusted peer.
MAX_JOB_SUMMARY_NAME_LEN = 128  # a single job name in job_summaries
MAX_ADVERTISED_JOB_SUMMARIES = 512  # per-node job_summaries cardinality
MAX_JOB_SUMMARY_TS_LEN = 64  # an ISO-8601 finished_at timestamp

# Cap on the transitively-discovered candidate names this node derives and
# re-advertises (_bridge_candidates, and quorate_vouched built on it): the
# one re-broadcast path NOT bounded by our own config, so uncapped it is the
# same re-advertised-set DoS the ran_reboot_jobs cap stops. Lists are sorted
# ASCENDING before slicing, so every node keeps the same lowest-names prefix
# and truncation cannot change a single-leader election (elect_leader reads
# ``min``). A truncation IS reported (log warning + `candidates_truncated`
# in /cluster): a fleet past this cap may drop a ``spread`` co-owner from
# the tail and double-run its jobs. See ClusterManager._bridge_candidates.
MAX_ADVERTISED_CANDIDATE_NAMES = 512

# the only run outcomes a peer summary may carry (mirrors JobRunInfo.outcome)
_SUMMARY_OUTCOMES = frozenset({"success", "failure", "cancelled"})

# The fixed numeric fields a gossiped node_stats block may carry (see
# cronstable.resources.NodeResourceSampler). A peer's block is rebuilt to
# exactly these keys, each coerced to a finite number, so a hostile peer
# cannot plant unbounded or non-JSON values in our /fleet response.
_NODE_STATS_FIELDS = (
    "cpu_percent",
    "cpu_count",
    "mem_percent",
    "mem_used_bytes",
    "mem_total_bytes",
    "proc_rss_bytes",
    "proc_cpu_percent",
)

# Poll intervals an absorbed peer node_stats reading stays renderable
# without a fresh one before expiring (serialized as None): last_seen keeps
# advancing regardless, and a load number cannot be re-derived from its age,
# so the reading must expire rather than show hours-old CPU/memory as
# current. Three rounds mirrors driftAfter's default debounce.
NODE_STATS_STALE_ROUNDS = 3

# Conditional /peer exchange (see _handle_peer / _observe_peer): the stored
# ETag is re-sent every round, so bound what a hostile peer can make us
# store and echo (ours is a quoted sha256 digest, 66 chars).
MAX_PEER_ETAG_LEN = 128  # a stored / echoed ETag header value
MIN_COMPRESS_BYTES = 1024  # smallest /peer body worth gzip-compressing

# How long _handle_peer may re-serve an already-built (payload, ETag) pair
# (seconds, monotonic): a pair built up to 1s early is indistinguishable
# from having polled 1s earlier. The TTL only bounds inputs the cache key
# cannot observe (chiefly the live summaries snapshot); election-relevant
# state is keyed exactly, so a view change invalidates immediately.
PEER_RESPONSE_CACHE_TTL = 1.0

# Gossiped node stats ride a /peer response HEADER, not the body: live
# values would roll the body's ETag every round and defeat the 304
# optimisation. Travels on the 200 and the 304 alike; absence means "not
# sharing right now". See _handle_peer / _observe_peer.
NODE_STATS_HEADER = "X-Cronstable-Node-Stats"
# Bound on a header value the poller will parse; belt-and-braces atop
# aiohttp's ~8K per-header limit against a hostile peer padding the value.
MAX_NODE_STATS_HEADER_LEN = 1024

# Cap on the per-generation rendezvous-owner memo each spread ownership
# method keeps (see ClusterManager._spread_owner_set). Keys are configured
# job names; at the cap extra names pay the plain rendezvous hash. 100k
# matches the largest documented fleet.
MAX_MEMOIZED_JOB_OWNERS = 100_000


def _parse_members(
    raw: Any,
    *,
    max_len: Optional[int] = None,
    max_items: Optional[int] = None,
) -> list["tuple[str, str, bool]"]:
    """Validate a peer's reported ``members`` list, dropping malformed entries.

    The peer is CA-vouched but otherwise untrusted: bad input degrades to "no
    mutual/transitive information" rather than poisoning the election, and
    ``max_len`` / ``max_items`` bound the work a hostile peer can force.
    """
    members: list["tuple[str, str, bool]"] = []
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
            # Drop empty names/instances: '' sorts below every real name, so
            # it could reach the election as a candidate no node can match.
            if not (name and instance):
                continue
            if max_len is not None and (
                len(name) > max_len or len(instance) > max_len
            ):
                continue
            # Drop control characters: a transitive member's node_name flows
            # (via conflict_names) into operator-facing logs, a log-injection
            # vector. Mirrors _poll_peer's isprintable() guard.
            if not (name.isprintable() and instance.isprintable()):
                continue
            members.append((name, instance, agreed))
            if max_items is not None and len(members) >= max_items:
                break
    return members


def _parse_str_list(
    raw: Any,
    *,
    max_len: Optional[int] = None,
    max_items: Optional[int] = None,
) -> "set[str]":
    """Validate an untrusted JSON value as a set of strings, dropping the rest.

    Used for the gossiped ``ran_reboot_jobs`` / ``mutual_agreeing`` sets:
    hostile input degrades to an empty set, an absent field contributes no
    evidence (the safe direction), and the caps bound what a CA-vouched peer
    can make us store and re-broadcast.
    """
    if not isinstance(raw, list):
        return set()
    out: "set[str]" = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        # Drop the empty string: it sorts BELOW every real name, so folded
        # into mutual_agreeing it would make elect_leader's ``min`` pick ''
        # cluster-wide and stop every Leader job firing.
        if not item:
            continue
        if max_len is not None and len(item) > max_len:
            continue
        # Drop control characters: a gossiped name can reach operator logs
        # (see _parse_members / _poll_peer).
        if not item.isprintable():
            continue
        out.add(item)
        if max_items is not None and len(out) >= max_items:
            break
    return out


def _finite_number(value: Any) -> Optional[float]:
    """An untrusted JSON value as a finite float, else ``None``.

    Rejects bools (an int subclass) and non-finite floats: Python's json
    module re-emits ``Infinity``/``NaN``, which JSON.parse rejects, so one
    planted value would blank the dashboard's fleet view cluster-wide.
    """
    kind = type(value)
    if kind is float:
        # a float, the common wire shape, needs no isinstance chain
        exact: float = value
        return exact if math.isfinite(exact) else None
    if kind is int:
        # bool is excluded (type(True) is bool); float(int) is finite
        return float(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    out = float(value)
    return out if math.isfinite(out) else None


def _parse_job_summaries(raw: Any) -> Optional[dict[str, dict[str, Any]]]:
    """Validate a peer's gossiped ``job_summaries`` block, field by field.

    Every field is type-checked and rebuilt into a fresh dict (never stored
    as-received); names are length-capped and printable (they reach the
    /fleet JSON) and the entry count is capped. Returns ``None`` (not ``{}``)
    when absent or not an object, so "an older build gossiping no summaries"
    stays distinguishable from "zero jobs".
    """
    if not isinstance(raw, dict):
        return None
    out: dict[str, dict[str, Any]] = {}
    for name, entry in raw.items():
        if (
            not isinstance(name, str)
            or not name
            or len(name) > MAX_JOB_SUMMARY_NAME_LEN
            or not name.isprintable()
            or not isinstance(entry, dict)
        ):
            continue
        summary: dict[str, Any] = {
            "running": entry.get("running") is True,
            "enabled": entry.get("enabled") is not False,
            "scheduled_in": _finite_number(entry.get("scheduled_in")),
            "last": None,
        }
        last = entry.get("last")
        if isinstance(last, dict):
            outcome = last.get("outcome")
            finished_at = last.get("finished_at")
            exit_code = last.get("exit_code")
            if (
                outcome in _SUMMARY_OUTCOMES
                and isinstance(finished_at, str)
                and len(finished_at) <= MAX_JOB_SUMMARY_TS_LEN
                and finished_at.isprintable()
            ):
                summary["last"] = {
                    "outcome": outcome,
                    "finished_at": finished_at,
                    "duration": _finite_number(last.get("duration")),
                    "exit_code": (
                        exit_code
                        if isinstance(exit_code, int)
                        and not isinstance(exit_code, bool)
                        else None
                    ),
                }
        out[name] = summary
        if len(out) >= MAX_ADVERTISED_JOB_SUMMARIES:
            break
    return out


def _aged_job_summaries(
    jobs: Optional[dict[str, dict[str, Any]]],
    taken_at: Optional[datetime.datetime],
    now: datetime.datetime,
) -> Optional[dict[str, dict[str, Any]]]:
    """A peer's stored job summaries with each countdown aged to ``now``.

    A stored ``scheduled_in`` is the peer's countdown as of receipt of the
    full /peer body (``taken_at``); with 304 rounds a snapshot outlives many
    polls. Ageing subtracts the snapshot's age on our clock alone (an elapsed
    duration, so peer clock offsets never leak in), clamped at 0; a fire
    rolls the peer's ETag, so the next poll ships the real successor value.
    Entries are copied, never mutated.
    """
    if jobs is None or taken_at is None:
        return jobs
    elapsed = (now - taken_at).total_seconds()
    if elapsed <= 0:
        return jobs
    aged: dict[str, dict[str, Any]] = {}
    for name, entry in jobs.items():
        scheduled = entry.get("scheduled_in")
        if scheduled is not None:
            remaining = scheduled - elapsed
            entry = dict(
                entry, scheduled_in=remaining if remaining > 0.0 else 0.0
            )
        aged[name] = entry
    return aged


def _parse_node_stats(raw: Any) -> Optional[dict[str, Any]]:
    """Validate a peer's gossiped ``node_stats`` block into a fresh dict.

    Every value is re-coerced through :func:`_finite_number` (rejecting
    bools, NaN/Inf and non-numbers) and only :data:`_NODE_STATS_FIELDS` are
    copied. Returns ``None`` (not ``{}``) when absent or unusable, so "a peer
    sharing no stats" stays distinguishable from "reporting zero load".
    """
    if not isinstance(raw, dict):
        return None
    out: dict[str, Any] = {}
    for field in _NODE_STATS_FIELDS:
        value = _finite_number(raw.get(field))
        if value is None:
            continue
        # cpu_count is a whole number of cores; keep it integral so the fleet
        # JSON reads naturally. The rest stay floats.
        out[field] = int(value) if field == "cpu_count" else value
    return out or None


def _parse_node_stats_header(raw: Optional[str]) -> Optional[dict[str, Any]]:
    """A peer's :data:`NODE_STATS_HEADER` value as a validated stats dict.

    ``None`` for an absent header (absence is the signal, not an error) and
    for any unusable value: over-long, unparseable, or rejected by
    :func:`_parse_node_stats`. Never fails the poll.
    """
    if raw is None or len(raw) > MAX_NODE_STATS_HEADER_LEN:
        return None
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError):
        # junk from a buggy/hostile peer, never a poll failure
        return None
    return _parse_node_stats(data)


def _peer_sees_me_agreed(
    peer_members: Optional[list["tuple[str, str, bool]"]],
    my_instance: str,
) -> bool:
    """Whether a peer's member list shows us (by ``instance_id``) as AGREED.

    The receiver half of the mutual-attestation gate: a peer counts toward
    quorum only when it confirms it sees us agreeing too (see
    :meth:`ClusterManager._agreeing_peer_names`).
    """
    if not peer_members:
        return False
    for _name, instance, agreed in peer_members:
        if agreed and instance == my_instance:
            return True
    return False


# the type of one peer-declared coordination value (see _declares_divergent);
# carrying it through the guard lets a caller keep the non-optional value.
_DeclaredT = TypeVar("_DeclaredT")


def _declares_divergent(
    declared: Optional[_DeclaredT], ours: Any
) -> TypeGuard[_DeclaredT]:
    """Whether a peer *declared* a coordination value and it is not ours.

    The one spelling of the divergence test every gate runs
    (:meth:`ClusterManager._agreeing_peers`, ``conflicting_sizes``,
    ``conflicting_policies``). ONE safety invariant: a peer the detectors
    flag MUST also be dropped from the gossiped mutually agreeing set, or a
    third node could bridge-confirm it as quorate; fenced by
    ``test_declared_fields_gate_both_agreement_and_conflict``. ``None`` (a
    peer too old to declare) never diverges: absence is no evidence, and
    failing legacy builds closed mid rolling upgrade would be wrong.
    """
    return declared is not None and declared != ours


async def _read_capped(resp: Any, limit: int) -> "tuple[bytes, bool]":
    """Read a response body, refusing to buffer more than ``limit`` bytes.

    Returns ``(body, too_large)``. Iterating bounds memory on a huge or
    chunked response, and since aiohttp decompresses as we read it also caps
    the decompressed size (a gzip-bomb guard).
    """
    chunks: list[bytes] = []
    total = 0
    async for chunk in resp.content.iter_chunked(_READ_CHUNK):
        total += len(chunk)
        if total > limit:
            return b"", True
        chunks.append(chunk)
    return b"".join(chunks), False


def quorum_size(cluster_size: int) -> int:
    """The strict majority of ``cluster_size`` nodes.

    More than half, so no two quorums can be disjoint: the property the
    leader gate relies on for safety.
    """
    return cluster_size // 2 + 1


def elect_leader(
    node_name: str,
    live_peer_names: Iterable[str],
    cluster_size: int,
    candidate_names: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """Pure, deterministic leader election from one node's point of view.

    Below a quorum of ``cluster_size`` (this node plus ``live_peer_names``)
    returns ``None``: a minority partition stands down. When quorate the
    leader is the lowest name among this node and ``candidate_names`` (the
    confirmed-quorate candidates, see
    :meth:`ClusterManager._eligible_candidates`), which defaults to
    ``live_peer_names`` and never affects the quorum gate. Residuals: a
    too-thin bridge may double-run, and a candidate confirmed from now-stale
    gossip can briefly draw the majority into a transient skip.
    """
    live = [node_name, *live_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    if candidate_names is None:
        return min(live)
    return min([node_name, *candidate_names])


def elect_available_leader(
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Leaderless election without the quorum gate (favours liveness).

    The lowest name among this node and the peers it sees agreeing; this
    node is always in the set, so a name is always returned. An isolated
    node runs; two partition sides may double-run. Used by ``PreferLeader``.
    """
    return min([node_name, *agreeing_peer_names])


def _hrw_score(job_name: str, node_name: str) -> int:
    """Rendezvous (highest-random-weight) score for one (job, node) pair.

    Deterministic across nodes and processes, well-mixed; only the ordering
    of scores matters.
    """
    digest = hashlib.sha256(
        job_name.encode("utf-8") + b"\x00" + node_name.encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big")


def _hrw_owner(job_name: str, members: list[str]) -> str:
    """The rendezvous winner for ``job_name`` among ``members``.

    Highest score wins; ties break on the node name for determinism. A
    membership change reassigns only the leaving/joining node's own share.
    """
    return max(members, key=lambda n: (_hrw_score(job_name, n), n))


def _hrw_owner_bytes(
    job_name: str, members: list[str], member_bytes: list[bytes]
) -> str:
    """:func:`_hrw_owner` with the members' name bytes pre-encoded.

    Identical result to ``_hrw_owner(job_name, members)``: the job prefix is
    absorbed once into a seed hash and cloned per member, and the 8-byte
    digest prefixes are compared raw (lexicographic bytes order on
    equal-length slices IS big-endian unsigned order, preserving
    :func:`_hrw_score`'s ordering and the name tie-break).
    """
    seed = hashlib.sha256(job_name.encode("utf-8") + b"\x00")
    copy = seed.copy
    first = copy()
    first.update(member_bytes[0])
    best_name = members[0]
    best_score = first.digest()[:8]
    # islice skips the first pair without copying either member list; the
    # strict zip is drained in full, so a length mismatch raises.
    for name, name_bytes in islice(
        zip(members, member_bytes, strict=True), 1, None
    ):
        digest = copy()
        digest.update(name_bytes)
        score = digest.digest()[:8]
        # (score, name) > (best_score, best_name), spelled out so that
        # the per-member compare allocates no tuples
        if score > best_score or (score == best_score and name > best_name):
            best_score, best_name = score, name
    return best_name


def elect_job_owner(
    job_name: str,
    node_name: str,
    live_peer_names: Iterable[str],
    cluster_size: int,
    candidate_names: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """Quorum-gated per-job owner (the ``distribution: spread`` analogue of
    :func:`elect_leader`).

    ``None`` below quorum. When quorate the owner is the rendezvous winner
    over this node and ``candidate_names`` (confirmed-quorate names, as in
    :func:`elect_leader`), so bridged quorate nodes pick the same owner and
    the owner never stands itself down. ``candidate_names`` never affects
    the quorum gate.
    """
    live = [node_name, *live_peer_names]
    if len(live) < quorum_size(cluster_size):
        return None
    names = live_peer_names if candidate_names is None else candidate_names
    return _hrw_owner(job_name, [node_name, *names])


def elect_available_job_owner(
    job_name: str,
    node_name: str,
    agreeing_peer_names: Iterable[str],
) -> str:
    """Per-job owner without the quorum gate (spread-mode ``PreferLeader``).

    Rendezvous winner among this node and its agreeing peers; always returns
    a value, so an isolated node owns all its jobs, at the cost of a
    possible double-run on partition.
    """
    return _hrw_owner(job_name, [node_name, *agreeing_peer_names])


@dataclass(slots=True)
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
    # whether a successful poll has positively identified this host as THIS
    # node. Latched so transient self-poll failures keep the entry
    # STATUS_SELF (else cluster_size would flap N<->N+1 on the poll
    # interval); re-evaluated on every successful poll. Internal; not in
    # to_dict.
    self_confirmed: bool = False
    last_seen: Optional[datetime.datetime] = None  # last successful contact
    last_error: Optional[str] = None
    # consecutive reachable-but-mismatched rounds, for the drift hysteresis
    mismatch_streak: int = 0
    # the peer's own reported observations (node_name, instance_id, agreed),
    # feeding mutual-agreement and transitive conflict detection (see
    # _agreeing_peer_names / conflict_names). None when no fresh response.
    # Internal; not in to_dict.
    members: Optional[list["tuple[str, str, bool]"]] = None
    # whether the last /peer response carried a ``members`` list. A legacy
    # build omits it and cannot attest us back, so _agreeing_peers falls
    # back to one-directional agreement for it (else a new node stands down
    # among legacy peers, a cluster-wide Leader halt). Defaults True, the
    # STRICT direction: only _observe_peer POSITIVELY seeing no list marks a
    # peer legacy; record_failure resets it. Internal; not in to_dict.
    reports_members: bool = True
    # the cluster size (len(peers)+1) the peer last declared; a divergence
    # is a first-class conflict (see conflicting_sizes). None when no fresh
    # result, or a peer too old to report it. Internal; not in to_dict.
    declared_size: Optional[int] = None
    # the coordination policy the peer last declared (distribution +
    # electLeader): behaviour-affecting yet NOT in the fingerprint, so
    # divergent nodes still see each other AGREED; a divergence is a
    # first-class conflict (see conflicting_policies). None when no fresh
    # result or a peer too old. Internal; not in to_dict.
    declared_distribution: Optional[str] = None
    declared_elect_leader: Optional[bool] = None
    # the @reboot job names the peer reports as already run in the cluster
    # (its own runs plus what it learned), used to retire our matching deferred
    # one-shots without re-running them (see ClusterManager.reboot_ran). Only
    # trusted from an AGREED peer (same job-set id). None when no fresh result.
    ran_reboot_jobs: Optional["set[str]"] = None
    # the names the peer reports it *mutually* agrees with (its own
    # _agreeing_peer_names). Unlike members' one-way ``agreed`` flag, this
    # two-way set is the only sound evidence that a transitively-reached
    # node is itself quorate (see _bridge_candidates). None when no fresh
    # result or an older peer (no bridge evidence: the safe direction).
    mutual_agreeing: Optional["set[str]"] = None
    # the names the peer can itself *confirm are quorate* (its own
    # _eligible_candidates). Load-bearing for the spread Leader-path fold
    # (_unconfirmed_contenders): folding an edge-reachable but sub-quorum
    # node would make every quorate node defer to one that then stands down,
    # a silent cluster-wide zero-run. None when no fresh result or an older
    # peer (which then vouches nothing: it can only lean toward running).
    quorate_vouched: Optional["set[str]"] = None
    # the peer's advertised per-job run summaries, feeding GET /fleet.
    # Observability only, never an election input, so deliberately NOT
    # cleared on a failed poll: the fleet view shows a briefly-unreachable
    # node's last-known state aged by last_seen rather than blanking it.
    # None = never reported. Internal; not in to_dict.
    job_summaries: Optional[dict[str, dict[str, Any]]] = None
    # whether the peer said it truncated its advertised summaries at its
    # cap, so the fleet view labels that node's column partial.
    job_summaries_truncated: bool = False
    # when the summaries snapshot was received. A 304 round refreshes
    # last_seen but NOT this (the replay passes the original receipt time
    # through record_success), so fleet_view ages each countdown by the
    # snapshot's true age instead of freezing it (see _aged_job_summaries).
    # Internal; not in to_dict.
    job_summaries_at: Optional[datetime.datetime] = None
    # the peer's last-reported whole-node CPU/memory, for the cluster panel
    # (to_dict) and fleet view. Like job_summaries: None leaves any absorbed
    # value in place rather than blanking a briefly-unreachable node.
    node_stats: Optional[dict[str, Any]] = None
    # when the node_stats reading was last known current: stamped on every
    # successful poll whose response carried NODE_STATS_HEADER (200 and 304
    # alike). A freshness bound, not an ageing baseline: last_seen keeps
    # advancing on header-less polls, so without this stamp the view would
    # serve an hours-old reading as current forever; see fresh_node_stats.
    node_stats_at: Optional[datetime.datetime] = None

    # Process-wide count of PeerState field writes (a ClassVar, ignored by
    # the dataclass machinery): the generation the manager's memoized
    # election-derived results are keyed on. See __setattr__ below and
    # ClusterManager._derived_state_key.
    _mutation_generation: ClassVar[int] = 0

    def __setattr__(self, name: str, value: Any) -> None:
        # Bump the generation on EVERY field write so a memoized result can
        # never outlive its inputs. Process-wide on purpose: an unrelated
        # view's bump only forces a spurious recompute, never staleness.
        # Caveat: this sees assignments, not in-place mutation of an
        # assigned collection; sound because every writer replaces peer
        # collections wholesale and none appends into them.
        object.__setattr__(self, name, value)
        PeerState._mutation_generation += 1

    def fresh_node_stats(
        self, now: datetime.datetime, max_age: float
    ) -> Optional[dict[str, Any]]:
        """The last-absorbed ``node_stats``, or ``None`` once expired.

        ``max_age`` is the staleness window in seconds (see
        ``ClusterManager._node_stats_max_age``); past it every consumer
        renders ``None``: "no data" beats a stale load number as current.
        """
        if self.node_stats is None or self.node_stats_at is None:
            return None
        if (now - self.node_stats_at).total_seconds() > max_age:
            return None
        return self.node_stats

    def to_dict(
        self,
        now: Optional[datetime.datetime] = None,
        node_stats_max_age: Optional[float] = None,
    ) -> dict[str, Any]:
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
            # this peer's last-absorbed load, expired once no fresh reading
            # arrived within the staleness window (see fresh_node_stats);
            # callers pass now + the window both-or-neither.
            "node_stats": (
                self.fresh_node_stats(now, node_stats_max_age)
                if now is not None and node_stats_max_age is not None
                else self.node_stats
            ),
        }


class ClusterView:
    """This node's peer table and the rules that update it.

    Pure (no I/O): the networking layer feeds it observations and reads back
    the table, which keeps the drift/state logic trivially testable.
    """

    def __init__(self, hosts: list[str], drift_after: int) -> None:
        self.drift_after = drift_after
        # preserve configured order for a stable view
        self.peers: "dict[str, PeerState]" = {
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
        peer_members: Optional[list["tuple[str, str, bool]"]] = None,
        peer_ran_reboot_jobs: Optional["set[str]"] = None,
        peer_size: Optional[int] = None,
        peer_mutual_agreeing: Optional["set[str]"] = None,
        peer_quorate_vouched: Optional["set[str]"] = None,
        peer_distribution: Optional[str] = None,
        peer_elect_leader: Optional[bool] = None,
        peer_reports_members: bool = True,
        peer_job_summaries: Optional[dict[str, dict[str, Any]]] = None,
        peer_job_summaries_truncated: bool = False,
        peer_job_summaries_at: Optional[datetime.datetime] = None,
        peer_node_stats: Optional[dict[str, Any]] = None,
    ) -> None:
        peer = self.peers[host]
        peer.last_seen = now
        peer.last_error = None
        peer.job_set_id = peer_id
        peer.node_name = peer_name
        peer.instance_id = peer_instance
        peer.members = peer_members
        # fleet-view summaries: None (an older build that gossips none)
        # leaves any previously-absorbed snapshot in place; the fleet view
        # prefers last-known over blanking, so only a real report overwrites.
        if peer_job_summaries is not None:
            peer.job_summaries = peer_job_summaries
            peer.job_summaries_truncated = peer_job_summaries_truncated
            # `now` for a freshly-parsed body; a conditional 304 replay
            # passes the ORIGINAL receipt time so countdown ageing keeps the
            # snapshot's true age instead of freezing every round.
            peer.job_summaries_at = (
                peer_job_summaries_at
                if peer_job_summaries_at is not None
                else now
            )
        # node stats: same None-keeps-last-known contract as job_summaries,
        # but stamped with node_stats_at so a never-replaced reading EXPIRES
        # (see fresh_node_stats). Freshness is per RESPONSE (the header
        # rides the 200 and the 304 alike), so stamping `now` is always
        # correct and a conditional round cannot resurrect a discontinued
        # reading.
        if peer_node_stats is not None:
            peer.node_stats = peer_node_stats
            peer.node_stats_at = now
        # whether this response actually carried a members list; drives the
        # one-directional legacy fallback in _agreeing_peers. Defaults True
        # so existing callers/tests keep the mutual gate; _observe_peer
        # passes the real value.
        peer.reports_members = peer_reports_members
        peer.ran_reboot_jobs = peer_ran_reboot_jobs
        peer.declared_size = peer_size
        peer.mutual_agreeing = peer_mutual_agreeing
        peer.quorate_vouched = peer_quorate_vouched
        peer.declared_distribution = peer_distribution
        peer.declared_elect_leader = peer_elect_leader
        # re-determine self-ness on every successful poll: an address that no
        # longer answers as us (reassigned) must be able to lose the flag.
        peer.self_confirmed = False

        if peer_name is not None and peer_name == my_name:
            if peer_instance is not None and peer_instance != my_instance:
                # A different running instance is announcing our own
                # nodeName: surface a hard conflict instead of masking it as
                # 'self'; the leader gate then fails closed.
                peer.status = STATUS_CONFLICT
                # Do NOT reset mismatch_streak: only a confirmed AGREED/SELF
                # observation clears it (see record_failure); zeroing here
                # would delay a genuinely drifting peer's DRIFTED by up to
                # driftAfter rounds.
                peer.last_error = (
                    "duplicate nodeName {!r}: peer is a different "
                    "instance".format(peer_name)
                )
                return
            # Same name *and* same instance id (the operator listed this node's
            # own address), or a peer too old to report an instance id: the
            # benign self case. Never counts toward agreement.
            peer.status = STATUS_SELF
            peer.self_confirmed = True  # latch: keep SELF across poll failures
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
        if peer.self_confirmed:
            # this host was positively identified as THIS node; a failed
            # poll (a hairpin/NAT quirk) does not change that. Keep it SELF
            # rather than flapping cluster_size on the poll interval.
            peer.status = STATUS_SELF
        else:
            peer.status = STATUS_UNTRUSTED if untrusted else STATUS_UNREACHABLE
        # no fresh observation this round, so drop the peer's last reported
        # view as stale. The drift streak is deliberately NOT reset: it
        # counts *reachable* mismatches; only a confirmed AGREED (or SELF)
        # observation in record_success resets it.
        peer.members = None
        peer.ran_reboot_jobs = None
        peer.mutual_agreeing = None
        peer.quorate_vouched = None
        # job_summaries is deliberately KEPT: observability-only, and the
        # fleet view shows last-known state aged by last_seen instead of
        # blanking it. reports_members is reset for tidiness (a failed peer
        # is not AGREED, so _agreeing_peers skips it regardless).
        peer.reports_members = False

    def to_list(
        self,
        now: Optional[datetime.datetime] = None,
        node_stats_max_age: Optional[float] = None,
    ) -> list[dict[str, Any]]:
        return [
            peer.to_dict(now, node_stats_max_age)
            for peer in self.peers.values()
        ]

    def local_members(
        self, my_name: str, my_instance: str
    ) -> list[dict[str, Any]]:
        """This node's current observations, for the /peer response body.

        Lists this node (always agreeing with itself) plus every peer with a
        fresh observation, tagged with whether we see it AGREED. A poller
        uses this to confirm mutual agreement and to detect duplicate
        nodeNames transitively.
        """
        members: list[dict[str, Any]] = [
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


def build_client_ssl_context(tls: dict[str, str]) -> ssl.SSLContext:
    """Client context: verify peer certs vs the CA, pin the hostname.

    Kept as a named function in this module (rather than callers reaching
    into :mod:`cronstable.tlsutil` directly) because the whole test suite
    neuters the cluster's TLS by patching this name.
    """
    return tlsutil.build_mutual_client_ssl_context(
        tls["ca"], tls["cert"], tls["key"]
    )


def build_server_ssl_context(tls: dict[str, str]) -> ssl.SSLContext:
    """Server context: require and verify a CA-signed client cert (mTLS).

    SECURITY: this is the cluster's membership boundary. A server cannot do
    hostname verification, so the CA file IS the allowlist: point it at a
    dedicated, single-purpose cluster CA, never a shared organisational CA,
    or any holder of any cert that CA ever signed can speak to ``/peer`` and
    ``/reboot-ran`` as a member. A hostile CA-signed member is out of scope
    (Byzantine): it can force a fail-closed ``Leader`` stand-down (the
    size/policy conflict gates credit first-party declarations by design),
    push ``reboot-ran`` suppression, and read topology, but never cause a
    double-run.

    Shared with the web listeners via
    :func:`cronstable.tlsutil.build_listener_ssl_context`, where the client
    CA is optional and its absence means no client authentication. Config
    load rejects a blank ``cluster.tls.ca``; the guard here refuses one
    from any other caller, so the CERT_REQUIRED arm always applies.
    """
    if not tls.get("ca"):
        # ValueError on purpose: outside the (OSError, SSLError) set the
        # loadable dry-runs swallow, so an empty ca fails loudly rather than
        # standing up an unauthenticated listener.
        raise ValueError("cluster.tls.ca is required for the peer listener")
    return tlsutil.build_listener_ssl_context(
        tls["cert"], tls["key"], client_ca=tls["ca"]
    )


# The on-disk files of a cluster ``tls:`` block, in signature order: the
# rotation check stats them via :func:`cronstable.tlsutil.tls_file_signature`
# (the SSL contexts are built once, so an in-place rotation would otherwise
# stay invisible until the old cert expires and the cluster loses quorum).
_TLS_SIGNATURE_KEYS = ("ca", "cert", "key")


def gossip_tls_loadable(cluster_config: ClusterConfig) -> bool:
    """Whether the gossip backend's TLS material in ``cluster_config`` loads.

    A side-effect-free dry-run of the manager's context builds, used by
    :meth:`cronstable.cron.Cron.start_stop_cluster` BEFORE it tears the
    running manager down for a config change (a config edit coinciding with
    an in-flight cert rotation would otherwise wedge ``Leader`` /
    ``PreferLeader`` closed). ``True`` for a non-gossip backend or no
    ``tls`` block. Unlike :meth:`ClusterManager.tls_files_loadable`, this
    validates the INCOMING config, which may repoint at different files.
    """
    if cluster_config.get("backend", "gossip") != "gossip":
        return True
    tls = cluster_config.get("tls")
    if not tls:
        return True
    try:
        build_client_ssl_context(tls)
        build_server_ssl_context(tls)
    except (OSError, ssl.SSLError):
        return False
    return True


def _split_host_port(addr: str) -> "tuple[str, int]":
    # Bracketed IPv6: split only on the final ":" after "]". A bare
    # unbracketed IPv6 literal is rejected at config load (see
    # config._require_host_port), so multiple colons here means bracketed.
    if addr.startswith("["):
        bracket, sep, port = addr.rpartition("]:")
        host = bracket[1:]  # strip the leading "["
        if not sep or not host or not port:
            raise ValueError("expected [ipv6]:port, got {!r}".format(addr))
        return host, int(port)
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError("expected host:port, got {!r}".format(addr))
    return host, int(port)


# the result type a memoized derived method computes (see _memoized_derived);
# preserving it through the decorator keeps call sites fully typed.
_DerivedT = TypeVar("_DerivedT")


def _memoized_derived(
    method: "Callable[[ClusterManager], _DerivedT]",
) -> "Callable[[ClusterManager], _DerivedT]":
    """Memoize a zero-argument election-derived ClusterManager method.

    The decorated methods are pure derivations over the peer table that
    change only when a poll round records an observation, yet the gates
    re-run them per due job, /peer request, and dashboard poll. Results are
    cached on the manager in one dict, keyed by
    :meth:`ClusterManager._derived_state_key`; any input change rolls the
    key and drops the whole cache at once.

    The cached object is shared, so callers MUST treat a returned list as
    frozen (audited per site). The one deliberate exception is the owner
    memo the spread-ownership derivations hand back (_spread_owner_set): a
    dict callers FILL, riding in the cache so the generation roll that
    changes the member set drops it too. Nested memoized calls are safe:
    the derivations perform no writes and the manager runs on a single
    event loop, so the key cannot move mid-computation.
    """
    name = method.__name__

    @functools.wraps(method)
    def wrapper(self: "ClusterManager") -> "_DerivedT":
        key = self._derived_state_key()
        if key != self._derived_cache_key:
            # an input changed since the cached round: every memoized result
            # derives from the same inputs, so drop them all together.
            self._derived_cache_key = key
            self._derived_cache.clear()
        try:
            # the cache is heterogeneous (keyed by method name); the
            # annotation restores the method's static result type.
            value: _DerivedT = self._derived_cache[name]
        except KeyError:
            value = method(self)
            self._derived_cache[name] = value
        return value

    return wrapper


class ClusterManager(LeadershipBackend):
    """Owns the mTLS ``/peer`` listener and the periodic peer-poll loop.

    The default, best-effort gossip leadership backend (see
    :class:`cronstable.leadership.LeadershipBackend`). It defines real
    bodies for every method on the seam, so subclassing the ABC is purely
    a conformance declaration and leaves behaviour byte-identical.
    """

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
        # config is immutable for a manager's lifetime (a cluster config
        # change rebuilds the manager), so init-time snapshots cannot go
        # stale.
        self._peer_count: int = len(config["peers"])
        self._elect_leader: bool = bool(config.get("electLeader"))
        hosts = [peer["host"] for peer in config["peers"]]
        self.view = ClusterView(hosts, config["driftAfter"])
        self._peer_urls = {
            host: "https://{}/peer".format(host) for host in hosts
        }
        self._reboot_ran_urls = {
            host: "https://{}/reboot-ran".format(host) for host in hosts
        }
        self._client_ssl = build_client_ssl_context(config["tls"])
        self._server_ssl = build_server_ssl_context(config["tls"])
        # snapshot the TLS material as loaded, so an in-place cert rotation can
        # be detected and the contexts rebuilt via a restart (see
        # tls_files_changed); the contexts themselves are never reloaded.
        self._tls_signature = tlsutil.tls_file_signature(
            config["tls"], _TLS_SIGNATURE_KEYS
        )
        self._runner: Optional[web.AppRunner] = None
        self._poll_task: Optional[asyncio.Task] = None
        # one client session for the lifetime of the manager, so peer polls and
        # reboot-ran pushes reuse connections instead of re-handshaking mTLS
        # every round; created in start(), closed in stop().
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop = asyncio.Event()
        # @reboot one-shots THIS node has run as the elected owner (plus any
        # learned via push), gossiped so peers retire their matching
        # deferred jobs without re-running them on failover. Scoped to the
        # current job-set: cleared when our job_set_id changes (see
        # _poll_all), so a config change cannot carry a stale "already ran".
        self._ran_reboot_jobs: set[str] = set()
        self._ran_jobs_job_set_id: Optional[str] = None
        # completed peer-poll rounds since this manager was built. A rebuilt
        # manager mints a fresh instance_id, so peers cannot attest it back
        # until they have re-polled it (~1-2 intervals); this counter bounds
        # the convergence hold view_settled() places on the never-skip
        # available_* gates during that window.
        self._poll_rounds = 0
        # emit-once latch for the degenerate 2-of-2 self-listing warning
        # (see _maybe_warn_degenerate_self_listing).
        self._warned_degenerate_self = False
        # permanent-True latch for view_settled(): set once settling can no
        # longer revert (no UNKNOWN peer and >= _SETTLE_ROUNDS completed
        # rounds).
        self._view_settled_latched = False
        # host -> (etag, record_success keyword set) of the last full /peer
        # body absorbed, driving conditional re-polls (see _observe_peer).
        # Content-addressed, so safe to keep across failed rounds: a later
        # match still proves identical content, and a restarted peer's fresh
        # instance_id changes its payload so a stale tag never matches.
        # Bounded at one entry per configured peer.
        self._peer_observation_cache: dict[
            str, "tuple[str, dict[str, Any]]"
        ] = {}
        # the scheduler's per-job run-summary snapshot callable, piggybacked
        # on the /peer response for the fleet view (installed by
        # Cron.start_stop_cluster before start(); None until then, and /peer
        # then simply advertises no summaries).
        self._job_summaries_provider: Optional[
            Callable[[], dict[str, Any]]
        ] = None
        # our own whole-node CPU/memory provider. Installing it makes the
        # local /cluster + /fleet self readouts work even when not shared;
        # _share_node_stats separately gates advertising it to peers.
        self._node_stats_provider: Optional[
            Callable[[], Optional[dict[str, Any]]]
        ] = None
        self._share_node_stats = False
        # Memoized election-derived results (one dict for all of them,
        # function name -> value), valid only for _derived_cache_key; see
        # _memoized_derived / _derived_state_key.
        self._derived_cache: dict[str, Any] = {}
        self._derived_cache_key: Optional["tuple"] = None
        # The last (state key, monotonic build time, etag, body bytes) built
        # by _handle_peer, re-served while the key matches and the TTL holds;
        # the encoded body is cached because the payload dict is frozen for
        # the entry's life and only the bytes are ever served.
        self._peer_response_cache: Optional[
            "tuple[Any, float, str, bytes]"
        ] = None
        # Per-source oversize observations from the last derive / advert
        # build ("bridge": the transitive-confirmation derive, "advert": the
        # quorate_vouched union): the full candidate count at the last
        # overflow of the advertisement cap, else 0.  Kept per source because
        # the two report DIFFERENT counts, and a shared scalar let the
        # operator's own /cluster read cascade into the derive and zero the
        # flag the advert build had just set.  The /cluster
        # `candidates_truncated` flag reads the max (see the property).
        self._candidates_trunc_seen: dict[str, int] = {}
        # last-logged oversize count per source: the warning's rate limiter,
        # deliberately distinct from the view cells above (see
        # _note_candidates_truncated).
        self._candidates_trunc_logged: dict[str, bool] = {}

    def _derived_state_key(self) -> "tuple":
        """The key every memoized election-derived result is valid under.

        Two kinds of input: the peer table, covered by the process-wide
        PeerState mutation generation (every field write bumps it via
        :meth:`PeerState.__setattr__`, so no mutation path can be missed,
        including replacing ``self.view`` wholesale); and a handful of
        identity scalars, all assigned only in ``__init__`` today but
        carried in the key as cheap insurance against an in-place
        reassignment ever serving stale results.
        """
        return (
            PeerState._mutation_generation,
            self._peer_count,
            self._elect_leader,
            self.distribution,
            self.node_name,
            self.instance_id,
        )

    def set_job_summaries_provider(
        self, provider: Callable[[], dict[str, Any]]
    ) -> None:
        self._job_summaries_provider = provider

    def set_node_stats_provider(
        self,
        provider: Callable[[], Optional[dict[str, Any]]],
        share: bool = True,
    ) -> None:
        self._node_stats_provider = provider
        self._share_node_stats = share

    def _local_node_stats(self) -> Optional[dict[str, Any]]:
        """This node's own whole-node CPU/memory, or ``None``.

        Trusted local input, emitted as-is (small fixed shape, no cap).
        ``None`` when no provider or the sampler returned nothing. Whether
        it is also gossiped to peers is gated by ``_share_node_stats`` (see
        :meth:`_advertised_node_stats`).
        """
        provider = self._node_stats_provider
        if provider is None:
            return None
        return provider()

    def _advertised_node_stats(self) -> Optional[dict[str, Any]]:
        """Our node stats for the outgoing /peer response, or ``None``.

        ``None`` unless sharing is on. A reading never enters the /peer
        body: it rides the :data:`NODE_STATS_HEADER` response header, and
        ``None`` means no header at all (absence is the signal).
        """
        if not self._share_node_stats:
            return None
        return self._local_node_stats()

    def _advertised_job_summaries(
        self,
    ) -> "tuple[dict[str, Any], bool]":
        """Our own gossiped per-job summaries: ``(block, truncated)``.

        The provider's snapshot is trusted local input, so only the emit-side
        caps apply: entries beyond MAX_ADVERTISED_JOB_SUMMARIES are dropped
        (deterministically, by sorted name, so the advertised subset is
        stable across rounds) and over-long names are skipped. Truncation is
        flagged so the fleet view can label this node's data as partial.
        """
        provider = self._job_summaries_provider
        if provider is None:
            return {}, False
        summaries = provider()
        names = [
            name for name in summaries if len(name) <= MAX_JOB_SUMMARY_NAME_LEN
        ]
        truncated = len(names) < len(summaries)
        if len(names) > MAX_ADVERTISED_JOB_SUMMARIES:
            # sorted so the surviving subset is stable across rounds; entry
            # order is otherwise meaningless (the ETag hashes a sort_keys
            # projection and the dashboard sorts its rows).
            names = sorted(names)[:MAX_ADVERTISED_JOB_SUMMARIES]
            truncated = True
        if not truncated:
            # the provider builds a fresh dict per call and consumers only
            # read it, so the untruncated snapshot is returned whole
            return summaries, False
        return {name: summaries[name] for name in names}, truncated

    # --- the mTLS /peer server -------------------------------------------

    def _peer_payload(self) -> dict[str, Any]:
        """The full /peer response body (see :meth:`_handle_peer`)."""
        job_summaries, summaries_truncated = self._advertised_job_summaries()
        my_id = self.get_job_set_id()
        payload: dict[str, Any] = {
            "node_name": self.node_name,
            "job_set_id": my_id,
            "scheme_version": SCHEME_VERSION,
            "instance_id": self.instance_id,
            # our declared cluster size (len(peers)+1): a peer declaring a
            # different N treats it as a conflict and fails Leader closed
            # (see conflicting_sizes).
            "cluster_size": self.cluster_size(),
            # our coordination policy: not in the fingerprint, so a
            # divergence is a conflict (see conflicting_policies).
            "distribution": self.distribution,
            "elect_leader": self._elect_leader,
            # our current observations: mutual agreement plus transitive
            # duplicate detection; see ClusterView.local_members.
            "members": self.view.local_members(
                self.node_name, self.instance_id
            ),
            # @reboot one-shots already run in the cluster, so a poller can
            # retire its matching deferred job without re-running it. Capped
            # so an inflated upstream set cannot push this response past the
            # byte cap (reboot_ran() still uses the full union).
            "ran_reboot_jobs": sorted(self.advertised_ran_jobs(my_id))[
                :MAX_ADVERTISED_REBOOT_JOBS
            ],
            # the peers we *mutually* agree with: a poller's only sound
            # evidence that a transitively-reached node is itself quorate
            # (see _bridge_candidates). Distinct from the one-directional
            # ``agreed`` flags in ``members``.
            "mutual_agreeing": sorted(self._agreeing_peer_names()),
            # the names WE can confirm are quorate (our _eligible_candidates):
            # a poller folds these, not the raw mutual_agreeing, into its
            # spread Leader-path owner set so it only defers a job to a node
            # vouched able to run it (see _unconfirmed_contenders). Capped
            # (see MAX_ADVERTISED_CANDIDATE_NAMES).
            "quorate_vouched": self._capped_vouched(),
            # per-job run summaries for the poller's fleet view.
            # Observability only, never an election input; capped.
            "job_summaries": job_summaries,
            "job_summaries_truncated": summaries_truncated,
        }
        # node stats deliberately do NOT ride this body: they travel as the
        # NODE_STATS_HEADER sidecar so live values never roll the body's
        # ETag (keeps the idle-304 optimisation).
        return payload

    @staticmethod
    def _stable_job_summaries(
        job_summaries: dict[str, Any], now_epoch: float
    ) -> dict[str, Any]:
        """The hash-stable (change-relevant) form of a job_summaries block.

        The ``scheduled_in`` normalisation documented on :meth:`_peer_etag`,
        factored out so :meth:`_handle_peer` builds it once per cached
        (payload, etag) pair.
        """
        stable: dict[str, Any] = {}
        for name, entry in job_summaries.items():
            # copy the entry, then rewrite the one key, so no per-key
            # Python compare runs (an entry without scheduled_in stays
            # without it)
            copy = dict(entry)
            if "scheduled_in" in copy:
                value = copy["scheduled_in"]
                copy["scheduled_in"] = (
                    round(now_epoch + value)
                    if isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    else None
                )
            stable[name] = copy
        return stable

    @staticmethod
    def _encode_peer_body(payload: dict[str, Any]) -> bytes:
        """Serialise a /peer payload to the bytes we will send.

        orjson-accelerated with a stdlib fallback. The ETag hashes a
        canonical projection of the payload (see :meth:`_peer_etag`), NOT
        these bytes, so the encoder choice cannot affect 304 matching.
        """
        try:
            return _json.dumps_bytes(payload)
        except _json.UnsupportedValue:
            return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _peer_etag(
        payload: dict[str, Any],
        now_epoch: float,
        stable_summaries: Optional[dict[str, Any]] = None,
    ) -> str:
        """A strong ETag for ``payload``: a hash of change-relevant content.

        Deterministic across rounds while nothing real changed. One field
        needs normalising: ``scheduled_in`` is a live countdown, different
        every round, yet DROPPING it would let pollers' derived countdowns
        freeze at zero after a fire; so the hash replaces it with the
        ABSOLUTE next-fire time rounded to whole seconds, constant between
        fires and rolling exactly when the schedule does (pollers re-derive
        the countdown, see ``_aged_job_summaries``). Rounding can rarely
        cost one spurious full body, never a wrong 304.
        ``stable_summaries`` lets a caller pass an already-normalised block.
        """
        stable = dict(payload)
        stable["job_summaries"] = (
            stable_summaries
            if stable_summaries is not None
            else ClusterManager._stable_job_summaries(
                payload["job_summaries"], now_epoch
            )
        )
        digest = hashlib.sha256(
            json.dumps(stable, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        return '"{}"'.format(digest)

    async def _handle_peer(self, request: web.Request) -> web.Response:
        # Build-once caching of the (payload, etag, body bytes) triple, so a
        # steady-state 304 costs no election cascade or hashing. Bounded two
        # ways: the state key (_derived_state_key plus the components below)
        # covers every election-relevant input, so election staleness is
        # exactly zero; PEER_RESPONSE_CACHE_TTL bounds inputs the key cannot
        # observe (chiefly the summaries provider snapshot). The cached
        # payload dict is shared across requests and must never be mutated
        # per request; per-request live data (node stats) rides a response
        # header. gzip stays per-response: it branches on Accept-Encoding.
        now_mono = time.monotonic()
        state_key = (
            self._derived_state_key(),
            # the live job-set id: a config reload changes it immediately
            # and the advertised ran-set gating depends on it.
            self.get_job_set_id(),
            # the @reboot-run state feeding advertised_ran_jobs(): scoping
            # id plus cardinality. Cardinality catches every add and clear;
            # the one same-length rewrite (a truncation swapping names) is
            # bounded by the TTL. Peer-learned ran-sets are already covered
            # by the generation in _derived_state_key().
            self._ran_jobs_job_set_id,
            len(self._ran_reboot_jobs),
            # the summaries provider itself (by identity): installing or
            # swapping one must invalidate at once.
            self._job_summaries_provider,
        )
        cached = self._peer_response_cache
        if (
            cached is not None
            and cached[0] == state_key
            and now_mono - cached[1] <= PEER_RESPONSE_CACHE_TTL
        ):
            etag, body_bytes = cached[2], cached[3]
        else:
            payload = self._peer_payload()
            body_bytes = self._encode_peer_body(payload)
            if len(body_bytes) > MAX_PEER_RESPONSE_BYTES:
                # Last-resort degradation (should be unreachable: every
                # re-advertised set is capped). An oversized body would cost
                # this node its place in the quorum, so drop job_summaries:
                # the largest, observability-only field. Compare the
                # UNCOMPRESSED length; the poller's cap applies to the
                # decompressed stream.
                oversized = len(body_bytes)
                if payload.get("job_summaries"):
                    payload = dict(payload)
                    payload["job_summaries"] = {}
                    payload["job_summaries_truncated"] = True
                    body_bytes = self._encode_peer_body(payload)
                if len(body_bytes) > MAX_PEER_RESPONSE_BYTES:
                    # Shedding was a no-op (no provider installed) or was
                    # not enough. Nothing else here is observability-only,
                    # so say plainly that peers will reject this body and
                    # drop us rather than log a drop that did not help.
                    logger.error(
                        "/peer response is %d bytes, over the %d byte cap "
                        "peers enforce, and shedding the fleet view left "
                        "%d: peers will record this node oversized and "
                        "drop it from their quorum",
                        oversized,
                        MAX_PEER_RESPONSE_BYTES,
                        len(body_bytes),
                    )
                else:
                    logger.warning(
                        "/peer response was %d bytes, over the %d byte cap "
                        "peers enforce: dropped job_summaries from the "
                        "fleet view (now %d bytes)",
                        oversized,
                        MAX_PEER_RESPONSE_BYTES,
                        len(body_bytes),
                    )
            now_epoch = time.time()
            # etag computed on the payload actually sent: a degraded body
            # must never carry the full body's tag (which would 304 a poller
            # into replaying a body it never received).
            etag = self._peer_etag(
                payload,
                now_epoch,
                self._stable_job_summaries(
                    payload["job_summaries"], now_epoch
                ),
            )
            self._peer_response_cache = (state_key, now_mono, etag, body_bytes)
        headers = {"ETag": etag}
        # The node-stats sidecar rides a response header so it never rolls
        # the ETag; attached to the 200 AND the 304 below. No header when
        # not sharing: absence is the signal.
        node_stats = self._advertised_node_stats()
        if node_stats is not None:
            headers[NODE_STATS_HEADER] = json.dumps(
                node_stats, separators=(",", ":")
            )
        # Conditional exchange: an unchanged body answers a matching echoed
        # tag with a bodyless 304 (see _observe_peer). Exact-match only; a
        # foreign If-None-Match shape simply gets the full body (a safe,
        # slightly-wasteful degradation).
        if request.headers.get("If-None-Match") == etag:
            return web.Response(status=304, headers=headers)
        resp = web.Response(
            body=body_bytes, headers=headers, content_type="application/json"
        )
        # gzip is picked EXPLICITLY when the client advertises it: bare
        # enable_compression() lets aiohttp negotiate deflate first,
        # contradicting the documented gzip exchange. The size floor skips
        # bodies where the CPU spend outweighs the saved bytes.
        if len(body_bytes) >= MIN_COMPRESS_BYTES:
            if "gzip" in request.headers.get("Accept-Encoding", "").lower():
                resp.enable_compression(web.ContentCoding.gzip)
            else:
                resp.enable_compression()
        return resp

    async def _handle_reboot_ran(self, request: web.Request) -> web.Response:
        """Receive an eager push of @reboot jobs a peer just ran.

        The pull-poll already carries this set; a push shrinks the window in
        which an owner runs a one-shot and dies unobserved (a new leader
        would re-run it). Best-effort: accepted only when the sender's
        job_set_id matches ours; a hostile CA-vouched peer fabricating
        "ran X" is the out-of-scope Byzantine class.
        """
        try:
            raw, too_large = await asyncio.wait_for(
                _read_capped(request, MAX_PEER_RESPONSE_BYTES),
                self.config["connectTimeout"],
            )
        except asyncio.TimeoutError:
            # a slow/stalled body read from a hung peer: bound by the same
            # per-request timeout the client side uses (_read_capped bounds
            # bytes, not time).
            return web.Response(status=408)
        if too_large:
            return web.Response(status=413)
        try:
            data = _json.loads(raw)
        except (ValueError, RecursionError):
            # malformed push from a buggy/hostile peer: reject cleanly
            # rather than 500 on an escaped exception.
            return web.Response(status=400)
        my_id = self.get_job_set_id()
        if isinstance(data, dict) and data.get("job_set_id") == my_id:
            # Reconcile our recorded runs to the current job set *before*
            # absorbing, mirroring _poll_all: otherwise names arriving just
            # after a reload would be seeded under the stale id and wiped by
            # the next poll.
            self._reconcile_job_set_id(my_id)
            self._ran_reboot_jobs |= _parse_str_list(
                data.get("names"),
                max_len=MAX_REBOOT_JOB_NAME_LEN,
                max_items=MAX_ADVERTISED_REBOOT_JOBS,
            )
            # Bound the PERSISTENT set so a peer cannot grow it (and the /peer
            # response that re-advertises it) past the byte cap and collapse
            # quorum cluster-wide. Dropping excess marks only risks rerunning a
            # one-shot (the documented PreferLeader envelope), never an outage.
            if len(self._ran_reboot_jobs) > MAX_ADVERTISED_REBOOT_JOBS:
                self._ran_reboot_jobs = set(
                    sorted(self._ran_reboot_jobs)[:MAX_ADVERTISED_REBOOT_JOBS]
                )
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
            self._runner = runner
            # one session for the manager's lifetime: peer polls and
            # reboot-ran pushes reuse its kept-alive mTLS connections
            # instead of re-handshaking every round.
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=self.config["connectTimeout"]
                )
            )
            logger.info(
                "cluster: node %r serving mTLS /peer on %s, polling %d "
                "peer(s) every %ds",
                self.node_name,
                self.config["listen"],
                len(self.config["peers"]),
                self.config["interval"],
            )
            # Run one full poll round up front so the never-skip available_*
            # gates and reboot_ran() reflect a real read BEFORE the first
            # spawn_jobs (mirrors the lease backends' inline store round):
            # without it every PreferLeader job runs on every node at boot
            # and deferred @reboot one-shots re-run. Best-effort and bounded
            # by connectTimeout; a failed round records peers unreachable.
            try:
                await self._poll_all()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive, as _poll_loop
                logger.exception("cluster: initial peer poll round failed")
            self._poll_task = asyncio.create_task(self._poll_loop())
        except BaseException:
            # a bad listen address, bind failure, session/task creation
            # failure, or cancellation must not leak the half-started
            # runner/session/task.
            if self._poll_task is not None:
                self._poll_task.cancel()
                self._poll_task = None
            if self._session is not None:
                await self._session.close()
                self._session = None
            await runner.cleanup()
            self._runner = None
            raise

    async def stop(self) -> None:
        self._stop.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._session is not None:
            # close after the poll task is cancelled, so no in-flight request
            # is using it; before the runner, mirroring teardown order.
            await self._session.close()
            self._session = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def tls_files_changed(self) -> bool:
        """Whether the CA/cert/key files differ from what we loaded at startup.

        True after an in-place cert rotation, so the daemon can restart the
        manager to rebuild the once-built SSL contexts. See
        :data:`_TLS_SIGNATURE_KEYS`.
        """
        return (
            tlsutil.tls_file_signature(self.config["tls"], _TLS_SIGNATURE_KEYS)
            != self._tls_signature
        )

    def tls_files_loadable(self) -> bool:
        """Whether the *current* on-disk CA/cert/key load into contexts now.

        A side-effect-free dry-run of the rebuild
        :meth:`cronstable.cron.Cron.start_stop_cluster` attempts on a cert
        rotation. ``False`` on a missing or half-written file, so the caller
        keeps the running manager (still serving the valid old cert) until
        the rotation settles.
        """
        tls = self.config["tls"]
        try:
            build_client_ssl_context(tls)
            build_server_ssl_context(tls)
        except (OSError, ssl.SSLError):
            return False
        return True

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

    def _reconcile_job_set_id(self, my_id: str) -> None:
        """Align the recorded-@reboot-runs set with the current job set.

        Clears ``_ran_reboot_jobs`` when our job set CHANGED (a reload):
        old-set runs no longer apply, so a still-deferred @reboot may run
        again, the safe direction. The first observation only establishes
        the id. mark_reboot_ran / _handle_reboot_ran also call this
        immediately before adding, so their entries land under the live id
        and the loop's next same-id reconcile cannot discard them.
        Idempotent and await-free, so the interleaving is safe.
        """
        if (
            self._ran_jobs_job_set_id is not None
            and my_id != self._ran_jobs_job_set_id
        ):
            self._ran_reboot_jobs.clear()
        self._ran_jobs_job_set_id = my_id

    async def _poll_all(self) -> None:
        my_id = self.get_job_set_id()
        self._reconcile_job_set_id(my_id)
        peers = self.config["peers"]
        session = self._session
        if not peers or session is None:
            # no peers to poll, or the manager is not running (a direct test
            # call): the reconcile above is the only work this round.
            return
        # return_exceptions so one peer raising an unexpected error (a bug;
        # network failures are handled inside _poll_peer) cannot abort the
        # round and detach the other peers' coroutines.
        results = await asyncio.gather(
            *(self._poll_peer(session, peer["host"], my_id) for peer in peers),
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
        # one full round completed: every configured peer now carries a real
        # observation (success or failure); feeds view_settled()'s bound.
        self._poll_rounds += 1
        # Re-run the degenerate-self check with the round's full information:
        # at the SELF transition a multi-homed duplicate may not be deduped
        # yet, so a transition-only check would never fire.
        self._maybe_warn_degenerate_self_listing()

    async def _poll_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        """Observe one peer, then log any status transition (once per change).

        The observation lives in :meth:`_observe_peer`; this wrapper diffs
        status across it so changes log at the manager seam and
        ``ClusterView`` stays pure (no I/O, no logging).
        """
        prev_status = self.view.peers[host].status
        await self._observe_peer(session, host, my_id)
        self._log_peer_status_change(host, prev_status)

    def _log_peer_status_change(self, host: str, prev: str) -> None:
        """Log a peer's status transition once, where the manager has a seam.

        Cert failures are the highest-value signal: a botched in-place
        rotation otherwise turns peers ``untrusted`` one by one in silence
        until quorum breaks. unknown-to-unreachable first contacts are not
        logged (no startup burst); a first successful contact logs once.
        """
        peer = self.view.peers[host]
        new = peer.status
        if new == prev:
            return
        if new == STATUS_UNTRUSTED:
            logger.warning(
                "cluster: peer %s is untrusted -- TLS/cert verification "
                "failed: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_UNREACHABLE and prev not in _STALE_STATUSES:
            # only warn when a previously-reached peer drops; a never-
            # contacted peer is startup noise.
            logger.warning(
                "cluster: peer %s became unreachable: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_DRIFTED:
            logger.warning(
                "cluster: peer %s drifted -- its job-set id has differed for "
                ">= driftAfter rounds (or reports a different fingerprint "
                "scheme)",
                host,
            )
        elif new == STATUS_CONFLICT:
            # the cluster-wide conflict is logged loudly by
            # Cron._log_cluster_role; a per-peer line at INFO just pinpoints
            # which peer collided.
            logger.info(
                "cluster: peer %s reports a duplicate nodeName: %s",
                host,
                peer.last_error,
            )
        elif new == STATUS_SELF:
            # a self-listing config-time dedup could not catch (see
            # config._is_self_listed) was just identified by its self-poll.
            # The degenerate 2-real-node case warns prominently; any other
            # self-listing logs once at INFO. The degenerate check also
            # re-runs at the end of every poll round (see _poll_all).
            if not self._maybe_warn_degenerate_self_listing():
                logger.info(
                    "cluster: peer %s is this node itself (a self-listing); "
                    "excluded from the cluster size",
                    host,
                )
        elif new == STATUS_AGREED and prev != STATUS_SELF:
            logger.info("cluster: peer %s now agreed", host)

    def _maybe_warn_degenerate_self_listing(self) -> bool:
        """Warn once when a runtime-identified self-listing leaves the
        effective ``electLeader`` cluster at 2 real nodes: the degenerate
        quorum-2-of-2 mode the config-time size==2 refusal forbids.

        Returns whether the warning fired now (the caller then skips its
        benign INFO line). Evaluated at the SELF transition AND at the end
        of every poll round: at the transition a multi-homed duplicate may
        not be deduped yet.
        """
        if self._warned_degenerate_self:
            return False
        if not self._elect_leader:
            return False
        self_hosts = [
            host
            for host, peer in self.view.peers.items()
            if peer.status == STATUS_SELF
        ]
        if not self_hosts or self.cluster_size() != 2:
            return False
        self._warned_degenerate_self = True
        logger.warning(
            "cluster: peer %s is this node itself (a self-listing "
            "not recognisable at config load), so the effective "
            "electLeader cluster is 2 nodes with a quorum of 2 -- "
            "the degenerate mode the 2-node refusal exists to "
            "forbid: BOTH nodes must be up for either to run, so "
            "any single failure stops all Leader jobs cluster-wide "
            "(strictly worse than a single replica). Remove the "
            "self entry from cluster.peers, or grow the cluster to "
            "3+ nodes.",
            ", ".join(self_hosts),
        )
        return True

    async def _observe_peer(
        self, session: aiohttp.ClientSession, host: str, my_id: str
    ) -> None:
        url = self._peer_urls[host]
        # Conditional re-poll: echo the ETag of the last full body this host
        # served us, so an unchanged peer can answer with a bodyless 304
        # instead of the full O(members + jobs) JSON (see _handle_peer).
        cached = self._peer_observation_cache.get(host)
        status = 0
        response_etag: Optional[str] = None
        raw_stats_header: Optional[str] = None
        raw, too_large = b"", False
        try:
            # allow_redirects=False: a legitimate peer never redirects;
            # following one would let a CA-vouched-but-hostile peer pivot us
            # into an attacker-chosen target (SSRF) or a plaintext http://
            # downgrade where the mTLS client context no longer applies.
            async with session.get(
                url,
                ssl=self._client_ssl,
                allow_redirects=False,
                headers=(
                    {"If-None-Match": cached[0]}
                    if cached is not None
                    else None
                ),
            ) as resp:
                status = resp.status
                # the node-stats sidecar rides the header on the 200 and the
                # 304 alike; capture before the body handling diverges.
                raw_stats_header = resp.headers.get(NODE_STATS_HEADER)
                if status != 304:
                    resp.raise_for_status()
                    raw, too_large = await _read_capped(
                        resp, MAX_PEER_RESPONSE_BYTES
                    )
                    response_etag = resp.headers.get("ETag")
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
        # Timestamp at RECEIPT, not before the request: the peer computed
        # its countdowns while serving, so the pre-request instant would
        # overstate the snapshot's age by the round-trip latency, and a 304
        # replay would carry that skew for as long as the tag holds.
        now = datetime.datetime.now(_UTC)
        # parse the sidecar once for both paths; missing or malformed
        # degrades to None and must never fail the poll.
        peer_node_stats = _parse_node_stats_header(raw_stats_header)
        if status == 304:
            if cached is None:
                # a 304 answers a conditional request, and we sent none: a
                # buggy or hostile peer. Treat it as a failed observation
                # rather than inventing a body we do not hold.
                self.view.record_failure(
                    host,
                    "unsolicited 304 to an unconditional /peer poll",
                    untrusted=False,
                )
                return
            # The peer attests its payload is exactly the one we cached, so
            # replay that observation with a fresh timestamp and the LIVE
            # my_id: every gate advances as if the identical body had been
            # re-sent. The cached kwargs carry the ORIGINAL job_summaries_at
            # so countdowns keep ageing; node stats are NOT cached (this
            # round's header supplies them), so a replay can never resurrect
            # a stale reading.
            self.view.record_success(
                host,
                my_id=my_id,
                now=now,
                my_name=self.node_name,
                my_instance=self.instance_id,
                peer_node_stats=peer_node_stats,
                **cached[1],
            )
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
            data = _json.loads(raw)
        except (ValueError, RecursionError):
            # unparseable JSON or a deeply-nested document (RecursionError
            # is NOT a ValueError): a failed observation; letting it escape
            # would skip record_failure and freeze the peer's stale view.
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
        fields: dict[str, Optional[str]] = {}
        for key in (
            "node_name",
            "job_set_id",
            "scheme_version",
            "instance_id",
            "distribution",
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
            # Bound length, reject the empty string ("".isprintable() is
            # True) and control characters: these strings are reflected
            # verbatim into /cluster JSON and log lines (payload bloat /
            # log injection), and an empty node_name would count toward
            # quorum as a peer no election fold can name (the set-shaped
            # fields drop empties the same way).
            if value is not None and (
                not value
                or len(value) > MAX_PEER_FIELD_LEN
                or not value.isprintable()
            ):
                self.view.record_failure(
                    host,
                    "malformed /peer response: {!r} is empty, over {} "
                    "chars or contains control characters".format(
                        key, MAX_PEER_FIELD_LEN
                    ),
                    untrusted=False,
                )
                return
            fields[key] = value
        # cluster_size is an int (bool is an int subclass; reject
        # explicitly). None (a peer too old) skips the size check for that
        # peer: a missing declared_size forgoes a fail-CLOSED guard, the
        # version-skew residual.
        size = data.get("cluster_size")
        if size is not None and (
            not isinstance(size, int) or isinstance(size, bool) or size < 1
        ):
            self.view.record_failure(
                host,
                "malformed /peer response: cluster_size is not a positive "
                "integer",
                untrusted=False,
            )
            return
        # elect_leader is a bool; None (a peer too old) forgoes the policy-
        # conflict guard for that peer (the safe direction). Reject a
        # non-bool before it reaches conflicting_policies.
        elect = data.get("elect_leader")
        if elect is not None and not isinstance(elect, bool):
            self.view.record_failure(
                host,
                "malformed /peer response: elect_leader is not a boolean",
                untrusted=False,
            )
            return
        # The validated observation, built once: record_success consumes it
        # now and _peer_observation_cache keeps it for 304 replays.
        # my_id/now/my_name/my_instance stay OUT (supplied live per call);
        # peer_node_stats stays out (per-RESPONSE header; caching it would
        # let a replay resurrect a stale reading).
        observation: dict[str, Any] = {
            "peer_name": fields["node_name"],
            "peer_id": fields["job_set_id"],
            "peer_scheme": fields["scheme_version"],
            "peer_instance": fields["instance_id"],
            "peer_members": _parse_members(
                data.get("members"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # whether the peer sent a members list; a legacy peer cannot
            # confirm it sees us, so _agreeing_peers falls back to one-
            # directional agreement. A non-list is "not reported".
            "peer_reports_members": isinstance(data.get("members"), list),
            "peer_ran_reboot_jobs": _parse_str_list(
                data.get("ran_reboot_jobs"),
                max_len=MAX_REBOOT_JOB_NAME_LEN,
                max_items=MAX_ADVERTISED_REBOOT_JOBS,
            ),
            "peer_size": size,
            "peer_distribution": fields["distribution"],
            "peer_elect_leader": elect,
            # An older build omitting the field or an empty set both parse
            # to an empty set: not confirmed quorate, so _eligible_candidates
            # won't elect it (the PeerState default None reads the same).
            "peer_mutual_agreeing": _parse_str_list(
                data.get("mutual_agreeing"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # the peer's vouch of which nodes it confirms quorate. An older
            # build or empty set vouches no transitive owner: lean toward
            # running, never a zero-run. Capped like the other absorbed sets.
            "peer_quorate_vouched": _parse_str_list(
                data.get("quorate_vouched"),
                max_len=MAX_PEER_FIELD_LEN,
                max_items=MAX_MEMBER_ENTRIES,
            ),
            # rebuilt field-by-field from the untrusted payload (see
            # _parse_job_summaries). None (absent/malformed) keeps any
            # previously-absorbed snapshot rather than blanking the node.
            "peer_job_summaries": _parse_job_summaries(
                data.get("job_summaries")
            ),
            "peer_job_summaries_truncated": (
                data.get("job_summaries_truncated") is True
            ),
            # a 304 replay passes this through unchanged, so fleet_view ages
            # countdowns from the true receipt time.
            "peer_job_summaries_at": now,
        }
        self.view.record_success(
            host,
            my_id=my_id,
            now=now,
            my_name=self.node_name,
            my_instance=self.instance_id,
            peer_node_stats=peer_node_stats,
            **observation,
        )
        # Remember (etag -> observation) for conditional re-polls. Only a
        # sane tag is stored (it is echoed back every round); junk or a
        # tagless response (an older build) clears the entry so we poll
        # unconditionally.
        if (
            response_etag is not None
            and len(response_etag) <= MAX_PEER_ETAG_LEN
            and response_etag.isprintable()
        ):
            self._peer_observation_cache[host] = (response_etag, observation)
        else:
            self._peer_observation_cache.pop(host, None)

    # --- deferred @reboot "already ran" gossip ---------------------------

    def advertised_ran_jobs(self, my_id: Optional[str] = None) -> set[str]:
        """@reboot one-shots known to have run under our *current* job set.

        Our own runs plus those reported by agreeing peers; re-advertising
        what we learned makes the fact survive the original owner's death
        (one-hop gossip). A peer's contribution is gated on its last
        reported ``job_set_id`` matching our LIVE id, not merely on cached
        ``STATUS_AGREED``: after a local reload a peer's stale AGREED
        ran-set could otherwise retire a redefined @reboot one-shot without
        running the new definition. Mirrors :meth:`_handle_reboot_ran`'s
        gate on pushes. ``my_id`` is the live job-set id, fetched when not
        supplied.
        """
        if my_id is None:
            my_id = self.get_job_set_id()
        # Gate our OWN recorded runs on the live id too: they were recorded
        # under _ran_jobs_job_set_id, which the poll reconciles only lazily;
        # between a reload and the next poll /peer would otherwise advertise
        # the old set under the NEW id, retiring an agreed peer's redefined
        # @reboot one-shot without running it. (None = no runs recorded yet.)
        ran_id = self._ran_jobs_job_set_id
        jobs = (
            set(self._ran_reboot_jobs)
            if ran_id is None or ran_id == my_id
            else set()
        )
        for peer in self.view.peers.values():
            if (
                peer.status == STATUS_AGREED
                and peer.job_set_id == my_id
                and peer.ran_reboot_jobs
            ):
                jobs |= peer.ran_reboot_jobs
        return jobs

    def reboot_ran(self, job_name: str) -> bool:
        """Whether ``job_name`` already ran in the cluster (this config).

        The same question :meth:`advertised_ran_jobs` answers, under
        identical gates, as a membership test that stops at the first hit
        instead of materialising the whole union.
        """
        my_id = self.get_job_set_id()
        ran_id = self._ran_jobs_job_set_id
        if (
            ran_id is None or ran_id == my_id
        ) and job_name in self._ran_reboot_jobs:
            return True
        for peer in self.view.peers.values():
            if (
                peer.status == STATUS_AGREED
                and peer.job_set_id == my_id
                and peer.ran_reboot_jobs
                and job_name in peer.ran_reboot_jobs
            ):
                return True
        return False

    async def mark_reboot_ran(self, job_name: str) -> None:
        """Record that we ran ``job_name`` as owner, and eagerly tell peers.

        The push is best-effort (the periodic pull carries the same set as a
        backstop); it just shrinks the window in which we could run the job and
        then die before any peer observed it.
        """
        # Reconcile to the live job set BEFORE adding, so the entry lands
        # under the current id and the poll loop waking during the push
        # cannot clear() it. _reconcile_job_set_id is await-free, so
        # reconcile+add cannot interleave.
        self._reconcile_job_set_id(self.get_job_set_id())
        self._ran_reboot_jobs.add(job_name)
        await self._push_reboot_ran()

    async def _push_reboot_ran(self) -> None:
        peers = self.config["peers"]
        my_id = self.get_job_set_id()
        # capped like the /peer serialization so a push body cannot exceed the
        # receiver's MAX_PEER_RESPONSE_BYTES (else rejected as oversized).
        names = sorted(self.advertised_ran_jobs(my_id))[
            :MAX_ADVERTISED_REBOOT_JOBS
        ]
        session = self._session
        if not peers or not names or session is None:
            return
        payload = {"job_set_id": my_id, "names": names}
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
        payload: dict[str, Any],
    ) -> None:
        url = self._reboot_ran_urls[host]
        try:
            # allow_redirects=False (see _observe_peer): a redirect would
            # replay this payload to an attacker-chosen target over a
            # possibly-plaintext connection.
            async with session.post(
                url,
                json=payload,
                ssl=self._client_ssl,
                allow_redirects=False,
            ) as resp:
                resp.raise_for_status()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            # best-effort: a delivery failure is fine, the periodic pull-poll
            # carries the same set; don't let it disturb the run loop.
            pass

    # --- leader election --------------------------------------------------

    @_memoized_derived
    def cluster_size(self) -> int:
        """Total number of cluster members.

        ``len(peers) + 1``, minus any entry that turns out to be THIS node
        listed in its own peer list (status ``self``). The config-load dedup
        (:func:`cronstable.config._is_self_listed`) catches literal and
        wildcard cases; this runtime subtraction is the backstop (e.g. a
        self-listing by FQDN), keeping N equal to what correctly-configured
        peers declare so a benign self-listing does not trip the size gate
        (see :meth:`conflicting_sizes`).
        """
        self_listed = sum(
            1
            for peer in self.view.peers.values()
            if peer.status == STATUS_SELF
        )
        # Two peer entries with the SAME observed instance_id (one node at
        # two addresses) are one fault domain; counting both inflates N and
        # quorum, silently enabling the degenerate 2-real-node mode. Count
        # each observed instance_id once; an unidentified entry (None)
        # counts (the safe, higher-quorum direction). The address->instance
        # binding is RETAINED while the peer is UNREACHABLE/UNTRUSTED:
        # dropping the dedup the moment the multi-homed node dies would
        # inflate quorum on every survivor for the whole outage. Self-
        # corrects: a successful poll of a reassigned address records the
        # new instance and stops the dedup.
        seen_instances: set[str] = set()
        duplicate_instances = 0
        for peer in self.view.peers.values():
            if peer.status == STATUS_SELF:
                continue
            if peer.instance_id is None:
                continue
            if peer.instance_id in seen_instances:
                duplicate_instances += 1
            else:
                seen_instances.add(peer.instance_id)
        return self._peer_count + 1 - self_listed - duplicate_instances

    def quorum(self) -> int:
        return quorum_size(self.cluster_size())

    def _our_declarations(self) -> dict[str, Any]:
        """Our own value for every coordination field a divergence gate reads.

        Keyed by the :class:`PeerState` attribute the peer's declaration
        lands in. The single definition of WHICH fields are divergence-gated:
        the :meth:`_agreeing_peers` exclude set iterates it, so adding a
        field here also stops us vouching a divergent peer to third nodes
        (see :func:`_declares_divergent`). The detectors still spell their
        own fields; ``test_declared_fields_gate_both_agreement_and_conflict``
        fences their coverage. Not memoized: three cheap reads, hoisted out
        of callers' peer loops.
        """
        return {
            "declared_size": self.cluster_size(),
            "declared_distribution": self.distribution,
            "declared_elect_leader": self._elect_leader,
        }

    @_memoized_derived
    def _agreeing_peers(self) -> list[PeerState]:
        """Peers we *mutually* agree with on job-set id, size, and policy.

        A peer counts only when both directions are confirmed: we see it
        AGREED and its last /peer response lists us (by ``instance_id``) as
        AGREED too. Mutuality keeps the quorum gate sound under asymmetric
        reachability (a one-way-linked pair can no longer both reach a
        majority), at the price of one extra poll round to converge.

        A peer that agrees on the job set but declares a different cluster
        size or policy is also excluded. Load-bearing: these names ARE the
        gossiped ``mutual_agreeing``, so a divergent node is never vouched
        to a third node that cannot see the divergence. Detection
        (:meth:`conflicting_sizes` / :meth:`conflicting_policies`) is
        independent, so the conflict still surfaces and fails closed. A
        peer too old to declare a field is not excluded: the safe direction.
        """
        # our side of every divergence-gated field, hoisted out of the loop
        # (see _our_declarations: its key set IS the exclude set below).
        my_declarations = self._our_declarations()
        agreeing: list[PeerState] = []
        # Dedup by instance_id: a node reachable at two listed addresses
        # must count ONCE toward the live set, or the quorum check could be
        # met by one physical peer counted twice. Mirrors cluster_size().
        seen_instances: set[str] = set()
        for peer in self.view.peers.values():
            if not (
                peer.status == STATUS_AGREED
                and peer.node_name is not None
                # mutual gate: count a current peer only if it sees us
                # AGREED too. A LEGACY peer (no members field) cannot report
                # this; fall back to one-directional agreement, or a new
                # node among legacy peers would halt every Leader job. The
                # node still won't defer to a legacy peer: the documented
                # lean-toward-running upgrade behaviour, not a halt.
                and (
                    _peer_sees_me_agreed(peer.members, self.instance_id)
                    or not peer.reports_members
                )
                # a peer declaring ANY gated field differently is dropped;
                # the gated fields are exactly the detectors' fields.
                and not any(
                    _declares_divergent(getattr(peer, field), mine)
                    for field, mine in my_declarations.items()
                )
            ):
                continue
            if peer.instance_id is not None:
                if peer.instance_id in seen_instances:
                    continue
                seen_instances.add(peer.instance_id)
            agreeing.append(peer)
        return agreeing

    @_memoized_derived
    def _agreeing_peer_names(self) -> list[str]:
        """Names of the peers we mutually agree with.

        See :meth:`_agreeing_peers`.
        """
        return [
            peer.node_name for peer in self._agreeing_peers() if peer.node_name
        ]

    @_memoized_derived
    def _bridge_candidates(self) -> list[str]:
        """Nodes we reach only *transitively* that we can confirm are quorate.

        Two nodes that never agree with each other can each still reach a
        quorum through shared members and would both elect themselves. For
        a node ``n`` we do not count directly, we tally the mutually
        agreeing peers that report ``n`` in their own ``mutual_agreeing``
        (witnessed two-way edges) and confirm ``n`` only when that tally
        plus ``n`` itself reaches a quorum: sound evidence ``n`` will run
        if elected, so the larger of two bridged would-be leaders defers.

        Mutual edges, not the one-way ``agreed`` flag, keep this live: a
        node merely reached one-way by a quorum is not quorate, and
        deferring to it would stand every node down. Adding candidates can
        only make this node defer, never lead more; the no-stand-down half
        holds only in a converged view (a since-isolated candidate can
        briefly pull the majority into a transient skip). Residuals: a
        too-thin bridge may double-run, nodes two gossip hops away are
        invisible, and convergence takes a poll interval.
        """
        agreeing = self._agreeing_peers()
        # nodes we already count directly: never "bridge" candidates
        direct = {self.node_name} | {
            peer.node_name for peer in agreeing if peer.node_name
        }
        quorum = self.quorum()
        # per transitively-discovered node, the set of our mutually-agreeing
        # peers that *also* mutually agree with it (a witnessed two-way edge)
        witnesses: dict[str, set[str]] = defaultdict(set)
        for peer in agreeing:
            witness = peer.node_name
            if witness is None:  # _agreeing_peers filters these out already
                continue
            for name in peer.mutual_agreeing or ():
                # ``name and`` matches _unconfirmed_contenders /
                # _available_contenders: all three folds spell the guard the
                # same way so a parser change cannot single one out.
                if name and name not in direct:
                    witnesses[name].add(witness)
        # confirmed quorate iff we witness >= quorum mutual agreers (the
        # witnesses plus the node itself). Sorted then capped: this set is
        # re-advertised as quorate_vouched, the one unbounded re-broadcast
        # path (see MAX_ADVERTISED_CANDIDATE_NAMES); slicing a sorted list
        # keeps the same lowest-names prefix on every node.
        confirmed = sorted(
            name for name, seen in witnesses.items() if len(seen) + 1 >= quorum
        )
        if len(confirmed) > MAX_ADVERTISED_CANDIDATE_NAMES:
            # a dropped tail name costs a ``spread`` co-owner; report it
            # rather than leaving a double-run to be inferred from history.
            self._note_candidates_truncated("bridge", len(confirmed))
            return confirmed[:MAX_ADVERTISED_CANDIDATE_NAMES]
        # clears this source's cell ONLY: the advert build owns its own,
        # so a /cluster read cascading into this derive can no longer
        # blank a truncation the advert side just observed.
        self._clear_candidates_truncated("bridge")
        return confirmed

    def _capped_vouched(self) -> list[str]:
        """The ``quorate_vouched`` set as advertised: sorted, then capped.

        The union of the direct and bridge halves of
        :meth:`_eligible_candidates` can exceed the cap even though the
        bridge half was capped at its derive, so the advert slices (and
        reports) again.
        """
        vouched = sorted(self._eligible_candidates())
        if len(vouched) > MAX_ADVERTISED_CANDIDATE_NAMES:
            self._note_candidates_truncated("advert", len(vouched))
            return vouched[:MAX_ADVERTISED_CANDIDATE_NAMES]
        self._clear_candidates_truncated("advert")
        return vouched

    @property
    def _candidates_truncated(self) -> int:
        """The /cluster ``candidates_truncated`` flag.

        The largest full count any source last saw overflow the cap, 0 when
        every source last fit. A max over per-source cells rather than one
        shared scalar, so the bridge derive clearing ITS observation cannot
        blank a truncation the advert build observed on the larger union.
        """
        # The advert cell is only rewritten when a /peer poll rebuilds the
        # response (_capped_vouched), so on a node nobody polls it would
        # otherwise report whatever the last build saw, in BOTH directions:
        # a latched overflow forever, or a stale 0 while the union has since
        # outgrown the cap and every future advert would drop a co-owner.
        # Re-derive it here either way. The bridge cell cannot go stale that
        # way (its derive refreshes it), and _eligible_candidates is memoized
        # per view generation, so this read is cheap.
        union = len(self._eligible_candidates())
        if union > MAX_ADVERTISED_CANDIDATE_NAMES:
            # through _note_ so a never-polled node still logs the warning
            self._note_candidates_truncated("advert", union)
        else:
            self._clear_candidates_truncated("advert")
        return max(self._candidates_trunc_seen.values(), default=0)

    def _clear_candidates_truncated(self, source: str) -> None:
        """``source`` fits the cap again: clear its cell and re-arm its log.

        The twin of :meth:`_note_candidates_truncated`. Re-arming here is
        what makes the log limiter a per-episode latch rather than a
        once-per-process one: a fleet that grows back over the cap after
        shrinking under it is a new episode and warns again.
        """
        self._candidates_trunc_seen[source] = 0
        self._candidates_trunc_logged.pop(source, None)

    def _note_candidates_truncated(self, source: str, seen: int) -> None:
        """Record and log a candidate-set truncation (rate-limited).

        ``source`` is ``"bridge"`` or ``"advert"``. Both the view cell and
        the log limiter are per source: the two report different counts.

        Logged once per oversize EPISODE per source, not once per distinct
        count: keying on the exact count meant any membership churn in an
        over-cap fleet (one node joining or leaving) changed the number and
        re-fired the warning every poll round, which is the flood the
        limiter exists to stop. The latch re-arms in
        :meth:`_clear_candidates_truncated` when the source drops back
        under the cap, so a genuinely new episode still warns. The current
        count still rides the message body.
        """
        self._candidates_trunc_seen[source] = seen
        if self._candidates_trunc_logged.get(source):
            return
        self._candidates_trunc_logged[source] = True
        what = (
            "bridge-confirmed candidates"
            if source == "bridge"
            else "advertised candidates (direct-eligible plus "
            "bridge-confirmed)"
        )
        logger.warning(
            "cluster: %d %s exceed the %d-name advertisement cap; the %d "
            "lowest names are gossiped and the rest are dropped, so a "
            "`spread` job whose owner falls in the dropped tail may run on "
            "more than one node. Reduce the fleet's bridge-discovered size, "
            "or raise MAX_ADVERTISED_CANDIDATE_NAMES and re-check the /peer "
            "body against MAX_PEER_RESPONSE_BYTES.",
            seen,
            what,
            MAX_ADVERTISED_CANDIDATE_NAMES,
            MAX_ADVERTISED_CANDIDATE_NAMES,
        )

    @_memoized_derived
    def _eligible_candidates(self) -> list[str]:
        """The names this node may actually *elect* as leader / job owner.

        The quorum gate decides whether THIS node is quorate; this decides
        which OTHER names it will defer to. Deferring to a node that cannot
        itself run would stand a healthy majority down, so we only elect a
        candidate we can confirm is quorate: a directly mutually-agreeing
        peer whose gossiped ``mutual_agreeing`` shows it at or above quorum,
        or a :meth:`_bridge_candidates` node. An unconfirmable peer (a
        sub-quorum set, or an older build reporting none) is NOT elected:
        in a converged uniform-version cluster a quorate node elects a
        runnable leader (the liveness choice). Residuals: a too-thin bridge
        may double-run; a candidate confirmed from now-stale gossip can
        cause a transient skip; a rolling upgrade leans new nodes toward
        running. See the module docstring.
        """
        quorum = self.quorum()
        eligible = [
            peer.node_name
            for peer in self._agreeing_peers()
            if peer.node_name and len(peer.mutual_agreeing or ()) + 1 >= quorum
        ]
        return eligible + self._bridge_candidates()

    @_memoized_derived
    def _unconfirmed_contenders(self) -> list[str]:
        """Quorate co-owners a peer *vouches* for that we cannot confirm,
        folded into the ``spread`` ``Leader`` owner gate so a thin bridge
        never double-runs (nor zero-runs) a job.

        We fold a peer's ``quorate_vouched`` set (its own
        :meth:`_eligible_candidates`), NOT its raw ``mutual_agreeing``: a
        witness may have a single edge to a sub-quorum node, and folding
        that node would let the rendezvous pick an owner that then stands
        itself down on its own quorum gate, a silent cluster-wide zero-run.
        Soundness: two strict
        majorities of one N cannot be disjoint, so any quorate node Z we
        cannot see shares a mutually-agreeing witness with us, and that
        witness vouches Z quorate. A vouched name we do not already account
        for becomes a possible co-owner: one that out-scores us for a job
        makes us defer (fail closed); one scoring below us cannot displace
        us. A name NO agreeing peer vouches is deliberately omitted: only
        sub-quorum nodes are dropped, so a crashed or partitioned node
        never stands our jobs down.
        """
        agreeing = self._agreeing_peers()
        # Names we have already placed: ourselves, every directly-agreeing
        # peer (quorate or not, it is in our view and will not silently
        # out-own us), and every bridge-confirmed candidate.
        accounted = {self.node_name}
        accounted |= {peer.node_name for peer in agreeing if peer.node_name}
        accounted |= set(self._eligible_candidates())
        possible: set[str] = set()
        for peer in agreeing:
            for name in peer.quorate_vouched or ():
                if name and name not in accounted:
                    possible.add(name)
        return sorted(possible)

    # --- duplicate-nodeName detection ------------------------------------

    @_memoized_derived
    def conflict_names(self) -> list[str]:
        """nodeNames currently claimed by more than one distinct instance.

        Non-empty makes the quorum election unsafe (two nodes would each
        elect themselves), so the ``Leader`` gate fails closed. Built by
        unioning our own fresh observations with every reachable peer's
        gossiped ``members`` list (one-hop transitivity: two peers that
        each see one copy still let us spot the collision). Identity is the
        per-process ``instance_id`` (falling back to the peer's host); a
        benign self-listing (same name AND instance id) is not a conflict,
        and stale peers contribute nothing.

        A members list is untrusted, so an instance counts only when
        credible: first-party (our own identity, or a peer's identity as we
        directly observed it) or corroborated (a transitive instance
        reported by at least two distinct peers). That keeps a genuine
        duplicate detectable while stopping a single buggy/hostile member
        from fabricating a conflict and wedging every ``Leader`` gate
        closed (an availability DoS). Residuals: a duplicate only a single
        peer can witness transitively is not flagged; and just after a node
        restart two not-yet-refreshed peers can still gossip the old
        instance, a transient fail-closed false positive for ~1-2 poll
        intervals. Deliberately NOT fixed by letting a fresh first-party
        observation suppress a stale transitive one: the same rule would
        suppress a genuine partial-mesh duplicate.
        """
        # name -> instance -> set(first-party sources: "self" or a peer host)
        first_party: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        # name -> instance -> set(distinct peers reporting it transitively)
        transitive: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        first_party[self.node_name][self.instance_id].add("self")
        for peer in self.view.peers.values():
            # Skip STALE peers (no fresh identity) AND STATUS_SELF: a SELF
            # peer is THIS node answering its own listed address, not
            # independent evidence. A self-listing reporting no instance_id
            # is classified benign SELF by record_success; processed here it
            # would synthesise a second "host:"+host instance for our OWN
            # nodeName and fabricate a phantom conflict. Mirrors
            # _agreeing_peers / cluster_size.
            if peer.status in _STALE_STATUSES or peer.status == STATUS_SELF:
                continue
            if peer.node_name is not None:
                # our own DIRECT observation of this peer's identity
                # (first-party evidence).
                first_party[peer.node_name][
                    peer.instance_id or "host:" + peer.host
                ].add(peer.host)
            # the peer's one-hop (transitive) view: weaker evidence,
            # credited only when a second distinct peer corroborates.
            for name, instance, _agreed in peer.members or ():
                transitive[name][instance].add(peer.host)
        conflicted: list[str] = []
        for name in set(first_party) | set(transitive):
            credible = set(first_party.get(name, {}))
            for instance, reporters in transitive.get(name, {}).items():
                if instance not in credible and len(reporters) >= 2:
                    credible.add(instance)
            if len(credible) >= 2:
                conflicted.append(name)
        return sorted(conflicted)

    # --- cluster-size (membership) divergence ----------------------------

    def _declaring_peers(self) -> list[PeerState]:
        """Peers whose last poll answered, whatever their job set.

        A fresh /peer response carries the peer's own declared N and
        policy; a stale status holds none, and SELF is this node.
        """
        return [
            peer
            for peer in self.view.peers.values()
            if peer.status not in _STALE_STATUSES
            and peer.status != STATUS_SELF
        ]

    @_memoized_derived
    def conflicting_sizes(self) -> list[int]:
        """Cluster sizes declared by reachable peers that differ from ours.

        Safety rests on every node sharing one N ("two strict majorities
        cannot be disjoint" holds only for a single N), yet N is each
        node's own ``len(peers) + 1`` and the fingerprint ignores the peer
        list, so two nodes mid-resize can still see each other AGREED and
        each reach quorum under its own N (split-brain). A divergent
        declared N is therefore a first-class conflict: the ``Leader``
        gate fails closed until the cluster reconverges (see
        :meth:`has_conflict` / :func:`cronstable.cron.Cron._cluster_allows`).

        Every fresh peer is compared, whatever its job set
        (:meth:`_declaring_peers`). A resize bundled with a job change puts
        the two config generations on different job-set ids, so the old-N
        and new-N sides see each other SYNCING or DRIFTED, never AGREED,
        while each still reaches quorum under its own N; a reachable
        peer's declared N is first-party evidence of a concurrent resize,
        and a pure job-change roll declares one N everywhere. NOT caught: a
        same-N membership swap; change membership one node at a time
        (module docstring). Residual (pre-release version skew only): a
        build from before the instance-id dedup declares raw
        ``len(peers)+1``, so a multi-homed peer makes the two builds flag
        each other mid upgrade; fail-closed and self-healing, accepted
        because it cannot survive a release.
        """
        my_size = self._our_declarations()["declared_size"]
        return sorted(
            {
                peer.declared_size
                for peer in self._declaring_peers()
                if _declares_divergent(peer.declared_size, my_size)
            }
        )

    @_memoized_derived
    def conflicting_policies(self) -> list[str]:
        """Coordination-policy divergences declared by reachable peers.

        Safety assumes every node coordinates the same way: single-leader
        elects ``min(live)`` while ``spread`` picks per-job rendezvous
        owners, and a node with ``electLeader`` off runs everything
        ungated. Neither field is in the fingerprint, so divergent nodes
        can see each other AGREED and would double-run or drop ``Leader``
        jobs; a divergence is a first-class conflict and the gate fails
        closed (see :meth:`has_conflict` /
        :func:`cronstable.cron.Cron._cluster_allows`). Every fresh peer is
        compared, whatever its job set (:meth:`_declaring_peers`, the
        same rule as :meth:`conflicting_sizes`); a peer too old to declare
        contributes nothing. Returns sorted, de-duplicated
        ``"field theirs != ours"`` descriptors for the dashboard / view.
        """
        my_declarations = self._our_declarations()
        my_distribution = my_declarations["declared_distribution"]
        my_elect = my_declarations["declared_elect_leader"]
        conflicts: set[str] = set()
        for peer in self._declaring_peers():
            if _declares_divergent(
                peer.declared_distribution, my_distribution
            ):
                conflicts.add(
                    "distribution {!r} != {!r}".format(
                        peer.declared_distribution, my_distribution
                    )
                )
            if _declares_divergent(peer.declared_elect_leader, my_elect):
                conflicts.add(
                    "electLeader {!r} != {!r}".format(
                        peer.declared_elect_leader, my_elect
                    )
                )
        return sorted(conflicts)

    @_memoized_derived
    def has_conflict(self) -> bool:
        """Whether any conflict that makes the election unsafe is visible here.

        A duplicate ``nodeName`` (:meth:`conflict_names`), a cluster-size
        disagreement (:meth:`conflicting_sizes`), or a policy divergence
        (:meth:`conflicting_policies`): all three fail the ``Leader`` gate
        closed. The size/policy gates fail closed on a SINGLE reachable
        peer's declaration (no corroboration), unlike the nodeName gate:
        a divergent size/policy is a first-party report about itself, and
        requiring corroboration would re-open the split-brain the gate
        closes. Accepted cost: one hostile CA-vouched member can wedge the
        gate closed cluster-wide (an availability DoS, never a double-run);
        see :func:`build_server_ssl_context`.

        Memoized like its three inputs: the Leader gate asks per job per
        tick, and each input's own memo lookup costs a state-key build.
        """
        return (
            bool(self.conflict_names())
            or bool(self.conflicting_sizes())
            or bool(self.conflicting_policies())
        )

    @_memoized_derived
    def leader_name(self) -> Optional[str]:
        """Elected leader as this node sees it, or ``None`` if not quorate.

        The quorum gate uses our full mutual live set; the elected name is
        the lowest among ourselves and the confirmed-quorate candidates
        (:meth:`_eligible_candidates`), so we defer across a bridge instead
        of double-leading and never elect a known-sub-quorum peer.
        """
        return elect_leader(
            self.node_name,
            self._agreeing_peer_names(),
            self.cluster_size(),
            self._eligible_candidates(),
        )

    def is_leader(self) -> bool:
        """Whether this node is the elected leader (quorate, lowest name)."""
        return self.leader_name() == self.node_name

    def view_settled(self) -> bool:
        """Whether the never-skip ``available_*`` gates may trust this view.

        A freshly built manager (boot, reload, TLS rotation) starts from a
        blank view and a new ``instance_id``: every peer is unknown, nobody
        attests us, the quorum-less election reduces to ``min([self])``,
        and EVERY node would claim available ownership at once (running
        each PreferLeader job and deferred @reboot one-shot on every node).
        The hold lasts only while the view is converging:

        * a peer never polled (``unknown``) holds until the first round
          completes (:meth:`start` runs one inline); and
        * an AGREED current-build peer whose ``members`` do not mention our
          ``instance_id`` has not re-polled this incarnation yet; bounded
          by ``_SETTLE_ROUNDS`` completed rounds, after which the link is
          treated as genuinely one-way and never-skip leans toward running.
          A legacy peer (no ``members`` field) never attests anyone: exempt.

        Unreachable/untrusted/self/conflict/drifted/syncing are real
        observations, not convergence: an isolated node settles on its
        first round. Cost: a PreferLeader firing skipped for <= ~2
        intervals (on every node when the held node is the rightful owner).

        While the hold is on, :meth:`is_available_leader` /
        :meth:`is_available_job_owner` return ``False`` even on the
        rightful owner (a quorate node can be held), so
        :meth:`cronstable.cron.Cron._cluster_owner_moved` must read the
        hold as a transient fail-closed denial, never as another node
        positively owning the job (which would abandon a rightful owner's
        pending retry; fatal for an @reboot keep-alive).
        """
        if self._view_settled_latched:
            return True
        for peer in self.view.peers.values():
            if peer.status == STATUS_UNKNOWN:
                return False
        if self._poll_rounds >= _SETTLE_ROUNDS:
            # no peer is UNKNOWN (a status never returns there) and the
            # round count only grows, so True is now permanent: latch it.
            self._view_settled_latched = True
            return True
        for peer in self.view.peers.values():
            if (
                peer.status == STATUS_AGREED
                and peer.reports_members
                and not any(
                    instance == self.instance_id
                    for _name, instance, _agreed in peer.members or ()
                )
            ):
                return False
        return True

    @_memoized_derived
    def available_leader_name(self) -> str:
        """Elected leader ignoring quorum (for the ``PreferLeader`` policy).

        The lowest name over ourselves, the peers we see agreeing, and the
        reachable co-owners a witness vouches a two-way edge to
        (:meth:`_available_contenders`): without the fold, two nodes blind
        to each other but sharing a witness both run on a converged
        cluster. It cannot zero-run: the global-minimum node is the min of
        any set containing it, so it always self-elects (never-skip).
        """
        return elect_available_leader(
            self.node_name,
            [*self._agreeing_peer_names(), *self._available_contenders()],
        )

    @_memoized_derived
    def _lower_instance_twins(self) -> "list[set[str]]":
        """Gossiped views of the strictly-lower-instance twins announcing our
        nodeName, the candidates :meth:`_cedes_to_lower_instance` may defer
        to. Normally empty (a duplicate nodeName is a misconfiguration).
        """
        twins: "list[set[str]]" = []
        for peer in self.view.peers.values():
            if peer.status in _STALE_STATUSES:
                continue
            if peer.node_name != self.node_name or not peer.instance_id:
                continue
            if peer.instance_id >= self.instance_id:
                continue  # not a strictly-lower-instance twin
            twins.append(peer.mutual_agreeing or set())
        return twins

    def _cedes_to_lower_instance(
        self, owns_for: "Callable[[str, set[str]], bool]"
    ) -> bool:
        """Whether a lower-instance twin sharing our nodeName would itself
        run this, so we defer to it: the duplicate-nodeName tiebreak on the
        never-skip ``available_*`` path.

        A duplicate nodeName is a misconfiguration the conflict gate does
        not protect ``PreferLeader`` against, so two same-named processes
        would both run every job they own. The tie breaks on the
        per-process ``instance_id``: the lowest instance runs, the rest
        defer. The deferral is gated on the lower twin ACTUALLY owning the
        job in its own gossiped view (``owns_for`` recomputes the election
        over the twin's ``mutual_agreeing``), not merely on its existence.
        Ceding only to a twin we can see will run
        degrades an asymmetric view to an accepted double-run, never a
        zero-run; a twin with an unknown view trivially self-owns, so we
        cede (the converged healthy case). Residual: a twin deferring via a
        folded contender we cannot see is mis-read as a self-owner, biasing
        to the accepted double-run.
        """
        for view in self._lower_instance_twins():
            if owns_for(self.node_name, view):
                return True
        return False

    def is_available_leader(self) -> bool:
        """Whether this node leads its reachable set, quorum or not."""
        if not self.view_settled():
            # still-converging view: hold (fail closed) rather than claim
            # leadership out of a blank view; see view_settled.
            return False
        if self.available_leader_name() != self.node_name:
            return False
        return not self._cedes_to_lower_instance(
            lambda name, view: elect_available_leader(name, view) == name
        )

    def is_quorate(self) -> bool:
        """Whether this node currently sees a quorum (so it may run jobs)."""
        return self.leader_name() is not None

    # --- per-job ownership (distribution: spread) -------------------------

    def job_owner(self, job_name: str) -> Optional[str]:
        """Quorum-gated owner of ``job_name`` (spread mode), else ``None``.

        The rendezvous winner over ourselves, the confirmed-quorate
        candidates (:meth:`_eligible_candidates`), AND the contenders a
        witness vouches quorate (:meth:`_unconfirmed_contenders`): a
        thin-bridged node we cannot confirm would otherwise self-own a job
        we also claim (the winner is per-job, so a thin bridge that
        single-leader's global-min shrugs off can split spread). The fold
        keeps ``spread`` at-most-once, at the fail-closed cost of standing
        a job down while its owner is unconfirmable; folding only
        quorate-vouched contenders keeps that a converging skip rather than
        a permanent zero-run (see :meth:`_unconfirmed_contenders`).
        """
        quorate, members, member_bytes, owners = self._spread_owner_set()
        if not quorate:
            return None
        owner = owners.get(job_name)
        if owner is None:
            owner = _hrw_owner_bytes(job_name, members, member_bytes)
            if len(owners) < MAX_MEMOIZED_JOB_OWNERS:
                owners[job_name] = owner
        return owner

    @_memoized_derived
    def _spread_owner_set(
        self,
    ) -> "tuple[bool, list[str], list[bytes], dict[str, str]]":
        """``(quorate, members, member_name_bytes, owner_memo)`` for spread.

        The first three are job-independent, derived once per generation
        instead of rebuilt per job; :meth:`job_owner` then pays only the
        per-(job, node) rendezvous hash.

        The memo is the ONLY mutable value in the derived cache: an
        initially empty ``{job_name: owner}`` dict :meth:`job_owner` fills.
        It rides here precisely so the generation roll that swaps the
        member set drops it in the same ``_derived_cache.clear()``; a stale
        owner is not cosmetic, for a ``Leader`` job it is a double-run or a
        zero-run. :meth:`available_job_owner` hashes over a DIFFERENT
        member set and keeps its own memo; sharing one dict would let
        whichever method asked first answer for the other.
        """
        live_count = 1 + len(self._agreeing_peer_names())
        quorate = live_count >= quorum_size(self.cluster_size())
        members = [
            self.node_name,
            *self._eligible_candidates(),
            *self._unconfirmed_contenders(),
        ]
        member_bytes = [name.encode("utf-8") for name in members]
        return quorate, members, member_bytes, {}

    def is_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` (quorate rendezvous winner).

        At-most-once: a node defers (this returns ``False``) when a possible
        co-owner it cannot confirm out-scores it for the job, so two quorate
        nodes never both own one ``Leader`` job across a thin bridge (see
        :meth:`job_owner` / :meth:`_unconfirmed_contenders`).
        """
        return self.job_owner(job_name) == self.node_name

    @_memoized_derived
    def _available_contenders(self) -> list[str]:
        """Reachable co-owners a peer vouches a two-way edge to that we
        cannot reach directly, folded into the never-skip ``available``
        owner set so two blind-to-each-other ``PreferLeader`` nodes agree
        on one owner per job.

        The ``PreferLeader`` analogue of :meth:`_unconfirmed_contenders`,
        but folding the RAW ``mutual_agreeing`` edge, not quorate_vouched:
        with no quorum gate a sub-quorum node still runs the jobs it owns,
        so it is a legitimate co-owner and must be in everyone's rendezvous
        set. Folding raw edges cannot zero-run here: a rendezvous winner
        has no quorum gate to stand down on, and the global-max node is the
        max of any set containing it, so it always self-owns and runs.
        """
        agreeing = self._agreeing_peers()
        accounted = {self.node_name}
        accounted |= {peer.node_name for peer in agreeing if peer.node_name}
        possible: set[str] = set()
        for peer in agreeing:
            for name in peer.mutual_agreeing or ():
                if name and name not in accounted:
                    possible.add(name)
        return sorted(possible)

    def available_job_owner(self, job_name: str) -> str:
        """Owner of ``job_name`` ignoring quorum (spread ``PreferLeader``).

        The rendezvous winner over ourselves, the peers we see agreeing,
        and :meth:`_available_contenders`: two never-skip nodes blind to
        each other converge on one owner (no double-run) while the absent
        quorum gate guarantees the winner runs (no zero-run).
        """
        members, member_bytes, owners = self._available_owner_members()
        owner = owners.get(job_name)
        if owner is None:
            owner = _hrw_owner_bytes(job_name, members, member_bytes)
            if len(owners) < MAX_MEMOIZED_JOB_OWNERS:
                owners[job_name] = owner
        return owner

    @_memoized_derived
    def _available_owner_members(
        self,
    ) -> "tuple[list[str], list[bytes], dict[str, str]]":
        """``(members, member_name_bytes, owner_memo)`` for never-skip spread.

        The job-independent rendezvous set of :meth:`available_job_owner`,
        derived once per generation with names pre-encoded. The duplicate-
        name tiebreak recomputes over a per-twin member set, so it keeps
        the un-memoized module helper. The memo is this method's OWN (see
        :meth:`_spread_owner_set`): this member set has no quorum gate and
        folds raw edges, so it answers differently from :meth:`job_owner`
        for the same name.
        """
        members = [
            self.node_name,
            *self._agreeing_peer_names(),
            *self._available_contenders(),
        ]
        return members, [name.encode("utf-8") for name in members], {}

    def is_available_job_owner(self, job_name: str) -> bool:
        """Whether this node owns ``job_name`` in its reachable set.

        Like :meth:`is_available_leader`, breaks a duplicate-nodeName tie
        on ``instance_id``, ceding only to a lower-instance twin that would
        itself own the job, so an asymmetric duplicate-name view never
        zero-runs it (see :meth:`_cedes_to_lower_instance`).
        """
        if not self.view_settled():
            # still-converging view: hold (fail closed) rather than claim
            # ownership out of a blank view; see view_settled.
            return False
        if self.available_job_owner(job_name) != self.node_name:
            return False
        return not self._cedes_to_lower_instance(
            lambda name, view: (
                elect_available_job_owner(job_name, name, view) == name
            )
        )

    def _node_stats_max_age(self) -> float:
        """Seconds before an absorbed peer node_stats reading expires.

        Scaled by the poll ``interval`` (a sharing peer refreshes the reading
        once per round, so a fixed wall-clock constant would wrongly expire
        healthy readings under a slow cadence); see NODE_STATS_STALE_ROUNDS
        for why expiry exists at all.
        """
        return NODE_STATS_STALE_ROUNDS * float(self.config["interval"])

    def fleet_view(self) -> dict[str, Any]:
        """The merged per-node job-summary view for ``GET /fleet``.

        One entry per distinct node: this node first (live state, stamped
        now), then every configured peer with its last-absorbed snapshot,
        aged by ``as_of`` (= last_seen). A conditional 304 round counts as
        fresh but re-ships no body, so each ``scheduled_in`` countdown is
        re-derived from the snapshot's actual age (see
        :func:`_aged_job_summaries`). Self-listings are skipped and peers
        deduped by instance_id, mirroring cluster_size. ``jobs: null``
        means no snapshot was ever absorbed: the dashboard renders "no
        data", not "no jobs".
        """
        job_summaries, summaries_truncated = self._advertised_job_summaries()
        now = datetime.datetime.now(_UTC)
        stats_max_age = self._node_stats_max_age()
        nodes: list[dict[str, Any]] = [
            {
                "node_name": self.node_name,
                "host": None,
                "self": True,
                "status": STATUS_SELF,
                "as_of": now.isoformat(),
                "jobs": job_summaries,
                "truncated": summaries_truncated,
                # our own live node load, sampled fresh: shown whenever the
                # provider is installed, even if not gossiped to peers
                # (None only when psutil is unavailable).
                "node_stats": self._local_node_stats(),
            }
        ]
        seen_instances = {self.instance_id}
        for peer in self.view.peers.values():
            if peer.status == STATUS_SELF or peer.self_confirmed:
                continue
            if peer.instance_id is not None:
                if peer.instance_id in seen_instances:
                    continue
                seen_instances.add(peer.instance_id)
            nodes.append(
                {
                    "node_name": peer.node_name,
                    "host": peer.host,
                    "self": False,
                    "status": peer.status,
                    "as_of": (
                        peer.last_seen.isoformat()
                        if peer.last_seen is not None
                        else None
                    ),
                    "jobs": _aged_job_summaries(
                        peer.job_summaries, peer.job_summaries_at, now
                    ),
                    "truncated": peer.job_summaries_truncated,
                    # the peer's last-absorbed load, expired once no fresh
                    # reading arrived in the window (see fresh_node_stats).
                    "node_stats": peer.fresh_node_stats(now, stats_max_age),
                }
            )
        return {
            "enabled": True,
            "backend": "gossip",
            "node_name": self.node_name,
            "distribution": self.distribution,
            "elect_leader": self._elect_leader,
            # the peer-poll cadence, so the dashboard can set expectations
            # for how stale a healthy peer's as_of may legitimately be
            "interval": self.config["interval"],
            "nodes": nodes,
        }

    def view_dict(self) -> dict[str, Any]:
        leader = self.leader_name()
        spread = self.distribution == "spread"
        conflicts = self.conflict_names()
        size_conflicts = self.conflicting_sizes()
        policy_conflicts = self.conflicting_policies()
        return {
            "backend": "gossip",
            "node_name": self.node_name,
            "job_set_id": self.get_job_set_id(),
            "cluster_size": self.cluster_size(),
            "quorum": self.quorum(),
            "elect_leader": self._elect_leader,
            "distribution": self.distribution,
            # a conflict was detected: Leader jobs fail closed until it clears
            # (see has_conflict / cron._cluster_allows). "conflict" is the
            # umbrella flag (any kind); the lists below say which.
            "conflict": (
                bool(conflicts)
                or bool(size_conflicts)
                or bool(policy_conflicts)
            ),
            # a duplicate nodeName (two nodes would each elect themselves)
            "conflict_names": conflicts,
            # reachable peers, whatever their job set, that declare a different
            # cluster size N (nodes quorate under different Ns -> split-brain)
            "size_conflict": bool(size_conflicts),
            "conflicting_sizes": size_conflicts,
            # reachable peers running a different distribution / electLeader
            # (independent owner selectors -> double-run or lost-run)
            "policy_conflict": bool(policy_conflicts),
            "conflicting_policies": policy_conflicts,
            # the bridge-confirmed candidate set outgrew its advertisement
            # cap, so a `spread` owner in the dropped tail may double-run
            # (see MAX_ADVERTISED_CANDIDATE_NAMES); zero when it fits, else
            # the full count. NOT folded into "conflict": failing a fleet
            # closed for outgrowing a gossip budget would be worse than the
            # residual.
            "candidates_truncated": self._candidates_truncated,
            "quorate": leader is not None,
            # In spread mode there is no single leader: ownership is per job,
            # so leader/is_leader are not meaningful (reported null/false).
            "leader": None if spread else leader,
            "is_leader": (
                False
                if spread
                else (leader is not None and leader == self.node_name)
            ),
            # now + the staleness window so each peer's absorbed node_stats
            # expires from the /cluster panel too (see fresh_node_stats).
            "peers": self.view.to_list(
                datetime.datetime.now(_UTC),
                self._node_stats_max_age(),
            ),
        }
