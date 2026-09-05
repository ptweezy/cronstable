import copy
import datetime
import hashlib
import ipaddress
import logging
import math
import os
import re
import socket
import sys
import types
from collections import Counter, OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import (
    Any,
    NamedTuple,
    NewType,
    Optional,
    Union,  # noqa
)
from urllib.parse import ParseResult, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import strictyaml
from strictyaml import Any as YamlAny
from strictyaml import (
    Bool,
    EmptyDict,
    EmptyNone,
    Enum,
    Float,
    Int,
    Map,
    MapPattern,
    Seq,
    Str,
)
from strictyaml import Optional as Opt
from strictyaml.ruamel.error import YAMLError

from cronstable import crontabs, platform
from cronstable.cronexpr import LOCAL_ZONE, CronTab
from cronstable.croninfo import Finding, lint_schedule


def _patch_strictyaml_seq_deepcopy() -> None:
    """Make strictyaml ``Seq`` validation linear in the number of elements.

    strictyaml validates a sequence by deep-copying the ruamel document, and
    its vendored ``CommentedSeq.__deepcopy__`` calls ``copy_attributes``
    INSIDE the element loop rather than after it, so an N-element sequence
    re-copies the sequence's whole Comment/Format/LineCol/Anchor/Tag/merge
    attribute set N times.  The ``CommentedMap`` twin 470 lines below it in
    the same file makes the identical call from outside its loop, which is
    what marks this an indentation slip rather than intent.

    ``jobs:`` is the only large sequence in a normal config, so it pays the
    entire bill and config parsing comes out quadratic.  Measured here:
    1k jobs 0.90s, 2k 3.06s, 4k 12.54s, 8k 49.61s, roughly 4x per doubling.
    With the call hoisted: 0.37s, 0.87s, 1.73s, 3.66s, roughly 2x per
    doubling, which is the linear shape the parse should have had.  It lands
    on every boot parse, every --validate-config, every --job-set-id and
    every reload that touches a file.

    The rebinding is a pure cost change.  ``copy_attributes`` reads only
    ``self``, which the loop never mutates, and it overwrites the same six
    attributes on every call, so only the last call's objects ever survive;
    running it once after the loop installs the same result.  The
    empty-sequence case deliberately keeps upstream's behaviour (no call at
    all) rather than the Map twin's, so the call COUNT is the only thing
    this changes.

    Only ``strictyaml.ruamel`` is touched: strictyaml vendors its own copy of
    ruamel, so a separately installed ``ruamel.yaml`` is unaffected.  A probe
    decides whether the slip is still present, so a strictyaml that ships the
    fix (or a second import of this module) is left alone.
    """
    try:
        from strictyaml.ruamel.comments import CommentedSeq
    except Exception:  # pragma: no cover - vendored layout changed
        return

    class _Probe(CommentedSeq):  # type: ignore[misc,valid-type]
        calls = 0

        def copy_attributes(self, t: Any, memo: Any = None) -> None:
            _Probe.calls += 1
            super().copy_attributes(t, memo=memo)

    try:
        copy.deepcopy(_Probe([0, 1, 2, 3]))
    except Exception:  # pragma: no cover - vendored layout changed
        return
    if _Probe.calls <= 1:
        return

    def _linear_deepcopy(self: Any, memo: Any) -> Any:
        res = self.__class__()
        memo[id(self)] = res
        for k in self:
            res.append(copy.deepcopy(k, memo))
        if len(self):
            self.copy_attributes(res, memo=memo)
        return res

    CommentedSeq.__deepcopy__ = _linear_deepcopy


_patch_strictyaml_seq_deepcopy()


def _patch_strictyaml_pointer_copy() -> None:
    """Make strictyaml's chunk navigation fork a pointer without deepcopy.

    strictyaml derives each child ``YAMLPointer`` (``val``/``key``/
    ``index``/``textslice``/``parent``) with ``copy.deepcopy(self)``.  A
    pointer's only state is ``_indices``, a list of tuples of strings and
    ints, so the generic deepcopy spends some 40 Python calls producing
    what one list copy produces; the walk runs once per key and element,
    about a quarter of a 300-job parse.

    A pure cost change: nothing mutates ``_indices`` in place, the tuples
    are immutable, and the argument assertions are kept.  Error rendering
    is unaffected (``_slice_segment`` deep-copies the document, not a
    pointer).

    Probed like the Seq shim: the rebinding happens only while upstream's
    methods deep-copy and a pointer carries ``_indices`` alone, so a
    strictyaml with a fix or added pointer state, or a second import of
    this module, is left alone.
    """
    try:
        from strictyaml.yamlpointer import YAMLPointer
    except Exception:  # pragma: no cover - vendored layout changed
        return
    try:
        stock_names = YAMLPointer.val.__code__.co_names
        state = set(vars(YAMLPointer()))
    except Exception:  # pragma: no cover - vendored layout changed
        return
    if "deepcopy" not in stock_names or state != {"_indices"}:
        return

    def _fork(pointer: Any) -> Any:
        new = pointer.__class__.__new__(pointer.__class__)
        new._indices = list(pointer._indices)
        return new

    def val(self: Any, regularkey: Any, strictkey: Any) -> Any:
        assert isinstance(regularkey, str), type(regularkey)
        assert isinstance(strictkey, str), type(strictkey)
        new = _fork(self)
        new._indices.append(("val", (regularkey, strictkey)))
        return new

    def key(self: Any, regularkey: Any, strictkey: Any) -> Any:
        assert isinstance(regularkey, str), type(regularkey)
        assert isinstance(strictkey, str), type(strictkey)
        new = _fork(self)
        new._indices.append(("key", (regularkey, strictkey)))
        return new

    def index(self: Any, index: Any) -> Any:
        new = _fork(self)
        new._indices.append(("index", index))
        return new

    def textslice(self: Any, start: Any, end: Any) -> Any:
        new = _fork(self)
        new._indices.append(("textslice", (start, end)))
        return new

    def parent(self: Any) -> Any:
        new = _fork(self)
        new._indices = new._indices[:-1]
        return new

    YAMLPointer.val = val
    YAMLPointer.key = key
    YAMLPointer.index = index
    YAMLPointer.textslice = textslice
    YAMLPointer.parent = parent


_patch_strictyaml_pointer_copy()

logger = logging.getLogger("cronstable.config")
WebConfig = NewType("WebConfig", dict[str, Any])
ClusterConfig = NewType("ClusterConfig", dict[str, Any])
StateConfig = NewType("StateConfig", dict[str, Any])
MCPConfig = NewType("MCPConfig", dict[str, Any])
JobDefaults = NewType("JobDefaults", dict[str, Any])
LoggingConfig = NewType("LoggingConfig", dict[str, Any])

# Defaults for an (optional) cluster block. Only applied when a `cluster`
# section is present; see _build_cluster_config.
DEFAULT_CLUSTER = {
    # which leadership backend gates jobs:
    #   "gossip" (default) - the embedded mTLS, no-shared-state, best-effort
    #                        quorum election (listen/tls/peers below);
    #   "kubernetes"       - a coordination.k8s.io/v1 Lease (fenced);
    #   "etcd"             - a lease-backed etcd key/election (fenced);
    #   "filesystem"       - a flock-guarded TTL lease on a shared POSIX
    #                        mount (fenced under NTP-bounded clock skew).
    "backend": "gossip",
    "interval": 30,  # seconds between peer-attestation rounds
    "driftAfter": 3,  # reachable-but-mismatched rounds before "drifted"
    "nodeName": None,  # defaults to the system hostname at load time
    "connectTimeout": 10,  # seconds per peer request
    # When true, only the elected leader runs *scheduled* jobs (manual API
    # triggers and retries are unaffected); see
    # cronstable.cluster.elect_leader.
    # Off by default so a cluster section is observe-only until opted in.
    "electLeader": False,
    # How leader-gated jobs spread across the quorate cluster: "single-leader"
    # (default, one leader runs every Leader job) or "spread" (per-job
    # ownership via rendezvous hashing, same quorum gate and guarantee).
    # Inert unless electLeader is on; see cronstable.cluster.elect_job_owner.
    "distribution": "single-leader",
}

# Defaults merged over a `cluster.kubernetes` block (backend: kubernetes). The
# values mirror client-go's leaderelection defaults; see
# cronstable.backends.kubernetes.
DEFAULT_K8S: dict[str, Any] = {
    "leaseName": "cronstable-leader",
    # None -> the in-cluster service-account namespace file at runtime.
    "leaseNamespace": None,
    "leaseDurationSeconds": 15,
    "renewDeadlineSeconds": 10,
    "retryPeriodSeconds": 2,
    "identity": None,  # None -> nodeName
    "kubeconfig": None,  # for out-of-cluster / local (Docker) testing
    # override the apiserver URL (e.g. a kube-rbac-proxy sidecar); must be
    # https. Wins on both credential paths while keeping their credentials
    # (see backends.kubernetes._setup_sync/_load_kubeconfig).
    "apiServer": None,
    # auto (native `kubernetes` client if importable, else hand-rolled HTTP) |
    # library (require the native client) | http (force hand-rolled).
    "clientLibrary": "auto",
}

# Defaults merged over a `cluster.etcd` block (backend: etcd). See
# cronstable.backends.etcd.
DEFAULT_ETCD: dict[str, Any] = {
    "endpoints": ["http://127.0.0.1:2379"],
    "electionName": "cronstable/leader",
    "ttl": 15,  # lease time-to-live, seconds
    "username": None,
    # resolved like web.authToken: value / fromFile / fromEnvVar
    "password": {"value": None, "fromFile": None, "fromEnvVar": None},
    # optional client TLS to the etcd endpoints
    "tls": {"ca": None, "cert": None, "key": None},
}

# Defaults merged over a `cluster.filesystem` block (backend: filesystem):
# leader election via a flock-guarded TTL lease on a shared POSIX mount.
# See cronstable.backends.filesystem.
DEFAULT_FILESYSTEM: dict[str, Any] = {
    # required: the directory the election lease lives in. Point it at the
    # same mount (and deploymentId) as the `state` section to keep one
    # coordination surface per deployment.
    "path": None,
    "electionName": "cluster/leader",
    "ttl": 15,  # lease time-to-live, seconds (floor 3, like etcd)
    # namespace prefix inside the store; None -> "default". Use the SAME
    # value as state.deploymentId when sharing a mount with the state store.
    "deploymentId": None,
    # auto (probe the mount) | single-node | shared -- same semantics as
    # state.topology. Windows/macOS cannot probe; assert `shared` explicitly
    # there.
    "topology": "auto",
}


# Defaults for an (optional) state block. Only applied when a `state` section
# is present; see _build_state_config. cronstable is stateless by default, so
# the whole section is absent unless the user opts in.
DEFAULT_STATE: dict[str, Any] = {
    # required: the directory the durable store lives in; a shared mount adds
    # fleet-wide coordination. Enforced non-empty in _build_state_config.
    "path": None,
    # auto (probe the mount) | single-node | shared. Gates whether cross-node
    # coordination may be offered; see cronstable.state.
    "topology": "auto",
    # optional stable prefix so several deployments can share one store/bucket
    # without colliding or cross-reading; None -> the "default" namespace.
    "deploymentId": None,
    # finished runs retained durably per job; the ledger is pruned to this
    # after each append. <= 0 disables pruning (unbounded).
    "maxRunsPerJob": 1000,
    # what the STATEFUL features do while the store is unavailable: "degrade"
    # (default) fails durable-truth gates open and drops writes with a
    # warning; "fail-closed" blocks the onlyIfLastSucceeded gate, defers due
    # durable retries and skips an unverifiable @reboot run. Plain scheduled
    # fires are never gated on the store under either policy.
    "onStoreUnavailable": "degrade",
    # age (seconds) past which durable state no recent manifest references is
    # garbage collected; <= 0 disables GC. Defaults to 7 days so a briefly
    # removed job (or a node down for a long weekend) keeps its history.
    "gcGraceSeconds": 604800,
    # token-bucket cap on store operations per second (0 disables). Lease
    # operations bypass the bucket: a renew queued behind bulk writes could
    # overshoot its TTL and double-run the job the lease fences.
    "maxOpsPerSecond": 0,
    # TTL (seconds) of the concurrencyScope: cluster slot lease, renewed at a
    # third of this while the job runs. Floor 5 (enforced): a tiny TTL leaves
    # no room for renew latency and would expire live holders.
    "slotTtlSeconds": 30,
    # the job-facing state API (loopback endpoint + job commands). Merged
    # (not replaced) over DEFAULT_JOB_API in _build_state_config, so a
    # partial `jobApi:` block keeps the untouched defaults.
    "jobApi": None,
}

# Defaults for the state.jobApi sub-section. Present only when a `state`
# section is (the loopback endpoint has no store to talk to otherwise). See
# cronstable.jobapi / cronstable.jobstate.
DEFAULT_JOB_API: dict[str, Any] = {
    # run the loopback endpoint and inject its address/token into every job's
    # environment. On by default when `state` is configured; set false to keep
    # the durable store's scheduler features but expose nothing to jobs.
    "enabled": True,
    # override the loopback bind (`http://host:port`); None binds an
    # OS-assigned ephemeral port on 127.0.0.1. A unix:// path is not accepted:
    # the job CLI reaches the endpoint over stdlib urllib, TCP only.
    "listen": None,
    # upper bound (bytes) on a single KV / cursor value; a larger set is
    # refused (HTTP 413).
    "maxValueBytes": 1024 * 1024,
    # upper bound (bytes) on a single artifact payload; a larger put is
    # refused (HTTP 413).
    "maxArtifactBytes": 64 * 1024 * 1024,
    # TTL (seconds) of a job mutex/semaphore lease, renewed at a third of
    # this on the run's behalf. Floor 5, like slotTtlSeconds.
    "lockTtlSeconds": 30,
    # explicit opt-in required for a non-loopback `listen` host: the endpoint
    # serves per-run bearer tokens and staged job secrets. Pair it with `tls`
    # below to encrypt them.
    "allowNonLoopbackBind": False,
    # native TLS for an `https://` listen. `cert` + `key` are all-or-nothing;
    # `ca` is the trust anchor handed to jobs as CRONSTABLE_STATE_CACERT so
    # the job CLI can verify an internally-issued certificate.
    "tls": {"cert": None, "key": None, "ca": None},
}

# The toolsets the MCP server groups its tools into (see cronstable.mcp). The
# read-only `observe` set is the default; `act` (mutating job/DAG control),
# `dags` (DAG introspection) and `state` (durable-state inspection) are opt-in.
MCP_TOOLSETS = ("observe", "act", "dags", "state")

# Defaults for the optional `mcp:` section. The server is served on the `web:`
# listeners (it reuses their auth + lifecycle), so it is inert without a `web`
# section; `enabled` defaults false so a plain install pays nothing. See
# cronstable.mcp.
DEFAULT_MCP: dict[str, Any] = {
    # serve the Model Context Protocol endpoint (POST /mcp) on the web
    # listeners, and expose the `cronstable mcp` stdio bridge. Off by default.
    "enabled": False,
    # strip every mutating tool. On by default; takes precedence over
    # `toolsets` (`act` is suppressed while this is true).
    "readOnly": True,
    # which tool groups to expose; `observe` (read-only) is the safe default.
    "toolsets": ["observe"],
    # exact-match browser Origins allowed to call /mcp; a present Origin not
    # on the list is refused (403, a DNS-rebinding defense). A non-empty list
    # additionally answers CORS preflight.
    "allowedOrigins": [],
    # serve /mcp on a routable listener even when no web.authToken is set.
    # Fail-closed default: with no token the app has no auth middleware at
    # all. Set true only when the endpoint is protected by other means.
    "allowUnauthenticated": False,
    # expose MCP resources (read-only context, e.g. cronstable://status) and
    # prompts (canned triage playbooks); both read-only, scope follows
    # `toolsets`.
    "resources": True,
    "prompts": True,
    # optional free-text `instructions` surfaced to the client at initialize
    # (a short operator note on how to use this server). None omits it.
    "instructions": None,
    # ceiling on any list tool's `limit`; a larger request is capped (never an
    # error) and an opaque cursor is offered for the rest.
    "maxRows": 200,
    # cap (bytes) on a single /mcp request body. Tool arguments arrive from an
    # LLM, so an oversized POST is refused (413) rather than buffered.
    "maxBodyBytes": 1024 * 1024,
}


class ConfigError(Exception):
    pass


DEFAULT_BODY_TEMPLATE = """
{% if fail_reason -%}
(job failed because {{fail_reason}})
{% endif %}
{% if stdout and stderr -%}
STDOUT:
---
{{stdout}}
---
STDERR:
{{stderr}}
{% elif stdout -%}
{{stdout}}
{% elif stderr -%}
{{stderr}}
{% else -%}
(no output was captured)
{% endif %}
"""

DEFAULT_SUBJECT_TEMPLATE = (
    "Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}"
)

# Same text as the default sentry body (subject + body), JSON-encoded into a
# {"text": ...} payload -- the shape Slack, Mattermost and Teams incoming
# webhooks accept out of the box. Override `body` for other services (e.g.
# Discord wants {"content": ...}, ntfy takes a plain-text body).
DEFAULT_WEBHOOK_BODY_TEMPLATE = (
    '{"text": {% filter tojson %}'
    + DEFAULT_SUBJECT_TEMPLATE
    + "\n"
    + DEFAULT_BODY_TEMPLATE
    + "{% endfilter %}}"
)

# Defaults for the onLate report block: an SLA breach has no run outcome, so
# the completed/failed wording of the standard templates does not apply.
DEFAULT_LATE_SUBJECT_TEMPLATE = (
    "Cron job '{{name}}' is overdue ({{sla_check}})"
)

DEFAULT_LATE_BODY_TEMPLATE = """
SLA check: {{sla_check}}
Threshold: {{threshold_seconds}} seconds
Observed: {{observed_seconds}} seconds
{% if last_success_at -%}
Last success: {{last_success_at}}
{% else -%}
Last success: (none recorded)
{% endif %}
"""

DEFAULT_LATE_WEBHOOK_BODY_TEMPLATE = (
    '{"text": {% filter tojson %}'
    + DEFAULT_LATE_SUBJECT_TEMPLATE
    + "\n"
    + DEFAULT_LATE_BODY_TEMPLATE
    + "{% endfilter %}}"
)

# The canonical daemon/orchestration events the `notify:` block reports on
# (and the values a `notify.events` allow-list may name). None is a job run,
# so they never fire onFailure/onSuccess; they fan out to the notify report
# block instead. See Cron._dispatch_notify.
NOTIFY_EVENTS = (
    "dag_failure",  # a DAG run reached a terminal FAILED state
    "approval_waiting",  # an approval gate began awaiting a decision
    "leader_change",  # this node acquired or lost scheduled-job leadership
    "quorum_loss",  # this node left quorum (Leader jobs stand down)
)

# Defaults for the notify report block: a daemon/orchestration event is not a
# job run, so the completed/failed wording of the standard templates does not
# fit.  These key on the event's own vars (event / subject / message) instead.
DEFAULT_NOTIFY_SUBJECT_TEMPLATE = "cronstable {{ event }}: {{ subject }}"

DEFAULT_NOTIFY_BODY_TEMPLATE = "{{ message }}\n"

DEFAULT_NOTIFY_WEBHOOK_BODY_TEMPLATE = (
    '{"text": {% filter tojson %}'
    + DEFAULT_NOTIFY_SUBJECT_TEMPLATE
    + "\n"
    + DEFAULT_NOTIFY_BODY_TEMPLATE
    + "{% endfilter %}}"
)

# Named (not inlined below) because cronstable.fingerprint compares against it
# to keep the reporter timeout out of a job's identity while it holds the
# default -- see canonical_job's omit-when-default rule.
DEFAULT_REPORT_SHELL_TIMEOUT = 60

# Named for the same fingerprint reason: the push block post-dates the v1
# identity scheme, so cronstable.fingerprint omits it from a job's canonical
# form while it still equals these defaults (an all-default block must not
# repoint every existing job's digest on upgrade).
DEFAULT_PUSH_REPORT = {
    "enabled": False,
    "priority": "time-sensitive",
    "includeLogTail": True,
}

# Named for the same fingerprint reason as DEFAULT_PUSH_REPORT above: the
# eventlog block post-dates the v1 identity scheme, so cronstable.fingerprint
# omits it from a job's canonical form while it still equals these defaults.
DEFAULT_EVENTLOG_REPORT = {
    "enabled": False,
    # The Event Log source name, which is also the registry key name a
    # message DLL would be registered under. cronstable never registers the
    # source (that needs HKLM writes, and buys nothing without a message
    # DLL); it is configurable so two instances on one host, or a shop that
    # does register a source of its own, can tell their records apart.
    "source": "cronstable",
    # Carry a bounded tail of the run's captured output. Off by default, the
    # opposite of push's includeLogTail, and not for symmetry's sake: a push
    # payload is sealed to a paired device's key, while the Application log
    # is readable by every authenticated account on the machine. Turning
    # this on is a decision to publish job output locally.
    "includeOutput": False,
}

_REPORT_DEFAULTS = {
    "sentry": {
        "dsn": {"value": None, "fromFile": None, "fromEnvVar": None},
        "body": DEFAULT_SUBJECT_TEMPLATE + "\n" + DEFAULT_BODY_TEMPLATE,
        "fingerprint": [
            "cronstable",
            "{{ environment.HOSTNAME }}",
            "{{ name }}",
        ],
        "environment": None,
        "maxStringLength": 8192,
    },
    "mail": {
        "from": None,
        "to": None,
        "smtpHost": None,
        "smtpPort": 25,
        "tls": False,
        "starttls": False,
        "validate_certs": True,
        "html": False,
        "subject": DEFAULT_SUBJECT_TEMPLATE,
        "body": DEFAULT_BODY_TEMPLATE,
        "username": None,
        "password": {"value": None, "fromFile": None, "fromEnvVar": None},
    },
    "shell": {
        "shell": platform.DEFAULT_SHELL,
        "command": None,
        # hard bound (seconds) on the reporter command: reports run INLINE on
        # the reaper, so a script that never exits would freeze completion
        # handling for every job. On expiry the process group is killed.
        "timeout": DEFAULT_REPORT_SHELL_TIMEOUT,
    },
    "webhook": {
        # resolved like sentry "dsn": value / fromFile / fromEnvVar. Treated
        # as a secret (a Slack/Discord webhook URL embeds its token).
        "url": {"value": None, "fromFile": None, "fromEnvVar": None},
        "method": "POST",
        "contentType": "application/json",
        "headers": {},
        "body": DEFAULT_WEBHOOK_BODY_TEMPLATE,
        "timeout": 10,
    },
    # end-to-end encrypted push alerts to paired devices (cronstable.push).
    # Off by default; the relay endpoint and device registry live in the
    # daemon-global `push:` section, this block only opts a job/event in.
    "push": dict(DEFAULT_PUSH_REPORT),
    # Windows Event Log records (cronstable.job.EventLogReporter). Off by
    # default and a no-op on POSIX, where the load says so once.
    "eventlog": dict(DEFAULT_EVENTLOG_REPORT),
}


DEFAULT_CONFIG: dict[str, Any] = {
    "shell": platform.DEFAULT_SHELL,
    "concurrencyPolicy": "Allow",
    # how far concurrencyPolicy reaches: "node" (default) or "cluster"
    # (Forbid/Replace also exclude instances on other nodes via a TTL slot
    # lease on the shared `state` store). Requires a `state` section;
    # Allow+cluster is refused as inert. See cronstable.cron.maybe_launch_job.
    "concurrencyScope": "node",
    # where this job runs under cluster leader election (inert unless
    # cluster.electLeader is set); see cronstable.cron._cluster_allows.
    "clusterPolicy": "Leader",
    # missed-run catch-up on restart (inert without a `state` backend):
    # skip (default) | run-once (coalesce all missed slots into one fire) |
    # run-all (replay each missed occurrence). See cronstable.cron._catch_up.
    "onMissed": "skip",
    # only occurrences missed within this many seconds are caught up; None (the
    # default) means no deadline. Bounds run-all to a recent window so a long
    # outage cannot stampede. Like Kubernetes CronJob startingDeadlineSeconds.
    "startingDeadlineSeconds": None,
    # spread the boot-time catch-up launches of different jobs over [0, N)
    # seconds (deterministic per job name) so a fleet of jobs does not all fire
    # at once on restart. 0 (default) fires them together.
    "catchupJitterSeconds": 0,
    # depends-on-past guard: skip a scheduled fire when the previous durable
    # run did not succeed (Airflow depends_on_past; inert without a `state`
    # backend). See cronstable.cron._depends_on_past_ok.
    "onlyIfLastSucceeded": False,
    # archive each finished run's captured output to the `state` store
    # (opt-in; encryption-at-rest is the mount's job).
    "archiveOutput": False,
    # scrub common secrets from archived output before it is written. On by
    # default; only applies when archiveOutput is set.
    "redactArchivedSecrets": True,
    # sample each run's CPU time and peak resident memory (opt-in; see
    # cronstable.resources). Observability only: never changes a run's
    # success/failure verdict.
    "monitorResources": False,
    "captureStderr": True,
    "captureStdout": False,
    "saveLimit": 4096,
    "maxLineLength": 16 * 1024 * 1024,
    "utc": True,
    "timezone": None,
    "failsWhen": {
        "producesStdout": False,
        "producesStderr": True,
        "nonzeroReturn": True,
        "always": False,
    },
    "onFailure": {
        "retry": {
            "maximumRetries": 0,
            "initialDelay": 1,
            "maximumDelay": 300,
            "backoffMultiplier": 2,
        },
        # deepcopy so the three report blocks below do not alias the same
        # mutable object (and its nested lists, e.g. sentry "fingerprint").
        "report": copy.deepcopy(_REPORT_DEFAULTS),
    },
    "onPermanentFailure": {"report": copy.deepcopy(_REPORT_DEFAULTS)},
    "onSuccess": {"report": copy.deepcopy(_REPORT_DEFAULTS)},
    # per-job SLA thresholds (seconds), each independent and off (None) until
    # set: staleness (time since last success), lateness (a due slot that has
    # not started) and overrun (a run still going). Evaluated once per minute
    # by the in-process monitor (cronstable.cron); breaches fire onLate.
    "sla": {
        "maxTimeSinceSuccessSeconds": None,
        "lateAfterSeconds": None,
        "maxRuntimeSeconds": None,
    },
    "onLate": {"report": copy.deepcopy(_REPORT_DEFAULTS)},
    "environment": [],
    # run-scoped secrets staged for the job over the loopback endpoint; each is
    # {name, value|fromFile|fromEnvVar}. Resolved fresh per run, never durably
    # stored. Inert without a `state` section with jobApi enabled.
    "secrets": [],
    # extra scope names this job's loopback state calls may explicitly name;
    # without an entry, a `--scope` naming another job's own name is refused
    # (403). See cronstable.jobapi.JobStateAPI._scope.
    "stateAllowedScopes": [],
    "env_file": None,
    # directory the job's process starts in ("Start in" on a Task Scheduler
    # action).  None, the default, inherits the daemon's own CWD, which is
    # what every job got before this key existed.
    "workingDirectory": None,
    "executionTimeout": None,
    "killTimeout": 30,
    # scheduling priority of the job's process ("Priority" on a Task
    # Scheduler task's Settings).  The default level is the one that is never
    # applied, so the spawn is unchanged for every job that says nothing.
    # How far a level reaches past the job's own process is per-platform; see
    # cronstable.platform.new_process_group_kwargs.
    "priority": platform.DEFAULT_PRIORITY,
    "statsd": None,
    "streamPrefix": "[{job_name} {stream_name}] ",
    "enabled": True,
}

# An SLA breach has no run to report on, so the onLate defaults swap the
# standard completed/failed wording for the overdue templates and give
# breaches their own sentry grouping (never folded into run failures).
# _REPORT_DEFAULTS itself stays untouched: a new key there would enter every
# job's report block and shift fingerprints (see cronstable.fingerprint).
_late_report = DEFAULT_CONFIG["onLate"]["report"]
_late_report["mail"]["subject"] = DEFAULT_LATE_SUBJECT_TEMPLATE
_late_report["mail"]["body"] = DEFAULT_LATE_BODY_TEMPLATE
_late_report["webhook"]["body"] = DEFAULT_LATE_WEBHOOK_BODY_TEMPLATE
_late_report["sentry"]["fingerprint"] = ["cronstable", "sla", "{{ name }}"]
del _late_report


def _onlate_names_a_destination(on_late: dict[str, Any]) -> bool:
    """Whether an ``onLate`` block would actually report anything.

    A reporter left at its all-None defaults is not "configured": only an
    onLate that would really fire demands an ``sla`` block to fire about.

    Every reporter has to be named here, and a new one is easy to forget:
    a reporter this function does not know about makes an onLate that WOULD
    really fire skip the "onLate requires sla" check below, so the hook
    loads clean and is then dead for the life of the config.  That is the
    exact failure push had until it was added.
    """
    report = on_late.get("report") or {}
    sentry_dsn = (report.get("sentry") or {}).get("dsn") or {}
    mail = report.get("mail") or {}
    shell = report.get("shell") or {}
    webhook_url = (report.get("webhook") or {}).get("url") or {}
    secret_keys = ("value", "fromFile", "fromEnvVar")
    return (
        any(sentry_dsn.get(k) is not None for k in secret_keys)
        or mail.get("to") is not None
        or mail.get("from") is not None
        or shell.get("command") is not None
        or any(webhook_url.get(k) is not None for k in secret_keys)
        # bool(), not the `is not None` the four above use: `enabled` is
        # always present carrying its False default, so `is not None` would
        # make this function true for every job and trip the import-time
        # drift guard below.
        or bool((report.get("push") or {}).get("enabled"))
        or bool((report.get("eventlog") or {}).get("enabled"))
    )


#: The stock ``onLate`` block.  ``mergedicts`` merges copy-on-write, so a job
#: that never wrote an ``onLate`` still points at this very object, which lets
#: JobConfig._validate_numeric_ranges settle the "onLate requires sla" check
#: with one identity test instead of walking four nested report chains for
#: every job in the fleet.  The shortcut is only sound while the stock block
#: names no destination, so that is checked here rather than assumed -- a
#: plain `if`, not an `assert`, because the release binary runs under -OO.
_DEFAULT_ONLATE = DEFAULT_CONFIG["onLate"]
if _onlate_names_a_destination(  # pragma: no cover - dev invariant
    _DEFAULT_ONLATE
):
    raise RuntimeError(
        "config: DEFAULT_CONFIG['onLate'] now names a report destination, "
        "so the identity shortcut in _validate_numeric_ranges would skip "
        "the 'onLate requires sla' check for every job"
    )

# The `notify:` report block merges over these event-shaped defaults, exactly
# as onLate merges over the overdue templates.  Kept wholly separate from
# _REPORT_DEFAULTS (a deepcopy) so a daemon event's wording never leaks into a
# job's report block or its fingerprint, and give events their own sentry
# grouping keyed on the event name.
_NOTIFY_REPORT_DEFAULTS: dict[str, Any] = copy.deepcopy(_REPORT_DEFAULTS)
_NOTIFY_REPORT_DEFAULTS["mail"]["subject"] = DEFAULT_NOTIFY_SUBJECT_TEMPLATE
_NOTIFY_REPORT_DEFAULTS["mail"]["body"] = DEFAULT_NOTIFY_BODY_TEMPLATE
_NOTIFY_REPORT_DEFAULTS["webhook"]["body"] = (
    DEFAULT_NOTIFY_WEBHOOK_BODY_TEMPLATE
)
_NOTIFY_REPORT_DEFAULTS["sentry"]["body"] = (
    DEFAULT_NOTIFY_SUBJECT_TEMPLATE + "\n" + DEFAULT_NOTIFY_BODY_TEMPLATE
)
_NOTIFY_REPORT_DEFAULTS["sentry"]["fingerprint"] = [
    "cronstable",
    "{{ event }}",
    "{{ name }}",
]


_report_schema = Map(
    {
        Opt("sentry"): Map(
            {
                Opt("dsn"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("fingerprint"): Seq(Str()),
                Opt("level"): Str(),
                Opt("extra"): MapPattern(Str(), Str() | Int() | Bool()),
                Opt("body"): Str(),
                Opt("environment"): Str(),
                Opt("maxStringLength"): Int(),
            }
        ),
        Opt("mail"): Map(
            {
                "from": EmptyNone() | Str(),
                "to": EmptyNone() | Str(),
                Opt("smtpHost"): Str(),
                Opt("smtpPort"): Int(),
                Opt("subject"): Str(),
                Opt("body"): Str(),
                Opt("username"): Str(),
                Opt("password"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("tls"): Bool(),
                Opt("starttls"): Bool(),
                Opt("validate_certs"): Bool(),
                Opt("html"): Bool(),
            }
        ),
        Opt("shell"): Map(
            {
                Opt("shell"): Str(),
                "command": Str() | Seq(Str()),
                # seconds the reporter command may run before its process
                # group is killed (default 60; it runs inline on the reaper).
                Opt("timeout"): Float(),
            }
        ),
        Opt("webhook"): Map(
            {
                Opt("url"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("method"): Str(),
                Opt("contentType"): Str(),
                Opt("headers"): MapPattern(Str(), Str()),
                Opt("body"): Str(),
                Opt("timeout"): Float(),
            }
        ),
        Opt("push"): Map(
            {
                # opt this job/event into the E2E encrypted push channel;
                # the relay and device registry come from the daemon-global
                # `push:` section (required when this is enabled anywhere).
                Opt("enabled"): Bool(),
                # relayed to APNs as the interruption level: time-sensitive
                # breaks through scheduled summaries, passive does not.
                Opt("priority"): Enum(["time-sensitive", "passive"]),
                # carry the last captured output lines inside the sealed
                # payload (trimmed oldest-first to fit the APNs size cap).
                Opt("includeLogTail"): Bool(),
            }
        ),
        Opt("eventlog"): Map(
            {
                # Windows only, and a no-op elsewhere rather than a load
                # error; the load announces that once, naming every hook
                # that enabled it (see _validate_eventlog_config).
                Opt("enabled"): Bool(),
                Opt("source"): Str(),
                # publish a bounded output tail into a log every local
                # account can read; see DEFAULT_EVENTLOG_REPORT.
                Opt("includeOutput"): Bool(),
            }
        ),
    }
)

# A schedule is a crontab string or the map form below.  An explicit second
# opts into second-level scheduling (see schedule_object_to_crontab /
# cronstable.cron).  One shared fragment: jobs (where schedule is required)
# and DAGs (where it is optional) accept exactly the same shapes.
_schedule_schema = Str() | Map(
    {
        Opt("second"): Str(),
        Opt("minute"): Str(),
        Opt("hour"): Str(),
        Opt("dayOfMonth"): Str(),
        Opt("month"): Str(),
        Opt("year"): Str(),
        Opt("dayOfWeek"): Str(),
    }
)

_job_defaults_common = {
    Opt("shell"): Str(),
    Opt("concurrencyPolicy"): Enum(["Allow", "Forbid", "Replace"]),
    Opt("concurrencyScope"): Enum(["node", "cluster"]),
    Opt("clusterPolicy"): Enum(["Leader", "PreferLeader", "EveryNode"]),
    Opt("onMissed"): Enum(["skip", "run-once", "run-all"]),
    Opt("startingDeadlineSeconds"): EmptyNone() | Int(),
    Opt("catchupJitterSeconds"): Int(),
    Opt("onlyIfLastSucceeded"): Bool(),
    Opt("archiveOutput"): Bool(),
    Opt("redactArchivedSecrets"): Bool(),
    # bool enables sampling with the defaults; the map form additionally
    # tunes the sampling cadence and the per-run series retention (see
    # _normalize_monitor_resources).
    Opt("monitorResources"): Bool()
    | Map(
        {
            Opt("enabled"): Bool(),
            Opt("interval"): Float(),
            Opt("history"): Int(),
        }
    ),
    Opt("captureStderr"): Bool(),
    Opt("captureStdout"): Bool(),
    Opt("saveLimit"): Int(),
    Opt("maxLineLength"): Int(),
    Opt("utc"): Bool(),
    Opt("timezone"): Str(),
    Opt("failsWhen"): Map(
        {
            "producesStdout": Bool(),
            Opt("producesStderr"): Bool(),
            Opt("nonzeroReturn"): Bool(),
            Opt("always"): Bool(),
        }
    ),
    Opt("onFailure"): Map(
        {
            Opt("retry"): Map(
                {
                    "maximumRetries": Int(),
                    "initialDelay": Float(),
                    "maximumDelay": Float(),
                    "backoffMultiplier": Float(),
                }
            ),
            Opt("report"): _report_schema,
        }
    ),
    Opt("onPermanentFailure"): Map({Opt("report"): _report_schema}),
    Opt("onSuccess"): Map({Opt("report"): _report_schema}),
    Opt("sla"): Map(
        {
            Opt("maxTimeSinceSuccessSeconds"): EmptyNone() | Int(),
            Opt("lateAfterSeconds"): EmptyNone() | Int(),
            Opt("maxRuntimeSeconds"): EmptyNone() | Int(),
        }
    ),
    Opt("onLate"): Map({Opt("report"): _report_schema}),
    Opt("environment"): Seq(Map({"key": Str(), "value": Str()})),
    # run-scoped secrets, resolved fresh per run and served over the loopback
    # endpoint rather than the environment (never shows in
    # /proc/<pid>/environ). Needs a `state` section with jobApi enabled.
    Opt("secrets"): Seq(
        Map(
            {
                "name": Str(),
                Opt("value"): EmptyNone() | Str(),
                Opt("fromFile"): EmptyNone() | Str(),
                Opt("fromEnvVar"): EmptyNone() | Str(),
            }
        )
    ),
    # allowlist of extra scope names this job may explicitly name in its
    # loopback state calls; see cronstable.jobapi.JobStateAPI._scope.
    Opt("stateAllowedScopes"): Seq(Str()),
    Opt("env_file"): Str(),
    # ~ and ${VAR} are expanded and the result is made absolute at load (see
    # JobConfig._resolve_working_directory).  EmptyNone() so a job under a
    # `defaults:` block that sets this key still has a spelling that means
    # "inherit the daemon's CWD instead": a bare `workingDirectory:` writes
    # the inherited value back to None, like startingDeadlineSeconds above.
    Opt("workingDirectory"): EmptyNone() | Str(),
    Opt("executionTimeout"): Float(),
    Opt("killTimeout"): Float(),
    # Built from platform.PRIORITY_LEVELS so the accepted values cannot drift
    # from the per-OS tables that have to map them.  Enum is what refuses
    # `priority: realtime` loudly, naming the values that are accepted,
    # instead of silently downgrading it to something safe.
    Opt("priority"): Enum(list(platform.PRIORITY_LEVELS)),
    Opt("statsd"): Map({"prefix": Str(), "host": Str(), "port": Int()}),
    # Int() is tried first so a numeric ``user: 1000`` parses as the integer
    # 1000 (a uid/gid), reaching the isinstance(..., int) branches in
    # _resolve_user_group. With Str() first, strictyaml's union would match the
    # always-accepting Str() and a bare number would arrive as the string
    # "1000", silently looked up as a login *name* (getpwnam("1000")) instead.
    # A non-numeric name (``user: www-data``) fails Int() and uses Str().
    Opt("user"): Int() | Str(),
    Opt("group"): Int() | Str(),
    Opt("streamPrefix"): Str(),
    Opt("enabled"): Bool(),
}

_job_schema_dict = dict(_job_defaults_common)
_job_schema_dict.update(
    {
        "name": Str(),
        "command": Str() | Seq(Str()),
        "schedule": _schedule_schema,
    }
)

# Orchestration: a task is a job invocation, so it reuses the shared
# launch fields (shell/environment/capture/timeouts/user/secrets/...) and adds
# the DAG-node fields (id, dependsOn edges, node type, per-task retries,
# dynamic mapping, sensor poke schedule, approval reject policy).  ``command``
# is optional only for an approval gate (which runs no subprocess).
# The launch fields are taken from _job_defaults_common by name so the two
# schemas cannot drift; the job fields NOT named here (scheduling, concurrency,
# the onFailure retry ladder) stay job-only because a task's cadence and
# attempts are graph-driven.
_DAG_TASK_LAUNCH_KEYS = frozenset(
    {
        "shell",
        "environment",
        "captureStderr",
        "captureStdout",
        "monitorResources",
        "saveLimit",
        "maxLineLength",
        "streamPrefix",
        "failsWhen",
        "executionTimeout",
        "killTimeout",
        "priority",
        "statsd",
        "user",
        "group",
        "env_file",
        "workingDirectory",
        "secrets",
        "stateAllowedScopes",
        "onSuccess",
    }
)
_dag_task_launch_fields = {
    key: validator
    for key, validator in _job_defaults_common.items()
    if key.key in _DAG_TASK_LAUNCH_KEYS
}
_missing_launch_keys = _DAG_TASK_LAUNCH_KEYS.difference(
    key.key for key in _dag_task_launch_fields
)
if _missing_launch_keys:  # pragma: no cover - dev invariant
    raise RuntimeError(
        "config: _DAG_TASK_LAUNCH_KEYS names fields _job_defaults_common "
        "no longer defines, so the DAG-task schema would silently drop "
        "them: {}".format(", ".join(sorted(_missing_launch_keys)))
    )

_dag_task_schema_dict = dict(_dag_task_launch_fields)
_dag_task_schema_dict.update(
    {
        "id": Str(),
        Opt("command"): Str() | Seq(Str()),
        Opt("type"): Enum(["task", "sensor", "approval"]),
        Opt("dependsOn"): Seq(Str()),
        Opt("triggerRule"): Enum(["all_success", "all_done"]),
        Opt("retries"): Int(),
        Opt("retryDelaySeconds"): Int() | Float(),
        Opt("expand"): Map({"fromTask": Str(), "key": Str()}),
        Opt("pokeIntervalSeconds"): Int() | Float(),
        Opt("pokeTimeoutSeconds"): Int() | Float(),
        Opt("pokeJitterSeconds"): Int() | Float(),
        Opt("onReject"): Enum(["fail", "skip"]),
        # Report-only, unlike the job-level onFailure (no `retry` key): a
        # task's attempts are graph-driven, so a retry ladder here would be
        # dead config. Fired by Cron._handle_finished_dag_task per task run.
        Opt("onFailure"): Map({Opt("report"): _report_schema}),
    }
)

_dag_schema_dict = {
    "name": Str(),
    Opt("schedule"): _schedule_schema,
    Opt("timezone"): Str(),
    Opt("utc"): Bool(),
    Opt("onMissed"): Enum(["skip", "run-once", "run-all"]),
    Opt("startingDeadlineSeconds"): EmptyNone() | Int(),
    Opt("catchupJitterSeconds"): Int(),
    Opt("clusterPolicy"): Enum(["Leader", "PreferLeader", "EveryNode"]),
    Opt("enabled"): Bool(),
    Opt("retainRuns"): Int(),
    "tasks": Seq(Map(_dag_task_schema_dict)),
}

# Bearer-token scopes for the web control API (web.authTokens[].scopes):
# `view` = read-only GETs; `control` = mutating POSTs; `approve` = the DAG
# approval decision only. `control`/`approve` imply `view`; the scalar
# web.authToken is all-scopes. Enforcement lives in
# cronstable.cron.Cron._make_auth_middleware / _required_web_scope.
WEB_TOKEN_SCOPES = ("view", "control", "approve")

CONFIG_SCHEMA = EmptyDict() | Map(
    {
        Opt("defaults"): Map(_job_defaults_common),
        Opt("jobs"): Seq(Map(_job_schema_dict)),
        Opt("dags"): Seq(Map(_dag_schema_dict)),
        Opt("web"): Map(
            {
                "listen": Seq(Str()),
                Opt("headers"): MapPattern(Str(), Str()),
                # extra exact-match browser Origins allowed to call the
                # MUTATING web endpoints. Same-origin and no-Origin clients
                # always pass; a foreign Origin is refused (403) as a
                # CSRF/DNS-rebinding defense. See
                # cronstable.cron.Cron._make_origin_middleware.
                Opt("allowedOrigins"): Seq(Str()),
                # optional opt-in bearer-token auth for the web API
                Opt("authToken"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                # additional per-device *scoped* bearer tokens (same
                # value/fromFile/fromEnvVar sources, a `scopes` list, an
                # optional `label` for revocation). Written block-style
                # (strictyaml rejects flow lists). See
                # cronstable.cron.Cron._resolve_web_tokens.
                Opt("authTokens"): Seq(
                    Map(
                        {
                            Opt("value"): EmptyNone() | Str(),
                            Opt("fromFile"): EmptyNone() | Str(),
                            Opt("fromEnvVar"): EmptyNone() | Str(),
                            "scopes": Seq(Enum(list(WEB_TOKEN_SCOPES))),
                            Opt("label"): Str(),
                        }
                    )
                ),
                # scopes granted to requests that present no credential at
                # all. Requires at least one authToken/authTokens entry (see
                # _validate_web_config); a wrong or unknown presented token
                # still 401s rather than degrading to anonymous.
                Opt("anonymousScopes"): Seq(Enum(["view"])),
                # octal permissions to apply to a unix:// listen socket
                Opt("socketMode"): Str(),
                # native TLS for the `https://` entries in `listen`;
                # `cert`/`key` required together. `clientCa` additionally
                # REQUIRES a client certificate signed by that CA (mutual
                # TLS): the CA file is the caller allowlist, so point it at
                # a dedicated CA, never a shared organisational one. In-place
                # rotation restarts the listener; see
                # cronstable.cron.Cron._web_tls_files_changed.
                Opt("tls"): Map(
                    {
                        Opt("cert"): EmptyNone() | Str(),
                        Opt("key"): EmptyNone() | Str(),
                        Opt("clientCa"): EmptyNone() | Str(),
                    }
                ),
                # serve the browser dashboard at "/" (default true)
                Opt("ui"): Bool(),
                # Prometheus exposition at GET /metrics (default on). The map
                # form can make /metrics public (no authToken) and override
                # the histogram buckets. See cronstable/prometheus.py.
                Opt("metrics"): Bool()
                | Map(
                    {
                        Opt("enabled"): Bool(),
                        Opt("public"): Bool(),
                        Opt("durationBuckets"): Seq(Float()),
                    }
                ),
                # node CPU/memory history ring for the dashboard's node chart
                # (GET /node/history). On by default; the map form tunes
                # cadence and window size.
                Opt("nodeHistory"): Bool()
                | Map(
                    {
                        Opt("enabled"): Bool(),
                        Opt("interval"): Float(),
                        Opt("points"): Int(),
                    }
                ),
                # opt-in mDNS/Bonjour advert of the web API
                # (`_cronstable._tcp`). Needs the `discovery` extra and a
                # TCP listen address; the map form overrides the instance
                # name. See cronstable.discovery.
                Opt("bonjour"): Bool()
                | Map(
                    {
                        Opt("enabled"): Bool(),
                        Opt("name"): Str(),
                    }
                ),
            }
        ),
        # Optional MCP server: expose jobs/DAGs/cluster/state as MCP tools on
        # the web listeners (POST /mcp) and a stdio bridge. Defaults live in
        # DEFAULT_MCP; off unless `enabled: true`. See cronstable.mcp.
        Opt("mcp"): EmptyDict()
        | Map(
            {
                Opt("enabled"): Bool(),
                Opt("readOnly"): Bool(),
                Opt("toolsets"): Seq(Enum(list(MCP_TOOLSETS))),
                Opt("allowedOrigins"): Seq(Str()),
                Opt("allowUnauthenticated"): Bool(),
                Opt("resources"): Bool(),
                Opt("prompts"): Bool(),
                Opt("instructions"): EmptyNone() | Str(),
                Opt("maxRows"): Int(),
                Opt("maxBodyBytes"): Int(),
            }
        ),
        # Optional cluster section: gate scheduled jobs on a leadership
        # backend (gossip mesh or a lease store; see cronstable.cluster /
        # .backends). listen/tls/peers are required for gossip only, enforced
        # in _build_cluster_config rather than the schema so a lease backend
        # need not carry them.
        Opt("cluster"): Map(
            {
                # gossip (default) | kubernetes | etcd | filesystem
                Opt("backend"): Enum(
                    ["gossip", "kubernetes", "etcd", "filesystem"]
                ),
                # --- gossip transport (required for backend: gossip) ---
                # host:port the mTLS cluster listener binds to
                Opt("listen"): Str(),
                Opt("tls"): Map(
                    {
                        "ca": Str(),  # trust anchor for peer certificates
                        "cert": Str(),  # this node's certificate
                        "key": Str(),  # this node's private key
                    }
                ),
                Opt("peers"): Seq(Map({"host": Str()})),
                Opt("nodeName"): Str(),
                Opt("interval"): Int(),
                Opt("driftAfter"): Int(),
                Opt("connectTimeout"): Int(),
                # run scheduled jobs on the elected leader only (default false;
                # implicitly true for the lease backends)
                Opt("electLeader"): Bool(),
                # how leader-gated jobs spread across the quorate cluster
                # (gossip only; rejected for the lease backends)
                Opt("distribution"): Enum(["single-leader", "spread"]),
                # --- kubernetes Lease backend (backend: kubernetes) ---
                Opt("kubernetes"): Map(
                    {
                        Opt("leaseName"): Str(),
                        Opt("leaseNamespace"): EmptyNone() | Str(),
                        Opt("leaseDurationSeconds"): Int(),
                        Opt("renewDeadlineSeconds"): Int(),
                        Opt("retryPeriodSeconds"): Int(),
                        Opt("identity"): EmptyNone() | Str(),
                        Opt("kubeconfig"): EmptyNone() | Str(),
                        Opt("apiServer"): EmptyNone() | Str(),
                        Opt("clientLibrary"): Enum(
                            ["auto", "http", "library"]
                        ),
                    }
                ),
                # --- etcd lease-backed election backend (backend: etcd) ---
                Opt("etcd"): Map(
                    {
                        Opt("endpoints"): Seq(Str()),
                        Opt("electionName"): Str(),
                        Opt("ttl"): Int(),
                        Opt("username"): EmptyNone() | Str(),
                        Opt("password"): Map(
                            {
                                Opt("value"): EmptyNone() | Str(),
                                Opt("fromFile"): EmptyNone() | Str(),
                                Opt("fromEnvVar"): EmptyNone() | Str(),
                            }
                        ),
                        Opt("tls"): Map(
                            {
                                Opt("ca"): EmptyNone() | Str(),
                                Opt("cert"): EmptyNone() | Str(),
                                Opt("key"): EmptyNone() | Str(),
                            }
                        ),
                    }
                ),
                # --- shared-mount election backend (backend: filesystem) ---
                Opt("filesystem"): Map(
                    {
                        Opt("path"): Str(),
                        Opt("electionName"): Str(),
                        Opt("ttl"): Int() | Float(),
                        Opt("deploymentId"): EmptyNone() | Str(),
                        Opt("topology"): Enum(
                            ["auto", "single-node", "shared"]
                        ),
                    }
                ),
                # --- observability overlay: gossip as a secondary data plane -
                # Share per-node CPU/memory across the cluster for the fleet
                # view. Under backend: gossip the election mesh already
                # carries it (transport and tuning keys are rejected); under
                # a lease backend this stands up a dedicated election-inert
                # gossip mesh, so listen/tls/peers are required. See
                # cronstable.cron.start_stop_observability and
                # _attach_observability.
                Opt("observability"): Map(
                    {
                        Opt("shareNodeStats"): Bool(),
                        Opt("listen"): Str(),
                        Opt("tls"): Map(
                            {
                                "ca": Str(),
                                "cert": Str(),
                                "key": Str(),
                            }
                        ),
                        Opt("peers"): Seq(Map({"host": Str()})),
                        Opt("nodeName"): Str(),
                        Opt("interval"): Int(),
                        Opt("driftAfter"): Int(),
                        Opt("connectTimeout"): Int(),
                    }
                ),
            }
        ),
        # Optional state section: an opt-in durable store (local path or
        # shared POSIX mount) enabling durable history, missed-run catch-up
        # and, on a shared mount, HA coordination. Absent, everything stays
        # in memory. See cronstable.state.
        Opt("state"): Map(
            {
                "path": Str(),
                Opt("topology"): Enum(["auto", "single-node", "shared"]),
                Opt("deploymentId"): Str(),
                Opt("maxRunsPerJob"): Int(),
                Opt("onStoreUnavailable"): Enum(["degrade", "fail-closed"]),
                Opt("gcGraceSeconds"): Int(),
                Opt("maxOpsPerSecond"): Int() | Float(),
                Opt("slotTtlSeconds"): Int() | Float(),
                # the job-facing state API: the loopback endpoint
                # and the `cronstable state|cursor|lock|artifact|...` commands.
                # See cronstable.jobapi. Defaults filled from DEFAULT_JOB_API.
                Opt("jobApi"): Map(
                    {
                        Opt("enabled"): Bool(),
                        Opt("listen"): Str(),
                        Opt("maxValueBytes"): Int(),
                        Opt("maxArtifactBytes"): Int(),
                        Opt("lockTtlSeconds"): Int() | Float(),
                        Opt("allowNonLoopbackBind"): Bool(),
                        # native TLS for an `https://` listen. Note `ca` here
                        # is the CLIENT-side trust anchor handed to jobs as
                        # CRONSTABLE_STATE_CACERT, the opposite direction
                        # from web.tls.clientCa (which authenticates callers).
                        Opt("tls"): Map(
                            {
                                Opt("cert"): EmptyNone() | Str(),
                                Opt("key"): EmptyNone() | Str(),
                                Opt("ca"): EmptyNone() | Str(),
                            }
                        ),
                    }
                ),
            }
        ),
        Opt("include"): Seq(Str()),
        Opt("logging"): Map(
            {
                "version": Int(),
                Opt("incremental"): Bool(),
                Opt("disable_existing_loggers"): Bool(),
                Opt("formatters"): YamlAny(),
                Opt("filters"): YamlAny(),
                Opt("handlers"): YamlAny(),
                Opt("loggers"): YamlAny(),
                Opt("root"): YamlAny(),
            }
        ),
        # Optional daemon-level event notifications: fan NOTIFY_EVENTS out to
        # the same reporters a job uses.  `events` is an optional allow-list
        # (default = every event).  See cronstable.cron.Cron._dispatch_notify.
        Opt("notify"): Map(
            {
                Opt("events"): Seq(Enum(list(NOTIFY_EVENTS))),
                Opt("report"): _report_schema,
            }
        ),
        # Optional E2E encrypted push alerts (cronstable.push): the relay
        # endpoint and paired-device registry the `push` reporter needs.
        # The relay URL is explicit and required: the daemon never posts
        # alert ciphertext anywhere the operator did not spell out.
        Opt("push"): Map(
            {
                "relay": Map(
                    {
                        "url": Str(),
                        Opt("timeout"): Float(),
                    }
                ),
                # registry storage for stateless installs; with a `state:`
                # section the registry rides the durable store instead
                # (cluster-visible). One of the two must exist.
                Opt("devicesFile"): EmptyNone() | Str(),
                # The escape hatch for the fail-closed web-auth gate, the
                # twin of mcp.allowUnauthenticated: serve the /push/devices
                # pairing endpoints on a routable listener that has no
                # web.authToken. See _validate_push_config.
                Opt("allowUnauthenticated"): Bool(),
            }
        ),
    }
)


_MONITOR_SAMPLING_DEFAULTS: Optional[tuple[float, int]] = None


def _monitor_sampling_defaults() -> tuple[float, int]:
    """``(interval, history)`` defaults for the monitorResources block.

    Imported lazily: ``cronstable.resources`` owns the two literals but
    drags asyncio and psutil into every importer of this module, so the cost
    is deferred to the first job actually built and the constants keep their
    single definition.
    """
    global _MONITOR_SAMPLING_DEFAULTS
    if _MONITOR_SAMPLING_DEFAULTS is None:
        from cronstable.resources import (
            MONITOR_HISTORY_DEFAULT,
            SAMPLE_INTERVAL,
        )

        _MONITOR_SAMPLING_DEFAULTS = (SAMPLE_INTERVAL, MONITOR_HISTORY_DEFAULT)
    return _MONITOR_SAMPLING_DEFAULTS


def __getattr__(name: str) -> float | int:
    """Serve the two resources constants without importing them eagerly.

    Callers read ``SAMPLE_INTERVAL`` / ``MONITOR_HISTORY_DEFAULT`` as module
    attributes; PEP 562 keeps that spelling while the import stays deferred.
    The return annotation is deliberately narrow rather than ``Any``, so a
    typo'd attribute still fails to type-check where its value is used.
    """
    if name == "SAMPLE_INTERVAL":
        return _monitor_sampling_defaults()[0]
    if name == "MONITOR_HISTORY_DEFAULT":
        return _monitor_sampling_defaults()[1]
    raise AttributeError(
        "module {!r} has no attribute {!r}".format(__name__, name)
    )


def _normalize_monitor_resources(raw: Any) -> tuple[bool, float, int]:
    """Collapse ``monitorResources``'s bool-or-map forms to one shape.

    Returns ``(enabled, interval, history)``: the map form reads its three
    optional keys (``enabled`` defaulting to true, so writing the map at all
    turns monitoring on), the bool form takes the sampling defaults.  Range
    checks live with the other numeric checks in ``_validate_numeric_ranges``.
    """
    interval, history = _monitor_sampling_defaults()
    if isinstance(raw, dict):
        return (
            bool(raw.get("enabled", True)),
            float(raw.get("interval", interval)),
            int(raw.get("history", history)),
        )
    return (bool(raw), interval, history)


def _merge_lists(key: str, base: list, override: list) -> list:
    """Combine two list values under the defaults-merge rules.

    Most lists concatenate (defaults first, override appended), with three
    key-specific exceptions:

    - ``environment`` is a list of ``{key, value}``: merge by variable name
      so a job's variable overrides the default instead of producing a
      duplicate-keyed concatenation.
    - ``secrets`` is a list of ``{name, ...}``: merge by secret name so a
      job's secret overrides a same-named default rather than staging two
      secrets under one name (mirrors ``environment``).
    - sentry ``fingerprint`` is replace-not-append: a job (or defaults
      block) that supplies its own fingerprint must override the default
      entirely -- concatenation would silently prepend the three default
      entries, making custom Sentry issue grouping impossible.
    """
    if key == "environment":
        by_name = {entry["key"]: entry["value"] for entry in base}
        for entry in override:
            by_name[entry["key"]] = entry["value"]
        return [{"key": k, "value": v} for k, v in by_name.items()]
    if key == "secrets":
        by_name = {entry["name"]: entry for entry in base}
        for entry in override:
            by_name[entry["name"]] = entry
        return list(by_name.values())
    if key == "fingerprint":
        return override
    return base + override


def mergedicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    """Merge config mapping ``dict2`` over ``dict1`` (the defaults).

    The override side wins for a plain value; two dicts merge recursively;
    two lists combine per :func:`_merge_lists`.  A dict overridden by
    ``None`` keeps the dict (an empty YAML section parses as ``None`` and
    must not wipe out a populated default section).  Keys are emitted in
    ``dict1`` order, then ``dict2``-only keys in their own order.
    """
    merged: dict[str, Any] = dict(dict1)
    for key, override in dict2.items():
        if key not in merged:
            merged[key] = override
            continue
        base = merged[key]
        if isinstance(base, dict):
            if isinstance(override, dict):
                merged[key] = mergedicts(base, override)
                continue
            if override is None:
                merged[key] = mergedicts(base, {})
                continue
        if isinstance(base, list) and isinstance(override, list):
            merged[key] = _merge_lists(key, base, override)
            continue
        merged[key] = override
    return merged


def schedule_object_to_crontab(spec: dict[str, Any]) -> str:
    """Render the object form of a ``schedule`` to a crontab string.

    Field layout: ``[second] minute hour dayOfMonth month dayOfWeek [year]``.
    Only the columns actually specified are emitted, so a schedule using
    neither ``second`` nor ``year`` renders as the classic 5-field line,
    keeping its job-set fingerprint stable.

    The single source of truth for the object->crontab mapping, shared by
    :meth:`JobConfig._parse_schedule`, :func:`cronstable.cron.schedule_str`
    and :func:`cronstable.fingerprint._schedule_repr`, so those cannot drift.

    Every value must render as exactly ONE non-empty, whitespace-free token;
    anything else raises :class:`ConfigError` naming the key.  The rendered
    line is re-read under a whitespace split, so a BLANK value would
    silently delete its column and an embedded space would inject one,
    shifting every later field.  ``str.split()`` is the exact predicate the
    engine splits with, so exotic Unicode whitespace is refused here too.
    """
    minute = _schedule_field(spec, "minute")
    hour = _schedule_field(spec, "hour")
    day = _schedule_field(spec, "dayOfMonth")
    month = _schedule_field(spec, "month")
    dow = _schedule_field(spec, "dayOfWeek")
    second = _schedule_field(spec, "second", default=None)
    year = _schedule_field(spec, "year", default=None)
    if second is not None:
        # 7-field: an explicit seconds column. year defaults to "*" (any).
        return "{} {} {} {} {} {} {}".format(
            second,
            minute,
            hour,
            day,
            month,
            dow,
            year if year is not None else "*",
        )
    if year is not None:
        # 6-field: the cron engine reads the trailing column as the year.
        return "{} {} {} {} {} {}".format(minute, hour, day, month, dow, year)
    return "{} {} {} {} {}".format(minute, hour, day, month, dow)


def _schedule_field(
    spec: dict[str, Any], key: str, default: Optional[str] = "*"
) -> Optional[str]:
    """One validated cron column of an object-form ``schedule:``.

    ``default`` when the key is absent (or set to null); otherwise the
    value rendered to a string, required to be a single non-empty token
    with no whitespace of any kind -- see
    :func:`schedule_object_to_crontab` for why anything looser corrupts
    neighbouring columns.
    """
    value = spec.get(key)
    if value is None:
        return default
    token = "{}".format(value)
    if token.split() != [token]:
        raise ConfigError(
            "schedule.{}: must be a single non-empty cron field with no "
            "whitespace, got {!r} (a blank value would drop this column "
            "and an embedded space would add one, silently shifting every "
            "later field; omit the key entirely for its default)".format(
                key, value
            )
        )
    return token


def schedule_has_seconds(
    schedule_unparsed: str | dict[str, Any],
) -> bool:
    """Whether a schedule pins specific seconds (fires at second granularity).

    True for the object ``second:`` key and for a full 7-field crontab string;
    such jobs make the scheduler tick once per second rather than once per
    minute (see :meth:`cronstable.cron.Cron._needs_subminute`).  A 5- or
    6-field
    string, ``@reboot`` and the ``@daily``/``@hourly``/... nicknames never do.
    """
    if isinstance(schedule_unparsed, dict):
        # Derive from the ACTUAL rendered field count, not key presence:
        # ``second: null`` renders no seconds column at all, and keying off
        # presence alone would force per-second ticking for a job that only
        # ever fires once a minute.
        return len(schedule_object_to_crontab(schedule_unparsed).split()) == 7
    if isinstance(schedule_unparsed, str):
        stripped = schedule_unparsed.strip()
        if not stripped or stripped.startswith("@"):
            return False
        # the cron engine only reads a leading seconds column at 7 fields; a 5-
        # or 6-field line has an implicit second of 0 (6th field is the year).
        return len(stripped.split()) == 7
    return False


#: Per-parse memo for :meth:`JobConfig._lint_schedule`, keyed by
#: ``(source text, H-resolved text, resolved zone)``.  Created by
#: :func:`_config_from_doc` and dropped when that parse returns.
LintCache = dict[tuple[str, str, Optional[datetime.tzinfo]], list[Finding]]

#: Shared empty threshold mapping for every job with no SLA check (the vast
#: majority): saves one dict per job, and the read-only proxy makes the
#: sharing safe by construction (an accidental edit fails loudly).
_NO_SLA_THRESHOLDS: Mapping[str, Any] = types.MappingProxyType({})

#: The findings of a clean schedule, shared by every such job: a fresh
#: empty list per JobConfig (and its JSON twin) is a GC-tracked container
#: walked on every full collection.  Read-only by convention, like
#: _NO_SLA_THRESHOLDS; a list rather than a tuple because the payload
#: serializes it and consumers compare it to ``[]``.
_NO_FINDINGS: list[Finding] = []
_NO_FINDINGS_JSON: list[dict[str, Any]] = []


class JobConfig:
    # __slots__ cuts steady-state memory (one JobConfig per job, rebuilt on
    # every reload) and speeds attribute access on the scheduler's hot path.
    # Every attribute the class ever sets must be listed here.  Nothing
    # outside this class assigns attributes to a JobConfig instance, so the
    # set is closed.
    __slots__ = (
        "name",
        "command",
        "schedule_unparsed",
        "schedule",
        "schedule_findings",
        "has_seconds",
        "shell",
        "concurrencyPolicy",
        "concurrencyScope",
        "clusterPolicy",
        "onMissed",
        "startingDeadlineSeconds",
        "catchupJitterSeconds",
        "onlyIfLastSucceeded",
        "archiveOutput",
        "redactArchivedSecrets",
        "monitorResources",
        "monitorResourcesInterval",
        "monitorResourcesHistory",
        "captureStderr",
        "captureStdout",
        "streamPrefix",
        "saveLimit",
        "maxLineLength",
        "utc",
        "enabled",
        "timezone",
        "failsWhen",
        "onFailure",
        "onPermanentFailure",
        "onSuccess",
        "sla",
        "has_sla",
        "onLate",
        "env_file",
        "environment",
        "secrets",
        "stateAllowedScopes",
        "workingDirectory",
        "executionTimeout",
        "killTimeout",
        "priority",
        "statsd",
        "user",
        "group",
        "uid",
        "gid",
        "username",
        # Precomputed payload fragments (see _precompute_payload_views).
        "schedule_display",
        "command_display",
        "schedule_findings_json",
        "schedule_resolved_or_none",
        "sla_thresholds",
        # Memoized job digest, filled on demand by
        # cronstable.fingerprint.job_digest_cached (never by this class).
        "_digest",
    )

    def __init__(
        self,
        config: dict,
        env_cache: Optional[dict[str, dict[str, str]]] = None,
        lint_cache: Optional["LintCache"] = None,
    ) -> None:
        self.name: str = config["name"]
        self.command: str | list[str] = config["command"]
        self.schedule_unparsed = config.pop("schedule")
        self.schedule: CronTab | str = self._parse_schedule(
            self.schedule_unparsed,
            config.pop(crontabs.PARSED_SCHEDULE_KEY, None),
        )
        # True when the schedule pins specific seconds; the scheduler then
        # ticks per-second for this job instead of per-minute.
        self.has_seconds: bool = schedule_has_seconds(self.schedule_unparsed)
        self.shell = config.pop("shell")
        self.concurrencyPolicy = config.pop("concurrencyPolicy")
        # cluster scope reaches across nodes, so it IS fingerprinted (like
        # clusterPolicy) -- but only when set, so pre-existing configs keep
        # their digests (see cronstable.fingerprint.canonical_job).
        self.concurrencyScope = config.pop("concurrencyScope")
        self.clusterPolicy = config.pop("clusterPolicy")
        # Catch-up config is deliberately NOT part of the job-set fingerprint
        # (cronstable.fingerprint): restart-time, node-local behaviour, so no
        # SCHEME_VERSION bump.  Same for the archival pair and for sla/onLate
        # (alerting-only).  onlyIfLastSucceeded IS fingerprinted, like
        # `enabled`: it gates every scheduled fire, so replicas disagreeing
        # on it must show as drift.
        self.onMissed = config.pop("onMissed")
        self.startingDeadlineSeconds = config.pop("startingDeadlineSeconds")
        self.catchupJitterSeconds = config.pop("catchupJitterSeconds")
        self.onlyIfLastSucceeded = config.pop("onlyIfLastSucceeded")
        self.archiveOutput = config.pop("archiveOutput")
        self.redactArchivedSecrets = config.pop("redactArchivedSecrets")
        # normalized from the bool-or-map config forms: a plain bool switch
        # plus the sampling cadence and per-run series retention beside it.
        (
            self.monitorResources,
            self.monitorResourcesInterval,
            self.monitorResourcesHistory,
        ) = _normalize_monitor_resources(config.pop("monitorResources"))
        self.captureStderr = config.pop("captureStderr")
        self.captureStdout = config.pop("captureStdout")
        self.streamPrefix = config.pop("streamPrefix")
        self.saveLimit = config.pop("saveLimit")
        self.maxLineLength = config.pop("maxLineLength")
        self.utc = config.pop("utc")
        self.enabled: bool = config.pop("enabled")
        # depends on self.utc, so resolve after it is set
        self.timezone: Optional[datetime.tzinfo] = self._resolve_timezone(
            config.pop("timezone")
        )
        # Advisory schedule lint, logged so the load that introduces a
        # footgun says so immediately, and kept on the job for the status
        # payloads.  A dead schedule stays a WARNING, not an error: a fixed
        # past year is the working idiom for parking a job, and failing the
        # load over it would turn an upgrade into an outage.
        self.schedule_findings: list[Finding] = self._lint_schedule(lint_cache)
        for finding in self.schedule_findings:
            logger.log(
                logging.WARNING
                if finding.level == "warning"
                else logging.INFO,
                "job %r: schedule %r: [%s] %s",
                self.name,
                str(self.schedule),
                finding.code,
                finding.message,
            )

        self.failsWhen = config.pop("failsWhen")
        self.onFailure = config.pop("onFailure")
        self.onPermanentFailure = config.pop("onPermanentFailure")
        self.onSuccess = config.pop("onSuccess")
        self.sla = config.pop("sla")
        # True iff any sla threshold is set: lets the per-minute monitor skip
        # jobs with no check (see cronstable.cron.Cron._sla_periodic).
        self.has_sla = any(v is not None for v in self.sla.values())
        self.onLate = config.pop("onLate")

        self.env_file = config.pop("env_file")
        self.environment = config.pop("environment")
        self.secrets = config.pop("secrets")
        self._validate_secrets()
        self.stateAllowedScopes = config.pop("stateAllowedScopes")
        if self.env_file is not None:
            self._merge_env_file(env_cache)
        # Where the job's process starts; None keeps the daemon's own CWD.
        # Deliberately NOT fingerprinted, for the reason the environment
        # VALUES are not (see cronstable.fingerprint.canonical_job): a path
        # is per-host, and a Windows replica running the same logical job
        # from D:\jobs must not read as drift against a Linux one on
        # /srv/jobs.
        self.workingDirectory: Optional[str] = config.pop("workingDirectory")
        self._resolve_working_directory()

        self.executionTimeout = config.pop("executionTimeout")
        self.killTimeout = config.pop("killTimeout")
        # How the job's process tree is scheduled.  Unlike workingDirectory
        # this IS fingerprinted (see cronstable.fingerprint.canonical_job):
        # the level is host-independent, and it changes how the job runs, so
        # replicas that disagree on it must show as drift.
        self.priority = config.pop("priority")
        self.statsd = config.pop("statsd")

        self.uid: int | None = None
        self.gid: int | None = None
        # Resolved login name of the target user, used by the child process'
        # privilege-drop (os.initgroups) so it gets the user's supplementary
        # groups instead of inheriting root's. None when unknown.
        self.username: Optional[str] = None
        self._resolve_user_group(config)
        self._warn_if_priority_needs_privilege()

        self._validate_numeric_ranges()

        # Memoized job digest slot, filled at most once per instance by
        # cronstable.fingerprint.job_digest_cached. A reload rebuilds every
        # JobConfig, so the memo cannot outlive the definition it describes
        # (tests/test_fingerprint.py pins that).
        self._digest: Optional[str] = None

        self._precompute_payload_views()

    def _precompute_payload_views(self) -> None:
        """Derive the per-job fragments the /jobs payload rebuilds each poll.

        Pure functions of already-set attributes, computed once at build
        time; JobConfigs are rebuilt wholesale on reload, so there is no
        invalidation hook to keep in step.  The shared values
        (``schedule_findings_json``, ``sla_thresholds``) are READ-ONLY to
        consumers: serialized into the payload, never edited.
        """
        unparsed = self.schedule_unparsed
        self.schedule_display: str = (
            unparsed
            if isinstance(unparsed, str)
            else schedule_object_to_crontab(unparsed)
        )
        command = self.command
        self.command_display: str = (
            command if isinstance(command, str) else " ".join(command)
        )
        findings = self.schedule_findings
        self.schedule_findings_json: list[dict[str, Any]] = (
            [finding._asdict() for finding in findings]
            if findings
            else _NO_FINDINGS_JSON
        )
        # The plain-dialect spelling an ``H`` schedule resolved to, else None
        # (the common case; the payload builder just tests for None).
        schedule = self.schedule
        resolved: Optional[str] = None
        if isinstance(schedule, CronTab) and schedule.resolved_differs:
            resolved = schedule.resolved_source
        self.schedule_resolved_or_none = resolved
        # The non-None sla thresholds; jobs with no check share one empty
        # read-only mapping (never written, so sharing is safe).
        self.sla_thresholds: Mapping[str, Any] = (
            {k: v for k, v in self.sla.items() if v is not None}
            if self.has_sla
            else _NO_SLA_THRESHOLDS
        )

    def _lint_schedule(
        self, lint_cache: Optional["LintCache"]
    ) -> list[Finding]:
        """Advisory findings for this job's schedule.

        ``lint_cache`` is a per-parse memo: fleets repeat schedule shapes,
        and the lint is the largest term in building a JobConfig.  The key
        includes BOTH the source and the H-resolved text so a literal
        schedule cannot collide with an ``H`` schedule that resolved to it.
        The cache must stay per parse, never process-lifetime: never-fires
        and the DST findings are answers about *now*.
        """
        tab = self.schedule
        if not isinstance(tab, CronTab):
            return _NO_FINDINGS
        if lint_cache is None:
            return lint_schedule(timezone=self.frame, tab=tab) or _NO_FINDINGS
        key = (str(tab), tab.resolved_source, self.frame)
        findings = lint_cache.get(key)
        if findings is None:
            findings = lint_schedule(timezone=self.frame, tab=tab)
            lint_cache[key] = findings
        # a copy, so every job with findings owns its own list; a clean
        # schedule shares the one empty list
        return list(findings) if findings else _NO_FINDINGS

    def _parse_schedule(
        self, schedule_unparsed, prebuilt: Optional[CronTab] = None
    ) -> CronTab | str:
        """Resolve the ``schedule:`` value to a CronTab (or ``@reboot``).

        ``prebuilt`` is the CronTab the classic-crontab front end already
        parsed from this very ``(expression, job name)`` pair; see
        :data:`cronstable.crontabs.PARSED_SCHEDULE_KEY`.  Nothing else can
        supply one: the YAML schema's key space is closed.
        """
        if isinstance(schedule_unparsed, str):
            if schedule_unparsed == "@reboot":
                return schedule_unparsed
            if isinstance(prebuilt, CronTab):
                return prebuilt
            return self._crontab(schedule_unparsed)
        if isinstance(schedule_unparsed, dict):
            tab = schedule_object_to_crontab(schedule_unparsed)
            logger.debug("Converted schedule to %r", tab)
            return self._crontab(tab)
        raise ConfigError("invalid schedule: {!r}".format(schedule_unparsed))

    def _crontab(self, tab: str) -> CronTab:
        # Surface CronTab's ValueError as a ConfigError naming the offending
        # expression.  The job's name seeds the H hash form (self.name is
        # assigned before the schedule parses), so an H slot is stable across
        # restarts, reloads and replicas.
        try:
            return CronTab(tab, hash_key=self.name)
        except ValueError as err:
            raise ConfigError(
                "invalid schedule {!r}: {}".format(tab, err)
            ) from err

    def _resolve_timezone(
        self, timezone: Optional[str]
    ) -> Optional[datetime.tzinfo]:
        if timezone is not None:
            try:
                return ZoneInfo(timezone)
            # ZoneInfo raises ZoneInfoNotFoundError (unknown zone), ValueError
            # (embedded NUL) or OSError (over-long/OS-invalid component).
            # Catch all three so a bad timezone fails the load as a
            # ConfigError, not a raw traceback.
            except (ZoneInfoNotFoundError, ValueError, OSError) as err:
                raise ConfigError(
                    "unknown timezone: {}".format(timezone)
                ) from err
        if self.utc:
            return datetime.timezone.utc
        return None

    @property
    def frame(self) -> datetime.tzinfo:
        """The zone the schedule is read in: ``timezone``, or the host clock
        (:data:`LOCAL_ZONE`) for a ``utc: false`` job without one."""
        return self.timezone or LOCAL_ZONE

    def next_delay(self, now_utc: datetime.datetime) -> Optional[float]:
        """Seconds from the aware instant ``now_utc`` to the schedule's next
        occurrence in :attr:`frame`; None when it never occurs again."""
        tab = self.schedule
        assert isinstance(tab, CronTab)
        if self.timezone is None:
            return tab.next_local(now_utc)
        return tab.next(now=now_utc.astimezone(self.timezone))

    def _validate_secrets(self) -> None:
        """Reject secret blocks that name no source.

        A sourceless secret could only ever stage empty: catch it at load.
        Whether a *configured* source resolves non-empty is checked when the
        run stages it (cronstable.jobapi), fail-closed like every other
        secret.
        """
        for entry in self.secrets:
            if not (
                entry.get("value")
                or entry.get("fromFile")
                or entry.get("fromEnvVar")
            ):
                raise ConfigError(
                    "job {!r}: secret {!r} needs a value, fromFile or "
                    "fromEnvVar source".format(self.name, entry.get("name"))
                )

    def _merge_env_file(
        self, env_cache: Optional[dict[str, dict[str, str]]] = None
    ) -> None:
        # Within one parse many jobs commonly share an env_file; read+parse
        # it once and reuse the result.  The cached dict is treated as
        # immutable (the config-var overlay below runs on a private copy),
        # and abspath keying matches how parse_config_with_sources records
        # the file so a later edit still busts the reparse signature.
        abspath = os.path.abspath(self.env_file)
        cached = env_cache.get(abspath) if env_cache is not None else None
        if cached is None:
            try:
                cached = parse_environment_file(self.env_file)
            except OSError as e:
                raise ConfigError(
                    "Could not load env_file: {}".format(e)
                ) from e
            if env_cache is not None:
                env_cache[abspath] = cached
        file_environs = dict(cached)
        # config-defined variables override those loaded from the file
        config_environs = {
            env["key"]: env["value"] for env in self.environment
        }
        file_environs.update(config_environs)
        self.environment = [
            {"key": key, "value": value}
            for key, value in file_environs.items()
        ]

    def _resolve_working_directory(self) -> None:
        """Normalize ``workingDirectory`` to an absolute path, once, at load.

        ``~`` is expanded first, for the reason the filesystem state backend
        expands it (see cronstable.state): ``~/jobs`` must mean the home
        directory, not a literal ``~`` under whatever directory the daemon
        happened to start in.  On a job that also sets ``user``, note that
        the expansion resolves against the DAEMON user's home, because it
        happens at load and the demotion happens per run; operators who want
        the target user's home should write ``${HOME}`` in an environment
        the demoted child sees instead.

        ``abspath`` then settles a relative value against the daemon's CWD
        at load time rather than at fire time, so the directory a job runs
        in is decided once, and the resolved form is what the spawn log and
        any launch failure show.

        There is deliberately no absolute-only rejection.  ``ntpath.isabs``
        was rewritten in 3.13 and answers differently for the same string
        across the interpreters this project supports, so gating on it would
        make ``\\\\fileserver\\share\\jobs`` legal config on one Python and a
        load failure on another.  cronstable.job.shell_spawn already carries
        that lesson for shell paths; which directory a job runs in must not
        depend on the interpreter that scheduled it either.

        Existence is not checked.  Config load runs on every hot reload and
        under ``--validate-config`` on machines that are not the target
        host, so one job naming a share that is not mounted yet must not
        fail the whole load.  The OS checks at spawn, where a bad directory
        lands on RunningJob.start's start_failed path like any command that
        cannot launch.
        """
        configured = self.workingDirectory
        if not configured:
            # Unset, or written back to None by a job opting out of an
            # inherited `defaults:` value.  The empty string an interpolated
            # ${VAR} can produce means the same thing, and must NOT reach
            # abspath, which would silently pin the daemon's load-time CWD.
            self.workingDirectory = None
            return
        self.workingDirectory = os.path.abspath(os.path.expanduser(configured))

    def _resolve_user_group(self, config: dict) -> None:
        user = config.pop("user", None)
        group = config.pop("group", None)
        # Retain the *configured* user/group (string name or numeric id, or
        # None) for the job-set fingerprint.  The resolved uid/gid below are
        # host-specific (the same name can map to different ids on different
        # hosts), so fingerprinting must use the configured value, not them.
        self.user: Optional[str | int] = user
        self.group: Optional[str | int] = group
        if user is None and group is None:
            return  # nothing to switch to: nothing POSIX-only to resolve

        # Windows has no setuid/setgid model that maps onto this feature, so
        # reject it with a clear error instead of silently running the job as
        # the wrong account.  Spelled as ``sys.platform == "win32"`` (rather
        # than platform.IS_WINDOWS) so the type checker statically prunes the
        # POSIX-only imports/calls below on Windows.
        if sys.platform == "win32":  # pragma: no cover (windows)
            raise ConfigError(
                "Job {}: changing user/group is not supported on "
                "Windows".format(self.name)
            )
        else:  # pragma: no cover (posix) - grp/pwd are POSIX-only
            # The ``else`` is what the pruning above needs: the reject comes
            # first because that is the rule being stated, and the POSIX
            # body has to be a clause of the same statement for mypy to
            # discard it under ``--platform win32`` and for the Windows
            # coverage profile to leave it out of the denominator.
            #
            # POSIX only: the passwd/group databases live in modules that
            # don't exist on Windows; imported lazily (reached only here).
            from grp import getgrnam
            from pwd import getpwnam, getpwuid

            if user is not None:
                if isinstance(user, int):
                    self.uid = user
                    # Derive the primary gid (and login name) from the
                    # passwd database so a numeric ``user`` without an
                    # explicit ``group`` does not silently keep
                    # cronstable's (root) gid 0.
                    try:
                        pw = getpwuid(user)
                    except KeyError:
                        pw = None
                    if pw is not None:
                        self.username = pw.pw_name
                        if self.gid is None:
                            self.gid = pw.pw_gid
                else:
                    try:
                        pw = getpwnam(user)
                        self.uid = pw.pw_uid
                        self.gid = pw.pw_gid
                        self.username = pw.pw_name
                    except KeyError as e:
                        raise ConfigError(
                            "User not found: {!r}".format(user)
                        ) from e

            if group is not None:
                if isinstance(group, int):
                    self.gid = group
                else:
                    try:
                        self.gid = getgrnam(group).gr_gid
                    except KeyError as e:
                        raise ConfigError(
                            "Group not found: {!r}".format(group)
                        ) from e

            if self.uid is not None or self.gid is not None:
                if os.geteuid() != 0:
                    raise ConfigError(
                        "Job {} wants to change user or group, "
                        "but cronstable is not running as superuser".format(
                            self.name
                        )
                    )

    def _warn_if_priority_needs_privilege(self) -> None:
        """Say at load time that this job's priority may be refused.

        Advisory, and the opposite call from the ``user``/``group`` check
        above: a job that cannot get the priority it asked for still does
        its work, so refusing the load over it would turn a preference into
        an outage.  It is said here, once per load, because the alternative
        is a per-run line describing a condition that will not change until
        the deployment does; the refusal itself is logged at DEBUG when it
        happens (:func:`cronstable.platform.apply_priority`).

        Only "may": an unprivileged process can still lower its nice value
        as far as its ``RLIMIT_NICE`` allows, so this is an advisory rather
        than a prediction.  The comparison is against cronstable's own nice
        rather than against zero because only a *raise* needs privilege, and
        a daemon deliberately started at nice 10 can hand a job
        ``below-normal`` without asking anyone.
        """
        wanted = platform.posix_nice_for(self.priority)
        if wanted is None:
            return  # the default level: nothing is ever applied for it
        if sys.platform == "win32":  # pragma: no cover (windows)
            # Windows hands every class this vocabulary offers to an
            # unprivileged account; the one that does need a privilege
            # (realtime) is deliberately not reachable, so there is nothing
            # to advise about.  Spelled as ``sys.platform``, as
            # _resolve_user_group above is, so the type checker prunes the
            # POSIX-only calls below.
            return
        else:  # pragma: no cover (posix) - os.nice/os.geteuid
            current = os.nice(0)
            if wanted >= current or os.geteuid() == 0:
                return
            logger.warning(
                "job %r asks for priority %r (nice %d), a raise from "
                "cronstable's own nice %d, which needs CAP_SYS_NICE or "
                "RLIMIT_NICE headroom; where the kernel refuses it the job "
                "still runs, at the priority it inherited",
                self.name,
                self.priority,
                wanted,
                current,
            )

    def _reject(self, message: str) -> "ConfigError":
        """Build (never raise) the job-scoped ConfigError for a failed check.

        Returning it keeps every ``raise self._reject(...)`` below a single
        statement, so the message is still built only when the check fails.
        """
        return ConfigError("Job {}: {}".format(self.name, message))

    def _validate_numeric_ranges(self) -> None:
        # strictyaml only enforces the type; fail fast on values that would
        # otherwise produce obscure runtime behavior.  Plain `if ... raise`
        # on purpose: this runs once per job on every load and reload.
        # The Float-typed checks stay in the negated `if not value >= bound`
        # form on purpose: NaN fails every comparison, so the tempting
        # inversion (`if value < bound`) waves NaN through (strictyaml's
        # Float() parses `.nan`) where this form rejects it.
        if self.saveLimit < 0:
            raise self._reject("saveLimit must be >= 0")
        if self.maxLineLength <= 0:
            raise self._reject("maxLineLength must be > 0")
        if not self.killTimeout >= 0:
            raise self._reject("killTimeout must be >= 0")
        # sampling walks the whole process table each tick, so a sub-100ms
        # cadence is a busy-loop footgun; the history cap bounds what one run
        # can add to a durable ledger record (0 = summary only, no series).
        if not self.monitorResourcesInterval >= 0.1:
            raise self._reject(
                "monitorResources.interval must be >= 0.1 (seconds)"
            )
        if not 0 <= self.monitorResourcesHistory <= 2000:
            raise self._reject(
                "monitorResources.history must be between 0 and 2000 (points)"
            )
        # Allow places no bound on concurrent instances, so cluster scope
        # would gate nothing; an option that silently does nothing is worse
        # than a load-time error.
        if (
            self.concurrencyScope == "cluster"
            and self.concurrencyPolicy == "Allow"
        ):
            raise self._reject(
                "concurrencyScope: cluster has no effect with "
                "concurrencyPolicy: Allow (the default); set Forbid or "
                "Replace, or drop concurrencyScope"
            )
        if self.catchupJitterSeconds < 0:
            raise self._reject("catchupJitterSeconds must be >= 0")
        if (
            self.startingDeadlineSeconds is not None
            and self.startingDeadlineSeconds <= 0
        ):
            raise self._reject("startingDeadlineSeconds must be > 0 when set")
        if self.executionTimeout is not None and not self.executionTimeout > 0:
            raise self._reject("executionTimeout must be > 0 when set")
        for key in (
            "maxTimeSinceSuccessSeconds",
            "lateAfterSeconds",
            "maxRuntimeSeconds",
        ):
            value = self.sla.get(key)
            if value is not None and value <= 0:
                raise self._reject("sla.{} must be > 0 when set".format(key))
        # The identity test comes first because it is the common case and it
        # settles the question outright: a job that never wrote an `onLate`
        # block still points at the (inert) default one, checked at import.
        if self.onLate is not _DEFAULT_ONLATE and all(
            v is None for v in self.sla.values()
        ):
            if _onlate_names_a_destination(self.onLate):
                raise self._reject("onLate requires sla")
        retry = self.onFailure.get("retry")
        if retry is not None:
            # -1 is the documented sentinel for "retry forever".
            if retry["maximumRetries"] < -1:
                raise self._reject(
                    "onFailure.retry.maximumRetries must be >= -1"
                )
            if not retry["initialDelay"] >= 0:
                raise self._reject("onFailure.retry.initialDelay must be >= 0")
            if not retry["maximumDelay"] > 0:
                raise self._reject("onFailure.retry.maximumDelay must be > 0")
            if not retry["backoffMultiplier"] > 0:
                raise self._reject(
                    "onFailure.retry.backoffMultiplier must be > 0"
                )


# Defaults for the DAG-node fields strictyaml leaves absent (unlike jobs, a
# task dict is not pre-merged over a full defaults dict, so absent optionals
# stay absent).  The launch fields are filled from the assembled job-defaults
# base (DEFAULT_CONFIG plus any `defaults:` block) when the per-task JobConfig
# template is built (see DagTaskConfig).
_DAG_TASK_DEFAULTS: dict[str, Any] = {
    "type": "task",
    "dependsOn": [],
    "triggerRule": "all_success",
    "retries": 0,
    "retryDelaySeconds": 0.0,
    "pokeIntervalSeconds": 30.0,
    "pokeTimeoutSeconds": 3600.0,
    "pokeJitterSeconds": 0.0,
    "onReject": "fail",
}

# the DAG-node keys consumed here; everything else in a task dict is a launch
# field forwarded to the per-task JobConfig template.
_DAG_TASK_NODE_KEYS = frozenset(
    {
        "id",
        "type",
        "dependsOn",
        "triggerRule",
        "retries",
        "retryDelaySeconds",
        "expand",
        "pokeIntervalSeconds",
        "pokeTimeoutSeconds",
        "pokeJitterSeconds",
        "onReject",
    }
)


class DagTaskConfig:
    """One DAG node: its state-machine :class:`cronstable.dag.TaskSpec` plus
    the :class:`JobConfig` launch template the scheduler runs it from.

    A task *is* a job invocation: the launch fields reuse the exact job
    machinery (the same :class:`~cronstable.job.RunningJob` path a scheduled
    job uses); the DAG-node fields drive the pure state machine.
    """

    __slots__ = ("id", "type", "job_template", "spec")

    def __init__(
        self,
        dag_name: str,
        raw_task: dict,
        defaults: Optional[dict[str, Any]] = None,
    ) -> None:
        # Imported at the point of use: only a config with a dags: section
        # needs the DAG state machine, and this module is on the
        # --validate-config and --job-set-id import paths (the daemon
        # imports dag anyway, through cron.py).
        from cronstable import dag

        # `defaults` is the assembled job-defaults base, the same base a
        # regular job is merged over (falls back to DEFAULT_CONFIG when a
        # DagConfig is built directly, e.g. in a test).  The DAG-node fields
        # are popped out below before this merge, so a `defaults:` block
        # never perturbs graph shape.
        base = defaults if defaults is not None else DEFAULT_CONFIG
        merged = mergedicts(_DAG_TASK_DEFAULTS, raw_task)
        self.id: str = merged["id"]
        self.type: str = merged["type"]
        node = {
            k: merged.pop(k) for k in list(merged) if k in _DAG_TASK_NODE_KEYS
        }
        expand = node.get("expand")
        command = merged.get("command")
        if self.type == "approval":
            # an approval gate runs no subprocess; a harmless placeholder keeps
            # the JobConfig template valid without demanding a command.
            merged["command"] = command or "true"
        elif not command:
            raise ConfigError(
                "dag {!r}: task {!r} needs a command".format(dag_name, self.id)
            )
        retries = int(node["retries"])
        if retries < 0:
            # the job-level onFailure.retry.maximumRetries documents -1 as
            # the "retry forever" sentinel; a dag task has no such sentinel,
            # and a negative value here would silently mean ZERO retries
            # (max_attempts = retries + 1), the opposite of that intent.
            raise ConfigError(
                "dag {!r}: task {!r}: retries must be >= 0 (the job-level "
                "-1 retry-forever sentinel is not supported for dag "
                "tasks)".format(dag_name, self.id)
            )
        job_dict = mergedicts(base, merged)
        job_dict["name"] = "{}.{}".format(dag_name, self.id)
        # never auto-fires: task templates are not in the scheduler's job set,
        # so this placeholder schedule is only there to satisfy JobConfig.
        job_dict["schedule"] = "@reboot"
        try:
            self.job_template = JobConfig(job_dict)
        except ConfigError as ex:
            raise ConfigError(
                "dag {!r}: task {!r}: {}".format(dag_name, self.id, ex)
            ) from ex
        self.spec = dag.TaskSpec(
            id=self.id,
            type=self.type,
            depends_on=tuple(node["dependsOn"]),
            trigger_rule=node["triggerRule"],
            max_attempts=retries + 1,
            retry_delay=float(node["retryDelaySeconds"]),
            expand=(
                dag.ExpandSpec(from_task=expand["fromTask"], key=expand["key"])
                if expand
                else None
            ),
            poke_interval=float(node["pokeIntervalSeconds"]),
            poke_timeout=float(node["pokeTimeoutSeconds"]),
            poke_jitter=float(node["pokeJitterSeconds"]),
            on_reject=dag.SKIPPED
            if node["onReject"] == "skip"
            else dag.FAILED,
        )


class DagConfig:
    """A whole DAG: its scheduling frame, its tasks, and the validated graph.

    ``schedule_job`` is a synthetic :class:`JobConfig` carrying only the DAG's
    schedule/timezone/onMissed frame, so the scheduler reuses
    ``_compute_next_fire`` / the catch-up discipline verbatim; it is ``None``
    for a manual-only DAG (triggered by API or backfill).  The graph is
    validated at construction, so a cycle or dangling dependency is a
    :class:`ConfigError` at load.
    """

    __slots__ = (
        "name",
        "enabled",
        "retain_runs",
        "schedule_job",
        "tasks",
        "spec",
        "task_templates",
    )

    def __init__(
        self, raw_dag: dict, defaults: Optional[dict[str, Any]] = None
    ) -> None:
        # deferred for the reason DagTaskConfig gives
        from cronstable import dag

        raw = dict(raw_dag)
        self.name: str = raw.pop("name")
        self.enabled: bool = bool(raw.pop("enabled", True))
        self.retain_runs: int = int(raw.pop("retainRuns", 50))
        if self.retain_runs < 1:
            raise ConfigError(
                "dag {!r}: retainRuns must be >= 1".format(self.name)
            )
        tasks_raw = raw.pop("tasks")
        if not tasks_raw:
            raise ConfigError(
                "dag {!r}: needs at least one task".format(self.name)
            )
        self.tasks = [DagTaskConfig(self.name, t, defaults) for t in tasks_raw]
        self.task_templates: dict[str, JobConfig] = {
            t.id: t.job_template for t in self.tasks
        }
        self.spec = dag.DagSpec.build(self.name, [t.spec for t in self.tasks])
        try:
            dag.validate_graph(self.spec)
        except dag.DagValidationError as ex:
            raise ConfigError("dag {!r}: {}".format(self.name, ex)) from ex
        schedule = raw.pop("schedule", None)
        self.schedule_job: Optional[JobConfig] = (
            self._build_schedule_job(raw, schedule)
            if schedule is not None
            else None
        )

    def _build_schedule_job(self, raw: dict, schedule: Any) -> JobConfig:
        overrides: dict[str, Any] = {
            "name": "dag:" + self.name,
            "command": "true",
            "schedule": schedule,
            "enabled": self.enabled,
        }
        for key in (
            "onMissed",
            "startingDeadlineSeconds",
            "catchupJitterSeconds",
            "timezone",
            "utc",
            "clusterPolicy",
        ):
            if key in raw:
                overrides[key] = raw[key]
        # The synthetic schedule-trigger job deliberately builds on
        # DEFAULT_CONFIG, NOT the user `defaults:` base the tasks use: it runs
        # a placeholder `true` on every DAG tick, so inheriting a global
        # onSuccess/onFailure reporter here would fire an alert per tick rather
        # than per DAG run.  DAG-run reporting is a separate concern.
        job_dict = mergedicts(DEFAULT_CONFIG, overrides)
        try:
            job = JobConfig(job_dict)
        except ConfigError as ex:
            raise ConfigError("dag {!r}: {}".format(self.name, ex)) from ex
        # Every DAG scheduling path computes next-fire instants from a
        # CronTab; a schedule left as a plain string ("@reboot") would crash
        # the scheduler at runtime instead of failing the load here.
        # Structural on purpose: whatever parses into a CronTab is fine,
        # anything that stays a string is not.
        if not isinstance(job.schedule, CronTab):
            raise ConfigError(
                "dag {!r}: schedule {!r} is not a cron expression; DAG "
                "schedules must be cron expressions (@reboot is not "
                "supported for dags)".format(self.name, schedule)
            )
        return job


def parse_environment_file(path: str) -> dict[str, str]:
    """Parse a ``VARIABLE_NAME=CONTENT`` environment file to a dict.

    Skips comments and blank lines.  Raises ConfigError for an unparsable
    line, non-UTF-8 content or an unusable path; OSError if the file cannot
    be opened.
    """
    environ: dict[str, str] = {}

    # open()/readlines() raise ValueError for a NUL in the path and
    # UnicodeDecodeError (a ValueError, NOT an OSError) for non-UTF-8 bytes;
    # config parsing must only ever raise ConfigError.
    try:
        # utf-8-sig: byte-identical to utf-8 for BOM-less files, and strips
        # the BOM that Windows editors (Notepad historically, PowerShell
        # `>` redirects) prepend, which otherwise rides invisibly into the
        # first variable's NAME: the job then has a variable whose name
        # starts with U+FEFF set, and the expected one absent, with no
        # error anywhere.
        with open(path, "r", encoding="utf-8-sig") as env_file:
            lines = env_file.readlines()
    except ValueError as err:
        raise ConfigError(
            "Could not load env_file {!r}: {}".format(path, err)
        ) from err

    for line in lines:
        line = line.strip(" ").rstrip("\n")
        if line.startswith("#") or not line:
            continue
        if "=" not in line:
            raise ConfigError("Invalid line in env_file: '{}'".format(line))
        key, value = line.split("=", 1)
        key = key.strip(" ")
        value = value.strip(" ")
        environ[key] = value

    return environ


# Hosts that mean "all interfaces" in a `listen` address. A peer entry can't be
# string-matched against these, so a node self-listed by hostname behind a
# wildcard listen needs the nodeName-based recognition in _is_self_listed.
_WILDCARD_LISTEN_HOSTS = frozenset({"0.0.0.0", "::", "[::]", "*", ""})

# The same, split by address family: a wildcard bind holds the port on every
# interface OF ITS FAMILY, which is what makes a same-family literal loopback
# peer entry unambiguously self (see _is_self_listed). "*" and "" bind
# everything, so they belong to both.
_V4_WILDCARD_LISTEN_HOSTS = frozenset({"0.0.0.0", "*", ""})
_V6_WILDCARD_LISTEN_HOSTS = frozenset({"::", "[::]", "*", ""})


def _loopback_ip_version(host: str) -> Optional[int]:
    """The IP version (4 or 6) of a literal loopback host, else ``None``.

    Accepts the bracketed IPv6 form peer entries use (``[::1]``).  Pure
    literal parsing via :mod:`ipaddress`: a hostname -- even ``localhost`` --
    never parses, so no DNS resolution happens and nothing is guessed.
    """
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    return ip.version if ip.is_loopback else None


def _is_self_listed(peer_host: str, listen: str, node_name: str) -> bool:
    """Whether a configured peer entry *unambiguously* points back at us.

    A self entry must be dropped from `peers`: it never counts toward
    agreement, yet it inflates `cluster_size()` (and so the quorum threshold,
    the size-divergence gate, and the 2-node refusal) by one.  An entry is
    dropped only when it can be *only* this node, never another member:

    * an exact match of our own `listen` address; or
    * a wildcard `listen` self-listed by a host equal to our `nodeName`
      *exactly*, on the same port; or
    * a *literal loopback* entry on the same port, under a wildcard `listen`
      of the matching family (loopback never leaves this host, and the
      wildcard bind holds the port on every interface of that family).
      `localhost` is deliberately NOT matched: resolving it would be the DNS
      guessing this function refuses; it gets an advisory instead
      (:func:`_likely_self_loopback`).

    The match is deliberately *exact*: never drop a peer on a fuzzy match of
    the host FQDN's *first label* against a bare `nodeName`.  Such an
    over-match can silently drop a genuinely distinct member that merely
    shares a first label, shrinking *our* `N` below everyone else's, which
    either pins `Leader` jobs closed cluster-wide on a permanent
    size-divergence conflict or opens a split-brain; no runtime backstop
    re-adds a config-dropped peer.  A genuine self-listing the exact match
    misses (e.g. FQDN vs short `nodeName`) falls back to the runtime
    `STATUS_SELF` recognition in `ClusterManager.cluster_size`; the brief
    `N` inflation before that first poll errs in the safe direction (a
    higher quorum, never a split-brain).  No DNS resolution is done at
    config time.
    """
    if peer_host == listen:
        return True
    listen_host, _, listen_port = listen.rpartition(":")
    if listen_host not in _WILDCARD_LISTEN_HOSTS:
        return False
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        return False
    if peer_h == node_name:
        return True
    # A literal loopback of the same family as the wildcard bind, on our own
    # port: unambiguously this node (see the docstring). The family must
    # match -- a "::"-only bind does not necessarily accept v4, so 127.0.0.1
    # there could in principle be a different colocated process.
    version = _loopback_ip_version(peer_h)
    if version == 4 and listen_host in _V4_WILDCARD_LISTEN_HOSTS:
        return True
    return version == 6 and listen_host in _V6_WILDCARD_LISTEN_HOSTS


def _likely_self_fqdn(peer_host: str, listen: str, node_name: str) -> bool:
    """Whether a peer entry *looks like* this node listed by its FQDN.

    A heuristic for diagnostics only (never used to drop a peer -- that fuzzy
    match is exactly the dangerous over-match :func:`_is_self_listed` refuses).
    True when, under a wildcard ``listen``, a peer on our port has a host
    whose first DNS label equals our ``nodeName`` but is not an exact match
    (which :func:`_is_self_listed` would already have dropped). Used to warn
    that a cluster declared as 3 nodes may really be a degenerate 2-node one.
    """
    if _is_self_listed(peer_host, listen, node_name):
        return False
    listen_host, _, listen_port = listen.rpartition(":")
    if listen_host not in _WILDCARD_LISTEN_HOSTS:
        return False
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        return False
    return peer_h.split(".", 1)[0] == node_name and peer_h != node_name


def _likely_self_loopback(peer_host: str, listen: str) -> bool:
    """Whether a peer entry *looks like* this node listed via loopback.

    A diagnostics-only heuristic like :func:`_likely_self_fqdn` (never used
    to drop a peer).  True for a loopback-ish entry on our listen port that
    :func:`_is_self_listed` could not drop as unambiguous (``localhost``, or
    a loopback literal under a non-wildcard or other-family ``listen``).
    Such an entry can never be another cluster member; used to warn when the
    remainder would leave the real cluster at <= 2 nodes.
    """
    _, _, listen_port = listen.rpartition(":")
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        # a colocated second daemon on another port of this host is a
        # legitimate (if unusual) distinct member; only our own port is
        # suspect.
        return False
    return peer_h == "localhost" or _loopback_ip_version(peer_h) is not None


def _cluster_base(raw: dict) -> "dict[str, Any]":
    """Fill the shared cluster defaults over a raw (schema-validated) block.

    Covers the keys every backend uses (backend, nodeName, connectTimeout,
    electLeader, distribution, and the inert gossip cadence fields). Each
    backend's builder then layers on its own block.
    """
    cfg: dict[str, Any] = dict(DEFAULT_CLUSTER)
    cfg.update(raw)
    if not cfg.get("nodeName"):
        # a stable, human-readable identity for this node, used as the lease
        # identity and so a gossip peer can recognise itself in someone else's
        # peer list; the system hostname is a sensible default.
        cfg["nodeName"] = socket.gethostname()
    if cfg["connectTimeout"] <= 0:
        raise ConfigError("cluster.connectTimeout must be > 0")
    return cfg


def _build_cluster_config(raw: dict) -> ClusterConfig:
    """Build a ClusterConfig, dispatching on the chosen ``backend``.

    An optional ``observability`` block is resolved on top of whichever backend
    was chosen and attached to the returned config as two derived keys the
    scheduler reads (the backends themselves ignore them):

    * ``shareNodeStats`` -- gossip this node's CPU/memory for the fleet view.
    * ``observabilityMesh`` -- a resolved, election-inert gossip ClusterConfig
      to stand up as a *second* manager (lease backends only); ``None`` under
      ``backend: gossip``, where the election mesh already carries the data.
    """
    backend = raw.get("backend", DEFAULT_CLUSTER["backend"])
    if backend == "kubernetes":
        cfg = _build_kubernetes_cluster_config(raw)
    elif backend == "etcd":
        cfg = _build_etcd_cluster_config(raw)
    elif backend == "filesystem":
        cfg = _build_filesystem_cluster_config(raw)
    else:
        cfg = _build_gossip_cluster_config(raw)
    _attach_observability(cfg, raw, backend)
    return cfg


def _attach_observability(
    cfg: "dict[str, Any]", raw: dict, backend: str
) -> None:
    """Resolve a ``cluster.observability`` block onto a built cluster config.

    Sets ``cfg["shareNodeStats"]`` and ``cfg["observabilityMesh"]`` (see
    :func:`_build_cluster_config`). No-op when the block is absent, so a
    config without it gossips the same bytes as one that never had it.
    """
    cfg["shareNodeStats"] = False
    cfg["observabilityMesh"] = None
    obs = raw.get("observability")
    if obs is None:
        return
    # explicit opt-out is allowed (configure the overlay mesh for fleet job
    # summaries but not CPU/memory); defaults on -- sharing load is the point.
    cfg["shareNodeStats"] = obs.get("shareNodeStats", True)
    transport_keys = ("listen", "tls", "peers")
    has_transport = any(obs.get(k) is not None for k in transport_keys)
    if backend == "gossip":
        # the election gossip mesh already exchanges /peer bodies, so the
        # overlay would be a redundant second mesh on the same nodes: reject
        # its transport and simply ride the existing mesh.
        if has_transport:
            raise ConfigError(
                "cluster.observability.{listen,tls,peers} is redundant with "
                "backend: gossip (the election mesh already carries fleet "
                "data); drop them -- an empty `observability:` block, or just "
                "`shareNodeStats`, is enough to share node CPU/memory"
            )
        # the overlay tuning keys only configure the separate mesh a lease
        # backend stands up; reject them under gossip rather than silently
        # ignoring a value the operator believes is in effect.
        for key in ("nodeName", "interval", "driftAfter", "connectTimeout"):
            if obs.get(key) is not None:
                raise ConfigError(
                    "cluster.observability.{} only applies to the overlay "
                    "mesh a lease backend (kubernetes/etcd/filesystem) "
                    "stands up; with backend: gossip node stats ride the "
                    "election mesh, so set cluster.{} instead".format(key, key)
                )
        return
    # a lease backend has no node-to-node channel, so the overlay must stand up
    # its own gossip mesh -- which needs the full gossip transport, and runs
    # election-inert (electLeader forced false: it never gates jobs).
    for key in transport_keys:
        if obs.get(key) is None:
            raise ConfigError(
                "cluster.observability requires cluster.observability.{} "
                "when backend is {!r} (the overlay stands up its own gossip "
                "mesh to carry fleet data)".format(key, backend)
            )
    mesh_raw = {
        "backend": "gossip",
        "electLeader": False,
        "distribution": "single-leader",
        "listen": obs["listen"],
        "tls": obs["tls"],
        "peers": obs["peers"],
    }
    for key in ("nodeName", "interval", "driftAfter", "connectTimeout"):
        if obs.get(key) is not None:
            mesh_raw[key] = obs[key]
    cfg["observabilityMesh"] = _build_gossip_cluster_config(
        mesh_raw, where="cluster.observability"
    )


def _build_state_config(raw: dict) -> StateConfig:
    """Fill the state defaults over a raw (schema-validated) block, validate.

    ``path`` is the one required key (the schema enforces its presence and
    string type; this guards against an empty/whitespace value that would
    otherwise resolve to a surprising directory).  ``topology`` is already
    constrained to the enum by the schema, and ``deploymentId`` is free-form.
    """
    cfg: dict[str, Any] = dict(DEFAULT_STATE)
    cfg.update(raw)
    if not cfg.get("path") or not str(cfg["path"]).strip():
        raise ConfigError("state.path is required and must be non-empty")
    # The float checks below are written NaN-rejecting on purpose: strictyaml's
    # Float() accepts 'nan' and overflow literals like '1e309' (== inf), and a
    # plain 'x < floor' comparison is False for NaN, so a non-finite value
    # would sail through into the lease/TTL arithmetic it silently breaks
    # (expires_at = now + nan is never "validly held"; + inf never expires).
    ops = float(cfg.get("maxOpsPerSecond") or 0)
    if not math.isfinite(ops) or ops < 0:
        raise ConfigError("state.maxOpsPerSecond must be >= 0 and finite")
    grace = int(cfg.get("gcGraceSeconds") or 0)
    if 0 < grace < 86400:
        # a grace below the manifest cadence would make every live peer's
        # manifests look stale and hand their state to the collector; a day
        # is the floor at which the anchoring stays sound.
        raise ConfigError(
            "state.gcGraceSeconds must be <= 0 (GC disabled) or >= 86400"
        )
    slot_ttl = float(cfg.get("slotTtlSeconds") or 0)
    if not math.isfinite(slot_ttl) or slot_ttl < 5:
        # the slot lease is renewed at ttl/3 by a live holder; below ~5s
        # one slow renew on a network mount expires a healthy holder's
        # slot and invites the cross-node double-run the lease fences.
        raise ConfigError("state.slotTtlSeconds must be >= 5 and finite")
    # jobApi is a nested block: merge its raw keys over the defaults explicitly
    # (cfg.update above is a shallow merge that would drop the untouched
    # DEFAULT_JOB_API keys of a partially-specified `jobApi:` block).
    job_api = dict(DEFAULT_JOB_API)
    job_api.update(cfg.get("jobApi") or {})
    # jobApi.tls is a third nesting level: same shallow-merge reason
    # (mirrors _build_etcd_cluster_config).
    job_api["tls"] = {
        **DEFAULT_JOB_API["tls"],
        **(job_api.get("tls") or {}),
    }
    cfg["jobApi"] = job_api
    lock_ttl = float(job_api.get("lockTtlSeconds") or 0)
    if not math.isfinite(lock_ttl) or lock_ttl < 5:
        raise ConfigError(
            "state.jobApi.lockTtlSeconds must be >= 5 and finite"
        )
    if int(job_api.get("maxValueBytes") or 0) < 0:
        raise ConfigError("state.jobApi.maxValueBytes must be >= 0")
    if int(job_api.get("maxArtifactBytes") or 0) < 0:
        raise ConfigError("state.jobApi.maxArtifactBytes must be >= 0")
    listen = job_api.get("listen")
    if (
        listen is not None
        and "://" in str(listen)
        and not str(listen).startswith(("http://", "https://"))
    ):
        raise ConfigError(
            "state.jobApi.listen must be an http:// or https:// URL or a "
            "bare host:port (the job CLI reaches the endpoint over TCP only)"
        )
    if listen:
        # validate the port the way the runtime bind parses it: an unchecked
        # ValueError would escape API startup and permanently disable the
        # loopback endpoint instead of failing the config load.  An explicit
        # :0 is fine (OS-assigned ephemeral, same as a missing port).
        text = str(listen)
        parsed = _safe_urlparse(
            text if "://" in text else "http://" + text,
            "state.jobApi.listen",
        )
        try:
            port = parsed.port
        except ValueError as err:
            raise ConfigError(
                "state.jobApi.listen has an invalid port in {!r}: the port "
                "must be an integer in 0-65535 (0 or omitted binds an "
                "OS-assigned ephemeral port)".format(text)
            ) from err
        if port is not None and not 0 <= port <= 65535:
            raise ConfigError(
                "state.jobApi.listen has an invalid port in {!r}: the port "
                "must be an integer in 0-65535 (0 or omitted binds an "
                "OS-assigned ephemeral port)".format(text)
            )
    _validate_job_api_tls(job_api, listen)
    if listen and not job_api.get("allowNonLoopbackBind"):
        text = str(listen)
        parsed = _safe_urlparse(
            text if "://" in text else "http://" + text,
            "state.jobApi.listen",
        )
        host = parsed.hostname or ""
        if host != "localhost" and _loopback_ip_version(host) is None:
            msg = (
                "state.jobApi.listen host {!r} is not loopback; this "
                "endpoint serves per-run bearer tokens and staged job "
                "secrets, so binding it beyond this host needs "
                "state.jobApi.allowNonLoopbackBind: true"
            )
            raise ConfigError(msg.format(host))
    return StateConfig(cfg)


def _is_wildcard_host(host: str) -> bool:
    """True for a bind address meaning "every interface".

    Compares parsed ``ipaddress`` values rather than a string list, so every
    spelling (``0.0.0.0``, ``::``, ``[::]``, ``::0``, empty) matches.
    Shared with :mod:`cronstable.jobapi` (a wildcard is not an address a job
    can dial); here it also gates a wildcard bind over ``https://`` (no
    certificate SAN can cover an unspecified address).
    """
    text = host.strip().strip("[]")
    if not text:
        return True
    try:
        return ipaddress.ip_address(text).is_unspecified
    except ValueError:
        # a name, not a literal: a job can resolve it, and a certificate SAN
        # can cover it
        return False


def _validate_job_api_tls(job_api: dict[str, Any], listen: Any) -> None:
    """Cross-checks between ``state.jobApi.listen``'s scheme and its `tls`.

    Same shape as the etcd client-TLS checks: material with no transport, or
    a transport with no material, is a silent-downgrade misconfiguration.
    The stakes: this endpoint hands every job a per-run bearer token and
    stages its secrets, so an inert `tls` block puts exactly those bytes on
    the wire in the clear.
    """
    tls = job_api.get("tls") or {}
    cert, key, ca = tls.get("cert"), tls.get("key"), tls.get("ca")
    text = str(listen) if listen else ""
    is_https = text.startswith("https://")
    if bool(cert) != bool(key):
        raise ConfigError(
            "state.jobApi.tls.cert and state.jobApi.tls.key must be set "
            "together (a server certificate needs its private key); got "
            "cert={!r}, key={!r}".format(bool(cert), bool(key))
        )
    if (cert or ca) and not is_https:
        # `ca` too: it is injected into every job as CRONSTABLE_STATE_CACERT,
        # so left set against a plaintext endpoint it is both inert and
        # actively misleading.
        raise ConfigError(
            "state.jobApi.tls is configured but state.jobApi.listen is not "
            "an https:// URL, so the TLS material would be ignored and "
            "per-run bearer tokens sent in cleartext; use an https:// listen "
            "address or remove state.jobApi.tls"
        )
    if is_https and not cert:
        raise ConfigError(
            "state.jobApi.listen is https:// but state.jobApi.tls.cert and "
            "state.jobApi.tls.key are not set, so the endpoint has no "
            "certificate to serve"
        )
    if is_https and _is_wildcard_host(
        (_safe_urlparse(text, "state.jobApi.listen").hostname or "")
    ):
        # A wildcard bind is advertised to jobs verbatim, and no certificate
        # can carry a SAN for "every interface", so every job would fail
        # hostname verification against the URL it was handed.
        raise ConfigError(
            "state.jobApi.listen cannot bind a wildcard host over "
            "https:// : jobs dial the address they are given, and no "
            "certificate covers an unspecified address; name the interface "
            "explicitly, e.g. https://10.0.0.5:9000"
        )
    if listen and job_api.get("allowNonLoopbackBind") and not is_https:
        # Warn, not fail: a TLS-terminating reverse proxy in front is still a
        # valid answer, but the combination is worth naming every boot.
        parsed = _safe_urlparse(
            text if "://" in text else "http://" + text,
            "state.jobApi.listen",
        )
        host = parsed.hostname or ""
        if host != "localhost" and _loopback_ip_version(host) is None:
            logger.warning(
                "state.jobApi.listen binds %r off-host without TLS, so "
                "per-run bearer tokens and staged job secrets cross the "
                "network in cleartext; set an https:// listen address with "
                "state.jobApi.tls, or terminate TLS in front of it",
                host,
            )


def _require_gossip_tls_paths(tls: Mapping[str, Any], where: str) -> None:
    """Every path in a gossip ``tls`` block must be a non-blank string.

    The schema requires the keys only, and strictyaml maps a blank scalar
    (what a template renders for an unset variable) to ``''``. An empty
    ``ca`` reaches the context builders as "no client CA": the ``/peer``
    listener would require no client certificate and the poller would trust
    the system root store.
    """
    for key in ("ca", "cert", "key"):
        if not str(tls.get(key) or "").strip():
            raise ConfigError(
                "{}.{} must be a non-empty path; a blank value would disable "
                "peer authentication".format(where, key)
            )


def _build_gossip_cluster_config(
    raw: dict, *, where: str = "cluster"
) -> ClusterConfig:
    # Fill defaults over the raw (schema-validated) cluster block and validate
    # the numeric fields, mirroring _validate_numeric_ranges for jobs.
    # ``where`` names the block in errors; the observability overlay builds
    # its mesh through here too.
    cfg = _cluster_base(raw)
    _reject_foreign_store_blocks(cfg, "gossip")
    # listen/tls/peers are schema-optional now (so a lease backend need not
    # carry them), but the gossip transport requires all three.
    for key in ("listen", "tls", "peers"):
        if cfg.get(key) is None:
            raise ConfigError(
                "{0}.backend gossip requires {0}.{1}".format(where, key)
            )
    _require_gossip_tls_paths(cfg["tls"], where + ".tls")
    if cfg["interval"] <= 0:
        raise ConfigError("{}.interval must be > 0".format(where))
    if cfg["driftAfter"] < 1:
        raise ConfigError("{}.driftAfter must be >= 1".format(where))

    # Validate every address is a well-formed host:port up front, so a typo
    # (a missing port, a non-numeric port) fails the config load pointing at
    # the offending value instead of surfacing later as an opaque per-peer
    # connection error. Mirrors cronstable.cluster._split_host_port, plus a
    # port range check; anything this accepts also parses at runtime.
    def _is_port(port: str) -> bool:
        # NOT a bare port.isdigit(): non-ASCII digits pass isdigit() and then
        # raise a bare ValueError inside int(), and an over-long run trips
        # CPython's int-conversion limit. Both must be a clean ConfigError,
        # so bound the text before converting it.
        if not (port.isascii() and port.isdigit() and len(port) <= 5):
            return False
        return 0 < int(port) <= 65535

    def _require_host_port(addr: str, what: str) -> None:
        # Bracketed IPv6 (``[2001:db8::1]:8900``): host is inside the brackets.
        if addr.startswith("["):
            bracket, sep, port = addr.rpartition("]:")
            host = bracket[1:]
            if not sep or not host or not _is_port(port):
                raise ConfigError(
                    "{}.{} must be [ipv6]:port, got {!r}".format(
                        where, what, addr
                    )
                )
            return
        host, _, port = addr.rpartition(":")
        # A bare (unbracketed) IPv6 literal has more colons left in ``host``;
        # rpartition would silently mis-split it (``2001:db8::1`` ->
        # host=``2001:db8:``, port=``1``), passing validation and then failing
        # opaquely at connect/bind time -- and for a peer, silently dropping it
        # from quorum with no error. Require the bracketed form instead.
        if ":" in host:
            raise ConfigError(
                "{}.{} looks like a bare IPv6 address; write it as "
                "[ipv6]:port, got {!r}".format(where, what, addr)
            )
        if not host or not _is_port(port):
            raise ConfigError(
                "{}.{} must be host:port, got {!r}".format(where, what, addr)
            )

    _require_host_port(cfg["listen"], "listen")
    for peer in cfg["peers"]:
        _require_host_port(peer["host"], "peers[].host")

    # De-duplicate peers and drop any entry pointing at our own listen
    # address: cluster_size() (and thus the quorum threshold) is derived
    # from this list, so a duplicate or self entry would inflate the quorum
    # and cost fault tolerance.  First occurrence wins to preserve order.
    # (An exotic self-listing _is_self_listed misses still degrades to the
    # runtime STATUS_SELF exclusion in ClusterManager.cluster_size.)
    seen: "set[str]" = set()
    deduped: list[dict[str, Any]] = []
    for peer in cfg["peers"]:
        host = peer["host"]
        if (
            _is_self_listed(host, cfg["listen"], cfg["nodeName"])
            or host in seen
        ):
            continue
        seen.add(host)
        deduped.append(peer)
    cfg["peers"] = deduped

    if cfg["electLeader"]:
        # `peers` lists every OTHER member, so the cluster is that many plus
        # this node.
        size = len(cfg["peers"]) + 1
        if size == 2:
            # A 2-node quorum is 2: both must be up for *either* to run, so it
            # is strictly worse than a single replica (lower availability, and
            # still no failover) with no upside. Refuse it rather than silently
            # degrade. This keys off the declared size, so a 3+ node cluster
            # with a peer transiently down (a rolling deploy) is unaffected.
            raise ConfigError(
                "cluster.electLeader needs a fault-tolerant cluster, but "
                "this config declares only 2 nodes (1 peer). A quorum of 2 "
                "requires both nodes up for either to run, so it is strictly "
                "worse than a single replica. Use 3 or more nodes (an odd "
                "count is best), or run a single replica without electLeader."
            )
    return ClusterConfig(cfg)


def _reject_lease_spread(cfg: dict, backend: str) -> None:
    # A single lease holder cannot also be a per-job (spread) owner: there is
    # one fenced identity, not a quorate set to rendezvous-hash across.
    if cfg.get("distribution", "single-leader") != "single-leader":
        raise ConfigError(
            "cluster.distribution: spread is not supported with the {!r} "
            "backend (a single lease holder cannot fan jobs out per-node); "
            "use distribution: single-leader, or the gossip backend".format(
                backend
            )
        )


# Lease store sub-blocks. Each lease builder reads ONLY its own; a block under
# the wrong backend is rejected so the operator's intended endpoints/TLS/creds
# are never silently discarded (see _reject_foreign_store_blocks).
_LEASE_STORE_KEYS = ("etcd", "kubernetes", "filesystem")

# Kubernetes object-name charsets, used to keep leaseName/leaseNamespace clear
# of URL path metacharacters ('/', '?', '#', whitespace) that could retarget
# the apiserver request (see _K8sHttpTransport._lease_url).
_RFC1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_RFC1123_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")


def _reject_foreign_store_blocks(cfg: dict, backend: str) -> None:
    """Reject a lease store sub-block that does not match the chosen backend.

    Each builder consumes ONLY its own block, so a block carried under the
    wrong ``backend:`` would be silently ignored, discarding the operator's
    intended endpoints/TLS/credentials and arbitrating leadership against an
    unintended (default) store.  Fail loudly.
    """
    for key in _LEASE_STORE_KEYS:
        if key == backend:
            continue
        if cfg.get(key) is not None:
            raise ConfigError(
                "cluster.{} is configured but cluster.backend is {!r}; that "
                "store block would be silently ignored. Set backend: {} to "
                "use it, or remove the cluster.{} block.".format(
                    key, backend, key, key
                )
            )


# Cluster keys only the gossip transport consumes; a lease backend silently
# ignores them (most dangerously a `tls` block the operator may believe
# secures store traffic). interval/driftAfter are always present in the
# built cfg via DEFAULT_CLUSTER, so detect them from the *raw* block, where
# they appear only if the operator wrote them.
_GOSSIP_ONLY_CLUSTER_KEYS = (
    "listen",
    "tls",
    "peers",
    "interval",
    "driftAfter",
)


def _lease_advisories(raw: dict, backend: str) -> list[str]:
    """Non-fatal advisories for a lease (kubernetes/etcd) cluster block.

    Surfaced (once, via :func:`cluster_config_warnings`) rather than raised so
    an upgrade does not fail a previously-accepted config; promote to a hard
    ConfigError behind a deprecation window if stricter validation is wanted.
    """
    advisories: list[str] = []
    present = [k for k in _GOSSIP_ONLY_CLUSTER_KEYS if raw.get(k) is not None]
    if present:
        msg = (
            "cluster.{} configured but ignored by the {!r} backend (those "
            "keys apply only to backend: gossip)".format(
                ", cluster.".join(present), backend
            )
        )
        if "tls" in present:
            # the one with a security-relevant false belief attached.
            msg += (
                "; note cluster.tls does NOT secure the lease store -- the "
                "{} store's TLS is configured under cluster.{}".format(
                    backend, backend
                )
            )
        advisories.append(msg)
    if raw.get("electLeader") is False:
        # the override to True is unconditional (a lease backend is opting into
        # leadership); flag the swallowed explicit contradiction.
        advisories.append(
            "cluster.electLeader: false is ignored by the {!r} backend; a "
            "lease backend always enables leader election".format(backend)
        )
    return advisories


def _resolve_secret(spec: Optional[dict], what: str) -> Optional[str]:
    """Resolve a value/fromFile/fromEnvVar secret block, or ``None`` if unset.

    Mirrors :meth:`cronstable.cron.Cron._resolve_web_token`, but tolerates "no
    source configured" by returning ``None`` (etcd may need no auth at all).
    A source that *is* configured yet resolves empty fails closed.
    """
    if not spec:
        return None
    if spec.get("value"):
        secret = str(spec["value"])
    elif spec.get("fromFile"):
        try:
            # utf-8-sig, not the locale default, for the reason
            # parse_environment_file gives: on Windows "rt" decodes the ANSI
            # code page, so a UTF-8 secret with any non-ASCII byte is
            # mojibake from a file that is correct on Linux, and a BOM
            # (Notepad, a PowerShell redirect) survives .strip() and rides
            # into the secret itself.
            with open(
                spec["fromFile"], "rt", encoding="utf-8-sig"
            ) as secret_file:
                secret = secret_file.read().strip()
        # Broad on purpose: callers only handle ConfigError, and on the
        # job-secret staging path anything else escapes the scheduler loop
        # and crash-loops the daemon at every fire of that job.  Beyond
        # OSError, open()/read() can raise ValueError (NUL in path),
        # UnicodeDecodeError/UnicodeEncodeError (ValueError subclasses) and
        # TypeError.
        except (OSError, ValueError, TypeError) as ex:
            raise ConfigError(
                "{}.fromFile could not be read: {}".format(what, ex)
            ) from ex
    elif spec.get("fromEnvVar"):
        try:
            secret = os.environ.get(spec["fromEnvVar"], "")
        # same stakes as fromFile above: os.environ.get can raise
        # UnicodeEncodeError (a ValueError) and TypeError.
        except (ValueError, TypeError) as ex:
            raise ConfigError(
                "{}.fromEnvVar could not be read: {}".format(what, ex)
            ) from ex
    else:
        return None  # no source configured
    if not secret:
        raise ConfigError(
            "{} is configured but resolved to an empty secret".format(what)
        )
    return secret


def _build_kubernetes_cluster_config(raw: dict) -> ClusterConfig:
    cfg = _cluster_base(raw)
    _reject_lease_spread(cfg, "kubernetes")
    _reject_foreign_store_blocks(cfg, "kubernetes")
    k8s = dict(DEFAULT_K8S)
    k8s.update(cfg.get("kubernetes") or {})
    cfg["kubernetes"] = k8s
    if not k8s.get("identity"):
        # the lease holderIdentity that distinguishes this node; default it to
        # the (already-defaulted) nodeName.
        k8s["identity"] = cfg["nodeName"]
    # leaseName/leaseNamespace are spliced into the apiserver URL path (see
    # _K8sHttpTransport._lease_url); constrain them to the RFC1123 charset
    # so a stray '/', '?', '#' or space cannot retarget the request.
    lease_name = k8s["leaseName"]
    if not isinstance(lease_name, str) or (
        len(lease_name) > 253 or not _RFC1123_SUBDOMAIN.match(lease_name)
    ):
        raise ConfigError(
            "cluster.kubernetes.leaseName must be a valid RFC1123 name "
            "(lowercase alphanumeric, '-' or '.', <= 253 chars); got "
            "{!r}".format(lease_name)
        )
    lease_ns = k8s.get("leaseNamespace")
    if lease_ns is not None and (
        not isinstance(lease_ns, str)
        or len(lease_ns) > 63
        or not _RFC1123_LABEL.match(lease_ns)
    ):
        raise ConfigError(
            "cluster.kubernetes.leaseNamespace must be a valid RFC1123 label "
            "(lowercase alphanumeric or '-', <= 63 chars); got {!r}".format(
                lease_ns
            )
        )
    # The apiserver override carries the ServiceAccount bearer token on
    # every request; require https so that high-value credential is never
    # sent in cleartext (mirrors the etcd auth-over-https guard).
    api_server = k8s.get("apiServer")
    if api_server and not str(api_server).lower().startswith("https://"):
        raise ConfigError(
            "cluster.kubernetes.apiServer must be an https:// URL so the "
            "ServiceAccount bearer token is not sent in cleartext; got "
            # redact any embedded userinfo: a non-https apiServer carrying
            # credentials (http://tok:secret@host) is echoed into this
            # ConfigError, which the reload loop logs -- never leak the secret.
            "{!r}".format(_redact_userinfo(str(api_server)))
        )
    duration = k8s["leaseDurationSeconds"]
    renew = k8s["renewDeadlineSeconds"]
    retry = k8s["retryPeriodSeconds"]
    if renew <= 0:
        raise ConfigError(
            "cluster.kubernetes.renewDeadlineSeconds must be > 0"
        )
    if duration <= renew:
        # client-go's invariant: a holder must be able to renew well within the
        # window before the lease is considered expired by others.
        raise ConfigError(
            "cluster.kubernetes.leaseDurationSeconds ({}) must be greater "
            "than renewDeadlineSeconds ({})".format(duration, renew)
        )
    if retry <= 0:
        raise ConfigError("cluster.kubernetes.retryPeriodSeconds must be > 0")
    if retry >= renew:
        # client-go's third leaderelection invariant (RenewDeadline must
        # exceed the RetryPeriod): with retry >= renew a holder cannot
        # complete a renewal before the next attempt is due, so it lapses
        # out of the lease every cycle, is_leader flaps and no Leader job
        # runs stably. Reject rather than silently defeat the at-most-once
        # guarantee a lease backend exists to provide.
        raise ConfigError(
            "cluster.kubernetes.retryPeriodSeconds ({}) must be less than "
            "renewDeadlineSeconds ({}): a holder must be able to renew "
            "within the renew window before the next retry, or it lapses "
            "out of the lease every cycle and no Leader job runs "
            "stably".format(retry, renew)
        )
    if renew + retry >= duration:
        # The worst-case interval between two lease refreshes is
        # renewDeadline + retryPeriod, but the self-demotion deadline is
        # only leaseDuration ahead of a round's START.  The pairwise
        # invariants above do NOT bound the SUM (duration=12/renew=11/
        # retry=10 passes them yet flaps every cycle under a slow apiserver).
        # Require the sum to fit inside the lease window.
        raise ConfigError(
            "cluster.kubernetes: renewDeadlineSeconds ({}) + "
            "retryPeriodSeconds ({}) must be less than leaseDurationSeconds "
            "({}); otherwise the worst-case gap between lease renewals "
            "exceeds the lease lifetime and the holder lapses out of the "
            "lease every cycle, so no Leader job runs stably".format(
                renew, retry, duration
            )
        )
    # configuring a lease backend is opting into leadership.
    cfg["electLeader"] = True
    advisories = _lease_advisories(raw, "kubernetes")
    if advisories:
        cfg["_advisories"] = advisories
    return ClusterConfig(cfg)


# An RFC 3986 scheme (ALPHA *(ALPHA / DIGIT / "+" / "-" / ".")) followed by
# ":" -- recognised as a scheme only when "//" (an authority) follows, so a
# scheme-less "user:pass@host" is NOT mistaken for scheme "user".
_URL_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:(?=//)")


def _url_authority_span(url: str) -> tuple[int, int]:
    """The ``[start, end)`` span of ``url`` that can carry userinfo.

    The ONE authority locator shared by :func:`_url_has_userinfo` and
    :func:`_redact_userinfo`, so the detector and the redactor can never
    disagree about where a credential may live.  Per RFC 3986 the authority
    follows ``scheme://`` (or a protocol-relative ``//``) and ends at the
    first ``/``, ``?`` or ``#``; a scheme-less ``user:pass@host`` (which
    urlparse misreads as scheme ``user``) is treated as all-authority, so a
    credentialed endpoint is caught whether or not a scheme is present.
    """
    match = _URL_SCHEME_RE.match(url)
    if match is not None:
        start = match.end() + 2
    elif url.startswith("//"):
        start = 2
    else:
        start = 0
    end = len(url)
    for delimiter in "/?#":
        found = url.find(delimiter, start)
        if found != -1 and found < end:
            end = found
    return start, end


def _url_has_userinfo(url: str) -> bool:
    """Whether ``url``'s authority carries ``user[:pass]@`` userinfo.

    Robust to a scheme-less ``user:pass@host:port``.  Shares
    :func:`_url_authority_span` with :func:`_redact_userinfo` so anything
    detected here is by construction redactable there.
    """
    start, end = _url_authority_span(url)
    return "@" in url[start:end]


def _redact_userinfo(url: str) -> str:
    """Replace ``user:pass@`` userinfo in ``url`` with ``***@`` for logs.

    Deliberately does NOT rely on ``urlparse(...).username``: a scheme-less
    ``user:pass@host`` parses as scheme ``user`` with no username/password,
    so trusting that would echo the secret verbatim (the leak this helper
    exists to prevent).  The authority is located via the same
    :func:`_url_authority_span` the detector uses.
    """
    start, end = _url_authority_span(url)
    authority = url[start:end]
    if "@" not in authority:
        return url
    # Split on the LAST '@': userinfo ends at the final '@', so a password that
    # itself contains '@' (e.g. user:p@ss@host) is not split on its first '@',
    # which would leave a tail of the secret ('ss@host') in the output.
    hostpart = authority.rsplit("@", 1)[1]
    return "{}***@{}{}".format(url[:start], hostpart, url[end:])


def _safe_urlparse(text: str, what: str) -> "ParseResult":
    """``urlparse(text)``, with a malformed URL as a ConfigError.

    urlsplit raises ValueError on an unterminated ``[`` bracket, and config
    parsing must only ever raise ConfigError, so every urlparse of
    operator-supplied text goes through here.  ``what`` names the offending
    key; the value is redacted, since these keys may carry credentials.
    """
    try:
        return urlparse(text)
    except ValueError as err:
        raise ConfigError(
            "{} is not a valid URL: {!r}".format(what, _redact_userinfo(text))
        ) from err


def _require_lease_ttl_floor(ttl: float, what: str) -> None:
    """Reject a lease ttl below 3 seconds, or a non-finite one.

    Below a 3s ttl the leader window (ttl minus a 1s clock-skew margin,
    renewed every max(1s, ttl/3)) collapses to <= the keepalive period: a
    node that wins the election immediately treats its own lease as expired
    and NO Leader job ever runs cluster-wide.  NaN-rejecting on purpose,
    like the state TTL floors: strictyaml's Float() accepts 'nan' (for which
    'x < 3' is False) and overflow literals like '1e309' (== inf), and
    either silently breaks the lease expiry arithmetic (multiple leaders /
    a crashed leader's lease never expiring).  Shared by the etcd and
    filesystem backends; kubernetes is protected by its own duration>renew
    invariants instead.
    """
    if not math.isfinite(float(ttl)) or ttl < 3:
        raise ConfigError(
            "{} must be >= 3 seconds and finite (the leader holds the "
            "lease only until ttl minus a clock-skew margin and renews "
            "every max(1s, ttl/3); a smaller ttl makes a node that wins "
            "the election immediately treat its own lease as expired, so "
            "no Leader job ever runs); got {}".format(what, ttl)
        )


def _build_etcd_cluster_config(raw: dict) -> ClusterConfig:
    cfg = _cluster_base(raw)
    _reject_lease_spread(cfg, "etcd")
    _reject_foreign_store_blocks(cfg, "etcd")
    raw_etcd = cfg.get("etcd") or {}
    etcd = copy.deepcopy(DEFAULT_ETCD)
    etcd.update(raw_etcd)
    # the nested password/tls blocks are merged (a plain update would replace
    # them wholesale, dropping the unset sub-keys' defaults).
    etcd["password"] = {
        **DEFAULT_ETCD["password"],
        **(raw_etcd.get("password") or {}),
    }
    etcd["tls"] = {**DEFAULT_ETCD["tls"], **(raw_etcd.get("tls") or {})}
    cfg["etcd"] = etcd
    _require_lease_ttl_floor(etcd["ttl"], "cluster.etcd.ttl")
    if not etcd["endpoints"]:
        raise ConfigError("cluster.etcd.endpoints must list at least one URL")
    for endpoint in etcd["endpoints"]:
        parsed = _safe_urlparse(endpoint, "cluster.etcd.endpoints")
        # Reject credentials embedded in the URL FIRST, before the
        # scheme/port check below: an endpoint with BOTH embedded
        # credentials AND a bad scheme/port must land here (always
        # redacted), never in the scheme/port branch.
        if _url_has_userinfo(endpoint):
            # _url_has_userinfo (not parsed.username) so a scheme-less
            # user:pass@host is still caught here and redacted.
            raise ConfigError(
                "cluster.etcd.endpoints must not embed credentials in the URL "
                "(userinfo@host); use cluster.etcd.username/password instead, "
                "got {!r}".format(_redact_userinfo(endpoint))
            )
        # urlparse's .port *raises* ValueError on a non-numeric or
        # out-of-range port; guard it so a typo surfaces as a clean
        # ConfigError.
        try:
            port = parsed.port
        except ValueError:
            bad_port = True
        else:
            # a missing port defaults to the scheme's port at connection
            # time; only an explicitly-present out-of-range port is rejected.
            bad_port = port is not None and not 0 < port <= 65535
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or bad_port
        ):
            # redact too (defence in depth): the userinfo check above already
            # rejected any credentialed endpoint; still, never echo a raw URL.
            raise ConfigError(
                "cluster.etcd.endpoints must be http(s)://host[:port], "
                "got {!r}".format(_redact_userinfo(endpoint))
            )
    # mTLS to etcd needs BOTH the client cert and key; one without the other
    # silently degrades to one-way TLS. Require them together.
    tls = etcd["tls"]
    if bool(tls.get("cert")) != bool(tls.get("key")):
        raise ConfigError(
            "cluster.etcd.tls.cert and cluster.etcd.tls.key must be set "
            "together (a client certificate needs its private key); got "
            "cert={!r}, key={!r}".format(
                bool(tls.get("cert")), bool(tls.get("key"))
            )
        )
    # TLS material supplied but every endpoint is plaintext -> the material is
    # silently ignored (_build_ssl only builds a context for an https endpoint)
    # and traffic goes in cleartext. That is always a misconfiguration (a
    # forgotten 's' in https://), so refuse it rather than quietly downgrade.
    if any(tls.get(key) for key in ("ca", "cert", "key")) and not any(
        _safe_urlparse(endpoint, "cluster.etcd.endpoints").scheme == "https"
        for endpoint in etcd["endpoints"]
    ):
        raise ConfigError(
            "cluster.etcd.tls is configured but no endpoint is https:// , so "
            "the TLS material would be ignored and traffic sent in cleartext; "
            "use https:// endpoints or remove cluster.etcd.tls"
        )
    # resolve the password once at load time (fail closed on an empty source),
    # like the web auth token; None when etcd needs no auth.
    etcd["resolved_password"] = _resolve_secret(
        etcd["password"], "cluster.etcd.password"
    )
    if etcd["username"] and not etcd["resolved_password"]:
        # etcd auth needs a password for the username; without one, auth
        # fails opaquely every round instead of as a clean config error.
        raise ConfigError(
            "cluster.etcd.username is set but no password is configured; set "
            "cluster.etcd.password (value/fromFile/fromEnvVar)"
        )
    if etcd["username"] or etcd["resolved_password"]:
        # Auth credentials (and the bearer token attached to every request
        # thereafter) must never travel unencrypted: the _post failover loop
        # would POST them over any plaintext member, including the plaintext
        # one in a mixed http/https list. Refuse at load time.
        insecure = [
            endpoint
            for endpoint in etcd["endpoints"]
            if _safe_urlparse(endpoint, "cluster.etcd.endpoints").scheme
            != "https"
        ]
        if insecure:
            raise ConfigError(
                "cluster.etcd: authentication (username/password) requires "
                "https:// endpoints so credentials are not sent in "
                "cleartext, but these endpoints are plaintext: {}".format(
                    ", ".join(insecure)
                )
            )
    cfg["electLeader"] = True
    advisories = _lease_advisories(raw, "etcd")
    # A small ttl shrinks the per-request renew timeout budget below a real
    # cross-AZ round-trip, making a reachable etcd look unreachable (Leader
    # jobs fail closed). Warn rather than reject: a local etcd is fine.
    # (Mirrors EtcdBackend's cadence constants: 1s skew, ttl/3 renew period,
    # 5 POSTs per renew cycle.)
    renew_period = max(1.0, etcd["ttl"] / 3)
    round_deadline = max(1.0, etcd["ttl"] - renew_period - 1.0)
    per_post_budget = round_deadline / 5
    if per_post_budget < 1.0:
        advisories.append(
            "cluster.etcd.ttl={}s leaves only a ~{:.1f}s per-request timeout "
            "for each renew POST to etcd; if a single round-trip is slower "
            "than that (e.g. a cross-AZ/region endpoint) every renew round "
            "will time out and this node will treat a reachable etcd as "
            "unreachable, so Leader jobs fail closed. Raise cluster.etcd.ttl "
            "unless etcd is local and low-latency.".format(
                etcd["ttl"], per_post_budget
            )
        )
    if advisories:
        cfg["_advisories"] = advisories
    return ClusterConfig(cfg)


def _build_filesystem_cluster_config(raw: dict) -> ClusterConfig:
    """Build the cluster config for the shared-mount (filesystem) backend.

    Validation: a non-empty path, a ttl floor (same rationale as etcd's) and
    the shared lease-backend rules (no spread, no foreign store blocks,
    electLeader implied).
    """
    cfg = _cluster_base(raw)
    _reject_lease_spread(cfg, "filesystem")
    _reject_foreign_store_blocks(cfg, "filesystem")
    raw_fs = cfg.get("filesystem") or {}
    fsb = dict(DEFAULT_FILESYSTEM)
    fsb.update(raw_fs)
    cfg["filesystem"] = fsb
    if not fsb.get("path") or not str(fsb["path"]).strip():
        raise ConfigError(
            "cluster.filesystem.path is required and must be non-empty "
            "(the directory -- normally a shared mount -- the election "
            "lease lives in)"
        )
    if not str(fsb.get("electionName") or "").strip():
        raise ConfigError("cluster.filesystem.electionName must be non-empty")
    _require_lease_ttl_floor(fsb["ttl"], "cluster.filesystem.ttl")
    cfg["electLeader"] = True
    advisories = _lease_advisories(raw, "filesystem")
    if advisories:
        cfg["_advisories"] = advisories
    return ClusterConfig(cfg)


def cluster_config_warnings(cfg: ClusterConfig) -> list[str]:
    """Non-fatal advisories for a cluster config, returned as messages.

    Returned (not logged) so the caller can emit them *once* when the
    cluster manager (re)starts: the daemon re-parses its config every
    wakeup, so logging here would spam the same warning every minute.
    """
    # Lease-backend advisories are computed at build time (where the raw
    # block is available) and stashed on the cfg; surfaced here so they ride
    # the same emit-once channel.
    warnings: list[str] = list(cfg.get("_advisories", ()))
    if cfg.get("backend", "gossip") != "gossip":
        # The lease backends have no static peer set or even/odd-size
        # trade-off, and always imply electLeader, so the gossip-only
        # advisories below (which read cfg["peers"]) do not apply.
        return warnings
    if cfg.get("electLeader"):
        # `peers` lists every OTHER member, so size is that many plus self.
        size = len(cfg["peers"]) + 1
        # size == 2 is rejected outright in _build_cluster_config; an even
        # size > 2 tolerates the same failures as the odd size below it (e.g. 4
        # tolerates 1, same as 3), so the extra node only adds something that
        # can fail. Allowed, but worth a warning.
        if size > 2 and size % 2 == 0:
            warnings.append(
                "cluster.electLeader: an even cluster size ({} nodes) "
                "tolerates no more failures than {} (the next-lower odd "
                "size); shrink to {} for the same tolerance with one fewer "
                "node, or grow to {} to tolerate one more failure; prefer an "
                "odd size.".format(size, size - 1, size - 1, size + 1)
            )
        # A self-listing by FQDN is not dropped at config time, so the
        # declared size can hide the degenerate 2-node case the size==2
        # refusal exists to catch. Warn so the operator fixes the listing
        # rather than discovering it as flapping leadership.
        listen = cfg.get("listen") or ""
        node_name = cfg["nodeName"]
        self_hosts = [
            peer["host"]
            for peer in cfg["peers"]
            if _likely_self_fqdn(peer["host"], listen, node_name)
        ]
        if self_hosts and size - len(self_hosts) <= 2:
            warnings.append(
                "cluster.electLeader: {} peer(s) look like this node listed "
                "by FQDN ({}) while nodeName is {!r}; if so the real cluster "
                "is only {} node(s) and leader election will be degenerate or "
                "refused at runtime. List the peer by its exact nodeName, or "
                "fix the addresses.".format(
                    len(self_hosts),
                    ", ".join(self_hosts),
                    node_name,
                    size - len(self_hosts),
                )
            )
        # The loopback analogue: unambiguous forms are dropped at load
        # (_is_self_listed); this advisory covers what remains (localhost,
        # or a family/listen mismatch). A self-listing by a routable IP is
        # undetectable without resolving addresses and is caught at runtime
        # as STATUS_SELF instead.
        loopback_hosts = [
            peer["host"]
            for peer in cfg["peers"]
            if _likely_self_loopback(peer["host"], listen)
        ]
        if loopback_hosts and size - len(loopback_hosts) <= 2:
            warnings.append(
                "cluster.electLeader: {} peer(s) are loopback addresses on "
                "this node's own port ({}); a loopback entry never reaches "
                "another host, so at best it is this node itself and the "
                "real cluster is only {} node(s) -- leader election will be "
                "degenerate or refused at runtime. List each peer by an "
                "address the other nodes are reached at.".format(
                    len(loopback_hosts),
                    ", ".join(loopback_hosts),
                    size - len(loopback_hosts),
                )
            )
    elif cfg.get("distribution") != DEFAULT_CLUSTER["distribution"]:
        # distribution only governs how *leader-gated* jobs spread, so it does
        # nothing without electLeader.
        warnings.append(
            "cluster.distribution={!r} has no effect without electLeader; "
            "without leader election every node runs every job.".format(
                cfg.get("distribution")
            )
        )
    return warnings


def _validate_web_tls(webconf: WebConfig) -> None:
    """Cross-checks between the `web.listen` schemes and the `web.tls` block.

    Mirrors the etcd client-TLS checks: material with no transport, or a
    transport with no material, is a silent-downgrade misconfiguration.
    Fails at parse time because the runtime bind loop skips a bad listener
    with only a warning ("the dashboard is just gone").

    Whether the files exist or load is deliberately NOT checked here:
    nothing in this module touches the filesystem, ``--validate-config`` may
    run off the deployment target, and a Kubernetes-mounted secret need not
    exist yet at first boot.  That gate lives at the listener, in
    :meth:`cronstable.cron.Cron.start_stop_web_app`.
    """
    tls = webconf.get("tls") or {}
    cert, key = tls.get("cert"), tls.get("key")
    client_ca = tls.get("clientCa")
    listen = webconf.get("listen") or []
    https = [a for a in listen if str(a).startswith("https://")]

    if bool(cert) != bool(key):
        raise ConfigError(
            "web.tls.cert and web.tls.key must be set together (a server "
            "certificate needs its private key); got cert={!r}, "
            "key={!r}".format(bool(cert), bool(key))
        )
    if client_ca and not cert:
        raise ConfigError(
            "web.tls.clientCa is set but web.tls.cert and web.tls.key are "
            "not; a listener cannot require client certificates without "
            "serving one of its own"
        )
    if (cert or client_ca) and not https:
        raise ConfigError(
            "web.tls is configured but no web.listen address uses https:// , "
            "so the TLS material would be ignored and the API served in "
            "cleartext; use an https:// listen address or remove web.tls"
        )
    if https and not cert:
        raise ConfigError(
            "web.listen has https:// address(es) {} but no web.tls.cert / "
            "web.tls.key, so those listeners have no certificate to serve "
            "and would be skipped at startup; set web.tls.cert and "
            "web.tls.key, or use http:// addresses".format(", ".join(https))
        )


def _validate_web_config(webconf: WebConfig) -> None:
    """Range checks the schema cannot express, mirroring the cluster
    builders: fail at parse time (so ``--validate-config`` catches it)
    rather than when the first scrape arrives."""
    # First: the early returns below would skip it.
    _validate_web_tls(webconf)
    if webconf.get("anonymousScopes") and not _web_has_any_token(webconf):
        raise ConfigError(
            "web.anonymousScopes is set but no web.authToken or "
            "web.authTokens entry is configured; with no tokens the web "
            "API installs no auth middleware and every request already "
            "holds every scope, so an anonymous grant would only mislead. "
            "Configure at least one token, or remove anonymousScopes."
        )
    for entry in webconf.get("authTokens") or ():
        if entry.get("label") == "anonymous":
            raise ConfigError(
                "web.authTokens label 'anonymous' is reserved: /whoami and "
                "the pairing audit log use it for the credential-less "
                "grant (web.anonymousScopes). Pick another label."
            )
    if resolve_bonjour_config(webconf) is not None:
        # Imported here so a config without the advert never pays for
        # the probe; discovery.py itself imports zeroconf guardedly.
        from cronstable.discovery import HAVE_ZEROCONF

        if not HAVE_ZEROCONF:
            raise ConfigError(
                "web.bonjour is enabled but python-zeroconf is not "
                "installed; install the discovery extra (pip install "
                '"cronstable[discovery]") or disable web.bonjour'
            )
        tcp_listens = [
            addr
            for addr in (webconf.get("listen") or [])
            if not str(addr).startswith("unix://")
        ]
        if not tcp_listens:
            raise ConfigError(
                "web.bonjour needs a TCP web listener to advertise, but "
                "every web.listen entry is a unix socket"
            )
    history = webconf.get("nodeHistory")
    if isinstance(history, dict):
        interval = history.get("interval")
        # the sampler memoises snapshots for ~1s (NODE_SNAPSHOT_TTL), so a
        # faster cadence would only record duplicate readings.
        if interval is not None and interval < 1.0:
            raise ConfigError("web.nodeHistory.interval must be >= 1 second")
        points = history.get("points")
        if points is not None and not (10 <= points <= 50000):
            raise ConfigError(
                "web.nodeHistory.points must be between 10 and 50000"
            )
    metrics = webconf.get("metrics")
    if not isinstance(metrics, dict):
        return
    buckets = metrics.get("durationBuckets")
    if buckets is None:
        return
    if not buckets:
        raise ConfigError("web.metrics.durationBuckets must not be empty")
    previous = 0.0
    for bound in buckets:
        # finite, positive, strictly increasing: anything else produces an
        # invalid or duplicate-le histogram exposition.
        if not math.isfinite(bound) or bound <= previous:
            raise ConfigError(
                "web.metrics.durationBuckets must be finite, positive and "
                "strictly increasing (got {!r})".format(buckets)
            )
        previous = bound


def _build_mcp_config(raw: Optional[dict]) -> MCPConfig:
    """Fill the optional ``mcp:`` section over :data:`DEFAULT_MCP`.

    An absent or empty block yields the defaults (the server disabled), so a
    bare ``mcp: {}`` is inert.  Cross-section constraints that also need the
    web section (the fail-closed no-auth check) live in
    :func:`_validate_mcp_config`, run once on the fully assembled config.
    """
    merged: dict[str, Any] = {**DEFAULT_MCP, **(raw or {})}
    # dedupe toolsets while preserving order, so `[observe, observe]` is one.
    seen: set = set()
    toolsets: list[str] = []
    for name in merged["toolsets"]:
        if name not in seen:
            seen.add(name)
            toolsets.append(name)
    merged["toolsets"] = toolsets
    if merged["maxRows"] < 1:
        raise ConfigError("mcp.maxRows must be >= 1")
    if merged["maxBodyBytes"] < 1:
        raise ConfigError("mcp.maxBodyBytes must be >= 1")
    return MCPConfig(merged)


def _is_local_listener(addr: str) -> bool:
    """True if a web ``listen`` address is loopback-only or a unix socket.

    These are the addresses on which an unauthenticated ``/mcp`` is
    acceptable -- nothing off-host can reach them; every other address is
    routable and must carry authentication (see :func:`_validate_mcp_config`).
    """
    text = addr if "://" in addr else "http://" + addr
    parsed = _safe_urlparse(text, "web.listen")
    if parsed.scheme == "unix":
        return True
    host = (parsed.hostname or "").lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _web_has_any_token(web: dict) -> bool:
    """Whether the web config declares any bearer token at all.

    True when either the scalar ``web.authToken`` or one or more scoped
    ``web.authTokens`` entries are present. A bare presence test (like the
    fail-closed MCP gate needs): it does not resolve the sources.
    """
    return bool(web.get("authToken") or web.get("authTokens"))


def _open_routable_listeners(web: dict) -> list[str]:
    """Web listeners a stranger could reach with no credential at all.

    Shared by the ``mcp`` and ``push`` fail-closed gates: cronstable
    installs its auth middleware only when a token resolves, so with no
    token every route answers whoever can connect.  Empty when any token is
    configured, or when every listener is loopback or a unix socket.

    An ``https`` listener with ``web.tls.clientCa`` authenticates callers
    at the transport, so it does not count as open.  Plain ``https`` does:
    transport encryption is not caller authentication.
    """
    if _web_has_any_token(web):
        return []
    mtls = bool((web.get("tls") or {}).get("clientCa"))
    return [
        a
        for a in web.get("listen") or ()
        if not _is_local_listener(a)
        and not (mtls and str(a).startswith("https://"))
    ]


def _validate_mcp_config(config: "CronstableConfig") -> None:
    """Fail-closed checks for the MCP server that also need the web section.

    Runs at the top-level parse (from :func:`_validate_cross_sections`), where
    the web and mcp sections are both fully merged -- an ``mcp`` block and the
    ``web`` listeners it rides on may legitimately live in different
    config-directory files.
    """
    mcp = config.mcp_config
    if mcp is None or not mcp.get("enabled"):
        return
    web = config.web_config
    if web is None or not web.get("listen"):
        raise ConfigError(
            "mcp.enabled requires a `web` section with at least one `listen` "
            "address: the MCP endpoint (POST /mcp) is served on the web "
            "listeners"
        )
    # An mcp block that names `act` but leaves readOnly on gets read-only tools
    # only; warn rather than fail so a "prepare to enable writes" config is
    # still loadable.
    if "act" in mcp.get("toolsets", ()) and mcp.get("readOnly"):
        logger.warning(
            "mcp.toolsets includes 'act' but mcp.readOnly is true; mutating "
            "tools stay suppressed until readOnly is set false"
        )
    if mcp.get("allowUnauthenticated"):
        return
    routable = _open_routable_listeners(web)
    if routable:
        raise ConfigError(
            "mcp.enabled is set but no web.authToken/authTokens is set, and "
            "the web API listens on non-loopback address(es) {}: /mcp would "
            "be served without authentication (with no token the web app "
            "installs no auth middleware at all). Set web.authToken or a "
            "web.authTokens entry, restrict web.listen "
            "to loopback/unix-socket addresses, set web.tls.clientCa so the "
            "listener authenticates callers by certificate, or set "
            "mcp.allowUnauthenticated: true when the endpoint is protected "
            "by other means (an mTLS-terminating proxy, a network "
            "policy).".format(", ".join(routable))
        )


def _push_report_users(config: "CronstableConfig") -> list[str]:
    """Every place the assembled config enables the push reporter.

    Human-readable names ("job backup", "dag etl task load", "notify"),
    used to point at the offenders when a ``push:`` section is missing.
    """

    def _uses(holder: Any) -> bool:
        for action in (
            "onFailure",
            "onPermanentFailure",
            "onSuccess",
            "onLate",
        ):
            block = getattr(holder, action, None)
            if isinstance(block, dict):
                push_report = (block.get("report") or {}).get("push") or {}
                if push_report.get("enabled"):
                    return True
        return False

    users = []
    for job in config.jobs:
        if _uses(job):
            users.append("job {}".format(job.name))
    for dag_config in config.dags:
        for taskkey, template in dag_config.task_templates.items():
            if _uses(template):
                users.append("dag {} task {}".format(dag_config.name, taskkey))
    notify = config.notify_config
    if notify is not None:
        push_report = (notify.get("report") or {}).get("push") or {}
        if push_report.get("enabled"):
            users.append("notify")
    return users


def _eventlog_report_users(config: "CronstableConfig") -> list[str]:
    """Every place the assembled config ENABLES the eventlog reporter.

    Deliberately a near-copy of :func:`_push_report_users` rather than the
    two sharing one walker.  That function's inner ``_uses`` returns on the
    first matching hook, so it yields at most one name per job, and it feeds
    a fail-closed refusal whose exact wording operators have been reading
    since push shipped.  Folding both through one traversal would either
    change that wording or constrain this one; nine duplicated lines is the
    cheaper of the two.
    """

    def _uses(holder: Any) -> bool:
        for action in (
            "onFailure",
            "onPermanentFailure",
            "onSuccess",
            "onLate",
        ):
            block = getattr(holder, action, None)
            if isinstance(block, dict):
                report = (block.get("report") or {}).get("eventlog") or {}
                if report.get("enabled"):
                    return True
        return False

    users = []
    for job in config.jobs:
        if _uses(job):
            users.append("job {}".format(job.name))
    for dag_config in config.dags:
        for taskkey, template in dag_config.task_templates.items():
            if _uses(template):
                users.append("dag {} task {}".format(dag_config.name, taskkey))
    notify = config.notify_config
    if notify is not None:
        report = (notify.get("report") or {}).get("eventlog") or {}
        if report.get("enabled"):
            users.append("notify")
    return users


#: Log names, which are not source names.  Registering a source under one of
#: these addresses the log's own key rather than a source under it, so the
#: write either lands somewhere the operator did not mean or (Security) is
#: refused for want of a privilege the daemon does not hold.
_EVENTLOG_RESERVED_SOURCES = frozenset({"application", "system", "security"})


def _validate_eventlog_config(config: "CronstableConfig") -> None:
    """Shape checks for ``report.eventlog``, plus the POSIX advisory.

    Both refusals apply ONLY to blocks that are enabled.  ``source`` carries
    a default into every report block of every job, every DAG task template
    and ``notify:`` through ``_REPORT_DEFAULTS``, so inspecting disabled
    blocks would let a stray value in a block nobody turned on refuse the
    whole configuration.

    On POSIX this warns rather than refusing, which is the opposite of what
    ``user``/``group`` does one screen up, and the difference is the point.
    ``user`` on Windows is a ConfigError because the alternative is running
    a job as the WRONG account, and ``push`` without a ``push:`` section is
    one because the operator can add the section.  Neither applies here:
    nothing can be installed on Linux to make an Event Log appear, so
    refusing would mean one config directory cannot serve a mixed fleet,
    which is precisely the complaint the Windows notes file against the
    ``user``/``group`` rejection.  What keeps a warning honest rather than a
    silent self-disable is that it fires on every load, names every hook
    that asked, and is emitted by ``--validate-config`` too.
    """
    users = _eventlog_report_users(config)

    def _blocks() -> Any:
        holders = [(job.name, job) for job in config.jobs]
        for dag_config in config.dags:
            for taskkey, template in dag_config.task_templates.items():
                holders.append(
                    (
                        "dag {} task {}".format(dag_config.name, taskkey),
                        template,
                    )
                )
        for where, holder in holders:
            for action in (
                "onFailure",
                "onPermanentFailure",
                "onSuccess",
                "onLate",
            ):
                block = getattr(holder, action, None)
                if isinstance(block, dict):
                    yield where, (block.get("report") or {})
        notify = config.notify_config
        if notify is not None:
            yield "notify", (notify.get("report") or {})

    for where, report in _blocks():
        eventlog = report.get("eventlog") or {}
        if not eventlog.get("enabled"):
            continue
        source = eventlog.get("source") or ""
        if not source or "\\" in source or "/" in source:
            raise ConfigError(
                "{}: report.eventlog.source must be a non-empty name with "
                "no path separator (it names a registry key under the "
                "event log, not a path); got {!r}".format(where, source)
            )
        if source.lower() in _EVENTLOG_RESERVED_SOURCES:
            raise ConfigError(
                "{}: report.eventlog.source {!r} is the name of a LOG, not "
                "of a source within one; choose a source name such as "
                "'cronstable'".format(where, source)
            )

    if users and not platform.IS_WINDOWS:
        logger.warning(
            "report.eventlog is enabled (%s) but there is no Windows Event "
            "Log on this platform, so those reports are dropped; the rest "
            "of each report block still fires normally",
            ", ".join(sorted(users)),
        )


def _validate_push_config(config: "CronstableConfig") -> None:
    """Fail-closed checks for push that need the fully assembled config.

    Runs at the top-level parse (from :func:`_validate_cross_sections`): the
    sections involved may live in different config-dir files.  Everything
    here refuses to start rather than degrade: an alerting channel that
    silently self-disables is a missed page at 2 a.m., the one failure mode
    this feature exists to prevent.
    """
    users = _push_report_users(config)
    push_conf = config.push_config
    if push_conf is None:
        if users:
            raise ConfigError(
                "report.push.enabled is set ({}) but no `push:` section "
                "is configured; add one (push.relay.url plus a `state:` "
                "section or push.devicesFile) or disable the push "
                "reporter".format(", ".join(sorted(users)))
            )
        return
    # Imported here, not at module top: cronstable.push pulls in aiohttp,
    # which a bare config parse should not pay for.
    from cronstable.push import HAVE_PYNACL

    if not HAVE_PYNACL:
        raise ConfigError(
            "a `push:` section is configured but PyNaCl is not installed; "
            'install the push extra (pip install "cronstable[push]") or '
            "remove the section. Push alerts fail closed rather than "
            "silently self-disabling."
        )
    if not push_conf.get("devicesFile") and config.state_config is None:
        raise ConfigError(
            "push: needs durable storage for its paired-device registry: "
            "configure a `state:` section (shared store, cluster-visible "
            "pairings) or set push.devicesFile (single node)"
        )
    # The same gate the mcp section gets: without a token there is no auth
    # middleware to gate /push/devices, and an open pairing endpoint's
    # damage outlives the exposure (a stranger who pairs once keeps
    # receiving every alert, forever).  No web section means no pairing
    # endpoint at all, a legitimate cluster shape (other nodes only send to
    # the shared registry), so gate on the listeners, never on push itself.
    web = config.web_config
    if web is not None and not push_conf.get("allowUnauthenticated"):
        routable = _open_routable_listeners(web)
        if routable:
            raise ConfigError(
                "a `push:` section is configured but no "
                "web.authToken/authTokens is set, and the web API listens "
                "on non-loopback address(es) {}: the /push/devices pairing "
                "endpoints would be served without authentication (with no "
                "token the web app installs no auth middleware at all), so "
                "anything that can reach the listener can pair its own key "
                "and receive every job failure, hostname, exit code and "
                "captured output tail from then on. Set web.authToken or a "
                "web.authTokens entry, restrict web.listen to "
                "loopback/unix-socket addresses, set web.tls.clientCa so "
                "the listener authenticates callers by certificate, or set "
                "push.allowUnauthenticated: true when the endpoints are "
                "protected by other means (an mTLS-terminating proxy, a "
                "network policy).".format(", ".join(routable))
            )


@dataclass(slots=True)
class CronstableConfig:
    jobs: list[JobConfig]
    web_config: Optional[WebConfig]
    job_defaults: JobDefaults
    logging_config: Optional[LoggingConfig]
    # The optional sections default to None (feature off, classic behaviour)
    # so existing constructors (e.g. the empty config in Cron.update_config)
    # need no change.
    cluster_config: Optional[ClusterConfig] = None
    state_config: Optional[StateConfig] = None
    # A mutable default needs field(default_factory), never a shared [].
    dags: list["DagConfig"] = field(default_factory=list)
    mcp_config: Optional[MCPConfig] = None
    notify_config: Optional[dict[str, Any]] = None
    push_config: Optional[dict[str, Any]] = None


# Environment-variable interpolation over the validated config document.
#
# After strictyaml validates a YAML file, every ``${VAR}`` /
# ``${VAR:-default}`` in a *string* value is replaced with the environment
# variable VAR (``:-default`` falls back when unset or empty, like the POSIX
# shell; ``$$`` escapes a literal ``$``).  An unset variable with no default
# is a hard ConfigError naming the variable and where it appeared.  Only the
# braced forms are recognised: a lone ``$``, a bare ``$VAR``, or a malformed
# ``${...}`` is left verbatim, so a config that never used the syntax is
# untouched.  Expansion runs post-validation, so only fields strictyaml
# accepted as strings are eligible (put a variable inside a string, e.g.
# ``listen: ["0.0.0.0:${PORT}"]``).
#
# Skipped whole, because a ``${...}`` there is another layer's syntax:
# * a job's or DAG task's ``command`` and ``shell``, and a shell reporter's
#   ``shell`` block: the runtime shell expands those against the *job's*
#   environment at execution time, not the daemon's;
# * the top-level ``logging`` section: handed to ``logging.config``
#   verbatim, where a ``$``-style formatter legitimately writes
#   ``${asctime}``.
# The skip is decided by WHERE a key sits, not by its spelling: a
# user-chosen key named ``command``/``shell``/``logging`` inside an
# arbitrary-key map is interpolated like any other value (see
# _env_child_kind).
#
# Fingerprints hash the post-expansion config, so a job set interpolating
# env vars gets a different job-set id per environment; intended, the
# configs really do differ.
#
# The scanner is hand-rolled and single-pass: ``re.sub`` with a
# ``:-([^}]*)`` default is O(n^2) on a value carrying many unterminated
# ``${x:-`` fragments, which would stall config load and hot-reload.  Names
# are ASCII ``[A-Za-z_][A-Za-z0-9_]*``, read by one anchored match that
# consumes the name and nothing else, so it can never rescan the tail.
_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _resolve_env(
    name: str, default: Optional[str], path: str, location: str
) -> str:
    """Resolve one ``${VAR}`` / ``${VAR:-default}`` reference to its value.

    ``default`` is the captured fallback text, or None for a braced reference
    that carried no ``:-`` at all.  ``location`` is a dotted path to the value
    inside the document (e.g. ``web.listen[0]``); it appears only in the
    unset-variable error so the operator can find the offending key.
    """
    present = name in os.environ
    value = os.environ.get(name, "")
    if default is not None:
        # `:-` falls back to the default when unset OR set-but-empty.
        return value if (present and value != "") else default
    # A bare `${VAR}` yields the value whenever the variable is set (even to
    # the empty string); only a genuinely unset variable is an error.
    if present:
        return value
    where = "config value {}".format(location) if location else "the config"
    raise ConfigError(
        "{}: {} references environment variable ${{{}}}, which is not "
        "set; export it, or write ${{{}:-default}} to supply a "
        "fallback".format(path, where, name, name)
    )


def _interpolate_env_value(raw: str, path: str, location: str) -> str:
    """Expand ``${VAR}`` / ``${VAR:-default}`` / ``$$`` in one string value.

    A single linear left-to-right pass equivalent to the grammar
    ``$$ | ${NAME} | ${NAME:-DEFAULT}`` (NAME is ``[A-Za-z_][A-Za-z0-9_]*``,
    DEFAULT is any run of non-``}`` characters); every other ``$`` (a lone
    ``$``, a bare ``$VAR``, a malformed ``${...``) is copied verbatim.
    """
    if "$" not in raw:  # the overwhelmingly common case
        return raw
    n = len(raw)
    out = []
    i = 0
    braces_possible = True  # cleared once no ``}`` can remain ahead
    while i < n:
        j = raw.find("$", i)
        if j < 0:
            out.append(raw[i:])
            break
        out.append(raw[i:j])  # verbatim run up to this ``$``
        nxt = raw[j + 1] if j + 1 < n else ""
        if nxt == "$":  # ``$$`` -> a literal ``$``
            out.append("$")
            i = j + 2
            continue
        if braces_possible and nxt == "{":
            match = _ENV_NAME_RE.match(raw, j + 2)
            if match is not None:
                k = match.end()
                name = match.group()
                if raw[k : k + 2] == ":-":
                    close = raw.find("}", k + 2)
                    if close < 0:
                        # No ``}`` at or after here means none remains anywhere
                        # ahead (the cursor only advances), so no braced form
                        # can ever complete again.  Stop scanning for one; this
                        # is what keeps the pass O(n) on ``${x:-`` heavy input.
                        braces_possible = False
                    else:
                        out.append(
                            _resolve_env(
                                name, raw[k + 2 : close], path, location
                            )
                        )
                        i = close + 1
                        continue
                elif k < n and raw[k] == "}":
                    out.append(_resolve_env(name, None, path, location))
                    i = k + 1
                    continue
        # a lone ``$``, a malformed ``${...``, or braces no longer possible
        out.append("$")
        i = j + 1
    return "".join(out)


# Map "kinds" whose ``command`` / ``shell`` keys are runtime-shell territory.
_ENV_SHELL_MAP_KINDS = frozenset({"job", "task", "defaults"})
# How a sequence's kind names the kind of each of its elements.
_ENV_SEQ_ELEM_KIND = {
    "job-seq": "job",
    "dag-seq": "dag",
    "task-seq": "task",
}


def _env_child_kind(kind: str, key: str) -> str:
    """Classify the child reached from a map of ``kind`` through ``key``.

    A map's kind tells the walk, when it later processes that map's own keys,
    whether a ``command`` / ``shell`` / ``logging`` there is a structural field
    (shell or logging territory) or just a user-chosen key in a value map.  A
    ``-seq`` kind is a sequence whose elements carry the singular kind (see
    :data:`_ENV_SEQ_ELEM_KIND`).
    """
    if key == "report":  # a report block may sit under any hook map
        return "report"
    if kind == "root":
        if key == "defaults":
            return "defaults"
        if key == "jobs":
            return "job-seq"
        if key == "dags":
            return "dag-seq"
        return "other"
    if kind == "dag" and key == "tasks":
        return "task-seq"
    return "other"


def _interpolate_env(doc: Any, path: str) -> Any:
    """Return ``doc`` with env-var references expanded in every string value.

    Walks the validated document, rebuilding it; see the module comment above
    :func:`_interpolate_env_value` for the grammar and the intentionally
    skipped structural fields (which are recognised by position, not by key
    name, via :func:`_env_child_kind`).
    """

    def walk(node: Any, location: str, kind: str) -> Any:
        if isinstance(node, str):
            # leaves outnumber containers: test for them first, and answer
            # a $-free one (nearly all of them) without a call
            if "$" not in node:
                return node
            return _interpolate_env_value(node, path, location)
        if isinstance(node, dict):
            out = {}
            for key, value in node.items():
                if (
                    (kind == "report" and key == "shell")
                    or (
                        kind in _ENV_SHELL_MAP_KINDS
                        and key in ("command", "shell")
                    )
                    or (kind == "root" and key == "logging")
                ):
                    out[key] = value  # another layer's ${...}: leave verbatim
                    continue
                child = "{}.{}".format(location, key) if location else str(key)
                out[key] = walk(value, child, _env_child_kind(kind, key))
            return out
        if isinstance(node, list):
            elem = _ENV_SEQ_ELEM_KIND.get(kind, "other")
            return [
                walk(item, "{}[{}]".format(location, index), elem)
                for index, item in enumerate(node)
            ]
        return node

    return walk(doc, "", "root")


def parse_config_string(
    data: str,
    path: str,
    _seen: Optional[set] = None,
    _sources: Optional[set] = None,
) -> CronstableConfig:
    try:
        doc = strictyaml.load(data, CONFIG_SCHEMA, label=path).data
    except YAMLError as ex:
        raise ConfigError(str(ex)) from ex
    except (ValueError, AttributeError) as ex:
        # strictyaml's scalar validators raise BARE ValueError for values
        # like '' or '.' on any Float()/Int() key, and its reader raises
        # AttributeError for a control character or lone surrogate.  Config
        # parsing must only ever raise ConfigError: without this, one bad
        # file aborts a whole config-directory load with a raw traceback.
        raise ConfigError("{}: {}".format(path, ex)) from ex
    # Expand ${VAR} references over the validated doc before building the
    # config (an unset-variable ConfigError propagates as-is). Runs per file,
    # so each included file expands against its own ${VAR}s.
    doc = _interpolate_env(doc, path)
    return _config_from_doc(doc, path, _seen, _sources)


def parse_crontab_string(data: str, path: str) -> CronstableConfig:
    """Parse classic (Vixie-style) crontab text into a CronstableConfig.

    The crontab is lowered to ordinary job dictionaries
    (:func:`cronstable.crontabs.parse_crontab`) and built exactly like a
    YAML ``jobs:`` section, so every entry gets cronstable's standard
    defaults rather than an emulation of cron's environment.  A crontab can
    only define jobs; every other section stays YAML-only.
    """
    try:
        job_docs = crontabs.parse_crontab(data, path)
    except crontabs.CrontabError as ex:
        raise ConfigError(str(ex)) from ex
    return _config_from_doc({"jobs": job_docs}, path, None)


def _build_notify_config(raw: dict) -> dict[str, Any]:
    """Assemble the ``notify:`` block: an event allow-list + a report block.

    ``report`` merges over the event-shaped :data:`_NOTIFY_REPORT_DEFAULTS`
    exactly as a job's report merges over the job defaults, so an omitted
    reporter simply stays disabled.  ``events`` (optional) is an allow-list of
    :data:`NOTIFY_EVENTS`; absent or empty means every event fires.
    """
    report = mergedicts(
        copy.deepcopy(_NOTIFY_REPORT_DEFAULTS), raw.get("report") or {}
    )
    events = raw.get("events")
    return {
        "events": frozenset(events) if events else None,
        "report": report,
    }


def _build_push_config(raw: dict) -> dict[str, Any]:
    """Assemble the ``push:`` block: relay endpoint + registry storage.

    Only shape/range checks live here; the cross-section requirements
    (PyNaCl installed, a ``state:`` section or ``devicesFile`` for the
    registry, and a ``push:`` block existing wherever ``report.push``
    is enabled) run once on the fully assembled config in
    :func:`_validate_push_config`, because the sections involved may
    live in different config-directory files.
    """
    relay = raw.get("relay") or {}
    url = (relay.get("url") or "").strip()
    parsed = _safe_urlparse(url, "push.relay.url")
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        # Redacted, like every other URL echoed into a ConfigError: a
        # scheme-less `user:pass@host` lands exactly here (urlparse reads
        # the userinfo as the scheme), and this message is printed at
        # startup and logged by the reload loop on every reparse.
        raise ConfigError(
            "push.relay.url must be an http(s) URL, got {!r}".format(
                _redact_userinfo(url)
            )
        )
    # `is None`, not `or`: an explicit `timeout: 0` must reach the range
    # check below and be refused, not silently become the default.
    timeout_raw = relay.get("timeout")
    timeout = 10.0 if timeout_raw is None else float(timeout_raw)
    if timeout <= 0:
        raise ConfigError("push.relay.timeout must be > 0 seconds")
    return {
        "relay": {"url": url, "timeout": timeout},
        "devicesFile": raw.get("devicesFile") or None,
        "allowUnauthenticated": bool(raw.get("allowUnauthenticated")),
    }


def resolve_bonjour_config(
    web_config: Optional[WebConfig],
) -> Optional[dict[str, Any]]:
    """Collapse ``web.bonjour``'s bool-or-map forms to one shape.

    Returns ``None`` when the advert is off (absent, ``false``, or the
    map form with ``enabled: false``), else ``{"name": ...}`` where
    ``name`` is the operator's instance-name override or ``None`` for
    the default (the node's hostname).
    """
    if web_config is None:
        return None
    raw = web_config.get("bonjour")
    if not raw:
        return None
    if isinstance(raw, dict):
        if not raw.get("enabled", True):
            return None
        return {"name": raw.get("name")}
    return {"name": None}


def _config_from_doc(
    doc: dict,
    path: str,
    _seen: Optional[set],
    _sources: Optional[set] = None,
) -> CronstableConfig:
    """Build a CronstableConfig from an already-validated plain config doc.

    The shared back half of both front ends: ``parse_config_string``
    arrives here from strictyaml, ``parse_crontab_string`` from the
    classic-crontab lowering, and from this point on the two formats are
    indistinguishable.
    """
    inc_defaults_merged: dict = {}
    jobs = []
    dags: list[DagConfig] = []
    webconf = WebConfig(doc["web"]) if "web" in doc else None
    if webconf is not None:
        # (an included file's web section was already validated when that
        # file was parsed, so validating the inline one here covers all)
        _validate_web_config(webconf)
    clusterconf = (
        _build_cluster_config(doc["cluster"]) if "cluster" in doc else None
    )
    stateconf = _build_state_config(doc["state"]) if "state" in doc else None
    mcpconf = _build_mcp_config(doc["mcp"]) if "mcp" in doc else None
    logging_conf = LoggingConfig(doc["logging"]) if "logging" in doc else None
    notifyconf = (
        _build_notify_config(doc["notify"]) if "notify" in doc else None
    )
    pushconf = _build_push_config(doc["push"]) if "push" in doc else None
    for include in doc.get("include", ()):
        inc_path = os.path.join(os.path.dirname(path), include)
        # Included jobs arrive already fully constructed, so they carry only
        # their own file's defaults; a top-level ``defaults`` block does NOT
        # retro-apply to them. Only the included files' defaults are merged
        # here, and they affect this file's inline jobs.
        inc_config = _parse_included_file(inc_path, _seen, _sources)
        inc_defaults_merged = mergedicts(
            inc_defaults_merged, inc_config.job_defaults
        )
        jobs.extend(inc_config.jobs)
        dags.extend(inc_config.dags)
        if inc_config.web_config:
            if webconf:
                raise ConfigError("multiple web configs")
            webconf = inc_config.web_config
        if inc_config.cluster_config:
            if clusterconf:
                raise ConfigError("multiple cluster configs")
            clusterconf = inc_config.cluster_config
        if inc_config.state_config:
            if stateconf:
                raise ConfigError("multiple state configs")
            stateconf = inc_config.state_config
        if inc_config.mcp_config:
            if mcpconf:
                raise ConfigError("multiple mcp configs")
            mcpconf = inc_config.mcp_config
        if inc_config.logging_config:
            if logging_conf:
                raise ConfigError("multiple logging configs")
            logging_conf = inc_config.logging_config
        if inc_config.notify_config:
            if notifyconf:
                raise ConfigError("multiple notify configs")
            notifyconf = inc_config.notify_config
        if inc_config.push_config:
            if pushconf:
                raise ConfigError("multiple push configs")
            pushconf = inc_config.push_config
    defaults = mergedicts(DEFAULT_CONFIG, inc_defaults_merged)
    defaults = mergedicts(defaults, doc.get("defaults", {}))
    # One env_file is frequently shared by many jobs in a doc; a per-doc
    # cache reads and parses each such file once instead of once per job.
    env_cache: dict[str, dict[str, str]] = {}
    # Likewise for the advisory schedule lint, which a fleet's jobs repeat far
    # more often than they repeat env_files (see JobConfig._lint_schedule).
    lint_cache: LintCache = {}
    for config_job in doc.get("jobs", []):
        job_dict = mergedicts(defaults, config_job)
        jobs.append(
            JobConfig(job_dict, env_cache=env_cache, lint_cache=lint_cache)
        )
    # A DAG task inherits the same `defaults:` base a regular job does; the
    # DAG-node fields are graph shape and never touched by `defaults:`.  The
    # synthetic schedule-trigger job stays on DEFAULT_CONFIG (see DagConfig)
    # so a global reporter does not fire on every DAG tick.
    for config_dag in doc.get("dags", []):
        dags.append(DagConfig(config_dag, defaults))
    return CronstableConfig(
        jobs=jobs,
        web_config=webconf,
        job_defaults=JobDefaults(defaults),
        logging_config=logging_conf,
        cluster_config=clusterconf,
        state_config=stateconf,
        dags=dags,
        mcp_config=mcpconf,
        notify_config=notifyconf,
        push_config=pushconf,
    )


#: Extensions the YAML front end owns.  A file with one of these names is
#: always YAML, never content-sniffed.
_YAML_EXTENSIONS = frozenset({".yml", ".yaml"})


def _is_crontab_config(path: str, data: str) -> bool:
    """Decide which front end parses ``path``: classic crontab or YAML.

    The file NAME decides whenever it can; content sniffing is a last
    resort for a neutral name and is conservative: only a first line no
    YAML config could open with reads as a crontab
    (see :func:`cronstable.crontabs.looks_like_crontab`).
    """
    if crontabs.is_crontab_path(path):
        return True
    if os.path.splitext(path)[1].lower() in _YAML_EXTENSIONS:
        return False
    return crontabs.looks_like_crontab(data)


def parse_config_file(
    path: str,
    _seen: Optional[set] = None,
    _sources: Optional[set] = None,
) -> CronstableConfig:
    # Guard against include cycles with a clear ConfigError instead of
    # RecursionError. _seen is scoped per top-level parse, so two
    # independent files including a common file is not flagged.
    abspath = os.path.abspath(path)
    # _sources accumulates every on-disk file the parse reads so a caller
    # can stat them and skip an unchanged reparse; deliberately NOT _seen
    # (per-file cycle scope), so a shared include is recorded without being
    # mistaken for a cycle.
    if _sources is not None:
        _sources.add(abspath)
    if _seen is None:
        _seen = set()
    if abspath in _seen:
        raise ConfigError("include cycle detected at {}".format(path))
    _seen.add(abspath)
    with open(path, "rt", encoding="utf-8") as stream:
        data = stream.read()
    if _is_crontab_config(path, data):
        return parse_crontab_string(data, path)
    return parse_config_string(data, path, _seen, _sources)


def _validate_cross_sections(config: CronstableConfig) -> None:
    """Validate constraints spanning jobs and the optional sections.

    Runs only at the top-level parse entry point (:func:`parse_config`),
    on the fully-assembled config -- never inside :func:`_config_from_doc`,
    where an included or config-dir sibling file is parsed standalone and
    the section a job depends on may legitimately live in another file.
    """
    # The scheduler indexes jobs by name, so of two same-named jobs only the
    # later definition would ever run, silently. The usual source is the
    # same name in two config-dir/included files, or two crontab files
    # sharing a basename (their '<basename>:<line>' names then collide).
    dup_jobs = sorted(
        name
        for name, count in Counter(job.name for job in config.jobs).items()
        if count > 1
    )
    if dup_jobs:
        raise ConfigError(
            "duplicate job name(s): {}; the scheduler tracks jobs by name, "
            "so all but the last definition of a name would silently never "
            "run. Rename the duplicates.".format(", ".join(dup_jobs))
        )
    if config.state_config is None:
        offenders = sorted(
            job.name
            for job in config.jobs
            if job.concurrencyScope == "cluster"
        )
        if offenders:
            raise ConfigError(
                "concurrencyScope: cluster requires a `state` section "
                "(the shared store is what coordinates the nodes), but "
                "none is configured; offending job(s): {}".format(
                    ", ".join(offenders)
                )
            )
        secret_offenders = sorted(
            job.name for job in config.jobs if job.secrets
        )
        if secret_offenders:
            raise ConfigError(
                "job `secrets` are staged over the state loopback endpoint, "
                "which requires a `state` section; none is configured, "
                "offending job(s): {}".format(", ".join(secret_offenders))
            )
    elif config.jobs:
        job_api = config.state_config.get("jobApi") or {}
        if not job_api.get("enabled", True):
            secret_offenders = sorted(
                job.name for job in config.jobs if job.secrets
            )
            if secret_offenders:
                raise ConfigError(
                    "job `secrets` need the state loopback endpoint, but "
                    "state.jobApi.enabled is false; offending job(s): "
                    "{}".format(", ".join(secret_offenders))
                )
    _validate_dags(config)
    _validate_mcp_config(config)
    _validate_push_config(config)
    _validate_eventlog_config(config)


def _validate_dags(config: CronstableConfig) -> None:
    """Cross-section invariants for the orchestration DAGs.

    DAGs live entirely on the durable store (each ``dag_run`` is a document,
    per-task state and XCom ride the durable store), and their tasks reach
    the store through the loopback endpoint, so a DAG needs a ``state`` section
    with ``jobApi`` enabled.  DAG names must be unique across the whole config.
    The per-DAG graph (acyclic, resolvable deps, valid expand targets) is
    already validated when each :class:`DagConfig` is built.
    """
    if not config.dags:
        return
    names = [d.name for d in config.dags]
    dups = sorted(n for n, c in Counter(names).items() if c > 1)
    if dups:
        raise ConfigError("duplicate dag name(s): {}".format(", ".join(dups)))
    # Each task's launch template is named '<dag>.<taskId>' and shares the
    # scheduler's per-name bookkeeping with regular jobs, so a name
    # collision entangles unrelated runs. Task ids may contain '.', so two
    # dags can also mint the same template name (dag 'a' task 'b.c' vs dag
    # 'a.b' task 'c'). Reject both at load. A job named after a bare dag is
    # fine: the synthetic schedule job carries a 'dag:' prefix and is never
    # launched.
    job_names = {job.name for job in config.jobs}
    template_owner: dict[str, tuple[str, str]] = {}
    for d in config.dags:
        for task in d.tasks:
            template = task.job_template.name
            if template in job_names:
                raise ConfigError(
                    "job {!r} collides with dag {!r} task {!r}: dag tasks "
                    "launch under the template name '<dag>.<taskId>' and "
                    "would share that job's concurrency bookkeeping; "
                    "rename the job or the task".format(
                        template, d.name, task.id
                    )
                )
            owner = template_owner.get(template)
            if owner is not None:
                raise ConfigError(
                    "dag {!r} task {!r} and dag {!r} task {!r} both launch "
                    "under the template name {!r} (task ids may contain "
                    "'.', so distinct dag/task pairs can collide); rename "
                    "one so their runs are not entangled".format(
                        owner[0], owner[1], d.name, task.id, template
                    )
                )
            template_owner[template] = (d.name, task.id)
    if config.state_config is None:
        raise ConfigError(
            "dags require a `state` section (each dag_run and its per-task "
            "state live on the durable store); none is configured, "
            "offending dag(s): {}".format(", ".join(sorted(names)))
        )
    job_api = config.state_config.get("jobApi") or {}
    if not job_api.get("enabled", True):
        raise ConfigError(
            "dags need the state loopback endpoint for XCom and task state, "
            "but state.jobApi.enabled is false; offending dag(s): "
            "{}".format(", ".join(sorted(names)))
        )


def parse_config(
    config_arg: str, _sources: Optional[set] = None
) -> CronstableConfig:
    if os.path.isdir(config_arg):
        config = _parse_config_dir(config_arg, _sources)
    else:
        try:
            config = parse_config_file(config_arg, _sources=_sources)
        except OSError as ex:
            # surface a clean ConfigError (e.g. file not found) rather than a
            # bare OSError, so callers (__main__) handle it uniformly.
            raise ConfigError(str(ex)) from ex
    _validate_cross_sections(config)
    return config


def parse_config_with_sources(
    config_arg: str,
) -> tuple[CronstableConfig, frozenset[str]]:
    """Parse ``config_arg`` and report the on-disk files the parse read.

    ``sources`` is the absolute path of every YAML/crontab file consulted
    (including transitive ``include``s) and every job's and DAG task's
    ``env_file``.  The scheduler stats this exact set to skip the reparse on
    an unchanged config; because it covers includes and env_files, an edit
    to any file that actually feeds the config is still noticed.
    """
    sources: set = set()
    config = parse_config(config_arg, sources)
    for job in config.jobs:
        if job.env_file is not None:
            sources.add(os.path.abspath(job.env_file))
    # DAG task templates read their env_file at parse time exactly like jobs
    # do, so an edit to one must bust the reparse-skip signature the same way.
    for dag_cfg in config.dags:
        for template in dag_cfg.task_templates.values():
            if template.env_file is not None:
                sources.add(os.path.abspath(template.env_file))
    return config, frozenset(sources)


class _CachedDirFile(NamedTuple):
    """A remembered per-file parse for the per-file parse cache.

    ``sources`` is every on-disk file the parse read; ``sig`` the sorted
    ``(abspath, content_digest)`` fingerprint of exactly those; ``config``
    the parsed result to reuse while they stay byte-for-byte current.
    ``included`` is the narrower set the include-cycle guard reasons about
    (config files only, no env_files): serving a cached parse must leave the
    caller's cycle scope holding exactly what a real parse would have added.
    """

    sources: frozenset[str]
    included: frozenset[str]
    sig: tuple[tuple[str, Optional[str]], ...]
    config: CronstableConfig


#: Per-file parse cache, keyed by absolute path and validated by hashing
#: each source's CONTENT: an edit to one file of a config directory or
#: include tree re-runs strictyaml only for that file.  A cache hit is
#: byte-exact with a full reparse, never merely mtime-close.  Bounded LRU;
#: a stale or evicted entry only costs a reparse, never correctness.
_DIR_FILE_CACHE: "OrderedDict[str, _CachedDirFile]" = OrderedDict()
_DIR_FILE_CACHE_MAX = 1024


def _dir_file_content_sig(
    sources: frozenset[str],
) -> tuple[tuple[str, Optional[str]], ...]:
    """Sorted ``(abspath, content_digest)`` fingerprint of a parse's inputs.

    Hashes each source's bytes rather than trusting a ``(mtime_ns, size)``
    stat, so a size- and mtime-preserving edit (coarse filesystems,
    ``rsync -a``) still invalidates the cache; the strictyaml parse is what
    the cache skips, not the read.  A vanished or unreadable source hashes
    to ``None`` so a deletion still reads as a change.
    """
    parts: list[tuple[str, Optional[str]]] = []
    for src in sorted(sources):
        try:
            with open(src, "rb") as handle:
                digest: Optional[str] = hashlib.blake2b(
                    handle.read(), digest_size=16
                ).hexdigest()
        except OSError:
            digest = None
        parts.append((src, digest))
    return tuple(parts)


def _parse_file_cached(
    path: str, _seen: Optional[set] = None
) -> tuple[CronstableConfig, frozenset[str], frozenset[str]]:
    """Parse one config file, reusing an unchanged prior parse.

    Returns ``(config, sources, included)``: every file the parse depended
    on, and the config files alone for the caller's include-cycle scope.
    ConfigError/OSError propagate unchanged (and nothing is cached).

    ``_seen`` is the caller's cycle scope: passed into a real parse, folded
    from the remembered ``included`` set on a cache hit, and a hit is
    declined outright when the two overlap, because that overlap IS the
    cycle and the uncached path words the error.
    """
    abspath = os.path.abspath(path)
    cached = _DIR_FILE_CACHE.get(abspath)
    if (
        cached is not None
        and (_seen is None or _seen.isdisjoint(cached.included))
        and _dir_file_content_sig(cached.sources) == cached.sig
    ):
        _DIR_FILE_CACHE.move_to_end(abspath)
        if _seen is not None:
            _seen.update(cached.included)
        return cached.config, cached.sources, cached.included
    file_sources: set = set()
    seen: set = set() if _seen is None else _seen
    before = frozenset(seen)
    config = parse_config_file(path, seen, file_sources)
    file_included = frozenset(seen) - before
    # env_files are read at parse time, so a change to one must invalidate
    # the cached parse too: fold them into the fingerprint.
    for job in config.jobs:
        if job.env_file is not None:
            file_sources.add(os.path.abspath(job.env_file))
    for dag_cfg in config.dags:
        for template in dag_cfg.task_templates.values():
            if template.env_file is not None:
                file_sources.add(os.path.abspath(template.env_file))
    frozen = frozenset(file_sources)
    _DIR_FILE_CACHE[abspath] = _CachedDirFile(
        frozen, file_included, _dir_file_content_sig(frozen), config
    )
    _DIR_FILE_CACHE.move_to_end(abspath)
    while len(_DIR_FILE_CACHE) > _DIR_FILE_CACHE_MAX:
        _DIR_FILE_CACHE.popitem(last=False)
    return config, frozen, file_included


def _parse_included_file(
    path: str, _seen: Optional[set], _sources: Optional[set]
) -> CronstableConfig:
    """Parse one ``include:`` target, reusing an unchanged prior parse.

    Routed through the same content-hash cache the config-directory loader
    uses, so editing one file of an include tree reparses only that file.
    """
    config, sources, _ = _parse_file_cached(path, _seen)
    if _sources is not None:
        _sources.update(sources)
    return config


def _claim_config_dir_section(
    kind: str,
    new_value: Any,
    current: Any,
    current_source: Optional[str],
    path: str,
) -> tuple[Any, Optional[str]]:
    """Adopt one at-most-once section from a config-dir file.

    web/cluster/state/mcp/logging/notify/push may each appear in at most
    one file of a config directory; a second appearance is refused with
    an error naming both files.
    """
    if new_value is None:
        return current, current_source
    if current is not None:
        raise ConfigError(
            "Multiple '{}' configurations found: "
            "first in {}, now in {}".format(kind, current_source, path)
        )
    return new_value, path


def _parse_config_dir(
    config_arg: str, _sources: Optional[set] = None
) -> CronstableConfig:
    jobs: list[JobConfig] = []
    dags: list[DagConfig] = []
    config_errors: dict[str, str] = {}
    web_config: Optional[WebConfig] = None
    web_config_source_fname: Optional[str] = None
    cluster_config: Optional[ClusterConfig] = None
    cluster_config_source_fname: Optional[str] = None
    state_config: Optional[StateConfig] = None
    state_config_source_fname: Optional[str] = None
    mcp_config: Optional[MCPConfig] = None
    mcp_config_source_fname: Optional[str] = None
    logging_config: Optional[LoggingConfig] = None
    logging_config_source_fname: Optional[str] = None
    notify_config: Optional[dict[str, Any]] = None
    notify_config_source_fname: Optional[str] = None
    push_config: Optional[dict[str, Any]] = None
    push_config_source_fname: Optional[str] = None
    job_defaults: JobDefaults = JobDefaults({})
    # Sort by name so job order and the "first config found" error messages
    # are deterministic; os.scandir yields entries in arbitrary FS order.
    for direntry in sorted(os.scandir(config_arg), key=lambda e: e.name):
        base, ext = os.path.splitext(direntry.name)
        if base[0] in {"_", "."}:
            continue
        # YAML by extension, or a classic crontab by filename marker
        # (.crontab / .cron / a file named "crontab"); anything else is
        # skipped, so a stray README or data file never becomes jobs.
        #
        # Case-folded, like every other place a config name is judged
        # (_is_crontab_config, crontabs.is_crontab_path, and
        # platform._holds_config, which picks the Windows default config
        # directory).  Windows filesystems preserve case without
        # distinguishing it, so a JOBS.YAML written by an editor that
        # upper-cases the suffix is the same file to the user, and a
        # case-sensitive test here would skip it in silence: a directory
        # of such files parses to zero jobs, and no error path reports
        # that.  It would also split this loop from the same file named
        # directly, which case-folds before it picks a front end.
        is_yaml = ext.lower() in _YAML_EXTENSIONS
        if not is_yaml and not crontabs.is_crontab_path(direntry.name):
            continue
        try:
            config, file_sources, _ = _parse_file_cached(direntry.path)
        except ConfigError as err:
            config_errors[direntry.path] = str(err)
            continue
        except OSError as ex:
            config_errors[config_arg] = str(ex)
            continue
        if _sources is not None:
            _sources.update(file_sources)
        jobs.extend(config.jobs)
        dags.extend(config.dags)
        web_config, web_config_source_fname = _claim_config_dir_section(
            "web",
            config.web_config,
            web_config,
            web_config_source_fname,
            direntry.path,
        )
        cluster_config, cluster_config_source_fname = (
            _claim_config_dir_section(
                "cluster",
                config.cluster_config,
                cluster_config,
                cluster_config_source_fname,
                direntry.path,
            )
        )
        state_config, state_config_source_fname = _claim_config_dir_section(
            "state",
            config.state_config,
            state_config,
            state_config_source_fname,
            direntry.path,
        )
        mcp_config, mcp_config_source_fname = _claim_config_dir_section(
            "mcp",
            config.mcp_config,
            mcp_config,
            mcp_config_source_fname,
            direntry.path,
        )
        logging_config, logging_config_source_fname = (
            _claim_config_dir_section(
                "logging",
                config.logging_config,
                logging_config,
                logging_config_source_fname,
                direntry.path,
            )
        )
        notify_config, notify_config_source_fname = _claim_config_dir_section(
            "notify",
            config.notify_config,
            notify_config,
            notify_config_source_fname,
            direntry.path,
        )
        push_config, push_config_source_fname = _claim_config_dir_section(
            "push",
            config.push_config,
            push_config,
            push_config_source_fname,
            direntry.path,
        )
        job_defaults = JobDefaults(
            mergedicts(job_defaults, config.job_defaults)
        )
    if config_errors:
        raise ConfigError("\n---".join(config_errors.values()))
    # Build the result from the accumulated values (never the last file's
    # config), and return an empty config for an empty/all-skipped directory
    # instead of raising UnboundLocalError.
    return CronstableConfig(
        jobs=jobs,
        web_config=web_config,
        job_defaults=job_defaults,
        logging_config=logging_config,
        cluster_config=cluster_config,
        state_config=state_config,
        dags=dags,
        mcp_config=mcp_config,
        notify_config=notify_config,
        push_config=push_config,
    )
