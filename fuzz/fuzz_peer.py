"""Fuzz the validators for data from cluster peers and the state backend.

Peers are CA-vouched but otherwise untrusted, other nodes write the state
backend, and phones send pairing requests. Each input is one JSON document,
decoded the way the daemon decodes a /peer body. The validators must accept
any value without raising. What the gossip parsers keep must stay within
its documented caps and encode without NaN or Infinity, which the
dashboard's JSON.parse rejects. Run records must survive a round trip
through their dictionary form.
"""

import json
import logging
import sys
from typing import Any

import atheris

with atheris.instrument_imports(include=["cronstable"]):
    from cronstable import _json, cluster, push
    from cronstable.leadership import decode_reboot_ran
    from cronstable.limits import LimitsRecord
    from cronstable.resources import ResourceUsage

# The /peer fields _observe_peer reads as name sets, with their caps.
NAME_SETS = (
    (
        "ran_reboot_jobs",
        cluster.MAX_REBOOT_JOB_NAME_LEN,
        cluster.MAX_ADVERTISED_REBOOT_JOBS,
    ),
    (
        "mutual_agreeing",
        cluster.MAX_PEER_FIELD_LEN,
        cluster.MAX_MEMBER_ENTRIES,
    ),
    (
        "quorate_vouched",
        cluster.MAX_PEER_FIELD_LEN,
        cluster.MAX_MEMBER_ENTRIES,
    ),
)


def check_json(value: Any) -> None:
    json.dumps(value, allow_nan=False)


def check_names(names: Any, max_len: int, max_items: int) -> None:
    assert len(names) <= max_items
    for name in names:
        assert isinstance(name, str)
        assert name and len(name) <= max_len and name.isprintable()


def check_gossip(doc: dict[str, Any]) -> None:
    members = cluster._parse_members(
        doc.get("members"),
        max_len=cluster.MAX_PEER_FIELD_LEN,
        max_items=cluster.MAX_MEMBER_ENTRIES,
    )
    assert len(members) <= cluster.MAX_MEMBER_ENTRIES
    for name, instance, agreed in members:
        check_names((name, instance), cluster.MAX_PEER_FIELD_LEN, 2)
        assert isinstance(agreed, bool)
    for key, max_len, max_items in NAME_SETS:
        names = cluster._parse_str_list(
            doc.get(key), max_len=max_len, max_items=max_items
        )
        check_names(names, max_len, max_items)
    summaries = cluster._parse_job_summaries(doc.get("job_summaries"))
    if summaries is not None:
        check_names(
            summaries,
            cluster.MAX_JOB_SUMMARY_NAME_LEN,
            cluster.MAX_ADVERTISED_JOB_SUMMARIES,
        )
        check_json(summaries)
    stats = cluster._parse_node_stats(doc.get("node_stats"))
    if stats is not None:
        check_json(stats)


def check_records(doc: Any) -> None:
    limits = LimitsRecord.from_dict(doc)
    if limits is not None:
        data = limits.to_dict()
        check_json(data)
        assert LimitsRecord.from_dict(data) == limits
    usage = ResourceUsage.from_dict(doc)
    if usage is not None:
        data = usage.to_dict(include_series=True)
        check_json(data)
        assert ResourceUsage.from_dict(data) == usage


def TestOneInput(data: bytes) -> None:
    # Two values travel as raw text: the node-stats header and the
    # @reboot-ran blob a backend stores.
    text = data.decode("utf-8", "replace")
    stats = cluster._parse_node_stats_header(text)
    if stats is not None:
        check_json(stats)
    decode_reboot_ran(text)
    try:
        doc = _json.loads(data)
    except (ValueError, RecursionError):
        return
    if isinstance(doc, dict):
        check_gossip(doc)
    check_records(doc)
    try:
        push.validate_pairing(doc)
    except push.PairingError:
        pass


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
