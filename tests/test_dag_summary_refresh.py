"""Concurrent refreshes of the DAG run summary cache."""

import asyncio

import pytest

from cronstable import dag, dagrun
from tests.test_state_dag_run import _XC_YAML, _mint_run


@pytest.mark.parametrize("expired", [False, True])
async def test_concurrent_requests_share_one_refresh(
    monkeypatch, dag_cron, expired
):
    monkeypatch.setattr(dagrun, "DAG_SUMMARY_LIST_TTL", 3600.0)
    cron = await dag_cron(_XC_YAML)
    await _mint_run(cron, "r1")
    scheduler = cron._dag
    backend = cron.state_backend
    if expired:
        await scheduler._run_summaries(backend, "xc")
        stamp, summaries = scheduler._summaries_memo["xc"]
        scheduler._summaries_memo["xc"] = (stamp - 7200, summaries)

    entered = asyncio.Event()
    release = asyncio.Event()
    listings = []
    real_keys = backend.list_document_keys

    async def gated_keys(ns):
        listings.append(ns)
        entered.set()
        await release.wait()
        return await real_keys(ns)

    monkeypatch.setattr(backend, "list_document_keys", gated_keys)
    requests = [
        asyncio.create_task(scheduler._run_summaries(backend, "xc"))
        for _ in range(10)
    ]
    try:
        await asyncio.wait_for(entered.wait(), 5)
    finally:
        release.set()
    results = await asyncio.wait_for(asyncio.gather(*requests), 5)
    assert listings == [scheduler._ns("xc")]
    assert all(result == results[0] for result in results)
    assert all(result is not results[0] for result in results[1:])
    results[0].clear()
    warm = await scheduler._run_summaries(backend, "xc")
    assert [s["runKey"] for s in warm] == ["r1"]
    assert len(listings) == 1
    assert not scheduler._summaries_inflight


@pytest.mark.parametrize("cancel_index", [0, 1])
async def test_client_cancellation_keeps_shared_refresh_running(
    monkeypatch, dag_cron, cancel_index
):
    cron = await dag_cron(_XC_YAML)
    await _mint_run(cron, "r1")
    scheduler = cron._dag
    backend = cron.state_backend
    entered = asyncio.Event()
    release = asyncio.Event()
    listings = []
    real_keys = backend.list_document_keys

    async def gated_keys(ns):
        listings.append(ns)
        entered.set()
        await release.wait()
        return await real_keys(ns)

    monkeypatch.setattr(backend, "list_document_keys", gated_keys)
    requests = [
        asyncio.create_task(scheduler._run_summaries(backend, "xc"))
        for _ in range(2)
    ]
    try:
        await asyncio.wait_for(entered.wait(), 5)
        requests[cancel_index].cancel()
        with pytest.raises(asyncio.CancelledError):
            await requests[cancel_index]
    finally:
        release.set()
    result = await asyncio.wait_for(requests[1 - cancel_index], 5)
    assert [s["runKey"] for s in result] == ["r1"]
    assert len(listings) == 1
    assert not scheduler._summaries_inflight


async def test_failed_refresh_is_shared_but_not_cached(monkeypatch, dag_cron):
    cron = await dag_cron(_XC_YAML)
    scheduler = cron._dag
    backend = cron.state_backend
    entered = asyncio.Event()
    release = asyncio.Event()
    listings = []
    real_keys = backend.list_document_keys

    async def failing_keys(ns):
        listings.append(ns)
        entered.set()
        await release.wait()
        raise OSError("store unavailable")

    monkeypatch.setattr(backend, "list_document_keys", failing_keys)
    requests = [
        asyncio.create_task(scheduler._run_summaries(backend, "xc"))
        for _ in range(10)
    ]
    try:
        await asyncio.wait_for(entered.wait(), 5)
    finally:
        release.set()
    assert await asyncio.wait_for(asyncio.gather(*requests), 5) == [None] * 10
    assert len(listings) == 1
    assert "xc" not in scheduler._summaries_memo
    assert not scheduler._summaries_inflight
    monkeypatch.setattr(backend, "list_document_keys", real_keys)
    assert await scheduler._run_summaries(backend, "xc") == []
    assert "xc" in scheduler._summaries_memo


@pytest.mark.parametrize("bulk", [False, True])
async def test_new_generation_refresh_keeps_older_reads_out_of_caches(
    monkeypatch, dag_cron, bulk
):
    monkeypatch.setattr(dagrun, "DAG_SUMMARY_LIST_TTL", 3600.0)
    if bulk:
        monkeypatch.setattr(dagrun, "DAG_ROLLUP_BULK_THRESHOLD", 0)
    cron = await dag_cron(_XC_YAML)
    await _mint_run(cron, "r1")
    scheduler = cron._dag
    backend = cron.state_backend

    def finish(body):
        body["state"] = dag.SUCCESS
        return body, None

    await scheduler._mutate("xc", "r1", finish)
    entered = asyncio.Event()
    release = asyncio.Event()
    read_op = "list_documents" if bulk else "read_document"
    real_read = getattr(backend, read_op)

    async def gated_read(*args):
        body = await real_read(*args)
        if not entered.is_set():
            entered.set()
            await release.wait()
        return body

    monkeypatch.setattr(backend, read_op, gated_read)
    older = asyncio.create_task(scheduler._run_summaries(backend, "xc"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await scheduler._delete_run(backend, "xc", "r1", None)
        await _mint_run(cron, "r1")
        fresh = await asyncio.wait_for(
            scheduler._run_summaries(backend, "xc"), 5
        )
        assert [s["state"] for s in fresh] == [dag.RUNNING]
    finally:
        release.set()
    stale = await asyncio.wait_for(older, 5)
    assert [s["state"] for s in stale] == [dag.SUCCESS]
    assert scheduler._summaries_memo["xc"][1] == fresh
    assert scheduler._dag_summary_cache["xc"]["r1"]["state"] == dag.RUNNING
    scheduler._summaries_memo.clear()
    assert await scheduler._run_summaries(backend, "xc") == fresh
    assert not scheduler._summaries_inflight


@pytest.mark.parametrize("stop", ["forget", "shutdown"])
async def test_stopping_scheduler_cancels_summary_refreshes(
    monkeypatch, dag_cron, stop
):
    cron = await dag_cron(_XC_YAML)
    scheduler = cron._dag
    backend = cron.state_backend
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def hanging_keys(ns):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(backend, "list_document_keys", hanging_keys)
    request = asyncio.create_task(scheduler._run_summaries(backend, "xc"))
    await asyncio.wait_for(entered.wait(), 5)
    if stop == "forget":
        scheduler.forget()
    else:
        await scheduler.shutdown()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, 5)
    assert cancelled.is_set()
    assert not scheduler._summaries_inflight
    assert "xc" not in scheduler._summaries_memo
