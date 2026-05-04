#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""Phase 1 unit tests for :class:`IndexDispatcher`.

Covers the four FailurePolicy branches (FATAL / OUTBOX / WARN / IGNORE)
on a stub :class:`Indexer`, plus single-op vs bulk fan-out.  The outbox
writer is None — Phase 2 will add coverage for the durable enqueue path.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, Indexer, IndexContext, IndexOp,
)
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher, IndexerFatal,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry, WriteMode,
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _StubIndexer:
    """Records calls; can be told to raise on a specific op_type."""

    def __init__(self, indexer_id: str, *, raise_on: Optional[str] = None) -> None:
        self.indexer_id = indexer_id
        self.raise_on = raise_on
        self.calls: List[IndexOp] = []
        self.bulk_calls: List[Sequence[IndexOp]] = []

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:
        self.calls.append(op)
        if self.raise_on and op.op_type == self.raise_on:
            raise RuntimeError(f"stub failure on {op.op_type}")

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.bulk_calls.append(ops)
        if self.raise_on:
            raise RuntimeError(f"stub bulk failure ({self.raise_on})")
        return BulkResult(total=len(ops), succeeded=len(ops))


class _StubRouting:
    def __init__(self, entries: List[OperationDriverEntry]) -> None:
        self.operations = {Operation.INDEX: entries}


def _make_dispatcher(
    entries: List[OperationDriverEntry],
    indexers: dict,
) -> IndexDispatcher:
    routing = _StubRouting(entries)

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return indexers.get(indexer_id)

    return IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
    )


def _entry(driver_id: str, *, on_failure: FailurePolicy) -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_id=driver_id,
        on_failure=on_failure,
        write_mode=WriteMode.SYNC,
        source="auto",
    )


def _ctx() -> IndexContext:
    return IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")


def _op(op_type: str = "upsert", entity_id: str = "item-1") -> IndexOp:
    return IndexOp(
        op_type=op_type,
        entity_type="item",
        entity_id=entity_id,
        payload={"foo": "bar"} if op_type == "upsert" else None,
    )


# ---------------------------------------------------------------------------
# Happy path — no failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_calls_every_configured_indexer():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    await dispatcher.fan_out(_ctx(), _op())
    assert len(a.calls) == 1
    assert len(b.calls) == 1


@pytest.mark.asyncio
async def test_fan_out_skips_when_indexer_not_registered():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("missing", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a},  # 'missing' not registered
    )
    await dispatcher.fan_out(_ctx(), _op())
    assert len(a.calls) == 1


# ---------------------------------------------------------------------------
# Failure policies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fatal_policy_raises_indexer_fatal():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.FATAL)],
        indexers={"a": a},
    )
    with pytest.raises(IndexerFatal) as exc_info:
        await dispatcher.fan_out(_ctx(), _op())
    assert exc_info.value.indexer_id == "a"
    assert exc_info.value.op.op_type == "upsert"


@pytest.mark.asyncio
async def test_warn_policy_swallows_failure_and_continues_to_next():
    a = _StubIndexer("a", raise_on="upsert")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    await dispatcher.fan_out(_ctx(), _op())  # must not raise
    assert len(a.calls) == 1
    assert len(b.calls) == 1


@pytest.mark.asyncio
async def test_ignore_policy_silent():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.IGNORE)],
        indexers={"a": a},
    )
    await dispatcher.fan_out(_ctx(), _op())  # must not raise


@pytest.mark.asyncio
async def test_outbox_policy_without_writer_degrades_to_warn(caplog):
    """Phase 1: no OutboxWriter wired → OUTBOX policy degrades to WARN.

    Phase 2 will replace this assertion: with an outbox, the row should
    be enqueued in the same TX.
    """
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.OUTBOX)],
        indexers={"a": a},
    )
    import logging as _logging
    with caplog.at_level(_logging.WARNING):
        await dispatcher.fan_out(_ctx(), _op())
    # Two warnings expected: one-time degrade-notice + the actual failure.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("on_failure=outbox" in m and "Phase 2" in m for m in msgs)


# ---------------------------------------------------------------------------
# Bulk fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_bulk_returns_per_indexer_results():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)
    assert results["a"].succeeded == 2
    assert results["b"].succeeded == 2


@pytest.mark.asyncio
async def test_fan_out_bulk_failure_with_warn_returns_failure_summary():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)
    assert results["a"].failed == 2
    assert results["a"].failures
