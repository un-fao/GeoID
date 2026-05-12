"""transform_runtime — chain composition semantics.

apply_transform_chain runs left-to-right; restore_transform_chain runs
right-to-left so the output shape matches the original entity. Empty chain
is identity. Exceptions propagate after a warning log.

Also exercises the per-(operation, driver) attachment introduced by #501:
input_transformers on INDEX entries, output_transformers on SEARCH entries,
validator rejection of dangling refs, deferred-hop WARN logs, and
per-item failure isolation inside IndexDispatcher.fan_out_bulk.
"""

from __future__ import annotations

import logging
from typing import Any, List
from unittest.mock import patch

import pytest

from dynastore.modules.storage.transform_runtime import (
    apply_transform_chain,
    restore_transform_chain,
)


class _AppendTransformer:
    """Test transformer that appends `tag` on apply, strips on restore."""

    def __init__(self, transform_id: str, tag: str):
        self.transform_id = transform_id
        self.tag = tag

    async def transform_for_index(self, entity, *, catalog_id, collection_id, entity_kind):
        return f"{entity}|{self.tag}"

    async def restore_from_index(self, doc, *, catalog_id, collection_id, entity_kind):
        suffix = f"|{self.tag}"
        assert doc.endswith(suffix), f"restore expected suffix {suffix}, got {doc!r}"
        return doc[: -len(suffix)]


class _RaisingTransformer:
    transform_id = "boom"

    async def transform_for_index(self, entity, **_):
        raise RuntimeError("apply boom")

    async def restore_from_index(self, doc, **_):
        raise RuntimeError("restore boom")


@pytest.mark.asyncio
async def test_apply_chain_runs_left_to_right():
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    result = await apply_transform_chain(
        "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == "raw|A|B"


@pytest.mark.asyncio
async def test_restore_chain_runs_right_to_left():
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    # Restore B first (matches its tag suffix), then A
    result = await restore_transform_chain(
        "raw|A|B", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == "raw"


@pytest.mark.asyncio
async def test_apply_empty_chain_is_identity():
    result = await apply_transform_chain(
        {"x": 1}, [], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == {"x": 1}


@pytest.mark.asyncio
async def test_restore_empty_chain_is_identity():
    result = await restore_transform_chain(
        {"x": 1}, [], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert result == {"x": 1}


@pytest.mark.asyncio
async def test_apply_propagates_exceptions():
    chain: List[Any] = [_RaisingTransformer()]
    with pytest.raises(RuntimeError, match="apply boom"):
        await apply_transform_chain(
            "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
        )


@pytest.mark.asyncio
async def test_restore_propagates_exceptions():
    chain: List[Any] = [_RaisingTransformer()]
    with pytest.raises(RuntimeError, match="restore boom"):
        await restore_transform_chain(
            "doc", chain, catalog_id="c", collection_id=None, entity_kind="item",
        )


@pytest.mark.asyncio
async def test_apply_then_restore_round_trips():
    """Composition guarantee: restore inverts apply for the same chain."""
    chain = [_AppendTransformer("a", "A"), _AppendTransformer("b", "B")]
    transformed = await apply_transform_chain(
        "raw", chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    restored = await restore_transform_chain(
        transformed, chain, catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert restored == "raw"


# ---------------------------------------------------------------------------
# #501 — per-(operation, driver) transformer attachment
# ---------------------------------------------------------------------------


class PayloadAppendTransformer:  # noqa: D401 — test fixture
    """Variant of ``_AppendTransformer`` that works on dict payloads.

    Mutates ``payload["tag"]`` so the dispatcher INDEX hop tests can
    verify the per-item payload reshape end-to-end.
    """

    def __init__(self, tag: str) -> None:
        self.tag = tag

    async def transform_for_index(self, entity, **_):
        out = dict(entity)
        existing = out.get("tag", "")
        out["tag"] = f"{existing}|{self.tag}" if existing else self.tag
        return out

    async def restore_from_index(self, doc, **_):
        out = dict(doc)
        current = out.get("tag", "")
        suffix = f"|{self.tag}"
        if current.endswith(suffix):
            out["tag"] = current[: -len(suffix)]
        elif current == self.tag:
            out.pop("tag")
        return out


class _Append:
    """Distinct class for the dangling-ref / chain composition tests."""

    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


@pytest.mark.asyncio
async def test_input_on_index_only_runs_at_index_hop_not_at_search():
    """When only INDEX declares input_transformers, the indexed doc is
    transformed; the SEARCH-side restore does not fire (no
    output_transformers declared)."""
    from dynastore.modules.storage.routing_config import (
        Operation,
        OperationDriverEntry,
        get_output_transformers_for_search,
    )

    index_entry = OperationDriverEntry(
        driver_ref="items_indexer",
        input_transformers=("payload_append_transformer",),
    )
    search_entry = OperationDriverEntry(driver_ref="items_indexer")
    ops = {
        Operation.INDEX: [index_entry],
        Operation.SEARCH: [search_entry],
        Operation.TRANSFORM: [
            OperationDriverEntry(driver_ref="payload_append_transformer"),
        ],
    }
    assert index_entry.input_transformers == ("payload_append_transformer",)
    assert search_entry.output_transformers == ()

    appender = PayloadAppendTransformer("X")
    # SEARCH entry has no output_transformers → empty chain at SEARCH hop.
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[appender],
    ):
        chain = await get_output_transformers_for_search(
            "cat", entity="item", collection_id="col", driver_ref="items_indexer",
        )
    assert chain == []


@pytest.mark.asyncio
async def test_output_on_search_only_resolves_inverse_chain():
    """SEARCH entry's output_transformers resolves to an instance chain
    even when the INDEX entry carries no input_transformers."""
    from dynastore.modules.storage.routing_config import (
        Operation,
        OperationDriverEntry,
        get_output_transformers_for_search,
    )

    ops = {
        Operation.SEARCH: [
            OperationDriverEntry(
                driver_ref="items_indexer",
                output_transformers=("payload_append_transformer",),
            ),
        ],
        Operation.TRANSFORM: [
            OperationDriverEntry(driver_ref="payload_append_transformer"),
        ],
    }
    appender = PayloadAppendTransformer("X")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[appender],
    ):
        chain = await get_output_transformers_for_search(
            "cat", entity="item", collection_id="col", driver_ref="items_indexer",
        )
    assert chain == [appender]
    # Inverse strips the tag back out — what the SEARCH wrap would yield.
    restored = await restore_transform_chain(
        {"tag": "X"}, chain, catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert restored == {}


@pytest.mark.asyncio
async def test_input_and_output_symmetric_round_trip():
    """The PrivateEntityTransformer pattern: same transformer attached to
    INDEX (input) and SEARCH (output). Round-trip restores the original."""
    appender = PayloadAppendTransformer("priv")
    raw = {"id": "1"}
    indexed = await apply_transform_chain(
        raw, [appender], catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert indexed == {"id": "1", "tag": "priv"}
    restored = await restore_transform_chain(
        indexed, [appender], catalog_id="c", collection_id="col", entity_kind="item",
    )
    assert restored == raw


@pytest.mark.asyncio
async def test_chain_composition_two_transformers_left_to_right_then_right_to_left():
    a = PayloadAppendTransformer("A")
    b = PayloadAppendTransformer("B")
    indexed = await apply_transform_chain(
        {}, [a, b], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert indexed == {"tag": "A|B"}
    restored = await restore_transform_chain(
        indexed, [a, b], catalog_id="c", collection_id=None, entity_kind="item",
    )
    assert restored == {}


def test_validator_rejects_dangling_input_transformer_ref():
    """A driver_ref listed in input_transformers that is not present in
    operations[TRANSFORM] makes the routing config raise ValueError at
    build time."""
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    with pytest.raises(ValueError, match="dangling_ref"):
        AssetRoutingConfig(
            operations={
                Operation.INDEX: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("dangling_ref",),
                    ),
                ],
            },
        )


def test_deferred_hop_warns_once_for_write_input_transformers(caplog):
    """Declaring input_transformers on a WRITE entry is a no-op until the
    hop is wired in a future PR. The config-load path emits exactly one
    WARN per (operation, driver, side) tuple."""
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        Operation,
        OperationDriverEntry,
        _DEFERRED_HOP_WARNED,
    )

    _DEFERRED_HOP_WARNED.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        AssetRoutingConfig(
            operations={
                Operation.WRITE: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("noop_attached_transformer",),
                    ),
                ],
                Operation.TRANSFORM: [
                    OperationDriverEntry(driver_ref="noop_attached_transformer"),
                ],
            },
        )
    matches = [
        rec for rec in caplog.records
        if "input_transformers declared on operation 'WRITE'" in rec.message
    ]
    assert len(matches) == 1, (
        "Expected exactly one deferred-hop WARN; got %d" % len(matches)
    )

    caplog.clear()
    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.storage.routing_config",
    ):
        AssetRoutingConfig(
            operations={
                Operation.WRITE: [
                    OperationDriverEntry(
                        driver_ref="asset_indexer",
                        input_transformers=("noop_attached_transformer",),
                    ),
                ],
                Operation.TRANSFORM: [
                    OperationDriverEntry(driver_ref="noop_attached_transformer"),
                ],
            },
        )
    matches = [
        rec for rec in caplog.records
        if "input_transformers declared on operation 'WRITE'" in rec.message
    ]
    assert matches == [], "Second load must not re-emit; dedupe failed"


@pytest.mark.asyncio
async def test_per_item_failure_isolation_with_transformer_at_index_hop():
    """When the input_transformers chain raises for one item in a bulk,
    that item is rejected; the rest reach the indexer and succeed."""
    from typing import Sequence as _Sequence

    from dynastore.models.protocols.indexer import (
        BulkResult, IndexContext, IndexOp,
    )
    from dynastore.modules.storage.index_dispatcher import IndexDispatcher
    from dynastore.modules.storage.routing_config import (
        Operation, OperationDriverEntry,
    )

    class SelectiveRaise:
        async def transform_for_index(self, entity, **_):
            if entity.get("id") == "bad":
                raise RuntimeError("boom on bad")
            out = dict(entity)
            out["seen"] = True
            return out

        async def restore_from_index(self, doc, **_):
            return doc

    class _Indexer:
        def __init__(self) -> None:
            self.received: List[IndexOp] = []

        async def ensure_indexer(self, ctx: IndexContext) -> None:
            return None

        async def index(self, ctx, op) -> None:
            self.received.append(op)

        async def index_bulk(self, ctx, ops: _Sequence[IndexOp]) -> BulkResult:
            self.received.extend(ops)
            return BulkResult(total=len(ops), succeeded=len(ops))

    entry = OperationDriverEntry(
        driver_ref="stub_indexer",
        input_transformers=("selective_raise",),
    )

    class _Routing:
        operations = {
            Operation.INDEX: [entry],
            Operation.TRANSFORM: [
                OperationDriverEntry(driver_ref="selective_raise"),
            ],
        }

    indexer = _Indexer()
    transformer = SelectiveRaise()

    async def routing_resolver(catalog, collection):
        return _Routing()

    async def indexer_registry(driver_id):
        if driver_id == "stub_indexer":
            return indexer
        return None

    dispatcher = IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
    )

    ctx = IndexContext(catalog="cat", collection="col", correlation_id="cid")
    ops = [
        IndexOp(op_type="upsert", entity_type="item", entity_id="ok-1", payload={"id": "ok-1"}),
        IndexOp(op_type="upsert", entity_type="item", entity_id="bad", payload={"id": "bad"}),
        IndexOp(op_type="upsert", entity_type="item", entity_id="ok-2", payload={"id": "ok-2"}),
    ]

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[transformer],
    ):
        results = await dispatcher.fan_out_bulk(ctx, ops)

    result = results["stub_indexer"]
    assert result.total == 3
    assert result.failed == 1
    assert any(
        "input_transformer_failed" in (f.get("reason") or "")
        and f.get("entity_id") == "bad"
        for f in result.failures
    )
    received_ids = [o.entity_id for o in indexer.received]
    assert received_ids == ["ok-1", "ok-2"]
    for op in indexer.received:
        assert op.payload is not None and op.payload.get("seen") is True
