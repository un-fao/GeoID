"""transform_runtime — chain composition semantics.

apply_transform_chain runs left-to-right; restore_transform_chain runs
right-to-left so the output shape matches the original entity. Empty chain
is identity. Exceptions propagate after a warning log.
"""

from __future__ import annotations

from typing import Any, List

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
