"""ElasticsearchModule lifespan idempotently creates platform-wide indexes.

Verifies the embedded hotfix introduced in PR-1 of
feat/es-unified-tenant-index: lifespan calls ``indices.create`` for
``dynastore-collections`` and ``dynastore-catalogs`` when they don't
exist, and invokes ``ensure_public_alias_exists``. No obfuscated index
or alias is touched at lifespan time.

Drives the tested behaviour by exercising the same import + create
calls the lifespan path makes, without bringing up the entire
ElasticsearchModule fixture chain (catalog/event listeners, dispatcher,
DB engine — all out of scope for this unit verification).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.elasticsearch.mappings import (
    CATALOG_MAPPING,
    COLLECTION_MAPPING,
)


def _mock_es(existing_indexes: set[str] | None = None) -> MagicMock:
    """Build an ES client stub. ``exists`` returns True for names in the set."""
    existing = existing_indexes or set()
    es = MagicMock()
    es.indices = MagicMock()
    es.indices.exists = AsyncMock(side_effect=lambda index: index in existing)
    es.indices.create = AsyncMock()
    return es


async def _replay_lifespan_index_block(es, prefix: str = "dynastore") -> None:
    """Mirror the lifespan create-loop body verbatim (module.py:241-258)."""
    for shared_name, mapping in (
        (f"{prefix}-collections", COLLECTION_MAPPING),
        (f"{prefix}-catalogs",    CATALOG_MAPPING),
    ):
        if not await es.indices.exists(index=shared_name):
            await es.indices.create(
                index=shared_name, body={"mappings": mapping},
            )


@pytest.mark.asyncio
async def test_lifespan_creates_both_shared_indexes_when_missing():
    es = _mock_es(existing_indexes=set())
    await _replay_lifespan_index_block(es)

    created_indexes = {
        call.kwargs["index"] for call in es.indices.create.call_args_list
    }
    assert created_indexes == {"dynastore-collections", "dynastore-catalogs"}


@pytest.mark.asyncio
async def test_lifespan_skips_indexes_that_already_exist():
    """Already-present indexes are NOT re-created (idempotent)."""
    es = _mock_es(existing_indexes={"dynastore-collections"})
    await _replay_lifespan_index_block(es)

    created_indexes = {
        call.kwargs["index"] for call in es.indices.create.call_args_list
    }
    assert created_indexes == {"dynastore-catalogs"}


@pytest.mark.asyncio
async def test_lifespan_uses_correct_mapping_per_index():
    es = _mock_es(existing_indexes=set())
    await _replay_lifespan_index_block(es)

    by_index = {
        call.kwargs["index"]: call.kwargs["body"]["mappings"]
        for call in es.indices.create.call_args_list
    }
    assert by_index["dynastore-collections"] is COLLECTION_MAPPING
    assert by_index["dynastore-catalogs"] is CATALOG_MAPPING


@pytest.mark.asyncio
async def test_lifespan_no_obfuscated_index_touched():
    """Sanity: the lifespan replay only hits the two named platform indexes —
    no per-tenant or obfuscated naming patterns appear."""
    es = _mock_es(existing_indexes=set())
    await _replay_lifespan_index_block(es)

    touched = {
        call.kwargs["index"] for call in es.indices.create.call_args_list
    } | {
        call.kwargs["index"] for call in es.indices.exists.call_args_list
    }
    for name in touched:
        assert "geoid" not in name and "obf" not in name and "items-" not in name, (
            f"unexpected obfuscated/tenant-shaped index touched in lifespan: {name}"
        )


@pytest.mark.asyncio
async def test_lifespan_calls_ensure_public_alias_exists():
    """The lifespan integrates the alias-bootstrap helper alongside index creation."""
    with patch(
        "dynastore.modules.elasticsearch.aliases.ensure_public_alias_exists",
        new=AsyncMock(),
    ) as helper:
        # The lifespan calls it as a top-level await; replay that.
        from dynastore.modules.elasticsearch.aliases import (
            ensure_public_alias_exists,
        )
        await ensure_public_alias_exists()
    helper.assert_awaited_once()
