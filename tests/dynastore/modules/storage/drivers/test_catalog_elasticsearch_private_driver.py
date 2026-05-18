"""#960 — smoke tests for ``CatalogElasticsearchPrivateDriver``.

Pins the structural invariants of the new driver class without
exercising the live ES client:

- Distinct ``driver_ref`` so the dispatcher routes to the right driver.
- ``auto_register_for_routing`` is empty — operators must explicitly
  pin this driver in :class:`CatalogRoutingConfig`; auto-default
  augmentation must NEVER reach for it.
- Per-tenant index name resolution via
  ``get_tenant_catalog_private_index`` matches the convention.
- ``location`` returns the per-tenant index path (not the shared
  ``{prefix}-catalogs`` of the public driver).
- ``upsert_catalog_metadata`` lazily ensures the per-tenant index
  exists before writing.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriver,
)
from dynastore.modules.elasticsearch.mappings import (
    get_tenant_catalog_private_index,
)
from dynastore.modules.storage.drivers.elasticsearch_private import (
    CatalogElasticsearchPrivateDriver,
)


def test_driver_ref_resolves_via_class_name_snake_case():
    """Driver identity is ``_to_snake(type(impl).__name__)``.  Pin that
    the private driver resolves to a *distinct* id from the public
    driver so routing-config can target either independently.
    """
    from dynastore.tools.typed_store.base import _to_snake

    private_id = _to_snake(CatalogElasticsearchPrivateDriver.__name__)
    public_id = _to_snake(CatalogElasticsearchDriver.__name__)
    assert private_id == "catalog_elasticsearch_private_driver"
    assert public_id == "catalog_elasticsearch_driver"
    assert private_id != public_id


def test_auto_register_for_routing_is_empty():
    """Operators must explicitly pin the private driver — never
    auto-injected into ``CatalogRoutingConfig.operations[INDEX]``."""
    assert CatalogElasticsearchPrivateDriver.auto_register_for_routing == frozenset()


def test_subclasses_public_driver():
    """The private driver inherits the public ``CatalogStore`` surface
    so we get capabilities, the is_catalog_indexer marker, and CRUD
    plumbing for free."""
    assert issubclass(
        CatalogElasticsearchPrivateDriver, CatalogElasticsearchDriver,
    )


def test_per_tenant_index_name():
    """Index name follows the per-catalog convention."""
    expected = get_tenant_catalog_private_index("dynastore", "cat-a")
    assert expected == "dynastore-cat-a-catalog-private"


def test_index_name_resolution_uses_per_tenant_helper():
    driver = CatalogElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        assert driver._private_index("cat-a") == "dynastore-cat-a-catalog-private"


def test_location_returns_per_tenant_path():
    driver = CatalogElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        loc = driver.location("cat-a")
    assert loc.backend == "elasticsearch_private"
    assert loc.canonical_uri == "es://dynastore-cat-a-catalog-private"
    assert loc.identifiers["index"] == "dynastore-cat-a-catalog-private"
    assert loc.identifiers["catalog_id"] == "cat-a"


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_lazily_ensures_index():
    """First write to a per-tenant private catalog index must
    self-create the index — otherwise auto_create-disabled clusters
    would reject the write OR auto-create the index without
    CATALOG_MAPPING."""
    driver = CatalogElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.exists = AsyncMock(return_value=False)
    es_client.indices.create = AsyncMock(return_value=None)
    es_client.index = AsyncMock(return_value={"result": "created"})
    with patch.object(driver, "_get_client", return_value=es_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        await driver.upsert_catalog_metadata("cat-a", {"id": "cat-a", "title": {"en": "T"}})
    es_client.indices.exists.assert_awaited_once()
    es_client.indices.create.assert_awaited_once()
    es_client.index.assert_awaited_once()
    create_kwargs = es_client.indices.create.await_args.kwargs
    assert create_kwargs["index"] == "dynastore-cat-a-catalog-private"
    assert "mappings" in create_kwargs["body"]


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_skips_create_when_index_exists():
    """ensure_storage short-circuits when the index already exists —
    avoids racing concurrent upserts on the create path."""
    driver = CatalogElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.exists = AsyncMock(return_value=True)
    es_client.indices.create = AsyncMock(return_value=None)
    es_client.index = AsyncMock(return_value={"result": "created"})
    with patch.object(driver, "_get_client", return_value=es_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        await driver.upsert_catalog_metadata("cat-a", {"id": "cat-a"})
    es_client.indices.exists.assert_awaited_once()
    es_client.indices.create.assert_not_awaited()
    es_client.index.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_storage_noop_without_catalog_id():
    """Per-tenant index requires a catalog_id — public-parent signature
    parity ``catalog_id=None`` is preserved but becomes a no-op (no
    shared 'platform' index to create at this tier)."""
    driver = CatalogElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.exists = AsyncMock(return_value=False)
    es_client.indices.create = AsyncMock(return_value=None)
    with patch.object(driver, "_get_client", return_value=es_client):
        await driver.ensure_storage(None)
    es_client.indices.exists.assert_not_awaited()
    es_client.indices.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_drop_storage_tolerates_missing_index():
    """Idempotent drop — index_not_found is fine."""
    driver = CatalogElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.delete = AsyncMock(side_effect=Exception("index_not_found_exception"))
    with patch.object(driver, "_get_client", return_value=es_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        await driver.drop_storage("cat-a")
    es_client.indices.delete.assert_awaited_once()
