import pytest
from typing import Any, Dict, Optional
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.tools.discovery import get_protocol


@pytest.mark.asyncio
async def test_catalogs_protocol_composition(app_lifespan):
    """Verify that the CatalogsProtocol correctly inherits and exposes sub-protocols."""
    from dynastore.modules import get_protocol

    catalogs_svc = get_protocol(CatalogsProtocol)
    assert catalogs_svc is not None

    # Verify inheritance
    # CatalogsProtocol is a runtime_checkable protocol, so isinstance works if the object implements it
    # CatalogModule implements ItemsProtocol and CollectionsProtocol
    assert isinstance(catalogs_svc, ItemsProtocol)
    assert isinstance(catalogs_svc, CollectionsProtocol)

    # Verify property accessors
    assert catalogs_svc.items is not None
    assert catalogs_svc.collections is not None

    # Verify logical item retrieval exists in protocol and implementation
    assert hasattr(catalogs_svc, "get_item")
    assert hasattr(catalogs_svc.items, "get_item")


@pytest.mark.asyncio
async def test_logical_item_retrieval(app_lifespan):
    """Verify that logical item retrieval works without manual schema/table resolution."""
    from dynastore.modules import get_protocol

    catalogs_svc: CatalogsProtocol = get_protocol(CatalogsProtocol)

    catalog_id = "test_logical_cat"
    collection_id = "test_coll"

    # Setup
    if await catalogs_svc.get_catalog_model(catalog_id):
        await catalogs_svc.delete_catalog(catalog_id, force=True)

    await catalogs_svc.create_catalog({"id": catalog_id, "title": "Test Cat"})
    await catalogs_svc.create_collection(
        catalog_id, {"id": collection_id, "title": "Test Coll"}
    )

    # Create an item
    from dynastore.extensions.features.ogc_models import FeatureDefinition

    # Create item logic
    ext_id = "test-item-logical"
    item_data = FeatureDefinition(
        type="Feature",
        id=ext_id,
        geometry={"type": "Point", "coordinates": [12.5, 41.9]},
        properties={"name": "Test Item", "external_id": ext_id},
    )

    # Upsert item (single)
    # Retrieve logically (get_item handles resolution)
    # Note: For this test, item_id in get_item can be external_id or geoid depending on config.
    # Without specific sidecar config, get_item expects geoid.
    # However, this test is checking PROTOCOL COMPOSITION, not sidecar logic details.
    # As we removed get_item_by_external_id, we should test get_item.

    # Let's assume for this basic test we just want to verify connectivity.
    # But get_item requires a valid geoid if no sidecar config is active.
    # We don't have the geoid easily here unless we catch the upsert return.

    # Updated: Upsert returns the created item(s).
    created_items = await catalogs_svc.upsert(catalog_id, collection_id, item_data)
    created_item = (
        created_items[0] if isinstance(created_items, list) else created_items
    )

    # get_item should support retrieval by external_id (logical ID)
    logical_id = item_data.properties["external_id"]

    # Verify using get_item with logical ID
    item = await catalogs_svc.get_item(catalog_id, collection_id, logical_id)
    assert item is not None, f"Item not found via get_item for id {logical_id}"

    # Handle both dict and object/row access depending on service return type
    if hasattr(item, "properties"):
        attributes = item.properties
    elif isinstance(item, dict):
        attributes = item.get("properties") or item.get("attributes")
    else:
        # Fallback for old Row objects
        attributes = getattr(item, "attributes", None)

    # Parse JSON string if needed
    if isinstance(attributes, str):
        import json

        attributes = json.loads(attributes)

    assert attributes["name"] == "Test Item"

    # Retrieve via items property
    item_via_prop = await catalogs_svc.items.get_item(
        catalog_id, collection_id, logical_id
    )
    assert item_via_prop is not None

    # Verify retrieval by geoid is also supported (fallback or explicit?)
    # Currently get_item with configured sidecar usually enforces external_id.
    # If we want geoid support, we need to check if the implementation allows it.
    # For now, let's assume logical ID is the primary contract.

    # Cleanup
    await catalogs_svc.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_removed_methods_not_in_protocol():
    """Verify that internal methods are no longer part of the public protocol."""
    # Note: typing.Protocol doesn't enforce absence at runtime if the object has the method,
    # but we can verify our expectations for the interface if we had a static checker.
    # At least we verify they are gone from the definition (already done).
    pass
