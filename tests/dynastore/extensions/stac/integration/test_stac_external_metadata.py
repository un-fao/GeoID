import pytest
from dynastore.extensions.stac.stac_models import STACItem
from dynastore.models.driver_context import DriverContext


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "config")
async def test_stac_external_metadata_flow(
    in_process_client, setup_catalog, collection_data, collection_id
):
    catalog_id = setup_catalog

    # 1. Create collection via STAC
    # We expect stac_metadata sidecar to be injected automatically
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code != 201:
        print(f"DEBUG: Response status: {r.status_code}")
        print(f"DEBUG: Response body: {r.text}")
    assert r.status_code == 201
    collection_res = r.json()

    # Verify the sidecar is configured (requires checking the config via internal protocol or checking the DB)
    # For now, we trust the injection worked if the following item ops succeed

    # 2. Add an item with external metadata and managed assets
    # We'll use a mix of:
    # - Title/Description in i18n format
    # - External assets
    # - Managed assets (simulated by existing providers)

    item_id = "test-metadata-item"
    external_item = {
        "id": item_id,
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/datacube/v2.3.0/schema.json",
            "https://example.com/custom-extension/v1.0.0/schema.json",  # External extension
        ],
        "bbox": [12.4, 41.8, 12.6, 42.0],
        "geometry": {"type": "Point", "coordinates": [12.4964, 41.9028]},
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "title": {"en": "English Title", "it": "Titolo Italiano"},
            "description": "Plain description",
            "custom:field": "custom value",
        },
        "assets": {
            "managed_asset": {
                "href": "http://managed.com",
                "is_managed": True,
                "title": "Managed Title",
            },
            "external_asset": {
                "href": "http://external.com",
                "title": {"en": "External En", "it": "Esterno It"},
                "roles": ["data"],
            },
        },
    }

    # Add Item
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=external_item,
    )
    if r.status_code != 201:
        print(f"DEBUG: Item Add Response status: {r.status_code}")
        print(f"DEBUG: Item Add Response body: {r.text}")
    assert r.status_code == 201

    # 3. Retrieve Item and Verify Merging
    # We retrieve in Italian to test i18n resolution
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}?lang=it"
    )
    assert r.status_code == 200
    retrieved = r.json()

    # Check properties merging/localization
    props = retrieved.get("properties", {})
    assert props.get("title") == "Titolo Italiano"
    assert props.get("description") == "Plain description"  # Fallback or preserved

    # Check assets merging/localization
    assets = retrieved.get("assets", {})
    assert "external_asset" in assets
    assert assets["external_asset"]["title"] == "Esterno It"

    # Managed assets should also be present (from providers)
    # Note: actual managed assets depends on which extensions are enabled and if they match the item
    # e.g. vector_tiles, geojson (from OGC Features)
    assert "geojson" in assets or "source_file" in assets

    # Check extensions merging
    extensions = retrieved.get("stac_extensions", [])
    assert "https://example.com/custom-extension/v1.0.0/schema.json" in extensions
    # Language extension should be added automatically
    assert any("language" in ext for ext in extensions)

    # Check extra fields
    # Extra fields are flattened into the item top-level or properties by PySTAC/GeoJSON spec
    # Our implementation puts them in properties if merged via Sidecar
    val = retrieved.get("custom:field") or retrieved["properties"].get("custom:field")
    assert val == "custom value"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "config")
async def test_stac_metadata_pruning_verification(
    in_process_client, setup_catalog, setup_collection, app_lifespan
):
    """
    Directly verify that data is pruned from attributes sidecar and stored in stac_metadata sidecar.
    """
    catalog_id = setup_catalog
    collection_id = setup_collection

    item_id = "test-pruning-verification"
    item_payload = {
        "id": item_id,
        "type": "Feature",
        "stac_version": "1.1.0",
        "bbox": [-10, -10, 10, 10],
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "title": "Pruned Title",
            "stac_metadata_only": "should be in sidecar",
        },
        "assets": {"external": {"href": "http://ext.com"}},
    }

    # Add Item
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_payload,
    )
    if r.status_code != 201:
        print(f"DEBUG: Item Add Pruning Response status: {r.status_code}")
        print(f"DEBUG: Item Add Pruning Response body: {r.text}")
    assert r.status_code == 201

    # Now inspect the database directly
    from dynastore.modules.db_config.tools import managed_transaction
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol
    from sqlalchemy import text

    catalogs = get_protocol(CatalogsProtocol)

    async with managed_transaction(app_lifespan.engine) as conn:
        schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
        table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        # 1. Check item_metadata table (formerly stac_metadata)
        stac_table = f"{table}_item_metadata"
        res = await conn.execute(
            text(f'SELECT title, external_assets FROM "{schema}"."{stac_table}"')
        )
        row = res.fetchone()
        assert row is not None
        import json

        title_json = row[0]
        if isinstance(title_json, str):
            title_json = json.loads(title_json)
        assert title_json.get("en") == "Pruned Title"

        # 2. Check attributes sidecar table - it should NOT contain the title or assets if we pruned them
        # Wait, current AttributesSidecar stores EVERYTHING in 'attributes' JSONB.
        # If we pruned the payload before upsert, it should be clean.
        attr_table = f"{table}_attributes"
        res = await conn.execute(
            text(f'SELECT attributes FROM "{schema}"."{attr_table}"')
        )
        row = res.fetchone()
        assert row is not None
        attr_json = row[0]
        if isinstance(attr_json, str):
            attr_json = json.loads(attr_json)

        # In DynaStore, 'title' is often a core field, but in STAC it's in properties.
        # If it was in the properties dict passed to AttributesSidecar, it would be in 'attributes' column.
        assert "title" not in attr_json
        assert "assets" not in attr_json
