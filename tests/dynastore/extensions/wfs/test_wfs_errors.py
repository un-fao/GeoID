import pytest
from sqlalchemy import text
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.models.driver_context import DriverContext


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_get_feature_missing_table(
    in_process_client, setup_collection, setup_catalog, db_engine
):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Verify we can call WFS and it works (returns empty) if table doesn't exist yet (lazy creation)
    wfs_url = f"/wfs/{catalog_id}"
    params = {
        "service": "WFS",
        "request": "GetFeature",
        "typenames": f"{catalog_id}:{collection_id}",
        "outputformat": "application/json",
    }
    r = await in_process_client.get(wfs_url, params=params)
    if r.status_code != 200:
        print(f"DEBUG: Initial WFS Error: {r.text}")
    assert r.status_code == 200
    try:
        import json

        data = json.loads(r.text)
    except Exception:
        print(f"DEBUG: Initial Response Content: {r.text}")
        raise
    assert len(data["features"]) == 0

    # Now manually drop the table IF it exists (it might have been created by previous tests or if we ingested)
    # But in this test, it shouldn't exist yet.
    # To truly test the fix, we should ingest one item to create the table, then drop it.

    # Ingest one item to materialize the table
    item_data = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"asset_code": "TEST"},
    }
    r = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_data,
    )
    assert r.status_code == 201

    # Verify table now exists and has 1 feature
    r = await in_process_client.get(wfs_url, params=params)
    assert r.status_code == 200
    assert len(r.json()["features"]) == 1

    # Now drop it
    catalogs = get_protocol(CatalogsProtocol)
    async with db_engine.connect() as conn:
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        # Drop Sidecar tables first (dependencies)
        await conn.execute(
            text(
                f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}_attributes" CASCADE'
            )
        )
        await conn.execute(
            text(
                f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}_geometries" CASCADE'
            )
        )
        await conn.execute(
            text(f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}" CASCADE')
        )
        await conn.commit()

    # Now call WFS again. It should NOT fail anymore.
    # It should return 200 OK with an empty feature collection if the collection exists in catalog but table is missing.

    r = await in_process_client.get(wfs_url, params=params)
    assert r.status_code == 200
    # Manual check to avoid JSONDecodeError in test environment
    content = r.text
    assert (
        '"type": "FeatureCollection"' in content
        or '"type":"FeatureCollection"' in content
    )
    assert '"features": []' in content or '"features":[]' in content

    assert len(data["features"]) == 0

    # Finally, test that a non-existent catalog returns 404 or 400 (ValueError)
    bad_wfs_url = "/wfs/non_existent_catalog"
    r = await in_process_client.get(bad_wfs_url, params=params)
    assert r.status_code in [400, 404]


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_describe_feature_type_missing_table(
    in_process_client, setup_collection, setup_catalog, db_engine
):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Typing is catalog:collection
    typename = f"{catalog_id}:{collection_id}"

    # Call DescribeFeatureType when table is missing
    wfs_url = f"/wfs/{catalog_id}"
    params = {"service": "WFS", "request": "DescribeFeatureType", "typename": typename}

    r = await in_process_client.get(wfs_url, params=params)

    # It should return 200 OK with an XSD
    assert r.status_code == 200
    assert "application/xsd" in r.headers["content-type"]
    assert f'name="{collection_id}"' in r.text
    # 'geoid' is internal and not exposed by default in sidecar config (expose_geoid=False)
    # WFS exposes 'id' as the standard identifier.
    assert 'name="id"' in r.text
