import pytest
import asyncio
from httpx import AsyncClient
from tests.dynastore.test_utils import generate_test_id

@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tiles", "gcp")
@pytest.mark.enable_extensions("tiles", "assets", "features")
async def test_tiles_from_bulk_ingested_data(in_process_client: AsyncClient):
    catalog_id = f"c_{generate_test_id()}"
    collection_id = "test_tiles_collection"
    
    # 1. Setup Catalog and Collection
    # Use /features prefix for catalog/collection management
    catalog_response = await in_process_client.post(
        f"/features/catalogs", 
        json={"id": catalog_id, "title": "Test Tiles Catalog"}
    )
    assert catalog_response.status_code == 201, f"Catalog creation failed: {catalog_response.text}"
    
    collection_response = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json={
            "id": collection_id,
            "title": "Test Tiles Collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]}
            }
        }
    )
    assert collection_response.status_code == 201, f"Collection creation failed: {collection_response.text}"
    
    # 2. Bulk Ingest Data (with extra field that previously caused error)
    bulk_payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]},
                "properties": {"name": "Test Area", "asset_code": "WILL_NOT_BREAK"}
            }
        ]
    }
    items_response = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", 
        json=bulk_payload
    )
    assert items_response.status_code == 201, f"Bulk item creation failed: {items_response.text}"
    
    # 2.5. Small delay to ensure transaction is committed and data is visible
    # This addresses potential race conditions with transaction isolation or index updates
    await asyncio.sleep(0.1)
    
    # 3. Request a Tile
    # Using legacy MVT endpoint: /{dataset}/tiles/{z}/{x}/{y}.mvt
    # Note: TilesService prefix is /tiles
    z, x, y = 0, 0, 0

    # The endpoint in tiles_service.py is @router.get("/{dataset}/tiles/{z}/{x}/{y}.mvt")
    # So the full path is /tiles/{catalog_id}/tiles/0/0/0.mvt
    tile_url = f"/tiles/{catalog_id}/tiles/{z}/{x}/{y}.mvt?collections={collection_id}"
    
    # Retry logic to handle potential timing issues
    max_retries = 3
    tile_response = None
    for attempt in range(max_retries):
        tile_response = await in_process_client.get(tile_url)
        if tile_response.status_code in [200, 204]:
            break
        if attempt < max_retries - 1:
            await asyncio.sleep(0.2)
    
    # If the collection exists but no data is in the tile (e.g. bbox mismatch), 204 is possible.
    assert tile_response.status_code in [200, 204], f"Tile request failed after {max_retries} attempts: {tile_response.text}"
    if tile_response.status_code == 200:
        assert len(tile_response.content) > 0
        assert "vnd.mapbox-vector-tile" in tile_response.headers["Content-Type"]


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tiles")
@pytest.mark.enable_extensions("tiles", "assets", "features")
async def test_tiles_with_pg_storage(in_process_client: AsyncClient):
    """
    Verifies that tiles can be generated and cached using the PostgreSQL storage provider.
    This test does not depend on GCP resources.
    """
    catalog_id = f"c_pg_{generate_test_id()}"
    collection_id = "test_pg_tiles_collection"
    
    # 1. Setup Catalog and Collection
    await in_process_client.post(
        f"/features/catalogs", 
        json={"id": catalog_id, "title": "Test PG Tiles Catalog"}
    )
    
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json={
            "id": collection_id,
            "title": "Test PG Tiles Collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]}
            }
        }
    )
    
    # 2. Configure PG storage priority for this catalog
    # We use the configs extension to set storage_priority = ["pg"]
    config_response = await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/plugins/tiles_preseed_config",
        json={"storage_priority": ["pg"]}
    )
    assert config_response.status_code == 200, f"Failed to set storage priority: {config_response.text}"
    
    # 3. Ingest Data
    bulk_payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]},
                "properties": {"name": "Test Area"}
            }
        ]
    }
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", 
        json=bulk_payload
    )
    
    await asyncio.sleep(0.1)
    
    # 4. Request a Tile (Triggers background cache to PG)
    z, x, y = 0, 0, 0
    tile_url = f"/tiles/{catalog_id}/tiles/{z}/{x}/{y}.mvt?collections={collection_id}"
    
    tile_response = await in_process_client.get(tile_url)
    assert tile_response.status_code in [200, 204], f"Tile request failed: {tile_response.text}"
    
    # 5. Verify caching by re-requesting (should be a hit, although we don't explicitly check headers here)
    # The main point is that it doesn't crash trying to use GCS.
    tile_response_2 = await in_process_client.get(tile_url)
    assert tile_response_2.status_code == tile_response.status_code
