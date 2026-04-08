import pytest
import pystac
from tests.dynastore.test_utils import generate_test_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "configs")
async def test_stac_metadata_enrichment_vector(in_process_client, test_data_loader):
    """
    Test that a virtual asset collection is enriched with Vector GDAL/OGR metadata.
    """
    run_id = generate_test_id()
    catalog_id = f"cat_v_{run_id}"
    collection_id = f"col_v_{run_id}"
    asset_id = f"vector_asset_{run_id}"

    # 1. Create Catalog
    cat_data = test_data_loader("catalog.json")
    cat_data["id"] = catalog_id
    await in_process_client.post("/stac/catalogs", json=cat_data)

    # 2. Create Collection
    col_data = test_data_loader("collection.json")
    col_data["id"] = collection_id
    await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=col_data
    )

    # 3. Create Asset with Vector Metadata (based on prompt example)
    vector_metadata = {
        "iso3": "BEL",
        "owner": "daniele_glims_02",
        "title": "Belgium Administrative Boundaries",
        "gdalinfo": {
            "driverShortName": "ESRI Shapefile",
            "layers": [
                {
                    "name": "BELL1_01",
                    "extent": [2.5, 49.4, 6.4, 51.5],
                    "featureCount": 3,
                    "geometryType": "Polygon",
                    "coordinateSystem": {
                        "wkt": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]]]'
                    },
                    "fields": [
                        {"name": "id", "type": "String"},
                        {"name": "LEVEL", "type": "Integer"},
                    ],
                }
            ],
        },
    }

    asset_payload = {
        "asset_id": asset_id,
        "uri": f"gs://bucket/{asset_id}.zip",
        "asset_type": "VECTORIAL",
        "metadata": vector_metadata,
    }
    await in_process_client.post(
        f"/assets/catalogs/{catalog_id}/collections/{collection_id}", json=asset_payload
    )

    # 4. Get Virtual Asset Collection
    resp = await in_process_client.get(
        f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert resp.status_code == 200
    data = resp.json()

    # --- Assertions ---

    # Custom fields with asset: prefix
    assert data["asset:iso3"] == "BEL"
    assert data["asset:owner"] == "daniele_glims_02"

    # Vector Extension fields
    assert data["vector:geometry_type"] == "Polygon"
    assert data["vector:count"] == 3

    # Projection Extension
    assert "proj:wkt2" in data
    assert 'GEOGCS["WGS 84"' in data["proj:wkt2"]

    # Table Extension
    assert "table:columns" in data
    assert data["table:columns"][0]["name"] == "id"

    # Extent
    assert data["extent"]["spatial"]["bbox"] == [[2.5, 49.4, 6.4, 51.5]]

    # Cleanup handled by session fixture
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}?force=true")


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "configs")
async def test_stac_metadata_enrichment_raster(in_process_client, test_data_loader):
    """
    Test that a virtual asset collection is enriched with Raster GDAL metadata.
    """
    run_id = generate_test_id()
    catalog_id = f"cat_r_{run_id}"
    collection_id = f"col_r_{run_id}"
    asset_id = f"raster_asset_{run_id}"

    # 1. Create Catalog
    cat_data = test_data_loader("catalog.json")
    cat_data["id"] = catalog_id
    await in_process_client.post("/stac/catalogs", json=cat_data)

    # 2. Create Collection
    col_data = test_data_loader("collection.json")
    col_data["id"] = collection_id
    await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=col_data
    )

    # 3. Create Asset with Raster Metadata
    raster_metadata = {
        "source": "Sentinel-2",
        "gdalinfo": {
            "driverShortName": "GTiff",
            "size": [1000, 1000],
            "coordinateSystem": {"wkt": 'GEOGCS["WGS 84"]'},
            "geoTransform": [300000.0, 10.0, 0.0, 5000000.0, 0.0, -10.0],
            "wgs84Extent": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [10.0, 40.0],
                        [11.0, 40.0],
                        [11.0, 41.0],
                        [10.0, 41.0],
                        [10.0, 40.0],
                    ]
                ],
            },
            "bands": [
                {"band": 1, "type": "Byte", "noDataValue": 0, "description": "Red Band"}
            ],
        },
    }

    asset_payload = {
        "asset_id": asset_id,
        "uri": f"gs://bucket/{asset_id}.tif",
        "asset_type": "RASTER",
        "metadata": raster_metadata,
    }
    await in_process_client.post(
        f"/assets/catalogs/{catalog_id}/collections/{collection_id}", json=asset_payload
    )

    # 4. Get Virtual Asset Collection
    resp = await in_process_client.get(
        f"/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert resp.status_code == 200
    data = resp.json()

    # --- Assertions ---

    # Custom field
    assert data["asset:source"] == "Sentinel-2"

    # Projection Extension
    assert data["proj:shape"] == [1000, 1000]
    assert data["proj:transform"] == [300000.0, 10.0, 0.0, 5000000.0, 0.0, -10.0]

    # Raster extension via summaries
    # Note: pystac-client or how pystac serializes summaries might vary,
    # but here we expect them in the summaries field or enriched properties.
    # In my implementation I used RasterExtension.summaries(collection, add_if_missing=True)
    # which usually adds them to 'summaries'.
    assert "raster:bands" in data["summaries"]
    assert data["summaries"]["raster:bands"][0]["nodata"] == 0

    # EO Extension
    assert "eo:bands" in data["summaries"]
    assert data["summaries"]["eo:bands"][0]["name"] == "band_1"

    # Extent
    assert data["extent"]["spatial"]["bbox"] == [[10.0, 40.0, 11.0, 41.0]]

    # Cleanup handled by session fixture
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}?force=true")
