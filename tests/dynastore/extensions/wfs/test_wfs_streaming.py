import pytest
import asyncio
import logging
import orjson
from typing import List

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_collection_core_postgresql", "metadata_collection_stac_postgresql", "metadata_catalog_core_postgresql", "metadata_catalog_stac_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_wfs_streaming_basic(in_process_client, setup_catalog, setup_collection):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # 1. Ingest some data
    features = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": f"Feature {i}", "value": i},
                "geometry": {
                    "type": "Point",
                    "coordinates": [10.0 + i * 0.1, 45.0 + i * 0.1],
                },
            }
            for i in range(10)
        ],
    }

    # Ingest data via Features API
    # Note: Using features extension endpoint
    url = f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"
    response = await in_process_client.post(url, json=features)
    assert response.status_code == 201


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_collection_core_postgresql", "metadata_collection_stac_postgresql", "metadata_catalog_core_postgresql", "metadata_catalog_stac_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_wfs_hits(in_process_client, setup_catalog, setup_collection):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Ingest some data
    features = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"val": i},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
            for i in range(5)
        ],
    }
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=features,
    )

    # Test resultType=hits (Count only)
    wfs_url = f"/wfs/{catalog_id}"
    params_hits = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"{catalog_id}:{collection_id}",
        "resultType": "hits",
    }

    response_hits = await in_process_client.get(wfs_url, params=params_hits)
    assert response_hits.status_code == 200
    content_hits = response_hits.text
    assert 'numberMatched="5"' in content_hits
    assert 'numberReturned="0"' in content_hits


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_collection_core_postgresql", "metadata_collection_stac_postgresql", "metadata_catalog_core_postgresql", "metadata_catalog_stac_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_wfs_streaming_geojson(
    in_process_client, setup_catalog, setup_collection
):
    catalog_id = setup_catalog
    collection_id = setup_collection
    # Ingest some data
    features = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"val": i},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
            for i in range(5)
        ],
    }
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=features,
    )

    wfs_url = f"/wfs/{catalog_id}"

    # Test Streaming GeoJSON
    params_geojson = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"{catalog_id}:{collection_id}",
        "outputFormat": "application/json",
    }

    response_geojson = await in_process_client.get(wfs_url, params=params_geojson)
    assert response_geojson.status_code == 200
    geojson_data = response_geojson.json()
    assert len(geojson_data["features"]) == 5


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "metadata_collection_core_postgresql", "metadata_collection_stac_postgresql", "metadata_catalog_core_postgresql", "metadata_catalog_stac_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_wfs_buffered_gml(in_process_client, setup_catalog, setup_collection):
    catalog_id = setup_catalog
    collection_id = setup_collection
    # Ingest some data
    features = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"val": i},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
            for i in range(5)
        ],
    }
    await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=features,
    )

    wfs_url = f"/wfs/{catalog_id}"

    # Test GML (Buffered)
    params_gml = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"{catalog_id}:{collection_id}",
        "outputFormat": "application/gml+xml; version=3.2",
    }

    response_gml = await in_process_client.get(wfs_url, params=params_gml)
    assert response_gml.status_code == 200
    assert 'numberMatched="5"' in response_gml.text
