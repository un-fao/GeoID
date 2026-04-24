import pytest
from dynastore.tools.identifiers import generate_id_hex


# Serialize all WFS tests onto a single xdist worker. Without this, parallel
# workers race on CatalogModule lifespan startup (DDL + advisory locks),
# producing flaky LockNotAvailableError / "core protocols UNRESOLVED" failures
# that surface as wrong-status-code asserts (500/422) or 180s pytest-timeouts.
def pytest_collection_modifyitems(config, items):
    for item in items:
        if "/extensions/wfs/" in item.nodeid.replace("\\", "/"):
            item.add_marker(pytest.mark.xdist_group("catalog_lifespan"))


@pytest.fixture
def catalog_id(data_id):
    # Use unique ID per test to avoid partition overlapping issues during rapid create/delete
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    return "region"


@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "title": f"Title for {catalog_id}",
        "description": "Test Catalog for WFS",
    }


@pytest.fixture
def collection_data(collection_id):
    return {
        "id": collection_id,
        "title": f"Title for {collection_id}",
        "description": "Test Collection for WFS",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }


@pytest.fixture
def config_data():
    return {
        "collection_type": "VECTOR",
        "sidecars": [
            {
                "sidecar_type": "geometries",
                "target_srid": 4326,
            },
            {
                "sidecar_type": "attributes",
            },
        ],
    }


@pytest.fixture
async def setup_catalog(in_process_client, catalog_data, catalog_id):
    # Cleanup with hard delete to avoid duplicate key constraints
    # await in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")
    # Create
    r = await in_process_client.post("/features/catalogs", json=catalog_data)
    assert r.status_code == 201
    yield catalog_id
    await in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.fixture
async def setup_collection(
    in_process_client, setup_catalog, collection_data, collection_id, config_data
):
    catalog_id = setup_catalog
    # Create
    r = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    # Configure
    r = await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/classes/CollectionPluginConfig",
        json=config_data,
    )
    assert r.status_code in [200, 204]

    yield collection_id
    # Cleanup is handled by setup_catalog (delete catalog deletes collections)
