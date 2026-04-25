import pytest

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xdist_group("catalog_lifespan"),
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql"
    ),
    pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac"),
]


async def test_get_capabilities_scoped(in_process_client_module, setup_collection):
    pass


async def test_get_capabilities_with_collection(
    in_process_client_module, setup_collection, setup_catalog
):
    catalog_id = setup_catalog
    wfs_url = f"/wfs/{catalog_id}"
    params = {"service": "WFS", "request": "GetCapabilities"}
    r = await in_process_client_module.get(wfs_url, params=params)
    assert r.status_code == 200
