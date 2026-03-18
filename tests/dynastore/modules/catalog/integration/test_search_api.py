import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_search_catalogs_and_collections(sysadmin_in_process_client: AsyncClient, setup_catalog_with_collection):
    catalog_id, collection_id = setup_catalog_with_collection
    
    # 1. Test Catalog Search (match) — use a 12-char prefix for uniqueness
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs?q={catalog_id[:12]}")
    assert res.status_code == 200
    data = res.json()
    assert any(c['id'] == catalog_id for c in data)

    # 2. Test Catalog Search (no match)
    res = await sysadmin_in_process_client.get("/web/dashboard/catalogs?q=this_should_not_match_anything")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 0

    # 3. Test Collection Search (match)
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs/{catalog_id}/collections?q={collection_id[:12]}")
    assert res.status_code == 200
    data = res.json()
    assert any(c['id'] == collection_id for c in data)

    # 4. Test Collection Search (no match)
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs/{catalog_id}/collections?q=this_should_not_match")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 0

