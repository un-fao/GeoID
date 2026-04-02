import pytest
from fastapi import status
from dynastore.tools.identifiers import generate_id_hex

# asyncio_mode=auto in pytest.ini handles async test discovery


async def test_stac_link_verification_scenario(sysadmin_in_process_client):
    """
    Comprehensive verification of STAC links and metadata handling in a single session.
    Combines Catalog, Collection, and Item verification to avoid DB reset issues.
    """
    client = sysadmin_in_process_client

    # --- Step 1: Create Catalog ---
    cat_id = f"aa_cat_verify_{generate_id_hex()[:8]}"
    create_res = await client.post(
        "/stac/catalogs",
        json={
            "id": cat_id,
            "title": "Verification Catalog",
            "description": "Testing links",
            "extra_metadata": {"foo": "bar"},
        },
    )
    assert create_res.status_code == status.HTTP_201_CREATED

    # --- Step 2: List Catalogs (Verify extra_metadata unpacking) ---
    res = await client.get("/stac/catalogs", params={"limit": 1000})
    assert res.status_code == 200
    catalogs = res.json()
    target_cat = next((c for c in catalogs if c["id"] == cat_id), None)
    if target_cat is None:
        pytest.fail(f"Catalog {cat_id} not found in response: {[c.get('id') for c in catalogs]}")
    assert target_cat is not None

    # Verify Structure (Summary View)
    assert target_cat["type"] == "Catalog"
    assert "conformsTo" not in target_cat  # Should be cleaned
    assert "extra_metadata" not in target_cat  # Should be flattened
    assert target_cat.get("foo") == "bar"  # Merged extra_metadata

    # Verify Links
    links = target_cat["links"]
    link_rels = {l["rel"]: l for l in links}
    assert "self" in link_rels
    assert link_rels["self"]["href"].endswith(f"/stac/catalogs/{cat_id}")
    assert link_rels["data"]["href"].endswith(f"/stac/catalogs/{cat_id}/collections")

    # --- Step 3: Get Single Catalog (Verify get_catalog works) ---
    res_get = await client.get(f"/stac/catalogs/{cat_id}")
    assert res_get.status_code == 200
    cat_single = res_get.json()
    assert cat_single["id"] == cat_id
    assert cat_single.get("foo") == "bar"

    # --- Step 4: Create Collection ---
    col_id = f"col_verify_{generate_id_hex()[:8]}"
    create_col = await client.post(
        f"/stac/catalogs/{cat_id}/collections",
        json={
            "id": col_id,
            "title": "Verification Collection",
            "description": "Desc",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )
    assert create_col.status_code == status.HTTP_201_CREATED

    # --- Step 5: List Collections (Verify items link) ---
    res_cols = await client.get(f"/stac/catalogs/{cat_id}/collections")
    assert res_cols.status_code == 200
    cols_data = res_cols.json()

    target_col = next((c for c in cols_data["collections"] if c["id"] == col_id), None)
    if target_col is None:
        pytest.fail(f"Collection {col_id} not found in response: {cols_data}")
    assert target_col is not None

    # Verify Items Link
    col_links = {l["rel"]: l for l in target_col.get("links", [])}
    items_href = col_links.get("items", {}).get("href", "")
    expected_suffix = f"/stac/catalogs/{cat_id}/collections/{col_id}/items"
    assert items_href.endswith(expected_suffix), f"items link missing/wrong: {items_href}"
    assert "/collections/collections/" not in items_href

    # --- Step 6: Get Empty Items (Verify no crash) ---
    res_items = await client.get(f"/stac/catalogs/{cat_id}/collections/{col_id}/items")
    assert res_items.status_code == 200
    items_data = res_items.json()
    assert items_data["numberMatched"] == 0

    # --- Step 7: Create Item ---
    item_id = "test_item_01"
    create_item = await client.post(
        f"/stac/catalogs/{cat_id}/collections/{col_id}/items",
        json={
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "bbox": [0.0, 0.0, 0.0, 0.0],
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {},
        },
    )
    if create_item.status_code != 201:
        print(f"Item creation failed with {create_item.status_code}")
        print(f"Response: {create_item.text}")
    assert create_item.status_code == 201

    # --- Step 8: Get Single Item (Verify links) ---
    res_item = await client.get(
        f"/stac/catalogs/{cat_id}/collections/{col_id}/items/{item_id}"
    )
    assert res_item.status_code == 200
    item = res_item.json()

    item_links = {l["rel"]: l for l in item["links"]}
    assert "self" in item_links
    assert "parent" in item_links
    assert "collection" in item_links

    assert item_links["self"]["href"].endswith(f"/items/{item_id}")
    assert item_links["parent"]["href"].endswith(f"/collections/{col_id}")
