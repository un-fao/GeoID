"""
Integration tests for the Search extension.

Covers:
- GET /search                      — simple item search (q, bbox, limit, collections)
- POST /search                     — full-featured item search body
- GET /search/catalogs             — cross-catalog catalog search
- GET /search/collections          — cross-catalog collection search
- POST /search/collections         — collection search POST

All tests use the sysadmin client (no auth complexity) and rely on the
shared catalog/collection fixtures from the root conftest.
"""

import pytest
from httpx import AsyncClient


MARKER = pytest.mark.enable_extensions("stac", "assets", "features", "config", "search")


# ---------------------------------------------------------------------------
# GET /search — simple item search
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_get_empty_returns_feature_collection(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search with no params returns a valid empty FeatureCollection."""
    r = await sysadmin_in_process_client.get("/search")
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert isinstance(data.get("features"), list)


@MARKER
@pytest.mark.asyncio
async def test_search_get_with_q(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search?q= — free-text query accepted, returns valid response."""
    r = await sysadmin_in_process_client.get("/search", params={"q": "nonexistent_xyz_123"})
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert data.get("features") == []


@MARKER
@pytest.mark.asyncio
async def test_search_get_with_bbox(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search?bbox= — spatial filter accepted."""
    r = await sysadmin_in_process_client.get(
        "/search", params={"bbox": "-180,-90,180,90"}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_search_get_with_limit(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search?limit= — limit parameter accepted."""
    r = await sysadmin_in_process_client.get("/search", params={"limit": 5})
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_get_with_collections_filter(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search?collections= — collection scoping accepted."""
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(
        "/search", params={"collections": collection_id}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


# ---------------------------------------------------------------------------
# POST /search — full-featured item search
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_post_empty_body(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search with empty body returns a valid FeatureCollection."""
    r = await sysadmin_in_process_client.post("/search", json={})
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_search_post_with_collections(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /search filtering by collections list."""
    collection_id = setup_collection
    r = await sysadmin_in_process_client.post(
        "/search", json={"collections": [collection_id]}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_search_post_with_bbox(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search with bbox spatial filter."""
    r = await sysadmin_in_process_client.post(
        "/search", json={"bbox": [-180, -90, 180, 90]}
    )
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_post_with_ids(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search filtering by item IDs returns valid response."""
    r = await sysadmin_in_process_client.post(
        "/search", json={"ids": ["nonexistent_id_xyz"]}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("features") == []


# ---------------------------------------------------------------------------
# GET /search/catalogs — catalog search
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_catalogs_get(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """GET /search/catalogs — returns catalog list."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get("/search/catalogs")
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_catalogs_get_with_q(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """GET /search/catalogs?q= — text search over catalogs."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        "/search/catalogs", params={"q": catalog_id[:8]}
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# GET /search/collections — collection search
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_collections_get(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search/collections — returns collection list."""
    r = await sysadmin_in_process_client.get("/search/collections")
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_collections_get_with_q(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search/collections?q= — text search over collections."""
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(
        "/search/collections", params={"q": collection_id[:8]}
    )
    assert r.status_code == 200
