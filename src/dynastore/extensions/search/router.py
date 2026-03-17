"""
FastAPI router implementing the STAC API Item Search specification.

GET  /search      – Simple filtering (bbox, datetime, intersects, limit, ids, collections, q, sortby)
POST /search      – Full-featured body-based filtering (same fields + token for pagination)
GET  /search/catalogs    – Keyword search over catalog index
POST /search/catalogs    – Body-based catalog search
GET  /search/collections – Keyword search over collection index
POST /search/collections – Body-based collection search
GET  /search/geoid/{geoid}                                       – Single geoid lookup (obfuscated index)
POST /search/geoid                                               – Batch geoid lookup (obfuscated index)
POST /search/reindex/catalogs/{catalog_id}                       – Trigger bulk catalog reindex (admin)
POST /search/reindex/catalogs/{catalog_id}/collections/{cid}     – Trigger single collection reindex (admin)

Conformance class: https://api.stacspec.org/v1.0.0/item-search

The router discovers its backend via ``SearchProtocol`` — no direct import
of any search implementation (Elasticsearch, Solr, etc.).
"""
import logging
from typing import Any, Dict, List, Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from .search_models import (
    CatalogSearchBody,
    GeoidCollection,
    GeoidSearchBody,
    GenericCollection,
    ItemCollection,
    SearchBody,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Item Search"])


def _base_url(request: Request) -> str:
    """Derive base URL (scheme + host) from request."""
    return str(request.base_url).rstrip("/")


def _get_search_service():
    """Discover the SearchProtocol implementation at runtime."""
    from dynastore.models.protocols.search import SearchProtocol
    from dynastore.tools.discovery import get_protocol
    svc = get_protocol(SearchProtocol)
    if not svc:
        raise HTTPException(
            status_code=503,
            detail="Search backend not available. No SearchProtocol implementation loaded.",
        )
    return svc


# ---------------------------------------------------------------------------
# STAC Item Search – GET (Simple)
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=ItemCollection,
    summary="Search STAC items with simple filtering.",
    description=(
        "Retrieve Items matching filters. Covers the full STAC API Item Search "
        "conformance class (https://api.stacspec.org/v1.0.0/item-search). "
        "All text fields (title, description, keywords) support multilingual values."
    ),
    response_model_exclude_none=True,
)
async def get_search(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description, and all item properties."),
    bbox: Optional[str] = Query(None, description="Bounding box: min_lon,min_lat,max_lon,max_lat", example="-180,-90,180,90"),
    datetime: Optional[str] = Query(None, description="RFC 3339 date-time or interval, e.g. '2021-01-01T00:00:00Z/..'"),
    limit: int = Query(10, ge=1, le=10_000, description="Max number of items to return."),
    ids: Optional[str] = Query(None, description="Comma-separated list of Item IDs."),
    collections: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    sortby: Optional[str] = Query(None, description="Sort field. Prefix with '+'/'-' for asc/desc. E.g. '+properties.title'"),
    token: Optional[str] = Query(None, description="Pagination cursor from a previous response's 'next' link."),
) -> ItemCollection:
    parsed_bbox = [float(x) for x in bbox.split(",")] if bbox else None
    parsed_ids = [i.strip() for i in ids.split(",")] if ids else None
    parsed_collections = [c.strip() for c in collections.split(",")] if collections else None

    body = SearchBody(
        q=q,
        bbox=parsed_bbox,
        datetime=datetime,
        limit=limit,
        ids=parsed_ids,
        collections=parsed_collections,
        sortby=sortby,
        token=token,
    )
    _service = _get_search_service()
    return await _service.search_items(body, base_url=_base_url(request))



# ---------------------------------------------------------------------------
# STAC Item Search – POST (Full-featured)
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=ItemCollection,
    summary="Search STAC items with full-featured filtering.",
    description=(
        "Retrieve items matching filters. Full-featured query following the "
        "STAC API Item Search spec. Supports q (free-text, multilingual), "
        "bbox, intersects, datetime, ids, collections, sortby, and cursor pagination."
    ),
    response_model_exclude_none=True,
)
async def post_search(request: Request, body: SearchBody) -> ItemCollection:
    _service = _get_search_service()
    return await _service.search_items(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Catalog Search – GET/POST
# ---------------------------------------------------------------------------

@router.get(
    "/catalogs",
    response_model=GenericCollection,
    summary="Search Catalogs.",
    response_model_exclude_none=True,
)
async def get_search_catalogs(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description."),
    ids: Optional[str] = Query(None, description="Comma-separated list of Catalog IDs."),
    limit: int = Query(10, ge=1, le=10_000),
    token: Optional[str] = Query(None),
) -> GenericCollection:
    body = CatalogSearchBody(
        q=q,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        limit=limit,
        token=token,
    )
    _service = _get_search_service()
    return await _service.search_catalogs(body, base_url=_base_url(request))



@router.post(
    "/catalogs",
    response_model=GenericCollection,
    summary="Search Catalogs (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_catalogs(request: Request, body: CatalogSearchBody) -> GenericCollection:
    _service = _get_search_service()
    return await _service.search_catalogs(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# Collection Search – GET/POST
# ---------------------------------------------------------------------------

@router.get(
    "/collections",
    response_model=GenericCollection,
    summary="Search Collections.",
    response_model_exclude_none=True,
)
async def get_search_collections(
    request: Request,
    q: Optional[str] = Query(None, description="Free-text query over id, title, description."),
    ids: Optional[str] = Query(None, description="Comma-separated list of Collection IDs."),
    limit: int = Query(10, ge=1, le=10_000),
    token: Optional[str] = Query(None),
) -> GenericCollection:
    body = CatalogSearchBody(
        q=q,
        ids=[i.strip() for i in ids.split(",")] if ids else None,
        limit=limit,
        token=token,
    )
    _service = _get_search_service()
    return await _service.search_collections(body, base_url=_base_url(request))



@router.post(
    "/collections",
    response_model=GenericCollection,
    summary="Search Collections (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_collections(request: Request, body: CatalogSearchBody) -> GenericCollection:
    _service = _get_search_service()
    return await _service.search_collections(body, base_url=_base_url(request))


# ---------------------------------------------------------------------------
# GeoID Lookup – Query the obfuscated index by geoid
# ---------------------------------------------------------------------------

@router.get(
    "/geoid/{geoid}",
    response_model=GeoidCollection,
    summary="Look up a single geoid in the obfuscated index.",
    description=(
        "Returns the catalog_id and collection_id for a geoid stored in "
        "an obfuscated index. Searches across all obfuscated catalogs unless "
        "catalog_id is specified."
    ),
    response_model_exclude_none=True,
)
async def get_geoid(
    request: Request,
    geoid: str,
    catalog_id: Optional[str] = Query(
        None,
        description="Restrict lookup to a single catalog's obfuscated index.",
    ),
) -> GeoidCollection:
    _service = _get_search_service()
    return await _service.search_by_geoid([geoid], catalog_id=catalog_id, limit=1)


@router.post(
    "/geoid",
    response_model=GeoidCollection,
    summary="Batch geoid lookup in the obfuscated index.",
    description=(
        "Look up one or more geoid values. Returns matching records from "
        "the obfuscated index ({geoid, catalog_id, collection_id}). "
        "Optionally restrict to a single catalog."
    ),
    response_model_exclude_none=True,
)
async def post_geoid(request: Request, body: GeoidSearchBody) -> GeoidCollection:
    _service = _get_search_service()
    return await _service.search_by_geoid(
        body.geoids, catalog_id=body.catalog_id, limit=body.limit,
    )


# ---------------------------------------------------------------------------
# Reindex – Admin-only bulk reindex triggers (POST only)
# ---------------------------------------------------------------------------

@router.post(
    "/reindex/catalogs/{catalog_id}",
    response_model=Dict[str, Any],
    summary="Trigger full catalog reindex (admin only).",
    description=(
        "Enqueues a bulk reindex task for all items in the catalog. "
        "When the catalog is configured with obfuscated=True, the task writes "
        "geoid-only documents to the obfuscated index. Otherwise, items are "
        "written to the STAC items index (collections with search_index=True "
        "only). Returns 202 with task_id."
    ),
    status_code=202,
)
async def post_reindex_catalog(
    request: Request,
    catalog_id: str,
    mode: Optional[Literal["catalog", "obfuscated"]] = Query(
        None,
        description="Reindex mode: 'catalog' or 'obfuscated'. Defaults to the catalog's indexer config.",
    ),
) -> Dict[str, Any]:
    _service = _get_search_service()
    return await _service.reindex_catalog(catalog_id, mode=mode)


@router.post(
    "/reindex/catalogs/{catalog_id}/collections/{collection_id}",
    response_model=Dict[str, Any],
    summary="Trigger single collection reindex (admin only).",
    description=(
        "Enqueues a bulk reindex task for one collection. "
        "Mode follows the same logic as the full-catalog endpoint."
    ),
    status_code=202,
)
async def post_reindex_collection(
    request: Request,
    catalog_id: str,
    collection_id: str,
    mode: Optional[Literal["catalog", "obfuscated"]] = Query(
        None,
        description="Reindex mode: 'catalog' or 'obfuscated'. Defaults to the catalog's indexer config.",
    ),
) -> Dict[str, Any]:
    _service = _get_search_service()
    return await _service.reindex_collection(catalog_id, collection_id, mode=mode)
