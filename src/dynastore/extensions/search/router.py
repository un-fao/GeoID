"""
FastAPI router implementing the STAC API Item Search specification.

GET  /search      – Simple filtering (bbox, datetime, intersects, limit, ids, collections, q, sortby)
POST /search      – Full-featured body-based filtering (same fields + token for pagination)
GET  /search/catalogs    – Keyword search over catalog index
POST /search/catalogs    – Body-based catalog search
GET  /search/collections – Keyword search over collection index
POST /search/collections – Body-based collection search

Conformance class: https://api.stacspec.org/v1.0.0/item-search
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Request

from .search_models import CatalogSearchBody, ItemCollection, GenericCollection, SearchBody
# Removed top-level SearchService import to avoid circular dependency

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Item Search"])

# We instantiate the service locally or via protocol discovery within endpoints
# to avoid circular imports.


def _base_url(request: Request) -> str:
    """Derive base URL (scheme + host) from request."""
    return str(request.base_url).rstrip("/")


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
    from .search_service import SearchService
    _service = SearchService()
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
    from .search_service import SearchService
    _service = SearchService()
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
    from .search_service import SearchService
    _service = SearchService()
    return await _service.search_catalogs(body, base_url=_base_url(request))



@router.post(
    "/catalogs",
    response_model=GenericCollection,
    summary="Search Catalogs (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_catalogs(request: Request, body: CatalogSearchBody) -> GenericCollection:
    from .search_service import SearchService
    _service = SearchService()
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
    from .search_service import SearchService
    _service = SearchService()
    return await _service.search_collections(body, base_url=_base_url(request))



@router.post(
    "/collections",
    response_model=GenericCollection,
    summary="Search Collections (full-featured body).",
    response_model_exclude_none=True,
)
async def post_search_collections(request: Request, body: CatalogSearchBody) -> GenericCollection:
    from .search_service import SearchService
    _service = SearchService()
    return await _service.search_collections(body, base_url=_base_url(request))
