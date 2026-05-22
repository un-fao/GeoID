"""Pydantic models for the geoid-extension item-search route.

These shapes are part of the customer-facing contract for
``POST /search/catalogs/{catalog_id}/items-search`` (PG-backed). The route
resolves an item within a catalog by exactly one of ``geoid`` or
``external_id`` and returns a :class:`GeoidCollection`.
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ItemsSearchBody(BaseModel):
    """Body for ``POST /search/catalogs/{catalog_id}/items-search`` (#1210).

    Resolve an item within the path catalog by **exactly one** of:

    * ``geoid`` — the platform-assigned id; resolved across all collections of
      the catalog (geoid is unique within a catalog).
    * ``external_id`` — the tenant's own id; resolved across all collections of
      the catalog unless ``collection_id`` narrows it to one. ``external_id``
      is not globally unique, so a cross-collection resolution may return more
      than one row.

    Supplying both ``geoid`` and ``external_id``, or neither, is a 400 — the
    route enforces the xor at the handler.
    """
    geoid: Optional[str] = Field(
        None, description="Platform-assigned geoid to resolve (xor external_id).",
    )
    external_id: Optional[str] = Field(
        None, description="Tenant's own item id to resolve (xor geoid).",
    )
    collection_id: Optional[str] = Field(
        None,
        description=(
            "Optional: narrow an external_id resolution to a single collection. "
            "When omitted, external_id is resolved across all collections of the "
            "catalog. Ignored when geoid is supplied."
        ),
    )
    limit: int = Field(
        10, ge=1, le=10_000, description="Maximum number of results to return.",
    )


class GeoidResult(BaseModel):
    """A single resolved item."""
    geoid: str
    catalog_id: str
    collection_id: str
    external_id: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    bbox: Optional[List[float]] = None
    properties: Optional[Dict[str, Any]] = None
    simplification_factor: Optional[float] = None
    simplification_mode: Optional[str] = None


class GeoidCollection(BaseModel):
    """Collection of resolved items."""
    type: str = "GeoidCollection"
    results: List[GeoidResult] = Field(default_factory=list)
    numberReturned: Optional[int] = None


__all__ = ["GeoidCollection", "GeoidResult", "ItemsSearchBody"]
