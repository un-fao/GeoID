#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Pydantic models for the geoid-extension item-search route.

These shapes are part of the customer-facing contract for
``POST /search/catalogs/{catalog_id}/geoid-search`` (PG-backed). The route
resolves items within a catalog by exactly one of ``geoid`` or
``external_id`` and returns a :class:`GeoidCollection`.
"""
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class ItemsSearchBody(BaseModel):
    """Body for ``POST /search/catalogs/{catalog_id}/geoid-search`` (#1210).

    Resolve items within the path catalog by **exactly one** of:

    * ``geoid`` — the platform-assigned id(s). Accepts a single geoid **or an
      array of geoids**; each is resolved across all collections of the catalog
      (geoid is unique within a catalog), so ``collection_id`` is not needed.
      When resolving many at once, raise ``limit`` to return them all — results
      are capped at ``limit`` and geoids with no match are silently skipped.
    * ``external_id`` — the tenant's own id; resolved **within a single named
      ``collection_id``**, which is required when ``external_id`` is supplied.
      external_id is not globally unique, so resolving it without a collection
      would mean a cross-collection scan — disallowed here to keep the public
      lookup a targeted, single-collection resolve (un-fao/GeoID#1204 R2).

    Supplying both ``geoid`` and ``external_id``, neither, or ``external_id``
    without ``collection_id`` is a 400 — the route enforces these at the
    handler.
    """
    geoid: Optional[Union[str, List[str]]] = Field(
        None,
        description=(
            "Platform-assigned geoid, or an array of geoids, to resolve "
            "catalog-wide (xor external_id)."
        ),
    )
    external_id: Optional[str] = Field(
        None,
        description="Tenant's own item id to resolve (xor geoid); requires collection_id.",
    )
    collection_id: Optional[str] = Field(
        None,
        description=(
            "The collection that owns the external_id. Required when external_id "
            "is supplied; ignored when geoid is supplied."
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
