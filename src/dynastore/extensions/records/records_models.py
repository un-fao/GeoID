#    Copyright 2025 FAO
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

"""OGC API - Records Part 1 Pydantic models.

These models represent the wire format for the Records API.  Internally,
records are stored as ``Feature(geometry=None, properties={...})`` and
transformed at the API boundary by ``records_generator.py``.

Reference: OGC API - Records - Part 1: Core (OGC 20-004)
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.models.shared_models import Extent, Link


# ---------------------------------------------------------------------------
# Record core properties (OGC 20-004 clause 8)
# ---------------------------------------------------------------------------


class RecordTime(BaseModel):
    """Temporal information for a record (OGC 20-004 clause 8.2.7)."""

    date: Optional[str] = None
    timestamp: Optional[str] = None
    interval: Optional[List[Optional[str]]] = None


class RecordTheme(BaseModel):
    """Theme / category (OGC 20-004 clause 8.2.12)."""

    concepts: Optional[List[Dict[str, Any]]] = None
    scheme: Optional[str] = None


class RecordContact(BaseModel):
    """Contact information (OGC 20-004 clause 8.2.14)."""

    name: Optional[str] = None
    identifier: Optional[str] = None
    position: Optional[str] = None
    organization: Optional[str] = None
    logo: Optional[Dict[str, Any]] = None
    phones: Optional[List[Dict[str, Any]]] = None
    emails: Optional[List[str]] = None
    addresses: Optional[List[Dict[str, Any]]] = None
    links: Optional[List[Link]] = None
    contactInstructions: Optional[str] = None
    roles: Optional[List[str]] = None


# ---------------------------------------------------------------------------
# Record (GeoJSON Feature with Records core properties)
# ---------------------------------------------------------------------------


class RecordProperties(BaseModel):
    """Core record properties as defined in OGC API - Records Part 1."""

    model_config = ConfigDict(extra="allow")

    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    language: Optional[str] = None
    languages: Optional[List[str]] = None
    resourceLanguages: Optional[List[str]] = None
    externalIds: Optional[List[Dict[str, str]]] = None
    themes: Optional[List[RecordTheme]] = None
    formats: Optional[List[str]] = None
    contacts: Optional[List[RecordContact]] = None
    license: Optional[str] = None
    rights: Optional[str] = None
    created: Optional[str] = None
    updated: Optional[str] = None
    type: Optional[str] = Field(None, description="Record type (e.g. 'dimension-member').")
    time: Optional[RecordTime] = None


class Record(BaseModel):
    """OGC API - Records record (GeoJSON Feature envelope).

    ``geometry`` is always null for non-spatial records (e.g. dimension
    members, elevation bands, admin codes).
    """

    model_config = ConfigDict(extra="allow")

    type: str = "Feature"
    id: Optional[str] = None
    geometry: Optional[Any] = None
    properties: RecordProperties = Field(default_factory=RecordProperties)
    links: Optional[List[Link]] = None


class RecordCollection(BaseModel):
    """OGC API - Records record collection response."""

    type: str = "FeatureCollection"
    features: List[Record] = Field(default_factory=list)
    links: Optional[List[Link]] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None


# ---------------------------------------------------------------------------
# Catalog-level models (OGC 20-004 clause 7)
# ---------------------------------------------------------------------------


class RecordsCatalogCollection(BaseModel):
    """A collection descriptor within the Records API."""

    model_config = ConfigDict(extra="allow")

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: Optional[List[Link]] = None
    extent: Optional[Extent] = None
    itemType: str = Field("record", description="Item type is always 'record'.")
    crs: Optional[List[str]] = None
    keywords: Optional[List[str]] = None


class RecordsCatalogCollections(BaseModel):
    """List of record collections."""

    collections: List[RecordsCatalogCollection] = Field(default_factory=list)
    links: Optional[List[Link]] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None


class Conformance(BaseModel):
    """OGC API conformance response."""

    conformsTo: List[str]


class LandingPage(BaseModel):
    """Records API landing page."""

    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link] = Field(default_factory=list)
