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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
CollectionMetadataDriverProtocol — pluggable storage for collection metadata.

Mirrors the ``CollectionStorageDriverProtocol`` architecture:
  - Capability-gated operations (CQL filter, spatial filter, fulltext, etc.)
  - Extensible schema (metadata fields are not fixed — enrichers and users
    can add arbitrary fields)
  - Driver config via ``ConfigsProtocol`` waterfall
  - Multiple backends: PostgreSQL (always available), Elasticsearch (default,
    faster for JSON), or any future backend

Metadata drivers are discovered via entry points (``dynastore.metadata_drivers``)
and routed through ``MetadataRoutingConfig``.
"""

from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)


class MetadataCapability:
    """Well-known capability constants for metadata drivers.

    Usage::

        capabilities: FrozenSet[str] = frozenset({
            MetadataCapability.READ, MetadataCapability.WRITE,
            MetadataCapability.SEARCH,
        })
    """

    READ = "read"
    WRITE = "write"
    SEARCH = "search"                # keyword/fulltext search (q parameter)
    CQL_FILTER = "cql_filter"        # CQL2-JSON/Text filter support
    SOFT_DELETE = "soft_delete"       # mark deleted but retain
    SPATIAL_FILTER = "spatial_filter" # filter by extent bbox / geo_shape
    AGGREGATION = "aggregation"      # faceted counts, stats on metadata fields


@runtime_checkable
class CollectionMetadataDriverProtocol(Protocol):
    """Pluggable storage abstraction for collection metadata.

    Each driver provides CRUD + search operations for metadata using
    a specific backend.  The ``driver_id`` is used by routing config
    to select the active metadata driver for a catalog.

    ``capabilities`` declares what the driver supports.  Callers check
    membership before invoking capability-gated methods.

    ``description`` is a ``ClassVar[LocalizedText]`` defined statically in each
    driver class.  It is returned by the driver discovery API and must not be
    stored in the database.
    """

    driver_id: str
    capabilities: FrozenSet[str]
    # description: ClassVar[LocalizedText]  — declared in each concrete driver class

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata by ID.

        Returns a dict with STAC/OGC metadata fields (title, description,
        extent, etc.) or None if no metadata exists for this collection.
        """
        ...

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Write or update collection metadata.

        Drivers handle merge semantics internally (INSERT ON CONFLICT for PG,
        index/update for ES, etc.).
        """
        ...

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete collection metadata.

        Args:
            soft: If True and ``MetadataCapability.SOFT_DELETE`` is supported,
                mark as deleted but retain for recovery.
        """
        ...

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Search/list collection metadata.

        Args:
            q: Fulltext keyword search (requires ``SEARCH``).
            bbox: Spatial filter on extent (requires ``SPATIAL_FILTER``).
            datetime_range: Temporal filter on extent.
            filter_cql: CQL2-JSON filter dict (requires ``CQL_FILTER``).
            limit: Maximum results.
            offset: Skip count.

        Returns:
            Tuple of (results_list, total_count).
        """
        ...

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Fetch driver-specific config from the config waterfall."""
        ...

    async def is_available(self) -> bool:
        """Health check — used for fallback logic on first resolution."""
        ...
