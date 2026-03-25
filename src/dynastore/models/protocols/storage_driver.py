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
Collection Storage Driver Protocol — entity-level storage abstraction.

Drivers implement this protocol to provide read/write/delete operations
on collections using any backend (PostgreSQL, Elasticsearch, DuckDB,
Iceberg, Neo4j, Spanner, BigQuery, Redis, static files, etc.).

Design principles:
  - Entity-level, not SQL-level: drivers work with typed Pydantic models.
  - Streaming-first (O(1) memory): ``read_entities`` returns ``AsyncIterator``.
  - Query via ``QueryRequest``: structured, validated queries with typed
    filters, sorts, and field selections.
  - Any driver can be primary or secondary — the framework is driver-agnostic.
  - Capabilities: drivers declare what they support via ``FrozenSet[str]``.
  - Unified soft delete: ``delete_entities(soft=True)`` and
    ``drop_storage(soft=True)`` for recoverable operations.

Type contracts:
  - Write input: ``Feature | FeatureCollection | Dict[str, Any] | List[Dict]``
    (same union accepted by ``ItemCrudProtocol.upsert``).
  - Read output: ``AsyncIterator[Feature]`` — typed Pydantic models, not raw dicts.
  - Query input: ``QueryRequest`` — structured, not opaque dicts.
"""

from enum import StrEnum
from typing import (
    Any,
    AsyncIterator,
    Dict,
    FrozenSet,
    List,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.query_builder import QueryRequest, QueryResponse


class Capability(StrEnum):
    """Standard capability tags. Drivers may add custom string tags."""

    READ_ONLY = "read_only"
    STREAMING = "streaming"
    SPATIAL_FILTER = "spatial_filter"
    FULLTEXT = "fulltext"
    EXPORT = "export"
    TIME_TRAVEL = "time_travel"
    SOFT_DELETE = "soft_delete"
    VERSIONING = "versioning"
    SCHEMA_EVOLUTION = "schema_evolution"
    SNAPSHOTS = "snapshots"


@runtime_checkable
class CollectionStorageDriverProtocol(Protocol):
    """Entity-level storage abstraction for collection data.

    Each driver provides CRUD + lifecycle operations for a specific backend.
    The ``driver_id`` is used by ``StorageRoutingConfig`` to select the
    active driver for a given collection.

    ``capabilities`` declares what the driver supports as a ``FrozenSet[str]``
    using ``Capability`` constants and/or custom strings.
    """

    driver_id: str
    capabilities: FrozenSet[str]

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write/upsert entities. Returns written Feature models."""
        ...

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """Stream entities as an async iterator (O(1) memory).

        Args:
            entity_ids: Fetch specific entities by ID.
            request: Structured query with filters, sorts, field selections.
            limit: Maximum entities to return.
            offset: Number of entities to skip.
            db_resource: Optional connection/transaction to reuse.

        Yields:
            ``Feature`` instances — typed Pydantic models.
        """
        ...

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Delete entities by ID. Returns count of entities deleted.

        Args:
            soft: If True, mark as deleted but retain data for recovery.
                Raises ``SoftDeleteNotSupportedError`` if the driver
                lacks ``Capability.SOFT_DELETE``.
        """
        ...

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """Ensure backing storage exists (create table/index/bucket/etc.)."""
        ...

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """Remove backing storage for a catalog or collection.

        Args:
            soft: If True, mark as deleted but retain data for recovery.
                Raises ``SoftDeleteNotSupportedError`` if the driver
                lacks ``Capability.SOFT_DELETE``.
        """
        ...

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        """Export entities to an interchange format. Returns path to exported data.

        Can be wrapped by ``TaskProtocol`` for async execution on task runners.
        """
        ...


@runtime_checkable
class StorageLocationResolver(Protocol):
    """Resolves physical storage coordinates for a catalog/collection.

    Every driver implements this. The resolver translates logical
    (catalog_id, collection_id) into driver-specific physical coordinates.
    """

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Return the physical storage coordinates for this driver + collection."""
        ...


class ReadOnlyDriverMixin:
    """Mixin for read-only drivers. Raises ``ReadOnlyDriverError`` on all writes."""

    async def write_entities(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )

    async def delete_entities(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )

    async def drop_storage(self, *args, **kwargs):
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        raise ReadOnlyDriverError(
            f"Driver '{getattr(self, 'driver_id', '?')}' is read-only"
        )
