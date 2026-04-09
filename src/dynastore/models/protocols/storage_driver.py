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
  - Capabilities: drivers declare what they support via ``FrozenSet[str]``
    using ``Capability`` string constants or any custom string.
  - Unified soft delete: ``delete_entities(soft=True)`` and
    ``drop_storage(soft=True)`` for recoverable operations.

Type contracts:
  - Write input: ``Feature | FeatureCollection | Dict[str, Any] | List[Dict]``
    (same union accepted by ``ItemCrudProtocol.upsert``).
  - Read output: ``AsyncIterator[Feature]`` — typed Pydantic models, not raw dicts.
  - Query input: ``QueryRequest`` — structured, not opaque dicts.
"""

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


class Capability:
    """Well-known capability constants. Drivers may declare any string.

    Usage::

        capabilities: FrozenSet[str] = frozenset({
            Capability.READ, Capability.WRITE, Capability.STREAMING,
        })

    Callers check membership::

        if Capability.WRITE not in driver.capabilities:
            raise ReadOnlyDriverError(...)
    """

    # --- I/O ---
    READ = "read"
    READ_ONLY = "read_only"
    WRITE = "write"
    STREAMING = "streaming"
    EXPORT = "export"

    # --- Query ---
    SPATIAL_FILTER = "spatial_filter"
    FULLTEXT = "fulltext"
    SORT = "sort"        # can sort results by arbitrary fields
    GROUP_BY = "group_by"  # can group/aggregate results

    # --- Data management ---
    SOFT_DELETE = "soft_delete"
    TIME_TRAVEL = "time_travel"
    VERSIONING = "versioning"
    SCHEMA_EVOLUTION = "schema_evolution"
    SNAPSHOTS = "snapshots"

    # --- Per-feature processing ---
    GEOSPATIAL = "geospatial"      # bbox, centroid, geometry validation/fix per row
    STATISTICS = "statistics"      # area, volume, length, morphological indices per row
    SPATIAL_INDEX = "spatial_index"  # H3/S2 indexing per row

    # --- Per-feature tracking & filtering ---
    ASSET_TRACKING = "asset_tracking"      # tracks asset_id per feature (source provenance)
    ATTRIBUTE_FILTER = "attribute_filter"  # can filter by feature attributes
    SOURCE_REFERENCE = "source_reference"  # provides source reference per feature

    # --- Cross-driver composition ---
    ENRICHMENT = "enrichment"  # can provide filter keys + extra attrs for cross-driver join

    # --- Write-time identity & versioning ---
    EXTERNAL_ID_TRACKING = "external_id_tracking"  # driver tracks external_id per feature
    TEMPORAL_VALIDITY = "temporal_validity"          # driver tracks valid_from / valid_to


@runtime_checkable
class CollectionStorageDriverProtocol(Protocol):
    """Entity-level storage abstraction for collection data.

    Each driver provides CRUD + lifecycle operations for a specific backend.
    The ``driver_id`` is used by routing config to select the active driver
    for a given collection.

    ``capabilities`` declares what the driver supports as a ``FrozenSet[str]``
    using ``Capability`` constants and/or custom strings.

    ``preferred_for`` declares which routing hints this driver is optimized for.
    The router uses this for auto-selection when no explicit hint mapping exists.

    ``supported_hints`` declares which hints this driver accepts in routing config.
    Config validation rejects entries with hints not in this set.
    """

    driver_id: str
    capabilities: FrozenSet[str]
    preferred_for: FrozenSet[str]
    supported_hints: FrozenSet[str]

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write/upsert entities. Returns written Feature models.

        Args:
            entities: One or more features to write.
            context: Runtime write context — carries ingestion-pipeline metadata
                that is not part of the feature payload itself:

                - ``asset_id``             — source asset URN (from ingestion)
                - ``external_id_override`` — explicit external_id bypassing field extraction
                - ``valid_from``           — validity range start (datetime or ISO-8601 str)
                - ``valid_to``             — validity range end (None = open-ended)

                Drivers that declare ``Capability.EXTERNAL_ID_TRACKING`` or
                ``Capability.TEMPORAL_VALIDITY`` MUST honour these keys and apply
                the ``CollectionWritePolicy`` (plugin_id ``"write_policy"``) retrieved
                from ``ConfigsProtocol``.

            db_resource: Optional connection/transaction to reuse (PG only).
        """
        ...

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """Stream entities as an async iterator (O(1) memory).

        Args:
            entity_ids: Fetch specific entities by ID.
            request: Structured query with filters, sorts, field selections.
            context: Driver-specific query context. Each driver documents what
                keys it expects (input) and what it publishes to
                ``FeaturePipelineContext`` (output). See driver docstrings.
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
        **kwargs: Any,
    ) -> None:
        """Ensure backing storage exists (create table/index/bucket/etc.).

        Each driver resolves its own connection and configuration internally
        (e.g. via ``ConfigsProtocol``). Drivers may accept driver-specific
        keyword arguments (e.g. ``physical_table``, ``layer_config``,
        ``db_resource`` for PostgreSQL).

        Args:
            catalog_id: The catalog that owns the collection.
            collection_id: Optional collection within the catalog.
            **kwargs: Driver-specific options.
        """
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

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return collection metadata managed by this driver.

        Returns a dict with fields like title, description, extent, keywords,
        license, providers, summaries, links, assets, item_assets, stac_version,
        stac_extensions, extra_metadata — or any subset thereof.

        Returns None if no metadata is stored for this collection.
        """
        ...

    async def set_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Store collection metadata in this driver's storage.

        Each driver persists in its own format:
        - PG: ``metadata`` table (UPSERT)
        - Iceberg: table properties via ``table.transaction().set_properties()``
        - DuckDB: sidecar JSON file or parquet metadata
        - ES: index settings/mappings or ``_meta`` field
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
