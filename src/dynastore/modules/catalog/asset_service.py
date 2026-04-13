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
Asset lifecycle management for the DynaStore catalog.

Overview
--------
An **asset** is a pointer to a file (local path, GCS URI, HTTP URL, S3 key, …)
registered in the catalog database.  Assets are the central linking mechanism:

- **Ingestion tasks** create an asset from a source-file URI, then write
  feature rows into the collection's physical table keyed by ``asset_id``.
- **GCS/S3 uploads** create assets automatically when the storage event fires
  (``GcsStorageEventTask`` / future ``S3EventTask``).
- **Direct API calls** create assets immediately via
  ``POST /assets/catalogs/{id}`` or the collection-scoped variant.

Key models
----------
``AssetBase``
    Input/creation shape.  ``owned_by`` marks which storage backend owns the
    underlying file; assets with this field set are protected from hard-deletion
    while non-cascading references remain active.

``Asset``
    Fully persisted asset returned by reads, including ``catalog_id``,
    ``collection_id``, and audit timestamps.

``AssetReference``
    A dependency row in ``{schema}.asset_references`` that links an asset to a
    referencing entity (collection, DuckDB table, Iceberg table, …).  The
    ``cascade_delete`` flag controls deletion safety:

    * ``True`` — informational; the referring driver handles its own cleanup
      (e.g. the PostgreSQL ``trg_asset_cleanup`` trigger).  **Does not block**
      hard-deletion.
    * ``False`` — protective; hard-deletion of the asset is **blocked**
      (raises ``AssetReferencedError`` → HTTP 409) until this reference is
      explicitly removed.

Reference type extension
------------------------
Each driver module defines its own ``AssetReferenceType`` subclass so values
are namespaced and type-safe::

    # In dynastore/modules/duckdb/models.py
    from dynastore.models.shared_models import AssetReferenceType

    class DuckDbReferenceType(AssetReferenceType):
        TABLE = "duckdb:table"

    # When creating a DuckDB-backed collection:
    await assets.add_asset_reference(
        asset_id=asset_id,
        catalog_id=catalog_id,
        ref_type=DuckDbReferenceType.TABLE,
        ref_id=table_name,
        cascade_delete=False,   # DuckDB cannot auto-cascade → blocks deletion
    )

    # On collection drop — must happen BEFORE the hard-delete attempt:
    await assets.remove_asset_reference(
        asset_id=asset_id,
        catalog_id=catalog_id,
        ref_type=DuckDbReferenceType.TABLE,
        ref_id=table_name,
    )

Ingestion (PostgreSQL, cascade-safe)
-------------------------------------
::

    from dynastore.modules.catalog.models import CoreAssetReferenceType

    # After successful ingestion the DB trigger already handles row cleanup,
    # so the reference is informational only (cascade_delete=True):
    await asset_manager.add_asset_reference(
        asset_id=asset.asset_id,
        catalog_id=catalog_id,
        ref_type=CoreAssetReferenceType.COLLECTION,
        ref_id=collection_id,
        cascade_delete=True,
        db_resource=engine,
    )

Upload flow (GCS)
-----------------
::

    from dynastore.models.protocols import AssetUploadProtocol
    from dynastore.modules import get_protocol

    upload = get_protocol(AssetUploadProtocol)
    ticket = await upload.initiate_upload(
        catalog_id="imagery_catalog",
        asset_def=AssetUploadDefinition(
            asset_id="LC09_198030_20251225",
            asset_type=AssetTypeEnum.RASTER,
            metadata={"sensor": "OLI-2"},
        ),
        filename="LC09_L1TP_198030_20251225_02_T1.tif",
        content_type="image/tiff",
        collection_id="landsat_scenes",
    )
    # → ticket.upload_url is a GCS signed resumable PUT URL
    # → PUT file to ticket.upload_url with ticket.headers
    # → GCS fires OBJECT_FINALIZE → GcsStorageEventTask → create_asset(owned_by="gcs")
    # → poll GET /assets/catalogs/{id}/upload/{ticket.ticket_id}/status

Deletion guard
--------------
Hard-deletion of an ``owned_by`` asset with blocking references raises
``AssetReferencedError`` (caught by the API layer → HTTP 409)::

    # Will succeed — soft delete never checks references:
    await assets.delete_assets(catalog_id, asset_id=id, hard=False)

    # Will raise AssetReferencedError if cascade_delete=False refs remain:
    await assets.delete_assets(catalog_id, asset_id=id, hard=True)

    # Inspect blocking references before retrying:
    refs = await assets.list_asset_references(asset_id=id, catalog_id=catalog_id)
    blocking = [r for r in refs if not r.cascade_delete]
"""

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Union, Callable, Annotated
from sqlalchemy import text
from dynastore.tools.cache import cached
from pydantic import BaseModel, Field, ConfigDict

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
    DbConnection,
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.catalog.models import AssetReferenceType, CoreAssetReferenceType, EventType
from dynastore.models.protocols.assets import AssetsProtocol
from dynastore.models.protocols.asset_driver import AssetDriverProtocol
from dynastore.models.query_builder import FilterOperator
from enum import Enum

logger = logging.getLogger(__name__)

# --- Asset-Specific Enums ---

CATALOG_LEVEL_COLLECTION_ID = "_catalog_"


class AssetTypeEnum(str, Enum):
    VECTORIAL = "VECTORIAL"
    RASTER = "RASTER"
    ASSET = "ASSET"


class AssetEventType(EventType):
    ASSET_CREATED = "asset_created"
    ASSET_UPDATED = "asset_updated"
    ASSET_DELETED = "asset_deleted"
    ASSET_HARD_DELETED = "asset_hard_deleted"
    ASSET_MAP_LINKED = "asset_map_linked"


# --- Asset Models ---


class AssetBase(BaseModel):
    """
    Core fields required to create or represent an asset.

    ``owned_by`` semantics
    ~~~~~~~~~~~~~~~~~~~~~~
    When set, ``owned_by`` declares that a storage backend (``"gcs"``,
    ``"local"``, ``"http"``, …) manages the underlying file.  This activates
    the deletion guard: if any ``AssetReference`` with ``cascade_delete=False``
    is active, ``delete_assets(hard=True)`` raises ``AssetReferencedError``
    (HTTP 409) instead of removing the row.

    Assets created by ingestion tasks (not file-owned) should leave
    ``owned_by=None`` so the existing PostgreSQL trigger cascade works without
    interference from the reference guard.

    Examples::

        # File uploaded to GCS — owned by the GCS backend
        AssetBase(
            asset_id="scene_20251225",
            uri="gs://my-bucket/landsat/scene_20251225.tif",
            asset_type=AssetTypeEnum.RASTER,
            metadata={"sensor": "OLI-2"},
            owned_by="gcs",
        )

        # Ingestion source file on local disk — NOT file-owned
        AssetBase(
            asset_id="stations_2025",
            uri="/data/uploads/stations_2025.csv",
            asset_type=AssetTypeEnum.ASSET,
            metadata={"year": 2025},
        )
    """

    asset_id: str = Field(..., description="Unique logical identifier for the asset.")
    uri: str = Field(
        ...,
        description="URI pointing to the asset location (e.g., gs://bucket/path/to/asset.tif).",
    )
    asset_type: AssetTypeEnum = Field(
        default=AssetTypeEnum.ASSET,
        description="Type of the asset. Could be VECTORIAL, RASTER, or generic ASSET.",
        examples=["VECTORIAL", "RASTER", "ASSET"],
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata associated with the asset.",
        examples=[{"owner": "", "provider": ""}],
    )
    owned_by: Optional[str] = Field(
        default=None,
        description=(
            "Identifier of the system that manages the underlying file "
            "(e.g. 'gcs', 'local', 'http'). "
            "Assets with this field set are protected from hard-deletion while "
            "non-cascading references (cascade_delete=False) remain active."
        ),
        examples=["gcs", "local", "http", None],
    )

    model_config = ConfigDict(populate_by_name=True)


class AssetUpdate(BaseModel):
    """Mutable fields for an Asset."""

    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary metadata for the asset."
    )


class AssetUploadDefinition(BaseModel):
    """
    Asset metadata supplied at upload-initiation time when the URI is not yet known.

    The backend fills in the ``uri`` after receiving the file and then calls
    ``AssetsProtocol.create_asset`` with a fully formed ``AssetBase``.

    Example::

        AssetUploadDefinition(
            asset_id="gadm_adm2_italy",
            asset_type=AssetTypeEnum.VECTORIAL,
            metadata={"source": "GADM", "version": "4.1", "country": "ITA"},
        )
    """

    asset_id: str = Field(..., description="Unique logical identifier for the asset.")
    asset_type: AssetTypeEnum = Field(
        AssetTypeEnum.ASSET, description="Type of the asset."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary metadata for the asset."
    )

    model_config = ConfigDict(populate_by_name=True)


class AssetReference(BaseModel):
    """
    Tracks a dependency between an asset and a referencing entity.

    Two modes
    ~~~~~~~~~
    ``cascade_delete=True`` — **informational**.  The referring driver handles
    its own cleanup (e.g. the PostgreSQL ``trg_asset_cleanup`` trigger cascades
    feature-row deletion automatically).  This reference **does not block**
    hard-deletion of the asset.

    ``cascade_delete=False`` — **protective**.  Hard-deletion of the asset is
    **blocked** (raises ``AssetReferencedError`` → HTTP 409) while this
    reference is active.  Use this for drivers that cannot auto-cascade (e.g.
    DuckDB, Iceberg, HTTP remote files).

    Typical usage
    ~~~~~~~~~~~~~
    Ingestion (PostgreSQL, cascade-safe)::

        await assets.add_asset_reference(
            asset_id="stations_2025",
            catalog_id="my_catalog",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="weather_stations",
            cascade_delete=True,   # DB trigger already handles row cleanup
        )

    DuckDB-backed collection (non-cascading)::

        # On collection creation:
        await assets.add_asset_reference(
            asset_id="parquet_file_id",
            catalog_id="my_catalog",
            ref_type=DuckDbReferenceType.TABLE,   # "duckdb:table"
            ref_id="weather_stations_duckdb",
            cascade_delete=False,  # blocks deletion until the table is dropped
        )

        # On collection deletion (MUST precede the hard-delete attempt):
        await assets.remove_asset_reference(
            asset_id="parquet_file_id",
            catalog_id="my_catalog",
            ref_type=DuckDbReferenceType.TABLE,
            ref_id="weather_stations_duckdb",
        )
    """

    asset_id: str = Field(..., description="The referenced asset ID.")
    catalog_id: str = Field(..., description="Catalog scope of the asset.")
    ref_type: Union[AssetReferenceType, str] = Field(
        ...,
        description=(
            "Pluggable reference kind (e.g. 'collection', 'duckdb:table'). "
            "Use namespaced strings to avoid collisions between modules. "
            "Raw strings are accepted for forward-compat with driver types "
            "unknown at parse time (e.g. records read back from the database)."
        ),
    )
    ref_id: str = Field(
        ...,
        description=(
            "Owner-scoped identifier of the referencing entity "
            "(e.g. collection_id, DuckDB table name)."
        ),
    )
    cascade_delete: bool = Field(
        default=True,
        description=(
            "If True: the referring driver handles cleanup on asset deletion. "
            "If False: hard-deletion is BLOCKED while this reference is active."
        ),
    )
    created_at: datetime = Field(..., description="Timestamp when the reference was registered.")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AssetReferencedError(ValueError):
    """
    Raised when a hard-delete of an owned asset is attempted while one or more
    ``cascade_delete=False`` references remain active.

    Caught by the API layer and converted to HTTP 409 Conflict.
    """

    def __init__(self, asset_id: str, blocking_refs: List[AssetReference]) -> None:
        self.asset_id = asset_id
        self.blocking_refs = blocking_refs
        refs_summary = ", ".join(
            f"{r.ref_type}:{r.ref_id}" for r in blocking_refs
        )
        super().__init__(
            f"Asset '{asset_id}' cannot be hard-deleted: "
            f"{len(blocking_refs)} blocking reference(s) remain — {refs_summary}. "
            "Remove the referencing entities first or use soft-delete."
        )


class Asset(AssetBase):
    """Fully formed Asset retrieved from DB."""

    # asset_id inherited from AssetBase (str)
    catalog_id: Annotated[str, Field(description="The catalog ID.")]
    collection_id: Annotated[
        Optional[str], Field(None, description="The collection ID.")
    ]
    created_at: datetime
    deleted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# --- Dynamic Query Helpers ---




class AssetFilter(BaseModel):
    field: str  # asset_id, uri, metadata.path.to.key
    op: FilterOperator = FilterOperator.EQ
    value: Any


# --- Dynamic Query Helpers ---
# Moved inside methods to allow schema injection

# Obsolete global DDL removed.
# Assets correspond to {schema}.assets in the tenant schema.


class AssetService(AssetsProtocol):
    """
    SQL-backed implementation of ``AssetsProtocol``.

    Provides full asset lifecycle management against a PostgreSQL tenant schema:
    CRUD, soft/hard delete with reference guard, paginated list/search, and
    the ``asset_references`` tracking table for cross-driver dependency management.

    The ``db_resource`` parameter is an **internal** optional kwarg preserved for
    intra-module transactional calls (e.g. ingestion passing an open engine).
    It is NOT part of the ``AssetsProtocol`` contract and must not be called
    from outside the catalog module.

    Schema layout
    ~~~~~~~~~~~~~
    ::

        {catalog_schema}.assets
          asset_id       VARCHAR  PK
          catalog_id     VARCHAR
          collection_id  VARCHAR  (NULL for catalog-level assets)
          uri            TEXT
          asset_type     VARCHAR
          metadata       JSONB
          owned_by       VARCHAR  (NULL = not file-owned; set = deletion guard active)
          created_at     TIMESTAMPTZ
          updated_at     TIMESTAMPTZ
          deleted_at     TIMESTAMPTZ  (soft-delete sentinel)

        {catalog_schema}.asset_references
          asset_id       VARCHAR  FK → assets
          catalog_id     VARCHAR
          ref_type       VARCHAR  (namespaced enum value, e.g. 'collection', 'duckdb:table')
          ref_id         VARCHAR  (collection_id, table_name, …)
          cascade_delete BOOLEAN  DEFAULT TRUE
          created_at     TIMESTAMPTZ
          PRIMARY KEY (catalog_id, asset_id, ref_type, ref_id)
          PARTIAL INDEX  on (catalog_id, asset_id) WHERE cascade_delete = FALSE
    """

    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        event_emitter: Optional[Callable] = None,
    ):
        self.engine = engine
        self._event_emitter = event_emitter

        # Instance-bound cache for assets.
        # TTL=60 s prevents stale reads across multi-worker deployments where
        # another process may have updated the row. Jitter spreads expiry to
        # avoid a thundering herd when many keys were warmed at the same time.
        self.get_asset_cached = cached(
            maxsize=128, ttl=60, jitter=5, namespace="assets"
        )(self._get_asset_db)

    def is_available(self) -> bool:
        """Returns True if the manager is initialized and ready."""
        return self.engine is not None

    async def _resolve_schema(
        self, catalog_id: str, db_resource: DbResource
    ) -> Optional[str]:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )

    def _get_partition_name(self, catalog_id: str, collection_id: str) -> str:
        """Generates the partition name for a given catalog and collection."""
        # Standardize: remove dots or other non-identifier chars if necessary, but here we assume safe
        return f"assets_{catalog_id}_{collection_id}"

    # --- Retrieval & Advanced Search ---

    async def _get_asset_db(
        self, catalog_id: str, asset_id: str, collection_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch asset dict from the configured read driver (cached path)."""
        from dynastore.modules.storage.router import get_asset_driver
        driver = await get_asset_driver("READ", catalog_id, collection_id)
        return await driver.get_asset(
            catalog_id, asset_id,
            collection_id=collection_id,
            db_resource=self.engine,
        )

    async def _apply_enricher_pipeline(
        self,
        asset_doc: Dict[str, Any],
        catalog_id: str,
        collection_id: Optional[str],
    ) -> Dict[str, Any]:
        """Apply AssetEnricherProtocol pipeline to an asset dict."""
        from dynastore.tools.discovery import get_protocols
        from dynastore.models.protocols.asset_enricher import AssetEnricherProtocol
        try:
            enrichers = sorted(
                get_protocols(AssetEnricherProtocol), key=lambda e: e.priority
            )
            for enricher in enrichers:
                try:
                    if enricher.can_enrich(catalog_id, collection_id):
                        asset_doc = await enricher.enrich_asset(
                            catalog_id, asset_doc, context={}
                        )
                except Exception as err:
                    logger.warning(
                        "AssetEnricher '%s' failed for %s/%s: %s",
                        getattr(enricher, "enricher_id", "?"),
                        catalog_id, collection_id, err,
                    )
        except Exception:
            pass
        return asset_doc

    async def _get_secondary_drivers(
        self, catalog_id: str, collection_id: Optional[str]
    ) -> List["ResolvedDriver"]:
        """Return non-primary WRITE asset drivers for fan-out.

        Uses ``AssetRoutingPluginConfig`` via the router — the primary
        (first) WRITE driver is excluded since the caller handles it
        separately.

        Returns :class:`ResolvedDriver` instances (preserving ``on_failure``
        and ``write_mode``) instead of raw driver objects.
        """
        from dynastore.modules.storage.router import get_asset_write_drivers

        try:
            resolved = await get_asset_write_drivers(catalog_id, collection_id)
            # Skip the first (primary) — caller already handles it
            return resolved[1:]
        except Exception:
            return []

    async def _fan_out_asset_writes(
        self,
        catalog_id: str,
        collection_id: Optional[str],
        asset_doc: Dict[str, Any],
        method_name: str,
    ) -> None:
        """Fan-out asset writes to secondary drivers, respecting on_failure + write_mode.

        Args:
            asset_doc: The asset document to write.
            method_name: Driver method to call (e.g. ``"index_asset"``).
        """
        import asyncio
        from dynastore.modules.storage.routing_config import FailurePolicy, WriteMode

        secondaries = await self._get_secondary_drivers(catalog_id, collection_id)
        if not secondaries:
            return

        sync_drivers = [r for r in secondaries if r.write_mode == WriteMode.SYNC]
        async_drivers = [r for r in secondaries if r.write_mode == WriteMode.ASYNC]

        # Sync phase: parallel writes
        if sync_drivers:
            tasks = [
                getattr(r.driver, method_name)(catalog_id, asset_doc)
                for r in sync_drivers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r, result in zip(sync_drivers, results):
                if isinstance(result, BaseException):
                    if r.on_failure == FailurePolicy.FATAL:
                        raise result if isinstance(result, Exception) else RuntimeError(str(result))
                    elif r.on_failure == FailurePolicy.WARN:
                        logger.warning(
                            "Secondary driver '%s' %s failed for %s: %s",
                            r.driver_id, method_name, catalog_id, result,
                        )

        # Async phase: fire-and-forget
        for r in async_drivers:
            asyncio.create_task(
                self._async_asset_write(r, catalog_id, asset_doc, method_name)
            )

    async def _async_asset_write(
        self,
        resolved: "ResolvedDriver",
        catalog_id: str,
        asset_doc: Dict[str, Any],
        method_name: str,
    ) -> None:
        """Fire-and-forget wrapper for async asset writes."""
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            await getattr(resolved.driver, method_name)(catalog_id, asset_doc)
        except Exception as err:
            if resolved.on_failure == FailurePolicy.FATAL:
                logger.error(
                    "Async asset driver '%s' %s FATAL failure for %s: %s",
                    resolved.driver_id, method_name, catalog_id, err,
                )
            elif resolved.on_failure == FailurePolicy.WARN:
                logger.warning(
                    "Async asset driver '%s' %s failed for %s: %s",
                    resolved.driver_id, method_name, catalog_id, err,
                )

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        """Get asset by ID, routing through the configured read driver."""
        from dynastore.modules.storage.router import get_asset_driver
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID

        if db_resource:
            driver = await get_asset_driver("READ", catalog_id, collection_id)
            asset_doc = await driver.get_asset(
                catalog_id, asset_id,
                collection_id=target_col_id,
                db_resource=db_resource,
            )
        else:
            asset_doc = await self.get_asset_cached(catalog_id, asset_id, target_col_id)

        if not asset_doc:
            return None

        asset_doc = await self._apply_enricher_pipeline(
            dict(asset_doc), catalog_id, collection_id
        )
        asset = Asset.model_validate(asset_doc)
        if asset.collection_id == CATALOG_LEVEL_COLLECTION_ID:
            asset.collection_id = None
        return asset

    # Deprecated alias methods for backward compatibility if needed, but we should switch to get_asset using internal logic
    async def get_asset_by_code(
        self,
        catalog_id: str,
        asset_code: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        # 'code' is now 'asset_id'. This is just an alias.
        return await self.get_asset(catalog_id, asset_code, collection_id, db_resource)

    # _get_asset_by_code_cached removed as it's redundant with _get_asset_cached

    async def list_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Asset]:
        from dynastore.modules.storage.router import get_asset_driver
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        driver = await get_asset_driver("READ", catalog_id, collection_id)
        docs = await driver.search_assets(
            catalog_id,
            collection_id=target_col_id,
            limit=limit,
            offset=offset,
            db_resource=db_resource or self.engine,
        )
        assets = []
        for doc in docs:
            enriched = await self._apply_enricher_pipeline(
                dict(doc), catalog_id, collection_id
            )
            asset = Asset.model_validate(enriched)
            if asset.collection_id == CATALOG_LEVEL_COLLECTION_ID:
                asset.collection_id = None
            assets.append(asset)
        return assets

    async def search_assets(
        self,
        catalog_id: str,
        filters: List[AssetFilter],
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Asset]:
        """
        Performs a granular search across assets using a list of filters.

        EQ-only filters are routed through the hint="search" driver (ES when
        configured).  Filters with other operators fall back to the default
        driver (PG) which supports full operator coverage via SQL.
        """
        from dynastore.modules.storage.router import get_asset_driver
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID

        # Build simple field=value dict for EQ-only filters (ES-compatible)
        eq_query: Dict[str, Any] = {}
        has_complex_filter = False
        for f in filters:
            if f.op == FilterOperator.EQ:
                eq_query[f.field] = f.value
            else:
                has_complex_filter = True
                break

        if not has_complex_filter:
            # Route through the search driver (ES if configured), fall back to READ
            try:
                driver = await get_asset_driver("SEARCH", catalog_id, collection_id, hint="search")
            except ValueError:
                driver = await get_asset_driver("READ", catalog_id, collection_id)
            docs = await driver.search_assets(
                catalog_id,
                collection_id=target_col_id,
                query=eq_query or None,
                limit=limit,
                offset=offset,
                db_resource=db_resource or self.engine,
            )
            assets = []
            for doc in docs:
                enriched = await self._apply_enricher_pipeline(
                    dict(doc), catalog_id, collection_id
                )
                asset = Asset.model_validate(enriched)
                if asset.collection_id == CATALOG_LEVEL_COLLECTION_ID:
                    asset.collection_id = None
                assets.append(asset)
            return assets

        # Complex filters: fall back to default driver (PG SQL with full operators)
        driver = await get_asset_driver("READ", catalog_id, collection_id)

        op_map = {
            FilterOperator.EQ: "=",
            FilterOperator.NE: "!=",
            FilterOperator.GT: ">",
            FilterOperator.GTE: ">=",
            FilterOperator.LT: "<",
            FilterOperator.LTE: "<=",
            FilterOperator.LIKE: "LIKE",
            FilterOperator.ILIKE: "ILIKE",
            FilterOperator.IN: "IN",
        }

        # Build PG-compatible query dict with operator encoding for pg_asset_driver
        pg_query: Dict[str, Any] = {}
        for i, f in enumerate(filters):
            if f.field.startswith("metadata."):
                pg_query[f.field] = f.value
            elif f.field == "id":
                pg_query["asset_id"] = f.value
            else:
                validate_sql_identifier(f.field)
                pg_query[f.field] = f.value

        docs = await driver.search_assets(
            catalog_id,
            collection_id=target_col_id,
            query=pg_query,
            limit=limit,
            offset=offset,
            db_resource=db_resource or self.engine,
        )
        assets = []
        for doc in docs:
            enriched = await self._apply_enricher_pipeline(
                dict(doc), catalog_id, collection_id
            )
            asset = Asset.model_validate(enriched)
            if asset.collection_id == CATALOG_LEVEL_COLLECTION_ID:
                asset.collection_id = None
            assets.append(asset)
        return assets

    # --- Lifecycle ---

    async def create_asset(
        self,
        catalog_id: str,
        asset: AssetBase,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Asset:
        from dynastore.modules.storage.router import get_asset_driver
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        now = datetime.now(timezone.utc)

        asset_doc: Dict[str, Any] = {
            "asset_id": asset.asset_id,
            "catalog_id": catalog_id,
            "collection_id": target_col_id,
            "asset_type": asset.asset_type.value,
            "uri": asset.uri,
            "created_at": now,
            "deleted_at": None,
            "metadata": asset.metadata,
            "owned_by": asset.owned_by,
        }

        write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
        await write_driver.index_asset(catalog_id, asset_doc, db_resource=db_resource)

        # Fan-out to secondary drivers respecting on_failure + write_mode
        await self._fan_out_asset_writes(
            catalog_id, collection_id, asset_doc, "index_asset",
        )

        # Fetch canonical state from the write driver (captures DB-set timestamps)
        fetched_doc = await write_driver.get_asset(
            catalog_id, asset.asset_id,
            collection_id=target_col_id,
            db_resource=db_resource,
        )
        created = Asset.model_validate(fetched_doc or asset_doc)
        if created.collection_id == CATALOG_LEVEL_COLLECTION_ID:
            created.collection_id = None

        self._invalidate_cache(created.asset_id, catalog_id, target_col_id)

        if self._event_emitter:
            await self._event_emitter(
                AssetEventType.ASSET_CREATED, created.model_dump()
            )
        return created

    async def update_asset(
        self,
        catalog_id: str,
        asset_id: str,
        update: AssetUpdate,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Asset:
        """Updates an existing asset's metadata via the configured write driver."""
        from dynastore.modules.storage.router import get_asset_driver
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID

        # Fetch current asset
        current = await self.get_asset(
            asset_id=asset_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        if not current:
            raise ValueError(
                f"Asset '{asset_id}' not found in catalog '{catalog_id}'."
            )

        # Build updated doc
        updated_doc: Dict[str, Any] = current.model_dump()
        updated_doc["metadata"] = update.metadata
        updated_doc["collection_id"] = target_col_id

        write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
        await write_driver.index_asset(catalog_id, updated_doc, db_resource=db_resource)

        # Fan-out to secondary drivers respecting on_failure + write_mode
        await self._fan_out_asset_writes(
            catalog_id, collection_id, updated_doc, "index_asset",
        )

        # Fetch canonical state
        fetched_doc = await write_driver.get_asset(
            catalog_id, asset_id,
            collection_id=target_col_id,
            db_resource=db_resource,
        )
        updated = Asset.model_validate(fetched_doc or updated_doc)
        if updated.collection_id == CATALOG_LEVEL_COLLECTION_ID:
            updated.collection_id = None

        self._invalidate_cache(updated.asset_id, catalog_id, target_col_id)

        if self._event_emitter:
            await self._event_emitter(
                AssetEventType.ASSET_UPDATED, updated.model_dump()
            )
        return updated

    def _invalidate_cache(self, asset_id: str, catalog_id: str, collection_id: str):
        """Invalidates related cache entries."""
        # Signature of _get_asset_db is (catalog_id, asset_id, collection_id)
        self.get_asset_cached.cache_invalidate(catalog_id, asset_id, collection_id)

    async def delete_assets(
        self,
        catalog_id: str,
        asset_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        hard: bool = False,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        """Delete assets matching the given criteria.

        Uses the PG driver for candidate lookup, reference guard, and the
        canonical delete.  Hard-deletes are fanned out to non-PG write driver
        and secondary drivers.
        """
        from dynastore.modules.storage.router import get_asset_driver
        from dynastore.modules.catalog.drivers.pg_asset_driver import DriverAssetPostgresql

        pg_driver = DriverAssetPostgresql(engine=db_resource or self.engine)
        rowcount, rows_or_blocking = await pg_driver.delete_assets_bulk(
            catalog_id,
            asset_id=asset_id,
            collection_id=collection_id,
            hard=hard,
            db_resource=db_resource,
        )

        # rowcount == -1 means blocking references were found
        if rowcount == -1:
            blocking_rows = [AssetReference.model_validate(r) for r in rows_or_blocking]
            first_asset_id = blocking_rows[0].asset_id
            asset_blocking = [
                r for r in blocking_rows if r.asset_id == first_asset_id
            ]
            raise AssetReferencedError(first_asset_id, asset_blocking)

        if rowcount == 0:
            return 0

        asset_rows = rows_or_blocking

        # Invalidate cache
        for a in asset_rows:
            self._invalidate_cache(a["asset_id"], a["catalog_id"], a["collection_id"])

        # Fan-out hard-deletes to write driver (if non-PG) and secondary drivers
        if hard:
            from dynastore.modules.storage.routing_config import FailurePolicy, WriteMode

            write_driver = await get_asset_driver("WRITE", catalog_id, collection_id)
            secondaries = await self._get_secondary_drivers(catalog_id, collection_id)

            # Build drivers to notify: non-PG primary + all secondaries
            drivers_to_delete = []
            from dynastore.modules.catalog.drivers.pg_asset_driver import (
                DriverAssetPostgresql,
            )
            if not isinstance(write_driver, DriverAssetPostgresql):
                from dynastore.modules.storage.router import ResolvedDriver
                drivers_to_delete.append(
                    ResolvedDriver(driver=write_driver, on_failure=FailurePolicy.WARN)
                )
            drivers_to_delete.extend(secondaries)

            for a in asset_rows:
                for resolved in drivers_to_delete:
                    try:
                        await resolved.driver.delete_asset(
                            a["catalog_id"], a["asset_id"]
                        )
                    except Exception as err:
                        if resolved.on_failure == FailurePolicy.FATAL:
                            raise
                        elif resolved.on_failure == FailurePolicy.WARN:
                            logger.warning(
                                "Driver '%s' delete_asset failed for %s/%s: %s",
                                resolved.driver_id, a["catalog_id"], a["asset_id"], err,
                            )

        # Emit events
        if self._event_emitter:
            event_type = (
                AssetEventType.ASSET_HARD_DELETED if hard
                else AssetEventType.ASSET_DELETED
            )
            for a in asset_rows:
                await self._event_emitter(event_type, dict(a))

        return rowcount

    async def soft_delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            asset_id=asset_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )

    async def hard_delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            asset_id=asset_id,
            hard=True,
            propagate=propagate,
            db_resource=db_resource,
        )

    async def soft_delete_collection_assets(
        self,
        catalog_id: str,
        collection_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id,
            collection_id=collection_id,
            propagate=propagate,
            db_resource=db_resource,
        )

    async def soft_delete_catalog_assets(
        self,
        catalog_id: str,
        propagate: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        return await self.delete_assets(
            catalog_id, propagate=propagate, db_resource=db_resource
        )

    async def ensure_asset_cleanup_trigger(
        self,
        schema: str,
        table: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """
        Ensures that the asset cleanup trigger is present on the specified table.
        This includes ensuring the 'asset_cleanup' function exists in the tenant schema.
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            # 1. Check if asset_id column exists
            check_sql = """
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = :schema 
              AND table_name = :table 
              AND column_name = 'asset_id'
            """
            has_asset_id = await DQLQuery(
                check_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(conn, schema=schema, table=table)
            
            if not has_asset_id:
                logger.debug(f"Skipping asset cleanup trigger for {schema}.{table}: column 'asset_id' not found.")
                return

            # 2. Ensure the stored procedure exists in this schema
            from .db_init.stored_procedures import ensure_stored_procedures
            await ensure_stored_procedures(conn)

            # 3. Derive Hub Physical Table Name
            # If it's a sidecar table, we need the hub name for the trigger argument.
            hub_table = table
            for suffix in ["_attributes", "_geometry", "_geoms"]:
                if table.endswith(suffix):
                    hub_table = table[: -len(suffix)]
                    break

            # 4. Create the trigger
            # Note: We use a multi-statement DDL; DDLQuery handles splitting.
            trigger_ddl = f"""
            DROP TRIGGER IF EXISTS trg_asset_cleanup ON "{schema}"."{table}";
            CREATE TRIGGER trg_asset_cleanup
            AFTER DELETE OR UPDATE OF asset_id ON "{schema}"."{table}"
            FOR EACH ROW
            EXECUTE FUNCTION platform.asset_cleanup('{hub_table}');
            """.strip()

            await DDLQuery(trigger_ddl).execute(conn, schema=schema)

    # -------------------------------------------------------------------------
    # Asset reference table bootstrap
    # -------------------------------------------------------------------------

    async def ensure_asset_references_table(
        self,
        schema: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """
        Idempotently creates the ``asset_references`` table in *schema* if it
        does not yet exist.  Called during tenant schema initialisation alongside
        the ``assets`` table creation.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".asset_references (
            asset_id       VARCHAR     NOT NULL,
            catalog_id     VARCHAR     NOT NULL,
            ref_type       VARCHAR     NOT NULL,
            ref_id         VARCHAR     NOT NULL,
            cascade_delete BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_id, asset_id, ref_type, ref_id)
        );
        CREATE INDEX IF NOT EXISTS idx_asset_refs_blocking_{schema}
            ON "{schema}".asset_references (catalog_id, asset_id)
            WHERE cascade_delete = FALSE;
        """.strip()
        async with managed_transaction(db_resource or self.engine) as conn:
            await DDLQuery(ddl).execute(conn, schema=schema)

    # -------------------------------------------------------------------------
    # Asset reference CRUD
    # -------------------------------------------------------------------------

    async def add_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: AssetReferenceType,
        ref_id: str,
        cascade_delete: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> AssetReference:
        """Registers a dependency on an asset. Delegates to DriverAssetPostgresql."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import DriverAssetPostgresql
        pg = DriverAssetPostgresql(engine=db_resource or self.engine)
        row = await pg.add_asset_reference(
            asset_id=asset_id,
            catalog_id=catalog_id,
            ref_type=ref_type,
            ref_id=ref_id,
            cascade_delete=cascade_delete,
            db_resource=db_resource,
        )
        return AssetReference.model_validate(row)

    async def remove_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: AssetReferenceType,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Removes a previously registered asset reference."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import DriverAssetPostgresql
        pg = DriverAssetPostgresql(engine=db_resource or self.engine)
        await pg.remove_asset_reference(
            asset_id=asset_id,
            catalog_id=catalog_id,
            ref_type=ref_type,
            ref_id=ref_id,
            db_resource=db_resource,
        )

    async def list_asset_references(
        self,
        asset_id: str,
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[AssetReference]:
        """Returns all active references for the given asset."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import DriverAssetPostgresql
        pg = DriverAssetPostgresql(engine=db_resource or self.engine)
        rows = await pg.list_asset_references(
            asset_id=asset_id,
            catalog_id=catalog_id,
            db_resource=db_resource,
        )
        return [AssetReference.model_validate(r) for r in rows]

    async def _list_blocking_references_bulk(
        self,
        asset_ids: List[str],
        catalog_id: str,
        conn: DbConnection,
        phys_schema: str,
    ) -> List[AssetReference]:
        """Returns cascade_delete=False references for all given asset IDs in one round trip."""
        from dynastore.modules.catalog.drivers.pg_asset_driver import DriverAssetPostgresql
        pg = DriverAssetPostgresql(engine=conn)
        rows = await pg.check_blocking_references(
            asset_ids=asset_ids,
            catalog_id=catalog_id,
            db_resource=conn,
        )
        return [AssetReference.model_validate(r) for r in rows]


# assets and asset_references tables are now created by
# DriverAssetPostgresql._pg_asset_driver_init_tenant (priority 5) in
# modules/catalog/drivers/pg_asset_driver.py
