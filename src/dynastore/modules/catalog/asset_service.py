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

import uuid
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Union, Callable, Awaitable, Annotated
from sqlalchemy import text
from dynastore.tools.cache import cached
from pydantic import BaseModel, Field, ConfigDict, AliasChoices

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
    DbConnection,
    PydanticResultHandler,
    GeoDQLQuery,
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.db_config.locking_tools import acquire_startup_lock
from dynastore.modules.db_config.partition_tools import (
    ensure_partition_exists as ensure_partition_tool,
    ensure_hierarchical_partitions_exist,
    PartitionDefinition,
)
from dynastore.modules.catalog.models import AssetReferenceType, CoreAssetReferenceType, EventType
from dynastore.models.protocols.assets import AssetsProtocol
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
        AssetTypeEnum.ASSET,
        description="Type of the asset. Could be VECTORIAL, RASTER, or generic ASSET.",
        examples=["VECTORIAL", "RASTER", "ASSET"],
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata associated with the asset.",
        examples=[{"owner": "", "provider": ""}],
    )
    owned_by: Optional[str] = Field(
        None,
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

        # Instance-bound cache for assets
        self.get_asset_cached = cached(maxsize=128, namespace="assets")(self._get_asset_db)

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
    ) -> Optional[Asset]:
        async with managed_transaction(self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return None
            # asset_id in DB is VARCHAR now
            sql = f'SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by FROM "{phys_schema}".assets WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id AND deleted_at IS NULL;'
            return await DQLQuery(
                sql, result_handler=PydanticResultHandler.pydantic_one(Asset)
            ).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Asset]:
        """
        Get asset by ID.
        """
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._resolve_schema(catalog_id, conn)
                if not phys_schema:
                    return None
                sql = f'SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by FROM "{phys_schema}".assets WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id AND deleted_at IS NULL;'
                return await DQLQuery(
                    sql, result_handler=PydanticResultHandler.pydantic_one(Asset)
                ).execute(
                    conn,
                    asset_id=asset_id,
                    catalog_id=catalog_id,
                    collection_id=target_col_id,
                )
        return await self.get_asset_cached(catalog_id, asset_id, target_col_id)

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
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return []

            sql = f"""
            SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by FROM "{phys_schema}".assets
            WHERE catalog_id = :catalog_id
              AND collection_id = :collection_id
              AND deleted_at IS NULL
            ORDER BY created_at DESC LIMIT :limit OFFSET :offset;
            """
            return await DQLQuery(
                sql, result_handler=PydanticResultHandler.pydantic_all(Asset)
            ).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=target_col_id,
                limit=limit,
                offset=offset,
            )

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
        Supports metadata path extraction (e.g., 'metadata.provider.name').
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return []

            sql_base = f'SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by FROM "{phys_schema}".assets WHERE catalog_id = :catalog_id AND deleted_at IS NULL'
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}

            target_col_id = (
                collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
            )
            sql_base += " AND collection_id = :collection_id"
            params["collection_id"] = target_col_id

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

            for i, f in enumerate(filters):
                # Parse field (handle JSONB paths)
                if f.field.startswith("metadata."):
                    parts = f.field.split(".")
                    root = parts[0]
                    path = "->".join(f"'{p}'" for p in parts[1:-1])
                    leaf = f"->>'{parts[-1]}'"
                    field_expr = f"{root}{'->' + path if path else ''}{leaf}"
                elif f.field == "id":
                    field_expr = '"asset_id"'  # Map 'id' filter to 'asset_id' column
                else:
                    # Basic fields: uri, asset_type, asset_id
                    validate_sql_identifier(f.field)
                    field_expr = f'"{f.field}"'

                val_key = f"val_{i}"
                sql_base += f" AND {field_expr} {op_map[f.op]} :{val_key}"

                # Formatting for LIKE/ILIKE
                if f.op in [
                    FilterOperator.LIKE,
                    FilterOperator.ILIKE,
                ] and "%" not in str(f.value):
                    params[val_key] = f"%{f.value}%"
                else:
                    params[val_key] = f.value

            sql_base += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset;"

            query = DQLQuery(
                sql_base, result_handler=PydanticResultHandler.pydantic_all(Asset)
            )
            return await query.execute(conn, **params)

    # --- Lifecycle ---

    async def create_asset(
        self,
        catalog_id: str,
        asset: AssetBase,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Asset:
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID

        async with managed_transaction(db_resource or self.engine) as conn:
            # Resolve physical schema
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                raise ValueError(
                    f"Catalog '{catalog_id}' does not exist or has no physical schema."
                )

            # Ensure partition exists for this collection in the tenant schema
            # Partitioning by collection_id
            await ensure_partition_tool(
                conn,
                table_name="assets",
                strategy="LIST",
                partition_value=target_col_id,
                schema=phys_schema,
                parent_table_name="assets",
                parent_table_schema=phys_schema,
            )

            # Assign asset_id (it's mandatory from AssetBase)
            # if provided in AssetBase.asset_id, use it. But AssetBase must have asset_id.

            now = datetime.now(timezone.utc)

            insert_sql = text(f"""
                INSERT INTO "{phys_schema}".assets
                    (asset_id, catalog_id, collection_id, asset_type, uri, created_at, metadata, owned_by)
                VALUES
                    (:asset_id, :catalog_id, :collection_id, :asset_type, :uri, :created_at, :metadata, :owned_by)
                ON CONFLICT (collection_id, asset_id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    metadata = EXCLUDED.metadata,
                    owned_by = EXCLUDED.owned_by,
                    deleted_at = NULL
                RETURNING asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by;
            """)

            res_row = await DQLQuery(
                insert_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(
                conn,
                asset_id=asset.asset_id,
                catalog_id=catalog_id,
                collection_id=target_col_id,
                asset_type=asset.asset_type.value,
                uri=asset.uri,
                created_at=now,
                metadata=json.dumps(asset.metadata, cls=CustomJSONEncoder),
                owned_by=asset.owned_by,
            )

            created = Asset.model_validate(res_row)

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
        """
        Updates an existing asset's metadata.
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            # 1. Fetch current asset
            current = await self.get_asset(
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                db_resource=conn,
            )
            if not current:
                raise ValueError(
                    f"Asset '{asset_id}' not found in catalog '{catalog_id}'."
                )

            phys_schema = await self._resolve_schema(catalog_id, conn)

            # 2. Merge/Replace metadata (we'll replace for PUT)
            current.metadata = update.metadata

            # 3. Use direct UPDATE for metadata-only modification
            target_col_id = (
                collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
            )

            update_sql = text(f"""
                UPDATE "{phys_schema}".assets
                SET metadata = :metadata
                WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id
                RETURNING asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata, owned_by;
            """)

            res_row = await DQLQuery(
                update_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(
                conn,
                metadata=json.dumps(current.metadata, cls=CustomJSONEncoder),
                asset_id=current.asset_id,
                catalog_id=catalog_id,
                collection_id=target_col_id,
            )
            updated = Asset.model_validate(res_row)

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
        now = datetime.now(timezone.utc)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return 0

            where_clauses = ["catalog_id = :cat"]
            params = {"cat": catalog_id, "now": now}
            if asset_id:
                where_clauses.append("asset_id = :aid")
                params["aid"] = asset_id

            # Determine collection filter
            if collection_id:
                where_clauses.append("collection_id = :coll")
                params["coll"] = collection_id
            elif asset_id:
                where_clauses.append("collection_id = :coll")
                params["coll"] = CATALOG_LEVEL_COLLECTION_ID

            where_stmt = " AND ".join(where_clauses)

            fetch_sql = text(
                f'SELECT asset_id, catalog_id, collection_id, owned_by FROM "{phys_schema}".assets WHERE {where_stmt}'
            )

            assets = await DQLQuery(
                fetch_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)

            if not assets:
                return 0

            # --- Reference guard: block hard-delete of owned assets with blocking references ---
            if hard:
                for a in assets:
                    if a.get("owned_by"):
                        blocking = await self._list_blocking_references(
                            a["asset_id"], a["catalog_id"], conn, phys_schema
                        )
                        if blocking:
                            raise AssetReferencedError(a["asset_id"], blocking)

            prefix = (
                f'DELETE FROM "{phys_schema}".assets'
                if hard
                else f'UPDATE "{phys_schema}".assets SET deleted_at = :now'
            )
            final_sql = text(f"{prefix} WHERE {where_stmt}")

            # Use DQLQuery with ROWCOUNT handler for UPDATE/DELETE to ensure sync/async compatibility
            # For HARD DELETE, DDLQuery could be used but DQL with ROWCOUNT is also fine and returns count
            rowcount = await DQLQuery(
                final_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, **params)

            # Invalidate cache
            for a in assets:
                self._invalidate_cache(
                    a["asset_id"], a["catalog_id"], a["collection_id"]
                )

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
            PRIMARY KEY (catalog_id, asset_id, ref_type, ref_id),
            FOREIGN KEY (catalog_id, asset_id)
                REFERENCES "{schema}".assets (catalog_id, asset_id)
                ON DELETE CASCADE
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
        """Registers a dependency on an asset from *ref_id*."""
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            now = datetime.now(timezone.utc)
            sql = text(f"""
                INSERT INTO "{phys_schema}".asset_references
                    (asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at)
                VALUES (:asset_id, :catalog_id, :ref_type, :ref_id, :cascade_delete, :created_at)
                ON CONFLICT (catalog_id, asset_id, ref_type, ref_id) DO UPDATE SET
                    cascade_delete = EXCLUDED.cascade_delete
                RETURNING asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at;
            """)
            row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                ref_type=ref_type.value,
                ref_id=ref_id,
                cascade_delete=cascade_delete,
                created_at=now,
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
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return
            sql = text(f"""
                DELETE FROM "{phys_schema}".asset_references
                WHERE catalog_id = :catalog_id
                  AND asset_id   = :asset_id
                  AND ref_type   = :ref_type
                  AND ref_id     = :ref_id;
            """)
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                catalog_id=catalog_id,
                asset_id=asset_id,
                ref_type=ref_type.value,
                ref_id=ref_id,
            )

    async def list_asset_references(
        self,
        asset_id: str,
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[AssetReference]:
        """Returns all active references for the given asset."""
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                return []
            sql = f"""
                SELECT asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at
                FROM "{phys_schema}".asset_references
                WHERE catalog_id = :catalog_id AND asset_id = :asset_id
                ORDER BY created_at ASC;
            """
            rows = await DQLQuery(
                sql, result_handler=PydanticResultHandler.pydantic_all(AssetReference)
            ).execute(conn, catalog_id=catalog_id, asset_id=asset_id)
            return rows or []

    async def _list_blocking_references(
        self,
        asset_id: str,
        catalog_id: str,
        conn: DbConnection,
        phys_schema: str,
    ) -> List[AssetReference]:
        """
        Returns only the ``cascade_delete=False`` references for an asset.
        Uses the partial index for efficiency.
        """
        sql = f"""
            SELECT asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at
            FROM "{phys_schema}".asset_references
            WHERE catalog_id = :catalog_id AND asset_id = :asset_id AND cascade_delete = FALSE;
        """
        rows = await DQLQuery(
            sql, result_handler=PydanticResultHandler.pydantic_all(AssetReference)
        ).execute(conn, catalog_id=catalog_id, asset_id=asset_id)
        return rows or []
