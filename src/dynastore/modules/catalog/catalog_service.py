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
CatalogService: Handles all catalog-level CRUD operations.

This service implements CatalogsProtocol and provides:
- Catalog creation, retrieval, updates, deletion
- Catalog listing and search
- Physical schema resolution
- Catalog-level caching
"""

import logging
import json
import uuid
from typing import List, Optional, Any, Dict, Union, Set, Callable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    from dynastore.modules.db_config.query_executor import DDLBatch
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext
from sqlalchemy import text

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.catalog.models import (
    Catalog,
    CatalogUpdate,
    EventType,
    LocalizedText,
    Collection,
)
from dynastore.models.shared_models import Feature
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.models.protocols import (
    CatalogsProtocol,
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol,
    ConfigsProtocol,
    LocalizationProtocol,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.event_service import CatalogEventType, emit_event
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.typed_store.ddl import PLATFORM_SCHEMAS_DDL, tenant_configs_ddl
from dynastore.tools.async_utils import signal_bus
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext

logger = logging.getLogger(__name__)

# ==============================================================================
#  CORE DDL DEFINITIONS (Base Catalog)
# ==============================================================================

# 1. COLLECTIONS
TENANT_COLLECTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collections (
    id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (id)
);
"""

# collection_metadata: stores all descriptive metadata for collections.
# Renamed from {schema}.metadata in Phase 2 of the naming harmonisation
# (items / collection / catalog / asset tier prefixes).  See
# db_init/metadata_domain_split.rename_legacy_metadata_tables for the
# idempotent migration that runs ahead of this CREATE TABLE.
METADATA_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_metadata (
    collection_id VARCHAR NOT NULL PRIMARY KEY,
    title JSONB,
    description JSONB,
    keywords JSONB,
    license JSONB,
    extent JSONB,
    providers JSONB,
    summaries JSONB,
    links JSONB,
    assets JSONB,
    item_assets JSONB,
    stac_version VARCHAR(20) DEFAULT '1.1.0',
    stac_extensions JSONB DEFAULT '[]'::jsonb,
    extra_metadata JSONB
);
"""


def _build_tenant_core_ddl_batch(schema: str) -> "DDLBatch":
    """Build the per-tenant core DDL batch.

    Warm path: ``collection_configs`` (the last table created by
    ``tenant_configs_ddl``) acts as the sentinel. If it exists, the
    collections + metadata + config tables are all skipped in a single
    round-trip. Cold path runs all three DDLs under one conn with nested
    savepoints.
    """
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.db_config.locking_tools import check_table_exists

    def _check_sentinel(conn):
        return check_table_exists(conn, "collection_configs", schema)

    tenant_configs_sql = tenant_configs_ddl(schema)
    return DDLBatch(
        sentinel=DDLQuery(tenant_configs_sql, check_query=_check_sentinel),
        steps=[
            DDLQuery(TENANT_COLLECTIONS_DDL + METADATA_DDL),
            DDLQuery(tenant_configs_sql, check_query=_check_sentinel),
        ],
    )


async def initialize_core_tenant_tables(conn: DbResource, schema: str, catalog_id: str):
    """
    Creates the complete isolated table set for a new tenant (Catalog).

    NOTE: Called DIRECTLY from `create_catalog` in the outer transaction, NOT via
    `lifecycle_registry.init_catalog`, to avoid SAVEPOINT (begin_nested) wrapping
    which prevents DDL from being visible to subsequent lifecycle hooks.
    Do NOT re-register this as a `sync_catalog_initializer`.
    """
    logger.info(f"Initializing core tenant tables for schema: {schema} (Catalog: {catalog_id})")

    schema = schema.strip('\'\'" ')

    # 1. Create Schema
    await ensure_schema_exists(conn, schema)

    # 2. Phase 2 rename — move any existing {schema}.metadata* tables to
    # their {schema}.collection_metadata* counterparts BEFORE the CREATE
    # TABLE IF NOT EXISTS statements run.  Idempotent and a no-op on
    # fresh schemas (DO block finds both source and target missing).
    # Running the rename first guarantees that an existing tenant's data
    # lands in the canonical table; a follow-up CREATE TABLE IF NOT EXISTS
    # then observes the canonical table already present and does nothing.
    from dynastore.modules.catalog.db_init.metadata_domain_split import (
        ensure_tenant_metadata_domain_tables,
        rename_legacy_metadata_tables,
    )
    await rename_legacy_metadata_tables(conn, schema)

    # 3. Core Tables (collections + collection_metadata + tenant configs) under
    # a single batch sentinel — warm path skips the whole thing in one
    # round-trip.
    await _build_tenant_core_ddl_batch(schema).execute(conn, schema=schema)
    logger.info(f"Core tenant tables (collections, configs, collection_metadata) initialized for {schema}.")

    # 4. M2.0 — per-tenant metadata-domain split tables
    # (collection_metadata_core + collection_metadata_stac).  Additive:
    # coexists with the legacy {schema}.collection_metadata table; no
    # reads / writes change in M2.0.  Idempotent.
    await ensure_tenant_metadata_domain_tables(conn, schema)
    logger.info(f"M2 metadata-domain split tables initialized for {schema}.")

from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.event_service import CatalogEventType, emit_event

logger = logging.getLogger(__name__)

# --- Helpers ---

BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def encode_base36(num: int) -> str:
    if num == 0:
        return BASE36[0]
    arr = []
    base = len(BASE36)
    while num:
        num, rem = divmod(num, base)
        arr.append(BASE36[rem])
    arr.reverse()
    return "".join(arr)


def generate_physical_name(prefix: str) -> str:
    """Generates a short, readable physical name using the last 8 chars of a Base36-encoded UUIDv7.

    Format: {prefix}_{8-char base36}   e.g.  s_2ka8fbc3  or  t_9xz01mq7
    Collision probability: ~1 in 2^41 for each new name — negligible for thousands
    of catalogs and millions of collections on the same platform.
    The short suffix also keeps derived identifiers (event partition tables, GCS bucket
    names) well within PostgreSQL's 63-char limit.
    """
    from dynastore.tools.identifiers import generate_uuidv7

    uid = generate_uuidv7().int
    full = encode_base36(uid)
    # Take the last 8 chars — they encode the random bits, not the timestamp prefix,
    # which maximises entropy for collision resistance at this short length.
    suffix = full[-8:]
    return f"{prefix}_{suffix}"


def get_catalog_engine(db_resource: Optional[DbResource] = None) -> DbResource:
    """Get database engine for catalog operations."""
    if db_resource:
        return db_resource

    from dynastore.tools.protocol_helpers import get_engine

    return get_engine()  # type: ignore[return-value]


def _build_catalog_metadata_payload(catalog_model: Catalog) -> Dict[str, Any]:
    """Flatten the Catalog model into a dict keyed by domain-metadata columns.

    Keys align with the column tuples in
    ``modules/storage/drivers/metadata_domain_postgresql.py`` so every
    registered ``sync_catalog_metadata_initializer`` hook can
    ``_filter_payload`` down to its own domain's columns without
    additional routing logic here.

    Absent fields are omitted (not set to ``None``) so the hook-level
    default-fast check (``if not catalog_metadata: return``) still
    fires when the caller passed an empty-body catalog create.

    This helper stays in the service layer because it knows the
    public Catalog model's shape.  The drivers read an opaque dict and
    are not coupled to the Catalog class.
    """
    out: Dict[str, Any] = {}

    # CORE domain fields
    if catalog_model.title is not None:
        out["title"] = catalog_model.title.model_dump(exclude_none=True) \
            if hasattr(catalog_model.title, "model_dump") else catalog_model.title
    if catalog_model.description is not None:
        out["description"] = catalog_model.description.model_dump(exclude_none=True) \
            if hasattr(catalog_model.description, "model_dump") else catalog_model.description
    if catalog_model.keywords is not None:
        out["keywords"] = catalog_model.keywords.model_dump(exclude_none=True) \
            if hasattr(catalog_model.keywords, "model_dump") else catalog_model.keywords
    if catalog_model.license is not None:
        out["license"] = catalog_model.license.model_dump(exclude_none=True) \
            if hasattr(catalog_model.license, "model_dump") else catalog_model.license
    if catalog_model.extra_metadata is not None:
        out["extra_metadata"] = catalog_model.extra_metadata.model_dump(exclude_none=True) \
            if hasattr(catalog_model.extra_metadata, "model_dump") else catalog_model.extra_metadata

    # STAC domain fields (catalog-tier subset — no extent / providers / summaries here)
    if catalog_model.stac_version:
        out["stac_version"] = catalog_model.stac_version
    if catalog_model.stac_extensions:
        out["stac_extensions"] = list(catalog_model.stac_extensions)
    conforms_to = getattr(catalog_model, "conformsTo", None)
    if conforms_to:
        out["conforms_to"] = list(conforms_to)
    if catalog_model.links:
        out["links"] = [
            link.model_dump(exclude_none=True) if hasattr(link, "model_dump") else link
            for link in catalog_model.links
        ]
    # ``assets`` on the Catalog envelope — catalog-level assets (not item assets).
    catalog_assets = getattr(catalog_model, "assets", None)
    if catalog_assets:
        out["assets"] = catalog_assets

    return out


# --- Queries ---

_create_catalog_strict_query = DQLQuery(
    "INSERT INTO catalog.catalogs (id, physical_schema, title, description, keywords, license, conforms_to, links, assets, extra_metadata, provisioning_status, stac_version, stac_extensions) "
    "VALUES (:id, :physical_schema, :title, :description, :keywords, :license, :conforms_to, :links, :assets, :extra_metadata, :provisioning_status, :stac_version, :stac_extensions) "
    "ON CONFLICT (id) DO NOTHING;",
    result_handler=ResultHandler.ROWCOUNT,
)

_get_catalog_query = DQLQuery(
    "SELECT * FROM catalog.catalogs WHERE id = :id AND deleted_at IS NULL;",
    result_handler=ResultHandler.ONE_DICT,
)

_list_catalogs_query = DQLQuery(
    "SELECT * FROM catalog.catalogs WHERE deleted_at IS NULL ORDER BY id LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL_DICTS,
)

_soft_delete_catalog_query = DQLQuery(
    "UPDATE catalog.catalogs SET deleted_at = NOW() WHERE id = :id AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)

_hard_delete_catalog_query = DQLQuery(
    "DELETE FROM catalog.catalogs WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

_drop_schema_query = DDLQuery("DROP SCHEMA IF EXISTS {schema} CASCADE;")

_delete_tenant_cron_jobs_query = DQLQuery(
    "DELETE FROM cron.job WHERE jobname LIKE :pattern;",
    result_handler=ResultHandler.ROWCOUNT,
)


from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService
from dynastore.tools.protocol_helpers import get_engine

# ... (Previous imports and helpers remain same, just adding these)


class CatalogService(CatalogsProtocol):
    """Service for catalog-level operations implementing CatalogsProtocol."""

    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        collection_service: Optional[CollectionService] = None,
        item_service: Optional[ItemService] = None,
    ):
        self.engine = engine
        self._collection_service = collection_service
        self._item_service = item_service
        
        # Initialize internal services if not provided, provided we have an engine
        if self.engine:
            if not self._collection_service:
                self._collection_service = CollectionService(self.engine)
            if not self._item_service:
                self._item_service = ItemService(self.engine)

        # Instance-bound caches (private)
        # Only cache 'ready' catalogs: transient states ('provisioning', 'failed')
        # would otherwise stick in per-worker L1 forever (no cross-worker invalidation),
        # making init-upload return 503 long after provisioning completes.
        self._get_catalog_model_cached = cached(
            maxsize=128,
            ttl=30,
            jitter=5,
            namespace="catalog_model",
            condition=lambda c: c is not None
            and getattr(c, "provisioning_status", None) == "ready",
        )(self._get_catalog_model_db)

    def is_available(self) -> bool:
        """Returns True if the service is initialized and ready."""
        return (
            self.engine is not None
            and self._collection_service is not None
            and self._col_svc.is_available()
            and self._item_service is not None
            and self._item_svc.is_available()
        )

    @property
    def _col_svc(self) -> CollectionService:
        assert self._collection_service is not None
        return self._collection_service

    @property
    def _item_svc(self) -> ItemService:
        assert self._item_service is not None
        return self._item_service

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        assert self._item_service is not None
        return self._item_service

    @property
    def collections(self) -> CollectionsProtocol:
        from typing import cast as _cast
        assert self._collection_service is not None
        # CollectionService implements most of CollectionsProtocol; aspirational methods not yet done
        return _cast(CollectionsProtocol, self._collection_service)

    @property
    def assets(self) -> Optional[AssetsProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(AssetsProtocol)

    @property
    def configs(self) -> Optional[ConfigsProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(ConfigsProtocol)

    @property
    def localization(self) -> Optional[LocalizationProtocol]:
        from dynastore.tools.discovery import get_protocol as _gp
        return _gp(LocalizationProtocol)

    # --- Schema Resolution ---

    # async def _resolve_physical_schema_db(self, catalog_id: str) -> Optional[str]:
    #     """Resolve physical schema from catalog_id."""
    #     async with managed_transaction(self.engine) as conn:
    #         result = await DQLQuery(
    #             "SELECT physical_schema FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL;",
    #             result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    #         ).execute(conn, catalog_id=catalog_id)
    #         return result

    async def resolve_physical_schema(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        """Resolve physical schema for a catalog."""
        db_resource = ctx.db_resource if ctx else None
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                res = await DQLQuery(
                    "SELECT physical_schema FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL;",
                    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                ).execute(conn, catalog_id=catalog_id)
                if not res and not allow_missing:
                    raise ValueError(f"Catalog '{catalog_id}' not found.")
                return res
        # Use cached catalog model to get physical schema
        catalog_model = await self._get_catalog_model_cached(catalog_id)
        if not catalog_model and not allow_missing:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        
        ps = catalog_model.physical_schema if catalog_model else None
        # logger.warning(f"Resolved physical schema for '{catalog_id}': {ps}")
        return ps

    # --- Collection Resolution ---
    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hint: Optional[str] = None,
    ):
        """Resolve the best storage driver for a collection.

        Delegates to the storage router which resolves via
        ``CollectionRoutingConfig`` operation → ordered driver list.
        """
        from dynastore.modules.storage.router import get_driver
        return await get_driver(operation, catalog_id, collection_id, hint=hint)

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        return await self._col_svc.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def is_active(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        return await self._col_svc.is_active(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        await self._col_svc.activate_collection(
            catalog_id, collection_id, ctx=ctx,
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        return await self._col_svc.set_physical_table(
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    # --- Catalog CRUD ---

    async def ensure_catalog_exists(
        self,
        catalog_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensures that a catalog exists, creating it if necessary (JIT creation)."""
        if not await self.get_catalog_model(catalog_id, ctx=ctx):
            # If lang is not '*', we provide a simple string which create_catalog will localize
            # If lang is '*', we provide the default 'en' dictionary
            title = {"en": catalog_id} if lang == "*" else catalog_id
            await self.create_catalog(
                {"id": catalog_id, "title": title},
                lang=lang,
                ctx=ctx,
            )

    async def ensure_collection_exists(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensures that a collection exists, creating it if necessary (JIT creation)."""
        db_resource = ctx.db_resource if ctx else None
        if not await self._col_svc.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            await self._col_svc.ensure_collection_exists(
                db_resource, catalog_id, collection_id, lang=lang  # type: ignore[arg-type]
            )

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: Any,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        return await self._item_svc.ensure_physical_table_exists(
            catalog_id, collection_id, config, db_resource=db_resource
        )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: Any,
        partition_value: Any,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        db_resource = ctx.db_resource if ctx else None
        return await self._item_svc.ensure_partition_exists(
            catalog_id, collection_id, config, partition_value, ctx=ctx
        )

    async def get_catalog(
        self,
        catalog_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Catalog:
        model = await self.get_catalog_model(catalog_id, ctx=ctx)
        if not model:
            raise ValueError(f"Catalog '{catalog_id}' not found.")
        return model

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], Catalog],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Catalog:
        """Create a new catalog."""
        db_resource = ctx.db_resource if ctx else None
        from dynastore.models.protocols import StorageProtocol
        from dynastore.modules.tasks.tasks_module import create_task
        from dynastore.modules.tasks.models import TaskCreate

        if isinstance(catalog_data, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(catalog_data, lang)

        catalog_model = (
            Catalog.create_from_localized_input(catalog_data, lang)
            if isinstance(catalog_data, dict)
            else catalog_data
        )
        validate_sql_identifier(catalog_model.id)

        # Determine initial provisioning status based on Storage availability
        from dynastore.tools.discovery import get_protocol
        storage_protocol = get_protocol(StorageProtocol)
        
        # If Storage is active, we start as 'provisioning'. 
        # On-premise (no Storage module) starts as 'ready'.
        initial_status = "provisioning" if storage_protocol is not None else "ready"
        catalog_model.provisioning_status = initial_status

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            # Lifecycle Phase 1: BEFORE
            await emit_event(
                CatalogEventType.BEFORE_CATALOG_CREATION,
                catalog_id=catalog_model.id,
                db_resource=conn,
            )

            # JIT Physical Schema Generation
            physical_schema = generate_physical_name("s")

            # --- CRITICAL: Core tenant tables MUST be created directly in the outer
            # transaction, NOT inside a lifecycle SAVEPOINT (begin_nested).
            #
            # PostgreSQL DDL (CREATE SCHEMA, CREATE TABLE) inside a SAVEPOINT is
            # problematic: if any error occurs, only the SAVEPOINT rolls back — but
            # because DDL is not transactional in some PG contexts (especially when
            # combined with the asyncpg driver), the schema/tables may or may not be
            # created, leaving subsequent SAVEPOINT-wrapped hooks (stats, tiles, gcp…)
            # with nothing to work against.
            #
            # By creating schema + core tables here (outer tx), all lifecycle hooks
            # are guaranteed to find them ready.

            # 1. Schema (+ global configs schema/tables for FK references)
            await ensure_schema_exists(conn, "configs")
            await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)
            await ensure_schema_exists(conn, physical_schema)

            # 2. Core Tables (collections, metadata, catalog_configs, collection_configs)
            # Single module-level batch — warm path skips everything in one
            # round-trip once collection_configs (the last table) exists.
            logger.info(
                f"Creating core tenant tables for schema: {physical_schema} (Catalog: {catalog_model.id})"
            )
            await _build_tenant_core_ddl_batch(physical_schema).execute(
                conn, schema=physical_schema
            )

            # 3. Module-specific lifecycle hooks (stats, tiles, …) all run AFTER
            #    the schema and core tables exist, inside their own SAVEPOINTs.
            await lifecycle_registry.init_catalog(
                conn, physical_schema, catalog_id=catalog_model.id
            )

            # Store only user-provided extra_metadata content (no envelope)
            user_extra_metadata = (
                json.dumps(
                    catalog_model.extra_metadata.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if catalog_model.extra_metadata
                else None
            )

            await _create_catalog_strict_query.execute(
                conn,
                id=catalog_model.id,
                physical_schema=physical_schema,
                title=json.dumps(
                    catalog_model.title.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if catalog_model.title
                else None,
                description=json.dumps(
                    catalog_model.description.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if catalog_model.description
                else None,
                keywords=json.dumps(
                    catalog_model.keywords.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if catalog_model.keywords
                else None,
                license=json.dumps(
                    catalog_model.license.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if catalog_model.license
                else None,
                conforms_to=json.dumps(catalog_model.conformsTo, cls=CustomJSONEncoder) if catalog_model.conformsTo else None,
                links=json.dumps([l.model_dump() for l in catalog_model.links], cls=CustomJSONEncoder) if catalog_model.links else None,
                assets=None, # Assets are not part of core Catalog model yet, or are managed separately
                extra_metadata=user_extra_metadata,
                provisioning_status=catalog_model.provisioning_status,
                stac_version=catalog_model.stac_version,
                stac_extensions=json.dumps(catalog_model.stac_extensions, cls=CustomJSONEncoder) if catalog_model.stac_extensions else None,
            )

            # M2.2 — catalog-metadata lifecycle phase.
            #
            # At this point the catalog.catalogs registry row is committed
            # (the INSERT above), so FK references from
            # catalog.catalog_metadata_core / _stac into catalog.catalogs(id)
            # are satisfied.  Dispatch the lifecycle phase so any registered
            # PG / STAC / future driver hooks can persist catalog-tier
            # metadata into their domain-scoped tables.  Default-fast: a
            # caller that supplies no metadata fields (title=None, etc.)
            # yields an empty dict here and every hook no-ops.
            #
            # The service stays driver-agnostic: the dict is built from the
            # public Catalog model; the lifecycle_registry decides which
            # hooks consume it (PG Primary today, ES Indexer later, etc.).
            catalog_metadata = _build_catalog_metadata_payload(catalog_model)
            await lifecycle_registry.init_catalog_metadata(
                conn,
                physical_schema,
                catalog_id=catalog_model.id,
                catalog_metadata=catalog_metadata,
            )

            # Lifecycle Phase 2: EVENT (Now after schema is ready AND record exists)
            await emit_event(
                CatalogEventType.CATALOG_CREATION,
                catalog_id=catalog_model.id,
                db_resource=conn,
            )

            # Lifecycle Phase 3: AFTER
            await emit_event(
                CatalogEventType.AFTER_CATALOG_CREATION,
                catalog_id=catalog_model.id,
                db_resource=conn,
            )

            # Invalidate cache to ensure it's re-fetched in subsequent calls
            self._get_catalog_model_cached.cache_invalidate(catalog_model.id)

        # Execute async external component initializers OUTSIDE transaction
        config_snapshot = {}
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            config_mgr = get_protocol(ConfigsProtocol)
            if config_mgr:
                config_snapshot.update(
                    await config_mgr.list_catalog_configs(catalog_model.id)
                )
        except Exception:
            pass

        lifecycle_registry.init_async_catalog(
            catalog_model.id,
            LifecycleContext(
                physical_schema=physical_schema,
                config=config_snapshot
            )
        )

        # Invalidate caches BEFORE emitting signal to prevent visibility gap race conditions
        self._get_catalog_model_cached.cache_invalidate(catalog_model.id)
        # Cache invalidation for catalog model (physical_schema is part of catalog model)
        self._get_catalog_model_cached.cache_invalidate(catalog_model.id)

        # Emit signal to wake up background tasks (Visibility Gap fix)
        # This must happen OUTSIDE the transaction above so that background listeners
        # (like GCP provisioning) can see the committed 'catalog' row.
        await signal_bus.emit("AFTER_CATALOG_CREATION", identifier=catalog_model.id)

        result = await _get_catalog_query.execute(
            get_catalog_engine(db_resource), id=catalog_model.id
        )
        return Catalog.model_validate(result)

    def _unpack_catalog_row(self, row: Any) -> Optional[Catalog]:
        """Unpacks a database row into a Catalog model.

        The extra_metadata column now stores only the user-provided extra metadata
        (a localized JSONB dict), not an envelope with type/conformsTo/links.
        Those fields come from the Catalog model defaults.
        """
        if not row:
            return None

        # Convert to dict
        data = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

        # Unpack STAC dedicated columns if present
        if "conforms_to" in data and data["conforms_to"]:
            data["conformsTo"] = data["conforms_to"]
        
        # Ensure jsonb fields are loaded correctly if driver doesn't cast automatically
        for key in ["conformsTo", "links", "assets", "extra_metadata", "stac_extensions"]:
            dict_val = data.get(key)
            if isinstance(dict_val, str):
                try:
                    data[key] = json.loads(dict_val)
                except Exception:
                    data[key] = None

        return Catalog.model_validate(data)

    async def _get_catalog_model_db(self, catalog_id: str) -> Optional[Catalog]:
        """Get catalog model from database."""
        async with managed_transaction(self.engine) as conn:
            result = await _get_catalog_query.execute(conn, id=catalog_id)
            return self._unpack_catalog_row(result)

    async def get_catalog_model(
        self, catalog_id: str, ctx: Optional["DriverContext"] = None
    ) -> Optional[Catalog]:
        """Get catalog by ID."""
        db_resource = ctx.db_resource if ctx else None
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                result = await _get_catalog_query.execute(conn, id=catalog_id)
                catalog = self._unpack_catalog_row(result)
        else:
            catalog = await self._get_catalog_model_cached(catalog_id)

        if catalog is None:
            return None

        return await self._run_catalog_pipeline(catalog_id, catalog)

    async def _run_catalog_pipeline(
        self, catalog_id: str, catalog: Catalog
    ) -> Optional[Catalog]:
        """Apply CatalogPipelineProtocol stages (optional, priority-ordered).

        Stages may augment, filter, or transform the catalog metadata dict.
        Stages returning ``None`` drop the catalog — the caller is
        responsible for rendering that as a 404 in the HTTP layer.

        An empty stage registry is safe: the input catalog passes through
        unchanged.
        """
        try:
            from dynastore.tools.discovery import get_protocols
            from dynastore.models.protocols.catalog_pipeline import CatalogPipelineProtocol

            stages = sorted(
                get_protocols(CatalogPipelineProtocol),
                key=lambda s: s.priority,
            )
            if not stages:
                return catalog

            data = catalog.model_dump(by_alias=True, exclude_none=True)
            for stage in stages:
                try:
                    if not stage.can_apply(catalog_id):
                        continue
                    result = await stage.apply(catalog_id, data, context={})
                except Exception as _stage_err:
                    logger.warning(
                        "CatalogPipeline stage '%s' failed for %s: %s",
                        getattr(stage, "pipeline_id", repr(stage)),
                        catalog_id,
                        _stage_err,
                    )
                    continue
                if result is None:
                    return None  # stage dropped the catalog
                data = result
            return Catalog.model_validate(data)
        except Exception:
            return catalog  # discovery failure must not break the read path

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], CatalogUpdate],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Catalog]:
        """Update a catalog."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)

        if isinstance(updates, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(updates, lang)

        update_model = (
            CatalogUpdate.create_from_localized_input(updates, lang)
            if isinstance(updates, dict)
            else updates
        )

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            existing_model = await self.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn))
            if not existing_model:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Merge updates into existing model
            merged_model = existing_model.merge_localized_updates(updates, lang)

            await emit_event(
                CatalogEventType.CATALOG_UPDATE, catalog_id=catalog_id, db_resource=conn
            )

            set_clauses: List[str] = []
            params: Dict[str, Any] = {"id": catalog_id}

            # Identify which fields were actually requested for update
            update_fields = (
                updates.keys()
                if isinstance(updates, dict)
                else updates.model_dump(exclude_unset=True).keys()
            )

            if "title" in update_fields:
                set_clauses.append("title = :title")
                params["title"] = (
                    json.dumps(
                        merged_model.title.model_dump(exclude_none=True),
                        cls=CustomJSONEncoder,
                    )
                    if merged_model.title
                    else None
                )

            if "description" in update_fields:
                set_clauses.append("description = :description")
                params["description"] = (
                    json.dumps(
                        merged_model.description.model_dump(exclude_none=True),
                        cls=CustomJSONEncoder,
                    )
                    if merged_model.description
                    else None
                )

            if "keywords" in update_fields:
                set_clauses.append("keywords = :keywords")
                params["keywords"] = (
                    json.dumps(
                        merged_model.keywords.model_dump(exclude_none=True),
                        cls=CustomJSONEncoder,
                    )
                    if merged_model.keywords
                    else None
                )

            if "license" in update_fields:
                set_clauses.append("license = :license")
                params["license"] = (
                    json.dumps(
                        merged_model.license.model_dump(exclude_none=True),
                        cls=CustomJSONEncoder,
                    )
                    if merged_model.license
                    else None
                )

            if not set_clauses:
                return merged_model

            sql = f"UPDATE catalog.catalogs SET {', '.join(set_clauses)} WHERE id = :id AND deleted_at IS NULL;"
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, **params
            )

            await emit_event(
                CatalogEventType.AFTER_CATALOG_UPDATE,
                catalog_id=catalog_id,
                db_resource=conn,
            )

        # Invalidate cache
        self._get_catalog_model_cached.cache_invalidate(catalog_id)

        return await self.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None)

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, ctx: Optional["DriverContext"] = None
    ) -> bool:
        """Deletes a specific language variant from a catalog."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            model = await self.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn))
            if not model:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Check if language exists and if it's not the last one
            from dynastore.models.localization import Language

            can_delete = False
            fields_to_update = {}

            for field in [
                "title",
                "description",
                "keywords",
                "license",
                "extra_metadata",
            ]:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(
                                f"Cannot delete language '{lang}' from field '{field}': it is the only language available."
                            )

                        # Use merge_updates with None to simulate deletion for that language?
                        # Actually LocalizedDTO.merge_updates doesn't support deletion of a language easily via merge.
                        # We might need a 'delete_language' on LocalizedDTO or just do it here.

                        # Let's do it manually for now
                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            fields_to_update[field] = json.dumps(
                                data, cls=CustomJSONEncoder
                            )
                            can_delete = True

            if not can_delete:
                return False

            set_clauses = [f"{k} = :{k}" for k in fields_to_update.keys()]
            params = {"id": catalog_id, **fields_to_update}

            sql = f"UPDATE catalog.catalogs SET {', '.join(set_clauses)} WHERE id = :id AND deleted_at IS NULL;"
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, **params
            )

            # Special case for extra_metadata inner extra_metadata (legacy blob)
            # Actually our CatalogsProtocol/model handling for extra_metadata is a bit mixed.
            # extra_metadata in DB is a JSONB blob containing conformsto, links, AND localized extra_metadata.

            self._get_catalog_model_cached.cache_invalidate(catalog_id)
            return True

    async def list_catalogs(
        self,
        limit: int = 100,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ) -> List[Catalog]:
        """List all catalogs."""
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            if not q:
                results = await _list_catalogs_query.execute(
                    conn, limit=limit, offset=offset
                )
            else:
                sql = "SELECT * FROM catalog.catalogs WHERE deleted_at IS NULL AND (id ILIKE :q OR title->>'en' ILIKE :q OR description->>'en' ILIKE :q) ORDER BY id LIMIT :limit OFFSET :offset;"
                query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
                results = await query.execute(conn, limit=limit, offset=offset, q=f"%{q}%")
                
            # Use unpacker to handle legacy JSON packing for list results too
            # Filter out Nones in case unpacking fails for some rows
            models = [self._unpack_catalog_row(r) for r in results]
            return [m for m in models if m is not None]

    async def search_catalogs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Catalog]:
        """Search catalogs with filters."""
        # TODO: implement this feature reusing ogc filters, reemove delegate to list_catalogs
        return await self.list_catalogs(
            limit=limit, offset=offset, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    # --- Config Operations (delegated to ConfigsProtocol via aggregation if needed, or keeping legacy) ---
    # Actually, the protocol says CatalogsProtocol has get_catalog_config and get_collection_config

    async def get_catalog_config(
        self, catalog_id: str, ctx: Optional["DriverContext"] = None
    ):
        db_resource = ctx.db_resource if ctx else None
        from dynastore.models.protocols.configs import ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        from dynastore.modules.catalog.catalog_config import CollectionPluginConfig

        return await configs.get_config(  # type: ignore[union-attr]
            CollectionPluginConfig, catalog_id, ctx=DriverContext(db_resource=db_resource
        ))

    async def get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ):
        db_resource = ctx.db_resource if ctx else None
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        return await driver.get_driver_config(  # type: ignore[attr-defined]
            catalog_id, collection_id, db_resource=db_resource
        )

    async def delete_catalog(
        self,
        catalog_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """
        Delete a catalog.

        If force=True, triggers a hard deletion (removal of schema and data).
        Otherwise, performs a soft delete (marks as deleted).
        """
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)

        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            # 1. Soft Delete
            rows = await _soft_delete_catalog_query.execute(conn, id=catalog_id)

            # If not found/already deleted
            if rows == 0:
                # If we are not forcing, we can't delete what doesn't exist
                # But if forcing, we might want to ensure cleanup even if soft-deleted previously?
                # Legacy behavior was strict. Protocol -> bool.
                # If we return False here, it means "not deleted" (maybe not found).

                # Check existence to distinguish "not found" vs "already deleted" vs "soft delete failed"
                # Optimization: just check if it exists in DB?
                # For now, if rows=0 and not force, we return False.
                if not force:
                    return False

            if not force:
                await emit_event(
                    CatalogEventType.CATALOG_DELETION,
                    catalog_id=catalog_id,
                    db_resource=conn,
                )
                self._get_catalog_model_cached.cache_invalidate(catalog_id)
                return True

            # 2. Hard Delete (Force)
            # Lifecycle: BEFORE -> HARD_DELETE internal -> AFTER
            await emit_event(
                CatalogEventType.BEFORE_CATALOG_HARD_DELETION,
                catalog_id=catalog_id,
                db_resource=conn,
            )

            # The actual hard deletion logic (dropping schema etc) matches what was in CatalogModule delegates
            # We need to drop the schema and delete the row.

            # Capture configuration before deletion to pass to async destroyers
            from dynastore.models.protocols import ConfigsProtocol
            config_manager = get_protocol(ConfigsProtocol)
            config_snapshot = {}
            if config_manager:
                try:
                    config_snapshot = await config_manager.list_catalog_configs(catalog_id, ctx=DriverContext(db_resource=conn))
                except Exception as e:
                    logger.debug(f"Could not list catalog configs before deletion: {e}")

            # Resolve physical schema directly (ignoring soft-delete status)
            physical_schema = await DQLQuery(
                "SELECT physical_schema FROM catalog.catalogs WHERE id = :catalog_id;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, catalog_id=catalog_id)

            # Dropping schema
            if physical_schema:
                logger.warning(f"DEBUG: delete_catalog: Dropping schema {physical_schema}")
                await _drop_schema_query.execute(conn, schema=physical_schema)
                logger.warning(f"DEBUG: delete_catalog: Schema dropped")

                # Remove all pg_cron jobs associated with this tenant schema.
                # Jobs use the physical schema name as a suffix or infix, e.g.:
                #   archive_catalog_events_s_abc12345
                #   monthly_cleanup_logs_s_abc12345
                #   prune_s_abc12345_events
                try:
                    deleted_jobs = await _delete_tenant_cron_jobs_query.execute(
                        conn, pattern=f"%{physical_schema}%"
                    )
                    if deleted_jobs:
                        logger.info(
                            f"Removed {deleted_jobs} cron job(s) for schema {physical_schema}"
                        )
                except Exception as cron_err:
                    logger.warning(
                        f"Could not remove cron jobs for {physical_schema} (non-fatal): {cron_err}"
                    )

            # Delete from catalogs table
            await _hard_delete_catalog_query.execute(conn, id=catalog_id)

            # Emit main HARD_DELETION event (triggers async destroyers)
            logger.warning(f"DEBUG: delete_catalog: Emitting CATALOG_HARD_DELETION")
            await emit_event(
                CatalogEventType.CATALOG_HARD_DELETION,
                catalog_id=catalog_id,
                db_resource=conn,
                physical_schema=physical_schema,
            )
            logger.warning(f"DEBUG: delete_catalog: Emitted CATALOG_HARD_DELETION")

            # Emit AFTER event
            logger.warning(f"DEBUG: delete_catalog: Emitting AFTER_CATALOG_HARD_DELETION")
            await emit_event(
                CatalogEventType.AFTER_CATALOG_HARD_DELETION,
                catalog_id=catalog_id,
                db_resource=conn,
                physical_schema=physical_schema,
            )
            logger.warning(f"DEBUG: delete_catalog: Emitted AFTER_CATALOG_HARD_DELETION")
        # Post-transaction cleanup
        self._get_catalog_model_cached.cache_invalidate(catalog_id)

        # Trigger async cleanup (external resources) if needed
        # The 'BEFORE_CATALOG_HARD_DELETION' event might have triggered async listeners?
        # In legacy, hard delete triggered `lifecycle_registry.destroy_async_catalog`

        if force and physical_schema:
            try:
                # Capture config snapshot if possible (best effort since it's already deleted)
                # In a real scenario, we should capture before delete.
                # But here we just trigger the destroyer.

                from dynastore.modules.catalog.lifecycle_manager import LifecycleContext

                lifecycle_registry.destroy_async_catalog(
                    catalog_id,
                    LifecycleContext(physical_schema=physical_schema, config=config_snapshot),
                )
            except Exception as e:
                logger.warning(
                    f"Failed to trigger async destroy for catalog {catalog_id}: {e}"
                )

        return True

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ):
        return await self._col_svc.list_collections(
            catalog_id, limit=limit, offset=offset, lang=lang, ctx=ctx, q=q
        )

    async def get_collection_model(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        db_resource = ctx.db_resource if ctx else None
        return await self._col_svc.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Set[str]:
        return await self._col_svc.get_collection_column_names(
            catalog_id, collection_id, ctx=ctx
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_definition: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> Collection:
        return await self._col_svc.create_collection(
            catalog_id,
            collection_definition,
            lang=lang,
            ctx=ctx,
            **kwargs,
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Dict[str, Any],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.update_collection(
            catalog_id, collection_id, updates, lang=lang, ctx=ctx
        )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        return await self._col_svc.delete_collection(
            catalog_id, collection_id, force=force, ctx=ctx
        )

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        return await self._col_svc.delete_collection_language(
            catalog_id, collection_id, lang, ctx=ctx
        )

    async def create_physical_collection(
        self,
        conn,
        schema: str,
        catalog_id: str,
        collection_id: str,
        physical_table: Optional[str] = None,
        layer_config=None,
        **kwargs,
    ):
        from dynastore.modules.storage.router import get_driver

        try:
            driver = await get_driver("WRITE", catalog_id, collection_id)
        except ValueError:
            return
        await driver.ensure_storage(  # type: ignore[attr-defined]
            catalog_id,
            collection_id,
            physical_table=physical_table,
            layer_config=layer_config,
            db_resource=conn,
        )

    # --- Item Operations (delegated) ---

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk) via ItemService."""
        return await self._item_svc.upsert(
            catalog_id,
            collection_id,
            items,
            ctx=ctx,
            processing_context=processing_context,
        )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: Any,
        ctx: Optional[DriverContext] = None,
        lang: str = "en",
        context: Optional[Any] = None,
    ):
        return await self._item_svc.get_item(
            catalog_id, collection_id, item_id, ctx=ctx, lang=lang, context=context
        )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        # Resolves ID internally in ItemService
        return await self._item_svc.delete_item(
            catalog_id, collection_id, item_id, ctx=ctx
        )

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        return await self._item_svc.delete_item_language(
            catalog_id, collection_id, item_id, lang, ctx=ctx
        )

    @property
    def count_items_by_asset_id_query(self) -> Any:
        return self._item_svc.count_items_by_asset_id_query

    def map_row_to_feature(
        self,
        row: Any,
        col_config: CollectionPluginConfig,
        lang: str = "en",
    ) -> Feature:
        return self._item_svc.map_row_to_feature(  # type: ignore[return-value]
            row, col_config, lang=lang
        )

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        return await self._item_svc.get_collection_schema(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def search(
        self,
        catalog_id: str,
        collection_id: str,
        filter_cql: Optional[str] = None,
        properties: Optional[List[str]] = None,
        include_geometry: bool = True,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """
        High-level search helper that returns a FeatureCollection structure.
        Uses raw_where for CQL support for now.
        """
        from dynastore.models.query_builder import (
            QueryRequest,
            FieldSelection,
            FilterCondition,
        )

        # 1. Build QueryRequest
        selects = []

        # Geometry
        if include_geometry:
            selects.append(FieldSelection(field="geom"))

        # Properties
        if properties:
            for p in properties:
                selects.append(FieldSelection(field=p))
        else:
            if properties is None:
                selects.append(FieldSelection(field="*"))

        # Build Request
        raw_where = filter_cql

        request = QueryRequest(  # type: ignore[call-arg]
            select=selects, limit=limit, offset=offset, raw_where=raw_where
        )

        items = await self.search_items(
            catalog_id, collection_id, request
        )

        return {"type": "FeatureCollection", "features": items}

    async def get_features_query(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        params: Dict[str, Any],
        param_suffix: str = "",
    ) -> Tuple[str, Dict[str, Any]]:
        return await self._item_svc.get_features_query(
            conn, catalog_id, collection_id, col_config, params, param_suffix
        )

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
    ) -> List[Dict[str, Any]]:
        """Search and retrieve items using optimized query generation."""
        return await self._item_svc.search_items(  # type: ignore[return-value]
            catalog_id, collection_id, request, config=config, ctx=ctx
        )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: "Optional[ConsumerType]" = None,
    ) -> QueryResponse:
        """Stream search results using an async iterator."""
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType as _CT
        return await self._item_svc.stream_items(
            catalog_id, collection_id, request,
            config=config, ctx=ctx,
            consumer=consumer or _CT.GENERIC,
        )

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """
        Retrieves field definitions for a physical table.
        Used by WFS to map SQL types without full reflection.
        Delegates to ItemService.
        """
        return await self._item_svc.get_collection_fields(
            catalog_id,
            collection_id,
            db_resource=db_resource,
        )

    async def update_provisioning_status(
        self, catalog_id: str, status: str, ctx: Optional["DriverContext"] = None
    ) -> bool:
        """Updates the provisioning status (provisioning | ready | failed) for a catalog."""
        db_resource = ctx.db_resource if ctx else None
        sql = "UPDATE catalog.catalogs SET provisioning_status = :status WHERE id = :id RETURNING id;"
        async with managed_transaction(get_catalog_engine(db_resource)) as conn:
            result = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn, id=catalog_id, status=status
            )
            if result:
                self._get_catalog_model_cached.cache_invalidate(catalog_id)
                return True
        return False


# --- Standalone Utilities ---


async def ensure_catalog_exists(
    db_resource: DbResource,
    catalog_id: str,
    title: Optional[LocalizedText] = None,
    description: Optional[LocalizedText] = None,
):
    """Standalone helper to ensure a catalog exists."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    _ctx = DriverContext(db_resource=db_resource) if db_resource else None
    if catalogs:
        await catalogs.ensure_catalog_exists(catalog_id, ctx=_ctx)
    else:
        # Fallback if discovery not ready
        service = CatalogService(db_resource)  # type: ignore[abstract]
        if not await service.get_catalog_model(catalog_id, ctx=_ctx):
            await service.create_catalog(
                {"id": catalog_id, "title": title, "description": description},
                ctx=_ctx,
            )


async def ensure_collection_exists(
    db_resource: DbResource,
    catalog_id: str,
    collection_id: str,
    title: Optional[LocalizedText] = None,
    description: Optional[LocalizedText] = None,
):
    """Standalone helper to ensure a collection exists."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)

    # Ensure catalog first
    await ensure_catalog_exists(db_resource, catalog_id)

    _ctx = DriverContext(db_resource=db_resource) if db_resource else None
    if catalogs:
        if not await catalogs.get_collection(  # type: ignore[attr-defined]
            catalog_id, collection_id, ctx=_ctx
        ):
            await catalogs.create_collection(
                catalog_id,
                {"id": collection_id, "title": title, "description": description},
                ctx=_ctx,
            )
    else:
        # Fallback
        service = CatalogService(db_resource)  # type: ignore[abstract]
        if not await service.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            await service.create_collection(
                catalog_id,
                {"id": collection_id, "title": title, "description": description},
                ctx=_ctx,
            )
