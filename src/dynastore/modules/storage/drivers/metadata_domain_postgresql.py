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

"""
Domain-scoped Primary metadata drivers (M2.1 of the role-based driver refactor).

Four drivers backed by the M2.0 DDL split:

- :class:`CollectionCorePostgresqlDriver`  → ``{schema}.collection_metadata_core``
- :class:`CollectionStacPostgresqlDriver`  → ``{schema}.collection_metadata_stac``
- :class:`CatalogCorePostgresqlDriver`     → ``catalog.catalog_metadata_core``
- :class:`CatalogStacPostgresqlDriver`     → ``catalog.catalog_metadata_stac``

Each driver owns only its **domain's** columns.  Callers that need the full
envelope run both Primary drivers (CORE + STAC) through the metadata router
and merge the results.  Wiring the router to fan out to these drivers
lands in M2.3 — this module is additive and does not change current
behaviour.  The legacy :class:`MetadataPostgresqlDriver` remains the
production source of truth until M2.5 deletes it.

Naming convention (Phase 1 of the naming harmonisation):

- ``<Tier><Domain><Backend>Driver`` — ``Collection`` / ``Catalog`` tier,
  ``Core`` / ``Stac`` domain, ``Postgresql`` backend.  No ``Metadata``
  infix — the tier + domain combination already makes the scope clear
  (``CollectionCorePostgresqlDriver`` reads as "collection-tier CORE
  metadata driver backed by PostgreSQL").

Capabilities
------------

All four drivers declare ``READ``, ``WRITE``, ``SOFT_DELETE``, and
``PHYSICAL_ADDRESSING``.  Additional capabilities per driver:

- CORE drivers        — ``SEARCH`` (keyword match on title/description).
- STAC collection     — ``SPATIAL_FILTER`` (bbox match on extent).
- STAC catalog        — no SPATIAL_FILTER (catalog STAC has no extent).

Configs
-------

Each driver has an empty-default ``PluginConfig`` subclass so the waterfall
can still dispatch to them without any explicit per-collection config.
No fields are declared — they exist purely as Pydantic identity markers
for the ``config_rewriter`` / ``ConfigsProtocol`` plumbing.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.driver_roles import MetadataDomain
from dynastore.models.protocols.metadata_driver import (
    CatalogMetadataStore,
    CollectionMetadataStore,
    MetadataCapability,
)
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.storage.driver_config import DriverPluginConfig

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain column sets (single source of truth — referenced by both the SQL
# builders and the domain-filter helpers below)
# ---------------------------------------------------------------------------

_COLLECTION_CORE_COLUMNS: Tuple[str, ...] = (
    "title", "description", "keywords", "license", "extra_metadata",
)
_COLLECTION_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "extent", "providers",
    "summaries", "links", "assets", "item_assets",
)
_CATALOG_CORE_COLUMNS: Tuple[str, ...] = (
    "title", "description", "keywords", "license", "extra_metadata",
)
_CATALOG_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "conforms_to", "links", "assets",
)


# ---------------------------------------------------------------------------
# Helpers — shared between the four drivers
# ---------------------------------------------------------------------------


def _get_engine() -> Any:
    """Fetch the async DatabaseProtocol engine, or return None if unavailable."""
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.tools.discovery import get_protocol

    db = get_protocol(DatabaseProtocol)
    return db.engine if db else None


async def _resolve_physical_schema(
    catalog_id: str, *, db_resource: Optional[DbResource] = None,
) -> Optional[str]:
    """Resolve the physical schema for ``catalog_id`` via CatalogsProtocol."""
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        return None
    return await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=db_resource)
    )


def _to_json(value: Any) -> Optional[str]:
    """Serialise ``value`` as a JSON string for PG JSONB storage."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except Exception:
        return None


def _deserialise_jsonb(row: Dict[str, Any], columns: Tuple[str, ...]) -> Dict[str, Any]:
    """Parse JSONB string columns back into Python objects in-place."""
    for key in columns:
        val = row.get(key)
        if isinstance(val, str):
            try:
                row[key] = json.loads(val)
            except Exception:
                row[key] = None
    return row


def _filter_payload(
    metadata: Dict[str, Any], columns: Tuple[str, ...],
) -> Dict[str, Any]:
    """Return the subset of ``metadata`` whose keys match ``columns``.

    Callers pass the full envelope; each driver only persists the keys it
    owns.  Unknown keys are silently dropped — the other domain's driver
    will pick them up in its own upsert.
    """
    return {k: metadata.get(k) for k in columns}


# ---------------------------------------------------------------------------
# PluginConfig subclasses (identity-only — no fields)
# ---------------------------------------------------------------------------


class CollectionCorePostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CollectionCorePostgresqlDriver.

    No fields today.  Future additions (e.g. batching, cache TTLs) layer
    here so the rewriter / waterfall plumbing needs no changes.
    """


class CollectionStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CollectionStacPostgresqlDriver."""


class CatalogCorePostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CatalogCorePostgresqlDriver."""


class CatalogStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CatalogStacPostgresqlDriver."""


# ---------------------------------------------------------------------------
# Collection-tier drivers (per-tenant {schema}.collection_metadata_*)
# ---------------------------------------------------------------------------


class _CollectionMetadataDomainBase:
    """Shared implementation for the two collection-tier metadata drivers.

    Domain-specific knobs (table name, column tuple, capability set,
    ``domain`` ClassVar) live on the concrete subclasses; this base
    handles the CRUD bodies that don't vary by domain.

    The class explicitly does NOT inherit ``CollectionMetadataStore`` —
    that protocol is a structural ``typing.Protocol`` (`runtime_checkable`)
    and instances of the concrete subclasses satisfy it via duck-typing.
    """

    # Declared on subclasses.
    _table: ClassVar[str]              # e.g. "collection_metadata_core"
    _columns: ClassVar[Tuple[str, ...]]  # columns this driver owns
    capabilities: FrozenSet[str]
    domain: ClassVar[MetadataDomain]

    async def is_available(self) -> bool:
        return _get_engine() is not None

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        engine = db_resource or _get_engine()
        if not engine:
            return None
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return None
            sql = (
                f'SELECT * FROM "{phys}".{self._table} '
                f'WHERE collection_id = :id;'
            )
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=collection_id)
            if not row:
                return None
            return _deserialise_jsonb(dict(row), self._columns)

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        payload = _filter_payload(metadata, self._columns)
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                raise RuntimeError(f"No physical schema for catalog '{catalog_id}'")
            col_list = ", ".join(self._columns)
            val_placeholders = ", ".join(f":{c}" for c in self._columns)
            update_list = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in self._columns
            )
            sql = (
                f'INSERT INTO "{phys}".{self._table} '
                f'(collection_id, {col_list}, updated_at) '
                f'VALUES (:id, {val_placeholders}, NOW()) '
                f'ON CONFLICT (collection_id) DO UPDATE SET '
                f'{update_list}, updated_at = NOW();'
            )
            params: Dict[str, Any] = {"id": collection_id}
            for c in self._columns:
                params[c] = _to_json(payload.get(c))
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, **params
            )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return
            if soft and "extra_metadata" in self._columns:
                # Tombstone via extra_metadata JSON — preserves row for recovery.
                sql = (
                    f'UPDATE "{phys}".{self._table} SET '
                    f"extra_metadata = jsonb_set("
                    f"  COALESCE(extra_metadata, '{{}}'::jsonb), "
                    f"  '{{_deleted_at}}', to_jsonb(now()::text)), "
                    f'updated_at = NOW() '
                    f'WHERE collection_id = :id;'
                )
            else:
                sql = (
                    f'DELETE FROM "{phys}".{self._table} '
                    f'WHERE collection_id = :id;'
                )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, id=collection_id
            )

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
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Default implementation: subclasses override when they support search."""
        return [], 0

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Empty default — these drivers have no per-collection config today."""
        return None

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        from dynastore.modules.storage.storage_location import StorageLocation

        phys = await _resolve_physical_schema(catalog_id) or catalog_id
        return StorageLocation(
            backend="postgresql",
            canonical_uri=f"postgresql://{phys}.{self._table}",
            identifiers={
                "schema": phys, "table": self._table, "domain": self.domain.value,
            },
            display_label=f"{phys}.{self._table}",
        )


class CollectionCorePostgresqlDriver(_CollectionMetadataDomainBase):
    """Primary driver for CORE collection metadata (``title``, ``description``, …).

    Backs ``{schema}.collection_metadata_core``.  Declares ``SEARCH`` because
    CORE fields carry the human-readable text the ``/search?q=…`` endpoint
    matches against.
    """

    _table: ClassVar[str] = "collection_metadata_core"
    _columns: ClassVar[Tuple[str, ...]] = _COLLECTION_CORE_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.CORE

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SEARCH,
        MetadataCapability.SEARCH_EXACT,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.PHYSICAL_ADDRESSING,
        MetadataCapability.QUERY_FALLBACK_SOURCE,
    })

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
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Keyword search on title/description (CORE columns only)."""
        engine = db_resource or _get_engine()
        if not engine:
            return [], 0
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return [], 0
            where_clauses = ["c.deleted_at IS NULL"]
            params: Dict[str, Any] = {"limit": limit, "offset": offset}
            if q:
                where_clauses.append(
                    "(c.id ILIKE :q OR m.title->>'en' ILIKE :q "
                    "OR m.description->>'en' ILIKE :q)"
                )
                params["q"] = f"%{q}%"
            where_sql = " AND ".join(where_clauses)
            count_sql = (
                f'SELECT COUNT(*) FROM "{phys}".collections c '
                f'LEFT JOIN "{phys}".collection_metadata_core m '
                f'ON m.collection_id = c.id WHERE {where_sql};'
            )
            total = await DQLQuery(
                count_sql, result_handler=ResultHandler.SCALAR,
            ).execute(conn, **params) or 0
            col_list = ", ".join(f"m.{c}" for c in self._columns)
            data_sql = (
                f'SELECT c.id, {col_list} '
                f'FROM "{phys}".collections c '
                f'LEFT JOIN "{phys}".collection_metadata_core m '
                f'ON m.collection_id = c.id '
                f'WHERE {where_sql} '
                f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
            )
            rows = await DQLQuery(
                data_sql, result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn, **params)
            return [_deserialise_jsonb(dict(r), self._columns) for r in rows], total


class CollectionStacPostgresqlDriver(_CollectionMetadataDomainBase):
    """Primary driver for STAC collection metadata (``extent``, ``providers``, …).

    Backs ``{schema}.collection_metadata_stac``.  Declares ``SPATIAL_FILTER``
    because the ``extent`` column carries the STAC bbox the spatial-filter
    endpoints match against.
    """

    _table: ClassVar[str] = "collection_metadata_stac"
    _columns: ClassVar[Tuple[str, ...]] = _COLLECTION_STAC_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.STAC

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.SPATIAL_FILTER,
        MetadataCapability.PHYSICAL_ADDRESSING,
    })


# ---------------------------------------------------------------------------
# Catalog-tier drivers (global catalog.catalog_metadata_*)
# ---------------------------------------------------------------------------


class _CatalogMetadataDomainBase:
    """Shared implementation for the two catalog-tier metadata drivers."""

    _table: ClassVar[str]              # e.g. "catalog_metadata_core"
    _columns: ClassVar[Tuple[str, ...]]  # columns this driver owns
    capabilities: FrozenSet[str]
    domain: ClassVar[MetadataDomain]

    async def is_available(self) -> bool:
        return _get_engine() is not None

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        engine = db_resource or _get_engine()
        if not engine:
            return None
        async with managed_transaction(engine) as conn:
            sql = (
                f"SELECT * FROM catalog.{self._table} "
                f"WHERE catalog_id = :id;"
            )
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=catalog_id)
            if not row:
                return None
            return _deserialise_jsonb(dict(row), self._columns)

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        payload = _filter_payload(metadata, self._columns)
        async with managed_transaction(engine) as conn:
            col_list = ", ".join(self._columns)
            val_placeholders = ", ".join(f":{c}" for c in self._columns)
            update_list = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in self._columns
            )
            sql = (
                f"INSERT INTO catalog.{self._table} "
                f"(catalog_id, {col_list}, updated_at) "
                f"VALUES (:id, {val_placeholders}, NOW()) "
                f"ON CONFLICT (catalog_id) DO UPDATE SET "
                f"{update_list}, updated_at = NOW();"
            )
            params: Dict[str, Any] = {"id": catalog_id}
            for c in self._columns:
                params[c] = _to_json(payload.get(c))
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, **params
            )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(engine) as conn:
            if soft and "extra_metadata" in self._columns:
                sql = (
                    f"UPDATE catalog.{self._table} SET "
                    f"extra_metadata = jsonb_set("
                    f"  COALESCE(extra_metadata, '{{}}'::jsonb), "
                    f"  '{{_deleted_at}}', to_jsonb(now()::text)), "
                    f"updated_at = NOW() "
                    f"WHERE catalog_id = :id;"
                )
            else:
                sql = (
                    f"DELETE FROM catalog.{self._table} WHERE catalog_id = :id;"
                )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, id=catalog_id
            )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        return None


class CatalogCorePostgresqlDriver(_CatalogMetadataDomainBase):
    """Primary driver for CORE catalog metadata.

    Backs ``catalog.catalog_metadata_core``.  Scope: ``title``,
    ``description``, ``keywords``, ``license``, ``extra_metadata``.
    """

    _table: ClassVar[str] = "catalog_metadata_core"
    _columns: ClassVar[Tuple[str, ...]] = _CATALOG_CORE_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.CORE

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.QUERY_FALLBACK_SOURCE,
    })


class CatalogStacPostgresqlDriver(_CatalogMetadataDomainBase):
    """Primary driver for STAC catalog metadata.

    Backs ``catalog.catalog_metadata_stac``.  Scope: ``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, ``assets``.
    """

    _table: ClassVar[str] = "catalog_metadata_stac"
    _columns: ClassVar[Tuple[str, ...]] = _CATALOG_STAC_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.STAC

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
    })


# ---------------------------------------------------------------------------
# M2.2 — catalog-metadata lifecycle hooks
# ---------------------------------------------------------------------------
#
# Registered on the lifecycle registry at module-import time.  Fire from
# ``init_catalog_metadata`` — the phase invoked by
# ``CatalogService.create_catalog`` AFTER the ``catalog.catalogs`` registry
# row has been committed (so FK references into catalog.catalogs(id) from
# the per-domain metadata tables are satisfied).
#
# Default-fast invariant: a hook is a no-op when the caller didn't supply
# a ``catalog_metadata`` kwarg.  Empty / missing → zero writes.
#
# The STAC hook is kept in this file for M2.2 simplicity.  A follow-up
# sub-PR can move it to the STAC extension so the STAC-specific table
# (``catalog.catalog_metadata_stac``) is owned end-to-end by the
# extension's lifecycle.


from dynastore.modules.catalog.lifecycle_manager import (  # noqa: E402
    sync_catalog_metadata_initializer,
)


@sync_catalog_metadata_initializer(priority=5)
async def _pg_catalog_core_init(
    conn: DbResource,
    schema: str,
    catalog_id: str,
    *,
    catalog_metadata: Optional[Dict[str, Any]] = None,
    **_ignored: Any,
) -> None:
    """Persist CORE catalog metadata into ``catalog.catalog_metadata_core``.

    Idempotent via ``ON CONFLICT (catalog_id) DO UPDATE`` inside
    :meth:`CatalogCorePostgresqlDriver.upsert_catalog_metadata`.  Falls
    back to a silent no-op when the caller supplies nothing to persist
    (default-fast invariant: empty-body catalog creation writes zero
    catalog_metadata_core rows).
    """
    if not catalog_metadata:
        return
    driver = CatalogCorePostgresqlDriver()
    await driver.upsert_catalog_metadata(
        catalog_id, catalog_metadata, db_resource=conn,
    )


@sync_catalog_metadata_initializer(priority=10)
async def _pg_catalog_stac_init(
    conn: DbResource,
    schema: str,
    catalog_id: str,
    *,
    catalog_metadata: Optional[Dict[str, Any]] = None,
    **_ignored: Any,
) -> None:
    """Persist STAC catalog metadata into ``catalog.catalog_metadata_stac``.

    Gated on the presence of at least one STAC-relevant key in the
    supplied ``catalog_metadata`` — absence means "this catalog has no
    STAC envelope" and we skip the write entirely.  Priority 10 keeps
    it ordered after CORE (priority 5) so a future STAC hook reading
    the CORE row finds it already in place.
    """
    if not catalog_metadata:
        return
    if not any(k in catalog_metadata for k in _CATALOG_STAC_COLUMNS):
        return
    driver = CatalogStacPostgresqlDriver()
    await driver.upsert_catalog_metadata(
        catalog_id, catalog_metadata, db_resource=conn,
    )
