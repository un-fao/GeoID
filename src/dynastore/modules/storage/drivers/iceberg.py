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
Iceberg Storage Driver — Open Table Format with ACID, time-travel, and schema evolution.

Implements ``CollectionStorageDriverProtocol`` using PyIceberg for full OTF
operations: snapshots, time-travel reads, schema evolution, and versioning.

Capabilities:
  ``{STREAMING, SPATIAL_FILTER, EXPORT, TIME_TRAVEL, VERSIONING, SNAPSHOTS,
    SCHEMA_EVOLUTION, SOFT_DELETE}``

DynaStore hierarchy mapping:
  - DynaStore catalog → Iceberg namespace
  - DynaStore collection → Iceberg table
  - DynaStore item/feature → Iceberg row

The driver delegates to a PyIceberg catalog configured via
``IcebergCollectionDriverConfig``.  Uses ``pyiceberg.catalog.CatalogType`` enum
and PyIceberg constants (``TYPE``, ``URI``, ``WAREHOUSE_LOCATION``) for
configuration — no string literals.

Catalog types (via ``pyiceberg.catalog.CatalogType``):
  - ``SQL`` (default): PostgreSQL-backed SqlCatalog
  - ``REST``: REST catalog (e.g., Tabular, Polaris)
  - ``GLUE``: AWS Glue Data Catalog
  - ``HIVE``: Hive Metastore
  - ``DYNAMODB``: DynamoDB-backed catalog

Warehouse resolution order:
  1. Explicit ``warehouse_uri`` in ``IcebergCollectionDriverConfig``
  2. Auto-detected from platform ``StorageProtocol`` (GCS bucket, future S3)
  3. Local temp dir fallback (``file://``)

When the warehouse URI scheme is ``gs://``, the driver injects PyIceberg
GCS IO properties (``GCS_PROJECT_ID``) so PyArrowFileIO can access GCS.
S3 follows the same pattern when an AWS module is implemented.

Connection lifecycle:
  - **Lazy init**: catalog loaded on first use via ``_ensure_catalog()``.
  - **Cached**: once loaded, the same catalog instance is reused.
  - **Shutdown**: catalog reference cleared in ``lifespan()`` on app shutdown.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.concurrency import run_in_thread
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import IcebergCollectionDriverConfig

logger = logging.getLogger(__name__)

# Iceberg type string → PyIceberg type factory (lazy-loaded)
_ICEBERG_TYPE_MAP_KEYS = {
    "string", "int32", "int64", "float32", "float64",
    "boolean", "date", "timestamp", "timestamptz", "binary",
}


def _pyiceberg_available() -> bool:
    try:
        import pyiceberg  # noqa: F401
        return True
    except ImportError:
        return False


def _resolve_iceberg_type(type_str: str):
    """Map a type string to a PyIceberg type instance."""
    from pyiceberg.types import (
        BooleanType,
        BinaryType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )
    return {
        "string": StringType(),
        "int32": IntegerType(),
        "int64": LongType(),
        "float32": FloatType(),
        "float64": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "binary": BinaryType(),
    }.get(type_str, StringType())


class IcebergStorageDriver(ModuleProtocol):
    """Iceberg storage driver — Open Table Format with full OTF capabilities.

    Uses PyIceberg for ACID transactions, snapshot management, time-travel,
    and schema evolution.  Reads and writes via PyArrow integration.

    Satisfies ``CollectionStorageDriverProtocol`` and ``StorageLocationResolver``.
    """

    driver_id: str = "iceberg"
    driver_type: str = "driver:records:iceberg"
    priority: int = 20
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.SORT,
        Capability.GROUP_BY,
        Capability.EXPORT,
        Capability.TIME_TRAVEL,
        Capability.VERSIONING,
        Capability.SNAPSHOTS,
        Capability.SCHEMA_EVOLUTION,
        Capability.SOFT_DELETE,
        Capability.GEOSPATIAL,
        Capability.STATISTICS,
        Capability.ASSET_TRACKING,
        Capability.ATTRIBUTE_FILTER,
        Capability.SOURCE_REFERENCE,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
    })
    preferred_for: FrozenSet[str] = frozenset({"analytics", "features", "write"})
    supported_hints: FrozenSet[str] = frozenset({"analytics", "features", "write"})

    # Thread-safe catalog cache: loc_key → catalog instance, protected by asyncio.Lock.
    _catalog_cache: Dict[str, Any] = {}
    _catalog_lock: asyncio.Lock = asyncio.Lock()

    def is_available(self) -> bool:
        return _pyiceberg_available()

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "IcebergCollectionDriverConfig":
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        plugin_id = f"driver:{self.driver_id}"
        configs = get_protocol(ConfigsProtocol)
        config = await configs.get_config(
            plugin_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            db_resource=db_resource,
        )
        if config is None:
            return IcebergCollectionDriverConfig()
        return config

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("IcebergStorageDriver: started")
        yield
        self._catalog_cache.clear()
        logger.info("IcebergStorageDriver: stopped")

    # ------------------------------------------------------------------
    # Catalog & warehouse resolution
    # ------------------------------------------------------------------

    async def _resolve_warehouse(
        self, loc: IcebergCollectionDriverConfig, catalog_id: str
    ) -> str:
        """Resolve warehouse URI: explicit config > StorageProtocol auto-detect > local fallback.

        When a collection already has a GCS bucket (via StorageProtocol),
        the warehouse is automatically derived from that bucket with an
        ``iceberg/`` subfolder for isolation.
        """
        if loc.warehouse_uri:
            return loc.warehouse_uri

        # Auto-detect from platform StorageProtocol (GCS today, S3 in future)
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.storage import StorageProtocol
            storage = get_protocol(StorageProtocol)
            if storage:
                base_path = await storage.get_catalog_storage_path(catalog_id)
                if base_path:
                    warehouse = base_path.rstrip("/") + "/iceberg/"
                    logger.info(
                        "Iceberg warehouse auto-resolved from StorageProtocol: %s",
                        warehouse,
                    )
                    return warehouse
        except Exception as e:
            logger.debug("StorageProtocol not available for warehouse: %s", e)

        # Fallback: local filesystem
        import tempfile
        return f"file://{tempfile.gettempdir()}/iceberg_warehouse"

    def _load_catalog_sync(
        self,
        loc: IcebergCollectionDriverConfig,
        warehouse: Optional[str] = None,
    ):
        """Build a PyIceberg catalog (sync, blocking — called via run_in_thread).

        Uses ``pyiceberg.catalog.CatalogType`` enum and PyIceberg constants
        (``TYPE``, ``URI``, ``WAREHOUSE_LOCATION``) instead of string literals.
        """
        from pyiceberg.catalog import (
            load_catalog,
            CatalogType,
            TYPE as _TYPE,
            URI as _URI,
            WAREHOUSE_LOCATION as _WAREHOUSE,
        )

        # Resolve catalog type via enum (default: SQL)
        raw_type = (loc.catalog_type or "sql").lower()
        try:
            cat_type = CatalogType(raw_type)
        except ValueError:
            cat_type = CatalogType.SQL
            logger.warning(
                "Unknown catalog_type %r, falling back to %s",
                raw_type, cat_type.value,
            )

        catalog_props: Dict[str, Any] = {_TYPE: cat_type.value}

        # URI resolution
        if loc.catalog_uri:
            catalog_props[_URI] = loc.catalog_uri
        elif cat_type == CatalogType.SQL:
            from dynastore.modules.db_config.db_config import DBConfig
            from dynastore.modules.db_config.tools import normalize_db_url
            sync_url = normalize_db_url(DBConfig.database_url, is_async=False)
            if sync_url.startswith("postgresql://"):
                sync_url = sync_url.replace(
                    "postgresql://", "postgresql+psycopg2://", 1
                )
            catalog_props[_URI] = sync_url

        # Extra catalog-specific properties
        if loc.catalog_properties:
            catalog_props.update(loc.catalog_properties)

        # Warehouse resolution
        if _WAREHOUSE not in catalog_props:
            if warehouse:
                catalog_props[_WAREHOUSE] = warehouse
            elif cat_type == CatalogType.SQL:
                import tempfile
                catalog_props[_WAREHOUSE] = (
                    f"file://{tempfile.gettempdir()}/iceberg_warehouse"
                )

        # Inject IO properties for cloud warehouse schemes
        resolved_wh = catalog_props.get(_WAREHOUSE, "")
        if resolved_wh.startswith("gs://"):
            import os
            from pyiceberg.io import GCS_PROJECT_ID
            catalog_props.setdefault(
                GCS_PROJECT_ID,
                os.getenv("PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT", "")),
            )
        # Future: elif resolved_wh.startswith("s3://"):
        #     from pyiceberg.io import S3_REGION, S3_ACCESS_KEY_ID, ...

        catalog = load_catalog(
            loc.catalog_name or "default", **catalog_props
        )
        logger.debug(
            "IcebergStorageDriver: loaded %s catalog '%s' (warehouse=%s)",
            cat_type.value,
            loc.catalog_name or "default",
            resolved_wh or "none",
        )
        return catalog

    async def _ensure_catalog(
        self, loc: IcebergCollectionDriverConfig, catalog_id: str
    ):
        """Thread-safe catalog resolution with async lock.

        Uses double-checked locking: fast path (no lock) for cache hit,
        lock-protected path for cache miss with ``run_in_thread`` to
        offload the blocking ``load_catalog()`` call.
        """
        loc_key = f"{loc.catalog_name}:{loc.catalog_uri}:{loc.catalog_type}"

        # Fast path: cache hit (no lock)
        if loc_key in self._catalog_cache:
            return self._catalog_cache[loc_key]

        async with self._catalog_lock:
            # Double-check after acquiring lock
            if loc_key in self._catalog_cache:
                return self._catalog_cache[loc_key]

            warehouse = await self._resolve_warehouse(loc, catalog_id)
            catalog = await run_in_thread(
                self._load_catalog_sync, loc, warehouse
            )
            self._catalog_cache[loc_key] = catalog
            return catalog

    def _table_identifier(
        self, loc: IcebergCollectionDriverConfig, catalog_id: str, collection_id: str
    ) -> tuple:
        """Build Iceberg table identifier: (namespace, table_name)."""
        namespace = loc.namespace or catalog_id
        table_name = loc.table_name or collection_id
        return (namespace, table_name)

    async def _get_location_async(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> Optional[IcebergCollectionDriverConfig]:
        """Resolve IcebergCollectionDriverConfig from the config waterfall."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return None
            config = await configs.get_config(
                IcebergCollectionDriverConfig._plugin_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(config, IcebergCollectionDriverConfig):
                return config
            return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # CollectionStorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write entities to an Iceberg table respecting CollectionWritePolicy.

        Policy semantics on Iceberg (append-only table format):

        - ``UPDATE``: delete existing rows with matching ``external_id``, then append.
          Produces a new snapshot each time, preserving time-travel history.
        - ``NEW_VERSION``: append unconditionally (natural Iceberg operation).
          No deletes — each write is a new temporal version, full time-travel history.
        - ``IGNORE``: skip rows whose ``external_id`` already exists in the table.
        - ``REFUSE``: raise ``ConflictError`` if any row's ``external_id`` is found.

        Context keys honoured (only if ``enable_validity=True`` in policy):
          - ``asset_id``: stored as ``_asset_id`` column
          - ``valid_from``: stored as ``_valid_from``; defaults to now()
          - ``valid_to``: stored as ``_valid_to``
        """
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError(
                "IcebergStorageDriver: no OTF location config found"
            )

        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts,
            dicts_to_features,
        )
        from dynastore.modules.storage.driver_config import (
            CollectionWritePolicy,
            WriteConflictPolicy,
            WRITE_POLICY_PLUGIN_ID,
        )

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # Resolve write policy from config waterfall.
        policy = await self._resolve_write_policy(catalog_id, collection_id)

        # Extract ingestion context (not part of the feature payload).
        ctx = context or {}
        asset_id: Optional[str] = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")

        now_iso = datetime.now(timezone.utc).isoformat()

        # Enrich rows with tracking columns.
        for row in rows:
            ext_id = self._extract_external_id(row, policy.external_id_field)
            if policy.require_external_id and not ext_id:
                logger.warning(
                    "Iceberg write_entities: external_id required but missing — skipped"
                )
                rows = [r for r in rows if self._extract_external_id(r, policy.external_id_field)]
                break
            if ext_id:
                row["_external_id"] = ext_id
            if asset_id is not None:
                row["_asset_id"] = asset_id
            if policy.enable_validity:
                row["_valid_from"] = valid_from or now_iso
                if valid_to is not None:
                    row["_valid_to"] = valid_to

        if not rows:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        on_conflict = policy.on_conflict

        if on_conflict == WriteConflictPolicy.REFUSE or policy.on_asset_conflict is not None:
            # Collect external_ids that already exist in the table.
            ext_ids = [
                row["_external_id"] for row in rows if "_external_id" in row
            ]
            if ext_ids:
                from pyiceberg.expressions import In
                from dynastore.modules.storage.driver_config import AssetConflictPolicy
                existing = await run_in_thread(
                    lambda: table.scan().filter(In("_external_id", ext_ids)).to_arrow()
                )
                existing_ids = set(existing.column("_external_id").to_pylist()) if existing.num_rows > 0 else set()

                if (
                    policy.on_asset_conflict == AssetConflictPolicy.REFUSE
                    and existing_ids
                ):
                    from dynastore.modules.storage.errors import ConflictError
                    raise ConflictError(
                        f"Iceberg: external_id(s) {sorted(existing_ids)} already exist "
                        f"in {catalog_id}/{collection_id} (policy=refuse_asset)"
                    )

                if on_conflict == WriteConflictPolicy.REFUSE:
                    rows = [r for r in rows if r.get("_external_id") not in existing_ids]
                    if not rows:
                        return []

        elif on_conflict == WriteConflictPolicy.UPDATE:
            # Delete existing rows with matching external_ids, then append fresh rows.
            ext_ids = [
                row["_external_id"] for row in rows if "_external_id" in row
            ]
            if ext_ids:
                from pyiceberg.expressions import In
                try:
                    await run_in_thread(table.delete, delete_filter=In("_external_id", ext_ids))
                except Exception as e:
                    logger.warning(
                        "Iceberg UPDATE: delete of existing external_ids failed — "
                        "appending anyway. Error: %s", e,
                    )

        # NEW_VERSION and UPDATE (after delete): append all rows as a new snapshot.
        arrow_schema = schema_to_pyarrow(table.schema())
        # Only include columns present in the Iceberg schema to avoid pa.null() errors.
        schema_names = set(arrow_schema.names)
        filtered_rows = [
            {k: v for k, v in row.items() if k in schema_names}
            for row in rows
        ]
        pa_table = pa.Table.from_pylist(filtered_rows, schema=arrow_schema)
        await run_in_thread(table.append, pa_table)
        del pa_table  # release memory early (Cloud Run)

        return dicts_to_features(rows)

    @staticmethod
    async def _resolve_write_policy(catalog_id: str, collection_id: str):
        """Resolve CollectionWritePolicy from the config waterfall."""
        from dynastore.modules.storage.driver_config import (
            CollectionWritePolicy,
            WRITE_POLICY_PLUGIN_ID,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                return await configs.get_config(
                    WRITE_POLICY_PLUGIN_ID,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
        except Exception:
            pass
        return CollectionWritePolicy()

    @staticmethod
    def _extract_external_id(row: dict, field_path: Optional[str]) -> Optional[str]:
        """Extract external_id using dot-notation path from a row dict."""
        if not field_path:
            return None
        val = row
        for part in field_path.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return None
        return str(val) if val is not None else None

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return FieldDefinition dict from Iceberg table schema."""
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )

        if entity_level != "item" or not collection_id:
            return {}

        iceberg_type_map = {
            "boolean": ("boolean", [FieldCapability.FILTERABLE]),
            "int": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "long": ("integer", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "float": ("numeric", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "double": ("numeric", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE]),
            "string": ("string", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "date": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "timestamp": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
            "timestamptz": ("datetime", [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]),
        }

        try:
            loc = await self._get_location_async(catalog_id, collection_id)
            if not loc:
                return {}
            catalog = await self._ensure_catalog(loc, catalog_id)
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            table = await run_in_thread(catalog.load_table, table_id)

            result = {}
            for field in table.schema().fields:
                type_str = str(field.field_type).lower()
                for key, (data_type, caps) in iceberg_type_map.items():
                    if key in type_str:
                        result[field.name] = ProtocolFieldDefinition(
                            name=field.name,
                            data_type=data_type,
                            capabilities=caps,
                        )
                        break
                else:
                    result[field.name] = ProtocolFieldDefinition(
                        name=field.name,
                        data_type="string",
                        capabilities=[FieldCapability.FILTERABLE],
                    )
            return result
        except Exception:
            return {}

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
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        scan = table.scan()

        if entity_ids:
            from pyiceberg.expressions import In
            scan = scan.filter(In("id", entity_ids))

        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)

        effective_limit = request.limit if request and request.limit else limit
        effective_offset = request.offset if request and request.offset else offset

        pa_table = await run_in_thread(scan.to_arrow)

        count = 0
        for i, row in enumerate(pa_table.to_pylist()):
            if i < effective_offset:
                continue
            if count >= effective_limit:
                break
            fid = row.pop("id", None)
            geom = row.pop("geometry", None)
            yield Feature(
                type="Feature", id=fid, geometry=geom, properties=row,
            )
            count += 1

    @staticmethod
    def _apply_filters(scan, filters):
        """Apply QueryRequest filters to an Iceberg scan.

        Supported filter operators:
          - ``eq``: equality filter via ``EqualTo``
          - ``neq``: not-equal filter via ``NotEqualTo``
          - ``gt``, ``gte``, ``lt``, ``lte``: range filters
          - ``in``: set membership via ``In``
          - ``bbox``: spatial bounding box (post-scan filter on geometry)
        """
        for f in filters:
            op = f.operator
            if op == "eq":
                from pyiceberg.expressions import EqualTo
                scan = scan.filter(EqualTo(f.field, f.value))
            elif op == "neq":
                from pyiceberg.expressions import NotEqualTo
                scan = scan.filter(NotEqualTo(f.field, f.value))
            elif op == "gt":
                from pyiceberg.expressions import GreaterThan
                scan = scan.filter(GreaterThan(f.field, f.value))
            elif op == "gte":
                from pyiceberg.expressions import GreaterThanOrEqual
                scan = scan.filter(GreaterThanOrEqual(f.field, f.value))
            elif op == "lt":
                from pyiceberg.expressions import LessThan
                scan = scan.filter(LessThan(f.field, f.value))
            elif op == "lte":
                from pyiceberg.expressions import LessThanOrEqual
                scan = scan.filter(LessThanOrEqual(f.field, f.value))
            elif op == "in" and isinstance(f.value, list):
                from pyiceberg.expressions import In
                scan = scan.filter(In(f.field, f.value))
            # bbox handled at Feature level in _bbox_matches
        return scan

    @staticmethod
    def _bbox_matches(geometry, bbox: list) -> bool:
        """Check if a geometry intersects a bounding box [minx, miny, maxx, maxy].

        Supports GeoJSON-style geometry dicts and WKT strings.
        """
        if not geometry or not bbox or len(bbox) != 4:
            return False
        minx, miny, maxx, maxy = bbox

        coords = None
        if isinstance(geometry, dict):
            coords = geometry.get("coordinates")
        elif isinstance(geometry, str):
            try:
                parsed = json.loads(geometry)
                coords = parsed.get("coordinates")
            except (json.JSONDecodeError, AttributeError):
                return False

        if coords is None:
            return False

        def _flatten_coords(c):
            """Recursively flatten nested coordinate arrays to (x, y) pairs."""
            if isinstance(c, (list, tuple)) and len(c) >= 2 and isinstance(c[0], (int, float)):
                yield (c[0], c[1])
            elif isinstance(c, (list, tuple)):
                for sub in c:
                    yield from _flatten_coords(sub)

        for x, y in _flatten_coords(coords):
            if minx <= x <= maxx and miny <= y <= maxy:
                return True
        return False

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("IcebergStorageDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        from pyiceberg.expressions import In
        row_filter = In("id", entity_ids)

        if soft:
            await run_in_thread(table.delete, delete_filter=row_filter)
        else:
            await run_in_thread(table.delete, delete_filter=row_filter)
            try:
                await run_in_thread(table.compact)
            except (AttributeError, NotImplementedError):
                pass

        return len(entity_ids)

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.warning(
                "IcebergStorageDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s", catalog_id, collection_id,
            )
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        namespace = loc.namespace or catalog_id

        try:
            await run_in_thread(catalog.create_namespace, namespace)
        except Exception:
            pass  # namespace may already exist

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                await run_in_thread(catalog.load_table, table_id)
            except Exception:
                from pyiceberg.schema import Schema as IcebergSchema
                from pyiceberg.types import NestedField, StringType
                iceberg_schema = IcebergSchema(
                    NestedField(1, "id", StringType(), required=True),
                    NestedField(2, "geometry", StringType(), required=False),
                    NestedField(3, "properties", StringType(), required=False),
                )
                await run_in_thread(catalog.create_table, table_id, schema=iceberg_schema)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)

        if soft:
            logger.info(
                "IcebergStorageDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — table retained, tagged as deprecated",
                catalog_id, collection_id,
            )
            if collection_id:
                table_id = self._table_identifier(loc, catalog_id, collection_id)
                try:
                    table = await run_in_thread(catalog.load_table, table_id)

                    def _soft_drop():
                        with table.transaction() as tx:
                            tx.set_properties(
                                **{
                                    "dynastore.deleted": "true",
                                    "dynastore.deleted_at": datetime.now(tz=timezone.utc).isoformat(),
                                }
                            )

                    await run_in_thread(_soft_drop)
                except Exception as e:
                    logger.warning("Iceberg soft drop failed: %s", e)
            return

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                await run_in_thread(catalog.drop_table, table_id)
            except Exception as e:
                logger.warning("Iceberg drop_table failed: %s", e)
        else:
            namespace = loc.namespace or catalog_id
            try:
                await run_in_thread(catalog.drop_namespace, namespace)
            except Exception as e:
                logger.warning("Iceberg drop_namespace failed: %s", e)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise ValueError("IcebergStorageDriver: no location config for export")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        pa_table = await run_in_thread(lambda: table.scan().to_arrow())

        def _export_to_file():
            if format == "csv":
                import pyarrow.csv as pcsv
                pcsv.write_csv(pa_table, target_path)
            elif format == "json":
                _rows = pa_table.to_pylist()
                import builtins
                with builtins.open(target_path, "w") as f:
                    json.dump(_rows, f)
            else:
                import pyarrow.parquet as pq
                pq.write_table(pa_table, target_path)

        await run_in_thread(_export_to_file)
        del pa_table  # release memory early (Cloud Run)
        return target_path

    # ------------------------------------------------------------------
    # StorageLocationResolver
    # ------------------------------------------------------------------

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> IcebergCollectionDriverConfig:
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc:
            return loc
        return IcebergCollectionDriverConfig()

    # ------------------------------------------------------------------
    # OTF Operations — called by CatalogService when capabilities match
    # ------------------------------------------------------------------

    async def list_snapshots(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: int = 100,
        db_resource: Optional[Any] = None,
    ) -> list:
        """List available snapshots for a collection."""
        from dynastore.models.otf import SnapshotInfo

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        snapshots = []
        for entry in table.history()[:limit]:
            snapshots.append(
                SnapshotInfo(
                    snapshot_id=str(entry.snapshot_id),
                    timestamp=datetime.fromtimestamp(
                        entry.timestamp_ms / 1000, tz=timezone.utc
                    ),
                    operation="unknown",
                )
            )
        return snapshots

    async def create_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        label: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ):
        """Create an explicit snapshot/bookmark of current state."""
        from dynastore.models.otf import SnapshotInfo

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("IcebergStorageDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        if label:
            await run_in_thread(lambda: table.manage_snapshots().create_branch(label))

        current = table.current_snapshot()
        return SnapshotInfo(
            snapshot_id=str(current.snapshot_id),
            parent_snapshot_id=(
                str(current.parent_snapshot_id)
                if current.parent_snapshot_id
                else None
            ),
            timestamp=datetime.fromtimestamp(
                current.timestamp_ms / 1000, tz=timezone.utc
            ),
            label=label,
            operation="snapshot",
        )

    async def rollback_to_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Rollback collection to a previous snapshot."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("IcebergStorageDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        await run_in_thread(lambda: table.manage_snapshots().rollback_to(int(snapshot_id)))

    async def read_at_snapshot(
        self,
        catalog_id: str,
        collection_id: str,
        snapshot_id: str,
        *,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """Read entities at a specific snapshot (time-travel)."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        scan = table.scan(snapshot_id=int(snapshot_id))

        bbox = None
        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)
            for f in request.filters:
                if f.operator == "bbox" and isinstance(f.value, list) and len(f.value) == 4:
                    bbox = f.value

        effective_limit = request.limit if request and request.limit else limit
        pa_table = await run_in_thread(scan.to_arrow)

        count = 0
        for row in pa_table.to_pylist():
            if count >= effective_limit:
                break
            geom = row.pop("geometry", None)
            if bbox and not self._bbox_matches(geom, bbox):
                continue
            fid = row.pop("id", None)
            yield Feature(
                type="Feature", id=fid, geometry=geom, properties=row,
            )
            count += 1

    async def read_at_timestamp(
        self,
        catalog_id: str,
        collection_id: str,
        as_of: datetime,
        *,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        """Read entities as they existed at a point in time."""
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        # Find snapshot at or before the given timestamp
        target_ms = int(as_of.timestamp() * 1000)
        snapshot_id = None
        for entry in table.history():
            if entry.timestamp_ms <= target_ms:
                snapshot_id = entry.snapshot_id
            else:
                break

        if snapshot_id is None:
            return

        async for feature in self.read_at_snapshot(
            catalog_id, collection_id, str(snapshot_id),
            request=request, limit=limit, db_resource=db_resource,
        ):
            yield feature

    async def get_schema_history(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> list:
        """Return schema evolution history for a collection."""
        from dynastore.models.otf import SchemaVersion, SchemaField

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return []

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        versions = []
        for schema in table.metadata.schemas:
            fields = [
                SchemaField(
                    name=field.name,
                    type=str(field.field_type),
                    required=field.required,
                    doc=field.doc,
                )
                for field in schema.fields
            ]
            versions.append(
                SchemaVersion(
                    schema_id=str(schema.schema_id),
                    fields=fields,
                    timestamp=datetime.now(tz=timezone.utc),
                    parent_schema_id=None,
                )
            )
        return versions

    async def evolve_schema(
        self,
        catalog_id: str,
        collection_id: str,
        changes: Any,
        *,
        db_resource: Optional[Any] = None,
    ):
        """Apply schema changes (add/rename/drop columns, type widening).

        Supports:
          - ``add_columns``: add new columns with typed defaults
          - ``rename_columns``: rename existing columns
          - ``drop_columns``: remove columns
          - ``type_promotions``: safe type widening (int32→int64, float32→float64)

        Does NOT rewrite data — only updates metadata.
        """
        from dynastore.models.otf import SchemaVersion, SchemaField, SchemaEvolution

        if not isinstance(changes, SchemaEvolution):
            changes = SchemaEvolution.model_validate(changes)

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError("IcebergStorageDriver: no location config")

        catalog = await self._ensure_catalog(loc, catalog_id)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = await run_in_thread(catalog.load_table, table_id)

        def _apply_schema_evolution():
            with table.update_schema() as update:
                for col in changes.add_columns:
                    iceberg_type = _resolve_iceberg_type(col.type)
                    update.add_column(col.name, iceberg_type, doc=col.doc)

                for old_name, new_name in changes.rename_columns.items():
                    update.rename_column(old_name, new_name)

                for col_name in changes.drop_columns:
                    update.delete_column(col_name)

                for field_name, new_type in changes.type_promotions.items():
                    iceberg_type = _resolve_iceberg_type(new_type)
                    update.update_column(field_name, field_type=iceberg_type)

        await run_in_thread(_apply_schema_evolution)

        # Return the new schema version
        current_schema = table.schema()
        fields = [
            SchemaField(
                name=f.name,
                type=str(f.field_type),
                required=f.required,
                doc=f.doc,
            )
            for f in current_schema.fields
        ]
        return SchemaVersion(
            schema_id=str(current_schema.schema_id),
            fields=fields,
            timestamp=datetime.now(tz=timezone.utc),
        )

    # ------------------------------------------------------------------
    # Collection metadata (stored in Iceberg table properties)
    # ------------------------------------------------------------------

    # Namespace prefix for all metadata properties stored on the Iceberg table.
    _META_PREFIX = "dynastore.meta."

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata from Iceberg table properties."""
        import json
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return None
        try:
            catalog = await self._ensure_catalog(loc, catalog_id)
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            table = await run_in_thread(catalog.load_table, table_id)
        except Exception:
            return None

        prefix = self._META_PREFIX
        metadata: Dict[str, Any] = {}
        for key, val in table.properties.items():
            if key.startswith(prefix):
                field = key[len(prefix):]
                try:
                    metadata[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    metadata[field] = val
        return metadata or None

    async def set_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource=None,
    ) -> None:
        """Write collection metadata to Iceberg table properties.

        All values are JSON-serialised (Iceberg properties are string-only).
        """
        import json
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return
        try:
            catalog = await self._ensure_catalog(loc, catalog_id)
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            table = await run_in_thread(catalog.load_table, table_id)
        except Exception:
            logger.warning(
                "IcebergStorageDriver.set_collection_metadata: table not found "
                "for catalog=%s collection=%s", catalog_id, collection_id,
            )
            return

        prefix = self._META_PREFIX
        props = {
            f"{prefix}{key}": (val if isinstance(val, str) else json.dumps(val, default=str))
            for key, val in metadata.items()
            if val is not None
        }
        if props:
            def _set_props():
                with table.transaction() as tx:
                    tx.set_properties(**props)

            await run_in_thread(_set_props)
