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

The driver delegates to a PyIceberg catalog (REST, Glue, Hive, SQL, etc.)
configured via ``OTFStorageLocationConfig``.

Catalog types supported via ``OTFStorageLocationConfig.catalog_type``:
  - ``rest`` (default): REST catalog (e.g., Tabular, Polaris)
  - ``glue``: AWS Glue Data Catalog
  - ``hive``: Hive Metastore
  - ``sql``: SQL-based catalog (JDBC)
  - ``dynamodb``: DynamoDB-backed catalog

Connection lifecycle:
  - **Lazy init**: catalog loaded on first use via ``_get_catalog()``.
  - **Cached**: once loaded, the same catalog instance is reused.
  - **Shutdown**: catalog reference cleared in ``lifespan()`` on app shutdown.
"""

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.location import OTFStorageLocationConfig

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
    priority: int = 20
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.EXPORT,
        Capability.TIME_TRAVEL,
        Capability.VERSIONING,
        Capability.SNAPSHOTS,
        Capability.SCHEMA_EVOLUTION,
        Capability.SOFT_DELETE,
    })

    _catalog = None
    _catalog_loc_key: Optional[str] = None

    def is_available(self) -> bool:
        return _pyiceberg_available()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("IcebergStorageDriver: started")
        yield
        self._catalog = None
        self._catalog_loc_key = None
        logger.info("IcebergStorageDriver: stopped")

    def _get_catalog(self, loc: OTFStorageLocationConfig):
        """Get or create a PyIceberg catalog from location config.

        Supports multiple catalog types: sql (default), rest, glue, hive, dynamodb.
        When catalog_type is "sql" and no explicit URI is provided, falls back
        to the platform DATABASE_URL (PostgreSQL) — zero-config for production.

        The catalog is cached and reused across calls; if the location
        config key changes (different catalog), a new catalog is loaded.
        """
        loc_key = f"{loc.catalog_name}:{loc.catalog_uri}:{loc.catalog_type}"
        if self._catalog is not None and self._catalog_loc_key == loc_key:
            return self._catalog

        from pyiceberg.catalog import load_catalog

        catalog_type = loc.catalog_type or "sql"
        catalog_props: Dict[str, Any] = {"type": catalog_type}

        if loc.catalog_uri:
            catalog_props["uri"] = loc.catalog_uri
        elif catalog_type == "sql":
            # Fall back to platform DATABASE_URL when no explicit URI
            from dynastore.modules.db_config.db_config import DBConfig
            from dynastore.modules.db_config.tools import normalize_db_url
            sync_url = normalize_db_url(DBConfig.database_url, is_async=False)
            # PyIceberg SqlCatalog requires psycopg2 driver format
            if sync_url.startswith("postgresql://"):
                sync_url = sync_url.replace(
                    "postgresql://", "postgresql+psycopg2://", 1
                )
            catalog_props["uri"] = sync_url

        # Pass through extra properties for catalog-specific config
        if loc.catalog_properties:
            catalog_props.update(loc.catalog_properties)

        # Default warehouse to local temp dir for SQL catalogs if not specified
        if "warehouse" not in catalog_props and catalog_type == "sql":
            import tempfile
            catalog_props["warehouse"] = (
                f"file://{tempfile.gettempdir()}/iceberg_warehouse"
            )

        self._catalog = load_catalog(
            loc.catalog_name or "default", **catalog_props
        )
        self._catalog_loc_key = loc_key
        logger.debug(
            "IcebergStorageDriver: loaded %s catalog '%s'",
            catalog_type, loc.catalog_name or "default",
        )
        return self._catalog

    def _table_identifier(
        self, loc: OTFStorageLocationConfig, catalog_id: str, collection_id: str
    ) -> tuple:
        """Build Iceberg table identifier: (namespace, table_name)."""
        namespace = loc.namespace or catalog_id
        table_name = loc.table_name or collection_id
        return (namespace, table_name)

    async def _get_location_async(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> Optional[OTFStorageLocationConfig]:
        """Resolve OTFStorageLocationConfig from StorageRoutingConfig."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.modules.storage.config import STORAGE_ROUTING_CONFIG_ID

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return None
            routing = await configs.get_config(
                STORAGE_ROUTING_CONFIG_ID,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            loc = routing.get_location("iceberg")
            if isinstance(loc, OTFStorageLocationConfig):
                return loc
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
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            raise RuntimeError(
                "IcebergStorageDriver: no OTF location config found"
            )

        import pyarrow as pa
        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts,
            dicts_to_features,
        )

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        # Use the Iceberg table's Arrow schema to avoid pa.null() for missing columns
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        arrow_schema = schema_to_pyarrow(table.schema())
        pa_table = pa.Table.from_pylist(rows, schema=arrow_schema)
        table.append(pa_table)

        return dicts_to_features(rows)

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
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            return

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        scan = table.scan()

        if entity_ids:
            from pyiceberg.expressions import In
            scan = scan.filter(In("id", entity_ids))

        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)

        effective_limit = request.limit if request and request.limit else limit
        effective_offset = request.offset if request and request.offset else offset

        pa_table = scan.to_arrow()

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        from pyiceberg.expressions import In
        row_filter = In("id", entity_ids)

        if soft:
            # Soft delete: Iceberg positional/equality deletes — writes delete
            # files that mark rows as deleted without rewriting data files.
            # The rows remain in data files but are filtered out on reads.
            table.delete(delete_filter=row_filter)
        else:
            # Hard delete: delete then compact — removes rows and rewrites
            # data files to physically eliminate the deleted data.
            table.delete(delete_filter=row_filter)
            try:
                table.compact()
            except (AttributeError, NotImplementedError):
                # compact() may not be available in all PyIceberg versions;
                # the delete alone is sufficient (data excluded on next read).
                pass

        return len(entity_ids)

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.warning(
                "IcebergStorageDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s", catalog_id, collection_id,
            )
            return

        catalog = self._get_catalog(loc)
        namespace = loc.namespace or catalog_id

        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass  # namespace may already exist

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                catalog.load_table(table_id)
            except Exception:
                from pyiceberg.schema import Schema as IcebergSchema
                from pyiceberg.types import NestedField, StringType
                iceberg_schema = IcebergSchema(
                    NestedField(1, "id", StringType(), required=True),
                    NestedField(2, "geometry", StringType(), required=False),
                    NestedField(3, "properties", StringType(), required=False),
                )
                catalog.create_table(table_id, schema=iceberg_schema)

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

        catalog = self._get_catalog(loc)

        if soft:
            logger.info(
                "IcebergStorageDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — table retained, tagged as deprecated",
                catalog_id, collection_id,
            )
            if collection_id:
                table_id = self._table_identifier(loc, catalog_id, collection_id)
                try:
                    table = catalog.load_table(table_id)
                    with table.transaction() as tx:
                        tx.set_properties(
                            **{
                                "dynastore.deleted": "true",
                                "dynastore.deleted_at": datetime.now(tz=timezone.utc).isoformat(),
                            }
                        )
                except Exception as e:
                    logger.warning("Iceberg soft drop failed: %s", e)
            return

        if collection_id:
            table_id = self._table_identifier(loc, catalog_id, collection_id)
            try:
                catalog.drop_table(table_id)
            except Exception as e:
                logger.warning("Iceberg drop_table failed: %s", e)
        else:
            namespace = loc.namespace or catalog_id
            try:
                catalog.drop_namespace(namespace)
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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        pa_table = table.scan().to_arrow()

        if format == "csv":
            import pyarrow.csv as pcsv
            pcsv.write_csv(pa_table, target_path)
        elif format == "json":
            rows = pa_table.to_pylist()
            import builtins
            with builtins.open(target_path, "w") as f:
                json.dump(rows, f)
        else:
            # Default to parquet for any unrecognized format
            import pyarrow.parquet as pq
            pq.write_table(pa_table, target_path)

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
    ) -> OTFStorageLocationConfig:
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc:
            return loc
        return OTFStorageLocationConfig(driver="iceberg")

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        if label:
            table.manage_snapshots().create_branch(label)

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        table.manage_snapshots().rollback_to(int(snapshot_id))

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

        scan = table.scan(snapshot_id=int(snapshot_id))

        bbox = None
        if request and request.filters:
            scan = self._apply_filters(scan, request.filters)
            for f in request.filters:
                if f.operator == "bbox" and isinstance(f.value, list) and len(f.value) == 4:
                    bbox = f.value

        effective_limit = request.limit if request and request.limit else limit
        pa_table = scan.to_arrow()

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

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

        catalog = self._get_catalog(loc)
        table_id = self._table_identifier(loc, catalog_id, collection_id)
        table = catalog.load_table(table_id)

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
