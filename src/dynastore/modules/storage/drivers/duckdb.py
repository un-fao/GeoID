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
DuckDB Storage Driver — file-based analytical reads via DuckDB.

Reads from configurable formats (parquet, csv, json, etc.) using DuckDB's
built-in readers.  Optionally writes to SQLite via DuckDB's ``sqlite``
extension when ``DuckDbCollectionDriverConfig.write_path`` is set.

Capabilities vary based on configuration:
  - Read-only (default):  ``{READ_ONLY, STREAMING, SPATIAL_FILTER, EXPORT}``
  - Read-write (SQLite):  ``{STREAMING, SPATIAL_FILTER, EXPORT}``

Connection lifecycle:
  - **Singleton**: one in-process DuckDB connection shared across all
    driver instances, protected by a threading lock.
  - **Lazy init**: connection created on first use, not at import time.
  - **Extensions loaded once**: ``spatial`` and ``sqlite`` extensions are
    installed/loaded at connection creation and tracked to avoid redundancy.
  - **Shutdown**: connection closed in ``lifespan()`` on app shutdown.
"""

import logging
import threading
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Set, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import DuckDbCollectionDriverConfig

logger = logging.getLogger(__name__)

# Format → DuckDB reader function
_FORMAT_READERS: Dict[str, str] = {
    "parquet": "read_parquet",
    "csv": "read_csv_auto",
    "json": "read_json_auto",
    "ndjson": "read_json_auto",
}

# ---------------------------------------------------------------------------
# Module-level singleton connection
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_conn = None
_loaded_extensions: Set[str] = set()


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        return False


def _get_singleton_conn():
    """Return the process-wide DuckDB connection, creating it if needed.

    Thread-safe via module-level lock.  Extensions are loaded exactly once.
    """
    global _conn
    if _conn is not None:
        return _conn

    with _lock:
        # Double-checked locking
        if _conn is not None:
            return _conn

        import duckdb
        _conn = duckdb.connect(":memory:")
        logger.info("DuckDB: singleton connection created")

        _try_load_extension("spatial")

    return _conn


def _try_load_extension(name: str) -> bool:
    """Install and load a DuckDB extension once. Returns True on success."""
    global _conn
    if name in _loaded_extensions:
        return True
    if _conn is None:
        return False
    try:
        _conn.install_extension(name)
        _conn.load_extension(name)
        _loaded_extensions.add(name)
        logger.debug("DuckDB: extension '%s' loaded", name)
        return True
    except Exception:
        logger.debug("DuckDB: extension '%s' not available", name)
        return False


def _close_singleton():
    """Close the singleton connection and reset state."""
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
            _loaded_extensions.clear()
            logger.info("DuckDB: singleton connection closed")


class DuckDBStorageDriver(ModuleProtocol):
    """DuckDB storage driver — file-based analytical reads.

    Reads from parquet, CSV, JSON, etc. via DuckDB's built-in readers.
    Optionally writes to SQLite when ``write_path`` is configured.

    Uses a **process-wide singleton** DuckDB connection (in-memory,
    thread-safe).  Extensions are loaded once at connection creation.

    Satisfies ``CollectionStorageDriverProtocol`` and ``StorageLocationResolver``.
    """

    driver_id: str = "duckdb"
    priority: int = 30

    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.SORT,
        Capability.GROUP_BY,
        Capability.EXPORT,
        Capability.GEOSPATIAL,
        Capability.SOURCE_REFERENCE,
        Capability.ATTRIBUTE_FILTER,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
    })
    preferred_for: FrozenSet[str] = frozenset({"analytics"})
    supported_hints: FrozenSet[str] = frozenset({"analytics"})

    def is_available(self) -> bool:
        return _duckdb_available()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("DuckDBStorageDriver: started")
        yield
        _close_singleton()
        logger.info("DuckDBStorageDriver: stopped")

    @staticmethod
    def _get_conn():
        """Return the singleton DuckDB connection."""
        return _get_singleton_conn()

    async def _get_location_async(
        self, catalog_id: str, collection_id: Optional[str] = None
    ) -> Optional[DuckDbCollectionDriverConfig]:
        """Resolve DuckDbCollectionDriverConfig from the config waterfall."""
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.configs import ConfigsProtocol

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return None
            config = await configs.get_config(
                DuckDbCollectionDriverConfig._plugin_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if isinstance(config, DuckDbCollectionDriverConfig):
                return config
            return None
        except Exception:
            return None

    @staticmethod
    def _is_writable(loc: DuckDbCollectionDriverConfig) -> bool:
        """Check if this location has a write path configured."""
        return loc.write_path is not None

    @staticmethod
    def _reader_func(fmt: str) -> str:
        """Map format string to DuckDB reader function."""
        return _FORMAT_READERS.get(fmt, "read_parquet")

    @staticmethod
    def _ensure_sqlite_extension():
        """Load the sqlite extension if not already loaded."""
        _try_load_extension("sqlite")

    @staticmethod
    def _extract_external_id(row: Dict[str, Any], field: str) -> Optional[str]:
        """Extract external_id from a row dict using dot-notation field path."""
        parts = field.split(".")
        val = row
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return None
        return str(val) if val is not None else None

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not self._is_writable(loc):
            raise ReadOnlyDriverError(
                "DuckDBStorageDriver: no write_path configured — driver is read-only"
            )

        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts,
            dicts_to_features,
        )
        from dynastore.modules.storage.driver_config import (
            CollectionWritePolicy, WriteConflictPolicy, WRITE_POLICY_PLUGIN_ID,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # Resolve write policy
        ctx = context or {}
        policy: CollectionWritePolicy = CollectionWritePolicy()
        try:
            _configs = get_protocol(ConfigsProtocol)
            if _configs:
                _p = await _configs.get_config(
                    WRITE_POLICY_PLUGIN_ID, catalog_id=catalog_id, collection_id=collection_id
                )
                if _p is not None:
                    policy = _p
        except Exception:
            pass

        # Enrich rows with context metadata (asset_id, validity)
        asset_id = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")
        for row in rows:
            if policy.track_asset_id and asset_id:
                row.setdefault("asset_id", asset_id)
            if policy.enable_validity:
                if valid_from:
                    row.setdefault("valid_from", str(valid_from))
                if valid_to:
                    row.setdefault("valid_to", str(valid_to))

        conn = self._get_conn()

        write_fmt = loc.write_format or "sqlite"
        if write_fmt == "sqlite":
            from dynastore.tools.db import validate_sql_identifier
            validate_sql_identifier(collection_id)

            self._ensure_sqlite_extension()
            conn.execute(f"ATTACH '{loc.write_path}' AS write_db (TYPE SQLITE)")
            try:
                table_name = f"write_db.{collection_id}"
                # Ensure table exists (idempotent after ensure_storage)
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} "
                    f"(id VARCHAR PRIMARY KEY, geometry VARCHAR, properties VARCHAR, "
                    f"external_id VARCHAR, asset_id VARCHAR, "
                    f"valid_from VARCHAR, valid_to VARCHAR)"
                )
                for row in rows:
                    ext_id = (
                        ctx.get("external_id_override")
                        or self._extract_external_id(row, policy.external_id_field)
                    )
                    if policy.require_external_id and not ext_id:
                        raise ValueError(
                            f"DuckDB write_entities: external_id required but missing "
                            f"(field='{policy.external_id_field}')"
                        )

                    on_conflict = policy.on_conflict
                    if on_conflict == WriteConflictPolicy.IGNORE and ext_id:
                        existing = conn.execute(
                            f"SELECT id FROM {table_name} WHERE external_id = ?", [ext_id]
                        ).fetchone()
                        if existing:
                            continue
                    elif on_conflict == WriteConflictPolicy.REFUSE and ext_id:
                        existing = conn.execute(
                            f"SELECT id FROM {table_name} WHERE external_id = ?", [ext_id]
                        ).fetchone()
                        if existing:
                            from dynastore.modules.storage.errors import ConflictError
                            raise ConflictError(
                                f"DuckDB: external_id '{ext_id}' already exists in "
                                f"{catalog_id}/{collection_id} (policy=refuse)"
                            )

                    row_with_ext = dict(row)
                    if ext_id:
                        row_with_ext["external_id"] = ext_id

                    cols = ", ".join(f'"{k}"' for k in row_with_ext.keys())
                    placeholders = ", ".join(["?"] * len(row_with_ext))
                    if on_conflict == WriteConflictPolicy.NEW_VERSION:
                        # Always insert a new row (no conflict clause)
                        conn.execute(
                            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                            list(row_with_ext.values()),
                        )
                    else:
                        conn.execute(
                            f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
                            list(row_with_ext.values()),
                        )
            finally:
                conn.execute("DETACH write_db")
        else:
            conn.execute(
                f"COPY (SELECT * FROM rows) TO '{loc.write_path}' (FORMAT {write_fmt})"
            )

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
        if not loc or not loc.path:
            return

        import json as _json

        conn = self._get_conn()
        reader = self._reader_func(loc.format)
        source = f"{reader}('{loc.path}')"

        # Detect geometry column type: GeoParquet stores geometry as WKB bytes.
        # When the spatial extension is loaded, convert to GeoJSON at query time
        # so the Feature model receives a dict instead of raw bytes.
        geo_col: Optional[str] = None
        if "spatial" in _loaded_extensions:
            try:
                schema = conn.execute(
                    f"DESCRIBE SELECT * FROM {source} LIMIT 0"
                ).fetchall()
                for col_name, col_type, *_ in schema:
                    if "GEOMETRY" in str(col_type).upper():
                        geo_col = col_name
                        break
            except Exception:
                pass

        if geo_col:
            # Build SELECT with explicit ST_AsGeoJSON conversion for the geometry column.
            schema_cols = [row[0] for row in conn.execute(
                f"DESCRIBE SELECT * FROM {source} LIMIT 0"
            ).fetchall()]
            col_exprs = [
                f"ST_AsGeoJSON({geo_col})::VARCHAR AS {geo_col}"
                if c == geo_col else f'"{c}"'
                for c in schema_cols
            ]
            base_sql = f"SELECT {', '.join(col_exprs)} FROM {source}"
        else:
            base_sql = f"SELECT * FROM {source}"

        where_clauses: List[str] = []
        params: List[Any] = []
        if entity_ids:
            placeholders = ", ".join(["?"] * len(entity_ids))
            where_clauses.append(f"id IN ({placeholders})")
            params.extend(entity_ids)

        if request and request.filters:
            for f in request.filters:
                if f.operator == "eq":
                    from dynastore.tools.db import validate_sql_identifier
                    validate_sql_identifier(f.field)
                    where_clauses.append(f"{f.field} = ?")
                    params.append(f.value)
                elif f.operator == "bbox" and isinstance(f.value, list) and len(f.value) == 4:
                    minx, miny, maxx, maxy = f.value
                    where_clauses.append(
                        f"ST_Intersects({geo_col or 'geometry'}, "
                        f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
                    )

        effective_limit = limit
        effective_offset = offset
        if request:
            if request.limit is not None:
                effective_limit = request.limit
            if request.offset is not None:
                effective_offset = request.offset

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql = f"{base_sql}{where_sql} LIMIT ? OFFSET ?"
        params.extend([effective_limit, effective_offset])

        try:
            result = conn.execute(sql, params)
            columns = [desc[0] for desc in result.description]
            for row in result.fetchall():
                row_dict = dict(zip(columns, row))
                feature_id = row_dict.pop("id", None)
                geometry = row_dict.pop(geo_col or "geometry", None)
                if isinstance(geometry, str):
                    try:
                        geometry = _json.loads(geometry)
                    except Exception:
                        geometry = None
                elif isinstance(geometry, (bytes, bytearray)):
                    geometry = None
                yield Feature(
                    type="Feature",
                    id=feature_id,
                    geometry=geometry,
                    properties=row_dict,
                )
        except Exception as e:
            logger.error("DuckDB read_entities failed: %s", e)

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
        if not loc or not self._is_writable(loc):
            raise ReadOnlyDriverError(
                "DuckDBStorageDriver: no write_path — cannot delete"
            )
        if soft:
            raise SoftDeleteNotSupportedError(
                "DuckDBStorageDriver does not support soft delete."
            )

        conn = self._get_conn()
        write_fmt = loc.write_format or "sqlite"
        if write_fmt != "sqlite":
            raise ReadOnlyDriverError(
                "DuckDBStorageDriver: delete only supported with SQLite write backend"
            )

        from dynastore.tools.db import validate_sql_identifier
        validate_sql_identifier(collection_id)

        self._ensure_sqlite_extension()
        conn.execute(f"ATTACH '{loc.write_path}' AS write_db (TYPE SQLITE)")
        try:
            placeholders = ", ".join(["?"] * len(entity_ids))
            result = conn.execute(
                f"DELETE FROM write_db.{collection_id} WHERE id IN ({placeholders})",
                list(entity_ids),
            )
            return result.fetchone()[0] if result.description else len(entity_ids)
        finally:
            conn.execute("DETACH write_db")

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Ensure backing storage exists, creating it if necessary.

        SQLite write backend (``write_path``): creates the parent directory and
        initialises the collection table with the canonical schema.

        Read-only parquet/csv (``path``): remote paths are skipped; local paths
        that don't exist yet are logged as info — they will be populated by an
        external ETL process or the first ``write_entities()`` call.
        """
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.info(
                "DuckDBStorageDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s — nothing to provision",
                catalog_id, collection_id,
            )
            return

        import os

        # Trigger singleton creation + extension loading
        conn = self._get_conn()

        # --- SQLite write backend: create dir + table ---
        if loc.write_path:
            write_dir = os.path.dirname(loc.write_path)
            if write_dir:
                os.makedirs(write_dir, exist_ok=True)

            write_fmt = loc.write_format or "sqlite"
            if write_fmt == "sqlite" and collection_id:
                from dynastore.tools.db import validate_sql_identifier
                validate_sql_identifier(collection_id)

                self._ensure_sqlite_extension()
                conn.execute(f"ATTACH '{loc.write_path}' AS write_db (TYPE SQLITE)")
                try:
                    conn.execute(
                        f"CREATE TABLE IF NOT EXISTS write_db.{collection_id} "
                        f"(id VARCHAR PRIMARY KEY, geometry VARCHAR, properties VARCHAR, "
                        f"external_id VARCHAR, asset_id VARCHAR, "
                        f"valid_from VARCHAR, valid_to VARCHAR)"
                    )
                    logger.info(
                        "DuckDB: initialised SQLite table '%s' in %s",
                        collection_id, loc.write_path,
                    )
                finally:
                    conn.execute("DETACH write_db")

        # --- Read-only source: warn if local path missing ---
        if loc.path and not loc.path.startswith(("s3://", "gs://", "http")):
            if not os.path.exists(loc.path):
                logger.info(
                    "DuckDB: source path %s does not exist yet — "
                    "it will be populated by an external ETL process or first write",
                    loc.path,
                )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "DuckDBStorageDriver does not support soft drop."
            )
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc and loc.write_path:
            import os
            if os.path.exists(loc.write_path):
                os.remove(loc.write_path)
                logger.info("DuckDB: removed write file %s", loc.write_path)

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
        if not loc or not loc.path:
            raise ValueError("DuckDB: no source path configured for export")

        conn = self._get_conn()
        reader = self._reader_func(loc.format)
        sql = f"COPY (SELECT * FROM {reader}('{loc.path}')) TO '{target_path}' (FORMAT {format})"
        conn.execute(sql)
        return target_path

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> DuckDbCollectionDriverConfig:
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc:
            return loc
        return DuckDbCollectionDriverConfig()

    # ------------------------------------------------------------------
    # Collection metadata (sidecar JSON file alongside the parquet)
    # ------------------------------------------------------------------

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata from the sidecar JSON file."""
        import json
        import os

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not loc.path:
            return None

        sidecar = os.path.join(
            os.path.dirname(loc.path),
            f".dynastore_meta_{collection_id}.json",
        )
        if not os.path.exists(sidecar):
            return None
        try:
            with open(sidecar, "r") as f:
                return json.load(f)
        except Exception:
            return None

    async def set_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource=None,
    ) -> None:
        """Write collection metadata to the sidecar JSON file.

        Sidecar location: ``{dirname(loc.path)}/.dynastore_meta_{collection_id}.json``
        """
        import json
        import os

        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc or not loc.path:
            return

        base_dir = os.path.dirname(loc.path)
        os.makedirs(base_dir, exist_ok=True)
        sidecar = os.path.join(base_dir, f".dynastore_meta_{collection_id}.json")
        with open(sidecar, "w") as f:
            json.dump(metadata, f, default=str)
