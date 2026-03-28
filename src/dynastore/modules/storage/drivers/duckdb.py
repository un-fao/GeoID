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
extension when ``FileStorageLocationConfig.write_path`` is set.

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
from dynastore.models.protocols.storage_driver import Capability, ReadOnlyDriverMixin
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.location import FileStorageLocationConfig

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
        Capability.READ_ONLY,
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.EXPORT,
    })

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
    ) -> Optional[FileStorageLocationConfig]:
        """Resolve FileStorageLocationConfig from StorageRoutingConfig."""
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
            loc = routing.get_location("duckdb")
            if isinstance(loc, FileStorageLocationConfig):
                return loc
            return None
        except Exception:
            return None

    @staticmethod
    def _is_writable(loc: FileStorageLocationConfig) -> bool:
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

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
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

        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        conn = self._get_conn()

        write_fmt = loc.write_format or "sqlite"
        if write_fmt == "sqlite":
            from dynastore.tools.db import validate_sql_identifier
            validate_sql_identifier(collection_id)

            self._ensure_sqlite_extension()
            conn.execute(f"ATTACH '{loc.write_path}' AS write_db (TYPE SQLITE)")
            try:
                table_name = f"write_db.{collection_id}"
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                    f"SELECT * FROM (VALUES {','.join(['(?)']*len(rows))}) LIMIT 0"
                )
                for row in rows:
                    cols = ", ".join(row.keys())
                    placeholders = ", ".join(["?"] * len(row))
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
                        list(row.values()),
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

        conn = self._get_conn()
        reader = self._reader_func(loc.format)
        base_sql = f"SELECT * FROM {reader}('{loc.path}')"

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
                        f"ST_Intersects(geometry, "
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
                geometry = row_dict.pop("geometry", None)
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
    ) -> None:
        loc = await self._get_location_async(catalog_id, collection_id)
        if not loc:
            logger.warning(
                "DuckDBStorageDriver.ensure_storage: no location config for "
                "catalog=%s collection=%s", catalog_id, collection_id,
            )
            return

        import os
        if loc.path and not loc.path.startswith(("s3://", "gs://", "http")):
            if not os.path.exists(loc.path):
                raise FileNotFoundError(
                    f"DuckDB source file not found: {loc.path}"
                )

        # Trigger singleton creation + extension loading
        self._get_conn()

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
    ) -> FileStorageLocationConfig:
        loc = await self._get_location_async(catalog_id, collection_id)
        if loc:
            return loc
        return FileStorageLocationConfig(driver="duckdb", format="parquet")
