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

import logging
import asyncio
import os
from contextlib import asynccontextmanager
import json
from dynastore.tools.json import CustomJSONEncoder
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction, DbResource
from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_LOGS_TABLE, SYSTEM_SCHEMA
from dynastore.modules.db_config.tools import get_any_engine
from dynastore.modules.catalog.tenant_schema import register_tenant_initializer
from dynastore.modules.db_config.maintenance_tools import ensure_future_partitions, register_retention_policy
from dynastore.modules.db_config.locking_tools import acquire_lock_if_needed, check_table_exists
from dynastore.models.protocols import LogsProtocol, CatalogsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# ==============================================================================
#  TENANT INITIALIZATION (Logs Slice)
# ==============================================================================


TENANT_LOGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.logs (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    catalog_id VARCHAR NOT NULL,
    collection_id VARCHAR,
    event_type VARCHAR,
    level VARCHAR(20),
    message TEXT,
    details JSONB,
    stacktrace TEXT,
    request_context JSONB,
    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);
"""

SYSTEM_LOGS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.{SYSTEM_LOGS_TABLE} (
    id BIGSERIAL,
    catalog_id VARCHAR,
    collection_id VARCHAR,
    event_type VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    message TEXT,
    details JSONB,
    stacktrace TEXT,
    request_context JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);
"""

@register_tenant_initializer
async def _initialize_logs_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the logs module's slice of the tenant schema."""
    from dynastore.modules.db_config.locking_tools import execute_safe_ddl
    try:
        # Logs table creation
        async def table_exists_check():
            return await check_table_exists(conn, "logs", schema)

        await execute_safe_ddl(
            conn=conn,
            ddl_statement=TENANT_LOGS_DDL,
            lock_key=f"{schema}_logs",
            existence_check=table_exists_check,
            schema=schema
        )
        
        await ensure_future_partitions(conn, schema=schema, table="logs", interval="monthly", periods_ahead=12, column="timestamp")
        
        # Retention policy
        await register_retention_policy(conn, schema=schema, table="logs", policy="prune", interval="daily", retention_period="1 year", column="timestamp")
    except Exception:
        import traceback
        traceback.print_exc()
        raise

async def initialize_system_logs(conn: DbResource):
    """Initializes the system-level logs table."""
    from dynastore.modules.db_config.locking_tools import execute_safe_ddl
    
    async def table_exists_check():
        return await check_table_exists(conn, "system_logs", "catalog")

    await execute_safe_ddl(
        conn=conn,
        ddl_statement=SYSTEM_LOGS_DDL,
        lock_key="system_logs",
        existence_check=table_exists_check
    )
    
    await ensure_future_partitions(conn, schema="catalog", table="system_logs", interval="monthly", periods_ahead=12, column="timestamp")
    await register_retention_policy(conn, schema="catalog", table="system_logs", policy="prune", interval="daily", retention_period="1 year", column="timestamp")

# --- Log Entry Model (matches extensions/logs/models.py) ---
# We import from extensions/logs to maintain compatibility, but the actual
# implementation and buffering lives here in the catalog module.
try:
    from dynastore.extensions.logs.models import LogEntryCreate
except ImportError:
    # Fallback if extensions/logs is not available
    class LogEntryCreate:
        """Pydantic-like model for creating log entries."""
        def __init__(
            self,
            catalog_id: str,
            event_type: str,
            level: str = "INFO",
            message: Optional[str] = None,
            collection_id: Optional[str] = None,
            details: Optional[Dict[str, Any]] = None,
            is_system: bool = False
        ):
            self.catalog_id = catalog_id
            self.collection_id = collection_id
            self.event_type = event_type
            self.level = level
            self.message = message
            self.details = details or {}
            self.is_system = is_system

# --- Log Buffer (similar to StatsBuffer) ---

class LogBuffer:
    """Thread-safe buffer for accumulating log entries before flushing."""
    def __init__(self, flush_threshold: int = 50, flush_interval: float = 5.0):
        self._buffer: List[LogEntryCreate] = []
        self._lock = asyncio.Lock()
        self._flush_threshold = flush_threshold
        self._flush_interval = flush_interval
        self._last_flush = datetime.now(timezone.utc)

    async def add(self, entry: LogEntryCreate) -> bool:
        """Adds a log entry to the buffer. Returns True if buffer should be flushed."""
        async with self._lock:
            self._buffer.append(entry)
            return len(self._buffer) >= self._flush_threshold

    async def drain(self) -> List[LogEntryCreate]:
        """Drains the buffer and returns all entries. Thread-safe."""
        async with self._lock:
            if not self._buffer:
                return []
            entries = self._buffer[:]
            self._buffer.clear()
            self._last_flush = datetime.now(timezone.utc)
            return entries

    async def should_flush_by_time(self) -> bool:
        """Checks if enough time has passed since last flush."""
        async with self._lock:
            elapsed = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
            return elapsed >= self._flush_interval

    def size(self) -> int:
        """Returns current buffer size (non-blocking)."""
        return len(self._buffer)

# --- Log Service (similar to StatsService) ---

class LogService(LogsProtocol):
    """Singleton service for buffered, high-throughput log ingestion."""
    
    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule
    
    _instance = None
    _engine: Optional[DbResource] = None
    _buffer: Optional[LogBuffer] = None
    _flush_task: Optional[asyncio.Task] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogService, cls).__new__(cls)
        return cls._instance
    
    def is_available(self) -> bool:
        """Returns True if the service is initialized and ready."""
        return self._engine is not None and self._buffer is not None

    @asynccontextmanager
    async def lifespan(self, app_state: Any, db_resource: Optional[DbResource] = None):
        """Initializes the log service with database connection and handles cleanup."""
        self._engine = db_resource or get_any_engine(app_state)
        if not self._engine:
            logger.warning("LogService: No database engine available. Logging will be disabled.")
            yield
            return

        flush_threshold = int(os.environ.get("LOG_FLUSH_THRESHOLD", 50))
        flush_interval = float(os.environ.get("LOG_FLUSH_INTERVAL", 5.0))
        self._buffer = LogBuffer(flush_threshold=flush_threshold, flush_interval=flush_interval)

        # Initialize system logs table
        if self._engine:
            async with managed_transaction(self._engine) as conn:
                await initialize_system_logs(conn)

        # Start periodic flush task
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush(flush_interval))
            logger.info(f"LogService initialized with flush_threshold={flush_threshold}, flush_interval={flush_interval}s")
            
        try:
            yield
        finally:
            # Cleanup
            logger.info("LogService shutting down...")
            if self._flush_task:
                self._flush_task.cancel()
                try:
                    await self._flush_task
                except asyncio.CancelledError:
                    pass
                self._flush_task = None
            
            # Final flush
            await self.flush()
            logger.info("LogService shutdown complete.")

    async def _periodic_flush(self, interval_seconds: float):
        """Background task that periodically flushes the buffer."""
        logger.info(f"Starting periodic log flush task every {interval_seconds} seconds.")
        while True:
            try:
                await asyncio.sleep(interval_seconds)
                if self._buffer and await self._buffer.should_flush_by_time():
                    await self.flush()
            except asyncio.CancelledError:
                logger.debug("Periodic log flush task cancelled.")
                break
            except Exception as e:
                logger.exception("Error during periodic log flush.")

    async def flush(self):
        """Flushes all buffered log entries to the database."""
        if not self._buffer or not self._engine:
            return

        entries = await self._buffer.drain()
        if not entries:
            return

        try:
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.query_executor import DbAsyncConnection
                for entry in entries:
                    try:
                        # Fallback: Use a savepoint if in a transaction to avoid poisoning 
                        # the main transaction if the logs table doesn't exist yet.
                        # This is common during catalog creation when async init or listeners try to log.
                        if isinstance(conn, DbAsyncConnection) and hasattr(conn, "begin_nested"):
                            async with conn.begin_nested():
                                await self._write_log_entry(conn, entry)
                        else:
                            # If not a DbAsyncConnection or doesn't support nested transactions,
                            # just try to write directly. This might fail the whole transaction
                            # if the table doesn't exist, but it's a fallback.
                            await self._write_log_entry(conn, entry)
                    except Exception as e:
                        # Log but don't re-raise to avoid crashing the main operation
                        logger.warning(f"LogService: Failed to write individual log entry: {e}")
                        # If a single entry fails, we continue with others in the batch.
                        # The SAVEPOINT (nested_trans) has been rolled back.
                        pass
            logger.debug(f"Flushed {len(entries)} log entries to database.")
        except Exception as e:
            logger.error(f"Failed to flush log entries: {e}", exc_info=True)
            # Re-buffer entries on failure (simple retry strategy)
            # In production, you might want a dead-letter queue
            for entry in entries:
                await self._buffer.add(entry)

    async def _write_log_entry(self, conn: DbResource, entry: LogEntryCreate) -> Optional[int]:
        """Writes a single log entry, ensuring partition exists. Returns log ID."""
        # Determine target schema and table
        catalogs = get_protocol(CatalogsProtocol)
        if entry.is_system or entry.catalog_id == SYSTEM_CATALOG_ID or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            phys_schema = await catalogs.resolve_physical_schema(entry.catalog_id, db_resource=conn)
            table_name = "logs"

        if not phys_schema:
            logger.warning(f"LogService: Physical schema not found for catalog '{entry.catalog_id}'. Falling back to system_logs.")
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE

        # Prepare details with stacktrace and request_context if provided
        details_dict = entry.details or {}
        stacktrace = details_dict.pop('stacktrace', None) if isinstance(details_dict, dict) else None
        request_context = details_dict.pop('request_context', None) if isinstance(details_dict, dict) else None

        catalog_id_val = entry.catalog_id

        # Insert log entry and return ID
        log_id = await DQLQuery(
            """
            INSERT INTO {schema}.{table} (timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context)
            VALUES (:timestamp, :catalog_id, :collection_id, :event_type, :level, :message, :details, :stacktrace, :request_context)
            RETURNING id;
            """,
            result_handler=ResultHandler.SCALAR_ONE
        ).execute(
            conn,
            schema=phys_schema,
            table=table_name,
            timestamp=datetime.now(timezone.utc),
            catalog_id=catalog_id_val,
            collection_id=entry.collection_id,
            event_type=entry.event_type,
            level=entry.level,
            message=entry.message,
            details=json.dumps(details_dict, cls=CustomJSONEncoder) if details_dict else None,
            stacktrace=stacktrace,
            request_context=json.dumps(request_context, cls=CustomJSONEncoder) if request_context else None
        )
        return log_id

    async def log_event(
        self,
        catalog_id: str,
        event_type: str,
        level: str = "INFO",
        message: Optional[str] = None,
        collection_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        db_resource: Optional[DbResource] = None,
        immediate: bool = False,
        is_system: bool = False
    ) -> Optional[int]:
        """
        Main entry point for logging events.
        
        Args:
            catalog_id: The catalog this event relates to (required).
            event_type: Type of event (e.g., "gcp_bucket_created", "catalog_creation").
            level: Log level (INFO, WARNING, ERROR).
            message: Human-readable message.
            collection_id: Optional collection ID if event is collection-scoped.
            details: Optional structured details dictionary. Can include 'stacktrace' and 'request_context'.
            db_resource: Optional database connection. If provided, writes immediately (bypasses buffer).
            immediate: If True and not under load, flush immediately. Otherwise, buffer for batch write.
            
        Returns:
            Log ID if db_resource is provided and write succeeds, None otherwise.
        """
        if not self._engine and not db_resource:
            # Fallback to standard logging if no DB available
            safe_msg = f"[LogService] {level} | {catalog_id} | {event_type}: {message}"
            if level.upper() == "ERROR":
                logger.error(safe_msg)
            elif level.upper() == "WARNING":
                logger.warning(safe_msg)
            else:
                logger.info(safe_msg)
            return None

        entry = LogEntryCreate(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            message=message,
            details=details,
            is_system=is_system
        )

        # If db_resource is provided, write immediately (transactional guarantee) and return ID
        if db_resource:
            return await self._write_log_entry(db_resource, entry)

        # Otherwise, buffer for batch write (no ID returned)
        if not self._buffer:
            logger.warning("LogService buffer not initialized. Logging to stdout only.")
            logger.info(f"{level} | {catalog_id} | {event_type}: {message}")
            return None

        needs_flush = await self._buffer.add(entry)

        # Flush if buffer is full
        if needs_flush:
            await self.flush()
        # Flush immediately if requested and buffer is small (not under load)
        elif immediate and self._buffer.size() < 10:
            # Small buffer + immediate flag = low load, safe to flush now
            await self.flush()
        
        return None  # Buffered writes don't return IDs

    async def shutdown(self):
        """Flushes all remaining logs and stops the periodic flush task."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
        logger.info("LogService shut down and flushed.")
    
    async def get_log_by_id(
        self,
        log_id: int,
        catalog_id: str,
        db_resource: Optional[DbResource] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific log entry by ID.
        
        Args:
            log_id: The log entry ID
            catalog_id: The catalog code (or "_system_")
            db_resource: Optional database connection
            
        Returns:
            Log entry as dict, or None if not found
        """
        # Determine schema and table
        catalogs = get_protocol(CatalogsProtocol)
        if catalog_id == SYSTEM_CATALOG_ID or catalog_id == "_system_" or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            if db_resource:
                phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)
            else:
                async with managed_transaction(self._engine) as conn:
                    phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn) # type: ignore
            table_name = "logs"
        
        if not phys_schema:
            return None
        
        # Query log entry
        async def _query(conn):
            return await DQLQuery(
                """
                SELECT id, timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context
                FROM {schema}.{table}
                WHERE id = :log_id
                LIMIT 1;
                """,
                result_handler=ResultHandler.ONE_DICT
            ).execute(conn, schema=phys_schema, table=table_name, log_id=log_id)
        
        if db_resource:
            return await _query(db_resource)
        else:
            async with managed_transaction(self._engine) as conn:
                return await _query(conn)
    
    async def list_logs(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        level: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        db_resource: Optional[DbResource] = None
    ) -> List[Dict[str, Any]]:
        """
        List log entries with filtering and pagination.
        
        Args:
            catalog_id: The catalog code (or "_system_")
            collection_id: Optional collection filter
            level: Optional level filter (ERROR, WARNING, INFO)
            event_type: Optional event type filter
            limit: Maximum number of results (default 50, max 1000)
            offset: Pagination offset
            db_resource: Optional database connection
            
        Returns:
            List of log entries as dicts
        """
        # Determine schema and table
        catalogs = get_protocol(CatalogsProtocol)
        if catalog_id == "_system_" or not catalogs:
            phys_schema = "catalog"
            table_name = SYSTEM_LOGS_TABLE
        else:
            if db_resource:
                phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)
            else:
                async with managed_transaction(self._engine) as conn:
                    phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn) # type: ignore
            table_name = "logs"
        
        if not phys_schema:
            return []
        
        # Build WHERE clause
        where_clauses = ["catalog_id = :catalog_id"]
        params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}
        
        if collection_id:
            where_clauses.append("collection_id = :collection_id")
            params["collection_id"] = collection_id
        
        if level:
            where_clauses.append("level = :level")
            params["level"] = level.upper()
        
        if event_type:
            where_clauses.append("event_type = :event_type")
            params["event_type"] = event_type
        
        where_clause = " AND ".join(where_clauses)
        
        # Query logs
        async def _query(conn):
            return await DQLQuery(
                f"""
                SELECT id, timestamp, catalog_id, collection_id, event_type, level, message, details, stacktrace, request_context
                FROM {{schema}}.{{table}}
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT :limit OFFSET :offset;
                """,
                result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, schema=phys_schema, table=table_name, **params)
        
        if db_resource:
            return await _query(db_resource)
        else:
            async with managed_transaction(self._engine) as conn:
                return await _query(conn)

# Singleton instance
LOG_SERVICE = LogService()

# --- Convenience Functions (matches extensions/logs/log_manager.py API) ---

async def log_event(
    catalog_id: str,
    event_type: str,
    level: str = "INFO",
    message: Optional[str] = None,
    collection_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    db_resource: Optional[DbResource] = None,
    immediate: bool = False,
    is_system: bool = False
) -> Optional[int]:
    """
    Main entry point for logging events to the Catalog Log Service.
    Fails safely (logs to stdout) if service is not initialized.
    
    Returns:
        Log ID if db_resource is provided and write succeeds, None otherwise.
    """
    return await LOG_SERVICE.log_event(
        catalog_id=catalog_id,
        event_type=event_type,
        level=level,
        message=message,
        collection_id=collection_id,
        details=details,
        db_resource=db_resource,
        immediate=immediate,
        is_system=is_system
    )

async def log_info(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for INFO level logs."""
    is_system = kwargs.pop('is_system', False)
    await log_event(catalog_id, event_type, level="INFO", message=message, is_system=is_system, **kwargs)

async def log_warning(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for WARNING level logs."""
    is_system = kwargs.pop('is_system', False)
    await log_event(catalog_id, event_type, level="WARNING", message=message, is_system=is_system, **kwargs)

async def log_error(catalog_id: str, event_type: str, message: str, **kwargs):
    """Convenience wrapper for ERROR level logs."""
    is_system = kwargs.pop('is_system', False)
    await log_event(catalog_id, event_type, level="ERROR", message=message, is_system=is_system, **kwargs)
