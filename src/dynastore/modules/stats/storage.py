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

import abc
import asyncio
import logging
import json
import hashlib
import ipaddress
import random
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
from ipaddress import IPv4Address, IPv6Address
from dynastore.tools.plugin import ProtocolPlugin
from dynastore.modules.db_config.query_executor import (
    DbConnection,
    DbResource,
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from pydantic import BaseModel, IPvAnyAddress, field_validator, ConfigDict
from dynastore.modules.db_config.partition_tools import ensure_partition_exists

from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.locking_tools import acquire_startup_lock

USAGE_SHARD_COUNT = 16

logger = logging.getLogger(__name__)

# --- Models ---


class AccessRecord(BaseModel):
    id: Optional[int] = None
    timestamp: datetime
    catalog_id: Optional[str] = None
    api_key_hash: Optional[str] = None
    principal_id: Optional[str] = None
    # Fix: Accept IPv4/IPv6 objects directly from the DB driver
    source_ip: Union[str, IPv4Address, IPv6Address]
    method: str
    path: str
    status_code: int
    processing_time_ms: float
    details: Dict[str, Any] = {}

    @field_validator("source_ip", mode="before")
    @classmethod
    def normalize_ip(cls, v):
        # Optional: ensure we always work with strings internally if preferred,
        # or keep them as objects. Pydantic handles serialization fine either way.
        if isinstance(v, (IPv4Address, IPv6Address)):
            return str(v)
        return v


class StatsSummary(BaseModel):
    """Aggregated statistics over a period."""

    total_requests: int
    average_latency_ms: float
    status_code_distribution: Dict[str, int]
    unique_principals: int


class AccessLogPage(BaseModel):
    """A paginated page of access logs."""

    logs: List[AccessRecord]
    summary: Optional[StatsSummary] = None
    next_cursor: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# --- Queries ---


CREATE_ACCESS_LOGS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.access_logs (
        id BIGSERIAL,
        timestamp TIMESTAMPTZ NOT NULL,
        catalog_id VARCHAR(100),
        api_key_hash VARCHAR(64),
        principal_id VARCHAR(255),
        source_ip INET,
        method VARCHAR(10),
        path TEXT,
        status_code INT,
        processing_time_ms FLOAT,
        details JSONB,
        PRIMARY KEY (timestamp, id)
    ) PARTITION BY RANGE (timestamp);
""")

CREATE_AGGREGATES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.stats_aggregates (
        period_start TIMESTAMPTZ NOT NULL, -- Rounded to the month
        catalog_id VARCHAR(100) NOT NULL DEFAULT 'none',
        principal_id VARCHAR(255) NOT NULL DEFAULT 'none',
        api_key_hash VARCHAR(64) NOT NULL DEFAULT 'none',
        status_code INT NOT NULL,
        shard_id INT NOT NULL DEFAULT 0,
        request_count BIGINT DEFAULT 0,
        total_latency_ms FLOAT DEFAULT 0,
        PRIMARY KEY (period_start, catalog_id, principal_id, api_key_hash, status_code, shard_id)
    ) PARTITION BY RANGE (period_start);
""")

INSERT_ACCESS_LOGS_BATCH = """
    INSERT INTO {schema}.access_logs 
    (timestamp, catalog_id, api_key_hash, principal_id, source_ip, method, path, status_code, processing_time_ms, details)
    VALUES 
"""

UPSERT_AGGREGATE = DDLQuery("""
    INSERT INTO {schema}.stats_aggregates 
    (period_start, catalog_id, principal_id, api_key_hash, status_code, shard_id, request_count, total_latency_ms)
    VALUES 
    (:period, :cat_id, :pid, :key_hash, :status, :shard, :count, :latency)
    ON CONFLICT (period_start, catalog_id, principal_id, api_key_hash, status_code, shard_id)
    DO UPDATE SET 
        request_count = {schema}.stats_aggregates.request_count + EXCLUDED.request_count,
        total_latency_ms = {schema}.stats_aggregates.total_latency_ms + EXCLUDED.total_latency_ms;
""")

# --- Buffering Logic ---

# Removed StatsBuffer in favor of AsyncBufferAggregator in async_utils

# --- Storage Driver ---


class AbstractStatsDriver(ProtocolPlugin[object]):
    """
    Abstract base class (and ProtocolPlugin category) for all stats drivers.

    Priority is evaluated **only** among AbstractStatsDriver subclasses via
    ``get_protocols(AbstractStatsDriver)``.  A higher ``priority`` means the
    driver is preferred when ``STATS_DRIVER`` env var is not explicitly set.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unique name used for env-var selection (STATS_DRIVER=<name>)."""
        ...

    @abc.abstractmethod
    async def buffer_record(self, record: AccessRecord): ...

    @abc.abstractmethod
    async def flush(self, conn: Optional[DbResource] = None): ...

    @abc.abstractmethod
    async def get_summary(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs: Any,
    ) -> StatsSummary: ...

    @abc.abstractmethod
    async def get_logs(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> AccessLogPage: ...


class PostgresStatsDriver(AbstractStatsDriver):
    priority: int = 10
    _engine: DbResource

    def __init__(self, engine: DbResource):
        from dynastore.tools.async_utils import AsyncBufferAggregator
        import os

        self._engine = engine
        self.buffer = AsyncBufferAggregator(
            flush_callback=self._flush_records,
            threshold=int(os.environ.get("STATS_FLUSH_THRESHOLD", 1000)),
            interval=float(os.environ.get("STATS_FLUSH_INTERVAL", 5.0)),
            name="stats",
        )
        # Simple cache to avoid hitting DB for partition checks repeatedly in the same process lifespan
        self._known_log_partitions = set()
        self._known_agg_partitions = set()

        # Check for testing environment to disable flushing
        self._is_testing = os.environ.get("PYTEST_CURRENT_TEST") is not None

    async def _ensure_log_partition_for_timestamp(
        self, conn: DbResource, schema: str, timestamp: datetime
    ):
        """Proactive JIT Partition Creation for Access Logs"""
        year, month = timestamp.year, timestamp.month
        partition_key = f"{schema}_logs_{year}_{month:02d}"

        if partition_key in self._known_log_partitions:
            return

        start_of_month = timestamp.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        await ensure_partition_exists(
            conn,
            table_name="access_logs",
            schema=schema,
            strategy="RANGE",
            partition_value=start_of_month,
            interval="monthly",
        )

        self._known_log_partitions.add(partition_key)

    async def _ensure_agg_partition_for_timestamp(
        self, conn: DbResource, schema: str, timestamp: datetime
    ):
        """Proactive JIT Partition Creation for Aggregates"""
        year = timestamp.year
        partition_key = f"{schema}_agg_{year}"

        if partition_key in self._known_agg_partitions:
            return

        start_of_year = timestamp.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        await ensure_partition_exists(
            conn,
            table_name="stats_aggregates",
            schema=schema,
            strategy="RANGE",
            partition_value=start_of_year,
            interval="yearly",
        )

        self._known_agg_partitions.add(partition_key)

    async def initialize(self, app_state=None):
        """
        No-op global initialization.
        Tables and partitions are created per-tenant during shell initialization
        or JIT during flush/resolution.
        """
        await self.buffer.start()
        logger.info("PostgresStatsDriver: Initialized (Tenant-scoped mode).")

    async def shutdown(self):
        """Stops the buffer and performs final flush."""
        await self.buffer.stop()

    async def buffer_record(self, record: AccessRecord):
        await self.buffer.add(record)

    async def flush(self, conn: Optional[DbResource] = None):
        """Manual flush trigger."""
        if conn:
            # If a connection is provided (e.g. from a test or a transaction),
            # we drain the buffer and flush directly using that connection.
            # Note: This bypasses the aggregator's background task for this batch.
            async with self.buffer._lock:
                records = self.buffer._buffer[:]
                self.buffer._buffer.clear()

            if records:
                await self._flush_records(records, conn=conn)
        else:
            # Regular flush triggers the aggregator and waits for it
            await self.buffer._trigger_flush(wait=True)

    async def _flush_records(
        self, records: List[AccessRecord], conn: Optional[DbResource] = None
    ):
        """Callback for the aggregator to perform batch writes."""
        if not records:
            return

        if self._is_testing:
            logger.debug(
                "PostgresStatsDriver: Flushing skipped in testing environment."
            )
            return

        async with managed_transaction(conn or self._engine) as db:
            await self._perform_flush(db, records)

    async def _perform_flush(self, conn: DbResource, records: List[AccessRecord]):

        from dynastore.modules import get_protocol
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_SCHEMA

        catalogs = get_protocol(CatalogsProtocol)
        schema_groups = defaultdict(list)
        for rec in records:
            # Resolve physical schema
            # None catalog_id or SYSTEM_CATALOG_ID always goes to SYSTEM_SCHEMA
            if rec.catalog_id is None or rec.catalog_id == SYSTEM_CATALOG_ID:
                phys_schema = SYSTEM_SCHEMA
            else:
                from dynastore.tools.protocol_helpers import get_engine

                db_resource = conn or get_engine()
                # Use allow_missing=True for graceful handling of deleted catalogs
                phys_schema = await catalogs.resolve_physical_schema(
                    rec.catalog_id, db_resource=db_resource, allow_missing=True
                )
                # If catalog not found (returns None), treat as system-level
                if phys_schema is None:
                    logger.warning(
                        f"PostgresStatsDriver: Catalog '{rec.catalog_id}' not found. Storing in system schema."
                    )
                    phys_schema = SYSTEM_SCHEMA

            schema_groups[phys_schema].append(rec)

        for schema, schema_records in schema_groups.items():
            try:
                # 1. Proactive Maintenance: Ensure partitions exist
                required_months = set()
                required_years = set()
                for rec in schema_records:
                    ts = rec.timestamp
                    required_months.add(
                        ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    )
                    required_years.add(
                        ts.replace(
                            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
                        )
                    )

                for month_ts in required_months:
                    await self._ensure_log_partition_for_timestamp(
                        conn, schema, month_ts
                    )
                for year_ts in required_years:
                    await self._ensure_agg_partition_for_timestamp(
                        conn, schema, year_ts
                    )

                # 2. Aggregate Updates (Monthly Aggregation)
                agg_data = defaultdict(lambda: {"count": 0, "latency": 0.0})
                for rec in schema_records:
                    # Aggregate by month
                    period = rec.timestamp.replace(
                        day=1, hour=0, minute=0, second=0, microsecond=0
                    )
                    pid = rec.principal_id or "none"
                    kh = rec.api_key_hash or "none"
                    cat = rec.catalog_id or "none"

                    key = (period, cat, pid, kh, rec.status_code)
                    agg_data[key]["count"] += 1
                    agg_data[key]["latency"] += rec.processing_time_ms

                for (period, cat, pid, kh, status), data in agg_data.items():
                    shard = random.randint(0, USAGE_SHARD_COUNT - 1)
                    await UPSERT_AGGREGATE.execute(
                        conn,
                        schema=schema,
                        period=period,
                        cat_id=cat,
                        pid=pid,
                        key_hash=kh,
                        status=status,
                        shard=shard,
                        count=data["count"],
                        latency=data["latency"],
                    )

                # 3. Batch Insert Access Logs
                values_clauses = []
                params = {}
                for i, rec in enumerate(schema_records):
                    p_prefix = f"r{i}"
                    values_clauses.append(
                        f"(:{p_prefix}_ts, :{p_prefix}_cat, :{p_prefix}_key, :{p_prefix}_pid, :{p_prefix}_ip, :{p_prefix}_meth, :{p_prefix}_path, :{p_prefix}_sc, :{p_prefix}_ms, :{p_prefix}_det)"
                    )
                    params.update(
                        {
                            f"{p_prefix}_ts": rec.timestamp,
                            f"{p_prefix}_cat": rec.catalog_id,
                            f"{p_prefix}_key": rec.api_key_hash,
                            f"{p_prefix}_pid": rec.principal_id,
                            f"{p_prefix}_ip": str(rec.source_ip),
                            f"{p_prefix}_meth": rec.method,
                            f"{p_prefix}_path": rec.path,
                            f"{p_prefix}_sc": rec.status_code,
                            f"{p_prefix}_ms": rec.processing_time_ms,
                            f"{p_prefix}_det": json.dumps(rec.details),
                        }
                    )

                if values_clauses:
                    full_query = INSERT_ACCESS_LOGS_BATCH.format(
                        schema=schema
                    ) + ", ".join(values_clauses)
                    await DDLQuery(full_query).execute(conn, **params)

            except Exception as e:
                logger.error(
                    f"PostgresStatsDriver: Failed to flush logs for schema '{schema}': {e}"
                )

        logger.info(
            f"Flushed {len(records)} access logs across {len(schema_groups)} schemas."
        )

    @property
    def name(self) -> str:
        return "postgres"

    async def get_summary(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs: Any,
    ) -> StatsSummary:
        from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_SCHEMA

        resolved_schema = schema or SYSTEM_SCHEMA
        if catalog_id and catalog_id != SYSTEM_CATALOG_ID and catalog_id != "_system_":
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import CatalogsProtocol

            catalogs = get_protocol(CatalogsProtocol)
            db_resource = self._engine or (catalogs.engine if catalogs else None)
            try:
                resolved_schema = (
                    await catalogs.resolve_physical_schema(
                        catalog_id, db_resource=db_resource
                    )
                    or SYSTEM_SCHEMA
                )
            except ValueError:
                pass

        return await get_stats_summary(
            self._engine,
            schema=resolved_schema,
            catalog_id=catalog_id,
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    async def get_logs(
        self,
        schema: Optional[str] = None,
        catalog_id: Optional[str] = None,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> AccessLogPage:
        from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_SCHEMA

        resolved_schema = schema or SYSTEM_SCHEMA
        if catalog_id and catalog_id != SYSTEM_CATALOG_ID and catalog_id != "_system_":
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import CatalogsProtocol

            catalogs = get_protocol(CatalogsProtocol)
            db_resource = self._engine or (catalogs.engine if catalogs else None)
            try:
                resolved_schema = (
                    await catalogs.resolve_physical_schema(
                        catalog_id, db_resource=db_resource
                    )
                    or SYSTEM_SCHEMA
                )
            except ValueError:
                pass

        return await get_access_logs(
            self._engine,
            schema=resolved_schema,
            catalog_id=catalog_id,
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
            **kwargs,
        )


# --- Optimized Query Functions ---


async def get_stats_summary(
    conn: DbResource,
    principal_id: Optional[str] = None,
    api_key_hash: Optional[str] = None,
    catalog_id: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    schema: str = "catalog",
    **kwargs,
) -> StatsSummary:
    """
    Fetches aggregated statistics using the pre-calculated stats_aggregates table.
    """
    where_clauses = ["1=1"]
    params = {}

    if principal_id:
        where_clauses.append("principal_id = :principal_id")
        params["principal_id"] = principal_id
    if api_key_hash:
        where_clauses.append("api_key_hash = :api_key_hash")
        params["api_key_hash"] = api_key_hash
    if catalog_id:
        where_clauses.append("catalog_id = :catalog_id")
        params["catalog_id"] = catalog_id
    if start_date:
        where_clauses.append("period_start >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where_clauses.append("period_start <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)
    params["empty_jsonb"] = "{}"

    query = f"""
    WITH agg AS (
        SELECT status_code, request_count, total_latency_ms, principal_id
        FROM {{schema}}.stats_aggregates
        WHERE {where_sql}
    )
    SELECT
        COALESCE(SUM(request_count), 0) as total_requests,
        COALESCE(SUM(total_latency_ms), 0) as total_time,
        COUNT(DISTINCT principal_id) as unique_principals,
        (
            SELECT COALESCE(jsonb_object_agg(status_code, c), CAST(:empty_jsonb AS jsonb))
            FROM (SELECT status_code, SUM(request_count) as c FROM agg GROUP BY status_code) s
        ) as status_code_distribution
    FROM agg
    """

    # Resolve schema from input context if we had one?
    # Usually summary/logs are called from a service that knows the catalog.
    # We'll expect 'schema' in kwargs or resolve it.
    # schema = kwargs.get("schema", "catalog")

    summary_data = await DQLQuery(
        query.format(schema=schema), result_handler=ResultHandler.ONE_DICT
    ).execute(conn, **params)

    total = int(summary_data.get("total_requests") or 0)
    total_time = float(summary_data.get("total_time") or 0)

    avg_latency = 0.0
    if total > 0:
        avg_latency = total_time / total

    return StatsSummary(
        total_requests=total,
        average_latency_ms=avg_latency,
        status_code_distribution=summary_data.get("status_code_distribution") or {},
        unique_principals=int(summary_data.get("unique_principals") or 0),
    )


async def get_access_logs(
    conn: DbResource,
    principal_id: Optional[str] = None,
    api_key_hash: Optional[str] = None,
    catalog_id: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    cursor: Optional[str] = None,
    page_size: int = 100,
    schema: str = "catalog",
    **kwargs,
) -> AccessLogPage:
    """Fetches a paginated list of access logs using keyset pagination."""

    where_clauses = []
    params = {"page_size": page_size + 1}

    if principal_id:
        where_clauses.append("principal_id = :principal_id")
        params["principal_id"] = principal_id
    if api_key_hash:
        where_clauses.append("api_key_hash = :api_key_hash")
        params["api_key_hash"] = api_key_hash
    if catalog_id:
        where_clauses.append("catalog_id = :catalog_id")
        params["catalog_id"] = catalog_id
    if start_date:
        where_clauses.append("timestamp >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where_clauses.append("timestamp <= :end_date")
        params["end_date"] = end_date

    if cursor:
        try:
            ts_str, last_id = cursor.rsplit(",", 1)
            where_clauses.append("(timestamp, id) < (:cursor_ts, :cursor_id)")
            params["cursor_ts"] = datetime.fromisoformat(ts_str)
            params["cursor_id"] = int(last_id)
        except (ValueError, IndexError):
            logger.warning(f"Invalid cursor format received: {cursor}. Ignoring.")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # schema = kwargs.get("schema", "catalog")

    query = f"""
        SELECT * FROM {schema}.access_logs
        WHERE {where_sql}
        ORDER BY timestamp DESC, id DESC
        LIMIT :page_size
    """

    result_rows = await DQLQuery(query, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, **params
    )
    all_logs = [AccessRecord.model_validate(row) for row in result_rows]

    next_cursor = None
    logs = all_logs
    if len(all_logs) > page_size:
        logs = all_logs[:page_size]
        last_log = logs[-1]
        next_cursor = f"{last_log.timestamp.isoformat()},{last_log.id}"

    return AccessLogPage(logs=logs, next_cursor=next_cursor)
