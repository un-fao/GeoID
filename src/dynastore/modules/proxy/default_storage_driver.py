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

from dynastore.modules.db_config.query_executor import DbConnection, DbResource, DDLQuery, managed_transaction
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.proxy import queries
from dynastore.modules.proxy.models import AnalyticsPage, ShortURL
from dynastore.modules.proxy.storage import AbstractProxyStorage
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, AsyncGenerator
from collections import defaultdict
import datetime
import asyncio
import random
import logging

logger = logging.getLogger(__name__)

# --- Buffering Logic ---

class ProxyAnalyticsBuffer:
    def __init__(self, flush_threshold: int = 500):
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._flush_threshold = flush_threshold

    async def add(self, record: Dict[str, Any]) -> bool:
        """Adds record to buffer. Returns True if flush is needed."""
        async with self._lock:
            self._buffer.append(record)
            return len(self._buffer) >= self._flush_threshold

    async def drain(self) -> List[Dict[str, Any]]:
        async with self._lock:
            if not self._buffer:
                return []
            records = self._buffer[:]
            self._buffer.clear()
            return records

class PostgresProxyStorage(AbstractProxyStorage):
    name = "postgres"
    priority = 10
    
    def __init__(self):
        self.buffer = ProxyAnalyticsBuffer()
        # Shard count for aggregates (reduces write contention)
        self.SHARD_COUNT = 16
        self._known_partitions = set()

    
    async def _ensure_partition_for_timestamp(self, conn: DbResource, schema: str, timestamp: datetime.datetime):
        """Proactive JIT Partition Creation"""
        year, month = timestamp.year, timestamp.month
        partition_key = f"{schema}_{year}_{month:02d}"
        
        if partition_key in self._known_partitions:
            return

        start_of_month = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Uses the shared helper which handles the specific CREATE TABLE syntax safely
        await ensure_partition_exists(
            conn, 
            table_name="url_analytics", 
            schema=schema, 
            strategy="RANGE", 
            partition_value=start_of_month,
            interval="monthly"
        )
        self._known_partitions.add(partition_key)

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Initialises the base tables, functions, and sequences for the proxy service.
        """
        from dynastore.tools.protocol_helpers import get_engine
        engine = get_engine(app_state)
        if not engine:
            logger.error("PostgresProxyStorage: engine not found during lifespan.")
            yield
            return

        async with managed_transaction(engine) as conn:
            # Use maintenance tools to ensure safe, locked initialization
            async with maintenance_tools.acquire_startup_lock(conn, "proxy_storage_init"):
                await queries.CREATE_SHORT_URL_SEQUENCE.execute(conn, schema="proxy")
                await queries.CREATE_BASE62_FUNCTION.execute(conn, schema="proxy")
                await queries.CREATE_OBFUSCATE_FUNCTION.execute(conn, schema="proxy")
                await queries.CREATE_SHORT_URLS_TABLE.execute(conn, schema="proxy")
                await queries.CREATE_URL_ANALYTICS_TABLE.execute(conn, schema="proxy")
                await queries.CREATE_PROXY_AGGREGATES_TABLE.execute(conn, schema="proxy")
                await queries.CREATE_PROXY_AGGREGATES_INDEX_KEY_PERIOD.execute(conn, schema="proxy")
                await queries.CREATE_PROXY_AGGREGATES_INDEX_PERIOD_BRIN.execute(conn, schema="proxy")
        
        yield

    async def setup_partitions(self, conn: DbConnection, schema: str, for_date: datetime.date):
        if queries.PROXY_ANALYTICS_PARTITION_INTERVAL == 'MONTHLY':
            partition_queries = queries.create_analytics_partition_for_month(for_date.year, for_date.month, schema=schema)
            for query in partition_queries:
                await query.execute(conn, schema=schema)
        else:
            raise NotImplementedError(f"Partition interval '{queries.PROXY_ANALYTICS_PARTITION_INTERVAL}' not implemented (Schema: {schema}).")

    async def insert_short_url(self, conn: DbResource, schema: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> ShortURL:
        if custom_key:
             return await queries.INSERT_SHORT_URL_WITH_CUSTOM_KEY.execute(
                conn, schema=schema, custom_key=custom_key, long_url=long_url, collection_id=collection_id or '_catalog_', comment=comment
            )
        return await queries.INSERT_SHORT_URL_WITH_GENERATED_KEY.execute(
            conn, schema=schema, long_url=long_url, collection_id=collection_id or '_catalog_', comment=comment
        )

    async def select_urls_by_collection(self, conn: DbResource, schema: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[ShortURL]:
        return await queries.GET_URLS_BY_COLLECTION.execute(
            conn, schema=schema, collection_id=collection_id, limit=limit, offset=offset
        )

    async def select_long_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        return await queries.GET_LONG_URL.execute(conn, schema=schema, short_key=short_key)

    async def insert_redirect_log(self, conn: DbResource, schema: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime):
        """
        Buffers the redirect log in memory. 
        If buffer exceeds threshold, it flushes to DB immediately.
        Note: 'conn' is kept for signature compatibility but used for flush if needed.
        """
        record = {
            "schema": schema,
            "short_key": short_key,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "referrer": referrer,
            "timestamp": timestamp
        }
        
        needs_flush = await self.buffer.add(record)
        
        if needs_flush:
            await self.flush(conn)

    async def flush(self, conn: DbResource):
        """
        Persists buffered logs and updates sharded aggregates.
        """
        records = await self.buffer.drain()
        if not records:
            return
        

        async with managed_transaction(conn) as tx_conn:
            # 1. Proactive Maintenance
            required_months = set()
            for rec in records:
                # Assuming timestamp is a datetime object
                await self._ensure_partition_for_timestamp(tx_conn, rec["schema"], rec["timestamp"])
            
            # 2. Aggregation Phase (Memory)
            # Group by (Schema, Hour, ShortKey)
            agg_data = defaultdict(int)
            for rec in records:
                # Round to hour
                period = rec["timestamp"].replace(minute=0, second=0, microsecond=0)
                key = (rec["schema"], period, rec["short_key"])
                agg_data[key] += 1
            
            # 3. Write Aggregates (Sharded Upsert)
            for (schema, period, short_key), count in agg_data.items():
                shard_id = random.randint(0, self.SHARD_COUNT - 1)
                await queries.UPSERT_PROXY_AGGREGATE.execute(
                    tx_conn,
                    schema=schema,
                    period=period,
                    short_key=short_key,
                    shard=shard_id,
                    count=count
                )

            # 4. Bulk Insert Raw Logs
            
            # Construct Bulk Insert Query (Grouped by Schema)
            schema_records = defaultdict(list)
            for rec in records:
                schema_records[rec["schema"]].append(rec)

            for schema, s_records in schema_records.items():
                values_clauses = []
                params = {}
                for i, rec in enumerate(s_records):
                    p_prefix = f"p{i}"
                    values_clauses.append(
                        f"(:{p_prefix}_key, :{p_prefix}_ip, :{p_prefix}_ua, :{p_prefix}_ref, :{p_prefix}_ts)"
                    )
                    params.update({
                        f"{p_prefix}_key": rec["short_key"],
                        f"{p_prefix}_ip": rec["ip_address"],
                        f"{p_prefix}_ua": rec["user_agent"],
                        f"{p_prefix}_ref": rec["referrer"],
                        f"{p_prefix}_ts": rec["timestamp"]
                    })

                if values_clauses:
                    try:
                        full_query_template = queries.INSERT_LOG_REDIRECT_BATCH.format(schema=schema)
                        full_query = full_query_template + ", ".join(values_clauses)
                        await DDLQuery(full_query).execute(tx_conn, **params)
                    except Exception as e:
                        # Schema might have been dropped during test teardown - log warning and continue
                        logger.warning(f"Failed to flush analytics for schema '{schema}': {e}")
                
        logger.info(f"Flushed {len(records)} proxy logs.")

    async def select_analytics(self, conn: DbResource, schema: str, short_key: str, cursor: Optional[str] = None, page_size: int = 100, aggregate: bool = False, start_date: Optional[datetime.datetime] = None, end_date: Optional[datetime.datetime] = None) -> AnalyticsPage:
        # Always fetch the long_url for the report
        long_url_task = self.select_long_url(conn, schema, short_key)

        # Conditionally choose the query based on whether date filters are provided
        if start_date and end_date:
            if cursor:
                results_task = queries.GET_ANALYTICS_FILTERED.execute(
                    conn, schema=schema, short_key=short_key, cursor=int(cursor), page_size=page_size, start_date=start_date, end_date=end_date
                )
            else:
                results_task = queries.GET_ANALYTICS_INITIAL_FILTERED.execute(
                    conn, schema=schema, short_key=short_key, page_size=page_size, start_date=start_date, end_date=end_date
                )
        else:
            if cursor:
                results_task = queries.GET_ANALYTICS.execute(
                    conn, schema=schema, short_key=short_key, cursor=int(cursor), page_size=page_size
                )
            else:
                results_task = queries.GET_ANALYTICS_INITIAL.execute(
                    conn, schema=schema, short_key=short_key, page_size=page_size
                )

        tasks = [long_url_task, results_task]

        if aggregate:
            # OPTIMIZED: These now query 'hourly_aggregates' instead of raw logs
            total_clicks_task = queries.GET_ANALYTICS_TOTAL_CLICKS.execute(conn, schema=schema, short_key=short_key)
            clicks_per_day_task = queries.GET_ANALYTICS_CLICKS_PER_DAY.execute(conn, schema=schema, short_key=short_key)
            
            # These still query raw logs (Top N is hard to pre-aggregate without explosion)
            top_referrers_task = queries.GET_ANALYTICS_TOP_REFERRERS.execute(conn, schema=schema, short_key=short_key)
            top_user_agents_task = queries.GET_ANALYTICS_TOP_USER_AGENTS.execute(conn, schema=schema, short_key=short_key)
            
            tasks.extend([total_clicks_task, clicks_per_day_task, top_referrers_task, top_user_agents_task])

        # Run all queries concurrently
        query_results = await asyncio.gather(*tasks)

        long_url = query_results[0]
        results = query_results[1]
        aggregations = None
        if aggregate:
            aggregations = {
                "total_clicks": query_results[2],
                "clicks_per_day": query_results[3],
                "top_referrers": query_results[4],
                "top_user_agents": query_results[5]
            }

        next_cursor = str(results[-1].id) if results and len(results) == page_size else None

        return AnalyticsPage(data=results, long_url=long_url, next_cursor=next_cursor, aggregations=aggregations)

    async def drop_short_url(self, conn: DbResource, schema: str, short_key: str) -> Optional[str]:
        # Fix: Return type must be Optional[str] to match AbstractProxyStorage
        deleted_short_key = await queries.DELETE_SHORT_URL.execute(conn, schema=schema, short_key=short_key)
        return deleted_short_key