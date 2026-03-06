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
import json
import typing
import asyncio
import inspect
from contextlib import asynccontextmanager, AsyncExitStack

from dynastore.modules.protocols import ModuleProtocol
from dynastore.models.auth import AuthenticationProtocol, AuthorizationProtocol
from .service import StatsService
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DDLQuery,
    DbResource,
)
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config import maintenance_tools
from dynastore.models.shared_models import SYSTEM_SCHEMA
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    check_table_exists,
)

logger = logging.getLogger(__name__)

# ==============================================================================
#  TENANT INITIALIZATION (Stats Slice)
# ==============================================================================

TENANT_STATS_DDL = """
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

CREATE INDEX IF NOT EXISTS idx_access_logs_catalog_id ON {schema}.access_logs (catalog_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_principal_id ON {schema}.access_logs (principal_id);

CREATE TABLE IF NOT EXISTS {schema}.stats_aggregates (
    period_start TIMESTAMPTZ NOT NULL,
    catalog_id VARCHAR(100) NOT NULL DEFAULT 'none',
    principal_id VARCHAR(255) NOT NULL DEFAULT 'none',
    api_key_hash VARCHAR(64) NOT NULL DEFAULT 'none',
    status_code INT NOT NULL,
    shard_id INT NOT NULL DEFAULT 0,
    request_count BIGINT DEFAULT 0,
    total_latency_ms FLOAT DEFAULT 0,
    PRIMARY KEY (period_start, catalog_id, principal_id, api_key_hash, status_code, shard_id)
) PARTITION BY RANGE (period_start);
"""

# ==============================================================================
#  SYSTEM SCHEMA INITIALIZATION (Platform-Level Stats)
# ==============================================================================

SYSTEM_STATS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.access_logs (
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

CREATE INDEX IF NOT EXISTS idx_system_access_logs_catalog_id ON {SYSTEM_SCHEMA}.access_logs (catalog_id);
CREATE INDEX IF NOT EXISTS idx_system_access_logs_principal_id ON {SYSTEM_SCHEMA}.access_logs (principal_id);

CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.stats_aggregates (
    period_start TIMESTAMPTZ NOT NULL,
    catalog_id VARCHAR(100) NOT NULL DEFAULT 'none',
    principal_id VARCHAR(255) NOT NULL DEFAULT 'none',
    api_key_hash VARCHAR(100) NOT NULL DEFAULT 'none',
    total_requests INTEGER DEFAULT 0,
    average_latency_ms FLOAT DEFAULT 0.0,
    PRIMARY KEY (period_start, catalog_id, principal_id, api_key_hash)
);
"""


async def initialize_system_stats(conn: DbResource):
    """Initializes the system-level stats tables in the catalog schema."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    async def stats_tables_exist_check():
        t1 = await check_table_exists(conn, "access_logs", SYSTEM_SCHEMA)
        t2 = await check_table_exists(conn, "stats_aggregates", SYSTEM_SCHEMA)
        return t1 and t2

    if not await stats_tables_exist_check():
        await DDLQuery(SYSTEM_STATS_DDL).execute(conn)

    # Create future partitions for system stats
    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=SYSTEM_SCHEMA,
        table="access_logs",
        interval="monthly",
        periods_ahead=12,
        column="timestamp",
    )
    await maintenance_tools.register_retention_policy(
        conn,
        schema=SYSTEM_SCHEMA,
        table="access_logs",
        policy="prune",
        interval="daily",
        retention_period="3 months",
        column="timestamp",
    )

    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=SYSTEM_SCHEMA,
        table="stats_aggregates",
        interval="yearly",
        periods_ahead=5,
        column="period_start",
    )
    await maintenance_tools.register_retention_policy(
        conn,
        schema=SYSTEM_SCHEMA,
        table="stats_aggregates",
        policy="prune",
        interval="monthly",
        retention_period="5 years",
        column="period_start",
    )


@lifecycle_registry.sync_catalog_initializer(priority=50)
async def _initialize_stats_tenant_slice(
    conn: DbResource, schema: str, catalog_id: str
):
    """Initializes the stats module's slice of the tenant schema."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    try:
        # Check if tables exist
        async def stats_tables_exist_check():
            t1 = await check_table_exists(conn, "access_logs", schema)
            t2 = await check_table_exists(conn, "stats_aggregates", schema)
            return t1 and t2

        if not await stats_tables_exist_check():
            await DDLQuery(TENANT_STATS_DDL).execute(conn, schema=schema)

        await maintenance_tools.ensure_future_partitions(
            conn,
            schema=schema,
            table="access_logs",
            interval="monthly",
            periods_ahead=12,
            column="timestamp",
        )
        await maintenance_tools.register_retention_policy(
            conn,
            schema=schema,
            table="access_logs",
            policy="prune",
            interval="daily",
            retention_period="3 months",
            column="timestamp",
        )

        await maintenance_tools.ensure_future_partitions(
            conn,
            schema=schema,
            table="stats_aggregates",
            interval="yearly",
            periods_ahead=10,
            column="period_start",
        )
        await maintenance_tools.register_retention_policy(
            conn,
            schema=schema,
            table="stats_aggregates",
            policy="prune",
            interval="monthly",
            retention_period="10 years",
            column="period_start",
        )
    except Exception:
        import traceback

        traceback.print_exc()
        raise


from dynastore.tools.discovery import register_plugin, unregister_plugin
from .storage import PostgresStatsDriver

class STATS(ModuleProtocol):
    priority: int = 100

    @asynccontextmanager
    async def lifespan(self, app_state):
        logger.info("Initializing stats module...")
        
        async with AsyncExitStack() as stack:
            # 1. Initialize system-level stats tables
            try:
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                async with managed_transaction(engine) as conn:
                    await initialize_system_stats(conn)
                    logger.info("System stats tables initialized in 'catalog' schema.")
            except Exception as e:
                logger.warning(
                    f"StatsModule: Could not initialize system stats tables: {e}"
                )

            # 2. Driver & Service (Plugins)
            if engine:
                # Instantiate driver
                driver = PostgresStatsDriver(engine=engine)
                await stack.enter_async_context(driver.lifespan(app_state))
                register_plugin(driver)
                stack.callback(unregister_plugin, driver)
                
                # Setup facade service
                from .service import STATS_SERVICE
                await stack.enter_async_context(STATS_SERVICE.lifespan(app_state))
                register_plugin(STATS_SERVICE)
                stack.callback(unregister_plugin, STATS_SERVICE)

            yield
            
        logger.info("Stats module shut down.")
