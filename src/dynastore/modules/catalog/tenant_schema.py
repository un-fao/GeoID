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
from typing import List, Callable, Awaitable
from dynastore.modules.db_config.query_executor import DDLQuery, DbResource
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists

logger = logging.getLogger(__name__)

# ==============================================================================
#  REGISTRY
# ==============================================================================

TenantInitializer = Callable[[DbResource, str, str], Awaitable[None]] # (conn, schema, catalog_id)
_tenant_initializers: List[TenantInitializer] = []

def register_tenant_initializer(func: TenantInitializer) -> TenantInitializer:
    """Decorator to register a function that initializes module-specific tables in the tenant schema."""
    logger.info(f"Registering tenant initializer: {func.__module__}.{func.__name__}")
    _tenant_initializers.append(func)
    return func

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

METADATA_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.metadata (
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

# 2. CONFIGS — class_key-keyed typed storage (see modules/db_config/typed_store/).
# Physical tenant isolation: tables live in the tenant's own PG schema, no catalog_id column.
TENANT_CATALOG_CONFIGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_configs (
    class_key   TEXT        PRIMARY KEY,
    schema_id   TEXT        NOT NULL REFERENCES configs.schemas(schema_id),
    config_data JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
TENANT_COLLECTION_CONFIGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_configs (
    collection_id TEXT        NOT NULL,
    class_key     TEXT        NOT NULL,
    schema_id     TEXT        NOT NULL REFERENCES configs.schemas(schema_id),
    config_data   JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (collection_id, class_key)
);
CREATE INDEX IF NOT EXISTS ix_collection_configs_class_key_{schema}
    ON {schema}.collection_configs (class_key);
"""

# ==============================================================================
#  INITIALIZATION LOGIC
# ==============================================================================

async def initialize_tenant_shell(conn: DbResource, schema: str, catalog_id: str):
    """
    Creates the complete isolated table set for a new tenant (Catalog).
    Steps:
    1. Create Schema
    2. Create Core Tables (Collections, Assets, Configs)
    3. Run Registered Initializers (Proxy, Tasks, etc.)
    """
    logger.info(f"Initializing tenant shell for schema: {schema} (Catalog: {catalog_id})")
    
    schema = schema.strip('\'" ')

    # 1. Create Schema (+ ensure the global configs schema/tables exist, since
    # our tenant DDL FK-references configs.schemas).
    from dynastore.modules.db_config.typed_store.ddl import PLATFORM_SCHEMAS_DDL
    await ensure_schema_exists(conn, "configs")
    await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)
    await ensure_schema_exists(conn, schema)

    # 2. Core Tables (Tenant-local, not globally partitioned) - Combined for efficiency
    await DDLQuery(
        TENANT_COLLECTIONS_DDL
        + METADATA_DDL
        + TENANT_CATALOG_CONFIGS_DDL
        + TENANT_COLLECTION_CONFIGS_DDL
    ).execute(conn, schema=schema)

    # 3. Run Registered Module Initializers
    if _tenant_initializers:
        logger.info(f"Running {len(_tenant_initializers)} tenant initializers: {[f.__module__ + '.' + f.__name__ for f in _tenant_initializers]}")
        for init_func in _tenant_initializers:
            try:
                await init_func(conn, schema, catalog_id)
            except Exception as e:
                logger.error(f"Tenant initializer {init_func.__name__} failed for {schema}: {e}", exc_info=True)
                raise e

    logger.info(f"Tenant shell initialized for {schema}.")
