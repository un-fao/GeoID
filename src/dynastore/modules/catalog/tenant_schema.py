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

PG_STORAGE_LOCATIONS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.pg_storage_locations (
    collection_id VARCHAR NOT NULL PRIMARY KEY,
    physical_table VARCHAR NOT NULL,
    schema_hash VARCHAR(64),
    created_at TIMESTAMPTZ DEFAULT NOW()
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

# 2. CONFIGS (assets table is now created by PostgresAssetDriver lifecycle hook)
TENANT_CATALOG_CONFIGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_configs (
    catalog_id  VARCHAR NOT NULL,
    plugin_id   VARCHAR NOT NULL,
    config_data JSONB NOT NULL,
    schema_hash VARCHAR(64),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (catalog_id, plugin_id)
);
"""
TENANT_COLLECTION_CONFIGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_configs (
    catalog_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    plugin_id VARCHAR NOT NULL,
    config_data JSONB NOT NULL,
    schema_hash VARCHAR(64),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (catalog_id, collection_id, plugin_id)
);
"""

# # 4. ASSET FEATURE MAP
# TENANT_ASSET_FEATURE_MAP_DDL = """
# CREATE TABLE IF NOT EXISTS {schema}.asset_feature_map (
#     asset_id VARCHAR NOT NULL,
#     catalog_id VARCHAR NOT NULL,
#     collection_id VARCHAR NOT NULL,
#     feature_geoid UUID NOT NULL,
#     created_at TIMESTAMPTZ DEFAULT NOW(),
#     PRIMARY KEY (collection_id, asset_id, feature_geoid)
# );
# CREATE INDEX IF NOT EXISTS idx_asset_feature_map_feature_geoid ON {schema}.asset_feature_map (feature_geoid);
# """

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

    # 1. Create Schema
    await ensure_schema_exists(conn, schema)

    # 2. Core Tables (Tenant-local, not globally partitioned) - Combined for efficiency
    await DDLQuery(
        TENANT_COLLECTIONS_DDL
        + PG_STORAGE_LOCATIONS_DDL
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
