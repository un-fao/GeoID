-- =============================================================================
--  Catalog Tenant Migration v0001 — Config Tables
--  Owned by: dynastore.modules.catalog (tenant scope)
--
--  Ensures catalog_configs and collection_configs tables exist in the
--  tenant schema.  New catalogs already get these via initialize_tenant_shell,
--  but pre-existing tenants created before configs support may be missing them.
--
--  Idempotent: uses CREATE TABLE IF NOT EXISTS.
-- =============================================================================

CREATE TABLE IF NOT EXISTS {schema}.catalog_configs (
    catalog_id   VARCHAR NOT NULL,
    plugin_id    VARCHAR NOT NULL,
    config_data  JSONB NOT NULL,
    schema_hash  VARCHAR(64),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (catalog_id, plugin_id)
);

CREATE TABLE IF NOT EXISTS {schema}.collection_configs (
    catalog_id    VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    plugin_id     VARCHAR NOT NULL,
    config_data   JSONB NOT NULL,
    schema_hash   VARCHAR(64),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (catalog_id, collection_id, plugin_id)
);
