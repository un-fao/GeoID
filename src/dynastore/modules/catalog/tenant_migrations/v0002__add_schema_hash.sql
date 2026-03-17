-- =============================================================================
--  Catalog Tenant Migration v0002 — Add schema_hash column
--  Owned by: dynastore.modules.catalog (tenant scope)
--
--  Adds schema_hash to collection_configs and catalog_configs for schemas
--  created before the column was introduced.
--
--  Idempotent: uses ADD COLUMN IF NOT EXISTS.
-- =============================================================================

ALTER TABLE {schema}.collection_configs
    ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64);

ALTER TABLE {schema}.catalog_configs
    ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64);
