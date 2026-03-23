-- =============================================================================
--  Catalog Tenant Migration v0003 — STAC Compliance Columns on Collections
--  Owned by: dynastore.modules.catalog (tenant scope)
--
--  Adds STAC compliance columns to {schema}.collections for tenants created
--  before these columns were part of the CREATE TABLE DDL.
--  No-op if collections table does not yet exist in this tenant schema.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = 'collections'
    ) THEN
        ALTER TABLE "{schema}".collections
            ADD COLUMN IF NOT EXISTS links            JSONB,
            ADD COLUMN IF NOT EXISTS assets           JSONB,
            ADD COLUMN IF NOT EXISTS extent           JSONB,
            ADD COLUMN IF NOT EXISTS providers        JSONB,
            ADD COLUMN IF NOT EXISTS summaries        JSONB,
            ADD COLUMN IF NOT EXISTS item_assets      JSONB,
            ADD COLUMN IF NOT EXISTS stac_version     VARCHAR,
            ADD COLUMN IF NOT EXISTS stac_extensions  JSONB;

        RAISE NOTICE 'catalog_tenant v0003: STAC columns applied to {schema}.collections.';
    ELSE
        RAISE NOTICE 'catalog_tenant v0003: {schema}.collections not found — skipping.';
    END IF;
END;
$$;
