-- =============================================================================
--  Catalog Module Global Migration v0002 — STAC Compliance Columns
--  Owned by: dynastore.modules.catalog (global scope)
--
--  Adds STAC compliance columns to catalog.catalogs for databases created
--  before these columns were part of the CREATE TABLE DDL.
--  No-op if catalog.catalogs does not yet exist.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'catalog' AND table_name = 'catalogs'
    ) THEN
        ALTER TABLE catalog.catalogs
            ADD COLUMN IF NOT EXISTS conforms_to      JSONB,
            ADD COLUMN IF NOT EXISTS links            JSONB,
            ADD COLUMN IF NOT EXISTS assets           JSONB,
            ADD COLUMN IF NOT EXISTS stac_version     VARCHAR(20) DEFAULT '1.0.0',
            ADD COLUMN IF NOT EXISTS stac_extensions  JSONB DEFAULT '[]'::jsonb;

        RAISE NOTICE 'catalog v0002: STAC columns applied to catalog.catalogs.';
    ELSE
        RAISE NOTICE 'catalog v0002: catalog.catalogs not found — skipping.';
    END IF;
END;
$$;
