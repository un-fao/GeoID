-- =============================================================================
--  Catalog Module Migration v0002 — STAC Spec Compliance
--  Owned by: dynastore.modules.catalog
--
--  Aligns catalog.catalogs and all tenant {{schema}}.collections tables with
--  the STAC Collection / Catalog specification:
--  https://stacspec.org/en/about/stac-spec/
--
--  New columns on catalog.catalogs:
--    conforms_to     — OGC API conformance classes (Catalog)
--    links           — External/additional links beyond auto-generated ones
--    assets          — Collection-level assets (thumbnails, metadata files, etc.)
--    stac_version    — STAC spec version the catalog was created with
--    stac_extensions — List of STAC extension URIs implemented
--
--  Idempotent: all DDL uses ADD COLUMN IF NOT EXISTS.
--  Tenant collections tables are updated via a PL/pgSQL loop which skips
--  schemas where the table does not exist.
-- =============================================================================

-- ─── 1. catalog.catalogs ─────────────────────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'catalog' AND table_name = 'catalogs'
    ) THEN
        ALTER TABLE catalog.catalogs
            ADD COLUMN IF NOT EXISTS conforms_to    JSONB,
            ADD COLUMN IF NOT EXISTS links          JSONB,
            ADD COLUMN IF NOT EXISTS assets         JSONB,
            ADD COLUMN IF NOT EXISTS stac_version   VARCHAR(20),
            ADD COLUMN IF NOT EXISTS stac_extensions JSONB;

        RAISE NOTICE 'catalog v0002: STAC columns added to catalog.catalogs.';
    ELSE
        RAISE NOTICE 'catalog v0002: catalog.catalogs not found — skipping.';
    END IF;
END;
$$;

-- ─── 2. All tenant {{schema}}.collections tables ───────────────────────────────
DO $$
DECLARE
    tenant_schema VARCHAR;
BEGIN
    -- Iterate every physical schema registered in catalog.catalogs.
    FOR tenant_schema IN
        SELECT physical_schema
          FROM catalog.catalogs
         WHERE physical_schema IS NOT NULL
    LOOP
        -- Guard: skip if collections table does not exist in this schema.
        IF to_regclass('"' || tenant_schema || '".collections') IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE %I.collections
                    ADD COLUMN IF NOT EXISTS links           JSONB,
                    ADD COLUMN IF NOT EXISTS assets          JSONB,
                    ADD COLUMN IF NOT EXISTS extent          JSONB,
                    ADD COLUMN IF NOT EXISTS providers       JSONB,
                    ADD COLUMN IF NOT EXISTS summaries       JSONB,
                    ADD COLUMN IF NOT EXISTS item_assets     JSONB,
                    ADD COLUMN IF NOT EXISTS stac_version    VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS stac_extensions JSONB;',
                tenant_schema
            );
            RAISE NOTICE 'catalog v0002: STAC columns added to %.collections.', tenant_schema;
        ELSE
            RAISE NOTICE 'catalog v0002: %.collections not found — skipping.', tenant_schema;
        END IF;
    END LOOP;
END;
$$;
