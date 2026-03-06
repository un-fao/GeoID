-- =============================================================================
--  Catalog Module Migration v0001 — Provisioning Status
--  Owned by: dynastore.modules.catalog
--
--  Adds provisioning_status column to catalog.catalogs to gate API access
--  during async GCP bucket creation. Default 'ready' means no gating (on-premise).
--  No-op if catalog.catalogs does not yet exist (catalog module not loaded).
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'catalog' AND table_name = 'catalogs'
    ) THEN
        ALTER TABLE catalog.catalogs
            ADD COLUMN IF NOT EXISTS provisioning_status VARCHAR(50) NOT NULL DEFAULT 'ready';

        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'catalog' AND indexname = 'idx_catalogs_provisioning'
        ) THEN
            CREATE INDEX idx_catalogs_provisioning
                ON catalog.catalogs (provisioning_status)
                WHERE provisioning_status != 'ready';
        END IF;

        RAISE NOTICE 'catalog v0001: provisioning_status column applied.';
    ELSE
        RAISE NOTICE 'catalog v0001: catalog.catalogs not found — skipping (module not loaded).';
    END IF;
END;
$$;
