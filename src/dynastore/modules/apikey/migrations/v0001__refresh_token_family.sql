-- =============================================================================
--  ApiKey Module Migration v0001 — Refresh Token Family Tracking
--  Owned by: dynastore.modules.apikey
--
--  Adds family_id column to refresh_tokens for token rotation detection.
--  No-op if apikey.refresh_tokens does not yet exist.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'apikey' AND table_name = 'refresh_tokens'
    ) THEN
        ALTER TABLE apikey.refresh_tokens
            ADD COLUMN IF NOT EXISTS family_id VARCHAR(128);

        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'apikey' AND indexname = 'idx_refresh_tokens_family'
        ) THEN
            CREATE INDEX idx_refresh_tokens_family
                ON apikey.refresh_tokens (family_id)
                WHERE family_id IS NOT NULL;
        END IF;

        RAISE NOTICE 'apikey v0001: family_id column and index applied.';
    ELSE
        RAISE NOTICE 'apikey v0001: apikey.refresh_tokens not found — skipping.';
    END IF;
END;
$$;
