-- =============================================================================
--  Core Migration v0002 — app_state manifest_hash column
--  Owned by: dynastore.modules.db_config (core)
--
--  Adds the manifest_hash column to public.app_state to enable the
--  ultra-fast single-string comparison startup path in migration_runner.py.
--  Without this column every startup falls back to full JSON comparison.
--  Idempotent: safe to re-run.
-- =============================================================================

ALTER TABLE public.app_state
    ADD COLUMN IF NOT EXISTS manifest_hash VARCHAR(16) NOT NULL DEFAULT '';

