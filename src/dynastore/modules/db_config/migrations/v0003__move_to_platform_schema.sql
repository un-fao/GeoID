-- =============================================================================
--  Core Migration v0003 — Move global tables to platform schema
--  Owned by: dynastore.modules.db_config (core)
--
--  Ensures the manifest_hash column exists on the (now platform-scoped)
--  app_state table, covering the case where v0002 was applied while the
--  table was still in the public schema and has since been moved.
--  Idempotent: safe to re-run.
-- =============================================================================

ALTER TABLE platform.app_state
    ADD COLUMN IF NOT EXISTS manifest_hash VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE platform.app_state
    ADD COLUMN IF NOT EXISTS tenant_manifest JSONB NOT NULL DEFAULT '{}';
ALTER TABLE platform.app_state
    ADD COLUMN IF NOT EXISTS tenant_hash VARCHAR(16) NOT NULL DEFAULT '';
