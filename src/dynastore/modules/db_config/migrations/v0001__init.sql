-- =============================================================================
--  DynaStore Migration v0001 — Core Extensions
--  Baseline: enables the required PostgreSQL extensions.
--  This is the canonical initial state of any DynaStore database.
--  Idempotent: safe to apply to a database that already has these extensions.
-- =============================================================================

-- Fundamental for all geospatial operations.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Required for advanced index types (GiST on B-Tree comparable types).
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Required for GIN indexing on primitive types (e.g., bigint arrays).
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Required for pg_cron scheduled jobs (partition maintenance, retention).
CREATE EXTENSION IF NOT EXISTS pg_cron;
