-- =============================================================================
--  Catalog Module Global Migration v0001 — Core Catalog Schema
--  Owned by: dynastore.modules.catalog (global scope)
--
--  Creates the `catalog` PostgreSQL schema and its two global tables:
--
--   * catalog.catalogs        — registry of all tenant catalogs
--   * catalog.shared_properties — platform-wide key/value store
--
--  Idempotent: all statements use IF NOT EXISTS.
-- =============================================================================

-- Ensure the catalog schema exists
CREATE SCHEMA IF NOT EXISTS catalog;

-- ---------------------------------------------------------------------------
-- catalog.catalogs
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS catalog.catalogs (
    id                   VARCHAR        PRIMARY KEY,
    physical_schema      VARCHAR        NOT NULL UNIQUE,
    title                JSONB,
    description          JSONB,
    keywords             JSONB,
    license              JSONB,
    conforms_to          JSONB,
    links                JSONB,
    assets               JSONB,
    stac_version         VARCHAR(20)    DEFAULT '1.0.0',
    stac_extensions      JSONB          DEFAULT '[]'::jsonb,
    extra_metadata       JSONB,
    provisioning_status  VARCHAR(50)    NOT NULL DEFAULT 'ready',
    deleted_at           TIMESTAMPTZ    DEFAULT NULL
);

-- ---------------------------------------------------------------------------
-- catalog.shared_properties
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS catalog.shared_properties (
    key_name    VARCHAR     PRIMARY KEY,
    key_value   VARCHAR     NOT NULL,
    owner_code  VARCHAR,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
