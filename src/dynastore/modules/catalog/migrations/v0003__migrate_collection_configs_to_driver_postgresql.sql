-- =============================================================================
--  Catalog Module Migration v0003 — Config Framework: collection → driver:postgresql
--  Owned by: dynastore.modules.catalog (global scope)
--
--  Context (D5b/D6):
--    Before this migration, PG-specific fields (sidecars, partitioning,
--    collection_type) were stored in collection_configs rows with
--    plugin_id = 'collection'.
--
--    After the D5b refactor these fields belong in rows with
--    plugin_id = 'driver:postgresql' (PostgresCollectionDriverConfig).
--
--  What this migration does for every tenant schema in catalog.catalogs:
--    1. For each collection_configs row where plugin_id = 'collection' and
--       config_data contains any of {sidecars, partitioning, collection_type}:
--       a. Upsert a 'driver:postgresql' row containing those fields
--          (merged with any pre-existing 'driver:postgresql' config).
--       b. Strip the migrated keys from the original 'collection' row.
--    2. Same treatment for catalog_configs (catalog-level overrides).
--
--  Idempotent: safe to run multiple times.
-- =============================================================================

DO $$
DECLARE
    r_schema    RECORD;
    r_row       RECORD;
    migrated_keys TEXT[] := ARRAY['sidecars', 'partitioning', 'collection_type'];
    moved_data  JSONB;
    stripped    JSONB;
    existing_pg JSONB;
    merged_pg   JSONB;
    processed   INTEGER := 0;
BEGIN
    -- Guard: catalog.catalogs must exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'catalog' AND table_name = 'catalogs'
    ) THEN
        RAISE NOTICE 'v0003: catalog.catalogs not found — skipping.';
        RETURN;
    END IF;

    -- Iterate over every tenant schema
    FOR r_schema IN SELECT physical_schema FROM catalog.catalogs LOOP

        -- ---------------------------------------------------------------
        -- 1. collection_configs
        -- ---------------------------------------------------------------
        IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = r_schema.physical_schema
              AND table_name = 'collection_configs'
        ) THEN
            FOR r_row IN
                EXECUTE format(
                    'SELECT catalog_id, collection_id, config_data
                     FROM %I.collection_configs
                     WHERE plugin_id = $1
                       AND (config_data ? $2 OR config_data ? $3 OR config_data ? $4)',
                    r_schema.physical_schema
                )
                USING 'collection', 'sidecars', 'partitioning', 'collection_type'
            LOOP
                -- Extract PG-specific keys
                moved_data := '{}';
                IF r_row.config_data ? 'sidecars' THEN
                    moved_data := moved_data || jsonb_build_object('sidecars', r_row.config_data->'sidecars');
                END IF;
                IF r_row.config_data ? 'partitioning' THEN
                    moved_data := moved_data || jsonb_build_object('partitioning', r_row.config_data->'partitioning');
                END IF;
                IF r_row.config_data ? 'collection_type' THEN
                    moved_data := moved_data || jsonb_build_object('collection_type', r_row.config_data->'collection_type');
                END IF;

                -- Read existing driver:postgresql row (if any) to merge
                EXECUTE format(
                    'SELECT config_data FROM %I.collection_configs
                     WHERE catalog_id = $1 AND collection_id = $2 AND plugin_id = $3',
                    r_schema.physical_schema
                )
                INTO existing_pg
                USING r_row.catalog_id, r_row.collection_id, 'driver:postgresql';

                -- Merge: moved_data wins over existing pg config for conflicting keys
                IF existing_pg IS NOT NULL THEN
                    merged_pg := existing_pg || moved_data;
                ELSE
                    merged_pg := moved_data;
                END IF;

                -- Upsert driver:postgresql row
                EXECUTE format(
                    'INSERT INTO %I.collection_configs
                         (catalog_id, collection_id, plugin_id, config_data, updated_at)
                     VALUES ($1, $2, $3, $4, NOW())
                     ON CONFLICT (catalog_id, collection_id, plugin_id)
                     DO UPDATE SET config_data = $4, updated_at = NOW()',
                    r_schema.physical_schema
                )
                USING r_row.catalog_id, r_row.collection_id, 'driver:postgresql', merged_pg;

                -- Strip migrated keys from the collection row
                stripped := r_row.config_data
                    - 'sidecars'
                    - 'partitioning'
                    - 'collection_type';

                EXECUTE format(
                    'UPDATE %I.collection_configs
                     SET config_data = $1, updated_at = NOW()
                     WHERE catalog_id = $2 AND collection_id = $3 AND plugin_id = $4',
                    r_schema.physical_schema
                )
                USING stripped, r_row.catalog_id, r_row.collection_id, 'collection';

                processed := processed + 1;
            END LOOP;
        END IF;

        -- ---------------------------------------------------------------
        -- 2. catalog_configs (catalog-level overrides)
        -- ---------------------------------------------------------------
        IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = r_schema.physical_schema
              AND table_name = 'catalog_configs'
        ) THEN
            FOR r_row IN
                EXECUTE format(
                    'SELECT catalog_id, config_data
                     FROM %I.catalog_configs
                     WHERE plugin_id = $1
                       AND (config_data ? $2 OR config_data ? $3 OR config_data ? $4)',
                    r_schema.physical_schema
                )
                USING 'collection', 'sidecars', 'partitioning', 'collection_type'
            LOOP
                moved_data := '{}';
                IF r_row.config_data ? 'sidecars' THEN
                    moved_data := moved_data || jsonb_build_object('sidecars', r_row.config_data->'sidecars');
                END IF;
                IF r_row.config_data ? 'partitioning' THEN
                    moved_data := moved_data || jsonb_build_object('partitioning', r_row.config_data->'partitioning');
                END IF;
                IF r_row.config_data ? 'collection_type' THEN
                    moved_data := moved_data || jsonb_build_object('collection_type', r_row.config_data->'collection_type');
                END IF;

                EXECUTE format(
                    'SELECT config_data FROM %I.catalog_configs
                     WHERE catalog_id = $1 AND plugin_id = $2',
                    r_schema.physical_schema
                )
                INTO existing_pg
                USING r_row.catalog_id, 'driver:postgresql';

                IF existing_pg IS NOT NULL THEN
                    merged_pg := existing_pg || moved_data;
                ELSE
                    merged_pg := moved_data;
                END IF;

                EXECUTE format(
                    'INSERT INTO %I.catalog_configs
                         (catalog_id, plugin_id, config_data, updated_at)
                     VALUES ($1, $2, $3, NOW())
                     ON CONFLICT (catalog_id, plugin_id)
                     DO UPDATE SET config_data = $3, updated_at = NOW()',
                    r_schema.physical_schema
                )
                USING r_row.catalog_id, 'driver:postgresql', merged_pg;

                stripped := r_row.config_data
                    - 'sidecars'
                    - 'partitioning'
                    - 'collection_type';

                EXECUTE format(
                    'UPDATE %I.catalog_configs
                     SET config_data = $1, updated_at = NOW()
                     WHERE catalog_id = $2 AND plugin_id = $3',
                    r_schema.physical_schema
                )
                USING stripped, r_row.catalog_id, 'collection';

                processed := processed + 1;
            END LOOP;
        END IF;

    END LOOP;

    RAISE NOTICE 'v0003: migrated % config rows (collection → driver:postgresql).', processed;
END;
$$;
