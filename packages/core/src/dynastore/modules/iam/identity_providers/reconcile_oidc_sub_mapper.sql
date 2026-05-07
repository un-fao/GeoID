-- reconcile_oidc_sub_mapper.sql
--
-- One-shot reconciliation script for environments that added the Keycloak
-- oidc-sub-mapper AFTER principals were already created.
--
-- Background
-- ----------
-- Before the mapper: Keycloak 26 emits sub=null; oidc_identity.py falls back
-- to preferred_username. Principal identifier stored as 'oidc:<username>'
-- (e.g. 'oidc:alice').
--
-- After the mapper: tokens carry a real UUID sub. First login after deploy
-- creates a NEW principal row 'oidc:<uuid>' with no grants. Old row is
-- orphaned: its grants still reference the old principal_id.
--
-- Symptom: every authenticated request returns "403 Deny by Default"
-- immediately after mapper is deployed, until grants are migrated.
--
-- What this script does
-- ---------------------
-- 1. Identifies old principals (identifier = 'oidc:<non-uuid>').
-- 2. For each, finds the matching new principal (identifier = 'oidc:<uuid>')
--    by display_name equality (display_name = email on both rows).
-- 3. Copies all platform grants from the old principal_id to the new one.
-- 4. Copies catalog-scoped grants across every tenant schema.
-- 5. Marks old principals as is_active=false (non-destructive; revert with
--    UPDATE if something goes wrong).
--
-- Prerequisites
-- -------------
-- - Run in a transaction so all steps are atomic. Roll back if unexpected
--   rows appear in the diff check.
-- - Requires superuser or a role with SELECT/INSERT/UPDATE on iam.* and
--   every catalog schema's grants table.
-- - Test on a staging DB first.
--
-- To run:
--   psql $DATABASE_URL -f reconcile_oidc_sub_mapper.sql
--
-- To dry-run (see what would change without applying):
--   Wrap in BEGIN; ... ROLLBACK; instead of COMMIT.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 0: Sanity check — show candidate pairs before touching anything.
-- Inspect this output before proceeding.
-- ─────────────────────────────────────────────────────────────────────────────
\echo '=== CANDIDATE PAIRS (old_identifier -> new_identifier) ==='
SELECT
    old_p.identifier  AS old_identifier,
    new_p.identifier  AS new_identifier,
    old_p.id          AS old_id,
    new_p.id          AS new_id,
    old_p.display_name
FROM iam.principals AS old_p
JOIN iam.principals AS new_p
    ON old_p.display_name = new_p.display_name
   AND old_p.identifier != new_p.identifier
WHERE old_p.identifier ~ '^oidc:[^0-9a-f-]'       -- non-UUID part after 'oidc:'
  AND new_p.identifier  ~ '^oidc:[0-9a-f]{8}-'     -- UUID pattern
ORDER BY old_p.display_name;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1: Copy platform grants (iam.grants) from old → new principal.
-- ON CONFLICT DO NOTHING skips grants the new principal already has.
-- ─────────────────────────────────────────────────────────────────────────────
\echo '=== MIGRATING PLATFORM GRANTS ==='
WITH pairs AS (
    SELECT old_p.id AS old_id, new_p.id AS new_id
    FROM iam.principals AS old_p
    JOIN iam.principals AS new_p
        ON old_p.display_name = new_p.display_name
       AND old_p.identifier != new_p.identifier
    WHERE old_p.identifier ~ '^oidc:[^0-9a-f-]'
      AND new_p.identifier  ~ '^oidc:[0-9a-f]{8}-'
)
INSERT INTO iam.grants (
    subject_kind, subject_ref, object_kind, object_ref,
    effect, valid_from, valid_until, conditions, quota, granted_by, granted_at
)
SELECT
    g.subject_kind,
    p.new_id::text,           -- repoint to new principal
    g.object_kind,
    g.object_ref,
    g.effect,
    g.valid_from,
    g.valid_until,
    g.conditions,
    g.quota,
    g.granted_by,
    NOW()
FROM iam.grants AS g
JOIN pairs AS p ON g.subject_ref = p.old_id::text
WHERE g.subject_kind = 'principal'
ON CONFLICT (subject_kind, subject_ref, object_kind, object_ref, effect)
    DO NOTHING;

\echo '  Platform grants migrated:' (SELECT count(*) FROM iam.grants);

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 2: Copy catalog-scoped grants across every tenant schema.
-- Uses a DO block to iterate schemas dynamically.
-- ─────────────────────────────────────────────────────────────────────────────
\echo '=== MIGRATING CATALOG-SCOPED GRANTS ==='
DO $$
DECLARE
    r RECORD;
    migrated_count INT := 0;
    schema_count   INT := 0;
BEGIN
    FOR r IN
        SELECT nspname AS schema_name
        FROM pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'iam', 'public', 'tasks')
          AND nspname NOT LIKE 'pg_%'
          AND EXISTS (
              SELECT 1 FROM pg_tables
              WHERE schemaname = nspname AND tablename = 'grants'
          )
    LOOP
        schema_count := schema_count + 1;
        EXECUTE format($$
            WITH pairs AS (
                SELECT old_p.id AS old_id, new_p.id AS new_id
                FROM iam.principals AS old_p
                JOIN iam.principals AS new_p
                    ON old_p.display_name = new_p.display_name
                   AND old_p.identifier != new_p.identifier
                WHERE old_p.identifier ~ '^oidc:[^0-9a-f-]'
                  AND new_p.identifier  ~ '^oidc:[0-9a-f]{8}-'
            )
            INSERT INTO %I.grants (
                subject_kind, subject_ref, object_kind, object_ref,
                effect, valid_from, valid_until, conditions, quota,
                granted_by, granted_at
            )
            SELECT
                g.subject_kind,
                p.new_id::text,
                g.object_kind,
                g.object_ref,
                g.effect,
                g.valid_from,
                g.valid_until,
                g.conditions,
                g.quota,
                g.granted_by,
                NOW()
            FROM %I.grants AS g
            JOIN pairs AS p ON g.subject_ref = p.old_id::text
            WHERE g.subject_kind = 'principal'
            ON CONFLICT (subject_kind, subject_ref, object_kind, object_ref, effect)
                DO NOTHING
        $$, r.schema_name, r.schema_name);
        GET DIAGNOSTICS migrated_count = ROW_COUNT;
        RAISE NOTICE 'Schema %: % grant(s) migrated', r.schema_name, migrated_count;
    END LOOP;
    RAISE NOTICE 'Total schemas processed: %', schema_count;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 3: Mark old principals inactive (reversible; does NOT delete rows).
-- ─────────────────────────────────────────────────────────────────────────────
\echo '=== DEACTIVATING OLD PRINCIPALS ==='
UPDATE iam.principals AS old_p
SET
    is_active  = FALSE,
    metadata   = jsonb_set(
                     COALESCE(metadata, '{}'::jsonb),
                     '{oidc_sub_migration}',
                     jsonb_build_object(
                         'migrated_at', NOW(),
                         'reason', 'oidc-sub-mapper deployed; identifier orphaned'
                     )
                 ),
    updated_at = NOW()
FROM iam.principals AS new_p
WHERE old_p.display_name  = new_p.display_name
  AND old_p.identifier   != new_p.identifier
  AND old_p.identifier    ~ '^oidc:[^0-9a-f-]'
  AND new_p.identifier    ~ '^oidc:[0-9a-f]{8}-';

\echo '  Old principals deactivated:' (SELECT count(*) FROM iam.principals WHERE is_active = false AND metadata ? 'oidc_sub_migration');

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 4: Verification — confirm new principals now have grants.
-- ─────────────────────────────────────────────────────────────────────────────
\echo '=== VERIFICATION: NEW PRINCIPALS WITH GRANTS ==='
SELECT
    p.identifier,
    p.display_name,
    count(g.id) AS grant_count
FROM iam.principals AS p
LEFT JOIN iam.grants AS g
    ON g.subject_ref = p.id::text AND g.subject_kind = 'principal'
WHERE p.identifier ~ '^oidc:[0-9a-f]{8}-'
  AND p.is_active = TRUE
GROUP BY p.identifier, p.display_name
HAVING count(g.id) > 0
ORDER BY p.display_name;

COMMIT;

-- To roll back instead of committing:
--   Replace COMMIT; with ROLLBACK;
