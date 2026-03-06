-- =============================================================================
--  Tasks Module Migration v0002 — Add job type column
--  Owned by: dynastore.modules.tasks
--
--  Adds the 'type' column to the tasks table to distinguish between
--  internal 'task' and public 'process'.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'tasks' AND table_name = 'tasks'
    ) THEN
        -- Add type column if it doesn't exist
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS type VARCHAR NOT NULL DEFAULT 'task';
        
        RAISE NOTICE 'tasks v0002: type column added.';
    ELSE
        RAISE NOTICE 'tasks v0002: tasks.tasks not found — skipping.';
    END IF;
END;
$$;
