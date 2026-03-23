-- =============================================================================
--  Tasks Module Migration v0003 — Tasks & Events Schema Evolution
--  Owned by: dynastore.modules.tasks
--
--  Adds columns introduced by the global table restructuring that are not
--  yet covered by v0001/v0002. Idempotent via ADD COLUMN IF NOT EXISTS.
-- =============================================================================

DO $$
BEGIN
    -- -----------------------------------------------------------------------
    --  tasks.tasks — additional columns
    -- -----------------------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'tasks' AND table_name = 'tasks'
    ) THEN
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS schema_name     VARCHAR(255);
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS scope           VARCHAR(50) DEFAULT 'CATALOG';
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS execution_mode  VARCHAR DEFAULT 'ASYNCHRONOUS';
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS dedup_key       VARCHAR(512);
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS started_at      TIMESTAMPTZ;
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS finished_at     TIMESTAMPTZ;
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS collection_id   VARCHAR(255);

        -- Indexes
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'tasks' AND indexname = 'idx_tasks_task_id'
        ) THEN
            CREATE INDEX idx_tasks_task_id ON tasks.tasks (task_id);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'tasks' AND indexname = 'idx_tasks_queue'
        ) THEN
            CREATE INDEX idx_tasks_queue
                ON tasks.tasks (status, task_type, execution_mode, locked_until)
                WHERE status IN ('PENDING', 'ACTIVE');
        END IF;

        RAISE NOTICE 'tasks v0003: tasks table columns and indexes applied.';
    ELSE
        RAISE NOTICE 'tasks v0003: tasks.tasks not found — skipping.';
    END IF;

    -- -----------------------------------------------------------------------
    --  tasks.events — additional columns
    -- -----------------------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'tasks' AND table_name = 'events'
    ) THEN
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS scope          VARCHAR(50) DEFAULT 'PLATFORM';
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS schema_name    VARCHAR(255);
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS collection_id  VARCHAR(255);
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS dedup_key      VARCHAR(512);
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS processed_at   TIMESTAMPTZ;
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS error_message  TEXT;
        ALTER TABLE tasks.events ADD COLUMN IF NOT EXISTS retry_count    INT DEFAULT 0;

        -- Index
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'tasks' AND indexname = 'idx_events_event_id'
        ) THEN
            CREATE INDEX idx_events_event_id ON tasks.events (event_id);
        END IF;

        RAISE NOTICE 'tasks v0003: events table columns and indexes applied.';
    ELSE
        RAISE NOTICE 'tasks v0003: tasks.events not found — skipping.';
    END IF;
END;
$$;
