-- =============================================================================
--  Tasks Module Migration v0001 — Durable Task Queue Fields
--  Owned by: dynastore.modules.tasks
--
--  Adds concurrency-control columns to the tasks schema and installs the
--  push-notification trigger for the QueueListener.
--  No-op if tasks.tasks does not yet exist (tasks module not loaded).
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'tasks' AND table_name = 'tasks'
    ) THEN
        -- Durable queue fields (all idempotent)
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS locked_until      TIMESTAMPTZ;
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS owner_id          VARCHAR(255);
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS retry_count       INT NOT NULL DEFAULT 0;
        ALTER TABLE tasks.tasks ADD COLUMN IF NOT EXISTS max_retries       INT NOT NULL DEFAULT 3;

        -- Fast queue polling index
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'tasks' AND indexname = 'idx_tasks_global_queue'
        ) THEN
            CREATE INDEX idx_tasks_global_queue
                ON tasks.tasks (status, locked_until)
                WHERE status IN ('PENDING', 'ACTIVE');
        END IF;

        -- Push-notification trigger (abstracts LISTEN/NOTIFY for QueueListener)
        CREATE OR REPLACE FUNCTION tasks.notify_task_ready()
        RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
        BEGIN
            PERFORM pg_notify('new_task_queued', NEW.task_id::text);
            RETURN NEW;
        END;
        $fn$;

        DROP TRIGGER IF EXISTS on_task_insert ON tasks.tasks;
        CREATE TRIGGER on_task_insert
            AFTER INSERT ON tasks.tasks
            FOR EACH ROW EXECUTE FUNCTION tasks.notify_task_ready();

        RAISE NOTICE 'tasks v0001: queue fields and notify trigger applied.';
    ELSE
        RAISE NOTICE 'tasks v0001: tasks.tasks not found — skipping (module not loaded).';
    END IF;
END;
$$;
