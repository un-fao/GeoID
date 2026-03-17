# structural_migration — Structural Migration Task

This task wraps `migration_runner.run_migrations()` as a `TaskProtocol` implementation so it can be triggered asynchronously from the admin API and tracked via the task system.

## Task Type

```
task_type = "structural_migration"
```

## Inputs

```python
class StructuralMigrationInputs(BaseModel):
    scope: Literal["all", "global", "tenant"] = "all"
    dry_run: bool = False
```

| Field | Default | Description |
|-------|---------|-------------|
| `scope` | `"all"` | `"global"` — global schema only; `"tenant"` — per-tenant schemas only; `"all"` — both |
| `dry_run` | `False` | When `True`, returns SQL that would be applied without executing anything |

## Outputs

The task stores its result in `task.outputs`:

```json
{
  "applied": ["v0003__rollback_support.sql", "..."],
  "status": "UP_TO_DATE | COMPLETED | DRY_RUN",
  "dry_run": false
}
```

When `dry_run=True`, `applied` contains the SQL strings that would be executed (not file names).

## Triggering via Admin API

The preferred way to trigger this task is via the admin REST API:

```
POST /admin/migrations/apply
Content-Type: application/json

{
  "scope": "all",
  "dry_run": false
}
```

This enqueues the task and returns the `task_id`. Poll task status at:

```
GET /tasks/{task_id}
```

## Direct Enqueue

From application code:

```python
from dynastore.modules.tasks.queue import TaskQueue
from dynastore.tasks.structural_migration.task import StructuralMigrationInputs

await TaskQueue.enqueue(
    task_type="structural_migration",
    schema_name="system",
    scope="SYSTEM",
    inputs=StructuralMigrationInputs(scope="all", dry_run=False).model_dump(),
)
```

## Advisory Lock

The underlying `run_migrations()` call acquires a cluster-wide PostgreSQL advisory lock. If another instance is already running migrations, this task blocks until the lock is released, then detects there is nothing left to apply and completes successfully with `status="UP_TO_DATE"`.

## Priority

`StructuralMigrationTask.priority = 5` — higher priority than most background jobs to ensure migrations are not queued behind large ingestion batches.
