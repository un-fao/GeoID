# Processes Module (OGC API Processes)

This document describes the OGC API Processes implementation, which provides a standard interface for executing server-side operations.

## Architecture

The processes system separates process **definition** (what can run) from **execution** (how it runs):

```
Process Definition (ProcessProtocol)
    ├── id, title, description
    ├── input/output schemas (JSON Schema)
    └── execution modes (sync / async)

Execution Flow:
    POST /processes/{id}/execution
        → resolve runners for execution mode
        → authorization check
        → dispatch to first capable runner
        → return Job ID (async) or result (sync)
```

### Runner System

Runners implement `RunnerProtocol` and are discovered via the plugin system. Each runner declares which process types and execution modes it supports:

| Runner | Mode | Description |
|--------|------|-------------|
| `TaskQueueRunner` | async | Enqueues work as a task in `tasks.tasks` |
| `InlineRunner` | sync | Executes directly in the request handler |
| `BackgroundRunner` | async | Uses FastAPI BackgroundTasks |

The module tries runners in priority order. If one fails, the next is attempted.

### RunnerContext

Each runner receives a `RunnerContext`:
```
RunnerContext
├── engine       — DB connection
├── task_type    — process ID
├── caller_id    — authenticated principal
├── inputs       — execution request payload
├── db_schema    — resolved physical schema for catalog context
└── extra_context — background_tasks, etc.
```

## OGC API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/processes/` | List available processes |
| GET | `/processes/{id}` | Process description with I/O schema |
| POST | `/processes/{id}/execution` | Execute a process |
| GET | `/processes/jobs/{job_id}` | Job status |
| GET | `/processes/jobs/{job_id}/results` | Job results |

## Process Registration

Processes are registered at import time via `@register_process` or the `ProcessRegistry`:

```python
from dynastore.modules.processes import register_process

@register_process(
    process_id="my_analysis",
    title="My Analysis",
    inputs={"catalog_id": {"type": "string"}, ...},
    outputs={"result": {"type": "object"}},
)
async def run_my_analysis(context: RunnerContext) -> dict:
    ...
```

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/processes/processes_module.py` | Core dispatch logic, runner resolution |
| `src/dynastore/modules/processes/models.py` | ProcessDescription, ExecutionRequest, StatusInfo |
| `src/dynastore/modules/processes/protocols.py` | ProcessProtocol, RunnerProtocol |
| `src/dynastore/extensions/processes/processes_service.py` | OGC API Processes REST endpoints |
