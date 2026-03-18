# The Query Executor Pattern

The `query_executor` framework is a bespoke data access layer designed to enforce consistency, safety, and a unified execution pathway bridging synchronous and asynchronous operations.

## Dual-Mode Operation

The core requirement of this framework is to transparently support passing both `AsyncConnection` objects (running in `asyncpg` within the API FastAPI layer) and `SyncConnection` objects (running via `psycopg2` in task workers) using the exact same definition and query object.

## Architecture

### 1. `ResultHandler` Abstraction
This relies on declarative ENUM lambda definitions:
- `ResultHandler.SCALAR_ONE`: Expects a single row, one column.
- `ResultHandler.ONE_DICT`: Single object returned as a dictionary.
- `ResultHandler.ALL_DICTS`: A list of objects represented as dictionaries.
- `ResultHandler.ROWCOUNT`: Count of mutations returned. 

### 2. `QueryBuilderStrategy`
Query building decouples the generation of text from execution:
- **`TemplateQueryBuilder`**: Prevents SQL injection by wrapping `SQLAlchemy` templates, mapping `{identifier}` for schema names and `:param` for dynamic values securely.
- **`FunctionQueryBuilder`**: Provides complex string query building functions injected dynamically at runtime.

### 3. Specialized Executors
- `DQLExecutor`: Used for Data Query Language (Requires ResultHandlers). 
- `DDLExecutor`: Used to apply Structural definitions. Handled transactions gracefully suppressing native "Object Exists" concurrency exceptions.
- `GeoDQLExecutor`: Sits above DQL by automatically parsing geometric object results from PostGIS `WKBElement` formats back instantly into user-readable `GeoJSON` geometries.

### 4. Implementation (Query Objects)
A developer uses standard `DQLQuery`/`DDLQuery` objects globally mapped.

```python
_get_task_query = DQLQuery(
    "SELECT * FROM tasks WHERE task_id = :task_id;",
    result_handler=ResultHandler.ONE_DICT
)

# And can then execute it anywhere with a simple, awaitable call:
task_dict = await _get_task_query.execute(conn, task_id=some_uuid)
```

## The Bridging Utilities

### The `run_in_event_loop` Wrapper
This utility allows a synchronous python script block to safely kick-start an `asyncio` event loop thread to await `Async` defined core-module logic bridging the sync boundary to fetch or sync states. 

### The Concurrency Backend (`dynastore.modules.concurrency_backend`)
This executes the reverse operation. It standardizes how asynchronous code securely drops backwards to trigger blocking IO/Sync scripts (like triggering a GCS event). 
- Tasks drop down to `asyncio.to_thread` natively. 
- API threads use `run_in_threadpool`, utilizing Starlette thread integrations transparently to the developer calling `run_in_threadpool`.
