# Platform Notebooks with Decoupled Module Registration

**Date:** 2026-04-09
**Status:** Draft
**Scope:** NotebooksModule, NotebooksExtension, pyproject.toml scopes

## Problem

Example notebooks are bundled as static files inside `extensions/notebooks/examples/` and served read-only from the filesystem. They are only meaningful in the tools service context and invisible to users of the geoid, catalog, and maps services. Modules that own domain knowledge (dimensions, catalog management, ingestion) have no way to ship their own example notebooks. There is no platform-level notebook storage, no logical deletion, and no ownership tracking.

## Goals

1. Platform-level notebook storage in a module-owned `notebooks` schema, following the established pattern (`tiles.*`, `styles.*`, `configs.*`).
2. Decoupled registration API so any module can register example notebooks without depending on the extension layer.
3. Logical deletion (`deleted_at`) and ownership tracking on both platform and tenant notebooks.
4. Copy semantics: users copy platform notebooks into their own catalog; copies are independent.
5. All services except worker expose and can register notebooks.

## Non-Goals

- Live sync between platform notebooks and tenant copies (fork tracking). Can be added later via a `forked_from` column.
- Versioning of platform notebooks. Module re-deploys with updated `.ipynb` files; sysadmin manages via REST.
- Notebook execution backend (JupyterLite runs client-side, unchanged).

---

## Architecture

### Storage: Two Tables

#### `notebooks.platform_notebooks` (global, cross-tenant)

Created by `NotebooksModule.lifespan()` using `ensure_schema_exists(conn, "notebooks")` + DDL, under a startup lock.

```sql
CREATE TABLE IF NOT EXISTS notebooks.platform_notebooks (
    notebook_id    VARCHAR NOT NULL PRIMARY KEY,
    title          TEXT NOT NULL,
    description    TEXT,
    content        JSONB NOT NULL,
    metadata       JSONB DEFAULT '{}',
    registered_by  VARCHAR NOT NULL,
    owner_type     VARCHAR NOT NULL CHECK (owner_type IN ('module', 'sysadmin')),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    deleted_at     TIMESTAMPTZ DEFAULT NULL
);
```

- `registered_by`: module name (e.g. `"dimensions_module"`) or `"sysadmin"`.
- `owner_type`: `"module"` for auto-registered, `"sysadmin"` for manually managed.
- `deleted_at`: `NULL` means active; set to `NOW()` on soft-delete.
- Queries always filter `WHERE deleted_at IS NULL` unless explicitly listing deleted items.

#### `{tenant_schema}.notebooks` (per-catalog, existing -- enhanced)

New columns added via migration:

```sql
ALTER TABLE {schema}.notebooks
    ADD COLUMN IF NOT EXISTS deleted_at  TIMESTAMPTZ DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS owner_id    VARCHAR,
    ADD COLUMN IF NOT EXISTS copied_from VARCHAR DEFAULT NULL;
```

- `deleted_at`: soft-delete, same semantics as platform table.
- `owner_id`: principal ID of the user who created or copied the notebook.
- `copied_from`: platform `notebook_id` reference (informational, no FK). `NULL` for user-created notebooks.
- All existing queries updated to include `WHERE deleted_at IS NULL`.

---

### Module Registration API

Located in `modules/notebooks/example_registry.py`.

```python
_platform_registry: list[_PendingNotebook] = []

def register_platform_notebook(
    notebook_id: str,
    registered_by: str,
    notebook_path: Path | None = None,
    notebook_content: dict | None = None,
) -> None:
    """Register an example notebook for platform-level seeding.

    Call at module import time. Seeding happens during NotebooksModule.lifespan().
    Provide either notebook_path (to a .ipynb file) or notebook_content (raw dict).
    """
```

- Collects registrations in an in-memory list before the DB is available.
- `NotebooksModule.lifespan()` calls `seed_platform_notebooks(conn)` after DDL.
- Seeding uses `INSERT ... ON CONFLICT (notebook_id) DO NOTHING` -- never overwrites existing rows, preserving sysadmin edits.
- Modules call `register_platform_notebook()` in their `__init__.py` or a dedicated `examples.py` file, shipping `.ipynb` files as package data in their own `examples/` directories.

### Module Lifespan: Schema Init + Seeding

Follows the established module pattern (see `StylesModule`, `TilesModule`) using `db_config` tools:

```python
# modules/notebooks/notebooks_module.py

from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery

class NotebooksModule(ModuleProtocol):
    priority: int = 100

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("NotebooksModule cannot initialize: database engine not found.")
            yield; return

        logger.info("NotebooksModule: Initializing schema...")
        try:
            # Check table existence BEFORE acquiring lock -- avoids lock
            # contention on normal restarts where the table already exists.
            async with managed_transaction(engine) as conn:
                table_exists = await check_table_exists(conn, "platform_notebooks", "notebooks")

            if not table_exists:
                async with managed_transaction(engine) as conn:
                    async with maintenance_tools.acquire_startup_lock(conn, "notebooks_module"):
                        await maintenance_tools.ensure_schema_exists(conn, "notebooks")
                        await DDLQuery(PLATFORM_NOTEBOOKS_DDL).execute(conn)

            # Seed module-registered platform notebooks (DO NOTHING on conflict)
            # Always run -- new modules may register new notebooks on upgrade.
            async with managed_transaction(engine) as conn:
                await seed_platform_notebooks(conn)

            logger.info("NotebooksModule: Initialization complete.")
        except Exception as e:
            logger.error(f"CRITICAL: NotebooksModule initialization failed: {e}", exc_info=True)

        yield
```

Key `db_config` tools used:
- `locking_tools.check_table_exists(conn, "platform_notebooks", "notebooks")` -- fast `pg_tables` lookup; skips lock entirely on subsequent startups.
- `maintenance_tools.acquire_startup_lock(conn, "notebooks_module")` -- only acquired on first-time DDL; prevents concurrent DDL in multi-instance deploys.
- `maintenance_tools.ensure_schema_exists(conn, "notebooks")` -- `CREATE SCHEMA IF NOT EXISTS notebooks`.
- `DDLQuery(DDL).execute(conn)` -- idempotent table creation.
- `managed_transaction(engine)` -- auto-commit/rollback transaction wrapper.

### Seeding Flow

```
App startup
  -> modules.discover_modules()
  -> module __init__.py imports trigger register_platform_notebook() calls
  -> NotebooksModule.lifespan():
       1. check_table_exists(conn, "platform_notebooks", "notebooks")
       2. IF NOT EXISTS:
            a. acquire_startup_lock(conn, "notebooks_module")
            b. ensure_schema_exists(conn, "notebooks")
            c. DDLQuery(PLATFORM_NOTEBOOKS_DDL).execute(conn)
       3. seed_platform_notebooks(conn)  -- always runs, ON CONFLICT DO NOTHING
  -> yield (module ready)
```

---

### Extension REST API

#### Platform Notebook Endpoints (new)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/notebooks/platform` | none | List active platform notebooks (metadata only) |
| `GET` | `/notebooks/platform/{notebook_id}` | none | Get full platform notebook content |
| `PUT` | `/notebooks/platform/{notebook_id}` | sysadmin | Create or update a platform notebook |
| `DELETE` | `/notebooks/platform/{notebook_id}` | sysadmin | Soft-delete a platform notebook |

#### Tenant Notebook Endpoints (existing, updated)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/notebooks/{catalog_id}` | user | List active tenant notebooks |
| `GET` | `/notebooks/{catalog_id}/{notebook_id}` | user | Get full tenant notebook content |
| `PUT` | `/notebooks/{catalog_id}/{notebook_id}` | user | Save tenant notebook |
| `DELETE` | `/notebooks/{catalog_id}/{notebook_id}` | user | Soft-delete tenant notebook |
| `POST` | `/notebooks/{catalog_id}/copy/{platform_notebook_id}` | user | Copy platform notebook into tenant catalog |

#### Deprecated Endpoints (to remove)

| Method | Path | Reason |
|--------|------|--------|
| `GET` | `/notebooks/examples` | Replaced by `/notebooks/platform` |
| `GET` | `/notebooks/examples/{notebook_code}` | Replaced by `/notebooks/platform/{notebook_id}` |

#### Copy Semantics

`POST /notebooks/{catalog_id}/copy/{platform_notebook_id}`:

1. Read platform notebook from `notebooks.platform_notebooks`.
2. Insert into `{tenant_schema}.notebooks` with:
   - Same `notebook_id` as the platform notebook.
   - `owner_id` = current user principal ID.
   - `copied_from` = platform `notebook_id`.
3. Uses `INSERT ... ON CONFLICT (notebook_id) DO NOTHING` -- does not overwrite if tenant already has a notebook with that ID.
4. Returns the tenant notebook.

---

### Scope Changes

In `pyproject.toml`, add `notebooks` to `scope_maps`:

```diff
-scope_maps = ["dynastore[core,catalog_grp,google_grp,maps_grp,monitor_grp,module_cache]"]
+scope_maps = ["dynastore[core,catalog_grp,google_grp,maps_grp,monitor_grp,module_cache,notebooks]"]
```

- `scope_catalog`: already includes `notebooks`.
- `scope_geoid`: inherits `scope_catalog`, already includes `notebooks`.
- `scope_tools`: already includes `notebooks`.
- `scope_worker`: excluded (no REST surface in cloud, simulates Cloud Run jobs).

---

### Lifecycle and Ownership

#### Platform Notebooks

- **Module-registered** (`owner_type = 'module'`): seeded at startup with `DO NOTHING` semantics. Immutable via DB seeding -- only sysadmin can update/delete via REST after initial registration. Module updates ship new `.ipynb` files; to re-seed, use a new `notebook_id` or sysadmin deletes the old one first.
- **Sysadmin-managed** (`owner_type = 'sysadmin'`): full CRUD via REST. `registered_by = 'sysadmin'`.
- **Soft-delete**: sets `deleted_at = NOW()`. Not returned by list/get queries. No hard-delete endpoint.

#### Tenant Notebooks

- **User-created** (`copied_from = NULL`): full CRUD by catalog users.
- **Copied from platform** (`copied_from = <platform_notebook_id>`): independent copy, full CRUD. No live sync with platform source.
- **Soft-delete**: sets `deleted_at = NOW()`. Same behavior as platform.
- **Ownership**: `owner_id` tracks the principal who created/copied.

---

### Cleanup

1. **Remove duplicate lifecycle hook**: the `@lifecycle_registry.sync_catalog_initializer` in `notebooks_db.py` (lines 35-42) duplicates the one in `tenant_initialization.py`. Keep one, remove the other.
2. **Move example `.ipynb` files**: from `extensions/notebooks/examples/` to the owning module's `examples/` directory. Each module calls `register_platform_notebook()` in its init.
3. **Remove `_examples_dir` logic**: from `NotebooksExtension.__init__()` and the two filesystem-based endpoints (`list_example_notebooks`, `get_example_notebook`).
4. **Update frontend**: `static/notebooks.html` to fetch from `/notebooks/platform` instead of `/notebooks/examples`, and add a "Copy to catalog" button.

---

### Files to Create or Modify

| File | Action |
|------|--------|
| `modules/notebooks/example_registry.py` | **Create** -- registration API + seeding logic |
| `modules/notebooks/platform_db.py` | **Create** -- DDL + CRUD for `notebooks.platform_notebooks` |
| `modules/notebooks/notebooks_module.py` | **Modify** -- add lifespan for global schema init + seeding |
| `modules/notebooks/notebooks_db.py` | **Modify** -- add `deleted_at`/`owner_id`/`copied_from` to tenant queries, remove duplicate hook |
| `modules/notebooks/models.py` | **Modify** -- add `PlatformNotebook`, `PlatformNotebookCreate` models; add `deleted_at`, `owner_id`, `copied_from` to `Notebook` |
| `modules/notebooks/__init__.py` | **Modify** -- export `register_platform_notebook` |
| `extensions/notebooks/notebooks_extension.py` | **Modify** -- replace filesystem examples with platform endpoints, add copy endpoint, soft-delete |
| `extensions/notebooks/tenant_initialization.py` | **Modify** -- add new columns to DDL for NEW catalogs |
| `extensions/notebooks/static/notebooks.html` | **Modify** -- update API calls, add copy button |
| `pyproject.toml` | **Modify** -- add `notebooks` to `scope_maps` |
| Owning modules (e.g. dimensions) | **Modify** -- add `examples/` dir, call `register_platform_notebook()` |

### Files to Remove

| File | Reason |
|------|--------|
| `extensions/notebooks/examples/*.ipynb` | Moved to owning modules |
| `extensions/notebooks/examples/__init__.py` | Directory removed |

---

### Migration Strategy

**Existing catalogs** -- a tenant-level migration script (run per schema via the migration runner) adds the new columns:

```sql
-- Migration: add soft-delete, ownership, and provenance to tenant notebooks
ALTER TABLE {schema}.notebooks
    ADD COLUMN IF NOT EXISTS deleted_at  TIMESTAMPTZ DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS owner_id    VARCHAR,
    ADD COLUMN IF NOT EXISTS copied_from VARCHAR DEFAULT NULL;
```

**New catalogs** -- `tenant_initialization.py` includes the new columns in the `CREATE TABLE` DDL, so they exist from the start.

**Platform table** -- created fresh by `NotebooksModule.lifespan()` (no migration needed -- it does not exist yet).

### Example Notebook Provenance

The two existing example notebooks (`01_creating_dimensions.ipynb`, `02_asis_dimensions.ipynb`) are dimension-related and should be moved to the module that owns dimension logic. Each module that ships examples places `.ipynb` files in its own `examples/` directory and calls `register_platform_notebook()` in its init.
