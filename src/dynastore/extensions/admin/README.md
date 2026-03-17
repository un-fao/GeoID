# admin — Admin Extension

The `admin` extension exposes management endpoints and a web dashboard for operators with `sysadmin` or `admin` roles. It is deployed as part of the `geospatial-catalog` service.

## Activation

The extension is loaded when `SCOPE` includes `api-catalog`:

```yaml
SCOPE: "api-catalog"
```

It registers under the `/admin` prefix via `AdminService.configure_app()`.

---

## Policies

All `/admin/*` routes and the migrations dashboard page require the `admin_access` policy, which is granted to the `sysadmin` and `admin` roles only. Anonymous users and users with the `user` role are denied.

Register admin policies at application startup:

```python
from dynastore.extensions.admin.policies import register_admin_policies
register_admin_policies()
```

This is called automatically by `AdminService.__init__`.

---

## API Endpoints

### Migration Management

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/migrations/status` | Current manifest state: `UP_TO_DATE`, `PENDING_MIGRATIONS`, or `DRIFT_DETECTED` |
| `GET` | `/admin/migrations/pending` | Dry-run: SQL that would be applied per pending script |
| `POST` | `/admin/migrations/apply` | Enqueue `StructuralMigrationTask`; body: `{"scope": "all\|global\|tenant", "dry_run": false}` |
| `GET` | `/admin/migrations/history` | Full history from `public.schema_migrations` |
| `POST` | `/admin/migrations/rollback` | Enqueue rollback; body: `{"module": "db_config", "version": "v0003", "schema_name": null}` |

### Schema Evolution

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/schemas/{catalog_id}/health` | Schema health for all collections: safe/unsafe ops, drift detected |
| `POST` | `/admin/schemas/{catalog_id}/{collection_id}/evolve` | Preview or apply safe column additions; body: `{"apply_safe": false}` |

### Schema Migration (Safe Data Pipeline)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/admin/schemas/{catalog_id}/{collection_id}/migrate` | Trigger safe export→backup→recreate→import migration; body: `{"dry_run": false}` |
| `GET` | `/admin/schemas/{catalog_id}/{collection_id}/backups` | List `_bkp_` backup tables left by previous migrations |
| `DELETE` | `/admin/schemas/{catalog_id}/{collection_id}/backups/{timestamp}` | Drop backup tables for a specific migration timestamp (explicit cleanup) |

### Config Portability

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/configs/{catalog_id}/export` | Export all catalog-level and collection-level config overrides as JSON |
| `POST` | `/admin/configs/{catalog_id}/import` | Import config overrides; body: `{"catalog_configs": [], "collection_configs": [], "overwrite": true}` |

---

## Web Dashboard

The migrations dashboard is available at:

```
/web/pages/migrations_panel
```

It provides:

- **Overview tab** — module version matrix, manifest hash, current status
- **Pending tab** — SQL preview per pending script, Apply button
- **History tab** — full migration history with per-row Rollback button (shown only when `has_rollback` is true)
- **Schema Health tab** — per-collection drift status, evolve and export actions

The page is registered via the `@expose_web_page` decorator on `AdminService` and appears in the DynaStore web navigation under the admin section.

---

## Files

| Path | Purpose |
|------|---------|
| `admin_service.py` | `AdminService` class; mounts sub-routers; registers web page |
| `migration_routes.py` | FastAPI routers: `router` (migrations), `schema_router` (schema evolution + migration), `configs_router` (config portability) |
| `policies.py` | `register_admin_policies()` — policy + role registration |
| `static/migrations_panel.html` | Single-page migrations dashboard |
| `static/admin_panel.html` | General admin panel (user management, etc.) |

---

## Adding a New Admin Endpoint

1. Add a route function to `migration_routes.py` (or a new `*_routes.py` file).
2. If it is a new router file, include it in `AdminService.configure_app()`:

   ```python
   self.router.include_router(my_router, prefix="")
   ```

3. Add the new resource path to the `admin_access` policy in `policies.py` if it needs separate access control.
