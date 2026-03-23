# Admin Extension

The admin extension provides system administration endpoints and dashboard pages for managing users, API keys, roles, policies, and database migrations.

## Endpoints

### User & Key Management

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/principals` | List all principals (paginated) |
| POST | `/admin/principals` | Create a new principal |
| GET | `/admin/principals/{id}` | Get principal details |
| PUT | `/admin/principals/{id}` | Update principal (roles, attributes) |
| DELETE | `/admin/principals/{id}` | Delete principal and all linked keys |
| GET | `/admin/keys` | Search API keys |
| POST | `/admin/keys` | Create API key for a principal |
| DELETE | `/admin/keys/{hash}` | Revoke an API key |
| POST | `/admin/users` | Create a user (username/password login) |

### Roles & Policies

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/roles` | List all roles |
| POST | `/admin/roles` | Create a role |
| PUT | `/admin/roles/{name}` | Update role policies |
| DELETE | `/admin/roles/{name}` | Delete a role |
| GET | `/admin/policies` | List all policies |
| POST | `/admin/policies` | Create a policy |
| PUT | `/admin/policies/{id}` | Update a policy |
| DELETE | `/admin/policies/{id}` | Delete a policy |

### Database Migrations

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/migrations/status` | Current migration status and manifest |
| GET | `/admin/migrations/pending` | Preview pending migrations (dry-run) |
| POST | `/admin/migrations/apply` | Apply pending migrations |
| GET | `/admin/migrations/history` | Full migration history |
| POST | `/admin/migrations/rollback` | Rollback a specific migration |

See [migrations.md](../architecture/migrations.md) for details on the migration system.

## Authorization

All admin endpoints require the `sysadmin` or `admin` role. The `admin_access` policy grants access to `/admin/.*` routes. The admin extension registers this policy during its `lifespan()`.

## Dashboard Pages

The admin extension contributes two web pages via `@expose_web_page`:

- **Admin Panel** (`admin_panel`) — user/key/role/policy management UI
- **Migrations Panel** (`migrations_panel`) — migration status, pending review, apply/rollback

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/extensions/admin/admin_service.py` | Main service — routes, CRUD endpoints |
| `src/dynastore/extensions/admin/migration_routes.py` | Migration API endpoints |
| `src/dynastore/extensions/admin/policies.py` | Policy registration for admin routes |
| `src/dynastore/extensions/admin/static/admin_panel.html` | Admin dashboard page |
| `src/dynastore/extensions/admin/static/migrations_panel.html` | Migrations dashboard page |
