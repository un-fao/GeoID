# schema_migration — Safe Data Migration Task

Implements the safe export → backup-rename → recreate → import pipeline for per-collection physical table migrations. Used when a collection's schema change is classified as **unsafe** (column type change, partition key change, column removal) by the `SchemaEvolutionEngine`.

## Task Type

```
task_type = "schema_migration"
```

## When to Use

Use this task when `POST /admin/schemas/{catalog_id}/{collection_id}/evolve` returns `unsafe_ops` in its response. Safe operations (`ADD COLUMN`, `CREATE INDEX`) can be applied directly via that endpoint. Unsafe ones require this pipeline.

## Inputs

```python
class SchemaMigrationInputs(BaseModel):
    catalog_id: str
    collection_id: str
    target_config: Optional[Dict[str, Any]] = None  # override current config
    dry_run: bool = False
```

| Field | Description |
|-------|-------------|
| `catalog_id` | Catalog containing the collection |
| `collection_id` | Collection whose physical tables will be migrated |
| `target_config` | Optional new `CollectionPostgresqlDriverConfig` dict; if omitted, current config is re-applied |
| `dry_run` | When `True`, returns what would happen without touching the database |

## Outputs (`SchemaMigrationReport`)

```json
{
  "catalog_id": "my-catalog",
  "collection_id": "my-collection",
  "physical_table": "c_abc123",
  "schema": "s_xyz789",
  "timestamp": "20260316120000",
  "tables": [
    {"table_name": "c_abc123", "backup_name": "c_abc123_bkp_20260316120000", "row_count": 42000, "columns": ["id", "geom", "name"]},
    {"table_name": "c_abc123_h3", "backup_name": "c_abc123_h3_bkp_20260316120000", "row_count": 42000, "columns": ["id", "h3_index"]}
  ],
  "imported_rows": {"c_abc123": 42000, "c_abc123_h3": 42000},
  "status": "completed",
  "error": null,
  "dry_run": false
}
```

`status` values: `"completed"` | `"failed"` | `"dry_run"` | `"no_op"`

## Migration Pipeline

```
Step 1  — Set collection provisioning_status = "migrating" (prevents concurrent writes)
Step 2  — Export hub + sidecar tables to Parquet files in a temp directory
           Geometry columns → WKB hex (portable, no GIS library required)
Step 3  — Rename originals: {table} → {table}_bkp_{timestamp}
           Backup tables are NEVER auto-dropped
Step 4  — Recreate tables with new config via create_physical_collection_impl
Step 5  — Import from Parquet with column mapping
           New columns → DB DEFAULT value
           Removed columns → silently dropped
           Geometry → ST_GeomFromEWKB(decode(:col, 'hex'))
Step 6  — Verify row counts: imported == exported
Step 7  — Update schema_hash in collection_configs
Step 8  — Set provisioning_status = "ready"

On failure at any step:
  → Restore backup table names
  → Set provisioning_status = "migration_failed"
```

## Triggering via Admin API

```
POST /admin/schemas/{catalog_id}/{collection_id}/migrate
Content-Type: application/json

{"dry_run": false}
```

## Backup Tables

After a successful migration, backup tables are left in place:

```
{physical_table}_bkp_{timestamp}
{physical_table}_{sidecar_id}_bkp_{timestamp}
```

List them:
```
GET /admin/schemas/{catalog_id}/{collection_id}/backups
```

Drop them when no longer needed:
```
DELETE /admin/schemas/{catalog_id}/{collection_id}/backups/{timestamp}
```

## Files

| Path | Purpose |
|------|---------|
| `task.py` | `SchemaMigrationTask` + `run_schema_migration()` — 10-step pipeline |
| `models.py` | `SchemaMigrationInputs`, `SchemaMigrationReport`, `TableExportReport` |
| `exporter.py` | `export_table()`, `export_collection()`, `backup_table_names()`, `restore_backup_table_names()` |
| `importer.py` | `import_table()`, `import_collection()` — batch INSERT from Parquet |
