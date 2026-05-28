# Schema Drift and Column Reconciliation

## Hard Invariant

The application **never issues `ALTER TABLE`** on an existing collection table. In-place DDL (add column, drop column, rename, type change) is forbidden at the application layer. Physical column changes happen only through a fresh (re)provision or an out-of-band schema change applied before the service restarts.

---

## How Drift Occurs

Collection physical tables are created by `ensure_storage` at provisioning time using `CREATE TABLE IF NOT EXISTS`. When an operator updates a collection's `ItemsPostgresqlDriverConfig` after the table already exists — for example, adding a new attribute field to the COLUMNAR sidecar — the DDL statement is a no-op on the already-materialised table. The new column is never added.

If the configuration were persisted as-is, the sidecar would advertise a `SELECT` of a column that does not physically exist, causing an `UndefinedColumnError` at read time.

---

## Drift is Surfaced Read-Only

`ensure_storage` reconciles the persisted driver config **down to the columns that physically exist**, never up. The reconciliation is performed by `reconcile_attribute_schema_to_columns` in `packages/core/src/dynastore/modules/storage/field_constraints.py`:

```python
def reconcile_attribute_schema_to_columns(
    entries: list[Any],
    physical_columns: set[str],
) -> tuple[list[Any], list[str]]:
    kept = [e for e in entries if e.name in physical_columns]
    dropped = [e.name for e in entries if e.name not in physical_columns]
    return kept, dropped
```

Entries whose column names are not present in `information_schema.columns` are silently dropped from the stored config. A `WARNING` is logged listing the removed names. The caller (`_reconcile_columnar_attribute_schema` in `postgresql.py`) fails open: any introspection error leaves the config untouched.

---

## Adding a Column: Fresh (Re)provision Required

The only supported path for adding a physical column to an existing collection table is to delete and recreate the collection (re-provision). `ensure_storage` will then run the full `CREATE TABLE` DDL with the updated schema.

There is no in-process `ALTER TABLE ADD COLUMN` path. Any field that the config declares but whose column is absent degrades to the read-side silent-skip + WARN path until the collection is re-provisioned.

---

## Removing or Renaming a Column

Removing or renaming a column requires:

1. Updating the collection config to remove or rename the field.
2. Re-provisioning the collection (delete + recreate), or applying the DDL change out of band (outside the application process) via a one-time SQL script.

The app does not drop or rename columns on existing tables.

---

## Summary

| Operation | Mechanism |
|-----------|-----------|
| Detect column drift | `reconcile_attribute_schema_to_columns` at `ensure_storage` time (read-only introspection of `information_schema`) |
| Add a column | Fresh (re)provision: delete collection, recreate with new config |
| Remove / rename a column | Out-of-band SQL or fresh (re)provision |
| App-side `ALTER TABLE` | **Forbidden** — violates project invariant |

---

## Key Files

| Path | Role |
|------|------|
| `packages/core/src/dynastore/modules/storage/drivers/postgresql.py` | `ensure_storage`, `_reconcile_columnar_attribute_schema` |
| `packages/core/src/dynastore/modules/storage/field_constraints.py` | `reconcile_attribute_schema_to_columns` |
| `packages/core/src/dynastore/modules/catalog/catalog_service.py` | `_build_tenant_core_ddl_batch` (tenant shell DDL at catalog provisioning) |
