"""
Unit tests for the schema_migration task.

Covers the pure-function helpers in exporter.py and the data models,
without requiring a live database.  DB-backed integration tests (export_table,
import_table, run_schema_migration) are left for future integration coverage
once a catalog+collection fixture is established.
"""

import pytest

from dynastore.tasks.schema_migration.models import (
    SchemaMigrationInputs,
    SchemaMigrationReport,
    TableExportReport,
)
from dynastore.tasks.schema_migration.exporter import (
    _build_select,
    _GEOMETRY_UDTS,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class TestSchemaMigrationInputs:
    def test_required_fields(self):
        m = SchemaMigrationInputs(catalog_id="cat1", collection_id="col1")
        assert m.catalog_id == "cat1"
        assert m.collection_id == "col1"
        assert m.target_config is None
        assert m.dry_run is False

    def test_dry_run_flag(self):
        m = SchemaMigrationInputs(catalog_id="c", collection_id="d", dry_run=True)
        assert m.dry_run is True

    def test_target_config_dict(self):
        cfg = {"physical_schema": "s_abc", "physical_table": "items"}
        m = SchemaMigrationInputs(catalog_id="c", collection_id="d", target_config=cfg)
        assert m.target_config == cfg

    def test_missing_required_fields_raises(self):
        with pytest.raises(Exception):
            SchemaMigrationInputs(catalog_id="c")  # missing collection_id


class TestTableExportReport:
    def test_construction(self):
        r = TableExportReport(
            table_name="items",
            backup_name="items_bkp_20260101",
            row_count=42,
            file_path="/tmp/items.parquet",
            columns=["id", "geom", "value"],
        )
        assert r.row_count == 42
        assert "geom" in r.columns

    def test_zero_rows(self):
        r = TableExportReport(
            table_name="t",
            backup_name="t_bkp",
            row_count=0,
            file_path="/tmp/t.parquet",
            columns=[],
        )
        assert r.row_count == 0


class TestSchemaMigrationReport:
    def test_completed_status(self):
        r = SchemaMigrationReport(
            catalog_id="cat",
            collection_id="col",
            physical_table="items",
            schema="s_abc",
            timestamp="2026-04-10T00:00:00Z",
            status="completed",
        )
        assert r.status == "completed"
        assert r.error is None

    def test_failed_status_with_error(self):
        r = SchemaMigrationReport(
            catalog_id="cat",
            collection_id="col",
            physical_table="items",
            schema="s_abc",
            timestamp="2026-04-10T00:00:00Z",
            status="failed",
            error="backup rename failed",
        )
        assert r.status == "failed"
        assert r.error == "backup rename failed"

    def test_dry_run_status(self):
        r = SchemaMigrationReport(
            catalog_id="cat",
            collection_id="col",
            physical_table="items",
            schema="s_abc",
            timestamp="2026-04-10T00:00:00Z",
            status="dry_run",
            dry_run=True,
        )
        assert r.dry_run is True

    def test_imported_rows_defaults_empty(self):
        r = SchemaMigrationReport(
            catalog_id="c",
            collection_id="d",
            physical_table="t",
            schema="s",
            timestamp="ts",
            status="completed",
        )
        assert r.imported_rows == {}
        assert r.tables == []


# ---------------------------------------------------------------------------
# _build_select (pure function — no DB required)
# ---------------------------------------------------------------------------


class TestBuildSelect:
    def test_plain_columns(self):
        columns = [
            {"name": "id", "udt_name": "text"},
            {"name": "value", "udt_name": "int4"},
        ]
        sql, names = _build_select(columns, '"s_abc"."items"')
        assert '"id"' in sql
        assert '"value"' in sql
        assert names == ["id", "value"]
        assert "encode" not in sql  # no geometry columns

    def test_geometry_column_encoded(self):
        columns = [
            {"name": "id", "udt_name": "text"},
            {"name": "geom", "udt_name": "geometry"},
        ]
        sql, names = _build_select(columns, '"s"."t"')
        assert "encode" in sql
        assert "ST_AsEWKB" in sql
        assert "geom" in names

    def test_geography_column_encoded(self):
        columns = [{"name": "location", "udt_name": "geography"}]
        sql, names = _build_select(columns, '"s"."t"')
        assert "encode" in sql
        assert names == ["location"]

    def test_mixed_columns(self):
        columns = [
            {"name": "id", "udt_name": "uuid"},
            {"name": "geom", "udt_name": "geometry"},
            {"name": "name", "udt_name": "varchar"},
        ]
        sql, names = _build_select(columns, '"s"."t"')
        assert names == ["id", "geom", "name"]
        # geom should be encoded, others plain
        assert 'encode' in sql
        assert '"id"' in sql
        assert '"name"' in sql

    def test_empty_columns_produces_select_star_like(self):
        sql, names = _build_select([], '"s"."t"')
        assert names == []
        assert 'SELECT' in sql

    def test_case_insensitive_udt_match(self):
        """geometry UDT may arrive in uppercase from some PG versions."""
        columns = [{"name": "geom", "udt_name": "GEOMETRY"}]
        sql, names = _build_select(columns, '"s"."t"')
        assert "encode" in sql


class TestGeometryUdtSet:
    def test_geometry_in_set(self):
        assert "geometry" in _GEOMETRY_UDTS

    def test_geography_in_set(self):
        assert "geography" in _GEOMETRY_UDTS

    def test_text_not_in_set(self):
        assert "text" not in _GEOMETRY_UDTS
