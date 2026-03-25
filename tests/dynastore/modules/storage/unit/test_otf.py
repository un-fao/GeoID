import pytest
from datetime import datetime, timezone

from dynastore.models.otf import (
    SchemaField,
    SchemaVersion,
    SchemaEvolution,
    SnapshotInfo,
)


class TestSnapshotInfo:
    def test_minimal(self):
        snap = SnapshotInfo(
            snapshot_id="snap-1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        assert snap.snapshot_id == "snap-1"
        assert snap.parent_snapshot_id is None
        assert snap.operation == "append"
        assert snap.summary == {}

    def test_full(self):
        snap = SnapshotInfo(
            snapshot_id="snap-2",
            parent_snapshot_id="snap-1",
            timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc),
            label="release-v1",
            operation="overwrite",
            summary={"added_rows": 1000, "deleted_rows": 50},
        )
        assert snap.parent_snapshot_id == "snap-1"
        assert snap.label == "release-v1"
        assert snap.summary["added_rows"] == 1000

    def test_serialization_roundtrip(self):
        snap = SnapshotInfo(
            snapshot_id="snap-1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            label="test",
        )
        data = snap.model_dump()
        restored = SnapshotInfo.model_validate(data)
        assert restored.snapshot_id == snap.snapshot_id
        assert restored.label == "test"


class TestSchemaField:
    def test_minimal(self):
        field = SchemaField(name="col1", type="string")
        assert field.name == "col1"
        assert field.type == "string"
        assert field.required is False
        assert field.doc is None

    def test_full(self):
        field = SchemaField(
            name="geometry", type="geometry", required=True, doc="WKB geometry"
        )
        assert field.required is True
        assert field.doc == "WKB geometry"


class TestSchemaVersion:
    def test_minimal(self):
        sv = SchemaVersion(
            schema_id="s-1",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        assert sv.schema_id == "s-1"
        assert sv.fields == []
        assert sv.parent_schema_id is None

    def test_with_fields(self):
        sv = SchemaVersion(
            schema_id="s-2",
            parent_schema_id="s-1",
            timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc),
            fields=[
                SchemaField(name="id", type="string", required=True),
                SchemaField(name="geometry", type="geometry"),
            ],
        )
        assert len(sv.fields) == 2
        assert sv.fields[0].name == "id"


class TestSchemaEvolution:
    def test_empty(self):
        ev = SchemaEvolution()
        assert ev.add_columns == []
        assert ev.rename_columns == {}
        assert ev.drop_columns == []
        assert ev.type_promotions == {}

    def test_add_columns(self):
        ev = SchemaEvolution(
            add_columns=[
                SchemaField(name="new_col", type="int64"),
            ]
        )
        assert len(ev.add_columns) == 1
        assert ev.add_columns[0].name == "new_col"

    def test_rename_columns(self):
        ev = SchemaEvolution(rename_columns={"old_name": "new_name"})
        assert ev.rename_columns == {"old_name": "new_name"}

    def test_drop_columns(self):
        ev = SchemaEvolution(drop_columns=["obsolete_col"])
        assert ev.drop_columns == ["obsolete_col"]

    def test_type_promotions(self):
        ev = SchemaEvolution(type_promotions={"count": "int64"})
        assert ev.type_promotions == {"count": "int64"}

    def test_combined_evolution(self):
        ev = SchemaEvolution(
            add_columns=[SchemaField(name="new", type="string")],
            rename_columns={"old": "renamed"},
            drop_columns=["removed"],
            type_promotions={"val": "float64"},
        )
        assert len(ev.add_columns) == 1
        assert len(ev.rename_columns) == 1
        assert len(ev.drop_columns) == 1
        assert len(ev.type_promotions) == 1

    def test_serialization_roundtrip(self):
        ev = SchemaEvolution(
            add_columns=[SchemaField(name="x", type="float64")],
            drop_columns=["y"],
        )
        data = ev.model_dump()
        restored = SchemaEvolution.model_validate(data)
        assert restored.add_columns[0].name == "x"
        assert restored.drop_columns == ["y"]
