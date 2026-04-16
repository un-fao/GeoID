"""
Unit tests for the Schema Evolution Engine.

Tests cover:
- EvolutionPlan properties (is_safe, requires_export_import)
- Column diff classification (safe adds, unsafe drops/type changes)
- Type compatibility checker
- Column definition parser (_split_column_defs, _extract helpers)
- Partition change detection
"""

import pytest
from dynastore.modules.catalog.schema_evolution import (
    IntrospectedSchema,
    ColumnInfo,
    ConstraintInfo,
    EvolutionPlan,
    IndexInfo,
    OpType,
    SchemaEvolutionEngine,
    SchemaOp,
    TableSchema,
)


# ---------------------------------------------------------------------------
# EvolutionPlan tests
# ---------------------------------------------------------------------------


class TestEvolutionPlan:
    def test_empty_plan(self):
        plan = EvolutionPlan()
        assert not plan.has_changes
        assert not plan.is_safe
        assert not plan.requires_export_import

    def test_safe_only(self):
        plan = EvolutionPlan(
            safe_ops=[SchemaOp(OpType.ADD_COLUMN, "t1", "add col x")]
        )
        assert plan.has_changes
        assert plan.is_safe
        assert not plan.requires_export_import

    def test_unsafe_present(self):
        plan = EvolutionPlan(
            safe_ops=[SchemaOp(OpType.ADD_COLUMN, "t1", "add col x")],
            unsafe_ops=[SchemaOp(OpType.DROP_COLUMN, "t1", "drop col y")],
        )
        assert plan.has_changes
        assert not plan.is_safe
        assert plan.requires_export_import

    def test_summary_structure(self):
        plan = EvolutionPlan(
            safe_ops=[SchemaOp(OpType.ADD_COLUMN, "t1", "add x")],
            unsafe_ops=[SchemaOp(OpType.ALTER_COLUMN_TYPE, "t1", "change y")],
        )
        s = plan.summary()
        assert s["has_changes"] is True
        assert s["is_safe"] is False
        assert len(s["safe_ops"]) == 1
        assert len(s["unsafe_ops"]) == 1
        assert s["safe_ops"][0]["op"] == "add_column"


# ---------------------------------------------------------------------------
# SchemaOp classification
# ---------------------------------------------------------------------------


class TestSchemaOpClassification:
    def test_safe_ops(self):
        for op_type in (OpType.ADD_COLUMN, OpType.CREATE_INDEX, OpType.ADD_CONSTRAINT):
            op = SchemaOp(op_type, "t", "detail")
            assert op.is_safe

    def test_unsafe_ops(self):
        for op_type in (
            OpType.DROP_COLUMN,
            OpType.ALTER_COLUMN_TYPE,
            OpType.ALTER_PARTITION,
            OpType.ALTER_PRIMARY_KEY,
            OpType.ALTER_FOREIGN_KEY,
            OpType.DROP_INDEX,
            OpType.DROP_CONSTRAINT,
        ):
            op = SchemaOp(op_type, "t", "detail")
            assert not op.is_safe


# ---------------------------------------------------------------------------
# Type compatibility
# ---------------------------------------------------------------------------


class TestTypeCompatibility:
    def setup_method(self):
        self.engine = SchemaEvolutionEngine()

    def _col(self, udt_name: str) -> ColumnInfo:
        return ColumnInfo(
            name="test", data_type="USER-DEFINED", udt_name=udt_name, is_nullable=True
        )

    def test_exact_match(self):
        assert self.engine._type_compatible(self._col("uuid"), "UUID NOT NULL")

    def test_varchar_match(self):
        assert self.engine._type_compatible(self._col("varchar"), "VARCHAR(255)")

    def test_int4_integer(self):
        assert self.engine._type_compatible(self._col("int4"), "INTEGER NOT NULL")

    def test_float8_double(self):
        assert self.engine._type_compatible(self._col("float8"), "DOUBLE PRECISION")

    def test_timestamptz(self):
        assert self.engine._type_compatible(
            self._col("timestamptz"), "TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        )

    def test_geometry(self):
        assert self.engine._type_compatible(
            self._col("geometry"), "GEOMETRY(GEOMETRYZ, 4326) NOT NULL"
        )

    def test_jsonb(self):
        assert self.engine._type_compatible(self._col("jsonb"), "JSONB")

    def test_tstzrange(self):
        assert self.engine._type_compatible(self._col("tstzrange"), "TSTZRANGE NOT NULL")

    def test_incompatible(self):
        assert not self.engine._type_compatible(self._col("int4"), "TEXT")

    def test_bool(self):
        assert self.engine._type_compatible(self._col("bool"), "BOOLEAN DEFAULT FALSE")


# ---------------------------------------------------------------------------
# Column diff
# ---------------------------------------------------------------------------


class TestColumnDiff:
    def setup_method(self):
        self.engine = SchemaEvolutionEngine()

    def _table(self, columns: dict) -> TableSchema:
        cols = {}
        for name, udt in columns.items():
            cols[name] = ColumnInfo(
                name=name, data_type="USER-DEFINED", udt_name=udt, is_nullable=True
            )
        return TableSchema(schema_name="test_schema", table_name="test_table", columns=cols)

    def test_no_changes(self):
        current = self._table({"geoid": "uuid", "name": "varchar"})
        target = {"geoid": "UUID NOT NULL", "name": "VARCHAR(255)"}
        plan = EvolutionPlan()
        self.engine._diff_columns(plan, current, target, "test_schema", "test_table")
        assert not plan.has_changes

    def test_add_column(self):
        current = self._table({"geoid": "uuid"})
        target = {"geoid": "UUID NOT NULL", "new_col": "TEXT"}
        plan = EvolutionPlan()
        self.engine._diff_columns(plan, current, target, "test_schema", "test_table")
        assert len(plan.safe_ops) == 1
        assert plan.safe_ops[0].op_type == OpType.ADD_COLUMN
        assert "new_col" in plan.safe_ops[0].detail
        assert plan.safe_ops[0].sql is not None

    def test_drop_column(self):
        current = self._table({"geoid": "uuid", "old_col": "text"})
        target = {"geoid": "UUID NOT NULL"}
        plan = EvolutionPlan()
        self.engine._diff_columns(plan, current, target, "test_schema", "test_table")
        assert len(plan.unsafe_ops) == 1
        assert plan.unsafe_ops[0].op_type == OpType.DROP_COLUMN
        assert "old_col" in plan.unsafe_ops[0].detail

    def test_type_change(self):
        current = self._table({"geoid": "uuid", "val": "int4"})
        target = {"geoid": "UUID NOT NULL", "val": "TEXT"}
        plan = EvolutionPlan()
        self.engine._diff_columns(plan, current, target, "test_schema", "test_table")
        assert len(plan.unsafe_ops) == 1
        assert plan.unsafe_ops[0].op_type == OpType.ALTER_COLUMN_TYPE

    def test_mixed_changes(self):
        current = self._table({"geoid": "uuid", "old": "text"})
        target = {"geoid": "UUID NOT NULL", "new": "INTEGER"}
        plan = EvolutionPlan()
        self.engine._diff_columns(plan, current, target, "test_schema", "test_table")
        assert len(plan.safe_ops) == 1  # add "new"
        assert len(plan.unsafe_ops) == 1  # drop "old"


# ---------------------------------------------------------------------------
# Split column defs
# ---------------------------------------------------------------------------


class TestSplitColumnDefs:
    def test_simple(self):
        body = '"geoid" UUID NOT NULL, "name" VARCHAR(255)'
        parts = SchemaEvolutionEngine._split_column_defs(body)
        assert len(parts) == 2

    def test_nested_parens(self):
        body = '"geom" GEOMETRY(GEOMETRYZ, 4326) NOT NULL, "id" UUID'
        parts = SchemaEvolutionEngine._split_column_defs(body)
        assert len(parts) == 2
        assert "GEOMETRY(GEOMETRYZ, 4326)" in parts[0]

    def test_with_constraint(self):
        body = '"geoid" UUID, PRIMARY KEY ("geoid")'
        parts = SchemaEvolutionEngine._split_column_defs(body)
        assert len(parts) == 2


# ---------------------------------------------------------------------------
# Partition change detection
# ---------------------------------------------------------------------------


class TestPartitionDiff:
    def setup_method(self):
        self.engine = SchemaEvolutionEngine()

    def test_partition_added(self):
        hub = TableSchema(
            schema_name="s", table_name="t",
            columns={"geoid": ColumnInfo("geoid", "uuid", "uuid", False)},
            is_partitioned=False,
        )
        current = IntrospectedSchema(hub=hub)

        class FakeConfig:
            sidecars = []
            partitioning = type("P", (), {"enabled": True, "partition_keys": ["h3"]})()
            def get_column_definitions(self):
                return {"geoid": "UUID NOT NULL"}

        plan = self.engine.diff(current, FakeConfig(), "t", ["h3"], {"h3": "BIGINT"})
        partition_ops = [o for o in plan.unsafe_ops if o.op_type == OpType.ALTER_PARTITION]
        assert len(partition_ops) == 1

    def test_no_partition_change(self):
        hub = TableSchema(
            schema_name="s", table_name="t",
            columns={"geoid": ColumnInfo("geoid", "uuid", "uuid", False)},
            is_partitioned=True,
            partition_key_expr="LIST (h3)",
        )
        current = IntrospectedSchema(hub=hub)

        class FakeConfig:
            sidecars = []
            partitioning = type("P", (), {"enabled": True, "partition_keys": ["h3"]})()
            def get_column_definitions(self):
                return {"geoid": "UUID NOT NULL"}

        plan = self.engine.diff(current, FakeConfig(), "t", ["h3"], {"h3": "BIGINT"})
        partition_ops = [o for o in plan.unsafe_ops if o.op_type == OpType.ALTER_PARTITION]
        assert len(partition_ops) == 0
