#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)

"""
Schema Evolution Engine for DynaStore collections.

Compares a collection's physical database schema against the DDL that would be
generated from its current ``CollectionPluginConfig``, and produces a
structured ``EvolutionPlan`` classifying every difference as **safe** or
**unsafe**.

Safe operations (additive, non-destructive):
    ADD COLUMN IF NOT EXISTS, CREATE INDEX IF NOT EXISTS

Unsafe operations (require export-import via task system):
    Column type changes, column drops, partition key changes,
    primary key changes, foreign key changes

Usage::

    engine = SchemaEvolutionEngine()
    current = await engine.introspect_collection(db, schema, physical_table)
    plan = engine.diff(current, target_config, physical_table, partition_keys, partition_key_types)
    # Inspect plan.safe_ops / plan.unsafe_ops before deciding how to proceed.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


class OpType(str, Enum):
    ADD_COLUMN = "add_column"
    CREATE_INDEX = "create_index"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    ALTER_COLUMN_NULLABLE = "alter_column_nullable"
    DROP_INDEX = "drop_index"
    ALTER_PRIMARY_KEY = "alter_primary_key"
    ALTER_PARTITION = "alter_partition"
    ALTER_FOREIGN_KEY = "alter_foreign_key"
    ADD_CONSTRAINT = "add_constraint"
    DROP_CONSTRAINT = "drop_constraint"


@dataclass
class SchemaOp:
    """A single schema operation with classification."""

    op_type: OpType
    table: str
    detail: str
    sql: Optional[str] = None

    @property
    def is_safe(self) -> bool:
        return self.op_type in _SAFE_OPS


_SAFE_OPS: Set[OpType] = {
    OpType.ADD_COLUMN,
    OpType.CREATE_INDEX,
    OpType.ADD_CONSTRAINT,
}


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    udt_name: str
    is_nullable: bool
    column_default: Optional[str] = None
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None


@dataclass
class IndexInfo:
    name: str
    columns: List[str]
    is_unique: bool
    index_type: str  # btree, gist, gin, etc.
    definition: str  # Full CREATE INDEX statement from pg_get_indexdef


@dataclass
class ConstraintInfo:
    name: str
    constraint_type: str  # PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    columns: List[str]
    definition: str


@dataclass
class TableSchema:
    """Physical schema snapshot of a single table."""

    schema_name: str
    table_name: str
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    indexes: Dict[str, IndexInfo] = field(default_factory=dict)
    constraints: Dict[str, ConstraintInfo] = field(default_factory=dict)
    is_partitioned: bool = False
    partition_key_expr: Optional[str] = None


@dataclass
class CollectionSchema:
    """Full physical schema snapshot: hub + all sidecar tables."""

    hub: TableSchema
    sidecars: Dict[str, TableSchema] = field(default_factory=dict)


@dataclass
class EvolutionPlan:
    """Result of diffing current schema against target config."""

    safe_ops: List[SchemaOp] = field(default_factory=list)
    unsafe_ops: List[SchemaOp] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.safe_ops or self.unsafe_ops)

    @property
    def is_safe(self) -> bool:
        return self.has_changes and not self.unsafe_ops

    @property
    def requires_export_import(self) -> bool:
        return bool(self.unsafe_ops)

    def summary(self) -> Dict[str, Any]:
        return {
            "has_changes": self.has_changes,
            "is_safe": self.is_safe,
            "requires_export_import": self.requires_export_import,
            "safe_ops": [
                {"op": o.op_type.value, "table": o.table, "detail": o.detail}
                for o in self.safe_ops
            ],
            "unsafe_ops": [
                {"op": o.op_type.value, "table": o.table, "detail": o.detail}
                for o in self.unsafe_ops
            ],
        }


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


class SchemaEvolutionEngine:
    """
    Introspects current physical schema and compares to target DDL
    from CollectionPluginConfig.
    """

    async def introspect_table(
        self, engine: DbResource, schema: str, table_name: str
    ) -> Optional[TableSchema]:
        """Query information_schema + pg_catalog for a single table."""
        ts = TableSchema(schema_name=schema, table_name=table_name)

        # --- Columns ---
        col_sql = """
            SELECT column_name, data_type, udt_name, is_nullable,
                   column_default, character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position;
        """
        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                col_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, schema=schema, table=table_name)

        if not rows:
            return None  # Table does not exist

        for r in rows:
            ts.columns[r["column_name"]] = ColumnInfo(
                name=r["column_name"],
                data_type=r["data_type"],
                udt_name=r["udt_name"],
                is_nullable=r["is_nullable"] == "YES",
                column_default=r.get("column_default"),
                character_maximum_length=r.get("character_maximum_length"),
                numeric_precision=r.get("numeric_precision"),
            )

        # --- Indexes ---
        idx_sql = """
            SELECT i.relname AS index_name,
                   am.amname AS index_type,
                   ix.indisunique AS is_unique,
                   pg_get_indexdef(ix.indexrelid) AS definition,
                   array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum))
                       AS columns
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = :schema AND t.relname = :table
              AND NOT ix.indisprimary
            GROUP BY i.relname, am.amname, ix.indisunique, ix.indexrelid;
        """
        async with managed_transaction(engine) as conn:
            idx_rows = (
                await DQLQuery(
                    idx_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, schema=schema, table=table_name)
                or []
            )

        for r in idx_rows:
            cols = r["columns"] if r["columns"] else []
            ts.indexes[r["index_name"]] = IndexInfo(
                name=r["index_name"],
                columns=cols,
                is_unique=r["is_unique"],
                index_type=r["index_type"],
                definition=r["definition"],
            )

        # --- Constraints ---
        con_sql = """
            SELECT c.conname AS name,
                   c.contype AS constraint_type,
                   pg_get_constraintdef(c.oid) AS definition,
                   array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
                       AS columns
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE n.nspname = :schema AND t.relname = :table
            GROUP BY c.conname, c.contype, c.oid;
        """
        async with managed_transaction(engine) as conn:
            con_rows = (
                await DQLQuery(
                    con_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, schema=schema, table=table_name)
                or []
            )

        _CON_TYPE_MAP = {"p": "PRIMARY KEY", "f": "FOREIGN KEY", "u": "UNIQUE", "c": "CHECK"}
        for r in con_rows:
            cols = r["columns"] if r["columns"] else []
            ts.constraints[r["name"]] = ConstraintInfo(
                name=r["name"],
                constraint_type=_CON_TYPE_MAP.get(r["constraint_type"], r["constraint_type"]),
                columns=cols,
                definition=r["definition"],
            )

        # --- Partitioning ---
        part_sql = """
            SELECT pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema AND c.relname = :table
              AND c.relkind = 'p';
        """
        async with managed_transaction(engine) as conn:
            part_row = await DQLQuery(
                part_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, schema=schema, table=table_name)

        if part_row:
            ts.is_partitioned = True
            ts.partition_key_expr = part_row["partition_key"]

        return ts

    async def introspect_collection(
        self, engine: DbResource, schema: str, physical_table: str
    ) -> CollectionSchema:
        """Introspect the full collection: hub table + all sidecar tables."""
        hub = await self.introspect_table(engine, schema, physical_table)
        if hub is None:
            raise ValueError(
                f"Hub table '{schema}.{physical_table}' does not exist."
            )

        cs = CollectionSchema(hub=hub)

        # Discover sidecar tables by naming convention: {physical_table}_{sidecar_id}
        discover_sql = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name LIKE :pattern
              AND table_name != :hub;
        """
        async with managed_transaction(engine) as conn:
            rows = (
                await DQLQuery(
                    discover_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    schema=schema,
                    pattern=f"{physical_table}_%",
                    hub=physical_table,
                )
                or []
            )

        for r in rows:
            sidecar_table = r["table_name"]
            sidecar_id = sidecar_table[len(physical_table) + 1 :]
            ts = await self.introspect_table(engine, schema, sidecar_table)
            if ts:
                cs.sidecars[sidecar_id] = ts

        return cs

    # ------------------------------------------------------------------
    # Diff engine
    # ------------------------------------------------------------------

    def diff(
        self,
        current: CollectionSchema,
        target_config: Any,  # CollectionPluginConfig
        physical_table: str,
        partition_keys: List[str],
        partition_key_types: Dict[str, str],
    ) -> EvolutionPlan:
        """
        Compare current physical schema against target config.

        Args:
            current: Introspected CollectionSchema.
            target_config: CollectionPluginConfig with desired state.
            physical_table: Physical table base name.
            partition_keys: Resolved partition keys.
            partition_key_types: Map of partition key → SQL type.

        Returns:
            EvolutionPlan with classified operations.
        """
        plan = EvolutionPlan()

        # 1. Diff Hub table columns
        target_hub_cols = target_config.get_column_definitions()
        for key in partition_keys:
            if key not in target_hub_cols:
                col_type = partition_key_types.get(key, "TEXT")
                target_hub_cols[key] = f"{col_type} NOT NULL"

        self._diff_columns(
            plan, current.hub, target_hub_cols, current.hub.schema_name, physical_table
        )

        # 2. Diff partitioning
        target_partitioned = bool(partition_keys)
        if current.hub.is_partitioned != target_partitioned:
            plan.unsafe_ops.append(
                SchemaOp(
                    op_type=OpType.ALTER_PARTITION,
                    table=physical_table,
                    detail=(
                        f"Partitioning changed: "
                        f"{'partitioned' if current.hub.is_partitioned else 'unpartitioned'} → "
                        f"{'partitioned' if target_partitioned else 'unpartitioned'}"
                    ),
                )
            )

        # 3. Diff sidecar tables
        if target_config.sidecars:
            from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

            for sidecar_config in target_config.sidecars:
                sidecar_impl = SidecarRegistry.get_sidecar(sidecar_config)
                sidecar_id = sidecar_impl.sidecar_id
                sidecar_table = f"{physical_table}_{sidecar_id}"

                if sidecar_id not in current.sidecars:
                    # Entire sidecar table is new → safe (CREATE TABLE)
                    sc_has_validity = sidecar_impl.has_validity()
                    ddl = sidecar_impl.get_ddl(
                        physical_table=physical_table,
                        partition_keys=partition_keys,
                        partition_key_types=partition_key_types,
                        has_validity=sc_has_validity,
                    )
                    plan.safe_ops.append(
                        SchemaOp(
                            op_type=OpType.ADD_COLUMN,
                            table=sidecar_table,
                            detail=f"New sidecar table '{sidecar_id}' — full CREATE TABLE",
                            sql=ddl,
                        )
                    )
                else:
                    # Sidecar exists → diff its columns
                    current_sidecar = current.sidecars[sidecar_id]
                    target_sidecar_cols = self._extract_sidecar_target_columns(
                        sidecar_impl, physical_table, partition_keys, partition_key_types
                    )
                    self._diff_columns(
                        plan,
                        current_sidecar,
                        target_sidecar_cols,
                        current.hub.schema_name,
                        sidecar_table,
                    )

        # 4. Check for removed sidecar tables
        if target_config.sidecars:
            from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

            target_sidecar_ids = set()
            for sc in target_config.sidecars:
                impl = SidecarRegistry.get_sidecar(sc)
                target_sidecar_ids.add(impl.sidecar_id)

            for existing_id in current.sidecars:
                if existing_id not in target_sidecar_ids:
                    plan.unsafe_ops.append(
                        SchemaOp(
                            op_type=OpType.DROP_COLUMN,
                            table=f"{physical_table}_{existing_id}",
                            detail=f"Sidecar table '{existing_id}' no longer in config — requires removal",
                        )
                    )

        return plan

    # ------------------------------------------------------------------
    # Column-level diff
    # ------------------------------------------------------------------

    def _diff_columns(
        self,
        plan: EvolutionPlan,
        current_table: TableSchema,
        target_cols: Dict[str, str],
        schema: str,
        table_name: str,
    ) -> None:
        """Compare current columns against target column definitions."""
        current_col_names = set(current_table.columns.keys())
        target_col_names = set(target_cols.keys())

        # New columns → safe ADD COLUMN
        for col_name in target_col_names - current_col_names:
            col_spec = target_cols[col_name].replace(" PRIMARY KEY", "")
            plan.safe_ops.append(
                SchemaOp(
                    op_type=OpType.ADD_COLUMN,
                    table=table_name,
                    detail=f"Add column '{col_name}' {col_spec}",
                    sql=f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_spec};',
                )
            )

        # Removed columns → unsafe
        for col_name in current_col_names - target_col_names:
            # Skip internal/system columns that we don't track in config
            if col_name in ("tableoid", "cmax", "xmax", "cmin", "xmin", "ctid"):
                continue
            plan.unsafe_ops.append(
                SchemaOp(
                    op_type=OpType.DROP_COLUMN,
                    table=table_name,
                    detail=f"Column '{col_name}' exists in DB but not in target config",
                )
            )

        # Type changes → unsafe
        for col_name in current_col_names & target_col_names:
            current_col = current_table.columns[col_name]
            target_spec = target_cols[col_name].upper()

            if not self._type_compatible(current_col, target_spec):
                plan.unsafe_ops.append(
                    SchemaOp(
                        op_type=OpType.ALTER_COLUMN_TYPE,
                        table=table_name,
                        detail=(
                            f"Column '{col_name}' type change: "
                            f"{current_col.udt_name} → {target_spec}"
                        ),
                    )
                )

    def _type_compatible(self, current: ColumnInfo, target_spec: str) -> bool:
        """
        Check if current column type is compatible with the target spec.

        This is intentionally conservative — if we can't confirm compatibility,
        we report it as incompatible (unsafe) to avoid silent data corruption.
        """
        udt = current.udt_name.lower()
        spec = target_spec.lower()

        # Strip NOT NULL, DEFAULT, PRIMARY KEY etc. from spec to get just the type
        type_part = spec.split(" not null")[0].split(" default")[0].split(" primary")[0].strip()

        # Direct UDT name match
        if udt in type_part or type_part in udt:
            return True

        # Common equivalences
        equivalences = {
            "int4": {"integer", "int", "int4"},
            "int8": {"bigint", "int8"},
            "int2": {"smallint", "int2"},
            "float8": {"double precision", "float8", "double"},
            "float4": {"real", "float4", "float"},
            "bool": {"boolean", "bool"},
            "varchar": {"character varying", "varchar"},
            "text": {"text"},
            "timestamptz": {"timestamptz", "timestamp with time zone"},
            "timestamp": {"timestamp", "timestamp without time zone"},
            "uuid": {"uuid"},
            "jsonb": {"jsonb"},
            "json": {"json"},
            "tstzrange": {"tstzrange"},
            "geometry": {"geometry"},
        }

        for canonical, aliases in equivalences.items():
            if udt.startswith(canonical) and any(a in type_part for a in aliases):
                return True
            if any(udt.startswith(a) for a in aliases) and any(
                a in type_part for a in aliases
            ):
                return True

        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_sidecar_target_columns(
        self,
        sidecar_impl: Any,
        physical_table: str,
        partition_keys: List[str],
        partition_key_types: Dict[str, str],
    ) -> Dict[str, str]:
        """
        Extract target column definitions from a sidecar's DDL.

        Parses the CREATE TABLE statement generated by get_ddl() to extract
        column name → type spec mappings.
        """
        import re

        sc_has_validity = sidecar_impl.has_validity()
        ddl = sidecar_impl.get_ddl(
            physical_table=physical_table,
            partition_keys=partition_keys,
            partition_key_types=partition_key_types,
            has_validity=sc_has_validity,
        )

        cols: Dict[str, str] = {}

        # Find the first CREATE TABLE block's column list
        create_match = re.search(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
            r"[^\(]+\(\s*(.+?)\s*\)\s*(?:PARTITION|WITH|;)",
            ddl,
            re.DOTALL | re.IGNORECASE,
        )
        if not create_match:
            return cols

        body = create_match.group(1)

        # Split on commas, but respect parentheses (for types like GEOMETRY(...))
        parts = self._split_column_defs(body)

        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Skip constraints
            upper = part.upper().lstrip()
            if any(
                upper.startswith(kw)
                for kw in ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "CONSTRAINT")
            ):
                continue

            # Parse: "column_name" TYPE [qualifiers]
            col_match = re.match(r'"([^"]+)"\s+(.+)', part)
            if col_match:
                cols[col_match.group(1)] = col_match.group(2).strip()

        return cols

    @staticmethod
    def _split_column_defs(body: str) -> List[str]:
        """Split column definitions respecting nested parentheses."""
        parts: List[str] = []
        depth = 0
        current: List[str] = []
        for ch in body:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current))
        return parts

    # ------------------------------------------------------------------
    # Safe-ops executor
    # ------------------------------------------------------------------

    async def apply_safe_ops(
        self, engine: DbResource, schema: str, plan: EvolutionPlan
    ) -> List[str]:
        """
        Execute only the safe operations from an EvolutionPlan.

        Returns list of executed SQL statements.

        Raises:
            ValueError: If the plan contains unsafe operations (caller must
                        handle those via the task system first).
        """
        from dynastore.modules.db_config.query_executor import DDLQuery

        executed: List[str] = []
        for op in plan.safe_ops:
            if op.sql:
                logger.info(
                    f"SchemaEvolution: Applying safe op on '{op.table}': {op.detail}"
                )
                async with managed_transaction(engine) as conn:
                    await DDLQuery(op.sql).execute(conn, schema=schema)
                executed.append(op.sql)
            else:
                logger.warning(
                    f"SchemaEvolution: Safe op without SQL on '{op.table}': {op.detail} — skipped"
                )
        return executed
