#    Copyright 2026 FAO
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
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for the strict binary storage rule in ``bridge_schema_to_attribute_sidecar``.

Strict binary layout (enforced since #1043):
  AUTOMATIC + schema present → ALL non-geometry fields become columns (COLUMNAR).
  AUTOMATIC + no schema      → JSONB (no columns).
  COLUMNAR  explicit         → ALL non-geometry fields become columns.
  JSONB     explicit         → no columns; blob catches every field.

Per-field ``access`` and capabilities (FieldAccess.FAST/COMPACT/AUTO) are
preserved on ``FieldDefinition`` for the driver-agnostic projection
(``field_projection.materialize_feature_fields`` used by Iceberg/DuckDB), but
they have NO influence on the PG sidecar bridge's binary COLUMNAR/JSONB
decision under AUTOMATIC mode — under AUTOMATIC+schema every declared
non-geometry field gets its own PG column, period.

The existing ``AttributeSchemaEntry`` overlay path is checked separately to
confirm the constraint-update logic remains intact.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import (
    FieldAccess,
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)
from dynastore.modules.storage.field_constraints import bridge_schema_to_attribute_sidecar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fd(
    *,
    data_type: str = "string",
    required: bool = False,
    unique: bool = False,
    access: FieldAccess = FieldAccess.AUTO,
    capabilities: list[FieldCapability] | None = None,
) -> FieldDefinition:
    return FieldDefinition(
        name="x",          # name is overridden by the dict key in schema.fields
        data_type=data_type,
        required=required,
        unique=unique,
        access=access,
        capabilities=capabilities or [],
    )


def _schema(fields: dict, *, materialize_all: bool = False):
    """Lightweight stand-in for ItemsSchema with only the attrs we need.

    ``materialize_all`` is the test-local shorthand for the schema-wide
    ``default_access=FAST`` intent (the successor of ``materialize_fields_as_columns``).
    """
    from unittest.mock import MagicMock
    s = MagicMock()
    s.fields = fields
    s.default_access = FieldAccess.FAST if materialize_all else FieldAccess.AUTO
    return s


def _empty_sidecar() -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig()


def _names(sidecar: FeatureAttributeSidecarConfig) -> set[str]:
    return {e.name for e in (sidecar.attribute_schema or [])}


# ---------------------------------------------------------------------------
# Rule 1 — hard constraint always wins (unique=True, required=True → COLUMN)
# ---------------------------------------------------------------------------

class TestRule1HardConstraint:
    def test_unique_and_required_lifts_even_when_materialize_false(self) -> None:
        """unique+required = hard DB constraint → must be a column."""
        schema = _schema({"col": _fd(required=True, unique=True, access=FieldAccess.COMPACT)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            "Rule 1: unique+required field must become a column even with access=FieldAccess.COMPACT"
        )

    def test_unique_required_overrides_no_capabilities(self) -> None:
        schema = _schema({"col": _fd(required=True, unique=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_required_only_lifts(self) -> None:
        """required alone is a constraint → COLUMN."""
        schema = _schema({"col": _fd(required=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_unique_only_lifts(self) -> None:
        """unique alone is a constraint → COLUMN."""
        schema = _schema({"col": _fd(unique=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 2 — access=FieldAccess.FAST forces a COLUMN (no constraint needed)
# ---------------------------------------------------------------------------

class TestRule2MaterializeTrue:
    def test_plain_field_with_materialize_true_lifts(self) -> None:
        schema = _schema({"col": _fd(access=FieldAccess.FAST)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_materialize_true_no_capabilities_still_lifts(self) -> None:
        schema = _schema({"col": _fd(access=FieldAccess.FAST, capabilities=[])})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_materialize_true_with_constraint_lifts(self) -> None:
        """Redundant but harmless: constraint + access=FieldAccess.FAST → COLUMN."""
        schema = _schema({"col": _fd(required=True, access=FieldAccess.FAST)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Strict binary: under AUTOMATIC + schema, ALL fields become columns.
# access=FieldAccess.COMPACT and access=FieldAccess.AUTO with no capabilities
# are promoted just like any other field — the binary rule supersedes per-field
# access hints for the PG sidecar bridge.
# ---------------------------------------------------------------------------

class TestStrictBinaryAllFieldsPromoted:
    """Strict binary rule: AUTOMATIC+schema → every non-geometry field gets a column."""

    def test_compact_field_promoted_under_automatic(self) -> None:
        """Under AUTOMATIC+schema, access=COMPACT still gets a PG column."""
        schema = _schema({"col": _fd(access=FieldAccess.COMPACT)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            "Strict binary: access=COMPACT must be promoted to column under AUTOMATIC+schema"
        )

    def test_compact_with_filterable_promoted(self) -> None:
        """COMPACT + capability → still a column under AUTOMATIC+schema."""
        schema = _schema({
            "col": _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.FILTERABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_plain_auto_no_caps_promoted_under_automatic(self) -> None:
        """access=AUTO, no capabilities → still promoted under AUTOMATIC+schema."""
        schema = _schema({"col": _fd(access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            "Strict binary: plain AUTO field must be promoted under AUTOMATIC+schema"
        )

    def test_groupable_only_field_promoted(self) -> None:
        """GROUPABLE alone → still a column under AUTOMATIC+schema (binary rule)."""
        schema = _schema({
            "col": _fd(access=FieldAccess.AUTO, capabilities=[FieldCapability.GROUPABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_fast_field_promoted(self) -> None:
        """access=FAST → column (unchanged behaviour + compatible with binary rule)."""
        schema = _schema({"col": _fd(access=FieldAccess.FAST)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_no_schema_stays_jsonb(self) -> None:
        """AUTOMATIC + no schema → JSONB; bridge returns sidecar unchanged."""
        from unittest.mock import MagicMock
        schema = MagicMock()
        schema.fields = {}
        sidecar = _empty_sidecar()
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        assert bridged is sidecar  # identity returned, no columns emitted

    def test_jsonb_explicit_suppresses_promotion(self) -> None:
        """Explicit JSONB pin: no column emitted even for plain AUTO fields."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeStorageMode,
        )
        jsonb_sidecar = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB
        )
        schema = _schema({"col": _fd(access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, jsonb_sidecar)
        assert "col" not in _names(bridged), (
            "Explicit JSONB: no column must be emitted; blob catches the field"
        )


# ---------------------------------------------------------------------------
# Rule 5 — access=FieldAccess.AUTO + column-implying capability → COLUMN
# (still holds under AUTOMATIC+schema, now subsumed by binary rule)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Rule 5 — access=FieldAccess.AUTO + column-implying capability → COLUMN
# ---------------------------------------------------------------------------

class TestRule5NoneWithCapability:
    @pytest.mark.parametrize("cap", [
        FieldCapability.FILTERABLE,
        FieldCapability.SORTABLE,
        FieldCapability.INDEXED,
    ])
    def test_none_column_implying_capability_lifts(self, cap: FieldCapability) -> None:
        """Each column-implying capability independently triggers synthesis."""
        schema = _schema({"col": _fd(access=FieldAccess.AUTO, capabilities=[cap])})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            f"Rule 5: capability {cap!r} should trigger column synthesis when access=FieldAccess.AUTO"
        )

    def test_none_multiple_column_caps_lifts(self) -> None:
        schema = _schema({
            "col": _fd(
                access=FieldAccess.AUTO,
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            )
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Mixed schema — multiple fields with different rules in one call
# ---------------------------------------------------------------------------

class TestMixedSchema:
    def test_mixed_access_all_promoted_under_automatic(self) -> None:
        """Strict binary: ALL non-geometry fields promoted under AUTOMATIC+schema.

        access=COMPACT, AUTO+no-cap, AUTO+FILTERABLE, FAST, and constrained
        fields all become columns — the binary rule admits no JSONB-routing
        exceptions in COLUMNAR mode.
        """
        schema = _schema({
            "constrained": _fd(required=True, unique=True, access=FieldAccess.COMPACT),
            "forced":       _fd(access=FieldAccess.FAST),
            "suppressed":   _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.FILTERABLE]),
            "plain_none":   _fd(access=FieldAccess.AUTO),
            "cap_none":     _fd(access=FieldAccess.AUTO, capabilities=[FieldCapability.FILTERABLE]),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        names = _names(bridged)
        # Every non-geometry field must be promoted under AUTOMATIC+schema.
        assert names == {"constrained", "forced", "suppressed", "plain_none", "cap_none"}


class TestSilentDropGuard:
    """Strict binary rule — AUTOMATIC+schema → all fields COLUMNAR.

    Resolution mirror at ``attributes.py:144-150``:
      * sidecar.storage_mode == COLUMNAR (explicit) → always COLUMNAR
      * sidecar.storage_mode == JSONB (explicit) → always JSONB (blob exists)
      * sidecar.storage_mode == AUTOMATIC + schema fields present → COLUMNAR
      * sidecar.storage_mode == AUTOMATIC + no schema fields → JSONB
    """

    def _columnar_sidecar(self) -> FeatureAttributeSidecarConfig:
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeStorageMode,
        )
        return FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.COLUMNAR)

    def _jsonb_sidecar(self) -> FeatureAttributeSidecarConfig:
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeStorageMode,
        )
        return FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)

    def test_regression_1488_automatic_all_fields_promoted(self) -> None:
        """The original #1488 shape: 6 required + 2 optional → all 8 get columns.

        Under the strict binary rule, AUTOMATIC+schema → all fields COLUMNAR
        (the 2 optional were already promoted by the prior silent-drop guard;
        under the new rule they are promoted unconditionally).
        """
        schema = _schema({
            "CODE":       _fd(required=True),
            "NAME":       _fd(required=True),
            "LEVEL":      _fd(required=True),
            "PARENT":     _fd(required=True),
            "LEVEL_TY":   _fd(required=True),
            "ADM0_NAME":  _fd(required=True),
            "START_DATE": _fd(),  # required=False, access=AUTO, no caps
            "END_DATE":   _fd(),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert _names(bridged) == {
            "CODE", "NAME", "LEVEL", "PARENT", "LEVEL_TY", "ADM0_NAME",
            "START_DATE", "END_DATE",
        }

    def test_columnar_explicit_promotes_every_field(self) -> None:
        """Explicit COLUMNAR sidecar → every non-geometry field gets a column."""
        schema = _schema({
            "plain":   _fd(),
            "compact": _fd(access=FieldAccess.COMPACT),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, self._columnar_sidecar())
        assert _names(bridged) == {"plain", "compact"}

    def test_automatic_with_schema_promotes_all(self) -> None:
        """AUTOMATIC + schema → all non-geometry fields become columns."""
        schema = _schema({
            "a": _fd(),
            "b": _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.FILTERABLE]),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert _names(bridged) == {"a", "b"}

    def test_jsonb_explicit_does_not_promote(self) -> None:
        """Explicit JSONB sidecar → blob always exists → no columns emitted."""
        schema = _schema({
            "constrained": _fd(required=True),
            "plain":       _fd(),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, self._jsonb_sidecar())
        assert _names(bridged) == set(), (
            "JSONB sidecar must not emit any columns; blob catches all fields"
        )

    def test_geometry_skipped_under_columnar(self) -> None:
        """COLUMNAR/AUTOMATIC must still skip geometry fields."""
        schema = _schema({
            "plain": _fd(),
            "geom":  _fd(data_type="geometry"),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        names = _names(bridged)
        assert "geom" not in names
        assert "plain" in names

    def test_existing_entry_automatic_promotes_new_fields(self) -> None:
        """Pre-existing attribute_schema entry → AUTOMATIC resolves COLUMNAR
        → new schema-declared fields also promoted.
        """
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="seeded", type=PostgresType.TEXT, nullable=True),
            ]
        )
        schema = _schema({
            "plain": _fd(),
            "extra": _fd(),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        assert _names(bridged) == {"seeded", "plain", "extra"}


# ---------------------------------------------------------------------------
# Geometry is never an attribute column — it is owned by the geometry sidecar /
# driver, so the bridge must skip it regardless of capabilities or
# ``materialize_fields_as_columns``. (Materialising it as a TEXT column breaks
# ingestion.)
# ---------------------------------------------------------------------------

class TestGeometryNeverColumn:
    def test_geometry_skipped_with_materialize_all_and_caps(self) -> None:
        """geometry-typed field + materialize_all + filterable/indexed → NOT a column."""
        schema = _schema(
            {
                "geometry": _fd(
                    data_type="geometry",
                    capabilities=[FieldCapability.FILTERABLE, FieldCapability.INDEXED],
                ),
                "name": _fd(data_type="string"),
                "lanes": _fd(data_type="integer"),
            },
            materialize_all=True,
        )
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        names = _names(bridged)
        assert "geometry" not in names, (
            "geometry must never be synthesised as an attribute column"
        )
        # The real attribute fields ARE present.
        assert "name" in names
        assert "lanes" in names

    def test_geometry_skipped_even_with_constraint(self) -> None:
        """A constraint on a geometry field still does not lift it to a column."""
        schema = _schema(
            {"geometry": _fd(data_type="geometry", required=True, unique=True)}
        )
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "geometry" not in _names(bridged)

    def test_geography_data_type_also_skipped(self) -> None:
        """Tolerant prefix match: ``geometry``-prefixed canonical types skip too."""
        schema = _schema(
            {"geom": _fd(data_type="geometry", access=FieldAccess.FAST)},
            materialize_all=True,
        )
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "geom" not in _names(bridged)

    def test_existing_geometry_entry_not_overlaid(self) -> None:
        """A pre-existing geometry entry is skipped before the overlay branch."""
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="geometry", type=PostgresType.TEXT, nullable=True)
            ]
        )
        schema = _schema({"geometry": _fd(data_type="geometry", required=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        # No geometry-driven change → identity sidecar returned unchanged.
        assert bridged is sidecar


# ---------------------------------------------------------------------------
# Overlay path — existing AttributeSchemaEntry updates remain intact
# ---------------------------------------------------------------------------

class TestExistingEntryOverlay:
    def test_existing_entry_overlay_respects_required(self) -> None:
        """Pre-existing entry gets nullable updated by fd.required."""
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="col", type=PostgresType.TEXT, nullable=True)
            ]
        )
        schema = _schema({"col": _fd(required=True, access=FieldAccess.COMPACT)})
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        by_name = {e.name: e for e in bridged.attribute_schema}
        # Overlay must set nullable=False even though access=FieldAccess.COMPACT
        assert by_name["col"].nullable is False

    def test_existing_entry_overlay_respects_unique(self) -> None:
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="col", type=PostgresType.TEXT, unique=False)
            ]
        )
        schema = _schema({"col": _fd(unique=True, access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        by_name = {e.name: e for e in bridged.attribute_schema}
        assert by_name["col"].unique is True
