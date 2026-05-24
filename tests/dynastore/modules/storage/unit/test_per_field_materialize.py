"""Unit tests for per-field ``materialize`` + capability-driven sidecar columns.

Covers the five-rule precedence in ``bridge_schema_to_attribute_sidecar``:

  Rule 1 — hard constraint (unique + required): COLUMN regardless of ``materialize``
  Rule 2 — ``access=FieldAccess.FAST``:  COLUMN
  Rule 3 — ``access=FieldAccess.COMPACT``: JSONB (do NOT lift), even when capabilities present
  Rule 4 — ``access=FieldAccess.AUTO``, no capabilities → JSONB (unless materialize_all)
  Rule 5 — ``access=FieldAccess.AUTO``, column-implying capability → COLUMN

The existing ``AttributeSchemaEntry`` overlay path is checked separately to
confirm the constraint-update logic remains intact when per-field materialize
is in play.
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
# Rule 3 — access=FieldAccess.COMPACT suppresses capability-driven lifting
# ---------------------------------------------------------------------------

class TestRule3MaterializeFalse:
    def test_materialize_false_no_constraint_stays_jsonb(self) -> None:
        schema = _schema({"col": _fd(access=FieldAccess.COMPACT)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged), (
            "Rule 3: access=FieldAccess.COMPACT must suppress column synthesis"
        )

    def test_materialize_false_with_filterable_still_stays_jsonb(self) -> None:
        """Explicit false beats capability-driven lifting."""
        schema = _schema({
            "col": _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.FILTERABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_with_sortable_still_stays_jsonb(self) -> None:
        schema = _schema({
            "col": _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.SORTABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_multiple_caps_still_stays_jsonb(self) -> None:
        schema = _schema({
            "col": _fd(
                access=FieldAccess.COMPACT,
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE,
                               FieldCapability.INDEXED],
            )
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_with_materialize_all_still_stays_jsonb(self) -> None:
        """Per-field False beats schema-level materialize_all."""
        schema = _schema({"col": _fd(access=FieldAccess.COMPACT)}, materialize_all=True)
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 4 — access=FieldAccess.AUTO, no column-implying capability → JSONB
# ---------------------------------------------------------------------------

class TestRule4NoneNoCapability:
    def test_plain_field_materialize_none_stays_jsonb(self) -> None:
        schema = _schema({"col": _fd(access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_none_with_groupable_stays_jsonb(self) -> None:
        """GROUPABLE alone does not imply a native column."""
        schema = _schema({
            "col": _fd(access=FieldAccess.AUTO, capabilities=[FieldCapability.GROUPABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_none_materialize_all_true_lifts(self) -> None:
        """materialize_all=True + None → schema-level opt-in lifts the field."""
        schema = _schema({"col": _fd(access=FieldAccess.AUTO)}, materialize_all=True)
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


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
    def test_mixed_precedence_rules(self) -> None:
        """Validate all rules apply correctly within a single schema pass."""
        schema = _schema({
            # Rule 1: constraint → COLUMN
            "constrained": _fd(required=True, unique=True, access=FieldAccess.COMPACT),
            # Rule 2: explicit True → COLUMN
            "forced":       _fd(access=FieldAccess.FAST),
            # Rule 3: explicit False → JSONB even with cap
            "suppressed":   _fd(access=FieldAccess.COMPACT, capabilities=[FieldCapability.FILTERABLE]),
            # Rule 4: None + no cap → JSONB
            "plain_none":   _fd(access=FieldAccess.AUTO),
            # Rule 5: None + FILTERABLE → COLUMN
            "cap_none":     _fd(access=FieldAccess.AUTO, capabilities=[FieldCapability.FILTERABLE]),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        names = _names(bridged)
        assert "constrained" in names, "Rule 1 failed"
        assert "forced"      in names, "Rule 2 failed"
        assert "suppressed" not in names, "Rule 3 failed"
        assert "plain_none" not in names, "Rule 4 failed"
        assert "cap_none"    in names, "Rule 5 failed"


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
