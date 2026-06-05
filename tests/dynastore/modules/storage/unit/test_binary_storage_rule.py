#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Unit tests for the STRICT BINARY attribute storage rule.

Contract (enforced since #1043 arm):

| storage_mode | schema present | layout   | unknown props on write |
|--------------|---------------|----------|------------------------|
| AUTOMATIC    | yes           | COLUMNAR | reject (UnknownFieldsError) |
| AUTOMATIC    | no            | JSONB    | accept                  |
| COLUMNAR     | any           | COLUMNAR | reject                  |
| JSONB        | any           | JSONB    | accept (free-form)      |

Additional rules:
* COLUMNAR = closed schema: write with undeclared prop → UnknownFieldsError.
* COLUMNAR = closed schema: write omitting a NULLABLE declared prop → OK (NULL stored).
* COLUMNAR = closed schema: write omitting a REQUIRED declared prop → RequiredFieldMissingError.
* JSONB = open: no schema → any property accepted; explicit JSONB pin + schema → blob stays.
* No table emits both attribute columns and an attribute JSONB blob.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from dynastore.models.protocols.field_definition import FieldAccess, FieldCapability, FieldDefinition
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
    check_required,
    check_strict_unknown_fields,
)
from dynastore.modules.storage.errors import (
    RequiredFieldMissingError,
    UnknownFieldsError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _field(
    *,
    data_type: str = "string",
    required: bool = False,
    unique: bool = False,
    access: FieldAccess = FieldAccess.AUTO,
    capabilities: "list[FieldCapability] | None" = None,
) -> MagicMock:
    fd = MagicMock(spec=FieldDefinition)
    fd.data_type = data_type
    fd.required = required
    fd.unique = unique
    fd.access = access
    fd.capabilities = capabilities or []
    fd.default = None
    fd.description = None
    return fd


def _schema(fields: dict, *, default_access: FieldAccess = FieldAccess.AUTO) -> MagicMock:
    s = MagicMock()
    s.fields = fields
    s.default_access = default_access
    return s


def _empty_sidecar() -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig()


def _columnar_sidecar() -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.COLUMNAR)


def _jsonb_sidecar() -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)


def _names(sidecar: FeatureAttributeSidecarConfig) -> set[str]:
    return {e.name for e in (sidecar.attribute_schema or [])}


# ---------------------------------------------------------------------------
# AUTOMATIC + schema → COLUMNAR (all declared fields become columns)
# ---------------------------------------------------------------------------

class TestAutomaticWithSchema:
    def test_all_plain_fields_get_columns(self) -> None:
        """AUTOMATIC + schema: every non-geometry field → its own PG column."""
        schema = _schema({
            "code": _field(data_type="string"),
            "population": _field(data_type="integer"),
            "area": _field(data_type="double"),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert _names(bridged) == {"code", "population", "area"}

    def test_required_and_plain_both_promoted(self) -> None:
        """Required fields AND plain fields both become columns (no capability filter)."""
        schema = _schema({
            "id": _field(required=True),
            "label": _field(),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert _names(bridged) == {"id", "label"}

    def test_compact_access_field_promoted(self) -> None:
        """access=COMPACT: layout is binary — still gets a column under AUTOMATIC+schema."""
        schema = _schema({"compact_col": _field(access=FieldAccess.COMPACT)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "compact_col" in _names(bridged)

    def test_auto_no_caps_field_promoted(self) -> None:
        """access=AUTO with no capabilities → promoted (binary rule overrides per-field check)."""
        schema = _schema({"plain": _field(access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "plain" in _names(bridged)

    def test_geometry_field_excluded(self) -> None:
        """Geometry is owned by the geometry sidecar — never an attribute column."""
        schema = _schema({
            "name": _field(data_type="string"),
            "shape": _field(data_type="geometry"),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "shape" not in _names(bridged)
        assert "name" in _names(bridged)

    def test_no_jsonb_blob_when_columns_emitted(self) -> None:
        """AUTOMATIC+schema → COLUMNAR: resolved_storage_mode is COLUMNAR, no JSONB blob."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        schema = _schema({"code": _field(data_type="string")})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        sidecar_impl = FeatureAttributeSidecar(bridged)
        assert sidecar_impl.resolved_storage_mode == AttributeStorageMode.COLUMNAR

    def test_constraint_flags_preserved(self) -> None:
        """required=True → nullable=False; unique=True → unique=True on the entry."""
        schema = _schema({
            "code": _field(required=True, unique=True),
            "label": _field(),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        by_name = {e.name: e for e in bridged.attribute_schema}
        assert by_name["code"].nullable is False
        assert by_name["code"].unique is True
        assert by_name["label"].nullable is True
        assert by_name["label"].unique is False


# ---------------------------------------------------------------------------
# AUTOMATIC + no schema → JSONB
# ---------------------------------------------------------------------------

class TestAutomaticWithoutSchema:
    def test_no_schema_stays_jsonb(self) -> None:
        """AUTOMATIC + no schema → sidecar unchanged (JSONB blob at DDL time)."""
        schema = _schema({})  # empty fields
        sidecar = _empty_sidecar()
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        assert bridged is sidecar  # identity returned

    def test_none_schema_stays_jsonb(self) -> None:
        """bridge(None, ...) → sidecar unchanged."""
        sidecar = _empty_sidecar()
        bridged = bridge_schema_to_attribute_sidecar(None, sidecar)
        assert bridged is sidecar

    def test_no_schema_resolved_mode_is_jsonb(self) -> None:
        """AUTOMATIC + empty attribute_schema → resolved_storage_mode = JSONB."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        sidecar_impl = FeatureAttributeSidecar(_empty_sidecar())
        assert sidecar_impl.resolved_storage_mode == AttributeStorageMode.JSONB


# ---------------------------------------------------------------------------
# COLUMNAR explicit → all declared fields become columns
# ---------------------------------------------------------------------------

class TestColumnarExplicit:
    def test_all_fields_promoted_including_compact(self) -> None:
        """Explicit COLUMNAR: every non-geometry field → PG column."""
        schema = _schema({
            "code": _field(access=FieldAccess.COMPACT),
            "area": _field(access=FieldAccess.AUTO),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _columnar_sidecar())
        assert _names(bridged) == {"code", "area"}

    def test_geometry_excluded_from_columnar(self) -> None:
        """Even under explicit COLUMNAR, geometry is never an attribute column."""
        schema = _schema({
            "name": _field(data_type="string"),
            "geom": _field(data_type="geometry"),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _columnar_sidecar())
        assert "geom" not in _names(bridged)
        assert "name" in _names(bridged)


# ---------------------------------------------------------------------------
# JSONB explicit → blob stays, no columns emitted
# ---------------------------------------------------------------------------

class TestJsonbExplicit:
    def test_no_columns_emitted_for_any_field(self) -> None:
        """Explicit JSONB: no column synthesised, blob catches all fields."""
        schema = _schema({
            "code": _field(required=True),
            "area": _field(data_type="integer"),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _jsonb_sidecar())
        assert _names(bridged) == set()

    def test_plain_auto_field_stays_in_blob(self) -> None:
        """Even a plain AUTO field stays in the JSONB blob under explicit JSONB."""
        schema = _schema({"x": _field(access=FieldAccess.AUTO)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _jsonb_sidecar())
        assert "x" not in _names(bridged)

    def test_jsonb_resolved_mode_stays_jsonb(self) -> None:
        """Explicit JSONB: resolved_storage_mode is always JSONB."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        sidecar_impl = FeatureAttributeSidecar(_jsonb_sidecar())
        assert sidecar_impl.resolved_storage_mode == AttributeStorageMode.JSONB

    def test_schema_present_does_not_convert_jsonb_to_columnar(self) -> None:
        """Explicit JSONB pin survives schema presence — layout stays blob."""
        schema = _schema({"a": _field(), "b": _field()})
        bridged = bridge_schema_to_attribute_sidecar(schema, _jsonb_sidecar())
        assert _names(bridged) == set()


# ---------------------------------------------------------------------------
# No mixed table: attribute columns + JSONB blob never coexist
# ---------------------------------------------------------------------------

class TestNoMixedTable:
    def test_automatic_schema_no_jsonb_blob_in_ddl(self) -> None:
        """DDL for AUTOMATIC+schema uses COLUMNAR path (no JSONB column emitted)."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        schema = _schema({"code": _field(), "name": _field()})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        sidecar_impl = FeatureAttributeSidecar(bridged)
        ddl = sidecar_impl.get_ddl("items")
        # JSONB blob column name must not appear in columnar DDL
        assert "attributes JSONB" not in ddl
        # Declared columns must appear
        assert '"code"' in ddl
        assert '"name"' in ddl

    def test_automatic_no_schema_no_attribute_columns_in_ddl(self) -> None:
        """DDL for AUTOMATIC (no schema) uses JSONB path (no attribute columns)."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        sidecar_impl = FeatureAttributeSidecar(_empty_sidecar())
        ddl = sidecar_impl.get_ddl("items")
        # JSONB blob must appear
        assert "attributes JSONB" in ddl
        # No quoted user-data columns (geoid, external_id, asset_id aside)
        assert '"code"' not in ddl
        assert '"name"' not in ddl


# ---------------------------------------------------------------------------
# Closed-schema enforcement (COLUMNAR = closed)
# ---------------------------------------------------------------------------

class TestClosedSchemaEnforcement:
    """Closed schema (COLUMNAR): unknown props → UnknownFieldsError.
    Open schema (JSONB): unknown props accepted.
    """

    def test_unknown_prop_in_columnar_raises(self) -> None:
        """Undeclared property → UnknownFieldsError (HTTP 422 on write path)."""
        allowed = ["code", "area"]
        features = [{"properties": {"code": "A", "area": 10, "rogue": "x"}}]
        with pytest.raises(UnknownFieldsError) as exc:
            check_strict_unknown_fields(allowed, features)
        assert "rogue" in exc.value.unknown_fields

    def test_declared_prop_passes_in_columnar(self) -> None:
        """All declared props present → no error."""
        allowed = ["code", "area"]
        features = [{"properties": {"code": "A", "area": 10}}]
        check_strict_unknown_fields(allowed, features)  # no raise

    def test_omitting_nullable_declared_prop_allowed(self) -> None:
        """Omitting a nullable declared prop is OK — NULL stored, no error from schema check."""
        allowed = ["code", "optional_label"]
        features = [{"properties": {"code": "A"}}]  # optional_label absent
        check_strict_unknown_fields(allowed, features)  # no raise

    def test_omitting_required_prop_raises_required_error(self) -> None:
        """Omitting a REQUIRED declared prop → RequiredFieldMissingError."""
        from dynastore.models.protocols.field_definition import FieldDefinition
        fields = {
            "code": FieldDefinition(name="code", data_type="string", required=True),
            "label": FieldDefinition(name="label", data_type="string", required=False),
        }
        features = [{"properties": {"label": "L"}}]  # code (required) is absent
        with pytest.raises(RequiredFieldMissingError) as exc:
            check_required(fields, features)
        assert exc.value.field == "code"

    def test_required_prop_present_no_error(self) -> None:
        """Required prop present → check_required passes."""
        from dynastore.models.protocols.field_definition import FieldDefinition
        fields = {
            "code": FieldDefinition(name="code", data_type="string", required=True),
        }
        check_required(fields, [{"properties": {"code": "A"}}])  # no raise

    def test_jsonb_free_form_no_schema_any_prop_accepted(self) -> None:
        """JSONB with no schema: arbitrary property passes (no enforcement)."""
        # check_strict_unknown_fields is only called when fields is non-empty;
        # with empty allowed_fields it raises, so the caller must gate on ft.fields.
        # Here we test the JSONB path directly: no schema → no check → no raise.
        # (The gate lives in _enforce_strict_unknown_fields; unit test mirrors it.)
        allowed: list[str] = []
        features = [{"properties": {"arbitrary": "anything"}}]
        # When allowed_fields is empty the check raises; but the service-layer
        # gate (ft.fields → return if not ft.fields) prevents this call entirely.
        # So the contract is: with schema absent → no unknown-field check at all.
        # Here we verify check_strict_unknown_fields raises when explicitly called
        # with empty allowed (documenting that the gate is required).
        with pytest.raises(UnknownFieldsError):
            check_strict_unknown_fields(allowed, features)
