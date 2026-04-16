"""Unit tests for schema_types.py — M8.

Covers:
- 5 FieldConstraint types (construct, constraint_type, frozen)
- SchemaViolation model
- SchemaExtension Protocol structural check
- StacSchemaExtension.validate_schema
- OgcFeaturesSchemaExtension.validate_schema
- ConfigScopeMixin
- WritePolicyDefaults (no external_id_field / validity_field / geohash_precision)
- CollectionSchema with constraints field
"""
import pytest
from pydantic import ValidationError

from dynastore.modules.storage.schema_types import (
    ConfigScopeMixin,
    ContentHashConstraint,
    FieldConstraint,
    IdentityKeyConstraint,
    OgcFeaturesSchemaExtension,
    RequiredConstraint,
    SchemaExtension,
    SchemaViolation,
    StacSchemaExtension,
    UniqueConstraint,
    ValidityConstraint,
)


# ---------------------------------------------------------------------------
# FieldConstraint types
# ---------------------------------------------------------------------------


class TestRequiredConstraint:
    def test_constraint_type(self):
        assert RequiredConstraint.constraint_type == "required"

    def test_construction(self):
        c = RequiredConstraint()
        assert c.constraint_type == "required"

    def test_frozen(self):
        c = RequiredConstraint()
        with pytest.raises(Exception):
            c.constraint_type = "other"  # type: ignore


class TestUniqueConstraint:
    def test_constraint_type(self):
        assert UniqueConstraint.constraint_type == "unique"

    def test_construction(self):
        c = UniqueConstraint()
        assert c.constraint_type == "unique"


class TestIdentityKeyConstraint:
    def test_constraint_type(self):
        assert IdentityKeyConstraint.constraint_type == "identity_key"

    def test_default_geohash_precision(self):
        c = IdentityKeyConstraint()
        assert c.geohash_precision == 9

    def test_custom_geohash_precision(self):
        c = IdentityKeyConstraint(geohash_precision=6)
        assert c.geohash_precision == 6

    def test_geohash_precision_bounds(self):
        with pytest.raises(ValidationError):
            IdentityKeyConstraint(geohash_precision=0)
        with pytest.raises(ValidationError):
            IdentityKeyConstraint(geohash_precision=13)


class TestValidityConstraint:
    def test_constraint_type(self):
        assert ValidityConstraint.constraint_type == "validity"

    def test_field_required(self):
        with pytest.raises(ValidationError):
            ValidityConstraint()  # 'field' is required

    def test_with_field(self):
        c = ValidityConstraint(field="valid_time")
        assert c.field == "valid_time"


class TestContentHashConstraint:
    def test_constraint_type(self):
        assert ContentHashConstraint.constraint_type == "content_hash"

    def test_construction(self):
        c = ContentHashConstraint()
        assert c.constraint_type == "content_hash"


class TestAllConstraintTypesDistinct:
    def test_five_distinct_types(self):
        types = {
            RequiredConstraint.constraint_type,
            UniqueConstraint.constraint_type,
            IdentityKeyConstraint.constraint_type,
            ValidityConstraint.constraint_type,
            ContentHashConstraint.constraint_type,
        }
        assert len(types) == 5


# ---------------------------------------------------------------------------
# SchemaViolation
# ---------------------------------------------------------------------------


class TestSchemaViolation:
    def test_construction(self):
        v = SchemaViolation(extension="StacSchemaExtension", message="missing field")
        assert v.level == "error"
        assert v.field is None

    def test_with_field(self):
        v = SchemaViolation(extension="ext", field="geometry", message="bad type", level="warning")
        assert v.field == "geometry"
        assert v.level == "warning"


# ---------------------------------------------------------------------------
# SchemaExtension protocol
# ---------------------------------------------------------------------------


class TestSchemaExtensionProtocol:
    def test_stac_extension_satisfies_protocol(self):
        ext = StacSchemaExtension()
        assert isinstance(ext, SchemaExtension)

    def test_ogc_extension_satisfies_protocol(self):
        ext = OgcFeaturesSchemaExtension()
        assert isinstance(ext, SchemaExtension)

    def test_custom_extension_satisfies_protocol(self):
        class MyExtension:
            def validate_schema(self, schema) -> list:
                return []

        assert isinstance(MyExtension(), SchemaExtension)

    def test_missing_method_does_not_satisfy_protocol(self):
        class NotAnExtension:
            pass

        assert not isinstance(NotAnExtension(), SchemaExtension)


# ---------------------------------------------------------------------------
# StacSchemaExtension
# ---------------------------------------------------------------------------


class _MockSchema:
    """Minimal CollectionSchema-like object for testing."""
    def __init__(self, fields):
        self.fields = fields


class TestStacSchemaExtension:
    def test_no_violations_when_stac_fields_present(self):
        schema = _MockSchema({"geometry": None, "bbox": None, "assets": None})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        assert violations == []

    def test_violations_when_mandatory_fields_missing(self):
        schema = _MockSchema({})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        assert len(violations) == 3  # geometry, bbox, assets

    def test_partial_fields(self):
        schema = _MockSchema({"geometry": None})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        missing = {v.field for v in violations}
        assert "bbox" in missing
        assert "assets" in missing
        assert "geometry" not in missing

    def test_violation_level_is_warning(self):
        schema = _MockSchema({})
        ext = StacSchemaExtension()
        for v in ext.validate_schema(schema):
            assert v.level == "warning"


# ---------------------------------------------------------------------------
# OgcFeaturesSchemaExtension
# ---------------------------------------------------------------------------


class _MockFieldDef:
    def __init__(self, data_type):
        self.data_type = data_type


class TestOgcFeaturesSchemaExtension:
    def test_no_violations_when_no_geometry_fields(self):
        schema = _MockSchema({"name": _MockFieldDef("text")})
        ext = OgcFeaturesSchemaExtension()
        assert ext.validate_schema(schema) == []

    def test_no_violations_when_geometry_field_type_correct(self):
        schema = _MockSchema({"geom": _MockFieldDef("geometry")})
        ext = OgcFeaturesSchemaExtension()
        assert ext.validate_schema(schema) == []

    def test_violation_when_geometry_field_type_wrong(self):
        schema = _MockSchema({"geom": _MockFieldDef("text")})
        ext = OgcFeaturesSchemaExtension()
        violations = ext.validate_schema(schema)
        assert len(violations) == 1
        assert violations[0].field == "geom"
        assert violations[0].level == "warning"


# ---------------------------------------------------------------------------
# ConfigScopeMixin
# ---------------------------------------------------------------------------


class TestConfigScopeMixin:
    def test_default_scope(self):
        assert ConfigScopeMixin.config_scope == "platform_waterfall"

    def test_subclass_can_override_scope(self):
        class MyConfig(ConfigScopeMixin):
            config_scope = "collection_intrinsic"

        assert MyConfig.config_scope == "collection_intrinsic"

    def test_scope_literal_values(self):
        valid_scopes = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        assert ConfigScopeMixin.config_scope in valid_scopes


# ---------------------------------------------------------------------------
# WritePolicyDefaults — posture-only, no field-name references
# ---------------------------------------------------------------------------


class TestWritePolicyDefaults:
    def test_importable(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        assert WritePolicyDefaults is not None

    def test_class_key(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        assert WritePolicyDefaults.class_key() == "WritePolicyDefaults"

    def test_default_construction(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        cfg = WritePolicyDefaults()
        assert cfg is not None

    def test_no_external_id_field(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        schema = WritePolicyDefaults.model_json_schema()
        properties = schema.get("properties", {})
        assert "external_id_field" not in properties

    def test_no_validity_field(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        schema = WritePolicyDefaults.model_json_schema()
        properties = schema.get("properties", {})
        assert "validity_field" not in properties

    def test_no_geohash_precision(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        schema = WritePolicyDefaults.model_json_schema()
        properties = schema.get("properties", {})
        assert "geohash_precision" not in properties

    def test_has_on_conflict(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        schema = WritePolicyDefaults.model_json_schema()
        assert "on_conflict" in schema.get("properties", {})


# ---------------------------------------------------------------------------
# CollectionSchema with constraints field
# ---------------------------------------------------------------------------


class TestCollectionSchemaConstraints:
    def test_empty_constraints_default(self):
        from dynastore.modules.storage.driver_config import CollectionSchema
        cfg = CollectionSchema()
        assert cfg.constraints == []

    def test_constraints_field_accepts_field_constraints(self):
        from dynastore.modules.storage.driver_config import CollectionSchema
        cfg = CollectionSchema(constraints=[
            RequiredConstraint(),
            IdentityKeyConstraint(geohash_precision=7),
        ])
        assert len(cfg.constraints) == 2

    def test_class_key(self):
        from dynastore.modules.storage.driver_config import CollectionSchema
        assert CollectionSchema.class_key() == "CollectionSchema"
