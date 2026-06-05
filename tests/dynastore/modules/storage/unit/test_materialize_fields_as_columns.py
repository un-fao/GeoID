"""Tests for the strict binary storage rule in ``bridge_schema_to_attribute_sidecar``.

Strict binary rule: the attributes table is either fully COLUMNAR (every
declared non-geometry field gets its own PG column) or fully JSONB (a single
blob column).  There are no mixed tables.

  AUTOMATIC + schema → ALL fields columnar.
  AUTOMATIC + no schema → JSONB.
  COLUMNAR explicit → ALL fields columnar.
  JSONB explicit → blob (no columns).

The ``default_access`` field on ``ItemsSchema`` is preserved for use by the
driver-agnostic ``field_projection.materialize_feature_fields`` (Iceberg,
DuckDB) but has no effect on the PG bridge's binary layout decision.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dynastore.models.protocols.field_definition import FieldAccess
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


def _field(data_type: str = "string", required: bool = False, unique: bool = False, default=None):
    fd = MagicMock()
    fd.data_type = data_type
    fd.required = required
    fd.unique = unique
    fd.default = default
    fd.description = None
    # Explicit AUTO so the bridge's per-field/schema-default resolution sees a real
    # FieldAccess (a bare MagicMock attribute would never equal AUTO).
    fd.access = FieldAccess.AUTO
    return fd


def _empty_sidecar() -> FeatureAttributeSidecarConfig:
    """Build a fresh attributes-sidecar config with an empty schema."""
    return FeatureAttributeSidecarConfig()


# ---------------------------------------------------------------------------
# Default behaviour — only constrained fields are lifted
# ---------------------------------------------------------------------------

def test_default_constrained_field_forces_all_columnar() -> None:
    """Constraint present → sidecar will resolve COLUMNAR-only → every
    non-geometry field must follow (silent-drop guard, #1488 / #1491).

    The empty sidecar starts at AUTOMATIC. The constrained field promotes
    to a column, which would flip ``resolved_storage_mode`` to COLUMNAR
    (``attributes.py:144-150``) — at DDL time no JSONB blob is created,
    so any field NOT promoted by the bridge would silently drop at ingest.
    The bridge force-promotes the plain field to close that hole.
    """
    schema = MagicMock()
    schema.default_access = FieldAccess.AUTO
    schema.fields = {
        "constrained": _field(required=True),
        "plain":       _field(),
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"constrained", "plain"}


def test_automatic_with_plain_fields_all_promoted() -> None:
    """AUTOMATIC + schema (plain fields only) → all fields promoted to columns.

    Strict binary rule: schema presence is the only trigger for COLUMNAR.
    Plain (AUTO/no-caps) fields get columns just like constrained fields.
    """
    schema = MagicMock()
    schema.default_access = FieldAccess.AUTO
    schema.fields = {
        "a": _field(),
        "b": _field(),
    }
    sidecar = _empty_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# default_access=FAST — every declared field lifts
# ---------------------------------------------------------------------------

def test_materialize_all_lifts_every_field() -> None:
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {
        "road_id":  _field(data_type="string"),
        "lanes":    _field(data_type="integer"),
        "highway":  _field(data_type="string"),
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"road_id", "lanes", "highway"}


def test_materialize_all_preserves_pg_types() -> None:
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {
        "lanes":    _field(data_type="integer"),
        "name":     _field(data_type="string"),
        "active":   _field(data_type="boolean"),
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["lanes"].type == PostgresType.INTEGER
    assert by_name["name"].type == PostgresType.TEXT
    assert by_name["active"].type == PostgresType.BOOLEAN


def test_materialize_all_combines_with_constraints() -> None:
    """Constrained + plain fields ALL get lifted; constraints carry through."""
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {
        "id":       _field(required=True, unique=True),
        "label":    _field(data_type="string"),  # plain
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert set(by_name.keys()) == {"id", "label"}
    # Constrained: NOT NULL + UNIQUE
    assert by_name["id"].nullable is False
    assert by_name["id"].unique is True
    # Plain: nullable, not unique
    assert by_name["label"].nullable is True
    assert by_name["label"].unique is False


def test_materialize_all_existing_entries_kept() -> None:
    """Pre-existing AttributeSchemaEntry stays; new fields are added on top."""
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {
        "existing":  _field(data_type="string"),
        "new_field": _field(data_type="integer"),
    }
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[AttributeSchemaEntry(
            name="existing", type=PostgresType.TEXT, nullable=True, unique=False,
        )],
    )

    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"existing", "new_field"}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_materialize_all_with_empty_fields_no_op() -> None:
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {}
    sidecar = _empty_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    assert bridged is sidecar


def test_materialize_all_unknown_data_type_falls_back_to_text() -> None:
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {"weird": _field(data_type="some_alien_type")}
    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["weird"].type == PostgresType.TEXT


# ---------------------------------------------------------------------------
# Per-field default (P6) — FieldDefinition.default threads into the sidecar
# ---------------------------------------------------------------------------

def test_default_threads_into_new_entry() -> None:
    """A field's ``default`` reaches the synthesised AttributeSchemaEntry."""
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {"status": _field(data_type="string", default="active")}
    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["status"].default == "active"
    # And it produces a SQL DEFAULT literal.
    assert by_name["status"].get_sql_default() == "'active'"


def test_default_overlays_existing_entry_when_field_declares_one() -> None:
    """SSOT field default wins over an existing entry's silence."""
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {"n": _field(data_type="integer", default=7)}
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[AttributeSchemaEntry(name="n", type=PostgresType.INTEGER)]
    )
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["n"].default == 7


def test_existing_entry_default_preserved_when_field_silent() -> None:
    """A field that declares no default leaves the entry's own default intact."""
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {"n": _field(data_type="integer", default=None)}
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[
            AttributeSchemaEntry(name="n", type=PostgresType.INTEGER, default=42)
        ]
    )
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["n"].default == 42
