"""Geometry is never synthesised as an attribute column.

Geometry is owned by the geometry sidecar / driver — it is neither a constraint
column nor an attribute column. Two layers defend this:

1. ``geometry_field_definition`` (the derived-schema producer) stamps
   ``access=COMPACT`` so the schema self-documents that geometry is not a
   column.
2. ``bridge_schema_to_attribute_sidecar`` skips any geometry-typed field before
   any column-synthesis decision, regardless of capabilities or the schema-wide
   ``default_access`` intent.

This lets ingestion use ``attributes_source_type="all"`` (no explicit
attribute_mapping) without materialising ``geometry`` as a TEXT column, which
breaks ingestion.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dynastore.models.protocols.field_definition import FieldAccess, FieldCapability
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
)
from dynastore.tasks.ingestion.schema_from_gdalinfo import geometry_field_definition


def _field(
    data_type: str = "string",
    required: bool = False,
    unique: bool = False,
    access: FieldAccess = FieldAccess.AUTO,
    capabilities=None,
):
    fd = MagicMock()
    fd.data_type = data_type
    fd.required = required
    fd.unique = unique
    fd.access = access
    fd.capabilities = capabilities or []
    fd.default = None
    fd.description = None
    return fd


def test_geometry_field_definition_is_not_materialized() -> None:
    """The derived geometry field self-documents that it is not a column."""
    assert geometry_field_definition().access == FieldAccess.COMPACT
    # Carrying a geometry-type label does not change the intent.
    assert geometry_field_definition("3D Multi Polygon").access == FieldAccess.COMPACT


def test_bridge_skips_geometry_under_materialize_all_with_caps() -> None:
    """default_access=FAST + geometry field with filterable/indexed
    capabilities → NO ``geometry`` entry in the bridged sidecar; real fields stay.
    """
    schema = MagicMock()
    schema.default_access = FieldAccess.FAST
    schema.fields = {
        "geometry": _field(
            data_type="geometry",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.INDEXED],
        ),
        "adm2_pcode": _field(data_type="string"),
        "area": _field(data_type="double"),
    }

    bridged = bridge_schema_to_attribute_sidecar(
        schema, FeatureAttributeSidecarConfig()
    )
    names = {e.name for e in (bridged.attribute_schema or [])}
    assert "geometry" not in names
    assert names == {"adm2_pcode", "area"}
