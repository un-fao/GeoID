"""Tests for ``extract_ogr_schema`` — derives a CollectionSchema from a vector source.

Covers the OGR-type → CollectionSchema-data_type mapping, layer
selection, the always-present geometry field, and error handling.
Mocks ``gdal.OpenEx`` + the OGR layer/field-defn objects so the tests
don't need actual GDAL files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


pytest.importorskip("osgeo")  # whole module is gated on libgdal availability

from dynastore.tasks.ingestion.schema_introspect import (
    _map_ogr_type,
    extract_ogr_schema,
)


def _mock_field_defn(name: str, ogr_type_name: str) -> MagicMock:
    fd = MagicMock()
    fd.GetName.return_value = name
    fd.GetType.return_value = 0  # value irrelevant; we patch GetFieldTypeName below
    return fd


def _mock_layer_defn(field_specs: list[tuple[str, str]]) -> MagicMock:
    """Build a mock layer-defn with ``[(name, ogr_type_name), ...]``."""
    layer_defn = MagicMock()
    field_defns = [_mock_field_defn(n, t) for n, t in field_specs]
    layer_defn.GetFieldCount.return_value = len(field_defns)
    layer_defn.GetFieldDefn.side_effect = lambda i: field_defns[i]
    return layer_defn


def _mock_layer(field_specs: list[tuple[str, str]], name: str = "roads") -> MagicMock:
    layer = MagicMock()
    layer.GetName.return_value = name
    layer.GetLayerDefn.return_value = _mock_layer_defn(field_specs)
    return layer


def _mock_dataset(layers: list[MagicMock]) -> MagicMock:
    ds = MagicMock()
    ds.GetLayerCount.return_value = len(layers)
    ds.GetLayer.side_effect = lambda i: layers[i]
    ds.GetLayerByName.side_effect = lambda n: next(
        (lyr for lyr in layers if lyr.GetName() == n), None,
    )
    return ds


# ---------------------------------------------------------------------------
# _map_ogr_type — covers every entry in the mapping table
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ogr_type_name, expected",
    [
        ("Integer",    "integer"),
        ("Integer64",  "bigint"),
        ("Real",       "float"),
        ("String",     "text"),
        ("Date",       "date"),
        ("Time",       "text"),
        ("DateTime",   "timestamp"),
        ("Binary",     "text"),
        ("IntegerList",   "jsonb"),
        ("Integer64List", "jsonb"),
        ("RealList",      "jsonb"),
        ("StringList",    "jsonb"),
    ],
)
def test_ogr_type_mapping(ogr_type_name: str, expected: str) -> None:
    fd = MagicMock()
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value=ogr_type_name,
    ):
        assert _map_ogr_type(fd) == expected


def test_unknown_ogr_type_falls_back_to_text() -> None:
    """An OGR type the table doesn't know defaults to ``text``
    (safe choice — every backend can store text)."""
    fd = MagicMock()
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="SomeNewOGRType",
    ):
        assert _map_ogr_type(fd) == "text"


# ---------------------------------------------------------------------------
# extract_ogr_schema — happy path + edge cases
# ---------------------------------------------------------------------------

def test_extract_schema_yields_geometry_first_then_fields() -> None:
    layer = _mock_layer([
        ("road_id", "String"),
        ("lanes",   "Integer"),
    ])
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        side_effect=lambda t: {0: "String"}.get(t, "Integer") if False else (
            # Field order: road_id (String), lanes (Integer)
            ["String", "Integer"][_call_count.next()]
        ),
    ):
        # Counter helper for the side_effect above
        class _Ctr:
            def __init__(self): self.i = -1
            def next(self): self.i += 1; return self.i
        _call_count = _Ctr()

        out = extract_ogr_schema("/tmp/roads.shp")

    assert list(out.keys()) == ["geometry", "road_id", "lanes"]
    assert out["geometry"].data_type == "geometry"
    assert out["road_id"].data_type == "text"
    assert out["lanes"].data_type == "integer"


def test_extract_schema_picks_first_layer_when_layer_name_none() -> None:
    layer1 = _mock_layer([("a", "String")], name="layer_a")
    layer2 = _mock_layer([("b", "Integer")], name="layer_b")
    ds = _mock_dataset([layer1, layer2])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="String",
    ):
        out = extract_ogr_schema("/tmp/multi.gpkg")

    # Picked layer_a, so only field "a" appears
    assert "a" in out
    assert "b" not in out


def test_extract_schema_selects_named_layer() -> None:
    layer1 = _mock_layer([("a", "String")], name="layer_a")
    layer2 = _mock_layer([("b", "Integer")], name="layer_b")
    ds = _mock_dataset([layer1, layer2])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="Integer",
    ):
        out = extract_ogr_schema("/tmp/multi.gpkg", layer_name="layer_b")

    assert "b" in out
    assert "a" not in out


def test_extract_schema_geometry_only_layer() -> None:
    """Layer with no fields beyond geometry still produces a schema
    (just the geometry entry)."""
    layer = _mock_layer([], name="just_geom")
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        out = extract_ogr_schema("/tmp/blank.shp")

    assert list(out.keys()) == ["geometry"]


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_open_failure_raises() -> None:
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="OGR could not open"):
            extract_ogr_schema("/tmp/nonexistent.shp")


def test_named_layer_not_found_raises() -> None:
    layer = _mock_layer([("a", "String")], name="real_layer")
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        with pytest.raises(RuntimeError, match="Layer 'missing_layer' not found"):
            extract_ogr_schema("/tmp/x.gpkg", layer_name="missing_layer")


def test_zero_layer_dataset_raises() -> None:
    ds = _mock_dataset([])
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        with pytest.raises(RuntimeError, match="has no layers"):
            extract_ogr_schema("/tmp/empty.gpkg")


# ---------------------------------------------------------------------------
# FieldDefinition shape — derived entries match the model contract
# ---------------------------------------------------------------------------

def test_derived_field_definitions_carry_descriptions() -> None:
    layer = _mock_layer([("name", "String")])
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="String",
    ):
        out = extract_ogr_schema("/tmp/x.shp")

    assert "name" in out["name"].description
    assert "OGR type: String" in out["name"].description
    # Geometry field also has a description
    assert "geometry" in out["geometry"].description.lower()
