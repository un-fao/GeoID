"""``ItemQueryMixin._build_base_query_request`` — storage-agnostic projection.

The base query that feeds tiles/features selects property fields via the
sidecars (JSONB blob vs COLUMNAR columns) and the geometry via the geometry
sidecar's column name, instead of hardcoding ``"attributes"`` / ``"geom"``.
"""
from __future__ import annotations

from types import SimpleNamespace

from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    PostgresType,
)


def _mixin() -> ItemQueryMixin:
    return ItemQueryMixin.__new__(ItemQueryMixin)


def _fields(req) -> list:
    return [s.field for s in req.select]


def test_jsonb_collection_selects_blob_and_skips_geom_for_mvt():
    cfg = SimpleNamespace(
        sidecars=[GeometriesSidecarConfig(), FeatureAttributeSidecarConfig()]
    )
    req = _mixin()._build_base_query_request({"geom_format": "MVT"}, cfg)
    fields = _fields(req)
    assert "geoid" in fields
    assert "attributes" in fields  # JSONB blob — behavior-preserving
    assert "geom" not in fields    # MVT geometry handled by the transform


def test_jsonb_collection_selects_geom_for_non_mvt():
    cfg = SimpleNamespace(
        sidecars=[GeometriesSidecarConfig(), FeatureAttributeSidecarConfig()]
    )
    req = _mixin()._build_base_query_request({"geom_format": "WKB"}, cfg)
    fields = _fields(req)
    assert "geom" in fields
    assert "attributes" in fields


def test_columnar_collection_selects_individual_columns():
    cfg = SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                attribute_schema=[
                    AttributeSchemaEntry(name="pop_total", type=PostgresType.BIGINT),
                    AttributeSchemaEntry(name="area_km2", type=PostgresType.NUMERIC),
                ]
            ),
        ]
    )
    req = _mixin()._build_base_query_request({"geom_format": "MVT"}, cfg)
    fields = _fields(req)
    assert "pop_total" in fields
    assert "area_km2" in fields
    assert "attributes" not in fields  # no JSONB blob in COLUMNAR mode


def test_renamed_geometry_column_selected_for_non_mvt():
    cfg = SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(geom_column="the_geom"),
            FeatureAttributeSidecarConfig(),
        ]
    )
    req = _mixin()._build_base_query_request({"geom_format": "WKB"}, cfg)
    assert "the_geom" in _fields(req)


def test_cql_filter_deferred_to_transform_path_not_parsed_with_null_mapping():
    """A ``cql_filter`` in params must be carried on ``QueryRequest.cql_filter``
    so ``_apply_query_transformations`` resolves it against the real
    field_mapping (built from the collection's queryable fields). The base
    builder must NOT pre-parse it with ``field_mapping=None`` — that raises
    "field_mapping is required for SQL conversion" and, on the tiles/MVT path,
    silently drops the whole collection from the tile (empty 204). This is the
    per-``asset_id`` tile-filter regression.
    """
    cfg = SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                attribute_schema=[
                    AttributeSchemaEntry(name="CODE", type=PostgresType.TEXT),
                ],
                asset_id_field="asset_id",
                index_asset_id=True,
            ),
        ]
    )
    req = _mixin()._build_base_query_request(
        {"geom_format": "MVT", "cql_filter": "asset_id='ITAL1_01'"}, cfg
    )
    # Carried through untouched — the transform path owns CQL→SQL conversion.
    assert req.cql_filter == "asset_id='ITAL1_01'"
    # And it must NOT have been baked into raw_where here (that only happens
    # in the transform path, with the proper field_mapping).
    assert not req.raw_where
