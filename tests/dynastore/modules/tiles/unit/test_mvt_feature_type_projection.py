"""MVT tile path must respect ``ItemsReadPolicy.feature_type``.

Bug-driven regression suite: the live tile path emitted per-feature
properties that bypassed the collection's read-shape contract — raw
geometry WKB hex (``_geom_source``), lowercase JSONB aliases, ``type``,
``id``/``geoid`` — regardless of ``feature_type.expose`` /
``expose_geoid``. ``ST_AsMVT(mvtgeom.*, …, 'geom')`` emits every column of
the inner SELECT as a tile property, so any projection has to happen at
SELECT build time (Python row-mapping doesn't run on the tile path).

These tests pin two layers:

  1. ``project_select_for_feature_type`` — pure helper, the SSOT shared
     with ``/items`` (both paths read the same ``feature_type``).
  2. The full MVT transform pipeline — empty/non-empty ``expose`` and
     ``expose_geoid`` map to the right SELECT/SQL wrap shape.
"""
from __future__ import annotations

from types import SimpleNamespace

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.storage.computed_fields import FeatureType
from dynastore.modules.storage.read_policy import (
    project_select_for_feature_type,
)
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.tiles.query_transform import MVTQueryTransform


def _schema(*names_and_types: tuple[str, str]) -> dict[str, FieldDefinition]:
    """Build an ``ItemsSchema.fields``-shaped dict for the helper."""
    return {
        name: FieldDefinition(name=name, data_type=data_type)
        for name, data_type in names_and_types
    }


def _col_config() -> SimpleNamespace:
    return SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(geom_column="geom"),
            FeatureAttributeSidecarConfig(),
        ]
    )


def _mvt_context(feature_type: FeatureType | None = None) -> dict:
    return {
        "geom_format": "MVT",
        "target_srid": 3857,
        "srid": 4326,
        "tile_wkb": b"\x00" * 8,
        "extent": 4096,
        "buffer": 256,
        "col_config": _col_config(),
        "feature_type": feature_type,
    }


# ---------------------------------------------------------------------------
# project_select_for_feature_type — pure unit
# ---------------------------------------------------------------------------


def test_default_none_expose_with_no_schema_yields_no_selects():
    """``expose=None`` (the default) with no declared schema → no selects.

    Trinary semantics: ``None`` means "surface all declared schema
    fields". When the caller supplies no ``declared_fields`` (and the
    other expose flags are off), the helper emits an empty SELECT list —
    the tile carries geometry only.
    """
    ft = FeatureType()
    assert ft.expose is None
    assert ft.expose_geoid is False
    assert ft.expose_created is False

    selects = project_select_for_feature_type(ft)
    assert selects == []


def test_default_none_expose_surfaces_declared_schema_fields():
    """``expose=None`` + declared schema → one select per schema field.

    The wire shape mirrors ``ItemsSchema.fields``: the default surfaces
    every declared property. Computed/derived fields are NOT added
    (they require an explicit non-empty ``expose`` list).
    """
    ft = FeatureType()
    selects = project_select_for_feature_type(
        ft, _schema(("pop_total", "integer"), ("area_km2", "double"))
    )
    assert [s.field for s in selects] == ["pop_total", "area_km2"]
    assert all(s.alias is None for s in selects)


def test_default_none_expose_skips_geometry_typed_field():
    """``data_type=geometry`` is owned by the geometry sidecar — never an
    attribute property. The helper drops it before building the SELECT
    list, regardless of ``expose``, so the read path never asks for
    ``attributes->>'geometry'`` (which would JOIN to a missing JSONB key
    and on the MVT path render an empty tile).

    Mirrors the write-side SSOTs in
    :mod:`dynastore.modules.storage.field_constraints` —
    ``schema_field_materializes_as_column`` returns ``False`` for
    geometry-typed fields and ``bridge_schema_to_attribute_sidecar``
    skips them, so the JSONB blob never carries the geometry key.
    """
    ft = FeatureType()
    selects = project_select_for_feature_type(
        ft,
        _schema(
            ("geometry", "geometry"),
            ("CODE", "string"),
            ("NAME", "string"),
        ),
    )
    assert [s.field for s in selects] == ["CODE", "NAME"]


def test_default_none_expose_skips_expose_false_field():
    """``expose=False`` on a ``FieldDefinition`` opts the field out of the
    wire shape — the schema still validates writes against it, but reads
    never surface it.
    """
    ft = FeatureType()
    fields = {
        "CODE": FieldDefinition(name="CODE", data_type="string"),
        "secret": FieldDefinition(name="secret", data_type="string", expose=False),
    }
    selects = project_select_for_feature_type(ft, fields)
    assert [s.field for s in selects] == ["CODE"]


def test_explicit_empty_expose_suppresses_schema_baseline():
    """``expose=[]`` → no schema, no computed — geometry-only tile.

    Even when the caller supplies declared schema, an explicit empty
    list overrides the schema baseline. This is the only way to ship a
    geometry-only tile when the collection has declared properties.
    """
    ft = FeatureType(expose=[])
    selects = project_select_for_feature_type(
        ft, _schema(("pop_total", "integer"), ("area_km2", "double"))
    )
    assert selects == []


def test_non_empty_expose_is_additive_to_declared_schema():
    """``expose=["computed_x"]`` → schema fields PLUS the listed computed.

    Listing computed fields does NOT replace the schema baseline; it
    adds to it. Use ``[]`` to suppress the baseline. Geometry-typed and
    ``expose=False`` schema entries are still filtered out (the additive
    list does not bring them back).
    """
    ft = FeatureType(expose=["area_m2"])
    selects = project_select_for_feature_type(
        ft,
        _schema(
            ("geometry", "geometry"),
            ("pop_total", "integer"),
        ),
    )
    assert [s.field for s in selects] == ["pop_total", "area_m2"]


def test_expose_list_yields_one_select_per_name_no_aliasing():
    """``expose=["CODE","NAME"]`` with no schema → exactly those two selects.

    No lowercase aliasing, no schema-name normalisation — the helper passes
    the names through verbatim so JSONB extraction (case-sensitive in
    Postgres) finds the original keys.
    """
    ft = FeatureType(expose=["CODE", "NAME"])
    selects = project_select_for_feature_type(ft)
    assert [s.field for s in selects] == ["CODE", "NAME"]
    assert [s.alias for s in selects] == [None, None]


def test_expose_geoid_adds_geoid_select():
    ft = FeatureType(expose_geoid=True)
    selects = project_select_for_feature_type(ft)
    assert [s.field for s in selects] == ["geoid"]


def test_expose_created_aliases_transaction_time():
    ft = FeatureType(expose_created=True)
    selects = project_select_for_feature_type(ft)
    assert len(selects) == 1
    assert selects[0].field == "transaction_time"
    assert selects[0].alias == "created"


def test_combined_flags_compose_select_order():
    ft = FeatureType(
        expose=["CODE"],
        expose_geoid=True,
        expose_created=True,
    )
    selects = project_select_for_feature_type(ft, _schema(("NAME", "string")))
    # geoid first, then created, then the schema baseline, then the
    # additive expose entries.
    assert [s.field for s in selects] == [
        "geoid",
        "transaction_time",
        "NAME",
        "CODE",
    ]


# ---------------------------------------------------------------------------
# MVTQueryTransform.post_process_sql — column-restricting wrap
# ---------------------------------------------------------------------------


def test_post_process_sql_no_op_when_feature_type_absent():
    """Without a feature_type in context the wrap is skipped — keeps the
    legacy (non-policy-aware) callers untouched.
    """
    sql_in = "SELECT h.geoid, geom FROM table"
    sql_out, params_out = MVTQueryTransform().post_process_sql(
        sql_in, {}, _mvt_context(feature_type=None)
    )
    assert sql_out == sql_in
    assert params_out == {}


def test_post_process_sql_default_with_schema_fields_keeps_them():
    """``expose=None`` (default) + ``context['schema_fields']`` (the
    ``ItemsSchema.fields`` mapping) → schema fields in tile.

    The wrap surfaces every declared schema field so the tile's wire
    shape mirrors the write schema. ``geoid`` and the raw JSONB blob are
    still projected away (they are not declared properties).
    """
    ft = FeatureType()  # expose=None
    ctx = _mvt_context(ft)
    ctx["schema_fields"] = _schema(("CODE", "string"), ("NAME", "string"))
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes->>'CODE' AS CODE, "
        "sc_attributes.attributes->>'NAME' AS NAME, ST_AsMVTGeom(...) AS geom FROM ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, ctx)
    assert sql_out.startswith('SELECT "geom", "CODE", "NAME" FROM (')
    assert '"geoid"' not in sql_out
    assert '"attributes"' not in sql_out


def test_post_process_sql_drops_geometry_typed_schema_field():
    """A ``geometry``-typed entry in ``ItemsSchema.fields`` (gdalinfo-derived
    schemas always declare one — :func:`...schema_from_gdalinfo.derive_schema_from_gdalinfo`)
    must NOT surface as a tile property. The geometry comes from the
    geometry sidecar via ``ST_AsMVTGeom(...) AS geom``; selecting
    ``attributes->>'geometry'`` would JOIN to a missing JSONB key and
    the tile would render empty (live regression on
    datamgr03/region6/ITAL1_01 post-trinary-expose).
    """
    ft = FeatureType()
    ctx = _mvt_context(ft)
    ctx["schema_fields"] = _schema(
        ("geometry", "geometry"),
        ("CODE", "string"),
        ("NAME", "string"),
    )
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes->>'CODE' AS CODE, "
        "sc_attributes.attributes->>'NAME' AS NAME, ST_AsMVTGeom(...) AS geom FROM ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, ctx)
    assert sql_out.startswith('SELECT "geom", "CODE", "NAME" FROM (')
    # No second "geometry" column would-be JSONB lookup.
    assert '"geometry"' not in sql_out


def test_post_process_sql_explicit_empty_expose_keeps_only_geom():
    """``expose=[]`` → outer SELECT projects ``geom`` only, even when the
    collection has declared schema fields.

    Explicit suppression: schema baseline is dropped, no `_geom_source`
    leak, no `geoid`, no `type`, no JSONB blob.
    """
    ft = FeatureType(expose=[])
    ctx = _mvt_context(ft)
    ctx["schema_fields"] = _schema(  # ignored when expose=[]
        ("CODE", "string"), ("NAME", "string")
    )
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes, ST_AsMVTGeom(...) AS geom "
        "FROM hub h LEFT JOIN sc_attributes ON ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, ctx)
    assert sql_out.startswith('SELECT "geom" FROM (')
    assert "_mvt_inner" in sql_out
    assert '"geoid"' not in sql_out
    assert '"CODE"' not in sql_out
    assert '"attributes"' not in sql_out


def test_post_process_sql_expose_list_is_additive_to_schema_baseline():
    """JSONB keys are case-sensitive in Postgres; the wrap must quote them
    so ``CODE``/``NAME`` survive the planner's lowercase folding.

    A non-empty ``expose`` is additive to the schema baseline: declared
    schema fields come first, then the listed computed/derived fields.
    """
    ft = FeatureType(expose=["area_m2"])
    ctx = _mvt_context(ft)
    ctx["schema_fields"] = _schema(("CODE", "string"), ("NAME", "string"))
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes->>'CODE' AS CODE, "
        "sc_attributes.attributes->>'NAME' AS NAME, "
        "sc_stats.area_m2 AS area_m2, ST_AsMVTGeom(...) AS geom FROM ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, ctx)
    # geom + schema baseline + listed computed, in that order.
    assert sql_out.startswith(
        'SELECT "geom", "CODE", "NAME", "area_m2" FROM ('
    )
    # No lowercase alias leak.
    assert '"code"' not in sql_out
    assert '"name"' not in sql_out
    # geoid projected away.
    assert '"geoid"' not in sql_out


def test_post_process_sql_expose_geoid_surfaces_geoid_column():
    ft = FeatureType(expose_geoid=True)
    sql_in = "SELECT h.geoid, ST_AsMVTGeom(...) AS geom FROM ..."
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, _mvt_context(ft))
    assert sql_out.startswith('SELECT "geom", "geoid" FROM (')


# ---------------------------------------------------------------------------
# Full MVTQueryTransform.transform_query — verifies the placeholder removal
# ---------------------------------------------------------------------------


def test_transform_query_no_geom_source_leak_in_select():
    """The transform must not insert a ``_geom_source`` placeholder.

    The QueryOptimizer joins the geometry sidecar unconditionally via
    ``require_geometry=True``; a placeholder selection of the raw geometry
    column would surface WKB hex as an MVT property (the symptom on the live
    review tile path).
    """
    req = MVTQueryTransform().transform_query(
        QueryRequest(select=[FieldSelection(field="geoid", alias="id")]),
        _mvt_context(),
    )
    # No FieldSelection refers to the raw geometry column or its alias.
    fields = {s.field for s in req.select}
    aliases = {s.alias for s in req.select if s.alias}
    assert "_geom_source" not in aliases
    assert "geom" not in fields  # the raw geom field name
    # The MVT geometry comes from raw_selects as the named ``geom`` column.
    raw = " ".join(req.raw_selects)
    assert raw.rstrip().endswith("AS geom")
