from dynastore.modules.volumes.sidecar_bounds import (
    BoundsQuerySpec,
    build_bounds_query,
    row_to_feature_bounds,
    rows_to_bounds,
)


def test_query_emits_schema_qualified_join():
    spec = BoundsQuerySpec(
        schema="tenant1", hub_table="assets", geometries_table="assets_geometries",
    )
    sql = build_bounds_query(spec)
    assert '"tenant1"."assets"' in sql
    assert '"tenant1"."assets_geometries"' in sql
    assert "ST_XMin(g.geom)" in sql
    assert 'WHERE g.geom IS NOT NULL' in sql
    # Default feature id is geoid.
    assert 'h."geoid" AS feature_id' in sql
    assert "LIMIT" not in sql  # no limit by default


def test_query_honors_limit_and_custom_ids():
    spec = BoundsQuerySpec(
        schema="s", hub_table="t", geometries_table="t_g",
        feature_id_column="fid", limit=42,
    )
    sql = build_bounds_query(spec)
    assert 'h."fid"' in sql
    assert "LIMIT 42" in sql


def test_query_with_height_column_widens_z_range():
    spec = BoundsQuerySpec(
        schema="s", hub_table="t", geometries_table="t_g",
        height_column="height",
    )
    sql = build_bounds_query(spec)
    # Without height fallback: plain ST_ZMin/ST_ZMax.
    # With height fallback: wrapped in LEAST / GREATEST COALESCE.
    assert "LEAST" in sql and "GREATEST" in sql
    assert 'COALESCE(h."height", 0)' in sql


def test_row_to_feature_bounds_roundtrip():
    fb = row_to_feature_bounds({
        "feature_id": "abc", "min_x": 0, "min_y": 1, "min_z": 2,
        "max_x": 10, "max_y": 11, "max_z": 12,
    })
    assert fb.feature_id == "abc"
    assert (fb.min_x, fb.min_y, fb.min_z) == (0.0, 1.0, 2.0)
    assert (fb.max_x, fb.max_y, fb.max_z) == (10.0, 11.0, 12.0)


def test_rows_to_bounds_skips_null_rows():
    rows = [
        {"feature_id": "a", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
        {"feature_id": "b", "min_x": None, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
        {"feature_id": "c", "min_x": 2, "min_y": 2, "min_z": 2,
         "max_x": 3, "max_y": 3, "max_z": 3},
    ]
    out = rows_to_bounds(rows)
    assert [f.feature_id for f in out] == ["a", "c"]


def test_row_to_feature_bounds_stringifies_numeric_ids():
    fb = row_to_feature_bounds({
        "feature_id": 12345, "min_x": 0, "min_y": 0, "min_z": 0,
        "max_x": 1, "max_y": 1, "max_z": 1,
    })
    assert fb.feature_id == "12345"
