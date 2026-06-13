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

"""Tests for modules/volumes/geometry_fetcher.py."""

from __future__ import annotations

import pytest

from dynastore.modules.volumes.geometry_fetcher import (
    GeometryQuerySpec,
    build_geometry_query,
    row_to_feature_geometry,
    rows_to_geometries,
    SidecarGeometryFetcher,
)
from dynastore.models.protocols.geometry_fetcher import FeatureGeometry


# ---------------------------------------------------------------------------
# build_geometry_query
# ---------------------------------------------------------------------------


def test_build_geometry_query_basic():
    spec = GeometryQuerySpec(
        schema="tenant",
        hub_table="buildings",
        geometries_table="buildings_geometries",
        feature_ids=["f1", "f2"],
    )
    sql = build_geometry_query(spec)
    assert "ST_AsBinary" in sql
    assert "ST_Force3D" in sql
    # feature IDs must be passed as a named bind parameter, never embedded in SQL
    assert ":feature_ids" in sql
    assert "f1" not in sql
    assert "f2" not in sql
    assert '"tenant"."buildings"' in sql
    assert '"tenant"."buildings_geometries"' in sql
    # geom_column must be double-quoted consistently with other columns
    assert '"geom"' in sql


def test_build_geometry_query_with_height_column():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=["x"],
        height_column="bldg_height",
    )
    sql = build_geometry_query(spec)
    assert '"bldg_height"' in sql
    assert "COALESCE" in sql


def test_build_geometry_query_no_height_column():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=["x"],
    )
    sql = build_geometry_query(spec)
    assert "COALESCE" not in sql
    assert "0.0" in sql


def test_build_geometry_query_empty_ids_returns_false_clause():
    spec = GeometryQuerySpec(
        schema="s",
        hub_table="h",
        geometries_table="h_geometries",
        feature_ids=[],
    )
    sql = build_geometry_query(spec)
    assert "WHERE false" in sql


# ---------------------------------------------------------------------------
# row_to_feature_geometry
# ---------------------------------------------------------------------------


def test_row_to_feature_geometry_basic():
    wkb = b"\x01\x01\x00\x00\x00" + b"\x00" * 16  # fake WKB
    row = {"feature_id": "abc", "geom_wkb": wkb, "height": 5.0}
    fg = row_to_feature_geometry(row)
    assert fg is not None
    assert fg.feature_id == "abc"
    assert fg.geom_wkb == wkb
    assert fg.height == 5.0


def test_row_to_feature_geometry_memoryview():
    wkb = b"\x01" * 5
    row = {"feature_id": "x", "geom_wkb": memoryview(wkb), "height": 0}
    fg = row_to_feature_geometry(row)
    assert isinstance(fg.geom_wkb, bytes)


def test_row_to_feature_geometry_null_geom_returns_none():
    row = {"feature_id": "x", "geom_wkb": None, "height": 1.0}
    assert row_to_feature_geometry(row) is None


def test_row_to_feature_geometry_missing_height_defaults_zero():
    wkb = b"\x01" * 5
    row = {"feature_id": "y", "geom_wkb": wkb}
    fg = row_to_feature_geometry(row)
    assert fg.height == 0.0


# ---------------------------------------------------------------------------
# rows_to_geometries helper (drain + filter)
# ---------------------------------------------------------------------------


def test_rows_to_geometries_skips_null_geom():
    rows = [
        {"feature_id": "a", "geom_wkb": b"\x01" * 5, "height": 2.0},
        {"feature_id": "b", "geom_wkb": None, "height": 0.0},
    ]
    result = rows_to_geometries(rows)
    assert len(result) == 1
    assert result[0].feature_id == "a"


# ---------------------------------------------------------------------------
# SidecarGeometryFetcher (async, injected connection)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sidecar_geometry_fetcher_returns_empty_for_no_ids():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _conn():
        yield None  # never called

    fetcher = SidecarGeometryFetcher(
        connection_factory=_conn,
        schema_resolver=lambda _: "s",
        hub_table_for_collection=lambda c, col: col,
        geometries_table_for_collection=lambda c, col: f"{col}_geometries",
    )
    result = await fetcher.get_geometries("cat", "col", [])
    assert result == []


@pytest.mark.asyncio
async def test_sidecar_geometry_fetcher_queries_db():
    """SidecarGeometryFetcher routes through DQLQuery; verify the bind style."""
    import inspect
    from dynastore.modules.volumes import geometry_fetcher as _gf_mod

    src = inspect.getsource(_gf_mod.SidecarGeometryFetcher.get_geometries)
    # Named-parameter style (DQLQuery-compatible); positional $1 is gone.
    assert ":feature_ids" in src or "feature_ids=" in src
    assert "$1::text[]" not in src


# ---------------------------------------------------------------------------
# Attributes-sidecar join (#2089): true height + z_base
# ---------------------------------------------------------------------------


def test_build_geometry_query_with_attributes_join_columnar():
    spec = GeometryQuerySpec(
        schema="tenant",
        hub_table="buildings",
        geometries_table="buildings_geometries",
        feature_ids=["f1"],
        attributes_table="buildings_attributes",
        zmin_expr='a."zmin"',
        zmax_expr='a."zmax"',
        height_expr='a."height"',
    )
    sql = build_geometry_query(spec)
    assert 'LEFT JOIN "tenant"."buildings_attributes" a' in sql
    assert "z_base" in sql
    # height prefers the sidecar height attr, falling back to zmax - zmin.
    assert 'a."height"' in sql
    assert 'a."zmax" - a."zmin"' in sql
    assert 'COALESCE(a."zmin"' in sql  # z_base from zmin, geom fallback


def test_build_geometry_query_attributes_height_from_zrange_when_no_height_expr():
    spec = GeometryQuerySpec(
        schema="t",
        hub_table="h",
        geometries_table="h_geom",
        feature_ids=["f1"],
        attributes_table="h_attrs",
        zmin_expr="(a.\"attributes\"->>'zmin')::float",
        zmax_expr="(a.\"attributes\"->>'zmax')::float",
        height_expr=None,
    )
    sql = build_geometry_query(spec)
    assert "LEFT JOIN" in sql
    assert "->>'zmax')::float - (a.\"attributes\"->>'zmin')::float" in sql


def test_row_to_feature_geometry_reads_z_base():
    wkb = b"\x01" * 8
    row = {"feature_id": "z", "geom_wkb": wkb, "height": 12.0, "z_base": 30.0}
    fg = row_to_feature_geometry(row)
    assert fg is not None
    assert fg.height == 12.0
    assert fg.z_base == 30.0


def test_row_to_feature_geometry_null_z_base_is_none():
    wkb = b"\x01" * 8
    row = {"feature_id": "z", "geom_wkb": wkb, "height": 0.0, "z_base": None}
    fg = row_to_feature_geometry(row)
    assert fg is not None
    assert fg.z_base is None
