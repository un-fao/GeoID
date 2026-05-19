#    Copyright 2025 FAO
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

"""Tests for the GeometriesStatisticsConfig -> List[ComputedField] bridge (#978).

The bridge is the single seam through which the storage-shape statistics
block joins the unified compute pipeline driven by
ItemsWritePolicy.compute. These unit pins lock the mapping rules.
"""

import math

import pytest
from shapely.geometry import Polygon, LineString, Point

from dynastore.modules.storage.computed_fields import ComputedKind
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesStatisticsConfig,
    MorphologicalIndex,
    StatisticIndexConfig,
    StatisticStorageMode,
    statistics_config_to_computed_fields,
)
from dynastore.tools.geospatial import compute_derived_fields


# ---------------------------------------------------------------------------
# Bridge: shape mapping
# ---------------------------------------------------------------------------


def test_none_config_yields_empty_field_list():
    assert statistics_config_to_computed_fields(None) == []


def test_disabled_config_yields_empty_field_list():
    cfg = GeometriesStatisticsConfig(enabled=False)
    assert statistics_config_to_computed_fields(cfg) == []


def test_all_disabled_flags_yield_empty_list_even_when_enabled_flag_set():
    cfg = GeometriesStatisticsConfig(enabled=True)
    # No sub-flag is enabled by default — explicit empty mapping.
    assert statistics_config_to_computed_fields(cfg) == []


def test_basic_metric_flags_map_one_to_one():
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        area=StatisticIndexConfig(enabled=True),
        volume=StatisticIndexConfig(enabled=True),
        length=StatisticIndexConfig(enabled=True),
        vertex_count=StatisticIndexConfig(enabled=True),
        hole_count=StatisticIndexConfig(enabled=True),
    )
    kinds = [f.kind for f in statistics_config_to_computed_fields(cfg)]
    assert kinds == [
        ComputedKind.AREA,
        ComputedKind.VOLUME,
        ComputedKind.LENGTH,
        ComputedKind.VERTEX_COUNT,
        ComputedKind.HOLE_COUNT,
    ]


def test_centroid_type_emits_centroid_kind():
    cfg = GeometriesStatisticsConfig(enabled=True, centroid_type="geometric")
    fields = statistics_config_to_computed_fields(cfg)
    assert [f.kind for f in fields] == [ComputedKind.CENTROID]


def test_morphological_indices_2d_emit_unified_kinds():
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        morphological_indices={
            MorphologicalIndex.CIRCULARITY: True,
            MorphologicalIndex.CONVEXITY: True,
            MorphologicalIndex.ASPECT_RATIO: True,
        },
    )
    kinds = {f.kind for f in statistics_config_to_computed_fields(cfg)}
    assert kinds == {
        ComputedKind.CIRCULARITY,
        ComputedKind.CONVEXITY,
        ComputedKind.ASPECT_RATIO,
    }


def test_morphological_indices_3d_only_kinds_are_dropped_silently():
    # SPHERICITY / FLATNESS are documented 3D-only and unimplemented in
    # both the legacy compute_geometry_statistics and the unified
    # compute_derived_fields runner. The bridge silently drops them so
    # operators don't get a noisy error for a config they never wrote.
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        morphological_indices={
            MorphologicalIndex.SPHERICITY: True,
            MorphologicalIndex.FLATNESS: True,
        },
    )
    assert statistics_config_to_computed_fields(cfg) == []


def test_morphological_indices_disabled_entries_skipped():
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        morphological_indices={
            MorphologicalIndex.CIRCULARITY: False,
            MorphologicalIndex.CONVEXITY: True,
        },
    )
    kinds = [f.kind for f in statistics_config_to_computed_fields(cfg)]
    assert kinds == [ComputedKind.CONVEXITY]


# ---------------------------------------------------------------------------
# End-to-end: bridge output runs through unified compute_derived_fields
# ---------------------------------------------------------------------------


def test_bridge_output_runs_through_unified_runner_for_polygon():
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        storage_mode=StatisticStorageMode.COLUMNAR,
        area=StatisticIndexConfig(enabled=True),
        length=StatisticIndexConfig(enabled=True),
        vertex_count=StatisticIndexConfig(enabled=True),
        hole_count=StatisticIndexConfig(enabled=True),
        morphological_indices={MorphologicalIndex.CIRCULARITY: True},
    )
    fields = statistics_config_to_computed_fields(cfg)

    # Square with side 2 → area=4, perimeter (length)=8.
    square = Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    out = compute_derived_fields(square, {}, fields)

    assert out["area"] == pytest.approx(4.0)
    assert out["length"] == pytest.approx(8.0)
    # Closed ring exterior has 5 coords (first==last). Pins runner behaviour.
    assert out["vertex_count"] == 5
    assert out["hole_count"] == 0
    expected_circ = (4 * math.pi * 4.0) / (8.0 ** 2)
    assert out["circularity"] == pytest.approx(expected_circ)


def test_bridge_length_on_linestring_uses_geometry_length():
    # Pins the doc-stated invariant: legacy stats["length"] was always
    # float(shapely_geom.length); the unified runner's LENGTH does the
    # same. Mapping LENGTH -> column "length" preserves both line length
    # and polygon perimeter under one column key.
    cfg = GeometriesStatisticsConfig(
        enabled=True, length=StatisticIndexConfig(enabled=True)
    )
    fields = statistics_config_to_computed_fields(cfg)
    line = LineString([(0, 0), (3, 0), (3, 4)])  # length = 7
    out = compute_derived_fields(line, {}, fields)
    assert out["length"] == pytest.approx(7.0)


def test_bridge_centroid_kind_emits_xy_list_at_runner_level():
    # The runner emits [x, y]; the PG sidecar wraps the WKB-encoding step
    # at the storage boundary (covered separately at integration level).
    cfg = GeometriesStatisticsConfig(enabled=True, centroid_type="geometric")
    fields = statistics_config_to_computed_fields(cfg)
    square = Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    out = compute_derived_fields(square, {}, fields)
    assert out["centroid"] == [pytest.approx(1.0), pytest.approx(1.0)]


def test_bridge_vertex_count_on_point():
    cfg = GeometriesStatisticsConfig(
        enabled=True, vertex_count=StatisticIndexConfig(enabled=True)
    )
    fields = statistics_config_to_computed_fields(cfg)
    out = compute_derived_fields(Point(1, 2), {}, fields)
    assert out["vertex_count"] == 1


def test_bridge_default_config_matches_default_sidecar_stats_keys():
    # The sidecar default config enables area/volume/length/centroid
    # under columnar storage mode. The bridge must preserve those keys
    # so the persisted column shapes are unchanged from pre-#978.
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        storage_mode=StatisticStorageMode.COLUMNAR,
        area=StatisticIndexConfig(enabled=True, index=True),
        volume=StatisticIndexConfig(enabled=True, index=True),
        length=StatisticIndexConfig(enabled=True, index=True),
        centroid_type="geometric",
        index_centroid=True,
    )
    fields = statistics_config_to_computed_fields(cfg)
    square = Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    out = compute_derived_fields(square, {}, fields)
    # Keys must match the historical column names exactly.
    assert set(out.keys()) == {"area", "volume", "length", "centroid"}


def test_bridge_only_emits_what_is_enabled():
    cfg = GeometriesStatisticsConfig(
        enabled=True,
        # only area enabled
        area=StatisticIndexConfig(enabled=True),
    )
    fields = statistics_config_to_computed_fields(cfg)
    assert [f.kind for f in fields] == [ComputedKind.AREA]
    assert all(f.name is None for f in fields)


def test_bridge_returns_frozen_fields_safe_to_share():
    # ComputedField is frozen=True — the bridge can safely return a
    # constant-shaped list without defensive copying.
    cfg = GeometriesStatisticsConfig(
        enabled=True, area=StatisticIndexConfig(enabled=True)
    )
    out = statistics_config_to_computed_fields(cfg)
    with pytest.raises(Exception):
        out[0].kind = ComputedKind.LENGTH  # type: ignore[misc]
