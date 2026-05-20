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

"""Runtime merge of ``ItemsReadPolicy.feature_type.expose`` computed values.

The geometry sidecar already projects storage-bearing computed fields
(area, perimeter, ...) into the read row via ``get_select_fields``. Until
now those values were dropped on the floor at read time. These tests pin
the behaviour that each ``expose``-named computed value is merged onto the
output ``feature.properties`` keyed by ``ComputedField.resolved_name``,
while non-exposed computed values stay hidden, and ``failure_mode`` decides
what happens when an exposed value is missing.
"""

import pytest

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    FeatureType,
    StatisticStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy

from geojson_pydantic import Feature


def _columnar_sidecar() -> GeometriesSidecar:
    """A sidecar declaring two COLUMNAR computed fields: area + perimeter."""
    cfg = GeometriesSidecarConfig(
        compute_fields_overlay=[
            ComputedField(
                kind=ComputedKind.AREA,
                storage_mode=StatisticStorageMode.COLUMNAR,
            ),
            ComputedField(
                kind=ComputedKind.PERIMETER,
                storage_mode=StatisticStorageMode.COLUMNAR,
            ),
        ]
    )
    return GeometriesSidecar(cfg)


def _ctx_with_policy(read_policy: ItemsReadPolicy) -> FeaturePipelineContext:
    ctx = FeaturePipelineContext()
    ctx["_items_read_policy"] = read_policy
    return ctx


def _blank_feature() -> Feature:
    return Feature(type="Feature", geometry=None, properties={})


class TestExposeValueMerge:
    def test_exposed_value_merged_onto_properties(self) -> None:
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": 12.5, "perimeter": 9.0}
        policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert feature.properties is not None
        assert feature.properties["area"] == 12.5

    def test_non_exposed_value_does_not_leak(self) -> None:
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": 12.5, "perimeter": 9.0}
        policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        # perimeter is in the row but NOT in expose -> must not surface.
        assert "perimeter" not in (feature.properties or {})

    def test_no_policy_merges_nothing(self) -> None:
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": 12.5, "perimeter": 9.0}
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, FeaturePipelineContext())

        assert "area" not in (feature.properties or {})
        assert "perimeter" not in (feature.properties or {})

    def test_empty_expose_merges_nothing(self) -> None:
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": 12.5}
        policy = ItemsReadPolicy(feature_type=FeatureType(expose=[]))
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert "area" not in (feature.properties or {})


class TestExposeFailureMode:
    def test_best_effort_silently_omits_missing(self) -> None:
        sidecar = _columnar_sidecar()
        # 'area' present; 'perimeter' is producible by the sidecar but its
        # column is absent from this row (e.g. partial projection).
        row = {"geoid": "g1", "area": 7.0}
        policy = ItemsReadPolicy(
            feature_type=FeatureType(
                expose=["area", "perimeter"], failure_mode="best_effort"
            )
        )
        feature = _blank_feature()

        # Must NOT raise.
        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert feature.properties["area"] == 7.0
        assert "perimeter" not in feature.properties

    def test_best_effort_ignores_unproducible_name(self) -> None:
        # A name this sidecar does not materialise (owned by another sidecar,
        # e.g. external_id) is never this sidecar's job to merge or to fail on.
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": 7.0}
        policy = ItemsReadPolicy(
            feature_type=FeatureType(
                expose=["area", "external_id"], failure_mode="strict"
            )
        )
        feature = _blank_feature()

        # Even under strict, the foreign name must not trip the geometry sidecar.
        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert feature.properties["area"] == 7.0
        assert "external_id" not in feature.properties

    def test_best_effort_omits_none_value(self) -> None:
        sidecar = _columnar_sidecar()
        # area present in the row but NULL (None) -> omit under best_effort.
        row = {"geoid": "g1", "area": None}
        policy = ItemsReadPolicy(
            feature_type=FeatureType(expose=["area"], failure_mode="best_effort")
        )
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert "area" not in (feature.properties or {})

    def test_strict_raises_on_missing(self) -> None:
        sidecar = _columnar_sidecar()
        # 'perimeter' is a producible field whose column is absent from the row.
        row = {"geoid": "g1", "area": 7.0}
        policy = ItemsReadPolicy(
            feature_type=FeatureType(
                expose=["area", "perimeter"], failure_mode="strict"
            )
        )
        feature = _blank_feature()

        with pytest.raises(Exception):
            sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

    def test_strict_raises_on_none_value(self) -> None:
        sidecar = _columnar_sidecar()
        row = {"geoid": "g1", "area": None}
        policy = ItemsReadPolicy(
            feature_type=FeatureType(expose=["area"], failure_mode="strict")
        )
        feature = _blank_feature()

        with pytest.raises(Exception):
            sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))


class TestExposeJsonbStats:
    def test_exposed_jsonb_value_merged(self) -> None:
        cfg = GeometriesSidecarConfig(
            compute_fields_overlay=[
                ComputedField(
                    kind=ComputedKind.AREA,
                    storage_mode=StatisticStorageMode.JSONB,
                ),
            ]
        )
        sidecar = GeometriesSidecar(cfg)
        # JSONB stats land in the shared geom_stats column (asyncpg -> dict).
        row = {"geoid": "g1", "geom_stats": {"area": 42.0}}
        policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
        feature = _blank_feature()

        sidecar.map_row_to_feature(row, feature, _ctx_with_policy(policy))

        assert feature.properties["area"] == 42.0
