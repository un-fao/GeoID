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

"""Guard: the validity-token fallback in build_optimized_query must be
'(,)'::tstzrange, not NULL::tstzrange.

Root cause (production bug): when a collection has no validity column
(ItemsWritePolicy.enable_validity=False, the post-#974 default), the
optimizer rewrites bare ``validity`` tokens in raw_where with a SQL-safe
placeholder. The previous placeholder was ``NULL::tstzrange``.

Under three-valued logic ``NULL && tstzrange(...)`` and ``NULL @> :dt``
both evaluate to NULL, so every row is silently dropped — a datetime-
filtered PG-only STAC /search returns nothing.

A collection without a validity column is "always valid", so the
all-of-time range ``'(,)'::tstzrange`` is the correct sentinel.  It
matches every temporal predicate and preserves the full result.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    TargetDimension,
)
from dynastore.models.query_builder import FieldSelection, QueryRequest


def _make_no_validity_config() -> ItemsPostgresqlDriverConfig:
    """Return a driver config whose attributes sidecar has no validity column.

    ``validity_column=None`` means ``enable_validity`` is False — the
    post-#974 default for new collections.
    """
    return ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(
                sidecar_type="geometries",
                target_srid=4326,
                target_dimension=TargetDimension.FORCE_2D,
                partition_strategy=None,
                partition_resolution=0,
                statistics=None,
            ),
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                storage_mode=AttributeStorageMode.JSONB,
                index_external_id=False,
                index_asset_id=False,
                validity_column=None,   # no validity — the bug trigger
                attribute_schema=None,
                jsonb_column_name="attributes",
                use_hot_updates=False,
                partition_strategy=None,
                partition_attribute=None,
            ),
        ],
    )


def test_validity_token_replaced_with_all_of_time_range_not_null():
    """When no sidecar exposes a validity column, a bare ``validity`` token in
    raw_where must be rewritten to ``'(,)'::tstzrange``, not
    ``NULL::tstzrange``.

    ``NULL::tstzrange && tstzrange(...)`` and ``NULL::tstzrange @> :dt``
    both evaluate to NULL under three-valued logic → every row silently
    dropped → datetime-filtered PG-only /search returns nothing.

    ``'(,)'::tstzrange`` is the all-of-time unbounded range; a collection
    with no validity column is always-valid, so every temporal predicate
    against it must match all rows.
    """
    col_config = _make_no_validity_config()

    class _SidecarMock(MagicMock):
        @classmethod
        def serves_consumers(cls):
            return None

    mock_geom = _SidecarMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.has_validity.return_value = False
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry(Geometry,4326)",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geometries ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None
    mock_geom.get_main_geometry_field.return_value = "geom"
    mock_geom.get_select_fields.return_value = [
        "ST_AsGeoJSON(sc_geometries.geom)::jsonb as geom"
    ]

    mock_attr = _SidecarMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    # has_validity() must return False — no validity column on this collection.
    mock_attr.has_validity.return_value = False
    mock_attr.get_queryable_fields.return_value = {}
    mock_attr.get_join_clause.return_value = (
        "LEFT JOIN attr_table sc_attributes ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None
    mock_attr.get_main_geometry_field.return_value = None
    mock_attr.get_select_fields.return_value = []

    with patch(
        "dynastore.modules.catalog.query_optimizer.driver_sidecars",
        return_value=[
            col_config.sidecars[0],
            col_config.sidecars[1],
        ],
    ), patch(
        "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry"
    ) as mock_registry:
        mock_registry.get_sidecar.side_effect = (
            lambda sc, lenient=True: mock_geom
            if getattr(sc, "sidecar_type", "") == "geometries"
            else mock_attr
        )

        optimizer = QueryOptimizer(col_config)

        # Verify that resolve_validity_expression() returns None for this config:
        # both the hub has no "validity" partition key and both sidecars
        # report has_validity() == False.
        assert optimizer.resolve_validity_expression() is None, (
            "precondition: no validity column configured → must return None"
        )

        # Feed a raw_where fragment that contains the bare ``validity`` token,
        # as produced by shared_queries.build_filter_clause when a STAC
        # datetime filter is present.
        req = QueryRequest(
            select=[FieldSelection(field="*")],
            raw_where="validity && tstzrange(:dt_start, :dt_end)",
            raw_params={"dt_start": "2023-01-01", "dt_end": "2024-01-01"},
            include_total_count=False,
        )

        sql, _ = optimizer.build_optimized_query(req, "myschema", "mytable")

    # The all-of-time sentinel must appear in the generated SQL.
    assert "'(,)'::tstzrange" in sql, (
        f"Expected all-of-time range sentinel in SQL; got:\n{sql}"
    )
    # The NULL placeholder must NOT appear — it silently drops every row.
    assert "NULL::tstzrange" not in sql, (
        f"NULL::tstzrange must not appear in SQL (three-valued-logic row drop); got:\n{sql}"
    )
