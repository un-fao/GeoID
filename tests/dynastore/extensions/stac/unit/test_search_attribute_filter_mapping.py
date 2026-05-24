"""STAC ``POST /search`` attribute filter — storage-mode-aware field mapping
(#1141 / Refs #1043).

Before this fix ``search.py._build_cql2_field_mapping`` mapped every property to
``text("attributes->>'col'")``: a ``TextClause`` that pygeofilter collapses to an
always-false ``1=0``, *and* a hard-coded JSONB accessor that does not exist for
COLUMNAR sidecar storage (declared queryables are physical columns
``sc_attributes."col"`` post #1065/#1074). The ``/items`` path resolves
queryables through :class:`QueryOptimizer` to
``literal_column(field_def.sql_expression)``; the search path must do the same so
the predicate binds against the correct physical column.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pygeofilter", reason="pygeofilter required for CQL tests")

from sqlalchemy import text

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
    AttributeSchemaEntry,
    PostgresType,
)
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
from dynastore.modules.tools.cql import parse_cql2_json_filter
from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
from dynastore.extensions.stac.search import (
    _build_cql2_field_mapping,
    _attributes_hydration_projection,
)


def _columnar_optimizer() -> QueryOptimizer:
    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.COLUMNAR,
                attribute_schema=[
                    AttributeSchemaEntry(name="adm2_pcode", type=PostgresType.TEXT),
                ],
            ),
        ]
    )
    return QueryOptimizer(cfg, consumer=ConsumerType.STAC)


def test_columnar_equality_binds_to_physical_column():
    mapping = _build_cql2_field_mapping(_columnar_optimizer())
    where, params = parse_cql2_json_filter(
        {"op": "=", "args": [{"property": "adm2_pcode"}, "PK001"]},
        field_mapping=mapping,
    )
    # Not the always-false clause and not the JSONB accessor.
    assert where != "1=0"
    assert "attributes->>" not in where
    assert 'sc_attributes."adm2_pcode"' in where
    # Value is bound, and the placeholder is text()-safe (binds via text()).
    assert "PK001" in params.values()
    assert set(text(where)._bindparams.keys()) == set(params.keys())


def test_unknown_property_is_rejected():
    mapping = _build_cql2_field_mapping(_columnar_optimizer())
    with pytest.raises(ValueError):
        parse_cql2_json_filter(
            {"op": "=", "args": [{"property": "no_such_col"}, "x"]},
            field_mapping=mapping,
        )


# --- Hydration projection (Refs #1253): the search UNION-ALL hydration must not
# project a bare ``s.attributes`` column, which does not exist for COLUMNAR
# sidecar storage and raises ``UndefinedColumnError``. ------------------------


def _attr_sidecar(cfg: ItemsPostgresqlDriverConfig):
    for sc in driver_sidecars(cfg):
        if getattr(sc, "sidecar_type", "") in ("attributes", "feature_attributes"):
            return sc
    raise AssertionError("config has no attributes sidecar")


def test_columnar_hydration_rebuilds_attributes_from_columns():
    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.COLUMNAR,
                attribute_schema=[
                    AttributeSchemaEntry(name="adm2_pcode", type=PostgresType.TEXT),
                    AttributeSchemaEntry(name="pop", type=PostgresType.INTEGER),
                ],
            ),
        ]
    )
    proj, is_columnar = _attributes_hydration_projection(_attr_sidecar(cfg))
    # The bare blob column is never referenced for COLUMNAR storage.
    assert is_columnar is True
    assert "s.attributes" not in proj
    assert proj.startswith("jsonb_build_object(")
    assert "'adm2_pcode', s.\"adm2_pcode\"" in proj
    assert "'pop', s.\"pop\"" in proj


def test_jsonb_hydration_projects_blob_column_unchanged():
    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB),
        ]
    )
    proj, is_columnar = _attributes_hydration_projection(_attr_sidecar(cfg))
    # JSONB storage projects the blob column directly (default name ``attributes``).
    assert is_columnar is False
    assert proj == 's."attributes"'
