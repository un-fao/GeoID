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

"""Unit tests for the heterogeneous-multi-collection guard in
``maps_db.get_features_for_rendering`` (#737 item 2 / F3).

The guard raises ``ValueError`` when a multi-collection UNION render request
spans collections with differing column sets or differing source SRIDs.
``maps_service.get_map_tile`` / ``get_map`` already convert ``ValueError`` to
``HTTPException(400)``, so this is the user-facing failure mode.

Single-collection requests stay on the existing one-pass path (no
homogeneity check).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.maps import maps_db


class _FakeDriver:
    def __init__(self, layer_config: Any) -> None:
        self._cfg = layer_config

    async def get_driver_config(self, schema: str, collection: str) -> Any:
        return self._cfg


def _layer_cfg(srid: int) -> MagicMock:
    """A layer config marker whose only contract is that
    ``driver_sidecars(cfg)`` returns a sidecar with ``target_srid == srid``."""
    cfg = MagicMock(name=f"layer_cfg_srid_{srid}")
    cfg.__srid__ = srid
    return cfg


def _patch_meta_resolution(monkeypatch, per_collection: dict[str, tuple[list[str], int]]) -> None:
    """Wire up the three external lookups
    (``get_driver``, ``get_table_column_names``, ``driver_sidecars``) so that
    each collection resolves to its declared (columns, srid)."""

    async def _fake_get_driver(_op, _schema, collection):
        cols, srid = per_collection[collection]
        return _FakeDriver(_layer_cfg(srid))

    async def _fake_get_table_column_names(_conn, _schema, collection):
        cols, _ = per_collection[collection]
        return cols

    def _fake_driver_sidecars(cfg):
        # Yield a single sidecar exposing the configured SRID, of the
        # GeometriesSidecarConfig type the production code filters on.
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )
        sc = MagicMock(spec=GeometriesSidecarConfig)
        sc.target_srid = cfg.__srid__
        return [sc]

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver", _fake_get_driver
    )
    monkeypatch.setattr(
        "dynastore.modules.db_config.shared_queries.get_table_column_names",
        _fake_get_table_column_names,
    )
    monkeypatch.setattr(
        "dynastore.modules.storage.drivers.pg_sidecars.driver_sidecars",
        _fake_driver_sidecars,
    )


@pytest.mark.asyncio
async def test_multi_collection_with_diverging_columns_raises_value_error(monkeypatch):
    _patch_meta_resolution(
        monkeypatch,
        {
            "col_a": (["geoid", "attributes", "year"], 4326),
            "col_b": (["geoid", "attributes", "month"], 4326),  # 'month' instead of 'year'
        },
    )
    with pytest.raises(ValueError, match="column sets differ"):
        await maps_db.get_features_for_rendering(
            conn=AsyncMock(),
            schema="cat_x",
            collections=["col_a", "col_b"],
            bbox=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=256,
            height=256,
        )


@pytest.mark.asyncio
async def test_multi_collection_with_diverging_source_srid_raises_value_error(monkeypatch):
    _patch_meta_resolution(
        monkeypatch,
        {
            "col_a": (["geoid", "attributes"], 4326),
            "col_b": (["geoid", "attributes"], 3857),  # different storage CRS
        },
    )
    with pytest.raises(ValueError, match="source SRID differs"):
        await maps_db.get_features_for_rendering(
            conn=AsyncMock(),
            schema="cat_x",
            collections=["col_a", "col_b"],
            bbox=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=256,
            height=256,
        )


@pytest.mark.asyncio
async def test_single_collection_skips_homogeneity_check(monkeypatch):
    """Hot path: one collection, no extra metadata fan-out, no ValueError."""
    _patch_meta_resolution(
        monkeypatch,
        {"col_a": (["geoid", "attributes"], 4326)},
    )
    # Stub the SQL execution so we don't need a real DB connection. The test
    # only cares that the function reaches the execute call (i.e. passes the
    # homogeneity guard) and propagates the stubbed return value.
    with patch.object(
        maps_db,
        "DQLQuery",
        return_value=MagicMock(execute=AsyncMock(return_value=[{"layer": "col_a"}])),
    ):
        rows = await maps_db.get_features_for_rendering(
            conn=AsyncMock(),
            schema="cat_x",
            collections=["col_a"],
            bbox=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=256,
            height=256,
        )
    assert rows == [{"layer": "col_a"}]


@pytest.mark.asyncio
async def test_homogeneous_multi_collection_passes_through(monkeypatch):
    """Matching columns + matching SRID → no raise, query is built and executed."""
    _patch_meta_resolution(
        monkeypatch,
        {
            "col_a": (["geoid", "attributes"], 4326),
            "col_b": (["geoid", "attributes"], 4326),
        },
    )
    with patch.object(
        maps_db,
        "DQLQuery",
        return_value=MagicMock(execute=AsyncMock(return_value=[])),
    ):
        rows = await maps_db.get_features_for_rendering(
            conn=AsyncMock(),
            schema="cat_x",
            collections=["col_a", "col_b"],
            bbox=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=256,
            height=256,
        )
    assert rows == []
