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

"""Unit tests for the tiles3d_samples preset (volumes extension).

Covers: registry sanity, region→bbox conversion, params defaults,
apply/revoke lifecycle with mocked catalog service.
"""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers / stubs  (mirror test_volumes_presets.py)
# ---------------------------------------------------------------------------


def _make_ctx(
    *,
    catalog_exists: bool = False,
    collection_exists: bool = False,
    items_count: int = 0,
) -> Any:
    catalogs = AsyncMock()
    catalogs.get_catalog_model = AsyncMock(
        return_value=MagicMock() if catalog_exists else None
    )
    catalogs.get_collection = AsyncMock(
        return_value=MagicMock() if collection_exists else None
    )
    catalogs.create_catalog = AsyncMock()
    catalogs.create_collection = AsyncMock()
    catalogs.delete_collection = AsyncMock()
    catalogs.delete_catalog = AsyncMock()
    catalogs.search_items = AsyncMock(return_value=[MagicMock()] * items_count)
    catalogs.list_collections = AsyncMock(return_value=[])

    ctx = MagicMock()
    ctx.catalogs = catalogs
    ctx.scope = "platform"
    return ctx


# ---------------------------------------------------------------------------
# Registry sanity
# ---------------------------------------------------------------------------


def test_registry_ids_are_unique():
    from dynastore.extensions.volumes.presets.tiles3d_samples import SAMPLE_REGISTRY

    ids = [s.id for s in SAMPLE_REGISTRY]
    assert len(ids) == len(set(ids)), "Duplicate sample ids in registry"


def test_registry_urls_are_https():
    from dynastore.extensions.volumes.presets.tiles3d_samples import SAMPLE_REGISTRY

    for sample in SAMPLE_REGISTRY:
        assert sample.url.startswith("https://"), (
            f"Sample {sample.id!r} url must use https://"
        )


def test_registry_3dbag_excluded_by_default():
    """The 3D BAG Rotterdam sample must NOT be included by default."""
    from dynastore.extensions.volumes.presets.tiles3d_samples import SAMPLE_REGISTRY

    defaults = [s for s in SAMPLE_REGISTRY if s.include_by_default]
    non_defaults = [s for s in SAMPLE_REGISTRY if not s.include_by_default]
    assert any("rotterdam" in s.id for s in non_defaults), (
        "rotterdam sample must have include_by_default=False"
    )
    assert not any("rotterdam" in s.id for s in defaults), (
        "rotterdam sample must NOT appear in defaults"
    )


def test_registry_default_sample_count():
    """Exactly 4 samples are enabled by default (the 5th requires include_3dbag)."""
    from dynastore.extensions.volumes.presets.tiles3d_samples import SAMPLE_REGISTRY

    defaults = [s for s in SAMPLE_REGISTRY if s.include_by_default]
    assert len(defaults) == 4


def test_registry_fallback_bboxes_are_6_elements():
    from dynastore.extensions.volumes.presets.tiles3d_samples import SAMPLE_REGISTRY

    for sample in SAMPLE_REGISTRY:
        assert len(sample.fallback_bbox) == 6, (
            f"Sample {sample.id!r} fallback_bbox must have 6 elements"
        )


# ---------------------------------------------------------------------------
# region_to_bbox conversion (pure function unit tests)
# ---------------------------------------------------------------------------


def test_region_to_bbox_converts_radians_to_degrees():
    from dynastore.extensions.volumes.presets.tiles3d_samples import region_to_bbox

    west  = math.radians(-75.62)
    south = math.radians(40.03)
    east  = math.radians(-75.60)
    north = math.radians(40.05)
    min_h, max_h = 0.0, 100.0

    result = region_to_bbox([west, south, east, north, min_h, max_h])

    assert len(result) == 6
    assert abs(result[0] - (-75.62)) < 1e-9, "west mismatch"
    assert abs(result[1] - 40.03)    < 1e-9, "south mismatch"
    assert result[2] == 0.0,                 "min_h mismatch"
    assert abs(result[3] - (-75.60)) < 1e-9, "east mismatch"
    assert abs(result[4] - 40.05)    < 1e-9, "north mismatch"
    assert result[5] == 100.0,               "max_h mismatch"


def test_region_to_bbox_preserves_height_as_metres():
    from dynastore.extensions.volumes.presets.tiles3d_samples import region_to_bbox

    # Heights are in metres and must not be converted.
    result = region_to_bbox([0.0, 0.0, 0.0, 0.0, -10.5, 250.75])
    assert result[2] == -10.5
    assert result[5] == 250.75


def test_region_to_bbox_rejects_wrong_arity():
    from dynastore.extensions.volumes.presets.tiles3d_samples import region_to_bbox

    with pytest.raises(ValueError, match="6 elements"):
        region_to_bbox([1.0, 2.0, 3.0])


# ---------------------------------------------------------------------------
# Params model defaults
# ---------------------------------------------------------------------------


def test_params_model_defaults():
    from dynastore.extensions.volumes.presets.tiles3d_samples import Tiles3dSamplesParams

    p = Tiles3dSamplesParams()
    assert p.catalog_id == "demo-3d"
    assert p.include_3dbag is False
    assert p.collection_prefix == ""


def test_params_include_3dbag_flag():
    from dynastore.extensions.volumes.presets.tiles3d_samples import Tiles3dSamplesParams

    p = Tiles3dSamplesParams(include_3dbag=True)
    assert p.include_3dbag is True


def test_params_collection_prefix():
    from dynastore.extensions.volumes.presets.tiles3d_samples import Tiles3dSamplesParams

    p = Tiles3dSamplesParams(collection_prefix="test-")
    assert p.collection_prefix == "test-"


# ---------------------------------------------------------------------------
# Preset metadata
# ---------------------------------------------------------------------------


def test_preset_name():
    from dynastore.extensions.volumes.presets.tiles3d_samples import TILES3D_SAMPLES_PRESET

    assert TILES3D_SAMPLES_PRESET.name == "tiles3d_samples"


def test_preset_keywords():
    from dynastore.extensions.volumes.presets.tiles3d_samples import TILES3D_SAMPLES_PRESET

    assert "geovolumes" in TILES3D_SAMPLES_PRESET.keywords
    assert "demo" in TILES3D_SAMPLES_PRESET.keywords
    assert "3dtiles" in TILES3D_SAMPLES_PRESET.keywords


# ---------------------------------------------------------------------------
# apply() — fresh catalog + collections
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_creates_catalog_and_default_collections():
    from unittest.mock import patch

    from dynastore.extensions.volumes.presets.tiles3d_samples import (
        TILES3D_SAMPLES_PRESET,
        Tiles3dSamplesParams,
    )

    ctx = _make_ctx(catalog_exists=False, collection_exists=False)
    params = Tiles3dSamplesParams()

    with patch(
        "dynastore.extensions.volumes.presets.tiles3d_samples._resolve_bbox",
        return_value=[-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    ):
        descriptor = await TILES3D_SAMPLES_PRESET.apply(params, "platform", ctx)

    ctx.catalogs.create_catalog.assert_awaited_once()
    # 4 default samples → 4 create_collection calls
    assert ctx.catalogs.create_collection.await_count == 4

    payload = descriptor.payload
    assert payload["created_catalog"] is True
    assert len(payload["created_collections"]) == 4
    assert payload["include_3dbag"] is False


@pytest.mark.asyncio
async def test_apply_includes_3dbag_when_flag_set():
    from unittest.mock import patch

    from dynastore.extensions.volumes.presets.tiles3d_samples import (
        TILES3D_SAMPLES_PRESET,
        Tiles3dSamplesParams,
    )

    ctx = _make_ctx(catalog_exists=False, collection_exists=False)
    params = Tiles3dSamplesParams(include_3dbag=True)

    with patch(
        "dynastore.extensions.volumes.presets.tiles3d_samples._resolve_bbox",
        return_value=[-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    ):
        descriptor = await TILES3D_SAMPLES_PRESET.apply(params, "platform", ctx)

    assert ctx.catalogs.create_collection.await_count == 5
    assert len(descriptor.payload["created_collections"]) == 5


@pytest.mark.asyncio
async def test_apply_skips_existing_catalog():
    from unittest.mock import patch

    from dynastore.extensions.volumes.presets.tiles3d_samples import (
        TILES3D_SAMPLES_PRESET,
        Tiles3dSamplesParams,
    )

    ctx = _make_ctx(catalog_exists=True, collection_exists=False)
    params = Tiles3dSamplesParams()

    with patch(
        "dynastore.extensions.volumes.presets.tiles3d_samples._resolve_bbox",
        return_value=[-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    ):
        descriptor = await TILES3D_SAMPLES_PRESET.apply(params, "platform", ctx)

    ctx.catalogs.create_catalog.assert_not_awaited()
    assert descriptor.payload["created_catalog"] is False


@pytest.mark.asyncio
async def test_apply_skips_existing_collections():
    from unittest.mock import patch

    from dynastore.extensions.volumes.presets.tiles3d_samples import (
        TILES3D_SAMPLES_PRESET,
        Tiles3dSamplesParams,
    )

    ctx = _make_ctx(catalog_exists=True, collection_exists=True)
    params = Tiles3dSamplesParams()

    with patch(
        "dynastore.extensions.volumes.presets.tiles3d_samples._resolve_bbox",
        return_value=[-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    ):
        descriptor = await TILES3D_SAMPLES_PRESET.apply(params, "platform", ctx)

    ctx.catalogs.create_collection.assert_not_awaited()
    assert descriptor.payload["created_collections"] == []


@pytest.mark.asyncio
async def test_apply_collection_payload_has_tileset_url():
    """Each collection payload must include geovolumes:tileset_url in extra_metadata."""
    from unittest.mock import patch

    from dynastore.extensions.volumes.presets.tiles3d_samples import (
        TILES3D_SAMPLES_PRESET,
        Tiles3dSamplesParams,
    )

    ctx = _make_ctx(catalog_exists=True, collection_exists=False)
    params = Tiles3dSamplesParams()

    with patch(
        "dynastore.extensions.volumes.presets.tiles3d_samples._resolve_bbox",
        return_value=[-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    ):
        await TILES3D_SAMPLES_PRESET.apply(params, "platform", ctx)

    for call in ctx.catalogs.create_collection.await_args_list:
        _cat_id, coll_payload = call.args[0], call.args[1]
        em = coll_payload.get("extra_metadata", {})
        inner = em.get("en", em)
        assert "geovolumes:tileset_url" in inner, (
            f"collection payload missing geovolumes:tileset_url: {coll_payload}"
        )
        assert inner["geovolumes:enabled"] is True


# ---------------------------------------------------------------------------
# revoke()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_removes_created_empty_collections():
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    from dynastore.extensions.volumes.presets.tiles3d_samples import TILES3D_SAMPLES_PRESET

    ctx = _make_ctx(items_count=0)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "created_catalog": True,
        "created_collections": ["tiles3d-dragon-lod", "tiles3d-trees-i3dm"],
    })

    await TILES3D_SAMPLES_PRESET.revoke(descriptor, ctx)

    assert ctx.catalogs.delete_collection.await_count == 2
    ctx.catalogs.delete_catalog.assert_awaited_once_with("demo-3d", force=True)


@pytest.mark.asyncio
async def test_revoke_leaves_non_empty_collections():
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    from dynastore.extensions.volumes.presets.tiles3d_samples import TILES3D_SAMPLES_PRESET

    ctx = _make_ctx(items_count=3)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "created_catalog": True,
        "created_collections": ["tiles3d-dragon-lod"],
    })

    await TILES3D_SAMPLES_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_not_awaited()
    ctx.catalogs.delete_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_revoke_no_op_when_nothing_created():
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    from dynastore.extensions.volumes.presets.tiles3d_samples import TILES3D_SAMPLES_PRESET

    ctx = _make_ctx()
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "created_catalog": False,
        "created_collections": [],
    })

    await TILES3D_SAMPLES_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_not_awaited()
    ctx.catalogs.delete_catalog.assert_not_awaited()
