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

"""Unit tests for the GeoVolumes demo preset (volumes extension).

The volumes extension serves 3D Tiles at runtime; no offline tileset task
is enqueued — tests assert that ingest runs but no task enqueue occurs.
"""

from __future__ import annotations

import pathlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------


def _make_ctx(
    *,
    catalog_exists: bool = False,
    collection_exists: bool = False,
    items_count: int = 0,
) -> Any:
    """Build a minimal PresetContext-shaped mock."""
    catalogs = AsyncMock()
    catalogs.get_catalog_model = AsyncMock(
        return_value=MagicMock() if catalog_exists else None
    )
    catalogs.get_collection = AsyncMock(
        return_value=MagicMock() if collection_exists else None
    )
    catalogs.create_catalog = AsyncMock()
    catalogs.create_collection = AsyncMock()
    catalogs.upsert = AsyncMock()
    catalogs.delete_item = AsyncMock()
    catalogs.delete_collection = AsyncMock()
    catalogs.delete_catalog = AsyncMock()

    catalogs.search_items = AsyncMock(
        return_value=[MagicMock()] * items_count
    )
    catalogs.list_collections = AsyncMock(return_value=[])

    ctx = MagicMock()
    ctx.catalogs = catalogs
    ctx.db = MagicMock()
    ctx.tasks = MagicMock()
    ctx.principal = None
    ctx.scope = "platform"
    return ctx


def _make_params(
    local_path: str | None = None,
    max_features: int | None = None,
) -> Any:
    from dynastore.extensions.volumes.presets.cityjson_demo import GeoVolumesDemoParams

    return GeoVolumesDemoParams(
        local_path=local_path,
        max_features=max_features,
    )


# ---------------------------------------------------------------------------
# Preset metadata
# ---------------------------------------------------------------------------


def test_preset_name():
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET

    assert GEOVOLUMES_DEMO_PRESET.name == "geovolumes_demo"


def test_preset_keywords():
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET

    assert "geovolumes" in GEOVOLUMES_DEMO_PRESET.keywords
    assert "demo" in GEOVOLUMES_DEMO_PRESET.keywords


def test_params_model_defaults():
    from dynastore.extensions.volumes.presets.cityjson_demo import GeoVolumesDemoParams

    p = GeoVolumesDemoParams()
    assert p.catalog_id == "demo-3d"
    assert p.collection_id == "denhaag"
    assert p.source_url.startswith("https://")
    assert p.local_path is None
    assert p.max_features is None


# ---------------------------------------------------------------------------
# apply() — fresh install
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_creates_catalog_and_collection():
    """apply() creates catalog + collection when they do not exist."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx(catalog_exists=False, collection_exists=False)
    params = GeoVolumesDemoParams(local_path=str(FIXTURES / "minimal.city.jsonl"))

    with patch(
        "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
        new_callable=AsyncMock,
        return_value={"items": 3, "failed": 0, "warnings": []},
    ) as mock_ingest:
        descriptor = await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    ctx.catalogs.create_catalog.assert_awaited_once()
    ctx.catalogs.create_collection.assert_awaited_once()
    mock_ingest.assert_awaited_once()

    payload = descriptor.payload
    assert payload["created_catalog"] is True
    assert payload["created_collection"] is True
    assert payload["catalog_id"] == "demo-3d"
    assert payload["collection_id"] == "denhaag"


@pytest.mark.asyncio
async def test_apply_no_task_enqueue():
    """apply() does NOT enqueue any offline tileset task — tiles are runtime."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx()
    params = GeoVolumesDemoParams(local_path=str(FIXTURES / "minimal.city.jsonl"))

    with patch(
        "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
        new_callable=AsyncMock,
        return_value={"items": 3, "failed": 0, "warnings": []},
    ):
        await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    # No task execution via ctx.tasks
    ctx.tasks.assert_not_called()


@pytest.mark.asyncio
async def test_apply_skips_existing_catalog_and_collection():
    """apply() skips create_catalog / create_collection when they already exist."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx(catalog_exists=True, collection_exists=True)
    params = GeoVolumesDemoParams(local_path=str(FIXTURES / "minimal.city.jsonl"))

    with patch(
        "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
        new_callable=AsyncMock,
        return_value={"items": 3, "failed": 0, "warnings": []},
    ):
        descriptor = await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    ctx.catalogs.create_catalog.assert_not_awaited()
    ctx.catalogs.create_collection.assert_not_awaited()
    assert descriptor.payload["created_catalog"] is False
    assert descriptor.payload["created_collection"] is False


@pytest.mark.asyncio
async def test_apply_idempotent_skip_when_collection_has_items():
    """apply() does NOT re-ingest when the collection already has items."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx(catalog_exists=True, collection_exists=True, items_count=5)
    params = GeoVolumesDemoParams(local_path=str(FIXTURES / "minimal.city.jsonl"))

    with patch(
        "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
        new_callable=AsyncMock,
    ) as mock_ingest:
        descriptor = await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    mock_ingest.assert_not_awaited()
    assert descriptor.payload.get("skipped") is True


@pytest.mark.asyncio
async def test_apply_ingest_called_with_correct_args():
    """apply() passes catalog_id, collection_id, and path to ingest_cityjson_file."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx()
    fixture_path = str(FIXTURES / "minimal.city.jsonl")
    params = GeoVolumesDemoParams(local_path=fixture_path)

    with patch(
        "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
        new_callable=AsyncMock,
        return_value={"items": 3, "failed": 0, "warnings": []},
    ) as mock_ingest:
        await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    call_kwargs = mock_ingest.call_args
    assert call_kwargs.args[0] == "demo-3d"
    assert call_kwargs.args[1] == "denhaag"


# ---------------------------------------------------------------------------
# apply() — download path (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_downloads_when_no_local_path():
    """apply() downloads source_url when local_path is None."""
    from dynastore.extensions.volumes.presets.cityjson_demo import (
        GEOVOLUMES_DEMO_PRESET,
        GeoVolumesDemoParams,
    )

    ctx = _make_ctx()
    params = GeoVolumesDemoParams(local_path=None)

    fixture_path = FIXTURES / "minimal.city.jsonl"

    with (
        patch(
            "dynastore.extensions.volumes.presets.cityjson_demo._download_source",
            return_value=fixture_path,
        ) as mock_dl,
        patch(
            "dynastore.extensions.volumes.presets.cityjson_demo.ingest_cityjson_file",
            new_callable=AsyncMock,
            return_value={"items": 3, "failed": 0, "warnings": []},
        ),
    ):
        await GEOVOLUMES_DEMO_PRESET.apply(params, "platform", ctx)

    mock_dl.assert_called_once()


# ---------------------------------------------------------------------------
# revoke()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_removes_what_preset_created():
    """revoke() deletes collection + catalog when this preset created them and they are empty."""
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    ctx = _make_ctx(items_count=0)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "collection_id": "denhaag",
        "created_catalog": True,
        "created_collection": True,
        "skipped": False,
    })

    await GEOVOLUMES_DEMO_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_awaited_once_with("demo-3d", "denhaag", force=True)
    ctx.catalogs.delete_catalog.assert_awaited_once_with("demo-3d", force=True)


@pytest.mark.asyncio
async def test_revoke_leaves_pre_existing_catalog_and_collection():
    """revoke() does NOT delete catalog/collection it did not create."""
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    ctx = _make_ctx(items_count=0)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "collection_id": "denhaag",
        "created_catalog": False,
        "created_collection": False,
        "skipped": False,
    })

    await GEOVOLUMES_DEMO_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_not_awaited()
    ctx.catalogs.delete_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_revoke_skips_non_empty_collection():
    """revoke() leaves collection in place when items remain after apply."""
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    ctx = _make_ctx(items_count=3)
    descriptor = AppliedDescriptor(payload={
        "catalog_id": "demo-3d",
        "collection_id": "denhaag",
        "created_catalog": True,
        "created_collection": True,
        "skipped": False,
    })

    await GEOVOLUMES_DEMO_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_not_awaited()
    ctx.catalogs.delete_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_revoke_noop_when_skipped():
    """revoke() is a no-op when apply was a skip (collection already had items)."""
    from dynastore.extensions.volumes.presets.cityjson_demo import GEOVOLUMES_DEMO_PRESET
    from dynastore.modules.storage.presets.preset import AppliedDescriptor

    ctx = _make_ctx()
    descriptor = AppliedDescriptor(payload={"skipped": True})

    await GEOVOLUMES_DEMO_PRESET.revoke(descriptor, ctx)

    ctx.catalogs.delete_collection.assert_not_awaited()
    ctx.catalogs.delete_catalog.assert_not_awaited()
