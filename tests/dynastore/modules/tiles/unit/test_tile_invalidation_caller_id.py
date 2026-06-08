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

"""Contract test: ``enqueue_tile_invalidation_task`` attributes the row to
the originating principal when provided, falls back to a system sentinel
otherwise.

Locks the wiring added so write-reactive tile-cache invalidation (#1296,
#1298) carries the user who triggered the write — not a hardcoded
``"tile_cache_invalidation"`` string that shadowed the real principal.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tiles import tile_cache_sync


class _FakeFeature:
    def __init__(self, bbox):
        self.bbox = bbox
        self.geometry = None
        self.id = "f1"


def _feature_with_bbox():
    return _FakeFeature([0.0, 0.0, 1.0, 1.0])


@pytest.mark.asyncio
async def test_uses_supplied_caller_id() -> None:
    captured: Dict[str, Any] = {}

    fake_task = MagicMock()
    fake_task.task_id = "t1"

    async def _fake_create_task(_engine, task_create, **kwargs):
        captured["task_create"] = task_create
        captured["kwargs"] = kwargs
        return fake_task

    with patch.object(
        tile_cache_sync, "is_tile_cache_active",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        new=AsyncMock(side_effect=_fake_create_task),
    ), patch.object(
        tile_cache_sync, "feature_bbox", return_value=(0.0, 0.0, 1.0, 1.0),
    ):
        n = await tile_cache_sync.enqueue_tile_invalidation_task(
            "c1", "col1", [_feature_with_bbox()],
            engine=MagicMock(),
            schema="s_xyz",
            caller_id="user:alice",
        )

    assert n == 1
    tc = captured["task_create"]
    assert tc.caller_id == "user:alice"
    assert tc.collection_id == "col1"
    assert tc.task_type == "tiles_invalidate"
    assert captured["kwargs"]["schema"] == "s_xyz"


@pytest.mark.asyncio
async def test_falls_back_to_system_sentinel_when_caller_unknown() -> None:
    """No principal in context → the enqueue still happens but the row is
    attributed to ``"system:tile_cache_invalidation"`` so audit queries on
    ``caller_id IS NULL`` cleanly flag any genuinely unattributed enqueue.
    """
    captured: Dict[str, Any] = {}

    fake_task = MagicMock()
    fake_task.task_id = "t1"

    async def _fake_create_task(_engine, task_create, **kwargs):
        captured["task_create"] = task_create
        return fake_task

    with patch.object(
        tile_cache_sync, "is_tile_cache_active",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.tasks.tasks_module.create_task",
        new=AsyncMock(side_effect=_fake_create_task),
    ), patch.object(
        tile_cache_sync, "feature_bbox", return_value=(0.0, 0.0, 1.0, 1.0),
    ):
        n = await tile_cache_sync.enqueue_tile_invalidation_task(
            "c1", "col1", [_feature_with_bbox()],
            engine=MagicMock(),
            schema="s_xyz",
        )

    assert n == 1
    assert captured["task_create"].caller_id == "system:tile_cache_invalidation"
