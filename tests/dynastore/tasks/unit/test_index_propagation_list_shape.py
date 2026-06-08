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

"""IndexPropagationInputs is list-shaped — a 1-op replay is a list of length 1."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence
from unittest.mock import patch
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, IndexContext, IndexOp,
)
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.index_propagation.task import (
    IndexPropagationInputs, IndexPropagationTask,
)


class _FakeIndexer:
    """Stand-in Indexer impl whose snake-cased class name matches the
    indexer_id the test enqueues."""

    def __init__(self, name: str) -> None:
        self.__class__.__name__ = name
        self.calls: List[Sequence[IndexOp]] = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:  # noqa: D401
        return None

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:  # noqa: D401
        raise AssertionError(
            "IndexPropagationTask.run must call index_bulk, never index"
        )

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.calls.append(list(ops))
        return BulkResult(
            total=len(ops), succeeded=len(ops), failed=0, failures=[],
        )


def _payload(inputs: Dict[str, Any]) -> TaskPayload:
    return TaskPayload(task_id=uuid4(), caller_id="test", inputs=inputs)


def test_list_shape_round_trips_ops() -> None:
    inputs = IndexPropagationInputs.model_validate({
        "indexer_id": "fake_indexer",
        "entity_type": "item",
        "catalog": "cat",
        "collection": "col",
        "ops": [
            {"entity_id": "e1", "op_type": "upsert", "payload": {"x": 1}},
            {"entity_id": "e2", "op_type": "delete", "payload": None},
        ],
    })
    assert len(inputs.ops) == 2
    assert inputs.ops[0].entity_id == "e1"
    assert inputs.ops[1].op_type == "delete"


@pytest.mark.asyncio
async def test_run_always_calls_index_bulk_for_bulk_of_one() -> None:
    fake = _FakeIndexer("FakeIndexer")
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        result = await IndexPropagationTask().run(_payload({
            "indexer_id": "fake_indexer",
            "entity_type": "item",
            "catalog": "cat",
            "collection": "col",
            "ops": [{"entity_id": "e1", "op_type": "upsert", "payload": {"x": 1}}],
        }))
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 1
    assert fake.calls[0][0].entity_id == "e1"
    assert result["status"] == "ok"
    assert result["total"] == 1


@pytest.mark.asyncio
async def test_run_reports_partial_on_per_op_failures() -> None:
    class _PartialFailIndexer(_FakeIndexer):
        async def index_bulk(  # type: ignore[override]
            self, ctx: IndexContext, ops: Sequence[IndexOp],
        ) -> BulkResult:
            return BulkResult(
                total=len(ops), succeeded=len(ops) - 1, failed=1,
                failures=[{"entity_id": ops[-1].entity_id, "error": "mapping"}],
            )

    fake = _PartialFailIndexer("FakeIndexer")
    with patch(
        "dynastore.tasks.index_propagation.task.get_protocols",
        return_value=[fake],
    ):
        result = await IndexPropagationTask().run(_payload({
            "indexer_id": "fake_indexer",
            "entity_type": "item",
            "catalog": "cat",
            "ops": [
                {"entity_id": "ok", "op_type": "upsert", "payload": {}},
                {"entity_id": "bad", "op_type": "upsert", "payload": {}},
            ],
        }))
    assert result["status"] == "partial"
    assert result["failed"] == 1
    assert result["failures"][0]["entity_id"] == "bad"
