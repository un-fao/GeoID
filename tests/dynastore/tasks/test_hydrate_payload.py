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

"""Tests for hydrate_task_payload — ensures dict inputs are correctly
converted to Pydantic models for all task types, including those that
inherit from Protocol-based generics (ProcessTaskProtocol)."""

import pytest

pytest.importorskip("morecantile")  # optional dep — skip when SCOPE excludes it

from dynastore.tools.identifiers import generate_task_id

from dynastore.tasks import hydrate_task_payload
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.tasks.tiles_preseed.task import TilePreseedTask
from dynastore.tasks.tiles_preseed.models import TilePreseedRequest
from dynastore.tasks.gcp.gcp_catalog_cleanup_task import (
    GcpCatalogCleanupTask,
    GcpCatalogCleanupInputs,
)


def _make_raw_payload(inputs: dict) -> dict:
    return {
        "task_id": str(generate_task_id()),
        "caller_id": "test",
        "inputs": inputs,
    }


class TestHydrateTaskPayload:
    """Verify hydration converts dict inputs to the correct Pydantic model."""

    def test_tile_preseed_request_hydrated(self):
        """TilePreseedTask (ProcessTaskProtocol subclass) hydrates inputs as
        ``ExecuteRequest`` — the OGC Process wrapper. The user-supplied dict
        (catalog_id + collection_id) lives under ``payload.inputs.inputs``;
        the task's body validates that inner dict against ``TilePreseedRequest``.
        Same wrap shape as DwhJoinExportTask and ExportFeaturesTask after the
        Phase H per-task unwrap fixes (PR #139)."""
        task = TilePreseedTask.__new__(TilePreseedTask)
        # OGC Process inputs are wrapped: outer `inputs` is the ExecuteRequest
        # body; inner `inputs` is the user-supplied request dict that the task
        # body parses against its own request model.
        raw = _make_raw_payload({
            "inputs": {
                "catalog_id": "cat_test",
                "collection_id": "col_test",
            },
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, ExecuteRequest)
        assert payload.inputs.inputs["catalog_id"] == "cat_test"
        # The task body would do: TilePreseedRequest.model_validate(payload.inputs.inputs)
        validated = TilePreseedRequest.model_validate(payload.inputs.inputs)
        assert validated.catalog_id == "cat_test"
        assert validated.collection_id == "col_test"

    def test_gcp_catalog_cleanup_hydrated(self):
        task = GcpCatalogCleanupTask.__new__(GcpCatalogCleanupTask)
        raw = _make_raw_payload({
            "catalog_id": "cat_test",
            "scope": "catalog",
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, GcpCatalogCleanupInputs)
        assert payload.inputs.catalog_id == "cat_test"

    def test_unknown_inputs_left_as_dict(self):
        """Tasks with bare TaskPayload (no generic) should keep inputs as dict."""
        from dynastore.tasks.elasticsearch_indexer.tasks import BulkCatalogReindexTask
        task = BulkCatalogReindexTask.__new__(BulkCatalogReindexTask)
        raw = _make_raw_payload({"catalog_id": "cat_test", "mode": "full"})
        payload = hydrate_task_payload(task, raw)
        # BulkCatalogReindexTask uses TaskPayload (no generic), so inputs stay as dict
        # and the task itself calls model_validate internally
        assert isinstance(payload.inputs, dict)
