"""Tests for hydrate_task_payload — ensures dict inputs are correctly
converted to Pydantic models for all task types, including those that
inherit from Protocol-based generics (ProcessTaskProtocol)."""

import pytest
from dynastore.tools.identifiers import generate_task_id

from dynastore.tasks import hydrate_task_payload
from dynastore.tasks.tiles_preseed.task import TilePreseedTask
from dynastore.tasks.tiles_preseed.models import TilePreseedRequest
from dynastore.tasks.elasticsearch.tasks import (
    ElasticsearchIndexTask,
    ElasticsearchIndexInputs,
    ElasticsearchDeleteTask,
    ElasticsearchDeleteInputs,
)
from dynastore.tasks.gcp.gcs_storage_event_task import (
    GcsStorageEventTask,
    GcsStorageEventInputs,
)
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
        """TilePreseedTask (ProcessTaskProtocol subclass) should hydrate inputs."""
        task = TilePreseedTask.__new__(TilePreseedTask)
        raw = _make_raw_payload({
            "catalog_id": "cat_test",
            "collection_id": "col_test",
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, TilePreseedRequest)
        assert payload.inputs.catalog_id == "cat_test"

    def test_elasticsearch_index_hydrated(self):
        task = ElasticsearchIndexTask.__new__(ElasticsearchIndexTask)
        raw = _make_raw_payload({
            "entity_type": "item",
            "entity_id": "test-id",
            "payload": {"foo": "bar"},
            "catalog_id": "cat_test",
            "collection_id": "col_test",
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, ElasticsearchIndexInputs)
        assert payload.inputs.entity_type == "item"

    def test_elasticsearch_delete_hydrated(self):
        task = ElasticsearchDeleteTask.__new__(ElasticsearchDeleteTask)
        raw = _make_raw_payload({
            "entity_type": "item",
            "entity_id": "del-id",
            "catalog_id": "cat_test",
            "collection_id": "col_test",
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, ElasticsearchDeleteInputs)
        assert payload.inputs.entity_id == "del-id"

    def test_gcs_storage_event_hydrated(self):
        task = GcsStorageEventTask.__new__(GcsStorageEventTask)
        raw = _make_raw_payload({
            "catalog_id": "cat_test",
            "collection_id": "col_test",
            "event_type": "OBJECT_FINALIZE",
            "asset_id": "asset_1",
            "uri": "gs://test-bucket/test-obj",
        })
        payload = hydrate_task_payload(task, raw)
        assert isinstance(payload.inputs, GcsStorageEventInputs)
        assert payload.inputs.event_type == "OBJECT_FINALIZE"

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
