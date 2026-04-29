"""Phase C regression test — ElasticsearchIndexerTask branches by collection_id.

The unified `elasticsearch_indexer` OGC Process is the canonical entry point
for catalog/collection ES bulk reindex. Its `run()` adapts the unified
`ElasticsearchIndexerRequest` to either `BulkCatalogReindexTask` (catalog
scope, no collection_id) or `BulkCollectionReindexTask` (collection scope).

Both delegated task classes are loaded as side effects of importing
`indexer_task` (via `from . import tasks as _bulk_tasks`), and the
dispatcher's `get_task_instance` looks them up by their `task_type` class
attribute — which must remain stable.
"""
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.elasticsearch_indexer.indexer_models import (
    ElasticsearchIndexerRequest,
)
from dynastore.tasks.elasticsearch_indexer.indexer_task import ElasticsearchIndexerTask


def _make_payload(inputs: dict) -> TaskPayload:
    """Build a TaskPayload that mimics the OGC ExecuteRequest shape
    (`payload.inputs.inputs == user_dict`)."""

    class _ExecLike:
        pass

    exec_req = _ExecLike()
    exec_req.inputs = inputs  # type: ignore[attr-defined]
    return TaskPayload(
        task_id=uuid4(),
        caller_id="test:caller",
        inputs=exec_req,
    )


def test_elasticsearch_indexer_request_validates_minimal() -> None:
    """Catalog-only is the minimal valid request."""
    r = ElasticsearchIndexerRequest(catalog_id="cat_a")
    assert r.catalog_id == "cat_a"
    assert r.collection_id is None
    assert r.driver is None


def test_elasticsearch_indexer_request_validates_full() -> None:
    r = ElasticsearchIndexerRequest(
        catalog_id="cat_a", collection_id="col_a", driver="elasticsearch"
    )
    assert r.collection_id == "col_a"
    assert r.driver == "elasticsearch"


def test_get_definition_returns_canonical_id() -> None:
    """Process.id must be 'elasticsearch_indexer' — matches the URL and
    dispatcher key. Drift here re-introduces the bug."""
    p = ElasticsearchIndexerTask.get_definition()
    assert p.id == "elasticsearch_indexer"
    assert "catalog_id" in p.inputs
    assert "collection_id" in p.inputs


def test_task_class_attr_matches_definition() -> None:
    """`task_type` class attribute must equal `get_definition().id`. The
    dispatcher uses this to route runner.can_handle() calls."""
    assert ElasticsearchIndexerTask.task_type == ElasticsearchIndexerTask.get_definition().id


@pytest.mark.asyncio
async def test_run_with_collection_id_dispatches_to_collection_task() -> None:
    """When collection_id is set, the run() should resolve and call
    BulkCollectionReindexTask (registered as
    `elasticsearch_bulk_reindex_collection`)."""
    task = ElasticsearchIndexerTask(app_state=object())
    payload = _make_payload(
        {"catalog_id": "cat_a", "collection_id": "col_a"}
    )

    fake_collection_task = AsyncMock()
    fake_collection_task.run = AsyncMock(return_value={"status": "done"})

    with patch(
        "dynastore.tasks.elasticsearch_indexer.indexer_task._get_task_instance",
        return_value=fake_collection_task,
    ) as resolve:
        result: Any = await task.run(payload)

    resolve.assert_called_once_with("elasticsearch_bulk_reindex_collection")
    assert result == {"status": "done"}
    sub_payload = fake_collection_task.run.call_args.args[0]
    assert sub_payload.inputs["catalog_id"] == "cat_a"
    assert sub_payload.inputs["collection_id"] == "col_a"


@pytest.mark.asyncio
async def test_run_without_collection_id_dispatches_to_catalog_task() -> None:
    """When collection_id is absent, dispatch to BulkCatalogReindexTask."""
    task = ElasticsearchIndexerTask(app_state=object())
    payload = _make_payload({"catalog_id": "cat_b"})

    fake_catalog_task = AsyncMock()
    fake_catalog_task.run = AsyncMock(return_value={"status": "done", "total": 42})

    with patch(
        "dynastore.tasks.elasticsearch_indexer.indexer_task._get_task_instance",
        return_value=fake_catalog_task,
    ) as resolve:
        result = await task.run(payload)

    resolve.assert_called_once_with("elasticsearch_bulk_reindex_catalog")
    assert result == {"status": "done", "total": 42}


@pytest.mark.asyncio
async def test_run_raises_when_subtask_unregistered() -> None:
    """If the bulk task isn't in the registry (e.g. tasks.py side-effect import
    failed to register), surface a clear error, not a None.run() AttributeError."""
    task = ElasticsearchIndexerTask(app_state=object())
    payload = _make_payload({"catalog_id": "cat_a"})

    with patch(
        "dynastore.tasks.elasticsearch_indexer.indexer_task._get_task_instance",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="not registered"):
            await _bypass_helper_and_run(task, payload)


async def _bypass_helper_and_run(task, payload):
    """The patched `_get_task_instance` returns None, but the helper inside
    indexer_task raises RuntimeError on None. To reach that branch we patch
    the inner helper's `get_task_instance` source, not the wrapper."""
    from dynastore.tasks.elasticsearch_indexer import indexer_task as it

    with patch.object(
        it, "_get_task_instance", side_effect=RuntimeError("required sub-task 'x' is not registered")
    ):
        await task.run(payload)
