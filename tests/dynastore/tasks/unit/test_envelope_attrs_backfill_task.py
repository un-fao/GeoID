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

"""Unit tests for EnvelopeAttrsBackfillTask.

All stubs in-process; no live ES, no live DB.

Patch targets follow the same pattern as test_elasticsearch_bulk_reindex.py:
  - ``dynastore.tools.discovery.get_protocol``  (canonical location; all callers
    resolve through it after the lazy ``from ... import`` inside run()).
  - ``dynastore.tasks.elasticsearch_indexer.tasks.get_es_client`` (imported by
    envelope_backfill_task as ``from ...tasks import get_es_client``).
  - ``dynastore.modules.elasticsearch.client.get_index_prefix`` (imported as
    ``_get_index_prefix`` inside run()).
  - ``dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name``
    (imported inside run() for the index name).
"""
from __future__ import annotations

from typing import Dict, List
from unittest.mock import patch
from uuid import uuid4

import pytest

pytest.importorskip("opensearchpy")  # optional dep — skip when SCOPE excludes it

from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.elasticsearch_indexer.envelope_backfill_task import (
    EnvelopeAttrsBackfillTask,
    EnvelopeAttrsBackfillInputs,
    _build_bulk_update_body,
)


# ---------------------------------------------------------------------------
# Test-infrastructure helpers
# ---------------------------------------------------------------------------

def _make_payload(inputs: EnvelopeAttrsBackfillInputs) -> TaskPayload:
    return TaskPayload(
        task_id=uuid4(),
        caller_id="tests:backfill",
        inputs=inputs.model_dump(),
    )


class _FakeEs:
    def __init__(self):
        self.bulk_calls: list = []

    async def bulk(self, *, body, **kwargs):
        self.bulk_calls.append(body)
        return {"items": []}


class _FakeCatalogs:
    """Returns pages until exhausted."""

    def __init__(self, features: List[Dict], page_size: int = 10):
        self._features = features
        self._page_size = page_size

    async def search(self, catalog_id, collection_id, *, limit, offset):
        chunk = self._features[offset: offset + limit]
        return {"features": chunk}


def _fake_policy(attribute_paths: Dict[str, str]):
    class _Policy:
        pass
    p = _Policy()
    p.attribute_paths = attribute_paths  # type: ignore[attr-defined]
    return p


def _make_configs(policy=None):
    """Return a fake ConfigsProtocol whose get_config returns ``policy``."""
    class _Configs:
        async def get_config(self, model, *, catalog_id=None, collection_id=None, **k):
            return policy

    return _Configs()


def _get_protocol_factory(es_configs=None, catalogs=None):
    """Return a get_protocol side_effect that dispatches by protocol name."""
    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return es_configs
        if "CatalogsProtocol" in name:
            return catalogs
        return None
    return _get_protocol


# Common patches for tests that need ES operations
def _common_patches(es, configs, catalogs=None):
    """Return list of context managers for the common patch set."""
    return [
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs, catalogs=catalogs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="pfx",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name",
            return_value="pfx-cat1-envelope-items",
        ),
    ]


# ---------------------------------------------------------------------------
# Test: policy absent → short-circuit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_policy_skips_without_es_calls():
    """When no AttributeStampingPolicy is found the task skips with reason."""
    es = _FakeEs()
    configs = _make_configs(policy=None)

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["status"] == "skipped"
    assert "no AttributeStampingPolicy" in result["reason"]
    assert es.bulk_calls == []


@pytest.mark.asyncio
async def test_empty_attribute_paths_skips():
    """Policy present but attribute_paths is empty → skipped."""
    es = _FakeEs()
    configs = _make_configs(policy=_fake_policy({}))

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["status"] == "skipped"
    assert es.bulk_calls == []


# ---------------------------------------------------------------------------
# Test: policy present + 2 paths → correct _update body shape
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_with_two_paths_produces_correct_update_body():
    """Policy with 2 paths → ES bulk gets _update actions with both attrs keys."""
    es = _FakeEs()
    features = [
        {"id": "g1", "properties": {"dept": "finance", "region": "EU"}},
        {"id": "g2", "properties": {"dept": "legal", "region": "US"}},
    ]
    catalogs = _FakeCatalogs(features)
    configs = _make_configs(policy=_fake_policy({
        "dept": "$.properties.dept",
        "region": "$.properties.region",
    }))

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs, catalogs=catalogs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="pfx",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name",
            return_value="pfx-cat1-envelope-items",
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["status"] == "done"
    assert result["updated"] == 2
    assert result["errors"] == []

    assert len(es.bulk_calls) == 1
    body = es.bulk_calls[0]
    # 2 features × (action, doc) = 4 entries
    assert len(body) == 4

    # First pair
    action0 = body[0]["update"]
    assert action0["_index"] == "pfx-cat1-envelope-items"
    assert action0["_id"] == "g1"
    assert action0["retry_on_conflict"] == 3
    doc0 = body[1]
    assert doc0["doc_as_upsert"] is False
    assert doc0["doc"]["attrs"] == {"dept": "finance", "region": "EU"}

    # Second pair
    action1 = body[2]["update"]
    assert action1["_id"] == "g2"
    doc1 = body[3]
    assert doc1["doc"]["attrs"] == {"dept": "legal", "region": "US"}


# ---------------------------------------------------------------------------
# Test: dry_run=True → counts but no ES write
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_counts_but_no_es_write():
    """dry_run=True returns would_update count without calling es.bulk."""
    es = _FakeEs()
    features = [{"id": "g1", "properties": {}}, {"id": "g2", "properties": {}}]
    catalogs = _FakeCatalogs(features)
    configs = _make_configs(policy=_fake_policy({"k": "$.properties.k"}))

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs, catalogs=catalogs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="pfx",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name",
            return_value="pfx-cat1-envelope-items",
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(
                catalog_id="cat1", collection_id="col1", dry_run=True,
            ),
        ))

    assert result["status"] == "dry_run"
    assert result["would_update"] == 2
    assert es.bulk_calls == []


# ---------------------------------------------------------------------------
# Test: pagination correctness (batch_size=2, 5 items)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pagination_correctness_five_items_batch_two():
    """5 items with batch_size=2 → 3 ES bulk calls (2 + 2 + 1)."""
    es = _FakeEs()
    features = [
        {"id": f"g{i}", "properties": {"v": str(i)}} for i in range(5)
    ]
    catalogs = _FakeCatalogs(features)
    configs = _make_configs(policy=_fake_policy({"v": "$.properties.v"}))

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs, catalogs=catalogs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="pfx",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name",
            return_value="pfx-cat1-envelope-items",
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(
                catalog_id="cat1", collection_id="col1", batch_size=2,
            ),
        ))

    assert result["status"] == "done"
    assert result["updated"] == 5
    # 3 bulk calls: pages of [0,1], [2,3], [4]
    assert len(es.bulk_calls) == 3
    sizes = [len(call) // 2 for call in es.bulk_calls]
    assert sizes == [2, 2, 1]


# ---------------------------------------------------------------------------
# Test: error mid-stream → continues, summarises errors
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_mid_stream_continues_and_summarises():
    """An ES error on the first batch is recorded; second batch succeeds."""
    call_count = 0

    class _ErrorFirstEs:
        def __init__(self):
            self.bulk_calls = []

        async def bulk(self, *, body, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Simulate a bulk response with an error item.
                return {
                    "items": [
                        {"update": {"_id": "g0", "error": {"reason": "boom"}}},
                        {"update": {"_id": "g1", "error": None}},
                    ]
                }
            self.bulk_calls.append(body)
            return {"items": []}

    es = _ErrorFirstEs()
    features = [
        {"id": "g0", "properties": {}},
        {"id": "g1", "properties": {}},
        {"id": "g2", "properties": {}},
    ]
    catalogs = _FakeCatalogs(features, page_size=10)
    configs = _make_configs(policy=_fake_policy({"k": "$.properties.k"}))

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_get_protocol_factory(es_configs=configs, catalogs=catalogs),
        ),
        patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_es_client",
            return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="pfx",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch_envelope.mappings.get_envelope_index_name",
            return_value="pfx-cat1-envelope-items",
        ),
    ):
        task = EnvelopeAttrsBackfillTask()
        result = await task.run(_make_payload(
            EnvelopeAttrsBackfillInputs(
                catalog_id="cat1", collection_id="col1", batch_size=10,
            ),
        ))

    assert result["status"] == "done"
    # 3 items total; one error reported for g0
    assert len(result["errors"]) == 1
    assert result["errors"][0]["id"] == "g0"
    assert "boom" in result["errors"][0]["reason"]


# ---------------------------------------------------------------------------
# Test: _build_bulk_update_body helper (pure)
# ---------------------------------------------------------------------------

def test_build_bulk_update_body_shape():
    """Pure helper: produces (action, doc) pairs with correct keys."""
    from dynastore.modules.iam.stamping_config import stamp_attrs_from_feature

    features = [{"id": "g1", "properties": {"dept": "hr"}}]
    body = _build_bulk_update_body(
        features,
        index_name="idx",
        catalog_id="cat",
        collection_id="col",
        attribute_paths={"dept": "$.properties.dept"},
        stamp_fn=stamp_attrs_from_feature,
    )
    assert len(body) == 2
    action = body[0]["update"]
    assert action["_index"] == "idx"
    assert action["_id"] == "g1"
    assert action["retry_on_conflict"] == 3
    doc = body[1]
    assert doc["doc_as_upsert"] is False
    assert doc["doc"]["attrs"] == {"dept": "hr"}


def test_build_bulk_update_body_skips_missing_id():
    """Items without an id are skipped (no action emitted)."""
    from dynastore.modules.iam.stamping_config import stamp_attrs_from_feature

    features = [{"properties": {"dept": "hr"}}]  # no id
    body = _build_bulk_update_body(
        features,
        index_name="idx",
        catalog_id="cat",
        collection_id="col",
        attribute_paths={"dept": "$.properties.dept"},
        stamp_fn=stamp_attrs_from_feature,
    )
    assert body == []
