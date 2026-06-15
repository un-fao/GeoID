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

"""ES-vs-PG index-drift report (#2044). Auth is enforced by IamMiddleware
(``/admin/.*`` policy, covered by the admin policy suite), so these tests pin
the aggregation and per-collection counting logic only — no DB or app needed."""
from __future__ import annotations

import pytest

from dynastore.extensions.admin import index_drift
from dynastore.extensions.admin.models import CollectionIndexDrift


@pytest.mark.asyncio
async def test_compute_index_drift_aggregates_active_collections_only(monkeypatch):
    # 'a' in sync, 'b' has a real ES gap, 'c' is PG-only (no ES driver).
    rows = {
        "a": CollectionIndexDrift(
            collection_id="a", es_active=True, pg_count=100, es_count=100,
            drift=0, in_sync=True,
        ),
        "b": CollectionIndexDrift(
            collection_id="b", es_active=True, pg_count=853, es_count=723,
            drift=130, in_sync=False,
        ),
        "c": CollectionIndexDrift(
            collection_id="c", es_active=False, pg_count=50, es_count=0,
            drift=50, in_sync=True,
        ),
    }

    async def _fake_ids(_cat):
        return ["a", "b", "c"]

    async def _fake_drift(_cat, cid):
        return rows[cid]

    monkeypatch.setattr(index_drift, "_collection_ids", _fake_ids)
    monkeypatch.setattr(index_drift, "_drift_for_collection", _fake_drift)

    report = await index_drift.compute_index_drift("demo-3d")

    # PG-only 'c' must not pollute the totals (apples-to-apples PG vs ES).
    assert report.total_pg == 953  # 100 (a) + 853 (b); 'c' excluded
    assert report.total_es == 823  # 100 + 723
    assert report.total_drift == 130
    assert report.in_sync is False  # 'b' is out of sync
    assert [r.collection_id for r in report.collections] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_compute_index_drift_single_collection(monkeypatch):
    async def _boom(_cat):  # _collection_ids must NOT be called for a single id
        raise AssertionError("_collection_ids called for single-collection path")

    async def _fake_drift(_cat, cid):
        return CollectionIndexDrift(
            collection_id=cid, es_active=True, pg_count=42, es_count=42,
            drift=0, in_sync=True,
        )

    monkeypatch.setattr(index_drift, "_collection_ids", _boom)
    monkeypatch.setattr(index_drift, "_drift_for_collection", _fake_drift)

    report = await index_drift.compute_index_drift("demo-3d", collection_id="only")
    assert [r.collection_id for r in report.collections] == ["only"]
    assert report.total_drift == 0
    assert report.in_sync is True


def _patch_drift_deps(monkeypatch, *, active, pg, es):
    class _Resolved:
        driver = property(lambda self: self)

        async def count_entities(self, _cat, _col):
            return pg

    resolved = _Resolved()

    async def _is_active(_cat, _col):
        return active

    async def _get_driver(_cat, _col, hints=None):
        return resolved

    async def _es_count(_es, _index, **_kw):
        return es

    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.bulk_reindex.is_es_active_for", _is_active
    )
    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_items_search_driver", _get_driver
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.items_es_ops.es_count_items", _es_count
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.bulk_reindex.get_es_client", lambda: object()
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix", lambda: "geoid"
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.mappings.get_tenant_items_index",
        lambda prefix, cat: f"{prefix}-{cat}-items",
    )


@pytest.mark.asyncio
async def test_drift_for_collection_es_active_reports_gap(monkeypatch):
    _patch_drift_deps(monkeypatch, active=True, pg=853, es=723)
    row = await index_drift._drift_for_collection("demo-3d", "rotterdam")
    assert row.es_active is True
    assert row.pg_count == 853
    assert row.es_count == 723
    assert row.drift == 130
    assert row.in_sync is False


@pytest.mark.asyncio
async def test_drift_for_collection_pg_only_is_in_sync(monkeypatch):
    # ES not active: the ES count is skipped (stays 0) and the collection is
    # in sync by definition — it must not register as a 'gap'.
    _patch_drift_deps(monkeypatch, active=False, pg=50, es=999)
    row = await index_drift._drift_for_collection("demo-3d", "pg-only")
    assert row.es_active is False
    assert row.pg_count == 50
    assert row.es_count == 0  # ES branch skipped, not the patched 999
    assert row.in_sync is True
