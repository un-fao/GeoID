"""Per-row rejection drain for ``OGCTransactionMixin._ingest_items``.

Covers the PG path contract:

* before calling ``CatalogsProtocol.upsert`` the mixin seeds
  ``ctx.extensions["_rejections"] = []`` so the core service can record
  per-row ``SidecarRejectedError`` events without collapsing the batch;
* after the call the mixin drains that list and merges the entries into
  the ``rejections`` tuple slot the HTTP layer consumes to build a 207
  ``IngestionReport``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List

import pytest

from dynastore.extensions.ogc_base import OGCTransactionMixin, OGCServiceMixin
from dynastore.models.driver_context import DriverContext


class _FakeCatalogsSvc:
    def __init__(self, created: List[Any], rejections_out: List[dict]):
        self._created = created
        self._rejections_out = rejections_out

    async def upsert(self, catalog_id, collection_id, items, ctx):
        # Core service would record per-row rejections here. We just hand
        # the seeded out-list back to the mixin through the ctx.
        assert ctx.extensions.get("_rejections") == [], (
            "_ingest_items must seed _rejections=[] before calling upsert"
        )
        ctx.extensions["_rejections"] = list(self._rejections_out)
        return self._created


class _Svc(OGCServiceMixin, OGCTransactionMixin):
    pass


@pytest.mark.asyncio
async def test_per_row_rejections_drained_from_ctx_extensions(monkeypatch):
    svc = _Svc()
    accepted_row = SimpleNamespace(
        id="accepted-1",
        properties={"external_id": "accepted-1"},
    )
    rejected = {
        "geoid": "geoid-2",
        "external_id": "ext-2",
        "sidecar_id": "dim",
        "matcher": "external_id",
        "reason": "sidecar_not_acceptable",
        "message": "refused by dim sidecar",
    }
    svc._ogc_catalogs_protocol = _FakeCatalogsSvc(
        created=[accepted_row], rejections_out=[rejected],
    )

    ctx = DriverContext()
    payload = [{"type": "Feature", "id": "a"}, {"type": "Feature", "id": "b"}]

    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        catalog_id="cat",
        collection_id="col",
        payload=payload,
        ctx=ctx,
        policy_source="/configs/...",
    )

    assert [r.id for r in accepted_rows] == ["accepted-1"]
    assert len(rejections) == 1
    r = rejections[0]
    assert r.external_id == "ext-2"
    assert r.sidecar_id == "dim"
    assert r.matcher == "external_id"
    assert r.reason == "sidecar_not_acceptable"
    assert r.message == "refused by dim sidecar"
    assert r.policy_source == "/configs/..."
    assert was_single is False
    assert batch_size == 2
    # Drain must empty the out-list
    assert "_rejections" not in ctx.extensions


@pytest.mark.asyncio
async def test_clean_batch_yields_empty_rejections(monkeypatch):
    svc = _Svc()
    accepted_row = SimpleNamespace(id="x", properties={"external_id": "x"})
    svc._ogc_catalogs_protocol = _FakeCatalogsSvc(
        created=[accepted_row], rejections_out=[],
    )

    ctx = DriverContext()
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        catalog_id="cat",
        collection_id="col",
        payload={"type": "Feature", "id": "x"},
        ctx=ctx,
        policy_source="/configs/...",
    )

    assert [r.id for r in accepted_rows] == ["x"]
    assert rejections == []
    assert was_single is True
    assert batch_size == 1
