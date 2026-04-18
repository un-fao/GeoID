from unittest.mock import patch

import pytest

from dynastore.modules.catalog.collection_pipeline_runner import (
    apply_collection_pipeline,
)


class _Rewrite:
    pipeline_id = "rewrite"
    priority = 100
    def can_apply(self, catalog_id, collection_id): return True
    async def apply(self, catalog_id, collection_id, collection, ctx):
        return {**collection, "stage_run": self.pipeline_id}


class _Drop:
    pipeline_id = "drop"
    priority = 50
    def can_apply(self, catalog_id, collection_id): return True
    async def apply(self, catalog_id, collection_id, collection, ctx):
        return None


class _ScopedGuard:
    """Only applies to col1."""
    pipeline_id = "col1_only"
    priority = 10
    def can_apply(self, catalog_id, collection_id): return collection_id == "col1"
    async def apply(self, catalog_id, collection_id, collection, ctx):
        return {**collection, "scoped": True}


class _Broken:
    pipeline_id = "broken"
    priority = 100
    def can_apply(self, catalog_id, collection_id): return True
    async def apply(self, catalog_id, collection_id, collection, ctx):
        raise RuntimeError("stage failure")


_PATCH_TARGET = "dynastore.tools.discovery.get_protocols"


@pytest.mark.asyncio
async def test_no_stages_returns_input_unchanged():
    with patch(_PATCH_TARGET, return_value=[]):
        out = await apply_collection_pipeline("c", "col", {"id": "col"}, {})
    assert out == {"id": "col"}


@pytest.mark.asyncio
async def test_single_rewrite_applied():
    with patch(_PATCH_TARGET, return_value=[_Rewrite()]):
        out = await apply_collection_pipeline("c", "col", {"id": "col"}, {})
    assert out == {"id": "col", "stage_run": "rewrite"}


@pytest.mark.asyncio
async def test_drop_short_circuits_and_returns_none():
    # Drop runs at priority 50 — before Rewrite at 100. Rewrite should never run.
    with patch(_PATCH_TARGET, return_value=[_Rewrite(), _Drop()]):
        out = await apply_collection_pipeline("c", "col", {"id": "col"}, {})
    assert out is None


@pytest.mark.asyncio
async def test_can_apply_gates_stage():
    # _ScopedGuard applies only to col1; col2 bypasses it.
    with patch(_PATCH_TARGET, return_value=[_ScopedGuard()]):
        out_col1 = await apply_collection_pipeline("c", "col1", {"id": "col1"}, {})
        out_col2 = await apply_collection_pipeline("c", "col2", {"id": "col2"}, {})
    assert out_col1 == {"id": "col1", "scoped": True}
    assert out_col2 == {"id": "col2"}


@pytest.mark.asyncio
async def test_broken_stage_is_skipped_not_propagated():
    with patch(_PATCH_TARGET, return_value=[_Broken(), _Rewrite()]):
        out = await apply_collection_pipeline("c", "col", {"id": "col"}, {})
    # _Broken raises; runner skips it; _Rewrite still runs.
    assert out == {"id": "col", "stage_run": "rewrite"}


@pytest.mark.asyncio
async def test_priority_order_is_respected():
    # Lower priority runs first; later stages see prior rewrites.
    class _Early:
        pipeline_id = "early"
        priority = 10
        def can_apply(self, *a, **kw): return True
        async def apply(self, cat, col, c, ctx): return {**c, "order": ["early"]}

    class _Late:
        pipeline_id = "late"
        priority = 90
        def can_apply(self, *a, **kw): return True
        async def apply(self, cat, col, c, ctx):
            return {**c, "order": c.get("order", []) + ["late"]}

    with patch(_PATCH_TARGET, return_value=[_Late(), _Early()]):
        out = await apply_collection_pipeline("c", "col", {"id": "col"}, {})
    assert out["order"] == ["early", "late"]
