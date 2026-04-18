import pytest

from dynastore.models.protocols.catalog_pipeline import CatalogPipelineProtocol


class DropStage:
    pipeline_id = "drop"
    priority = 100

    def can_apply(self, catalog_id):
        return True

    async def apply(self, catalog_id, catalog, context):
        return None


class AddSummaryStage:
    pipeline_id = "add_summary"
    priority = 100

    def can_apply(self, catalog_id):
        return True

    async def apply(self, catalog_id, catalog, context):
        return {**catalog, "summary": {"collections": 0}}


def test_stages_satisfy_protocol():
    assert isinstance(DropStage(), CatalogPipelineProtocol)
    assert isinstance(AddSummaryStage(), CatalogPipelineProtocol)


@pytest.mark.asyncio
async def test_drop_returns_none():
    out = await DropStage().apply("cat1", {"id": "cat1"}, {})
    assert out is None


@pytest.mark.asyncio
async def test_additive_rewrite_preserves_input():
    stage = AddSummaryStage()
    cat = {"id": "cat1", "title": "X"}
    out = await stage.apply("cat1", cat, {})
    assert out == {"id": "cat1", "title": "X", "summary": {"collections": 0}}
    assert cat == {"id": "cat1", "title": "X"}, "input must not be mutated"
