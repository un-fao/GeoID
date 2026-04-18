import pytest

from dynastore.models.protocols.item_pipeline import ItemPipelineProtocol


class DropStage:
    stage_id = "drop"
    priority = 100

    def can_apply(self, catalog_id, collection_id):
        return True

    async def apply(self, catalog_id, collection_id, feature, context):
        return None  # drop


class RewriteStage:
    stage_id = "rewrite"
    priority = 200

    def can_apply(self, catalog_id, collection_id):
        return True

    async def apply(self, catalog_id, collection_id, feature, context):
        return {**feature, "extra": 42}


class FilterStage:
    """Drops items where properties.secret is true."""

    stage_id = "filter"
    priority = 50

    def can_apply(self, catalog_id, collection_id):
        return True

    async def apply(self, catalog_id, collection_id, feature, context):
        if feature.get("properties", {}).get("secret"):
            return None
        return feature


def test_stages_satisfy_protocol():
    assert isinstance(DropStage(), ItemPipelineProtocol)
    assert isinstance(RewriteStage(), ItemPipelineProtocol)
    assert isinstance(FilterStage(), ItemPipelineProtocol)


@pytest.mark.asyncio
async def test_drop_semantics():
    stage = DropStage()
    out = await stage.apply("cat", "col", {"id": 1}, {})
    assert out is None


@pytest.mark.asyncio
async def test_rewrite_produces_new_dict_and_does_not_mutate_input():
    stage = RewriteStage()
    feat = {"id": 1}
    out = await stage.apply("cat", "col", feat, {})
    assert out == {"id": 1, "extra": 42}
    assert feat == {"id": 1}, "input must not be mutated"


@pytest.mark.asyncio
async def test_filter_drops_matching_items_only():
    stage = FilterStage()
    assert await stage.apply("c", "col", {"properties": {"secret": True}}, {}) is None
    kept = await stage.apply("c", "col", {"properties": {"secret": False}, "id": 2}, {})
    assert kept == {"properties": {"secret": False}, "id": 2}
