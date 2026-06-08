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

import pytest

from dynastore.models.protocols.collection_pipeline import CollectionPipelineProtocol


class DropStage:
    pipeline_id = "drop"
    priority = 100

    def can_apply(self, catalog_id, collection_id):
        return True

    async def apply(self, catalog_id, collection_id, collection, context):
        return None


class MergeItemAssets:
    pipeline_id = "merge_item_assets"
    priority = 100

    def can_apply(self, catalog_id, collection_id):
        return True

    async def apply(self, catalog_id, collection_id, collection, context):
        merged = {**collection}
        existing = dict(merged.get("item_assets", {}))
        existing.setdefault(
            "default_style", {"href": "/styles/x", "roles": ["style"]},
        )
        merged["item_assets"] = existing
        return merged


def test_stages_satisfy_protocol():
    assert isinstance(DropStage(), CollectionPipelineProtocol)
    assert isinstance(MergeItemAssets(), CollectionPipelineProtocol)


@pytest.mark.asyncio
async def test_drop_returns_none():
    out = await DropStage().apply("c", "col", {"id": "col"}, {})
    assert out is None


@pytest.mark.asyncio
async def test_additive_merge_no_overwrite():
    stage = MergeItemAssets()
    coll = {"id": "col", "item_assets": {"default_style": {"href": "/existing"}}}
    out = await stage.apply("c", "col", coll, {})
    assert out is not None
    # pre-existing default_style wins (setdefault semantic)
    assert out["item_assets"]["default_style"] == {"href": "/existing"}
    # original not mutated
    assert coll["item_assets"]["default_style"] == {"href": "/existing"}


@pytest.mark.asyncio
async def test_merge_adds_default_when_missing():
    stage = MergeItemAssets()
    coll = {"id": "col"}  # no item_assets at all
    out = await stage.apply("c", "col", coll, {})
    assert out is not None
    assert out["item_assets"]["default_style"] == {
        "href": "/styles/x", "roles": ["style"],
    }
    # original not mutated
    assert "item_assets" not in coll
