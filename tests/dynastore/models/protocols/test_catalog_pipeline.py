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
