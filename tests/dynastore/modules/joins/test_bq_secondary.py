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
from unittest.mock import AsyncMock

from dynastore.models.ogc import Feature
from dynastore.modules.joins.bq_secondary import stream_bigquery_secondary
from dynastore.modules.joins.models import BigQuerySecondarySpec
from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget


@pytest.mark.asyncio
async def test_partial_target_raises():
    spec = BigQuerySecondarySpec(target=BigQueryTarget(project_id="p"))
    with pytest.raises(ValueError):
        async for _ in stream_bigquery_secondary(spec, secondary_column="user_id"):
            pass


@pytest.mark.asyncio
async def test_streams_via_inline_driver(monkeypatch):
    spec = BigQuerySecondarySpec(
        target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
    )

    class _FakeDriver:
        def __init__(self):
            self.read_calls = []
            self.get_driver_config = AsyncMock()

        async def read_entities(self, catalog_id, collection_id, **kwargs):
            self.read_calls.append((catalog_id, collection_id, kwargs))
            yield Feature(type="Feature", id="x", geometry=None,
                          properties={"user_id": "alice", "score": 1})
            yield Feature(type="Feature", id="y", geometry=None,
                          properties={"user_id": "bob", "score": 2})

    fake = _FakeDriver()
    feats = [
        f async for f in stream_bigquery_secondary(
            spec, secondary_column="user_id",
            driver_factory=lambda: fake,
        )
    ]
    assert [f.properties["user_id"] for f in feats] == ["alice", "bob"]
    # Confirm the secondary_column was passed as the id column.
    assert fake.read_calls[0][2]["context"]["id_column"] == "user_id"
