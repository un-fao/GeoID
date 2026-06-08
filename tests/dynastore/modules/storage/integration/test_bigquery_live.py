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

"""Live BQ integration — reads a tiny public dataset.

Requires ``GOOGLE_APPLICATION_CREDENTIALS`` in the environment (or
``GCLOUD_PROJECT`` + ADC). Skipped otherwise.
"""

from __future__ import annotations

import os

import pytest

_HAS_AUTH = (
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    or os.environ.get("GCLOUD_PROJECT")
)
skipif_no_gcloud = pytest.mark.skipif(
    not _HAS_AUTH, reason="no GCP credentials in env",
)


@skipif_no_gcloud
@pytest.mark.asyncio
async def test_read_entities_from_public_dataset(monkeypatch):
    from unittest.mock import AsyncMock

    from dynastore.modules.storage.drivers.bigquery import (
        ItemsBigQueryDriver,
    )
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryTarget,
        ItemsBigQueryDriverConfig,
    )

    d = ItemsBigQueryDriver()
    cfg = ItemsBigQueryDriverConfig(
        target=BigQueryTarget(
            project_id="bigquery-public-data",
            dataset_id="usa_names",
            table_name="usa_1910_2013",
        ),
        page_size=5,
    )
    monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))

    rows = []
    async for feat in d.read_entities(
        "cat",
        "col",
        limit=5,
        context={"id_column": "name", "geometry_column": None},
    ):
        rows.append(feat)
    assert len(rows) == 5


@skipif_no_gcloud
@pytest.mark.asyncio
async def test_count_entities_on_public_dataset(monkeypatch):
    from unittest.mock import AsyncMock

    from dynastore.modules.storage.drivers.bigquery import (
        ItemsBigQueryDriver,
    )
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryTarget,
        ItemsBigQueryDriverConfig,
    )

    d = ItemsBigQueryDriver()
    cfg = ItemsBigQueryDriverConfig(
        target=BigQueryTarget(
            project_id="bigquery-public-data",
            dataset_id="usa_names",
            table_name="usa_1910_2013",
        ),
    )
    monkeypatch.setattr(d, "get_driver_config", AsyncMock(return_value=cfg))
    n = await d.count_entities("cat", "col")
    assert n > 1_000_000
