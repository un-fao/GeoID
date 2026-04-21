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
