"""BigQuery secondary stream adapter for /join.

Wraps Phase 4a's CollectionBigQueryDriver to expose a per-request
target (BigQuerySecondarySpec.target) as a Feature stream, indexable
by the join executor's ``index_secondary``.

Phase 4b PR-2 scope: target identity only (auth via CloudIdentityProtocol).
Phase 4e adds Secret-wrapped credential overrides.
"""

from __future__ import annotations

from typing import AsyncIterator
from unittest.mock import AsyncMock

from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import BigQuerySecondarySpec
from dynastore.modules.storage.drivers.bigquery import CollectionBigQueryDriver
from dynastore.modules.storage.drivers.bigquery_models import (
    CollectionBigQueryDriverConfig,
)


async def stream_bigquery_secondary(
    spec: BigQuerySecondarySpec,
    *,
    secondary_column: str,
    page_size: int = 1000,
    limit: int = 100_000,
    driver_factory=CollectionBigQueryDriver,
) -> AsyncIterator[Feature]:
    """Yield Features from a per-request BigQuery target.

    The target MUST be fully qualified (project_id, dataset_id,
    table_name); the resolver raises if not.
    """
    if not spec.target.is_fully_qualified():
        raise ValueError(
            "BigQuerySecondarySpec.target must include project_id, dataset_id, "
            "and table_name; partial targets are rejected at execution time.",
        )

    driver = driver_factory()
    cfg = CollectionBigQueryDriverConfig(target=spec.target, page_size=page_size)
    # Bypass the platform's config waterfall: this driver instance only
    # serves THIS request, so we monkey-patch its config resolver to
    # return the inline cfg. Phase 4a's read_entities then streams as
    # normal. The ad-hoc catalog/collection identifiers are placeholders
    # — the BQ driver only uses cfg.target for FQN construction.
    driver.get_driver_config = AsyncMock(return_value=cfg)

    async for feat in driver.read_entities(
        "_inline_", "_inline_",
        limit=limit,
        context={"id_column": secondary_column},
    ):
        yield feat
