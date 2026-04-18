"""CollectionBigQueryDriver — Phase 4a READ + STREAMING implementation."""

from __future__ import annotations

import logging
from typing import Any, FrozenSet, Optional

from dynastore.models.protocols import (
    BigQueryProtocol,
    ConfigsProtocol,
)
from dynastore.modules.storage.drivers.bigquery_models import (
    CollectionBigQueryDriverConfig,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def _get_bq_service() -> Optional[BigQueryProtocol]:
    return get_protocol(BigQueryProtocol)


class CollectionBigQueryDriver:
    """BigQuery read driver (Phase 4a — READ + STREAMING capabilities)."""

    priority: int = 50

    capabilities: FrozenSet[str] = frozenset({
        "READ", "STREAMING", "INTROSPECTION", "COUNT", "AGGREGATION",
    })
    preferred_for: FrozenSet[str] = frozenset({"features"})
    supported_hints: FrozenSet[str] = frozenset({"features", "bigquery"})

    def is_available(self) -> bool:
        return _get_bq_service() is not None

    async def lifespan(self, app_state: Any) -> None:
        return None

    async def get_driver_config(
        self, catalog_id: str, collection_id: Optional[str] = None,
        *, db_resource: Optional[Any] = None,
    ) -> Optional[CollectionBigQueryDriverConfig]:
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        return await configs.get_config(
            CollectionBigQueryDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
