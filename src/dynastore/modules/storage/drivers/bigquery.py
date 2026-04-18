"""CollectionBigQueryDriver — Phase 4a READ + STREAMING implementation."""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional

from dynastore.models.ogc import Feature
from dynastore.models.protocols import (
    BigQueryProtocol,
    ConfigsProtocol,
)
from dynastore.modules.storage.drivers.bigquery_models import (
    CollectionBigQueryDriverConfig,
)
from dynastore.modules.storage.drivers.bigquery_stream import (
    paged_feature_stream,
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

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError(
                f"BigQuery driver requires a fully-qualified target "
                f"(project_id, dataset_id, table_name) for "
                f"{catalog_id}/{collection_id}",
            )
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")

        fqn = cfg.target.fqn()
        id_col = (context or {}).get("id_column", "id")
        geom_col = (context or {}).get("geometry_column")
        select_cols = (context or {}).get("select_columns", "*")
        where = (context or {}).get("where_clause")
        base_query = f"SELECT {select_cols} FROM `{fqn}`"
        if where:
            base_query += f" WHERE {where}"

        async for feat in paged_feature_stream(
            service.execute_query,
            project_id=cfg.target.project_id,
            base_query=base_query,
            id_column=id_col,
            geometry_column=geom_col,
            page_size=cfg.page_size,
            max_items=limit,
        ):
            yield feat
