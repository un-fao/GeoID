"""CollectionBigQueryDriver — Phase 4a READ + STREAMING implementation."""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional

from dynastore.models.ogc import Feature
from dynastore.models.protocols import (
    BigQueryProtocol,
    CloudIdentityProtocol,
    ConfigsProtocol,
)
from dynastore.models.protocols.field_definition import FieldDefinition
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

    async def count_entities(
        self, catalog_id: str, collection_id: str,
        *, request: Optional[Any] = None, db_resource: Optional[Any] = None,
    ) -> int:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")
        rows = await service.execute_query(
            f"SELECT COUNT(*) FROM `{cfg.target.fqn()}`",
            cfg.target.project_id,
        )
        return int(next(iter(rows[0].values()))) if rows else 0

    async def aggregate(
        self, catalog_id: str, collection_id: str,
        *, aggregation_type: str, field: Optional[str] = None,
        request: Optional[Any] = None, db_resource: Optional[Any] = None,
    ) -> Any:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")

        agg = aggregation_type.upper()
        if agg not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation_type: {aggregation_type!r}")
        if field and not _is_safe_identifier(field):
            raise ValueError(f"Unsafe field identifier: {field!r}")
        expr = "*" if agg == "COUNT" and not field else field
        rows = await service.execute_query(
            f"SELECT {agg}({expr}) FROM `{cfg.target.fqn()}`",
            cfg.target.project_id,
        )
        return next(iter(rows[0].values())) if rows else None


    async def introspect_schema(
        self, catalog_id: str, collection_id: str,
        *, db_resource: Optional[Any] = None,
    ) -> List[FieldDefinition]:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")

        client = _make_bq_client(cfg.target.project_id)
        table = client.get_table(cfg.target.fqn())
        return [_bq_field_to_field_definition(f) for f in table.schema]


def _is_safe_identifier(name: str) -> bool:
    import re
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))


def _make_bq_client(project_id: Optional[str]):
    from google.cloud import bigquery
    identity = get_protocol(CloudIdentityProtocol)
    creds = identity.get_credentials_object() if identity else None
    return bigquery.Client(project=project_id, credentials=creds)


def _bq_field_to_field_definition(f) -> FieldDefinition:
    bq_to_stac = {
        "STRING": "string", "INTEGER": "int64", "INT64": "int64",
        "FLOAT": "float64", "FLOAT64": "float64", "BOOL": "bool",
        "BOOLEAN": "bool", "TIMESTAMP": "timestamp", "DATE": "date",
        "GEOGRAPHY": "geometry",
    }
    dtype = bq_to_stac.get(f.field_type.upper(), "string")
    return FieldDefinition(
        name=f.name,
        data_type=dtype,
        required=(f.mode == "REQUIRED"),
    )
