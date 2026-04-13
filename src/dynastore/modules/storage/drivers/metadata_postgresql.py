#    Copyright 2025 FAO
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

"""
PostgreSQL metadata driver — implements CollectionMetadataDriverProtocol.

Reads/writes collection metadata from the PG ``metadata`` table (always
available — PG is the authoritative store for the thin registry).

Capabilities: READ, WRITE, SEARCH, SOFT_DELETE.
"""

import json
import logging
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.metadata_driver import (
    CollectionMetadataDriverProtocol,
    MetadataCapability,
)
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.json import CustomJSONEncoder

logger = logging.getLogger(__name__)

# Metadata columns in the PG metadata table
_META_COLUMNS = (
    "title", "description", "keywords", "license", "links",
    "assets", "extent", "providers", "summaries", "item_assets",
    "extra_metadata", "stac_extensions",
)

METADATA_DRIVER_CONFIG_ID = "driver:collection:metadata:postgresql"


class DriverMetadataPostgresql:
    """PostgreSQL implementation of CollectionMetadataDriverProtocol.

    Uses the existing ``{schema}.metadata`` table for CRUD + search.
    """

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SEARCH,
        MetadataCapability.SOFT_DELETE,
    })

    def _get_engine(self) -> Any:
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.tools.discovery import get_protocol

        db = get_protocol(DatabaseProtocol)
        return db.engine if db else None

    async def _resolve_physical_schema(
        self, catalog_id: str, *, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.tools.discovery import get_protocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource)
        )

    @staticmethod
    def _deserialize_jsonb(row: Dict[str, Any]) -> Dict[str, Any]:
        """Parse JSONB string columns into Python objects."""
        for key in _META_COLUMNS:
            val = row.get(key)
            if isinstance(val, str):
                try:
                    row[key] = json.loads(val)
                except Exception:
                    row[key] = None
        return row

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        engine = db_resource or self._get_engine()
        if not engine:
            return None

        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return None

            sql = f'SELECT * FROM "{phys_schema}".metadata WHERE collection_id = :id;'
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=collection_id)

            if not row:
                return None

            return self._deserialize_jsonb(row)

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or self._get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")

        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                raise RuntimeError(f"No physical schema for catalog '{catalog_id}'")

            sql = f"""
                INSERT INTO "{phys_schema}".metadata
                    (collection_id, title, description, keywords, license,
                     links, assets, extent, providers, summaries, item_assets,
                     extra_metadata)
                VALUES
                    (:id, :title, :description, :keywords, :license,
                     :links, :assets, :extent, :providers, :summaries,
                     :item_assets, :extra_metadata)
                ON CONFLICT (collection_id) DO UPDATE SET
                    title          = EXCLUDED.title,
                    description    = EXCLUDED.description,
                    keywords       = EXCLUDED.keywords,
                    license        = EXCLUDED.license,
                    links          = EXCLUDED.links,
                    assets         = EXCLUDED.assets,
                    extent         = EXCLUDED.extent,
                    providers      = EXCLUDED.providers,
                    summaries      = EXCLUDED.summaries,
                    item_assets    = EXCLUDED.item_assets,
                    extra_metadata = EXCLUDED.extra_metadata;
            """
            await DDLQuery(sql).execute(
                conn,
                id=collection_id,
                title=self._to_json(metadata.get("title")),
                description=self._to_json(metadata.get("description")),
                keywords=self._to_json(metadata.get("keywords")),
                license=self._to_json(metadata.get("license")),
                links=self._to_json(metadata.get("links")),
                assets=self._to_json(metadata.get("assets")),
                extent=self._to_json(metadata.get("extent")),
                providers=self._to_json(metadata.get("providers")),
                summaries=self._to_json(metadata.get("summaries")),
                item_assets=self._to_json(metadata.get("item_assets")),
                extra_metadata=self._to_json(metadata.get("extra_metadata")),
            )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or self._get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")

        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return

            if soft:
                # Soft delete: set a deleted_at timestamp on the metadata row
                sql = f"""
                    UPDATE "{phys_schema}".metadata
                    SET extra_metadata = jsonb_set(
                        COALESCE(extra_metadata, '{{}}'::jsonb),
                        '{{_deleted_at}}',
                        to_jsonb(now()::text)
                    )
                    WHERE collection_id = :id;
                """
            else:
                sql = f'DELETE FROM "{phys_schema}".metadata WHERE collection_id = :id;'

            await DDLQuery(sql).execute(conn, id=collection_id)

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        engine = db_resource or self._get_engine()
        if not engine:
            return [], 0

        async with managed_transaction(engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return [], 0

            where_clauses = ["c.deleted_at IS NULL"]
            params: Dict[str, Any] = {"limit": limit, "offset": offset}

            if q:
                where_clauses.append(
                    "(c.id ILIKE :q OR m.title->>'en' ILIKE :q "
                    "OR m.description->>'en' ILIKE :q)"
                )
                params["q"] = f"%{q}%"

            where_sql = " AND ".join(where_clauses)

            # Count query
            count_sql = (
                f'SELECT COUNT(*) FROM "{phys_schema}".collections c '
                f'LEFT JOIN "{phys_schema}".metadata m ON m.collection_id = c.id '
                f'WHERE {where_sql};'
            )
            total = await DQLQuery(
                count_sql, result_handler=ResultHandler.SCALAR
            ).execute(conn, **params) or 0

            # Data query
            data_sql = (
                f'SELECT c.id, m.title, m.description, m.keywords, m.license, '
                f'm.links, m.assets, m.extent, m.providers, m.summaries, '
                f'm.item_assets, m.extra_metadata '
                f'FROM "{phys_schema}".collections c '
                f'LEFT JOIN "{phys_schema}".metadata m ON m.collection_id = c.id '
                f'WHERE {where_sql} '
                f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
            )
            rows = await DQLQuery(
                data_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)

            results = [self._deserialize_jsonb(row) for row in rows]
            return results, total

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return {}
        try:
            return await configs.get_config(
                METADATA_DRIVER_CONFIG_ID,
                catalog_id=catalog_id,
                ctx=DriverContext(db_resource=db_resource),
            )
        except Exception:
            return {}

    async def is_available(self) -> bool:
        return self._get_engine() is not None

    @staticmethod
    def _to_json(value: Any) -> Optional[str]:
        """Serialize a value to JSON string for PG JSONB storage."""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, cls=CustomJSONEncoder)
