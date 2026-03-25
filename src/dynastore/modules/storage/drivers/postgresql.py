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
PostgreSQL Storage Driver — wraps existing ``ItemsProtocol`` services.

Zero SQL rewrite.  All complex PG logic (sidecars, query optimizer,
ON CONFLICT, RETURNING, PostGIS) stays exactly where it is.  This driver
is a thin adapter that maps ``CollectionStorageDriverProtocol``
to the existing PG-based service layer.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.location import PostgresStorageLocationConfig

logger = logging.getLogger(__name__)


class PostgresStorageDriver(ModuleProtocol):
    """PostgreSQL storage driver — delegates to existing ItemsProtocol.

    Satisfies ``CollectionStorageDriverProtocol`` by wrapping the existing
    PG-based item services, preserving all sidecar logic, query
    optimization, and streaming.
    """

    driver_id: str = "postgresql"
    priority: int = 10
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.SOFT_DELETE,
        Capability.EXPORT,
    })

    def is_available(self) -> bool:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.items import ItemsProtocol

        return get_protocol(ItemsProtocol) is not None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("PostgresStorageDriver: started (wraps existing ItemsProtocol)")
        yield
        logger.info("PostgresStorageDriver: stopped")

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        items_svc = self._get_crud_protocol()
        result = await items_svc.upsert(
            catalog_id, collection_id, entities, db_resource=db_resource
        )
        if isinstance(result, list):
            return result
        return [result]

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        if request is None:
            request = QueryRequest(limit=limit, offset=offset, item_ids=entity_ids)
        else:
            if entity_ids is not None and request.item_ids is None:
                request = request.model_copy(update={"item_ids": entity_ids})
            if request.limit is None:
                request = request.model_copy(update={"limit": limit})
            if request.offset is None:
                request = request.model_copy(update={"offset": offset})

        query_svc = self._get_query_protocol()
        response = await query_svc.stream_items(
            catalog_id, collection_id, request, db_resource=db_resource
        )

        async for feature in response.items:
            yield feature

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "PostgresStorageDriver: soft delete for individual entities "
                "is not yet implemented. Use drop_storage(soft=True) for "
                "collection-level soft deletion."
            )
        items_svc = self._get_crud_protocol()
        total = 0
        for eid in entity_ids:
            total += await items_svc.delete_item(
                catalog_id, collection_id, eid, db_resource=db_resource
            )
        return total

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")
        logger.debug(
            "PostgresStorageDriver.ensure_storage: catalog=%s collection=%s "
            "(delegated to CatalogsProtocol lifecycle)",
            catalog_id, collection_id,
        )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")

        if soft:
            logger.info(
                "PostgresStorageDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — marking as deleted via deleted_at",
                catalog_id, collection_id,
            )

        if collection_id:
            await catalogs.delete_collection(catalog_id, collection_id)
        else:
            await catalogs.delete_catalog(catalog_id)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "PostgresStorageDriver.export_entities: use ExportFeaturesTask "
            "for async export via the task runner system."
        )

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> PostgresStorageLocationConfig:
        """Resolve PG storage coordinates via existing protocol methods."""
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.collections import CollectionsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")

        schema = await catalogs.resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        table = None
        if collection_id:
            collections = get_protocol(CollectionsProtocol)
            if collections:
                table = await collections.resolve_physical_table(
                    catalog_id, collection_id, db_resource=db_resource
                )

        return PostgresStorageLocationConfig(
            physical_schema=schema,
            physical_table=table,
        )

    # --- Internal helpers ---

    def _get_crud_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_crud import ItemCrudProtocol

        svc = get_protocol(ItemCrudProtocol)
        if not svc:
            raise RuntimeError("ItemCrudProtocol not available")
        return svc

    def _get_query_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_query import ItemQueryProtocol

        svc = get_protocol(ItemQueryProtocol)
        if not svc:
            raise RuntimeError("ItemQueryProtocol not available")
        return svc
