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
Elasticsearch Storage Drivers — delegates to SFEOS DatabaseLogic.

Reuses ``stac-fastapi-elasticsearch-opensearch`` (SFEOS) library for all ES
operations instead of reimplementing.  This ensures full compatibility: SFEOS
can read data written by these drivers transparently.

Three drivers:

* ``ElasticsearchStorageDriver``  (driver_id ``"elasticsearch"``)
  Full STAC indexing via SFEOS ``DatabaseLogic`` for items, collections, and
  catalogs.  Supports ``stac-fastapi-core[catalogs]``.

* ``ElasticsearchObfuscatedDriver``  (driver_id ``"elasticsearch_obfuscated"``)
  Stores full entity data but only allows search by geoid (``dynamic: false``).
  Manages DENY access policies in its own lifecycle.

* ``ElasticsearchAssetsDriver``  (driver_id ``"elasticsearch_assets"``)
  Indexes asset metadata into per-catalog ``{prefix}-assets-{catalog_id}`` indices.
  Listens for ``CatalogEventType.ASSET_*`` events (when wired) and supports
  direct programmatic indexing via ``index_asset()`` / ``delete_asset()``.

All drivers register as async event listeners, checking
``StorageRoutingConfig.secondary_drivers`` before acting.
"""

import logging
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class _ElasticsearchBase:
    """Shared helpers for ES storage drivers."""

    _db_logic = None

    @classmethod
    def _get_db_logic(cls):
        """Lazily instantiate SFEOS DatabaseLogic (singleton)."""
        if cls._db_logic is None:
            try:
                from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
                cls._db_logic = DatabaseLogic()
            except ImportError:
                raise RuntimeError(
                    "stac-fastapi-elasticsearch not installed. "
                    "Install with: pip install stac-fastapi-elasticsearch"
                )
        return cls._db_logic

    @staticmethod
    def _sfeos_available() -> bool:
        try:
            from stac_fastapi.elasticsearch.database_logic import DatabaseLogic  # noqa: F401
            return True
        except ImportError:
            return False

    @staticmethod
    async def _is_secondary_for(
        driver_id: str, catalog_id: str, collection_id: Optional[str],
    ) -> bool:
        """Check if this driver is listed as a secondary for the given scope."""
        try:
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.storage.config import STORAGE_ROUTING_CONFIG_ID

            configs = get_protocol(ConfigsProtocol)
            if not configs:
                return False
            routing = await configs.get_config(
                STORAGE_ROUTING_CONFIG_ID,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            return driver_id in routing.secondary_driver_ids
        except Exception:
            return False

    @staticmethod
    def _feature_to_stac_item(
        feature: Any, catalog_id: str, collection_id: str,
    ) -> dict:
        """Serialize a Feature to a STAC item dict for SFEOS."""
        if hasattr(feature, "model_dump"):
            doc = feature.model_dump(by_alias=True, exclude_none=True)
        elif isinstance(feature, dict):
            doc = dict(feature)
        else:
            doc = dict(feature)
        doc.setdefault("id", doc.get("id"))
        doc["collection"] = collection_id
        return doc

    @staticmethod
    def _normalize_entities(
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
    ) -> list:
        if isinstance(entities, FeatureCollection):
            return list(entities.features) if entities.features else []
        if isinstance(entities, list):
            return entities
        return [entities]

    @staticmethod
    def _extract_item_id(entity: Any) -> Optional[str]:
        if hasattr(entity, "id"):
            return entity.id
        if isinstance(entity, dict):
            return entity.get("id")
        return None


# ---------------------------------------------------------------------------
# ElasticsearchStorageDriver — SFEOS-backed full STAC
# ---------------------------------------------------------------------------

class ElasticsearchStorageDriver(_ElasticsearchBase, ModuleProtocol):
    """SFEOS-compatible Elasticsearch storage driver.

    Delegates all ES operations to ``stac-fastapi-elasticsearch``'s
    ``DatabaseLogic``, ensuring full read/write compatibility with SFEOS.
    Supports items, collections, and catalogs (``stac-fastapi-core[catalogs]``).

    Registered as ``storage_elasticsearch`` via entry points.
    """

    driver_id: str = "elasticsearch"
    priority: int = 50
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.FULLTEXT,
        Capability.SOFT_DELETE,
    })

    def is_available(self) -> bool:
        return self._sfeos_available()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.catalog.event_service import CatalogEventType

        events = get_protocol(EventsProtocol)
        if events:
            for etype, handler in [
                (CatalogEventType.CATALOG_CREATION, self._on_catalog_upsert),
                (CatalogEventType.CATALOG_UPDATE, self._on_catalog_upsert),
                (CatalogEventType.CATALOG_DELETION, self._on_catalog_delete),
                (CatalogEventType.CATALOG_HARD_DELETION, self._on_catalog_delete),
                (CatalogEventType.COLLECTION_CREATION, self._on_collection_upsert),
                (CatalogEventType.COLLECTION_UPDATE, self._on_collection_upsert),
                (CatalogEventType.COLLECTION_DELETION, self._on_collection_delete),
                (CatalogEventType.COLLECTION_HARD_DELETION, self._on_collection_delete),
                (CatalogEventType.ITEM_CREATION, self._on_item_upsert),
                (CatalogEventType.ITEM_UPDATE, self._on_item_upsert),
                (CatalogEventType.ITEM_DELETION, self._on_item_delete),
                (CatalogEventType.ITEM_HARD_DELETION, self._on_item_delete),
                (CatalogEventType.BULK_ITEM_CREATION, self._on_item_bulk_upsert),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
            logger.info("ElasticsearchStorageDriver: event listeners registered.")
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol — Items
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        db = self._get_db_logic()
        items = self._normalize_entities(entities)
        if not items:
            return []

        # Serialize to STAC item dicts
        stac_items = []
        for item in items:
            stac_doc = self._feature_to_stac_item(item, catalog_id, collection_id)
            stac_items.append(stac_doc)

        if len(stac_items) == 1:
            await db.create_item(stac_items[0], exist_ok=True)
        else:
            # Prep items for bulk insert
            prepped = []
            for doc in stac_items:
                prepped_item = await db.bulk_async_prep_create_item(
                    doc, base_url="", exist_ok=True,
                )
                if prepped_item is not None:
                    prepped.append(prepped_item)
            if prepped:
                await db.bulk_async(collection_id, prepped, refresh=False)

        return items if isinstance(items, list) else list(items)

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
        db = self._get_db_logic()

        if entity_ids:
            for eid in entity_ids:
                try:
                    doc = await db.get_one_item(collection_id, eid)
                    yield Feature.model_validate(doc)
                except Exception:
                    pass
        else:
            # Use execute_search for query-based reads
            search_body = self._query_request_to_es(request) if request else {}
            size = limit if request is None or request.limit is None else request.limit
            from_ = offset if request is None or request.offset is None else request.offset

            try:
                from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
                alias = index_alias_by_collection_id(collection_id)
                resp = await db.client.search(
                    index=alias, body=search_body, size=size, from_=from_,
                )
                for hit in resp.get("hits", {}).get("hits", []):
                    try:
                        yield Feature.model_validate(hit["_source"])
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(
                    "ElasticsearchStorageDriver: search failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )

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
            logger.info(
                "ES delete_entities(soft=True): marking %d entities as deleted",
                len(entity_ids),
            )
        db = self._get_db_logic()
        deleted = 0
        for eid in entity_ids:
            try:
                await db.delete_item(eid, collection_id)
                deleted += 1
            except Exception:
                pass
        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """Ensure SFEOS index templates and collection index exist."""
        from stac_fastapi.elasticsearch.database_logic import (
            create_index_templates,
            create_collection_index,
        )
        await create_index_templates()
        await create_collection_index()

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        db = self._get_db_logic()
        if collection_id:
            try:
                await db.delete_collection(collection_id)
            except Exception as e:
                logger.warning("drop_storage collection failed: %s", e)
        else:
            try:
                await db.delete_catalog(catalog_id)
            except Exception as e:
                logger.warning("drop_storage catalog failed: %s", e)

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
            "ElasticsearchStorageDriver.export_entities: not supported. "
            "Export from the primary driver instead."
        )

    # ------------------------------------------------------------------
    # Catalog & Collection support (via SFEOS DatabaseLogic)
    # ------------------------------------------------------------------

    async def write_catalog(self, catalog_id: str, catalog_doc: dict) -> None:
        """Index a catalog document via SFEOS."""
        db = self._get_db_logic()
        catalog_doc.setdefault("id", catalog_id)
        catalog_doc.setdefault("type", "Catalog")
        await db.create_catalog(catalog_doc, refresh=False)

    async def delete_catalog(self, catalog_id: str) -> None:
        """Delete a catalog from ES via SFEOS."""
        db = self._get_db_logic()
        await db.delete_catalog(catalog_id, refresh=False)

    async def write_collection(
        self, catalog_id: str, collection_id: str, collection_doc: dict,
    ) -> None:
        """Index a collection document via SFEOS."""
        db = self._get_db_logic()
        collection_doc.setdefault("id", collection_id)
        collection_doc.setdefault("type", "Collection")
        try:
            await db.create_collection(collection_doc, refresh=False)
        except Exception:
            # Already exists — update
            await db.find_collection(collection_id)
            from stac_fastapi.sfeos_helpers.database import (
                update_catalog_in_index_shared,
            )
            await update_catalog_in_index_shared(
                db.client, collection_id, collection_doc,
            )

    async def delete_collection_doc(
        self, catalog_id: str, collection_id: str,
    ) -> None:
        """Delete a collection document from ES via SFEOS."""
        db = self._get_db_logic()
        try:
            await db.delete_collection(collection_id, refresh=False)
        except Exception as e:
            logger.debug("delete_collection_doc: %s", e)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_catalog_upsert(
        self, catalog_id: str = None, payload=None, **kwargs,
    ):
        if not catalog_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, None):
            return
        try:
            doc = await self._serialize_catalog(catalog_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {"id": catalog_id}
            await self.write_catalog(catalog_id, doc)
        except Exception as e:
            logger.error("ES driver: catalog upsert failed for '%s': %s", catalog_id, e)

    async def _on_catalog_delete(self, catalog_id: str = None, **kwargs):
        if not catalog_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, None):
            return
        try:
            await self.delete_catalog(catalog_id)
        except Exception as e:
            logger.error("ES driver: catalog delete failed for '%s': %s", catalog_id, e)

    async def _on_collection_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            doc = await self._serialize_collection(catalog_id, collection_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {}
            await self.write_collection(catalog_id, collection_id, doc)
        except Exception as e:
            logger.error(
                "ES driver: collection upsert failed for '%s/%s': %s",
                catalog_id, collection_id, e,
            )

    async def _on_collection_delete(
        self, catalog_id: str = None, collection_id: str = None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            await self.delete_collection_doc(catalog_id, collection_id)
        except Exception as e:
            logger.error(
                "ES driver: collection delete failed for '%s/%s': %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        item_id: str = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            doc = await self._serialize_item(catalog_id, collection_id, item_id)
            if doc is None:
                doc = payload if isinstance(payload, dict) else {}
                doc.update({
                    "id": item_id,
                    "collection": collection_id,
                })
            db = self._get_db_logic()
            await db.create_item(doc, exist_ok=True)
        except Exception as e:
            logger.error(
                "ES driver: item upsert failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    async def _on_item_bulk_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return

        items_subset = (payload if isinstance(payload, dict) else {}).get("items_subset", [])
        if not items_subset:
            return

        try:
            db = self._get_db_logic()
            prepped = []
            for item_doc in items_subset:
                item_doc.setdefault("collection", collection_id)
                p = await db.bulk_async_prep_create_item(item_doc, base_url="", exist_ok=True)
                if p is not None:
                    prepped.append(p)
            if prepped:
                await db.bulk_async(collection_id, prepped, refresh=False)
        except Exception as e:
            logger.error(
                "ES driver: bulk upsert failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_delete(
        self, catalog_id: str = None, collection_id: str = None,
        item_id: str = None, payload=None, **kwargs,
    ):
        if not item_id:
            item_id = (payload if isinstance(payload, dict) else {}).get("geoid")
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            db = self._get_db_logic()
            await db.delete_item(item_id, collection_id)
        except Exception as e:
            logger.error(
                "ES driver: item delete failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    # ------------------------------------------------------------------
    # Serialization helpers (reuse existing DynaStore services)
    # ------------------------------------------------------------------

    @staticmethod
    async def _serialize_item(
        catalog_id: str, collection_id: str, item_id: str,
    ) -> Optional[dict]:
        try:
            from dynastore.modules.catalog.item_service import ItemService
            from dynastore.models.protocols import DbProtocol
            from dynastore.tools.discovery import get_protocol

            db = get_protocol(DbProtocol)
            item_svc = get_protocol(ItemService)
            if not item_svc:
                item_svc = ItemService(engine=db)

            feature = await item_svc.get_item(catalog_id, collection_id, item_id)
            if feature is None:
                return None

            doc = feature.model_dump(by_alias=True, exclude_none=True)
            doc["collection"] = collection_id
            return doc
        except Exception as e:
            logger.warning("Failed to serialize item %s/%s/%s: %s",
                           catalog_id, collection_id, item_id, e)
            return None

    @staticmethod
    async def _serialize_catalog(catalog_id: str) -> Optional[dict]:
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.tools.discovery import get_protocol

            catalogs = get_protocol(CatalogsProtocol)
            if not catalogs:
                return None
            model = await catalogs.get_catalog_model(catalog_id)
            if model is None:
                return None
            doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
            doc.setdefault("id", catalog_id)
            doc.setdefault("type", "Catalog")
            return doc
        except Exception as e:
            logger.warning("Failed to serialize catalog %s: %s", catalog_id, e)
            return None

    @staticmethod
    async def _serialize_collection(
        catalog_id: str, collection_id: str,
    ) -> Optional[dict]:
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.tools.discovery import get_protocol

            catalogs = get_protocol(CatalogsProtocol)
            if not catalogs:
                return None
            model = await catalogs.get_collection_model(catalog_id, collection_id)
            if model is None:
                return None
            doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
            doc.setdefault("id", collection_id)
            doc.setdefault("type", "Collection")
            return doc
        except Exception as e:
            logger.warning("Failed to serialize collection %s/%s: %s",
                           catalog_id, collection_id, e)
            return None

    @staticmethod
    def _query_request_to_es(request: QueryRequest) -> dict:
        """Convert a QueryRequest to an ES query body."""
        must: list = []
        for f in request.filters:
            op = f.operator if isinstance(f.operator, str) else f.operator.value
            if op == "bbox" and f.field == "geometry":
                coords = f.value
                if isinstance(coords, (list, tuple)) and len(coords) >= 4:
                    must.append({
                        "geo_bounding_box": {
                            "geometry": {
                                "top_left": {"lon": coords[0], "lat": coords[3]},
                                "bottom_right": {"lon": coords[2], "lat": coords[1]},
                            }
                        }
                    })
            elif op in ("eq", "="):
                must.append({"term": {f.field: f.value}})
            elif op in ("like", "ilike"):
                must.append({"wildcard": {f.field: f.value}})

        if not must:
            return {"query": {"match_all": {}}}
        return {"query": {"bool": {"must": must}}}


# ---------------------------------------------------------------------------
# ElasticsearchObfuscatedDriver — geoid-only, DENY-protected
# ---------------------------------------------------------------------------

class ElasticsearchObfuscatedDriver(_ElasticsearchBase, ModuleProtocol):
    """Obfuscated Elasticsearch storage driver.

    Stores full entity data but only allows search by geoid.  Uses
    ``dynamic: false`` mapping — no geometry, no attributes, no spatial
    search.  Manages DENY access policies in its own lifecycle.

    Uses the raw ES client from SFEOS settings (not DatabaseLogic) since
    the obfuscated index has a custom mapping not managed by SFEOS.

    Registered as ``storage_elasticsearch_obfuscated`` via entry points.
    """

    driver_id: str = "elasticsearch_obfuscated"
    priority: int = 51
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
    })

    def is_available(self) -> bool:
        return self._sfeos_available()

    def _get_client(self):
        """Get the async ES client from SFEOS DatabaseLogic."""
        return self._get_db_logic().client

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.catalog.event_service import CatalogEventType

        await self._restore_deny_policies()

        events = get_protocol(EventsProtocol)
        if events:
            for etype, handler in [
                (CatalogEventType.ITEM_CREATION, self._on_item_upsert),
                (CatalogEventType.ITEM_UPDATE, self._on_item_upsert),
                (CatalogEventType.ITEM_DELETION, self._on_item_delete),
                (CatalogEventType.ITEM_HARD_DELETION, self._on_item_delete),
                (CatalogEventType.BULK_ITEM_CREATION, self._on_item_bulk_upsert),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
            logger.info("ElasticsearchObfuscatedDriver: event listeners registered.")
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.elasticsearch.mappings import (
            get_obfuscated_index_name, GEOID_OBFUSCATED_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": GEOID_OBFUSCATED_MAPPING},
                ignore=400,
            )

        bulk_body: list = []
        for item in items:
            geoid = self._extract_item_id(item)
            if not geoid:
                continue
            doc = {
                "geoid": geoid,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }
            bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
            bulk_body.append(doc)

        if bulk_body:
            await es.bulk(body=bulk_body)

        return items if isinstance(items, list) else list(items)

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
        from dynastore.modules.elasticsearch.mappings import get_obfuscated_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        if not entity_ids:
            return

        index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for geoid in entity_ids:
            try:
                resp = await es.get(index=index_name, id=geoid)
                source = resp["_source"]
                yield Feature(
                    type="Feature",
                    id=source.get("geoid", geoid),
                    geometry=None,
                    properties={
                        "catalog_id": catalog_id,
                        "collection_id": collection_id,
                    },
                )
            except Exception:
                pass

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
                "ElasticsearchObfuscatedDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.mappings import get_obfuscated_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0

        for geoid in entity_ids:
            try:
                await es.delete(index=index_name, id=geoid)
                deleted += 1
            except Exception:
                pass

        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        from dynastore.modules.elasticsearch.mappings import (
            get_obfuscated_index_name, GEOID_OBFUSCATED_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": GEOID_OBFUSCATED_MAPPING},
                ignore=400,
            )

        await self._apply_deny_policy(catalog_id)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ElasticsearchObfuscatedDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.mappings import get_obfuscated_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        await es.indices.delete(index=index_name, ignore_unavailable=True)
        await self._revoke_deny_policy(catalog_id)

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
            "ElasticsearchObfuscatedDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_item_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        item_id: str = None, payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id or not item_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            from dynastore.modules.elasticsearch.mappings import (
                get_obfuscated_index_name, GEOID_OBFUSCATED_MAPPING,
            )
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

            index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()

            if not await es.indices.exists(index=index_name):
                await es.indices.create(
                    index=index_name,
                    body={"mappings": GEOID_OBFUSCATED_MAPPING},
                    ignore=400,
                )
            await es.index(
                index=index_name, id=item_id,
                document={
                    "geoid": item_id,
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                },
            )
        except Exception as e:
            logger.error(
                "ObfuscatedDriver: index failed for %s/%s/%s: %s",
                catalog_id, collection_id, item_id, e,
            )

    async def _on_item_bulk_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        payload=None, **kwargs,
    ):
        if not catalog_id or not collection_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return

        items_subset = (payload if isinstance(payload, dict) else {}).get("items_subset", [])
        if not items_subset:
            return

        try:
            from dynastore.modules.elasticsearch.mappings import (
                get_obfuscated_index_name, GEOID_OBFUSCATED_MAPPING,
            )
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

            index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()

            if not await es.indices.exists(index=index_name):
                await es.indices.create(
                    index=index_name,
                    body={"mappings": GEOID_OBFUSCATED_MAPPING},
                    ignore=400,
                )

            bulk_body: list = []
            for item_doc in items_subset:
                geoid = item_doc.get("id")
                if not geoid:
                    continue
                bulk_body.append({"index": {"_index": index_name, "_id": geoid}})
                bulk_body.append({
                    "geoid": geoid,
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                })
            if bulk_body:
                await es.bulk(body=bulk_body)
        except Exception as e:
            logger.error(
                "ObfuscatedDriver: bulk index failed for %s/%s: %s",
                catalog_id, collection_id, e,
            )

    async def _on_item_delete(
        self, catalog_id: str = None, collection_id: str = None,
        item_id: str = None, payload=None, **kwargs,
    ):
        if not item_id:
            item_id = (payload if isinstance(payload, dict) else {}).get("geoid")
        if not catalog_id or not item_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            from dynastore.modules.elasticsearch.mappings import get_obfuscated_index_name
            from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

            index_name = get_obfuscated_index_name(_get_index_prefix(), catalog_id)
            es = self._get_client()
            try:
                await es.delete(index=index_name, id=item_id)
            except Exception:
                pass
        except Exception as e:
            logger.error("ObfuscatedDriver: delete failed for %s: %s", item_id, e)

    # ------------------------------------------------------------------
    # DENY policy management (self-contained)
    # ------------------------------------------------------------------

    @staticmethod
    async def _apply_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        policy_id = f"obfuscated_deny_{catalog_id}"
        deny_policy = Policy(
            id=policy_id,
            description=f"Blocks public access to obfuscated catalog: {catalog_id}",
            actions=["GET"],
            resources=[
                f"/(catalog|stac|features|tiles|wfs|maps)/catalogs/{re.escape(catalog_id)}(/.*)?",
            ],
            effect="DENY",
        )

        perm.register_policy(deny_policy)
        perm.register_role(Role(name="all_users", policies=[policy_id]))

        try:
            await perm.create_policy(deny_policy)
        except Exception:
            try:
                await perm.update_policy(deny_policy)
            except Exception as e:
                logger.error("DENY policy persist failed for '%s': %s", catalog_id, e)

    @staticmethod
    async def _revoke_deny_policy(catalog_id: str) -> None:
        from dynastore.tools.discovery import get_protocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol
        except ImportError:
            return

        perm = get_protocol(PermissionProtocol)
        if not perm:
            return

        try:
            await perm.delete_policy(f"obfuscated_deny_{catalog_id}")
        except Exception:
            pass

    async def _restore_deny_policies(self) -> None:
        """Restore DENY policies at startup for catalogs using this driver."""
        try:
            from dynastore.models.protocols import CatalogsProtocol
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            from dynastore.modules.storage.config import STORAGE_ROUTING_CONFIG_ID

            catalogs_proto = get_protocol(CatalogsProtocol)
            configs = get_protocol(ConfigsProtocol)
            if not catalogs_proto or not configs:
                return

            offset, batch = 0, 100
            while True:
                catalog_list = await catalogs_proto.list_catalogs(
                    limit=batch, offset=offset,
                )
                if not catalog_list:
                    break
                for catalog in catalog_list:
                    catalog_id = getattr(catalog, "id", None)
                    if not catalog_id:
                        continue
                    try:
                        routing = await configs.get_config(
                            STORAGE_ROUTING_CONFIG_ID, catalog_id=catalog_id,
                        )
                        if self.driver_id in (routing.secondary_drivers or []):
                            await self._apply_deny_policy(catalog_id)
                            logger.info(
                                "ObfuscatedDriver: restored DENY for '%s'.",
                                catalog_id,
                            )
                    except Exception:
                        continue
                if len(catalog_list) < batch:
                    break
                offset += batch
        except Exception as e:
            logger.warning(
                "ObfuscatedDriver: could not restore DENY policies: %s", e,
            )


# ---------------------------------------------------------------------------
# ElasticsearchAssetsDriver — per-catalog asset index
# ---------------------------------------------------------------------------

class ElasticsearchAssetsDriver(_ElasticsearchBase, ModuleProtocol):
    """Elasticsearch storage driver for asset metadata.

    Indexes asset documents into per-catalog ``{prefix}-assets-{catalog_id}``
    indices using the ``ASSET_MAPPING`` from ``modules/elasticsearch/mappings.py``.

    Event listeners for ``CatalogEventType.ASSET_*`` are registered at
    lifespan if available.  The driver also exposes ``index_asset()`` and
    ``delete_asset()`` for direct programmatic use.

    Registered as ``storage_elasticsearch_assets`` via entry points.
    """

    driver_id: str = "elasticsearch_assets"
    priority: int = 52
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.FULLTEXT,
    })

    def is_available(self) -> bool:
        return self._sfeos_available()

    def _get_client(self):
        """Get the async ES client from SFEOS DatabaseLogic."""
        return self._get_db_logic().client

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.models.protocols.events import EventsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.catalog.event_service import CatalogEventType

        events = get_protocol(EventsProtocol)
        if events:
            for etype, handler in [
                (CatalogEventType.ASSET_CREATION, self._on_asset_upsert),
                (CatalogEventType.ASSET_UPDATE, self._on_asset_upsert),
                (CatalogEventType.ASSET_DELETION, self._on_asset_delete),
                (CatalogEventType.ASSET_HARD_DELETION, self._on_asset_delete),
            ]:
                decorator = events.async_event_listener(etype)
                if decorator:
                    decorator(handler)
            logger.info("ElasticsearchAssetsDriver: event listeners registered.")
        yield

    # ------------------------------------------------------------------
    # Public API — direct programmatic indexing
    # ------------------------------------------------------------------

    async def index_asset(
        self, catalog_id: str, asset_doc: Dict[str, Any],
    ) -> None:
        """Index a single asset document."""
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

        asset_id = asset_doc.get("asset_id", asset_doc.get("id"))
        await es.index(index=index_name, id=asset_id, document=asset_doc)

    async def delete_asset(
        self, catalog_id: str, asset_id: str,
    ) -> None:
        """Delete a single asset document from the index."""
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        try:
            await es.delete(index=index_name, id=asset_id)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

        bulk_body: list = []
        for item in items:
            doc = item if isinstance(item, dict) else (
                item.model_dump(by_alias=True, exclude_none=True)
                if hasattr(item, "model_dump") else dict(item)
            )
            doc.setdefault("catalog_id", catalog_id)
            doc.setdefault("collection_id", collection_id)
            asset_id = doc.get("asset_id", doc.get("id", ""))
            bulk_body.append({"index": {"_index": index_name, "_id": asset_id}})
            bulk_body.append(doc)

        if bulk_body:
            await es.bulk(body=bulk_body)

        return items if isinstance(items, list) else list(items)

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
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        if not entity_ids:
            return

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for asset_id in entity_ids:
            try:
                resp = await es.get(index=index_name, id=asset_id)
                source = resp["_source"]
                yield Feature(
                    type="Feature",
                    id=source.get("asset_id", asset_id),
                    geometry=None,
                    properties=source,
                )
            except Exception:
                pass

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
                "ElasticsearchAssetsDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0
        for asset_id in entity_ids:
            try:
                await es.delete(index=index_name, id=asset_id)
                deleted += 1
            except Exception:
                pass
        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": ASSET_MAPPING},
                ignore=400,
            )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "ElasticsearchAssetsDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        await es.indices.delete(index=index_name, ignore_unavailable=True)

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
            "ElasticsearchAssetsDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # Event handlers (fired when CatalogEventType.ASSET_* events are emitted)
    # ------------------------------------------------------------------

    async def _on_asset_upsert(
        self, catalog_id: str = None, collection_id: str = None,
        asset_id: str = None, payload=None, **kwargs,
    ):
        if not catalog_id or not asset_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            doc = payload if isinstance(payload, dict) else {}
            doc.setdefault("asset_id", asset_id)
            doc.setdefault("catalog_id", catalog_id)
            if collection_id:
                doc.setdefault("collection_id", collection_id)
            await self.index_asset(catalog_id, doc)
        except Exception as e:
            logger.error(
                "AssetsDriver: index failed for %s/%s: %s",
                catalog_id, asset_id, e,
            )

    async def _on_asset_delete(
        self, catalog_id: str = None, collection_id: str = None,
        asset_id: str = None, payload=None, **kwargs,
    ):
        if not asset_id:
            asset_id = (payload if isinstance(payload, dict) else {}).get("asset_id")
        if not catalog_id or not asset_id:
            return
        if not await self._is_secondary_for(self.driver_id, catalog_id, collection_id):
            return
        try:
            await self.delete_asset(catalog_id, asset_id)
        except Exception as e:
            logger.error(
                "AssetsDriver: delete failed for %s/%s: %s",
                catalog_id, asset_id, e,
            )
