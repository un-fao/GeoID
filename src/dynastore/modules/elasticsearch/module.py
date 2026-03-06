import logging
import json
from typing import Optional, Any
from contextlib import asynccontextmanager

from dynastore.modules.protocols import ModuleProtocol
from dynastore.models.protocols.events import EventsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskCreate
from dynastore.modules.catalog.event_service import CatalogEventType

logger = logging.getLogger(__name__)


async def _get_search_index_config(catalog_id: str, collection_id: str) -> bool:
    """Return True only when the collection has search_index=True."""
    try:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return False
        col_config = await catalogs.get_collection_config(catalog_id, collection_id)
        return bool(getattr(col_config, "search_index", False))
    except Exception as e:
        logger.debug(f"Could not resolve search_index config for {catalog_id}/{collection_id}: {e}")
        return False


async def _stac_serialize_item(catalog_id: str, collection_id: str, item_id: str) -> Optional[dict]:
    """Fetch the item and serialize it as a full STAC document."""
    try:
        from dynastore.modules.catalog.item_service import ItemService
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import DbProtocol

        db = get_protocol(DbProtocol)
        item_svc = get_protocol(ItemService)
        if not item_svc:
            # Fall back to standalone instantiation
            item_svc = ItemService(engine=db)

        feature = await item_svc.get_item(catalog_id, collection_id, item_id)
        if feature is None:
            return None

        doc = feature.model_dump(by_alias=True, exclude_none=True)
        # Enrich with catalog_id and collection_id for ES filter queries
        doc["catalog_id"] = catalog_id
        doc["collection_id"] = collection_id
        return doc
    except Exception as e:
        logger.warning(f"Failed to STAC-serialize item {catalog_id}/{collection_id}/{item_id}: {e}")
        return None


async def _stac_serialize_catalog(catalog_id: str) -> Optional[dict]:
    """Serialize a catalog as a STAC dict from its metadata model."""
    try:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        model = await catalogs.get_catalog_model(catalog_id)
        if model is None:
            return None
        doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
        doc["catalog_id"] = catalog_id
        doc.setdefault("id", catalog_id)
        return doc
    except Exception as e:
        logger.warning(f"Failed to serialize catalog {catalog_id}: {e}")
        return None


async def _stac_serialize_collection(catalog_id: str, collection_id: str) -> Optional[dict]:
    """Serialize a collection as a STAC dict from its metadata model."""
    try:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        model = await catalogs.get_collection_model(catalog_id, collection_id)
        if model is None:
            return None
        doc = model.model_dump(by_alias=True, exclude_none=True) if hasattr(model, "model_dump") else {}
        doc["catalog_id"] = catalog_id
        doc["collection_id"] = collection_id
        doc.setdefault("id", collection_id)
        return doc
    except Exception as e:
        logger.warning(f"Failed to serialize collection {catalog_id}/{collection_id}: {e}")
        return None


class ElasticsearchModule(ModuleProtocol):
    priority: int = 100
    """
    Listens to domain events and dispatches indexing tasks to Elasticsearch.
    """
    priority: int = 50

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        events = get_protocol(EventsProtocol)

        if events:
            # Catalog events
            events.async_event_listener(CatalogEventType.CATALOG_CREATION)(self._on_catalog_upsert)
            events.async_event_listener(CatalogEventType.CATALOG_UPDATE)(self._on_catalog_upsert)
            events.async_event_listener(CatalogEventType.CATALOG_DELETION)(self._on_catalog_delete)
            events.async_event_listener(CatalogEventType.CATALOG_HARD_DELETION)(self._on_catalog_delete)

            # Collection events
            events.async_event_listener(CatalogEventType.COLLECTION_CREATION)(self._on_collection_upsert)
            events.async_event_listener(CatalogEventType.COLLECTION_UPDATE)(self._on_collection_upsert)
            events.async_event_listener(CatalogEventType.COLLECTION_DELETION)(self._on_collection_delete)
            events.async_event_listener(CatalogEventType.COLLECTION_HARD_DELETION)(self._on_collection_delete)

            # Item events
            events.async_event_listener(CatalogEventType.ITEM_CREATION)(self._on_item_upsert)
            events.async_event_listener(CatalogEventType.ITEM_UPDATE)(self._on_item_upsert)
            events.async_event_listener(CatalogEventType.ITEM_DELETION)(self._on_item_delete)
            events.async_event_listener(CatalogEventType.ITEM_HARD_DELETION)(self._on_item_delete)
            events.async_event_listener(CatalogEventType.BULK_ITEM_CREATION)(self._on_item_bulk_upsert)

            logger.info("ElasticsearchModule: Registered async event listeners.")
        else:
            logger.warning("ElasticsearchModule: EventsProtocol not found. Indexing events will not be captured.")

        yield

    async def _dispatch_task(self, task_type: str, inputs: Any):
        """Helper to enqueue a task into the default task schema."""
        from dynastore.models.protocols import DatabaseProtocol
        db = get_protocol(DatabaseProtocol)
        if not db:
            logger.warning(f"ElasticsearchModule: DatabaseProtocol not found. Cannot dispatch {task_type}.")
            return

        try:
            await tasks_module.create_task(
                engine=db,
                task_data=TaskCreate(
                    caller_id="system:elasticsearch",
                    task_type=task_type,
                    inputs=inputs
                ),
                schema=tasks_module.get_task_schema()
            )
        except Exception as e:
            logger.error(f"ElasticsearchModule: Failed to dispatch task {task_type}: {e}")

    # --- Async Event Handlers ---

    async def _on_catalog_upsert(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        if not catalog_id:
            return
        doc = await _stac_serialize_catalog(catalog_id)
        if doc is None:
            doc = json.loads(event.get("payload", "{}"))

        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="catalog",
                entity_id=catalog_id,
                catalog_id=catalog_id,
                payload=doc,
            ).model_dump()
        )

    async def _on_catalog_delete(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        if not catalog_id:
            return
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="catalog",
                entity_id=catalog_id,
            ).model_dump()
        )

    async def _on_collection_upsert(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        if not catalog_id or not collection_id:
            return

        doc = await _stac_serialize_collection(catalog_id, collection_id)
        if doc is None:
            doc = json.loads(event.get("payload", "{}"))

        entity_id = f"{catalog_id}:{collection_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="collection",
                entity_id=entity_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                payload=doc,
            ).model_dump()
        )

    async def _on_collection_delete(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        if not catalog_id or not collection_id:
            return
        entity_id = f"{catalog_id}:{collection_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="collection",
                entity_id=entity_id,
            ).model_dump()
        )

    async def _on_item_upsert(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        item_id = event.get("item_id")
        if not catalog_id or not collection_id or not item_id:
            return

        if not await _get_search_index_config(catalog_id, collection_id):
            return

        doc = await _stac_serialize_item(catalog_id, collection_id, item_id)
        if doc is None:
            doc = event.get("payload") or {}
            doc.update({"id": item_id, "catalog_id": catalog_id, "collection_id": collection_id})

        entity_id = f"{catalog_id}:{collection_id}:{item_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        await self._dispatch_task(
            task_type="elasticsearch_index",
            inputs=ElasticsearchIndexInputs(
                entity_type="item",
                entity_id=entity_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                item_id=item_id,
                payload=doc,
            ).model_dump()
        )

    async def _on_item_bulk_upsert(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        if not catalog_id or not collection_id:
            return

        if not await _get_search_index_config(catalog_id, collection_id):
            return

        items_subset = (event.get("payload") or {}).get("items_subset", [])
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchIndexInputs
        for item_doc in items_subset:
            item_id = item_doc.get("id")
            if not item_id:
                continue
            doc = {**item_doc, "catalog_id": catalog_id, "collection_id": collection_id}
            entity_id = f"{catalog_id}:{collection_id}:{item_id}"
            await self._dispatch_task(
                task_type="elasticsearch_index",
                inputs=ElasticsearchIndexInputs(
                    entity_type="item",
                    entity_id=entity_id,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    item_id=item_id,
                    payload=doc,
                ).model_dump()
            )

    async def _on_item_delete(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        item_id = event.get("item_id") or (event.get("payload") or {}).get("geoid")
        if not catalog_id or not collection_id or not item_id:
            return
        entity_id = f"{catalog_id}:{collection_id}:{item_id}"
        from dynastore.tasks.elasticsearch.tasks import ElasticsearchDeleteInputs
        await self._dispatch_task(
            task_type="elasticsearch_delete",
            inputs=ElasticsearchDeleteInputs(
                entity_type="item",
                entity_id=entity_id,
            ).model_dump()
        )
