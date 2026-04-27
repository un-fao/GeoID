"""
Asset entity sync — event-driven fan-out to AssetIndexer drivers.

``AssetEntitySyncSubscriber`` listens for ``CatalogEventType.ASSET_*`` events
emitted by ``AssetService`` and dispatches the row write/delete to every
driver registered under ``AssetRoutingConfig.operations[INDEX]`` (auto-
augmented with discoverable ``AssetIndexer`` implementors such as
``AssetElasticsearchDriver``).

This collapses the prior dual-write race where the ES driver received writes
from both the routing-config fan-out and a private listener block: the row
goes to the primary WRITE driver synchronously inside ``AssetService``; INDEX
fan-out happens via this subscriber, fed by the events outbox so failures
are replayable.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from dynastore.modules.catalog.event_service import (
    CatalogEventType,
    async_event_listener,
)

logger = logging.getLogger(__name__)


class AssetEntitySyncSubscriber:
    """Async event subscribers that drive ``AssetIndexer`` fan-out."""

    @staticmethod
    async def on_asset_upsert(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        if not catalog_id or not asset_id:
            return

        from dynastore.modules.storage.router import get_asset_index_drivers
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            return
        if not indexers:
            return

        doc = dict(payload) if isinstance(payload, dict) else {}
        doc.setdefault("asset_id", asset_id)
        doc.setdefault("catalog_id", catalog_id)
        if collection_id:
            doc.setdefault("collection_id", collection_id)

        results = await asyncio.gather(
            *(r.driver.index_asset(catalog_id, doc) for r in indexers),
            return_exceptions=True,
        )
        for r, result in zip(indexers, results):
            if isinstance(result, BaseException):
                level = (
                    logger.error if r.on_failure == FailurePolicy.FATAL
                    else logger.warning
                )
                level(
                    "AssetEntitySync: indexer '%s' index_asset failed for "
                    "%s/%s: %s", r.driver_id, catalog_id, asset_id, result,
                )

    @staticmethod
    async def on_asset_delete(
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ) -> None:
        if not asset_id:
            _val = (payload if isinstance(payload, dict) else {}).get("asset_id")
            asset_id = str(_val) if _val is not None else None
        if not catalog_id or not asset_id:
            return

        from dynastore.modules.storage.router import get_asset_index_drivers
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            indexers = await get_asset_index_drivers(catalog_id, collection_id)
        except Exception as exc:
            logger.warning(
                "AssetEntitySync: index-driver resolution failed for %s/%s: %s",
                catalog_id, asset_id, exc,
            )
            return
        if not indexers:
            return

        results = await asyncio.gather(
            *(r.driver.delete_asset(catalog_id, asset_id) for r in indexers),
            return_exceptions=True,
        )
        for r, result in zip(indexers, results):
            if isinstance(result, BaseException):
                level = (
                    logger.error if r.on_failure == FailurePolicy.FATAL
                    else logger.warning
                )
                level(
                    "AssetEntitySync: indexer '%s' delete_asset failed for "
                    "%s/%s: %s", r.driver_id, catalog_id, asset_id, result,
                )


def register_asset_entity_sync_subscriber() -> None:
    """Register ``AssetEntitySyncSubscriber`` on the global event bus.

    Wires ``CatalogEventType.ASSET_*`` to the upsert / delete handlers as
    async listeners (background dispatch, decoupled from the primary write).
    Idempotent at the registration site — duplicate registrations would
    cause duplicate dispatches but not data corruption.
    """
    async_event_listener(CatalogEventType.ASSET_CREATION)(
        AssetEntitySyncSubscriber.on_asset_upsert
    )
    async_event_listener(CatalogEventType.ASSET_UPDATE)(
        AssetEntitySyncSubscriber.on_asset_upsert
    )
    async_event_listener(CatalogEventType.ASSET_DELETION)(
        AssetEntitySyncSubscriber.on_asset_delete
    )
    async_event_listener(CatalogEventType.ASSET_HARD_DELETION)(
        AssetEntitySyncSubscriber.on_asset_delete
    )
    logger.info("AssetEntitySyncSubscriber: registered on CatalogEventType.ASSET_*")
