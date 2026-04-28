"""
Elasticsearch bulk reindex tasks for the regular items driver.

Two task types:
  elasticsearch_bulk_reindex_catalog    — full catalog reindex (Cloud Run Job)
  elasticsearch_bulk_reindex_collection — single collection reindex (Cloud Run Job)

Both target the per-tenant index ``{prefix}-items-{catalog_id}`` (helper
:func:`dynastore.modules.elasticsearch.mappings.get_tenant_items_index`)
keyed by ``_routing=collection_id``. They are designed to be executed by
the ``geospatial-elasticsearch-indexer`` Cloud Run Job (triggered by an
admin reindex endpoint) and also run in the worker for smaller catalogs.

Per-event private tasks (``elasticsearch_private_index`` /
``elasticsearch_private_delete``) live in the private driver
subpackage at
:mod:`dynastore.modules.storage.drivers.elasticsearch_private.tasks`.
A bulk private reindex is intentionally not provided here — the
fresh-start cutover protocol (drop PG + delete ES indexes pre-deploy)
makes operator-triggered bulk reindex unnecessary for the private
driver. If one is needed it belongs in the private subpackage.
"""

import json
import logging
from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import BaseModel

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``opensearch-py`` so
# the CapabilityMap doesn't list these tasks as claimable there.
import opensearchpy  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """Fallback serializer for Decimal and other non-JSON types."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

class BulkCatalogReindexInputs(BaseModel):
    catalog_id: str
    driver: Optional[str] = None


class BulkCollectionReindexInputs(BaseModel):
    catalog_id: str
    collection_id: str
    driver: Optional[str] = None



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_es_client():
    """Return the shared singleton AsyncElasticsearch client."""
    from dynastore.modules.elasticsearch.client import get_client
    es = get_client()
    if es is None:
        raise RuntimeError(
            "Elasticsearch client is not initialized. "
            "Ensure ElasticsearchModule is registered and its lifespan has started."
        )
    return es


async def _is_es_active_for(catalog_id: str, collection_id: str) -> bool:
    """Whether the regular ES driver is currently routed for this collection."""
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig
    from dynastore.tools.discovery import get_protocol as _get_protocol

    configs = _get_protocol(ConfigsProtocol)
    if not configs:
        return False
    try:
        routing = await configs.get_config(
            CollectionRoutingConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception:
        return False
    return any(
        entry.driver_id == "ItemsElasticsearchDriver"
        for entries in routing.operations.values()
        for entry in entries
    )


async def _reindex_collection(
    es,
    catalogs_proto,
    catalog_id: str,
    collection_id: str,
    index_name: str,
    page_size: int = 500,
) -> int:
    """Stream every item of a collection from the SoR and bulk-index it
    into the per-tenant items index with ``_routing=collection_id``.

    Returns the number of documents indexed. Skips collections that don't
    currently route through the regular ES driver.
    """
    if not await _is_es_active_for(catalog_id, collection_id):
        logger.debug(
            "Skipping collection %s/%s — elasticsearch not configured as driver.",
            catalog_id, collection_id,
        )
        return 0

    total = 0
    offset = 0

    while True:
        result = await catalogs_proto.search(
            catalog_id,
            collection_id,
            limit=page_size,
            offset=offset,
        )
        features = result.get("features", [])
        if not features:
            break

        bulk_body: list = []
        for feature in features:
            item_id = getattr(feature, "id", None) or (
                feature.get("id") if isinstance(feature, dict) else None
            )
            if not item_id:
                continue

            if isinstance(feature, BaseModel):
                doc = feature.model_dump(by_alias=True, exclude_none=True, mode="json")
            else:
                doc = json.loads(json.dumps(dict(feature), default=_json_default))
            doc["catalog_id"] = catalog_id
            doc["collection"] = collection_id

            doc_id = f"{catalog_id}:{collection_id}:{item_id}"
            bulk_body.append({"index": {
                "_index": index_name,
                "_id": doc_id,
                "routing": collection_id,
            }})
            bulk_body.append(doc)

        if bulk_body:
            resp = await es.bulk(body=bulk_body, params={"timeout": "60s"})
            errors = [i for i in resp.get("items", []) if "error" in i.get("index", {})]
            if errors:
                logger.warning(
                    "Bulk index: %d errors in collection %s/%s at offset %d.",
                    len(errors), catalog_id, collection_id, offset,
                )
            total += len(bulk_body) // 2

        if len(features) < page_size:
            break
        offset += page_size

    return total


# ---------------------------------------------------------------------------
# Task: BulkCatalogReindexTask
# ---------------------------------------------------------------------------

class BulkCatalogReindexTask(TaskProtocol):
    """Reindex every collection of a catalog into the per-tenant items index.

    Iterates the catalog's collections, skips those that don't route
    through the regular ES driver, and streams each collection's items
    via the SoR into ``{prefix}-items-{catalog_id}`` with
    ``_routing=collection_id``. Stale items for the catalog are removed
    via ``delete_by_query`` before reindex begins.
    """

    task_type = "elasticsearch_bulk_reindex_catalog"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCatalogReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        index_name = get_tenant_items_index(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available in this process.")

        es = _build_es_client()

        # Wipe stale items for this catalog. delete_by_query is bounded to
        # the per-tenant index — other catalogs are unaffected.
        try:
            await es.delete_by_query(
                index=index_name,
                body={"query": {"match_all": {}}},
                params={"refresh": "false", "ignore_unavailable": "true"},
            )
        except Exception as exc:
            logger.warning(
                "BulkCatalogReindexTask: pre-reindex delete_by_query failed for "
                "%s: %s", catalog_id, exc,
            )

        total_indexed = 0
        offset, batch = 0, 50
        while True:
            collections = await catalogs_proto.list_collections(
                catalog_id, limit=batch, offset=offset
            )
            if not collections:
                break
            for collection in collections:
                collection_id = getattr(collection, "id", None)
                if not collection_id:
                    continue
                count = await _reindex_collection(
                    es, catalogs_proto, catalog_id, collection_id, index_name,
                )
                total_indexed += count
                logger.info(
                    "BulkCatalogReindexTask: %s/%s — %d docs indexed.",
                    catalog_id, collection_id, count,
                )
            if len(collections) < batch:
                break
            offset += batch

        return {
            "catalog_id": catalog_id,
            "total_indexed": total_indexed,
            "status": "done",
        }


# ---------------------------------------------------------------------------
# Task: BulkCollectionReindexTask
# ---------------------------------------------------------------------------

class BulkCollectionReindexTask(TaskProtocol):
    """Reindex one collection into the per-tenant items index.

    Triggered by the admin reindex endpoint at
    ``POST /search/reindex/catalogs/{id}/collections/{cid}``.
    """

    task_type = "elasticsearch_bulk_reindex_collection"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCollectionReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id
        index_name = get_tenant_items_index(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available.")

        es = _build_es_client()

        # Wipe stale items for just this collection.
        try:
            await es.delete_by_query(
                index=index_name,
                body={"query": {"term": {"collection": collection_id}}},
                params={
                    "routing": collection_id,
                    "refresh": "false",
                    "ignore_unavailable": "true",
                },
            )
        except Exception as exc:
            logger.warning(
                "BulkCollectionReindexTask: pre-reindex delete_by_query failed "
                "for %s/%s: %s", catalog_id, collection_id, exc,
            )

        count = await _reindex_collection(
            es, catalogs_proto, catalog_id, collection_id, index_name,
        )

        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "total_indexed": count,
            "status": "done",
        }
