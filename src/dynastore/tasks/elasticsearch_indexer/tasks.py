"""
Elasticsearch bulk reindex and obfuscated index tasks.

Four task types:
  elasticsearch_bulk_reindex_catalog    — full catalog reindex (Cloud Run Job)
  elasticsearch_bulk_reindex_collection — single collection reindex (Cloud Run Job)
  elasticsearch_obfuscated_index        — index one item as {geoid, catalog_id, collection_id}
  elasticsearch_obfuscated_delete       — remove one geoid doc from the obfuscated index

BulkCatalogReindexTask and BulkCollectionReindexTask are designed to be
executed by the `geospatial-elasticsearch-indexer` Cloud Run Job, which is
triggered by the admin endpoint POST /search/reindex/catalogs/{id}.
They also run in the worker for smaller catalogs.

Mode values:
  "obfuscated" — write to {prefix}-geoid-{catalog_id}, only {geoid, catalog_id, collection_id}
  "catalog"    — write to the normal items index; skips collections with search_index=False
"""

import logging
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

class BulkCatalogReindexInputs(BaseModel):
    catalog_id: str
    mode: Literal["catalog", "obfuscated"] = "catalog"
    driver: Optional[str] = None


class BulkCollectionReindexInputs(BaseModel):
    catalog_id: str
    collection_id: str
    mode: Literal["catalog", "obfuscated"] = "catalog"
    driver: Optional[str] = None


class ObfuscatedIndexInputs(BaseModel):
    geoid: str
    catalog_id: str
    collection_id: str


class ObfuscatedDeleteInputs(BaseModel):
    geoid: str
    catalog_id: str


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


async def _reindex_collection(
    es,
    catalogs_proto,
    catalog_id: str,
    collection_id: str,
    mode: str,
    stac_index: str,
    obfuscated_index: str,
    page_size: int = 500,
) -> int:
    """
    Stream all items in a collection from AlloyDB and bulk-index into ES.
    Returns the number of documents indexed.
    """
    from dynastore.modules.elasticsearch.mappings import GEOID_OBFUSCATED_MAPPING

    if mode == "catalog":
        col_config = await catalogs_proto.get_collection_config(catalog_id, collection_id)
        if not getattr(col_config, "search_index", False):
            logger.debug(
                "Skipping collection %s/%s — search_index=False.",
                catalog_id, collection_id,
            )
            return 0

    total = 0
    offset = 0

    while True:
        features = await catalogs_proto.get_features(
            catalogs_proto,   # conn: ItemsProtocol impls accept self as the resource
            catalog_id,
            collection_id,
            limit=page_size,
            offset=offset,
        )
        if not features:
            break

        bulk_body: list = []
        for feature in features:
            item_id = getattr(feature, "id", None) or (
                feature.get("id") if isinstance(feature, dict) else None
            )
            if not item_id:
                continue

            if mode == "obfuscated":
                index_name = obfuscated_index
                doc = {
                    "geoid": item_id,
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                }
            else:
                index_name = stac_index
                doc = (
                    feature.model_dump(by_alias=True, exclude_none=True)
                    if hasattr(feature, "model_dump")
                    else dict(feature)
                )
                doc["catalog_id"] = catalog_id
                doc["collection_id"] = collection_id

            bulk_body.append({"index": {"_index": index_name, "_id": item_id}})
            bulk_body.append(doc)

        if bulk_body:
            resp = await es.bulk(body=bulk_body)
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
    """
    Reindex all items in a catalog into Elasticsearch.

    mode="obfuscated" → geoid-only index, all collections.
    mode="catalog"    → items index, only collections with search_index=True.

    Also cleans up the complementary index via delete_by_query before reindexing
    to avoid stale documents when switching modes.
    """

    task_type = "elasticsearch_bulk_reindex_catalog"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_index_name,
            get_obfuscated_index_name,
            GEOID_OBFUSCATED_MAPPING,
        )
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCatalogReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        mode = inputs.mode

        stac_index = get_index_name(_get_index_prefix(), "item")
        obfuscated_index = get_obfuscated_index_name(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available in this process.")

        total_indexed = 0
        es = _build_es_client()
        # Ensure obfuscated index exists when needed.
        if mode == "obfuscated":
            if not await es.indices.exists(index=obfuscated_index):
                await es.indices.create(
                    index=obfuscated_index,
                    body={"mappings": GEOID_OBFUSCATED_MAPPING},
                )
            # Remove stale STAC items for this catalog.
            await es.delete_by_query(
                index=stac_index,
                body={"query": {"term": {"catalog_id": catalog_id}}},
                ignore_unavailable=True,
            )
        else:
            # Remove stale obfuscated docs for this catalog.
            await es.delete_by_query(
                index=obfuscated_index,
                body={"query": {"match_all": {}}},
                ignore_unavailable=True,
            )

        # Reindex all collections.
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
                    es, catalogs_proto, catalog_id, collection_id,
                    mode, stac_index, obfuscated_index,
                )
                total_indexed += count
                logger.info(
                    "BulkCatalogReindexTask: %s/%s — %d docs indexed (%s mode).",
                    catalog_id, collection_id, count, mode,
                )
            if len(collections) < batch:
                break
            offset += batch

        return {
            "catalog_id": catalog_id,
            "mode": mode,
            "total_indexed": total_indexed,
            "status": "done",
        }


# ---------------------------------------------------------------------------
# Task: BulkCollectionReindexTask
# ---------------------------------------------------------------------------

class BulkCollectionReindexTask(TaskProtocol):
    """
    Reindex all items in a single collection into Elasticsearch.
    Triggered by POST /search/reindex/catalogs/{id}/collections/{cid}.
    """

    task_type = "elasticsearch_bulk_reindex_collection"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_index_name,
            get_obfuscated_index_name,
            GEOID_OBFUSCATED_MAPPING,
        )
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.tools.discovery import get_protocol

        inputs = BulkCollectionReindexInputs.model_validate(payload.inputs)
        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id
        mode = inputs.mode

        stac_index = get_index_name(_get_index_prefix(), "item")
        obfuscated_index = get_obfuscated_index_name(_get_index_prefix(), catalog_id)

        catalogs_proto = get_protocol(CatalogsProtocol)
        if not catalogs_proto:
            raise RuntimeError("CatalogsProtocol not available.")

        es = _build_es_client()
        if mode == "obfuscated" and not await es.indices.exists(index=obfuscated_index):
            await es.indices.create(
                index=obfuscated_index,
                body={"mappings": GEOID_OBFUSCATED_MAPPING},
            )
        count = await _reindex_collection(
            es, catalogs_proto, catalog_id, collection_id,
            mode, stac_index, obfuscated_index,
        )

        return {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "mode": mode,
            "total_indexed": count,
            "status": "done",
        }


# ---------------------------------------------------------------------------
# Task: ObfuscatedIndexTask  (per-item, incremental)
# ---------------------------------------------------------------------------

class ObfuscatedIndexTask(TaskProtocol):
    """
    Index a single item as {geoid, catalog_id, collection_id} into the
    obfuscated geoid index. Dispatched per-item by ElasticsearchModule
    event handlers when the catalog has obfuscated=True.
    """

    task_type = "elasticsearch_obfuscated_index"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import (
            get_obfuscated_index_name,
            GEOID_OBFUSCATED_MAPPING,
        )

        inputs = ObfuscatedIndexInputs.model_validate(payload.inputs)
        index_name = get_obfuscated_index_name(_get_index_prefix(), inputs.catalog_id)

        es = _build_es_client()
        if not await es.indices.exists(index=index_name):
            await es.indices.create(
                index=index_name,
                body={"mappings": GEOID_OBFUSCATED_MAPPING},
            )
        await es.index(
            index=index_name,
            id=inputs.geoid,
            document={
                "geoid": inputs.geoid,
                "catalog_id": inputs.catalog_id,
                "collection_id": inputs.collection_id,
            },
        )

        return {"geoid": inputs.geoid, "index": index_name, "status": "indexed"}


# ---------------------------------------------------------------------------
# Task: ObfuscatedDeleteTask  (per-item, incremental)
# ---------------------------------------------------------------------------

class ObfuscatedDeleteTask(TaskProtocol):
    """
    Remove a single geoid document from the obfuscated index.
    Safe to run even if the catalog is not obfuscated (no-op via NotFoundError).
    """

    task_type = "elasticsearch_obfuscated_delete"

    async def run(self, payload: TaskPayload) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_obfuscated_index_name

        try:
            from opensearchpy.exceptions import NotFoundError
        except ImportError:
            from elasticsearch import NotFoundError

        inputs = ObfuscatedDeleteInputs.model_validate(payload.inputs)
        index_name = get_obfuscated_index_name(_get_index_prefix(), inputs.catalog_id)

        es = _build_es_client()
        try:
            await es.delete(index=index_name, id=inputs.geoid)
        except NotFoundError:
            pass  # safe: document may not be in the obfuscated index

        return {"geoid": inputs.geoid, "index": index_name, "status": "deleted"}
