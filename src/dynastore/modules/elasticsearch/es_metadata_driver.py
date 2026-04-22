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
Elasticsearch metadata driver — implements CollectionMetadataStore.

Stores collection metadata in an ES index per catalog.  Provides fulltext
search (multi_match on title/description/keywords), CQL2-JSON filter support,
spatial filtering on extent bbox (geo_shape), and aggregations.

Index naming: ``{prefix}_collection_metadata_{catalog_id}``

The schema is extensible — metadata fields are stored as-is in ES
(dynamic mapping).  Custom fields added by enrichers or users are
automatically indexed and searchable.
"""

import logging
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.metadata_driver import (
    CollectionMetadataStore,
    MetadataCapability,
)
from dynastore.modules.storage.storage_location import StorageLocation
from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from pydantic import Field

logger = logging.getLogger(__name__)

class MetadataElasticsearchDriverConfig(PluginConfig):
    """Configuration for the Elasticsearch collection metadata driver.

    ``index_prefix`` is ``Immutable`` — once set it cannot change, because
    altering the prefix would orphan all existing catalog metadata indexes.

    The final index name is ``{index_prefix}_collection_metadata_{catalog_id}``.
    """

    index_prefix: Immutable[str] = Field(
        "meta",
        description=(
            "Index name prefix.  "
            "Final index: ``{index_prefix}_collection_metadata_{catalog_id}``.  "
            "Immutable once set — changing it would orphan existing indexes."
        ),
    )


# MetadataElasticsearchDriverConfig auto-registers via PluginConfig.__init_subclass__.


async def _on_apply_es_metadata_driver_config(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Create the ES metadata index for the catalog when the driver config is applied."""
    if not isinstance(config, MetadataElasticsearchDriverConfig):
        return
    if not catalog_id:
        return  # platform-level config — no catalog to create index for

    from dynastore.tools.discovery import get_protocols

    drivers = [
        d for d in get_protocols(CollectionMetadataStore)
        if getattr(d, "driver_id", None) == "elasticsearch_metadata"
    ]
    for driver in drivers:
        ensure = getattr(driver, "ensure_storage", None)
        if ensure is None:
            continue
        try:
            await ensure(catalog_id)
        except Exception as exc:
            logger.warning(
                "ensure_storage failed for elasticsearch_metadata on catalog '%s': %s",
                catalog_id, exc,
            )


MetadataElasticsearchDriverConfig.register_apply_handler(_on_apply_es_metadata_driver_config)

# ---------------------------------------------------------------------------
# Mapping — explicit typing only for fields ES cannot auto-detect
# ---------------------------------------------------------------------------

_COLLECTION_METADATA_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": [
        {
            "multilingual_text": {
                "path_match": "*.en",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text",
                    "analyzer": "standard",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
            }
        },
        {
            "strings": {
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword",
                    "fields": {"text": {"type": "text", "analyzer": "standard"}},
                },
            }
        },
    ],
    "properties": {
        "id": {"type": "keyword"},
        "catalog_id": {"type": "keyword"},
        "title": {"type": "object", "dynamic": True},
        "description": {"type": "object", "dynamic": True},
        "keywords": {"type": "object", "dynamic": True},
        "license": {"type": "object", "dynamic": True},
        "links": {"type": "object", "enabled": False},
        "assets": {"type": "object", "enabled": False},
        "extent": {
            "properties": {
                "spatial": {
                    "properties": {
                        "bbox": {"type": "float"},
                        "bbox_shape": {"type": "geo_shape"},
                    }
                },
                "temporal": {
                    "properties": {
                        "interval": {"type": "date_range"},
                    }
                },
            }
        },
        "providers": {"type": "object", "dynamic": True},
        "summaries": {"type": "object", "dynamic": True},
        "item_assets": {"type": "object", "enabled": False},
        "stac_version": {"type": "keyword"},
        "stac_extensions": {"type": "keyword"},
        "created": {"type": "date"},
        "updated": {"type": "date"},
    },
}


def _metadata_index_name(prefix: str, catalog_id: str) -> str:
    return f"{prefix}_collection_metadata_{catalog_id}"


def _bbox_to_envelope(bbox: List[float]) -> Optional[Dict[str, Any]]:
    """Convert [west, south, east, north] to ES envelope for geo_shape."""
    if not bbox or len(bbox) < 4:
        return None
    return {
        "type": "envelope",
        "coordinates": [[bbox[0], bbox[3]], [bbox[2], bbox[1]]],
    }


class MetadataElasticsearchDriver:
    """Elasticsearch implementation of CollectionMetadataStore.

    Uses opensearch-py client (wire-compatible with ES and OpenSearch).
    """

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SEARCH,
        MetadataCapability.CQL_FILTER,
        MetadataCapability.SPATIAL_FILTER,
        MetadataCapability.AGGREGATION,
        MetadataCapability.PHYSICAL_ADDRESSING,
    })

    def location(self, catalog_id: str, collection_id: Optional[str] = None) -> StorageLocation:
        prefix = self._get_prefix()
        index = _metadata_index_name(prefix, catalog_id)
        return StorageLocation(
            backend="elasticsearch",
            canonical_uri=f"es://{index}",
            identifiers={"index": index, "prefix": prefix, "catalog_id": catalog_id},
            display_label=f"ES metadata index: {index}",
        )

    def _get_client(self):
        from dynastore.modules.elasticsearch.client import get_client

        return get_client()

    def _get_prefix(self) -> str:
        from dynastore.modules.elasticsearch.client import get_index_prefix

        return get_index_prefix()

    async def ensure_storage(self, catalog_id: str) -> None:
        """Ensure the ES metadata index exists for the given catalog (idempotent).

        Called by ``MetadataElasticsearchDriverConfig`` apply handler
        when the driver config is applied at catalog scope.
        """
        await self._ensure_index(catalog_id)

    async def _ensure_index(self, catalog_id: str) -> str:
        """Create metadata index if it doesn't exist. Returns index name."""
        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")

        index_name = _metadata_index_name(self._get_prefix(), catalog_id)
        exists = await client.indices.exists(index=index_name)
        if not exists:
            await client.indices.create(
                index=index_name,
                body={
                    "mappings": _COLLECTION_METADATA_MAPPING,
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    },
                },
            )
            logger.info("Created metadata index: %s", index_name)
        return index_name

    @staticmethod
    def _enrich_doc(metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare doc for ES: add bbox_shape and convert temporal interval to date_range format."""
        doc = dict(metadata)
        extent = doc.get("extent")
        if isinstance(extent, dict):
            spatial = extent.get("spatial")
            if isinstance(spatial, dict):
                bboxes = spatial.get("bbox")
                if isinstance(bboxes, list) and bboxes:
                    first_bbox = bboxes[0] if isinstance(bboxes[0], list) else bboxes
                    envelope = _bbox_to_envelope(first_bbox)
                    if envelope:
                        spatial["bbox_shape"] = envelope

            temporal = extent.get("temporal")
            if isinstance(temporal, dict):
                interval = temporal.get("interval")
                if isinstance(interval, list):
                    # STAC: [[start, end], ...] → ES date_range: [{"gte": start, "lte": end}, ...]
                    # Skip null-null bounds (no useful range for ES date_range queries).
                    date_ranges = []
                    for bounds in interval:
                        if isinstance(bounds, list) and len(bounds) >= 2:
                            start, end = bounds[0], bounds[1]
                            if start is not None or end is not None:
                                range_obj: Dict[str, Any] = {}
                                if start is not None:
                                    range_obj["gte"] = start
                                if end is not None:
                                    range_obj["lte"] = end
                                date_ranges.append(range_obj)
                    if date_ranges:
                        temporal["interval"] = date_ranges
                    else:
                        temporal.pop("interval", None)
        return doc

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return None

        index_name = _metadata_index_name(self._get_prefix(), catalog_id)
        if not await client.indices.exists(index=index_name):
            return None
        try:
            resp = await client.get(index=index_name, id=collection_id)
            return resp["_source"]
        except Exception:
            return None

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        await self._ensure_index(catalog_id)
        client = self._get_client()
        if not client:
            raise RuntimeError("Elasticsearch client not available")

        index_name = _metadata_index_name(self._get_prefix(), catalog_id)
        doc = self._enrich_doc(metadata)
        doc["id"] = collection_id
        doc["catalog_id"] = catalog_id

        await client.index(
            index=index_name,
            id=collection_id,
            body=doc,
            refresh="wait_for",  # type: ignore[call-arg]
        )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        client = self._get_client()
        if not client:
            return

        index_name = _metadata_index_name(self._get_prefix(), catalog_id)
        try:
            if soft:
                # Mark as deleted but retain the document
                await client.update(
                    index=index_name,
                    id=collection_id,
                    body={"doc": {"_deleted": True}},
                    refresh="wait_for",  # type: ignore[call-arg]
                )
            else:
                await client.delete(
                    index=index_name,
                    id=collection_id,
                    refresh="wait_for",  # type: ignore[call-arg]
                )
        except Exception as e:
            logger.debug("delete_metadata ES error for %s/%s: %s", catalog_id, collection_id, e)

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
        client = self._get_client()
        if not client:
            return [], 0

        index_name = _metadata_index_name(self._get_prefix(), catalog_id)

        # Check index exists
        try:
            exists = await client.indices.exists(index=index_name)
            if not exists:
                return [], 0
        except Exception:
            return [], 0

        must_clauses: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = [
            {"bool": {"must_not": [{"term": {"_deleted": True}}]}}
        ]

        # Fulltext search
        if q:
            must_clauses.append({
                "multi_match": {
                    "query": q,
                    "fields": [
                        "title.en^3", "title.en.keyword^2",
                        "description.en^2",
                        "keywords.*.text",
                        "id^2",
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            })

        # Spatial filter on extent.spatial.bbox_shape
        if bbox and len(bbox) >= 4:
            envelope = _bbox_to_envelope(bbox)
            if envelope:
                filter_clauses.append({
                    "geo_shape": {
                        "extent.spatial.bbox_shape": {
                            "shape": envelope,
                            "relation": "intersects",
                        }
                    }
                })

        # CQL2-JSON filter — not yet implemented for ES metadata driver
        if filter_cql:
            logger.warning(
                "CQL2-JSON filter on ES metadata index is not implemented; ignoring"
            )

        query_body: Dict[str, Any] = {
            "bool": {
                "must": must_clauses if must_clauses else [{"match_all": {}}],
                "filter": filter_clauses,
            }
        }

        body: Dict[str, Any] = {
            "query": query_body,
            "from": offset,
            "size": limit,
            "sort": [{"_score": "desc"}, {"id": "asc"}],
        }

        try:
            resp = await client.search(index=index_name, body=body)
            hits = resp.get("hits", {})
            total = hits.get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total
            results = [hit["_source"] for hit in hits.get("hits", [])]
            return results, total_count
        except Exception as e:
            logger.warning("search_metadata ES error for %s: %s", catalog_id, e)
            return [], 0

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
                MetadataElasticsearchDriverConfig,
                catalog_id=catalog_id,
                ctx=DriverContext(db_resource=db_resource),
            )
        except Exception:
            return {}

    async def is_available(self) -> bool:
        client = self._get_client()
        if not client:
            return False
        try:
            info = await client.info()
            return bool(info)
        except Exception:
            return False
