"""
Search Service — Elasticsearch-backed implementation of SearchProtocol.

Primary goal: search by id, title, description, or any property of items,
             supporting multilingual fields (title, description stored as JSON objects
             with language keys, e.g. {"en": "Food security", "fr": "Sécurité alimentaire"}).

The service uses ES wildcard field patterns (title.*, description.*) to search
across all language variants transparently. Callers can optionally pass `lang`
to boost a specific language.

Implements ``SearchProtocol`` so that the router and other consumers discover
the search backend via protocol discovery, not direct imports.
"""
import logging
from typing import Any, Dict, List, Literal, Optional, Tuple

from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager
from dynastore.extensions import ExtensionProtocol

from dynastore.modules.elasticsearch.config import config as es_config
from dynastore.modules.elasticsearch.mappings import get_index_name, get_obfuscated_index_name
from .search_models import (
    CatalogSearchBody,
    GeoidCollection,
    GeoidResult,
    GenericCollection,
    ItemCollection,
    SearchBody,
    SearchLink,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_es_client():
    """Build and return an AsyncElasticsearch client."""
    try:
        from elasticsearch import AsyncElasticsearch
    except ImportError:
        raise RuntimeError(
            "Elasticsearch client is not installed. Run: poetry add elasticsearch[async]"
        )
    kwargs: Dict[str, Any] = {
        "hosts": [es_config.url],
        "verify_certs": es_config.verify_certs,
    }
    if es_config.username and es_config.password:
        kwargs["basic_auth"] = (es_config.username, es_config.password)
    return AsyncElasticsearch(**kwargs)


def _parse_sort(sortby: Optional[str]) -> List[Dict[str, Any]]:
    """
    Parse STAC sortby string into ES sort clause.
    
    Examples:
      '+properties.title'  → [{"properties.title.keyword": {"order": "asc"}}]
      '-properties.datetime' → [{"properties.datetime": {"order": "desc"}}]
    """
    if not sortby:
        return [{"_score": {"order": "desc"}}]
    direction = "desc" if sortby.startswith("-") else "asc"
    field = sortby.lstrip("+-")
    # For text fields we sort on the .keyword sub-field
    text_fields = {"properties.title", "title", "description", "properties.description"}
    if field in text_fields:
        field = f"{field}.keyword"
    return [{field: {"order": direction}}, {"_score": {"order": "desc"}}]


def _parse_datetime_filter(datetime_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """Convert STAC datetime string to ES range filter."""
    if not datetime_str:
        return None
    if "/" in datetime_str:
        parts = datetime_str.split("/")
        gte = None if parts[0] == ".." else parts[0]
        lte = None if parts[1] == ".." else parts[1]
        r: Dict[str, Any] = {}
        if gte:
            r["gte"] = gte
        if lte:
            r["lte"] = lte
        return {"range": {"properties.datetime": r}}
    # Single date-time value
    return {
        "bool": {
            "should": [
                {"term": {"properties.datetime": datetime_str}},
                {"range": {"properties.datetime": {"lte": datetime_str}}},
            ]
        }
    }


def _build_item_query(body: SearchBody) -> Dict[str, Any]:
    """
    Build an Elasticsearch query DSL from a STAC SearchBody.
    
    Multilingual fields (title, description, keywords) are stored as objects
    with language-code keys, e.g. {"en": "Food", "fr": "Aliment"}.
    We use wildcard patterns in multi_match to cover all languages.
    """
    must: List[Dict[str, Any]] = []
    filter_: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 1. Free-text query (PRIMARY GOAL)
    # Covers id, title.*, description.*, keywords.*, and all properties.*
    # ------------------------------------------------------------------
    if body.q:
        must.append({
            "multi_match": {
                "query": body.q,
                "type": "best_fields",
                # Wildcard field patterns cover all language variants:
                #  title.en, title.fr, title.ar, ...
                #  properties.title.en, properties.title.fr, ...
                "fields": [
                    "id^3",               # Boost exact ID matches
                    "title.*^2",          # Multilingual title at root level
                    "properties.title.*^2",  # Multilingual title inside properties
                    "description.*",
                    "properties.description.*",
                    "keywords.*",
                    "properties.keywords.*",
                    "properties.*",       # Any other property (lower relevance)
                ],
                "fuzziness": "AUTO",
                "minimum_should_match": "1",
            }
        })

    # ------------------------------------------------------------------
    # 2. IDs filter
    # ------------------------------------------------------------------
    if body.ids:
        filter_.append({"terms": {"id": body.ids}})

    # ------------------------------------------------------------------
    # 3. Collections filter
    # ------------------------------------------------------------------
    if body.collections:
        filter_.append({"terms": {"collection_id": body.collections}})

    # ------------------------------------------------------------------
    # 4. Spatial filter – bbox or intersects (mutually exclusive)
    # ------------------------------------------------------------------
    if body.bbox:
        lon_min, lat_min, lon_max, lat_max = body.bbox[:4]
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": {
                        "type": "envelope",
                        "coordinates": [[lon_min, lat_max], [lon_max, lat_min]],
                    },
                    "relation": "intersects",
                }
            }
        })
    elif body.intersects:
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": body.intersects,
                    "relation": "intersects",
                }
            }
        })

    # ------------------------------------------------------------------
    # 5. Datetime filter
    # ------------------------------------------------------------------
    dt_filter = _parse_datetime_filter(body.datetime)
    if dt_filter:
        filter_.append(dt_filter)

    # ------------------------------------------------------------------
    # Build compound query
    # ------------------------------------------------------------------
    if not must and not filter_:
        query = {"match_all": {}}
    else:
        query = {
            "bool": {
                **({"must": must} if must else {}),
                **({"filter": filter_} if filter_ else {}),
            }
        }

    return query


def _build_generic_query(body: CatalogSearchBody) -> Dict[str, Any]:
    """Build a query for catalog/collection search."""
    must: List[Dict[str, Any]] = []
    filter_: List[Dict[str, Any]] = []

    if body.q:
        must.append({
            "multi_match": {
                "query": body.q,
                "type": "best_fields",
                "fields": [
                    "id^3",
                    "title.*^2",
                    "description.*",
                    "keywords.*",
                ],
                "fuzziness": "AUTO",
            }
        })
    if body.ids:
        filter_.append({"terms": {"id": body.ids}})

    if not must and not filter_:
        return {"match_all": {}}
    return {
        "bool": {
            **({"must": must} if must else {}),
            **({"filter": filter_} if filter_ else {}),
        }
    }


# ---------------------------------------------------------------------------
# SearchService — implements SearchProtocol (discoverable via get_protocol)
# ---------------------------------------------------------------------------
class SearchService(ExtensionProtocol):
    """
    Elasticsearch-backed implementation of ``SearchProtocol``.

    Discovered at runtime by the router via ``get_protocol(SearchProtocol)``
    — the router has zero direct imports from this module or from ES.

    To swap to a different backend (Solr, Meilisearch, etc.), create a new
    ``ExtensionProtocol`` that satisfies the same ``SearchProtocol`` contract
    and ensure it is loaded instead of this one.
    """

    priority: int = 100
    router: APIRouter

    def __init__(self):
        from .router import router as search_router
        self.router = search_router
        logger.info("SearchService: Initializing extension.")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """
        Protocol-discoverable service that provides STAC-compliant search
        backed by Elasticsearch.

        Implements paginated search for Items, Catalogs, and Collections.
        """
        from .policies import register_search_policies
        register_search_policies()
        yield

    async def search_items(
        self,
        body: SearchBody,
        base_url: str = "",
    ) -> ItemCollection:
        """
        Search STAC Items.

        Supports: q (full-text, multilingual), bbox, intersects, datetime,
                  ids, collections, sortby, and cursor-based pagination via token.
        """
        index = get_index_name(es_config.index_prefix, "item")
        sort = _parse_sort(body.sortby)
        query = _build_item_query(body)

        es_body: Dict[str, Any] = {
            "query": query,
            "size": body.limit,
            "sort": sort,
        }

        # Cursor-based pagination (search_after)
        if body.token:
            import json as _json
            try:
                es_body["search_after"] = _json.loads(body.token)
            except Exception:
                logger.warning(f"Ignoring invalid search token: {body.token!r}")

        es = _build_es_client()
        try:
            resp = await es.search(index=index, body=es_body)
        finally:
            await es.close()

        hits = resp.get("hits", {})
        total = hits.get("total", {}).get("value", 0)
        raw_hits = hits.get("hits", [])
        features = [h["_source"] for h in raw_hits]

        links: List[SearchLink] = [
            SearchLink(rel="self", href=f"{base_url}/search", type="application/geo+json")
        ]

        # Next page link using search_after token
        if raw_hits and len(raw_hits) == body.limit:
            import json as _json
            last_sort = raw_hits[-1].get("sort", [])
            token = _json.dumps(last_sort)
            import urllib.parse
            links.append(SearchLink(
                rel="next",
                href=f"{base_url}/search",
                type="application/geo+json",
                method="POST",
                body={
                    **body.model_dump(exclude_none=True, exclude={"token"}),
                    "token": token,
                },
                merge=False,
            ))

        return ItemCollection(
            features=features,
            links=links,
            numberMatched=total,
            numberReturned=len(features),
        )

    async def search_by_geoid(
        self,
        geoids: List[str],
        catalog_id: Optional[str] = None,
        limit: int = 100,
    ) -> GeoidCollection:
        """
        Look up geoid values in the obfuscated index.

        If catalog_id is provided, searches only that catalog's obfuscated
        index ({prefix}-geoid-{catalog_id}). Otherwise searches across all
        obfuscated indexes ({prefix}-geoid-*).
        """
        if catalog_id:
            index = get_obfuscated_index_name(es_config.index_prefix, catalog_id)
        else:
            index = f"{es_config.index_prefix}-geoid-*"

        es_body: Dict[str, Any] = {
            "query": {"terms": {"geoid": geoids}},
            "size": limit,
        }

        es = _build_es_client()
        try:
            resp = await es.search(index=index, body=es_body, ignore_unavailable=True)
        finally:
            await es.close()

        raw_hits = resp.get("hits", {}).get("hits", [])
        results = [
            GeoidResult(
                geoid=h["_source"]["geoid"],
                catalog_id=h["_source"]["catalog_id"],
                collection_id=h["_source"]["collection_id"],
            )
            for h in raw_hits
            if "geoid" in h.get("_source", {})
        ]

        return GeoidCollection(
            results=results,
            numberReturned=len(results),
        )

    async def reindex_catalog(
        self,
        catalog_id: str,
        mode: Optional[Literal["catalog", "obfuscated"]] = None,
    ) -> Dict[str, Any]:
        """
        Dispatch a BulkCatalogReindexTask and return 202 + task_id.

        If `mode` is omitted, falls back to the catalog's indexer config
        (obfuscated=True → "obfuscated", otherwise "catalog").
        """
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks as tasks_module
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.tools.discovery import get_protocol

        resolved_mode = mode or await self._resolve_mode(catalog_id)

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")

        task = await tasks_module.create_task(
            engine=db,
            task_data=TaskCreate(
                caller_id="system:search",
                task_type="elasticsearch_bulk_reindex_catalog",
                inputs={"catalog_id": catalog_id, "mode": resolved_mode},
            ),
            schema=tasks_module.get_task_schema(),
        )
        return {"task_id": str(task.id), "catalog_id": catalog_id, "mode": resolved_mode, "status": "queued"}

    async def reindex_collection(
        self,
        catalog_id: str,
        collection_id: str,
        mode: Optional[Literal["catalog", "obfuscated"]] = None,
    ) -> Dict[str, Any]:
        """
        Dispatch a BulkCollectionReindexTask and return 202 + task_id.
        """
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks as tasks_module
        from dynastore.modules.tasks.models import TaskCreate
        from dynastore.tools.discovery import get_protocol

        resolved_mode = mode or await self._resolve_mode(catalog_id)

        db = get_protocol(DatabaseProtocol)
        if not db:
            raise RuntimeError("DatabaseProtocol not available.")

        task = await tasks_module.create_task(
            engine=db,
            task_data=TaskCreate(
                caller_id="system:search",
                task_type="elasticsearch_bulk_reindex_collection",
                inputs={"catalog_id": catalog_id, "collection_id": collection_id, "mode": resolved_mode},
            ),
            schema=tasks_module.get_task_schema(),
        )
        return {
            "task_id": str(task.id),
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "mode": resolved_mode,
            "status": "queued",
        }

    async def _resolve_mode(self, catalog_id: str) -> Literal["catalog", "obfuscated"]:
        """Resolve the reindex mode from the catalog's indexer config.

        Uses ConfigsProtocol discovery — no direct import from any module.
        The config key ``"elasticsearch"`` is a plain string, not an imported constant.
        """
        try:
            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.tools.discovery import get_protocol
            configs_proto = get_protocol(ConfigsProtocol)
            if configs_proto:
                cfg = await configs_proto.get_config("elasticsearch", catalog_id=catalog_id)
                if cfg and getattr(cfg, "obfuscated", False):
                    return "obfuscated"
        except Exception:
            pass
        return "catalog"

    async def search_catalogs(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """Search Catalogs."""
        return await self._search_generic(body, "catalog", base_url)

    async def search_collections(
        self,
        body: CatalogSearchBody,
        base_url: str = "",
    ) -> GenericCollection:
        """Search Collections."""
        return await self._search_generic(body, "collection", base_url)

    async def _search_generic(
        self,
        body: CatalogSearchBody,
        entity_type: str,
        base_url: str = "",
    ) -> GenericCollection:
        index = get_index_name(es_config.index_prefix, entity_type)
        query = _build_generic_query(body)

        es_body: Dict[str, Any] = {
            "query": query,
            "size": body.limit,
            "sort": [{"_score": {"order": "desc"}}],
        }
        if body.token:
            import json as _json
            try:
                es_body["search_after"] = _json.loads(body.token)
            except Exception:
                pass

        es = _build_es_client()
        try:
            resp = await es.search(index=index, body=es_body)
        finally:
            await es.close()

        raw_hits = resp.get("hits", {}).get("hits", [])
        entities = [h["_source"] for h in raw_hits]

        links: List[SearchLink] = []
        if raw_hits and len(raw_hits) == body.limit:
            import json as _json
            token = _json.dumps(raw_hits[-1].get("sort", []))
            links.append(SearchLink(
                rel="next",
                href=f"{base_url}/search/{entity_type}s",
                type="application/json",
                method="POST",
                body={**body.model_dump(exclude_none=True, exclude={"token"}), "token": token},
                merge=False,
            ))

        return GenericCollection(
            entities=entities,
            links=links,
            numberReturned=len(entities),
        )
