#    Copyright 2026 FAO
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

"""Composer for the centralised composed-config API.

Response shape: ``configs[scope][topic][(sub)?][ClassName] -> payload``.
Class name IS the identity — no ``class_key`` field inside the payload.
Routing configs' ``operations[OP]`` become slim ``DriverRef`` dicts
pointing at sibling driver configs in ``configs.storage.drivers.*``.
Per-scope filtering is derived from the class name / module path —
there is no per-class ClassVar override to maintain.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Type

from dynastore.extensions.configs.config_api_dto import (
    CatalogConfigResponse,
    CollectionConfigResponse,
    ConfigMeta,
    ConfigPage,
    DriverRef,
    PlatformConfigResponse,
)
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    list_registered_configs,
)

logger = logging.getLogger(__name__)

# --- Placement + relevance (inline — keeps the table next to the
# composer that is its only consumer). -------------------------------

# Abstract base configs are filtered via the ``is_abstract_base = True``
# ClassVar on the class itself (read via ``cls.__dict__.get(...)`` so
# concrete subclasses don't inherit the marker).  Single source of truth
# lives on the class — no parallel hand-maintained name set here.

# Per-class name prefixes that bucket classes into relevance tiers.
_COLLECTION_PREFIXES = ("Items", "Asset", "Collection", "WritePolicyDefaults")
_COLLECTION_NAMES = frozenset({"GcpCollectionBucketConfig"})
_CATALOG_PREFIXES = ("Catalog",)
_CATALOG_NAMES = frozenset({"GcpCatalogBucketConfig", "GcpEventingConfig"})

# Module prefix → (scope, topic).  Longer prefixes first.
_MODULE_RULES: List[Tuple[str, Tuple[str, str]]] = [
    ("dynastore.modules.storage.routing_config",        ("storage", "routing")),
    ("dynastore.modules.storage.drivers",               ("storage", "drivers")),
    ("dynastore.modules.storage.driver_config",         ("storage", "drivers")),
    ("dynastore.modules.elasticsearch.es_metadata",     ("storage", "drivers")),
    ("dynastore.modules.catalog.catalog_config",        ("catalog", "collection")),
    ("dynastore.modules.catalog.asset_config",          ("catalog", "asset")),
    ("dynastore.modules.iam",                           ("platform", "security")),
    ("dynastore.modules.web",                           ("platform", "web")),
    ("dynastore.extensions.web",                        ("platform", "web")),
    ("dynastore.modules.tasks",                         ("platform", "tasks")),
    ("dynastore.modules.stats",                         ("platform", "stats")),
    ("dynastore.modules.gcp",                           ("platform", "gcp")),
    ("dynastore.modules.tiles",                         ("platform", "tiles")),
    ("dynastore.modules.stac",                          ("extensions", "stac")),
]
_CLASS_SCOPE_TOPIC: Dict[str, Tuple[str, str]] = {
    "CollectionWritePolicy": ("storage", "policy"),
    "WritePolicyDefaults":   ("storage", "policy"),
    "CollectionSchema":      ("storage", "schema"),
}
# Driver sub-bucket by class-name prefix (only when topic == "drivers").
_DRIVER_SUB: List[Tuple[str, str]] = [
    ("Items", "items"), ("Asset", "assets"),
    ("Catalog", "catalog"), ("Collection", "collection"),
    ("Metadata", "metadata"),
]

# Routing config keys whose ``operations[OP]`` is rewritten as DriverRefs.
_ROUTING_CONFIG_KEYS = frozenset({
    "CollectionRoutingConfig", "AssetRoutingConfig", "CatalogRoutingConfig",
})


def _place(cls: Type[PluginConfig], active_scope: str) -> Optional[Tuple[str, str, Optional[str]]]:
    """Return ``(scope, topic, sub)`` for ``cls`` at ``active_scope``, or ``None`` to drop.

    ``None`` means either the class is an abstract base or its relevance
    tier does not include ``active_scope``.
    """
    name = cls.__name__
    if cls.__dict__.get("is_abstract_base", False):
        return None

    is_catalog = name in _CATALOG_NAMES or name.startswith(_CATALOG_PREFIXES)
    is_collection = not is_catalog and (
        name in _COLLECTION_NAMES or name.startswith(_COLLECTION_PREFIXES)
    )
    if is_catalog and active_scope == "collection":
        return None
    if is_collection and active_scope != "collection":
        return None

    if name in _CLASS_SCOPE_TOPIC:
        scope, topic = _CLASS_SCOPE_TOPIC[name]
    else:
        mod = cls.__module__
        hit = next((g for prefix, g in _MODULE_RULES if mod.startswith(prefix)), None)
        if hit is not None:
            scope, topic = hit
        elif mod.startswith("dynastore.extensions."):
            scope = "extensions"
            topic = mod[len("dynastore.extensions."):].split(".", 1)[0]
        else:
            scope, topic = "platform", "misc"

    sub: Optional[str] = None
    if topic == "drivers":
        sub = next((s for pfx, s in _DRIVER_SUB if name.startswith(pfx)), "misc")
    return scope, topic, sub


class ConfigApiService:
    """Composes the scope→topic→class-name configuration tree."""

    def __init__(
        self,
        config_service: ConfigsProtocol,
        assets_service: Optional[Any] = None,
        catalogs_service: Optional[Any] = None,
    ):
        self._config_service = config_service
        self._assets_service = assets_service
        self._catalogs_service = catalogs_service

    # --- Effective-value resolution (unchanged shape). ---

    async def _get_effective_configs(
        self,
        catalog_id: Optional[str],
        collection_id: Optional[str],
        resolved: bool,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
        """Return ``(by_class, sources)`` for every registered config class.

        * ``by_class[ClassName]`` — effective payload (waterfall-resolved
          when ``resolved``).
        * ``sources[ClassName]`` — ``"collection" | "catalog" | "platform" | "default"``.
        """
        svc = self._config_service
        all_classes = list_registered_configs()

        if not resolved:
            if catalog_id and collection_id:
                result = await svc.list_configs(
                    catalog_id=catalog_id, collection_id=collection_id,
                    limit=1000, offset=0,
                )
                source = "collection"
            elif catalog_id:
                result = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
                source = "catalog"
            else:
                result = await svc.list_configs(limit=1000, offset=0)
                source = "platform"

            by_class: Dict[str, Dict[str, Any]] = {}
            sources: Dict[str, str] = {}
            for item in result.get("items", []):
                class_key: str = item["plugin_id"]
                cls = all_classes.get(class_key)
                raw: Dict[str, Any] = item.get("config_data") or {}
                try:
                    by_class[class_key] = (
                        cls.model_validate(raw).model_dump() if cls else raw
                    )
                except Exception:
                    by_class[class_key] = raw
                sources[class_key] = source
            return by_class, sources

        # Resolved path — pull each tier's per-class delta dict ONCE (3
        # SELECTs total) then merge in memory, mirroring the waterfall in
        # ``ConfigService.get_config`` (code defaults > platform > catalog
        # > collection, right-most wins).  Replaces the previous per-class
        # ``await svc.get_config(cls, ...)`` loop which fired ~N awaits per
        # request — large win on the deep view that the dashboard hits.
        def _to_data_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
            return {it["plugin_id"]: (it.get("config_data") or {}) for it in items}

        collection_data: Dict[str, Dict[str, Any]] = {}
        catalog_data: Dict[str, Dict[str, Any]] = {}
        if catalog_id and collection_id:
            r = await svc.list_configs(
                catalog_id=catalog_id, collection_id=collection_id,
                limit=1000, offset=0,
            )
            collection_data = _to_data_map(r.get("items", []))
        if catalog_id:
            r = await svc.list_configs(catalog_id=catalog_id, limit=1000, offset=0)
            catalog_data = _to_data_map(r.get("items", []))
        r = await svc.list_configs(limit=1000, offset=0)
        platform_data = _to_data_map(r.get("items", []))

        by_class: Dict[str, Dict[str, Any]] = {}
        sources: Dict[str, str] = {}
        for class_key, cls in all_classes.items():
            if class_key in collection_data:
                sources[class_key] = "collection"
            elif class_key in catalog_data:
                sources[class_key] = "catalog"
            elif class_key in platform_data:
                sources[class_key] = "platform"
            else:
                sources[class_key] = "default"

            # Build the effective payload by merging deltas onto the code
            # default model.  Tier rows are stored as deltas (only fields
            # the caller explicitly set), so dict-update with last-wins
            # mirrors ``ConfigService.get_config``.
            try:
                merged: Dict[str, Any] = cls().model_dump(mode="python")
            except Exception:
                merged = {}
            for tier_data in (platform_data, catalog_data, collection_data):
                delta = tier_data.get(class_key)
                if delta:
                    merged.update(delta)

            # Round-trip through the model so defaults / coercions are applied
            # consistently with the single-class read path.
            try:
                by_class[class_key] = cls.model_validate(merged).model_dump()
            except Exception:
                by_class[class_key] = merged or {}
        return by_class, sources

    # --- Tree + meta in one pass. ---

    @staticmethod
    def _compose_tree(
        by_class: Dict[str, Dict[str, Any]],
        sources: Dict[str, str],
        active_scope: str,
        include_meta: bool,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, ConfigMeta]]]:
        """Bucket visible classes into scope/topic tree and optionally build meta."""
        all_classes = list_registered_configs()
        tree: Dict[str, Any] = {}
        meta: Dict[str, ConfigMeta] = {}
        for class_key, payload in by_class.items():
            cls = all_classes.get(class_key)
            if cls is None:
                continue
            placed = _place(cls, active_scope)
            if placed is None:
                continue
            scope, topic, sub = placed
            topic_node = tree.setdefault(scope, {}).setdefault(topic, {})
            if sub is None:
                topic_node[class_key] = payload
            else:
                topic_node.setdefault(sub, {})[class_key] = payload
            if include_meta and class_key in sources:
                meta[class_key] = ConfigMeta(source=sources[class_key])
        return tree, (meta if include_meta else None)

    # --- Routing-ref rewrite. ---

    @staticmethod
    def _build_routing_refs(by_class: Dict[str, Dict[str, Any]]) -> None:
        """Rewrite ``operations[OP]`` in routing configs as slim ``DriverRef``s.

        Driver → config-class lookup uses the registry directly: post-TypedDriver
        bind, ``class_key()`` for ``XDriverConfig`` returns ``"XDriver"`` — the
        same string used as ``driver_id`` in routing entries.  ``driver_id`` IS
        the lookup key into ``list_registered_configs()``.
        """
        all_classes = list_registered_configs()
        for class_key in _ROUTING_CONFIG_KEYS:
            routing = by_class.get(class_key)
            if not routing:
                continue
            operations = routing.get("operations")
            if not isinstance(operations, dict):
                continue
            rewritten: Dict[str, List[Dict[str, Any]]] = {}
            for op, entries in operations.items():
                refs: List[Dict[str, Any]] = []
                for entry in entries or []:
                    if not isinstance(entry, dict):
                        continue
                    driver_id = entry.get("driver_id", "")
                    config_ref = driver_id if driver_id in all_classes else None
                    refs.append(DriverRef(
                        driver_id=driver_id,
                        config_ref=config_ref,
                        on_failure=entry.get("on_failure", "fatal"),
                        write_mode=entry.get("write_mode", "sync"),
                    ).model_dump())
                rewritten[op] = refs
            routing["operations"] = rewritten

    # --- Compose endpoints. ---

    async def compose_collection_config(
        self,
        base_url: str,
        catalog_id: str,
        collection_id: str,
        depth: int = 0,
        assets_page: int = 1,
        page_size: int = 15,
        resolved: bool = True,
        meta: bool = False,
    ) -> CollectionConfigResponse:
        by_class, sources = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=collection_id, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict = self._compose_tree(by_class, sources, "collection", meta)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0:
            categories = {}
            assets_total, assets_items = await self._list_assets(
                catalog_id, collection_id, assets_page, page_size,
            )
            pg = self._build_config_page(
                base_url=base_url, category="assets", total=assets_total,
                page=assets_page, page_size=page_size, extra_params={"depth": depth},
            )
            pg.items = assets_items
            categories["assets"] = pg

        return CollectionConfigResponse(
            collection_id=collection_id, catalog_id=catalog_id,
            configs=tree, meta=meta_dict, categories=categories,
        )

    async def compose_catalog_config(
        self,
        base_url: str,
        catalog_id: str,
        depth: int = 0,
        collections_page: int = 1,
        assets_page: int = 1,
        page_size: int = 15,
        resolved: bool = True,
        meta: bool = False,
    ) -> CatalogConfigResponse:
        by_class, sources = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=None, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict = self._compose_tree(by_class, sources, "catalog", meta)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0:
            categories = {}
            coll_total, coll_items = await self._list_collections(
                base_url, catalog_id, collections_page, page_size, depth,
                resolved=resolved, meta=meta,
            )
            coll_pg = self._build_config_page(
                base_url=base_url, category="collections", total=coll_total,
                page=collections_page, page_size=page_size,
                extra_params={"depth": depth, "assets_page": assets_page},
            )
            coll_pg.items = coll_items
            categories["collections"] = coll_pg

            assets_total, assets_items = await self._list_assets(
                catalog_id, "", assets_page, page_size,
            )
            assets_pg = self._build_config_page(
                base_url=base_url, category="assets", total=assets_total,
                page=assets_page, page_size=page_size,
                extra_params={"depth": depth, "collections_page": collections_page},
            )
            assets_pg.items = assets_items
            categories["assets"] = assets_pg

        return CatalogConfigResponse(
            catalog_id=catalog_id, configs=tree, meta=meta_dict, categories=categories,
        )

    async def compose_platform_config(
        self,
        base_url: str,
        depth: int = 0,
        catalogs_page: int = 1,
        page_size: int = 15,
        resolved: bool = True,
        meta: bool = False,
    ) -> PlatformConfigResponse:
        by_class, sources = await self._get_effective_configs(
            catalog_id=None, collection_id=None, resolved=resolved,
        )
        self._build_routing_refs(by_class)
        tree, meta_dict = self._compose_tree(by_class, sources, "platform", meta)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0 and self._catalogs_service is not None:
            categories = {}
            offset = (catalogs_page - 1) * page_size
            try:
                cats = await self._catalogs_service.list_catalogs(
                    limit=page_size, offset=offset,
                )
                count_fn = getattr(self._catalogs_service, "count_catalogs", None)
                total = await count_fn() if count_fn is not None else len(cats)
            except Exception:
                cats, total = [], 0

            items: List[Any] = []
            for cat in cats:
                cat_id = cat.id if hasattr(cat, "id") else cat.get("id", "")
                if depth >= 2:
                    cat_response = await self.compose_catalog_config(
                        base_url=f"{base_url.rstrip('/')}/catalogs/{cat_id}",
                        catalog_id=cat_id, depth=depth - 1, page_size=page_size,
                        resolved=resolved, meta=meta,
                    )
                    items.append(cat_response.model_dump())
                else:
                    items.append({"catalog_id": cat_id})

            pg = self._build_config_page(
                base_url=base_url, category="catalogs", total=total,
                page=catalogs_page, page_size=page_size, extra_params={"depth": depth},
            )
            pg.items = items
            categories["catalogs"] = pg

        return PlatformConfigResponse(
            scope="platform", configs=tree, meta=meta_dict, categories=categories,
        )

    # --- PATCH. ---

    async def patch_config(
        self,
        *,
        catalog_id: Optional[str],
        body: Dict[str, Optional[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Validate-then-write partial update of one or more configs at a scope."""
        all_classes = list_registered_configs()
        prepared: List[Tuple[str, Type[PluginConfig], Optional[Dict[str, Any]]]] = []

        for plugin_id, value in body.items():
            cls = all_classes.get(plugin_id)
            if cls is None:
                raise ValueError(f"Unknown config class '{plugin_id}'")
            if value is None:
                prepared.append((plugin_id, cls, None))
                continue
            current = (await self._config_service.get_persisted_config(
                cls, catalog_id=catalog_id,
            )) or {}
            merged = {**current, **value}
            cls.model_validate(merged)  # raises on bad data
            prepared.append((plugin_id, cls, merged))

        for plugin_id, cls, merged in prepared:
            if merged is None:
                await self._config_service.delete_config(cls, catalog_id=catalog_id)
            else:
                validated = cls.model_validate(merged)
                await self._config_service.set_config(cls, validated, catalog_id=catalog_id)
        return {"updated": [p for p, _, _ in prepared]}

    # --- Pagination helpers. ---

    def _build_config_page(
        self,
        base_url: str,
        category: str,
        total: int,
        page: int,
        page_size: int,
        extra_params: Dict[str, Any],
    ) -> ConfigPage:
        links: List[Dict[str, str]] = []
        page_param = f"{category}_page"

        def _url(p: int) -> str:
            params = {**extra_params, page_param: p, "page_size": page_size}
            qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            sep = "&" if "?" in base_url else "?"
            return f"{base_url}{sep}{qs}"

        if page > 1:
            links.append({"rel": "prev", "href": _url(page - 1)})
        if page * page_size < total:
            links.append({"rel": "next", "href": _url(page + 1)})

        return ConfigPage(
            category=category, total=total, page=page, page_size=page_size,
            links=links, items=None,
        )

    async def _list_assets(
        self,
        catalog_id: str,
        collection_id: str,
        page: int,
        page_size: int,
    ) -> Tuple[int, List[Any]]:
        if self._assets_service is None:
            return 0, []
        offset = (page - 1) * page_size
        try:
            col_id = collection_id or None
            items = await self._assets_service.list_assets(
                catalog_id=catalog_id, collection_id=col_id,
                limit=page_size, offset=offset,
            )
            count_fn = getattr(self._assets_service, "count_assets", None)
            if count_fn is not None:
                total = await count_fn(catalog_id=catalog_id, collection_id=col_id)
            else:
                total = len(items)
            return total, [
                a.model_dump() if hasattr(a, "model_dump") else dict(a) for a in items
            ]
        except Exception:
            logger.debug("Could not list assets for %s/%s", catalog_id, collection_id)
            return 0, []

    async def _list_collections(
        self,
        base_url: str,
        catalog_id: str,
        page: int,
        page_size: int,
        parent_depth: int,
        resolved: bool = True,
        meta: bool = False,
    ) -> Tuple[int, List[Any]]:
        if self._catalogs_service is None:
            return 0, []
        offset = (page - 1) * page_size
        try:
            collections = await self._catalogs_service.list_collections(
                catalog_id=catalog_id, limit=page_size, offset=offset,
            )
            count_fn = getattr(self._catalogs_service, "count_collections", None)
            total = (
                await count_fn(catalog_id=catalog_id) if count_fn is not None
                else len(collections)
            )
        except Exception:
            logger.debug("Could not list collections for %s", catalog_id)
            return 0, []

        items: List[Any] = []
        for col in collections:
            col_id = col.id if hasattr(col, "id") else col.get("id", "")
            col_base = f"{base_url.rstrip('/')}/collections/{col_id}"
            next_depth = parent_depth - 1 if parent_depth >= 2 else 0
            col_response = await self.compose_collection_config(
                base_url=col_base, catalog_id=catalog_id, collection_id=col_id,
                depth=next_depth, page_size=page_size,
                resolved=resolved, meta=meta,
            )
            items.append(col_response.model_dump())
        return total, items
