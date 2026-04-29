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

Placement is read from each ``PluginConfig`` subclass's mandatory
``_address: ClassVar[Tuple[str, str, Optional[str]]]`` ClassVar.
Scope filtering is read from the optional ``_visibility`` ClassVar
(``"collection"`` / ``"catalog"`` / ``None`` = visible everywhere).
The composer no longer owns a placement heuristic — there is exactly
one source of truth per class.
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


def _routing_config_keys() -> frozenset[str]:
    """Routing config wire keys (snake_case via ``class_key()``).

    Derived once on first call so renaming any routing class flows through
    without touching the composer.  No hardcoded PascalCase strings.
    """
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        CatalogRoutingConfig,
        CollectionRoutingConfig,
    )
    return frozenset({
        cls.class_key() for cls in (
            CollectionRoutingConfig, AssetRoutingConfig, CatalogRoutingConfig,
        )
    })


def _place(cls: Type[PluginConfig], active_scope: str) -> Optional[Tuple[str, str, Optional[str]]]:
    """Return ``cls._address`` if visible at ``active_scope``, else ``None``.

    Filters:
    - Abstract bases (``is_abstract_base = True``) → dropped.
    - ``_visibility = "collection"`` and ``active_scope != "collection"`` → dropped.
    - ``_visibility = "catalog"`` and ``active_scope == "collection"`` → dropped.
    """
    if cls.__dict__.get("is_abstract_base", False):
        return None
    visibility = getattr(cls, "_visibility", None)
    if visibility == "collection" and active_scope != "collection":
        return None
    if visibility == "catalog" and active_scope == "collection":
        return None
    address = getattr(cls, "_address", None)
    if not address or address == ("", "", None):
        # Defensive — concrete subclasses must declare _address (enforced in
        # PluginConfig.__init_subclass__) but skip rather than crash if a
        # malformed entry slips through.
        logger.warning(
            "PluginConfig %s.%s has no _address; skipping placement.",
            cls.__module__, cls.__name__,
        )
        return None
    return address


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
        """Bucket visible classes into scope/topic tree and optionally build meta.

        At collection scope, catalog-tier configs (``_visibility = "catalog"``)
        are surfaced under a sibling ``inherited_from_catalog`` block instead
        of being dropped — the operator sees which catalog-tier configs
        influence this collection (e.g. ``ElasticsearchCatalogConfig.private``
        decides whether items go to the public or private ES index, but is
        editable only at catalog scope).  The block uses the SAME
        ``scope/topic/sub`` shape as the main tree so dashboards can render
        it with the same form-builder.
        """
        all_classes = list_registered_configs()
        tree: Dict[str, Any] = {}
        meta: Dict[str, ConfigMeta] = {}
        for class_key, payload in by_class.items():
            cls = all_classes.get(class_key)
            if cls is None:
                continue

            # At collection scope, intercept catalog-visibility configs and
            # bucket them under ``inherited_from_catalog`` instead of dropping
            # them via _place().  Same scope/topic/sub address as in the main
            # tree so the rendering is symmetric.
            if (
                active_scope == "collection"
                and getattr(cls, "_visibility", None) == "catalog"
                and not cls.__dict__.get("is_abstract_base", False)
            ):
                address = getattr(cls, "_address", None)
                if address and address != ("", "", None):
                    scope, topic, sub = address
                    inherited = tree.setdefault("inherited_from_catalog", {})
                    topic_node = inherited.setdefault(scope, {}).setdefault(topic, {})
                    if sub is None:
                        topic_node[class_key] = payload
                    else:
                        topic_node.setdefault(sub, {})[class_key] = payload
                    if include_meta and class_key in sources:
                        meta[class_key] = ConfigMeta(source=sources[class_key])
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
        bind, ``class_key()`` for ``XDriverConfig`` returns the snake_case form
        of the bound driver class (e.g. ``"items_postgresql_driver"``) — the
        same string used as ``driver_id`` in routing entries.  ``driver_id`` IS
        the lookup key into ``list_registered_configs()``.
        """
        all_classes = list_registered_configs()
        for class_key in _routing_config_keys():
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

    @staticmethod
    async def _build_routing_resolution(
        catalog_id: str, collection_id: str,
    ) -> Dict[str, Dict[str, Dict[str, str]]]:
        """Resolve which DRIVER actually fires per items operation at this
        collection — the operator's "where do my items go?" answer.

        Currently covers ``items.{WRITE,READ,SEARCH}`` and factors in the
        Elasticsearch private-mode resolution (``ElasticsearchCatalogConfig.private``
        + per-collection override).  Other entities (assets, catalog metadata)
        will be added as their resolvers stabilise.

        Returns ``{entity: {op: {driver_id, reason}}}`` — empty when the
        ``is_collection_private`` resolver isn't reachable (e.g. in tests
        with no ConfigsProtocol).  Never raises.
        """
        try:
            from dynastore.modules.elasticsearch.es_collection_config import (
                is_collection_private,
            )
            private = await is_collection_private(catalog_id, collection_id)
        except Exception as exc:
            logger.debug(
                "routing_resolution: is_collection_private fetch failed for "
                "%s/%s (%s) — emitting empty block",
                catalog_id, collection_id, exc,
            )
            return {}

        if private:
            items_driver = "items_elasticsearch_private_driver"
            reason = (
                "ElasticsearchCatalogConfig.private=True (or per-collection "
                "override) — private mode active for this collection"
            )
        else:
            items_driver = "items_elasticsearch_driver"
            reason = "private mode not active — public items index"

        return {
            "items": {
                "WRITE":  {"driver_id": items_driver, "reason": reason},
                "READ":   {"driver_id": items_driver, "reason": reason},
                "SEARCH": {"driver_id": items_driver, "reason": reason},
            },
        }

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

        # Phase 1.5d: surface per-op driver resolution alongside the per-class
        # tier-of-origin diagnostics in ``meta``.  Async (factors in
        # ``is_collection_private``); kept opt-in via the existing
        # ``?meta=true`` query so the cheap ``meta=False`` path stays cheap.
        routing_resolution: Optional[Dict[str, Dict[str, Dict[str, str]]]] = None
        if meta:
            routing_resolution = await self._build_routing_resolution(
                catalog_id, collection_id,
            ) or None

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
            configs=tree, meta=meta_dict,
            routing_resolution=routing_resolution,
            categories=categories,
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
        collection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate-then-write partial update of one or more configs at a scope.

        Body is RFC 7396 merge-patch over the scope's plugin set: each top-level
        key is a ``plugin_id``; value is the new payload, or ``null`` to delete
        the override.  Atomic at the scope level — the validation pass runs to
        completion before any write fires.
        """
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
                cls, catalog_id=catalog_id, collection_id=collection_id,
            )) or {}
            merged = {**current, **value}
            cls.model_validate(merged)  # raises on bad data
            prepared.append((plugin_id, cls, merged))

        for plugin_id, cls, merged in prepared:
            if merged is None:
                await self._config_service.delete_config(
                    cls, catalog_id=catalog_id, collection_id=collection_id,
                )
            else:
                validated = cls.model_validate(merged)
                await self._config_service.set_config(
                    cls, validated, catalog_id=catalog_id, collection_id=collection_id,
                )
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
