"""Business logic for the centralised Config API composed views."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Type

from dynastore.extensions.configs.config_api_dto import (
    CatalogConfigResponse,
    CollectionConfigResponse,
    ConfigEntry,
    ConfigPage,
    PlatformConfigResponse,
    ResolvedDriverEntry,
)
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    list_registered_configs,
    resolve_config_class,
)
from dynastore.modules.storage.driver_registry import DriverRegistry

logger = logging.getLogger(__name__)

_ROUTING_CONFIG_KEYS = frozenset({"CollectionRoutingConfig", "AssetRoutingConfig"})


class ConfigApiService:
    """Composes paginated configuration responses for platform / catalog / collection scope."""

    def __init__(
        self,
        config_service: ConfigsProtocol,
        assets_service: Optional[Any] = None,
        catalogs_service: Optional[Any] = None,
    ):
        self._config_service = config_service
        self._assets_service = assets_service
        self._catalogs_service = catalogs_service

    async def _get_effective_configs(
        self,
        catalog_id: Optional[str],
        collection_id: Optional[str],
        resolved: bool = True,
    ) -> Dict[str, ConfigEntry]:
        configs_svc = self._config_service
        all_classes = list_registered_configs()

        if not resolved:
            # Return only configs explicitly stored at the innermost scope.
            # Values are validated through the Pydantic class (class defaults fill
            # in unset fields; parent-tier values are NOT inherited).
            if catalog_id and collection_id:
                result = await configs_svc.list_configs(
                    catalog_id=catalog_id, collection_id=collection_id, limit=1000, offset=0
                )
                scope_name = "collection"
            elif catalog_id:
                result = await configs_svc.list_configs(
                    catalog_id=catalog_id, limit=1000, offset=0
                )
                scope_name = "catalog"
            else:
                result = await configs_svc.list_configs(limit=1000, offset=0)
                scope_name = "platform"

            out: Dict[str, ConfigEntry] = {}
            for item in result.get("items", []):
                class_key: str = item["plugin_id"]
                cls = all_classes.get(class_key)
                raw: Dict[str, Any] = item.get("config_data") or {}
                if cls is not None:
                    try:
                        value: Dict[str, Any] = cls.model_validate(raw).model_dump()
                    except Exception:
                        value = raw
                else:
                    value = raw
                out[class_key] = ConfigEntry(  # type: ignore[call-arg]
                    class_key=class_key,
                    value=value,
                    source=scope_name,
                )
            return out

        collection_keys: set = set()
        catalog_keys: set = set()
        platform_keys: set = set()

        if catalog_id and collection_id:
            result = await configs_svc.list_configs(
                catalog_id=catalog_id, collection_id=collection_id, limit=1000, offset=0
            )
            collection_keys = {e["plugin_id"] for e in result.get("items", [])}

        if catalog_id:
            result = await configs_svc.list_configs(
                catalog_id=catalog_id, limit=1000, offset=0
            )
            catalog_keys = {e["plugin_id"] for e in result.get("items", [])}

        result = await configs_svc.list_configs(limit=1000, offset=0)
        platform_keys = {e["plugin_id"] for e in result.get("items", [])}

        out: Dict[str, ConfigEntry] = {}
        for class_key, cls in all_classes.items():
            if class_key in collection_keys:
                source = "collection"
            elif class_key in catalog_keys:
                source = "catalog"
            elif class_key in platform_keys:
                source = "platform"
            else:
                source = "default"

            try:
                effective = await configs_svc.get_config(
                    cls,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                value = effective.model_dump() if effective is not None else cls().model_dump()
            except Exception:
                try:
                    value = cls().model_dump()
                except Exception:
                    value = {}

            out[class_key] = ConfigEntry(  # type: ignore[call-arg]
                class_key=class_key,
                value=value,
                source=source,
            )

        return out

    async def _resolve_driver_configs(
        self,
        routing_value: Dict[str, Any],
        catalog_id: Optional[str],
        collection_id: Optional[str],
    ) -> Dict[str, List[ResolvedDriverEntry]]:
        driver_index = DriverRegistry.collection_index()
        operations: Dict[str, Any] = routing_value.get("operations", {})
        result: Dict[str, List[ResolvedDriverEntry]] = {}

        for operation, entries in operations.items():
            resolved: List[ResolvedDriverEntry] = []
            for entry in entries:
                driver_id: str = entry.get("driver_id", "")
                on_failure: str = entry.get("on_failure", "fatal")
                write_mode: str = entry.get("write_mode", "sync")

                driver_instance = driver_index.get(driver_id)
                config_class_key: Optional[str] = None
                config_value: Optional[Dict[str, Any]] = None

                if driver_instance is not None:
                    driver_cls = type(driver_instance)
                    config_cls = resolve_config_class(f"{driver_cls.__name__}Config")
                    if config_cls is None:
                        alt_key = driver_cls.__name__.replace("Driver", "DriverConfig")
                        config_cls = resolve_config_class(alt_key)
                    if config_cls is not None:
                        try:
                            config_class_key = config_cls.class_key()
                        except Exception:
                            config_class_key = config_cls.__name__
                        try:
                            eff = await self._config_service.get_config(
                                config_cls,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                            )
                            if eff is not None:
                                config_value = eff.model_dump()
                        except Exception:
                            logger.debug(
                                "Could not resolve config for driver %s", driver_id
                            )

                resolved.append(
                    ResolvedDriverEntry(
                        driver_id=driver_id,
                        on_failure=on_failure,
                        write_mode=write_mode,
                        config_class_key=config_class_key,
                        config=config_value,
                    )
                )
            result[operation] = resolved

        return result

    async def _enrich_routing_configs(
        self,
        configs: Dict[str, ConfigEntry],
        catalog_id: Optional[str],
        collection_id: Optional[str],
    ) -> None:
        for class_key, entry in configs.items():
            if class_key in _ROUTING_CONFIG_KEYS:
                try:
                    entry.resolved_drivers = await self._resolve_driver_configs(
                        routing_value=entry.value,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                    )
                except Exception:
                    logger.debug("Could not resolve drivers for %s", class_key)

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

        return ConfigPage(  # type: ignore[call-arg]
            category=category,
            total=total,
            page=page,
            page_size=page_size,
            links=links,
        )

    async def compose_collection_config(
        self,
        base_url: str,
        catalog_id: str,
        collection_id: str,
        depth: int = 0,
        assets_page: int = 1,
        page_size: int = 15,
        resolved: bool = True,
    ) -> CollectionConfigResponse:
        configs = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=collection_id, resolved=resolved
        )
        await self._enrich_routing_configs(configs, catalog_id, collection_id)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0:
            categories = {}
            assets_total, assets_items = await self._list_assets(
                catalog_id, collection_id, assets_page, page_size
            )
            pg = self._build_config_page(
                base_url=base_url,
                category="assets",
                total=assets_total,
                page=assets_page,
                page_size=page_size,
                extra_params={"depth": depth},
            )
            pg.items = assets_items
            categories["assets"] = pg

        return CollectionConfigResponse(
            collection_id=collection_id,
            catalog_id=catalog_id,
            configs=configs,
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
    ) -> CatalogConfigResponse:
        configs = await self._get_effective_configs(
            catalog_id=catalog_id, collection_id=None, resolved=resolved
        )
        await self._enrich_routing_configs(configs, catalog_id, None)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0:
            categories = {}
            coll_total, coll_items = await self._list_collections(
                base_url, catalog_id, collections_page, page_size, depth, resolved=resolved
            )
            coll_pg = self._build_config_page(
                base_url=base_url,
                category="collections",
                total=coll_total,
                page=collections_page,
                page_size=page_size,
                extra_params={"depth": depth, "assets_page": assets_page},
            )
            coll_pg.items = coll_items
            categories["collections"] = coll_pg

            assets_total, assets_items = await self._list_assets(
                catalog_id, "", assets_page, page_size
            )
            assets_pg = self._build_config_page(
                base_url=base_url,
                category="assets",
                total=assets_total,
                page=assets_page,
                page_size=page_size,
                extra_params={"depth": depth, "collections_page": collections_page},
            )
            assets_pg.items = assets_items
            categories["assets"] = assets_pg

        return CatalogConfigResponse(
            catalog_id=catalog_id, configs=configs, categories=categories
        )

    async def compose_platform_config(
        self,
        base_url: str,
        depth: int = 0,
        catalogs_page: int = 1,
        page_size: int = 15,
        resolved: bool = True,
    ) -> PlatformConfigResponse:
        configs = await self._get_effective_configs(
            catalog_id=None, collection_id=None, resolved=resolved
        )
        await self._enrich_routing_configs(configs, None, None)

        categories: Optional[Dict[str, ConfigPage]] = None
        if depth > 0 and self._catalogs_service is not None:
            categories = {}
            offset = (catalogs_page - 1) * page_size
            try:
                cats = await self._catalogs_service.list_catalogs(
                    limit=page_size, offset=offset
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
                        base_url=f"{base_url.rstrip('/')}/catalogs/{cat_id}/config",
                        catalog_id=cat_id,
                        depth=depth - 1,
                        page_size=page_size,
                        resolved=resolved,
                    )
                    items.append(cat_response.model_dump())
                else:
                    items.append({"catalog_id": cat_id})

            pg = self._build_config_page(
                base_url=base_url,
                category="catalogs",
                total=total,
                page=catalogs_page,
                page_size=page_size,
                extra_params={"depth": depth},
            )
            pg.items = items
            categories["catalogs"] = pg

        return PlatformConfigResponse(configs=configs, categories=categories)  # type: ignore[call-arg]

    async def patch_config(
        self,
        *,
        catalog_id: Optional[str],
        body: Dict[str, Optional[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Apply a partial update to one or more configs at the given scope.

        Validates ALL entries before writing any of them. A 422 on any entry
        means zero writes occur (validate-first, write-second approach rather
        than bulk_write(), which does not exist on ConfigsProtocol).
        """
        all_classes = list_registered_configs()
        prepared: List[Tuple[str, Type, Optional[Dict[str, Any]]]] = []

        for plugin_id, value in body.items():
            cls = all_classes.get(plugin_id)
            if cls is None:
                raise ValueError(f"Unknown plugin_id '{plugin_id}'")
            if value is None:
                prepared.append((plugin_id, cls, None))
                continue
            # Merge incoming partial dict over the class defaults, then validate.
            current = await self._config_service.get_config(cls, catalog_id=catalog_id)
            current_data = current.model_dump() if current is not None else cls().model_dump()
            merged = {**current_data, **value}
            cls.model_validate(merged)  # raises ValidationError on bad data
            prepared.append((plugin_id, cls, merged))

        # All entries validated — now write sequentially.
        for plugin_id, cls, merged in prepared:
            if merged is None:
                await self._config_service.delete_config(cls, catalog_id=catalog_id)
            else:
                validated = cls.model_validate(merged)
                await self._config_service.set_config(cls, validated, catalog_id=catalog_id)

        return {"updated": [p for p, _, _ in prepared]}

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
                catalog_id=catalog_id,
                collection_id=col_id,
                limit=page_size,
                offset=offset,
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
            logger.debug(
                "Could not list assets for %s/%s", catalog_id, collection_id
            )
            return 0, []

    async def _list_collections(
        self,
        base_url: str,
        catalog_id: str,
        page: int,
        page_size: int,
        parent_depth: int,
        resolved: bool = True,
    ) -> Tuple[int, List[Any]]:
        if self._catalogs_service is None:
            return 0, []
        offset = (page - 1) * page_size
        try:
            collections = await self._catalogs_service.list_collections(
                catalog_id=catalog_id, limit=page_size, offset=offset
            )
            count_fn = getattr(self._catalogs_service, "count_collections", None)
            if count_fn is not None:
                total = await count_fn(catalog_id=catalog_id)
            else:
                total = len(collections)
        except Exception:
            logger.debug("Could not list collections for %s", catalog_id)
            return 0, []

        items: List[Any] = []
        for col in collections:
            col_id = col.id if hasattr(col, "id") else col.get("id", "")
            col_base = f"{base_url.rstrip('/')}/collections/{col_id}/config"
            next_depth = parent_depth - 1 if parent_depth >= 2 else 0
            col_response = await self.compose_collection_config(
                base_url=col_base,
                catalog_id=catalog_id,
                collection_id=col_id,
                depth=next_depth,
                page_size=page_size,
                resolved=resolved,
            )
            items.append(col_response.model_dump())
        return total, items
