"""Business logic for the deep paginated configuration view."""

import logging
from typing import Any, Dict, List, Optional

from dynastore.extensions.configs.deep_dto import ConfigViewEntry, ResolvedDriverEntry
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    list_registered_configs,
    resolve_config_class,
)
from dynastore.modules.storage.driver_registry import DriverRegistry

logger = logging.getLogger(__name__)

_ROUTING_CONFIG_KEYS = frozenset({"CollectionRoutingConfig", "AssetRoutingConfig"})


class DeepConfigService:
    """Composes deep, paginated configuration views for platform / catalog / collection scope."""

    def __init__(self, config_service: ConfigsProtocol):
        self._config_service = config_service

    async def _get_effective_configs(
        self,
        catalog_id: Optional[str],
        collection_id: Optional[str],
    ) -> Dict[str, ConfigViewEntry]:
        configs_svc = self._config_service
        all_classes = list_registered_configs()

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

        out: Dict[str, ConfigViewEntry] = {}
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

            out[class_key] = ConfigViewEntry(
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
        configs: Dict[str, ConfigViewEntry],
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
