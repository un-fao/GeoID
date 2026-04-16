"""Business logic for the deep paginated configuration view."""

import logging
from typing import Dict, Optional

from dynastore.extensions.configs.deep_dto import ConfigViewEntry
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import list_registered_configs

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
