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
Metadata driver routing — resolves the active CollectionMetadataDriverProtocol.

Resolution strategy:

1. Check ``RoutingPluginConfig.metadata.override`` (plugin_id ``"collection:drivers"``).
   If override entries are configured, use the first available driver.

2. If ``metadata.override`` is empty, fall back to protocol discovery:
   discover all ``CollectionMetadataDriverProtocol`` implementations, prefer
   ES if available, otherwise PG.

3. **First resolution only**: if the preferred driver is unavailable
   (``is_available()`` returns False) and a fallback driver exists, use the
   fallback.  After the first successful resolution, the choice is cached.
   If the config declares a specific driver and it's not available → error.

The resolved metadata driver is cached per catalog (300 s TTL).
"""

import logging
from typing import Dict, Optional

from dynastore.models.protocols.metadata_driver import CollectionMetadataDriverProtocol
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)

# Preferred driver ordering for auto-discovery (first available wins)
_PREFERRED_DRIVER_ORDER = ["elasticsearch_metadata", "postgresql_metadata"]


def _build_metadata_driver_index() -> Dict[str, CollectionMetadataDriverProtocol]:
    """Build driver_id → driver instance lookup for metadata drivers."""
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(CollectionMetadataDriverProtocol)}


@cached(maxsize=256, ttl=300, namespace="metadata_router", distributed=False)
async def _resolve_metadata_driver_cached(
    catalog_id: str,
) -> Optional[str]:
    """Cached resolution: returns the driver_id of the active metadata driver.

    Returns None if no metadata driver is available.
    """
    # 1. Try routing config (RoutingPluginConfig.metadata.override)
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.routing_config import ROUTING_PLUGIN_CONFIG_ID
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs:
            routing_config = await configs.get_config(
                ROUTING_PLUGIN_CONFIG_ID, catalog_id=catalog_id
            )
            entries = routing_config.metadata.override
            if entries:
                # Config explicitly declares override metadata drivers — use first available.
                driver_index = _build_metadata_driver_index()
                for entry in entries:
                    driver = driver_index.get(entry.driver_id)
                    if driver:
                        if await driver.is_available():
                            return driver.driver_id
                        # Config declares this driver but it's unavailable → error
                        raise RuntimeError(
                            f"Metadata driver '{entry.driver_id}' is configured "
                            f"for catalog '{catalog_id}' but is not available"
                        )
                    logger.warning(
                        "Metadata driver '%s' from routing config is not registered",
                        entry.driver_id,
                    )
    except RuntimeError:
        raise  # re-raise explicit unavailability errors
    except Exception as e:
        logger.debug("Metadata routing config lookup failed for '%s': %s", catalog_id, e)

    # 2. Auto-discover: try preferred order, first available wins
    driver_index = _build_metadata_driver_index()
    for driver_id in _PREFERRED_DRIVER_ORDER:
        driver = driver_index.get(driver_id)
        if driver and await driver.is_available():
            logger.info(
                "Auto-discovered metadata driver '%s' for catalog '%s'",
                driver_id, catalog_id,
            )
            return driver_id

    # 3. Try any registered metadata driver
    for driver_id, driver in driver_index.items():
        if await driver.is_available():
            logger.info(
                "Using metadata driver '%s' (fallback) for catalog '%s'",
                driver_id, catalog_id,
            )
            return driver_id

    return None


async def get_metadata_driver(
    catalog_id: str,
) -> Optional[CollectionMetadataDriverProtocol]:
    """Resolve the active metadata driver for the given catalog.

    Returns the driver instance or None if no metadata driver is available.
    """
    driver_id = await _resolve_metadata_driver_cached(catalog_id)
    if not driver_id:
        return None

    driver_index = _build_metadata_driver_index()
    return driver_index.get(driver_id)


def invalidate_metadata_router_cache(
    catalog_id: Optional[str] = None,
) -> None:
    """Invalidate cached metadata driver resolution."""
    try:
        from dynastore.tools.cache import cache_clear

        cache_clear(_resolve_metadata_driver_cached)
    except Exception:
        pass
