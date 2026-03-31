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
Asset Router — resolves the correct ``AssetDriverProtocol`` for a given
catalog/collection based on ``AssetPluginConfig``.

Resolution order:
1. write=True → write_driver (always)
2. read_drivers[hint] → explicit mapping
3. read_drivers["default"] → explicit default mapping
4. Auto-select: driver whose preferred_for includes this hint
5. Fallback → write_driver

Performance: driver lookup is cached (300 s TTL, same as config cache) to
avoid repeated config lookups on every request.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from dynastore.models.protocols.asset_driver import AssetDriverProtocol
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


def _build_asset_driver_index() -> Dict[str, AssetDriverProtocol]:
    """Build a driver_id → driver instance lookup dict."""
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(AssetDriverProtocol)}


def _get_asset_drivers(
    asset_config: Any,
    driver_index: Dict[str, AssetDriverProtocol],
) -> List[AssetDriverProtocol]:
    """Return the write driver + all secondary driver instances for this collection."""
    drivers = []
    write_driver = driver_index.get(asset_config.write_driver_id)
    if write_driver:
        drivers.append(write_driver)
    for driver_id in asset_config.secondary_driver_ids:
        d = driver_index.get(driver_id)
        if d:
            drivers.append(d)
    return drivers


def _resolve_asset_driver_id(
    asset_config: Any,
    *,
    hint: str,
    write: bool,
    driver_index: Dict[str, AssetDriverProtocol],
) -> str:
    """Pure function: pick driver_id from asset config + intent.

    Resolution order:
    1. write=True → write_driver (always)
    2. read_drivers[hint] → explicit mapping
    3. read_drivers["default"] → explicit default
    4. Auto-select: driver whose preferred_for includes this hint
    5. Fallback → write_driver
    """
    if write:
        return asset_config.write_driver_id

    ref = asset_config.read_drivers.get(hint)
    if ref:
        return ref.driver_id

    default_ref = asset_config.read_drivers.get("default")
    if default_ref:
        return default_ref.driver_id

    available_drivers = _get_asset_drivers(asset_config, driver_index)
    for driver in available_drivers:
        preferred = getattr(driver, "preferred_for", frozenset())
        if hint in preferred:
            return driver.driver_id

    return asset_config.write_driver_id


@cached(maxsize=4096, ttl=300, namespace="asset_router", distributed=False)
async def _resolve_asset_driver_cached(
    catalog_id: str,
    collection_id: Optional[str],
    hint: str,
    write: bool,
) -> str:
    """Cached resolution: (catalog, collection, hint, write) -> driver_id."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.catalog.asset_config import ASSET_PLUGIN_CONFIG_ID

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise RuntimeError("ConfigsProtocol not available — cannot resolve asset routing")

    asset_config = await configs.get_config(
        ASSET_PLUGIN_CONFIG_ID,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    driver_index = _build_asset_driver_index()
    return _resolve_asset_driver_id(
        asset_config, hint=hint, write=write, driver_index=driver_index
    )


def invalidate_asset_router_cache(
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> None:
    """Invalidate cached resolution for this catalog/collection.

    Called by ``_on_apply_asset_config`` when ``AssetPluginConfig`` changes.
    """
    try:
        _resolve_asset_driver_cached.invalidate(  # type: ignore[attr-defined]
            catalog_id, collection_id
        )
    except Exception:
        # Best-effort: full cache clear if per-key invalidation not supported
        try:
            _resolve_asset_driver_cached.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass


async def get_asset_driver(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Union[str] = "default",
    write: bool = False,
) -> AssetDriverProtocol:
    """Look up the correct asset driver for a catalog/collection.

    Args:
        catalog_id:    The catalog that owns the assets.
        collection_id: Optional collection within the catalog.
        hint:          Read-intent hint (``"default"``, ``"search"``,
                       ``"metadata"``, or a custom string).
                       Ignored when ``write=True``.
        write:         If ``True``, always returns the write driver.

    Returns:
        An ``AssetDriverProtocol`` instance.

    Raises:
        ValueError: If the resolved driver ID is not registered.
    """
    hint_str = str(hint)
    driver_id = await _resolve_asset_driver_cached(
        catalog_id, collection_id, hint_str, write
    )

    index = _build_asset_driver_index()
    driver = index.get(driver_id)
    if not driver:
        raise ValueError(
            f"Asset driver '{driver_id}' not found or not available. "
            f"Registered drivers: {list(index.keys())}"
        )

    return driver
