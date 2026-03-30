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
Storage Router — resolves the correct ``CollectionStorageDriverProtocol``
for a given catalog/collection based on ``CollectionPluginConfig``.

Resolution order:
1. write=True → write_driver (always)
2. read_drivers[hint] → explicit mapping
3. Auto-select: driver whose preferred_for includes this hint
4. Fallback → write_driver

Performance: driver lookup is cached (300s TTL, same as config) to
avoid repeated linear scans on every request.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from dynastore.models.protocols.storage_driver import (
    Capability,
    CollectionStorageDriverProtocol,
)
from dynastore.modules.storage.errors import ReadOnlyDriverError
from dynastore.modules.storage.hints import ReadHint
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


def _build_driver_index() -> Dict[str, CollectionStorageDriverProtocol]:
    """Build a driver_id -> driver instance lookup dict."""
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(CollectionStorageDriverProtocol)}


def _get_collection_drivers(
    col_config: Any,
    driver_index: Dict[str, CollectionStorageDriverProtocol],
) -> List[CollectionStorageDriverProtocol]:
    """Return the write driver + all secondary driver instances for this collection."""
    drivers = []
    write_driver = driver_index.get(col_config.write_driver_id)
    if write_driver:
        drivers.append(write_driver)
    for driver_id in col_config.secondary_driver_ids:
        d = driver_index.get(driver_id)
        if d:
            drivers.append(d)
    return drivers


def _resolve_driver_id(
    col_config: Any,
    *,
    hint: str,
    write: bool,
    driver_index: Dict[str, CollectionStorageDriverProtocol],
) -> str:
    """Pure function: pick driver_id from collection config + intent.

    Resolution order:
    1. write=True → write_driver (always)
    2. read_drivers[hint] → explicit mapping
    3. read_drivers["default"] → explicit default mapping
    4. Auto-select: driver whose preferred_for includes this hint
    5. Fallback → write_driver
    """
    if write:
        return col_config.write_driver_id

    # 1. Explicit hint mapping
    ref = col_config.read_drivers.get(hint)
    if ref:
        return ref.driver_id

    # 2. Explicit default mapping
    default_ref = col_config.read_drivers.get("default")
    if default_ref:
        return default_ref.driver_id

    # 3. Auto-select by preferred_for from available drivers
    available_drivers = _get_collection_drivers(col_config, driver_index)
    for driver in available_drivers:
        preferred = getattr(driver, "preferred_for", frozenset())
        if hint in preferred:
            return driver.driver_id

    # 4. Fallback to write_driver
    return col_config.write_driver_id


@cached(maxsize=4096, ttl=300, namespace="storage_router", distributed=False)
async def _resolve_driver_cached(
    catalog_id: str,
    collection_id: Optional[str],
    hint: str,
    write: bool,
) -> str:
    """Cached resolution: (catalog, collection, hint, write) -> driver_id."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.catalog.catalog_config import COLLECTION_PLUGIN_CONFIG_ID

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise RuntimeError("ConfigsProtocol not available — cannot resolve storage routing")

    col_config = await configs.get_config(
        COLLECTION_PLUGIN_CONFIG_ID,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    driver_index = _build_driver_index()
    return _resolve_driver_id(col_config, hint=hint, write=write, driver_index=driver_index)


async def get_driver(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Union[ReadHint, str] = ReadHint.DEFAULT,
    write: bool = False,
) -> CollectionStorageDriverProtocol:
    """Look up the correct storage driver for a collection.

    Args:
        catalog_id: The catalog that owns the collection.
        collection_id: Optional collection within the catalog.
        hint: Read-intent hint (``ReadHint`` enum or custom string).
            Ignored when ``write=True``.
        write: If ``True``, always returns the write driver.

    Returns:
        A ``CollectionStorageDriverProtocol`` instance.

    Raises:
        ValueError: If the resolved driver ID is not registered.
        ReadOnlyDriverError: If ``write=True`` and driver lacks WRITE capability.
    """
    hint_str = hint.value if isinstance(hint, ReadHint) else str(hint)
    driver_id = await _resolve_driver_cached(catalog_id, collection_id, hint_str, write)

    index = _build_driver_index()
    driver = index.get(driver_id)
    if not driver:
        raise ValueError(
            f"Storage driver '{driver_id}' not found or not available. "
            f"Registered drivers: {list(index.keys())}"
        )

    if write and Capability.WRITE not in driver.capabilities:
        raise ReadOnlyDriverError(
            f"Driver '{driver_id}' does not support writes (missing Capability.WRITE)"
        )

    return driver
