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
for a given catalog/collection based on ``StorageRoutingConfig``.

Performance: driver lookup is cached (300s TTL, same as config) to
avoid repeated linear scans on every request.
"""

import logging
from typing import Dict, Optional, Union

from dynastore.models.protocols.storage_driver import (
    Capability,
    CollectionStorageDriverProtocol,
)
from dynastore.modules.storage.config import STORAGE_ROUTING_CONFIG_ID, StorageRoutingConfig
from dynastore.modules.storage.errors import ReadOnlyDriverError
from dynastore.modules.storage.hints import ReadHint
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


def _build_driver_index() -> Dict[str, CollectionStorageDriverProtocol]:
    """Build a driver_id -> driver instance lookup dict."""
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(CollectionStorageDriverProtocol)}


def _resolve_driver_id(routing: StorageRoutingConfig, *, hint: str, write: bool) -> str:
    """Pure function: pick driver_id from routing config + intent."""
    if write:
        return routing.primary_driver_id
    return routing.resolve_read_driver_id(hint)


@cached(maxsize=4096, ttl=300, namespace="storage_router")
async def _resolve_driver_cached(
    catalog_id: str,
    collection_id: Optional[str],
    hint: str,
    write: bool,
) -> str:
    """Cached resolution: (catalog, collection, hint, write) -> driver_id."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise RuntimeError("ConfigsProtocol not available — cannot resolve storage routing")

    routing = await configs.get_config(
        STORAGE_ROUTING_CONFIG_ID,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    return _resolve_driver_id(routing, hint=hint, write=write)


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
        write: If ``True``, always returns the primary driver.

    Returns:
        A ``CollectionStorageDriverProtocol`` instance.

    Raises:
        ValueError: If the resolved driver ID is not registered.
        ReadOnlyDriverError: If ``write=True`` and driver is read-only.
    """
    hint_str = hint.value if isinstance(hint, ReadHint) else hint
    driver_id = await _resolve_driver_cached(catalog_id, collection_id, hint_str, write)

    index = _build_driver_index()
    driver = index.get(driver_id)
    if not driver:
        raise ValueError(
            f"Storage driver '{driver_id}' not found or not available. "
            f"Registered drivers: {list(index.keys())}"
        )

    if write and Capability.READ_ONLY in driver.capabilities:
        raise ReadOnlyDriverError(
            f"Driver '{driver_id}' is read-only and cannot handle writes"
        )

    return driver
