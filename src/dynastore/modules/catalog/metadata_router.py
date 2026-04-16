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
Metadata driver routing — resolves CollectionMetadataStore drivers.

Two entry points, both read from ``CollectionRoutingConfig.metadata.operations``
via the waterfall and fall back to class-level code defaults:

1. ``resolve_metadata_drivers(catalog_id, operation)`` — returns the ordered
   list of available ``CollectionMetadataStore`` instances for ``operation``
   (READ or WRITE). This is the protocol-driven entry point used by every
   catalog/collection service when persisting or fetching metadata.

2. ``get_metadata_driver(catalog_id)`` — legacy single-driver READ resolver,
   preserved for call-sites that only need "the" reader. Equivalent to
   ``resolve_metadata_drivers(catalog_id, Operation.READ)[0]``.

Default behaviour when ``metadata.operations[op]`` is empty (fresh deploy, no
platform override):

- READ  → discover registered drivers, prefer ES then PG (first available).
- WRITE → PG-only (authoritative writer); ES is purely a read-path store.

If the configured operation declares a driver that is not registered or not
available, we raise ``ConfigResolutionError`` — the deploy is missing the
driver or its dependency. Empty resolution on WRITE is also a ``ConfigResolutionError``
(no way to persist metadata at all).

The resolved driver list is cached per ``(catalog_id, operation)`` with a
300 s TTL. Cache is invalidated via ``invalidate_metadata_router_cache``.
"""

import logging
from typing import Dict, List, Optional

from dynastore.models.protocols.metadata_driver import CollectionMetadataStore
from dynastore.modules.db_config.exceptions import ConfigResolutionError
from dynastore.modules.storage.routing_config import Operation
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)

# Preferred driver ordering for auto-discovery (first available wins)
_PREFERRED_DRIVER_ORDER = ["MetadataElasticsearchDriver", "MetadataPostgresqlDriver"]

# Default WRITE target when no config: PG is the authoritative writer;
# ES is a read-path accelerator that should be populated via TRANSFORM,
# not as a primary writer.
_DEFAULT_WRITE_DRIVERS = ["MetadataPostgresqlDriver"]


def _build_metadata_driver_index() -> Dict[str, CollectionMetadataStore]:
    """Build driver_id → driver instance lookup for metadata drivers."""
    from dynastore.tools.discovery import get_protocols

    return {type(d).__name__: d for d in get_protocols(CollectionMetadataStore)}


async def _resolve_metadata_driver_ids(
    catalog_id: str,
    operation: Operation,
) -> List[str]:
    """Return the ordered driver_ids to use for ``operation``.

    Priority:
      1. ``CollectionRoutingConfig.metadata.operations[operation]`` if non-empty.
      2. Code-level defaults:
         - READ  → ``_PREFERRED_DRIVER_ORDER`` (ES then PG)
         - WRITE → ``_DEFAULT_WRITE_DRIVERS`` (PG-primary)

    The caller is responsible for filtering by ``is_available()`` and for
    raising ``ConfigResolutionError`` if the filtered list is empty for WRITE.
    """
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.routing_config import CollectionRoutingConfig
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs:
            routing_config = await configs.get_config(
                CollectionRoutingConfig, catalog_id=catalog_id
            )
            entries = routing_config.metadata.operations.get(operation, [])
            if entries:
                return [e.driver_id for e in entries]
    except ConfigResolutionError:
        raise
    except Exception as exc:
        logger.debug(
            "Metadata routing config lookup failed for '%s'/%s: %s",
            catalog_id, operation, exc,
        )

    if operation == Operation.READ:
        return list(_PREFERRED_DRIVER_ORDER)
    if operation == Operation.WRITE:
        return list(_DEFAULT_WRITE_DRIVERS)
    return []


async def resolve_metadata_drivers(
    catalog_id: str,
    operation: Operation = Operation.READ,
) -> List[CollectionMetadataStore]:
    """Resolve the ordered list of available metadata drivers for ``operation``.

    For READ: ordered by priority; callers should read from position 0 first,
    falling back to subsequent entries only if the primary lacks the data.

    For WRITE: every returned driver must receive the write (fan-out). An
    empty list here is a deploy misconfiguration and raises
    ``ConfigResolutionError``.
    """
    driver_ids = await _resolve_metadata_driver_ids(catalog_id, operation)
    driver_index = _build_metadata_driver_index()

    resolved: List[CollectionMetadataStore] = []
    missing: List[str] = []
    for driver_id in driver_ids:
        driver = driver_index.get(driver_id)
        if driver is None:
            missing.append(driver_id)
            continue
        if await driver.is_available():
            resolved.append(driver)
        else:
            missing.append(f"{driver_id}(unavailable)")

    if resolved:
        return resolved

    # No driver from the configured/default list is available. For WRITE this
    # is an ops error — we have no way to persist. For READ we allow an empty
    # result (readers will emit 404 on the actual resource, not on routing).
    if operation == Operation.WRITE:
        raise ConfigResolutionError(
            (
                f"No available CollectionMetadataStore for WRITE on catalog "
                f"'{catalog_id}'. Configured/default drivers: {driver_ids}. "
                f"Missing/unavailable: {missing}."
            ),
            missing_key="CollectionRoutingConfig.metadata.operations[WRITE]",
            required_fields=[],
            scope_tried=["collection", "catalog", "platform", "code_default"],
            hint=(
                "Register a CollectionMetadataStore driver (e.g. "
                "MetadataPostgresqlDriver) and ensure its is_available() "
                "returns True, or configure "
                "CollectionRoutingConfig.metadata.operations[WRITE] explicitly."
            ),
        )
    logger.warning(
        "No available CollectionMetadataStore for READ on catalog '%s'; "
        "tried %s (missing/unavailable: %s)",
        catalog_id, driver_ids, missing,
    )
    return []


@cached(maxsize=256, ttl=300, namespace="metadata_router", distributed=False)
async def _resolve_metadata_driver_cached(
    catalog_id: str,
) -> Optional[str]:
    """Cached single-driver READ resolution: returns a driver_id."""
    drivers = await resolve_metadata_drivers(catalog_id, operation=Operation.READ)
    if not drivers:
        return None
    return type(drivers[0]).__name__


async def get_metadata_driver(
    catalog_id: str,
) -> Optional[CollectionMetadataStore]:
    """Resolve the active metadata driver for the given catalog (READ).

    Legacy single-driver helper. Prefer ``resolve_metadata_drivers`` at new
    call-sites so both READ and WRITE funnel through the same resolver.
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
