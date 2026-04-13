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
Storage Router — resolves drivers for a given operation + catalog/collection.

Resolution is based on ``RoutingPluginConfig`` (operation → ordered driver
list) with optional hint-based filtering.

For **WRITE**: all matching drivers execute (fan-out), each with its own
``FailurePolicy``.

For **READ/SEARCH**: the first matching driver is returned.

Performance: resolution is cached (300 s TTL) keyed on
``(routing_plugin_id, catalog_id, collection_id, operation, hint)``.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Generic, List, Optional, Protocol, TypeVar, Union, cast, runtime_checkable

if TYPE_CHECKING:
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol
    from dynastore.models.protocols.asset_driver import AssetDriverProtocol
    AnyDriver = Union["CollectionStorageDriverProtocol", "AssetDriverProtocol"]

_D = TypeVar("_D")

from dynastore.modules.storage.routing_config import (
    ROUTING_ASSETS_PLUGIN_CONFIG_ID,
    ROUTING_PLUGIN_CONFIG_ID,
    FailurePolicy,
    Operation,
    WriteMode,
)
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Resolved driver container
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedDriver(Generic[_D]):
    """A driver resolved for a specific operation, with its failure policy and write mode."""

    driver: _D
    on_failure: FailurePolicy = FailurePolicy.FATAL
    write_mode: WriteMode = WriteMode.SYNC

    @property
    def driver_id(self) -> str:
        return getattr(self.driver, "driver_id", "unknown")


# ---------------------------------------------------------------------------
# Driver index builders
# ---------------------------------------------------------------------------


def _build_collection_driver_index() -> "Dict[str, CollectionStorageDriverProtocol]":
    """Build driver_id → driver instance lookup for collection drivers."""
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(CollectionStorageDriverProtocol)}


def _build_asset_driver_index() -> "Dict[str, AssetDriverProtocol]":
    """Build driver_id → driver instance lookup for asset drivers."""
    from dynastore.models.protocols.asset_driver import AssetDriverProtocol
    from dynastore.tools.discovery import get_protocols

    return {d.driver_id: d for d in get_protocols(AssetDriverProtocol)}


# ---------------------------------------------------------------------------
# Core resolution
# ---------------------------------------------------------------------------


@cached(maxsize=4096, ttl=300, namespace="storage_router", distributed=False)
async def _resolve_driver_ids_cached(
    routing_plugin_id: str,
    catalog_id: str,
    collection_id: Optional[str],
    operation: str,
    hint: Optional[str],
) -> List[tuple]:
    """Cached resolution: returns list of (driver_id, on_failure, write_mode) tuples."""
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise RuntimeError("ConfigsProtocol not available — cannot resolve storage routing")

    from dynastore.modules.storage.routing_config import RoutingPluginConfig as _RPC
    _raw_config = await configs.get_config(
        routing_plugin_id,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )
    routing_config = cast(_RPC, _raw_config)

    from dynastore.modules.storage.routing_config import OperationDriverEntry as _ODE
    _ops = cast(Dict[str, List[_ODE]], routing_config.operations)
    entries = _ops.get(operation, [])

    if hint:
        entries = [e for e in entries if hint in e.hints]

    return [(e.driver_id, e.on_failure, e.write_mode) for e in entries]


async def resolve_drivers(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
    routing_plugin_id: str = ROUTING_PLUGIN_CONFIG_ID,
) -> List[ResolvedDriver]:
    """Resolve an ordered list of drivers for the requested operation.

    For **READ/SEARCH**: caller uses the first result.
    For **WRITE**: caller executes all (fan-out), respecting ``on_failure``.

    Args:
        operation: Required. ``WRITE``, ``READ``, ``SEARCH``, etc.
        catalog_id: Catalog context.
        collection_id: Optional collection context.
        hint: Optional preference to select specific driver(s).
        routing_plugin_id: Config key — ``"collection:drivers"`` for collections,
            ``"assets:drivers"`` for assets.

    Returns:
        Ordered list of :class:`ResolvedDriver`. Empty if hint is not
        satisfiable by any configured driver.
    """
    resolved_ids = await _resolve_driver_ids_cached(
        routing_plugin_id, catalog_id, collection_id, operation, hint,
    )

    if routing_plugin_id == ROUTING_ASSETS_PLUGIN_CONFIG_ID:
        driver_index = _build_asset_driver_index()
    else:
        driver_index = _build_collection_driver_index()

    result = []
    for driver_id, on_failure, write_mode in resolved_ids:
        driver = driver_index.get(driver_id)
        if driver:
            result.append(ResolvedDriver(driver=driver, on_failure=on_failure, write_mode=write_mode))
        else:
            logger.warning(
                "Driver '%s' for operation '%s' is not registered. Skipping.",
                driver_id,
                operation,
            )

    return result


# ---------------------------------------------------------------------------
# Convenience wrappers — collection drivers
# ---------------------------------------------------------------------------


async def get_driver(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
) -> "CollectionStorageDriverProtocol":
    """Single-driver resolution for collection READ/SEARCH.

    Returns the first matching ``CollectionStorageDriverProtocol`` or raises.
    """
    resolved = await resolve_drivers(
        operation, catalog_id, collection_id, hint=hint,
    )
    if not resolved:
        raise ValueError(
            f"No collection driver found for operation='{operation}', "
            f"hint='{hint}', catalog='{catalog_id}', collection='{collection_id}'"
        )
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol as _CSDP
    return cast(_CSDP, resolved[0].driver)


async def get_write_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
) -> "List[ResolvedDriver[CollectionStorageDriverProtocol]]":
    """Multi-driver resolution for collection WRITE fan-out."""
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol as _CSDP
    result = await resolve_drivers(
        Operation.WRITE, catalog_id, collection_id, hint=hint,
    )
    return cast(List["ResolvedDriver[_CSDP]"], result)


# ---------------------------------------------------------------------------
# Convenience wrappers — asset drivers
# ---------------------------------------------------------------------------


async def get_asset_driver(
    operation: str,
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
):
    """Single-driver resolution for asset READ/SEARCH.

    Returns the first matching ``AssetDriverProtocol`` or raises.
    """
    resolved = await resolve_drivers(
        operation,
        catalog_id,
        collection_id,
        hint=hint,
        routing_plugin_id=ROUTING_ASSETS_PLUGIN_CONFIG_ID,
    )
    if not resolved:
        raise ValueError(
            f"No asset driver found for operation='{operation}', "
            f"hint='{hint}', catalog='{catalog_id}', collection='{collection_id}'"
        )
    return resolved[0].driver


async def get_asset_write_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
) -> "List[ResolvedDriver[AssetDriverProtocol]]":
    """Multi-driver resolution for asset WRITE fan-out."""
    from dynastore.models.protocols.asset_driver import AssetDriverProtocol as _ADP
    result = await resolve_drivers(
        Operation.WRITE,
        catalog_id,
        collection_id,
        hint=hint,
        routing_plugin_id=ROUTING_ASSETS_PLUGIN_CONFIG_ID,
    )
    return cast(List["ResolvedDriver[_ADP]"], result)


# ---------------------------------------------------------------------------
# Cache invalidation
# ---------------------------------------------------------------------------


def invalidate_router_cache(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> None:
    """Invalidate cached resolution for collection routing."""
    try:
        getattr(_resolve_driver_ids_cached, "cache_clear")()
    except Exception:
        pass


def invalidate_asset_router_cache(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> None:
    """Invalidate cached resolution for asset routing.

    Note: shares the same underlying cache as collection routing
    (differentiated by ``routing_plugin_id`` in the cache key).
    Full cache clear is the safest approach.
    """
    invalidate_router_cache(catalog_id, collection_id)
