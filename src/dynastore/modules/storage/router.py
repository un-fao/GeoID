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

Resolution is based on ``CollectionRoutingConfig`` (operation → ordered driver
list) with optional hint-based filtering.

For **WRITE**: all matching drivers execute (fan-out), each with its own
``FailurePolicy``.

For **READ/SEARCH**: the first matching driver is returned.

Performance: driver index lookup uses the process-wide ``DriverRegistry``
singleton (L0 cache, built once at startup) so there is no per-request dict
allocation.  Routing resolution is cached (300 s TTL) keyed on
``(routing_config_class_key, catalog_id, collection_id, operation, hint)``.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Generic, List, Optional, Protocol, Type, TypeVar, Union, cast, runtime_checkable

if TYPE_CHECKING:
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.modules.db_config.platform_config_service import PluginConfig
    AnyDriver = Union["CollectionItemsStore", "AssetStore"]

_D = TypeVar("_D")

from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    FailurePolicy,
    Operation,
    CollectionRoutingConfig,
    WriteMode,
)
from dynastore.modules.storage.driver_registry import DriverRegistry
from dynastore.modules.storage.config_cache import get_request_driver_cache
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
        return type(self.driver).__name__


# ---------------------------------------------------------------------------
# Core resolution
# ---------------------------------------------------------------------------


@cached(maxsize=4096, ttl=300, namespace="storage_router", distributed=True)
async def _resolve_driver_ids_cached(
    routing_plugin_cls: "Type[PluginConfig]",
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

    from dynastore.modules.storage.routing_config import CollectionRoutingConfig as _RPC
    _raw_config = await configs.get_config(
        routing_plugin_cls,
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
    routing_plugin_cls: "Type[PluginConfig]" = CollectionRoutingConfig,
) -> List[ResolvedDriver]:
    """Resolve an ordered list of drivers for the requested operation.

    For **READ/SEARCH**: caller uses the first result.
    For **WRITE**: caller executes all (fan-out), respecting ``on_failure``.

    Resolution layers (fast → slow):
    - **L4** per-request context var — zero-cost within a single request
    - **L1** in-process ``@cached`` LRU — sub-microsecond after first resolution
    - **L2** Valkey-backed shared cache — shared across workers (TTL 300 s)
    - **L3** DB waterfall query — cold path, triggered on cache miss

    Args:
        operation: Required. ``WRITE``, ``READ``, ``SEARCH``, etc.
        catalog_id: Catalog context.
        collection_id: Optional collection context.
        hint: Optional preference to select specific driver(s).
        routing_plugin_cls: PluginConfig class — ``CollectionRoutingConfig`` for
            collections, ``AssetRoutingConfig`` for assets.

    Returns:
        Ordered list of :class:`ResolvedDriver`. Empty if hint is not
        satisfiable by any configured driver.
    """
    # L4 — per-request memoisation: if the same resolution was already performed
    # earlier in this request, return the cached result without touching L1/L2/L3.
    l4_key = (routing_plugin_cls, catalog_id, collection_id, operation, hint)
    l4 = get_request_driver_cache()
    if l4_key in l4:
        return l4[l4_key]  # type: ignore[return-value]

    resolved_ids = await _resolve_driver_ids_cached(
        routing_plugin_cls, catalog_id, collection_id, operation, hint,
    )

    if routing_plugin_cls is AssetRoutingConfig:
        driver_index = DriverRegistry.asset_index()
    else:
        driver_index = DriverRegistry.collection_index()

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

    # Store in L4 for reuse later in the same request
    l4[l4_key] = result
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
) -> "CollectionItemsStore":
    """Single-driver resolution for collection READ/SEARCH.

    Returns the first matching ``CollectionItemsStore`` or raises.
    """
    resolved = await resolve_drivers(
        operation, catalog_id, collection_id, hint=hint,
    )
    if not resolved:
        raise ValueError(
            f"No collection driver found for operation='{operation}', "
            f"hint='{hint}', catalog='{catalog_id}', collection='{collection_id}'"
        )
    from dynastore.models.protocols.storage_driver import CollectionItemsStore as _CSDP
    return cast(_CSDP, resolved[0].driver)


async def get_write_drivers(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    hint: Optional[str] = None,
) -> "List[ResolvedDriver[CollectionItemsStore]]":
    """Multi-driver resolution for collection WRITE fan-out.

    Always returns ≥1 entry in a correctly bootstrapped deploy. The waterfall
    has a code-level default (``CollectionRoutingConfig.operations[WRITE] =
    [ItemsPostgresqlDriver]``), so an empty result indicates a deploy/ops
    misconfiguration and is raised as :class:`ConfigResolutionError`.
    """
    from dynastore.models.protocols.storage_driver import CollectionItemsStore as _CSDP
    result = await resolve_drivers(
        Operation.WRITE, catalog_id, collection_id, hint=hint,
    )
    if not result:
        from dynastore.modules.db_config.exceptions import ConfigResolutionError

        raise ConfigResolutionError(
            (
                f"No CollectionItemsStore resolved for WRITE on "
                f"'{catalog_id}/{collection_id}'. Routing waterfall produced "
                f"an empty list — neither CollectionRoutingConfig.operations[WRITE] "
                f"nor its code default is supplying a registered, available driver."
            ),
            missing_key="CollectionRoutingConfig.operations[WRITE]",
            required_fields=[],
            scope_tried=["collection", "catalog", "platform", "code_default"],
            hint=(
                "Register a CollectionItemsStore driver (e.g. "
                "ItemsPostgresqlDriver) or set "
                "CollectionRoutingConfig.operations[WRITE] at platform scope."
            ),
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

    Returns the first matching ``AssetStore`` or raises.
    """
    resolved = await resolve_drivers(
        operation,
        catalog_id,
        collection_id,
        hint=hint,
        routing_plugin_cls=AssetRoutingConfig,
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
) -> "List[ResolvedDriver[AssetStore]]":
    """Multi-driver resolution for asset WRITE fan-out."""
    from dynastore.models.protocols.asset_driver import AssetStore as _ADP
    result = await resolve_drivers(
        Operation.WRITE,
        catalog_id,
        collection_id,
        hint=hint,
        routing_plugin_cls=AssetRoutingConfig,
    )
    return cast(List["ResolvedDriver[_ADP]"], result)


# ---------------------------------------------------------------------------
# Cache invalidation
# ---------------------------------------------------------------------------


def invalidate_router_cache(
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> None:
    """Invalidate cached resolution for collection routing.

    Also clears the ``DriverRegistry`` L0 cache so that any driver
    (un)registration events are reflected on the next request.
    """
    try:
        getattr(_resolve_driver_ids_cached, "cache_clear")()
    except Exception:
        pass
    DriverRegistry.clear()


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
