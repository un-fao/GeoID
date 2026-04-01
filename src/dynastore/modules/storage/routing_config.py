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
Routing plugin configuration — operation-based driver composition.

Maps **operations** (WRITE, READ, SEARCH) to an ordered list of drivers,
each with optional hints and a failure policy.

Key concepts:

- **Operations** = what the caller wants (WRITE, READ, SEARCH) — defined here
- **Capabilities** = how the driver performs it (SYNC, ASYNC, etc.) — in driver_config.py
- **Hints** = caller-provided preferences to select a specific driver within an operation
- **Failure policy** = per-driver behaviour on error: fatal, warn, or ignore

Resolution semantics:

- **WRITE** (no hint): execute ALL drivers in list (fan-out), respecting ``on_failure``
- **WRITE** (with hint): filter to matching drivers, execute those
- **READ/SEARCH** (no hint): return first driver in list (primary by position)
- **READ/SEARCH** (with hint): filter to matching, return first match
"""

import logging
from enum import StrEnum
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROUTING_PLUGIN_CONFIG_ID = "routing"
ROUTING_ASSETS_PLUGIN_CONFIG_ID = "routing_assets"


class FailurePolicy(StrEnum):
    """Per-driver failure behaviour within an operation."""

    FATAL = "fatal"    # operation fails if this driver fails
    WARN = "warn"      # log warning, continue with other drivers
    IGNORE = "ignore"  # silently skip on failure


class Operation(StrEnum):
    """Standard operations."""

    WRITE = "WRITE"
    READ = "READ"
    SEARCH = "SEARCH"


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class OperationDriverEntry(BaseModel):
    """A driver configured for a specific operation.

    ``driver_id`` is immutable — changing which drivers participate in an
    operation is a structural decision.  ``hints`` and ``on_failure`` are
    mutable preferences that can evolve without structural impact.
    """

    driver_id: Immutable[str] = Field(
        ..., min_length=1, description="Driver identifier (e.g. 'postgresql')."
    )
    hints: Set[str] = Field(
        default_factory=set,
        description="Hints this driver responds to for this operation.",
    )
    on_failure: FailurePolicy = Field(
        FailurePolicy.FATAL,
        description="What happens if this driver fails: fatal, warn, or ignore.",
    )


class RoutingPluginConfig(PluginConfig):
    """Operation-based routing for collection storage drivers.

    Each operation maps to an ordered list of :class:`OperationDriverEntry`.
    Position in the list determines priority (first = primary).

    Registered as ``plugin_id = "routing"`` in the config waterfall.
    """

    _plugin_id: ClassVar[Optional[str]] = ROUTING_PLUGIN_CONFIG_ID

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            Operation.WRITE: [OperationDriverEntry(driver_id="postgresql")],
            Operation.READ: [OperationDriverEntry(driver_id="postgresql")],
        },
        description=(
            "Operation → ordered driver list.  "
            "Immutable: to change driver mapping, create a new config.  "
            "Hints and on_failure within entries are mutable."
        ),
    )


class AssetRoutingPluginConfig(PluginConfig):
    """Operation-based routing for asset storage drivers.

    Same structure as :class:`RoutingPluginConfig` but scoped to
    asset-domain drivers.

    Registered as ``plugin_id = "routing_assets"`` in the config waterfall.
    """

    _plugin_id: ClassVar[Optional[str]] = ROUTING_ASSETS_PLUGIN_CONFIG_ID

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            Operation.WRITE: [OperationDriverEntry(driver_id="postgresql")],
            Operation.READ: [OperationDriverEntry(driver_id="postgresql")],
        },
        description="Operation → ordered driver list for asset drivers.",
    )


# ---------------------------------------------------------------------------
# on_apply handlers
# ---------------------------------------------------------------------------


async def _on_apply_routing_config(
    config: RoutingPluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after routing config is written.

    Validates that referenced drivers are registered and invalidates
    the router cache.
    """
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol
    from dynastore.tools.discovery import get_protocols

    registered = {d.driver_id for d in get_protocols(CollectionStorageDriverProtocol)}

    for operation, entries in config.operations.items():
        for entry in entries:
            if entry.driver_id not in registered:
                logger.warning(
                    "Routing config: driver '%s' for operation '%s' is not registered. "
                    "Available: %s",
                    entry.driver_id,
                    operation,
                    sorted(registered),
                )

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_router_cache

        invalidate_router_cache(catalog_id, collection_id)
    except Exception:
        pass


async def _on_apply_asset_routing_config(
    config: AssetRoutingPluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after asset routing config is written."""
    from dynastore.models.protocols.asset_driver import AssetDriverProtocol
    from dynastore.tools.discovery import get_protocols

    registered = {d.driver_id for d in get_protocols(AssetDriverProtocol)}

    for operation, entries in config.operations.items():
        for entry in entries:
            if entry.driver_id not in registered:
                logger.warning(
                    "Asset routing config: driver '%s' for operation '%s' is not registered. "
                    "Available: %s",
                    entry.driver_id,
                    operation,
                    sorted(registered),
                )

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_asset_router_cache

        invalidate_asset_router_cache(catalog_id, collection_id)
    except Exception:
        pass


# Register handlers
from dynastore.modules.db_config.platform_config_service import ConfigRegistry  # noqa: E402

ConfigRegistry.register_apply_handler(ROUTING_PLUGIN_CONFIG_ID, _on_apply_routing_config)
ConfigRegistry.register_apply_handler(ROUTING_ASSETS_PLUGIN_CONFIG_ID, _on_apply_asset_routing_config)
