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
from typing import Any, Callable, ClassVar, Dict, FrozenSet, List, Optional, Set, cast

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)
from dynastore.tools.ui_hints import ui

logger = logging.getLogger(__name__)


class FailurePolicy(StrEnum):
    """Per-driver failure behaviour within an operation."""

    FATAL = "fatal"    # operation fails if this driver fails
    WARN = "warn"      # log warning, continue with other drivers
    IGNORE = "ignore"  # silently skip on failure


class Operation(StrEnum):
    """Standard operations configured in routing configs.

    Collection routing (``CollectionRoutingConfig.operations``):
    - WRITE  : fan-out to all configured drivers (position 0 = primary)
    - READ   : single-driver for browsing/pagination (streaming)
    - SEARCH : single-driver for filtered queries (bbox, attributes, fulltext)

    Metadata routing (``CollectionRoutingConfig.metadata.operations``):
    - READ      : first-match metadata driver (CollectionMetadataStore)
    - TRANSFORM : ordered transform chain — storage drivers that enrich metadata
                  at READ time (replaces ``MetadataOperationConfig.storage``)
    """

    WRITE = "WRITE"
    READ = "READ"
    SEARCH = "SEARCH"
    TRANSFORM = "TRANSFORM"


class WriteMode(StrEnum):
    """Execution / composition mode for an operation entry.

    Collection write semantics:
    - ``sync``   : await result; participates in coordinated rollback
                   (all sync writes run in parallel via ``asyncio.gather``)
    - ``async``  : fire-and-forget after sync phase succeeds

    Metadata routing composition semantics:
    - ``first``    : return result from the first driver that succeeds
                     (used with ``Operation.READ`` on metadata routing)
    - ``fan_out``  : call all drivers independently; merge results
                     (used with ``Operation.WRITE`` on metadata routing)
    - ``chain``    : pipe output through drivers in declared order;
                     each driver receives the previous output
                     (used with ``Operation.TRANSFORM`` on metadata routing)
    """

    SYNC = "sync"
    ASYNC = "async"
    FIRST = "first"
    FAN_OUT = "fan_out"
    CHAIN = "chain"


# ---------------------------------------------------------------------------
# Capability → Operation mapping
# ---------------------------------------------------------------------------


def derive_supported_operations(capabilities: FrozenSet[str]) -> FrozenSet[str]:
    """Derive which Operations a driver supports from its Capability set.

    Uses :data:`_CAPABILITY_TO_OPERATIONS` to map driver capabilities to the
    operations they can handle.  This is used by apply-handler validation and
    the driver discovery endpoint.
    """
    from dynastore.models.protocols.storage_driver import Capability

    mapping: Dict[str, Set[str]] = {
        Capability.WRITE: {Operation.WRITE},
        Capability.READ: {Operation.READ},
        Capability.FULLTEXT: {Operation.SEARCH},
        Capability.ATTRIBUTE_FILTER: {Operation.SEARCH},
        Capability.SPATIAL_FILTER: {Operation.SEARCH},
    }
    ops: Set[str] = set()
    for cap in capabilities:
        if cap in mapping:
            ops.update(mapping[cap])
    return frozenset(ops)


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
        default=FailurePolicy.FATAL,
        description="What happens if this driver fails: fatal, warn, or ignore.",
    )
    write_mode: WriteMode = Field(
        default=WriteMode.SYNC,
        description=(
            "Execution mode for WRITE operations.  "
            "'sync' = await result (parallel with other sync drivers, participates "
            "in coordinated rollback).  "
            "'async' = fire-and-forget after sync phase succeeds."
        ),
    )


class MetadataRoutingConfig(BaseModel):
    """Metadata routing sub-configuration within ``CollectionRoutingConfig``.

    Uses the same ``operations`` dict shape as collection routing, with two
    standard operations:

    ``READ`` (``write_mode=first``):
        ``CollectionMetadataStore`` backends for metadata persistence
        and search.  First available driver wins.
        Empty → auto-discovery fallback (ES if registered, otherwise PG).
        Replaces the former ``override`` list.

    ``TRANSFORM`` (``write_mode=chain``):
        ``CollectionItemsStore`` backends that enrich collection
        metadata at READ time (e.g. Iceberg table properties, DuckDB sidecar
        JSON).  Drivers are called in declared order; non-fatal failures are
        logged and skipped.
        Replaces the former ``storage`` list.
    """

    operations: Dict[str, List[OperationDriverEntry]] = Field(
        default_factory=dict,
        description=(
            "Operation → ordered driver list for metadata routing.  "
            "READ  = first-match metadata drivers (CollectionMetadataStore).  "
            "TRANSFORM = ordered storage drivers that enrich metadata at READ time."
        ),
    )


class CollectionRoutingConfig(PluginConfig):
    """Operation-based routing for collection storage drivers.

    Each operation maps to an ordered list of :class:`OperationDriverEntry`.
    Position in the list determines priority (first = primary).

    ``metadata`` is a separate sub-object for collection metadata routing —
    see :class:`MetadataRoutingConfig`.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """

    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    enabled: bool = Field(default=True, description="Enable this routing configuration.")

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            Operation.WRITE: [OperationDriverEntry(driver_id="CollectionPostgresqlDriver")],
            Operation.READ: [OperationDriverEntry(driver_id="CollectionPostgresqlDriver")],
        },
        description=(
            "Operation → ordered driver list.  "
            "Immutable: to change driver mapping, create a new config.  "
            "Hints and on_failure within entries are mutable."
        ),
    )

    metadata: MetadataRoutingConfig = Field(
        default_factory=MetadataRoutingConfig,
        description=(
            "Metadata routing sub-configuration. "
            "``operations[READ]`` selects the CollectionMetadataStore backend "
            "(first available wins); "
            "``operations[TRANSFORM]`` selects CollectionItemsStore backends "
            "that contribute metadata at READ time."
        ),
    )


class AssetRoutingConfig(PluginConfig):
    """Operation-based routing for asset storage drivers.

    Same structure as :class:`CollectionRoutingConfig` but scoped to
    asset-domain drivers.

    Identity is the class itself; see ``class_key()`` in ``platform_config_service.py``.
    """

    model_config = ConfigDict(json_schema_extra=ui(category="routing"))

    enabled: bool = Field(default=True, description="Enable this routing configuration.")

    operations: Immutable[Dict[str, List[OperationDriverEntry]]] = Field(
        default_factory=lambda: {
            Operation.WRITE: [OperationDriverEntry(driver_id="AssetPostgresqlDriver")],
            Operation.READ: [OperationDriverEntry(driver_id="AssetPostgresqlDriver")],
        },
        description="Operation → ordered driver list for asset drivers.",
    )


# ---------------------------------------------------------------------------
# on_apply handlers
# ---------------------------------------------------------------------------


def _validate_routing_entries(
    config: "CollectionRoutingConfig | AssetRoutingConfig",
    driver_index: Dict[str, Any],
    label: str,
) -> None:
    """Shared validation for routing config apply handlers.

    Raises ``ValueError`` on:
    1. Unknown ``driver_id``
    2. Hint not in ``driver.supported_hints``
    3. Operation not supported (derived from driver capabilities)
    4. ``write_mode=async`` on a driver without ``DriverCapability.ASYNC``
    """
    from dynastore.modules.storage.driver_config import DriverCapability

    for operation, entries in config.operations.items():
        for entry in entries:
            # 1. Unknown driver
            driver = driver_index.get(entry.driver_id)
            if driver is None:
                raise ValueError(
                    f"{label}: driver '{entry.driver_id}' for operation "
                    f"'{operation}' is not registered. "
                    f"Available: {sorted(driver_index)}"
                )

            # 2. Hint validation
            driver_hints = getattr(driver, "supported_hints", frozenset())
            invalid_hints = entry.hints - driver_hints
            if invalid_hints:
                raise ValueError(
                    f"{label}: hints {sorted(invalid_hints)} are not supported "
                    f"by driver '{entry.driver_id}'. "
                    f"Supported: {sorted(driver_hints)}"
                )

            # 3. Operation supported (derived from capabilities)
            driver_caps = getattr(driver, "capabilities", frozenset())
            supported_ops = derive_supported_operations(driver_caps)
            if operation not in supported_ops:
                raise ValueError(
                    f"{label}: driver '{entry.driver_id}' does not support "
                    f"operation '{operation}'. "
                    f"Supported operations: {sorted(supported_ops)} "
                    f"(derived from capabilities: {sorted(driver_caps)})"
                )

            # 4. write_mode compatibility — check DriverCapability.ASYNC
            if entry.write_mode == WriteMode.ASYNC:
                # Resolve driver config via naming convention: ClassName + "Config"
                try:
                    from dynastore.modules.db_config.platform_config_service import (
                        resolve_config_class,
                    )

                    driver_config_key = type(driver).__name__ + "Config"
                    driver_cls = resolve_config_class(driver_config_key)
                    if driver_cls is not None:
                        driver_config = driver_cls()
                        config_caps = getattr(driver_config, "capabilities", frozenset())
                        if DriverCapability.ASYNC not in config_caps:
                            raise ValueError(
                                f"{label}: write_mode='async' requires "
                                f"DriverCapability.ASYNC on driver '{entry.driver_id}'. "
                                f"Driver capabilities: {sorted(config_caps)}"
                            )
                except ValueError:
                    raise  # re-raise validation errors
                except Exception:
                    pass  # driver config may not exist — skip check

    # 5. Primary driver capability check
    #    Position 0 in WRITE must support WRITE; position 0 in READ/SEARCH
    #    must support READ.  Warn only — don't hard-fail for forward-compat.
    from dynastore.models.protocols.storage_driver import Capability

    _op_required_cap: Dict[str, str] = {
        Operation.WRITE.value: Capability.WRITE,
        Operation.READ.value: Capability.READ,
        Operation.SEARCH.value: Capability.READ,
    }
    for operation, entries in config.operations.items():
        if not entries:
            continue
        primary_id = entries[0].driver_id
        primary_driver = driver_index.get(primary_id)
        if primary_driver is None:
            continue
        required_cap = _op_required_cap.get(operation)
        if required_cap is None:
            continue
        driver_caps = getattr(primary_driver, "capabilities", frozenset())
        if required_cap not in driver_caps:
            logger.warning(
                "%s: primary driver '%s' for operation '%s' lacks capability '%s'. "
                "This may cause runtime errors.",
                label, primary_id, operation, required_cap,
            )


async def _on_apply_routing_config(
    config: CollectionRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after routing config is written.

    Validates driver_id, hints, operations, write_mode, and metadata entries,
    then invalidates the router and metadata-router caches.
    """
    from dynastore.models.protocols.metadata_driver import CollectionMetadataStore
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {type(d).__name__: d for d in get_protocols(CollectionItemsStore)}
    _validate_routing_entries(config, driver_index, "Collection routing config")

    # Validate metadata.operations[READ] entries (CollectionMetadataStore drivers)
    metadata_driver_index = {type(d).__name__: d for d in get_protocols(CollectionMetadataStore)}
    for entry in config.metadata.operations.get(Operation.READ, []):
        if entry.driver_id not in metadata_driver_index:
            raise ValueError(
                f"Collection routing config: metadata.operations[READ] driver "
                f"'{entry.driver_id}' is not registered. "
                f"Available: {sorted(metadata_driver_index)}"
            )

    # Validate metadata.operations[TRANSFORM] entries (CollectionItemsStore drivers)
    for entry in config.metadata.operations.get(Operation.TRANSFORM, []):
        if entry.driver_id not in driver_index:
            raise ValueError(
                f"Collection routing config: metadata.operations[TRANSFORM] driver "
                f"'{entry.driver_id}' is not registered. "
                f"Available: {sorted(driver_index)}"
            )

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_router_cache

        invalidate_router_cache(catalog_id, collection_id)
    except Exception:
        pass

    # Invalidate metadata router cache
    try:
        from dynastore.modules.catalog.metadata_router import invalidate_metadata_router_cache

        invalidate_metadata_router_cache(catalog_id)
    except Exception:
        pass

    # Call ensure_storage() on metadata READ drivers (idempotent, catalog-scoped).
    if catalog_id:
        for entry in config.metadata.operations.get(Operation.READ, []):
            driver = metadata_driver_index.get(entry.driver_id)
            ensure_fn = getattr(driver, "ensure_storage", None) if driver else None
            if ensure_fn is not None:
                try:
                    await ensure_fn(catalog_id)
                except Exception as exc:
                    logger.warning(
                        "ensure_storage failed for metadata driver '%s' on catalog '%s': %s",
                        entry.driver_id, catalog_id, exc,
                    )

    # NOTE: ensure_storage() for collection WRITE/READ drivers is intentionally
    # NOT called here. It is invoked by the collection-creation flow
    # (CollectionService._create_collection_internal step 6) on the write driver,
    # which is the only correct point because the CollectionPostgresqlDriverConfig
    # (physical_table, sidecars) must be fully resolved before storage is
    # provisioned.  Calling ensure_storage() here — potentially before the
    # collection row exists — causes ImmutableConfigError for WriteOnce /
    # Immutable fields when collection creation later tries to write the
    # initial driver config with default (None / empty) values.


async def _on_apply_asset_routing_config(
    config: AssetRoutingConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after asset routing config is written."""
    from dynastore.models.protocols.asset_driver import AssetStore
    from dynastore.tools.discovery import get_protocols

    driver_index = {type(d).__name__: d for d in get_protocols(AssetStore)}
    _validate_routing_entries(config, driver_index, "Asset routing config")

    # Invalidate router cache
    try:
        from dynastore.modules.storage.router import invalidate_asset_router_cache

        invalidate_asset_router_cache(catalog_id, collection_id)
    except Exception:
        pass

    # Call ensure_storage() on all referenced asset drivers (idempotent).
    if catalog_id and collection_id:
        seen_ids: set[str] = set()
        for entries in config.operations.values():
            for entry in entries:
                seen_ids.add(entry.driver_id)
        for did in seen_ids:
            driver = driver_index.get(did)
            ensure_fn = getattr(driver, "ensure_storage", None) if driver else None
            if ensure_fn is not None:
                try:
                    await ensure_fn(catalog_id, collection_id)
                except Exception as exc:
                    logger.warning(
                        "ensure_storage failed for asset driver '%s' on %s/%s: %s",
                        did, catalog_id, collection_id, exc,
                    )


# Register handlers on the config classes themselves.
_HandlerSig = Callable[[PluginConfig, Optional[str], Optional[str], Optional[Any]], Any]
CollectionRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_routing_config))
AssetRoutingConfig.register_apply_handler(cast(_HandlerSig, _on_apply_asset_routing_config))
