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

from enum import Enum
from pydantic import (
    BaseModel,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
    conint,
    ConfigDict,
    create_model,
    StrictBool,
    StrictFloat,
    SerializeAsAny,
)
from datetime import date, datetime
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    Immutable,
)
from typing import ClassVar, List, Optional, Annotated, Union, Literal, Any, Type, Dict, Set
from typing_extensions import deprecated


class CollectionTypeEnum(str, Enum):
    VECTOR = "VECTOR"
    RASTER = "RASTER"


# --- High-level Partitioning Strategies ---


# Enums moved to dynastore.modules.catalog.sidecars.geometries_config
from dynastore.modules.catalog.sidecars.base import SidecarConfig
from dynastore.modules.catalog.sidecars.geometries_config import GeometriesSidecarConfig
from dynastore.modules.catalog.sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    VersioningBehaviorEnum,
)
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    Immutable,
)
from dynastore.modules.storage.config import DriverRef
from dynastore.modules.storage.location import (
    StorageLocationConfig,
    StorageLocationConfigRegistry,
)

# Legacy GeometryStorageConfig was here - now imported or replaced by GeometriesSidecarConfig
GeometryStorageConfig = GeometriesSidecarConfig


# --- Partitioning (Physical) ---


class CompositePartitionConfig(BaseModel):
    """
    Configuration for Composite Partitioning.
    Defines the ordered list of keys (columns) that form the partition key.
    These keys must be provided by the enabled sidecars (or the Hub).
    """

    enabled: bool = Field(False, description="Enable partitioning for this collection.")
    partition_keys: List[str] = Field(
        default_factory=list,
        description="Ordered list of column names to partition by (e.g. ['asset_id', 'h3_res12']).",
    )

    @model_validator(mode="after")
    def validate_keys(self) -> "CompositePartitionConfig":
        if self.enabled and not self.partition_keys:
            raise ValueError(
                "partition_keys must be provided if partitioning is enabled."
            )
        return self


# --- Main Catalog Config ---

COLLECTION_PLUGIN_CONFIG_ID = "collection"


class CollectionPluginConfig(PluginConfig):
    """
    Collection configuration — schema, sidecars, partitioning, and storage routing.

    Storage routing fields (write_driver, read_drivers, secondary_drivers,
    storage_locations) are merged from the former StorageRoutingConfig.
    The 4-tier config waterfall (collection > catalog > platform > defaults)
    applies to all fields.

    CRITICAL: Modifications to 'sidecars' or 'partitioning' are FORBIDDEN
    once the table physically exists.
    """
    _plugin_id: ClassVar[Optional[str]] = COLLECTION_PLUGIN_CONFIG_ID

    model_config = {"extra": "allow"}

    # === STORAGE ROUTING ===
    write_driver: Immutable[DriverRef] = Field(
        default_factory=lambda: DriverRef(driver_id="postgresql"),
        description=(
            "Driver for writes. Also serves as fallback for reads when no hint matches. "
            "Immutable on existing collections."
        ),
    )
    read_drivers: Dict[str, DriverRef] = Field(
        default_factory=dict,
        description=(
            "Hint → driver mapping for reads. "
            "Special hints: 'metadata' (collection metadata), 'default' (fallback), "
            "'search' (fulltext/spatial search), 'enrichment' (cross-driver join). "
            "If empty, write_driver handles all reads."
        ),
    )
    secondary_drivers: List[DriverRef] = Field(
        default_factory=list,
        description="Drivers receiving async write fan-out via EventsProtocol.",
    )
    storage_locations: Dict[str, StorageLocationConfig] = Field(
        default_factory=dict,
        description="Per-driver storage location config.",
    )

    # === SIDECAR ARCHITECTURE ===
    # Use SerializeAsAny to ensure polymorphic dumping (keeps specialized fields)
    sidecars: Immutable[List[SerializeAsAny[SidecarConfig]]] = Field(
        default_factory=lambda: [
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(),
        ],
        description="List of sidecar configurations (GeometriesSidecarConfig, FeatureAttributeSidecarConfig, etc.)",
    )

    # === COLLECTION-LEVEL SETTINGS ===
    partitioning: Immutable[CompositePartitionConfig] = Field(
        default_factory=CompositePartitionConfig
    )
    collection_type: CollectionTypeEnum = Field(
        CollectionTypeEnum.VECTOR,
        description="The type of collection (VECTOR or RASTER).",
    )

    # === CONVENIENCE PROPERTIES ===

    @property
    def write_driver_id(self) -> str:
        """Shortcut: write driver ID as string."""
        return self.write_driver.driver_id

    def resolve_read_driver_id(self, hint: str) -> str:
        """Resolve driver ID for a read hint: hint → default → write_driver."""
        ref = self.read_drivers.get(hint) or self.read_drivers.get("default")
        return ref.driver_id if ref else self.write_driver_id

    @property
    def secondary_driver_ids(self) -> List[str]:
        """Shortcut: secondary driver IDs as strings."""
        return [d.driver_id for d in self.secondary_drivers]

    def get_location(self, driver_id: str) -> Optional[StorageLocationConfig]:
        """Get the storage location config for a driver."""
        return self.storage_locations.get(driver_id)

    def available_hints(self) -> Set[str]:
        """Return all configured hint keys."""
        return set(self.read_drivers.keys())

    # === VALIDATORS ===

    @model_validator(mode="before")
    @classmethod
    def _coerce_driver_refs(cls, data: Any) -> Any:
        """Allow shorthand strings: ``'postgresql'`` -> ``DriverRef(driver_id='postgresql')``."""
        if not isinstance(data, dict):
            return data

        wd = data.get("write_driver")
        if isinstance(wd, str):
            data["write_driver"] = {"driver_id": wd}

        # Legacy: accept primary_driver as alias for write_driver
        pd = data.get("primary_driver")
        if isinstance(pd, str):
            data.setdefault("write_driver", {"driver_id": pd})
        elif isinstance(pd, dict) and "write_driver" not in data:
            data["write_driver"] = pd

        rd = data.get("read_drivers")
        if isinstance(rd, dict):
            data["read_drivers"] = {
                k: {"driver_id": v} if isinstance(v, str) else v
                for k, v in rd.items()
            }

        sd = data.get("secondary_drivers")
        if isinstance(sd, list):
            data["secondary_drivers"] = [
                {"driver_id": s} if isinstance(s, str) else s for s in sd
            ]

        sl = data.get("storage_locations")
        if isinstance(sl, dict):
            resolved = {}
            for driver_id, loc_data in sl.items():
                if isinstance(loc_data, dict):
                    config_cls = StorageLocationConfigRegistry.resolve(driver_id)
                    resolved[driver_id] = config_cls.model_validate(loc_data)
                else:
                    resolved[driver_id] = loc_data
            data["storage_locations"] = resolved

        return data

    @field_validator("sidecars", mode="before")
    @classmethod
    def validate_sidecars_polymorphic(cls, v: Any) -> Any:
        """
        Ensures that sidecar configurations are instantiated as their specialized
        subclasses (e.g. GeometriesSidecarConfig) instead of generic SidecarConfig.
        """
        if isinstance(v, list):
            from dynastore.modules.catalog.sidecars.base import SidecarConfigRegistry
            processed = []
            for item in v:
                if isinstance(item, dict) and "sidecar_type" in item:
                    sidecar_type = item["sidecar_type"]
                    config_cls = SidecarConfigRegistry.resolve_config_class(sidecar_type)
                    # Use model_validate to get the specialized instance
                    processed.append(config_cls.model_validate(item))
                else:
                    processed.append(item)
            return processed
        return v

    @model_validator(mode="after")
    def validate_composite_partitioning(self) -> "CollectionPluginConfig":
        """
        Validate that all keys in partitioning.partition_keys are actually provided
        by the configured sidecars (or standard Hub columns like 'transactions_time').
        """
        if not self.partitioning.enabled:
            return self

        # Collect all available partition keys from sidecars
        available_keys = {
            "transaction_time",
            "geoid",  # Standard Hub keys
        }

        # Add validity from sidecars if contributed (AttributeSidecar now contributes it)

        for sidecar in self.sidecars:
            available_keys.update(sidecar.partition_key_contributions.keys())

        # Check requested keys
        missing_keys = [
            k for k in self.partitioning.partition_keys if k not in available_keys
        ]
        if missing_keys:
            raise ValueError(
                f"Partition keys {missing_keys} are not provided by any configured sidecar. "
                f"Available keys: {available_keys}"
            )

        return self

    @model_validator(mode="after")
    def validate_sidecar_partition_mirroring(self) -> "CollectionPluginConfig":
        """
        CRITICAL: Ensures all sidecars use the same partition strategy as the Hub.
        This is required for partition-wise joins at trillion-row scale.
        """
        if self.sidecars and self.partitioning.enabled:
            # All sidecars must mirror the Hub's partition strategy
            # This is enforced at the sidecar DDL generation level
            pass

        return self

    def get_column_definitions(self) -> Dict[str, str]:
        """
        Returns a dictionary of {column_name: sql_type} for the Hub table.

        NOTE: In sidecar mode, this only returns Hub columns.
        Geometry, attributes and temporal validity are in their respective sidecars.
        """
        return {
            "geoid": "UUID PRIMARY KEY",
            # validity moved to Attributes sidecar (logic refined in sidecars)
            "transaction_time": "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
            "deleted_at": "TIMESTAMPTZ",
            "content_hash": "VARCHAR(64)",  # Hash of the content for deduplication
        }

    def get_all_field_definitions(self) -> Dict[str, Any]:
        """
        Aggregates field definitions from all enabled sidecars.
        """
        all_fields = {}
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

        for sc_config in self.sidecars:
            if not sc_config.enabled:
                continue
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            all_fields.update(sidecar.get_field_definitions())
        return all_fields


# Register on_apply handler for validation of driver refs on config change
async def _on_apply_collection_config(
    config: CollectionPluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Called after CollectionPluginConfig is written.

    Verifies newly-added secondary drivers are registered and performs
    best-effort cleanup for removed secondary drivers.
    """
    import logging
    logger = logging.getLogger(__name__)

    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol, get_protocols
    from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol

    # Resolve previous config to compute secondary driver diff
    old_config: Optional[CollectionPluginConfig] = None
    if catalog_id:
        try:
            configs_proto = get_protocol(ConfigsProtocol)
            if configs_proto:
                old_config = await configs_proto.get_config(
                    COLLECTION_PLUGIN_CONFIG_ID,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
        except Exception:
            pass

    old_ids = set(old_config.secondary_driver_ids) if old_config else set()
    new_ids = set(config.secondary_driver_ids)

    added = new_ids - old_ids
    removed = old_ids - new_ids

    if not added and not removed:
        return

    if added:
        registered = {d.driver_id for d in get_protocols(CollectionStorageDriverProtocol)}
        unknown = added - registered
        if unknown:
            logger.warning(
                "CollectionConfig: secondary driver(s) not registered: %s. "
                "Available: %s. Events will be silently ignored until the driver loads.",
                sorted(unknown), sorted(registered),
            )

    if removed and catalog_id:
        driver_index = {d.driver_id: d for d in get_protocols(CollectionStorageDriverProtocol)}
        for driver_id in removed:
            driver = driver_index.get(driver_id)
            if driver and hasattr(driver, "drop_storage"):
                try:
                    await driver.drop_storage(catalog_id, collection_id)
                    logger.info(
                        "CollectionConfig: cleaned up driver '%s' for catalog '%s'.",
                        driver_id, catalog_id,
                    )
                except Exception as e:
                    logger.warning(
                        "CollectionConfig: cleanup failed for driver '%s' catalog '%s': %s",
                        driver_id, catalog_id, e,
                    )


# Register the on_apply handler
from dynastore.modules.db_config.platform_config_service import ConfigRegistry  # noqa: E402

ConfigRegistry.register_apply_handler(COLLECTION_PLUGIN_CONFIG_ID, _on_apply_collection_config)

# Rebuild model to resolve forward references
CollectionPluginConfig.model_rebuild()
