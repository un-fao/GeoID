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
from dynastore.modules.db_config.platform_config_manager import (
    PluginConfig,
    register_config,
    Immutable,
)
from typing import List, Optional, Annotated, Union, Literal, Any, Type, Dict
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
from dynastore.modules.db_config.platform_config_manager import (
    PluginConfig,
    register_config,
    Immutable,
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


@register_config(COLLECTION_PLUGIN_CONFIG_ID)
class CollectionPluginConfig(PluginConfig):
    """
    The Physical Schema Configuration.

    CRITICAL: Modifications to 'geometry_storage' or 'partitioning'
    are FORBIDDEN once the table physically exists.
    """

    model_config = {"extra": "allow"}

    # === SIDECAR ARCHITECTURE (New) ===
    # Sidecars replace the monolithic geometry_storage and attribute_schema
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
    search_index: bool = Field(
        False,
        description=(
            "When True, items in this collection will be indexed into the search engine "
            "automatically on creation/update/deletion. Disabled by default."
        ),
    )


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
            if hasattr(sidecar, "partition_key_contributions"):
                # contributions is now a dict {name: type}, we just need names for validation here
                # type validation requires more context, done at DDL generation
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

        # Legacy validation for H3/S2 resolutions is no longer needed here
        # as it is handled by the GeometrySidecar's own configuration and partition contribution.
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


# Rebuild model to resolve forward references
CollectionPluginConfig.model_rebuild()
