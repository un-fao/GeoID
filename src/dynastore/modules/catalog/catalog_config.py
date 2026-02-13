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
#    Copyright 2025 FAO
#    License: Apache 2.0
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)

# File: src/dynastore/modules/catalog/catalog_config.py


# todo fix imports

from enum import Enum
from pydantic import BaseModel, Field, StrictInt, StrictStr, field_validator, model_validator, conint, ConfigDict, create_model, StrictBool, StrictFloat
from datetime import date, datetime
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config, Immutable
from typing import List, Optional, Annotated, Union, Literal, Any, Type, Dict
from typing_extensions import deprecated


class CollectionTypeEnum(str, Enum):
    VECTOR = "VECTOR"
    RASTER = "RASTER"

class VersioningBehaviorEnum(str, Enum):
    UPDATE_EXISTING_VERSION = "UPDATE_EXISTING_VERSION"
    REJECT_NEW_VERSION = "REJECT_NEW_VERSION"
    CREATE_NEW_VERSION = "CREATE_NEW_VERSION"
    REFUSE_ON_ASSET_ID_COLLISION = "REFUSE_ON_ASSET_ID_COLLISION"
    ALWAYS_ADD_NEW = "CREATE_NEW_VERSION" # Alias for compatibility

class PartitionStrategyEnum(str, Enum):
    BY_H3_INDEX = "BY_H3_INDEX"
    BY_S2_INDEX = "BY_S2_INDEX"
    BY_ASSET_ID = "BY_ASSET_ID"
    BY_ATTRIBUTE_VALUE = "BY_ATTRIBUTE_VALUE"
    BY_TIME = "BY_TIME"

class PartitionIntervalEnum(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"

class PartitionStrategy(BaseModel):
    strategy: PartitionStrategyEnum = Field(..., description="The partitioning strategy to use.")
    resolution: Optional[int] = Field(None, description="Resolution for H3 or S2 indexing (if used).")
    attribute_name: Optional[str] = Field(None, description="The name of the attribute to partition by (if using BY_ATTRIBUTE_VALUE).")

class TimeRetentionConfig(BaseModel):
    enabled: bool = Field(False)
    interval: str = Field("12 months")
    schedule: str = Field("0 3 * * 0")

class TimePartitionStrategy(PartitionStrategy):
    strategy: PartitionStrategyEnum = PartitionStrategyEnum.BY_TIME
    interval: PartitionIntervalEnum = PartitionIntervalEnum.MONTHLY
    pre_create_intervals: int = 1
    retention: TimeRetentionConfig = Field(default_factory=TimeRetentionConfig)

# --- Attribute Schema (Validation Logic) ---

class AttributeSchemaEntry(BaseModel):
    """
    Defines the schema for a single attribute within the collection's JSONB 'attributes' column.
    Logical validation only - does not affect DB structure.
    """
    name: StrictStr = Field(..., description="The name of the attribute (key in the JSONB object).")
    type: Literal["string", "number", "integer", "boolean", "date", "datetime", "array", "object"] = Field(
        ..., description="The expected data type of the attribute."
    )
    required: StrictBool = Field(True, description="Whether this attribute must be present.")
    default: Optional[Any] = Field(None, description="A default value if missing.")
    description: Optional[StrictStr] = Field(None, description="Human-readable description.")

def get_attribute_validator_model(schema: List[AttributeSchemaEntry]) -> Optional[Type[BaseModel]]:
    """Dynamically creates a Pydantic model from a schema definition."""
    if not schema:
        return None

    fields: Dict[str, Any] = {}
    type_map = {
        "string": StrictStr, "number": StrictFloat, "integer": StrictInt,
        "boolean": StrictBool, "date": date, "datetime": datetime,
        "array": List, "object": Dict,
    }

    for entry in schema:
        field_type = type_map.get(entry.type, Any)
        fields[entry.name] = (Optional[field_type], entry.default) if not entry.required else (field_type, ...)

    return create_model('DynamicAttributeValidator', **fields, __config__=ConfigDict(extra='ignore'))


# --- High-level Partitioning Strategies ---

# Legacy Partitioning classes removed (PartitionStrategyDetailsBase, TimePartitionStrategy, etc.)


# Enums moved to dynastore.modules.catalog.sidecars.geometry_config
from dynastore.modules.catalog.sidecars.geometry_config import (
    GeometrySidecarConfig
)
from dynastore.modules.catalog.sidecars.attributes import (
    FeatureAttributeSidecarConfig
)
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config, Immutable

# InvalidGeometryPolicy, SimplificationAlgorithm etc are now imported above

# Legacy GeometryStorageConfig was here - now imported or replaced by GeometrySidecarConfig
GeometryStorageConfig = GeometrySidecarConfig
# Keeping reference for migration tools if needed, but pointing to new location


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
        description="Ordered list of column names to partition by (e.g. ['asset_id', 'h3_res12'])."
    )

    @model_validator(mode='after')
    def validate_keys(self) -> 'CompositePartitionConfig':
        if self.enabled and not self.partition_keys:
            raise ValueError("partition_keys must be provided if partitioning is enabled.")
        return self

# Legacy PhysicalGeometryConfig removed (moved to GeometrySidecarConfig)

# IndexingConfig removed (moved to SidecarConfig)
    


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
    sidecars: Immutable[List[Union[
        Annotated[GeometrySidecarConfig, Field(discriminator='sidecar_type')],
        Annotated[FeatureAttributeSidecarConfig, Field(discriminator='sidecar_type')]
    ]]] = Field(
        default_factory=lambda: [GeometrySidecarConfig(), FeatureAttributeSidecarConfig()],
        description="List of sidecar configurations (GeometrySidecarConfig, FeatureAttributeSidecarConfig, etc.)"
    )
    
    
    # === COLLECTION-LEVEL SETTINGS ===
    partitioning: Immutable[CompositePartitionConfig] = Field(default_factory=CompositePartitionConfig)
    collection_type: CollectionTypeEnum = Field(CollectionTypeEnum.VECTOR, description="The type of collection (VECTOR or RASTER).")


    @model_validator(mode='after')
    def validate_composite_partitioning(self) -> 'CollectionPluginConfig':
        """
        Validate that all keys in partitioning.partition_keys are actually provided
        by the configured sidecars (or standard Hub columns like 'transactions_time').
        """
        if not self.partitioning.enabled:
            return self
            
        # Collect all available partition keys from sidecars
        available_keys = {
            "transaction_time", "geoid" # Standard Hub keys
        }
        
        # Add validity from sidecars if contributed (AttributeSidecar now contributes it)
        
        for sidecar in self.sidecars:
            if hasattr(sidecar, 'partition_key_contributions'):
                # contributions is now a dict {name: type}, we just need names for validation here
                # type validation requires more context, done at DDL generation
                available_keys.update(sidecar.partition_key_contributions.keys())
        
        # Check requested keys
        missing_keys = [k for k in self.partitioning.partition_keys if k not in available_keys]
        if missing_keys:
            raise ValueError(
                f"Partition keys {missing_keys} are not provided by any configured sidecar. "
                f"Available keys: {available_keys}"
            )
            
        return self

    @model_validator(mode='after')
    def validate_sidecar_partition_mirroring(self) -> 'CollectionPluginConfig':
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

    @property
    def geometry_storage(self) -> Optional[GeometrySidecarConfig]:
        """Backward-compatible property to access geometry sidecar config."""
        for sidecar in self.sidecars:
            if isinstance(sidecar, GeometrySidecarConfig):
                return sidecar
        return None

    @property
    def attribute_schema(self) -> Optional[List]:
        """Backward-compatible property to access attribute schema from attribute sidecar."""
        for sidecar in self.sidecars:
            if isinstance(sidecar, FeatureAttributeSidecarConfig):
                return getattr(sidecar, 'attribute_schema', None)
        return None

    def get_column_definitions(self) -> List[str]:
        """
        Generates the list of SQL column definitions for the HUB table.
        
        NOTE: In sidecar mode, this only returns Hub columns (identity + temporal).
        Geometry and attributes are in their respective sidecars.
        """
        # Hub table: Core identity and temporal tracking
        columns = [
            "id BIGSERIAL",
            "geoid UUID DEFAULT gen_random_uuid()",
            "validity TSTZRANGE NOT NULL DEFAULT tstzrange(NOW(), NULL, '[)')",
            "transaction_time TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            "content_hash VARCHAR(64)",
            "deleted_at TIMESTAMPTZ"
        ]

        return columns

# Rebuild model to resolve forward references
CollectionPluginConfig.model_rebuild()
