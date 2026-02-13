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
Feature Attribute Sidecar Configuration Models.

Provides enhanced configuration for attribute storage, including storage modes
(Relational vs JSONB), per-attribute indexing, and partitioning control.
"""

from enum import Enum
from typing import Optional, Dict, List, Literal
from pydantic import BaseModel, Field
from dynastore.modules.catalog.sidecars.base import SidecarConfig


class AttributeStorageMode(str, Enum):
    """Storage mode for feature attributes."""
    COLUMNAR = "columnar"      # Mode A: Physical columns
    JSONB = "jsonb"            # Mode B: JSONB column
    AUTOMATIC = "automatic"    # Automatic selection based on attribute schema (fallback to JSONB if schema is not specified)


class PostgresType(str, Enum):
    """Common PostgreSQL types for attributes."""
    TEXT = "TEXT"
    VARCHAR_255 = "VARCHAR(255)"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    JSONB = "JSONB"
    UUID = "UUID"


class AttributeIndexType(str, Enum):
    """Index types for attributes."""
    NONE = "none"
    BTREE = "btree"
    GIN = "gin"      # For JSONB/array columns
    GIST = "gist"    # For range types
    HASH = "hash"    # For equality only


class AttributeSchemaEntry(BaseModel):
    """Enhanced schema definition for a single attribute."""
    name: str = Field(..., description="Attribute name (column name or JSON key)")
    type: PostgresType = Field(PostgresType.TEXT, description="PostgreSQL data type")
    
    # Indexing control (B-Tree recommended for numeric/text at scale)
    index: AttributeIndexType = Field(
        AttributeIndexType.NONE,
        description="Index strategy for this attribute"
    )
    
    # Partitioning control
    can_partition: bool = Field(
        False,
        description="Whether this attribute can be used in a partition key"
    )
    
    # Constraints
    nullable: bool = Field(True, description="Allow NULL values")
    unique: bool = Field(False, description="Enforce uniqueness (at table/partition level)")
    
    # Metadata
    description: Optional[str] = Field(None, description="Human-readable description")


class FeatureAttributeSidecarConfig(SidecarConfig):
    """
    Enhanced configuration for FeatureAttributeSidecar.
    
    Provides granular control over how attributes are stored, indexed, and
    used for partitioning.
    """
    sidecar_type: Literal["attributes"] = "attributes"
    
    # Storage Configuration
    storage_mode: AttributeStorageMode = Field(
        AttributeStorageMode.AUTOMATIC, 
        description="Storage mode: 'automatic', 'columnar' physical columns or 'jsonb' document"
    )
    
    # Identity Columns Configuration
    enable_external_id: bool = Field(True, description="Store external_id column")
    external_id_field: str = Field("id", description="Input field to map to external_id (e.g. 'id', 'properties.code')")
    index_external_id: bool = Field(True, description="Create unique index on external_id")
    
    enable_asset_id: bool = Field(True, description="Store asset_id column")
    asset_id_field: str = Field("asset_id", description="Input field to map to asset_id")
    index_asset_id: bool = Field(True, description="Create index on asset_id")
    
    # Validity Configuration
    enable_validity: bool = Field(True, description="Extract and store validity (valid_from/valid_to)")
    
    # Mode A: Relational schema
    attribute_schema: Optional[List[AttributeSchemaEntry]] = Field(
        None, 
        description="List of attribute definitions for relational mode"
    )
    
    # Mode B: JSONB settings
    jsonb_column_name: str = Field("attributes", description="Name of JSONB column")
    use_hot_updates: bool = Field(True, description="Enable HOT updates with FILLFACTOR=80")
    
    # JSONB functional indexes (Strategic B-Tree indexes on specific JSON paths)
    # Map of JSON path -> Cast Type (e.g., {"population": "bigint", "area": "numeric"})
    jsonb_indexed_paths: Dict[str, PostgresType] = Field(
        default_factory=dict,
        description="Functional B-Tree indexes on specific JSONB paths for performance at scale"
    )

    # Partitioning
    partition_attribute: Optional[str] = Field(
        None, 
        description="Attribute name to partition by (must be in identity or have can_partition=True)."
    )

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """Determine partition key contribution based on the requested partition_attribute."""
        if not self.partition_attribute:
            return {}
        
        # Check Identity Columns
        if self.partition_attribute == "asset_id" and self.enable_asset_id:
            return {"asset_id": "VARCHAR(255)"}
        
        if self.partition_attribute == "external_id" and self.enable_external_id:
            return {"external_id": "VARCHAR(255)"}

        # Check Schema Attributes
        if self.attribute_schema:
            for attr in self.attribute_schema:
                if attr.name == self.partition_attribute:
                    if not attr.can_partition:
                        raise ValueError(f"Attribute '{attr.name}' is not marked as can_partition=True")
                    return {attr.name: attr.type.value}
            
        raise ValueError(f"Partition attribute '{self.partition_attribute}' not found in configuration.")

    model_config = {"extra": "allow"}
