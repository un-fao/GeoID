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

# dynastore/modules/stac/stac_config.py

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, model_validator
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config

# --- Configuration Identifiers ---
STAC_PLUGIN_CONFIG_ID = "stac"

# --- STAC Core & Navigation ---

class StacLink(BaseModel):
    """Represents a standard STAC Link Object."""
    rel: str
    href: str
    type: Optional[str] = "application/json"
    title: Optional[str] = None
    method: Optional[str] = "GET"

# --- Dynamic Hierarchy Configuration ---

class HierarchyStrategy(str, Enum):
    """Defines the strategy to build the hierarchy."""
    FIXED = "FIXED"         # Levels are defined by fixed rules (e.g. L1, L2, L3)
    RECURSIVE = "RECURSIVE" # Levels are defined recursively (Parent -> Child)

class DatacubeDimensionType(str, Enum):
    """Common types for datacube dimensions."""
    SPATIAL = "spatial"
    TEMPORAL = "temporal"
    ORDINAL = "ordinal"
    NOMINAL = "nominal"

class DynamicSourceType(str, Enum):
    """Defines how to dynamically populate datacube dimension values."""
    ATTRIBUTE_SCAN = "attribute_scan"
    SQL_QUERY = "sql_query"
    STATIC = "static"

class DynamicSource(BaseModel):
    type: DynamicSourceType = DynamicSourceType.ATTRIBUTE_SCAN
    target_attribute: Optional[str] = None
    custom_query: Optional[str] = None

class DatacubeDimension(BaseModel):
    type: DatacubeDimensionType
    description: Optional[str] = None
    axis: Optional[str] = None
    extent: Optional[List[Any]] = None
    values: Optional[List[Any]] = None
    step: Optional[Union[float, str]] = None
    unit: Optional[str] = None
    reference_system: Optional[Union[str, int, dict]] = 4326
    dynamic_source: Optional[DynamicSource] = None

class HierarchyRule(BaseModel):
    """Defines a specific hierarchy level rule based on row content."""
    strategy: HierarchyStrategy = Field(HierarchyStrategy.FIXED, description="The strategy used for this rule.")
    
    # Identification
    hierarchy_id: str = Field(..., description="Unique identifier for this hierarchy rule (e.g., 'admin_level_0', 'region').")
    parent_hierarchy_id: Optional[str] = Field(None, description="The hierarchy_id of the parent level (e.g., 'admin_level_0' for a region level). Used to generate parent/child links between hierarchy levels.")
    
    # Common Fields
    item_code_field: str = Field(..., description="The feature attribute used as the ID (e.g., iso_code, l4).")
    parent_code_field: Optional[str] = Field(None, description="The attribute for the parent ID within items (e.g., continent_code). Links items to their parent in the same collection.")
    
    # Fixed Strategy Fields
    level_name: Optional[str] = Field(None, description="Name of this level (e.g. 'Region', 'Level 4').")
    condition: Optional[str] = Field(None, description="A SQL-like filter string (e.g., type='admin_0', l4 IS NOT NULL).")
    
    # Recursive Strategy Fields
    root_condition: Optional[str] = Field(None, description="(For Recursive Strategy) A filter to identify root nodes (e.g., parent_code IS NULL).")
    
    # Virtual Collection Metadata
    collection_title_template: Optional[str] = Field(None, description="Template for virtual collection title (e.g., '{level_name}: {value}').")
    collection_description_template: Optional[str] = Field(None, description="Template for virtual collection description.")
    
    # Filtering and Links
    link_properties: Optional[List[str]] = Field(None, description="Properties to expose in hierarchy links.")
    item_filter_properties: Optional[List[str]] = Field(None, description="Additional properties for item filtering.")
    
    # Metadata
    datacube_dim: Optional[DatacubeDimension] = Field(None, description="Metadata for OGC Cube dimensions associated with this level.")
    
    # Virtual View Mapping (deprecated in favor of hierarchy_id)
    id_alias: Optional[str] = Field(None, description="An alias for the generated collection ID to be used in virtual hierarchy endpoints.")

class HierarchyConfig(BaseModel):
    """Defines how to dynamically extract parent/child relationships."""
    enabled: bool = Field(True, description="Enable or disable hierarchical link generation.")
    rules: Dict[str, HierarchyRule] = Field(default_factory=dict, description="Dictionary of rules keyed by hierarchy_id to define parent/child relationships.")

    @model_validator(mode='after')
    def check_rules_if_enabled(self) -> 'HierarchyConfig':
        """
        Validates that if the hierarchy is enabled, at least one rule is defined.
        """
        if self.enabled and not self.rules:
            # We allow empty rules if just enabled but no specific logic is configured yet
            pass 
        return self


class DatacubeVariable(BaseModel):
    type: str = "data"
    description: Optional[str] = None
    unit: Optional[str] = None
    dimensions: List[str] = Field(default_factory=list)

# --- Aggregation Configuration (OGC STAC Aggregation Extension) ---

class AggregationType(str, Enum):
    """OGC STAC Aggregation types."""
    TERM = "term"              # Frequency counts by unique values
    STATS = "stats"            # Statistical aggregation (min, max, avg, sum, count)
    GEOHASH = "geohash"        # Spatial aggregation using geohash grid
    GEOTILE = "geotile"        # Tile-based spatial aggregation
    DATETIME = "datetime"      # Temporal histogram aggregation
    BBOX = "bbox"              # Bounding box extent aggregation
    TEMPORAL_EXTENT = "temporal_extent"  # Temporal extent aggregation (min/max datetime)

class AggregationRule(BaseModel):
    """Defines a single aggregation rule."""
    name: str = Field(..., description="Unique name for this aggregation")
    type: AggregationType = Field(..., description="Type of aggregation to perform")
    property: str = Field(..., description="Property to aggregate (e.g., 'properties.asset_code', 'properties.country')")
    limit: int = Field(10, ge=1, le=1000, description="Maximum number of buckets to return")
    precision: Optional[int] = Field(None, description="Precision for geohash/geotile aggregations (1-12 for geohash)")
    interval: Optional[str] = Field(None, description="Interval for datetime histograms (e.g., '1d', '1M', '1y')")

class AggregationConfig(BaseModel):
    """Configuration for STAC aggregations following OGC extension."""
    enabled: bool = Field(True, description="Enable or disable aggregation support")
    default_rules: List[AggregationRule] = Field(default_factory=list, description="Pre-configured aggregations available by default")
    allow_custom: bool = Field(True, description="Allow ad-hoc aggregations via API requests")
    max_aggregations_per_request: int = Field(5, ge=1, le=10, description="Maximum number of aggregations per request")

# --- Simplification (Replaces env vars) ---

class SimplificationConfig(BaseModel):
    """
    Dynamic geometry simplification rules applied at query time.
    """
    vertex_thresholds: Dict[int, float] = Field(
        default_factory=lambda: {50000: 0.1, 10000: 0.01, 1000: 0.001}
    )
    default_tolerance: float = 0.0001

class AssetAccessMode(str, Enum):
    """Defines how the asset file is exposed in the STAC Item."""
    PROXY = "proxy"       # Use the internal proxy service (safe, default)
    DIRECT = "direct"     # Expose the raw storage URL (e.g. s3:// or https://storage..., good for public buckets)

class AssetTrackingConfig(BaseModel):
    """Configuration for dynamic asset tracking."""
    enabled: bool = Field(True, description="Enable or disable dynamic tracking for ingestion sources.")
    access_mode: AssetAccessMode = Field(AssetAccessMode.DIRECT, description="Method to expose the source file.")

# --- THE PLUGIN DEFINITION ---

@register_config(STAC_PLUGIN_CONFIG_ID)
class StacPluginConfig(PluginConfig):
    """
    Mutable STAC metadata and behavior configuration.
    """
    # Extension schemas
    enabled_extensions: List[str] = Field(default_factory=list)
    
    # Metadata summaries
    summaries: Dict[str, dict] = Field(default_factory=dict)
    
    # Datacube definitions
    cube_dimensions: Dict[str, DatacubeDimension] = Field(default_factory=dict)
    cube_variables: Dict[str, DatacubeVariable] = Field(default_factory=dict)
    
    # Navigation & Hierarchy
    navigation_links: List[StacLink] = Field(default_factory=list)
    hierarchy: Optional[HierarchyConfig] = None

    # --- Asset Tracking (Runtime only) ---
    asset_tracking: AssetTrackingConfig = Field(default_factory=AssetTrackingConfig)

    # --- Aggregations (OGC STAC Extension) ---
    aggregations: Optional[AggregationConfig] = None

    # Geometry Simplification (Query time)
    simplification: SimplificationConfig = Field(default_factory=SimplificationConfig)
