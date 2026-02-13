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
Geometry Statistics Configuration Models.

Provides configuration for computing and storing geospatial statistics
alongside geometry data with strategic indexing for trillion-row scale.
"""

from enum import Enum
from typing import Optional, Literal, Dict
from pydantic import BaseModel, Field


class MorphologicalIndex(str, Enum):
    """Supported morphological indices for geometry analysis."""
    CIRCULARITY = "circularity"
    CONVEXITY = "convexity"
    ASPECT_RATIO = "aspect_ratio"
    SPHERICITY = "sphericity"  # 3D only
    FLATNESS = "flatness"      # 3D only


class StatisticStorageMode(str, Enum):
    """How to store computed statistics."""
    JSONB = "jsonb"       # All stats in single JSONB column
    COLUMNAR = "columnar" # Individual typed columns


class StatisticIndexConfig(BaseModel):
    """Per-statistic configuration."""
    enabled: bool = Field(False, description="Compute this statistic")
    index: bool = Field(False, description="Create B-Tree index on this statistic")


class GeometryStatisticsConfig(BaseModel):
    """
    Configuration for geometry statistics computation and storage.
    
    Supports dual storage modes:
    - JSONB: All stats in single column with functional B-Tree indexes
    - Columnar: Individual typed columns with direct B-Tree indexes
    """
    enabled: bool = Field(False, description="Enable statistics computation")
    storage_mode: StatisticStorageMode = Field(
        StatisticStorageMode.JSONB,
        description="Storage mode: JSONB or individual columns"
    )
    
    # Basic geometric metrics with indexing control
    area: StatisticIndexConfig = Field(
        default_factory=StatisticIndexConfig,
        description="2D surface area or 3D surface area"
    )
    volume: StatisticIndexConfig = Field(
        default_factory=StatisticIndexConfig,
        description="Volume (3D closed meshes only)"
    )
    length: StatisticIndexConfig = Field(
        default_factory=StatisticIndexConfig,
        description="Perimeter or line length"
    )
    
    # Centroid
    centroid_type: Optional[Literal["geometric", "weighted", "median"]] = Field(
        None,
        description="Type of centroid to compute"
    )
    index_centroid: bool = Field(False, description="Create index on centroid coordinates")
    
    # Morphological indices (map of index -> should_index)
    morphological_indices: Dict[MorphologicalIndex, bool] = Field(
        default_factory=dict,
        description="Map of morphological index to whether it should be indexed"
    )
    
    # Topology
    vertex_count: StatisticIndexConfig = Field(
        default_factory=StatisticIndexConfig,
        description="Number of vertices in geometry"
    )
    hole_count: StatisticIndexConfig = Field(
        default_factory=StatisticIndexConfig,
        description="Number of holes (interior rings)"
    )
    
    # Legacy GIN support (not recommended for large datasets)
    create_gin_index: bool = Field(
        False, 
        description="Create GIN index on geom_stats JSONB (WARNING: Large at trillion-row scale)"
    )
