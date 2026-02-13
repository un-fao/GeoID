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
Geometry Sidecar Configuration and Enums.

This module is extracted to avoid circular dependencies between:
- catalog_config
- sidecars/geometry
- tools/geospatial
"""

from typing import List, Optional, Dict, Literal
from enum import Enum
from pydantic import Field
from dynastore.modules.catalog.sidecars.base import SidecarConfig

# ============================================================================
# ENUMS
# ============================================================================

class TargetDimension(str, Enum):
    FORCE_2D = "force_2d"
    FORCE_3D = "force_3d"


class InvalidGeometryPolicy(str, Enum):
    REJECT = "reject"
    ATTEMPT_FIX = "attempt_fix"


class SridMismatchPolicy(str, Enum):
    REJECT = "reject"
    TRANSFORM = "transform"


class SimplificationAlgorithm(str, Enum):
    DOUGLAS_PEUCKER = "douglas_peucker"
    TOPOLOGY_PRESERVING = "topology_preserving"
    VISVALINGAM_WHYATT = "visvalingam_whyatt"


class GeometryPartitionStrategyPreset(str, Enum):
    """
    Partition strategies supported by the Geometry Sidecar.
    """
    H3_CELL = "h3_cell"        # Partition by H3 cell ID (BIGINT)
    S2_CELL = "s2_cell"        # Partition by S2 cell ID (BIGINT)


from dynastore.modules.catalog.sidecars.geometry_stats_config import GeometryStatisticsConfig, StatisticIndexConfig

# ============================================================================
# CONFIGURATION
# ============================================================================

class GeometrySidecarConfig(SidecarConfig):
    """Configuration for GeometrySidecar."""
    sidecar_type: Literal["geometry"] = "geometry"
    
    # Geometry storage settings
    target_srid: int = Field(4326, description="Target SRID for geometry storage")
    target_dimension: TargetDimension = Field(TargetDimension.FORCE_2D)
    write_bbox: bool = Field(True, description="Create separate bbox_geom column")
    
    # Processing policies
    invalid_geom_policy: InvalidGeometryPolicy = Field(InvalidGeometryPolicy.ATTEMPT_FIX)
    allowed_geometry_types: List[str] = Field(default_factory=list)
    srid_mismatch_policy: SridMismatchPolicy = Field(SridMismatchPolicy.TRANSFORM)
    simplification_algorithm: Optional[SimplificationAlgorithm] = None
    simplification_tolerance: Optional[float] = None
    remove_redundant_vertices: bool = False
    
    # Spatial indexes configuration
    h3_resolutions: List[int] = Field(default_factory=list, description="H3 resolutions to index (0-15)")
    s2_resolutions: List[int] = Field(default_factory=list, description="S2 resolutions to index (0-30)")

    # Partitioning
    partition_strategy: Optional[GeometryPartitionStrategyPreset] = Field(
        None, description="Strategy to use for contributing to the global partition key."
    )
    partition_resolution: int = Field(
        0, description="Resolution to use for partitioning (must be in h3_resolutions or s2_resolutions)."
    )
    
    # Geometry Statistics
    statistics: Optional[GeometryStatisticsConfig] = Field(
        GeometryStatisticsConfig(
                enabled= True,
            storage_mode= "columnar",
            area=StatisticIndexConfig(enabled=True, index=True),
            volume=StatisticIndexConfig(enabled=True, index=True),
            length=StatisticIndexConfig(enabled=True, index=True),
            centroid_type= "geometric",
            index_centroid= True
        ),
        description="Geometry statistics configuration for computing and storing metrics"
    )

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        if not self.partition_strategy:
            return {}
        
        if self.partition_strategy == GeometryPartitionStrategyPreset.H3_CELL:
            return {f"h3_res{self.partition_resolution}": "BIGINT"}
        
        if self.partition_strategy == GeometryPartitionStrategyPreset.S2_CELL:
            return {f"s2_res{self.partition_resolution}": "BIGINT"}
            
        return {}

# Rebuild model to resolve forward references
GeometrySidecarConfig.model_rebuild()
