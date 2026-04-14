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
    model_validator,
)
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
)
from typing import ClassVar, List, Optional

from dynastore.modules.catalog.sidecars.base import SidecarConfig
from dynastore.modules.catalog.sidecars.geometries_config import GeometriesSidecarConfig


class CollectionTypeEnum(str, Enum):
    VECTOR = "VECTOR"
    RASTER = "RASTER"
    RECORDS = "RECORDS"


# Legacy alias
GeometryStorageConfig = GeometriesSidecarConfig


# --- Partitioning (Physical) ---


class CompositePartitionConfig(BaseModel):
    """
    Configuration for Composite Partitioning.
    Defines the ordered list of keys (columns) that form the partition key.
    These keys must be provided by the enabled sidecars (or the Hub).
    """

    enabled: bool = Field(default=False, description="Enable partitioning for this collection.")
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


class CollectionPluginConfig(PluginConfig):
    """Collection configuration — structural only.

    PG-specific fields (``sidecars``, ``partitioning``, ``collection_type``)
    have moved to ``DriverRecordsPostgresqlConfig``
    (``plugin_id = "driver:records:postgresql"``).

    Storage routing is handled by ``RoutingPluginConfig``
    (``plugin_id = "collection:drivers"``).
    """

    model_config = {"extra": "allow"}

    max_bulk_features: int = Field(
        default=10000,
        description="Maximum number of features allowed in a single bulk insert.",
    )


CollectionPluginConfig.model_rebuild()
