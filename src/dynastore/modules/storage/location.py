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
Storage location configuration — registry-based polymorphism.

Each driver registers its own typed ``StorageLocationConfig`` subclass
via ``__init_subclass__``. The ``StorageLocationConfigRegistry`` resolves
the correct class for deserialization, following the same pattern as
``SidecarConfigRegistry``.
"""

from enum import StrEnum
from typing import Any, ClassVar, Dict, Optional, Type

from pydantic import BaseModel, ConfigDict, Field


class StorageLocationConfigRegistry:
    """Registry for driver-specific StorageLocationConfig subclasses."""

    _registry: Dict[str, Type["StorageLocationConfig"]] = {}

    @classmethod
    def register(cls, driver_id: str, config_cls: Type["StorageLocationConfig"]):
        cls._registry[driver_id] = config_cls

    @classmethod
    def resolve(cls, driver_id: str) -> Type["StorageLocationConfig"]:
        return cls._registry.get(driver_id, StorageLocationConfig)

    @classmethod
    def registered_drivers(cls) -> Dict[str, Type["StorageLocationConfig"]]:
        return dict(cls._registry)


class StorageLocationConfig(BaseModel):
    """Base storage location config. Each driver registers its own typed subclass."""

    driver: str
    uri: Optional[str] = Field(
        None, description="Primary URI (s3://, gs://, file://, etc.)"
    )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        driver_id = cls.__dict__.get("_driver_id")
        if driver_id:
            StorageLocationConfigRegistry.register(driver_id, cls)


class PostgresStorageLocationConfig(StorageLocationConfig):
    """PG location — resolved dynamically from catalog/collection metadata."""

    _driver_id: ClassVar[str] = "postgresql"
    driver: str = "postgresql"
    physical_schema: Optional[str] = Field(
        None, description="Override auto-resolved schema"
    )
    physical_table: Optional[str] = Field(
        None, description="Override auto-resolved table"
    )


class FileStorageLocationConfig(StorageLocationConfig):
    """For file-based drivers (DuckDB, static files)."""

    _driver_id: ClassVar[str] = "duckdb"
    driver: str = "duckdb"
    path: Optional[str] = Field(None, description="Read path (file or glob)")
    format: str = Field("parquet", description="File format: parquet, csv, json, etc.")
    write_path: Optional[str] = Field(
        None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Optional[str] = Field(
        None, description="Write format if different from read"
    )


class WarehouseScheme(StrEnum):
    """Supported warehouse URI schemes for OTF drivers."""

    GCS = "gs"
    S3 = "s3"
    FILE = "file"


class OTFStorageLocationConfig(StorageLocationConfig):
    """For Open Table Format drivers (Iceberg, Delta Lake, Hudi).

    Warehouse resolution order:
      1. Explicit ``warehouse_uri`` if set
      2. Auto-detected from platform ``StorageProtocol`` (GCS bucket, future S3)
      3. Local temp dir fallback (``file://``)
    """

    model_config = ConfigDict(extra="allow")

    _driver_id: ClassVar[str] = "iceberg"
    driver: str = "iceberg"

    # Catalog
    catalog_name: Optional[str] = Field(
        None, description="OTF catalog name (e.g., Glue, Hive, REST)"
    )
    catalog_uri: Optional[str] = Field(None, description="OTF catalog URI")
    catalog_type: Optional[str] = Field(
        None,
        description="Catalog type: sql (default, uses platform DATABASE_URL), rest, glue, hive, dynamodb",
    )
    catalog_properties: Optional[Dict[str, Any]] = Field(
        None,
        description="Extra catalog-specific properties (e.g., warehouse, credentials)",
    )

    # Warehouse — auto-resolved from StorageProtocol by default
    warehouse_uri: Optional[str] = Field(
        None,
        description="Manual override for warehouse URI. Auto-resolved from the "
        "collection's existing storage bucket (GCS/S3) when not set.",
    )
    warehouse_scheme: Optional[WarehouseScheme] = Field(
        None,
        description="Manual override for warehouse scheme. "
        "Auto-detected from warehouse_uri or StorageProtocol.",
    )

    # Table location
    namespace: Optional[str] = Field(None, description="OTF namespace/database")
    table_name: Optional[str] = Field(None, description="OTF table name")
