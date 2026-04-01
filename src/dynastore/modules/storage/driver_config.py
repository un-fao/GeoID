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
Per-driver plugin configurations.

Each driver instance registers its own ``PluginConfig`` subclass with
``_plugin_id = "driver:<driver_id>"``.  The config is stored/retrieved via
the existing config API and 4-tier waterfall
(collection > catalog > platform > code defaults).

Domain separation uses **class inheritance**:

- ``CollectionDriverConfig`` — base for collection-domain drivers
- ``AssetDriverConfig`` — base for asset-domain drivers

Capabilities describe *how* the driver operates (SYNC, ASYNC, TRANSACTIONAL,
etc.), not *what operation* it performs.  Operations are defined in
``RoutingPluginConfig`` (see ``routing_config.py``).
"""

from enum import StrEnum
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional

from pydantic import ConfigDict, Field, SerializeAsAny, field_validator, model_validator

from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
)


# ---------------------------------------------------------------------------
# Driver capabilities — describe HOW the driver operates
# ---------------------------------------------------------------------------


class DriverCapability(StrEnum):
    """Standard capabilities describing driver behaviour."""

    SYNC = "SYNC"
    ASYNC = "ASYNC"
    TRANSACTIONAL = "TRANSACTIONAL"
    STREAMING = "STREAMING"
    BATCH = "BATCH"


# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


class DriverPluginConfig(PluginConfig):
    """Base for all per-driver configs.

    Subclasses **must** set ``_plugin_id = "driver:<driver_id>"`` to
    auto-register with the ``ConfigRegistry``.

    Fields shared across all drivers:

    * ``capabilities`` — frozenset of :class:`DriverCapability` strings
      describing how the driver performs operations.
    """

    capabilities: FrozenSet[str] = Field(
        default_factory=frozenset,
        description="How the driver operates: SYNC, ASYNC, TRANSACTIONAL, etc.",
    )


class CollectionDriverConfig(DriverPluginConfig):
    """Base for collection-domain driver configs."""

    pass


class AssetDriverConfig(DriverPluginConfig):
    """Base for asset-domain driver configs."""

    pass


# ---------------------------------------------------------------------------
# Collection-domain concrete configs
# ---------------------------------------------------------------------------


class PostgresCollectionDriverConfig(CollectionDriverConfig):
    """PostgreSQL collection driver config.

    Absorbs fields previously in ``PostgresStorageLocationConfig`` and
    PG-specific fields from ``CollectionPluginConfig`` (sidecars,
    partitioning, collection_type).

    CRITICAL: ``sidecars`` and ``partitioning`` are **Immutable** — they
    cannot be changed once the physical table exists.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:postgresql"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )

    # From PostgresStorageLocationConfig
    physical_schema: Optional[str] = Field(
        default=None, description="Override auto-resolved schema"
    )
    physical_table: Optional[str] = Field(
        default=None, description="Override auto-resolved table"
    )

    # From CollectionPluginConfig — PG-specific structural fields
    sidecars: Immutable[List[SerializeAsAny[Any]]] = Field(
        default_factory=lambda: _default_sidecars(),
        description="Sidecar table configs (GeometriesSidecarConfig, FeatureAttributeSidecarConfig, etc.)",
    )
    partitioning: Immutable[Any] = Field(
        default_factory=lambda: _default_partitioning(),
        description="Composite partition config for PG tables.",
    )
    collection_type: str = Field(
        "VECTOR",
        description="Collection type: VECTOR or RASTER.",
    )

    # ------------------------------------------------------------------
    # Validators (moved from CollectionPluginConfig)
    # ------------------------------------------------------------------

    @field_validator("sidecars", mode="before")
    @classmethod
    def validate_sidecars_polymorphic(cls, v: Any) -> Any:
        """Instantiate sidecar configs as their specialized subclasses."""
        if isinstance(v, list):
            from dynastore.modules.catalog.sidecars.base import SidecarConfigRegistry

            processed = []
            for item in v:
                if isinstance(item, dict) and "sidecar_type" in item:
                    sidecar_type = item["sidecar_type"]
                    config_cls = SidecarConfigRegistry.resolve_config_class(sidecar_type)
                    processed.append(config_cls.model_validate(item))
                else:
                    processed.append(item)
            return processed
        return v

    @field_validator("partitioning", mode="before")
    @classmethod
    def coerce_partitioning(cls, v: Any) -> Any:
        """Convert plain dict back to CompositePartitionConfig on deserialization."""
        if isinstance(v, dict):
            from dynastore.modules.catalog.catalog_config import CompositePartitionConfig
            return CompositePartitionConfig.model_validate(v)
        return v

    @model_validator(mode="after")
    def validate_composite_partitioning(self) -> "PostgresCollectionDriverConfig":
        """Validate partition keys are provided by configured sidecars."""
        if not self.partitioning.enabled:
            return self

        available_keys = {"transaction_time", "geoid"}
        for sidecar in self.sidecars:
            available_keys.update(sidecar.partition_key_contributions.keys())

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
    def validate_sidecar_partition_mirroring(self) -> "PostgresCollectionDriverConfig":
        """Ensure all sidecars mirror the Hub's partition strategy."""
        if self.sidecars and self.partitioning.enabled:
            pass  # Enforced at sidecar DDL generation level
        return self

    def get_column_definitions(self) -> Dict[str, str]:
        """Hub table column definitions (sidecar columns are separate)."""
        return {
            "geoid": "UUID PRIMARY KEY",
            "transaction_time": "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
            "deleted_at": "TIMESTAMPTZ",
            "content_hash": "VARCHAR(64)",
        }

    def get_all_field_definitions(self) -> Dict[str, Any]:
        """Aggregate field definitions from all enabled sidecars."""
        all_fields: Dict[str, Any] = {}
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

        for sc_config in self.sidecars:
            if not sc_config.enabled:
                continue
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            all_fields.update(sidecar.get_field_definitions())
        return all_fields


def _default_sidecars() -> list:
    """Lazy import to avoid circular dependency with sidecar configs."""
    from dynastore.modules.catalog.sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )
    from dynastore.modules.catalog.sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
    )

    return [GeometriesSidecarConfig(), FeatureAttributeSidecarConfig()]


def _default_partitioning() -> Any:
    """Lazy import to avoid circular dependency with catalog_config."""
    from dynastore.modules.catalog.catalog_config import CompositePartitionConfig

    return CompositePartitionConfig()


class ElasticsearchCollectionDriverConfig(CollectionDriverConfig):
    """Elasticsearch collection driver config."""

    _plugin_id: ClassVar[Optional[str]] = "driver:elasticsearch"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field("stac_", description="Index name prefix.")
    mapping: Dict[str, Any] = Field(
        default_factory=dict, description="ES index mapping overrides."
    )


class DuckDbCollectionDriverConfig(CollectionDriverConfig):
    """DuckDB collection driver config.

    Absorbs fields previously in ``FileStorageLocationConfig``.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:duckdb"

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )
    path: Optional[str] = Field(None, description="Read path (file or glob)")
    format: str = Field("parquet", description="File format: parquet, csv, json, etc.")
    write_path: Optional[str] = Field(
        None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Optional[str] = Field(
        None, description="Write format if different from read"
    )


class IcebergCollectionDriverConfig(CollectionDriverConfig):
    """Iceberg collection driver config.

    Absorbs fields previously in ``OTFStorageLocationConfig``.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:iceberg"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )

    # Catalog
    catalog_name: Optional[str] = Field(
        None, description="OTF catalog name (e.g., Glue, Hive, REST)"
    )
    catalog_uri: Optional[str] = Field(None, description="OTF catalog URI")
    catalog_type: Optional[str] = Field(
        None,
        description="Catalog type: sql (default), rest, glue, hive, dynamodb",
    )
    catalog_properties: Optional[Dict[str, Any]] = Field(
        None, description="Extra catalog-specific properties"
    )

    # Warehouse
    warehouse_uri: Optional[str] = Field(
        None, description="Manual override for warehouse URI."
    )
    warehouse_scheme: Optional[str] = Field(
        None, description="Manual override for warehouse scheme (gs, s3, file)."
    )

    # Table location
    namespace: Optional[str] = Field(None, description="OTF namespace/database")
    table_name: Optional[str] = Field(None, description="OTF table name")
    uri: Optional[str] = Field(
        None, description="Primary URI (s3://, gs://, file://, etc.)"
    )


# ---------------------------------------------------------------------------
# Asset-domain concrete configs
# ---------------------------------------------------------------------------


class PostgresAssetDriverConfig(AssetDriverConfig):
    """PostgreSQL asset driver config."""

    _plugin_id: ClassVar[Optional[str]] = "driver:postgresql_assets"

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )


class ElasticsearchAssetDriverConfig(AssetDriverConfig):
    """Elasticsearch asset driver config."""

    _plugin_id: ClassVar[Optional[str]] = "driver:elasticsearch_assets"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field("assets_", description="Asset index name prefix.")


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

PG_DRIVER_PLUGIN_ID = "driver:postgresql"


async def get_collection_driver_config(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    operation: str = "WRITE",
    db_resource: Any = None,
) -> "PostgresCollectionDriverConfig":
    """Fetch the active write (or read) driver config for a collection.

    Resolves driver identity from the routing config, then fetches the
    driver-specific ``CollectionPluginConfig`` subclass.  Falls back to a
    default ``PostgresCollectionDriverConfig`` when no config is stored.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol
    from dynastore.modules.storage.routing_config import ROUTING_PLUGIN_CONFIG_ID, RoutingPluginConfig

    configs = get_protocol(ConfigsProtocol)
    routing: RoutingPluginConfig = await configs.get_config(
        ROUTING_PLUGIN_CONFIG_ID,
        catalog_id=catalog_id,
        collection_id=collection_id,
        db_resource=db_resource,
    )
    entries = routing.operations.get(operation, [])
    driver_id = entries[0].driver_id if entries else "postgresql"
    plugin_id = f"driver:{driver_id}"
    config = await configs.get_config(
        plugin_id,
        catalog_id=catalog_id,
        collection_id=collection_id,
        db_resource=db_resource,
    )
    if config is None:
        return PostgresCollectionDriverConfig()
    return config


async def get_pg_collection_config(
    catalog_id: str,
    collection_id: Optional[str] = None,
    *,
    db_resource: Any = None,
) -> "PostgresCollectionDriverConfig":
    """Fetch ``PostgresCollectionDriverConfig`` via the config waterfall.

    Returns the default instance if no config has been explicitly set.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    configs = get_protocol(ConfigsProtocol)
    config = await configs.get_config(
        PG_DRIVER_PLUGIN_ID,
        catalog_id=catalog_id,
        collection_id=collection_id,
        db_resource=db_resource,
    )
    if config is None:
        return PostgresCollectionDriverConfig()
    return config
