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
``_plugin_id = "driver:{domain}:{driver_id}"``.  The config is stored/retrieved via
the existing config API and 4-tier waterfall
(collection > catalog > platform > code defaults).

Domain separation uses **class inheritance**:

- ``CollectionDriverConfig`` — base for collection-domain drivers
- ``AssetDriverConfig`` — base for asset-domain drivers

Capabilities describe *how* the driver operates (SYNC, ASYNC, TRANSACTIONAL,
etc.), not *what operation* it performs.  Operations are defined in
``RoutingPluginConfig`` (see ``routing_config.py``).
"""

import os
from enum import StrEnum
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional

from pydantic import ConfigDict, Field, SerializeAsAny, field_validator, model_validator

from dynastore.modules.db_config.platform_config_service import (
    Immutable,
    PluginConfig,
    WriteOnce,
)


# ---------------------------------------------------------------------------
# Driver-level configuration — env-var-based, follows DBConfig pattern
# ---------------------------------------------------------------------------


class DuckDBConfig:
    """DuckDB driver-level configuration (env vars).

    Controls connection pooling, resource limits, and extension loading for
    the DuckDB storage driver.  Follows the same pattern as
    ``DBConfig`` (``DB_POOL_*``) and Elasticsearch (``ES_*``).
    """

    pool_size: int = int(os.getenv("DUCKDB_POOL_SIZE", "4"))
    max_memory: str = os.getenv("DUCKDB_MAX_MEMORY", "4GB")
    threads: int = int(os.getenv("DUCKDB_THREADS", "4"))
    extensions: str = os.getenv("DUCKDB_EXTENSIONS", "spatial")
    read_timeout: int = int(os.getenv("DUCKDB_READ_TIMEOUT", "30"))
    write_timeout: int = int(os.getenv("DUCKDB_WRITE_TIMEOUT", "60"))
    fetch_chunk_size: int = int(os.getenv("DUCKDB_FETCH_CHUNK_SIZE", "500"))


class IcebergConfig:
    """Iceberg driver-level configuration (env vars).

    Controls catalog pool sizing and timeouts for the Iceberg storage driver.
    """

    catalog_pool_size: int = int(os.getenv("ICEBERG_CATALOG_POOL_SIZE", "4"))
    catalog_timeout: int = int(os.getenv("ICEBERG_CATALOG_TIMEOUT", "30"))


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
# Collection-level write policy — driver-agnostic, applies to all drivers
# ---------------------------------------------------------------------------


class WriteConflictPolicy(StrEnum):
    """Item-level conflict policy — applied per entity when identity already exists.

    Drivers that declare ``Capability.EXTERNAL_ID_TRACKING`` read this policy
    from ``CollectionWritePolicy`` via the config waterfall and apply it during
    ``write_entities()``.  Drivers without that capability fall back to their
    native behaviour (typically UPDATE).

    Identity keys:
      - ``geoid``: always a new UUID, always unique. Never duplicated, even by
        ``new_version`` (which archives the old entity and inserts a fresh one
        with a new geoid).  This is the system identity key.
      - ``external_id``: optional.  When ``external_id_field`` is set, a fresh
        geoid is always generated and conflict resolution uses external_id.
        When absent, conflict resolution uses geoid directly.
    """

    UPDATE = "update"           # overwrite in place (default)
    NEW_VERSION = "new_version" # archive old, insert new with new geoid
    REFUSE = "refuse"           # skip this entity, continue batch


class AssetConflictPolicy(StrEnum):
    """Asset-level (batch-level) conflict policy — checked before item processing.

    When set, the driver checks whether any entity in the incoming asset batch
    has a duplicate identity before processing items.  A positive match triggers
    the configured response for the entire batch.

    This operates at a different level than ``WriteConflictPolicy`` (per-item).
    Both can be combined: e.g. ``on_conflict=REFUSE`` (skip individual item
    duplicates) and ``on_asset_conflict=REFUSE`` (reject entire batch if any
    duplicate is found).
    """

    REFUSE = "refuse_asset"     # hard stop — reject the entire asset batch


class CollectionWritePolicy(PluginConfig):
    """Collection-level write behaviour, applied by all capable drivers.

    Registered as ``plugin_id = "collection:write_policy"`` in the config waterfall
    (collection > catalog > platform > code default).

    All drivers (PG, ES, Iceberg, DuckDB) read this single config during
    ``write_entities()`` via::

        configs = get_protocol(ConfigsProtocol)
        policy = await configs.get_config(
            "collection:write_policy", catalog_id=catalog_id, collection_id=collection_id
        )

    The ``context`` dict passed to ``write_entities()`` carries runtime values
    that override config defaults:
    - ``asset_id``             — source asset reference (from ingestion pipeline)
    - ``external_id_override`` — explicit external_id bypassing field extraction
    - ``valid_from``           — validity range start (ISO-8601 or datetime)
    - ``valid_to``             — validity range end (None = open-ended)

    Composable policies:
      ``on_conflict`` (item-level) and ``on_asset_conflict`` (batch-level) act
      independently — both can be set simultaneously.  Example::

          on_conflict = WriteConflictPolicy.REFUSE        # skip item duplicates
          on_asset_conflict = AssetConflictPolicy.REFUSE  # reject whole batch
    """

    _plugin_id: ClassVar[Optional[str]] = "collection:write_policy"

    on_conflict: WriteConflictPolicy = Field(
        default=WriteConflictPolicy.UPDATE,
        description=(
            "Item-level conflict policy applied per entity. "
            "UPDATE overwrites in place; NEW_VERSION archives old and inserts "
            "new with a fresh geoid; REFUSE skips the entity and continues."
        ),
    )
    on_asset_conflict: Optional[AssetConflictPolicy] = Field(
        default=None,
        description=(
            "Asset-level (batch-level) conflict policy, checked before item processing. "
            "None = no batch-level check. "
            "REFUSE rejects the entire asset batch if any duplicate identity is found."
        ),
    )
    track_asset_id: bool = Field(
        default=True,
        description="Store asset_id from write context in the entity document.",
    )
    external_id_field: Optional[str] = Field(
        default=None,
        description=(
            "Dot-notation path to extract external_id from the entity. "
            "E.g. 'id' (Feature.id), 'properties.code', 'properties.src_id'. "
            "When None, conflict detection uses geoid directly. "
            "When set, a fresh geoid is always generated and conflict resolution "
            "uses the extracted external_id."
        ),
    )
    require_external_id: bool = Field(
        default=False,
        description="Refuse entity if external_id cannot be extracted.",
    )
    enable_validity: bool = Field(
        default=False,
        description="Track valid_from / valid_to temporal range per entity.",
    )
    validity_field: str = Field(
        default="valid_from",
        description="Field to extract validity start from entity.",
    )


# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


class DriverPluginConfig(PluginConfig):
    """Base for all per-driver configs.

    Subclasses **must** set ``_plugin_id = "driver:{domain}:{driver_id}"`` to
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


class DriverRecordsPostgresqlConfig(CollectionDriverConfig):
    """PostgreSQL collection driver config.

    Absorbs fields previously in ``PostgresStorageLocationConfig`` and
    PG-specific fields from ``CollectionPluginConfig`` (sidecars,
    partitioning, collection_type).

    CRITICAL: ``sidecars`` and ``partitioning`` are **Immutable** — they
    cannot be changed once the physical table exists.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:records:postgresql"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )

    # From PostgresStorageLocationConfig — WriteOnce: None → value allowed (set by
    # ensure_storage()); once set to a non-None value, mutation is rejected.
    physical_schema: WriteOnce[Optional[str]] = Field(
        default=None, description="Override auto-resolved schema. Set once by ensure_storage()."
    )
    physical_table: WriteOnce[Optional[str]] = Field(
        default=None, description="Override auto-resolved table. Set once by ensure_storage()."
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
        default="VECTOR",
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

    @model_validator(mode="before")
    @classmethod
    def strip_geometry_for_records(cls, data: Any) -> Any:
        """RECORDS collections have no spatial component — strip geometry from defaults.

        Runs before field assignment so the Immutable guard is not violated.
        Only affects new construction (from dict/defaults); existing DB-loaded
        RECORDS configs should already lack a geometry sidecar.
        """
        if isinstance(data, dict) and data.get("collection_type") == "RECORDS":
            sidecars = data.get("sidecars")
            if sidecars and isinstance(sidecars, list):
                from dynastore.modules.catalog.sidecars.geometries_config import (
                    GeometriesSidecarConfig,
                )
                data["sidecars"] = [
                    s for s in sidecars if not (
                        isinstance(s, GeometriesSidecarConfig)
                        or (isinstance(s, dict) and s.get("sidecar_type") == "geometries")
                    )
                ]
        return data

    @model_validator(mode="after")
    def validate_composite_partitioning(self) -> "DriverRecordsPostgresqlConfig":
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
    def validate_sidecar_partition_mirroring(self) -> "DriverRecordsPostgresqlConfig":
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
            if sidecar is None:
                continue
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


class DriverRecordsElasticsearchConfig(CollectionDriverConfig):
    """Elasticsearch collection driver config.

    Uses the stac-fastapi-elasticsearch-opensearch (SFEOS) library by
    convention.  The default ``index_prefix`` matches SFEOS's
    ``STAC_ITEMS_INDEX_PREFIX`` (``items_``), producing per-collection
    indexes like ``items_{collection_id}`` that are natively readable by
    an external SFEOS app running in read-only mode.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:records:elasticsearch"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field(
        default="items_",
        description=(
            "Item index name prefix.  Default ``items_`` matches SFEOS convention "
            "(env: STAC_ITEMS_INDEX_PREFIX), producing per-collection indexes "
            "``items_{collection_id}``."
        ),
    )
    mapping: Dict[str, Any] = Field(
        default_factory=dict,
        description="ES index mapping overrides merged with SFEOS defaults.",
    )


class DuckDbCollectionDriverConfig(CollectionDriverConfig):
    """DuckDB collection driver config.

    Absorbs fields previously in ``FileStorageLocationConfig``.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:records:duckdb"

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )
    path: Optional[str] = Field(default=None, description="Read path (file or glob)")
    format: str = Field(default="parquet", description="File format: parquet, csv, json, etc.")
    write_path: Optional[str] = Field(default=None, description="Separate write path (e.g., SQLite file)"
    )
    write_format: Optional[str] = Field(default=None, description="Write format if different from read"
    )


class DriverRecordsIcebergConfig(CollectionDriverConfig):
    """Iceberg collection driver config.

    Absorbs fields previously in ``OTFStorageLocationConfig``.
    """

    _plugin_id: ClassVar[Optional[str]] = "driver:records:iceberg"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC, DriverCapability.BATCH}),
    )

    # Catalog
    catalog_name: Optional[str] = Field(
        default=None, description="OTF catalog name (e.g., Glue, Hive, REST)"
    )
    catalog_uri: Optional[str] = Field(default=None, description="OTF catalog URI")
    catalog_type: Optional[str] = Field(
        default=None,
        description="Catalog type: sql (default), rest, glue, hive, dynamodb",
    )
    catalog_properties: Optional[Dict[str, Any]] = Field(
        default=None, description="Extra catalog-specific properties"
    )

    # Warehouse
    warehouse_uri: Optional[str] = Field(
        default=None, description="Manual override for warehouse URI."
    )
    warehouse_scheme: Optional[str] = Field(
        default=None, description="Manual override for warehouse scheme (gs, s3, file)."
    )

    # Table location
    namespace: Optional[str] = Field(default=None, description="OTF namespace/database")
    table_name: Optional[str] = Field(default=None, description="OTF table name")
    uri: Optional[str] = Field(
        default=None, description="Primary URI (s3://, gs://, file://, etc.)"
    )


# ---------------------------------------------------------------------------
# Asset-domain concrete configs
# ---------------------------------------------------------------------------


class DriverAssetPostgresqlConfig(AssetDriverConfig):
    """PostgreSQL asset driver config."""

    _plugin_id: ClassVar[Optional[str]] = "driver:asset:postgresql"

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}),
    )


class DriverAssetElasticsearchConfig(AssetDriverConfig):
    """Elasticsearch asset driver config."""

    _plugin_id: ClassVar[Optional[str]] = "driver:asset:elasticsearch"

    model_config = ConfigDict(extra="allow")

    capabilities: FrozenSet[str] = Field(
        default=frozenset({DriverCapability.ASYNC}),
    )
    index_prefix: str = Field("assets_", description="Asset index name prefix.")


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

WRITE_POLICY_PLUGIN_ID = "collection:write_policy"

# Register CollectionWritePolicy so the waterfall can look it up
from dynastore.modules.db_config.platform_config_service import ConfigRegistry as _CR  # noqa: E402

_CR.register(WRITE_POLICY_PLUGIN_ID, CollectionWritePolicy)

from dynastore.models.protocols.field_definition import (  # noqa: E402
    FeatureTypeDefinition as _FeatureTypeBase,
    FieldDefinition as _FieldDefinition,
    EntityLevel as _EntityLevel,
    FEATURE_TYPE_PLUGIN_ID,
)


class FeatureTypePluginConfig(PluginConfig):
    """PluginConfig wrapper for FeatureTypeDefinition — registerable in the waterfall.

    Inherits all fields from the protocol-level ``FeatureTypeDefinition``
    and adds ``PluginConfig`` compliance (``enabled``, ``_plugin_id``).
    """

    _plugin_id: ClassVar[Optional[str]] = FEATURE_TYPE_PLUGIN_ID

    level: _EntityLevel = _EntityLevel.ITEM
    fields: Dict[str, _FieldDefinition] = Field(default_factory=dict)
    exclude_fields: Optional[List[str]] = None
    metadata_fields: Optional[Dict[str, Any]] = None


_CR.register(FEATURE_TYPE_PLUGIN_ID, FeatureTypePluginConfig)


# ---------------------------------------------------------------------------
# Apply handler — write_policy ↔ feature_type cross-validation (Task E)
# ---------------------------------------------------------------------------

import logging as _logging  # noqa: E402

_logger = _logging.getLogger(__name__)

_ALWAYS_VALID_EXTERNAL_ID_FIELDS = frozenset({"geoid", "id"})


async def _on_apply_write_policy(
    config: PluginConfig,
    catalog_id: "Optional[str]",
    collection_id: "Optional[str]",
    db_resource: "Optional[Any]",
) -> None:
    """Cross-validate write_policy.external_id_field against feature_type.fields.

    If ``external_id_field`` is set and a ``feature_type`` config exists at the
    same scope, the referenced field must appear in ``feature_type.fields``.

    ``external_id_field = "geoid"`` or ``"id"`` are always accepted (system fields).
    If ``feature_type`` is not yet configured, validation is skipped.
    """
    if not isinstance(config, CollectionWritePolicy):
        return
    ext_id = config.external_id_field
    if not ext_id or ext_id in _ALWAYS_VALID_EXTERNAL_ID_FIELDS:
        return

    if not (catalog_id and collection_id):
        return  # only validate at collection scope

    # Extract leaf field name from dot-path (e.g. "properties.code" → "code")
    field_key = ext_id.split(".")[-1]

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return

        feature_type = await configs.get_config(
            FEATURE_TYPE_PLUGIN_ID,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        defined_fields = getattr(feature_type, "fields", {})
        if not defined_fields:
            return  # no fields defined yet — skip validation

        if field_key not in defined_fields:
            raise ValueError(
                f"write_policy.external_id_field '{ext_id}' (field key: '{field_key}') "
                f"is not defined in feature_type.fields for {catalog_id}/{collection_id}. "
                f"Defined fields: {sorted(defined_fields)}. "
                f"Set 'geoid' or 'id' to use system identity fields without feature_type restriction."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "write_policy cross-validation skipped for %s/%s: %s",
            catalog_id, collection_id, exc,
        )


_CR.register_apply_handler(WRITE_POLICY_PLUGIN_ID, _on_apply_write_policy)
